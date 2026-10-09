//! Stable private delivery uses the shared PM row rather than a sequence cursor.
//! A later-committing lower ID must remain pending after a newer response.
use super::{ApiError, ChatMessage, append, id32, wire};
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};

#[cfg(test)]
#[path = "stable_chat_tests.rs"]
mod tests;

pub(super) const DELIVERY_BATCH: i64 = 100;

// Blocks and restrictions can be revoked. Keep those rows pending while
// excluding them before LIMIT, so they do not starve newer allowed messages.
// Friends-only is an admission policy for new sends, not a history read marker.
const PENDING: &str = "SELECT m.id,m.sender_id FROM protocol_private_messages m JOIN users sender ON sender.id=m.sender_id WHERE m.recipient_id=$1 AND m.stable_delivery_state='pending' AND NOT sender.is_restricted AND NOT EXISTS(SELECT 1 FROM user_relationships r WHERE r.relation_type='block' AND ((r.user_id=$1 AND r.target_id=m.sender_id) OR (r.target_id=$1 AND r.user_id=m.sender_id))) ORDER BY m.id LIMIT $2";

pub(super) async fn outgoing_hints(
    tx: &mut Transaction<'_, Postgres>,
    names: &[String],
) -> Result<Vec<(String, i64)>, ApiError> {
    Ok(sqlx::query("SELECT requested.name,u.id FROM unnest($1::text[]) requested(name) JOIN users u ON lower(u.username)=lower(requested.name)")
        .bind(names).fetch_all(&mut **tx).await?.into_iter().map(|row|(row.get("name"),row.get("id"))).collect())
}

pub(super) async fn incoming_hints(
    tx: &mut Transaction<'_, Postgres>,
    user: i64,
) -> Result<Vec<(i64, i64)>, ApiError> {
    Ok(sqlx::query(PENDING)
        .bind(user)
        .bind(DELIVERY_BATCH)
        .fetch_all(&mut **tx)
        .await?
        .into_iter()
        .map(|row| (row.get("id"), row.get("sender_id")))
        .collect())
}

pub(super) async fn recipient(
    tx: &mut Transaction<'_, Postgres>,
    actor: i64,
    name: &str,
    expected_target: Option<i64>,
) -> Result<Option<PgRow>, ApiError> {
    // A newly created or renamed target which was not prelocked is deferred to
    // the next request; never acquire another user lock after the session lock.
    Ok(sqlx::query("SELECT u.id,u.username,(COALESCE(u.preferences->>'pm_friends_only','false')='true' OR EXISTS(SELECT 1 FROM bancho_sessions b WHERE b.user_id=u.id AND b.expires_at>clock_timestamp() AND b.block_non_friend_dms)) friends_only,EXISTS(SELECT 1 FROM user_relationships WHERE user_id=u.id AND target_id=$2 AND relation_type='friend') friend,EXISTS(SELECT 1 FROM user_relationships WHERE relation_type='block' AND ((user_id=u.id AND target_id=$2) OR (user_id=$2 AND target_id=u.id))) blocked FROM users u WHERE lower(u.username)=lower($1) AND u.id<>$2 AND u.id=$3 AND NOT u.is_restricted")
        .bind(name).bind(actor).bind(expected_target).fetch_optional(&mut **tx).await?)
}

pub(super) async fn deliver(
    tx: &mut Transaction<'_, Postgres>,
    actor: i64,
    username: &str,
    candidate_ids: &[i64],
    output: &mut Vec<u8>,
) -> Result<(), ApiError> {
    let rows = sqlx::query("SELECT m.id,m.sender_id,m.content,m.is_action,sender.username FROM protocol_private_messages m JOIN users sender ON sender.id=m.sender_id WHERE m.recipient_id=$1 AND m.id=ANY($2) AND m.stable_delivery_state='pending' AND NOT sender.is_restricted AND NOT EXISTS(SELECT 1 FROM user_relationships r WHERE r.relation_type='block' AND ((r.user_id=$1 AND r.target_id=m.sender_id) OR (r.target_id=$1 AND r.user_id=m.sender_id))) ORDER BY m.id FOR UPDATE OF m")
        .bind(actor).bind(candidate_ids).fetch_all(&mut **tx).await?;
    let mut emitted = Vec::with_capacity(rows.len());
    // Encode the complete batch before changing any delivery state. Any error
    // rolls back this poll, including outbound writes and queued room packets.
    let mut batch = Vec::new();
    for row in rows {
        let message = ChatMessage {
            sender: row.get("username"),
            text: if row.get::<bool, _>("is_action") {
                format!("\u{1}ACTION {}\u{1}", row.get::<String, _>("content"))
            } else {
                row.get("content")
            },
            recipient: username.to_owned(),
            sender_id: id32(row.get("sender_id"))?,
        };
        append(
            &mut batch,
            wire::encode_packet(wire::server::SEND_MESSAGE, &message.encode()?),
        )?;
        emitted.push(row.get::<i64, _>("id"));
    }
    sqlx::query("UPDATE protocol_private_messages SET stable_delivery_state='delivered' WHERE recipient_id=$1 AND id=ANY($2) AND stable_delivery_state='pending'")
        .bind(actor).bind(emitted).execute(&mut **tx).await?;
    output.extend(batch);
    Ok(())
}
