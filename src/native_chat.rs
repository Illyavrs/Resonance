//! Timed send policy, independent of channel access and account permissions.
use crate::common::ApiError;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Silence {
    pub until: DateTime<Utc>,
    pub remaining_seconds: i32,
}

/// Call after the sender's user lock for writes. Database wall time is sampled
/// after lock waits so an expired silence never blocks a newly accepted send.
pub(crate) async fn silence(db: &mut PgConnection, user: i64) -> Result<Option<Silence>, ApiError> {
    let row = sqlx::query("WITH instant AS MATERIALIZED (SELECT clock_timestamp() now) SELECT u.chat_silenced_until,LEAST(2147483647,CEIL(EXTRACT(EPOCH FROM (u.chat_silenced_until-instant.now))))::integer remaining_seconds FROM users u CROSS JOIN instant WHERE u.id=$1 AND u.chat_silenced_until>instant.now")
        .bind(user)
        .fetch_optional(db)
        .await?;
    Ok(row.and_then(|row| {
        let remaining_seconds: i32 = row.get("remaining_seconds");
        (remaining_seconds > 0).then(|| Silence {
            until: row.get("chat_silenced_until"),
            remaining_seconds,
        })
    }))
}
