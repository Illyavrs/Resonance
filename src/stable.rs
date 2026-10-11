use crate::common::{self, Account, ApiError, AppState};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{ConnectInfo, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Duration, Utc};
use illya_domain::ScoreVariant;
use resonance_protocol::stable::{
    self as wire, ChatMessage, LoginRequest, Presence, Reader, UserAction, UserStats,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::Row;
use std::{collections::HashMap, net::SocketAddr};

#[path = "stable_chat.rs"]
mod chat;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/web/osu-getseasonal.php", get(seasonal))
        .route("/web/bancho_connect.php", get(bancho_connect))
        .route("/web/osu-getfriends.php", get(friends))
        .route("/web/osu-getreplay.php", get(replay))
        .route("/web/osu-osz2-getscores.php", get(leaderboard))
        .route("/web/osu-search.php", get(search))
        .route("/web/osu-search-set.php", get(search_set))
        .route("/web/osu-getbeatmapinfo.php", post(beatmap_info))
        .route(
            "/web/osu-submit-modular-selector.php",
            post(crate::stable_scores::submit)
                .layer(axum::extract::DefaultBodyLimit::max(20_971_520)),
        )
        .route("/osu/{id}", get(beatmap_file))
        .route("/d/{id}", get(beatmap_archive))
}

async fn seasonal() -> Json<Vec<String>> {
    Json(Vec::new())
}

async fn bancho_connect() -> &'static str {
    ""
}

fn binary(body: Vec<u8>, token: Option<&str>) -> Response {
    let mut response = (
        [
            ("content-type", "application/octet-stream"),
            ("cho-protocol", "19"),
        ],
        body,
    )
        .into_response();
    if let Some(token) = token.and_then(|v| v.parse().ok()) {
        response.headers_mut().insert("cho-token", token);
    }
    response
}
fn append(
    output: &mut Vec<u8>,
    packet: resonance_protocol::Result<Vec<u8>>,
) -> Result<(), ApiError> {
    output.extend(packet?);
    Ok(())
}
fn id32(id: i64) -> Result<i32, ApiError> {
    i32::try_from(id)
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "stable_user_id_overflow"))
}
fn failure(code: i32, message: &str) -> Response {
    let mut bytes = wire::int_packet(wire::server::USER_ID, code).unwrap_or_default();
    bytes.extend(wire::string_packet(wire::server::NOTIFICATION, message).unwrap_or_default());
    binary(bytes, Some("no"))
}

pub async fn bancho(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    match headers.get("osu-token") {
        None => login(&state, &common::client_key(&state, peer, &headers), &body).await,
        Some(token) => {
            let token = token
                .to_str()
                .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid_session_token"))?;
            if token.len() > 128 {
                return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_session_token"));
            }
            poll(&state, token, &body).await
        }
    }
}

async fn login(state: &AppState, client: &str, body: &[u8]) -> Result<Response, ApiError> {
    common::rate_limit(state, format!("stable-login:{client}"), 20).await?;
    let login = match LoginRequest::parse(body) {
        Ok(value) => value,
        Err(_) => return Ok(failure(-1, "Invalid login request.")),
    };
    let credentials =
        match common::verify_credentials(state, &login.username, &login.password_md5, true).await {
            Ok(credentials) => credentials,
            Err(ApiError(StatusCode::UNAUTHORIZED, _)) => {
                return Ok(failure(-1, "Incorrect credentials or restricted account."));
            }
            Err(error) => return Err(error),
        };
    let mut tx = state.pool.begin().await?;
    let player = match common::lock_authenticated(&mut tx, &credentials).await {
        Ok(player) => player,
        Err(ApiError(StatusCode::UNAUTHORIZED, _)) => {
            return Ok(failure(-1, "Incorrect credentials or restricted account."));
        }
        Err(error) => return Err(error),
    };
    id32(player.id)?;
    sqlx::query("DELETE FROM bancho_sessions WHERE user_id=$1")
        .bind(player.id)
        .execute(&mut *tx)
        .await?;
    crate::multiplayer::disconnect(&mut tx, player.id).await?;
    let channel_rows =
        sqlx::query("SELECT id,name,description,required_permission FROM channels ORDER BY id")
            .fetch_all(&mut *tx)
            .await?;
    let mut channel_ids = Vec::new();
    let mut output = Vec::new();
    append(
        &mut output,
        wire::int_packet(wire::server::USER_ID, id32(player.id)?),
    )?;
    append(
        &mut output,
        wire::int_packet(wire::server::PROTOCOL_VERSION, 19),
    )?;
    // No administrative wire flags grant server permissions.
    append(&mut output, wire::int_packet(wire::server::PRIVILEGES, 5))?;
    let silence = crate::native_chat::silence(&mut tx, player.id).await?;
    append_silence(&mut output, silence, silence.is_some())?;
    append(
        &mut output,
        wire::friends_list(&common::friend_ids_in_tx(&mut tx, player.id).await?),
    )?;
    for channel in channel_rows {
        let permission: Option<String> = channel.get("required_permission");
        if permission
            .as_deref()
            .is_some_and(|p| matches!(p, "lazer.room" | "chat.pm") || !player.can(p))
        {
            continue;
        }
        let name = format!("#{}", channel.get::<String, _>("name"));
        let topic: String = channel.get("description");
        channel_ids.push(channel.get::<i64, _>("id"));
        append(&mut output, wire::channel_info(&name, &topic, 0))?;
        append(
            &mut output,
            wire::channel_packet(wire::server::CHANNEL_AUTO_JOIN, &name, &topic, 0),
        )?;
    }
    append(
        &mut output,
        wire::encode_packet(wire::server::CHANNEL_INFO_END, &[]),
    )?;
    let last_chat: i64 = sqlx::query_scalar("SELECT COALESCE(max(id),0) FROM chat_messages")
        .fetch_one(&mut *tx)
        .await?;
    // Pending PM delivery belongs to the user, independently of this session.
    // The old sequence cursor remains a compatibility field only.
    let last_dm = 0_i64;
    let token = common::secret();
    sqlx::query("INSERT INTO bancho_sessions(id,token_hash,user_id,expires_at,last_chat_id,last_dm_id,channels,utc_offset,block_non_friend_dms,action,client_version,client_hash_hash,last_chat_silence_until) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)")
        .bind(uuid::Uuid::new_v4()).bind(common::token_hash(&token)).bind(player.id).bind(Utc::now()+Duration::seconds(180)).bind(last_chat).bind(last_dm).bind(&channel_ids).bind(i16::from(login.utc_offset)).bind(login.block_non_friend_dms).bind(json!(UserAction::default())).bind(&login.client_version).bind(common::token_hash(&login.client_hashes)).bind(silence.map(|value| value.until)).execute(&mut *tx).await?;
    tx.commit().await?;
    append_presence(&mut output, &player, login.utc_offset, 0)?;
    let self_stats = {
        let mut connection = state.pool.acquire().await?;
        stats_on_connection(&mut connection, &player, UserAction::default()).await?
    };
    append(&mut output, self_stats.packet())?;
    {
        // Everyone already online learns about the new player, so room slots and the user list show them.
        let mut announcement = Vec::new();
        append_presence(&mut announcement, &player, login.utc_offset, 0)?;
        append(&mut announcement, self_stats.packet())?;
        let mut connection = state.pool.acquire().await?;
        queue_for_others(&mut connection, player.id, &announcement).await?;
    }
    let online = sqlx::query("SELECT user_id,utc_offset,action FROM bancho_sessions WHERE expires_at>now() AND user_id<>$1 LIMIT 1000").bind(player.id).fetch_all(&state.pool).await?;
    for session in online {
        if let Ok(other) = common::account(state, session.get("user_id")).await {
            let action: UserAction =
                serde_json::from_value(session.get("action")).unwrap_or_default();
            append_presence(
                &mut output,
                &other,
                session.get::<i16, _>("utc_offset") as i8,
                action.mode,
            )?;
        }
    }
    {
        // The bot is always shown as online, like BanchoBot.
        let mut connection = state.pool.acquire().await?;
        append_bot(&mut connection, &mut output).await?;
    }
    append(
        &mut output,
        wire::string_packet(wire::server::NOTIFICATION, "Welcome to Illyaverse."),
    )?;
    Ok(binary(output, Some(&token)))
}

/// Queue a packet for every other connected player. Clients only learn who a user is from the
/// presence and stats they are sent, so a player joining or leaving must be pushed to the rest.
pub(crate) async fn queue_for_others(
    db: &mut sqlx::PgConnection,
    user: i64,
    payload: &[u8],
) -> Result<(), ApiError> {
    sqlx::query("INSERT INTO bancho_packet_queue(user_id,payload,expires_at) SELECT user_id,$2,now()+interval '2 minutes' FROM bancho_sessions WHERE user_id<>$1 AND expires_at>now()")
        .bind(user)
        .bind(payload)
        .execute(db)
        .await?;
    Ok(())
}

/// Presence and stats for IllyaBot, which has no session of its own.
async fn append_bot(db: &mut sqlx::PgConnection, output: &mut Vec<u8>) -> Result<(), ApiError> {
    let Ok(bot) = common::account_on_connection(&mut *db, crate::bot::BOT_ID).await else {
        return Ok(());
    };
    append_presence(output, &bot, 0, 0)?;
    append(
        output,
        stats_on_connection(db, &bot, UserAction::default())
            .await?
            .packet(),
    )
}

fn append_presence(
    output: &mut Vec<u8>,
    player: &Account,
    utc_offset: i8,
    mode: u8,
) -> Result<(), ApiError> {
    append(
        output,
        Presence {
            user_id: id32(player.id)?,
            username: player.username.clone(),
            utc_offset,
            country_code: wire::country_code(&player.country),
            client_privileges: 1,
            mode,
            longitude: 0.0,
            latitude: 0.0,
            rank: 0,
        }
        .packet(),
    )
}

pub(crate) async fn stats_on_connection(
    db: &mut sqlx::PgConnection,
    player: &Account,
    action: UserAction,
) -> Result<UserStats, ApiError> {
    let mode = i16::from(action.mode);
    let variant = ScoreVariant::from_legacy(action.mode, action.mods)
        .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid_score_variant"))?;
    let row = crate::score_aggregates::lifetime(&mut *db, player.id, mode, variant).await?;
    let ranking = crate::score_aggregates::ranking(&mut *db, player.id, mode, variant).await?;
    let pp: f64 = ranking.get("pp");
    Ok(UserStats {
        user_id: id32(player.id)?,
        action,
        ranked_score: ranking.get("ranked_score"),
        accuracy: (row.get::<f64, _>("accuracy") / 100.0) as f32,
        play_count: row.get::<i64, _>("plays").min(i64::from(i32::MAX)) as i32,
        total_score: row.get("total"),
        rank: ranking
            .get::<i64, _>("global_rank")
            .min(i64::from(i32::MAX)) as i32,
        pp: pp.round().clamp(0.0, 65535.0) as u16,
    })
}

async fn poll(state: &AppState, token: &str, body: &[u8]) -> Result<Response, ApiError> {
    let packets = wire::decode_packets(body)?;
    // The relationship rate budget also bounds the rows a crafted batch may
    // lock. Validate every friend payload before starting any operation.
    let mut friend_targets = Vec::new();
    let mut private_names = Vec::new();
    let mut sends_chat = false;
    for packet in &packets {
        if matches!(
            packet.id,
            wire::client::FRIEND_ADD | wire::client::FRIEND_REMOVE
        ) {
            let mut reader = Reader::new(packet.payload);
            friend_targets.push(i64::from(reader.i32()?));
            reader.finish()?;
        }
        if matches!(
            packet.id,
            wire::client::SEND_PUBLIC_MESSAGE | wire::client::SEND_PRIVATE_MESSAGE
        ) {
            sends_chat = true;
        }
        if packet.id == wire::client::SEND_PRIVATE_MESSAGE {
            private_names.push(ChatMessage::decode(packet.payload)?.recipient);
        }
    }
    if friend_targets.len() > 60 {
        return Err(ApiError(StatusCode::TOO_MANY_REQUESTS, "rate_limited"));
    }
    if private_names.len() > 60 {
        return Err(ApiError(StatusCode::TOO_MANY_REQUESTS, "rate_limited"));
    }
    let mut tx = state.pool.begin().await?;
    if sends_chat {
        // Permission writers acquire this fence before users and sessions.
        sqlx::query("LOCK TABLE roles,user_roles IN SHARE MODE")
            .execute(&mut *tx)
            .await?;
    }
    let token_hash = common::token_hash(token);
    // This first read is only a lock-order hint. The token is rechecked under
    // its session lock after the user locks, matching password reset/login and
    // other relationship writers. No second transaction reacquires those rows.
    let user_id = sqlx::query_scalar::<_, i64>(
        "SELECT user_id FROM bancho_sessions WHERE token_hash=$1 AND expires_at>clock_timestamp()",
    )
    .bind(&token_hash)
    .fetch_optional(&mut *tx)
    .await?;
    let mut incoming_ids = Vec::new();
    let mut private_targets = HashMap::new();
    let locked_users = if let Some(user_id) = user_id {
        for (name, target_id) in chat::outgoing_hints(&mut tx, &private_names).await? {
            friend_targets.push(target_id);
            private_targets.insert(name, target_id);
        }
        for (message_id, sender_id) in chat::incoming_hints(&mut tx, user_id).await? {
            incoming_ids.push(message_id);
            friend_targets.push(sender_id);
        }
        friend_targets.push(user_id);
        friend_targets.sort_unstable();
        friend_targets.dedup();
        sqlx::query_scalar::<_, i64>("SELECT id FROM users WHERE id=ANY($1) ORDER BY id FOR UPDATE")
            .bind(&friend_targets)
            .fetch_all(&mut *tx)
            .await?
    } else {
        Vec::new()
    };
    let session = sqlx::query("SELECT user_id,channels,last_chat_id,utc_offset,action,expires_at,last_chat_silence_until FROM bancho_sessions WHERE token_hash=$1 AND user_id=$2 FOR UPDATE").bind(&token_hash).bind(user_id).fetch_optional(&mut *tx).await?;
    let Some(session) = session else {
        return expired_session();
    };
    let deadline: chrono::DateTime<Utc> = session.get("expires_at");
    let alive: bool = sqlx::query_scalar("SELECT $1::timestamptz>clock_timestamp()")
        .bind(deadline)
        .fetch_one(&mut *tx)
        .await?;
    if !alive {
        return expired_session();
    }
    let player = common::account_in_tx(&mut tx, session.get("user_id")).await?;
    crate::multiplayer::refresh_spectator(&mut tx, player.id).await?;
    let mut channels: Vec<i64> = session.get("channels");
    let mut action: UserAction = serde_json::from_value(session.get("action")).unwrap_or_default();
    let mut output = Vec::new();
    let mut last_silence: Option<DateTime<Utc>> = session.get("last_chat_silence_until");
    let silence = crate::native_chat::silence(&mut tx, player.id).await?;
    let mut send_notice =
        sync_silence(&mut output, &mut last_silence, silence)? && silence.is_some();
    for packet in packets {
        if crate::multiplayer::handle(
            state,
            &mut tx,
            &player,
            packet.id,
            packet.payload,
            &mut output,
        )
        .await?
        {
            continue;
        }
        match packet.id {
            wire::client::LOGOUT => {
                crate::multiplayer::disconnect(&mut tx, player.id).await?;
                let mut quit = id32(player.id)?.to_le_bytes().to_vec();
                quit.push(0);
                queue_for_others(
                    &mut tx,
                    player.id,
                    &wire::encode_packet(wire::server::USER_LOGOUT, &quit)?,
                )
                .await?;
                sqlx::query("DELETE FROM bancho_sessions WHERE token_hash=$1")
                    .bind(common::token_hash(token))
                    .execute(&mut *tx)
                    .await?;
                let alive: bool = sqlx::query_scalar("SELECT $1::timestamptz>clock_timestamp()")
                    .bind(deadline)
                    .fetch_one(&mut *tx)
                    .await?;
                if !alive {
                    return expired_session();
                }
                tx.commit().await?;
                return Ok(binary(Vec::new(), None));
            }
            wire::client::PING => {
                if !packet.payload.is_empty() {
                    return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_ping"));
                }
                // A heartbeat only drains pending events; an idle poll has no packets.
            }
            wire::client::CHANGE_ACTION => {
                action = UserAction::decode(packet.payload)?;
                let stats = stats_on_connection(&mut tx, &player, action.clone())
                    .await?
                    .packet();
                if let Ok(bytes) = &stats {
                    queue_for_others(&mut tx, player.id, bytes).await?;
                }
                append(&mut output, stats)?;
            }
            wire::client::REQUEST_STATUS_UPDATE => {
                append(
                    &mut output,
                    stats_on_connection(&mut tx, &player, action.clone())
                        .await?
                        .packet(),
                )?;
            }
            wire::client::FRIEND_ADD | wire::client::FRIEND_REMOVE => {
                let mut reader = Reader::new(packet.payload);
                let target = i64::from(reader.i32()?);
                reader.finish()?;
                change_friend_in_tx(
                    state,
                    &mut tx,
                    &locked_users,
                    player.id,
                    target,
                    packet.id == wire::client::FRIEND_REMOVE,
                )
                .await?;
            }
            wire::client::TOGGLE_BLOCK_NON_FRIEND_DMS => {
                let mut reader = Reader::new(packet.payload);
                let blocked = reader.i32()?;
                reader.finish()?;
                if !matches!(blocked, 0 | 1) {
                    return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_dm_setting"));
                }
                sqlx::query("UPDATE bancho_sessions SET block_non_friend_dms=$2 WHERE user_id=$1")
                    .bind(player.id)
                    .bind(blocked == 1)
                    .execute(&mut *tx)
                    .await?;
            }
            wire::client::CHANNEL_JOIN | wire::client::CHANNEL_PART => {
                let mut reader = Reader::new(packet.payload);
                let name = reader.string()?.to_owned();
                reader.finish()?;
                if let Some(row) =
                    sqlx::query("SELECT id,required_permission FROM channels WHERE name=$1")
                        .bind(name.trim_start_matches('#'))
                        .fetch_optional(&mut *tx)
                        .await?
                {
                    let id: i64 = row.get("id");
                    let permission: Option<String> = row.get("required_permission");
                    if packet.id == wire::client::CHANNEL_PART {
                        channels.retain(|value| *value != id);
                    } else if permission
                        .as_deref()
                        .is_none_or(|p| !matches!(p, "lazer.room" | "chat.pm") && player.can(p))
                    {
                        if !channels.contains(&id) {
                            channels.push(id);
                        }
                        append(
                            &mut output,
                            wire::string_packet(wire::server::CHANNEL_JOIN_SUCCESS, &name),
                        )?;
                    }
                }
            }
            wire::client::SEND_PUBLIC_MESSAGE | wire::client::SEND_PRIVATE_MESSAGE => {
                let message = ChatMessage::decode(packet.payload)?;
                if !player.can("chat.send") {
                    return Err(ApiError(StatusCode::FORBIDDEN, "chat_permission_required"));
                }
                // This gate precedes both special channels and ordinary PMs.
                // A refused send must not discard other packets or incoming PMs.
                let silence = crate::native_chat::silence(&mut tx, player.id).await?;
                if sync_silence(&mut output, &mut last_silence, silence)? && silence.is_some() {
                    send_notice = true;
                }
                if let Some(silence) = silence {
                    if !send_notice {
                        append_silence(&mut output, Some(silence), true)?;
                        send_notice = true;
                    }
                    continue;
                }
                let text = message.text.trim();
                let (content, is_action) = if let Some(action) = text
                    .strip_prefix("\u{1}ACTION ")
                    .and_then(|v| v.strip_suffix('\u{1}'))
                {
                    (action, true)
                } else {
                    (text, false)
                };
                if content.is_empty() || content.chars().count() > 1000 {
                    return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_chat_length"));
                }
                common::rate_limit(state, format!("chat:{}", player.id), 30).await?;
                if packet.id == wire::client::SEND_PUBLIC_MESSAGE {
                    if crate::multiplayer::special_chat(
                        state,
                        &mut tx,
                        &player,
                        &message.recipient,
                        content,
                    )
                    .await?
                    {
                        continue;
                    }
                    if let Some(row) =
                        sqlx::query("SELECT id,required_permission,read_only FROM channels WHERE name=$1")
                            .bind(message.recipient.trim_start_matches('#'))
                            .fetch_optional(&mut *tx)
                            .await?
                    {
                        let id: i64 = row.get("id");
                        let permission: Option<String> = row.get("required_permission");
                        if channels.contains(&id)
                            && !row.get::<bool, _>("read_only")
                            && permission.as_deref().is_none_or(|p| {
                                !matches!(p, "lazer.room" | "chat.pm") && player.can(p)
                            })
                        {
                            sqlx::query("INSERT INTO chat_messages(channel_id,user_id,content,is_action) VALUES($1,$2,$3,$4)").bind(id).bind(player.id).bind(content).bind(is_action).execute(&mut *tx).await?;
                        }
                    }
                } else {
                    let recipient = chat::recipient(
                        &mut tx,
                        player.id,
                        &message.recipient,
                        private_targets.get(&message.recipient).copied(),
                    )
                    .await?;
                    if let Some(row) = recipient {
                        if !row.get::<bool, _>("blocked")
                            && (!row.get::<bool, _>("friends_only") || row.get::<bool, _>("friend"))
                        {
                            sqlx::query("INSERT INTO protocol_private_messages(sender_id,recipient_id,content,is_action) VALUES($1,$2,$3,$4)").bind(player.id).bind(row.get::<i64,_>("id")).bind(content).bind(is_action).execute(&mut *tx).await?;
                        } else {
                            append(
                                &mut output,
                                wire::encode_packet(
                                    wire::server::USER_DM_BLOCKED,
                                    &ChatMessage {
                                        sender: String::new(),
                                        text: String::new(),
                                        recipient: row.get("username"),
                                        sender_id: 0,
                                    }
                                    .encode()?,
                                ),
                            )?;
                        }
                    } else {
                        append(
                            &mut output,
                            wire::string_packet(
                                wire::server::NOTIFICATION,
                                "This player is unavailable.",
                            ),
                        )?;
                    }
                }
            }
            wire::client::USER_STATS_REQUEST | wire::client::USER_PRESENCE_REQUEST => {
                let mut reader = Reader::new(packet.payload);
                let ids = reader.i32_list()?;
                reader.finish()?;
                if ids.len() > 100 {
                    return Err(ApiError(StatusCode::BAD_REQUEST, "too_many_users"));
                }
                for id in ids {
                    if i64::from(id) == crate::bot::BOT_ID {
                        if packet.id == wire::client::USER_STATS_REQUEST {
                            let bot = common::account_in_tx(&mut tx, crate::bot::BOT_ID).await?;
                            append(
                                &mut output,
                                stats_on_connection(&mut tx, &bot, UserAction::default())
                                    .await?
                                    .packet(),
                            )?;
                        } else {
                            append_bot(&mut tx, &mut output).await?;
                        }
                        continue;
                    }
                    let Some(online) = sqlx::query("SELECT action,utc_offset FROM bancho_sessions WHERE user_id=$1 AND expires_at>now()").bind(i64::from(id)).fetch_optional(&mut *tx).await? else {continue};
                    let Ok(other) = common::account_in_tx(&mut tx, i64::from(id)).await else {
                        continue;
                    };
                    let other_action =
                        serde_json::from_value(online.get("action")).unwrap_or_default();
                    if packet.id == wire::client::USER_STATS_REQUEST {
                        append(
                            &mut output,
                            stats_on_connection(&mut tx, &other, other_action)
                                .await?
                                .packet(),
                        )?;
                    } else {
                        append_presence(
                            &mut output,
                            &other,
                            online.get::<i16, _>("utc_offset") as i8,
                            other_action.mode,
                        )?;
                    }
                }
            }
            _ => {}
        }
    }
    // Recheck channel permissions on every poll; a role change takes effect now.
    let allowed = sqlx::query("SELECT id,name,required_permission FROM channels WHERE id=ANY($1)")
        .bind(&channels)
        .fetch_all(&mut *tx)
        .await?;
    for row in allowed {
        if row
            .get::<Option<String>, _>("required_permission")
            .as_deref()
            .is_some_and(|p| matches!(p, "lazer.room" | "chat.pm") || !player.can(p))
        {
            channels.retain(|id| *id != row.get::<i64, _>("id"));
            append(
                &mut output,
                wire::string_packet(
                    wire::server::CHANNEL_KICK,
                    &format!("#{}", row.get::<String, _>("name")),
                ),
            )?;
        }
    }
    let messages = sqlx::query("SELECT m.id,m.content,m.is_action,m.user_id,u.username,c.name,EXISTS(SELECT 1 FROM user_relationships WHERE relation_type='block' AND ((user_id=$3 AND target_id=m.user_id) OR (target_id=$3 AND user_id=m.user_id))) blocked FROM chat_messages m JOIN users u ON u.id=m.user_id JOIN channels c ON c.id=m.channel_id WHERE m.id>$1 AND m.channel_id=ANY($2) AND NOT u.is_restricted ORDER BY m.id LIMIT 100")
        .bind(session.get::<i64,_>("last_chat_id")).bind(&channels).bind(player.id).fetch_all(&mut *tx).await?;
    let mut last_chat: i64 = session.get("last_chat_id");
    for row in messages {
        last_chat = row.get("id");
        if row.get::<i64, _>("user_id") != player.id && !row.get::<bool, _>("blocked") {
            let message = ChatMessage {
                sender: row.get("username"),
                text: if row.get::<bool, _>("is_action") {
                    format!("\u{1}ACTION {}\u{1}", row.get::<String, _>("content"))
                } else {
                    row.get("content")
                },
                recipient: format!("#{}", row.get::<String, _>("name")),
                sender_id: id32(row.get("user_id"))?,
            };
            append(
                &mut output,
                wire::encode_packet(wire::server::SEND_MESSAGE, &message.encode()?),
            )?;
        }
    }
    chat::deliver(
        &mut tx,
        player.id,
        &player.username,
        &incoming_ids,
        &mut output,
    )
    .await?;
    let queued=sqlx::query("SELECT id,payload FROM bancho_packet_queue WHERE user_id=$1 AND expires_at>now() ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED").bind(player.id).fetch_all(&mut *tx).await?;
    let mut consumed = Vec::new();
    for row in queued {
        output.extend(row.get::<Vec<u8>, _>("payload"));
        consumed.push(row.get::<i64, _>("id"));
    }
    sqlx::query("DELETE FROM bancho_packet_queue WHERE id=ANY($1) OR expires_at<=now()")
        .bind(consumed)
        .execute(&mut *tx)
        .await?;
    // Use the unchanged locked deadline after all resource waits. An expired
    // request cannot consume pending PMs or resurrect its own session.
    // A long poll may cross natural expiry while waiting for another resource.
    let silence = crate::native_chat::silence(&mut tx, player.id).await?;
    sync_silence(&mut output, &mut last_silence, silence)?;
    let refreshed = sqlx::query("UPDATE bancho_sessions SET expires_at=$2,last_chat_id=$3,channels=$4,action=$5,last_chat_silence_until=$7 WHERE token_hash=$1 AND expires_at=$6 AND expires_at>clock_timestamp()")
        .bind(&token_hash).bind(Utc::now()+Duration::seconds(180)).bind(last_chat).bind(channels).bind(json!(action)).bind(deadline).bind(last_silence).execute(&mut *tx).await?;
    if refreshed.rows_affected() != 1 {
        return expired_session();
    }
    tx.commit().await?;
    Ok(binary(output, None))
}

fn expired_session() -> Result<Response, ApiError> {
    let mut output = wire::int_packet(wire::server::RESTART, 0)?;
    append(
        &mut output,
        wire::string_packet(
            wire::server::NOTIFICATION,
            "Session expired. Please reconnect.",
        ),
    )?;
    Ok(binary(output, None))
}

fn append_silence(
    output: &mut Vec<u8>,
    silence: Option<crate::native_chat::Silence>,
    notify: bool,
) -> Result<(), ApiError> {
    append(
        output,
        wire::int_packet(
            wire::server::SILENCE_END,
            silence.map_or(0, |value| value.remaining_seconds),
        ),
    )?;
    if notify {
        let message = silence.map_or_else(
            || "Your chat silence has ended. You can send messages again.".to_owned(),
            |value| {
                format!(
                    "Chat is silenced for {} more seconds. You can still read messages and play.",
                    value.remaining_seconds
                )
            },
        );
        append(
            output,
            wire::string_packet(wire::server::NOTIFICATION, &message),
        )?;
    }
    Ok(())
}

fn sync_silence(
    output: &mut Vec<u8>,
    previous: &mut Option<DateTime<Utc>>,
    current: Option<crate::native_chat::Silence>,
) -> Result<bool, ApiError> {
    let until = current.map(|value| value.until);
    if *previous != until {
        append_silence(output, current, true)?;
        *previous = until;
        return Ok(true);
    }
    Ok(false)
}

#[cfg(test)]
mod timed_silence_wire {
    use super::*;

    #[test]
    fn notices_only_announce_policy_transitions_and_use_remaining_seconds() {
        let until = DateTime::parse_from_rfc3339("2026-10-09T18:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let current = crate::native_chat::Silence {
            until,
            remaining_seconds: 37,
        };
        let mut previous = None;
        let mut bytes = Vec::new();
        assert!(sync_silence(&mut bytes, &mut previous, Some(current)).unwrap());
        let unchanged = bytes.len();
        assert!(
            !sync_silence(
                &mut bytes,
                &mut previous,
                Some(crate::native_chat::Silence {
                    remaining_seconds: 36,
                    ..current
                })
            )
            .unwrap()
        );
        assert_eq!(
            bytes.len(),
            unchanged,
            "countdown changes are not policy changes"
        );
        assert!(sync_silence(&mut bytes, &mut previous, None).unwrap());
        assert!(!sync_silence(&mut bytes, &mut previous, None).unwrap());
        let packets = wire::decode_packets(&bytes).unwrap();
        let durations: Vec<_> = packets
            .iter()
            .filter(|packet| packet.id == wire::server::SILENCE_END)
            .map(|packet| i32::from_le_bytes(packet.payload.try_into().unwrap()))
            .collect();
        assert_eq!(durations, vec![37, 0]);
        assert_eq!(
            packets
                .iter()
                .filter(|packet| packet.id == wire::server::NOTIFICATION)
                .count(),
            2
        );
    }
}

/// The poll has already locked the actor and every friend target in ID order,
/// before locking its Bancho session. Mutations and notifications share that
/// transaction so failed/revoked polls cannot leave committed social changes.
async fn change_friend_in_tx(
    state: &AppState,
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    locked_users: &[i64],
    user_id: i64,
    target_id: i64,
    remove: bool,
) -> Result<(), ApiError> {
    if !common::account_in_tx(tx, user_id)
        .await?
        .can("social.manage")
    {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "social_permission_required",
        ));
    }
    if user_id == target_id {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_relationship"));
    }
    common::rate_limit(state, format!("relationships:{user_id}"), 60).await?;
    // A user inserted after the prelock query is not a valid target in this
    // request: acquiring its FK lock now would invert the session/user order.
    if locked_users.binary_search(&target_id).is_err() {
        return Err(ApiError(StatusCode::NOT_FOUND, "user_not_found"));
    }
    let target_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM users WHERE id=$1 AND NOT is_restricted)")
            .bind(target_id)
            .fetch_one(&mut **tx)
            .await?;
    if !target_exists {
        return Err(ApiError(StatusCode::NOT_FOUND, "user_not_found"));
    }
    if remove {
        sqlx::query("DELETE FROM user_relationships WHERE user_id=$1 AND target_id=$2 AND relation_type='friend'")
            .bind(user_id).bind(target_id).execute(&mut **tx).await?;
    } else {
        let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM user_relationships WHERE ((user_id=$1 AND target_id=$2) OR (user_id=$2 AND target_id=$1)) AND relation_type='block')")
            .bind(user_id).bind(target_id).fetch_one(&mut **tx).await?;
        if blocked {
            return Err(ApiError(StatusCode::FORBIDDEN, "relationship_blocked"));
        }
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM user_relationships WHERE user_id=$1 AND target_id<>$2 AND relation_type='friend'")
            .bind(user_id).bind(target_id).fetch_one(&mut **tx).await?;
        if count >= 4096 {
            return Err(ApiError(StatusCode::BAD_REQUEST, "friend_limit_reached"));
        }
        sqlx::query("INSERT INTO user_relationships(user_id,target_id,relation_type) VALUES($1,$2,'friend') ON CONFLICT(user_id,target_id) DO UPDATE SET relation_type=EXCLUDED.relation_type")
            .bind(user_id).bind(target_id).execute(&mut **tx).await?;
    }
    for id in [user_id, target_id] {
        let packet = wire::friends_list(&common::friend_ids_in_tx(tx, id).await?)?;
        sqlx::query("INSERT INTO bancho_packet_queue(user_id,payload) SELECT $1,$2 WHERE EXISTS(SELECT 1 FROM bancho_sessions WHERE user_id=$1 AND expires_at>now())")
            .bind(id).bind(packet).execute(&mut **tx).await?;
    }
    Ok(())
}

async fn web_auth(state: &AppState, query: &HashMap<String, String>) -> Result<Account, ApiError> {
    let username = query
        .get("u")
        .or_else(|| query.get("us"))
        .ok_or(ApiError(StatusCode::UNAUTHORIZED, "missing_credentials"))?;
    let password = query
        .get("ha")
        .or_else(|| query.get("h"))
        .ok_or(ApiError(StatusCode::UNAUTHORIZED, "missing_credentials"))?;
    if !wire::is_md5(password) {
        return Err(ApiError(StatusCode::UNAUTHORIZED, "invalid_credentials"));
    }
    common::rate_limit(
        state,
        format!("stable-web:{}", username.to_ascii_lowercase()),
        120,
    )
    .await?;
    common::authenticate(state, username, &password.to_ascii_lowercase(), true).await
}
async fn friends(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<String, ApiError> {
    let user = web_auth(&state, &query).await?;
    Ok(common::friend_ids(&state, user.id)
        .await?
        .into_iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join("\n"))
}

async fn replay(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let player = web_auth(&state, &query).await?;
    let id: i64 = query
        .get("c")
        .and_then(|s| s.parse().ok())
        .ok_or(ApiError(StatusCode::BAD_REQUEST, "invalid_score_id"))?;
    let mode: i16 = query
        .get("m")
        .and_then(|s| s.parse().ok())
        .filter(|m| (0..=3).contains(m))
        .ok_or(ApiError(StatusCode::BAD_REQUEST, "invalid_mode"))?;
    let path = sqlx::query_scalar::<_,Option<String>>("SELECT s.replay_path FROM scores s JOIN users u ON u.id=s.user_id WHERE s.id=$1 AND s.mode=$2 AND NOT u.is_restricted AND (s.verification='verified' OR s.user_id=$3)").bind(id).bind(mode).bind(player.id).fetch_optional(&state.pool).await?.flatten().ok_or(ApiError(StatusCode::NOT_FOUND,"replay_not_found"))?;
    let bytes = common::stored_file(&state, &path, "replays", 16_777_216).await?;
    // Sanctuary stores .osr containers; stable's web endpoint consumes compressed frames.
    let container = resonance_protocol::replay::LegacyReplay::parse(&bytes)?;
    Ok(binary(container.compressed_frames.to_vec(), None))
}

pub async fn beatmap_file(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Response, ApiError> {
    let id = crate::native_maps::known_id(&state, id)
        .await?
        .ok_or(ApiError(StatusCode::NOT_FOUND, "beatmap_not_found"))?;
    let path: String = sqlx::query_scalar("SELECT osu_path FROM beatmaps WHERE id=$1")
        .bind(id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or(ApiError(StatusCode::NOT_FOUND, "beatmap_not_found"))?;
    let bytes = common::stored_file(&state, &path, "maps", 8_388_608).await?;
    crate::native_maps::require_active_map(&state.pool, id).await?;
    Ok(([("content-type", "text/plain; charset=utf-8")], bytes).into_response())
}
pub async fn beatmap_archive(
    State(state): State<AppState>,
    Path(raw_id): Path<String>,
) -> Result<Response, ApiError> {
    let id: i64 = raw_id
        .trim_end_matches('n')
        .parse()
        .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid_beatmap_id"))?;
    let archive = crate::native_archives::download(&state, id, None).await?;
    Ok((
        [
            (
                "content-type",
                "application/x-osu-beatmap-archive".to_owned(),
            ),
            ("content-disposition", archive.content_disposition),
            ("cache-control", "private, no-store".to_owned()),
            ("x-content-type-options", "nosniff".to_owned()),
        ],
        archive.bytes,
    )
        .into_response())
}

async fn leaderboard(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<String, ApiError> {
    let player = web_auth(&state, &query).await?;
    let md5 = query
        .get("c")
        .filter(|s| wire::is_md5(s))
        .ok_or(ApiError(StatusCode::BAD_REQUEST, "invalid_beatmap_hash"))?;
    let mode: i16 = query
        .get("m")
        .and_then(|s| s.parse().ok())
        .filter(|m| (0..=3).contains(m))
        .ok_or(ApiError(StatusCode::BAD_REQUEST, "invalid_mode"))?;
    let leaderboard_type: u8 = query.get("v").and_then(|s| s.parse().ok()).unwrap_or(1);
    if !(1..=4).contains(&leaderboard_type) {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "invalid_leaderboard_type",
        ));
    }
    let requested_mods = query
        .get("mods")
        .map(|value| value.parse::<u32>())
        .transpose()
        .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid_score_mods"))?
        .unwrap_or(0);
    let variant = ScoreVariant::from_legacy(mode as u8, requested_mods)
        .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid_score_variant"))?;
    let requested_mods = i64::from(requested_mods);
    let Some(id) = crate::native_maps::resolve_checksum(&state, player.id, md5).await? else {
        return Ok("-1|false|0|0|0|0|\n0\nUnknown beatmap\n0\n\n".into());
    };
    let map = sqlx::query(
        "SELECT id,native_id,native_set_id,title,artist,version,status FROM beatmaps WHERE id=$1",
    )
    .bind(id)
    .fetch_one(&state.pool)
    .await?;
    let friend_filter = if leaderboard_type == 4 {
        let mut ids = common::friend_ids(&state, player.id)
            .await?
            .into_iter()
            .map(i64::from)
            .collect::<Vec<_>>();
        ids.push(player.id);
        Some(ids)
    } else {
        None
    };
    let rows = sqlx::query("WITH best AS (SELECT DISTINCT ON(s.user_id) s.id,s.user_id,s.score,s.pp,s.max_combo,s.mods,s.hit_counts,s.created_at,s.replay_path,u.username FROM scores s JOIN users u ON u.id=s.user_id WHERE s.beatmap_id=$1 AND s.mode=$2 AND s.score_variant=$8 AND s.client='stable' AND (s.mods & 536870912)=$7 AND s.verification='verified' AND s.passed AND NOT u.is_restricted AND ($3::bigint IS NULL OR s.mods=$3) AND ($3::bigint IS NOT NULL OR s.pp_eligible) AND ($8='vanilla' OR s.pp_eligible) AND ($4::text IS NULL OR u.country=$4) AND ($5::bigint[] IS NULL OR s.user_id=ANY($5)) ORDER BY s.user_id,CASE WHEN $8='relax' THEN s.pp ELSE s.score::float8 END DESC,s.id),placed AS (SELECT *,row_number() OVER(ORDER BY CASE WHEN $8='relax' THEN pp ELSE score::float8 END DESC,id) AS placement FROM best) SELECT * FROM placed WHERE placement<=100 OR user_id=$6 ORDER BY placement")
        .bind(id).bind(mode).bind(if leaderboard_type==2 {Some(requested_mods)} else {None})
        .bind(if leaderboard_type==3 {Some(&player.country)}else{None}).bind(friend_filter).bind(player.id).bind(requested_mods & 536870912).bind(variant.as_str()).fetch_all(&state.pool).await?;
    let status = match map.get::<String, _>("status").as_str() {
        "ranked" => 2,
        "loved" => 5,
        _ => 0,
    };
    let (public_id, set_id) = crate::native_maps::row_ids(&map)?;
    let map_name = clean(&format!(
        "{} - {} [{}]",
        map.get::<String, _>("artist"),
        map.get::<String, _>("title"),
        map.get::<String, _>("version")
    ));
    let lines: Vec<String> = rows
        .iter()
        .filter(|row| row.get::<i64, _>("placement") <= 100)
        .map(|row| score_line(row, row.get::<i64, _>("placement") as usize))
        .collect();
    let personal = rows
        .iter()
        .find(|r| r.get::<i64, _>("user_id") == player.id)
        .map(|row| score_line(row, row.get::<i64, _>("placement") as usize))
        .unwrap_or_default();
    crate::native_maps::require_active_map(&state.pool, id).await?;
    Ok(format!(
        "{status}|false|{public_id}|{set_id}|{}|0|\n0\n{map_name}\n0\n{personal}\n{}\n",
        lines.len(),
        lines.join("\n")
    ))
}
fn clean(value: &str) -> String {
    value.replace(['|', '\n', '\r'], " ")
}
fn score_line(row: &sqlx::postgres::PgRow, rank: usize) -> String {
    let hits: Value = row.get("hit_counts");
    let hit = |key| hits.get(key).and_then(Value::as_u64).unwrap_or(0);
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|0|{}|{}|{}|{}|{}",
        row.get::<i64, _>("id"),
        clean(&row.get::<String, _>("username")),
        row.get::<i64, _>("score"),
        row.get::<i32, _>("max_combo"),
        hit("n50"),
        hit("n100"),
        hit("n300"),
        hit("misses"),
        hit("katu"),
        hit("geki"),
        row.get::<i64, _>("mods"),
        row.get::<i64, _>("user_id"),
        rank,
        row.get::<chrono::DateTime<Utc>, _>("created_at")
            .timestamp(),
        if row.get::<Option<String>, _>("replay_path").is_some() {
            1
        } else {
            0
        }
    )
}

async fn search(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<String, ApiError> {
    web_auth(&state, &query).await?;
    let search = query.get("q").cloned().unwrap_or_default();
    if search.len() > 128 {
        return Err(ApiError(StatusCode::BAD_REQUEST, "query_too_long"));
    }
    let page: i64 = query
        .get("p")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
        .clamp(0, 1000);
    let mode: i16 = query.get("m").and_then(|v| v.parse().ok()).unwrap_or(-1);
    if !(-1..=3).contains(&mode) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_mode"));
    }
    let sets = sqlx::query_scalar::<_, i64>("SELECT b.native_set_id FROM active_beatmaps b LEFT JOIN beatmap_packages p ON p.id=b.local_package_id LEFT JOIN official_beatmapset_archives a ON a.official_set_id=b.official_set_id WHERE COALESCE(p.archive_path,a.archive_path,b.archive_path) IS NOT NULL AND ($1=-1 OR b.mode=$1) AND (b.title ILIKE $2 OR b.artist ILIKE $2 OR b.creator ILIKE $2) GROUP BY b.native_set_id ORDER BY max(b.created_at) DESC,b.native_set_id DESC LIMIT 100 OFFSET $3").bind(mode).bind(format!("%{search}%")).bind(page*100).fetch_all(&state.pool).await?;
    let mut output = format!("{}\n", sets.len());
    for set in sets {
        output.push_str(
            &direct_set_line(&state, set, None)
                .await?
                .ok_or(ApiError(StatusCode::NOT_FOUND, "archive_not_available"))?,
        );
        output.push('\n');
    }
    Ok(output)
}
async fn search_set(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<String, ApiError> {
    let player = web_auth(&state, &query).await?;
    let set = if let Some(raw) = query.get("b") {
        let id = raw
            .parse::<i64>()
            .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid_beatmap_id"))?;
        let internal = crate::native_maps::known_id(&state, id).await?;
        if let Some(internal) = internal {
            sqlx::query_scalar("SELECT native_set_id FROM beatmaps WHERE id=$1")
                .bind(internal)
                .fetch_optional(&state.pool)
                .await?
        } else {
            None
        }
    } else if let Some(raw) = query.get("s") {
        let set = raw
            .parse::<i64>()
            .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid_beatmapset_id"))?;
        crate::native_maps::require_id(set)?;
        Some(set)
    } else if let Some(md5) = query.get("c") {
        if !wire::is_md5(md5) {
            return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_beatmap_hash"));
        }
        sqlx::query_scalar("SELECT b.native_set_id FROM beatmap_checksum_identities c JOIN beatmaps b ON b.id=c.beatmap_id WHERE c.md5=$1")
            .bind(md5.to_ascii_lowercase())
            .fetch_optional(&state.pool)
            .await?
    } else {
        None
    };
    let result = match set {
        Some(set) => direct_set_line(&state, set, Some(player.id))
            .await
            .map(|line| line.unwrap_or_default()),
        None => Ok(String::new()),
    };
    // Recheck credentials and account restriction after the metadata wait.
    web_auth(&state, &query).await?;
    result
}
async fn direct_set_line(
    state: &AppState,
    id: i64,
    user: Option<i64>,
) -> Result<Option<String>, ApiError> {
    if crate::native_maps::archive_path(state, id).await?.is_none() {
        // Bulk osu!direct search deliberately keeps its existing retained-set
        // catalogue and does no metadata request per candidate. Explicit set
        // lookup may advertise a separately configured verified acquisition.
        if let Some(user) = user.filter(|_| id < crate::native_maps::LOCAL_ID_MIN) {
            let mut set = crate::native_set_metadata::set_json(state, user, id).await?;
            crate::beatmaps::apply_private_difficulty_statuses(state, id, &mut set).await?;
            if set["download_disabled"] == false {
                if crate::native_maps::archive_path(state, id).await?.is_none() {
                    return metadata_direct_set_line(id, &set).map(Some);
                }
                // Publication during metadata lookup switches back to the
                // existing retained serializer, including its saved timestamp.
            } else {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }
    }
    let rows = sqlx::query("SELECT * FROM beatmaps WHERE native_set_id=$1 ORDER BY mode,stars,id")
        .bind(id)
        .fetch_all(&state.pool)
        .await?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let status = match row.get::<String, _>("status").as_str() {
        "ranked" => 1,
        "loved" => 4,
        _ => 0,
    };
    let difficulties = rows
        .iter()
        .map(|row| {
            format!(
                "[{}*] {}@{}",
                row.get::<f64, _>("stars"),
                clean(&row.get::<String, _>("version")).replace(',', " "),
                row.get::<i16, _>("mode")
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    crate::native_maps::require_active(&state.pool, id).await?;
    Ok(Some(format!(
        "{id}.osz|{}|{}|{}|{status}|0|{}|{id}|0|0|0|0|{difficulties}",
        clean(&row.get::<String, _>("artist")),
        clean(&row.get::<String, _>("title")),
        clean(&row.get::<String, _>("creator")),
        row.get::<chrono::DateTime<Utc>, _>("created_at")
            .format("%Y-%m-%d %H:%M:%S"),
    )))
}

fn metadata_direct_set_line(id: i64, set: &Value) -> Result<String, ApiError> {
    let invalid = || ApiError(StatusCode::BAD_GATEWAY, "invalid_set_metadata_response");
    if set["id"].as_i64() != Some(id) {
        return Err(invalid());
    }
    let maps = set["beatmaps"]
        .as_array()
        .filter(|maps| !maps.is_empty() && maps.len() <= 512)
        .ok_or_else(invalid)?;
    let mut difficulties = Vec::with_capacity(maps.len());
    for map in maps {
        let stars = map["difficulty_rating"]
            .as_f64()
            .filter(|stars| stars.is_finite() && *stars >= 0.0)
            .ok_or_else(invalid)?;
        let version = map["version"].as_str().ok_or_else(invalid)?;
        let mode = map["mode_int"]
            .as_i64()
            .filter(|mode| (0..=3).contains(mode))
            .ok_or_else(invalid)?;
        difficulties.push(format!(
            "[{stars}*] {}@{mode}",
            clean(version).replace(',', " ")
        ));
    }
    let text = |name: &str| set[name].as_str().map(clean).ok_or_else(invalid);
    let submitted =
        chrono::DateTime::parse_from_rfc3339(set["submitted_date"].as_str().ok_or_else(invalid)?)
            .map_err(|_| invalid())?
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
    let status = match set["status"].as_str() {
        Some("ranked") => 1,
        Some("loved") => 4,
        _ => 0,
    };
    Ok(format!(
        "{id}.osz|{}|{}|{}|{status}|0|{submitted}|{id}|0|0|0|0|{}",
        text("artist")?,
        text("title")?,
        text("creator")?,
        difficulties.join(",")
    ))
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct BeatmapInfoRequest {
    #[serde(default)]
    filenames: Vec<String>,
    #[serde(default)]
    ids: Vec<i64>,
}
async fn beatmap_info(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
    axum::Json(request): axum::Json<BeatmapInfoRequest>,
) -> Result<String, ApiError> {
    web_auth(&state, &query).await?;
    if request.ids.len() + request.filenames.len() > 100 {
        return Err(ApiError(StatusCode::BAD_REQUEST, "too_many_beatmaps"));
    }
    let mut resolved = Vec::new();
    if request.ids.is_empty() {
        for (index, filename) in request.filenames.iter().enumerate() {
            if let Some(id) = crate::native_maps::known_filename(&state, filename).await? {
                resolved.push((index, id));
            }
        }
    } else {
        // Keep the existing ID-index contract for mixed requests until native
        // evidence establishes how their two independent arrays are indexed.
        for (index, id) in request.ids.into_iter().enumerate() {
            if let Some(id) = crate::native_maps::known_id(&state, id).await? {
                resolved.push((index, id));
            }
        }
    }
    let mut lines = Vec::new();
    for (index, local_id) in &resolved {
        if let Some(row) =
            sqlx::query("SELECT id,native_id,native_set_id,illya_beatmap_preferred_checksum(id) AS md5,status FROM beatmaps WHERE id=$1")
                .bind(local_id)
                .fetch_optional(&state.pool)
                .await?
        {
            let status = match row.get::<String, _>("status").as_str() {
                "ranked" => 1,
                "loved" => 4,
                _ => 0,
            };
            let (public_id, set_id) = crate::native_maps::row_ids(&row)?;
            lines.push(format!(
                "{index}|{public_id}|{}|{}|{status}|N|N|N|N",
                set_id,
                row.get::<String, _>("md5")
            ));
        }
    }
    for (_, local_id) in &resolved {
        crate::native_maps::require_active_map(&state.pool, *local_id).await?;
    }
    Ok(lines.join("\n"))
}
