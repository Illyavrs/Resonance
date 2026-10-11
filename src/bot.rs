//! IllyaBot, the server's chat bot. It reads commands sent to it as private messages and `!`
//! commands typed in public channels, whichever client wrote them, and answers as an ordinary user.
//! Multiplayer (`!mp`) commands run inside the room chat; see `multiplayer::bot_command`.
use crate::common::{self, Account, ApiError, AppState};
use chrono::{DateTime, Utc};
use resonance_protocol::stable as wire;
use sqlx::{PgConnection, Row};
use std::{sync::OnceLock, time::Duration, time::Instant};

/// Human accounts stay below this id; the bot sits in the reserved system range.
pub const BOT_ID: i64 = 1_000_000;
pub const BOT_NAME: &str = "IllyaBot";
const MAX_REPLY_LINES: usize = 8;

static STARTED: OnceLock<Instant> = OnceLock::new();

fn site() -> String {
    std::env::var("PUBLIC_ORIGIN").unwrap_or_else(|_| "https://shdcloud.top".into())
}

pub fn spawn(state: AppState) {
    STARTED.get_or_init(Instant::now);
    tokio::spawn(async move {
        loop {
            if let Err(error) = tick(&state).await {
                tracing::warn!(code = error.1, "bot tick failed");
            }
            tokio::time::sleep(Duration::from_millis(600)).await;
        }
    });
}

async fn cursor(db: &mut PgConnection, key: &str, initial: &str) -> Result<i64, ApiError> {
    if let Some(value) = sqlx::query_scalar::<_, i64>("SELECT value FROM bot_state WHERE key=$1")
        .bind(key)
        .fetch_optional(&mut *db)
        .await?
    {
        return Ok(value);
    }
    // First run: start from now so the history is not answered retroactively.
    let value: i64 = sqlx::query_scalar(initial).fetch_one(&mut *db).await?;
    set_cursor(db, key, value).await?;
    Ok(value)
}

async fn set_cursor(db: &mut PgConnection, key: &str, value: i64) -> Result<(), ApiError> {
    sqlx::query("INSERT INTO bot_state(key,value) VALUES($1,$2) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value")
        .bind(key)
        .bind(value)
        .execute(db)
        .await?;
    Ok(())
}

async fn tick(state: &AppState) -> Result<(), ApiError> {
    crate::multiplayer::room_bot::run_timers(state).await?;
    let mut db = state.pool.acquire().await?;
    let pm_cursor = cursor(
        &mut db,
        "pm",
        "SELECT COALESCE(max(id),0) FROM protocol_private_messages",
    )
    .await?;
    let rows = sqlx::query("SELECT id,sender_id,content,is_action FROM protocol_private_messages WHERE recipient_id=$1 AND id>$2 ORDER BY id LIMIT 50")
        .bind(BOT_ID)
        .bind(pm_cursor)
        .fetch_all(&mut *db)
        .await?;
    for row in rows {
        let id: i64 = row.get("id");
        set_cursor(&mut db, "pm", id).await?;
        // The bot has no stable session to acknowledge deliveries, so settle them here.
        sqlx::query("UPDATE protocol_private_messages SET stable_delivery_state='delivered' WHERE id=$1")
            .bind(id)
            .execute(&mut *db)
            .await?;
        let sender: i64 = row.get("sender_id");
        let text: String = row.get("content");
        handle_private(state, &mut db, sender, &text, row.get("is_action")).await?;
    }

    let chat_cursor = cursor(
        &mut db,
        "chat",
        "SELECT COALESCE(max(id),0) FROM chat_messages",
    )
    .await?;
    let newest: i64 = sqlx::query_scalar("SELECT COALESCE(max(id),0) FROM chat_messages")
        .fetch_one(&mut *db)
        .await?;
    if newest > chat_cursor {
        let rows = sqlx::query("SELECT m.user_id,m.content,m.channel_id FROM chat_messages m JOIN channels c ON c.id=m.channel_id WHERE m.id>$1 AND m.id<=$2 AND c.kind='PUBLIC' AND c.read_only=false AND m.user_id<>$3 AND m.content LIKE '!%' AND m.private_message_id IS NULL ORDER BY m.id LIMIT 50")
            .bind(chat_cursor)
            .bind(newest)
            .bind(BOT_ID)
            .fetch_all(&mut *db)
            .await?;
        set_cursor(&mut db, "chat", newest).await?;
        for row in rows {
            let Ok(who) = common::account_on_connection(&mut db, row.get("user_id")).await else {
                continue;
            };
            let text: String = row.get("content");
            let lines = match dispatch(state, &mut db, &who, &text, false).await {
                Ok(lines) => lines,
                Err(_) => vec!["指令執行失敗，請稍後再試。".into()],
            };
            for line in lines.into_iter().take(MAX_REPLY_LINES) {
                sqlx::query("INSERT INTO chat_messages(channel_id,user_id,content) VALUES($1,$2,$3)")
                    .bind(row.get::<i64, _>("channel_id"))
                    .bind(BOT_ID)
                    .bind(clip(&line))
                    .execute(&mut *db)
                    .await?;
            }
        }
    }
    Ok(())
}

fn clip(text: &str) -> String {
    text.chars().take(1000).collect()
}

pub(crate) async fn send_private(db: &mut PgConnection, to: i64, text: &str) -> Result<(), ApiError> {
    sqlx::query("INSERT INTO protocol_private_messages(sender_id,recipient_id,content) VALUES($1,$2,$3)")
        .bind(BOT_ID)
        .bind(to)
        .bind(clip(text))
        .execute(db)
        .await?;
    Ok(())
}

async fn handle_private(
    state: &AppState,
    db: &mut PgConnection,
    sender: i64,
    text: &str,
    action: bool,
) -> Result<(), ApiError> {
    let Ok(who) = common::account_on_connection(&mut *db, sender).await else {
        return Ok(());
    };
    let is_bot: bool = sqlx::query_scalar("SELECT is_bot FROM users WHERE id=$1")
        .bind(sender)
        .fetch_one(&mut *db)
        .await?;
    if is_bot {
        return Ok(());
    }
    if action {
        // "/np" arrives as an action naming the map the player is looking at.
        if let Some(id) = parse_now_playing(text) {
            let found: Option<i64> =
                sqlx::query_scalar("SELECT id FROM beatmaps WHERE official_id=$1 OR native_id=$1 ORDER BY (official_id=$1) DESC LIMIT 1")
                    .bind(id)
                    .fetch_optional(&mut *db)
                    .await?;
            match found {
                Some(beatmap) => {
                    sqlx::query("INSERT INTO bot_np(user_id,beatmap_id) VALUES($1,$2) ON CONFLICT(user_id) DO UPDATE SET beatmap_id=EXCLUDED.beatmap_id,updated_at=now()")
                        .bind(sender)
                        .bind(beatmap)
                        .execute(&mut *db)
                        .await?;
                    let line = describe_map(db, beatmap).await?;
                    send_private(db, sender, &format!("已記下你正在看：{line}")).await?;
                }
                None => {
                    send_private(db, sender, "這張圖譜不在本伺服器的資料庫裡。").await?;
                }
            }
        }
        return Ok(());
    }
    let lines = if text.starts_with('!') {
        match dispatch(state, db, &who, text, true).await {
            Ok(lines) => lines,
            Err(_) => vec!["指令執行失敗，請稍後再試。".into()],
        }
    } else {
        vec!["我是 IllyaBot。輸入 !help 查看我能做什麼。".into()]
    };
    for line in lines.into_iter().take(MAX_REPLY_LINES) {
        send_private(db, sender, &line).await?;
    }
    Ok(())
}

/// The beatmap id inside a "/np" action such as `is listening to [https://osu.ppy.sh/beatmapsets/1#osu/2 Title]`.
fn parse_now_playing(text: &str) -> Option<i64> {
    let url_start = text.find("http")?;
    let rest = &text[url_start..];
    let url = rest.split([' ', ']']).next()?;
    let tail = url
        .rsplit(['/', '#'])
        .find(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))?;
    tail.parse().ok()
}

async fn describe_map(db: &mut PgConnection, beatmap: i64) -> Result<String, ApiError> {
    let row = sqlx::query("SELECT artist,title,version,stars FROM beatmaps WHERE id=$1")
        .bind(beatmap)
        .fetch_one(db)
        .await?;
    Ok(format!(
        "{} - {} [{}] ({:.2}★)",
        row.get::<String, _>("artist"),
        row.get::<String, _>("title"),
        row.get::<String, _>("version"),
        row.get::<f64, _>("stars")
    ))
}

fn mods_text(bits: i64) -> String {
    const NAMES: [(i64, &str); 14] = [
        (1, "NF"),
        (2, "EZ"),
        (8, "HD"),
        (16, "HR"),
        (32, "SD"),
        (64, "DT"),
        (256, "HT"),
        (512, "NC"),
        (1024, "FL"),
        (4096, "SO"),
        (8192, "AP"),
        (16384, "PF"),
        (4, "TD"),
        (128, "RX"),
    ];
    let mut out = String::new();
    for (bit, name) in NAMES {
        if bits & bit != 0 && !(bit == 64 && bits & 512 != 0) {
            out.push_str(name);
        }
    }
    if out.is_empty() { "NM".into() } else { out }
}

fn ago(when: DateTime<Utc>) -> String {
    let seconds = (Utc::now() - when).num_seconds().max(0);
    match seconds {
        0..=59 => format!("{seconds} 秒前"),
        60..=3599 => format!("{} 分鐘前", seconds / 60),
        3600..=86399 => format!("{} 小時前", seconds / 3600),
        _ => format!("{} 天前", seconds / 86400),
    }
}

async fn find_user(db: &mut PgConnection, name: &str) -> Result<Option<Account>, ApiError> {
    let id: Option<i64> = sqlx::query_scalar(
        "SELECT id FROM users WHERE lower(username)=lower($1) AND NOT is_restricted AND NOT is_bot",
    )
    .bind(name)
    .fetch_optional(&mut *db)
    .await?;
    match id {
        Some(id) => Ok(common::account_on_connection(db, id).await.ok()),
        None => Ok(None),
    }
}

/// Everything after the command name, rejoined, for commands that take free text.
fn rest(parts: &[&str], from: usize) -> String {
    parts.get(from..).map(|p| p.join(" ")).unwrap_or_default()
}

const HELP: &[(&str, &str)] = &[
    ("!help", "顯示指令列表"),
    ("!roll [數字]", "擲骰子(1 到 N,預設 100)"),
    ("!ping", "確認我還活著"),
    ("!server", "伺服器在線人數與運作時間"),
    ("!stats [玩家]", "排名、pp、準確率與遊玩次數"),
    ("!recent [玩家]", "最近一筆成績"),
    ("!top [玩家]", "pp 最高的五筆成績"),
    ("!np", "顯示你用 /np 分享的圖譜"),
    ("!maplink", "目前圖譜的網站連結"),
    ("!block 玩家 / !unblock 玩家", "封鎖或解除封鎖私訊"),
    ("!mp help", "多人房間指令(在房間聊天輸入)"),
];

/// Run one command line. Staff commands check permissions here, not in the client.
pub(crate) async fn dispatch(
    state: &AppState,
    db: &mut PgConnection,
    who: &Account,
    text: &str,
    private: bool,
) -> Result<Vec<String>, ApiError> {
    let parts: Vec<&str> = text[1..].split_whitespace().collect();
    let Some(command) = parts.first().map(|c| c.to_lowercase()) else {
        return Ok(vec![]);
    };
    if command == "mp" {
        return Ok(vec!["多人房間指令只能在房間聊天裡使用。".into()]);
    }
    common::rate_limit(state, format!("bot:{}", who.id), 40).await?;
    let site = site();
    let name_arg = |from: usize| parts.get(from).map(|s| s.to_string());
    Ok(match command.as_str() {
        "help" | "h" => {
            let mut lines = vec!["我是 IllyaBot，可用指令：".to_string()];
            for (usage, about) in HELP {
                lines.push(format!("{usage} — {about}"));
            }
            if who.can("broadcast.send") {
                lines.push("!alert 訊息 / !alertuser 玩家 訊息 — 全服或單人通知(管理員)".into());
            }
            if who.can("users.moderate") {
                lines.push("!user 玩家 — 查看帳號資訊(管理員)".into());
            }
            lines
        }
        "ping" => vec!["pong".into()],
        "roll" => {
            let max: u128 = parts
                .get(1)
                .and_then(|n| n.parse::<u128>().ok())
                .filter(|n| (1..=1_000_000).contains(n))
                .unwrap_or(100);
            let value = uuid::Uuid::new_v4().as_u128() % max + 1;
            vec![format!("{} 擲出了 {value}", who.username)]
        }
        "server" => {
            let stable: i64 = sqlx::query_scalar("SELECT count(DISTINCT user_id) FROM bancho_sessions WHERE expires_at>now()")
                .fetch_one(&mut *db)
                .await?;
            let lazer: i64 = sqlx::query_scalar("SELECT count(*) FROM lazer_chat_activity WHERE active_until>now()")
                .fetch_one(&mut *db)
                .await?;
            let uptime = STARTED.get().map(|s| s.elapsed().as_secs()).unwrap_or(0);
            vec![format!(
                "Illyaverse 在線：stable {stable} 人、lazer {lazer} 人。已持續運作 {} 小時 {} 分鐘。網站：{site}",
                uptime / 3600,
                uptime % 3600 / 60
            )]
        }
        "stats" | "stat" => {
            let target = match name_arg(1) {
                Some(name) => find_user(db, &name).await?,
                None => Some(who.clone()),
            };
            let Some(target) = target else {
                return Ok(vec!["找不到這位玩家。".into()]);
            };
            let stats = crate::stable::stats_on_connection(db, &target, Default::default()).await?;
            vec![format!(
                "{} — 全球 #{}，{}pp，準確率 {:.2}%，遊玩 {} 次。{site}/users/{}",
                target.username,
                stats.rank,
                stats.pp,
                stats.accuracy * 100.0,
                stats.play_count,
                target.id
            )]
        }
        "recent" | "r" | "last" => {
            let target = match name_arg(1) {
                Some(name) => find_user(db, &name).await?,
                None => Some(who.clone()),
            };
            let Some(target) = target else {
                return Ok(vec!["找不到這位玩家。".into()]);
            };
            let row = sqlx::query("SELECT s.id,s.passed,s.accuracy,s.max_combo,s.pp,s.mods,s.created_at,b.title,b.artist,b.version,b.stars FROM scores s JOIN beatmaps b ON b.id=s.beatmap_id WHERE s.user_id=$1 AND s.verification<>'rejected' ORDER BY s.created_at DESC,s.id DESC LIMIT 1")
                .bind(target.id)
                .fetch_optional(&mut *db)
                .await?;
            match row {
                None => vec![format!("{} 還沒有成績。", target.username)],
                Some(row) => vec![format!(
                    "{} 的最近成績：{} - {} [{}] ({:.2}★) +{} | {:.2}% | {}x | {:.0}pp | {} | {} [{site}/scores/{} 詳細]",
                    target.username,
                    row.get::<String, _>("artist"),
                    row.get::<String, _>("title"),
                    row.get::<String, _>("version"),
                    row.get::<f64, _>("stars"),
                    mods_text(row.get("mods")),
                    row.get::<f64, _>("accuracy"),
                    row.get::<i32, _>("max_combo"),
                    row.get::<f64, _>("pp"),
                    if row.get::<bool, _>("passed") { "通過" } else { "失敗" },
                    ago(row.get("created_at")),
                    row.get::<i64, _>("id")
                )],
            }
        }
        "top" => {
            let target = match name_arg(1) {
                Some(name) => find_user(db, &name).await?,
                None => Some(who.clone()),
            };
            let Some(target) = target else {
                return Ok(vec!["找不到這位玩家。".into()]);
            };
            let rows = sqlx::query("SELECT s.pp,s.accuracy,s.mods,b.title,b.artist,b.version FROM scores s JOIN beatmaps b ON b.id=s.beatmap_id WHERE s.user_id=$1 AND s.passed AND s.verification='verified' AND s.pp_eligible ORDER BY s.pp DESC LIMIT 5")
                .bind(target.id)
                .fetch_all(&mut *db)
                .await?;
            if rows.is_empty() {
                vec![format!("{} 還沒有可計 pp 的成績。", target.username)]
            } else {
                let mut lines = vec![format!("{} 的最佳成績：", target.username)];
                for (index, row) in rows.iter().enumerate() {
                    lines.push(format!(
                        "{}. {} - {} [{}] +{} | {:.2}% | {:.0}pp",
                        index + 1,
                        row.get::<String, _>("artist"),
                        row.get::<String, _>("title"),
                        row.get::<String, _>("version"),
                        mods_text(row.get("mods")),
                        row.get::<f64, _>("accuracy"),
                        row.get::<f64, _>("pp")
                    ));
                }
                lines
            }
        }
        "np" | "maplink" | "q" => {
            let beatmap: Option<i64> =
                sqlx::query_scalar("SELECT beatmap_id FROM bot_np WHERE user_id=$1")
                    .bind(who.id)
                    .fetch_optional(&mut *db)
                    .await?;
            match beatmap {
                None => vec!["先用 /np 分享一張圖譜給我。".into()],
                Some(beatmap) => {
                    let line = describe_map(db, beatmap).await?;
                    if command == "np" {
                        vec![line]
                    } else {
                        vec![format!("{line} [{site}/beatmaps/{beatmap} 網站連結]")]
                    }
                }
            }
        }
        "block" | "unblock" => {
            let Some(name) = name_arg(1) else {
                return Ok(vec![format!("用法：!{command} 玩家名稱")]);
            };
            let Some(target) = find_user(db, &name).await? else {
                return Ok(vec!["找不到這位玩家。".into()]);
            };
            if target.id == who.id {
                return Ok(vec!["不能對自己這麼做。".into()]);
            }
            if command == "block" {
                sqlx::query("INSERT INTO user_relationships(user_id,target_id,relation_type) VALUES($1,$2,'block') ON CONFLICT(user_id,target_id) DO UPDATE SET relation_type='block'")
                    .bind(who.id)
                    .bind(target.id)
                    .execute(&mut *db)
                    .await?;
                vec![format!("已封鎖 {}。", target.username)]
            } else {
                sqlx::query("DELETE FROM user_relationships WHERE user_id=$1 AND target_id=$2 AND relation_type='block'")
                    .bind(who.id)
                    .bind(target.id)
                    .execute(&mut *db)
                    .await?;
                vec![format!("已解除對 {} 的封鎖。", target.username)]
            }
        }
        "changename" => vec![format!("改名請到網站的設定頁：{site}/settings")],
        "request" | "req" => vec![format!("圖譜上架請求請到網站提出：{site}/beatmaps")],
        "alert" => {
            if !who.can("broadcast.send") {
                return Ok(vec!["你沒有權限使用這個指令。".into()]);
            }
            let message = rest(&parts, 1);
            if message.is_empty() {
                return Ok(vec!["用法：!alert 訊息".into()]);
            }
            let delivered = broadcast_game(db, "公告", &message).await?;
            sqlx::query("INSERT INTO audit_events(actor_id,action,target_type,target_id,details) VALUES($1,'bot.alert','broadcast',NULL,$2)")
                .bind(who.id)
                .bind(serde_json::json!({"message":message,"recipients":delivered}))
                .execute(&mut *db)
                .await?;
            vec![format!("已發送給 {delivered} 位在線玩家。")]
        }
        "alertuser" | "alertu" => {
            if !who.can("broadcast.send") {
                return Ok(vec!["你沒有權限使用這個指令。".into()]);
            }
            let Some(name) = name_arg(1) else {
                return Ok(vec!["用法：!alertuser 玩家 訊息".into()]);
            };
            let message = rest(&parts, 2);
            let Some(target) = find_user(db, &name).await? else {
                return Ok(vec!["找不到這位玩家。".into()]);
            };
            if message.is_empty() {
                return Ok(vec!["用法：!alertuser 玩家 訊息".into()]);
            }
            let packet = wire::string_packet(wire::server::NOTIFICATION, &message)
                .map_err(|_| ApiError(axum::http::StatusCode::BAD_REQUEST, "invalid_message"))?;
            sqlx::query("INSERT INTO bancho_packet_queue(user_id,payload,expires_at) SELECT $1,$2,now()+interval '10 minutes' WHERE EXISTS(SELECT 1 FROM bancho_sessions WHERE user_id=$1 AND expires_at>now())")
                .bind(target.id)
                .bind(packet)
                .execute(&mut *db)
                .await?;
            send_private(db, target.id, &message).await?;
            sqlx::query("INSERT INTO audit_events(actor_id,action,target_type,target_id,details) VALUES($1,'bot.alertuser','user',$2,$3)")
                .bind(who.id)
                .bind(target.id.to_string())
                .bind(serde_json::json!({"message":message}))
                .execute(&mut *db)
                .await?;
            vec![format!("已通知 {}。", target.username)]
        }
        "user" | "u" => {
            if !who.can("users.moderate") {
                return Ok(vec!["你沒有權限使用這個指令。".into()]);
            }
            let Some(name) = name_arg(1) else {
                return Ok(vec!["用法：!user 玩家".into()]);
            };
            let row = sqlx::query("SELECT u.id,u.username,u.country,u.joined_at,u.is_restricted,(SELECT string_agg(r.name,',') FROM user_roles ur JOIN roles r ON r.id=ur.role_id WHERE ur.user_id=u.id) roles,EXISTS(SELECT 1 FROM bancho_sessions b WHERE b.user_id=u.id AND b.expires_at>now()) online FROM users u WHERE lower(u.username)=lower($1) AND NOT u.is_bot")
                .bind(name)
                .fetch_optional(&mut *db)
                .await?;
            match row {
                None => vec!["找不到這位玩家。".into()],
                Some(row) => vec![format!(
                    "#{} {} ({}) 加入於 {} | 身分組：{} | {} | stable {}",
                    row.get::<i64, _>("id"),
                    row.get::<String, _>("username"),
                    row.get::<String, _>("country"),
                    row.get::<DateTime<Utc>, _>("joined_at").format("%Y-%m-%d"),
                    row.get::<Option<String>, _>("roles").unwrap_or_default(),
                    if row.get::<bool, _>("is_restricted") { "已限制" } else { "正常" },
                    if row.get::<bool, _>("online") { "在線" } else { "離線" },
                )],
            }
        }
        _ => {
            if private {
                vec![format!("不認識的指令 !{command}。輸入 !help 查看列表。")]
            } else {
                vec![]
            }
        }
    })
}

/// Push a staff broadcast to everyone connected: a notification popup for stable and, because
/// lazer has no server popup, a private message from the bot for lazer players.
pub async fn broadcast_game(
    db: &mut PgConnection,
    title: &str,
    message: &str,
) -> Result<i64, ApiError> {
    let packet = wire::string_packet(wire::server::NOTIFICATION, &format!("{title}\n{message}"))
        .map_err(|_| ApiError(axum::http::StatusCode::BAD_REQUEST, "invalid_message"))?;
    let stable = sqlx::query("INSERT INTO bancho_packet_queue(user_id,payload,expires_at) SELECT DISTINCT user_id,$1,now()+interval '10 minutes' FROM bancho_sessions WHERE expires_at>now()")
        .bind(packet)
        .execute(&mut *db)
        .await?
        .rows_affected() as i64;
    let line = clip(&format!("[{title}] {}", message.replace('\n', " ")));
    let lazer = sqlx::query("INSERT INTO protocol_private_messages(sender_id,recipient_id,content,stable_delivery_state) SELECT $1,a.user_id,$2,'legacy' FROM lazer_chat_activity a JOIN users u ON u.id=a.user_id WHERE a.active_until>now() AND NOT u.is_restricted AND a.user_id<>$1")
        .bind(BOT_ID)
        .bind(line)
        .execute(&mut *db)
        .await?
        .rows_affected() as i64;
    Ok(stable + lazer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_playing_extracts_the_beatmap_id() {
        assert_eq!(
            parse_now_playing("is listening to [https://osu.ppy.sh/beatmapsets/12#osu/345 Artist - Song]"),
            Some(345)
        );
        assert_eq!(
            parse_now_playing("is playing [https://osu.ppy.sh/b/678 Artist - Song [Hard]]"),
            Some(678)
        );
        assert_eq!(parse_now_playing("is afk"), None);
    }

    #[test]
    fn mods_render_in_a_stable_order() {
        assert_eq!(mods_text(0), "NM");
        assert_eq!(mods_text(8 | 64), "HDDT");
        assert_eq!(mods_text(64 | 512), "NC");
    }
}
