//! IllyaBot inside a stable multiplayer room. Anything typed in the room chat that starts with `!`
//! reaches the bot; `!mp ...` manages the room (host, referees and staff only), other commands are
//! answered like anywhere else. Replies go to everyone in the room.
use super::*;
use crate::bot::{self, BOT_ID, BOT_NAME};

/// Server packet that stops a running match on every client.
const ABORT: u16 = 106;
const SPEED: u32 = 64 | 256 | 512;

const HELP: &[(&str, &str)] = &[
    ("!mp settings", "顯示房間設定"),
    (
        "!mp start [秒數|force]",
        "開始遊戲（可倒數；force 無視未準備）",
    ),
    ("!mp abort", "中止進行中的遊戲"),
    ("!mp map <圖譜ID>", "更換圖譜"),
    ("!mp mods <HD DT ...>", "設定 mods（none 為清除）"),
    ("!mp freemods [on|off]", "切換自由 mods"),
    ("!mp host <玩家>", "轉移房主"),
    ("!mp kick <玩家>", "踢出玩家"),
    ("!mp move <玩家> <位置>", "移動玩家到指定位置"),
    ("!mp team <玩家> <red|blue>", "更換玩家隊伍"),
    ("!mp lock / unlock", "鎖定或開放所有空位"),
    ("!mp size <人數>", "限制房間人數"),
    ("!mp password [密碼] / randpw", "設定、清除或隨機化密碼"),
    ("!mp name <名稱>", "更改房間名稱"),
    ("!mp teams <ffa|tag|team|tagteam>", "更改隊伍模式"),
    ("!mp condition <score|acc|combo|v2>", "更改勝利條件"),
    ("!mp invite <玩家>", "邀請玩家"),
    ("!mp addref / rmref / listref", "管理裁判"),
    ("!mp timer <秒數> / aborttimer", "倒數提示與取消"),
    ("!mp close", "解散房間"),
];

fn parse_mods(words: &[&str]) -> Option<u32> {
    let joined: String = words.concat().to_ascii_uppercase().replace('+', "");
    if joined.is_empty() || joined == "NONE" || joined == "NM" {
        return Some(0);
    }
    if !joined.len().is_multiple_of(2) {
        return None;
    }
    let mut bits = 0;
    for pair in joined.as_bytes().chunks(2) {
        bits |= match std::str::from_utf8(pair).ok()? {
            "NF" => 1,
            "EZ" => 2,
            "TD" => 4,
            "HD" => 8,
            "HR" => 16,
            "SD" => 32,
            "DT" => 64,
            "HT" => 256,
            "NC" => 64 | 512,
            "FL" => 1024,
            "SO" => 4096,
            "PF" => 16384 | 32,
            _ => return None,
        };
    }
    // Speed mods cannot be combined.
    if (bits & 64 != 0 && bits & 256 != 0) || (bits & 2 != 0 && bits & 16 != 0) {
        return None;
    }
    Some(bits)
}

async fn say(db: &mut PgConnection, room_id: i16, lines: &[String]) -> Result<(), ApiError> {
    for line in lines.iter().take(24) {
        let text: String = line.chars().take(400).collect();
        let packet = wire::encode_packet(
            wire::server::SEND_MESSAGE,
            &ChatMessage {
                sender: BOT_NAME.into(),
                text,
                recipient: "#multiplayer".into(),
                sender_id: id32(BOT_ID)?,
            }
            .encode()?,
        )?;
        send_room(db, room_id, &packet, None).await?;
    }
    Ok(())
}

async fn names(
    db: &mut PgConnection,
    m: &room::Match,
) -> Result<Vec<(usize, i64, String)>, ApiError> {
    let ids: Vec<i64> = m.users.iter().flatten().map(|id| i64::from(*id)).collect();
    let rows = sqlx::query("SELECT id,username FROM users WHERE id=ANY($1)")
        .bind(ids)
        .fetch_all(db)
        .await?;
    let mut found = Vec::new();
    for (slot, user) in m.users.iter().enumerate() {
        if let Some(id) = user
            && let Some(row) = rows
                .iter()
                .find(|r| r.get::<i64, _>("id") == i64::from(*id))
        {
            found.push((slot, i64::from(*id), row.get::<String, _>("username")));
        }
    }
    Ok(found)
}

fn slot_of(players: &[(usize, i64, String)], name: &str) -> Option<(usize, i64)> {
    players
        .iter()
        .find(|(_, _, n)| n.eq_ignore_ascii_case(name))
        .map(|(slot, id, _)| (*slot, *id))
}

pub(super) async fn run(
    state: &AppState,
    db: &mut PgConnection,
    user: &Account,
    text: &str,
) -> Result<(), ApiError> {
    let Some((mut m, slot)) = current(db, user.id).await? else {
        return Ok(());
    };
    let room_id = m.id as i16;
    let parts: Vec<&str> = text[1..].split_whitespace().collect();
    if !parts.first().is_some_and(|c| c.eq_ignore_ascii_case("mp")) {
        let lines = bot::dispatch(state, db, user, text, false)
            .await
            .unwrap_or_else(|_| vec!["指令執行失敗，請稍後再試。".into()]);
        return say(db, room_id, &lines).await;
    }
    let Some(sub) = parts.get(1).map(|s| s.to_ascii_lowercase()) else {
        return say(db, room_id, &["輸入 !mp help 查看多人房間指令。".into()]).await;
    };
    let referee: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM stable_room_referees WHERE room_id=$1 AND user_id=$2)",
    )
    .bind(room_id)
    .bind(user.id)
    .fetch_one(&mut *db)
    .await?;
    let allowed = m.host_id == id32(user.id)? || referee || user.can("users.moderate");
    let open_to_all = matches!(sub.as_str(), "help" | "h" | "settings" | "set" | "listref");
    if !allowed && !open_to_all {
        return say(
            db,
            room_id,
            &["只有房主、裁判或管理員能使用這個指令。".into()],
        )
        .await;
    }
    let players = names(db, &m).await?;
    let mut lines: Vec<String> = Vec::new();
    let mut changed = false;
    match sub.as_str() {
        "help" | "h" => {
            lines.push("多人房間指令：".into());
            lines.extend(
                HELP.iter()
                    .map(|(usage, about)| format!("{usage} — {about}")),
            );
        }
        "settings" | "set" => {
            let win = ["分數", "準確率", "連擊", "ScoreV2"];
            let team = ["混戰", "標籤合作", "隊伍對戰", "標籤隊伍對戰"];
            lines.push(format!(
                "房間 #{}「{}」| 圖譜：{} | mods：{}{} | 勝利條件：{} | 模式：{}",
                m.id,
                m.name,
                if m.map_name.is_empty() {
                    "未選擇"
                } else {
                    &m.map_name
                },
                if m.mods == 0 {
                    "無".to_string()
                } else {
                    format!("{:#x}", m.mods)
                },
                if m.freemods { "（自由 mods）" } else { "" },
                win.get(m.win_condition as usize).unwrap_or(&"?"),
                team.get(m.team_type as usize).unwrap_or(&"?"),
            ));
            for (slot, _, name) in &players {
                lines.push(format!(
                    "位置 {}：{}{}",
                    slot + 1,
                    name,
                    if m.users[*slot] == Some(m.host_id) {
                        "（房主）"
                    } else {
                        ""
                    }
                ));
            }
        }
        "start" | "st" => {
            if m.in_progress {
                lines.push("遊戲已經在進行中。".into());
            } else if m.map_md5.is_empty() {
                lines.push("請先選擇圖譜。".into());
            } else if parts.get(2).and_then(|s| s.parse::<u32>().ok()).is_some() {
                let seconds = parts[2].parse::<u32>().unwrap().min(300);
                sqlx::query("INSERT INTO stable_room_timers(room_id,due_at,kind) VALUES($1,now()+make_interval(secs=>$2),'start') ON CONFLICT(room_id) DO UPDATE SET due_at=EXCLUDED.due_at,kind=EXCLUDED.kind")
                    .bind(room_id)
                    .bind(f64::from(seconds))
                    .execute(&mut *db)
                    .await?;
                lines.push(format!("遊戲將在 {seconds} 秒後開始。"));
            } else {
                let force = parts
                    .get(2)
                    .is_some_and(|s| s.eq_ignore_ascii_case("force"));
                if let Err(reason) = start_match(db, &mut m, force).await? {
                    lines.push(reason);
                } else {
                    changed = true;
                    lines.push("遊戲開始，祝大家順利！".into());
                }
            }
        }
        "abort" => {
            if !m.in_progress {
                lines.push("目前沒有進行中的遊戲。".into());
            } else {
                m.in_progress = false;
                for i in 0..room::SLOTS {
                    if m.users[i].is_some() && m.statuses[i] == room::PLAYING {
                        m.statuses[i] = room::NOT_READY;
                    }
                }
                send_room(db, room_id, &wire::encode_packet(ABORT, &[])?, None).await?;
                changed = true;
                lines.push("已中止遊戲。".into());
            }
        }
        "map" => match parts.get(2).and_then(|s| s.parse::<i64>().ok()) {
            None => lines.push("用法：!mp map <圖譜ID>".into()),
            Some(id) if m.in_progress => {
                let _ = id;
                lines.push("遊戲進行中無法換圖。".into());
            }
            Some(id) => {
                let row = sqlx::query("SELECT b.native_id,b.mode,b.artist,b.title,b.version,(SELECT h.md5 FROM beatmap_checksum_identities h WHERE h.beatmap_id=b.id ORDER BY h.md5 LIMIT 1) md5 FROM beatmaps b WHERE b.native_id=$1 OR b.official_id=$1 ORDER BY (b.native_id=$1) DESC LIMIT 1")
                    .bind(id)
                    .fetch_optional(&mut *db)
                    .await?;
                match row {
                    Some(row)
                        if row.get::<Option<String>, _>("md5").is_some()
                            && row.get::<Option<i64>, _>("native_id").is_some() =>
                    {
                        m.map_id = row.get::<i64, _>("native_id") as i32;
                        m.map_md5 = row.get::<String, _>("md5");
                        m.map_name = format!(
                            "{} - {} [{}]",
                            row.get::<String, _>("artist"),
                            row.get::<String, _>("title"),
                            row.get::<String, _>("version")
                        );
                        let mode = row.get::<i16, _>("mode");
                        if mode != 0 {
                            m.mode = mode as u8;
                        }
                        for i in 0..room::SLOTS {
                            if m.users[i].is_some() {
                                m.statuses[i] = room::NOT_READY;
                            }
                        }
                        changed = true;
                        lines.push(format!("已選擇圖譜：{}", m.map_name));
                    }
                    _ => lines
                        .push("伺服器上找不到這張圖譜，或它還不能在 stable 多人房間使用。".into()),
                }
            }
        },
        "mods" => {
            if m.in_progress {
                lines.push("遊戲進行中無法更改 mods。".into());
            } else if let Some(bits) = parse_mods(&parts[2.min(parts.len())..]) {
                if m.freemods {
                    m.mods = bits & SPEED;
                    for i in 0..room::SLOTS {
                        m.slot_mods[i] = bits & !SPEED;
                    }
                } else {
                    m.mods = bits;
                }
                changed = true;
                lines.push("已更新 mods。".into());
            } else {
                lines.push("無法辨識的 mods，例如：!mp mods HD DT".into());
            }
        }
        "freemods" | "fm" => {
            let target = match parts.get(2).map(|s| s.to_ascii_lowercase()).as_deref() {
                Some("on") | Some("true") | Some("1") => true,
                Some("off") | Some("false") | Some("0") => false,
                _ => !m.freemods,
            };
            if m.in_progress {
                lines.push("遊戲進行中無法更改。".into());
            } else if target != m.freemods {
                for i in 0..room::SLOTS {
                    m.slot_mods[i] = if target { m.mods & !SPEED } else { 0 };
                }
                if target {
                    m.mods &= SPEED;
                }
                m.freemods = target;
                changed = true;
                lines.push(format!(
                    "自由 mods 已{}。",
                    if target { "開啟" } else { "關閉" }
                ));
            }
        }
        "host" => match parts.get(2).and_then(|n| slot_of(&players, n)) {
            None => lines.push("房間裡沒有這位玩家。".into()),
            Some((_, id)) => {
                m.host_id = id32(id)?;
                queue(db, id, &wire::encode_packet(TRANSFER, &[])?).await?;
                changed = true;
                lines.push("已轉移房主。".into());
            }
        },
        "kick" => match parts.get(2).and_then(|n| slot_of(&players, n)) {
            None => lines.push("房間裡沒有這位玩家。".into()),
            Some((_, id)) if id == user.id => lines.push("不能踢出自己。".into()),
            Some((_, id)) => {
                leave(db, id).await?;
                lines.push("已踢出玩家。".into());
                // `leave` saved and broadcast the room; reload to stay in sync.
                if let Some((fresh, _)) = current(db, user.id).await? {
                    m = fresh;
                }
            }
        },
        "move" => match (
            parts.get(2).and_then(|n| slot_of(&players, n)),
            parts.get(3).and_then(|s| s.parse::<usize>().ok()),
        ) {
            (Some((from, _)), Some(to))
                if (1..=room::SLOTS).contains(&to)
                    && m.statuses[to - 1] == room::OPEN
                    && !m.in_progress =>
            {
                let to = to - 1;
                m.statuses.swap(from, to);
                m.users.swap(from, to);
                m.teams.swap(from, to);
                m.slot_mods.swap(from, to);
                if let Some(id) = m.users[to] {
                    sqlx::query("UPDATE stable_room_members SET slot=$2 WHERE user_id=$1")
                        .bind(i64::from(id))
                        .bind(to as i16)
                        .execute(&mut *db)
                        .await?;
                }
                changed = true;
                lines.push("已移動玩家。".into());
            }
            _ => lines.push("用法：!mp move <玩家> <空位編號 1-16>".into()),
        },
        "team" => match (
            parts.get(2).and_then(|n| slot_of(&players, n)),
            parts.get(3).map(|s| s.to_ascii_lowercase()),
        ) {
            (Some((target, _)), Some(color))
                if m.team_type >= 2 && matches!(color.as_str(), "red" | "blue") =>
            {
                m.teams[target] = if color == "red" { 2 } else { 1 };
                changed = true;
                lines.push("已更換隊伍。".into());
            }
            _ => lines.push("用法：!mp team <玩家> <red|blue>（隊伍模式限定）".into()),
        },
        "lock" | "unlock" => {
            if m.in_progress {
                lines.push("遊戲進行中無法更改。".into());
            } else {
                let (from, to) = if sub == "lock" {
                    (room::OPEN, room::LOCKED)
                } else {
                    (room::LOCKED, room::OPEN)
                };
                for i in 0..room::SLOTS {
                    if m.users[i].is_none() && m.statuses[i] == from {
                        m.statuses[i] = to;
                    }
                }
                changed = true;
                lines.push(
                    if sub == "lock" {
                        "已鎖定所有空位。"
                    } else {
                        "已開放所有空位。"
                    }
                    .into(),
                );
            }
        }
        "size" => match parts.get(2).and_then(|s| s.parse::<usize>().ok()) {
            Some(size) if (1..=room::SLOTS).contains(&size) && !m.in_progress => {
                let occupied = m
                    .users
                    .iter()
                    .rposition(Option::is_some)
                    .map_or(0, |i| i + 1);
                if size < occupied {
                    lines.push(format!("房間最後一位玩家在位置 {occupied}，請先移動玩家。"));
                } else {
                    for i in 0..room::SLOTS {
                        if m.users[i].is_none() {
                            m.statuses[i] = if i < size { room::OPEN } else { room::LOCKED };
                        }
                    }
                    changed = true;
                    lines.push(format!("房間人數上限已設為 {size}。"));
                }
            }
            _ => lines.push("用法：!mp size <1-16>".into()),
        },
        "password" | "pass" | "randpw" => {
            let password = if sub == "randpw" {
                uuid::Uuid::new_v4().simple().to_string()[..8].to_string()
            } else {
                rest_text(&parts, 2)
            };
            let hash = password_hash(password.clone()).await?;
            m.password = if hash.is_some() {
                "#".into()
            } else {
                String::new()
            };
            sqlx::query("UPDATE stable_rooms SET password_hash=$2 WHERE id=$1")
                .bind(room_id)
                .bind(hash)
                .execute(&mut *db)
                .await?;
            changed = true;
            lines.push(if password.is_empty() {
                "已移除房間密碼。".to_string()
            } else if sub == "randpw" {
                format!("新的房間密碼：{password}")
            } else {
                "已更新房間密碼。".to_string()
            });
        }
        "name" => {
            let name = rest_text(&parts, 2);
            if name.is_empty() || name.chars().count() > 50 {
                lines.push("用法：!mp name <名稱>（最多 50 字）".into());
            } else {
                m.name = name;
                changed = true;
                lines.push("已更改房間名稱。".into());
            }
        }
        "teams" | "teamtype" => {
            let kind = match parts.get(2).map(|s| s.to_ascii_lowercase()).as_deref() {
                Some("ffa" | "head-to-head" | "h2h") => Some(0),
                Some("tag" | "tag-coop" | "coop") => Some(1),
                Some("team" | "team-vs" | "teamvs") => Some(2),
                Some("tagteam" | "tag-team-vs" | "tagteamvs") => Some(3),
                _ => None,
            };
            match kind {
                Some(kind) if !m.in_progress => {
                    m.team_type = kind;
                    for i in 0..room::SLOTS {
                        if m.users[i].is_some() {
                            m.teams[i] = if kind >= 2 { m.teams[i].max(1) } else { 0 };
                        }
                    }
                    changed = true;
                    lines.push("已更改隊伍模式。".into());
                }
                _ => lines.push("用法：!mp teams <ffa|tag|team|tagteam>".into()),
            }
        }
        "condition" | "cond" => {
            let kind = match parts.get(2).map(|s| s.to_ascii_lowercase()).as_deref() {
                Some("score") => Some(0),
                Some("acc" | "accuracy") => Some(1),
                Some("combo") => Some(2),
                Some("v2" | "scorev2") => Some(3),
                _ => None,
            };
            match kind {
                Some(kind) if !m.in_progress => {
                    m.win_condition = kind;
                    changed = true;
                    lines.push("已更改勝利條件。".into());
                }
                _ => lines.push("用法：!mp condition <score|acc|combo|v2>".into()),
            }
        }
        "invite" | "inv" => {
            let found: Option<(i64, String)> = match parts.get(2) {
                Some(name) => sqlx::query("SELECT id,username FROM users WHERE lower(username)=lower($1) AND NOT is_restricted AND NOT is_bot")
                    .bind(*name)
                    .fetch_optional(&mut *db)
                    .await?
                    .map(|r| (r.get("id"), r.get("username"))),
                None => None,
            };
            match found {
                None => lines.push("找不到這位玩家。".into()),
                Some((id, name)) => {
                    let invite = ChatMessage {
                        sender: user.username.clone(),
                        text: format!("Join my room: [osump://{}/ {}]", m.id, m.name),
                        recipient: name.clone(),
                        sender_id: id32(user.id)?,
                    };
                    queue(db, id, &wire::encode_packet(88, &invite.encode()?)?).await?;
                    lines.push(format!("已邀請 {name}。"));
                }
            }
        }
        "addref" | "rmref" | "listref" => {
            if sub == "listref" {
                let refs: Vec<String> = sqlx::query_scalar("SELECT u.username FROM stable_room_referees r JOIN users u ON u.id=r.user_id WHERE r.room_id=$1 ORDER BY u.username")
                    .bind(room_id)
                    .fetch_all(&mut *db)
                    .await?;
                lines.push(if refs.is_empty() {
                    "目前沒有裁判。".to_string()
                } else {
                    format!("裁判：{}", refs.join("、"))
                });
            } else {
                let target: Option<(i64, String)> = match parts.get(2) {
                    Some(name) => sqlx::query("SELECT id,username FROM users WHERE lower(username)=lower($1) AND NOT is_restricted AND NOT is_bot")
                        .bind(*name)
                        .fetch_optional(&mut *db)
                        .await?
                        .map(|r| (r.get("id"), r.get("username"))),
                    None => None,
                };
                match target {
                    None => lines.push("找不到這位玩家。".into()),
                    Some((id, name)) if sub == "addref" => {
                        sqlx::query("INSERT INTO stable_room_referees(room_id,user_id) VALUES($1,$2) ON CONFLICT DO NOTHING")
                            .bind(room_id)
                            .bind(id)
                            .execute(&mut *db)
                            .await?;
                        lines.push(format!("{name} 現在是裁判。"));
                    }
                    Some((id, name)) => {
                        sqlx::query(
                            "DELETE FROM stable_room_referees WHERE room_id=$1 AND user_id=$2",
                        )
                        .bind(room_id)
                        .bind(id)
                        .execute(&mut *db)
                        .await?;
                        lines.push(format!("{name} 不再是裁判。"));
                    }
                }
            }
        }
        "timer" => match parts.get(2).and_then(|s| s.parse::<u32>().ok()) {
            Some(seconds) if (1..=300).contains(&seconds) => {
                sqlx::query("INSERT INTO stable_room_timers(room_id,due_at,kind) VALUES($1,now()+make_interval(secs=>$2),'notice') ON CONFLICT(room_id) DO UPDATE SET due_at=EXCLUDED.due_at,kind=EXCLUDED.kind")
                    .bind(room_id)
                    .bind(f64::from(seconds))
                    .execute(&mut *db)
                    .await?;
                lines.push(format!("倒數 {seconds} 秒開始。"));
            }
            _ => lines.push("用法：!mp timer <1-300 秒>".into()),
        },
        "aborttimer" | "at" => {
            let removed = sqlx::query("DELETE FROM stable_room_timers WHERE room_id=$1")
                .bind(room_id)
                .execute(&mut *db)
                .await?
                .rows_affected();
            lines.push(
                if removed > 0 {
                    "已取消倒數。"
                } else {
                    "目前沒有倒數。"
                }
                .into(),
            );
        }
        "close" => {
            say(db, room_id, &["房間即將解散。".into()]).await?;
            for (_, id, _) in &players {
                leave(db, *id).await?;
            }
            return Ok(());
        }
        _ => lines.push("不認識的指令，輸入 !mp help 查看列表。".into()),
    }
    let _ = slot;
    if changed {
        save(db, &m).await?;
        broadcast(db, &m).await?;
    }
    say(db, room_id, &lines).await
}

fn rest_text(parts: &[&str], from: usize) -> String {
    parts.get(from..).map(|p| p.join(" ")).unwrap_or_default()
}

/// Start the match like the host's start button. Outer error is a database failure, inner a
/// reason to tell the room.
pub(super) async fn start_match(
    db: &mut PgConnection,
    m: &mut room::Match,
    force: bool,
) -> Result<Result<(), String>, ApiError> {
    if m.in_progress {
        return Ok(Err("遊戲已經在進行中。".into()));
    }
    if m.map_md5.is_empty() {
        return Ok(Err("請先選擇圖譜。".into()));
    }
    if !force && m.statuses.contains(&room::NOT_READY) {
        return Ok(Err(
            "還有玩家沒有準備好。（用 !mp start force 強制開始）".into()
        ));
    }
    let mut participants = 0;
    for status in &mut m.statuses {
        if matches!(*status, room::READY | room::NOT_READY) {
            *status = room::PLAYING;
            participants += 1;
        }
    }
    if participants == 0 {
        return Ok(Err("沒有可以開始的玩家。".into()));
    }
    m.in_progress = true;
    sqlx::query(
        "UPDATE stable_room_members SET loaded=false,skipped=false,failed=false WHERE room_id=$1",
    )
    .bind(m.id as i16)
    .execute(&mut *db)
    .await?;
    send_room(
        db,
        m.id as i16,
        &wire::encode_packet(START, &m.encode(true)?)?,
        None,
    )
    .await?;
    Ok(Ok(()))
}

/// Fire due room timers. A "start" timer starts the match; a "notice" timer only announces.
pub(crate) async fn run_timers(state: &AppState) -> Result<(), ApiError> {
    let mut tx = state.pool.begin().await?;
    let due =
        sqlx::query("DELETE FROM stable_room_timers WHERE due_at<=now() RETURNING room_id,kind")
            .fetch_all(&mut *tx)
            .await?;
    for row in due {
        let room_id: i16 = row.get("room_id");
        let kind: String = row.get("kind");
        sqlx::query("SELECT pg_advisory_xact_lock(512918376)")
            .execute(&mut *tx)
            .await?;
        let state_json: Option<serde_json::Value> =
            sqlx::query_scalar("SELECT state FROM stable_rooms WHERE id=$1 FOR UPDATE")
                .bind(room_id)
                .fetch_optional(&mut *tx)
                .await?;
        let Some(value) = state_json else { continue };
        let Ok(mut m) = serde_json::from_value::<room::Match>(value) else {
            continue;
        };
        if kind == "start" {
            match start_match(&mut tx, &mut m, true).await? {
                Ok(()) => {
                    save(&mut tx, &m).await?;
                    broadcast(&mut tx, &m).await?;
                    say(&mut tx, room_id, &["倒數結束，遊戲開始！".into()]).await?;
                }
                Err(reason) => say(&mut tx, room_id, &[reason]).await?,
            }
        } else {
            say(&mut tx, room_id, &["倒數結束！".into()]).await?;
        }
    }
    tx.commit().await?;
    Ok(())
}
