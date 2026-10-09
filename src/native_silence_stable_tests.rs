//! Real binary polling under timed send restrictions; no native client claim.
use super::*;
use chrono::{DateTime, Utc};

fn public(target: &str, text: &str) -> Vec<u8> {
    wire::encode_packet(
        wire::client::SEND_PUBLIC_MESSAGE,
        &wire::ChatMessage {
            sender: "forged".into(),
            text: text.into(),
            recipient: target.into(),
            sender_id: 999,
        }
        .encode()
        .unwrap(),
    )
    .unwrap()
}
fn silence_seconds(packets: &[(u16, Vec<u8>)]) -> Vec<i32> {
    packets
        .iter()
        .filter(|(id, _)| *id == wire::server::SILENCE_END)
        .map(|(_, body)| i32::from_le_bytes(body.as_slice().try_into().unwrap()))
        .collect()
}
fn notices(packets: &[(u16, Vec<u8>)]) -> Vec<String> {
    packets
        .iter()
        .filter(|(id, _)| *id == wire::server::NOTIFICATION)
        .map(|(_, body)| wire::Reader::new(body).string().unwrap().to_owned())
        .collect()
}
async fn silence_for(pool: &PgPool, user: i64, seconds: i32) -> DateTime<Utc> {
    sqlx::query_scalar("UPDATE users SET chat_silenced_until=clock_timestamp()+make_interval(secs=>$2) WHERE id=$1 RETURNING chat_silenced_until")
        .bind(user).bind(f64::from(seconds)).fetch_one(pool).await.unwrap()
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL; isolated schema, real TCP stable requests"]
async fn stable_timed_silence_blocks_all_send_paths_and_preserves_poll_and_incoming() {
    let db = Database::new(false).await;
    let server = db.server().await;
    let result=std::panic::AssertUnwindSafe(async {
        let sender=user(&db.pool,"SilenceStable").await;
        let peer=user(&db.pool,"SilencePeer").await;
        let peer_token=server.login("SilencePeer").await;
        let until=silence_for(&db.pool,sender,60).await;
        let response=server.http.post(format!("{}/",server.url)).body(format!(
            "SilenceStable\n{:x}\nb20261007|8|0|fixture:hashes:|0\n",md5::compute("Chat-Fixture-Password")))
            .send().await.unwrap();
        let token=response.headers()["cho-token"].to_str().unwrap().to_owned();
        assert_ne!(token,"no");
        let login=packets(response).await;
        assert!(silence_seconds(&login)[0]>0);
        assert_eq!(sqlx::query_scalar::<_,Option<DateTime<Utc>>>("SELECT last_chat_silence_until FROM bancho_sessions WHERE user_id=$1").bind(sender).fetch_one(&db.pool).await.unwrap(),Some(until));

        // Silencing leaves gameplay participation available: create a room and
        // enter a live spectator session through their real packet handlers.
        let room=resonance_protocol::multiplayer::Match {
            id:0,in_progress:false,mods:0,name:"silence fixture".into(),password:String::new(),
            map_name:String::new(),map_id:0,map_md5:String::new(),statuses:[1;16],teams:[0;16],
            users:[None;16],host_id:sender as i32,mode:0,win_condition:0,team_type:0,
            freemods:false,slot_mods:[0;16],seed:0,
        };
        let create=packets(server.poll(&token,wire::encode_packet(31,&room.encode(false).unwrap()).unwrap()).await).await;
        assert!(has_packet(&create,36),"room creation remains available");
        let spectate=packets(server.poll(&token,wire::int_packet(16,peer as i32).unwrap()).await).await;
        assert!(has_packet(&spectate,wire::server::CHANNEL_JOIN_SUCCESS));
        let incoming=shared_pm(&db.pool,peer,sender,"previously accepted incoming",Some("silence:incoming")).await;
        silence_for(&db.pool,peer,60).await;
        let mut body=public("#general","blocked public");
        body.extend(public("#multiplayer","blocked multiplayer"));
        body.extend(public("#spectator","blocked spectator"));
        body.extend(pm("SilencePeer","blocked private"));
        body.extend(wire::encode_packet(wire::client::REQUEST_STATUS_UPDATE,&[]).unwrap());
        body.extend(wire::int_packet(wire::client::FRIEND_ADD,peer as i32).unwrap());
        let mixed=packets(server.poll(&token,body).await).await;
        assert_eq!(silence_seconds(&mixed).len(),1,"one blocked-send notice per poll");
        assert_eq!(notices(&mixed).iter().filter(|n| n.starts_with("Chat is silenced")).count(),1);
        assert!(has_packet(&mixed,wire::server::USER_STATS));
        let delivered:Vec<_>=mixed.iter().filter(|(id,_)| *id==wire::server::SEND_MESSAGE)
            .map(|(_,payload)| wire::ChatMessage::decode(payload).unwrap()).collect();
        assert_eq!(delivered.len(),1); assert_eq!(delivered[0].text,"previously accepted incoming");
        assert_eq!(state(&db.pool,incoming).await,"delivered");
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM chat_messages WHERE user_id=$1").bind(sender).fetch_one(&db.pool).await.unwrap(),0);
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM protocol_private_messages WHERE sender_id=$1").bind(sender).fetch_one(&db.pool).await.unwrap(),0);
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM user_relationships WHERE user_id=$1 AND target_id=$2 AND relation_type='friend'").bind(sender).bind(peer).fetch_one(&db.pool).await.unwrap(),1);
        assert!(messages(server.poll(&peer_token,Vec::new()).await).await.is_empty(),"no special-channel messages are queued");
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM stable_room_members WHERE user_id=$1").bind(sender).fetch_one(&db.pool).await.unwrap(),1);
        assert_eq!(sqlx::query_scalar::<_,Option<i64>>("SELECT spectator_host_id FROM bancho_sessions WHERE user_id=$1").bind(sender).fetch_one(&db.pool).await.unwrap(),Some(peer));
    }).catch_unwind().await;
    server.cleanup().await;
    db.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL; isolated schema, real TCP stable requests"]
async fn stable_timed_silence_apply_extend_lift_and_expire_are_committed_once() {
    let db = Database::new(false).await;
    let server = db.server().await;
    let result = std::panic::AssertUnwindSafe(async {
        let sender = user(&db.pool, "SilenceTransitions").await;
        let token = server.login("SilenceTransitions").await;
        let applied = silence_for(&db.pool, sender, 40).await;
        let first = packets(
            server
                .poll(&token, public("#general", "blocked during apply"))
                .await,
        )
        .await;
        assert_eq!(silence_seconds(&first).len(), 1);
        assert_eq!(notices(&first).len(), 1);
        assert!(silence_seconds(&first)[0] > 0);
        assert!(silence_seconds(&packets(server.poll(&token, Vec::new()).await).await).is_empty());
        let extended = silence_for(&db.pool, sender, 80).await;
        assert!(extended > applied);
        let change = packets(server.poll(&token, Vec::new()).await).await;
        assert!(silence_seconds(&change)[0] > 40);
        assert_eq!(notices(&change).len(), 1);
        sqlx::query("UPDATE users SET chat_silenced_until=NULL WHERE id=$1")
            .bind(sender)
            .execute(&db.pool)
            .await
            .unwrap();
        let lifted = packets(server.poll(&token, Vec::new()).await).await;
        assert_eq!(silence_seconds(&lifted), vec![0]);
        assert!(notices(&lifted)[0].contains("has ended"));
        assert!(silence_seconds(&packets(server.poll(&token, Vec::new()).await).await).is_empty());
        silence_for(&db.pool, sender, 1).await;
        let active = packets(server.poll(&token, Vec::new()).await).await;
        assert!(silence_seconds(&active)[0] > 0);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let expired: bool = sqlx::query_scalar(
                    "SELECT chat_silenced_until<=clock_timestamp() FROM users WHERE id=$1",
                )
                .bind(sender)
                .fetch_one(&db.pool)
                .await
                .unwrap();
                if expired {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let expired = packets(
            server
                .poll(&token, public("#general", "allowed after expiry"))
                .await,
        )
        .await;
        assert_eq!(silence_seconds(&expired), vec![0]);
        assert_eq!(
            sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
                "SELECT last_chat_silence_until FROM bancho_sessions WHERE user_id=$1"
            )
            .bind(sender)
            .fetch_one(&db.pool)
            .await
            .unwrap(),
            None
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM chat_messages WHERE user_id=$1")
                .bind(sender)
                .fetch_one(&db.pool)
                .await
                .unwrap(),
            1
        );
        assert!(silence_seconds(&packets(server.poll(&token, Vec::new()).await).await).is_empty());
    })
    .catch_unwind()
    .await;
    server.cleanup().await;
    db.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL; isolated schema, real TCP stable requests"]
async fn stable_timed_silence_rechecks_after_sender_lock_wait() {
    let db = Database::new(false).await;
    let server = db.server().await;
    let result=std::panic::AssertUnwindSafe(async {
        let sender=user(&db.pool,"SilenceLockSender").await;
        let token=server.login("SilenceLockSender").await;
        let mut blocker=db.pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM users WHERE id=$1 FOR UPDATE").bind(sender).execute(&mut *blocker).await.unwrap();
        let request=server.spawn_poll(token.clone(),public("#general","blocked after lock"));
        db.wait_lock("%FROM users%FOR UPDATE%").await;
        sqlx::query("UPDATE users SET chat_silenced_until=clock_timestamp()+interval '40 seconds' WHERE id=$1").bind(sender).execute(&mut *blocker).await.unwrap();
        blocker.commit().await.unwrap();
        assert!(silence_seconds(&packets(request.await.unwrap()).await)[0]>0);
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM chat_messages WHERE user_id=$1").bind(sender).fetch_one(&db.pool).await.unwrap(),0);
        silence_for(&db.pool,sender,1).await;
        let mut blocker=db.pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM users WHERE id=$1 FOR UPDATE").bind(sender).execute(&mut *blocker).await.unwrap();
        let request=server.spawn_poll(token,public("#general","allowed after lock expiry"));
        db.wait_lock("%FROM users%FOR UPDATE%").await;
        tokio::time::timeout(Duration::from_secs(5),async {loop {
            let expired:bool=sqlx::query_scalar("SELECT chat_silenced_until<=clock_timestamp() FROM users WHERE id=$1").bind(sender).fetch_one(&db.pool).await.unwrap();
            if expired { break; } tokio::time::sleep(Duration::from_millis(10)).await;
        }}).await.unwrap();
        blocker.commit().await.unwrap();
        assert_eq!(silence_seconds(&packets(request.await.unwrap()).await),vec![0]);
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM chat_messages WHERE user_id=$1").bind(sender).fetch_one(&db.pool).await.unwrap(),1);
    }).catch_unwind().await;
    server.cleanup().await;
    db.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
