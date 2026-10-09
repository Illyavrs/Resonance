//! Real stable packet requests and shared PM writers on isolated PostgreSQL.
//! Shared-row website fixtures exercise the production bridge, not browser UI.
use crate::{common, stable};
use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
use axum::{Router, routing::post};
use futures_util::FutureExt;
use resonance_protocol::stable as wire;
use serde_json::json;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{collections::HashSet, net::SocketAddr, str::FromStr, sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

struct Database {
    control: PgPool,
    pool: PgPool,
    schema: String,
}
impl Database {
    async fn new(legacy: bool) -> Self {
        let url = std::env::var("ILLYA_TEST_DATABASE_URL").expect("disposable PostgreSQL URL");
        let parsed = reqwest::Url::parse(&url).unwrap();
        assert!(matches!(
            parsed.host_str(),
            Some("127.0.0.1" | "localhost" | "[::1]")
        ));
        let control = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .unwrap();
        let schema = format!("stable_chat_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&control)
            .await
            .unwrap();
        let options = PgConnectOptions::from_str(&url)
            .unwrap()
            .application_name(&schema)
            .options([("search_path", schema.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(12)
            .connect_with(options)
            .await
            .unwrap();
        if legacy {
            let migrator = sqlx::migrate!("../../migrations");
            for migration in migrator.iter().filter(|migration| migration.version < 23) {
                sqlx::raw_sql(&migration.sql).execute(&pool).await.unwrap();
            }
        } else {
            sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
        }
        Self {
            control,
            pool,
            schema,
        }
    }
    async fn server(&self) -> Server {
        let state = common::AppState {
            pool: self.pool.clone(),
            storage: Arc::new(std::env::temp_dir()),
            sanctuary_url: Arc::new("http://127.0.0.1:1".into()),
            service_key: Arc::new(String::new()),
            http: reqwest::Client::new(),
            limits: Default::default(),
            trusted_proxies: Default::default(),
            signalr: Arc::new(crate::signalr::Runtime::new()),
            lazer_chat: Arc::new(crate::lazer_chat::Runtime::new()),
        };
        let router = Router::new()
            .route("/", post(stable::bancho))
            .merge(crate::lazer_chat::router())
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
        });
        Server {
            url,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
            stop,
            task,
        }
    }
    async fn wait_lock(&self, query: &str) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let count:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE $2")
                    .bind(&self.schema).bind(query).fetch_one(&self.control).await.unwrap();
                if count > 0 { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("request must reach real database lock barrier");
    }
    async fn cleanup(self) {
        self.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.control)
            .await
            .unwrap();
        self.control.close().await;
    }
}
struct Server {
    url: String,
    http: reqwest::Client,
    stop: oneshot::Sender<()>,
    task: JoinHandle<()>,
}
impl Server {
    async fn login(&self, username: &str) -> String {
        let response = self
            .http
            .post(format!("{}/", self.url))
            .body(format!(
                "{username}\n{:x}\nb20261007|8|0|fixture:hashes:|0\n",
                md5::compute("Chat-Fixture-Password")
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let token = response.headers()["cho-token"].to_str().unwrap().to_owned();
        assert_ne!(token, "no");
        assert!(
            !wire::decode_packets(&response.bytes().await.unwrap())
                .unwrap()
                .iter()
                .any(|packet| packet.id == wire::server::SEND_MESSAGE)
        );
        token
    }
    async fn poll(&self, token: &str, body: Vec<u8>) -> reqwest::Response {
        self.http
            .post(format!("{}/", self.url))
            .header("osu-token", token)
            .body(body)
            .send()
            .await
            .unwrap()
    }
    fn spawn_poll(&self, token: String, body: Vec<u8>) -> JoinHandle<reqwest::Response> {
        let http = self.http.clone();
        let url = self.url.clone();
        tokio::spawn(async move {
            http.post(format!("{url}/"))
                .header("osu-token", token)
                .body(body)
                .send()
                .await
                .unwrap()
        })
    }
    async fn cleanup(self) {
        let _ = self.stop.send(());
        tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .unwrap()
            .unwrap();
    }
}
async fn user(pool: &PgPool, name: &str) -> i64 {
    let salt = SaltString::encode_b64(uuid::Uuid::new_v4().as_bytes()).unwrap();
    let password = "Chat-Fixture-Password";
    let hash = |value: &str| {
        Argon2::default()
            .hash_password(value.as_bytes(), &salt)
            .unwrap()
            .to_string()
    };
    let id=sqlx::query_scalar("INSERT INTO users(username,email,password_hash,password_md5_hash) VALUES($1,$2,$3,$4) RETURNING id")
        .bind(name).bind(format!("{}@invalid.test",uuid::Uuid::new_v4().simple())).bind(hash(password)).bind(hash(&format!("{:x}",md5::compute(password)))).fetch_one(pool).await.unwrap();
    sqlx::query(
        "INSERT INTO user_roles(user_id,role_id) SELECT $1,id FROM roles WHERE name='player'",
    )
    .bind(id)
    .execute(pool)
    .await
    .unwrap();
    id
}
async fn shared_pm(
    pool: &PgPool,
    sender: i64,
    recipient: i64,
    text: &str,
    uuid: Option<&str>,
) -> i64 {
    sqlx::query_scalar("INSERT INTO protocol_private_messages(sender_id,recipient_id,content,client_uuid) VALUES($1,$2,$3,$4) RETURNING id")
        .bind(sender).bind(recipient).bind(text).bind(uuid).fetch_one(pool).await.unwrap()
}
async fn state(pool: &PgPool, id: i64) -> String {
    sqlx::query_scalar("SELECT stable_delivery_state FROM protocol_private_messages WHERE id=$1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}
async fn pending(pool: &PgPool, recipient: i64) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM protocol_private_messages WHERE recipient_id=$1 AND stable_delivery_state='pending'").bind(recipient).fetch_one(pool).await.unwrap()
}
fn pm(target: &str, text: &str) -> Vec<u8> {
    wire::encode_packet(
        wire::client::SEND_PRIVATE_MESSAGE,
        &wire::ChatMessage {
            sender: "ForgedSender".into(),
            text: text.into(),
            recipient: target.into(),
            sender_id: 999,
        }
        .encode()
        .unwrap(),
    )
    .unwrap()
}
async fn packets(response: reqwest::Response) -> Vec<(u16, Vec<u8>)> {
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body = response.bytes().await.unwrap();
    // Check the frame header independently from the production decoder.
    let mut offset = 0;
    let mut result = Vec::new();
    while offset < body.len() {
        let header = &body[offset..offset + 7];
        assert_eq!(header[2], 0);
        let id = u16::from_le_bytes(header[..2].try_into().unwrap());
        let size = u32::from_le_bytes(header[3..7].try_into().unwrap()) as usize;
        offset += 7;
        let end = offset + size;
        result.push((id, body[offset..end].to_vec()));
        offset = end;
    }
    assert_eq!(offset, body.len());
    result
}
async fn messages(response: reqwest::Response) -> Vec<wire::ChatMessage> {
    packets(response)
        .await
        .into_iter()
        .filter(|(id, _)| *id == wire::server::SEND_MESSAGE)
        .map(|(_, body)| wire::ChatMessage::decode(&body).unwrap())
        .collect()
}
fn has_packet(packets: &[(u16, Vec<u8>)], id: u16) -> bool {
    packets.iter().any(|(packet, _)| *packet == id)
}
async fn bearer(pool: &PgPool, user: i64) -> String {
    let token = common::secret();
    sqlx::query("INSERT INTO lazer_sessions(id,user_id,access_hash,refresh_hash,access_expires_at,refresh_expires_at) VALUES($1,$2,$3,$4,now()+interval '1 hour',now()+interval '2 hours')")
        .bind(uuid::Uuid::new_v4()).bind(user).bind(common::token_hash(&token)).bind(common::token_hash(&common::secret())).execute(pool).await.unwrap();
    token
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL; isolated schema, real TCP stable requests"]
async fn stable_pm_cutover_preserves_active_tail_without_replaying_unknown_archive() {
    let db = Database::new(true).await;
    let result=std::panic::AssertUnwindSafe(async {
        let sender=user(&db.pool,"CutoverSender").await;
        let active=user(&db.pool,"CutoverActive").await;
        let offline=user(&db.pool,"CutoverOffline").await;
        let consumed=shared_pm(&db.pool,sender,active,"previous response",None).await;
        let tail=shared_pm(&db.pool,sender,active,"old pending tail",None).await;
        let archive=shared_pm(&db.pool,sender,offline,"unknown past archive",None).await;
        sqlx::query("INSERT INTO bancho_sessions(id,token_hash,user_id,expires_at,last_dm_id) VALUES($1,$2,$3,now()+interval '1 hour',$4)")
            .bind(uuid::Uuid::new_v4()).bind(common::token_hash(&common::secret())).bind(active).bind(consumed).execute(&db.pool).await.unwrap();
        let mut tx=db.pool.begin().await.unwrap();
        sqlx::raw_sql(include_str!("../../../migrations/0023_stable_chat_delivery.sql")).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        assert_eq!(state(&db.pool,consumed).await,"legacy");
        assert_eq!(state(&db.pool,archive).await,"legacy");
        assert_eq!(state(&db.pool,tail).await,"pending");
        let future=shared_pm(&db.pool,sender,offline,"future offline",Some("website:cutover")).await;
        assert_eq!(state(&db.pool,future).await,"pending");
        let mirrored:i64=sqlx::query_scalar("SELECT count(*) FROM chat_messages WHERE private_message_id=ANY($1)")
            .bind(vec![consumed,tail,archive,future]).fetch_one(&db.pool).await.unwrap();
        assert_eq!(mirrored,4,"shared private history remains intact");
    }).catch_unwind().await;
    db.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL; isolated schema, real TCP stable requests"]
async fn stable_pm_cross_client_offline_privacy_and_uuid_retry() {
    let db = Database::new(false).await;
    let server = db.server().await;
    let result=std::panic::AssertUnwindSafe(async {
        let alice=user(&db.pool,"Stable Alice").await;
        let bob=user(&db.pool,"Website Bob").await;
        let other=user(&db.pool,"OtherPlayer").await;
        sqlx::query("INSERT INTO sessions(id,user_id,expires_at) VALUES($1,$2,now()+interval '1 hour')")
            .bind(uuid::Uuid::new_v4()).bind(bob).execute(&db.pool).await.unwrap();
        let alice_token=server.login("Stable Alice").await;
        let result=packets(server.poll(&alice_token,pm("wEbSiTe BoB","stable to website-only player")).await).await;
        assert!(!has_packet(&result,wire::server::USER_DM_BLOCKED));
        assert!(!has_packet(&result,wire::server::NOTIFICATION));
        let stored:(i64,i64,String)=sqlx::query_as("SELECT sender_id,recipient_id,content FROM protocol_private_messages WHERE content='stable to website-only player'").fetch_one(&db.pool).await.unwrap();
        assert_eq!(stored,(alice,bob,"stable to website-only player".into()));
        assert_eq!(pending(&db.pool,bob).await,1);
        let mirrored:(i64,i64,i64)=sqlx::query_as("SELECT m.user_id,c.dm_user_low,c.dm_user_high FROM chat_messages m JOIN channels c ON c.id=m.channel_id WHERE m.content='stable to website-only player'").fetch_one(&db.pool).await.unwrap();
        assert_eq!(mirrored,(alice,alice.min(bob),alice.max(bob)));
        let bob_token=server.login("Website Bob").await;
        let delivered=messages(server.poll(&bob_token,Vec::new()).await).await;
        assert_eq!(delivered.len(),1); assert_eq!(delivered[0].sender,"Stable Alice"); assert_eq!(delivered[0].recipient,"Website Bob"); assert_eq!(i64::from(delivered[0].sender_id),alice);
        assert!(messages(server.poll(&bob_token,Vec::new()).await).await.is_empty());

        // A website writer inserts this exact shared PM row. Its production
        // bridge queues it even while the recipient has no stable session.
        packets(server.poll(&alice_token,wire::encode_packet(wire::client::LOGOUT,&[]).unwrap()).await).await;
        let website=shared_pm(&db.pool,bob,alice,"website offline fixture",Some("website:offline")).await;
        let foreign=shared_pm(&db.pool,bob,other,"foreign private fixture",Some("website:foreign")).await;
        let alice_token=server.login("Stable Alice").await;
        let received=messages(server.poll(&alice_token,Vec::new()).await).await;
        assert_eq!(received.iter().map(|m|m.text.as_str()).collect::<Vec<_>>(),vec!["website offline fixture"]);
        assert_eq!(state(&db.pool,website).await,"delivered"); assert_eq!(state(&db.pool,foreign).await,"pending");

        let bearer=bearer(&db.pool,bob).await;
        for _ in 0..2 {
            let response=server.http.post(format!("{}/api/v2/chat/new",server.url)).bearer_auth(&bearer)
                .json(&json!({"target_id":alice,"message":"native uuid action","is_action":true,"uuid":"native:retry"})).send().await.unwrap();
            assert_eq!(response.status(),reqwest::StatusCode::OK);
        }
        let count:i64=sqlx::query_scalar("SELECT count(*) FROM protocol_private_messages WHERE sender_id=$1 AND client_uuid='native:retry'").bind(bob).fetch_one(&db.pool).await.unwrap(); assert_eq!(count,1);
        let received=messages(server.poll(&alice_token,Vec::new()).await).await;
        assert_eq!(received.len(),1); assert_eq!(received[0].text,"\u{1}ACTION native uuid action\u{1}");
        let response=server.http.post(format!("{}/api/v2/chat/new",server.url)).bearer_auth(&bearer)
            .json(&json!({"target_id":alice,"message":"native uuid action","is_action":true,"uuid":"native:retry"})).send().await.unwrap();
        assert_eq!(response.status(),reqwest::StatusCode::OK);
        assert!(messages(server.poll(&alice_token,Vec::new()).await).await.is_empty());

        // The new setting rejects a new send; it does not retract accepted PMs.
        let accepted=shared_pm(&db.pool,bob,alice,"accepted before preference",Some("website:accepted")).await;
        sqlx::query("UPDATE users SET preferences=preferences||'{\"pm_friends_only\":true}'::jsonb WHERE id=$1").bind(alice).execute(&db.pool).await.unwrap();
        let response=packets(server.poll(&bob_token,pm("sTaBlE aLiCe","refused new send")).await).await;
        let blocked=response.iter().find(|(id,_)|*id==wire::server::USER_DM_BLOCKED).unwrap();
        assert_eq!(wire::ChatMessage::decode(&blocked.1).unwrap().recipient,"Stable Alice");
        assert_eq!(messages(server.poll(&alice_token,Vec::new()).await).await[0].text,"accepted before preference");
        assert_eq!(state(&db.pool,accepted).await,"delivered");
        sqlx::query("INSERT INTO user_relationships(user_id,target_id,relation_type) VALUES($1,$2,'friend')").bind(alice).bind(bob).execute(&db.pool).await.unwrap();
        assert!(!has_packet(&packets(server.poll(&bob_token,pm("Stable Alice","friend admitted")).await).await,wire::server::USER_DM_BLOCKED));
        assert_eq!(messages(server.poll(&alice_token,Vec::new()).await).await[0].text,"friend admitted");

        for (first,second) in [(alice,bob),(bob,alice)] {
            sqlx::query("INSERT INTO user_relationships(user_id,target_id,relation_type) VALUES($1,$2,'block') ON CONFLICT(user_id,target_id) DO UPDATE SET relation_type='block'").bind(first).bind(second).execute(&db.pool).await.unwrap();
            let before:i64=sqlx::query_scalar("SELECT count(*) FROM protocol_private_messages").fetch_one(&db.pool).await.unwrap();
            assert!(has_packet(&packets(server.poll(&bob_token,pm("Stable Alice","blocked new send")).await).await,wire::server::USER_DM_BLOCKED));
            let after:i64=sqlx::query_scalar("SELECT count(*) FROM protocol_private_messages").fetch_one(&db.pool).await.unwrap(); assert_eq!(before,after);
            sqlx::query("DELETE FROM user_relationships WHERE user_id=$1 AND target_id=$2").bind(first).bind(second).execute(&db.pool).await.unwrap();
        }
        sqlx::query("UPDATE users SET is_restricted=true WHERE id=$1").bind(alice).execute(&db.pool).await.unwrap();
        let response=packets(server.poll(&bob_token,pm("Stable Alice","restricted recipient")).await).await;
        assert!(has_packet(&response,wire::server::NOTIFICATION));
        sqlx::query("UPDATE users SET is_restricted=false,preferences='{}' WHERE id=$1").bind(alice).execute(&db.pool).await.unwrap();
        let accepted=shared_pm(&db.pool,bob,alice,"accepted before native toggle",Some("website:toggle")).await;
        packets(server.poll(&alice_token,wire::int_packet(wire::client::TOGGLE_BLOCK_NON_FRIEND_DMS,1).unwrap()).await).await;
        assert_eq!(state(&db.pool,accepted).await,"delivered");
        assert!(has_packet(&packets(server.poll(&bob_token,pm("Stable Alice","native friends-only refusal")).await).await,wire::server::USER_DM_BLOCKED));
        packets(server.poll(&alice_token,wire::int_packet(wire::client::TOGGLE_BLOCK_NON_FRIEND_DMS,0).unwrap()).await).await;
        sqlx::query("DELETE FROM user_roles WHERE user_id=$1").bind(bob).execute(&db.pool).await.unwrap();
        assert_eq!(server.poll(&bob_token,pm("Stable Alice","silenced by permission")).await.status(),reqwest::StatusCode::FORBIDDEN);
        shared_pm(&db.pool,alice,bob,"reading needs no send permission",Some("website:read")).await;
        assert_eq!(messages(server.poll(&bob_token,Vec::new()).await).await[0].text,"reading needs no send permission");
        assert_eq!(server.poll(&alice_token,vec![0]).await.status(),reqwest::StatusCode::BAD_REQUEST);
        let burst=(0..61).flat_map(|_|pm("OtherPlayer","oversized batch")).collect();
        assert_eq!(server.poll(&alice_token,burst).await.status(),reqwest::StatusCode::TOO_MANY_REQUESTS);
    }).catch_unwind().await;
    server.cleanup().await;
    db.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL; isolated schema, real TCP stable requests"]
async fn stable_pm_name_hint_does_not_redirect_and_expired_logout_rolls_back_send() {
    let db = Database::new(false).await;
    let server = db.server().await;
    let result = std::panic::AssertUnwindSafe(async {
        let actor=user(&db.pool,"RenameActor").await;
        let original=user(&db.pool,"OriginalTarget").await;
        let peer=user(&db.pool,"IncomingPeer").await;
        let token=server.login("RenameActor").await;
        shared_pm(&db.pool,peer,actor,"incoming sender is already prelocked",None).await;
        let mut blocker=db.pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM users WHERE id=ANY($1) ORDER BY id FOR UPDATE").bind(vec![actor,original,peer]).execute(&mut *blocker).await.unwrap();
        let request=server.spawn_poll(token.clone(),pm("OriginalTarget","must not redirect"));
        db.wait_lock("%FROM users%FOR UPDATE%").await;
        sqlx::query("UPDATE users SET username='RenamedTarget' WHERE id=$1").bind(original).execute(&mut *blocker).await.unwrap();
        sqlx::query("UPDATE users SET username='OriginalTarget' WHERE id=$1").bind(peer).execute(&mut *blocker).await.unwrap();
        blocker.commit().await.unwrap();
        let response=packets(request.await.unwrap()).await;
        assert!(has_packet(&response,wire::server::NOTIFICATION));
        let count:i64=sqlx::query_scalar("SELECT count(*) FROM protocol_private_messages WHERE content='must not redirect'").fetch_one(&db.pool).await.unwrap();
        assert_eq!(count,0,"an already prelocked peer cannot take over the resolved target name");

        packets(server.poll(&token,pm("RenamedTarget","create original channel")).await).await;
        let channel:i64=sqlx::query_scalar("SELECT id FROM channels WHERE kind='PM' AND dm_user_low=$1 AND dm_user_high=$2")
            .bind(actor.min(original)).bind(actor.max(original)).fetch_one(&db.pool).await.unwrap();
        sqlx::query("UPDATE bancho_sessions SET expires_at=clock_timestamp()+interval '1 second' WHERE user_id=$1").bind(actor).execute(&db.pool).await.unwrap();
        let mut blocker=db.pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM channels WHERE id=$1 FOR UPDATE").bind(channel).execute(&mut *blocker).await.unwrap();
        let mut body=pm("RenamedTarget","must roll back expired logout");
        body.extend(wire::encode_packet(wire::client::LOGOUT,&[]).unwrap());
        let request=server.spawn_poll(token.clone(),body);
        db.wait_lock("%INSERT INTO protocol_private_messages%").await;
        tokio::time::timeout(Duration::from_secs(5),async {loop {
            let expired:bool=sqlx::query_scalar("SELECT expires_at<=clock_timestamp() FROM bancho_sessions WHERE user_id=$1")
                .bind(actor).fetch_one(&db.pool).await.unwrap();
            if expired {break;} tokio::time::sleep(Duration::from_millis(10)).await;
        }}).await.unwrap();
        blocker.commit().await.unwrap();
        assert!(has_packet(&packets(request.await.unwrap()).await,wire::server::RESTART));
        let count:i64=sqlx::query_scalar("SELECT count(*) FROM protocol_private_messages WHERE content='must roll back expired logout'").fetch_one(&db.pool).await.unwrap();
        assert_eq!(count,0);
        let retained:i64=sqlx::query_scalar("SELECT count(*) FROM bancho_sessions WHERE user_id=$1 AND token_hash=$2")
            .bind(actor).bind(common::token_hash(&token)).fetch_one(&db.pool).await.unwrap();
        assert_eq!(retained,1,"failed expiry check rolls back session deletion as well");
    }).catch_unwind().await;
    server.cleanup().await;
    db.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL; isolated schema, real TCP stable requests"]
async fn stable_pm_bounded_concurrent_drains_and_lower_id_late_commit() {
    let db = Database::new(false).await;
    let server = db.server().await;
    let result=std::panic::AssertUnwindSafe(async {
        let sender=user(&db.pool,"QueueSender").await; let blocked=user(&db.pool,"QueueBlocked").await; let receiver=user(&db.pool,"QueueReceiver").await;
        let token=server.login("QueueReceiver").await;
        sqlx::query("INSERT INTO protocol_private_messages(sender_id,recipient_id,content) SELECT $1,$2,'blocked-'||n FROM generate_series(1,150) n").bind(blocked).bind(receiver).execute(&db.pool).await.unwrap();
        sqlx::query("INSERT INTO user_relationships(user_id,target_id,relation_type) VALUES($1,$2,'block')").bind(receiver).bind(blocked).execute(&db.pool).await.unwrap();
        sqlx::query("INSERT INTO protocol_private_messages(sender_id,recipient_id,content) SELECT $1,$2,'allowed-'||n FROM generate_series(1,205) n").bind(sender).bind(receiver).execute(&db.pool).await.unwrap();
        let first=messages(server.poll(&token,Vec::new()).await).await; assert_eq!(first.len(),100); assert_eq!(first[0].text,"allowed-1"); assert_eq!(first[99].text,"allowed-100");
        let a=server.spawn_poll(token.clone(),Vec::new()); let b=server.spawn_poll(token.clone(),Vec::new());
        let mut all=first; all.extend(messages(a.await.unwrap()).await); all.extend(messages(b.await.unwrap()).await);
        // Concurrent requests may freeze the same prelock candidate set. The
        // loser can return empty; a later poll safely snapshots the remainder.
        all.extend(messages(server.poll(&token,Vec::new()).await).await);
        assert_eq!(all.len(),205); let unique=all.into_iter().map(|m|m.text).collect::<HashSet<_>>(); assert_eq!(unique.len(),205);
        assert_eq!(pending(&db.pool,receiver).await,150,"blocked rows remain pending without starving later allowed rows");
        sqlx::query("DELETE FROM user_relationships WHERE user_id=$1 AND target_id=$2").bind(receiver).bind(blocked).execute(&db.pool).await.unwrap();
        assert_eq!(messages(server.poll(&token,Vec::new()).await).await.len(),100);
        assert_eq!(messages(server.poll(&token,Vec::new()).await).await.len(),50);
        assert_eq!(pending(&db.pool,receiver).await,0);
        let mut late=db.pool.begin().await.unwrap();
        let lower:i64=sqlx::query_scalar("SELECT nextval(pg_get_serial_sequence('protocol_private_messages','id'))").fetch_one(&mut *late).await.unwrap();
        let higher=shared_pm(&db.pool,sender,receiver,"newer committed first",Some("website:higher")).await; assert!(lower<higher);
        assert_eq!(messages(server.poll(&token,Vec::new()).await).await[0].text,"newer committed first");
        sqlx::query("INSERT INTO protocol_private_messages(id,sender_id,recipient_id,content) VALUES($1,$2,$3,'lower ID late commit')").bind(lower).bind(sender).bind(receiver).execute(&mut *late).await.unwrap();
        late.commit().await.unwrap();
        assert_eq!(messages(server.poll(&token,Vec::new()).await).await[0].text,"lower ID late commit");
        assert_eq!(state(&db.pool,lower).await,"delivered");
        let sender_token=server.login("QueueSender").await;
        let a=server.spawn_poll(sender_token.clone(),pm("QueueReceiver","opposite-a"));
        let b=server.spawn_poll(token.clone(),pm("QueueSender","opposite-b"));
        let mut delivered=messages(a.await.unwrap()).await; delivered.extend(messages(b.await.unwrap()).await);
        delivered.extend(messages(server.poll(&sender_token,Vec::new()).await).await); delivered.extend(messages(server.poll(&token,Vec::new()).await).await);
        assert_eq!(delivered.iter().filter(|m|m.text.starts_with("opposite-")).count(),2,"opposite sends must complete without deadlock or duplicates");
    }).catch_unwind().await;
    server.cleanup().await;
    db.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL; isolated schema, real TCP stable requests"]
async fn stable_pm_lock_wait_security_and_failed_response_preserve_pending() {
    let db = Database::new(false).await;
    let server = db.server().await;
    let result=std::panic::AssertUnwindSafe(async {
        let sender=user(&db.pool,"SecuritySender").await; let receiver=user(&db.pool,"SecurityReceiver").await;
        let mut token=server.login("SecurityReceiver").await;
        let queued=shared_pm(&db.pool,sender,receiver,"queued through expiry",Some("website:expiry")).await;
        let mut blocker=db.pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM users WHERE id=$1 FOR UPDATE").bind(receiver).execute(&mut *blocker).await.unwrap();
        let request=server.spawn_poll(token.clone(),Vec::new()); db.wait_lock("%FROM users%FOR UPDATE%").await;
        sqlx::query("UPDATE bancho_sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE user_id=$1").bind(receiver).execute(&mut *blocker).await.unwrap(); blocker.commit().await.unwrap();
        assert!(has_packet(&packets(request.await.unwrap()).await,wire::server::RESTART)); assert_eq!(state(&db.pool,queued).await,"pending");
        token=server.login("SecurityReceiver").await;
        assert_eq!(messages(server.poll(&token,Vec::new()).await).await[0].text,"queued through expiry");

        let queued=shared_pm(&db.pool,sender,receiver,"queued through replacement",Some("website:replacement")).await;
        let mut blocker=db.pool.begin().await.unwrap(); sqlx::query("SELECT id FROM users WHERE id=$1 FOR UPDATE").bind(receiver).execute(&mut *blocker).await.unwrap();
        let request=server.spawn_poll(token.clone(),Vec::new()); db.wait_lock("%FROM users%FOR UPDATE%").await;
        let replacement=common::secret();
        sqlx::query("UPDATE bancho_sessions SET token_hash=$2 WHERE user_id=$1").bind(receiver).bind(common::token_hash(&replacement)).execute(&mut *blocker).await.unwrap(); blocker.commit().await.unwrap();
        assert!(has_packet(&packets(request.await.unwrap()).await,wire::server::RESTART)); assert_eq!(state(&db.pool,queued).await,"pending"); token=replacement;
        assert_eq!(messages(server.poll(&token,Vec::new()).await).await[0].text,"queued through replacement");

        for policy in ["restriction","block"] {
            let queued=shared_pm(&db.pool,sender,receiver,policy,None).await;
            let mut blocker=db.pool.begin().await.unwrap(); sqlx::query("SELECT id FROM users WHERE id=ANY($1) ORDER BY id FOR UPDATE").bind(vec![sender,receiver]).execute(&mut *blocker).await.unwrap();
            let request=server.spawn_poll(token.clone(),Vec::new()); db.wait_lock("%FROM users%FOR UPDATE%").await;
            if policy=="restriction" {sqlx::query("UPDATE users SET is_restricted=true WHERE id=$1").bind(sender).execute(&mut *blocker).await.unwrap();}
            else {sqlx::query("INSERT INTO user_relationships(user_id,target_id,relation_type) VALUES($1,$2,'block')").bind(sender).bind(receiver).execute(&mut *blocker).await.unwrap();}
            blocker.commit().await.unwrap(); assert!(messages(request.await.unwrap()).await.is_empty()); assert_eq!(state(&db.pool,queued).await,"pending");
            sqlx::query("UPDATE users SET is_restricted=false WHERE id=$1").bind(sender).execute(&db.pool).await.unwrap(); sqlx::query("DELETE FROM user_relationships WHERE user_id=$1 AND target_id=$2").bind(sender).bind(receiver).execute(&db.pool).await.unwrap();
            assert_eq!(messages(server.poll(&token,Vec::new()).await).await[0].text,policy);
        }

        let good=shared_pm(&db.pool,sender,receiver,"encoding rollback",None).await;
        sqlx::query("UPDATE users SET username=$2 WHERE id=$1").bind(sender).bind("x".repeat(wire::MAX_STRING_BYTES+1)).execute(&db.pool).await.unwrap();
        let response=server.poll(&token,Vec::new()).await; assert_eq!(response.status(),reqwest::StatusCode::BAD_REQUEST); assert_eq!(state(&db.pool,good).await,"pending");
        sqlx::query("UPDATE users SET username='SecuritySender' WHERE id=$1").bind(sender).execute(&db.pool).await.unwrap(); assert_eq!(messages(server.poll(&token,Vec::new()).await).await[0].text,"encoding rollback");

        // Expire after PM encoding while a later resource is still blocked:
        // the final deadline check must roll back the delivery state mutation.
        let queued=shared_pm(&db.pool,sender,receiver,"expiry after encoding",None).await;
        sqlx::query("UPDATE bancho_sessions SET expires_at=clock_timestamp()+interval '1 second' WHERE user_id=$1").bind(receiver).execute(&db.pool).await.unwrap();
        let mut blocker=db.pool.begin().await.unwrap(); sqlx::query("LOCK TABLE bancho_packet_queue IN ACCESS EXCLUSIVE MODE").execute(&mut *blocker).await.unwrap();
        let request=server.spawn_poll(token.clone(),Vec::new()); db.wait_lock("%FROM bancho_packet_queue%").await;
        tokio::time::timeout(Duration::from_secs(5),async {loop {let expired:bool=sqlx::query_scalar("SELECT expires_at<=clock_timestamp() FROM bancho_sessions WHERE user_id=$1").bind(receiver).fetch_one(&db.pool).await.unwrap(); if expired {break;} tokio::time::sleep(Duration::from_millis(10)).await;}}).await.unwrap();
        blocker.commit().await.unwrap(); assert!(has_packet(&packets(request.await.unwrap()).await,wire::server::RESTART)); assert_eq!(state(&db.pool,queued).await,"pending");
        token=server.login("SecurityReceiver").await; assert_eq!(messages(server.poll(&token,Vec::new()).await).await[0].text,"expiry after encoding");
    }).catch_unwind().await;
    server.cleanup().await;
    db.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
