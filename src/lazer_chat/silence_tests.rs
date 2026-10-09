//! Native REST and notification wire fixtures on a disposable PostgreSQL schema.
use super::*;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::str::FromStr;
use tokio::{sync::oneshot, task::JoinHandle};

struct Fixture {
    admin: PgPool,
    pool: PgPool,
    schema: String,
    state: AppState,
    base: String,
    client: reqwest::Client,
    users: Vec<i64>,
    tokens: Vec<String>,
    stop: oneshot::Sender<()>,
    server: JoinHandle<()>,
}
impl Fixture {
    async fn new() -> Self {
        let url = std::env::var("ILLYA_TEST_DATABASE_URL").expect("disposable PostgreSQL URL");
        assert!(matches!(
            reqwest::Url::parse(&url).unwrap().host_str(),
            Some("127.0.0.1" | "localhost" | "[::1]")
        ));
        let admin = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .unwrap();
        let schema = format!("native_silence_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await
            .unwrap();
        let options = PgConnectOptions::from_str(&url)
            .unwrap()
            .application_name(&schema)
            .options([("search_path", schema.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(18)
            .connect_with(options)
            .await
            .unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let state = AppState {
            pool: pool.clone(),
            storage: Arc::new(std::env::temp_dir()),
            sanctuary_url: Arc::new("http://127.0.0.1:1".into()),
            service_key: Arc::new(String::new()),
            http: reqwest::Client::new(),
            limits: Default::default(),
            trusted_proxies: Default::default(),
            signalr: Arc::new(crate::signalr::Runtime::new()),
            lazer_chat: Arc::new(Runtime::with_endpoint(
                base.replacen("http://", "ws://", 1) + "/notification-server",
            )),
        };
        state.lazer_chat.start(pool.clone()).await.unwrap();
        let app = router().with_state(state.clone());
        let (stop, stopped) = oneshot::channel();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        let mut users = Vec::new();
        let mut tokens = Vec::new();
        for n in 0..3 {
            let id:i64=sqlx::query_scalar("INSERT INTO users(username,email,password_hash,password_md5_hash) VALUES($1,$2,'unused','unused') RETURNING id")
                .bind(format!("SilencedLazer{n}")).bind(format!("silence{n}@invalid.test")).fetch_one(&pool).await.unwrap();
            sqlx::query("INSERT INTO user_roles(user_id,role_id) SELECT $1,id FROM roles WHERE name='player'")
                .bind(id).execute(&pool).await.unwrap();
            let token = common::secret();
            sqlx::query("INSERT INTO lazer_sessions(id,user_id,access_hash,refresh_hash,access_expires_at,refresh_expires_at) VALUES($1,$2,$3,$4,now()+interval '1 hour',now()+interval '1 day')")
                .bind(uuid::Uuid::new_v4()).bind(id).bind(common::token_hash(&token)).bind(common::secret()).execute(&pool).await.unwrap();
            initialise(&state, id).await.unwrap();
            users.push(id);
            tokens.push(token);
        }
        Self {
            admin,
            pool,
            schema,
            state,
            base,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
            users,
            tokens,
            stop,
            server,
        }
    }
    async fn general(&self) -> i64 {
        sqlx::query_scalar("SELECT id FROM channels WHERE name='general'")
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }
    async fn silence(&self, user: usize, seconds: i32) {
        sqlx::query("UPDATE users SET chat_silenced_until=clock_timestamp()+make_interval(secs=>$2) WHERE id=$1")
            .bind(self.users[user]).bind(f64::from(seconds)).execute(&self.pool).await.unwrap();
    }
    async fn new_message(
        &self,
        sender: usize,
        target: usize,
        text: &str,
        uuid: &str,
    ) -> reqwest::Response {
        self.client
            .post(format!("{}/api/v2/chat/new", self.base))
            .bearer_auth(&self.tokens[sender])
            .json(&json!({"target_id":self.users[target],"message":text,"uuid":uuid}))
            .send()
            .await
            .unwrap()
    }
    async fn channel(&self, user: usize, id: i64) -> Value {
        let response = self
            .client
            .get(format!("{}/api/v2/chat/channels/{id}", self.base))
            .bearer_auth(&self.tokens[user])
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        response.json().await.unwrap()
    }
    async fn wait_lock(&self, query: &str) {
        tokio::time::timeout(Duration::from_secs(10),async { loop {
            let n:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE $2")
                .bind(&self.schema).bind(query).fetch_one(&self.admin).await.unwrap();
            if n>0 {break;} tokio::time::sleep(Duration::from_millis(10)).await;
        }}).await.expect("request reached expected lock barrier");
    }
    async fn wait_expired(&self, user: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let expired: bool = sqlx::query_scalar(
                    "SELECT chat_silenced_until<=clock_timestamp() FROM users WHERE id=$1",
                )
                .bind(self.users[user])
                .fetch_one(&self.pool)
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
    }
    async fn cleanup(self) {
        let _ = self.stop.send(());
        tokio::time::timeout(Duration::from_secs(10), self.server)
            .await
            .unwrap()
            .unwrap();
        drop(self.state);
        self.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await
            .unwrap();
        self.admin.close().await;
    }
}
async fn denied(response: reqwest::Response) {
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let error: Value = response.json().await.unwrap();
    assert_eq!(error["error"], "chat_silenced");
    assert!(error["remaining_seconds"].as_i64().unwrap() > 0);
    assert!(error["message"].as_str().unwrap().contains("more seconds"));
    assert!(error["silenced_until"].as_str().is_some());
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL pointing at disposable local PostgreSQL"]
async fn lazer_timed_silence_enforces_new_messages_preserves_retry_history_and_access() {
    let f = Fixture::new().await;
    let result=std::panic::AssertUnwindSafe(async {
        let general=f.general().await;
        let public:Value=submit(&f.client,&f.base,&f.tokens[0],general,"accepted public","silence-public").await.json().await.unwrap();
        let pm:Value=f.new_message(0,1,"accepted private","silence-private").await.json().await.unwrap();
        let private=pm["new_channel_id"].as_i64().unwrap();
        let room_channel:i64=sqlx::query_scalar("INSERT INTO channels(name,required_permission) VALUES('silence-room','lazer.room') RETURNING id").fetch_one(&f.pool).await.unwrap();
        let room:i64=sqlx::query_scalar("INSERT INTO lazer_rooms(host_id,channel_id,settings) VALUES($1,$2,'{}') RETURNING id").bind(f.users[0]).bind(room_channel).fetch_one(&f.pool).await.unwrap();
        sqlx::query("INSERT INTO lazer_room_members(room_id,user_id) VALUES($1,$2)").bind(room).bind(f.users[0]).execute(&f.pool).await.unwrap();
        sqlx::query("INSERT INTO lazer_channel_members(user_id,channel_id) VALUES($1,$2)").bind(f.users[0]).bind(room_channel).execute(&f.pool).await.unwrap();
        f.silence(0,60).await;
        for channel in [general,private,room_channel] {
            denied(submit(&f.client,&f.base,&f.tokens[0],channel,"new blocked","new-blocked").await).await;
            let metadata=f.channel(0,channel).await;
            assert_eq!(metadata["channel"]["current_user_attributes"]["can_message"],false);
            assert_eq!(metadata["channel"]["current_user_attributes"]["can_message_error"],"chat_silenced");
        }
        denied(f.new_message(0,2,"new conversation blocked","blocked-new-pm").await).await;
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM chat_messages WHERE user_id=$1").bind(f.users[0]).fetch_one(&f.pool).await.unwrap(),2);
        let retried:Value=submit(&f.client,&f.base,&f.tokens[0],general,"accepted public","silence-public").await.json().await.unwrap();
        assert_eq!(retried,public);
        let retried:Value=f.new_message(0,1,"accepted private","silence-private").await.json().await.unwrap();
        assert_eq!(retried["message"],pm["message"]);
        assert_eq!(submit(&f.client,&f.base,&f.tokens[0],general,"different content","silence-public").await.status(),StatusCode::CONFLICT);
        for (channel,message) in [(general,&public),(private,&pm["message"])] {
            let history:Value=f.client.get(format!("{}/api/v2/chat/channels/{channel}/messages",f.base)).bearer_auth(&f.tokens[0]).send().await.unwrap().json().await.unwrap();
            assert!(history.as_array().unwrap().contains(message),"accepted history is retained");
            assert_eq!(f.client.put(format!("{}/api/v2/chat/channels/{channel}/mark-as-read/{}",f.base,message["message_id"].as_i64().unwrap())).bearer_auth(&f.tokens[0]).send().await.unwrap().status(),StatusCode::NO_CONTENT);
        }
        let read_only=f.client.post(format!("{}/api/v2/chat/channels",f.base)).bearer_auth(&f.tokens[0]).json(&json!({"type":"PM","target_id":f.users[2]})).send().await.unwrap();
        assert_eq!(read_only.status(),StatusCode::OK,"conversation creation for reading is allowed");
        let conversation:Value=read_only.json().await.unwrap();
        assert_eq!(conversation["current_user_attributes"]["can_message"],false);
        assert_eq!(conversation["recent_messages"],json!([]));
        assert_eq!(f.client.put(format!("{}/api/v2/chat/channels/{room_channel}/users/{}",f.base,f.users[0])).bearer_auth(&f.tokens[0]).send().await.unwrap().status(),StatusCode::OK,"room channel joins remain available");
        assert!(common::account(&f.state,f.users[0]).await.unwrap().can("chat.send"),"timed policy does not replace room permissions");
        for path in ["chat/ack","chat/updates?since=0"] {
            let response=if path=="chat/ack" { f.client.post(format!("{}/api/v2/{path}",f.base)).bearer_auth(&f.tokens[0]).send().await.unwrap() }
                else {f.client.get(format!("{}/api/v2/{path}",f.base)).bearer_auth(&f.tokens[0]).send().await.unwrap()};
            let value:Value=response.json().await.unwrap(); assert_eq!(value["silences"],json!([]),"wire silences never purge history");
        }
        // Matching UUIDs bypass only timed silence, never a current block.
        sqlx::query("INSERT INTO user_relationships(user_id,target_id,relation_type) VALUES($1,$2,'block')").bind(f.users[1]).bind(f.users[0]).execute(&f.pool).await.unwrap();
        assert_eq!(f.new_message(0,1,"accepted private","silence-private").await.status(),StatusCode::NOT_FOUND);
        sqlx::query("DELETE FROM user_relationships WHERE user_id=$1 AND target_id=$2").bind(f.users[1]).bind(f.users[0]).execute(&f.pool).await.unwrap();
        sqlx::query("UPDATE users SET chat_silenced_until=NULL WHERE id=$1").bind(f.users[0]).execute(&f.pool).await.unwrap();
        assert_eq!(f.channel(0,general).await["channel"]["current_user_attributes"]["can_message"],true);
        assert_eq!(submit(&f.client,&f.base,&f.tokens[0],general,"after lift","after-lift").await.status(),StatusCode::OK);
    }).catch_unwind().await;
    f.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL pointing at disposable local PostgreSQL"]
async fn lazer_timed_silence_rechecks_after_user_and_membership_lock_waits() {
    let f = Fixture::new().await;
    let result=std::panic::AssertUnwindSafe(async {
        let general=f.general().await;
        let mut blocker=f.pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM users WHERE id=$1 FOR UPDATE").bind(f.users[0]).execute(&mut *blocker).await.unwrap();
        let (client,base,token)=(f.client.clone(),f.base.clone(),f.tokens[0].clone());
        let request=tokio::spawn(async move {submit(&client,&base,&token,general,"blocked after wait","wait-apply").await});
        f.wait_lock("%FROM users%FOR UPDATE%").await;
        sqlx::query("UPDATE users SET chat_silenced_until=clock_timestamp()+interval '60 seconds' WHERE id=$1").bind(f.users[0]).execute(&mut *blocker).await.unwrap();
        blocker.commit().await.unwrap(); denied(request.await.unwrap()).await;
        f.silence(0,1).await;
        let mut blocker=f.pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM users WHERE id=$1 FOR UPDATE").bind(f.users[0]).execute(&mut *blocker).await.unwrap();
        let (client,base,token)=(f.client.clone(),f.base.clone(),f.tokens[0].clone());
        let request=tokio::spawn(async move {submit(&client,&base,&token,general,"expired while waiting","wait-expire").await});
        f.wait_lock("%FROM users%FOR UPDATE%").await; f.wait_expired(0).await;
        blocker.commit().await.unwrap(); assert_eq!(request.await.unwrap().status(),StatusCode::OK);
        f.silence(0,1).await;
        let mut blocker=f.pool.begin().await.unwrap();
        sqlx::query("SELECT user_id FROM lazer_channel_members WHERE user_id=$1 AND channel_id=$2 FOR UPDATE").bind(f.users[0]).bind(general).execute(&mut *blocker).await.unwrap();
        let (client,base,token)=(f.client.clone(),f.base.clone(),f.tokens[0].clone());
        let request=tokio::spawn(async move {submit(&client,&base,&token,general,"expired during membership wait","membership-expire").await});
        f.wait_lock("%FROM lazer_channel_members%FOR SHARE%").await; f.wait_expired(0).await;
        blocker.commit().await.unwrap(); assert_eq!(request.await.unwrap().status(),StatusCode::OK);
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM chat_messages WHERE user_id=$1").bind(f.users[0]).fetch_one(&f.pool).await.unwrap(),2);
        assert_eq!(f.channel(0,general).await["channel"]["current_user_attributes"]["can_message"],true);
    }).catch_unwind().await;
    f.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL pointing at disposable local PostgreSQL"]
async fn lazer_timed_policy_notify_keeps_socket_and_accepted_history() {
    let f = Fixture::new().await;
    let result = std::panic::AssertUnwindSafe(async {
        let general = f.general().await;
        let before: Value = submit(
            &f.client,
            &f.base,
            &f.tokens[1],
            general,
            "accepted before silence",
            "socket-before",
        )
        .await
        .json()
        .await
        .unwrap();
        f.silence(1, 60).await;
        let mut socket = open(&f.base, &f.tokens[0]).await;
        assert_eq!(
            next_message(&mut socket).await["messages"][0],
            before,
            "silenced senders' accepted history remains readable"
        );
        let mut observed = f.state.lazer_chat.events.subscribe();
        f.silence(0, 60).await;
        sqlx::query("SELECT pg_notify('illya_chat',$1)")
            .bind(
                json!({"kind":"chat_policy_changed","schema":f.schema,"user":f.users[0]})
                    .to_string(),
            )
            .execute(&f.pool)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let event = observed.recv().await.unwrap();
                if event["kind"] == "chat_policy_changed" {
                    break;
                }
            }
        })
        .await
        .unwrap();
        let incoming: Value = submit(
            &f.client,
            &f.base,
            &f.tokens[2],
            general,
            "incoming while silenced",
            "socket-incoming",
        )
        .await
        .json()
        .await
        .unwrap();
        assert_eq!(
            next_message(&mut socket).await["messages"][0],
            incoming,
            "policy notification does not close the socket"
        );
        denied(
            submit(
                &f.client,
                &f.base,
                &f.tokens[0],
                general,
                "blocked socket sender",
                "socket-blocked",
            )
            .await,
        )
        .await;
        assert_eq!(
            f.channel(0, general).await["channel"]["current_user_attributes"]["can_message"],
            false
        );
        sqlx::query("UPDATE users SET chat_silenced_until=NULL WHERE id=$1")
            .bind(f.users[0])
            .execute(&f.pool)
            .await
            .unwrap();
        sqlx::query("SELECT pg_notify('illya_chat',$1)")
            .bind(
                json!({"kind":"chat_policy_changed","schema":f.schema,"user":f.users[0]})
                    .to_string(),
            )
            .execute(&f.pool)
            .await
            .unwrap();
        let incoming: Value = submit(
            &f.client,
            &f.base,
            &f.tokens[2],
            general,
            "incoming after lift",
            "socket-lifted",
        )
        .await
        .json()
        .await
        .unwrap();
        assert_eq!(next_message(&mut socket).await["messages"][0], incoming);
        socket.close(None).await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM chat_messages")
                .fetch_one(&f.pool)
                .await
                .unwrap(),
            3
        );
    })
    .catch_unwind()
    .await;
    f.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
