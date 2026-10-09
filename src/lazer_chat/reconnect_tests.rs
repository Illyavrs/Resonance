use super::*;

async fn connect_without_start(base: &str, token: &str) -> Socket {
    let mut request = format!(
        "{}/notification-server",
        base.replacen("http://", "ws://", 1)
    )
    .into_client_request()
    .unwrap();
    request
        .headers_mut()
        .insert("Authorization", format!("Bearer {token}").parse().unwrap());
    tokio_tungstenite::connect_async(request).await.unwrap().0
}

async fn start(socket: &mut Socket) {
    socket
        .send(ClientMessage::Text(r#"{"event":"chat.start"}"#.into()))
        .await
        .unwrap();
}

async fn recovered(socket: &mut Socket, expected: &[i64], expected_channels: &[i64]) {
    let (messages, channels) = tokio::time::timeout(Duration::from_secs(10), async {
        let mut messages = Vec::new();
        let mut channels = HashSet::new();
        while messages.len() < expected.len() || channels.len() < expected_channels.len() {
            let frame = socket
                .next()
                .await
                .expect("recovery socket closed")
                .unwrap();
            if let ClientMessage::Text(text) = frame {
                let value: Value = serde_json::from_str(&text).unwrap();
                match value["event"].as_str().unwrap() {
                    "chat.channel.join" => {
                        assert!(messages.is_empty(), "channel joined after recovery began");
                        assert!(channels.insert(value["data"]["channel_id"].as_i64().unwrap()));
                    }
                    "chat.message.new" => {
                        let message = &value["data"]["messages"][0];
                        assert!(channels.contains(&message["channel_id"].as_i64().unwrap()));
                        assert_eq!(value["data"]["users"][0]["id"], message["sender_id"]);
                        messages.push(message["message_id"].as_i64().unwrap());
                    }
                    event => panic!("unexpected recovery event: {event}"),
                }
            }
        }
        (messages, channels)
    })
    .await
    .expect("recovery timed out");
    assert_eq!(
        messages, expected,
        "recovery must preserve IDs and ascending order"
    );
    assert_eq!(channels, expected_channels.iter().copied().collect());
    quiet_messages(socket).await;
}

async fn closed_without_messages(socket: &mut Socket) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(frame) = socket.next().await {
            match frame {
                Ok(ClientMessage::Text(text)) => {
                    let value: Value = serde_json::from_str(&text).unwrap();
                    assert_ne!(value["event"], "chat.message.new");
                    assert_ne!(value["event"], "chat.channel.join");
                }
                Ok(ClientMessage::Close(_)) | Err(_) => break,
                _ => {}
            }
        }
    })
    .await
    .expect("revoked socket did not close");
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL pointing at disposable local PostgreSQL"]
async fn persisted_reconnect_is_bounded_authorized_and_once_per_socket() {
    let url = std::env::var("ILLYA_TEST_DATABASE_URL").expect("ILLYA_TEST_DATABASE_URL");
    assert!(matches!(
        reqwest::Url::parse(&url).unwrap().host_str(),
        Some("127.0.0.1" | "localhost" | "[::1]")
    ));
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .unwrap();
    let schema = format!("native_reconnect_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await
        .unwrap();
    let search = format!("SET search_path TO {schema}");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(18)
        .after_connect(move |connection, _| {
            let query = search.clone();
            Box::pin(async move {
                sqlx::query(&query).execute(connection).await?;
                Ok(())
            })
        })
        .connect(&url)
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
        lazer_chat: Arc::new(Runtime::with_endpoint(base.replacen("http://", "ws://", 1))),
    };
    state.lazer_chat.start(pool.clone()).await.unwrap();
    let app = router().with_state(state.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let result = std::panic::AssertUnwindSafe(async {
        let mut users = Vec::new();
        let mut tokens = Vec::new();
        for index in 0..4 {
            let user: i64 = sqlx::query_scalar("INSERT INTO users(username,email,password_hash,password_md5_hash) VALUES($1,$2,'unused','unused') RETURNING id")
                .bind(format!("RecoveryPlayer{index}")).bind(format!("recovery{index}@example.test"))
                .fetch_one(&pool).await.unwrap();
            sqlx::query("INSERT INTO user_roles SELECT $1,id FROM roles WHERE name=$2")
                .bind(user).bind(if index == 2 { "administrator" } else { "player" })
                .execute(&pool).await.unwrap();
            let token = common::secret();
            sqlx::query("INSERT INTO lazer_sessions(id,user_id,access_hash,refresh_hash,access_expires_at,refresh_expires_at) VALUES($1,$2,$3,$4,now()+interval '1 hour',now()+interval '1 day')")
                .bind(uuid::Uuid::new_v4()).bind(user).bind(common::token_hash(&token)).bind(common::secret())
                .execute(&pool).await.unwrap();
            initialise(&state, user).await.unwrap();
            users.push(user);
            tokens.push(token);
        }
        let initial: Vec<i64> = sqlx::query_scalar("SELECT channel_id FROM lazer_channel_members WHERE user_id=$1 ORDER BY channel_id")
            .bind(users[0]).fetch_all(&pool).await.unwrap();
        let general: i64 = sqlx::query_scalar("SELECT id FROM channels WHERE name='general'")
            .fetch_one(&pool).await.unwrap();
        let client = reqwest::Client::new();
        let mut original = open(&base, &tokens[0]).await;
        recovered(&mut original, &[], &initial).await;
        let before: Value = submit(&client, &base, &tokens[1], general, "before disconnect", "recovery-before")
            .await.json().await.unwrap();
        assert_eq!(next_message(&mut original).await["messages"][0], before);
        original.close(None).await.unwrap();
        let offline: Value = submit(&client, &base, &tokens[1], general, "while disconnected", "recovery-offline")
            .await.json().await.unwrap();
        let pm: Value = client.post(format!("{base}/api/v2/chat/new")).bearer_auth(&tokens[1])
            .json(&json!({"target_id":users[0],"message":"offline private","uuid":"recovery-private"}))
            .send().await.unwrap().json().await.unwrap();
        let private = pm["channel"]["channel_id"].as_i64().unwrap();
        let mut expected = vec![before["message_id"].as_i64().unwrap(), offline["message_id"].as_i64().unwrap(), pm["message"]["message_id"].as_i64().unwrap()];
        let mut joined = initial.clone();
        joined.push(private);
        let mut socket = open(&base, &tokens[0]).await;
        recovered(&mut socket, &expected, &joined).await;
        // These requests mirror a stock reconnect: REST presence does not need
        // deprecated messages, and the socket supplies missed persisted IDs.
        let presence: Value = client.get(format!("{base}/api/v2/chat/updates?since=0&includes[]=presence"))
            .bearer_auth(&tokens[0]).send().await.unwrap().json().await.unwrap();
        assert!(presence.get("messages").is_none());
        start(&mut socket).await;
        start(&mut socket).await;
        quiet_messages(&mut socket).await;
        // A duplicate queued commit notification overlaps the recovered window.
        state.lazer_chat.events.send(json!({"kind":"message","id":expected[1]})).unwrap();
        quiet_messages(&mut socket).await;
        let live: Value = submit(&client, &base, &tokens[1], general, "after reconnect", "recovery-live")
            .await.json().await.unwrap();
        assert_eq!(next_message(&mut socket).await["messages"][0], live);
        socket.send(ClientMessage::Text(r#"{"event":"chat.end"}"#.into())).await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        start(&mut socket).await;
        quiet_messages(&mut socket).await;
        socket.close(None).await.unwrap();

        // An administrator's stale membership cannot reveal another pair's PM.
        let foreign: i64 = sqlx::query_scalar("SELECT illya_pm_channel($1,$2)")
            .bind(users[1]).bind(users[3]).fetch_one(&pool).await.unwrap();
        sqlx::query("INSERT INTO protocol_private_messages(sender_id,recipient_id,content) VALUES($1,$2,'foreign private')")
            .bind(users[1]).bind(users[3]).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO lazer_channel_members(user_id,channel_id) VALUES($1,$2),($3,$2)")
            .bind(users[0]).bind(foreign).bind(users[2]).execute(&pool).await.unwrap();
        let room_channel: i64 = sqlx::query_scalar("INSERT INTO channels(name,required_permission) VALUES('recovery-room','lazer.room') RETURNING id")
            .fetch_one(&pool).await.unwrap();
        let room: i64 = sqlx::query_scalar("INSERT INTO lazer_rooms(host_id,channel_id,settings) VALUES($1,$2,'{}') RETURNING id")
            .bind(users[1]).bind(room_channel).fetch_one(&pool).await.unwrap();
        sqlx::query("INSERT INTO lazer_room_members(room_id,user_id) VALUES($1,$2)")
            .bind(room).bind(users[1]).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO lazer_channel_members(user_id,channel_id) VALUES($1,$2),($3,$2)")
            .bind(users[0]).bind(room_channel).bind(users[2]).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO chat_messages(channel_id,user_id,content) VALUES($1,$2,'for room members')")
            .bind(room_channel).bind(users[1]).execute(&pool).await.unwrap();
        let permission: i64 = sqlx::query_scalar("INSERT INTO channels(name,required_permission) VALUES('recovery-permission','test.private') RETURNING id")
            .fetch_one(&pool).await.unwrap();
        sqlx::query("INSERT INTO lazer_channel_members(user_id,channel_id) VALUES($1,$2)")
            .bind(users[0]).bind(permission).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO chat_messages(channel_id,user_id,content) VALUES($1,$2,'for permission holders')")
            .bind(permission).bind(users[1]).execute(&pool).await.unwrap();
        let mut outsider = open(&base, &tokens[2]).await;
        expected = vec![before["message_id"].as_i64().unwrap(), offline["message_id"].as_i64().unwrap(), live["message_id"].as_i64().unwrap()];
        recovered(&mut outsider, &expected, &initial).await;
        outsider.close(None).await.unwrap();
        // Blocks remove both the private conversation and sender's public rows.
        sqlx::query("INSERT INTO user_relationships(user_id,target_id,relation_type) VALUES($1,$2,'block')")
            .bind(users[1]).bind(users[0]).execute(&pool).await.unwrap();
        let mut blocked_socket = open(&base, &tokens[0]).await;
        recovered(&mut blocked_socket, &[], &initial).await;
        blocked_socket.close(None).await.unwrap();
        sqlx::query("DELETE FROM user_relationships WHERE relation_type='block'")
            .execute(&pool).await.unwrap();
        sqlx::query("UPDATE users SET is_restricted=true WHERE id=$1")
            .bind(users[1]).execute(&pool).await.unwrap();
        let mut restricted_sender = open(&base, &tokens[0]).await;
        recovered(&mut restricted_sender, &[], &initial).await;
        restricted_sender.close(None).await.unwrap();
        sqlx::query("UPDATE users SET is_restricted=false WHERE id=$1")
            .bind(users[1]).execute(&pool).await.unwrap();
        // Leaving a public channel or PM excludes its persisted recovery.
        sqlx::query("DELETE FROM lazer_channel_members WHERE user_id=$1 AND channel_id IN($2,$3)")
            .bind(users[0]).bind(general).bind(private).execute(&pool).await.unwrap();
        let remaining: Vec<_> = initial.iter().copied().filter(|id| *id != general).collect();
        let mut left = open(&base, &tokens[0]).await;
        recovered(&mut left, &[], &remaining).await;
        left.close(None).await.unwrap();

        // No cursor is supplied by stock chat.start: verify the total bound as
        // well as each channel's 50-row bound against a larger persisted set.
        sqlx::query("DELETE FROM chat_messages").execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM lazer_channel_members WHERE user_id=$1").bind(users[0]).execute(&pool).await.unwrap();
        let mut bounded_channels = Vec::new();
        let mut per_channel = HashMap::new();
        for index in 0..5 {
            let channel: i64 = sqlx::query_scalar("INSERT INTO channels(name) VALUES($1) RETURNING id")
                .bind(format!("recovery-bound-{index}")).fetch_one(&pool).await.unwrap();
            sqlx::query("INSERT INTO lazer_channel_members(user_id,channel_id) VALUES($1,$2)")
                .bind(users[0]).bind(channel).execute(&pool).await.unwrap();
            let ids: Vec<i64> = sqlx::query_scalar("INSERT INTO chat_messages(channel_id,user_id,content) SELECT $1,$2,'bounded '||n FROM generate_series(1,70) n RETURNING id")
                .bind(channel).bind(users[1]).fetch_all(&pool).await.unwrap();
            bounded_channels.push(channel);
            per_channel.insert(channel, ids);
        }
        let expected: Vec<i64> = per_channel.values().flat_map(|ids| ids.iter().rev().take(50).copied()).collect();
        let mut expected = expected;
        expected.sort_unstable();
        expected = expected.into_iter().rev().take(200).collect();
        expected.reverse();
        let candidates = reconnect_ids(&state, users[0], &bounded_channels).await.unwrap();
        assert_eq!(candidates, expected);
        assert_eq!(candidates.len(), 200);
        for ids in per_channel.values() {
            assert!(candidates.iter().filter(|id| ids.contains(id)).count() <= 50);
        }
        let mut bounded = open(&base, &tokens[0]).await;
        recovered(&mut bounded, &expected, &bounded_channels).await;
        bounded.close(None).await.unwrap();
        // Hidden tails must not cause recovery to traverse an older archive to
        // fill its visible quota. The raw 50-row window is bounded first.
        sqlx::query("UPDATE users SET is_restricted=true WHERE id=$1")
            .bind(users[3]).execute(&pool).await.unwrap();
        for channel in &bounded_channels {
            sqlx::query("INSERT INTO chat_messages(channel_id,user_id,content) SELECT $1,$2,'hidden tail '||n FROM generate_series(1,50) n")
                .bind(channel).bind(users[3]).execute(&pool).await.unwrap();
        }
        assert!(reconnect_ids(&state, users[0], &bounded_channels).await.unwrap().is_empty());
        let mut hidden_tail = open(&base, &tokens[0]).await;
        recovered(&mut hidden_tail, &[], &bounded_channels).await;
        hidden_tail.close(None).await.unwrap();
        sqlx::query("UPDATE users SET is_restricted=false WHERE id=$1")
            .bind(users[3]).execute(&pool).await.unwrap();

        // Revocation/restriction after HTTP upgrade but before chat.start must
        // close the socket without exposing channel or persisted message data.
        let mut revoked = connect_without_start(&base, &tokens[0]).await;
        sqlx::query("DELETE FROM lazer_sessions WHERE access_hash=$1")
            .bind(common::token_hash(&tokens[0])).execute(&pool).await.unwrap();
        start(&mut revoked).await;
        closed_without_messages(&mut revoked).await;
        let mut restricted = connect_without_start(&base, &tokens[3]).await;
        sqlx::query("UPDATE users SET is_restricted=true WHERE id=$1")
            .bind(users[3]).execute(&pool).await.unwrap();
        start(&mut restricted).await;
        closed_without_messages(&mut restricted).await;
    })
    .catch_unwind()
    .await;
    server.abort();
    let _ = server.await;
    if let Some(task) = state.lazer_chat.listener.lock().unwrap().take() {
        task.abort();
    }
    drop(state);
    tokio::time::timeout(Duration::from_secs(10), pool.close())
        .await
        .expect("reconnect test connections did not release");
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
