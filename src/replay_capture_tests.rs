//! True TCP HTTP/WebSocket integration against a fresh PostgreSQL schema.
use super::*;
use crate::{common, lazer, signalr};
use axum::{
    Json, Router,
    extract::State,
    response::{IntoResponse, Response},
    routing::post,
};
use futures_util::{FutureExt, SinkExt, StreamExt};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::{
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Clone)]
struct Ingestion {
    pool: sqlx::PgPool,
    failure: Arc<AtomicU8>,
}
async fn ingest(State(state): State<Ingestion>, Json(input): Json<Value>) -> Response {
    let mode = state.failure.load(Ordering::Acquire);
    if mode == 1 {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    if mode == 3 {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let pool = state.pool;
    let id:i64=sqlx::query_scalar("INSERT INTO scores(user_id,beatmap_id,mode,score,accuracy,max_combo,mods,pp,passed,hit_counts,client,ingestion_key,api_mods,native_submission,client_beatmap_md5) VALUES($1,$2,$3,$4,$5,$6,$7,0,$8,$9,'lazer',$10,$11,$12,$13) ON CONFLICT(ingestion_key) DO UPDATE SET ingestion_key=excluded.ingestion_key RETURNING id").bind(input["user_id"].as_i64().unwrap()).bind(input["beatmap_id"].as_i64().unwrap()).bind(input["mode"].as_i64().unwrap() as i16).bind(input["score"].as_i64().unwrap()).bind(input["accuracy"].as_f64().unwrap()).bind(input["max_combo"].as_i64().unwrap() as i32).bind(input["mods"].as_i64().unwrap()).bind(input["passed"].as_bool().unwrap()).bind(&input["hit_counts"]).bind(input["ingestion_key"].as_str().unwrap()).bind(&input["api_mods"]).bind(&input["native_submission"]).bind(input["client_beatmap_md5"].as_str()).fetch_one(&pool).await.unwrap();
    if mode == 2 {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    Json(json!({"id":id})).into_response()
}
type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
struct Ws {
    socket: Socket,
    binary: bool,
    events: Vec<Value>,
}
async fn websocket(base: &str, secret: &str) -> Ws {
    websocket_protocol(base, secret, false).await
}
async fn websocket_protocol(base: &str, secret: &str, binary: bool) -> Ws {
    let (mut socket, _) = connect_async(format!(
        "{}/spectator?access_token={secret}",
        base.replacen("http", "ws", 1)
    ))
    .await
    .unwrap();
    socket
        .send(Message::Text(
            format!(
                "{{\"protocol\":\"{}\",\"version\":1}}\u{1e}",
                if binary { "messagepack" } else { "json" }
            )
            .into(),
        ))
        .await
        .unwrap();
    assert_eq!(
        socket.next().await.unwrap().unwrap().into_text().unwrap(),
        "{}\u{1e}"
    );
    Ws {
        socket,
        binary,
        events: vec![],
    }
}
async fn read(socket: &mut Ws) -> Value {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), socket.socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let value: Value = match frame {
            Message::Text(text) => serde_json::from_str(text.trim_end_matches('\u{1e}')).unwrap(),
            Message::Binary(bytes) => {
                let record =
                    resonance_protocol::signalr::decode_messagepack_record(&bytes).unwrap();
                match record[0].as_i64().unwrap() {
                    1 => json!({"type":1,"target":record[3],"arguments":record[4]}),
                    3 if record[3] == 1 => json!({"type":3,"error":record[4]}),
                    3 => json!({"type":3,"result":record[4]}),
                    kind => json!({"type":kind}),
                }
            }
            Message::Ping(_) | Message::Pong(_) => continue,
            other => panic!("unexpected frame {other:?}"),
        };
        if value["type"] != 6 {
            return value;
        }
    }
}
async fn call(socket: &mut Ws, target: &str, args: Vec<Value>) -> Value {
    use resonance_protocol::signalr::{Codec, Protocol};
    let bytes = Codec::new(if socket.binary {
        Protocol::MessagePack
    } else {
        Protocol::Json
    })
    .invocation_with_id(Some("test"), target, args)
    .unwrap();
    let frame = if socket.binary {
        Message::Binary(bytes.into())
    } else {
        Message::Text(String::from_utf8(bytes).unwrap().into())
    };
    socket.socket.send(frame).await.unwrap();
    loop {
        let value = read(socket).await;
        if value["type"] == 3 {
            return value;
        }
        socket.events.push(value);
    }
}
async fn processed(socket: &mut Ws) -> Value {
    if let Some(index) = socket
        .events
        .iter()
        .position(|event| event["target"] == "UserScoreProcessed")
    {
        return socket.events.remove(index)["arguments"].clone();
    }
    loop {
        let event = read(socket).await;
        if event["target"] == "UserScoreProcessed" {
            return event["arguments"].clone();
        }
        socket.events.push(event);
    }
}
async fn no_processed(socket: &mut Ws) {
    assert!(
        !socket
            .events
            .iter()
            .any(|event| event["target"] == "UserScoreProcessed")
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(150), async {
            loop {
                let event = read(socket).await;
                assert_ne!(
                    event["target"], "UserScoreProcessed",
                    "unexpected callback {event}"
                );
                socket.events.push(event);
            }
        })
        .await
        .is_err()
    );
}
fn ok(value: &Value) {
    assert!(value.get("error").is_none(), "{value}");
}
fn initial(map: i64) -> Value {
    json!({"beatmapID":map,"rulesetID":0,"mods":[{"acronym":"DT","settings":{"speed_change":1.2}}],"state":1,"maximumStatistics":{"5":2}})
}
fn bundle(time: f64) -> Value {
    json!({"header":{"totalScore":1234,"accuracy":1.0,"combo":2,"maxCombo":2,"statistics":{"5":2},"scoreProcessorStatistics":[600.0,600.0,2,0.5,0.0],"receivedTime":null,"mods":[{"acronym":"DT","settings":{"speed_change":1.2}}],"totalScoreWithoutMods":1234,"pauses":[]},"frames":[{"time":time,"mouseX":256.0,"mouseY":192.0,"buttonState":1}]})
}
fn submission() -> Value {
    json!({"ruleset_id":0,"passed":true,"total_score":1234,"total_score_without_mods":1234,"accuracy":1.0,"max_combo":2,"rank":"X","mods":[{"acronym":"DT","settings":{"speed_change":1.2}}],"statistics":{"great":2},"maximum_statistics":{"great":2},"pauses":[]})
}

async fn moderate(pool: &sqlx::PgPool, actor: i64, score: i64, status: &str) {
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM users WHERE id=$1 FOR UPDATE")
        .bind(actor)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE scores SET verification=$2 WHERE id=$1")
        .bind(score)
        .bind(status)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO audit_events(actor_id,action,target_type,target_id,details) VALUES($1,'scores.verify','score',$2,$3)")
        .bind(actor).bind(score.to_string()).bind(json!({"verification":status}))
        .execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL, every run creates/drops its own schema"]
async fn native_replay_http_websocket_persistence() {
    let url = std::env::var("ILLYA_TEST_DATABASE_URL").unwrap();
    let control = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .unwrap();
    let schema = format!("replay_test_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&control)
        .await
        .unwrap();
    let options = PgConnectOptions::from_str(&url)
        .unwrap()
        .options([("search_path", schema.as_str())]);
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
    let storage = std::env::temp_dir().join(&schema);
    tokio::fs::create_dir_all(&storage).await.unwrap();
    let ingest_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ingest_url = format!("http://{}", ingest_listener.local_addr().unwrap());
    let ingestion_failure = Arc::new(AtomicU8::new(0));
    let ingest_app = Router::new()
        .route("/api/v1/internal/scores", post(ingest))
        .with_state(Ingestion {
            pool: pool.clone(),
            failure: ingestion_failure.clone(),
        });
    let ingest_server =
        tokio::spawn(async move { axum::serve(ingest_listener, ingest_app).await.unwrap() });
    let state = AppState {
        pool: pool.clone(),
        storage: Arc::new(storage.clone()),
        sanctuary_url: Arc::new(ingest_url),
        service_key: Arc::new("test-service-key-with-at-least-thirty-two-bytes".into()),
        http: reqwest::Client::new(),
        limits: Default::default(),
        trusted_proxies: Default::default(),
        signalr: Arc::new(signalr::Runtime::new()),
        lazer_chat: Arc::new(crate::lazer_chat::Runtime::new()),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .merge(lazer::router())
        .merge(signalr::router())
        .with_state(state.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let result=std::panic::AssertUnwindSafe(async {
        let mut users=vec![];let mut secrets=vec![];for i in 0..2 {let id:i64=sqlx::query_scalar("INSERT INTO users(username,email,password_hash,password_md5_hash) VALUES($1,$2,'unused','unused') RETURNING id").bind(format!("ReplayPlayer{i}")).bind(format!("replay{i}@example.invalid")).fetch_one(&pool).await.unwrap();sqlx::query("INSERT INTO user_roles SELECT $1,id FROM roles WHERE name='player'").bind(id).execute(&pool).await.unwrap();let secret=common::secret();sqlx::query("INSERT INTO lazer_sessions(id,user_id,access_hash,refresh_hash,access_expires_at,refresh_expires_at) VALUES($1,$2,$3,$4,now()+interval '1 hour',now()+interval '1 day')").bind(uuid::Uuid::new_v4()).bind(id).bind(common::token_hash(&secret)).bind(common::secret()).execute(&pool).await.unwrap();users.push(id);secrets.push(secret);}
        let map:i64=sqlx::query_scalar("INSERT INTO beatmaps(title,artist,creator,version,mode,stars,max_combo,md5,osu_path) VALUES('Fixture','Illyaverse','Tests','Easy',0,1,2,'0123456789abcdef0123456789abcdef','fixture.osu') RETURNING id").fetch_one(&pool).await.unwrap();let native_map:i64=sqlx::query_scalar("SELECT native_id FROM beatmaps WHERE id=$1").bind(map).fetch_one(&pool).await.unwrap();assert_ne!(map,native_map);let client=reqwest::Client::new();
        let mut ws=websocket(&base,&secrets[0]).await;let mut guest=websocket(&base,&secrets[1]).await;let mut same_user_other=websocket(&base,&secrets[0]).await;
        for (binary,score_before_end) in [(false,false),(false,true),(true,false),(true,true)] {
            ws.socket.close(None).await.unwrap();
            ws=websocket_protocol(&base,&secrets[0],binary).await;
            let token:Value=client.post(format!("{base}/api/v2/beatmaps/{native_map}/solo/scores")).bearer_auth(&secrets[0]).form(&[("beatmap_hash","0123456789abcdef0123456789abcdef"),("version_hash","fixture-version"),("ruleset_id","0")]).send().await.unwrap().json().await.unwrap();let token=token["id"].as_i64().unwrap();
            let denial=call(&mut guest,"BeginPlaySessionV2",vec![json!(token),initial(native_map)]).await;assert_eq!(denial["error"],"score_token_owner_mismatch");ok(&call(&mut ws,"BeginPlaySessionV2",vec![json!(token),initial(native_map)]).await);ok(&call(&mut ws,"SendFrameDataV2",vec![json!(token),bundle(100.0)]).await);ok(&call(&mut ws,"SendFrameDataV2",vec![json!(token),bundle(100.0)]).await);
            if !score_before_end {ok(&call(&mut ws,"EndPlaySessionV2",vec![json!(token),json!(3)]).await);}
            let response=client.put(format!("{base}/api/v2/beatmaps/{native_map}/solo/scores/{token}")).bearer_auth(&secrets[0]).json(&submission()).send().await.unwrap();assert!(response.status().is_success());let score:Value=response.json().await.unwrap();let id=score["id"].as_i64().unwrap();
            if score_before_end {assert_eq!(score["has_replay"],false);ok(&call(&mut ws,"EndPlaySessionV2",vec![json!(token),json!(3)]).await);}
            assert_eq!(processed(&mut ws).await,json!([users[0],id]));
            no_processed(&mut guest).await;
            no_processed(&mut same_user_other).await;
            let retry:Value=client.put(format!("{base}/api/v2/beatmaps/{native_map}/solo/scores/{token}")).bearer_auth(&secrets[0]).json(&submission()).send().await.unwrap().json().await.unwrap();assert_eq!(retry["id"],id);assert_eq!(retry["has_replay"],true);assert_eq!(retry["verification"],"verified");
            no_processed(&mut ws).await;
            let response=client.get(format!("{base}/api/v2/scores/{id}/download")).bearer_auth(&secrets[0]).send().await.unwrap();assert_eq!(response.status(),StatusCode::OK);let bytes=response.bytes().await.unwrap();let replay=lazer_replay::ExtendedReplay::parse(&bytes).unwrap();assert_eq!(replay.metadata.online_id,id);assert_eq!(replay.metadata.user_id,users[0]);assert_eq!(replay.frame_count,1);
            assert_eq!(client.get(format!("{base}/api/v2/scores/{id}/download")).bearer_auth(&secrets[1]).send().await.unwrap().status(),StatusCode::OK);
            let capture=sqlx::query("SELECT completed_at,(SELECT count(*) FROM lazer_replay_chunks WHERE token_id=$1) chunks,frame_count FROM lazer_replay_captures WHERE token_id=$1").bind(token).fetch_one(&pool).await.unwrap();assert!(capture.get::<Option<chrono::DateTime<chrono::Utc>>,_>("completed_at").is_some());assert_eq!(capture.get::<i64,_>("chunks"),0);assert_eq!(capture.get::<i32,_>("frame_count"),1);assert_eq!(retry["replay_complete"],true);
            let (first, second) = tokio::join!(finalize(&state, users[0], token), finalize(&state, users[0], token));
            assert_eq!(first.unwrap(), Some(id));assert_eq!(second.unwrap(), Some(id));
            assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM audit_events WHERE action='scores.native_publish' AND target_id=$1").bind(id.to_string()).fetch_one(&pool).await.unwrap(), 1);
            // A completed replay still checks immutable score/capture identity.
            sqlx::query("UPDATE scores SET score=score+1 WHERE id=$1").bind(id).execute(&pool).await.unwrap();
            assert_eq!(finalize(&state, users[0], token).await.unwrap_err().1, "score_ingestion_link_mismatch");
            sqlx::query("UPDATE scores SET score=score-1 WHERE id=$1").bind(id).execute(&pool).await.unwrap();
            moderate(&pool, users[0], id, "pending").await;
            assert_eq!(finalize(&state, users[0], token).await.unwrap(), Some(id));
            assert_eq!(sqlx::query_scalar::<_, String>("SELECT verification FROM scores WHERE id=$1").bind(id).fetch_one(&pool).await.unwrap(), "pending");
            assert_eq!(client.get(format!("{base}/api/v2/scores/{id}/download")).bearer_auth(&secrets[1]).send().await.unwrap().status(), StatusCode::NOT_FOUND);
            moderate(&pool, users[0], id, "rejected").await;
            assert_eq!(finalize(&state, users[0], token).await.unwrap(), None);
            assert_eq!(sqlx::query_scalar::<_, String>("SELECT verification FROM scores WHERE id=$1").bind(id).fetch_one(&pool).await.unwrap(), "rejected");
            assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM audit_events WHERE action='scores.native_publish' AND target_id=$1").bind(id.to_string()).fetch_one(&pool).await.unwrap(), 1);
            moderate(&pool, users[0], id, "verified").await;
            sqlx::query("UPDATE users SET is_restricted=true WHERE id=$1").bind(users[0]).execute(&pool).await.unwrap();assert_eq!(client.get(format!("{base}/api/v2/scores/{id}/download")).bearer_auth(&secrets[1]).send().await.unwrap().status(),StatusCode::NOT_FOUND);sqlx::query("UPDATE users SET is_restricted=false WHERE id=$1").bind(users[0]).execute(&pool).await.unwrap();
        }
        // A durable accepted intent recovers if ingestion was unavailable,
        // or if the score committed but its response/link was lost.
        for fail_after_commit in [false,true] {
            let token:Value=client.post(format!("{base}/api/v2/beatmaps/{native_map}/solo/scores"))
                .bearer_auth(&secrets[0])
                .form(&[("beatmap_hash","0123456789abcdef0123456789abcdef"),("version_hash","fixture-version"),("ruleset_id","0")])
                .send().await.unwrap().json().await.unwrap();
            let token=token["id"].as_i64().unwrap();
            ok(&call(&mut ws,"BeginPlaySessionV2",vec![json!(token),initial(native_map)]).await);
            ok(&call(&mut ws,"SendFrameDataV2",vec![json!(token),bundle(100.0)]).await);
            ok(&call(&mut ws,"EndPlaySessionV2",vec![json!(token),json!(3)]).await);
            ingestion_failure.store(if fail_after_commit {2} else {1},Ordering::Release);
            let endpoint=format!("{base}/api/v2/beatmaps/{native_map}/solo/scores/{token}");
            assert_eq!(client.put(&endpoint).bearer_auth(&secrets[0]).json(&submission()).send().await.unwrap().status(),StatusCode::SERVICE_UNAVAILABLE);
            let receipt=sqlx::query("SELECT submission,submission_sha256,received_session_id,received_at,score_id,processing_status FROM lazer_score_tokens WHERE id=$1")
                .bind(token).fetch_one(&pool).await.unwrap();
            assert_eq!(receipt.get::<String,_>("processing_status"),"received");
            assert!(receipt.get::<Option<i64>,_>("score_id").is_none());
            assert!(receipt.get::<Option<uuid::Uuid>,_>("received_session_id").is_some());
            assert!(receipt.get::<Option<chrono::DateTime<chrono::Utc>>,_>("received_at").is_some());
            let payload=receipt.get::<Option<Value>,_>("submission").unwrap();
            assert_eq!(receipt.get::<Option<String>,_>("submission_sha256").unwrap(),digest(&serde_json::to_vec(&payload).unwrap()));
            let before:i64=sqlx::query_scalar("SELECT count(*) FROM scores WHERE ingestion_key=$1").bind(format!("lazer:{token}")).fetch_one(&pool).await.unwrap();
            assert_eq!(before,i64::from(fail_after_commit));
            let mut altered=submission();altered["total_score"]=json!(1235);
            assert_eq!(client.put(&endpoint).bearer_auth(&secrets[0]).json(&altered).send().await.unwrap().status(),StatusCode::CONFLICT);
            assert_eq!(client.put(&endpoint).bearer_auth(&secrets[1]).json(&submission()).send().await.unwrap().status(),StatusCode::NOT_FOUND);
            ingestion_failure.store(0,Ordering::Release);
            sqlx::query("UPDATE lazer_score_tokens SET retry_at=now() WHERE id=$1").bind(token).execute(&pool).await.unwrap();
            let (a,b)=tokio::join!(lazer::recover_solo_scores(&state),lazer::recover_solo_scores(&state));
            assert_eq!(a.unwrap()+b.unwrap(),1);
            let score:i64=sqlx::query_scalar("SELECT score_id FROM lazer_score_tokens WHERE id=$1 AND processing_status='processed'").bind(token).fetch_one(&pool).await.unwrap();
            assert_eq!(processed(&mut ws).await,json!([users[0],score]));
            assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM scores WHERE ingestion_key=$1").bind(format!("lazer:{token}")).fetch_one(&pool).await.unwrap(),1);
            let published=sqlx::query("SELECT replay_complete,verification FROM scores WHERE id=$1").bind(score).fetch_one(&pool).await.unwrap();
            assert!(published.get::<bool,_>("replay_complete"));assert_eq!(published.get::<String,_>("verification"),"verified");
            for action in ["scores.native_publish","scores.lazer_replay_capture"] {
                assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM audit_events WHERE action=$1 AND target_id=$2").bind(action).bind(score.to_string()).fetch_one(&pool).await.unwrap(),1);
            }
            assert_eq!(lazer::recover_solo_scores(&state).await.unwrap(),0);
            let retry:Value=client.put(&endpoint).bearer_auth(&secrets[0]).json(&submission()).send().await.unwrap().json().await.unwrap();
            assert_eq!(retry["id"],score);
            no_processed(&mut ws).await;no_processed(&mut guest).await;no_processed(&mut same_user_other).await;
        }
        // Withdrawal blocks unpersisted recovery while an exact saved orphan
        // can finish linkage. A mutated saved score cannot use that bypass.
        let native_set:i64=sqlx::query_scalar("SELECT native_set_id FROM beatmaps WHERE id=$1").bind(map).fetch_one(&pool).await.unwrap();
        for saved in [false,true] {
            let token:Value=client.post(format!("{base}/api/v2/beatmaps/{native_map}/solo/scores"))
                .bearer_auth(&secrets[0])
                .form(&[("beatmap_hash","0123456789abcdef0123456789abcdef"),("version_hash","fixture-version"),("ruleset_id","0")])
                .send().await.unwrap().json().await.unwrap();let token=token["id"].as_i64().unwrap();
            let key=format!("lazer:{token}");
            let endpoint=format!("{base}/api/v2/beatmaps/{native_map}/solo/scores/{token}");
            ingestion_failure.store(if saved {2} else {1},Ordering::Release);
            assert_eq!(client.put(&endpoint).bearer_auth(&secrets[0]).json(&submission()).send().await.unwrap().status(),StatusCode::SERVICE_UNAVAILABLE);
            sqlx::query("INSERT INTO beatmapset_availability(native_set_id,withdrawn,revision,reason,actor_id) VALUES($1,true,1,'Solo recovery fixture',$2) ON CONFLICT(native_set_id) DO UPDATE SET withdrawn=true,revision=beatmapset_availability.revision+1")
                .bind(native_set).bind(users[0]).execute(&pool).await.unwrap();
            ingestion_failure.store(0,Ordering::Release);
            if saved {
                sqlx::query("UPDATE scores SET score=score+1 WHERE ingestion_key=$1").bind(&key).execute(&pool).await.unwrap();
                sqlx::query("UPDATE lazer_score_tokens SET retry_at=now() WHERE id=$1").bind(token).execute(&pool).await.unwrap();
                assert_eq!(lazer::recover_solo_scores(&state).await.unwrap(),0);
                let current=sqlx::query("SELECT score_id,processing_error FROM lazer_score_tokens WHERE id=$1").bind(token).fetch_one(&pool).await.unwrap();
                assert!(current.get::<Option<i64>,_>("score_id").is_none());
                assert_eq!(current.get::<Option<String>,_>("processing_error").as_deref(),Some("score_ingestion_link_mismatch"));
                sqlx::query("UPDATE scores SET score=score-1 WHERE ingestion_key=$1").bind(&key).execute(&pool).await.unwrap();
            }
            sqlx::query("UPDATE lazer_score_tokens SET retry_at=now() WHERE id=$1").bind(token).execute(&pool).await.unwrap();
            assert_eq!(lazer::recover_solo_scores(&state).await.unwrap(),usize::from(saved));
            if !saved {
                assert_eq!(client.put(&endpoint).bearer_auth(&secrets[0]).json(&submission()).send().await.unwrap().status(),StatusCode::GONE);
                assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM scores WHERE ingestion_key=$1").bind(&key).fetch_one(&pool).await.unwrap(),0);
            }
            sqlx::query("UPDATE beatmapset_availability SET withdrawn=false,revision=revision+1 WHERE native_set_id=$1").bind(native_set).execute(&pool).await.unwrap();
            if !saved {
                sqlx::query("UPDATE lazer_score_tokens SET retry_at=now() WHERE id=$1").bind(token).execute(&pool).await.unwrap();
                assert_eq!(lazer::recover_solo_scores(&state).await.unwrap(),1);
            }
            assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM scores WHERE ingestion_key=$1").bind(&key).fetch_one(&pool).await.unwrap(),1);
        }
        // Current native V1 methods, with score submission racing EndPlaySession.
        let native_token:i64=sqlx::query_scalar("INSERT INTO lazer_score_tokens(user_id,beatmap_id,ruleset_id,version_hash,expires_at) VALUES($1,$2,0,'fixture',now()+interval '1 hour') RETURNING id").bind(users[0]).bind(map).fetch_one(&pool).await.unwrap();ok(&call(&mut ws,"BeginPlaySession",vec![json!(native_token),initial(native_map)]).await);ok(&call(&mut ws,"SendFrameData",vec![bundle(100.0)]).await);let mut finalstate=initial(native_map);finalstate["state"]=json!(3);let mut wrongstate=finalstate.clone();wrongstate["beatmapID"]=json!(native_map+1);assert_eq!(call(&mut ws,"EndPlaySession",vec![wrongstate]).await["error"],"play_session_state_mismatch");
        let request=client.put(format!("{base}/api/v2/beatmaps/{native_map}/solo/scores/{native_token}")).bearer_auth(&secrets[0]).json(&submission());let (ended,submitted)=tokio::join!(call(&mut ws,"EndPlaySession",vec![finalstate]),request.send());ok(&ended);assert!(submitted.unwrap().status().is_success());let ready:Value=client.put(format!("{base}/api/v2/beatmaps/{native_map}/solo/scores/{native_token}")).bearer_auth(&secrets[0]).json(&submission()).send().await.unwrap().json().await.unwrap();assert_eq!(ready["has_replay"],true);assert_eq!(ready["replay_complete"],true);assert_eq!(ready["verification"],"verified");
        assert_eq!(processed(&mut ws).await,json!([users[0],ready["id"]]));
        no_processed(&mut ws).await;
        // Reconnect preserves bytes and invalidates the old connection's writer.
        let token:i64=sqlx::query_scalar("INSERT INTO lazer_score_tokens(user_id,beatmap_id,ruleset_id,version_hash,expires_at) VALUES($1,$2,0,'fixture',now()+interval '1 hour') RETURNING id").bind(users[0]).bind(map).fetch_one(&pool).await.unwrap();begin(&state,users[0],token,"connection-a",&initial(native_map)).await.unwrap();frames(&state,users[0],token,"connection-a",&bundle(100.0)).await.unwrap();begin(&state,users[0],token,"connection-b",&initial(native_map)).await.unwrap();assert_eq!(frames(&state,users[0],token,"connection-a",&bundle(200.0)).await.unwrap_err().1,"replay_connection_replaced");frames(&state,users[0],token,"connection-b",&bundle(200.0)).await.unwrap();assert_eq!(frames(&state,users[0],token,"connection-b",&bundle(150.0)).await.unwrap_err().1,"replay_frame_out_of_order");assert!(sqlx::query_scalar::<_,bool>("SELECT interrupted FROM lazer_replay_captures WHERE token_id=$1").bind(token).fetch_one(&pool).await.unwrap());
        end(&state,users[0],token,"connection-b",3).await.unwrap();
        let score:Value=client.put(format!("{base}/api/v2/beatmaps/{native_map}/solo/scores/{token}")).bearer_auth(&secrets[0]).json(&submission()).send().await.unwrap().json().await.unwrap();let id=score["id"].as_i64().unwrap();assert_eq!(score["has_replay"],true);assert_eq!(score["replay_complete"],false);assert_eq!(score["verification"],"pending");assert_eq!(client.get(format!("{base}/api/v2/scores/{id}/download")).bearer_auth(&secrets[1]).send().await.unwrap().status(),StatusCode::NOT_FOUND);let bytes=client.get(format!("{base}/api/v2/scores/{id}/download")).bearer_auth(&secrets[0]).send().await.unwrap().bytes().await.unwrap();let replay=lazer_replay::ExtendedReplay::parse(&bytes).unwrap();assert_eq!(replay.frame_count,2);assert_eq!(replay.metadata.mods[0].settings["speed_change"],1.2);
        let (first,second)=tokio::join!(finalize(&state,users[0],token),finalize(&state,users[0],token));assert_eq!(first.unwrap(),Some(id));assert_eq!(second.unwrap(),Some(id));assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM audit_events WHERE action='scores.lazer_replay_capture' AND target_id=$1").bind(id.to_string()).fetch_one(&pool).await.unwrap(),1);assert_eq!(begin(&state,users[0],token,"connection-c",&initial(native_map)).await.unwrap_err().1,"replay_token_already_used");
        let expired:i64=sqlx::query_scalar("INSERT INTO lazer_score_tokens(user_id,beatmap_id,ruleset_id,version_hash,expires_at) VALUES($1,$2,0,'fixture',now()-interval '1 second') RETURNING id").bind(users[0]).bind(map).fetch_one(&pool).await.unwrap();assert_eq!(begin(&state,users[0],expired,"connection-c",&initial(native_map)).await.unwrap_err().1,"invalid_score_token");
        let quit:i64=sqlx::query_scalar("INSERT INTO lazer_score_tokens(user_id,beatmap_id,ruleset_id,version_hash,expires_at) VALUES($1,$2,0,'fixture',now()+interval '1 hour') RETURNING id").bind(users[0]).bind(map).fetch_one(&pool).await.unwrap();begin(&state,users[0],quit,"connection-q",&initial(native_map)).await.unwrap();frames(&state,users[0],quit,"connection-q",&bundle(100.0)).await.unwrap();end(&state,users[0],quit,"connection-q",5).await.unwrap();assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM lazer_replay_chunks WHERE token_id=$1").bind(quit).fetch_one(&pool).await.unwrap(),0);
        // Simulate a process stopping after committing token.score_id but before
        // calling finalize; persisted state alone must publish after recovery.
        let recovery:i64=sqlx::query_scalar("INSERT INTO lazer_score_tokens(user_id,beatmap_id,ruleset_id,version_hash,expires_at) VALUES($1,$2,0,'fixture',now()+interval '1 hour') RETURNING id").bind(users[0]).bind(map).fetch_one(&pool).await.unwrap();begin(&state,users[0],recovery,"connection-recovery",&initial(native_map)).await.unwrap();frames(&state,users[0],recovery,"connection-recovery",&bundle(100.0)).await.unwrap();end(&state,users[0],recovery,"connection-recovery",3).await.unwrap();let input:SoloScoreSubmission=serde_json::from_value(submission()).unwrap();let recovery_score:i64=sqlx::query_scalar("INSERT INTO scores(user_id,beatmap_id,mode,score,accuracy,max_combo,mods,pp,passed,hit_counts,client,api_mods,native_submission,ingestion_key) VALUES($1,$2,0,1234,100,2,64,0,true,$3,'lazer',$4,$5,$6) RETURNING id").bind(users[0]).bind(map).bind(input.legacy_hit_counts()).bind(submission()["mods"].clone()).bind(submission()).bind(format!("lazer:{recovery}")).fetch_one(&pool).await.unwrap();sqlx::query("UPDATE lazer_score_tokens SET score_id=$2,submission=$3,consumed_at=now() WHERE id=$1").bind(recovery).bind(recovery_score).bind(submission()).execute(&pool).await.unwrap();assert_eq!(recover_pending(&state).await.unwrap(),1);assert_eq!(recover_pending(&state).await.unwrap(),0);assert!(sqlx::query_scalar::<_,Option<String>>("SELECT replay_path FROM scores WHERE id=$1").bind(recovery_score).fetch_one(&pool).await.unwrap().is_some());assert_eq!(sqlx::query_scalar::<_,String>("SELECT verification FROM scores WHERE id=$1").bind(recovery_score).fetch_one(&pool).await.unwrap(),"verified");
        // A durable complete replay can recover an older pending receipt once.
        sqlx::query("UPDATE scores SET verification='pending' WHERE id=$1").bind(recovery_score).execute(&pool).await.unwrap();
        sqlx::query("DELETE FROM audit_events WHERE action='scores.native_publish' AND target_id=$1").bind(recovery_score.to_string()).execute(&pool).await.unwrap();
        assert_eq!(finalize(&state,users[0],recovery).await.unwrap(),Some(recovery_score));
        assert_eq!(sqlx::query_scalar::<_,String>("SELECT verification FROM scores WHERE id=$1").bind(recovery_score).fetch_one(&pool).await.unwrap(),"verified");
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM audit_events WHERE action='scores.native_publish' AND target_id=$1").bind(recovery_score.to_string()).fetch_one(&pool).await.unwrap(),1);
        // A decision made before replay finalization survives the replay write.
        for decision in ["pending", "verified", "rejected"] {
            let token: Value = client.post(format!("{base}/api/v2/beatmaps/{native_map}/solo/scores"))
                .bearer_auth(&secrets[0])
                .form(&[("beatmap_hash", "0123456789abcdef0123456789abcdef"), ("version_hash", "fixture-version"), ("ruleset_id", "0")])
                .send().await.unwrap().json().await.unwrap();
            let token = token["id"].as_i64().unwrap();
            begin(&state, users[0], token, "connection-moderated", &initial(native_map)).await.unwrap();
            frames(&state, users[0], token, "connection-moderated", &bundle(100.0)).await.unwrap();
            let score: Value = client.put(format!("{base}/api/v2/beatmaps/{native_map}/solo/scores/{token}"))
                .bearer_auth(&secrets[0]).json(&submission()).send().await.unwrap().json().await.unwrap();
            let id = score["id"].as_i64().unwrap();
            assert_eq!(score["verification"], "pending");
            moderate(&pool, users[0], id, decision).await;
            let result = end(&state, users[0], token, "connection-moderated", 3).await.unwrap();
            assert_eq!(result, (decision != "rejected").then_some(id));
            let stored = sqlx::query("SELECT verification,replay_path FROM scores WHERE id=$1")
                .bind(id).fetch_one(&pool).await.unwrap();
            assert_eq!(stored.get::<String, _>("verification"), decision);
            assert_eq!(stored.get::<Option<String>, _>("replay_path").is_some(), decision != "rejected");
            assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM audit_events WHERE action='scores.native_publish' AND target_id=$1")
                .bind(id.to_string()).fetch_one(&pool).await.unwrap(), 0);
        }
        // Original credentials, expiry, restriction, revocation and immutable
        // payload binding cannot be bypassed by the unattended worker.
        for guard in ["expired","revoked","tampered","restricted","session"] {
            let user=usize::from(guard=="session");
            let token:Value=client.post(format!("{base}/api/v2/beatmaps/{native_map}/solo/scores"))
                .bearer_auth(&secrets[user])
                .form(&[("beatmap_hash","0123456789abcdef0123456789abcdef"),("version_hash","fixture-version"),("ruleset_id","0")])
                .send().await.unwrap().json().await.unwrap();
            let token=token["id"].as_i64().unwrap();
            ingestion_failure.store(1,Ordering::Release);
            assert_eq!(client.put(format!("{base}/api/v2/beatmaps/{native_map}/solo/scores/{token}")).bearer_auth(&secrets[user]).json(&submission()).send().await.unwrap().status(),StatusCode::SERVICE_UNAVAILABLE);
            match guard {
                "expired" => {sqlx::query("UPDATE lazer_score_tokens SET expires_at=now()-interval '1 second' WHERE id=$1").bind(token).execute(&pool).await.unwrap();}
                "revoked" => {sqlx::query("UPDATE lazer_score_tokens SET revoked_at=now() WHERE id=$1").bind(token).execute(&pool).await.unwrap();}
                "tampered" => {sqlx::query("UPDATE lazer_score_tokens SET submission=jsonb_set(submission,'{total_score}','9999') WHERE id=$1").bind(token).execute(&pool).await.unwrap();}
                "restricted" => {sqlx::query("UPDATE users SET is_restricted=true WHERE id=$1").bind(users[user]).execute(&pool).await.unwrap();}
                "session" => {sqlx::query("DELETE FROM lazer_sessions WHERE user_id=$1").bind(users[user]).execute(&pool).await.unwrap();}
                _ => unreachable!(),
            }
            sqlx::query("UPDATE lazer_score_tokens SET retry_at=now() WHERE id=$1").bind(token).execute(&pool).await.unwrap();
            ingestion_failure.store(0,Ordering::Release);
            assert_eq!(lazer::recover_solo_scores(&state).await.unwrap(),0);
            assert_eq!(sqlx::query_scalar::<_,String>("SELECT processing_status FROM lazer_score_tokens WHERE id=$1").bind(token).fetch_one(&pool).await.unwrap(),"revoked");
            assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM scores WHERE ingestion_key=$1").bind(format!("lazer:{token}")).fetch_one(&pool).await.unwrap(),0);
            if guard=="restricted" {sqlx::query("UPDATE users SET is_restricted=false WHERE id=$1").bind(users[user]).execute(&pool).await.unwrap();}
        }
        // Each recovery pass is bounded, and a lease held by another claimant
        // leaves the exact received intent untouched until it becomes due.
        ingestion_failure.store(1,Ordering::Release);
        let mut batch=vec![];
        for _ in 0..20 {
            let token:Value=client.post(format!("{base}/api/v2/beatmaps/{native_map}/solo/scores"))
                .bearer_auth(&secrets[0])
                .form(&[("beatmap_hash","0123456789abcdef0123456789abcdef"),("version_hash","fixture-version"),("ruleset_id","0")])
                .send().await.unwrap().json().await.unwrap();let token=token["id"].as_i64().unwrap();
            assert_eq!(client.put(format!("{base}/api/v2/beatmaps/{native_map}/solo/scores/{token}")).bearer_auth(&secrets[0]).json(&submission()).send().await.unwrap().status(),StatusCode::SERVICE_UNAVAILABLE);
            batch.push(token);
        }
        ingestion_failure.store(0,Ordering::Release);
        sqlx::query("UPDATE lazer_score_tokens SET retry_at=now()+interval '1 minute' WHERE id=ANY($1)").bind(&batch).execute(&pool).await.unwrap();
        assert_eq!(lazer::recover_solo_scores(&state).await.unwrap(),0);
        sqlx::query("UPDATE lazer_score_tokens SET retry_at=now() WHERE id=ANY($1)").bind(&batch).execute(&pool).await.unwrap();
        assert_eq!(lazer::recover_solo_scores(&state).await.unwrap(),16);
        assert_eq!(lazer::recover_solo_scores(&state).await.unwrap(),4);
        assert_eq!(lazer::recover_solo_scores(&state).await.unwrap(),0);
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM lazer_score_tokens WHERE id=ANY($1) AND processing_status='processed' AND score_id IS NOT NULL").bind(&batch).fetch_one(&pool).await.unwrap(),20);
        let _=ws.socket.close(None).await;let _=guest.socket.close(None).await;let _=same_user_other.socket.close(None).await;
    }).catch_unwind().await;
    server.abort();
    ingest_server.abort();
    let _ = server.await;
    let _ = ingest_server.await;
    pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&control)
        .await
        .unwrap();
    control.close().await;
    assert!(storage.parent() == Some(std::env::temp_dir().as_path()));
    tokio::fs::remove_dir_all(storage).await.unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic)
    }
}
