//! Original stock-shaped room fixtures using only disposable PostgreSQL and
//! loopback HTTP/WebSocket servers. These are not native game-client evidence.
use super::*;
use crate::native_maps::checksum_tests::{DERIVED, Fixture};
use futures_util::FutureExt;
use std::sync::{Weak, atomic::AtomicUsize};
use tokio::sync::Notify;

const SERVICE_KEY: &str = "room-discovery-fixture-internal-key-32-bytes";

#[derive(Clone, Copy)]
enum Reply {
    Valid,
    Missing,
    Unavailable,
    WrongId,
    WrongHash,
    Withdrawn,
    WrongMode,
    Paused,
}

#[derive(Clone)]
struct Discovery {
    state: AppState,
    replies: Arc<Mutex<HashMap<String, (i64, Reply)>>>,
    hits: Arc<AtomicUsize>,
    started: Arc<Notify>,
    release: Arc<Notify>,
    connections: Arc<Mutex<Vec<Weak<Connection>>>>,
}

impl Discovery {
    async fn reply(&self, native: i64, behavior: Reply) -> String {
        let checksum = format!("{native:032x}");
        self.replies
            .lock()
            .await
            .insert(checksum.clone(), (native, behavior));
        checksum
    }
}

async fn discover_stub(
    State(stub): State<Discovery>,
    headers: HeaderMap,
    Json(input): Json<Value>,
) -> Response {
    assert_eq!(headers["x-illya-service-key"], SERVICE_KEY);
    assert!(input["user_id"].as_i64().is_some_and(|id| id > 0));
    let checksum = input["checksum"].as_str().unwrap();
    stub.hits.fetch_add(1, Ordering::SeqCst);
    let (requested, behavior) = *stub.replies.lock().await.get(checksum).unwrap();
    match behavior {
        Reply::Missing => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error":{"code":"beatmap_not_found"}})),
            )
                .into_response();
        }
        Reply::Unavailable => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        _ => {}
    }
    let native = requested + i64::from(matches!(behavior, Reply::WrongId));
    let actual = if matches!(behavior, Reply::WrongHash) {
        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".to_owned()
    } else {
        checksum.to_owned()
    };
    let mode: i16 = if matches!(behavior, Reply::WrongMode) {
        3
    } else {
        0
    };
    let set = native + 1_000_000;
    let internal: i64 = sqlx::query_scalar("INSERT INTO beatmaps(official_id,official_set_id,title,artist,creator,version,mode,status,source,stars,max_combo,md5,osu_path) VALUES($1,$2,'Uncached fixture','Fixture','Fixture','Original', $3,'ranked','official',1.5,10,$4,'unused-fixture.osu') RETURNING id")
        .bind(native).bind(set).bind(mode).bind(&actual)
        .fetch_one(&stub.state.pool).await.unwrap();
    if matches!(behavior, Reply::Withdrawn) {
        sqlx::query("INSERT INTO beatmapset_availability(native_set_id,withdrawn,revision,reason,actor_id) VALUES($1,true,1,'Original room fixture',$2)")
            .bind(set).bind(input["user_id"].as_i64().unwrap()).execute(&stub.state.pool).await.unwrap();
    }
    if matches!(behavior, Reply::Paused) {
        stub.started.notify_one();
        stub.release.notified().await;
    }
    Json(
        json!({"id":internal,"official_id":native,"official_set_id":set,
        "md5":actual,"status":"ranked","source":"official","mode":mode}),
    )
    .into_response()
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct Client {
    socket: Socket,
    binary: bool,
    counter: u32,
}

impl Client {
    async fn open(base: &str, token: &str, binary: bool, stub: &Discovery) -> Self {
        let url =
            format!("{base}/multiplayer?access_token={token}").replacen("http://", "ws://", 1);
        let (mut socket, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        socket
            .send(tokio_tungstenite::tungstenite::Message::Text(
                json!({"protocol":if binary {"messagepack"} else {"json"},"version":1})
                    .to_string()
                    .chars()
                    .chain(std::iter::once('\u{1e}'))
                    .collect::<String>()
                    .into(),
            ))
            .await
            .unwrap();
        let record = tokio::time::timeout(Duration::from_secs(3), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(record.into_text().unwrap(), "{}\u{1e}");
        // Hold a weak lifetime witness from before client shutdown. The runtime
        // removes its entry at the beginning of disconnect, before SQL cleanup.
        let connection = {
            let connections = stub.state.signalr.connections.lock().await;
            assert_eq!(connections.len(), 1, "one fixture websocket per runtime");
            Arc::downgrade(connections.values().next().unwrap())
        };
        stub.connections.lock().await.push(connection);
        Self {
            socket,
            binary,
            counter: 0,
        }
    }

    async fn call(&mut self, target: &str, value: Value) -> Value {
        self.counter += 1;
        let id = self.counter.to_string();
        let codec = Codec::new(if self.binary {
            Protocol::MessagePack
        } else {
            Protocol::Json
        });
        let bytes = codec
            .invocation_with_id(Some(&id), target, vec![value])
            .unwrap();
        let message = if self.binary {
            tokio_tungstenite::tungstenite::Message::Binary(bytes.into())
        } else {
            tokio_tungstenite::tungstenite::Message::Text(String::from_utf8(bytes).unwrap().into())
        };
        self.socket.send(message).await.unwrap();
        loop {
            let message = tokio::time::timeout(Duration::from_secs(5), self.socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let record = match message {
                tokio_tungstenite::tungstenite::Message::Text(text) => {
                    serde_json::from_str(text.trim_end_matches('\u{1e}')).unwrap()
                }
                tokio_tungstenite::tungstenite::Message::Binary(bytes) => {
                    resonance_protocol::signalr::decode_messagepack_record(&bytes).unwrap()
                }
                _ => continue,
            };
            if (self.binary && record[0] == 3 && record[2] == id)
                || (!self.binary && record["type"] == 3 && record["invocationId"] == id)
            {
                return record;
            }
        }
    }

    fn success(&self, value: &Value) {
        if self.binary {
            assert!(matches!(value[3].as_i64(), Some(2 | 3)), "{value}");
        } else {
            assert!(value.get("error").is_none(), "{value}");
        }
    }

    fn failure(&self, value: &Value, error: &str) {
        if self.binary {
            assert_eq!(value[3], 1, "{value}");
            assert_eq!(value[4], error, "{value}");
        } else {
            assert_eq!(value["error"], error, "{value}");
        }
    }

    async fn close(mut self) {
        if let Err(error) = self.socket.close(None).await {
            if shutdown_termination(&error) {
                return;
            }
            panic!("fixture websocket close failed: {error}");
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            while let Some(record) = self.socket.next().await {
                match record {
                    Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => break,
                    Ok(_) => {}
                    Err(error) if shutdown_termination(&error) => break,
                    Err(error) => panic!("fixture websocket shutdown failed: {error}"),
                }
            }
        })
        .await
        .expect("fixture websocket transport termination");
    }
}

fn shutdown_termination(error: &tokio_tungstenite::tungstenite::Error) -> bool {
    // Production may drop its split transport after receiving Close without
    // completing the peer handshake. Only this exact terminal condition is
    // tolerated after all call/assertion work; cleanup still waits for the
    // tracked Connection and its detached disconnect task to finish.
    use tokio_tungstenite::tungstenite::{Error, error::ProtocolError};
    matches!(
        error,
        Error::ConnectionClosed
            | Error::AlreadyClosed
            | Error::Protocol(ProtocolError::ResetWithoutClosingHandshake)
    )
}

fn item(binary: bool, native: i64, checksum: &str, id: i64) -> Value {
    if binary {
        // MultiplayerPlaylistItem's published numeric model keys, not a
        // dictionary encoded as MessagePack. Client star rating is untrusted.
        json!([
            id,
            0,
            native,
            checksum,
            0,
            [],
            [],
            false,
            0,
            null,
            999,
            false
        ])
    } else {
        json!({"id":id,"ownerID":0,"beatmapID":native,"beatmapChecksum":checksum,
            "rulesetID":0,"requiredMods":[],"allowedMods":[],"expired":false,
            "playlistOrder":0,"playedAt":null,"starRating":999,"freestyle":false})
    }
}

fn room_input(binary: bool, playlist: Value) -> Value {
    if binary {
        json!([
            0,
            0,
            ["Discovery fixture", 0, "", 1, 0, 0, false, 16],
            [],
            null,
            null,
            [playlist],
            [],
            0
        ])
    } else {
        json!({"settings":{"name":"Discovery fixture","playlistItemId":0,
            "password":"","matchType":1,"queueMode":0,"autoStartDuration":0,
            "autoSkip":false,"maxParticipants":16},"playlist":[playlist]})
    }
}

#[test]
fn signalr_playlist_discovery_validates_both_native_model_shapes() {
    for binary in [false, true] {
        assert!(playlist_input(&item(binary, 7_000_001, DERIVED, 0)).is_ok());
        for native in [0, -1, crate::native_maps::MAX_NATIVE_ID + 1] {
            assert!(matches!(
                playlist_input(&item(binary, native, DERIVED, 0)),
                Err("invalid_beatmap_identity")
            ));
        }
        for checksum in ["", "not-an-md5", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"] {
            assert!(matches!(
                playlist_input(&item(binary, 7_000_001, checksum, 0)),
                Err("invalid_beatmap_identity")
            ));
        }
    }
}

async fn session(state: &AppState, user: i64) -> String {
    let token = common::secret();
    sqlx::query("INSERT INTO lazer_sessions(id,user_id,access_hash,refresh_hash,access_expires_at,refresh_expires_at) VALUES($1,$2,$3,$4,now()+interval '1 hour',now()+interval '1 day')")
        .bind(uuid::Uuid::new_v4()).bind(user).bind(common::token_hash(&token))
        .bind(common::secret()).execute(&state.pool).await.unwrap();
    token
}

async fn setup() -> (
    Fixture,
    Discovery,
    String,
    String,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
) {
    let mut fixture = Fixture::new().await;
    sqlx::query("INSERT INTO user_roles SELECT $1,id FROM roles WHERE name='player'")
        .bind(fixture.user)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
    let token = session(&fixture.state, fixture.user).await;
    let service_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    fixture.state.sanctuary_url =
        Arc::new(format!("http://{}", service_listener.local_addr().unwrap()));
    fixture.state.service_key = Arc::new(SERVICE_KEY.to_owned());
    let stub = Discovery {
        state: fixture.state.clone(),
        replies: Arc::new(Mutex::new(HashMap::new())),
        hits: Arc::new(AtomicUsize::new(0)),
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        connections: Arc::new(Mutex::new(Vec::new())),
    };
    let service = Router::new()
        .route("/api/v1/internal/beatmaps/resolve", post(discover_stub))
        .with_state(stub.clone());
    let service_task = tokio::spawn(async move {
        axum::serve(service_listener, service).await.unwrap();
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let app = router().with_state(fixture.state.clone());
    let hub_task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (fixture, stub, base, token, service_task, hub_task)
}

async fn room_snapshot_for_test(state: &AppState, user: i64) -> Value {
    sqlx::query_scalar("SELECT to_jsonb(r) FROM lazer_rooms r JOIN lazer_room_members m ON m.room_id=r.id WHERE m.user_id=$1")
        .bind(user).fetch_one(&state.pool).await.unwrap()
}

async fn cleanup(
    fixture: Fixture,
    service: tokio::task::JoinHandle<()>,
    hub: tokio::task::JoinHandle<()>,
    stub: &Discovery,
    result: Result<(), Box<dyn std::any::Any + Send>>,
) {
    hub.abort();
    let _ = hub.await;
    service.abort();
    let _ = service.await;
    // Acknowledging close and joining the HTTP accept loop do not join Axum's
    // upgrade task or the disconnect task spawned by Lease::drop. The exact
    // Connection allocation remains alive until both finish. Wait for these
    // captured witnesses before closing the pool and destroying its schema.
    let connections = stub.connections.lock().await.clone();
    let shutdown = tokio::time::timeout(Duration::from_secs(5), async {
        while connections
            .iter()
            .any(|connection| connection.strong_count() != 0)
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        shutdown.is_ok(),
        "fixture websocket/disconnect handles still live before schema teardown: {:?}",
        connections
            .iter()
            .map(Weak::strong_count)
            .collect::<Vec<_>>()
    );
    fixture.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL pointing at disposable local PostgreSQL"]
async fn signalr_room_discovers_official_playlist_json_and_messagepack() {
    for binary in [false, true] {
        let (fixture, stub, base, token, service, hub) = setup().await;
        let result = std::panic::AssertUnwindSafe(async {
            let mut client = Client::open(&base, &token, binary, &stub).await;
            let first = stub.reply(7_001_001, Reply::Valid).await;
            let response = client
                .call(
                    "CreateRoom",
                    room_input(binary, item(binary, 7_001_001, &first, 0)),
                )
                .await;
            client.success(&response);
            let created = room_snapshot_for_test(&fixture.state, fixture.user).await;
            assert_eq!(created["playlist"][0]["starRating"], 1.5);
            assert_eq!(created["playlist"][0]["beatmapID"], 7_001_001);
            assert_eq!(created["playlist"][0]["beatmapChecksum"], first);
            assert_eq!(stub.hits.load(Ordering::SeqCst), 1);
            let response = client
                .call("AddPlaylistItem", item(binary, 7_001_001, &first, 0))
                .await;
            client.success(&response);
            assert_eq!(
                stub.hits.load(Ordering::SeqCst),
                1,
                "cached add must not call HTTP"
            );
            let second = stub.reply(7_001_002, Reply::Valid).await;
            let response = client
                .call("AddPlaylistItem", item(binary, 7_001_002, &second, 0))
                .await;
            client.success(&response);
            let third = stub.reply(7_001_003, Reply::Valid).await;
            let response = client
                .call("EditPlaylistItem", item(binary, 7_001_003, &third, 3))
                .await;
            client.success(&response);
            let response = client
                .call("EditPlaylistItem", item(binary, 7_001_003, &third, 3))
                .await;
            client.success(&response);
            assert_eq!(
                stub.hits.load(Ordering::SeqCst),
                3,
                "cached edit must not call HTTP"
            );
            let current = room_snapshot_for_test(&fixture.state, fixture.user).await;
            assert_eq!(current["playlist"].as_array().unwrap().len(), 3);
            assert_eq!(current["playlist"][2]["beatmapID"], 7_001_003);
            assert_eq!(current["playlist"][2]["ownerID"], fixture.user);
            assert_eq!(current["next_playlist_item_id"], 4);
            assert_eq!(
                sqlx::query_scalar::<_, i64>("SELECT count(*) FROM lazer_rooms")
                    .fetch_one(&fixture.state.pool)
                    .await
                    .unwrap(),
                1
            );
            client.close().await;
        })
        .catch_unwind()
        .await;
        cleanup(fixture, service, hub, &stub, result).await;
    }
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL pointing at disposable local PostgreSQL"]
async fn signalr_room_discovery_rejects_invalid_missing_and_withdrawn_maps_without_room_writes() {
    for binary in [false, true] {
        let (fixture, stub, base, token, service, hub) = setup().await;
        let result = std::panic::AssertUnwindSafe(async {
            let mut client = Client::open(&base, &token, binary, &stub).await;
            let response = client
                .call(
                    "CreateRoom",
                    room_input(binary, item(binary, fixture.native, DERIVED, 0)),
                )
                .await;
            client.success(&response);
            let before = room_snapshot_for_test(&fixture.state, fixture.user).await;
            let cases = [
                (Reply::Missing, "beatmap_not_found"),
                (Reply::Unavailable, "map_resolution_unavailable"),
                (Reply::WrongId, "beatmap_checksum_mismatch"),
                (Reply::WrongHash, "invalid_map_resolution_response"),
                (Reply::Withdrawn, "beatmap_withdrawn"),
                (Reply::WrongMode, "invalid_ruleset_conversion"),
            ];
            for (index, (behavior, error)) in cases.into_iter().enumerate() {
                let native = 7_002_000 + 10 * index as i64;
                let checksum = stub.reply(native, behavior).await;
                let target = if index % 2 == 0 {
                    "AddPlaylistItem"
                } else {
                    "EditPlaylistItem"
                };
                let response = client
                    .call(target, item(binary, native, &checksum, 1))
                    .await;
                client.failure(&response, error);
                assert_eq!(
                    room_snapshot_for_test(&fixture.state, fixture.user).await,
                    before
                );
                // Creation must fail for the same immutable selector too, and
                // must neither allocate another room nor attach its channel.
                let response = client
                    .call(
                        "CreateRoom",
                        room_input(binary, item(binary, native, &checksum, 0)),
                    )
                    .await;
                let cached_error = match behavior {
                    Reply::WrongHash => "beatmap_checksum_mismatch",
                    _ => error,
                };
                client.failure(&response, cached_error);
                assert_eq!(
                    sqlx::query_scalar::<_, i64>("SELECT count(*) FROM lazer_rooms")
                        .fetch_one(&fixture.state.pool)
                        .await
                        .unwrap(),
                    1
                );
                assert_eq!(
                    room_snapshot_for_test(&fixture.state, fixture.user).await,
                    before
                );
            }
            let hits = stub.hits.load(Ordering::SeqCst);
            let response = client
                .call(
                    "AddPlaylistItem",
                    item(binary, crate::native_maps::MAX_NATIVE_ID, DERIVED, 0),
                )
                .await;
            client.failure(&response, "beatmap_not_found");
            assert_eq!(
                stub.hits.load(Ordering::SeqCst),
                hits,
                "local misses must never discover upstream"
            );
            let response = client
                .call(
                    "EditPlaylistItem",
                    item(
                        binary,
                        fixture.native,
                        "ffffffffffffffffffffffffffffffff",
                        1,
                    ),
                )
                .await;
            client.failure(&response, "beatmap_checksum_mismatch");
            assert_eq!(
                stub.hits.load(Ordering::SeqCst),
                hits,
                "cached mismatches must never discover upstream"
            );
            assert_eq!(
                room_snapshot_for_test(&fixture.state, fixture.user).await,
                before
            );
            let mut bad_playlist = room_input(binary, item(binary, 7_099_001, DERIVED, 0));
            let second = item(binary, 7_099_002, "invalid-checksum", 0);
            if binary {
                bad_playlist[6].as_array_mut().unwrap().push(second);
            } else {
                bad_playlist["playlist"]
                    .as_array_mut()
                    .unwrap()
                    .push(second);
            }
            let response = client.call("CreateRoom", bad_playlist).await;
            client.failure(&response, "invalid_beatmap_identity");
            assert_eq!(
                stub.hits.load(Ordering::SeqCst),
                hits,
                "validate the entire playlist before upstream HTTP"
            );
            client.close().await;
        })
        .catch_unwind()
        .await;
        cleanup(fixture, service, hub, &stub, result).await;
    }
}

#[derive(Clone, Copy)]
enum Race {
    Revoke,
    Withdraw,
    TransferHost,
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL pointing at disposable local PostgreSQL"]
async fn signalr_room_discovery_revalidates_session_availability_and_authority_after_http() {
    for binary in [false, true] {
        for target in ["CreateRoom", "AddPlaylistItem", "EditPlaylistItem"] {
            for race in [Race::Revoke, Race::Withdraw, Race::TransferHost] {
                if target == "CreateRoom" && matches!(race, Race::TransferHost) {
                    continue;
                }
                let (fixture, stub, base, token, service, hub) = setup().await;
                let result = std::panic::AssertUnwindSafe(async {
                    let mut client = Client::open(&base, &token, binary, &stub).await;
                    let before = if target != "CreateRoom" {
                        let response = client.call("CreateRoom", room_input(binary, item(binary, fixture.native, DERIVED, 0))).await;
                        client.success(&response);
                        Some(room_snapshot_for_test(&fixture.state, fixture.user).await)
                    } else {
                        None
                    };
                    let native = 7_003_001;
                    let checksum = stub.reply(native, Reply::Paused).await;
                    let request_item = item(binary, native, &checksum, 1);
                    let input = if target == "CreateRoom" { room_input(binary, request_item) } else { request_item };
                    let request = tokio::spawn(async move {
                        let response = client.call(target, input).await;
                        (client, response)
                    });
                    tokio::time::timeout(Duration::from_secs(3), stub.started.notified()).await.unwrap();
                    // These complete while upstream HTTP is paused. A room,
                    // user or session lock held over discovery would deadlock.
                    tokio::time::timeout(Duration::from_secs(2), async {
                        match race {
                            Race::Revoke => {
                                sqlx::query("DELETE FROM lazer_sessions WHERE access_hash=$1")
                                    .bind(common::token_hash(&token)).execute(&fixture.state.pool).await.unwrap();
                            }
                            Race::Withdraw => {
                                sqlx::query("INSERT INTO beatmapset_availability(native_set_id,withdrawn,revision,reason,actor_id) VALUES($1,true,1,'Discovery race fixture',$2)")
                                    .bind(native + 1_000_000).bind(fixture.user).execute(&fixture.state.pool).await.unwrap();
                            }
                            Race::TransferHost => {
                                let user: i64 = sqlx::query_scalar("INSERT INTO users(username,email,password_hash,password_md5_hash) VALUES('NewFixtureHost','new-fixture-host@invalid.test','unused','unused') RETURNING id")
                                    .fetch_one(&fixture.state.pool).await.unwrap();
                                sqlx::query("UPDATE lazer_rooms SET host_id=$2 WHERE id=$1")
                                    .bind(before.as_ref().unwrap()["id"].as_i64().unwrap()).bind(user)
                                    .execute(&fixture.state.pool).await.unwrap();
                            }
                        }
                    }).await.expect("discovery must not hold mutation locks");
                    stub.release.notify_one();
                    let (client, response) = tokio::time::timeout(Duration::from_secs(5), request).await.unwrap().unwrap();
                    client.failure(&response, match race {
                        Race::Revoke => "session_revoked",
                        Race::Withdraw => "beatmap_withdrawn",
                        Race::TransferHost => "host_required",
                    });
                    assert_eq!(stub.hits.load(Ordering::SeqCst), 1);
                    if let Some(before) = before {
                        let after = room_snapshot_for_test(&fixture.state, fixture.user).await;
                        for key in ["playlist", "settings", "revision", "next_playlist_item_id", "updated_at"] {
                            assert_eq!(after[key], before[key], "room write after failed discovery: {key}");
                        }
                    } else {
                        assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM lazer_rooms")
                            .fetch_one(&fixture.state.pool).await.unwrap(), 0);
                        assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM lazer_room_members")
                            .fetch_one(&fixture.state.pool).await.unwrap(), 0);
                        assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM channels WHERE required_permission='lazer.room'")
                            .fetch_one(&fixture.state.pool).await.unwrap(), 0);
                    }
                    client.close().await;
                }).catch_unwind().await;
                cleanup(fixture, service, hub, &stub, result).await;
            }
        }
    }
}
