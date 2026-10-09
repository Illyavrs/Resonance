//! Original authenticated HTTP fixtures for native official catalogue discovery.
use super::*;
use axum::{body::Body, extract::State, http::Request, routing::post};
use futures_util::FutureExt;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{str::FromStr, sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{Mutex, Notify},
    task::JoinHandle,
};
use tower::ServiceExt;

const ACCESS: &str = "original-native-search-access";
const KEY: &str = "original-native-search-service-key-32-bytes";

fn result() -> Value {
    json!({"beatmapsets":[{"id":820001,"title":"搜尋 Fixture","beatmaps":[{"id":720001,"beatmapset_id":820001,"version":"Hard","mode_int":0,"difficulty_rating":4.2}]}],"total":51,"cursor":{"approved_date":"2026-01-01T00:00:00+00:00","id":820001},"cursor_string":"opaque-future-cursor"})
}

#[test]
fn native_search_encoding_duplicates_and_source_are_strict() {
    for raw in [
        "q=%",
        "q=%0",
        "q=%GG",
        "q=%FF",
        "q=%E6%98",
        "q=a&q=b",
        "cursor[id]=1&cursor[id]=2",
        "source=mirror",
        "source=local&source=official",
    ] {
        assert!(search_query(Some(raw.into())).is_err(), "{raw}");
    }
    assert!(search_query(Some("q=a".repeat(3000))).is_err());
    let query = search_query(Some("q=%E6%98%9F+%26+100%25&cursor%5Btitle.raw%5D=%E6%98%9F%2B%2F%3D&cursor%5Bid%5D=42&source=official".into())).unwrap();
    assert_eq!(query["q"], "星 & 100%");
    assert_eq!(query["cursor[title.raw]"], "星+/=");
    assert_eq!(query["cursor[id]"], "42");
}

#[test]
fn native_local_set_summary_does_not_promote_unranked_siblings() {
    let mut set = json!({"status":"ranked","beatmaps":[{"id":2000000001,"status":"ranked"},{"id":2000000002,"status":"wip"}]});
    summarize_local_set(&mut set).unwrap();
    assert_eq!(set["status"], "pending");
    assert_eq!(set["ranked"], 0);
    assert_eq!(set["beatmaps"][0]["status"], "ranked");
    assert_eq!(set["beatmaps"][1]["status"], "wip");
    set["beatmaps"][0]["status"] = json!("wip");
    summarize_local_set(&mut set).unwrap();
    assert_eq!(set["status"], "wip");
    assert_eq!(set["ranked"], -1);
}

#[test]
fn native_search_rejects_invalid_internal_identity_and_cursor_shape() {
    for (field, bad) in [
        ("beatmapsets", json!({})),
        ("total", json!(-1)),
        ("total", json!(2147483648i64)),
        ("cursor", json!("offset")),
        ("cursor_string", json!(42)),
    ] {
        let mut value = result();
        value[field] = bad;
        assert!(validate_official_search(&value).is_err(), "{field}");
    }
    for id in [0, 2_000_000_000, i64::MAX] {
        let mut value = result();
        value["beatmapsets"][0]["id"] = json!(id);
        assert!(validate_official_search(&value).is_err());
    }
    let mut value = result();
    value["beatmapsets"][0]["beatmaps"][0]["beatmapset_id"] = json!(820002);
    assert!(validate_official_search(&value).is_err());
    let mut value = result();
    let duplicate = value["beatmapsets"][0].clone();
    value["beatmapsets"].as_array_mut().unwrap().push(duplicate);
    assert!(validate_official_search(&value).is_err());
    assert!(validate_official_search(&result()).is_ok());
    assert!(
        validate_official_search(
            &json!({"beatmapsets":[],"total":0,"cursor":null,"cursor_string":null})
        )
        .is_ok()
    );
}

#[derive(Clone)]
struct Backend {
    reply: Arc<Mutex<(StatusCode, Vec<u8>)>>,
    calls: Arc<Mutex<Vec<Value>>>,
    pause: Arc<std::sync::atomic::AtomicBool>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

async fn search_backend(
    State(state): State<Backend>,
    headers: HeaderMap,
    Json(input): Json<Value>,
) -> Response {
    assert_eq!(headers["x-illya-service-key"], KEY);
    assert!(headers.get("authorization").is_none());
    assert_eq!(input.as_object().unwrap().len(), 2);
    assert!(input["user_id"].as_i64().unwrap() > 0);
    state.calls.lock().await.push(input);
    if state.pause.load(std::sync::atomic::Ordering::SeqCst) {
        state.entered.notify_one();
        tokio::time::timeout(Duration::from_secs(3), state.release.notified())
            .await
            .unwrap();
    }
    let (status, bytes) = state.reply.lock().await.clone();
    (status, bytes).into_response()
}

struct Fixture {
    control: PgPool,
    state: AppState,
    app: Router,
    schema: String,
    storage: std::path::PathBuf,
    backend: Backend,
    task: JoinHandle<()>,
    user: i64,
}

impl Fixture {
    async fn new() -> Self {
        let url = std::env::var("ILLYA_TEST_DATABASE_URL")
            .expect("owned disposable PostgreSQL URL required");
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
        let schema = format!("native_search_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&control)
            .await
            .unwrap();
        let options = PgConnectOptions::from_str(&url)
            .unwrap()
            .options([("search_path", schema.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
            .unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
        let user = sqlx::query_scalar::<_, i64>("INSERT INTO users(username,email,password_hash,password_md5_hash) VALUES('NativeSearchFixture','native-search@invalid.test','unused','unused') RETURNING id").fetch_one(&pool).await.unwrap();
        sqlx::query("INSERT INTO lazer_sessions(id,user_id,access_hash,refresh_hash,access_expires_at,refresh_expires_at) VALUES($1,$2,$3,$4,now()+interval '1 hour',now()+interval '1 day')")
            .bind(uuid::Uuid::new_v4()).bind(user).bind(common::token_hash(ACCESS)).bind(common::token_hash("original-search-refresh")).execute(&pool).await.unwrap();
        let backend = Backend {
            reply: Arc::new(Mutex::new((
                StatusCode::OK,
                result().to_string().into_bytes(),
            ))),
            calls: Default::default(),
            pause: Default::default(),
            entered: Default::default(),
            release: Default::default(),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/api/v1/internal/beatmapsets/search", post(search_backend))
            .with_state(backend.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let storage = std::env::temp_dir().join(&schema);
        tokio::fs::create_dir_all(&storage).await.unwrap();
        let storage = tokio::fs::canonicalize(storage).await.unwrap();
        let state = AppState {
            pool,
            storage: Arc::new(storage.clone()),
            sanctuary_url: Arc::new(origin),
            service_key: Arc::new(KEY.into()),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            limits: Default::default(),
            trusted_proxies: Default::default(),
            signalr: Arc::new(crate::signalr::Runtime::new()),
            lazer_chat: Arc::new(crate::lazer_chat::Runtime::new()),
        };
        let app = super::router().with_state(state.clone());
        Self {
            control,
            state,
            app,
            schema,
            storage,
            backend,
            task,
            user,
        }
    }
    async fn get(&self, query: &str, authenticated: bool) -> (StatusCode, Value) {
        let mut request = Request::builder().uri(format!("/api/v2/beatmapsets/search?{query}"));
        if authenticated {
            request = request.header("authorization", format!("Bearer {ACCESS}"));
        }
        let response = self
            .app
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 5 * 1024 * 1024)
            .await
            .unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }
    async fn cleanup(self) {
        self.task.abort();
        self.state.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.control)
            .await
            .unwrap();
        self.control.close().await;
        tokio::fs::remove_dir_all(self.storage).await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires an owned disposable PostgreSQL 17 database"]
async fn native_official_search_auth_query_forwarding_and_no_imports() {
    let f = Fixture::new().await;
    let check = std::panic::AssertUnwindSafe(async {
        assert_eq!(f.get("", false).await.0, StatusCode::UNAUTHORIZED);
        assert!(f.backend.calls.lock().await.is_empty());
        assert_eq!(f.get("q=a&q=b", true).await.0, StatusCode::BAD_REQUEST);
        assert!(f.backend.calls.lock().await.is_empty());
        let raw = "q=%E6%98%9F+%26+100%25&s=leaderboard&m=3&sort=relevance_desc&nsfw=false&c=featured_artists&cursor%5Btitle.raw%5D=%E6%98%9F%2B%2F%3D&cursor%5Bid%5D=42&source=official";
        let (status, value) = f.get(raw, true).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value, result());
        let calls = f.backend.calls.lock().await;
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["user_id"], f.user);
        let forwarded: HashMap<String, String> = serde_urlencoded::from_str(calls[0]["query"].as_str().unwrap()).unwrap();
        assert_eq!(forwarded["q"], "星 & 100%");
        assert_eq!(forwarded["cursor[title.raw]"], "星+/=");
        assert_eq!(forwarded["cursor[id]"], "42");
        assert!(!forwarded.contains_key("source"));
        drop(calls);
        assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM beatmaps").fetch_one(&f.state.pool).await.unwrap(), 0);
        assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM audit_events").fetch_one(&f.state.pool).await.unwrap(), 0);
    }).catch_unwind().await;
    f.cleanup().await;
    if let Err(error) = check {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires an owned disposable PostgreSQL 17 database"]
async fn native_official_search_failures_never_fall_back_to_local_catalogue() {
    let f = Fixture::new().await;
    let check = std::panic::AssertUnwindSafe(async {
        for (upstream, expected) in [
            (StatusCode::BAD_REQUEST, StatusCode::BAD_REQUEST),
            (StatusCode::UNAUTHORIZED, StatusCode::UNAUTHORIZED),
            (StatusCode::TOO_MANY_REQUESTS, StatusCode::TOO_MANY_REQUESTS),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (StatusCode::INTERNAL_SERVER_ERROR, StatusCode::BAD_GATEWAY),
        ] {
            *f.backend.reply.lock().await =
                (upstream, b"private synthetic provider failure".to_vec());
            let (status, value) = f.get("q=Unknown", true).await;
            assert_eq!(status, expected);
            assert!(value.get("beatmapsets").is_none());
            assert!(!value.to_string().contains("provider failure"));
        }
        *f.backend.reply.lock().await = (StatusCode::OK, vec![b'x'; 4 * 1024 * 1024 + 1]);
        assert_eq!(f.get("", true).await.0, StatusCode::BAD_GATEWAY);
        *f.backend.reply.lock().await = (
            StatusCode::OK,
            b"{\"beatmapsets\":[],\"total\":0,\"cursor\":\"bad\"}".to_vec(),
        );
        assert_eq!(f.get("", true).await.0, StatusCode::BAD_GATEWAY);
        let before = f.backend.calls.lock().await.len();
        assert_eq!(
            f.get("source=local", true).await,
            (
                StatusCode::OK,
                json!({"beatmapsets":[],"total":0,"cursor":null,"cursor_string":null,"error":null})
            )
        );
        assert_eq!(f.backend.calls.lock().await.len(), before);
        for query in [
            "source=local&m=banana",
            "source=local&s=invalid",
            "source=local&cursor_string=opaque",
            "source=local&sort=ranked_desc",
        ] {
            assert_eq!(
                f.get(query, true).await.0,
                StatusCode::BAD_REQUEST,
                "{query}"
            );
        }
    })
    .catch_unwind()
    .await;
    f.cleanup().await;
    if let Err(error) = check {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires an owned disposable PostgreSQL 17 database"]
async fn native_official_search_rechecks_token_after_success_and_upstream_error() {
    let f = Fixture::new().await;
    let check = std::panic::AssertUnwindSafe(async {
        for status in [StatusCode::OK, StatusCode::TOO_MANY_REQUESTS] {
            sqlx::query("UPDATE lazer_sessions SET access_expires_at=now()+interval '1 hour'")
                .execute(&f.state.pool)
                .await
                .unwrap();
            *f.backend.reply.lock().await = (status, result().to_string().into_bytes());
            f.backend
                .pause
                .store(true, std::sync::atomic::Ordering::SeqCst);
            let get = f.get("", true);
            let revoke = async {
                tokio::time::timeout(Duration::from_secs(2), f.backend.entered.notified())
                    .await
                    .unwrap();
                sqlx::query(
                    "UPDATE lazer_sessions SET access_expires_at=now()-interval '1 second'",
                )
                .execute(&f.state.pool)
                .await
                .unwrap();
                f.backend.release.notify_one();
            };
            let (response, ()) = tokio::join!(get, revoke);
            assert_eq!(response.0, StatusCode::UNAUTHORIZED);
            assert!(response.1.get("beatmapsets").is_none());
        }
    })
    .catch_unwind()
    .await;
    f.cleanup().await;
    if let Err(error) = check {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires an owned disposable PostgreSQL 17 database"]
async fn native_official_search_rechecks_withdrawal_after_backend_wait() {
    let f = Fixture::new().await;
    let check = std::panic::AssertUnwindSafe(async {
        f.backend.pause.store(true, std::sync::atomic::Ordering::SeqCst);
        let get = f.get("", true);
        let withdraw = async {
            tokio::time::timeout(Duration::from_secs(2), f.backend.entered.notified()).await.unwrap();
            sqlx::query("INSERT INTO beatmapset_availability(native_set_id,withdrawn,revision,reason,actor_id) VALUES(820001,true,1,'Original native search fixture',$1)")
                .bind(f.user).execute(&f.state.pool).await.unwrap();
            f.backend.release.notify_one();
        };
        let (response, ()) = tokio::join!(get, withdraw);
        assert_eq!(response.0, StatusCode::OK);
        assert_eq!(response.1["beatmapsets"], json!([]));
        assert_eq!(response.1["total"], 51);
        assert_eq!(response.1["cursor"], result()["cursor"]);
        f.backend.pause.store(false, std::sync::atomic::Ordering::SeqCst);
        sqlx::query("UPDATE beatmapset_availability SET withdrawn=false,revision=revision+1 WHERE native_set_id=820001")
            .execute(&f.state.pool).await.unwrap();
        assert_eq!(f.get("", true).await, (StatusCode::OK, result()));
    }).catch_unwind().await;
    f.cleanup().await;
    if let Err(error) = check {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires an owned disposable PostgreSQL 17 database"]
async fn native_local_graveyard_and_wip_resolve_with_negative_wire_ranks() {
    let f = Fixture::new().await;
    let check = std::panic::AssertUnwindSafe(async {
        use argon2::{Argon2, PasswordHasher, password_hash::SaltString};

        let digest = "11111111111111111111111111111111";
        let salt = SaltString::encode_b64(b"original-stable-status-fixture").unwrap();
        let credential = Argon2::default()
            .hash_password(digest.as_bytes(), &salt)
            .unwrap()
            .to_string();
        sqlx::query("UPDATE users SET password_md5_hash=$2 WHERE id=$1")
            .bind(f.user).bind(credential).execute(&f.state.pool).await.unwrap();
        let stable = crate::stable::router().with_state(f.state.clone());
        for (index, status, rank) in [(0, "graveyard", -2), (1, "wip", -1)] {
            let hash = format!("{index:032x}");
            let id: i64 = sqlx::query_scalar("INSERT INTO beatmaps(title,artist,creator,version,mode,status,source,stars,md5,osu_path) VALUES('Original native status','Fixture','Fixture','Hard',0,$1,'local',1,$2,'fixture.osu') RETURNING id")
                .bind(status).bind(&hash).fetch_one(&f.state.pool).await.unwrap();
            assert_eq!(crate::native_maps::resolve_checksum(&f.state, f.user, &hash).await.unwrap(), Some(id));
            let value = crate::native_maps::map_json(&f.state, id).await.unwrap();
            assert_eq!(value["status"], status);
            assert_eq!(value["ranked"], rank);
            assert_eq!(value["beatmapset"]["status"], status);
            let (search_status, page) = f.get(&format!("source=local&s={status}"), true).await;
            assert_eq!(search_status, StatusCode::OK);
            assert_eq!(page["total"], 1);
            assert_eq!(page["beatmapsets"].as_array().unwrap().len(), 1);
            assert_eq!(page["beatmapsets"][0]["status"], status);
            assert_eq!(page["beatmapsets"][0]["beatmaps"][0]["id"], value["id"]);
            // Stable's submitted-unranked header must not receive lazer's negative ranks.
            let response = stable.clone().oneshot(Request::builder()
                .uri(format!("/web/osu-osz2-getscores.php?c={hash}&m=0&v=1&u=NativeSearchFixture&ha={digest}"))
                .body(Body::empty()).unwrap()).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = axum::body::to_bytes(response.into_body(), 512 * 1024).await.unwrap();
            let text = std::str::from_utf8(&bytes).unwrap();
            assert!(text.starts_with(&format!("0|false|{}|{}|", value["id"].as_i64().unwrap(), value["beatmapset"]["id"].as_i64().unwrap())));
        }
        assert!(f.backend.calls.lock().await.is_empty());
    }).catch_unwind().await;
    f.cleanup().await;
    if let Err(error) = check {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires an owned disposable PostgreSQL 17 database"]
async fn native_set_detail_applies_private_status_per_difficulty_without_promoting_siblings() {
    let f = Fixture::new().await;
    let check = std::panic::AssertUnwindSafe(async {
        for (id, status) in [(720001, "ranked"), (720002, "wip")] {
            sqlx::query("INSERT INTO beatmap_status_overrides(native_id,native_set_id,status,actor_id,reason) VALUES($1,820001,$2,$3,'Original native difficulty fixture')")
                .bind(id).bind(status).bind(f.user).execute(&f.state.pool).await.unwrap();
        }
        let original = json!({"id":820001,"status":"pending","ranked":0,"beatmaps":[{"id":720001,"beatmapset_id":820001,"status":"pending","ranked":0},{"id":720002,"beatmapset_id":820001,"status":"pending","ranked":0},{"id":720003,"beatmapset_id":820001,"status":"pending","ranked":0}]});
        let mut value = original.clone();
        apply_private_difficulty_statuses(&f.state, 820001, &mut value).await.unwrap();
        assert_eq!(value["status"], "pending");
        assert_eq!(value["beatmaps"][0]["status"], "ranked");
        assert_eq!(value["beatmaps"][0]["ranked"], 1);
        assert_eq!(value["beatmaps"][1]["status"], "wip");
        assert_eq!(value["beatmaps"][1]["ranked"], -1);
        assert_eq!(value["beatmaps"][2]["status"], "pending");
        sqlx::query("UPDATE beatmap_status_overrides SET status='graveyard' WHERE native_id=720001").execute(&f.state.pool).await.unwrap();
        apply_private_difficulty_statuses(&f.state, 820001, &mut value).await.unwrap();
        assert_eq!(value["beatmaps"][0]["status"], "graveyard");
        assert_eq!(value["beatmaps"][0]["ranked"], -2);
        assert_eq!(value["beatmaps"][1]["status"], "wip");
        sqlx::query("UPDATE beatmap_status_overrides SET status='loved'").execute(&f.state.pool).await.unwrap();
        sqlx::query("INSERT INTO beatmap_status_overrides(native_id,native_set_id,status,actor_id,reason) VALUES(720003,820001,'loved',$1,'Original native difficulty fixture')")
            .bind(f.user).execute(&f.state.pool).await.unwrap();
        apply_private_difficulty_statuses(&f.state, 820001, &mut value).await.unwrap();
        assert_eq!(value["status"], "loved");
        assert_eq!(value["ranked"], 4);
        assert!(value["beatmaps"].as_array().unwrap().iter().all(|map| map["status"] == "loved"));
        assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM beatmaps").fetch_one(&f.state.pool).await.unwrap(), 0);
        assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM audit_events").fetch_one(&f.state.pool).await.unwrap(), 0);
    }).catch_unwind().await;
    f.cleanup().await;
    if let Err(error) = check {
        std::panic::resume_unwind(error);
    }
}
