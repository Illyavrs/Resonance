//! Original service-boundary transport fixtures. Sanctuary owns ZIP member
//! verification; these tests exercise native auth, bounded delivery and races.
use super::*;
use crate::{beatmaps, stable};
use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
use axum::{
    Json, Router,
    body::Body,
    extract::State,
    http::{HeaderMap, Request},
    response::{IntoResponse, Response},
    routing::post,
};
use futures_util::FutureExt;
use serde_json::{Value, json};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    net::TcpListener,
    sync::{Mutex, Notify},
    task::JoinHandle,
};
use tower::ServiceExt;

const SET: i64 = 850001;
const KEY: &str = "native-archive-original-service-key-32-bytes";
const ACCESS: &str = "native-archive-original-access";
const USER: &str = "NativeArchiveFixture";
const PASSWORD: &str = "original-native-archive-password";
const PRIVATE: &str = "PRIVATE_INTERNAL_ARCHIVE_MARKER";
// Transport fixture bytes only; the Sanctuary provider has separate complete
// ZIP/manifest tests. This boundary must preserve these verified bytes exactly.
const BYTES: &[u8] = b"PK\x03\x04original native archive boundary transport fixture";

#[test]
fn native_archive_transport_headers_and_service_errors_are_strict() {
    let mut headers = reqwest::header::HeaderMap::new();
    assert!(response_digest(&headers).is_err());
    headers.insert("content-type", "application/x-osu-archive".parse().unwrap());
    headers.insert("x-illya-archive-sha256", "a".repeat(64).parse().unwrap());
    assert_eq!(response_digest(&headers).unwrap(), "a".repeat(64));
    let filename =
        illya_domain::download_filename::archive_filename(SET, "Fixture artist", "Fixture title")
            .unwrap();
    let disposition = illya_domain::download_filename::attachment(&filename);
    headers.insert("content-disposition", disposition.parse().unwrap());
    assert_eq!(response_name(&headers, SET).unwrap(), disposition);
    assert!(response_name(&headers, SET + 1).is_err());
    headers.insert(
        "content-disposition",
        "attachment; filename=untrusted.osz".parse().unwrap(),
    );
    assert!(response_name(&headers, SET).is_err());
    for bad in [
        "A".repeat(64),
        "g".repeat(64),
        "a".repeat(63),
        "a".repeat(65),
    ] {
        headers.insert("x-illya-archive-sha256", bad.parse().unwrap());
        assert!(response_digest(&headers).is_err());
    }
    for status in [
        StatusCode::FOUND,
        StatusCode::TEMPORARY_REDIRECT,
        StatusCode::INTERNAL_SERVER_ERROR,
        StatusCode::IM_A_TEAPOT,
    ] {
        assert_eq!(service_error(status).0, StatusCode::BAD_GATEWAY);
        assert_eq!(service_error(status).1, "archive_acquisition_failed");
    }
}

#[derive(Clone)]
enum Reply {
    Bytes,
    DigestMismatch,
    Html,
    OversizedLength,
    OversizedStream,
    Redirect,
    Error(StatusCode),
}

#[derive(Clone)]
struct Backend {
    reply: Arc<Mutex<Reply>>,
    calls: Arc<Mutex<Vec<Value>>>,
    metadata_calls: Arc<Mutex<Vec<Value>>>,
    trap_calls: Arc<Mutex<usize>>,
    pause: Arc<AtomicBool>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    metadata_available: Arc<AtomicBool>,
    origin: String,
}

fn metadata(available: bool) -> Value {
    json!({"id":SET,"artist":"Fixture artist","artist_unicode":"Fixture artist","title":"Fixture title","title_unicode":"Fixture title","creator":"Fixture mapper","status":"ranked","bpm":160.0,"source":"","tags":"fixture","nsfw":false,"submitted_date":"2020-01-01T00:00:00Z","last_updated":"2020-01-02T00:00:00Z","ranked_date":null,"genre":{"id":1,"name":"Unspecified"},"language":{"id":1,"name":"Unspecified"},"download_disabled":!available,"archive_download_available":available,"beatmaps":[{"id":750001,"beatmapset_id":SET,"checksum":"0123456789abcdef0123456789abcdef","version":"Original difficulty","mode_int":0,"status":"ranked","last_updated":"2020-01-02T00:00:00Z","difficulty_rating":4.2,"bpm":160.0,"total_length":120,"hit_length":100,"max_combo":300,"ar":9.0,"cs":4.0,"od":8.0,"hp":6.0,"count_circles":50,"count_sliders":50,"count_spinners":0}]})
}

async fn archive_backend(
    State(state): State<Backend>,
    headers: HeaderMap,
    Json(input): Json<Value>,
) -> Response {
    assert_eq!(headers["x-illya-service-key"], KEY);
    assert!(headers.get("authorization").is_none());
    assert!(headers.get("cookie").is_none());
    assert_eq!(input["official_set_id"], SET);
    assert!(
        input
            .as_object()
            .unwrap()
            .keys()
            .all(|key| matches!(key.as_str(), "official_set_id" | "user_id"))
    );
    state.calls.lock().await.push(input);
    if state.pause.load(Ordering::SeqCst) {
        state.entered.notify_one();
        tokio::time::timeout(Duration::from_secs(5), state.release.notified())
            .await
            .expect("fixture releases its provider response");
    }
    let digest = format!("{:x}", Sha256::digest(BYTES));
    let disposition = illya_domain::download_filename::attachment(
        &illya_domain::download_filename::archive_filename(SET, "Fixture artist", "Fixture title")
            .unwrap(),
    );
    match state.reply.lock().await.clone() {
        Reply::Bytes => (
            [
                ("content-type", "application/x-osu-archive"),
                ("x-illya-archive-sha256", digest.as_str()),
                ("content-disposition", disposition.as_str()),
            ],
            BYTES,
        )
            .into_response(),
        Reply::DigestMismatch => (
            [
                ("content-type", "application/x-osu-archive"),
                ("x-illya-archive-sha256", "0".repeat(64).as_str()),
                ("content-disposition", disposition.as_str()),
            ],
            BYTES,
        )
            .into_response(),
        Reply::Html => (
            [
                ("content-type", "text/html"),
                ("x-illya-archive-sha256", digest.as_str()),
            ],
            PRIVATE,
        )
            .into_response(),
        Reply::OversizedLength => Response::builder()
            .status(200)
            .header("content-type", "application/x-osu-archive")
            .header("x-illya-archive-sha256", digest)
            .header("content-disposition", disposition)
            .header("content-length", (MAX_PROVIDER_BYTES + 1).to_string())
            .body(Body::from_stream(futures_util::stream::once(async {
                Ok::<_, std::io::Error>(b"x".as_slice())
            })))
            .unwrap(),
        Reply::OversizedStream => {
            let stream = futures_util::stream::unfold(0, |index| async move {
                if index < 65 {
                    Some((Ok::<_, std::io::Error>(vec![0; 1024 * 1024]), index + 1))
                } else {
                    None
                }
            });
            Response::builder()
                .status(200)
                .header("content-type", "application/x-osu-archive")
                .header("x-illya-archive-sha256", digest)
                .header("content-disposition", disposition)
                .body(Body::from_stream(stream))
                .unwrap()
        }
        Reply::Redirect => (
            StatusCode::TEMPORARY_REDIRECT,
            [("location", format!("{}/provider-trap", state.origin))],
            PRIVATE,
        )
            .into_response(),
        Reply::Error(status) => (status, PRIVATE).into_response(),
    }
}

async fn metadata_backend(
    State(state): State<Backend>,
    headers: HeaderMap,
    Json(input): Json<Value>,
) -> Json<Value> {
    assert_eq!(headers["x-illya-service-key"], KEY);
    assert!(headers.get("authorization").is_none());
    assert!(input["user_id"].as_i64().unwrap() > 0);
    state.metadata_calls.lock().await.push(input);
    Json(metadata(state.metadata_available.load(Ordering::SeqCst)))
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
        let schema = format!("native_archive_{}", uuid::Uuid::new_v4().simple());
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
        let salt = SaltString::encode_b64(b"native-archive-original-salt").unwrap();
        let password = Argon2::default()
            .hash_password(format!("{:x}", md5::compute(PASSWORD)).as_bytes(), &salt)
            .unwrap()
            .to_string();
        let user = sqlx::query_scalar::<_,i64>("INSERT INTO users(username,email,password_hash,password_md5_hash) VALUES($1,'native-archive@invalid.test','unused',$2) RETURNING id").bind(USER).bind(password).fetch_one(&pool).await.unwrap();
        sqlx::query("INSERT INTO lazer_sessions(id,user_id,access_hash,refresh_hash,access_expires_at,refresh_expires_at) VALUES($1,$2,$3,$4,now()+interval '1 hour',now()+interval '1 day')").bind(uuid::Uuid::new_v4()).bind(user).bind(common::token_hash(ACCESS)).bind(common::token_hash("native-archive-refresh")).execute(&pool).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let backend = Backend {
            reply: Arc::new(Mutex::new(Reply::Bytes)),
            calls: Default::default(),
            metadata_calls: Default::default(),
            trap_calls: Default::default(),
            pause: Default::default(),
            entered: Default::default(),
            release: Default::default(),
            metadata_available: Arc::new(AtomicBool::new(true)),
            origin: origin.clone(),
        };
        let service = Router::new()
            .route(
                "/api/v1/internal/beatmapsets/archive",
                post(archive_backend),
            )
            .route(
                "/api/v1/internal/beatmapsets/metadata",
                post(metadata_backend),
            )
            .route(
                "/provider-trap",
                post(|State(state): State<Backend>| async move {
                    *state.trap_calls.lock().await += 1;
                    PRIVATE
                }),
            )
            .with_state(backend.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, service).await.unwrap();
        });
        let storage = std::env::temp_dir().join(&schema);
        tokio::fs::create_dir_all(storage.join("maps"))
            .await
            .unwrap();
        let storage = tokio::fs::canonicalize(storage).await.unwrap();
        let state = AppState {
            pool,
            storage: Arc::new(storage.clone()),
            sanctuary_url: Arc::new(origin),
            service_key: Arc::new(KEY.into()),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
            limits: Default::default(),
            trusted_proxies: Default::default(),
            signalr: Arc::new(crate::signalr::Runtime::new()),
            lazer_chat: Arc::new(crate::lazer_chat::Runtime::new()),
        };
        let app = Router::new()
            .merge(beatmaps::router())
            .merge(stable::router())
            .with_state(state.clone());
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

    async fn get(&self, path: &str, authenticated: bool) -> (StatusCode, HeaderMap, Vec<u8>) {
        let mut request = Request::builder().uri(path);
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
        let headers = response.headers().clone();
        let bytes = axum::body::to_bytes(response.into_body(), 104_857_601)
            .await
            .unwrap()
            .to_vec();
        (status, headers, bytes)
    }

    async fn retain(&self, body: &[u8]) {
        tokio::fs::write(self.storage.join("maps/retained.osz"), body)
            .await
            .unwrap();
        sqlx::query("INSERT INTO beatmaps(official_id,official_set_id,title,artist,creator,version,mode,status,source,stars,md5,osu_path,archive_path) VALUES(750001,$1,'Retained fixture','Fixture','Fixture','Original',0,'ranked','official',4.2,'0123456789abcdef0123456789abcdef','unused.osu','retained.osz')").bind(SET).execute(&self.state.pool).await.unwrap();
    }

    async fn cleanup(self) {
        self.backend.release.notify_one();
        self.task.abort();
        let _ = self.task.await;
        self.state.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.control)
            .await
            .unwrap();
        self.control.close().await;
        tokio::fs::remove_dir_all(self.storage).await.unwrap();
    }
}

macro_rules! database_test {
    ($name:ident, $fixture:ident, $body:block) => {
        #[tokio::test]
        #[ignore = "requires an owned disposable PostgreSQL 17 database"]
        async fn $name() {
            let $fixture = Fixture::new().await;
            let result = std::panic::AssertUnwindSafe(async $body).catch_unwind().await;
            $fixture.cleanup().await;
            if let Err(error) = result { std::panic::resume_unwind(error); }
        }
    };
}

database_test!(
    native_archive_anonymous_stable_and_authenticated_lazer_keep_native_origin,
    f,
    {
        assert_eq!(
            f.get(&format!("/api/v2/beatmapsets/{SET}/download"), false)
                .await
                .0,
            StatusCode::UNAUTHORIZED
        );
        assert!(f.backend.calls.lock().await.is_empty());
        for (path, auth) in [
            (format!("/d/{SET}n"), false),
            (format!("/api/v2/beatmapsets/{SET}/download"), true),
        ] {
            let (status, headers, bytes) = f.get(&path, auth).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(bytes, BYTES);
            assert_eq!(headers["content-type"], "application/x-osu-beatmap-archive");
            assert_eq!(headers["cache-control"], "private, no-store");
            assert_eq!(
                headers["content-disposition"],
                illya_domain::download_filename::attachment(
                    &illya_domain::download_filename::archive_filename(
                        SET,
                        "Fixture artist",
                        "Fixture title"
                    )
                    .unwrap()
                )
            );
            assert!(headers.get("location").is_none());
        }
        let calls = f.backend.calls.lock().await;
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].as_object().unwrap().len(), 1);
        assert_eq!(calls[1]["user_id"], f.user);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM beatmaps")
                .fetch_one(&f.state.pool)
                .await
                .unwrap(),
            0,
            "transient acquisition never publishes map rows"
        );
    }
);

database_test!(
    native_archive_retained_and_local_paths_do_not_depend_on_provider,
    f,
    {
        f.retain(b"immutable retained fixture bytes").await;
        *f.backend.reply.lock().await = Reply::Error(StatusCode::SERVICE_UNAVAILABLE);
        for (path, auth) in [
            (format!("/d/{SET}"), false),
            (format!("/api/v2/beatmapsets/{SET}/download"), true),
        ] {
            let (status, _, bytes) = f.get(&path, auth).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(bytes, b"immutable retained fixture bytes");
        }
        assert_eq!(f.get("/d/2000000123", false).await.0, StatusCode::NOT_FOUND);
        assert!(f.backend.calls.lock().await.is_empty());
        assert!(f.backend.metadata_calls.lock().await.is_empty());
    }
);

database_test!(
    native_archive_rejects_redirects_without_forwarding_service_key,
    f,
    {
        *f.backend.reply.lock().await = Reply::Redirect;
        let (status, headers, bytes) = f
            .get(&format!("/api/v2/beatmapsets/{SET}/download"), true)
            .await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert!(headers.get("location").is_none());
        assert!(!String::from_utf8(bytes).unwrap().contains(PRIVATE));
        assert_eq!(*f.backend.trap_calls.lock().await, 0);
    }
);

database_test!(
    native_archive_provider_policy_errors_do_not_expose_internal_payloads,
    f,
    {
        for upstream in [
            StatusCode::FORBIDDEN,
            StatusCode::NOT_FOUND,
            StatusCode::GONE,
            StatusCode::CONFLICT,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            *f.backend.reply.lock().await = Reply::Error(upstream);
            let (status, headers, bytes) = f
                .get(&format!("/api/v2/beatmapsets/{SET}/download"), true)
                .await;
            assert_eq!(status, upstream);
            assert!(headers.get("location").is_none());
            assert!(!String::from_utf8(bytes).unwrap().contains(PRIVATE));
        }
    }
);

database_test!(
    native_archive_bounds_and_digest_reject_invalid_service_bodies,
    f,
    {
        for reply in [
            Reply::DigestMismatch,
            Reply::Html,
            Reply::OversizedLength,
            Reply::OversizedStream,
        ] {
            *f.backend.reply.lock().await = reply;
            let (status, _, bytes) = f.get(&format!("/d/{SET}"), false).await;
            assert!(matches!(
                status,
                StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE
            ));
            assert!(!String::from_utf8(bytes).unwrap().contains(PRIVATE));
        }
    }
);

database_test!(
    native_archive_withdrawal_during_provider_wait_prevents_delivery,
    f,
    {
        f.backend.pause.store(true, Ordering::SeqCst);
        let path = format!("/api/v2/beatmapsets/{SET}/download");
        let pending = f.get(&path, true);
        tokio::pin!(pending);
        tokio::select! { _ = f.backend.entered.notified() => {}, _ = &mut pending => panic!("fixture pauses before download") }
        sqlx::query("INSERT INTO beatmapset_availability(native_set_id,withdrawn,revision,reason,actor_id) VALUES($1,true,1,'Archive fixture withdrawal',$2)").bind(SET).bind(f.user).execute(&f.state.pool).await.unwrap();
        f.backend.release.notify_one();
        let (status, _, bytes) = pending.await;
        assert_eq!(status, StatusCode::GONE);
        assert_ne!(bytes, BYTES);
    }
);

database_test!(
    native_archive_revoked_bearer_takes_precedence_over_provider_failure,
    f,
    {
        f.backend.pause.store(true, Ordering::SeqCst);
        *f.backend.reply.lock().await = Reply::Error(StatusCode::SERVICE_UNAVAILABLE);
        let path = format!("/api/v2/beatmapsets/{SET}/download");
        let pending = f.get(&path, true);
        tokio::pin!(pending);
        tokio::select! { _ = f.backend.entered.notified() => {}, _ = &mut pending => panic!("fixture pauses before failure") }
        sqlx::query("DELETE FROM lazer_sessions WHERE user_id=$1")
            .bind(f.user)
            .execute(&f.state.pool)
            .await
            .unwrap();
        f.backend.release.notify_one();
        let (status, _, bytes) = pending.await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(!String::from_utf8(bytes).unwrap().contains(PRIVATE));
    }
);

database_test!(
    native_archive_restricted_account_during_provider_wait_prevents_delivery,
    f,
    {
        f.backend.pause.store(true, Ordering::SeqCst);
        let path = format!("/api/v2/beatmapsets/{SET}/download");
        let pending = f.get(&path, true);
        tokio::pin!(pending);
        tokio::select! { _ = f.backend.entered.notified() => {}, _ = &mut pending => panic!("fixture pauses before account check") }
        sqlx::query("UPDATE users SET is_restricted=true WHERE id=$1")
            .bind(f.user)
            .execute(&f.state.pool)
            .await
            .unwrap();
        f.backend.release.notify_one();
        let (status, _, bytes) = pending.await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_ne!(bytes, BYTES);
    }
);

database_test!(
    native_archive_concurrent_retained_publication_wins_over_transient_bytes,
    f,
    {
        f.backend.pause.store(true, Ordering::SeqCst);
        let path = format!("/d/{SET}");
        let pending = f.get(&path, false);
        tokio::pin!(pending);
        tokio::select! { _ = f.backend.entered.notified() => {}, _ = &mut pending => panic!("fixture pauses before publication") }
        f.retain(b"later authoritative retained bytes").await;
        f.backend.release.notify_one();
        let (status, _, bytes) = pending.await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(bytes, b"later authoritative retained bytes");
    }
);

database_test!(
    native_archive_concurrent_retained_publication_wins_over_provider_failure,
    f,
    {
        f.backend.pause.store(true, Ordering::SeqCst);
        *f.backend.reply.lock().await = Reply::Error(StatusCode::SERVICE_UNAVAILABLE);
        let path = format!("/d/{SET}");
        let pending = f.get(&path, false);
        tokio::pin!(pending);
        tokio::select! { _ = f.backend.entered.notified() => {}, _ = &mut pending => panic!("fixture pauses before failed provider response") }
        f.retain(b"retained package survives provider failure")
            .await;
        f.backend.release.notify_one();
        let (status, _, bytes) = pending.await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(bytes, b"retained package survives provider failure");
    }
);

database_test!(
    native_archive_stable_withdrawal_takes_precedence_over_provider_failure,
    f,
    {
        f.backend.pause.store(true, Ordering::SeqCst);
        *f.backend.reply.lock().await = Reply::Error(StatusCode::SERVICE_UNAVAILABLE);
        let path = format!("/d/{SET}");
        let pending = f.get(&path, false);
        tokio::pin!(pending);
        tokio::select! { _ = f.backend.entered.notified() => {}, _ = &mut pending => panic!("fixture pauses before failed provider response") }
        sqlx::query("INSERT INTO beatmapset_availability(native_set_id,withdrawn,revision,reason,actor_id) VALUES($1,true,1,'Archive failure fixture withdrawal',$2)").bind(SET).bind(f.user).execute(&f.state.pool).await.unwrap();
        f.backend.release.notify_one();
        let (status, _, bytes) = pending.await;
        assert_eq!(status, StatusCode::GONE);
        assert_ne!(bytes, BYTES);
    }
);

database_test!(
    native_archive_metadata_available_and_disabled_are_truthful_without_publication,
    f,
    {
        for available in [true, false] {
            f.backend
                .metadata_available
                .store(available, Ordering::SeqCst);
            let (status, _, bytes) = f.get(&format!("/api/v2/beatmapsets/{SET}"), true).await;
            assert_eq!(status, StatusCode::OK);
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["download_disabled"], !available);
            assert_eq!(body["availability"]["download_disabled"], !available);
            assert_eq!(body["archive_download_available"], available);
            assert_eq!(body["beatmaps"].as_array().unwrap().len(), 1);
            assert_eq!(body["beatmaps"][0]["id"], 750001);
        }
        assert!(
            f.backend.calls.lock().await.is_empty(),
            "browsing never fetches a ZIP"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM beatmaps")
                .fetch_one(&f.state.pool)
                .await
                .unwrap(),
            0
        );
    }
);

database_test!(
    native_archive_stable_explicit_set_lookup_can_offer_configured_acquisition,
    f,
    {
        let password = format!("{:x}", md5::compute(PASSWORD));
        let path = format!("/web/osu-search-set.php?s={SET}&u={USER}&h={password}");
        let (status, _, bytes) = f.get(&path, false).await;
        assert_eq!(status, StatusCode::OK);
        let line = String::from_utf8(bytes).unwrap();
        assert!(line.starts_with(&format!(
            "{SET}.osz|Fixture artist|Fixture title|Fixture mapper|1|"
        )));
        assert!(line.ends_with("[4.2*] Original difficulty@0"));
        assert_eq!(f.backend.metadata_calls.lock().await.len(), 1);
        assert!(f.backend.calls.lock().await.is_empty());
        f.backend.metadata_available.store(false, Ordering::SeqCst);
        assert_eq!(f.get(&path, false).await.2, b"");
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM beatmaps")
                .fetch_one(&f.state.pool)
                .await
                .unwrap(),
            0
        );
    }
);
