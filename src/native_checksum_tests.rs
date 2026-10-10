//! Synthetic registered-distribution admission fixtures; no retained client data.
use super::*;
use crate::common;
use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use futures_util::FutureExt;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{str::FromStr, sync::Arc};
use tower::ServiceExt;

pub(crate) const ORIGINAL: &str = "0123456789abcdef0123456789abcdef";
pub(crate) const DERIVED: &str = "abcdef0123456789abcdef0123456789";
const OTHER: &str = "11111111111111111111111111111111";
const FORGED: &str = "ffffffffffffffffffffffffffffffff";

pub(crate) struct Fixture {
    control: PgPool,
    schema: String,
    pub(crate) state: AppState,
    pub(crate) map: i64,
    pub(crate) native: i64,
    pub(crate) set: i64,
    pub(crate) other: i64,
    pub(crate) other_native: i64,
    pub(crate) user: i64,
    token: String,
}

impl Fixture {
    pub(crate) async fn new() -> Self {
        let url = std::env::var("ILLYA_TEST_DATABASE_URL").expect("disposable PostgreSQL URL");
        assert!(matches!(
            reqwest::Url::parse(&url).unwrap().host_str(),
            Some("127.0.0.1" | "localhost" | "[::1]")
        ));
        let control = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .unwrap();
        let schema = format!("native_checksum_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&control)
            .await
            .unwrap();
        let connection = PgPoolOptions::new()
            .max_connections(6)
            .connect_with(
                PgConnectOptions::from_str(&url)
                    .unwrap()
                    .options([("search_path", schema.as_str())]),
            )
            .await;
        let pool = match connection {
            Ok(pool) => pool,
            Err(error) => {
                sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
                    .execute(&control)
                    .await
                    .unwrap();
                control.close().await;
                panic!("disposable checksum fixture connection failed: {error}");
            }
        };
        let result = std::panic::AssertUnwindSafe(async {
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
        let user: i64 = sqlx::query_scalar("INSERT INTO users(username,email,password_hash,password_md5_hash) VALUES('ChecksumFixture','checksum-fixture@invalid.test','unused','unused') RETURNING id")
            .fetch_one(&pool).await.unwrap();
        let token = common::secret();
        sqlx::query("INSERT INTO lazer_sessions(id,user_id,access_hash,refresh_hash,access_expires_at,refresh_expires_at) VALUES($1,$2,$3,$4,now()+interval '1 hour',now()+interval '1 day')")
            .bind(uuid::Uuid::new_v4()).bind(user).bind(common::token_hash(&token)).bind(common::secret()).execute(&pool).await.unwrap();
        let mut tx = pool.begin().await.unwrap();
        let map: i64 = sqlx::query_scalar("INSERT INTO beatmaps(uploaded_by,title,artist,creator,version,mode,status,source,stars,max_combo,md5,osu_path) VALUES($1,'Checksum fixture','Fixture','Fixture','Local',0,'ranked','local',1,10,$2,'checksum-original.osu') RETURNING id")
            .bind(user).bind(ORIGINAL).fetch_one(&mut *tx).await.unwrap();
        let identity = sqlx::query("SELECT local_package_id,native_id,native_set_id FROM beatmaps WHERE id=$1").bind(map).fetch_one(&mut *tx).await.unwrap();
        let native: i64 = identity.get("native_id");
        let set: i64 = identity.get("native_set_id");
        let distribution = uuid::Uuid::new_v4();
        sqlx::query("INSERT INTO local_beatmap_distributions(package_id,id,native_set_id,format_version,kind,original_sha256,original_byte_length) VALUES($1,$2,$3,1,'osu',$4,1)")
            .bind(identity.get::<uuid::Uuid,_>("local_package_id")).bind(distribution).bind(set).bind("a".repeat(64)).execute(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO local_beatmap_distribution_members(distribution_id,beatmap_id,original_md5,derived_md5,native_id,native_set_id,osu_path,sha256,byte_length) VALUES($1,$2,$3,$4,$5,$6,$7,$8,1)")
            .bind(distribution).bind(map).bind(ORIGINAL).bind(DERIVED).bind(native).bind(set).bind(format!("distribution-{distribution}/{DERIVED}.osu")).bind("b".repeat(64)).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        let other: i64 = sqlx::query_scalar("INSERT INTO beatmaps(uploaded_by,title,artist,creator,version,mode,status,source,stars,md5,osu_path) VALUES($1,'Other fixture','Fixture','Fixture','Other',0,'ranked','local',1,$2,'checksum-other.osu') RETURNING id")
            .bind(user).bind(OTHER).fetch_one(&pool).await.unwrap();
        let other_native = sqlx::query_scalar("SELECT native_id FROM beatmaps WHERE id=$1").bind(other).fetch_one(&pool).await.unwrap();
        Self {
            control: control.clone(), schema: schema.clone(), map, native, set, other, other_native, user, token,
            state: AppState {
                pool: pool.clone(), storage: Arc::new(std::env::temp_dir()), sanctuary_url: Arc::new("http://127.0.0.1:1".into()),
                service_key: Arc::new(String::new()), http: reqwest::Client::new(), limits: Default::default(),
                trusted_proxies: Default::default(), signalr: Arc::new(crate::signalr::Runtime::new()),
                lazer_chat: Arc::new(crate::lazer_chat::Runtime::new()),
            },
        }
        }).catch_unwind().await;
        match result {
            Ok(fixture) => fixture,
            Err(error) => {
                pool.close().await;
                sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
                    .execute(&control)
                    .await
                    .unwrap();
                control.close().await;
                std::panic::resume_unwind(error);
            }
        }
    }

    pub(crate) async fn withdraw(&self) {
        sqlx::query("INSERT INTO beatmapset_availability(native_set_id,withdrawn,revision,reason,actor_id) VALUES($1,true,1,'Original checksum withdrawal fixture',$2)")
            .bind(self.set).bind(self.user).execute(&self.state.pool).await.unwrap();
    }

    pub(crate) async fn cleanup(self) {
        self.state.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.control)
            .await
            .unwrap();
        self.control.close().await;
    }

    async fn issue(&self, native: i64, checksum: &str) -> (StatusCode, serde_json::Value) {
        let response = crate::lazer::router().with_state(self.state.clone()).oneshot(
            Request::builder().method("POST").uri(format!("/api/v2/beatmaps/{native}/solo/scores"))
                .header("authorization", format!("Bearer {}", self.token)).header("content-type", "application/json")
                .body(Body::from(json!({"beatmap_hash":checksum,"version_hash":"original-fixture-version","ruleset_id":0}).to_string())).unwrap(),
        ).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 65536).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL pointing at disposable local PostgreSQL"]
async fn native_checksum_registered_alias_stays_bound_to_canonical_id_and_withdrawal() {
    let fixture = Fixture::new().await;
    let result = std::panic::AssertUnwindSafe(async {
        for checksum in [
            ORIGINAL.to_owned(),
            DERIVED.to_owned(),
            DERIVED.to_ascii_uppercase(),
        ] {
            assert_eq!(
                resolve_checksum(&fixture.state, fixture.user, &checksum)
                    .await
                    .unwrap(),
                Some(fixture.map)
            );
            assert_eq!(
                resolve_checksum_for_id(&fixture.state, fixture.user, &checksum, fixture.native)
                    .await
                    .unwrap(),
                Some(fixture.map)
            );
        }
        assert_eq!(known_checksum(&fixture.state, FORGED).await.unwrap(), None);
        assert_eq!(
            resolve_checksum_for_id(&fixture.state, fixture.user, DERIVED, fixture.other_native)
                .await
                .unwrap_err()
                .1,
            "beatmap_id_mismatch"
        );
        assert_eq!(
            require_checksum(&fixture.state.pool, fixture.other, DERIVED)
                .await
                .unwrap_err()
                .1,
            "beatmap_hash_mismatch"
        );
        assert_eq!(
            require_checksum(&fixture.state.pool, fixture.map, FORGED)
                .await
                .unwrap_err()
                .1,
            "beatmap_hash_mismatch"
        );
        let dto = map_json(&fixture.state, fixture.map).await.unwrap();
        assert_eq!(dto["id"], fixture.native);
        assert_eq!(dto["checksum"], DERIVED);
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT md5 FROM beatmaps WHERE id=$1")
                .bind(fixture.map)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            ORIGINAL
        );
        fixture.withdraw().await;
        for checksum in [ORIGINAL, DERIVED] {
            assert_eq!(
                resolve_checksum(&fixture.state, fixture.user, checksum)
                    .await
                    .unwrap_err()
                    .0,
                StatusCode::GONE
            );
        }
        let retained = retained_map_json(&fixture.state, fixture.map)
            .await
            .unwrap();
        assert_eq!(retained["checksum"], DERIVED);
        assert_eq!(retained["is_scoreable"], false);
    })
    .catch_unwind()
    .await;
    fixture.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL pointing at disposable local PostgreSQL"]
async fn native_checksum_token_freezes_exact_accepted_original_or_derived_bytes() {
    let fixture = Fixture::new().await;
    let result = std::panic::AssertUnwindSafe(async {
        for checksum in [ORIGINAL.to_owned(), DERIVED.to_ascii_uppercase()] {
            let (status, response) = fixture.issue(fixture.native, &checksum).await;
            assert_eq!(status, StatusCode::OK);
            let id = response["id"].as_i64().unwrap();
            let saved: String =
                sqlx::query_scalar("SELECT client_beatmap_md5 FROM lazer_score_tokens WHERE id=$1")
                    .bind(id)
                    .fetch_one(&fixture.state.pool)
                    .await
                    .unwrap();
            assert_eq!(saved, checksum.to_ascii_lowercase());
            let substituted = if saved == DERIVED { ORIGINAL } else { DERIVED };
            assert!(
                sqlx::query("UPDATE lazer_score_tokens SET client_beatmap_md5=$2 WHERE id=$1")
                    .bind(id)
                    .bind(substituted)
                    .execute(&fixture.state.pool)
                    .await
                    .is_err()
            );
        }
        assert_eq!(
            fixture.issue(fixture.other_native, DERIVED).await.0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            fixture.issue(fixture.native, FORGED).await.0,
            StatusCode::BAD_REQUEST
        );
        fixture.withdraw().await;
        assert_eq!(
            fixture.issue(fixture.native, DERIVED).await.0,
            StatusCode::GONE
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM lazer_score_tokens")
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            2
        );
    })
    .catch_unwind()
    .await;
    fixture.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL pointing at disposable local PostgreSQL"]
async fn native_checksum_internal_discovery_requires_exact_alias_to_reply_binding() {
    let mut fixture = Fixture::new().await;
    let reply = json!({"id":fixture.map,"official_id":null,"official_set_id":null,
        "md5":ORIGINAL,"status":"ranked","source":"local","mode":0});
    let service = axum::Router::new().route(
        "/api/v1/internal/beatmaps/resolve",
        axum::routing::post(move || {
            let reply = reply.clone();
            async move { axum::Json(reply) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    fixture.state.sanctuary_url = Arc::new(format!("http://{}", listener.local_addr().unwrap()));
    fixture.state.service_key = Arc::new("checksum-fixture-internal-service-key-32-bytes".into());
    let task = tokio::spawn(async move {
        axum::serve(listener, service).await.unwrap();
    });
    let result = std::panic::AssertUnwindSafe(async {
        for checksum in [ORIGINAL, DERIVED] {
            assert_eq!(
                discover(&fixture.state, fixture.user, Selector::Checksum(checksum))
                    .await
                    .unwrap(),
                Some(fixture.map)
            );
        }
        for checksum in [OTHER, FORGED] {
            let error = discover(&fixture.state, fixture.user, Selector::Checksum(checksum))
                .await
                .unwrap_err();
            assert_eq!(error.0, StatusCode::BAD_GATEWAY);
            assert_eq!(error.1, "invalid_map_resolution_response");
        }
    })
    .catch_unwind()
    .await;
    task.abort();
    let _ = task.await;
    fixture.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
