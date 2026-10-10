//! Synthetic replay admission against complete registered local distributions.
//! Every fixture owns its schema and replay directory; no native client data.
use super::*;
use futures_util::{FutureExt, future::BoxFuture};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

const ORIGINAL: &str = "0123456789abcdef0123456789abcdef";
const DERIVED: &str = "abcdef0123456789abcdef0123456789";
const OTHER: &str = "11111111111111111111111111111111";
const FORGED: &str = "ffffffffffffffffffffffffffffffff";
const USERNAME: &str = "ReplayChecksumFixture";
const CONNECTION: &str = "owned-replay-checksum-connection";
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct Fixture {
    state: AppState,
    user: i64,
    map: i64,
    native: i64,
    set: i64,
    package: uuid::Uuid,
    other: i64,
}

async fn with_fixture<F>(test: F)
where
    F: for<'a> FnOnce(&'a Fixture) -> BoxFuture<'a, ()>,
{
    let _serial = SERIAL.lock().await;
    let url = std::env::var("ILLYA_TEST_DATABASE_URL")
        .expect("ILLYA_TEST_DATABASE_URL must identify owned disposable local PostgreSQL");
    assert!(
        matches!(
            reqwest::Url::parse(&url).unwrap().host_str(),
            Some("127.0.0.1" | "localhost" | "[::1]")
        ),
        "loopback disposable PostgreSQL required"
    );
    let control = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .unwrap();
    let nonce = uuid::Uuid::new_v4().simple().to_string();
    assert_eq!(nonce.len(), 32);
    assert!(nonce.bytes().all(|byte| byte.is_ascii_hexdigit()));
    let schema = format!("replay_checksum_{nonce}");
    let storage = std::env::temp_dir().join(&schema);
    assert!(storage.is_absolute());
    assert_eq!(storage.parent(), Some(std::env::temp_dir().as_path()));
    let mut owned_pool = None;
    // Migration, fixture setup and all assertions share the cleanup boundary.
    let result = std::panic::AssertUnwindSafe(async {
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&control)
            .await
            .unwrap();
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(
                PgConnectOptions::from_str(&url)
                    .unwrap()
                    .application_name(&schema)
                    .options([("search_path", schema.as_str())]),
            )
            .await
            .unwrap();
        owned_pool = Some(pool.clone());
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
        tokio::fs::create_dir_all(&storage).await.unwrap();
        let fixture = Fixture::seed(pool, storage.clone()).await;
        test(&fixture).await;
    })
    .catch_unwind()
    .await;
    if let Some(pool) = owned_pool {
        pool.close().await;
    }
    let schema_cleanup = sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(&control)
        .await;
    let file_cleanup = cleanup_owned_storage(&storage, &schema).await;
    control.close().await;
    schema_cleanup.expect("owned replay checksum schema cleanup");
    file_cleanup.expect("owned replay checksum storage cleanup");
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

async fn cleanup_owned_storage(storage: &Path, schema: &str) -> std::io::Result<()> {
    let resolved = match tokio::fs::canonicalize(storage).await {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        other => other?,
    };
    let temp_root = tokio::fs::canonicalize(std::env::temp_dir()).await?;
    if !resolved.is_absolute()
        || resolved.parent() != Some(temp_root.as_path())
        || resolved.file_name() != Some(std::ffi::OsStr::new(schema))
    {
        return Err(std::io::Error::other(
            "owned replay fixture cleanup path mismatch",
        ));
    }
    tokio::fs::remove_dir_all(resolved).await
}

impl Fixture {
    async fn seed(pool: PgPool, storage: PathBuf) -> Self {
        let user = sqlx::query_scalar("INSERT INTO users(username,email,password_hash,password_md5_hash) VALUES($1,'replay-checksum@invalid.test','unused','unused') RETURNING id")
            .bind(USERNAME).fetch_one(&pool).await.unwrap();
        let package: uuid::Uuid = sqlx::query_scalar("INSERT INTO beatmap_packages(uploaded_by,kind,sha256,byte_length,member_md5) VALUES($1,'osu',$2,1,$3) RETURNING id")
            .bind(user).bind("a".repeat(64)).bind(vec![ORIGINAL]).fetch_one(&pool).await.unwrap();
        let map: i64 = sqlx::query_scalar("INSERT INTO beatmaps(uploaded_by,local_package_id,title,artist,creator,version,mode,status,source,stars,max_combo,md5,osu_path) VALUES($1,$2,'Replay checksum fixture','Fixture artist','Fixture creator','Original difficulty',0,'ranked','local',1,2,$3,'original-fixture.osu') RETURNING id")
            .bind(user).bind(package).bind(ORIGINAL).fetch_one(&pool).await.unwrap();
        let identity = sqlx::query("SELECT native_id,native_set_id FROM beatmaps WHERE id=$1")
            .bind(map)
            .fetch_one(&pool)
            .await
            .unwrap();
        let other = sqlx::query_scalar("INSERT INTO beatmaps(uploaded_by,title,artist,creator,version,mode,status,source,stars,md5,osu_path) VALUES($1,'Other checksum fixture','Fixture','Fixture','Other',0,'ranked','local',1,$2,'other-fixture.osu') RETURNING id")
            .bind(user).bind(OTHER).fetch_one(&pool).await.unwrap();
        Self {
            state: AppState {
                pool,
                storage: Arc::new(storage),
                sanctuary_url: Arc::new("http://127.0.0.1:1".into()),
                service_key: Arc::new(String::new()),
                http: reqwest::Client::new(),
                limits: Default::default(),
                trusted_proxies: Default::default(),
                signalr: Arc::new(crate::signalr::Runtime::new()),
                lazer_chat: Arc::new(crate::lazer_chat::Runtime::new()),
            },
            user,
            map,
            native: identity.get("native_id"),
            set: identity.get("native_set_id"),
            package,
            other,
        }
    }

    async fn register_distribution(&self) {
        let distribution = uuid::Uuid::new_v4();
        let mut tx = self.state.pool.begin().await.unwrap();
        sqlx::query("INSERT INTO local_beatmap_distributions(package_id,id,native_set_id,format_version,kind,original_sha256,original_byte_length) VALUES($1,$2,$3,1,'osu',$4,1)")
            .bind(self.package).bind(distribution).bind(self.set).bind("a".repeat(64)).execute(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO local_beatmap_distribution_members(distribution_id,beatmap_id,original_md5,derived_md5,native_id,native_set_id,osu_path,sha256,byte_length) VALUES($1,$2,$3,$4,$5,$6,$7,$8,1)")
            .bind(distribution).bind(self.map).bind(ORIGINAL).bind(DERIVED)
            .bind(self.native).bind(self.set).bind(format!("distribution-{distribution}/{DERIVED}.osu"))
            .bind("b".repeat(64)).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT illya_beatmap_preferred_checksum($1)")
                .bind(self.map)
                .fetch_one(&self.state.pool)
                .await
                .unwrap(),
            DERIVED
        );
    }

    async fn token_result(&self, map: i64, checksum: Option<&str>) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar("INSERT INTO lazer_score_tokens(user_id,beatmap_id,ruleset_id,version_hash,expires_at,client_beatmap_md5) VALUES($1,$2,0,'owned-checksum-fixture',now()+interval '1 hour',$3) RETURNING id")
            .bind(self.user).bind(map).bind(checksum).fetch_one(&self.state.pool).await
    }

    async fn token(&self, checksum: Option<&str>) -> i64 {
        self.token_result(self.map, checksum).await.unwrap()
    }

    async fn score_result(
        &self,
        token: i64,
        map: i64,
        checksum: Option<&str>,
    ) -> Result<i64, sqlx::Error> {
        let input = submission();
        sqlx::query_scalar("INSERT INTO scores(user_id,beatmap_id,mode,score,accuracy,max_combo,mods,pp,passed,hit_counts,client,ingestion_key,api_mods,native_submission,client_beatmap_md5) VALUES($1,$2,0,$3,$4,$5,0,0,true,$6,'lazer',$7,$8,$9,$10) RETURNING id")
            .bind(self.user).bind(map).bind(input.total_score as i64).bind(input.accuracy * 100.0)
            .bind(input.max_combo as i32).bind(input.legacy_hit_counts()).bind(format!("lazer:{token}"))
            .bind(json!(input.mods)).bind(json!(input)).bind(checksum).fetch_one(&self.state.pool).await
    }

    async fn attach_score(&self, token: i64, checksum: Option<&str>) -> i64 {
        let score = self.score_result(token, self.map, checksum).await.unwrap();
        sqlx::query(
            "UPDATE lazer_score_tokens SET submission=$2,score_id=$3,consumed_at=now() WHERE id=$1",
        )
        .bind(token)
        .bind(json!(submission()))
        .bind(score)
        .execute(&self.state.pool)
        .await
        .unwrap();
        score
    }

    async fn started_capture(&self, checksum: Option<&str>) -> i64 {
        let token = self.token(checksum).await;
        begin(
            &self.state,
            self.user,
            token,
            CONNECTION,
            &initial(self.native),
        )
        .await
        .unwrap();
        frames(&self.state, self.user, token, CONNECTION, &bundle())
            .await
            .unwrap();
        token
    }

    async fn outcome(&self, score: i64) -> Value {
        sqlx::query_scalar("SELECT jsonb_build_object('verification',s.verification,'path',s.replay_path,'sha256',s.replay_sha256,'complete',s.replay_complete,'checksum',s.client_beatmap_md5,'audits',(SELECT count(*) FROM audit_events WHERE action='scores.native_publish' AND target_type='score' AND target_id=s.id::text)) FROM scores s WHERE s.id=$1")
            .bind(score).fetch_one(&self.state.pool).await.unwrap()
    }

    async fn replay_bytes(&self, score: i64) -> Vec<u8> {
        let path: String = sqlx::query_scalar("SELECT replay_path FROM scores WHERE id=$1")
            .bind(score)
            .fetch_one(&self.state.pool)
            .await
            .unwrap();
        tokio::fs::read(self.state.storage.join("replays").join(path))
            .await
            .unwrap()
    }

    async fn mutate_labels(&self) {
        sqlx::query("UPDATE beatmaps SET title='Renamed fixture',artist='Renamed artist',version='Renamed difficulty',stars=9 WHERE id=$1")
            .bind(self.map).execute(&self.state.pool).await.unwrap();
    }

    async fn pending_durable_replay(
        &self,
        checksum: Option<&str>,
        encoded_checksum: &str,
    ) -> (i64, i64, Vec<u8>) {
        let token = self.started_capture(checksum).await;
        let score = self.attach_score(token, checksum).await;
        sqlx::query(
            "UPDATE lazer_replay_captures SET ended_at=now(),final_state=3 WHERE token_id=$1",
        )
        .bind(token)
        .execute(&self.state.pool)
        .await
        .unwrap();
        let date: chrono::DateTime<chrono::Utc> =
            sqlx::query_scalar("SELECT created_at FROM scores WHERE id=$1")
                .bind(score)
                .fetch_one(&self.state.pool)
                .await
                .unwrap();
        let input = submission();
        let bytes = lazer_replay::encode(
            &ReplayScore {
                online_id: score,
                user_id: self.user,
                username: USERNAME,
                beatmap_md5: encoded_checksum,
                client_version: "",
                mods_bits: 0,
                timestamp_ticks: date.timestamp() * 10_000_000
                    + i64::from(date.timestamp_subsec_nanos() / 100)
                    + 621_355_968_000_000_000,
                submission: &input,
            },
            b"100|256|192|1,",
        )
        .unwrap();
        let path = format!("owned-recovery-{score}.osr");
        let directory = self.state.storage.join("replays");
        tokio::fs::create_dir_all(&directory).await.unwrap();
        tokio::fs::write(directory.join(&path), &bytes)
            .await
            .unwrap();
        sqlx::query("UPDATE scores SET replay_path=$2,replay_sha256=$3,replay_format='lazer',replay_complete=true WHERE id=$1")
            .bind(score).bind(path).bind(digest(&bytes)).execute(&self.state.pool).await.unwrap();
        sqlx::query("UPDATE lazer_score_tokens SET expires_at=now()-interval '1 hour' WHERE id=$1")
            .bind(token)
            .execute(&self.state.pool)
            .await
            .unwrap();
        (token, score, bytes)
    }
}

fn submission() -> SoloScoreSubmission {
    serde_json::from_value(json!({
        "ruleset_id":0,"passed":true,"total_score":1234,"total_score_without_mods":1234,
        "accuracy":1.0,"max_combo":2,"rank":"X","mods":[],
        "statistics":{"great":2},"maximum_statistics":{"great":2},"pauses":[]
    }))
    .unwrap()
}

fn initial(native: i64) -> Value {
    json!({"beatmapID":native,"rulesetID":0,"mods":[],"state":1,"maximumStatistics":{"5":2}})
}

fn bundle() -> Value {
    json!({"header":{"totalScore":1234,"totalScoreWithoutMods":1234,"accuracy":1.0,
        "combo":2,"maxCombo":2,"statistics":{"5":2},"mods":[],"pauses":[]},
        "frames":[{"time":100.0,"mouseX":256.0,"mouseY":192.0,"buttonState":1}]})
}

fn assert_checksum_guard(error: sqlx::Error) {
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("23514")
    );
}

async fn attempt_publication(f: &Fixture, score: i64, token: i64, checksum: &str) -> bool {
    let mut tx = f.state.pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM users WHERE id=$1 AND NOT is_restricted FOR SHARE")
        .bind(f.user)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let accepted = crate::native_score_publication::publish(
        &mut tx,
        score,
        f.user,
        crate::native_score_publication::NativeClient::Lazer,
        &format!("lazer:{token}"),
        checksum,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    accepted
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL pointing at owned disposable loopback PostgreSQL"]
async fn native_replay_checksum_original_and_derived_headers_preserve_historical_bytes() {
    with_fixture(|f| {
        Box::pin(async move {
            // This legacy NULL capture predates alias registration in this schema.
            let historical_token = f.started_capture(None).await;
            let historical = f.attach_score(historical_token, None).await;
            assert_eq!(
                end(&f.state, f.user, historical_token, CONNECTION, 3)
                    .await
                    .unwrap(),
                Some(historical)
            );
            let historical_bytes = f.replay_bytes(historical).await;
            assert_eq!(
                lazer_replay::ExtendedReplay::parse(&historical_bytes)
                    .unwrap()
                    .legacy
                    .beatmap_md5,
                ORIGINAL
            );
            let historical_outcome = f.outcome(historical).await;
            f.register_distribution().await;
            f.mutate_labels().await;
            for admitted in [Some(ORIGINAL), Some(DERIVED), None] {
                let token = f.started_capture(admitted).await;
                let score = f.attach_score(token, admitted).await;
                assert_eq!(
                    end(&f.state, f.user, token, CONNECTION, 3).await.unwrap(),
                    Some(score)
                );
                let bytes = f.replay_bytes(score).await;
                let replay = lazer_replay::ExtendedReplay::parse(&bytes).unwrap();
                assert_eq!(replay.legacy.beatmap_md5, admitted.unwrap_or(ORIGINAL));
                assert_eq!(replay.metadata.online_id, score);
                assert_eq!(replay.metadata.user_id, f.user);
                assert_eq!(replay.frame_count, 1);
                let outcome = f.outcome(score).await;
                assert_eq!(outcome["checksum"], json!(admitted));
                assert_eq!(outcome["verification"], "verified");
                assert_eq!(outcome["complete"], true);
                assert_eq!(outcome["audits"], 1);
                assert_eq!(outcome["sha256"], digest(&bytes));
                assert_eq!(
                    finalize(&f.state, f.user, token).await.unwrap(),
                    Some(score)
                );
                assert_eq!(f.replay_bytes(score).await, bytes);
                assert_eq!(f.outcome(score).await, outcome);
            }
            assert_eq!(f.replay_bytes(historical).await, historical_bytes);
            assert_eq!(f.outcome(historical).await, historical_outcome);
            assert_eq!(
                sqlx::query_scalar::<_, String>("SELECT md5 FROM beatmaps WHERE id=$1")
                    .bind(f.map)
                    .fetch_one(&f.state.pool)
                    .await
                    .unwrap(),
                ORIGINAL
            );
        })
    })
    .await;
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL pointing at owned disposable loopback PostgreSQL"]
async fn native_replay_checksum_guards_reject_forgery_wrong_map_and_alias_substitution() {
    with_fixture(|f| Box::pin(async move {
        f.register_distribution().await;
        assert_checksum_guard(f.token_result(f.map, Some(FORGED)).await.unwrap_err());
        assert_checksum_guard(f.token_result(f.other, Some(DERIVED)).await.unwrap_err());
        let insert_token = f.token(Some(DERIVED)).await;
        assert_checksum_guard(f.score_result(insert_token, f.map, Some(FORGED)).await.unwrap_err());
        assert_checksum_guard(f.score_result(insert_token, f.other, Some(DERIVED)).await.unwrap_err());
        // Each checksum separately identifies this map, but the accepted token
        // and score must still agree on the exact bytes for this submission.
        for (token_checksum, score_checksum) in [(None, Some(DERIVED)), (Some(ORIGINAL), Some(DERIVED)), (Some(DERIVED), Some(ORIGINAL))] {
            let token = f.started_capture(token_checksum).await;
            let score = f.attach_score(token, score_checksum).await;
            assert_eq!(end(&f.state, f.user, token, CONNECTION, 3).await.unwrap_err().1, "score_ingestion_link_mismatch");
            let before = f.outcome(score).await;
            assert_eq!(before["verification"], "pending");
            assert_eq!(before["path"], Value::Null);
            assert_eq!(before["audits"], 0);
            for table in ["lazer_score_tokens", "scores"] {
                let id = if table == "scores" { score } else { token };
                let alternate = if table == "scores" { token_checksum.unwrap_or(ORIGINAL) } else { score_checksum.unwrap() };
                assert_checksum_guard(sqlx::query(&format!("UPDATE {table} SET client_beatmap_md5=$2 WHERE id=$1"))
                    .bind(id).bind(alternate).execute(&f.state.pool).await.unwrap_err());
                assert_checksum_guard(sqlx::query(&format!("UPDATE {table} SET beatmap_id=$2 WHERE id=$1"))
                    .bind(id).bind(f.other).execute(&f.state.pool).await.unwrap_err());
            }
            // The publication helper also refuses the other lawful alias even
            // when its caller has a saved replay reference and completeness.
            sqlx::query("UPDATE scores SET replay_path='owned-unread-guard-fixture.osr',replay_complete=true WHERE id=$1")
                .bind(score).execute(&f.state.pool).await.unwrap();
            assert!(!attempt_publication(f, score, token, token_checksum.unwrap_or(ORIGINAL)).await);
            assert!(!attempt_publication(f, score, token, FORGED).await);
            assert_eq!(f.outcome(score).await["verification"], "pending");
            assert_eq!(f.outcome(score).await["audits"], 0);
        }
        assert!(!f.state.storage.join("replays").exists());
    })).await;
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL pointing at owned disposable loopback PostgreSQL"]
async fn native_replay_checksum_incomplete_or_interrupted_capture_never_publishes() {
    with_fixture(|f| {
        Box::pin(async move {
            f.register_distribution().await;
            let missing_frames = f.token(Some(DERIVED)).await;
            begin(
                &f.state,
                f.user,
                missing_frames,
                CONNECTION,
                &initial(f.native),
            )
            .await
            .unwrap();
            let missing_score = f.attach_score(missing_frames, Some(DERIVED)).await;
            assert_eq!(
                end(&f.state, f.user, missing_frames, CONNECTION, 3)
                    .await
                    .unwrap_err()
                    .1,
                "replay_frames_missing"
            );
            assert_eq!(f.outcome(missing_score).await["path"], Value::Null);
            assert_eq!(f.outcome(missing_score).await["verification"], "pending");
            let unfinished = f.started_capture(Some(DERIVED)).await;
            let unfinished_score = f.attach_score(unfinished, Some(DERIVED)).await;
            assert_eq!(finalize(&f.state, f.user, unfinished).await.unwrap(), None);
            assert_eq!(f.outcome(unfinished_score).await["path"], Value::Null);
            let interrupted = f.started_capture(Some(DERIVED)).await;
            begin(
                &f.state,
                f.user,
                interrupted,
                "replacement-owned-connection",
                &initial(f.native),
            )
            .await
            .unwrap();
            let interrupted_score = f.attach_score(interrupted, Some(DERIVED)).await;
            assert_eq!(
                end(
                    &f.state,
                    f.user,
                    interrupted,
                    "replacement-owned-connection",
                    3
                )
                .await
                .unwrap(),
                Some(interrupted_score)
            );
            let bytes = f.replay_bytes(interrupted_score).await;
            assert_eq!(
                lazer_replay::ExtendedReplay::parse(&bytes)
                    .unwrap()
                    .legacy
                    .beatmap_md5,
                DERIVED
            );
            let interrupted_outcome = f.outcome(interrupted_score).await;
            assert_eq!(interrupted_outcome["complete"], false);
            assert_eq!(interrupted_outcome["verification"], "pending");
            assert!(!attempt_publication(f, interrupted_score, interrupted, DERIVED).await);
            assert_eq!(
                finalize(&f.state, f.user, interrupted).await.unwrap(),
                Some(interrupted_score)
            );
            assert_eq!(f.replay_bytes(interrupted_score).await, bytes);
            assert_eq!(f.outcome(interrupted_score).await, interrupted_outcome);
            for score in [missing_score, unfinished_score, interrupted_score] {
                assert_eq!(f.outcome(score).await["verification"], "pending");
                assert_eq!(f.outcome(score).await["audits"], 0);
            }
        })
    })
    .await;
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL pointing at owned disposable loopback PostgreSQL"]
async fn native_replay_checksum_recovery_uses_frozen_original_or_derived_bytes() {
    with_fixture(|f| Box::pin(async move {
        f.register_distribution().await;
        let mut retained = Vec::new();
        for checksum in [None, Some(ORIGINAL), Some(DERIVED)] {
            let expected = checksum.unwrap_or(ORIGINAL);
            let (token, score, bytes) = f.pending_durable_replay(checksum, expected).await;
            assert_eq!(f.outcome(score).await["verification"], "pending");
            assert_eq!(lazer_replay::ExtendedReplay::parse(&bytes).unwrap().legacy.beatmap_md5, expected);
            retained.push((token, score, bytes));
        }
        f.mutate_labels().await;
        assert_eq!(recover_pending(&f.state).await.unwrap(), retained.len());
        for (token, score, bytes) in &retained {
            let outcome = f.outcome(*score).await;
            assert_eq!(outcome["verification"], "verified");
            assert_eq!(outcome["audits"], 1);
            assert_eq!(f.replay_bytes(*score).await, *bytes);
            let capture = sqlx::query("SELECT completed_at,(SELECT count(*) FROM lazer_replay_chunks WHERE token_id=$1) chunks FROM lazer_replay_captures WHERE token_id=$1")
                .bind(token).fetch_one(&f.state.pool).await.unwrap();
            assert!(capture.get::<Option<chrono::DateTime<chrono::Utc>>, _>("completed_at").is_some());
            assert_eq!(capture.get::<i64, _>("chunks"), 0);
            assert_eq!(finalize(&f.state, f.user, *token).await.unwrap(), Some(*score));
            assert_eq!(f.outcome(*score).await, outcome);
            assert_eq!(f.replay_bytes(*score).await, *bytes);
        }
        assert_eq!(recover_pending(&f.state).await.unwrap(), 0);
        // A valid digest cannot authorize a replay encoded for the other alias.
        let (token, score, bytes) = f.pending_durable_replay(Some(DERIVED), ORIGINAL).await;
        let before = f.outcome(score).await;
        assert_eq!(finalize(&f.state, f.user, token).await.unwrap_err().1, "invalid_protocol_payload");
        assert_eq!(f.outcome(score).await, before);
        assert_eq!(f.replay_bytes(score).await, bytes);
        assert_eq!(sqlx::query_scalar::<_, Option<chrono::DateTime<chrono::Utc>>>("SELECT completed_at FROM lazer_replay_captures WHERE token_id=$1")
            .bind(token).fetch_one(&f.state.pool).await.unwrap(), None);
    })).await;
}
