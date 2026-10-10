use super::*;
use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use futures_util::FutureExt;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::{str::FromStr, sync::Arc};
use tower::ServiceExt;

fn state(pool: sqlx::PgPool) -> AppState {
    AppState {
        pool,
        storage: Arc::new(std::env::temp_dir()),
        sanctuary_url: Arc::new("http://127.0.0.1:1".into()),
        service_key: Arc::new(String::new()),
        http: reqwest::Client::new(),
        limits: Default::default(),
        trusted_proxies: Default::default(),
        signalr: Arc::new(crate::signalr::Runtime::new()),
        lazer_chat: Arc::new(crate::lazer_chat::Runtime::new()),
    }
}

#[test]
fn native_profile_pagination_accepts_stock_lookahead_and_bounds() {
    for limit in ["6", "51", "100"] {
        let query = HashMap::from([
            ("limit".into(), limit.into()),
            ("offset".into(), "50".into()),
        ]);
        assert_eq!(pagination(&query).unwrap(), (limit.parse().unwrap(), 50));
    }
    assert_eq!(pagination(&HashMap::new()).unwrap(), (50, 0));
    for (name, value) in [
        ("limit", "0"),
        ("limit", "101"),
        ("limit", "oops"),
        ("offset", "-1"),
        ("offset", "10001"),
    ] {
        assert!(pagination(&HashMap::from([(name.into(), value.into())])).is_err());
    }
}

#[test]
fn native_profile_assets_are_absolute_deployment_urls_with_bounded_media_names() {
    assert_eq!(
        asset_url(
            "https://illya.test",
            "/media/banner/retained.webp",
            "banner"
        ),
        Some("https://illya.test/media/banner/retained.webp".into())
    );
    for value in [
        "https://osu.ppy.sh/banner.webp",
        "//osu.ppy.sh/avatar.webp",
        "/media/banner/../secret.webp",
        "/media/avatar/file.webp",
        "/media/banner/file.webp?token=unused",
        "/media/banner/file.webp#hash",
        "/media/banner/file.webp/extra",
    ] {
        assert!(
            asset_url("https://illya.test", value, "banner").is_none(),
            "{value}"
        );
    }
}

#[tokio::test]
async fn every_native_profile_section_retains_bearer_authentication() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
        .unwrap();
    let app = super::super::router().with_state(state(pool));
    for suffix in [
        "recent_activity",
        "kudosu",
        "scores/best",
        "scores/firsts",
        "scores/recent",
        "scores/pinned",
        "beatmapsets/favourite",
        "beatmapsets/ranked",
        "beatmapsets/loved",
        "beatmapsets/guest",
        "beatmapsets/pending",
        "beatmapsets/graveyard",
        "beatmapsets/nominated",
        "beatmapsets/most_played",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v2/users/1/{suffix}?limit=6&offset=0"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{suffix}");
        assert!(response.headers().get("location").is_none());
    }
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v2/me/beatmapset-favourites")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

struct Fixture {
    admin: sqlx::PgPool,
    state: AppState,
    app: Router,
    schema: String,
    user: i64,
    other: i64,
    token: String,
}
impl Fixture {
    async fn new() -> Self {
        let url = std::env::var("ILLYA_TEST_DATABASE_URL")
            .expect("owned disposable local PostgreSQL required");
        assert!(matches!(
            reqwest::Url::parse(&url).unwrap().host_str(),
            Some("127.0.0.1" | "localhost" | "[::1]")
        ));
        let admin = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .unwrap();
        let schema = format!("native_profile_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await
            .unwrap();
        let options = PgConnectOptions::from_str(&url)
            .unwrap()
            .options([("search_path", schema.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(3)
            .connect_with(options)
            .await
            .unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
        let user=sqlx::query_scalar("INSERT INTO users(username,email,password_hash,password_md5_hash,bio,banner_url) VALUES('NativeProfile','profile@invalid.test','unused','unused','Original biography','/media/banner/retained.webp') RETURNING id").fetch_one(&pool).await.unwrap();
        let other=sqlx::query_scalar("INSERT INTO users(username,email,password_hash,password_md5_hash) VALUES('NativeOther','other@invalid.test','unused','unused') RETURNING id").fetch_one(&pool).await.unwrap();
        let token = "original-profile-fixture-access".to_owned();
        sqlx::query("INSERT INTO lazer_sessions(id,user_id,access_hash,refresh_hash,access_expires_at,refresh_expires_at,created_at) VALUES($1,$2,$3,$4,now()+interval '1 hour',now()+interval '1 day','2026-01-02T03:04:05Z')")
            .bind(uuid::Uuid::new_v4()).bind(user).bind(common::token_hash(&token)).bind(common::secret()).execute(&pool).await.unwrap();
        let state = state(pool);
        let app = super::super::router()
            .merge(crate::beatmaps::router())
            .with_state(state.clone());
        Self {
            admin,
            state,
            app,
            schema,
            user,
            other,
            token,
        }
    }
    async fn get(&self, path: &str) -> (StatusCode, Value) {
        let response = self
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header("authorization", format!("Bearer {}", self.token))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1_048_576).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }
    async fn map(&self, owner: i64, official: bool, hash: &str, status: &str) -> i64 {
        sqlx::query_scalar("INSERT INTO beatmaps(official_id,official_set_id,uploaded_by,title,artist,creator,version,mode,status,stars,md5,osu_path,source) VALUES($1,$2,$3,'Original Fixture','Illyaverse','Fixture','Hard',0,$4,4,$5,'fixture.osu',$6) RETURNING id")
            .bind(if official{Some(740001i64)}else{None}).bind(if official{Some(840001i64)}else{None}).bind(owner).bind(status).bind(hash).bind(if official{"official"}else{"local"}).fetch_one(&self.state.pool).await.unwrap()
    }
    async fn score(
        &self,
        user: i64,
        map: i64,
        total: i64,
        pp: f64,
        review: &str,
        passed: bool,
    ) -> i64 {
        sqlx::query_scalar("INSERT INTO scores(user_id,beatmap_id,mode,score,accuracy,max_combo,pp,client,verification,standardised_score,classic_score,score_scale_status,hit_counts,passed,native_submission) VALUES($1,$2,0,$3,98,120,$4,'lazer',$5,$3,$3,'native_reported','{\"n300\":120}', $6,'{\"rank\":\"S\",\"statistics\":{\"great\":120},\"mods\":[]}') RETURNING id")
            .bind(user).bind(map).bind(total).bind(pp).bind(review).bind(passed).fetch_one(&self.state.pool).await.unwrap()
    }
    async fn cleanup(self) {
        drop(self.app);
        self.state.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await
            .unwrap();
        self.admin.close().await;
    }
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL pointing to owned disposable local PostgreSQL"]
async fn native_profile_returns_stored_sections_histories_and_complete_score_maps() {
    let f = Fixture::new().await;
    let result=std::panic::AssertUnwindSafe(async {
        let official=f.map(f.other,true,"11111111111111111111111111111111","ranked").await;
        let local=f.map(f.user,false,"22222222222222222222222222222222","graveyard").await;
        let winner=f.score(f.user,official,800_000,100.0,"verified",true).await;
        f.score(f.other,official,700_000,90.0,"verified",true).await;
        f.score(f.user,official,900_000,150.0,"pending",true).await;
        f.score(f.user,official,200_000,0.0,"verified",false).await;
        f.score(f.user,local,400_000,0.0,"verified",true).await;
        sqlx::query("INSERT INTO user_relationships(user_id,target_id,relation_type) VALUES($1,$2,'friend')").bind(f.other).bind(f.user).execute(&f.state.pool).await.unwrap();
        sqlx::query("INSERT INTO rank_history_batches(date,captured_at,row_count) VALUES((now() AT TIME ZONE 'UTC')::date-1,(((now() AT TIME ZONE 'UTC')::date-1)+time '12:00') AT TIME ZONE 'UTC',1)").execute(&f.state.pool).await.unwrap();
        sqlx::query("INSERT INTO rank_history_daily(user_id,date,mode,variant,captured_at,country,pp,rank,country_rank) SELECT $1,date,0,'vanilla',captured_at,'TW',100,1,1 FROM rank_history_batches").bind(f.user).execute(&f.state.pool).await.unwrap();
        let (status,profile)=f.get(&format!("/api/v2/users/{}/osu",f.user)).await;
        assert_eq!(status,StatusCode::OK,"{profile}");
        assert_eq!(profile["profile_order"],json!(["recent_activity","top_ranks","historical","beatmaps","kudosu"]));
        assert!(profile["last_visit"].is_string(),"visible login must not look hidden to LocalUserState");
        assert_eq!(profile["cover"]["url"],format!("{}/media/banner/retained.webp",crate::native_media::deployment_origin().unwrap()));
        assert_eq!(profile["scores_best_count"],1);assert_eq!(profile["scores_first_count"],1);assert_eq!(profile["scores_recent_count"],2);
        assert_eq!(profile["beatmap_playcounts_count"],2);assert_eq!(profile["graveyard_beatmapset_count"],1);assert_eq!(profile["ranked_beatmapset_count"],0);
        assert_eq!(profile["follower_count"],1);assert_eq!(profile["statistics"]["play_time"],Value::Null);
        let ranks=profile["rank_history"]["data"].as_array().unwrap();assert_eq!(ranks.len(),90);assert_eq!(ranks[88],1);assert_eq!(ranks[89],0);assert_eq!(ranks.iter().filter(|rank|rank.as_i64()==Some(1)).count(),1);
        assert_eq!(profile["rank_highest"]["rank"],1);assert_eq!(profile["monthly_playcounts"][0]["count"],3);
        for kind in ["best","firsts","recent"] {
            let (status,scores)=f.get(&format!("/api/v2/users/{}/scores/{kind}?mode=osu&limit=6&offset=0",f.user)).await;
            assert_eq!(status,StatusCode::OK,"{kind}:{scores}");
            let values=scores.as_array().unwrap();assert!(!values.is_empty());
            for score in values { assert!(score["beatmap"]["beatmapset"].is_object());assert_eq!(score["beatmap"]["id"],score["beatmap_id"]);assert!(score.get("beatmapset").is_none()); }
            if kind!="recent" {assert_eq!(values[0]["id"],winner);}
        }
        let (_,most)=f.get(&format!("/api/v2/users/{}/beatmapsets/most_played?limit=6",f.user)).await;
        assert_eq!(most.as_array().unwrap().len(),2);assert_eq!(most[0]["count"],2);assert!(most[0]["beatmapset"].is_object());
        let (_,maps)=f.get(&format!("/api/v2/users/{}/beatmapsets/graveyard?limit=6",f.user)).await;
        assert_eq!(maps.as_array().unwrap().len(),1);assert_eq!(maps[0]["title"],"Original Fixture");
        let (_,activity)=f.get(&format!("/api/v2/users/{}/recent_activity?limit=6",f.user)).await;
        assert_eq!(activity.as_array().unwrap().len(),1);assert_eq!(activity[0]["type"],"beatmapsetUpload");
        assert!(activity[0]["beatmapset"]["url"].as_str().unwrap().strip_prefix("/beatmapsets/").unwrap().parse::<i32>().is_ok());
        for suffix in ["scores/pinned","beatmapsets/favourite","beatmapsets/guest","beatmapsets/nominated","kudosu"] {
            let (status,value)=f.get(&format!("/api/v2/users/{}/{suffix}?limit=51&offset=0",f.user)).await;assert_eq!(status,StatusCode::OK);assert_eq!(value,json!([]));
        }
        let (status,favourites)=f.get("/api/v2/me/beatmapset-favourites").await;assert_eq!(status,StatusCode::OK);assert_eq!(favourites,json!({"beatmapset_ids":[]}));
        let (status,empty)=f.get(&format!("/api/v2/users/{}/beatmapsets/most_played?limit=6&offset=100",f.user)).await;assert_eq!(status,StatusCode::OK);assert_eq!(empty,json!([]));
        let (status,_)=f.get(&format!("/api/v2/users/{}/kudosu?limit=0",f.user)).await;assert_eq!(status,StatusCode::BAD_REQUEST);
        sqlx::query("UPDATE users SET is_restricted=true WHERE id=$1").bind(f.other).execute(&f.state.pool).await.unwrap();
        let (status,_)=f.get(&format!("/api/v2/users/{}/recent_activity",f.other)).await;assert_ne!(status,StatusCode::OK);
    }).catch_unwind().await;
    f.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL pointing to owned disposable local PostgreSQL"]
async fn stock_global_country_friend_and_mod_boards_return_native_collections() {
    let f = Fixture::new().await;
    let result = std::panic::AssertUnwindSafe(async {
        let map = f
            .map(f.user, true, "33333333333333333333333333333333", "ranked")
            .await;
        let own = f.score(f.user, map, 800_000, 100.0, "verified", true).await;
        f.score(f.other, map, 700_000, 90.0, "verified", true).await;
        f.score(f.other, map, 1_000_000, 150.0, "pending", true)
            .await;
        let classic=f.score(f.user,map,600_000,80.0,"verified",true).await;
        sqlx::query("UPDATE scores SET client='stable',score_scale_status='legacy_estimated',native_submission=NULL WHERE id=$1")
            .bind(classic).execute(&f.state.pool).await.unwrap();
        sqlx::query("INSERT INTO audit_events(action,target_type,target_id,details) VALUES('scores.stable_ingest','score',$1,'{\"rank\":\"S\"}')")
            .bind(classic.to_string()).execute(&f.state.pool).await.unwrap();
        for (query, count) in [
            ("type=global&mode=osu&limit=50", 2),
            ("type=country&mode=osu&limit=100", 2),
            ("type=friend&mode=osu&limit=100", 1),
            ("type=global&mode=osu&mods%5B%5D=NM&limit=100", 2),
        ] {
            let (status, board) = f
                .get(&format!("/api/v2/beatmaps/740001/scores?{query}"))
                .await;
            assert_eq!(status, StatusCode::OK, "{query}: {board}");
            assert_eq!(board["score_count"], count);
            assert_eq!(board["scores"].as_array().unwrap().len(), count as usize);
            assert_eq!(board["user_score"]["position"], 1);
            assert_eq!(board["user_score"]["score"]["id"], own);
            for score in board["scores"].as_array().unwrap() {
                assert!(score["user"]["id"].is_number());
                assert!(score["statistics"].is_object());
                assert!(score["mods"].is_array());
                assert!(score["ended_at"].is_string());
            }
        }
        // The supported stock GetScoresRequest serializer was exercised with
        // an inert API provider. These native parameter tuples contain no
        // variant; the explicit partition is a separate bridge extension.
        for filters in ["mods%5B%5D=CL","mods%5B%5D=NM&mods%5B%5D=CL"] {
            let (status,board)=f.get(&format!("/api/v2/beatmaps/740001/scores?type=global&mode=osu&{filters}&limit=100")).await;
            assert_eq!(status,StatusCode::OK,"stock Classic filter {filters}: {board}");
            assert_eq!(board["score_count"],1);assert_eq!(board["scores"][0]["id"],classic);assert_eq!(board["scores"][0]["mods"],json!([{"acronym":"CL"}]));
        }
        let (status,board)=f.get("/api/v2/beatmaps/740001/scores?type=global&mode=osu&mods%5B%5D=RX&limit=100").await;
        assert_eq!(status,StatusCode::OK,"stock RX filter without bridge partition: {board}");
        assert_eq!(board["scores"],json!([]));assert_eq!(board["score_variant"],"relax");
        for filters in ["mods%5B%5D=WU&variant=vanilla","mods%5B%5D=AT&variant=vanilla","mods%5B%5D=WU&mods%5B%5D=RX&variant=relax"] {
            let (status,board)=f.get(&format!("/api/v2/beatmaps/740001/scores?type=global&mode=osu&limit=50&{filters}")).await;
            assert_eq!(status,StatusCode::OK,"{filters}: {board}");assert_eq!(board["scores"],json!([]));assert_eq!(board["score_count"],0);assert_eq!(board["user_score"],Value::Null);
        }
        let (status,_)=f.get("/api/v2/beatmaps/740001/scores?type=global&mode=osu&mods%5B%5D=RX&variant=vanilla").await;
        assert_eq!(status,StatusCode::BAD_REQUEST,"conflicting explicit partitions still fail");
        sqlx::query("UPDATE scores SET verification='rejected' WHERE beatmap_id=$1")
            .bind(map)
            .execute(&f.state.pool)
            .await
            .unwrap();
        let (status, board) = f
            .get("/api/v2/beatmaps/740001/scores?type=global&mode=osu&limit=50")
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(board["scores"], json!([]));
        assert_eq!(board["user_score"], Value::Null);
        assert_eq!(board["score_count"], 0);
    })
    .catch_unwind()
    .await;
    f.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL pointing to owned disposable local PostgreSQL"]
async fn profile_score_maps_retain_each_admitted_client_checksum() {
    let f = Fixture::new().await;
    let result = std::panic::AssertUnwindSafe(async {
        let original = "44444444444444444444444444444444";
        let derived = "55555555555555555555555555555555";
        let map = f.map(f.user, false, original, "ranked").await;
        // This predates a derived distribution and therefore has a legacy
        // NULL admission, which remains bound to the original file checksum.
        let historical = f.score(f.user, map, 400_000, 50.0, "verified", true).await;
        let identity = sqlx::query("SELECT local_package_id,native_id,native_set_id FROM beatmaps WHERE id=$1")
            .bind(map).fetch_one(&f.state.pool).await.unwrap();
        let distribution = uuid::Uuid::new_v4();
        let mut tx = f.state.pool.begin().await.unwrap();
        sqlx::query("INSERT INTO local_beatmap_distributions(package_id,id,native_set_id,format_version,kind,original_sha256,original_byte_length) VALUES($1,$2,$3,1,'osu',$4,1)")
            .bind(identity.get::<uuid::Uuid,_>("local_package_id")).bind(distribution)
            .bind(identity.get::<i64,_>("native_set_id")).bind("a".repeat(64))
            .execute(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO local_beatmap_distribution_members(distribution_id,beatmap_id,original_md5,derived_md5,native_id,native_set_id,osu_path,sha256,byte_length) VALUES($1,$2,$3,$4,$5,$6,$7,$8,1)")
            .bind(distribution).bind(map).bind(original).bind(derived)
            .bind(identity.get::<i64,_>("native_id")).bind(identity.get::<i64,_>("native_set_id"))
            .bind(format!("distribution-{distribution}/{derived}.osu")).bind("b".repeat(64))
            .execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        let admitted:i64=sqlx::query_scalar("INSERT INTO scores(user_id,beatmap_id,mode,score,accuracy,max_combo,pp,client,verification,standardised_score,classic_score,score_scale_status,hit_counts,passed,native_submission,client_beatmap_md5) VALUES($1,$2,0,800000,98,120,100,'lazer','verified',800000,800000,'native_reported','{\"n300\":120}',true,'{\"rank\":\"S\",\"statistics\":{\"great\":120},\"mods\":[]}',$3) RETURNING id")
            .bind(f.user).bind(map).bind(derived).fetch_one(&f.state.pool).await.unwrap();
        assert_eq!(sqlx::query_scalar::<_,String>("SELECT illya_beatmap_preferred_checksum($1)")
            .bind(map).fetch_one(&f.state.pool).await.unwrap(),derived);
        for (score_id, checksum) in [(historical, original), (admitted, derived)] {
            let player = common::account(&f.state, f.user).await.unwrap();
            let compact = super::super::score_json(&f.state, score_id, &player).await.unwrap();
            let expanded = enrich_score(&f.state, compact).await.unwrap();
            assert_eq!(expanded["beatmap"]["checksum"], checksum);
            assert_eq!(expanded["beatmap"]["id"],identity.get::<i64,_>("native_id"));
            assert!(expanded["beatmap"]["beatmapset"].is_object());
        }
        let (status, recent) = f.get(&format!("/api/v2/users/{}/scores/recent?mode=osu&limit=6", f.user)).await;
        assert_eq!(status,StatusCode::OK);
        for score in recent.as_array().unwrap() {
            let checksum = if score["id"] == admitted { derived } else { original };
            assert_eq!(score["beatmap"]["checksum"],checksum);
        }
    }).catch_unwind().await;
    f.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
