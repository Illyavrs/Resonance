//! Exact retained retry identity remains valid after local set withdrawal.
use super::*;
use crate::native_maps::checksum_tests::{DERIVED, Fixture, ORIGINAL};
use futures_util::FutureExt;

fn submission(checksum: &str) -> StableSubmission {
    StableSubmission::parse(&format!("{checksum}:ChecksumFixture:00000000000000000000000000000000:10:0:0:0:0:0:1000:10:True:X:0:True:0:261007120000:20261007")).unwrap()
}

#[tokio::test]
#[ignore = "requires ILLYA_TEST_DATABASE_URL pointing at disposable local PostgreSQL"]
async fn native_checksum_stable_retry_retains_exact_alias_and_legacy_original_contract() {
    let fixture = Fixture::new().await;
    let result = std::panic::AssertUnwindSafe(async {
        for (index, admitted) in [None, Some(ORIGINAL), Some(DERIVED)].into_iter().enumerate() {
            let key = format!("stable:{}:original-checksum-fixture-{index}", fixture.user);
            sqlx::query("INSERT INTO scores(user_id,beatmap_id,mode,score,accuracy,max_combo,mods,pp,passed,hit_counts,client,ingestion_key,client_beatmap_md5) VALUES($1,$2,0,1000,100,10,0,0,true,$3,'stable',$4,$5)")
                .bind(fixture.user).bind(fixture.map).bind(json!({"n300":10})).bind(&key).bind(admitted).execute(&fixture.state.pool).await.unwrap();
            let row = saved_score(&fixture.state, &key).await.unwrap().unwrap();
            let expected = admitted.unwrap_or(ORIGINAL);
            require_saved_payload(&row, &submission(expected), fixture.user, Some(fixture.map), &key, illya_domain::ScoreVariant::Vanilla).unwrap();
            let substituted = if expected == DERIVED { ORIGINAL } else { DERIVED };
            assert_eq!(require_saved_payload(&row, &submission(substituted), fixture.user, Some(fixture.map), &key, illya_domain::ScoreVariant::Vanilla).unwrap_err().0, StatusCode::CONFLICT);
            assert_eq!(require_saved_payload(&row, &submission(expected), fixture.user, Some(fixture.other), &key, illya_domain::ScoreVariant::Vanilla).unwrap_err().0, StatusCode::CONFLICT);
        }
        fixture.withdraw().await;
        for (index, expected) in [ORIGINAL, ORIGINAL, DERIVED].into_iter().enumerate() {
            let key = format!("stable:{}:original-checksum-fixture-{index}", fixture.user);
            let row = saved_score(&fixture.state, &key).await.unwrap().unwrap();
            require_saved_payload(&row, &submission(expected), fixture.user, Some(fixture.map), &key, illya_domain::ScoreVariant::Vanilla).unwrap();
        }
    }).catch_unwind().await;
    fixture.cleanup().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
