//! Durable solo-score intent processing. No database lock spans ingestion HTTP.
use crate::common::{self, ApiError, AppState};
use axum::http::{HeaderMap, StatusCode};
use chrono::{DateTime, Utc};
use illya_domain::ScoreVariant;
use resonance_protocol::lazer::SoloScoreSubmission;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};

const BATCH: i64 = 16;
const LEASE_SECONDS: f64 = 45.0;

fn invalid(code: &'static str) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, code)
}
fn unavailable(code: &'static str) -> ApiError {
    ApiError(StatusCode::SERVICE_UNAVAILABLE, code)
}
fn digest(payload: &Value) -> Result<String, ApiError> {
    let bytes = serde_json::to_vec(payload).map_err(|_| invalid("invalid_stored_submission"))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

async fn originating_session_valid(
    tx: &mut Transaction<'_, Postgres>,
    row: &PgRow,
) -> Result<bool, ApiError> {
    let Some(session) = row.get::<Option<uuid::Uuid>, _>("received_session_id") else {
        return Ok(false);
    };
    Ok(sqlx::query("SELECT id FROM lazer_sessions WHERE id=$1 AND user_id=$2 AND access_expires_at>clock_timestamp() FOR UPDATE")
        .bind(session).bind(row.get::<i64, _>("user_id")).fetch_optional(&mut **tx).await?.is_some())
}

fn validate_intent(
    row: &PgRow,
    payload: &Value,
    input: &SoloScoreSubmission,
) -> Result<(), ApiError> {
    input.validate()?;
    crate::lazer::legacy_mods(input.ruleset_id, &input.mods)?;
    if row.get::<Option<DateTime<Utc>>, _>("revoked_at").is_some()
        || row.get::<String, _>("processing_status") == "revoked"
    {
        return Err(invalid("score_token_revoked"));
    }
    if row.get::<DateTime<Utc>, _>("expires_at") <= Utc::now()
        && row.get::<Option<i64>, _>("score_id").is_none()
    {
        return Err(invalid("score_token_expired"));
    }
    if row.get::<i16, _>("ruleset_id") != i16::from(input.ruleset_id) {
        return Err(invalid("score_ruleset_mismatch"));
    }
    if row
        .get::<Option<Value>, _>("submission")
        .as_ref()
        .is_some_and(|saved| saved != payload)
        || row
            .get::<Option<String>, _>("submission_sha256")
            .as_deref()
            .is_some_and(|saved| Some(saved) != digest(payload).ok().as_deref())
    {
        return Err(ApiError(StatusCode::CONFLICT, "score_token_already_used"));
    }
    Ok(())
}

enum Preflight {
    Done(i64),
    Claimed,
    Busy,
}

async fn claim(
    tx: &mut Transaction<'_, Postgres>,
    token: i64,
    manual: bool,
) -> Result<bool, ApiError> {
    Ok(sqlx::query("UPDATE lazer_score_tokens SET retry_at=now()+make_interval(secs=>$2::double precision),processing_attempts=processing_attempts+1,processing_error=NULL WHERE id=$1 AND scope='solo' AND processing_status='received' AND score_id IS NULL AND (retry_at<=now() OR ($3 AND processing_error IS NOT NULL))")
        .bind(token).bind(LEASE_SECONDS).bind(manual).execute(&mut **tx).await?.rows_affected() == 1)
}

async fn preflight(
    state: &AppState,
    headers: &HeaderMap,
    uid: i64,
    native_map: i64,
    token: i64,
    payload: &Value,
    input: &SoloScoreSubmission,
) -> Result<Preflight, ApiError> {
    let mut tx = state.pool.begin().await?;
    common::lazer_account_in_tx(&mut tx, headers, uid).await?;
    let row = sqlx::query("SELECT t.*,COALESCE(t.client_beatmap_md5,b.md5) client_checksum FROM lazer_score_tokens t JOIN beatmaps b ON b.id=t.beatmap_id WHERE t.id=$1 AND t.user_id=$2 AND t.scope='solo' AND b.native_id=$3 AND (t.expires_at>now() OR t.score_id IS NOT NULL) FOR UPDATE OF t")
        .bind(token).bind(uid).bind(native_map).fetch_optional(&mut *tx).await?
        .ok_or(ApiError(StatusCode::NOT_FOUND, "invalid_score_token"))?;
    validate_intent(&row, payload, input)?;
    if let Some(score) = row.get::<Option<i64>, _>("score_id") {
        // Older linked receipts may predate durable intent metadata. Exact
        // score identity is still checked before returning their receipt.
        let saved = sqlx::query("SELECT s.*,COALESCE(s.client_beatmap_md5,b.md5) score_client_checksum FROM scores s JOIN beatmaps b ON b.id=s.beatmap_id WHERE s.id=$1 FOR SHARE OF s")
            .bind(score)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(invalid("score_ingestion_link_mismatch"))?;
        validate_score(
            &saved,
            uid,
            row.get("beatmap_id"),
            token,
            payload,
            input,
            &row.get::<String, _>("client_checksum"),
        )?;
        tx.commit().await?;
        return Ok(Preflight::Done(score));
    }
    if row.get::<String, _>("processing_status") != "issued"
        && !originating_session_valid(&mut tx, &row).await?
    {
        return Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "invalid_originating_session",
        ));
    }
    let mods = crate::lazer::legacy_mods(input.ruleset_id, &input.mods)?;
    let variant = ScoreVariant::from_legacy(input.ruleset_id, mods)
        .map_err(|_| invalid("invalid_score_variant"))?;
    if let Err(error) = crate::native_maps::lock_score_admission(
        &mut tx,
        token,
        uid,
        row.get("beatmap_id"),
        i16::from(input.ruleset_id),
        variant,
    )
    .await
    {
        if error.0 == StatusCode::GONE && row.get::<String, _>("processing_status") == "received" {
            sqlx::query("UPDATE lazer_score_tokens SET processing_error='beatmap_withdrawn',retry_at=now()+interval '5 minutes' WHERE id=$1 AND scope='solo' AND score_id IS NULL")
                .bind(token).execute(&mut *tx).await?;
            tx.commit().await?;
        }
        return Err(error);
    }
    if row.get::<String, _>("processing_status") == "issued" {
        let session: uuid::Uuid = sqlx::query_scalar("SELECT id FROM lazer_sessions WHERE user_id=$1 AND access_hash=$2 AND access_expires_at>clock_timestamp()")
            .bind(uid).bind(common::bearer_hash(headers)?).fetch_optional(&mut *tx).await?
            .ok_or(ApiError(StatusCode::UNAUTHORIZED, "invalid_access_token"))?;
        sqlx::query("UPDATE lazer_score_tokens SET submission=$2,submission_sha256=$3,received_at=now(),received_session_id=$4,processing_status='received',retry_at=now() WHERE id=$1")
            .bind(token).bind(payload).bind(digest(payload)?).bind(session).execute(&mut *tx).await?;
    }
    let claimed = claim(&mut tx, token, true).await?;
    tx.commit().await?;
    Ok(if claimed {
        Preflight::Claimed
    } else {
        Preflight::Busy
    })
}

fn validate_score(
    saved: &PgRow,
    uid: i64,
    map: i64,
    token: i64,
    payload: &Value,
    input: &SoloScoreSubmission,
    client_checksum: &str,
) -> Result<(), ApiError> {
    let mods = crate::lazer::legacy_mods(input.ruleset_id, &input.mods)?;
    let variant = ScoreVariant::from_legacy(input.ruleset_id, mods)
        .map_err(|_| invalid("invalid_score_variant"))?;
    if saved.get::<i64, _>("user_id") != uid
        || saved.get::<i64, _>("beatmap_id") != map
        || saved.get::<i16, _>("mode") != i16::from(input.ruleset_id)
        || saved.get::<String, _>("score_variant") != variant.as_str()
        || saved.get::<String, _>("client") != "lazer"
        || saved.get::<String, _>("score_client_checksum") != client_checksum
        || saved.get::<Option<String>, _>("ingestion_key").as_deref()
            != Some(format!("lazer:{token}").as_str())
        || saved.get::<Option<Value>, _>("native_submission").as_ref() != Some(payload)
        || saved.get::<i64, _>("score") != input.total_score as i64
        || (saved.get::<f64, _>("accuracy") - input.accuracy * 100.0).abs() > 1e-6
        || saved.get::<i32, _>("max_combo") != input.max_combo as i32
        || saved.get::<i64, _>("mods") != i64::from(mods)
        || saved.get::<bool, _>("passed") != input.passed
        || saved.get::<Value, _>("hit_counts") != input.legacy_hit_counts()
        || crate::room_scores::canonical_mods(&saved.get::<Value, _>("api_mods"))?
            != crate::room_scores::canonical_mods(&json!(input.mods))?
    {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "score_ingestion_link_mismatch",
        ));
    }
    Ok(())
}

fn ingestion_error(status: StatusCode) -> ApiError {
    if status == StatusCode::GONE {
        ApiError(StatusCode::GONE, "beatmap_withdrawn")
    } else if status.is_server_error() || matches!(status.as_u16(), 401 | 403 | 404 | 429) {
        unavailable("score_ingestion_unavailable")
    } else {
        invalid("score_rejected_by_ingestion")
    }
}

async fn process(
    state: &AppState,
    uid: i64,
    token: i64,
    payload: &Value,
    headers: Option<&HeaderMap>,
) -> Result<i64, ApiError> {
    if state.service_key.len() < 32 {
        return Err(unavailable("internal_ingestion_not_configured"));
    }
    let input: SoloScoreSubmission = serde_json::from_value(payload.clone())
        .map_err(|_| invalid("invalid_stored_submission"))?;
    input.validate()?;
    let mods = crate::lazer::legacy_mods(input.ruleset_id, &input.mods)?;
    let (map, client_checksum): (i64, String) = sqlx::query_as("SELECT t.beatmap_id,COALESCE(t.client_beatmap_md5,b.md5) FROM lazer_score_tokens t JOIN beatmaps b ON b.id=t.beatmap_id WHERE t.id=$1 AND t.user_id=$2 AND t.scope='solo' AND t.submission=$3 AND t.processing_status='received' AND t.revoked_at IS NULL")
        .bind(token).bind(uid).bind(payload).fetch_optional(&state.pool).await?
        .ok_or(invalid("invalid_score_token"))?;
    let response = state.http.post(format!("{}/api/v1/internal/scores", state.sanctuary_url.trim_end_matches('/')))
        .header("x-illya-service-key", state.service_key.as_str())
        .json(&json!({"user_id":uid,"beatmap_id":map,"client_beatmap_md5":client_checksum,"mode":input.ruleset_id,"score":input.total_score,
            "accuracy":input.accuracy*100.0,"max_combo":input.max_combo,"mods":mods,"api_mods":input.mods,
            "passed":input.passed,"hit_counts":input.legacy_hit_counts(),"native_submission":input,
            "client":"lazer","ingestion_key":format!("lazer:{token}")}))
        .timeout(std::time::Duration::from_secs(30)).send().await
        .map_err(|_| unavailable("score_ingestion_unavailable"))?;
    if !response.status().is_success() {
        return Err(ingestion_error(response.status()));
    }
    let result: Value = response
        .json()
        .await
        .map_err(|_| ApiError(StatusCode::BAD_GATEWAY, "invalid_ingestion_response"))?;
    let score_id = result["id"].as_i64().filter(|id| *id > 0).ok_or(ApiError(
        StatusCode::BAD_GATEWAY,
        "invalid_ingestion_response",
    ))?;
    let mut tx = state.pool.begin().await?;
    if let Some(headers) = headers {
        common::lazer_account_in_tx(&mut tx, headers, uid).await?;
    } else {
        sqlx::query("SELECT id FROM users WHERE id=$1 AND NOT is_restricted FOR UPDATE")
            .bind(uid)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(ApiError(StatusCode::UNAUTHORIZED, "invalid_account"))?;
    }
    let row = sqlx::query("SELECT * FROM lazer_score_tokens WHERE id=$1 AND user_id=$2 AND scope='solo' AND beatmap_id=$3 FOR UPDATE")
        .bind(token).bind(uid).bind(map).fetch_optional(&mut *tx).await?
        .ok_or(invalid("invalid_score_token"))?;
    validate_intent(&row, payload, &input)?;
    if row.get::<Option<Value>, _>("submission").as_ref() != Some(payload)
        || row.get::<Option<String>, _>("submission_sha256").as_deref()
            != Some(digest(payload)?.as_str())
        || row
            .get::<Option<i64>, _>("score_id")
            .is_some_and(|saved| saved != score_id)
    {
        return Err(ApiError(StatusCode::CONFLICT, "score_token_already_used"));
    }
    if row.get::<Option<i64>, _>("score_id").is_none()
        && !originating_session_valid(&mut tx, &row).await?
    {
        return Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "invalid_originating_session",
        ));
    }
    let saved = sqlx::query("SELECT s.*,COALESCE(s.client_beatmap_md5,b.md5) score_client_checksum FROM scores s JOIN beatmaps b ON b.id=s.beatmap_id WHERE s.id=$1 FOR SHARE OF s")
        .bind(score_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(ApiError(
            StatusCode::BAD_GATEWAY,
            "invalid_ingestion_response",
        ))?;
    validate_score(&saved, uid, map, token, payload, &input, &client_checksum)?;
    sqlx::query("UPDATE lazer_score_tokens SET consumed_at=COALESCE(consumed_at,now()),score_id=$2,processing_status='processed',processing_error=NULL WHERE id=$1")
        .bind(token).bind(score_id).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(score_id)
}

async fn defer(state: &AppState, token: i64, code: &'static str) -> Result<(), ApiError> {
    sqlx::query("UPDATE lazer_score_tokens SET processing_error=$2,retry_at=now()+make_interval(secs=>LEAST(300,5*power(2,LEAST(processing_attempts,6)))::double precision) WHERE id=$1 AND scope='solo' AND processing_status='received' AND score_id IS NULL")
        .bind(token).bind(code).execute(&state.pool).await?;
    Ok(())
}

async fn after_processing(state: &AppState, uid: i64, token: i64) {
    if let Err(error) = crate::replay_capture::finalize(state, uid, token).await {
        tracing::warn!(
            token_id = token,
            code = error.1,
            "solo score replay publication deferred"
        );
    }
    if let Err(error) = crate::signalr::publish_processed_score(state, token).await {
        tracing::warn!(
            token_id = token,
            code = error.1,
            "solo score event delivery deferred"
        );
    }
}

pub(super) async fn submit(
    state: &AppState,
    headers: &HeaderMap,
    uid: i64,
    native_map: i64,
    token: i64,
    input: &SoloScoreSubmission,
) -> Result<i64, ApiError> {
    input.validate()?;
    crate::lazer::legacy_mods(input.ruleset_id, &input.mods)?;
    if !crate::native_maps::valid_id(native_map) || token <= 0 {
        return Err(invalid("invalid_score_token"));
    }
    if state.service_key.len() < 32 {
        return Err(unavailable("internal_ingestion_not_configured"));
    }
    let payload = json!(input);
    for _ in 0..100 {
        let score = match preflight(state, headers, uid, native_map, token, &payload, input).await?
        {
            Preflight::Done(score) => score,
            Preflight::Claimed => match process(state, uid, token, &payload, Some(headers)).await {
                Ok(score) => score,
                Err(error) => {
                    defer(state, token, error.1).await?;
                    return Err(error);
                }
            },
            Preflight::Busy => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
        };
        after_processing(state, uid, token).await;
        return Ok(score);
    }
    Err(unavailable("solo_score_processing_pending"))
}

/// Recover accepted intents both before ingestion and after ingestion commit.
/// The original session and exact payload remain authoritative on every retry.
pub(crate) async fn recover_pending(state: &AppState) -> Result<usize, ApiError> {
    let candidates: Vec<(i64, i64)> = sqlx::query_as("SELECT id,user_id FROM lazer_score_tokens WHERE scope='solo' AND processing_status='received' AND score_id IS NULL AND retry_at<=now() ORDER BY retry_at,id LIMIT $1")
        .bind(BATCH).fetch_all(&state.pool).await?;
    let mut processed = 0;
    for (token, uid) in candidates {
        let mut tx = state.pool.begin().await?;
        let active =
            sqlx::query("SELECT id FROM users WHERE id=$1 AND NOT is_restricted FOR UPDATE")
                .bind(uid)
                .fetch_optional(&mut *tx)
                .await?
                .is_some();
        let Some(row) = sqlx::query("SELECT * FROM lazer_score_tokens WHERE id=$1 AND user_id=$2 AND scope='solo' AND processing_status='received' AND score_id IS NULL FOR UPDATE")
            .bind(token).bind(uid).fetch_optional(&mut *tx).await? else { continue; };
        let payload = row.get::<Option<Value>, _>("submission");
        let input = payload.as_ref().and_then(|payload| {
            serde_json::from_value::<SoloScoreSubmission>(payload.clone()).ok()
        });
        let session_valid = originating_session_valid(&mut tx, &row).await?;
        let valid = active
            && session_valid
            && input
                .as_ref()
                .zip(payload.as_ref())
                .is_some_and(|(input, payload)| {
                    validate_intent(&row, payload, input).is_ok()
                        && row.get::<Option<String>, _>("submission_sha256").as_deref()
                            == digest(payload).ok().as_deref()
                        && row.get::<Option<DateTime<Utc>>, _>("received_at").is_some()
                });
        if !valid {
            sqlx::query("UPDATE lazer_score_tokens SET processing_status='revoked',revoked_at=COALESCE(revoked_at,now()),revoke_reason=COALESCE(revoke_reason,'invalid_saved_intent'),processing_error='invalid_saved_intent' WHERE id=$1 AND processing_status='received'")
                .bind(token).execute(&mut *tx).await?;
            tx.commit().await?;
            continue;
        }
        let input = input.unwrap();
        let payload = payload.unwrap();
        let mods = crate::lazer::legacy_mods(input.ruleset_id, &input.mods)?;
        let variant = ScoreVariant::from_legacy(input.ruleset_id, mods)
            .map_err(|_| invalid("invalid_score_variant"))?;
        if let Err(error) = crate::native_maps::lock_score_admission(
            &mut tx,
            token,
            uid,
            row.get("beatmap_id"),
            i16::from(input.ruleset_id),
            variant,
        )
        .await
        {
            if error.0 != StatusCode::GONE {
                return Err(error);
            }
            sqlx::query("UPDATE lazer_score_tokens SET processing_error='beatmap_withdrawn',retry_at=now()+interval '5 minutes' WHERE id=$1 AND processing_status='received'")
                .bind(token).execute(&mut *tx).await?;
            tx.commit().await?;
            continue;
        }
        if !claim(&mut tx, token, false).await? {
            continue;
        }
        tx.commit().await?;
        match process(state, uid, token, &payload, None).await {
            Ok(_) => {
                processed += 1;
                after_processing(state, uid, token).await;
            }
            Err(error) => {
                defer(state, token, error.1).await?;
                tracing::warn!(
                    token_id = token,
                    code = error.1,
                    "solo score recovery deferred"
                );
            }
        }
    }
    crate::signalr::publish_pending_processed_scores(state).await?;
    Ok(processed)
}

/// The ended spectator callback only accepts the exact committed solo receipt.
pub(crate) async fn processed_score_id(
    state: &AppState,
    uid: i64,
    token: i64,
) -> Result<Option<i64>, ApiError> {
    let Some(row) = sqlx::query("SELECT s.*,t.submission intent,t.submission_sha256,COALESCE(t.client_beatmap_md5,b.md5) client_checksum,COALESCE(s.client_beatmap_md5,b.md5) score_client_checksum FROM lazer_score_tokens t JOIN users u ON u.id=t.user_id JOIN scores s ON s.id=t.score_id AND s.user_id=t.user_id AND s.beatmap_id=t.beatmap_id AND s.mode=t.ruleset_id AND s.client='lazer' AND s.ingestion_key='lazer:'||t.id::text JOIN beatmaps b ON b.id=t.beatmap_id WHERE t.id=$1 AND t.user_id=$2 AND t.scope='solo' AND t.processing_status='processed' AND t.revoked_at IS NULL AND t.consumed_at IS NOT NULL AND NOT u.is_restricted AND s.verification<>'rejected'")
        .bind(token).bind(uid).fetch_optional(&state.pool).await? else { return Ok(None); };
    let Some(payload) = row.get::<Option<Value>, _>("intent") else {
        return Ok(None);
    };
    let Ok(input) = serde_json::from_value::<SoloScoreSubmission>(payload.clone()) else {
        return Ok(None);
    };
    if input.validate().is_err()
        || row.get::<Option<String>, _>("submission_sha256").as_deref()
            != Some(digest(&payload)?.as_str())
        || validate_score(
            &row,
            uid,
            row.get("beatmap_id"),
            token,
            &payload,
            &input,
            &row.get::<String, _>("client_checksum"),
        )
        .is_err()
    {
        return Ok(None);
    }
    Ok(Some(row.get("id")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_maps::checksum_tests::{DERIVED, Fixture, ORIGINAL};
    use futures_util::FutureExt;

    #[tokio::test]
    #[ignore = "requires ILLYA_TEST_DATABASE_URL pointing at disposable local PostgreSQL"]
    async fn native_checksum_solo_receipt_rejects_a_different_valid_alias() {
        let fixture = Fixture::new().await;
        let result = std::panic::AssertUnwindSafe(async {
            let input: SoloScoreSubmission = serde_json::from_value(json!({
                "ruleset_id":0,"passed":true,"total_score":1000,"accuracy":1.0,
                "max_combo":10,"rank":"X","mods":[],"statistics":{"great":10},
                "maximum_statistics":{"great":10}
            })).unwrap();
            let payload = json!(input);
            for (index, admitted) in [None, Some(ORIGINAL), Some(DERIVED)].into_iter().enumerate() {
                let token = 40 + index as i64;
                let id: i64 = sqlx::query_scalar("INSERT INTO scores(user_id,beatmap_id,mode,score,accuracy,max_combo,mods,pp,passed,hit_counts,client,ingestion_key,client_beatmap_md5,native_submission,api_mods) VALUES($1,$2,0,1000,100,10,0,0,true,$3,'lazer',$4,$5,$6,'[]') RETURNING id")
                    .bind(fixture.user).bind(fixture.map).bind(input.legacy_hit_counts()).bind(format!("lazer:{token}")).bind(admitted).bind(&payload).fetch_one(&fixture.state.pool).await.unwrap();
                let saved = sqlx::query("SELECT s.*,COALESCE(s.client_beatmap_md5,b.md5) score_client_checksum FROM scores s JOIN beatmaps b ON b.id=s.beatmap_id WHERE s.id=$1")
                    .bind(id).fetch_one(&fixture.state.pool).await.unwrap();
                let expected = admitted.unwrap_or(ORIGINAL);
                validate_score(&saved, fixture.user, fixture.map, token, &payload, &input, expected).unwrap();
                let substituted = if expected == DERIVED { ORIGINAL } else { DERIVED };
                assert_eq!(validate_score(&saved, fixture.user, fixture.map, token, &payload, &input, substituted).unwrap_err().1, "score_ingestion_link_mismatch");
            }
        }).catch_unwind().await;
        fixture.cleanup().await;
        if let Err(error) = result {
            std::panic::resume_unwind(error);
        }
    }

    #[test]
    fn transient_ingestion_is_retryable_without_reclassifying_validation() {
        for status in [
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::BAD_GATEWAY,
            StatusCode::TOO_MANY_REQUESTS,
        ] {
            assert_eq!(ingestion_error(status).0, StatusCode::SERVICE_UNAVAILABLE);
        }
        assert_eq!(
            ingestion_error(StatusCode::BAD_REQUEST).0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            ingestion_error(StatusCode::UNPROCESSABLE_ENTITY).0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(ingestion_error(StatusCode::GONE).0, StatusCode::GONE);
    }
}
