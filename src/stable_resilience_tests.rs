//! Synthetic serialization/identity fixtures; no large valid LZMA/client claim.
use super::{preflight_replay_container, require_replay_identity, score_ingestion_rejection};
use axum::http::StatusCode;
use resonance_protocol::{
    replay::{LegacyReplay, MAX_REPLAY_BYTES},
    submission::{MAX_STABLE_COMPRESSED_REPLAY_BYTES, StableSubmission},
};

fn submission() -> StableSubmission {
    StableSubmission::parse(
        "0123456789abcdef0123456789abcdef:StableFixture:00000000000000000000000000000000:10:0:0:0:0:0:1000:10:True:X:0:True:0:261007120000:20261007",
    )
    .unwrap()
}

#[test]
fn transient_ingestion_failures_remain_retryable() {
    for status in [
        StatusCode::INTERNAL_SERVER_ERROR,
        StatusCode::BAD_GATEWAY,
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::GATEWAY_TIMEOUT,
        StatusCode::REQUEST_TIMEOUT,
        StatusCode::TOO_MANY_REQUESTS,
    ] {
        let error = score_ingestion_rejection(status);
        assert_eq!(error.0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error.1, "score_ingestion_unavailable");
    }
    for status in [
        StatusCode::BAD_REQUEST,
        StatusCode::UNAUTHORIZED,
        StatusCode::FORBIDDEN,
        StatusCode::NOT_FOUND,
        StatusCode::UNPROCESSABLE_ENTITY,
    ] {
        let error = score_ingestion_rejection(status);
        assert_eq!(error.0, StatusCode::BAD_REQUEST);
        assert_eq!(error.1, "score_rejected_by_ingestion");
    }
    let withdrawn = score_ingestion_rejection(StatusCode::GONE);
    assert_eq!(withdrawn.0, StatusCode::GONE);
    assert_eq!(withdrawn.1, "beatmap_withdrawn");
    let conflict = score_ingestion_rejection(StatusCode::CONFLICT);
    assert_eq!(conflict.0, StatusCode::CONFLICT);
    assert_eq!(conflict.1, "duplicate_score_payload_mismatch");
}

#[test]
fn preflight_rejects_unrepresentable_replays_before_ingestion() {
    let mut score = submission();
    let frames = vec![0x81; MAX_STABLE_COMPRESSED_REPLAY_BYTES + 1];
    let error = preflight_replay_container(&score, "20261007", &frames).unwrap_err();
    assert_eq!(error.0, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(error.1, "score_upload_too_large");
    let error = preflight_replay_container(&score, "invalid-version", &[]).unwrap_err();
    assert_eq!(error.0, StatusCode::BAD_REQUEST);
    assert_eq!(error.1, "invalid_score_version");
    score.username = "x".repeat(2048);
    let error = preflight_replay_container(
        &score,
        "20261007",
        &frames[..MAX_STABLE_COMPRESSED_REPLAY_BYTES],
    )
    .unwrap_err();
    assert_eq!(error.0, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(error.1, "score_upload_too_large");
}

#[test]
fn capacity_container_can_complete_the_fixed_receipt_id_and_match_retry_identity() {
    let mut score = submission();
    score.username = "x".repeat(64);
    let frames = vec![0x81; MAX_STABLE_COMPRESSED_REPLAY_BYTES];
    let mut prepared = preflight_replay_container(&score, "20261007", &frames)
        .unwrap()
        .unwrap();
    assert!(prepared.len() <= MAX_REPLAY_BYTES);
    assert_eq!(
        LegacyReplay::parse(&prepared).unwrap().online_score_id,
        Some(0)
    );
    let offset = prepared.len() - 8;
    prepared[offset..].copy_from_slice(&42_i64.to_le_bytes());
    let expected = score.replay_container(20261007, &frames, 42).unwrap();
    assert_eq!(prepared, expected);
    let prepared = LegacyReplay::parse(&prepared).unwrap();
    let expected = LegacyReplay::parse(&expected).unwrap();
    require_replay_identity(&prepared, &expected).unwrap();
    assert_eq!(prepared.compressed_frames, frames);
}
