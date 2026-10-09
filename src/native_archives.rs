//! Native downloads keep their own origin and never forward player credentials.
//! Sanctuary verifies transient official ZIPs; retained immutable packages stay
//! authoritative and do not depend on an upstream provider being online.
use crate::{
    common,
    common::{ApiError, AppState},
    native_maps,
};
use axum::http::StatusCode;
use sha2::{Digest, Sha256};
use std::time::Duration;

const MAX_PROVIDER_BYTES: usize = 64 * 1024 * 1024;
const MAX_RETAINED_BYTES: u64 = 100 * 1024 * 1024;

fn service_error(status: StatusCode) -> ApiError {
    match status {
        StatusCode::NOT_FOUND => ApiError(status, "archive_not_available"),
        StatusCode::UNAUTHORIZED => ApiError(status, "invalid_account"),
        StatusCode::FORBIDDEN => ApiError(status, "beatmap_download_disabled"),
        StatusCode::GONE => ApiError(status, "beatmap_withdrawn"),
        StatusCode::CONFLICT => ApiError(status, "beatmap_set_version_conflict"),
        StatusCode::TOO_MANY_REQUESTS => ApiError(status, "archive_acquisition_rate_limited"),
        StatusCode::SERVICE_UNAVAILABLE => ApiError(status, "archive_acquisition_unavailable"),
        _ => ApiError(StatusCode::BAD_GATEWAY, "archive_acquisition_failed"),
    }
}

fn response_digest(headers: &reqwest::header::HeaderMap) -> Result<String, ApiError> {
    let invalid = || {
        ApiError(
            StatusCode::BAD_GATEWAY,
            "invalid_archive_acquisition_response",
        )
    };
    if headers
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        != Some("application/x-osu-archive")
    {
        return Err(invalid());
    }
    headers
        .get("x-illya-archive-sha256")
        .and_then(|value| value.to_str().ok())
        .filter(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        .map(str::to_owned)
        .ok_or_else(invalid)
}

/// This helper accepts an already authenticated account ID for lazer, or no
/// account for stable's public read-only /d route. Only the service key crosses
/// the internal boundary. Callers recheck their exact session after this wait.
pub(crate) async fn bytes(
    state: &AppState,
    set: i64,
    user: Option<i64>,
) -> Result<Vec<u8>, ApiError> {
    native_maps::require_id(set)?;
    native_maps::require_active(&state.pool, set).await?;
    if user.is_some_and(|user| user <= 0) {
        return Err(ApiError(StatusCode::UNAUTHORIZED, "invalid_account"));
    }
    if let Some(path) = native_maps::archive_path(state, set).await? {
        let bytes = common::stored_file(state, &path, "maps", MAX_RETAINED_BYTES).await?;
        native_maps::require_active(&state.pool, set).await?;
        return Ok(bytes);
    }
    if set >= native_maps::LOCAL_ID_MIN {
        return Err(ApiError(StatusCode::NOT_FOUND, "archive_not_available"));
    }
    let result = acquire(state, set, user).await;
    // Fresh local withdrawal and publication take precedence over a provider
    // failure as well as success. A failed upstream transfer cannot hide a
    // package that became authoritative while the request was waiting.
    native_maps::require_active(&state.pool, set).await?;
    if let Some(path) = native_maps::archive_path(state, set).await? {
        let bytes = common::stored_file(state, &path, "maps", MAX_RETAINED_BYTES).await?;
        native_maps::require_active(&state.pool, set).await?;
        return Ok(bytes);
    }
    result
}

async fn acquire(state: &AppState, set: i64, user: Option<i64>) -> Result<Vec<u8>, ApiError> {
    if state.service_key.len() < 32 {
        return Err(ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "archive_acquisition_not_configured",
        ));
    }
    // A service accidentally returning a redirect must never send this key to
    // a mirror. The player bearer is neither copied nor accepted as an input.
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(50))
        .build()
        .map_err(|_| {
            ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "archive_acquisition_unavailable",
            )
        })?;
    let mut input = serde_json::json!({"official_set_id":set});
    if let Some(user) = user {
        input["user_id"] = serde_json::json!(user);
    }
    let mut response = client
        .post(format!(
            "{}/api/v1/internal/beatmapsets/archive",
            state.sanctuary_url.trim_end_matches('/')
        ))
        .header("x-illya-service-key", state.service_key.as_str())
        .json(&input)
        .send()
        .await
        .map_err(|_| {
            ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "archive_acquisition_unavailable",
            )
        })?;
    if response.status() != StatusCode::OK {
        return Err(service_error(response.status()));
    }
    let digest = response_digest(response.headers())?;
    let invalid = || {
        ApiError(
            StatusCode::BAD_GATEWAY,
            "invalid_archive_acquisition_response",
        )
    };
    if response
        .content_length()
        .is_some_and(|length| length == 0 || length > MAX_PROVIDER_BYTES as u64)
    {
        return Err(invalid());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| invalid())? {
        if chunk.len() > MAX_PROVIDER_BYTES - bytes.len() {
            return Err(invalid());
        }
        bytes.extend_from_slice(&chunk);
    }
    // Completeness/member identity is validated by Sanctuary. This transport
    // guard additionally binds the exact verified bytes and rejects HTML/JSON.
    if !bytes.starts_with(b"PK\x03\x04") || format!("{:x}", Sha256::digest(&bytes)) != digest {
        return Err(invalid());
    }
    Ok(bytes)
}

#[cfg(test)]
#[path = "native_archive_tests.rs"]
mod tests;
