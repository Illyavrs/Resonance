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
use sqlx::Row;
use std::time::Duration;

const MAX_PROVIDER_BYTES: usize = 64 * 1024 * 1024;
const MAX_RETAINED_BYTES: u64 = 100 * 1024 * 1024;

pub(crate) struct Download {
    pub bytes: Vec<u8>,
    pub content_disposition: String,
}

fn response_name(headers: &reqwest::header::HeaderMap, set: i64) -> Result<String, ApiError> {
    headers
        .get(reqwest::header::CONTENT_DISPOSITION)
        .and_then(|value| value.to_str().ok())
        .filter(|value| {
            illya_domain::download_filename::attachment_filename(value)
                .is_some_and(|name| name.starts_with(&format!("{set} ")) && name.ends_with(".osz"))
        })
        .map(str::to_owned)
        .ok_or(ApiError(
            StatusCode::BAD_GATEWAY,
            "invalid_archive_acquisition_response",
        ))
}

async fn retained_download(state: &AppState, set: i64, path: &str) -> Result<Download, ApiError> {
    let row = sqlx::query(
        "SELECT artist,title FROM beatmaps WHERE native_set_id=$1 ORDER BY native_id LIMIT 1",
    )
    .bind(set)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError(
        StatusCode::INTERNAL_SERVER_ERROR,
        "invalid_stored_beatmap_archive",
    ))?;
    let filename = illya_domain::download_filename::archive_filename(
        set,
        &row.get::<String, _>("artist"),
        &row.get::<String, _>("title"),
    )
    .ok_or(ApiError(
        StatusCode::INTERNAL_SERVER_ERROR,
        "invalid_stored_beatmap_archive",
    ))?;
    let bytes = common::stored_file(state, path, "maps", MAX_RETAINED_BYTES).await?;
    native_maps::require_active(&state.pool, set).await?;
    Ok(Download {
        bytes,
        content_disposition: illya_domain::download_filename::attachment(&filename),
    })
}

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
pub(crate) async fn download(
    state: &AppState,
    set: i64,
    user: Option<i64>,
) -> Result<Download, ApiError> {
    native_maps::require_id(set)?;
    native_maps::require_active(&state.pool, set).await?;
    if user.is_some_and(|user| user <= 0) {
        return Err(ApiError(StatusCode::UNAUTHORIZED, "invalid_account"));
    }
    if set >= native_maps::LOCAL_ID_MIN {
        // Sanctuary materializes and verifies canonical local chart IDs. Never
        // bypass its checksum aliases by returning the original upload ZIP.
        let result = acquire(state, set, user).await;
        native_maps::require_active(&state.pool, set).await?;
        return result;
    }
    if let Some(path) = native_maps::archive_path(state, set).await? {
        return retained_download(state, set, &path).await;
    }
    let result = acquire(state, set, user).await;
    // Fresh local withdrawal and publication take precedence over a provider
    // failure as well as success. A failed upstream transfer cannot hide a
    // package that became authoritative while the request was waiting.
    native_maps::require_active(&state.pool, set).await?;
    if let Some(path) = native_maps::archive_path(state, set).await? {
        return retained_download(state, set, &path).await;
    }
    result
}

async fn acquire(state: &AppState, set: i64, user: Option<i64>) -> Result<Download, ApiError> {
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
    let local = set >= native_maps::LOCAL_ID_MIN;
    let max_bytes = if local {
        MAX_RETAINED_BYTES as usize
    } else {
        MAX_PROVIDER_BYTES
    };
    let mut input = if local {
        serde_json::json!({})
    } else {
        serde_json::json!({"official_set_id":set})
    };
    if let Some(user) = user {
        input["user_id"] = serde_json::json!(user);
    }
    let url = if local {
        format!(
            "{}/api/v1/internal/beatmapsets/{set}/archive",
            state.sanctuary_url.trim_end_matches('/')
        )
    } else {
        format!(
            "{}/api/v1/internal/beatmapsets/archive",
            state.sanctuary_url.trim_end_matches('/')
        )
    };
    let mut response = client
        .post(url)
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
    let content_disposition = response_name(response.headers(), set)?;
    let invalid = || {
        ApiError(
            StatusCode::BAD_GATEWAY,
            "invalid_archive_acquisition_response",
        )
    };
    if response
        .content_length()
        .is_some_and(|length| length == 0 || length > max_bytes as u64)
    {
        return Err(invalid());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| invalid())? {
        if chunk.len() > max_bytes - bytes.len() {
            return Err(invalid());
        }
        bytes.extend_from_slice(&chunk);
    }
    // Completeness/member identity is validated by Sanctuary. This transport
    // guard additionally binds the exact verified bytes and rejects HTML/JSON.
    if !bytes.starts_with(b"PK\x03\x04") || format!("{:x}", Sha256::digest(&bytes)) != digest {
        return Err(invalid());
    }
    Ok(Download {
        bytes,
        content_disposition,
    })
}

#[cfg(test)]
#[path = "native_archive_tests.rs"]
mod tests;
