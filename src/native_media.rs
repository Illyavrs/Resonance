//! Native cover and preview resources stay on this deployment. The game never
//! contacts an official API, authenticates upstream, or follows a media redirect.
use crate::{
    common::{self, ApiError, AppState},
    native_maps,
};
use axum::{
    Extension, Router,
    body::{Body, Bytes},
    extract::{ConnectInfo, Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use image::ImageDecoder;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

#[path = "native_local_media.rs"]
mod local;

const COVER_LIMIT: usize = 2 * 1024 * 1024;
const PREVIEW_LIMIT: usize = 16 * 1024 * 1024;
const CACHE_LIMIT: usize = 32 * 1024 * 1024;
const CACHE_ENTRIES: usize = 128;
const CACHE_AGE: Duration = Duration::from_secs(300);
const FETCH_DEADLINE: Duration = Duration::from_secs(8);
const COVER_VARIANTS: [&str; 8] = [
    "cover",
    "cover@2x",
    "card",
    "card@2x",
    "list",
    "list@2x",
    "slimcover",
    "slimcover@2x",
];
static CACHE: OnceLock<Mutex<VecDeque<(MediaKey, Instant, Media)>>> = OnceLock::new();
static WORKERS: OnceLock<Arc<Semaphore>> = OnceLock::new();
static HTTP: OnceLock<reqwest::Client> = OnceLock::new();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Cover(usize),
    Preview,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MediaKey {
    set: i64,
    kind: Kind,
}
#[derive(Clone)]
struct Media {
    bytes: Bytes,
    content_type: &'static str,
    etag: HeaderValue,
}

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/v2/beatmapsets/{id}/media/{variant}", get(cover))
        .route("/api/v2/beatmapsets/{id}/preview", get(preview))
}

fn public_origin(value: &str) -> Result<String, ApiError> {
    let invalid = || {
        ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "native_media_origin_not_configured",
        )
    };
    if value.len() > 2048 || value.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err(invalid());
    }
    let url = reqwest::Url::parse(value).map_err(|_| invalid())?;
    let host = url.host_str().ok_or_else(invalid)?;
    let loopback = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<std::net::IpAddr>()
        .is_ok_and(|ip| ip.is_loopback());
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
        || host == "ppy.sh"
        || host.ends_with(".ppy.sh")
        || (url.scheme() == "http" && host != "localhost" && !loopback && !host.ends_with(".test"))
    {
        return Err(invalid());
    }
    Ok(url.as_str().trim_end_matches('/').to_owned())
}

pub(crate) fn deployment_origin() -> Result<String, ApiError> {
    let configured =
        std::env::var("PUBLIC_ORIGIN").unwrap_or_else(|_| "http://localhost:3000".into());
    public_origin(&configured)
}

/// Covers and previews are public resources: their downloader sends no bearer.
/// Do not forward untrusted upstream URLs even if an upstream API supplied them.
pub(crate) fn decorate_set(set: &mut Value) -> Result<(), ApiError> {
    let id = set["id"]
        .as_i64()
        .filter(|id| native_maps::valid_id(*id))
        .ok_or(ApiError(
            StatusCode::BAD_GATEWAY,
            "invalid_native_media_identity",
        ))?;
    let origin = deployment_origin()?;
    decorate_set_at(set, id, &origin);
    Ok(())
}

fn decorate_set_at(set: &mut Value, id: i64, origin: &str) {
    let covers = COVER_VARIANTS
        .into_iter()
        .map(|variant| {
            (
                variant.to_owned(),
                json!(format!("{origin}/api/v2/beatmapsets/{id}/media/{variant}")),
            )
        })
        .collect::<serde_json::Map<String, Value>>();
    set["covers"] = Value::Object(covers);
    set["preview_url"] = json!(format!("{origin}/api/v2/beatmapsets/{id}/preview"));
}

fn upstream_url(key: MediaKey) -> Result<String, ApiError> {
    if !(1..native_maps::LOCAL_ID_MIN).contains(&key.set) {
        return Err(ApiError(StatusCode::NOT_FOUND, "native_media_not_found"));
    }
    Ok(match key.kind {
        Kind::Cover(index) => format!(
            "https://assets.ppy.sh/beatmaps/{}/covers/{}.jpg",
            key.set,
            COVER_VARIANTS
                .get(index)
                .ok_or(ApiError(StatusCode::NOT_FOUND, "native_media_not_found"))?
        ),
        Kind::Preview => format!("https://b.ppy.sh/preview/{}.mp3", key.set),
    })
}

fn http() -> Result<reqwest::Client, ApiError> {
    if let Some(client) = HTTP.get() {
        return Ok(client.clone());
    }
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(3))
        .timeout(FETCH_DEADLINE)
        .referer(false)
        .build()
        .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "native_media_unavailable"))?;
    let _ = HTTP.set(client.clone());
    Ok(client)
}

fn cached(key: MediaKey) -> Option<Media> {
    let mut cache = CACHE.get_or_init(Mutex::default).lock().ok()?;
    let now = Instant::now();
    cache.retain(|(_, at, _)| now.duration_since(*at) < CACHE_AGE);
    let index = cache.iter().position(|entry| entry.0 == key)?;
    let entry = cache.remove(index)?;
    let media = entry.2.clone();
    cache.push_back(entry);
    Some(media)
}

fn remember(key: MediaKey, media: Media) {
    if let Ok(mut cache) = CACHE.get_or_init(Mutex::default).lock() {
        cache.retain(|entry| entry.0 != key);
        let mut size = cache.iter().map(|entry| entry.2.bytes.len()).sum::<usize>();
        while !cache.is_empty()
            && (cache.len() >= CACHE_ENTRIES
                || media.bytes.len() > CACHE_LIMIT.saturating_sub(size))
        {
            size -= cache.pop_front().unwrap().2.bytes.len();
        }
        if media.bytes.len() <= CACHE_LIMIT {
            cache.push_back((key, Instant::now(), media));
        }
    }
}

fn validate_body(bytes: &[u8], kind: Kind, advertised: &str) -> Result<&'static str, ApiError> {
    let invalid = || ApiError(StatusCode::BAD_GATEWAY, "invalid_native_media_response");
    let mime = advertised.split(';').next().unwrap_or_default().trim();
    if bytes.is_empty()
        || bytes.len()
            > match kind {
                Kind::Cover(_) => COVER_LIMIT,
                Kind::Preview => PREVIEW_LIMIT,
            }
    {
        return Err(invalid());
    }
    let content_type = match kind {
        Kind::Cover(_) if bytes.starts_with(b"\xff\xd8\xff") && mime == "image/jpeg" => {
            Ok("image/jpeg")
        }
        Kind::Cover(_) if bytes.starts_with(b"\x89PNG\r\n\x1a\n") && mime == "image/png" => {
            Ok("image/png")
        }
        Kind::Cover(_)
            if bytes.len() >= 12
                && &bytes[..4] == b"RIFF"
                && &bytes[8..12] == b"WEBP"
                && mime == "image/webp" =>
        {
            Ok("image/webp")
        }
        Kind::Preview
            if (bytes.starts_with(b"ID3")
                || (bytes.len() >= 2 && bytes[0] == 0xff && bytes[1] & 0xe0 == 0xe0))
                && matches!(mime, "audio/mpeg" | "audio/mp3") =>
        {
            Ok("audio/mpeg")
        }
        Kind::Preview
            if bytes.starts_with(b"OggS") && matches!(mime, "audio/ogg" | "application/ogg") =>
        {
            Ok("audio/ogg")
        }
        Kind::Preview
            if bytes.len() >= 12
                && &bytes[..4] == b"RIFF"
                && &bytes[8..12] == b"WAVE"
                && matches!(mime, "audio/wav" | "audio/x-wav") =>
        {
            Ok("audio/wav")
        }
        _ => Err(invalid()),
    }?;
    if matches!(kind, Kind::Cover(_)) {
        let mut reader = image::ImageReader::new(std::io::Cursor::new(bytes))
            .with_guessed_format()
            .map_err(|_| invalid())?;
        let mut limits = image::Limits::default();
        limits.max_image_width = Some(4096);
        limits.max_image_height = Some(4096);
        limits.max_alloc = Some(64 * 1024 * 1024);
        reader.limits(limits);
        let decoder = reader.into_decoder().map_err(|_| invalid())?;
        let (width, height) = decoder.dimensions();
        if width == 0
            || height == 0
            || width > 4096
            || height > 4096
            || u64::from(width) * u64::from(height) > 16 * 1024 * 1024
        {
            return Err(invalid());
        }
    }
    Ok(content_type)
}

fn media(bytes: Vec<u8>, kind: Kind, mime: &str) -> Result<Media, ApiError> {
    let content_type = validate_body(&bytes, kind, mime)?;
    let etag = HeaderValue::from_str(&format!("\"{:x}\"", Sha256::digest(&bytes)))
        .map_err(|_| ApiError(StatusCode::BAD_GATEWAY, "invalid_native_media_response"))?;
    Ok(Media {
        bytes: Bytes::from(bytes),
        content_type,
        etag,
    })
}

async fn fetch(client: &reqwest::Client, url: &str, kind: Kind) -> Result<Media, ApiError> {
    let unavailable = || ApiError(StatusCode::SERVICE_UNAVAILABLE, "native_media_unavailable");
    let mut response = client.get(url).send().await.map_err(|_| unavailable())?;
    if response.status() != StatusCode::OK {
        return Err(match response.status() {
            StatusCode::NOT_FOUND => ApiError(StatusCode::NOT_FOUND, "native_media_not_found"),
            StatusCode::TOO_MANY_REQUESTS => {
                ApiError(StatusCode::TOO_MANY_REQUESTS, "native_media_rate_limited")
            }
            _ => ApiError(StatusCode::BAD_GATEWAY, "native_media_upstream_failed"),
        });
    }
    let limit = match kind {
        Kind::Cover(_) => COVER_LIMIT,
        Kind::Preview => PREVIEW_LIMIT,
    };
    if response
        .content_length()
        .is_some_and(|size| size > limit as u64)
    {
        return Err(ApiError(
            StatusCode::BAD_GATEWAY,
            "invalid_native_media_response",
        ));
    }
    let mime = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| unavailable())? {
        if chunk.len() > limit.saturating_sub(bytes.len()) {
            return Err(ApiError(
                StatusCode::BAD_GATEWAY,
                "invalid_native_media_response",
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    media(bytes, kind, &mime)
}

async fn load(state: &AppState, key: MediaKey, limiter: String) -> Result<Media, ApiError> {
    // Every local size variant uses the same retained background bytes.
    let key = if key.set >= native_maps::LOCAL_ID_MIN && matches!(key.kind, Kind::Cover(_)) {
        MediaKey {
            kind: Kind::Cover(0),
            ..key
        }
    } else {
        key
    };
    native_maps::require_active(&state.pool, key.set).await?;
    if let Some(media) = cached(key) {
        return Ok(media);
    }
    common::rate_limit(state, format!("native-media:{limiter}"), 180).await?;
    let result = tokio::time::timeout(FETCH_DEADLINE, async {
        let permit = WORKERS
            .get_or_init(|| Arc::new(Semaphore::new(4)))
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "native_media_unavailable"))?;
        if let Some(media) = cached(key) {
            return Ok(media);
        }
        let media = if key.set >= native_maps::LOCAL_ID_MIN {
            let (bytes, mime) = local::load(state, key.set, key.kind == Kind::Preview).await?;
            media(bytes, key.kind, mime)?
        } else {
            fetch(&http()?, &upstream_url(key)?, key.kind).await?
        };
        drop(permit);
        Ok::<_, ApiError>(media)
    })
    .await;
    // Withdrawal takes precedence over a successful resource or failed wait.
    native_maps::require_active(&state.pool, key.set).await?;
    let media =
        result.map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "native_media_timeout"))??;
    remember(key, media.clone());
    Ok(media)
}

fn byte_range(value: &str, length: usize) -> Result<(usize, usize), ()> {
    if value.len() > 96 || length == 0 {
        return Err(());
    }
    let (start, end) = value
        .strip_prefix("bytes=")
        .and_then(|value| value.split_once('-'))
        .ok_or(())?;
    let parse = |value: &str| {
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            Err(())
        } else {
            value.parse::<usize>().map_err(|_| ())
        }
    };
    if start.is_empty() {
        let suffix = parse(end)?;
        if suffix == 0 {
            return Err(());
        }
        return Ok((length.saturating_sub(suffix), length - 1));
    }
    let start = parse(start)?;
    let end = if end.is_empty() {
        length - 1
    } else {
        parse(end)?.min(length - 1)
    };
    if start > end || start >= length {
        return Err(());
    }
    Ok((start, end))
}

fn response(media: Media, headers: &HeaderMap) -> Response {
    let length = media.bytes.len();
    let not_modified = headers.get_all(header::IF_NONE_MATCH).iter().any(|value| {
        value.to_str().is_ok_and(|value| {
            value.len() <= 4096
                && value.split(',').any(|tag| {
                    let tag = tag.trim();
                    tag == "*" || tag.strip_prefix("W/").unwrap_or(tag) == media.etag
                })
        })
    });
    let mut result = if not_modified {
        StatusCode::NOT_MODIFIED.into_response()
    } else {
        let range = headers.get_all(header::RANGE);
        let range_matches = headers
            .get(header::IF_RANGE)
            .is_none_or(|value| value == media.etag);
        if range.iter().count() > 1 {
            StatusCode::RANGE_NOT_SATISFIABLE.into_response()
        } else if let Some(range) = headers.get(header::RANGE).filter(|_| range_matches) {
            match range
                .to_str()
                .map_err(|_| ())
                .and_then(|value| byte_range(value, length))
            {
                Ok((start, end)) => {
                    let mut result = (
                        StatusCode::PARTIAL_CONTENT,
                        Body::from(media.bytes.slice(start..=end)),
                    )
                        .into_response();
                    result.headers_mut().insert(
                        header::CONTENT_RANGE,
                        HeaderValue::from_str(&format!("bytes {start}-{end}/{length}")).unwrap(),
                    );
                    result
                }
                Err(_) => StatusCode::RANGE_NOT_SATISFIABLE.into_response(),
            }
        } else {
            Body::from(media.bytes).into_response()
        }
    };
    if result.status() == StatusCode::RANGE_NOT_SATISFIABLE {
        result.headers_mut().insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes */{length}")).unwrap(),
        );
    }
    result.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(media.content_type),
    );
    result.headers_mut().insert(header::ETAG, media.etag);
    result
        .headers_mut()
        .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    result.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-cache"),
    );
    result.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    result
}

async fn cover(
    State(state): State<AppState>,
    Path((id, variant)): Path<(i64, String)>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let index = COVER_VARIANTS
        .iter()
        .position(|candidate| *candidate == variant)
        .ok_or(ApiError(StatusCode::NOT_FOUND, "native_media_not_found"))?;
    let limiter = peer
        .map(|Extension(ConnectInfo(peer))| common::client_key(&state, peer, &headers))
        .unwrap_or_else(|| "unidentified".into());
    Ok(response(
        load(
            &state,
            MediaKey {
                set: id,
                kind: Kind::Cover(index),
            },
            limiter,
        )
        .await?,
        &headers,
    ))
}
async fn preview(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let limiter = peer
        .map(|Extension(ConnectInfo(peer))| common::client_key(&state, peer, &headers))
        .unwrap_or_else(|| "unidentified".into());
    Ok(response(
        load(
            &state,
            MediaKey {
                set: id,
                kind: Kind::Preview,
            },
            limiter,
        )
        .await?,
        &headers,
    ))
}

#[cfg(test)]
#[path = "native_media_tests.rs"]
mod tests;
