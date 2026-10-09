//! Native lazer map lookup and archive routes. The map hash selects immutable
//! stored content; external osu! IDs are translated to internal primary keys.
use crate::{
    common::{self, ApiError, AppState},
    lazer,
};
use axum::{
    Json, Router,
    extract::{Path, Query, RawQuery, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use serde_json::{Value, json};
use sqlx::Row;
use std::collections::HashMap;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/v2/beatmaps/lookup", get(lookup))
        .route("/api/v2/beatmapsets/lookup", get(set_lookup))
        .route("/api/v2/beatmapsets/search", get(search))
        .route("/api/v2/beatmapsets/{id}", get(beatmapset))
        .route("/api/v2/beatmapsets/{id}/download", get(download))
        .route("/api/v2/beatmaps/{id}/scores", get(scores))
}

pub async fn resolve(state: &AppState, user_id: i64, id: i64) -> Result<i64, ApiError> {
    if !crate::native_maps::valid_id(id) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_beatmap_id"));
    }
    crate::native_maps::resolve_id(state, user_id, id)
        .await?
        .ok_or(ApiError(StatusCode::NOT_FOUND, "beatmap_not_found"))
}
async fn lookup(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let player = common::bearer(&state, &headers).await?;
    let native_id = query
        .get("id")
        .map(|value| {
            value
                .parse::<i64>()
                .ok()
                .filter(|id| crate::native_maps::valid_id(*id))
                .ok_or(ApiError(StatusCode::BAD_REQUEST, "invalid_beatmap_id"))
        })
        .transpose()?;
    let id = if let Some(hash) = query.get("checksum") {
        if !resonance_protocol::stable::is_md5(hash) {
            return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_beatmap_hash"));
        }
        let resolved = if let Some(native) = native_id {
            crate::native_maps::resolve_checksum_for_id(&state, player.id, hash, native).await?
        } else {
            crate::native_maps::resolve_checksum(&state, player.id, hash).await?
        };
        resolved.ok_or(ApiError(StatusCode::NOT_FOUND, "beatmap_not_found"))?
    } else if let Some(native) = native_id {
        resolve(&state, player.id, native).await?
    } else if let Some(filename) = query.get("filename") {
        crate::native_maps::resolve_filename(&state, player.id, filename)
            .await?
            .ok_or(ApiError(StatusCode::NOT_FOUND, "beatmap_not_found"))?
    } else {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "beatmap_id_or_checksum_required",
        ));
    };
    let map = lazer::map_json(&state, id).await?;
    if native_id.is_some_and(|native| map["id"].as_i64() != Some(native)) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "beatmap_id_mismatch"));
    }
    Ok(Json(map))
}
pub(crate) async fn local_set_json(state: &AppState, id: i64) -> Result<Value, ApiError> {
    if !crate::native_maps::valid_id(id) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_beatmapset_id"));
    }
    crate::native_maps::require_active(&state.pool, id).await?;
    let rows = sqlx::query("SELECT id FROM beatmaps WHERE native_set_id=$1 ORDER BY id")
        .bind(id)
        .fetch_all(&state.pool)
        .await?;
    if rows.is_empty() {
        return Err(ApiError(StatusCode::NOT_FOUND, "beatmapset_not_found"));
    }
    let mut maps = Vec::new();
    let mut set = None;
    for row in rows {
        let mut map = lazer::map_json(state, row.get("id")).await?;
        if set.is_none() {
            set = map.get("beatmapset").cloned();
        }
        map.as_object_mut()
            .ok_or(ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid_beatmap",
            ))?
            .remove("beatmapset");
        maps.push(map);
    }
    let mut set = set.ok_or(ApiError(
        StatusCode::INTERNAL_SERVER_ERROR,
        "invalid_beatmapset",
    ))?;
    set["beatmaps"] = json!(maps);
    summarize_local_set(&mut set)?;
    crate::native_maps::require_active(&state.pool, id).await?;
    Ok(set)
}

fn native_rank(status: &str) -> Result<i32, ApiError> {
    match status {
        "graveyard" => Ok(-2),
        "wip" => Ok(-1),
        "pending" | "unranked" => Ok(0),
        "ranked" => Ok(1),
        "approved" => Ok(2),
        "qualified" => Ok(3),
        "loved" => Ok(4),
        _ => Err(ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "invalid_stored_beatmap_status",
        )),
    }
}

fn summarize_local_set(set: &mut Value) -> Result<(), ApiError> {
    let maps = set["beatmaps"]
        .as_array()
        .filter(|maps| !maps.is_empty())
        .ok_or(ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "invalid_stored_beatmapset",
        ))?;
    let first = maps[0]["status"].as_str().ok_or(ApiError(
        StatusCode::INTERNAL_SERVER_ERROR,
        "invalid_stored_beatmap_status",
    ))?;
    for map in maps {
        native_rank(map["status"].as_str().unwrap_or_default())?;
    }
    let status = if maps.iter().all(|map| map["status"].as_str() == Some(first)) {
        first.to_owned()
    } else {
        "pending".into()
    };
    set["ranked"] = json!(native_rank(&status)?);
    set["status"] = json!(if status == "unranked" {
        "pending"
    } else {
        &status
    });
    Ok(())
}
async fn beatmapset(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Result<Json<Value>, ApiError> {
    let player = common::bearer(&state, &headers).await?;
    let result = async {
        let mut set = crate::native_set_metadata::set_json(&state, player.id, id).await?;
        apply_private_difficulty_statuses(&state, id, &mut set).await?;
        Ok::<_, ApiError>(set)
    }
    .await;
    common::bearer(&state, &headers).await?;
    Ok(Json(result?))
}
async fn set_lookup(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let player = common::bearer(&state, &headers).await?;
    let native = query
        .get("beatmap_id")
        .and_then(|v| v.parse().ok())
        .filter(|id| crate::native_maps::valid_id(*id))
        .ok_or(ApiError(StatusCode::BAD_REQUEST, "invalid_beatmap_id"))?;
    let internal = resolve(&state, player.id, native).await?;
    let set: i64 = sqlx::query_scalar("SELECT native_set_id FROM beatmaps WHERE id=$1")
        .bind(internal)
        .fetch_one(&state.pool)
        .await?;
    let result = async {
        let mut value = crate::native_set_metadata::set_json(&state, player.id, set).await?;
        apply_private_difficulty_statuses(&state, set, &mut value).await?;
        Ok::<_, ApiError>(value)
    }
    .await;
    common::bearer(&state, &headers).await?;
    Ok(Json(result?))
}

async fn apply_private_difficulty_statuses(
    state: &AppState,
    set_id: i64,
    set: &mut Value,
) -> Result<(), ApiError> {
    let maps = set["beatmaps"]
        .as_array()
        .filter(|maps| !maps.is_empty() && maps.len() <= 512)
        .ok_or(ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "invalid_stored_beatmapset",
        ))?;
    let ids = maps
        .iter()
        .map(|map| {
            map["id"]
                .as_i64()
                .filter(|id| crate::native_maps::valid_id(*id))
                .ok_or(ApiError(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "invalid_stored_beatmap_identity",
                ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let rows = sqlx::query("SELECT candidate.id AS native_id, o.status, illya_beatmapset_active($1) AS active FROM unnest($2::bigint[]) AS candidate(id) LEFT JOIN beatmap_status_overrides o ON o.native_id=candidate.id AND o.native_set_id=$1")
        .bind(set_id).bind(ids).fetch_all(&state.pool).await?;
    let mut changed = false;
    let maps = set["beatmaps"].as_array_mut().unwrap();
    for row in rows {
        if !row.get::<bool, _>("active") {
            return Err(ApiError(StatusCode::GONE, "beatmap_withdrawn"));
        }
        if let Some(status) = row.get::<Option<String>, _>("status") {
            let rank = native_rank(&status)?;
            let id = row.get::<i64, _>("native_id");
            let map = maps
                .iter_mut()
                .find(|map| map["id"].as_i64() == Some(id))
                .ok_or(ApiError(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "invalid_stored_beatmap_identity",
                ))?;
            map["status"] = json!(if status == "unranked" {
                "pending"
            } else {
                &status
            });
            map["ranked"] = json!(rank);
            changed = true;
        }
    }
    // A single ranked difficulty cannot promote its siblings. Keep the
    // upstream set label for mixed official members and aggregate unanimity.
    if changed {
        let first = maps[0]["status"].as_str().unwrap_or_default().to_owned();
        if maps
            .iter()
            .all(|map| map["status"].as_str() == Some(&first))
        {
            set["status"] = json!(first);
            set["ranked"] = json!(native_rank(set["status"].as_str().unwrap())?);
        }
    }
    Ok(())
}
async fn download(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Result<Response, ApiError> {
    common::bearer(&state, &headers).await?;
    if !crate::native_maps::valid_id(id) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_beatmapset_id"));
    }
    let path = crate::native_maps::archive_path(&state, id)
        .await?
        .ok_or(ApiError(StatusCode::NOT_FOUND, "archive_not_available"))?;
    let bytes = common::stored_file(&state, &path, "maps", 104_857_600).await?;
    crate::native_maps::require_active(&state.pool, id).await?;
    Ok((
        [
            ("content-type", "application/x-osu-beatmap-archive"),
            ("content-disposition", "attachment; filename=beatmap.osz"),
        ],
        bytes,
    )
        .into_response())
}
async fn search(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Result<Json<Value>, ApiError> {
    let player = common::bearer(&state, &headers).await?;
    let query = search_query(raw)?;
    let result = if query.get("source").map(String::as_str) == Some("local") {
        local_search(&state, &query).await
    } else {
        let pairs = query
            .iter()
            .filter(|(name, _)| name.as_str() != "source")
            .collect::<Vec<_>>();
        let encoded = serde_urlencoded::to_string(pairs)
            .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid_search_query"))?;
        match tokio::time::timeout(
            std::time::Duration::from_secs(8),
            official_search(&state, player.id, &encoded),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "official_search_unavailable",
            )),
        }
    };
    // A token revoked while waiting cannot consume a cached official response
    // or disclose an upstream failure in place of its authentication failure.
    common::bearer(&state, &headers).await?;
    Ok(Json(result?))
}

fn search_query(raw: Option<String>) -> Result<HashMap<String, String>, ApiError> {
    let raw = raw.unwrap_or_default();
    if raw.len() > 8192 {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_search_query"));
    }
    let mut decoded = Vec::with_capacity(raw.len());
    let mut bytes = raw.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'%' {
            let first = bytes.next().and_then(|byte| (byte as char).to_digit(16));
            let second = bytes.next().and_then(|byte| (byte as char).to_digit(16));
            match (first, second) {
                (Some(first), Some(second)) => decoded.push((first * 16 + second) as u8),
                _ => return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_search_query")),
            }
        } else {
            decoded.push(byte);
        }
    }
    if std::str::from_utf8(&decoded).is_err() {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_search_query"));
    }
    let pairs: Vec<(String, String)> = serde_urlencoded::from_str(&raw)
        .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid_search_query"))?;
    if pairs.len() > 32 {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_search_query"));
    }
    let mut query = HashMap::new();
    for (name, value) in pairs {
        if query.insert(name, value).is_some() {
            return Err(ApiError(StatusCode::BAD_REQUEST, "duplicate_search_query"));
        }
    }
    if query
        .get("source")
        .is_some_and(|source| !matches!(source.as_str(), "official" | "local"))
    {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_search_source"));
    }
    Ok(query)
}

async fn official_search(state: &AppState, user_id: i64, query: &str) -> Result<Value, ApiError> {
    if state.service_key.len() < 32 {
        return Err(ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "official_search_not_configured",
        ));
    }
    let mut response = state
        .http
        .post(format!(
            "{}/api/v1/internal/beatmapsets/search",
            state.sanctuary_url.trim_end_matches('/')
        ))
        .header("x-illya-service-key", state.service_key.as_str())
        .json(&json!({"user_id":user_id,"query":query}))
        .send()
        .await
        .map_err(|_| {
            ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "official_search_unavailable",
            )
        })?;
    if response.status() != StatusCode::OK {
        return Err(match response.status() {
            StatusCode::BAD_REQUEST => ApiError(
                StatusCode::BAD_REQUEST,
                "unsupported_or_invalid_search_filter",
            ),
            StatusCode::UNAUTHORIZED => ApiError(StatusCode::UNAUTHORIZED, "invalid_account"),
            StatusCode::TOO_MANY_REQUESTS => ApiError(
                StatusCode::TOO_MANY_REQUESTS,
                "official_search_rate_limited",
            ),
            StatusCode::SERVICE_UNAVAILABLE => ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "official_search_unavailable",
            ),
            _ => ApiError(StatusCode::BAD_GATEWAY, "official_search_failed"),
        });
    }
    const LIMIT: usize = 4 * 1024 * 1024;
    let invalid = || ApiError(StatusCode::BAD_GATEWAY, "invalid_official_search_response");
    if response
        .content_length()
        .is_some_and(|length| length > LIMIT as u64)
    {
        return Err(invalid());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| invalid())? {
        if chunk.len() > LIMIT - bytes.len() {
            return Err(invalid());
        }
        bytes.extend_from_slice(&chunk);
    }
    let mut value: Value = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    validate_official_search(&value)?;
    let sets = value["beatmapsets"].as_array_mut().unwrap();
    if !sets.is_empty() {
        let ids = sets
            .iter()
            .map(|set| set["id"].as_i64().unwrap())
            .collect::<Vec<_>>();
        let active: Vec<i64> = sqlx::query_scalar("SELECT id FROM unnest($1::bigint[]) AS candidate(id) WHERE illya_beatmapset_active(id)")
            .bind(ids).fetch_all(&state.pool).await?;
        sets.retain(|set| active.contains(&set["id"].as_i64().unwrap()));
    }
    Ok(value)
}

fn validate_official_search(value: &Value) -> Result<(), ApiError> {
    let invalid = || ApiError(StatusCode::BAD_GATEWAY, "invalid_official_search_response");
    let sets = value["beatmapsets"]
        .as_array()
        .filter(|sets| sets.len() <= 50)
        .ok_or_else(invalid)?;
    value["total"]
        .as_i64()
        .filter(|total| (0..=i32::MAX as i64).contains(total))
        .ok_or_else(invalid)?;
    if !matches!(value.get("cursor"), Some(Value::Null | Value::Object(_)))
        || !matches!(
            value.get("cursor_string"),
            None | Some(Value::Null | Value::String(_))
        )
    {
        return Err(invalid());
    }
    let mut set_ids = std::collections::HashSet::new();
    let mut map_ids = std::collections::HashSet::new();
    for set in sets {
        let id = set["id"]
            .as_i64()
            .filter(|id| (1..crate::native_maps::LOCAL_ID_MIN).contains(id))
            .ok_or_else(invalid)?;
        if !set_ids.insert(id) {
            return Err(invalid());
        }
        let maps = set["beatmaps"]
            .as_array()
            .filter(|maps| !maps.is_empty() && maps.len() <= 512)
            .ok_or_else(invalid)?;
        for map in maps {
            let map_id = map["id"]
                .as_i64()
                .filter(|id| (1..crate::native_maps::LOCAL_ID_MIN).contains(id))
                .ok_or_else(invalid)?;
            if !map_ids.insert(map_id) || map["beatmapset_id"].as_i64() != Some(id) {
                return Err(invalid());
            }
        }
    }
    Ok(())
}

async fn local_search(
    state: &AppState,
    query: &HashMap<String, String>,
) -> Result<Value, ApiError> {
    if query
        .keys()
        .any(|name| !matches!(name.as_str(), "source" | "q" | "m" | "s" | "cursor_string"))
    {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "unsupported_local_search_filter",
        ));
    }
    let q = query.get("q").cloned().unwrap_or_default();
    if q.len() > 800 || q.chars().count() > 200 || q.chars().any(char::is_control) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_search_query"));
    }
    let mode = query
        .get("m")
        .map(|value| {
            value
                .parse::<i16>()
                .ok()
                .filter(|mode| (-1..=3).contains(mode))
                .ok_or(ApiError(StatusCode::BAD_REQUEST, "invalid_mode"))
        })
        .transpose()?
        .filter(|mode| *mode >= 0);
    let status = match query.get("s").map(String::as_str) {
        None | Some("any") => None,
        Some(status @ ("ranked" | "loved" | "pending" | "unranked" | "graveyard" | "wip")) => {
            Some(status)
        }
        Some(_) => return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_search_status")),
    };
    let offset: i64 = query
        .get("cursor_string")
        .map(|value| {
            value
                .parse()
                .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid_search_cursor"))
        })
        .transpose()?
        .unwrap_or(0);
    if !(0..=1_000_000).contains(&offset) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_search_cursor"));
    }
    // Search the canonical set membership, including every package difficulty.
    let total:i64=sqlx::query_scalar("SELECT count(DISTINCT b.native_set_id) FROM active_beatmaps b WHERE concat_ws(' ',b.title,b.artist,b.creator,b.version) ILIKE '%'||$1||'%' AND ($2::smallint IS NULL OR b.mode=$2) AND ($3::text IS NULL OR b.status=$3)").bind(&q).bind(mode).bind(status).fetch_one(&state.pool).await?;
    let ids=sqlx::query_scalar::<_,i64>("SELECT DISTINCT b.native_set_id FROM active_beatmaps b WHERE concat_ws(' ',b.title,b.artist,b.creator,b.version) ILIKE '%'||$1||'%' AND ($2::smallint IS NULL OR b.mode=$2) AND ($3::text IS NULL OR b.status=$3) ORDER BY 1 DESC LIMIT 50 OFFSET $4").bind(q).bind(mode).bind(status).bind(offset).fetch_all(&state.pool).await?;
    let mut sets = Vec::new();
    for id in ids {
        sets.push(local_set_json(state, id).await?);
    }
    Ok(
        json!({"beatmapsets":sets,"total":total,"cursor":null,"cursor_string":if offset+50<total{Some((offset+50).to_string())}else{None},"error":null}),
    )
}

#[cfg(test)]
#[path = "lazer_search_tests.rs"]
mod search_tests;
async fn scores(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    RawQuery(raw): RawQuery,
) -> Result<Json<Value>, ApiError> {
    let raw = raw.unwrap_or_default();
    if raw.len() > 4096 {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_score_query"));
    }
    let pairs: Vec<(String, String)> = serde_urlencoded::from_str(&raw)
        .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid_score_query"))?;
    if pairs.len() > 64 {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_score_query"));
    }
    let mut query = HashMap::new();
    let mut requested_mods = Vec::new();
    for (key, value) in pairs {
        if key == "mods[]" {
            requested_mods.push(resonance_protocol::lazer::ApiMod {
                acronym: value,
                settings: Default::default(),
            });
        } else if query.insert(key, value).is_some() {
            return Err(ApiError(StatusCode::BAD_REQUEST, "duplicate_score_query"));
        }
    }
    let user = common::bearer(&state, &headers).await?;
    let mode = match query.get("mode").map(String::as_str).unwrap_or("osu") {
        "osu" => 0i16,
        "taiko" => 1,
        "fruits" | "catch" => 2,
        "mania" => 3,
        _ => return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_mode")),
    };
    let scope = query.get("type").map(String::as_str).unwrap_or("global");
    if !matches!(scope, "global" | "country" | "friend" | "friends") {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_score_scope"));
    }
    let limit: i64 = query
        .get("limit")
        .and_then(|v| v.parse().ok())
        .unwrap_or(50)
        .clamp(1, 100);
    let filter_acronyms = if requested_mods.len() == 1 && requested_mods[0].acronym == "NM" {
        requested_mods.clear();
        Some(Vec::<String>::new())
    } else if requested_mods.is_empty() {
        None
    } else {
        Some(
            requested_mods
                .iter()
                .map(|modifier| modifier.acronym.clone())
                .collect::<Vec<_>>(),
        )
    };
    let mods = if filter_acronyms.is_none() {
        None
    } else {
        Some(i64::from(lazer::legacy_mods(mode as u8, &requested_mods)?))
    };
    let derived_variant = match mods {
        Some(bits) => illya_domain::ScoreVariant::from_legacy(mode as u8, bits as u32)
            .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid_score_variant"))?,
        None => illya_domain::ScoreVariant::Vanilla,
    };
    let variant = if let Some(value) = query.get("variant") {
        let explicit = lazer::requested_variant(mode as u8, Some(value))?;
        if mods.is_some() && explicit != derived_variant {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "conflicting_score_variant",
            ));
        }
        explicit
    } else {
        derived_variant
    };
    let map = resolve(&state, user.id, id).await?;
    let friends = if matches!(scope, "friend" | "friends") {
        let mut ids = common::friend_ids(&state, user.id)
            .await?
            .into_iter()
            .map(i64::from)
            .collect::<Vec<_>>();
        ids.push(user.id);
        Some(ids)
    } else {
        None
    };
    let rows=sqlx::query("WITH best AS (SELECT DISTINCT ON(s.user_id) s.id,s.user_id,CASE WHEN $9='relax' THEN s.pp ELSE s.standardised_score::float8 END rank_value FROM scores s JOIN users u ON u.id=s.user_id WHERE s.beatmap_id=$1 AND s.mode=$2 AND s.score_variant=$9 AND (s.standardised_score IS NOT NULL OR $9='relax') AND s.client IN ('stable','lazer') AND s.verification='verified' AND s.passed AND NOT u.is_restricted AND ($3::bigint IS NULL OR (s.api_mods IS NULL AND s.mods=$3 AND ((s.client='stable' AND 'CL'=ANY($8::text[])) OR (s.client='lazer' AND NOT ('CL'=ANY($8::text[]))))) OR (s.api_mods IS NOT NULL AND jsonb_array_length(s.api_mods)=cardinality($8::text[]) AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(s.api_mods) modifier WHERE NOT (modifier->>'acronym'=ANY($8::text[]))))) AND ($3::bigint IS NOT NULL OR s.pp_eligible) AND ($9='vanilla' OR s.pp_eligible) AND ($4::text IS NULL OR u.country=$4) AND ($5::bigint[] IS NULL OR s.user_id=ANY($5)) ORDER BY s.user_id,CASE WHEN $9='relax' THEN s.pp ELSE s.standardised_score::float8 END DESC,s.id),placed AS (SELECT *,row_number() OVER(ORDER BY rank_value DESC,id) position,count(*) OVER() score_count FROM best) SELECT * FROM placed WHERE position<=$6 OR user_id=$7 ORDER BY position").bind(map).bind(mode).bind(mods).bind(if scope=="country"{Some(&user.country)}else{None}).bind(friends).bind(limit).bind(user.id).bind(filter_acronyms).bind(variant.as_str()).fetch_all(&state.pool).await?;
    let score_count = rows
        .first()
        .map(|r| r.get::<i64, _>("score_count"))
        .unwrap_or(0);
    let mut scores = Vec::new();
    let mut own = Value::Null;
    for row in rows {
        let score = lazer::score_json(&state, row.get("id"), &user).await?;
        let position: i64 = row.get("position");
        if row.get::<i64, _>("user_id") == user.id {
            own = json!({"position":position,"score":score});
        }
        if position <= limit {
            scores.push(score);
        }
    }
    crate::native_maps::require_active_map(&state.pool, map).await?;
    Ok(Json(
        json!({"scores":scores,"user_score":own,"score_count":score_count,"score_variant":variant,"ruleset_id":mode}),
    ))
}
