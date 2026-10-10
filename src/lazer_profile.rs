//! Native profile sections use stored community data, never the official user's
//! identity or invented activity. Empty unsupported collections remain empty.
use super::*;

const FIRST_SCORES: &str = "WITH firsts AS (
    SELECT DISTINCT ON(s.beatmap_id) s.id,s.user_id,
        CASE WHEN $3='relax' THEN s.pp ELSE s.standardised_score::float8 END rank_value
    FROM scores s JOIN users u ON u.id=s.user_id JOIN active_beatmaps b ON b.id=s.beatmap_id
    WHERE s.mode=$2 AND s.score_variant=$3 AND s.verification='verified' AND s.passed AND NOT u.is_restricted
      AND s.client IN('stable','lazer') AND s.pp_eligible AND b.status IN('ranked','loved')
      AND (s.standardised_score IS NOT NULL OR $3='relax')
    ORDER BY s.beatmap_id,rank_value DESC,s.id) ";

const OWNED_SETS: &str = "WITH sets AS (
    SELECT b.native_set_id,min(b.id) first_id,
        CASE WHEN bool_and(b.status='ranked') THEN 'ranked'
             WHEN bool_and(b.status='loved') THEN 'loved'
             WHEN bool_and(b.status='graveyard') THEN 'graveyard'
             ELSE 'pending' END status
    FROM active_beatmaps b WHERE b.source='local' AND b.uploaded_by=$1 GROUP BY b.native_set_id) ";

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/v2/users/{id}/recent_activity", get(recent_activity))
        .route(
            "/api/v2/users/{id}/beatmapsets/{kind}",
            get(user_beatmapsets),
        )
        .route("/api/v2/users/{id}/kudosu", get(kudosu))
        .route("/api/v2/me/beatmapset-favourites", get(my_favourites))
}

fn pagination(query: &HashMap<String, String>) -> Result<(i64, i64), ApiError> {
    let parse = |name: &str, default: i64| {
        query
            .get(name)
            .map(|v| v.parse::<i64>())
            .transpose()
            .map(|v| v.unwrap_or(default))
            .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid_profile_pagination"))
    };
    let limit = parse("limit", 50)?;
    let offset = parse("offset", 0)?;
    if !(1..=100).contains(&limit) || !(0..=10000).contains(&offset) {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "invalid_profile_pagination",
        ));
    }
    Ok((limit, offset))
}

fn asset_url(origin: &str, path: &str, kind: &str) -> Option<String> {
    let name = path.strip_prefix(&format!("/media/{kind}/"))?;
    if !(6..=128).contains(&name.len())
        || !name.ends_with(".webp")
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        || name.contains("..")
    {
        return None;
    }
    Some(format!("{origin}{path}"))
}

pub(super) fn avatar_url(path: Option<&str>) -> Option<String> {
    let path = path?;
    let origin = crate::native_media::deployment_origin().ok()?;
    asset_url(&origin, path, "avatar")
}

pub(super) async fn enrich_user(
    state: &AppState,
    player: &Account,
    ruleset: u8,
    variant: ScoreVariant,
    value: &mut Value,
) -> Result<(), ApiError> {
    let details = sqlx::query("SELECT bio,banner_url,accent_color,
        (SELECT max(visited) FROM (
            SELECT last_seen_at visited FROM lazer_sessions WHERE user_id=users.id
            UNION ALL SELECT created_at FROM bancho_sessions WHERE user_id=users.id
            UNION ALL SELECT created_at FROM scores WHERE user_id=users.id AND verification='verified'
        ) visits) last_visit,
        (SELECT count(*) FROM user_relationships WHERE target_id=users.id AND relation_type='friend') follower_count
        FROM users WHERE id=$1 AND NOT is_restricted")
        .bind(player.id).fetch_one(&state.pool).await?;
    // Stock LocalUserState treats a null last_visit as a privacy choice and
    // forces Offline on login. A recorded session is a real visible visit; the
    // launcher's separately saved Offline choice remains authoritative.
    value["last_visit"] = json!(details.get::<Option<chrono::DateTime<Utc>>, _>("last_visit"));
    let banner: Option<String> = details.get("banner_url");
    let origin = if banner.is_some() || player.avatar_url.is_some() {
        Some(crate::native_media::deployment_origin()?)
    } else {
        None
    };
    let banner = banner
        .as_deref()
        .and_then(|path| asset_url(origin.as_deref()?, path, "banner"));
    value["cover"] = json!({"custom_url":banner,"url":banner,"id":null});
    value["avatar_url"] = json!(player.avatar_url.as_deref().and_then(|path| asset_url(
        origin.as_deref()?,
        path,
        "avatar"
    )));
    value["profile_colour"] = json!(details.get::<String, _>("accent_color"));
    value["profile_order"] = json!([
        "recent_activity",
        "top_ranks",
        "historical",
        "beatmaps",
        "kudosu"
    ]);
    value["follower_count"] = json!(details.get::<i64, _>("follower_count"));
    value["mapping_follower_count"] = json!(0);
    value["page"] = json!({"html":null,"raw":details.get::<String,_>("bio")});
    value["previous_usernames"] = json!([]);
    value["badges"] = json!([]);
    value["active_tournament_banners"] = json!([]);

    let counts = sqlx::query("SELECT
        count(*) FILTER(WHERE s.passed)::bigint recent,
        count(DISTINCT s.beatmap_id) FILTER(WHERE s.passed AND s.pp_eligible AND b.status='ranked' AND illya_beatmapset_active(b.native_set_id))::bigint best
        FROM scores s JOIN beatmaps b ON b.id=s.beatmap_id
        WHERE s.user_id=$1 AND s.mode=$2 AND s.score_variant=$3 AND s.verification='verified'")
        .bind(player.id).bind(i16::from(ruleset)).bind(variant.as_str()).fetch_one(&state.pool).await?;
    value["scores_best_count"] = json!(counts.get::<i64, _>("best"));
    value["scores_recent_count"] = json!(counts.get::<i64, _>("recent"));
    value["scores_first_count"] = json!(
        sqlx::query_scalar::<_, i64>(&format!(
            "{FIRST_SCORES}SELECT count(*) FROM firsts WHERE user_id=$1"
        ))
        .bind(player.id)
        .bind(i16::from(ruleset))
        .bind(variant.as_str())
        .fetch_one(&state.pool)
        .await?
    );
    value["scores_pinned_count"] = json!(0);
    let most_played: i64 = sqlx::query_scalar("SELECT count(DISTINCT beatmap_id) FROM scores WHERE user_id=$1 AND verification='verified'")
        .bind(player.id).fetch_one(&state.pool).await?;
    value["beatmap_playcounts_count"] = json!(most_played);
    for (kind, status) in [
        ("ranked", "ranked"),
        ("loved", "loved"),
        ("pending", "pending"),
        ("graveyard", "graveyard"),
    ] {
        value[format!("{kind}_beatmapset_count")] = json!(
            sqlx::query_scalar::<_, i64>(&format!(
                "{OWNED_SETS}SELECT count(*) FROM sets WHERE status=$2"
            ))
            .bind(player.id)
            .bind(status)
            .fetch_one(&state.pool)
            .await?
        );
    }
    for kind in ["favourite", "guest", "nominated"] {
        value[format!("{kind}_beatmapset_count")] = json!(0);
    }
    // Badge roles only, with the operator-chosen label and colour (osu-web group style).
    let groups = sqlx::query("SELECT r.id,r.name,r.short_name,r.colour FROM roles r JOIN user_roles ur ON ur.role_id=r.id WHERE ur.user_id=$1 AND r.display_badge ORDER BY r.id")
        .bind(player.id).fetch_all(&state.pool).await?;
    value["groups"] = json!(groups.into_iter().map(|row| {
        let name: String = row.get("name");
        let colour = row.get::<Option<String>,_>("colour").unwrap_or_else(|| details.get::<String,_>("accent_color"));
        json!({"id":row.get::<i64,_>("id"),"name":name,"short_name":row.get::<String,_>("short_name"),"identifier":name,"colour":colour,"playmodes":null,"is_probationary":false})
    }).collect::<Vec<_>>());
    value["is_admin"] = json!(player.can("*"));
    value["is_gmt"] = json!(player.can("users.moderate"));
    value["is_bng"] = json!(player.can("beatmaps.moderate"));

    let months = sqlx::query("SELECT date_trunc('month',created_at AT TIME ZONE 'UTC')::date start_date,count(*)::bigint count FROM scores WHERE user_id=$1 AND mode=$2 AND score_variant=$3 AND verification='verified' GROUP BY start_date ORDER BY start_date")
        .bind(player.id).bind(i16::from(ruleset)).bind(variant.as_str()).fetch_all(&state.pool).await?;
    value["monthly_playcounts"] = json!(months.into_iter().map(|row| json!({"start_date":row.get::<chrono::NaiveDate,_>("start_date"),"count":row.get::<i64,_>("count")})).collect::<Vec<_>>());
    // RankGraph explicitly drops zero observations, preserving gaps without
    // pretending a missing collector day held the preceding rank.
    let history = sqlx::query_scalar::<_, Option<i64>>("SELECT h.rank FROM generate_series((clock_timestamp() AT TIME ZONE 'UTC')::date-89,(clock_timestamp() AT TIME ZONE 'UTC')::date,interval '1 day') days(date) LEFT JOIN rank_history_daily h ON h.date=days.date::date AND h.user_id=$1 AND h.mode=$2 AND h.variant=$3 ORDER BY days.date")
        .bind(player.id).bind(i16::from(ruleset)).bind(variant.as_str()).fetch_all(&state.pool).await?;
    let short_name = ["osu", "taiko", "fruits", "mania"][ruleset as usize];
    value["rank_history"] = json!({"mode":short_name,"data":history.into_iter().map(|rank|rank.unwrap_or(0)).collect::<Vec<_>>()});
    value["rank_highest"] = sqlx::query_scalar::<_,Value>("SELECT jsonb_build_object('rank',rank,'updated_at',captured_at) FROM rank_history_daily WHERE user_id=$1 AND mode=$2 AND variant=$3 AND rank IS NOT NULL ORDER BY rank,captured_at LIMIT 1")
        .bind(player.id).bind(i16::from(ruleset)).bind(variant.as_str()).fetch_optional(&state.pool).await?.unwrap_or(Value::Null);
    // No gameplay-duration or replay-view telemetry is stored. Null and empty
    // history express unknown rather than deriving playtime from idle tokens.
    value["statistics"]["play_time"] = Value::Null;
    value["replays_watched_counts"] = json!([]);
    Ok(())
}

pub(super) async fn enrich_score(state: &AppState, mut value: Value) -> Result<Value, ApiError> {
    let id = value["beatmap_id"].as_i64().ok_or(ApiError(
        StatusCode::INTERNAL_SERVER_ERROR,
        "invalid_stored_score_identity",
    ))?;
    let score_id = value["id"].as_i64().ok_or(ApiError(
        StatusCode::INTERNAL_SERVER_ERROR,
        "invalid_stored_score_identity",
    ))?;
    let (internal, checksum): (i64, String) = sqlx::query_as(
        "SELECT b.id,COALESCE(s.client_beatmap_md5,b.md5) FROM scores s JOIN beatmaps b ON b.id=s.beatmap_id WHERE s.id=$1 AND b.native_id=$2",
    )
        .bind(score_id)
        .bind(id)
        .fetch_one(&state.pool)
        .await?;
    // Native profile cards dereference beatmap. Keep its beatmapset nested so
    // deserialization has no property-order dependency in SoloScoreInfo.
    value["beatmap"] = crate::native_maps::retained_map_json(state, internal).await?;
    // A downloaded local package can advertise a derivative checksum later.
    // Historical score/replay lookup must retain the checksum admitted when
    // this score was played, including original-checksum legacy NULL records.
    value["beatmap"]["checksum"] = json!(checksum);
    Ok(value)
}

pub(super) async fn first_score_ids(
    pool: &sqlx::PgPool,
    user: i64,
    ruleset: u8,
    variant: ScoreVariant,
    limit: i64,
    offset: i64,
) -> Result<Vec<i64>, ApiError> {
    Ok(sqlx::query_scalar(&format!("{FIRST_SCORES}SELECT id FROM firsts WHERE user_id=$1 ORDER BY rank_value DESC,id LIMIT $4 OFFSET $5"))
        .bind(user).bind(i16::from(ruleset)).bind(variant.as_str()).bind(limit).bind(offset).fetch_all(pool).await?)
}

async fn owned_set_ids(
    pool: &sqlx::PgPool,
    owner: i64,
    status: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<i64>, ApiError> {
    Ok(sqlx::query_scalar(&format!("{OWNED_SETS}SELECT native_set_id FROM sets WHERE status=$2 ORDER BY first_id DESC LIMIT $3 OFFSET $4"))
        .bind(owner).bind(status).bind(limit).bind(offset).fetch_all(pool).await?)
}

async fn user_beatmapsets(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((id, kind)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    common::bearer(&state, &headers).await?;
    let owner = find_user(&state, &id).await?;
    let (limit, offset) = pagination(&query)?;
    if kind == "most_played" {
        let rows = sqlx::query("SELECT beatmap_id,count(*)::bigint count FROM scores WHERE user_id=$1 AND verification='verified' GROUP BY beatmap_id ORDER BY count DESC,beatmap_id LIMIT $2 OFFSET $3")
            .bind(owner.id).bind(limit).bind(offset).fetch_all(&state.pool).await?;
        let mut maps = Vec::new();
        for row in rows {
            let map = crate::native_maps::retained_map_json(&state, row.get("beatmap_id")).await?;
            maps.push(json!({"beatmap_id":map["id"],"count":row.get::<i64,_>("count"),"beatmapset":map["beatmapset"],"beatmap":map}));
        }
        return Ok(Json(json!(maps)));
    }
    let sets = match kind.as_str() {
        "ranked" | "loved" | "pending" | "graveyard" => {
            owned_set_ids(&state.pool, owner.id, &kind, limit, offset).await?
        }
        "favourite" | "guest" | "nominated" => Vec::new(),
        _ => {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "unsupported_beatmapset_list",
            ));
        }
    };
    let mut result = Vec::new();
    for set in sets {
        result.push(crate::beatmaps::local_set_json(&state, set).await?);
    }
    Ok(Json(json!(result)))
}

async fn recent_activity(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    common::bearer(&state, &headers).await?;
    let owner = find_user(&state, &id).await?;
    let (limit, offset) = pagination(&query)?;
    let rows = sqlx::query("SELECT min(b.id) id,b.native_set_id,min(b.created_at) created_at,min(b.title) title,min(b.artist) artist FROM active_beatmaps b WHERE b.source='local' AND b.uploaded_by=$1 GROUP BY b.native_set_id ORDER BY created_at DESC,id DESC LIMIT $2 OFFSET $3")
        .bind(owner.id).bind(limit).bind(offset).fetch_all(&state.pool).await?;
    Ok(Json(json!(rows.into_iter().map(|row| json!({"id":row.get::<i64,_>("id"),"type":"beatmapsetUpload","createdAt":row.get::<chrono::DateTime<Utc>,_>("created_at"),"user":{"username":owner.username,"url":format!("/users/{}",owner.id)},"beatmapset":{"title":format!("{} - {}",row.get::<String,_>("artist"),row.get::<String,_>("title")),"url":format!("/beatmapsets/{}",row.get::<i64,_>("native_set_id"))}})).collect::<Vec<_>>())) )
}

async fn kudosu(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    common::bearer(&state, &headers).await?;
    find_user(&state, &id).await?;
    pagination(&query)?;
    // Illyaverse has no kudosu awards ledger; no award is fabricated.
    Ok(Json(json!([])))
}

async fn my_favourites(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    common::bearer(&state, &headers).await?;
    // The catalogue has no persisted favourites yet. Stock expects the named
    // ID collection; routing this path as an invalid ruleset breaks loading.
    Ok(Json(json!({"beatmapset_ids":[]})))
}

#[cfg(test)]
#[path = "lazer_profile_tests.rs"]
mod tests;
