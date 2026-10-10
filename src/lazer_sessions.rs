//! Private client credential lifecycle. Stock lazer clears local credentials on
//! logout without notifying its server; the bridge can explicitly revoke the
//! current credential here without signing out other devices.
use crate::common::{self, ApiError, AppState};
use axum::{
    Router,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::delete,
};
use sqlx::{Postgres, Transaction};

pub(super) fn router() -> Router<AppState> {
    Router::new().route("/api/v2/oauth/tokens/current", delete(revoke_current))
}

async fn revoke_current(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let hash = common::bearer_hash(&headers)?;
    let mut tx = state.pool.begin().await?;
    // Discovery takes no session lock. All credential mutations acquire users
    // before sessions, including refresh, password recovery and website revoke.
    let user_id: i64 = sqlx::query_scalar(
        "SELECT user_id FROM lazer_sessions WHERE access_hash=$1 AND access_expires_at>clock_timestamp()",
    )
    .bind(&hash)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError(StatusCode::UNAUTHORIZED, "invalid_access_token"))?;
    sqlx::query("SELECT id FROM users WHERE id=$1 FOR UPDATE")
        .bind(user_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(ApiError(StatusCode::UNAUTHORIZED, "invalid_access_token"))?;
    // A restricted account may still discard its own credential. Recheck the
    // exact access generation and expiry after waiting; an old access token
    // cannot delete the newer credential if a refresh won the user lock.
    let deleted = sqlx::query(
        "DELETE FROM lazer_sessions WHERE user_id=$1 AND access_hash=$2 AND access_expires_at>clock_timestamp() RETURNING id",
    )
    .bind(user_id)
    .bind(hash)
    .fetch_optional(&mut *tx)
    .await?;
    if deleted.is_none() {
        return Err(ApiError(StatusCode::UNAUTHORIZED, "invalid_access_token"));
    }
    tx.commit().await?;
    Ok((
        [(header::CACHE_CONTROL, "private, no-store")],
        StatusCode::NO_CONTENT,
    )
        .into_response())
}

/// Login already holds the account lock. Limit incidental maintenance while
/// leaving every refreshable session and other account's credentials intact.
pub(super) async fn cleanup_expired(
    tx: &mut Transaction<'_, Postgres>,
    user_id: i64,
) -> Result<(), ApiError> {
    sqlx::query(
        "WITH expired AS (
            SELECT id FROM lazer_sessions
            WHERE user_id=$1 AND refresh_expires_at<=clock_timestamp()
            ORDER BY refresh_expires_at,id LIMIT 100 FOR UPDATE SKIP LOCKED
        ) DELETE FROM lazer_sessions WHERE id IN (SELECT id FROM expired)",
    )
    .bind(user_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
