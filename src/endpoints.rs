//! REST endpoint handlers for the OMG Bot Alpha Engine.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::alpha::{self, AlphaSignal};
use crate::ev_engine::{self, EvRequest, EvResult};
use crate::trades::{self, RawTrade};

// ── Query Params ───────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct TradesQuery {
    pub since: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct AlphaQuery {
    pub signal_type: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct UserSignalsQuery {
    pub user_id: String,
    pub status: Option<String>,
    pub limit: Option<i64>,
}

// ── Response Wrappers ──────────────────────────────────────────────

#[derive(Serialize)]
pub struct TradesResponse {
    pub event_id: String,
    pub trades: Vec<RawTrade>,
    pub count: usize,
}

#[derive(Serialize)]
pub struct AlphaResponse {
    pub event_id: String,
    pub signals: Vec<AlphaSignal>,
    pub count: usize,
}

#[derive(Serialize)]
pub struct EvResponse {
    pub success: bool,
    pub result: EvResult,
    pub signal_id: Option<String>,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

// ── Route Registration ─────────────────────────────────────────────

/// Returns a Router<PgPool> — state is provided by the parent router's
/// `.with_state(pool)` call, so no `.with_state()` here.
pub fn alpha_routes() -> Router<PgPool> {
    Router::new()
        .route("/trades/:event_id", get(get_trades_handler))
        .route("/alpha_signals/:event_id", get(get_alpha_signals_handler))
        .route("/calculate_ev", post(calculate_ev_handler))
        .route("/trade_signals", get(get_trade_signals_handler))
}

// ── GET /trades/:event_id ──────────────────────────────────────────

async fn get_trades_handler(
    State(pool): State<PgPool>,
    Path(event_id): Path<String>,
    Query(params): Query<TradesQuery>,
) -> Result<Json<TradesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let since = params
        .since
        .as_deref()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc));

    let limit = params.limit.unwrap_or(1000).min(5000);

    match trades::get_trades(&pool, &event_id, since, limit).await {
        Ok(trades) => {
            let count = trades.len();
            Ok(Json(TradesResponse {
                event_id,
                trades,
                count,
            }))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Failed to fetch trades: {}", e),
            }),
        )),
    }
}

// ── GET /alpha_signals/:event_id ───────────────────────────────────

async fn get_alpha_signals_handler(
    State(pool): State<PgPool>,
    Path(event_id): Path<String>,
    Query(params): Query<AlphaQuery>,
) -> Result<Json<AlphaResponse>, (StatusCode, Json<ErrorResponse>)> {
    let limit = params.limit.unwrap_or(100).min(500);
    let signal_type = params.signal_type.as_deref();

    match alpha::get_signals(&pool, &event_id, signal_type, limit).await {
        Ok(signals) => {
            let count = signals.len();
            Ok(Json(AlphaResponse {
                event_id,
                signals,
                count,
            }))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Failed to fetch signals: {}", e),
            }),
        )),
    }
}

// ── POST /calculate_ev ─────────────────────────────────────────────

async fn calculate_ev_handler(
    State(pool): State<PgPool>,
    Json(req): Json<EvRequest>,
) -> Result<Json<EvResponse>, (StatusCode, Json<ErrorResponse>)> {
    if req.ai_probability <= 0.0 || req.ai_probability >= 1.0 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "ai_probability must be between 0 and 1 (exclusive)".to_string(),
            }),
        ));
    }
    if req.market_price <= 0.0 || req.market_price >= 1.0 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "market_price must be between 0 and 1 (exclusive)".to_string(),
            }),
        ));
    }

    let result = ev_engine::compute_edge(&req);

    let signal_id = if req.user_id.is_some() {
        match ev_engine::persist_trade_signal(&pool, &req, &result).await {
            Ok(signal) => Some(signal.id.to_string()),
            Err(e) => {
                tracing::warn!("Failed to persist trade signal: {}", e);
                None
            }
        }
    } else {
        None
    };

    Ok(Json(EvResponse {
        success: true,
        result,
        signal_id,
    }))
}

// ── GET /trade_signals ─────────────────────────────────────────────

async fn get_trade_signals_handler(
    State(pool): State<PgPool>,
    Query(params): Query<UserSignalsQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let limit = params.limit.unwrap_or(50).min(200);

    match ev_engine::get_user_signals(
        &pool,
        &params.user_id,
        params.status.as_deref(),
        limit,
    )
    .await
    {
        Ok(signals) => Ok(Json(serde_json::json!({
            "user_id": params.user_id,
            "signals": signals,
            "count": signals.len(),
        }))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Failed to fetch trade signals: {}", e),
            }),
        )),
    }
}
