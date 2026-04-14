//! REST endpoint handlers for the OMG Bot Alpha Engine.
//!
//! Wire these into your existing Actix-web / Axum router in main.rs.
//! These examples use Axum — adapt if using Actix.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;

use crate::alpha::{self, AlphaSignal};
use crate::ev_engine::{self, EvRequest, EvResult};
use crate::trades::{self, RawTrade};

// ── App State ──────────────────────────────────────────────────────

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
}

// ── Query Params ───────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct TradesQuery {
    pub since: Option<String>,  // ISO 8601 timestamp
    pub limit: Option<i64>,     // Max trades to return (default 1000)
}

#[derive(Debug, Deserialize)]
pub struct AlphaQuery {
    pub signal_type: Option<String>, // Filter by 'wick', 'volume_spike', 'liquidity_gap'
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct UserSignalsQuery {
    pub user_id: String,
    pub status: Option<String>, // 'active', 'executed', 'expired'
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

/// Add these routes to your existing Axum router.
///
/// Example in main.rs:
/// ```
/// let app = Router::new()
///     // ... existing routes ...
///     .merge(endpoints::alpha_routes(state.clone()));
/// ```
pub fn alpha_routes(state: AppState) -> Router {
    Router::new()
        .route("/trades/:event_id", get(get_trades_handler))
        .route("/alpha_signals/:event_id", get(get_alpha_signals_handler))
        .route("/calculate_ev", post(calculate_ev_handler))
        .route("/trade_signals", get(get_trade_signals_handler))
        .with_state(Arc::new(state))
}

// ── GET /trades/:event_id ──────────────────────────────────────────

/// Returns raw unbucketed trades for an event.
/// These are the individual fills that platform UIs hide.
///
/// Query params:
///   - since: ISO 8601 timestamp (default: last 24h)
///   - limit: max trades (default: 1000, max: 5000)
async fn get_trades_handler(
    State(state): State<Arc<AppState>>,
    Path(event_id): Path<String>,
    Query(params): Query<TradesQuery>,
) -> Result<Json<TradesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let since = params
        .since
        .as_deref()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc));

    let limit = params.limit.unwrap_or(1000).min(5000);

    match trades::get_trades(&state.pool, &event_id, since, limit).await {
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

/// Returns alpha signals (wicks, volume spikes, liquidity gaps) for an event.
///
/// Query params:
///   - signal_type: filter by type ('wick', 'volume_spike', 'liquidity_gap')
///   - limit: max signals (default: 100)
async fn get_alpha_signals_handler(
    State(state): State<Arc<AppState>>,
    Path(event_id): Path<String>,
    Query(params): Query<AlphaQuery>,
) -> Result<Json<AlphaResponse>, (StatusCode, Json<ErrorResponse>)> {
    let limit = params.limit.unwrap_or(100).min(500);
    let signal_type = params.signal_type.as_deref();

    match alpha::get_signals(&state.pool, &event_id, signal_type, limit).await {
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

/// Calculate Expected Value and Kelly position sizing for a contract.
///
/// Request body:
/// ```json
/// {
///   "event_id": "abc123",
///   "ai_probability": 0.65,
///   "market_price": 0.52,
///   "bankroll": 1000.0,
///   "user_id": "user-uuid",
///   "platform": "polymarket"
/// }
/// ```
///
/// Returns EV analysis with recommended side, edge %, and position size.
async fn calculate_ev_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<EvRequest>,
) -> Result<Json<EvResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Validate inputs
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

    // Compute edge
    let result = ev_engine::compute_edge(&req);

    // Persist signal if user provided
    let signal_id = if req.user_id.is_some() {
        match ev_engine::persist_trade_signal(&state.pool, &req, &result).await {
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

/// Get trade signals for a user.
///
/// Query params:
///   - user_id: required
///   - status: filter by status ('active', 'executed', 'expired')
///   - limit: max signals (default: 50)
async fn get_trade_signals_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<UserSignalsQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let limit = params.limit.unwrap_or(50).min(200);

    match ev_engine::get_user_signals(
        &state.pool,
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

// ── Main.rs Integration Guide ──────────────────────────────────────

/// Add to your main.rs:
///
/// ```rust
/// mod trades;
/// mod alpha;
/// mod ev_engine;
/// mod endpoints;
///
/// #[tokio::main]
/// async fn main() {
///     // ... existing setup ...
///
///     let state = endpoints::AppState { pool: pool.clone() };
///
///     // Spawn background loops
///     let pool_clone = pool.clone();
///     let client = reqwest::Client::new();
///     tokio::spawn(trades::run_trade_ingestion_loop(pool_clone, client));
///     tokio::spawn(alpha::run_alpha_detection_loop(pool.clone()));
///
///     // Add alpha routes to existing router
///     let app = Router::new()
///         // ... existing routes ...
///         .merge(endpoints::alpha_routes(state));
///
///     // ... start server ...
/// }
/// ```
