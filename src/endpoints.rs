//! REST endpoint handlers for the OMG Bot Alpha Engine.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::{get, patch, post},
    Router,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::alpha::{self, AlphaSignal};
use crate::ev_engine::{self, EvRequest, EvResult};
use crate::trades::{self, RawTrade};
use crate::user_bots;

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

#[derive(Debug, Deserialize)]
pub struct BacktestParams {
    pub event_id: String,
    pub rsi_buy: f64,
    pub rsi_sell: f64,
    pub max_position: f64,
    pub timeframe: String,
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
pub struct BacktestTrade {
    pub timestamp: String,
    pub action: String, // "buy" or "sell"
    pub price: f64,
    pub rsi: f64,
    pub pnl: f64,
}

#[derive(Serialize)]
pub struct BacktestResult {
    pub trades: Vec<BacktestTrade>,
    pub total_pnl: f64,
    pub win_rate: f64,
    pub total_trades: usize,
    pub max_drawdown: f64,
    pub sharpe_ratio: f64,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

// ── Route Registration ─────────────────────────────────────────────

pub fn alpha_routes() -> Router<PgPool> {
    Router::new()
        // alpha + trades
        .route("/trades/:event_id", get(get_trades_handler))
        .route("/alpha_signals/:event_id", get(get_alpha_signals_handler))
        .route("/calculate_ev", post(calculate_ev_handler))
        .route("/trade_signals", get(get_trade_signals_handler))
        .route("/backtest", post(backtest_handler))
        // user bots — NEW
        .route(
            "/user_bots",
            get(user_bots::get_user_bot).post(user_bots::create_user_bot),
        )
        .route("/user_bots/:id", patch(user_bots::update_user_bot))
}

// ── POST /backtest ─────────────────────────────────────────────────

async fn backtest_handler(
    State(pool): State<PgPool>,
    Json(params): Json<BacktestParams>,
) -> Result<Json<BacktestResult>, (StatusCode, Json<ErrorResponse>)> {
    let trades = trades::get_trades(&pool, &params.event_id, None, 5000)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("Failed to fetch trades: {}", e),
                }),
            )
        })?;

    if trades.len() < 14 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!(
                    "Not enough trades for RSI calculation (need 14, got {})",
                    trades.len()
                ),
            }),
        ));
    }

    let prices: Vec<f64> = trades.iter().map(|t| t.price).collect();
    let rsi_values = compute_rsi_series(&prices, 14);

    let mut backtest_trades: Vec<BacktestTrade> = Vec::new();
    let mut position: Option<f64> = None;
    let mut total_pnl = 0.0_f64;
    let mut wins = 0_usize;
    let mut peak_equity = 0.0_f64;
    let mut max_drawdown = 0.0_f64;
    let mut pnl_series: Vec<f64> = Vec::new();

    for (i, &rsi) in rsi_values.iter().enumerate() {
        let trade_idx = i + 14;
        if trade_idx >= trades.len() {
            break;
        }

        let price = trades[trade_idx].price;
        let timestamp = trades[trade_idx].trade_timestamp;

        match position {
            None => {
                if rsi <= params.rsi_buy {
                    position = Some(price);
                    backtest_trades.push(BacktestTrade {
                        timestamp: timestamp.to_rfc3339(),
                        action: "buy".to_string(),
                        price,
                        rsi,
                        pnl: 0.0,
                    });
                }
            }
            Some(entry_price) => {
                if rsi >= params.rsi_sell {
                    let trade_pnl = (price - entry_price) * params.max_position;
                    total_pnl += trade_pnl;
                    if trade_pnl > 0.0 {
                        wins += 1;
                    }

                    pnl_series.push(total_pnl);
                    if total_pnl > peak_equity {
                        peak_equity = total_pnl;
                    }
                    let drawdown = peak_equity - total_pnl;
                    if drawdown > max_drawdown {
                        max_drawdown = drawdown;
                    }

                    backtest_trades.push(BacktestTrade {
                        timestamp: timestamp.to_rfc3339(),
                        action: "sell".to_string(),
                        price,
                        rsi,
                        pnl: trade_pnl,
                    });
                    position = None;
                }
            }
        }
    }

    let round_trips = backtest_trades.iter().filter(|t| t.action == "sell").count();
    let win_rate = if round_trips > 0 {
        wins as f64 / round_trips as f64
    } else {
        0.0
    };
    let sharpe = compute_sharpe(&pnl_series);

    Ok(Json(BacktestResult {
        trades: backtest_trades,
        total_pnl,
        win_rate,
        total_trades: round_trips,
        max_drawdown,
        sharpe_ratio: sharpe,
    }))
}

// ── RSI helpers ────────────────────────────────────────────────────

fn compute_rsi_series(prices: &[f64], period: usize) -> Vec<f64> {
    let mut rsi_values = Vec::new();
    if prices.len() < period + 1 {
        return rsi_values;
    }

    let mut avg_gain = 0.0_f64;
    let mut avg_loss = 0.0_f64;

    for i in 1..=period {
        let change = prices[i] - prices[i - 1];
        if change > 0.0 {
            avg_gain += change;
        } else {
            avg_loss += change.abs();
        }
    }
    avg_gain /= period as f64;
    avg_loss /= period as f64;

    let rs = if avg_loss == 0.0 { 100.0 } else { avg_gain / avg_loss };
    rsi_values.push(100.0 - (100.0 / (1.0 + rs)));

    for i in (period + 1)..prices.len() {
        let change = prices[i] - prices[i - 1];
        let (gain, loss) = if change > 0.0 {
            (change, 0.0)
        } else {
            (0.0, change.abs())
        };

        avg_gain = (avg_gain * (period as f64 - 1.0) + gain) / period as f64;
        avg_loss = (avg_loss * (period as f64 - 1.0) + loss) / period as f64;

        let rs = if avg_loss == 0.0 { 100.0 } else { avg_gain / avg_loss };
        rsi_values.push(100.0 - (100.0 / (1.0 + rs)));
    }

    rsi_values
}

fn compute_sharpe(pnl_series: &[f64]) -> f64 {
    if pnl_series.len() < 2 {
        return 0.0;
    }

    let returns: Vec<f64> = pnl_series.windows(2).map(|w| w[1] - w[0]).collect();
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let variance =
        returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;
    let std_dev = variance.sqrt();

    if std_dev == 0.0 { 0.0 } else { mean / std_dev }
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
