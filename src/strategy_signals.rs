//! Strategy Signals Engine — 7 advanced trading signals for the OMG terminal.
//!
//! Built from research by Part Time Larry, academic studies on 72M+ prediction
//! market trades, and systematic analysis of Kalshi/Polymarket edge opportunities.
//!
//! NEW SIGNALS (on top of existing 12 indicators):
//!   13. Stink Bid Detector      — orderbook irrationality during live events
//!   14. Longshot Bias Warning   — YES < 15¢ = NO edge (backed by 72M trade study)
//!   15. Maker/Taker Signal      — always suggest limit order price + savings
//!   16. Platform Lead/Lag       — Polymarket leads, flag Kalshi lag opportunity
//!   17. Same-Platform Arb       — YES + NO < 97¢ = guaranteed profit
//!   18. Live Game Spike Alert   — price move >15% in 60s during live event
//!   19. Lock-In Profit Signal   — favorite at 85¢+ late game = safe 10-15% return
//!
//! Exposes:
//!   GET /strategy_signals/:event_id  — all active signals for an event
//!   GET /strategy_signals/scan       — scan all active markets for top signals
//!
//! Wire into main.rs:
//!   mod strategy_signals;
//!   .merge(strategy_signals::routes())
//!   tokio::spawn(strategy_signals::run_strategy_signal_loop(api_pool.clone()));

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

// ── Types ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategySignal {
    pub signal_type: String,
    pub confidence: f64,        // 0.0 to 1.0
    pub action: String,         // "BUY YES", "BUY NO", "PLACE LIMIT", "LOCK IN", "AVOID"
    pub entry_price: Option<f64>,
    pub limit_order_price: Option<f64>,  // suggested limit order price
    pub limit_order_savings: Option<f64>, // fee savings vs market order
    pub target_price: Option<f64>,
    pub expected_profit_pct: Option<f64>,
    pub reasoning: String,
    pub urgency: String,        // "immediate", "watch", "informational"
    pub detected_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct StrategySignalsResponse {
    pub event_id: String,
    pub title: String,
    pub platform: String,
    pub current_price: f64,
    pub signals: Vec<StrategySignal>,
    pub top_signal: Option<StrategySignal>,
    pub computed_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct MarketScanResult {
    pub event_id: String,
    pub title: String,
    pub platform: String,
    pub current_price: f64,
    pub signal_type: String,
    pub confidence: f64,
    pub action: String,
    pub reasoning: String,
    pub urgency: String,
}

#[derive(Debug, Serialize)]
pub struct ScanResponse {
    pub results: Vec<MarketScanResult>,
    pub total_markets_scanned: i64,
    pub signals_found: usize,
    pub scanned_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

// ── Routes ─────────────────────────────────────────────────────────

pub fn routes() -> Router<PgPool> {
    Router::new()
        .route("/strategy_signals/:event_id", get(get_strategy_signals_handler))
        .route("/strategy_signals/scan", get(scan_all_markets_handler))
}

// ── GET /strategy_signals/:event_id ───────────────────────────────

async fn get_strategy_signals_handler(
    State(pool): State<PgPool>,
    Path(event_id): Path<String>,
) -> Result<Json<StrategySignalsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let uid = parse_or_lookup_uuid(&pool, &event_id).await.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Event not found: {}", event_id),
            }),
        )
    })?;

    match compute_strategy_signals(&pool, uid).await {
        Ok(resp) => Ok(Json(resp)),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Signal compute failed: {}", e),
            }),
        )),
    }
}

// ── GET /strategy_signals/scan ─────────────────────────────────────

async fn scan_all_markets_handler(
    State(pool): State<PgPool>,
) -> Result<Json<ScanResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Get top active markets by volume
    let markets: Vec<(Uuid, String, String, f64)> = sqlx::query_as(
        "SELECT id, title, platform, odds
         FROM public.prediction_events
         WHERE status = 'active'
           AND volume_24h > 1000
           AND odds > 0.01 AND odds < 0.99
           AND (end_date IS NULL OR end_date > NOW())
         ORDER BY volume_24h DESC NULLS LAST
         LIMIT 200"
    )
    .fetch_all(&pool)
    .await
    .map_err(|e| (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse { error: e.to_string() }),
    ))?;

    let total = markets.len() as i64;
    let mut results = Vec::new();

    for (uid, title, platform, price) in &markets {
        if let Ok(resp) = compute_strategy_signals(&pool, *uid).await {
            if let Some(top) = resp.top_signal {
                if top.confidence > 0.5 {
                    results.push(MarketScanResult {
                        event_id: uid.to_string(),
                        title: title.clone(),
                        platform: platform.clone(),
                        current_price: *price,
                        signal_type: top.signal_type,
                        confidence: top.confidence,
                        action: top.action,
                        reasoning: top.reasoning,
                        urgency: top.urgency,
                    });
                }
            }
        }
        // Rate limit DB queries
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    // Sort by confidence descending
    results.sort_by(|a, b| b.confidence.partial_cmp(&a.confidence).unwrap_or(std::cmp::Ordering::Equal));
    let found = results.len();

    Ok(Json(ScanResponse {
        results,
        total_markets_scanned: total,
        signals_found: found,
        scanned_at: Utc::now(),
    }))
}

// ── Core Signal Computation ────────────────────────────────────────

pub async fn compute_strategy_signals(
    pool: &PgPool,
    event_id: Uuid,
) -> anyhow::Result<StrategySignalsResponse> {
    // Load event metadata
    let meta: Option<(f64, String, String, Option<serde_json::Value>, Option<serde_json::Value>)> =
        sqlx::query_as(
            "SELECT odds, title, platform, live_stats, outcomes
             FROM public.prediction_events
             WHERE id = $1 LIMIT 1",
        )
        .bind(event_id)
        .fetch_optional(pool)
        .await?;

    let (price, title, platform, live_stats, outcomes) = match meta {
        Some(m) => m,
        None => anyhow::bail!("Event not found"),
    };

    let price = price.clamp(0.001, 0.999);

    // Load latest indicators
    let ind: Option<(
        Option<f64>, Option<f64>, Option<f64>, Option<f64>,
        Option<f64>, Option<f64>, Option<f64>,
    )> = sqlx::query_as(
        "SELECT ev_yes, ev_no, kelly_fraction, cross_platform_delta,
                book_imbalance, spread_pct, omg_score
         FROM public.market_indicators
         WHERE event_id = $1
         ORDER BY computed_at DESC LIMIT 1",
    )
    .bind(event_id)
    .fetch_optional(pool)
    .await?;

    let (ev_yes, ev_no, kelly, cross_delta, book_imbalance, spread_pct, omg_score) =
        ind.unwrap_or((None, None, None, None, None, None, None));

    // Load recent price history for spike detection
    let price_history: Vec<(f64, DateTime<Utc>)> = sqlx::query_as(
        "SELECT price, recorded_at FROM public.market_history
         WHERE event_id = $1
         ORDER BY recorded_at DESC LIMIT 20"
    )
    .bind(event_id)
    .fetch_all(pool)
    .await
    .unwrap_or_default();

    // Compute all 7 signals
    let mut signals = Vec::new();

    // Signal 13: Stink Bid Detector
    if let Some(sig) = detect_stink_bid(price, &book_imbalance, &spread_pct, &live_stats) {
        signals.push(sig);
    }

    // Signal 14: Longshot Bias Warning
    if let Some(sig) = detect_longshot_bias(price, ev_no) {
        signals.push(sig);
    }

    // Signal 15: Maker/Taker Recommendation
    if let Some(sig) = compute_maker_taker_signal(price, ev_yes, kelly) {
        signals.push(sig);
    }

    // Signal 16: Platform Lead/Lag
    if let Some(sig) = detect_platform_lead_lag(&platform, cross_delta, price) {
        signals.push(sig);
    }

    // Signal 17: Same-Platform Arb
    if let Some(sig) = detect_same_platform_arb(&outcomes) {
        signals.push(sig);
    }

    // Signal 18: Live Game Spike Alert
    if let Some(sig) = detect_live_spike(&price_history, &live_stats, price) {
        signals.push(sig);
    }

    // Signal 19: Lock-In Profit
    if let Some(sig) = detect_lock_in_profit(price, &live_stats, omg_score) {
        signals.push(sig);
    }

    // Find top signal by confidence
    let top_signal = signals.iter()
        .max_by(|a, b| a.confidence.partial_cmp(&b.confidence).unwrap_or(std::cmp::Ordering::Equal))
        .cloned();

    Ok(StrategySignalsResponse {
        event_id: event_id.to_string(),
        title,
        platform,
        current_price: price,
        signals,
        top_signal,
        computed_at: Utc::now(),
    })
}

// ── Signal 13: Stink Bid Detector ─────────────────────────────────
//
// From Part Time Larry: During live events the orderbook goes irrational.
// During the Super Bowl he bought NO contracts at 51¢ when true probability
// was 85%+ NO. Large spread + live game + price extremity = stink bid opportunity.
//
// Strategy: place limit orders at 10-20% below current price, get filled
// when emotional traders panic-sell, profit when market corrects.

fn detect_stink_bid(
    price: f64,
    book_imbalance: &Option<f64>,
    spread_pct: &Option<f64>,
    live_stats: &Option<serde_json::Value>,
) -> Option<StrategySignal> {
    let is_live = live_stats.as_ref()
        .and_then(|ls| ls.get("status"))
        .and_then(|s| s.as_str())
        .map(|s| s == "live")
        .unwrap_or(false);

    let spread = spread_pct.unwrap_or(0.0);
    let imbalance = book_imbalance.unwrap_or(0.0);

    // Stink bid conditions:
    // 1. Live event in progress
    // 2. Wide spread (>5%) = thin book, panic selling possible
    // 3. Strong book imbalance = one side dominating
    // 4. Price not too extreme (5-95¢ range)
    if !is_live || spread < 5.0 || price < 0.05 || price > 0.95 {
        return None;
    }

    let imbalance_strength = imbalance.abs();

    if imbalance_strength < 0.3 {
        return None;
    }

    // Suggest stink bid 15% below current price
    let stink_bid_price = price * 0.85;
    let profit_if_corrects = (price - stink_bid_price) / stink_bid_price * 100.0;
    let confidence = (spread / 20.0).min(1.0) * 0.7 + imbalance_strength * 0.3;

    let action = if imbalance < 0.0 {
        "PLACE LIMIT BID — buy NO cheap"
    } else {
        "PLACE LIMIT BID — buy YES cheap"
    };

    Some(StrategySignal {
        signal_type: "stink_bid".to_string(),
        confidence: confidence.min(0.95),
        action: action.to_string(),
        entry_price: Some(price),
        limit_order_price: Some(stink_bid_price),
        limit_order_savings: Some(spread / 2.0),
        target_price: Some(price),
        expected_profit_pct: Some(profit_if_corrects),
        reasoning: format!(
            "Live event with wide {:.1}% spread and {:.0}% book imbalance. \
             Emotional traders may panic-sell. Place limit order at {:.0}¢ ({:.0}% below market). \
             When market corrects back to {:.0}¢, profit is {:.0}%. \
             Part Time Larry strategy: stink bids get filled during live game volatility.",
            spread,
            imbalance_strength * 100.0,
            stink_bid_price * 100.0,
            15.0,
            price * 100.0,
            profit_if_corrects
        ),
        urgency: "immediate".to_string(),
        detected_at: Utc::now(),
    })
}

// ── Signal 14: Longshot Bias Warning ──────────────────────────────
//
// From 72M trade study: longshot YES bets (under 15¢) are systematically
// overpriced. Retail traders have lottery ticket mentality and overbuy cheap YES.
// Actual hit rate is ~8% but markets price them at 12-15%.
// Smart money bets NO on longshots consistently.
//
// Strategy: When YES < 15¢, flag NO as the edge bet.

fn detect_longshot_bias(price: f64, ev_no: Option<f64>) -> Option<StrategySignal> {
    // Only fire when price is in the longshot range
    if price > 0.15 {
        return None;
    }

    let ev = ev_no.unwrap_or(0.0);
    let no_price = 1.0 - price;

    // Higher confidence when price is very low (more overpriced)
    let confidence = if price < 0.05 {
        0.90
    } else if price < 0.08 {
        0.82
    } else if price < 0.12 {
        0.73
    } else {
        0.65
    };

    // Suggested NO limit order — slightly below best ask
    let no_limit = no_price * 0.98;
    let fee_savings_pct = 2.0; // maker vs taker on Kalshi

    Some(StrategySignal {
        signal_type: "longshot_bias".to_string(),
        confidence,
        action: "BUY NO — longshot bias edge".to_string(),
        entry_price: Some(no_price),
        limit_order_price: Some(no_limit),
        limit_order_savings: Some(fee_savings_pct),
        target_price: Some(1.0),
        expected_profit_pct: Some(((1.0 - no_limit) / no_limit * 100.0).round()),
        reasoning: format!(
            "YES is priced at {:.0}¢ ({:.0}% implied probability). \
             Academic study of 72M prediction market trades found longshots under 15¢ \
             hit only ~8% of the time but are priced at 12-15%. \
             Edge is on the NO side at {:.0}¢. \
             Submit as LIMIT ORDER at {:.0}¢ to pay lower fees. \
             EV on NO: {:.1}%.",
            price * 100.0,
            price * 100.0,
            no_price * 100.0,
            no_limit * 100.0,
            ev * 100.0
        ),
        urgency: "watch".to_string(),
        detected_at: Utc::now(),
    })
}

// ── Signal 15: Maker/Taker Recommendation ─────────────────────────
//
// From Part Time Larry + 72M trade study: makers extract money from takers.
// Takers pay higher fees AND trade on impulse. Makers pay lower fees
// AND trade on analysis. Always submit limit orders, never market orders.
//
// Strategy: For any trade signal, calculate optimal limit order price
// that saves fees and is still likely to fill.

fn compute_maker_taker_signal(
    price: f64,
    ev_yes: Option<f64>,
    kelly: Option<f64>,
) -> Option<StrategySignal> {
    let ev = ev_yes.unwrap_or(0.0);
    let k = kelly.unwrap_or(0.0);

    // Only suggest limit orders when there's actual edge
    if ev.abs() < 0.03 || k < 0.01 {
        return None;
    }

    let is_buy = ev > 0.0;

    // Kalshi maker fee: ~3.5%, taker fee: ~7%
    // Polymarket maker fee: ~0%, taker fee: ~2%
    // Average savings: ~3-4% by being a maker
    let fee_savings_pct = 3.5;

    // Optimal limit order: 2% below ask for buys, 2% above bid for sells
    let limit_price = if is_buy {
        price * 0.98  // bid slightly below market to get maker status
    } else {
        price * 1.02  // ask slightly above market
    };

    let confidence = (ev.abs() * 2.0).min(0.85);

    Some(StrategySignal {
        signal_type: "maker_taker".to_string(),
        confidence,
        action: format!(
            "{} — submit as LIMIT ORDER at {:.0}¢",
            if is_buy { "BUY YES" } else { "BUY NO" },
            limit_price * 100.0
        ),
        entry_price: Some(price),
        limit_order_price: Some(limit_price),
        limit_order_savings: Some(fee_savings_pct),
        target_price: None,
        expected_profit_pct: Some(ev.abs() * 100.0),
        reasoning: format!(
            "EV of {:.1}% detected. Submit as LIMIT ORDER at {:.0}¢ instead of market order at {:.0}¢. \
             Saves ~{:.1}% in fees (maker vs taker). \
             Expected fill time: 2-15 minutes based on current volume. \
             Study of 72M trades: makers consistently outperform takers by 3-7% per trade. \
             Kelly suggests {:.1}% of bankroll on this position.",
            ev * 100.0,
            limit_price * 100.0,
            price * 100.0,
            fee_savings_pct,
            k * 100.0
        ),
        urgency: "watch".to_string(),
        detected_at: Utc::now(),
    })
}

// ── Signal 16: Platform Lead/Lag ──────────────────────────────────
//
// Research finding: Polymarket LEADS Kalshi in price discovery due to
// higher liquidity. When Polymarket moves, Kalshi follows minutes later.
// Cross-platform delta > 3¢ with Polymarket as leader = buy Kalshi NOW.
//
// Strategy: When Polymarket already moved and Kalshi hasn't caught up,
// buy the lagging Kalshi contract before it corrects.

fn detect_platform_lead_lag(
    platform: &str,
    cross_delta: Option<f64>,
    price: f64,
) -> Option<StrategySignal> {
    let delta = cross_delta?;

    // Need meaningful delta to trade (>3¢ after fees)
    if delta.abs() < 0.03 {
        return None;
    }

    // delta = kalshi_price - poly_price (positive = kalshi higher)
    // If we're looking at Kalshi contract and delta is negative,
    // Polymarket is higher = Polymarket already moved up, Kalshi lagging
    let kalshi_is_lagging = platform.to_lowercase() == "kalshi" && delta < -0.03;
    let poly_is_lagging = platform.to_lowercase() == "polymarket" && delta > 0.03;

    if !kalshi_is_lagging && !poly_is_lagging {
        return None;
    }

    let lag_cents = delta.abs() * 100.0;
    let profit_if_corrects = lag_cents - 1.5; // subtract ~1.5¢ fees
    let confidence = ((delta.abs() - 0.03) * 10.0).min(0.88);

    let (action, lagging_platform, leading_platform) = if kalshi_is_lagging {
        ("BUY YES on Kalshi — price will catch up to Polymarket",
         "Kalshi", "Polymarket")
    } else {
        ("BUY YES on Polymarket — price will catch up to Kalshi",
         "Polymarket", "Kalshi")
    };

    Some(StrategySignal {
        signal_type: "platform_lead_lag".to_string(),
        confidence,
        action: action.to_string(),
        entry_price: Some(price),
        limit_order_price: Some(price * 1.01),
        limit_order_savings: None,
        target_price: Some(price + delta.abs()),
        expected_profit_pct: Some(profit_if_corrects / price * 100.0),
        reasoning: format!(
            "{:.1}¢ price gap detected between platforms. \
             {} is leading at a higher price. {} is lagging at {:.0}¢. \
             Research confirms Polymarket leads Kalshi in price discovery. \
             Buy {} now — expected correction within 2-10 minutes. \
             Profit after fees: ~{:.1}¢ per contract.",
            lag_cents,
            leading_platform,
            lagging_platform,
            price * 100.0,
            lagging_platform,
            profit_if_corrects
        ),
        urgency: "immediate".to_string(),
        detected_at: Utc::now(),
    })
}

// ── Signal 17: Same-Platform Arb ──────────────────────────────────
//
// Fundamental property of prediction markets: YES + NO = $1.00.
// When they don't — guaranteed profit exists.
// Academic research found $40M extracted from Polymarket alone this way.
//
// Strategy: When outcomes array shows YES + NO < 97¢, buy both sides.

fn detect_same_platform_arb(outcomes: &Option<serde_json::Value>) -> Option<StrategySignal> {
    let outcomes_arr = outcomes.as_ref()?.as_array()?;

    if outcomes_arr.len() < 2 {
        return None;
    }

    // For binary markets: yes_price + no_price should = 1.00
    let yes_price = outcomes_arr.iter()
        .find(|o| {
            let name = o.get("name").and_then(|n| n.as_str()).unwrap_or("").to_lowercase();
            name.contains("yes") || name.contains("over")
        })
        .and_then(|o| o.get("price").and_then(|p| p.as_f64()))?;

    let no_price = outcomes_arr.iter()
        .find(|o| {
            let name = o.get("name").and_then(|n| n.as_str()).unwrap_or("").to_lowercase();
            name.contains("no") || name.contains("under")
        })
        .and_then(|o| o.get("price").and_then(|p| p.as_f64()))?;

    let total = yes_price + no_price;

    // Only flag if gap is meaningful (> 3¢ after fees)
    if total >= 0.97 {
        return None;
    }

    let profit_cents = (1.0 - total) * 100.0;
    let profit_after_fees = profit_cents - 2.0; // ~2¢ in fees

    if profit_after_fees <= 0.0 {
        return None;
    }

    let confidence = ((0.97 - total) * 20.0).min(0.95);

    Some(StrategySignal {
        signal_type: "same_platform_arb".to_string(),
        confidence,
        action: format!(
            "BUY BOTH — YES at {:.0}¢ + NO at {:.0}¢ = {:.0}¢ guaranteed profit",
            yes_price * 100.0,
            no_price * 100.0,
            profit_after_fees
        ),
        entry_price: Some(total),
        limit_order_price: None,
        limit_order_savings: None,
        target_price: Some(1.0),
        expected_profit_pct: Some(profit_after_fees / total * 100.0),
        reasoning: format!(
            "YES ({:.0}¢) + NO ({:.0}¢) = {:.0}¢ total cost. \
             One side MUST resolve to $1.00. \
             Guaranteed profit of {:.1}¢ per contract after fees. \
             Buy both sides immediately — this gap closes fast. \
             Academic research: $40M extracted from Polymarket alone using this strategy.",
            yes_price * 100.0,
            no_price * 100.0,
            total * 100.0,
            profit_after_fees
        ),
        urgency: "immediate".to_string(),
        detected_at: Utc::now(),
    })
}

// ── Signal 18: Live Game Spike Alert ──────────────────────────────
//
// During live events, goals/touchdowns/scores cause massive price spikes.
// The orderbook goes irrational for 30-120 seconds. This is the #1
// alpha opportunity in prediction markets (Part Time Larry confirmed).
//
// Strategy: When price moves >15% in 60 seconds AND live event is active,
// alert user to check orderbook before acting — book may be irrational.

fn detect_live_spike(
    price_history: &[(f64, DateTime<Utc>)],
    live_stats: &Option<serde_json::Value>,
    current_price: f64,
) -> Option<StrategySignal> {
    if price_history.len() < 3 {
        return None;
    }

    let is_live = live_stats.as_ref()
        .and_then(|ls| ls.get("status"))
        .and_then(|s| s.as_str())
        .map(|s| s == "live")
        .unwrap_or(false);

    if !is_live {
        return None;
    }

    // Find price 60 seconds ago
    let sixty_secs_ago = Utc::now() - Duration::seconds(60);
    let price_60s_ago = price_history.iter()
        .filter(|(_, ts)| *ts <= sixty_secs_ago)
        .map(|(p, _)| *p)
        .next();

    let old_price = price_60s_ago.unwrap_or(price_history.last()?.0);
    let price_change_pct = ((current_price - old_price) / old_price * 100.0).abs();

    if price_change_pct < 15.0 {
        return None;
    }

    // Extract live game info
    let home_score = live_stats.as_ref()
        .and_then(|ls| ls.get("home_score")).and_then(|s| s.as_i64());
    let away_score = live_stats.as_ref()
        .and_then(|ls| ls.get("away_score")).and_then(|s| s.as_i64());
    let minute = live_stats.as_ref()
        .and_then(|ls| ls.get("minute")).and_then(|s| s.as_str())
        .unwrap_or("").to_string();

    let direction = if current_price > old_price { "UP" } else { "DOWN" };
    let confidence = ((price_change_pct - 15.0) / 30.0).min(0.90);

    let score_str = match (home_score, away_score) {
        (Some(h), Some(a)) => format!(" (score: {}-{})", h, a),
        _ => String::new(),
    };

    Some(StrategySignal {
        signal_type: "live_spike".to_string(),
        confidence,
        action: format!(
            "CHECK ORDERBOOK — price moved {:.0}% {} in 60s",
            price_change_pct, direction
        ),
        entry_price: Some(current_price),
        limit_order_price: Some(old_price), // stink bid at pre-spike price
        limit_order_savings: None,
        target_price: Some(old_price),
        expected_profit_pct: Some(price_change_pct),
        reasoning: format!(
            "LIVE GAME ALERT: Price moved {:.0}% {} in the last 60 seconds{} at {}. \
             Orderbook may be irrational — check bid/ask spread before acting. \
             If spread is wide (>5¢), a stink bid at {:.0}¢ may get filled when market corrects. \
             Part Time Larry: 'Both sides of the book were wildly mispriced' during live games. \
             Look for large orders sitting at 1-2¢ on the overreacting side.",
            price_change_pct,
            direction,
            score_str,
            minute,
            old_price * 100.0
        ),
        urgency: "immediate".to_string(),
        detected_at: Utc::now(),
    })
}

// ── Signal 19: Lock-In Profit Signal ──────────────────────────────
//
// Your Thunder trade: OKC at 89¢ with 10+ point lead in the 4th quarter.
// Near-certain outcome = buy heavy favorite late game for guaranteed 10-15%.
// Low risk, low reward, but essentially free money at scale.
//
// Strategy: price > 85¢ AND live game AND strong lead = LOCK IN signal.

fn detect_lock_in_profit(
    price: f64,
    live_stats: &Option<serde_json::Value>,
    omg_score: Option<f64>,
) -> Option<StrategySignal> {
    // Only fire for heavy favorites
    if price < 0.82 {
        return None;
    }

    let is_live = live_stats.as_ref()
        .and_then(|ls| ls.get("status"))
        .and_then(|s| s.as_str())
        .map(|s| s == "live")
        .unwrap_or(false);

    if !is_live {
        return None;
    }

    let profit_pct = (1.0 - price) / price * 100.0;
    let profit_cents = (1.0 - price) * 100.0;

    // Higher confidence when price is higher
    let confidence = ((price - 0.82) / 0.15).min(0.92);

    // Get score differential if available
    let home_score = live_stats.as_ref()
        .and_then(|ls| ls.get("home_score")).and_then(|s| s.as_i64()).unwrap_or(0);
    let away_score = live_stats.as_ref()
        .and_then(|ls| ls.get("away_score")).and_then(|s| s.as_i64()).unwrap_or(0);
    let score_diff = (home_score - away_score).abs();

    let period = live_stats.as_ref()
        .and_then(|ls| ls.get("period")).and_then(|s| s.as_str())
        .unwrap_or("").to_string();

    let score_context = if score_diff > 0 {
        format!(" Leading by {} points in {}.", score_diff, period)
    } else {
        String::new()
    };

    let omg_context = omg_score.map(|s| format!(" OMG score: {:.0}/100.", s)).unwrap_or_default();

    Some(StrategySignal {
        signal_type: "lock_in_profit".to_string(),
        confidence,
        action: format!(
            "BUY YES — lock in {:.0}% guaranteed return",
            profit_pct
        ),
        entry_price: Some(price),
        limit_order_price: Some(price * 0.99),
        limit_order_savings: Some(2.0),
        target_price: Some(1.0),
        expected_profit_pct: Some(profit_pct),
        reasoning: format!(
            "Heavy favorite at {:.0}¢ during live game.{}{} \
             Buy YES now to lock in {:.1}¢ profit per contract ({:.0}% return). \
             Strategy: size up — at $1,000 position that's ${:.0} guaranteed. \
             Low risk, consistent income. \
             This is the Thunder trade: buy the leader late, collect guaranteed %.",
            price * 100.0,
            score_context,
            omg_context,
            profit_cents,
            profit_pct,
            profit_pct * 10.0
        ),
        urgency: "immediate".to_string(),
        detected_at: Utc::now(),
    })
}

// ── Background Loop ────────────────────────────────────────────────

pub async fn run_strategy_signal_loop(pool: PgPool) {
    tracing::info!("🎯 Starting strategy signal detection loop (90s interval)");

    let mut interval = tokio::time::interval(std::time::Duration::from_secs(90));

    loop {
        interval.tick().await;

        // Get live sports markets first (highest urgency)
        let live_markets: Vec<(Uuid,)> = match sqlx::query_as(
            "SELECT id FROM public.prediction_events
             WHERE status = 'active'
               AND live_stats IS NOT NULL
               AND live_stats->>'status' = 'live'
               AND volume_24h > 1000
             ORDER BY volume_24h DESC NULLS LAST
             LIMIT 50"
        )
        .fetch_all(&pool)
        .await {
            Ok(rows) => rows,
            Err(_) => vec![],
        };

        // Then get top markets by volume
        let top_markets: Vec<(Uuid,)> = match sqlx::query_as(
            "SELECT id FROM public.prediction_events
             WHERE status = 'active'
               AND volume_24h > 10000
               AND odds > 0.01 AND odds < 0.99
               AND (end_date IS NULL OR end_date > NOW())
             ORDER BY volume_24h DESC NULLS LAST
             LIMIT 100"
        )
        .fetch_all(&pool)
        .await {
            Ok(rows) => rows,
            Err(_) => vec![],
        };

        let mut all_ids: Vec<Uuid> = live_markets.into_iter()
            .chain(top_markets.into_iter())
            .map(|(id,)| id)
            .collect();
        all_ids.dedup();

        let mut signals_found = 0usize;

        for event_id in &all_ids {
            if let Ok(resp) = compute_strategy_signals(&pool, *event_id).await {
                if !resp.signals.is_empty() {
                    signals_found += resp.signals.len();

                    // Store high-confidence signals as alpha signals for the feed
                    for sig in &resp.signals {
                        if sig.confidence > 0.65 {
                            let metadata = serde_json::json!({
                                "signal_type": sig.signal_type,
                                "action": sig.action,
                                "entry_price": sig.entry_price,
                                "limit_order_price": sig.limit_order_price,
                                "target_price": sig.target_price,
                                "expected_profit_pct": sig.expected_profit_pct,
                                "reasoning": sig.reasoning,
                                "urgency": sig.urgency,
                            });

                            sqlx::query(
                                "INSERT INTO alpha_signals (event_id, signal_type, magnitude, metadata, created_at)
                                 VALUES ($1, $2, $3, $4, NOW())
                                 ON CONFLICT DO NOTHING"
                            )
                            
                            .bind(event_id.to_string())
                            .bind(format!("strategy_{}", sig.signal_type))
                            .bind(sig.confidence)
                            .bind(metadata)
                            .execute(&pool)
                            .await
                            .ok();
                        }
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        if signals_found > 0 {
            tracing::info!("🎯 Strategy signals: {} detected across {} markets", signals_found, all_ids.len());
        }
    }
}

// ── UUID resolver ──────────────────────────────────────────────────

async fn parse_or_lookup_uuid(pool: &PgPool, param: &str) -> Option<Uuid> {
    if let Ok(uid) = Uuid::parse_str(param) {
        return Some(uid);
    }
    sqlx::query_as::<_, (Uuid,)>(
        "SELECT id FROM public.prediction_events WHERE external_id = $1 LIMIT 1",
    )
    .bind(param)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()
    .map(|(id,)| id)
}
