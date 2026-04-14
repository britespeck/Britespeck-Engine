//! Expected Value (EV) calculator and Quarter Kelly position sizing.
//!
//! Given an AI-estimated true probability and the current market price,
//! calculates whether a contract is mispriced and how much to bet.
//! This is the math engine that turns signals into actionable trades.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

// ── Types ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvRequest {
    pub event_id: String,
    pub user_id: Option<String>,
    pub ai_probability: f64,     // AI's estimated true prob (0.0-1.0)
    pub market_price: f64,       // Current market YES price (0.0-1.0)
    pub bankroll: Option<f64>,   // User's total bankroll (optional)
    pub platform: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvResult {
    pub event_id: String,
    pub ev_yes: f64,              // Expected value of buying YES
    pub ev_no: f64,               // Expected value of buying NO
    pub recommended_side: String, // "yes", "no", or "pass"
    pub edge_percent: f64,        // How mispriced (positive = opportunity)
    pub kelly_fraction: f64,      // Full Kelly fraction
    pub quarter_kelly: f64,       // Conservative Quarter Kelly
    pub suggested_size: Option<f64>, // Dollar amount if bankroll provided
    pub confidence: String,       // "high", "medium", "low"
    pub reasoning: String,        // Human-readable explanation
    pub ai_probability: f64,
    pub market_price: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct TradeSignal {
    pub id: Uuid,
    pub event_id: String,
    pub user_id: Option<String>,
    pub ev: f64,
    pub kelly_fraction: f64,
    pub suggested_size: Option<f64>,
    pub ai_probability: f64,
    pub market_price: f64,
    pub recommended_side: String,
    pub edge_percent: f64,
    pub signal_status: String,  // "active", "executed", "expired", "passed"
    pub created_at: DateTime<Utc>,
}

// ── Constants ──────────────────────────────────────────────────────

/// Minimum edge (AI prob - market price) to recommend a trade.
/// Below this, it's noise.
const MIN_EDGE_THRESHOLD: f64 = 0.03; // 3 cents

/// Maximum Kelly fraction to ever recommend (safety cap).
const MAX_KELLY_CAP: f64 = 0.25; // Never bet more than 25% of bankroll

/// Kelly divisor for Quarter Kelly (conservative sizing).
const KELLY_DIVISOR: f64 = 4.0;

// ── EV Calculation ─────────────────────────────────────────────────

/// Calculate Expected Value for both YES and NO sides.
///
/// EV formula:
///   EV_YES = (ai_prob × payout_yes) - ((1 - ai_prob) × cost_yes)
///   Where payout_yes = 1.0 - market_price (what you gain if YES wins)
///         cost_yes = market_price (what you pay)
///
/// For prediction markets: you buy at market_price, payout is $1 if correct.
///   EV_YES = (ai_prob × (1.0 - market_price)) - ((1 - ai_prob) × market_price)
///          = ai_prob - market_price  (simplified)
pub fn calculate_ev(ai_prob: f64, market_price: f64) -> (f64, f64) {
    // Clamp inputs
    let p = ai_prob.clamp(0.001, 0.999);
    let m = market_price.clamp(0.001, 0.999);

    // EV for buying YES at market_price
    // Win: gain (1 - m), Lose: lose m
    let ev_yes = (p * (1.0 - m)) - ((1.0 - p) * m);

    // EV for buying NO at (1 - market_price)
    // Win: gain m, Lose: lose (1 - m)
    let ev_no = ((1.0 - p) * m) - (p * (1.0 - m));

    (ev_yes, ev_no)
}

// ── Kelly Criterion ────────────────────────────────────────────────

/// Calculate Kelly fraction for optimal bet sizing.
///
/// Kelly formula for binary bets:
///   f* = (p × b - q) / b
///   Where: p = prob of winning, q = 1-p, b = net odds (payout/cost)
///
/// Quarter Kelly = f* / 4 (much more conservative, protects bankroll)
pub fn calculate_kelly(ai_prob: f64, market_price: f64) -> (f64, f64) {
    let p = ai_prob.clamp(0.001, 0.999);
    let m = market_price.clamp(0.001, 0.999);

    // For YES side:
    let b_yes = (1.0 - m) / m; // Net odds: what you win / what you risk
    let q = 1.0 - p;
    let kelly_yes = ((p * b_yes) - q) / b_yes;

    // For NO side:
    let b_no = m / (1.0 - m);
    let kelly_no = (((1.0 - p) * b_no) - p) / b_no;

    // Use the positive side
    let kelly = if kelly_yes > kelly_no {
        kelly_yes
    } else {
        kelly_no
    };

    // Cap at maximum and floor at 0
    let full_kelly = kelly.clamp(0.0, MAX_KELLY_CAP);
    let quarter_kelly = (full_kelly / KELLY_DIVISOR).clamp(0.0, MAX_KELLY_CAP);

    (full_kelly, quarter_kelly)
}

// ── Full EV Engine ─────────────────────────────────────────────────

/// Main entry point: takes an EV request, returns full analysis.
pub fn compute_edge(req: &EvRequest) -> EvResult {
    let (ev_yes, ev_no) = calculate_ev(req.ai_probability, req.market_price);
    let (full_kelly, quarter_kelly) = calculate_kelly(req.ai_probability, req.market_price);

    // Determine recommended side
    let edge_yes = req.ai_probability - req.market_price;
    let edge_no = (1.0 - req.ai_probability) - (1.0 - req.market_price);

    let (recommended_side, edge_percent, _best_ev) = if edge_yes > edge_no && edge_yes > MIN_EDGE_THRESHOLD {
        ("yes".to_string(), edge_yes * 100.0, ev_yes)
    } else if edge_no > MIN_EDGE_THRESHOLD {
        ("no".to_string(), edge_no * 100.0, ev_no)
    } else {
        ("pass".to_string(), edge_yes.abs().max(edge_no.abs()) * 100.0, 0.0)
    };

    // Position sizing
    let suggested_size = req.bankroll.map(|b| (quarter_kelly * b).max(0.0));

    // Confidence assessment
    let confidence = if edge_percent.abs() > 15.0 {
        "high"
    } else if edge_percent.abs() > 7.0 {
        "medium"
    } else {
        "low"
    };

    // Build reasoning
    let reasoning = build_reasoning(
        &recommended_side,
        req.ai_probability,
        req.market_price,
        edge_percent,
        quarter_kelly,
        suggested_size,
        req.platform.as_deref(),
    );

    EvResult {
        event_id: req.event_id.clone(),
        ev_yes,
        ev_no,
        recommended_side,
        edge_percent,
        kelly_fraction: full_kelly,
        quarter_kelly,
        suggested_size,
        confidence: confidence.to_string(),
        reasoning,
        ai_probability: req.ai_probability,
        market_price: req.market_price,
    }
}

fn build_reasoning(
    side: &str,
    ai_prob: f64,
    market_price: f64,
    edge: f64,
    qk: f64,
    size: Option<f64>,
    platform: Option<&str>,
) -> String {
    let platform_name = platform.unwrap_or("the market");

    match side {
        "pass" => format!(
            "Edge too thin ({:.1}%). AI estimates {:.0}% vs {:.0}% on {}. \
             No recommended position — wait for better pricing.",
            edge, ai_prob * 100.0, market_price * 100.0, platform_name
        ),
        "yes" => {
            let mut s = format!(
                "BUY YES — AI sees {:.0}% true probability vs {:.0}% on {}, \
                 giving a +{:.1}% edge. Quarter Kelly: {:.1}% of bankroll.",
                ai_prob * 100.0, market_price * 100.0, platform_name,
                edge, qk * 100.0
            );
            if let Some(sz) = size {
                s.push_str(&format!(" Suggested position: ${:.2}.", sz));
            }
            s
        }
        "no" => {
            let mut s = format!(
                "BUY NO — AI sees {:.0}% YES probability but market prices at {:.0}% on {}. \
                 The NO side has a +{:.1}% edge. Quarter Kelly: {:.1}% of bankroll.",
                ai_prob * 100.0, market_price * 100.0, platform_name,
                edge, qk * 100.0
            );
            if let Some(sz) = size {
                s.push_str(&format!(" Suggested position: ${:.2}.", sz));
            }
            s
        }
        _ => "Unable to compute edge.".to_string(),
    }
}

// ── Database Persistence ───────────────────────────────────────────

/// Store a trade signal in the database.
pub async fn persist_trade_signal(
    pool: &PgPool,
    req: &EvRequest,
    result: &EvResult,
) -> anyhow::Result<TradeSignal> {
    let signal = TradeSignal {
        id: Uuid::new_v4(),
        event_id: req.event_id.clone(),
        user_id: req.user_id.clone(),
        ev: if result.recommended_side == "yes" { result.ev_yes } else { result.ev_no },
        kelly_fraction: result.quarter_kelly,
        suggested_size: result.suggested_size,
        ai_probability: result.ai_probability,
        market_price: result.market_price,
        recommended_side: result.recommended_side.clone(),
        edge_percent: result.edge_percent,
        signal_status: if result.recommended_side == "pass" {
            "passed".to_string()
        } else {
            "active".to_string()
        },
        created_at: Utc::now(),
    };

    sqlx::query(
        "INSERT INTO trade_signals 
         (id, event_id, user_id, ev, kelly_fraction, suggested_size, 
          ai_probability, market_price, recommended_side, edge_percent, 
          signal_status, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"
    )
    .bind(signal.id)
    .bind(&signal.event_id)
    .bind(&signal.user_id)
    .bind(signal.ev)
    .bind(signal.kelly_fraction)
    .bind(signal.suggested_size)
    .bind(signal.ai_probability)
    .bind(signal.market_price)
    .bind(&signal.recommended_side)
    .bind(signal.edge_percent)
    .bind(&signal.signal_status)
    .bind(signal.created_at)
    .execute(pool)
    .await?;

    Ok(signal)
}

/// Get active trade signals for a user.
pub async fn get_user_signals(
    pool: &PgPool,
    user_id: &str,
    status: Option<&str>,
    limit: i64,
) -> anyhow::Result<Vec<TradeSignal>> {
    let signals = if let Some(s) = status {
        sqlx::query_as::<_, TradeSignal>(
            "SELECT id, event_id, user_id, ev, kelly_fraction, suggested_size,
                    ai_probability, market_price, recommended_side, edge_percent,
                    signal_status, created_at
             FROM trade_signals
             WHERE user_id = $1 AND signal_status = $2
             ORDER BY created_at DESC
             LIMIT $3"
        )
        .bind(user_id)
        .bind(s)
        .bind(limit)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query_as::<_, TradeSignal>(
            "SELECT id, event_id, user_id, ev, kelly_fraction, suggested_size,
                    ai_probability, market_price, recommended_side, edge_percent,
                    signal_status, created_at
             FROM trade_signals
             WHERE user_id = $1
             ORDER BY created_at DESC
             LIMIT $2"
        )
        .bind(user_id)
        .bind(limit)
        .fetch_all(pool)
        .await?
    };

    Ok(signals)
}

/// Expire old active signals (e.g., market price moved significantly).
pub async fn expire_stale_signals(pool: &PgPool, max_age_hours: i64) -> anyhow::Result<usize> {
    let cutoff = Utc::now() - chrono::Duration::hours(max_age_hours);

    let result = sqlx::query(
        "UPDATE trade_signals SET signal_status = 'expired'
         WHERE signal_status = 'active' AND created_at < $1"
    )
    .bind(cutoff)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() as usize)
}
