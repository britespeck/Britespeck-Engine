//! Expected Value (EV) calculator and Quarter Kelly position sizing.
//!
//! Given an AI-estimated true probability and the current market price,
//! calculates whether a contract is mispriced and how much to bet.

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
    pub bankroll: Option<f64>,
    pub platform: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvResult {
    pub event_id: String,
    /// Best-side EV (matches frontend `result.ev`). Positive = +EV trade.
    pub ev: f64,
    pub ev_yes: f64,
    pub ev_no: f64,
    pub recommended_side: String, // "yes", "no", or "pass"
    pub edge_percent: f64,
    /// Frontend reads this as the displayed Kelly%. We expose Quarter Kelly here.
    pub kelly_fraction: f64,
    pub full_kelly: f64,
    pub quarter_kelly: f64,
    pub suggested_size: Option<f64>,
    pub confidence: String,       // "high", "medium", "low"
    pub reasoning: String,
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
    pub signal_status: String,
    pub created_at: DateTime<Utc>,
}

// ── Constants ──────────────────────────────────────────────────────

const MIN_EDGE_THRESHOLD: f64 = 0.03; // 3 cents
const MAX_KELLY_CAP: f64 = 0.25;
const KELLY_DIVISOR: f64 = 4.0;

// ── EV Calculation ─────────────────────────────────────────────────

pub fn calculate_ev(ai_prob: f64, market_price: f64) -> (f64, f64) {
    let p = ai_prob.clamp(0.001, 0.999);
    let m = market_price.clamp(0.001, 0.999);

    let ev_yes = (p * (1.0 - m)) - ((1.0 - p) * m);
    let ev_no = ((1.0 - p) * m) - (p * (1.0 - m));

    (ev_yes, ev_no)
}

// ── Kelly Criterion ────────────────────────────────────────────────

pub fn calculate_kelly(ai_prob: f64, market_price: f64) -> (f64, f64) {
    let p = ai_prob.clamp(0.001, 0.999);
    let m = market_price.clamp(0.001, 0.999);

    let b_yes = (1.0 - m) / m;
    let q = 1.0 - p;
    let kelly_yes = ((p * b_yes) - q) / b_yes;

    let b_no = m / (1.0 - m);
    let kelly_no = (((1.0 - p) * b_no) - p) / b_no;

    let kelly = if kelly_yes > kelly_no { kelly_yes } else { kelly_no };

    let full_kelly = kelly.clamp(0.0, MAX_KELLY_CAP);
    let quarter_kelly = (full_kelly / KELLY_DIVISOR).clamp(0.0, MAX_KELLY_CAP);

    (full_kelly, quarter_kelly)
}

// ── Full EV Engine ─────────────────────────────────────────────────

pub fn compute_edge(req: &EvRequest) -> EvResult {
    // Sanitize inputs (caller already validates, but defense in depth)
    let ai_prob = req.ai_probability.clamp(0.001, 0.999);
    let market_price = req.market_price.clamp(0.001, 0.999);

    let (ev_yes, ev_no) = calculate_ev(ai_prob, market_price);
    let (full_kelly, quarter_kelly) = calculate_kelly(ai_prob, market_price);

    let edge_yes = ai_prob - market_price;
    let edge_no = (1.0 - ai_prob) - (1.0 - market_price);

    let (recommended_side, edge_percent, best_ev) =
        if edge_yes > edge_no && edge_yes > MIN_EDGE_THRESHOLD {
            ("yes".to_string(), edge_yes * 100.0, ev_yes)
        } else if edge_no > MIN_EDGE_THRESHOLD {
            ("no".to_string(), edge_no * 100.0, ev_no)
        } else {
            (
                "pass".to_string(),
                edge_yes.abs().max(edge_no.abs()) * 100.0,
                0.0,
            )
        };

    let suggested_size = req.bankroll.map(|b| (quarter_kelly * b).max(0.0));

    let confidence = if edge_percent.abs() > 15.0 {
        "high"
    } else if edge_percent.abs() > 7.0 {
        "medium"
    } else {
        "low"
    };

    let reasoning = build_reasoning(
        &recommended_side,
        ai_prob,
        market_price,
        edge_percent,
        quarter_kelly,
        suggested_size,
        req.platform.as_deref(),
    );

    EvResult {
        event_id: req.event_id.clone(),
        ev: best_ev,
        ev_yes,
        ev_no,
        recommended_side,
        edge_percent,
        kelly_fraction: quarter_kelly, // what the UI displays
        full_kelly,
        quarter_kelly,
        suggested_size,
        confidence: confidence.to_string(),
        reasoning,
        ai_probability: ai_prob,
        market_price,
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
            edge,
            ai_prob * 100.0,
            market_price * 100.0,
            platform_name
        ),
        "yes" => {
            let mut s = format!(
                "BUY YES — AI sees {:.0}% true probability vs {:.0}% on {}, \
                 giving a +{:.1}% edge. Quarter Kelly: {:.1}% of bankroll.",
                ai_prob * 100.0,
                market_price * 100.0,
                platform_name,
                edge,
                qk * 100.0
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
                ai_prob * 100.0,
                market_price * 100.0,
                platform_name,
                edge,
                qk * 100.0
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

/// Store a trade signal. Skips writes for "pass" recommendations to avoid noise.
pub async fn persist_trade_signal(
    pool: &PgPool,
    req: &EvRequest,
    result: &EvResult,
) -> anyhow::Result<TradeSignal> {
    let signal_status = if result.recommended_side == "pass" {
        "passed".to_string()
    } else {
        "active".to_string()
    };

    let signal = TradeSignal {
        id: Uuid::new_v4(),
        event_id: req.event_id.clone(),
        user_id: req.user_id.clone(),
        ev: result.ev,
        kelly_fraction: result.quarter_kelly,
        suggested_size: result.suggested_size,
        ai_probability: result.ai_probability,
        market_price: result.market_price,
        recommended_side: result.recommended_side.clone(),
        edge_percent: result.edge_percent,
        signal_status,
        created_at: Utc::now(),
    };

    // Only persist actionable signals
    if signal.signal_status == "active" {
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
    }

    Ok(signal)
}

pub async fn get_user_signals(
    pool: &PgPool,
    user_id: &str,
    status: Option<&str>,
    limit: i64,
) -> anyhow::Result<Vec<TradeSignal>> {
    let limit = limit.clamp(1, 500);

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
