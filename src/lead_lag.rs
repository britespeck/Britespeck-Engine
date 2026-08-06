//! Lead/Lag Signal Detection Engine — v1 (LOCKED for first 100 signals)
//!
//! ══════════════════════════════════════════════════════════════════
//! W* FORMULA
//! ══════════════════════════════════════════════════════════════════
//!
//!   leader_pct_move = (leader_now - leader_before) / leader_before
//!   W* = W0 × (1 + ε × leader_pct_move)
//!
//!   ε = FIXED per relationship type (no dynamic adjustment in v1)
//!       Dynamic elasticity was removed — it inflates Δ endogenously
//!       and introduces false positives in late-stage games.
//!       Re-evaluate after 100 signals using elasticity_audit view.
//!
//! ══════════════════════════════════════════════════════════════════
//! Z-SCORE
//! ══════════════════════════════════════════════════════════════════
//!
//!   Z = Δ / σ
//!   σ = rolling std dev of lagger price CHANGES over last N ticks
//!   σ floored at MIN_SIGMA to prevent blowup in thin markets
//!
//! ══════════════════════════════════════════════════════════════════
//! ENTRY FILTERS (all must pass)
//! ══════════════════════════════════════════════════════════════════
//!
//!   1. Leader moved ≥ MIN_LEADER_PCT_MOVE (% not absolute — avoids bias)
//!   2. Z ≥ Z_THRESHOLD
//!   3. Depth ≥ MIN_DEPTH_DOLLARS (raised to $1,500 for real scalability)
//!   4. Spread ≤ MAX_SPREAD_PCT
//!   5. Time remaining ≥ MIN_TIME_REMAINING_SECS
//!
//! ══════════════════════════════════════════════════════════════════
//! EXIT RULE (locked)
//! ══════════════════════════════════════════════════════════════════
//!
//!   Exit A: current_price ≥ W0 + 0.80 × Δ
//!   Exit B: elapsed ≥ MAX_HOLD_SECS (300s)
//!
//! ══════════════════════════════════════════════════════════════════
//! DO NOT CHANGE ANY CONSTANT until 100 signals logged
//! ══════════════════════════════════════════════════════════════════

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing::{info, warn};
use anyhow::Result;

// ── LOCKED CONSTANTS ───────────────────────────────────────────────────────

/// Rolling window for σ computation (price change ticks per contract)
const N_SIGMA_WINDOW: usize = 20;

/// σ floor — prevents Z explosion in illiquid contracts
const MIN_SIGMA: f64 = 0.005;

/// Minimum leader PERCENTAGE move to trigger scan
/// Using % not absolute — prevents bias toward low-price contracts.
/// 0.08 = 8% move required (e.g. 0.50→0.54 or 0.20→0.216)
const MIN_LEADER_PCT_MOVE: f64 = 0.08;

/// Z-score threshold
const Z_THRESHOLD: f64 = 1.5;

/// Minimum order book depth at best bid/ask
/// Raised from $500 — if avg trader deploys $1-2K, $500 depth = real slippage
const MIN_DEPTH_DOLLARS: f64 = 1500.0;

/// Maximum spread as fraction of mid price
const MAX_SPREAD_PCT: f64 = 0.08;

/// Minimum seconds to contract close
const MIN_TIME_REMAINING_SECS: i64 = 120;

/// Exit A: capture this fraction of Δ
const CONVERGENCE_TARGET: f64 = 0.80;

/// Exit B: max hold time
const MAX_HOLD_SECS: i64 = 300;

// ── FIXED ELASTICITY TABLE (replaces dynamic adjustment) ───────────────────
//
// These are fixed for v1. Do NOT adjust until elasticity_audit shows drift.
// Dynamic elasticity was removed because:
//   - It makes Δ endogenous to leader price
//   - Inflates Z in late-stage games (more false positives)
//   - Makes first 100 signals incomparable (moving target)
//
// After 100 signals: compare elasticity_used vs actual_lagger_move_pct
// to recalibrate each type.

pub fn fixed_elasticity(relationship_type: &str) -> f64 {
    match relationship_type {
        "TournamentAdvancement" => 0.55,
        "GameToSpread"          => 0.80,
        "GameToTotal"           => 0.45,
        "GameToPlayerProp"      => 0.35,
        _                       => 0.50,
    }
}

// ── Data Structures ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeadLagPair {
    pub leader_ticker: String,
    pub leader_title: String,
    pub leader_price_before: f64,
    pub leader_price_now: f64,
    pub leader_pct_move: f64,

    pub lagger_ticker: String,
    pub lagger_title: String,
    pub lagger_price_w0: f64,
    pub lagger_implied_wstar: f64,
    pub deviation_delta: f64,
    pub deviation_pct: f64,

    pub z_score: f64,
    pub sigma_used: f64,
    pub sigma_window_size: usize,
    pub elasticity: f64,                 // fixed value used

    pub lagger_depth_dollars: f64,
    pub lagger_spread: f64,
    pub lagger_spread_pct: f64,
    pub seconds_to_close: i64,

    pub signal_strength: SignalStrength,
    pub relationship_type: String,
    pub detected_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SignalStrength {
    Strong,  // Z >= 2.5
    Medium,  // Z >= 1.5
}

impl std::fmt::Display for SignalStrength {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Strong => write!(f, "Strong"),
            Self::Medium => write!(f, "Medium"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeadLagSignal {
    pub id: uuid::Uuid,
    pub pair: LeadLagPair,

    pub entry_triggered: bool,
    pub entry_blocked_reason: Option<String>,
    pub entry_price: Option<f64>,
    pub entry_at: Option<DateTime<Utc>>,
    pub position_size_dollars: Option<f64>,

    pub exit_price: Option<f64>,
    pub exit_at: Option<DateTime<Utc>>,
    pub exit_reason: Option<ExitReason>,

    pub convergence_achieved_pct: Option<f64>,
    pub pnl_dollars: Option<f64>,
    pub pnl_vs_model_optimal: Option<f64>,

    pub entry_latency_ms: Option<i64>,
    pub exit_latency_vs_optimal_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExitReason {
    ConvergenceTarget,
    TimeCutoff,
    ManualOverride,
    MarketResolved,
}

impl std::fmt::Display for ExitReason {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::ConvergenceTarget => write!(f, "ConvergenceTarget"),
            Self::TimeCutoff        => write!(f, "TimeCutoff"),
            Self::ManualOverride    => write!(f, "ManualOverride"),
            Self::MarketResolved    => write!(f, "MarketResolved"),
        }
    }
}

// ── Core Formulas ──────────────────────────────────────────────────────────

/// W* = W0 × (1 + ε × leader_pct_move)
///
/// ε is FIXED per relationship type — no dynamic adjustment.
pub fn calculate_wstar(
    leader_price_before: f64,
    leader_price_now: f64,
    lagger_w0: f64,
    elasticity: f64,
) -> f64 {
    let leader_pct_move = (leader_price_now - leader_price_before)
        / leader_price_before.max(0.001);
    let wstar = lagger_w0 * (1.0 + elasticity * leader_pct_move);
    wstar.min(0.97).max(0.01)
}

/// σ = std dev of last N lagger price changes (per contract)
pub fn calculate_sigma(recent_prices: &[f64], spread: f64) -> (f64, usize) {
    if recent_prices.len() < 3 {
        return ((spread * 0.5).max(MIN_SIGMA), recent_prices.len());
    }

    let window = &recent_prices[recent_prices.len().saturating_sub(N_SIGMA_WINDOW)..];
    let changes: Vec<f64> = window.windows(2).map(|w| w[1] - w[0]).collect();

    if changes.is_empty() {
        return ((spread * 0.5).max(MIN_SIGMA), 0);
    }

    let mean = changes.iter().sum::<f64>() / changes.len() as f64;
    let variance = changes.iter()
        .map(|c| (c - mean).powi(2))
        .sum::<f64>() / changes.len() as f64;

    (variance.sqrt().max(MIN_SIGMA), window.len())
}

// ── Detector ───────────────────────────────────────────────────────────────

pub struct LeadLagDetector;

impl LeadLagDetector {

    pub fn detect(
        leader_ticker: &str,
        leader_title: &str,
        leader_price_before: f64,
        leader_price_now: f64,
        lagger_ticker: &str,
        lagger_title: &str,
        lagger_price_w0: f64,
        lagger_recent_prices: &[f64],
        lagger_depth_dollars: f64,
        lagger_bid: f64,
        lagger_ask: f64,
        seconds_to_close: i64,
        relationship_type: &str,
    ) -> Option<LeadLagPair> {

        let leader_move_abs = leader_price_now - leader_price_before;

        // Only detect upward leader moves (lagger should catch up)
        if leader_move_abs <= 0.0 { return None; }

        // ── Filter 1: % move threshold (not absolute) ──────────────────────
        let leader_pct_move = leader_move_abs / leader_price_before.max(0.001);
        if leader_pct_move < MIN_LEADER_PCT_MOVE {
            return None; // silent — fires constantly
        }

        // ── Filter 2: Depth ────────────────────────────────────────────────
        if lagger_depth_dollars < MIN_DEPTH_DOLLARS {
            info!("BLOCKED depth ${:.0} < ${:.0} | {}", lagger_depth_dollars, MIN_DEPTH_DOLLARS, lagger_ticker);
            return None;
        }

        // ── Filter 3: Spread ───────────────────────────────────────────────
        let mid = (lagger_bid + lagger_ask) / 2.0;
        let spread = lagger_ask - lagger_bid;
        let spread_pct = spread / mid.max(0.001);
        if spread_pct > MAX_SPREAD_PCT {
            info!("BLOCKED spread {:.1}% > {:.1}% | {}", spread_pct * 100.0, MAX_SPREAD_PCT * 100.0, lagger_ticker);
            return None;
        }

        // ── Filter 4: Time ─────────────────────────────────────────────────
        if seconds_to_close < MIN_TIME_REMAINING_SECS {
            return None;
        }

        // ── W* (fixed ε) ───────────────────────────────────────────────────
        let epsilon = fixed_elasticity(relationship_type);
        let wstar = calculate_wstar(leader_price_before, leader_price_now, lagger_price_w0, epsilon);
        let delta = wstar - lagger_price_w0;
        let deviation_pct = delta / lagger_price_w0.max(0.001) * 100.0;

        if delta <= 0.0 { return None; }

        // ── Z-score ────────────────────────────────────────────────────────
        let (sigma, window_size) = calculate_sigma(lagger_recent_prices, spread);
        let z_score = delta / sigma;

        // ── Filter 5: Z threshold ──────────────────────────────────────────
        if z_score < Z_THRESHOLD {
            info!("BLOCKED Z={:.2} < {:.1} | {} | σ={:.4}", z_score, Z_THRESHOLD, lagger_ticker, sigma);
            return None;
        }

        let signal_strength = if z_score >= 2.5 { SignalStrength::Strong } else { SignalStrength::Medium };

        info!(
            "🎯 LEAD/LAG [{:?}] | Leader {} {:.0}¢→{:.0}¢ (+{:.1}%) | \
             Lagger {} W0={:.0}¢ W*={:.0}¢ Δ={:.1}¢ Z={:.2} ε={:.2} \
             depth=${:.0} spread={:.1}% {}s | {}",
            signal_strength,
            leader_ticker, leader_price_before*100.0, leader_price_now*100.0, leader_pct_move*100.0,
            lagger_ticker, lagger_price_w0*100.0, wstar*100.0, delta*100.0,
            z_score, epsilon, lagger_depth_dollars, spread_pct*100.0, seconds_to_close,
            relationship_type,
        );

        Some(LeadLagPair {
            leader_ticker: leader_ticker.to_string(),
            leader_title: leader_title.to_string(),
            leader_price_before,
            leader_price_now,
            leader_pct_move,
            lagger_ticker: lagger_ticker.to_string(),
            lagger_title: lagger_title.to_string(),
            lagger_price_w0,
            lagger_implied_wstar: wstar,
            deviation_delta: delta,
            deviation_pct,
            z_score,
            sigma_used: sigma,
            sigma_window_size: window_size,
            elasticity: epsilon,
            lagger_depth_dollars,
            lagger_spread: spread,
            lagger_spread_pct: spread_pct,
            seconds_to_close,
            signal_strength,
            relationship_type: relationship_type.to_string(),
            detected_at: Utc::now(),
        })
    }

    /// Check exit. Call every 5–10 seconds for open positions.
    pub fn check_exit(signal: &LeadLagSignal, current_price: f64) -> Option<ExitReason> {
        let entry = signal.entry_price?;
        let entry_at = signal.entry_at?;
        let delta = signal.pair.lagger_implied_wstar - entry;
        if delta <= 0.0 { return None; }

        let target = entry + CONVERGENCE_TARGET * delta;
        let elapsed = (Utc::now() - entry_at).num_seconds();
        let conv_pct = ((current_price - entry) / delta * 100.0).max(0.0);

        if current_price >= target {
            info!("✅ EXIT-A Convergence | {} | {:.0}¢→{:.0}¢ | {:.1}% Δ | {}s",
                signal.pair.lagger_ticker, entry*100.0, current_price*100.0, conv_pct, elapsed);
            return Some(ExitReason::ConvergenceTarget);
        }

        if elapsed >= MAX_HOLD_SECS {
            warn!("⏰ EXIT-B Timeout | {} | {:.0}¢→{:.0}¢ | {:.1}% Δ | {}s",
                signal.pair.lagger_ticker, entry*100.0, current_price*100.0, conv_pct, elapsed);
            return Some(ExitReason::TimeCutoff);
        }

        None
    }

    /// Compute outcome metrics after exit.
    pub fn finalize(
        signal: &mut LeadLagSignal,
        exit_price: f64,
        exit_at: DateTime<Utc>,
        exit_reason: ExitReason,
        position_size_dollars: f64,
    ) {
        let entry = match signal.entry_price { Some(p) => p, None => return };
        let entry_at = match signal.entry_at { Some(t) => t, None => return };
        let delta = signal.pair.lagger_implied_wstar - entry;

        let contracts = position_size_dollars / entry.max(0.001);
        let pnl = contracts * (exit_price - entry);

        let convergence_pct = if delta > 0.0 {
            Some((exit_price - entry) / delta * 100.0)
        } else { None };

        let optimal_exit = entry + CONVERGENCE_TARGET * delta;
        let optimal_pnl = contracts * (optimal_exit - entry);

        let entry_latency_ms = (entry_at - signal.pair.detected_at).num_milliseconds();

        signal.exit_price = Some(exit_price);
        signal.exit_at = Some(exit_at);
        signal.exit_reason = Some(exit_reason);
        signal.position_size_dollars = Some(position_size_dollars);
        signal.convergence_achieved_pct = convergence_pct;
        signal.pnl_dollars = Some(pnl);
        signal.pnl_vs_model_optimal = Some(pnl - optimal_pnl);
        signal.entry_latency_ms = Some(entry_latency_ms);
    }
}

// ── Database ───────────────────────────────────────────────────────────────

pub async fn upsert_signal(pool: &PgPool, s: &LeadLagSignal) -> Result<()> {
    sqlx::query(
        "INSERT INTO lead_lag_signals (
            id,
            leader_ticker, leader_title, leader_price_before, leader_price_now, leader_pct_move,
            lagger_ticker, lagger_title, lagger_price_w0, lagger_implied_wstar,
            deviation_delta, deviation_pct, z_score, sigma_used, sigma_window_size, elasticity_used,
            lagger_depth_dollars, lagger_spread, lagger_spread_pct, seconds_to_close,
            signal_strength, relationship_type, detected_at,
            entry_triggered, entry_blocked_reason, entry_price, entry_at, position_size_dollars,
            exit_price, exit_at, exit_reason,
            convergence_achieved_pct, pnl_dollars, pnl_vs_model_optimal,
            entry_latency_ms, exit_latency_vs_optimal_ms
        ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
            $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
            $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,
            $31,$32,$33,$34,$35,$36
        )
        ON CONFLICT (id) DO UPDATE SET
            exit_price = EXCLUDED.exit_price,
            exit_at = EXCLUDED.exit_at,
            exit_reason = EXCLUDED.exit_reason,
            convergence_achieved_pct = EXCLUDED.convergence_achieved_pct,
            pnl_dollars = EXCLUDED.pnl_dollars,
            pnl_vs_model_optimal = EXCLUDED.pnl_vs_model_optimal,
            position_size_dollars = EXCLUDED.position_size_dollars,
            entry_latency_ms = EXCLUDED.entry_latency_ms,
            exit_latency_vs_optimal_ms = EXCLUDED.exit_latency_vs_optimal_ms,
            updated_at = NOW()"
    )
    .bind(s.id)
    .bind(&s.pair.leader_ticker).bind(&s.pair.leader_title)
    .bind(s.pair.leader_price_before).bind(s.pair.leader_price_now).bind(s.pair.leader_pct_move)
    .bind(&s.pair.lagger_ticker).bind(&s.pair.lagger_title)
    .bind(s.pair.lagger_price_w0).bind(s.pair.lagger_implied_wstar)
    .bind(s.pair.deviation_delta).bind(s.pair.deviation_pct)
    .bind(s.pair.z_score).bind(s.pair.sigma_used).bind(s.pair.sigma_window_size as i32)
    .bind(s.pair.elasticity)
    .bind(s.pair.lagger_depth_dollars).bind(s.pair.lagger_spread).bind(s.pair.lagger_spread_pct)
    .bind(s.pair.seconds_to_close)
    .bind(s.pair.signal_strength.to_string()).bind(&s.pair.relationship_type).bind(s.pair.detected_at)
    .bind(s.entry_triggered).bind(&s.entry_blocked_reason)
    .bind(s.entry_price).bind(s.entry_at).bind(s.position_size_dollars)
    .bind(s.exit_price).bind(s.exit_at)
    .bind(s.exit_reason.as_ref().map(|e| e.to_string()))
    .bind(s.convergence_achieved_pct).bind(s.pnl_dollars).bind(s.pnl_vs_model_optimal)
    .bind(s.entry_latency_ms).bind(s.exit_latency_vs_optimal_ms)
    .execute(pool)
    .await?;
    Ok(())
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wstar() {
        // Spain WC match 0.50→0.70, WC winner lagger 0.25, ε=0.55
        let w = calculate_wstar(0.50, 0.70, 0.25, 0.55);
        // 0.25 × (1 + 0.55 × 0.40) = 0.25 × 1.22 = 0.305
        assert!((w - 0.305).abs() < 0.001, "W*={}", w);
    }

    #[test]
    fn test_fixed_elasticity_not_dynamic() {
        // Elasticity must NOT change with leader price
        let e1 = fixed_elasticity("TournamentAdvancement");
        let e2 = fixed_elasticity("TournamentAdvancement");
        assert_eq!(e1, e2, "ε must be fixed");
        assert_eq!(e1, 0.55);
    }

    #[test]
    fn test_pct_move_filter() {
        // 0.70 → 0.75 = +7.1% — should fail MIN_LEADER_PCT_MOVE=8%
        let pct = (0.75 - 0.70) / 0.70;
        assert!(pct < MIN_LEADER_PCT_MOVE, "7.1% should be blocked");

        // 0.50 → 0.55 = +10% — should pass
        let pct2 = (0.55 - 0.50) / 0.50;
        assert!(pct2 >= MIN_LEADER_PCT_MOVE, "10% should pass");
    }

    #[test]
    fn test_depth_floor() {
        assert!(MIN_DEPTH_DOLLARS >= 1500.0, "Depth floor too low for real scalability");
    }

    #[test]
    fn test_sigma_floor() {
        let (sigma, _) = calculate_sigma(&[0.25], 0.04);
        assert!(sigma >= MIN_SIGMA);
    }

    #[test]
    fn test_exit_target_math() {
        let entry = 0.25f64;
        let wstar = 0.305f64;
        let delta = wstar - entry;
        let target = entry + CONVERGENCE_TARGET * delta;
        // 0.25 + 0.80 × 0.055 = 0.294
        assert!((target - 0.294).abs() < 0.001, "target={}", target);
    }
}

// ── Leader Move Bucket (diagnostic — no schema changes needed) ─────────────

/// Buckets the leader's % move for post-hoc threshold analysis.
/// Stored in lead_lag_signals.leader_move_bucket.
/// After 100 signals, query move_bucket_performance view to answer:
/// "Was 8% too strict? Should we use 6%? 10%?"
pub fn leader_move_bucket(pct_move: f64) -> &'static str {
    let pct = pct_move * 100.0;
    if pct < 5.0       { "<5%" }
    else if pct < 8.0  { "5-8%" }
    else if pct < 12.0 { "8-12%" }
    else if pct < 20.0 { "12-20%" }
    else               { ">20%" }
}

#[test]
fn test_bucket_assignment() {
    assert_eq!(leader_move_bucket(0.06), "5-8%");   // 6% → between thresholds
    assert_eq!(leader_move_bucket(0.09), "8-12%");  // passes filter
    assert_eq!(leader_move_bucket(0.25), ">20%");   // strong move
    assert_eq!(leader_move_bucket(0.04), "<5%");    // blocked by filter
}