//! Arbitrage signal detector for Britespeck.
//!
//! Detects price discrepancies between Kalshi and Polymarket
//! for the same real-world event. Shows users the opportunity
//! and profit — we do NOT execute trades.
//!
//! Math from taetaehoho/poly-kalshi-arb:
//!   Arb exists when: YES_ask + NO_ask + Kalshi_fee < $1.00
//!   Kalshi fee: ceil(0.07 × price × (1-price)) per contract
//!   Polymarket fee: $0
//!
//! Signal types:
//!   poly_yes_kalshi_no — Buy YES on Polymarket + NO on Kalshi
//!   kalshi_yes_poly_no — Buy YES on Kalshi + NO on Polymarket

use anyhow::Result;
use chrono::Utc;
use sqlx::PgPool;
use tracing::info;

// ─────────────────────────────────────────────────────────────────────────────
// FEE TABLE
// Precomputed Kalshi fee in cents for prices 1–99
// Formula: ceil(7 × p × (100-p) / 10000) cents
// Source: taetaehoho/poly-kalshi-arb types.rs
// ─────────────────────────────────────────────────────────────────────────────

static KALSHI_FEE_TABLE: [u16; 101] = {
    let mut table = [0u16; 101];
    let mut p = 1u32;
    while p < 100 {
        let numerator = 7 * p * (100 - p) + 9999;
        table[p as usize] = (numerator / 10000) as u16;
        p += 1;
    }
    table
};

/// Kalshi fee in cents for a single contract at price_cents (1–99)
#[inline]
fn kalshi_fee(price_cents: u16) -> u16 {
    if price_cents > 100 { return 0; }
    KALSHI_FEE_TABLE[price_cents as usize]
}

// ─────────────────────────────────────────────────────────────────────────────
// TICKER PARSING
// Adapted from taetaehoho/poly-kalshi-arb discovery.rs
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct ParsedTicker {
    date: String,   // "25DEC27"
    team1: String,  // "CFC"
    team2: String,  // "AVL"
}

/// Parse Kalshi event ticker into components.
/// Handles two formats:
///   "KXEPLGAME-25DEC27CFCAVL"         (date+teams combined)
///   "KXNCAAFGAME-25DEC27M-OHFRES"     (date and teams separate)
fn parse_kalshi_ticker(ticker: &str) -> Option<ParsedTicker> {
    let parts: Vec<&str> = ticker.split('-').collect();
    if parts.len() < 2 { return None; }

    let (date, teams_part) = if parts.len() >= 3 && parts[2].len() >= 4 {
        let date_part = parts[1];
        let date = if date_part.len() >= 7 {
            date_part[..7].to_uppercase()
        } else { return None; };
        (date, parts[2])
    } else {
        let date_teams = parts[1];
        if date_teams.len() < 11 { return None; }
        let date = date_teams[..7].to_uppercase();
        let teams = &date_teams[7..];
        (date, teams)
    };

    let (team1, team2) = split_team_codes(teams_part);
    Some(ParsedTicker { date, team1, team2 })
}

/// Split combined team string into two team codes.
/// Handles all the weird edge cases from real Kalshi tickers.
fn split_team_codes(teams: &str) -> (String, String) {
    let len = teams.len();
    match len {
        4 => (teams[..2].to_uppercase(), teams[2..].to_uppercase()),
        5 => (teams[..2].to_uppercase(), teams[2..].to_uppercase()),
        6 => {
            let first_two = &teams[..2].to_uppercase();
            if is_two_letter_code(first_two) {
                (first_two.clone(), teams[2..].to_uppercase())
            } else {
                (teams[..3].to_uppercase(), teams[3..].to_uppercase())
            }
        }
        7 => (teams[..3].to_uppercase(), teams[3..].to_uppercase()),
        _ if len >= 8 => (teams[..4].to_uppercase(), teams[4..].to_uppercase()),
        _ => {
            let mid = len / 2;
            (teams[..mid].to_uppercase(), teams[mid..].to_uppercase())
        }
    }
}

fn is_two_letter_code(code: &str) -> bool {
    matches!(code,
        "OM" | "OL" | "FC" |
        "OH" | "SF" | "LA" | "NY" | "KC" | "TB" | "GB" | "NE" | "NO" | "LV" |
        "BC" | "SC" | "AC" | "AS" | "US"
    )
}

/// "25DEC27" → "2025-12-27"
fn kalshi_date_to_iso(d: &str) -> String {
    if d.len() != 7 { return d.to_string(); }
    let year = format!("20{}", &d[..2]);
    let month = match &d[2..5].to_uppercase()[..] {
        "JAN" => "01", "FEB" => "02", "MAR" => "03", "APR" => "04",
        "MAY" => "05", "JUN" => "06", "JUL" => "07", "AUG" => "08",
        "SEP" => "09", "OCT" => "10", "NOV" => "11", "DEC" => "12",
        _ => "01",
    };
    format!("{}-{}-{}", year, month, &d[5..7])
}

// ─────────────────────────────────────────────────────────────────────────────
// LEAGUE CONFIG
// Series prefixes from taetaehoho/poly-kalshi-arb config.rs
// ─────────────────────────────────────────────────────────────────────────────

struct LeaguePrefix {
    kalshi_prefix: &'static str,
    poly_prefix: &'static str,
}

const LEAGUE_PREFIXES: &[LeaguePrefix] = &[
    LeaguePrefix { kalshi_prefix: "KXEPLGAME",             poly_prefix: "epl" },
    LeaguePrefix { kalshi_prefix: "KXBUNDESLIGAGAME",      poly_prefix: "bun" },
    LeaguePrefix { kalshi_prefix: "KXLALIGAGAME",          poly_prefix: "lal" },
    LeaguePrefix { kalshi_prefix: "KXSERIEAGAME",          poly_prefix: "sea" },
    LeaguePrefix { kalshi_prefix: "KXLIGUE1GAME",          poly_prefix: "fl1" },
    LeaguePrefix { kalshi_prefix: "KXUCLGAME",             poly_prefix: "ucl" },
    LeaguePrefix { kalshi_prefix: "KXUELGAME",             poly_prefix: "uel" },
    LeaguePrefix { kalshi_prefix: "KXEFLCHAMPIONSHIPGAME", poly_prefix: "elc" },
    LeaguePrefix { kalshi_prefix: "KXNBAGAME",             poly_prefix: "nba" },
    LeaguePrefix { kalshi_prefix: "KXNFLGAME",             poly_prefix: "nfl" },
    LeaguePrefix { kalshi_prefix: "KXNHLGAME",             poly_prefix: "nhl" },
    LeaguePrefix { kalshi_prefix: "KXMLBGAME",             poly_prefix: "mlb" },
    LeaguePrefix { kalshi_prefix: "KXMLSGAME",             poly_prefix: "mls" },
    LeaguePrefix { kalshi_prefix: "KXNCAAFGAME",           poly_prefix: "cfb" },
];

fn poly_prefix_for_kalshi_ticker(ticker: &str) -> Option<&'static str> {
    for lp in LEAGUE_PREFIXES {
        if ticker.starts_with(lp.kalshi_prefix) {
            return Some(lp.poly_prefix);
        }
    }
    None
}

// ─────────────────────────────────────────────────────────────────────────────
// ARB DETECTION
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ArbSignal {
    /// Kalshi contract ticker
    pub kalshi_ticker: String,
    /// Matching Polymarket external_id
    pub poly_id: String,
    /// Human-readable description
    pub description: String,
    /// Type: "poly_yes_kalshi_no" or "kalshi_yes_poly_no"
    pub arb_type: String,
    /// YES price in cents (the buy-YES leg)
    pub yes_price_cents: u16,
    /// NO price in cents (the buy-NO leg)
    pub no_price_cents: u16,
    /// Kalshi fee in cents
    pub fee_cents: u16,
    /// Guaranteed profit in cents per contract (after fees)
    pub profit_cents: i16,
    /// Profit as percentage of $1 payout
    pub profit_pct: f64,
    /// Combined cost in cents (yes + no + fee)
    pub total_cost_cents: u16,
    /// Detected at
    pub detected_at: chrono::DateTime<Utc>,
}

impl ArbSignal {
    /// Returns true if this is a real arb (profit > 0)
    pub fn is_profitable(&self) -> bool {
        self.profit_cents > 0
    }
}

/// Check if two contracts from different platforms have an arb.
/// Both contracts must be for the SAME real-world outcome (YES on one, NO on other).
///
/// Returns Some(ArbSignal) if profitable arb exists.
pub fn check_arb(
    kalshi_ticker: &str,
    kalshi_yes_ask: f64,  // 0.0 – 1.0
    kalshi_no_ask: f64,
    poly_id: &str,
    poly_yes_ask: f64,
    poly_no_ask: f64,
    description: &str,
    min_profit_cents: i16,
) -> Option<ArbSignal> {
    // Convert to cents
    let k_yes = (kalshi_yes_ask * 100.0).round() as u16;
    let k_no  = (kalshi_no_ask  * 100.0).round() as u16;
    let p_yes = (poly_yes_ask   * 100.0).round() as u16;
    let p_no  = (poly_no_ask    * 100.0).round() as u16;

    // Require valid prices
    if k_yes == 0 || k_no == 0 || p_yes == 0 || p_no == 0 { return None; }
    if k_yes >= 100 || k_no >= 100 || p_yes >= 100 || p_no >= 100 { return None; }

    // Check both cross-platform arb directions
    // Direction 1: Buy YES on Polymarket + NO on Kalshi
    let fee1 = kalshi_fee(k_no);
    let cost1 = p_yes + k_no + fee1;
    let profit1 = 100i16 - cost1 as i16;

    // Direction 2: Buy YES on Kalshi + NO on Polymarket
    let fee2 = kalshi_fee(k_yes);
    let cost2 = k_yes + p_no + fee2;
    let profit2 = 100i16 - cost2 as i16;

    let (arb_type, yes_price, no_price, fee, cost, profit) = if profit1 >= profit2 {
        ("poly_yes_kalshi_no", p_yes, k_no, fee1, cost1, profit1)
    } else {
        ("kalshi_yes_poly_no", k_yes, p_no, fee2, cost2, profit2)
    };

    if profit < min_profit_cents { return None; }

    Some(ArbSignal {
        kalshi_ticker: kalshi_ticker.to_string(),
        poly_id: poly_id.to_string(),
        description: description.to_string(),
        arb_type: arb_type.to_string(),
        yes_price_cents: yes_price,
        no_price_cents: no_price,
        fee_cents: fee,
        profit_cents: profit,
        profit_pct: profit as f64 / 100.0,
        total_cost_cents: cost,
        detected_at: Utc::now(),
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// DATABASE SCAN
// Scans all active paired contracts in prediction_events and checks for arbs
// ─────────────────────────────────────────────────────────────────────────────

/// Run a full arb scan across all paired Kalshi/Polymarket contracts in DB.
/// Inserts any found signals into arb_signals table.
pub async fn run_arb_scan(pool: &PgPool, min_profit_cents: i16) -> Result<Vec<ArbSignal>> {
    // Find all Kalshi contracts that have a matching Polymarket contract
    // Matching is done by looking up the poly_slug from the Kalshi ticker
    let rows = sqlx::query!(
        r#"
        SELECT
            k.external_id   AS "kalshi_id",
            k.title         AS "kalshi_title",
            k.odds          AS "kalshi_odds",
            p.external_id   AS "poly_id",
            p.title         AS "poly_title",
            p.odds          AS "poly_odds",
            k.updated_at    AS "kalshi_updated_at",
            p.updated_at    AS "poly_updated_at"
        FROM public.prediction_events k
        JOIN public.prediction_events p
            ON k.poly_pair_id = p.external_id
        WHERE k.platform = 'Kalshi'
          AND p.platform = 'Polymarket'
          AND k.status = 'active'
          AND p.status = 'active'
          AND k.odds IS NOT NULL
          AND p.odds IS NOT NULL
          AND k.odds > 0.01 AND k.odds < 0.99
          AND p.odds > 0.01 AND p.odds < 0.99
          AND k.updated_at > NOW() - INTERVAL '10 minutes'
          AND p.updated_at > NOW() - INTERVAL '10 minutes'
        "#
    )
    .fetch_all(pool)
    .await?;

    let mut signals = Vec::new();

    for row in rows {
        let kalshi_yes = row.kalshi_odds.unwrap_or(0.0);
        let poly_yes = row.poly_odds.unwrap_or(0.0);

        // YES on one platform, NO on the other
        // NO price ≈ 1 - YES price (for binary markets)
        let kalshi_no = 1.0 - kalshi_yes;
        let poly_no = 1.0 - poly_yes;

        let description = format!("{}", row.kalshi_title.as_deref().unwrap_or("Unknown"));

        if let Some(signal) = check_arb(
            row.kalshi_id.as_deref().unwrap_or(""),
            kalshi_yes,
            kalshi_no,
            row.poly_id.as_deref().unwrap_or(""),
            poly_yes,
            poly_no,
            &description,
            min_profit_cents,
        ) {
            signals.push(signal);
        }
    }

    if !signals.is_empty() {
        info!("🎯 Found {} arb signals (min profit: {}¢)", signals.len(), min_profit_cents);
        save_arb_signals(pool, &signals).await?;
    }

    Ok(signals)
}

/// Save arb signals to DB
async fn save_arb_signals(pool: &PgPool, signals: &[ArbSignal]) -> Result<()> {
    for s in signals {
        sqlx::query(
            "INSERT INTO arb_signals
             (kalshi_id, poly_id, description, arb_type,
              yes_price_cents, no_price_cents, fee_cents,
              profit_cents, profit_pct, total_cost_cents, detected_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
             ON CONFLICT (kalshi_id, poly_id, arb_type)
             DO UPDATE SET
               profit_cents = EXCLUDED.profit_cents,
               profit_pct = EXCLUDED.profit_pct,
               total_cost_cents = EXCLUDED.total_cost_cents,
               detected_at = EXCLUDED.detected_at"
        )
        .bind(&s.kalshi_ticker)
        .bind(&s.poly_id)
        .bind(&s.description)
        .bind(&s.arb_type)
        .bind(s.yes_price_cents as i32)
        .bind(s.no_price_cents as i32)
        .bind(s.fee_cents as i32)
        .bind(s.profit_cents as i32)
        .bind(s.profit_pct)
        .bind(s.total_cost_cents as i32)
        .bind(s.detected_at)
        .execute(pool)
        .await?;
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// GET CURRENT ARB SIGNALS (for API endpoint)
// ─────────────────────────────────────────────────────────────────────────────

pub async fn get_active_arb_signals(pool: &PgPool) -> Result<Vec<serde_json::Value>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            kalshi_id, poly_id, description, arb_type,
            yes_price_cents, no_price_cents, fee_cents,
            profit_cents, profit_pct, total_cost_cents, detected_at
        FROM arb_signals
        WHERE detected_at > NOW() - INTERVAL '15 minutes'
          AND profit_cents > 0
        ORDER BY profit_cents DESC
        LIMIT 50
        "#
    )
    .fetch_all(pool)
    .await?;

    let signals: Vec<serde_json::Value> = rows.iter().map(|r| {
        serde_json::json!({
            "kalshi_id": r.kalshi_id,
            "poly_id": r.poly_id,
            "description": r.description,
            "arb_type": r.arb_type,
            "yes_price_cents": r.yes_price_cents,
            "no_price_cents": r.no_price_cents,
            "fee_cents": r.fee_cents,
            "profit_cents": r.profit_cents,
            "profit_pct": r.profit_pct,
            "total_cost_cents": r.total_cost_cents,
            "detected_at": r.detected_at,
        })
    }).collect();

    Ok(signals)
}

// ─────────────────────────────────────────────────────────────────────────────
// TESTS
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arb_detected_poly_yes_kalshi_no() {
        // Poly YES 40¢ + Kalshi NO 50¢
        // Fee on 50¢ = 2¢
        // Cost = 92¢ → Profit = 8¢
        let signal = check_arb(
            "KXNBAGAME-25MAY10NYKPHI", 0.55, 0.50,
            "polymarket:nba-nyknicks-philaph-2025-05-10", 0.40, 0.65,
            "NY Knicks vs Philadelphia",
            1,
        ).expect("Should find arb");

        assert_eq!(signal.arb_type, "poly_yes_kalshi_no");
        assert_eq!(signal.yes_price_cents, 40);
        assert_eq!(signal.no_price_cents, 50);
        assert_eq!(signal.fee_cents, 2);
        assert_eq!(signal.profit_cents, 8);
    }

    #[test]
    fn test_no_arb_when_fees_eliminate() {
        // Poly YES 49¢ + Kalshi NO 50¢ + fee 2¢ = 101¢ → NO ARB
        let signal = check_arb(
            "KXNBAGAME-25MAY10NYKPHI", 0.55, 0.50,
            "polymarket:nba-nyknicks-philaph-2025-05-10", 0.49, 0.55,
            "NY Knicks vs Philadelphia",
            1,
        );
        assert!(signal.is_none(), "Fees should eliminate marginal arb");
    }

    #[test]
    fn test_fee_table_at_50_cents() {
        // ceil(7 × 50 × 50 / 10000) = ceil(1.75) = 2
        assert_eq!(kalshi_fee(50), 2);
    }

    #[test]
    fn test_fee_table_at_10_cents() {
        // ceil(7 × 10 × 90 / 10000) = ceil(0.63) = 1
        assert_eq!(kalshi_fee(10), 1);
    }

    #[test]
    fn test_parse_kalshi_ticker() {
        let parsed = parse_kalshi_ticker("KXEPLGAME-25DEC27CFCAVL").unwrap();
        assert_eq!(parsed.date, "25DEC27");
        assert_eq!(parsed.team1, "CFC");
        assert_eq!(parsed.team2, "AVL");
    }

    #[test]
    fn test_kalshi_date_to_iso() {
        assert_eq!(kalshi_date_to_iso("25DEC27"), "2025-12-27");
        assert_eq!(kalshi_date_to_iso("25MAY10"), "2025-05-10");
    }

    #[test]
    fn test_poly_prefix_lookup() {
        assert_eq!(poly_prefix_for_kalshi_ticker("KXNBAGAME-25MAY10NYKPHI"), Some("nba"));
        assert_eq!(poly_prefix_for_kalshi_ticker("KXEPLGAME-25DEC27CFCAVL"), Some("epl"));
        assert_eq!(poly_prefix_for_kalshi_ticker("KXNFLGAME-25SEP14NEVSF"), Some("nfl"));
    }
}