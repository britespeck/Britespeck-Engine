//! Arbitrage signal detector for Britespeck.

use anyhow::Result;
use chrono::Utc;
use sqlx::PgPool;
use tracing::info;

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

#[inline]
fn kalshi_fee(price_cents: u16) -> u16 {
    if price_cents > 100 { return 0; }
    KALSHI_FEE_TABLE[price_cents as usize]
}

#[derive(Debug, Clone)]
pub struct ArbSignal {
    pub kalshi_ticker: String,
    pub poly_id: String,
    pub description: String,
    pub arb_type: String,
    pub yes_price_cents: u16,
    pub no_price_cents: u16,
    pub fee_cents: u16,
    pub profit_cents: i16,
    pub profit_pct: f64,
    pub total_cost_cents: u16,
    pub detected_at: chrono::DateTime<Utc>,
}

impl ArbSignal {
    pub fn is_profitable(&self) -> bool {
        self.profit_cents > 0
    }
}

pub fn check_arb(
    kalshi_ticker: &str,
    kalshi_yes_ask: f64,
    kalshi_no_ask: f64,
    poly_id: &str,
    poly_yes_ask: f64,
    poly_no_ask: f64,
    description: &str,
    min_profit_cents: i16,
) -> Option<ArbSignal> {
    let k_yes = (kalshi_yes_ask * 100.0).round() as u16;
    let k_no  = (kalshi_no_ask  * 100.0).round() as u16;
    let p_yes = (poly_yes_ask   * 100.0).round() as u16;
    let p_no  = (poly_no_ask    * 100.0).round() as u16;

    if k_yes == 0 || k_no == 0 || p_yes == 0 || p_no == 0 { return None; }
    if k_yes >= 100 || k_no >= 100 || p_yes >= 100 || p_no >= 100 { return None; }

    let fee1 = kalshi_fee(k_no);
    let cost1 = p_yes + k_no + fee1;
    let profit1 = 100i16 - cost1 as i16;

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

pub async fn run_arb_scan(pool: &PgPool, min_profit_cents: i16) -> Result<Vec<ArbSignal>> {
    Ok(vec![])
}

pub async fn get_active_arb_signals(pool: &PgPool) -> Result<Vec<serde_json::Value>> {
    Ok(vec![])
}
