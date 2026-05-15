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
        let kalshi_no = 1.0 - kalshi_yes;
        let poly_no = 1.0 - poly_yes;
        let description = row.kalshi_title.as_deref().unwrap_or("Unknown").to_string();

        if let Some(signal) = check_arb(
            row.kalshi_id.as_deref().unwrap_or(""),
            kalshi_yes, kalshi_no,
            row.poly_id.as_deref().unwrap_or(""),
            poly_yes, poly_no,
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
        .bind(&s.kalshi_ticker).bind(&s.poly_id)
        .bind(&s.description).bind(&s.arb_type)
        .bind(s.yes_price_cents as i32).bind(s.no_price_cents as i32)
        .bind(s.fee_cents as i32).bind(s.profit_cents as i32)
        .bind(s.profit_pct).bind(s.total_cost_cents as i32)
        .bind(s.detected_at)
        .execute(pool).await?;
    }
    Ok(())
}

pub async fn get_active_arb_signals(pool: &PgPool) -> Result<Vec<serde_json::Value>> {
    let rows = sqlx::query!(
        r#"
        SELECT kalshi_id, poly_id, description, arb_type,
               yes_price_cents, no_price_cents, fee_cents,
               profit_cents, profit_pct, total_cost_cents, detected_at
        FROM arb_signals
        WHERE detected_at > NOW() - INTERVAL '15 minutes'
          AND profit_cents > 0
        ORDER BY profit_cents DESC
        LIMIT 50
        "#
    )
    .fetch_all(pool).await?;

    Ok(rows.iter().map(|r| serde_json::json!({
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
    })).collect())
}
