use serde::{Deserialize, Serialize};
use uuid::Uuid;
use chrono::{DateTime, Utc};
 
#[derive(Debug, Serialize, Deserialize, Clone, sqlx::FromRow)]
pub struct MarketOutcome {
    pub name: String,
    pub price: f64,
    pub volume: f64,
    // Rich Kalshi fields — None for Polymarket
    pub yes_bid: Option<f64>,
    pub yes_ask: Option<f64>,
    pub no_bid: Option<f64>,
    pub no_ask: Option<f64>,
    pub open_interest: Option<f64>,
    pub volume_24h: Option<f64>,
    pub yes_sub_title: Option<String>,
    pub no_sub_title: Option<String>,
}
 
#[derive(Debug, Serialize, Deserialize, Clone, sqlx::FromRow)]
pub struct PredictionEvent {
    pub id: Uuid,
    pub title: String,
    pub platform: String,
    pub odds: f64,
    pub category: String,
    pub external_id: String,
    pub volume_24h: f64,
    pub open_interest: f64,
    pub icon_url: Option<String>,
    pub updated_at: DateTime<Utc>,
    pub status: String,
    pub end_date: Option<DateTime<Utc>>,
    pub outcomes: Vec<MarketOutcome>, // JSONB column
    pub market_url: Option<String>,
    /// Polymarket CLOB token id used by the trade ingestion loop.
    /// None for Kalshi / unknown markets.
    pub clob_token_yes: Option<String>,
}