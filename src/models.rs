use serde::{Deserialize, Serialize};
use uuid::Uuid;
use chrono::{DateTime, Utc};

#[derive(Debug, Serialize, Deserialize, Clone, sqlx::FromRow)]
pub struct MarketOutcome {
    pub name: String,
    pub price: f64,
    pub volume: f64,
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
    /// Polymarket CLOB token id (0x-prefixed hex) used by the trade
    /// ingestion loop. None for Kalshi / unknown markets.
    pub clob_token_yes: Option<String>,
}
