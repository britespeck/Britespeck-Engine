use serde::{Deserialize, Serialize};
use uuid::Uuid;
use chrono::{DateTime, Utc};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MarketOutcome {
    pub name: String,
    pub price: f64,
    pub volume: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PredictionEvent {
    pub id: Uuid,
    pub title: String,
    pub platform: String,
    pub odds: f64,
    pub category: String,
    pub external_id: String,
    pub volume_24h: f64,
    pub icon_url: Option<String>,
    pub updated_at: DateTime<Utc>,
    pub status: String,
    pub end_date: Option<DateTime<Utc>>,
    pub outcomes: Vec<MarketOutcome>,
    pub market_url: Option<String>,
}