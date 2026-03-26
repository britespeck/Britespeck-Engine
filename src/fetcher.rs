use crate::models::{PredictionEvent, MarketOutcome};
use chrono::Utc;
use uuid::Uuid;
use serde_json::Value;
use std::time::Duration;

pub struct MarketFetcher {}

fn categorize_by_title(title: &str) -> Option<&'static str> {
    let t = title.to_lowercase();
    if t.contains("nba") || t.contains("nfl") || t.contains("mlb") || t.contains("nhl")
        || t.contains("premier league") || t.contains("champions league")
        || t.contains("world cup") || t.contains("super bowl") || t.contains("playoff")
        || t.contains("finals") || t.contains("mvp") || t.contains("masters tournament")
        || t.contains("grand slam") || t.contains("wimbledon") || t.contains("olympics")
        || t.contains("ufc") || t.contains("boxing") || t.contains("f1 ")
        || t.contains("formula 1") || t.contains("pga") || t.contains("golf")
        || t.contains("tennis") || t.contains("soccer") || t.contains("football")
        || t.contains("basketball") || t.contains("baseball") || t.contains("hockey")
    {
        return Some("sports");
    }
    if t.contains("president") || t.contains("election") || t.contains("senate")
        || t.contains("congress") || t.contains("governor") || t.contains("democrat")
        || t.contains("republican") || t.contains("nomination") || t.contains("prime minister")
        || t.contains("parliament") || t.contains("vote") || t.contains("impeach")
        || t.contains("cabinet") || t.contains("political") || t.contains("party")
    {
        return Some("politics");
    }
    if t.contains("bitcoin") || t.contains("ethereum") || t.contains("btc")
        || t.contains("eth") || t.contains("crypto") || t.contains("solana")
        || t.contains("dogecoin") || t.contains("token")
    {
        return Some("crypto");
    }
    if t.contains("stock") || t.contains("s&p") || t.contains("nasdaq") || t.contains("fed ")
        || t.contains("interest rate") || t.contains("inflation") || t.contains("gdp")
        || t.contains("recession") || t.contains("treasury") || t.contains("dow jones")
        || t.contains("market cap") || t.contains("ipo")
    {
        return Some("finance");
    }
    if t.contains("ai ") || t.contains("artificial intelligence") || t.contains("openai")
        || t.contains("google") || t.contains("apple") || t.contains("tesla")
        || t.contains("spacex") || t.contains("launch") || t.contains("tech")
    {
        return Some("tech");
    }
    if t.contains("temperature") || t.contains("hurricane") || t.contains("wildfire")
        || t.contains("climate") || t.contains("weather") || t.contains("earthquake")
    {
        return Some("climate");
    }
    if t.contains("oscar") || t.contains("grammy") || t.contains("emmy")
        || t.contains("celebrity") || t.contains("movie") || t.contains("album")
        || t.contains("streaming") || t.contains("twitter") || t.contains("tiktok")
    {
        return Some("culture");
    }
    None
}

fn map_kalshi_category(api_category: &str, title: &str) -> &'static str {
    let from_api = match api_category.to_lowercase().as_str() {
        "politics"    => Some("politics"),
        "economics" | "economy" | "financials" | "companies" => Some("finance"),
        "sports"      => Some("sports"),
        "culture"     => Some("culture"),
        "crypto"      => Some("crypto"),
        "climate" | "weather" => Some("climate"),
        "tech" | "science" => Some("tech"),
        _             => None,
    };
    from_api.or_else(|| categorize_by_title(title)).unwrap_or("global")
}

fn map_polymarket_category(tags: &[String], title: &str) -> &'static str {
    let tag = tags.first().map(|t| t.to_lowercase()).unwrap_or_default();
    let from_tag = match tag.as_str() {
        "politics" | "elections" | "geopolitics" => Some("politics"),
        "finance" | "economy" => Some("finance"),
        "sports"      => Some("sports"),
        "culture" | "mentions" => Some("culture"),
        "crypto"      => Some("crypto"),
        "weather" | "climate" => Some("climate"),
        "science" | "tech" => Some("tech"),
        _             => None,
    };
    from_tag.or_else(|| categorize_by_title(title)).unwrap_or("global")
}

fn extract_image(obj: &Value, fields: &[&str]) -> Option<String> {
    for field in fields {
        if let Some(url) = obj.get(*field).and_then(|v| v.as_str()) {
            if !url.is_empty() { return Some(url.to_string()); }
        }
    }
    None
}

fn is_kalshi_active(market: &Value) -> bool {
    let status = market.get("status").and_then(|v| v.as_str()).unwrap_or("");
    let result = market.get("result").and_then(|v| v.as_str()).unwrap_or("");
    status != "settled" && status != "closed" && result.is_empty()
}

fn is_poly_active(market: &Value) -> bool {
    let closed = market.get("closed").and_then(|v| v.as_bool()).unwrap_or(false);
    let active = market.get("active").and_then(|v| v.as_bool()).unwrap_or(true);
    let resolved = market.get("resolved").and_then(|v| v.as_bool()).unwrap_or(false);
    active && !closed && !resolved
}

fn parse_end_date(obj: &Value, fields: &[&str]) -> Option<chrono::DateTime<Utc>> {
    for field in fields {
        if let Some(s) = obj.get(*field).and_then(|v| v.as_str()) {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) { return Some(dt.with_timezone(&Utc)); }
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") { return Some(dt.and_utc()); }
        }
    }
    None
}

impl MarketFetcher {
    pub fn new() -> Self { Self {} }

    pub async fn get_unified_events(&self, client: &reqwest::Client) -> Vec<PredictionEvent> {
        // 1. SLEEPER - Maintaining low profile on AWS
        println!("⏳ Throttling fetch for 5 minutes...");
        tokio::time::sleep(Duration::from_secs(300)).await;

        let mut unified = Vec::new();

        // 2. KALSHI (PARENT-CHILD LOGIC)
        let k_url = "https://api.kalshi.com";
        if let Ok(resp) = client.get(k_url).send().await {
            if resp.status().is_success() {
                if let Ok(json) = resp.json::<Value>().await {
                    if let Some(events) = json.get("events").and_then(|e| e.as_array()) {
                        println!("📡 DEBUG: Kalshi found {} events", events.len());
                        for event in events {
                            let event_title = event.get("title").and_then(|v| v.as_str()).unwrap_or("Unknown");
                            let ticker = event.get("event_ticker").and_then(|v| v.as_str()).unwrap_or("");
                            
                            let mut outcomes = Vec::new();
                            if let Some(markets) = event.get("markets").and_then(|m| m.as_array()) {
                                for m in markets {
                                    if !is_kalshi_active(m) { continue; }
                                    let price = m.get("last_price").and_then(|v| v.as_f64()).unwrap_or(50.0) / 100.0;
                                    outcomes.push(MarketOutcome {
                                        name: m.get("title").and_then(|v| v.as_str()).unwrap_or("Outcome").to_string(),
                                        price,
                                    });
                                }
                            }
                            
                            if !outcomes.is_empty() && !ticker.is_empty() {
                                unified.push(PredictionEvent {
                                    id: Uuid::new_v4(),
                                    title: event_title.to_string(),
                                    platform: "Kalshi".to_string(),
                                    odds: outcomes.first().map(|o| o.price).unwrap_or(0.5),
                                    category: map_kalshi_category("general", event_title).to_string(),
                                    external_id: ticker.to_string(),
                                    volume_24h: 0.0,
                                    icon_url: extract_image(event, &["image_url"]),
                                    updated_at: Utc::now(),
                                    status: "active".to_string(),
                                    end_date: parse_end_date(event, &["strike_date", "expiration_time"]),
                                    outcomes,
                                });
                            }
                        }
                    }
                }
            } else {
                println!("❌ Kalshi API Error: HTTP {}", resp.status());
            }
        }

        // 3. POLYMARKET (PARENT-CHILD LOGIC)
        let p_url = "https://gamma-api.polymarket.com";
        if let Ok(resp) = client.get(p_url).send().await {
            if resp.status().is_success() {
                if let Ok(json) = resp.json::<Value>().await {
                    let events_list = json.as_array().or_else(|| json.get("data").and_then(|d| d.as_array()));
                    if let Some(events) = events_list {
                        println!("📡 DEBUG: Polymarket found {} events", events.len());
                        for event in events {
                            let event_title = event.get("title").and_then(|v| v.as_str()).unwrap_or("Unknown");
                            let mother_id = event.get("id").and_then(|v| v.as_str()).unwrap_or("");
                            
                            let mut outcomes = Vec::new();
                            if let Some(markets) = event.get("markets").and_then(|m| m.as_array()) {
                                for m in markets {
                                    let price = m.get("outcomePrices")
                                        .and_then(|p| p.as_array())
                                        .and_then(|arr| arr.first())
                                        .and_then(|v| v.as_str())
                                        .and_then(|s| s.parse::<f64>().ok())
                                        .unwrap_or(0.0);
                                    
                                    let outcome_name = m.get("groupItemTitle")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("Outcome");

                                    outcomes.push(MarketOutcome { name: outcome_name.to_string(), price });
                                }
                            }

                            if !outcomes.is_empty() && !mother_id.is_empty() {
                                unified.push(PredictionEvent {
                                    id: Uuid::new_v4(),
                                    title: event_title.to_string(),
                                    platform: "Polymarket".to_string(),
                                    odds: outcomes.first().map(|o| o.price).unwrap_or(0.5),
                                    category: map_polymarket_category(&[], event_title).to_string(),
                                    external_id: mother_id.to_string(),
                                    volume_24h: event.get("volume").and_then(|v| v.as_f64()).unwrap_or(0.0),
                                    icon_url: event.get("image").and_then(|v| v.as_str()).map(|s| s.to_string()),
                                    updated_at: Utc::now(),
                                    status: "active".to_string(),
                                    end_date: parse_end_date(event, &["endDate"]),
                                    outcomes,
                                });
                            }
                        }
                    }
                }
            } else {
                println!("❌ Polymarket API Error: HTTP {}", resp.status());
            }
        }

        unified
    }
}