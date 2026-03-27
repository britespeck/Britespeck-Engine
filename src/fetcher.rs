use crate::models::{PredictionEvent, MarketOutcome};

use chrono::Utc;
use uuid::Uuid;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;


pub struct MarketFetcher {
    kalshi_image_cache: Mutex<HashMap<String, Option<String>>>,
}


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


async fn scrape_kalshi_og_image(client: &reqwest::Client, event_ticker: &str) -> Option<String> {
    let page_url = format!("https://kalshi.com/markets/{}", event_ticker.to_lowercase());
    let resp = client.get(&page_url).send().await.ok()?;
    if !resp.status().is_success() { return None; }
    let html = resp.text().await.ok()?;

    let marker = "og:image";
    let pos = html.find(marker)?;
    let after = &html[pos..];
    let content_start = after.find("content=\"")? + 9;
    let content_slice = &after[content_start..];
    let content_end = content_slice.find('"')?;
    let url = &content_slice[..content_end];

    if url.starts_with("http") { Some(url.to_string()) } else { None }
}


impl MarketFetcher {
    pub fn new() -> Self {
        Self {
            kalshi_image_cache: Mutex::new(HashMap::new()),
        }
    }

    async fn get_kalshi_image(&self, client: &reqwest::Client, ticker: &str) -> Option<String> {
        {
            let cache = self.kalshi_image_cache.lock().unwrap();
            if let Some(cached) = cache.get(ticker) {
                return cached.clone();
            }
        }
        let image = scrape_kalshi_og_image(client, ticker).await;
        {
            let mut cache = self.kalshi_image_cache.lock().unwrap();
            cache.insert(ticker.to_string(), image.clone());
        }
        image
    }

    pub async fn get_unified_events(&self, client: &reqwest::Client) -> Vec<PredictionEvent> {
        println!("🚀 Fetching ALL markets (paginated)...");

        let mut unified = Vec::new();

        // ═══════════════════════════════════════════════════════════
        // 1. KALSHI — Paginate through ALL open events
        // ═══════════════════════════════════════════════════════════
        let kalshi_client = reqwest::Client::builder()
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
            .timeout(Duration::from_secs(25))
            .build()
            .unwrap_or_else(|_| client.clone());

        let mut kalshi_cursor: Option<String> = None;
        let mut kalshi_total = 0u32;

        loop {
            let url = match &kalshi_cursor {
                Some(cursor) => format!(
                    "https://api.elections.kalshi.com/trade-api/v2/events?limit=200&status=open&with_nested_markets=true&cursor={}",
                    cursor
                ),
                None => "https://api.elections.kalshi.com/trade-api/v2/events?limit=200&status=open&with_nested_markets=true".to_string(),
            };

            match kalshi_client.get(&url).send().await {
                Ok(resp) => {
                    if !resp.status().is_success() {
                        println!("❌ Kalshi HTTP {}", resp.status());
                        break;
                    }
                    match resp.json::<Value>().await {
                        Ok(json) => {
                            let events = json.get("events").and_then(|e| e.as_array());
                            let batch_count = events.map(|e| e.len()).unwrap_or(0);

                            if let Some(events) = events {
                                for event in events {
                                    let event_title = event.get("title").and_then(|v| v.as_str()).unwrap_or("Unknown");
                                    let ticker = event.get("event_ticker").and_then(|v| v.as_str()).unwrap_or("");
                                    let api_category = event.get("category").and_then(|v| v.as_str()).unwrap_or("general");

                                    let mut outcomes = Vec::new();
                                    let mut kalshi_volume: f64 = 0.0;

                                    if let Some(markets) = event.get("markets").and_then(|m| m.as_array()) {
                                        for m in markets {
                                            if !is_kalshi_active(m) { continue; }

                                            // Sum volume from each market
                                            kalshi_volume += m.get("volume_24h")
                                                .and_then(|v| v.as_f64())
                                                .or_else(|| m.get("volume").and_then(|v| v.as_f64()))
                                                .unwrap_or(0.0);

                                            let price = m.get("last_price").and_then(|v| v.as_f64()).unwrap_or(50.0) / 100.0;
                                            outcomes.push(MarketOutcome {
                                                name: m.get("title").and_then(|v| v.as_str()).unwrap_or("Outcome").to_string(),
                                                price,
                                            });
                                        }
                                    }

                                    if !outcomes.is_empty() && !ticker.is_empty() {
                                        let mut icon = extract_image(event, &["image_url", "thumbnail_url"]);
                                        if icon.is_none() {
                                            icon = self.get_kalshi_image(&kalshi_client, ticker).await;
                                        }

                                        unified.push(PredictionEvent {
                                            id: Uuid::new_v4(),
                                            title: event_title.to_string(),
                                            platform: "Kalshi".to_string(),
                                            odds: outcomes.first().map(|o| o.price).unwrap_or(0.5),
                                            category: map_kalshi_category(api_category, event_title).to_string(),
                                            external_id: ticker.to_string(),
                                            volume_24h: kalshi_volume,
                                            icon_url: icon,
                                            updated_at: Utc::now(),
                                            status: "active".to_string(),
                                            end_date: parse_end_date(event, &["strike_date", "expiration_time"]),
                                            outcomes,
                                        });
                                    }
                                }
                            }

                            kalshi_total += batch_count as u32;

                            let next_cursor = json.get("cursor").and_then(|v| v.as_str()).map(|s| s.to_string());
                            if batch_count < 200 || next_cursor.is_none() || next_cursor.as_deref() == Some("") {
                                println!("📡 Kalshi total: {} events (done)", kalshi_total);
                                break;
                            }
                            kalshi_cursor = next_cursor;
                            println!("📡 Kalshi page fetched: {} events so far...", kalshi_total);
                        },
                        Err(e) => {
                            println!("❌ Kalshi JSON parse error: {}", e);
                            break;
                        }
                    }
                },
                Err(e) => {
                    println!("❌ Kalshi connection failed: {}", e);
                    break;
                }
            }
        }

        // ═══════════════════════════════════════════════════════════
        // 2. POLYMARKET — Paginate using offset
        // ═══════════════════════════════════════════════════════════
        let mut poly_offset: u32 = 0;
        let mut poly_total: u32 = 0;
        let poly_limit: u32 = 200;

        loop {
            let url = format!(
                "https://gamma-api.polymarket.com/events?limit={}&offset={}&active=true&closed=false",
                poly_limit, poly_offset
            );

            match client.get(&url).send().await {
                Ok(resp) => {
                    if !resp.status().is_success() {
                        println!("❌ Polymarket HTTP {}", resp.status());
                        break;
                    }
                    match resp.json::<Value>().await {
                        Ok(json) => {
                            let events_list = json.as_array().or_else(|| json.get("data").and_then(|d| d.as_array()));
                            let batch_count = events_list.map(|e| e.len()).unwrap_or(0);

                            if let Some(events) = events_list {
                                for event in events {
                                    if !is_poly_active(event) { continue; }

                                    let event_title = event.get("title").and_then(|v| v.as_str()).unwrap_or("Unknown");
                                    let mother_id = event.get("id").and_then(|v| v.as_str()).unwrap_or("");

                                    let tags: Vec<String> = event.get("tags")
                                        .and_then(|v| v.as_array())
                                        .map(|arr| arr.iter().filter_map(|t| t.as_str().map(|s| s.to_string())).collect())
                                        .unwrap_or_default();

                                    let mut outcomes = Vec::new();
                                    let mut total_volume: f64 = 0.0;

                                    if let Some(markets) = event.get("markets").and_then(|m| m.as_array()) {
                                        for m in markets {
                                            let price = m.get("outcomePrices")
                                                .and_then(|p| p.as_str())
                                                .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
                                                .and_then(|arr| arr.first().cloned())
                                                .and_then(|s| s.parse::<f64>().ok())
                                                .unwrap_or(0.0);

                                            let outcome_name = m.get("groupItemTitle")
                                                .or_else(|| m.get("question"))
                                                .and_then(|v| v.as_str())
                                                .unwrap_or("Outcome");

                                            total_volume += m.get("volume24hr")
                                                .and_then(|v| v.as_f64())
                                                .unwrap_or(0.0);

                                            outcomes.push(MarketOutcome {
                                                name: outcome_name.to_string(),
                                                price,
                                            });
                                        }
                                    }

                        