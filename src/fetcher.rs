use crate::models::{PredictionEvent, MarketOutcome};

use chrono::{DateTime, Utc};
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
        || t.contains("premier league") || t.contains("uefa") || t.contains("soccer")
        || t.contains("tennis") || t.contains("f1") || t.contains("nascar")
        || t.contains("boxing") || t.contains("ufc") || t.contains("mma")
        || t.contains("golf") || t.contains("pga") || t.contains("march madness")
        || t.contains("ncaa") || t.contains("draft") || t.contains("super bowl")
        || t.contains("world series") || t.contains("stanley cup")
        || t.contains("championship") || t.contains("playoff")
        || t.contains("wnba") || t.contains("mls")
    {
        return Some("Sports");
    }
    if t.contains("president") || t.contains("trump") || t.contains("biden")
        || t.contains("congress") || t.contains("senate") || t.contains("election")
        || t.contains("republican") || t.contains("democrat") || t.contains("governor")
        || t.contains("mayor") || t.contains("supreme court") || t.contains("political")
        || t.contains("legislation") || t.contains("impeach") || t.contains("veto")
        || t.contains("executive order") || t.contains("cabinet")
        || t.contains("parliament") || t.contains("nato")
    {
        return Some("Politics");
    }
    if t.contains("fed") || t.contains("interest rate") || t.contains("inflation")
        || t.contains("gdp") || t.contains("recession") || t.contains("s&p")
        || t.contains("stock") || t.contains("nasdaq") || t.contains("dow")
        || t.contains("treasury") || t.contains("unemployment") || t.contains("tariff")
        || t.contains("trade war") || t.contains("cpi") || t.contains("fomc")
        || t.contains("economic") || t.contains("jobs report")
    {
        return Some("Economics");
    }
    if t.contains("bitcoin") || t.contains("ethereum") || t.contains("crypto")
        || t.contains("btc") || t.contains("eth") || t.contains("solana")
        || t.contains("defi") || t.contains("nft") || t.contains("blockchain")
        || t.contains("stablecoin") || t.contains("altcoin")
    {
        return Some("Crypto");
    }
    if t.contains("ai") || t.contains("artificial intelligence") || t.contains("openai")
        || t.contains("google") || t.contains("apple") || t.contains("tesla")
        || t.contains("spacex") || t.contains("meta") || t.contains("microsoft")
        || t.contains("amazon") || t.contains("nvidia") || t.contains("robot")
        || t.contains("quantum") || t.contains("chip") || t.contains("semiconductor")
        || t.contains("llm") || t.contains("gpt")
    {
        return Some("Tech");
    }
    if t.contains("hurricane") || t.contains("earthquake") || t.contains("temperature")
        || t.contains("weather") || t.contains("climate") || t.contains("wildfire")
        || t.contains("flood") || t.contains("tornado") || t.contains("storm")
        || t.contains("drought") || t.contains("el nino") || t.contains("la nina")
    {
        return Some("Weather");
    }
    if t.contains("oscar") || t.contains("grammy") || t.contains("emmy")
        || t.contains("movie") || t.contains("film") || t.contains("album")
        || t.contains("tiktok") || t.contains("twitter") || t.contains("viral")
        || t.contains("celebrity") || t.contains("netflix") || t.contains("spotify")
        || t.contains("youtube") || t.contains("streaming") || t.contains("box office")
        || t.contains("met gala") || t.contains("reality tv") || t.contains("influencer")
        || t.contains("measles") || t.contains("pandemic") || t.contains("vaccine")
        || t.contains("health") || t.contains("fda") || t.contains("cdc")
    {
        return Some("Social");
    }
    None
}

fn extract_image(value: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(v) = value.get(key).and_then(|v| v.as_str()) {
            if !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

/// Extract volume from a FULL market object.
/// Kalshi uses string-formatted fields like "volume_24h_fp": "1234.00"
fn extract_market_volume(market: &Value) -> f64 {
    let candidates = [
        "volume_24h_fp", "volume_fp", "dollar_volume",
        "volume_24h", "volume24h", "volume",
        "open_interest_fp", "dollar_open_interest", "open_interest",
    ];
    for key in &candidates {
        if let Some(v) = market.get(key) {
            if let Some(n) = v.as_f64() {
                if n > 0.0 { return n; }
            }
            if let Some(n) = v.as_i64() {
                if n > 0 { return n as f64; }
            }
            if let Some(s) = v.as_str() {
                if let Ok(n) = s.parse::<f64>() {
                    if n > 0.0 { return n; }
                }
            }
        }
    }
    0.0
}

/// Extract a price from a Kalshi market object.
/// Kalshi uses string-formatted dollar fields like "last_price_dollars": "0.65"
fn extract_kalshi_price(market: &Value) -> f64 {
    let candidates = [
        "last_price_dollars", "yes_bid_dollars", "yes_ask_dollars", "yes_price",
    ];
    for key in &candidates {
        if let Some(v) = market.get(key) {
            if let Some(n) = v.as_f64() {
                if n > 0.0 { return n; }
            }
            if let Some(s) = v.as_str() {
                if let Ok(n) = s.parse::<f64>() {
                    if n > 0.0 { return n; }
                }
            }
        }
    }
    0.5
}

fn parse_datetime(s: &str) -> Option<DateTime<Utc>> {
    s.parse::<DateTime<Utc>>().ok()
}

impl MarketFetcher {
    pub fn new() -> Self {
        Self {
            kalshi_image_cache: Mutex::new(HashMap::new()),
        }
    }

    async fn fetch_kalshi_series_image(
        client: &reqwest::Client,
        event_ticker: &str,
    ) -> Option<String> {
        let series_ticker = event_ticker
            .split('-')
            .next()
            .unwrap_or(event_ticker)
            .to_lowercase();
        let url = format!(
            "https://api.elections.kalshi.com/trade-api/v2/series/{}",
            series_ticker
        );
        match client.get(&url).send().await {
            Ok(resp) => {
                if !resp.status().is_success() { return None; }
                match resp.json::<Value>().await {
                    Ok(json) => {
                        let img = json
                            .get("series")
                            .and_then(|s| s.get("image_url"))
                            .and_then(|v| v.as_str())
                            .filter(|s| !s.is_empty())
                            .map(|s| s.to_string());
                        println!("🖼️ Kalshi series API for {}: {:?}", series_ticker, img);
                        img
                    }
                    Err(_) => None,
                }
            }
            Err(_) => None,
        }
    }

    async fn get_kalshi_image(
        &self,
        client: &reqwest::Client,
        event_ticker: &str,
        event_level_image: &Option<String>,
    ) -> Option<String> {
        if let Some(img) = event_level_image {
            if !img.is_empty() {
                println!("🖼️ Kalshi {} icon from event API: {}", event_ticker, img);
                return Some(img.clone());
            }
        }
        {
            let cache = self.kalshi_image_cache.lock().unwrap();
            if let Some(cached) = cache.get(event_ticker) {
                return cached.clone();
            }
        }
        let result = Self::fetch_kalshi_series_image(client, event_ticker).await;
        {
            let mut cache = self.kalshi_image_cache.lock().unwrap();
            cache.insert(event_ticker.to_string(), result.clone());
        }
        result
    }

    async fn fetch_kalshi_market_volume(
        client: &reqwest::Client,
        event_ticker: &str,
    ) -> f64 {
        let url = format!(
            "https://api.elections.kalshi.com/trade-api/v2/markets?event_ticker={}&limit=50",
            event_ticker
        );
        match client.get(&url).send().await {
            Ok(resp) => {
                if !resp.status().is_success() {
                    println!("⚠️ Kalshi markets endpoint {} returned {}", event_ticker, resp.status());
                    return 0.0;
                }
                match resp.json::<Value>().await {
                    Ok(json) => {
                        let markets = json
                            .get("markets")
                            .and_then(|m| m.as_array())
                            .cloned()
                            .unwrap_or_default();

                        let total_vol: f64 = markets
                            .iter()
                            .filter(|m| {
                                m.get("status")
                                    .and_then(|s| s.as_str())
                                    .map(|s| s == "active" || s == "open")
                                    .unwrap_or(true)
                            })
                            .map(|m| extract_market_volume(m))
                            .sum();

                        println!("💰 Kalshi {} vol from /markets: {}", event_ticker, total_vol);
                        total_vol
                    }
                    Err(e) => {
                        println!("❌ Kalshi markets JSON parse error for {}: {}", event_ticker, e);
                        0.0
                    }
                }
            }
            Err(e) => {
                println!("❌ Kalshi markets fetch error for {}: {}", event_ticker, e);
                0.0
            }
        }
    }

    pub async fn fetch_all(&self, client: &reqwest::Client) -> Vec<PredictionEvent> {
        let mut unified: Vec<PredictionEvent> = Vec::new();

        // ─── KALSHI ───────────────────────────────────────────────
        let mut kalshi_cursor: Option<String> = None;
        let kalshi_limit = 200;

        loop {
            let mut url = format!(
                "https://api.elections.kalshi.com/trade-api/v2/events?limit={}&status=open&with_nested_markets=true",
                kalshi_limit
            );
            if let Some(ref cursor) = kalshi_cursor {
                url.push_str(&format!("&cursor={}", cursor));
            }

            match client.get(&url).send().await {
                Ok(resp) => {
                    match resp.json::<Value>().await {
                        Ok(json) => {
                            let events = json
                                .get("events")
                                .and_then(|e| e.as_array())
                                .cloned()
                                .unwrap_or_default();

                            if events.is_empty() {
                                break;
                            }

                            for event in &events {
                                let ticker = event
                                    .get("event_ticker")
                                    .and_then(|t| t.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                let title = event
                                    .get("title")
                                    .and_then(|t| t.as_str())
                                    .unwrap_or("Unknown")
                                    .to_string();
                                let category_raw = event
                                    .get("category")
                                    .and_then(|c| c.as_str())
                                    .unwrap_or("");

                                let event_image = extract_image(
                                    event,
                                    &["image_url", "thumbnail_url", "series_image_url"],
                                );

                                let icon = self
                                    .get_kalshi_image(&client, &ticker, &event_image)
                                    .await;

                                let volume = Self::fetch_kalshi_market_volume(&client, &ticker).await;

                                let markets = event
                                    .get("markets")
                                    .and_then(|m| m.as_array())
                                    .cloned()
                                    .unwrap_or_default();

                                let active_markets: Vec<&Value> = markets
                                    .iter()
                                    .filter(|m| {
                                        m.get("status")
                                            .and_then(|s| s.as_str())
                                            .map(|s| s == "active" || s == "open")
                                            .unwrap_or(false)
                                    })
                                    .collect();

                                if active_markets.is_empty() {
                                    continue;
                                }

                                // Pick most contested market (closest to 50/50)
                                let best_market = active_markets
                                    .iter()
                                    .min_by(|a, b| {
                                        let pa = extract_kalshi_price(a);
                                        let pb = extract_kalshi_price(b);
                                        let da = (pa - 0.5_f64).abs();
                                        let db = (pb - 0.5_f64).abs();
                                        da.partial_cmp(&db).unwrap()
                                    })
                                    .unwrap();

                                let odds = extract_kalshi_price(best_market);

                                // Build outcomes from top 5 markets
                                let mut outcomes: Vec<MarketOutcome> = active_markets
                                    .iter()
                                    .take(5)
                                    .filter_map(|m| {
                                        let name = m
                                            .get("title")
                                            .or_else(|| m.get("subtitle"))
                                            .or_else(|| m.get("yes_sub_title"))
                                            .and_then(|t| t.as_str())
                                            .unwrap_or("Yes")
                                            .to_string();
                                        let price = extract_kalshi_price(m);
                                        Some(MarketOutcome { name, price })
                                    })
                                    .collect();

                                if outcomes.is_empty() {
                                    outcomes.push(MarketOutcome {
                                        name: "Yes".to_string(),
                                        price: odds,
                                    });
                                }

                                let category = categorize_by_title(&title)
                                    .map(|s| s.to_string())
                                    .unwrap_or_else(|| {
                                        match category_raw.to_lowercase().as_str() {
                                            "politics" => "Politics".to_string(),
                                            "economics" | "finance" => "Economics".to_string(),
                                            "tech" | "science" | "technology" => "Tech".to_string(),
                                            "sports" => "Sports".to_string(),
                                            "crypto" => "Crypto".to_string(),
                                            "climate" | "weather" => "Weather".to_string(),
                                            _ => "Social".to_string(),
                                        }
                                    });

                                let end_date: Option<DateTime<Utc>> = event
                                    .get("close_time")
                                    .or_else(|| event.get("expected_expiration_time"))
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| parse_datetime(s));

                                unified.push(PredictionEvent {
                                    id: Uuid::new_v4(),
                                    title,
                                    platform: "Kalshi".to_string(),
                                    odds,
                                    category,
                                    external_id: ticker.clone(),
                                    updated_at: Utc::now(),
                                    volume_24h: volume,
                                    icon_url: icon,
                                    outcomes,
                                    status: "active".to_string(),
                                    end_date,
                                });
                            }

                            kalshi_cursor = json
                                .get("cursor")
                                .and_then(|c| c.as_str())
                                .map(|s| s.to_string())
                                .filter(|s| !s.is_empty());

                            if kalshi_cursor.is_none() {
                                break;
                            }

                            println!("📡 Kalshi page fetched: {} events so far...", unified.len());
                        }
                        Err(e) => {
                            println!("❌ Kalshi JSON parse error: {}", e);
                            break;
                        }
                    }
                }
                Err(e) => {
                    println!("❌ Kalshi connection failed: {}", e);
                    break;
                }
            }
        }

        let kalshi_count = unified.len();
        println!("✅ Kalshi events collected: {}", kalshi_count);

        // ─── POLYMARKET ───────────────────────────────────────────
        let poly_limit = 100;
        let mut poly_offset = 0;
        let poly_max = 2000;
        let mut poly_total = 0;

        loop {
            if poly_offset >= poly_max {
                break;
            }

            let url = format!(
                "https://gamma-api.polymarket.com/events?closed=false&limit={}&offset={}&order=volume24hr&ascending=false",
                poly_limit, poly_offset
            );

            match client.get(&url).send().await {
                Ok(resp) => {
                    match resp.json::<Value>().await {
                        Ok(json) => {
                            let events = match json.as_array() {
                                Some(arr) => arr.clone(),
                                None => { break; }
                            };

                            if events.is_empty() {
                                break;
                            }

                            for event in &events {
                                let title = event
                                    .get("title")
                                    .and_then(|t| t.as_str())
                                    .unwrap_or("Unknown")
                                    .to_string();
                                let ext_id = event
                                    .get("id")
                                    .and_then(|i| i.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                let icon = event
                                    .get("image")
                                    .or_else(|| event.get("icon"))
                                    .and_then(|i| i.as_str())
                                    .map(|s| s.to_string());

                                let end_date: Option<DateTime<Utc>> = event
                                    .get("endDate")
                                    .or_else(|| event.get("end_date"))
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| parse_datetime(s));

                                let markets = event
                                    .get("markets")
                                    .and_then(|m| m.as_array())
                                    .cloned()
                                    .unwrap_or_default();

                                let active_markets: Vec<&Value> = markets
                                    .iter()
                                    .filter(|m| {
                                        let active = m.get("active").and_then(|a| a.as_bool()).unwrap_or(true);
                                        let closed = m.get("closed").and_then(|c| c.as_bool()).unwrap_or(false);
                                        active && !closed
                                    })
                                    .collect();

                                if active_markets.is_empty() {
                                    continue;
                                }

                                let total_vol: f64 = active_markets
                                    .iter()
                                    .map(|m| {
                                        m.get("volume24hr")
                                            .and_then(|v| v.as_f64())
                                            .or_else(|| {
                                                m.get("volume24hr")
                                                    .and_then(|v| v.as_str())
                                                    .and_then(|s| s.parse::<f64>().ok())
                                            })
                                            .unwrap_or(0.0)
                                    })
                                    .sum();

                                let best_market = active_markets
                                    .iter()
                                    .max_by(|a, b| {
                                        let va = a.get("volume24hr").and_then(|v| v.as_f64()).unwrap_or(0.0);
                                        let vb = b.get("volume24hr").and_then(|v| v.as_f64()).unwrap_or(0.0);
                                        va.partial_cmp(&vb).unwrap()
                                    })
                                    .unwrap();

                                let odds = best_market
                                    .get("outcomePrices")
                                    .and_then(|op| op.as_str())
                                    .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
                                    .and_then(|prices| prices.first().and_then(|p| p.parse::<f64>().ok()))
                                    .unwrap_or(0.5);

                                let mut outcomes: Vec<MarketOutcome> = active_markets
                                    .iter()
                                    .take(5)
                                    .filter_map(|m| {
                                        let name = m
                                            .get("question")
                                            .or_else(|| m.get("groupItemTitle"))
                                            .and_then(|t| t.as_str())
                                            .unwrap_or("Yes")
                                            .to_string();
                                        let price = m
                                            .get("outcomePrices")
                                            .and_then(|op| op.as_str())
                                            .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
                                            .and_then(|prices| prices.first().and_then(|p| p.parse::<f64>().ok()))
                                            .unwrap_or(0.5);
                                        Some(MarketOutcome { name, price })
                                    })
                                    .collect();

                                if outcomes.is_empty() {
                                    outcomes.push(MarketOutcome {
                                        name: "Yes".to_string(),
                                        price: odds,
                                    });
                                }

                                let category = categorize_by_title(&title)
                                    .map(|s| s.to_string())
                                    .unwrap_or_else(|| {
                                        let tags = event
                                            .get("tags")
                                            .and_then(|t| t.as_array())
                                            .cloned()
                                            .unwrap_or_default();
                                        let tag_str: Vec<String> = tags
                                            .iter()
                                            .filter_map(|t| {
                                                t.get("label")
                                                    .or_else(|| t.get("name"))
                                                    .and_then(|v| v.as_str())
                                                    .map(|s| s.to_lowercase())
                                            })
                                            .collect();
                                        let all_tags = tag_str.join(" ");
                                        if all_tags.contains("politic") || all_tags.contains("election") {
                                            "Politics".to_string()
                                        } else if all_tags.contains("crypto") || all_tags.contains("bitcoin") {
                                            "Crypto".to_string()
                                        } else if all_tags.contains("sport") {
                                            "Sports".to_string()
                                        } else if all_tags.contains("tech") || all_tags.contains("ai") {
                                            "Tech".to_string()
                                        } else if all_tags.contains("econ") || all_tags.contains("financ") {
                                            "Economics".to_string()
                                        } else if all_tags.contains("climate") || all_tags.contains("weather") {
                                            "Weather".to_string()
                                        } else {
                                            "Social".to_string()
                                        }
                                    });

                                unified.push(PredictionEvent {
                                    id: Uuid::new_v4(),
                                    title,
                                    platform: "Polymarket".to_string(),
                                    odds,
                                    category,
                                    external_id: ext_id,
                                    updated_at: Utc::now(),
                                    volume_24h: total_vol,
                                    icon_url: icon,
                                    outcomes,
                                    status: "active".to_string(),
                                    end_date,
                                });

                                poly_total += 1;
                            }

                            if events.len() < poly_limit as usize {
                                break;
                            }

                            poly_offset += poly_limit;
                            println!("📡 Polymarket page fetched: {} events so far...", poly_total);
                        }
                        Err(e) => {
                            println!("❌ Polymarket JSON parse error: {}", e);
                            break;
                        }
                    }
                }
                Err(e) => {
                    println!("❌ Polymarket connection failed: {}", e);
                    break;
                }
            }
        }

        println!("✅ Total unified events: {}", unified.len());
        unified
    }
}