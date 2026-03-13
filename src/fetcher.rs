use crate::models::PredictionEvent;
use chrono::{Datelike, Utc};
use uuid::Uuid;
use serde_json::Value;

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
    from_api
        .or_else(|| categorize_by_title(title))
        .unwrap_or("global")
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
    from_tag
        .or_else(|| categorize_by_title(title))
        .unwrap_or("global")
}

fn extract_image(obj: &Value, fields: &[&str]) -> Option<String> {
    for field in fields {
        if let Some(url) = obj.get(*field).and_then(|v| v.as_str()) {
            if !url.is_empty() {
                return Some(url.to_string());
            }
        }
    }
    None
}

fn is_past_event(title: &str) -> bool {
    let current_year = Utc::now().year();
    let re = regex::Regex::new(r"\b(20\d{2})\b").unwrap();
    for cap in re.captures_iter(title) {
        if let Ok(year) = cap[1].parse::<i32>() {
            if year < current_year {
                return true;
            }
        }
    }
    false
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
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                return Some(dt.with_timezone(&Utc));
            }
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                return Some(dt.and_utc());
            }
        }
    }
    None
}

impl MarketFetcher {
    pub fn new() -> Self { Self {} }

    pub async fn get_unified_events(&self, client: &reqwest::Client) -> Vec<PredictionEvent> {
        let mut unified = Vec::new();
        let mut images_found = 0u32;
        let mut skipped = 0u32;

        // ═══════════════════════════════════════════
        // KALSHI
        // ═══════════════════════════════════════════
        let k_url = "https://api.elections.kalshi.com/trade-api/v2/events?status=open&with_nested_markets=true";
        match client.get(k_url).send().await {
            Ok(resp) => {
                println!("📡 Kalshi status: {}", resp.status());
                if resp.status().is_success() {
                    if let Ok(json) = resp.json::<Value>().await {
                        if let Some(events) = json.get("events").and_then(|e| e.as_array()) {
                            println!("   ✅ Kalshi events found: {}", events.len());

                            for event in events {
                                let raw_cat = event.get("category").and_then(|v| v.as_str()).unwrap_or("general");
                                let icon = extract_image(event, &["image_url", "imageUrl", "image", "thumbnail"]);
                                if icon.is_some() { images_found += 1; }

                                if let Some(markets) = event.get("markets").and_then(|m| m.as_array()) {
                                    if let Some(m) = markets.first() {
                                        let title = m.get("title").and_then(|v| v.as_str()).unwrap_or("Unknown");
                                        let category = map_kalshi_category(raw_cat, title);
                                        let odds = m.get("last_price").and_then(|v| v.as_f64()).unwrap_or(50.0) / 100.0;
                                        let end_date = parse_end_date(m, &["close_time", "expiration_time", "end_date"]);

                                        if !is_kalshi_active(m) || is_past_event(title) || odds == 0.0 || odds == 1.0 {
                                            skipped += 1;
                                            continue;
                                        }
                                        if let Some(ed) = end_date {
                                            if ed < Utc::now() {
                                                skipped += 1;
                                                continue;
                                            }
                                        }

                                        unified.push(PredictionEvent {
                                            id: Uuid::new_v4(),
                                            title: title.to_string(),
                                            platform: "kalshi".to_string(),
                                            odds,
                                            category: category.to_string(),
                                            external_id: format!("kalshi-{}", m.get("ticker").and_then(|v| v.as_str()).unwrap_or("0")),
                                            volume_24h: m.get("volume_24h").and_then(|v| v.as_f64()).unwrap_or(0.0),
                                            icon_url: icon.clone(),
                                            updated_at: Utc::now(),
                                            status: "active".to_string(),
                                            end_date,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => println!("❌ Kalshi request failed: {}", e),
        }

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // ═══════════════════════════════════════════
        // POLYMARKET
        // ═══════════════════════════════════════════
        let p_url = "https://gamma-api.polymarket.com/events?active=true&closed=false&limit=100";
        match client.get(p_url).send().await {
            Ok(resp) => {
                println!("📡 Polymarket status: {}", resp.status());
                if resp.status().is_success() {
                    if let Ok(json) = resp.json::<Value>().await {
                        let events_list = json.as_array()
                            .or_else(|| json.get("events").and_then(|e| e.as_array()));

                        if let Some(events) = events_list {
                            println!("   ✅ Polymarket events found: {}", events.len());

                            for event in events {
                                let tags: Vec<String> = event.get("tags")
                                    .and_then(|t| t.as_array())
                                    .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                                    .unwrap_or_default();
                                let event_title = event.get("title").and_then(|v| v.as_str()).unwrap_or("");
                                let category = map_polymarket_category(&tags, event_title);
                                let icon = extract_image(event, &["imageOptimized", "image", "imageUrl", "icon", "thumbnail"]);
                                if icon.is_some() { images_found += 1; }

                                if let Some(markets) = event.get("markets").and_then(|m| m.as_array()) {
                                    for m in markets {
                                        let title = m.get("question").and_then(|v| v.as_str()).unwrap_or("Unknown");
                                        let prices_str = m.get("outcomePrices").and_then(|v| v.as_str()).unwrap_or("[\"0.5\"]");
                                        let prices: Vec<String> = serde_json::from_str(prices_str).unwrap_or_default();
                                        let odds = prices.get(0).and_then(|p| p.parse::<f64>().ok()).unwrap_or(0.5);
                                        let end_date = parse_end_date(m, &["endDate", "end_date_iso", "expirationDate"]);

                                        if !is_poly_active(m) || is_past_event(title) || odds == 0.0 || odds == 1.0 {
                                            skipped += 1;
                                            continue;
                                        }
                                        if let Some(ed) = end_date {
                                            if ed < Utc::now() {
                                                skipped += 1;
                                                continue;
                                            }
                                        }

                                        unified.push(PredictionEvent {
                                            id: Uuid::new_v4(),
                                            title: title.to_string(),
                                            platform: "polymarket".to_string(),
                                            odds,
                                            category: category.to_string(),
                                            external_id: format!("poly-{}", m.get("id").unwrap_or(&Value::Null)),
                                            volume_24h: m.get("volume24hr").and_then(|v| v.as_f64()).unwrap_or(0.0),
                                            icon_url: icon.clone(),
                                            updated_at: Utc::now(),
                                            status: "active".to_string(),
                                            end_date,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => println!("❌ Polymarket request failed: {}", e),
        }

        println!("🖼️ Total events with images: {}/{}", images_found, unified.len());
        println!("🚫 Skipped resolved/expired/past: {}", skipped);
        unified
    }
}