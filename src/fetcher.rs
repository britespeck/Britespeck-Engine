use serde_json::Value;
use uuid::Uuid;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Mutex;

use crate::models::{PredictionEvent, MarketOutcome};

pub struct MarketFetcher {
    kalshi_image_cache: Mutex<HashMap<String, Option<String>>>,
}

fn categorize_by_title(title: &str) -> Option<&'static str> {
    let t = title.to_lowercase();

    // Gaming (before Sports so esports doesn't fall into Sports)
    if t.contains("esports") || t.contains("e-sports") || t.contains("valorant")
        || t.contains("league of legends") || t.contains("lcs") || t.contains("dota")
        || t.contains("csgo") || t.contains("cs2") || t.contains("overwatch league")
        || t.contains("call of duty league") || t.contains("cdl")
        || t.contains("gaming") || t.contains("twitch") || t.contains("fortnite")
        || t.contains("apex legends") || t.contains("rocket league")
    {
        return Some("Gaming");
    }

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

    // Global (before Politics so geopolitical doesn't fall into Politics)
    if t.contains("war") || t.contains("ukraine") || t.contains("russia")
        || t.contains("china") || t.contains("iran") || t.contains("israel")
        || t.contains("gaza") || t.contains("ceasefire") || t.contains("sanctions")
        || t.contains("united nations") || t.contains("diplomatic")
        || t.contains("geopolit") || t.contains("territorial")
        || t.contains("invasion") || t.contains("missile") || t.contains("nuclear")
        || t.contains("north korea") || t.contains("taiwan")
        || t.contains("middle east") || t.contains("houthi")
    {
        return Some("Global");
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

fn extract_market_volume(market: &Value) -> f64 {
    let dollar_candidates = ["dollar_volume", "volume_24h_fp", "volume_fp"];
    for key in &dollar_candidates {
        if let Some(v) = market.get(key) {
            if let Some(n) = v.as_f64() { if n > 0.0 { return n; } }
            if let Some(n) = v.as_i64() { if n > 0 { return n as f64; } }
        }
    }

    let raw_vol_candidates = ["volume_24h", "volume24h", "volume", "open_interest"];
    let mut raw_vol: f64 = 0.0;
    for key in &raw_vol_candidates {
        if let Some(v) = market.get(key) {
            if let Some(n) = v.as_f64() { if n > 0.0 { raw_vol = n; break; } }
            if let Some(n) = v.as_i64() { if n > 0 { raw_vol = n as f64; break; } }
            if let Some(s) = v.as_str() { if let Ok(n) = s.parse::<f64>() { if n > 0.0 { raw_vol = n; break; } } }
        }
    }

    if raw_vol > 0.0 {
        let price = extract_kalshi_price(market);
        return raw_vol * price;
    }

    0.0
}

fn extract_poly_market_volume(market: &Value) -> f64 {
    let candidates = ["volume24hr", "volume", "volumeNum"];
    for key in &candidates {
        if let Some(v) = market.get(key) {
            if let Some(n) = v.as_f64() { return n; }
            if let Some(s) = v.as_str() {
                if let Ok(n) = s.parse::<f64>() { return n; }
            }
        }
    }
    0.0
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
        let event_url = format!(
            "https://api.elections.kalshi.com/trade-api/v2/events/{}",
            event_ticker
        );
        match client.get(&event_url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    if let Ok(json) = resp.json::<Value>().await {
                        let event_obj = json.get("event").unwrap_or(&json);
                        let img_keys = [
                            "image_url",
                            "featured_image_url",
                            "thumbnail_url",
                            "category_image_url",
                            "og_image_url",
                        ];
                        for key in &img_keys {
                            if let Some(url) = event_obj.get(key)
                                .and_then(|v| v.as_str())
                                .filter(|s| !s.is_empty() && s.starts_with("http"))
                            {
                                return Some(url.to_string());
                            }
                        }
                        if let Some(markets) = event_obj.get("markets").and_then(|m| m.as_array()) {
                            for m in markets {
                                if let Some(url) = m.get("image_url")
                                    .and_then(|v| v.as_str())
                                    .filter(|s| !s.is_empty() && s.starts_with("http"))
                                {
                                    return Some(url.to_string());
                                }
                            }
                        }
                    }
                }
            }
            Err(_) => {}
        }

        let series_ticker = event_ticker
            .split('-')
            .next()
            .unwrap_or(event_ticker)
            .to_lowercase();

        let series_url = format!(
            "https://api.elections.kalshi.com/trade-api/v2/series/{}",
            series_ticker
        );
        match client.get(&series_url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    if let Ok(json) = resp.json::<Value>().await {
                        let series_obj = json.get("series").unwrap_or(&json);
                        let img_keys = ["image_url", "category_image_url"];
                        for key in &img_keys {
                            if let Some(url) = series_obj.get(key)
                                .and_then(|v| v.as_str())
                                .filter(|s| !s.is_empty() && s.starts_with("http"))
                            {
                                return Some(url.to_string());
                            }
                        }
                    }
                }
            }
            Err(_) => {}
        }

        None
    }

    async fn get_kalshi_image(
        &self,
        client: &reqwest::Client,
        event_ticker: &str,
        event_level_image: &Option<String>,
    ) -> Option<String> {
        if let Some(img) = event_level_image {
            if !img.is_empty() {
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

    pub async fn fetch_all(
        &self,
        kalshi_client: &reqwest::Client,
        poly_client: &reqwest::Client,
    ) -> Vec<PredictionEvent> {
        let mut unified: Vec<PredictionEvent> = Vec::new();

        // ─── KALSHI ───
        let mut kalshi_cursor: Option<String> = None;
        let kalshi_limit = 200;
        let mut kalshi_retries = 0;
        let max_kalshi_retries = 3;

        loop {
            let mut url = format!(
                "https://api.elections.kalshi.com/trade-api/v2/events?limit={}&status=open&with_nested_markets=true",
                kalshi_limit
            );
            if let Some(ref cursor) = kalshi_cursor {
                url.push_str(&format!("&cursor={}", cursor));
            }

            match kalshi_client.get(&url).send().await {
                Ok(resp) => {
                    let status = resp.status();

                    if status == 429 || status.is_server_error() {
                        kalshi_retries += 1;
                        if kalshi_retries > max_kalshi_retries {
                            println!("⚠️ Kalshi rate-limited after {} retries, moving on", max_kalshi_retries);
                            break;
                        }
                        let wait = 2u64.pow(kalshi_retries);
                        println!("⏳ Kalshi {} — backing off {}s (attempt {}/{})", status, wait, kalshi_retries, max_kalshi_retries);
                        tokio::time::sleep(std::time::Duration::from_secs(wait)).await;
                        continue;
                    }

                    let body = match resp.text().await {
                        Ok(b) => b,
                        Err(e) => {
                            println!("❌ Kalshi body read error: {}", e);
                            break;
                        }
                    };

                    if body.trim().is_empty() {
                        kalshi_retries += 1;
                        if kalshi_retries > max_kalshi_retries {
                            println!("⚠️ Kalshi returned empty body after {} retries", max_kalshi_retries);
                            break;
                        }
                        let wait = 2u64.pow(kalshi_retries);
                        println!("⏳ Kalshi empty response — retrying in {}s (attempt {}/{})", wait, kalshi_retries, max_kalshi_retries);
                        tokio::time::sleep(std::time::Duration::from_secs(wait)).await;
                        continue;
                    }

                    let json: Value = match serde_json::from_str(&body) {
                        Ok(v) => v,
                        Err(e) => {
                            println!("❌ Kalshi JSON parse error: {} (body len={})", e, body.len());
                            kalshi_retries += 1;
                            if kalshi_retries > max_kalshi_retries { break; }
                            let wait = 2u64.pow(kalshi_retries);
                            tokio::time::sleep(std::time::Duration::from_secs(wait)).await;
                            continue;
                        }
                    };

                    kalshi_retries = 0;

                    let events = json
                        .get("events")
                        .and_then(|e| e.as_array())
                        .cloned()
                        .unwrap_or_default();

                    if events.is_empty() { break; }

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
                            &["image_url", "thumbnail_url", "series_image_url", "category_image_url", "og_image_url"],
                        );
                        let icon = self.get_kalshi_image(kalshi_client, &ticker, &event_image).await;

                        let nested_markets = event
                            .get("markets")
                            .and_then(|m| m.as_array())
                            .cloned()
                            .unwrap_or_default();

                        let active_markets: Vec<Value> = nested_markets
                            .into_iter()
                            .filter(|m| {
                                m.get("status")
                                    .and_then(|s| s.as_str())
                                    .map(|s| s == "active" || s == "open")
                                    .unwrap_or(true)
                            })
                            .collect();

                        let total_volume: f64 = active_markets.iter().map(|m| extract_market_volume(m)).sum();

                        if active_markets.is_empty() { continue; }

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

                        let outcomes: Vec<MarketOutcome> = active_markets
                            .iter()
                            .take(5)
                            .map(|m| {
                                let name = m
                                    .get("title")
                                    .or_else(|| m.get("subtitle"))
                                    .or_else(|| m.get("yes_sub_title"))
                                    .and_then(|t| t.as_str())
                                    .unwrap_or("Yes")
                                    .to_string();
                                let price = extract_kalshi_price(m);
                                let volume = extract_market_volume(m);
                                MarketOutcome { name, price, volume }
                            })
                            .collect();

                        let outcomes = if outcomes.is_empty() {
                            vec![MarketOutcome {
                                name: "Yes".to_string(),
                                price: odds,
                                volume: total_volume,
                            }]
                        } else {
                            outcomes
                        };

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
                                    "gaming" | "esports" => "Gaming".to_string(),
                                    _ => "Social".to_string(),
                                }
                            });

                        let end_date: Option<DateTime<Utc>> = event
                            .get("close_time")
                            .or_else(|| event.get("expected_expiration_time"))
                            .and_then(|v| v.as_str())
                            .and_then(|s| parse_datetime(s));

                        let market_url = Some(format!(
                            "https://kalshi.com/markets/{}",
                            ticker.split('-').next().unwrap_or(&ticker).to_lowercase()
                        ));

                        unified.push(PredictionEvent {
                            id: Uuid::new_v4(),
                            title,
                            platform: "Kalshi".to_string(),
                            odds,
                            category,
                            external_id: ticker.clone(),
                            updated_at: Utc::now(),
                            volume_24h: total_volume,
                            icon_url: icon,
                            outcomes,
                            status: "active".to_string(),
                            end_date,
                            market_url,
                        });
                    }

                    kalshi_cursor = json
                        .get("cursor")
                        .and_then(|c| c.as_str())
                        .map(|s| s.to_string())
                        .filter(|s| !s.is_empty());

                    if kalshi_cursor.is_none() { break; }

                    println!("📡 Kalshi page fetched: {} events so far...", unified.len());
                }
                Err(e) => {
                    println!("❌ Kalshi connection failed: {}", e);
                    kalshi_retries += 1;
                    if kalshi_retries > max_kalshi_retries { break; }
                    let wait = 2u64.pow(kalshi_retries);
                    println!("⏳ Retrying Kalshi in {}s...", wait);
                    tokio::time::sleep(std::time::Duration::from_secs(wait)).await;
                    continue;
                }
            }
        }

        let kalshi_count = unified.len();
        println!("✅ Kalshi events collected: {}", kalshi_count);

        // ─── POLYMARKET ───
        let poly_limit = 100;
        let mut poly_offset = 0;
        let poly_max = 2000;
        let mut poly_total = 0;

        loop {
            if poly_offset >= poly_max { break; }

            let url = format!(
                "https://gamma-api.polymarket.com/events?closed=false&limit={}&offset={}&order=volume24hr&ascending=false",
                poly_limit, poly_offset
            );

            match poly_client.get(&url).send().await {
                Ok(resp) => {
                    match resp.json::<Value>().await {
                        Ok(json) => {
                            let events = match json.as_array() {
                                Some(arr) => arr.clone(),
                                None => { break; }
                            };

                            if events.is_empty() { break; }

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

                                let slug = event
                                    .get("slug")
                                    .and_then(|s| s.as_str())
                                    .map(|s| s.to_string());

                                let market_url = slug
                                    .as_ref()
                                    .map(|s| format!("https://polymarket.com/event/{}", s))
                                    .or_else(|| Some(format!(
                                        "https://polymarket.com/markets?query={}",
                                        urlencoding::encode(&title)
                                    )));

                                let end_date: Option<DateTime<Utc>> = event
                                    .get("endDate")
                                    .or_else(|| event.get("end_date"))
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| parse_datetime(s));

                                let event_volume: f64 = event
                                    .get("volume")
                                    .and_then(|v| {
                                        v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                                    })
                                    .unwrap_or(0.0);

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

                                if active_markets.is_empty() { continue; }

                                let total_vol = if event_volume > 0.0 {
                                    event_volume
                                } else {
                                    active_markets.iter().map(|m| extract_poly_market_volume(m)).sum()
                                };

                                let best_market = active_markets
                                    .iter()
                                    .max_by(|a, b| {
                                        let va = extract_poly_market_volume(a);
                                        let vb = extract_poly_market_volume(b);
                                        va.partial_cmp(&vb).unwrap()
                                    })
                                    .unwrap();

                                let odds = best_market
                                    .get("outcomePrices")
                                    .and_then(|op| op.as_str())
                                    .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
                                    .and_then(|prices| prices.first().and_then(|p| p.parse::<f64>().ok()))
                                    .unwrap_or(0.5);

                                let outcomes: Vec<MarketOutcome> = active_markets
                                    .iter()
                                    .take(5)
                                    .map(|m| {
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
                                        let volume = extract_poly_market_volume(m);
                                        MarketOutcome { name, price, volume }
                                    })
                                    .collect();

                                let outcomes = if outcomes.is_empty() {
                                    vec![MarketOutcome {
                                        name: "Yes".to_string(),
                                        price: odds,
                                        volume: total_vol,
                                    }]
                                } else {
                                    outcomes
                                };

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
                                        if all_tags.contains("esport") || all_tags.contains("gaming") {
                                            "Gaming".to_string()
                                        } else if all_tags.contains("politic") || all_tags.contains("election") {
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
                                        } else if all_tags.contains("geopolit") || all_tags.contains("war") || all_tags.contains("global") {
                                            "Global".to_string()
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
                                    market_url,
                                });

                                poly_total += 1;
                            }

                            if events.len() < poly_limit as usize { break; }

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