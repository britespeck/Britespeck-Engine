use serde_json::Value;
use uuid::Uuid;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Mutex;

use crate::models::{PredictionEvent, MarketOutcome};

pub struct MarketFetcher {
    kalshi_image_cache: Mutex<HashMap<String, Option<String>>>,
}

// ─── CATEGORY HEURISTIC (TITLE FALLBACK ONLY) ────────────────────────
fn categorize_by_title(title: &str) -> Option<&'static str> {
    let t = title.to_lowercase();

    // GAMING / ESPORTS
    if t.contains("esports") || t.contains("e-sports")
        || t.contains("valorant") || t.contains("league of legends") || t.contains("lcs")
        || t.contains("dota") || t.contains("csgo") || t.contains("cs2")
        || t.contains("counter-strike") || t.contains("counter strike")
        || t.contains("overwatch league") || t.contains("call of duty league") || t.contains("cdl")
        || t.contains("gaming") || t.contains("twitch") || t.contains("fortnite")
        || t.contains("apex legends") || t.contains("rocket league")
        || t.contains("gta") || t.contains("nintendo") || t.contains("playstation")
        || t.contains("xbox") || t.contains("steam ") || t.contains("video game")
        || t.contains("minecraft") || t.contains("roblox") || t.contains("starcraft")
        || t.contains("hearthstone") || t.contains("the international") || t.contains("worlds 20")
    {
        return Some("Gaming");
    }

    // SPORTS
    if t.contains("nba") || t.contains("nfl") || t.contains("mlb") || t.contains("nhl")
        || t.contains("wnba") || t.contains("mls") || t.contains("premier league")
        || t.contains("uefa") || t.contains("champions league") || t.contains("europa league")
        || t.contains("la liga") || t.contains("bundesliga") || t.contains("serie a")
        || t.contains("ligue 1") || t.contains("soccer") || t.contains("football match")
        || t.contains("tennis") || t.contains("wimbledon") || t.contains("us open")
        || t.contains("french open") || t.contains("australian open") || t.contains("atp")
        || t.contains("wta") || t.contains("f1 ") || t.contains("formula 1") || t.contains("formula one")
        || t.contains("nascar") || t.contains("indycar") || t.contains("motogp")
        || t.contains("boxing") || t.contains("ufc") || t.contains("mma") || t.contains("bellator")
        || t.contains("golf") || t.contains("pga") || t.contains("liv golf") || t.contains("masters tournament")
        || t.contains("march madness") || t.contains("ncaa") || t.contains("nfl draft")
        || t.contains("super bowl") || t.contains("world series") || t.contains("stanley cup")
        || t.contains("world cup") || t.contains("olympic") || t.contains("paralympic")
        || t.contains("championship") || t.contains("playoff") || t.contains("finals")
        || t.contains("cricket") || t.contains("ipl") || t.contains("rugby") || t.contains("six nations")
        || t.contains("kentucky derby") || t.contains("triple crown")
        || t.contains("basketball") || t.contains("baseball") || t.contains("hockey")
    {
        return Some("Sports");
    }

    // GLOBAL / GEOPOLITICS
    if t.contains("war ") || t.contains("ukraine") || t.contains("russia") || t.contains("putin")
        || t.contains("china") || t.contains("xi jinping") || t.contains("iran") || t.contains("israel")
        || t.contains("gaza") || t.contains("hamas") || t.contains("hezbollah") || t.contains("houthi")
        || t.contains("ceasefire") || t.contains("sanctions") || t.contains("united nations") || t.contains("u.n.")
        || t.contains("diplomatic") || t.contains("geopolit") || t.contains("territorial")
        || t.contains("invasion") || t.contains("missile") || t.contains("nuclear")
        || t.contains("north korea") || t.contains("kim jong") || t.contains("taiwan")
        || t.contains("middle east") || t.contains("syria") || t.contains("yemen")
        || t.contains("venezuela") || t.contains("cuba") || t.contains("brexit")
        || t.contains("eu ") || t.contains("european union") || t.contains("nato")
        || t.contains("g7") || t.contains("g20") || t.contains("brics")
        || t.contains("imf") || t.contains("world bank")
    {
        return Some("Global");
    }

    // POLITICS
    if t.contains("president") || t.contains("trump") || t.contains("biden") || t.contains("harris")
        || t.contains("vance") || t.contains("desantis") || t.contains("newsom") || t.contains("mamdani")
        || t.contains("congress") || t.contains("senate") || t.contains("house of representatives")
        || t.contains("election") || t.contains("primary") || t.contains("caucus")
        || t.contains("republican") || t.contains("democrat") || t.contains("gop")
        || t.contains("dnc") || t.contains("rnc")
        || t.contains("governor") || t.contains("mayor") || t.contains("attorney general")
        || t.contains("supreme court") || t.contains("scotus") || t.contains("doj")
        || t.contains("political") || t.contains("legislation") || t.contains("filibuster")
        || t.contains("impeach") || t.contains("indictment") || t.contains("veto")
        || t.contains("executive order") || t.contains("cabinet") || t.contains("parliament")
        || t.contains("prime minister") || t.contains("chancellor") || t.contains("polling")
        || t.contains("ballot") || t.contains("voter") || t.contains("redistricting")
    {
        return Some("Politics");
    }

    // FINANCE / ECONOMICS
    if t.contains("fed ") || t.contains("federal reserve") || t.contains("powell")
        || t.contains("interest rate") || t.contains("rate cut") || t.contains("rate hike")
        || t.contains("inflation") || t.contains("deflation") || t.contains("gdp")
        || t.contains("recession") || t.contains("depression")
        || t.contains("s&p") || t.contains("stock") || t.contains("nasdaq") || t.contains("dow ")
        || t.contains("russell") || t.contains("vix") || t.contains("treasury") || t.contains("yield")
        || t.contains("unemployment") || t.contains("payroll") || t.contains("jobs report")
        || t.contains("tariff") || t.contains("trade war") || t.contains("cpi") || t.contains("ppi") || t.contains("pce")
        || t.contains("fomc") || t.contains("ecb") || t.contains("boj") || t.contains("boe")
        || t.contains("economic") || t.contains("earnings") || t.contains("ipo") || t.contains("merger")
        || t.contains("acquisition") || t.contains("bankrupt") || t.contains("layoff")
        || t.contains("housing market") || t.contains("mortgage") || t.contains("oil price")
        || t.contains("crude") || t.contains("opec") || t.contains("gold price") || t.contains("silver price")
    {
        return Some("Finance");
    }

    // CRYPTO
    if t.contains("bitcoin") || t.contains("btc") || t.contains("ethereum") || t.contains("eth ")
        || t.contains("crypto") || t.contains("solana") || t.contains("sol ")
        || t.contains("xrp") || t.contains("ripple") || t.contains("doge") || t.contains("dogecoin")
        || t.contains("shiba") || t.contains("hype") || t.contains("bnb") || t.contains("binance")
        || t.contains("coinbase") || t.contains("kraken") || t.contains("defi") || t.contains("nft")
        || t.contains("blockchain") || t.contains("stablecoin") || t.contains("usdc") || t.contains("usdt") || t.contains("tether")
        || t.contains("altcoin") || t.contains("memecoin") || t.contains("staking")
        || t.contains("bitcoin etf") || t.contains("ether etf")
        || t.contains("halving") || t.contains("ftx") || t.contains("sbf")
    {
        return Some("Crypto");
    }

    // TECH
    if t.contains(" ai ") || t.contains("artificial intelligence")
        || t.contains("openai") || t.contains("chatgpt") || t.contains("gpt-")
        || t.contains("anthropic") || t.contains("claude") || t.contains("gemini")
        || t.contains("alphabet") || t.contains("apple") || t.contains("iphone")
        || t.contains("tesla") || t.contains("elon musk") || t.contains("spacex") || t.contains("starship")
        || t.contains("meta ") || t.contains("zuckerberg") || t.contains("microsoft") || t.contains("satya")
        || t.contains("amazon") || t.contains("aws") || t.contains("bezos")
        || t.contains("nvidia") || t.contains("amd") || t.contains("intel") || t.contains("tsmc")
        || t.contains("robot") || t.contains("humanoid") || t.contains("quantum")
        || t.contains("chip") || t.contains("semiconductor") || t.contains("llm")
        || t.contains("self-driving") || t.contains("autonomous vehicle") || t.contains("waymo")
        || t.contains("uber") || t.contains("lyft") || t.contains("airbnb")
    {
        return Some("Tech");
    }

    // CLIMATE / WEATHER
    if t.contains("hurricane") || t.contains("typhoon") || t.contains("cyclone")
        || t.contains("earthquake") || t.contains("tsunami") || t.contains("volcano")
        || t.contains("temperature") || t.contains("heatwave") || t.contains("cold snap")
        || t.contains("weather") || t.contains("climate") || t.contains("global warming")
        || t.contains("carbon") || t.contains("emissions") || t.contains("net zero")
        || t.contains("wildfire") || t.contains("flood") || t.contains("tornado")
        || t.contains("storm") || t.contains("blizzard") || t.contains("drought")
        || t.contains("el nino") || t.contains("la nina") || t.contains("snowfall") || t.contains("rainfall")
    {
        return Some("Climate");
    }

    // CULTURE / ENTERTAINMENT
    if t.contains("oscar") || t.contains("academy award") || t.contains("grammy") || t.contains("emmy")
        || t.contains("golden globe") || t.contains("tony award") || t.contains("eurovision")
        || t.contains("movie") || t.contains("film") || t.contains("box office") || t.contains("blockbuster")
        || t.contains("album") || t.contains("song of the year") || t.contains("billboard")
        || t.contains("netflix") || t.contains("spotify") || t.contains("youtube") || t.contains("tiktok")
        || t.contains("disney") || t.contains("hbo") || t.contains("hulu") || t.contains("paramount")
        || t.contains("streaming") || t.contains("celebrity") || t.contains("kardashian") || t.contains("taylor swift")
        || t.contains("beyonce") || t.contains("drake") || t.contains("kanye") || t.contains("rihanna")
        || t.contains("met gala") || t.contains("reality tv") || t.contains("influencer")
        || t.contains("viral") || t.contains("twitter") || t.contains("instagram") || t.contains("threads")
        || t.contains("podcast") || t.contains("rogan")
    {
        return Some("Culture");
    }

    // HEALTH
    if t.contains("measles") || t.contains("pandemic") || t.contains("epidemic")
        || t.contains("vaccine") || t.contains("vaccination") || t.contains("covid")
        || t.contains("h5n1") || t.contains("bird flu") || t.contains("monkeypox") || t.contains("mpox")
        || t.contains("ebola") || t.contains("fda") || t.contains("cdc ") || t.contains("who ")
        || t.contains("ozempic") || t.contains("weight loss drug") || t.contains("cancer trial")
        || t.contains("alzheimer") || t.contains("opioid")
    {
        return Some("Health");
    }

    None
}

// ─── HELPERS ─────────────────────────────────────────────────────────
fn extract_image(value: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(v) = value.get(key).and_then(|v| v.as_str()) {
            if !v.is_empty() && v.starts_with("http") {
                return Some(v.to_string());
            }
        }
    }
    None
}

fn value_as_f64(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_i64().map(|n| n as f64))
        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
}

fn value_as_bool(v: &Value) -> bool {
    v.as_bool()
        .or_else(|| v.as_str().map(|s| s.eq_ignore_ascii_case("true")))
        .unwrap_or(false)
}

/// Kalshi: prefer new "*_dollars" string fields; fall back to legacy cent integers.
fn extract_kalshi_price(market: &Value) -> f64 {
    // New schema (already in dollars: "0.5600")
    for key in &["last_price_dollars", "yes_bid_dollars", "yes_ask_dollars", "yes_price_dollars"] {
        if let Some(v) = market.get(key).and_then(value_as_f64) {
            if v > 0.0 { return v; }
        }
    }
    // Legacy schema (cents 0–100)
    for key in &["yes_price", "last_price", "yes_bid", "yes_ask"] {
        if let Some(v) = market.get(key).and_then(value_as_f64) {
            if v > 0.0 {
                return if v > 1.0 { v / 100.0 } else { v };
            }
        }
    }
    0.5
}

/// Kalshi volume in dollars: prefer pre-computed dollar volume, then `*_fp` (contracts) × price,
/// then legacy `volume_24h` × price.
fn extract_kalshi_market_volume(market: &Value) -> f64 {
    // Pre-computed dollar volume
    for key in &["dollar_volume_24h", "dollar_volume"] {
        if let Some(v) = market.get(key).and_then(value_as_f64) {
            if v > 0.0 { return v; }
        }
    }

    let price = extract_kalshi_price(market);

    // New: *_fp string fields (contracts)
    for key in &["volume_24h_fp", "volume_fp"] {
        if let Some(v) = market.get(key).and_then(value_as_f64) {
            if v > 0.0 { return v * price; }
        }
    }

    // Legacy: integer contracts
    for key in &["volume_24h", "volume24h", "volume"] {
        if let Some(v) = market.get(key).and_then(value_as_f64) {
            if v > 0.0 { return v * price; }
        }
    }

    0.0
}

fn extract_kalshi_open_interest(market: &Value) -> f64 {
    for key in &["open_interest_fp", "open_interest"] {
        if let Some(v) = market.get(key).and_then(value_as_f64) {
            if v > 0.0 { return v; }
        }
    }
    0.0
}

fn extract_poly_market_volume(market: &Value) -> f64 {
    for key in &["volume24hr", "volume24h", "volume", "volumeNum"] {
        if let Some(v) = market.get(key).and_then(value_as_f64) {
            if v > 0.0 { return v; }
        }
    }
    0.0
}

fn extract_poly_price(market: &Value) -> f64 {
    if let Some(v) = market.get("outcomePrices") {
        if let Some(arr) = v.as_array() {
            if let Some(first) = arr.first().and_then(value_as_f64) {
                return if first > 1.0 { first / 100.0 } else { first };
            }
        }
        if let Some(s) = v.as_str() {
            if let Ok(prices) = serde_json::from_str::<Vec<String>>(s) {
                if let Some(first) = prices.first().and_then(|p| p.parse::<f64>().ok()) {
                    return if first > 1.0 { first / 100.0 } else { first };
                }
            }
        }
    }
    for key in &["yesPrice", "price"] {
        if let Some(v) = market.get(key).and_then(value_as_f64) {
            return if v > 1.0 { v / 100.0 } else { v };
        }
    }
    0.5
}

fn parse_datetime(s: &str) -> Option<DateTime<Utc>> {
    s.parse::<DateTime<Utc>>().ok()
}

/// Map Kalshi raw category string → app bucket (platform-first).
fn map_kalshi_category(raw: &str, title: &str) -> String {
    match raw.to_lowercase().as_str() {
        "politics"                                              => "Politics".to_string(),
        "economics" | "finance" | "financials" | "companies"    => "Finance".to_string(),
        "tech" | "science" | "technology"                       => "Tech".to_string(),
        "sports"                                                => "Sports".to_string(),
        "crypto"                                                => "Crypto".to_string(),
        "climate" | "weather"                                   => "Climate".to_string(),
        "gaming" | "esports"                                    => "Gaming".to_string(),
        "culture" | "entertainment"                             => "Culture".to_string(),
        "health"                                                => "Health".to_string(),
        "world" | "global" | "geopolitics"                      => "Global".to_string(),
        _ => categorize_by_title(title)
                .map(|s| s.to_string())
                .unwrap_or_else(|| "Trending".to_string()),
    }
}

/// Map Polymarket tag list → app bucket.
fn map_poly_category(tags: &[Value], title: &str) -> String {
    let all: String = tags.iter()
        .filter_map(|t| t.get("label").or_else(|| t.get("name")).and_then(|v| v.as_str()))
        .map(|s| s.to_lowercase())
        .collect::<Vec<_>>()
        .join(" ");

    if all.contains("esport") || all.contains("gaming")          { return "Gaming".to_string(); }
    if all.contains("politic") || all.contains("election")       { return "Politics".to_string(); }
    if all.contains("crypto") || all.contains("bitcoin")         { return "Crypto".to_string(); }
    if all.contains("sport")                                     { return "Sports".to_string(); }
    if all.contains("tech") || all.contains(" ai")               { return "Tech".to_string(); }
    if all.contains("econ") || all.contains("financ")            { return "Finance".to_string(); }
    if all.contains("climate") || all.contains("weather")        { return "Climate".to_string(); }
    if all.contains("health") || all.contains("medic")           { return "Health".to_string(); }
    if all.contains("culture") || all.contains("entertain")      { return "Culture".to_string(); }
    if all.contains("geopolit") || all.contains("war") || all.contains("global") {
        return "Global".to_string();
    }

    categorize_by_title(title)
        .map(|s| s.to_string())
        .unwrap_or_else(|| "Trending".to_string())
}

impl MarketFetcher {
    pub fn new() -> Self {
        Self {
            kalshi_image_cache: Mutex::new(HashMap::new()),
        }
    }

    async fn fetch_kalshi_series_image(client: &reqwest::Client, event_ticker: &str) -> Option<String> {
        let event_url = format!(
            "https://api.elections.kalshi.com/trade-api/v2/events/{}",
            event_ticker
        );

        if let Ok(resp) = client.get(&event_url).send().await {
            if resp.status().is_success() {
                if let Ok(json) = resp.json::<Value>().await {
                    let event_obj = json.get("event").unwrap_or(&json);
                    let img_keys = ["image_url", "featured_image_url", "thumbnail_url",
                                    "category_image_url", "og_image_url"];
                    for key in &img_keys {
                        if let Some(url) = event_obj.get(key).and_then(|v| v.as_str())
                            .filter(|s| !s.is_empty() && s.starts_with("http"))
                        {
                            return Some(url.to_string());
                        }
                    }
                    if let Some(markets) = event_obj.get("markets").and_then(|m| m.as_array()) {
                        for m in markets {
                            if let Some(url) = m.get("image_url").and_then(|v| v.as_str())
                                .filter(|s| !s.is_empty() && s.starts_with("http"))
                            {
                                return Some(url.to_string());
                            }
                        }
                    }
                }
            }
        }

        let series_ticker = event_ticker.split('-').next().unwrap_or(event_ticker).to_lowercase();
        let series_url = format!(
            "https://api.elections.kalshi.com/trade-api/v2/series/{}",
            series_ticker
        );
        if let Ok(resp) = client.get(&series_url).send().await {
            if resp.status().is_success() {
                if let Ok(json) = resp.json::<Value>().await {
                    let series_obj = json.get("series").unwrap_or(&json);
                    for key in &["image_url", "category_image_url"] {
                        if let Some(url) = series_obj.get(key).and_then(|v| v.as_str())
                            .filter(|s| !s.is_empty() && s.starts_with("http"))
                        {
                            return Some(url.to_string());
                        }
                    }
                }
            }
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
            if !img.is_empty() { return Some(img.clone()); }
        }
        {
            let cache = self.kalshi_image_cache.lock().unwrap();
            if let Some(cached) = cache.get(event_ticker) { return cached.clone(); }
        }
        let result = Self::fetch_kalshi_series_image(client, event_ticker).await;
        {
            let mut cache = self.kalshi_image_cache.lock().unwrap();
            cache.insert(event_ticker.to_string(), result.clone());
        }
        result
    }

    /// NOTE: Build reqwest clients in main.rs with:
    ///   reqwest::Client::builder()
    ///     .user_agent("britespeck-engine/1.0")
    ///     .timeout(std::time::Duration::from_secs(15))
    ///     .build()?
    pub async fn fetch_all(
        &self,
        kalshi_client: &reqwest::Client,
        poly_client: &reqwest::Client,
    ) -> Vec<PredictionEvent> {
        // ─── KALSHI ──────────────────────────────────────────────────
        let mut kalshi_map: HashMap<String, PredictionEvent> = HashMap::new();
        let mut kalshi_cursor: Option<String> = None;
        let kalshi_limit: u32 = 200;
        let mut kalshi_pages = 0u32;
        let mut kalshi_retries = 0u32;
        let max_kalshi_retries = 3u32;

        loop {
            kalshi_pages += 1;
            if kalshi_pages > 50 {
                println!("⚠️  Kalshi page cap reached (50)");
                break;
            }

            let mut url = format!(
                "https://api.elections.kalshi.com/trade-api/v2/events?limit={}&status=open&with_nested_markets=true",
                kalshi_limit
            );
            if let Some(ref cursor) = kalshi_cursor {
                if !cursor.is_empty() {
                    url.push_str(&format!("&cursor={}", urlencoding::encode(cursor)));
                }
            }

            let resp = match kalshi_client.get(&url).send().await {
                Ok(r) => r,
                Err(e) => {
                    println!("❌ Kalshi connection failed: {}", e);
                    kalshi_retries += 1;
                    if kalshi_retries > max_kalshi_retries { break; }
                    let wait = 2u64.pow(kalshi_retries);
                    println!("⏳ Retrying Kalshi in {}s...", wait);
                    tokio::time::sleep(std::time::Duration::from_secs(wait)).await;
                    continue;
                }
            };

            let status = resp.status();
            if status == 429 || status.is_server_error() {
                kalshi_retries += 1;
                if kalshi_retries > max_kalshi_retries {
                    println!("⚠️ Kalshi rate-limited after {} retries", max_kalshi_retries);
                    break;
                }
                let wait = 2u64.pow(kalshi_retries);
                println!("⏳ Kalshi {} — backing off {}s", status, wait);
                tokio::time::sleep(std::time::Duration::from_secs(wait)).await;
                continue;
            }
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                println!("❌ Kalshi HTTP {}: {}", status, body);
                break;
            }

            let body = match resp.text().await {
                Ok(b) => b,
                Err(e) => { println!("❌ Kalshi body read error: {}", e); break; }
            };
            if body.trim().is_empty() {
                kalshi_retries += 1;
                if kalshi_retries > max_kalshi_retries { break; }
                tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(kalshi_retries))).await;
                continue;
            }

            let json: Value = match serde_json::from_str(&body) {
                Ok(v) => v,
                Err(e) => {
                    println!("❌ Kalshi JSON parse error: {}", e);
                    kalshi_retries += 1;
                    if kalshi_retries > max_kalshi_retries { break; }
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(kalshi_retries))).await;
                    continue;
                }
            };
            kalshi_retries = 0;

            let events = json.get("events").and_then(|e| e.as_array()).cloned().unwrap_or_default();
            if events.is_empty() { break; }

            for event in &events {
                // Skip determined / settled / closed events
                let ev_status = event.get("status").and_then(|s| s.as_str()).unwrap_or("").to_lowercase();
                if matches!(ev_status.as_str(), "determined" | "settled" | "closed" | "finalized") {
                    continue;
                }

                let ticker = event.get("event_ticker").and_then(|t| t.as_str()).unwrap_or("").to_string();
                if ticker.is_empty() { continue; }

                let title = event.get("title").and_then(|t| t.as_str()).unwrap_or("Unknown").to_string();
                let category_raw = event.get("category").and_then(|c| c.as_str()).unwrap_or("");

                let event_image = extract_image(event, &[
                    "image_url", "thumbnail_url", "series_image_url",
                    "category_image_url", "og_image_url",
                ]);
                let icon = self.get_kalshi_image(kalshi_client, &ticker, &event_image).await;

                let nested = event.get("markets").and_then(|m| m.as_array()).cloned().unwrap_or_default();

                // Filter active markets, skip determined/settled/closed at market level
                let active_markets: Vec<Value> = nested.into_iter()
                    .filter(|m| {
                        let s = m.get("status").and_then(|s| s.as_str()).unwrap_or("").to_lowercase();
                        !matches!(s.as_str(), "determined" | "settled" | "closed" | "finalized")
                    })
                    .collect();

                if active_markets.is_empty() { continue; }

                let total_volume: f64 = active_markets.iter().map(extract_kalshi_market_volume).sum();
                let total_oi: f64 = active_markets.iter().map(extract_kalshi_open_interest).sum();

                // Pick the most contested market (closest to 0.5) as headline odds
                let best_market = active_markets.iter()
                    .min_by(|a, b| {
                        let da = (extract_kalshi_price(a) - 0.5).abs();
                        let db = (extract_kalshi_price(b) - 0.5).abs();
                        da.total_cmp(&db)
                    })
                    .unwrap();
                let odds = extract_kalshi_price(best_market);

                let mut outcomes: Vec<MarketOutcome> = active_markets.iter().take(5).map(|m| {
                    let name = m.get("yes_sub_title")
                        .or_else(|| m.get("title"))
                        .or_else(|| m.get("subtitle"))
                        .and_then(|t| t.as_str())
                        .unwrap_or("Yes")
                        .to_string();
                    MarketOutcome {
                        name,
                        price: extract_kalshi_price(m),
                        volume: extract_kalshi_market_volume(m),
                    }
                }).collect();
                if outcomes.is_empty() {
                    outcomes.push(MarketOutcome {
                        name: "Yes".to_string(), price: odds, volume: total_volume,
                    });
                }

                let category = map_kalshi_category(category_raw, &title);

                let end_date = event.get("close_time")
                    .or_else(|| event.get("expected_expiration_time"))
                    .and_then(|v| v.as_str())
                    .and_then(parse_datetime);

                let series = ticker.split('-').next().unwrap_or(ticker.as_str()).to_lowercase();
                let market_url = Some(format!(
                    "https://kalshi.com/markets/{}/{}",
                    series, ticker.to_lowercase()
                ));

                let pe = PredictionEvent {
                    id: Uuid::new_v4(),
                    title,
                    platform: "Kalshi".to_string(),
                    odds,
                    category,
                    external_id: format!("kalshi:{}", ticker),
                    updated_at: Utc::now(),
                    volume_24h: total_volume,
                    open_interest: total_oi,
                    icon_url: icon,
                    outcomes,
                    status: "active".to_string(),
                    end_date,
                    market_url,
                };

                // 🔑 Dedup by external_id (last write wins)
                kalshi_map.insert(pe.external_id.clone(), pe);
            }

            kalshi_cursor = json.get("cursor").and_then(|c| c.as_str())
                .map(|s| s.to_string()).filter(|s| !s.is_empty());

            println!("📡 Kalshi page fetched: {} unique events so far...", kalshi_map.len());

            if kalshi_cursor.is_none() { break; }
        }

        println!("✅ Kalshi events collected (deduped): {}", kalshi_map.len());

        // ─── POLYMARKET ──────────────────────────────────────────────
        let mut poly_map: HashMap<String, PredictionEvent> = HashMap::new();
        let poly_limit: u32 = 100;
        let mut poly_offset: u32 = 0;
        let poly_max: u32 = 5000;

        loop {
            if poly_offset >= poly_max {
                println!("⚠️  Polymarket offset cap reached ({})", poly_max);
                break;
            }

            let url = format!(
                "https://gamma-api.polymarket.com/events?limit={}&offset={}&order=volume24hr&ascending=false&closed=false&active=true",
                poly_limit, poly_offset
            );

            let resp = match poly_client.get(&url).send().await {
                Ok(r) => r,
                Err(e) => { println!("❌ Polymarket connection failed: {}", e); break; }
            };

            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                println!("❌ Polymarket HTTP {}: {}", status, body);
                break;
            }

            let json: Value = match resp.json::<Value>().await {
                Ok(j) => j,
                Err(e) => { println!("❌ Polymarket JSON parse error: {}", e); break; }
            };

            let events = match json.as_array() { Some(a) => a.clone(), None => break };
            if events.is_empty() { break; }

            for event in &events {
                // Skip resolved / closed / archived
                if value_as_bool(event.get("closed").unwrap_or(&Value::Null))
                    || value_as_bool(event.get("archived").unwrap_or(&Value::Null))
                {
                    continue;
                }
                let ev_status = event.get("status").and_then(|s| s.as_str()).unwrap_or("").to_lowercase();
                if matches!(ev_status.as_str(), "resolved" | "closed") { continue; }

                let title = event.get("title").and_then(|t| t.as_str()).unwrap_or("Unknown").to_string();
                let ext_id = event.get("id").map(|v| {
                    if let Some(s) = v.as_str() { s.to_string() }
                    else { v.to_string().trim_matches('"').to_string() }
                }).unwrap_or_default();
                if ext_id.is_empty() { continue; }

                let icon = event.get("image").or_else(|| event.get("icon"))
                    .and_then(|i| i.as_str()).map(|s| s.to_string());

                let slug = event.get("slug").and_then(|s| s.as_str()).map(|s| s.to_string());
                let market_url = slug.as_ref()
                    .map(|s| format!("https://polymarket.com/event/{}", s))
                    .or_else(|| Some(format!(
                        "https://polymarket.com/markets?query={}",
                        urlencoding::encode(&title)
                    )));

                let end_date = event.get("endDate").or_else(|| event.get("end_date"))
                    .and_then(|v| v.as_str()).and_then(parse_datetime);

                let event_volume = extract_poly_market_volume(event);

                let markets = event.get("markets").and_then(|m| m.as_array()).cloned().unwrap_or_default();
                let active_markets: Vec<&Value> = markets.iter()
                    .filter(|m| {
                        let active = m.get("active").and_then(|a| a.as_bool()).unwrap_or(true);
                        let closed = m.get("closed").and_then(|c| c.as_bool()).unwrap_or(false);
                        let archived = m.get("archived").and_then(|a| a.as_bool()).unwrap_or(false);
                        active && !closed && !archived
                    })
                    .collect();

                if active_markets.is_empty() { continue; }

                let total_vol = if event_volume > 0.0 {
                    event_volume
                } else {
                    active_markets.iter().map(|m| extract_poly_market_volume(m)).sum()
                };

                let best_market = active_markets.iter()
                    .max_by(|a, b| {
                        extract_poly_market_volume(a).total_cmp(&extract_poly_market_volume(b))
                    })
                    .unwrap();
                let odds = extract_poly_price(best_market);

                let mut outcomes: Vec<MarketOutcome> = active_markets.iter().take(5).map(|m| {
                    let name = m.get("question").or_else(|| m.get("groupItemTitle"))
                        .and_then(|t| t.as_str()).unwrap_or("Yes").to_string();
                    MarketOutcome {
                        name,
                        price: extract_poly_price(m),
                        volume: extract_poly_market_volume(m),
                    }
                }).collect();
                if outcomes.is_empty() {
                    outcomes.push(MarketOutcome {
                        name: "Yes".to_string(), price: odds, volume: total_vol,
                    });
                }

                let tags = event.get("tags").and_then(|t| t.as_array()).cloned().unwrap_or_default();
                let category = map_poly_category(&tags, &title);

                let pe = PredictionEvent {
                    id: Uuid::new_v4(),
                    title,
                    platform: "Polymarket".to_string(),
                    odds,
                    category,
                    external_id: format!("polymarket:{}", ext_id),
                    updated_at: Utc::now(),
                    volume_24h: total_vol,
                    open_interest: 0.0,
                    icon_url: icon,
                    outcomes,
                    status: "active".to_string(),
                    end_date,
                    market_url,
                };

                // 🔑 Dedup by external_id (last write wins)
                poly_map.insert(pe.external_id.clone(), pe);
            }

            if events.len() < poly_limit as usize { break; }
            poly_offset += poly_limit;
            println!("📡 Polymarket page fetched: {} unique events so far...", poly_map.len());
        }

        println!("✅ Polymarket events collected (deduped): {}", poly_map.len());

        // ─── MERGE + FINAL DEDUP ─────────────────────────────────────
        let mut unified_map: HashMap<String, PredictionEvent> =
            HashMap::with_capacity(kalshi_map.len() + poly_map.len());
        for (k, v) in kalshi_map { unified_map.insert(k, v); }
        for (k, v) in poly_map   { unified_map.insert(k, v); }

        let unified: Vec<PredictionEvent> = unified_map.into_values().collect();
        println!("✅ Total unified events (deduped): {}", unified.len());
        unified
    }
}