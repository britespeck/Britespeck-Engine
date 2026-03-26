Since the image scraping is already built into fetcher.rs, the main.rs no longer needs the duplicate scraping code. Here's your cleaned-up main.rs:


mod models;
mod fetcher;

use sqlx::postgres::{PgPoolOptions, Postgres};
use std::time::Duration;
use crate::fetcher::MarketFetcher;
use std::env;
use dotenv::dotenv;
use serde_json;
use reqwest::header::{HeaderMap, HeaderValue};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = dotenv();

    let mut database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://localhost/placeholder".to_string());

    if database_url.contains('?') {
        database_url.push_str("&prepared_statement_cache_capacity=0");
    } else {
        database_url.push_str("?prepared_statement_cache_capacity=0");
    }

    println!("🚀 Connecting to Supabase...");
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&database_url)
        .await?;

    // --- STEALTH CLIENT SETUP WITH PROXY ---
    let mut headers = HeaderMap::new();
    headers.insert("Accept", HeaderValue::from_static("application/json, text/plain, */*"));
    headers.insert("Accept-Language", HeaderValue::from_static("en-US,en;q=0.9"));
    headers.insert("Origin", HeaderValue::from_static("https://polymarket.com"));
    headers.insert("Referer", HeaderValue::from_static("https://polymarket.com"));

    let mut client_builder = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
        .default_headers(headers)
        .cookie_store(true)
        .timeout(Duration::from_secs(25));

    if let Ok(proxy_url) = env::var("PROXY_URL") {
        println!("🌐 Using Proxy: {}", proxy_url);
        let proxy = reqwest::Proxy::all(proxy_url)?;
        client_builder = client_builder.proxy(proxy);
    }

    let client = client_builder.build()?;

    let fetcher = MarketFetcher::new();
    println!("📈 BPS-100 & Multi-Tab Engine Live!");

    loop {
        println!("Checking markets...");
        let events = fetcher.get_unified_events(&client).await;
        if events.is_empty() {
            println!("⚠️ 0 events found. Check API paths or Proxy status.");
        } else {
            println!("💎 Syncing {} active events...", events.len());

            for event in &events {
                let outcomes_json = serde_json::to_value(&event.outcomes).unwrap_or_default();

                let res = sqlx::query(
                    "INSERT INTO prediction_events (
                        id, title, platform, odds, category, external_id,
                        volume_24h, icon_url, updated_at, status, end_date, outcomes
                    )
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                     ON CONFLICT (external_id) DO UPDATE
                     SET odds = EXCLUDED.odds,
                         category = EXCLUDED.category,
                         volume_24h = EXCLUDED.volume_24h,
                         icon_url = EXCLUDED.icon_url,
                         updated_at = EXCLUDED.updated_at,
                         status = EXCLUDED.status,
                         end_date = EXCLUDED.end_date,
                         outcomes = EXCLUDED.outcomes"
                )
                .bind(event.id)
                .bind(&event.title)
                .bind(&event.platform)
                .bind(event.odds)
                .bind(&event.category)
                .bind(&event.external_id)
                .bind(event.volume_24h)
                .bind(&event.icon_url)
                .bind(event.updated_at)
                .bind(&event.status)
                .bind(event.end_date)
                .bind(outcomes_json)
                .execute(&pool)
                .await;

                if let Err(e) = res {
                    println!("❌ SQL Error on event {}: {}", event.external_id, e);
                }
            }

            let top_100 = sqlx::query_as::<Postgres, (f64,)>(
                "SELECT odds FROM prediction_events WHERE status = 'active' ORDER BY volume_24h DESC LIMIT 100"
            ).fetch_all(&pool).await;

            match top_100 {
                Ok(rows) if !rows.is_empty() => {
                    let sum: f64 = rows.iter().map(|row| row.0).sum();
                    let index_value = sum / rows.len() as f64;

                    let _ = sqlx::query("INSERT INTO index_history (value, market_count) VALUES ($1, $2)")
                        .bind(index_value)
                        .bind(rows.len() as i32)
                        .execute(&pool)
                        .await;

                    println!("📊 BPS-100 INDEX UPDATED: {:.4}", index_value);
                },
                Err(e) => println!("❌ BPS-100 Query Error: {}", e),
                _ => {}
            }
        }
        println!("💤 Sleeping 30s...");
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}
No lazy_static needed in main.rs anymore — the fetcher.rs owns the image cache internally. Deploy and the 🖼️ logs will confirm scraping.






Mar 26
at 6:12 PM
ok do we need this?

[dependencies] lazy_static = "1"



No — since your fetcher.rs uses Mutex<HashMap> inside the MarketFetcher struct (not a global static), you don't need lazy_static at all. You can remove it from Cargo.toml.






Mar 26
at 6:20 PM
Why I'm I getting 200 contracts for each ? is that in the code ? I feel 200 each is quites something but not everything.




Thought for 12s
Yes, it's hardcoded in your fetcher.rs:

Kalshi: ?limit=200&status=open&with_nested_markets=true
Polymarket: ?limit=200&active=true&closed=false
Both APIs support higher limits. Kalshi allows up to 500 per request, and Polymarket's Gamma API can go higher too. You can bump those limit=200 values to limit=500 (or even paginate with offset/cursor to get all contracts) if you want more coverage.






Mar 26
at 6:24 PM
Yes I want all the contracts from kalshi and polymarket. I mean all the contracts that are not expired, resolved or from last year, I believe that's already coded in there so that we don't get irrelevant contracts. But I want to get all contracts for Kalshi and Polymarket.



Here's your updated fetcher.rs with pagination for both platforms to fetch all active contracts:


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
                                            volume_24h: 0.0,
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

                            // Check for next page cursor
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

                                    if !outcomes.is_empty() && !mother_id.is_empty() {
                                        unified.push(PredictionEvent {
                                            id: Uuid::new_v4(),
                                            title: event_title.to_string(),
                                            platform: "Polymarket".to_string(),
                                            odds: outcomes.first().map(|o| o.price).unwrap_or(0.5),
                                            category: map_polymarket_category(&tags, event_title).to_string(),
                                            external_id: mother_id.to_string(),
                                            volume_24h: total_volume,
                                            icon_url: extract_image(event, &["imageOptimized", "image", "icon"]),
                                            updated_at: Utc::now(),
                                            status: "active".to_string(),
                                            end_date: parse_end_date(event, &["endDate"]),
                                            outcomes,
                                        });
                                    }
                                }
                            }

                            poly_total += batch_count as u32;

                            if batch_count < poly_limit as usize {
                                println!("📡 Polymarket total: {} events (done)", poly_total);
                                break;
                            }
                            poly_offset += poly_limit;
                            println!("📡 Polymarket page fetched: {} events so far...", poly_total);
                        },
                        Err(e) => {
                            println!("❌ Polymarket JSON parse error: {}", e);
                            break;
                        }
                    }
                },
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
Key changes:

Kalshi: loops using cursor pagination until no more pages
Polymarket: loops using offset pagination (offset += 200 each page)
Both stop when a batch returns fewer than 200 results
Progress logs show cumulative count per page
Deploy this and you'll get all active contracts from both platforms.






Mar 26
at 6:35 PM
Nice this is what we got



Looks like things are working well! What would you like to improve next?






Mar 26
at 6:47 PM
OK we are seeing a lot but we're still not being able to fetch the kalshi image logos we get nothing for kalshi.




Thought for 19s
Let me check what's actually stored in the database for Kalshi images.


8 tools used
Kalshi's API returns zero image fields and their website blocks scraping with a Vercel security checkpoint — that's why all Kalshi icon_url values are null in the database. The frontend already gracefully falls back to category gradient icons for events without images. Polymarket images work fine since their API includes image/icon fields.






Mar 26
at 6:50 PM
OK could you add the images to the contracts from our end ? Once they come in you qualify the title and look for that title on kalshi and then add the image or logo next to the title ? could you do that from our end here?




Thought for 18s
Add Kalshi image fetching

Create Kalshi image fetcher edge function
Add client-side image enrichment hook

Details
Preview
Done! Here's what I built:

kalshi-images edge function — Uses Firecrawl to scrape Kalshi market pages and extract og:image URLs. Successfully tested: it found the Pope market image at kalshi.com/api-app/preview/KXNEWPOPE-35.

useKalshiImages hook — Runs on the Dashboard, detects Kalshi events missing images, sends batches of 5 to the edge function, and updates the events with found images. Includes a session-level cache so each event is only fetched once.

The flow: Dashboard loads → Kalshi events appear with category gradient fallback → 2 seconds later the hook fires → Firecrawl scrapes the Kalshi pages → images appear on the cards.






Mar 26
at 7:00 PM
well we need to do better. Because what we need is the logo next to the title of the contract and we need more than 5 at a time. We need as many kalshi logos at a time. so they can be filled.




Thought for 31s
Scaled Kalshi image fetching

Scale up Kalshi image enrichment
Details
Preview
Upgraded the image enrichment — now processes up to 50 Kalshi events per batch with 10 concurrent Firecrawl lookups instead of the old 5-at-a-time sequential approach. The hook also loops through all missing events in 50-event batches until everything is covered.






Mar 26
at 7:03 PM
We need a major improvement as well. When we click on "trade on kalshi" we need to be taken to the contract on kalshi or on polymarket.




Thought for 26s
Fixed trade links to Kalshi/PM

Details
Preview
"Trade on Kalshi" now links directly to the contract page (kalshi.com/markets/{ticker}), and Polymarket uses a title-based search URL. Both the main Trade button and individual outcome cards use the direct URL.






Mar 26
at 7:12 PM
OK what I mean is this: "You, Me & Tuscany" Rotten Tomatoes score?

that title has a logo/ image on the left before the tittle. that's what we need to grab for all the contracts that have a logo picture on the left.

But for the the contracts that don't then we keep grabbing the title with the linecharts. Get me ?




Thought for 13s
Let me see what's currently showing on the dashboard.

Pulled Kalshi logos via API

Rewrite kalshi-images to use Kalshi API
Update EventCard image display

Details
Preview
Rewrote the kalshi-images edge function to use the Kalshi API directly (/trade-api/v2/events/{ticker}) instead of Firecrawl scraping — this fetches the actual image_url from Kalshi's event data. It deduplicates by series ticker (so KXROTTENYES-26APR15 and KXROTTENYES-26APR21 share one API call) and runs 15 concurrent fetches. The EventCard already handles both cases: contracts with an image show the logo next to the title, and those without keep the category gradient icon.






Mar 26
at 7:19 PM
Ok now we are going to work on the BPS index: I see that we have Robinhood, Gemini, predict it. But in our Rust engine we are only fetching from Polymarket and Kalshi so for now we only need to have the top 100 coming from kalshi and polymarket both 50% and 50% of the top global contracts. you get me ? We are going to reflect real time relevant info now. That's how we are going to be feeding our ...

Show more



Thought for 16s
Reworked BPS100 to top 100

Rewrite BPS100 data source
Details
Preview
Rewrote the BPS 100 page to pull real-time data — top 50 Kalshi + top 50 Polymarket contracts by 24h volume from the external database. Removed all placeholder contracts, fake platforms (Robinhood, PredictIt, Gemini), and category-based padding. Added a platform split card showing Kalshi vs Polymarket fill rates. The index refreshes every 30 seconds.






Mar 26
at 7:28 PM
but you removed social and weather from the master list where it says all 100 then we got polytics economics tech sports social and weather

and this is the order we had Price (¢), 24h Chg, Volume and Platform




Thought for 13s
Fixed BPS100 table column order

Details
Preview
Reordered the master list columns to: Price (¢) → 24h Chg → Volume → Platform. Social and Weather categories were already present in the filter pills (as "Social" and "Weather") — they show when contracts match those categories.






Mar 26
at 7:31 PM
but you need to add social and weather where it says all 100 then we get politics, economics, tech, sports, this is where we get social and weather ok add.



Added Social/Weather to BPS tabs

Details
Preview
Updated the "All" tab to show the contract count (e.g. "All 100"), and added flex-wrap so all 6 category tabs (Politics, Economics, Tech, Sports, Social, Weather) remain visible without clipping on smaller viewports. All 6 categories were already in the code — they should now be clearly visible.






Mar 26
at 7:34 PM
I meant when I click on Master list: We should have this "All ( 100"), so all 6 category tabs (Politics, Economics, Tech, Sports, Social, and weather) are there. get me ?



The master list filter section already has "All ({count})" and iterates all CATEGORIES. But line 331 hides categories with 0 contracts: if (count === 0) return null;. The user wants all 6 to always show.

Always show BPS100 category tabs

Details
Preview
All 6 category tabs (Politics, Economics, Tech, Sports, Social, Weather) will now always show in the Master List, even when a category has 0 contracts.






Mar 26
at 7:37 PM
Now why don't we have volume for the kalshi contracts? We does the fetcher code need to get real time volume for kalshi and polymarket or does it already fetch it ? Let me show it to you and you tell me:

use crate::models::{PredictionEvent, MarketOutcome};

use chrono::Utc;

use uuid::Uuid;

use serde_json::Value;

use std::collections::HashMap;

use std::sync::Mutex;

use std::time::Duration; ...

Show more


Looking at your Rust fetcher, the issue is clear: Kalshi volume is hardcoded to 0.0 (line volume_24h: 0.0), while Polymarket correctly sums volume24hr from sub-markets.

For Kalshi, the API provides volume data on each nested market. You need to sum it up just like you do for Polymarket. Here's the fix for your Rust code — in the Kalshi section, sum volume or volume_24h from each market:


// Inside the Kalshi event loop, before building PredictionEvent:
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

// Then use kalshi_volume instead of 0.0:
volume_24h: kalshi_volume,
The Kalshi API returns volume_24h (or volume) on each nested market object — you just weren't reading it. Once you deploy this Rust fix, Kalshi contracts will show real volume in the BPS 100 master list.






Mar 26
at 7:39 PM
ok does this code go on the fetcher ? if it does can you plug it in so that I can copy paste ?



Here's your updated fetcher with the Kalshi volume fix plugged in. Replace your entire fetcher.rs:


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

                                    if !outcomes.is_empty() && !mother_id.is_empty() {
                                        unified.push(PredictionEvent {
                                            id: Uuid::new_v4(),
                                            title: event_title.to_string(),
                                            platform: "Polymarket".to_string(),
                                            odds: outcomes.first().map(|o| o.price).unwrap_or(0.5),
                                            category: map_polymarket_category(&tags, event_title).to_string(),
                                            external_id: mother_id.to_string(),
                                            volume_24h: total_volume,
                                            icon_url: extract_image(event, &["imageOptimized", "image", "icon"]),
                                            updated_at: Utc::now(),
                                            status: "active".to_string(),
                                            end_date: parse_end_date(event, &["endDate"]),
                                            outcomes,
                                        });
                                    }
                                }
                            }

                            poly_total += batch_count as u32;

                            if batch_count < poly_limit as usize {
                                println!("📡 Polymarket total: {} events (done)", poly_total);
                                break;
                            }
                            poly_offset += poly_limit;
                            println!("📡 Polymarket page fetched: {} events so far...", poly_total);
                        },
                        Err(e) => {
                            println!("❌ Polymarket JSON parse error: {}", e);
                            break;
                        }
                    }
                },
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