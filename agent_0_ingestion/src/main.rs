//! Agent 0: Live Price Feed — Tokocrypto + Indodax WebSocket Multiplexer
//!
//! Architecture:
//!   Tokocrypto `btcusdt@depth` — real-time differential LOB stream.
//!   Each exchange message fires on every individual order book change (no 100ms
//!   batching).  Agent 0 applies each diff to an in-memory DepthBook, then:
//!     • SET  toko:btc_usdt:lob        — latest 10-level snapshot (any consumer)
//!     • PUBLISH toko:btc_usdt:lob:tick — same JSON on a Pub/Sub channel so
//!       Engine C can subscribe and process every tick without polling.
//!
//! Redis key schema:
//!   toko:btc_usdt:lob       → 10-level LOB JSON snapshot        (Engine C GET)
//!   toko:btc_usdt:lob:tick  → Pub/Sub channel, same JSON/tick   (Engine C SUB)
//!   toko:btc_usdt:ticker    → best bid/ask ticker               (Engine B)
//!   toko:eth_usdt:ticker    → best bid/ask ticker               (Engine B)
//!   toko:sol_idr:ticker     → SOL/IDR best bid/ask              (Engine A)
//!   toko:btc_idr:ticker     → BTC/IDR best bid/ask              (Engine A/C)
//!   indo:sol_idr:ask        → Indodax SOL/IDR orderbook         (Engine A)
//!   indo:btc_idr:ask        → Indodax BTC/IDR orderbook         (Engine A)

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use redis::AsyncCommands;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

// ── Tokocrypto combined stream URL ────────────────────────────────────────────
//
// btcusdt@depth        — real-time differential LOB stream.
//                        Fires on every individual order book change.
//                        No time qualifier = maximum tick resolution.
//                        Engine C subscribes to toko:btc_usdt:lob:tick.
//
// btcusdt@bookTicker   — best bid/ask for Engine B ratio signal
// ethusdt@bookTicker   — best bid/ask for Engine B ratio signal
// solidr@bookTicker    — SOL/IDR best bid/ask for Engine A
// btcidr@bookTicker    — BTC/IDR best bid/ask for Engine A + Engine C execution
const TOKO_WSS_DEFAULT: &str = concat!(
    "wss://stream-cloud.tokocrypto.site/stream?streams=",
    "btcusdt@depth",
    "/btcusdt@bookTicker",
    "/ethusdt@bookTicker",
    "/solidr@bookTicker",
    "/btcidr@bookTicker"
);

// ── Indodax WebSocket ─────────────────────────────────────────────────────────
const INDO_WSS: &str = "wss://ws3.indodax.com/ws/";
const INDO_STATIC_TOKEN: &str =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9\
     .eyJleHAiOjE5NDY2MTg0MTV9\
     .UR1lBM6Eqh0yWz-PVirw1uPCxe60FdchR8eNVdsskeo";

// ── In-memory limit-order book ────────────────────────────────────────────────

/// Maintains a live BTC/USDT LOB by applying real-time differential updates.
///
/// Keys are the price strings exactly as received from the exchange (avoids
/// floating-point equality issues in HashMap lookups).  Values are quantities.
struct DepthBook {
    bids: HashMap<String, f64>,  // price_str → qty (descending on read)
    asks: HashMap<String, f64>,  // price_str → qty (ascending on read)
    tick_count: u64,
}

impl DepthBook {
    fn new() -> Self {
        Self { bids: HashMap::new(), asks: HashMap::new(), tick_count: 0 }
    }

    /// Apply one diff event.  Levels with qty == 0 are removed (Binance protocol).
    fn apply(&mut self, bid_updates: &[[String; 2]], ask_updates: &[[String; 2]]) {
        for [price, qty] in bid_updates {
            let q: f64 = qty.parse().unwrap_or(0.0);
            if q == 0.0 { self.bids.remove(price); } else { self.bids.insert(price.clone(), q); }
        }
        for [price, qty] in ask_updates {
            let q: f64 = qty.parse().unwrap_or(0.0);
            if q == 0.0 { self.asks.remove(price); } else { self.asks.insert(price.clone(), q); }
        }
        self.tick_count += 1;
    }

    /// Returns top-N bid and ask levels, sorted (bids desc, asks asc).
    fn top(&self, n: usize) -> (Vec<(f64, f64)>, Vec<(f64, f64)>) {
        let mut bids: Vec<(f64, f64)> = self.bids.iter()
            .filter_map(|(p, q)| p.parse::<f64>().ok().map(|pf| (pf, *q)))
            .collect();
        bids.sort_unstable_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        let mut asks: Vec<(f64, f64)> = self.asks.iter()
            .filter_map(|(p, q)| p.parse::<f64>().ok().map(|pf| (pf, *q)))
            .collect();
        asks.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        (bids.into_iter().take(n).collect(), asks.into_iter().take(n).collect())
    }

    fn is_ready(&self) -> bool { !self.bids.is_empty() && !self.asks.is_empty() }
}

// ── Serde models ──────────────────────────────────────────────────────────────

/// Combined stream envelope: {"stream":"…","data":{…}}
#[derive(Deserialize)]
struct CombinedMessage {
    stream: String,
    data: serde_json::Value,
}

/// Real-time differential LOB update (btcusdt@depth, no time qualifier).
/// Fires on every individual order book change.
#[derive(Deserialize)]
struct DepthDiff {
    /// Bid level updates: [price_str, qty_str].  qty="0" means remove level.
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    /// Ask level updates: [price_str, qty_str].  qty="0" means remove level.
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}

/// BookTicker: best bid/ask snapshot
#[derive(Deserialize)]
struct BookTicker {
    #[serde(rename = "b")] bid_price: String,
    #[serde(rename = "B")] bid_qty:   String,
    #[serde(rename = "a")] ask_price: String,
    #[serde(rename = "A")] ask_qty:   String,
}

// ── Indodax serde models ──────────────────────────────────────────────────────

#[derive(Deserialize)]
struct IndoPush {
    result: Option<IndoPushResult>,
    id: Option<u64>,
}
#[derive(Deserialize)]
struct IndoPushResult {
    channel: Option<String>,
    data: Option<IndoDataWrapper>,
}
#[derive(Deserialize)]
struct IndoDataWrapper {
    data: Option<serde_json::Value>,
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn micros_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_micros() as u64
}

/// {"ts":…µs,"ask":…,"bid":…,"volume":…}
fn ticker_json(ask: f64, bid: f64, volume: f64) -> String {
    format!(r#"{{"ts":{},"ask":{:.8},"bid":{:.8},"volume":{:.4}}}"#,
        micros_now(), ask, bid, volume)
}

/// {"ts":…µs,"bids":[[p,v],…],"asks":[[p,v],…]}
fn lob_json(bids: &[(f64, f64)], asks: &[(f64, f64)]) -> String {
    let fmt = |levels: &[(f64, f64)]| -> String {
        levels.iter().map(|(p, v)| format!("[{:.2},{:.8}]", p, v)).collect::<Vec<_>>().join(",")
    };
    format!(r#"{{"ts":{},"bids":[{}],"asks":[{}]}}"#, micros_now(), fmt(bids), fmt(asks))
}

fn parse_book_ticker(data: &serde_json::Value) -> Option<BookTicker> {
    serde_json::from_value(data.clone()).ok()
}

// ── Redis helpers ─────────────────────────────────────────────────────────────

async fn redis_set(con: &mut redis::aio::MultiplexedConnection, key: &str, value: &str) {
    if let Err(e) = con.set::<_, _, ()>(key, value).await {
        warn!("Redis SET {} failed: {}", key, e);
    }
}

/// SET the key AND PUBLISH to the Pub/Sub tick channel (key + ":tick").
/// Engine C subscribes to the channel for zero-latency tick delivery.
async fn redis_set_and_publish(
    con: &mut redis::aio::MultiplexedConnection,
    key: &str,
    value: &str,
) {
    if let Err(e) = con.set::<_, _, ()>(key, value).await {
        warn!("Redis SET {} failed: {}", key, e);
        return;
    }
    let channel = format!("{}:tick", key);
    if let Err(e) = con.publish::<_, _, ()>(&channel, value).await {
        warn!("Redis PUBLISH {} failed: {}", channel, e);
    }
}

// ── Tokocrypto WebSocket loop ─────────────────────────────────────────────────

async fn stream_loop(
    con: &mut redis::aio::MultiplexedConnection,
    wss_url: &str,
) -> Result<()> {
    info!("Connecting to Tokocrypto WSS: {}", wss_url);

    // Fresh DepthBook on every (re)connect — avoids stale state from previous session
    let mut btcusdt_book = DepthBook::new();

    let url = url::Url::parse(wss_url)?;
    let (ws_stream, _) = connect_async(url).await?;
    info!("Tokocrypto WSS connected. Streaming tick-level market data.");

    let (_, mut reader) = ws_stream.split();

    while let Some(msg) = reader.next().await {
        let msg = match msg {
            Ok(m)  => m,
            Err(e) => { warn!("WSS receive error: {}", e); break; }
        };

        let text = match msg {
            Message::Text(t) => t,
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue,
            Message::Close(f) => { info!("WSS closed by server: {:?}", f); break; }
            Message::Binary(_) => continue,
        };

        let combined: CombinedMessage = match serde_json::from_str(&text) {
            Ok(c)  => c,
            Err(e) => { warn!("WSS parse error: {} — raw: {:.80}", e, text); continue; }
        };

        match combined.stream.as_str() {

            // ── BTC/USDT real-time tick diff (Engine C) ──────────────────────
            // Fires on every individual order-book change — no time batching.
            // Maintains in-memory DepthBook; publishes snapshot after each tick.
            "btcusdt@depth" => {
                let diff: DepthDiff = match serde_json::from_value(combined.data) {
                    Ok(d)  => d,
                    Err(e) => { warn!("btcusdt@depth parse error: {}", e); continue; }
                };

                btcusdt_book.apply(&diff.bids, &diff.asks);

                if !btcusdt_book.is_ready() { continue; }

                let (bids, asks) = btcusdt_book.top(10);
                if bids.is_empty() || asks.is_empty() { continue; }

                // Log first ready tick and every 1 000 ticks thereafter
                if btcusdt_book.tick_count == 1 || btcusdt_book.tick_count % 1_000 == 0 {
                    info!(
                        "BTC/USDT LOB tick #{} — best bid={:.2} ask={:.2}",
                        btcusdt_book.tick_count, bids[0].0, asks[0].0
                    );
                }

                let json = lob_json(&bids, &asks);
                // SET for any polling consumer + PUBLISH for Engine C subscription
                redis_set_and_publish(con, "toko:btc_usdt:lob", &json).await;
            }

            // ── BTC/USDT bookTicker ──────────────────────────────────────────
            // Engine B: writes toko:btc_usdt:ticker for ratio signal.
            // Engine C: fires on every top-of-book change (event-driven, not
            //           batched at 1s like @depth).  We build the LOB snapshot
            //           from the current DepthBook (levels 2–10) and override
            //           level 1 with the bookTicker values (most current ToB).
            "btcusdt@bookTicker" => {
                let bt = match parse_book_ticker(&combined.data) {
                    Some(b) => b,
                    None    => { warn!("btcusdt bookTicker parse error"); continue; }
                };
                let bid     = bt.bid_price.parse::<f64>().unwrap_or(0.0);
                let ask     = bt.ask_price.parse::<f64>().unwrap_or(0.0);
                let bid_qty = bt.bid_qty.parse::<f64>().unwrap_or(0.0);
                let ask_qty = bt.ask_qty.parse::<f64>().unwrap_or(0.0);
                if bid <= 0.0 || ask <= 0.0 { continue; }

                // Engine B ticker (best bid/ask + bid side liquidity)
                redis_set(con, "toko:btc_usdt:ticker", &ticker_json(ask, bid, bid_qty)).await;

                // Engine C LOB tick — event-driven on every ToB change
                if btcusdt_book.is_ready() {
                    let (mut bids, mut asks) = btcusdt_book.top(10);
                    // Override top-of-book with bookTicker (fresher than @depth snapshot)
                    if !bids.is_empty() { bids[0] = (bid, bid_qty); }
                    if !asks.is_empty() { asks[0] = (ask, ask_qty); }
                    let json = lob_json(&bids, &asks);
                    redis_set_and_publish(con, "toko:btc_usdt:lob", &json).await;
                }
            }

            // ── ETH/USDT bookTicker (Engine B ratio signal) ──────────────────
            "ethusdt@bookTicker" => {
                let bt = match parse_book_ticker(&combined.data) {
                    Some(b) => b,
                    None    => { warn!("ethusdt bookTicker parse error"); continue; }
                };
                let bid = bt.bid_price.parse::<f64>().unwrap_or(0.0);
                let ask = bt.ask_price.parse::<f64>().unwrap_or(0.0);
                let vol = bt.bid_qty.parse::<f64>().unwrap_or(0.0);
                if bid <= 0.0 || ask <= 0.0 { continue; }
                redis_set(con, "toko:eth_usdt:ticker", &ticker_json(ask, bid, vol)).await;
            }

            // ── SOL/IDR bookTicker (Engine A) ────────────────────────────────
            "solidr@bookTicker" => {
                let bt = match parse_book_ticker(&combined.data) {
                    Some(b) => b,
                    None    => { warn!("solidr bookTicker parse error"); continue; }
                };
                let bid     = bt.bid_price.parse::<f64>().unwrap_or(0.0);
                let ask     = bt.ask_price.parse::<f64>().unwrap_or(0.0);
                let ask_qty = bt.ask_qty.parse::<f64>().unwrap_or(0.0);
                if bid <= 0.0 || ask <= 0.0 { continue; }
                redis_set(con, "toko:sol_idr:ticker", &ticker_json(ask, bid, ask_qty * ask)).await;
            }

            // ── BTC/IDR bookTicker (Engine A spread + Engine C execution) ────
            "btcidr@bookTicker" => {
                let bt = match parse_book_ticker(&combined.data) {
                    Some(b) => b,
                    None    => { warn!("btcidr bookTicker parse error"); continue; }
                };
                let bid     = bt.bid_price.parse::<f64>().unwrap_or(0.0);
                let ask     = bt.ask_price.parse::<f64>().unwrap_or(0.0);
                let ask_qty = bt.ask_qty.parse::<f64>().unwrap_or(0.0);
                if bid <= 0.0 || ask <= 0.0 { continue; }
                redis_set(con, "toko:btc_idr:ticker", &ticker_json(ask, bid, ask_qty * ask)).await;
            }

            other => { warn!("Unhandled stream: {}", other); }
        }
    }

    info!("Tokocrypto WSS stream loop ended.");
    Ok(())
}

// ── Indodax WebSocket loop ────────────────────────────────────────────────────

async fn indo_stream_loop(con: &mut redis::aio::MultiplexedConnection) -> Result<()> {
    info!("Connecting to Indodax WSS: {}", INDO_WSS);

    let url = url::Url::parse(INDO_WSS)?;
    let (ws_stream, _) = connect_async(url).await?;
    info!("Indodax WSS connected. Authenticating…");

    let (mut writer, mut reader) = ws_stream.split();

    let auth = serde_json::json!({ "params": { "token": INDO_STATIC_TOKEN }, "id": 1 });
    writer.send(Message::Text(auth.to_string())).await?;

    let mut subscribed = false;

    while let Some(msg) = reader.next().await {
        let msg = match msg { Ok(m) => m, Err(e) => { warn!("Indodax WSS error: {}", e); break; } };

        let text = match msg {
            Message::Text(t)  => t,
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue,
            Message::Close(f) => { info!("Indodax WSS closed: {:?}", f); break; }
            Message::Binary(_) => continue,
        };

        let push: IndoPush = match serde_json::from_str(&text) {
            Ok(p)  => p,
            Err(e) => { warn!("Indodax parse error: {} — raw: {:.80}", e, text); continue; }
        };

        // Auth ACK → subscribe to orderbook channels
        if push.id == Some(1) && !subscribed {
            info!("Indodax authenticated. Subscribing to BTC/IDR and SOL/IDR orderbooks.");
            for (id, channel) in [(2u64, "market:order-book-btcidr"), (3, "market:order-book-solidr")] {
                let sub = serde_json::json!({ "method": 1, "params": { "channel": channel }, "id": id });
                writer.send(Message::Text(sub.to_string())).await?;
            }
            subscribed = true;
            continue;
        }

        let result   = match &push.result   { Some(r) => r, None => continue };
        let channel  = match &result.channel { Some(c) => c.as_str(), None => continue };
        let data_val = match result.data.as_ref().and_then(|d| d.data.as_ref()) {
            Some(v) => v, None => continue,
        };

        let parse_best = |side: &str| -> Option<f64> {
            data_val.get(side)?.as_array()?.first()?.get("price")?.as_str()?.parse::<f64>().ok()
        };

        // Sum idr_volume across top-5 levels for Engine A's volume liquidity check
        let parse_idr_vol = |side: &str| -> f64 {
            data_val.get(side).and_then(|a| a.as_array()).map(|arr| {
                arr.iter().take(5)
                    .filter_map(|l| l.get("idr_volume")?.as_str()?.parse::<f64>().ok())
                    .sum()
            }).unwrap_or(0.0)
        };

        match channel {
            "market:order-book-btcidr" => {
                if let (Some(ask), Some(bid)) = (parse_best("ask"), parse_best("bid")) {
                    redis_set(con, "indo:btc_idr:ask", &ticker_json(ask, bid, parse_idr_vol("ask"))).await;
                }
            }
            "market:order-book-solidr" => {
                if let (Some(ask), Some(bid)) = (parse_best("ask"), parse_best("bid")) {
                    redis_set(con, "indo:sol_idr:ask", &ticker_json(ask, bid, parse_idr_vol("ask"))).await;
                }
            }
            _ => {}
        }
    }

    info!("Indodax WSS stream loop ended.");
    Ok(())
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("agent_0_ingestion=info,info")
        .init();

    let toko_key    = std::env::var("TOKO_API_KEY").unwrap_or_default();
    let toko_secret = std::env::var("TOKO_API_SECRET").unwrap_or_default();
    let indo_key    = std::env::var("INDO_API_KEY").unwrap_or_default();
    let _indo_secret = std::env::var("INDO_API_SECRET").unwrap_or_default();
    let redis_url   = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let wss_url     = std::env::var("TOKO_STREAM_URL").unwrap_or_else(|_| TOKO_WSS_DEFAULT.to_string());

    if toko_key.is_empty() || toko_secret.is_empty() {
        warn!("TOKO_API_KEY / TOKO_API_SECRET not set — market-data streams work without them.");
    } else {
        info!("Toko credentials loaded (key: {}…{})", &toko_key[..4], &toko_key[toko_key.len()-4..]);
    }
    if indo_key.is_empty() {
        warn!("INDO_API_KEY not set — Indodax market data uses public static token.");
    } else {
        info!("Indo credentials loaded (key: {}…{})", &indo_key[..4], &indo_key[indo_key.len()-4..]);
    }

    info!("Connecting to Redis at {}", redis_url);
    let client = redis::Client::open(redis_url)?;

    let mut con = loop {
        match client.get_multiplexed_tokio_connection().await {
            Ok(c)  => { info!("Redis connected."); break c; }
            Err(e) => { warn!("Redis connection failed: {}. Retrying in 2s…", e); sleep(Duration::from_secs(2)).await; }
        }
    };

    let mut indo_con = con.clone();

    // Indodax runs in a background task with its own exponential-backoff reconnect
    tokio::spawn(async move {
        let mut delay = Duration::from_secs(1);
        loop {
            match indo_stream_loop(&mut indo_con).await {
                Ok(())  => info!("Indodax WSS disconnected cleanly. Reconnecting in {:?}…", delay),
                Err(e)  => error!("Indodax WSS error: {}. Reconnecting in {:?}…", e, delay),
            }
            sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(30));
        }
    });

    // Tokocrypto main loop with exponential-backoff reconnect
    let mut delay = Duration::from_secs(1);
    loop {
        match stream_loop(&mut con, &wss_url).await {
            Ok(())  => info!("Toko WSS disconnected cleanly. Reconnecting in {:?}…", delay),
            Err(e)  => error!("Toko WSS error: {}. Reconnecting in {:?}…", e, delay),
        }
        sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(30));
    }
}
