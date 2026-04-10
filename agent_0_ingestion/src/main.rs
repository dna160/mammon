//! Agent 0: Live Price Feed — Tokocrypto + Indodax WebSocket Multiplexer
//!
//! Architecture:
//!   Tokocrypto combined stream subscribes to:
//!     • btcusdt@depth@100ms  — 100 ms-batched differential LOB (Engine D HFT)
//!     • btcusdt@aggTrade     — individual market order flow    (Engine D HFT)
//!     • btcusdt@bookTicker   — best bid/ask snapshot           (Engine B)
//!     • ethusdt@bookTicker   — best bid/ask snapshot           (Engine B)
//!     • solidr@bookTicker    — SOL/IDR best bid/ask            (Engine A)
//!     • btcidr@bookTicker    — BTC/IDR best bid/ask            (Engine A / C)
//!
//! Redis key schema:
//!   toko:btc_usdt:lob          → 10-level LOB JSON snapshot        (Engine C GET)
//!   toko:btc_usdt:lob:tick     → Pub/Sub channel, same JSON/tick   (Engine C SUB)
//!   toko:btc_usdt:ticker       → BTC/USDT best bid/ask ticker      (Engine B)
//!   toko:eth_usdt:ticker       → ETH/USDT best bid/ask ticker      (Engine B)
//!   toko:sol_idr:ticker        → SOL/IDR best bid/ask              (Engine A)
//!   toko:btc_idr:ticker        → BTC/IDR best bid/ask              (Engine A / C)
//!   indo:sol_idr:ask           → Indodax SOL/IDR orderbook         (Engine A)
//!   indo:btc_idr:ask           → Indodax BTC/IDR orderbook         (Engine A)
//!   telemetry:engine_d         → Engine D HFT snapshot (1 s cadence)

mod engine_d_hft;

use anyhow::Result;
use engine_d_hft::HFTEngine;
use futures_util::{SinkExt, StreamExt};
use redis::AsyncCommands;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

// ── Tokocrypto combined stream URL (HFT branch) ───────────────────────────────
//
// btcusdt@depth@100ms   — 100 ms-batched differential LOB updates.
//                         Feeds Engine D's DepthBook for micro-price / OBI.
//
// btcusdt@aggTrade      — individual market order fills.
//                         Provides TFI signal and fill-simulator price.
//
// btcusdt@bookTicker    — best bid/ask for Engine B ratio signal.
// ethusdt@bookTicker    — best bid/ask for Engine B ratio signal.
// solidr@bookTicker     — SOL/IDR best bid/ask for Engine A.
// btcidr@bookTicker     — BTC/IDR best bid/ask for Engine A + Engine C.
const TOKO_WSS_DEFAULT: &str = concat!(
    "wss://stream-cloud.tokocrypto.site/stream?streams=",
    "btcusdt@depth@100ms",
    "/btcusdt@aggTrade",
    "/btcusdt@bookTicker",
    "/ethusdt@bookTicker",
    "/solidr@bookTicker",
    "/btcidr@bookTicker"
);

// ── Indodax WebSocket ─────────────────────────────────────────────────────────
const INDO_WSS: &str = "wss://ws3.indodax.com/ws/";
// INDO_WS_TOKEN is loaded from the environment variable INDO_WS_TOKEN at runtime.
// It must NOT be hardcoded here. Set it in your .env file (never committed to git).

// ── In-memory limit-order book ────────────────────────────────────────────────

/// Maintains a live BTC/USDT LOB by applying real-time differential updates.
///
/// Keys are the price strings exactly as received from the exchange (avoids
/// floating-point equality issues in HashMap lookups).  Values are quantities.
struct DepthBook {
    bids: HashMap<String, f64>,
    asks: HashMap<String, f64>,
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

/// Real-time differential LOB update (btcusdt@depth@100ms).
#[derive(Deserialize)]
struct DepthDiff {
    #[serde(rename = "b")] bids: Vec<[String; 2]>,
    #[serde(rename = "a")] asks: Vec<[String; 2]>,
}

/// Aggregated trade (btcusdt@aggTrade).
/// is_buyer_maker = true  → the buyer was the passive maker; seller was aggressor.
/// is_buyer_maker = false → the seller was the passive maker; buyer was aggressor.
#[derive(Deserialize)]
struct AggTrade {
    /// Price in quote currency (USDT on Tokocrypto; IDR on btcidr streams).
    #[serde(rename = "p")] price: String,
    /// Trade quantity in base currency (BTC).
    #[serde(rename = "q")] qty: String,
    /// true = buy order was the maker (passive); aggressor was a seller.
    #[serde(rename = "m")] is_buyer_maker: bool,
}

/// BookTicker: best bid/ask snapshot.
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

fn millis_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
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

// ── Tokocrypto WebSocket loop (HFT branch) ────────────────────────────────────

async fn stream_loop(
    con: &mut redis::aio::MultiplexedConnection,
    wss_url: &str,
) -> Result<()> {
    info!("Connecting to Tokocrypto WSS (HFT): {}", wss_url);

    // Fresh state on every (re)connect — avoids stale data from previous session.
    let mut btcusdt_book = DepthBook::new();

    // Engine D — instantiated outside the tick loop per PRD §4 (Prompt 4).
    let mut engine_d = HFTEngine::init();

    // Last known aggTrade values; carried across depth-only ticks.
    let mut last_trade_price: f64 = 0.0;
    let mut last_trade_vol: f64   = 0.0;
    let mut last_is_buyer_maker: bool = true;

    // Telemetry: fire a Redis write every 1 s without blocking the tick loop.
    // tokio::spawn is intentionally here in the I/O loop — NOT inside tick().
    let mut last_telemetry = Instant::now();

    let url = url::Url::parse(wss_url)?;
    let (ws_stream, _) = connect_async(url).await?;
    info!("Tokocrypto WSS connected (HFT). Streaming btcusdt@depth@100ms + btcusdt@aggTrade.");

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

            // ── BTC/USDT 100 ms-batched depth diff (Engine D LOB) ────────────
            // Apply the diff to the in-memory DepthBook, extract top-of-book,
            // publish the LOB snapshot for Engine C, then call Engine D.tick().
            "btcusdt@depth@100ms" => {
                let diff: DepthDiff = match serde_json::from_value(combined.data) {
                    Ok(d)  => d,
                    Err(e) => { warn!("depth@100ms parse error: {}", e); continue; }
                };

                btcusdt_book.apply(&diff.bids, &diff.asks);
                if !btcusdt_book.is_ready() { continue; }

                let (bids, asks) = btcusdt_book.top(10);
                if bids.is_empty() || asks.is_empty() { continue; }

                // Periodic log: tick 1, then every 1 000 ticks.
                if btcusdt_book.tick_count == 1 || btcusdt_book.tick_count % 1_000 == 0 {
                    info!(
                        "[EngineD] LOB tick #{} — best bid={:.0} IDR  ask={:.0} IDR",
                        btcusdt_book.tick_count, bids[0].0, asks[0].0
                    );
                }

                // Publish LOB snapshot for Engine C.
                let json = lob_json(&bids, &asks);
                redis_set_and_publish(con, "toko:btc_usdt:lob", &json).await;

                // ── Engine D tick (synchronous, zero-allocation) ──────────────
                let now_ms = millis_now();
                let _out = engine_d.tick(
                    bids[0].0, bids[0].1,   // best_bid, bid_vol
                    asks[0].0, asks[0].1,   // best_ask, ask_vol
                    last_trade_price,
                    last_trade_vol,
                    last_is_buyer_maker,
                    now_ms,
                );
            }

            // ── BTC/USDT aggTrade (Engine D TFI + fill simulator) ────────────
            // Parse the market order, update last-trade cache, then immediately
            // drive Engine D with the current top-of-book (if ready).
            "btcusdt@aggTrade" => {
                let trade: AggTrade = match serde_json::from_value(combined.data) {
                    Ok(t)  => t,
                    Err(e) => { warn!("aggTrade parse error: {}", e); continue; }
                };

                let price = trade.price.parse::<f64>().unwrap_or(0.0);
                let qty   = trade.qty.parse::<f64>().unwrap_or(0.0);
                if price <= 0.0 || qty <= 0.0 { continue; }

                last_trade_price    = price;
                last_trade_vol      = qty;
                last_is_buyer_maker = trade.is_buyer_maker;

                // Fire Engine D immediately on every aggTrade if the book is ready.
                if btcusdt_book.is_ready() {
                    let (bids, asks) = btcusdt_book.top(1);
                    if !bids.is_empty() && !asks.is_empty() {
                        let now_ms = millis_now();
                        let _out = engine_d.tick(
                            bids[0].0, bids[0].1,
                            asks[0].0, asks[0].1,
                            price, qty,
                            trade.is_buyer_maker,
                            now_ms,
                        );
                    }
                }
            }

            // ── BTC/USDT bookTicker ──────────────────────────────────────────
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

                redis_set(con, "toko:btc_usdt:ticker", &ticker_json(ask, bid, bid_qty)).await;

                // Override top-of-book in the LOB snapshot with the freshest values.
                if btcusdt_book.is_ready() {
                    let (mut bids, mut asks) = btcusdt_book.top(10);
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

        // ── State 4: Telemetry Offload ────────────────────────────────────────
        // Every 1.0 s, snapshot Engine D state and push it to Redis from a
        // separate async task so the HFT tick loop is never blocked by I/O.
        // tokio::spawn lives HERE — not inside engine_d.tick().
        if last_telemetry.elapsed() >= Duration::from_secs(1) {
            let inv_btc    = engine_d.inventory_btc;
            let pnl_idr    = engine_d.pnl_idr;
            let trades     = engine_d.total_trades;
            let variance   = engine_d.variance;

            // Clone the multiplexed connection — cheap, no new TCP socket.
            let mut tel_con = con.clone();

            tokio::spawn(async move {
                // Serialize into a fixed-layout JSON string (no heap alloc in tick path).
                let payload = format!(
                    r#"{{"ts":{},"inventory_btc":{:.8},"pnl_idr":{:.2},"total_trades":{},"variance":{:.10}}}"#,
                    millis_now(), inv_btc, pnl_idr, trades, variance
                );
                if let Err(e) = tel_con.set::<_, _, ()>("telemetry:engine_d", &payload).await {
                    // Non-fatal — telemetry failure must never crash the HFT loop.
                    warn!("Engine D telemetry Redis write failed: {}", e);
                }
            });

            last_telemetry = Instant::now();
        }
    }

    info!("Tokocrypto WSS stream loop ended (HFT).");
    Ok(())
}

// ── Indodax WebSocket loop ────────────────────────────────────────────────────

async fn indo_stream_loop(
    con: &mut redis::aio::MultiplexedConnection,
    indo_ws_token: &str,
) -> Result<()> {
    info!("Connecting to Indodax WSS: {}", INDO_WSS);

    let url = url::Url::parse(INDO_WSS)?;
    let (ws_stream, _) = connect_async(url).await?;
    info!("Indodax WSS connected. Authenticating…");

    let (mut writer, mut reader) = ws_stream.split();

    let auth = serde_json::json!({ "params": { "token": indo_ws_token }, "id": 1 });
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

        // Auth ACK → subscribe to orderbook channels.
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

    // ── Credential loading ────────────────────────────────────────────────────
    // All secrets come from environment variables only — never from source code.
    // The .env file is gitignored; secrets must never be committed.

    let toko_key    = std::env::var("TOKO_API_KEY").unwrap_or_default();
    let toko_secret = std::env::var("TOKO_API_SECRET").unwrap_or_default();
    let indo_key    = std::env::var("INDO_API_KEY").unwrap_or_default();
    let _indo_secret = std::env::var("INDO_API_SECRET").unwrap_or_default();

    // INDO_WS_TOKEN: Indodax WebSocket JWT.  Must be set; binary exits if absent.
    let indo_ws_token = std::env::var("INDO_WS_TOKEN").unwrap_or_default();

    let redis_url   = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let wss_url     = std::env::var("TOKO_STREAM_URL")
        .unwrap_or_else(|_| TOKO_WSS_DEFAULT.to_string());

    // ── Credential validation ─────────────────────────────────────────────────
    // Log a redacted prefix/suffix only (never the full key).
    // Use saturating slice to avoid panics on short/empty values.
    fn redact(s: &str) -> String {
        let n = s.len();
        if n < 8 { return "[too short]".to_string(); }
        format!("{}…{}", &s[..4], &s[n - 4..])
    }

    if toko_key.is_empty() || toko_secret.is_empty() {
        warn!("TOKO_API_KEY / TOKO_API_SECRET not set — market-data WebSocket works without auth, but order execution will fail.");
    } else {
        info!("Toko credentials loaded (key: {})", redact(&toko_key));
    }

    if indo_key.is_empty() {
        warn!("INDO_API_KEY not set.");
    } else {
        info!("Indo credentials loaded (key: {})", redact(&indo_key));
    }

    if indo_ws_token.is_empty() {
        // Indodax WebSocket requires a token; without it we cannot subscribe.
        // Log a warning but continue — the background task will fail gracefully.
        warn!("INDO_WS_TOKEN not set — Indodax WebSocket will not authenticate.");
    } else {
        info!("Indodax WS token loaded ({} bytes)", indo_ws_token.len());
    }

    // ── Redis connection ──────────────────────────────────────────────────────
    // REDIS_URL may include a password: redis://:password@host:port
    // e.g. redis://:s3cr3t@redis_hft:6379
    // The URL is never logged to avoid leaking credentials.
    info!("Connecting to Redis…");
    let client = redis::Client::open(redis_url)?;

    let mut con = loop {
        match client.get_multiplexed_tokio_connection().await {
            Ok(c)  => { info!("Redis connected."); break c; }
            Err(e) => { warn!("Redis connection failed: {}. Retrying in 2s…", e); sleep(Duration::from_secs(2)).await; }
        }
    };

    let mut indo_con = con.clone();

    // Indodax runs in a background task with its own exponential-backoff reconnect.
    tokio::spawn(async move {
        let mut delay = Duration::from_secs(1);
        loop {
            match indo_stream_loop(&mut indo_con, &indo_ws_token).await {
                Ok(())  => info!("Indodax WSS disconnected cleanly. Reconnecting in {:?}…", delay),
                Err(e)  => error!("Indodax WSS error: {}. Reconnecting in {:?}…", e, delay),
            }
            sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(30));
        }
    });

    // Tokocrypto main loop with exponential-backoff reconnect.
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
