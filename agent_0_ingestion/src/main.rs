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
mod toko_rest;

use anyhow::Result;
use engine_d_hft::HFTEngine;
use toko_rest::{qty_from_balance, round_price, TokoClient, SIDE_BUY, SIDE_SELL};
use futures_util::{SinkExt, StreamExt};
use redis::AsyncCommands;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

// ── Order manager command channel ─────────────────────────────────────────────

/// Command sent from the tick loop to the order manager task.
/// The tick loop never blocks on REST I/O — it just sends a command and continues.
#[derive(Debug)]
enum OrderCmd {
    /// Engine D produced new optimal quotes.  Manager decides whether to requote.
    Requote {
        bid_price: f64,
        ask_price: f64,
        bid_qty:   f64,
        ask_qty:   f64,
    },
    /// Engine switched to SKEW-BID: cancel ask, hold bid at best_bid.
    SkewBid { bid_price: f64, bid_qty: f64 },
    /// Engine switched to SKEW-ASK: cancel bid, hold ask at best_ask.
    SkewAsk { ask_price: f64, ask_qty: f64 },
    /// Cancel all open orders (called on reconnect / shutdown).
    CancelAll,
    /// Immediate fill poll — triggered when aggTrade price crosses an open quote.
    /// Non-throttled: skips the 2 s Requote poll cooldown.
    CheckFills,
    /// Aggressive taker BUY limit — priced above the ask to guarantee immediate fill.
    TakerBuy { price: f64, qty: f64 },
    /// Aggressive taker SELL limit — priced below the bid to guarantee immediate fill.
    TakerSell { price: f64, qty: f64 },
}

// ── Tokocrypto combined stream URL (HFT branch) ───────────────────────────────
//
// Tokocrypto only provides @depth and @aggTrade streams for USDT pairs.
// IDR pairs support @bookTicker only.
//
// Engine D hybrid strategy (all formulas run in IDR):
//   btcusdt@depth@100ms  — LOB volume structure for OBI.
//                          BTC volumes are currency-agnostic (same physical orders).
//   btcusdt@aggTrade     — BTC trade volumes for TFI (qty is in BTC, not USDT).
//   btcidr@bookTicker    — IDR prices; drives Engine D tick on every ToB change.
//                          Overrides the USDT prices from the depth stream so that
//                          micro-price, reservation price, and spread are all in IDR.
//
// btcusdt@bookTicker    — Engine B ratio signal (USDT pair).
// ethusdt@bookTicker    — Engine B ratio signal (USDT pair).
// solidr@bookTicker     — SOL/IDR for Engine A.
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

// Fee logic is now entirely inside toko_rest::TradeFill (trading_fee_idr,
// pph_fee_idr, ppn_fee_idr, cfx_fee_idr, total_fee_idr).
// The commission_to_idr helper has been superseded by TradeFill.total_fee_idr.

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

// ── Order manager task ────────────────────────────────────────────────────────
//
// Runs in its own tokio task.  Receives OrderCmd from the tick loop via a
// bounded channel and executes REST calls asynchronously, completely decoupled
// from the hot WebSocket path.
//
// Requote policy:
//   • Only replace quotes when price shifted ≥ REQUOTE_BPS basis-points.
//   • Minimum REQUOTE_COOLDOWN_MS between consecutive requotes to respect
//     Tokocrypto rate limits (10 orders per 10s window).
//   • Balance is refreshed every BALANCE_REFRESH_S seconds so qty is always
//     grounded in real available funds.

const REQUOTE_BPS:         f64 = 10.0;   // 0.10 % price shift triggers requote
const REQUOTE_COOLDOWN_MS: u64 = 3_000;  // 3 s minimum between requotes
const BALANCE_REFRESH_S:   u64 = 30;     // refresh balances every 30 s
const QUOTE_FRACTION:      f64 = 0.45;   // use 45 % of available balance per side

async fn order_manager(
    client:  std::sync::Arc<TokoClient>,
    mut rx:  mpsc::Receiver<OrderCmd>,
    mut redis_con: redis::aio::MultiplexedConnection,
) {
    // ── State ─────────────────────────────────────────────────────────────────
    let mut bid_id:   Option<u64> = None;
    let mut ask_id:   Option<u64> = None;
    let mut last_bid: f64         = 0.0;
    let mut last_ask: f64         = 0.0;
    let mut last_requote = std::time::Instant::now()
        .checked_sub(Duration::from_millis(REQUOTE_COOLDOWN_MS))
        .unwrap_or(std::time::Instant::now());
    let mut btc_free:  f64 = 0.0;
    let mut idr_free:  f64 = 0.0;
    let mut last_bal_refresh = std::time::Instant::now()
        .checked_sub(Duration::from_secs(BALANCE_REFRESH_S + 1))
        .unwrap_or(std::time::Instant::now());

    // Real fill tracking — polled from Tokocrypto trade history.
    let mut last_trade_id:    u64  = 0;    // cursor: last processed tradeId
    let mut real_pnl_idr:     f64  = 0.0; // cumulative IDR P&L from real fills
    let mut real_inventory:   f64  = 0.0; // net BTC from real fills
    let mut real_total_trades: u64 = 0;   // confirmed fill count
    let mut last_fill_poll = std::time::Instant::now()
        .checked_sub(Duration::from_secs(10))
        .unwrap_or(std::time::Instant::now());

    info!("[OrderMgr] started — cancelling any stale BTC_IDR orders…");
    if let Err(e) = client.cancel_all_open().await {
        error!("[OrderMgr] startup cancel_all error: {}", e);
    }

    // Seed the trade cursor to "now" so we don't replay historical fills.
    match client.fetch_trades(0, 1).await {
        Ok(t) if !t.is_empty() => {
            last_trade_id = t.last().unwrap().trade_id;
            info!("[OrderMgr] Trade cursor seeded at tradeId={}", last_trade_id);
        }
        _ => info!("[OrderMgr] No prior trades found — starting fresh."),
    }

    // ── Helper: refresh balances ───────────────────────────────────────────────
    async fn refresh_bal(
        client: &TokoClient,
        btc: &mut f64, idr: &mut f64,
        last: &mut std::time::Instant,
    ) {
        if last.elapsed().as_secs() < BALANCE_REFRESH_S { return; }
        match client.get_balances().await {
            Ok(b) => {
                *btc = b.btc_free;
                *idr = b.idr_free;
                *last = std::time::Instant::now();
                info!("[OrderMgr] balance — BTC free={:.8}  IDR free={:.2}", b.btc_free, b.idr_free);
            }
            Err(e) => error!("[OrderMgr] balance refresh error: {}", e),
        }
    }

    // ── Helper: should we requote? ─────────────────────────────────────────────
    fn moved_enough(new_price: f64, old_price: f64) -> bool {
        if old_price == 0.0 { return true; }
        ((new_price - old_price).abs() / old_price) * 10_000.0 >= REQUOTE_BPS
    }

    // ── Main receive loop ──────────────────────────────────────────────────────
    while let Some(cmd) = rx.recv().await {
        match cmd {
            OrderCmd::CancelAll => {
                info!("[OrderMgr] CancelAll received");
                if let Some(id) = bid_id.take() { let _ = client.cancel_order(id).await; }
                if let Some(id) = ask_id.take() { let _ = client.cancel_order(id).await; }
                last_bid = 0.0; last_ask = 0.0;
            }

            OrderCmd::SkewBid { bid_price, bid_qty } => {
                // Cancel ask side; place bid at best_bid if not already there.
                if let Some(id) = ask_id.take() {
                    let _ = client.cancel_order(id).await;
                    last_ask = 0.0;
                }
                if moved_enough(bid_price, last_bid)
                    && last_requote.elapsed().as_millis() as u64 >= REQUOTE_COOLDOWN_MS
                {
                    if let Some(id) = bid_id.take() { let _ = client.cancel_order(id).await; }
                    refresh_bal(&client, &mut btc_free, &mut idr_free, &mut last_bal_refresh).await;
                    let qty = bid_qty.min(qty_from_balance(idr_free, bid_price, QUOTE_FRACTION));
                    if qty > 0.0 {
                        match client.place_limit_order(SIDE_BUY, bid_price, qty).await {
                            Ok(id) => { bid_id = Some(id); last_bid = bid_price; last_requote = std::time::Instant::now(); }
                            Err(e) => error!("[OrderMgr] SkewBid place error: {}", e),
                        }
                    }
                }
            }

            OrderCmd::SkewAsk { ask_price, ask_qty } => {
                // Cancel bid side; place ask at best_ask if not already there.
                if let Some(id) = bid_id.take() {
                    let _ = client.cancel_order(id).await;
                    last_bid = 0.0;
                }
                if moved_enough(ask_price, last_ask)
                    && last_requote.elapsed().as_millis() as u64 >= REQUOTE_COOLDOWN_MS
                {
                    if let Some(id) = ask_id.take() { let _ = client.cancel_order(id).await; }
                    refresh_bal(&client, &mut btc_free, &mut idr_free, &mut last_bal_refresh).await;
                    // ASK: selling BTC — qty denominated in BTC, not derived from IDR.
                    let available_ask = toko_rest::round_qty(btc_free * QUOTE_FRACTION);
                    let qty = ask_qty.min(available_ask);
                    if qty >= toko_rest::LOT_STEP_BTC && qty * ask_price >= toko_rest::MIN_NOTIONAL_IDR {
                        match client.place_limit_order(SIDE_SELL, ask_price, qty).await {
                            Ok(id) => { ask_id = Some(id); last_ask = ask_price; last_requote = std::time::Instant::now(); }
                            Err(e) => error!("[OrderMgr] SkewAsk place error: {}", e),
                        }
                    } else {
                        warn!("[OrderMgr] SkewAsk insufficient BTC: free={:.8} qty={:.8}", btc_free, qty);
                    }
                }
            }

            OrderCmd::Requote { bid_price, ask_price, bid_qty, ask_qty } => {
                let bid_moved = moved_enough(bid_price, last_bid);
                let ask_moved = moved_enough(ask_price, last_ask);
                if (!bid_moved && !ask_moved)
                    || (last_requote.elapsed().as_millis() as u64) < REQUOTE_COOLDOWN_MS
                {
                    continue; // nothing to do yet
                }

                refresh_bal(&client, &mut btc_free, &mut idr_free, &mut last_bal_refresh).await;

                // Cancel stale bid.
                if bid_moved {
                    if let Some(id) = bid_id.take() { let _ = client.cancel_order(id).await; }
                    let qty = bid_qty.min(qty_from_balance(idr_free, bid_price, QUOTE_FRACTION));
                    if qty > 0.0 {
                        match client.place_limit_order(SIDE_BUY, bid_price, qty).await {
                            Ok(id) => { bid_id = Some(id); last_bid = bid_price; }
                            Err(e) => error!("[OrderMgr] Requote bid error: {}", e),
                        }
                    }
                }

                // Cancel stale ask.
                if ask_moved {
                    if let Some(id) = ask_id.take() { let _ = client.cancel_order(id).await; }
                    // ASK: selling BTC — qty in BTC, not derived from IDR.
                    let available_ask = toko_rest::round_qty(btc_free * QUOTE_FRACTION);
                    let qty = ask_qty.min(available_ask);
                    if qty >= toko_rest::LOT_STEP_BTC && qty * ask_price >= toko_rest::MIN_NOTIONAL_IDR {
                        match client.place_limit_order(SIDE_SELL, ask_price, qty).await {
                            Ok(id) => { ask_id = Some(id); last_ask = ask_price; }
                            Err(e) => error!("[OrderMgr] Requote ask error: {}", e),
                        }
                    } else {
                        warn!("[OrderMgr] Requote ask insufficient BTC: free={:.8} qty={:.8}", btc_free, qty);
                    }
                }

                last_requote = std::time::Instant::now();

                // Publish order state to Redis for the dashboard.
                let payload = format!(
                    r#"{{"ts":{},"bid_id":{},"ask_id":{},"bid_price":{:.0},"ask_price":{:.0},"btc_free":{:.8},"idr_free":{:.2},"real_pnl_idr":{:.2},"real_inventory":{:.8},"real_trades":{}}}"#,
                    millis_now(),
                    bid_id.map(|v| v.to_string()).unwrap_or_else(|| "null".into()),
                    ask_id.map(|v| v.to_string()).unwrap_or_else(|| "null".into()),
                    last_bid, last_ask, btc_free, idr_free,
                    real_pnl_idr, real_inventory, real_total_trades
                );
                let _ = redis_con.set::<_, _, ()>("engine_d:orders", &payload).await;

                // Poll for new real fills after every requote (rate-limited to 2 s).
                if last_fill_poll.elapsed().as_secs() >= 2 {
                    match client.fetch_trades(last_trade_id, 50).await {
                        Ok(fills) => {
                            for fill in &fills {
                                if fill.trade_id <= last_trade_id { continue; }
                                last_trade_id = fill.trade_id;
                                let notional = if fill.quote_qty > 0.0 { fill.quote_qty } else { fill.price * fill.qty };
                                if fill.is_buyer {
                                    real_inventory += fill.qty;
                                    real_pnl_idr   -= notional + fill.total_fee_idr;
                                } else {
                                    real_inventory -= fill.qty;
                                    real_pnl_idr   += notional - fill.total_fee_idr;
                                }
                                real_total_trades += 1;
                                info!(
                                    "[RealFill] tradeId={} {} {:.8} BTC @ {:.0} IDR  notional={:.0}  \
                                     fees=trading:{:.2}+pph:{:.2}+ppn:{:.2}+cfx:{:.2}={:.2}  pnl={:.2}  inv={:+.8}",
                                    fill.trade_id,
                                    if fill.is_buyer { "BUY " } else { "SELL" },
                                    fill.qty, fill.price, notional,
                                    fill.trading_fee_idr, fill.pph_fee_idr, fill.ppn_fee_idr, fill.cfx_fee_idr, fill.total_fee_idr,
                                    real_pnl_idr, real_inventory
                                );
                                let fp = format!(
                                    r#"{{"ts":{},"tradeId":{},"orderId":{},"side":"{}","price":{:.0},"qty":{:.8},"notional":{:.0},"trading_fee":{:.2},"pph_fee":{:.2},"ppn_fee":{:.2},"cfx_fee":{:.2},"total_fee":{:.2},"pnl_running":{:.2},"inventory":{:.8},"total_trades":{}}}"#,
                                    fill.time_ms, fill.trade_id, fill.order_id,
                                    if fill.is_buyer { "BUY" } else { "SELL" },
                                    fill.price, fill.qty, notional,
                                    fill.trading_fee_idr, fill.pph_fee_idr, fill.ppn_fee_idr, fill.cfx_fee_idr, fill.total_fee_idr,
                                    real_pnl_idr, real_inventory, real_total_trades
                                );
                                let _ = redis_con.set::<_, _, ()>("engine_d:last_fill", &fp).await;
                            }
                            last_fill_poll = std::time::Instant::now();
                        }
                        Err(e) => warn!("[OrderMgr] fill poll error: {}", e),
                    }
                }
            }

            // ── Immediate fill check (aggTrade cross) ─────────────────────
            OrderCmd::CheckFills => {
                match client.fetch_trades(last_trade_id, 20).await {
                    Ok(fills) => {
                        for fill in &fills {
                            if fill.trade_id <= last_trade_id { continue; }
                            last_trade_id = fill.trade_id;
                            let notional = if fill.quote_qty > 0.0 { fill.quote_qty } else { fill.price * fill.qty };
                            if fill.is_buyer {
                                real_inventory += fill.qty;
                                real_pnl_idr   -= notional + fill.total_fee_idr;
                            } else {
                                real_inventory -= fill.qty;
                                real_pnl_idr   += notional - fill.total_fee_idr;
                            }
                            real_total_trades += 1;
                            info!(
                                "[RealFill cross] tradeId={} {} {:.8} BTC @ {:.0} IDR  notional={:.0}  \
                                 fees=trading:{:.2}+pph:{:.2}+ppn:{:.2}+cfx:{:.2}={:.2}  pnl={:.2}  inv={:+.8}",
                                fill.trade_id,
                                if fill.is_buyer { "BUY " } else { "SELL" },
                                fill.qty, fill.price, notional,
                                fill.trading_fee_idr, fill.pph_fee_idr, fill.ppn_fee_idr, fill.cfx_fee_idr, fill.total_fee_idr,
                                real_pnl_idr, real_inventory
                            );
                            let fp = format!(
                                r#"{{"ts":{},"tradeId":{},"orderId":{},"side":"{}","price":{:.0},"qty":{:.8},"notional":{:.0},"trading_fee":{:.2},"pph_fee":{:.2},"ppn_fee":{:.2},"cfx_fee":{:.2},"total_fee":{:.2},"pnl_running":{:.2},"inventory":{:.8},"total_trades":{},"source":"aggTrade_cross"}}"#,
                                fill.time_ms, fill.trade_id, fill.order_id,
                                if fill.is_buyer { "BUY" } else { "SELL" },
                                fill.price, fill.qty, notional,
                                fill.trading_fee_idr, fill.pph_fee_idr, fill.ppn_fee_idr, fill.cfx_fee_idr, fill.total_fee_idr,
                                real_pnl_idr, real_inventory, real_total_trades
                            );
                            let _ = redis_con.set::<_, _, ()>("engine_d:last_fill", &fp).await;
                        }
                        last_fill_poll = std::time::Instant::now();
                    }
                    Err(e) => warn!("[OrderMgr] CheckFills poll error: {}", e),
                }
            }

            // ── Aggressive taker BUY ──────────────────────────────────────
            // Prices passed in are already above the ask — this fires as a
            // taker order and should fill immediately.
            OrderCmd::TakerBuy { price, qty } => {
                if (last_requote.elapsed().as_millis() as u64) < REQUOTE_COOLDOWN_MS {
                    // Respect exchange rate limits even for taker orders.
                    continue;
                }
                refresh_bal(&client, &mut btc_free, &mut idr_free, &mut last_bal_refresh).await;
                let actual_qty = qty.min(qty_from_balance(idr_free, price, QUOTE_FRACTION));
                if actual_qty < toko_rest::LOT_STEP_BTC || actual_qty * price < toko_rest::MIN_NOTIONAL_IDR {
                    warn!("[OrderMgr] TakerBuy insufficient balance: idr_free={:.0} qty={:.8}", idr_free, actual_qty);
                    continue;
                }
                info!("[OrderMgr] TakerBuy: aggressive LIMIT BUY {:.5} BTC @ {:.0} IDR", actual_qty, price);
                match client.place_limit_order(SIDE_BUY, price, actual_qty).await {
                    Ok(id) => {
                        // Keep track of this order; it will likely fill before the next poll.
                        if bid_id.is_none() { bid_id = Some(id); }
                        last_bid = price;
                        last_requote = std::time::Instant::now();
                        // Wait briefly then poll for the fill.
                        tokio::time::sleep(Duration::from_millis(800)).await;
                        match client.fetch_trades(last_trade_id, 20).await {
                            Ok(fills) => {
                                for fill in &fills {
                                    if fill.trade_id <= last_trade_id { continue; }
                                    last_trade_id = fill.trade_id;
                                    let notional = if fill.quote_qty > 0.0 { fill.quote_qty } else { fill.price * fill.qty };
                                    real_inventory += fill.qty;
                                    real_pnl_idr   -= notional + fill.total_fee_idr;
                                    real_total_trades += 1;
                                    info!(
                                        "[RealFill TakerBuy] tradeId={} BUY  {:.8} BTC @ {:.0} IDR  notional={:.0}  \
                                         fees=trading:{:.2}+pph:{:.2}+ppn:{:.2}+cfx:{:.2}={:.2}  pnl={:.2}  inv={:+.8}",
                                        fill.trade_id, fill.qty, fill.price, notional,
                                        fill.trading_fee_idr, fill.pph_fee_idr, fill.ppn_fee_idr, fill.cfx_fee_idr, fill.total_fee_idr,
                                        real_pnl_idr, real_inventory
                                    );
                                    let fp = format!(
                                        r#"{{"ts":{},"tradeId":{},"orderId":{},"side":"BUY","price":{:.0},"qty":{:.8},"notional":{:.0},"trading_fee":{:.2},"pph_fee":{:.2},"ppn_fee":{:.2},"cfx_fee":{:.2},"total_fee":{:.2},"pnl_running":{:.2},"inventory":{:.8},"total_trades":{},"source":"taker_buy"}}"#,
                                        fill.time_ms, fill.trade_id, fill.order_id,
                                        fill.price, fill.qty, notional,
                                        fill.trading_fee_idr, fill.pph_fee_idr, fill.ppn_fee_idr, fill.cfx_fee_idr, fill.total_fee_idr,
                                        real_pnl_idr, real_inventory, real_total_trades
                                    );
                                    let _ = redis_con.set::<_, _, ()>("engine_d:last_fill", &fp).await;
                                }
                                last_fill_poll = std::time::Instant::now();
                            }
                            Err(e) => warn!("[OrderMgr] TakerBuy fill poll error: {}", e),
                        }
                    }
                    Err(e) => error!("[OrderMgr] TakerBuy place error: {}", e),
                }
            }

            // ── Aggressive taker SELL ─────────────────────────────────────
            // Price is already below the bid — fills immediately as a taker.
            OrderCmd::TakerSell { price, qty } => {
                if (last_requote.elapsed().as_millis() as u64) < REQUOTE_COOLDOWN_MS {
                    continue;
                }
                refresh_bal(&client, &mut btc_free, &mut idr_free, &mut last_bal_refresh).await;
                let available_ask = toko_rest::round_qty(btc_free * QUOTE_FRACTION);
                let actual_qty = qty.min(available_ask);
                if actual_qty < toko_rest::LOT_STEP_BTC || actual_qty * price < toko_rest::MIN_NOTIONAL_IDR {
                    warn!("[OrderMgr] TakerSell insufficient balance: btc_free={:.8} qty={:.8}", btc_free, actual_qty);
                    continue;
                }
                info!("[OrderMgr] TakerSell: aggressive LIMIT SELL {:.5} BTC @ {:.0} IDR", actual_qty, price);
                match client.place_limit_order(SIDE_SELL, price, actual_qty).await {
                    Ok(id) => {
                        if ask_id.is_none() { ask_id = Some(id); }
                        last_ask = price;
                        last_requote = std::time::Instant::now();
                        tokio::time::sleep(Duration::from_millis(800)).await;
                        match client.fetch_trades(last_trade_id, 20).await {
                            Ok(fills) => {
                                for fill in &fills {
                                    if fill.trade_id <= last_trade_id { continue; }
                                    last_trade_id = fill.trade_id;
                                    let notional = if fill.quote_qty > 0.0 { fill.quote_qty } else { fill.price * fill.qty };
                                    real_inventory -= fill.qty;
                                    real_pnl_idr   += notional - fill.total_fee_idr;
                                    real_total_trades += 1;
                                    info!(
                                        "[RealFill TakerSell] tradeId={} SELL {:.8} BTC @ {:.0} IDR  notional={:.0}  \
                                         fees=trading:{:.2}+pph:{:.2}+ppn:{:.2}+cfx:{:.2}={:.2}  pnl={:.2}  inv={:+.8}",
                                        fill.trade_id, fill.qty, fill.price, notional,
                                        fill.trading_fee_idr, fill.pph_fee_idr, fill.ppn_fee_idr, fill.cfx_fee_idr, fill.total_fee_idr,
                                        real_pnl_idr, real_inventory
                                    );
                                    let fp = format!(
                                        r#"{{"ts":{},"tradeId":{},"orderId":{},"side":"SELL","price":{:.0},"qty":{:.8},"notional":{:.0},"trading_fee":{:.2},"pph_fee":{:.2},"ppn_fee":{:.2},"cfx_fee":{:.2},"total_fee":{:.2},"pnl_running":{:.2},"inventory":{:.8},"total_trades":{},"source":"taker_sell"}}"#,
                                        fill.time_ms, fill.trade_id, fill.order_id,
                                        fill.price, fill.qty, notional,
                                        fill.trading_fee_idr, fill.pph_fee_idr, fill.ppn_fee_idr, fill.cfx_fee_idr, fill.total_fee_idr,
                                        real_pnl_idr, real_inventory, real_total_trades
                                    );
                                    let _ = redis_con.set::<_, _, ()>("engine_d:last_fill", &fp).await;
                                }
                                last_fill_poll = std::time::Instant::now();
                            }
                            Err(e) => warn!("[OrderMgr] TakerSell fill poll error: {}", e),
                        }
                    }
                    Err(e) => error!("[OrderMgr] TakerSell place error: {}", e),
                }
            }
        } // end match cmd
    } // end while

    info!("[OrderMgr] channel closed — cancelling remaining orders…");
    if let Some(id) = bid_id { let _ = client.cancel_order(id).await; }
    if let Some(id) = ask_id { let _ = client.cancel_order(id).await; }
}

// ── Tokocrypto WebSocket loop (HFT branch) ────────────────────────────────────

async fn stream_loop(
    con: &mut redis::aio::MultiplexedConnection,
    wss_url: &str,
    order_tx: mpsc::Sender<OrderCmd>,
) -> Result<()> {
    info!("Connecting to Tokocrypto WSS (HFT): {}", wss_url);
    // On every reconnect cancel all stale orders.
    let _ = order_tx.try_send(OrderCmd::CancelAll);

    // Fresh state on every (re)connect — avoids stale data from previous session.
    let mut btcusdt_book = DepthBook::new();

    // Engine D — instantiated outside the tick loop per PRD §4 (Prompt 4).
    let mut engine_d = HFTEngine::init();

    // Last known aggTrade values (volumes in BTC — currency-agnostic).
    // Price is NOT used from aggTrade; IDR prices come from btcidr@bookTicker.
    let mut last_trade_vol: f64       = 0.0;
    let mut last_is_buyer_maker: bool = true;

    // Last IDR prices from btcidr@bookTicker — the authoritative price source.
    let mut last_idr_bid: f64     = 0.0;
    let mut last_idr_ask: f64     = 0.0;
    let mut last_idr_bid_vol: f64 = 0.0;
    let mut last_idr_ask_vol: f64 = 0.0;

    // Diagnostic counters — how many times each stream has fired.
    let mut tick_count_bookidr: u64  = 0;
    let mut tick_count_aggtrade: u64 = 0;

    // Taker execution state.
    // requested_bid / requested_ask: the prices of our most recently dispatched
    // passive quotes — used to detect when an aggTrade crosses our open order.
    // last_taker_ms: epoch-ms of the most recent TakerBuy / TakerSell dispatch
    // (prevents taker rate-limit bursts — min 8 s between taker orders).
    let mut requested_bid: f64 = 0.0;
    let mut requested_ask: f64 = 0.0;
    let mut last_taker_ms: u64 = 0;

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

            // ── BTC/USDT 100 ms-batched depth (LOB volume structure) ────────
            // Tokocrypto does not offer btcidr@depth. We use the USDT LOB for
            // OBI volume ratios — BTC quantities are currency-agnostic.
            // Prices from this stream are NOT passed to Engine D; IDR prices
            // come exclusively from btcidr@bookTicker below.
            "btcusdt@depth@100ms" => {
                let diff: DepthDiff = match serde_json::from_value(combined.data) {
                    Ok(d)  => d,
                    Err(e) => { warn!("depth@100ms parse error: {}", e); continue; }
                };

                btcusdt_book.apply(&diff.bids, &diff.asks);

                if btcusdt_book.tick_count == 1 || btcusdt_book.tick_count % 1_000 == 0 {
                    let (bids, asks) = btcusdt_book.top(1);
                    if !bids.is_empty() && !asks.is_empty() {
                        info!(
                            "[EngineD] LOB volume tick #{} — bid_vol={:.5} BTC  ask_vol={:.5} BTC  (IDR prices from bookTicker)",
                            btcusdt_book.tick_count, bids[0].1, asks[0].1
                        );
                    }
                }

                // Publish LOB for Engine C (USDT snapshot, unchanged from baseline).
                if btcusdt_book.is_ready() {
                    let (bids, asks) = btcusdt_book.top(10);
                    if !bids.is_empty() && !asks.is_empty() {
                        let json = lob_json(&bids, &asks);
                        redis_set_and_publish(con, "toko:btc_usdt:lob", &json).await;
                    }
                }
                // Engine D fires from btcidr@bookTicker, not here.
            }

            // ── BTC/USDT aggTrade (Engine D TFI volume signal) ──────────────
            // aggTrade qty is in BTC — no currency conversion needed for TFI.
            // is_buyer_maker determines the sign of each flow contribution.
            // The USDT price is discarded; fill detection uses IDR bookTicker prices.
            "btcusdt@aggTrade" => {
                let trade: AggTrade = match serde_json::from_value(combined.data) {
                    Ok(t)  => t,
                    Err(e) => { warn!("aggTrade parse error: {}", e); continue; }
                };

                let qty = trade.qty.parse::<f64>().unwrap_or(0.0);
                if qty <= 0.0 { continue; }

                // Store volume and direction; price (USDT) deliberately ignored.
                last_trade_vol      = qty;
                last_is_buyer_maker = trade.is_buyer_maker;
                tick_count_aggtrade += 1;

                // Fire Engine D immediately with the latest IDR prices if available.
                // Uses USDT LOB volumes for OBI — IDR prices from last bookTicker tick.
                // btcidr@bookTicker fires very rarely on Tokocrypto (~once per minute)
                // so aggTrade is the primary Engine D tick driver.
                if last_idr_bid > 0.0 && last_idr_ask > 0.0 {
                    let (bid_vol, ask_vol) = if btcusdt_book.is_ready() {
                        let (bids, asks) = btcusdt_book.top(1);
                        let bv = bids.first().map(|b| b.1).unwrap_or(last_idr_bid_vol);
                        let av = asks.first().map(|a| a.1).unwrap_or(last_idr_ask_vol);
                        (bv, av)
                    } else {
                        (last_idr_bid_vol, last_idr_ask_vol)
                    };
                    let now_ms = millis_now();
                    // Pass IDR mid as the "trade price" for the fill simulator —
                    // best approximation when no IDR aggTrade stream exists.
                    let idr_mid = (last_idr_bid + last_idr_ask) * 0.5;
                    let agg_out = engine_d.tick(
                        last_idr_bid, bid_vol,
                        last_idr_ask, ask_vol,
                        idr_mid, qty,
                        trade.is_buyer_maker,
                        now_ms,
                    );

                    // ── Real-time fill detection via aggTrade cross ───────────
                    // When the market trades through our open passive quote price
                    // it is very likely our limit order was hit.  Signal the
                    // order manager to poll trade history immediately (no 2 s wait).
                    let cross_bid = requested_bid > 0.0 && idr_mid <= requested_bid;
                    let cross_ask = requested_ask > 0.0 && idr_mid >= requested_ask;
                    if cross_bid || cross_ask {
                        let _ = order_tx.try_send(OrderCmd::CheckFills);
                    }

                    // ── Strong-signal taker execution ─────────────────────────
                    // When OBI and TFI both agree strongly on direction we place
                    // an aggressive limit order to capture the momentum move.
                    // Minimum 8 s between consecutive taker dispatches.
                    let taker_elapsed_ms = millis_now().saturating_sub(last_taker_ms);
                    if agg_out.warm_ticks >= 20
                        && taker_elapsed_ms >= 8_000
                        && last_idr_ask > 0.0
                        && last_idr_bid > 0.0
                    {
                        if agg_out.obi > 0.85 && agg_out.tfi > 0.002 {
                            // Cross the ask + 0.05 % to guarantee taker fill.
                            let taker_price = round_price(last_idr_ask * 1.0005);
                            let _ = order_tx.try_send(OrderCmd::TakerBuy {
                                price: taker_price,
                                qty:   toko_rest::LOT_STEP_BTC * 2.0,
                            });
                            last_taker_ms = millis_now();
                            info!(
                                "[EngineD agg] TAKER BUY signal: OBI={:+.4} TFI={:+.6} price={:.0} IDR",
                                agg_out.obi, agg_out.tfi, taker_price
                            );
                        } else if agg_out.obi < -0.85 && agg_out.tfi < -0.002 {
                            // Cross the bid − 0.05 % to guarantee taker fill.
                            let taker_price = round_price(last_idr_bid * 0.9995);
                            let _ = order_tx.try_send(OrderCmd::TakerSell {
                                price: taker_price,
                                qty:   toko_rest::LOT_STEP_BTC * 2.0,
                            });
                            last_taker_ms = millis_now();
                            info!(
                                "[EngineD agg] TAKER SELL signal: OBI={:+.4} TFI={:+.6} price={:.0} IDR",
                                agg_out.obi, agg_out.tfi, taker_price
                            );
                        }
                    }

                    // Publish pipeline to Redis every 100 aggTrade ticks so the
                    // dashboard reflects live Engine D state even when IDR bookTicker
                    // is silent.  btcidr@bookTicker arm still publishes on every fire.
                    if tick_count_aggtrade % 100 == 0 {
                        let spread     = agg_out.optimal_ask - agg_out.optimal_bid;
                        let wt         = agg_out.warm_ticks;
                        let agg_label  = if wt < 20 { "WARMING-UP" }
                                         else if agg_out.open_bid.is_some() && agg_out.open_ask.is_none() { "SKEW-BID" }
                                         else if agg_out.open_bid.is_none() && agg_out.open_ask.is_some() { "SKEW-ASK" }
                                         else { "MARKET-MAKE" };
                        let pp = format!(
                            r#"{{"ts":{},"tick":{},"tick_us":0,"warm_ticks":{},"micro_price":{:.0},"obi":{:.6},"tfi":{:.8},"variance":{:.2},"reservation":{:.0},"optimal_bid":{:.0},"optimal_ask":{:.0},"spread":{:.0},"open_bid":{},"open_ask":{},"inventory_btc":{:.8},"pnl_idr":{:.2},"total_trades":{},"agg_flows":{},"decision":"{}"}}"#,
                            now_ms,
                            tick_count_aggtrade,
                            wt,
                            agg_out.micro_price, agg_out.obi, agg_out.tfi,
                            agg_out.variance, agg_out.reservation_price,
                            agg_out.optimal_bid, agg_out.optimal_ask, spread,
                            agg_out.open_bid.map(|v| format!("{:.0}", v)).unwrap_or_else(|| "null".into()),
                            agg_out.open_ask.map(|v| format!("{:.0}", v)).unwrap_or_else(|| "null".into()),
                            agg_out.inventory_btc, agg_out.pnl_idr, agg_out.total_trades,
                            tick_count_aggtrade, agg_label,
                        );
                        let _ = con.set::<_, _, ()>("engine_d:pipeline", &pp).await;
                        info!(
                            "[EngineD agg#{:>6}] warm={:>3} | \
                             mid={:.0} IDR | spread={:.0} IDR | \
                             OBI={:+.4} | TFI={:+.6} BTC | σ²={:.0} | → {}",
                            tick_count_aggtrade, wt,
                            agg_out.micro_price, spread,
                            agg_out.obi, agg_out.tfi, agg_out.variance, agg_label,
                        );
                    }
                }
            }

            // ── BTC/USDT bookTicker (Engine B ratio signal only) ─────────────
            "btcusdt@bookTicker" => {
                let bt = match parse_book_ticker(&combined.data) {
                    Some(b) => b,
                    None    => { warn!("btcusdt bookTicker parse error"); continue; }
                };
                let bid     = bt.bid_price.parse::<f64>().unwrap_or(0.0);
                let ask     = bt.ask_price.parse::<f64>().unwrap_or(0.0);
                let bid_qty = bt.bid_qty.parse::<f64>().unwrap_or(0.0);
                if bid <= 0.0 || ask <= 0.0 { continue; }
                // Engine B only — USDT pair not used by Engine D.
                redis_set(con, "toko:btc_usdt:ticker", &ticker_json(ask, bid, bid_qty)).await;
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

            // ── BTC/IDR bookTicker — primary Engine D price feed ────────────
            // This is the ONLY source of IDR prices on Tokocrypto.
            // Fires on every top-of-book change; we use it as the heartbeat
            // that drives all price-sensitive Engine D formulas:
            //   • Micro-Price  (IDR bid/ask prices + USDT LOB volumes for OBI)
            //   • Reservation Price (IDR)
            //   • Optimal Spread   (IDR)
            //   • Fill detection   (IDR mid as proxy trade price)
            "btcidr@bookTicker" => {
                let bt = match parse_book_ticker(&combined.data) {
                    Some(b) => b,
                    None    => { warn!("btcidr bookTicker parse error"); continue; }
                };
                let bid     = bt.bid_price.parse::<f64>().unwrap_or(0.0);
                let ask     = bt.ask_price.parse::<f64>().unwrap_or(0.0);
                let bid_qty = bt.bid_qty.parse::<f64>().unwrap_or(0.0);
                let ask_qty = bt.ask_qty.parse::<f64>().unwrap_or(0.0);
                if bid <= 0.0 || ask <= 0.0 { continue; }

                // Cache IDR prices for use in aggTrade handler.
                last_idr_bid     = bid;
                last_idr_ask     = ask;
                last_idr_bid_vol = bid_qty;
                last_idr_ask_vol = ask_qty;

                // Publish ticker for Engine A / C consumers.
                redis_set(con, "toko:btc_idr:ticker", &ticker_json(ask, bid, ask_qty * ask)).await;

                // Blend: use IDR prices as the top-of-book, USDT LOB for depth levels 2-10.
                // This gives the best available IDR LOB approximation.
                let (bid_vol, ask_vol) = if btcusdt_book.is_ready() {
                    let (mut lob_bids, mut lob_asks) = btcusdt_book.top(10);
                    if !lob_bids.is_empty() { lob_bids[0] = (bid, bid_qty); }
                    if !lob_asks.is_empty() { lob_asks[0] = (ask, ask_qty); }
                    let json = lob_json(&lob_bids, &lob_asks);
                    redis_set_and_publish(con, "toko:btc_idr:lob", &json).await;
                    (
                        lob_bids.first().map(|b| b.1).unwrap_or(bid_qty),
                        lob_asks.first().map(|a| a.1).unwrap_or(ask_qty),
                    )
                } else {
                    (bid_qty, ask_qty)
                };

                // ── Engine D tick — timed, all values in IDR ─────────────────
                let idr_mid    = (bid + ask) * 0.5;
                let now_ms     = millis_now();
                let tick_start = Instant::now();

                let out = engine_d.tick(
                    bid, bid_vol,
                    ask, ask_vol,
                    idr_mid,
                    last_trade_vol,
                    last_is_buyer_maker,
                    now_ms,
                );

                let tick_us = tick_start.elapsed().as_micros();
                tick_count_bookidr += 1;

                // Determine state-machine decision label.
                // During warm-up (first 20 IDR ticks) skew logic is suppressed;
                // reflect that in the label so logs show the engine state clearly.
                let decision = if out.warm_ticks < 20 {
                    "WARMING-UP"
                } else {
                    match (out.open_bid, out.open_ask) {
                        (Some(_), None)    => "SKEW-BID  [cancel ask, ride bid]",
                        (None, Some(_))    => "SKEW-ASK  [cancel bid, ride ask]",
                        (Some(_), Some(_)) => "MARKET-MAKE",
                        (None, None)       => "FLAT",
                    }
                };

                // Log every 25 bookTicker ticks — enough to see the pipeline
                // in motion without drowning the log in noise.
                if tick_count_bookidr % 25 == 1 {
                    info!(
                        "[EngineD tick#{:>6}] {:>3}µs | \
                         mid={:.0} IDR | spread={:.0} IDR | \
                         OBI={:+.4} | TFI={:+.6} BTC | \
                         σ²={:.0} | r={:.0} IDR | \
                         bid*={:.0} | ask*={:.0} | \
                         inv={:+.5} BTC | flows={}agg | → {}",
                        tick_count_bookidr,
                        tick_us,
                        out.micro_price,
                        out.optimal_ask - out.optimal_bid,
                        out.obi,
                        out.tfi,
                        out.variance,
                        out.reservation_price,
                        out.optimal_bid,
                        out.optimal_ask,
                        out.inventory_btc,
                        tick_count_aggtrade,
                        decision,
                    );
                }

                // Real fills are tracked by the order manager — no simulated
                // fill logging here.

                // Write live pipeline snapshot to Redis on every tick.
                // This key is NOT throttled — consumers see every bookTicker event.
                {
                    let spread    = out.optimal_ask - out.optimal_bid;
                    // Use the warm-up-aware decision label computed above.
                    let ob_label = decision;
                    let pipeline_payload = format!(
                        r#"{{"ts":{},"tick":{},"tick_us":{},"warm_ticks":{},"micro_price":{:.0},"obi":{:.6},"tfi":{:.8},"variance":{:.2},"reservation":{:.0},"optimal_bid":{:.0},"optimal_ask":{:.0},"spread":{:.0},"open_bid":{},"open_ask":{},"inventory_btc":{:.8},"pnl_idr":{:.2},"total_trades":{},"agg_flows":{},"decision":"{}"}}"#,
                        now_ms,
                        tick_count_bookidr,
                        tick_us,
                        out.warm_ticks,
                        out.micro_price,
                        out.obi,
                        out.tfi,
                        out.variance,
                        out.reservation_price,
                        out.optimal_bid,
                        out.optimal_ask,
                        spread,
                        out.open_bid.map(|v| format!("{:.0}", v)).unwrap_or_else(|| "null".into()),
                        out.open_ask.map(|v| format!("{:.0}", v)).unwrap_or_else(|| "null".into()),
                        out.inventory_btc,
                        out.pnl_idr,
                        out.total_trades,
                        tick_count_aggtrade,
                        ob_label,
                    );
                    // Non-blocking inline SET — no spawn needed; multiplexed conn handles it.
                    let _ = con.set::<_, _, ()>("engine_d:pipeline", &pipeline_payload).await;
                }

                // ── Real order dispatch (State 4 execution) ───────────────────
                // Only dispatch when Engine D is warmed up (≥ 20 IDR ticks).
                // The order manager task executes REST calls asynchronously so
                // the WebSocket receive loop is NEVER blocked by HTTP I/O.
                if out.warm_ticks >= 20 {
                    let cmd = match (out.open_bid, out.open_ask) {
                        // SKEW-BID: cancel ask, place bid at best bid price.
                        (Some(bp), None) => Some(OrderCmd::SkewBid {
                            bid_price: round_price(bp),
                            bid_qty:   toko_rest::LOT_STEP_BTC * 2.0, // 0.00002 BTC minimum viable
                        }),
                        // SKEW-ASK: cancel bid, place ask at best ask price.
                        (None, Some(ap)) => Some(OrderCmd::SkewAsk {
                            ask_price: round_price(ap),
                            ask_qty:   toko_rest::LOT_STEP_BTC * 2.0,
                        }),
                        // MARKET-MAKE: place symmetric limit quotes.
                        (Some(bp), Some(ap)) => Some(OrderCmd::Requote {
                            bid_price: round_price(bp),
                            ask_price: round_price(ap),
                            bid_qty:   toko_rest::LOT_STEP_BTC * 2.0,
                            ask_qty:   toko_rest::LOT_STEP_BTC * 2.0,
                        }),
                        // FLAT / no quotes.
                        (None, None) => None,
                    };
                    if let Some(cmd) = cmd {
                        // try_send: non-blocking; drops command if manager is busy.
                        if order_tx.try_send(cmd).is_ok() {
                            // Update the cross-detection prices used by the aggTrade
                            // arm so CheckFills fires when the market hits our quote.
                            match (out.open_bid, out.open_ask) {
                                (Some(bp), None)     => { requested_bid = round_price(bp); requested_ask = 0.0; }
                                (None, Some(ap))     => { requested_bid = 0.0; requested_ask = round_price(ap); }
                                (Some(bp), Some(ap)) => { requested_bid = round_price(bp); requested_ask = round_price(ap); }
                                (None, None)         => {}
                            }
                        }
                    }
                }
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
            let ticks_idr  = tick_count_bookidr;
            let ticks_agg  = tick_count_aggtrade;

            // Clone the multiplexed connection — cheap, no new TCP socket.
            let mut tel_con = con.clone();

            tokio::spawn(async move {
                let payload = format!(
                    r#"{{"ts":{},"inventory_btc":{:.8},"pnl_idr":{:.2},"total_trades":{},"variance":{:.4},"ticks_idr":{},"ticks_agg":{}}}"#,
                    millis_now(), inv_btc, pnl_idr, trades, variance, ticks_idr, ticks_agg
                );
                if let Err(e) = tel_con.set::<_, _, ()>("telemetry:engine_d", &payload).await {
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

    // ── Tokocrypto REST client + order manager ───────────────────────────────
    // The order manager runs in its own task and receives commands via a
    // bounded channel from the WebSocket tick loop.  This keeps REST I/O
    // fully decoupled from the hot tick path.
    let toko_client = match TokoClient::new(toko_key.clone(), toko_secret.clone()) {
        Ok(c)  => std::sync::Arc::new(c),
        Err(e) => { error!("Failed to build Tokocrypto REST client: {}", e); return Err(e); }
    };

    // Channel capacity = 8: enough for burst; drops if manager is saturated.
    let (order_tx, order_rx) = mpsc::channel::<OrderCmd>(8);

    {
        let client_arc = std::sync::Arc::clone(&toko_client);
        let om_con     = con.clone();
        tokio::spawn(async move {
            order_manager(client_arc, order_rx, om_con).await;
        });
        info!("Order manager task spawned.");
    }

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
        match stream_loop(&mut con, &wss_url, order_tx.clone()).await {
            Ok(())  => info!("Toko WSS disconnected cleanly. Reconnecting in {:?}…", delay),
            Err(e)  => error!("Toko WSS error: {}. Reconnecting in {:?}…", e, delay),
        }
        sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(30));
    }
}
