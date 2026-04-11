//! Agent 0: Binance Multi-Coin HFT Engine (Engine D) — 5-pair multiplexer.
//!
//! One WebSocket connection subscribes to all 5 pairs simultaneously.
//! Each coin gets its own HFTEngine instance with correct exchange physics.
//! A single order_manager task handles all REST execution, keyed by symbol.
//!
//! Pairs: BTCFDUSD · ADAFDUSD · DOTFDUSD · DOGEFDUSD · XRPFDUSD
//!
//! WSS stream routing:
//!   msg["stream"] = "btcfdusd@depth@100ms"  → symbol="BTCFDUSD", type="depth"
//!   msg["stream"] = "adafdusd@aggTrade"      → symbol="ADAFDUSD", type="aggTrade"
//!   msg["stream"] = "dogefdusd@bookTicker"   → symbol="DOGEFDUSD", type="bookTicker"
//!
//! Redis key schema (per-coin):
//!   engine_d:{SYMBOL}:pipeline    → live A-S formula outputs
//!   engine_d:{SYMBOL}:orders      → order manager state
//!   engine_d:{SYMBOL}:last_fill   → most recent confirmed fill
//!   telemetry:engine_d:{SYMBOL}   → 1s telemetry snapshot

mod binance_rest;
mod engine_d_hft;

use anyhow::Result;
use binance_rest::{qty_from_fixed_notional, qty_from_notional, round_price, round_qty, BinanceClient};
use engine_d_hft::{HFTEngine, MarketRegime};
use futures_util::{SinkExt, StreamExt};
use redis::AsyncCommands;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_postgres::NoTls;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

// ── Agent Q message types ─────────────────────────────────────────────────────

/// Tactical parameters injected every 15 minutes by the Parameter Tuner.
#[derive(Debug, Deserialize)]
struct AgentQParams {
    gamma:            f64,
    min_spread_ticks: f64,
    tfi_threshold:    f64,
    #[allow(dead_code)]
    system_status:    Option<String>,
}

/// Regime payload injected every 5 minutes by the Oracle Classifier.
#[derive(Debug, Deserialize)]
struct RegimePayload {
    regime:     String,          // "MEAN_REVERTING", "RETAIL_FRENZY_UP", …
    #[allow(dead_code)]
    confidence: Option<f64>,
}

impl RegimePayload {
    fn parse_regime(&self) -> MarketRegime {
        match self.regime.as_str() {
            "RETAIL_FRENZY_UP"               => MarketRegime::RetailFrenzyUp,
            "INSTITUTIONAL_ABSORPTION_DOWN"  => MarketRegime::InstitutionalAbsorptionDown,
            "DEAD_ZONE"                      => MarketRegime::DeadZone,
            "TOXIC_LIQUIDATION_CASCADE"      => MarketRegime::ToxicLiquidationCascade,
            _                                => MarketRegime::MeanReverting,
        }
    }
}

/// Unified message sent from the Redis subscriber task to the stream loop.
enum AgentQMessage {
    Params(AgentQParams),
    Regime(MarketRegime),
}

type AgentQUpdate = (String, AgentQMessage);

// ── Coin configuration table ──────────────────────────────────────────────────

struct CoinConfig {
    symbol:        &'static str,  // "BTCFDUSD"
    stream_prefix: &'static str,  // "btcfdusd"
    tick_size:     f64,
    lot_step:      f64,
    max_inventory: f64,
    coin_asset:    &'static str,  // "BTC", "ADA", …
}

const COIN_CONFIGS: &[CoinConfig] = &[
    CoinConfig { symbol: "ADAFDUSD",  stream_prefix: "adafdusd",  tick_size: 0.0001,  lot_step: 0.1,     max_inventory: 50.0,    coin_asset: "ADA"  },
    CoinConfig { symbol: "DOTFDUSD",  stream_prefix: "dotfdusd",  tick_size: 0.001,   lot_step: 0.01,    max_inventory: 3.5,     coin_asset: "DOT"  },
    CoinConfig { symbol: "DOGEFDUSD", stream_prefix: "dogefdusd", tick_size: 0.00001, lot_step: 1.0,     max_inventory: 180.0,   coin_asset: "DOGE" },
    CoinConfig { symbol: "XRPFDUSD",  stream_prefix: "xrpfdusd",  tick_size: 0.0001,  lot_step: 1.0,     max_inventory: 45.0,    coin_asset: "XRP"  },
];

// ── WebSocket URL builder ─────────────────────────────────────────────────────

fn build_wss_url() -> String {
    let streams: Vec<String> = COIN_CONFIGS.iter().flat_map(|cfg| {
        vec![
            format!("{}@depth@100ms", cfg.stream_prefix),
            format!("{}@aggTrade",    cfg.stream_prefix),
            format!("{}@bookTicker",  cfg.stream_prefix),
        ]
    }).collect();
    format!("wss://stream.binance.com:9443/stream?streams={}", streams.join("/"))
}

// ── Order manager command channel ─────────────────────────────────────────────

#[derive(Debug)]
enum OrderCmd {
    Requote  { symbol: String, bid_price: f64, ask_price: f64 },
    SkewAsk  { symbol: String, ask_price: f64 },
    SkewBid  { symbol: String, bid_price: f64 },
    CancelAll { symbol: String },
    PanicSell  { symbol: String, qty: f64 },
}

/// Fill notification: order_manager → stream_loop.
/// (symbol, is_buy, price, qty_coin, fee_usd)
type FillNotif = (String, bool, f64, f64, f64);

// ── Order manager per-coin state ──────────────────────────────────────────────

struct CoinState {
    tick_size:        f64,
    lot_step:         f64,
    coin_asset:       String,
    bid_id:           Option<u64>,
    ask_id:           Option<u64>,
    last_bid:         f64,
    last_ask:         f64,
    last_requote:     Instant,
    last_trade_id:    u64,
    real_pnl_usd:     f64,
    real_inventory:   f64,
    real_total_trades: u64,
}

impl CoinState {
    fn new(cfg: &CoinConfig) -> Self {
        let past = Instant::now()
            .checked_sub(Duration::from_secs(60))
            .unwrap_or_else(Instant::now);
        Self {
            tick_size:         cfg.tick_size,
            lot_step:          cfg.lot_step,
            coin_asset:        cfg.coin_asset.to_string(),
            bid_id:            None,
            ask_id:            None,
            last_bid:          0.0,
            last_ask:          0.0,
            last_requote:      past,
            last_trade_id:     0,
            real_pnl_usd:      0.0,
            real_inventory:    0.0,
            real_total_trades: 0,
        }
    }
}

// ── Constants ─────────────────────────────────────────────────────────────────

/// Minimum ms between consecutive requotes per coin (max 5 req/s).
const REQUOTE_MIN_MS: u128 = 200;

/// Refresh all balances every N seconds.
const BALANCE_REFRESH_S: u64 = 30;

/// Minimum FDUSD to attempt a bid — $0.10 buffer above Binance's $5.00 MIN_NOTIONAL.
/// When balance is between MIN_BID_FDUSD and TARGET_NOTIONAL_USD, the bot trades
/// with what it has (capped at TARGET) rather than sitting completely idle.
const MIN_BID_FDUSD: f64 = binance_rest::MIN_NOTIONAL + 0.10;

fn moved_enough(new_price: f64, old_price: f64, tick_size: f64) -> bool {
    if old_price == 0.0 { return true; }
    (new_price - old_price).abs() >= tick_size
}

// ── Order manager task ────────────────────────────────────────────────────────

/// Spawn a non-blocking PostgreSQL INSERT for a confirmed Engine D trade fill.
/// Called from the order_manager after each fill; never blocks the hot path.
fn spawn_telemetry_insert(
    db:        Arc<tokio_postgres::Client>,
    symbol:    String,
    notional:  f64,
    is_buyer:  bool,
    fee:       f64,
) {
    tokio::spawn(async move {
        let gross_pnl = if is_buyer { -notional } else { notional };
        let net_pnl   = if is_buyer { -(notional + fee) } else { notional - fee };
        let roe_pct   = (net_pnl / 28.0) * 100.0;
        if let Err(e) = db.execute(
            "INSERT INTO trade_telemetry \
             (timestamp, engine_id, asset_pair, trade_size_idr, entry_signal_value, \
              gross_pnl, fees_paid, net_pnl, trade_roe_pct) \
             VALUES (NOW(), 'D', $1, $2, $3, $4, $5, $6, $7)",
            &[&symbol, &notional, &fee, &gross_pnl, &fee, &net_pnl, &roe_pct],
        ).await {
            warn!("[{}] Telemetry INSERT failed: {}", symbol, e);
        }
    });
}

async fn order_manager(
    client:     std::sync::Arc<BinanceClient>,
    mut rx:     mpsc::Receiver<OrderCmd>,
    mut redis_con: redis::aio::MultiplexedConnection,
    fill_tx:    mpsc::Sender<FillNotif>,
    db:         Arc<tokio_postgres::Client>,
    mut uds_rx: mpsc::Receiver<ExecutionReport>,
) {
    // Per-coin state
    let mut states: HashMap<String, CoinState> = COIN_CONFIGS.iter()
        .map(|cfg| (cfg.symbol.to_string(), CoinState::new(cfg)))
        .collect();

    // Shared balance cache
    let mut balances: HashMap<String, f64> = HashMap::new();
    let past = Instant::now()
        .checked_sub(Duration::from_secs(BALANCE_REFRESH_S + 1))
        .unwrap_or_else(Instant::now);
    let mut last_bal_refresh = past;

    // Startup: cancel all stale orders on every symbol.
    info!("[OrderMgr] Startup — cancelling stale orders on all symbols…");
    for cfg in COIN_CONFIGS {
        if let Err(e) = client.cancel_all_open(cfg.symbol).await {
            error!("[OrderMgr] startup cancel_all {} error: {}", cfg.symbol, e);
        }
    }

    // Seed trade cursors.
    for cfg in COIN_CONFIGS {
        match client.fetch_trades(cfg.symbol, 0, 1).await {
            Ok(t) if !t.is_empty() => {
                let id = t.last().unwrap().trade_id;
                if let Some(s) = states.get_mut(cfg.symbol) { s.last_trade_id = id; }
                info!("[{}] Trade cursor seeded at tradeId={}", cfg.symbol, id);
            }
            _ => info!("[{}] No prior trades — starting fresh.", cfg.symbol),
        }
    }

    // Initial balance fetch — seeds real_inventory AND engine.inventory_coin from
    // free+locked ground truth to survive container restarts (Inventory Amnesia fix).
    match client.get_balances().await {
        Ok(b) => {
            info!("[OrderMgr] Initial balances: FDUSD={:.2}", b.get("FDUSD").copied().unwrap_or(0.0));
            for cfg in COIN_CONFIGS {
                let total = b.get(cfg.coin_asset).copied().unwrap_or(0.0);
                info!("  {} total(free+locked)={:.8}", cfg.coin_asset, total);
                if let Some(s) = states.get_mut(cfg.symbol) {
                    s.real_inventory = total;
                    if total > 0.0 {
                        info!("[{}] Shadow ledger seeded from balance: {:.8}", cfg.symbol, total);
                        // Send synthetic seed fill (price=0 → no PnL impact) so
                        // eng.inventory_coin in the tick loop also starts correct.
                        let _ = fill_tx.try_send((cfg.symbol.to_string(), true, 0.0, total, 0.0));
                    }
                }
            }
            balances = b;
            last_bal_refresh = Instant::now();
        }
        Err(e) => error!("[OrderMgr] Initial balance fetch error: {}", e),
    }

    // ── Main receive loop (tokio::select! multiplexes OrderCmd + UDS fills) ───
    loop {
        // Refresh balances if stale — also reconciles real_inventory from exchange ground truth.
        if last_bal_refresh.elapsed().as_secs() >= BALANCE_REFRESH_S {
            match client.get_balances().await {
                Ok(b) => {
                    let fdusd = b.get("FDUSD").copied().unwrap_or(0.0);
                    info!("[OrderMgr] Balance refresh — FDUSD={:.2}", fdusd);
                    // AMNESIA FIX: reconcile shadow ledger with free+locked ground truth.
                    // Protects against drift between UDS deltas and exchange reality.
                    for cfg in COIN_CONFIGS {
                        let total = b.get(cfg.coin_asset).copied().unwrap_or(0.0);
                        if let Some(s) = states.get_mut(cfg.symbol) {
                            if (s.real_inventory - total).abs() > cfg.lot_step {
                                info!(
                                    "[{}] Inventory reconciled: shadow={:.8} → exchange={:.8}",
                                    cfg.symbol, s.real_inventory, total
                                );
                                s.real_inventory = total;
                            }
                        }
                    }
                    balances = b;
                    last_bal_refresh = Instant::now();
                }
                Err(e) => error!("[OrderMgr] balance refresh error: {}", e),
            }
        }

        tokio::select! {

            // ── HANDLE INSTANT ZERO-LATENCY FILLS (UDS execution report) ─────
            Some(report) = uds_rx.recv() => {
                let symbol = report.symbol.clone();
                if let Some(s) = states.get_mut(&symbol) {
                    let filled_qty:   f64 = report.last_filled_qty.parse().unwrap_or(0.0);
                    let filled_price: f64 = report.last_filled_price.parse().unwrap_or(0.0);
                    let fee:          f64 = report.commission.parse().unwrap_or(0.0);
                    let is_buyer = report.side == "BUY";
                    let notional = filled_price * filled_qty;

                    // Update Shadow Ledger instantly
                    if is_buyer {
                        s.real_inventory += filled_qty;
                        s.real_pnl_usd   -= notional + fee;
                    } else {
                        s.real_inventory -= filled_qty;
                        s.real_pnl_usd   += notional - fee;
                    }
                    s.real_total_trades += 1;
                    if report.trade_id > 0 {
                        s.last_trade_id = report.trade_id as u64;
                    }

                    // Fix 1: Clear tracked order IDs on FILLED to prevent phantom cancel
                    // API calls on already-filled orders (burns API weight, risks IP ban).
                    if report.order_status == "FILLED" {
                        if is_buyer {
                            if s.bid_id == Some(report.order_id as u64) { s.bid_id = None; }
                        } else {
                            if s.ask_id == Some(report.order_id as u64) { s.ask_id = None; }
                        }
                    }

                    // Alert main loop to update engine math
                    let _ = fill_tx.try_send((symbol.clone(), is_buyer, filled_price, filled_qty, fee));

                    info!(
                        "[{}][UDS FILL] {} {:.8} @ {:.8} | pnl={:.2} | inv={:+.8}",
                        symbol, report.side, filled_qty, filled_price,
                        s.real_pnl_usd, s.real_inventory
                    );

                    // Log to Redis & Postgres
                    let fp = format!(
                        r#"{{"ts":{},"tradeId":{},"orderId":{},"side":"{}","price":{:.8},"qty":{:.8},"notional":{:.4},"fee":{:.4},"pnl_running":{:.2},"inventory":{:.8},"total_trades":{}}}"#,
                        millis_now(), report.trade_id, report.order_id, report.side,
                        filled_price, filled_qty, notional, fee,
                        s.real_pnl_usd, s.real_inventory, s.real_total_trades
                    );
                    let key = format!("engine_d:{}:last_fill", symbol);
                    let _ = redis_con.set::<_, _, ()>(&key, &fp).await;
                    spawn_telemetry_insert(Arc::clone(&db), symbol, notional, is_buyer, fee);
                }
                continue;
            }

            // ── OrderCmd from the stream loop ─────────────────────────────────
            cmd = rx.recv() => {
                let cmd = match cmd {
                    Some(c) => c,
                    None    => break,      // channel closed — shutdown
                };

        match cmd {

            // ── Cancel all quotes for one coin ────────────────────────────────
            OrderCmd::CancelAll { symbol } => {
                if let Some(s) = states.get_mut(&symbol) {
                    info!("[{}] CancelAll — pulling all quotes.", symbol);
                    if let Some(id) = s.bid_id.take() { let _ = client.cancel_order(&symbol, id).await; }
                    if let Some(id) = s.ask_id.take() { let _ = client.cancel_order(&symbol, id).await; }
                    s.last_bid = 0.0; s.last_ask = 0.0;
                }
            }

            // ── Panic stop-loss — MARKET SELL ─────────────────────────────────
            OrderCmd::PanicSell { symbol, qty: _ } => {
                if let Some(s) = states.get_mut(&symbol) {
                    if let Some(id) = s.bid_id.take() { let _ = client.cancel_order(&symbol, id).await; }
                    if let Some(id) = s.ask_id.take() { let _ = client.cancel_order(&symbol, id).await; }
                    s.last_bid = 0.0; s.last_ask = 0.0;

                    // CRITICAL FIX: sell what we physically own per shadow ledger
                    let sell_qty = (s.real_inventory / s.lot_step).floor() * s.lot_step;
                    if sell_qty >= s.lot_step {
                        match client.place_market_sell(&symbol, sell_qty, s.lot_step).await {
                            Ok(id) => {
                                info!("[{}] PANIC SELL executed orderId={} qty={:.8}", symbol, id, sell_qty);
                                last_bal_refresh = Instant::now()
                                    .checked_sub(Duration::from_secs(BALANCE_REFRESH_S + 1))
                                    .unwrap_or_else(Instant::now);
                            }
                            Err(e) => error!("[{}] PANIC SELL failed: {}", symbol, e),
                        }
                    } else {
                        warn!("[{}] PanicSell real_inventory={:.8} too small — skipped", symbol, s.real_inventory);
                    }
                }
            }

            // ── Skew-BID (inventory clamped short) ────────────────────────────
            OrderCmd::SkewBid { symbol, bid_price } => {
                let s = match states.get_mut(&symbol) { Some(s) => s, None => continue };
                if let Some(id) = s.ask_id.take() {
                    let _ = client.cancel_order(&symbol, id).await;
                    s.last_ask = 0.0;
                }
                if moved_enough(bid_price, s.last_bid, s.tick_size)
                    && s.last_requote.elapsed().as_millis() >= REQUOTE_MIN_MS
                {
                    if let Some(id) = s.bid_id.take() { let _ = client.cancel_order(&symbol, id).await; }
                    let fdusd = balances.get("FDUSD").copied().unwrap_or(0.0);
                    if fdusd >= MIN_BID_FDUSD {
                        let effective_notional = fdusd.min(binance_rest::TARGET_NOTIONAL_USD);
                        let qty = qty_from_notional(effective_notional, bid_price, s.lot_step);
                        if qty > 0.0 {
                            match client.place_limit_order(&symbol, "BUY", bid_price, qty, s.tick_size, s.lot_step).await {
                                Ok(id) => { s.bid_id = Some(id); s.last_bid = bid_price; s.last_requote = Instant::now(); }
                                Err(e) => warn!("[{}] SkewBid place skipped: {}", symbol, e),
                            }
                        }
                    } else {
                        warn!("[{}] SkewBid skipped — FDUSD {:.2} < {:.2}", symbol, fdusd, MIN_BID_FDUSD);
                    }
                }
            }

            // ── Skew-ASK (inventory clamped long) ─────────────────────────────
            OrderCmd::SkewAsk { symbol, ask_price } => {
                let s = match states.get_mut(&symbol) { Some(s) => s, None => continue };
                if let Some(id) = s.bid_id.take() {
                    let _ = client.cancel_order(&symbol, id).await;
                    s.last_bid = 0.0;
                }
                if moved_enough(ask_price, s.last_ask, s.tick_size)
                    && s.last_requote.elapsed().as_millis() >= REQUOTE_MIN_MS
                {
                    if let Some(id) = s.ask_id.take() { let _ = client.cancel_order(&symbol, id).await; }
                    // CRITICAL FIX: sell what we physically own per shadow ledger
                    let sell_qty = (s.real_inventory / s.lot_step).floor() * s.lot_step;
                    let notional  = sell_qty * ask_price;
                    if sell_qty >= s.lot_step && notional >= binance_rest::MIN_NOTIONAL {
                        match client.place_limit_order(&symbol, "SELL", ask_price, sell_qty, s.tick_size, s.lot_step).await {
                            Ok(id) => { s.ask_id = Some(id); s.last_ask = ask_price; s.last_requote = Instant::now(); }
                            Err(e) => warn!("[{}] SkewAsk place skipped: {}", symbol, e),
                        }
                    } else {
                        warn!("[{}] SkewAsk insufficient inventory: real={:.8}", symbol, s.real_inventory);
                    }
                }
            }

            // ── Requote (symmetric A-S market making) ─────────────────────────
            OrderCmd::Requote { symbol, bid_price, ask_price } => {
                let s = match states.get_mut(&symbol) { Some(s) => s, None => continue };

                let bid_moved = moved_enough(bid_price, s.last_bid, s.tick_size);
                let ask_moved = moved_enough(ask_price, s.last_ask, s.tick_size);

                if (!bid_moved && !ask_moved)
                    || s.last_requote.elapsed().as_millis() < REQUOTE_MIN_MS
                {
                    continue;
                }

                // BID side
                if bid_moved {
                    if let Some(id) = s.bid_id.take() { let _ = client.cancel_order(&symbol, id).await; }
                    let fdusd = balances.get("FDUSD").copied().unwrap_or(0.0);
                    if fdusd >= MIN_BID_FDUSD {
                        let effective_notional = fdusd.min(binance_rest::TARGET_NOTIONAL_USD);
                        let qty = qty_from_notional(effective_notional, bid_price, s.lot_step);
                        if qty > 0.0 {
                            match client.place_limit_order(&symbol, "BUY", bid_price, qty, s.tick_size, s.lot_step).await {
                                Ok(id) => { s.bid_id = Some(id); s.last_bid = bid_price; }
                                Err(e) => warn!("[{}] Requote bid skipped: {}", symbol, e),
                            }
                        }
                    } else {
                        warn!("[{}] Requote bid skipped — FDUSD {:.2} < {:.2}", symbol, fdusd, MIN_BID_FDUSD);
                    }
                }

                // ASK side — sell what we physically own per shadow ledger.
                if ask_moved {
                    if let Some(id) = s.ask_id.take() { let _ = client.cancel_order(&symbol, id).await; }
                    // CRITICAL FIX: sell what we physically own per shadow ledger
                    let sell_qty = (s.real_inventory / s.lot_step).floor() * s.lot_step;
                    let notional  = sell_qty * ask_price;
                    if sell_qty >= s.lot_step && notional >= binance_rest::MIN_NOTIONAL {
                        match client.place_limit_order(&symbol, "SELL", ask_price, sell_qty, s.tick_size, s.lot_step).await {
                            Ok(id) => { s.ask_id = Some(id); s.last_ask = ask_price; }
                            Err(e) => warn!("[{}] Requote ask skipped: {}", symbol, e),
                        }
                    } else {
                        warn!("[{}] Requote ask insufficient inventory: real={:.8}", symbol, s.real_inventory);
                    }
                }

                s.last_requote = Instant::now();

                // Publish order state snapshot.
                let s = states.get(&symbol).unwrap();
                let payload = format!(
                    r#"{{"ts":{},"symbol":"{}","bid_id":{},"ask_id":{},"bid_price":{:.8},"ask_price":{:.8},"real_pnl_usd":{:.2},"real_inventory":{:.8},"real_trades":{}}}"#,
                    millis_now(), symbol,
                    s.bid_id.map(|v| v.to_string()).unwrap_or_else(|| "null".into()),
                    s.ask_id.map(|v| v.to_string()).unwrap_or_else(|| "null".into()),
                    s.last_bid, s.last_ask,
                    s.real_pnl_usd, s.real_inventory, s.real_total_trades
                );
                let key = format!("engine_d:{}:orders", symbol);
                let _ = redis_con.set::<_, _, ()>(&key, &payload).await;
            }
        }      // close: match cmd
        }      // close: cmd = rx.recv() select arm
        }      // close: tokio::select!
    }          // close: loop

    // Cleanup on channel close.
    info!("[OrderMgr] channel closed — cancelling remaining orders…");
    for (sym, s) in &mut states {
        if let Some(id) = s.bid_id { let _ = client.cancel_order(sym, id).await; }
        if let Some(id) = s.ask_id { let _ = client.cancel_order(sym, id).await; }
    }
}

// ── In-memory limit-order book ────────────────────────────────────────────────

struct DepthBook {
    bids:       HashMap<String, f64>,
    asks:       HashMap<String, f64>,
    tick_count: u64,
}

impl DepthBook {
    fn new() -> Self { Self { bids: HashMap::new(), asks: HashMap::new(), tick_count: 0 } }

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

// ── User Data Stream execution report ─────────────────────────────────────────

/// Inbound `executionReport` event from the Tokocrypto User Data Stream.
/// Only `FILLED` and `PARTIALLY_FILLED` events with `x = "TRADE"` are acted upon.
#[derive(Debug, Deserialize, Clone)]
struct ExecutionReport {
    #[serde(rename = "e")] event_type:             String,  // "executionReport"
    #[serde(rename = "s")] symbol:                 String,  // "ADAFDUSD"
    #[serde(rename = "S")] side:                   String,  // "BUY" | "SELL"
    #[serde(rename = "x")] current_execution_type: String,  // "TRADE"
    #[serde(rename = "X")] order_status:           String,  // "FILLED" | "PARTIALLY_FILLED"
    #[serde(rename = "q")] order_qty:              String,
    #[serde(rename = "p")] order_price:            String,
    #[serde(rename = "l")] last_filled_qty:        String,  // last executed qty (coin)
    #[serde(rename = "L")] last_filled_price:      String,  // last executed price
    #[serde(rename = "n")] commission:             String,  // fee amount
    #[serde(rename = "t")] trade_id:               i64,     // trade ID (-1 if no fill yet)
    #[serde(rename = "i")] order_id:               i64,     // order ID
}

// ── Serde models ──────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct CombinedMessage {
    stream: String,
    data:   serde_json::Value,
}

#[derive(Deserialize)]
struct DepthDiff {
    #[serde(rename = "b")] bids: Vec<[String; 2]>,
    #[serde(rename = "a")] asks: Vec<[String; 2]>,
}

#[derive(Deserialize)]
struct AggTrade {
    #[serde(rename = "p")] price:          String,
    #[serde(rename = "q")] qty:            String,
    #[serde(rename = "m")] is_buyer_maker: bool,
}

#[derive(Deserialize)]
struct BookTicker {
    #[serde(rename = "b")] bid_price: String,
    #[serde(rename = "B")] bid_qty:   String,
    #[serde(rename = "a")] ask_price: String,
    #[serde(rename = "A")] ask_qty:   String,
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn micros_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_micros() as u64
}

fn millis_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

fn ticker_json(symbol: &str, ask: f64, bid: f64, volume: f64) -> String {
    format!(r#"{{"ts":{},"symbol":"{}","ask":{:.8},"bid":{:.8},"volume":{:.4}}}"#,
        micros_now(), symbol, ask, bid, volume)
}

fn lob_json(symbol: &str, bids: &[(f64, f64)], asks: &[(f64, f64)]) -> String {
    let fmt = |levels: &[(f64, f64)]| -> String {
        levels.iter().map(|(p, v)| format!("[{:.8},{:.4}]", p, v)).collect::<Vec<_>>().join(",")
    };
    format!(r#"{{"ts":{},"symbol":"{}","bids":[{}],"asks":[{}]}}"#,
        micros_now(), symbol, fmt(bids), fmt(asks))
}

async fn redis_set(con: &mut redis::aio::MultiplexedConnection, key: &str, value: &str) {
    if let Err(e) = con.set::<_, _, ()>(key, value).await {
        warn!("Redis SET {} failed: {}", key, e);
    }
}

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

// ── Binance WebSocket loop ────────────────────────────────────────────────────

async fn stream_loop(
    con:    &mut redis::aio::MultiplexedConnection,
    wss_url: &str,
    order_tx: mpsc::Sender<OrderCmd>,
    fill_rx:  &mut mpsc::Receiver<FillNotif>,
    aq_rx:    &mut mpsc::UnboundedReceiver<AgentQUpdate>,
) -> Result<()> {
    info!("Connecting to Binance WSS: {}", wss_url);
    let _ = order_tx.try_send(OrderCmd::CancelAll { symbol: "ALL".into() });

    // Per-coin engine instances (fresh on each reconnect; fill_rx reconstructs inventory).
    let mut engines: HashMap<String, HFTEngine> = COIN_CONFIGS.iter()
        .map(|cfg| (
            cfg.symbol.to_string(),
            HFTEngine::new(cfg.symbol.to_string(), cfg.tick_size, cfg.lot_step, cfg.max_inventory),
        ))
        .collect();

    // Per-coin depth books.
    let mut books: HashMap<String, DepthBook> = COIN_CONFIGS.iter()
        .map(|cfg| (cfg.symbol.to_string(), DepthBook::new()))
        .collect();

    // Per-coin requested quotes (for cross-detection and CancelAll spam guard).
    // Maps symbol → (requested_bid, requested_ask).
    let mut requested: HashMap<String, (f64, f64)> = COIN_CONFIGS.iter()
        .map(|cfg| (cfg.symbol.to_string(), (0.0_f64, 0.0_f64)))
        .collect();

    // Coin config lookup (symbol → &CoinConfig).
    let coin_map: HashMap<String, &CoinConfig> = COIN_CONFIGS.iter()
        .map(|cfg| (cfg.symbol.to_string(), cfg))
        .collect();

    // Per-coin tick counters.
    let mut tick_book:     HashMap<String, u64> = COIN_CONFIGS.iter().map(|c| (c.symbol.to_string(), 0u64)).collect();
    let mut tick_aggtrade: HashMap<String, u64> = COIN_CONFIGS.iter().map(|c| (c.symbol.to_string(), 0u64)).collect();
    // Last computed TFI per symbol — updated every bookTicker tick, read by 1s telemetry snapshot.
    let mut last_tfi: HashMap<String, f64> = COIN_CONFIGS.iter().map(|c| (c.symbol.to_string(), 0.0f64)).collect();

    let mut last_telemetry = Instant::now();

    let url = url::Url::parse(wss_url)?;
    let (ws_stream, _) = connect_async(url).await?;
    info!("Binance WSS connected — streaming {} pairs.", COIN_CONFIGS.len());

    let (_, mut reader) = ws_stream.split();

    while let Some(msg) = reader.next().await {
        let msg = match msg {
            Ok(m)  => m,
            Err(e) => { warn!("WSS receive error: {}", e); break; }
        };

        let text = match msg {
            Message::Text(t)  => t,
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue,
            Message::Close(f) => { info!("WSS closed by server: {:?}", f); break; }
            Message::Binary(_) => continue,
        };

        let combined: CombinedMessage = match serde_json::from_str(&text) {
            Ok(c)  => c,
            Err(e) => { warn!("WSS parse error: {} — raw: {:.80}", e, text); continue; }
        };

        // Extract symbol and stream type from "btcfdusd@depth@100ms".
        let mut parts = combined.stream.splitn(2, '@');
        let prefix      = parts.next().unwrap_or("");
        let stream_type = parts.next().unwrap_or("");
        let symbol      = prefix.to_uppercase();  // "BTCFDUSD"

        // Only process known symbols.
        if !engines.contains_key(&symbol) {
            warn!("Unhandled stream symbol: {}", symbol);
            continue;
        }

        match stream_type {

            // ── 100ms LOB depth update ────────────────────────────────────────
            "depth@100ms" => {
                let diff: DepthDiff = match serde_json::from_value(combined.data) {
                    Ok(d)  => d,
                    Err(e) => { warn!("[{}] depth parse error: {}", symbol, e); continue; }
                };
                let book = books.get_mut(&symbol).unwrap();
                book.apply(&diff.bids, &diff.asks);

                if book.is_ready() {
                    let (bids, asks) = book.top(10);
                    if !bids.is_empty() && !asks.is_empty() {
                        let json = lob_json(&symbol, &bids, &asks);
                        let key  = format!("toko:{}:lob", symbol.to_lowercase());
                        redis_set_and_publish(con, &key, &json).await;
                    }
                }
            }

            // ── aggTrade — TFI accumulation + fill cross-detection ────────────
            "aggTrade" => {
                let trade: AggTrade = match serde_json::from_value(combined.data) {
                    Ok(t)  => t,
                    Err(e) => { warn!("[{}] aggTrade parse error: {}", symbol, e); continue; }
                };

                let qty         = trade.qty.parse::<f64>().unwrap_or(0.0);
                let trade_price = trade.price.parse::<f64>().unwrap_or(0.0);
                if qty <= 0.0 || trade_price <= 0.0 { continue; }

                let now_ms = millis_now();
                engines.get_mut(&symbol).unwrap().record_agg_trade(qty, trade_price, trade.is_buyer_maker, now_ms);
                *tick_aggtrade.get_mut(&symbol).unwrap() += 1;

                // Cross-detection retained for logging purposes; fills handled by UDS.
                let (req_bid, req_ask) = requested.get(&symbol).copied().unwrap_or((0.0, 0.0));
                let cross_bid = req_bid > 0.0 && trade_price <= req_bid;
                let cross_ask = req_ask > 0.0 && trade_price >= req_ask;
                if cross_bid || cross_ask {
                    // Fill notification now arrives via User Data Stream — no REST poll needed.
                    let _ = (cross_bid, cross_ask); // suppress unused warning
                }
            }

            // ── bookTicker — sole Engine D tick driver ────────────────────────
            "bookTicker" => {
                let bt: BookTicker = match serde_json::from_value(combined.data) {
                    Ok(b)  => b,
                    Err(e) => { warn!("[{}] bookTicker parse error: {}", symbol, e); continue; }
                };

                let bid     = bt.bid_price.parse::<f64>().unwrap_or(0.0);
                let ask     = bt.ask_price.parse::<f64>().unwrap_or(0.0);
                let bid_qty = bt.bid_qty.parse::<f64>().unwrap_or(0.0);
                let ask_qty = bt.ask_qty.parse::<f64>().unwrap_or(0.0);
                if bid <= 0.0 || ask <= 0.0 { continue; }

                let ticker_key = format!("toko:{}:ticker", symbol.to_lowercase());
                redis_set(con, &ticker_key, &ticker_json(&symbol, ask, bid, bid_qty * bid)).await;

                let (bid_vol, ask_vol) = {
                    let book = books.get(&symbol).unwrap();
                    if book.is_ready() {
                        let (bids, asks) = book.top(5);
                        let bv: f64 = bids.iter().map(|b| b.1).sum();
                        let av: f64 = asks.iter().map(|a| a.1).sum();
                        (if bv > 0.0 { bv } else { bid_qty },
                         if av > 0.0 { av } else { ask_qty })
                    } else {
                        (bid_qty, ask_qty)
                    }
                };

                // Apply confirmed fills to engine inventory.
                while let Ok((sym, is_buy, price, qty, fee)) = fill_rx.try_recv() {
                    if let Some(eng) = engines.get_mut(&sym) {
                        eng.record_real_fill(is_buy, price, qty, fee);
                    }
                }

                // ── Agent Q injection: Params (15m) + Regime (5m), between ticks
                while let Ok((sym, msg)) = aq_rx.try_recv() {
                    if let Some(eng) = engines.get_mut(&sym) {
                        match msg {
                            AgentQMessage::Params(p) => {
                                eng.update_params(p.gamma, p.min_spread_ticks, p.tfi_threshold);
                                info!(
                                    "[{}][AgentQ-Tactical] γ={:.2} spread={:.1} tfi={:.2} status={:?}",
                                    sym, p.gamma, p.min_spread_ticks, p.tfi_threshold, p.system_status
                                );
                            }
                            AgentQMessage::Regime(r) => {
                                eng.update_regime(r);
                                info!("[{}][AgentQ-Oracle] Regime → {:?}", sym, r);
                            }
                        }
                    }
                }

                let engine = engines.get_mut(&symbol).unwrap();
                let cfg    = coin_map[&symbol];

                let now_ms     = millis_now();
                let tick_start = Instant::now();
                let out        = engine.tick(bid, bid_vol, ask, ask_vol, now_ms);
                let tick_us    = tick_start.elapsed().as_micros();
                // Store latest TFI for the 1s Redis telemetry heartbeat (Python reads this).
                *last_tfi.get_mut(&symbol).unwrap() = out.tfi;
                *tick_book.get_mut(&symbol).unwrap() += 1;
                let tb = tick_book[&symbol];

                // ── Panic stop-loss dispatch ──────────────────────────────────
                if out.is_panic {
                    let panic_qty = engine.inventory_coin;
                    engine.inventory_coin  = 0.0;
                    engine.open_bid        = None;
                    engine.open_ask        = None;
                    engine.last_fill_price = 0.0;
                    warn!(
                        "[{}] PANIC STOP-LOSS! inv={:.8}  micro={:.8}",
                        symbol, panic_qty, out.micro_price
                    );
                    let _ = order_tx.try_send(OrderCmd::PanicSell { symbol: symbol.clone(), qty: panic_qty });
                    if let Some(r) = requested.get_mut(&symbol) { *r = (0.0, 0.0); }
                    continue;
                }

                let spread   = out.optimal_ask - out.optimal_bid;
                let decision = if out.warm_ticks < 20 {
                    "WARMING-UP"
                } else {
                    match (out.open_bid, out.open_ask) {
                        (Some(_), Some(_)) => "MARKET-MAKE",
                        (Some(_), None)    => "SKEW-BID",
                        (None, Some(_))    => "SKEW-ASK",
                        (None, None)       => "FLAT (shadow/toxic)",
                    }
                };

                if tb % 50 == 1 {
                    info!(
                        "[{}][#{:>6}] {:>2}µs | mid={:.8} | spread={:.8} | \
                         OBI={:+.4} | TFI={:+.4} | σ²={:.8} | \
                         r={:.8} | bid*={:.8} | ask*={:.8} | \
                         inv={:+.8} | → {}",
                        symbol, tb, tick_us,
                        out.micro_price, spread,
                        out.obi, out.tfi, out.variance,
                        out.reservation_price,
                        out.optimal_bid, out.optimal_ask,
                        out.inventory_coin, decision
                    );
                }

                let pp = format!(
                    r#"{{"ts":{},"symbol":"{}","tick":{},"tick_us":{},"warm_ticks":{},"micro_price":{:.8},"obi":{:.6},"tfi":{:.6},"variance":{:.10},"reservation":{:.8},"optimal_bid":{:.8},"optimal_ask":{:.8},"spread":{:.8},"open_bid":{},"open_ask":{},"inventory_coin":{:.8},"pnl_usd":{:.4},"total_trades":{},"decision":"{}"}}"#,
                    now_ms, symbol, tb, tick_us, out.warm_ticks,
                    out.micro_price, out.obi, out.tfi, out.variance,
                    out.reservation_price,
                    out.optimal_bid, out.optimal_ask, spread,
                    out.open_bid.map(|v| format!("{:.8}", v)).unwrap_or_else(|| "null".into()),
                    out.open_ask.map(|v| format!("{:.8}", v)).unwrap_or_else(|| "null".into()),
                    out.inventory_coin, out.pnl_usd, out.total_trades, decision,
                );
                let pipeline_key = format!("engine_d:{}:pipeline", symbol);
                let _ = con.set::<_, _, ()>(&pipeline_key, &pp).await;

                // ── Order dispatch (warmed-up only) ───────────────────────────
                if out.warm_ticks >= 20 {
                    let (req_bid, req_ask) = requested.get(&symbol).copied().unwrap_or((0.0, 0.0));
                    let cmd = match (out.open_bid, out.open_ask) {
                        (Some(bp), Some(ap)) => Some(OrderCmd::Requote {
                            symbol:    symbol.clone(),
                            bid_price: round_price(bp, cfg.tick_size),
                            ask_price: round_price(ap, cfg.tick_size),
                        }),
                        (Some(bp), None) => Some(OrderCmd::SkewBid {
                            symbol:    symbol.clone(),
                            bid_price: round_price(bp, cfg.tick_size),
                        }),
                        (None, Some(ap)) => Some(OrderCmd::SkewAsk {
                            symbol:    symbol.clone(),
                            ask_price: round_price(ap, cfg.tick_size),
                        }),
                        (None, None) => {
                            if req_bid > 0.0 || req_ask > 0.0 {
                                Some(OrderCmd::CancelAll { symbol: symbol.clone() })
                            } else {
                                None
                            }
                        }
                    };

                    if let Some(cmd) = cmd {
                        if order_tx.try_send(cmd).is_ok() {
                            let r = requested.get_mut(&symbol).unwrap();
                            match (out.open_bid, out.open_ask) {
                                (Some(bp), Some(ap)) => { r.0 = round_price(bp, cfg.tick_size); r.1 = round_price(ap, cfg.tick_size); }
                                (Some(bp), None)     => { r.0 = round_price(bp, cfg.tick_size); r.1 = 0.0; }
                                (None, Some(ap))     => { r.0 = 0.0; r.1 = round_price(ap, cfg.tick_size); }
                                (None, None)         => { r.0 = 0.0; r.1 = 0.0; }
                            }
                        }
                    }
                }
            }

            other => { warn!("[{}] Unhandled stream type: {}", symbol, other); }
        }

        // ── 1s aggregate telemetry snapshot ───────────────────────────────────
        if last_telemetry.elapsed() >= Duration::from_secs(1) {
            let ts = millis_now();
            let mut tel_con = con.clone();
            for cfg in COIN_CONFIGS {
                let sym = cfg.symbol;
                if let Some(eng) = engines.get(sym) {
                    let inv    = eng.inventory_coin;
                    let pnl    = eng.pnl_usd;
                    let trades = eng.total_trades;
                    let var    = eng.variance;
                    let tfi    = last_tfi.get(sym).copied().unwrap_or(0.0);
                    let tb_v   = tick_book.get(sym).copied().unwrap_or(0);
                    let ta_v   = tick_aggtrade.get(sym).copied().unwrap_or(0);
                    // Include tfi so Python Agent Q can read live order-flow without needing trades.
                    let payload = format!(
                        r#"{{"ts":{},"symbol":"{}","inventory_coin":{:.8},"pnl_usd":{:.4},"total_trades":{},"variance":{:.10},"tfi":{:.4},"ticks_book":{},"ticks_agg":{}}}"#,
                        ts, sym, inv, pnl, trades, var, tfi, tb_v, ta_v
                    );
                    let key = format!("telemetry:engine_d:{}", sym);
                    let mut c2 = tel_con.clone();
                    let key2 = key.clone();
                    tokio::spawn(async move {
                        if let Err(e) = c2.set::<_, _, ()>(&key2, &payload).await {
                            warn!("[{}] telemetry write failed: {}", sym, e);
                        }
                    });
                }
            }
            let _ = tel_con; // suppress unused warning
            last_telemetry = Instant::now();
        }
    }

    info!("Binance WSS stream loop ended.");
    Ok(())
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("agent_0_ingestion=info,info")
        .init();

    let binance_key    = std::env::var("BINANCE_API_KEY").unwrap_or_default();
    let binance_secret = std::env::var("BINANCE_API_SECRET").unwrap_or_default();
    let redis_url      = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let db_dsn         = std::env::var("DB_DSN")
        .unwrap_or_else(|_| "postgresql://mammon:mammon@localhost:5432/mammon".to_string());

    fn redact(s: &str) -> String {
        let n = s.len();
        if n < 8 { return "[too short]".to_string(); }
        format!("{}…{}", &s[..4], &s[n - 4..])
    }

    if binance_key.is_empty() || binance_secret.is_empty() {
        warn!("BINANCE_API_KEY / BINANCE_API_SECRET not set — order execution will fail.");
    } else {
        info!("Binance credentials loaded (key: {})", redact(&binance_key));
    }

    let wss_url = build_wss_url();
    info!("WSS URL: {}", wss_url);

    info!("Connecting to PostgreSQL…");
    let (db_client, db_conn) = tokio_postgres::connect(&db_dsn, NoTls).await
        .map_err(|e| anyhow::anyhow!("PostgreSQL connect failed: {}", e))?;
    tokio::spawn(async move {
        if let Err(e) = db_conn.await {
            error!("PostgreSQL connection driver error: {}", e);
        }
    });
    let db_client = Arc::new(db_client);
    info!("PostgreSQL connected — telemetry writes enabled.");

    info!("Connecting to Redis…");
    let redis_client = redis::Client::open(redis_url)?;
    let mut con = loop {
        match redis_client.get_multiplexed_tokio_connection().await {
            Ok(c)  => { info!("Redis connected."); break c; }
            Err(e) => { warn!("Redis connection failed: {}. Retrying in 2s…", e); sleep(Duration::from_secs(2)).await; }
        }
    };

    let binance_client = match BinanceClient::new(binance_key, binance_secret) {
        Ok(c)  => std::sync::Arc::new(c),
        Err(e) => { error!("Failed to build Binance REST client: {}", e); return Err(e); }
    };

    let (order_tx, order_rx) = mpsc::channel::<OrderCmd>(256);
    let (fill_tx,  fill_rx)  = mpsc::channel::<FillNotif>(128);
    let (aq_tx,    aq_rx)    = mpsc::unbounded_channel::<AgentQUpdate>();

    // ── User Data Stream (Phase 4 — full PRD implementation) ─────────────────
    // PRD Phase 4 steps:
    //   Step 1: REST POST /api/v3/userDataStream → listenKey (weight: 2)
    //           Fallback: WebSocket API (wss://ws-api.binance.com) if REST is blocked
    //   Step 2: WSS wss://stream.binance.com:9443/ws/<listenKey> → push stream
    //           Fallback: WebSocket API subscribe on same connection if stream blocked
    //   Step 3: Deserialize executionReport events (e == "executionReport")
    //   Step 4: filled_qty → real_inventory via tokio::select! in order_manager
    //
    // Keepalive: REST PUT /api/v3/userDataStream every 30 min
    let (uds_tx, uds_rx) = mpsc::channel::<ExecutionReport>(1024);
    {
        let uds_client = Arc::clone(&binance_client);

        tokio::spawn(async move {
            const WS_API_URL: &str = "wss://ws-api.binance.com:443/ws-api/v3";

            loop {
                // ══════════════════════════════════════════════════════════════
                // Step 1: Obtain listenKey
                //   Primary:  REST POST /api/v3/userDataStream
                //   Fallback: WebSocket API userDataStream.start
                // ══════════════════════════════════════════════════════════════
                enum UdsMode { Stream(String), WsApi }

                let mode = match uds_client.get_listen_key().await {
                    Ok(k) => {
                        info!("[UDS] listenKey obtained via REST (POST /api/v3/userDataStream).");
                        UdsMode::Stream(k)
                    }
                    Err(e) => {
                        warn!("[UDS] REST get_listen_key unavailable: {} → falling back to WebSocket API.", e);
                        UdsMode::WsApi
                    }
                };

                match mode {
                    // ──────────────────────────────────────────────────────────
                    // Step 2 PRIMARY: stream.binance.com:9443/ws/<listenKey>
                    // ──────────────────────────────────────────────────────────
                    UdsMode::Stream(listen_key) => {
                        let stream_url = format!("wss://stream.binance.com:9443/ws/{}", listen_key);
                        info!("[UDS] Connecting stream endpoint: wss://stream.binance.com:9443/ws/<key>");

                        match connect_async(&stream_url).await {
                            Err(e) => {
                                warn!("[UDS] stream.binance.com connect failed: {} — retrying in 5s…", e);
                                sleep(Duration::from_secs(5)).await;
                                continue;
                            }
                            Ok((ws_stream, _)) => {
                                info!("[UDS] stream.binance.com connected — listening for executionReports.");
                                let (mut writer, mut reader) = ws_stream.split();

                                // Step 3: Receive executionReports + REST keepalive every 30 min
                                let mut keepalive_timer =
                                    tokio::time::interval(Duration::from_secs(30 * 60));
                                keepalive_timer.tick().await;

                                'stream: loop {
                                    tokio::select! {
                                        _ = keepalive_timer.tick() => {
                                            // Keepalive: REST PUT /api/v3/userDataStream
                                            match uds_client.keepalive_listen_key(&listen_key).await {
                                                Ok(()) => info!("[UDS] listenKey keepalive OK (PUT /api/v3/userDataStream)."),
                                                Err(e) => {
                                                    warn!("[UDS] keepalive failed: {} — reconnecting…", e);
                                                    break 'stream;
                                                }
                                            }
                                        }
                                        msg = reader.next() => {
                                            match msg {
                                                Some(Ok(Message::Text(text))) => {
                                                    // Step 3: Deserialize executionReport
                                                    if let Ok(report) = serde_json::from_str::<ExecutionReport>(&text) {
                                                        if report.event_type == "executionReport"
                                                            && (report.order_status == "FILLED"
                                                                || report.order_status == "PARTIALLY_FILLED")
                                                        {
                                                            // Step 4: forward to order_manager → real_inventory
                                                            let _ = uds_tx.send(report).await;
                                                        }
                                                    }
                                                }
                                                Some(Ok(Message::Ping(d))) => {
                                                    let _ = writer.send(Message::Pong(d)).await;
                                                }
                                                Some(Ok(Message::Close(_))) | None => {
                                                    // Fix 2: UDS Silent Death — stream dropped.
                                                    // Exit so Docker auto-restarts and fetches a fresh listenKey.
                                                    tracing::error!("CRITICAL: User Data Stream disconnected! Forcing process exit for Docker restart.");
                                                    std::process::exit(1);
                                                }
                                                Some(Err(e)) => {
                                                    tracing::error!("CRITICAL: UDS stream error: {} — forcing process exit for Docker restart.", e);
                                                    std::process::exit(1);
                                                }
                                                _ => {}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // ──────────────────────────────────────────────────────────
                    // Step 2 FALLBACK: WebSocket API (ws-api.binance.com)
                    //   Used when REST endpoint is blocked by regional routing.
                    //   userDataStream.start → listenKey, subscribe → events.
                    // ──────────────────────────────────────────────────────────
                    UdsMode::WsApi => {
                        match connect_async(WS_API_URL).await {
                            Err(e) => {
                                warn!("[UDS] WS-API connect failed: {} — retrying in 5s…", e);
                                sleep(Duration::from_secs(5)).await;
                                continue;
                            }
                            Ok((ws_stream, _)) => {
                                info!("[UDS] WebSocket API connected (fallback path).");
                                let (mut writer, mut reader) = ws_stream.split();

                                // Obtain listenKey via WS API
                                let start_req = serde_json::json!({
                                    "id": "uds-start", "method": "userDataStream.start",
                                    "params": { "apiKey": uds_client.api_key() }
                                }).to_string();
                                if writer.send(Message::Text(start_req)).await.is_err() {
                                    sleep(Duration::from_secs(5)).await;
                                    continue;
                                }

                                let listen_key = {
                                    let mut key = None;
                                    while let Some(Ok(Message::Text(text))) = reader.next().await {
                                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                                            if v["id"] == "uds-start" {
                                                if v["status"] == 200 {
                                                    key = v["result"]["listenKey"].as_str().map(String::from);
                                                } else {
                                                    warn!("[UDS] WS-API start error: {}", text);
                                                }
                                                break;
                                            }
                                        }
                                    }
                                    key
                                };

                                let listen_key = match listen_key {
                                    Some(k) => { info!("[UDS] listenKey obtained via WebSocket API (fallback)."); k }
                                    None => { sleep(Duration::from_secs(5)).await; continue; }
                                };

                                // Subscribe
                                let sub_req = serde_json::json!({
                                    "id": "uds-subscribe", "method": "userDataStream.subscribe",
                                    "params": { "listenKey": &listen_key }
                                }).to_string();
                                if writer.send(Message::Text(sub_req)).await.is_err() {
                                    sleep(Duration::from_secs(5)).await;
                                    continue;
                                }
                                info!("[UDS] WebSocket API subscribed — listening for executionReports.");

                                let mut ping_timer =
                                    tokio::time::interval(Duration::from_secs(30 * 60));
                                ping_timer.tick().await;

                                'wsapi: loop {
                                    tokio::select! {
                                        _ = ping_timer.tick() => {
                                            let ping_req = serde_json::json!({
                                                "id": "uds-ping", "method": "userDataStream.ping",
                                                "params": { "listenKey": &listen_key }
                                            }).to_string();
                                            if writer.send(Message::Text(ping_req)).await.is_err() {
                                                break 'wsapi;
                                            }
                                            info!("[UDS] WS-API ping sent.");
                                        }
                                        msg = reader.next() => {
                                            match msg {
                                                Some(Ok(Message::Text(text))) => {
                                                    if text.contains("\"id\"") { continue 'wsapi; }
                                                    // Step 3: Deserialize executionReport
                                                    if let Ok(report) = serde_json::from_str::<ExecutionReport>(&text) {
                                                        if report.event_type == "executionReport"
                                                            && (report.order_status == "FILLED"
                                                                || report.order_status == "PARTIALLY_FILLED")
                                                        {
                                                            // Step 4: forward to order_manager → real_inventory
                                                            let _ = uds_tx.send(report).await;
                                                        }
                                                    }
                                                }
                                                Some(Ok(Message::Ping(d))) => { let _ = writer.send(Message::Pong(d)).await; }
                                                Some(Ok(Message::Close(_))) | None => {
                                                    tracing::error!("CRITICAL: User Data Stream (WS-API) disconnected! Forcing process exit for Docker restart.");
                                                    std::process::exit(1);
                                                }
                                                Some(Err(e)) => {
                                                    tracing::error!("CRITICAL: UDS WS-API error: {} — forcing process exit for Docker restart.", e);
                                                    std::process::exit(1);
                                                }
                                                _ => {}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                sleep(Duration::from_secs(5)).await;
            }
        });
        info!("[UDS] User Data Stream task spawned (REST+stream primary / WS-API fallback).");
    }

    {
        let client_arc = std::sync::Arc::clone(&binance_client);
        let om_con     = con.clone();
        let om_db      = Arc::clone(&db_client);
        tokio::spawn(async move {
            order_manager(client_arc, order_rx, om_con, fill_tx, om_db, uds_rx).await;
        });
        info!("Order manager task spawned — managing {} coins.", COIN_CONFIGS.len());
    }

    // ── Agent Q Redis subscriber task ─────────────────────────────────────────
    // Subscribes to:
    //   hft:live_params:{symbol}  — Tactical params  (every 15m)
    //   hft:regime:{symbol}       — Oracle regime     (every 5m)
    // Forwards both as AgentQMessage variants via the aq_tx unbounded channel.
    {
        let sub_redis_url = std::env::var("REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

        tokio::spawn(async move {
            loop {
                let result: Result<(), anyhow::Error> = async {
                    let sub_client = redis::Client::open(sub_redis_url.as_str())?;
                    let sub_conn   = sub_client.get_async_connection().await?;
                    let mut pubsub = sub_conn.into_pubsub();

                    for cfg in COIN_CONFIGS {
                        let params_ch = format!("hft:live_params:{}", cfg.stream_prefix);
                        let regime_ch = format!("hft:regime:{}", cfg.stream_prefix);
                        pubsub.subscribe(&params_ch).await?;
                        pubsub.subscribe(&regime_ch).await?;
                        info!("[AgentQ] Subscribed: {} | {}", params_ch, regime_ch);
                    }

                    let mut stream = pubsub.into_on_message();
                    while let Some(msg) = stream.next().await {
                        let channel: String = msg.get_channel_name().to_string();
                        let payload: String = match msg.get_payload() {
                            Ok(p)  => p,
                            Err(e) => { warn!("[AgentQ] Payload error: {}", e); continue; }
                        };

                        // Route by channel prefix
                        if let Some(sfx) = channel.strip_prefix("hft:live_params:") {
                            let symbol = sfx.to_uppercase();
                            match serde_json::from_str::<AgentQParams>(&payload) {
                                Ok(p) => { let _ = aq_tx.send((symbol, AgentQMessage::Params(p))); }
                                Err(e) => warn!("[AgentQ][{}] Params parse error: {} raw:{:.80}", symbol, e, payload),
                            }
                        } else if let Some(sfx) = channel.strip_prefix("hft:regime:") {
                            let symbol = sfx.to_uppercase();
                            match serde_json::from_str::<RegimePayload>(&payload) {
                                Ok(rp) => {
                                    let regime = rp.parse_regime();
                                    let _ = aq_tx.send((symbol, AgentQMessage::Regime(regime)));
                                }
                                Err(e) => warn!("[AgentQ][{}] Regime parse error: {} raw:{:.80}", symbol, e, payload),
                            }
                        }

                        // Exit if receiver dropped
                        if aq_tx.is_closed() { break; }
                    }
                    Ok(())
                }.await;

                if let Err(e) = result {
                    warn!("[AgentQ] Subscriber error: {}. Reconnecting in 5s…", e);
                }
                sleep(Duration::from_secs(5)).await;
            }
        });
        info!("Agent Q Redis subscriber task spawned (params + regime channels).");
    }

    let mut fill_rx = fill_rx;
    let mut aq_rx   = aq_rx;
    let mut backoff = 1u64;
    loop {
        match stream_loop(&mut con, &wss_url, order_tx.clone(), &mut fill_rx, &mut aq_rx).await {
            Ok(_)  => { info!("Stream loop exited cleanly."); backoff = 1; }
            Err(e) => { warn!("Stream loop error: {}. Reconnecting in {}s…", e, backoff); }
        }
        sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(30);
    }
}
