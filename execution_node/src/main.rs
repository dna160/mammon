// ============================================================
// main.rs — Mammon V3 Execution Node
//
// Architecture replicated from mammond/project_sniper:
//
//   HOT PATH (bookTicker WSS, μs latency):
//     tick → sniper_hft.tick() → send OrderCmd to mpsc channel
//     NEVER awaits REST. Zero-copy fire-and-forget.
//
//   ORDER MANAGER TASK (cold path, sequential):
//     recv OrderCmd → moved_enough() + REQUOTE_MIN_MS guard
//     → cancel only if order exists → place new LIMIT_MAKER
//     Naturally serialises all REST calls → max ~5 req/s
//
//   UDS TASK (fill notifications, weight=2 on init only):
//     Primary:  REST POST /api/v3/userDataStream → listenKey
//     Fallback: WS-API wss://ws-api.binance.com:443/ws-api/v3
//               (obtains listenKey when REST is 410-blocked)
//     Both cases then connect stream.binance.com:9443/ws/<key>
//
//   RESULT: bookTicker ticks never burn API weight.
//           REST calls only fire when price actually moves.
// ============================================================

use anyhow::Result;
use dotenvy::dotenv;
use futures_util::{SinkExt, StreamExt};
use redis::AsyncCommands;
use serde::Deserialize;
use std::{env, sync::Arc, time::Instant};
use tokio::{
    sync::{mpsc, Mutex},
    time::{self, Duration},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

mod binance_v3;
mod sniper_hft;

use binance_v3::BinanceClient;
use sniper_hft::{LiveParams, SniperEngine};

// ── Constants ─────────────────────────────────────────────────────────────────

// Minimum ms between any two REST order operations per side.
// Matches mammond REQUOTE_MIN_MS. Max ~5 REST ops/s → never hits 100/s limit.
const REQUOTE_MIN_MS: u128 = 200;

// Minimum price drift (in ticks) before a requote fires.
// Matches mammond default tolerance_ticks. Prevents cancel/replace storms
// when price oscillates within a single tick.
const TOLERANCE_TICKS: f64 = 1.0;

const TRANCHE_USD:               f64 = 6.00;
const LISTEN_KEY_KEEPALIVE_SECS: u64 = 30 * 60;
const REDIS_POLL_SECS:           u64 = 30;

// SOLFDUSD market constants (mammond COIN_CONFIGS)
const SOL_TICK_SIZE: f64 = 0.01;
const SOL_LOT_STEP:  f64 = 0.01;

fn wss_base() -> String {
    std::env::var("BINANCE_WS_BASE")
        .unwrap_or_else(|_| "wss://stream.binance.com:9443".to_string())
}

const WS_API_URL: &str = "wss://ws-api.binance.com:443/ws-api/v3";

// ── Order Command Channel ─────────────────────────────────────────────────────
// Sent from the bookTicker hot path → consumed by the order manager task.
// Hot path NEVER awaits REST — it just sends and moves on.

#[derive(Debug)]
enum OrderCmd {
    Requote { bid_price: Option<f64>, ask_price: Option<f64> },
    Emergency,
}

// ── Order Manager State (per-side) ────────────────────────────────────────────

struct OrderState {
    bid_id:       Option<u64>,
    ask_id:       Option<u64>,
    last_bid:     f64,
    last_ask:     f64,
    last_requote: Instant,
    inventory:    f64,
    aep:          f64,
}

impl OrderState {
    fn new() -> Self {
        // Set last_requote to 60s in the past so the first tick fires immediately
        let past = Instant::now()
            .checked_sub(Duration::from_secs(60))
            .unwrap_or_else(Instant::now);
        Self {
            bid_id:       None,
            ask_id:       None,
            last_bid:     0.0,
            last_ask:     0.0,
            last_requote: past,
            inventory:    0.0,
            aep:          0.0,
        }
    }
}

// ── Price drift guard — replicated from mammond moved_enough() ────────────────
// Only requote when the price has drifted by >= tolerance_ticks × tick_size.
// High tolerance = fewer REST calls, better queue position.
#[inline]
fn moved_enough(new_price: f64, old_price: f64, tick_size: f64) -> bool {
    if old_price == 0.0 { return true; }
    (new_price - old_price).abs() >= tick_size * TOLERANCE_TICKS
}

// ── UDS ExecutionReport ───────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
struct ExecutionReport {
    #[serde(rename = "e")] event_type:    String,
    #[serde(rename = "s")] symbol:        String,
    #[serde(rename = "S")] side:          String,
    #[serde(rename = "X")] order_status:  String,
    #[serde(rename = "i")] order_id:      u64,
    #[serde(rename = "l")] last_filled_qty:   String,
    #[serde(rename = "L")] last_filled_price: String,
}

// ── BookTicker ────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct BookTickerMsg {
    #[serde(rename = "b")] bid:     String,
    #[serde(rename = "B")] bid_qty: String,
    #[serde(rename = "a")] ask:     String,
    #[serde(rename = "A")] ask_qty: String,
}

// ── Shared sniper state (engine + Redis params) ───────────────────────────────

struct SharedState {
    engine: SniperEngine,
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "execution_node=info,warn".to_string()),
        )
        .with_target(false)
        .init();

    info!("▶ Mammon V3 Execution Node — mammond channel architecture");

    let api_key      = env::var("BINANCE_API_KEY").expect("BINANCE_API_KEY not set");
    let api_secret   = env::var("BINANCE_API_SECRET").expect("BINANCE_API_SECRET not set");
    let symbol       = env::var("SYMBOL").unwrap_or_else(|_| "SOLFDUSD".to_string());
    let redis_url    = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let postgres_url = env::var("POSTGRES_URL").expect("POSTGRES_URL not set");

    // ── PostgreSQL ────────────────────────────────────────────────────────────
    let (pg_client, pg_conn) = tokio_postgres::connect(&postgres_url, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = pg_conn.await { error!("PostgreSQL connection error: {}", e); }
    });
    let pg_client = Arc::new(pg_client);
    info!("✓ PostgreSQL connected");

    // ── Redis ─────────────────────────────────────────────────────────────────
    let redis_client = redis::Client::open(redis_url.as_str())?;
    let mut redis_con = loop {
        match redis_client.get_multiplexed_tokio_connection().await {
            Ok(c)  => { info!("✓ Redis connected"); break c; }
            Err(e) => { warn!("Redis connect failed: {}. Retrying in 2s…", e); time::sleep(Duration::from_secs(2)).await; }
        }
    };

    // ── Binance REST client ───────────────────────────────────────────────────
    let binance = Arc::new(BinanceClient::new(&api_key, &api_secret));

    // ── Shared state ──────────────────────────────────────────────────────────
    let state = Arc::new(Mutex::new(SharedState {
        engine: SniperEngine::new(&symbol, SOL_TICK_SIZE, SOL_LOT_STEP),
    }));
    info!("✓ Engine initialised (tick={} lot={}) — SOLFDUSD defaults", SOL_TICK_SIZE, SOL_LOT_STEP);

    // ── Spawn: async exchangeInfo fetch ───────────────────────────────────────
    {
        let b2 = binance.clone();
        let s2 = state.clone();
        let sym = symbol.clone();
        tokio::spawn(async move {
            match b2.get_tick_info(&sym).await {
                Ok(ti) => {
                    let mut s = s2.lock().await;
                    s.engine.tick_size = ti.tick_size;
                    s.engine.lot_step  = ti.lot_step;
                    info!("✓ exchangeInfo: tick={} lot={}", ti.tick_size, ti.lot_step);
                }
                Err(e) => warn!("exchangeInfo unavailable ({}). Using SOLFDUSD defaults.", e),
            }
        });
    }

    // ── Spawn: Redis param poller ─────────────────────────────────────────────
    {
        let s2  = state.clone();
        let sym = symbol.clone();
        let mut rc = redis_client.get_multiplexed_async_connection().await?;
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(REDIS_POLL_SECS));
            loop {
                interval.tick().await;
                let key = format!("hft:live_params:{}", sym);
                let result: redis::RedisResult<String> = rc.get(&key).await;
                if let Ok(json) = result {
                    if let Ok(params) = serde_json::from_str::<LiveParams>(&json) {
                        let mut s = s2.lock().await;
                        s.engine.update_params(&params);
                        info!("↻ Redis params updated: {:?}", params);
                    }
                }
            }
        });
    }

    // ── mpsc channel: hot path → order manager ────────────────────────────────
    // Buffer=256 matches mammond. If the order manager falls behind, oldest
    // cmds are dropped — always better to have fresh prices than stale ones.
    let (order_tx, order_rx) = mpsc::channel::<OrderCmd>(256);

    // ── mpsc channel: UDS → order manager ────────────────────────────────────
    let (uds_tx, uds_rx) = mpsc::channel::<ExecutionReport>(1024);

    // ── Spawn: UDS task (REST primary / WS-API fallback) ─────────────────────
    // Replicated from mammond lines 1286-1556.
    {
        let b2   = binance.clone();
        let sym2 = symbol.clone();
        let wss  = wss_base();
        tokio::spawn(async move {
            loop {
                // Step 1: obtain listenKey (REST primary → WS-API fallback)
                enum UdsMode { Stream(String), WsApi }

                let mode = match b2.get_listen_key().await {
                    Ok(k) => {
                        info!("[UDS] listenKey via REST");
                        UdsMode::Stream(k)
                    }
                    Err(e) => {
                        warn!("[UDS] REST get_listen_key failed: {} → WS-API fallback", e);
                        UdsMode::WsApi
                    }
                };

                let listen_key: Option<String> = match mode {
                    UdsMode::Stream(k) => Some(k),
                    UdsMode::WsApi => {
                        // Obtain listenKey via wss://ws-api.binance.com
                        match connect_async(WS_API_URL).await {
                            Err(e) => {
                                warn!("[UDS] WS-API connect failed: {}. Retry in 10s.", e);
                                time::sleep(Duration::from_secs(10)).await;
                                continue;
                            }
                            Ok((ws, _)) => {
                                let (mut writer, mut reader) = ws.split();
                                let req = serde_json::json!({
                                    "id": "uds-start",
                                    "method": "userDataStream.start",
                                    "params": { "apiKey": b2.api_key() }
                                }).to_string();
                                if writer.send(Message::Text(req)).await.is_err() {
                                    time::sleep(Duration::from_secs(5)).await;
                                    continue;
                                }
                                let mut key = None;
                                while let Some(Ok(Message::Text(text))) = reader.next().await {
                                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                                        if v["id"] == "uds-start" {
                                            if v["status"] == 200 {
                                                key = v["result"]["listenKey"].as_str().map(String::from);
                                                info!("[UDS] listenKey via WS-API");
                                            } else {
                                                warn!("[UDS] WS-API start error: {}", text);
                                            }
                                            break;
                                        }
                                    }
                                }
                                key
                            }
                        }
                    }
                };

                let listen_key = match listen_key {
                    Some(k) => k,
                    None    => { time::sleep(Duration::from_secs(10)).await; continue; }
                };

                // Step 2: connect stream.binance.com/ws/<key>
                let stream_url = format!("{}/ws/{}", wss, listen_key);
                info!("[UDS] Connecting: {}/ws/<key>", wss);

                let (ws, _) = match connect_async(&stream_url).await {
                    Ok(r)  => r,
                    Err(e) => {
                        warn!("[UDS] stream connect failed: {}. Retry in 5s.", e);
                        time::sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };
                info!("[UDS] Connected — receiving executionReports");
                let (mut writer, mut reader) = ws.split();

                // Keepalive every 30 min
                let b3 = b2.clone();
                let lk = listen_key.clone();
                let keepalive_hdl = tokio::spawn(async move {
                    let mut iv = time::interval(Duration::from_secs(LISTEN_KEY_KEEPALIVE_SECS));
                    iv.tick().await;
                    loop {
                        iv.tick().await;
                        match b3.keepalive_listen_key(&lk).await {
                            Ok(_)  => info!("[UDS] listenKey keepalive OK"),
                            Err(e) => { warn!("[UDS] keepalive failed: {}", e); break; }
                        }
                    }
                });

                // Receive executionReports
                while let Some(msg) = reader.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            // Fast path: skip non-executionReport frames immediately
                            if !text.contains("\"executionReport\"") { continue; }
                            match serde_json::from_str::<ExecutionReport>(&text) {
                                Ok(r) if r.order_status == "FILLED" || r.order_status == "PARTIALLY_FILLED" => {
                                    let _ = uds_tx.send(r).await;
                                }
                                Ok(_)  => {}
                                Err(e) => warn!("[UDS] parse error: {} raw={:.80}", e, text),
                            }
                        }
                        Ok(Message::Ping(d)) => { let _ = writer.send(Message::Pong(d)).await; }
                        Ok(Message::Close(_)) | Err(_) => {
                            error!("[UDS] Stream disconnected — reconnecting");
                            break;
                        }
                        _ => {}
                    }
                }
                keepalive_hdl.abort();
                time::sleep(Duration::from_secs(5)).await;
            }
        });
        info!("[UDS] Task spawned (REST primary / WS-API fallback)");
    }

    // ── Spawn: Order Manager task ─────────────────────────────────────────────
    // Sequential REST executor — replicated from mammond order_manager().
    // This is the ONLY task that calls Binance REST. All weight is here.
    {
        let b2         = binance.clone();
        let sym2       = symbol.clone();
        let pg2        = pg_client.clone();
        let tick       = SOL_TICK_SIZE;
        let lot        = SOL_LOT_STEP;
        let shared     = state.clone();          // <-- engine inventory sync
        tokio::spawn(async move {
            order_manager(b2, order_rx, uds_rx, pg2, sym2, tick, lot, shared).await;
        });
        info!("✓ Order manager spawned");
    }

    // ── MAIN: bookTicker hot path ─────────────────────────────────────────────
    // Connects, streams ticks, fires OrderCmd to channel. Zero REST here.
    let book_url = format!("{}/ws/{}@bookTicker", wss_base(), symbol.to_lowercase());
    info!("▶ bookTicker WSS: {}", book_url);

    let mut backoff = 1u64;
    loop {
        run_book_ticker(&book_url, state.clone(), order_tx.clone()).await;
        warn!("bookTicker disconnected — reconnecting in {}s", backoff);
        time::sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(30);
    }
}

// ── bookTicker hot path ───────────────────────────────────────────────────────
// This function NEVER touches REST. It only:
//   1. Parses a bookTicker JSON frame (μs)
//   2. Runs sniper_hft.tick() (μs, pure math)
//   3. Sends an OrderCmd to the mpsc channel (μs, non-blocking)

async fn run_book_ticker(
    url:      &str,
    state:    Arc<Mutex<SharedState>>,
    order_tx: mpsc::Sender<OrderCmd>,
) {
    let parsed = match url::Url::parse(url) {
        Ok(u)  => u,
        Err(e) => { error!("URL parse failed: {}", e); return; }
    };
    let (ws, _) = match connect_async(parsed).await {
        Ok(r)  => r,
        Err(e) => { error!("bookTicker connect failed: {}", e); return; }
    };
    info!("✓ bookTicker connected");
    let (_, mut reader) = ws.split();

    while let Some(msg) = reader.next().await {
        let text = match msg {
            Ok(Message::Text(t))  => t,
            Ok(Message::Close(_)) => { info!("bookTicker closed by server"); break; }
            Err(e) => { error!("bookTicker WS error: {}", e); break; }
            _ => continue,
        };

        let ticker: BookTickerMsg = match serde_json::from_str(&text) {
            Ok(t)  => t,
            Err(_) => continue,
        };

        let best_bid: f64 = ticker.bid.parse().unwrap_or(0.0);
        let bid_vol:  f64 = ticker.bid_qty.parse().unwrap_or(0.0);
        let best_ask: f64 = ticker.ask.parse().unwrap_or(0.0);
        let ask_vol:  f64 = ticker.ask_qty.parse().unwrap_or(0.0);

        if best_bid <= 0.0 || best_ask <= 0.0 { continue; }

        // Run sniper engine (pure math, μs, no I/O)
        let cmd = {
            let mut s = state.lock().await;
            s.engine.tick(best_bid, bid_vol, best_ask, ask_vol);

            if s.engine.emergency_dump_triggered {
                Some(OrderCmd::Emergency)
            } else if s.engine.open_bid.is_some() || s.engine.open_ask.is_some() {
                Some(OrderCmd::Requote {
                    bid_price: s.engine.open_bid,
                    ask_price: s.engine.open_ask,
                })
            } else {
                None
            }
        };

        // Non-blocking send — if channel full, drop (stale price, irrelevant)
        if let Some(cmd) = cmd {
            let _ = order_tx.try_send(cmd);
        }
    }
}

// ── Order Manager Task ────────────────────────────────────────────────────────
// The ONLY place that issues REST calls.
// Enforces moved_enough() + REQUOTE_MIN_MS before every cancel/place.
// Processes UDS fills via tokio::select! — identical to mammond order_manager().

async fn order_manager(
    binance:   Arc<BinanceClient>,
    mut rx:    mpsc::Receiver<OrderCmd>,
    mut uds_rx: mpsc::Receiver<ExecutionReport>,
    pg:        Arc<tokio_postgres::Client>,
    symbol:    String,
    tick_size: f64,
    lot_step:  f64,
    shared:    Arc<Mutex<SharedState>>,   // engine inventory sync
) {
    let mut state = OrderState::new();

    // Cancel all stale orders from a previous session
    let _ = binance.cancel_all_orders(&symbol).await;
    info!("[OM] Startup — stale orders cleared for {}", symbol);

    loop {
        tokio::select! {

            // ── Priority 1: UDS fill notifications (zero-latency) ─────────────
            Some(report) = uds_rx.recv() => {
                let qty:   f64 = report.last_filled_qty.parse().unwrap_or(0.0);
                let price: f64 = report.last_filled_price.parse().unwrap_or(0.0);
                if qty <= 0.0 || price <= 0.0 { continue; }

                info!("✓ FILL {} {:.5} @ {:.4} [orderId={} status={}]",
                    report.side, qty, price, report.order_id, report.order_status);

                // Update local order-manager inventory mirror
                match report.side.as_str() {
                    "BUY" => {
                        let old_notional = state.inventory * state.aep;
                        state.inventory += qty;
                        if state.inventory > 0.0 {
                            state.aep = (old_notional + qty * price) / state.inventory;
                        }
                        if report.order_status == "FILLED" && state.bid_id == Some(report.order_id) {
                            state.bid_id  = None;
                            state.last_bid = 0.0;
                        }
                    }
                    "SELL" => {
                        state.inventory = (state.inventory - qty).max(0.0);
                        if state.inventory <= 0.0 { state.aep = 0.0; }
                        if report.order_status == "FILLED" && state.ask_id == Some(report.order_id) {
                            state.ask_id  = None;
                            state.last_ask = 0.0;
                        }
                    }
                    _ => {}
                }

                // CRITICAL: sync engine inventory so bookTicker tick() sees the fill
                // and generates open_ask on next tick
                {
                    let mut eng = shared.lock().await;
                    eng.engine.on_fill(qty, price, &report.side);
                }

                // Background telemetry insert
                {
                    let pool   = pg.clone();
                    let sym    = symbol.clone();
                    let side   = report.side.clone();
                    let notional = qty * price;
                    tokio::spawn(async move {
                        let _ = pool.execute(
                            "INSERT INTO trade_telemetry \
                             (symbol, side, filled_qty, filled_price, trade_size_usd, \
                              gross_pnl, fees_paid, net_pnl) \
                             VALUES ($1, $2, $3, $4, $5, 0.0, 0.0, 0.0)",
                            &[&sym, &side, &qty, &price, &notional],
                        ).await;
                    });
                }
            }

            // ── Priority 2: OrderCmd from bookTicker hot path ─────────────────
            Some(cmd) = rx.recv() => {
                match cmd {

                    // ── Emergency market dump ─────────────────────────────────
                    OrderCmd::Emergency => {
                        error!("[OM] 🚨 EMERGENCY DUMP — cancelling all, market selling {:.5} {}",
                            state.inventory, symbol);
                        let _ = binance.cancel_all_orders(&symbol).await;
                        let qty = BinanceClient::floor_to_lot(state.inventory, lot_step);
                        if qty > 0.0 { let _ = binance.market_sell(&symbol, qty).await; }
                        state.bid_id    = None;
                        state.ask_id    = None;
                        state.last_bid  = 0.0;
                        state.last_ask  = 0.0;
                        state.inventory = 0.0;
                        state.aep       = 0.0;
                        state.last_requote = Instant::now();
                    }

                    // ── Requote (bid + ask, one or both sides) ────────────────
                    // Matches mammond OrderCmd::Requote handling:
                    //   1. Check moved_enough() per side
                    //   2. Check REQUOTE_MIN_MS time gate
                    //   3. Cancel old order only if id exists
                    //   4. Place new LIMIT_MAKER
                    OrderCmd::Requote { bid_price, ask_price } => {

                        let bid_moved = bid_price.map_or(false, |p| moved_enough(p, state.last_bid, tick_size));
                        let ask_moved = ask_price.map_or(false, |p| moved_enough(p, state.last_ask, tick_size));

                        // Neither side moved — skip entirely, no REST calls
                        if !bid_moved && !ask_moved { continue; }

                        // Time gate — max 5 REST ops/s
                        if state.last_requote.elapsed().as_millis() < REQUOTE_MIN_MS { continue; }

                        // ── BID side ──────────────────────────────────────────
                        if bid_moved {
                            if let Some(new_bid) = bid_price {
                                let bid_price_r = BinanceClient::round_to_tick(new_bid, tick_size);

                                // Cancel existing bid only if it actually exists
                                if let Some(old_id) = state.bid_id.take() {
                                    let _ = binance.cancel_order(&symbol, old_id).await;
                                    state.last_bid = 0.0;
                                }

                                // Size tranche to fixed notional
                                let qty = BinanceClient::floor_to_lot(TRANCHE_USD / bid_price_r, lot_step);
                                if qty > 0.0 {
                                    match binance.place_limit_maker(&symbol, "BUY", qty, bid_price_r).await {
                                        Ok(id) => {
                                            state.bid_id  = Some(id);
                                            state.last_bid = bid_price_r;
                                        }
                                        Err(e) => warn!("[OM] BID failed: {}", e),
                                    }
                                }
                            } else {
                                // Engine cleared the bid — cancel if live
                                if let Some(old_id) = state.bid_id.take() {
                                    let _ = binance.cancel_order(&symbol, old_id).await;
                                    state.last_bid = 0.0;
                                }
                            }
                        }

                        // ── ASK side ──────────────────────────────────────────
                        if ask_moved {
                            if let Some(new_ask) = ask_price {
                                let ask_price_r = BinanceClient::round_to_tick(new_ask, tick_size);

                                if let Some(old_id) = state.ask_id.take() {
                                    let _ = binance.cancel_order(&symbol, old_id).await;
                                    state.last_ask = 0.0;
                                }

                                let qty = BinanceClient::floor_to_lot(state.inventory, lot_step);
                                if qty > 0.0 {
                                    match binance.place_limit_maker(&symbol, "SELL", qty, ask_price_r).await {
                                        Ok(id) => {
                                            state.ask_id  = Some(id);
                                            state.last_ask = ask_price_r;
                                        }
                                        Err(e) => warn!("[OM] ASK failed: {}", e),
                                    }
                                }
                            } else {
                                if let Some(old_id) = state.ask_id.take() {
                                    let _ = binance.cancel_order(&symbol, old_id).await;
                                    state.last_ask = 0.0;
                                }
                            }
                        }

                        state.last_requote = Instant::now();
                    }
                } // match cmd
            }

        } // select!
    } // loop
}
