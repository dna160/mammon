//! Engine D — Avellaneda-Stoikov HFT Market Maker (multi-coin, Binance).
//!
//! Instantiate one HFTEngine per trading pair; each carries its own exchange
//! physics (tick_size, lot_step, max_inventory_coin) so the AS math scales
//! correctly across BTC, ADA, DOT, DOGE, and XRP simultaneously.
//!
//! Mathematical framework:
//!   F1  Micro-Price:    P_micro = (V_ask·P_bid + V_bid·P_ask) / (V_bid + V_ask)
//!   F2  OBI:            (V_bid − V_ask) / (V_bid + V_ask)  ∈ [−1, +1]
//!   F3  TFI ring-buf:   Σ V_buy_market − Σ V_sell_market  (rolling 1 s, coin)
//!   F4  EWMA σ²:        σ²_t = 0.99·σ²_{t-1} + 0.01·(ΔP_micro)²
//!   F5  Reservation:    r = P_micro − (q · γ · σ²)
//!   F6  Half-spread:    δ = max(tick_size + γ·σ², tick_size · MIN_SPREAD_TICKS)
//!                       P*_bid = snap(r − δ, tick_size)
//!                       P*_ask = snap(r + δ, tick_size)
//!
//! State machine (shadow check is FIRST — overrides everything):
//!   |TFI| > TFI_SHADOW_THRESHOLD     → shadow mode (pull all quotes)
//!   inventory ≥  max_inventory_coin  → open_bid = None  (long clamped, dump)
//!   inventory ≤ −max_inventory_coin  → open_ask = None  (short clamped, cover)
//!   else                             → symmetric AS quotes both sides
//!
//! Panic stop-loss:
//!   inventory > 0 AND micro_price < last_fill_price * 0.9985
//!   → is_panic = true → caller fires MARKET SELL immediately.

// ── Module-level parameters (shared across all coin engines) ──────────────────

/// EWMA decay α for HF variance (≈ 100-tick half-life).
const ALPHA: f64 = 0.01;

// Default values for the live engine parameters (overridable via Agent Q every 15m).
const DEFAULT_LIVE_GAMMA:         f64 = 0.8;
const DEFAULT_LIVE_MIN_SPREAD:    f64 = 5.0;
const DEFAULT_LIVE_TFI_THRESHOLD: f64 = 65_000.0; // USD notional — safe default until Agent Q sets it

// ── Market Regime (injected by Oracle every 5m) ───────────────────────────────

/// Five-state regime classification produced by Agent Q's Oracle loop.
/// Dictates the structural quoting playbook inside tick().
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketRegime {
    /// Low volatility, balanced flow — standard 2-sided quoting.
    MeanReverting,
    /// Retail momentum up — accumulate inventory, pull asks.
    RetailFrenzyUp,
    /// Institutional distribution — shadow bids, only quote asks.
    InstitutionalAbsorptionDown,
    /// Thin/stale book — quote both sides but spread forced ≥ 20 ticks.
    DeadZone,
    /// Cascading liquidations — hard stop, panic-sell all inventory.
    ToxicLiquidationCascade,
}

/// Panic stop-loss threshold: 0.15% drawdown below last fill price.
const PANIC_DRAWDOWN: f64 = 0.9985;

/// Fixed-size ring buffer for TFI rolling window (zero-allocation).
const TFI_CAP: usize = 512;

/// Rolling TFI window in milliseconds.
const TFI_WINDOW_MS: u64 = 1_000;

// ── Fill side ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillSide { Buy, Sell }

// ── Tick output ───────────────────────────────────────────────────────────────

/// Plain-data result of one tick.  No heap allocation.
#[derive(Debug, Clone, Copy)]
pub struct TickOutput {
    pub micro_price:       f64,
    pub obi:               f64,
    pub tfi:               f64,
    pub variance:          f64,
    pub reservation_price: f64,
    pub optimal_bid:       f64,
    pub optimal_ask:       f64,
    pub open_bid:          Option<f64>,
    pub open_ask:          Option<f64>,
    pub inventory_coin:    f64,
    pub pnl_usd:           f64,
    pub total_trades:      u64,
    pub fill_this_tick:    Option<FillSide>,
    pub warm_ticks:        u64,
    /// True when micro_price drops 0.15% below last_fill_price with long inventory.
    pub is_panic:          bool,
}

// ── Engine ────────────────────────────────────────────────────────────────────

/// Avellaneda-Stoikov HFT Engine — stateful, synchronous, zero-allocation.
///
/// Create one per symbol via `HFTEngine::new(symbol, tick_size, lot_step, max_inv)`.
/// Agent Q injects updated risk parameters at runtime via `update_params()`.
pub struct HFTEngine {
    // ── Exchange physics (per-coin, set at construction) ──────────────────────
    pub symbol:             String,
    pub tick_size:          f64,
    pub lot_step:           f64,
    pub max_inventory_coin: f64,

    // ── Agent Q Oracle (every 5m) — structural quoting playbook ─────────────
    /// Current market regime — drives which sides are quoted.
    pub current_regime: MarketRegime,

    // ── Agent Q Tactical (every 15m) — live math parameters ──────────────────
    /// Risk-aversion γ — reservation-price skew and spread width.
    pub live_gamma:          f64,
    /// Minimum spread in ticks — latency defense buffer.
    pub live_min_spread:     f64,
    /// TFI threshold — coin/s above which toxic flow is considered extreme.
    pub live_tfi_threshold:  f64,

    // ── Engine state ──────────────────────────────────────────────────────────
    /// Net coin inventory.  Positive = long, negative = short.
    pub inventory_coin: f64,

    /// EWMA high-frequency variance σ² of micro-price returns (FDUSD²).
    pub variance: f64,

    /// Micro-price from the previous tick (for ΔP variance update).
    pub last_micro_price: f64,

    /// Price of the last confirmed buy fill — panic stop-loss anchor.
    pub last_fill_price: f64,

    /// Current active bid price (None = no active bid).
    pub open_bid: Option<f64>,

    /// Current active ask price (None = no active ask).
    pub open_ask: Option<f64>,

    /// Accumulated confirmed PnL in FDUSD.
    pub pnl_usd: f64,

    /// Total confirmed fills (both sides).
    pub total_trades: u64,

    /// Number of bookTicker ticks processed (warm-up gate: ≥ 20).
    pub warm_ticks: u64,

    /// Ticks held in long inventory — Stale Inventory Dump gate (PRD §5).
    /// Resets to 0 on every BUY fill or when inventory drops to flat.
    /// If > 600 (~60 seconds), profit floor is surrendered to free frozen capital.
    pub ticks_held: u64,

    // ── TFI ring buffer (zero-allocation) ────────────────────────────────────
    tfi_vols: [f64; TFI_CAP],
    tfi_ts:   [u64; TFI_CAP],
    tfi_sign: [i8;  TFI_CAP],
    tfi_head: usize,
    tfi_len:  usize,
}

impl HFTEngine {
    /// Construct a new engine for the given symbol and exchange physics.
    ///
    /// * `symbol`             — e.g. "BTCFDUSD"
    /// * `tick_size`          — minimum price increment (e.g. 0.01 for BTC)
    /// * `lot_step`           — minimum qty increment (e.g. 0.00001 for BTC)
    /// * `max_inventory_coin` — clamp threshold before one-sided quoting kicks in
    pub fn new(
        symbol:             String,
        tick_size:          f64,
        lot_step:           f64,
        max_inventory_coin: f64,
    ) -> Self {
        Self {
            symbol,
            tick_size,
            lot_step,
            max_inventory_coin,
            current_regime:    MarketRegime::MeanReverting,
            live_gamma:        DEFAULT_LIVE_GAMMA,
            live_min_spread:   DEFAULT_LIVE_MIN_SPREAD,
            live_tfi_threshold: DEFAULT_LIVE_TFI_THRESHOLD,
            inventory_coin:   0.0,
            variance:         0.0,
            last_micro_price: 0.0,
            last_fill_price:  0.0,
            open_bid:         None,
            open_ask:         None,
            pnl_usd:          0.0,
            total_trades:     0,
            warm_ticks:       0,
            ticks_held:       0,
            tfi_vols: [0.0; TFI_CAP],
            tfi_ts:   [0;   TFI_CAP],
            tfi_sign: [0;   TFI_CAP],
            tfi_head: 0,
            tfi_len:  0,
        }
    }

    /// Apply Agent Q Tactical parameters (every 15m) injected via Redis.
    ///
    /// Called from the main thread between ticks — no locking needed.
    pub fn update_params(&mut self, gamma: f64, min_spread: f64, tfi_threshold: f64) {
        self.live_gamma        = gamma.clamp(0.1, 1.0);
        self.live_min_spread   = min_spread.clamp(5.0, 50.0);
        self.live_tfi_threshold = tfi_threshold.clamp(200.0, 150_000.0); // USD notional bounds (PRD §4)
    }

    /// Apply Agent Q Oracle regime (every 5m) injected via Redis.
    ///
    /// Called from the main thread between ticks — no locking needed.
    pub fn update_regime(&mut self, regime: MarketRegime) {
        self.current_regime = regime;
    }

    /// Insert an aggTrade into the TFI ring buffer WITHOUT computing quotes.
    ///
    /// Stores USD notional (qty × price) instead of raw coin volume so TFI
    /// scales equally across BTC (~$85k/coin) and DOGE (~$0.09/coin).
    /// `is_buyer_maker = true` → seller was aggressor; `false` → buyer aggressor.
    pub fn record_agg_trade(&mut self, qty: f64, price: f64, is_buyer_maker: bool, now_ms: u64) {
        if qty <= 0.0 || price <= 0.0 { return; }
        let notional_volume = qty * price;   // USD notional — normalizes across all pairs
        let sign: i8 = if !is_buyer_maker { 1 } else { -1 };
        let idx = self.tfi_head % TFI_CAP;
        self.tfi_vols[idx] = notional_volume;  // store notional, not raw qty
        self.tfi_ts[idx]   = now_ms;
        self.tfi_sign[idx] = sign;
        self.tfi_head = (self.tfi_head + 1) % TFI_CAP;
        if self.tfi_len < TFI_CAP { self.tfi_len += 1; }
    }

    /// Apply a confirmed Binance fill to inventory, PnL, and last_fill_price.
    pub fn record_real_fill(&mut self, is_buy: bool, price: f64, qty: f64, fee_usd: f64) {
        let notional = price * qty;
        if is_buy {
            self.inventory_coin += qty;
            self.pnl_usd        -= notional + fee_usd;
            self.last_fill_price = price;
            self.ticks_held      = 0;   // fresh fill — reset stale counter
        } else {
            self.inventory_coin -= qty;
            self.pnl_usd        += notional - fee_usd;
            self.ticks_held      = 0;   // sold — back to flat, reset counter
        }
        self.total_trades += 1;
    }

    /// Process one market-data tick (driven by bookTicker).
    ///
    /// Returns TickOutput including `is_panic` flag.  When is_panic=true the
    /// caller must immediately dispatch OrderCmd::PanicSell to the order manager.
    pub fn tick(
        &mut self,
        best_bid: f64,
        bid_vol:  f64,
        best_ask: f64,
        ask_vol:  f64,
        now_ms:   u64,
    ) -> TickOutput {

        // ── F1: Micro-Price ───────────────────────────────────────────────────
        let total_vol = bid_vol + ask_vol;
        let micro_price = if total_vol > 0.0 {
            (ask_vol * best_bid + bid_vol * best_ask) / total_vol
        } else {
            (best_bid + best_ask) * 0.5
        };

        // ── F2: Order-Book Imbalance ──────────────────────────────────────────
        let obi = if total_vol > 0.0 {
            (bid_vol - ask_vol) / total_vol
        } else {
            0.0
        };

        // ── F4: EWMA High-Frequency Variance ─────────────────────────────────
        if self.last_micro_price > 0.0 {
            let delta_p = micro_price - self.last_micro_price;
            self.variance = (1.0 - ALPHA) * self.variance + ALPHA * delta_p * delta_p;
            self.warm_ticks += 1;
        }
        self.last_micro_price = micro_price;

        // ── F3: Trade-Flow Imbalance (pre-accumulated ring buffer) ────────────
        let tfi: f64 = {
            let mut sum = 0.0_f64;
            let mut i   = 0_usize;
            while i < self.tfi_len {
                let real_idx = if self.tfi_head >= self.tfi_len {
                    (self.tfi_head - self.tfi_len + i) % TFI_CAP
                } else {
                    (TFI_CAP + self.tfi_head - self.tfi_len + i) % TFI_CAP
                };
                if now_ms.saturating_sub(self.tfi_ts[real_idx]) <= TFI_WINDOW_MS {
                    sum += self.tfi_sign[real_idx] as f64 * self.tfi_vols[real_idx];
                }
                i += 1;
            }
            sum
        };

        // ── F5: Reservation Price (live_gamma from Agent Q Tactical) ─────────
        let reservation_price =
            micro_price - (self.inventory_coin * self.live_gamma * self.variance);

        // ── F6: Spread (live_min_spread from Agent Q Tactical) ────────────────
        let ts           = self.tick_size;
        let raw_delta    = ts + (self.live_gamma * self.variance);
        let spread_delta = raw_delta.max(ts * self.live_min_spread);

        let snap        = |p: f64| -> f64 { (p / ts).round() * ts };
        let optimal_bid = snap(reservation_price - spread_delta);
        let optimal_ask = snap(reservation_price + spread_delta);

        // ── Execution: TFI Shield → Strict Ping-Pong ─────────────────────────
        let warmed = self.warm_ticks >= 20;

        if !warmed {
            // Gate: not enough ticks to trust variance estimate.
            self.open_bid = None;
            self.open_ask = None;
        } else if tfi.abs() > self.live_tfi_threshold {
            // TFI Toxic Flow Shield — outermost guard, overrides everything.
            self.open_bid = None;
            self.open_ask = None;
        } else if self.inventory_coin >= self.lot_step {
            // STRICT EXIT MODE: we hold ≥1 lot — only sell, never buy.
            // Prevents multi-tranche accumulation and inventory amnesia loops.

            // ── Stale Inventory Dump (PRD §5) ────────────────────────────────
            // Track ticks held. If > 600 (~60 seconds of order book activity),
            // surrender the profit floor so AS math can dump at break-even or
            // micro-loss to instantly free frozen capital for the next cycle.
            self.ticks_held += 1;
            let min_profit_price = if self.ticks_held > 600 {
                0.0  // Surrender floor — accept break-even / micro-loss for velocity
            } else {
                self.last_fill_price + self.tick_size  // Target 1-tick pure profit
            };
            self.open_bid = None;
            self.open_ask = Some(optimal_ask.max(min_profit_price));
        } else {
            // FLAT — reset stale inventory counter, enter acquisition mode.
            self.ticks_held = 0;
            // STRICT ACQUISITION MODE: we are flat — only buy, never sell.
            let aggressive_spread = spread_delta * 0.8; // 20% tighter to ensure fill
            let bid_price = reservation_price - aggressive_spread;
            self.open_bid = Some((bid_price / self.tick_size).round() * self.tick_size);
            self.open_ask = None;
        }

        // ── Panic Stop-Loss ───────────────────────────────────────────────────
        // Triggers on: (a) price drawdown below last fill, or
        //              (b) ToxicLiquidationCascade regime with long inventory.
        let is_panic = self.inventory_coin > 0.0 && (
            (self.last_fill_price > 0.0 &&
             micro_price < self.last_fill_price * PANIC_DRAWDOWN) ||
            self.current_regime == MarketRegime::ToxicLiquidationCascade
        );

        TickOutput {
            micro_price,
            obi,
            tfi,
            variance:          self.variance,
            reservation_price,
            optimal_bid,
            optimal_ask,
            open_bid:          self.open_bid,
            open_ask:          self.open_ask,
            inventory_coin:    self.inventory_coin,
            pnl_usd:           self.pnl_usd,
            total_trades:      self.total_trades,
            fill_this_tick:    None,
            warm_ticks:        self.warm_ticks,
            is_panic,
        }
    }
}
