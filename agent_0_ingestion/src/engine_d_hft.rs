//! Engine D — Avellaneda-Stoikov HFT Market Maker (multi-coin, Binance).
//!
//! Mathematical framework (V2.2 — Institutional Math Hardened):
//!   F1  Micro-Price:    P_micro = (V_ask·P_bid + V_bid·P_ask) / (V_bid + V_ask)
//!   F2  OBI:            (V_bid − V_ask) / (V_bid + V_ask) ∈ [−1, +1]
//!   F3  TFI (exp-decay): 5s half-life EWMA — eliminates infinite accumulation bug
//!   F4  EWMA σ²(t):     α = 1 − exp(−dt/60)  (continuous-time, uncoupled from tick rate)
//!   F5  Reservation:    r = P_micro − (q_skew · γ · σ²)  [spot-symmetry corrected]
//!   F6  Spread:         δ = max(tick_size + γ·σ², tick_size · MIN_SPREAD_TICKS)
//!   F7  Grid:           bid = snap(optimal_bid − active_tranches · tick · grid_offset_ticks)
//!
//! State machine:
//!   !warmed              → no quotes (variance not trusted yet)
//!   |TFI| > threshold    → shadow mode (toxic flow shield)
//!   else multi-tranche capacity math:
//!     active_tranches = floor(inventory_notional / $6)
//!     bid: active_tranches < max_tranches AND safe_to_buy
//!          → grid-stepped bid (optimal_bid − active_tranches × tick × grid_offset_ticks)
//!     ask: active_tranches > 0 AND safe_to_sell
//!          → max(optimal_ask, last_fill + 1 tick)
//!          → stale >600 ticks: accept break-even (last_fill_price only)

// ── Module-level constants ────────────────────────────────────────────────────

const DEFAULT_LIVE_GAMMA:          f64 = 0.8;
const DEFAULT_LIVE_MIN_SPREAD:     f64 = 5.0;
const DEFAULT_LIVE_TFI_THRESHOLD:  f64 = 65_000.0;
const DEFAULT_LIVE_OBI_THRESHOLD:  f64 = 1.0;
const DEFAULT_LIVE_MAX_TRANCHES:   u32 = 1;
/// AI-controlled grid spacing. Default 2.0 ticks between each tranche.
/// Agent Q scales this up during high-volatility to prevent allocation collapse.
const DEFAULT_LIVE_GRID_OFFSET:    f64 = 2.0;

/// TFI exponential decay half-life (seconds). Flow pressure decays 50% every 5s.
const TFI_HALF_LIFE_S: f64 = 5.0;

/// Continuous-time variance lookback (seconds). 60s = slow EWMA, stable estimate.
const VAR_LOOKBACK_S: f64 = 60.0;

/// Minimum FDUSD notional — engine uses to detect dust traps.
const MIN_NOTIONAL_USD: f64 = 5.0;

// ── Market Regime ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketRegime {
    MeanReverting,
    RetailFrenzyUp,
    InstitutionalAbsorptionDown,
    DeadZone,
    ToxicLiquidationCascade,
}

// ── Fill side ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillSide { Buy, Sell }

// ── Tick output ───────────────────────────────────────────────────────────────

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
    pub is_panic:          bool,
}

// ── Engine ────────────────────────────────────────────────────────────────────

pub struct HFTEngine {
    // ── Exchange physics ───────────────────────────────────────────────────────
    pub symbol:             String,
    pub tick_size:          f64,
    pub lot_step:           f64,
    pub max_inventory_coin: f64,

    // ── Agent Q Oracle (5m) ────────────────────────────────────────────────────
    pub current_regime: MarketRegime,

    // ── Agent Q Tactical (3m) — live parameters ───────────────────────────────
    pub live_gamma:             f64,
    pub live_min_spread:        f64,
    pub live_tfi_threshold:     f64,
    /// OBI momentum shield [0.0–1.0]: 1.0=permissive, 0.1=defensive.
    pub live_obi_threshold:     f64,
    /// Max concurrent $6 tranches [1–10]. Scales grid depth.
    pub live_max_tranches:      u32,
    /// AI-controlled grid spacing in ticks [1.0–50.0].
    /// Each tranche steps the bid lower by this many ticks, preventing price collapse.
    pub live_grid_offset_ticks: f64,

    // ── Engine state ───────────────────────────────────────────────────────────
    pub inventory_coin:   f64,
    pub variance:         f64,
    pub last_micro_price: f64,
    pub last_fill_price:  f64,
    pub open_bid:         Option<f64>,
    pub open_ask:         Option<f64>,
    pub pnl_usd:          f64,
    pub total_trades:     u64,
    pub warm_ticks:       u64,
    /// Ticks held long — stale inventory dump gate (>600 → surrender profit floor).
    pub ticks_held:       u64,

    // ── Fix 1: TFI Exponential Decay (replaces ring buffer) ───────────────────
    /// Running signed USD-notional flow with 5s half-life.
    pub tfi_rolling_sum:    f64,
    /// Timestamp (ms) of last aggTrade — used to compute decay interval.
    pub last_tfi_update_ms: u64,

    // ── Fix 2: Continuous-Time Variance ──────────────────────────────────────
    /// Timestamp (ms) of last bookTicker — used to compute time-accurate alpha.
    pub last_var_update_ms: u64,
}

impl HFTEngine {
    pub fn new(symbol: String, tick_size: f64, lot_step: f64, max_inventory_coin: f64) -> Self {
        Self {
            symbol,
            tick_size,
            lot_step,
            max_inventory_coin,
            current_regime:        MarketRegime::MeanReverting,
            live_gamma:            DEFAULT_LIVE_GAMMA,
            live_min_spread:       DEFAULT_LIVE_MIN_SPREAD,
            live_tfi_threshold:    DEFAULT_LIVE_TFI_THRESHOLD,
            live_obi_threshold:    DEFAULT_LIVE_OBI_THRESHOLD,
            live_max_tranches:     DEFAULT_LIVE_MAX_TRANCHES,
            live_grid_offset_ticks: DEFAULT_LIVE_GRID_OFFSET,
            inventory_coin:        0.0,
            variance:              0.0,
            last_micro_price:      0.0,
            last_fill_price:       0.0,
            open_bid:              None,
            open_ask:              None,
            pnl_usd:               0.0,
            total_trades:          0,
            warm_ticks:            0,
            ticks_held:            0,
            tfi_rolling_sum:       0.0,
            last_tfi_update_ms:    0,
            last_var_update_ms:    0,
        }
    }

    /// Apply Agent Q Tactical parameters (every 3m) injected via Redis.
    pub fn update_params(
        &mut self,
        gamma: f64,
        min_spread: f64,
        tfi_threshold: f64,
        obi_threshold: f64,
        max_tranches: u32,
        grid_offset_ticks: f64,
    ) {
        self.live_gamma             = gamma.clamp(0.1, 1.0);
        self.live_min_spread        = min_spread.clamp(1.0, 50.0);
        self.live_tfi_threshold     = tfi_threshold.clamp(200.0, 200_000.0);
        self.live_obi_threshold     = obi_threshold.clamp(0.0, 1.0);
        self.live_max_tranches      = max_tranches.clamp(1, 10);
        self.live_grid_offset_ticks = grid_offset_ticks.clamp(1.0, 50.0);
    }

    /// Apply Agent Q Oracle regime (every 5m) injected via Redis.
    pub fn update_regime(&mut self, regime: MarketRegime) {
        self.current_regime = regime;
    }

    /// Insert an aggTrade into the TFI exponential rolling sum (O(1), zero-allocation).
    ///
    /// Fix 1: replaces the fixed-size ring buffer with a 5s half-life EWMA.
    /// USD notional (qty × price) normalises flow signal across all price scales.
    pub fn record_agg_trade(&mut self, qty: f64, price: f64, is_buyer_maker: bool, now_ms: u64) {
        if qty <= 0.0 || price <= 0.0 { return; }

        // Decay existing sum for elapsed time since last trade.
        let dt           = (now_ms.saturating_sub(self.last_tfi_update_ms)) as f64 / 1_000.0;
        let decay_factor = (-dt * std::f64::consts::LN_2 / TFI_HALF_LIFE_S).exp();
        self.tfi_rolling_sum   *= decay_factor;
        self.last_tfi_update_ms = now_ms;

        // Add signed USD notional of this trade.
        let notional    = qty * price;
        let signed_flow = if !is_buyer_maker { notional } else { -notional };
        self.tfi_rolling_sum += signed_flow;
    }

    /// Apply a confirmed fill to inventory and PnL.
    pub fn record_real_fill(&mut self, is_buy: bool, price: f64, qty: f64, fee_usd: f64) {
        let notional = price * qty;
        if is_buy {
            self.inventory_coin += qty;
            self.pnl_usd        -= notional + fee_usd;
            self.last_fill_price = price;
            self.ticks_held      = 0;
        } else {
            self.inventory_coin -= qty;
            self.pnl_usd        += notional - fee_usd;
            if self.inventory_coin <= self.lot_step {
                self.ticks_held = 0;
            }
        }
        self.total_trades += 1;
    }

    /// Process one market-data tick (driven by bookTicker).
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

        // ── F4: Continuous-Time EWMA Variance (Fix 2) ─────────────────────────
        // α = 1 − exp(−dt/τ), τ = 60s → weight per tick scales with real time elapsed,
        // not tick arrival rate. Eliminates variance distortion from irregular ticks.
        if self.last_micro_price > 0.0 {
            let dt_sec     = (now_ms.saturating_sub(self.last_var_update_ms)) as f64 / 1_000.0;
            let dt_clamped = dt_sec.min(VAR_LOOKBACK_S); // cap at 60s to prevent blow-up on reconnect
            let alpha      = 1.0 - (-dt_clamped / VAR_LOOKBACK_S).exp();
            let delta_p    = micro_price - self.last_micro_price;
            self.variance  = (1.0 - alpha) * self.variance + alpha * delta_p * delta_p;
            self.warm_ticks += 1;
        }
        self.last_micro_price  = micro_price;
        self.last_var_update_ms = now_ms;

        // ── F3: TFI (read decayed rolling sum to now) ────────────────────────
        let dt_tfi    = (now_ms.saturating_sub(self.last_tfi_update_ms)) as f64 / 1_000.0;
        let tfi_decay = (-dt_tfi * std::f64::consts::LN_2 / TFI_HALF_LIFE_S).exp();
        let tfi       = self.tfi_rolling_sum * tfi_decay;

        // ── F5: Reservation Price — Spot-Symmetry Corrected (Fix 3) ──────────
        // Neutral inventory = MAX/2 (halfway through grid capacity).
        // inventory_risk_skew > 0 → we are over-long → reservation skews lower.
        // inventory_risk_skew < 0 → we are under-long → reservation skews higher.
        let neutral_inventory   = self.live_max_tranches as f64
            * (6.00 / micro_price.max(1e-9))
            / 2.0;
        let inventory_risk_skew = self.inventory_coin - neutral_inventory;
        let reservation_price   =
            micro_price - (inventory_risk_skew * self.live_gamma * self.variance);

        // ── F6: Spread ────────────────────────────────────────────────────────
        let ts           = self.tick_size;
        let raw_delta    = ts + (self.live_gamma * self.variance);
        let spread_delta = raw_delta.max(ts * self.live_min_spread);

        let snap        = |p: f64| -> f64 { (p / ts).round() * ts };
        let optimal_bid = snap(reservation_price - spread_delta);
        let optimal_ask = snap(reservation_price + spread_delta);

        // ── Execution: Multi-Tranche Grid (PRD §5) ───────────────────────────
        let warmed = self.warm_ticks >= 20;

        let safe_to_buy  = obi > -self.live_obi_threshold.abs();
        let safe_to_sell = obi <  self.live_obi_threshold.abs();

        let current_notional = self.inventory_coin * micro_price;

        if !warmed {
            self.open_bid = None;
            self.open_ask = None;
        } else if tfi.abs() > self.live_tfi_threshold {
            // Toxic Flow Shield — outermost guard.
            self.open_bid = None;
            self.open_ask = None;
        } else {
            // ── Multi-Tranche Capacity Math ───────────────────────────────────
            let active_tranches = (current_notional / 6.00).floor() as u32;

            if active_tranches > 0 {
                self.ticks_held += 1;
            } else {
                self.ticks_held = 0;
            }

            // ── F7: Dynamic Grid Spacing (AI-Controlled) ──────────────────────
            // Each tranche held steps the next bid lower by live_grid_offset_ticks.
            // Replaces hardcoded 2.0 — Agent Q adjusts spacing to market volatility.
            // High volatility → wider spacing to catch dip bottoms without collapse.
            // Chop/range → tight spacing (1.0–3.0) to densely farm fees.
            let grid_offset = (active_tranches as f64) * (ts * self.live_grid_offset_ticks);

            // ── BID SIDE ──────────────────────────────────────────────────────
            if active_tranches < self.live_max_tranches && safe_to_buy {
                let stepped_bid_price = optimal_bid - grid_offset;
                self.open_bid = Some((stepped_bid_price / ts).round() * ts);
            } else {
                self.open_bid = None;
            }

            // ── ASK SIDE — Exit Plan & Stale Dump ────────────────────────────
            if active_tranches > 0 && safe_to_sell {
                // PRD §5: Stale Inventory Dump
                //   Normal (≤600 ticks):  demand 1-tick profit floor
                //   Stale  (>600 ticks):  accept break-even to regain velocity
                let min_profit_price = if self.ticks_held > 600 {
                    self.last_fill_price             // break-even: free frozen capital
                } else {
                    self.last_fill_price + ts        // iron profit floor: 1 tick
                };
                self.open_ask = Some(optimal_ask.max(min_profit_price));
            } else {
                self.open_ask = None;
            }
        }

        // ── Panic Stop-Loss (ToxicLiquidationCascade only) ───────────────────
        let is_panic = self.inventory_coin > 0.0 &&
            self.current_regime == MarketRegime::ToxicLiquidationCascade;

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
