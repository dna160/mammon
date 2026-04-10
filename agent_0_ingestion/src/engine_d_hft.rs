//! Engine D — Avellaneda-Stoikov HFT Market Maker (BTC/IDR)
//!
//! Pure synchronous, zero-allocation tick processor.  Every formula runs in
//! the hot path without touching the heap.  No async, no tokio::spawn, no
//! String, Vec::push, or Box anywhere inside tick().
//!
//! Mathematical framework (PRD §3):
//!   Formula 1 — Micro-Price        P_micro = (V_ask·P_bid + V_bid·P_ask) / (V_bid + V_ask)
//!   Formula 2 — Order-Book Imbal.  OBI     = (V_bid - V_ask) / (V_bid + V_ask)
//!   Formula 3 — Trade-Flow Imbal.  TFI     = Σ V_buy_market − Σ V_sell_market  (rolling 1 s)
//!   Formula 4 — HF Variance        σ²_t    = (1−α)·σ²_{t−1} + α·(ΔP)²          α = 0.01
//!   Formula 5 — AS Reservation     r       = P_micro − (q · γ · σ²)
//!   Formula 6 — Optimal Spread     δ       = Tick_Size_IDR + (γ · σ²)
//!                                  P*_bid  = r − δ
//!                                  P*_ask  = r + δ
//!
//! All prices and inventory are denominated in IDR (Indonesian Rupiah).

// ── Parameters ────────────────────────────────────────────────────────────────

/// EWMA decay α for HF variance (≈ 100-tick half-life).
const ALPHA: f64 = 0.01;

/// Risk-aversion γ.  Higher → quotes skew faster away from inventory.
const GAMMA: f64 = 0.5;

/// Minimum tick size in IDR for BTC/IDR on Tokocrypto.
/// Used as the floor term in δ (prevents quotes collapsing to zero spread).
const TICK_SIZE_IDR: f64 = 1_000.0;

/// OBI magnitude threshold for execution-skew override.
const OBI_SKEW_THRESHOLD: f64 = 0.8;

/// Fixed-size ring buffer for TFI rolling window.
/// At ~10 aggTrade msgs/sec this covers ≥ 50 seconds — plenty for a 1 s window.
const TFI_CAP: usize = 512;

/// Rolling TFI window length in milliseconds.
const TFI_WINDOW_MS: u64 = 1_000;

// ── Fill side ─────────────────────────────────────────────────────────────────

/// Which side of the book was filled this tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillSide {
    /// Our passive bid was hit (we bought BTC, spent IDR).
    Buy,
    /// Our passive ask was hit (we sold BTC, received IDR).
    Sell,
}

// ── Tick output ───────────────────────────────────────────────────────────────

/// Plain-data result of one tick.  No heap allocation.
/// Caller may clone this for async telemetry offload.
#[derive(Debug, Clone, Copy)]
pub struct TickOutput {
    /// Liquidity-weighted micro-price in IDR.
    pub micro_price: f64,
    /// Order-book imbalance ∈ [−1, +1].
    pub obi: f64,
    /// Trade-flow imbalance over the last 1 s (IDR-volume signed).
    pub tfi: f64,
    /// Avellaneda-Stoikov reservation price in IDR.
    pub reservation_price: f64,
    /// Optimal passive bid placement in IDR.
    pub optimal_bid: f64,
    /// Optimal passive ask placement in IDR.
    pub optimal_ask: f64,
    /// Active open bid (may differ from optimal_bid under skew).
    pub open_bid: Option<f64>,
    /// Active open ask (may differ from optimal_ask under skew).
    pub open_ask: Option<f64>,
    /// Net BTC inventory (positive = long, negative = short).
    pub inventory_btc: f64,
    /// Accumulated simulated PnL in IDR.
    pub pnl_idr: f64,
    /// Total simulated fill count.
    pub total_trades: u64,
    /// Fill recorded on this specific tick, if any.
    pub fill_this_tick: Option<FillSide>,
    /// Current EWMA σ² (IDR²) — exposed for logging/telemetry.
    pub variance: f64,
    /// Number of IDR price ticks seen so far (for warm-up tracking).
    pub warm_ticks: u64,
}

// ── Engine ────────────────────────────────────────────────────────────────────

/// Avellaneda-Stoikov HFT Engine — stateful, synchronous, zero-allocation.
pub struct HFTEngine {
    // ── Quote state ──────────────────────────────────────────────────────────
    /// Net BTC inventory.  Positive = long (holding BTC), negative = short.
    pub inventory_btc: f64,

    /// EWMA high-frequency variance σ² of micro-price returns (IDR²).
    pub variance: f64,

    /// Micro-price from the previous tick (for ΔP in variance update).
    pub last_micro_price: f64,

    /// Current simulated open bid order price in IDR (None = no active bid).
    pub open_bid: Option<f64>,

    /// Current simulated open ask order price in IDR (None = no active ask).
    pub open_ask: Option<f64>,

    // ── P&L ──────────────────────────────────────────────────────────────────
    /// Accumulated simulated PnL in IDR (positive = profit).
    pub pnl_idr: f64,

    /// Total simulated fills (both sides combined).
    pub total_trades: u64,

    /// Number of IDR bookTicker ticks processed (used for EWMA warm-up guard).
    /// Skew logic and fill simulation are suppressed until warm_ticks ≥ 20.
    pub warm_ticks: u64,

    // ── TFI ring buffer (zero-allocation) ────────────────────────────────────
    /// Trade volumes stored in insertion order (ring buffer).
    tfi_vols: [f64; TFI_CAP],

    /// Corresponding wall-clock timestamps in milliseconds.
    tfi_ts: [u64; TFI_CAP],

    /// Sign of each trade: +1 = aggressive buy, −1 = aggressive sell.
    tfi_sign: [i8; TFI_CAP],

    /// Next write position (wraps mod TFI_CAP).
    tfi_head: usize,

    /// Number of valid entries currently in the buffer (≤ TFI_CAP).
    tfi_len: usize,
}

impl HFTEngine {
    /// Apply a confirmed exchange fill to inventory and PnL.
    /// Called by the order manager when Tokocrypto's trade history returns a
    /// new fill for one of our live orders.  Never called from tick().
    ///
    /// `is_buy` — true = we bought BTC (our bid was hit), false = we sold BTC.
    /// `price`  — actual execution price in IDR.
    /// `qty`    — executed quantity in BTC.
    pub fn record_real_fill(&mut self, is_buy: bool, price: f64, qty: f64) {
        let notional = price * qty;
        if is_buy {
            self.inventory_btc += qty;
            self.pnl_idr       -= notional; // IDR spent buying BTC
        } else {
            self.inventory_btc -= qty;
            self.pnl_idr       += notional; // IDR received selling BTC
        }
        self.total_trades += 1;
    }

    /// Return a fully zeroed instance ready for the first tick.
    pub fn init() -> Self {
        Self {
            inventory_btc: 0.0,
            variance: 0.0,
            last_micro_price: 0.0,
            open_bid: None,
            open_ask: None,
            pnl_idr: 0.0,
            total_trades: 0,
            warm_ticks: 0,
            tfi_vols: [0.0; TFI_CAP],
            tfi_ts:   [0;   TFI_CAP],
            tfi_sign: [0;   TFI_CAP],
            tfi_head: 0,
            tfi_len:  0,
        }
    }

    /// Process one market tick.  Pure synchronous f64 math — zero heap
    /// allocation.  No tokio::spawn, no async, no String, Vec, or Box.
    ///
    /// # Arguments
    /// | param                | description                                          |
    /// |----------------------|------------------------------------------------------|
    /// | `best_bid`           | Top-of-book bid price in IDR                         |
    /// | `bid_vol`            | Volume at best bid (BTC)                             |
    /// | `best_ask`           | Top-of-book ask price in IDR                         |
    /// | `ask_vol`            | Volume at best ask (BTC)                             |
    /// | `latest_trade_price` | aggTrade price in IDR (0.0 if depth-only tick)       |
    /// | `latest_trade_vol`   | aggTrade quantity in BTC (0.0 if depth-only tick)    |
    /// | `is_buyer_maker`     | true → seller was aggressor; false → buyer aggressor |
    /// | `now_ms`             | Current wall-clock time in milliseconds              |
    pub fn tick(
        &mut self,
        best_bid: f64,
        bid_vol: f64,
        best_ask: f64,
        ask_vol: f64,
        latest_trade_price: f64,
        latest_trade_vol: f64,
        is_buyer_maker: bool,
        now_ms: u64,
    ) -> TickOutput {

        // ── Formula 1: Micro-Price ────────────────────────────────────────────
        // P_micro = (V_ask·P_bid + V_bid·P_ask) / (V_bid + V_ask)
        // Falls back to plain mid-price when book is empty.
        let total_vol = bid_vol + ask_vol;
        let micro_price = if total_vol > 0.0 {
            ((ask_vol * best_bid) + (bid_vol * best_ask)) / total_vol
        } else {
            (best_bid + best_ask) * 0.5
        };

        // ── Formula 2: Order-Book Imbalance ──────────────────────────────────
        // OBI = (V_bid − V_ask) / (V_bid + V_ask)  ∈ [−1, +1]
        let obi = if total_vol > 0.0 {
            (bid_vol - ask_vol) / total_vol
        } else {
            0.0
        };

        // ── Formula 4: EWMA High-Frequency Variance ───────────────────────────
        // ΔP = P_micro_t − P_micro_{t−1}
        // σ²_t = (1−α)·σ²_{t−1} + α·(ΔP)²     α = 0.01
        //
        // Warm-up note: btcidr@bookTicker fires ~1 Hz.  At that rate ΔP between
        // consecutive ticks can be ~100–200 K IDR (intraday drift), driving σ²
        // toward (ΔP)² ≈ 36 B IDR² at steady state.  We use two guards:
        //   1. warm_ticks counter — gates skew/fill logic until EWMA has stabilised.
        //   2. Variance cap at (0.05% of micro_price)² — hard upper bound so
        //      spread_delta never leaves tradeable territory even before warm-up.
        if self.last_micro_price > 0.0 {
            let delta_p = micro_price - self.last_micro_price;
            self.variance = (1.0 - ALPHA) * self.variance + ALPHA * delta_p * delta_p;
            // Cap σ² so that γ·σ² ≤ 0.05 % of micro_price.
            // This bounds BOTH the spread half-width AND the inventory skew term
            // (q · γ · σ²) so that reservation_price and optimal_bid/ask remain
            // positive regardless of how large ΔP ticks become (e.g. from the
            // ~1 Hz btcidr@bookTicker stream where ΔP can be ~200 K IDR).
            //
            // max_spread_half = 0.05 % of price
            // max σ² = max_spread_half / GAMMA   (so γ · σ² = max_spread_half)
            //
            // At 1.228 B IDR: max σ² ≈ 1.228 M IDR²  →  δ_max ≈ 614 K IDR (0.05 %)
            // For q = 1 BTC:  q · γ · σ² ≤ 614 K IDR  →  |r − P_micro| ≤ 0.05 %
            let max_variance = (micro_price * 5e-4) / GAMMA;
            if self.variance > max_variance {
                self.variance = max_variance;
            }
            self.warm_ticks += 1;
        }
        self.last_micro_price = micro_price;

        // ── Formula 3: Trade-Flow Imbalance (ring-buffer update) ─────────────
        // Insert this aggTrade into the fixed ring buffer when we have trade data.
        if latest_trade_vol > 0.0 {
            let sign: i8 = if !is_buyer_maker { 1 } else { -1 };
            let idx = self.tfi_head % TFI_CAP;
            self.tfi_vols[idx] = latest_trade_vol;
            self.tfi_ts[idx]   = now_ms;
            self.tfi_sign[idx] = sign;
            self.tfi_head = (self.tfi_head + 1) % TFI_CAP;
            if self.tfi_len < TFI_CAP { self.tfi_len += 1; }
        }

        // Sum all ring-buffer entries whose timestamp falls within the last 1 s.
        // Pure array iteration — no allocation.
        let tfi: f64 = {
            let mut sum = 0.0_f64;
            let mut i = 0_usize;
            while i < self.tfi_len {
                // Walk the ring in insertion order (oldest → newest).
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

        // ── Formula 5: Avellaneda-Stoikov Reservation Price ──────────────────
        // r = P_micro − (q · γ · σ²)
        // q is net BTC inventory; positive inventory skews r downward (engine
        // wants to sell to reduce risk), negative inventory skews r upward.
        let reservation_price = micro_price - (self.inventory_btc * GAMMA * self.variance);

        // ── Formula 6: Optimal Spread & Quote Placement ───────────────────────
        // δ = Tick_Size_IDR + (γ · σ²)
        // P*_bid = r − δ      P*_ask = r + δ
        //
        // Clamp δ to at most 0.05 % of micro_price so that even a cold EWMA
        // (or one large-ΔP tick at start-up) cannot push optimal_bid negative.
        let raw_delta   = TICK_SIZE_IDR + (GAMMA * self.variance);
        let max_delta   = micro_price * 5e-4; // 0.05 % of current price
        let spread_delta = raw_delta.min(max_delta).max(TICK_SIZE_IDR);
        let optimal_bid  = reservation_price - spread_delta;
        let optimal_ask  = reservation_price + spread_delta;

        // ── State 2: Execution Skew — Alpha Overlay ───────────────────────────
        // Suppressed during EWMA warm-up (first 20 IDR ticks) so that a cold
        // variance estimate cannot produce nonsensical skew decisions.
        //
        // OBI > +0.8 AND TFI > 0  → massive buy pressure:
        //   cancel ask (don't get run over), ride bid at best bid.
        // OBI < −0.8 AND TFI < 0  → massive sell pressure:
        //   cancel bid, place ask at best ask.
        // Otherwise: symmetric AS quotes.
        let warmed_up = self.warm_ticks >= 20;
        if warmed_up && obi > OBI_SKEW_THRESHOLD && tfi > 0.0 {
            self.open_ask = None;
            self.open_bid = Some(best_bid);
        } else if warmed_up && obi < -OBI_SKEW_THRESHOLD && tfi < 0.0 {
            self.open_bid = None;
            self.open_ask = Some(best_ask);
        } else {
            self.open_bid = Some(optimal_bid);
            self.open_ask = Some(optimal_ask);
        }

        // ── State 3: Real Fill Tracking ───────────────────────────────────────
        // Simulated fill engine removed.  inventory_btc, pnl_idr, and
        // total_trades are now driven exclusively by confirmed Tokocrypto fills
        // polled by the order manager task via record_real_fill() below.
        // The tick() path no longer mutates those fields, so there are no
        // phantom "fills" from market aggTrades that were never real orders.
        let fill_this_tick: Option<FillSide> = None;

        TickOutput {
            micro_price,
            obi,
            tfi,
            reservation_price,
            optimal_bid,
            optimal_ask,
            open_bid: self.open_bid,
            open_ask: self.open_ask,
            inventory_btc: self.inventory_btc,
            pnl_idr: self.pnl_idr,
            total_trades: self.total_trades,
            fill_this_tick,
            variance: self.variance,
            warm_ticks: self.warm_ticks,
        }
    }
}
