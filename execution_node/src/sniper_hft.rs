// ============================================================
// sniper_hft.rs — Ironclad State Machine (The Sniper)
//
// ZERO continuous math. Pure deterministic tick logic.
// All parameters are injected externally from Redis via Agent Q.
// Rules enforced here are MATHEMATICALLY ABSOLUTE — no exceptions.
// ============================================================

use serde::{Deserialize, Serialize};
use tracing::warn;

/// Live parameters pushed by the Python Cognitive Node every 3 minutes.
/// Redis key: hft:live_params:{symbol}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveParams {
    pub momentum_trigger_obi:  f64,  // OBI threshold to trigger BUY  [0.30, 0.80]
    pub take_profit_ticks:     u32,  // Ticks above AEP for ask       [1, 10]
    pub stop_loss_ticks:       u32,  // Ticks below AEP for dump      [5, 30]
    pub max_active_tranches:   u32,  // Max concurrent $6 positions   [1, 5]
}

impl Default for LiveParams {
    fn default() -> Self {
        Self {
            momentum_trigger_obi: 0.55,
            take_profit_ticks:    3,
            stop_loss_ticks:      10,
            max_active_tranches:  3,
        }
    }
}

/// The core deterministic state machine.
/// Receives LOB ticks → outputs action directives (open_bid, open_ask, emergency_dump).
pub struct SniperEngine {
    pub symbol:          String,
    pub tick_size:       f64,
    pub lot_step:        f64,

    // ── Core Execution State ───────────────────────────────────────────────
    pub aep:             f64,       // Average Entry Price (volume-weighted)
    pub inventory_coin:  f64,       // Physical coin balance tracked from UDS fills

    // ── Trade Flow Imbalance Ring Buffer (notional USD) ────────────────────
    pub tfi_window:      [f64; 100],
    pub tfi_idx:         usize,

    // ── AI Sniper Levers (Injected via Redis) ─────────────────────────────
    pub live_momentum_trigger_obi: f64,
    pub live_take_profit_ticks:    u32,
    pub live_stop_loss_ticks:      u32,
    pub live_max_tranches:         u32,

    // ── Action Directives (OUTPUT of tick()) ──────────────────────────────
    // Set by tick(), consumed by main.rs order manager.
    pub open_bid:               Option<f64>,  // Price to place LIMIT_MAKER BUY
    pub open_ask:               Option<f64>,  // Price to place LIMIT_MAKER SELL
    pub emergency_dump_triggered: bool,       // Market sell all inventory NOW
}

impl SniperEngine {
    pub fn new(symbol: &str, tick_size: f64, lot_step: f64) -> Self {
        let params = LiveParams::default();
        Self {
            symbol:          symbol.to_string(),
            tick_size,
            lot_step,
            aep:             0.0,
            inventory_coin:  0.0,
            tfi_window:      [0.0; 100],
            tfi_idx:         0,
            live_momentum_trigger_obi: params.momentum_trigger_obi,
            live_take_profit_ticks:    params.take_profit_ticks,
            live_stop_loss_ticks:      params.stop_loss_ticks,
            live_max_tranches:         params.max_active_tranches,
            open_bid:               None,
            open_ask:               None,
            emergency_dump_triggered: false,
        }
    }

    /// Atomically update all sniper levers from Redis params.
    pub fn update_params(&mut self, params: &LiveParams) {
        self.live_momentum_trigger_obi = params.momentum_trigger_obi;
        self.live_take_profit_ticks    = params.take_profit_ticks;
        self.live_stop_loss_ticks      = params.stop_loss_ticks;
        self.live_max_tranches         = params.max_active_tranches;
    }

    /// Push a notional trade flow observation into the ring buffer.
    pub fn push_tfi(&mut self, notional_usd: f64) {
        self.tfi_window[self.tfi_idx % 100] = notional_usd;
        self.tfi_idx = self.tfi_idx.wrapping_add(1);
    }

    /// Called by UDS fill handler.
    /// Updates AEP and inventory_coin on every FILLED executionReport.
    pub fn on_fill(&mut self, filled_qty: f64, filled_price: f64, side: &str) {
        if filled_qty <= 0.0 || filled_price <= 0.0 {
            return;
        }
        match side {
            "BUY" => {
                let old_notional = self.inventory_coin * self.aep;
                let new_notional = filled_qty * filled_price;
                self.inventory_coin += filled_qty;
                if self.inventory_coin > 0.0 {
                    self.aep = (old_notional + new_notional) / self.inventory_coin;
                }
            }
            "SELL" => {
                self.inventory_coin -= filled_qty;
                if self.inventory_coin <= 0.0 {
                    // Fully flat — reset position state
                    self.inventory_coin = 0.0;
                    self.aep = 0.0;
                }
            }
            _ => warn!("Unknown side: {}", side),
        }
    }

    /// Core execution tick — called on every LOB (book ticker) update.
    ///
    /// Strict execution sequence (PRD spec):
    ///   1. DUST RECOVERY — trapped below $5.10 notional
    ///   2. ENTRY         — momentum penny-jump + AEP defense
    ///   3. EXIT          — profit strike ask
    ///   4. STOP-LOSS     — hard amputation
    pub fn tick(
        &mut self,
        best_bid: f64,
        bid_vol:  f64,
        best_ask: f64,
        ask_vol:  f64,
    ) {
        // Derived state
        let current_notional = self.inventory_coin * best_bid;

        // CRITICAL FIX: floor($5.99/$6.00) = 0, so sub-$6 inventory was invisible.
        // Any inventory >= lot_step counts as at least 1 active tranche.
        // This ensures the exit and stop-loss fire even on the first partial fill.
        let has_inventory = self.inventory_coin >= self.lot_step;
        let active_tranches: u32 = if has_inventory {
            1_u32.max((current_notional / 6.00).floor() as u32)
        } else {
            0
        };

        let obi = (bid_vol - ask_vol) / (bid_vol + ask_vol + f64::EPSILON);

        // Reset action directives on every tick
        self.emergency_dump_triggered = false;
        self.open_bid = None;
        self.open_ask = None;

        // ──────────────────────────────────────────────────────────────────
        // 1. DUST RECOVERY
        // If inventory is trapped below Binance's $5.00 notional floor,
        // aggressively bid at best_bid to accumulate enough to sell.
        // ──────────────────────────────────────────────────────────────────
        if self.inventory_coin > 0.0 && current_notional < 5.10 {
            // Clamp below ask — dust recovery must still be post-only
            let safe_bid = (best_ask - self.tick_size).max(best_bid);
            self.open_bid = Some(safe_bid);
            return; // Early return — nothing else should fire
        }

        // ──────────────────────────────────────────────────────────────────
        // 2. ENTRY — The Momentum Penny-Jump
        // Only enter if we have room for another tranche AND OBI exceeds
        // the live trigger threshold.
        // ──────────────────────────────────────────────────────────────────
        if active_tranches < self.live_max_tranches {
            if obi > self.live_momentum_trigger_obi {
                // Penny-jump: bid one tick above best_bid
                let jump_bid = best_bid + self.tick_size;

                // SPREAD GUARD — hard ceiling: bid must stay strictly below ask.
                // On thin SOL spreads this prevents -2010 "would immediately match".
                if jump_bid >= best_ask {
                    // Spread too tight to penny-jump — place at best_bid instead
                    self.open_bid = Some(best_bid);
                    return;
                }

                // AEP DEFENSE — forbid averaging UP into a pump.
                let averaging_up = active_tranches > 0
                    && self.aep > 0.0
                    && jump_bid >= self.aep;

                if !averaging_up {
                    self.open_bid = Some(jump_bid);
                }
            }
        }

        // ──────────────────────────────────────────────────────────────────
        // 3. EXIT — The Profit Strike
        // Place a resting ask at AEP + (tick_size × take_profit_ticks).
        // Only active when we hold inventory.
        // ──────────────────────────────────────────────────────────────────
        if active_tranches > 0 && self.aep > 0.0 {
            let target_price =
                self.aep + (self.tick_size * self.live_take_profit_ticks as f64);
            self.open_ask = Some(target_price);
        }

        // ──────────────────────────────────────────────────────────────────
        // 4. FALLBACK — The Hard Stop-Loss (Amputation)
        // If best_bid has collapsed below AEP - stop_loss_ticks,
        // trigger emergency dump. Overrides the ask.
        // ──────────────────────────────────────────────────────────────────
        if active_tranches > 0 && self.aep > 0.0 {
            let stop_price =
                self.aep - (self.tick_size * self.live_stop_loss_ticks as f64);
            if best_bid <= stop_price {
                self.emergency_dump_triggered = true;
                self.open_ask = None;
                self.open_bid = None;
            }
        }
    }

    /// Quantize a price to the exchange tick_size grid.
    pub fn round_to_tick(price: f64, tick_size: f64) -> f64 {
        (price / tick_size).round() * tick_size
    }

    /// Quantize a quantity to the lot_step grid (always floor, never exceed balance).
    pub fn floor_to_lot(qty: f64, lot_step: f64) -> f64 {
        (qty / lot_step).floor() * lot_step
    }

    /// Current position notional value in USD.
    pub fn notional_usd(&self, price: f64) -> f64 {
        self.inventory_coin * price
    }
}
