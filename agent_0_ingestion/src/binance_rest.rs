//! Binance Spot V3 REST client — multi-coin order execution for Engine D.
//!
//! All order functions accept `symbol: &str` so the same client serves
//! BTCFDUSD, ADAFDUSD, DOTFDUSD, DOGEFDUSD, and XRPFDUSD simultaneously.
//! Price/qty formatting is driven by the caller-supplied tick_size / lot_step.
//!
//! Zero-fee guarantee: ALL limit orders use type=LIMIT_MAKER (post-only).
//! Panic stop-loss uses type=MARKET (taker) — only fires on 0.15% drawdown.

use anyhow::{anyhow, Context, Result};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{error, info, warn};

// ── Shared constants ──────────────────────────────────────────────────────────

const BASE_URL: &str = "https://api.binance.com";

/// Minimum order notional in FDUSD (Binance rule for all FDUSD pairs).
pub const MIN_NOTIONAL: f64 = 5.0;

/// Fixed order notional per tranche — $1.00 buffer above MIN_NOTIONAL to prevent
/// dust traps when price drops reduce position value below the $5.00 limit.
pub const TARGET_NOTIONAL_USD: f64 = 6.00;

// ── Data types ────────────────────────────────────────────────────────────────

/// Minimal open order representation.
#[derive(Debug, Clone)]
pub struct OpenOrder {
    pub order_id:     u64,
    pub side:         String,  // "BUY" or "SELL"
    pub price:        f64,
    pub orig_qty:     f64,
    pub executed_qty: f64,
}

/// Confirmed fill from GET /api/v3/myTrades.
#[derive(Debug, Clone)]
pub struct TradeFill {
    pub trade_id:        u64,
    pub order_id:        u64,
    pub price:           f64,
    pub qty:             f64,
    pub quote_qty:       f64,  // FDUSD notional (authoritative)
    pub is_buyer:        bool,
    pub commission:      f64,
    pub comm_asset:      String,
    pub time_ms:         u64,
    /// Fee converted to FDUSD.  0.0 for LIMIT_MAKER orders.
    pub trading_fee_usd: f64,
}

// ── Client ────────────────────────────────────────────────────────────────────

pub struct BinanceClient {
    api_key:    String,
    api_secret: String,
    http:       reqwest::Client,
}

impl BinanceClient {
    pub fn new(api_key: String, api_secret: String) -> Result<Self> {
        let http = reqwest::Client::builder()
            .use_rustls_tls()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .context("failed to build reqwest client")?;
        Ok(Self { api_key, api_secret, http })
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    fn sign(&self, params: &str) -> String {
        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC accepts any key length");
        mac.update(params.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    fn append_sig(&self, params: &str) -> String {
        format!("{}&signature={}", params, self.sign(params))
    }

    /// Public signing helper — used by the UDS WebSocket task in main.rs.
    pub fn sign_params(&self, params: &str) -> String {
        self.sign(params)
    }

    /// Returns the API key — used by the UDS WebSocket task in main.rs.
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    // ── User Data Stream (REST management) ───────────────────────────────────

    /// Phase 4 Step 1 — POST /api/v3/userDataStream
    /// Creates a listenKey for the User Data Stream (weight: 2).
    /// The listenKey is then used to connect to:
    ///   wss://stream.binance.com:9443/ws/<listenKey>
    pub async fn get_listen_key(&self) -> Result<String> {
        let url = format!("{}/api/v3/userDataStream", BASE_URL);
        let http_resp = self.http
            .post(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send().await
            .context("get_listen_key: network error")?;

        let status = http_resp.status();
        let body   = http_resp.text().await.unwrap_or_default();

        let resp: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| anyhow::anyhow!(
                "get_listen_key: non-JSON response (HTTP {}): {} — body: {}",
                status, e, &body[..body.len().min(200)]
            ))?;

        resp["listenKey"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("get_listen_key: missing listenKey in response: {}", resp))
    }

    /// Phase 4 Step 3 (keepalive) — PUT /api/v3/userDataStream
    /// Extends the listenKey validity by 60 minutes. Call every ~30 minutes.
    pub async fn keepalive_listen_key(&self, listen_key: &str) -> Result<()> {
        let url = format!("{}/api/v3/userDataStream?listenKey={}", BASE_URL, listen_key);
        let resp = self.http
            .put(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send().await
            .context("keepalive_listen_key: network error")?;

        if resp.status().is_success() {
            Ok(())
        } else {
            let body = resp.text().await.unwrap_or_default();
            Err(anyhow::anyhow!("keepalive_listen_key failed: {}", body))
        }
    }

    // ── Account ───────────────────────────────────────────────────────────────

    /// Fetch all non-zero spot balances.
    /// Returns HashMap<asset, free_amount>, e.g. {"BTC": 0.00012, "FDUSD": 23.41}.
    pub async fn get_balances(&self) -> Result<HashMap<String, f64>> {
        let params = format!("recvWindow=5000&timestamp={}", Self::now_ms());
        let url = format!("{}/api/v3/account?{}", BASE_URL, self.append_sig(&params));

        let resp: serde_json::Value = self.http
            .get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send().await
            .context("GET /api/v3/account failed")?
            .json().await
            .context("parse /api/v3/account response")?;

        if let Some(code) = resp["code"].as_i64() {
            if code < 0 {
                return Err(anyhow!("account error {}: {}", code, resp["msg"]));
            }
        }

        let balances = resp["balances"]
            .as_array()
            .ok_or_else(|| anyhow!("missing balances array"))?;

        // AMNESIA FIX: sum free + locked so assets sitting in open SELL orders
        // are counted — Binance moves coins to "locked" the moment a limit ask
        // is placed, which was causing the shadow-ledger to read 0 and spam bids.
        let mut map = HashMap::new();
        for b in balances {
            let asset = b["asset"].as_str().unwrap_or("").to_string();
            let free:   f64 = b["free"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
            let locked: f64 = b["locked"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
            let total = free + locked;
            if total > 0.0 || asset == "FDUSD" {
                map.insert(asset, total);
            }
        }
        Ok(map)
    }

    // ── Orders ────────────────────────────────────────────────────────────────

    /// Place a LIMIT_MAKER (post-only, 0% fee) order.
    ///
    /// `tick_size` and `lot_step` drive precision formatting so the same
    /// function serves all FDUSD pairs without hardcoded decimal counts.
    pub async fn place_limit_order(
        &self,
        symbol:    &str,
        side:      &str,
        price:     f64,
        qty:       f64,
        tick_size: f64,
        lot_step:  f64,
    ) -> Result<u64> {
        let price_r = round_price(price, tick_size);
        let qty_r   = round_qty(qty, lot_step);

        let notional = qty_r * price_r;
        if qty_r < lot_step {
            return Err(anyhow!("[{}] qty {:.8} < min lot {}", symbol, qty_r, lot_step));
        }
        if notional < MIN_NOTIONAL {
            return Err(anyhow!("[{}] notional {:.4} < MIN_NOTIONAL {}", symbol, notional, MIN_NOTIONAL));
        }

        let qty_s   = fmt_f64(qty_r,   lot_step);
        let price_s = fmt_f64(price_r, tick_size);

        let body = format!(
            "symbol={}&side={}&type=LIMIT_MAKER&quantity={}&price={}&selfTradePreventionMode=EXPIRE_MAKER&recvWindow=5000&timestamp={}",
            symbol, side, qty_s, price_s, Self::now_ms()
        );
        let body_signed = self.append_sig(&body);

        let resp: serde_json::Value = self.http
            .post(format!("{}/api/v3/order", BASE_URL))
            .header("X-MBX-APIKEY", &self.api_key)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body_signed)
            .send().await
            .context("POST /api/v3/order failed")?
            .json().await
            .context("parse order placement response")?;

        if let Some(code) = resp["code"].as_i64() {
            if code < 0 {
                return Err(anyhow!("[{}] place_order error {}: {}", symbol, code, resp["msg"]));
            }
        }

        let order_id = resp["orderId"]
            .as_u64()
            .ok_or_else(|| anyhow!("[{}] missing orderId: {}", symbol, resp))?;

        info!(
            "[{}] PLACED {} LIMIT_MAKER orderId={} price={} qty={} notional={:.4} FDUSD",
            symbol, side, order_id, price_s, qty_s, notional
        );
        Ok(order_id)
    }

    /// Place an immediate MARKET SELL — panic stop-loss only.
    pub async fn place_market_sell(&self, symbol: &str, qty: f64, lot_step: f64) -> Result<u64> {
        let qty_r = round_qty(qty, lot_step);
        if qty_r < lot_step {
            return Err(anyhow!("[{}] panic sell qty {:.8} < min lot {}", symbol, qty_r, lot_step));
        }

        let qty_s = fmt_f64(qty_r, lot_step);
        let body = format!(
            "symbol={}&side=SELL&type=MARKET&quantity={}&recvWindow=5000&timestamp={}",
            symbol, qty_s, Self::now_ms()
        );
        let body_signed = self.append_sig(&body);

        let resp: serde_json::Value = self.http
            .post(format!("{}/api/v3/order", BASE_URL))
            .header("X-MBX-APIKEY", &self.api_key)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body_signed)
            .send().await
            .context("POST /api/v3/order (MARKET SELL) failed")?
            .json().await
            .context("parse market sell response")?;

        if let Some(code) = resp["code"].as_i64() {
            if code < 0 {
                return Err(anyhow!("[{}] market_sell error {}: {}", symbol, code, resp["msg"]));
            }
        }

        let order_id = resp["orderId"]
            .as_u64()
            .ok_or_else(|| anyhow!("[{}] missing orderId in market sell: {}", symbol, resp))?;

        info!("[{}] PANIC SELL MARKET orderId={} qty={}", symbol, order_id, qty_s);
        Ok(order_id)
    }

    /// Cancel a single order by orderId.
    pub async fn cancel_order(&self, symbol: &str, order_id: u64) -> Result<()> {
        let params = format!(
            "symbol={}&orderId={}&recvWindow=5000&timestamp={}",
            symbol, order_id, Self::now_ms()
        );
        let url = format!("{}/api/v3/order?{}", BASE_URL, self.append_sig(&params));

        let resp: serde_json::Value = self.http
            .delete(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send().await
            .context("DELETE /api/v3/order failed")?
            .json().await
            .context("parse cancel response")?;

        let code = resp["code"].as_i64().unwrap_or(0);
        if code < 0 {
            warn!("[{}] cancel orderId={} → code={} msg={}", symbol, order_id, code, resp["msg"]);
        } else {
            info!("[{}] CANCELLED orderId={}", symbol, order_id);
        }
        Ok(())
    }

    /// Fetch all open orders for a specific symbol.
    pub async fn get_open_orders(&self, symbol: &str) -> Result<Vec<OpenOrder>> {
        let params = format!("symbol={}&recvWindow=5000&timestamp={}", symbol, Self::now_ms());
        let url = format!("{}/api/v3/openOrders?{}", BASE_URL, self.append_sig(&params));

        let resp: serde_json::Value = self.http
            .get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send().await
            .context("GET /api/v3/openOrders failed")?
            .json().await
            .context("parse openOrders response")?;

        if let Some(code) = resp["code"].as_i64() {
            if code < 0 {
                return Err(anyhow!("[{}] get_open_orders error {}: {}", symbol, code, resp["msg"]));
            }
        }

        let list = match resp.as_array() {
            Some(a) => a,
            None    => return Ok(vec![]),
        };

        let mut orders = Vec::with_capacity(list.len());
        for o in list {
            let order_id = o["orderId"].as_u64().unwrap_or(0);
            if order_id == 0 { continue; }
            orders.push(OpenOrder {
                order_id,
                side:         o["side"].as_str().unwrap_or("").to_string(),
                price:        o["price"].as_str().unwrap_or("0").parse().unwrap_or(0.0),
                orig_qty:     o["origQty"].as_str().unwrap_or("0").parse().unwrap_or(0.0),
                executed_qty: o["executedQty"].as_str().unwrap_or("0").parse().unwrap_or(0.0),
            });
        }
        Ok(orders)
    }

    /// Cancel ALL open orders for a symbol in one request.
    pub async fn cancel_all_open(&self, symbol: &str) -> Result<()> {
        let params = format!("symbol={}&recvWindow=5000&timestamp={}", symbol, Self::now_ms());
        let url = format!("{}/api/v3/openOrders?{}", BASE_URL, self.append_sig(&params));

        let resp: serde_json::Value = self.http
            .delete(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send().await
            .context("DELETE /api/v3/openOrders failed")?
            .json().await
            .context("parse bulk cancel response")?;

        let code = resp["code"].as_i64().unwrap_or(0);
        if code == -2011 || code == 0 {
            let n = resp.as_array().map(|a| a.len()).unwrap_or(0);
            if n > 0 {
                info!("[{}] Bulk-cancelled {} order(s) on startup.", symbol, n);
            }
        } else if code < 0 {
            warn!("[{}] Bulk cancel code={} — falling back to individual cancel.", symbol, code);
            let orders = self.get_open_orders(symbol).await?;
            for o in orders {
                if let Err(e) = self.cancel_order(symbol, o.order_id).await {
                    error!("[{}] Failed cancel orderId={}: {}", symbol, o.order_id, e);
                }
            }
        }
        Ok(())
    }

    // ── Trade history ─────────────────────────────────────────────────────────

    /// Fetch recent fills for `symbol`.
    /// `after_trade_id = 0` → most recent `limit` trades.
    pub async fn fetch_trades(&self, symbol: &str, after_trade_id: u64, limit: u32) -> Result<Vec<TradeFill>> {
        let params = if after_trade_id > 0 {
            format!(
                "symbol={}&fromId={}&limit={}&recvWindow=5000&timestamp={}",
                symbol, after_trade_id, limit, Self::now_ms()
            )
        } else {
            format!(
                "symbol={}&limit={}&recvWindow=5000&timestamp={}",
                symbol, limit, Self::now_ms()
            )
        };
        let url = format!("{}/api/v3/myTrades?{}", BASE_URL, self.append_sig(&params));

        let resp: serde_json::Value = self.http
            .get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send().await
            .context("GET /api/v3/myTrades failed")?
            .json().await
            .context("parse myTrades response")?;

        if let Some(code) = resp["code"].as_i64() {
            if code < 0 {
                return Err(anyhow!("[{}] fetch_trades error {}: {}", symbol, code, resp["msg"]));
            }
        }

        let list = match resp.as_array() {
            Some(a) => a,
            None    => return Ok(vec![]),
        };

        let mut fills = Vec::with_capacity(list.len());
        for t in list {
            let trade_id: u64 = t["id"].as_u64().unwrap_or(0);
            if trade_id == 0 { continue; }

            let price: f64     = t["price"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
            let qty: f64       = t["qty"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
            let quote_qty: f64 = t["quoteQty"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
            let is_buyer       = t["isBuyer"].as_bool().unwrap_or(false);
            let commission: f64 = t["commission"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
            let comm_asset     = t["commissionAsset"].as_str().unwrap_or("").to_string();
            let time_ms: u64   = t["time"].as_u64().unwrap_or(0);
            let order_id: u64  = t["orderId"].as_u64().unwrap_or(0);

            if price <= 0.0 || qty <= 0.0 { continue; }

            // Convert commission to FDUSD.
            let trading_fee_usd = if comm_asset == "FDUSD" {
                commission
            } else {
                commission * price  // coin-denominated or BNB → approx FDUSD
            };

            fills.push(TradeFill {
                trade_id, order_id, price, qty, quote_qty,
                is_buyer, commission, comm_asset, time_ms,
                trading_fee_usd,
            });
        }
        fills.sort_by_key(|f| f.trade_id);
        Ok(fills)
    }

}

// ── Pure utility functions ────────────────────────────────────────────────────

/// How many decimal places does this step size need?
#[inline]
fn decimal_places(step: f64) -> usize {
    if      step >= 1.0    { 0 }
    else if step >= 0.1    { 1 }
    else if step >= 0.01   { 2 }
    else if step >= 0.001  { 3 }
    else if step >= 0.0001 { 4 }
    else                   { 5 }
}

/// Format a float with the precision implied by `step`.
#[inline]
pub fn fmt_f64(value: f64, step: f64) -> String {
    format!("{:.prec$}", value, prec = decimal_places(step))
}

/// Truncate qty to lot_step precision (floor — never rounds up).
#[inline]
pub fn round_qty(qty: f64, lot_step: f64) -> f64 {
    let steps   = (qty / lot_step).floor();
    let rounded = steps * lot_step;
    if rounded < lot_step { 0.0 } else { rounded }
}

/// Round price to nearest tick_size.
#[inline]
pub fn round_price(price: f64, tick_size: f64) -> f64 {
    (price / tick_size).round() * tick_size
}

/// Compute coin qty to hit TARGET_NOTIONAL_USD ($6.00) at the given price.
/// Rounds UP to the nearest lot_step so notional always meets MIN_NOTIONAL.
pub fn qty_from_fixed_notional(price: f64, lot_step: f64) -> f64 {
    qty_from_notional(TARGET_NOTIONAL_USD, price, lot_step)
}

/// Compute coin qty to hit an explicit `notional` USD at the given price.
/// Used when the available quote balance is below TARGET_NOTIONAL_USD but
/// still above MIN_NOTIONAL — lets the bot trade with what it actually has.
pub fn qty_from_notional(notional: f64, price: f64, lot_step: f64) -> f64 {
    if price <= 0.0 || notional <= 0.0 { return 0.0; }
    let raw     = notional / price;
    let stepped = (raw / lot_step).ceil() * lot_step;
    if stepped * price < MIN_NOTIONAL { 0.0 } else { stepped }
}
