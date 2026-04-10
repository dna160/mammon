//! Tokocrypto REST API client — order execution for Engine D.
//!
//! API reference: https://www.tokocrypto.com/apidocs/
//! Base URL:      https://www.tokocrypto.com
//! Auth:          HMAC-SHA256 over query/body params; X-MBX-APIKEY header.
//!
//! BTC_IDR exchange constraints (from /open/v1/common/symbols):
//!   LOT_SIZE      min=0.00001 BTC, step=0.00001 BTC, max=3.0 BTC
//!   NOTIONAL      min=20,000 IDR
//!   PRICE_FILTER  tick=1 IDR (integer prices only)
//!
//! Order side enums (Tokocrypto): 0 = BUY, 1 = SELL
//! Order type enums:              1 = LIMIT
//! timeInForce enums:             1 = GTC

use anyhow::{anyhow, Context, Result};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{error, info, warn};

// ── Exchange constants ────────────────────────────────────────────────────────

const BASE_URL: &str  = "https://www.tokocrypto.com";
pub const SYMBOL: &str = "BTC_IDR";

/// Minimum lot in BTC (LOT_SIZE filter minQty / stepSize).
pub const LOT_STEP_BTC: f64  = 0.00001;

/// Minimum order notional in IDR (NOTIONAL filter minNotional).
pub const MIN_NOTIONAL_IDR: f64 = 20_000.0;

/// Price tick size in IDR — all prices must be integers.
pub const PRICE_TICK_IDR: f64 = 1.0;

// ── Fee rates (applied to gross notional unless API returns exact amounts) ────
//
// Indonesian regulation (BAPPEBTI / PMK 68/2022 & 69/2022) mandates that
// BAPPEBTI-registered crypto exchanges collect the following on every trade:
//
//   PPh  (Pajak Penghasilan / final income tax): 0.10 % of transaction value
//   PPN  (Pajak Pertambahan Nilai / VAT):        0.11 % of transaction value
//
// CFX is the commodity futures exchange clearing fee charged through KBI
// (Kliring Berjangka Indonesia).  Default: 0.01 % of notional.
// Adjust CFX_RATE if your Tokocrypto account tier differs.
//
// Tokocrypto returns the trading commission directly in the `commission` field
// of each trade record.  PPh, PPN, and CFX amounts are read from the API
// response when available (fields: `pphAmount`, `ppnAmount`, `cfxFee`);
// otherwise they are computed from the rates below.

/// PPh (income tax) rate on gross notional — 0.10 %.
pub const PPH_RATE: f64 = 0.001;
/// PPN (VAT) rate on gross notional — 0.11 %.
pub const PPN_RATE: f64 = 0.0011;
/// CFX clearing/settlement fee rate on gross notional — 0.01 %.
pub const CFX_RATE: f64 = 0.0001;

// ── Order side / type helpers ─────────────────────────────────────────────────

pub const SIDE_BUY:  u8 = 0;
pub const SIDE_SELL: u8 = 1;

// ── Public data types ─────────────────────────────────────────────────────────

/// Spot account balances relevant to BTC/IDR market making.
#[derive(Debug, Clone, Copy)]
pub struct Balance {
    pub btc_free: f64,
    pub idr_free: f64,
}

/// Minimal representation of an open order returned by GET /open/v1/orders.
#[derive(Debug, Clone)]
pub struct OpenOrder {
    pub order_id:    u64,
    pub side:        u8,
    pub price:       f64,
    pub orig_qty:    f64,
    pub executed_qty: f64,
}

// ── Client ────────────────────────────────────────────────────────────────────

/// Authenticated Tokocrypto REST client.
/// All methods are async and non-blocking; call them from tokio tasks.
pub struct TokoClient {
    api_key:    String,
    api_secret: String,
    http:       reqwest::Client,
}

impl TokoClient {
    /// Build a new client.  Panics if the TLS stack cannot be initialised
    /// (this should never happen with bundled rustls roots).
    pub fn new(api_key: String, api_secret: String) -> Result<Self> {
        let http = reqwest::Client::builder()
            .use_rustls_tls()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .context("failed to build reqwest client")?;
        Ok(Self { api_key, api_secret, http })
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /// Current Unix time in milliseconds (local clock).
    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    /// HMAC-SHA256 of `params` using the API secret.
    fn sign(&self, params: &str) -> String {
        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC accepts any key length");
        mac.update(params.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// Append `&signature=<hmac>` to a param string.
    fn append_sig(&self, params: &str) -> String {
        format!("{}&signature={}", params, self.sign(params))
    }

    // ── Account ───────────────────────────────────────────────────────────────

    /// Fetch BTC and IDR free balances from the spot account.
    pub async fn get_balances(&self) -> Result<Balance> {
        let params = format!("recvWindow=5000&timestamp={}", Self::now_ms());
        let url    = format!("{}/open/v1/account/spot?{}", BASE_URL, self.append_sig(&params));

        let resp: serde_json::Value = self.http
            .get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send().await
            .context("GET /account/spot failed")?
            .json().await
            .context("parse /account/spot response")?;

        if resp["code"].as_i64().unwrap_or(-1) != 0 {
            return Err(anyhow!("account/spot error: {}", resp["msg"]));
        }

        let assets = resp["data"]["accountAssets"]
            .as_array()
            .ok_or_else(|| anyhow!("missing accountAssets"))?;

        let mut bal = Balance { btc_free: 0.0, idr_free: 0.0 };
        for a in assets {
            let asset = a["asset"].as_str().unwrap_or("");
            let free: f64 = a["free"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
            match asset {
                "BTC" => bal.btc_free = free,
                "IDR" => bal.idr_free = free,
                _ => {}
            }
        }
        Ok(bal)
    }

    // ── Orders ────────────────────────────────────────────────────────────────

    /// Place a GTC limit order.
    /// Returns the exchange-assigned `orderId`.
    ///
    /// `side`: 0 = BUY, 1 = SELL
    /// `price`: in IDR, will be rounded to the nearest integer.
    /// `qty`: in BTC, will be truncated to LOT_STEP_BTC precision.
    pub async fn place_limit_order(
        &self,
        side:  u8,
        price: f64,
        qty:   f64,
    ) -> Result<u64> {
        let price_rounded = price.round() as u64;
        let qty_stepped   = round_qty(qty);

        // Safety: reject orders below exchange minimums.
        let notional = qty_stepped * price_rounded as f64;
        if qty_stepped < LOT_STEP_BTC {
            return Err(anyhow!("qty {qty_stepped} < min lot {LOT_STEP_BTC}"));
        }
        if notional < MIN_NOTIONAL_IDR {
            return Err(anyhow!("notional {notional:.0} IDR < min {MIN_NOTIONAL_IDR}"));
        }

        let body = format!(
            "symbol={}&side={}&type=1&quantity={:.5}&price={}&timeInForce=1&recvWindow=5000&timestamp={}",
            SYMBOL, side, qty_stepped, price_rounded, Self::now_ms()
        );
        let body_signed = self.append_sig(&body);

        let resp: serde_json::Value = self.http
            .post(format!("{}/open/v1/orders", BASE_URL))
            .header("X-MBX-APIKEY", &self.api_key)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body_signed)
            .send().await
            .context("POST /open/v1/orders failed")?
            .json().await
            .context("parse order placement response")?;

        if resp["code"].as_i64().unwrap_or(-1) != 0 {
            return Err(anyhow!("place_order error: {}", resp["msg"]));
        }

        let order_id = resp["data"]["orderId"]
            .as_u64()
            .or_else(|| resp["data"]["orderId"].as_str()?.parse().ok())
            .ok_or_else(|| anyhow!("missing orderId in response"))?;

        info!(
            "[TokoREST] PLACED {} orderId={} price={} qty={:.5} notional={:.0} IDR",
            if side == SIDE_BUY { "BUY" } else { "SELL" },
            order_id, price_rounded, qty_stepped, notional
        );
        Ok(order_id)
    }

    /// Cancel a single order by its exchange orderId.
    pub async fn cancel_order(&self, order_id: u64) -> Result<()> {
        let body = format!(
            "orderId={}&recvWindow=5000&timestamp={}",
            order_id, Self::now_ms()
        );
        let body_signed = self.append_sig(&body);

        let resp: serde_json::Value = self.http
            .post(format!("{}/open/v1/orders/cancel", BASE_URL))
            .header("X-MBX-APIKEY", &self.api_key)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body_signed)
            .send().await
            .context("POST /open/v1/orders/cancel failed")?
            .json().await
            .context("parse cancel response")?;

        let code = resp["code"].as_i64().unwrap_or(-1);
        if code != 0 {
            // 404 / order already filled or cancelled — not an error for our purposes.
            warn!("[TokoREST] cancel orderId={} → code={} msg={}", order_id, code, resp["msg"]);
        } else {
            info!("[TokoREST] CANCELLED orderId={}", order_id);
        }
        Ok(())
    }

    /// Fetch all open BTC_IDR orders.
    pub async fn get_open_orders(&self) -> Result<Vec<OpenOrder>> {
        let params = format!("recvWindow=5000&timestamp={}", Self::now_ms());
        let url    = format!("{}/open/v1/orders?{}", BASE_URL, self.append_sig(&params));

        let resp: serde_json::Value = self.http
            .get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send().await
            .context("GET /open/v1/orders failed")?
            .json().await
            .context("parse open orders response")?;

        if resp["code"].as_i64().unwrap_or(-1) != 0 {
            return Err(anyhow!("get_open_orders error: {}", resp["msg"]));
        }

        let list = match resp["data"]["list"].as_array() {
            Some(l) => l,
            None    => return Ok(vec![]),
        };

        let mut orders = Vec::with_capacity(list.len());
        for o in list {
            // Only return BTC_IDR orders (the endpoint returns all symbols).
            if o["symbol"].as_str().unwrap_or("") != SYMBOL { continue; }

            let order_id = o["orderId"]
                .as_u64()
                .or_else(|| o["orderId"].as_str()?.parse().ok())
                .unwrap_or(0);

            orders.push(OpenOrder {
                order_id,
                side:        o["side"].as_u64().unwrap_or(0) as u8,
                price:       o["price"].as_str().unwrap_or("0").parse().unwrap_or(0.0),
                orig_qty:    o["origQty"].as_f64()
                                .or_else(|| o["origQty"].as_str()?.parse().ok())
                                .unwrap_or(0.0),
                executed_qty: o["executedQty"].as_f64()
                                .or_else(|| o["executedQty"].as_str()?.parse().ok())
                                .unwrap_or(0.0),
            });
        }
        Ok(orders)
    }

    /// Cancel every open BTC_IDR order.  Call on startup and shutdown.
    pub async fn cancel_all_open(&self) -> Result<()> {
        let orders = self.get_open_orders().await?;
        if orders.is_empty() {
            info!("[TokoREST] No open BTC_IDR orders to cancel on startup.");
            return Ok(());
        }
        info!("[TokoREST] Cancelling {} open BTC_IDR order(s) on startup…", orders.len());
        for o in orders {
            if let Err(e) = self.cancel_order(o.order_id).await {
                error!("[TokoREST] Failed to cancel orderId={}: {}", o.order_id, e);
            }
        }
        Ok(())
    }

    // ── Trade history ─────────────────────────────────────────────────────────

    /// Fetch recent fills for BTC_IDR.
    /// `after_trade_id = 0` → return the most recent `limit` trades.
    /// `after_trade_id > 0` → return trades with tradeId > after_trade_id
    ///                        (ascending, oldest first).
    pub async fn fetch_trades(&self, after_trade_id: u64, limit: u32) -> Result<Vec<TradeFill>> {
        let params = if after_trade_id > 0 {
            format!(
                "symbol={}&fromId={}&direct=prev&limit={}&recvWindow=5000&timestamp={}",
                SYMBOL, after_trade_id, limit, Self::now_ms()
            )
        } else {
            format!(
                "symbol={}&limit={}&recvWindow=5000&timestamp={}",
                SYMBOL, limit, Self::now_ms()
            )
        };
        let url = format!("{}/open/v1/orders/trades?{}", BASE_URL, self.append_sig(&params));

        let resp: serde_json::Value = self.http
            .get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send().await
            .context("GET /orders/trades failed")?
            .json().await
            .context("parse /orders/trades response")?;

        if resp["code"].as_i64().unwrap_or(-1) != 0 {
            return Err(anyhow!("fetch_trades error: {}", resp["msg"]));
        }

        let list = match resp["data"]["list"].as_array() {
            Some(l) => l,
            None    => return Ok(vec![]),
        };

        let mut fills = Vec::with_capacity(list.len());
        for t in list {
            let trade_id: u64 = t["tradeId"]
                .as_str().unwrap_or("0").parse().unwrap_or(0);
            if trade_id == 0 { continue; }

            let price: f64      = t["price"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
            let qty: f64        = t["qty"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
            let quote_qty: f64  = t["quoteQty"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
            let is_buyer        = t["isBuyer"].as_i64().unwrap_or(0) == 1;
            let commission: f64 = t["commission"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
            let comm_asset      = t["commissionAsset"].as_str().unwrap_or("").to_string();
            let time_ms: u64    = t["time"].as_str().unwrap_or("0").parse().unwrap_or(0);
            let order_id: u64   = t["orderId"].as_str().unwrap_or("0").parse().unwrap_or(0);

            if price <= 0.0 || qty <= 0.0 { continue; }

            // ── Fee breakdown ─────────────────────────────────────────────────
            // Use the API's authoritative quote notional if available (avoids
            // minor f64 rounding differences vs. price × qty).
            let notional = if quote_qty > 0.0 { quote_qty } else { price * qty };

            // 1. Trading fee — commission field; convert to IDR if in BTC.
            let trading_fee_idr = if comm_asset == "BTC" {
                commission * price   // BTC-denominated fee → IDR
            } else {
                commission           // IDR-denominated fee (Tokocrypto norm for IDR pairs)
            };

            // 2. PPh (income tax 0.10 %) — read from API or compute.
            //    Tokocrypto may return this as "pphAmount", "pphamount", or "pph".
            let pph_fee_idr: f64 = ["pphAmount", "pphamount", "pph"]
                .iter()
                .find_map(|key| {
                    t[key].as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .filter(|&v| v > 0.0)
                })
                .unwrap_or(notional * PPH_RATE);

            // 3. PPN (VAT 0.11 %) — read from API or compute.
            //    Tokocrypto may return this as "ppnAmount", "ppnamount", or "ppn".
            let ppn_fee_idr: f64 = ["ppnAmount", "ppnamount", "ppn"]
                .iter()
                .find_map(|key| {
                    t[key].as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .filter(|&v| v > 0.0)
                })
                .unwrap_or(notional * PPN_RATE);

            // 4. CFX clearing fee (0.01 %) — read from API or compute.
            //    Possible API field names: "cfxFee", "cfxAmount", "clearingFee".
            let cfx_fee_idr: f64 = ["cfxFee", "cfxAmount", "clearingFee", "cfx"]
                .iter()
                .find_map(|key| {
                    t[key].as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .filter(|&v| v > 0.0)
                })
                .unwrap_or(notional * CFX_RATE);

            let total_fee_idr = trading_fee_idr + pph_fee_idr + ppn_fee_idr + cfx_fee_idr;

            fills.push(TradeFill {
                trade_id, order_id, price, qty, quote_qty,
                is_buyer, commission, comm_asset, time_ms,
                trading_fee_idr, pph_fee_idr, ppn_fee_idr, cfx_fee_idr, total_fee_idr,
            });
        }
        // Return in ascending trade_id order for reliable cursor advancement.
        fills.sort_by_key(|f| f.trade_id);
        Ok(fills)
    }
} // end impl TokoClient

/// A confirmed exchange fill returned by GET /open/v1/orders/trades.
///
/// All four fee fields are in IDR.  `total_fee_idr` is their sum and is the
/// only value callers need to deduct from PnL on each side:
///   BUY  → pnl -= notional + total_fee_idr
///   SELL → pnl += notional - total_fee_idr
#[derive(Debug, Clone)]
pub struct TradeFill {
    pub trade_id:        u64,
    pub order_id:        u64,
    pub price:           f64,
    pub qty:             f64,
    pub quote_qty:       f64,   // price × qty (IDR notional, authoritative from API)
    pub is_buyer:        bool,  // true = we bought BTC

    // Raw commission fields from the API (kept for auditing).
    pub commission:      f64,
    pub comm_asset:      String,
    pub time_ms:         u64,

    // ── Fee breakdown in IDR ─────────────────────────────────────────────────
    /// Exchange maker/taker commission converted to IDR.
    pub trading_fee_idr: f64,
    /// PPh final income tax (0.10 % of notional, BAPPEBTI mandate).
    pub pph_fee_idr:     f64,
    /// PPN VAT (0.11 % of notional, BAPPEBTI mandate).
    pub ppn_fee_idr:     f64,
    /// CFX clearing/settlement fee (0.01 % of notional, KBI mandate).
    pub cfx_fee_idr:     f64,
    /// Sum of all four fees — use this to adjust PnL.
    pub total_fee_idr:   f64,
}

// ── Pure utility functions (no heap allocation) ───────────────────────────────

/// Truncate `qty` to the nearest `LOT_STEP_BTC` (floor, no rounding up).
/// Returns 0.0 if the result is below the minimum lot.
#[inline]
pub fn round_qty(qty: f64) -> f64 {
    let steps = (qty / LOT_STEP_BTC).floor();
    let rounded = steps * LOT_STEP_BTC;
    // Guard against floating-point artefacts producing negative values.
    if rounded < LOT_STEP_BTC { 0.0 } else { rounded }
}

/// Round `price` to the nearest integer IDR tick (PRICE_TICK_IDR = 1.0).
#[inline]
pub fn round_price(price: f64) -> f64 {
    price.round()
}

/// Compute the order quantity for one side given the available balance.
/// `fraction` is how much of the balance to commit per side (e.g. 0.5 = 50%).
/// Returns 0.0 if the resulting notional is below MIN_NOTIONAL_IDR.
pub fn qty_from_balance(balance: f64, price: f64, fraction: f64) -> f64 {
    if price <= 0.0 { return 0.0; }
    let raw = (balance * fraction) / price;
    let stepped = round_qty(raw);
    let notional = stepped * price;
    if notional < MIN_NOTIONAL_IDR { 0.0 } else { stepped }
}
