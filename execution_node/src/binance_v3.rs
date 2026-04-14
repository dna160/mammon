// ============================================================
// binance_v3.rs — Binance REST Client
//
// Connection method replicated from mammond/project_sniper (proven working).
// Key differences from broken version:
//   1. reqwest 0.12 with use_rustls_tls() — no native-tls / OpenSSL
//   2. Order params sent as application/x-www-form-urlencoded BODY
//      (not URL query string) — matches mammond's POST pattern
//   3. recvWindow=5000 on every signed request
//   4. base URL overridable via BINANCE_REST_BASE env var
// ============================================================

use anyhow::{anyhow, Context, Result};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{error, info, warn};

type HmacSha256 = Hmac<Sha256>;

// Base URL — overridable via env for testnet/regional endpoints
fn binance_base() -> String {
    std::env::var("BINANCE_REST_BASE")
        .unwrap_or_else(|_| "https://api.binance.com".to_string())
}

// ── Response Types ────────────────────────────────────────────────────────────

/// Exchange info for a single symbol
#[derive(Debug, serde::Deserialize)]
struct ExchangeInfoResponse {
    symbols: Vec<SymbolInfo>,
}

#[derive(Debug, serde::Deserialize)]
struct SymbolInfo {
    symbol:  String,
    filters: Vec<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct TickInfo {
    pub tick_size: f64,
    pub lot_step:  f64,
}

// ── Client ────────────────────────────────────────────────────────────────────

pub struct BinanceClient {
    http:       reqwest::Client,
    api_key:    String,
    api_secret: String,
}

impl BinanceClient {
    pub fn new(api_key: &str, api_secret: &str) -> Self {
        // Replicated exactly from mammond BinanceClient::new()
        let http = reqwest::Client::builder()
            .use_rustls_tls()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("failed to build reqwest client");
        Self {
            http,
            api_key:    api_key.to_string(),
            api_secret: api_secret.to_string(),
        }
    }

    pub fn api_key(&self) -> &str { &self.api_key }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    fn sign(&self, params: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC accepts any key length");
        mac.update(params.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// Appends &signature=... to a param string — same as mammond append_sig()
    fn append_sig(&self, params: &str) -> String {
        format!("{}&signature={}", params, self.sign(params))
    }

    // ── Exchange Info ─────────────────────────────────────────────────────────

    /// Fetch tick_size and lot_step from Binance exchangeInfo.
    /// Non-fatal: engine boots with hardcoded BTCUSDT defaults if this fails.
    pub async fn get_tick_info(&self, symbol: &str) -> Result<TickInfo> {
        let url = format!(
            "{}/api/v3/exchangeInfo?symbol={}",
            binance_base(), symbol
        );
        let resp = self.http.get(&url).send().await
            .context("exchangeInfo request failed")?;

        let status = resp.status();
        let body   = resp.text().await.context("exchangeInfo body read failed")?;

        if !status.is_success() {
            anyhow::bail!(
                "exchangeInfo HTTP {} — body: {}",
                status, &body[..body.len().min(300)]
            );
        }

        let info: ExchangeInfoResponse = serde_json::from_str(&body)
            .with_context(|| format!("exchangeInfo parse failed: {}", &body[..body.len().min(200)]))?;

        let sym = info.symbols.into_iter()
            .find(|s| s.symbol == symbol)
            .with_context(|| format!("symbol {} not in exchangeInfo", symbol))?;

        let mut tick_size = 0.01_f64;
        let mut lot_step  = 0.00001_f64;

        for f in &sym.filters {
            match f["filterType"].as_str().unwrap_or("") {
                "PRICE_FILTER" => {
                    if let Some(ts) = f["tickSize"].as_str() {
                        tick_size = ts.parse().unwrap_or(0.01);
                    }
                }
                "LOT_SIZE" => {
                    if let Some(ss) = f["stepSize"].as_str() {
                        lot_step = ss.parse().unwrap_or(0.00001);
                    }
                }
                _ => {}
            }
        }

        Ok(TickInfo { tick_size, lot_step })
    }

    // ── User Data Stream ──────────────────────────────────────────────────────

    /// POST /api/v3/userDataStream — creates a listenKey
    pub async fn get_listen_key(&self) -> Result<String> {
        let url = format!("{}/api/v3/userDataStream", binance_base());
        let http_resp = self.http
            .post(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send().await
            .context("get_listen_key: network error")?;

        let status = http_resp.status();
        let body   = http_resp.text().await.unwrap_or_default();

        if !status.is_success() {
            anyhow::bail!("get_listen_key HTTP {}: {}", status, &body[..body.len().min(200)]);
        }

        let resp: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| anyhow!(
                "get_listen_key: non-JSON response (HTTP {}): {} — body: {}",
                status, e, &body[..body.len().min(200)]
            ))?;

        resp["listenKey"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("get_listen_key: missing listenKey in: {}", resp))
    }

    /// PUT /api/v3/userDataStream — keepalive (call every 25 min)
    pub async fn keepalive_listen_key(&self, listen_key: &str) -> Result<()> {
        let url = format!(
            "{}/api/v3/userDataStream?listenKey={}",
            binance_base(), listen_key
        );
        let resp = self.http
            .put(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send().await
            .context("keepalive_listen_key: network error")?;

        if resp.status().is_success() {
            Ok(())
        } else {
            let body = resp.text().await.unwrap_or_default();
            Err(anyhow!("keepalive_listen_key failed: {}", body))
        }
    }

    // ── Order Placement ───────────────────────────────────────────────────────

    /// Place a LIMIT_MAKER (post-only, 0% fee) order.
    /// Params sent as application/x-www-form-urlencoded POST body — mammond pattern.
    pub async fn place_limit_maker(
        &self,
        symbol: &str,
        side:   &str,
        qty:    f64,
        price:  f64,
    ) -> Result<u64> {
        let tick_size = 0.01_f64; // formatting fallback — engine already rounds
        let lot_step  = 0.00001_f64;
        let price_s   = fmt_f64(price, tick_size);
        let qty_s     = fmt_f64(qty,   lot_step);

        let body = format!(
            "symbol={}&side={}&type=LIMIT_MAKER&quantity={}&price={}&selfTradePreventionMode=EXPIRE_MAKER&recvWindow=5000&timestamp={}",
            symbol, side, qty_s, price_s, Self::now_ms()
        );
        let body_signed = self.append_sig(&body);

        let resp: serde_json::Value = self.http
            .post(format!("{}/api/v3/order", binance_base()))
            .header("X-MBX-APIKEY", &self.api_key)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body_signed)
            .send().await
            .context("POST /api/v3/order (LIMIT_MAKER) failed")?
            .json().await
            .context("parse LIMIT_MAKER response")?;

        if let Some(code) = resp["code"].as_i64() {
            if code < 0 {
                return Err(anyhow!("[{}] place_limit_maker error {}: {}", symbol, code, resp["msg"]));
            }
        }

        let order_id = resp["orderId"]
            .as_u64()
            .ok_or_else(|| anyhow!("[{}] missing orderId: {}", symbol, resp))?;

        info!(
            "[{}] PLACED {} LIMIT_MAKER orderId={} price={} qty={}",
            symbol, side, order_id, price_s, qty_s
        );
        Ok(order_id)
    }

    /// Emergency MARKET SELL — only on emergency_dump_triggered.
    pub async fn market_sell(&self, symbol: &str, qty: f64) -> Result<()> {
        let qty_s = fmt_f64(qty, 0.00001);
        let body  = format!(
            "symbol={}&side=SELL&type=MARKET&quantity={}&recvWindow=5000&timestamp={}",
            symbol, qty_s, Self::now_ms()
        );
        let body_signed = self.append_sig(&body);

        let resp: serde_json::Value = self.http
            .post(format!("{}/api/v3/order", binance_base()))
            .header("X-MBX-APIKEY", &self.api_key)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body_signed)
            .send().await
            .context("POST /api/v3/order (MARKET SELL) failed")?
            .json().await
            .context("parse MARKET SELL response")?;

        if let Some(code) = resp["code"].as_i64() {
            if code < 0 {
                error!("[{}] MARKET SELL error {}: {}", symbol, code, resp["msg"]);
                anyhow::bail!("market_sell failed: code={}", code);
            }
        }

        info!("[{}] MARKET SELL executed qty={}", symbol, qty_s);
        Ok(())
    }

    /// Cancel a specific order by orderId — mammond pattern (URL params + sig).
    pub async fn cancel_order(&self, symbol: &str, order_id: u64) -> Result<()> {
        let params = format!(
            "symbol={}&orderId={}&recvWindow=5000&timestamp={}",
            symbol, order_id, Self::now_ms()
        );
        let url = format!("{}/api/v3/order?{}", binance_base(), self.append_sig(&params));

        let resp: serde_json::Value = self.http
            .delete(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send().await
            .context("DELETE /api/v3/order failed")?
            .json().await
            .context("parse cancel response")?;

        let code = resp["code"].as_i64().unwrap_or(0);
        if code == -2011 || code == 0 {
            // -2011: order not found (already filled/cancelled) — non-fatal
        } else if code < 0 {
            warn!("[{}] cancel orderId={} code={} msg={}", symbol, order_id, code, resp["msg"]);
        } else {
            info!("[{}] CANCELLED orderId={}", symbol, order_id);
        }
        Ok(())
    }

    /// Cancel ALL open orders for a symbol — mammond's cancel_all_open() pattern.
    pub async fn cancel_all_orders(&self, symbol: &str) -> Result<()> {
        let params = format!(
            "symbol={}&recvWindow=5000&timestamp={}",
            symbol, Self::now_ms()
        );
        let url = format!(
            "{}/api/v3/openOrders?{}",
            binance_base(), self.append_sig(&params)
        );

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
            if n > 0 { info!("[{}] Bulk-cancelled {} orders.", symbol, n); }
        } else if code < 0 {
            warn!("[{}] Bulk cancel code={} msg={}", symbol, code, resp["msg"]);
        }
        Ok(())
    }

    // ── Utility Formatters ────────────────────────────────────────────────────

    /// Round price to nearest tick_size grid.
    pub fn round_to_tick(price: f64, tick_size: f64) -> f64 {
        (price / tick_size).round() * tick_size
    }

    /// Floor quantity to lot_step (never round up / over-commit).
    pub fn floor_to_lot(qty: f64, lot_step: f64) -> f64 {
        (qty / lot_step).floor() * lot_step
    }
}

// ── Formatting helpers (replicated from mammond) ──────────────────────────────

#[inline]
fn decimal_places(step: f64) -> usize {
    if      step >= 1.0    { 0 }
    else if step >= 0.1    { 1 }
    else if step >= 0.01   { 2 }
    else if step >= 0.001  { 3 }
    else if step >= 0.0001 { 4 }
    else                   { 5 }
}

#[inline]
pub fn fmt_f64(value: f64, step: f64) -> String {
    format!("{:.prec$}", value, prec = decimal_places(step))
}
