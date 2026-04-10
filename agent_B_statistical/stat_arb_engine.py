"""
Engine B: Statistical Arbitrage (High-Velocity Mean Reversion)

Strategy: Trade mean-reversion on BTC/USDT vs ETH/USDT price ratio on Tokocrypto.
Execution: MARKET orders on BTC_IDR (wallet is denominated in IDR).

Math:
  Ratio_t   = BTC_price / ETH_price
  Rolling 15-min window (min 30 periods) → Mean(μ), StdDev(σ)
  Z_Score   = (Ratio - μ) / σ

Entry (LONG only — wallet is IDR):
  z < -THRESHOLD  → BUY BTC_IDR  (BTC cheap relative to ETH)
  z > +THRESHOLD  → SKIP         (would need BTC to sell short)

Exit: unrealized PnL >= TP OR <= -SL OR elapsed >= 3600s
  → MARKET SELL BTC_IDR to recover IDR
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import math
import os
import time
import urllib.parse
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import psycopg2
import psycopg2.pool
import redis.asyncio as aioredis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ENGINE_B] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
DB_DSN    = os.getenv("DB_DSN",    "postgresql://mammon:mammon@localhost:5432/mammon")

TOKO_API_KEY    = os.getenv("TOKO_API_KEY", "")
TOKO_API_SECRET = os.getenv("TOKO_API_SECRET", "")
TOKO_BASE       = "https://www.tokocrypto.com"

# Trade sizing — IDR wallet
TRADE_SIZE_IDR      = 130_000   # IDR per trade (~$7.7 at 16,900 IDR/USD)
WALLET_IDR          = 800_000   # IDR allocated to Engine B (leaving 200k buffer)
ZSCORE_THRESHOLD    = 2.0
WINDOW_SECONDS      = 900       # 15-minute rolling window
MIN_PERIODS         = 30
MAX_DURATION_SECS   = 3600      # 1-hour timeout
POLL_INTERVAL_SECS  = 1.0
ROUND_TRIP_FEE      = 0.003     # 0.3% round-trip taker fee (1.5% maker each side)
SYMBOL              = "BTC_IDR"


@dataclass
class Position:
    direction:   str    # "LONG" only for now
    entry_ratio: float
    entry_price: float  # BTC_IDR entry price in IDR
    entry_time:  float
    btc_qty:     float  # BTC bought
    tp_idr:      float  # take-profit threshold in IDR
    sl_idr:      float  # stop-loss threshold in IDR (positive value)
    order_id:    Optional[int] = None


# ── Tokocrypto REST client ────────────────────────────────────────────────────

def _sign(params: dict) -> str:
    qs = urllib.parse.urlencode(params)
    return hmac.new(TOKO_API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()


async def toko_request(
    session: aiohttp.ClientSession,
    method: str,
    path: str,
    params: dict,
) -> dict:
    params["timestamp"] = int(time.time() * 1000)
    params["signature"] = _sign(params)
    url = f"{TOKO_BASE}{path}"
    headers = {"X-MBX-APIKEY": TOKO_API_KEY}
    try:
        if method == "GET":
            async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as r:
                return await r.json()
        else:
            async with session.post(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as r:
                return await r.json()
    except Exception as exc:
        log.error("Toko REST %s %s failed: %s", method, path, exc)
        return {"code": -1, "msg": str(exc)}


async def market_buy_btc(session: aiohttp.ClientSession, idr_amount: float) -> Optional[dict]:
    """Place a MARKET BUY on BTC_IDR using quoteOrderQty (spend N IDR)."""
    params = {
        "symbol":        SYMBOL,
        "side":          0,          # 0 = BUY
        "type":          2,          # 2 = MARKET
        "quoteOrderQty": str(int(idr_amount)),
    }
    result = await toko_request(session, "POST", "/open/v1/orders", params)
    if result.get("code") == 0:
        log.info("MARKET BUY placed: %s", result.get("data", {}))
        return result.get("data")
    log.error("MARKET BUY failed: %s", result)
    return None


_SELL_FORCE_CLEAR = "FORCE_CLEAR"   # sentinel: BTC gone, clear position without retry


async def market_sell_btc(session: aiohttp.ClientSession, btc_qty: float):
    """Place a MARKET SELL on BTC_IDR to liquidate BTC position.
    Returns dict on success, _SELL_FORCE_CLEAR when balance is gone (code 2202 / qty below min),
    or None on a retriable network/API error.
    """
    # Round down to stepSize 0.00001
    qty = math.floor(btc_qty / 0.00001) * 0.00001
    if qty < 0.00001:
        log.warning("BTC qty %.8f below minimum step — force-clearing phantom position", btc_qty)
        return _SELL_FORCE_CLEAR
    params = {
        "symbol":   SYMBOL,
        "side":     1,          # 1 = SELL
        "type":     2,          # 2 = MARKET
        "quantity": f"{qty:.5f}",
    }
    result = await toko_request(session, "POST", "/open/v1/orders", params)
    if result.get("code") == 0:
        log.info("MARKET SELL placed: %s", result.get("data", {}))
        return result.get("data")
    # Code 2202 = insufficient balance — BTC is already gone; stop retrying
    if result.get("code") == 2202:
        log.warning("MARKET SELL: insufficient balance (code 2202) — force-clearing phantom position")
        return _SELL_FORCE_CLEAR
    log.error("MARKET SELL failed: %s", result)
    return None


async def get_order_status(session: aiohttp.ClientSession, order_id: int) -> Optional[dict]:
    params = {"orderId": order_id}
    result = await toko_request(session, "GET", "/open/v1/orders/detail", params)
    if result.get("code") == 0:
        return result.get("data")
    return None


# ── Database ──────────────────────────────────────────────────────────────────

def get_db_pool() -> psycopg2.pool.ThreadedConnectionPool:
    for attempt in range(10):
        try:
            pool = psycopg2.pool.ThreadedConnectionPool(1, 5, dsn=DB_DSN)
            log.info("PostgreSQL connected.")
            return pool
        except Exception as exc:
            wait = 2 ** attempt
            log.warning("DB attempt %d/10 failed: %s. Waiting %ds...", attempt + 1, exc, wait)
            time.sleep(wait)
    raise RuntimeError("Could not connect to PostgreSQL after 10 attempts.")


def log_trade(
    pool: psycopg2.pool.ThreadedConnectionPool,
    direction: str,
    z_score: float,
    gross_pnl: float,
    fees_paid: float,
    net_pnl: float,
    roe_pct: float,
) -> None:
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO trade_telemetry
                  (timestamp, engine_id, asset_pair, trade_size_usdt_idr,
                   entry_signal_value, gross_pnl, fees_paid, net_pnl, trade_roe_pct)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    datetime.now(timezone.utc), "B", "BTC_IDR",
                    round(TRADE_SIZE_IDR, 4),
                    round(z_score, 8),
                    round(gross_pnl, 8),
                    round(fees_paid, 8),
                    round(net_pnl, 8),
                    round(roe_pct, 6),
                ),
            )
            conn.commit()
        log.info(
            "Trade closed | %s | z=%.4f | net_pnl=Rp%.0f | roe=%.4f%%",
            direction, z_score, net_pnl, roe_pct,
        )
    except Exception as exc:
        conn.rollback()
        log.error("DB insert failed: %s", exc)
    finally:
        pool.putconn(conn)


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_bid(raw: Optional[str]) -> Optional[float]:
    if not raw:
        return None
    try:
        data = json.loads(raw)
        val = data.get("bid") or data.get("ask")
        return float(val) if val else None
    except (KeyError, ValueError, json.JSONDecodeError):
        return None


def compute_zscore(ratio_window: deque, current_ratio: float) -> tuple[float, float, float]:
    n = len(ratio_window)
    if n < MIN_PERIODS:
        return math.nan, math.nan, math.nan
    mean = sum(ratio_window) / n
    variance = sum((r - mean) ** 2 for r in ratio_window) / n
    stddev = math.sqrt(variance)
    if stddev < 1e-10:
        return math.nan, mean, stddev
    z = (current_ratio - mean) / stddev
    if not math.isfinite(z):
        return math.nan, mean, stddev
    return z, mean, stddev


def compute_unrealized_pnl_idr(pos: Position, btc_idr_bid: float) -> float:
    """Unrealized P&L in IDR for a LONG BTC position."""
    return (btc_idr_bid - pos.entry_price) * pos.btc_qty


# ── Main loop ─────────────────────────────────────────────────────────────────

async def run(
    redis_client: aioredis.Redis,
    db_pool: psycopg2.pool.ThreadedConnectionPool,
) -> None:
    ratio_window:   deque = deque()
    timestamps:     deque = deque()
    position:       Optional[Position] = None
    entry_z:        float = 0.0
    last_heartbeat: float = 0.0

    if not TOKO_API_KEY or not TOKO_API_SECRET:
        log.error("TOKO_API_KEY / TOKO_API_SECRET not set. Cannot trade. Exiting.")
        return

    log.info(
        "Engine B LIVE trading activated. Symbol=%s, TradeSize=Rp%d, Wallet=Rp%d",
        SYMBOL, TRADE_SIZE_IDR, WALLET_IDR,
    )

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                btc_raw, eth_raw, btc_idr_raw = await asyncio.gather(
                    redis_client.get("toko:btc_usdt:ticker"),
                    redis_client.get("toko:eth_usdt:ticker"),
                    redis_client.get("toko:btc_idr:ticker"),
                )
                btc_price     = parse_bid(btc_raw)
                eth_price     = parse_bid(eth_raw)
                btc_idr_bid   = parse_bid(btc_idr_raw)

                if btc_price is None or eth_price is None or eth_price == 0:
                    await asyncio.sleep(POLL_INTERVAL_SECS)
                    continue

                ratio = btc_price / eth_price
                now   = time.monotonic()

                # Maintain 15-min rolling window
                ratio_window.append(ratio)
                timestamps.append(now)
                while timestamps and (now - timestamps[0]) > WINDOW_SECONDS:
                    timestamps.popleft()
                    ratio_window.popleft()

                z_score, mean, _stddev = compute_zscore(ratio_window, ratio)

                # ── Position management ─────────────────────────────────────────
                if position is not None and btc_idr_bid:
                    unrealized = compute_unrealized_pnl_idr(position, btc_idr_bid)
                    elapsed    = now - position.entry_time

                    close_reason: Optional[str] = None
                    if unrealized >= position.tp_idr:
                        close_reason = "TP"
                    elif unrealized <= -position.sl_idr:
                        close_reason = "SL"
                    elif elapsed >= MAX_DURATION_SECS:
                        close_reason = "TIMEOUT"

                    if close_reason:
                        sell_result = await market_sell_btc(session, position.btc_qty)
                        if sell_result == _SELL_FORCE_CLEAR:
                            # BTC is gone (balance 0 or qty below min) — write zero PnL and clear
                            fees_paid = TRADE_SIZE_IDR * ROUND_TRIP_FEE
                            log_trade(db_pool, position.direction, entry_z,
                                      0.0, fees_paid, -fees_paid,
                                      (-fees_paid / WALLET_IDR) * 100.0)
                            log.warning("Position force-cleared (%s) — BTC balance gone", close_reason)
                            position = None
                        elif sell_result is not None:
                            gross_pnl = unrealized
                            fees_paid = TRADE_SIZE_IDR * ROUND_TRIP_FEE
                            net_pnl   = gross_pnl - fees_paid
                            roe_pct   = (net_pnl / WALLET_IDR) * 100.0
                            log_trade(db_pool, position.direction, entry_z,
                                      gross_pnl, fees_paid, net_pnl, roe_pct)
                            log.info("Position closed (%s) | unrealized=Rp%.0f", close_reason, unrealized)
                            position = None
                        else:
                            log.error("SELL order failed (network/API) — will retry next tick.")

                # ── Entry logic (LONG only) ─────────────────────────────────────
                if position is None and not math.isnan(z_score) and mean > 0 and btc_idr_bid:
                    if z_score < -ZSCORE_THRESHOLD:
                        buy_result = await market_buy_btc(session, TRADE_SIZE_IDR)
                        if buy_result is not None:
                            # Derive BTC qty from executed amount (market fill)
                            # If API doesn't return filled qty, estimate from price
                            executed_qty = float(buy_result.get("executedQty", 0) or 0)
                            if executed_qty == 0:
                                executed_qty = TRADE_SIZE_IDR / btc_idr_bid
                            executed_qty = round(executed_qty, 5)

                            # TP/SL based on fixed % of BTC_IDR notional value,
                            # not the tiny ratio-deviation (which produced Rp11-84 SL,
                            # hit within minutes by normal BTC volatility).
                            # 1.5% TP and 0.75% SL on the BTC notional gives room
                            # for mean-reversion to develop (typically 15-60 min).
                            btc_notional = executed_qty * btc_idr_bid
                            tp_idr = btc_notional * 0.015    # 1.5% of notional
                            sl_idr = btc_notional * 0.0075   # 0.75% of notional

                            position = Position(
                                direction="LONG",
                                entry_ratio=ratio,
                                entry_price=btc_idr_bid,
                                entry_time=now,
                                btc_qty=executed_qty,
                                tp_idr=tp_idr,
                                sl_idr=sl_idr,
                                order_id=buy_result.get("orderId"),
                            )
                            entry_z = z_score
                            log.info(
                                "Position opened LONG | z=%.4f | ratio=%.4f | btc=%.5f | TP=Rp%.0f | SL=Rp%.0f",
                                z_score, ratio, executed_qty, tp_idr, sl_idr,
                            )

            except Exception as exc:
                log.error("Engine B error: %s", exc)

            # Periodic heartbeat every 60s (outside try/except)
            now_wall = time.monotonic()
            if now_wall - last_heartbeat >= 60.0:
                last_heartbeat = now_wall
                _z = z_score if 'z_score' in dir() and math.isfinite(z_score) else 0.0
                _r = ratio if 'ratio' in dir() else 0.0
                log.info(
                    "Heartbeat | window=%d/%d | z=%.4f | ratio=%.4f | position=%s",
                    len(ratio_window), MIN_PERIODS, _z, _r,
                    position.direction if position else "none",
                )

            await asyncio.sleep(POLL_INTERVAL_SECS)


async def main() -> None:
    redis_client = None
    for attempt in range(10):
        try:
            redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
            await redis_client.ping()
            log.info("Redis connected.")
            break
        except Exception as exc:
            wait = 2 ** attempt
            log.warning("Redis attempt %d/10 failed: %s. Waiting %ds...", attempt + 1, exc, wait)
            await asyncio.sleep(wait)
    if redis_client is None:
        raise RuntimeError("Could not connect to Redis after 10 attempts.")

    db_pool = get_db_pool()
    await run(redis_client, db_pool)


if __name__ == "__main__":
    asyncio.run(main())
