"""
Engine C: Microstructure Trading (Maker-Only) — Tick-Level OFI via Redis Pub/Sub

Data flow:
  Agent 0 subscribes to btcusdt@depth (no time qualifier — real-time diff stream).
  Every individual order-book change fires immediately and is published to the Redis
  channel  toko:btc_usdt:lob:tick.  Engine C subscribes to that channel and processes
  every tick as it arrives — no polling, no fixed sleep interval, no missed ticks.

Signal math:
  OFI_t      = BidFlow – AskFlow  (per tick, computed on BTC/USDT LOB)
  EMA_OFI    = time-decay EMA, τ = 5 min  (independent of tick rate)
               decay = exp(−Δt / τ),  new_ema = ofi·(1−decay) + prev·decay
  VPIN       = rolling toxic-flow filter over last WINDOW_TICKS ticks
  Target     = 0.5 × EMA_OFI − 0.2 × inventory × σ²

Execution (Maker-only — real LIMIT orders on Tokocrypto BTC_IDR, IDR wallet):
  Target ≥ TARGET_THRESHOLD  AND  inventory == 0 → LIMIT BUY  at BTC/IDR best bid
  Cancel BUY if target drops below threshold before fill
  Exit: LIMIT SELL when target crosses zero (target ≤ 0 after being > 0)
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
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import numpy as np
import psycopg2
import psycopg2.pool
import redis.asyncio as aioredis

from math_lib import compute_ofi, compute_vpin, compute_hjb_target

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ENGINE_C] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
DB_DSN    = os.getenv("DB_DSN",    "postgresql://mammon:mammon@localhost:5432/mammon")

TOKO_API_KEY    = os.getenv("TOKO_API_KEY", "")
TOKO_API_SECRET = os.getenv("TOKO_API_SECRET", "")
TOKO_BASE       = "https://www.tokocrypto.com"

# ── Signal parameters ─────────────────────────────────────────────────────────
#
# EMA uses a TIME-DECAY formula (not fixed alpha) so it is independent of tick rate.
# τ = 300s (5-min time constant).  At τ seconds of sustained +3 BTC/tick OFI:
#   ema reaches 63% of steady-state ≈ 1.9 BTC
#   target = 0.5 × 1.9 = 0.95
#
# TARGET_THRESHOLD = 0.3 → requires ~75–90 s of sustained directional buy flow.
EMA_TAU_SECS        = 300.0     # 5-minute EMA time constant (seconds)
TARGET_THRESHOLD    = 0.3       # HJB entry threshold (BTC units)
VPIN_THRESHOLD      = 0.75
VPIN_PAUSE_SECS     = 300
WINDOW_TICKS        = 3_000     # rolling deque depth for VPIN & σ² (~5 min at 10Hz)

# ── Execution parameters ──────────────────────────────────────────────────────
ORDER_POLL_SECS     = 2.0       # how often to poll open order fill status via REST
ORDER_SIZE_BTC      = 0.0001    # BTC per order (~Rp123,000 at 1.23B IDR/BTC)
WALLET_IDR          = 150_000   # IDR allocated (covers one order + fees)
SYMBOL              = "BTC_IDR"
TICK_SIZE           = 1.0       # IDR price tick size
HEARTBEAT_INTERVAL  = 10        # seconds between heartbeat log lines


@dataclass
class Order:
    side:     str    # "BUY" or "SELL"
    price:    float
    size:     float = ORDER_SIZE_BTC
    filled:   bool  = False
    order_id: Optional[int] = None


# ── Time-decay EMA ────────────────────────────────────────────────────────────

def time_decay_ema(prev_ema: float, new_value: float, dt_secs: float) -> float:
    """
    Exponential moving average with a fixed time constant τ = EMA_TAU_SECS.
    Independent of tick rate — correct whether ticks arrive at 5 Hz or 500 Hz.

      decay   = exp(−dt / τ)
      new_ema = new_value × (1 − decay) + prev_ema × decay
    """
    if dt_secs <= 0.0:
        return prev_ema
    decay = math.exp(-dt_secs / EMA_TAU_SECS)
    return new_value * (1.0 - decay) + prev_ema * decay


# ── Tokocrypto REST client ────────────────────────────────────────────────────

def _sign(params: dict) -> str:
    qs = urllib.parse.urlencode(params)
    return hmac.new(TOKO_API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()


async def toko_request(session: aiohttp.ClientSession, method: str, path: str, params: dict) -> dict:
    params["timestamp"] = int(time.time() * 1000)
    params["signature"] = _sign(params)
    url = f"{TOKO_BASE}{path}"
    headers = {"X-MBX-APIKEY": TOKO_API_KEY}
    try:
        if method == "GET":
            async with session.get(url, params=params, headers=headers,
                                   timeout=aiohttp.ClientTimeout(total=8)) as r:
                return await r.json()
        else:
            async with session.post(url, params=params, headers=headers,
                                    timeout=aiohttp.ClientTimeout(total=8)) as r:
                return await r.json()
    except Exception as exc:
        log.error("Toko REST %s %s failed: %s", method, path, exc)
        return {"code": -1, "msg": str(exc)}


def _round_price(price: float) -> int:
    return int(round(price / TICK_SIZE) * TICK_SIZE)


async def place_limit_order(session: aiohttp.ClientSession, side: str, price: float) -> Optional[int]:
    params = {
        "symbol":      SYMBOL,
        "side":        0 if side == "BUY" else 1,
        "type":        1,   # LIMIT
        "timeInForce": 1,   # GTC
        "quantity":    f"{ORDER_SIZE_BTC:.5f}",
        "price":       str(_round_price(price)),
    }
    result = await toko_request(session, "POST", "/open/v1/orders", params)
    if result.get("code") == 0:
        oid = result["data"].get("orderId")
        log.info("LIMIT %s placed @ Rp%d | orderId=%s", side, _round_price(price), oid)
        return oid
    log.error("LIMIT %s failed: %s", side, result)
    return None


async def cancel_order(session: aiohttp.ClientSession, order_id: int) -> bool:
    result = await toko_request(session, "POST", "/open/v1/orders/cancel", {"orderId": order_id})
    if result.get("code") == 0:
        log.info("Order %d cancelled.", order_id)
        return True
    log.warning("Cancel order %d failed: %s", order_id, result)
    return False


async def poll_order_fill(session: aiohttp.ClientSession, order_id: int) -> Optional[str]:
    result = await toko_request(session, "GET", "/open/v1/orders/detail", {"orderId": order_id})
    if result.get("code") == 0:
        return result["data"].get("status")
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


def log_event(pool, event_type, target, inventory, gross_pnl, fees_paid, net_pnl, roe_pct, btc_price_idr=0.0):
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
                (datetime.now(timezone.utc), "C", "BTC_IDR",
                 round(ORDER_SIZE_BTC * btc_price_idr, 4),
                 round(target, 8), round(gross_pnl, 8), round(fees_paid, 8),
                 round(net_pnl, 8), round(roe_pct, 6)),
            )
            conn.commit()
        log.info("Event %s | target=%.4f | inv=%.0f | net_pnl=Rp%.0f",
                 event_type, target, inventory, net_pnl)
    except Exception as exc:
        conn.rollback()
        log.error("DB insert failed: %s", exc)
    finally:
        pool.putconn(conn)


# ── LOB parsing ───────────────────────────────────────────────────────────────

def parse_lob(raw: Optional[str]) -> Optional[dict]:
    if not raw:
        return None
    try:
        data = json.loads(raw)
        if "bids" not in data or "asks" not in data:
            return None
        if not data["bids"] or not data["asks"]:
            return None
        return {
            "bids": [[float(x) for x in level] for level in data["bids"]],
            "asks": [[float(x) for x in level] for level in data["asks"]],
        }
    except (KeyError, ValueError, json.JSONDecodeError):
        return None


# ── Main loop (Pub/Sub — event-driven, processes every tick) ──────────────────

async def run(
    redis_client: aioredis.Redis,
    db_pool: psycopg2.pool.ThreadedConnectionPool,
) -> None:

    if not TOKO_API_KEY or not TOKO_API_SECRET:
        log.error("TOKO_API_KEY / TOKO_API_SECRET not set. Cannot trade. Exiting.")
        return

    # ── Subscribe to the BTC/USDT tick channel ────────────────────────────────
    # Agent 0 publishes here on every individual order-book change from
    # the btcusdt@depth real-time diff stream — no batching, no sleep.
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("toko:btc_usdt:lob:tick")
    log.info(
        "Engine C LIVE | subscribed to toko:btc_usdt:lob:tick | "
        "Symbol=%s OrderSize=%.4f BTC Wallet=Rp%d τ=%.0fs threshold=%.3f",
        SYMBOL, ORDER_SIZE_BTC, WALLET_IDR, EMA_TAU_SECS, TARGET_THRESHOLD,
    )

    # ── State ─────────────────────────────────────────────────────────────────
    prev_lob:       Optional[dict]  = None
    ema_ofi:        float           = 0.0
    inventory:      float           = 0.0
    open_order:     Optional[Order] = None
    entry_price:    float           = 0.0
    prev_target:    float           = 0.0
    last_order_poll: float          = 0.0
    last_order_fail: float          = 0.0
    last_heartbeat:  float          = 0.0
    last_tick_wall:  Optional[float] = None   # wall clock of last processed tick
    tick_count:      int            = 0

    buy_vol_window:   deque = deque(maxlen=WINDOW_TICKS)
    total_vol_window: deque = deque(maxlen=WINDOW_TICKS)
    price_window:     deque = deque(maxlen=WINDOW_TICKS)

    async with aiohttp.ClientSession() as session:

        async for message in pubsub.listen():
            # ── Filter pub/sub control messages ──────────────────────────────
            if message["type"] != "message":
                continue

            raw_lob = message["data"]
            if isinstance(raw_lob, bytes):
                raw_lob = raw_lob.decode("utf-8")

            try:
                lob = parse_lob(raw_lob)
                if lob is None:
                    continue

                # ── Extract BTC/USDT signal levels ────────────────────────────
                best_bid_px  = lob["bids"][0][0]
                best_bid_vol = lob["bids"][0][1]
                best_ask_px  = lob["asks"][0][0]
                best_ask_vol = lob["asks"][0][1]
                mid_price    = (best_bid_px + best_ask_px) / 2.0

                # ── Get BTC/IDR execution prices (non-blocking GET) ───────────
                exec_bid_idr: Optional[float] = None
                exec_ask_idr: Optional[float] = None
                raw_idr = await redis_client.get("toko:btc_idr:ticker")
                if raw_idr:
                    try:
                        d = json.loads(raw_idr)
                        exec_bid_idr = float(d.get("bid") or 0) or None
                        exec_ask_idr = float(d.get("ask") or 0) or None
                    except (ValueError, json.JSONDecodeError):
                        pass

                # ── Update rolling windows ────────────────────────────────────
                price_window.append(mid_price)
                total_vol = best_bid_vol + best_ask_vol
                total_vol_window.append(max(total_vol, 1e-10))
                buy_vol_window.append(best_ask_vol)

                sigma_sq = float(np.var(np.array(list(price_window), dtype=np.float64))) \
                           if len(price_window) >= 2 else 1e-4

                # ── OFI + time-decay EMA ──────────────────────────────────────
                now_wall = time.monotonic()
                if prev_lob is not None and last_tick_wall is not None:
                    ofi = compute_ofi(
                        prev_lob["bids"][0][0], prev_lob["bids"][0][1],
                        best_bid_px,            best_bid_vol,
                        prev_lob["asks"][0][0], prev_lob["asks"][0][1],
                        best_ask_px,            best_ask_vol,
                    )
                    dt = now_wall - last_tick_wall
                    ema_ofi = time_decay_ema(ema_ofi, ofi, dt)

                last_tick_wall = now_wall
                prev_lob = lob
                tick_count += 1

                # ── VPIN toxic-flow filter ────────────────────────────────────
                if len(buy_vol_window) >= 2:
                    vpin = compute_vpin(
                        np.array(list(buy_vol_window),   dtype=np.float64),
                        np.array(list(total_vol_window), dtype=np.float64),
                    )
                else:
                    vpin = 0.0

                if vpin > VPIN_THRESHOLD:
                    if open_order is not None and not open_order.filled and open_order.order_id:
                        log.warning("VPIN=%.4f — cancelling open order, pausing %ds.", vpin, VPIN_PAUSE_SECS)
                        await cancel_order(session, open_order.order_id)
                        open_order = None
                    else:
                        log.warning("VPIN=%.4f — pausing %ds.", vpin, VPIN_PAUSE_SECS)
                    await asyncio.sleep(VPIN_PAUSE_SECS)
                    continue

                # ── HJB target ────────────────────────────────────────────────
                target = compute_hjb_target(ema_ofi, inventory, sigma_sq)

                # ── Poll open order fill status (throttled) ───────────────────
                now_t = time.monotonic()
                if (open_order is not None and not open_order.filled
                        and open_order.order_id is not None
                        and (now_t - last_order_poll) >= ORDER_POLL_SECS):
                    last_order_poll = now_t
                    status = await poll_order_fill(session, open_order.order_id)
                    if status == "FILLED":
                        open_order.filled = True
                        inventory   = 1.0 if open_order.side == "BUY" else -1.0
                        entry_price = open_order.price
                        log.info("%s order %d FILLED at Rp%.0f",
                                 open_order.side, open_order.order_id, open_order.price)
                    elif status in ("CANCELED", "EXPIRED", "REJECTED"):
                        log.info("Order %d status=%s — clearing.", open_order.order_id, status)
                        open_order = None

                # ── Cancel unfilled BUY if signal reversed ────────────────────
                if (open_order is not None and not open_order.filled
                        and open_order.side == "BUY" and target < TARGET_THRESHOLD
                        and open_order.order_id is not None):
                    log.info("BUY order cancelled (target=%.4f dropped below threshold).", target)
                    await cancel_order(session, open_order.order_id)
                    open_order = None

                # ── Exit: close long when HJB target crosses zero ─────────────
                if open_order is not None and open_order.filled and inventory != 0:
                    crossed_zero = (
                        (prev_target > 0 and target <= 0)
                        or (prev_target < 0 and target >= 0)
                    )
                    if crossed_zero and exec_ask_idr and exec_bid_idr:
                        if inventory > 0:
                            close_px, close_side = exec_ask_idr, "SELL"
                            gross_pnl = (close_px - entry_price) * open_order.size
                        else:
                            close_px, close_side = exec_bid_idr, "BUY"
                            gross_pnl = (entry_price - close_px) * open_order.size

                        close_oid = await place_limit_order(session, close_side, close_px)
                        if close_oid is not None:
                            fees_paid = close_px * open_order.size * 0.0015
                            net_pnl   = gross_pnl - fees_paid
                            log_event(db_pool, "CLOSE", target, inventory,
                                      gross_pnl, fees_paid, net_pnl,
                                      (net_pnl / WALLET_IDR) * 100.0, close_px)
                            open_order  = None
                            inventory   = 0.0
                            entry_price = 0.0

                # ── Entry: LIMIT BUY when flat and OFI signal is bullish ──────
                now_mono = time.monotonic()
                if (open_order is None and inventory == 0
                        and exec_bid_idr is not None
                        and (now_mono - last_order_fail) > 60.0
                        and target >= TARGET_THRESHOLD):
                    oid = await place_limit_order(session, "BUY", exec_bid_idr)
                    if oid is not None:
                        open_order      = Order(side="BUY", price=exec_bid_idr, order_id=oid)
                        last_order_poll = time.monotonic()
                    else:
                        last_order_fail = time.monotonic()

                prev_target = target

                # ── Periodic heartbeat ────────────────────────────────────────
                if now_wall - last_heartbeat >= HEARTBEAT_INTERVAL:
                    last_heartbeat = now_wall
                    log.info(
                        "Heartbeat | ticks=%d | ema_ofi=%.6f | target=%.6f | "
                        "threshold=%.4f | inv=%.0f | order=%s",
                        tick_count, ema_ofi, target, TARGET_THRESHOLD,
                        inventory, open_order.side if open_order else "none",
                    )

            except Exception as exc:
                log.error("Engine C tick error: %s", exc)


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
