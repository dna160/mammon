"""
Engine A: Spatial Arbitrage (Cross-Exchange Simulation)

Strategy: Exploit spread between Indodax bid and Tokocrypto ask for SOL/IDR and PEPE/IDR.

Math:
  Spread     = ((Indo_Bid - Toko_Ask) / Toko_Ask) * 100
  Gross_PnL  = TRADE_SIZE_IDR * (Spread / 100)
  Fees_Paid  = TRADE_SIZE_IDR * FRICTION
  Net_PnL    = Gross_PnL - Fees_Paid
  ROE_Pct    = (Net_PnL / WALLET_IDR) * 100

Entry: Spread >= TARGET_SPREAD (1.07%) AND both ToB volumes >= MIN_VOLUME_IDR
No close logic — trade completes immediately upon execution (simultaneous buy/sell).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.pool
import redis.asyncio as aioredis

from config import (
    DB_DSN, FRICTION, MIN_VOLUME_IDR, POLL_INTERVAL_MS,
    REDIS_URL, TARGET_SPREAD, TRADE_SIZE_IDR, WALLET_IDR,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ENGINE_A] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


# ── Database ─────────────────────────────────────────────────────────────────

def get_db_pool() -> psycopg2.pool.ThreadedConnectionPool:
    """Connect to PostgreSQL with exponential backoff (10 attempts)."""
    for attempt in range(10):
        try:
            pool = psycopg2.pool.ThreadedConnectionPool(minconn=1, maxconn=5, dsn=DB_DSN)
            log.info("PostgreSQL connected.")
            return pool
        except Exception as exc:
            wait = 2 ** attempt
            log.warning("DB attempt %d/10 failed: %s. Retrying in %ds...", attempt + 1, exc, wait)
            time.sleep(wait)
    raise RuntimeError("Could not connect to PostgreSQL after 10 attempts.")


def log_trade(
    pool: psycopg2.pool.ThreadedConnectionPool,
    asset_pair: str,
    spread: float,
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
                    datetime.now(timezone.utc),
                    "A",
                    asset_pair,
                    float(TRADE_SIZE_IDR),
                    round(spread, 8),
                    round(gross_pnl, 8),
                    round(fees_paid, 8),
                    round(net_pnl, 8),
                    round(roe_pct, 6),
                ),
            )
            conn.commit()
        log.info(
            "Trade | %s | spread=%.4f%% | net_pnl=%.2f IDR | roe=%.4f%%",
            asset_pair, spread, net_pnl, roe_pct,
        )
    except Exception as exc:
        conn.rollback()
        log.error("DB insert failed: %s", exc)
    finally:
        pool.putconn(conn)


# ── Parsing ───────────────────────────────────────────────────────────────────

def parse_ticker(raw: Optional[str]) -> Optional[tuple[float, float, float]]:
    """Parse ticker JSON → (ask, bid, volume). Returns None on any error."""
    if not raw:
        return None
    try:
        data = json.loads(raw)
        return float(data["ask"]), float(data["bid"]), float(data["volume"])
    except (KeyError, ValueError, json.JSONDecodeError):
        return None


# ── Spread logic ──────────────────────────────────────────────────────────────

def compute_spread(toko_ask: float, indo_bid: float) -> float:
    """Spread = ((Indo_Bid - Toko_Ask) / Toko_Ask) * 100"""
    if toko_ask <= 0:
        return 0.0
    return ((indo_bid - toko_ask) / toko_ask) * 100.0


def should_trade(spread: float, toko_vol: float, indo_vol: float) -> bool:
    # Only check Tokocrypto volume — that is the execution side.
    # Indodax is used for price discovery only; we cannot execute there.
    return (
        spread >= TARGET_SPREAD
        and toko_vol >= MIN_VOLUME_IDR
    )


# ── Main loop ─────────────────────────────────────────────────────────────────

async def run(
    redis_client: aioredis.Redis,
    db_pool: psycopg2.pool.ThreadedConnectionPool,
) -> None:
    poll_s = POLL_INTERVAL_MS / 1000.0
    pairs = [
        ("toko:sol_idr:ticker", "indo:sol_idr:ask",  "SOL/IDR"),
        ("toko:btc_idr:ticker", "indo:btc_idr:ask",  "BTC/IDR"),
    ]
    log.info("Engine A started. Polling every %dms.", POLL_INTERVAL_MS)

    while True:
        for toko_key, indo_key, asset_name in pairs:
            try:
                toko_raw, indo_raw = await asyncio.gather(
                    redis_client.get(toko_key),
                    redis_client.get(indo_key),
                )
                toko = parse_ticker(toko_raw)
                indo = parse_ticker(indo_raw)
                if toko is None or indo is None:
                    continue

                toko_ask, _toko_bid, toko_vol = toko
                _indo_ask, indo_bid, indo_vol = indo

                spread = compute_spread(toko_ask, indo_bid)

                if should_trade(spread, toko_vol, indo_vol):
                    gross_pnl = TRADE_SIZE_IDR * (spread / 100.0)
                    fees_paid = TRADE_SIZE_IDR * FRICTION
                    net_pnl   = gross_pnl - fees_paid
                    roe_pct   = (net_pnl / WALLET_IDR) * 100.0
                    log_trade(db_pool, asset_name, spread, gross_pnl, fees_paid, net_pnl, roe_pct)

            except Exception as exc:
                log.error("Error processing %s: %s", asset_name, exc)

        await asyncio.sleep(poll_s)


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
