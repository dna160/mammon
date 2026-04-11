"""
Data Pipeline — Hybrid Redis (live physics) + PostgreSQL (execution history).

PRD: Agent Q Blindspot & Data Starvation Fix.

The previous version read ONLY from PostgreSQL, causing a death spiral:
  no trades → DEAD_ZONE signal → wider spreads → no trades → repeat.

New architecture (two sources):
  1. Redis telemetry:engine_d:{SYMBOL}  — live variance + TFI from Rust (every 1s)
     This gives real market physics even when the bot has 0 fills.
  2. PostgreSQL trade_telemetry          — PnL and win-rate history (every fill)

The combined string is injected into the LLM context window.
Key rule: if 15m_total_trades == 0, DO NOT default to DEAD_ZONE.
           The live_market_variance is the ground truth.
"""
import os
import json
import logging
import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)

DB_DSN    = os.getenv("DB_DSN",    "postgresql://mammon:mammon@postgres:5432/mammon")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")

# Lazy Redis client — shared with redis_bridge to avoid double-connection overhead.
_redis_client = None


def _get_redis():
    global _redis_client
    if _redis_client is None:
        import redis as redis_lib
        _redis_client = redis_lib.from_url(REDIS_URL, decode_responses=True)
    return _redis_client


_PG_QUERY = """
    SELECT
        COUNT(*)                                                                 AS total_trades,
        ROUND(COALESCE(SUM(net_pnl), 0)::numeric, 4)                            AS net_pnl
    FROM  trade_telemetry
    WHERE engine_id  = 'D'
      AND asset_pair = %s
      AND timestamp  >= NOW() - INTERVAL %s
"""


def fetch_telemetry_window(symbol: str, window_minutes: int = 15) -> str:
    """
    Returns a dense one-line stats string for `symbol` over the last
    `window_minutes` minutes, combining:
      - Live market physics (variance, TFI) from Redis
      - Historical execution stats (trades, PnL) from PostgreSQL

    Format:
      SYMBOL=DOGEFDUSD | WINDOW=15min | LIVE_MARKET_VARIANCE=0.0000001234 |
      LIVE_TFI_ORDER_FLOW=47234.5000 | TOTAL_TRADES=8 | NET_PNL=0.2400
    """
    # ── 1. Live market physics from Redis (1s heartbeat from Rust) ────────────
    live_variance: float = 0.0
    live_tfi:      float = 0.0

    try:
        r = _get_redis()
        raw = r.get(f"telemetry:engine_d:{symbol.upper()}")
        if raw:
            data = json.loads(raw)
            live_variance = float(data.get("variance", 0.0))
            live_tfi      = float(data.get("tfi",      0.0))
        else:
            log.warning("[%s] No Redis telemetry key yet — using 0 baselines.", symbol)
    except Exception as e:
        log.error("[%s] Redis telemetry read failed: %s", symbol, e)

    # ── 2. Historical execution stats from PostgreSQL ─────────────────────────
    total_trades: int   = 0
    net_pnl:      float = 0.0

    try:
        conn = psycopg2.connect(DB_DSN, connect_timeout=5)
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(_PG_QUERY, (symbol, f"{window_minutes} minutes"))
            row = cur.fetchone()
        conn.close()
        if row and row["total_trades"]:
            total_trades = int(row["total_trades"])
            net_pnl      = float(row["net_pnl"] or 0.0)
    except Exception as e:
        log.error("[%s] PostgreSQL telemetry read failed: %s", symbol, e)

    # ── 3. Combine into LLM-ready one-liner ───────────────────────────────────
    return (
        f"SYMBOL={symbol} | WINDOW={window_minutes}min | "
        f"LIVE_MARKET_VARIANCE={live_variance:.10f} | "
        f"LIVE_TFI_ORDER_FLOW={live_tfi:.4f} | "
        f"TOTAL_TRADES={total_trades} | "
        f"NET_PNL={net_pnl:.4f}"
    )
