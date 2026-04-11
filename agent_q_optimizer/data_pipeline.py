"""
Data Pipeline — PostgreSQL trade_telemetry aggregator.

Called by both the Oracle (5-min window) and the Tactical Tuner (15-min window).
Returns a dense, purely quantitative one-line string for LLM context injection.

Aggregated fields (engine D, per symbol, per window):
  Total Trades        : COUNT(*) in window
  Win Rate            : % trades with net_pnl > 0
  Avg Spread Captured : AVG(entry_signal_value)  — OBI/signal at fill
  TFI Volatility      : STDDEV(entry_signal_value) — proxy for flow-imbalance stddev
  Price Variance      : VARIANCE(net_pnl / trade_size_idr) — normalized PnL dispersion
  Net PnL             : SUM(net_pnl) in IDR
"""
import os
import logging
import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)

DB_DSN = os.getenv("DB_DSN", "postgresql://mammon:mammon@postgres:5432/mammon")

_QUERY = """
    SELECT
        COUNT(*)                                                                 AS total_trades,
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE net_pnl > 0)
            / NULLIF(COUNT(*), 0),
        2)                                                                       AS win_rate_pct,
        ROUND(AVG(entry_signal_value)::numeric,                                6) AS avg_entry_signal,
        ROUND(COALESCE(STDDEV(entry_signal_value), 0)::numeric,               8) AS tfi_volatility,
        ROUND(COALESCE(VARIANCE(net_pnl / NULLIF(trade_size_idr, 0)), 0)::numeric, 10) AS price_variance,
        ROUND(COALESCE(SUM(net_pnl), 0)::numeric,                             4) AS net_pnl
    FROM  trade_telemetry
    WHERE engine_id  = 'D'
      AND asset_pair = %s
      AND timestamp  >= NOW() - INTERVAL %s
"""


def fetch_telemetry_window(symbol: str, window_minutes: int = 15) -> str:
    """
    Returns a dense one-line stats string for `symbol` over the last
    `window_minutes` minutes. Falls back to zero-baseline on empty data or DB error.
    """
    interval = f"{window_minutes} minutes"
    try:
        conn = psycopg2.connect(DB_DSN, connect_timeout=5)
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(_QUERY, (symbol, interval))
            row = cur.fetchone()
        conn.close()

        if row and row["total_trades"] and int(row["total_trades"]) > 0:
            return (
                f"SYMBOL={symbol} | WINDOW={window_minutes}min | "
                f"TOTAL_TRADES={int(row['total_trades'])} | "
                f"WIN_RATE={float(row['win_rate_pct'] or 0):.2f}% | "
                f"AVG_SPREAD_CAPTURED={float(row['avg_entry_signal'] or 0):.6f} | "
                f"TFI_VOLATILITY_STDDEV={float(row['tfi_volatility'] or 0):.8f} | "
                f"PRICE_VARIANCE={float(row['price_variance'] or 0):.10f} | "
                f"NET_PNL={float(row['net_pnl'] or 0):.4f}"
            )

        log.warning("[%s] No trade_telemetry rows in last %d min — zero baseline.", symbol, window_minutes)
        return _zero_baseline(symbol, window_minutes)

    except Exception as exc:
        log.error("[%s] DB query failed: %s", symbol, exc)
        return _zero_baseline(symbol, window_minutes)


def _zero_baseline(symbol: str, window_minutes: int) -> str:
    return (
        f"SYMBOL={symbol} | WINDOW={window_minutes}min | TOTAL_TRADES=0 | WIN_RATE=0.00% | "
        f"AVG_SPREAD_CAPTURED=0.000000 | TFI_VOLATILITY_STDDEV=0.00000000 | "
        f"PRICE_VARIANCE=0.0000000000 | NET_PNL=0.0000"
    )
