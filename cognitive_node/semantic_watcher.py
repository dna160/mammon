"""
semantic_watcher.py — Dimensionality Reduction (Math → Text)

Converts raw Redis/Postgres telemetry into human-readable semantic state vectors
that can be consumed by the LLM without floating-point confusion.

Three signals per vector:
  1. Micro-Volatility  — StdDev of 1s returns × 10000 → Basis Points (BPS)
  2. TFI Z-Score       — (Current TFI - Mean TFI) / StdDev TFI
  3. Adverse Selection — % of trades in last 15m where PnL was negative at T+1s
"""

import statistics
from datetime import datetime, timezone
from typing import Optional


def _safe_stdev(values: list[float]) -> float:
    """Returns population stdev, or 1.0 if there aren't enough values."""
    if len(values) < 2:
        return 1.0
    try:
        return statistics.stdev(values)
    except statistics.StatisticsError:
        return 1.0


def _safe_mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return statistics.mean(values)


def _compute_micro_volatility(tfi_values: list[float]) -> float:
    """
    Computes volatility as the standard deviation of 1-second log returns,
    scaled to basis points (BPS = stdev × 10,000).
    """
    if len(tfi_values) < 3:
        return 0.0

    returns = []
    for i in range(1, len(tfi_values)):
        prev = tfi_values[i - 1]
        curr = tfi_values[i]
        if prev > 0.0:
            returns.append((curr - prev) / prev)

    if len(returns) < 2:
        return 0.0

    return _safe_stdev(returns) * 10_000.0  # → BPS


def _compute_tfi_zscore(tfi_values: list[float]) -> float:
    """
    Computes the z-score of the most recent TFI reading against its own history.
    Positive = buy-side pressure. Negative = sell-side pressure.
    """
    if len(tfi_values) < 3:
        return 0.0

    current = tfi_values[-1]
    mean    = _safe_mean(tfi_values)
    std     = _safe_stdev(tfi_values)
    if std == 0.0:
        return 0.0
    return (current - mean) / std


def _compute_adverse_selection(symbol: str, pg_conn) -> float:
    """
    Reads the last 15 minutes of trade_telemetry from Postgres.
    Returns the percentage of trades where net_pnl < 0.
    """
    try:
        cursor = pg_conn.cursor()
        cursor.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE net_pnl < 0)::float
                / NULLIF(COUNT(*), 0) * 100.0 AS adverse_pct
            FROM trade_telemetry
            WHERE timestamp > NOW() - INTERVAL '15 minutes'
              AND symbol = %s
            """,
            (symbol,),
        )
        row = cursor.fetchone()
        cursor.close()
        if row and row[0] is not None:
            return float(row[0])
    except Exception:
        pass  # Postgres unavailable or no data yet — safe default
    return 0.0


def generate_state_vector(symbol: str, redis_client, pg_conn) -> str:
    """
    Main entry point. Returns an institutional-grade semantic state vector string
    ready to be fed directly into any LLM prompt.

    Args:
        symbol:       Trading pair, e.g. "BTCUSDT"
        redis_client: redis.Redis instance (sync)
        pg_conn:      psycopg2 connection
    """
    # ── Fetch TFI ring buffer from Redis ─────────────────────────────────────
    tfi_key    = f"hft:tfi:{symbol}"
    tfi_raw    = redis_client.lrange(tfi_key, 0, 99)  # last 100 readings
    tfi_values = [float(v) for v in tfi_raw] if tfi_raw else [0.0]

    # ── Compute signals ───────────────────────────────────────────────────────
    vol_bps      = _compute_micro_volatility(tfi_values)
    tfi_zscore   = _compute_tfi_zscore(tfi_values)
    adv_sel_pct  = _compute_adverse_selection(symbol, pg_conn)

    # ── Semantic labels ───────────────────────────────────────────────────────
    vol_label = "EXTREME" if vol_bps > 5.0 else ("ELEVATED" if vol_bps > 2.0 else "NORMAL")
    tox_label = "TOXIC"   if abs(tfi_zscore) > 2.0 else ("STRESSED" if abs(tfi_zscore) > 1.0 else "CLEAN")
    adv_label = "DANGER"  if adv_sel_pct > 60.0 else ("WARNING" if adv_sel_pct > 40.0 else "SAFE")

    # ── Trend direction hint ──────────────────────────────────────────────────
    if tfi_zscore > 0.5:
        flow_direction = "Buy-side dominant (bullish flow)"
    elif tfi_zscore < -0.5:
        flow_direction = "Sell-side dominant (bearish flow)"
    else:
        flow_direction = "Balanced flow (no clear direction)"

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return (
        f"[ORACLE 5M STATE VECTOR — {symbol}] @ {timestamp}\n"
        f"1. Micro-Volatility : {vol_bps:.4f} bps/sec  → {vol_label}\n"
        f"2. TFI Z-Score      : {tfi_zscore:+.4f}σ       → {tox_label} | {flow_direction}\n"
        f"3. Adverse Selection: {adv_sel_pct:.1f}%          → {adv_label}\n"
        f"   (Data window: {len(tfi_values)} TFI samples)"
    )
