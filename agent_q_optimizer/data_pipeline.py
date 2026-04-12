"""
Data Pipeline — Institutional State Vector (V2 PRD).

Architecture:
  1. Per-symbol in-memory rolling window (≤300 data points, ~1s cadence = 5 min).
  2. Watcher computes 5 institutional metrics (BPS, Z-Scores, adverse selection).
  3. Compressed 5-line State Vector replaces raw floats as LLM input.
  4. [V2] Publishes structured JSON to Redis cognitive:state_vector:{symbol}
     so the telemetry dashboard can display live state vectors without DB queries.

Metrics:
  A. vol_bps      — Micro-Volatility in Basis Points (std of 1s returns × 10000)
  B. tfi_zscore   — TFI Z-Score: (current_tfi − mean_tfi) / std_tfi
  C. drift_bps    — Micro-Trend Drift in BPS over the window (total price change)
  D. native_spread — Native LOB Spread in ticks (5m average of spread/tick_size)
  E. adverse_pct  — Adverse Selection % (% of fills with negative PnL from Postgres)
"""
import os
import json
import logging
import time
import numpy as np
import psycopg2
import psycopg2.extras
from collections import deque
from typing      import Dict, Tuple

log = logging.getLogger(__name__)

DB_DSN    = os.getenv("DB_DSN",    "postgresql://mammon:mammon@postgres:5432/mammon")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")

# Must match Rust COIN_CONFIGS tick_size values
TICK_SIZES: Dict[str, float] = {
    "ADAFDUSD":  0.0001,
    "DOTFDUSD":  0.001,
    "DOGEFDUSD": 0.00001,
    "XRPFDUSD":  0.0001,
}

WINDOW_SIZE = 300   # ≈5 minutes at 1 sample/second

# Per-symbol rolling window: deque of (price, tfi, spread_ticks)
_history: Dict[str, deque] = {}

_redis_client = None


def _get_redis():
    global _redis_client
    if _redis_client is None:
        import redis as redis_lib
        _redis_client = redis_lib.from_url(REDIS_URL, decode_responses=True)
    return _redis_client


def _ensure_history(symbol: str) -> deque:
    if symbol not in _history:
        _history[symbol] = deque(maxlen=WINDOW_SIZE)
    return _history[symbol]


def _fetch_latest_tick(symbol: str) -> Tuple[float, float, float]:
    """
    Pull latest (price, tfi, spread_ticks) from Redis.
    Returns (0.0, 0.0, 5.0) on any error.
    """
    try:
        r         = _get_redis()
        tick_size = TICK_SIZES.get(symbol, 0.0001)

        price        = 0.0
        tfi          = 0.0
        spread_ticks = 5.0

        raw_pipe = r.get(f"engine_d:{symbol}:pipeline")
        if raw_pipe:
            d         = json.loads(raw_pipe)
            price     = float(d.get("micro_price", 0.0))
            lob_bid   = float(d.get("lob_bid", 0.0))
            lob_ask   = float(d.get("lob_ask", 0.0))
            if lob_bid > 0.0 and lob_ask > lob_bid:
                spread_ticks = (lob_ask - lob_bid) / tick_size
            else:
                raw_spread   = float(d.get("spread", 0.0))
                spread_ticks = (raw_spread / tick_size) if tick_size > 0 else 5.0

        raw_tel = r.get(f"telemetry:engine_d:{symbol}")
        if raw_tel:
            d   = json.loads(raw_tel)
            tfi = float(d.get("tfi", 0.0))

        return price, tfi, spread_ticks

    except Exception as e:
        log.error("[%s] Redis tick fetch failed: %s", symbol, e)
        return 0.0, 0.0, 5.0


def _fetch_adverse_selection(symbol: str, window_minutes: int) -> float:
    """Query Postgres for % of fills with negative net_pnl over the window."""
    try:
        conn = psycopg2.connect(DB_DSN, connect_timeout=5)
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT
                    COUNT(*)                                             AS total,
                    SUM(CASE WHEN net_pnl < 0 THEN 1 ELSE 0 END)       AS losers
                FROM  trade_telemetry
                WHERE engine_id  = 'D'
                  AND asset_pair = %s
                  AND timestamp  >= NOW() - INTERVAL %s
            """, (symbol, f"{window_minutes} minutes"))
            row = cur.fetchone()
        conn.close()
        if row and row["total"] and int(row["total"]) > 0:
            return float(row["losers"] or 0) / float(row["total"]) * 100.0
    except Exception as e:
        log.error("[%s] Postgres adverse selection query failed: %s", symbol, e)
    return 0.0


def get_state_vector(symbol: str, window_minutes: int = 5) -> str:
    """
    Build the ORACLE 5M STATE VECTOR for `symbol`.

    Samples the latest Redis tick into the rolling history, computes 5
    institutional metrics, returns the compressed 5-line string for LLM injection,
    and [V2] publishes structured JSON to cognitive:state_vector:{symbol.lower()}.
    """
    sym  = symbol.upper()
    hist = _ensure_history(sym)

    # ── 1. Sample latest tick into rolling window ─────────────────────────────
    price, tfi, spread_ticks = _fetch_latest_tick(sym)
    if price > 0.0:
        hist.append((price, tfi, spread_ticks))

    # ── 2. Compute institutional metrics ─────────────────────────────────────
    vol_bps               = 0.0
    tfi_zscore            = 0.0
    drift_bps             = 0.0
    native_spread         = 5.0
    adverse_selection_pct = 0.0

    try:
        if len(hist) >= 2:
            prices  = np.array([p for p, _, _ in hist], dtype=float)
            tfis    = np.array([t for _, t, _ in hist], dtype=float)
            spreads = np.array([s for _, _, s in hist], dtype=float)

            returns    = np.diff(prices) / (prices[:-1] + 1e-12)
            vol_bps    = float(np.std(returns) * 10_000)
            tfi_zscore = float((tfis[-1] - np.mean(tfis)) / (np.std(tfis) + 1e-9))
            drift_bps  = float(((prices[-1] - prices[0]) / (prices[0] + 1e-12)) * 10_000)
            native_spread = float(np.mean(spreads))

        adverse_selection_pct = _fetch_adverse_selection(sym, window_minutes)

    except Exception as e:
        log.error("[%s] State vector calculation failed: %s", sym, e)

    # ── 3. Semantic labels ────────────────────────────────────────────────────
    vol_label   = "EXTREME"           if vol_bps    >  20   else \
                  "HIGH"              if vol_bps    >  10   else "NORMAL"
    tox_label   = "TOXIC BUYING"      if tfi_zscore >   2.0 else \
                  "TOXIC DUMPING"     if tfi_zscore <  -2.0 else "CLEAN"
    trend_label = "STRONG UPTREND"    if drift_bps  >  30   else \
                  "STRONG DOWNTREND"  if drift_bps  < -30   else "CHOP/RANGE"
    adv_label   = "DANGER (Run Over)" if adverse_selection_pct > 60 else \
                  "SAFE (Capturing Spread)"

    # ── 4. [V2] Publish structured JSON to Redis for dashboard ───────────────
    try:
        structured = {
            "symbol":            sym,
            "ts":                int(time.time() * 1000),
            "vol_bps":           round(vol_bps, 4),
            "vol_label":         vol_label,
            "tfi_zscore":        round(tfi_zscore, 4),
            "tfi_label":         tox_label,
            "drift_bps":         round(drift_bps, 4),
            "drift_label":       trend_label,
            "native_spread":     round(native_spread, 2),
            "adverse_pct":       round(adverse_selection_pct, 2),
            "adverse_label":     adv_label,
            "window_minutes":    window_minutes,
            "sample_count":      len(hist),
        }
        r = _get_redis()
        r.set(
            f"cognitive:state_vector:{sym.lower()}",
            json.dumps(structured),
            ex=600,   # 10-min TTL — stale if pipeline stops
        )
    except Exception as e:
        log.warning("[%s] State vector Redis publish failed: %s", sym, e)

    # ── 5. Compressed string for LLM (~50 tokens) ────────────────────────────
    return (
        f"[ORACLE 5M STATE VECTOR - {sym}]\n"
        f"1. Micro-Volatility: {vol_bps:.2f} bps/sec ({vol_label})\n"
        f"2. Order Flow Toxicity: {tfi_zscore:+.2f}\u03c3 ({tox_label})\n"
        f"3. Market Drift: {drift_bps:+.2f} bps/5m ({trend_label})\n"
        f"4. Native LOB Spread: {native_spread:.1f} ticks\n"
        f"5. Adverse Selection: {adverse_selection_pct:.1f}% ({adv_label})"
    )


def get_state_vector_structured(symbol: str, window_minutes: int = 5) -> dict:
    """
    Returns structured metrics dict (after calling get_state_vector to populate cache).
    Used by parameter_tuner to capture metrics for agent_q_memory logging.
    """
    sym  = symbol.upper()
    hist = _ensure_history(sym)

    vol_bps = tfi_zscore = drift_bps = native_spread = adverse_selection_pct = 0.0

    try:
        if len(hist) >= 2:
            prices  = np.array([p for p, _, _ in hist], dtype=float)
            tfis    = np.array([t for _, t, _ in hist], dtype=float)
            spreads = np.array([s for _, _, s in hist], dtype=float)

            returns    = np.diff(prices) / (prices[:-1] + 1e-12)
            vol_bps    = float(np.std(returns) * 10_000)
            tfi_zscore = float((tfis[-1] - np.mean(tfis)) / (np.std(tfis) + 1e-9))
            drift_bps  = float(((prices[-1] - prices[0]) / (prices[0] + 1e-12)) * 10_000)
            native_spread = float(np.mean(spreads))

        adverse_selection_pct = _fetch_adverse_selection(sym, window_minutes)
    except Exception:
        pass

    return {
        "vol_bps":      vol_bps,
        "tfi_zscore":   tfi_zscore,
        "drift_bps":    drift_bps,
        "native_spread": native_spread,
        "adverse_pct":  adverse_selection_pct,
    }


def fetch_telemetry_window(symbol: str, window_minutes: int = 15) -> str:
    """Backward-compat shim — delegates to get_state_vector()."""
    return get_state_vector(symbol, window_minutes=window_minutes)
