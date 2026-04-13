"""
Data Pipeline — Institutional State Vector (V2.2 PRD).

Architecture:
  1. Per-symbol in-memory rolling window (≤300 data points, ~1s cadence = 5 min).
  2. Watcher computes 5 institutional metrics (BPS, Z-Scores, adverse selection).
  3. Compressed 5-line State Vector replaces raw floats as LLM input.
  4. [V2] Publishes structured JSON to Redis cognitive:state_vector:{symbol}
     so the telemetry dashboard can display live state vectors without DB queries.

Metrics:
  A. vol_bps       — Micro-Volatility in Basis Points
  B. tfi_zscore    — TFI Z-Score
  C. drift_bps     — Micro-Trend Drift in BPS over the window
  D. native_spread — Native LOB Spread in ticks (5m average)
  E. adverse_pct   — Adverse Selection % from execution_log
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
    "SOLFDUSD":  0.01,
    "XRPFDUSD":  0.0001,
    "DOGEFDUSD": 0.00001,
    "ETHFDUSD":  0.01,
    "BNBFDUSD":  0.1,
}

WINDOW_SIZE = 300   # ≈5 minutes at 1 sample/second

# Per-symbol rolling window: deque of (price, tfi, spread_ticks, obi)
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


def _fetch_latest_tick(symbol: str) -> Tuple[float, float, float, float]:
    """Pull latest (price, tfi, spread_ticks, obi) from Redis.

    OBI (Order Book Imbalance) is now included so the state vector can show
    Agent Q what the actual imbalance signal is — essential context for setting
    obi_threshold sensibly.
    """
    try:
        r         = _get_redis()
        tick_size = TICK_SIZES.get(symbol, 0.0001)

        price        = 0.0
        tfi          = 0.0
        spread_ticks = 5.0
        obi          = 0.0

        raw_pipe = r.get(f"engine_d:{symbol}:pipeline")
        if raw_pipe:
            d         = json.loads(raw_pipe)
            price     = float(d.get("micro_price", 0.0))
            obi       = float(d.get("obi", 0.0))
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

        return price, tfi, spread_ticks, obi

    except Exception as e:
        log.error("[%s] Redis tick fetch failed: %s", symbol, e)
        return 0.0, 0.0, 5.0, 0.0


def _fetch_live_engine_context(symbol: str) -> dict:
    """
    Read the engine's current live state from Redis pipeline key.
    Returns inventory, pnl_mtm, decision, active tranches, and applied params.
    This gives Agent Q the situational awareness to make lever decisions.
    """
    ctx = {
        "inventory_coin":        0.0,
        "pnl_mtm":               0.0,
        "decision":              "WARMING",
        "active_tranches":       0,
        "applied_max_tranches":  1,
        "applied_grid_offset":   2.0,
        "obi_live":              0.0,
        "micro_price":           0.0,
        "ticks_held":            0,
        "emergency_dump":        False,
    }
    try:
        r       = _get_redis()
        raw     = r.get(f"engine_d:{symbol}:pipeline")
        if not raw:
            return ctx
        d = json.loads(raw)

        inv        = float(d.get("inventory_coin", 0.0))
        price      = float(d.get("micro_price", 0.0))
        pnl_cash   = float(d.get("pnl_usd", 0.0))
        pnl_mtm    = pnl_cash + inv * price   # mark-to-market

        ctx["inventory_coin"]       = inv
        ctx["pnl_mtm"]              = pnl_mtm
        ctx["decision"]             = d.get("decision", "WARMING")
        ctx["applied_max_tranches"] = int(d.get("max_active_tranches", 1))
        ctx["applied_grid_offset"]  = float(d.get("grid_offset_ticks", 2.0))
        ctx["obi_live"]             = float(d.get("obi", 0.0))
        ctx["micro_price"]          = price
        ctx["ticks_held"]           = int(d.get("ticks_held", 0))
        ctx["emergency_dump"]       = bool(d.get("emergency_dump", False))

        # Approximate active tranches from inventory notional ($6 per tranche)
        notional = abs(inv) * price
        ctx["active_tranches"] = int(notional / 6.0) if price > 0 else 0

    except Exception as e:
        log.debug("[%s] Live engine context fetch failed: %s", symbol, e)
    return ctx


def _fetch_adverse_selection(symbol: str, window_minutes: int) -> float:
    """
    Query execution_log for % of SELL fills with negative net_pnl_usd over window.

    BUG FIX: Previously queried `trade_telemetry` (wrong table — Rust engine
    writes fills to `execution_log` with column `net_pnl_usd` and `symbol`).
    That caused adverse selection to always return 0%, suppressing
    TOXIC_LIQUIDATION_CASCADE detection entirely.
    """
    try:
        conn = psycopg2.connect(DB_DSN, connect_timeout=5)
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE side = 'SELL')                      AS total,
                    COUNT(*) FILTER (WHERE side = 'SELL' AND net_pnl_usd < 0)  AS losers
                FROM  execution_log
                WHERE symbol    = %s
                  AND timestamp >= NOW() - INTERVAL %s
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
    and publishes structured JSON to cognitive:state_vector:{symbol.lower()}.
    """
    sym  = symbol.upper()
    hist = _ensure_history(sym)

    # ── 1. Sample latest tick into rolling window ─────────────────────────────
    price, tfi, spread_ticks, obi = _fetch_latest_tick(sym)
    if price > 0.0:
        hist.append((price, tfi, spread_ticks, obi))

    # ── 1b. Fetch live engine context (situational awareness for Agent Q) ─────
    eng_ctx = _fetch_live_engine_context(sym)

    # ── 2. Compute institutional metrics ─────────────────────────────────────
    vol_bps               = 0.0
    tfi_zscore            = 0.0
    drift_bps             = 0.0
    native_spread         = 5.0
    adverse_selection_pct = 0.0
    obi_mean              = 0.0

    try:
        if len(hist) >= 2:
            prices  = np.array([p for p, _, _, _ in hist], dtype=float)
            tfis    = np.array([t for _, t, _, _ in hist], dtype=float)
            spreads = np.array([s for _, _, s, _ in hist], dtype=float)
            obis    = np.array([o for _, _, _, o in hist], dtype=float)

            returns    = np.diff(prices) / (prices[:-1] + 1e-12)
            vol_bps    = float(np.std(returns) * 10_000)
            tfi_zscore = float((tfis[-1] - np.mean(tfis)) / (np.std(tfis) + 1e-9))
            drift_bps  = float(((prices[-1] - prices[0]) / (prices[0] + 1e-12)) * 10_000)
            native_spread = float(np.mean(spreads))
            obi_mean   = float(np.mean(obis))

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

    # ── 4. Publish structured JSON to Redis for dashboard ────────────────────
    try:
        structured = {
            "symbol":                sym,
            "ts":                    int(time.time() * 1000),
            "vol_bps":               round(vol_bps, 4),
            "vol_label":             vol_label,
            "tfi_zscore":            round(tfi_zscore, 4),
            "tfi_label":             tox_label,
            "drift_bps":             round(drift_bps, 4),
            "drift_label":           trend_label,
            "native_spread":         round(native_spread, 2),
            "obi_mean":              round(obi_mean, 4),
            "obi_live":              round(eng_ctx["obi_live"], 4),
            "adverse_pct":           round(adverse_selection_pct, 2),
            "adverse_label":         adv_label,
            "window_minutes":        window_minutes,
            "sample_count":          len(hist),
            # Live engine context
            "inventory_coin":        round(eng_ctx["inventory_coin"], 6),
            "pnl_mtm":               round(eng_ctx["pnl_mtm"], 4),
            "decision":              eng_ctx["decision"],
            "active_tranches":       eng_ctx["active_tranches"],
            "applied_max_tranches":  eng_ctx["applied_max_tranches"],
            "applied_grid_offset":   eng_ctx["applied_grid_offset"],
            "ticks_held":            eng_ctx["ticks_held"],
            "emergency_dump":        eng_ctx["emergency_dump"],
        }
        r = _get_redis()
        r.set(
            f"cognitive:state_vector:{sym.lower()}",
            json.dumps(structured),
            ex=600,   # 10-min TTL
        )
    except Exception as e:
        log.warning("[%s] State vector Redis publish failed: %s", sym, e)

    # ── 5. Compressed string for LLM — includes OBI + engine context ─────────
    # Engine context line gives Agent Q situational awareness:
    # Is the engine loaded? Running at what tranche depth? MTM profitable?
    bailout_flag = " ⚠ TAKER BAILOUT IMMINENT" if eng_ctx["ticks_held"] > 1000 else (
        " [EMERGENCY DUMP]" if eng_ctx["emergency_dump"] else ""
    )
    eng_line = (
        f"inv={eng_ctx['inventory_coin']:+.4f} coin | "
        f"pnl_mtm={eng_ctx['pnl_mtm']:+.4f} USD | "
        f"decision={eng_ctx['decision']} | "
        f"tranches={eng_ctx['active_tranches']}/{eng_ctx['applied_max_tranches']} | "
        f"offset={eng_ctx['applied_grid_offset']:.1f}t | "
        f"ticks_held={eng_ctx['ticks_held']}{bailout_flag}"
    )
    return (
        f"[ORACLE 5M STATE VECTOR - {sym}]\n"
        f"1. Micro-Volatility: {vol_bps:.2f} bps/sec ({vol_label})\n"
        f"2. Order Flow Toxicity: {tfi_zscore:+.2f}\u03c3 ({tox_label})\n"
        f"3. Market Drift: {drift_bps:+.2f} bps/5m ({trend_label})\n"
        f"4. Native LOB Spread: {native_spread:.1f} ticks\n"
        f"5. Adverse Selection: {adverse_selection_pct:.1f}% ({adv_label})\n"
        f"6. OBI (5m mean): {obi_mean:+.4f} (live: {eng_ctx['obi_live']:+.4f})\n"
        f"7. Engine State: {eng_line}"
    )


def get_state_vector_structured(symbol: str, window_minutes: int = 5) -> dict:
    """Returns structured metrics dict for agent_q_memory logging."""
    sym  = symbol.upper()
    hist = _ensure_history(sym)

    vol_bps = tfi_zscore = drift_bps = native_spread = adverse_selection_pct = obi_mean = 0.0

    try:
        if len(hist) >= 2:
            prices  = np.array([p for p, _, _, _ in hist], dtype=float)
            tfis    = np.array([t for _, t, _, _ in hist], dtype=float)
            spreads = np.array([s for _, _, s, _ in hist], dtype=float)
            obis    = np.array([o for _, _, _, o in hist], dtype=float)

            returns    = np.diff(prices) / (prices[:-1] + 1e-12)
            vol_bps    = float(np.std(returns) * 10_000)
            tfi_zscore = float((tfis[-1] - np.mean(tfis)) / (np.std(tfis) + 1e-9))
            drift_bps  = float(((prices[-1] - prices[0]) / (prices[0] + 1e-12)) * 10_000)
            native_spread = float(np.mean(spreads))
            obi_mean   = float(np.mean(obis))

        adverse_selection_pct = _fetch_adverse_selection(sym, window_minutes)
    except Exception:
        pass

    return {
        "vol_bps":       vol_bps,
        "tfi_zscore":    tfi_zscore,
        "drift_bps":     drift_bps,
        "native_spread": native_spread,
        "adverse_pct":   adverse_selection_pct,
        "obi_mean":      obi_mean,
    }


def fetch_telemetry_window(symbol: str, window_minutes: int = 3) -> str:
    """Backward-compat shim — delegates to get_state_vector()."""
    return get_state_vector(symbol, window_minutes=window_minutes)


# ── Hyper-Cadence Reward Function (V2.2 PRD §3B) ─────────────────────────────

def calculate_rl_reward(round_trips: int, win_rate: float, net_pnl: float) -> float:
    """
    V2.2 Velocity-First Reward Function — 3-minute evaluation cycle.

    Priority 1: Volume (Capital Velocity).
      Target = 15 round trips per 3-minute cycle.
      SEVERE linear punishment for starvation/bag-holding.
      Below 15: score = -200 × (1 − trips/15)  → max penalty -200 at 0 trips
      Above 15: score = 50  + (trips − 15) × 2  → bonus for hyper-cadence

    Priority 2: Win Rate (bonus for efficiency > 50%).
      score = (win_rate − 0.5) × 50  → range [−25, +25]

    Priority 3: Realized Net PnL.
      score = net_pnl × 10

    Examples (3m cycle):
        0 trades, 50% WR,  $0.00  → vol=-200 + wr=  0 + pnl=  0 = -200.0
        3 trades, 50% WR,  $0.00  → vol=-160 + wr=  0 + pnl=  0 = -160.0
       15 trades, 55% WR, +$0.05  → vol=  50 + wr=+2.5 + pnl=+0.5 = +53.0
       30 trades, 60% WR, +$0.10  → vol=  80 + wr=+5.0 + pnl=+1.0 = +86.0
    """
    target_trades = 15.0  # 15 round trips per 3-minute cycle

    # 1. Volume / Capital Velocity Score
    if round_trips < target_trades:
        # Linear punishment — brutal for bag-holding
        volume_score = -200.0 * (1.0 - (round_trips / target_trades))
    else:
        # Linear bonus above target — reward hyper-cadence
        volume_score = 50.0 + (round_trips - target_trades) * 2.0

    # 2. Win Rate Score (centred on 50%)
    win_rate_score = (win_rate - 0.5) * 50.0

    # 3. PnL Score — linear reinforcement
    pnl_score = net_pnl * 10.0

    return volume_score + win_rate_score + pnl_score


def get_rl_metrics_for_symbol(symbol: str, window_minutes: int = 3) -> dict:
    """
    Query execution_log for the most recent `window_minutes` window.
    Returns round_trips, win_rate, net_pnl.
    Default window is 3 minutes (V2.2 cadence mandate).
    """
    try:
        conn = psycopg2.connect(DB_DSN, connect_timeout=5)
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE side = 'SELL')                     AS round_trips,
                    COUNT(*) FILTER (WHERE side = 'SELL' AND net_pnl_usd > 0) AS winners,
                    COALESCE(SUM(net_pnl_usd), 0.0)                           AS net_pnl
                FROM  execution_log
                WHERE symbol    = %s
                  AND timestamp >= NOW() - INTERVAL %s
            """, (symbol, f"{window_minutes} minutes"))
            row = cur.fetchone()
        conn.close()

        round_trips = int(row["round_trips"] or 0)
        winners     = int(row["winners"]     or 0)
        net_pnl     = float(row["net_pnl"]   or 0.0)
        win_rate    = (winners / round_trips) if round_trips > 0 else 0.0
        return {
            "round_trips": round_trips,
            "win_rate":    win_rate,
            "net_pnl":     net_pnl,
        }
    except Exception as e:
        log.error("[%s] RL metrics query failed: %s", symbol, e)
        return {"round_trips": 0, "win_rate": 0.0, "net_pnl": 0.0}


def get_last_cycle_reward_string(symbol: str) -> str:
    """
    Return the most recent evaluated agent_q_memory row for `symbol` as a
    human-readable string for injection into the Alpha LLM prompt.
    Includes grid_offset_ticks and max_active_tranches for the AI to learn from.
    """
    try:
        conn = psycopg2.connect(DB_DSN, connect_timeout=5)
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT reward_score, total_round_trips, win_rate_pct, net_pnl,
                       max_active_tranches, grid_offset_ticks, obi_threshold
                FROM   agent_q_memory
                WHERE  symbol       = %s
                  AND  evaluated_at IS NOT NULL
                ORDER  BY evaluated_at DESC
                LIMIT  1
            """, (symbol,))
            row = cur.fetchone()
        conn.close()

        if row:
            sign    = "+" if row["net_pnl"] >= 0 else ""
            wr      = row["win_rate_pct"] or 0.0
            tranches = row["max_active_tranches"] or 1
            offset  = row["grid_offset_ticks"] or 2.0
            obi     = row["obi_threshold"] or 1.0
            return (
                f"Reward: {row['reward_score']:.1f} | "
                f"Trades: {row['total_round_trips']}/15 | "
                f"WR: {wr:.0f}% | "
                f"PnL: {sign}${row['net_pnl']:.2f} | "
                f"Tranches: {tranches} | Offset: {offset:.1f}t | OBI: {obi:.2f}"
            )
    except Exception as e:
        log.warning("[%s] Last cycle reward query failed: %s", symbol, e)
    return "No history yet — first cycle."
