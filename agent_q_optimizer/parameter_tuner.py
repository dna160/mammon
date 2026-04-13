"""
Parameter Tuner — Agent Q Tactical Loop (V2.2: every 3 minutes).

V2.2 changes per PRD §3:
  - 3-minute evaluation cycle (TACTICAL_INTERVAL_S=180 in main.py)
  - 3-minute reward back-fill window
  - grid_offset_ticks lever injected into Alpha/CRO pipeline
  - max_active_tranches, obi_threshold, grid_offset_ticks logged to agent_q_memory
  - Hyper-cadence reward: -200 × (1 - trips/15) below target

Pipeline:
  1. Read current regime from Redis (Oracle every 5m)
  2. Query 3-minute telemetry from PostgreSQL
  3. [RAG] Retrieve Top 1 best / Bottom 1 worst memory for active regime
  4. Agent 1 (Alpha Quant) — propose all 6 levers with T-1 memory context
  5. Agent 2 (CRO)         — enforce Dynamic Bounding Matrix
  6. Log decision to agent_q_memory (all 6 levers + state snapshot)
  7. Publish final params  → hft:live_params:{symbol} (Rust reads immediately)
  8. Schedule reward back-fill 3 min from now
"""
import json
import logging
import os
import threading
import time

import psycopg2
import psycopg2.extras

from data_pipeline    import (fetch_telemetry_window, get_state_vector_structured,
                              get_rl_metrics_for_symbol, calculate_rl_reward,
                              get_last_cycle_reward_string)
from lm_studio_client import call_agent_1_alpha, call_agent_2_risk
from json_sanitizer   import extract_json
from redis_bridge     import publish_params, publish_safe_mode, get_current_regime

log = logging.getLogger(__name__)

DB_DSN = os.getenv("DB_DSN", "postgresql://mammon:mammon@postgres:5432/mammon")

# V2.2: 3-minute evaluation window
EVAL_WINDOW_MINUTES = 3
EVAL_WAIT_SECONDS   = EVAL_WINDOW_MINUTES * 60   # 180s

# ── Pending reward queue: list of (memory_id, symbol, eval_after_ts) ─────────
_reward_queue: list[tuple[int, str, float]] = []
_reward_lock  = threading.Lock()


# ── Database helpers ──────────────────────────────────────────────────────────

def _get_db():
    return psycopg2.connect(DB_DSN, connect_timeout=5)


def _retrieve_rag_memory(symbol: str, regime: str) -> str:
    """
    Query agent_q_memory for Top 1 best and Bottom 1 worst decisions in this regime.
    Returns a compact memory block string for LLM injection.
    """
    try:
        conn = _get_db()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT proposed_gamma, proposed_min_spread, max_active_tranches,
                       grid_offset_ticks, obi_threshold, net_pnl, total_round_trips,
                       reward_score, alpha_reasoning
                FROM   agent_q_memory
                WHERE  symbol  = %s
                  AND  regime  = %s
                  AND  evaluated_at IS NOT NULL
                ORDER  BY reward_score DESC
                LIMIT  1
            """, (symbol, regime))
            best = cur.fetchall()

            cur.execute("""
                SELECT proposed_gamma, proposed_min_spread, max_active_tranches,
                       grid_offset_ticks, obi_threshold, net_pnl, total_round_trips,
                       reward_score, alpha_reasoning
                FROM   agent_q_memory
                WHERE  symbol  = %s
                  AND  regime  = %s
                  AND  evaluated_at IS NOT NULL
                ORDER  BY reward_score ASC
                LIMIT  1
            """, (symbol, regime))
            worst = cur.fetchall()
        conn.close()

        if not best and not worst:
            return ""

        lines = [f"MEM[{regime[:4]}]:"]
        for r in best[:1]:
            lines.append(
                f"BEST sp={r['proposed_min_spread']:.0f} g={r['proposed_gamma']:.1f} "
                f"t={r['max_active_tranches']} off={r['grid_offset_ticks']:.1f} "
                f"trips={r['total_round_trips']} pnl={r['net_pnl']:.3f}"
            )
        for r in worst[:1]:
            lines.append(
                f"WORST sp={r['proposed_min_spread']:.0f} g={r['proposed_gamma']:.1f} "
                f"t={r['max_active_tranches']} off={r['grid_offset_ticks']:.1f} "
                f"trips={r['total_round_trips']} pnl={r['net_pnl']:.3f}"
            )
        return " | ".join(lines)

    except Exception as exc:
        log.warning("[%s] RAG memory retrieval failed: %s", symbol, exc)
        return ""


def _log_decision(
    symbol: str,
    regime: str,
    gamma: float,
    min_spread: float,
    tfi_threshold: float,
    max_active_tranches: int,
    obi_threshold: float,
    grid_offset_ticks: float,
    metrics: dict,
    alpha_reasoning: str,
    cro_reasoning: str,
    override_applied: bool,
) -> int | None:
    """
    Insert a new agent_q_memory row and return its id.
    V2.2: now logs max_active_tranches, obi_threshold, and grid_offset_ticks.
    """
    try:
        conn = _get_db()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO agent_q_memory
                    (symbol, regime,
                     proposed_gamma, proposed_min_spread, tfi_threshold,
                     max_active_tranches, obi_threshold, grid_offset_ticks,
                     vol_bps, tfi_zscore, drift_bps, native_spread,
                     alpha_reasoning, cro_reasoning, override_applied)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (
                symbol, regime,
                gamma, min_spread, tfi_threshold,
                max_active_tranches, obi_threshold, grid_offset_ticks,
                metrics.get("vol_bps",       0),
                metrics.get("tfi_zscore",    0),
                metrics.get("drift_bps",     0),
                metrics.get("native_spread", 0),
                alpha_reasoning[:500] if alpha_reasoning else None,
                cro_reasoning[:500]   if cro_reasoning   else None,
                override_applied,
            ))
            mem_id = cur.fetchone()[0]
        conn.commit()
        conn.close()
        log.info("[%s] agent_q_memory id=%d logged (tranches=%d offset=%.1f obi=%.2f).",
                 symbol, mem_id, max_active_tranches, grid_offset_ticks, obi_threshold)
        return mem_id
    except Exception as exc:
        log.error("[%s] Failed to log agent_q_memory: %s", symbol, exc)
        return None


def _evaluate_reward(mem_id: int, symbol: str) -> None:
    """
    PRD §3: Reward evaluated 3 minutes after parameter injection.
    Back-fills total_round_trips, win_rate_pct, net_pnl, adverse_selection, reward_score.
    """
    try:
        conn = _get_db()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT timestamp FROM agent_q_memory WHERE id = %s", (mem_id,))
            row = cur.fetchone()
            if not row:
                conn.close()
                return
            decision_ts = row["timestamp"]

            # PnL and adverse selection in the 3-minute window after the decision
            cur.execute("""
                SELECT COALESCE(SUM(net_pnl_usd), 0) AS net_pnl,
                       COALESCE(
                           100.0 * COUNT(*) FILTER (WHERE side = 'SELL' AND net_pnl_usd < 0)
                           / NULLIF(COUNT(*) FILTER (WHERE side = 'SELL'), 0),
                           0
                       ) AS adverse_pct
                FROM  execution_log
                WHERE symbol    = %s
                  AND timestamp BETWEEN %s AND %s + INTERVAL '3 minutes'
            """, (symbol, decision_ts, decision_ts))
            res = cur.fetchone()

            net_pnl  = float(res["net_pnl"]    if res else 0)
            adv_pct  = float(res["adverse_pct"] if res else 0)

            # V2.2: 3-minute RL metrics window
            rl_metrics   = get_rl_metrics_for_symbol(symbol, window_minutes=EVAL_WINDOW_MINUTES)
            round_trips  = rl_metrics["round_trips"]
            win_rate     = rl_metrics["win_rate"]
            win_rate_pct = win_rate * 100.0

            reward_score = calculate_rl_reward(round_trips, win_rate, net_pnl)

            cur.execute("""
                UPDATE agent_q_memory
                SET    total_round_trips  = %s,
                       win_rate_pct       = %s,
                       net_pnl            = %s,
                       adverse_selection  = %s,
                       reward_score       = %s,
                       evaluated_at       = NOW()
                WHERE  id = %s
            """, (round_trips, win_rate_pct, net_pnl, adv_pct, reward_score, mem_id))

        conn.commit()
        conn.close()
        log.info(
            "[%s] RL Reward id=%d: trips=%d/15 win=%.0f%% PnL=%.4f score=%.2f",
            symbol, mem_id, round_trips, win_rate_pct, net_pnl, reward_score,
        )
    except Exception as exc:
        log.error("[%s] Reward evaluation failed for id=%d: %s", symbol, mem_id, exc)


# ── Background reward evaluator ───────────────────────────────────────────────

def _recover_orphaned_rewards() -> None:
    """Re-queue any PENDING agent_q_memory rows on startup (handles restarts)."""
    try:
        conn = _get_db()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, symbol, timestamp
                FROM   agent_q_memory
                WHERE  evaluated_at IS NULL
                ORDER  BY timestamp ASC
            """)
            rows = cur.fetchall()
        conn.close()

        if not rows:
            log.info("Reward recovery: no orphaned PENDING rows.")
            return

        import datetime
        now_wall = time.monotonic()
        now_utc  = datetime.datetime.now(datetime.timezone.utc)
        recover_count = 0
        with _reward_lock:
            already = {mid for (mid, _, _) in _reward_queue}
            for (mem_id, symbol, ts) in rows:
                if mem_id in already:
                    continue
                age_s      = (now_utc - ts).total_seconds()
                wait_s     = max(0.0, EVAL_WAIT_SECONDS - age_s)
                eval_after = now_wall + wait_s
                _reward_queue.append((mem_id, symbol, eval_after))
                recover_count += 1
                log.info(
                    "Reward recovery: id=%d [%s] age=%.0fs → eval in %.0fs",
                    mem_id, symbol, age_s, wait_s,
                )
        if recover_count:
            log.info("Reward recovery: re-queued %d orphaned row(s).", recover_count)
    except Exception as exc:
        log.warning("Reward recovery sweep failed: %s", exc)


def _reward_evaluator_loop() -> None:
    """Daemon thread — checks the pending queue every 30s for entries ready to score."""
    log.info("Reward evaluator started (3-min evaluation window).")
    _recover_orphaned_rewards()
    while True:
        time.sleep(30)   # Check every 30s (window is 3m so plenty of resolution)
        now = time.monotonic()
        with _reward_lock:
            ready    = [(mid, sym) for (mid, sym, after) in _reward_queue if now >= after]
            _reward_queue[:] = [(mid, sym, after) for (mid, sym, after) in _reward_queue if now < after]

        for mem_id, symbol in ready:
            try:
                _evaluate_reward(mem_id, symbol)
            except Exception as exc:
                log.error("Reward eval error id=%d: %s", mem_id, exc)


# Start the daemon reward thread on first import
_reward_thread = threading.Thread(target=_reward_evaluator_loop, name="RewardEval", daemon=True)
_reward_thread.start()


# ── Main pipeline ─────────────────────────────────────────────────────────────

def tune_parameters_for_symbol(symbol: str) -> None:
    """
    V2.2: 3-minute tactical parameter-tuning pipeline.
    Raises on any error — caller handles safe-mode fallback.
    """
    log.info("[%s] ── Tactical cycle start (3m) ──────────────────────────────", symbol)

    # Step 0: Read regime from Oracle (Redis)
    current_regime = get_current_regime(symbol)
    log.info("[%s] Active regime: %s", symbol, current_regime)

    # Step 1: 3-minute telemetry + structured metrics
    stats_str = fetch_telemetry_window(symbol, window_minutes=EVAL_WINDOW_MINUTES)
    metrics   = get_state_vector_structured(symbol, window_minutes=EVAL_WINDOW_MINUTES)
    log.info("[%s] Tactical telemetry: %s", symbol, stats_str)

    # Step 1b: T-1 cycle reward string for Alpha prompt injection
    last_cycle_memory = get_last_cycle_reward_string(symbol)
    log.info("[%s] T-1 RL memory: %s", symbol, last_cycle_memory)

    # Step 2: RAG memory for this regime
    rag_block = _retrieve_rag_memory(symbol, current_regime)
    if rag_block:
        log.info("[%s] RAG memory injected (%d chars).", symbol, len(rag_block))
        stats_str_with_rag = stats_str + "\n" + rag_block
    else:
        stats_str_with_rag = stats_str

    # Step 3: Alpha Quant — propose all 6 levers with T-1 RL memory
    raw_alpha  = call_agent_1_alpha(
        stats_str_with_rag,
        current_regime=current_regime,
        last_cycle_memory=last_cycle_memory,
    )
    log.info("[%s] Agent-1 raw (%.120s…)", symbol, raw_alpha)
    alpha_dict = extract_json(raw_alpha)
    alpha_reasoning = alpha_dict.get("reasoning", "")
    log.info(
        "[%s] Agent-1: γ=%.2f spread=%.1f tfi=$%.0f tranches=%d offset=%.1f obi=%.2f | %s",
        symbol,
        float(alpha_dict.get("gamma",               0)),
        float(alpha_dict.get("min_spread_ticks",     0)),
        float(alpha_dict.get("tfi_threshold",        0)),
        int(alpha_dict.get("max_active_tranches",    1)),
        float(alpha_dict.get("grid_offset_ticks",    2.0)),
        float(alpha_dict.get("obi_threshold",        1.0)),
        alpha_reasoning,
    )

    # Step 4: CRO — Dynamic Bounding Matrix
    raw_cro  = call_agent_2_risk(
        stats_str,
        json.dumps(alpha_dict, separators=(",", ":")),
        current_regime=current_regime,
    )
    log.info("[%s] Agent-2 raw (%.120s…)", symbol, raw_cro)
    cro_dict = extract_json(raw_cro)
    cro_reasoning    = cro_dict.get("cro_reasoning", "")
    override_applied = bool(cro_dict.get("override_applied", False))

    # Handle TOXIC_LIQUIDATION_CASCADE veto
    if cro_dict.get("panic_sell_flag") is True:
        log.critical("[%s] CRO VETO — TOXIC_LIQUIDATION_CASCADE. Publishing safe mode.", symbol)
        publish_safe_mode(symbol)
        return

    final_gamma    = float(cro_dict.get("final_gamma",               0.8))
    final_spread   = float(cro_dict.get("final_min_spread_ticks",    5.0))
    final_tfi      = float(cro_dict.get("final_tfi_threshold",       65_000.0))
    final_obi      = float(cro_dict.get("final_obi_threshold",       1.0))
    final_tranches = int(cro_dict.get("final_max_active_tranches",   1))
    final_offset   = float(cro_dict.get("final_grid_offset_ticks",   2.0))

    log.info(
        "[%s] Agent-2: γ=%.2f spread=%.1f tfi=$%.0f obi=%.2f tranches=%d offset=%.1f override=%s",
        symbol, final_gamma, final_spread, final_tfi, final_obi,
        final_tranches, final_offset, override_applied,
    )

    # Step 5: Log decision to agent_q_memory BEFORE publishing
    mem_id = _log_decision(
        symbol               = symbol,
        regime               = current_regime,
        gamma                = final_gamma,
        min_spread           = final_spread,
        tfi_threshold        = final_tfi,
        max_active_tranches  = final_tranches,
        obi_threshold        = final_obi,
        grid_offset_ticks    = final_offset,
        metrics              = metrics,
        alpha_reasoning      = alpha_reasoning,
        cro_reasoning        = cro_reasoning,
        override_applied     = override_applied,
    )

    # Schedule reward back-fill 3 min from now (V2.2 cadence)
    if mem_id is not None:
        eval_after = time.monotonic() + EVAL_WAIT_SECONDS
        with _reward_lock:
            _reward_queue.append((mem_id, symbol, eval_after))

    # Step 6: Publish tuned params to Rust engine via Redis
    publish_params(symbol, cro_dict)
    log.info("[%s] ── Tactical params injected ────────────────────────────────", symbol)
