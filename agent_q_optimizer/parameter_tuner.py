"""
Parameter Tuner — Agent Q Tactical Loop (every 1 minute).

V2 additions per PRD §5 (RAG Memory):
  - Writes each parameter decision to agent_q_memory table before publishing.
  - Retrieves Top 2 / Bottom 2 historical actions for the active regime and
    injects them into the Alpha prompt (Retrieval-Augmented Reflection).
  - A background evaluator thread continuously back-fills net_pnl and
    reward_score on entries older than 1 minute (async, never blocks trading).

Pipeline:
  1. Read current regime from Redis (set by Oracle every 5m)
  2. Query 1-minute telemetry from PostgreSQL
  3. [V2] Retrieve RAG memory for current regime (Top 2 best / Bottom 2 worst)
  4. Agent 1 (Alpha Quant) — propose gamma/spread/obi_threshold with historical context
  5. Agent 2 (CRO)         — enforce Dynamic Bounding Matrix
  6. [V2] Log decision to agent_q_memory (id returned for 1m reward back-fill)
  7. Publish final params  → hft:live_params:{symbol}
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

# ── Pending reward queue: list of (memory_id, symbol, eval_after_ts) ─────────
_reward_queue: list[tuple[int, str, float]] = []
_reward_lock  = threading.Lock()


# ── Database helpers ──────────────────────────────────────────────────────────

def _get_db():
    return psycopg2.connect(DB_DSN, connect_timeout=5)


def _retrieve_rag_memory(symbol: str, regime: str) -> str:
    """
    Query agent_q_memory for Top 2 best and Bottom 2 worst decisions in this regime.
    Returns an injected memory block string ready for the LLM prompt.
    PRD §5C: Retrieval-Augmented Reflection.
    """
    try:
        conn = _get_db()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Top 2 rewarded in this regime
            cur.execute("""
                SELECT proposed_gamma, proposed_min_spread, net_pnl, adverse_selection,
                       reward_score, alpha_reasoning
                FROM   agent_q_memory
                WHERE  symbol  = %s
                  AND  regime  = %s
                  AND  evaluated_at IS NOT NULL
                ORDER  BY reward_score DESC
                LIMIT  2
            """, (symbol, regime))
            best = cur.fetchall()

            # Bottom 2 punished
            cur.execute("""
                SELECT proposed_gamma, proposed_min_spread, net_pnl, adverse_selection,
                       reward_score, alpha_reasoning
                FROM   agent_q_memory
                WHERE  symbol  = %s
                  AND  regime  = %s
                  AND  evaluated_at IS NOT NULL
                ORDER  BY reward_score ASC
                LIMIT  2
            """, (symbol, regime))
            worst = cur.fetchall()
        conn.close()

        if not best and not worst:
            return ""

        # Keep RAG block compact — 3B model has limited context (≤150 chars target)
        lines = [f"MEM[{regime[:4]}]:"]
        for r in best[:1]:   # Top 1 only
            lines.append(f"BEST sp={r['proposed_min_spread']:.0f} g={r['proposed_gamma']:.1f} pnl={r['net_pnl']:.3f}")
        for r in worst[:1]:  # Bottom 1 only
            lines.append(f"WORST sp={r['proposed_min_spread']:.0f} g={r['proposed_gamma']:.1f} pnl={r['net_pnl']:.3f}")
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
    metrics: dict,
    alpha_reasoning: str,
    cro_reasoning: str,
    override_applied: bool,
) -> int | None:
    """
    Insert a new agent_q_memory row and return its id.
    The reward back-filler will update net_pnl + reward_score 15 min later.
    """
    try:
        conn = _get_db()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO agent_q_memory
                    (symbol, regime, proposed_gamma, proposed_min_spread, tfi_threshold,
                     vol_bps, tfi_zscore, drift_bps, native_spread,
                     alpha_reasoning, cro_reasoning, override_applied)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (
                symbol, regime, gamma, min_spread, tfi_threshold,
                metrics.get("vol_bps",      0),
                metrics.get("tfi_zscore",   0),
                metrics.get("drift_bps",    0),
                metrics.get("native_spread",0),
                alpha_reasoning[:500] if alpha_reasoning else None,
                cro_reasoning[:500]   if cro_reasoning   else None,
                override_applied,
            ))
            # Note: total_round_trips and win_rate_pct are back-filled by _evaluate_reward
            mem_id = cur.fetchone()[0]
        conn.commit()
        conn.close()
        log.info("[%s] agent_q_memory id=%d logged.", symbol, mem_id)
        return mem_id
    except Exception as exc:
        log.error("[%s] Failed to log agent_q_memory: %s", symbol, exc)
        return None


def _evaluate_reward(mem_id: int, symbol: str) -> None:
    """
    PRD §5B: Reward = Net PnL - Adverse Selection Penalty.
    Queries the 15-minute PnL window that started when parameters were injected
    and back-fills agent_q_memory with net_pnl, adverse_selection, reward_score.
    """
    try:
        conn = _get_db()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Get the timestamp of the decision
            cur.execute("SELECT timestamp FROM agent_q_memory WHERE id = %s", (mem_id,))
            row = cur.fetchone()
            if not row:
                conn.close()
                return
            decision_ts = row["timestamp"]

            # Net PnL accumulated in the 1m window after the decision
            # Uses execution_log (primary) — has symbol, net_pnl_usd, side columns.
            # Adverse selection = % of SELL fills that closed at a loss (net_pnl_usd < 0).
            cur.execute("""
                SELECT COALESCE(SUM(net_pnl_usd), 0) AS net_pnl,
                       COALESCE(
                           100.0 * COUNT(*) FILTER (WHERE side = 'SELL' AND net_pnl_usd < 0)
                           / NULLIF(COUNT(*) FILTER (WHERE side = 'SELL'), 0),
                           0
                       ) AS adverse_pct
                FROM  execution_log
                WHERE symbol    = %s
                  AND timestamp BETWEEN %s AND %s + INTERVAL '1 minutes'
            """, (symbol, decision_ts, decision_ts))
            res = cur.fetchone()

            net_pnl  = float(res["net_pnl"]    if res else 0)
            adv_pct  = float(res["adverse_pct"] if res else 0)

            # ── RL Hyper-Cadence Reward (PRD §4) ─────────────────────────────
            # Get round trips + win rate for the 1m window after this decision
            rl_metrics  = get_rl_metrics_for_symbol(symbol, window_minutes=1)
            round_trips = rl_metrics["round_trips"]
            win_rate    = rl_metrics["win_rate"]
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
            "[%s] RL Reward id=%d: trips=%d win=%.0f%% PnL=%.4f score=%.2f",
            symbol, mem_id, round_trips, win_rate_pct, net_pnl, reward_score,
        )
    except Exception as exc:
        log.error("[%s] Reward evaluation failed for id=%d: %s", symbol, mem_id, exc)


# ── Background reward evaluator (runs every 60s) ─────────────────────────────

def _recover_orphaned_rewards() -> None:
    """
    Startup sweep — re-queue any agent_q_memory rows that are un-evaluated
    and whose 15-minute window has already closed (or is about to close).

    This handles the case where agent_q restarted before _reward_evaluator_loop
    could fire: the in-memory queue was lost but the DB rows remain PENDING.

    Rows whose window has NOT closed yet (timestamp > NOW() - 15m) are also
    re-queued so they still get scored after the remaining wait.
    """
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
                # How many seconds remain until 1m after the decision?
                age_s    = (now_utc - ts).total_seconds()
                wait_s   = max(0.0, 1 * 60 - age_s)
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
    """Daemon thread — continuously checks the pending queue for entries ready to score."""
    log.info("Reward evaluator started.")
    _recover_orphaned_rewards()   # ← re-queue anything lost across restarts
    while True:
        time.sleep(60)
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
    Run the adversarial parameter-tuning pipeline for one symbol.
    V2: injects RAG memory into Alpha prompt + logs decision to agent_q_memory.
    Raises on any error — caller handles safe-mode fallback.
    """
    log.info("[%s] \u2500\u2500 Tactical cycle start \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500", symbol)

    # Step 0: Read regime from Oracle (Redis)
    current_regime = get_current_regime(symbol)
    log.info("[%s] Active regime: %s", symbol, current_regime)

    # Step 1: 1-minute telemetry + structured metrics
    stats_str = fetch_telemetry_window(symbol, window_minutes=1)
    metrics   = get_state_vector_structured(symbol, window_minutes=1)
    log.info("[%s] Tactical telemetry: %s", symbol, stats_str)

    # Step 1b: [RL] Get T-1 cycle reward string for Alpha prompt injection
    last_cycle_memory = get_last_cycle_reward_string(symbol)
    log.info("[%s] T-1 RL memory: %s", symbol, last_cycle_memory)

    # Step 2: [V2] Retrieve RAG memory for this regime
    rag_block = _retrieve_rag_memory(symbol, current_regime)
    if rag_block:
        log.info("[%s] RAG memory injected (%d chars).", symbol, len(rag_block))
        stats_str_with_rag = stats_str + "\n" + rag_block
    else:
        stats_str_with_rag = stats_str

    # Step 3: Alpha Quant — regime-aware proposal with RAG context + T-1 RL memory
    raw_alpha  = call_agent_1_alpha(
        stats_str_with_rag,
        current_regime=current_regime,
        last_cycle_memory=last_cycle_memory,
    )
    log.info("[%s] Agent-1 raw (%.120s\u2026)", symbol, raw_alpha)
    alpha_dict = extract_json(raw_alpha)
    alpha_reasoning = alpha_dict.get("reasoning", "")
    log.info(
        "[%s] Agent-1: \u03b3=%.2f spread=%.1f tfi=$%.0f | %s",
        symbol,
        float(alpha_dict.get("gamma", 0)),
        float(alpha_dict.get("min_spread_ticks", 0)),
        float(alpha_dict.get("tfi_threshold", 0)),
        alpha_reasoning,
    )

    # Step 4: CRO — Dynamic Bounding Matrix for current_regime
    raw_cro  = call_agent_2_risk(
        stats_str,
        json.dumps(alpha_dict, separators=(",", ":")),
        current_regime=current_regime,
    )
    log.info("[%s] Agent-2 raw (%.120s\u2026)", symbol, raw_cro)
    cro_dict = extract_json(raw_cro)
    cro_reasoning    = cro_dict.get("cro_reasoning", "")
    override_applied = bool(cro_dict.get("override_applied", False))

    # Handle TOXIC_LIQUIDATION_CASCADE veto
    if cro_dict.get("panic_sell_flag") is True:
        log.critical(
            "[%s] CRO VETO \u2014 TOXIC_LIQUIDATION_CASCADE. gamma forced 1.0, safe mode.", symbol
        )
        publish_safe_mode(symbol)
        return

    final_gamma  = float(cro_dict.get("final_gamma",            0.8))
    final_spread = float(cro_dict.get("final_min_spread_ticks", 10.0))
    final_tfi    = float(cro_dict.get("final_tfi_threshold",    65_000.0))
    final_obi    = float(cro_dict.get("final_obi_threshold",    1.0))    # 1.0 = permissive default

    log.info(
        "[%s] Agent-2: \u03b3=%.2f spread=%.1f tfi=$%.0f obi=%.2f override=%s",
        symbol, final_gamma, final_spread, final_tfi, final_obi, override_applied,
    )

    # Step 5: [V2] Log decision to agent_q_memory BEFORE publishing
    mem_id = _log_decision(
        symbol         = symbol,
        regime         = current_regime,
        gamma          = final_gamma,
        min_spread     = final_spread,
        tfi_threshold  = final_tfi,
        metrics        = metrics,
        alpha_reasoning = alpha_reasoning,
        cro_reasoning   = cro_reasoning,
        override_applied = override_applied,
    )

    # Schedule reward back-fill 1 min from now
    if mem_id is not None:
        eval_after = time.monotonic() + 1 * 60
        with _reward_lock:
            _reward_queue.append((mem_id, symbol, eval_after))

    # Step 6: Publish tuned params to Rust engine
    publish_params(symbol, cro_dict)
    log.info("[%s] \u2500\u2500 Tactical params injected \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500", symbol)
