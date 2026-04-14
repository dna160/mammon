"""
memory_ledger.py — RL Reward & RAG Memory

Two responsibilities:
  1. calculate_reward()  — Cadence-First RL reward function (target: 15 trips/3min)
  2. fetch_rag_memory()  — Retrieves last N cycles as a formatted RAG context string
  3. fetch_last_cycle_outcomes() — Measures what happened in the last 3 minutes
  4. persist_cycle()     — Writes one completed tactical cycle to agent_q_memory
"""

import logging
from typing import Any

log = logging.getLogger("memory_ledger")


# ── Reward Function ───────────────────────────────────────────────────────────

def calculate_reward(trips: int, win_rate: float, pnl: float) -> float:
    """
    Cadence-First RL reward function.

    The primary KPI is round-trip cadence: 15 trips per 3 minutes.
    Failing this target incurs a severe exponential penalty that overwhelms
    any PnL or win rate gains made with too-low trade volume.

    Args:
        trips:    Total round trips completed in the last 3 minutes
        win_rate: Fraction of trades with positive net_pnl  [0.0, 1.0]
        pnl:      Total net PnL in USD for the cycle

    Returns:
        float: Composite reward score (higher = better)
    """
    # ── Volume Score ──────────────────────────────────────────────────────────
    if trips < 15:
        # Severe exponential penalty for failing the cadence quota
        # At 0 trips: -200. At 7 trips: -106. At 14 trips: -13.3
        volume_score = -200.0 * (1.0 - (trips / 15.0))
    else:
        # Diminishing bonus above target to prevent over-optimising for volume
        volume_score = 50.0 + (trips - 15) * 2.0

    # ── Win Rate Score ────────────────────────────────────────────────────────
    # 50% win rate = 0 score. Each % above/below 50% adds/removes 0.5 points.
    win_score = (win_rate - 0.5) * 50.0

    # ── PnL Score ─────────────────────────────────────────────────────────────
    pnl_score = pnl * 10.0

    total = volume_score + win_score + pnl_score
    return round(total, 4)


# ── Cycle Outcomes ────────────────────────────────────────────────────────────

def fetch_last_cycle_outcomes(symbol: str, pg_conn) -> dict[str, Any]:
    """
    Reads trade_telemetry for the last 3 minutes to measure the previous
    tactical cycle's performance. Used to compute the RL reward.

    Returns:
        dict with keys: total_round_trips, win_rate_pct, net_pnl
    """
    defaults = {"total_round_trips": 0, "win_rate_pct": 0.5, "net_pnl": 0.0}
    try:
        cursor = pg_conn.cursor()
        cursor.execute(
            """
            SELECT
                COUNT(*)::int                                                          AS trips,
                COALESCE(
                    AVG(CASE WHEN net_pnl > 0 THEN 1.0 ELSE 0.0 END), 0.5
                )::float                                                               AS win_rate,
                COALESCE(SUM(net_pnl), 0.0)::float                                    AS total_pnl
            FROM trade_telemetry
            WHERE timestamp > NOW() - INTERVAL '3 minutes'
              AND symbol = %s
            """,
            (symbol,),
        )
        row = cursor.fetchone()
        cursor.close()
        if row:
            return {
                "total_round_trips": int(row[0]),
                "win_rate_pct":      float(row[1]),
                "net_pnl":          float(row[2]),
            }
    except Exception as exc:
        log.warning("fetch_last_cycle_outcomes error: %s", exc)
        try:
            pg_conn.rollback()  # reset aborted transaction
        except Exception:
            pass
    return defaults


# ── Persist Cycle ─────────────────────────────────────────────────────────────

def persist_cycle(
    symbol:   str,
    regime:   str,
    levers:   dict,
    outcomes: dict,
    reward:   float,
    pg_conn,
) -> None:
    """
    Writes one completed tactical cycle to agent_q_memory.
    This is the dataset the Alpha uses for RAG retrieval next cycle.
    """
    try:
        cursor = pg_conn.cursor()
        cursor.execute(
            """
            INSERT INTO agent_q_memory (
                symbol, regime,
                momentum_trigger_obi, take_profit_ticks, stop_loss_ticks, max_active_tranches,
                total_round_trips, win_rate_pct, net_pnl, reward_score
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                symbol,
                regime,
                float(levers.get("momentum_trigger_obi", 0.55)),
                int(levers.get("take_profit_ticks", 3)),
                int(levers.get("stop_loss_ticks", 10)),
                int(levers.get("max_active_tranches", 3)),
                outcomes["total_round_trips"],
                outcomes["win_rate_pct"],
                outcomes["net_pnl"],
                reward,
            ),
        )
        pg_conn.commit()
        cursor.close()
        log.info(
            "Cycle persisted → regime=%s trips=%d win=%.1f%% pnl=%.4f reward=%.2f",
            regime,
            outcomes["total_round_trips"],
            outcomes["win_rate_pct"] * 100,
            outcomes["net_pnl"],
            reward,
        )
    except Exception as exc:
        log.error("persist_cycle error: %s", exc)
        try:
            pg_conn.rollback()
        except Exception:
            pass


# ── RAG Memory ────────────────────────────────────────────────────────────────

def fetch_rag_memory(symbol: str, pg_conn, limit: int = 5) -> str:
    """
    Retrieves the last `limit` tactical cycles from agent_q_memory, formatted
    as a readable RAG context block for the Alpha agent's prompt.

    Format clearly shows: what parameters were chosen, what happened, and
    what the reward was — so the LLM can learn and adapt.
    """
    try:
        cursor = pg_conn.cursor()
        cursor.execute(
            """
            SELECT
                regime,
                momentum_trigger_obi,
                take_profit_ticks,
                stop_loss_ticks,
                max_active_tranches,
                total_round_trips,
                win_rate_pct,
                net_pnl,
                reward_score,
                timestamp
            FROM agent_q_memory
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT %s
            """,
            (symbol, limit),
        )
        rows = cursor.fetchall()
        cursor.close()
    except Exception as exc:
        log.warning("fetch_rag_memory error: %s", exc)
        try:
            pg_conn.rollback()  # reset aborted transaction
        except Exception:
            pass
        return "[RAG MEMORY] Unavailable — using first-run defaults."

    if not rows:
        return "[RAG MEMORY] No historical cycles recorded yet. This is the first run."

    lines = [f"[RAG MEMORY — Last {len(rows)} Tactical Cycles for {symbol}]"]
    lines.append("─" * 60)

    for i, row in enumerate(rows):
        (regime, obi, tp, sl, tranches, trips, win_rate, pnl, reward, ts) = row
        reward_label = "✓ GOOD" if reward > 0 else "✗ BAD"
        lines.append(
            f"T-{i+1} [{ts.strftime('%H:%M:%S') if ts else 'N/A'}] "
            f"Regime={regime} | "
            f"OBI={float(obi):.2f} TP={tp}t SL={sl}t Tranches={tranches} | "
            f"Trips={trips} WinRate={float(win_rate):.0%} PnL={float(pnl):+.4f} | "
            f"Reward={float(reward):+.1f} {reward_label}"
        )

    lines.append("─" * 60)

    # Compute trend hint
    rewards = [float(r[8]) for r in rows]
    if len(rewards) >= 2:
        trend = rewards[0] - rewards[-1]
        if trend > 20:
            lines.append("TREND: Improving ↑ — stay the course or push harder.")
        elif trend < -20:
            lines.append("TREND: Declining ↓ — CHANGE your strategy. What you are doing is not working.")
        else:
            lines.append("TREND: Stable → — minor tuning may be sufficient.")

    return "\n".join(lines)
