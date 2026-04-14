"""
agent_q_loop.py — LLM Orchestration & RAG Memory (The Brain)

The main orchestration loop for the Mammon V3 Cognitive Pipeline.

Agent Q Hierarchy (3 agents):
  ┌─────────────────────────────────────────────────────────┐
  │  Oracle (every 5 min)                                   │
  │  → Reads State Vector → Classifies Regime               │
  │  → Publishes regime to Redis                            │
  ├─────────────────────────────────────────────────────────┤
  │  Tactical Alpha (every 3 min)                           │
  │  → Reads Regime + State Vector + RAG Memory             │
  │  → Proposes sniper levers                               │
  ├─────────────────────────────────────────────────────────┤
  │  Risk Manager (every 3 min, after Alpha)                │
  │  → Enforces hard safety bounds on Alpha proposal        │
  │  → Publishes final levers to Redis → Rust engine reads  │
  └─────────────────────────────────────────────────────────┘

LLM Backend: LM Studio at http://127.0.0.1:1234 (OpenAI-compatible API)
"""

import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any

import psycopg2
import redis
from dotenv import load_dotenv
from openai import OpenAI

from memory_ledger import (
    calculate_reward,
    fetch_last_cycle_outcomes,
    fetch_rag_memory,
    persist_cycle,
)
from semantic_watcher import generate_state_vector

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("agent_q")

# ── Config ────────────────────────────────────────────────────────────────────
SYMBOL         = os.getenv("SYMBOL", "SOLFDUSD")
LM_STUDIO_URL  = os.getenv("LM_STUDIO_URL", "http://127.0.0.1:1234")
LM_MODEL       = os.getenv("LM_MODEL", "local-model")
REDIS_URL      = os.getenv("REDIS_URL", "redis://127.0.0.1:6379")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "mammon_hft_redis")
POSTGRES_URL   = os.getenv("POSTGRES_URL", "postgresql://mammon:mammon@localhost:5432/mammon")
PROMPTS_DIR    = Path(__file__).parent / "prompts"

# Orchestration cadence — aggressive continuous injection
ORACLE_INTERVAL_SEC = 90    # re-classify regime every 90 seconds
ALPHA_INTERVAL_SEC  = 30    # re-optimise levers every 30 seconds
MAIN_LOOP_SLEEP_SEC = 1     # tight poll — never miss a window

# Lever defaults — tuned for SOLFDUSD: tight spread, high cadence, 3 parallel tranches
DEFAULT_LEVERS: dict[str, Any] = {
    "momentum_trigger_obi": 0.40,   # SOL is liquid — lower OBI needed to fire
    "take_profit_ticks":    2,      # SOL tick = $0.01 → $0.02 TP per tranche
    "stop_loss_ticks":      6,      # 3R:1 asymmetry
    "max_active_tranches":  3,      # fire 3 simultaneous $6 tranches
}

# ── Utilities ─────────────────────────────────────────────────────────────────

def load_prompt(name: str) -> str:
    """Load a prompt file from the prompts/ directory."""
    path = PROMPTS_DIR / f"{name}.txt"
    return path.read_text(encoding="utf-8").strip()


def extract_json(text: str) -> dict:
    """
    Robustly extract a JSON object from LLM output.
    Handles markdown code fences, leading/trailing text, etc.
    """
    text = text.strip()
    # Strip markdown fences
    text = re.sub(r"```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"```\s*$", "", text, flags=re.MULTILINE)
    # Find first complete JSON object
    match = re.search(r"\{[^{}]*\}", text, re.DOTALL)
    if match:
        return json.loads(match.group())
    raise ValueError(f"No JSON object found in response: {text[:300]!r}")


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def enforce_lever_bounds(levers: dict) -> dict:
    """
    Code-level hard clamp — safety net that runs regardless of LLM output.
    Mirrors the Risk Manager rules in pure Python for guaranteed enforcement.
    """
    obi      = clamp(float(levers.get("momentum_trigger_obi", 0.40)), 0.20, 0.80)
    tp       = int(clamp(int(levers.get("take_profit_ticks",    2)),  1,     8))
    sl       = int(clamp(int(levers.get("stop_loss_ticks",       4)), 3,    20))
    tranches = int(clamp(int(levers.get("max_active_tranches",  3)),  1,     5))

    # Asymmetric risk enforcement — stop must be at least 2× profit target
    if sl < tp * 2:
        sl = tp * 2
        log.warning("[BOUNDS] Asymmetric risk enforced: stop_loss → %d", sl)

    return {
        "momentum_trigger_obi": round(obi, 4),
        "take_profit_ticks":    tp,
        "stop_loss_ticks":      sl,
        "max_active_tranches":  tranches,
    }


# ── Agent Q ───────────────────────────────────────────────────────────────────

class AgentQ:
    """
    Orchestrates the three-agent LLM hierarchy:
      Oracle → Tactical Alpha → Risk Manager
    Connects to LM Studio via OpenAI-compatible API.
    """

    def __init__(self) -> None:
        # ── LM Studio client ──────────────────────────────────────────────────
        self.llm = OpenAI(
            base_url=f"{LM_STUDIO_URL}/v1",
            api_key="lm-studio",   # Required by SDK; LM Studio ignores it
        )

        # ── Redis (sync) ──────────────────────────────────────────────────────
        self.redis = redis.from_url(
            REDIS_URL,
            password=REDIS_PASSWORD,
            decode_responses=True,
        )

        # ── PostgreSQL ────────────────────────────────────────────────────────
        self.pg_conn = psycopg2.connect(POSTGRES_URL)
        self.pg_conn.autocommit = False

        # ── Load prompts once ─────────────────────────────────────────────────
        self.oracle_prompt  = load_prompt("oracle")
        self.alpha_prompt   = load_prompt("alpha")
        self.risk_prompt    = load_prompt("risk_manager")

        # ── Working state ─────────────────────────────────────────────────────
        self.current_regime = "MEAN_REVERTING"
        self.current_levers = DEFAULT_LEVERS.copy()
        # Auto-detect LM Studio model (must be after self.llm is set)
        self._lm_model = self._detect_model()

        log.info(
            "▶ Agent Q initialised | Symbol=%s | LM Studio=%s | Model=%s",
            SYMBOL, LM_STUDIO_URL, self._lm_model,
        )

    def _detect_model(self) -> str:
        """Query LM Studio /v1/models to find the currently loaded model."""
        try:
            models = self.llm.models.list()
            if models.data:
                model_id = models.data[0].id
                log.info("[LM Studio] Auto-detected model: %s", model_id)
                return model_id
        except Exception as exc:
            log.warning("[LM Studio] Model detection failed: %s", exc)
        # Fall back to env var
        return LM_MODEL

    def _pg_conn(self):
        """Return a healthy Postgres connection, reconnecting if needed."""
        try:
            # Quick ping to check connection is alive
            cur = self.pg_conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
        except Exception:
            log.warning("Postgres connection dead, reconnecting...")
            try:
                self.pg_conn.close()
            except Exception:
                pass
            self.pg_conn = psycopg2.connect(POSTGRES_URL)
            self.pg_conn.autocommit = False
        return self.pg_conn

    # ── LLM call ─────────────────────────────────────────────────────────────

    def _chat(
        self,
        system_prompt: str,
        user_content: str,
        temperature: float = 0.15,
        max_tokens: int = 256,
    ) -> str:
        """Single LLM call to LM Studio."""
        response = self.llm.chat.completions.create(
            model=self._lm_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_content},
            ],
            temperature=temperature,
            max_tokens=max_tokens,
        )
        return response.choices[0].message.content.strip()

    # ── Oracle ────────────────────────────────────────────────────────────────

    def run_oracle(self, state_vector: str) -> str:
        """
        5-minute macro regime classifier.
        Reads the State Vector → outputs one of four regime labels.
        Publishes result to Redis for both the Alpha and the Rust engine.
        """
        log.info("[ORACLE] Classifying regime from state vector...")
        try:
            raw    = self._chat(self.oracle_prompt, state_vector)
            result = extract_json(raw)
            regime = result.get("regime", self.current_regime)

            # Validate it's a known regime
            known = {"RETAIL_FRENZY_UP", "INSTITUTIONAL_DUMP", "MEAN_REVERTING", "TOXIC_CASCADE"}
            if regime not in known:
                log.warning("[ORACLE] Unknown regime '%s', defaulting to MEAN_REVERTING", regime)
                regime = "MEAN_REVERTING"

            log.info("[ORACLE] ✓ Regime → %s", regime)
            self.redis.set(f"hft:regime:{SYMBOL}", regime, ex=360)  # TTL: 6 minutes
            return regime

        except Exception as exc:
            log.error("[ORACLE] Failed: %s — keeping last regime: %s", exc, self.current_regime)
            return self.current_regime

    # ── Tactical Alpha ────────────────────────────────────────────────────────

    def run_alpha(self, state_vector: str, regime: str, rag_memory: str) -> dict:
        """
        3-minute tactical lever optimizer.
        Proposes optimal sniper parameters based on regime + RAG memory.
        """
        log.info("[ALPHA] Generating sniper levers for regime=%s...", regime)
        user_content = (
            f"REGIME: {regime}\n\n"
            f"{state_vector}\n\n"
            f"{rag_memory}"
        )
        try:
            raw    = self._chat(self.alpha_prompt, user_content)
            result = extract_json(raw)
            log.info("[ALPHA] Proposed: %s", result)
            return result

        except Exception as exc:
            log.error("[ALPHA] Failed: %s — using current levers as proposal.", exc)
            return self.current_levers.copy()

    # ── Risk Manager ──────────────────────────────────────────────────────────

    def run_risk_manager(self, regime: str, alpha_levers: dict) -> dict:
        """
        3-minute safety gate. Forcefully clamps Alpha's proposal.
        Also applies Python-level hard bounds as a guaranteed fallback.
        """
        log.info("[RISK] Applying safety constraints (regime=%s)...", regime)
        user_content = (
            f"REGIME: {regime}\n\n"
            f"PROPOSED LEVERS:\n{json.dumps(alpha_levers, indent=2)}"
        )
        try:
            raw    = self._chat(self.risk_prompt, user_content)
            result = extract_json(raw)
            log.info("[RISK] LLM output: %s", result)
        except Exception as exc:
            log.error("[RISK] LLM failed: %s — hard-clamping Alpha proposal.", exc)
            result = alpha_levers.copy()

        # ── Python-level hard enforcement (code beats LLM every time) ─────────
        final = enforce_lever_bounds(result)

        # ── TOXIC_CASCADE emergency override ──────────────────────────────────
        if regime == "TOXIC_CASCADE":
            final["max_active_tranches"]  = 1
            final["momentum_trigger_obi"] = max(final["momentum_trigger_obi"], 0.75)
            log.warning("[RISK] ⚠ TOXIC_CASCADE override applied! max_tranches=1, obi≥0.75")

        # ── INSTITUTIONAL_DUMP caution override ───────────────────────────────
        elif regime == "INSTITUTIONAL_DUMP":
            final["max_active_tranches"] = min(final["max_active_tranches"], 2)
            final["stop_loss_ticks"]     = max(final["stop_loss_ticks"], 15)
            log.warning("[RISK] ⚠ INSTITUTIONAL_DUMP caution applied! max_tranches≤2, sl≥15")

        log.info("[RISK] ✓ Final levers: %s", final)
        return final

    # ── Publish ───────────────────────────────────────────────────────────────

    def publish_levers(self, levers: dict) -> None:
        """
        Publishes final sniper levers to Redis.
        The Rust engine reads this key every 30 seconds.
        TTL = 200s so Rust falls back to defaults if Python dies.
        """
        key = f"hft:live_params:{SYMBOL}"
        self.redis.set(key, json.dumps(levers), ex=200)
        log.info("[PUBLISH] → Redis[%s] = %s", key, levers)

    # ── Cycle Runners ─────────────────────────────────────────────────────────

    def run_oracle_cycle(self) -> None:
        """Full 5-minute Oracle cycle."""
        log.info("=" * 60)
        log.info("[ORACLE CYCLE] Starting...")
        try:
            pg = self._pg_conn()
            state_vector = generate_state_vector(SYMBOL, self.redis, pg)
            log.info("[STATE VECTOR]\n%s", state_vector)
            self.current_regime = self.run_oracle(state_vector)
        except Exception as exc:
            log.error("[ORACLE CYCLE] Unhandled error: %s", exc)

    def run_alpha_cycle(self) -> None:
        """Full 3-minute Alpha → Risk → Publish cycle."""
        log.info("-" * 60)
        log.info("[ALPHA CYCLE] Starting (regime=%s)...", self.current_regime)
        try:
            pg = self._pg_conn()
            # Generate fresh state vector
            state_vector = generate_state_vector(SYMBOL, self.redis, pg)

            # Fetch RAG memory (last 5 cycles)
            rag_memory = fetch_rag_memory(SYMBOL, pg, limit=5)
            log.debug("[RAG]\n%s", rag_memory)

            # Run Alpha → Risk Manager
            alpha_proposal = self.run_alpha(state_vector, self.current_regime, rag_memory)
            final_levers   = self.run_risk_manager(self.current_regime, alpha_proposal)

            # Publish to Rust engine
            self.publish_levers(final_levers)
            self.current_levers = final_levers

            # ── RL: Measure and persist previous cycle ──────────────────────
            outcomes = fetch_last_cycle_outcomes(SYMBOL, pg)
            reward   = calculate_reward(
                outcomes["total_round_trips"],
                outcomes["win_rate_pct"],
                outcomes["net_pnl"],
            )
            log.info(
                "[RL] trips=%d win=%.1f%% pnl=%+.4f reward=%+.2f",
                outcomes["total_round_trips"],
                outcomes["win_rate_pct"] * 100,
                outcomes["net_pnl"],
                reward,
            )
            persist_cycle(
                SYMBOL,
                self.current_regime,
                self.current_levers,
                outcomes,
                reward,
                pg,
            )

        except Exception as exc:
            log.error("[ALPHA CYCLE] Unhandled error: %s", exc)
            try:
                self.pg_conn.rollback()
            except Exception:
                pass

    # ── Main Loop ─────────────────────────────────────────────────────────────

    def run(self) -> None:
        """Main orchestration loop — continuous injection, runs until killed."""
        log.info("▶ Agent Q CONTINUOUS MODE | Symbol=%s | Oracle=%ds | Alpha=%ds",
                 SYMBOL, ORACLE_INTERVAL_SEC, ALPHA_INTERVAL_SEC)

        # Publish defaults immediately — Rust engine never starves
        self.publish_levers(DEFAULT_LEVERS)

        # Fire both cycles immediately on boot
        self.run_oracle_cycle()
        self.run_alpha_cycle()

        last_oracle_at = time.time()
        last_alpha_at  = time.time()

        while True:
            now = time.time()

            # Re-classify regime on Oracle interval
            if now - last_oracle_at >= ORACLE_INTERVAL_SEC:
                self.run_oracle_cycle()
                last_oracle_at = now

            # Re-optimise and inject levers on Alpha interval
            if now - last_alpha_at >= ALPHA_INTERVAL_SEC:
                self.run_alpha_cycle()
                last_alpha_at = now

                # Immediately re-publish after every alpha cycle so the
                # Rust engine gets fresh levers without waiting for its
                # 30-second Redis poll
                self.publish_levers(self.current_levers)

            time.sleep(MAIN_LOOP_SLEEP_SEC)


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    agent = AgentQ()
    agent.run()
