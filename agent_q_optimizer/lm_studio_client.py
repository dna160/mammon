"""
LM Studio Client — OpenAI-compatible REST wrapper for local Llama 3.2 3B inference.

Two model instances are loaded in LM Studio and used in parallel:
  MODEL_1 (llama-3.2-3b-instruct)   — Oracle Regime Classifier + Agent 1 Alpha (proposing)
  MODEL_2 (llama-3.2-3b-instruct:2) — Agent 2 CRO (auditing / enforcing)

This split means that while one symbol's Alpha call is in-flight on model 1,
another symbol's CRO call can run simultaneously on model 2.

Rules (PRD §5):
  base_url    : http://host.docker.internal:1234/v1
  temperature : 0.0   — deterministic, no hallucinations
  top_p       : 0.1   — tight nucleus sampling
  timeout     : 15.0s — hard deadline → caller triggers safe mode
"""
import os
import logging
from openai import OpenAI

log = logging.getLogger(__name__)

LM_STUDIO_URL = os.getenv("LM_STUDIO_URL", "http://host.docker.internal:1234/v1")
MODEL_1       = os.getenv("LM_STUDIO_MODEL_1", "llama-3.2-3b-instruct")    # Oracle + Alpha
MODEL_2       = os.getenv("LM_STUDIO_MODEL_2", "llama-3.2-3b-instruct:2")  # CRO

_client1: OpenAI | None = None
_client2: OpenAI | None = None


def _get_client1() -> OpenAI:
    global _client1
    if _client1 is None:
        _client1 = OpenAI(base_url=LM_STUDIO_URL, api_key="lm-studio")
    return _client1


def _get_client2() -> OpenAI:
    global _client2
    if _client2 is None:
        _client2 = OpenAI(base_url=LM_STUDIO_URL, api_key="lm-studio")
    return _client2


def _load_prompt(filename: str, **fmt_kwargs) -> str:
    """Load a prompt template and optionally substitute {placeholders}."""
    path = os.path.join(os.path.dirname(__file__), "prompt_engineering", filename)
    with open(path, "r") as fh:
        text = fh.read().strip()
    if fmt_kwargs:
        text = text.format(**fmt_kwargs)
    return text


def _chat(client: OpenAI, model_id: str, system: str, user: str) -> str:
    """Single LLM call with deterministic settings and hard timeout."""
    response = client.chat.completions.create(
        model=model_id,
        messages=[
            {"role": "system", "content": system},
            {"role": "user",   "content": user},
        ],
        temperature=0.0,
        top_p=0.1,
        max_tokens=512,
        timeout=15.0,
    )
    return response.choices[0].message.content or ""


# ── Oracle Agent — Model 1 ────────────────────────────────────────────────────

def call_regime_classifier(stats_str: str) -> str:
    """
    Oracle loop (every 5m): classify macro market regime.
    Runs on MODEL_1 — returns raw LLM text with 'regime', 'confidence', 'reasoning'.
    """
    system = _load_prompt("agent_regime_classifier.txt")
    user   = f"MARKET_TELEMETRY_5MIN: {stats_str}\n\nRespond with JSON only."
    log.debug("[Oracle] Calling %s …", MODEL_1)
    return _chat(_get_client1(), MODEL_1, system, user)


# ── Tactical Agents ───────────────────────────────────────────────────────────

def call_agent_1_alpha(
    stats_str: str,
    current_regime: str = "MEAN_REVERTING",
    last_cycle_memory: str = "No history yet — first cycle.",
) -> str:
    """
    Alpha Quant (Stage 1): propose parameters tailored for current_regime.
    Injects T-1 RL reward memory so the LLM can self-correct on low-volume cycles.
    Runs on MODEL_1 — returns raw LLM text with proposed_gamma / spread / tfi.
    """
    system = _load_prompt("agent_1_alpha.txt", last_cycle_memory=last_cycle_memory)
    user   = (
        f"CURRENT_REGIME: {current_regime}\n"
        f"T-1 MEMORY: {last_cycle_memory}\n"
        f"MARKET_TELEMETRY_15MIN: {stats_str}\n\n"
        "Propose optimal parameters for this regime. Respond with JSON only."
    )
    log.debug("[Alpha] Calling %s …", MODEL_1)
    return _chat(_get_client1(), MODEL_1, system, user)


def call_agent_2_risk(
    stats_str: str,
    alpha_proposal: str,
    current_regime: str = "MEAN_REVERTING",
) -> str:
    """
    Chief Risk Officer (Stage 2): enforce Dynamic Bounding Matrix for current_regime.
    Runs on MODEL_2 — returns raw LLM text with final_gamma / spread / tfi.
    """
    system = _load_prompt("agent_2_risk.txt", current_regime=current_regime)
    user   = (
        f"CURRENT_REGIME: {current_regime}\n"
        f"MARKET_TELEMETRY_15MIN: {stats_str}\n\n"
        f"ALPHA_PROPOSAL: {alpha_proposal}\n\n"
        "Enforce the Dynamic Bounding Matrix and respond with JSON only."
    )
    log.debug("[CRO] Calling %s …", MODEL_2)
    return _chat(_get_client2(), MODEL_2, system, user)
