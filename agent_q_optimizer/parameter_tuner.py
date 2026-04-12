"""
Parameter Tuner — Agent Q Tactical Loop (every 15 minutes).

CRITICAL: Reads the currently active regime from Redis BEFORE running the
adversarial pipeline. The CRO Agent enforces the Dynamic Bounding Matrix
specific to that regime — not static global bounds.

Pipeline:
  1. Read current regime from Redis (set by Oracle every 5m)
  2. Query 15-minute telemetry from PostgreSQL
  3. Agent 1 (Alpha Quant) — propose gamma/spread tailored to current regime
  4. Agent 2 (CRO)         — enforce regime-specific Dynamic Bounding Matrix
  5. Publish final params  → hft:live_params:{symbol}
"""
import json
import logging

from data_pipeline    import fetch_telemetry_window
from lm_studio_client import call_agent_1_alpha, call_agent_2_risk
from json_sanitizer   import extract_json
from redis_bridge     import publish_params, publish_safe_mode, get_current_regime

log = logging.getLogger(__name__)


def tune_parameters_for_symbol(symbol: str) -> None:
    """
    Run the adversarial parameter-tuning pipeline for one symbol.
    Raises on any error — caller handles safe-mode fallback.
    """
    log.info("[%s] ── Tactical cycle start ────────────────────────────────", symbol)

    # Step 0: Read regime from Oracle (Redis)
    current_regime = get_current_regime(symbol)
    log.info("[%s] Active regime: %s", symbol, current_regime)

    # Step 1: 15-minute telemetry
    stats_str = fetch_telemetry_window(symbol, window_minutes=15)
    log.info("[%s] Tactical telemetry: %s", symbol, stats_str)

    # Step 2: Alpha Quant — regime-aware proposal
    # Inject regime context into user message so Alpha proposes within bounds
    raw_alpha  = call_agent_1_alpha(stats_str, current_regime=current_regime)
    log.info("[%s] Agent-1 raw (%.120s…)", symbol, raw_alpha)
    alpha_dict = extract_json(raw_alpha)
    log.info(
        "[%s] Agent-1: γ=%.2f spread=%.1f tfi=$%.0f | %s",
        symbol,
        float(alpha_dict.get("gamma", 0)),
        float(alpha_dict.get("min_spread_ticks", 0)),
        float(alpha_dict.get("tfi_threshold", 0)),
        alpha_dict.get("reasoning", ""),
    )

    # Step 3: CRO — Dynamic Bounding Matrix for current_regime
    raw_cro  = call_agent_2_risk(
        stats_str,
        json.dumps(alpha_dict, separators=(",", ":")),
        current_regime=current_regime,
    )
    log.info("[%s] Agent-2 raw (%.120s…)", symbol, raw_cro)
    cro_dict = extract_json(raw_cro)

    # Handle TOXIC_LIQUIDATION_CASCADE veto
    if cro_dict.get("panic_sell_flag") is True:
        log.critical(
            "[%s] CRO VETO — TOXIC_LIQUIDATION_CASCADE. gamma forced 1.0, safe mode.", symbol
        )
        publish_safe_mode(symbol)
        return

    log.info(
        "[%s] Agent-2: γ=%.2f spread=%.1f tfi=$%.0f override=%s",
        symbol,
        float(cro_dict.get("final_gamma", 0)),
        float(cro_dict.get("final_min_spread_ticks", 0)),
        float(cro_dict.get("final_tfi_threshold", 0)),
        cro_dict.get("override_applied"),
    )

    # Step 4: Publish tuned params to Rust
    publish_params(symbol, cro_dict)
    log.info("[%s] ── Tactical params injected ──────────────────────────────", symbol)
