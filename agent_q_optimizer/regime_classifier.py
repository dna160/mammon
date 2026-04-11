"""
Regime Classifier — Agent Q Oracle Loop (every 5 minutes).

Queries the last 5-minute telemetry window, calls Llama 3.2 3B to classify
the macro market regime, and publishes the result to:
  Redis channel : hft:regime:{symbol.lower()}
  Persistence   : hft:regime:{symbol.lower()}:latest

Valid regimes:
  MEAN_REVERTING               — balanced, 2-sided quoting
  RETAIL_FRENZY_UP             — momentum up, accumulate inventory
  INSTITUTIONAL_ABSORPTION_DOWN — distribution, dump inventory
  DEAD_ZONE                    — thin/stale book, force wide spread
  TOXIC_LIQUIDATION_CASCADE    — hard stop, panic-sell all inventory
"""
import json
import logging

from data_pipeline    import fetch_telemetry_window
from lm_studio_client import call_regime_classifier
from json_sanitizer   import extract_json
from redis_bridge     import publish_regime, publish_safe_mode

log = logging.getLogger(__name__)

VALID_REGIMES = {
    "MEAN_REVERTING",
    "RETAIL_FRENZY_UP",
    "INSTITUTIONAL_ABSORPTION_DOWN",
    "DEAD_ZONE",
    "TOXIC_LIQUIDATION_CASCADE",
}


def classify_regime_for_symbol(symbol: str) -> str:
    """
    Run the Oracle pipeline for one symbol.
    Returns the classified regime string (e.g. 'MEAN_REVERTING').
    Raises on any error — caller handles safe-mode fallback.
    """
    log.info("[%s] ── Oracle cycle start ──────────────────────────────────", symbol)

    # Stage 0: 5-minute telemetry window
    stats_str = fetch_telemetry_window(symbol, window_minutes=5)
    log.info("[%s] Oracle telemetry: %s", symbol, stats_str)

    # Stage 1: LLM regime classification
    raw = call_regime_classifier(stats_str)
    log.info("[%s] Oracle raw (%.120s…)", symbol, raw)

    parsed    = extract_json(raw)
    regime    = parsed.get("regime", "MEAN_REVERTING").strip().upper()
    confidence = float(parsed.get("confidence", 0.5))

    # Sanitise: reject unknown regimes → safe default
    if regime not in VALID_REGIMES:
        log.warning("[%s] Unknown regime '%s' — defaulting to MEAN_REVERTING.", symbol, regime)
        regime = "MEAN_REVERTING"

    log.info("[%s] Oracle classified: %s (conf=%.2f) — %s",
             symbol, regime, confidence, parsed.get("reasoning", ""))

    # Stage 2: Publish to Redis
    publish_regime(symbol, regime, confidence)
    log.info("[%s] ── Oracle regime published ─────────────────────────────", symbol)
    return regime
