"""
Redis Bridge — Publishes Agent Q outputs to the Rust HFT engine via Pub/Sub.

Channel schema:
  hft:live_params:{symbol.lower()}         — Tactical params  (every 3m, V2.2)
  hft:regime:{symbol.lower()}              — Oracle regime    (every 5m)

Persistence keys (:latest) allow Rust to recover on reconnect.

V2.2 additions: grid_offset_ticks injected into every params payload.
"""
import os
import json
import logging
import redis

log = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")

_redis_client: redis.Redis | None = None


def _get_redis() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    return _redis_client


# ── Regime (Oracle) ───────────────────────────────────────────────────────────

def publish_regime(symbol: str, regime: str, confidence: float = 1.0) -> None:
    """Publish Oracle regime classification to Rust engine."""
    channel = f"hft:regime:{symbol.lower()}"
    payload = {"regime": regime, "confidence": round(confidence, 4)}
    r = _get_redis()
    r.publish(channel, json.dumps(payload))
    r.set(f"{channel}:latest", json.dumps(payload))
    log.info("[%s] Regime → %s: %s (conf=%.2f)", symbol, channel, regime, confidence)


def get_current_regime(symbol: str) -> str:
    """Read the latest Oracle regime from Redis. Returns 'MEAN_REVERTING' if unset."""
    try:
        r   = _get_redis()
        raw = r.get(f"hft:regime:{symbol.lower()}:latest")
        if raw:
            return json.loads(raw).get("regime", "MEAN_REVERTING")
    except Exception as exc:
        log.warning("[%s] Could not read regime from Redis: %s", symbol, exc)
    return "MEAN_REVERTING"


# ── Params (Tactical) ─────────────────────────────────────────────────────────

def publish_params(symbol: str, cro_output: dict) -> None:
    """
    Map CRO JSON → Rust engine parameter payload and publish via Redis.

    CRO fields                   → Rust payload fields
    final_gamma                  → gamma
    final_min_spread_ticks       → min_spread_ticks
    final_tfi_threshold          → tfi_threshold
    final_obi_threshold          → obi_threshold
    final_max_active_tranches    → max_active_tranches
    final_grid_offset_ticks      → grid_offset_ticks  [V2.2]
    """
    channel = f"hft:live_params:{symbol.lower()}"
    payload = {
        "gamma":               float(cro_output.get("final_gamma",               0.8)),
        "min_spread_ticks":    float(cro_output.get("final_min_spread_ticks",    5.0)),
        "tfi_threshold":       float(cro_output.get("final_tfi_threshold",       65_000.0)),
        "obi_threshold":       float(cro_output.get("final_obi_threshold",       1.0)),
        "max_active_tranches": int(cro_output.get("final_max_active_tranches",   1)),
        "grid_offset_ticks":   float(cro_output.get("final_grid_offset_ticks",   2.0)),
        "system_status":       "LIVE",
    }
    r = _get_redis()
    r.publish(channel, json.dumps(payload))
    r.set(f"{channel}:latest", json.dumps(payload))
    log.info("[%s] Params → %s: γ=%.2f spread=%.1f tfi=%.0f obi=%.2f tranches=%d offset=%.1f",
             symbol, channel,
             payload["gamma"], payload["min_spread_ticks"], payload["tfi_threshold"],
             payload["obi_threshold"], payload["max_active_tranches"], payload["grid_offset_ticks"])


# ── Dead-Man's Switch ─────────────────────────────────────────────────────────

def publish_safe_mode(symbol: str) -> None:
    """
    Emergency lockdown: forces Rust engine to wide spread + inventory dump.
    Published on ANY unhandled exception in Oracle or Tactical loop.
    V2.2: grid_offset_ticks=2.0 (safe default spacing).
    """
    channel = f"hft:live_params:{symbol.lower()}"
    payload = {
        "gamma":               0.9,
        "min_spread_ticks":    20.0,
        "tfi_threshold":       0.1,
        "obi_threshold":       0.3,
        "max_active_tranches": 1,      # Strict ping-pong in safe mode
        "grid_offset_ticks":   2.0,    # Safe default spacing
        "system_status":       "SAFE_MODE_LOCKDOWN",
    }
    r = _get_redis()
    r.publish(channel, json.dumps(payload))
    r.set(f"{channel}:latest", json.dumps(payload))
    log.warning("[%s] ⚠ SAFE MODE LOCKDOWN → %s", symbol, channel)
