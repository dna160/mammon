"""
Agent Q v3 — Hybrid Hierarchical Optimizer (Master Orchestrator).

Two loops run concurrently on separate threads:

  Oracle Loop   (every 5 min)  — regime_classifier.py
    Classifies macro regime → hft:regime:{symbol}
    Rust engine adopts structural quoting playbook instantly.

  Tactical Loop (every 15 min) — parameter_tuner.py
    Reads active regime from Redis.
    Adversarial Alpha/CRO pipeline with Dynamic Bounding Matrix.
    Publishes tuned params → hft:live_params:{symbol}.

Dead-Man's Switch: any unhandled exception in either loop triggers
SAFE_MODE_LOCKDOWN on all affected symbols.
"""
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from regime_classifier import classify_regime_for_symbol
from parameter_tuner   import tune_parameters_for_symbol
from redis_bridge      import publish_safe_mode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("agent_q")

ORACLE_INTERVAL_S   = int(os.getenv("ORACLE_INTERVAL_S",   str(5  * 60)))   # 5  min
TACTICAL_INTERVAL_S = int(os.getenv("TACTICAL_INTERVAL_S", str(15 * 60)))   # 15 min
ACTIVE_SYMBOLS = [
    s.strip()
    for s in os.getenv(
        "ACTIVE_SYMBOLS",
        "ADAFDUSD,DOTFDUSD,DOGEFDUSD,XRPFDUSD",
    ).split(",")
    if s.strip()
]


# ── Oracle Loop (every 5 min) ─────────────────────────────────────────────────

def _run_oracle_cycle() -> None:
    # Process all symbols concurrently — 2 workers maps to the 2 loaded LM Studio models.
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="Oracle") as pool:
        futures = {pool.submit(classify_regime_for_symbol, sym): sym for sym in ACTIVE_SYMBOLS}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                fut.result()
            except Exception as exc:
                log.error("[%s][Oracle] Failed: %s — publishing safe mode.", sym, exc, exc_info=True)
                try:
                    publish_safe_mode(sym)
                except Exception as e2:
                    log.critical("[%s][Oracle] Safe-mode publish also failed: %s", sym, e2)


def oracle_loop() -> None:
    log.info("Oracle loop started — interval=%ds, symbols=%s", ORACLE_INTERVAL_S, ACTIVE_SYMBOLS)
    while True:
        t0 = time.monotonic()
        try:
            _run_oracle_cycle()
        except Exception as exc:
            log.critical("[Oracle] Catastrophic failure: %s", exc, exc_info=True)
            for sym in ACTIVE_SYMBOLS:
                try:
                    publish_safe_mode(sym)
                except Exception:
                    pass
        elapsed   = time.monotonic() - t0
        sleep_for = max(0.0, ORACLE_INTERVAL_S - elapsed)
        log.info("[Oracle] Cycle complete in %.1fs — sleeping %.0fs.", elapsed, sleep_for)
        time.sleep(sleep_for)


# ── Tactical Loop (every 15 min) ──────────────────────────────────────────────

def _run_tactical_cycle() -> None:
    # 2 symbols run concurrently: while one is at Agent-1/Alpha (model 1),
    # the other can be at Agent-2/CRO (model 2) — true parallel inference.
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="Tactical") as pool:
        futures = {pool.submit(tune_parameters_for_symbol, sym): sym for sym in ACTIVE_SYMBOLS}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                fut.result()
            except Exception as exc:
                log.error("[%s][Tactical] Failed: %s — publishing safe mode.", sym, exc, exc_info=True)
                try:
                    publish_safe_mode(sym)
                except Exception as e2:
                    log.critical("[%s][Tactical] Safe-mode publish also failed: %s", sym, e2)


def tactical_loop() -> None:
    log.info("Tactical loop started — interval=%ds, symbols=%s", TACTICAL_INTERVAL_S, ACTIVE_SYMBOLS)
    while True:
        t0 = time.monotonic()
        try:
            _run_tactical_cycle()
        except Exception as exc:
            log.critical("[Tactical] Catastrophic failure: %s", exc, exc_info=True)
            for sym in ACTIVE_SYMBOLS:
                try:
                    publish_safe_mode(sym)
                except Exception:
                    pass
        elapsed   = time.monotonic() - t0
        sleep_for = max(0.0, TACTICAL_INTERVAL_S - elapsed)
        log.info("[Tactical] Cycle complete in %.1fs — sleeping %.0fs.", elapsed, sleep_for)
        time.sleep(sleep_for)


# ── Entry Point ───────────────────────────────────────────────────────────────

def main() -> None:
    log.info("Agent Q v3 — Hybrid Hierarchical Optimizer starting.")
    log.info("Active symbols  : %s", ACTIVE_SYMBOLS)
    log.info("Oracle interval : %ds (%dm)", ORACLE_INTERVAL_S,   ORACLE_INTERVAL_S   // 60)
    log.info("Tactical interval: %ds (%dm)", TACTICAL_INTERVAL_S, TACTICAL_INTERVAL_S // 60)

    # Oracle runs on a daemon thread — exits when main thread exits.
    oracle_thread = threading.Thread(target=oracle_loop, name="OracleLoop", daemon=True)
    oracle_thread.start()
    log.info("Oracle thread launched.")

    # Tactical loop runs on the main thread.
    tactical_loop()


if __name__ == "__main__":
    main()
