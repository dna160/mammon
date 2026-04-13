"""
Agent Q v3 — Hybrid Hierarchical Optimizer (Master Orchestrator).

Three loops run concurrently on daemon threads:

  Watcher Loop  (every 1 sec)  — data_pipeline.py
    Continuously samples LOB ticks from Redis into the per-symbol rolling window
    (≤300 points ≈ 5 min of data at 1 sample/sec), computes the 5-metric
    State Vector, and publishes structured JSON to:
        cognitive:state_vector:{symbol.lower()}
    PRD §4: "A Python 'Watcher' must compress millions of order book ticks
    into a 5-line semantic State Vector (Basis Points, Z-Scores) before the
    AI is invoked."  — this loop IS that watcher.

  Oracle Loop   (every 5 min)  — regime_classifier.py
    Reads the fully-populated rolling window from the Watcher, calls LLM to
    classify macro regime → hft:regime:{symbol}
    Rust engine adopts structural quoting playbook instantly.

  Tactical Loop (every 3 min) — parameter_tuner.py  [V2.2 cadence]
    Reads active regime from Redis.
    Adversarial Alpha/CRO pipeline with Dynamic Bounding Matrix + RAG memory.
    Publishes tuned params (gamma, spread, tfi, obi_threshold, max_active_tranches,
    grid_offset_ticks) → hft:live_params:{symbol}.
    Logs decision to agent_q_memory for 3-min reward back-fill.

Dead-Man's Switch: any unhandled exception in Oracle or Tactical triggers
SAFE_MODE_LOCKDOWN on all affected symbols.
"""
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from data_pipeline     import get_state_vector
from regime_classifier import classify_regime_for_symbol
from parameter_tuner   import tune_parameters_for_symbol
from redis_bridge      import publish_safe_mode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("agent_q")

ORACLE_INTERVAL_S   = int(os.getenv("ORACLE_INTERVAL_S",   str(5 * 60)))    # 5  min
TACTICAL_INTERVAL_S = int(os.getenv("TACTICAL_INTERVAL_S", str(3 * 60)))    # 3  min (V2.2 cadence mandate)
WATCHER_INTERVAL_S  = float(os.getenv("WATCHER_INTERVAL_S", "1.0"))         # 1  sec

ACTIVE_SYMBOLS = [
    s.strip()
    for s in os.getenv(
        "ACTIVE_SYMBOLS",
        "SOLFDUSD,XRPFDUSD,DOGEFDUSD,ETHFDUSD,BNBFDUSD",
    ).split(",")
    if s.strip()
]


# ── Watcher Loop (every 1 sec) — PRD §4 ──────────────────────────────────────
# Continuously samples Redis LOB ticks into the rolling 300-point window so
# that the Oracle/Tactical LLM cycles always see a fully-populated State Vector
# with meaningful BPS / Z-Score metrics (not all-zeros from a 1-sample window).

def watcher_loop() -> None:
    log.info(
        "Watcher loop started — interval=%.1fs, symbols=%s",
        WATCHER_INTERVAL_S, ACTIVE_SYMBOLS,
    )
    tick_count = 0
    while True:
        t0 = time.monotonic()
        for sym in ACTIVE_SYMBOLS:
            try:
                # get_state_vector() appends the latest Redis tick to the rolling
                # window AND publishes the structured JSON to Redis so the
                # dashboard can display it live.
                get_state_vector(sym, window_minutes=5)
            except Exception as exc:
                log.debug("[%s][Watcher] tick failed: %s", sym, exc)

        tick_count += 1
        if tick_count % 60 == 0:
            log.info("[Watcher] %d ticks sampled for each of %s", tick_count, ACTIVE_SYMBOLS)

        elapsed   = time.monotonic() - t0
        sleep_for = max(0.0, WATCHER_INTERVAL_S - elapsed)
        time.sleep(sleep_for)


# ── Oracle Loop (every 5 min) ─────────────────────────────────────────────────

def _run_oracle_cycle() -> None:
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


# ── Tactical Loop (every 1 min) ───────────────────────────────────────────────

def _run_tactical_cycle() -> None:
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
    log.info("Active symbols    : %s", ACTIVE_SYMBOLS)
    log.info("Watcher interval  : %.1fs (continuous tick sampler)", WATCHER_INTERVAL_S)
    log.info("Oracle interval   : %ds (%dm)", ORACLE_INTERVAL_S,   ORACLE_INTERVAL_S   // 60)
    log.info("Tactical interval : %ds (%dm)", TACTICAL_INTERVAL_S, TACTICAL_INTERVAL_S // 60)

    # Watcher — daemon thread, runs every 1 second forever.
    # Must start BEFORE Oracle/Tactical so the window is pre-warmed.
    watcher_thread = threading.Thread(target=watcher_loop, name="WatcherLoop", daemon=True)
    watcher_thread.start()
    log.info("Watcher thread launched — pre-warming rolling window (300 ticks ≈ 5 min).")

    # Give watcher 3 seconds of head start before Oracle fires
    time.sleep(3)

    # Oracle runs on a daemon thread.
    oracle_thread = threading.Thread(target=oracle_loop, name="OracleLoop", daemon=True)
    oracle_thread.start()
    log.info("Oracle thread launched.")

    # Tactical loop runs on the main thread (keeps process alive).
    tactical_loop()


if __name__ == "__main__":
    main()
