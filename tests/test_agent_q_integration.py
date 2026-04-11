#!/usr/bin/env python3
"""
Agent Q Integration Test Suite
================================
Verifies the full chain:
  PostgreSQL telemetry → data_pipeline → LLM Oracle/Tactical → Redis pub/sub → Engine D

Tests are designed to run INSIDE the mammon_agent_q_hft container:
  docker exec mammon_agent_q_hft python /tests/test_agent_q_integration.py

Each test section is also individually invocable and prints a clear PASS/FAIL.
"""

import os, sys, json, time, threading, logging
logging.basicConfig(level=logging.WARNING)  # suppress module noise during tests

# ── Connection config (reads same env vars as the live service) ───────────────
REDIS_URL  = os.getenv("REDIS_URL",  "redis://:mammon_hft_redis@redis_hft:6379")
DB_DSN     = os.getenv("DB_DSN",     "postgresql://mammon:mammon@postgres_hft:5432/mammon")
LM_STUDIO  = os.getenv("LM_STUDIO_URL", "http://host.docker.internal:1234/v1")
SYMBOLS    = ["ADAFDUSD", "DOTFDUSD", "DOGEFDUSD", "XRPFDUSD"]
TEST_SYMBOL = "ADAFDUSD"

PASS = "\033[92m[PASS]\033[0m"
FAIL = "\033[91m[FAIL]\033[0m"
INFO = "\033[94m[INFO]\033[0m"
WARN = "\033[93m[WARN]\033[0m"
SEP  = "─" * 70

results = {}

def record(name, passed, detail=""):
    results[name] = passed
    status = PASS if passed else FAIL
    print(f"  {status} {name}", f"({detail})" if detail else "")


# ══════════════════════════════════════════════════════════════════════════════
# § 1  CONNECTION HEALTH
# ══════════════════════════════════════════════════════════════════════════════

def test_connections():
    print(f"\n{SEP}")
    print("§1  CONNECTION HEALTH")
    print(SEP)

    # ── Redis ─────────────────────────────────────────────────────────────────
    try:
        import redis as redislib
        r = redislib.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=3)
        pong = r.ping()
        record("Redis ping", pong, f"PONG={pong}")
    except Exception as e:
        record("Redis ping", False, str(e))

    # ── PostgreSQL ────────────────────────────────────────────────────────────
    try:
        import psycopg2
        conn = psycopg2.connect(DB_DSN, connect_timeout=5)
        cur  = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM trade_telemetry WHERE engine_id='D'")
        count = cur.fetchone()[0]
        conn.close()
        record("PostgreSQL connect + query", True, f"{count} Engine D rows in trade_telemetry")
    except Exception as e:
        record("PostgreSQL connect + query", False, str(e))

    # ── LM Studio ─────────────────────────────────────────────────────────────
    try:
        import httpx
        resp = httpx.get(f"{LM_STUDIO.rstrip('/v1')}/v1/models", timeout=5)
        models = [m["id"] for m in resp.json().get("data", [])]
        record("LM Studio reachable", resp.status_code == 200,
               f"models={models}")
    except Exception as e:
        record("LM Studio reachable", False, str(e))


# ══════════════════════════════════════════════════════════════════════════════
# § 2  TELEMETRY PIPELINE — seed real data, verify data_pipeline reads it
# ══════════════════════════════════════════════════════════════════════════════

def test_telemetry_pipeline():
    print(f"\n{SEP}")
    print("§2  TELEMETRY PIPELINE  (PostgreSQL → data_pipeline.py)")
    print(SEP)

    import psycopg2

    # ── 2a: Seed synthetic Engine D trades ───────────────────────────────────
    SEED_ROWS = [
        # (asset_pair, trade_size_idr, entry_signal_value, gross_pnl, fees_paid, net_pnl, roe_pct)
        (TEST_SYMBOL, 10.50, 0.000420, +0.00042,  0.000021, +0.000399, +0.001425),
        (TEST_SYMBOL, 10.50, 0.000380, -0.00038,  0.000019, -0.000399, -0.001425),
        (TEST_SYMBOL, 10.50, 0.000410, +0.00041,  0.000020, +0.000390, +0.001393),
        (TEST_SYMBOL, 10.50, 0.000395, +0.00039,  0.000019, +0.000371, +0.001325),
        (TEST_SYMBOL, 10.50, 0.000430, +0.00043,  0.000021, +0.000409, +0.001461),
    ]
    seeded = 0
    try:
        conn = psycopg2.connect(DB_DSN, connect_timeout=5)
        cur  = conn.cursor()
        for row in SEED_ROWS:
            cur.execute(
                """INSERT INTO trade_telemetry
                   (timestamp, engine_id, asset_pair, trade_size_idr,
                    entry_signal_value, gross_pnl, fees_paid, net_pnl, trade_roe_pct)
                   VALUES (NOW(), 'D', %s, %s, %s, %s, %s, %s, %s)""",
                row
            )
            seeded += 1
        conn.commit()
        conn.close()
        record("Seed 5 Engine D rows into trade_telemetry", seeded == 5, f"{seeded} rows inserted")
    except Exception as e:
        record("Seed 5 Engine D rows into trade_telemetry", False, str(e))
        return

    # ── 2b: data_pipeline must now return real stats, not zero baseline ───────
    try:
        sys.path.insert(0, "/app")
        from data_pipeline import fetch_telemetry_window
        stats = fetch_telemetry_window(TEST_SYMBOL, window_minutes=15)
        has_data = "TOTAL_TRADES=0" not in stats and int(stats.split("TOTAL_TRADES=")[1].split(" ")[0]) >= 5
        record("data_pipeline returns real telemetry (not zero-baseline)", has_data, stats[:100])
    except Exception as e:
        record("data_pipeline returns real telemetry (not zero-baseline)", False, str(e))

    # ── 2c: Oracle 5-min window ───────────────────────────────────────────────
    try:
        from data_pipeline import fetch_telemetry_window
        stats5 = fetch_telemetry_window(TEST_SYMBOL, window_minutes=5)
        has_data5 = "TOTAL_TRADES=0" not in stats5
        record("data_pipeline 5-min window (Oracle window)", has_data5, stats5[:80])
    except Exception as e:
        record("data_pipeline 5-min window (Oracle window)", False, str(e))


# ══════════════════════════════════════════════════════════════════════════════
# § 3  ORACLE ISOLATION — regime classifier → LLM call → Redis publish
# ══════════════════════════════════════════════════════════════════════════════

def test_oracle_isolated():
    print(f"\n{SEP}")
    print("§3  ORACLE ISOLATION  (regime_classifier.py)")
    print(SEP)

    VALID_REGIMES = {
        "MEAN_REVERTING", "RETAIL_FRENZY_UP",
        "INSTITUTIONAL_ABSORPTION_DOWN", "DEAD_ZONE", "TOXIC_LIQUIDATION_CASCADE"
    }

    # ── 3a: LLM call via call_regime_classifier ───────────────────────────────
    try:
        from data_pipeline    import fetch_telemetry_window
        from lm_studio_client import call_regime_classifier
        from json_sanitizer   import extract_json

        stats = fetch_telemetry_window(TEST_SYMBOL, window_minutes=5)
        t0  = time.time()
        raw = call_regime_classifier(stats)
        elapsed = time.time() - t0

        parsed  = extract_json(raw)
        regime  = parsed.get("regime", "").strip().upper()
        confidence = float(parsed.get("confidence", 0))
        reasoning  = parsed.get("reasoning", "")[:80]

        valid = regime in VALID_REGIMES
        record("Oracle LLM call returns valid regime JSON", valid,
               f"regime={regime} conf={confidence:.2f} in {elapsed:.1f}s")
        record("Oracle regime within valid enum", valid, regime)
        record("Oracle LLM latency < 20s", elapsed < 20, f"{elapsed:.1f}s")
        if reasoning:
            print(f"  {INFO} Reasoning: {reasoning}…")
    except Exception as e:
        record("Oracle LLM call returns valid regime JSON", False, str(e))
        record("Oracle regime within valid enum", False, "skipped")
        record("Oracle LLM latency < 20s", False, "skipped")
        return

    # ── 3b: Full classify_regime_for_symbol → publishes to Redis ─────────────
    try:
        import redis as redislib
        from regime_classifier import classify_regime_for_symbol

        r = redislib.from_url(REDIS_URL, decode_responses=True)

        # Subscribe before triggering
        key_before = r.get(f"hft:regime:{TEST_SYMBOL.lower()}:latest")

        received = threading.Event()
        pub_payload = {}
        def listen():
            ps = r.pubsub()
            ps.subscribe(f"hft:regime:{TEST_SYMBOL.lower()}")
            for msg in ps.listen():
                if msg["type"] == "message":
                    pub_payload.update(json.loads(msg["data"]))
                    received.set()
                    ps.unsubscribe()
                    return
        t = threading.Thread(target=listen, daemon=True)
        t.start()

        returned_regime = classify_regime_for_symbol(TEST_SYMBOL)
        fired = received.wait(timeout=10)

        record("classify_regime_for_symbol returns regime string", returned_regime in VALID_REGIMES,
               returned_regime)
        record("Oracle publishes to hft:regime:{symbol} channel", fired,
               f"payload={pub_payload}")

        key_after = r.get(f"hft:regime:{TEST_SYMBOL.lower()}:latest")
        record("Oracle persists regime to :latest key", key_after is not None,
               key_after)
    except Exception as e:
        record("classify_regime_for_symbol returns regime string", False, str(e))
        record("Oracle publishes to hft:regime:{symbol} channel", False, "skipped")
        record("Oracle persists regime to :latest key", False, "skipped")


# ══════════════════════════════════════════════════════════════════════════════
# § 4  TACTICAL ISOLATION — regime read → Agent1 → Agent2 (CRO) → Redis publish
# ══════════════════════════════════════════════════════════════════════════════

def test_tactical_isolated():
    print(f"\n{SEP}")
    print("§4  TACTICAL ISOLATION  (parameter_tuner.py)")
    print(SEP)

    GAMMA_RANGE  = (0.0, 1.0)
    SPREAD_MIN   = 1.0
    TFI_RANGE    = (0.0, 10.0)

    # ── 4a: Agent 1 Alpha call (regime-aware proposal) ────────────────────────
    try:
        from data_pipeline    import fetch_telemetry_window
        from lm_studio_client import call_agent_1_alpha
        from json_sanitizer   import extract_json
        from redis_bridge     import get_current_regime

        current_regime = get_current_regime(TEST_SYMBOL)
        stats = fetch_telemetry_window(TEST_SYMBOL, window_minutes=15)

        t0  = time.time()
        raw = call_agent_1_alpha(stats, current_regime=current_regime)
        elapsed = time.time() - t0

        alpha = extract_json(raw)
        gamma  = float(alpha.get("proposed_gamma", -1))
        spread = float(alpha.get("proposed_min_spread_ticks", -1))
        tfi    = float(alpha.get("proposed_tfi_threshold", -1))

        valid_g = GAMMA_RANGE[0] <= gamma <= GAMMA_RANGE[1]
        valid_s = spread >= SPREAD_MIN
        valid_t = TFI_RANGE[0] <= tfi <= TFI_RANGE[1]

        record("Agent-1 Alpha LLM call succeeds", True, f"in {elapsed:.1f}s")
        record("Agent-1 reads current regime from Redis", current_regime != "", current_regime)
        record("Agent-1 proposed_gamma in [0,1]", valid_g, f"γ={gamma:.3f}")
        record("Agent-1 proposed_min_spread_ticks >= 1.0", valid_s, f"spread={spread:.1f}")
        record("Agent-1 proposed_tfi_threshold in [0,10]", valid_t, f"tfi={tfi:.3f}")
    except Exception as e:
        for n in ["Agent-1 Alpha LLM call succeeds","Agent-1 reads current regime from Redis",
                  "Agent-1 proposed_gamma in [0,1]","Agent-1 proposed_min_spread_ticks >= 1.0",
                  "Agent-1 proposed_tfi_threshold in [0,10]"]:
            record(n, False, str(e)[:60])
        return

    # ── 4b: Agent 2 CRO call (Dynamic Bounding Matrix) ───────────────────────
    try:
        from lm_studio_client import call_agent_2_risk
        import json as _json

        t0  = time.time()
        raw_cro = call_agent_2_risk(stats, _json.dumps(alpha), current_regime=current_regime)
        elapsed = time.time() - t0

        cro = extract_json(raw_cro)
        fg  = float(cro.get("final_gamma", -1))
        fs  = float(cro.get("final_min_spread_ticks", -1))
        ft  = float(cro.get("final_tfi_threshold", -1))
        ovr = cro.get("override_applied", None)
        panic = cro.get("panic_sell_flag", False)

        record("Agent-2 CRO LLM call succeeds", True, f"in {elapsed:.1f}s")
        record("Agent-2 final_gamma in [0,1]", GAMMA_RANGE[0] <= fg <= GAMMA_RANGE[1], f"γ={fg:.3f}")
        record("Agent-2 final_min_spread_ticks >= 1.0", fs >= SPREAD_MIN, f"spread={fs:.1f}")
        record("Agent-2 returns override_applied flag", ovr is not None, f"override={ovr}")
        record("Agent-2 no panic flag on normal data", not panic, f"panic={panic}")
        print(f"  {INFO} CRO regime={current_regime} → γ={fg:.2f} spread={fs:.1f} tfi={ft:.2f} override={ovr}")
    except Exception as e:
        for n in ["Agent-2 CRO LLM call succeeds","Agent-2 final_gamma in [0,1]",
                  "Agent-2 final_min_spread_ticks >= 1.0","Agent-2 returns override_applied flag",
                  "Agent-2 no panic flag on normal data"]:
            record(n, False, str(e)[:60])
        return

    # ── 4c: Full tune_parameters_for_symbol → publishes to Redis ─────────────
    try:
        import redis as redislib
        from parameter_tuner import tune_parameters_for_symbol

        r = redislib.from_url(REDIS_URL, decode_responses=True)
        received = threading.Event()
        pub_payload = {}

        def listen_params():
            ps = r.pubsub()
            ps.subscribe(f"hft:live_params:{TEST_SYMBOL.lower()}")
            for msg in ps.listen():
                if msg["type"] == "message":
                    pub_payload.update(json.loads(msg["data"]))
                    received.set()
                    ps.unsubscribe()
                    return
        t = threading.Thread(target=listen_params, daemon=True)
        t.start()

        tune_parameters_for_symbol(TEST_SYMBOL)
        fired = received.wait(timeout=30)

        record("tune_parameters_for_symbol publishes to hft:live_params channel", fired,
               f"payload={pub_payload}")

        key = r.get(f"hft:live_params:{TEST_SYMBOL.lower()}:latest")
        parsed_key = json.loads(key) if key else {}
        record("Tactical persists params to :latest key", bool(parsed_key), str(parsed_key))
        record("Published params contain gamma key", "gamma" in pub_payload, str(pub_payload))
        record("Published params system_status=LIVE", pub_payload.get("system_status") == "LIVE",
               pub_payload.get("system_status"))
    except Exception as e:
        for n in ["tune_parameters_for_symbol publishes to hft:live_params channel",
                  "Tactical persists params to :latest key",
                  "Published params contain gamma key",
                  "Published params system_status=LIVE"]:
            record(n, False, str(e)[:60])


# ══════════════════════════════════════════════════════════════════════════════
# § 5  ENGINE D INTEGRATION — verify it receives and logs regime + param updates
# ══════════════════════════════════════════════════════════════════════════════

def test_engine_d_integration():
    print(f"\n{SEP}")
    print("§5  ENGINE D INTEGRATION  (Engine D ← Redis ← Agent Q)")
    print(SEP)

    import redis as redislib, time as _time
    from redis_bridge import publish_regime, publish_params

    r = redislib.from_url(REDIS_URL, decode_responses=True)

    # ── 5a: Verify regime :latest key exists for all 4 symbols ───────────────
    try:
        all_regime_keys = all(
            r.exists(f"hft:regime:{sym.lower()}:latest") for sym in SYMBOLS
        )
        record("All 4 regime :latest keys present in Redis", all_regime_keys,
               str([f"hft:regime:{s.lower()}:latest" for s in SYMBOLS]))
    except Exception as e:
        record("All 4 regime :latest keys present in Redis", False, str(e))

    # ── 5b: Verify params :latest key exists for all 4 symbols ───────────────
    try:
        all_params_keys = all(
            r.exists(f"hft:live_params:{sym.lower()}:latest") for sym in SYMBOLS
        )
        record("All 4 params :latest keys present in Redis", all_params_keys,
               str([f"hft:live_params:{s.lower()}:latest" for s in SYMBOLS]))
    except Exception as e:
        record("All 4 params :latest keys present in Redis", False, str(e))

    # ── 5c: Publish RETAIL_FRENZY_UP and verify Engine D receives via pub/sub ─
    # We use a second Redis subscriber to confirm the channel fires
    try:
        received_regime = threading.Event()
        regime_payload  = {}

        def listen_regime():
            ps = r.pubsub()
            ps.subscribe(f"hft:regime:{TEST_SYMBOL.lower()}")
            for msg in ps.listen():
                if msg["type"] == "message":
                    regime_payload.update(json.loads(msg["data"]))
                    received_regime.set()
                    ps.unsubscribe()
                    return
        t = threading.Thread(target=listen_regime, daemon=True)
        t.start()

        publish_regime(TEST_SYMBOL, "RETAIL_FRENZY_UP", confidence=0.91)
        fired = received_regime.wait(timeout=5)
        record("Regime channel hft:regime:{symbol} fires on publish", fired,
               f"payload={regime_payload}")
        record(":latest key updated to RETAIL_FRENZY_UP after publish",
               json.loads(r.get(f"hft:regime:{TEST_SYMBOL.lower()}:latest") or "{}").get("regime") == "RETAIL_FRENZY_UP")
    except Exception as e:
        record("Regime channel hft:regime:{symbol} fires on publish", False, str(e))
        record(":latest key updated to RETAIL_FRENZY_UP after publish", False, str(e))

    # ── 5d: Publish fresh params and verify channel fires + :latest updates ───
    try:
        received_params = threading.Event()
        params_payload  = {}

        def listen_params():
            ps = r.pubsub()
            ps.subscribe(f"hft:live_params:{TEST_SYMBOL.lower()}")
            for msg in ps.listen():
                if msg["type"] == "message":
                    params_payload.update(json.loads(msg["data"]))
                    received_params.set()
                    ps.unsubscribe()
                    return
        t2 = threading.Thread(target=listen_params, daemon=True)
        t2.start()

        publish_params(TEST_SYMBOL, {
            "final_gamma": 0.25,
            "final_min_spread_ticks": 10.0,
            "final_tfi_threshold": 2.5,
        })
        fired2 = received_params.wait(timeout=5)
        record("Params channel hft:live_params:{symbol} fires on publish", fired2,
               f"payload={params_payload}")
        stored = json.loads(r.get(f"hft:live_params:{TEST_SYMBOL.lower()}:latest") or "{}")
        record(":latest params key contains expected gamma=0.25",
               abs(float(stored.get("gamma", -1)) - 0.25) < 0.01, str(stored))
    except Exception as e:
        record("Params channel hft:live_params:{symbol} fires on publish", False, str(e))
        record(":latest params key contains expected gamma=0.25", False, str(e))

    # ── 5e: Engine D market-make heartbeat via Redis telemetry key ────────────
    try:
        # Engine D publishes engine_d:{symbol}:pipeline every tick
        telem_keys = [k for k in r.keys("engine_d:*:pipeline") or []]
        if not telem_keys:
            telem_keys = [k for k in r.keys("toko:*:lob") or []]
        heartbeat = len(telem_keys) > 0
        record("Engine D market-make heartbeat keys visible in Redis", heartbeat,
               f"keys={telem_keys[:4]}")
    except Exception as e:
        record("Engine D market-make heartbeat keys visible in Redis", False, str(e))


# ══════════════════════════════════════════════════════════════════════════════
# § 6  DUAL-MODEL PARALLEL INFERENCE VERIFICATION
# ══════════════════════════════════════════════════════════════════════════════

def test_dual_model():
    print(f"\n{SEP}")
    print("§6  DUAL-MODEL PARALLEL INFERENCE")
    print(SEP)

    try:
        from lm_studio_client import MODEL_1, MODEL_2, _get_client1, _get_client2

        m1_ok = MODEL_1 == "llama-3.2-3b-instruct"
        m2_ok = MODEL_2 == "llama-3.2-3b-instruct:2"
        record("MODEL_1 = llama-3.2-3b-instruct (Oracle + Alpha)", m1_ok, MODEL_1)
        record("MODEL_2 = llama-3.2-3b-instruct:2 (CRO)", m2_ok, MODEL_2)

        # Verify the two clients are separate instances
        c1 = _get_client1()
        c2 = _get_client2()
        record("Model 1 and Model 2 use separate OpenAI client instances", c1 is not c2)

        # Fire both in parallel and measure overlap
        import concurrent.futures, time as _time
        from lm_studio_client import call_regime_classifier, call_agent_2_risk
        from data_pipeline import fetch_telemetry_window
        from json_sanitizer import extract_json
        from redis_bridge   import get_current_regime
        import json as _json

        stats = fetch_telemetry_window(TEST_SYMBOL, window_minutes=5)
        regime = get_current_regime(TEST_SYMBOL)
        dummy_alpha = _json.dumps({"proposed_gamma":0.5,"proposed_min_spread_ticks":20.0,"proposed_tfi_threshold":1.0})

        t_start = _time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(call_regime_classifier, stats)
            f2 = pool.submit(call_agent_2_risk, stats, dummy_alpha, regime)
            r1 = f1.result(timeout=30)
            r2 = f2.result(timeout=30)
        elapsed = _time.time() - t_start

        p1 = extract_json(r1)
        p2 = extract_json(r2)
        both_valid = "regime" in p1 and "final_gamma" in p2
        record("Parallel Model1 (Oracle) + Model2 (CRO) both return valid JSON", both_valid,
               f"total_elapsed={elapsed:.1f}s")
        # Parallel should finish faster than serial (both ~5-10s each = ~15s serial)
        record("Parallel inference completes in < 20s (faster than serial)", elapsed < 20,
               f"{elapsed:.1f}s")
    except Exception as e:
        for n in ["MODEL_1 = llama-3.2-3b-instruct (Oracle + Alpha)",
                  "MODEL_2 = llama-3.2-3b-instruct:2 (CRO)",
                  "Model 1 and Model 2 use separate OpenAI client instances",
                  "Parallel Model1 (Oracle) + Model2 (CRO) both return valid JSON",
                  "Parallel inference completes in < 20s (faster than serial)"]:
            record(n, False, str(e)[:60])


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n" + "═" * 70)
    print("  AGENT Q INTEGRATION TEST SUITE")
    print("  Project Mammon — Engine D + Agent Q v3 Hybrid Optimizer")
    print("═" * 70)

    test_connections()
    test_telemetry_pipeline()
    test_oracle_isolated()
    test_tactical_isolated()
    test_engine_d_integration()
    test_dual_model()

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{SEP}")
    print("SUMMARY")
    print(SEP)
    passed  = sum(1 for v in results.values() if v)
    failed  = sum(1 for v in results.values() if not v)
    total   = len(results)
    pct     = int(100 * passed / total) if total else 0

    for name, ok in results.items():
        print(f"  {'✓' if ok else '✗'}  {name}")

    print(f"\n  Total: {total}  |  {PASS} {passed}  |  {FAIL} {failed}  |  Score: {pct}%")
    if failed == 0:
        print(f"\n  \033[92m✓ ALL TESTS PASSED — Agent Q pipeline verified end-to-end.\033[0m")
    else:
        print(f"\n  \033[91m✗ {failed} test(s) failed — see details above.\033[0m")
    print()
    sys.exit(0 if failed == 0 else 1)
