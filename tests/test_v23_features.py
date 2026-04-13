#!/usr/bin/env python3
"""
V2.3 Feature Unit Tests — Microstructure Skew & True Taker Bailout
==================================================================
Tests the V2.3 logic without requiring Docker/Redis/Postgres.
Runs standalone with: python tests/test_v23_features.py

Covers:
  1. OBI microstructure skew: obi_price_warp = obi × 5 × tick_size
  2. True Taker Bailout: emergency_dump_triggered when ticks_held > 1200
  3. Break-even zone: ticks_held 600-1200 accepts last_fill_price
  4. data_pipeline 4-tuple rolling window (price, tfi, spread, obi)
  5. State vector line 6 (obi_mean) and line 7 (ticks_held)
  6. agent_1_alpha.txt contains bailout guidance
  7. agent_2_risk.txt OBI threshold calibration rule
"""

import os, sys, json, math, time

PASS = "\033[92m[PASS]\033[0m"
FAIL = "\033[91m[FAIL]\033[0m"
INFO = "\033[94m[INFO]\033[0m"
SEP  = "─" * 70

results = {}

def record(name, passed, detail=""):
    results[name] = passed
    status = PASS if passed else FAIL
    print(f"  {status} {name}", f"({detail})" if detail else "")


# ══════════════════════════════════════════════════════════════════════════════
# § 1  OBI MICROSTRUCTURE SKEW — Python simulation of Rust F5 formula
# ══════════════════════════════════════════════════════════════════════════════

def simulate_reservation_price(
    micro_price, inventory_coin, live_gamma, variance,
    live_max_tranches, obi, tick_size
):
    """Mirrors the Rust F5 calculation in engine_d_hft.rs (V2.3)."""
    neutral_inventory   = (live_max_tranches * (6.0 / max(micro_price, 1e-9))) / 2.0
    inventory_risk_skew = inventory_coin - neutral_inventory
    obi_price_warp      = obi * 5.0 * tick_size
    reservation_price   = micro_price - (inventory_risk_skew * live_gamma * variance) + obi_price_warp
    return reservation_price, obi_price_warp


def test_obi_skew():
    print(f"\n{SEP}")
    print("§1  OBI MICROSTRUCTURE SKEW (F5 formula verification)")
    print(SEP)

    tick_size = 0.01  # SOL-FDUSD

    # Test 1: Positive OBI (bid-heavy) should RAISE reservation price
    r_neutral, warp_neutral = simulate_reservation_price(
        micro_price=150.00, inventory_coin=0.0, live_gamma=0.5, variance=0.01,
        live_max_tranches=5, obi=0.0, tick_size=tick_size
    )
    r_bid_heavy, warp_bid = simulate_reservation_price(
        micro_price=150.00, inventory_coin=0.0, live_gamma=0.5, variance=0.01,
        live_max_tranches=5, obi=0.6, tick_size=tick_size
    )
    record(
        "OBI=+0.6 raises reservation price above neutral",
        r_bid_heavy > r_neutral,
        f"neutral={r_neutral:.4f} bid_heavy={r_bid_heavy:.4f} warp={warp_bid:.4f}"
    )

    # Test 2: Negative OBI (ask-heavy) should LOWER reservation price
    r_ask_heavy, warp_ask = simulate_reservation_price(
        micro_price=150.00, inventory_coin=0.0, live_gamma=0.5, variance=0.01,
        live_max_tranches=5, obi=-0.6, tick_size=tick_size
    )
    record(
        "OBI=-0.6 lowers reservation price below neutral",
        r_ask_heavy < r_neutral,
        f"neutral={r_neutral:.4f} ask_heavy={r_ask_heavy:.4f} warp={warp_ask:.4f}"
    )

    # Test 3: OBI warp magnitude is exactly obi × 5 × tick_size
    expected_warp = 0.6 * 5.0 * tick_size
    record(
        "OBI warp = obi × 5 × tick_size (exact formula match)",
        abs(warp_bid - expected_warp) < 1e-9,
        f"expected={expected_warp:.5f} got={warp_bid:.5f}"
    )

    # Test 4: OBI=0 should give same result as pre-V2.3 (no warp)
    record(
        "OBI=0.0 gives zero warp (backward-compatible)",
        abs(warp_neutral) < 1e-9,
        f"warp={warp_neutral}"
    )

    # Test 5: Symmetric — +OBI and -OBI warps are equal and opposite
    record(
        "OBI skew is symmetric (+OBI warp = -(-OBI warp))",
        abs(warp_bid + warp_ask) < 1e-9,
        f"+warp={warp_bid:.5f} -warp={warp_ask:.5f}"
    )

    # Test 6: Warp is proportional to tick_size (different coins)
    _, warp_xrp = simulate_reservation_price(
        micro_price=0.5, inventory_coin=0.0, live_gamma=0.5, variance=0.0001,
        live_max_tranches=3, obi=0.4, tick_size=0.0001  # XRP tick
    )
    expected_xrp_warp = 0.4 * 5.0 * 0.0001
    record(
        "OBI warp scales with tick_size (XRP 0.0001 tick check)",
        abs(warp_xrp - expected_xrp_warp) < 1e-10,
        f"expected={expected_xrp_warp:.7f} got={warp_xrp:.7f}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# § 2  TAKER BAILOUT LOGIC — simulated tick() state machine
# ══════════════════════════════════════════════════════════════════════════════

def simulate_tick_ask_side(ticks_held, active_tranches, safe_to_sell,
                            last_fill_price, optimal_ask, tick_size):
    """
    Mirrors the V2.3 ASK SIDE logic in engine_d_hft.rs tick().
    Returns (open_ask, emergency_dump_triggered, open_bid_cancelled).
    """
    emergency_dump_triggered = False
    open_ask = None
    open_bid_cancelled = False

    if active_tranches > 0:
        if ticks_held > 1200:
            emergency_dump_triggered = True
            open_ask = None
            open_bid_cancelled = True   # bail cancels bid too
        elif safe_to_sell:
            if ticks_held > 600:
                min_profit_price = last_fill_price       # break-even
            else:
                min_profit_price = last_fill_price + tick_size  # 1-tick floor
            open_ask = max(optimal_ask, min_profit_price)
        else:
            open_ask = None
    else:
        open_ask = None

    return open_ask, emergency_dump_triggered, open_bid_cancelled


def test_taker_bailout():
    print(f"\n{SEP}")
    print("§2  TRUE TAKER BAILOUT LOGIC (>1200 tick gate)")
    print(SEP)

    tick_size      = 0.01
    last_fill      = 150.00
    optimal_ask    = 150.05

    # Test 1: Normal (< 600 ticks) — 1-tick profit floor required
    ask, dump, bid_cancelled = simulate_tick_ask_side(
        ticks_held=100, active_tranches=2, safe_to_sell=True,
        last_fill_price=last_fill, optimal_ask=optimal_ask, tick_size=tick_size
    )
    record(
        "Normal (<600t): open_ask >= last_fill + 1 tick",
        ask is not None and ask >= last_fill + tick_size,
        f"ask={ask}"
    )
    record("Normal (<600t): no emergency dump", not dump, f"dump={dump}")

    # Test 2: Stale zone (600–1200 ticks) — break-even accepted
    ask2, dump2, _ = simulate_tick_ask_side(
        ticks_held=800, active_tranches=2, safe_to_sell=True,
        last_fill_price=last_fill, optimal_ask=optimal_ask, tick_size=tick_size
    )
    record(
        "Stale (800t): open_ask >= last_fill_price (break-even accepted)",
        ask2 is not None and ask2 >= last_fill,
        f"ask={ask2}"
    )
    record("Stale (800t): no emergency dump", not dump2, f"dump={dump2}")

    # Test 3: Bailout zone (> 1200 ticks) — emergency dump triggered
    ask3, dump3, bid3 = simulate_tick_ask_side(
        ticks_held=1201, active_tranches=2, safe_to_sell=True,
        last_fill_price=last_fill, optimal_ask=optimal_ask, tick_size=tick_size
    )
    record(
        "Bailout (>1200t): emergency_dump_triggered=True",
        dump3 is True,
        f"dump={dump3}"
    )
    record(
        "Bailout (>1200t): open_ask=None (no maker orders)",
        ask3 is None,
        f"ask={ask3}"
    )
    record(
        "Bailout (>1200t): open_bid cancelled",
        bid3 is True,
        f"bid_cancelled={bid3}"
    )

    # Test 4: Exactly at boundary (1200 ticks) — NOT triggered yet
    ask4, dump4, _ = simulate_tick_ask_side(
        ticks_held=1200, active_tranches=2, safe_to_sell=True,
        last_fill_price=last_fill, optimal_ask=optimal_ask, tick_size=tick_size
    )
    record(
        "Boundary (1200t exactly): no bailout yet (strictly >1200 triggers)",
        dump4 is False,
        f"dump={dump4} ask={ask4}"
    )

    # Test 5: Empty (active_tranches=0) — no ask regardless of ticks_held
    ask5, dump5, _ = simulate_tick_ask_side(
        ticks_held=5000, active_tranches=0, safe_to_sell=True,
        last_fill_price=last_fill, optimal_ask=optimal_ask, tick_size=tick_size
    )
    record(
        "Flat (tranches=0): no ask even with 5000 ticks held",
        ask5 is None and not dump5,
        f"ask={ask5} dump={dump5}"
    )

    # Test 6: Not safe to sell (obi filter) — no ask in normal zone
    ask6, dump6, _ = simulate_tick_ask_side(
        ticks_held=100, active_tranches=2, safe_to_sell=False,
        last_fill_price=last_fill, optimal_ask=optimal_ask, tick_size=tick_size
    )
    record(
        "OBI block: no ask when safe_to_sell=False (in normal zone)",
        ask6 is None and not dump6,
        f"ask={ask6}"
    )

    # Test 7: Bailout overrides OBI block (>1200 ticks, safe_to_sell=False)
    ask7, dump7, _ = simulate_tick_ask_side(
        ticks_held=1500, active_tranches=3, safe_to_sell=False,
        last_fill_price=last_fill, optimal_ask=optimal_ask, tick_size=tick_size
    )
    record(
        "Bailout overrides OBI (>1200t, safe_to_sell=False → still dumps)",
        dump7 is True,
        f"dump={dump7} ask={ask7}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# § 3  DATA PIPELINE — 4-tuple rolling window and state vector
# ══════════════════════════════════════════════════════════════════════════════

def test_data_pipeline_4tuple():
    print(f"\n{SEP}")
    print("§3  DATA PIPELINE — 4-tuple window (price, tfi, spread, obi)")
    print(SEP)

    # Simulate what data_pipeline.py does internally (no Redis/DB needed)
    from collections import deque
    import numpy as np

    window = deque(maxlen=300)
    # Seed 30 ticks with a clear OBI trend
    for i in range(30):
        price        = 150.0 + i * 0.01
        tfi          = (i - 15) * 1000.0      # ramp from negative to positive
        spread_ticks = 1.5
        obi          = 0.3 + i * 0.01         # rising OBI
        window.append((price, tfi, spread_ticks, obi))

    # Unpack all 4 components
    prices  = np.array([p for p, _, _, _ in window], dtype=float)
    tfis    = np.array([t for _, t, _, _ in window], dtype=float)
    spreads = np.array([s for _, _, s, _ in window], dtype=float)
    obis    = np.array([o for _, _, _, o in window], dtype=float)

    record(
        "4-tuple window unpacks price correctly (30 samples)",
        len(prices) == 30 and abs(prices[-1] - (150.0 + 29 * 0.01)) < 1e-6,
        f"prices[-1]={prices[-1]:.4f}"
    )
    record(
        "4-tuple window unpacks OBI correctly",
        len(obis) == 30 and abs(obis[0] - 0.3) < 1e-6,
        f"obis[0]={obis[0]:.4f} obis[-1]={obis[-1]:.4f}"
    )

    obi_mean = float(np.mean(obis))
    record(
        "OBI mean computes correctly from 4th element",
        0.3 < obi_mean < 0.6,
        f"obi_mean={obi_mean:.4f}"
    )

    # Verify the 4-tuple structure doesn't corrupt other metrics
    vol_bps = float(np.std(np.diff(prices) / (prices[:-1] + 1e-12)) * 10_000)
    record(
        "Vol BPS computes from price (1st element) without OBI contamination",
        vol_bps > 0 and vol_bps < 10,
        f"vol_bps={vol_bps:.4f}"
    )

    tfi_zscore = float((tfis[-1] - np.mean(tfis)) / (np.std(tfis) + 1e-9))
    record(
        "TFI Z-Score computes from tfi (2nd element) correctly",
        abs(tfi_zscore) > 0,
        f"tfi_zscore={tfi_zscore:.4f}"
    )

    native_spread = float(np.mean(spreads))
    record(
        "Native spread computes from spread (3rd element)",
        abs(native_spread - 1.5) < 0.01,
        f"native_spread={native_spread:.4f}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# § 4  STATE VECTOR FORMAT — verify lines 6 and 7 structure
# ══════════════════════════════════════════════════════════════════════════════

def test_state_vector_format():
    print(f"\n{SEP}")
    print("§4  STATE VECTOR FORMAT — lines 6 (OBI) and 7 (Engine State)")
    print(SEP)

    # Simulate what get_state_vector() returns
    eng_ctx = {
        "inventory_coin":        0.04,
        "pnl_mtm":               -0.0023,
        "decision":              "SKEW-ASK",
        "active_tranches":       2,
        "applied_max_tranches":  7,
        "applied_grid_offset":   3.0,
        "obi_live":              0.42,
        "micro_price":           150.23,
        "ticks_held":            820,
        "emergency_dump":        False,
    }
    obi_mean   = 0.38
    vol_bps    = 4.5
    tfi_zscore = 0.21
    drift_bps  = 12.3
    native_spread = 1.3
    adverse_pct   = 18.0

    vol_label   = "NORMAL"
    tox_label   = "CLEAN"
    trend_label = "CHOP/RANGE"
    adv_label   = "SAFE (Capturing Spread)"

    bailout_flag = " ⚠ TAKER BAILOUT IMMINENT" if eng_ctx["ticks_held"] > 1000 else (
        " [EMERGENCY DUMP]" if eng_ctx["emergency_dump"] else ""
    )
    eng_line = (
        f"inv={eng_ctx['inventory_coin']:+.4f} coin | "
        f"pnl_mtm={eng_ctx['pnl_mtm']:+.4f} USD | "
        f"decision={eng_ctx['decision']} | "
        f"tranches={eng_ctx['active_tranches']}/{eng_ctx['applied_max_tranches']} | "
        f"offset={eng_ctx['applied_grid_offset']:.1f}t | "
        f"ticks_held={eng_ctx['ticks_held']}{bailout_flag}"
    )

    sv = (
        f"[ORACLE 5M STATE VECTOR - SOLFDUSD]\n"
        f"1. Micro-Volatility: {vol_bps:.2f} bps/sec ({vol_label})\n"
        f"2. Order Flow Toxicity: {tfi_zscore:+.2f}\u03c3 ({tox_label})\n"
        f"3. Market Drift: {drift_bps:+.2f} bps/5m ({trend_label})\n"
        f"4. Native LOB Spread: {native_spread:.1f} ticks\n"
        f"5. Adverse Selection: {adverse_pct:.1f}% ({adv_label})\n"
        f"6. OBI (5m mean): {obi_mean:+.4f} (live: {eng_ctx['obi_live']:+.4f})\n"
        f"7. Engine State: {eng_line}"
    )

    lines = sv.strip().split("\n")
    record("State vector has exactly 7 lines (header + 7 metrics)", len(lines) == 8, f"{len(lines)} lines")

    record(
        "Line 6 contains OBI mean and live OBI",
        "OBI (5m mean)" in lines[6] and "live:" in lines[6],
        lines[6][:80]
    )
    record(
        "Line 7 contains ticks_held",
        "ticks_held=" in lines[7],
        lines[7][:100]
    )
    record(
        "Line 7 contains decision",
        "decision=SKEW-ASK" in lines[7],
        lines[7][:100]
    )
    record(
        "Line 7 contains tranche info",
        "tranches=2/7" in lines[7],
        lines[7][:100]
    )

    # Test bailout flag appears when ticks_held > 1000
    eng_ctx2 = dict(eng_ctx, ticks_held=1050, emergency_dump=False)
    bailout_flag2 = " ⚠ TAKER BAILOUT IMMINENT" if eng_ctx2["ticks_held"] > 1000 else ""
    record(
        "State vector warns ⚠ TAKER BAILOUT IMMINENT when ticks_held > 1000",
        "⚠ TAKER BAILOUT IMMINENT" in bailout_flag2,
        f"ticks_held={eng_ctx2['ticks_held']}"
    )

    eng_ctx3 = dict(eng_ctx, ticks_held=500, emergency_dump=True)
    bailout_flag3 = " ⚠ TAKER BAILOUT IMMINENT" if eng_ctx3["ticks_held"] > 1000 else (
        " [EMERGENCY DUMP]" if eng_ctx3["emergency_dump"] else ""
    )
    record(
        "State vector shows [EMERGENCY DUMP] when flag active",
        "[EMERGENCY DUMP]" in bailout_flag3,
        f"flag={bailout_flag3}"
    )

    print(f"\n  {INFO} Sample state vector:")
    for line in lines:
        print(f"    {line}")


# ══════════════════════════════════════════════════════════════════════════════
# § 5  PROMPT FILES — verify V2.3 content is present
# ══════════════════════════════════════════════════════════════════════════════

def test_prompt_files():
    print(f"\n{SEP}")
    print("§5  PROMPT FILES — V2.3 content verification")
    print(SEP)

    base = os.path.join(
        os.path.dirname(__file__), "..",
        "agent_q_optimizer", "prompt_engineering"
    )

    # agent_1_alpha.txt — must have bailout section
    alpha_path = os.path.join(base, "agent_1_alpha.txt")
    try:
        txt = open(alpha_path).read()
        record("agent_1_alpha.txt: TAKER BAILOUT section present",
               "TAKER BAILOUT" in txt, alpha_path)
        record("agent_1_alpha.txt: 1200 tick threshold mentioned",
               "1200" in txt, "")
        record("agent_1_alpha.txt: grid_offset_ticks lever present",
               "grid_offset_ticks" in txt, "")
        record("agent_1_alpha.txt: OBI calibration section present",
               "OBI CALIBRATION" in txt or "obi_threshold above the 5m OBI mean" in txt, "")
    except Exception as e:
        record("agent_1_alpha.txt readable", False, str(e))

    # agent_2_risk.txt — must have OBI threshold calibration rule
    risk_path = os.path.join(base, "agent_2_risk.txt")
    try:
        txt = open(risk_path).read()
        record("agent_2_risk.txt: OBI threshold calibration rule present",
               "5m OBI mean" in txt or "obi_threshold" in txt, "")
        record("agent_2_risk.txt: final_grid_offset_ticks in bounding matrix",
               "grid_offset_ticks" in txt, "")
        record("agent_2_risk.txt: TOXIC_LIQUIDATION_CASCADE force=2.0 offset",
               "2.0" in txt, "")
    except Exception as e:
        record("agent_2_risk.txt readable", False, str(e))

    # agent_regime_classifier.txt — must warn not to use lines 6-7 for regime
    regime_path = os.path.join(base, "agent_regime_classifier.txt")
    try:
        txt = open(regime_path).read()
        record("agent_regime_classifier.txt: lines 6-7 context-only note present",
               "context only" in txt or "do NOT use them for regime" in txt, "")
    except Exception as e:
        record("agent_regime_classifier.txt readable", False, str(e))


# ══════════════════════════════════════════════════════════════════════════════
# § 6  REWARD FUNCTION — V2.2 cadence consistency
# ══════════════════════════════════════════════════════════════════════════════

def test_reward_function():
    print(f"\n{SEP}")
    print("§6  REWARD FUNCTION — V2.2 velocity-first scoring")
    print(SEP)

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agent_q_optimizer"))
    try:
        from data_pipeline import calculate_rl_reward

        # Starvation: 0 trades → maximum penalty
        r0 = calculate_rl_reward(0, 0.5, 0.0)
        record("0 trades → max penalty = -200.0",
               abs(r0 - -200.0) < 0.01, f"reward={r0:.1f}")

        # On target: 15 trades → base bonus = 50
        r15 = calculate_rl_reward(15, 0.5, 0.0)
        record("15 trades (target) → vol_score=50.0 (+ wr/pnl terms)",
               abs(r15 - 50.0) < 0.01, f"reward={r15:.1f}")

        # Hyper-cadence: 30 trades → 50 + (30-15)×2 = 80
        r30 = calculate_rl_reward(30, 0.5, 0.0)
        record("30 trades (2x target) → vol_score=80.0",
               abs(r30 - 80.0) < 0.01, f"reward={r30:.1f}")

        # Win rate bonus: 60% WR = (0.6-0.5)×50 = +5
        r_wr = calculate_rl_reward(15, 0.6, 0.0)
        record("60% WR at target → total=50+5=55.0",
               abs(r_wr - 55.0) < 0.01, f"reward={r_wr:.1f}")

        # PnL reward: $0.1 profit = +1.0
        r_pnl = calculate_rl_reward(15, 0.5, 0.1)
        record("$0.10 PnL at target → total=50+0+1=51.0",
               abs(r_pnl - 51.0) < 0.01, f"reward={r_pnl:.1f}")

        # Partial starve: 3 trades → -200 × (1-3/15) = -160
        r3 = calculate_rl_reward(3, 0.5, 0.0)
        record("3 trades → vol_score=-160.0",
               abs(r3 - -160.0) < 0.01, f"reward={r3:.1f}")

    except ImportError as e:
        record("data_pipeline importable from tests/", False, str(e))
    except Exception as e:
        record("reward function computes correctly", False, str(e))


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n" + "═" * 70)
    print("  V2.3 FEATURE UNIT TESTS")
    print("  Microstructure Skew + True Taker Bailout")
    print("═" * 70)

    test_obi_skew()
    test_taker_bailout()
    test_data_pipeline_4tuple()
    test_state_vector_format()
    test_prompt_files()
    test_reward_function()

    print(f"\n{SEP}")
    print("SUMMARY")
    print(SEP)
    passed = sum(1 for v in results.values() if v)
    failed = sum(1 for v in results.values() if not v)
    total  = len(results)
    pct    = int(100 * passed / total) if total else 0

    for name, ok in results.items():
        print(f"  {'✓' if ok else '✗'}  {name}")

    print(f"\n  Total: {total}  |  {PASS} {passed}  |  {FAIL} {failed}  |  Score: {pct}%")
    if failed == 0:
        print(f"\n  \033[92m✓ ALL V2.3 TESTS PASSED\033[0m")
    else:
        print(f"\n  \033[91m✗ {failed} test(s) failed — see details above.\033[0m")
    print()
    sys.exit(0 if failed == 0 else 1)
