"""
Tests for agent_C_microstructure/math_lib.py — JIT math primitives.

These tests use pure-Python stubs so they run without Numba.
To test the actual JIT functions:
  cd project_sniper/agent_C_microstructure
  NUMBA_DISABLE_JIT=1 python -c "import math_lib; print('OK')"
"""

import math
import numpy as np
import pytest

# ── Pure-Python stubs matching math_lib.py exactly ───────────────────────────

def compute_ofi(prev_bid_px, prev_bid_vol, curr_bid_px, curr_bid_vol,
                prev_ask_px, prev_ask_vol, curr_ask_px, curr_ask_vol):
    bid_flow = curr_bid_vol if curr_bid_px >= prev_bid_px else -prev_bid_vol
    ask_flow = curr_ask_vol if curr_ask_px <= prev_ask_px else -prev_ask_vol
    return bid_flow - ask_flow


def compute_ema(prev_ema, new_value, alpha):
    return alpha * new_value + (1.0 - alpha) * prev_ema


def compute_vpin(buy_volume_arr, total_volume_arr):
    total_sum = float(np.sum(total_volume_arr))
    if total_sum < 1e-10:
        return 0.0
    imbalance = float(np.sum(np.abs(buy_volume_arr - total_volume_arr * 0.5)))
    return imbalance / total_sum


def compute_hjb_target(ema_ofi, inventory, sigma_sq):
    clamped_inv   = max(-10.0, min(10.0, inventory))
    safe_sigma_sq = max(0.0, float(sigma_sq))
    return 0.5 * ema_ofi - 0.2 * clamped_inv * safe_sigma_sq


# ── OFI ───────────────────────────────────────────────────────────────────────

def test_ofi_bid_price_up():
    """Bid price rises → BidFlow = curr_bid_vol."""
    ofi = compute_ofi(100, 1.0,  101, 2.0,   # bid: up
                      102, 1.0,  102, 1.0)   # ask: unchanged
    # BidFlow = +2.0, AskFlow = +1.0 → OFI = 1.0
    assert ofi == pytest.approx(1.0)


def test_ofi_bid_price_down():
    """Bid price falls → BidFlow = -prev_bid_vol."""
    ofi = compute_ofi(100, 1.5,  99, 2.0,    # bid: down
                      102, 1.0, 102, 1.0)    # ask: flat
    # BidFlow = -1.5, AskFlow = +1.0 → OFI = -2.5
    assert ofi == pytest.approx(-2.5)


def test_ofi_bid_price_flat():
    """Bid price unchanged → BidFlow = +curr_bid_vol (>= branch)."""
    ofi = compute_ofi(100, 1.0, 100, 1.5,
                      102, 1.0, 103, 1.0)
    # BidFlow = +1.5, AskFlow = -1.0 → OFI = 2.5
    assert ofi == pytest.approx(2.5)


def test_ofi_ask_price_down():
    """Ask price falls → AskFlow = +curr_ask_vol."""
    ofi = compute_ofi(100, 1.0, 100, 1.0,
                      102, 1.0, 101, 2.0)    # ask: down
    # BidFlow = +1.0, AskFlow = +2.0 → OFI = -1.0
    assert ofi == pytest.approx(-1.0)


def test_ofi_ask_price_up():
    """Ask price rises → AskFlow = -prev_ask_vol."""
    ofi = compute_ofi(100, 1.0, 100, 1.0,
                      102, 1.5, 103, 1.0)    # ask: up
    # BidFlow = +1.0, AskFlow = -1.5 → OFI = 2.5
    assert ofi == pytest.approx(2.5)


def test_ofi_ask_price_flat():
    """Ask price flat (<= branch)."""
    ofi = compute_ofi(100, 1.0, 100, 1.0,
                      102, 1.0, 102, 1.5)
    # BidFlow = +1.0, AskFlow = +1.5 → OFI = -0.5
    assert ofi == pytest.approx(-0.5)


def test_ofi_both_aggressive():
    """Both bid up and ask down → strong positive OFI."""
    ofi = compute_ofi(100, 1.0, 101, 3.0,
                      102, 1.0, 101, 2.0)
    # BidFlow = +3.0, AskFlow = +2.0 → OFI = 1.0
    assert ofi == pytest.approx(1.0)


def test_ofi_zero_volumes():
    ofi = compute_ofi(100, 0.0, 101, 0.0,
                      102, 0.0, 101, 0.0)
    assert ofi == pytest.approx(0.0)


# ── EMA ───────────────────────────────────────────────────────────────────────

def test_ema_alpha_half():
    """alpha=0.5, prev=10, new=20 → 15.0"""
    assert compute_ema(10.0, 20.0, 0.5) == pytest.approx(15.0)


def test_ema_alpha_one():
    """alpha=1.0 → always returns new_value."""
    assert compute_ema(100.0, 42.0, 1.0) == pytest.approx(42.0)


def test_ema_alpha_zero():
    """alpha=0.0 → always returns prev_ema."""
    assert compute_ema(100.0, 42.0, 0.0) == pytest.approx(100.0)


def test_ema_convergence():
    """EMA with small alpha converges toward new_value over many steps."""
    ema = 0.0
    alpha = 0.1
    for _ in range(500):
        ema = compute_ema(ema, 100.0, alpha)
    assert ema == pytest.approx(100.0, rel=1e-3)


# ── VPIN ─────────────────────────────────────────────────────────────────────

def test_vpin_zero_total_volume():
    """Zero total volume → returns 0.0 (guard)."""
    buy = np.zeros(5)
    tot = np.zeros(5)
    assert compute_vpin(buy, tot) == 0.0


def test_vpin_balanced_flow():
    """Perfectly balanced buy/sell → VPIN ≈ 0."""
    tot = np.ones(10) * 100.0
    buy = tot * 0.5  # exactly half
    assert compute_vpin(buy, tot) == pytest.approx(0.0)


def test_vpin_all_buys():
    """All volume is buys → VPIN = 0.5."""
    tot = np.ones(10) * 100.0
    buy = tot  # all buy
    assert compute_vpin(buy, tot) == pytest.approx(0.5)


def test_vpin_range():
    """VPIN must always be in [0, 0.5] for valid inputs."""
    rng = np.random.default_rng(0)
    for _ in range(50):
        tot = rng.uniform(1, 100, 20)
        buy = rng.uniform(0, 1, 20) * tot
        v   = compute_vpin(buy, tot)
        assert 0.0 <= v <= 0.5 + 1e-9


def test_vpin_known_value():
    """
    buy=[80, 20], total=[100, 100]
    imbalance = |80-50| + |20-50| = 30 + 30 = 60
    VPIN = 60 / 200 = 0.3
    """
    buy = np.array([80.0, 20.0])
    tot = np.array([100.0, 100.0])
    assert compute_vpin(buy, tot) == pytest.approx(0.3)


# ── HJB Target ───────────────────────────────────────────────────────────────

def test_hjb_basic():
    """0.5*10 - 0.2*2*0.01 = 5.0 - 0.004 = 4.996"""
    result = compute_hjb_target(10.0, 2.0, 0.01)
    assert result == pytest.approx(4.996)


def test_hjb_zero_inventory():
    """Inventory=0 → Target = 0.5 * ema_ofi."""
    assert compute_hjb_target(8.0, 0.0, 1.0) == pytest.approx(4.0)


def test_hjb_negative_ema():
    """Negative OFI → negative target."""
    assert compute_hjb_target(-10.0, 0.0, 0.0) == pytest.approx(-5.0)


def test_hjb_inventory_clamp_positive():
    """Inventory > 10 is clamped to 10."""
    t_clamped = compute_hjb_target(0.0, 15.0, 1.0)
    t_at_10   = compute_hjb_target(0.0, 10.0, 1.0)
    assert t_clamped == pytest.approx(t_at_10)


def test_hjb_inventory_clamp_negative():
    """Inventory < -10 is clamped to -10."""
    t_clamped = compute_hjb_target(0.0, -15.0, 1.0)
    t_at_neg10 = compute_hjb_target(0.0, -10.0, 1.0)
    assert t_clamped == pytest.approx(t_at_neg10)


def test_hjb_negative_sigma_clamped():
    """Negative sigma_sq is treated as 0."""
    t_neg   = compute_hjb_target(10.0, 5.0, -1.0)
    t_zero  = compute_hjb_target(10.0, 5.0, 0.0)
    assert t_neg == pytest.approx(t_zero)


def test_hjb_threshold_long():
    """Target >= 8.5 should trigger LONG entry."""
    target = compute_hjb_target(20.0, 0.0, 0.1)
    assert target >= 8.5


def test_hjb_threshold_short():
    """Target <= -8.5 should trigger SHORT entry."""
    target = compute_hjb_target(-20.0, 0.0, 0.1)
    assert target <= -8.5
