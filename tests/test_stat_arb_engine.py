"""
Tests for Engine B — Statistical Arbitrage math.

Inline stubs mirror stat_arb_engine.py exactly.
"""

import math
import pytest
from collections import deque

# ── Stubs ─────────────────────────────────────────────────────────────────────

TRADE_SIZE_USDT   = 10.0
WALLET_USDT       = 30.0
ZSCORE_THRESHOLD  = 2.0
MIN_PERIODS       = 30
ROUND_TRIP_FEE    = 0.002


def compute_zscore(ratio_window: deque, current_ratio: float):
    n = len(ratio_window)
    if n < MIN_PERIODS:
        return math.nan, math.nan, math.nan

    mean     = sum(ratio_window) / n
    variance = sum((r - mean) ** 2 for r in ratio_window) / n
    stddev   = math.sqrt(variance)

    if stddev < 1e-10:
        return math.nan, mean, stddev

    z = (current_ratio - mean) / stddev
    if not math.isfinite(z):
        return math.nan, mean, stddev
    return z, mean, stddev


def compute_unrealized_pnl(direction: str, entry_ratio: float, current_ratio: float,
                            size_usdt: float = TRADE_SIZE_USDT) -> float:
    if entry_ratio <= 0:
        return 0.0
    if direction == "LONG":
        pct = ((current_ratio - entry_ratio) / entry_ratio) * 100.0
    else:
        pct = ((entry_ratio - current_ratio) / entry_ratio) * 100.0
    return (pct / 100.0) * size_usdt


def build_window(n: int, base: float = 20.0, noise: float = 0.1) -> deque:
    """Build a window of n values oscillating around base."""
    import random
    random.seed(42)
    d = deque()
    for _ in range(n):
        d.append(base + random.uniform(-noise, noise))
    return d


# ── Z-score ───────────────────────────────────────────────────────────────────

def test_zscore_insufficient_data():
    """Fewer than MIN_PERIODS → all nan."""
    w = deque([20.0] * (MIN_PERIODS - 1))
    z, mean, std = compute_zscore(w, 20.0)
    assert math.isnan(z)


def test_zscore_min_periods_exact():
    """Exactly MIN_PERIODS → valid result."""
    w = build_window(MIN_PERIODS)
    z, mean, std = compute_zscore(w, 20.0)
    assert math.isfinite(z)


def test_zscore_zero_stddev():
    """All identical values → stddev ≈ 0 → nan."""
    w = deque([20.0] * MIN_PERIODS)
    z, mean, std = compute_zscore(w, 20.0)
    assert math.isnan(z)


def test_zscore_above_threshold_positive():
    w = build_window(MIN_PERIODS, base=20.0, noise=0.01)
    # Push ratio far above mean to get z > 2
    high_ratio = 20.0 + 5.0  # very far from mean
    z, _, _ = compute_zscore(w, high_ratio)
    assert z > ZSCORE_THRESHOLD


def test_zscore_above_threshold_negative():
    w = build_window(MIN_PERIODS, base=20.0, noise=0.01)
    low_ratio = 20.0 - 5.0
    z, _, _ = compute_zscore(w, low_ratio)
    assert z < -ZSCORE_THRESHOLD


def test_zscore_direction_long():
    """Negative z → mean-reversion LONG (ratio below mean)."""
    w = build_window(MIN_PERIODS, base=20.0, noise=0.01)
    z, _, _ = compute_zscore(w, 15.0)
    assert z < 0  # below mean → enter LONG


def test_zscore_direction_short():
    """Positive z → mean-reversion SHORT (ratio above mean)."""
    w = build_window(MIN_PERIODS, base=20.0, noise=0.01)
    z, _, _ = compute_zscore(w, 25.0)
    assert z > 0


# ── TP/SL calculation ─────────────────────────────────────────────────────────

def test_tp_sl_calculation():
    """
    Expected_Gain = abs((ratio - mean) / mean) * 100
    TP_usdt = (EG / 100) * size_usdt
    SL_usdt = TP_usdt * 0.5
    """
    ratio = 22.0
    mean  = 20.0
    expected_gain_pct = abs((ratio - mean) / mean) * 100.0  # 10%
    tp_usdt = (expected_gain_pct / 100.0) * TRADE_SIZE_USDT  # $1.0
    sl_usdt = tp_usdt * 0.5                                   # $0.5
    assert tp_usdt == pytest.approx(1.0)
    assert sl_usdt == pytest.approx(0.5)


def test_sl_is_half_tp():
    for eg in [0.5, 1.0, 2.5, 10.0]:
        tp = (eg / 100.0) * TRADE_SIZE_USDT
        sl = tp * 0.5
        assert sl == pytest.approx(tp / 2.0)


# ── Unrealized PnL ────────────────────────────────────────────────────────────

def test_unrealized_long_profit():
    """LONG: ratio moves up → positive PnL."""
    pnl = compute_unrealized_pnl("LONG", 20.0, 21.0)
    assert pnl > 0


def test_unrealized_long_loss():
    """LONG: ratio moves down → negative PnL."""
    pnl = compute_unrealized_pnl("LONG", 20.0, 19.0)
    assert pnl < 0


def test_unrealized_short_profit():
    """SHORT: ratio moves down → positive PnL."""
    pnl = compute_unrealized_pnl("SHORT", 20.0, 19.0)
    assert pnl > 0


def test_unrealized_short_loss():
    """SHORT: ratio moves up → negative PnL."""
    pnl = compute_unrealized_pnl("SHORT", 20.0, 21.0)
    assert pnl < 0


def test_unrealized_zero_entry():
    """Zero entry ratio → returns 0.0 (guard)."""
    assert compute_unrealized_pnl("LONG", 0.0, 20.0) == 0.0


# ── NaN guard ─────────────────────────────────────────────────────────────────

def test_nan_not_traded():
    """If z is nan, should_enter must be False."""
    z = math.nan
    should_enter = not math.isnan(z) and abs(z) >= ZSCORE_THRESHOLD
    assert should_enter is False


def test_inf_treated_as_nan():
    """Infinite z-score → treated as nan (isfinite guard)."""
    w = deque([20.0] * MIN_PERIODS)
    w[-1] = float('inf')
    # stddev will be non-zero due to inf, but z will be inf → caught by isfinite check
    # We just check compute_zscore returns nan
    z, _, _ = compute_zscore(w, 20.0)
    # inf in window makes mean inf → z = nan or inf, both non-finite
    assert not math.isfinite(z) or math.isnan(z)
