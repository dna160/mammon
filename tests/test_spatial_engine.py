"""
Tests for Engine A — Spatial Arbitrage math.

These tests use inline pure-Python stubs matching spatial_engine.py exactly.
Swap the stub imports for real imports once the package is installed.
"""

import pytest

# ── Stubs (mirrors spatial_engine.py) ─────────────────────────────────────────

FRICTION        = 0.0092
TARGET_SPREAD   = 1.07
TRADE_SIZE_IDR  = 100_000
WALLET_IDR      = 1_000_000
MIN_VOLUME_IDR  = 100_000


def compute_spread(toko_ask: float, indo_bid: float) -> float:
    if toko_ask <= 0:
        return 0.0
    return ((indo_bid - toko_ask) / toko_ask) * 100.0


def should_trade(spread: float, toko_vol: float, indo_vol: float) -> bool:
    return (
        spread >= TARGET_SPREAD
        and toko_vol >= MIN_VOLUME_IDR
        and indo_vol >= MIN_VOLUME_IDR
    )


def compute_pnl(spread: float):
    gross = TRADE_SIZE_IDR * (spread / 100.0)
    fees  = TRADE_SIZE_IDR * FRICTION
    net   = gross - fees
    roe   = (net / WALLET_IDR) * 100.0
    return gross, fees, net, roe


# ── Spread calculation ─────────────────────────────────────────────────────────

def test_spread_basic():
    """((105 - 100) / 100) * 100 == 5.0"""
    assert compute_spread(100.0, 105.0) == pytest.approx(5.0)


def test_spread_negative():
    """Indo bid below Toko ask → negative spread."""
    assert compute_spread(100.0, 98.0) == pytest.approx(-2.0)


def test_spread_zero_ask():
    """Zero ask price → returns 0.0 (guard)."""
    assert compute_spread(0.0, 105.0) == 0.0


def test_spread_equal_prices():
    assert compute_spread(100.0, 100.0) == pytest.approx(0.0)


def test_spread_above_threshold():
    spread = compute_spread(100.0, 102.0)  # 2.0%
    assert spread >= TARGET_SPREAD


def test_spread_below_threshold():
    spread = compute_spread(100.0, 100.5)  # 0.5%
    assert spread < TARGET_SPREAD


# ── Trade gate ────────────────────────────────────────────────────────────────

def test_should_trade_all_ok():
    """Spread ok, volumes ok → should trade."""
    assert should_trade(1.5, 200_000, 200_000) is True


def test_should_trade_spread_too_low():
    assert should_trade(0.5, 200_000, 200_000) is False


def test_should_trade_toko_volume_too_low():
    assert should_trade(1.5, 50_000, 200_000) is False


def test_should_trade_indo_volume_too_low():
    assert should_trade(1.5, 200_000, 50_000) is False


def test_should_trade_both_volumes_too_low():
    assert should_trade(1.5, 50_000, 50_000) is False


def test_should_trade_exact_threshold():
    """Spread exactly at threshold → should trade."""
    assert should_trade(TARGET_SPREAD, MIN_VOLUME_IDR, MIN_VOLUME_IDR) is True


# ── PnL math ──────────────────────────────────────────────────────────────────

def test_net_pnl_at_threshold():
    """
    Net_PnL = (100000 * 1.07/100) - (100000 * 0.0092)
            = 1070 - 920 = 150 IDR
    """
    gross, fees, net, roe = compute_pnl(TARGET_SPREAD)
    assert gross == pytest.approx(1070.0)
    assert fees  == pytest.approx(920.0)
    assert net   == pytest.approx(150.0)


def test_roe_at_threshold():
    """ROE = 150 / 1_000_000 * 100 = 0.015%"""
    _, _, net, roe = compute_pnl(TARGET_SPREAD)
    assert roe == pytest.approx(0.015, rel=1e-4)


def test_negative_net_pnl_below_friction():
    """Spread of 0.5% < 0.92% friction → negative net PnL."""
    _, _, net, _ = compute_pnl(0.5)
    assert net < 0


def test_gross_pnl_proportional():
    """Gross scales linearly with spread."""
    _, _, _, _ = compute_pnl(2.0)
    gross_2, _, _, _ = compute_pnl(2.0)
    gross_4, _, _, _ = compute_pnl(4.0)
    assert gross_4 == pytest.approx(gross_2 * 2.0)
