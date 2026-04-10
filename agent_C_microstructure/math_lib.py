"""
math_lib.py — JIT-compiled math primitives for Engine C.

All functions use @numba.njit (nopython=True, cache=True) for maximum throughput.
Set NUMBA_DISABLE_JIT=1 to run in pure-Python mode for testing.

Functions
---------
compute_ofi      — Order Flow Imbalance per LOB tick
compute_ema      — Exponential moving average step
compute_vpin     — Volume-Synchronized Probability of Informed Trading
compute_hjb_target — HJB approximation for optimal inventory control
"""

import numpy as np
import numba as nb


@nb.njit(cache=True)
def compute_ofi(
    prev_bid_px: float, prev_bid_vol: float,
    curr_bid_px: float, curr_bid_vol: float,
    prev_ask_px: float, prev_ask_vol: float,
    curr_ask_px: float, curr_ask_vol: float,
) -> float:
    """
    Order Flow Imbalance for one LOB tick.

    Bid Flow:
      curr_bid_px >= prev_bid_px → BidFlow = +curr_bid_vol  (aggressive buying)
      curr_bid_px <  prev_bid_px → BidFlow = -prev_bid_vol  (bid withdrawal)

    Ask Flow:
      curr_ask_px <= prev_ask_px → AskFlow = +curr_ask_vol  (aggressive selling)
      curr_ask_px >  prev_ask_px → AskFlow = -prev_ask_vol  (ask withdrawal)

    OFI = BidFlow - AskFlow
    """
    if curr_bid_px >= prev_bid_px:
        bid_flow = curr_bid_vol
    else:
        bid_flow = -prev_bid_vol

    if curr_ask_px <= prev_ask_px:
        ask_flow = curr_ask_vol
    else:
        ask_flow = -prev_ask_vol

    return bid_flow - ask_flow


@nb.njit(cache=True)
def compute_ema(prev_ema: float, new_value: float, alpha: float) -> float:
    """
    Single-step EMA update.
    alpha = 2 / (N + 1) for an N-period EMA.
    Engine C at 50ms tick rate: 5-min span = 6000 ticks → alpha = 2/(6000+1) ≈ 0.000333
    """
    return alpha * new_value + (1.0 - alpha) * prev_ema


@nb.njit(cache=True)
def compute_vpin(
    buy_volume_arr: nb.float64[:],
    total_volume_arr: nb.float64[:],
) -> float:
    """
    Volume-Synchronized Probability of Informed Trading (VPIN).

    VPIN = Σ|buy_vol_i - total_vol_i / 2| / Σtotal_vol_i

    Returns 0.0 when total volume is zero (safe denominator guard).
    """
    total_sum = 0.0
    for v in total_volume_arr:
        total_sum += v
    if total_sum < 1e-10:
        return 0.0

    imbalance = 0.0
    for i in range(len(buy_volume_arr)):
        imbalance += abs(buy_volume_arr[i] - total_volume_arr[i] * 0.5)

    return imbalance / total_sum


@nb.njit(cache=True)
def compute_hjb_target(
    ema_ofi:   float,
    inventory: float,
    sigma_sq:  float,
) -> float:
    """
    HJB Approximation for optimal market-making inventory target.

    Target = 0.5 * EMA_OFI - 0.2 * clamp(inventory, -10, 10) * sigma_sq

    Parameters
    ----------
    ema_ofi   : 5-min EMA of OFI
    inventory : current inventory in units of base asset (-10 to +10)
    sigma_sq  : 5-min rolling price variance (non-negative; clamped to 0 if negative)
    """
    clamped_inv    = max(-10.0, min(10.0, inventory))
    safe_sigma_sq  = max(0.0, float(sigma_sq))
    return 0.5 * ema_ofi - 0.2 * clamped_inv * safe_sigma_sq
