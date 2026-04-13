"""
Tests for Multi-Tranche Scaling (updated for V2.2 PRD linear reward formula):
  1. RL reward target = 15 round-trips per 3-min cycle (linear cadence score)
  2. max_active_tranches flows correctly through redis_bridge publish_params
  3. safe_mode payload includes max_active_tranches=1
  4. json_sanitizer correctly parses max_active_tranches from LLM output
"""
import sys
import os
import json

# Allow importing from parent directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_pipeline import calculate_rl_reward
from json_sanitizer import extract_json


# ── 1. RL Reward: V2.2 linear formula, 3-min target of 15 trips ───────────────

def test_reward_zero_trades_scores_minus_100_volume():
    """V2.2: 0 trades, 50% WR → volume=-200×(1-0/15)=-200, wr=0, pnl=0 → -200"""
    reward = calculate_rl_reward(round_trips=0, win_rate=0.5, net_pnl=0.0)
    assert abs(reward - (-200.0)) < 0.01, f"Expected -200, got {reward}"

def test_reward_15_trades_hits_target():
    """V2.2: exactly 15 trades, 50% WR → volume=50, wr=0, pnl=0 → +50.0"""
    reward = calculate_rl_reward(round_trips=15, win_rate=0.5, net_pnl=0.0)
    assert abs(reward - 50.0) < 0.01, f"Expected 50.0, got {reward}"

def test_reward_15_trades_good_wr_positive():
    """15 trades, 60% WR, +$0.05 PnL → volume=50 + wr=5 + pnl=0.5 = 55.5"""
    reward = calculate_rl_reward(round_trips=15, win_rate=0.6, net_pnl=0.05)
    assert reward > 0, f"Expected positive reward, got {reward}"
    assert abs(reward - 55.5) < 0.01, f"Expected ~55.5, got {reward}"

def test_reward_3_trades_partial_penalty():
    """V2.2: 3 trades (20% of target) → volume = -200×(1-3/15) = -160"""
    reward = calculate_rl_reward(round_trips=3, win_rate=0.5, net_pnl=0.0)
    assert abs(reward - (-160.0)) < 0.01, f"Expected -160.0, got {reward}"

def test_reward_30_trades_logarithmic_bonus():
    """V2.2: 30 trades → volume = 50 + (30-15)×2 = 80; wr=0; pnl=0 → 80.0"""
    reward = calculate_rl_reward(round_trips=30, win_rate=0.5, net_pnl=0.0)
    assert abs(reward - 80.0) < 0.01, f"Expected 80.00, got {reward:.2f}"

def test_reward_pnl_is_linear():
    """PnL score = net_pnl × 10. At 15 trips: volume=50, wr=0, pnl=±10."""
    r_pos = calculate_rl_reward(round_trips=15, win_rate=0.5, net_pnl=1.0)
    r_neg = calculate_rl_reward(round_trips=15, win_rate=0.5, net_pnl=-1.0)
    assert abs(r_pos - 60.0) < 0.01, f"Expected +60.0, got {r_pos}"
    assert abs(r_neg - 40.0)  < 0.01, f"Expected +40.0, got {r_neg}"


# ── 2. JSON sanitizer: parses max_active_tranches ─────────────────────────────

def test_extract_json_with_max_active_tranches():
    """extract_json handles max_active_tranches int field from Alpha output."""
    raw = '{"gamma":0.4,"min_spread_ticks":2.0,"tfi_threshold":75000,"max_active_tranches":7,"obi_threshold":0.8,"reasoning":"test"}'
    result = extract_json(raw)
    assert result["max_active_tranches"] == 7
    assert result["obi_threshold"] == 0.8
    assert result["gamma"] == 0.4

def test_extract_json_with_final_max_active_tranches():
    """extract_json handles final_max_active_tranches from CRO output."""
    raw = '{"final_gamma":0.4,"final_min_spread_ticks":2.0,"final_tfi_threshold":75000.0,"final_obi_threshold":0.8,"final_max_active_tranches":7,"override_applied":false,"panic_sell_flag":false,"cro_reasoning":"Approved."}'
    result = extract_json(raw)
    assert result["final_max_active_tranches"] == 7
    assert result["override_applied"] is False

def test_extract_json_strips_markdown_fences():
    """extract_json handles ```json ... ``` wrapping (common 3B model output)."""
    raw = '```json\n{"gamma":0.3,"min_spread_ticks":1.0,"tfi_threshold":50000,"max_active_tranches":5,"obi_threshold":0.7,"reasoning":"ok"}\n```'
    result = extract_json(raw)
    assert result["max_active_tranches"] == 5


# ── 3. redis_bridge: publish_params includes max_active_tranches ──────────────

def test_publish_params_includes_max_active_tranches(monkeypatch):
    """publish_params injects final_max_active_tranches into Redis payload."""
    published = {}

    class MockRedis:
        def publish(self, channel, data): published['data'] = json.loads(data)
        def set(self, *a, **kw): pass

    import redis_bridge
    monkeypatch.setattr(redis_bridge, '_redis_client', MockRedis())

    cro_output = {
        "final_gamma": 0.4,
        "final_min_spread_ticks": 2.0,
        "final_tfi_threshold": 75000.0,
        "final_obi_threshold": 0.8,
        "final_max_active_tranches": 7,
    }
    redis_bridge.publish_params("ETHFDUSD", cro_output)

    assert published['data']['max_active_tranches'] == 7
    assert published['data']['obi_threshold'] == 0.8
    assert published['data']['system_status'] == 'LIVE'

def test_safe_mode_has_tranches_1(monkeypatch):
    """Safe mode payload locks max_active_tranches to 1."""
    published = {}

    class MockRedis:
        def publish(self, channel, data): published['data'] = json.loads(data)
        def set(self, *a, **kw): pass

    import redis_bridge
    monkeypatch.setattr(redis_bridge, '_redis_client', MockRedis())

    redis_bridge.publish_safe_mode("ETHFDUSD")

    assert published['data']['max_active_tranches'] == 1
    assert published['data']['system_status'] == 'SAFE_MODE_LOCKDOWN'


# ── 4. publish_params defaults gracefully when field missing ──────────────────

def test_publish_params_defaults_tranches_to_1(monkeypatch):
    """If CRO omits final_max_active_tranches, default is 1 (safe)."""
    published = {}

    class MockRedis:
        def publish(self, channel, data): published['data'] = json.loads(data)
        def set(self, *a, **kw): pass

    import redis_bridge
    monkeypatch.setattr(redis_bridge, '_redis_client', MockRedis())

    cro_output = {
        "final_gamma": 0.5,
        "final_min_spread_ticks": 3.0,
        "final_tfi_threshold": 60000.0,
        "final_obi_threshold": 0.7,
        # no final_max_active_tranches
    }
    redis_bridge.publish_params("SOLFDUSD", cro_output)
    assert published['data']['max_active_tranches'] == 1


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
