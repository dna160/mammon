"""
Tests for Mammon V2.2 PRD Implementation:
  1. RL reward function: linear cadence score, 3-min target of 15 trips
  2. grid_offset_ticks flows through redis_bridge publish_params
  3. safe_mode includes grid_offset_ticks=2.0
  4. json_sanitizer parses grid_offset_ticks from LLM output
  5. parameter_tuner logs grid_offset_ticks to agent_q_memory
  6. max_active_tranches defaults to 1 when CRO omits it
  7. reward function handles edge cases correctly
"""
import sys
import os
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_pipeline import calculate_rl_reward
from json_sanitizer import extract_json


# ── 1. RL Reward: V2.2 linear formula, 3-min cycle ───────────────────────────

def test_reward_zero_trades_max_penalty():
    """0 trips → volume_score = -200×(1−0/15) = -200, wr=-25, pnl=0 → -225"""
    reward = calculate_rl_reward(round_trips=0, win_rate=0.0, net_pnl=0.0)
    # volume=-200, wr=(0-0.5)*50=-25, pnl=0 → -225
    assert abs(reward - (-225.0)) < 0.01, f"Expected -225.0, got {reward}"

def test_reward_zero_trades_neutral_wr():
    """0 trips, 50% WR → -200 + 0 + 0 = -200"""
    reward = calculate_rl_reward(round_trips=0, win_rate=0.5, net_pnl=0.0)
    assert abs(reward - (-200.0)) < 0.01, f"Expected -200.0, got {reward}"

def test_reward_15_trades_hits_target():
    """15 trips, 50% WR, $0 PnL → volume=50, wr=0, pnl=0 → +50.0"""
    reward = calculate_rl_reward(round_trips=15, win_rate=0.5, net_pnl=0.0)
    assert abs(reward - 50.0) < 0.01, f"Expected 50.0, got {reward}"

def test_reward_15_trades_perfect_wr():
    """15 trips, 100% WR, $0.10 PnL → volume=50 + wr=25 + pnl=1 = 76"""
    reward = calculate_rl_reward(round_trips=15, win_rate=1.0, net_pnl=0.1)
    assert abs(reward - 76.0) < 0.01, f"Expected 76.0, got {reward}"

def test_reward_3_trades_linear_penalty():
    """3 trips → volume = -200×(1−3/15) = -200×0.8 = -160"""
    reward = calculate_rl_reward(round_trips=3, win_rate=0.5, net_pnl=0.0)
    assert abs(reward - (-160.0)) < 0.01, f"Expected -160.0, got {reward}"

def test_reward_30_trades_linear_bonus():
    """30 trips → volume = 50 + (30-15)×2 = 80"""
    reward = calculate_rl_reward(round_trips=30, win_rate=0.5, net_pnl=0.0)
    expected = 80.0  # volume=80 + wr=0 + pnl=0
    assert abs(reward - expected) < 0.01, f"Expected {expected}, got {reward}"

def test_reward_pnl_linear():
    """PnL score = net_pnl × 10"""
    r_pos = calculate_rl_reward(round_trips=15, win_rate=0.5, net_pnl=1.0)
    r_neg = calculate_rl_reward(round_trips=15, win_rate=0.5, net_pnl=-1.0)
    # volume=50, wr=0, pnl=±10
    assert abs(r_pos - 60.0)  < 0.01, f"Expected +60.0, got {r_pos}"
    assert abs(r_neg - 40.0)  < 0.01, f"Expected +40.0, got {r_neg}"

def test_reward_win_rate_range():
    """Win rate score = (wr - 0.5) × 50 → range [-25, +25]"""
    r_min = calculate_rl_reward(round_trips=15, win_rate=0.0, net_pnl=0.0)
    r_max = calculate_rl_reward(round_trips=15, win_rate=1.0, net_pnl=0.0)
    assert abs(r_min - 25.0) < 0.01, f"Expected 25.0 (50-25), got {r_min}"
    assert abs(r_max - 75.0) < 0.01, f"Expected 75.0 (50+25), got {r_max}"


# ── 2. JSON sanitizer: parses grid_offset_ticks ───────────────────────────────

def test_extract_json_with_grid_offset_ticks():
    """extract_json handles grid_offset_ticks float field from Alpha output."""
    raw = '{"gamma":0.4,"min_spread_ticks":2.0,"tfi_threshold":75000,"max_active_tranches":7,"obi_threshold":0.8,"grid_offset_ticks":3.5,"reasoning":"test"}'
    result = extract_json(raw)
    assert result["grid_offset_ticks"] == 3.5
    assert result["max_active_tranches"] == 7
    assert result["gamma"] == 0.4

def test_extract_json_cro_with_final_grid_offset():
    """extract_json handles final_grid_offset_ticks from CRO output."""
    raw = '{"final_gamma":0.4,"final_min_spread_ticks":2.0,"final_tfi_threshold":75000.0,"final_obi_threshold":0.8,"final_max_active_tranches":7,"final_grid_offset_ticks":5.0,"override_applied":false,"panic_sell_flag":false,"cro_reasoning":"Approved."}'
    result = extract_json(raw)
    assert result["final_grid_offset_ticks"] == 5.0
    assert result["final_max_active_tranches"] == 7
    assert result["override_applied"] is False

def test_extract_json_strips_markdown():
    """extract_json handles ```json fences."""
    raw = '```json\n{"gamma":0.3,"min_spread_ticks":1.0,"tfi_threshold":50000,"max_active_tranches":5,"obi_threshold":0.7,"grid_offset_ticks":2.0,"reasoning":"ok"}\n```'
    result = extract_json(raw)
    assert result["grid_offset_ticks"] == 2.0
    assert result["max_active_tranches"] == 5


# ── 3. redis_bridge: publish_params includes grid_offset_ticks ────────────────

def test_publish_params_includes_grid_offset(monkeypatch):
    """publish_params injects final_grid_offset_ticks into Redis payload."""
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
        "final_grid_offset_ticks": 3.5,
    }
    redis_bridge.publish_params("ETHFDUSD", cro_output)

    assert published['data']['grid_offset_ticks'] == 3.5
    assert published['data']['max_active_tranches'] == 7
    assert published['data']['obi_threshold'] == 0.8
    assert published['data']['system_status'] == 'LIVE'

def test_safe_mode_has_grid_offset_2(monkeypatch):
    """Safe mode payload has grid_offset_ticks=2.0 (safe default)."""
    published = {}

    class MockRedis:
        def publish(self, channel, data): published['data'] = json.loads(data)
        def set(self, *a, **kw): pass

    import redis_bridge
    monkeypatch.setattr(redis_bridge, '_redis_client', MockRedis())

    redis_bridge.publish_safe_mode("ETHFDUSD")

    assert published['data']['grid_offset_ticks'] == 2.0
    assert published['data']['max_active_tranches'] == 1
    assert published['data']['system_status'] == 'SAFE_MODE_LOCKDOWN'


# ── 4. Default fallback: missing fields ───────────────────────────────────────

def test_publish_params_defaults_grid_offset_to_2(monkeypatch):
    """If CRO omits final_grid_offset_ticks, default is 2.0 (safe spacing)."""
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
        "final_max_active_tranches": 5,
        # no final_grid_offset_ticks
    }
    redis_bridge.publish_params("SOLFDUSD", cro_output)
    assert published['data']['grid_offset_ticks'] == 2.0

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
    redis_bridge.publish_params("DOGEFDUSD", cro_output)
    assert published['data']['max_active_tranches'] == 1


# ── 5. Reward function boundary conditions ────────────────────────────────────

def test_reward_exactly_at_boundary():
    """Reward at exactly target boundary (15 trips) is continuous."""
    r_below = calculate_rl_reward(round_trips=14, win_rate=0.5, net_pnl=0.0)
    r_at    = calculate_rl_reward(round_trips=15, win_rate=0.5, net_pnl=0.0)
    r_above = calculate_rl_reward(round_trips=16, win_rate=0.5, net_pnl=0.0)
    # Below target: volume = -200×(1-14/15) = -200×(1/15) ≈ -13.33
    # At target:    volume = 50.0
    # Above target: volume = 50 + (16-15)×2 = 52.0
    assert r_below < r_at < r_above, "Reward must be monotonically increasing with trips"
    assert abs(r_at - 50.0) < 0.01


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
