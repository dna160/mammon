"""
Tests for the Express.js dashboard backend API.

Live mode: Set BACKEND_URL=http://localhost:4000 to test against a running server.
Stub mode: Default — uses an in-process stub that mirrors the API contract.
"""

import os
import json
import pytest
from datetime import datetime, timezone

BACKEND_URL = os.getenv("BACKEND_URL", "")


# ── Stub client (default, no live server needed) ──────────────────────────────

STUB_KPIS = {
    "A": {"daily_trades": 12, "daily_roe": 0.1800},
    "B": {"daily_trades":  5, "daily_roe": 0.0250},
    "C": {"daily_trades":  3, "daily_roe": -0.0120},
}

STUB_TRADES = [
    {
        "trade_id":            "00000000-0000-0000-0000-000000000001",
        "timestamp":           datetime.now(timezone.utc).isoformat(),
        "engine_id":           "A",
        "asset_pair":          "SOL/IDR",
        "trade_size_usdt_idr": "100000.0000",
        "entry_signal_value":  "1.12000000",
        "gross_pnl":           "1120.00000000",
        "fees_paid":           "920.00000000",
        "net_pnl":             "200.00000000",
        "trade_roe_pct":       "0.020000",
    },
    {
        "trade_id":            "00000000-0000-0000-0000-000000000002",
        "timestamp":           datetime.now(timezone.utc).isoformat(),
        "engine_id":           "B",
        "asset_pair":          "BTC_ETH_RATIO",
        "trade_size_usdt_idr": "10.0000",
        "entry_signal_value":  "2.30000000",
        "gross_pnl":           "0.05000000",
        "fees_paid":           "0.02000000",
        "net_pnl":             "0.03000000",
        "trade_roe_pct":       "0.000100",
    },
    {
        "trade_id":            "00000000-0000-0000-0000-000000000003",
        "timestamp":           datetime.now(timezone.utc).isoformat(),
        "engine_id":           "C",
        "asset_pair":          "BTC_USDT",
        "trade_size_usdt_idr": "68.0000",
        "entry_signal_value":  "9.50000000",
        "gross_pnl":           "0.01000000",
        "fees_paid":           "0.00500000",
        "net_pnl":             "0.00500000",
        "trade_roe_pct":       "0.000008",
    },
]

STUB_CHART = [
    {"time": "2024-01-01T00:00:00.000Z", "A": 0.01, "B": 0.005, "C": -0.002},
    {"time": "2024-01-01T00:01:00.000Z", "A": 0.02, "B": 0.008, "C": 0.001},
]


class _StubResponse:
    def __init__(self, data, status=200):
        self._data   = data
        self.status_code = status

    def json(self):
        return self._data

    @property
    def ok(self):
        return self.status_code < 400


class _StubClient:
    """Mimics requests.Session but returns stub data."""
    def get(self, url, **_kwargs):
        if url.endswith('/health'):
            return _StubResponse({"status": "ok"})
        if url.endswith('/api/kpis'):
            return _StubResponse(STUB_KPIS)
        if url.endswith('/api/trades'):
            return _StubResponse(STUB_TRADES)
        if url.endswith('/api/chart'):
            return _StubResponse(STUB_CHART)
        return _StubResponse({}, status=404)


@pytest.fixture
def client():
    if BACKEND_URL:
        import requests
        s = requests.Session()
        s.base_url = BACKEND_URL.rstrip('/')
        s.get_url  = lambda path: s.get(s.base_url + path)
        return s
    else:
        c = _StubClient()
        c.get_url = lambda path: c.get('http://stub' + path)
        return c


# ── Health ────────────────────────────────────────────────────────────────────

def test_health_returns_200(client):
    r = client.get_url('/health')
    assert r.status_code == 200


def test_health_has_status_ok(client):
    r = client.get_url('/health')
    assert r.json()['status'] == 'ok'


# ── /api/kpis ─────────────────────────────────────────────────────────────────

def test_kpis_returns_all_engines(client):
    r = client.get_url('/api/kpis')
    data = r.json()
    assert 'A' in data
    assert 'B' in data
    assert 'C' in data


def test_kpis_engine_a_has_required_fields(client):
    data = client.get_url('/api/kpis').json()
    assert 'daily_trades' in data['A']
    assert 'daily_roe'    in data['A']


def test_kpis_engine_b_has_required_fields(client):
    data = client.get_url('/api/kpis').json()
    assert 'daily_trades' in data['B']
    assert 'daily_roe'    in data['B']


def test_kpis_engine_c_has_required_fields(client):
    data = client.get_url('/api/kpis').json()
    assert 'daily_trades' in data['C']
    assert 'daily_roe'    in data['C']


def test_kpis_daily_trades_is_int(client):
    data = client.get_url('/api/kpis').json()
    for eng in ['A', 'B', 'C']:
        assert isinstance(data[eng]['daily_trades'], int)


def test_kpis_daily_roe_is_numeric(client):
    data = client.get_url('/api/kpis').json()
    for eng in ['A', 'B', 'C']:
        float(data[eng]['daily_roe'])  # must not raise


# ── /api/trades ───────────────────────────────────────────────────────────────

def test_trades_returns_list(client):
    data = client.get_url('/api/trades').json()
    assert isinstance(data, list)


def test_trades_max_50(client):
    data = client.get_url('/api/trades').json()
    assert len(data) <= 50


def test_trades_required_fields(client):
    data = client.get_url('/api/trades').json()
    if not data:
        pytest.skip("No trade data available")
    required = ['trade_id', 'timestamp', 'engine_id', 'net_pnl', 'trade_roe_pct']
    for field in required:
        assert field in data[0], f"Missing field: {field}"


def test_trades_engine_id_valid(client):
    data = client.get_url('/api/trades').json()
    for row in data:
        assert row['engine_id'] in ('A', 'B', 'C')


def test_trades_net_pnl_parseable(client):
    data = client.get_url('/api/trades').json()
    for row in data:
        float(row['net_pnl'])  # must not raise


# ── /api/chart ────────────────────────────────────────────────────────────────

def test_chart_returns_list(client):
    data = client.get_url('/api/chart').json()
    assert isinstance(data, list)


def test_chart_has_time_key(client):
    data = client.get_url('/api/chart').json()
    if not data:
        pytest.skip("No chart data available")
    assert 'time' in data[0]


def test_chart_has_engine_keys(client):
    data = client.get_url('/api/chart').json()
    if not data:
        pytest.skip("No chart data available")
    for key in ('A', 'B', 'C'):
        assert key in data[0]


# ── 404 ───────────────────────────────────────────────────────────────────────

def test_unknown_route_returns_404(client):
    r = client.get_url('/api/this-route-does-not-exist')
    assert r.status_code == 404
