"""
conftest.py — Pytest configuration for Project Mammon tests.

All test modules embed pure-Python stubs of the engine formulas so the
test suite runs without Redis, PostgreSQL, or Numba installed.

To run against the real Engine C math with Numba:
  pip install numba numpy
  pytest tests/

To disable Numba JIT (faster cold start for CI):
  NUMBA_DISABLE_JIT=1 pytest tests/

To run live backend API tests:
  BACKEND_URL=http://localhost:4000 pytest tests/test_backend_api.py
"""
