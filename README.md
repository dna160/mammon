# Project Mammon — Algorithmic Arbitrage Simulation

A micro-capital multi-agent quantitative trading simulation for Indonesian crypto markets
(Tokocrypto & Indodax). Built as an empirical MVP to test spatial, statistical, and
microstructure arbitrage strategies.

## Quick Start

```bash
cd project_sniper
docker compose up --build
```

Access the dashboard at **http://localhost:3000**

> **Note:** First build compiles Rust and installs Python/Node dependencies. This takes
> 3-5 minutes. Subsequent starts are instant.

## Architecture

| Service             | Language    | Description                              | Port |
|---------------------|-------------|------------------------------------------|------|
| `agent_0`           | Rust        | Price simulator → Redis                  | —    |
| `agent_a`           | Python 3.11 | Spatial arbitrage engine                 | —    |
| `agent_b`           | Python 3.11 | Statistical arbitrage engine             | —    |
| `agent_c`           | Python 3.11 | Microstructure (OFI/VPIN/HJB) engine     | —    |
| `dashboard_backend` | Node.js 20  | REST API (Express)                       | 4000 |
| `dashboard_frontend`| React/Nginx | Telemetry dashboard                      | 3000 |
| `redis`             | Redis 7     | Shared price state cache                 | 6379 |
| `postgres`          | TimescaleDB | Trade telemetry database                 | 5432 |

## Engines

### Engine A — Spatial Arbitrage
- Monitors SOL/IDR and PEPE/IDR spread between Tokocrypto (ask) and Indodax (bid)
- Formula: `Spread = ((Indo_Bid - Toko_Ask) / Toko_Ask) * 100`
- Triggers when `Spread >= 1.07%` AND both top-of-book volumes >= 100,000 IDR
- Net PnL = `(100,000 IDR × Spread%) - (100,000 IDR × 0.92% fees)`

### Engine B — Statistical Arbitrage
- Mean-reversion on BTC/ETH price ratio (Tokocrypto only)
- 15-minute rolling Z-Score; triggers at `|Z| >= 2.0`
- Dynamic TP/SL set at entry based on expected ratio deviation

### Engine C — Microstructure (MFT)
- OFI → EMA → HJB optimal target on BTC/USDT 10-level LOB
- VPIN toxic-flow filter: pauses 300s when `VPIN > 0.75`
- Maker-only limit orders; exits when HJB target crosses zero

## Simulation Notes

- **No real exchange connections or API keys required.**
- `agent_0` generates realistic random-walk price data for all feeds.
- All trade executions are simulated and logged to PostgreSQL.
- Numba JIT in Engine C has a ~2-5s cold-start compile delay on first tick.

## Useful Commands

```bash
# Start everything
docker compose up --build

# Start in background
docker compose up --build -d

# View logs for a specific service
docker compose logs -f agent_c

# Stop and remove volumes
docker compose down -v

# Rebuild a single service
docker compose build agent_a && docker compose up -d agent_a
```

## Capital Constraints (Micro-MVP)

| Engine | Wallet         | Trade Size    |
|--------|----------------|---------------|
| A      | 1,000,000 IDR  | 100,000 IDR   |
| B      | ~60 USDT       | 10 USDT       |
| C      | ~60 USDT       | 0.001 BTC     |
