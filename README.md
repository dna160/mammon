# Mammon V3 — HFT System

A two-node crypto HFT system for Binance built on **Rust** (execution) + **Python/LM Studio** (AI cognition).

```
Binance WSS (bookTicker)
        ↓
┌────────────────────────┐      Redis: hft:live_params:{symbol}
│  Rust Execution Node   │ ←────────────────────────────────────┐
│  sniper_hft.rs         │                                       │
│  Tick → Order Manager  │      Redis: hft:tfi:{symbol}         │
│  UDS Fill Tracker      │ ─────────────────────────────────────┤
└────────────────────────┘                                       │
        ↓ (Pg write)                              ┌─────────────────────────┐
┌────────────────────────┐                        │  Python Cognitive Node  │
│     PostgreSQL         │ ──── RAG Memory ──────▶│  Agent Q (LM Studio)   │
│  trade_telemetry       │                        │  Oracle → Alpha → Risk  │
│  agent_q_memory        │                        └─────────────────────────┘
└────────────────────────┘
```

---

## Prerequisites

| Tool | Purpose |
|---|---|
| Docker + Docker Compose v2 | Runs the entire stack |
| LM Studio (`http://127.0.0.1:1234`) | Local LLM for Agent Q |

> **LM Studio**: Load any chat model (Mistral, Llama 3, Qwen, etc.) and start the server. The cognitive node connects automatically.

---

## Quick Start

```bash
# 1. Clone / enter the directory
cd mammonv3

# 2. Review / edit the symbol (default: BTCUSDT)
nano .env          # set SYMBOL=BTCUSDT (or ETHUSDT, etc.)

# 3. Start LM Studio on your Mac, load a model, click "Start Server"
#    (server must be on http://127.0.0.1:1234)

# 4. Boot the full stack
docker compose up --build

# 5. Watch logs
docker compose logs -f execution_node   # Rust sniper
docker compose logs -f cognitive_node   # Agent Q
```

---

## Environment Variables (`.env`)

| Variable | Default | Description |
|---|---|---|
| `SYMBOL` | `BTCUSDT` | Trading pair |
| `BINANCE_API_KEY` | *(set)* | Binance REST/WS |
| `BINANCE_API_SECRET` | *(set)* | Binance signing |
| `LM_STUDIO_URL` | `http://127.0.0.1:1234` | LM Studio server |
| `LM_MODEL` | `local-model` | Model name in LM Studio |
| `REDIS_PASSWORD` | `mammon_hft_redis` | Redis auth |
| `POSTGRES_DB/USER/PASSWORD` | `mammon` | Postgres auth |

---

## Project Structure

```
mammonv3/
├── .env                          # All credentials & config
├── docker-compose.yml            # 4-service stack
├── init_db.sql                   # Postgres schema (auto-runs on first boot)
│
├── execution_node/               # RUST — The Sniper
│   ├── Cargo.toml
│   ├── Dockerfile
│   └── src/
│       ├── main.rs               # WSS multiplexer + UDS fill tracker
│       ├── sniper_hft.rs         # Deterministic state machine
│       └── binance_v3.rs         # REST client (LIMIT_MAKER only)
│
└── cognitive_node/               # PYTHON — Agent Q Brain
    ├── Dockerfile
    ├── requirements.txt
    ├── agent_q_loop.py           # Main orchestration loop
    ├── semantic_watcher.py       # Math → Text state vectors
    ├── memory_ledger.py          # RL reward + RAG memory
    └── prompts/
        ├── oracle.txt            # 5-min macro classifier
        ├── alpha.txt             # 3-min lever optimizer
        └── risk_manager.txt      # Hard safety bounds
```

---

## Architecture Rules (Enforced in Code)

### Rust Engine (Ironclad — No Exceptions)

1. **Never Average Up** — `jump_bid >= aep` blocks the entry unconditionally
2. **Dust Recovery** — inventory below $5.10 notional → bid to accumulate before selling
3. **Zero-Latency Fills** — only UDS WebSocket updates `aep` + `inventory_coin`, never REST polling
4. **Post-Only Orders** — every order uses `type=LIMIT_MAKER` + `selfTradePreventionMode=EXPIRE_MAKER`

### Agent Q Cadence

| Agent | Interval | Redis Output |
|---|---|---|
| Oracle | 5 minutes | `hft:regime:{symbol}` |
| Tactical Alpha + Risk Manager | 3 minutes | `hft:live_params:{symbol}` |

### RL Reward Function (Cadence-First)

```
Target: 15 round trips per 3 minutes

If trips < 15:  reward += -200.0 × (1 - trips/15)   ← severe exponential penalty
If trips ≥ 15:  reward += 50.0 + (trips-15) × 2.0
                reward += (win_rate - 0.5) × 50.0
                reward += pnl × 10.0
```

---

## Redis Key Reference

| Key | Writer | Reader | TTL |
|---|---|---|---|
| `hft:live_params:{symbol}` | Python (Risk Manager) | Rust (every 30s) | 200s |
| `hft:regime:{symbol}` | Python (Oracle) | Python (Alpha) | 360s |
| `hft:tfi:{symbol}` | Rust (trade flow) | Python (semantic_watcher) | — |

---

## Stopping

```bash
docker compose down          # stop containers, keep volumes
docker compose down -v       # stop + wipe Postgres/Redis data
```
