-- Project Mammon V2 — Master Schema
-- PostgreSQL 15 + TimescaleDB

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ── Core trade telemetry ───────────────────────────────────────────────────────
-- All monetary values in USD (FDUSD).

CREATE TABLE IF NOT EXISTS trade_telemetry (
    trade_id             UUID        NOT NULL DEFAULT gen_random_uuid(),
    timestamp            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    engine_id            VARCHAR(1)  NOT NULL,
    asset_pair           VARCHAR(20),
    trade_size_idr       NUMERIC(20,4),   -- notional size
    entry_signal_value   NUMERIC(20,8),   -- OBI / TFI Z-score
    gross_pnl            NUMERIC(20,8),
    fees_paid            NUMERIC(20,8),
    net_pnl              NUMERIC(20,8),
    trade_roe_pct        NUMERIC(10,6),
    PRIMARY KEY (trade_id, timestamp)
);

SELECT create_hypertable('trade_telemetry', 'timestamp', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_trade_telemetry_engine_time
    ON trade_telemetry (engine_id, timestamp DESC);

-- ── Engine D — 1-second HFT snapshots ────────────────────────────────────────

CREATE TABLE IF NOT EXISTS engine_d_telemetry (
    id               BIGSERIAL,
    timestamp        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    symbol           VARCHAR(20)   NOT NULL DEFAULT 'UNKNOWN',
    inventory_coin   NUMERIC(20,8) NOT NULL DEFAULT 0,
    pnl_usd          NUMERIC(24,4) NOT NULL DEFAULT 0,
    total_trades     BIGINT        NOT NULL DEFAULT 0,
    variance         NUMERIC(30,16) NOT NULL DEFAULT 0,
    micro_price      NUMERIC(20,8) NOT NULL DEFAULT 0,
    ping_pong_state  SMALLINT      NOT NULL DEFAULT 0,  -- 0=Empty/Bidding, 1=Loaded/Asking
    regime           VARCHAR(50)   NOT NULL DEFAULT 'UNKNOWN',
    PRIMARY KEY (id, timestamp)
);

SELECT create_hypertable('engine_d_telemetry', 'timestamp', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_engine_d_telemetry_time
    ON engine_d_telemetry (timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_engine_d_telemetry_symbol_time
    ON engine_d_telemetry (symbol, timestamp DESC);

-- ── Agent Q Memory — RAG Hippocampus (PRD §5) ─────────────────────────────────
-- Each row = one 15-min tactical parameter decision + its measured outcome.
-- reward_score is back-filled 15 minutes after parameters are injected.
-- This table IS the self-learning memory injected into future LLM prompts.

CREATE TABLE IF NOT EXISTS agent_q_memory (
    id                 SERIAL PRIMARY KEY,
    timestamp          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    evaluated_at       TIMESTAMPTZ,                    -- when reward was scored
    symbol             VARCHAR(20)  NOT NULL,
    regime             VARCHAR(50)  NOT NULL,          -- Oracle regime at time of decision
    -- Actions proposed by Alpha + approved by CRO
    proposed_gamma         FLOAT   NOT NULL DEFAULT 0,
    proposed_min_spread    FLOAT   NOT NULL DEFAULT 0,
    tfi_threshold          FLOAT   NOT NULL DEFAULT 0,
    -- State vector snapshot at time of decision
    vol_bps            FLOAT   NOT NULL DEFAULT 0,     -- Micro-Volatility
    tfi_zscore         FLOAT   NOT NULL DEFAULT 0,     -- Order Flow Z-Score
    drift_bps          FLOAT   NOT NULL DEFAULT 0,     -- Market Drift
    native_spread      FLOAT   NOT NULL DEFAULT 0,     -- LOB Spread (ticks)
    -- Outcomes (back-filled after 15 min) — RL Hyper-Cadence metrics
    total_round_trips  INT     NOT NULL DEFAULT 0,   -- filled round trips in the 15m window
    win_rate_pct       FLOAT   NOT NULL DEFAULT 0.0, -- % of profitable round trips
    net_pnl            FLOAT   NOT NULL DEFAULT 0,
    adverse_selection  FLOAT   NOT NULL DEFAULT 0,
    reward_score       FLOAT   NOT NULL DEFAULT 0,   -- RenTech RL score (vol+winrate+pnl)
    -- LLM reasoning (for display)
    alpha_reasoning    TEXT,
    cro_reasoning      TEXT,
    override_applied   BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_agent_q_memory_symbol_time
    ON agent_q_memory (symbol, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_agent_q_memory_regime
    ON agent_q_memory (regime, reward_score DESC);

-- ── Execution Log — individual fill records ───────────────────────────────────
-- Written by the Rust engine via UDS (executionReport) events.
-- This is separate from trade_telemetry (which is the agent-level view).

CREATE TABLE IF NOT EXISTS execution_log (
    id               BIGSERIAL,
    timestamp        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    symbol           VARCHAR(20)  NOT NULL,
    side             VARCHAR(4)   NOT NULL,   -- 'BUY' or 'SELL'
    fill_price       NUMERIC(20,8) NOT NULL,
    fill_qty         NUMERIC(20,8) NOT NULL,
    notional_usd     NUMERIC(20,4) NOT NULL,
    fee_usd          NUMERIC(20,8) NOT NULL DEFAULT 0,
    net_pnl_usd      NUMERIC(20,8) NOT NULL DEFAULT 0,
    ping_pong_state  SMALLINT      NOT NULL DEFAULT 0,
    regime           VARCHAR(50),
    order_id         BIGINT,
    trade_id         BIGINT,
    PRIMARY KEY (id, timestamp)
);

SELECT create_hypertable('execution_log', 'timestamp', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_execution_log_symbol_time
    ON execution_log (symbol, timestamp DESC);
