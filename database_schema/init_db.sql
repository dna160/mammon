-- Project Mammon V2.2 — Master Schema
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

-- ── Agent Q Memory — RAG Hippocampus (V2.2 PRD §2) ───────────────────────────
-- Each row = one 3-minute tactical parameter decision + its measured outcome.
-- reward_score is back-filled 3 minutes after parameters are injected.
-- This table IS the self-learning memory injected into future LLM prompts.

CREATE TABLE IF NOT EXISTS agent_q_memory (
    id                    SERIAL PRIMARY KEY,
    timestamp             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    evaluated_at          TIMESTAMPTZ,                   -- when reward was scored (3m later)
    symbol                VARCHAR(20)  NOT NULL,
    regime                VARCHAR(50)  NOT NULL,         -- Oracle regime at time of decision
    -- AI Levers / Actions (proposed by Alpha, approved by CRO)
    proposed_gamma        FLOAT NOT NULL DEFAULT 0,      -- reservation-price aversion
    proposed_min_spread   FLOAT NOT NULL DEFAULT 0,      -- min spread in ticks
    tfi_threshold         FLOAT NOT NULL DEFAULT 0,      -- toxic-flow breaker (USD notional)
    max_active_tranches   INT   NOT NULL DEFAULT 1,      -- grid depth (1=ping-pong, 10=full)
    obi_threshold         FLOAT NOT NULL DEFAULT 1.0,    -- momentum shield (0=conservative, 1=permissive)
    grid_offset_ticks     FLOAT NOT NULL DEFAULT 2.0,    -- spacing between grid tranches (ticks)
    -- State vector snapshot at time of decision
    vol_bps               FLOAT NOT NULL DEFAULT 0,      -- Micro-Volatility (BPS)
    tfi_zscore            FLOAT NOT NULL DEFAULT 0,      -- Order Flow Z-Score
    drift_bps             FLOAT NOT NULL DEFAULT 0,      -- Market Drift (BPS)
    native_spread         FLOAT NOT NULL DEFAULT 0,      -- LOB Spread (ticks, 5m avg)
    -- Outcomes (back-filled 3 min later)
    total_round_trips     INT   NOT NULL DEFAULT 0,      -- SELL fills in the 3m window
    win_rate_pct          FLOAT NOT NULL DEFAULT 0.0,    -- % of profitable round trips
    net_pnl               FLOAT NOT NULL DEFAULT 0,      -- realized PnL in the window
    adverse_selection     FLOAT NOT NULL DEFAULT 0,      -- % of fills with negative PnL
    reward_score          FLOAT NOT NULL DEFAULT 0,      -- Hyper-Cadence RL score
    -- LLM reasoning (for display + RAG retrieval)
    alpha_reasoning       TEXT,
    cro_reasoning         TEXT,
    override_applied      BOOLEAN NOT NULL DEFAULT FALSE
);

-- ── Live migration: add new V2.2 columns if upgrading from V2.1 ───────────────
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='agent_q_memory' AND column_name='max_active_tranches') THEN
        ALTER TABLE agent_q_memory ADD COLUMN max_active_tranches INT NOT NULL DEFAULT 1;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='agent_q_memory' AND column_name='obi_threshold') THEN
        ALTER TABLE agent_q_memory ADD COLUMN obi_threshold FLOAT NOT NULL DEFAULT 1.0;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='agent_q_memory' AND column_name='grid_offset_ticks') THEN
        ALTER TABLE agent_q_memory ADD COLUMN grid_offset_ticks FLOAT NOT NULL DEFAULT 2.0;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_agent_q_memory_symbol_time
    ON agent_q_memory (symbol, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_agent_q_memory_regime
    ON agent_q_memory (regime, reward_score DESC);

CREATE INDEX IF NOT EXISTS idx_agent_q_memory_evaluated
    ON agent_q_memory (evaluated_at DESC NULLS LAST);

-- ── Execution Log — individual fill records ───────────────────────────────────
-- Written by the Rust engine via UDS (executionReport) events.

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

CREATE INDEX IF NOT EXISTS idx_execution_log_side_time
    ON execution_log (side, timestamp DESC);
