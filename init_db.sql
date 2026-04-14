-- ============================================================
-- Mammon V3 — PostgreSQL Schema (The Hippocampus)
-- Run automatically via docker-entrypoint-initdb.d on first boot
-- ============================================================

-- ── Trade Telemetry ──────────────────────────────────────────────────────────
-- Physical fills recorded by the Rust engine in real-time via UDS.
CREATE TABLE IF NOT EXISTS trade_telemetry (
    id              SERIAL PRIMARY KEY,
    timestamp       TIMESTAMPTZ DEFAULT NOW(),
    symbol          VARCHAR(20)  NOT NULL,
    side            VARCHAR(4)   NOT NULL DEFAULT 'BUY',  -- BUY or SELL
    filled_qty      FLOAT        NOT NULL DEFAULT 0.0,
    filled_price    FLOAT        NOT NULL DEFAULT 0.0,
    trade_size_usd  FLOAT        NOT NULL DEFAULT 0.0,
    gross_pnl       FLOAT        NOT NULL DEFAULT 0.0,
    fees_paid       FLOAT        NOT NULL DEFAULT 0.0,
    net_pnl         FLOAT        NOT NULL DEFAULT 0.0
);

-- ── Agent Q Memory ────────────────────────────────────────────────────────────
-- Records each 3-minute tactical cycle: AI decisions + measured outcomes.
-- This is the dataset that enables Reinforcement Learning.
CREATE TABLE IF NOT EXISTS agent_q_memory (
    id                    SERIAL PRIMARY KEY,
    timestamp             TIMESTAMPTZ DEFAULT NOW(),
    symbol                VARCHAR(20)  NOT NULL,
    regime                VARCHAR(50)  NOT NULL,
    -- V3 Sniper Levers (AI Decisions)
    momentum_trigger_obi  FLOAT        NOT NULL DEFAULT 0.55,
    take_profit_ticks     INT          NOT NULL DEFAULT 3,
    stop_loss_ticks       INT          NOT NULL DEFAULT 10,
    max_active_tranches   INT          NOT NULL DEFAULT 3,
    -- Outcomes (Measured from trade_telemetry at cycle close)
    total_round_trips     INT          NOT NULL DEFAULT 0,
    win_rate_pct          FLOAT        NOT NULL DEFAULT 0.0,
    net_pnl               FLOAT        NOT NULL DEFAULT 0.0,
    reward_score          FLOAT        NOT NULL DEFAULT 0.0
);

-- ── Indexes ───────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_trade_tel_symbol    ON trade_telemetry (symbol);
CREATE INDEX IF NOT EXISTS idx_trade_tel_timestamp ON trade_telemetry (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_aq_mem_symbol       ON agent_q_memory (symbol);
CREATE INDEX IF NOT EXISTS idx_aq_mem_timestamp    ON agent_q_memory (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_aq_mem_regime       ON agent_q_memory (regime);
