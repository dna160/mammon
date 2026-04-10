-- Project Mammon — Trade Telemetry Schema
-- PostgreSQL 15 + TimescaleDB

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ── Core trade telemetry (Engines A, B, C, D) ─────────────────────────────────
-- All monetary values are in IDR (Indonesian Rupiah).
-- engine_id: 'A' = Spatial Arb, 'B' = Stat Arb, 'C' = Microstructure, 'D' = HFT

CREATE TABLE IF NOT EXISTS trade_telemetry (
    trade_id             UUID        NOT NULL DEFAULT gen_random_uuid(),
    timestamp            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    engine_id            VARCHAR(1)  NOT NULL,
    asset_pair           VARCHAR(20),
    trade_size_idr       NUMERIC(20,4),   -- notional size in IDR
    entry_signal_value   NUMERIC(20,8),   -- spread%, Z-score, OFI target, OBI, etc.
    gross_pnl            NUMERIC(20,8),   -- PnL before fees (IDR)
    fees_paid            NUMERIC(20,8),   -- exchange fees (IDR)
    net_pnl              NUMERIC(20,8),   -- PnL after fees (IDR)
    trade_roe_pct        NUMERIC(10,6),   -- return on wallet %
    PRIMARY KEY (trade_id, timestamp)
);

SELECT create_hypertable('trade_telemetry', 'timestamp', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_trade_telemetry_engine_time
    ON trade_telemetry (engine_id, timestamp DESC);

-- ── Engine D — HFT real-time telemetry ────────────────────────────────────────
-- Receives 1-second snapshots written by the tokio::spawn telemetry offload
-- in agent_0_ingestion/src/main.rs (State 4 of the HFT state machine).
-- inventory_btc: net BTC position (positive = long, negative = short).
-- pnl_idr: cumulative simulated PnL in IDR.

CREATE TABLE IF NOT EXISTS engine_d_telemetry (
    id               BIGSERIAL,
    timestamp        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    inventory_btc    NUMERIC(20,8) NOT NULL DEFAULT 0,
    pnl_idr          NUMERIC(24,4) NOT NULL DEFAULT 0,
    total_trades     BIGINT        NOT NULL DEFAULT 0,
    variance         NUMERIC(30,16) NOT NULL DEFAULT 0,
    PRIMARY KEY (id, timestamp)
);

SELECT create_hypertable('engine_d_telemetry', 'timestamp', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_engine_d_telemetry_time
    ON engine_d_telemetry (timestamp DESC);
