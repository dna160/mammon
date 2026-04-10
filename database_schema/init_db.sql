-- Project Mammon — Trade Telemetry Schema
-- PostgreSQL 15 + TimescaleDB

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS trade_telemetry (
    trade_id             UUID        NOT NULL DEFAULT gen_random_uuid(),
    timestamp            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    engine_id            VARCHAR(1)  NOT NULL,
    asset_pair           VARCHAR(20),
    trade_size_usdt_idr  NUMERIC(20,4),
    entry_signal_value   NUMERIC(20,8),
    gross_pnl            NUMERIC(20,8),
    fees_paid            NUMERIC(20,8),
    net_pnl              NUMERIC(20,8),
    trade_roe_pct        NUMERIC(10,6),
    PRIMARY KEY (trade_id, timestamp)
);

SELECT create_hypertable('trade_telemetry', 'timestamp', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_trade_telemetry_engine_time
    ON trade_telemetry (engine_id, timestamp DESC);
