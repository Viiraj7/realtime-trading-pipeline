/*
This script activates the TimescaleDB extension
and creates our main 1-minute OHLCV hypertable.
*/

-- 1. ACTIVATE the TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 2. CREATE the main table just like before
CREATE TABLE IF NOT EXISTS stock_bars_1min (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    open NUMERIC(10, 4) NOT NULL,
    high NUMERIC(10, 4) NOT NULL,
    low NUMERIC(10, 4) NOT NULL,
    close NUMERIC(10, 4) NOT NULL,
    volume NUMERIC(15, 4) NOT NULL,
    PRIMARY KEY (timestamp, symbol)
);

-- 3. TRANSFORM the table into a high-performance Hypertable
-- This is the line you asked about, now active!
SELECT create_hypertable('stock_bars_1min', 'timestamp', if_not_exists => TRUE);