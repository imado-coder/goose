CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

CREATE TABLE IF NOT EXISTS trades (
    ts_exchange TIMESTAMPTZ NOT NULL, ts_received TIMESTAMPTZ NOT NULL,
    venue TEXT NOT NULL, symbol TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL, qty DOUBLE PRECISION NOT NULL,
    side TEXT NOT NULL, trade_id BIGINT NOT NULL, seq BIGINT DEFAULT 0
);
SELECT create_hypertable('trades','ts_exchange',if_not_exists=>TRUE);
CREATE INDEX IF NOT EXISTS trades_sym_ts ON trades (symbol, ts_exchange DESC);

CREATE TABLE IF NOT EXISTS book_top (
    ts_exchange TIMESTAMPTZ NOT NULL, ts_received TIMESTAMPTZ NOT NULL,
    venue TEXT NOT NULL, symbol TEXT NOT NULL, kind TEXT NOT NULL,
    bids JSONB NOT NULL, asks JSONB NOT NULL, last_update_id BIGINT NOT NULL
);
SELECT create_hypertable('book_top','ts_exchange',if_not_exists=>TRUE);
CREATE INDEX IF NOT EXISTS book_sym_ts ON book_top (symbol, ts_exchange DESC);

CREATE TABLE IF NOT EXISTS open_interest (
    ts_exchange TIMESTAMPTZ NOT NULL, ts_received TIMESTAMPTZ NOT NULL,
    venue TEXT NOT NULL, symbol TEXT NOT NULL,
    open_interest DOUBLE PRECISION NOT NULL, open_interest_value DOUBLE PRECISION NOT NULL DEFAULT 0
);
SELECT create_hypertable('open_interest','ts_exchange',if_not_exists=>TRUE);

CREATE TABLE IF NOT EXISTS funding (
    ts_exchange TIMESTAMPTZ NOT NULL, ts_received TIMESTAMPTZ NOT NULL,
    venue TEXT NOT NULL, symbol TEXT NOT NULL,
    funding_rate DOUBLE PRECISION NOT NULL, next_funding_time TIMESTAMPTZ NOT NULL
);
SELECT create_hypertable('funding','ts_exchange',if_not_exists=>TRUE);

CREATE TABLE IF NOT EXISTS liquidations (
    ts_exchange TIMESTAMPTZ NOT NULL, ts_received TIMESTAMPTZ NOT NULL,
    venue TEXT NOT NULL, symbol TEXT NOT NULL,
    side TEXT NOT NULL, price DOUBLE PRECISION NOT NULL,
    qty DOUBLE PRECISION NOT NULL, order_type TEXT NOT NULL
);
SELECT create_hypertable('liquidations','ts_exchange',if_not_exists=>TRUE);
CREATE INDEX IF NOT EXISTS liq_sym_ts ON liquidations (symbol, ts_exchange DESC);

CREATE TABLE IF NOT EXISTS heartbeats (
    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source TEXT NOT NULL, event_count BIGINT NOT NULL DEFAULT 0, note TEXT
);
SELECT create_hypertable('heartbeats','ts',if_not_exists=>TRUE);

CREATE TABLE IF NOT EXISTS data_gaps (
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source TEXT NOT NULL, symbol TEXT NOT NULL,
    gap_start TIMESTAMPTZ NOT NULL, gap_end TIMESTAMPTZ,
    gap_sec DOUBLE PRECISION, resolved BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_1m
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 minute',ts_exchange) AS bucket, symbol, venue,
    first(price,ts_exchange) AS open, max(price) AS high,
    min(price) AS low, last(price,ts_exchange) AS close,
    sum(qty) AS volume,
    sum(CASE WHEN side='buy' THEN qty ELSE 0 END) - sum(CASE WHEN side='sell' THEN qty ELSE 0 END) AS delta
FROM trades GROUP BY bucket,symbol,venue WITH NO DATA;

SELECT add_continuous_aggregate_policy('ohlcv_1m',
    start_offset=>INTERVAL '10 minutes', end_offset=>INTERVAL '1 minute',
    schedule_interval=>INTERVAL '1 minute', if_not_exists=>TRUE);

SELECT add_compression_policy('trades',       INTERVAL '7 days',if_not_exists=>TRUE);
SELECT add_compression_policy('book_top',     INTERVAL '7 days',if_not_exists=>TRUE);
SELECT add_compression_policy('open_interest',INTERVAL '7 days',if_not_exists=>TRUE);
SELECT add_compression_policy('funding',      INTERVAL '7 days',if_not_exists=>TRUE);
SELECT add_compression_policy('liquidations', INTERVAL '7 days',if_not_exists=>TRUE);
