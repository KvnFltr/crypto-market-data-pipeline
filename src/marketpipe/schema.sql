-- marketpipe schema (PostgreSQL 15+ with TimescaleDB 2.13+). Idempotent: safe to run at every start.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- One row per trade, live (source = 'ws') or recovered from the REST history (source = 'rest').
CREATE TABLE IF NOT EXISTS trades (
    venue        text             NOT NULL,
    symbol       text             NOT NULL,
    trade_id     bigint           NOT NULL,
    price        double precision NOT NULL,
    qty          double precision NOT NULL,
    side         smallint         NOT NULL CHECK (side IN (-1, 1)),  -- aggressor: +1 buy, -1 sell
    exchange_ts  timestamptz      NOT NULL,  -- trade time on the venue
    event_ts     timestamptz,                -- publication time on the venue, when provided
    recv_ts      timestamptz,                -- local receipt time (raw clock); NULL if recovered
    source       text             NOT NULL DEFAULT 'ws',
    -- a unique index on a hypertable must contain the partitioning column
    PRIMARY KEY (venue, symbol, trade_id, exchange_ts)
);
SELECT create_hypertable('trades', by_range('exchange_ts', INTERVAL '1 day'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS trades_venue_symbol_ts ON trades (venue, symbol, exchange_ts DESC);

-- Best bid and offer from the local order books, one row per change.
CREATE TABLE IF NOT EXISTS quotes (
    venue        text             NOT NULL,
    symbol       text             NOT NULL,
    update_id    bigint           NOT NULL,
    bid_px       double precision NOT NULL,
    bid_qty      double precision NOT NULL,
    ask_px       double precision NOT NULL,
    ask_qty      double precision NOT NULL,
    exchange_ts  timestamptz      NOT NULL,
    recv_ts      timestamptz      NOT NULL
);
SELECT create_hypertable('quotes', by_range('exchange_ts', INTERVAL '1 day'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS quotes_venue_symbol_ts ON quotes (venue, symbol, exchange_ts DESC);

-- Pipeline runs, so that experiments (clean run, fault injection...) can be told apart.
CREATE TABLE IF NOT EXISTS runs (
    run_id      bigserial PRIMARY KEY,
    label       text        NOT NULL,
    started_at  timestamptz NOT NULL DEFAULT now(),
    ended_at    timestamptz,
    host        text,
    config      jsonb
);

-- Every detected run of missing sequence numbers, and what became of it.
CREATE TABLE IF NOT EXISTS gaps (
    gap_id          bigserial   PRIMARY KEY,
    run_id          bigint      REFERENCES runs,
    venue           text        NOT NULL,
    symbol          text        NOT NULL,
    stream          text        NOT NULL,  -- trades | book
    first_missing   bigint      NOT NULL,
    last_missing    bigint      NOT NULL,
    size            bigint      GENERATED ALWAYS AS (last_missing - first_missing + 1) STORED,
    detected_at     timestamptz NOT NULL,
    after_reconnect boolean     NOT NULL,
    status          text        NOT NULL,  -- open | recovered | partial | unrecoverable | resynced
    recovered       bigint      NOT NULL DEFAULT 0,
    resolved_at     timestamptz
);
CREATE INDEX IF NOT EXISTS gaps_detected ON gaps (detected_at);
ALTER TABLE gaps ADD COLUMN IF NOT EXISTS cause text;  -- in_session | reconnect | restart | heartbeat

CREATE TABLE IF NOT EXISTS connection_events (
    ts         timestamptz NOT NULL,
    run_id     bigint      REFERENCES runs,
    venue      text        NOT NULL,
    event      text        NOT NULL,  -- connected | disconnected
    reason     text        NOT NULL,
    session_s  double precision
);

-- Offset of the local clock against reference clocks (reference minus local).
CREATE TABLE IF NOT EXISTS clock_samples (
    ts         timestamptz      NOT NULL,
    run_id     bigint           REFERENCES runs,
    reference  text             NOT NULL,
    offset_s   double precision NOT NULL,
    delay_s    double precision NOT NULL
);

-- One-minute bars maintained incrementally by TimescaleDB.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'ohlcv_1m') THEN
        CREATE MATERIALIZED VIEW ohlcv_1m WITH (timescaledb.continuous) AS
        SELECT venue,
               symbol,
               time_bucket(INTERVAL '1 minute', exchange_ts) AS bucket,
               first(price, exchange_ts)                     AS open,
               max(price)                                    AS high,
               min(price)                                    AS low,
               last(price, exchange_ts)                      AS close,
               sum(qty)                                      AS volume,
               sum(price * qty) / nullif(sum(qty), 0)        AS vwap,
               count(*)                                      AS trades,
               sum(qty) FILTER (WHERE side = 1)              AS buy_volume
        FROM trades
        GROUP BY venue, symbol, bucket
        WITH NO DATA;
        PERFORM add_continuous_aggregate_policy('ohlcv_1m',
            start_offset => INTERVAL '2 hours',
            end_offset => INTERVAL '1 minute',
            schedule_interval => INTERVAL '1 minute');
    END IF;
END
$$;

-- Columnar compression of chunks older than one day.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM timescaledb_information.jobs
                   WHERE proc_name = 'policy_compression' AND hypertable_name = 'trades') THEN
        ALTER TABLE trades SET (timescaledb.compress,
                                timescaledb.compress_segmentby = 'venue, symbol',
                                timescaledb.compress_orderby = 'exchange_ts, trade_id');
        PERFORM add_compression_policy('trades', INTERVAL '1 day');
        ALTER TABLE quotes SET (timescaledb.compress,
                                timescaledb.compress_segmentby = 'venue, symbol',
                                timescaledb.compress_orderby = 'exchange_ts');
        PERFORM add_compression_policy('quotes', INTERVAL '1 day');
    END IF;
END
$$;
