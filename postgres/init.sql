-- =============================================================================
-- DATABASE SCHEMA — Crypto Market Anomaly Detection
-- =============================================================================
-- This file runs ONCE automatically when PostgreSQL first starts.
-- PostgreSQL looks for .sql files inside /docker-entrypoint-initdb.d/
-- and executes them in alphabetical order.
--
-- We create 3 tables:
--   1. ohlc_candles    → stores computed OHLC candles from Flink windows
--   2. anomaly_events  → stores fraud/anomaly alerts detected by Flink CEP
--   3. price_ticker    → stores the latest price per symbol (updated every second)
-- =============================================================================


-- =============================================================================
-- TABLE 1: ohlc_candles
-- =============================================================================
-- WHAT IS OHLC?
-- OHLC = Open, High, Low, Close — the standard way to represent price
-- movement over a time window. Every candlestick on a trading chart
-- is one OHLC record.
--
-- Example: For BTCUSDT during 10:00–10:01
--   open_price  = 43200.00  (first trade price in the window)
--   high_price  = 43250.00  (highest trade price in the window)
--   low_price   = 43180.00  (lowest trade price in the window)
--   close_price = 43230.00  (last trade price in the window)
--   trade_volume = 1.2345   (total BTC traded in the window)
--   trade_count  = 47       (how many individual trades happened)
--
-- Flink writes one row here every 1 minute AND every 5 minutes per symbol.
-- Grafana reads this table to draw the candlestick chart.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ohlc_candles (

    -- Auto-incrementing unique ID for each row
    -- SERIAL = PostgreSQL shorthand for "integer that auto-increments"
    id           SERIAL PRIMARY KEY,

    -- Which crypto pair this candle is for: 'BTCUSDT', 'ETHUSDT', or 'BNBUSDT'
    -- VARCHAR(20) = variable-length text, max 20 characters
    -- NOT NULL = this field is required, Flink must always provide it
    symbol       VARCHAR(20)      NOT NULL,

    -- When this time window STARTED (e.g. 10:00:00 UTC)
    -- TIMESTAMPTZ = timestamp WITH timezone — always stored in UTC
    window_start TIMESTAMPTZ      NOT NULL,

    -- When this time window ENDED (e.g. 10:01:00 UTC)
    window_end   TIMESTAMPTZ      NOT NULL,

    -- Label for the window size: either '1min' or '5min'
    -- Grafana uses this to filter which candle size to display
    window_size  VARCHAR(5)       NOT NULL,

    -- OHLC price values
    -- DOUBLE PRECISION = 64-bit floating point number (good for prices)
    open_price   DOUBLE PRECISION NOT NULL,   -- first trade price in window
    high_price   DOUBLE PRECISION NOT NULL,   -- highest trade price in window
    low_price    DOUBLE PRECISION NOT NULL,   -- lowest trade price in window
    close_price  DOUBLE PRECISION NOT NULL,   -- last trade price in window

    -- Total quantity of crypto traded in this window
    -- e.g. 1.2345 means 1.2345 BTC was traded across all trades
    trade_volume DOUBLE PRECISION NOT NULL,

    -- How many individual trade events happened in this window
    -- INTEGER = whole number (you can't have 47.5 trades!)
    trade_count  INTEGER          NOT NULL,

    -- When Flink inserted this row into the database
    -- DEFAULT NOW() = automatically filled with current timestamp if not provided
    created_at   TIMESTAMPTZ      DEFAULT NOW()
);

-- CREATE INDEX = makes queries on this table much faster
-- Without an index, Grafana would scan every row to find BTCUSDT candles.
-- With this index, PostgreSQL jumps directly to the right rows.
--
-- We index on (symbol, window_start DESC) because Grafana always queries like:
--   WHERE symbol = 'BTCUSDT' AND window_start >= NOW() - INTERVAL '1 hour'
-- The DESC means newest candles are found first (faster for dashboards)
CREATE INDEX IF NOT EXISTS idx_ohlc_symbol_window
    ON ohlc_candles (symbol, window_start DESC);


-- =============================================================================
-- TABLE 2: anomaly_events
-- =============================================================================
-- WHAT IS THIS TABLE?
-- Every time Flink's CEP (Complex Event Processing) engine detects a
-- suspicious trading pattern, it writes one row here.
--
-- We detect 3 types of patterns:
--   WASH_TRADE   → price swings ±2% within 30s with volume >5× average
--                  (someone is buying and selling to themselves to inflate volume)
--   PUMP_AND_DUMP → price rises >5% in 2 min, then drops >4% in next 2 min
--                  (coordinated buy-up then sell-off to trap retail traders)
--   VOLUME_SPIKE → trade count in 10s is >10× the 10-min rolling average
--                  (sudden burst of activity, could be a flash crash)
--
-- Grafana reads this table for the "Anomaly Alerts" panel at the bottom.
-- =============================================================================

CREATE TABLE IF NOT EXISTS anomaly_events (

    id           SERIAL PRIMARY KEY,

    -- Which symbol triggered this anomaly
    symbol       VARCHAR(20)  NOT NULL,

    -- WHEN the anomaly was detected (not when Flink wrote it, but when it happened)
    detected_at  TIMESTAMPTZ  NOT NULL,

    -- Which CEP pattern fired: 'WASH_TRADE', 'PUMP_AND_DUMP', or 'VOLUME_SPIKE'
    pattern_type VARCHAR(50)  NOT NULL,

    -- How serious is this event: 'LOW', 'MEDIUM', or 'HIGH'
    -- Used by Grafana to color-code rows (red=HIGH, orange=MEDIUM, yellow=LOW)
    severity     VARCHAR(10)  NOT NULL,

    -- The price at the START of the detected pattern window
    -- NULL allowed because not all patterns have a meaningful start price
    price_start  DOUBLE PRECISION,

    -- The price at the END of the detected pattern window
    price_end    DOUBLE PRECISION,

    -- How much the price changed as a percentage
    -- Positive = price went up, Negative = price went down
    -- e.g. -4.2 means price dropped 4.2%
    price_change_pct DOUBLE PRECISION,

    -- Ratio of current volume to average volume
    -- e.g. 12.5 means current volume is 12.5× the average
    volume_ratio DOUBLE PRECISION,

    -- Human-readable description of what was detected
    -- TEXT = unlimited length string (unlike VARCHAR which has a limit)
    -- e.g. "Price swung 3.1% (hi=43250, lo=41900) with volume 7.2× average"
    description  TEXT,

    -- When Flink wrote this row to the database
    created_at   TIMESTAMPTZ  DEFAULT NOW()
);

-- Index for fast queries by symbol and time
-- Grafana queries: WHERE symbol = 'BTCUSDT' ORDER BY detected_at DESC LIMIT 50
CREATE INDEX IF NOT EXISTS idx_anomaly_symbol_time
    ON anomaly_events (symbol, detected_at DESC);


-- =============================================================================
-- TABLE 3: price_ticker
-- =============================================================================
-- WHAT IS THIS TABLE?
-- A simple "latest price" table — one row per symbol, updated every second.
-- Flink uses INSERT ... ON CONFLICT DO UPDATE (called "upsert") to keep
-- only the most recent price for each symbol.
--
-- Grafana uses this for the price stat panels at the top of the dashboard.
-- =============================================================================

CREATE TABLE IF NOT EXISTS price_ticker (

    -- Symbol is the PRIMARY KEY here — there is exactly ONE row per symbol
    -- PRIMARY KEY automatically creates a unique index on this column
    symbol           VARCHAR(20)      PRIMARY KEY,

    -- The most recent trade price
    price            DOUBLE PRECISION NOT NULL,

    -- Price change % compared to the price 1 minute ago
    -- Positive = price went up, Negative = price went down
    -- NULL on startup before we have historical data to compare
    price_change_pct DOUBLE PRECISION,

    -- Total volume traded in the last 1-minute window
    volume_1min      DOUBLE PRECISION,

    -- When Flink last updated this row
    updated_at       TIMESTAMPTZ      DEFAULT NOW()
);

-- =============================================================================
-- PERMISSIONS
-- =============================================================================
-- Grant the 'flink' user (defined in docker-compose.yml) full access
-- to all tables and sequences in the public schema.
--
-- GRANT ALL PRIVILEGES = can SELECT, INSERT, UPDATE, DELETE
-- ON ALL TABLES       = applies to every table we just created
-- ON ALL SEQUENCES    = applies to SERIAL columns (auto-increment counters)
-- =============================================================================

GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA public TO flink;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO flink;
