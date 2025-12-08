CREATE TABLE IF NOT EXISTS chainlink_prices (
    id BIGSERIAL PRIMARY KEY,
    ingested_at TIMESTAMPTZ NOT NULL,
    source TEXT,
    symbol TEXT,
    value DOUBLE PRECISION,
    full_accuracy_value TEXT,
    update_timestamp TIMESTAMPTZ(3) NOT NULL, -- timestamp of update
    send_timestamp TIMESTAMPTZ(3), -- timestamp POLYMARKET SENDS US
    arrival_timestamp TIMESTAMPTZ(3), -- timestamp we got the update
    raw_payload JSONB NOT NULL
);

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uniq_chainlink
ON chainlink_prices (symbol, value, update_timestamp);
