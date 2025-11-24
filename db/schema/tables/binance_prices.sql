CREATE TABLE IF NOT EXISTS binance_prices (
    id BIGSERIAL PRIMARY KEY,
    ingested_at TIMESTAMPTZ NOT NULL,
    source TEXT,
    symbol TEXT,
    value DOUBLE PRECISION,
    full_accuracy_value TEXT,
    timestamp_ms BIGINT,
    raw_payload JSONB NOT NULL
);
