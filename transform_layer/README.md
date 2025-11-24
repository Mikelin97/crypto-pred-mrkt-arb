# Transform Layer

This package sits between the inflow gateway (Redis) and the persistence tier (PostgreSQL) using Kafka as the transport. It contains two standalone services:

- **kafka_bridge.py** subscribes to the Redis channels published by `inflow_gateway/stream.py`, republishes those events to Kafka, and can optionally backfill messages by querying the persistence layer (PostgreSQL) and pushing the results into a Kafka topic that consumers can replay.
- **transform_writer.py** consumes from Kafka, performs a light transformation, and batches inserts into PostgreSQL so downstream consumers read from Kafka rather than querying the database directly.

Both services are plain asyncio scripts and can be run locally or inside a container.

## Key environment variables

Common:
- `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`)
- `KAFKA_RAW_TOPIC` – topic for live Redis fan-out
- `KAFKA_PERSIST_TOPIC` – topic carrying backfilled rows from Postgres
- `KAFKA_CONSUMER_GROUP` – group id for the writer.
- Python deps for the transform layer are listed in `transform_layer/requirements.txt`.

Kafka bridge:
- `REDIS_URL` – same Redis endpoint used by `inflow_gateway`.
- `REDIS_CHANNELS` – comma-separated Redis channels to subscribe to.
- `REDIS_CHANNEL_PATTERNS` – optional Redis channel glob patterns (e.g. `markets.crypto:*`) to capture dynamic market channels.
- `PERSISTENCE_QUERY` – SQL used to read existing rows to republish (e.g. `SELECT * FROM raw_events WHERE processed=false LIMIT 1000`).
- `PERSISTENCE_POLL_SECONDS` (default `60`) – how often to re-run the persistence query.
- `PERSISTENCE_MAX_ROWS` (default `5000`) – safety cap if your query omits a `LIMIT`.

Transform writer:
- `POSTGRES_DSN` – e.g. `postgresql://user:pass@localhost:5432/dbname`.
- `TARGET_TABLE` – table to write price changes into (defaults to `kafka_price_changes`).
- `CHAINLINK_TABLE` – table for chainlink price snapshots (default `chainlink_prices`).
- `BINANCE_TABLE` – table for binance price snapshots (default `binance_prices`).
- `BATCH_SIZE` (default `500`) and `BATCH_INTERVAL_SECONDS` (default `1.0`) control batching.

## Running the services

```
python -m transform_layer.kafka_bridge
python -m transform_layer.transform_writer
python -m transform_layer.kafka_sub  # simple topic tail for testing
```

If you prefer containers, use:
- `persistence/docker-compose.yaml` to run Postgres on the shared `data_pipeline` network (container name `persistence_postgres`).
- `transform_layer/docker-compose.yaml` to run Apache Kafka (single-node KRaft, no ZooKeeper) plus the transform services using the `apache/kafka-native` image. It joins the `data_pipeline` network to reach Postgres and the `inflow_gateway` network to reach Redis. Set `REDIS_URL=redis://redis:6379/0` (or your host) and ensure both networks exist (`docker network create inflow_gateway data_pipeline` if absent). Host port for Kafka defaults to `19092`; container port remains `9092` (and is also advertised as `EXTERNAL://localhost:19092` for host tooling).
  - Kafka has a TCP healthcheck; `kafka_bridge` and `transform_writer` wait for it before starting. Transform-layer Python deps live in `transform_layer/requirements.txt` (now includes `pandas`).
