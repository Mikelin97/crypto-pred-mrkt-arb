import os
from dataclasses import dataclass, field
from typing import List, Optional


def _split_env(value: str | None, default: List[str]) -> List[str]:
    if not value:
        return default
    return [token.strip() for token in value.split(",") if token.strip()]


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


@dataclass
class KafkaSettings:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    raw_topic: str = os.getenv("KAFKA_RAW_TOPIC", "market.raw")
    persistence_topic: str = os.getenv("KAFKA_PERSIST_TOPIC", "market.persisted")
    consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "transform-writer")
    client_id: str = os.getenv("KAFKA_CLIENT_ID", "transform-layer")


@dataclass
class RedisSettings:
    url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    channels: List[str] = field(
        default_factory=lambda: _split_env(
            os.getenv("REDIS_CHANNELS"),
            [
                "binance.crypto.prices",
                "chainlink.crypto.prices",
            ],
        )
    )
    channel_patterns: List[str] = field(
        default_factory=lambda: _split_env(os.getenv("REDIS_CHANNEL_PATTERNS"), [])
    )


@dataclass
class PostgresSettings:
    dsn: str = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/postgres")
    order_book_updates_table: str = os.getenv(
        "ORDER_BOOK_UPDATES_TABLE", os.getenv("TARGET_TABLE", "order_book_updates")
    )
    order_book_snapshots_table: str = os.getenv(
        "ORDER_BOOK_SNAPSHOTS_TABLE", "order_book_snapshots"
    )
    chainlink_table: str = os.getenv("CHAINLINK_TABLE", "chainlink_prices")
    binance_table: str = os.getenv("BINANCE_TABLE", "binance_prices")
    persistence_query: Optional[str] = os.getenv("PERSISTENCE_QUERY")
    persistence_poll_seconds: float = _get_float("PERSISTENCE_POLL_SECONDS", 60.0)
    persistence_max_rows: int = _get_int("PERSISTENCE_MAX_ROWS", 5000)


@dataclass
class BatchSettings:
    batch_size: int = _get_int("BATCH_SIZE", 500)
    batch_interval_seconds: float = _get_float("BATCH_INTERVAL_SECONDS", 1.0)
    insert_timeout_seconds: float = _get_float("INSERT_TIMEOUT_SECONDS", 5.0)
    fk_retry_backoff_seconds: float = _get_float("FK_RETRY_BACKOFF_SECONDS", 3600.0)


@dataclass
class Settings:
    kafka: KafkaSettings = field(default_factory=KafkaSettings)
    redis: RedisSettings = field(default_factory=RedisSettings)
    postgres: PostgresSettings = field(default_factory=PostgresSettings)
    batch: BatchSettings = field(default_factory=BatchSettings)


def load_settings() -> Settings:
    """Load settings from environment with sane defaults for local development."""
    return Settings()
