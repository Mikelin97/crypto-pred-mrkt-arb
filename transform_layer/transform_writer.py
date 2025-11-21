import asyncio
import json
import logging
import signal
import string
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
from aiokafka import AIOKafkaConsumer

from transform_layer.config import Settings, load_settings


logger = logging.getLogger("transform_layer.transform_writer")


@dataclass
class TransformedRecord:
    ingested_at: datetime
    source: Optional[str]
    event_type: Optional[str]
    market: Optional[str]
    asset_id: Optional[str]
    side: Optional[str]
    price: Optional[float]
    size: Optional[float]
    hash: Optional[str]
    best_bid: Optional[float]
    best_ask: Optional[float]
    timestamp_ms: Optional[int]
    raw_payload: Dict[str, Any]

    def as_tuple(self) -> Tuple:
        return (
            self.ingested_at,
            self.source,
            self.event_type,
            self.market,
            self.asset_id,
            self.side,
            self.price,
            self.size,
            self.hash,
            self.best_bid,
            self.best_ask,
            self.timestamp_ms,
            json.dumps(self.raw_payload),
        )


class TransformWriter:
    """Consume Kafka events, normalise them, and batch insert into Postgres."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._pg_pool: Optional[asyncpg.Pool] = None
        self._pending: List[Tuple[str, Tuple]] = []
        self._stop_event = asyncio.Event()
        self._table_price_changes = self._validate_table(settings.postgres.target_table)
        self._table_chainlink = self._validate_table(settings.postgres.chainlink_table)
        self._table_binance = self._validate_table(settings.postgres.binance_table)
        self._table_passthrough = self._validate_table(os.getenv("PASSTHROUGH_TABLE", "kafka_raw_events"))
        self._bypass_parsing = os.getenv("BYPASS_PARSING", "").lower() in {"1", "true", "yes"}

    async def start(self) -> None:
        await self._start_clients()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._stop_event.set)
            except NotImplementedError:
                signal.signal(sig, lambda *_: self._stop_event.set())

        tasks = [
            asyncio.create_task(self._consume_loop(), name="kafka_consumer"),
            asyncio.create_task(self._periodic_flush(), name="periodic_flush"),
        ]
        try:
            await self._stop_event.wait()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self._flush()
            await self.close()

    async def close(self) -> None:
        if self._consumer:
            await self._consumer.stop()
        if self._pg_pool:
            await self._pg_pool.close()

    async def _start_clients(self) -> None:
        topics = {
            self.settings.kafka.raw_topic,
            self.settings.kafka.persistence_topic,
        }
        consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self.settings.kafka.bootstrap_servers,
            group_id=self.settings.kafka.consumer_group,
            enable_auto_commit=False,
            client_id=self.settings.kafka.client_id,
            value_deserializer=lambda data: json.loads(data.decode("utf-8")),
        )
        await consumer.start()
        self._consumer = consumer
        self._pg_pool = await asyncpg.create_pool(self.settings.postgres.dsn, min_size=1, max_size=4)
        logger.info("Transform writer started; consuming topics: %s", ", ".join(topics))

    async def _consume_loop(self) -> None:
        assert self._consumer is not None
        async for message in self._consumer:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "Consumed msg topic=%s partition=%s offset=%s",
                    getattr(message, "topic", None),
                    getattr(message, "partition", None),
                    getattr(message, "offset", None),
                )
            records = self._to_records(message.value)
            if records:
                self._pending.extend(records)
            if len(self._pending) >= self.settings.batch.batch_size:
                await self._flush()

    def _to_records(self, message: object) -> List[Tuple[str, Tuple]]:
        try:
            payload = message if isinstance(message, dict) else json.loads(str(message))
        except json.JSONDecodeError:
            logger.warning("Skipping non-JSON message: %s", message)
            return []

        if not isinstance(payload, dict):
            logger.warning("Skipping non-dict payload: %s", payload)
            return []

        base = payload.get("payload", payload)
        if not isinstance(base, dict):
            base = {"value": base}

        meta = base.get("meta") if isinstance(base.get("meta"), dict) else {}
        source = base.get("source") or meta.get("source")
        event_type = base.get("event_type") or meta.get("event_type")
        ingested_at = datetime.now(timezone.utc)
        

        if event_type == "price_change":
            return self._build_price_change_records(base, source, event_type, ingested_at)
        if source and source.lower() == "chainlink":
            return self._build_chainlink_records(base, source, ingested_at)
        if source and source.lower() == "binance":
            return self._build_binance_records(base, source, ingested_at)

        logger.debug(
            "Dropping message with source=%s event_type=%s keys=%s",
            source,
            event_type,
            list(base.keys()),
        )
        return []

    def _build_price_change_records(
        self, base: Dict[str, Any], source: Optional[str], event_type: Optional[str], ingested_at: datetime
    ) -> List[Tuple[str, Tuple]]:
        market = str(base.get("market")) if base.get("market") is not None else None
        timestamp_ms = _to_int(base.get("timestamp"))

        price_changes = base.get("price_changes")
        records: List[Tuple[str, Tuple]] = []
        if isinstance(price_changes, list) and price_changes:
            for change in price_changes:
                if not isinstance(change, dict):
                    continue
                records.append(
                    (
                        self._table_price_changes,
                        TransformedRecord(
                            ingested_at=ingested_at,
                            source=source,
                            event_type=event_type or "price_change",
                            market=market,
                            asset_id=_to_str(change.get("asset_id")),
                            side=_to_str(change.get("side")),
                            price=_to_float(change.get("price")),
                            size=_to_float(change.get("size")),
                            hash=_to_str(change.get("hash")),
                            best_bid=_to_float(change.get("best_bid")),
                            best_ask=_to_float(change.get("best_ask")),
                            timestamp_ms=timestamp_ms,
                            raw_payload=base,
                        ).as_tuple(),
                    )
                )
            return records

        records.append(
            (
                self._table_price_changes,
                TransformedRecord(
                    ingested_at=ingested_at,
                    source=source,
                    event_type=event_type,
                    market=market,
                    asset_id=_to_str(base.get("asset_id")),
                    side=_to_str(base.get("side")),
                    price=_to_float(base.get("price")),
                    size=_to_float(base.get("size")),
                    hash=_to_str(base.get("hash")),
                    best_bid=_to_float(base.get("best_bid")),
                    best_ask=_to_float(base.get("best_ask")),
                    timestamp_ms=timestamp_ms,
                    raw_payload=base,
                ).as_tuple(),
            )
        )
        return records

    def _build_chainlink_records(
        self, base: Dict[str, Any], source: Optional[str], ingested_at: datetime
    ) -> List[Tuple[str, Tuple]]:
        payload = base if isinstance(base, dict) else {}
        return [
            (
                self._table_chainlink,
                (
                    ingested_at,
                    source,
                    _to_str(payload.get("symbol")),
                    _to_float(payload.get("value")),
                    _to_str(payload.get("full_accuracy_value")),
                    _to_int(payload.get("timestamp")),
                    json.dumps(base),
                ),
            )
        ]

    def _build_binance_records(
        self, base: Dict[str, Any], source: Optional[str], ingested_at: datetime
    ) -> List[Tuple[str, Tuple]]:
        payload = base if isinstance(base, dict) else {}
        return [
            (
                self._table_binance,
                (
                    ingested_at,
                    source,
                    _to_str(payload.get("symbol")),
                    _to_float(payload.get("value")),
                    _to_str(payload.get("full_accuracy_value")),
                    _to_int(payload.get("timestamp")),
                    json.dumps(base),
                ),
            )
        ]

    async def _periodic_flush(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self.settings.batch.batch_interval_seconds
                )
            except asyncio.TimeoutError:
                await self._flush()

    async def _flush(self) -> None:
        if not self._pending or not self._pg_pool:
            return
        records = self._pending
        self._pending = []

        grouped: Dict[str, List[Tuple]] = {}
        for table, values in records:
            grouped.setdefault(table, []).append(values)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "Flushing grouped batches: %s",
                {table: len(rows) for table, rows in grouped.items()},
            )

        sql_map = {
            self._table_price_changes: (
                "INSERT INTO "
                f"{self._table_price_changes} "
                "(ingested_at, source, event_type, market, asset_id, side, price, size, hash, "
                "best_bid, best_ask, timestamp_ms, raw_payload) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)"
            ),
            self._table_chainlink: (
                "INSERT INTO "
                f"{self._table_chainlink} "
                "(ingested_at, source, symbol, value, full_accuracy_value, timestamp_ms, raw_payload) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7)"
            ),
            self._table_binance: (
                "INSERT INTO "
                f"{self._table_binance} "
                "(ingested_at, source, symbol, value, full_accuracy_value, timestamp_ms, raw_payload) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7)"
            ),
        }

        try:
            async with self._pg_pool.acquire() as conn:
                for table, rows in grouped.items():
                    if table not in sql_map:
                        logger.warning("No SQL mapping for table %s; skipping %d rows", table, len(rows))
                        continue
                    await conn.executemany(sql_map[table], rows)
            if self._consumer:
                await self._consumer.commit()
            logger.info(
                "Flushed %d records across %d tables", sum(len(rows) for rows in grouped.values()), len(grouped)
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to flush %d records: %s", len(records), exc)
            self._pending = records + self._pending

    def _validate_table(self, table: str) -> str:
        allowed = set(string.ascii_letters + string.digits + "_.")
        if all(ch in allowed for ch in table):
            return table
        raise ValueError(f"Unsafe characters in table name: {table!r}")


def _to_float(value: object) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: object) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_str(value: object) -> Optional[str]:
    if value is None:
        return None
    return str(value)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = load_settings()
    writer = TransformWriter(settings)
    asyncio.run(writer.start())


if __name__ == "__main__":
    main()
