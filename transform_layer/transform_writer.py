import asyncio
import json
import logging
import os
import signal
import string
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
from aiokafka import AIOKafkaConsumer

from transform_layer.config import Settings, load_settings


logger = logging.getLogger("transform_layer.transform_writer")


class TransformWriter:
    """Consume Kafka events, normalise them, and batch insert into Postgres."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._pg_pool: Optional[asyncpg.Pool] = None
        self._pending: List[Tuple[str, Tuple]] = []
        self._deferred_fk: List[Tuple[str, Tuple, datetime]] = []
        self._stop_event = asyncio.Event()
        self._table_order_book_updates = self._validate_table(settings.postgres.order_book_updates_table)
        self._table_order_book_snapshots = self._validate_table(settings.postgres.order_book_snapshots_table)
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
        

        if event_type == "book":
            return self._build_snapshot_records(base)
        if event_type == "price_change":
            return self._build_order_book_update_records(base, ingested_at)
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

    def _build_snapshot_records(self, base: Dict[str, Any]) -> List[Tuple[str, Tuple]]:
        """Convert a full book event into snapshot rows with full side JSON."""
        asset_id = _to_str(base.get("asset_id"))
        ts = _ms_to_datetime(base.get("timestamp"))
        if not asset_id or ts is None:
            return []

        records: List[Tuple[str, Tuple]] = []
        for side_key, side_value in (("bids", "bid"), ("asks", "ask")):
            levels = base.get(side_key)
            if not isinstance(levels, list):
                continue
            top = levels[-1] if levels else {}
            top_price = _to_float(top.get("price"))
            top_size = _to_float(top.get("size"))
            records.append(
                (
                    self._table_order_book_snapshots,
                    (
                        asset_id,
                        json.dumps(levels),
                        side_value,
                        top_price,
                        top_size,
                        ts,
                    ),
                )
            )
        return records

    def _build_order_book_update_records(self, base: Dict[str, Any], ingested_at: datetime) -> List[Tuple[str, Tuple]]:
        """Convert price_change events into order book updates."""
        update_ts = _ms_to_datetime(base.get("timestamp"))
        send_ts = update_ts
        arrival_ts = ingested_at

        price_changes = base.get("price_changes")
        records: List[Tuple[str, Tuple]] = []
        if isinstance(price_changes, list) and price_changes:
            for change in price_changes:
                if not isinstance(change, dict):
                    continue
                token_id = _to_str(change.get("asset_id"))
                price = _to_float(change.get("price"))
                size = _to_float(change.get("size"))
                side = change.get("side")
                side_value = "bid" if (isinstance(side, str) and side.upper() == "BUY") else "ask"
                if token_id and price is not None and size is not None and update_ts is not None:
                    records.append(
                        (
                            self._table_order_book_updates,
                            (
                                token_id,
                                price,
                                size,
                                side_value,
                                update_ts,
                                send_ts,
                                arrival_ts,
                            ),
                        )
                    )
            return records

        token_id = _to_str(base.get("asset_id"))
        price = _to_float(base.get("price"))
        size = _to_float(base.get("size"))
        side = base.get("side")
        side_value = "bid" if (isinstance(side, str) and side.upper() == "BUY") else "ask"
        if token_id and price is not None and size is not None and update_ts is not None:
            records.append(
                (
                    self._table_order_book_updates,
                    (
                        token_id,
                        price,
                        size,
                        side_value,
                        update_ts,
                        send_ts,
                        arrival_ts,
                    ),
                )
            )
        return records

    def _build_chainlink_records(
        self, base: Dict[str, Any], source: Optional[str], ingested_at: datetime
    ) -> List[Tuple[str, Tuple]]:
        payload = base.get("payload") if isinstance(base, dict) else {}
        update_ts = _ms_to_datetime(payload.get("timestamp"))
        send_ts = _ms_to_datetime(base.get("timestamp"))
        arrival_ts = ingested_at
        return [
            (
                self._table_chainlink,
                (
                    ingested_at,
                    source,
                    _to_str(payload.get("symbol")),
                    _to_float(payload.get("value")),
                    _to_str(payload.get("full_accuracy_value")),
                    update_ts,
                    send_ts,
                    arrival_ts,
                    json.dumps(base),
                ),
            )
        ]

    def _build_binance_records(
        self, base: Dict[str, Any], source: Optional[str], ingested_at: datetime
    ) -> List[Tuple[str, Tuple]]:
        payload = base.get("payload") if isinstance(base, dict) else {}
        update_ts = _ms_to_datetime(payload.get("timestamp"))
        send_ts = _ms_to_datetime(base.get("timestamp"))
        arrival_ts = ingested_at
        return [
            (
                self._table_binance,
                (
                    ingested_at,
                    source,
                    _to_str(payload.get("symbol")),
                    _to_float(payload.get("value")),
                    _to_str(payload.get("full_accuracy_value")),
                    update_ts,
                    send_ts,
                    arrival_ts,
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
        if not self._pg_pool:
            return
        now = datetime.now(timezone.utc)

        # Pull in any deferred FK-violating rows that are ready to retry.
        ready_deferred: List[Tuple[str, Tuple]] = []
        if self._deferred_fk:
            still_waiting: List[Tuple[str, Tuple, datetime]] = []
            for table, row, retry_at in self._deferred_fk:
                if retry_at <= now:
                    ready_deferred.append((table, row))
                else:
                    still_waiting.append((table, row, retry_at))
            self._deferred_fk = still_waiting

        if not self._pending and not ready_deferred:
            return

        records = self._pending + ready_deferred
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
            self._table_order_book_updates: (
                "INSERT INTO "
                f"{self._table_order_book_updates} "
                "(token_id, price, size, side, update_timestamp, send_timestamp, arrival_timestamp) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7)"
                "ON CONFLICT (token_id, price, size, side, update_timestamp) DO NOTHING"
            ),
            self._table_order_book_snapshots: (
                "INSERT INTO "
                f"{self._table_order_book_snapshots} "
                "(token_id, book, side, top_price, top_size, snapshot_timestamp) "
                "VALUES ($1, $2, $3, $4, $5, $6)"
                "ON CONFLICT (token_id, side, snapshot_timestamp) DO NOTHING"
            ),
            self._table_chainlink: (
                "INSERT INTO "
                f"{self._table_chainlink} "
                "(ingested_at, source, symbol, value, full_accuracy_value, update_timestamp, send_timestamp, arrival_timestamp, raw_payload) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) "
                "ON CONFLICT (symbol, value, update_timestamp) DO NOTHING"
            ),
            self._table_binance: (
                "INSERT INTO "
                f"{self._table_binance} "
                "(ingested_at, source, symbol, value, full_accuracy_value, update_timestamp, send_timestamp, arrival_timestamp, raw_payload) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) "
                "ON CONFLICT (symbol, value, update_timestamp) DO NOTHING"
            ),
        }

        retry_records: List[Tuple[str, Tuple]] = []
        deferred_fk_records = 0
        success_counts: Dict[str, int] = {}

        try:
            async with self._pg_pool.acquire() as conn:
                for table, rows in grouped.items():
                    if table not in sql_map:
                        logger.warning("No SQL mapping for table %s; skipping %d rows", table, len(rows))
                        continue
                    try:
                        async with conn.transaction():
                            await conn.executemany(sql_map[table], rows)
                        success_counts[table] = success_counts.get(table, 0) + len(rows)
                    except asyncpg.exceptions.ForeignKeyViolationError as exc:
                        retry_at = now + timedelta(seconds=self.settings.batch.fk_retry_backoff_seconds)
                        self._deferred_fk.extend((table, row, retry_at) for row in rows)
                        deferred_fk_records += len(rows)
                        logger.warning(
                            "Deferred %d rows for table %s due to foreign key violation (next retry after %s): %s; first_row=%s",
                            len(rows),
                            table,
                            retry_at.isoformat(),
                            exc,
                            rows[0] if rows else None,
                        )
                    except Exception as exc:  # noqa: BLE001
                        logger.error(
                            "Failed to flush %d rows into %s (transaction rolled back): %s",
                            len(rows),
                            table,
                            exc,
                        )
                        retry_records.extend((table, row) for row in rows)

            if success_counts:
                logger.info("Inserted rows by table (not yet committed): %s", success_counts)

            if retry_records:
                self._pending = retry_records + self._pending
                logger.warning(
                    "Re-queued %d rows for immediate retry; pending buffer size now %d",
                    len(retry_records),
                    len(self._pending),
                )
                return

            if self._consumer:
                await self._consumer.commit()

            if self._deferred_fk:
                next_retry = min((retry_at for _, _, retry_at in self._deferred_fk), default=None)
                pending_total = len(self._pending) + len(self._deferred_fk)
                logger.info(
                    "Committed offsets after inserting %d rows; deferred %d rows awaiting FK backfill (next retry %s); pending buffer size %d",
                    sum(success_counts.values()) if success_counts else 0,
                    deferred_fk_records,
                    next_retry.isoformat() if next_retry else "n/a",
                    pending_total,
                )
                return

            flushed = sum(success_counts.values()) if success_counts else 0
            logger.info(
                "Flushed %d records across %d tables; committed offsets",
                flushed,
                len(success_counts) if success_counts else len(grouped),
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


def _ms_to_datetime(value: object) -> Optional[datetime]:
    try:
        return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


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
