import asyncio
import json
import logging
import signal
from typing import Any, Dict, Iterable, List, Optional

import asyncpg
from aiokafka import AIOKafkaProducer

from shared.subscriber import RedisSubscriber
from transform_layer.config import Settings, load_settings


logger = logging.getLogger("transform_layer.kafka_bridge")


class KafkaBridge:
    """Fan Redis events and optional Postgres snapshots into Kafka topics."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._producer: Optional[AIOKafkaProducer] = None
        self._redis = RedisSubscriber(settings.redis.url)
        self._stop_event = asyncio.Event()
        self._pg_pool: Optional[asyncpg.Pool] = None

    async def start(self) -> None:
        await self._start_producer()
        if self.settings.postgres.persistence_query:
            self._pg_pool = await asyncpg.create_pool(self.settings.postgres.dsn, min_size=1, max_size=2)

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._stop_event.set)
            except NotImplementedError:
                signal.signal(sig, lambda *_: self._stop_event.set())

        tasks = [
            asyncio.create_task(self._run_redis_consumer(), name="redis_consumer"),
        ]

        if self.settings.postgres.persistence_query:
            tasks.append(asyncio.create_task(self._run_persistence_loop(), name="persistence_loop"))

        try:
            await self._stop_event.wait()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.close()

    async def close(self) -> None:
        if self._producer:
            await self._producer.stop()
        await self._redis.close()
        if self._pg_pool:
            await self._pg_pool.close()

    async def _start_producer(self) -> None:
        producer = AIOKafkaProducer(
            bootstrap_servers=self.settings.kafka.bootstrap_servers,
            value_serializer=lambda payload: json.dumps(payload, default=str).encode("utf-8"),
            client_id=self.settings.kafka.client_id,
        )
        await producer.start()
        self._producer = producer
        logger.info("Kafka producer started for %s", self.settings.kafka.bootstrap_servers)

    async def _run_redis_consumer(self) -> None:
        channels = self.settings.redis.channels
        patterns = self.settings.redis.channel_patterns
        log_parts = []
        if channels:
            log_parts.append(f"channels={', '.join(channels)}")
        if patterns:
            log_parts.append(f"patterns={', '.join(patterns)}")
        logger.info("Subscribing to Redis %s", "; ".join(log_parts) if log_parts else "nothing")
        async for channel, payload in self._redis.iter_messages(channels, patterns=patterns):
            await self._publish_to_kafka(
                topic=self.settings.kafka.raw_topic,
                message={
                    "source": "redis",
                    "channel": channel,
                    "payload": payload,
                },
            )

    async def _run_persistence_loop(self) -> None:
        assert self._pg_pool is not None
        poll_seconds = max(self.settings.postgres.persistence_poll_seconds, 5.0)
        logger.info(
            "Starting persistence polling every %.1fs with query: %s",
            poll_seconds,
            self.settings.postgres.persistence_query,
        )
        while not self._stop_event.is_set():
            await self._publish_persistence_snapshot()
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=poll_seconds)
            except asyncio.TimeoutError:
                continue

    async def _publish_persistence_snapshot(self) -> None:
        if not self.settings.postgres.persistence_query or not self._pg_pool:
            return
        try:
            rows = await self._fetch_rows(
                self.settings.postgres.persistence_query,
                self.settings.postgres.persistence_max_rows,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to fetch persistence rows: %s", exc)
            return

        if not rows:
            return

        logger.info("Publishing %d rows from persistence to %s", len(rows), self.settings.kafka.persistence_topic)
        for row in rows:
            await self._publish_to_kafka(
                topic=self.settings.kafka.persistence_topic,
                message={"source": "persistence", "payload": row},
            )

    async def _fetch_rows(self, query: str, max_rows: int) -> List[Dict[str, Any]]:
        assert self._pg_pool is not None
        text = query.strip().rstrip(";")
        if "limit" not in text.lower():
            text = f"{text} LIMIT {max_rows}"
        async with self._pg_pool.acquire() as conn:
            results = await conn.fetch(text)
        rows: List[Dict[str, Any]] = []
        for row in results:
            rows.append(dict(row))
        return rows

    async def _publish_to_kafka(self, *, topic: str, message: Dict[str, Any]) -> None:
        if not self._producer:
            raise RuntimeError("Producer is not started")
        try:
            await self._producer.send_and_wait(topic, message)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to publish to Kafka topic %s: %s", topic, exc)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = load_settings()
    bridge = KafkaBridge(settings)
    asyncio.run(bridge.start())


if __name__ == "__main__":
    main()
