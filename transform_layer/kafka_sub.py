import asyncio
import json
import logging
import os

from aiokafka import AIOKafkaConsumer

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("kafka_sub")


async def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    topic = os.getenv("KAFKA_TOPIC", "market.raw")
    group = os.getenv("KAFKA_CONSUMER_GROUP", "kafka-sub-test")

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group,
        enable_auto_commit=False,
        value_deserializer=lambda data: json.loads(data.decode("utf-8")),
    )
    await consumer.start()
    logger.info("Subscribed to %s on %s (group=%s)", topic, bootstrap, group)
    try:
        async for msg in consumer:
            logger.info(
                "topic=%s partition=%s offset=%s value=%s",
                msg.topic,
                msg.partition,
                msg.offset,
                msg.value,
            )
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
