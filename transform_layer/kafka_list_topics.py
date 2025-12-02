import logging
import os
from typing import Iterable

from kafka import KafkaAdminClient


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("kafka_list_topics")


def _filter_topics(topics: Iterable[str], include_internal: bool) -> list[str]:
    if include_internal:
        return sorted(topics)
    return sorted(t for t in topics if not t.startswith("__"))


def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    include_internal = os.getenv("INCLUDE_INTERNAL_TOPICS", "false").lower() in {"1", "true", "yes"}

    logger.info("Connecting to Kafka at %s", bootstrap)
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="topic-lister")
        topics = _filter_topics(admin.list_topics(), include_internal)
        logger.info("Found %d topics:", len(topics))
        for name in topics:
            print(name)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to list topics: %s", exc)


if __name__ == "__main__":
    main()
