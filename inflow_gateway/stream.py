import asyncio
import datetime as dt
import json
import logging
import os
import signal
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

import redis.asyncio as redis
import requests
import websockets

GAMMA_EVENTS_URL = os.getenv(
    "GAMMA_EVENTS_URL", "https://gamma-api.polymarket.com/events"
)
MARKET_WS_URL = os.getenv(
    "MARKET_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market"
)
LIVE_DATA_WS_URL = os.getenv(
    "LIVE_DATA_WS_URL", "wss://ws-live-data.polymarket.com"
)
DEFAULT_MARKET_TAG = os.getenv("DEFAULT_MARKET_TAG", "102467")

MARKET_DURATION_SECONDS = int(os.getenv("MARKET_DURATION_SECONDS", str(15 * 60)))
MARKET_START_LEAD_SECONDS = int(os.getenv("MARKET_START_LEAD_SECONDS", str(1 * 60)))

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
CHAINLINK_REDIS_CHANNEL = os.getenv(
    "CHAINLINK_REDIS_CHANNEL", "chainlink.crypto.prices"
)
BINANCE_REDIS_CHANNEL = os.getenv("BINANCE_REDIS_CHANNEL", "binance.crypto.prices")
MARKET_REDIS_CHANNEL_PREFIX = os.getenv(
    "MARKET_REDIS_CHANNEL_PREFIX", "markets.crypto"
)

CHAINLINK_SUBSCRIPTION = {
    "action": "subscribe",
    "subscriptions": [
        {"topic": "crypto_prices_chainlink", "type": "*", "filters": ""},
    ],
}
BINANCE_SUBSCRIPTION = {
    "action": "subscribe",
    "subscriptions": [
        {"topic": "crypto_prices", "type": "update", "filters": ""},
    ],
}

# ASSET_CONFIGS: Dict[str, Dict[str, str]] = {
#     "btc": {"keyword": "btc", "tag_id": DEFAULT_MARKET_TAG},
#     "eth": {"keyword": "eth", "tag_id": DEFAULT_MARKET_TAG},
#     "xrp": {"keyword": "xrp", "tag_id": DEFAULT_MARKET_TAG},
#     "sol": {"keyword": "sol", "tag_id": DEFAULT_MARKET_TAG},
# }

ASSET_CONFIGS: Dict[str, Dict[str, str]] = {
    "btc": {"keyword": "btc", "tag_id": DEFAULT_MARKET_TAG},
}

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("inflow_gateway")


@dataclass
class MarketDescriptor:
    slug: str
    asset_ids: List[str]
    timestamp: Optional[int]


class RedisPublisher:
    """Simple helper to fan websocket payloads into Redis topics."""

    def __init__(self, redis_url: str):
        self._client = redis.from_url(
            redis_url, encoding="utf-8", decode_responses=True
        )

    async def publish(self, channel: str, payload: object) -> None:
        message = payload if isinstance(payload, str) else json.dumps(payload)
        try:
            await self._client.publish(channel, message)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to publish to %s: %s", channel, exc)

    async def close(self) -> None:
        await self._client.close()
        await self._client.connection_pool.disconnect()


def _extract_timestamp(slug: str) -> Optional[int]:
    if not slug:
        return None

    parts = slug.split("-")
    if not parts:
        return None

    try:
        return int(parts[-1])
    except ValueError:
        return None


def _normalise_token_ids(value: object) -> Optional[List[str]]:
    if isinstance(value, list):
        return [str(item) for item in value]

    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return None
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
    return None


def fetch_events(tag_id: Optional[str] = None, closed: str = "false") -> List[dict]:
    params: Dict[str, object] = {"closed": closed, "limit": 500, "offset": 0}
    if tag_id:
        params["tag_id"] = tag_id

    events: List[dict] = []
    while True:
        response = requests.get(GAMMA_EVENTS_URL, params=params, timeout=15)
        response.raise_for_status()
        batch = response.json()
        if not batch:
            break
        events.extend(batch)
        params["offset"] += params["limit"]
    return events


def collect_markets_for_asset(
    events: List[dict], asset_keyword: str
) -> List[MarketDescriptor]:
    collected: List[MarketDescriptor] = []
    keyword = asset_keyword.lower()

    for event in events:
        fallback_slug = str(event.get("slug", ""))
        for market in event.get("markets", []):
            slug = str(market.get("slug") or fallback_slug)
            if keyword not in slug.lower():
                continue

            token_ids = _normalise_token_ids(market.get("clobTokenIds"))
            if not token_ids:
                continue

            timestamp = _extract_timestamp(slug)

            collected.append(
                MarketDescriptor(slug=slug, asset_ids=token_ids, timestamp=timestamp)
            )

    collected.sort(key=lambda item: item.timestamp or 0)
    return collected


def _ensure_meta(payload: object) -> Optional[dict]:
    if not isinstance(payload, dict):
        return None

    meta = payload.get("meta")
    if not isinstance(meta, dict):
        meta = {}
        payload["meta"] = meta
    return meta


def _classify_market_window(market: MarketDescriptor) -> str:
    if market.timestamp is None:
        return "active"

    now_ts = time.time()
    start_ts = market.timestamp - MARKET_START_LEAD_SECONDS
    end_ts = market.timestamp + MARKET_DURATION_SECONDS

    if now_ts < start_ts:
        return "future"
    if now_ts > end_ts:
        return "expired"
    return "active"


def _seconds_until_market_start(market: MarketDescriptor) -> float:
    if market.timestamp is None:
        return 0.0
    start_ts = market.timestamp - MARKET_START_LEAD_SECONDS
    return max(start_ts - time.time(), 0.0)


async def populate_market_queue(
    asset_name: str,
    config: Dict[str, str],
    queue: asyncio.Queue[MarketDescriptor],
    stop_event: asyncio.Event,
    refresh_interval: float = 60.0,
    min_queue_size: int = 3,
) -> None:
    seen_slugs: Set[str] = set()

    while not stop_event.is_set():
        if queue.qsize() >= min_queue_size:
            await asyncio.sleep(refresh_interval)
            continue

        try:
            events = await asyncio.to_thread(
                fetch_events, config.get("tag_id"), "false"
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to fetch events for %s: %s", asset_name, exc)
            await asyncio.sleep(refresh_interval)
            continue

        markets = collect_markets_for_asset(events, config["keyword"])
        new_markets = [market for market in markets if market.slug not in seen_slugs]

        if not new_markets:
            await asyncio.sleep(refresh_interval)
            continue

        for market in new_markets:
            await queue.put(market)
            seen_slugs.add(market.slug)


async def stream_single_market(
    asset_name: str,
    market: MarketDescriptor,
    stop_event: asyncio.Event,
    publisher: RedisPublisher,
    inactivity_timeout: float = 10.0,
    ping_interval: float = 10.0,
) -> None:
    redis_channel = f"{MARKET_REDIS_CHANNEL_PREFIX}:{asset_name}:{market.slug}"
    while not stop_event.is_set():
        try:
            async with websockets.connect(MARKET_WS_URL, ping_interval=None) as ws:
                await ws.send(json.dumps({"assets_ids": market.asset_ids, "type": "market"}))
                last_ping = dt.datetime.now()

                while not stop_event.is_set():
                    if (
                        dt.datetime.now() - last_ping
                    ).total_seconds() >= ping_interval:
                        await ws.send("PING")
                        last_ping = dt.datetime.now()

                    try:
                        message = await asyncio.wait_for(
                            ws.recv(), timeout=inactivity_timeout
                        )
                    except asyncio.TimeoutError:
                        return

                    if message == "PONG":
                        last_ping = dt.datetime.now()
                        continue

                    try:
                        payload = json.loads(message)
                    except json.JSONDecodeError:
                        logger.warning(
                            "Non-JSON payload for market %s: %s", market.slug, message
                        )
                        continue

                    payload_to_publish = payload
                    meta = _ensure_meta(payload)
                    if meta is None:
                        payload_to_publish = {
                            "raw_payload": payload,
                            "meta": {
                                "market_slug": market.slug,
                                "asset": asset_name,
                                "note": "wrapped_non_dict_payload",
                            },
                        }
                    else:
                        meta["market_slug"] = market.slug
                        meta["asset"] = asset_name
                    await publisher.publish(redis_channel, payload_to_publish)

                return
        except websockets.ConnectionClosed as exc:
            logger.warning(
                "Connection closed for market %s: %s; retrying in 2s",
                market.slug,
                exc,
            )
            await asyncio.sleep(2)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Error streaming market %s: %s; retrying in 5s", market.slug, exc
            )
            await asyncio.sleep(5)


async def stream_markets_for_asset(
    asset_name: str,
    queue: asyncio.Queue[MarketDescriptor],
    stop_event: asyncio.Event,
    publisher: RedisPublisher,
    inactivity_timeout: float = 10.0,
) -> None:
    active_tasks: Set[asyncio.Task] = set()

    async def _run_market(market: MarketDescriptor) -> None:
        try:
            delay = _seconds_until_market_start(market)
            if delay > 0:
                logger.info(
                    "Waiting %.2fs before starting %s market stream.",
                    delay,
                    market.slug,
                )
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=delay)
                    logger.info(
                        "Stop signal received before starting market %s", market.slug
                    )
                    return
                except asyncio.TimeoutError:
                    pass
                except asyncio.CancelledError:
                    raise

            if stop_event.is_set():
                return

            logger.info("Streaming %s market: %s", asset_name.upper(), market.slug)
            await stream_single_market(
                asset_name,
                market,
                stop_event,
                publisher,
                inactivity_timeout=inactivity_timeout,
            )
        finally:
            queue.task_done()

    while not stop_event.is_set():
        done_tasks = {task for task in active_tasks if task.done()}
        for task in done_tasks:
            active_tasks.discard(task)
            if task.cancelled():
                continue
            try:
                task.result()
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "Market stream task error (%s): %s",
                    task.get_name() if hasattr(task, "get_name") else "unknown",
                    exc,
                )

        try:
            market = await asyncio.wait_for(queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue

        status = _classify_market_window(market)
        if status == "expired":
            logger.info("Skipping expired market %s", market.slug)
            queue.task_done()
            continue

        task = asyncio.create_task(_run_market(market), name=f"{asset_name}:{market.slug}")
        active_tasks.add(task)

    for task in active_tasks:
        task.cancel()
    await asyncio.gather(*active_tasks, return_exceptions=True)


async def stream_live_feed(
    name: str,
    subscription_payload: dict,
    redis_channel: str,
    publisher: RedisPublisher,
    stop_event: asyncio.Event,
    ping_interval: float = 10.0,
) -> None:
    while not stop_event.is_set():
        try:
            async with websockets.connect(LIVE_DATA_WS_URL, ping_interval=None) as ws:
                await ws.send(json.dumps(subscription_payload))
                last_ping = dt.datetime.now()

                while not stop_event.is_set():
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=10)
                    except asyncio.TimeoutError:
                        continue

                    try:
                        payload = json.loads(message)
                    except json.JSONDecodeError:
                        logger.warning(
                            "Received non-JSON message on %s feed: %s", name, message
                        )
                        continue

                    payload_to_publish = payload
                    meta = _ensure_meta(payload)
                    if meta is None:
                        payload_to_publish = {
                            "raw_payload": payload,
                            "meta": {"source": name, "note": "wrapped_non_dict_payload"},
                        }
                    else:
                        meta["source"] = name
                    await publisher.publish(redis_channel, payload_to_publish)

                    if (
                        dt.datetime.now() - last_ping
                    ).total_seconds() >= ping_interval:
                        await ws.ping()
                        last_ping = dt.datetime.now()
        except websockets.ConnectionClosed as exc:
            if stop_event.is_set():
                return
            logger.warning("%s websocket closed: %s; reconnecting in 2s", name, exc)
            await asyncio.sleep(2)
        except Exception as exc:  # noqa: BLE001
            if stop_event.is_set():
                return
            logger.error("%s websocket error: %s; retrying in 5s", name, exc)
            await asyncio.sleep(5)


async def run_gateway() -> None:
    stop_event = asyncio.Event()
    publisher = RedisPublisher(REDIS_URL)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: stop_event.set())

    contract_queues: Dict[str, asyncio.Queue[MarketDescriptor]] = {
        asset: asyncio.Queue() for asset in ASSET_CONFIGS
    }

    tasks = [
        asyncio.create_task(
            stream_live_feed(
                "chainlink",
                CHAINLINK_SUBSCRIPTION,
                CHAINLINK_REDIS_CHANNEL,
                publisher,
                stop_event,
            ),
            name="chainlink_feed",
        ),
        asyncio.create_task(
            stream_live_feed(
                "binance",
                BINANCE_SUBSCRIPTION,
                BINANCE_REDIS_CHANNEL,
                publisher,
                stop_event,
            ),
            name="binance_feed",
        ),
    ]

    tasks.extend(
        asyncio.create_task(
            populate_market_queue(asset, config, contract_queues[asset], stop_event),
            name=f"{asset}_populate",
        )
        for asset, config in ASSET_CONFIGS.items()
    )

    tasks.extend(
        asyncio.create_task(
            stream_markets_for_asset(
                asset, contract_queues[asset], stop_event, publisher
            ),
            name=f"{asset}_stream",
        )
        for asset in ASSET_CONFIGS
    )

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Tasks cancelled, shutting down gateway...")
    finally:
        stop_event.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await publisher.close()


def main() -> None:
    asyncio.run(run_gateway())


if __name__ == "__main__":
    main()
