import datetime as dt
import asyncio
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set

import aiofiles
import boto3
import requests
import websockets
import logging

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
DEFAULT_MARKET_TAG = "102467"  # 15 minute recurring markets
DATA_OUTPUT_ROOT = Path("data")
CONTRACT_OUTPUT_ROOT = DATA_OUTPUT_ROOT / "contracts"
S3_BUCKET_NAME = os.getenv("S3_TARGET_BUCKET", "poly-punting-raw")


def _build_s3_client() -> boto3.client:
    """Initialise an S3 client honouring explicit env credentials when provided."""
    session_kwargs = {}

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    session_token = os.getenv("AWS_SESSION_TOKEN")
    region = os.getenv("AWS_REGION")

    if access_key and secret_key:
        session_kwargs["aws_access_key_id"] = access_key
        session_kwargs["aws_secret_access_key"] = secret_key
        if session_token:
            session_kwargs["aws_session_token"] = session_token

    if region:
        session_kwargs["region_name"] = region

    return boto3.client("s3", **session_kwargs)


s3_client = _build_s3_client()
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("poly_punting")

ASSET_CONFIGS: Dict[str, Dict[str, str]] = {
    "btc": {"keyword": "btc", "tag_id": DEFAULT_MARKET_TAG},
    "eth": {"keyword": "eth", "tag_id": DEFAULT_MARKET_TAG},
    "xrp": {"keyword": "xrp", "tag_id": DEFAULT_MARKET_TAG},
}


def _default_object_key(file_path: Path) -> str:
    """Build an S3 object key using the path relative to the data directory."""
    try:
        relative = file_path.relative_to(DATA_OUTPUT_ROOT)
        return str(relative)
    except ValueError:
        return file_path.name


def _upload_file_to_s3_sync(file_path: Path, object_key: Optional[str]) -> bool:
    if not S3_BUCKET_NAME:
        logger.warning("S3 bucket name is not configured; skipping upload.")
        return False

    if not file_path.exists():
        logger.warning("File not found, skipping S3 upload: %s", file_path)
        return False

    key = object_key or _default_object_key(file_path)
    try:
        s3_client.upload_file(str(file_path), S3_BUCKET_NAME, key)
        logger.info("Uploaded %s to s3://%s/%s", file_path, S3_BUCKET_NAME, key)
        return True
    except Exception as exc:
        logger.error("Error uploading %s to S3: %s", file_path, exc)
        return False


async def upload_file_to_s3(file_path: Path, object_key: Optional[str] = None) -> bool:
    """Async wrapper for uploading a single file to S3."""
    if not S3_BUCKET_NAME:
        return False
    return await asyncio.to_thread(_upload_file_to_s3_sync, file_path, object_key)


async def upload_price_stream_snapshots() -> None:
    """Upload the latest chainlink/binance price stream files to S3 if they exist."""
    if not S3_BUCKET_NAME:
        return

    today = dt.datetime.now(dt.timezone.utc).date()
    for prefix in ("chainlink_crypto_prices", "binance_crypto_prices"):
        file_path = DATA_OUTPUT_ROOT / f"{prefix}_{today}.jsonl"
        if file_path.exists():
            await upload_file_to_s3(file_path)


@dataclass
class MarketDescriptor:
    slug: str
    asset_ids: List[str]
    file_path: Path
    timestamp: Optional[int]


def _extract_timestamp(slug: str) -> Optional[int]:
    """Extract trailing numeric timestamp from a Polymarket slug."""
    if not slug:
        return None

    parts = slug.split("-")
    if not parts:
        return None

    tail = parts[-1]
    try:
        return int(tail)
    except ValueError:
        return None


def _normalise_token_ids(value: object) -> Optional[List[str]]:
    """Ensure token identifiers are returned as a list of strings."""
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
    """Pull events from the Gamma API, following pagination."""
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
    events: List[dict], asset_keyword: str, destination_dir: Path
) -> List[MarketDescriptor]:
    """Parse events into market descriptors for a specific asset keyword."""
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
            file_path = destination_dir / f"{slug}.jsonl"

            collected.append(
                MarketDescriptor(
                    slug=slug,
                    asset_ids=token_ids,
                    file_path=file_path,
                    timestamp=timestamp,
                )
            )

    collected.sort(key=lambda item: item.timestamp or 0)
    return collected


async def populate_market_queue(
    asset_name: str,
    config: Dict[str, str],
    queue: asyncio.Queue,
    stop_event: asyncio.Event,
    refresh_interval: float = 60.0,
    min_queue_size: int = 3,
) -> None:
    """Keep the per-asset market queue topped up with fresh slugs."""
    seen_slugs: Set[str] = set()
    destination = CONTRACT_OUTPUT_ROOT / asset_name
    destination.mkdir(parents=True, exist_ok=True)

    while not stop_event.is_set():
        if queue.qsize() >= min_queue_size:
            await asyncio.sleep(refresh_interval)
            continue

        try:
            events = await asyncio.to_thread(
                fetch_events, config.get("tag_id"), "false"
            )
        except Exception as exc:
            logger.error("Failed to fetch events for %s: %s", asset_name, exc)
            await asyncio.sleep(refresh_interval)
            continue

        markets = collect_markets_for_asset(events, config["keyword"], destination)
        new_markets = [market for market in markets if market.slug not in seen_slugs]

        if not new_markets:
            await asyncio.sleep(refresh_interval)
            continue

        for market in new_markets:
            await queue.put(market)
            seen_slugs.add(market.slug)


async def stream_single_market(
    market: MarketDescriptor,
    stop_event: asyncio.Event,
    inactivity_timeout: float = 10.0,
    ping_interval: float = 10.0,
) -> None:
    """Subscribe to a specific market and persist payloads until inactivity timeout."""
    while not stop_event.is_set():
        try:
            async with websockets.connect(MARKET_WS_URL, ping_interval=None) as websocket:
                await websocket.send(
                    json.dumps({"assets_ids": market.asset_ids, "type": "market"})
                )

                market.file_path.parent.mkdir(parents=True, exist_ok=True)
                async with aiofiles.open(market.file_path, "w") as handle:
                    last_ping = dt.datetime.now()
                    last_flush = dt.datetime.now()

                    while not stop_event.is_set():
                        if (dt.datetime.now() - last_ping).total_seconds() >= ping_interval:
                            await websocket.send("PING")
                            last_ping = dt.datetime.now()

                        try:
                            message = await asyncio.wait_for(
                                websocket.recv(), timeout=inactivity_timeout
                            )
                        except asyncio.TimeoutError:
                            await handle.flush()
                            return

                        if message == "PONG":
                            last_ping = dt.datetime.now()
                            continue

                        try:
                            payload = json.loads(message)
                        except json.JSONDecodeError:
                            logger.warning("Non-JSON payload for market %s: %s", market.slug, message)
                            continue

                        await handle.write(json.dumps(payload) + "\n")

                        if (dt.datetime.now() - last_flush).total_seconds() >= 5:
                            await handle.flush()
                            last_flush = dt.datetime.now()

                return
        except websockets.ConnectionClosed as exc:
            logger.warning("Connection closed for market %s: %s; retrying in 2s", market.slug, exc)
            await asyncio.sleep(2)
        except Exception as exc:
            logger.error("Error streaming market %s: %s; retrying in 5s", market.slug, exc)
            await asyncio.sleep(5)


async def stream_markets_for_asset(
    asset_name: str,
    queue: asyncio.Queue,
    stop_event: asyncio.Event,
    inactivity_timeout: float = 10.0,
) -> None:
    """Continuously pull market descriptors from the queue and stream them."""
    while not stop_event.is_set():
        try:
            market: MarketDescriptor = await asyncio.wait_for(queue.get(), timeout=5.0)
        except asyncio.TimeoutError:
            continue

        logger.info("Streaming %s market: %s", asset_name.upper(), market.slug)
        try:
            await stream_single_market(
                market, stop_event, inactivity_timeout=inactivity_timeout
            )
            if not stop_event.is_set():
                await upload_file_to_s3(market.file_path)
                await upload_price_stream_snapshots()
        finally:
            queue.task_done()


# ATTEMPTING TO SUBSCRIBE TO CHAINLINK DATA
async def stream_chainlink_data(queue: asyncio.Queue, stop_event: asyncio.Event):
    url = "wss://ws-live-data.polymarket.com"
    last_time_ping = dt.datetime.now()
    subscribe_message = {
        "action": "subscribe",
        "subscriptions": [
            {
            "topic": "crypto_prices_chainlink",
            "type": "*",
            "filters": "" # "{\"symbol\":\"eth/usd\"}" doesn't seem capable of handling multiple specific ones... just all or nothing
            }
        ]
        }
    while not stop_event.is_set():
        try:
            async with websockets.connect(url) as websocket:
                await websocket.send(json.dumps(subscribe_message))

                while not stop_event.is_set():
                    try:
                        m = await asyncio.wait_for(websocket.recv(), timeout=10)
                    except asyncio.TimeoutError:
                        continue

                    try:
                        d = json.loads(m)
                        await queue.put(d)
                    except json.JSONDecodeError:
                        logger.warning("Received non-JSON message on chainlink feed: %s", m)
                        continue

                    if last_time_ping + dt.timedelta(seconds=10) < dt.datetime.now():
                        await websocket.ping()
                        last_time_ping = dt.datetime.now()
                        logger.debug("PINGING chainlink websocket")
        except websockets.ConnectionClosed as exc:
            if stop_event.is_set():
                return
            logger.warning("Chainlink websocket closed: %s; reconnecting in 2s", exc)
            await asyncio.sleep(2)
        except Exception as exc:
            if stop_event.is_set():
                return
            logger.error("Chainlink websocket error: %s; retrying in 5s", exc)
            await asyncio.sleep(5)


async def file_writer(queue: asyncio.Queue, stop_event: asyncio.Event, base_name: str):
    current_date = dt.datetime.now(dt.timezone.utc).date()

    while not stop_event.is_set():
        filename = DATA_OUTPUT_ROOT / f"{base_name}_{current_date}.jsonl"
        filename.parent.mkdir(parents=True, exist_ok=True)

        async with aiofiles.open(filename, "a") as f:

            last_flush = dt.datetime.now()
            while not stop_event.is_set():
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=5)
                except asyncio.TimeoutError:
                    continue
                
                # rotate file if the day changed
                if dt.datetime.now(dt.timezone.utc).date() != current_date:
                    await f.flush()
                    break  # exit loop → reopen file

                await f.write(json.dumps(msg) + "\n")

                # flush every 5 seconds
                if (dt.datetime.now() - last_flush).total_seconds() > 5:
                    await f.flush()
                    last_flush = dt.datetime.now()

async def stream_binance_data(queue: asyncio.Queue, stop_event: asyncio.Event):
    url = "wss://ws-live-data.polymarket.com"
    last_time_ping = dt.datetime.now()
    subscribe_message = {
        "action": "subscribe", 
        "subscriptions": [
            {
                "topic": "crypto_prices",
                "type": "update",
                # "filters": "solusdt,btcusdt,ethusdt"
                "filters": ""
            }
        ]
    }
    while not stop_event.is_set():
        try:
            async with websockets.connect(url) as websocket:
                await websocket.send(json.dumps(subscribe_message))

                while not stop_event.is_set():
                    try:
                        m = await asyncio.wait_for(websocket.recv(), timeout=10)
                    except asyncio.TimeoutError:
                        continue

                    try:
                        d = json.loads(m)
                        await queue.put(d)
                    except json.JSONDecodeError:
                        logger.warning("Received non-JSON message on binance feed: %s", m)
                        continue

                    if last_time_ping + dt.timedelta(seconds=10) < dt.datetime.now():
                        await websocket.ping()
                        last_time_ping = dt.datetime.now()
                        logger.debug("PINGING binance websocket")
        except websockets.ConnectionClosed as exc:
            if stop_event.is_set():
                return
            logger.warning("Binance websocket closed: %s; reconnecting in 2s", exc)
            await asyncio.sleep(2)
        except Exception as exc:
            if stop_event.is_set():
                return
            logger.error("Binance websocket error: %s; retrying in 5s", exc)
            await asyncio.sleep(5)

async def main():
    stop_event = asyncio.Event()
    DATA_OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    CONTRACT_OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    
    # Queues for streaming data
    chainlink_queue = asyncio.Queue()
    binance_queue = asyncio.Queue()
    contract_queues: Dict[str, asyncio.Queue] = {
        asset: asyncio.Queue() for asset in ASSET_CONFIGS
    }
    
    # Start streaming tasks
    get_chainlink_task = asyncio.create_task(stream_chainlink_data(chainlink_queue, stop_event))
    get_binance_task = asyncio.create_task(stream_binance_data(binance_queue, stop_event))
    populate_contract_tasks = [
        asyncio.create_task(
            populate_market_queue(asset, config, contract_queues[asset], stop_event)
        )
        for asset, config in ASSET_CONFIGS.items()
    ]
    contract_stream_tasks = [
        asyncio.create_task(
            stream_markets_for_asset(asset, contract_queues[asset], stop_event)
        )
        for asset in ASSET_CONFIGS
    ]
    
    # Start file writer tasks
    chainlink_store_task = asyncio.create_task(
        file_writer(chainlink_queue, stop_event, base_name="chainlink_crypto_prices")
    )
    binance_store_task = asyncio.create_task(
        file_writer(binance_queue, stop_event, base_name="binance_crypto_prices")
    )
    
    all_tasks = (
        [get_chainlink_task, get_binance_task, chainlink_store_task, binance_store_task]
        + populate_contract_tasks
        + contract_stream_tasks
    )

    try:
        # Wait for all tasks to complete (they probably run forever)
        await asyncio.gather(*all_tasks)
    except asyncio.CancelledError:
        logger.info("Tasks cancelled, shutting down...")
    finally:
        # Set stop_event to signal tasks to exit
        stop_event.set()
        for task in all_tasks:
            task.cancel()
        await asyncio.gather(*all_tasks, return_exceptions=True)



if __name__ == "__main__":

    asyncio.run(main())
