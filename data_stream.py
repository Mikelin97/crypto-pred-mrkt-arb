import websockets
import requests
import json
import datetime as dt
import asyncio
from collections import deque
from pathlib import Path
from typing import Deque, Dict, List, Optional


def _flush_messages(buffer, file_path: str = 'data_stream.json') -> None:
    """Persist buffered payloads by extending a JSON array on disk."""
    if not buffer:
        return

    destination = Path(file_path)
    existing_payloads = []

    if destination.exists():
        try:
            with destination.open('r', encoding='utf-8') as source:
                contents = source.read().strip()
                if contents:
                    existing_payloads = json.loads(contents)
        except json.JSONDecodeError:
            print(f"Existing data in {file_path} is not valid JSON; overwriting.")

    existing_payloads.extend(buffer)

    with destination.open('w', encoding='utf-8') as dest:
        json.dump(existing_payloads, dest)

    buffer.clear()


def _extract_timestamp(slug: str) -> Optional[int]:
    """Pull the trailing numeric timestamp from a slug like 'btc-updown-15m-1761709500'."""
    if not slug:
        return None

    *_, tail = slug.split('-') if '-' in slug else (None,)
    try:
        return int(tail)
    except (TypeError, ValueError):
        return None


def _collect_btc_markets(events: List[dict], output_dir: Path) -> Deque[Dict[str, object]]:
    """Return BTC market descriptors sorted by timestamp ascending."""
    collected: List[Dict[str, object]] = []

    for event in events:
        event_slug = event.get('slug', '')
        for market in event.get('markets', []):
            slug = market.get('slug') or event_slug
            if 'btc' not in slug.lower():
                continue

            timestamp = _extract_timestamp(slug)
            if timestamp is None:
                continue

            token_ids = market.get('clobTokenIds')
            asset_ids = token_ids
            if isinstance(token_ids, str):
                try:
                    asset_ids = json.loads(token_ids)
                except json.JSONDecodeError:
                    continue

            if not isinstance(asset_ids, list) or not asset_ids:
                continue

            asset_ids = [str(asset) for asset in asset_ids]

            file_path = output_dir / f"{slug}.json"
            collected.append({
                'timestamp': timestamp,
                'slug': slug,
                'asset_ids': asset_ids,
                'file_path': file_path,
            })

    collected.sort(key=lambda item: item['timestamp'])
    return deque(collected)

def get_all_events(closed="false", tag_id = ''):
    params = {
        "closed": closed,
        "limit" : 500,
        "offset" : 0,
        # 'tag_id': ''
    }
    if tag_id:
        params['tag_id'] = tag_id

    events = []
    r = requests.get(url="https://gamma-api.polymarket.com/events", params=params)
    response = r.json()
    while response:
        events += response
        params['offset'] += 500
        r = requests.get(url="https://gamma-api.polymarket.com/events", params=params)
        response = r.json()
    
    return events

async def _stream_single_market(
    websocket: websockets.WebSocketClientProtocol,
    asset_ids: List[str],
    file_path: Path,
    inactivity_timeout: float,
) -> None:
    """Subscribe to a single market over an existing websocket connection."""
    await websocket.send(json.dumps({"assets_ids": asset_ids, "type": "market"}))

    last_time_pong = dt.datetime.now()
    buffer: List[dict] = []

    try:
        while True:
            if last_time_pong + dt.timedelta(seconds=10) < dt.datetime.now():
                await websocket.send("PING")
                last_time_pong = dt.datetime.now()

            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=inactivity_timeout)
            except asyncio.TimeoutError:
                print(f"No messages received in {inactivity_timeout} seconds; moving to next market.")
                break

            if message == "PONG":
                last_time_pong = dt.datetime.now()
                continue

            last_time_pong = dt.datetime.now()

            try:
                payload = json.loads(message)
            except json.JSONDecodeError:
                print("Received non-JSON message:", message)
                continue

            buffer.append(payload)

            if len(buffer) >= 1000:
                _flush_messages(buffer, file_path=str(file_path))
    except asyncio.CancelledError:
        print("Market stream cancelled; flushing buffered messages before exit.")
        raise
    finally:
        _flush_messages(buffer, file_path=str(file_path))


async def stream_btc_markets(
    tag_id: str,
    output_dir: Path,
    inactivity_timeout: float = 10.0,
    poll_delay: float = 60.0,
) -> None:
    """Continuously stream BTC markets under a given tag."""
    output_dir.mkdir(parents=True, exist_ok=True)

    url = 'wss://ws-subscriptions-clob.polymarket.com/ws/market'
    market_stack: Deque[Dict[str, object]] = deque()

    while True:
        if not market_stack:
            events = get_all_events(tag_id=tag_id)
            market_stack = _collect_btc_markets(events, output_dir)
            if not market_stack:
                print("No BTC markets found; retrying after delay.")
                await asyncio.sleep(poll_delay)
                continue

        try:
            async with websockets.connect(url, ping_interval=None) as websocket:
                print("Connected to websocket.")
                while True:
                    if not market_stack:
                        events = get_all_events(tag_id=tag_id)
                        market_stack = _collect_btc_markets(events, output_dir)
                        if not market_stack:
                            print("No BTC markets found; retrying after delay.")
                            await asyncio.sleep(poll_delay)
                            continue

                    market = market_stack.pop()
                    slug = market['slug']
                    asset_ids = market['asset_ids']
                    file_path = market['file_path']

                    print(f"Streaming market {slug} -> {file_path}")

                    try:
                        await _stream_single_market(
                            websocket,
                            asset_ids,
                            file_path,
                            inactivity_timeout,
                        )
                    except websockets.ConnectionClosed as exc:
                        print(f"Websocket closed while streaming {slug}: {exc}")
                        market_stack.append(market)
                        raise
        except websockets.ConnectionClosed as exc:
            print(f"Websocket connection closed; reconnecting in 5s: {exc}")
            await asyncio.sleep(5)
        except Exception as exc:
            print(f"Unexpected error: {exc}; retrying in 5s")
            await asyncio.sleep(5)
    
    
    





if __name__ == "__main__":
    DEFAULT_TAG_ID = '102467'
    OUTPUT_DIR = Path('data')

    try:
        asyncio.run(stream_btc_markets(DEFAULT_TAG_ID, OUTPUT_DIR))
    except KeyboardInterrupt:
        print("Keyboard interrupt received; streamer stopped.")



    
    
