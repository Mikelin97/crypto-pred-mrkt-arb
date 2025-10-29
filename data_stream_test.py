import websockets
import requests
import json
import datetime as dt
import asyncio
from pathlib import Path


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

async def subscribe(asset_ids, file_path: str = 'data_stream.json', inactivity_timeout: float = 10.0):

    url = 'wss://ws-subscriptions-clob.polymarket.com/ws/market'
    last_time_pong = dt.datetime.now()
    msgs = []

    async with websockets.connect(url, ping_interval=None) as websocket:
        await websocket.send(json.dumps({"assets_ids": asset_ids, "type": "market"}))

        try:
            while True:
                if last_time_pong + dt.timedelta(seconds=10) < dt.datetime.now():
                    await websocket.send("PING")
                    last_time_pong = dt.datetime.now()

                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=inactivity_timeout)
                except asyncio.TimeoutError:
                    print(f"No messages received in {inactivity_timeout} seconds; closing subscription.")
                    break
                except websockets.ConnectionClosed as exc:
                    print(f"Websocket connection closed: {exc}")
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

                msgs.append(payload)
                print(len(msgs))

                if len(msgs) >= 1000:
                    _flush_messages(msgs, file_path=file_path)
        except asyncio.CancelledError:
            print("Subscription cancelled; flushing buffered messages before exit.")
            raise
        finally:
            _flush_messages(msgs, file_path=file_path)
    
    
    





if __name__ == "__main__":
    
    tag_15m_markets = tag_id='102467'
    binary_15m_events = get_all_events(tag_id=tag_15m_markets)

    asset1, asset2 = json.loads(binary_15m_events[1]['markets'][0]['clobTokenIds']) 

    print(f"Asset 1 Token ID: {asset1}")
    print(f"Asset 2 Token ID: {asset2}")


    try:
        asyncio.run(subscribe([asset1]))
    except KeyboardInterrupt:
        print("Keyboard interrupt received; subscription stopped.")



    
    
