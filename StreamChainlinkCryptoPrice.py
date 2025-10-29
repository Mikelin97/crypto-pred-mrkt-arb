import datetime as dt
import websockets
import json
import asyncio
import aiofiles
from pathlib import Path



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
    async with websockets.connect(url) as websocket:
        await websocket.send(json.dumps(subscribe_message))

        # while True:
        while not stop_event.is_set():
            try:
                m = await asyncio.wait_for(websocket.recv(), timeout=10)
            except asyncio.TimeoutError:
                continue
  
            try:
                d = json.loads(m)
                await queue.put(d)
            except json.JSONDecodeError:
                print("Received non-JSON message:", m)
                continue

            if last_time_ping + dt.timedelta(seconds=10) < dt.datetime.now():
                await websocket.ping()
                last_time_ping = dt.datetime.now()
                print("PINGING")


async def file_writer(queue: asyncio.QueueEmpty, stop_event: asyncio.Event, base_name: str):
    current_date = dt.datetime.now(dt.timezone.utc).date()

    while not stop_event.is_set():
        filename = Path(f"{base_name}_{current_date}.jsonl")
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

if __name__ == "__main__":
    
    stop_event = asyncio.Event()
    queue = asyncio.Queue()
    get_task = asyncio.create_task(stream_chainlink_data(queue, stop_event))
    store_task = asyncio.create_task(file_writer(queue, stop_event, base_name="chainlink_crypto_prices"))