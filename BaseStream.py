import asyncio
import websockets
import json
import datetime as dt
import logging

class BaseStream:
    """Parent class for any websocket streaming data source."""
    def __init__(self, url: str, logger: logging.Logger = None):
        self.url = url
        self.websocket = None
        self.keep_running = True
        self.last_pong = dt.datetime.now()

        # Queues for external communication
        self.command_queue = asyncio.Queue()
        self.data_queue = asyncio.Queue()

        # Track active subscriptions
        self.subscriptions = set()

        # Logger: use child-provided or default
        if logger is None:
            self.logger = logging.getLogger(self.__class__.__name__)
            self.logger.setLevel(logging.INFO)
            ch = logging.StreamHandler()
            ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
            self.logger.addHandler(ch)
        else:
            self.logger = logger

    # ---------- Public API ----------
    async def subscribe(self, item):
        await self.command_queue.put(("subscribe", item))

    async def unsubscribe(self, item):
        await self.command_queue.put(("unsubscribe", item))

    async def next_message(self):
        return await self.data_queue.get()

    async def close(self):
        await self.command_queue.put(("close", None))

    # ---------- Core connection ----------
    async def connect(self):
        async with websockets.connect(self.url) as ws:
            self.websocket = ws
            self.logger.info(f"✅ Connected to {self.url}")

            if self.subscriptions:
                await self._send_subscribe()

            await asyncio.gather(
                self._receiver(),
                self._heartbeat(),
                self._command_listener()
            )

    # ---------- Internal tasks ----------
    async def _receiver(self):
        try:
            async for msg in self.websocket:
                if msg == "PONG":
                    self.last_pong = dt.datetime.now()
                    continue

                try:
                    data = json.loads(msg)
                except json.JSONDecodeError:
                    self.logger.warning(f"Non-JSON message: {msg}")
                    continue

                await self._process_message(data)

        except Exception as e:
            self.logger.error(f"Receiver error: {e}")

    async def _heartbeat(self, interval=5):
        while self.keep_running:
            if (dt.datetime.now() - self.last_pong).seconds > interval:
                try:
                    await self.websocket.send("PING")
                    self.logger.debug("Sent PING")
                except Exception as e:
                    self.logger.error(f"Heartbeat error: {e}")
            await asyncio.sleep(interval)

    async def _command_listener(self):
        while self.keep_running:
            cmd, item = await self.command_queue.get()

            if cmd == "subscribe":
                if item not in self.subscriptions:
                    self.subscriptions.add(item)
                    await self._send_subscribe(item)

            elif cmd == "unsubscribe":
                if item in self.subscriptions:
                    self.subscriptions.discard(item)
                    await self._send_unsubscribe(item)

            elif cmd == "close":
                self.keep_running = False
                if self.websocket:
                    await self.websocket.close()
                self.logger.info("WebSocket closed")
                break

    # ---------- Methods to implement in subclasses ----------
    async def _send_subscribe(self, item):
        """Send a subscription message for a specific item."""
        raise NotImplementedError

    async def _send_unsubscribe(self, item):
        """Send an unsubscribe message for a specific item."""
        raise NotImplementedError

    async def _process_message(self, message):
        """Process incoming websocket message and put it in data_queue."""
        await self.data_queue.put(message)
