import json
import logging
from BaseStream import BaseStream

class PolyMarketStream(BaseStream):
    def __init__(self, url='wss://ws-subscriptions-clob.polymarket.com/ws/market'):
        super().__init__(url)

        # Child-specific logger
        self.logger = logging.getLogger("PolyMarketStream")
        self.logger.setLevel(logging.INFO)

        # Console handler
        ch = logging.StreamHandler()
        ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        self.logger.addHandler(ch)

        # File handler
        fh = logging.FileHandler("polymarket.log", mode="a")
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        self.logger.addHandler(fh)

    async def _send_subscribe(self, asset_id=None):
        """Send subscription message for all active subscriptions."""
        if not self.subscriptions:
            return  # nothing to subscribe to
        msg = {"type": "market", "assets_ids": list(self.subscriptions)}
        try:
            await self.websocket.send(json.dumps(msg))
            self.logger.info(f"📡 Subscribed to: {self.subscriptions}")
        except Exception as e:
            self.logger.error(f"Failed to send subscribe message: {e}")

    async def _send_unsubscribe(self, asset_id=None):
        """Send unsubscribe message for all active subscriptions."""
        if not self.subscriptions:
            return
        msg = {"type": "unsubscribe", "assets_ids": list(self.subscriptions)}
        try:
            await self.websocket.send(json.dumps(msg))
            self.logger.info(f"❎ Unsubscribed from: {self.subscriptions}")
        except Exception as e:
            self.logger.error(f"Failed to send unsubscribe message: {e}")
