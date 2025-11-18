import argparse
import asyncio
import json
import math
import signal
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, TYPE_CHECKING

import dotenv
from py_clob_client.clob_types import OrderType

FILE_DIR = Path(__file__).resolve().parent
REPO_ROOT = FILE_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from shared.subscriber import RedisSubscriber
from shared.strategies.data_track import DataTrackStrategy

if TYPE_CHECKING:
    from shared.execution import ExecutionClient
dotenv.load_dotenv()

ORDER_TYPE_CHOICES = ("GTC", "FOK", "GTD", "FAK")


def _minutes_from_token(token: str) -> int:
    if token.endswith("m") and token[:-1].isdigit():
        return int(token[:-1])
    if token.endswith("h") and token[:-1].isdigit():
        return int(token[:-1]) * 60
    return 15


def _precision_multiplier(time_precision: int) -> int:
    return 10 ** max(time_precision - 10, 0)


def _parse_slug(slug: str, time_precision: int) -> Dict[str, int]:
    normalized = slug.removesuffix(".jsonl")
    parts = normalized.split("-")
    if len(parts) < 2:
        raise ValueError(f"Cannot parse slug '{slug}'.")
    epoch_seconds = int(parts[-1])
    window_token = next(
        (token for token in reversed(parts[:-1]) if token[-1] in {"m", "h"}),
        "15m",
    )
    window_minutes = _minutes_from_token(window_token)
    multiplier = _precision_multiplier(time_precision)
    unix_start = epoch_seconds * multiplier
    unix_end = unix_start + window_minutes * 60 * multiplier
    return {
        "start_ms": unix_start,
        "end_ms": unix_end,
        "window_minutes": window_minutes,
        "multiplier": multiplier,
    }


def _to_float(value: object) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class LiveStrategyEngine:
    def __init__(
        self,
        *,
        strategy: DataTrackStrategy,
        execution_client: Optional["ExecutionClient"],
        market_channel: str,
        chainlink_symbol: str,
        order_type: OrderType,
        order_size: float,
        threshold: float,
        cooldown_seconds: float,
        warmup_messages: int,
    ) -> None:
        self.strategy = strategy
        self.exec_client = execution_client
        self.market_channel = market_channel
        self.chainlink_symbol = chainlink_symbol.lower()
        self.order_type = order_type
        self.order_size = order_size
        self.threshold = threshold
        self.cooldown_seconds = cooldown_seconds
        self._warmup_remaining = warmup_messages
        self._last_crypto_price = math.nan
        self._last_order_time = 0.0
        self.positions: Dict[str, float] = {}
        self._loop = asyncio.get_running_loop()

    async def handle_message(self, channel: str, payload: object) -> None:
        if channel == self.market_channel:
            await self._handle_market_payload(payload)
        else:
            self._handle_chainlink_payload(payload)

    def _extract_payload(self, payload: object) -> Optional[Dict]:
        if isinstance(payload, dict):
            base = payload.get("raw_payload", payload)
            if isinstance(base, str):
                try:
                    return json.loads(base)
                except json.JSONDecodeError:
                    return None
            if isinstance(base, dict):
                return base
        elif isinstance(payload, str):
            try:
                return json.loads(payload)
            except json.JSONDecodeError:
                return None
        return None

    def _handle_chainlink_payload(self, payload: object) -> None:
        data = self._extract_payload(payload)
        if not data:
            return
        body = data.get("payload") if isinstance(data.get("payload"), dict) else data
        symbol = str(body.get("symbol", "")).lower()
        if symbol != self.chainlink_symbol:
            return
        price = _to_float(body.get("value"))
        if price is None:
            return
        self._last_crypto_price = price

    async def _handle_market_payload(self, payload: object) -> None:
        data = self._extract_payload(payload)
        if not data:
            return
        event_type = data.get("event_type")
        timestamp = int(data.get("timestamp", 0))

        if event_type == "last_trade_price":
            self.strategy.on_trade(data)
            return

        updates_by_asset = self._to_orderbook_updates(data)
        if not updates_by_asset:
            return

        crypto_price = self._last_crypto_price
        if math.isnan(crypto_price):
            return

        flat_updates: List[Dict] = []
        for asset_id, asset_updates in updates_by_asset.items():
            for update in asset_updates:
                update["asset_id"] = asset_id
                flat_updates.append(update)

        if self._warmup_remaining > 0:
            self.strategy.on_warmup(timestamp, crypto_price, flat_updates)
            self._warmup_remaining -= 1
        else:
            self.strategy.on_update(timestamp, crypto_price, flat_updates)
            state = self.strategy.get_state()
            theo = state.get("theo_price")
            print(
                f"[PRICING] ts={timestamp} asset_price={crypto_price:.4f} theo={theo if theo is not None else 'nan'}"
            )
            await self._maybe_place_order()

    def _to_orderbook_updates(self, data: Dict) -> Dict[str, List[Dict]]:
        updates: Dict[str, List[Dict]] = {}
        timestamp = int(data.get("timestamp", 0))
        event_type = data.get("event_type")
        if event_type == "book":
            asset_id = data.get("asset_id")
            if not asset_id:
                return updates
            updates.setdefault(asset_id, [])
            for bid in data.get("bids", []):
                price = _to_float(bid.get("price"))
                size = _to_float(bid.get("size"))
                if price is None or size is None:
                    continue
                updates[asset_id].append(
                    {"side": "bid", "price": price, "size": size, "timestamp": timestamp}
                )
            for ask in data.get("asks", []):
                price = _to_float(ask.get("price"))
                size = _to_float(ask.get("size"))
                if price is None or size is None:
                    continue
                updates[asset_id].append(
                    {"side": "ask", "price": price, "size": size, "timestamp": timestamp}
                )
        elif event_type == "price_change":
            for change in data.get("price_changes", []):
                asset_id = change.get("asset_id")
                if not asset_id:
                    continue
                updates.setdefault(asset_id, [])
                price = _to_float(change.get("price"))
                size = _to_float(change.get("size"))
                if price is None or size is None:
                    continue
                side = change.get("side", "").lower()
                ob_side = "bid" if side == "buy" else "ask"
                updates[asset_id].append(
                    {"side": ob_side, "price": price, "size": size, "timestamp": timestamp}
                )
        return updates

    async def _maybe_place_order(self) -> None:
        if time.time() - self._last_order_time < self.cooldown_seconds:
            return

        state = self.strategy.get_state()
        theo = state.get("theo_price")
        if theo is None or (isinstance(theo, float) and math.isnan(theo)):
            return

        books: Dict[str, Dict[str, float]] = state.get("books", {})
        trades: List[Dict[str, object]] = []

        for asset_id, stats in books.items():
            best_bid = stats.get("best_bid")
            best_ask = stats.get("best_ask")
            mid = stats.get("mid_price")
            if any(
                value is None or (isinstance(value, float) and math.isnan(value))
                for value in (best_bid, best_ask, mid)
            ):
                continue

            fair_value = theo if mid >= 0.5 else 1 - theo

            if fair_value - best_ask >= self.threshold:
                trades.append(
                    {
                        "asset_id": asset_id,
                        "side": "buy",
                        "price": best_ask,
                        "fair": fair_value,
                    }
                )
            elif best_bid - fair_value >= self.threshold:
                holdings = self.positions.get(asset_id, 0.0)
                if holdings <= 0:
                    continue
                trades.append(
                    {
                        "asset_id": asset_id,
                        "side": "sell",
                        "price": best_bid,
                        "fair": fair_value,
                    }
                )

        if not trades:
            return

        for trade in trades:
            asset_id = trade["asset_id"]
            side = trade["side"]
            price = trade["price"]
            fair = trade["fair"]

            if side == "buy":
                self.positions[asset_id] = self.positions.get(asset_id, 0.0) + self.order_size
            else:
                holdings = self.positions.get(asset_id, 0.0)
                if holdings <= 0:
                    continue
                sell_size = min(self.order_size, holdings)
                self.positions[asset_id] = holdings - sell_size

            print(f"[TRADE] {side.upper()} {self.order_size} {asset_id} @ {price:.4f} ")

        self._last_order_time = time.time()


async def main_async(args: argparse.Namespace) -> None:
    slug_meta = _parse_slug(args.market_slug, args.time_precision)
    strategy = DataTrackStrategy(
        slug_meta["start_ms"],
        slug_meta["end_ms"],
        args.time_precision,
        effective_memory=args.effective_memory,
        risk_free_rate=args.risk_free_rate,
    )
    if args.strike_price is not None:
        strategy.strike_price = args.strike_price
    execution_client = None
    chainlink_channel = args.chainlink_channel
    if not chainlink_channel:
        chainlink_channel = "chainlink.crypto.prices"

    engine = LiveStrategyEngine(
        strategy=strategy,
        execution_client=execution_client,
        market_channel=f"markets.crypto:{args.asset}:{args.market_slug}",
        chainlink_symbol=args.chainlink_symbol.lower(),
        order_type=getattr(OrderType, args.order_type),
        order_size=args.order_size,
        threshold=args.threshold,
        cooldown_seconds=args.order_cooldown,
        warmup_messages=args.warmup_messages,
    )

    subscriber = RedisSubscriber(args.redis_url)
    stop_event = asyncio.Event()

    def _stop(*_: object) -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            signal.signal(sig, _stop)

    try:
        await subscriber.run(
            [
                f"markets.crypto:{args.asset}:{args.market_slug}",
                chainlink_channel,
            ],
            engine.handle_message,
            poll_interval=0.5,
            stop_event=stop_event,
        )
    finally:
        await subscriber.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Live trading loop using Redis streams.")
    parser.add_argument("--redis-url", default="redis://localhost:6379/0")
    parser.add_argument("--asset", required=True, help="Asset name used in Redis channel (e.g. btc).")
    parser.add_argument("--market-slug", required=True, help="Market slug such as btc-updown-15m-<ts>.")
    parser.add_argument("--chainlink-symbol", help="Chainlink symbol to track (default <asset>/usd).")
    parser.add_argument("--chainlink-channel", help="Override chainlink Redis channel name.")
    parser.add_argument("--order-size", type=float, default=1.0)
    parser.add_argument("--order-type", choices=ORDER_TYPE_CHOICES, default="GTC")
    parser.add_argument("--threshold", type=float, default=0.02, help="Min theo edge vs quote to trigger order.")
    parser.add_argument("--strike-price", type=float, help="Override strike price for strategy warmup/testing.")
    parser.add_argument("--order-cooldown", type=float, default=30.0, help="Seconds to wait between orders.")
    parser.add_argument("--warmup-messages", type=int, default=5)
    parser.add_argument("--time-precision", type=int, default=13)
    parser.add_argument("--effective-memory", type=int, default=300)
    parser.add_argument("--risk-free-rate", type=float, default=0.042)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if not args.chainlink_symbol:
        args.chainlink_symbol = f"{args.asset}/usd"
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
