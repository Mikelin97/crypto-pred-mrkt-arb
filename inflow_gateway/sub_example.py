import argparse
import asyncio
import json
import os
import signal
import sys
import time
from typing import Any, Dict, Optional

import redis.asyncio as redis


def _now_ms() -> float:
    """Return current wall-clock time in milliseconds."""
    return time.time() * 1000


def _safe_timestamp(value: Any) -> Optional[float]:
    """Return a numeric timestamp (ms) when available."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


async def _subscribe_and_log(redis_url: str, channel: str) -> None:
    client = redis.from_url(redis_url, encoding="utf-8", decode_responses=True)
    pubsub = client.pubsub()
    await pubsub.subscribe(channel)
    print(f"Subscribed to {channel} via {redis_url}")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _stop_handler(*_: object) -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop_handler)
        except NotImplementedError:
            signal.signal(sig, _stop_handler)

    try:
        while not stop_event.is_set():
            try:
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
            except asyncio.CancelledError:
                break

            if message is None:
                continue

            if message.get("type") != "message":
                continue

            receive_ms = _now_ms()
            raw_payload = message.get("data")
            parse_ms = receive_ms
            parsed: Optional[Dict[str, Any]] = None

            if isinstance(raw_payload, str):
                try:
                    parsed = json.loads(raw_payload)
                except json.JSONDecodeError as exc:
                    print(f"[WARN] Failed to decode payload: {exc} ({raw_payload[:80]}...)")
                    continue
                parse_ms = _now_ms()
            else:
                print(f"[WARN] Unexpected payload type: {type(raw_payload).__name__}")
                continue

            outer_ts = _safe_timestamp(parsed.get("timestamp"))
            payload_obj = parsed.get("payload")
            inner_ts = (
                _safe_timestamp(payload_obj.get("timestamp"))
                if isinstance(payload_obj, dict)
                else None
            )

            delta_recv_outer = (
                receive_ms - outer_ts if outer_ts is not None else None
            )
            delta_outer_inner = (
                outer_ts - inner_ts
                if outer_ts is not None and inner_ts is not None
                else None
            )

            delta_recv_outer_str = (
                f"{delta_recv_outer:.3f}" if delta_recv_outer is not None else "n/a"
            )
            delta_outer_inner_str = (
                f"{delta_outer_inner:.3f}" if delta_outer_inner is not None else "n/a"
            )

            outer_ts_str = f"{outer_ts:.3f}" if outer_ts is not None else "n/a"
            inner_ts_str = f"{inner_ts:.3f}" if inner_ts is not None else "n/a"

            print(
                f"[{channel}] recv_ms={receive_ms:.3f} parse_ms={parse_ms:.3f} "
                f"outer_ts={outer_ts_str} inner_ts={inner_ts_str} "
                f"recv-outer_ms={delta_recv_outer_str} outer-inner_ms={delta_outer_inner_str}",
                flush=True,
            )
    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.close()
        await client.close()
        await client.connection_pool.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Redis subscriber performance probe for chainlink crypto prices."
    )
    parser.add_argument(
        "--redis-url",
        default=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        help="Redis connection URL (default: %(default)s)",
    )
    parser.add_argument(
        "--channel",
        default=os.getenv("REDIS_CHANNEL", "chainlink.crypto.prices"),
        help="Redis PUB/SUB channel to subscribe (default: %(default)s)",
    )
    args = parser.parse_args()

    try:
        asyncio.run(_subscribe_and_log(args.redis_url, args.channel))
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
