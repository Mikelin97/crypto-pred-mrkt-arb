"""Async Redis PUB/SUB helper."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from typing import AsyncIterator, Callable, Iterable, Tuple

import redis.asyncio as redis


class RedisSubscriber:
    """Utility to subscribe to redis channels and fan messages to handlers."""

    def __init__(self, redis_url: str = "redis://localhost:6379/0", *, decode_json: bool = True):
        self.redis_url = redis_url
        self.decode_json = decode_json
        self._client = redis.from_url(redis_url, encoding="utf-8", decode_responses=True)

    async def close(self) -> None:
        await self._client.close()
        await self._client.connection_pool.disconnect()

    async def _decode(self, raw: str):
        if not self.decode_json:
            return raw
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw

    @asynccontextmanager
    async def subscription(self, channels: Iterable[str], patterns: Iterable[str] | None = None):
        pubsub = self._client.pubsub()
        channels = tuple(channels)
        patterns = tuple(patterns or ())
        if channels:
            await pubsub.subscribe(*channels)
        if patterns:
            await pubsub.psubscribe(*patterns)
        try:
            yield pubsub
        finally:
            if channels:
                await pubsub.unsubscribe(*channels)
            if patterns:
                await pubsub.punsubscribe(*patterns)
            await pubsub.close()

    async def iter_messages(
        self,
        channels: Iterable[str],
        *,
        patterns: Iterable[str] | None = None,
        poll_interval: float = 0.5,
    ) -> AsyncIterator[Tuple[str, object]]:
        async with self.subscription(channels, patterns) as pubsub:
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=poll_interval)
                if message is None:
                    await asyncio.sleep(0)
                    continue
                if message.get("type") not in {"message", "pmessage"}:
                    continue
                channel = message.get("channel")
                yield channel, await self._decode(message["data"])

    async def run(
        self,
        channels: Iterable[str],
        handler: Callable[[str, object], asyncio.Future | None],
        *,
        patterns: Iterable[str] | None = None,
        poll_interval: float = 0.5,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        stop_event = stop_event or asyncio.Event()
        async with self.subscription(channels, patterns) as pubsub:
            while not stop_event.is_set():
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=poll_interval)
                if message is None:
                    continue
                if message.get("type") not in {"message", "pmessage"}:
                    continue
                payload = await self._decode(message["data"])
                maybe = handler(message["channel"], payload)
                if asyncio.iscoroutine(maybe):
                    await maybe
