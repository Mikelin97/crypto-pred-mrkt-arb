"""Shared trading primitives that can be reused across services.

Heavy dependencies (e.g., boto3/pandas in Market) are lazily imported so modules
that only need lightweight helpers don’t require the full dependency set.
"""

__all__ = [
    "Market",
    "OrderBook",
    "Strategy",
    "Vol",
    "ExecutionClient",
    "ExecutionResult",
    "RedisSubscriber",
    "Fetcher",
]


def __getattr__(name):
    if name == "Market":
        from .market import Market

        return Market
    if name == "OrderBook":
        from .orderbook import OrderBook

        return OrderBook
    if name == "Strategy":
        from .strategy import Strategy

        return Strategy
    if name == "Vol":
        from .vol import Vol

        return Vol
    if name in {"ExecutionClient", "ExecutionResult"}:
        from .execution import ExecutionClient, ExecutionResult

        return ExecutionClient if name == "ExecutionClient" else ExecutionResult
    if name == "RedisSubscriber":
        from .subscriber import RedisSubscriber

        return RedisSubscriber
    if name == "Fetcher":
        from .fetcher import Fetcher

        return Fetcher
    raise AttributeError(f"module 'shared' has no attribute '{name}'")
