"""Lightweight execution wrapper for submitting orders to Polymarket."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Iterable, Optional

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, RequestArgs
from py_clob_client.endpoints import POST_ORDER
from py_clob_client.headers.headers import create_level_2_headers
from py_clob_client.http_helpers.helpers import post
from py_clob_client.order_builder.constants import BUY, SELL
from py_clob_client.utilities import order_to_json


@dataclass
class ExecutionResult:
    response: object
    timings_ms: list[tuple[str, float]]


class ExecutionClient:
    """Convenience wrapper around `ClobClient` with timing + env helpers."""

    def __init__(
        self,
        *,
        host: str,
        private_key: str,
        chain_id: int = 137,
        signature_type: int = 2,
        funder: Optional[str] = None,
    ) -> None:
        self.client = ClobClient(
            host,
            key=private_key,
            chain_id=chain_id,
            signature_type=signature_type,
            funder=funder,
        )
        self._creds_ready = False

    @classmethod
    def from_env(cls) -> "ExecutionClient":
        host = os.getenv("POLYMARKET_HOST", "https://clob.polymarket.com")
        private_key = os.environ["PRIVATE_KEY"]
        chain_id = int(os.getenv("POLYMARKET_CHAIN_ID", "137"))
        signature_type = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "2"))
        funder = os.getenv("POLYMARKET_PROXY_ADDRESS")
        return cls(
            host=host,
            private_key=private_key,
            chain_id=chain_id,
            signature_type=signature_type,
            funder=funder,
        )

    def ensure_api_creds(self) -> None:
        if self._creds_ready:
            return
        creds = self.client.create_or_derive_api_creds()
        self.client.set_api_creds(creds)
        self._creds_ready = True

    def build_order_args(
        self,
        *,
        price: float,
        size: float,
        side: str,
        token_id: str,
        **kwargs,
    ) -> OrderArgs:
        if side.lower() not in ("buy", "sell"):
            raise ValueError("side must be 'buy' or 'sell'")
        side_constant = BUY if side.lower() == "buy" else SELL
        return OrderArgs(price=price, size=size, side=side_constant, token_id=token_id, **kwargs)

    def place_order(
        self,
        order_args: OrderArgs,
        order_type: OrderType = OrderType.GTC,
        *,
        timed: bool = True,
    ) -> ExecutionResult:
        self.ensure_api_creds()
        signed_order = self.client.create_order(order_args)
        if not timed:
            resp = self.client.post_order(signed_order, order_type)
            return ExecutionResult(response=resp, timings_ms=[])
        timings: list[tuple[str, float]] = []

        start = time.perf_counter()
        self.client.assert_level_2_auth()
        timings.append(("assert_level_2_auth", (time.perf_counter() - start) * 1000))

        start = time.perf_counter()
        body = order_to_json(signed_order, self.client.creds.api_key, order_type)
        timings.append(("order_to_json", (time.perf_counter() - start) * 1000))

        start = time.perf_counter()
        request_args = RequestArgs(method="POST", request_path=POST_ORDER, body=body)
        timings.append(("build_request_args", (time.perf_counter() - start) * 1000))

        start = time.perf_counter()
        headers = create_level_2_headers(self.client.signer, self.client.creds, request_args)
        timings.append(("create_level_2_headers", (time.perf_counter() - start) * 1000))

        builder_headers = None
        if self.client.can_builder_auth():
            start = time.perf_counter()
            builder_headers = self.client._generate_builder_headers(request_args, headers)
            timings.append(("builder_headers", (time.perf_counter() - start) * 1000))

        start = time.perf_counter()
        resp = post(
            f"{self.client.host}{POST_ORDER}",
            headers=builder_headers or headers,
            data=body,
        )
        timings.append(("http_post", (time.perf_counter() - start) * 1000))
        return ExecutionResult(response=resp, timings_ms=timings)


__all__ = ["ExecutionClient", "ExecutionResult"]
