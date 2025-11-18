"""Strategy implementations for polymarket backtesting."""

from __future__ import annotations

import numpy as np

from typing import Dict

from shared.orderbook import OrderBook
from shared.strategy import Strategy
from shared.vol import Vol
from useful_functions import binary_price


class DataTrackStrategy(Strategy):
    """Simple theoretical-value tracker used for on-demand backtests."""

    def __init__(
        self,
        market_strike_set_time: int,
        market_resolve_time: int,
        time_precision: int,
        *,
        effective_memory: int = 300,
        risk_free_rate: float = 0.042,
    ) -> None:
        self.crypto_price = np.nan
        self.market_strike_set_time = market_strike_set_time
        self.market_resolve_set_time = market_resolve_time
        self.annualize_factor = 3.154e7 * 10 ** (time_precision - 10)
        self.strike_price = np.nan
        self.resolve_price = np.nan
        self.theo_price = np.nan
        self.risk_free_rate = risk_free_rate

        self.vol_tracker = Vol(effective_memory=effective_memory)
        self.order_books: Dict[str, OrderBook] = {}

    def _sync_crypto_price(self, crypto_price: float) -> None:
        if np.isnan(crypto_price):
            return
        if crypto_price != self.crypto_price:
            self.vol_tracker.update_vol_from_price(self.crypto_price)
            self.crypto_price = crypto_price

    def _handle_market_markers(self, timestamp: int) -> None:
        if timestamp == self.market_strike_set_time:
            self.strike_price = self.crypto_price
        if timestamp == self.market_resolve_set_time:
            self.resolve_price = self.crypto_price

    def _update_order_books(self, updates) -> None:
        for update in updates:
            asset_id = update.get("asset_id")
            if not asset_id:
                continue
            book = self.order_books.setdefault(asset_id, OrderBook())
            book.update(
                side=update["side"],
                price=update["price"],
                size=update["size"],
                timestamp=update["timestamp"],
                check_cross=False,
            )

    def on_warmup(self, timestamp, crypto_price, order_book_updates):
        self._sync_crypto_price(crypto_price)
        self._update_order_books(order_book_updates)
        self._handle_market_markers(timestamp)

    def on_update(self, timestamp, crypto_price, order_book_updates):
        self._sync_crypto_price(crypto_price)
        self._update_order_books(order_book_updates)

        time_to_expiry = self.market_resolve_set_time - timestamp
        if time_to_expiry > 0 and not np.isnan(self.strike_price):
            t_years = time_to_expiry / self.annualize_factor
            self.theo_price = binary_price(
                self.crypto_price,
                self.strike_price,
                t_years,
                self.vol_tracker.vol,
                r=self.risk_free_rate,
            )
        else:
            self.theo_price = np.nan

        self._handle_market_markers(timestamp)

    def on_trade(self, trade):
        pass

    def get_state(self):
        books_state = {}
        top_mid = -np.inf
        top_asset_id = None
        for asset_id, book in self.order_books.items():
            best_bid = book.bids.peekitem(0)[0] if book.bids else np.nan
            best_ask = book.asks.peekitem(0)[0] if book.asks else np.nan
            mid = book.mid
            books_state[asset_id] = {
                "best_bid": best_bid,
                "best_ask": best_ask,
                "mid_price": mid,
                "spread": book.spread,
            }
            if not np.isnan(mid) and mid > top_mid:
                top_mid = mid
                top_asset_id = asset_id

        top_book = books_state.get(top_asset_id) if top_asset_id else None
        best_bid = top_book["best_bid"] if top_book else np.nan
        best_ask = top_book["best_ask"] if top_book else np.nan
        lwm_price = (
            self.order_books[top_asset_id].lwm if top_asset_id else np.nan
        )

        return {
            "crypto_price": self.crypto_price,
            "vol": self.vol_tracker.vol,
            "market_spread": top_book["spread"] if top_book else np.nan,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "theo_price": self.theo_price,
            "mid_price": top_book["mid_price"] if top_book else np.nan,
            "lwm_price": lwm_price,
            "books": books_state,
        }
