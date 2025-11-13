"""Strategy implementations for polymarket backtesting."""

from __future__ import annotations

import numpy as np

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
        self.order_book = OrderBook()

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

    def on_warmup(self, timestamp, crypto_price, order_book_updates):
        self._sync_crypto_price(crypto_price)
        self.order_book.batch_update(order_book_updates)
        self._handle_market_markers(timestamp)

    def on_update(self, timestamp, crypto_price, order_book_updates):
        self._sync_crypto_price(crypto_price)
        self.order_book.batch_update(order_book_updates)

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
        return {
            "crypto_price": self.crypto_price,
            "vol": self.vol_tracker.vol,
            "market_spread": self.order_book.spread,
            "best_bid": self.order_book.best_bid,
            "best_ask": self.order_book.best_ask,
            "theo_price": self.theo_price,
            "mid_price": self.order_book.mid,
            "lwm_price": self.order_book.lwm,
        }
