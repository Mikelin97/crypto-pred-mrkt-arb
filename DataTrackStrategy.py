from Strategy import Strategy
import numpy as np
from Vol import Vol
from OrderBook import OrderBook
from useful_functions import binary_price

class DataTrackStrategy(Strategy):
    def __init__(self, market_strike_set_time, market_resolve_time, time_precision, effective_memory=300):
        self.crypto_price = np.nan

        self.market_strike_set_time = market_strike_set_time
        self.market_resolve_set_time = market_resolve_time

        secs_in_year = 3.154e+7
        self.annualize_factor = secs_in_year * 10**(time_precision - 10)
        self.tte = np.nan

        self.strike_price = np.nan
        self.resolve_price = np.nan

        self.theo_price = np.nan

        self.vol_tracker = Vol(effective_memory=effective_memory) # 5 minutes of effective memory?
        self.order_book = OrderBook()
        
    def on_warmup(self, timestamp, crypto_price, book_updates, trades):
        """Called during warm-up period before official backtest start."""
        #TODO -> this assumes if the crypto price is the same, then it wasn't an update for vol
        if crypto_price != self.crypto_price:
            self.vol_tracker.update_vol_from_price(self.crypto_price)
            self.crypto_price = crypto_price

        self.order_book.batch_update(book_updates)
        
        if timestamp == self.market_strike_set_time:
            self.strike_price = crypto_price
        
        if timestamp == self.market_resolve_set_time:
            self.resolve_price = crypto_price

    def on_update(self, timestamp, crypto_price, book_updates, trades):
        """Main decision point — return actions or orders."""
        r = 0.042
        if crypto_price != self.crypto_price:
            self.vol_tracker.update_vol_from_price(self.crypto_price)
            # self.vol_tracker.vol = 0.38 # TODO -> fix
            self.crypto_price = crypto_price

        self.order_book.batch_update(book_updates)
        
        T = (self.market_resolve_set_time - timestamp) / self.annualize_factor
        self.tte = T
        self.theo_price = binary_price(self.crypto_price, self.strike_price,
                                    T, self.vol_tracker.vol, r=r
                        )
        
        if timestamp == self.market_strike_set_time:
            self.strike_price = crypto_price
        
        if timestamp == self.market_resolve_set_time:
            self.resolve_price = crypto_price

    def on_trade(self, trade):
        """Optional: Handle trade events if they matter."""
        pass

    def get_state(self):
        """Return current strategy metrics for logging."""
        return{
            "crypto_price": self.crypto_price,
            "strike_price" : self.strike_price,
            "tte": self.tte,
            "vol": self.vol_tracker.vol,
            "market_spread": self.order_book.spread,
            "best_bid": self.order_book.best_bid,
            "best_ask": self.order_book.best_ask,
            "theo_price": self.theo_price,
            "mid_price": self.order_book.mid,
            "lwm_price": self.order_book.lwm,
            "bid_data": self.order_book.bids.copy(),
            "ask_data": self.order_book.asks.copy(),
        }