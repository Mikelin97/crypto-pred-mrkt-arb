from Strategy import Strategy
import numpy as np
from Vol import Vol
from OrderBook import OrderBook
from useful_functions import binary_price, implied_vol_binary

class TheoFollowerTradingStrategy(Strategy):
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

        self.bid_implied_vol = np.nan
        self.ask_implied_vol = np.nan
        self.mid_implied_vol = np.nan

        self.trades = []
        self.position = 0
        self.entry_price = np.nan
        self.total_pnl = 0.0

        
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
        
        r = 0.042 # TODO -> need to fix this?
        if crypto_price != self.crypto_price:
            self.vol_tracker.update_vol_from_price(self.crypto_price)
            # self.vol_tracker.vol = 0.38 # TODO -> fix
            self.crypto_price = crypto_price

        self.order_book.batch_update(book_updates)
        
        T = (self.market_resolve_set_time - timestamp) / self.annualize_factor
        self.tte = T
        self.theo_price = binary_price(self.crypto_price, self.strike_price,
                                    T, self.vol_tracker.vol, r
                        )
        
        if timestamp == self.market_strike_set_time:
            self.strike_price = crypto_price
        
        if timestamp == self.market_resolve_set_time:
            self.resolve_price = crypto_price

        self.bid_implied_vol, _ = implied_vol_binary(self.crypto_price, self.strike_price, T, r, self.order_book.best_bid, anchor_vol=self.vol_tracker.vol)
        self.ask_implied_vol, _ = implied_vol_binary(self.crypto_price, self.strike_price, T, r, self.order_book.best_ask, anchor_vol=self.vol_tracker.vol)
        self.mid_implied_vol, _ = implied_vol_binary(self.crypto_price, self.strike_price, T, r, self.order_book.mid, anchor_vol=self.vol_tracker.vol)

        if not np.isnan(self.resolve_price) and self.position != 0:
            
            if self.resolve_price > self.strike_price:
                self.total_pnl += self.position*(1-self.entry_price)
                self.trades.append({"type": "resolve", "timestamp": timestamp, "price": 1.0})
            elif self.resolve_price < self.strike_price:
                self.total_pnl += -(self.position*self.entry_price)
                self.trades.append({"type": "resolve", "timestamp": timestamp, "price": 0.0})
            
            self.position = 0

        if self.position != -1 and self.theo_price < self.order_book.best_bid:
            if self.position == 1:
                self.total_pnl += (self.order_book.best_bid - self.entry_price)
                self.trades.append({"type": "sell", "timestamp": timestamp, "price": self.order_book.best_bid})
            
            self.position = -1
            self.entry_price = self.order_book.best_bid
        
        elif self.position != 1 and self.theo_price > self.order_book.best_ask:
            if self.position == -1:
                self.total_pnl += (self.entry_price - self.order_book.best_ask)
                self.trades.append({"type": "buy", "timestamp": timestamp, "price": self.order_book.best_ask})
            
            self.position = 1
            self.entry_price = self.order_book.best_ask
        
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
            "bid_implied_vol": self.bid_implied_vol,
            "ask_implied_vol": self.ask_implied_vol,
            "mid_implied_vol": self.mid_implied_vol,
            "trades": self.trades,
            "position": self.position,
            "total_pnl": self.total_pnl
        }