from Strategy import Strategy
import numpy as np
from Vol import Vol
from OrderBook import OrderBook
from useful_functions import binary_price

class BounceGroup:

    def __init__(self, open_timestamp, start_value, up_value, down_value, init_price = 0.5):
        self.open_timestamp = open_timestamp
        self.start_value = start_value
        self.up_value = up_value
        self.down_value = down_value
        self.init_price = init_price
        self.cross_side = 1 if start_value > self.init_price else -1
        self.start_reached = True if start_value == self.init_price else False
        self.start_reached_timestamp = 0 if self.start_reached else np.nan

        self.up_hit_first = False
        self.down_hit_first = False
        self.hit_timestamp = np.nan
    
    def __repr__(self):
        return f"{self.start_value:.3f} up to {self.up_value:.3f} or down to {self.down_value:.3f}"
    
    def update(self, timestamp, new_price) -> bool:
        if self.up_hit_first or self.down_hit_first:
            return True
        
        if not self.start_reached and self.cross_side * (new_price - self.start_value) >= 0:
            self.start_reached = True

        if self.start_reached and new_price >= self.up_value:
            self.up_hit_first = True
            self.hit_timestamp = timestamp
            return True
            
        if self.start_reached and new_price <= self.down_value:
            self.down_hit_first = True
            self.hit_timestamp = timestamp
            return True
        
        return False
    
    @property
    def side_hit_first(self):
        return 1*self.up_hit_first + -1 * self.down_hit_first


class BounceTrackStrategy(Strategy):
    def __init__(self, bounce_pairs, market_strike_set_time, market_resolve_time, time_precision, effective_memory=300):

        """
            Bounce pairs =  list of tuples(strike, up_value, down_value)
        """

        self.crypto_price = np.nan

        self.market_strike_set_time = market_strike_set_time
        self.market_resolve_set_time = market_resolve_time

        secs_in_year = 3.154e+7
        self.annualize_factor = secs_in_year * 10**(time_precision - 10)

        self.strike_price = np.nan
        self.resolve_price = np.nan

        self.theo_price = np.nan

        self.vol_tracker = Vol(effective_memory=effective_memory) # 5 minutes of effective memory?
        self.order_book = OrderBook()

        # Set up tracking for ups and downs
        self.active_bounce_pairs = [BounceGroup(market_strike_set_time, *group) for group in bounce_pairs]
        self.resolved_bounce_pairs = []
        self.num_pairs = len(bounce_pairs)
        
    def on_warmup(self, timestamp, crypto_price, book_updates):
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

    def on_update(self, timestamp, crypto_price, book_updates):
        """Main decision point — return actions or orders."""
        
        if crypto_price != self.crypto_price:
            self.vol_tracker.update_vol_from_price(self.crypto_price)
            self.crypto_price = crypto_price

        self.order_book.batch_update(book_updates)
        
        T = (self.market_resolve_set_time - timestamp) / self.annualize_factor
        self.theo_price = binary_price(self.crypto_price, self.strike_price,
                                    T, self.vol_tracker.vol, r=4.2
                        )
        
        if timestamp == self.market_strike_set_time:
            self.strike_price = crypto_price
        
        if timestamp == self.market_resolve_set_time:
            self.resolve_price = crypto_price
        
        # deal with the bounce pairs
        new_active_pairs = []
        for bounce_group in self.active_bounce_pairs:
            if bounce_group.update(timestamp, self.order_book.mid):
                self.resolved_bounce_pairs.append(bounce_group)
            else:
                new_active_pairs.append(bounce_group)
        self.active_bounce_pairs = new_active_pairs


    def on_trade(self, trade):
        """Optional: Handle trade events if they matter."""
        pass

    def get_state(self):
        """Return current strategy metrics for logging."""
        return {
            "crypto_price": self.crypto_price,
            "vol": self.vol_tracker.vol,
            "market_spread": self.order_book.spread,
            "best_bid": self.order_book.best_bid,
            "best_ask": self.order_book.best_ask,
            "theo_price": self.theo_price,
            "mid_price": self.order_book.mid,
            "lwm_price": self.order_book.lwm,
            "fraction_pairs_resolved": len(self.resolved_bounce_pairs) / self.num_pairs,
            "fraction_pairs_open": len(self.active_bounce_pairs) / self.num_pairs,
            "resolved_pairs": self.resolved_bounce_pairs.copy(),
            "unresolved_pairs" : self.active_bounce_pairs.copy()
        }