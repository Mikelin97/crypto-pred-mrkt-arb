from sortedcontainers import SortedDict
import numpy as np

class OrderBook:

    def __init__(self):
        self.bids = SortedDict(lambda x: -x) # higher bids are top (front) of book
        self.asks = SortedDict() # lowest asks are top (front) of book
        self.updated_at: str = "0" # UNIX TIMESTAMP

    def update(self, side, price, size, timestamp):
        if side not in {"bid", "ask"}:
            raise ValueError(f"Side must be bid or ask, not {side}")

        if size == 0:
            getattr(self, f"{side}s").pop(price, None)
        else:
            getattr(self, f"{side}s")[price] = size
        
        self.updated_at: str = timestamp
    
    def snapshot(self, depth=5):
        bids = list(self.bids.items())[:depth]
        asks = list(self.asks.items())[:depth]
        return {"bids": bids, "asks": asks, "timestamp": self.updated_at}

    def total_volume(self, side):
        book = getattr(self, f"{side}s")
        return sum(book.values())
    
    def execution_ave_price_for_size(self, order_direction: str, size: float) -> float:
        """
        Compute VWAP required to fill `size` units on the opposite side.
        If side='buy', we consume asks; if 'sell', we consume bids.
        """
        if order_direction not in {"buy", "sell"}:
            raise ValueError(f"Order direction must be buy or sell, not {order_direction}")

        book = self.asks if order_direction == "buy" else self.bids
        if not book:
            return np.nan

        remaining = size
        total_value = 0.0
        total_filled = 0.0

        for price, qty in book.items():
            if remaining <= 0:
                break
            trade_size = min(remaining, qty)
            total_value += price * trade_size
            total_filled += trade_size
            remaining -= trade_size

        return total_value / total_filled if total_filled > 0 else np.nan
    
    @property
    def best_bid(self):
        return self.bids.peekitem(0)[0] if self.bids else 0
    
    @property
    def top_market(self):
        bid_level = self.bids.peekitem(0) if self.bids else (np.nan, np.nan)
        ask_level = self.asks.peekitem(0) if self.asks else (np.nan, np.nan)
        return bid_level, ask_level
    
    @property
    def best_ask(self):
        return self.asks.peekitem(0)[0] if self.asks else np.inf
    
    @property
    def spread(self):
        return self.best_ask - self.best_bid if self.bids and self.asks else np.nan # TODO -> could make this max 1 if not
    
    @property
    def mid(self):
        return (self.best_bid + self.best_ask) / 2 if self.bids and self.asks else np.nan

    @property
    def lwm(self):
        """
            Liquidity weighted midpoint to get market price
        """

        (bid_price, bid_size), (ask_price, ask_size) = self.top_market
        if np.nan in (bid_price, bid_size, ask_price, ask_size):
            # print(bid_price, bid_size, ask_price, ask_size, "ONE OF THESE FAILED")
            return np.nan

        return (bid_price * ask_size + ask_price * bid_size) / (bid_size + ask_size)