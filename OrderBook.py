from sortedcontainers import SortedDict
import numpy as np

class OrderBook:

    def __init__(self):
        self.bids = SortedDict(lambda x: -x) # higher bids are top (front) of book
        self.asks = SortedDict() # lowest asks are top (front) of book
        self.updated_at: str = "0" # UNIX TIMESTAMP

    def update(self, side, price, size, timestamp, check_cross = True):
        if side not in {"bid", "ask"}:
            raise ValueError(f"Side must be bid or ask, not {side}")

        if size == 0:
            getattr(self, f"{side}s").pop(price, None)
        else:
            getattr(self, f"{side}s")[price] = size
        
        self.updated_at: str = timestamp
        if check_cross:
            return self.check_cross()
        else:
            return False
    
    def batch_update(self, updates: list):
        for update in updates:
            self.update(side=update['side'], price=update['price'], size=update['size'], timestamp = update['timestamp'], check_cross=False)
        
        return self.check_cross()

    
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
    
    def check_cross(self):
        if not self.bids or not self.asks:
            return False  # Can't be crossed if one side empty
        bid = self.best_bid
        ask = self.best_ask
        if bid >= ask:
            print(f"[CROSS WARNING] Book crossed at {self.updated_at}: bid={bid}, ask={ask}")
            return True
        return False
    
    def resolve_cross(self):
        """
            Can implement if we want to, but polymarket data has been pretty good at resolving on its own
            within 1 second or so (usually just at the same mid price temporarily, which is super easy)
        """
        pass
    
    def pretty_print(self):
        print("-"*5, "ORDER BOOK", "-"*5)
        print("|  PRICE  |  SIZE  |")
        print("ASKS")
        for price, size in list(self.asks.items())[::-1]:
            print(f"|  {price}  |  {size}  |")
        
        print("-"*20)
        print("BIDS")
        for price, size in self.bids.items():
            print(f"|  {price}  |  {size}  |")
    
    @property
    def best_bid(self):
        return self.bids.peekitem(0)[0] if self.bids else 0.0
    
    @property
    def top_bid_level(self):
        price, size = self.bids.peekitem(0) if self.bids else (np.nan, np.nan)
        return price, size
    
    @property
    def top_market(self):
        bid_level = self.bids.peekitem(0) if self.bids else (np.nan, np.nan)
        ask_level = self.asks.peekitem(0) if self.asks else (np.nan, np.nan)
        return bid_level, ask_level
    
    @property
    def best_ask(self):
        return self.asks.peekitem(0)[0] if self.asks else 1.0
    
    @property
    def top_ask_level(self):
        price, size = self.asks.peekitem(0) if self.asks else (np.nan, np.nan)
        return price, size
    
    @property
    def spread(self):
        return self.best_ask - self.best_bid if self.bids and self.asks else np.nan # TODO -> could make this max 1 if not
    
    def min_volume_spread(self, min_volume):
        ask_volume = 0
        bid_volume = 0
        bid_price = np.nan
        ask_price = np.nan

        for price, size in self.bids.items():
            bid_volume += size
            if bid_volume >= min_volume:
                bid_price = price
                break
        
        for price, size in self.asks.items():
            ask_volume += size
            if ask_volume >= min_volume:
                ask_price = price
                break
        
        return ask_price - bid_price
    
    @property
    def mid(self):
        if self.bids and self.asks:
            return (self.best_bid + self.best_ask) / 2
        elif self.bids:
            return (self.best_bid + 1.0) / 2
        elif self.asks:
            return (0.0 + self.best_ask) / 2
        else:
            return np.nan
    
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