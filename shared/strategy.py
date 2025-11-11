class Strategy:
    def __init__(self):
        pass

    def on_warmup(self, timestamp, crypto_price, order_book):
        """Called during warm-up period before official backtest start."""
        raise NotImplementedError

    def on_update(self, timestamp, crypto_price, order_book):
        """Main decision point — return actions or orders."""
        raise NotImplementedError

    def on_trade(self, trade):
        """Optional: Handle trade events if they matter."""
        raise NotImplementedError

    def get_state(self) -> dict:
        """Return current strategy metrics for logging."""
        raise NotImplementedError