import pandas as pd
import numpy as np
from collections import deque
import json
import requests
from useful_functions import crypto_raw_to_pandas, parse_all_book_updates, parse_all_trade_prices
from Strategy import Strategy
from OrderBook import OrderBook

def backtest_market(crypto_data_filepath, crypto_data_filename, crypto_symbol, 
                    market_data_filepath, market_slug,
                    unix_start_time, unix_end_time, 
                    warm_up_duration, # how long to warm the models up before the start time
                    strategy:Strategy, # the strategy that is used 
                    time_precision: int = 13 # default milliseconds unix precision -> convert all timestamps to this
                    ):
        
        # TODO -> do we need to sort trades and book updates just in case?
        # TODO -> Need to iterate through the timestamps of all of the market events to change the precision to the expected precision
        # TODO -> need to add reacting to trades 

        if time_precision < 10:
            raise ValueError("time precision can't be less than 10 (secondly)")

        # ensure all times have same precision
        def adjust_precision(timestamp:int, precision:int) -> int:
            return int(np.ceil(timestamp * 10**(precision - len(str(timestamp)))))
        
        unix_start_time = adjust_precision(unix_start_time, time_precision)
        unix_end_time = adjust_precision(unix_end_time, time_precision)
        # warm_up_duration = adjust_precision(warm_up_duration, time_precision) -> can't because it's a time difference
        
        # read in crypto data
        all_coin_df = crypto_raw_to_pandas(f"{crypto_data_filepath}{crypto_data_filename}")
        coin_df = all_coin_df[all_coin_df['symbol'] == crypto_symbol].sort_values(by="unix_timestamp").reset_index(drop=True)

        # get token IDs of the market
        url = "https://gamma-api.polymarket.com/events"
        response = requests.get(url, params={"slug": market_slug})
        yes_token_id, no_token_id = json.loads(response.json()[0]['markets'][0]['clobTokenIds'])

        # get order book updates from our historical data
        messages = []
        with open (f"{market_data_filepath}{market_slug}.jsonl") as f:
            for line in f:
                line_data = json.loads(line)
                messages.append(line_data)

        order_book_updates = parse_all_book_updates(messages)
        yes_book_updates = [update for update in order_book_updates if update['asset_id'] == yes_token_id]
        no_book_updates = [update for update in order_book_updates if update['asset_id'] == no_token_id]
        
        # get trades from both yes and no book perspectives
        trades_yes_perspective, trades_no_perspective = parse_all_trade_prices(messages, yes_token_id, no_token_id)
        
        # create queues for time arrival
        crypto_price_queue = deque(zip(coin_df['unix_timestamp'], coin_df['value']))
        market_update_queue = deque(yes_book_updates)
        # trade_queue = deque(trades_yes_perspective)  # TODO -> add trade data?

        warm_up_start_time = unix_start_time - warm_up_duration

        warm_up_data = []
        backtest_data = []

        # flush any data that is before the warm up start window
        latest_crypto_price = np.nan
        while crypto_price_queue and crypto_price_queue[0][0] < warm_up_start_time:
            _, latest_crypto_price = crypto_price_queue.popleft()
        while market_update_queue and market_update_queue[0]['timestamp'] < warm_up_start_time:
            market_update_queue.popleft()

        # warm up strategy
        while crypto_price_queue or market_update_queue:
            # pick whichever event occurs next
            next_times = []
            if crypto_price_queue:
                next_crypto_time, _ = crypto_price_queue[0]
                next_times.append(next_crypto_time)
            
            if market_update_queue:
                next_market_time = market_update_queue[0]['timestamp']
                next_times.append(next_market_time)
            
            next_timestamp = min(next_times)

            if next_timestamp >= unix_start_time:
                # exit once we hit backtest time
                break
                
            # update crypto price if needed
            if crypto_price_queue and crypto_price_queue[0][0] == next_timestamp:
                _, latest_crypto_price = crypto_price_queue.popleft()
            
            # update order book if needed
            market_updates = []
            while market_update_queue[0]['timestamp'] == next_timestamp:
                market_update = market_update_queue.popleft()
                market_updates.append(market_update)

            strategy.on_warmup(next_timestamp, latest_crypto_price, market_updates)
            warm_up_data.append({
                    "timestamp": next_timestamp,
                    **strategy.get_state()
                }
            )
            
        # run the backtest
        while crypto_price_queue or market_update_queue:
            
            # pick whichever event occurs next
            next_times = []
            if crypto_price_queue:
                next_crypto_time, _ = crypto_price_queue[0]
                next_times.append(next_crypto_time)
            
            if market_update_queue:
                next_market_time = market_update_queue[0]['timestamp']
                next_times.append(next_market_time)
            
            next_timestamp = min(next_times)

            if next_timestamp > unix_end_time:
                # exit once we hit end of backtest
                break
                
            # update crypto price if needed
            if crypto_price_queue and crypto_price_queue[0][0] == next_timestamp:
                _, latest_crypto_price = crypto_price_queue.popleft()
            
            # update order book if needed
            market_updates = []
            while market_update_queue[0]['timestamp'] == next_timestamp:
                market_update = market_update_queue.popleft()
                market_updates.append(market_update)

            strategy.on_update(next_timestamp, latest_crypto_price, market_updates)
            backtest_data.append({
                    "timestamp": next_timestamp,
                    **strategy.get_state()
                }
            )    
        
        return warm_up_data, backtest_data