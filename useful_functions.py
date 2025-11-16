import numpy as np
from scipy.stats import norm
import requests
import pandas as pd
from sortedcontainers import SortedDict

# d1 = (np.log(S/X) + (b + (vol**2)/2) * T) / (vol * np.sqrt(T))
# d2 = d1 - vol * np.sqrt(T)
def binary_price(S, X, T, vol, r):
    """
        Binary option pricing model based on the Black-Scholes equation
    """
    if T <= 0:
        return 1 if S > X else 0

    d2 = (np.log(S/X) + (r - (vol**2)/2) * T) / (vol * np.sqrt(T))

    price = np.exp(-r*T) * norm.cdf(d2)
    return price

def implied_vol_binary(S, X, T, r, price, anchor_vol, dist_weight = 10):
    """
        Solve for implied volatility(s) sigma from a binary (cash-or-nothing) call price
        p = exp(-rT) * N(d2) under Black-Scholes.
        Returns:
        IF WE HAVE AN ANCHOR:
            - a single sigma if `anchor_vol` is provided (the root closest to anchor_vol)
                - and a confidence label based on distance from anchor (0 to 1 for high and 0 to -1 for low)
            - np.nan if no valid root + confidence = 0.0
        
            - if you want the other level, you can back it out
        
    """
    # ensure the price is within no arbitrage
    p_min = 0.0
    p_max = np.exp(-r * T)
    if not (p_min < price < p_max):
        return np.nan, 0.0

    q = price / np.exp(-r * T)
    if not (0.0 < q < 1.0):
        return np.nan, 0.0
    
    z = norm.ppf(q) #inverse cdf of price / e^-rt

    a = 0.5*T
    b = z*np.sqrt(T)
    c = -(np.log(S/X) + r*T)

    disc = b**2 - 4 * a * c
    if disc < 0:
        return np.nan, 0.0

    sqrt_disc = np.sqrt(disc)
    vol_high = (-b + sqrt_disc) / (2 * a)
    vol_low = (-b - sqrt_disc) / (2 * a)

    # With anchor, figure out which root to use
    if vol_high < 0 and vol_low < 0:
        return np.nan, 0.0
    
    if vol_low < 0:
        return vol_high, 1.0
    
    if vol_high < 0:
        return vol_low, -1.0

    # if both roots could be valid and we have an anchor to work with
    dist_high = abs(vol_high - anchor_vol)
    dist_low = abs(vol_low - anchor_vol)
    
    def flipped_sigmoid(x, weight = 10):
        # weight = how heavily to penalize excess distance in confidence
        return 1 / (1 + np.exp(weight*x))
    
    sig_high = flipped_sigmoid(dist_high, weight=dist_weight)
    sig_low = flipped_sigmoid(dist_low, weight=dist_weight)
    
    confidence = (sig_high - sig_low)/ (sig_high + sig_low)

    if sig_high > sig_low:
        return vol_high, confidence
    else:
        return vol_low, confidence

def get_all_events(closed="false", tag_id = ''):

    """
        Get all non-closed market events with the option to filter based on a tag_id, which is useful
    """
    params = {
        "closed": closed,
        "limit" : 500,
        "offset" : 0,
        # 'tag_id': ''
    }
    if tag_id:
        params['tag_id'] = tag_id

    events = []
    r = requests.get(url="https://gamma-api.polymarket.com/events", params=params)
    response = r.json()
    while response:
        events += response
        params['offset'] += 500
        r = requests.get(url="https://gamma-api.polymarket.com/events", params=params)
        response = r.json()
    
    return events
# get_all_events(tag_id='102467') -> get 15 minute events, which is just the crypto up-down at the moment

def filter_crypto_events(events):
    """
        Filter events based on these known crypto tags
    """
    crypto_events = []

    for event in events:
        if 'tags' not in event:
            print("No tags in", event['ticker'])
            continue
        tags = event['tags']
        for tag in tags:
            if tag['slug'] in ['crypto', 'crypto-prices', 'bitcoin', 'ethereum', 'stablecoins', 'solana', 'xrp']:
                crypto_events.append(event)
                break
    
    return crypto_events

def get_short_term_events(events):
    """
        Filter events based on the 3 short term tag slugs
    """
    short_events = []
    for event in events:
        if 'tags' not in event:
            print("No tags in", event['ticker'])
            continue
        tags = event['tags']
        for tag in tags:
            if tag['slug'] in ['15M', '1H', '4h']:
                short_events.append(event)
                break
    return short_events


def get_market_trades(condition_id):
    """
        Get all trades for a market based on its conition id
    """
    # condition_id = "0x56f8de8878e825b73937be862fdcd7af2497c5b73df1dac0fd9113bf11846176"
    url = "https://data-api.polymarket.com/trades"
    params = {
        "limit": 500,
        "offset": 0,
        "market": [condition_id],
        # "takerOnly": "true",
    }
    
    total_trades = []
    response = requests.get(url, params=params)
    cur_trades = response.json()

    while cur_trades:
        total_trades += cur_trades
        params['offset'] += 500
        response = requests.get(url, params=params)
        cur_trades = response.json()

    return total_trades


def crypto_raw_to_pandas(file_path):
    """
        Take chainlink or binance raw data from a file and convert it inot pandas format for analysis
    """
    json_df = pd.read_json(path_or_buf=file_path, lines=True, convert_dates=False)
    expanded_payload = pd.json_normalize(json_df['payload'])
    expanded_payload = expanded_payload.rename(columns={'timestamp': "unix_timestamp"})
    crypto_df = pd.concat([json_df.drop(['payload', 'symbol', 'symbol_timestamp'], axis=1, errors='ignore'), expanded_payload], axis=1)

    return crypto_df

def compute_vol(returns: pd.Series, effective_memory):
    """
        Compute the annualized volatility of a 1-second data 
        pandas return series using exponentially weighted volatilty 

        Effective memory = ~ the number of data points that have
        significant weight in the calculation, with greater effective
        memory = smoother curve
    """
    # effective_memory = 1/(1-var_lambda)
    var_lambda = 1-1/effective_memory
    ewm_var = returns.ewm(alpha = (1-var_lambda)).var(bias=True)
    secs_in_year = 3.154e+7 # TODO -> make this ENV instead of just a random constant
    vol = np.sqrt(ewm_var) * np.sqrt(secs_in_year)
    return vol

# PARSING ORDER BOOK DATA
def parse_book(book_message):
    """
        Get all of the order book updates from a book
        message event in the polymarket market data stream

        # TODO -> could add 0 size to all bids and asks that aren't there
    """
    condition_id = book_message['market']
    asset_id = book_message['asset_id']
    timestamp = book_message['timestamp']
    hash = book_message['hash'] # hash summary of orderbook content
    bids = book_message['bids']
    asks = book_message['asks']

    # use this to generate updates
    order_book_updates = []
    for bid in bids:
        update = {
            "asset_id": asset_id,
            "timestamp": int(timestamp),
            "price": float(bid['price']),
            "size": float(bid['size']),
            "side": "bid",
        }
        order_book_updates.append(update)
    for ask in asks:
        update = {
            "asset_id": asset_id,
            "timestamp": int(timestamp),
            "price": float(ask['price']),
            "size": float(ask['size']),
            "side": "ask",
        }
        order_book_updates.append(update)

    return order_book_updates

def parse_price_change(price_change_event):
    """
        Get the order book updates from a price change
        event in the polymarket market data stream
    """
    condition_id = price_change_event['market']
    price_changes = price_change_event['price_changes']
    timestamp = price_change_event['timestamp']
    order_book_updates = []
    
    for price_change in price_changes:
        # NOTE, also has best bid and best ask data, but idrc
        update = {
                "asset_id": price_change['asset_id'],
                "timestamp": int(timestamp),
                "price": float(price_change['price']),
                "size": float(price_change['size']),
                "side": "bid" if price_change['side'] == "BUY" else "ask"
            }
        order_book_updates.append(update)

    return order_book_updates

def parse_all_book_updates(stream_data):
    """
        Get all of the order book updates, both full book
        and price changes

        TODO -> whenever a book is sent, we should add updates
        that are all 0 for the other levels to ensure they happen
        properly
    """
    order_book_data = []
    for message in stream_data:
        order_book_updates = []
        if isinstance(message, list):
            for event in message:
                event_type = event['event_type']
                if event_type == "book":
                    order_book_updates = parse_book(event)
                elif event_type == "price_change":
                    order_book_updates = parse_price_change(event)
                elif event_type == "tick_size_change":
                    pass # old tick size vs new tick size
                elif event_type == "last_trade_price":
                    pass
                    # trade_event.append(event)
                else:
                    pass

                order_book_data += order_book_updates
                order_book_updates.clear()

        elif isinstance(message, dict):
            event = message
            event_type = event['event_type']
            if event_type == "book":
                order_book_updates = parse_book(event)
            elif event_type == "price_change":
                order_book_updates = parse_price_change(event)
            elif event_type == "tick_size_change":
                pass # old tick size vs new tick size
                print("TICK")
            elif event_type == "last_trade_price":
                pass
                # trade_event.append(event)
                # print("TRADE")
            else:
                pass
            order_book_data += order_book_updates
            order_book_updates.clear()
        else:
            pass
    
    return order_book_data

def parse_last_trade_price(trade_event):
    # condition_id = trade_event['market']
    update = {
        "asset_id": trade_event['asset_id'],
        "timestamp": int(trade_event['timestamp']),
        "price": float(trade_event['price']),
        "size": float(trade_event['size']),
        "side": "bid" if trade_event['side'] == "BUY" else "ask",
        "fee_rate_bps": float(trade_event['fee_rate_bps'])
    }

    return update

def parse_all_trade_prices(stream_data, yes_token_id, no_token_id):
    yes_perspective_trades = []
    no_perspective_trades = []
    for message in stream_data:
        if isinstance(message, dict):
            event = message
            event_type = event['event_type']
            if event_type == "last_trade_price":
                trade_data = parse_last_trade_price(event)
                if trade_data['asset_id'] == yes_token_id:
                    yes_perspective_trades.append(trade_data.copy())
                    # convert to no perspective
                    trade_data['asset_id'] = no_token_id
                    trade_data['price'] = 1-trade_data['price']
                    trade_data['side'] = 'bid' if trade_data['side'] == 'ask' else 'ask'
                    no_perspective_trades.append(trade_data)
                else:
                    no_perspective_trades.append(trade_data.copy())
                    # convert to yes perspective
                    trade_data['asset_id'] = yes_token_id
                    trade_data['price'] = 1-trade_data['price']
                    trade_data['side'] = 'bid' if trade_data['side'] == 'ask' else 'ask'
                    yes_perspective_trades.append(trade_data)

    return yes_perspective_trades, no_perspective_trades



def reconstruct_order_book(data:pd.DataFrame, asset_id, timestamp):

    """
        Take in pandas data an asset id and a timestamp, and return an
        order book up to date with that timestamp
    """
    order_book = {}
    # order_book['bid'] = {{"price": x, "size": 0} for x in np.linspace(0, 1, int(1/tick_size)+1)}
    # order_book['ask'] = {{"price": x, "size": 0} for x in np.linspace(0, 1, int(1/tick_size)+1)}
    order_book['bid'] = SortedDict()
    order_book['ask'] = SortedDict()
    updates = data[data['asset_id']==asset_id]
    updates = updates.sort_values(by="timestamp")

    for _, row in  updates[updates['timestamp'] <= timestamp].iterrows():
        if row['size'] == 0:
            order_book[row['side']].pop(row['price'], None)
        else:
            order_book[row['side']][row['price']] = row['size']
    
    return order_book


