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
    d2 = (np.log(S/X) - (r - (vol**2)/2) * T) / (vol * np.sqrt(T))

    price = np.exp(-r*T) * norm.cdf(d2)
    return price


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
    json_df = pd.read_json(path_or_buf=file_path, lines=True)
    expanded_payload = pd.json_normalize(json_df['payload'])
    expanded_payload = expanded_payload.rename(columns={'timestamp': "unix_timestamp"})
    crypto_df = pd.concat([json_df.drop(['payload', 'symbol', 'symbol_timestamp'], axis=1), expanded_payload], axis=1)

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
            "timestamp": timestamp,
            "price": bid['price'],
            "size": bid['size'],
            "side": "bid",
        }
        order_book_updates.append(update)
    for ask in asks:
        update = {
            "asset_id": asset_id,
            "timestamp": timestamp,
            "price": ask['price'],
            "size": ask['size'],
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
                "timestamp": timestamp,
                "price": price_change['price'],
                "size": price_change['size'],
                "side": "bid" if 'side' == "BUY" else "ask"
            }
        order_book_updates.append(update)

    return order_book_updates


def parse_all_book_updates(stream_data):
    """
        Currently broken, as the book type is identical
        for the Yes and No markets, but the updates show
        the mirrored view, which means that they don't work naturally
        together as this function assumed... will need to flip the book for no
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


