from InteractiveOrderBookPlot import InteractiveBarPlot
import json
import matplotlib.pyplot as plt

# This is just run a backtest using a datatrackstrategy and then save the backtest data
with open("orderbook_interactive_plot_test_data.json") as f:
# with open("orderbook_interactive_plot_test_VOL_data.json") as f:
    data = json.load(f)

data = [dp for dp in data if dp['bid_data'] or dp['ask_data']] # ensure there is some book data

timestamps = [datapoint['timestamp'] for datapoint in data]

plot_data_by_time = {}
for datapoint in data:

    bid_prices = []
    bid_sizes = []
    ask_prices = []
    ask_sizes = []
    for price, size in datapoint['bid_data'].items():
        bid_prices.append(float(price))
        bid_sizes.append(float(size))
    
    if bid_prices and bid_sizes:
        bid_prices, bid_sizes = zip(*sorted(zip(bid_prices, bid_sizes), key=lambda x: x[0]))
        bid_prices = list(bid_prices)
        bid_sizes = list(bid_sizes)
    
    for price, size in datapoint['ask_data'].items():
        ask_prices.append(float(price))
        ask_sizes.append(float(size))
    
    if ask_prices and ask_sizes:
        ask_prices, ask_sizes = zip(*sorted(zip(ask_prices, ask_sizes)))
        ask_prices = list(ask_prices)
        ask_sizes = list(ask_sizes)
    
    ts = int(datapoint['timestamp'])
    theo = float(datapoint['theo_price'])
    crypto_price = datapoint['crypto_price']
    strike = datapoint['strike_price']
    
    colors = ["green"]*len(bid_prices) + ["red"]*len(ask_prices)
    
    plot_data_by_time[ts] = (bid_prices+ask_prices, bid_sizes+ask_sizes, colors, theo, crypto_price, strike)

if __name__ == "__main__":
    plot = InteractiveBarPlot(timestamps, plot_data_by_time)
    plt.show(block=True)