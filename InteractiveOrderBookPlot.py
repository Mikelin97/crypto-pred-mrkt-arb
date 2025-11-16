import matplotlib.pyplot as plt
from matplotlib.widgets import Button, TextBox
import numpy as np
import datetime as dt

class InteractiveBarPlot:
    def __init__(self, timestamps:list, plot_data_by_time:dict, initial_jump=5, initial_percentile=80):
        '''
            timestamps is a list of all of the timestamps present in the order book data
            plot_data_by_time is a dict with timestamp as the key and a tuple of (prices, sizes, colors, theo, crypto_price, strike_price) as the value
        
        '''
        self.timestamps = timestamps
        self.plot_data_by_time = plot_data_by_time
        self.current_index = 0
        self.jump_amount = initial_jump
        self.max_bar_percentile = initial_percentile

        # Create figure and axis
        self.fig, self.ax = plt.subplots(figsize=(24, 6))
        plt.subplots_adjust(bottom=0.2)

        # Create buttons and text boxes
        self._create_widgets()

        # Initial plot
        self.update_plot(self.current_index)

    def update_plot(self, idx):
        """Redraw the bar chart for a given timestamp index."""
        self.ax.clear()
        ts = self.timestamps[idx]
        prices, sizes, colors, theo, crypto_price, strike = self.plot_data_by_time[ts]

        self.ax.bar(prices, sizes, color=colors, width=0.008)
        self.ax.set_title(f"Strike: {strike:.4f}, Price: {crypto_price:.4f} ({(crypto_price-strike):.4f}) | Timestamp: {ts}, {dt.datetime.fromtimestamp(ts/1000)}")
        self.ax.set_xlabel("Price")
        self.ax.set_ylabel("Size")
        self.ax.set_xticks(prices)
        self.ax.set_xticklabels(prices, rotation=-90)
        max_shown_y = np.percentile(sizes, self.max_bar_percentile)
        self.ax.set_ylim(bottom=0, top=max_shown_y)

        self.ax.axvline(theo, ymin=0, ymax=max_shown_y, color="gold", linestyle="--", linewidth=2, label="Theo")
        self.ax.text(theo, max_shown_y * 0.95, f"Theo: {theo:.4f}",
                     rotation=90, va='top', ha='right', color='black', fontsize=10)

        self.fig.canvas.draw_idle()

    def next_frame(self, event):
        """Move forward by jump_amount frames."""
        self.current_index = (self.current_index + self.jump_amount) % len(self.timestamps)
        self.update_plot(self.current_index)

    def prev_frame(self, event):
        """Move backward by jump_amount frames."""
        self.current_index = (self.current_index - self.jump_amount) % len(self.timestamps)
        self.update_plot(self.current_index)

    def update_jump_amount(self, text):
        """Update how many frames to jump per button press."""
        try:
            value = int(text)
            if value <= 0:
                raise ValueError
            self.jump_amount = value
        except ValueError:
            print("Jump amount must be a positive integer")

    def update_max_bar(self, text):
        """Update the percentile for Y-axis scaling."""
        try:
            value = float(text)
            if not (0 < value <= 100):
                raise ValueError
            self.max_bar_percentile = value
            self.update_plot(self.current_index)
        except ValueError:
            print("Please enter a valid number between 0 and 100.")

    def _create_widgets(self):
        """Set up the interactive UI elements."""
        # Buttons
        self.ax_prev = plt.axes([0.7, 0.05, 0.1, 0.075])
        self.ax_next = plt.axes([0.8, 0.05, 0.1, 0.075])
        self.prev_btn = Button(self.ax_prev, 'Prev')
        self.next_btn = Button(self.ax_next, 'Next')
        self.prev_btn.on_clicked(self.prev_frame)
        self.next_btn.on_clicked(self.next_frame)

        # Jump amount text box
        self.ax_jump = plt.axes([0.55, 0.05, 0.03, 0.05])
        self.jump_box = TextBox(self.ax_jump, "Index Jump: ", initial=str(self.jump_amount))
        self.jump_box.on_submit(self.update_jump_amount)

        # Max bar percentile text box
        self.ax_bar = plt.axes([0.2, 0.05, 0.03, 0.05])
        self.bar_box = TextBox(self.ax_bar, "Bar %: ", initial=str(self.max_bar_percentile))
        self.bar_box.on_submit(self.update_max_bar)

# Usage example:
# plot = InteractiveBarPlot(timestamps, plot_data_by_time)