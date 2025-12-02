from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Tuple

import pandas as pd
import streamlit as st
import altair as alt


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from shared.orderbook import OrderBook
from shared.fetcher import Fetcher

EXCLUDED_PRICES = {0.99, 0.01}


st.set_page_config(page_title="Order Book Visualizer", layout="wide")
st.title("Order Book Visualizer")
st.caption("Inspect ClickHouse order book snapshots and updates for a token, then scrub through time.")


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


@st.cache_resource(show_spinner=False)
def get_fetcher() -> Fetcher | None:
    try:
        return Fetcher()
    except Exception:
        return None


@st.cache_data(show_spinner=False)
def fetch_time_bounds(token_id: str) -> Tuple[datetime | None, datetime | None]:
    fetcher = get_fetcher()
    if fetcher is None:
        return None, None

    return fetcher.fetch_time_bounds(token_id)


@st.cache_data(show_spinner=False)
def fetch_snapshots(token_id: str, start: datetime, end: datetime, limit: int | None, excluded_prices=None) -> pd.DataFrame:
    fetcher = get_fetcher()
    if fetcher is None:
        return pd.DataFrame()
    df = fetcher.fetch_snapshots(token_id, start, end, limit, excluded_prices=excluded_prices)
    if not df.empty and "snapshot_timestamp" in df:
        df["snapshot_timestamp"] = pd.to_datetime(df["snapshot_timestamp"], utc=True, format="%Y-%m-%d %H:%M:%S.%f")
    return df


@st.cache_data(show_spinner=False)
def fetch_updates(token_id: str, start: datetime, end: datetime, limit: int | None, excluded_prices=None) -> pd.DataFrame:
    fetcher = get_fetcher()
    if fetcher is None:
        return pd.DataFrame()
    df = fetcher.fetch_updates(token_id, start, end, limit, excluded_prices=excluded_prices)
    return df


@st.cache_data(show_spinner=False)
def fetch_tokens_for_series(series_id: str) -> pd.DataFrame:
    fetcher = get_fetcher()
    if fetcher is None:
        return pd.DataFrame()
    return fetcher.fetch_tokens_for_series(series_id)


def _top_mid_series(snapshots: pd.DataFrame) -> pd.DataFrame:
    """Compute volume-weighted mid using top bid/ask from snapshots."""
    if snapshots.empty:
        return pd.DataFrame(columns=["timestamp", "mid"])
    records = []
    for ts, group in snapshots.groupby("snapshot_timestamp"):
        ts_floor = pd.to_datetime(ts, utc=True).floor("S")
        bid = group[group["side"] == "bid"]
        ask = group[group["side"] == "ask"]
        if bid.empty or ask.empty:
            continue
        bid_price = bid["top_price"].iloc[0]
        bid_size = bid["top_size"].iloc[0]
        ask_price = ask["top_price"].iloc[0]
        ask_size = ask["top_size"].iloc[0]
        try:
            bid_p = float(bid_price)
            bid_s = float(bid_size)
            ask_p = float(ask_price)
            ask_s = float(ask_size)
        except (TypeError, ValueError):
            continue
        if bid_s <= 0 or ask_s <= 0:
            continue
        mid = (bid_p * ask_s + ask_p * bid_s) / (bid_s + ask_s)
        records.append({"timestamp": ts_floor, "mid": mid})
    if not records:
        return pd.DataFrame(columns=["timestamp", "mid"])
    df = pd.DataFrame(records)
    df = df.groupby("timestamp", as_index=False)["mid"].mean()
    return df.sort_values("timestamp")


def build_book_at(ts: datetime, snapshots: pd.DataFrame, updates: pd.DataFrame) -> OrderBook:
    book = OrderBook()
    base_ts: datetime | None = None

    if not snapshots.empty:
        eligible = snapshots[snapshots["snapshot_timestamp"] <= ts]
        if eligible.empty:
            base_ts = snapshots["snapshot_timestamp"].min()
            eligible = snapshots[snapshots["snapshot_timestamp"] == base_ts]
        else:
            base_ts = eligible["snapshot_timestamp"].max()
            eligible = snapshots[snapshots["snapshot_timestamp"] == base_ts]

        for _, row in eligible.iterrows():
            levels = row.get("book") or []
            for level in levels:
                price = _to_float(level.get("price"))
                size = _to_float(level.get("size"))
                if price is None or size is None:
                    continue
                book.update(
                    side=row.get("side", "bid"),
                    price=price,
                    size=size,
                    timestamp=base_ts.isoformat() if base_ts else "0",
                    check_cross=False,
                )

    if not updates.empty:
        if base_ts:
            window = updates[(updates["update_timestamp"] > base_ts) & (updates["update_timestamp"] <= ts)]
        else:
            window = updates[updates["update_timestamp"] <= ts]
        for _, row in window.iterrows():
            price = _to_float(row.get("price"))
            size = _to_float(row.get("size"))
            side = str(row.get("side", "")).lower()
            if price is None or size is None or side not in {"bid", "ask"}:
                continue
            book.update(side=side, price=price, size=size, timestamp=row["update_timestamp"].isoformat(), check_cross=False)

    return book


def book_to_frame(book: OrderBook) -> pd.DataFrame:
    rows = []
    for price, size in book.bids.items():
        rows.append({"price": price, "size": size, "side": "bid", "signed_size": -size})
    for price, size in book.asks.items():
        rows.append({"price": price, "size": size, "side": "ask", "signed_size": size})
    return pd.DataFrame(rows)


def _with_formatted_ts(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Return a copy with timestamp columns formatted in place for display precision."""
    if df.empty:
        return df
    formatted = df.copy()
    for col in columns:
        if col in formatted.columns:
            formatted[col] = pd.to_datetime(formatted[col], utc=True, errors="coerce").dt.strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            )
    if "book" in formatted.columns:
        formatted["book"] = formatted["book"].apply(_format_book_for_display)
    return formatted


def _format_book_for_display(book: Any, *, max_levels: int = 5) -> str:
    """Compact string representation of book levels for dataframe display."""
    if isinstance(book, str):
        try:
            parsed = json.loads(book)
            book = parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return book
    levels = list(book) if isinstance(book, (list, tuple)) else []
    ## invert the order of book levels for display (so best prices appear first)
    levels = levels[::-1]
    if not levels:
        return ""
    parts = []
    for level in levels[:max_levels]:
        price = _to_float(level.get("price"))
        size = _to_float(level.get("size"))
        if price is None or size is None:
            continue
        parts.append(f"{price:.4f}@{size:g}")
    if len(levels) > max_levels:
        parts.append(f"...(+{len(levels) - max_levels} more)")
    return " | ".join(parts)


def main() -> None:
    fetcher = get_fetcher()
    if fetcher is None:
        st.error("Install `clickhouse-connect` and verify ClickHouse connection settings.")
        st.stop()

    with st.sidebar:
        series_id = st.text_input("Series ID", value="10192", help="Series id to expand into tokens")
        exclude_guard = st.checkbox("Exclude 0.99 / 0.01", value=True)

    tokens_df = fetch_tokens_for_series(series_id) if series_id else pd.DataFrame()
    if tokens_df.empty:
        st.info("Enter a valid series id to load markets and tokens.")
        return

    with st.sidebar.expander("Tokens"):
        st.dataframe(tokens_df[["event_title", "event_slug", "market_slug", "token_id", "outcome"]], height=240, use_container_width=True)

    tokens_df = tokens_df.fillna("")
    tokens_df["label"] = tokens_df.apply(
        lambda row: f"{row.get('event_title') or row.get('event_slug')} | {row.get('market_slug')} | {row.get('outcome')} ({row.get('token_id')})",
        axis=1,
    )
    token_options = tokens_df.to_dict("records")
    selected_token = st.sidebar.selectbox("Token", options=token_options, format_func=lambda r: r["label"] if isinstance(r, dict) else str(r))
    token_id = selected_token["token_id"] if isinstance(selected_token, dict) else None

    if not token_id:
        st.info("Select a token to load order book data.")
        return

    min_ts, max_ts = fetch_time_bounds(token_id)
    if max_ts is None:
        st.warning(f"No snapshots or updates found for token id '{token_id}'.")
        return

    end_time = max_ts
    start_time = min_ts if min_ts else end_time

    st.caption(f"Data available from {min_ts} to {max_ts} (UTC)")

    excluded_prices = EXCLUDED_PRICES if exclude_guard else None
    snapshots = fetch_snapshots(token_id, start_time, end_time, None, excluded_prices=excluded_prices)
    updates = fetch_updates(token_id, start_time, end_time, None, excluded_prices=excluded_prices)

    if snapshots.empty and updates.empty:
        st.warning("No data in the selected window. Expand the lookback or verify the token id.")
        return

    st.subheader("Book playback")
    timeline_series = []
    if not snapshots.empty:
        timeline_series.append(snapshots["snapshot_timestamp"])
    if not updates.empty:
        timeline_series.append(updates["update_timestamp"])
    if timeline_series:
        all_times = pd.concat(timeline_series).dropna().drop_duplicates().sort_values()
        slider_options = all_times.tolist()
    else:
        slider_options = [start_time, end_time]

    def _fmt_ts(ts: datetime) -> str:
        return ts.strftime("%Y-%m-%d %H:%M:%S.%f")

    mid_series_df = _top_mid_series(snapshots)
    book_time = st.select_slider(
        "Book timestamp (UTC)",
        options=slider_options,
        value=slider_options[-1],
        format_func=_fmt_ts,
    )

    st.write(f"Selected timestamp: {_fmt_ts(book_time)}")
    book = build_book_at(book_time, snapshots, updates)
    plot_df = book_to_frame(book)
    mid_lookup = {row["timestamp"]: row["mid"] for _, row in mid_series_df.iterrows()}
    book_time_floor = pd.to_datetime(book_time, utc=True).floor("S")
    mid_price = mid_lookup.get(book_time_floor)
    if mid_price is None and (book.bids and book.asks):
        mid_price = book.mid

    metrics = st.columns(3)
    best_bid = book.best_bid if book.bids else None
    best_ask = book.best_ask if book.asks else None
    with metrics[0]:
        st.metric("Best Bid", f"{best_bid:.4f}" if best_bid is not None else "—")
    with metrics[1]:
        st.metric("Best Ask", f"{best_ask:.4f}" if best_ask is not None else "—")
    with metrics[2]:
        spread = book.spread if (book.bids and book.asks) else None
        st.metric("Spread", f"{spread:.4f}" if spread is not None else "—")

    # Mid-price over time with highlight window (first chart)
    if not mid_series_df.empty:
    
        mid_df = mid_series_df.dropna()
        highlight_ts = pd.to_datetime(book_time, utc=True).floor("S")
        window_start = highlight_ts - pd.Timedelta(seconds=5)
        window_end = highlight_ts + pd.Timedelta(seconds=5)
        mid_line = (
            alt.Chart(mid_df)
            .mark_line()
            .encode(
                x=alt.X("timestamp:T", title="Time"),
                y=alt.Y("mid:Q", title="Mid Price"),
                tooltip=["timestamp:T", "mid:Q"],
            )
        )
        highlight_point = (
            alt.Chart(pd.DataFrame({"timestamp": [highlight_ts], "mid": [mid_price]}))
            .mark_circle(color="red", size=80)
            .encode(x="timestamp:T", y="mid:Q")
        )
        window_band = (
            alt.Chart(pd.DataFrame({"start": [window_start], "end": [window_end]}))
            .mark_rect(opacity=0.1, color="gray")
            .encode(x="start:T", x2="end:T")
        )
        mid_chart = (window_band + mid_line + highlight_point).properties(height=300, title="Mid Price Over Time")
        st.altair_chart(mid_chart, use_container_width=True)
    # Depth and cumulative charts
    if plot_df.empty:
        st.info("No order book levels to plot at this timestamp.")
    else:
    
        plot_df["size_abs"] = plot_df["size"].abs()

        chart = (
            alt.Chart(plot_df)
            .mark_bar()
            .encode(
                x=alt.X("price:Q", title="Price"),
                y=alt.Y("size_abs:Q", title="Size"),
                color=alt.Color("side:N", scale=alt.Scale(domain=["bid", "ask"], range=["#2c7be5", "#d9534f"])),
                tooltip=["side", "price", "size"],
            )
            .properties(height=420)
        )

        overlays = []
        if best_bid is not None:
            overlays.append(
                alt.Chart(pd.DataFrame({"price": [best_bid], "label": ["Best Bid"]}))
                .mark_rule(color="#2c7be5", strokeDash=[4, 2])
                .encode(x="price:Q")
            )
        if best_ask is not None:
            overlays.append(
                alt.Chart(pd.DataFrame({"price": [best_ask], "label": ["Best Ask"]}))
                .mark_rule(color="#d9534f", strokeDash=[4, 2])
                .encode(x="price:Q")
            )
        if mid_price is not None:
            overlays.append(
                alt.Chart(pd.DataFrame({"price": [mid_price], "label": ["Mid"]}))
                .mark_rule(color="#6c757d")
                .encode(x="price:Q")
            )

        for overlay in overlays:
            chart = chart + overlay
        st.altair_chart(chart, use_container_width=True)
        # Cumulative depth around mid to gauge imbalance
        if mid_price is not None and not pd.isna(mid_price):
            bids_sorted = plot_df[plot_df["side"] == "bid"].sort_values("price", ascending=False).copy()
            asks_sorted = plot_df[plot_df["side"] == "ask"].sort_values("price", ascending=True).copy()
            bids_sorted["cum_size"] = bids_sorted["size_abs"].cumsum()
            asks_sorted["cum_size"] = asks_sorted["size_abs"].cumsum()
            cum_df = pd.concat([bids_sorted, asks_sorted], ignore_index=True)
            cum_chart = (
                alt.Chart(cum_df)
                .mark_area(opacity=0.4)
                .encode(
                    x=alt.X("price:Q", title="Price"),
                    y=alt.Y("cum_size:Q", title="Cumulative Size"),
                    color=alt.Color("side:N", scale=alt.Scale(domain=["bid", "ask"], range=["#2c7be5", "#d9534f"])),
                    tooltip=["side", "price", "cum_size"],
                )
                .properties(height=260, title="Cumulative Depth (centered at mid)")
            )
            mid_rule = (
                alt.Chart(pd.DataFrame({"price": [mid_price]}))
                .mark_rule(color="#6c757d", strokeDash=[4, 2])
                .encode(x="price:Q")
            )
            st.altair_chart(cum_chart + mid_rule, use_container_width=True)
    st.subheader("Loaded data")
    stats = f"{len(snapshots)} snapshots, {len(updates)} updates between {start_time} and {end_time}."
    st.caption(stats)

    with st.expander("Raw snapshots"):
        display_snapshots = _with_formatted_ts(snapshots, ["snapshot_timestamp"])
        st.dataframe(display_snapshots, use_container_width=True)
    with st.expander("Raw updates"):
        display_updates = _with_formatted_ts(updates, ["update_timestamp"])
        st.dataframe(display_updates, use_container_width=True)


if __name__ == "__main__":
    main()
