from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Tuple

import pandas as pd
import streamlit as st


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
def fetch_snapshots(token_id: str, start: datetime, end: datetime, limit: int) -> pd.DataFrame:
    fetcher = get_fetcher()
    if fetcher is None:
        return pd.DataFrame()
    df = fetcher.fetch_snapshots(token_id, start, end, limit)
    if not df.empty and "snapshot_timestamp" in df:
        df["snapshot_timestamp"] = pd.to_datetime(df["snapshot_timestamp"], utc=True, format="%Y-%m-%d %H:%M:%S.%f")
    return df


@st.cache_data(show_spinner=False)
def fetch_updates(token_id: str, start: datetime, end: datetime, limit: int) -> pd.DataFrame:
    fetcher = get_fetcher()
    if fetcher is None:
        return pd.DataFrame()
    df = fetcher.fetch_updates(token_id, start, end, limit, excluded_prices=EXCLUDED_PRICES)
    return df


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
        token_id = st.text_input("Token ID", help="CLOB token id to inspect")
        lookback_minutes = st.slider("Lookback window (minutes)", min_value=1, max_value=720, value=60, step=1)
        snapshot_limit = st.number_input("Snapshot rows", min_value=10, max_value=5000, value=500, step=10)
        update_limit = st.number_input("Update rows", min_value=100, max_value=50000, value=5000, step=100)

    if not token_id:
        st.info("Enter a token id to load order book data.")
        return

    min_ts, max_ts = fetch_time_bounds(token_id)
    if max_ts is None:
        st.warning(f"No snapshots or updates found for token id '{token_id}'.")
        return

    end_time = max_ts
    default_start = end_time - timedelta(minutes=lookback_minutes)
    start_time = max(default_start, min_ts) if min_ts else default_start

    st.caption(f"Data available from {min_ts} to {max_ts} (UTC)")

    snapshots = fetch_snapshots(token_id, start_time, end_time, int(snapshot_limit))
    updates = fetch_updates(token_id, start_time, end_time, int(update_limit))

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

    book_time = st.select_slider(
        "Book timestamp (UTC)",
        options=slider_options,
        value=slider_options[-1],
        format_func=_fmt_ts,
    )

    book = build_book_at(book_time, snapshots, updates)
    plot_df = book_to_frame(book)

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

    if plot_df.empty:
        st.info("No order book levels to plot at this timestamp.")
    else:
        try:
            import altair as alt
        except ImportError:
            st.error("Install `altair` to render the depth chart.")
        else:
            plot_df["size_abs"] = plot_df["size"].abs()
            mid_price = book.mid if (book.bids and book.asks) else None

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
    st.write(plot_df)
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
