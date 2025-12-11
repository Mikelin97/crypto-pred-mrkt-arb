from __future__ import annotations

import json
import sys
import time
from collections import deque
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


PLAYBACK_GRANULARITY = {
    "By tick": "tick",
    "1 second buckets": "1s",
    "5 second buckets": "5s",
}




EXCLUDED_PRICES = {0.99, 0.01}
CHAINLINK_BTC_SYMBOL = "btc/usd"


st.set_page_config(page_title="Order Book Visualizer", layout="wide")
st.title("Order Book Visualizer")


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
def fetch_trades(token_id: str, start: datetime, end: datetime, limit: int | None = None) -> pd.DataFrame:
    fetcher = get_fetcher()
    if fetcher is None:
        return pd.DataFrame()
    df = fetcher.fetch_trades(token_id, start, end, limit)
    return df


@st.cache_data(show_spinner=False)
def fetch_chainlink_prices(symbol: str, start: datetime, end: datetime, limit: int | None = None) -> pd.DataFrame:
    fetcher = get_fetcher()
    if fetcher is None:
        return pd.DataFrame()
    return fetcher.fetch_chainlink_prices(symbol, start, end, limit)


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
        ts_floor = pd.to_datetime(ts, utc=True).floor("s")
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
        mid = round((bid_p * ask_s + ask_p * bid_s) / (bid_s + ask_s), 4)
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


def _fifo_pnl(trades: pd.DataFrame, mark_price: float | None) -> tuple[float, float | pd.NA, float]:
    """FIFO realized PnL plus mark-to-mid for remaining inventory; returns (realized, total_pnl, holding)."""
    if trades.empty:
        total = 0.0 if mark_price is not None else pd.NA
        return 0.0, total, 0.0
    queue: deque[tuple[float, float]] = deque()  # (qty, price), qty>0 long, qty<0 short
    realized = 0.0
    for _, row in trades.iterrows():
        try:
            qty = float(row["size"])
            price = float(row["price"])
        except (TypeError, ValueError):
            continue
        side = str(row["side"]).lower()
        if side not in {"buy", "sell"} or qty <= 0:
            continue
        if side == "buy":
            incoming = qty
            # Offset existing shorts first
            while incoming > 0 and queue and queue[0][0] < 0:
                short_qty, short_price = queue[0]
                offset = min(incoming, -short_qty)
                realized += (short_price - price) * offset  # profit if covering below short price
                short_qty += offset  # less negative
                incoming -= offset
                if abs(short_qty) < 1e-12:
                    queue.popleft()
                else:
                    queue[0] = (short_qty, short_price)
            if incoming > 1e-12:
                queue.append((incoming, price))
        else:  # sell
            incoming = qty
            while incoming > 0 and queue and queue[0][0] > 0:
                long_qty, long_price = queue[0]
                offset = min(incoming, long_qty)
                realized += (price - long_price) * offset
                long_qty -= offset
                incoming -= offset
                if long_qty <= 1e-12:
                    queue.popleft()
                else:
                    queue[0] = (long_qty, long_price)
            if incoming > 1e-12:
                queue.append((-incoming, price))  # new short

    holding = sum(q for q, _ in queue)
    if mark_price is None or pd.isna(mark_price):
        # If no mark and still holding, total PnL is unknown; realized is always known.
        if abs(holding) > 1e-12:
            return realized, pd.NA, holding
        return realized, realized, holding

    mark_component = sum((mark_price - price) * qty for qty, price in queue)
    total = realized + mark_component
    return realized, total, holding


def _user_trade_summary(trades: pd.DataFrame, mark_price: float | None) -> pd.DataFrame:
    """Aggregate trades by user, compute FIFO realized/total PnL and end-of-window holdings."""
    if trades.empty:
        return pd.DataFrame(columns=["user_id", "proxy_wallet", "name", "trade_count", "holding", "realized_pnl", "pnl"])
    df = trades.dropna(subset=["trade_timestamp", "side", "size", "price"]).copy()
    df["size"] = pd.to_numeric(df["size"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["size", "price"])
    df["display_name"] = df.get("name")
    if "pseudonym" in df.columns:
        df["display_name"] = df["display_name"].fillna(df["pseudonym"])
    df["display_name"] = df["display_name"].fillna("")
    df = df.sort_values(["trade_timestamp", "transaction_hash"], na_position="last")

    rows = []
    for (user_id, proxy_wallet, display_name), group in df.groupby(["user_id", "proxy_wallet", "display_name"], dropna=False):
        trade_count = group["transaction_hash"].nunique()
        realized, total_pnl, holding = _fifo_pnl(group, mark_price)
        rows.append(
            {
                "user_id": "" if pd.isna(user_id) else str(user_id),
                "proxy_wallet": "" if pd.isna(proxy_wallet) else str(proxy_wallet),
                "name": "" if pd.isna(display_name) else str(display_name),
                "trade_count": trade_count,
                "holding": holding,
                "realized_pnl": realized,
                "pnl": total_pnl,
            }
        )
    result = pd.DataFrame(rows)
    if result.empty:
        return result
    if result["pnl"].notna().any():
        result = result.sort_values("pnl", ascending=False)
    elif result["realized_pnl"].notna().any():
        result = result.sort_values("realized_pnl", ascending=False)
    else:
        result = result.sort_values("trade_count", ascending=False)
    return result.reset_index(drop=True)


def _pnl_color(val: Any) -> str:
    try:
        v = float(val)
    except (TypeError, ValueError):
        return ""
    if v > 0:
        return "background-color: #d4edda; color: #1b4332;"
    if v < 0:
        return "background-color: #f8d7da; color: #7f1d1d;"
    return "background-color: #fdf7d9; color: #7c6f00;"


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


def _build_slider_options(timeline_series: list[pd.Series], granularity: str) -> list[datetime]:
    """Build slider positions based on chosen playback granularity."""
    if not timeline_series:
        return []
    all_times = pd.concat(timeline_series).dropna()
    if all_times.empty:
        return []
    if granularity == "tick":
        return all_times.drop_duplicates().sort_values().to_list()
    freq = "1s" if granularity == "1s" else "5s"
    bucket_starts = all_times.dt.floor(freq)
    unique_starts = bucket_starts.drop_duplicates().sort_values()
    bucket_ends = unique_starts + pd.Timedelta(freq) - pd.Timedelta(microseconds=1)
    return bucket_ends.to_list()


def _sidebar_line(label: str, value: str) -> None:
    st.sidebar.markdown(
        f"<div style='word-break:break-word; white-space:normal'><strong>{label}:</strong><br>{value}</div>",
        unsafe_allow_html=True,
    )


def _start_from_market_slug(slug: str | None) -> datetime | None:
    """Extract the market start time from a slug formatted like btc-updown-15m-<epoch>."""
    if not slug:
        return None
    try:
        epoch = int(slug.removesuffix(".jsonl").split("-")[-1])
    except (ValueError, AttributeError):
        return None
    return pd.to_datetime(epoch, unit="s", utc=True)


def main() -> None:
    fetcher = get_fetcher()
    if fetcher is None:
        st.error("Install `clickhouse-connect` and verify ClickHouse connection settings.")
        st.stop()

    with st.sidebar:
        series_id = st.text_input("Series ID", value="10192", help="Series id to expand into tokens")
        exclude_guard = st.checkbox("Exclude 0.99 / 0.01", value=True)
        granularity_label = st.radio(
            "Book playback granularity",
            list(PLAYBACK_GRANULARITY.keys()),
            index=0,
            help="Choose how the playback slider advances through the book timeline.",
        )
        playback_granularity = PLAYBACK_GRANULARITY[granularity_label]

    tokens_df = fetch_tokens_for_series(series_id) if series_id else pd.DataFrame()
    if tokens_df.empty:
        st.info("Enter a valid series id to load markets and tokens.")
        return

    with st.sidebar.expander("Tokens"):
        st.dataframe(tokens_df[["event_title", "market_slug", "token_id", "outcome", "snapshot_count", "update_count"]], height=240, width='stretch')

    tokens_df = tokens_df.fillna("")
    tokens_df = tokens_df[(tokens_df["snapshot_count"] > 0) | (tokens_df["update_count"] > 0)]
    tokens_df["label"] = tokens_df.apply(
        lambda row: f"{row.get('event_title').split('-')[1]} | {row.get('outcome')} | #snapshot: {row.get('snapshot_count')} #update: {row.get('update_count')}",
        axis=1,
    )
    market_name = tokens_df["event_title"].iloc[0].split("-")[0] if "event_title" in tokens_df.columns else "Market"
    token_options = tokens_df.to_dict("records")
    selected_token = st.sidebar.selectbox(f"{market_name}Token", options=token_options, format_func=lambda r: r["label"] if isinstance(r, dict) else str(r))
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
    slug_start_time = _start_from_market_slug(selected_token.get("market_slug"))
    strike_reference_time = slug_start_time if slug_start_time is not None else start_time
    chainlink_start_time = strike_reference_time - timedelta(minutes=5)

    st.caption(f"Data available from {min_ts} to {max_ts} (UTC)")

    excluded_prices = EXCLUDED_PRICES if exclude_guard else None
    snapshots = fetch_snapshots(token_id, start_time, end_time, None, excluded_prices=excluded_prices)
    updates = fetch_updates(token_id, start_time, end_time, None, excluded_prices=excluded_prices)
    trades_df = fetch_trades(token_id, start_time, end_time, None)
    chainlink_prices_df = fetch_chainlink_prices(CHAINLINK_BTC_SYMBOL, chainlink_start_time, end_time)

    if snapshots.empty and updates.empty:
        st.warning("No data in the selected window. Expand the lookback or verify the token id.")
        return

    timeline_series = []
    if not snapshots.empty:
        timeline_series.append(snapshots["snapshot_timestamp"])
    if not updates.empty:
        timeline_series.append(updates["update_timestamp"])
    if not trades_df.empty and "trade_timestamp" in trades_df.columns:
        timeline_series.append(trades_df["trade_timestamp"])
    if timeline_series:
        slider_options = _build_slider_options(timeline_series, playback_granularity)
        if not slider_options:
            slider_options = [start_time, end_time]
    else:
        slider_options = [start_time, end_time]

    def _fmt_ts(ts: datetime) -> str:
        return ts.strftime("%Y-%m-%d %H:%M:%S.%f")

    mid_series_df = _top_mid_series(snapshots)
    book_time = st.sidebar.select_slider(
        "Book timestamp (UTC)",
        options=slider_options,
        value=slider_options[-1],
        format_func=_fmt_ts,
    )
    with st.sidebar:
        _sidebar_line("Event", selected_token["event_title"])
        _sidebar_line("Market Slug", selected_token["market_slug"])
        _sidebar_line("Token Id", token_id)
        _sidebar_line("Outcome", selected_token["outcome"])
    highlight_ts = pd.to_datetime(book_time, utc=True).floor("s")
    window_start = highlight_ts - pd.Timedelta(seconds=5)
    window_end = highlight_ts + pd.Timedelta(seconds=5)
    highlight_window = (
        alt.Chart(pd.DataFrame({"start": [window_start], "end": [window_end]}))
        .mark_rect(opacity=0.1, color="gray")
        .encode(x="start:T", x2="end:T")
    )

    book = build_book_at(book_time, snapshots, updates)
    plot_df = book_to_frame(book)
    mid_price = None
    if book.bids and book.asks:
        mid_price = book.vmid

    if mid_price is None:
        mid_lookup = {row["timestamp"]: row["mid"] for _, row in mid_series_df.iterrows()}
        mid_price = mid_lookup.get(highlight_ts)

    mark_price_for_pnl: float | None = mid_price
    strike_price_value: float | None = None
    resolve_price_value: float | None = None

    # Chainlink BTC/USD price with the same highlight window
    if not chainlink_prices_df.empty:
        cl_df = (
            chainlink_prices_df.rename(columns={"update_timestamp": "timestamp", "value": "price"})
            .dropna(subset=["price"])
            .sort_values("timestamp")
        )
        price_at_ts = None
        upto_highlight = cl_df[cl_df["timestamp"] <= highlight_ts]
        if not upto_highlight.empty:
            price_at_ts = upto_highlight.iloc[-1]["price"]

        strike_ts = pd.to_datetime(strike_reference_time, utc=True)
        strike_row = None
        strike_after = cl_df[cl_df["timestamp"] >= strike_ts]
        if not strike_after.empty:
            strike_row = strike_after.iloc[0]
        else:
            strike_before = cl_df[cl_df["timestamp"] <= strike_ts]
            if not strike_before.empty:
                strike_row = strike_before.iloc[-1]
        strike_price_value = strike_row["price"] if strike_row is not None else None
        strike_price_ts = strike_row["timestamp"] if strike_row is not None else strike_ts
        resolve_ts = strike_ts + pd.Timedelta(minutes=15)
        resolve_row = None
        resolve_after = cl_df[cl_df["timestamp"] >= resolve_ts]
        if not resolve_after.empty:
            resolve_row = resolve_after.iloc[0]
        else:
            resolve_before = cl_df[cl_df["timestamp"] <= resolve_ts]
            if not resolve_before.empty:
                resolve_row = resolve_before.iloc[-1]
        resolve_price_value = resolve_row["price"] if resolve_row is not None else None
        resolve_price_ts = resolve_row["timestamp"] if resolve_row is not None else resolve_ts

        price_min = cl_df["price"].min()
        price_max = cl_df["price"].max()
        price_span = price_max - price_min
        pad = max(price_span * 0.1, max(abs(price_max), 1) * 0.0005)
        y_domain = [price_min - pad, price_max + pad]

        warmup_band = (
            alt.Chart(pd.DataFrame({"start": [chainlink_start_time], "end": [strike_reference_time]}))
            .mark_rect(opacity=0.1, color="#b0c4de")
            .encode(x="start:T", x2="end:T")
        )
        resolve_band_start = resolve_price_ts if resolve_price_ts is not None else resolve_ts
        post_resolve_band = (
            alt.Chart(pd.DataFrame({"start": [resolve_band_start], "end": [end_time]}))
            .mark_rect(opacity=0.1, color="#f4d8cd")
            .encode(x="start:T", x2="end:T")
        )

        strike_text = (
            f"{strike_price_value:.2f}"
            if strike_price_value is not None and not pd.isna(strike_price_value)
            else "—"
        )
        highlight_text = f"{price_at_ts:.2f}" if price_at_ts is not None and not pd.isna(price_at_ts) else "—"
        delta_price = None
        if (
            strike_price_value is not None
            and price_at_ts is not None
            and not pd.isna(strike_price_value)
            and not pd.isna(price_at_ts)
        ):
            delta_price = price_at_ts - strike_price_value
        strike_label_ts = None
        try:
            strike_label_ts = (
                strike_price_ts.tz_convert("UTC") if hasattr(strike_price_ts, "tz_convert") else strike_price_ts
            )
        except Exception:
            strike_label_ts = strike_price_ts
        strike_label = "Strike price"
        if strike_label_ts is not None and not pd.isna(strike_label_ts):
            strike_label = f"Strike price ({strike_label_ts.strftime('%Y-%m-%d %H:%M:%S %Z')})"
        resolve_text = (
            f"{resolve_price_value:.2f}"
            if resolve_price_value is not None and not pd.isna(resolve_price_value)
            else "—"
        )
        resolve_label_ts = None
        try:
            resolve_label_ts = (
                resolve_price_ts.tz_convert("UTC") if hasattr(resolve_price_ts, "tz_convert") else resolve_price_ts
            )
        except Exception:
            resolve_label_ts = resolve_price_ts
        resolve_label = "Resolve price"
        if resolve_label_ts is not None and not pd.isna(resolve_label_ts):
            resolve_label = f"Resolve price ({resolve_label_ts.strftime('%Y-%m-%d %H:%M:%S %Z')})"
        price_summary = st.columns(3)
        with price_summary[0]:
            st.metric(strike_label, strike_text)
        with price_summary[1]:
            st.metric(
                resolve_label,
                resolve_text,
                delta=(
                    None
                    if resolve_price_value is None or strike_price_value is None
                    else f"{resolve_price_value - strike_price_value:+.2f}"
                ),
            )
        with price_summary[2]:
            st.metric("Price at selection", highlight_text, delta=None if delta_price is None else f"{delta_price:+.2f}")

        chainlink_line = (
            alt.Chart(cl_df)
            .mark_line(color="#f4a261")
            .encode(
                x=alt.X("timestamp:T", title="Time"),
                y=alt.Y("price:Q", title="Chainlink BTC/USD Price", scale=alt.Scale(domain=y_domain, zero=False)),
                tooltip=["timestamp:T", "price:Q"],
            )
        )
        overlays = warmup_band + post_resolve_band + highlight_window + chainlink_line
        if strike_price_value is not None:
            strike_rule = (
                alt.Chart(pd.DataFrame({"price": [strike_price_value]}))
                .mark_rule(color="#264653", strokeDash=[6, 3])
                .encode(y="price:Q")
            )
            overlays = overlays + strike_rule
            overlays = overlays + (
                alt.Chart(pd.DataFrame({"timestamp": [strike_price_ts], "price": [strike_price_value]}))
                .mark_circle(color="#264653", size=70)
                .encode(x="timestamp:T", y="price:Q")
            )
        if resolve_price_value is not None:
            overlays = overlays + (
                alt.Chart(pd.DataFrame({"timestamp": [resolve_price_ts], "price": [resolve_price_value]}))
                .mark_circle(color="#2a9d8f", size=70)
                .encode(x="timestamp:T", y="price:Q")
            )
        if price_at_ts is not None:
            overlays = overlays + (
                alt.Chart(pd.DataFrame({"timestamp": [highlight_ts], "price": [price_at_ts]}))
                .mark_circle(color="#e76f51", size=80)
                .encode(x="timestamp:T", y="price:Q")
            )
        chainlink_chart = overlays.properties(height=300, title="Chainlink BTC/USD Price (start extended +5m)")
        st.altair_chart(chainlink_chart, use_container_width=True)
    else:
        st.info("No Chainlink BTC/USD price data available in this window.")

    # Determine mark price for PnL using resolve vs strike and token outcome.
    outcome = str(selected_token.get("outcome", "")).lower() if isinstance(selected_token, dict) else ""
    if strike_price_value is not None and resolve_price_value is not None:
        resolved_up = resolve_price_value > strike_price_value
        if "up" in outcome:
            mark_price_for_pnl = 1.0 if resolved_up else 0.0
        elif "down" in outcome:
            mark_price_for_pnl = 0.0 if resolved_up else 1.0

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
        mid_line = (
            alt.Chart(mid_df)
            .mark_line()
            .encode(
                x=alt.X("timestamp:T", title="Time"),
                y=alt.Y("mid:Q", title="Mid Price"),
                tooltip=["timestamp:T", "mid:Q"],
            )
        )
        highlight_point = None
        if mid_price is not None:
            highlight_point = (
                alt.Chart(pd.DataFrame({"timestamp": [highlight_ts], "mid": [mid_price]}))
                .mark_circle(color="red", size=80)
                .encode(x="timestamp:T", y="mid:Q")
            )

        trades_plot = None
        if not trades_df.empty:
            trades_plot_df = trades_df.rename(columns={"trade_timestamp": "timestamp"}).copy()
            trades_plot_df["abs_size"] = trades_plot_df["size"].abs()
            trades_plot = (
                alt.Chart(trades_plot_df)
                .mark_circle(opacity=0.7)
                .encode(
                    x=alt.X("timestamp:T", title="Time"),
                    y=alt.Y("price:Q", title="Price"),
                    color=alt.Color(
                        "side:N",
                        scale=alt.Scale(domain=["buy", "sell"], range=["#2ecc71", "#e74c3c"]),
                        title="Side",
                    ),
                    size=alt.Size(
                        "abs_size:Q",
                        scale=alt.Scale(range=[40, 400]),
                        title="Size",
                    ),
                    tooltip=[
                        alt.Tooltip("timestamp:T", title="Time"),
                        alt.Tooltip("price:Q", title="Price"),
                        alt.Tooltip("size:Q", title="Size"),
                        alt.Tooltip("side:N", title="Side"),
                        alt.Tooltip("transaction_hash:N", title="Tx Hash"),
                    ],
                )
            )

        mid_chart = highlight_window + mid_line
        if highlight_point is not None:
            mid_chart = mid_chart + highlight_point
        if trades_plot is not None:
            mid_chart = mid_chart + trades_plot
        mid_chart = mid_chart.properties(height=300, title="Mid Price (Order Book Snapshot Only) Over Time")
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
    st.subheader("Trader summary")
    if trades_df.empty:
        st.info("No trades available for this token in the selected window.")
    else:
        summary_df = _user_trade_summary(trades_df, mark_price_for_pnl)
        if summary_df.empty:
            st.info("No trader summary to display.")
        else:
            formatters: dict[str, Any] = {
                "trade_count": "{:.0f}".format,
                "holding": (lambda x: f"{x:.4f}"),
                "realized_pnl": (lambda x: f"{x:.4f}"),
                "pnl": (lambda x: "—" if pd.isna(x) else f"{x:.4f}"),
            }
            styled = summary_df.style.format(formatters).applymap(_pnl_color, subset=["pnl"])
            st.dataframe(styled, width='stretch')

    st.subheader("Loaded data")
    stats = f"{len(snapshots)} snapshots, {len(updates)} updates, {len(trades_df)} trades between {start_time} and {end_time}."
    st.caption(stats)

    with st.expander("Raw snapshots"):
        display_snapshots = _with_formatted_ts(snapshots, ["snapshot_timestamp"])
        st.dataframe(display_snapshots, width='stretch')
    with st.expander("Raw updates"):
        display_updates = _with_formatted_ts(updates, ["update_timestamp"])
        st.dataframe(display_updates, width='stretch')
    with st.expander("Raw trades"):
        display_trades = _with_formatted_ts(trades_df, ["trade_timestamp"])
        st.dataframe(display_trades, width='stretch')


if __name__ == "__main__":
    main()
