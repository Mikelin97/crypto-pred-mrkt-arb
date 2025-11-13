from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Dict

import boto3
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

FILE_DIR = Path(__file__).resolve().parent
REPO_ROOT = FILE_DIR if (FILE_DIR / "backtest.py").exists() else FILE_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from backtest import backtest_market
from shared.market import Market
from shared.strategies.data_track import DataTrackStrategy

st.set_page_config(page_title="Poly Punt Backtester", layout="wide")

DATA_ROOT = FILE_DIR / "data"
PRICE_DIR = DATA_ROOT / "prices"
PRICE_DIR.mkdir(parents=True, exist_ok=True)

PRICE_PREFIXES: Dict[str, str] = {
    "Chainlink": "chainlink_crypto_prices",
    "Binance": "binance_crypto_prices",
}

s3_client = boto3.client("s3")
PRICE_LOOKUP: Dict[str, Dict[str, str]] = {}


def _minutes_from_token(token: str) -> int:
    if token.endswith("m") and token[:-1].isdigit():
        return int(token[:-1])
    if token.endswith("h") and token[:-1].isdigit():
        return int(token[:-1]) * 60
    return 15


def _precision_multiplier(time_precision: int) -> int:
    return 10 ** max(time_precision - 10, 0)


def _parse_slug(slug: str, time_precision: int) -> Dict[str, int | datetime]:
    normalized = slug.removesuffix(".jsonl")
    parts = normalized.split("-")
    if len(parts) < 2:
        raise ValueError(f"Cannot parse slug '{slug}'.")

    epoch_seconds = int(parts[-1])
    window_token = next(
        (token for token in reversed(parts[:-1]) if token[-1] in {"m", "h"}), "15m"
    )
    window_minutes = _minutes_from_token(window_token)
    multiplier = _precision_multiplier(time_precision)

    unix_start = epoch_seconds * multiplier
    unix_end = unix_start + window_minutes * 60 * multiplier
    start_dt = datetime.fromtimestamp(unix_start / multiplier, UTC)

    return {
        "window_minutes": window_minutes,
        "start_ms": unix_start,
        "end_ms": unix_end,
        "start_dt": start_dt,
    }


def _get_price_lookup(source: str) -> Dict[str, str]:
    if source in PRICE_LOOKUP:
        return PRICE_LOOKUP[source]

    prefix = PRICE_PREFIXES[source]
    lookup: Dict[str, str] = {}
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=Market.bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            filename = key.split("/")[-1]
            lookup[filename] = key

    if not lookup:
        raise ValueError(f"No price files found for prefix '{prefix}'.")

    PRICE_LOOKUP[source] = lookup
    return lookup


def _download_price_file(date_str: str, source: str) -> Path:
    lookup = _get_price_lookup(source)
    candidates = sorted(name for name in lookup if date_str in name)
    if not candidates:
        PRICE_LOOKUP.pop(source, None)
        raise ValueError(f"No {source} price file found for {date_str}.")

    filename = candidates[-1]
    destination = PRICE_DIR / filename
    if destination.exists():
        return destination

    s3_client.download_file(Market.bucket, lookup[filename], str(destination))
    return destination


def _path_components(path: Path, *, keep_suffix: bool) -> tuple[str, str]:
    parent = str(path.parent)
    if not parent.endswith("/"):
        parent += "/"
    name = path.name if keep_suffix else path.stem
    return parent, name


def _to_datetime_series(timestamps: pd.Series, multiplier: int) -> pd.Series:
    return pd.to_datetime(timestamps / multiplier, unit="s", utc=True)


def _run_backtest(
    *,
    market: Market,
    slug: str,
    crypto_symbol: str,
    warm_up_minutes: int,
    strategy_kwargs: dict,
    price_source: str,
    time_precision: int,
):
    slug_meta = _parse_slug(slug, time_precision)
    multiplier = _precision_multiplier(time_precision)
    warm_up_duration = warm_up_minutes * 60 * multiplier

    contract_path = market.get_slug(slug, download=True)
    price_date = slug_meta["start_dt"].date().isoformat()
    price_path = _download_price_file(price_date, price_source)

    crypto_dir, crypto_filename = _path_components(price_path, keep_suffix=True)
    market_dir, market_slug = _path_components(contract_path, keep_suffix=False)

    strategy = DataTrackStrategy(
        slug_meta["start_ms"],
        slug_meta["end_ms"],
        time_precision,
        **strategy_kwargs,
    )
    print("Starting backtest...")
    print(f"Crypto Dir: {crypto_dir}, Filename: {crypto_filename}")
    print(f"Market Dir: {market_dir}, Slug: {market_slug}")
    warm_up, backtest = backtest_market(
        crypto_dir,
        crypto_filename,
        crypto_symbol,
        market_dir,
        market_slug,
        slug_meta["start_ms"],
        slug_meta["end_ms"],
        warm_up_duration,
        strategy,
        time_precision=time_precision,
    )

    return {
        "warm_up": pd.DataFrame(warm_up),
        "backtest": pd.DataFrame(backtest),
        "meta": slug_meta,
        "price_path": price_path,
        "contract_path": contract_path,
        "multiplier": multiplier,
    }


st.title("Poly Punt Theoretical Price Backtester")
st.caption("Select a Polymarket slug, fetch the data, and compare theoretical vs mid prices.")

with st.sidebar:
    asset_name = st.text_input("Asset (e.g. btc, eth)", value="btc").strip().lower()
    price_source = st.selectbox("Price feed", list(PRICE_PREFIXES.keys()), index=0)
    time_precision = st.number_input(
        "Time precision",
        min_value=10,
        max_value=16,
        value=13,
        step=1,
    )
    warm_up_minutes = st.slider("Warm-up window (minutes)", 1, 60, 5)
    effective_memory = st.slider("Vol effective memory", 60, 5000, 300, step=60)
    risk_free_rate = st.number_input(
        "Risk-free rate",
        min_value=0.0,
        max_value=0.2,
        value=0.042,
        step=0.001,
        format="%.3f",
    )
    if st.button("Refresh price files cache"):
        PRICE_LOOKUP.pop(price_source, None)

results_asset = st.session_state.get("results_asset")
if results_asset != asset_name:
    st.session_state["results_asset"] = asset_name
    st.session_state.pop("backtest_results", None)

market = None
slug_df = pd.DataFrame()
if asset_name:
    refresh = st.sidebar.button("Refresh slugs")
    cached_asset = st.session_state.get("market_asset")
    market_obj = st.session_state.get("market")
    need_refresh = refresh or market_obj is None or cached_asset != asset_name
    if need_refresh:
        try:
            market = Market(asset_name, data_root=DATA_ROOT)
        except NoCredentialsError:
            st.error("AWS credentials not configured. Please set them to browse markets.")
        except (ClientError, BotoCoreError) as exc:
            st.error(f"Unable to load market '{asset_name}': {exc}")
        else:
            st.session_state["market"] = market
            st.session_state["market_asset"] = asset_name
            slug_df = market.get_available_slugs()
    else:
        market = market_obj
        slug_df = market.get_available_slugs()

if market and slug_df.empty:
    st.info(f"No snapshots available for '{asset_name}'.")

selected_slug = None
if market and not slug_df.empty:
    st.subheader(f"Available contracts for {market.name.upper()}")
    display_cols = [col for col in ["slug", "size_mb", "last_modified"] if col in slug_df.columns]
    df_to_show = slug_df[display_cols] if display_cols else slug_df
    st.dataframe(df_to_show, width="stretch", hide_index=True)
    if "slug" in slug_df.columns:
        selected_slug = st.selectbox("Choose a slug", slug_df["slug"])
    else:
        st.warning("Slug metadata unavailable; please refresh.")

if market and selected_slug:
    slug_info = _parse_slug(selected_slug, time_precision)
    default_symbol = f"{asset_name}/usd"
    crypto_symbol = st.text_input("Crypto symbol (matches price feed)", value=default_symbol)

    multiplier = _precision_multiplier(time_precision)
    start_dt = slug_info["start_dt"].strftime("%Y-%m-%d %H:%M")
    end_dt = datetime.fromtimestamp(slug_info["end_ms"] / multiplier, UTC).strftime(
        "%Y-%m-%d %H:%M"
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("Start (UTC)", start_dt)
    c2.metric("Window (min)", slug_info["window_minutes"])
    c3.metric("End (UTC)", end_dt)

    if st.button("Download snapshot locally"):
        try:
            path = market.get_slug(selected_slug, download=True, refresh=True)
        except (ClientError, BotoCoreError) as exc:
            st.error(f"Failed to download snapshot: {exc}")
        else:
            st.success(f"Saved to {path}")

    strategy_kwargs = {
        "effective_memory": effective_memory,
        "risk_free_rate": risk_free_rate,
    }

    if st.button("Run backtest", type="primary"):
        with st.spinner("Pulling data and running backtest..."):
            try:
                results = _run_backtest(
                    market=market,
                    slug=selected_slug,
                    crypto_symbol=crypto_symbol,
                    warm_up_minutes=warm_up_minutes,
                    strategy_kwargs=strategy_kwargs,
                    price_source=price_source,
                    time_precision=time_precision,
                )
            except NoCredentialsError:
                st.error("AWS credentials not configured. Unable to download data.")
            except (ClientError, BotoCoreError) as exc:
                st.error(f"S3 error: {exc}")
            except ValueError as exc:
                st.error(str(exc))
            except Exception as exc:  # pragma: no cover - interactive surface
                st.error(f"Backtest failed: {exc}")
            else:
                st.session_state["backtest_results"] = results
                st.success("Backtest complete.")

results = st.session_state.get("backtest_results")
if results:
    backtest_df = results["backtest"]
    meta = results["meta"]
    if backtest_df.empty:
        st.warning("No datapoints produced. Try a different window.")
    else:
        filtered = backtest_df[
            (backtest_df["timestamp"] >= meta["start_ms"])
            & (backtest_df["timestamp"] <= meta["end_ms"])
        ].copy()

        if filtered.empty:
            st.warning("No datapoints inside the requested time span.")
        else:
            filtered["timestamp_dt"] = _to_datetime_series(
                filtered["timestamp"],
                results["multiplier"],
            )

            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=filtered["timestamp_dt"],
                    y=filtered["theo_price"],
                    name="Theo",
                    mode="lines",
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=filtered["timestamp_dt"],
                    y=filtered["mid_price"],
                    name="Mid",
                    mode="lines",
                )
            )
            fig.update_layout(
                height=500,
                margin=dict(l=0, r=0, t=30, b=0),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig, width="stretch")

            st.subheader("Backtest rows")
            st.dataframe(
                filtered[
                    [
                        "timestamp_dt",
                        "crypto_price",
                        "theo_price",
                        "mid_price",
                        "best_bid",
                        "best_ask",
                        "market_spread",
                        "vol",
                    ]
                ],
                width="stretch",
                hide_index=True,
            )

            st.caption(
                f"Contract file: {results['contract_path']} • Price file: {results['price_path']}"
            )
