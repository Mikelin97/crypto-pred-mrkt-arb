from __future__ import annotations

import io
import sys
import zipfile
from pathlib import Path
from typing import Dict, Iterable, List

import boto3
import pandas as pd
import streamlit as st
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from shared.market import Market

DATA_ROOT = Path(__file__).resolve().parent / "data"
PRICE_DIR = DATA_ROOT
s3_client = boto3.client("s3")


def _build_zip_from_paths(paths: Iterable[Path]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in paths:
            archive.write(path, arcname=path.name)
    buffer.seek(0)
    return buffer.getvalue()


def _normalise_paths(value) -> List[Path]:
    if isinstance(value, Path):
        return [value]
    return list(value)


def _fetch_price_index(prefix: str) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=Market.bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            filename = key.split("/")[-1]
            rows.append(
                {
                    "filename": filename,
                    "key": key,
                    "size_mb": round(item.get("Size", 0) / (1024 * 1024), 2),
                    "last_modified": item.get("LastModified"),
                }
            )

    if not rows:
        return pd.DataFrame(columns=["filename", "key", "size_mb", "last_modified"])

    df = pd.DataFrame(rows)
    df.sort_values(by="last_modified", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def _download_price_files(selected: Iterable[str], lookup: Dict[str, str]) -> List[Path]:
    PRICE_DIR.mkdir(parents=True, exist_ok=True)
    downloaded: List[Path] = []
    for filename in selected:
        key = lookup[filename]
        destination = PRICE_DIR / filename
        s3_client.download_file(Market.bucket, key, str(destination))
        downloaded.append(destination)
    return downloaded


def _prepare_price_archive(label: str, selected: List[str], lookup: Dict[str, str]) -> None:
    try:
        paths = _download_price_files(selected, lookup)
        archive_data = _build_zip_from_paths(paths)
    except Exception as exc:
        st.error(f"Failed to prepare {label} download: {exc}")
        st.session_state.pop(f"{label}_archive", None)
        return

    st.session_state[f"{label}_archive"] = {
        "data": archive_data,
        "selection": selected.copy(),
        "file_name": f"{label}_prices.zip",
    }


def _prepare_contract_archive(market: Market, selected: List[str]) -> None:
    try:
        paths = _normalise_paths(market.get_slug(selected, download=True))
        archive_data = _build_zip_from_paths(paths)
    except Exception as exc:
        st.error(f"Failed to prepare contract download: {exc}")
        st.session_state.pop("contract_archive", None)
        return

    st.session_state["contract_archive"] = {
        "data": archive_data,
        "selection": selected.copy(),
        "asset": market.name,
        "file_name": f"{market.name.lower()}_contracts.zip",
    }


st.set_page_config(page_title="Market Data Browser", layout="wide")
st.title("Polymarket Data Explorer")

st.subheader("Contract Snapshots")
asset_name = st.text_input("Asset name", placeholder="btc, eth, sol, xrp")

market: Market | None = None
if asset_name:
    try:
        market = Market(asset_name, data_root=DATA_ROOT)
    except NoCredentialsError:
        st.error("AWS credentials not configured. Please set them before browsing markets.")
    except (ClientError, BotoCoreError) as exc:
        st.error(f"Unable to load market '{asset_name}': {exc}")

if market:
    slug_df = market.get_available_slugs()
    if slug_df.empty:
        st.info(f"No slugs available for market '{market.name}'.")
    else:
        display_columns = ["slug", "size_mb", "last_modified"]
        available_columns = [col for col in display_columns if col in slug_df.columns]
        st.dataframe(slug_df[available_columns], width="stretch")
        slugs = slug_df["slug"].tolist()
        selected_slugs = st.multiselect(
            "Select contract snapshots",
            options=slugs,
            key="selected_contract_slugs",
        )

        archive_state = st.session_state.get("contract_archive")
        if archive_state and (
            archive_state.get("asset") != market.name
            or archive_state.get("selection") != selected_slugs
        ):
            st.session_state.pop("contract_archive", None)
            archive_state = None

        cols = st.columns([1, 1])
        with cols[0]:
            if st.button("Prepare contract download", disabled=not selected_slugs):
                _prepare_contract_archive(market, selected_slugs)
                archive_state = st.session_state.get("contract_archive")

        if archive_state:
            with cols[1]:
                st.download_button(
                    "Download contracts archive",
                    data=archive_state["data"],
                    file_name=archive_state["file_name"],
                    mime="application/zip",
                    width="stretch",
                )

st.subheader("Daily Price Feeds")
price_tabs = st.tabs(["Chainlink", "Binance"])
price_prefix_map = {
    "Chainlink": "chainlink_crypto_prices",
    "Binance": "binance_crypto_prices",
}

for tab, label in zip(price_tabs, price_prefix_map):
    prefix = price_prefix_map[label]
    with tab:
        try:
            df = _fetch_price_index(prefix)
        except NoCredentialsError:
            st.error("AWS credentials not configured. Please set them before browsing prices.")
            continue
        except (ClientError, BotoCoreError) as exc:
            st.error(f"Unable to list {label} files: {exc}")
            continue

        if df.empty:
            st.info(f"No {label.lower()} price files found.")
            continue

        display_df = df.drop(columns=["key"], errors="ignore")
        st.dataframe(display_df, width="stretch")
        options = df["filename"].tolist()
        selection_key = f"{label.lower()}_selection"
        selected_files = st.multiselect(
            "Select files",
            options=options,
            key=selection_key,
        )

        lookup = dict(zip(df["filename"], df["key"]))
        archive_key = f"{label}_archive"
        archive_state = st.session_state.get(archive_key)
        if archive_state and archive_state.get("selection") != selected_files:
            st.session_state.pop(archive_key, None)
            archive_state = None

        prepare_key = f"prepare_{label.lower()}_download"
        download_key = f"download_{label.lower()}_archive"
        cols = st.columns([1, 1])
        with cols[0]:
            if st.button(
                "Prepare download",
                key=prepare_key,
                disabled=not selected_files,
            ):
                _prepare_price_archive(label, selected_files, lookup)
                archive_state = st.session_state.get(archive_key)

        if archive_state:
            with cols[1]:
                st.download_button(
                    "Download archive",
                    data=archive_state["data"],
                    file_name=archive_state["file_name"],
                    mime="application/zip",
                    width="stretch",
                    key=download_key,
                )
