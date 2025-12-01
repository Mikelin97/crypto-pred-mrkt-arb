from __future__ import annotations

import os
import json
from datetime import datetime
from typing import Any, Dict, Iterable, Optional, Sequence, Tuple

import pandas as pd


import clickhouse_connect



class Fetcher:
    """Lightweight ClickHouse helper with env-driven defaults and simple query helpers."""

    def __init__(
        self,
        *,
        host: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        secure: Optional[bool] = None,
    ) -> None:
        
        self.host = host or os.getenv("CLICKHOUSE_HOST", "localhost")
        self.port = int(port or os.getenv("CLICKHOUSE_PORT", "8123"))
        self.username = username or os.getenv("CLICKHOUSE_USER") or os.getenv("CLICKHOUSE_USERNAME") or "default"
        self.password = password if password is not None else os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")
        self.database = database or os.getenv("CLICKHOUSE_DATABASE") or os.getenv("CLICKHOUSE_DB") or "analytics"
        self.secure = (
            secure
            if secure is not None
            else os.getenv("CLICKHOUSE_SECURE", "false").lower() in {"1", "true", "yes"}
        )
        self._client = None

    def _connect(self):
        return clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
            secure=self.secure,
        )

    @property
    def client(self):
        if self._client is None:
            self._client = self._connect()
        return self._client

    def _table(self, name: str) -> str:
        if "." in name:
            return name
        return f"{self.database}.{name}" if self.database else name

    def query_df(self, sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Run a query and return a DataFrame."""
        return self.client.query_df(sql, params or {})

    def command(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a command and return the result."""
        return self.client.command(sql, params=params)

    def fetch_time_bounds(
        self,
        token_id: str,
        *,
        updates_table: str = "order_book_updates",
        snapshots_table: str = "order_book_snapshots",
    ) -> Tuple[datetime | None, datetime | None]:
        updates_table = self._table(updates_table)
        snapshots_table = self._table(snapshots_table)
        sql = f"""
            SELECT min(ts) AS min_ts, max(ts) AS max_ts
            FROM (
                SELECT min(update_timestamp) AS ts FROM {updates_table} WHERE token_id = %(token_id)s
                UNION ALL
                SELECT max(update_timestamp) AS ts FROM {updates_table} WHERE token_id = %(token_id)s
                UNION ALL
                SELECT min(snapshot_timestamp) AS ts FROM {snapshots_table} WHERE token_id = %(token_id)s
                UNION ALL
                SELECT max(snapshot_timestamp) AS ts FROM {snapshots_table} WHERE token_id = %(token_id)s
            )
        """
        df = self.query_df(sql, {"token_id": token_id})
        if df.empty:
            return None, None
        min_ts = self._to_datetime(df.iloc[0].get("min_ts"))
        max_ts = self._to_datetime(df.iloc[0].get("max_ts"))
        return min_ts, max_ts

    def fetch_snapshots(
        self,
        token_id: str,
        start: datetime,
        end: datetime,
        limit: Optional[int] = None,
        *,
        snapshots_table: str = "order_book_snapshots",
        drop_empty_books: bool = True,
        excluded_prices: Sequence[float] | None = None,
    ) -> pd.DataFrame:
        snapshots_table = self._table(snapshots_table)
        sql = f"""
            SELECT snapshot_timestamp, side, book, top_price, top_size
            FROM {snapshots_table}
            WHERE token_id = %(token_id)s
              AND snapshot_timestamp BETWEEN %(start)s AND %(end)s
            ORDER BY snapshot_timestamp ASC
        """
        params = {"token_id": token_id, "start": start, "end": end}
        if limit is not None:
            sql += "\nLIMIT %(limit)s"
            params["limit"] = limit
        df = self.query_df(sql, params)
        if df.empty:
            return df
        if "snapshot_timestamp" in df:
            df["snapshot_timestamp"] = pd.to_datetime(df["snapshot_timestamp"], utc=True)
        if "book" in df:
            df["book"] = df["book"].apply(self._parse_book_column)
            if excluded_prices:
                excluded_set = {float(x) for x in excluded_prices}
                df["book"] = df["book"].apply(
                    lambda levels: [
                        lvl for lvl in (levels or []) if not self._is_excluded_price(lvl.get("price"), excluded_set)
                    ]
                )
            if drop_empty_books:
                df = df[df["book"].apply(self._book_has_volume)]
        return df

    def fetch_tokens_for_series(
        self,
        series_id: str,
        *,
        series_table: str = "series",
        events_table: str = "events",
        markets_table: str = "markets",
        tokens_table: str = "tokens",
    ) -> pd.DataFrame:
        series_table = self._table(series_table)
        events_table = self._table(events_table)
        markets_table = self._table(markets_table)
        tokens_table = self._table(tokens_table)
        sql = f"""
            SELECT
                s.series_id,
                s.slug AS series_slug,
                e.event_id,
                e.slug AS event_slug,
                e.title AS event_title,
                m.market_id,
                m.slug AS market_slug,
                t.token_id,
                t.outcome
            FROM {series_table} s
            JOIN {events_table} e ON e.series_id = s.series_id
            JOIN {markets_table} m ON m.event_id = e.event_id
            JOIN {tokens_table} t ON t.market_id = m.market_id
            WHERE s.series_id = %(series_id)s
            ORDER BY e.title, m.market_id, t.token_id
        """
        return self.query_df(sql, {"series_id": series_id})

    def fetch_updates(
        self,
        token_id: str,
        start: datetime,
        end: datetime,
        limit: Optional[int] = None,
        *,
        updates_table: str = "order_book_updates",
        excluded_prices: Sequence[float] | None = None,
    ) -> pd.DataFrame:
        updates_table = self._table(updates_table)
        sql = f"""
            SELECT update_timestamp, side, price, size
            FROM {updates_table}
            WHERE token_id = %(token_id)s
              AND update_timestamp BETWEEN %(start)s AND %(end)s
            ORDER BY update_timestamp ASC
        """
        params = {"token_id": token_id, "start": start, "end": end}
        if limit is not None:
            sql += "\nLIMIT %(limit)s"
            params["limit"] = limit
        df = self.query_df(sql, params)
        if df.empty:
            return df
        if "update_timestamp" in df:
            df["update_timestamp"] = pd.to_datetime(df["update_timestamp"], utc=True)
        df = df.dropna(subset=["price", "size", "side"])
        df["side"] = df["side"].astype(str).str.lower()
        df = df[df["side"].isin(["bid", "ask"])]
        if excluded_prices:
            excluded_set = {float(x) for x in excluded_prices}
            df = df[~df["price"].apply(lambda x: self._is_excluded_price(x, excluded_set))]
        return df

    def fetch_tokens_for_series(
        self,
        series_id: str,
        *,
        series_table: str = "series",
        events_table: str = "events",
        markets_table: str = "markets",
        tokens_table: str = "tokens",
    ) -> pd.DataFrame:
        series_table = self._table(series_table)
        events_table = self._table(events_table)
        markets_table = self._table(markets_table)
        tokens_table = self._table(tokens_table)
        sql = f"""
            SELECT
                s.series_id,
                s.slug AS series_slug,
                e.event_id,
                e.slug AS event_slug,
                e.title AS event_title,
                m.market_id,
                m.slug AS market_slug,
                t.token_id,
                t.outcome
            FROM {series_table} s
            JOIN {events_table} e ON e.series_id = s.series_id
            JOIN {markets_table} m ON m.event_id = e.event_id
            JOIN {tokens_table} t ON t.market_id = m.market_id
            WHERE s.series_id = %(series_id)s
            ORDER BY e.event_id, m.market_id, t.token_id
        """
        return self.query_df(sql, {"series_id": series_id})

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            finally:
                self._client = None

    @staticmethod
    def _to_datetime(value: Any) -> datetime | None:
        ts = pd.to_datetime(value, utc=True, errors="coerce")
        if pd.isna(ts):
            return None
        if isinstance(ts, pd.Timestamp):
            return ts.to_pydatetime()
        return ts

    @staticmethod
    def _parse_book_column(value: Any) -> Iterable[Dict[str, Any]]:
        if isinstance(value, (list, tuple)):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return parsed
            except json.JSONDecodeError:
                return []
        return []

    @staticmethod
    def _book_has_volume(book: Iterable[Dict[str, Any]]) -> bool:
        for level in book or []:
            try:
                size = float(level.get("size"))
            except (TypeError, ValueError):
                continue
            if size != 0:
                return True
        return False

    @staticmethod
    def _is_excluded_price(price: Any, excluded: set[float]) -> bool:
        try:
            val = float(price)
        except (TypeError, ValueError):
            return False
        for target in excluded:
            if abs(val - target) < 1e-9:
                return True
        return False
