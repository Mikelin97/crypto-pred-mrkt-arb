import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import psycopg2
import requests
from psycopg2.extras import execute_values




def get_all_events(closed="false", tag_id='', max_events=None):

    """
        Get all market events with the option to filter based on a tag_id.
        If max_events is provided, stop after collecting that many.
    """
    params = {
        "closed": closed,
        "limit": 500,
        "offset": 0,
        # 'tag_id': ''
    }
    if tag_id:
        params["tag_id"] = tag_id

    events = []
    r = requests.get(url="https://gamma-api.polymarket.com/events", params=params)
    response = r.json()
    while response:
        events += response
        if max_events and len(events) >= max_events:
            return events[:max_events]
        params["offset"] += 500
        r = requests.get(url="https://gamma-api.polymarket.com/events", params=params)
        response = r.json()

    return events


# ---------- Shared helpers ----------
def safe_float(value: Any) -> Optional[float]:
    """Convert numeric-like values to float, returning None on failure."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_list(value: Any) -> Optional[List[Any]]:
    """Parse JSON list strings or pass through existing lists."""
    if value is None:
        return None
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            return None
    return None


# ---------- Series ----------
def fetch_series(limit: Optional[int] = None) -> List[Dict]:
    """Pull all series from the Polymarket API, optionally limiting count."""
    url = "https://gamma-api.polymarket.com/series"
    params = {"limit": 100, "offset": 0}
    series: List[Dict] = []

    response = requests.get(url, params=params)
    new_series = response.json()
    while new_series:
        series += new_series
        if limit and len(series) >= limit:
            return series[:limit]
        params["offset"] += 100
        response = requests.get(url, params=params)
        new_series = response.json()

    return series


def populate_series(conn, series_data: Optional[List[Dict]] = None) -> List[Dict]:
    """Insert series rows into the DB and return the data used."""
    cur = conn.cursor()
    series_rows = series_data if series_data is not None else fetch_series()

    rows = []
    for s in series_rows:
        rows.append(
            [
                s.get("id") or s.get("series_id") or s.get("seriesId"),
                s.get("ticker"),
                s.get("slug"),
                s.get("title"),
                s.get("seriesType") or s.get("series_type"),
                s.get("recurrence"),
                s.get("active"),
                s.get("closed"),
                s.get("published_at") or s.get("publishedAt"),
                s.get("updated_at") or s.get("updatedAt"),
                s.get("start_date") or s.get("startDate"),
                s.get("created_at") or s.get("createdAt"),
            ]
        )

    if rows:
        execute_values(
            cur,
            """
            INSERT INTO series (
                series_id,
                ticker,
                slug,
                title,
                series_type,
                recurrence,
                active,
                closed,
                published_at,
                updated_at,
                start_date,
                created_at
            )
            VALUES %s
            ON CONFLICT (series_id) DO NOTHING
            """,
            rows,
        )

    conn.commit()
    cur.close()
    return series_rows


# ---------- Events ----------
EVENT_COLUMNS = [
    "event_id",
    "series_id",
    "parent_event_id",
    "ticker",
    "slug",
    "title",
    "description",
    "resolution_source",
    "category",
    "subcategory",
    "active",
    "closed",
    "archived",
    "new",
    "featured",
    "restricted",
    "cyom",
    "enable_order_book",
    "neg_risk",
    "enable_neg_risk",
    "neg_risk_augmented",
    "automatically_active",
    "automatically_resolved",
    "creation_date",
    "create_at",
    "updated_at",
    "start_date",
    "start_time",
    "end_date",
    "closed_time",
    "finished_timestamp",
    "volume",
    "open_interest",
    "liquidity",
    "volume_24hr",
    "volume_1wk",
    "volume_1mo",
    "liquidity_amm",
    "liquidity_clob",
    "comment_count",
]


def build_event_row(event: Dict) -> List:
    """Transform a Polymarket event into the DB shape."""
    return [
        event.get("id"),
        event.get("seriesId") or event.get("series_id"),
        event.get("parentEventId"),
        event.get("ticker"),
        event.get("slug"),
        event.get("title"),
        event.get("description"),
        event.get("resolutionSource"),
        event.get("category"),
        event.get("subcategory"),
        event.get("active"),
        event.get("closed"),
        event.get("archived"),
        event.get("new"),
        event.get("featured"),
        event.get("restricted"),
        event.get("cyom"),
        event.get("enableOrderBook"),
        event.get("negRisk"),
        event.get("enableNegRisk"),
        event.get("negRiskAugmented"),
        event.get("automaticallyActive"),
        event.get("automaticallyResolved"),
        event.get("creationDate"),
        event.get("createdAt"),
        event.get("updatedAt"),
        event.get("startDate"),
        event.get("startTime"),
        event.get("endDate"),
        event.get("closedTime"),
        event.get("finishedTimestamp"),
        safe_float(event.get("volume")),
        safe_float(event.get("openInterest")),
        safe_float(event.get("liquidity")),
        safe_float(event.get("volume24h")),
        safe_float(event.get("volume1wk")),
        safe_float(event.get("volume1mo")),
        safe_float(event.get("liquidityAmm")),
        safe_float(event.get("liquidityClob")),
        event.get("commentCount"),
    ]


def populate_events(conn, events: List[Dict]) -> None:
    """Bulk insert events into the DB."""
    cur = conn.cursor()
    rows = [build_event_row(event) for event in events]

    if rows:
        execute_values(
            cur,
            f"""
            INSERT INTO events ({", ".join(EVENT_COLUMNS)})
            VALUES %s
            ON CONFLICT (event_id) DO NOTHING
            """,
            rows,
        )

    conn.commit()
    cur.close()


# ---------- Markets ----------
MARKET_COLUMNS = [
    "market_id",
    "event_id",
    "condition_id",
    "slug",
    "resolution_source",
    "game_id",
    "sports_market_type",
    "question",
    "description",
    "category",
    "subcategory",
    "market_type",
    "market_maker_address",
    "outcomes",
    "clob_token_ids",
    "active",
    "closed",
    "fpmm_live",
    "ready",
    "funded",
    "approved",
    "neg_risk",
    "neg_risk_other",
    "accepting_orders",
    "holding_rewards_enabled",
    "enable_order_book",
    "comments_enabled",
    "uma_resolution_status",
    "rewards_min_size",
    "rewards_max_spread",
    "clob_rewards",
    "seconds_delay",
    "lower_bound",
    "upper_bound",
    "order_price_min_tick_size",
    "order_min_size",
    "maker_base_fee",
    "taker_base_fee",
    "spread",
    "last_trade_price",
    "best_bid",
    "best_ask",
    "fee",
    "start_date",
    "end_date",
    "created_at",
    "updated_at",
    "closed_time",
    "game_start_time",
    "event_start_time",
    "accepting_orders_timestamp",
    "liquidity",
    "liquidity_num",
    "volume",
    "volume_num",
    "volume_24h",
    "volume_1wk",
    "volume_1mo",
    "volume_1yr",
]


def _parse_uma_status(market: Dict) -> Any:
    status = market.get("umaResolutionStatus")
    if status is not None:
        return status
    fallback = market.get("umaResolutionStatuses")
    if isinstance(fallback, list):
        return ",".join(fallback)
    return fallback


def build_market_row(event: Dict, market: Dict) -> List:
    """Transform a Polymarket market into the DB shape."""
    return [
        market.get("id"),
        event.get("id"),
        market.get("conditionId"),
        market.get("slug"),
        market.get("resolutionSource"),
        market.get("gameId"),
        market.get("sportsMarketType"),
        market.get("question"),
        market.get("description"),
        market.get("category"),
        market.get("subcategory"),
        market.get("marketType"),
        market.get("marketMakerAddress"),
        parse_list(market.get("outcomes")),
        parse_list(market.get("clobTokenIds")),
        market.get("active"),
        market.get("closed"),
        market.get("fpmmLive"),
        market.get("ready"),
        market.get("funded"),
        market.get("approved"),
        market.get("negRisk"),
        market.get("negRiskOther"),
        market.get("acceptingOrders"),
        market.get("holdingRewardsEnabled"),
        market.get("enableOrderBook"),
        market.get("commentsEnabled"),
        _parse_uma_status(market),
        safe_float(market.get("rewardsMinSize")),
        safe_float(market.get("rewardsMaxSpread")),
        safe_float(market.get("clobRewards")),
        safe_float(market.get("secondsDelay")),
        safe_float(market.get("lowerBound")),
        safe_float(market.get("upperBound")),
        safe_float(market.get("orderPriceMinTickSize")),
        safe_float(market.get("orderMinSize")),
        safe_float(market.get("makerBaseFee")),
        safe_float(market.get("takerBaseFee")),
        safe_float(market.get("spread")),
        safe_float(market.get("lastTradePrice")),
        safe_float(market.get("bestBid")),
        safe_float(market.get("bestAsk")),
        safe_float(market.get("fee")),
        market.get("startDate"),
        market.get("endDate"),
        market.get("createdAt"),
        market.get("updatedAt"),
        market.get("closedTime"),
        market.get("gameStartTime"),
        market.get("eventStartTime"),
        market.get("acceptingOrdersTimestamp"),
        safe_float(market.get("liquidity")),
        safe_float(market.get("liquidityNum")),
        safe_float(market.get("volume")),
        safe_float(market.get("volumeNum")),
        safe_float(market.get("volume24h")),
        safe_float(market.get("volume1wk")),
        safe_float(market.get("volume1mo")),
        safe_float(market.get("volume1yr")),
    ]


def populate_markets(conn, events: List[Dict]) -> None:
    """Bulk insert markets for all events."""
    cur = conn.cursor()
    rows: List[List] = []

    for event in events:
        for market in event.get("markets", []):
            rows.append(build_market_row(event, market))

    if rows:
        execute_values(
            cur,
            f"""
            INSERT INTO markets ({", ".join(MARKET_COLUMNS)})
            VALUES %s
            ON CONFLICT (market_id) DO NOTHING
            """,
            rows,
        )

    conn.commit()
    cur.close()


# ---------- Tokens ----------
def populate_tokens(conn, events: List[Dict]) -> None:
    """Insert outcome tokens for each market."""
    cur = conn.cursor()
    rows: List[List] = []

    for event in events:
        for market in event.get("markets", []):
            outcomes = parse_list(market.get("outcomes")) or []
            token_ids = parse_list(market.get("clobTokenIds")) or []
            for outcome, token_id in zip(outcomes, token_ids):
                rows.append([token_id, market.get("id"), outcome])

    if rows:
        execute_values(
            cur,
            """
            INSERT INTO tokens (token_id, market_id, outcome)
            VALUES %s
            ON CONFLICT (token_id) DO NOTHING
            """,
            rows,
        )

    conn.commit()
    cur.close()


# ---------- Tags ----------
def populate_tags(conn, events: List[Dict]) -> None:
    """Insert unique tags from all events."""
    cur = conn.cursor()
    rows: List[List] = []
    seen: Set[str] = set()

    for event in events:
        for tag in event.get("tags", []):
            tag_id = str(tag.get("id"))
            if tag_id in seen:
                continue
            seen.add(tag_id)
            rows.append(
                [
                    tag_id,
                    tag.get("label"),
                    tag.get("updatedAt") or tag.get("updated_at"),
                    tag.get("createdAt"),
                    tag.get("publishedAt") or tag.get("published_at"),
                ]
            )

    if rows:
        execute_values(
            cur,
            """
            INSERT INTO tags (tag_id, label, updated_at, created_at, published_at)
            VALUES %s
            ON CONFLICT (tag_id) DO NOTHING
            """,
            rows,
        )

    conn.commit()
    cur.close()


# ---------- Event-tag linkage ----------
def populate_tag_list(conn, events: List[Dict]) -> None:
    """Insert event-to-tag mappings."""
    cur = conn.cursor()
    rows: List[List] = []
    seen: Set[Tuple[str, str]] = set()

    for event in events:
        event_id = event.get("id")
        for tag in event.get("tags", []):
            tag_id = str(tag.get("id"))
            key = (event_id, tag_id)
            if not event_id or not tag_id or key in seen:
                continue
            seen.add(key)
            rows.append([event_id, tag_id])

    if rows:
        execute_values(
            cur,
            """
            INSERT INTO tag_list (event_id, tag_id)
            VALUES %s
            """,
            rows,
        )

    conn.commit()
    cur.close()


# ---------- Helpers for linking series ----------
def attach_series_ids_to_events(events: List[Dict], series_data: List[Dict]) -> None:
    """Ensure each event has a seriesId by mapping series->events from series data."""
    event_to_series: Dict[str, str] = {}
    for series in series_data:
        sid = series.get("id") or series.get("seriesId") or series.get("series_id")
        if not sid:
            continue
        for ev in series.get("events", []):
            eid = ev.get("id")
            if eid:
                event_to_series[str(eid)] = sid

    for ev in events:
        if ev.get("seriesId") or ev.get("series_id"):
            continue
        eid = ev.get("id")
        if eid and str(eid) in event_to_series:
            ev["seriesId"] = event_to_series[str(eid)]


def _parse_int(value: Optional[str]) -> Optional[int]:
    """Try to parse an int from a string env var."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _is_truthy(value: Optional[str], default: bool = False) -> bool:
    """Interpret common truthy strings (e.g., '1', 'true')."""
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _log(message: str, *, error: bool = False) -> None:
    """Emit a log line with an ISO timestamp."""
    timestamp = datetime.utcnow().isoformat() + "Z"
    prefix = "[data-init]"
    target = sys.stderr if error else sys.stdout
    print(f"{prefix} {timestamp} {message}", file=target, flush=True)


# ---------- Orchestration ----------
def populate_database(
    db_name: str,
    closed: str = "false",
    limit: Optional[int] = None,
    host: Optional[str] = None,
    port: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
) -> None:
    """Populate all tables using data from Polymarket."""
    conn_kwargs = {"dbname": db_name}
    if host:
        conn_kwargs["host"] = host
    if port:
        conn_kwargs["port"] = port
    if user:
        conn_kwargs["user"] = user
    if password:
        conn_kwargs["password"] = password

    conn = psycopg2.connect(**conn_kwargs)
    try:
        # Load series first to ensure FK target exists
        series_data = fetch_series()
        populate_series(conn, series_data)

        # Pull events and attach series IDs from series data (events API does not include them by default).
        events = get_all_events(closed=closed, max_events=limit)
        attach_series_ids_to_events(events, series_data)

        # Dependency order
        populate_events(conn, events)
        populate_markets(conn, events)
        populate_tokens(conn, events)
        populate_tags(conn, events)
        populate_tag_list(conn, events)
    finally:
        conn.close()


def main() -> None:
    db_name = os.environ.get("POSTGRES_DB", "postgres")
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    user = os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_PASSWORD", "postgres")
    closed = os.environ.get("CLOSED", "false")
    limit = _parse_int(os.environ.get("LIMIT"))

    # Scheduling controls
    interval_seconds = _parse_int(os.environ.get("REFRESH_INTERVAL_SECONDS")) or 43200  # default 12h
    run_once = _is_truthy(os.environ.get("RUN_ONCE")) or interval_seconds <= 0

    while True:
        started_at = time.time()
        _log(f"Starting load into {db_name}@{host}:{port} (limit={limit}, closed={closed})")
        try:
            populate_database(
                db_name,
                closed=closed,
                limit=limit,
                host=host,
                port=port,
                user=user,
                password=password,
            )
        except Exception as exc:
            _log(f"ERROR during load: {exc}", error=True)
        else:
            elapsed = time.time() - started_at
            _log(f"Load finished in {elapsed:.1f}s")

        if run_once:
            break

        _log(f"Sleeping for {interval_seconds} seconds before next run...")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
