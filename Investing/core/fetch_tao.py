"""Fetch TAO/USD 1-minute prices from Pyth benchmarks into database/tao_pyth_1min.csv.

If the CSV already has rows, fetches only timestamps after the last stored row.
If the file is missing or has no data, fetches from two calendar months ago through now.
"""

from __future__ import annotations

import calendar
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd

from Investing.core.price_data_provider import PriceDataProvider
from Investing.core.utils import database_csv_path

CSV_FILENAME = "tao_pyth_1min.csv"
SYMBOL = "TAO"
DAY_SECONDS = 86400
# Same cushion as data_loader: avoid the freshest bars that may be incomplete.
END_LAG = timedelta(minutes=2)


def _two_calendar_months_ago(utc_now: datetime) -> datetime:
    y, m = utc_now.year, utc_now.month
    m -= 2
    while m <= 0:
        m += 12
        y -= 1
    last_day = calendar.monthrange(y, m)[1]
    d = min(utc_now.day, last_day)
    return utc_now.replace(
        year=y, month=m, day=d, hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
    )


def _read_csv(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True, on_bad_lines="skip")
    except (FileNotFoundError, pd.errors.EmptyDataError):
        return pd.DataFrame(columns=["price"])
    if df.index.name is None:
        df.index.name = "time"
    df.index = pd.to_datetime(df.index, utc=True)
    if "price" not in df.columns:
        return pd.DataFrame(columns=["price"])
    df = df[["price"]].dropna(how="all")
    return df


def _fetch_chunks(symbol: str, fetch_start: datetime, fetch_end: datetime) -> pd.DataFrame:
    if fetch_start.tzinfo is None:
        fetch_start = fetch_start.replace(tzinfo=timezone.utc)
    if fetch_end.tzinfo is None:
        fetch_end = fetch_end.replace(tzinfo=timezone.utc)

    seconds_to_fetch = (fetch_end - fetch_start).total_seconds()
    if seconds_to_fetch <= 0:
        return pd.DataFrame(columns=["price"])

    provider = PriceDataProvider()
    parts: list[pd.DataFrame] = []
    seconds_processed = 0.0

    while seconds_processed < seconds_to_fetch:
        remaining = seconds_to_fetch - seconds_processed
        chunk_size = min(DAY_SECONDS, int(remaining // 60) * 60)
        if chunk_size <= 0:
            break

        start_dt = fetch_start + timedelta(seconds=seconds_processed)
        start_iso = start_dt.isoformat()

        try:
            batch = provider.fetch_data(symbol, start_iso, int(chunk_size), transformed=True)
            print(f"Fetched {len(batch)} points from {start_iso}")
        except Exception as e:  # noqa: BLE001
            print(f"Chunk failed at {start_iso}: {e}", file=sys.stderr)
            batch = []

        if batch:
            chunk_df = pd.DataFrame(batch)
            chunk_df["time"] = pd.to_datetime(chunk_df["time"], utc=True)
            chunk_df.set_index("time", inplace=True)
            chunk_df = chunk_df[(chunk_df.index >= fetch_start) & (chunk_df.index <= fetch_end)]
            if len(chunk_df) > 0:
                parts.append(chunk_df)

        seconds_processed += chunk_size

    if not parts:
        return pd.DataFrame(columns=["price"])

    out = pd.concat(parts)
    out = out[~out.index.duplicated(keep="last")].sort_index()
    return out


def fetch_tao():
    path = database_csv_path(CSV_FILENAME)
    existing = _read_csv(path)

    utc_now = datetime.now(timezone.utc)
    fetch_end = utc_now - END_LAG

    if len(existing) == 0:
        fetch_start = _two_calendar_months_ago(utc_now)
        print(f"No existing data; fetching from {fetch_start.isoformat()} to {fetch_end.isoformat()}")
    else:
        last_ts = existing.index[-1]
        if not isinstance(last_ts, pd.Timestamp):
            last_ts = pd.to_datetime(last_ts, utc=True)
        fetch_start = (last_ts + timedelta(minutes=1)).to_pydatetime()
        if fetch_start.tzinfo is None:
            fetch_start = fetch_start.replace(tzinfo=timezone.utc)
        print(f"Incremental fetch after {last_ts} through {fetch_end.isoformat()}")

    if fetch_start >= fetch_end:
        print("Already up to date.")
        return

    new_df = _fetch_chunks(SYMBOL, fetch_start, fetch_end)
    if new_df.empty:
        print("No new rows returned from API.")
        return

    if len(existing) == 0:
        merged = new_df
    else:
        merged = pd.concat([existing, new_df])
        merged = merged[~merged.index.duplicated(keep="last")].sort_index()

    merged.index.name = "time"
    merged.to_csv(path, index=True)
    print(f"Wrote {len(merged)} rows to {path} ({len(new_df)} new)")

