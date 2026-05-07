#!/usr/bin/env python3
"""
Fetch Investing88 dashboard History rows for a miner UID from api.investing88.ai
and write CSV. Rows match the expanded History table (uid, hotkey, date, time,
block, kind, fund, strat).

python fetch_history.py {miner_id} -n {strategy_count} -o {output_filename}
eg. python fetch_history.py 132 -n 10 -o last10.csv

"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from typing import Any

import requests

API_STRAT = "https://api.investing88.ai/strat"

# Column order returned by /strat?uid=
HISTORY_COLUMNS = (
    "uid",
    "hotkey",
    "date",
    "time",
    "block",
    "kind",
    "fund",
    "strat",
)


def fetch_strat_rows(uid: int) -> list[list[Any]]:
    r = requests.get(API_STRAT, params={"uid": uid}, timeout=120)
    r.raise_for_status()
    outer = r.json()
    if not isinstance(outer, str):
        raise ValueError(f"Unexpected API payload type: {type(outer)}")
    rows = json.loads(outer)
    if not isinstance(rows, list):
        raise ValueError(f"Unexpected rows type: {type(rows)}")
    return rows


def rows_to_csv(rows: list[list[Any]], out_path: str) -> None:
    n = len(HISTORY_COLUMNS)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        w.writerow(HISTORY_COLUMNS)
        for row in rows:
            if len(row) < n:
                raise ValueError(
                    f"Row has {len(row)} fields, need {n}: {row!r}"
                )
            w.writerow(row[:n])


def main() -> int:
    p = argparse.ArgumentParser(
        description="Export Investing88 History table for a UID to CSV."
    )
    p.add_argument("uid", type=int, help="Miner UID")
    p.add_argument(
        "-o",
        "--output",
        default="",
        help="Output CSV path (default: history_<uid>.csv)",
    )
    p.add_argument(
        "-n",
        "--max-rows",
        type=int,
        default=0,
        metavar="N",
        help=(
            "Export at most N rows. The API returns oldest-first; this keeps the "
            "N most recent rows (same chronological order). 0 means all rows."
        ),
    )
    args = p.parse_args()
    out = args.output or f"history_{args.uid}.csv"

    try:
        rows = fetch_strat_rows(args.uid)
    except requests.HTTPError as e:
        print(f"HTTP error: {e}", file=sys.stderr)
        if e.response is not None and e.response.text:
            print(e.response.text[:500], file=sys.stderr)
        return 1
    except (requests.RequestException, ValueError, json.JSONDecodeError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    if not rows:
        print("No history rows returned.", file=sys.stderr)
        return 1

    if args.max_rows < 0:
        print("Error: --max-rows must be >= 0", file=sys.stderr)
        return 1
    if args.max_rows > 0:
        rows = rows[-args.max_rows :]

    try:
        rows_to_csv(rows, out)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    print(f"Wrote {len(rows)} rows, {len(HISTORY_COLUMNS)} columns -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
