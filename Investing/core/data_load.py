import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
import requests
from Investing.core.define import *

def convert_raw_amount(raw_value):
    if raw_value in (None, ""):
        return ""

    value = Decimal(str(raw_value)) / RAO_PER_UNIT
    formatted = format(value.normalize(), "f")
    return formatted.rstrip("0").rstrip(".") if "." in formatted else formatted


def extract_json_array(text: str, start_index: int) -> str:
    depth = 0
    in_string = False
    escaped = False

    for index in range(start_index, len(text)):
        char = text[index]

        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
        elif char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth == 0:
                return text[start_index : index + 1]

    raise RuntimeError("Could not extract a complete JSON array from the page response.")


def extract_subnet_payload(html: str) -> list[dict]:
    candidates = []
    decoded_html = html.encode("utf-8").decode("unicode_escape")
    marker = '"data":[{"netuid"'
    search_index = 0

    while True:
        marker_index = decoded_html.find(marker, search_index)
        if marker_index == -1:
            break

        array_start = decoded_html.find("[", marker_index)
        payload = extract_json_array(decoded_html, array_start)
        score = sum(
            token in payload
            for token in ('"price"', '"market_cap"', '"volume_24h"', '"symbol"', '"name"')
        )
        candidates.append((score, len(payload), payload))
        search_index = marker_index + len(marker)

    if not candidates:
        raise RuntimeError("Could not find subnet price data in the page response.")

    _, _, best_payload = max(candidates)
    return json.loads(best_payload)


def fetch_alpha_prices() -> list[dict]:
    response = requests.get(
        SOURCE_URL,
        timeout=30,
    )
    response.raise_for_status()

    subnets = extract_subnet_payload(response.text)
    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00:00")

    rows = []
    for subnet in subnets:
        netuid = subnet.get("netuid")
        if netuid in (None, 0):
            continue

        alpha_in_raw = subnet.get("alpha_in_pool") or ""
        tao_in_raw = (
            str(
                int(subnet.get("protocol_provided_tao") or 0)
                + int(subnet.get("user_provided_tao") or 0)
            )
            if subnet.get("protocol_provided_tao") is not None
            or subnet.get("user_provided_tao") is not None
            else ""
        )

        rows.append(
            {
                "time": fetched_at,
                "block": subnet.get("block_number") or "",
                "netuid": netuid,
                "alpha_in": convert_raw_amount(alpha_in_raw),
                "tao_in": convert_raw_amount(tao_in_raw),
                "price": subnet.get("price") or ""
            }
        )

    rows.sort(key=lambda row: int(row["netuid"]))
    return rows

def append_rows_to_csv(rows: list[dict], filename: str) -> None:
    if not rows:
        raise RuntimeError("No alpha token rows were returned.")

    fieldnames = list(rows[0].keys())
    output_path = Path(filename)  # Use the filename parameter
    mode = "a"
    write_header = not output_path.exists()

    if output_path.exists():
        with output_path.open("r", newline="", encoding="utf-8") as existing_file:
            existing_reader = csv.reader(existing_file)
            existing_header = next(existing_reader, [])

        if existing_header != fieldnames:
            mode = "w"
            write_header = True

    with output_path.open(mode, newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)