#!/usr/bin/env python3
"""Fetch and validate the single pinned Binance USD-M H1 sample, using stdlib only."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import urllib.request
import zipfile
from datetime import UTC, datetime
from decimal import ROUND_HALF_EVEN, Decimal

ZIP_URL = (
    "https://data.binance.vision/data/futures/um/daily/klines/"
    "BTCUSDT/1h/BTCUSDT-1h-2024-01-01.zip"
)
CHECKSUM_URL = ZIP_URL + ".CHECKSUM"
EXPECTED_SHA256 = "09a24635f090c9b2ab17f4a61aa3c4d262a7ccc88e96ff0199ce66a54a8ab66b"
EXPECTED_MEMBER = "BTCUSDT-1h-2024-01-01.csv"
EXPECTED_HEADER = [
    "open_time", "open", "high", "low", "close", "volume", "close_time",
    "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume", "ignore",
]
FIRST_OPEN_MS = 1_704_067_200_000
EXPECTED_ROWS = 24


def fetch(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "ATLAS-RED-E1-sample-verifier/1"})
    with urllib.request.urlopen(request, timeout=30) as response:
        final_host = response.geturl().split("/", 3)[2].split(":", 1)[0].lower()
        if final_host != "data.binance.vision":
            raise ValueError(f"refusing unexpected redirect host: {final_host}")
        if response.status != 200:
            raise ValueError(f"unexpected HTTP status: {response.status}")
        return response.read()


def percentage(value: Decimal) -> str:
    return str(value.quantize(Decimal("0.000001"), rounding=ROUND_HALF_EVEN))


def main() -> None:
    checksum_bytes = fetch(CHECKSUM_URL)
    try:
        published_sha, published_name = checksum_bytes.decode("ascii").strip().split()
    except (UnicodeDecodeError, ValueError) as exc:
        raise ValueError("official CHECKSUM has an unexpected format") from exc
    if published_name != EXPECTED_MEMBER.replace(".csv", ".zip"):
        raise ValueError(f"unexpected CHECKSUM filename: {published_name}")
    if published_sha != EXPECTED_SHA256:
        raise ValueError("official CHECKSUM differs from the pinned SHA-256; stop for source revision review")

    archive = fetch(ZIP_URL)
    actual_sha = hashlib.sha256(archive).hexdigest()
    if actual_sha != EXPECTED_SHA256 or actual_sha != published_sha:
        raise ValueError("downloaded ZIP SHA-256 does not match both pinned and published checksum")

    with zipfile.ZipFile(io.BytesIO(archive)) as zipped:
        bad_member = zipped.testzip()
        if bad_member is not None:
            raise ValueError(f"ZIP CRC validation failed: {bad_member}")
        if zipped.namelist() != [EXPECTED_MEMBER]:
            raise ValueError(f"unexpected ZIP members: {zipped.namelist()}")
        text = zipped.read(EXPECTED_MEMBER).decode("utf-8")

    rows = list(csv.reader(io.StringIO(text, newline="")))
    if not rows or rows[0] != EXPECTED_HEADER:
        raise ValueError("CSV header differs from the pinned 12-column USD-M kline schema")
    data = rows[1:]
    if len(data) != EXPECTED_ROWS or any(len(row) != len(EXPECTED_HEADER) for row in data):
        raise ValueError(f"expected {EXPECTED_ROWS} twelve-field rows; received {len(data)}")
    opens = [int(row[0]) for row in data]
    expected_opens = [FIRST_OPEN_MS + i * 3_600_000 for i in range(EXPECTED_ROWS)]
    if opens != expected_opens:
        raise ValueError("hourly open timestamps are not the exact expected UTC sequence")
    if any(int(row[6]) != int(row[0]) + 3_600_000 - 1 for row in data):
        raise ValueError("one or more kline close timestamps do not match the hourly interval")

    traced_rows = []
    previous_close: Decimal | None = None
    fields = EXPECTED_HEADER
    for index, values in enumerate(data, start=1):
        record = dict(zip(fields, values, strict=True))
        open_ms = int(record["open_time"])
        close = Decimal(record["close"])
        open_price = Decimal(record["open"])
        row_features = {
            "h1_full_range_pct_of_open": percentage(
                (Decimal(record["high"]) - Decimal(record["low"])) / open_price * Decimal(100)
            ),
            "h1_close_to_close_pct": None if previous_close is None else percentage(
                (close / previous_close - Decimal(1)) * Decimal(100)
            ),
            "h1_close_to_close_missing_reason": (
                "no preceding in-sample H1 close for first row" if previous_close is None else None
            ),
        }
        traced_rows.append({
            "source_row_id": f"{EXPECTED_MEMBER}#data-row-{index}",
            "event_open_at_utc": datetime.fromtimestamp(open_ms / 1000, tz=UTC).isoformat(),
            "event_close_inclusive_at_utc": datetime.fromtimestamp(
                int(record["close_time"]) / 1000, tz=UTC
            ).isoformat(),
            "market_available_at": None,
            "market_available_at_reason": "archive bars contain no original provider/system receive timestamp",
            "raw_bar_fields": record,
            "derived_features": row_features,
            "feature_grade": "CONDITIONAL_DIAGNOSTIC_RECONSTRUCTED_HISTORICAL",
        })
        previous_close = close

    trace = {
        "trace_schema": "ATLAS_RED_E1_SINGLE_ARCHIVE_TRACE_V1",
        "source": {
            "url": ZIP_URL,
            "member": EXPECTED_MEMBER,
            "zip_sha256": actual_sha,
            "official_checksum_url": CHECKSUM_URL,
            "official_checksum_file_sha256": hashlib.sha256(checksum_bytes).hexdigest(),
            "published_and_pinned_sha_match": True,
            "instrument": "BTCUSDT",
            "interval": "1h",
            "event_date_utc": "2024-01-01",
            "archive_download_bytes": len(archive),
            "zip_member_rows": len(data),
            "market_available_at": None,
            "market_available_at_reason": "historical archive retrieval is not the original observation/receipt time",
        },
        "parsed_to_feature_rows": traced_rows,
        "policy_gates": {
            "S1_MTF_TREND_PULLBACK_1_0_0_shadow": {
                "input_gate": "BLOCKED_INPUT",
                "evaluation": "NOT_RUN",
                "missing_requirements": [
                    "aligned completed 4H bars and causal 4H regime",
                    "confirmed 15M trigger bars",
                    "ATR/volatility feature warmup and exact causal setup state",
                    "fresh decision-time BBO and mark/index",
                    "event gate and source-health evidence",
                    "point-in-time contract/filter eligibility and action deadline",
                ],
                "signal_state": None,
                "signal_state_reason": "policy consumer was not called; no signal/no-signal result inferred",
            },
            "S6_CROSS_SECTIONAL_RELATIVE_STRENGTH_1_0_0_shadow_research": {
                "input_gate": "BLOCKED_INPUT",
                "evaluation": "NOT_RUN",
                "available_in_sample_h1_bars": 24,
                "available_in_sample_adjacent_h1_returns": 23,
                "missing_requirements": [
                    "720 consecutive completed H1 return observations (only 23 adjacent in-sample returns)",
                    "aligned BTC/ETH/peer cohort and point-in-time eligible breadth of at least 20 symbols",
                    "4H context and per-slot source-health/universe exclusions",
                    "later 15M action bars, fresh BBO/mark, and historical contract/filter eligibility",
                ],
                "signal_state": None,
                "signal_state_reason": "policy consumer was not called; no rank or signal/no-signal result inferred",
            },
        },
        "outcomes": {
            "label_rows": 0,
            "future_outcomes_read_or_joined": False,
            "profitability_or_win_rate_calculated": False,
            "selection_or_holdout_comparison_performed": False,
        },
        "archive_retained": False,
    }
    print(json.dumps(trace, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
