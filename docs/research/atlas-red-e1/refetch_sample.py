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

    print(json.dumps({
        "status": "SAMPLE_VERIFIED",
        "zip_url": ZIP_URL,
        "checksum_url": CHECKSUM_URL,
        "sha256": actual_sha,
        "checksum_sha256": hashlib.sha256(checksum_bytes).hexdigest(),
        "zip_bytes": len(archive),
        "checksum_bytes": len(checksum_bytes),
        "member": EXPECTED_MEMBER,
        "rows": len(data),
        "columns": len(EXPECTED_HEADER),
        "first_open_utc": datetime.fromtimestamp(opens[0] / 1000, tz=UTC).isoformat(),
        "last_open_utc": datetime.fromtimestamp(opens[-1] / 1000, tz=UTC).isoformat(),
        "archive_retained": False,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
