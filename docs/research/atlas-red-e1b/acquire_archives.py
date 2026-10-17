#!/usr/bin/env python3
"""Bounded anonymous Binance USD-M monthly kline fetch and validation.

Only official archive URLs are used. Raw ZIPs live in a caller supplied scratch
directory and are not part of the repository. Each fetched ZIP is verified
against its published sibling CHECKSUM before its CSV rows are accepted.
"""
from __future__ import annotations

import concurrent.futures
import csv
import hashlib
import io
import json
import math
import os
import pathlib
import re
import threading
import time
import urllib.error
import urllib.request
import zipfile

COHORT = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT", "LTCUSDT", "BCHUSDT", "ETCUSDT", "TRXUSDT", "XLMUSDT", "ATOMUSDT", "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "FILUSDT", "UNIUSDT", "INJUSDT", "SUIUSDT", "SEIUSDT", "RUNEUSDT", "ICPUSDT", "CRVUSDT", "AAVEUSDT", "LDOUSDT", "FTMUSDT", "MATICUSDT", "EOSUSDT", "ALGOUSDT", "HBARUSDT"]
MONTHS = [f"{y}-{m:02d}" for y in (2024, 2025) for m in range(1, 7)]
COHORT_SHA256 = hashlib.sha256(("\n".join(COHORT) + "\n").encode()).hexdigest()
BASE = "https://data.binance.vision/data/futures/um/monthly/klines"
MAX_ONE = 20 * 1024 * 1024
MAX_TOTAL = 1024 * 1024 * 1024
_bytes = 0
_byte_lock = threading.Lock()


def get(url: str) -> tuple[bytes, int]:
    """GET with one bounded retry for transient server/rate errors."""
    for attempt in range(2):
        started = time.monotonic()
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "ATLAS-Research-Archive/1"})
            with urllib.request.urlopen(req, timeout=30) as response:
                body = response.read(MAX_ONE + 1)
                if len(body) > MAX_ONE:
                    raise ValueError("ARCHIVE_EXCEEDS_20_MIB_CAP")
                return body, round((time.monotonic() - started) * 1000)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise FileNotFoundError(f"HTTP_404 {url}") from exc
            if attempt == 0 and exc.code in (429, 500, 502, 503, 504):
                time.sleep(1.5)
                continue
            raise RuntimeError(f"HTTP_{exc.code} {url}") from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            if attempt == 0:
                time.sleep(1.0)
                continue
            raise RuntimeError(f"NETWORK_ERROR {type(exc).__name__}: {exc}") from exc
    raise AssertionError("unreachable")


def one(task: tuple[str, str, str], scratch: pathlib.Path) -> dict[str, object]:
    symbol, interval, month = task
    name = f"{symbol}-{interval}-{month}.zip"
    url = f"{BASE}/{symbol}/{interval}/{name}"
    row: dict[str, object] = {"symbol": symbol, "interval": interval, "month": month,
                              "url": url, "schema": "Binance USD-M klines 12 columns"}
    try:
        published, checksum_ms = get(url + ".CHECKSUM")
        # Binance checksum text convention is '<hex>  <filename>'.
        match = re.fullmatch(rb"([0-9a-fA-F]{64})\s+([^\r\n]+)\s*", published)
        if not match or match.group(2).decode() != name:
            raise ValueError("CHECKSUM_SIBLING_SCHEMA_MISMATCH")
        expected = match.group(1).decode().lower()
        data, download_ms = get(url)
        actual = hashlib.sha256(data).hexdigest()
        if actual != expected:
            raise ValueError("PUBLISHED_CHECKSUM_MISMATCH")
        global _bytes
        with _byte_lock:
            _bytes += len(data) + len(published)
            if _bytes > MAX_TOTAL:
                raise ValueError("TOTAL_ARCHIVE_BYTE_CAP_EXCEEDED")
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            bad = archive.testzip()
            if bad:
                raise ValueError("ZIP_CRC_MISMATCH")
            members = archive.namelist()
            if len(members) != 1 or members[0] != name.removesuffix(".zip") + ".csv":
                raise ValueError("ZIP_MEMBER_SCHEMA_MISMATCH")
            text = archive.read(members[0]).decode("utf-8")
        parsed = []
        prior = None
        gaps = 0
        duration = 3_600_000 if interval == "1h" else 900_000
        header = ["open_time", "open", "high", "low", "close", "volume", "close_time",
                  "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume", "ignore"]
        for index, fields in enumerate(csv.reader(io.StringIO(text))):
            if index == 0 and fields == header:
                continue
            if len(fields) != 12:
                raise ValueError(f"CSV_COLUMN_COUNT_{len(fields)}")
            op = int(fields[0])
            close = int(fields[6])
            # USD-M archives use milliseconds. Fail closed on other plausible units.
            if op < 1_000_000_000_000 or op > 2_000_000_000_000 or close <= op:
                raise ValueError("TIMESTAMP_UNIT_OR_ORDER_INVALID")
            if op % duration or close != op + duration - 1:
                raise ValueError("UTC_BOUNDARY_OR_CLOSE_TIME_INVALID")
            if prior is not None:
                if op <= prior:
                    raise ValueError("DUPLICATE_OR_OUT_OF_ORDER_TIMESTAMP")
                if op != prior + duration:
                    gaps += 1
            prior = op
            numbers = [float(fields[col]) for col in (1, 2, 3, 4, 5)]
            if any(not math.isfinite(v) for v in numbers):
                raise ValueError("NONFINITE_OHLCV")
            if any(v <= 0 for v in numbers[:4]) or numbers[4] < 0:
                raise ValueError("NONPOSITIVE_PRICE_OR_NEGATIVE_VOLUME")
            opx, high, low, closepx, _volume = numbers
            if low > min(opx, closepx) or high < max(opx, closepx) or low > high:
                raise ValueError("INCONSISTENT_OHLC")
            parsed.append((index, op, close, fields[:12]))
        # CSV bytes are small; retain the verified rows as line records for deterministic build.
        raw_csv = scratch / (name.removesuffix(".zip") + ".csv")
        raw_csv.write_text(text, encoding="utf-8")
        row.update({"status": "VERIFIED", "published_sha256": expected,
                    "downloaded_sha256": actual, "checksum_bytes": len(published),
                    "archive_bytes": len(data), "csv_bytes": len(text.encode()),
                    "checksum_ms": checksum_ms, "download_ms": download_ms,
                    "member": members[0], "rows": len(parsed),
                    "first_open_ms": parsed[0][1] if parsed else None,
                    "last_open_ms": parsed[-1][1] if parsed else None,
                    "gap_count": gaps, "csv_scratch": raw_csv.name,
                    "availability_grade": "RECONSTRUCTED_MARKET; downloaded now; historical receive time unavailable"})
    except FileNotFoundError as exc:
        row.update({"status": "MISSING_OBJECT_404", "reason": str(exc)})
    except Exception as exc:
        row.update({"status": "FAILED_VALIDATION_OR_FETCH", "reason": f"{type(exc).__name__}: {exc}"})
    return row


def run(interval: str, symbols: list[str], months: list[str], scratch: pathlib.Path) -> list[dict[str, object]]:
    tasks = [(symbol, interval, month) for symbol in symbols for month in months]
    rows: list[dict[str, object]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(one, task, scratch) for task in tasks]
        for future in concurrent.futures.as_completed(futures):
            rows.append(future.result())
    rows.sort(key=lambda x: (str(x["symbol"]), str(x["interval"]), str(x["month"])))
    return rows


def main() -> None:
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--interval", choices=("1h", "15m"), required=True)
    p.add_argument("--scratch", type=pathlib.Path, required=True)
    p.add_argument("--symbols", nargs="*", default=COHORT)
    p.add_argument("--months", nargs="*", default=MONTHS if "1h" in os.sys.argv else [m for m in MONTHS if m[5:] not in ("01", "02")])
    p.add_argument("--output", type=pathlib.Path, required=True)
    args = p.parse_args()
    args.scratch.mkdir(parents=True, exist_ok=True)
    records = run(args.interval, args.symbols, args.months, args.scratch)
    with args.output.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
    print(json.dumps({"interval": args.interval, "attempted": len(records),
                      "verified": sum(r["status"] == "VERIFIED" for r in records),
                      "missing": sum(r["status"] == "MISSING_OBJECT_404" for r in records),
                      "failed": sum(r["status"] == "FAILED_VALIDATION_OR_FETCH" for r in records),
                      "cohort_sha256": COHORT_SHA256}, sort_keys=True))


if __name__ == "__main__":
    main()
