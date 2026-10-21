"""Small, bounded official USD-M archive adapter; retrospective evidence only.

No exchange SDK, credentials, synthetic receipt times or live service. ZIPs and
checksums stay permanently outside git; no member is extracted to disk.
"""

from __future__ import annotations

import calendar
import csv
import hashlib
import io
import json
import math
import os
import re
import shutil
import time
import urllib.error
import urllib.request
import zipfile
from decimal import Decimal
from pathlib import Path

from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.raw import AvailabilityClassV2, RawObservationV2
from atlas.v2.instruments import EnvironmentV2, InstrumentKeyV2, ProductTypeV2, VenueV2

BASE = "https://data.binance.vision/data/futures/um"
MAX_ZIP = 2_000_000
MAX_MEMBER = 8_000_000
HEADER = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "ignore",
]


def digest(blob: bytes) -> str:
    return hashlib.sha256(blob).hexdigest()


def key_for(symbol: str) -> InstrumentKeyV2:
    # Namespace identity, explicitly NOT an observed historical product revision.
    rev = digest(("UNKNOWN_HISTORICAL_UM_CONTRACT_REVISION:" + symbol).encode())
    return InstrumentKeyV2(
        VenueV2.BINANCE,
        EnvironmentV2.MAINNET,
        ProductTypeV2.LINEAR_PERPETUAL,
        symbol,
        symbol.removesuffix("USDT"),
        "USDT",
        "USDT",
        rev,
    )


def read_zip(blob: bytes, name: str, interval: str, month: str) -> tuple[bytes, list[list[str]], dict]:
    if len(blob) > MAX_ZIP:
        raise ValueError("ZIP_BYTE_LIMIT")
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        infos = z.infolist()
        if len(infos) != 1 or infos[0].filename != name.removesuffix(".zip") + ".csv":
            raise ValueError("ZIP_MEMBER_IDENTITY")
        if infos[0].file_size > MAX_MEMBER:
            raise ValueError("ZIP_EXPANSION_LIMIT")
        body = z.read(infos[0])  # CRC is checked by zipfile; bounded before decompression.
    reader = csv.reader(io.StringIO(body.decode("utf-8")))
    rows = list(reader)
    if rows and rows[0] == HEADER:
        rows = rows[1:]
    step = {"15m": 900000, "1h": 3600000}[interval]
    y, m = map(int, month.split("-"))
    begin = calendar.timegm((y, m, 1, 0, 0, 0)) * 1000
    end = begin + calendar.monthrange(y, m)[1] * 86400000
    previous = None
    gaps = 0
    for r in rows:
        if len(r) != 12:
            raise ValueError("CSV_COLUMN_COUNT")
        op, cl = int(r[0]), int(r[6])
        if not begin <= op < end or op % step or cl != op + step - 1:
            raise ValueError("TIMESTAMP_UNITS_OR_UTC_BOUNDARY")
        if previous is not None:
            if op <= previous:
                raise ValueError("DUPLICATE_OR_NONMONOTONE")
            gaps += (op - previous) // step - 1
        previous = op
        o, h, low, c, v, qv = [float(r[k]) for k in (1, 2, 3, 4, 5, 7)]
        if not all(math.isfinite(x) for x in (o, h, low, c, v, qv)):
            raise ValueError("NONFINITE")
        if not 0 < low <= min(o, c) <= max(o, c) <= h or min(v, qv) < 0 or int(r[8]) < 0:
            raise ValueError("INVALID_MARKET_ROW")
    if not rows:
        raise ValueError("EMPTY_ARCHIVE")
    info = {
        "csv_sha256": digest(body),
        "csv_bytes": len(body),
        "rows": len(rows),
        "first_open_ms": int(rows[0][0]),
        "last_close_ms": int(rows[-1][6]),
        "internal_missing_slots": gaps,
        "missing_month_slots": (end - begin) // step - len(rows),
        "duplicate_rows": 0,
        "timestamp_unit": "UTC Unix milliseconds",
        "bar_close_semantics": "exclusive close = archive close_time + 1ms",
        "member": name.removesuffix(".zip") + ".csv",
    }
    return body, rows, info


def get(url: str, limit: int) -> tuple[bytes, dict]:
    if not url.startswith(BASE + "/") or not re.fullmatch(r"[A-Za-z0-9./:_-]+", url):
        raise ValueError("OFFICIAL_ARCHIVE_URL_REQUIRED")
    started = time.time_ns()
    request = urllib.request.Request(url, headers={"User-Agent": "ATLAS-Historical-Research/1"})
    with urllib.request.urlopen(request, timeout=25) as response:
        if response.geturl() != url:
            raise ValueError("UNEXPECTED_REDIRECT")
        chunks, size = [], 0
        while True:
            chunk = response.read(min(65536, limit + 1 - size))
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
            if size > limit:
                raise ValueError("HTTP_BYTE_LIMIT")
            if time.time_ns() - started > 40_000_000_000:
                raise TimeoutError("HTTP_TOTAL_TIME_LIMIT")
        blob = b"".join(chunks)
        return blob, {
            "url": url,
            "status": response.status,
            "bytes": len(blob),
            "requested_at_ns": started,
            "received_at_ns": time.time_ns(),
            "sha256": digest(blob),
            "last_modified": response.headers.get("Last-Modified"),
            "timeout_seconds": 25,
            "byte_limit": limit,
        }


def acquire(root: Path, symbol: str, interval: str, month: str) -> dict:
    if not re.fullmatch(r"[A-Z0-9]+USDT", symbol) or interval not in ("15m", "1h"):
        raise ValueError("INVALID_MARKET_IDENTITY")
    if not re.fullmatch(r"20[0-9]{2}-[01][0-9]", month):
        raise ValueError("INVALID_MONTH")
    root.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(root).free < 2_000_000_000 + MAX_ZIP:
        raise OSError("PHYSICAL_RESERVE_2GB")
    name = f"{symbol}-{interval}-{month}.zip"
    url = f"{BASE}/monthly/klines/{symbol}/{interval}/{name}"
    record = {
        "symbol": symbol,
        "interval": interval,
        "month": month,
        "market": "BINANCE_USD_M_FUTURES",
        "url": url,
        "checksum_url": url + ".CHECKSUM",
        "availability_class": "RECONSTRUCTED_MARKET",
        "source_revision_history": "UNKNOWN",
    }
    meta = root / (name + ".json")
    if meta.exists():
        old = json.loads(meta.read_text())
        if old["status"] == "VERIFIED":
            blob = (root / name).read_bytes()
            if digest(blob) != old["archive_sha256"]:
                raise ValueError("RETAINED_ARCHIVE_CHANGED")
            read_zip(blob, name, interval, month)
            return old
    try:
        checksum, cr = get(url + ".CHECKSUM", 4096)
        fields = checksum.decode().split()
        if len(fields) != 2 or fields[1] != name or not re.fullmatch("[0-9a-f]{64}", fields[0]):
            raise ValueError("OFFICIAL_CHECKSUM_SCHEMA")
        blob, ar = get(url, MAX_ZIP)
        if digest(blob) != fields[0]:
            raise ValueError("OFFICIAL_CHECKSUM_MISMATCH")
        _, _, info = read_zip(blob, name, interval, month)
        # Retained evidence, bounded writes; refuse conflicts, never replace originals.
        for filename, data in ((name, blob), (name + ".CHECKSUM", checksum)):
            path = root / filename
            if path.exists():
                if path.read_bytes() != data:
                    raise ValueError("IMMUTABLE_SOURCE_CONFLICT")
            else:
                with path.open("xb") as f:
                    f.write(data)
                    f.flush()
                    os.fsync(f.fileno())
        record.update(
            status="VERIFIED",
            archive_sha256=digest(blob),
            official_sha256=fields[0],
            archive_bytes=len(blob),
            requests=[cr, ar],
            actual_acquired_at_ns=ar["received_at_ns"],
            **info,
        )
    except urllib.error.HTTPError as exc:
        record.update(
            status="HTTP_ERROR",
            status_code=exc.code,
            failed_request=exc.url,
            error_body=exc.read(1024).decode(errors="replace"),
            attempted_at_ns=time.time_ns(),
        )
    except Exception as exc:
        record.update(status="FAILED", error=f"{type(exc).__name__}: {exc}", attempted_at_ns=time.time_ns())
    meta.write_text(json.dumps(record, indent=2) + "\n")
    return record


def bars_for(root: Path, records: list[dict], symbol: str, interval: str) -> list[CausalBarV2]:
    key = key_for(symbol)
    kind = {"15m": BarIntervalV2.M15, "1h": BarIntervalV2.H1}[interval]
    bars = []
    for record in sorted(records, key=lambda r: r["month"]):
        if record["symbol"] != symbol or record["interval"] != interval or record["status"] != "VERIFIED":
            continue
        name = record["url"].rsplit("/", 1)[1]
        blob = (root / name).read_bytes()
        if digest(blob) != record["archive_sha256"]:
            raise ValueError("SOURCE_CONTENT_CHANGED")
        _, rows, _ = read_zip(blob, name, interval, record["month"])
        received = record["actual_acquired_at_ns"]
        for i, row in enumerate(rows):
            op = int(row[0]) * 1_000_000
            cl = op + kind.duration_ns
            raw = RawObservationV2.build(
                instrument_revision=key.contract_revision,
                source_id="BINANCE_OFFICIAL_UM_MONTHLY_ARCHIVE",
                event_type="BAR_" + kind.value,
                event_at_ns=cl,
                received_at_ns=received,
                ingested_at_ns=received,
                available_at_ns=received,
                translation_version="UM_ARCHIVE_RESEARCH_V1",
                payload=",".join(row),
                sequence=record["archive_sha256"] + ":" + str(i),
                availability_class=AvailabilityClassV2.RECONSTRUCTED_MARKET,
                replay_available_at_ns=cl,
                quality_flags=("HISTORICAL_PRODUCT_REVISION_UNKNOWN",),
            )
            bars.append(CausalBarV2(raw, kind, op, cl, *(Decimal(row[j]) for j in (1, 2, 3, 4, 5)), True))
    if any(b.open_at_ns <= a.open_at_ns for a, b in zip(bars, bars[1:], strict=False)):
        raise ValueError("CROSS_ARCHIVE_DUPLICATE_OR_ORDER")
    return bars


def h4_from_h1(bars: list[CausalBarV2]) -> list[CausalBarV2]:
    groups: dict[int, list[CausalBarV2]] = {}
    duration = BarIntervalV2.H4.duration_ns
    for bar in bars:
        groups.setdefault(bar.open_at_ns // duration * duration, []).append(bar)
    result = []
    for op, group in sorted(groups.items()):
        if [b.open_at_ns for b in group] != [op + i * BarIntervalV2.H1.duration_ns for i in range(4)]:
            continue
        receipt = max(b.raw.received_at_ns for b in group)
        raw = RawObservationV2.build(
            instrument_revision=group[0].instrument_revision,
            source_id="BINANCE_UM_ARCHIVE_H1_AGGREGATION",
            event_type="BAR_4H",
            event_at_ns=op + duration,
            received_at_ns=receipt,
            ingested_at_ns=receipt,
            available_at_ns=receipt,
            translation_version="EXACT_4_CONTIGUOUS_H1_V1",
            payload=[b.content_hash for b in group],
            availability_class=AvailabilityClassV2.RECONSTRUCTED_MARKET,
            replay_available_at_ns=op + duration,
        )
        result.append(
            CausalBarV2(
                raw,
                BarIntervalV2.H4,
                op,
                op + duration,
                group[0].open,
                max(b.high for b in group),
                min(b.low for b in group),
                group[-1].close,
                sum((b.volume for b in group), Decimal(0)),
                True,
            )
        )
    return result
