#!/usr/bin/env python3
"""Independently bind cached CSV bytes to pinned official ZIP/checksum objects.

No exchange API, credentials, strategy code, or producer helpers are used.
Only the exact 770 archive URLs in the reviewed manifest are contacted.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import io
import json
import pathlib
import re
import time
import urllib.error
import urllib.request
import zipfile
from datetime import UTC, datetime

MAX_ZIP = 20 * 1024 * 1024
MAX_MEMBER = 20 * 1024 * 1024
PINNED_SOURCE_SHA = "c64a12c3aa5e4c500f0b431fe65c14fc07e45f18bc30100b6298f539f4c80d7b"


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def fetch(url: str, limit: int) -> bytes:
    if not url.startswith("https://data.binance.vision/data/futures/um/monthly/klines/"):
        raise ValueError("NONOFFICIAL_URL")
    for attempt in range(2):
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                data = response.read(limit + 1)
            if len(data) > limit:
                raise ValueError("SOURCE_BYTE_LIMIT")
            return data
        except urllib.error.HTTPError as exc:
            if exc.code == 404 or attempt or exc.code not in (429, 500, 502, 503, 504):
                raise
            time.sleep(1)
    raise AssertionError("unreachable")


def verify_member(payload: bytes, row: dict, cached_csv: bytes) -> dict:
    expected = row["published_sha256"]
    if digest(payload) != expected or expected != row["downloaded_sha256"]:
        raise ValueError("PINNED_ARCHIVE_SHA_MISMATCH")
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        infos = archive.infolist()
        if len(infos) != 1 or infos[0].filename != row["member"] or infos[0].file_size > MAX_MEMBER:
            raise ValueError("ZIP_MEMBER_IDENTITY_OR_SIZE")
        member = archive.read(infos[0])  # zipfile verifies the member CRC.
    if member != cached_csv:
        raise ValueError("CACHED_CSV_DIFFERS_FROM_VERIFIED_ZIP_MEMBER")
    return {
        "archive_sha256": expected,
        "archive_bytes": len(payload),
        "member": row["member"],
        "csv_sha256": digest(member),
        "csv_bytes": len(member),
    }


def verify_one(row: dict, cache: pathlib.Path, csv_cache: pathlib.Path) -> tuple[dict, str]:
    url = row["url"]
    name = url.rsplit("/", 1)[1]
    base = {"url": url, "symbol": row["symbol"], "interval": row["interval"], "month": row["month"]}
    if row["status"] == "MISSING_OBJECT_404":
        try:
            fetch(url + ".CHECKSUM", 4096)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return {
                    **base,
                    "status": "OFFICIAL_404_REOBSERVED",
                    "endpoint": "sibling .CHECKSUM; payload not retried",
                }, "404"
            raise
        raise ValueError("PINNED_MISSING_SOURCE_STATUS_CHANGED")
    zip_path, checksum_path = cache / name, cache / (name + ".CHECKSUM")
    route = "REUSED_PINNED_ZIP"
    if not zip_path.exists() or not checksum_path.exists():
        checksum = fetch(url + ".CHECKSUM", 4096)
        payload = fetch(url, MAX_ZIP)
        route = "FRESH_OFFICIAL_ZIP"
    else:
        checksum, payload = checksum_path.read_bytes(), zip_path.read_bytes()
    match = re.fullmatch(rb"([0-9a-fA-F]{64})\s+([^\r\n]+)\s*", checksum)
    if not match or match[2].decode() != name or match[1].decode().lower() != row["published_sha256"]:
        raise ValueError("OFFICIAL_CHECKSUM_IDENTITY_CHANGED")
    proof = verify_member(payload, row, (csv_cache / row["csv_scratch"]).read_bytes())
    if route == "FRESH_OFFICIAL_ZIP":
        zip_path.write_bytes(payload)
        checksum_path.write_bytes(checksum)
    return {
        **base,
        "status": "INDEPENDENT_ZIP_CHECKSUM_AND_CSV_MATCH",
        **proof,
        "published_sha256": row["published_sha256"],
        "data_rows": row["rows"],
    }, route


def run(
    manifest: pathlib.Path, cache: pathlib.Path, csv_cache: pathlib.Path, output: pathlib.Path, execution: pathlib.Path
) -> dict:
    if digest(manifest.read_bytes()) != PINNED_SOURCE_SHA:
        raise ValueError("PINNED_SOURCE_MANIFEST_MISMATCH")
    rows = [json.loads(line) for line in manifest.read_text().splitlines()]
    objects = [r for r in rows if r["status"] in ("VERIFIED", "MISSING_OBJECT_404")]
    if len(objects) != 770 or sum(r["status"] == "VERIFIED" for r in objects) != 757:
        raise ValueError("SOURCE_OBJECT_DENOMINATOR")
    if sum(int(r.get("archive_bytes", 0)) for r in objects) != 55_549_936:
        raise ValueError("SOURCE_TOTAL_BYTES_CHANGED")
    cache.mkdir(parents=True, exist_ok=True)
    started = datetime.now(UTC).isoformat()
    clock = time.monotonic()
    proofs, routes = [], {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(verify_one, row, cache, csv_cache) for row in objects]
        for future in concurrent.futures.as_completed(futures):
            proof, route = future.result()
            proofs.append(proof)
            routes[route] = routes.get(route, 0) + 1
            if len(proofs) % 100 == 0:
                print(json.dumps({"verified_objects": len(proofs)}), flush=True)
    output.write_text(
        "".join(
            json.dumps(p, sort_keys=True, separators=(",", ":")) + "\n" for p in sorted(proofs, key=lambda p: p["url"])
        )
    )
    report = {
        "started_utc": started,
        "completed_utc": datetime.now(UTC).isoformat(),
        "wall_seconds": time.monotonic() - clock,
        "routes": routes,
        "objects": len(proofs),
        "archive_payload_bytes": 55_549_936,
        "source_proof_sha256": digest(output.read_bytes()),
        "historical_receipt": "UNKNOWN_NOT_CAPTURED_AT_ACQUISITION",
        "current_run_receipt": "2026 independent acquisition; never historical receipt",
        "retained_historical_parser_rejects": 420,
        "historical_rejected_download_bytes": "UNKNOWN",
    }
    execution.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    return report


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest", type=pathlib.Path, required=True)
    p.add_argument("--cache", type=pathlib.Path, required=True)
    p.add_argument("--csv-cache", type=pathlib.Path, required=True)
    p.add_argument("--output", type=pathlib.Path, required=True)
    p.add_argument("--execution", type=pathlib.Path, required=True)
    a = p.parse_args()
    print(json.dumps(run(a.manifest, a.cache, a.csv_cache, a.output, a.execution), sort_keys=True))
