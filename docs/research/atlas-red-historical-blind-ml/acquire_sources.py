"""Acquire only preregistered public objects; reuse the accepted kline parser."""

from __future__ import annotations

import argparse
import collections
import concurrent.futures
import datetime as dt
import hashlib
import pathlib
import threading
import time
import urllib.error
import urllib.request

import common

# common establishes the pinned repository reader path before this import.
# isort: split
import acquire_archives as accepted


def acquire(cache, *, offline=False):
    p = common.protocol()
    common.verify_prior_inputs()
    cache.mkdir(parents=True, exist_ok=True)
    scope = p["source_scope"]
    tasks = [(s, i, m) for s in p["cohort"] for i in scope["native_intervals"] for m in scope["new_months"]]
    if len(tasks) != 210:
        raise ValueError("SOURCE_OBJECT_SCOPE")
    allowed = set()
    for s, i, m in tasks:
        name = f"{s}-{i}-{m}.zip"
        url = f"{scope['base']}/{s}/{i}/{name}"
        allowed.update((url, url + ".CHECKSUM"))
    lock = threading.Lock()
    records = []
    network_bytes = 0
    network_requests = 0

    def get(url):
        nonlocal network_bytes, network_requests
        if url not in allowed:
            raise ValueError("SOURCE_URL_OUTSIDE_PREREGISTRATION")
        name = url.rsplit("/", 1)[1]
        path = cache / name
        absent = cache / (name + ".404.json")
        if path.exists():
            return path.read_bytes(), 0
        if absent.exists():
            raise FileNotFoundError("PINNED_OFFICIAL_404 " + url)
        if offline:
            raise ValueError("OFFLINE_CACHE_INCOMPLETE:" + name)
        for attempt in range(scope["maximum_attempts_per_URL"]):
            started = time.monotonic()
            status = 0
            try:
                request = urllib.request.Request(url, headers={"User-Agent": "ATLAS-Sealed-Research/1"})
                with urllib.request.urlopen(request, timeout=30) as response:
                    status = response.status
                    body = response.read(scope["maximum_single_response_bytes"] + 1)
            except urllib.error.HTTPError as exc:
                status = exc.code
                body = exc.read(scope["maximum_single_response_bytes"] + 1)
            except (urllib.error.URLError, TimeoutError):
                status, body = 0, b""
            with lock:
                network_requests += 1
                network_bytes += len(body)
                record = {
                    "url": url,
                    "status": status,
                    "attempt": attempt + 1,
                    "actual_download_completed_at": dt.datetime.now(dt.UTC).isoformat(),
                    "response_bytes": len(body),
                    "response_sha256": hashlib.sha256(body).hexdigest(),
                }
                records.append(record)
                if network_bytes > scope["maximum_new_payload_plus_metadata_bytes"]:
                    raise ValueError("REGISTERED_TOTAL_BYTE_CAP")
            if len(body) > scope["maximum_single_response_bytes"]:
                raise ValueError("REGISTERED_OBJECT_BYTE_CAP")
            if status == 200:
                path.write_bytes(body)
                return body, round((time.monotonic() - started) * 1000)
            if status == 404:
                common.save(absent, record)
                raise FileNotFoundError("HTTP_404 " + url)
            if attempt + 1 < scope["maximum_attempts_per_URL"] and status in (0, 429, 500, 502, 503, 504):
                continue
            raise RuntimeError("HTTP_OR_TRANSPORT_FAILURE:" + str(status))
        raise AssertionError("unreachable")

    original_get = accepted.get
    accepted.get = get
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=scope["workers"]) as executor:
            rows = list(executor.map(lambda task: accepted.one(task, cache), tasks))
    finally:
        accepted.get = original_get
    failures = [r for r in rows if r["status"] not in ("VERIFIED", "MISSING_OBJECT_404")]
    stable = []
    for row in rows:
        obj = {k: v for k, v in row.items() if k not in ("checksum_ms", "download_ms")}
        if obj["status"] == "VERIFIED":
            obj["csv_sha256"] = common.sha(cache / obj["csv_scratch"])
            obj["checksum_file_sha256"] = common.sha(cache / (obj["url"].rsplit("/", 1)[1] + ".CHECKSUM"))
        else:
            obj.pop("reason", None)
            obj["reason"] = "OFFICIAL_SIBLING_CHECKSUM_HTTP_404; no substitute symbol/source"
        stable.append(obj)
    manifest = common.HERE / "SOURCE_OBJECTS.jsonl"
    content = "".join(common.canonical(r) + "\n" for r in stable)
    if offline:
        if manifest.read_text() != content:
            raise ValueError("OFFLINE_SOURCE_MANIFEST_CHANGED")
    else:
        manifest.write_text(content)
        (common.HERE / "ACQUISITION.jsonl").write_text("".join(common.canonical(r) + "\n" for r in records))
    result = {
        "requested_zip_objects": len(tasks),
        "status_counts": dict(collections.Counter(r["status"] for r in rows)),
        "verified_archive_bytes": sum(int(r.get("archive_bytes", 0)) for r in rows),
        "verified_csv_rows": sum(int(r.get("rows", 0)) for r in rows),
        "network_requests": network_requests,
        "network_response_bytes": network_bytes,
        "market_source_manifest_sha256": common.sha(manifest),
        "offline": offline,
        "failed_validation_or_fetch": failures,
        "genuine_historical_actual_receipt": "UNKNOWN_NOT_CAPTURED_AT_ACQUISITION",
    }
    if not offline:
        common.save(common.HERE / "SOURCE_ACQUISITION_SUMMARY.json", result)
    if failures:
        raise ValueError("SOURCE_GATE_FAILED:" + common.canonical(result))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache", type=pathlib.Path, required=True)
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    print(common.canonical(acquire(args.cache, offline=args.offline)))
