#!/usr/bin/env python3
"""Bounded archive pilot, invoking ATLAS native research owners. No live access."""

from __future__ import annotations

import argparse
import concurrent.futures
import importlib.util
import json
import os
import resource
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from atlas.v2.research.full_cycle.historical_archive import acquire, read_zip  # noqa: E402

PACKAGE = ROOT / "docs/research/atlas-red-historical-economic-investigation-2026-10-21"


def write_json(path, data):
    path.write_text(json.dumps(data, indent=2, sort_keys=True, default=str) + "\n")


def fetch_many(root, tasks):
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(acquire, root, *t) for t in tasks]
        records = [f.result() for f in futures]
    return sorted(records, key=lambda r: (r["symbol"], r["interval"], r["month"]))


def acquisition(root, out):
    spec = importlib.util.spec_from_file_location(
        "prior_archive", ROOT / "docs/research/atlas-red-e1b/acquire_archives.py"
    )
    prior = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(prior)
    records = fetch_many(root, [(s, "15m", "2024-06") for s in prior.COHORT])
    write_json(out / "selection_sources.json", records)
    ranking = []
    for r in records:
        if r["status"] != "VERIFIED":
            continue
        name = r["url"].rsplit("/", 1)[1]
        _, rows, _ = read_zip((root / name).read_bytes(), name, "15m", "2024-06")
        from decimal import Decimal

        turnover = sum((Decimal(x[7]) for x in rows), Decimal(0))
        if r["symbol"] not in ("BTCUSDT", "ETHUSDT"):
            ranking.append((turnover, r["symbol"]))
    ranking.sort(key=lambda x: (-x[0], x[1]))
    selected = ["BTCUSDT", "ETHUSDT"]
    selected += [ranking[i - 1][1] for i in (1, 9, 17, 25, 33) if i <= len(ranking)]
    selected = list(dict.fromkeys(selected + ["MATICUSDT", "EOSUSDT", "FTMUSDT"]))
    selection = {
        "frame": prior.COHORT,
        "frame_sha256": prior.COHORT_SHA256,
        "ranked_june_2024_quote_turnover": [
            {"rank": i + 1, "symbol": s, "quote_turnover": str(q)} for i, (q, s) in enumerate(ranking)
        ],
        "selected": selected,
        "selection_rules": "PROTOCOL.md; ranks 1,9,17,25,33 plus BTC/ETH/MATIC/EOS/FTM",
        "not_a_point_in_time_full_universe": True,
    }
    write_json(out / "COHORT_SELECTION.json", selection)
    print("Frozen selected cohort:", ",".join(selected), flush=True)
    tasks = [
        (s, interval, m)
        for s in selected
        for interval in ("15m", "1h")
        for m in ("2024-05", "2024-06", "2024-07", "2024-08", "2024-09")
    ]
    # One extra completed month allows maturation; only first four hours consumed.
    records += fetch_many(
        root, [t for t in tasks if t not in [(r["symbol"], r["interval"], r["month"]) for r in records]]
    )
    records = sorted(records, key=lambda r: (r["symbol"], r["interval"], r["month"]))
    write_json(out / "archive_sources.json", records)
    print(
        json.dumps(
            {
                "archives": len(records),
                "verified": sum(r["status"] == "VERIFIED" for r in records),
                "rows": sum(r.get("rows", 0) for r in records),
                "zip_bytes": sum(r.get("archive_bytes", 0) for r in records),
            }
        ),
        flush=True,
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--raw-root", type=Path, required=True)
    p.add_argument("--output", type=Path, default=PACKAGE)
    p.add_argument("--phase", choices=("acquire", "july", "all"), required=True)
    a = p.parse_args()
    a.output.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    stop = threading.Event()
    temporary = Path(os.environ["ATLAS_TEMP_ROOT"])
    sampled = {"tmpfs_peak_used_bytes": 0, "physical_free_min_bytes": 2**63 - 1, "samples": 0}

    def monitor():
        while not stop.is_set():
            st = os.statvfs(temporary)
            sampled["tmpfs_peak_used_bytes"] = max(
                sampled["tmpfs_peak_used_bytes"], (st.f_blocks - st.f_bfree) * st.f_frsize
            )
            ps = os.statvfs(a.raw_root)
            sampled["physical_free_min_bytes"] = min(sampled["physical_free_min_bytes"], ps.f_bavail * ps.f_frsize)
            sampled["tmpfs_capacity_bytes"] = st.f_blocks * st.f_frsize
            sampled["samples"] += 1
            stop.wait(0.1)

    t = threading.Thread(target=monitor, daemon=True)
    t.start()
    if a.phase == "acquire":
        acquisition(a.raw_root, a.output)
    else:
        from atlas.v2.research.full_cycle.historical_economics import evaluate

        evaluate(a.raw_root, a.output, holdout=a.phase == "all")
    stop.set()
    t.join()
    sampled.update(
        external_temporary_reservation_bytes=200_000_000,
        physical_reserve_bytes=2_000_000_000,
        measured_peak_is_sampled=True,
        sample_interval_seconds=0.1,
        raw_root=str(a.raw_root),
        permanent_root=str(a.raw_root.parent),
    )
    write_json(a.raw_root.parent / f"resources-{a.phase}.json", sampled)
    print(
        json.dumps(
            {
                "phase": a.phase,
                "wall_seconds": time.monotonic() - started,
                "maxrss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
