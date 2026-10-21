#!/usr/bin/env python3
"""Official funding/mark acquisition and conditional funding cash intervals.

calc_time is preserved. Treating it as the settlement instant is an explicit
assumption, not proof of an account payment or original-time availability.
"""

import argparse
import concurrent.futures
import csv
import io
import json
import sys
import time
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from atlas.v2.research.full_cycle.historical_archive import digest, get, read_zip  # noqa: E402

parser = argparse.ArgumentParser()
parser.add_argument("--permanent-root", type=Path, required=True)
parser.add_argument(
    "--output", type=Path, default=ROOT / "docs/research/atlas-red-historical-economic-investigation-2026-10-21"
)
parser.add_argument("--phase", choices=("acquire", "bounds"), required=True)
args = parser.parse_args()
out = args.output
root = args.permanent_root / "aux"
root.mkdir(exist_ok=True)
symbols = json.loads((out / "COHORT_SELECTION.json").read_text())["selected"]


def one(task):
    s, m, kind = task
    name = f"{s}-fundingRate-{m}.zip" if kind == "fundingRate" else f"{s}-15m-{m}.zip"
    sub = f"{s}/{name}" if kind == "fundingRate" else f"{s}/15m/{name}"
    u = f"https://data.binance.vision/data/futures/um/monthly/{kind}/{sub}"
    p = root / kind
    p.mkdir(exist_ok=True)
    meta = p / (name + ".json")
    if meta.exists():
        return json.loads(meta.read_text())
    r = {"symbol": s, "month": m, "type": kind, "url": u}
    try:
        c, cr = get(u + ".CHECKSUM", 4096)
        b, br = get(u, 2000000)
        assert digest(b) == c.decode().split()[0]
        if kind == "markPriceKlines":
            _, rows, info = read_zip(b, name, "15m", m)
        else:
            with zipfile.ZipFile(io.BytesIO(b)) as z:
                assert len(z.infolist()) == 1 and z.infolist()[0].file_size < 100000
                assert z.namelist() == [name.removesuffix(".zip") + ".csv"]
                body = z.read(z.namelist()[0])
                rows = list(csv.DictReader(io.StringIO(body.decode())))
                assert all(int(x["funding_interval_hours"]) > 0 for x in rows)
                times = [int(x["calc_time"]) for x in rows]
                assert times == sorted(set(times))
                info = {
                    "csv_sha256": digest(body),
                    "rows": len(rows),
                    "first_calc_time_ms": times[0],
                    "last_calc_time_ms": times[-1],
                    "fields": ["calc_time", "funding_interval_hours", "last_funding_rate"],
                    "timestamp_unit": "UTC milliseconds; original 0/1ms offsets preserved",
                }
        for n, data in ((name, b), (name + ".CHECKSUM", c)):
            q = p / n
            if q.exists():
                assert q.read_bytes() == data
            else:
                q.write_bytes(data)
        r.update(
            status="VERIFIED",
            archive_sha256=digest(b),
            official_sha256=c.decode().split()[0],
            archive_bytes=len(b),
            requests=[cr, br],
            info=info,
        )
    except Exception as e:
        r.update(status="FAILED", error=str(e), attempted_at_ns=time.time_ns())
    meta.write_text(json.dumps(r, indent=2) + "\n")
    return r


if args.phase == "acquire":
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        records = list(
            pool.map(
                one,
                [
                    (s, m, k)
                    for s in symbols
                    for m in ("2024-07", "2024-08", "2024-09")
                    for k in ("fundingRate", "markPriceKlines")
                ],
            )
        )
    (out / "AUXILIARY_SOURCES.json").write_text(json.dumps(records, indent=2, sort_keys=True) + "\n")
    print(
        json.dumps(
            {
                "sources": len(records),
                "verified": sum(x["status"] == "VERIFIED" for x in records),
                "bytes": sum(x.get("archive_bytes", 0) for x in records),
            }
        )
    )
else:
    from atlas.v2.research.full_cycle.historical_economics import funding_bounds_report

    funding_bounds_report(args.permanent_root, out)
