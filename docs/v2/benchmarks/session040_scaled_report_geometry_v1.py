"""Synthetic 48-hour projection geometry; no genuine market/endurance claim.

Run with PYTHONPATH=src and the locked runtime:
python docs/v2/benchmarks/session040_scaled_report_geometry_v1.py --root NEW_DIRECTORY
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import sqlite3
import subprocess
import time
from pathlib import Path

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.product import resource_sample
from atlas.v2.science.tuning_export import (
    TuningRunIdentityV1,
    _ProjectionCursor,
    export_tuning_snapshot,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--resume-synthetic-population", action="store_true")
    parser.add_argument("--projection-rows-per-second", type=int, default=16)
    args = parser.parse_args()
    if not args.resume_synthetic_population:
        args.root.mkdir(parents=True, exist_ok=False)
    else:
        assert args.root.is_dir() and (args.root / "interrupted-population-attempt.json").is_file()
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    assert 1 <= args.projection_rows_per_second <= 32
    # The owner profile has four BTC/ETH book/trade channels. A one-second
    # health + continuity publication contributes eight compact rows/s;
    # default sixteen provides 2x ordinary-rate geometry headroom. This does
    # not count every raw frame as a compact analytical row.
    count = args.projection_rows_per_second * 48 * 60 * 60
    database = args.root / "ops.sqlite"
    result = {"schema_version": 1, "status": "TEST GATE", "implementation_sha": sha,
        "scope": "SYNTHETIC_PROJECTION_GEOMETRY_ONLY", "projection_rows": count,
        "raw_padding_rows": 200_000, "represented_seconds": 48 * 60 * 60,
        "declared_projection_rows_per_second": args.projection_rows_per_second,
        "scale_basis": "4 channels * 2 compact reports/s * 2x geometry margin",
        "capital_enabled": False, "assisted_enabled": False, "authority": "ZERO",
        "source_or_48h_endurance_qualification": "TEST GATE", "measurements": []}
    body = {"status": "SYNTHETIC_S40_SCALE_FIXTURE", "rss_bytes": 123456,
            "wal_bytes": 100, "cpu_percent": 2.5}
    started = time.monotonic()
    try:
        with OpsRepository(database) as writer:
            connection = writer._connection
            existing = connection.execute("SELECT count(*) FROM artifact_index").fetchone()[0]
            if args.resume_synthetic_population:
                assert 0 < existing < count and existing % 1024 == 0
                assert connection.execute("SELECT count(*) FROM artifact_index WHERE artifact_type "
                    "!= 'ResearchResourceSampleV1'").fetchone()[0] == 0
                for rowid in (1, existing):
                    record = connection.execute("SELECT metadata_json,created_at_ns FROM artifact_index "
                        "WHERE rowid=?", (rowid,)).fetchone()
                    assert json.loads(record[0]) == body and record[1] == rowid
            else:
                assert existing == 0
            result["resumed_synthetic_rows"] = existing
            # These are declared synthetic fixture rows. The same simple typed
            # resource projection is used by the existing exporter tests. They
            # are not transplanted genuine book/continuity/decision evidence.
            for low in range(existing, count, 1024):
                rows = []
                for index in range(low, min(count, low + 1024)):
                    at = index + 1
                    ref = sha256_json({"at": at, "body": body})
                    rows.append((ref, "ResearchResourceSampleV1", ref, at, at, canonical_json(body)))
                with writer.atomic_composition():
                    connection.executemany("INSERT INTO artifact_index(artifact_ref,artifact_type,content_hash,"
                        "created_at_ns,available_at_ns,metadata_json) VALUES(?,?,?,?,?,?)", rows)
                if low % 65536 == 0:
                    print(json.dumps({"inserted": min(count, low + 1024)}), flush=True)
            with writer.atomic_composition():
                connection.execute("WITH RECURSIVE padding(n) AS (SELECT 1 UNION ALL "
                    "SELECT n+1 FROM padding WHERE n<200000) "
                    "INSERT INTO artifact_index(artifact_ref,artifact_type,content_hash,"
                    "created_at_ns,available_at_ns,metadata_json) "
                    "SELECT printf('%064x',n),'S40_SYNTHETIC_RAW_PADDING_V1',printf('%064x',n),1,1,'{}' FROM padding")
            result["population_seconds"] = time.monotonic() - started
            total = count + 200_000
            for after in (0, count // 2, count - 512):
                work = [0]

                def budget(work=work) -> int:
                    work[0] += 100
                    return int(work[0] > 100_000)

                connection.set_progress_handler(budget, 100)
                at = time.monotonic()
                try:
                    cursor = _ProjectionCursor(connection, after=after, through=total, limit=512)
                    rows = cursor.fetchmany(512)
                    assert len(rows) == 512
                    assert [row["source_rowid"] for row in rows] == list(range(after + 1, after + 513))
                    assert cursor.has_more is (after + 512 < count)
                finally:
                    connection.set_progress_handler(None, 0)
                result["measurements"].append({"cursor_after": after, "rows": len(rows),
                    "sqlite_vm_operations_upper_bound": work[0], "seconds": time.monotonic() - at})
            writer.checkpoint()
        identity = TuningRunIdentityV1("s40-scaled-synthetic", "a" * 64, sha, 0)
        previous = None
        exports = []
        original_snapshot = OpsRepository.read_snapshot
        snapshot_seconds = []

        @contextlib.contextmanager
        def timed_snapshot(self):
            at = time.monotonic()
            try:
                with original_snapshot(self) as connection:
                    yield connection
            finally:
                snapshot_seconds.append(time.monotonic() - at)

        OpsRepository.read_snapshot = timed_snapshot
        for page in range(8):
            at = time.monotonic()
            snapshot_seconds.clear()
            exported = export_tuning_snapshot(database, args.root / "reports", identity,
                cutoff_ns=count + 1, max_rows=512)
            assert not exported["validation_failures"] and exported["rows_written"] == 512
            assert exported["has_more"]
            assert sum(snapshot_seconds) < 5, "Scaled read snapshot lacks fixed-budget safety margin"
            if previous is not None:
                assert exported["after_rowid"] == previous["through_rowid"]
                assert exported["previous_manifest_sha256"] == sha256_json(previous)
            exports.append({"page": page, "seconds": time.monotonic() - at,
                "snapshot_seconds": sum(snapshot_seconds),
                "after_rowid": exported["after_rowid"], "through_rowid": exported["through_rowid"],
                "rows": exported["rows_written"], "has_more": exported["has_more"],
                "validation_failures": exported["validation_failures"],
                "manifest_sha256": sha256_json(exported)})
            previous = exported
        OpsRepository.read_snapshot = original_snapshot
        with sqlite3.connect("file:" + str(database) + "?mode=ro", uri=True) as reader:
            assert reader.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            assert reader.execute("SELECT count(*) FROM artifact_index").fetchone()[0] == total
        result.update(status="TESTED", exports=exports, sqlite_bytes=database.stat().st_size,
            resources=resource_sample(args.root), elapsed_seconds=time.monotonic() - started,
            benchmark_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            limitations=["Compact typed projection/index/manifest geometry, not 691200 genuine continuity records",
                "Complex exact raw/book dependency validation is covered by separate production/native fixtures",
                "Eight genuine incremental pages with explicit backlog, not an instant whole-campaign export",
                "No live provider/network, Windows11 host or 48-hour actual-wall qualification"])
    except Exception as error:
        result.update(error_type=type(error).__name__, elapsed_seconds=time.monotonic() - started)
        raise
    finally:
        (args.root / "scaled-export-result.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"status": result["status"], "result": str(args.root / "scaled-export-result.json")}), flush=True)


if __name__ == "__main__":
    main()
