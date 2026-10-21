"""Verify temporary-only layout; optionally exercise a small disposable kernel quota."""

from __future__ import annotations

import argparse
import errno
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from atlas.v2.research.full_cycle.storage import StorageLayoutV2  # noqa:E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exercise-small-quota", action="store_true")
    args = parser.parse_args()
    layout = StorageLayoutV2(
        Path(os.environ["ATLAS_TEMP_ROOT"]),
        Path(os.environ["ATLAS_PERMANENT_ROOT"]),
        external_temporary_bytes=int(os.environ.get("ATLAS_EXTERNAL_TEMPORARY_BYTES", "0")),
    )
    result = layout.verify()
    with tempfile.TemporaryDirectory(prefix="atlas-own-probe-", dir=layout.temporary_root) as own:
        db = Path(own) / "crash.sqlite"
        script = "import sqlite3,os,sys; c=sqlite3.connect(sys.argv[1]); c.execute('pragma journal_mode=wal'); c.execute('pragma synchronous=full'); c.execute('create table evidence(id integer primary key)'); c.execute('insert into evidence values(1)'); c.commit(); c.execute('insert into evidence values(2)'); os._exit(0)"
        subprocess.run([sys.executable, "-c", script, str(db)], check=True)
        with sqlite3.connect(db) as c:
            assert c.execute("select count(*) from evidence").fetchone()[0] == 1
            assert c.execute("pragma integrity_check").fetchone()[0] == "ok"
        result["sqlite_wal_crash_recovery"] = "PASS_COMMITTED_ONLY"
        if args.exercise_small_quota:
            if result["temporary_capacity_bytes"] > 64 * 1024 * 1024:
                raise ValueError("Quota exercise requires a disposable mount of at most 64 MiB")
            written = 0
            try:
                with (Path(own) / "quota-fill").open("wb", buffering=0) as f:
                    block = b"x" * (1 << 20)
                    while written <= result["temporary_capacity_bytes"]:
                        written += f.write(block)
            except OSError as exc:
                if exc.errno != errno.ENOSPC:
                    raise
                result["kernel_overquota_write"] = "REJECTED_ENOSPC"
                result["written_before_enospc_bytes"] = written
            if "kernel_overquota_write" not in result:
                raise AssertionError("Kernel quota was not enforced")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
