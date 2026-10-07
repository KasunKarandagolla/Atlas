"""Run an attributable S41 validation command and preserve completion evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SUITES = {
    "v1": ["tests", "--ignore=tests/v2"],
    "v2": ["tests/v2"],
    "s41": ["tests/runtime", "tests/v2", "-k", "session041"],
}


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_identity() -> dict[str, object]:
    names = subprocess.check_output(
        ["git", "ls-files", "-c", "-o", "--exclude-standard"], cwd=ROOT, text=True).splitlines()
    names.extend(("src/atlas/v2/data/broad_public_source.py", "src/atlas/v2/data/broad_stream_source.py"))
    files = {name: digest(ROOT / name) for name in sorted(set(names))
        if (ROOT / name).is_file() and (
            name.startswith(("src/", "tests/", "scripts/", "configs/", ".github/"))
            or name.startswith("requirements") or name == "pyproject.toml")}
    canonical = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
    return {"head_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "file_sha256": files, "tree_sha256": hashlib.sha256(canonical).hexdigest()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", choices=SUITES, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    xml = args.output / f"{args.suite}.xml"
    log = args.output / f"{args.suite}.log"
    result_path = args.output / f"{args.suite}.json"
    command = [sys.executable, "-m", "pytest", *SUITES[args.suite], "-q", f"--junitxml={xml}"]
    before = source_identity()
    started = time.time_ns()
    result: dict[str, object] = {"schema_version": 1, "suite": args.suite, "command": command,
        "started_at_ns": started, "source_before": before, "status": "UNVERIFIED", "completed": False}
    result_path.write_text(json.dumps(result, indent=2) + "\n")
    with log.open("wb") as handle:
        code = subprocess.run(command, cwd=ROOT, stdout=handle, stderr=subprocess.STDOUT, check=False).returncode
    after = source_identity()
    result.update(completed=True, finished_at_ns=time.time_ns(), exit_code=code,
        source_after=after, source_stable=before == after, log_sha256=digest(log))
    if xml.exists():
        root = ET.parse(xml).getroot()
        totals = {key: sum(int(s.get(key, "0")) for s in root.iter("testsuite"))
                  for key in ("tests", "failures", "errors", "skipped")}
        result.update(junit_sha256=digest(xml), totals=totals,
            skips=[{"name": case.get("name"), "class": case.get("classname"),
                    "reason": case.find("skipped").get("message")}
                   for case in root.iter("testcase") if case.find("skipped") is not None])
        result["status"] = "TESTED" if code == 0 and before == after else "UNVERIFIED"
    result_path.write_text(json.dumps(result, indent=2) + "\n")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
