#!/usr/bin/env python3
"""Run every repository pytest case offline and retain JUnit/resource evidence."""

from __future__ import annotations

import argparse
import json
import re
from datetime import UTC, datetime
from pathlib import Path

from atlas_s49_offline_integration import ROOT, _invoke


def _mark_interrupted_run(report: dict[str, object]) -> bool:
    """Keep an interrupted pytest run distinct from a completed test result."""
    result = report.get("suite_result")
    if not isinstance(result, dict) or result.get("return_code") not in (137, -9):
        return False
    log_path = ROOT / str(result.get("log", ""))
    if not log_path.is_file():
        return False
    output = log_path.read_text(encoding="utf-8", errors="replace")
    progress = re.findall(r"^([.FEsxX]+)\s+\[\s*(\d+)%\]\s*$", output, re.MULTILINE)
    if not progress:
        return False
    marked_lines = re.findall(r"^([.FEsxX]+)(?:\s+\[\s*\d+%\])?\s*$", output, re.MULTILINE)
    marks = "".join(marked_lines)
    counts = {mark: marks.count(mark) for mark in ".FEsxX"}
    collection_path = ROOT / "docs/v2/transition-s49-evidence/full-repository-collection.json"
    collected = None
    if collection_path.is_file():
        collection = json.loads(collection_path.read_text(encoding="utf-8"))
        collected = collection.get("collected_tests")
    result.update(
        {
            "status": "INCOMPLETE",
            "tests": 0,
            "failures": 0,
            "summary_complete": False,
            "collected_tests": collected,
            "tests_reached_before_interrupt": len(marks),
            "reached_percent": round(len(marks) / collected * 100, 1) if collected else None,
            "partial_marker_counts": {
                "passed": counts["."],
                "failures": counts["F"],
                "errors": counts["E"],
                "skipped": counts["s"],
                "xfailed": counts["x"],
                "xpassed": counts["X"],
            },
            "last_flushed_progress_percent": int(progress[-1][1]),
            "interrupt_reason": "bounded one-hour run stopped during a long historical fixture; no complete JUnit summary",
        }
    )
    report["completion"] = "INCOMPLETE: no repository-wide pass/fail total is available"
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--evidence-dir",
        type=Path,
        default=ROOT / "docs/v2/transition-s49-evidence/full-repository",
    )
    args = parser.parse_args()
    evidence = args.evidence_dir.resolve()
    result = _invoke("full-repository-pytest", ("tests",), evidence)
    report = {
        "schema": "ATLAS_S49_FULL_REPOSITORY_TEST_V1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "offline": True,
        "network_guard": "BUBBLEWRAP_UNSHARE_NET plus Python socket/URL non-loopback guard",
        "provider_calls": 0,
        "authenticated_exchange_calls": 0,
        "suite_result": result,
    }
    _mark_interrupted_run(report)
    evidence.mkdir(parents=True, exist_ok=True)
    (evidence / "full_repository_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        f"[S49] full repository: {result['status']} "
        f"({result.get('tests', 0)} tests, {result.get('failures', 0)} failures, "
        f"{result['duration_seconds']}s)",
        flush=True,
    )
    if result["status"] == "PASS":
        return 0
    return 2 if result["status"] == "INCOMPLETE" else 1


if __name__ == "__main__":
    raise SystemExit(main())
