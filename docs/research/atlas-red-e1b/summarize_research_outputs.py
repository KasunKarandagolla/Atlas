#!/usr/bin/env python3
"""Validate and summarize deterministic R-E1B outputs after a full rebuild."""
from __future__ import annotations

import collections
import csv
import gzip
import hashlib
import json
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from acquire_archives import COHORT, COHORT_SHA256  # noqa: E402
from build_research_panel import WINDOWS, ns  # noqa: E402
from make_compact_sample import make_sample  # noqa: E402


def digest(path: pathlib.Path) -> dict[str, object]:
    return {"path": path.name, "bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def run() -> dict[str, object]:
    panel_path = HERE / "DIAGNOSTIC_FEATURE_PANEL.csv.gz"
    coverage_path = HERE / "ASOF_COVERAGE.csv.gz"
    source_path = HERE / "SOURCE_MANIFEST.jsonl"
    sample_path = HERE / "COMPACT_REAL_MULTI_SYMBOL_SAMPLE.csv.gz"
    sample_info = make_sample(panel_path, sample_path)

    panel_rows = collections.Counter()
    s1_states = collections.Counter()
    s1_triggers = collections.Counter()
    s6_states = collections.Counter()
    panel_missing = collections.Counter()
    with gzip.open(panel_path, "rt", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            window = row["window"]
            panel_rows[window] += 1
            s1_states[(window, row["s1_precursor_state"])] += 1
            s1_triggers[(window, row["s1_trigger_state"])] += 1
            s6_states[(window, row["s6_state"])] += 1
            if row["source_status"] == "SOURCE_MISSING":
                panel_missing[(window, row["source_missing_reason"])] += 1

    coverage_rows = collections.Counter()
    coverage_missing = collections.Counter()
    with gzip.open(coverage_path, "rt", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            key = (row["window"], row["interval"])
            coverage_rows[key] += 1
            if row["expected_status"] == "SOURCE_MISSING":
                coverage_missing[(row["window"], row["interval"], row["reason"])] += 1

    source_rows = [json.loads(line) for line in source_path.read_text().splitlines() if line]
    statuses = collections.Counter(row.get("status") for row in source_rows)
    archives = [row for row in source_rows if row.get("status") == "VERIFIED"]
    archive_counts = collections.Counter(row["interval"] for row in archives)
    archive_bytes = sum(int(row["archive_bytes"]) for row in archives)
    archive_bytes_by_role = collections.Counter()
    for row in archives:
        role = "H1" if row["interval"] == "1h" else "M15_EVALUATION" if row["month"][5:] != "02" else "M15_PREWINDOW_SEED"
        archive_bytes_by_role[role] += int(row["archive_bytes"])
    archive_rows = sum(int(row["rows"]) for row in archives)
    rejected = statuses["PARSER_REJECTED_PRECEDING_ATTEMPT"]

    expected_panel = len(COHORT) * sum((ns(ey, em, ed + 1) - ns(sy, sm, sd)) // (15 * 60 * 1_000_000_000)
                                     for _, sy, sm, sd, ey, em, ed in WINDOWS)
    if sum(panel_rows.values()) != expected_panel:
        raise ValueError(f"PANEL_DENOMINATOR_MISMATCH:{sum(panel_rows.values())}:{expected_panel}")
    for window, count in panel_rows.items():
        if count != expected_panel // 2 or coverage_rows[(window, "15m")] != count:
            raise ValueError(f"PANEL_COVERAGE_COUNT_MISMATCH:{window}:{count}:{coverage_rows[(window, '15m')]}")
    for (window, reason), count in panel_missing.items():
        if coverage_missing[(window, "15m", reason)] != count:
            raise ValueError(f"PANEL_COVERAGE_MISSINGNESS_MISMATCH:{window}:{reason}")

    report = {
        "dataset": "ATLAS_RED_LUNA_MEDIUM_REAL_BINANCE_COHORT_R_E1B",
        "model_effort_attestation": "UNVERIFIED: no independent model/effort attestation was supplied",
        "cohort_sha256": COHORT_SHA256,
        "cohort_symbols": COHORT,
        "windows_utc": {label: {"decision_cutoff_begin_ns": ns(sy, sm, sd),
                                "decision_cutoff_final_exclusive_ns": ns(ey, em, ed + 1)}
                        for label, sy, sm, sd, ey, em, ed in WINDOWS},
        "boundary_convention": "Decision cutoffs are native close boundaries in [begin, final); slot_open=decision_cutoff-native_interval. First cutoff includes the prior February seed bar; slots closing at/after final are excluded.",
        "source_acquisition": {
            "manifest_status_counts_including_prior_sample": dict(sorted(statuses.items())),
            "verified_archive_counts_by_interval": dict(sorted(archive_counts.items())),
            "verified_unique_archive_bytes_not_retry_deduplicated": archive_bytes,
            "verified_archive_bytes_by_role": dict(sorted(archive_bytes_by_role.items())),
            "verified_csv_rows": archive_rows,
            "initial_parser_rejected_attempt_rows": rejected,
            "initial_parser_rejected_download_bytes": "NOT_CAPTURED; not added to accepted-archive byte totals",
            "archive_limits": {"max_single_object_bytes": 20 * 1024 * 1024,
                                "max_aggregate_archive_bytes": 1024 * 1024 * 1024,
                                "max_concurrent_fetches": 4},
        },
        "panel": {
            "rows_by_window": dict(sorted(panel_rows.items())),
            "rows_total": sum(panel_rows.values()),
            "s1_precursor_state_counts": {f"{w}:{s}": n for (w, s), n in sorted(s1_states.items())},
            "s1_trigger_state_counts": {f"{w}:{s}": n for (w, s), n in sorted(s1_triggers.items())},
            "s6_state_counts": {f"{w}:{s}": n for (w, s), n in sorted(s6_states.items())},
            "m15_source_missing_counts": {f"{w}:{r}": n for (w, r), n in sorted(panel_missing.items())},
            "full_panel_git_status": "OMITTED_FROM_COMMIT: compressed full panel exceeds GitHub's 100 MiB per-file limit; regenerate with the command below",
            "full_panel_artifact": digest(panel_path),
            "compact_sample_artifact": sample_info,
        },
        "coverage": {
            "rows_by_window_interval": {f"{w}:{i}": n for (w, i), n in sorted(coverage_rows.items())},
            "rows_total": sum(coverage_rows.values()),
            "missing_counts": {f"{w}:{i}:{r}": n for (w, i, r), n in sorted(coverage_missing.items())},
            "artifact": digest(coverage_path),
        },
        "source_manifest_artifact": digest(source_path),
        "clean_clone_rebuild_tested": False,
        "full_rebuild_command": "python3 docs/research/atlas-red-e1b/build_research_panel.py --scratch /path/to/scratch/e1b --outdir docs/research/atlas-red-e1b",
        "compact_sample_command": "python3 docs/research/atlas-red-e1b/summarize_research_outputs.py",
    }
    output = HERE / "DATASET_MANIFEST.json"
    output.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return report


if __name__ == "__main__":
    print(json.dumps(run(), sort_keys=True))
