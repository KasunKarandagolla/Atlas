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
from collections.abc import Iterable

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from acquire_archives import COHORT, COHORT_SHA256  # noqa: E402
from build_research_panel import WINDOWS, ns  # noqa: E402
from make_compact_sample import make_sample  # noqa: E402

PRIOR_CHECKPOINT = {
    "commit": "ab645e792fe6593d9fb206954585b2996673f96c",
    "dataset_manifest_sha256": "f22f7e16ab8fd9d92b84b8df51cf9d6e662d1ffc03465acdcc5d9e7c8aa89e0e",
    "verified_archives": 757,
    "official_404_objects": 13,
    "parser_rejected_attempt_rows": 420,
    "parser_rejected_attempt_download_bytes": "NOT_CAPTURED_BY_REJECTED_PARSER",
}


def reconcile_s1_trigger_rows(
    rows: Iterable[dict[str, str]],
    window_bounds: dict[str, tuple[int, int]],
    m15_ns: int,
) -> dict[str, dict[str, object]]:
    """Reconcile setup identities separately from M15 observation rows."""
    setups: dict[tuple[str, str, str], dict[str, object]] = {}
    counters: dict[tuple[str, str], collections.Counter[str]] = collections.defaultdict(collections.Counter)
    missing_rows: collections.Counter[tuple[str, str, str]] = collections.Counter()

    def add_setup_id(key: tuple[str, str], metric: str, setup_id: str) -> None:
        bucket = counters[key].setdefault(metric, set())
        bucket.add(setup_id)

    def record_for(key: tuple[str, str, str]) -> dict[str, object]:
        return setups.setdefault(key, {
            "setup_cutoff": None, "lineage": None, "pre_window_status": None,
            "opportunities": {}, "first_event_ids": set(), "first_event_cutoffs": set(),
            "episode_statuses": set(), "raw_positive_rows": 0, "positive_after_censor_rows": 0,
            "censored_pre_window": False, "censored_active_window": False,
            "previously_established": False,
        })

    def set_consistent(record: dict[str, object], field: str, value: object, key: tuple[str, str, str]) -> None:
        if value in (None, ""):
            return
        existing = record[field]
        if existing is not None and existing != value:
            raise ValueError(f"SETUP_IDENTITY_INCONSISTENT:{key}:{field}:{existing}:{value}")
        record[field] = value

    for row in rows:
        window, symbol = row["window"], row["symbol"]
        cutoff = int(row["decision_cutoff_utc_ns"])
        key_ws = (window, symbol)
        counters[key_ws]["m15_rows"] += 1
        raw = int(row["s1_raw_trigger_predicate"])
        if raw not in (0, 1):
            raise ValueError(f"INVALID_RAW_TRIGGER_PREDICATE:{key_ws}:{cutoff}:{raw}")
        counters[key_ws]["raw_positive_predicate_rows"] += raw
        if row["s1_trigger_state"] == "SOURCE_MISSING":
            reason = row["s1_trigger_source_missing_reason"] or "UNSPECIFIED"
            counters[key_ws]["source_missing_trigger_rows"] += 1
            missing_rows[(window, symbol, reason)] += 1
        if raw and (row["s1_trigger_window_active"] != "1" or row["s1_trigger_state"] != "BAR_TRIGGER"):
            raise ValueError(f"RAW_TRIGGER_WITHOUT_OBSERVED_ACTIVE_BAR:{key_ws}:{cutoff}")

        candidates = []
        if row.get("s1_setup_event_id"):
            candidates.append((row["s1_setup_event_id"], row.get("s1_setup_event_cutoff_utc_ns", ""),
                              (row.get("s1_setup_event_side", ""), row.get("s1_setup_event_extreme", ""),
                               row.get("s1_setup_event_ref", "")), None))
        if row.get("s1_trigger_setup_id"):
            candidates.append((row["s1_trigger_setup_id"], row.get("s1_trigger_setup_cutoff_utc_ns", ""),
                              (row.get("s1_trigger_setup_side", ""), row.get("s1_trigger_setup_extreme", ""),
                               row.get("s1_trigger_setup_ref", "")), row))
        for setup_id, origin_cutoff, lineage, trigger_row in candidates:
            key = (window, symbol, setup_id)
            record = record_for(key)
            set_consistent(record, "setup_cutoff", int(origin_cutoff) if origin_cutoff else None, key)
            if all(lineage):
                set_consistent(record, "lineage", lineage, key)
            if trigger_row is None:
                continue
            pre_status = trigger_row.get("s1_trigger_pre_window_history_status", "")
            if pre_status:
                set_consistent(record, "pre_window_status", pre_status, key)
            episode = trigger_row.get("s1_trigger_episode_status", "")
            if episode:
                record["episode_statuses"].add(episode)
            if episode in {"CENSORED_PREWINDOW", "CENSORED_PREWINDOW_AND_ACTIVE"}:
                record["censored_pre_window"] = True
            if episode in {"CENSORED_ACTIVE", "CENSORED_PREWINDOW_AND_ACTIVE"}:
                record["censored_active_window"] = True
            if pre_status == "PRIOR_TRIGGER_HISTORY_MISSING":
                record["censored_pre_window"] = True
            if pre_status == "TRIGGERED_BEFORE_WINDOW" or episode in {
                "TRIGGERED_BEFORE_WINDOW", "TRIGGER_ALREADY_ESTABLISHED"
            }:
                record["previously_established"] = True
            if raw:
                record["raw_positive_rows"] += 1
                if episode.startswith("CENSORED_"):
                    record["positive_after_censor_rows"] += 1
            if trigger_row.get("s1_trigger_window_active") == "1":
                opportunities = record["opportunities"]
                if cutoff in opportunities:
                    raise ValueError(f"DUPLICATE_SETUP_OPPORTUNITY:{key}:{cutoff}")
                opportunities[cutoff] = (trigger_row["s1_trigger_state"], raw, episode)
            if trigger_row.get("s1_first_trigger_event") == "1":
                event_id = trigger_row.get("s1_first_trigger_event_id", "")
                required = (setup_id, origin_cutoff, *lineage, event_id)
                if not all(required) or trigger_row.get("s1_trigger_window_active") != "1":
                    raise ValueError(f"FIRST_TRIGGER_LINEAGE_INCOMPLETE:{key}:{cutoff}:{required}")
                expected_event_id = hashlib.sha256(f"{setup_id}:{cutoff}".encode()).hexdigest()
                if event_id != expected_event_id:
                    raise ValueError(f"FIRST_TRIGGER_EVENT_ID_MISMATCH:{key}:{cutoff}:{event_id}")
                if trigger_row["s1_trigger_state"] != "BAR_TRIGGER" or raw != 1:
                    raise ValueError(f"FIRST_TRIGGER_WITHOUT_RAW_OBSERVED_MATCH:{key}:{cutoff}")
                if episode != "VERIFIED_FIRST_TRIGGER":
                    raise ValueError(f"FIRST_TRIGGER_STATUS_MISMATCH:{key}:{cutoff}:{episode}")
                record["first_event_ids"].add(event_id)
                record["first_event_cutoffs"].add(cutoff)

    for key, record in setups.items():
        if len(record["first_event_ids"]) > 1 or len(record["first_event_cutoffs"]) > 1:
            raise ValueError(f"MULTIPLE_VERIFIED_FIRST_EVENTS_PER_SETUP:{key}")
        if record["first_event_ids"] and (record["censored_pre_window"] or record["censored_active_window"]):
            raise ValueError(f"CENSORED_SETUP_HAS_VERIFIED_FIRST_EVENT:{key}")
        if record["censored_pre_window"] or record["censored_active_window"]:
            add_setup_id(key[:2], "censored_setup_ids", key[2])
        if record["censored_pre_window"]:
            add_setup_id(key[:2], "censored_pre_window_setup_ids", key[2])
        if record["censored_active_window"]:
            add_setup_id(key[:2], "censored_active_window_setup_ids", key[2])
        if record["first_event_ids"]:
            add_setup_id(key[:2], "verified_first_setup_ids", key[2])
        if record["previously_established"]:
            add_setup_id(key[:2], "previously_established_setup_ids", key[2])
        if record["positive_after_censor_rows"]:
            counters[key[:2]]["observed_positive_after_censor_rows"] += record["positive_after_censor_rows"]

        window, _symbol, setup_id = key
        begin, final = window_bounds[window]
        origin = record["setup_cutoff"]
        pre_status = record["pre_window_status"]
        complete_no_match = bool(origin is not None and pre_status == "NO_PRIOR_MATCH"
                                 and not record["censored_pre_window"]
                                 and not record["censored_active_window"]
                                 and not record["raw_positive_rows"]
                                 and not record["first_event_ids"])
        if complete_no_match:
            for opportunity_i in range(1, 5):
                opportunity_cutoff = int(origin) + opportunity_i * m15_ns
                if opportunity_cutoff < begin:
                    continue  # NO_PRIOR_MATCH certifies every prior opportunity was observed as a nonmatch.
                if opportunity_cutoff >= final:
                    complete_no_match = False
                    break
                opportunity = record["opportunities"].get(opportunity_cutoff)
                if opportunity != ("NO_TRIGGER", 0, "OBSERVED_NO_MATCH"):
                    complete_no_match = False
                    break
        if complete_no_match:
            add_setup_id(key[:2], "complete_observed_no_match_setup_ids", setup_id)

        if pre_status == "TRIGGERED_BEFORE_WINDOW":
            outcome = "triggered_before_window"
        elif record["first_event_ids"]:
            outcome = "verified_first_in_window"
        elif record["censored_pre_window"] or record["censored_active_window"]:
            outcome = "censored"
        elif complete_no_match:
            outcome = "complete_observed_no_match"
        else:
            outcome = "incomplete_or_unresolved"
        record["exclusive_outcome"] = outcome
        add_setup_id(key[:2], f"outcome_{outcome}_setup_ids", setup_id)

    output: dict[str, dict[str, object]] = {}
    keys = set(counters) | {key[:2] for key in setups}
    for key in sorted(keys):
        count = counters[key]
        unique_ids = {setup_id for (window, symbol, setup_id) in setups if (window, symbol) == key}
        outcome_fields = {
            "triggered_before_window", "verified_first_in_window", "censored",
            "complete_observed_no_match", "incomplete_or_unresolved",
        }
        outcome_ids = {name: count.get(f"outcome_{name}_setup_ids", set()) for name in outcome_fields}
        partition_total = sum(len(ids) for ids in outcome_ids.values())
        if partition_total != len(unique_ids) or set().union(*outcome_ids.values()) != unique_ids:
            raise ValueError(f"SETUP_OUTCOME_RECONCILIATION_FAILED:{key}:{len(unique_ids)}:{partition_total}")
        output[f"{key[0]}:{key[1]}"] = {
            "m15_row_counts": {
                "rows": count["m15_rows"],
                "raw_positive_predicates": count["raw_positive_predicate_rows"],
                "source_missing_trigger_rows": count["source_missing_trigger_rows"],
                "source_missing_trigger_rows_by_reason": {
                    reason: n for (window, symbol, reason), n in sorted(missing_rows.items())
                    if (window, symbol) == key
                },
                "observed_positive_predicates_after_censorship": count["observed_positive_after_censor_rows"],
            },
            "setup_counts": {
                "unique_setup_identities": len(unique_ids),
                "verified_first_trigger_events": len(count.get("verified_first_setup_ids", set())),
                "censored_by_missing_pre_window_history": len(count.get("censored_pre_window_setup_ids", set())),
                "censored_by_missing_active_window_observation": len(count.get("censored_active_window_setup_ids", set())),
                "censored_by_either_missing_history": len(count.get("censored_setup_ids", set())),
                "complete_observation_no_match": len(count.get("complete_observed_no_match_setup_ids", set())),
                "previously_established_trigger": len(count.get("previously_established_setup_ids", set())),
                "exclusive_outcome_partition": {name: len(ids) for name, ids in sorted(outcome_ids.items())},
                "exclusive_outcome_partition_total": partition_total,
                "exclusive_outcome_partition_reconciles": partition_total == len(unique_ids),
            },
        }
    return output


def digest(path: pathlib.Path) -> dict[str, object]:
    return {"path": path.name, "bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def clean_clone_rebuild_qualified(*, producer_commit_sha: str | None, scratch_empty: bool,
                                 output_empty: bool, official_retrieval_and_checksum_verified: bool) -> bool:
    return bool(producer_commit_sha and scratch_empty and output_empty
                and official_retrieval_and_checksum_verified)


def run(*, outdir: pathlib.Path | None = None, clean_clone_code_sha: str | None = None, clean_scratch_empty_at_start: bool = False,
        clean_output_empty_at_start: bool = False,
        official_anonymous_retrieval_verified: bool = False) -> dict[str, object]:
    directory = outdir or HERE
    panel_path = directory / "DIAGNOSTIC_FEATURE_PANEL.csv.gz"
    coverage_path = directory / "ASOF_COVERAGE.csv.gz"
    source_path = directory / "SOURCE_MANIFEST.jsonl"
    sample_path = directory / "COMPACT_REAL_MULTI_SYMBOL_SAMPLE.csv.gz"
    sample_info = make_sample(panel_path, sample_path)

    panel_rows = collections.Counter()
    s1_states = collections.Counter()
    s1_triggers = collections.Counter()
    s1_setup_state = collections.Counter()
    s1_setup_event_count = collections.Counter()
    s1_raw_predicates = collections.Counter()
    s1_first_events = collections.Counter()
    s1_trigger_source_missing = collections.Counter()
    s1_trigger_missing_reason = collections.Counter()
    s6_unique_hourly = collections.Counter()
    s6_hourly_keys: set[tuple[str, str, int]] = set()
    setup_ids: dict[tuple[str, str], set[str]] = collections.defaultdict(set)
    first_trigger_ids: dict[tuple[str, str], set[str]] = collections.defaultdict(set)
    s6_states = collections.Counter()
    panel_missing = collections.Counter()
    with gzip.open(panel_path, "rt", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            window = row["window"]
            panel_rows[window] += 1
            s1_states[(window, row["s1_precursor_state"])] += 1
            s1_triggers[(window, row["s1_trigger_state"])] += 1
            s1_setup_state[(window, row["symbol"], row["s1_precursor_state"])] += 1
            s1_setup_event_count[(window, row["symbol"])] += int(row["s1_setup_event"])
            s1_raw_predicates[(window, row["symbol"])] += int(row["s1_raw_trigger_predicate"])
            s1_first_events[(window, row["symbol"])] += int(row["s1_first_trigger_event"])
            s1_trigger_source_missing[(window, row["symbol"])] += int(row["s1_trigger_state"] == "SOURCE_MISSING")
            if row["s1_trigger_state"] == "SOURCE_MISSING":
                s1_trigger_missing_reason[(window, row["symbol"], row["s1_trigger_source_missing_reason"])] += 1
            if row["s1_setup_event_id"]:
                setup_ids[(window, row["symbol"])].add(row["s1_setup_event_id"])
            if row["s1_first_trigger_event"] == "1":
                event_key = (window, row["symbol"])
                setup_id = row["s1_trigger_setup_id"]
                if setup_id in first_trigger_ids[event_key]:
                    raise ValueError(f"DUPLICATE_FIRST_TRIGGER_EVENT:{event_key}:{setup_id}")
                first_trigger_ids[event_key].add(setup_id)
            s6_key = (window, row["symbol"], int(row["decision_cutoff_utc_ns"]) // (60 * 60 * 1_000_000_000))
            if s6_key not in s6_hourly_keys:
                s6_hourly_keys.add(s6_key)
                s6_unique_hourly[(window, row["symbol"], row["s6_state"])] += 1
            s6_states[(window, row["s6_state"])] += 1
            if row["source_status"] == "SOURCE_MISSING":
                panel_missing[(window, row["source_missing_reason"])] += 1

    with gzip.open(panel_path, "rt", encoding="utf-8", newline="") as f:
        trigger_reconciliation = reconcile_s1_trigger_rows(
            csv.DictReader(f),
            {label: (ns(sy, sm, sd), ns(ey, em, ed + 1))
             for label, sy, sm, sd, ey, em, ed in WINDOWS},
            15 * 60 * 1_000_000_000,
        )

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
    if any(row.get("downloaded_sha256") != row.get("published_sha256") for row in archives):
        raise ValueError("VERIFIED_ARCHIVE_CHECKSUM_IDENTITY_MISMATCH")
    archive_counts = collections.Counter(row["interval"] for row in archives)
    archive_bytes = sum(int(row["archive_bytes"]) for row in archives)
    archive_bytes_by_role = collections.Counter()
    for row in archives:
        role = "H1" if row["interval"] == "1h" else "M15_EVALUATION" if row["month"][5:] != "02" else "M15_PREWINDOW_SEED"
        archive_bytes_by_role[role] += int(row["archive_bytes"])
    archive_rows = sum(int(row["rows"]) for row in archives)
    current_manifest_rejected = statuses["PARSER_REJECTED_PRECEDING_ATTEMPT"]

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
        "model_effort_attestation": "USER CONFIRMED GPT-6 Luna, Medium selection; runtime model/effort metadata is not exposed here for independent verification",
        "producer_code_sha256": hashlib.sha256((HERE / "build_research_panel.py").read_bytes()).hexdigest(),
        "summarizer_code_sha256": hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
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
            "historical_prior_checkpoint": PRIOR_CHECKPOINT,
            "historical_initial_parser_rejected_attempt_rows": PRIOR_CHECKPOINT["parser_rejected_attempt_rows"],
            "current_source_manifest_parser_rejected_attempt_rows": current_manifest_rejected,
            "historical_initial_parser_rejected_download_bytes": PRIOR_CHECKPOINT["parser_rejected_attempt_download_bytes"],
            "archive_limits": {"max_single_object_bytes": 20 * 1024 * 1024,
                                "max_aggregate_archive_bytes": 1024 * 1024 * 1024,
                                "max_concurrent_fetches": 4},
        },
        "panel": {
            "rows_by_window": dict(sorted(panel_rows.items())),
            "rows_total": sum(panel_rows.values()),
            "s1_precursor_state_counts": {f"{w}:{s}": n for (w, s), n in sorted(s1_states.items())},
            "s1_trigger_state_counts": {f"{w}:{s}": n for (w, s), n in sorted(s1_triggers.items())},
            "s1_setup_state_counts_by_window_symbol": {f"{w}:{symbol}:{state}": n for (w, symbol, state), n in sorted(s1_setup_state.items())},
            "s1_setup_event_counts_by_window_symbol": {f"{w}:{symbol}": n for (w, symbol), n in sorted(s1_setup_event_count.items())},
            "s1_unique_setup_id_counts_by_window_symbol": {f"{w}:{symbol}": len(ids) for (w, symbol), ids in sorted(setup_ids.items())},
            "s1_raw_trigger_predicate_counts_by_window_symbol": {f"{w}:{symbol}": n for (w, symbol), n in sorted(s1_raw_predicates.items())},
            "s1_first_trigger_event_counts_by_window_symbol": {f"{w}:{symbol}": n for (w, symbol), n in sorted(s1_first_events.items())},
            "s1_trigger_source_missing_counts_by_window_symbol": {f"{w}:{symbol}": n for (w, symbol), n in sorted(s1_trigger_source_missing.items())},
            "s1_trigger_source_missing_reason_counts_by_window_symbol": {f"{w}:{symbol}:{reason}": n for (w, symbol, reason), n in sorted(s1_trigger_missing_reason.items())},
            "s1_trigger_reconciliation_by_window_symbol": trigger_reconciliation,
            "s1_reconciliation_count_notes": "M15 rows and setup identities use separate denominators. The exclusive outcome partition is mutually exclusive and must reconcile to unique setup identities; censor subcauses and previously-established-trigger diagnostics can overlap that partition.",
            "s6_unique_hourly_cutoff_state_counts": {f"{w}:{symbol}:{state}": n for (w, symbol, state), n in sorted(s6_unique_hourly.items())},
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
        "clean_clone_rebuild_tested": clean_clone_rebuild_qualified(
            producer_commit_sha=clean_clone_code_sha,
            scratch_empty=clean_scratch_empty_at_start,
            output_empty=clean_output_empty_at_start,
            official_retrieval_and_checksum_verified=official_anonymous_retrieval_verified),
        "clean_clone_rebuild": {
            "producer_commit_sha": clean_clone_code_sha or "NOT_RECORDED",
            "clone_depth": 1 if clean_clone_code_sha else None,
            "scratch_empty_at_start": clean_scratch_empty_at_start,
            "output_empty_at_start": clean_output_empty_at_start,
            "official_anonymous_retrieval_and_checksum_verification": official_anonymous_retrieval_verified,
        },
        "prior_parser_rejected_attempts_extraction_command": "git show ab645e792fe6593d9fb206954585b2996673f96c:docs/research/atlas-red-e1b/SOURCE_MANIFEST.jsonl | python3 -c 'import json,sys; [print(json.dumps(r,sort_keys=True,separators=(\",\",\":\"))) for r in map(json.loads,sys.stdin) if r.get(\"status\")==\"PARSER_REJECTED_PRECEDING_ATTEMPT\"]' > /path/to/prior-parser-rejected-attempts.jsonl",
        "full_rebuild_command": "python3 docs/research/atlas-red-e1b/build_research_panel.py --scratch /path/to/scratch/e1b --outdir docs/research/atlas-red-e1b --rejected-attempt /path/to/prior-parser-rejected-attempts.jsonl",
        "compact_sample_command": "python3 docs/research/atlas-red-e1b/summarize_research_outputs.py",
    }
    if outdir is not None:
        report["dataset"] = "ATLAS_RED_E1B_CORRECTED_V3"
        report["model_effort_attestation"] = "Owner-selected GPT-6.1 Sol, High preferred; runtime model/effort UNVERIFIED; no subagents"
        report["research_history_initialization"] = {
            "method": "SEPARATE_EXPLICIT_FINITE_PREFIX_BOOTSTRAP_PER_DIAGNOSTIC_WINDOW",
            "origins_utc_ns": [ns(2024, 1, 1), ns(2025, 1, 1)],
            "ema": "unchanged repository recurrence; first-20/50 arithmetic-mean seed",
            "not_claimed": "original live EMA checkpoint or silent production restart",
            "gap_behavior": "in-window incomplete prefixes remain SOURCE_MISSING; never reseed",
        }
    output = directory / "DATASET_MANIFEST.json"
    output.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return report


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=pathlib.Path)
    parser.add_argument("--clean-clone-code-sha")
    parser.add_argument("--clean-scratch-empty-at-start", action="store_true")
    parser.add_argument("--clean-output-empty-at-start", action="store_true")
    parser.add_argument("--official-anonymous-retrieval-verified", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run(outdir=args.outdir, clean_clone_code_sha=args.clean_clone_code_sha,
                         clean_scratch_empty_at_start=args.clean_scratch_empty_at_start,
                         clean_output_empty_at_start=args.clean_output_empty_at_start,
                         official_anonymous_retrieval_verified=args.official_anonymous_retrieval_verified),
                     sort_keys=True))
