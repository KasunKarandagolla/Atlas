"""Synthetic cloud timing and exact saved B replay; run through run_guarded.py."""
from __future__ import annotations

import argparse
import gzip
import json
import resource
import statistics
import time
from dataclasses import fields, replace
from pathlib import Path

from atlas.v2._serialization import json_value
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.science.mean_calibration_validation import (
    require_calibration_validation,
    validation_implementation_ref,
)
from atlas.v2.science.mean_evaluation import index_mean_artifact
from atlas.v2.science.mean_evaluation_validation import require_evaluation_validation
from atlas.v2.science.mean_population import import_population_evidence
from tests.p1.conftest import T, build_case
from tests.p1.test_bootstrap_integrity import snapshot_entries, uncertain_chain
from tests.p1.test_mean_evaluation import run
from tests.p1.test_mean_selection import request_response, validate


def timings(function, count):
    samples = []
    for _ in range(count):
        started = time.perf_counter_ns()
        function()
        samples.append(time.perf_counter_ns() - started)
    return {"samples": count, "unit": "ns", "median": statistics.median(samples),
            "p95": sorted(samples)[max(0, int(.95 * count) - 1)], "maximum": max(samples)}


parser = argparse.ArgumentParser()
parser.add_argument("--work-dir", type=Path, required=True)
parser.add_argument("--output-dir", type=Path, required=True)
args = parser.parse_args()
args.work_dir.mkdir(parents=True, exist_ok=True)
args.output_dir.mkdir(parents=True, exist_ok=True)
started = time.perf_counter_ns()
cpu_started = time.process_time_ns()
generator = build_case(args.work_dir)
fixture = next(generator)
try:
    results = {"classification": "synthetic cloud samples; NOT target-host, market or realtime qualification",
               "implementation_ref": validation_implementation_ref(),
               "four_cell_fixture_offline_publication_wall_ns": time.perf_counter_ns() - started,
               "four_cell_fixture_offline_publication_cpu_ns": time.process_time_ns() - cpu_started}
    inp, protocol, residuals, groups, effective, lags = uncertain_chain(fixture)
    eligible, request, response = request_response(fixture)
    evaluation = next(e for e in eligible.members if e.candidate_ref == inp.candidate.content_hash)
    results["calibration_receipt"] = timings(lambda: require_calibration_validation(
        fixture.repo, inp.calibration, cutoff_ns=T, now_ns=T + 12), 200)
    results["evaluation_receipt"] = timings(lambda: require_evaluation_validation(
        fixture.repo, evaluation, now_ns=T + 12), 200)
    results["connected_evaluation"] = timings(lambda: run(fixture), 5)
    results["evaluation_budget"] = {"cpu_ns": fixture.population.cpu_budget_ns,
        "wall_ns": fixture.population.wall_budget_ns,
        "observed_cpu_ns": eligible.cpu_used_ns, "observed_wall_ns": eligible.wall_used_ns}
    samples = []
    for i in range(10):
        q = replace(request, nonce=f"measured-{i}")
        index_mean_artifact(fixture.repo, q, available_at_ns=q.issued_at_ns)
        r = replace(response, request_ref=q.content_hash, nonce=q.nonce, response_id=f"measured-{i}")
        samples.append(timings(lambda q=q, r=r: validate(fixture, eligible, q, r), 1)["median"])
    results["connected_B"] = {"samples": len(samples), "unit": "ns",
        "median": statistics.median(samples), "maximum": max(samples)}
    entries = tuple(e for e in snapshot_entries(fixture.repo) if e.artifact_type != "OfflineSelectionConsumptionV1")
    expected = validate(fixture, eligible, request, response)
    capsule = {"grade": "SYNTHETIC_OFFLINE_QUALIFIED_CONTRACT", "authority": "ZERO",
        "note": "Hypothetical clocks; original records require explicitly trusted source Ops owner. Bytes alone confer no trust.",
        "implementation_ref": results["implementation_ref"],
        "entries": [{f.name: json_value(getattr(e, f.name)) for f in fields(e)} for e in entries],
        "eligible_set": eligible.to_dict(), "request": request.to_dict(), "response": response.to_dict(),
        "snapshot": fixture.snapshot.to_dict(), "result": expected.to_dict()}
    (args.output_dir / "synthetic-capsule.json.gz").write_bytes(gzip.compress(json.dumps(capsule).encode(), mtime=0))
    original_repo = fixture.repo
    fresh_path = args.work_dir / "trusted-replay.sqlite"
    fresh = OpsRepository(fresh_path)
    started = time.perf_counter_ns()
    import_population_evidence(fresh, trusted_source=original_repo, entries=tuple(reversed(entries)))
    results["trusted_import_ns"] = time.perf_counter_ns() - started
    fresh.close()
    fixture.repo = OpsRepository(fresh_path, read_only=True)
    assert fixture.repo.get_artifact(eligible.content_hash) is not None
    fixture.repo.close()
    fixture.repo = OpsRepository(fresh_path)
    assert validate(fixture, eligible, request, response) == expected
    fixture.repo.close()
    fixture.repo = OpsRepository(fresh_path)
    try:
        validate(fixture, eligible, request, response)
    except ValueError as exc:
        assert "CONSUMED" in str(exc)
    else:
        raise AssertionError("duplicate B accepted after restart")
    fixture.repo.close()
    fixture.repo = original_repo
    results["replay"] = {"entries": len(entries), "calibration_receipts": sum(e.artifact_type == "MeanCalibrationValidationV2" for e in entries),
        "evaluation_receipts": sum(e.artifact_type == "MeanEvaluationValidationV1" for e in entries),
        "readonly_reopen": "READABLE", "new_database_B": "IDENTICAL", "second_reopen_duplicate": "REFUSED"}
    results["numerical"] = {"selected": len(residuals), "weekly_blocks": len(groups), "effective": effective,
        "lag_correlations": lags, "allowance_cash": str(inp.calibration.allowance_cash),
        "genuine_eligible_score_cash": str(evaluation.score_cash)}
    results["process_peak_rss_kib"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    (args.output_dir / "performance-and-replay.json").write_text(json.dumps(results, indent=2) + "\n")
    print(json.dumps(results, indent=2), flush=True)
finally:
    try:
        next(generator)
    except StopIteration:
        pass
