"""Reproduce synthetic bootstrap integrity, durable import/replay and timings.

Run with the pinned interpreter; scrubs credentials and installs the existing
offline socket/DNS guard before importing Atlas or its real fixture producers.
"""

from __future__ import annotations

import gzip
import json
import math
import os
import sys
import tempfile
import time
from dataclasses import fields, replace
from decimal import Decimal
from pathlib import Path

root = Path(__file__).resolve().parents[3]
for key in tuple(os.environ):
    if any(s in key.upper() for s in (
        "BINANCE", "BYBIT", "GEMINI", "GEMMA", "OPENAI_API", "ANTHROPIC",
        "TELEGRAM", "AWS_SECRET", "AWS_ACCESS",
    )) or key.upper().endswith(("_API_KEY", "_API_SECRET", "_ACCESS_TOKEN")):
        del os.environ[key]
sys.path[:0] = [str(root / "scripts"), str(root / "src"), str(root)]
import atlas_s49_offline_guard  # noqa: E402

atlas_s49_offline_guard.install()


def timed(call, count):
    samples = []
    for _ in range(count):
        start = time.perf_counter_ns()
        call()
        samples.append(time.perf_counter_ns() - start)
    ordered = sorted(samples)
    return {
        "samples": count, "unit": "ns", "minimum": ordered[0],
        "median": ordered[count // 2], "p95": ordered[math.ceil(.95 * count) - 1],
        "maximum": ordered[-1],
    }


def main():
    from atlas.v2._serialization import json_value
    from atlas.v2.contracts import EligibleSetV1
    from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
    from atlas.v2.research.mean_selection import (
        MeanSelectionRequestV1,
        MeanSelectionResponseV1,
        validate_offline_b_selection,
    )
    from atlas.v2.risk import OfflineRiskSnapshotV1
    from atlas.v2.science.mean_calibration import _verify_mean_calibration_chain, verify_mean_calibration
    from atlas.v2.science.mean_calibration_validation import (
        MeanCalibrationValidationV1,
        _implementation_sources,
        calibration_validation_entry,
        require_calibration_validation,
        validation_implementation_ref,
    )
    from tests.p1.conftest import T, case
    from tests.p1.test_bootstrap_integrity import (
        forge_allowance,
        reference_bootstrap,
        snapshot_entries,
        uncertain_chain,
    )
    from tests.p1.test_mean_evaluation import run
    from tests.p1.test_mean_selection import request_response, validate

    output = Path(__file__).resolve().parent
    build = json.loads((output / "source-build.json").read_text())
    with tempfile.TemporaryDirectory(prefix="atlas-bootstrap-offline-") as directory:
        fixture_iterator = case.__wrapped__(Path(directory))
        fixture = next(fixture_iterator)
        inp, protocol, _, groups, effective, lags = uncertain_chain(fixture)
        required = reference_bootstrap(groups, protocol)
        assert required == inp.calibration.allowance_cash
        args = {"model": inp.model, "policy_ref": fixture.policy.content_hash,
                "product_ref": inp.product.content_hash, "cutoff_ns": T, "now_ns": T + 12}
        forged_results = []
        for amount in (Decimal("0"), required / 2):
            forged = forge_allowance(fixture, inp, amount)
            try:
                verify_mean_calibration(fixture.repo, forged, **args)
            except ValueError as exc:
                admission = str(exc)
            else:
                raise AssertionError("forged calibration accepted")
            try:
                fixture.repo.register_artifact(calibration_validation_entry(forged, amount))
            except ValueError as exc:
                publishing = str(exc)
            else:
                raise AssertionError("forged validation accepted")
            forged_results.append({"allowance_cash": str(amount), "admission": admission, "offline_publish": publishing})
        eligible, request, response = request_response(fixture)
        member = next(e for e in eligible.members if e.candidate_ref == inp.candidate.content_hash)
        numerical = replace(inp.numerical, cash=Decimal("2"))
        from atlas.v2.science.mean_evaluation import index_mean_artifact

        index_mean_artifact(fixture.repo, numerical, available_at_ns=numerical.available_at_ns)
        fixture.inputs[inp.candidate.content_hash] = replace(inp, numerical=numerical)
        crossing_set = run(fixture)
        crossing = next(e for e in crossing_set.evaluations if e.candidate_ref == inp.candidate.content_hash)
        assert crossing.status == "INELIGIBLE" and crossing.score_cash < 0
        fixture.inputs[inp.candidate.content_hash] = inp
        timings = {
            "receipt_check": timed(lambda: require_calibration_validation(fixture.repo, inp.calibration, cutoff_ns=T, now_ns=T + 12), 200),
            "existing_causal_chain": timed(lambda: _verify_mean_calibration_chain(fixture.repo, inp.calibration, **args), 20),
            "complete_verifier": timed(lambda: verify_mean_calibration(fixture.repo, inp.calibration, **args), 20),
            "connected_evaluator": timed(lambda: run(fixture), 5),
            "work_cpu_budget_ns": fixture.population.cpu_budget_ns,
            "work_wall_budget_ns": fixture.population.wall_budget_ns,
            "observed_evaluator_cpu_ns": eligible.cpu_used_ns,
            "observed_evaluator_wall_ns": eligible.wall_used_ns,
            "classification": "cloud warm isolated measurements; not target-host or real-time qualification",
        }
        _implementation_sources.cache_clear()
        start = time.perf_counter_ns()
        implementation = validation_implementation_ref()
        timings["startup_source_fingerprint_ns"] = time.perf_counter_ns() - start
        b_samples = []
        for i in range(10):
            q = replace(request, nonce=f"timing-{i}")
            index_mean_artifact(fixture.repo, q, available_at_ns=q.issued_at_ns)
            r = replace(response, request_ref=q.content_hash, nonce=q.nonce, response_id=f"timing-{i}")
            b_samples.append(timed(lambda q=q, r=r: validate(fixture, eligible, q, r), 1)["median"])
        timings["connected_B_ns"] = {"samples": len(b_samples), "median": sorted(b_samples)[5], "maximum": max(b_samples)}
        entries = snapshot_entries(fixture.repo)
        entries = tuple(e for e in entries if e.artifact_type != "OfflineSelectionConsumptionV1")
        result = validate(fixture, eligible, request, response)
        capsule = {
            "source_build_hash": build["source_build_hash"], "implementation_ref": implementation,
            "grade": "SYNTHETIC_OFFLINE_QUALIFIED_CONTRACT", "authority": "ZERO", "capital": "NO-GO",
            "data": "fake COIN1..4 TESTNET; hypothetical fixture clocks, no actual market/account history",
            "entries": [{f.name: json_value(getattr(e, f.name)) for f in fields(e)} for e in entries],
            "eligible_set": eligible.to_dict(), "request": request.to_dict(), "response": response.to_dict(),
            "snapshot": fixture.snapshot.to_dict(), "result": result.to_dict(),
        }
        (output / "qualified-capsule.json.gz").write_bytes(gzip.compress(json.dumps(capsule, indent=2).encode(), mtime=0))
        fresh_path = Path(directory) / "replay.sqlite"
        fresh = OpsRepository(fresh_path)
        # Complete generic import is an offline operation; it revalidates all receipts.
        start = time.perf_counter_ns()
        fresh.register_artifacts(tuple(ArtifactIndexEntryV2(**e) for e in capsule["entries"]))
        timings["complete_capsule_import_ns"] = time.perf_counter_ns() - start
        fresh.close()
        fresh = OpsRepository(fresh_path)
        e = EligibleSetV1.from_dict(capsule["eligible_set"])
        q = MeanSelectionRequestV1.from_dict(capsule["request"])
        r = MeanSelectionResponseV1.from_dict(capsule["response"])
        snapshot = OfflineRiskSnapshotV1.from_dict(capsule["snapshot"])
        replay_args = {"request": q, "response": r, "eligible": e, "snapshot": snapshot, "now_ns": T + 12,
                       "current_evaluation_refs": tuple(x.content_hash for x in e.evaluations)}
        assert validate_offline_b_selection(fresh, **replay_args) == result
        fresh.close()
        fresh = OpsRepository(fresh_path)
        try:
            validate_offline_b_selection(fresh, **replay_args)
        except ValueError as exc:
            assert "CONSUMED" in str(exc)
        else:
            raise AssertionError("duplicate selection accepted")
        fresh.close()
        weighted, weighted_protocol, _, weighted_groups, *_ = uncertain_chain(fixture, unequal=True)
        unweighted = reference_bootstrap({k: [sum(v, Decimal(0)) / len(v)] for k, v in weighted_groups.items()}, weighted_protocol)
        numbers = {
            "source_build_hash": build["source_build_hash"], "implementation_ref": implementation,
            "empirical_selected_action_error_cash": "0", "calendar_blocks": len(groups),
            "effective_blocks": effective, "lag_correlations": lags,
            "alpha": protocol.alpha, "resamples": protocol.resamples, "seed": 6101,
            "registered_bound_cash": str(required), "independent_reference_bound_cash": str(required),
            "forged": forged_results,
            "positive_evaluation": member.to_dict(), "nonpositive_evaluation": crossing.to_dict(),
            "no_allowance_counterfactual_score_cash": str(crossing.mean_cash - crossing.numerical_allowance),
            "weighted_selected_count": sum(map(len, weighted_groups.values())),
            "weighted_bound_cash": str(weighted.calibration.allowance_cash),
            "incorrect_unweighted_bound_cash": str(unweighted),
            "capsule_entries": len(entries),
            "import_revalidated_receipts": sum(x.artifact_type == MeanCalibrationValidationV1.__name__ for x in entries),
            "new_database_reopen_B_result": "IDENTICAL", "second_reopen_duplicate": "REFUSED",
            "old_calibration_bytes": "unchanged V2 codec; absent validation receipt never grants new admission",
            "capital": "NO-GO",
        }
        for filename, body in (("numerical-and-replay.json", numbers), ("performance.json", timings)):
            (output / filename).write_text(json.dumps(body, indent=2) + "\n")
        print(json.dumps({"bound": str(required), "positive_score": str(member.score_cash),
                          "nonpositive_score": str(crossing.score_cash), "replay": "PASS", "timings": timings}))
        try:
            next(fixture_iterator)
        except StopIteration:
            pass


if __name__ == "__main__":
    main()
