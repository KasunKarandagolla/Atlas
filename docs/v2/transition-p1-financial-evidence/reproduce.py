"""Reproduce the correction's synthetic qualified-contract capsule and DB replay.

Run with the pinned Atlas interpreter. This calls the actual test producers,
public evaluator and B validator. No provider/venue I/O or SQL migration.
Measured work timings may change content hashes between complete runs.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from dataclasses import fields
from pathlib import Path

root = Path(__file__).resolve().parents[3]
for key in tuple(os.environ):
    if any(
        s in key.upper()
        for s in (
            "BINANCE",
            "BYBIT",
            "GEMINI",
            "GEMMA",
            "OPENAI_API",
            "ANTHROPIC",
            "TELEGRAM",
            "AWS_SECRET",
            "AWS_ACCESS",
        )
    ) or key.upper().endswith(("_API_KEY", "_API_SECRET", "_ACCESS_TOKEN")):
        del os.environ[key]
sys.path[:0] = [str(root / "scripts"), str(root / "src"), str(root)]
import atlas_s49_offline_guard  # noqa: E402 -- credential scrub and source path precede the guard

atlas_s49_offline_guard.install()


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
    from atlas.v2.science.mean_calibration import (
        MeanCalibrationProtocolV1,
        MeanDependenceQualificationV1,
        SelectedPolicyResidualV2,
        calibrate_verified_selected_policy,
    )
    from atlas.v2.science.mean_evaluation import read_mean_artifact
    from tests.p1.conftest import T, case
    from tests.p1.test_mean_selection import request_response, validate

    output = Path(__file__).resolve().parent
    source_build_hash = json.loads((output / "source-build.json").read_text())["source_build_hash"]
    with tempfile.TemporaryDirectory(prefix="atlas-financial-evidence-") as directory:
        factory = case.__wrapped__(Path(directory))
        fixture = next(factory)
        entries = {}
        original = fixture.repo.get_artifact

        def capture(ref):
            entry = original(ref)
            if entry is not None and entry.artifact_type != "OfflineSelectionConsumptionV1":
                entries[ref] = {f.name: json_value(getattr(entry, f.name)) for f in fields(entry)}
            return entry

        fixture.repo.get_artifact = capture
        eligible, request, response = request_response(fixture)
        result = validate(fixture, eligible, request, response)
        # Independently replay the offline bootstrap; it never runs on admission.
        for inp in fixture.inputs.values():
            proof = read_mean_artifact(
                fixture.repo, inp.calibration.qualification_ref, MeanDependenceQualificationV1, consumer_ns=T
            )
            protocol = read_mean_artifact(fixture.repo, proof.protocol_ref, MeanCalibrationProtocolV1, consumer_ns=T)
            residuals = tuple(
                read_mean_artifact(fixture.repo, r, SelectedPolicyResidualV2, consumer_ns=T)
                for r in proof.residual_refs
            )
            rebuilt = calibrate_verified_selected_policy(
                fixture.repo,
                model=inp.model,
                protocol=protocol,
                residuals=residuals,
                cutoff_ns=inp.calibration.available_at_ns,
                expires_at_ns=inp.calibration.expires_at_ns,
            )
            assert rebuilt == inp.calibration
        capsule = {
            "source_build_hash": source_build_hash,
            "grade": "SYNTHETIC_OFFLINE_QUALIFIED_CONTRACT",
            "clock": "hypothetical fixture UTC nanoseconds; not observed live history",
            "account_and_market": "fake COIN1..4 TESTNET, synthetic data only",
            "authority": "ZERO",
            "capital": "NO-GO",
            "entries": list(entries.values()),
            "eligible_set": eligible.to_dict(),
            "snapshot": fixture.snapshot.to_dict(),
            "request": request.to_dict(),
            "response": response.to_dict(),
            "result": result.to_dict(),
        }
        (output / "qualified-capsule.json").write_text(json.dumps(capsule, indent=2) + "\n")
        fixture.repo.get_artifact = original
        try:
            next(factory)
        except StopIteration:
            pass
        replay = OpsRepository(Path(directory) / "replay.sqlite")
        replay.register_artifacts(tuple(ArtifactIndexEntryV2(**e) for e in capsule["entries"]))
        replay.close()
        replay = OpsRepository(Path(directory) / "replay.sqlite")
        e = EligibleSetV1.from_dict(capsule["eligible_set"])
        q = MeanSelectionRequestV1.from_dict(capsule["request"])
        r = MeanSelectionResponseV1.from_dict(capsule["response"])
        s = OfflineRiskSnapshotV1.from_dict(capsule["snapshot"])
        args = {
            "request": q,
            "response": r,
            "eligible": e,
            "snapshot": s,
            "now_ns": T + 12,
            "current_evaluation_refs": tuple(x.content_hash for x in e.evaluations),
        }
        replay_result = validate_offline_b_selection(replay, **args)
        assert replay_result.to_dict() == capsule["result"]
        replay.close()
        replay = OpsRepository(Path(directory) / "replay.sqlite")
        try:
            validate_offline_b_selection(replay, **args)
        except ValueError as exc:
            assert "CONSUMED" in str(exc)
        else:
            raise AssertionError("response replay accepted twice")
        replay.close()
        receipt = {
            "source_build_hash": source_build_hash,
            "grade": capsule["grade"],
            "indexed_entries": len(entries),
            "strict_saved_capsule_roundtrip": "PASS",
            "four_offline_bootstrap_recomputations": "PASS: same calibration and qualification hashes",
            "new_db_load_and_reopen_then_real_B_validation": "PASS: same result",
            "second_reopen_duplicate_consumption": "REFUSED",
            "capital": "NO-GO",
        }
        (output / "capsule-replay.json").write_text(json.dumps(receipt, indent=2) + "\n")
        print(json.dumps(receipt))


if __name__ == "__main__":
    main()
