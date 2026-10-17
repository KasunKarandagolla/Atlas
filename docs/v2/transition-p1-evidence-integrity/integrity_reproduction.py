"""N1--N3 parent reproductions. Synthetic only; no financial functions patched.

Run from the exact source checkout with PYTHONPATH=scripts:src:tests:.
The guard is installed before importing Atlas or its genuine P1 fixtures.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import socket
import subprocess
import time
from dataclasses import replace
from decimal import Decimal as D
from pathlib import Path


def refuse(*args, **kwargs):
    raise OSError("INTEGRITY_REPRODUCTION_NETWORK_REFUSED")


socket.socket.connect = refuse
socket.socket.connect_ex = refuse
socket.create_connection = refuse
socket.getaddrinfo = refuse

from atlas.v2._serialization import FrozenMap
from atlas.v2.contracts import EligibleSetV1
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.research.mean_selection import MeanSelectionRequestV1, MeanSelectionResponseV1
from atlas.v2.science.mean_calibration import (
    MeanOOFPredictionV1, MeanOriginEvidenceV1, MeanOutcomeEvidenceV1,
    SelectedPolicyResidualV2, calibrate_verified_selected_policy, produce_verified_mean_oof,
)
from atlas.v2.science.mean_common import DAY_NS
from atlas.v2.science.mean_evaluation import index_mean_artifact, read_mean_artifact
from atlas.v2.science.mean_models import MeanModelV1, MeanTrainingRowV1
from p1.conftest import T, case as fixture
from p1.test_bootstrap_integrity import snapshot_entries, uncertain_chain, verify
from p1.test_financial_evidence import read
from p1.test_mean_evaluation import run
from p1.test_mean_selection import request_response, validate


def publish(repo, obj):
    index_mean_artifact(repo, obj, available_at_ns=obj.available_at_ns)
    return obj


def evaluation(case, inp):
    return next(e for e in run(case).evaluations if e.candidate_ref == inp.candidate.content_hash)


def baseline(case):
    inp, protocol, residuals, *_ = uncertain_chain(case)
    numerical = publish(case.repo, replace(inp.numerical, cash=D("2")))
    inp = replace(inp, numerical=numerical)
    case.inputs[inp.candidate.content_hash] = inp
    e = evaluation(case, inp)
    assert e.mean_cash == 5 and e.score_cash < 0 and e.status == "INELIGIBLE"
    assert verify(case, inp, inp.calibration)
    return inp, protocol, residuals, {
        "mean_cash": str(e.mean_cash), "allowance_cash": str(e.mean_selection_allowance),
        "numerical_cash": str(e.numerical_allowance), "score_cash": str(e.score_cash), "status": e.status,
    }


def n1(case):
    inp, protocol, residuals, control = baseline(case)
    forged = []
    for residual in residuals:
        prediction = read(case, residual.prediction_ref, MeanOOFPredictionV1)
        row = read(case, residual.target_row_ref, MeanTrainingRowV1)
        model = read(case, prediction.model_ref, MeanModelV1)
        model = publish(case.repo, replace(model, parameters=FrozenMap({
            "coefficients": [float(row.target_cash), 0., 0., 0., 0., 0., 0.],
            "ridge": protocol.ridge, "objective": "squared_loss",
        })))
        prediction = publish(case.repo, replace(prediction, model_ref=model.content_hash,
                                               prediction_cash=row.target_cash))
        forged.append(publish(case.repo, replace(residual, prediction_ref=prediction.content_hash)))
    calibration = calibrate_verified_selected_policy(case.repo, model=inp.model, protocol=protocol,
        residuals=tuple(forged), cutoff_ns=inp.calibration.available_at_ns,
        expires_at_ns=inp.calibration.expires_at_ns)
    assert calibration.allowance_cash == 0 and verify(case, inp, calibration)
    case.inputs[inp.candidate.content_hash] = replace(inp, calibration=calibration)
    e = evaluation(case, inp)
    assert e.status == "ELIGIBLE" and e.mean_cash == 5 and e.score_cash == 3
    return {"finding": "REPRODUCED", "control": control,
            "forged_fold_count": len(forged), "serving_model_unchanged": True,
            "writer_accepted": True, "allowance_cash": str(calibration.allowance_cash),
            "evaluation_status": e.status, "score_cash": str(e.score_cash)}


def n2(case, directory):
    inp, _, _, control = baseline(case)
    eligible = run(case)
    original = next(e for e in eligible.evaluations if e.candidate_ref == inp.candidate.content_hash)
    score = D("100") - original.mean_selection_allowance - original.execution_bias_allowance \
        - original.numerical_allowance - original.incremental_programme_cost
    forged = publish(case.repo, replace(original, mean_cash=D("100"), score_cash=score,
        j_cash=score - case.policy.risk_lambda * original.risk_increment_cash,
        status="ELIGIBLE", reasons=()))
    work = tuple(replace(w, evaluation_ref=forged.content_hash)
                 if w.evaluation_ref == original.content_hash else w for w in eligible.work)
    eligible = replace(eligible, evaluations=tuple(forged if e == original else e
                       for e in eligible.evaluations), work=work)
    index_mean_artifact(case.repo, eligible, available_at_ns=eligible.sealed_at_ns)
    request = MeanSelectionRequestV1(eligible.content_hash, case.snapshot.content_hash,
        case.snapshot.permission_ref, eligible.population.instrument_revision_ref,
        T + 10, eligible.population.deadline_ns, "n2-nonce", ())
    index_mean_artifact(case.repo, request, available_at_ns=request.issued_at_ns)
    response = MeanSelectionResponseV1(request.content_hash, eligible.content_hash,
        case.snapshot.content_hash, case.snapshot.permission_ref, request.source_prefix_ref,
        "n2-response", request.nonce, "SELECT_ONE", forged.action_ref, T + 11, (),
        "Synthetic integrity attack on saved economics")
    owner = case.repo
    fresh = OpsRepository(directory / "fresh-import.sqlite")
    entries = snapshot_entries(owner)
    fresh.register_artifacts(tuple(reversed(entries)))
    fresh.close()
    case.repo = OpsRepository(directory / "fresh-import.sqlite", read_only=True)
    assert read_mean_artifact(case.repo, eligible.content_hash, EligibleSetV1, consumer_ns=T+12) == eligible
    case.repo.close()
    case.repo = OpsRepository(directory / "fresh-import.sqlite")
    result = validate(case, eligible, request, response)
    assert result.decision == "SELECT_ONE" and result.action_ref == forged.action_ref
    case.repo.close()
    case.repo = OpsRepository(directory / "fresh-import.sqlite")
    duplicate = ""
    try:
        validate(case, eligible, request, response)
    except ValueError as exc:
        duplicate = str(exc)
    assert duplicate == "DUPLICATE_OR_CONSUMED_RESPONSE"
    case.repo.close()
    case.repo = owner
    return {"finding": "REPRODUCED", "control": control, "forged_mean_cash": "100",
        "forged_score_cash": str(score), "fresh_import_entries": len(entries),
        "readonly_reopen": True, "current_evaluation_refs": "all mutually rehashed",
        "b_decision": result.decision, "b_authority": result.authority,
        "duplicate_negative_control": duplicate}


def n3(case):
    inp, protocol, residuals, control = baseline(case)
    prediction = read(case, residuals[0].prediction_ref, MeanOOFPredictionV1)
    training = tuple(read(case, ref, MeanTrainingRowV1) for ref in prediction.training_row_refs)
    chosen, alternatives = [], []
    for k, residual in enumerate(residuals):
        row = read(case, residual.target_row_ref, MeanTrainingRowV1)
        origin = publish(case.repo, MeanOriginEvidenceV1(protocol.scope_ref,
            f"n3-alternate-{k}", row.origin_ns, row.origin_ns, row.features))
        outcome = publish(case.repo, MeanOutcomeEvidenceV1(protocol.scope_ref,
            origin.content_hash, row.origin_ns, row.label_end_ns, row.label_available_at_ns,
            row.features, D("5"), "FULL_FILL"))
        from atlas.v2.science.mean_calibration import MeanEvidenceScopeV1
        scope = read(case, protocol.scope_ref, MeanEvidenceScopeV1)
        alternate = outcome.training_row(scope)
        index_mean_artifact(case.repo, alternate, available_at_ns=alternate.label_available_at_ns)
        produced = produce_verified_mean_oof(case.repo, (*training, alternate), protocol=protocol)
        assert len(produced) == 1
        alternatives.append(produced[0])
        # This branch deliberately uses the already matured outcome.
        chosen.append(residual if row.target_cash > D("5") else produced[0])
    selected = calibrate_verified_selected_policy(case.repo, model=inp.model, protocol=protocol,
        residuals=tuple(chosen), cutoff_ns=inp.calibration.available_at_ns,
        expires_at_ns=inp.calibration.expires_at_ns)
    assert selected.allowance_cash == 0 and verify(case, inp, selected)
    complete = calibrate_verified_selected_policy(case.repo, model=inp.model, protocol=protocol,
        residuals=tuple((*residuals, *alternatives)), cutoff_ns=inp.calibration.available_at_ns,
        expires_at_ns=inp.calibration.expires_at_ns)
    assert complete.allowance_cash > 0
    case.inputs[inp.candidate.content_hash] = replace(inp, calibration=selected)
    e = evaluation(case, inp)
    assert e.status == "ELIGIBLE" and e.score_cash == 3
    return {"finding": "REPRODUCED", "control": control, "available_candidates": 60,
        "weekly_positions": 30, "genuine_fit_for_every_prediction": True,
        "post_outcome_chosen_count": len(chosen), "chosen_allowance_cash": str(selected.allowance_cash),
        "complete_60_candidate_allowance_cash": str(complete.allowance_cash),
        "variable_30_candidate_allowance_cash": control["allowance_cash"],
        "policy_scope_protocol_unchanged": True, "writer_accepted": True,
        "evaluation_status": e.status, "score_cash": str(e.score_cash),
        "original_selection_or_complete_calendar_proof": "not required by existing verifier"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    start = time.perf_counter()
    results = {"source_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "source_dirty_before": subprocess.check_output(["git", "status", "--porcelain"], text=True),
        "reproducer_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "evidence_grade": "SYNTHETIC_OFFLINE_QUALIFIED_CONTRACT", "network": "socket and DNS refused"}
    for name, attack in (("N1", n1), ("N2", n2), ("N3", n3)):
        directory = args.output / name
        directory.mkdir(exist_ok=True)
        generator = fixture.__wrapped__(directory)
        case = next(generator)
        try:
            results[name] = attack(case, directory) if name == "N2" else attack(case)
        finally:
            try:
                next(generator)
            except StopIteration:
                pass
        print(json.dumps({name: results[name]}, sort_keys=True), flush=True)
    results["wall_seconds"] = time.perf_counter() - start
    (args.output / "integrity_results.json").write_text(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    main()
