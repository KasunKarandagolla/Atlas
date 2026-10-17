"""Registered-bootstrap integrity through the real Ops/evaluator/B path."""

import math
import random
from dataclasses import replace
from decimal import Decimal as D

import pytest

from atlas.v2.science.mean_calibration import (
    EvidenceReader,
    MeanDependenceQualificationV1,
    dependence_diagnostics,
    verify_mean_calibration,
    verify_oof_residuals,
)
from atlas.v2.science.mean_common import DAY_NS, ZERO
from atlas.v2.science.mean_evaluation import index_mean_artifact

from .conftest import H, T
from .test_financial_evidence import read
from .test_mean_evaluation import run
from .test_mean_selection import request_response, validate


def uncertain_chain(case, *, unequal=False):
    """Actual causal M0 folds, fixed pre-origin training, never declared predictions."""
    from .evidence_fixtures import verified_calibration, verified_scenarios

    inp = list(case.inputs.values())[1]
    signs = [-1] * 15 + [1] * 15
    random.Random(1).shuffle(signs)
    compat = H(f"preoutcome-bootstrap-cell-{unequal}")
    model, calibration, scope, hold, protocol, residuals = verified_calibration(
        case.repo, product=inp.product, policy=case.policy, compatibility_ref=compat, cash="5", now_ns=T,
        residual_errors=tuple(D(10 * s) for s in signs), unequal=unequal,
    )
    scenarios = verified_scenarios(case.repo, replace(inp.scenarios, compatibility_ref=compat),
                                  scope=scope, hold=hold, policy=case.policy, snapshot=case.snapshot)
    changes = {name: replace(getattr(inp, name), model_ref=model.content_hash, compatibility_ref=compat)
               for name in ("execution_bias", "numerical", "incremental_programme")}
    changes["context"] = replace(inp.context, compatibility_ref=compat)
    for obj in changes.values():
        index_mean_artifact(case.repo, obj, available_at_ns=obj.available_at_ns)
    inp = replace(inp, model=model, calibration=calibration, scenarios=scenarios, **changes)
    case.inputs[inp.candidate.content_hash] = inp
    reader = EvidenceReader(case.repo, cutoff_ns=T, now_ns=T)
    _, values = verify_oof_residuals(reader, protocol, tuple(r.content_hash for r in residuals))
    groups, effective, lags = dependence_diagnostics(values, protocol)
    if not unequal:
        assert len(values) == len(groups) == effective == 30
        assert sorted(v for _, v in values) == [D("-10")] * 15 + [D("10")] * 15
        assert sum((v for _, v in values), ZERO) / len(values) == 0
    assert calibration.allowance_cash > 0
    return inp, protocol, tuple(residuals), groups, effective, lags


def forge_allowance(case, inp, allowance):
    proof = read(case, inp.calibration.qualification_ref, MeanDependenceQualificationV1)
    proof = replace(proof, allowance_cash=allowance)
    index_mean_artifact(case.repo, proof, available_at_ns=proof.available_at_ns)
    forged = replace(inp.calibration, allowance_cash=allowance, qualification_ref=proof.content_hash)
    index_mean_artifact(case.repo, forged, available_at_ns=forged.available_at_ns)
    return forged


def verify(case, inp, calibration):
    return verify_mean_calibration(
        case.repo, calibration, model=inp.model, policy_ref=case.policy.content_hash,
        product_ref=inp.product.content_hash, cutoff_ns=T, now_ns=T + 12,
    )


@pytest.mark.parametrize("fraction", [D("0"), D(".5")], ids=["CAL-NEG-01-zero", "CAL-NEG-02-between"])
def test_rehashed_understatement_rejected(case, fraction):
    inp, *_ = uncertain_chain(case)
    forged = forge_allowance(case, inp, inp.calibration.allowance_cash * fraction)
    with pytest.raises(ValueError):
        verify(case, inp, forged)


def test_cal_neg_03_connected_evaluator_rejects_mutually_rehashed_claim(case):
    inp, *_ = uncertain_chain(case)
    forged = forge_allowance(case, inp, D("0"))
    case.inputs[inp.candidate.content_hash] = replace(inp, calibration=forged)
    evaluated = run(case)
    assert inp.candidate.content_hash not in {e.candidate_ref for e in evaluated.members}


def substituted_selection(case, eligible, request, response, forged):
    evaluations = tuple(
        replace(
            e, calibration_ref=forged.content_hash, mean_selection_allowance=forged.allowance_cash,
            score_cash=e.score_cash + e.mean_selection_allowance - forged.allowance_cash,
            j_cash=e.j_cash + e.mean_selection_allowance - forged.allowance_cash,
        )
        if e.calibration_ref == list(case.inputs.values())[1].calibration.content_hash else e
        for e in eligible.evaluations
    )
    for e in evaluations:
        index_mean_artifact(case.repo, e, available_at_ns=e.available_at_ns)
    revisions = {old.content_hash: new.content_hash for old, new in zip(eligible.evaluations, evaluations, strict=True)}
    work = tuple(replace(w, evaluation_ref=revisions.get(w.evaluation_ref, w.evaluation_ref)) for w in eligible.work)
    eligible = replace(eligible, evaluations=evaluations, work=work)
    index_mean_artifact(case.repo, eligible, available_at_ns=eligible.sealed_at_ns)
    request = replace(request, eligible_set_ref=eligible.content_hash)
    index_mean_artifact(case.repo, request, available_at_ns=request.issued_at_ns)
    response = replace(response, request_ref=request.content_hash, eligible_set_ref=eligible.content_hash)
    return eligible, request, response


def test_cal_neg_05_saved_b_refuses_substituted_proof(case):
    inp, *_ = uncertain_chain(case)
    eligible, request, response = request_response(case)
    forged = forge_allowance(case, inp, D("0"))
    eligible, request, response = substituted_selection(case, eligible, request, response, forged)
    with pytest.raises(ValueError):
        validate(case, eligible, request, response)
    from atlas.v2.memory.repository import OpsRepository

    path = case.repo.path
    case.repo.close()
    case.repo = OpsRepository(path)
    with pytest.raises(ValueError):
        validate(case, eligible, request, response)
    case.inputs[inp.candidate.content_hash] = replace(inp, calibration=forged)
    assert inp.candidate.content_hash not in {e.candidate_ref for e in run(case).members}


def reference_bootstrap(groups, protocol):
    """Independent test implementation: sampled integer block indices and counts."""
    blocks = [groups[k] for k in sorted(groups)]
    rng = random.Random(6101)
    results = []
    for _ in range(protocol.resamples):
        indices = [rng.randrange(len(blocks)) for _ in blocks]
        total = sum((v for i in indices for v in blocks[i]), ZERO)
        count = sum(len(blocks[i]) for i in indices)
        results.append(total / count)
    results.sort()
    empirical = sum((v for block in blocks for v in block), ZERO) / sum(map(len, blocks))
    return max(ZERO, empirical, results[math.ceil((1 - protocol.alpha) * len(results)) - 1])


def test_cal_neg_03_generic_receipt_registration_recomputes_not_self_certifies(case):
    from atlas.v2.memory.repository import ArtifactIndexEntryV2
    from atlas.v2.science.mean_calibration_validation import calibration_validation_entry

    inp, protocol, _, groups, *_ = uncertain_chain(case)
    assert reference_bootstrap(groups, protocol) == inp.calibration.allowance_cash == D("3.333333333333333333333333333")
    for amount in (D("0"), inp.calibration.allowance_cash / 2):
        forged = forge_allowance(case, inp, amount)
        receipt = calibration_validation_entry(forged, amount)
        # Register_artifact and register_artifacts are public import boundaries.
        with pytest.raises(ValueError, match="BELOW_REGISTERED_BOOTSTRAP"):
            case.repo.register_artifact(receipt)
        assert case.repo.get_artifact(receipt.artifact_ref) is None
        sibling = ArtifactIndexEntryV2(H(f"rolled-back-{amount}"), "SyntheticP1Source", H("body"), 0, 0, {})
        with pytest.raises(ValueError, match="BELOW_REGISTERED_BOOTSTRAP"):
            case.repo.register_artifacts((receipt, sibling))
        assert case.repo.get_artifact(sibling.artifact_ref) is None
        assert case.repo.get_artifact(receipt.artifact_ref) is None
    # The alternative SQL-stream publication API must not bypass this boundary.
    with case.repo._transaction() as connection, pytest.raises(ValueError, match="checked offline"):
        case.repo.publish_stream_slice_v1(
            connection, publication_id="forged", run_id="offline", descriptor_hash=H("descriptor"),
            slice_id="slice", logical_ready_at_ns=T, entries=(receipt,),
        )


@pytest.mark.parametrize("fault", ["alpha", "resamples", "membership", "grouping", "model", "policy", "cell", "source", "expiry", "implementation"])
def test_cal_neg_04_changed_binding_cannot_borrow_old_validation(case, fault, monkeypatch):
    from atlas.v2.science import mean_calibration_validation as boundary

    inp, *_ = uncertain_chain(case)
    proof = read(case, inp.calibration.qualification_ref, MeanDependenceQualificationV1)
    from atlas.v2.science.mean_calibration import MeanCalibrationProtocolV1

    protocol = read(case, proof.protocol_ref, MeanCalibrationProtocolV1)
    calibration = inp.calibration
    model = inp.model
    if fault in ("alpha", "resamples", "grouping"):
        protocol = replace(protocol, **{
            "alpha": {"alpha": .01}, "resamples": {"resamples": 20000}, "grouping": {"block_ns": 14 * DAY_NS},
        }[fault])
        index_mean_artifact(case.repo, protocol, available_at_ns=protocol.registered_at_ns)
        proof = replace(proof, protocol_ref=protocol.content_hash)
    elif fault == "membership":
        proof = replace(proof, residual_refs=proof.residual_refs[:-1])
        calibration = replace(calibration, calibration_refs=proof.residual_refs)
    elif fault == "model":
        model = replace(model, expires_at_ns=model.expires_at_ns + 1)
        index_mean_artifact(case.repo, model, available_at_ns=model.available_at_ns)
        proof = replace(proof, serving_model_ref=model.content_hash)
        calibration = replace(calibration, model_ref=model.content_hash)
    elif fault == "expiry":
        proof = replace(proof, expires_at_ns=proof.expires_at_ns + 1)
        calibration = replace(calibration, expires_at_ns=calibration.expires_at_ns + 1)
    elif fault == "implementation":
        monkeypatch.setattr(boundary, "validation_implementation_ref", lambda: H("new-implementation"))
    else:
        calibration = replace(calibration, **{
            "policy": {"policy_ref": H("changed-policy")}, "cell": {"compatibility_ref": H("changed-cell")},
            "source": {"source_ref": H("changed-source")},
        }[fault])
    index_mean_artifact(case.repo, proof, available_at_ns=proof.available_at_ns)
    calibration = replace(calibration, qualification_ref=proof.content_hash)
    index_mean_artifact(case.repo, calibration, available_at_ns=calibration.available_at_ns)
    with pytest.raises(ValueError):
        verify(case, replace(inp, model=model), calibration)


def test_cal_neg_06_and_pos_01_nonzero_admission_and_b_never_generate_statistics(case, monkeypatch):
    import atlas.v2.science.mean_calibration as calibration_module
    import atlas.v2.science.mean_models as models
    import atlas.v2.science.mean_research as research
    from atlas.v2.memory.repository import OpsRepository
    from atlas.v2.science.mean_calibration_validation import (
        calibration_validation_entry,
        require_calibration_validation,
    )

    inp, *_ = uncertain_chain(case)
    assert verify(case, inp, inp.calibration)
    path = case.repo.path
    case.repo.close()
    case.repo = OpsRepository(path)

    def forbidden(*args, **kwargs):
        raise AssertionError("offline statistical generation on hot path")

    for module, names in (
        (calibration_module, ("selected_policy_upper_cash", "produce_verified_mean_oof", "calibrate_verified_selected_policy")),
        (models, ("fit_m0_mean", "fit_m1_mean", "fit_empirical_mean")),
        (research, ("chronological_mean_oof",)),
    ):
        for name in names:
            monkeypatch.setattr(module, name, forbidden)
    monkeypatch.setattr(random.Random, "choice", forbidden)
    require_calibration_validation(case.repo, inp.calibration, cutoff_ns=T, now_ns=T + 12)
    # Identical durable retry also must not regenerate bootstrap.
    case.repo.register_artifact(calibration_validation_entry(inp.calibration, inp.calibration.allowance_cash))
    eligible, request, response = request_response(case)
    member = next(e for e in eligible.members if e.candidate_ref == inp.candidate.content_hash)
    assert member.mean_cash == 5
    assert member.mean_selection_allowance == inp.calibration.allowance_cash
    assert member.score_cash == 5 - inp.calibration.allowance_cash > 0
    result = validate(case, eligible, request, response)
    assert result.action_ref == member.action_ref
    with pytest.raises(ValueError, match="CONSUMED"):
        validate(case, eligible, request, response)
    case.repo.close()
    case.repo = OpsRepository(path)
    with pytest.raises(ValueError, match="CONSUMED"):
        validate(case, eligible, request, response)


def test_cal_pos_02_unequal_selected_counts_are_weighted(case):
    inp, protocol, _, groups, *_ = uncertain_chain(case, unequal=True)
    correct = reference_bootstrap(groups, protocol)
    unweighted = reference_bootstrap({k: [sum(v, ZERO) / len(v)] for k, v in groups.items()}, protocol)
    assert inp.calibration.allowance_cash == correct
    assert correct != unweighted
    assert verify(case, inp, inp.calibration)


def test_exact_action_crosses_zero_only_after_correct_allowance(case):
    inp, *_ = uncertain_chain(case)
    numerical = replace(inp.numerical, cash=D("2"))
    index_mean_artifact(case.repo, numerical, available_at_ns=numerical.available_at_ns)
    case.inputs[inp.candidate.content_hash] = replace(inp, numerical=numerical)
    evaluated = run(case)
    action = next(e for e in evaluated.evaluations if e.candidate_ref == inp.candidate.content_hash)
    assert action.mean_cash - action.numerical_allowance == 3 > 0
    assert action.mean_selection_allowance == inp.calibration.allowance_cash > 3
    assert action.score_cash == 3 - inp.calibration.allowance_cash < 0
    assert action.status == "INELIGIBLE"
    assert action.action_ref not in {e.action_ref for e in evaluated.members}
    eligible, request, response = request_response(case)
    assert response.action_ref != action.action_ref
    with pytest.raises(ValueError, match="OUTSIDE_ELIGIBLE_SET"):
        validate(case, eligible, request, replace(response, action_ref=action.action_ref))


def snapshot_entries(repo):
    # Read-only export of the actual existing owner; no direct SQL writes.
    refs = tuple(row[0] for row in repo._connection.execute("SELECT artifact_ref FROM artifact_index"))
    return tuple(repo.get_artifact(ref) for ref in refs)


def test_fresh_database_import_recomputes_receipts_and_preserves_b_replay(case, tmp_path, monkeypatch):
    import atlas.v2.science.mean_calibration as calibration_module
    from atlas.v2.memory.repository import OpsRepository
    from atlas.v2.science.mean_calibration_validation import MeanCalibrationValidationV2
    from atlas.v2.science.mean_population import import_population_evidence

    inp, *_ = uncertain_chain(case)
    eligible, request, response = request_response(case)
    entries = snapshot_entries(case.repo)
    fresh = OpsRepository(tmp_path / "fresh.sqlite")
    original = calibration_module.selected_policy_upper_cash
    computed = []

    def counted(*args, **kwargs):
        computed.append(kwargs)
        return original(*args, **kwargs)

    monkeypatch.setattr(calibration_module, "selected_policy_upper_cash", counted)
    # Reverse dependency order intentionally, receipt first, in one atomic batch.
    receipts = tuple(e for e in entries if e.artifact_type == MeanCalibrationValidationV2.__name__)
    data = tuple(e for e in entries if e not in receipts and e.artifact_type != "MeanEvaluationValidationV1")
    import_population_evidence(fresh, trusted_source=case.repo, entries=data)
    saved = read(case, inp.calibration.content_hash, type(inp.calibration))
    assert saved == inp.calibration  # Old V2 bytes/meaning still decode.
    owner = case.repo
    case.repo = fresh
    with pytest.raises(ValueError, match="VALIDATION_UNAVAILABLE"):
        verify(case, inp, saved)
    assert not run(case).members  # No silent promotion of old saved V2 artifacts.
    import_population_evidence(fresh, trusted_source=owner, entries=tuple(reversed(entries)))
    assert len(computed) == len(receipts) == 5
    assert any(c["resamples"] == 10000 for c in computed)
    fresh.close()
    case.repo = OpsRepository(tmp_path / "fresh.sqlite", read_only=True)
    assert verify(case, inp, saved)
    case.repo.close()
    case.repo = OpsRepository(tmp_path / "fresh.sqlite")
    result = validate(case, eligible, request, response)
    assert result.action_ref == response.action_ref
    assert len(computed) == 5  # Reopen/admission/B never recompute.
    case.repo.close()
    case.repo = OpsRepository(tmp_path / "fresh.sqlite")
    with pytest.raises(ValueError, match="CONSUMED"):
        validate(case, eligible, request, response)
    owner.close()


def test_self_declared_certificate_for_genuine_calibration_is_not_trust(case, tmp_path):
    from atlas.v2.memory.repository import OpsRepository
    from atlas.v2.science.mean_calibration_validation import (
        MeanCalibrationValidationV2,
        calibration_validation_entry,
    )

    inp, *_ = uncertain_chain(case)
    from atlas.v2.science.mean_population import import_population_evidence

    fresh = OpsRepository(tmp_path / "untrusted-import.sqlite")
    import_population_evidence(fresh, trusted_source=case.repo,
        entries=tuple(e for e in snapshot_entries(case.repo)
                      if e.artifact_type != MeanCalibrationValidationV2.__name__))
    request = calibration_validation_entry(inp.calibration, D("0"))
    with pytest.raises(ValueError, match="VALIDATION_BINDING_CHANGED"):
        fresh.register_artifact(request)
    assert fresh.get_artifact(request.artifact_ref) is None
    with pytest.raises(ValueError):
        fresh.register_artifact(replace(request, artifact_ref=H("arbitrary-certificate-ref")))
    fresh.close()


def test_saved_b_cannot_use_zero_evaluation_allowance_with_authentic_calibration(case):
    inp, *_ = uncertain_chain(case)
    eligible, request, response = request_response(case)
    # All references/hashes agree, and the calibration itself is authentic.
    # The saved financial term must still equal the validated calibration term.
    forged = replace(inp.calibration, allowance_cash=D("0"))
    eligible, request, response = substituted_selection(case, eligible, request, response, forged)
    evaluations = tuple(replace(e, calibration_ref=inp.calibration.content_hash)
                        if e.calibration_ref == forged.content_hash else e for e in eligible.evaluations)
    revisions = {old.content_hash: new.content_hash for old, new in zip(eligible.evaluations, evaluations, strict=True)}
    eligible = replace(eligible, evaluations=evaluations, work=tuple(
        replace(w, evaluation_ref=revisions.get(w.evaluation_ref, w.evaluation_ref)) for w in eligible.work))
    for e in evaluations:
        index_mean_artifact(case.repo, e, available_at_ns=e.available_at_ns)
    index_mean_artifact(case.repo, eligible, available_at_ns=eligible.sealed_at_ns)
    request = replace(request, eligible_set_ref=eligible.content_hash)
    index_mean_artifact(case.repo, request, available_at_ns=request.issued_at_ns)
    response = replace(response, request_ref=request.content_hash, eligible_set_ref=eligible.content_hash)
    with pytest.raises(ValueError, match="SAVED_EVALUATION_CALIBRATION_ALLOWANCE_CHANGED"):
        validate(case, eligible, request, response)
