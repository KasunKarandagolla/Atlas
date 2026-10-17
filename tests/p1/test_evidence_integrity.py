"""Connected N1--N3 attacks and genuine original-producer/import controls."""
from dataclasses import replace
from decimal import Decimal as D

import pytest

from atlas.v2._serialization import FrozenMap
from atlas.v2.contracts import EligibleSetV1
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.science.mean_calibration import (
    EvidenceReader,
    MeanOOFPredictionV1,
    MeanOriginEvidenceV1,
    MeanOutcomeEvidenceV1,
    calibrate_verified_selected_policy,
    dependence_diagnostics,
    produce_verified_mean_oof,
    selected_policy_upper_cash,
    verify_oof_residuals,
)
from atlas.v2.science.mean_calibration_validation import calibration_validation_entry
from atlas.v2.science.mean_common import DAY_NS
from atlas.v2.science.mean_evaluation import index_mean_artifact
from atlas.v2.science.mean_evaluation_validation import (
    MeanEvaluationValidationV1,
)
from atlas.v2.science.mean_evaluation_validation import (
    _entry as evaluation_entry,
)
from atlas.v2.science.mean_models import MeanModelV1, MeanTrainingRowV1
from atlas.v2.science.mean_population import (
    ORIGINAL_TYPES,
    MeanPopulationRegistrationV1,
    MeanPopulationSlotV1,
    import_population_evidence,
    record_population_registration,
    record_population_slot,
    registration_ref,
    slot_ref,
    verify_selected_population,
)

from .conftest import H, T
from .test_bootstrap_integrity import snapshot_entries, uncertain_chain, verify
from .test_financial_evidence import a_chain, read
from .test_mean_selection import request_response, validate


def publish(case, obj):
    index_mean_artifact(case.repo, obj, available_at_ns=obj.available_at_ns)
    return obj


def calibrate(case, inp, protocol, residuals):
    return calibrate_verified_selected_policy(case.repo, model=inp.model, protocol=protocol,
        residuals=tuple(residuals), cutoff_ns=inp.calibration.available_at_ns,
        expires_at_ns=inp.calibration.expires_at_ns)


@pytest.mark.parametrize("fault", ["oracle", "scaler", "support", "library", "training"])
def test_n1_forged_fold_cannot_receive_checked_publication(case, fault):
    inp, protocol, residuals, *_ = uncertain_chain(case)
    forged = []
    for residual in residuals:
        pred = read(case, residual.prediction_ref, MeanOOFPredictionV1)
        row = read(case, residual.target_row_ref, MeanTrainingRowV1)
        model = read(case, pred.model_ref, MeanModelV1)
        changes = {}
        if fault == "oracle":
            changes["parameters"] = FrozenMap({"coefficients": [float(row.target_cash), 0., 0., 0., 0., 0., 0.],
                                              "ridge": protocol.ridge, "objective": "squared_loss"})
            pred = replace(pred, prediction_cash=row.target_cash)
        elif fault == "scaler":
            changes["centers"] = (model.centers[0] + 1, *model.centers[1:])
        elif fault == "support":
            changes["support_min"] = (model.support_min[0] - 1, *model.support_min[1:])
        elif fault == "library":
            changes["library_identity"] = "invented-solver"
        else:
            changes["training_refs"] = model.training_refs[1:]
            pred = replace(pred, training_row_refs=pred.training_row_refs[1:])
        model = publish(case, replace(model, **changes))
        pred = publish(case, replace(pred, model_ref=model.content_hash))
        forged.append(publish(case, replace(residual, prediction_ref=pred.content_hash)))
    with pytest.raises(ValueError, match="NOT_DERIVED_FROM_CLAIMED_TRAINING"):
        calibrate(case, inp, protocol, forged)
    assert verify(case, inp, inp.calibration)  # Genuine control still works.


def forge_saved_evaluation(case, field):
    inp, *_ = uncertain_chain(case)
    if field in ("mean_cash", "numerical_allowance"):
        inp = replace(inp, numerical=publish(case, replace(inp.numerical, cash=D("2"))))
    elif field == "execution_bias_allowance":
        inp = replace(inp, execution_bias=publish(case, replace(inp.execution_bias, cash=D("3"))))
    elif field == "incremental_programme_cost":
        inp = replace(inp, incremental_programme=publish(case, replace(inp.incremental_programme, cash=D("3"))))
    case.inputs[inp.candidate.content_hash] = inp
    eligible, request, response = request_response(case)
    original = next(e for e in eligible.evaluations if e.candidate_ref == inp.candidate.content_hash)
    if field not in ("risk_increment_cash", "j_cash"):
        assert original.status == "INELIGIBLE" and original.score_cash < 0
    changes = {"status": "ELIGIBLE", "reasons": ()}
    if field == "mean_cash":
        changes[field] = D("100")
    elif field != "j_cash":
        changes[field] = D("-1") if field == "risk_increment_cash" else D("0")
    terms = {name: changes.get(name, getattr(original, name)) for name in (
        "mean_cash", "mean_selection_allowance", "execution_bias_allowance",
        "numerical_allowance", "incremental_programme_cost", "risk_increment_cash")}
    score = terms["mean_cash"] - sum(terms[name] for name in (
        "mean_selection_allowance", "execution_bias_allowance", "numerical_allowance", "incremental_programme_cost"))
    forged = publish(case, replace(original, **changes, score_cash=score,
        j_cash=D("1000") if field == "j_cash" else score - case.policy.risk_lambda * terms["risk_increment_cash"]))
    evaluations = tuple(forged if e == original else e for e in eligible.evaluations)
    eligible = replace(eligible, evaluations=evaluations, work=tuple(
        replace(w, evaluation_ref=forged.content_hash) if w.evaluation_ref == original.content_hash else w
        for w in eligible.work))
    index_mean_artifact(case.repo, eligible, available_at_ns=eligible.sealed_at_ns)
    request = replace(request, eligible_set_ref=eligible.content_hash)
    index_mean_artifact(case.repo, request, available_at_ns=request.issued_at_ns)
    response = replace(response, request_ref=request.content_hash, eligible_set_ref=eligible.content_hash,
                       action_ref=forged.action_ref)
    return inp, forged, eligible, request, response


@pytest.mark.parametrize("field", ["mean_cash", "numerical_allowance", "execution_bias_allowance",
                                  "incremental_programme_cost", "risk_increment_cash", "j_cash"])
def test_n2_mutually_rehashed_saved_e_and_b_fail_after_trusted_fresh_import(case, tmp_path, field):
    _, _, eligible, request, response = forge_saved_evaluation(case, field)
    with pytest.raises(ValueError, match="CHECKED_PRODUCER_PROOF_UNAVAILABLE"):
        validate(case, eligible, request, response)
    owner = case.repo
    fresh = OpsRepository(tmp_path / "fresh.sqlite")
    import_population_evidence(fresh, trusted_source=owner, entries=tuple(reversed(snapshot_entries(owner))))
    fresh.close()
    case.repo = OpsRepository(tmp_path / "fresh.sqlite", read_only=True)
    # Saved object remains readable as diagnostic evidence, never eligible authority.
    from atlas.v2.science.mean_evaluation import read_mean_artifact

    assert read_mean_artifact(case.repo, eligible.content_hash, EligibleSetV1, consumer_ns=T+12) == eligible
    case.repo.close()
    case.repo = OpsRepository(tmp_path / "fresh.sqlite")
    with pytest.raises(ValueError, match="CHECKED_PRODUCER_PROOF_UNAVAILABLE"):
        validate(case, eligible, request, response)
    owner.close()


def test_n2_generic_rehashed_receipt_independently_recomputes_and_rolls_back(case):
    inp, forged, _, _, _ = forge_saved_evaluation(case, "mean_cash")
    # Reuse the genuine original inputs manifest; change every public receipt hash.
    from atlas.v2.science.mean_calibration_validation import validation_implementation_ref
    from atlas.v2.science.mean_evaluation_validation import MeanEvaluationInputsV1

    manifest = MeanEvaluationInputsV1(tuple(obj.content_hash for obj in vars(case.inputs[inp.candidate.content_hash]).values()))
    index_mean_artifact(case.repo, manifest, available_at_ns=forged.available_at_ns)
    receipt = MeanEvaluationValidationV1(forged.content_hash, manifest.content_hash,
        validation_implementation_ref(), forged.available_at_ns, case.population.deadline_ns)
    entry = evaluation_entry(receipt)
    from atlas.v2.science.mean_evaluation_validation import publish_evaluation_validation

    with pytest.raises(ValueError, match="NOT_DERIVED_FROM_CHECKED_INPUTS"):
        publish_evaluation_validation(case.repo, forged, case.inputs[inp.candidate.content_hash])
    sibling = ArtifactIndexEntryV2(H("n2-rollback"), "SyntheticP1Source", H("n2-body"), 0, 0, {})
    for entries in ((entry,), (sibling, entry)):
        with pytest.raises(ValueError, match="NOT_DERIVED_FROM_CHECKED_INPUTS"):
            case.repo.register_artifacts(entries)
        assert case.repo.get_artifact(entry.artifact_ref) is None
        assert case.repo.get_artifact(sibling.artifact_ref) is None
    with case.repo._transaction() as connection, pytest.raises(ValueError, match="checked offline"):
        case.repo.publish_stream_slice_v1(connection, publication_id="n2-forged", run_id="offline",
            descriptor_hash=H("n2-descriptor"), slice_id="slice", logical_ready_at_ns=T, entries=(entry,))


def test_n3_original_decisions_refuse_outcome_chosen_alternates_and_omission(case):
    inp, protocol, residuals, *_ = uncertain_chain(case)
    pred = read(case, residuals[0].prediction_ref, MeanOOFPredictionV1)
    training = tuple(read(case, r, MeanTrainingRowV1) for r in pred.training_row_refs)
    selected = []
    alternatives = []
    reg_ref = registration_ref(protocol.content_hash)
    for residual in residuals:
        row = read(case, residual.target_row_ref, MeanTrainingRowV1)
        entry = case.repo.get_artifact(slot_ref(reg_ref, row.origin_ns))
        slot = MeanPopulationSlotV1.from_dict(entry.metadata["mean_artifact"])
        rejected = next(ref for ref, status, _ in slot.decisions if status == "REJECTED")
        origin = read(case, rejected, MeanOriginEvidenceV1)
        evidence = publish(case, MeanOutcomeEvidenceV1(protocol.scope_ref, rejected, row.origin_ns,
            row.label_end_ns, row.label_available_at_ns, origin.features, D("5"), "FULL_FILL"))
        from atlas.v2.science.mean_calibration import MeanEvidenceScopeV1

        scope = read(case, protocol.scope_ref, MeanEvidenceScopeV1)
        alternate = evidence.training_row(scope)
        index_mean_artifact(case.repo, alternate, available_at_ns=alternate.label_available_at_ns)
        other = produce_verified_mean_oof(case.repo, (*training, alternate), protocol=protocol)[0]
        alternatives.append(other)
        selected.append(residual if row.target_cash > 5 else other)  # Deliberate outcome peeking.
    reader = EvidenceReader(case.repo, cutoff_ns=T, now_ns=T)
    _, values = verify_oof_residuals(reader, protocol, tuple(r.content_hash for r in selected))
    groups, *_ = dependence_diagnostics(values, protocol)
    assert selected_policy_upper_cash(groups, alpha=protocol.alpha, resamples=protocol.resamples) == 0
    assert inp.calibration.allowance_cash == D("3.333333333333333333333333333")
    with pytest.raises(ValueError, match="NOT_ORIGINALLY_SELECTED"):
        calibrate(case, inp, protocol, selected)
    with pytest.raises(ValueError, match="INCOMPLETE_OR_OUTCOME_CHOSEN"):
        calibrate(case, inp, protocol, residuals[:-1])
    assert verify(case, inp, inp.calibration)


def test_n3_original_records_cannot_be_minted_imported_or_rewritten_by_generic_apis(case, tmp_path):
    inp, _, protocol, _ = a_chain(case)
    entries = snapshot_entries(case.repo)
    originals = tuple(e for e in entries if e.artifact_type in ORIGINAL_TYPES)
    fresh = OpsRepository(tmp_path / "untrusted.sqlite")
    sibling = ArtifactIndexEntryV2(H("n3-rollback"), "SyntheticP1Source", H("n3-body"), 0, 0, {})
    for batch in ((originals[0],), (sibling, *originals), entries):
        with pytest.raises(ValueError, match="TRUSTED_RECORDER_OR_IMPORT"):
            fresh.register_artifacts(batch)
        assert fresh.get_artifact(sibling.artifact_ref) is None
        assert not snapshot_entries(fresh)
    for kind in ORIGINAL_TYPES:
        reserved = next(e for e in originals if e.artifact_type == kind)
        with pytest.raises(KeyError):  # Compact archive supports only its public-source kinds.
            fresh.register_public_archive_entries((reserved,), archive_chunk_id=H("not-a-public-archive"))
        assert fresh.get_artifact(reserved.artifact_ref) is None
    import_population_evidence(fresh, trusted_source=case.repo, entries=tuple(reversed(entries)))
    assert verify_selected_population(fresh, protocol, inp.calibration.calibration_refs, cutoff_ns=T)
    reg = MeanPopulationRegistrationV1.from_dict(case.repo.get_artifact(registration_ref(protocol.content_hash)).metadata["mean_artifact"])
    slot = MeanPopulationSlotV1.from_dict(case.repo.get_artifact(slot_ref(registration_ref(protocol.content_hash), reg.slots[0][0])).metadata["mean_artifact"])
    changed = replace(slot, decisions=tuple((r, "REJECTED", "retrospective rewrite") for r, _, _ in slot.decisions))
    original_entry = case.repo.get_artifact(slot_ref(registration_ref(protocol.content_hash), slot.slot_ns))
    bad = replace(original_entry, content_hash=changed.content_hash,
                  metadata={"mean_artifact": changed.to_dict(), "authority": "ZERO"})
    with pytest.raises(ValueError, match="NOT_IN_TRUSTED_SOURCE"):
        import_population_evidence(fresh, trusted_source=case.repo, entries=(bad,))
    with pytest.raises(ValueError, match="SELECTION_REWRITE"):
        record_population_slot(case.repo, protocol=protocol, slot_ns=slot.slot_ns, decisions=changed.decisions)
    fresh.close()


def test_n3_legacy_cohort_without_original_proof_remains_readable_without_authority(case, tmp_path):
    inp, _, protocol, _ = a_chain(case)
    from atlas.v2.science.mean_calibration_validation import MeanCalibrationValidationV2

    fresh = OpsRepository(tmp_path / "legacy.sqlite")
    fresh.register_artifacts(tuple(e for e in snapshot_entries(case.repo)
        if e.artifact_type not in (*ORIGINAL_TYPES, MeanCalibrationValidationV2.__name__)))
    from atlas.v2.science.mean_evaluation import read_mean_artifact

    assert read_mean_artifact(fresh, inp.calibration.content_hash, type(inp.calibration), consumer_ns=T) == inp.calibration
    with pytest.raises(ValueError, match="ORIGINAL_POPULATION_PROOF_UNAVAILABLE"):
        fresh.register_artifact(calibration_validation_entry(inp.calibration, inp.calibration.allowance_cash))
    with pytest.raises(ValueError, match="NOT_ORIGINAL"):
        record_population_registration(fresh, protocol=protocol, slots=((protocol.start_ns, protocol.start_ns,
            protocol.start_ns+1),), producer_id="backdated")
    fresh.close()


def test_positive_b_consumption_uses_checked_proofs_without_offline_recomputation(case, monkeypatch):
    from pathlib import Path

    eligible, request, response = request_response(case)
    import atlas.v2.science.mean_calibration as calibration_module
    import atlas.v2.science.mean_evaluation_validation as evaluation_module
    import atlas.v2.science.mean_population as population_module

    def forbidden(*args, **kwargs):
        raise AssertionError("offline verification/replay invoked on B deadline")

    monkeypatch.setattr(evaluation_module, "_evaluate_one", forbidden)
    monkeypatch.setattr(population_module, "verify_selected_population", forbidden)
    monkeypatch.setattr(calibration_module, "_verify_fitted_model", forbidden)
    monkeypatch.setattr(calibration_module, "selected_policy_upper_cash", forbidden)
    monkeypatch.setattr(Path, "read_bytes", forbidden)
    assert validate(case, eligible, request, response).decision == "SELECT_ONE"
    with pytest.raises(ValueError, match="CONSUMED"):
        validate(case, eligible, request, response)


def test_n1_serving_fit_and_estimator_cannot_borrow_m0_proof(case):
    inp, _, protocol, residuals = a_chain(case)
    for changes in (
        {"parameters": FrozenMap({"coefficients": [100., 0., 0., 0., 0., 0., 0.],
                                  "ridge": protocol.ridge, "objective": "squared_loss"})},
        {"scales": (2., *inp.model.scales[1:])},
        {"estimator": "EMPIRICAL_MEAN_V1", "parameters": FrozenMap({"mean_cash": "5"})},
        {"estimator": "M1_MEAN_LIGHTGBM_L2_V1", "parameters": FrozenMap({
            "trees": [{"leaf_value": 5.}], "objective": "regression_l2"})},
    ):
        forged = publish(case, replace(inp.model, **changes))
        with pytest.raises(ValueError, match="NOT_DERIVED_FROM_CLAIMED_TRAINING|SERVING_MODEL_CELL_CHANGED"):
            calibrate(case, replace(inp, model=forged), protocol, residuals)
    assert verify(case, inp, inp.calibration)


def test_n3_omitted_slot_import_is_atomic_and_updated_build_cannot_reuse_proof(case, tmp_path, monkeypatch):
    inp, _, protocol, _ = a_chain(case)
    entries = snapshot_entries(case.repo)
    missing = slot_ref(registration_ref(protocol.content_hash), protocol.start_ns)
    fresh = OpsRepository(tmp_path / "omitted-slot.sqlite")
    with pytest.raises(ValueError, match="ORIGINAL_POPULATION_PROOF_UNAVAILABLE"):
        import_population_evidence(fresh, trusted_source=case.repo,
                                  entries=tuple(e for e in entries if e.artifact_ref != missing))
    assert not snapshot_entries(fresh)
    fresh.close()
    import atlas.v2.science.mean_calibration_validation as validation_module

    monkeypatch.setattr(validation_module, "validation_implementation_ref", lambda: H("new-checked-build"))
    with pytest.raises(ValueError, match="VALIDATION_UNAVAILABLE"):
        verify(case, inp, inp.calibration)
    with pytest.raises(ValueError, match="PRODUCER_CHANGED"):
        verify_selected_population(case.repo, protocol, inp.calibration.calibration_refs, cutoff_ns=T)


def test_n3_original_outcome_cannot_be_replaced_and_late_or_censored_target_cannot_qualify(case):
    inp, _, protocol, residuals = a_chain(case)
    residual = residuals[0]
    prediction = read(case, residual.prediction_ref, MeanOOFPredictionV1)
    row = read(case, residual.target_row_ref, MeanTrainingRowV1)
    evidence = read(case, row.outcome_ref, MeanOutcomeEvidenceV1)
    from unittest.mock import patch

    from atlas.v2.science.mean_calibration import MeanEvidenceScopeV1
    from atlas.v2.science.mean_population import record_population_outcome

    scope = read(case, protocol.scope_ref, MeanEvidenceScopeV1)
    for changes in ({"target_cash": evidence.target_cash + 1}, {"execution_state": "CENSORED"},
                    {"available_at_ns": T + 1}):
        altered = publish(case, replace(evidence, **changes))
        altered_row = altered.training_row(scope)
        index_mean_artifact(case.repo, altered_row, available_at_ns=altered_row.label_available_at_ns)
        forged = publish(case, replace(residual, target_row_ref=altered_row.content_hash,
            available_at_ns=altered_row.label_available_at_ns))
        with pytest.raises(ValueError):
            calibrate(case, inp, protocol, (forged, *residuals[1:]))
        with patch("atlas.v2.science.mean_population._observed_now_ns", return_value=max(T+2, altered.available_at_ns)), pytest.raises(ValueError, match="OUTCOME_REWRITE"):
            record_population_outcome(case.repo, protocol=protocol, outcome=altered)
    # Genuine original fitting/prediction remains unchanged and qualified.
    assert prediction.prediction_cash == 5
    assert verify(case, inp, inp.calibration)


def test_original_ledger_retains_unavailable_empty_and_missed_slots_without_backdating(tmp_path):
    from unittest.mock import patch

    from atlas.v2.science.mean_calibration import FEATURE_REF, GRADE, MeanCalibrationProtocolV1, MeanEvidenceScopeV1

    from .evidence_fixtures import record_synthetic_slot, register_synthetic_population

    repo = OpsRepository(tmp_path / "original-ledger.sqlite")
    scope = MeanEvidenceScopeV1(H("ledger-product"), H("ledger-policy"), H("ledger-cell"), FEATURE_REF, DAY_NS,
        (1., 6., .01, 2., 1., 0.), (1., 8., .03, 6., 5., 1.), 0, 400*DAY_NS, "CAUSAL_REPLAY", GRADE)
    index_mean_artifact(repo, scope, available_at_ns=0)
    protocol = MeanCalibrationProtocolV1(scope.content_hash, H("ledger-holdout"), DAY_NS//2,
                                         100*DAY_NS, 320*DAY_NS, resamples=100)
    index_mean_artifact(repo, protocol, available_at_ns=protocol.registered_at_ns)
    slots = (100*DAY_NS, 107*DAY_NS, 114*DAY_NS)
    original = register_synthetic_population(repo, protocol, slots)
    origins = tuple(MeanOriginEvidenceV1(scope.content_hash, f"ledger-{k}", slots[0], slots[0],
                                        scope.support_min) for k in range(2))
    for origin in origins:
        index_mean_artifact(repo, origin, available_at_ns=origin.available_at_ns)
    slot = record_synthetic_slot(repo, protocol, slots[0], (
        (origins[0].content_hash, "SELECTED", "original choice"),
        (origins[1].content_hash, "UNAVAILABLE", "missing required deep evidence")))
    assert len(slot.origin_refs) == 2 and any(s == "UNAVAILABLE" for _, s, _ in slot.decisions)
    empty = record_synthetic_slot(repo, protocol, slots[1], ())
    assert empty.slot_status == "NO_CANDIDATE"
    # Actual late registration records only a miss, never an original selection.
    with patch("atlas.v2.science.mean_population._observed_now_ns", return_value=slots[2]+DAY_NS):
        with pytest.raises(ValueError, match="ORIGINAL_DEADLINE"):
            record_population_slot(repo, protocol=protocol, slot_ns=slots[2], decisions=())
        missed = record_population_slot(repo, protocol=protocol, slot_ns=slots[2], decisions=(),
                                        slot_status="NOT_EVALUATED_DEADLINE", reason="recorder unavailable at original slot")
        assert missed.recorded_at_ns == slots[2]+DAY_NS
        assert record_population_registration(repo, protocol=protocol, slots=original.slots,
            producer_id=original.producer_id).recorded_at_ns == original.recorded_at_ns
    repo.close()
