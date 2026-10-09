from __future__ import annotations

import hashlib
import sqlite3
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.frozen_action_comparison import (
    FrozenActionComparisonMaintenanceV1,
    _load_analogue,
    _load_m1,
)
from atlas.v2.runtime.ops_supervisor import (
    PIPELINE_STAGE_ORDER,
    OpsDecisionEventV1,
    OpsDecisionResultV1,
    OpsStageResultV1,
    OpsStageStatusV1,
    OpsSupervisorReceiptV1,
    OpsSupervisorV2,
    OpsTerminalStatusV1,
    PipelineStageV1,
)


def H(n: int) -> str:
    return f"{n:064x}"


DEFAULT_ACTION_REF = H(51)
NOW = 2_000
CUTOFF = 1_000
DEADLINE = 1_900


class MemoryRepo:
    read_only = False

    def __init__(self):
        self.entries = {}

    def get_artifact(self, ref):
        return self.entries.get(ref)

    def register_artifact(self, entry):
        self.entries[entry.artifact_ref] = entry
        return entry


@pytest.fixture(scope="module")
def real_pipeline(tmp_path_factory):
    """Use the real frozen case, M0 evaluator, M1 fitter and analogue runtime."""
    from atlas.v2.runtime.analogue_diagnostic import run_analogue_diagnostic_v1
    from atlas.v2.science.action import freeze_action
    from atlas.v2.science.admission import (
        ADMISSION_POLICY_VERSION,
        AdmissionPolicyV2,
        VenueCapabilitySnapshotV2,
    )
    from atlas.v2.science.evaluation_service import run_phase2_economic_evaluation
    from atlas.v2.science.m1 import fit_m1
    from atlas.v2.science.pretrade import CausalInputV2
    from atlas.v2.strategies.s1_trend import S1_POLICY

    from .session023_support import research_case
    from .test_session014_core import KEY
    from .test_session017_risk import CUTOFF, size

    repo = OpsRepository(tmp_path_factory.mktemp("frozen-action") / "ops.sqlite")
    case = research_case(repo)
    sizing = size(repo, case)
    action = freeze_action(repo, candidate=case.candidate, candidate_set=case.candidate_set,
        sizing=sizing, product=case.product, policy=S1_POLICY, v1=case.v1, v2=case.v2)

    def causal(kind):
        body = {"kind": kind, "decision_cutoff_ns": CUTOFF, "synthetic": True}
        ref = sha256_json(body)
        repo.register_artifact(ArtifactIndexEntryV2(ref, kind, ref, CUTOFF, CUTOFF, body))
        return CausalInputV2(ref, kind, CUTOFF, CUTOFF)

    profiles = tuple(sha256_json(["session041-comparison-profile", name])
        for name in ("runtime", "execution", "protection"))
    admission_policy = AdmissionPolicyV2(ADMISSION_POLICY_VERSION, Decimal(1), 30, 30, 20,
        Decimal("1.96"), "ISOLATED", "ONE_WAY", "nautilus_trader", "2.0.0rc5",
        "1b0a49d2792a9432a3aca3fcb617ce7a630d905e", *profiles, "SESSION041_TEST_CAPABILITY_V1")
    capability = VenueCapabilitySnapshotV2(KEY.venue, KEY.environment, case.account.account_scope,
        action.action.product_ref, KEY.content_hash, "ISOLATED", "ONE_WAY", "nautilus_trader",
        "2.0.0rc5", "1b0a49d2792a9432a3aca3fcb617ce7a630d905e", *profiles,
        "SESSION041_TEST_CAPABILITY_V1", "UNVERIFIED", (), CUTOFF)
    evaluated = run_phase2_economic_evaluation(repo, action=action, candidate=case.candidate,
        candidate_set=case.candidate_set, sizing=sizing, product=case.product, risk_policy=case.v1,
        risk_policy_v2=case.v2, account=case.account, fee=case.fee,
        admission_policy=admission_policy, capability=capability,
        model_input=causal("FrozenComparisonModelFixtureV1"),
        calibration_input=causal("FrozenComparisonCalibrationFixtureV1"),
        execution_model_input=causal("FrozenComparisonExecutionFixtureV1"),
        available_at_ns=CUTOFF + 10, scenario_seed=23041, scenario_count=100)
    lock_hash = hashlib.sha256(Path("requirements-lock.txt").read_bytes()).hexdigest()
    m1_run = fit_m1(repo, action=action, candidate=case.candidate,
        candidate_set=case.candidate_set, cutoff_ns=CUTOFF, available_at_ns=CUTOFF + 1,
        dependency_lock_hash=lock_hash)
    analogue_run = run_analogue_diagnostic_v1(repo, action=action, candidate=case.candidate,
        candidate_set=case.candidate_set, cutoff_ns=CUTOFF, available_at_ns=CUTOFF + 10)
    yield SimpleNamespace(repo=repo, case=case, action=action, sizing=sizing, evaluated=evaluated,
        m1_run=m1_run, analogue_run=analogue_run, cutoff_ns=CUTOFF)
    repo.close()


def _full_receipt(inputs, *, created_at_ns):
    from atlas.v2.runtime.ops_supervisor import OpsDecisionEventV1, OpsDecisionResultV1

    case, action, evaluation, m1_run, analogue = (
        inputs.case, inputs.action, inputs.evaluated, inputs.m1_run, inputs.analogue_run)
    event = OpsDecisionEventV1(H(60), "FROZEN_COMPARISON_FIXTURE", "fixture", H(61),
        inputs.cutoff_ns - 2, inputs.cutoff_ns - 2, inputs.cutoff_ns - 1, inputs.cutoff_ns - 1,
        inputs.cutoff_ns, case.candidate.deadline_ns, ())
    refs = {
        PipelineStageV1.CANDIDATE_SET: (case.candidate_set.content_hash, None),
        PipelineStageV1.HARD_RISK: (inputs.sizing.content_hash, None),
        PipelineStageV1.FROZEN_ACTION: (action.content_hash, action.action.action_hash),
        PipelineStageV1.ECONOMIC_EVALUATION: (evaluation.evaluation_ref, action.action.action_hash),
        PipelineStageV1.M1_DIAGNOSTIC: (m1_run.prediction.content_hash, action.action.action_hash),
        PipelineStageV1.ANALOGUE_DIAGNOSTIC: (analogue.result_ref, action.action.action_hash),
        PipelineStageV1.DECISION_CALENDAR: (evaluation.calendar_ref, action.action.action_hash),
    }
    stages = []
    for stage in PIPELINE_STAGE_ORDER:
        if stage in refs:
            artifacts, action_hash = refs[stage]
            if stage == PipelineStageV1.ANALOGUE_DIAGNOSTIC:
                artifacts = (artifacts, analogue.retrieval_receipt_ref)
            else:
                artifacts = (artifacts,)
            stages.append(OpsStageResultV1(stage, OpsStageStatusV1.COMPLETE, artifacts,
                created_at_ns, bound_action_hash=action_hash))
        else:
            stages.append(OpsStageResultV1(stage, OpsStageStatusV1.SKIPPED, (), created_at_ns))
    result = OpsDecisionResultV1(tuple(stages), OpsTerminalStatusV1.NOT_ESTIMABLE)
    return OpsSupervisorReceiptV1("session041-fixture", H(62), event, "HEALTHY", (),
        result, created_at_ns)


def test_real_frozen_case_produces_persisted_zero_authority_comparison(real_pipeline):
    inputs = real_pipeline
    repo, action, case = inputs.repo, inputs.action, inputs.case
    prediction = inputs.m1_run.prediction
    model_fit = repo.get_artifact(prediction.model_fit_ref)
    assert model_fit is not None
    fit_body = model_fit.metadata["model_fit"]
    reservation = repo.get_artifact(fit_body["final_holdout_reservation_ref"])
    assert reservation is not None
    reservation_body = reservation.metadata["reservation"]
    calibration_entry = repo.get_artifact(prediction.calibration_ref)
    assert calibration_entry is not None
    calibration_body = calibration_entry.metadata["calibration"]
    assert calibration_body["cutoff_ns"] == reservation_body["start_ns"]
    assert calibration_body["cutoff_ns"] != inputs.cutoff_ns

    recovered_m1, reason = _load_m1(repo, prediction.content_hash, now_ns=inputs.cutoff_ns + 30,
        deadline_ns=case.candidate.deadline_ns, expected_action_hash=action.action.action_hash,
        expected_action_ref=action.content_hash, expected_candidate_ref=case.candidate.content_hash,
        expected_candidate_set_ref=case.candidate_set.content_hash, cutoff_ns=inputs.cutoff_ns)
    recovered_analogue, analogue_reason = _load_analogue(repo, inputs.analogue_run.result_ref,
        now_ns=inputs.cutoff_ns + 30, deadline_ns=case.candidate.deadline_ns,
        expected_action_hash=action.action.action_hash, expected_action_ref=action.content_hash,
        expected_candidate_ref=case.candidate.content_hash,
        expected_candidate_set_ref=case.candidate_set.content_hash, cutoff_ns=inputs.cutoff_ns)
    assert reason is None and analogue_reason is None
    assert recovered_m1.prediction.content_hash == prediction.content_hash
    assert recovered_analogue.content_hash == inputs.analogue_run.result_ref

    receipt = _full_receipt(inputs, created_at_ns=inputs.cutoff_ns + 20)
    receipt_ref = OpsSupervisorV2._persist_final_receipt(repo, receipt)
    assert repo.due_work_items("FROZEN_ACTION_COMPARISON_V1", as_of_ns=inputs.cutoff_ns + 30,
        limit=1)[0].source_ref == receipt_ref
    work = FrozenActionComparisonMaintenanceV1(repo,
        clock_ns=lambda: inputs.cutoff_ns + 30).run_one()
    assert work.status == "COMPLETE" and work.comparison_ref
    comparison_entry = repo.get_artifact(work.comparison_ref)
    assert comparison_entry is not None and comparison_entry.artifact_type == "FROZEN_ACTION_M0_M1_ANALOGUE_COMPARISON_V2_V1"
    comparison = comparison_entry.metadata["comparison"]
    assert comparison["action_hash"] == action.action.action_hash
    assert comparison["action_artifact_ref"] == action.content_hash
    assert comparison["candidate_ref"] == case.candidate.content_hash
    assert comparison["candidate_set_ref"] == case.candidate_set.content_hash
    assert comparison["decision_cutoff_ns"] == inputs.cutoff_ns
    assert comparison["capital_authority"] == "ZERO" and comparison["model_voting"] is False
    assert comparison["model_averaging"] is False and comparison["incremental_difference_vs_m0"] is None
    assert repo.due_work_pressure("FROZEN_ACTION_COMPARISON_V1", as_of_ns=inputs.cutoff_ns + 30)["pending_count"] == 0


def test_absent_or_future_model_artifact_is_explicit_not_estimable(real_pipeline):
    repo = MemoryRepo()
    missing, reason = _load_m1(repo, H(30), now_ns=NOW, deadline_ns=DEADLINE,
        expected_action_hash=H(1), expected_action_ref=H(2), expected_candidate_ref=H(3),
        expected_candidate_set_ref=H(4), cutoff_ns=CUTOFF)
    assert missing is None and reason == "MISSING_M1PREDICTIONV2_ARTIFACT"
    inputs = real_pipeline
    m1_ref = inputs.m1_run.prediction.content_hash
    entry = inputs.repo.get_artifact(m1_ref)
    repo.entries[m1_ref] = ArtifactIndexEntryV2(entry.artifact_ref, entry.artifact_type,
        entry.content_hash, entry.created_at_ns, inputs.case.candidate.deadline_ns + 1, entry.metadata)
    future, reason = _load_m1(repo, m1_ref, now_ns=inputs.case.candidate.deadline_ns + 2,
        deadline_ns=inputs.case.candidate.deadline_ns, expected_action_hash=inputs.action.action.action_hash,
        expected_action_ref=inputs.action.content_hash, expected_candidate_ref=inputs.case.candidate.content_hash,
        expected_candidate_set_ref=inputs.case.candidate_set.content_hash, cutoff_ns=inputs.cutoff_ns)
    assert future is None and reason == "LATE_M1PREDICTIONV2_ARTIFACT"


def test_wrong_action_or_cutoff_is_rejected_before_comparison(real_pipeline):
    inputs = real_pipeline
    repo = inputs.repo
    m1_ref = inputs.m1_run.prediction.content_hash
    analogue_ref = inputs.analogue_run.result_ref
    _, reason = _load_m1(repo, m1_ref, now_ns=inputs.cutoff_ns + 30,
        deadline_ns=inputs.case.candidate.deadline_ns, expected_action_hash=H(99),
        expected_action_ref=inputs.action.content_hash, expected_candidate_ref=inputs.case.candidate.content_hash,
        expected_candidate_set_ref=inputs.case.candidate_set.content_hash, cutoff_ns=inputs.cutoff_ns)
    assert reason == "MISMATCHED_M1_PREDICTION"
    _, reason = _load_analogue(repo, analogue_ref, now_ns=inputs.cutoff_ns + 30,
        deadline_ns=inputs.case.candidate.deadline_ns, expected_action_hash=inputs.action.action.action_hash,
        expected_action_ref=inputs.action.content_hash, expected_candidate_ref=inputs.case.candidate.content_hash,
        expected_candidate_set_ref=inputs.case.candidate_set.content_hash, cutoff_ns=inputs.cutoff_ns + 1)
    assert reason == "MISMATCHED_ANALOGUE_ACTION_VALUE"


def _receipt(now=1_200, *, with_action=False, event_num=40, action_ref=DEFAULT_ACTION_REF):
    event = OpsDecisionEventV1(H(event_num), "FIXTURE", "fixture", H(event_num + 1), 900, 900,
        1_000, 1_000, CUTOFF, DEADLINE, ())
    stages = []
    for stage in PIPELINE_STAGE_ORDER:
        if with_action and stage == PipelineStageV1.CANDIDATE_SET:
            stages.append(OpsStageResultV1(stage, OpsStageStatusV1.COMPLETE, (H(49),), now))
        elif with_action and stage == PipelineStageV1.HARD_RISK:
            stages.append(OpsStageResultV1(stage, OpsStageStatusV1.COMPLETE, (H(50),), now))
        elif with_action and stage == PipelineStageV1.FROZEN_ACTION:
            stages.append(OpsStageResultV1(stage, OpsStageStatusV1.COMPLETE, (action_ref,), now,
                bound_action_hash=H(52)))
        else:
            stages.append(OpsStageResultV1(stage, OpsStageStatusV1.SKIPPED, (), now))
    result = OpsDecisionResultV1(tuple(stages),
        OpsTerminalStatusV1.NOT_ESTIMABLE if with_action else OpsTerminalStatusV1.NO_CANDIDATE)
    return OpsSupervisorReceiptV1("test", H(event_num + 2), event, "HEALTHY", (), result, now)


def test_missing_inputs_seal_once_and_restart_does_not_repeat_work(tmp_path: Path):
    path = tmp_path / "ops.sqlite"
    repo = OpsRepository(path)
    receipt = _receipt(with_action=True)
    receipt_ref = OpsSupervisorV2._persist_final_receipt(repo, receipt)
    assert receipt_ref != receipt.content_hash
    assert repo.due_work_items("FROZEN_ACTION_COMPARISON_V1", as_of_ns=NOW, limit=1)[0].source_ref == receipt_ref
    def clock():
        return NOW

    first = FrozenActionComparisonMaintenanceV1(repo, clock_ns=clock).run_one()
    assert first.status == "NOT_ESTIMABLE"
    sealed = repo.get_artifact(first.receipt_ref)
    assert sealed and sealed.metadata["comparison_receipt"]["authority"] == "ZERO"
    assert "MISSING_ACTIONARTIFACTV2_ARTIFACT" in sealed.metadata["comparison_receipt"]["reason_codes"]
    assert repo.due_work_pressure("FROZEN_ACTION_COMPARISON_V1", as_of_ns=NOW)["pending_count"] == 0
    repo.close()

    reopened = OpsRepository(path)
    again = FrozenActionComparisonMaintenanceV1(reopened, clock_ns=clock).run_one()
    assert again.status == "IDLE"
    assert len(reopened.artifact_entries_by_types(("FrozenActionComparisonMaintenanceReceiptV1",))) == 1
    reopened.close()


def test_supervisor_scheduling_excludes_no_action_receipts(tmp_path: Path):
    repo = OpsRepository(tmp_path / "ops.sqlite")
    no_action = _receipt()
    OpsSupervisorV2._persist_final_receipt(repo, no_action)
    assert repo.due_work_items("FROZEN_ACTION_COMPARISON_V1", as_of_ns=NOW, limit=1) == ()
    repo.close()


def test_maintenance_uses_only_one_bounded_due_work_lookup():
    class NoScanRepository:
        read_only = False
        past_completed_origin_count = 10_001

        def due_work_items(self, lane, *, as_of_ns, limit):
            assert lane == "FROZEN_ACTION_COMPARISON_V1" and limit == 1
            return ()

        def artifact_entries_by_types(self, *args, **kwargs):
            raise AssertionError("maintenance must not scan completed receipt history")

    assert FrozenActionComparisonMaintenanceV1(NoScanRepository(), clock_ns=lambda: NOW).run_one().status == "IDLE"


def test_corrupt_source_is_sealed_and_retired_before_next_due_origin(tmp_path: Path, real_pipeline):
    repo = OpsRepository(tmp_path / "corrupt-source.sqlite")
    corrupted_receipt = _receipt(now=real_pipeline.cutoff_ns + 20, with_action=True,
        event_num=70, action_ref=H(80))
    OpsSupervisorV2._persist_final_receipt(repo, corrupted_receipt)
    source_types = ("ActionArtifactV2", "CandidateActionV2", "CandidateSetV2", "EvaluationArtifactV2",
        "M0PredictionV2", "M1PredictionV2", "M1ModelFitV2", "M1SupportV2", "M1CalibrationV2",
        "M1OODV2", "M1FinalHoldoutReservationV2", "M1OOFArchiveV2", "M1ChronologyV2",
        "AnalogueActionValueV2")
    for source_entry in real_pipeline.repo.artifact_entries_by_types(source_types, limit=10_000):
        repo.register_artifact(source_entry)
    valid_receipt = _full_receipt(real_pipeline, created_at_ns=real_pipeline.cutoff_ns + 21)
    valid_receipt_ref = OpsSupervisorV2._persist_final_receipt(repo, valid_receipt)
    bad_action_ref = H(80)
    repo.register_artifact(ArtifactIndexEntryV2(bad_action_ref, "ActionArtifactV2", bad_action_ref,
        real_pipeline.cutoff_ns + 19, real_pipeline.cutoff_ns + 19,
        {"action_artifact": {"fixture": "will be corrupted"}}))
    # Simulate invalid persisted JSON in the actual artifact index. This makes
    # OpsRepository.get_artifact raise JSONDecodeError before body validation.
    with repo._lock:
        repo._connection.execute("UPDATE artifact_index SET metadata_json=? WHERE artifact_ref=?",
            ("{broken-json", bad_action_ref))
    def clock():
        return real_pipeline.cutoff_ns + 30

    maintenance = FrozenActionComparisonMaintenanceV1(repo, clock_ns=clock)
    first = maintenance.run_one()
    assert first.processed_receipt_ref is not None and first.status == "NOT_ESTIMABLE"
    first_body = repo.get_artifact(first.receipt_ref).metadata["comparison_receipt"]
    assert first_body["reason_codes"] == ("INVALID_SOURCE_ARTIFACT",)
    assert repo.due_work_items("FROZEN_ACTION_COMPARISON_V1", as_of_ns=clock(), limit=1)[0].source_ref == valid_receipt_ref

    second = maintenance.run_one()
    assert second.processed_receipt_ref is not None and second.processed_receipt_ref != first.processed_receipt_ref
    second_body = repo.get_artifact(second.receipt_ref).metadata["comparison_receipt"]
    assert second.processed_receipt_ref == valid_receipt_ref
    assert second.status == "COMPLETE" and second_body["comparison_ref"] == second.comparison_ref, second_body["reason_codes"]
    assert repo.due_work_pressure("FROZEN_ACTION_COMPARISON_V1", as_of_ns=clock())["pending_count"] == 0
    repo.close()


def test_sqlite_operational_failure_keeps_due_work_pending(tmp_path: Path, monkeypatch):
    repo = OpsRepository(tmp_path / "sqlite-failure.sqlite")
    receipt = _receipt(now=1_200, with_action=True, event_num=77, action_ref=H(88))
    OpsSupervisorV2._persist_final_receipt(repo, receipt)
    get_artifact = repo.get_artifact

    def fail_on_action(ref):
        if ref == H(88):
            raise sqlite3.OperationalError("fixture storage failure")
        return get_artifact(ref)

    monkeypatch.setattr(repo, "get_artifact", fail_on_action)
    with pytest.raises(sqlite3.OperationalError):
        FrozenActionComparisonMaintenanceV1(repo, clock_ns=lambda: 2_000).run_one()
    assert repo.due_work_pressure("FROZEN_ACTION_COMPARISON_V1", as_of_ns=2_000)["pending_count"] == 1
    monkeypatch.setattr(repo, "get_artifact", get_artifact)
    repo.close()


def test_corrupt_source_is_failure_isolated_into_not_estimable_reason(real_pipeline):
    repo = MemoryRepo()
    inputs = real_pipeline
    m1_ref = inputs.m1_run.prediction.content_hash
    original = inputs.repo.get_artifact(m1_ref)
    entry = original
    corrupt = dict(entry.metadata["prediction"])
    corrupt["expected_net_value"] = "not-a-decimal"
    repo.entries[m1_ref] = ArtifactIndexEntryV2(entry.artifact_ref, entry.artifact_type,
        entry.content_hash, entry.created_at_ns, entry.available_at_ns, {"prediction": corrupt})
    prediction, reason = _load_m1(repo, m1_ref, now_ns=inputs.cutoff_ns + 30,
        deadline_ns=inputs.case.candidate.deadline_ns, expected_action_hash=inputs.action.action.action_hash,
        expected_action_ref=inputs.action.content_hash, expected_candidate_ref=inputs.case.candidate.content_hash,
        expected_candidate_set_ref=inputs.case.candidate_set.content_hash, cutoff_ns=inputs.cutoff_ns)
    assert prediction is None and reason == "INVALID_M1PREDICTIONV2_ARTIFACT"


def test_malformed_holdout_reservation_chronology_is_fail_closed(real_pipeline, monkeypatch):
    import atlas.v2.runtime.frozen_action_comparison as comparison

    original_artifact = comparison._artifact

    def invalid_reservation(repository, ref, kind, **kwargs):
        entry, body, reason = original_artifact(repository, ref, kind, **kwargs)
        if kind == "M1FinalHoldoutReservationV2" and body is not None:
            return entry, {**body, "end_ns": None}, None
        return entry, body, reason

    monkeypatch.setattr(comparison, "_artifact", invalid_reservation)
    inputs = real_pipeline
    prediction, reason = _load_m1(inputs.repo, inputs.m1_run.prediction.content_hash,
        now_ns=inputs.cutoff_ns + 30, deadline_ns=inputs.case.candidate.deadline_ns,
        expected_action_hash=inputs.action.action.action_hash,
        expected_action_ref=inputs.action.content_hash,
        expected_candidate_ref=inputs.case.candidate.content_hash,
        expected_candidate_set_ref=inputs.case.candidate_set.content_hash,
        cutoff_ns=inputs.cutoff_ns)

    assert prediction is None
    assert reason == "INVALID_M1_HOLDOUT_RESERVATION_CHRONOLOGY"
