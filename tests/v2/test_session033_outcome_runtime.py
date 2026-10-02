from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.risk import index_research_evidence
from atlas.v2.runtime import outcome_maturity
from atlas.v2.runtime.ops_supervisor import (
    OpsCycleBatchV1,
    OpsCyclePortV1,
    OpsDecisionEventV1,
    OpsDecisionResultV1,
    OpsRecoverySnapshotV1,
    OpsRunResultV1,
    OpsSupervisorV2,
)
from atlas.v2.science import outcomes as outcome_contract
from atlas.v2.science.outcomes import (
    AdmissionStateV2,
    DecisionCalendarEntryV2,
    DecisionSourceStageV2,
    DiagnosticTargetEvidenceV2,
    SelectionStateV2,
    index_decision_calendar_entry,
    index_diagnostic_target_evidence,
)
from atlas.v2.strategies.s2_breakout import S2_POLICY
from tests.v2.test_session016_candidate_selection import CUTOFF
from tests.v2.test_session016_candidate_selection import candidate as candidate_for_cutoff
from tests.v2.test_session017_risk import risk_case
from tests.v2.test_session018_remediation import _payoff_case
from tests.v2.test_session033_outcome_maturity import _calendar as maturity_calendar


class _Clock:
    def __init__(self, now_ns: int = 1_800_000_000_000_000_000) -> None:
        self.now_ns = now_ns

    def __call__(self) -> int:
        return self.now_ns


class _SingleEventPort(OpsCyclePortV1):
    def __init__(self, now_ns: int) -> None:
        self.event = OpsDecisionEventV1(
            sha256_json({"session033_event": now_ns}),
            "BAR_CLOSE_15M",
            "FIXTURE",
            sha256_json({"session033_trigger": now_ns}),
            now_ns,
            None,
            now_ns,
            now_ns,
            now_ns,
            now_ns + 1_000,
            (),
        )

    def recover(self, repository: OpsRepository, *, now_ns: int) -> OpsRecoverySnapshotV1:
        return OpsRecoverySnapshotV1((), (), (), None, True, now_ns)

    def collect(
        self, repository: OpsRepository, *, now_ns: int, recovery: OpsRecoverySnapshotV1
    ) -> OpsCycleBatchV1:
        return OpsCycleBatchV1((self.event,), (), (), (), True, now_ns)

    def process_event(
        self,
        repository: OpsRepository,
        event: OpsDecisionEventV1,
        *,
        now_ns: int,
        source_health_state: str,
        completed_stages: Any,
        checkpoint: Any,
    ) -> OpsDecisionResultV1:
        raise AssertionError("fixture source health must close the event before pipeline processing")


def _prepare_advancing_maturity_inputs(repository: OpsRepository) -> dict[str, Any]:
    _case, _action, payoff, fixture_outcome = _payoff_case(repository)
    late_decision_at_ns = CUTOFF + 1
    def make_late_candidate(_repository, _universe, product):
        return candidate_for_cutoff(
            key=product.key,
            decision_at_ns=late_decision_at_ns,
            deadline_ns=late_decision_at_ns + 5_000_000_000,
        )

    late_s2_candidate = candidate_for_cutoff(
        policy=S2_POLICY,
        serial=1,
        decision_at_ns=late_decision_at_ns,
        deadline_ns=late_decision_at_ns + 5_000_000_000,
    )
    case = risk_case(
        repository,
        cutoff_ns=late_decision_at_ns,
        candidate_factory=make_late_candidate,
        additional_candidates=(late_s2_candidate,),
    )
    candidate = (
        case.s2_candidate
        if case.candidate_set.selected_candidate_id == case.candidate.candidate_id
        else case.candidate
    )
    assert candidate is not None
    selection = next(
        row for row in case.candidate_set.candidates
        if row.candidate_id == candidate.candidate_id
    )
    decision = DecisionCalendarEntryV2(
        candidate_set_ref=case.candidate_set.content_hash,
        candidate_ref=candidate.content_hash,
        policy_id=selection.policy_id,
        policy_version="1",
        policy_hash=candidate.policy_hash,
        decision_at_ns=late_decision_at_ns,
        selection_state=SelectionStateV2.UNSELECTED,
        admission_state=AdmissionStateV2.NOT_APPLICABLE,
        action_hash=None,
        action_artifact_ref=None,
        source_stage=DecisionSourceStageV2.CANDIDATE_SET,
        reason_codes=(),
        source_artifact_ref=case.candidate_set.content_hash,
        created_at_ns=late_decision_at_ns,
        available_at_ns=late_decision_at_ns,
    )
    decision_ref = index_decision_calendar_entry(repository, decision)
    declaration = {"label_definition": "future_mid_return_v1", "unit": "FRACTION"}
    declaration_ref = sha256_json(declaration)
    index_research_evidence(
        repository, "DiagnosticTargetDefinitionV2", declaration_ref, late_decision_at_ns, declaration,
    )
    source_body = {"candidate_ref": candidate.content_hash, "value": "0.02"}
    return {
        "cutoff_ns": max(payoff.available_at_ns + 7, candidate.horizon_end_ns),
        "decision_ref": decision_ref,
        "decision_at_ns": late_decision_at_ns,
        "candidate_set_ref": case.candidate_set.content_hash,
        "candidate_ref": candidate.content_hash,
        "candidate_horizon_ns": candidate.horizon_end_ns,
        "declaration_ref": declaration_ref,
        "source_body": source_body,
        "source_ref": sha256_json(source_body),
        "outcome_decision_ref": fixture_outcome.decision_ref,
        "outcome_horizon_ns": fixture_outcome.horizon_end_ns,
    }


def _install_future_diagnostic(
    repository: OpsRepository,
    inputs: dict[str, Any],
    available_at_ns: int,
) -> str:
    index_research_evidence(
        repository, "CausalMarketDiagnosticV2", inputs["source_ref"], available_at_ns,
        inputs["source_body"],
    )
    diagnostic = DiagnosticTargetEvidenceV2(
        decision_ref=inputs["decision_ref"],
        candidate_set_ref=inputs["candidate_set_ref"],
        candidate_ref=inputs["candidate_ref"],
        label_definition="future_mid_return_v1",
        target_declaration_ref=inputs["declaration_ref"],
        decision_at_ns=inputs["decision_at_ns"],
        horizon_end_ns=inputs["candidate_horizon_ns"],
        value=Decimal("0.02"),
        unit="FRACTION",
        source_refs=(inputs["source_ref"],),
        completed_at_ns=available_at_ns,
        available_at_ns=available_at_ns,
    )
    return index_diagnostic_target_evidence(repository, diagnostic)


def test_downstream_maintenance_uses_writer_after_receipt_is_sealed(tmp_path: Path) -> None:
    now_ns = 1_800_000_000_000_000_000
    clock = _Clock(now_ns)
    seen: list[OpsRepository] = []

    def maintenance(repository: OpsRepository, attempted_at_ns: int) -> None:
        assert attempted_at_ns == now_ns
        assert len(repository.artifact_entries("OpsSupervisorCycleReceiptV1")) == 1
        assert len(repository.artifact_entries("OpsSupervisorReceiptV1")) == 1
        seen.append(repository)
        body = {"attempted_at_ns": attempted_at_ns}
        ref = sha256_json(body)
        repository.register_artifact(
            ArtifactIndexEntryV2(ref, "S33MaintenanceProbeV1", ref, attempted_at_ns, attempted_at_ns, body)
        )

    supervisor = OpsSupervisorV2(
        tmp_path / "ops.sqlite",
        _SingleEventPort(now_ns),
        clock_ns=clock,
        post_cycle_maintenance=maintenance,
    )
    with supervisor:
        result = supervisor.run_once()
        assert supervisor.repository is seen[0]
    assert len(result.event_receipts) == 1
    with OpsRepository(tmp_path / "ops.sqlite", read_only=True) as reader:
        assert len(reader.artifact_entries("S33MaintenanceProbeV1")) == 1


def test_maintenance_failure_is_sanitized_and_cannot_change_decision_receipt(tmp_path: Path) -> None:
    now_ns = 1_800_000_000_000_000_000
    secret = "external-content-that-must-not-be-persisted"

    baseline = OpsSupervisorV2(
        tmp_path / "baseline.sqlite",
        _SingleEventPort(now_ns),
        clock_ns=_Clock(now_ns),
        post_cycle_maintenance=lambda _repository, _attempted_at_ns: None,
    )
    with baseline:
        baseline_result: OpsRunResultV1 = baseline.run_once()

    def fail(_repository: OpsRepository, _attempted_at_ns: int) -> None:
        raise RuntimeError(secret)

    supervisor = OpsSupervisorV2(
        tmp_path / "ops.sqlite",
        _SingleEventPort(now_ns),
        clock_ns=_Clock(now_ns),
        post_cycle_maintenance=fail,
    )
    with supervisor:
        result: OpsRunResultV1 = supervisor.run_once()

    assert len(result.event_receipts) == 1
    assert result.cycle == baseline_result.cycle
    assert result.event_receipts == baseline_result.event_receipts
    assert result.cycle.failure_types == ()
    with OpsRepository(tmp_path / "ops.sqlite", read_only=True) as reader:
        receipt_rows = reader.artifact_entries("OpsSupervisorReceiptV1")
        failures = reader.artifact_entries("OpsOutcomeMaturityFailureV1")
        assert len(receipt_rows) == 1
        assert len(failures) == 1
        assert failures[0].metadata["failure_type"] == "RuntimeError"
        assert failures[0].metadata["reason_code"] == "OUTCOME_MATURITY_CYCLE_FAILED"
        assert secret not in str(failures[0].metadata)


def test_default_supervisor_uses_outcome_maturity_coordinator(tmp_path: Path, monkeypatch: Any) -> None:
    from atlas.v2.runtime import outcome_maturity

    seen: list[tuple[OpsRepository, int, Any, Any, int]] = []

    def maintenance(
        repository: OpsRepository,
        *,
        evidence_cutoff_ns: int,
        production_clock_ns: Any,
        monotonic_ns: Any,
        maintenance_budget_ns: int,
    ) -> object:
        seen.append((repository, evidence_cutoff_ns, production_clock_ns, monotonic_ns, maintenance_budget_ns))
        return outcome_maturity.OutcomeMaturityCycleReportV1(
            cycle_at_ns=production_clock_ns(),
            evidence_cutoff_ns=evidence_cutoff_ns,
            computation_started_ns=evidence_cutoff_ns,
            computation_finished_ns=evidence_cutoff_ns,
            maintenance_budget_ns=maintenance_budget_ns,
            decisions_inspected=3,
            pending_count=1,
            unresolved_count=2,
            bounded_work_exhausted=True,
        )

    monkeypatch.setattr(outcome_maturity, "run_outcome_maturity_cycle", maintenance)
    now_ns = 1_800_000_000_000_000_000
    supervisor = OpsSupervisorV2(tmp_path / "ops.sqlite", _SingleEventPort(now_ns), clock_ns=_Clock(now_ns))
    with supervisor:
        supervisor.run_once()
        assert len(seen) == 1
        assert seen[0][0] is supervisor.repository
        assert seen[0][1] == now_ns
        assert seen[0][2] is supervisor.clock_ns
        assert seen[0][3] is supervisor.monotonic_ns
        assert seen[0][4] == outcome_maturity.OUTCOME_MAINTENANCE_BUDGET_NS_V1
    with OpsRepository(tmp_path / "ops.sqlite", read_only=True) as reader:
        reports = reader.artifact_entries("OutcomeMaturityCycleReportV1")
        assert len(reports) == 1
        assert reports[0].metadata["report"]["pending_count"] == 1
        assert reports[0].metadata["report"]["unresolved_count"] == 2
        assert reports[0].available_at_ns >= reports[0].metadata["report"]["cycle_at_ns"]


def test_supervisor_advancing_clock_keeps_fixed_cutoff_and_sealed_receipts(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    actual_maintenance = outcome_maturity.run_outcome_maturity_cycle

    def prepare(path: Path) -> dict[str, Any]:
        with OpsRepository(path) as repository:
            return _prepare_advancing_maturity_inputs(repository)

    baseline_path = tmp_path / "timing-baseline.sqlite"
    baseline_inputs = prepare(baseline_path)
    t0 = baseline_inputs["cutoff_ns"]
    t1, t2, t3, t4 = t0 + 1, t0 + 2, t0 + 3, t0 + 4

    def install_after_receipt(repository: OpsRepository, inputs: dict[str, Any]) -> None:
        assert len(repository.artifact_entries("OpsSupervisorReceiptV1")) == 1
        assert len(repository.artifact_entries("OpsSupervisorCycleReceiptV1")) == 1
        _install_future_diagnostic(repository, inputs, t1)

    baseline = OpsSupervisorV2(
        baseline_path,
        _SingleEventPort(t0),
        clock_ns=_Clock(t0),
        post_cycle_maintenance=lambda repository, _cutoff: install_after_receipt(repository, baseline_inputs),
    )
    with baseline:
        baseline_result = baseline.run_once()

    treatment_path = tmp_path / "timing-treatment.sqlite"
    treatment_inputs = prepare(treatment_path)
    assert treatment_inputs["cutoff_ns"] == t0

    class TimelineClock:
        def __init__(self) -> None:
            self.started = False
            self.maintenance_started = False
            self.validation_finished = False
            self.events: list[tuple[str, int]] = []

        def __call__(self) -> int:
            if not self.started:
                self.started = True
                self.events.append(("cycle_started", t0))
                return t0
            if not self.maintenance_started or not self.validation_finished:
                return t2
            return t4

        def mark_receipts_sealed(self) -> None:
            self.events.append(("receipts_sealed", t1))
            self.maintenance_started = True

        def mark_validation_finished(self) -> None:
            self.events.append(("validation_finished", t3))
            self.validation_finished = True

    timeline = TimelineClock()
    late_diagnostic_refs: list[str] = []

    def maintenance(
        repository: OpsRepository,
        *,
        evidence_cutoff_ns: int,
        production_clock_ns: Any,
        monotonic_ns: Any,
        maintenance_budget_ns: int,
    ) -> object:
        assert evidence_cutoff_ns == t0
        timeline.mark_receipts_sealed()
        late_diagnostic_refs.append(_install_future_diagnostic(repository, treatment_inputs, t1))
        return actual_maintenance(
            repository,
            evidence_cutoff_ns=evidence_cutoff_ns,
            production_clock_ns=production_clock_ns,
            monotonic_ns=monotonic_ns,
            maintenance_budget_ns=maintenance_budget_ns,
        )

    monkeypatch.setattr(outcome_maturity, "run_outcome_maturity_cycle", maintenance)
    validate = outcome_contract._validate_policy_payoff

    def validate_then_advance(repository, outcome, identity):
        validate(repository, outcome, identity)
        timeline.mark_validation_finished()

    monkeypatch.setattr(outcome_contract, "_validate_policy_payoff", validate_then_advance)
    supervisor = OpsSupervisorV2(
        treatment_path,
        _SingleEventPort(t0),
        clock_ns=timeline,
        monotonic_ns=lambda: 0,
    )
    with supervisor:
        repository = supervisor._ensure_open()
        late_evidence_cutoffs: list[int] = []
        query_identity = repository.artifact_entries_by_metadata_identity

        def track_identity_query(*args: Any, **kwargs: Any):
            if args[0] == "DiagnosticTargetEvidenceV2" and args[2] == treatment_inputs["decision_ref"]:
                late_evidence_cutoffs.append(kwargs["as_of_ns"])
            return query_identity(*args, **kwargs)

        monkeypatch.setattr(repository, "artifact_entries_by_metadata_identity", track_identity_query)
        result = supervisor.run_once()
        assert timeline.events[:3] == [
            ("cycle_started", t0),
            ("receipts_sealed", t1),
            ("validation_finished", t3),
        ]
        assert t0 < t1 <= t2 <= t3 <= t4
        assert set(late_evidence_cutoffs) == {t0}
        assert late_diagnostic_refs

        # Late outcome work cannot change the event, action or decision result.
        # Runtime completion timestamps may differ because the treatment clock
        # advances while the sealed receipt is being produced.
        assert result.cycle.event_ids == baseline_result.cycle.event_ids
        assert result.cycle.source_health_state == baseline_result.cycle.source_health_state
        assert result.event_receipts[0].event.content_hash == baseline_result.event_receipts[0].event.content_hash
        assert result.event_receipts[0].action_ref == baseline_result.event_receipts[0].action_ref
        assert result.event_receipts[0].result.terminal_status == baseline_result.event_receipts[0].result.terminal_status
        for actual_stage, baseline_stage in zip(
            result.event_receipts[0].result.stages, baseline_result.event_receipts[0].result.stages,
            strict=True,
        ):
            assert actual_stage.stage == baseline_stage.stage
            assert actual_stage.status == baseline_stage.status
            assert actual_stage.artifact_refs == baseline_stage.artifact_refs
            assert actual_stage.bound_action_hash == baseline_stage.bound_action_hash
            assert actual_stage.reason == baseline_stage.reason
        assert result.event_receipts[0].result.terminal_status.value == "NOT_ESTIMABLE"

        late_ref = treatment_inputs["decision_ref"]
        late_statuses = repository.artifact_entries_by_metadata_identity(
            "OutcomeMaturityStatusV1", ("status", "decision_ref"), late_ref,
            as_of_ns=t4, limit=8,
        ).entries
        assert len(late_statuses) == 1
        assert late_statuses[0].metadata["status"]["status"] == "UNRESOLVED"
        assert repository.artifact_entries_by_metadata_identity(
            "MaturedOutcomeV2", ("outcome", "decision_ref"), late_ref,
            as_of_ns=t4, limit=1,
        ).entries == ()

        expected_ref = treatment_inputs["outcome_decision_ref"]
        outcome_entries = repository.artifact_entries_by_metadata_identity(
            "MaturedOutcomeV2", ("outcome", "decision_ref"), expected_ref,
            as_of_ns=t4, limit=1,
        ).entries
        assert len(outcome_entries) == 1
        assert outcome_entries[0].available_at_ns >= t3
        assert outcome_entries[0].available_at_ns >= t4
        assert outcome_entries[0].available_at_ns > t0

        outcome_statuses = repository.artifact_entries_by_metadata_identity(
            "OutcomeMaturityStatusV1", ("status", "decision_ref"), expected_ref,
            as_of_ns=t4, limit=8,
        ).entries
        assert outcome_statuses
        assert all(item.available_at_ns >= t3 for item in outcome_statuses)
        checkpoint_entries = repository.artifact_entries("OutcomeMaturityCheckpointV1")
        assert checkpoint_entries
        assert max(item.available_at_ns for item in checkpoint_entries) >= t3
        reports = repository.artifact_entries("OutcomeMaturityCycleReportV1")
        assert reports and reports[0].available_at_ns >= t4

        # Later evidence is eligible once a subsequent maturity cycle has a later
        # evidence cutoff; the first fixed-cutoff attempt left it unresolved.
        later_cutoff = t4 + 1
        matured_late = False
        for _ in range(4):
            production_at = later_cutoff + 1
            actual_maintenance(
                repository,
                evidence_cutoff_ns=later_cutoff,
                production_clock_ns=lambda production_at=production_at: production_at,
                monotonic_ns=lambda: 0,
                maintenance_budget_ns=outcome_maturity.OUTCOME_MAINTENANCE_BUDGET_NS_V1,
            )
            status_entries = repository.artifact_entries_by_metadata_identity(
                "OutcomeMaturityStatusV1", ("status", "decision_ref"), late_ref,
                as_of_ns=production_at, limit=8,
            ).entries
            if any(item.metadata["status"]["status"] == "MATURED" for item in status_entries):
                late_outcomes = repository.artifact_entries_by_metadata_identity(
                    "MaturedOutcomeV2", ("outcome", "decision_ref"), late_ref,
                    as_of_ns=production_at, limit=1,
                ).entries
                matured_late = len(late_outcomes) == 1
                break
            later_cutoff = production_at + 1
        assert matured_late


@pytest.mark.parametrize("scenario", ("budget_exhaustion", "malformed_calendar"))
def test_downstream_maturity_degradation_preserves_supervisor_receipts(
    tmp_path: Path, monkeypatch: Any, scenario: str,
) -> None:
    now_ns = 1_800_000_000_000_000_000
    baseline_path = tmp_path / f"{scenario}-baseline.sqlite"
    treatment_path = tmp_path / f"{scenario}-treatment.sqlite"

    def prepare(path: Path) -> None:
        with OpsRepository(path) as repository:
            if scenario == "budget_exhaustion":
                maturity_calendar(repository, 1, at_ns=now_ns - 100)
            if scenario == "malformed_calendar":
                raw_ref = sha256_json({"malformed_runtime_calendar": scenario})
                repository._connection.execute(
                    "INSERT INTO artifact_index(artifact_ref,artifact_type,content_hash,created_at_ns,"
                    "available_at_ns,metadata_json) VALUES(?,?,?,?,?,?)",
                    (raw_ref, "DecisionCalendarEntryV2", raw_ref, now_ns - 2, now_ns - 2, "{malformed-json"),
                )

    prepare(baseline_path)
    prepare(treatment_path)
    baseline = OpsSupervisorV2(
        baseline_path, _SingleEventPort(now_ns), clock_ns=_Clock(now_ns),
        post_cycle_maintenance=lambda _repository, _cutoff: None,
    )
    with baseline:
        baseline_result = baseline.run_once()

    resolver_calls: list[str] = []

    def unexpected_resolver(_repository, entry, _cutoff, **_kwargs):
        resolver_calls.append(entry.artifact_ref)
        raise AssertionError("expired or malformed work must not start outcome resolution")

    monkeypatch.setattr(outcome_maturity, "resolve_decision_outcome", unexpected_resolver)
    monotonic_calls = [0]

    def monotonic_ns() -> int:
        monotonic_calls[0] += 1
        if scenario == "budget_exhaustion" and monotonic_calls[0] > 6:
            return 1
        return 0

    supervisor = OpsSupervisorV2(
        treatment_path,
        _SingleEventPort(now_ns),
        clock_ns=_Clock(now_ns),
        monotonic_ns=monotonic_ns,
        outcome_maintenance_budget_ns=1 if scenario == "budget_exhaustion" else None,
    )
    with supervisor:
        result = supervisor.run_once()

    assert result.cycle == baseline_result.cycle
    assert tuple(item.to_dict() for item in result.event_receipts) == tuple(
        item.to_dict() for item in baseline_result.event_receipts
    )
    assert result.event_receipts[0].result == baseline_result.event_receipts[0].result
    assert result.event_receipts[0].action_ref == baseline_result.event_receipts[0].action_ref
    assert result.cycle.failure_types == ()
    assert resolver_calls == []

    with OpsRepository(treatment_path, read_only=True) as reader:
        report = reader.artifact_entries("OutcomeMaturityCycleReportV1")[0].metadata["report"]
        if scenario == "budget_exhaustion":
            assert report["maintenance_budget_status"] == "MAINTENANCE_BUDGET_EXHAUSTED"
            assert report["decisions_inspected"] == 0
        else:
            assert report["failure_code"] == "MALFORMED_CALENDAR_INDEX_ROWS"
            assert report["invalid_calendar_entries"] == 1


def test_metadata_identity_lookup_is_exact_bounded_and_causal(tmp_path: Path) -> None:
    path = tmp_path / "identity.sqlite"
    identity = sha256_json({"action": "exact"})
    with OpsRepository(path) as repository:
        for created_at_ns, available_at_ns, selected_identity in (
            (10, 10, identity),
            (20, 20, identity),
            (30, 30, identity),
            (15, 15, sha256_json({"action": "other"})),
        ):
            body = {"payoff": {"action_hash": selected_identity}}
            ref = sha256_json({"created": created_at_ns, "body": body})
            repository.register_artifact(ArtifactIndexEntryV2(
                ref, "PolicyPayoffV2", ref, created_at_ns, available_at_ns, body
            ))

    with OpsRepository(path, read_only=True) as reader:
        first = reader.artifact_entries_by_metadata_identity(
            "PolicyPayoffV2", ("payoff", "action_hash"), identity,
            as_of_ns=20, limit=1,
        )
        assert len(first.entries) == 1 and first.has_more
        assert first.entries[0].available_at_ns == 20
        assert first.next_cursor is not None
        second = reader.artifact_entries_by_metadata_identity(
            "PolicyPayoffV2", ("payoff", "action_hash"), identity,
            as_of_ns=20, after=first.next_cursor, limit=1,
        )
        assert len(second.entries) == 1 and not second.has_more
        assert second.entries[0].available_at_ns == 10
        assert not reader.artifact_entries_by_metadata_identity(
            "PolicyPayoffV2", ("payoff", "action_hash"), identity,
            as_of_ns=9, limit=2,
        ).entries
        plan = reader._connection.execute(
            "EXPLAIN QUERY PLAN SELECT * FROM artifact_index WHERE artifact_type='PolicyPayoffV2' "
            "AND CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.payoff.action_hash') END=? "
            "AND available_at_ns<=? ORDER BY created_at_ns DESC,artifact_ref DESC LIMIT ?",
            (identity, 20, 2),
        ).fetchall()
        assert any("artifact_policy_payoff_action_lookup" in row["detail"] for row in plan)


def test_repository_restores_additive_lookup_indexes_on_existing_schema(tmp_path: Path) -> None:
    path = tmp_path / "existing-v1-schema.sqlite"
    with OpsRepository(path) as repository:
        repository._connection.execute("DROP INDEX artifact_policy_payoff_action_lookup")

    with OpsRepository(path) as repository:
        indexes = {
            row["name"] for row in repository._connection.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            ).fetchall()
        }
        assert "artifact_policy_payoff_action_lookup" in indexes
