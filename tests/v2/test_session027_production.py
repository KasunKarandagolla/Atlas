"""Session-027 tests for the ATLAS-owned production composition."""

from __future__ import annotations

import ast
import builtins
import importlib
import socket
import urllib.request
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.collector import PublicCollectorV2
from atlas.v2.data.health import PublicSourceHealthV2, PublicSourceStateV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.risk import size_selected_candidate
from atlas.v2.runtime import production
from atlas.v2.runtime.ops_supervisor import (
    OpsCycleBatchV1,
    OpsDecisionEventV1,
    OpsRecoverySnapshotV1,
    OpsSourceStateV1,
    OpsSupervisorV2,
    OpsTerminalStatusV1,
    PipelineStageV1,
)
from atlas.v2.science.action import freeze_action
from atlas.v2.science.research_selection import assemble_multisleeve_research_candidate_set
from atlas.v2.strategies.s1_trend import S1_POLICY
from atlas.v2.strategies.s2_breakout import S2_POLICY
from atlas.v2.strategies.s3_mean_reversion import S3_POLICY

from .session023_support import research_case
from .test_session016_candidate_selection import CUTOFF, evidence
from .test_session020_phase2_e2e import (
    _admission_policy,
    _capability_for_action,
    _causal_input,
)
from .test_session027_ops_supervisor import FakeClock, make_event


class StaticInputsProvider:
    def __init__(self, event_id: str, inputs: production.ProductionEventInputsV1) -> None:
        self.event_id = event_id
        self.inputs = inputs
        self.calls = 0

    def resolve(self, repository: OpsRepository, event: OpsDecisionEventV1):
        del repository
        assert event.event_id == self.event_id
        self.calls += 1
        return self.inputs


class ReconciledFixturePublicSource:
    """Deterministic public-source seam that exercises the real collector object."""

    def __init__(self, event: OpsDecisionEventV1 | None, *, source_id: str = "PUBLIC_MARKET") -> None:
        self.event = event
        self.source_id = source_id
        self.calls: list[str] = []
        self.repositories: list[OpsRepository] = []
        self.collectors: list[PublicCollectorV2] = []
        self.recovery_snapshots: list[OpsRecoverySnapshotV1] = []
        self.trigger_refs: list[str] = []

    @property
    def required_source_ids(self) -> tuple[str, ...]:
        return (self.source_id,)

    def collect(self, repository, collector, *, now_ns, recovery):
        self.calls.append("collect")
        self.repositories.append(repository)
        self.collectors.append(collector)
        self.recovery_snapshots.append(recovery)
        assert collector.repository is repository
        health = collector.health.latest(self.source_id)
        if health is None:
            collector.reconnected(self.source_id, at_ns=now_ns - 2)
            health = collector.reconcile_after_reconnect(
                self.source_id,
                at_ns=now_ns - 1,
                complete_snapshot=True,
                missed_interval_repaired=True,
            )
        elif health.state != PublicSourceStateV2.HEALTHY_CURRENT and health.observed_at_ns < now_ns:
            health = collector.reconcile_after_reconnect(
                self.source_id,
                at_ns=now_ns,
                complete_snapshot=True,
                missed_interval_repaired=True,
            )
        healthy = health.state == PublicSourceStateV2.HEALTHY_CURRENT
        events = (self.event,) if self.event is not None and healthy else ()
        if self.event is not None and events:
            body = {
                "event_id": self.event.event_id,
                "trigger": self.event.event_type,
                "cutoff_ns": self.event.information_cutoff_ns,
            }
            repository.register_artifact(_trigger_entry(self.event, body))
            self.trigger_refs.append(self.event.trigger_ref)
        state = OpsSourceStateV1(
            self.source_id,
            health.state.value,
            health.observed_at_ns,
            health.available_at_ns,
        )
        return OpsCycleBatchV1(events, (state,), (self.source_id,), (), healthy, now_ns)


def _trigger_entry(event: OpsDecisionEventV1, body: dict[str, object]) -> ArtifactIndexEntryV2:
    return ArtifactIndexEntryV2(
        event.trigger_ref,
        "DecisionTriggerFixtureV1",
        event.trigger_ref,
        event.available_at_ns,
        event.available_at_ns,
        body,
    )


def _risk_inputs(case, *, account=None, fee=None, stress=None):
    return production.ProductionRiskInputsV1(
        case.product,
        case.v1,
        case.v2,
        case.account if account is None else account,
        case.exposures,
        case.outcomes,
        case.venue,
        case.stress if stress is None else stress,
        case.fee if fee is None else fee,
    )


def _make_fixture_inputs(
    repository: OpsRepository,
    event_id: str,
    *,
    missing_account: bool = False,
    future_account: bool = False,
    future_fee: bool = False,
    future_stress: bool = False,
    future_capability: bool = False,
    no_economic_inputs: bool = False,
):
    case = research_case(repository)
    scanner_refs = {
        item.candidate_id: (evidence(repository, item, case.universe, rank, event=event_id),)
        for rank, item in enumerate(case.competitors, start=1)
    }
    candidate_set = assemble_multisleeve_research_candidate_set(
        repository,
        universe=case.universe,
        decision_event_id=event_id,
        cutoff_ns=CUTOFF,
        candidates=case.competitors,
        policies={policy.policy_hash: policy for policy in (S1_POLICY, S2_POLICY, S3_POLICY)},
        scanner_evidence_refs=scanner_refs,
    )
    selected = next(item for item in case.competitors if item.candidate_id == candidate_set.selected_candidate_id)
    if selected.candidate_id != case.candidate.candidate_id:
        raise AssertionError("deterministic fixture must select the original exact S1 action")

    account = replace(case.account, available_at_ns=CUTOFF + 1) if future_account else case.account
    fee = replace(case.fee, available_at_ns=CUTOFF + 1) if future_fee else case.fee
    stress = replace(case.stress, available_at_ns=CUTOFF + 1) if future_stress else case.stress
    risk = _risk_inputs(case, account=account, fee=fee, stress=stress)
    if missing_account:
        risk = replace(risk, account=None)
    risk_by_candidate = {selected.candidate_id: risk}

    economic_by_candidate = {}
    expected_sizing_ref = None
    expected_action_ref = None
    expected_action_hash = None
    if account is not None and not missing_account and not any((future_fee, future_stress, future_account)):
        sizing = size_selected_candidate(
            repository,
            candidate_set=candidate_set,
            candidate=selected,
            universe=case.universe,
            policy=S1_POLICY,
            product=case.product,
            v1=case.v1,
            v2=case.v2,
            account=case.account,
            exposures=case.exposures,
            outcomes=case.outcomes,
            venue=case.venue,
            stress=case.stress,
            fee=case.fee,
            cutoff_ns=CUTOFF,
        )
        if sizing.status.value == "SIZED":
            expected_sizing_ref = sizing.content_hash
            action = freeze_action(
                repository,
                candidate=selected,
                candidate_set=candidate_set,
                sizing=sizing,
                product=case.product,
                policy=S1_POLICY,
                v1=case.v1,
                v2=case.v2,
            )
            expected_action_ref = action.content_hash
            expected_action_hash = action.action.action_hash
            if not no_economic_inputs:
                capability = _capability_for_action(action, case, CUTOFF)
                if future_capability:
                    capability = replace(capability, available_at_ns=CUTOFF + 1)
                economic_by_candidate[selected.candidate_id] = production.ProductionEconomicInputsV1(
                    _admission_policy(),
                    capability,
                    _causal_input(repository, "Session027ProductionM0InputV1", CUTOFF),
                    _causal_input(repository, "Session027ProductionCalibrationInputV1", CUTOFF),
                    _causal_input(repository, "Session027ProductionExecutionInputV1", CUTOFF),
                    CUTOFF + 10,
                    27027,
                    100,
                )

    feature_refs = tuple(sorted({item.snapshot_hash for item in case.competitors}))
    inputs = production.ProductionEventInputsV1(
        case.universe,
        case.competitors,
        scanner_refs,
        risk_by_candidate,
        economic_by_candidate,
        feature_refs,
    )
    required_refs = {
        case.universe.content_hash,
        *(item.content_hash for item in case.competitors),
        *(ref for refs in scanner_refs.values() for ref in refs),
        *feature_refs,
    }
    return (
        inputs,
        case,
        candidate_set,
        selected,
        tuple(sorted(required_refs)),
        expected_sizing_ref,
        expected_action_ref,
        expected_action_hash,
    )


def _production_event(repository, **options):
    seed_event = make_event(cutoff_ns=CUTOFF, deadline_delta_ns=10_000_000_000)
    inputs, case, candidate_set, selected, refs, sizing_ref, action_ref, action_hash = _make_fixture_inputs(
        repository, seed_event.event_id, **options
    )
    event = replace(seed_event, causal_input_refs=refs)
    return event, inputs, case, candidate_set, selected, sizing_ref, action_ref, action_hash


def _run_with_production_port(path, event, inputs, clock, *, port=None):
    source = ReconciledFixturePublicSource(event)
    adapter = port or production.ProductionOpsCyclePortV1(
        public_source=source,
        inputs_provider=StaticInputsProvider(event.event_id, inputs),
    )
    return adapter, source, OpsSupervisorV2(path, adapter, clock_ns=clock)


def test_builtin_atlas_ops_cli_imports_and_runs_without_external_adapter(tmp_path, capsys):
    assert production.OPS_PRODUCTION_ADAPTER_ID == "ATLAS_V2_PRODUCTION_OPS_COMPOSITION_V1"
    assert production.create_production_port().__class__ is production.ProductionOpsCyclePortV1
    from atlas.v2.runtime.ops_supervisor import main

    assert main(["--db", str(tmp_path / "ops.sqlite"), "--once"]) == 0
    output = capsys.readouterr().out
    assert '"recovered":true' in output


def test_actual_adapter_recovers_before_collecting_and_uses_existing_pipeline_apis(tmp_path, monkeypatch):
    path = tmp_path / "ops.sqlite"
    source = ReconciledFixturePublicSource(None)
    port = production.ProductionOpsCyclePortV1(public_source=source)
    clock = FakeClock(CUTOFF + 100)
    with OpsSupervisorV2(path, port, clock_ns=clock) as supervisor:
        result = supervisor.run_once()
        assert supervisor.repository is not None
        assert source.repositories == [supervisor.repository]
    assert port._recovery_calls == 1
    assert port._collection_calls == 1
    assert source.calls == ["collect"]
    assert source.collectors[0].repository is source.repositories[0]
    assert source.recovery_snapshots[0].required_subscription_ref is not None

    calls: list[str] = []
    with OpsRepository(tmp_path / "api-call.sqlite") as repo:
        event, inputs, case, expected_set, selected, _, _, _ = _production_event(repo)
        source = ReconciledFixturePublicSource(event)
        provider = StaticInputsProvider(event.event_id, inputs)
        port = production.ProductionOpsCyclePortV1(public_source=source, inputs_provider=provider)
        wrappers = (
            ("candidate_set", "assemble_multisleeve_research_candidate_set"),
            ("acceptance", "accept_research_candidates"),
            ("hard_risk", "size_selected_candidate"),
            ("action", "freeze_action"),
            ("evaluation", "run_phase2_economic_evaluation"),
            ("m1", "fit_m1"),
            ("analogue", "not_estimable_analogue"),
            ("calendar", "index_decision_calendar_entry"),
        )
        original = {name: getattr(production, name) for _, name in wrappers}
        from atlas.v2.science import admission

        calendar_index = admission.index_decision_calendar_entry
        with monkeypatch.context() as patcher:
            def calendar_wrapper(*args, **kwargs):
                calls.append("calendar")
                return calendar_index(*args, **kwargs)

            patcher.setattr(admission, "index_decision_calendar_entry", calendar_wrapper)
            for label, name in wrappers:
                function = original[name]

                def wrapper(*args, __label=label, __function=function, **kwargs):
                    calls.append(__label)
                    return __function(*args, **kwargs)

                patcher.setattr(production, name, wrapper)
            event_ref = event.trigger_ref
            repo.register_artifact(
                _trigger_entry(event, {"event_id": event.event_id, "trigger": event.event_type,
                                                         "cutoff_ns": event.information_cutoff_ns})
            )
            recovery = port.recover(repo, now_ns=CUTOFF + 100)
            batch = port.collect(repo, now_ns=CUTOFF + 100, recovery=recovery)
            assert batch.events == (event,)
            staged: dict[PipelineStageV1, Any] = {}

            def checkpoint(item):
                staged[item.stage] = item

            result = port.process_event(
                repo,
                event,
                now_ns=CUTOFF + 100,
                source_health_state="HEALTHY_CURRENT",
                completed_stages={},
                checkpoint=checkpoint,
            )
        assert result.terminal_status in (OpsTerminalStatusV1.NOT_ESTIMABLE, OpsTerminalStatusV1.NO_TRADE)
        assert result.stages[3].artifact_refs[0] == expected_set.content_hash
        assert selected.candidate_id == expected_set.selected_candidate_id
        assert event_ref in event.causal_input_refs
        assert set(calls) >= {label for label, _ in wrappers}
        assert provider.calls == 1
        assert repo.get_artifact(expected_set.content_hash) is not None
        assert staged and len(staged) == len(production.PIPELINE_STAGE_ORDER)
        assert all(item.authority == "ZERO" for item in result.stages)


def test_public_collector_restart_subscriptions_cursor_and_health_are_used_by_adapter(tmp_path):
    from .test_memory import watch

    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        active = replace(watch("session027-production-watch", expires=CUTOFF + 10_000),
                         required_next_event="BAR_CLOSE_15M")
        repo.create_watch(active)
        cursor_metadata = {
            "source_id": "PUBLIC_MARKET",
            "channel": "KLINE_15M",
            "high_water_sequence": 42,
            "checkpoint_at_ns": CUTOFF,
            "recent_payload_hashes": {"record-41": sha256_json("payload-41")},
        }
        cursor_ref = sha256_json({"artifact_type": "PublicCollectorCursorV2", "metadata": cursor_metadata})
        repo.register_artifact(ArtifactIndexEntryV2(
            cursor_ref, "PublicCollectorCursorV2", sha256_json(cursor_metadata), CUTOFF, CUTOFF, cursor_metadata
        ))
    event = make_event()
    source = ReconciledFixturePublicSource(event)
    port = production.ProductionOpsCyclePortV1(public_source=source)
    clock = FakeClock(CUTOFF + 100)
    with OpsSupervisorV2(path, port, clock_ns=clock) as supervisor:
        supervisor.run_once()
        assert port._collector_recovery is not None
        collector = port._collector_recovery.collector
        assert collector._last_sequence[("PUBLIC_MARKET", "KLINE_15M")] == 42
        assert port._collector_recovery.restored_subscription_plan.plan_id
        assert "session027-production-watch" in source.recovery_snapshots[0].restored_watch_ids
        assert collector.health.latest("PUBLIC_MARKET").state == PublicSourceStateV2.HEALTHY_CURRENT

    clock.now_ns += 10
    restarted_source = ReconciledFixturePublicSource(event)
    restarted_port = production.ProductionOpsCyclePortV1(public_source=restarted_source)
    with OpsSupervisorV2(path, restarted_port, clock_ns=clock) as restarted:
        restarted.run_once()
        assert restarted_port._collector_recovery is not None
        restored = restarted_port._collector_recovery.collector.health.latest("PUBLIC_MARKET")
        assert restored is not None
        assert restored.state == PublicSourceStateV2.INCOMPLETE_SNAPSHOT
        clock.now_ns += 10
        repaired = restarted.run_once()
        assert repaired.event_receipts
        assert restarted_port._collector_recovery.collector.health.latest("PUBLIC_MARKET").state == (
            PublicSourceStateV2.HEALTHY_CURRENT
        )


def test_builtin_event_handoff_waits_for_collector_reconnect_reconciliation(tmp_path):
    event = make_event()
    path = tmp_path / "indexed-source.sqlite"
    with OpsRepository(path) as repo:
        repo.register_artifact(_trigger_entry(event, {"event_id": event.event_id}))
        repo.register_artifact(ArtifactIndexEntryV2(
            sha256_json({"event_source": event.event_id}),
            "OpsDecisionEventSourceV1",
            sha256_json({"event_source": event.event_id}),
            event.available_at_ns,
            event.available_at_ns,
            {"event": event.to_dict()},
        ))
        repo.record_source_health(PublicSourceHealthV2(
            "PUBLIC_MARKET",
            CUTOFF,
            CUTOFF,
            PublicSourceStateV2.HEALTHY_CURRENT,
            sha256_json("initial-public-health"),
            "fixture public source was current before restart",
        ).to_ops_record())
    clock = FakeClock(CUTOFF + 100)
    port = production.create_production_port()
    with OpsSupervisorV2(path, port, clock_ns=clock) as supervisor:
        first = supervisor.run_once()
        assert not first.event_receipts
        assert port._collector_recovery is not None
        assert port._collector_recovery.collector.health.latest("PUBLIC_MARKET").state == (
            PublicSourceStateV2.INCOMPLETE_SNAPSHOT
        )
        clock.now_ns += 10
        port._collector_recovery.collector.reconcile_after_reconnect(
            "PUBLIC_MARKET",
            at_ns=clock.now_ns,
            complete_snapshot=True,
            missed_interval_repaired=True,
        )
        second = supervisor.run_once()
    assert len(second.event_receipts) == 1
    assert second.event_receipts[0].result.terminal_status == OpsTerminalStatusV1.NO_CANDIDATE


def test_successful_production_composition_preserves_ids_and_binds_zero_authority_diagnostics(tmp_path):
    path = tmp_path / "ops.sqlite"
    clock = FakeClock(CUTOFF + 100)
    with OpsRepository(path) as setup_repo:
        event, inputs, case, expected_set, selected, sizing_ref, action_ref, action_hash = _production_event(setup_repo)
        # The adapter will re-run these existing immutable APIs. Record artifact counts
        # after fixture construction to prove retries do not create duplicate identities.
        before = {
            kind: len(setup_repo.artifact_entries(kind))
            for kind in ("CandidateSetV2", "ActionArtifactV2", "EvaluationArtifactV2", "DecisionCalendarEntryV2")
        }
    port, source, supervisor = _run_with_production_port(path, event, inputs, clock)
    with supervisor:
        output = supervisor.run_once()
        receipt = output.event_receipts[0]
        assert supervisor.repository is not None
        repo = supervisor.repository
        after_first = {
            kind: len(repo.artifact_entries(kind))
            for kind in before
        }
        sizing = repo.get_artifact(receipt.sizing_ref or "")
        action = repo.get_artifact(receipt.action_ref or "")
        evaluation = repo.get_artifact(receipt.evaluation_ref or "")
        calendar = repo.get_artifact(receipt.calendar_refs[0])
        m1 = receipt.result.stages[8]
        analogue = receipt.result.stages[9]
        assert sizing is not None and sizing.artifact_type == "SizingDecisionV2"
        assert action is not None and action.artifact_type == "ActionArtifactV2"
        assert evaluation is not None and evaluation.artifact_type == "EvaluationArtifactV2"
        assert calendar is not None and calendar.artifact_type == "DecisionCalendarEntryV2"
        assert receipt.candidate_set_ref == expected_set.content_hash
        assert receipt.sizing_ref == sizing_ref
        assert receipt.action_ref == action_ref
        assert receipt.to_dict()["action_hash"] == action_hash == sha256_json(action.metadata["action_identity"])
        assert m1.bound_action_hash == analogue.bound_action_hash == receipt.to_dict()["action_hash"]
        assert m1.authority == analogue.authority == "ZERO"
        assert m1.status != "SKIPPED" and analogue.status != "SKIPPED"
        assert receipt.to_dict()["agent_mode"] == "DISABLED"
        assert not receipt.to_dict()["capital_enabled"] and not receipt.to_dict()["assisted_enabled"]
        assert receipt.result.terminal_status == OpsTerminalStatusV1.NOT_ESTIMABLE
        assert source.repositories[0] is repo
        assert port._recovery_calls == port._collection_calls == 1

    clock.now_ns += 10
    restarted_source = ReconciledFixturePublicSource(event)
    restarted_port = production.ProductionOpsCyclePortV1(
        public_source=restarted_source,
        inputs_provider=StaticInputsProvider(event.event_id, inputs),
    )
    with OpsSupervisorV2(path, restarted_port, clock_ns=clock) as restarted:
        restarted.run_once()
        clock.now_ns += 10
        replay_output = restarted.run_once()
        replay = replay_output.event_receipts[0]
        assert restarted.repository is not None
        after_restart = {
            kind: len(restarted.repository.artifact_entries(kind))
            for kind in before
        }
    assert replay.content_hash == receipt.content_hash
    assert after_first == after_restart


def test_missing_and_future_mandatory_evidence_terminates_without_fabricating_progress(tmp_path):
    cases = (
        {"missing_account": True},
        {"future_account": True},
        {"future_fee": True},
        {"future_stress": True},
    )
    for index, options in enumerate(cases):
        path = tmp_path / f"safe-{index}.sqlite"
        with OpsRepository(path) as setup_repo:
            event, inputs, _case, _candidate_set, _selected, _, _, _ = _production_event(setup_repo, **options)
            prior_refs = {
                kind: tuple(entry.artifact_ref for entry in setup_repo.artifact_entries(kind))
                for kind in ("ActionArtifactV2", "EvaluationArtifactV2")
            }
        clock = FakeClock(CUTOFF + 100)
        source = ReconciledFixturePublicSource(event)
        port = production.ProductionOpsCyclePortV1(
            public_source=source,
            inputs_provider=StaticInputsProvider(event.event_id, inputs),
        )
        with OpsSupervisorV2(path, port, clock_ns=clock) as supervisor:
            receipt = supervisor.run_once().event_receipts[0]
            repo = supervisor.repository
            assert repo is not None
            assert receipt.result.terminal_status == OpsTerminalStatusV1.NOT_ESTIMABLE
            assert not receipt.action_ref
            assert not receipt.evaluation_ref
            assert repo.artifact_entries("TradePlanEnvelopeV2") == ()
            assert repo.artifact_entries("OrderIntentV2") == ()
            assert repo.artifact_entries("Approval") == ()
            assert tuple(entry.artifact_ref for entry in repo.artifact_entries("ActionArtifactV2")) == prior_refs[
                "ActionArtifactV2"
            ]
            assert tuple(entry.artifact_ref for entry in repo.artifact_entries("EvaluationArtifactV2")) == prior_refs[
                "EvaluationArtifactV2"
            ]
            calendar = repo.get_artifact(receipt.calendar_refs[0])
            assert calendar is not None and calendar.artifact_type == "DecisionCalendarEntryV2"
            assert calendar.metadata["decision_entry"]["admission_state"] == "NOT_ESTIMABLE"


def test_missing_economic_evidence_keeps_only_existing_hard_risk_action(tmp_path):
    path = tmp_path / "missing-economic.sqlite"
    with OpsRepository(path) as setup_repo:
        event, inputs, _case, expected_set, _selected, sizing_ref, action_ref, action_hash = _production_event(
            setup_repo, no_economic_inputs=True
        )
    source = ReconciledFixturePublicSource(event)
    port = production.ProductionOpsCyclePortV1(
        public_source=source,
        inputs_provider=StaticInputsProvider(event.event_id, inputs),
    )
    with OpsSupervisorV2(path, port, clock_ns=FakeClock(CUTOFF + 100)) as supervisor:
        receipt = supervisor.run_once().event_receipts[0]
        assert supervisor.repository is not None
        assert receipt.candidate_set_ref == expected_set.content_hash
        assert receipt.sizing_ref == sizing_ref
        assert receipt.action_ref == action_ref
        assert receipt.to_dict()["action_hash"] == action_hash
        assert receipt.evaluation_ref is None
        assert receipt.result.terminal_status == OpsTerminalStatusV1.NOT_ESTIMABLE
        assert supervisor.repository.artifact_entries("EvaluationArtifactV2") == ()
        calendar = supervisor.repository.get_artifact(receipt.calendar_refs[0])
        assert calendar is not None
        assert calendar.metadata["decision_entry"]["admission_state"] == "NOT_ESTIMABLE"


def test_future_capability_keeps_hard_risk_action_but_blocks_evaluation(tmp_path):
    path = tmp_path / "future-capability.sqlite"
    with OpsRepository(path) as setup_repo:
        event, inputs, _case, expected_set, _selected, sizing_ref, action_ref, action_hash = _production_event(
            setup_repo, future_capability=True
        )
    source = ReconciledFixturePublicSource(event)
    port = production.ProductionOpsCyclePortV1(
        public_source=source,
        inputs_provider=StaticInputsProvider(event.event_id, inputs),
    )
    with OpsSupervisorV2(path, port, clock_ns=FakeClock(CUTOFF + 100)) as supervisor:
        receipt = supervisor.run_once().event_receipts[0]
        assert supervisor.repository is not None
        assert receipt.candidate_set_ref == expected_set.content_hash
        assert receipt.sizing_ref == sizing_ref
        assert receipt.action_ref == action_ref
        assert receipt.to_dict()["action_hash"] == action_hash
        assert receipt.evaluation_ref is None
        assert receipt.result.terminal_status == OpsTerminalStatusV1.NOT_ESTIMABLE
        assert supervisor.repository.artifact_entries("EvaluationArtifactV2") == ()


def test_injected_production_crashes_resume_existing_artifacts_in_order(tmp_path):
    for stage in (
        PipelineStageV1.UNIVERSE,
        PipelineStageV1.CANDIDATE_SET,
        PipelineStageV1.HARD_RISK,
        PipelineStageV1.ECONOMIC_EVALUATION,
    ):
        path = tmp_path / f"crash-{stage.value}.sqlite"
        with OpsRepository(path) as setup_repo:
            event, inputs, _case, _candidate_set, _selected, _, _, _ = _production_event(setup_repo)
        clock = FakeClock(CUTOFF + 100)
        failed = False

        def crash(checkpoint_stage, *, expected_stage=stage):
            nonlocal failed
            if checkpoint_stage == expected_stage and not failed:
                failed = True
                raise RuntimeError("injected production stage crash")

        source = ReconciledFixturePublicSource(event)
        port = production.ProductionOpsCyclePortV1(
            public_source=source,
            inputs_provider=StaticInputsProvider(event.event_id, inputs),
            crash_after_checkpoint=crash,
        )
        with OpsSupervisorV2(path, port, clock_ns=clock) as supervisor:
            first = supervisor.run_once()
            assert failed and not first.event_receipts
            before = {
                kind: tuple(item.artifact_ref for item in supervisor.repository.artifact_entries(kind))
                for kind in ("CandidateSetV2", "SizingDecisionV2", "ActionArtifactV2", "EvaluationArtifactV2",
                             "DecisionCalendarEntryV2")
            }
        clock.now_ns += 10
        source_after_restart = ReconciledFixturePublicSource(event)
        restarted_port = production.ProductionOpsCyclePortV1(
            public_source=source_after_restart,
            inputs_provider=StaticInputsProvider(event.event_id, inputs),
        )
        with OpsSupervisorV2(path, restarted_port, clock_ns=clock) as restarted:
            waiting = restarted.run_once()
            assert not waiting.event_receipts
            clock.now_ns += 10
            resumed = restarted.run_once()
            assert len(resumed.event_receipts) == 1
            receipt = resumed.event_receipts[0]
            after = {
                kind: tuple(item.artifact_ref for item in restarted.repository.artifact_entries(kind))
                for kind in before
            }
            checkpoints = [
                restarted.repository.get_artifact(restarted._checkpoint_ref(event.event_id, item))
                for item in production.PIPELINE_STAGE_ORDER
            ]
        for kind, prior_refs in before.items():
            assert set(prior_refs).issubset(after[kind])
            assert len(after[kind]) == len(set(after[kind]))
        assert len(after["SizingDecisionV2"]) == 1
        assert len(after["ActionArtifactV2"]) == 1
        assert len(after["EvaluationArtifactV2"]) == 1
        assert len(after["DecisionCalendarEntryV2"]) == 1
        assert all(item is not None for item in checkpoints)
        assert tuple(result.stage for result in receipt.result.stages) == production.PIPELINE_STAGE_ORDER
        assert all(result.completed_at_ns >= event.available_at_ns for result in receipt.result.stages)
        assert all(result.completed_at_ns <= clock.now_ns for result in receipt.result.stages)
        assert restarted_port._collection_calls == 2


def test_agent_dependencies_and_external_provider_calls_are_not_required_or_invoked(monkeypatch):
    production_path = Path(production.__file__)
    tree = ast.parse(production_path.read_text())
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    assert not any("agent_intelligence" in name for name in imported)
    assert not any(name.startswith(("httpx", "urllib", "websockets")) for name in imported)

    original_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        if name.startswith("atlas.v2.agent_intelligence"):
            raise AssertionError("agent packages must remain optional for ops runtime")
        return original_import(name, *args, **kwargs)

    network_calls: list[str] = []
    monkeypatch.setattr(builtins, "__import__", guarded_import)
    monkeypatch.setattr(urllib.request, "urlopen", lambda *a, **k: network_calls.append("urlopen"))
    monkeypatch.setattr(socket.socket, "connect", lambda *a, **k: network_calls.append("connect"))
    reloaded = importlib.reload(production)
    reloaded.create_production_port()
    assert network_calls == []


def test_production_composition_has_no_capital_or_live_control_boundary():
    source = Path(production.__file__).read_text()
    tree = ast.parse(source)
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    forbidden_imports = (
        "atlas.runtime",
        "atlas.v1",
        "atlas.desktop",
        "atlas.v2.agent_intelligence",
    )
    assert not any(name.startswith(forbidden_imports) for name in imported)
    forbidden_symbols = (
        "SafeRuntime",
        "OrderIntent",
        "TradePlanEnvelope",
        "ApprovalPort",
        "ProtectionPort",
        "submit_order",
        "enable_capital",
        "assisted_execution",
    )
    assert not any(symbol in source for symbol in forbidden_symbols)
    assert 'OPS_PRODUCTION_ADAPTER_ID = "ATLAS_V2_PRODUCTION_OPS_COMPOSITION_V1"' in source
    assert "agent_mode" not in source or '"DISABLED"' not in source


def test_production_inputs_reject_non_exact_or_sized_sleeve_actions(tmp_path):
    from atlas.v2.contracts import CandidateActionV2

    with OpsRepository(tmp_path / "contract.sqlite") as repo:
        case = research_case(repo)
        with pytest.raises(ValueError, match="S1-S3"):
            production.ProductionEventInputsV1(
                case.universe,
                (replace(case.candidate, policy_hash=sha256_json("S4_CONTEXT"),
                         envelope=replace(case.candidate.envelope, content_hash="")),),
                {},
                {},
                {},
            )
        assert isinstance(case.candidate, CandidateActionV2)
        with pytest.raises(ValueError, match="unsized"):
            production.ProductionEventInputsV1(
                case.universe,
                (replace(case.candidate, quantity=Decimal("1"),
                         envelope=replace(case.candidate.envelope, content_hash="")),),
                {},
                {},
                {},
            )
