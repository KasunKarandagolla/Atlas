"""Deterministic Session-027 supervisor and existing-pipeline integration tests."""

from __future__ import annotations

import ast
import hashlib
from dataclasses import replace
from pathlib import Path

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.collector import PublicCollectorV2
from atlas.v2.data.history import ParquetObservationArchiveV2
from atlas.v2.instruments import InstrumentRegistryV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.risk import SizingStatus, size_selected_candidate
from atlas.v2.runtime.ops_supervisor import (
    PIPELINE_STAGE_ORDER,
    OpsCycleBatchV1,
    OpsDecisionEventV1,
    OpsDecisionResultV1,
    OpsRecoverySnapshotV1,
    OpsSourceStateV1,
    OpsStageResultV1,
    OpsStageStatusV1,
    OpsSupervisorV2,
    OpsTerminalStatusV1,
    PipelineStageV1,
)
from atlas.v2.strategies.s1_trend import S1_POLICY

from .test_session016_candidate_selection import CUTOFF
from .test_session017_risk import risk_case

HEALTHY = (OpsSourceStateV1("PUBLIC_MARKET", "HEALTHY_CURRENT", CUTOFF, CUTOFF),)


class FakeClock:
    def __init__(self, now_ns: int = CUTOFF) -> None:
        self.now_ns = now_ns

    def __call__(self) -> int:
        return self.now_ns


class DeterministicPort:
    def __init__(
        self,
        *,
        event: OpsDecisionEventV1 | None = None,
        required_sources: tuple[str, ...] = ("PUBLIC_MARKET",),
        source_states: tuple[OpsSourceStateV1, ...] = HEALTHY,
        reconciled: bool = True,
        fail_after_stage: PipelineStageV1 | None = None,
    ) -> None:
        self.event = event
        self.required_sources = required_sources
        self.source_states = source_states
        self.reconciled = reconciled
        self.fail_after_stage = fail_after_stage
        self.failed_once = False
        self.calls: list[str] = []
        self.recovery_calls = 0
        self.process_calls = 0

    @staticmethod
    def _index_trigger(repository: OpsRepository, event: OpsDecisionEventV1) -> None:
        body = {"event_id": event.event_id, "trigger": event.event_type, "cutoff_ns": event.information_cutoff_ns}
        repository.register_artifact(
            ArtifactIndexEntryV2(
                event.trigger_ref,
                "DecisionTriggerFixtureV1",
                event.trigger_ref,
                event.available_at_ns,
                event.available_at_ns,
                body,
            )
        )

    def recover(self, repository: OpsRepository, *, now_ns: int) -> OpsRecoverySnapshotV1:
        self.calls.append("recover")
        self.recovery_calls += 1
        return OpsRecoverySnapshotV1(
            self.required_sources,
            self.source_states,
            (),
            None,
            self.reconciled,
            now_ns,
        )

    def collect(self, repository: OpsRepository, *, now_ns: int, recovery: OpsRecoverySnapshotV1) -> OpsCycleBatchV1:
        self.calls.append("collect")
        events = (self.event,) if self.event is not None else ()
        if self.event is not None:
            self._index_trigger(repository, self.event)
        return OpsCycleBatchV1(
            events,
            self.source_states,
            self.required_sources,
            (),
            self.reconciled,
            now_ns,
        )

    def process_event(
        self,
        repository: OpsRepository,
        event: OpsDecisionEventV1,
        *,
        now_ns: int,
        source_health_state: str,
        completed_stages: dict[PipelineStageV1, OpsStageResultV1],
        checkpoint,
    ) -> OpsDecisionResultV1:
        self.calls.append("process")
        self.process_calls += 1
        results: dict[PipelineStageV1, OpsStageResultV1] = dict(completed_stages)
        for stage in PIPELINE_STAGE_ORDER:
            if stage in results:
                continue
            status = (
                OpsStageStatusV1.NOT_ESTIMABLE
                if stage == PipelineStageV1.DECISION_CALENDAR
                else OpsStageStatusV1.SKIPPED
            )
            item = OpsStageResultV1(stage, status, (), now_ns, "FIXTURE_NO_PIPELINE_OUTPUT")
            checkpoint(item)
            results[stage] = item
            if stage == self.fail_after_stage and not self.failed_once:
                self.failed_once = True
                raise RuntimeError("injected deterministic stage crash")
        return OpsDecisionResultV1(
            tuple(results[stage] for stage in PIPELINE_STAGE_ORDER),
            OpsTerminalStatusV1.NOT_ESTIMABLE,
            "FIXTURE_NO_PIPELINE_OUTPUT",
        )


def make_event(
    *,
    cutoff_ns: int = CUTOFF,
    event_id: str | None = None,
    trigger_ref: str | None = None,
    deadline_delta_ns: int = 10_000_000_000,
    causal_input_refs: tuple[str, ...] = (),
) -> OpsDecisionEventV1:
    identity = event_id or sha256_json({"session027_event": cutoff_ns})
    trigger = trigger_ref or sha256_json({"session027_trigger": identity})
    return OpsDecisionEventV1(
        identity,
        "BAR_CLOSE_15M",
        "PUBLIC_MARKET",
        trigger,
        cutoff_ns,
        None,
        cutoff_ns,
        cutoff_ns,
        cutoff_ns,
        cutoff_ns + deadline_delta_ns,
        causal_input_refs,
    )


def test_run_once_is_bounded_and_continuous_loop_uses_injected_sleep(tmp_path):
    clock = FakeClock()
    sleeps: list[float] = []
    port = DeterministicPort(event=None)

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock.now_ns += int(seconds * 1_000_000_000)

    with OpsSupervisorV2(tmp_path / "ops.sqlite", port, clock_ns=clock, sleep_fn=sleep) as supervisor:
        first = supervisor.run_once()
        continuous = tuple(supervisor.run_forever(max_cycles=2, interval_s=0.25))
    assert first.cycle.recovered and all(item.cycle.recovered for item in continuous)
    assert port.calls[:2] == ["recover", "collect"]
    assert port.recovery_calls == 1
    assert sleeps == [0.25]


def test_absent_required_feed_is_unknown_and_recovery_precedes_collection(tmp_path):
    event = make_event()
    port = DeterministicPort(event=event, source_states=(), reconciled=False)
    with OpsSupervisorV2(tmp_path / "ops.sqlite", port, clock_ns=FakeClock()) as supervisor:
        output = supervisor.run_once()
    receipt = output.event_receipts[0]
    assert port.calls[:2] == ["recover", "collect"]
    assert port.process_calls == 0
    assert receipt.source_health_state == "UNKNOWN"
    assert receipt.result.terminal_status == OpsTerminalStatusV1.NOT_ESTIMABLE
    assert not receipt.candidate_set_ref and not receipt.action_ref


class CollectorRecoveryPort(DeterministicPort):
    """Exercise existing collector restart/subscription/reconnect facilities."""

    def __init__(self, *, event: OpsDecisionEventV1) -> None:
        super().__init__(event=event, required_sources=("PUBLIC_MARKET",), source_states=(), reconciled=False)
        self.collector: PublicCollectorV2 | None = None
        self.registry = InstrumentRegistryV2()
        self.archive_root: Path | None = None
        self.collect_count = 0
        self.restored_watch_ids: tuple[str, ...] = ()
        self.subscription_plan_id: str | None = None

    def recover(self, repository: OpsRepository, *, now_ns: int) -> OpsRecoverySnapshotV1:
        self.calls.append("recover")
        self.recovery_calls += 1
        self.archive_root = Path(repository.path).parent / "archive"
        archive = ParquetObservationArchiveV2(self.archive_root)
        self.collector = PublicCollectorV2(
            repository=repository,
            registry=self.registry,
            clock_ns=lambda: now_ns,
            archive=archive,
        )
        restart = self.collector.restore_subscriptions({}, now_ns=now_ns)
        self.restored_watch_ids = tuple(watch.watch_id for watch in restart.watches.active_watches)
        self.subscription_plan_id = restart.subscriptions.plan_id
        self.collector.reconnected("PUBLIC_MARKET", at_ns=now_ns)
        return OpsRecoverySnapshotV1(
            ("PUBLIC_MARKET",),
            (OpsSourceStateV1("PUBLIC_MARKET", "INCOMPLETE_SNAPSHOT", now_ns, now_ns),),
            self.restored_watch_ids,
            restart.subscriptions.plan_id,
            False,
            now_ns,
        )

    def collect(self, repository: OpsRepository, *, now_ns: int, recovery: OpsRecoverySnapshotV1) -> OpsCycleBatchV1:
        assert self.collector is not None
        self.calls.append("collect")
        self.collect_count += 1
        events: tuple[OpsDecisionEventV1, ...] = ()
        if self.collect_count == 1:
            health = self.collector.health.latest("PUBLIC_MARKET")
            reconciled = False
        else:
            health = self.collector.reconcile_after_reconnect(
                "PUBLIC_MARKET", at_ns=now_ns, complete_snapshot=True, missed_interval_repaired=True
            )
            events = (self.event,) if self.event is not None else ()
            reconciled = True
        assert health is not None
        if self.event is not None and events:
            self._index_trigger(repository, self.event)
        source = OpsSourceStateV1("PUBLIC_MARKET", health.state.value, health.observed_at_ns, health.available_at_ns)
        return OpsCycleBatchV1(events, (source,), ("PUBLIC_MARKET",), (), reconciled, now_ns)


def test_restart_restores_subscriptions_and_health_stays_incomplete_until_reconciled(tmp_path):
    clock = FakeClock()
    port = CollectorRecoveryPort(event=make_event())
    path = tmp_path / "ops.sqlite"
    from .test_memory import watch

    with OpsRepository(path) as seed_repository:
        active_watch = replace(watch("session027-active", expires=CUTOFF + 1_000), required_next_event="BAR_CLOSE_15M")
        seed_repository.create_watch(active_watch)
    with OpsSupervisorV2(path, port, clock_ns=clock) as supervisor:
        first = supervisor.run_once()
        assert first.cycle.source_health_state == "INCOMPLETE_SNAPSHOT"
        assert not first.event_receipts
        clock.now_ns += 10
        second = supervisor.run_once()
        assert port.collector is not None
        assert port.collector.repository.get_watch("session027-active") is not None
    assert second.cycle.source_health_state == "HEALTHY_CURRENT"
    assert len(second.event_receipts) == 1
    assert port.collector is not None
    assert port.collector.repository.read_only is False
    assert port.restored_watch_ids == ("session027-active",)
    assert port.subscription_plan_id is not None


def test_same_immutable_event_is_idempotent_across_cycles_and_restart(tmp_path):
    event = make_event()
    path = tmp_path / "ops.sqlite"
    port = DeterministicPort(event=event)
    clock = FakeClock()
    with OpsSupervisorV2(path, port, clock_ns=clock) as supervisor:
        first = supervisor.run_once().event_receipts[0]
        clock.now_ns += 1
        replay = supervisor.run_once().event_receipts[0]
    assert port.process_calls == 1
    assert replay.content_hash == first.content_hash

    restarted_port = DeterministicPort(event=event)
    clock.now_ns += 1
    with OpsSupervisorV2(path, restarted_port, clock_ns=clock) as restarted:
        after_restart = restarted.run_once().event_receipts[0]
        assert restarted.repository is not None
        identities = restarted.repository.artifact_entries("OpsSupervisorReceiptIdentityV1")
        event_identities = restarted.repository.artifact_entries("OpsDecisionEventIdentityV1")
    assert restarted_port.process_calls == 0
    assert after_restart.content_hash == first.content_hash
    assert len(identities) == len(event_identities) == 1


@pytest.mark.parametrize(
    "failure_stage",
    (
        PipelineStageV1.UNIVERSE,
        PipelineStageV1.CANDIDATE_SET,
        PipelineStageV1.HARD_RISK,
        PipelineStageV1.ECONOMIC_EVALUATION,
    ),
)
def test_stage_crash_checkpoints_resume_without_reordering_or_backdating(tmp_path, failure_stage):
    event = make_event()
    clock = FakeClock()
    path = tmp_path / "ops.sqlite"
    port = DeterministicPort(event=event, fail_after_stage=failure_stage)
    with OpsSupervisorV2(path, port, clock_ns=clock) as supervisor:
        interrupted = supervisor.run_once()
        assert not interrupted.event_receipts
    clock.now_ns += 10
    restarted_port = DeterministicPort(event=event, fail_after_stage=failure_stage)
    with OpsSupervisorV2(path, restarted_port, clock_ns=clock) as supervisor:
        resumed = supervisor.run_once()
        assert len(resumed.event_receipts) == 1
        assert supervisor.repository is not None
        checkpoints = [
            supervisor.repository.get_artifact(supervisor._checkpoint_ref(event.event_id, stage))
            for stage in PIPELINE_STAGE_ORDER
        ]
    assert port.process_calls == restarted_port.process_calls == 1
    assert all(item is not None for item in checkpoints)
    assert resumed.event_receipts[0].event.information_cutoff_ns == event.information_cutoff_ns
    assert all(result.completed_at_ns <= clock.now_ns for result in resumed.event_receipts[0].result.stages)


def test_future_or_unavailable_evidence_cannot_enter_earlier_event(tmp_path):
    future_ref = sha256_json("future-artifact")
    event = make_event(causal_input_refs=(future_ref,))
    port = DeterministicPort(event=event)
    clock = FakeClock()
    with OpsSupervisorV2(tmp_path / "ops.sqlite", port, clock_ns=clock) as supervisor:
        output = supervisor.run_once()
        repo = supervisor.repository
        assert repo is not None
        assert not output.event_receipts[0].candidate_set_ref
        original_receipt = output.event_receipts[0]
        repo.register_artifact(
            ArtifactIndexEntryV2(future_ref, "FutureFixtureV1", future_ref, CUTOFF + 100, CUTOFF + 100, {})
        )
        clock.now_ns = CUTOFF + 1
        port.event = event
        replay = supervisor.run_once().event_receipts[0]
        assert replay.content_hash == original_receipt.content_hash
        assert port.process_calls == 0
        late = make_event(cutoff_ns=clock.now_ns, event_id=sha256_json("late-event"), causal_input_refs=(future_ref,))
        port.event = late
        output = supervisor.run_once()
    assert output.event_receipts[0].result.terminal_status == OpsTerminalStatusV1.NOT_ESTIMABLE
    assert output.event_receipts[0].result.missing_reason == "MISSING_OR_FUTURE_CAUSAL_EVENT_EVIDENCE"


def test_hard_risk_api_terminates_future_account_or_fee_as_not_estimable(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        future_fee = replace(case.fee, available_at_ns=CUTOFF + 1)
        decision = size_selected_candidate(
            repo,
            candidate_set=case.candidate_set,
            candidate=case.candidate,
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
            fee=future_fee,
            cutoff_ns=CUTOFF,
        )
        assert decision.status == SizingStatus.NOT_ESTIMABLE
        assert "FUTURE_REQUIRED_RISK_EVIDENCE" in decision.reasons
        assert repo.artifact_entries("ActionArtifactV2") == ()
        assert repo.artifact_entries("EvaluationArtifactV2") == ()


def test_non_action_sleeve_contracts_and_s8_basket_role_remain_explicit(tmp_path):
    from atlas.v2.science.research_selection import SLEEVE_AVAILABILITY, research_sleeve_audit
    from atlas.v2.strategies.s8_pairs import build_research_basket_forecast

    from .test_session023_discovery_s8 import basket_fixture

    audit = research_sleeve_audit(CUTOFF)
    roles = {name: (status, reason) for name, status, reason in audit.sleeves}
    for name in ("S4", "S5", "S6", "S7"):
        assert roles[name] == ("EXCLUDED", "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT")
    assert roles["S8"] == ("EXCLUDED", "RESEARCH_BASKET_ONLY_NO_SINGLE_ACTION_CONTRACT")
    pair, prices_a, prices_b, seed = basket_fixture()
    basket = build_research_basket_forecast(
        pair,
        prices_a=prices_a,
        prices_b=prices_b,
        cutoff_ns=seed.information_cutoff_ns,
        leg_a_evidence=seed.leg_a_evidence,
        leg_b_evidence=seed.leg_b_evidence,
    )
    assert not isinstance(basket, __import__("atlas.v2.contracts", fromlist=["CandidateActionV2"]).CandidateActionV2)
    assert {name for name, _, _ in SLEEVE_AVAILABILITY} == {"S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8"}


def test_missing_risk_evidence_blocks_action_and_economic_progression(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        future_account = replace(case.account, available_at_ns=CUTOFF + 1)
        sizing = size_selected_candidate(
            repo,
            candidate_set=case.candidate_set,
            candidate=case.candidate,
            universe=case.universe,
            policy=S1_POLICY,
            product=case.product,
            v1=case.v1,
            v2=case.v2,
            account=future_account,
            exposures=case.exposures,
            outcomes=case.outcomes,
            venue=case.venue,
            stress=case.stress,
            fee=case.fee,
            cutoff_ns=CUTOFF,
        )
        assert sizing.status == SizingStatus.NOT_ESTIMABLE
        assert "FUTURE_REQUIRED_RISK_EVIDENCE" in sizing.reasons
        assert repo.artifact_entries("ActionArtifactV2") == ()
        assert repo.artifact_entries("EvaluationArtifactV2") == ()


def test_projection_and_runtime_import_boundaries_remain_read_only_and_non_capital():
    import atlas.v2.runtime.ops_supervisor as runtime

    tree = ast.parse(Path(runtime.__file__).read_text())
    imported = {
        alias.name for node in ast.walk(tree) if isinstance(node, (ast.Import, ast.ImportFrom)) for alias in node.names
    }
    assert not any(name.startswith("atlas.runtime") for name in imported)
    assert not any("agent_intelligence" in name for name in imported)
    assert "atlas.v2.desktop" not in imported
    assert runtime.OPS_SUPERVISOR_VERSION == "ATLAS_OPS_SUPERVISOR_V2_V1"


def test_dependency_locks_and_frozen_identity_inputs_are_unchanged():
    assert hashlib.sha256(Path("requirements-lock.txt").read_bytes()).hexdigest() == (
        "711c2abda6c2152b3acf98ba151bf62bf7e13ab6a4259e5c99034d2fa3abba2b"
    )
    assert hashlib.sha256(Path("requirements-agent-lock.txt").read_bytes()).hexdigest() == (
        "47184aa3a8ba6045e527d47f274093c4821157ba208996659329872e7892f4e3"
    )
    assert Path("docs/v2/V1_GOLDEN_BASELINE.json").is_file()


def test_foreground_supervisor_owns_one_database_writer_and_receipts_are_content_addressed(tmp_path):
    event = make_event()
    port = DeterministicPort(event=event)
    path = tmp_path / "ops.sqlite"
    clock = FakeClock()
    supervisor = OpsSupervisorV2(path, port, clock_ns=clock)
    with supervisor:
        first = supervisor.run_once()
        clock.now_ns += 1
        second = supervisor.run_once()
        assert supervisor.repository is not None
        receipts = supervisor.repository.artifact_entries("OpsSupervisorReceiptV1")
        cycles = supervisor.repository.artifact_entries("OpsSupervisorCycleReceiptV1")
    assert port.recovery_calls == 1
    assert len(receipts) == 1
    assert len(cycles) == 2
    assert first.event_receipts[0].content_hash == second.event_receipts[0].content_hash
    with OpsRepository(path, read_only=True) as reader:
        assert reader.read_only


def test_no_live_control_or_order_boundary_is_reachable_from_runtime_module():
    source = Path("src/atlas/v2/runtime/ops_supervisor.py").read_text()
    forbidden = ("SafeRuntime", "OrderIntent", "Approval", "ProtectionPort", "LiveControl", "submit_order")
    assert not any(name in source for name in forbidden)
    assert "capital_enabled: bool = False" in source
    assert 'agent_mode: str = "DISABLED"' in source
