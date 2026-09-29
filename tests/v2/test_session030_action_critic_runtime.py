"""Session-030 offline tests for nonblocking, writer-owned hidden critic runtime."""

from __future__ import annotations

import ast
import shutil
import sqlite3
import threading
import uuid
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.agent_intelligence.budget import DeepSeekPriceScheduleV1
from atlas.v2.agent_intelligence.contracts import ProviderResultV1
from atlas.v2.agent_intelligence.controller import (
    ActionAssessmentController,
    ActionAssessmentRunOutcomeV1,
    DirectActionAssessmentBrokerPort,
)
from atlas.v2.agent_intelligence.persistence import ActionAssessmentRepository
from atlas.v2.agent_intelligence.shadow_measurement import (
    ActionCriticShadowObservationV1,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.action_critic_dispatcher import (
    ActionAssessmentDispatchCompletionV1,
    ActionAssessmentShadowDispatcher,
)
from atlas.v2.runtime.action_critic_shadow import (
    ActionAssessmentShadowCoordinator,
    build_sealed_action_assessment,
)
from atlas.v2.runtime.ops_supervisor import OpsCycleBatchV1, OpsRecoverySnapshotV1, OpsSupervisorV2
from atlas.v2.science.action_critic_outcomes import link_action_critic_matured_outcome
from atlas.v2.science.outcomes import LabelStateV2, index_matured_outcome
from tests.v2.test_session018_remediation import _payoff_case
from tests.v2.test_session027_ops_supervisor import FakeClock
from tests.v2.test_session029_action_critic import (
    CAPABILITY_KEY,
    _valid_output,
)
from tests.v2.test_session029_action_critic import (
    frozen_case as session029_frozen_case,  # noqa: F401
)


@pytest.fixture
def frozen_case(request: pytest.FixtureRequest) -> Any:
    return request.getfixturevalue("session029_frozen_case")


def _ledger(path: Path, schedule: DeepSeekPriceScheduleV1) -> ActionAssessmentRepository:
    with OpsRepository(path):
        pass
    return ActionAssessmentRepository(path, price_schedule=schedule)


class _ReplayEventPort:
    """Return an already-receipted event to exercise supervisor replay callbacks."""

    def __init__(self, event: Any) -> None:
        self.event = event

    def recover(self, _repository: OpsRepository, *, now_ns: int) -> OpsRecoverySnapshotV1:
        return OpsRecoverySnapshotV1((), (), (), None, True, now_ns)

    def collect(self, _repository: OpsRepository, *, now_ns: int,
                recovery: OpsRecoverySnapshotV1) -> OpsCycleBatchV1:
        return OpsCycleBatchV1((self.event,), (), (), (), True, now_ns)


class _UnusedProvider:
    def __init__(self) -> None:
        self.calls = 0

    def assess(self, **_kwargs: Any) -> ProviderResultV1:
        self.calls += 1
        raise AssertionError("provider effects belong only to the I/O worker")


class _BlockingIO:
    def __init__(self, packet_output: str, *, release: threading.Event,
                 entered: threading.Event, finished: threading.Event) -> None:
        self.packet_output = packet_output
        self.release = release
        self.entered = entered
        self.finished = finished
        self.calls = 0
        self.active = 0
        self.max_active = 0
        self.worker_ids: set[int] = set()
        self.received_work: list[Any] = []
        self._lock = threading.Lock()

    def execute(self, work: Any) -> ProviderResultV1:
        with self._lock:
            self.calls += 1
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            self.worker_ids.add(threading.get_ident())
            self.received_work.append(work)
            current = self.calls
        try:
            if current == 1:
                self.entered.set()
                if not self.release.wait(timeout=10):
                    return ProviderResultV1("", None, None, False, False, 0, 0, None, "PROVIDER_TIMEOUT")
                return ProviderResultV1(self.packet_output, "deepseek-flash", None,
                    False, False, 100, 20, "local-fake")
            return ProviderResultV1("", None, None, False, False, 0, 0, None, "PROVIDER_TIMEOUT")
        finally:
            with self._lock:
                self.active -= 1
            if current == 1:
                self.finished.set()


def test_prepare_is_durable_before_work_emission_and_profile_identity_is_frozen(frozen_case, tmp_path: Path):
    path, _receipt, _receipt_ref, sealed, profile, schedule = frozen_case
    ledger = _ledger(tmp_path / "prepare.sqlite", schedule)
    provider = _UnusedProvider()
    controller = ActionAssessmentController(ledger=ledger, profile=profile,
        capability_signing_key=CAPABILITY_KEY, provider=DirectActionAssessmentBrokerPort(provider),
        now_ns=lambda: sealed.packet.sealed_cutoff_t_ns)
    work = controller.prepare(sealed.request, sealed.packet)
    try:
        assert work.identity.request_id == sealed.request.request_id
        assert work.identity.request_hash == sealed.request.content_hash
        assert work.identity.packet_ref == sealed.packet.packet_ref
        assert work.identity.packet_hash == sealed.packet.content_hash
        assert work.identity.action_hash == sealed.packet.action_hash
        assert work.identity.profile_hash == profile.content_hash == sealed.request.profile_hash
        assert work.authorization.profile_hash == profile.content_hash
        assert work.identity.deadline_ns == sealed.request.deadline_ns == sealed.packet.original_deadline_d_ns
        assert provider.calls == 0
        state = ledger.request_state(sealed.request.request_id)
        assert state is not None and state["attempt_id"] == work.identity.attempt_id
        assert state["authorization_id"] == work.identity.authorization_id
        assert state["authorization_hash"] == work.identity.authorization_hash
        assert ledger.has_dispatch(sealed.request.request_id)
        with sqlite3.connect(ledger.path) as connection:
            assert connection.execute("SELECT COUNT(*) FROM agent_action_assessment_packets").fetchone()[0] == 1
            assert connection.execute("SELECT COUNT(*) FROM agent_action_assessment_requests").fetchone()[0] == 1
            assert connection.execute("SELECT COUNT(*) FROM agent_action_assessment_attempts").fetchone()[0] == 1
            assert connection.execute("SELECT COUNT(*) FROM agent_action_assessment_reservations").fetchone()[0] == 1
            assert connection.execute("SELECT COUNT(*) FROM agent_action_assessment_dispatches").fetchone()[0] == 1
            assert connection.execute("SELECT COUNT(*) FROM agent_authorities").fetchone()[0] == 0
    finally:
        ledger.close()


def test_s29_packet_request_and_critic_profile_hashes_are_unchanged(frozen_case):
    path, receipt, receipt_ref, sealed, profile, _schedule = frozen_case
    with OpsRepository(path) as repository:
        replay = build_sealed_action_assessment(repository=repository, receipt=receipt,
            receipt_ref=receipt_ref, profile=profile)
    assert replay.packet.to_dict() == sealed.packet.to_dict()
    assert replay.packet.packet_ref == sealed.packet.packet_ref
    assert replay.packet.content_hash == sealed.packet.content_hash
    assert replay.request.to_dict() == sealed.request.to_dict()
    assert replay.request.request_id == sealed.request.request_id
    assert replay.request.content_hash == sealed.request.content_hash
    assert replay.request.profile_hash == profile.content_hash
    assert replay.request.provider_binding_hash == profile.provider_binding_hash


def test_full_dispatch_capacity_skips_without_authorization_or_provider_effect(frozen_case, tmp_path: Path):
    source_path, receipt, receipt_ref, sealed, profile, schedule = frozen_case

    class FullDispatcher:
        def reserve_capacity(self) -> None:
            return None

        def release_capacity(self, _token: str) -> None:
            raise AssertionError("capacity was unavailable before prepare")

        def submit_reserved(self, _token: str, _work: Any) -> bool:
            raise AssertionError("capacity was unavailable before prepare")

        def close(self) -> None:
            return None

    db = tmp_path / "capacity-skip.sqlite"
    shutil.copy2(source_path, db)
    ledger = _ledger(db, schedule)
    provider = _UnusedProvider()
    controller = ActionAssessmentController(ledger=ledger, profile=profile,
        capability_signing_key=CAPABILITY_KEY, provider=DirectActionAssessmentBrokerPort(provider),
        now_ns=lambda: receipt.created_at_ns)
    coordinator = ActionAssessmentShadowCoordinator(profile=profile, controller=controller,
        ledger=ledger, dispatcher=FullDispatcher(), now_ns=lambda: receipt.created_at_ns)  # type: ignore[arg-type]
    with OpsRepository(db) as repository:
        coordinator(receipt, receipt_ref, repository)
    state = ledger.request_state(sealed.request.request_id)
    assert state is not None and state["status"] == "SKIPPED"
    assert state["failure_code"] == "DISPATCHER_CAPACITY_UNAVAILABLE"
    assert state["attempt_id"] is None and state["authorization_id"] is None
    assert provider.calls == 0
    with sqlite3.connect(ledger.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM agent_action_assessment_attempts").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM agent_action_assessment_dispatches").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM agent_action_assessment_acceptances").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM agent_authorities").fetchone()[0] == 0
    coordinator.close()


def test_blocked_worker_cannot_block_run_once_and_result_is_main_writer_finalized(
        frozen_case, tmp_path: Path):
    path, original_receipt, receipt_ref, sealed, profile, schedule = frozen_case
    clock = FakeClock(original_receipt.created_at_ns)
    release = threading.Event()
    entered = threading.Event()
    finished = threading.Event()
    first_cycle_done = threading.Event()
    second_cycle_gate = threading.Event()
    second_cycle_done = threading.Event()
    third_cycle_gate = threading.Event()
    third_cycle_done = threading.Event()
    loop_stopped = threading.Event()
    io = _BlockingIO(_valid_output(sealed.packet), release=release, entered=entered, finished=finished)
    outputs: dict[str, Any] = {}
    auth_committed = threading.Event()

    def run_controller_thread() -> None:
        ledger = _ledger(path, schedule)
        trace_ids: list[int] = []
        ledger._connection.set_trace_callback(
            lambda sql: trace_ids.append(threading.get_ident())
            if sql.lstrip().upper().startswith(("BEGIN", "INSERT", "UPDATE", "DELETE", "REPLACE", "DROP", "CREATE"))
            else None)
        persist_authorization = ledger.persist_dispatch_authorization

        def persist_and_signal(authorization: Any, *, now_ns: int) -> None:
            persist_authorization(authorization, now_ns=now_ns)
            auth_committed.set()

        ledger.persist_dispatch_authorization = persist_and_signal  # type: ignore[method-assign]
        fake_provider = _UnusedProvider()
        controller = ActionAssessmentController(ledger=ledger, profile=profile,
            capability_signing_key=CAPABILITY_KEY, provider=DirectActionAssessmentBrokerPort(fake_provider),
            now_ns=clock)
        dispatcher = ActionAssessmentShadowDispatcher(io, now_ns=clock)
        coordinator = ActionAssessmentShadowCoordinator(profile=profile, controller=controller,
            ledger=ledger, dispatcher=dispatcher, now_ns=clock)
        port = _ReplayEventPort(original_receipt.event)
        supervisor = OpsSupervisorV2(path, port, clock_ns=clock, post_receipt_shadow=coordinator)
        try:
            repository = supervisor._ensure_open()
            coordinator(original_receipt, receipt_ref, repository)
            if not entered.wait(timeout=3):
                raise AssertionError("fake I/O worker did not enter its blocked state")
            first = supervisor.run_once()
            outputs["first"] = first
            first_cycle_done.set()
            if not second_cycle_gate.wait(timeout=8):
                return
            second = supervisor.run_once()
            outputs["second"] = second
            second_cycle_done.set()
            if not third_cycle_gate.wait(timeout=8):
                return
            third = supervisor.run_once()
            outputs["third"] = third
            outputs["terminal"] = ledger.request_state(sealed.request.request_id)
            work = io.received_work[0]
            duplicate_completion = ActionAssessmentDispatchCompletionV1(work.identity,
                ProviderResultV1(_valid_output(sealed.packet), "deepseek-flash", None,
                    False, False, 100, 20, "local-fake"),
                work.identity.authorized_at_ns, work.identity.authorized_at_ns)
            outputs["duplicate"] = controller.finalize(work, duplicate_completion)
            with sqlite3.connect(ledger.path) as connection:
                outputs["acceptance_count"] = connection.execute(
                    "SELECT COUNT(*) FROM agent_action_assessment_acceptances").fetchone()[0]
            outputs["observation_rows"] = controller.pending_observation_records(limit=2)
            repository = supervisor.repository
            assert repository is not None
            if outputs["observation_rows"]:
                from atlas.v2.agent_intelligence.shadow_measurement import build_action_critic_shadow_observation
                try:
                    observation = build_action_critic_shadow_observation(repository,
                        outputs["observation_rows"][0], recorded_at_ns=clock())
                    from atlas.v2.agent_intelligence.shadow_measurement import index_action_critic_shadow_observation
                    index_action_critic_shadow_observation(repository, observation)
                except Exception as error:
                    outputs["observation_error"] = f"{type(error).__name__}: {error}"
            outputs["observations"] = repository.artifact_entries_by_types(
                ("ActionCriticShadowObservationV1",), limit=10)
            outputs["io_calls"] = io.calls
            outputs["worker_id"] = dispatcher.worker_thread_id
            outputs["writer_id"] = threading.get_ident()
            outputs["provider_calls"] = fake_provider.calls
            outputs["trace_ids"] = tuple(trace_ids)
            outputs["auth_committed"] = auth_committed.is_set()
            outputs["work_types"] = tuple(type(item).__name__ for item in io.received_work)
            third_cycle_done.set()
        finally:
            supervisor.close()
            coordinator.close()
            loop_stopped.set()

    # The controller and all SQLite writers stay on this thread; only fake provider I/O
    # crosses to the dispatcher worker.
    worker = threading.Thread(target=run_controller_thread, name="s30-controller-test")
    worker.start()
    try:
        assert first_cycle_done.wait(timeout=8), "blocked critic delayed the first deterministic run_once"
        assert entered.wait(timeout=3), "fake I/O worker did not enter the blocked provider state"
        assert auth_committed.is_set(), "provider effect began before durable dispatch authorization"
        assert io.worker_ids and len(io.worker_ids) == 1
        assert io.received_work and all(not hasattr(item, name) for item in io.received_work
            for name in ("ledger", "repository", "connection", "_connection"))
        second_cycle_gate.set()
        assert second_cycle_done.wait(timeout=8), "blocked critic prevented the next deterministic cycle"
        assert not release.is_set(), "worker was released before the second cycle completed"
        first = outputs["first"]
        second = outputs["second"]
        assert tuple(item.content_hash for item in first.event_receipts) == (original_receipt.content_hash,)
        assert tuple(item.content_hash for item in second.event_receipts) == (original_receipt.content_hash,)
        assert io.calls == 1 and io.max_active == 1
        release.set()
        assert finished.wait(timeout=3)
        third_cycle_gate.set()
        assert third_cycle_done.wait(timeout=8), "later nonblocking drain failed to finalize the completion"
        assert outputs["terminal"] is not None
        assert outputs["terminal"]["status"] == "COMPLETE"
        assert outputs["terminal"]["eligible"] == 1
        assert outputs["duplicate"].status == "COMPLETE"
        assert outputs["acceptance_count"] == 1
        assert outputs["provider_calls"] == 0
        assert outputs["worker_id"] not in outputs["trace_ids"]
        assert outputs["worker_id"] != outputs["writer_id"]
        assert set(outputs["trace_ids"]) == {outputs["writer_id"]}
        assert outputs["auth_committed"]
        assert outputs["work_types"] == ("ActionAssessmentDispatchWorkV1",)
        assert outputs["observation_rows"] == (), outputs.get("observation_error")
        assert len(outputs["observations"]) == 1
        observation = outputs["observations"][0].metadata["observation"]
        assert observation["originating_receipt_ref"] == receipt_ref
        assert observation["decision_calendar_ref"] in original_receipt.calendar_refs
        assert observation["packet_ref"] == sealed.packet.packet_ref
        assert observation["packet_hash"] == sealed.packet.content_hash
        assert observation["request_ref"] == sealed.request.content_hash
        assert observation["request_hash"] == sealed.request.content_hash
        assert observation["action_hash"] == sealed.packet.action_hash
        assert observation["provider_profile_hash"] == profile.content_hash
        assert observation["model_profile_hash"] == profile.provider_binding_hash
        assert observation["critic_terminal_status"] == "COMPLETE"
        assert observation["accepted_shadow_evidence"] is True
        assert observation["decision_influence"] is False
        assert observation["admission_influence"] is False
        assert io.max_active == 1
        assert tuple(item.content_hash for item in outputs["third"].event_receipts) == (original_receipt.content_hash,)
    finally:
        release.set()
        second_cycle_gate.set()
        third_cycle_gate.set()
        worker.join(timeout=10)
        if worker.is_alive():
            pytest.fail("controller test thread failed to exit after releasing the fake provider")
    assert loop_stopped.is_set()


def test_bounded_dispatcher_has_one_worker_one_pending_and_nonblocking_close(frozen_case, tmp_path: Path):
    _path, _receipt, _receipt_ref, sealed, profile, schedule = frozen_case
    ledger = _ledger(tmp_path / "dispatcher.sqlite", schedule)
    clock = FakeClock(sealed.packet.sealed_cutoff_t_ns)
    provider = _UnusedProvider()
    controller = ActionAssessmentController(ledger=ledger, profile=profile,
        capability_signing_key=CAPABILITY_KEY, provider=DirectActionAssessmentBrokerPort(provider), now_ns=clock)
    work = controller.prepare(sealed.request, sealed.packet)
    assert not isinstance(work, ActionAssessmentRunOutcomeV1)
    release, entered, finished = threading.Event(), threading.Event(), threading.Event()
    io = _BlockingIO(_valid_output(sealed.packet), release=release, entered=entered, finished=finished)
    dispatcher = ActionAssessmentShadowDispatcher(io, now_ns=clock)
    try:
        first = dispatcher.reserve_capacity()
        assert first is not None and dispatcher.submit_reserved(first, work)
        assert entered.wait(timeout=3)
        second = dispatcher.reserve_capacity()
        assert second is not None
        # The pending bound is exactly one. The worker can run only one item at a time.
        assert dispatcher.submit_reserved(second, work)
        assert dispatcher.active_count == dispatcher.MAX_ACTIVE == 2
        assert dispatcher.pending_count == dispatcher.MAX_PENDING == 1
        assert dispatcher.reserve_capacity() is None
        dispatcher.close()
        assert not release.is_set() and not finished.is_set()
        assert dispatcher.active_count <= dispatcher.MAX_ACTIVE
    finally:
        release.set()
        dispatcher.close()
        ledger.close()
    assert finished.wait(timeout=3)
    assert io.max_active == 1
    assert io.calls == 1, "closed pending work must not create a second provider effect"


def test_restart_seals_authorized_work_lost_without_redispatch_and_late_duplicate_is_idempotent(
        frozen_case, tmp_path: Path):
    _path, _receipt, _receipt_ref, sealed, profile, schedule = frozen_case
    db = tmp_path / "restart-s30.sqlite"
    ledger = _ledger(db, schedule)
    provider = _UnusedProvider()
    clock = FakeClock(sealed.packet.sealed_cutoff_t_ns)
    controller = ActionAssessmentController(ledger=ledger, profile=profile,
        capability_signing_key=CAPABILITY_KEY, provider=DirectActionAssessmentBrokerPort(provider), now_ns=clock)
    work = controller.prepare(sealed.request, sealed.packet)
    assert not isinstance(work, ActionAssessmentRunOutcomeV1)
    assert sealed.request.deadline_ns == work.identity.deadline_ns
    ledger.close()

    restarted = ActionAssessmentRepository(db, price_schedule=schedule)
    controller2 = ActionAssessmentController(ledger=restarted, profile=profile,
        capability_signing_key=CAPABILITY_KEY, provider=DirectActionAssessmentBrokerPort(provider), now_ns=clock)
    assert controller2.recover_open_dispatches() == 1
    lost = controller2.prepare(sealed.request, sealed.packet)
    assert isinstance(lost, ActionAssessmentRunOutcomeV1)
    assert lost.status == "UNAVAILABLE" and lost.reason_code == "DISPATCH_OUTCOME_LOST_ON_RESTART"
    assert provider.calls == 0
    completion = ActionAssessmentDispatchCompletionV1(work.identity,
        ProviderResultV1(_valid_output(sealed.packet), "deepseek-flash", None, False, False, 20, 10),
        work.identity.authorized_at_ns, work.identity.authorized_at_ns)
    late = controller2.finalize(work, completion)
    duplicate = controller2.finalize(work, completion)
    assert late == duplicate == lost
    assert not late.accepted_shadow_evidence
    state = restarted.request_state(sealed.request.request_id)
    assert state is not None and state["status"] == "UNAVAILABLE"
    assert state["failure_code"] == "DISPATCH_OUTCOME_LOST_ON_RESTART"
    assert restarted.has_dispatch(sealed.request.request_id)
    restarted.close()


def test_restart_before_authorization_can_attempt_the_same_immutable_request(frozen_case, tmp_path: Path):
    _path, _receipt, _receipt_ref, sealed, profile, schedule = frozen_case
    db = tmp_path / "restart-before-auth-s30.sqlite"
    ledger = _ledger(db, schedule)
    ledger.persist_packet_request(sealed.packet, sealed.request, now_ns=sealed.packet.sealed_cutoff_t_ns)
    ledger.close()

    restarted = ActionAssessmentRepository(db, price_schedule=schedule)
    controller = ActionAssessmentController(ledger=restarted, profile=profile,
        capability_signing_key=CAPABILITY_KEY, provider=DirectActionAssessmentBrokerPort(_UnusedProvider()),
        now_ns=lambda: sealed.packet.sealed_cutoff_t_ns)
    work = controller.prepare(sealed.request, sealed.packet)
    assert not isinstance(work, ActionAssessmentRunOutcomeV1)
    assert work.identity.request_id == sealed.request.request_id
    assert work.identity.request_hash == sealed.request.content_hash
    assert work.identity.packet_ref == sealed.packet.packet_ref
    assert work.identity.packet_hash == sealed.packet.content_hash
    assert work.identity.deadline_ns == sealed.request.deadline_ns
    assert restarted.has_dispatch(sealed.request.request_id)
    restarted.close()


def test_action_critic_ledger_v1_migrates_additively_to_observation_projection_v2(
        frozen_case, tmp_path: Path):
    _path, _receipt, _receipt_ref, sealed, _profile, schedule = frozen_case
    db = tmp_path / "ledger-migration-s30.sqlite"
    ledger = _ledger(db, schedule)
    ledger.persist_packet_request(sealed.packet, sealed.request, now_ns=sealed.packet.sealed_cutoff_t_ns)
    ledger.close()
    with sqlite3.connect(db) as connection:
        connection.execute("DROP TABLE agent_action_assessment_observation_projections")
        connection.execute("DROP TABLE agent_action_assessment_meta")
        connection.execute("CREATE TABLE agent_action_assessment_meta(namespace TEXT PRIMARY KEY, "
                           "schema_version INTEGER NOT NULL CHECK(schema_version=1))")
        connection.execute("INSERT INTO agent_action_assessment_meta VALUES(?,1)",
                           ("atlas-agent-action-assessment",))

    migrated = ActionAssessmentRepository(db, price_schedule=schedule)
    try:
        with sqlite3.connect(db) as connection:
            version = connection.execute("SELECT schema_version FROM agent_action_assessment_meta "
                "WHERE namespace='atlas-agent-action-assessment'").fetchone()[0]
            tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            packet_count = connection.execute("SELECT COUNT(*) FROM agent_action_assessment_packets").fetchone()[0]
            request_count = connection.execute("SELECT COUNT(*) FROM agent_action_assessment_requests").fetchone()[0]
        assert version == 2
        assert "agent_action_assessment_observation_projections" in tables
        assert packet_count == request_count == 1
        state = migrated.request_state(sealed.request.request_id)
        assert state is not None and state["request_hash"] == sealed.request.content_hash
    finally:
        migrated.close()


@pytest.mark.parametrize(("result", "expected_status"), [
    (ProviderResultV1("", None, None, False, False, 0, 0, None, "PROVIDER_TIMEOUT"), "UNAVAILABLE"),
    (ProviderResultV1("", None, None, True, False, 0, 0, None), "REFUSED"),
    (ProviderResultV1("{bad", "deepseek-flash", None, False, False, 1, 1, None), "INVALID"),
    (ProviderResultV1("", "different-model", None, False, False, 1, 1, None), "INVALID"),
])
def test_async_terminal_variants_do_not_change_deterministic_receipt(frozen_case, tmp_path: Path,
        result: ProviderResultV1, expected_status: str):
    _path, receipt, _receipt_ref, sealed, profile, schedule = frozen_case
    ledger = _ledger(tmp_path / f"terminal-{expected_status}-{uuid.uuid4()}.sqlite", schedule)
    clock = FakeClock(sealed.packet.sealed_cutoff_t_ns)
    controller = ActionAssessmentController(ledger=ledger, profile=profile,
        capability_signing_key=CAPABILITY_KEY, provider=DirectActionAssessmentBrokerPort(_UnusedProvider()),
        now_ns=clock)
    work = controller.prepare(sealed.request, sealed.packet)
    assert not isinstance(work, ActionAssessmentRunOutcomeV1)
    completion = ActionAssessmentDispatchCompletionV1(work.identity, result,
        work.identity.authorized_at_ns, work.identity.authorized_at_ns)
    terminal = controller.finalize(work, completion)
    duplicate = controller.finalize(work, completion)
    assert terminal.status == expected_status and not terminal.accepted_shadow_evidence
    assert duplicate == terminal
    assert receipt.content_hash == frozen_case[1].content_hash
    ledger.close()


def test_shadow_observation_and_maturity_link_require_exact_existing_matured_identity(tmp_path: Path):
    db = tmp_path / "measurement.sqlite"
    with OpsRepository(db) as repository:
        case, action, _payoff, outcome = _payoff_case(repository)
        outcome_ref = index_matured_outcome(repository, outcome)
        observation = ActionCriticShadowObservationV1(
            originating_receipt_ref=sha256_json({"receipt": "deterministic"}),
            decision_calendar_ref=outcome.decision_ref,
            candidate_set_ref=outcome.candidate_set_ref,
            packet_ref=sha256_json({"packet": "fixed"}),
            packet_hash=sha256_json({"packet_body": "fixed"}),
            request_id=str(uuid.uuid4()),
            request_ref=sha256_json({"request": "fixed"}),
            request_hash=sha256_json({"request_body": "fixed"}),
            action_artifact_ref=action.content_hash,
            action_hash=action.action.action_hash,
            critic_terminal_status="COMPLETE",
            terminal_reason_code=None,
            accepted_shadow_evidence=True,
            finding_types=("ARTIFACT_SEMANTIC_MISMATCH",),
            dispatch_authorized_at_ns=outcome.decision_at_ns,
            result_received_at_ns=outcome.decision_at_ns + 1,
            dispatch_to_result_latency_ns=1,
            provider_profile_hash=sha256_json({"provider_profile": "fixed"}),
            model_profile_hash=sha256_json({"model_profile": "fixed"}),
            decision_influence=False,
            admission_influence=False,
            deterministic_terminal_status="NOT_ESTIMABLE",
            deterministic_admission_status=outcome.admission_state.value,
            recorded_at_ns=outcome.decision_at_ns + 2,
        )
        observation_ref = observation.content_hash
        repository.register_artifact(ArtifactIndexEntryV2(observation_ref, observation.VERSION,
            observation_ref, observation.recorded_at_ns, observation.recorded_at_ns,
            {"observation": observation.to_dict()}))
        link = link_action_critic_matured_outcome(repository, observation_ref, outcome_ref,
            as_of_ns=outcome.available_at_ns)
        assert link.observation_ref == observation_ref
        assert link.decision_calendar_ref == outcome.decision_ref
        assert link.candidate_set_ref == case.candidate_set.content_hash
        assert link.action_hash == action.action.action_hash
        assert link.matured_outcome_ref == outcome_ref
        with pytest.raises(ValueError, match="future matured outcome"):
            link_action_critic_matured_outcome(repository, observation_ref, outcome_ref,
                as_of_ns=outcome.available_at_ns - 1)
        mismatched = replace(outcome, candidate_set_ref=sha256_json({"candidate_set": "different"}))
        mismatched_ref = mismatched.content_hash
        repository.register_artifact(ArtifactIndexEntryV2(mismatched_ref, "MaturedOutcomeV2", mismatched_ref,
            mismatched.available_at_ns, mismatched.available_at_ns, {"outcome": mismatched.to_dict()}))
        with pytest.raises(ValueError, match="decision/candidate/action"):
            link_action_critic_matured_outcome(repository, observation_ref, mismatched_ref,
                as_of_ns=outcome.available_at_ns)
        mismatched_decision = replace(outcome, decision_ref=sha256_json({"decision": "different"}))
        mismatched_decision_ref = mismatched_decision.content_hash
        repository.register_artifact(ArtifactIndexEntryV2(mismatched_decision_ref, "MaturedOutcomeV2",
            mismatched_decision_ref, mismatched_decision.available_at_ns, mismatched_decision.available_at_ns,
            {"outcome": mismatched_decision.to_dict()}))
        with pytest.raises(ValueError, match="decision/candidate/action"):
            link_action_critic_matured_outcome(repository, observation_ref, mismatched_decision_ref,
                as_of_ns=outcome.available_at_ns)

        censored = replace(outcome, label_state=LabelStateV2.CENSORED,
            gross_payoff=None, fees=None, funding_cashflow=None, net_payoff=None,
            fill_quantity=None, reason="OUTCOME_EVIDENCE_CENSORED")
        censored_ref = censored.content_hash
        repository.register_artifact(ArtifactIndexEntryV2(censored_ref, "MaturedOutcomeV2", censored_ref,
            censored.available_at_ns, censored.available_at_ns, {"outcome": censored.to_dict()}))
        with pytest.raises(ValueError, match="cannot be linked as matured"):
            link_action_critic_matured_outcome(repository, observation_ref, censored_ref,
                as_of_ns=censored.available_at_ns)

        ambiguous = replace(outcome, matured_at_ns=outcome.matured_at_ns + 1,
            available_at_ns=outcome.available_at_ns + 1)
        ambiguous_ref = ambiguous.content_hash
        repository.register_artifact(ArtifactIndexEntryV2(ambiguous_ref, "MaturedOutcomeV2", ambiguous_ref,
            ambiguous.available_at_ns, ambiguous.available_at_ns, {"outcome": ambiguous.to_dict()}))
        with pytest.raises(ValueError, match="multiple or mismatched"):
            link_action_critic_matured_outcome(repository, observation_ref, outcome_ref,
                as_of_ns=ambiguous.available_at_ns)


def test_runtime_worker_source_has_no_database_or_decision_authority_imports():
    from atlas.v2.agent_intelligence import shadow_measurement
    from atlas.v2.runtime import action_critic_dispatcher
    from atlas.v2.science import action_critic_outcomes

    source = "\n".join((Path(action_critic_dispatcher.__file__).read_text(),
                       Path(action_critic_outcomes.__file__).read_text(),
                       Path(shadow_measurement.__file__).read_text()))
    tree = ast.parse(source)
    imports = {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import)
               for alias in node.names}
    imports.update(node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom))
    assert not any(any(token in module for token in ("risk", "admission", "trade_plan", "execution"))
                   for module in imports)
    assert "final_holdout" not in source.lower() and "holdout" not in source.lower()


def test_maturity_link_rejects_candidate_and_action_mismatch(tmp_path: Path):
    db = tmp_path / "maturity-mismatch.sqlite"
    with OpsRepository(db) as repository:
        _case, action, _payoff, outcome = _payoff_case(repository)
        outcome_ref = index_matured_outcome(repository, outcome)
        observation = ActionCriticShadowObservationV1(
            sha256_json({"receipt": 1}), outcome.decision_ref, outcome.candidate_set_ref,
            sha256_json({"packet": 1}), sha256_json({"packet_hash": 1}), str(uuid.uuid4()),
            sha256_json({"request": 1}), sha256_json({"request_hash": 1}), action.content_hash,
            sha256_json({"different-action": 2}), "COMPLETE", None, True, (),
            outcome.decision_at_ns, outcome.decision_at_ns, 0,
            sha256_json({"provider": 1}), sha256_json({"model": 1}), False, False,
            "NOT_ESTIMABLE", outcome.admission_state.value, outcome.available_at_ns)
        ref = observation.content_hash
        repository.register_artifact(ArtifactIndexEntryV2(ref, observation.VERSION, ref,
            observation.recorded_at_ns, observation.recorded_at_ns, {"observation": observation.to_dict()}))
        with pytest.raises(ValueError, match="identity does not match"):
            link_action_critic_matured_outcome(repository, ref, outcome_ref, as_of_ns=outcome.available_at_ns)
