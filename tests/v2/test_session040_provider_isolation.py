"""S40 fake-provider failures stay outside the durable public/sole-writer path."""

from __future__ import annotations

import json
import os
import shutil
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from atlas.v2 import product
from atlas.v2._serialization import sha256_json
from atlas.v2.agent_intelligence.contracts import ProviderResultV1
from atlas.v2.agent_intelligence.controller import (
    ActionAssessmentController,
    ActionAssessmentRunOutcomeV1,
    DirectActionAssessmentBrokerPort,
)
from atlas.v2.agent_intelligence.persistence import ActionAssessmentRepository
from atlas.v2.data.durable_public_capture import DurablePublicCaptureV1
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.action_critic_dispatcher import ActionAssessmentShadowDispatcher
from atlas.v2.runtime.production import create_bybit_public_ws_port
from atlas.v2.runtime.read_only_report_worker import ReadOnlyReportWorkerV1
from atlas.v2.science.tuning_export import TuningRunIdentityV1, export_tuning_snapshot
from tests.v2.test_s38_sustained_public_stream import FixturePublicSource, MixedWorkload, QueueStream, transport_rows
from tests.v2.test_session029_action_critic import CAPABILITY_KEY, _valid_output
from tests.v2.test_session029_action_critic import frozen_case as session029_frozen_case  # noqa: F401


@pytest.fixture
def frozen_case(request: pytest.FixtureRequest) -> Any:
    return request.getfixturevalue("session029_frozen_case")


def _wait(predicate: Any, *, seconds: float = 8) -> None:
    deadline = time.monotonic() + seconds
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError("fake-provider isolation condition did not complete")
        time.sleep(.01)


class _ControllerProviderMustNotRun:
    def assess(self, **_kwargs: Any) -> ProviderResultV1:
        raise AssertionError("provider calls must remain on the non-DB dispatcher")


class _FakeProviderIO:
    def __init__(self, result: Any, *, error: Exception | None = None,
                 release: threading.Event | None = None) -> None:
        self.result = result
        self.error = error
        self.release = release
        self.entered = threading.Event()
        self.calls = 0
        self.thread_ids: set[int] = set()

    def execute(self, work: Any) -> Any:
        self.calls += 1
        self.thread_ids.add(threading.get_ident())
        assert all(not hasattr(work, name) for name in ("repository", "ledger", "connection", "_connection"))
        assert work.request.action_hash == work.packet.action_hash == work.identity.action_hash
        self.entered.set()
        if self.release is not None and not self.release.wait(30):
            raise TimeoutError
        if self.error is not None:
            raise self.error
        return self.result


def _fresh_controller(path: Path, sealed: Any, profile: Any, schedule: Any, *, now: Any = None):
    ledger = ActionAssessmentRepository(path, price_schedule=schedule)
    clock = now or (lambda: sealed.packet.sealed_cutoff_t_ns)
    controller = ActionAssessmentController(ledger=ledger, profile=profile,
        capability_signing_key=CAPABILITY_KEY,
        provider=DirectActionAssessmentBrokerPort(_ControllerProviderMustNotRun()), now_ns=clock)
    return ledger, controller, clock


@pytest.mark.parametrize(("case", "expected_status", "expected_reason"), [
    ("normal", "COMPLETE", None),
    ("slow", "COMPLETE", None),
    ("timeout", "UNAVAILABLE", "PROVIDER_TIMEOUT"),
    ("unavailable", "UNAVAILABLE", "PROVIDER_UNAVAILABLE"),
    ("rate-limit", "UNAVAILABLE", "RATE_LIMITED"),
    ("malformed", "INVALID", "SCHEMA_ROOT_NOT_OBJECT"),
    ("oversized", "INVALID", "OUTPUT_SIZE_LIMIT"),
    ("unexpected-type", "INVALID", "BROKER_PROTOCOL_ERROR"),
    ("broker-crash", "UNAVAILABLE", "BROKER_UNAVAILABLE"),
    ("unknown-failure", "INVALID", "BROKER_PROTOCOL_ERROR"),
    ("altered-model-id", "INVALID", "RETURNED_MODEL_ID_DRIFT"),
    ("token-budget", "INVALID", "TOKEN_LIMIT_EXCEEDED"),
    ("late", "EXPIRED", "LATE_OUTPUT_INELIGIBLE"),
])
def test_provider_terminal_matrix_preserves_exact_action_and_sole_writer(
        frozen_case, tmp_path: Path, case: str, expected_status: str, expected_reason: str | None):
    source_path, receipt, receipt_ref, sealed, profile, schedule = frozen_case
    path = tmp_path / "ops.sqlite"
    shutil.copy2(source_path, path)
    now = [sealed.packet.sealed_cutoff_t_ns]
    ledger, controller, clock = _fresh_controller(path, sealed, profile, schedule, now=lambda: now[0])
    release = threading.Event() if case in {"slow", "late"} else None
    result: Any = ProviderResultV1(_valid_output(sealed.packet), "deepseek-flash", None,
                                 False, False, 100, 20, "s40-local-fake")
    error: Exception | None = None
    failures = {"unavailable": "PROVIDER_UNAVAILABLE", "rate-limit": "RATE_LIMITED"}
    if case in failures:
        result = ProviderResultV1("", None, None, False, False, 0, 0, None, failures[case])
    elif case == "timeout":
        error = TimeoutError()
    elif case == "broker-crash":
        error = ConnectionResetError()
    elif case == "malformed":
        result = ProviderResultV1("[]", "deepseek-flash", None, False, False, 1, 1)
    elif case == "oversized":
        result = ProviderResultV1("x" * 12_001, "deepseek-flash", None, False, False, 1, 1)
    elif case == "unexpected-type":
        result = {"untrusted": "not a provider result"}
    elif case == "token-budget":
        result = ProviderResultV1(_valid_output(sealed.packet), "deepseek-flash", None,
                                 False, False, 12_001, 20)
    elif case == "unknown-failure":
        result = ProviderResultV1(_valid_output(sealed.packet), "deepseek-flash", None,
                                 False, False, 100, 20, None, "UNRECOGNIZED_PROVIDER_FAILURE")
    elif case == "altered-model-id":
        result = ProviderResultV1(_valid_output(sealed.packet), "deepseek-\x00flash", None,
                                 False, False, 100, 20)
    io = _FakeProviderIO(result, error=error, release=release)
    dispatcher = ActionAssessmentShadowDispatcher(io, now_ns=clock)
    owner_id = threading.get_ident()
    write_ids: list[int] = []
    ledger._connection.set_trace_callback(lambda sql: write_ids.append(threading.get_ident())
        if sql.lstrip().upper().startswith(("BEGIN", "INSERT", "UPDATE", "DELETE", "REPLACE")) else None)
    try:
        with OpsRepository(path) as repository:
            repository._connection.set_trace_callback(lambda sql: write_ids.append(threading.get_ident())
                if sql.lstrip().upper().startswith(("BEGIN", "INSERT", "UPDATE", "DELETE", "REPLACE")) else None)
            action_before = repository.get_artifact(receipt.action_ref)
            receipt_before = repository.get_artifact(receipt_ref)
            assert action_before is not None and receipt_before is not None
            work = controller.prepare(sealed.request, sealed.packet)
            assert not isinstance(work, ActionAssessmentRunOutcomeV1)
            token = dispatcher.reserve_capacity()
            assert token is not None and dispatcher.submit_reserved(token, work)
            assert io.entered.wait(3)
            # Work and heartbeat publication remain possible before slow inference returns.
            heartbeat = {"observed_at_ns": clock(), "authority": "ZERO"}
            ref = sha256_json(heartbeat)
            repository.register_artifact(ArtifactIndexEntryV2(ref, "S40IsolationHeartbeatV1", ref,
                clock(), clock(), {"heartbeat": heartbeat}))
            assert repository.get_artifact(ref) is not None
            if release is not None:
                assert not dispatcher.drain_completed(max_items=1)
                if case == "late":
                    now[0] = sealed.request.deadline_ns
                release.set()
            _wait(lambda: dispatcher._completion_queue.qsize() == 1)
            completion = dispatcher.drain_completed(max_items=1)[0]
            terminal = controller.finalize(work, completion)
            assert terminal.status == expected_status
            if expected_reason is not None:
                assert terminal.reason_code == expected_reason
            assert terminal.accepted_shadow_evidence == (expected_status == "COMPLETE")
            # Repeated delivery/preparation cannot retry or alter the frozen action.
            assert controller.finalize(work, completion) == terminal
            assert controller.prepare(sealed.request, sealed.packet) == terminal
            assert io.calls == 1 and dispatcher.active_count == 0
            assert repository.get_artifact(receipt.action_ref) == action_before
            assert repository.get_artifact(receipt_ref) == receipt_before
            identity = action_before.metadata["action_identity"]
            assert identity["stop_price"] == sealed.packet.summaries[receipt.action_ref]["summary"]["action_identity"]["stop_price"]
            assert receipt.capital_enabled is receipt.assisted_enabled is False
            assert ledger._connection.execute("SELECT COUNT(*) FROM agent_authorities").fetchone()[0] == 0
            assert set(write_ids) == {owner_id}
            assert io.thread_ids == {dispatcher.worker_thread_id} and owner_id not in io.thread_ids
    finally:
        if release is not None:
            release.set()
        dispatcher.close()
        dispatcher._thread.join(timeout=3)
        ledger.close()


@pytest.mark.parametrize("stream_seconds", [1, pytest.param(35, marks=pytest.mark.skipif(
    os.environ.get("ATLAS_S40_PROVIDER_SOAK") != "1",
    reason="Explicit native actual-wall provider timeout/stream isolation gate"))])
def test_blocked_critic_public_capture_writer_heartbeats_and_read_only_export_progress(
        frozen_case, tmp_path: Path, stream_seconds: int):
    source_path, receipt, receipt_ref, sealed, profile, schedule = frozen_case
    path = tmp_path / "ops.sqlite"
    shutil.copy2(source_path, path)
    ledger, controller, clock = _fresh_controller(path, sealed, profile, schedule)
    release = threading.Event()
    io = _FakeProviderIO(ProviderResultV1(_valid_output(sealed.packet), "deepseek-flash", None,
                                        False, False, 100, 20), release=release)
    dispatcher = ActionAssessmentShadowDispatcher(io, now_ns=clock)
    source = QueueStream(time.time_ns)
    capture = DurablePublicCaptureV1(source)
    port = create_bybit_public_ws_port(public_source=FixturePublicSource(), public_stream_source=capture)
    expected: list[Any] = []
    errors: list[str] = []
    report = ReadOnlyReportWorkerV1(lambda: export_tuning_snapshot(path, tmp_path / "reports",
        TuningRunIdentityV1("s40-provider-isolation", "a" * 64, "b" * 40, 0),
        cutoff_ns=time.time_ns(), max_rows=100))
    producer: threading.Thread | None = None
    try:
        with OpsRepository(path) as repository:
            before = repository.get_artifact(receipt.action_ref)
            # S29's replay source emits an event but does not index it. Complete
            # the generated fixture's source binding required by the current
            # exporter; this does not repair or suppress an invalid real row.
            if repository.get_artifact(receipt.event.content_hash) is None:
                event = receipt.event
                repository.register_artifact(ArtifactIndexEntryV2(event.content_hash,
                    "OpsDecisionEventSourceV1", event.content_hash,
                    event.information_cutoff_ns, event.information_cutoff_ns,
                    {"event": event.to_dict(), "fixture": "S40_PROVIDER_ISOLATION"}))
            port.recover(repository, now_ns=time.time_ns())
            work = controller.prepare(sealed.request, sealed.packet)
            assert not isinstance(work, ActionAssessmentRunOutcomeV1)
            token = dispatcher.reserve_capacity()
            assert token is not None and dispatcher.submit_reserved(token, work)
            assert io.entered.wait(3)

            def produce():
                try:
                    workload = MixedWorkload()
                    started = time.monotonic()
                    for ordinal in range(160 * stream_seconds):
                        time.sleep(max(0, started + ordinal / 160 - time.monotonic()))
                        frame = workload.frame(time.time_ns())
                        expected.append(frame)
                        assert source.handoff.offer(frame)
                except Exception as exc:
                    errors.append(type(exc).__name__)

            producer = threading.Thread(target=produce, name="s40-provider-public-producer")
            live_started = time.monotonic()
            producer.start()
            assert report.start()
            heartbeat_refs = []
            while producer.is_alive() or capture.status().pending_frames:
                port._collect_public_stream_evidence(repository, now_ns=time.time_ns())
                body = {"observed_at_ns": time.time_ns(), "authority": "ZERO"}
                ref = sha256_json(body)
                repository.register_artifact(ArtifactIndexEntryV2(ref, "S40IsolationHeartbeatV1", ref,
                    body["observed_at_ns"], body["observed_at_ns"], {"heartbeat": body}))
                heartbeat_refs.append(ref)
                time.sleep(.01)
            producer.join(timeout=3)
            assert not errors, (errors, capture.status())
            _wait(lambda: capture.status().capture["captured_frames"] == 160 * stream_seconds)
            while capture.status().pending_frames:
                port._collect_public_stream_evidence(repository, now_ns=time.time_ns())
            _wait(lambda: report._completion is not None, seconds=12)
            completed = report.poll()
            assert completed is not None and completed.error_type is None
            assert completed.result["validation_failures"] == {}
            assert completed.completed_at_ns - completed.started_at_ns < 10_000_000_000
            assert not release.is_set()
            if stream_seconds == 1:
                assert dispatcher.active_count == 1
            else:
                # This fixture really blocks for the fake provider's existing
                # 30-second timeout, then proves another five seconds of core
                # arrivals remain serviceable. The provider request clock is
                # deliberately frozen; no deadline or real provider behavior
                # is reconfigured to obtain this isolation result.
                assert time.monotonic() - live_started >= stream_seconds - .1
                assert io.calls == 1 and dispatcher.active_count == 1
                assert dispatcher._completion_queue.qsize() == 1
            assert not errors and heartbeat_refs and all(repository.get_artifact(ref) for ref in heartbeat_refs)
            status = capture.status()
            assert status.handoff.frames_rejected == 0 and not status.handoff.overflowed
            assert status.handoff.high_water_items < status.handoff.max_queue_items
            rows = transport_rows(repository, tmp_path)
            assert [row["raw_payload_bytes"] for row in rows] == [frame.raw_payload_bytes for frame in expected]
            assert repository.get_artifact(receipt.action_ref) == before
            release.set()
            _wait(lambda: dispatcher._completion_queue.qsize() == 1)
            outcome = controller.finalize(work, dispatcher.drain_completed(max_items=1)[0])
            assert outcome.status == ("COMPLETE" if stream_seconds == 1 else "UNAVAILABLE")
            if stream_seconds != 1:
                assert outcome.reason_code == "PROVIDER_TIMEOUT"
            assert dispatcher.active_count == 0
            port.finish_public_capture(repository)
    finally:
        release.set()
        if producer is not None:
            producer.join(timeout=3)
        report.close(timeout_s=1)
        port.close()
        dispatcher.close()
        dispatcher._thread.join(timeout=3)
        ledger.close()


def test_cost_reservation_failure_prevents_provider_dispatch(frozen_case, tmp_path: Path, monkeypatch):
    _source, _receipt, _receipt_ref, sealed, profile, schedule = frozen_case
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path):
        pass
    ledger, controller, _clock = _fresh_controller(path, sealed, profile, schedule)
    try:
        def fail_reservation(*_args, **_kwargs):
            raise ValueError("offline fixture budget exhausted")

        monkeypatch.setattr(ledger, "reserve_cost", fail_reservation)
        outcome = controller.prepare(sealed.request, sealed.packet)
        assert isinstance(outcome, ActionAssessmentRunOutcomeV1)
        assert outcome.status == "UNAVAILABLE" and outcome.reason_code == "COST_RESERVATION_UNAVAILABLE"
        assert not ledger.has_dispatch(sealed.request.request_id)
        assert controller.prepare(sealed.request, sealed.packet) == outcome
    finally:
        ledger.close()


def test_broker_restart_seals_abandoned_authorization_and_late_result_cannot_revive_it(
        frozen_case, tmp_path: Path):
    _source, _receipt, _receipt_ref, sealed, profile, schedule = frozen_case
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path):
        pass
    ledger, controller, clock = _fresh_controller(path, sealed, profile, schedule)
    work = controller.prepare(sealed.request, sealed.packet)
    assert not isinstance(work, ActionAssessmentRunOutcomeV1)
    ledger.close()
    reopened, recovered, _clock = _fresh_controller(path, sealed, profile, schedule)
    from atlas.v2.runtime.action_critic_dispatcher import ActionAssessmentDispatchCompletionV1

    try:
        assert recovered.recover_open_dispatches() == 1
        outcome = recovered.prepare(sealed.request, sealed.packet)
        assert isinstance(outcome, ActionAssessmentRunOutcomeV1)
        assert outcome.status == "UNAVAILABLE" and outcome.reason_code == "DISPATCH_OUTCOME_LOST_ON_RESTART"
        late = ActionAssessmentDispatchCompletionV1(work.identity,
            ProviderResultV1(_valid_output(sealed.packet), "deepseek-flash", None, False, False, 1, 1),
            clock(), clock())
        assert recovered.finalize(work, late) == recovered.finalize(work, late) == outcome
        assert reopened._connection.execute("SELECT COUNT(*) FROM agent_authorities").fetchone()[0] == 0
    finally:
        reopened.close()


def test_failed_broker_detachment_seals_pending_shadow_without_late_resurrection(
        frozen_case, tmp_path: Path):
    from atlas.v2.runtime.action_critic_shadow import ActionAssessmentShadowCoordinator

    source_path, receipt, receipt_ref, sealed, profile, schedule = frozen_case
    path = tmp_path / "ops.sqlite"
    shutil.copy2(source_path, path)
    ledger, controller, clock = _fresh_controller(path, sealed, profile, schedule)
    release = threading.Event()
    io = _FakeProviderIO(ProviderResultV1(_valid_output(sealed.packet), "deepseek-flash", None,
                                        False, False, 100, 20), release=release)
    dispatcher = ActionAssessmentShadowDispatcher(io, now_ns=clock)
    coordinator = ActionAssessmentShadowCoordinator(profile=profile, controller=controller,
        ledger=ledger, dispatcher=dispatcher, now_ns=clock)
    try:
        with OpsRepository(path) as repository:
            action = repository.get_artifact(receipt.action_ref)
            coordinator(receipt, receipt_ref, repository)
            assert io.entered.wait(3) and len(coordinator._works) == 1
            coordinator.abandon_pending(repository=repository)
            state = ledger.request_state(sealed.request.request_id)
            assert state is not None and state["status"] == "UNAVAILABLE"
            assert state["failure_code"] == "BROKER_UNAVAILABLE" and not state["eligible"]
            assert not coordinator._works and not release.is_set()
            release.set()
            _wait(lambda: dispatcher._completion_queue.qsize() == 1)
            assert coordinator.drain_completed(max_items=1, repository=repository) == 0
            assert ledger.request_state(sealed.request.request_id) == state
            assert repository.get_artifact(receipt.action_ref) == action
            assert len(repository.artifact_entries("ActionCriticShadowObservationV1")) == 1
    finally:
        release.set()
        coordinator.close()
        dispatcher._thread.join(timeout=3)


@pytest.mark.parametrize("failure", ["startup", "process-exit"])
def test_installed_optional_broker_failure_keeps_public_runtime_running(
        tmp_path: Path, monkeypatch, failure: str):
    from atlas.v2.runtime import production, public_context

    monkeypatch.setattr(product, "build_identity", lambda: {
        "source_sha": "a" * 40, "version": "2.0.40.0", "runtime_lock_sha256": "b" * 64})
    run = product.create_run(tmp_path,
        product.ResearchRunConfigV1(provider_profile="deepseek-v41-action-critic-v1"))
    port = production.create_production_port()
    collect = port.collect
    collected = []
    detachments = []

    def collect_and_record(repository, **kwargs):
        result = collect(repository, **kwargs)
        collected.append(result)
        return result

    monkeypatch.setattr(port, "collect", collect_and_record)
    monkeypatch.setattr(production, "create_bybit_public_ws_port", lambda: port)
    monkeypatch.setattr(public_context, "PublicContextMaintenanceV1", lambda: SimpleNamespace(
        run_cycle=lambda *_args, **_kwargs: None, close=lambda: None))

    class FakeTime:
        value = 0.0
        time_ns = staticmethod(time.time_ns)
        process_time = staticmethod(time.process_time)

        def monotonic(self):
            self.value += .1
            return self.value

        def sleep(self, _seconds):
            pass

    monkeypatch.setattr(product, "time", FakeTime())
    shadow = SimpleNamespace(
        abandon_pending=lambda **_kwargs: detachments.append("sealed"),
        close=lambda: detachments.append("closed"))

    def start(_run, _epoch, *, service):
        service()
        if failure == "startup":
            raise RuntimeError("offline broker startup fixture failure")
        return SimpleNamespace(process=SimpleNamespace(poll=lambda: 2), shadow=shadow)

    monkeypatch.setattr(product, "_start_installed_critic", start)
    assert product.run_component(run, stop_requested=lambda: len(collected) >= 2) == 0
    assert len(collected) == 2
    state = json.loads((run / "status.json").read_text())
    assert state["provider_health"] == "TEST GATE"
    expected = "CONFIGURED_PROVIDER_STARTUP_FAILED" if failure == "startup" else "CONFIGURED_PROVIDER_BROKER_LOST"
    assert state["provider_reason"] == expected
    assert state["capital_enabled"] is state["assisted_enabled"] is False
    assert detachments == ([] if failure == "startup" else ["sealed", "closed"])
    assert state["reason"] == "PROCESS_STOP_REQUESTED"


def test_clean_owner_stop_seals_pending_critic_before_closing_and_rejects_late_result(
        frozen_case, tmp_path: Path, monkeypatch):
    from atlas.v2.runtime import production, public_context
    from atlas.v2.runtime.action_critic_dispatcher import ActionAssessmentDispatchCompletionV1
    from atlas.v2.runtime.action_critic_shadow import ActionAssessmentShadowCoordinator

    _source, _receipt, _receipt_ref, sealed, profile, schedule = frozen_case
    monkeypatch.setattr(product, "build_identity", lambda: {
        "source_sha": "a" * 40, "version": "2.0.40.0", "runtime_lock_sha256": "b" * 64})
    run = product.create_run(tmp_path,
        product.ResearchRunConfigV1(provider_profile="deepseek-v41-action-critic-v1"))
    release = threading.Event()
    stopped = threading.Event()
    io = _FakeProviderIO(ProviderResultV1(_valid_output(sealed.packet), "deepseek-flash", None,
                                        False, False, 100, 20), release=release)
    created = {}
    monkeypatch.setattr(production, "create_bybit_public_ws_port", production.create_production_port)
    monkeypatch.setattr(public_context, "PublicContextMaintenanceV1", lambda: SimpleNamespace(
        run_cycle=lambda *_args, **_kwargs: stopped.set(), close=lambda: None))

    def start(_run, epoch, *, service):
        service()
        ledger, controller, clock = _fresh_controller(run / "ops.sqlite", sealed, profile, schedule)
        dispatcher = ActionAssessmentShadowDispatcher(io, now_ns=clock)
        coordinator = ActionAssessmentShadowCoordinator(profile=profile, controller=controller,
            ledger=ledger, dispatcher=dispatcher, now_ns=clock)
        work = controller.prepare(sealed.request, sealed.packet)
        assert not isinstance(work, ActionAssessmentRunOutcomeV1)
        token = dispatcher.reserve_capacity()
        assert token is not None and dispatcher.submit_reserved(token, work)
        coordinator._works[work.identity.request_id] = work
        assert io.entered.wait(3)
        created.update(dispatcher=dispatcher, work=work, clock=clock)
        process = SimpleNamespace(poll=lambda: None, wait=lambda timeout: 0)
        return product._InstalledCriticRuntime(process, coordinator, run, epoch)

    monkeypatch.setattr(product, "_start_installed_critic", start)
    try:
        assert product.run_component(run, stop_requested=stopped.is_set) == 0
        assert not release.is_set() and io.calls == 1
        ledger, controller, clock = _fresh_controller(run / "ops.sqlite", sealed, profile, schedule)
        try:
            outcome = controller.prepare(sealed.request, sealed.packet)
            assert isinstance(outcome, ActionAssessmentRunOutcomeV1)
            assert outcome.status == "UNAVAILABLE" and outcome.reason_code == "BROKER_UNAVAILABLE"
            assert controller.recover_open_dispatches() == 0
            release.set()
            created["dispatcher"]._thread.join(timeout=3)
            late = ActionAssessmentDispatchCompletionV1(created["work"].identity,
                ProviderResultV1(_valid_output(sealed.packet), "deepseek-flash", None, False, False, 1, 1),
                clock(), clock())
            assert controller.finalize(created["work"], late) == outcome
            assert ledger._connection.execute("SELECT COUNT(*) FROM agent_authorities").fetchone()[0] == 0
        finally:
            ledger.close()
        assert list((run / "epochs").glob("*-broker-stop.json"))
    finally:
        release.set()
        if created:
            created["dispatcher"].close()
            created["dispatcher"]._thread.join(timeout=3)


def test_clean_owner_stop_closes_critic_even_when_pending_abandonment_fails(tmp_path: Path, monkeypatch):
    from atlas.v2.runtime import production, public_context

    monkeypatch.setattr(product, "build_identity", lambda: {
        "source_sha": "a" * 40, "version": "2.0.40.0", "runtime_lock_sha256": "b" * 64})
    run = product.create_run(tmp_path,
        product.ResearchRunConfigV1(provider_profile="deepseek-v41-action-critic-v1"))
    stopped = threading.Event()
    events = []
    monkeypatch.setattr(production, "create_bybit_public_ws_port", production.create_production_port)
    monkeypatch.setattr(public_context, "PublicContextMaintenanceV1", lambda: SimpleNamespace(
        run_cycle=lambda *_args, **_kwargs: stopped.set(), close=lambda: events.append("context-closed")))

    def failed_abandon(**_kwargs):
        events.append("abandon-failed")
        raise OSError("offline injected pending-terminal persistence failure")

    shadow = SimpleNamespace(abandon_pending=failed_abandon)
    critic = SimpleNamespace(process=SimpleNamespace(poll=lambda: None), shadow=shadow,
                             close=lambda: events.append("critic-closed"))
    monkeypatch.setattr(product, "_start_installed_critic", lambda *_args, **_kwargs: critic)
    assert product.run_component(run, stop_requested=stopped.is_set) == 2
    assert events == ["context-closed", "abandon-failed", "critic-closed"]
    state = json.loads((run / "status.json").read_text())
    assert state["status"] == "TEST GATE" and state["reason"] == "PROVIDER_SHUTDOWN_FAILED"
    assert state["error_type"] == "OSError"
    with OpsRepository(run / "ops.sqlite"):
        pass  # Sole-writer lease is released despite the shutdown fault.


@pytest.mark.parametrize("outcome", ["ready", "timeout", "process-exit"])
def test_installed_broker_readiness_services_public_backlog_before_ready_or_failure(
        tmp_path: Path, monkeypatch, outcome: str):
    from atlas.v2.agent_intelligence import windows_broker
    from atlas.v2.runtime import action_critic_shadow

    run = tmp_path / "run"
    (run / "epochs").mkdir(parents=True)
    epoch = "e" * 32
    services = []
    shadow = SimpleNamespace(close=lambda: None)

    class FakeTime:
        value = 0.0

        def monotonic(self):
            return self.value

        def sleep(self, seconds):
            self.value += seconds
            if outcome == "ready" and len(services) == 3:
                product._publish(run / "epochs" / (epoch + "-broker-status.json"),
                    {"run_id": run.name, "epoch_id": epoch, "health": {"status": "IMPLEMENTED"}})

    clock = FakeTime()

    class Process:
        terminated = False

        def poll(self):
            return 2 if outcome == "process-exit" and clock.value > .1 else None

        def terminate(self):
            self.terminated = True

        def wait(self, timeout):
            return 0

    process = Process()
    monkeypatch.setattr(product, "time", clock)
    monkeypatch.setattr(product.WindowsSecretStore, "_crypt", lambda body, decrypt: body)
    monkeypatch.setattr(product.subprocess, "Popen", lambda *_args, **_kwargs: process)
    monkeypatch.setattr(windows_broker, "current_owner_identity", lambda: {})
    monkeypatch.setattr(windows_broker, "WindowsActionCriticClientPort", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(action_critic_shadow, "create_action_assessment_shadow", lambda *_args, **_kwargs: shadow)
    monkeypatch.setattr(product, "_component_command", lambda *_args: ["offline-fake-broker"])
    monkeypatch.setattr(product, "resource_file", lambda *_args: tmp_path)

    def service():
        services.append(clock.value)

    if outcome == "ready":
        runtime = product._start_installed_critic(run, epoch, service=service)
        assert runtime.shadow is shadow and runtime.process is process
        assert len(services) == 3 and not process.terminated
    else:
        with pytest.raises(RuntimeError, match="did not become ready"):
            product._start_installed_critic(run, epoch, service=service)
        assert services and process.terminated
        assert max(b - a for a, b in zip(services, services[1:], strict=False)) <= .051
        assert clock.value < 15.1
