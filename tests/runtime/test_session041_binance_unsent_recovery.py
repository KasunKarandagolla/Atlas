"""Crash after prepare commits, before the native scheduling queue is reached."""
from __future__ import annotations

import asyncio
import json
import queue
import threading
from contextlib import closing
from types import SimpleNamespace

import pytest
from test_session041_binance_readiness import (
    T0,
    EmptyOpenViews,
    active_intent,
    command_payload,
    identity,
    plan,
    product,
    snapshot,
)

from atlas.domain.enums import CommandOutcome, CommandType, LifecycleState
from atlas.domain.execution import make_command
from atlas.persistence.sqlite import PersistenceError, SQLiteJournal
from atlas.runtime.binance_demo import dispatch_persisted_demo_command
from atlas.runtime.binance_native import BinanceNativeNode
from atlas.runtime.binance_readiness import build_binance_command_readiness


def _prepare(journal, command_type=CommandType.SUBMIT_EXIT):
    id_value, product_value = identity(), product()
    plan(journal)
    initial_lifecycle = {CommandType.REPAIR_STOP: LifecycleState.OPEN_UNPROTECTED,
                         CommandType.CANCEL_ENTRY: LifecycleState.ENTRY_WORKING}.get(
                             command_type, LifecycleState.OPEN_PROTECTED)
    original = active_intent(journal, lifecycle=initial_lifecycle)
    payload = command_payload(id_value, product_value, original, "crash-window-command")
    next_lifecycle = LifecycleState.EXIT_PENDING
    if command_type == CommandType.CANCEL_ENTRY:
        payload.update(client_order_id=original.client_order_id, side="BUY", reduce_only=False)
        next_lifecycle = LifecycleState.CANCEL_PENDING
    elif command_type == CommandType.REPAIR_STOP:
        payload.update(quantity="2", price=None)
        next_lifecycle = LifecycleState.OPEN_UNPROTECTED
    command = journal.prepare_dispatch(
        intent_id=original.intent_id, expected_state_version=0, expected_reservation_version=1,
        command_id="crash-window-command", command_type=command_type, payload_dict=payload,
        created_at_ns=T0, next_lifecycle=next_lifecycle,
    )
    return id_value, product_value, command


def _runtime(journal, id_value, product_value, *, writer_epoch=2):
    runtime = BinanceNativeNode.__new__(BinanceNativeNode)
    runtime.journal = journal
    runtime.identity = id_value
    runtime._product = product_value
    runtime.writer_epoch = writer_epoch
    runtime.assert_writer = lambda: None
    runtime.command_queue = queue.Queue(maxsize=32)
    runtime.event_queue = queue.Queue(maxsize=256)
    runtime._queue_lock = threading.Lock()
    runtime._queued_command_ids = set()
    runtime._readiness_lock = threading.RLock()
    runtime._readiness_proof = object()  # prior proof may never survive hydration
    runtime._state_generation = 0
    runtime.reconciliation_ready = True
    runtime.readiness_reason = None
    runtime.last_failure_code = None
    runtime.queue_overflow = False
    runtime.stopped = False
    runtime._run_lock = asyncio.Lock()
    runtime._run_task = None
    return runtime


@pytest.mark.parametrize("command_type", [CommandType.SUBMIT_EXIT, CommandType.FLATTEN,
                                         CommandType.CANCEL_ENTRY, CommandType.REPAIR_STOP])
def test_prepare_commit_reopen_hydrates_only_the_existing_unsent_identity(tmp_path, command_type):
    path = tmp_path / "demo-control.sqlite"
    journal = SQLiteJournal(path)
    id_value, product_value, prepared = _prepare(journal, command_type)
    intent = journal.load_intent(prepared.intent_id)
    reservation = journal.load_reservation(prepared.intent_id)
    journal.close()  # process dies before enqueue_persisted_command
    with closing(SQLiteJournal(path)) as reopened:
        runtime = _runtime(reopened, id_value, product_value)
        assert runtime.hydrate_unsent_command() == prepared.command_id
        assert tuple(runtime.command_queue.queue) == (prepared.command_id,)
        assert runtime._readiness_proof is None and runtime.reconciliation_ready is False
        assert reopened.load_command(prepared.command_id) == prepared
        assert reopened.load_intent(prepared.intent_id) == intent
        assert reopened.load_reservation(prepared.intent_id) == reservation
        runtime.hydrate_unsent_command()
        assert runtime.command_queue.qsize() == 1


@pytest.mark.parametrize("outcome", [CommandOutcome.UNKNOWN, CommandOutcome.DEFINITE_ACCEPT,
                                    CommandOutcome.RECONCILED, CommandOutcome.DEFINITE_REJECT])
def test_restart_never_enqueues_started_or_terminal_commands(tmp_path, outcome):
    path = tmp_path / "demo-control.sqlite"
    journal = SQLiteJournal(path)
    id_value, product_value, prepared = _prepare(journal)
    journal.mark_send_started(prepared.command_id, T0 + 1)
    if outcome != CommandOutcome.UNKNOWN:
        journal.update_command_outcome(prepared.command_id, outcome)
    journal.close()
    with closing(SQLiteJournal(path)) as reopened:
        runtime = _runtime(reopened, id_value, product_value)
        assert runtime.hydrate_unsent_command() is None
        assert runtime.command_queue.empty()
        assert reopened.load_command(prepared.command_id).outcome == outcome


def test_opening_or_two_pending_identities_are_never_hydrated(journal):
    id_value, product_value, prepared = _prepare(journal)
    journal.persist_command(make_command(command_id="opening", intent_id=prepared.intent_id,
        command_type=CommandType.SUBMIT_ENTRY, payload_dict={"client_order_id": "b" * 32},
        expected_state_version=1, created_at_ns=T0 + 1))
    runtime = _runtime(journal, id_value, product_value)
    assert runtime.hydrate_unsent_command() is None
    assert runtime.command_queue.empty()
    assert runtime.readiness_reason == "BINANCE_UNSENT_RECOVERY_CONFLICT"
    journal.mark_send_started(prepared.command_id, T0 + 1)
    journal.update_command_outcome(prepared.command_id, CommandOutcome.RECONCILED)
    assert runtime.hydrate_unsent_command() is None
    assert runtime.command_queue.empty()
    assert runtime.readiness_reason == "BINANCE_UNSENT_RECOVERY_RECONCILIATION_REQUIRED"


@pytest.mark.parametrize("mismatch", ["account", "product", "chronology", "state", "writer"])
def test_unsent_recovery_refuses_incompatible_or_fenced_context(journal, mismatch):
    id_value, product_value, prepared = _prepare(journal)
    runtime = _runtime(journal, id_value, product_value)
    if mismatch == "account":
        runtime.identity = type(id_value)("different-account", id_value.credential_ref)
    elif mismatch == "product":
        from dataclasses import replace
        runtime._product = replace(product_value, metadata_ref="different-revision-source")
    elif mismatch == "chronology":
        from dataclasses import replace
        runtime._product = replace(product_value, effective_at_ns=T0 + 1,
                                   observed_at_ns=T0 + 1, available_at_ns=T0 + 1)
    elif mismatch == "state":
        intent = journal.load_intent(prepared.intent_id)
        journal.update_intent_lifecycle(intent.intent_id, intent.lifecycle, intent.protection_status,
                                       intent.reconciliation_health)
    else:
        def fenced():
            raise PersistenceError("stale writer")
        runtime.assert_writer = fenced
    with pytest.raises(PersistenceError):
        runtime.hydrate_unsent_command()
    assert runtime.command_queue.empty()
    assert journal.load_command(prepared.command_id).outcome == CommandOutcome.UNSENT


def test_native_start_hydrates_before_opening_connection(journal):
    id_value, product_value, prepared = _prepare(journal)
    runtime = _runtime(journal, id_value, product_value)

    async def run():
        def start():
            assert tuple(runtime.command_queue.queue) == (prepared.command_id,)
            assert runtime._readiness_proof is None
            future = asyncio.get_running_loop().create_future()
            future.set_result(None)
            return future
        runtime.node = SimpleNamespace(run_async=start)
        await runtime.run()
    asyncio.run(run())


def test_hydration_cannot_dispatch_without_new_exact_proof(journal):
    id_value, product_value, prepared = _prepare(journal)
    runtime = _runtime(journal, id_value, product_value)
    runtime.hydrate_unsent_command()
    account = snapshot(id_value)
    effects = []
    port = SimpleNamespace(identity=id_value, product=product_value,
        dispatch=lambda command, **kwargs: effects.append(command.command_id))
    options = {"journal": journal, "command_id": prepared.command_id, "port": port, "now_ns": T0,
               "writer_epoch": 2, "assert_writer": lambda: None, "snapshot_getter": lambda: account,
               "generation_getter": lambda: runtime.state_generation, "clock_ns": lambda: T0}
    with pytest.raises(PersistenceError, match="readiness"):
        dispatch_persisted_demo_command(**options)
    assert not effects and journal.load_command(prepared.command_id) == prepared
    proof = build_binance_command_readiness(EmptyOpenViews(id_value), journal,
        identity=id_value, product=product_value, snapshot=account, writer_epoch=2,
        native_generation=runtime.state_generation, assert_writer=lambda: None)
    assert proof is not None and proof.writer_epoch == 2
    result = dispatch_persisted_demo_command(**options, readiness_proof=proof)
    assert effects == [prepared.command_id] and result.outcome == CommandOutcome.UNKNOWN
    assert json.loads(result.payload)["client_order_id"] == json.loads(prepared.payload)["client_order_id"]
