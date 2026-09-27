from __future__ import annotations

import json

import pytest
from support.assisted_control_fixture import (
    NOW_NS,
    evidence,
    make_approval,
    make_plan,
    persist_ready_recovery,
)

from atlas.domain.enums import CommandOutcome
from atlas.persistence.sqlite import PersistenceError, SQLiteJournal
from atlas.runtime.assisted_control import AssistedControlShell, _dispatch_with_port


class _PortMustNotBeCalled:
    def __init__(self):
        self.called = False

    def dispatch(self, command):
        self.called = True
        raise AssertionError("venue boundary must not run")


def test_atomic_approval_intent_reservation_failure_has_no_external_effect(tmp_path, monkeypatch):
    journal = SQLiteJournal(tmp_path / "persist-before-intent.sqlite")
    persist_ready_recovery(journal)
    before = {table: journal.count(table) for table in ("intents", "reservations", "commands")}
    plan = make_plan(journal)
    approval = make_approval(journal, plan)
    port = _PortMustNotBeCalled()

    def disk_full(**_kwargs):
        raise PersistenceError("injected disk-full before atomic commit")

    monkeypatch.setattr(journal, "consume_approval_with_intent_reservation", disk_full)
    shell = AssistedControlShell(journal=journal, runtime_instance_id="runtime", writer_id="writer",
        writer_epoch=1, nautilus_port=port)
    result = shell.prepare_entry(plan=plan, approval_id=approval.approval_id, user_identity="user-1",
        evidence=evidence(plan))
    assert result.status == "PERSISTENCE_BLOCKED"
    assert port.called is False
    assert journal.load_approval(approval.approval_id).consumed_at_ns is None
    assert {table: journal.count(table) for table in before} == before
    journal.close()


def test_crash_after_atomic_reservation_before_command_keeps_risk_and_consumed_approval(tmp_path, monkeypatch):
    journal = SQLiteJournal(tmp_path / "persist-before-command.sqlite")
    persist_ready_recovery(journal)
    before = {table: journal.count(table) for table in ("intents", "reservations", "commands")}
    plan = make_plan(journal)
    approval = make_approval(journal, plan)
    port = _PortMustNotBeCalled()

    def command_write_fails(**_kwargs):
        raise PersistenceError("injected crash before command persistence")

    monkeypatch.setattr(journal, "prepare_dispatch", command_write_fails)
    shell = AssistedControlShell(journal=journal, runtime_instance_id="runtime", writer_id="writer",
        writer_epoch=1, nautilus_port=port)
    with pytest.raises(PersistenceError, match="before command persistence"):
        shell.prepare_entry(plan=plan, approval_id=approval.approval_id, user_identity="user-1",
            evidence=evidence(plan))
    assert port.called is False
    assert journal.load_approval(approval.approval_id).consumed_at_ns == NOW_NS
    assert journal.count("intents") == before["intents"] + 1
    assert journal.count("reservations") == before["reservations"] + 1
    assert journal.count("commands") == before["commands"]
    assert len(journal.load_unresolved_intents()) == 1
    journal.close()


def test_crash_after_send_started_keeps_original_identity_and_reservation(tmp_path):
    journal = SQLiteJournal(tmp_path / "send-started.sqlite")
    persist_ready_recovery(journal)
    plan = make_plan(journal)
    approval = make_approval(journal, plan)
    shell = AssistedControlShell(journal=journal, runtime_instance_id="runtime", writer_id="writer",
        writer_epoch=1)
    prepared = shell.prepare_entry(plan=plan, approval_id=approval.approval_id, user_identity="user-1",
        evidence=evidence(plan))
    assert prepared.command is not None and prepared.intent is not None
    original_identity = prepared.intent.client_order_id

    class CrashAfterMarker:
        def dispatch(self, command):
            stored = journal.load_command(command.command_id)
            assert stored.send_started_at_ns == NOW_NS
            assert json.loads(stored.payload)["orderLinkId"] == original_identity
            raise TimeoutError("injected writer death at venue call boundary")

    with pytest.raises(TimeoutError):
        _dispatch_with_port(journal=journal, command_id=prepared.command.command_id,
            port=CrashAfterMarker(), now_ns=NOW_NS)
    command = journal.load_command(prepared.command.command_id)
    intent = journal.load_intent(prepared.intent.intent_id)
    assert command.send_started_at_ns == NOW_NS
    assert command.outcome == CommandOutcome.UNKNOWN
    assert intent.client_order_id == original_identity
    assert journal.load_reservation(intent.intent_id).remaining_open_qty == plan.qty_limit
    retry_approval = make_approval(journal, plan, approval_id="no-duplicate-retry")
    retry = shell.prepare_entry(plan=plan, approval_id=retry_approval.approval_id, user_identity="user-1",
        evidence=evidence(plan))
    assert retry.status == "REVALIDATION_BLOCKED"
    assert len(journal.load_commands_for_intent(intent.intent_id)) == 1
    journal.close()
