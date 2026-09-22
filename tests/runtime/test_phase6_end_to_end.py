from __future__ import annotations

import json
from decimal import Decimal

from support.assisted_control_fixture import (
    NOW_NS,
    FakeNautilusPort,
    evidence,
    make_approval,
    make_plan,
    open_intent,
    persist_ready_recovery,
)

from atlas.domain.capability import initial_unverified_fixture
from atlas.domain.enums import LifecycleState
from atlas.persistence.sqlite import SQLiteJournal
from atlas.runtime.assisted_control import (
    AssistedControlShell,
    DispatchAck,
    _dispatch_with_port,
)


def test_phase6_unknown_path_keeps_identity_and_requires_recovery(tmp_path):
    journal = SQLiteJournal(tmp_path / "phase6.db")
    persist_ready_recovery(journal)
    plan = make_plan(journal)
    approval = make_approval(journal, plan)
    port = FakeNautilusPort(ack=DispatchAck.UNKNOWN, journal=journal)
    contract = initial_unverified_fixture()
    shell = AssistedControlShell(journal=journal, runtime_instance_id="runtime", writer_id="writer",
                                 writer_epoch=1, capability_contract=contract,
                                 capability_hash=contract.contract_hash(), all_qualified=False,
                                 assisted_enabled=False, nautilus_port=port)
    prepared = shell.prepare_entry(plan=plan, approval_id=approval.approval_id, user_identity="user-1",
                                   evidence=evidence(plan))
    assert prepared.status == "DISPATCH_BLOCKED"
    assert prepared.intent is not None and prepared.command is not None
    client_order_id = prepared.intent.client_order_id
    assert port.calls == []

    dispatched = _dispatch_with_port(journal=journal, command_id=prepared.command.command_id, port=port,
                                     now_ns=NOW_NS)
    assert dispatched.outcome.value == "UNKNOWN"
    unknown_intent = journal.load_intent(prepared.intent.intent_id)
    assert unknown_intent.lifecycle is LifecycleState.SUBMIT_UNKNOWN
    assert unknown_intent.client_order_id == client_order_id
    assert journal.load_reservation(unknown_intent.intent_id).remaining_open_qty == Decimal("0.01")
    assert journal.count("execution_evidence") == 0

    retry_approval = make_approval(journal, plan, approval_id="retry-approval")
    retry = shell.prepare_entry(plan=plan, approval_id=retry_approval.approval_id, user_identity="user-1",
                                evidence=evidence(plan))
    assert retry.status == "REVALIDATION_BLOCKED"
    assert journal.load_approval(retry_approval.approval_id).consumed_at_ns is None
    assert len([command for command in journal.load_commands_for_intent(unknown_intent.intent_id)
                if command.command_type.value == "SUBMIT_ENTRY"]) == 1
    status = shell.status(recovery_run_id="recovery-p6")
    assert status.unresolved_intents == 1 and status.unknown_commands == 1
    journal.close()


def test_phase6_reduce_only_flatten_path_never_reverses(tmp_path):
    journal = SQLiteJournal(tmp_path / "phase6.db")
    intent = open_intent(journal, intent_id="flatten-intent")
    shell = AssistedControlShell(journal=journal, runtime_instance_id="runtime", writer_id="writer",
                                 writer_epoch=1)
    prepared = shell.flatten(intent_id=intent.intent_id, reconciled_signed_qty=Decimal("0.01"),
                             now_ns=NOW_NS)
    assert prepared.status == "RISK_REDUCTION_DISPATCH_BLOCKED"
    assert prepared.command is not None and prepared.command.send_started_at_ns is None
    payload = json.loads(prepared.command.payload)
    assert payload["reduceOnly"] is True and payload["qty"] == "0.01" and payload["side"] == "Sell"
    port = FakeNautilusPort(ack=DispatchAck.DEFINITE_ACCEPT, journal=journal)
    dispatched = _dispatch_with_port(journal=journal, command_id=prepared.command.command_id, port=port,
                                     now_ns=NOW_NS)
    assert dispatched.outcome.value == "DEFINITE_ACCEPT"
    assert journal.load_reservation(intent.intent_id).remaining_open_qty == Decimal("0.01")
    journal.close()
