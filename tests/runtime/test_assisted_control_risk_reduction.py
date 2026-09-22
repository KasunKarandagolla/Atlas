from __future__ import annotations

import json
from decimal import Decimal

from support.assisted_control_fixture import (
    NOW_NS,
    FakeNautilusPort,
    FakeProtectionPort,
    evidence,
    make_approval,
    make_plan,
    open_intent,
    persist_ready_recovery,
)

from atlas.domain.enums import LifecycleState
from atlas.persistence.sqlite import SQLiteJournal
from atlas.runtime.assisted_control import (
    AssistedControlShell,
    DispatchAck,
    _dispatch_with_port,
    _ensure_stop_with_port,
)


def _shell(journal: SQLiteJournal) -> AssistedControlShell:
    return AssistedControlShell(journal=journal, runtime_instance_id="runtime", writer_id="writer",
                                writer_epoch=1)


def test_close_is_explicit_quantity_reduce_only_and_dispatches_after_durable_command(tmp_path):
    journal = SQLiteJournal(tmp_path / "reduce.db")
    intent = open_intent(journal)
    shell = _shell(journal)
    result = shell.close(intent_id=intent.intent_id, reconciled_signed_qty=Decimal("0.01"),
                         quantity=Decimal("0.01"), exit_price=Decimal("49000"), now_ns=NOW_NS)
    assert result.status == "RISK_REDUCTION_DISPATCH_BLOCKED"
    assert result.command is not None and result.command.send_started_at_ns is None
    assert journal.load_intent(intent.intent_id).lifecycle is LifecycleState.EXIT_PENDING
    payload = json.loads(result.command.payload)
    assert payload["reduceOnly"] is True and payload["qty"] == "0.01" and payload["side"] == "Sell"
    port = FakeNautilusPort(ack=DispatchAck.DEFINITE_ACCEPT, journal=journal)
    dispatched = _dispatch_with_port(journal=journal, command_id=result.command.command_id, port=port,
                                     now_ns=NOW_NS)
    assert dispatched.outcome.value == "DEFINITE_ACCEPT"
    assert journal.load_command(result.command.command_id).send_started_at_ns == NOW_NS
    journal.close()


def test_close_cannot_exceed_reconciled_exposure_or_reverse(tmp_path):
    journal = SQLiteJournal(tmp_path / "reduce.db")
    intent = open_intent(journal)
    shell = _shell(journal)
    commands_before = journal.count("commands")
    too_large = shell.close(intent_id=intent.intent_id, reconciled_signed_qty=Decimal("0.01"),
                            quantity=Decimal("0.02"), exit_price=Decimal("49000"), now_ns=NOW_NS)
    assert too_large.status == "RISK_REDUCTION_BLOCKED"
    assert "close exceeds current position" in too_large.reasons
    assert journal.count("commands") == commands_before
    journal.close()


def test_flatten_is_explicit_quantity_reduce_only_market_and_no_reversal(tmp_path):
    journal = SQLiteJournal(tmp_path / "reduce.db")
    intent = open_intent(journal)
    shell = _shell(journal)
    result = shell.flatten(intent_id=intent.intent_id, reconciled_signed_qty=Decimal("0.01"),
                           now_ns=NOW_NS)
    assert result.status == "RISK_REDUCTION_DISPATCH_BLOCKED"
    assert result.command is not None
    payload = json.loads(result.command.payload)
    assert payload["reduceOnly"] is True and payload["qty"] == "0.01"
    assert payload["orderType"] == "MARKET" and "price" not in payload
    port = FakeNautilusPort(ack=DispatchAck.DEFINITE_ACCEPT, journal=journal)
    dispatched = _dispatch_with_port(journal=journal, command_id=result.command.command_id, port=port,
                                     now_ns=NOW_NS)
    assert dispatched.outcome.value == "DEFINITE_ACCEPT"
    journal.close()


def test_protect_uses_only_protection_port_and_never_creates_entry_command(tmp_path):
    journal = SQLiteJournal(tmp_path / "reduce.db")
    intent = open_intent(journal)
    shell = _shell(journal)
    protection = FakeProtectionPort()
    entry_commands_before = sum(1 for command in journal.load_commands_for_intent(intent.intent_id)
                                if command.command_type.value == "SUBMIT_ENTRY")
    blocked = shell.protect(intent_id=intent.intent_id, reconciled_signed_qty=Decimal("0.01"),
                            expected_signed_qty=Decimal("0.01"),
                            stop_price=Decimal("48000"), protection_port=protection, now_ns=NOW_NS)
    assert blocked.status == "PROTECTION_BLOCKED"
    assert protection.calls == []
    plan = journal.load_trade_plan(intent.plan_id)
    protected = _ensure_stop_with_port(journal=journal, plan=plan, intent=intent,
                                       expected_signed_qty=Decimal("0.01"), stop_price=Decimal("48000"),
                                       protection_port=protection, now_ns=NOW_NS)
    assert protected.status == "PROTECTED"
    assert protection.calls == [(0, Decimal("0.01"), Decimal("48000"), "MarkPrice")]
    assert sum(1 for command in journal.load_commands_for_intent(intent.intent_id)
               if command.command_type.value == "SUBMIT_ENTRY") == entry_commands_before
    journal.close()


def test_pause_blocks_new_opening_but_not_risk_reducing_close(tmp_path):
    journal = SQLiteJournal(tmp_path / "reduce.db")
    persist_ready_recovery(journal)
    plan = make_plan(journal)
    approval = make_approval(journal, plan)
    shell = _shell(journal)
    shell.pause()
    opened = shell.prepare_entry(plan=plan, approval_id=approval.approval_id, user_identity="user-1",
                                 evidence=evidence(plan))
    assert opened.status == "PAUSED"
    assert journal.load_approval(approval.approval_id).consumed_at_ns is None
    intent = open_intent(journal)
    reduced = shell.close(intent_id=intent.intent_id, reconciled_signed_qty=Decimal("0.01"),
                          quantity=Decimal("0.01"), exit_price=Decimal("49000"), now_ns=NOW_NS)
    assert reduced.status == "RISK_REDUCTION_DISPATCH_BLOCKED"
    assert reduced.command is not None
    assert shell.status().paused is True
    journal.close()
