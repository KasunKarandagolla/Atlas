from __future__ import annotations

import json
from decimal import Decimal

from support.assisted_control_fixture import (
    NOW_NS,
    FakeProtectionPort,
    open_intent,
)

from atlas.domain.enums import LifecycleState, ProtectionStatus
from atlas.persistence.sqlite import SQLiteJournal
from atlas.runtime.assisted_control import (
    AssistedControlShell,
    _execute_protection_repair,
)


def _repair_command(tmp_path):
    journal = SQLiteJournal(tmp_path / "protection.db")
    intent = open_intent(journal)
    shell = AssistedControlShell(journal=journal, runtime_instance_id="runtime", writer_id="writer",
                                 writer_epoch=1)
    result = shell.protect(intent_id=intent.intent_id, reconciled_signed_qty=Decimal("0.01"),
                           expected_signed_qty=Decimal("0.01"), stop_price=Decimal("48000"),
                           protection_port=None, now_ns=NOW_NS)
    assert result.status == "PROTECTION_BLOCKED"
    assert result.command is not None and result.command.command_type.value == "REPAIR_STOP"
    return journal, intent, journal.load_trade_plan(intent.plan_id), result.command


def _run(journal, intent, plan, command, port):
    return _execute_protection_repair(journal=journal, plan=plan, intent=intent,
                                      command_id=command.command_id,
                                      expected_signed_qty=Decimal("0.01"), stop_price=Decimal("48000"),
                                      protection_port=port, now_ns=NOW_NS)


def test_valid_read_back_after_durable_repair_is_protected_and_persisted(tmp_path):
    journal, intent, plan, command = _repair_command(tmp_path)
    port = FakeProtectionPort(journal=journal, command_id=command.command_id,
                              position_epoch=intent.position_epoch, qty=Decimal("0.01"),
                              stop=Decimal("48000"), observed_at_ns=NOW_NS - 100_000_000)
    assert journal.load_command(command.command_id).send_started_at_ns is None
    result = _run(journal, intent, plan, command, port)
    assert result.status == "PROTECTED"
    durable = journal.load_intent(intent.intent_id)
    assert durable.lifecycle is LifecycleState.OPEN_PROTECTED
    assert durable.protection_status is ProtectionStatus.CONFIRMED
    assert journal.load_command(command.command_id).outcome.value == "RECONCILED"
    evidence = journal.load_protection_evidence(f"protection-{command.command_id}")
    assert evidence.is_confirmed and evidence.position_epoch == durable.position_epoch
    assert json.loads(command.payload)["position_epoch"] == durable.position_epoch
    assert port.ensure_calls and port.read_calls
    assert not [item for item in journal.load_commands_for_intent(intent.intent_id)
                if item.command_type.value == "SUBMIT_ENTRY"]
    journal.close()


def test_acknowledgement_without_valid_read_back_is_not_protected(tmp_path):
    journal, intent, plan, command = _repair_command(tmp_path)
    port = FakeProtectionPort(journal=journal, command_id=command.command_id,
                              position_epoch=intent.position_epoch, qty=Decimal("0.02"),
                              stop=Decimal("48000"), observed_at_ns=NOW_NS - 100_000_000)
    result = _run(journal, intent, plan, command, port)
    assert result.status == "PROTECTION_UNCONFIRMED"
    durable = journal.load_intent(intent.intent_id)
    assert durable.protection_status is ProtectionStatus.UNCONFIRMED
    assert durable.lifecycle is LifecycleState.OPEN_UNPROTECTED
    assert "signed quantity not exact current exposure" in result.reasons
    assert journal.load_command(command.command_id).outcome.value == "UNKNOWN"
    assert not journal.load_protection_evidence(f"protection-{command.command_id}").is_confirmed
    journal.close()


def test_wrong_stop_trigger_semantics_and_stale_evidence_are_unconfirmed(tmp_path):
    cases = [
        {"stop": Decimal("47000")},
        {"trigger_basis": "LastPrice"},
        {"semantics": "FULL_POSITION_MARKET"} ,
        {"observed_at_ns": NOW_NS - 3_000_000_000},
    ]
    for index, overrides in enumerate(cases):
        journal = SQLiteJournal(tmp_path / f"protection-{index}.db")
        intent = open_intent(journal)
        shell = AssistedControlShell(journal=journal, runtime_instance_id="runtime", writer_id="writer",
                                     writer_epoch=1)
        blocked = shell.protect(intent_id=intent.intent_id, reconciled_signed_qty=Decimal("0.01"),
                                expected_signed_qty=Decimal("0.01"), stop_price=Decimal("48000"),
                                protection_port=None, now_ns=NOW_NS)
        assert blocked.command is not None
        plan = journal.load_trade_plan(intent.plan_id)
        defaults = {"qty": Decimal("0.01"), "stop": Decimal("48000"), "observed_at_ns": NOW_NS - 100_000_000}
        defaults.update(overrides)
        port = FakeProtectionPort(journal=journal, command_id=blocked.command.command_id,
                                  position_epoch=intent.position_epoch, **defaults)
        result = _run(journal, intent, plan, blocked.command, port)
        assert result.status == "PROTECTION_UNCONFIRMED", overrides
        assert journal.load_intent(intent.intent_id).protection_status is ProtectionStatus.UNCONFIRMED
        assert journal.load_command(blocked.command.command_id).outcome.value == "UNKNOWN"
        journal.close()


class _TimeoutProtectionPort:
    def ensure_full_stop(self, *args, **kwargs):
        raise TimeoutError("no acknowledgement")

    def read_protection(self, *args, **kwargs):
        raise AssertionError("read-back must not follow an uncertain acknowledgement")


def test_timeout_stays_unconfirmed_unknown_and_never_protected(tmp_path):
    journal, intent, plan, command = _repair_command(tmp_path)
    result = _run(journal, intent, plan, command, _TimeoutProtectionPort())
    assert result.status == "PROTECTION_UNCONFIRMED"
    assert journal.load_command(command.command_id).outcome.value == "UNKNOWN"
    journal.close()


def test_open_unprotected_valid_readback_transitions_to_open_protected(tmp_path):
    journal = SQLiteJournal(tmp_path / "protection-open.db")
    intent = open_intent(journal, lifecycle=LifecycleState.OPEN_UNPROTECTED)
    shell = AssistedControlShell(journal=journal, runtime_instance_id="runtime", writer_id="writer",
                                 writer_epoch=1)
    blocked = shell.protect(intent_id=intent.intent_id, reconciled_signed_qty=Decimal("0.01"),
                            expected_signed_qty=Decimal("0.01"), stop_price=Decimal("48000"),
                            protection_port=None, now_ns=NOW_NS)
    assert blocked.command is not None
    plan = journal.load_trade_plan(intent.plan_id)
    port = FakeProtectionPort(journal=journal, command_id=blocked.command.command_id,
                              position_epoch=intent.position_epoch, qty=Decimal("0.01"),
                              stop=Decimal("48000"), observed_at_ns=NOW_NS - 100_000_000)
    result = _run(journal, intent, plan, blocked.command, port)
    assert result.status == "PROTECTED"
    durable = journal.load_intent(intent.intent_id)
    assert durable.lifecycle is LifecycleState.OPEN_PROTECTED
    assert durable.protection_status is ProtectionStatus.CONFIRMED
    journal.close()


def test_open_unprotected_invalid_readback_records_unconfirmed(tmp_path):
    journal = SQLiteJournal(tmp_path / "protection-open.db")
    intent = open_intent(journal, lifecycle=LifecycleState.OPEN_UNPROTECTED)
    shell = AssistedControlShell(journal=journal, runtime_instance_id="runtime", writer_id="writer",
                                 writer_epoch=1)
    blocked = shell.protect(intent_id=intent.intent_id, reconciled_signed_qty=Decimal("0.01"),
                            expected_signed_qty=Decimal("0.01"), stop_price=Decimal("48000"),
                            protection_port=None, now_ns=NOW_NS)
    assert blocked.command is not None
    plan = journal.load_trade_plan(intent.plan_id)
    port = FakeProtectionPort(journal=journal, command_id=blocked.command.command_id,
                              position_epoch=intent.position_epoch, qty=Decimal("0.02"),
                              stop=Decimal("48000"), observed_at_ns=NOW_NS - 100_000_000)
    result = _run(journal, intent, plan, blocked.command, port)
    assert result.status == "PROTECTION_UNCONFIRMED"
    durable = journal.load_intent(intent.intent_id)
    assert durable.lifecycle is LifecycleState.OPEN_UNPROTECTED
    assert durable.protection_status is ProtectionStatus.UNCONFIRMED
    journal.close()


def test_protection_evidence_from_another_epoch_is_unconfirmed(tmp_path):
    journal, intent, plan, command = _repair_command(tmp_path)
    port = FakeProtectionPort(journal=journal, command_id=command.command_id,
                              position_epoch=intent.position_epoch + 1, qty=Decimal("0.01"),
                              stop=Decimal("48000"), observed_at_ns=NOW_NS - 100_000_000)
    result = _run(journal, intent, plan, command, port)
    assert result.status == "PROTECTION_UNCONFIRMED"
    assert "position epoch mismatch" in result.reasons
    assert journal.load_command(command.command_id).outcome.value == "UNKNOWN"
    journal.close()
