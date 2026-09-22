from __future__ import annotations

import json
from decimal import Decimal

import pytest
from support.assisted_control_fixture import (
    NOW_NS,
    FakeNautilusPort,
    evidence,
    make_approval,
    make_plan,
    persist_ready_recovery,
)

from atlas.domain.capability import initial_unverified_fixture
from atlas.domain.enums import CommandType, LifecycleState, ProtectionStatus, ReconciliationHealth
from atlas.domain.execution import Intent, Reservation, generate_client_order_id
from atlas.persistence.sqlite import PersistenceError, SQLiteJournal
from atlas.runtime.assisted_control import (
    AssistedControlShell,
    DispatchAck,
    _dispatch_with_port,
)


def _prepared_shell(journal: SQLiteJournal, *, port=None):
    persist_ready_recovery(journal)
    plan = make_plan(journal)
    approval = make_approval(journal, plan)
    shell = AssistedControlShell(journal=journal, runtime_instance_id="runtime", writer_id="writer",
                                 writer_epoch=1, nautilus_port=port)
    result = shell.prepare_entry(plan=plan, approval_id=approval.approval_id, user_identity="user-1",
                                 evidence=evidence(plan))
    assert result.status == "DISPATCH_BLOCKED"
    assert result.command is not None
    return shell, plan, approval, result


def test_durable_command_ordering_precedes_port_call(tmp_path):
    journal = SQLiteJournal(tmp_path / "dispatch.db")
    port = FakeNautilusPort(ack=DispatchAck.DEFINITE_ACCEPT, journal=journal)
    _, _, _, result = _prepared_shell(journal, port=port)
    assert result.command is not None and result.command.send_started_at_ns is None
    assert result.intent is not None
    assert json.loads(result.command.payload)["orderLinkId"] == result.intent.client_order_id
    dispatched = _dispatch_with_port(journal=journal, command_id=result.command.command_id, port=port,
                                     now_ns=NOW_NS)
    assert dispatched.outcome.value == "DEFINITE_ACCEPT"
    assert journal.load_command(result.command.command_id).send_started_at_ns == NOW_NS
    assert port.calls == [result.command.command_id]
    assert port.observed_client_order_ids == [result.intent.client_order_id]
    assert port.observed == [(result.command.command_id, "SUBMITTING", 1)]
    journal.close()


def test_unknown_dispatch_retains_same_identity_and_reservation(tmp_path):
    journal = SQLiteJournal(tmp_path / "dispatch.db")
    port = FakeNautilusPort(ack=DispatchAck.UNKNOWN, journal=journal)
    shell, plan, first_approval, result = _prepared_shell(journal, port=port)
    assert result.intent is not None and result.command is not None
    client_order_id = result.intent.client_order_id
    dispatched = _dispatch_with_port(journal=journal, command_id=result.command.command_id, port=port,
                                     now_ns=NOW_NS)
    assert dispatched.outcome.value == "UNKNOWN"
    intent = journal.load_intent(result.intent.intent_id)
    assert intent.lifecycle is LifecycleState.SUBMIT_UNKNOWN
    assert intent.client_order_id == client_order_id
    assert json.loads(dispatched.payload)["orderLinkId"] == client_order_id
    assert journal.load_reservation(intent.intent_id).remaining_open_qty == Decimal("0.01")
    assert [command.command_type for command in journal.load_commands_for_intent(intent.intent_id)
            if command.command_type is CommandType.SUBMIT_ENTRY] == [CommandType.SUBMIT_ENTRY]

    second_approval = make_approval(journal, plan, approval_id="second-approval")
    retry = shell.prepare_entry(plan=plan, approval_id=second_approval.approval_id, user_identity="user-1",
                                evidence=evidence(plan))
    assert retry.status == "REVALIDATION_BLOCKED"
    assert "existing unresolved intent" in retry.reasons
    assert journal.load_approval(second_approval.approval_id).consumed_at_ns is None
    journal.close()


def test_monotonic_position_epoch_advances_after_closed_cycle(tmp_path):
    journal = SQLiteJournal(tmp_path / "dispatch.db")
    persist_ready_recovery(journal)
    plan = make_plan(journal)
    approval = make_approval(journal, plan)
    shell = AssistedControlShell(journal=journal, runtime_instance_id="runtime", writer_id="writer",
                                 writer_epoch=1)
    first = shell.prepare_entry(plan=plan, approval_id=approval.approval_id, user_identity="user-1",
                                evidence=evidence(plan))
    assert first.status == "DISPATCH_BLOCKED" and first.intent is not None
    assert first.intent.position_epoch == 1
    reject_port = FakeNautilusPort(ack=DispatchAck.DEFINITE_REJECT, journal=journal)
    assert first.command is not None
    _dispatch_with_port(journal=journal, command_id=first.command.command_id, port=reject_port, now_ns=NOW_NS)
    current = journal.load_intent(first.intent.intent_id)
    journal.update_intent_state(intent_id=current.intent_id, lifecycle=LifecycleState.CLOSED,
                                protection=ProtectionStatus.NONE, health=ReconciliationHealth.CURRENT,
                                expected_version=current.state_version)
    second_approval = make_approval(journal, plan, approval_id="approval-2")
    second = shell.prepare_entry(plan=plan, approval_id=second_approval.approval_id, user_identity="user-1",
                                 evidence=evidence(plan))
    assert second.status == "DISPATCH_BLOCKED" and second.intent is not None
    assert second.intent.position_epoch == 2
    journal.close()


def test_atomic_epoch_claim_allows_only_one_concurrent_winner(tmp_path):
    journal = SQLiteJournal(tmp_path / "dispatch.db")
    plan = make_plan(journal)
    first_approval = make_approval(journal, plan, approval_id="epoch-a")
    second_approval = make_approval(journal, plan, approval_id="epoch-b")
    epoch = journal.next_position_epoch()

    def preparation(intent_id: str):
        intent = Intent(intent_id, epoch, plan.plan_id, plan.version, generate_client_order_id(), 1,
                        LifecycleState.INTENT_PERSISTED, ProtectionStatus.NONE, ReconciliationHealth.CURRENT,
                        NOW_NS)
        reservation = Reservation(f"res-{intent_id}", intent_id, Decimal("0.01"), Decimal("10"), Decimal("25"),
                                  Decimal("490"), Decimal("490"), Decimal("100"), Decimal("0"))
        return intent, reservation

    first_intent, first_reservation = preparation("epoch-intent-a")
    second_intent, second_reservation = preparation("epoch-intent-b")
    journal.consume_approval_with_intent_reservation(
        approval_id=first_approval.approval_id, plan_id=plan.plan_id, plan_version=plan.version, now_ns=NOW_NS,
        intent=first_intent, reservation=first_reservation, expected_position_epoch=epoch)
    with pytest.raises(PersistenceError, match="expected next epoch"):
        journal.consume_approval_with_intent_reservation(
            approval_id=second_approval.approval_id, plan_id=plan.plan_id, plan_version=plan.version,
            now_ns=NOW_NS, intent=second_intent, reservation=second_reservation,
            expected_position_epoch=epoch)
    assert journal.load_approval(second_approval.approval_id).consumed_at_ns is None
    with pytest.raises(PersistenceError, match="intent not found"):
        journal.load_intent("epoch-intent-b")
    journal.close()


def test_definite_reject_is_distinct_and_does_not_release_reservation(tmp_path):
    journal = SQLiteJournal(tmp_path / "dispatch.db")
    port = FakeNautilusPort(ack=DispatchAck.DEFINITE_REJECT, journal=journal)
    _, _, _, result = _prepared_shell(journal, port=port)
    assert result.command is not None and result.intent is not None
    dispatched = _dispatch_with_port(journal=journal, command_id=result.command.command_id, port=port,
                                     now_ns=NOW_NS)
    assert dispatched.outcome.value == "DEFINITE_REJECT"
    intent = journal.load_intent(result.intent.intent_id)
    assert intent.lifecycle is LifecycleState.FLAT_PENDING_RECONCILIATION
    assert journal.load_reservation(intent.intent_id).remaining_open_qty == Decimal("0.01")
    with pytest.raises(PersistenceError, match="certified-flat"):
        journal.release_reservation(intent_id=intent.intent_id, certificate_id="missing",
                                    released_at_ns=NOW_NS)
    journal.close()


def test_atomic_consumption_rolls_back_on_injected_conflict(tmp_path):
    journal = SQLiteJournal(tmp_path / "dispatch.db")
    persist_ready_recovery(journal)
    plan = make_plan(journal)
    approval = make_approval(journal, plan)
    make_plan(journal, plan_id="collision-plan")
    collision = Intent(
        intent_id="collision",
        position_epoch=0,
        plan_id="collision-plan",
        plan_version="v1",
        client_order_id=generate_client_order_id(),
        writer_epoch=1,
        lifecycle=LifecycleState.CLOSED,
        protection_status=ProtectionStatus.NONE,
        reconciliation_health=ReconciliationHealth.CURRENT,
        created_at_ns=NOW_NS,
    )
    reservation = Reservation("res-collision", "collision", Decimal("0.01"), Decimal("10"), Decimal("25"),
                              Decimal("490"), Decimal("490"), Decimal("100"), Decimal("0"))
    journal.create_intent_with_reservation(collision, reservation)
    before = journal.count("reservations")
    result = AssistedControlShell(journal=journal, runtime_instance_id="runtime", writer_id="writer",
                                  writer_epoch=1).prepare_entry(
        plan=plan, approval_id=approval.approval_id, user_identity="user-1", evidence=evidence(plan),
        intent_id="collision")
    assert result.status == "PERSISTENCE_BLOCKED"
    assert journal.load_approval(approval.approval_id).consumed_at_ns is None
    assert journal.load_intent("collision").lifecycle is LifecycleState.CLOSED
    assert journal.count("reservations") == before
    journal.close()


def test_public_entry_dispatch_is_hard_disabled_with_unverified_capabilities(tmp_path):
    journal = SQLiteJournal(tmp_path / "dispatch.db")
    persist_ready_recovery(journal)
    plan = make_plan(journal)
    approval = make_approval(journal, plan)
    port = FakeNautilusPort(ack=DispatchAck.DEFINITE_ACCEPT, journal=journal)
    contract = initial_unverified_fixture()
    shell = AssistedControlShell(journal=journal, runtime_instance_id="runtime", writer_id="writer",
                                 writer_epoch=1, capability_contract=contract,
                                 capability_hash=contract.contract_hash(), all_qualified=False,
                                 assisted_enabled=False, nautilus_port=port)
    result = shell.prepare_entry(plan=plan, approval_id=approval.approval_id, user_identity="user-1",
                                 evidence=evidence(plan))
    assert result.status == "DISPATCH_BLOCKED"
    assert "assisted_enabled false" in result.reasons
    assert any("UNVERIFIED" in reason for reason in result.reasons)
    assert port.calls == []
    journal.close()
