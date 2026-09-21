from __future__ import annotations

from decimal import Decimal

from atlas.domain.execution import ProtectionObservation
from atlas.runtime.fill_dedup import FillRecord
from atlas.runtime.protection_deadline import (
    ProtectionDeadlineMachine,
    ProtectionDeadlineState,
    persist_deadline_actions,
)
from tests.support.fault_injection import DeterministicVenueSimulator

T0 = 1_700_000_000_000_000_000
T1 = T0 + 1_000_000_000


def _fill(execution_id: str, qty: str, time_ns: int) -> FillRecord:
    return FillRecord(
        execution_id=execution_id, order_id="order-1", client_order_id="a" * 32,
        intent_id="intent-1", instrument="BTCUSDT", side="Buy", qty=Decimal(qty),
        price=Decimal("49000"), fee=Decimal("0"), fee_currency="USDT",
        trade_time_ns=time_ns, receive_time_ns=time_ns + 1, source="private_stream", raw_hash=execution_id,
    )


def _observation(qty: str, *, stop: str = "48000", trigger: str = "MarkPrice", at: int = T0 + 1_000_000_000) -> ProtectionObservation:
    return ProtectionObservation(
        position_epoch=0, desired_stop_version=1, qty=Decimal(qty), trigger_basis=trigger,
        stop_price=Decimal(stop), semantics="Full Market ReduceOnly", evidence_ids=("position-1",), observed_at_ns=at,
    )


def test_two_second_breach_schedules_all_recovery_actions():
    machine = ProtectionDeadlineMachine()
    machine.on_fill("intent-1", 0, _fill("exec-1", "0.004", T0), 1, Decimal("48000"))
    transitions = machine.tick(T0 + 2_000_000_000)
    snapshot = transitions["intent-1"]
    assert snapshot.state == ProtectionDeadlineState.RECOVERY_REQUIRED
    assert machine.get_scheduled_actions("intent-1") == {
        "cancel_entry_leaves": True,
        "reduce_only_flatten_eligible": True,
        "stop_repair_intent": {"position_epoch": 0, "desired_stop_version": 1, "attempt": 1},
        "recovery_required": True,
    }


def test_additional_fill_after_breach_does_not_clear_actions():
    machine = ProtectionDeadlineMachine()
    machine.on_fill("intent-1", 0, _fill("exec-1", "0.004", T0), 1, Decimal("48000"))
    machine.tick(T0 + 2_000_000_000)
    snapshot = machine.on_additional_fill("intent-1", _fill("exec-2", "0.006", T0 + 3_000_000_000))
    assert snapshot is not None
    assert snapshot.state == ProtectionDeadlineState.RECOVERY_REQUIRED
    assert snapshot.observed_signed_qty == Decimal("0.010")
    assert machine.get_scheduled_actions("intent-1")["reduce_only_flatten_eligible"]


def test_wrong_semantics_stale_and_future_observations_do_not_confirm():
    machine = ProtectionDeadlineMachine()
    machine.on_fill("intent-1", 0, _fill("exec-1", "0.010", T0), 1, Decimal("48000"))
    wrong = _observation("0.010", stop="47000")
    future = _observation("0.010", at=T0 + 3_000_000_000)
    assert machine.on_protection_observation("intent-1", wrong, T0 + 1_000_000_000).state != ProtectionDeadlineState.CONFIRMED
    assert machine.on_protection_observation("intent-1", future, T0 + 1_000_000_000).state != ProtectionDeadlineState.CONFIRMED
    valid = _observation("0.010")
    assert machine.on_protection_observation("intent-1", valid, T0 + 1_000_000_000).state == ProtectionDeadlineState.CONFIRMED


def test_deadline_actions_are_durable_unsent_commands_without_transport():
    simulator = DeterministicVenueSimulator()
    simulator._open()
    try:
        fill = simulator._fill("exec-durable", "0.010")
        machine = ProtectionDeadlineMachine()
        machine.on_fill("intent-fault", 0, fill, 1, Decimal("48000"))
        snapshot = machine.tick(T1 + 2_000_000_000)["intent-fault"]
        command_ids = persist_deadline_actions(simulator.journal, snapshot, T0 + 2_000_000_001)
        commands = [simulator.journal.load_command(command_id) for command_id in command_ids]
        assert simulator.journal.load_intent("intent-fault").lifecycle.value == "RECOVERY_REQUIRED"
        assert {command.command_type.value for command in commands} == {"CANCEL_ENTRY", "REPAIR_STOP", "FLATTEN"}
        assert all(command.send_started_at_ns is None for command in commands)
    finally:
        simulator._close()
