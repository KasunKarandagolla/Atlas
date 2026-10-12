"""S48 deterministic protection faults. Fake readbacks never qualify a venue."""

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.runtime.binance_demo import verify_binance_protection
from atlas.runtime.protection_deadline import ProtectionDeadlineMachine, ProtectionDeadlineState
from tests.runtime.test_protection_deadline import T0, _fill, _observation


def confirmed():
    m = ProtectionDeadlineMachine()
    m.on_fill("intent-1", 0, _fill("one", "0.010", T0), 1, Decimal("48000"))
    m.on_protection_observation("intent-1", _observation("0.010"), T0 + 1_000_000_000)
    return m


def test_confirmed_readback_expires_without_an_llm_or_stream_event():
    m = confirmed()
    assert not m.tick(T0 + 2_000_000_000)
    s = m.tick(T0 + 3_000_000_000)["intent-1"]
    assert s.state == ProtectionDeadlineState.RECOVERY_REQUIRED
    assert s.cancel_scheduled and s.flatten_eligible


@pytest.mark.parametrize("fault", ["quantity", "epoch", "basis", "missing_refs", "future"])
def test_contradictory_current_readback_invalidates_confirmation(fault):
    changes = {
        "quantity": {"qty": Decimal("0.02")},
        "epoch": {"position_epoch": 1},
        "basis": {"trigger_basis": "LastPrice"},
        "missing_refs": {"evidence_ids": ()},
        "future": {"observed_at_ns": T0 + 10_000_000_000},
    }[fault]
    m = confirmed()
    s = m.on_protection_observation("intent-1", replace(_observation("0.010"), **changes), T0 + 1_000_000_001)
    assert s.state == ProtectionDeadlineState.RECOVERY_REQUIRED
    assert s.flatten_eligible


def test_placeholder_refs_do_not_verify_native_protection():
    m = confirmed()
    assert (
        m.on_protection_observation(
            "intent-1", replace(_observation("0.010"), evidence_ids=("UNVERIFIED",)), T0 + 1_000_000_001
        ).state
        == ProtectionDeadlineState.RECOVERY_REQUIRED
    )


def algo_row():
    # Fields/combination derived from pinned official Query Algo Order contract;
    # this is fake evidence and cannot establish native-wire qualification.
    return {
        "symbol": "SOLUSDT",
        "clientAlgoId": "a" * 32,
        "algoId": 123,
        "algoType": "CONDITIONAL",
        "orderType": "STOP_MARKET",
        "workingType": "MARK_PRICE",
        "positionSide": "BOTH",
        "side": "SELL",
        "closePosition": True,
        "algoStatus": "NEW",
        "triggerPrice": "120.00",
    }


def readback(row):
    return verify_binance_protection(
        row,
        symbol="SOLUSDT",
        client_order_id="a" * 32,
        stop=Decimal("120"),
        signed_position=Decimal("2"),
        now_ns=T0,
        received_at_ns=T0,
    )


@pytest.mark.parametrize(
    "change",
    [
        {"algoType": "NORMAL"},
        {"algoType": None},
        {"algoId": ""},
        {"orderType": None, "type": "STOP_MARKET"},
        {"algoStatus": None, "status": "NEW"},
        {"triggerPrice": None, "stopPrice": "120"},
        {"workingType": "CONTRACT_PRICE"},
        {"reduceOnly": True},
        {"quantity": "1"},
    ],
)
def test_normal_order_and_inconsistent_conditional_identity_cannot_certify_stop(change):
    assert not readback(algo_row() | change).verified


def test_conditional_query_response_zero_quantity_and_false_reduce_only_are_valid_response_fields():
    assert readback(algo_row() | {"quantity": "0", "reduceOnly": False}).verified


def test_failed_repair_keeps_existing_native_stop_and_unknown_command(tmp_path):
    from tests.runtime.test_assisted_control_protection import _repair_command, _run

    journal, intent, plan, command = _repair_command(tmp_path)

    class FailedRepair:
        def __init__(self):
            self.native_stop = "existing-full-mark-stop"

        def ensure_full_stop(self, *args):
            raise TimeoutError("fake ACK lost")

        def read_protection(self, *args):
            raise AssertionError("failed call cannot create positive readback")

        def cancel_order(self, *args):
            raise AssertionError("last protection must not be deleted")

    port = FailedRepair()
    try:
        result = _run(journal, intent, plan, command, port)
        assert result.command_outcome.value == "UNKNOWN"
        assert port.native_stop == "existing-full-mark-stop"
        assert journal.load_command(command.command_id).outcome.value == "UNKNOWN"
        assert journal.count("protection_evidence") == 0
    finally:
        journal.close()


def test_additional_fill_invalidates_old_quantity_stop_obligation():
    m = confirmed()
    snap = m.on_additional_fill("intent-1", _fill("late", "0.005", T0 + 1_000_000_001))
    assert snap.observed_signed_qty == Decimal("0.015")
    assert snap.state == ProtectionDeadlineState.UNCONFIRMED_POST_FILL
    assert (
        m.on_protection_observation("intent-1", _observation("0.010"), T0 + 1_000_000_002).state
        != ProtectionDeadlineState.CONFIRMED
    )
