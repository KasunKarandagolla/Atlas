"""Execution records tests: client IDs, UNKNOWN preservation, independence, reservation."""

from __future__ import annotations

from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from atlas.domain.enums import (
    CommandOutcome,
    CommandType,
    LifecycleState,
    ProtectionStatus,
    ReconciliationHealth,
)
from atlas.domain.execution import (
    generate_client_order_id,
    make_command,
    validate_client_order_id,
)


def test_client_order_id_format():
    cid = generate_client_order_id()
    assert len(cid) == 32
    assert cid == cid.lower()
    int(cid, 16)  # valid hex
    validate_client_order_id(cid)
    with pytest.raises(ValueError):
        validate_client_order_id("ABC" + "0" * 29)  # uppercase
    with pytest.raises(ValueError):
        validate_client_order_id("short")
    with pytest.raises(ValueError):
        validate_client_order_id("z" * 32)


@given(st.integers(min_value=50, max_value=500))
def test_client_order_id_uniqueness_sample(n):
    ids = {generate_client_order_id() for _ in range(n)}
    assert len(ids) == n
    for cid in ids:
        validate_client_order_id(cid)


def test_lifecycle_enum_integrity_and_unknown_preserved():
    # Every lifecycle value round-trips
    for s in LifecycleState:
        assert LifecycleState(s.value) is s
    cmd = make_command(
        command_id="cmd-1",
        intent_id="intent-1",
        command_type=CommandType.SUBMIT_ENTRY,
        payload_dict={"side": "LONG", "qty": "0.01"},
        expected_state_version=0,
        created_at_ns=1_000,
    )
    assert cmd.outcome == CommandOutcome.UNSENT
    # UNKNOWN must be representable and preserved (no auto-resolution)
    import dataclasses

    unknown = dataclasses.replace(cmd, outcome=CommandOutcome.UNKNOWN)
    assert unknown.outcome == CommandOutcome.UNKNOWN
    assert unknown.exact_payload_hash == cmd.exact_payload_hash


def test_command_payload_hash_binding():
    cmd = make_command(
        command_id="cmd-2",
        intent_id="i-2",
        command_type=CommandType.SUBMIT_EXIT,
        payload_dict={"a": 1},
        expected_state_version=0,
        created_at_ns=100,
    )
    import dataclasses

    with pytest.raises(ValueError, match="exact_payload_hash does not match"):
        dataclasses.replace(cmd, payload='{"tampered":true}')


def test_protection_reconciliation_independent():
    from atlas.domain.execution import Intent

    for prot in ProtectionStatus:
        for health in ReconciliationHealth:
            i = Intent(
                intent_id=f"i-{prot.value}-{health.value}",
                position_epoch=0,
                plan_id="p",
                plan_version="v1",
                client_order_id=generate_client_order_id(),
                writer_epoch=1,
                lifecycle=LifecycleState.OPEN_UNPROTECTED
                if prot != ProtectionStatus.CONFIRMED
                else LifecycleState.OPEN_PROTECTED,
                protection_status=prot,
                reconciliation_health=health,
                created_at_ns=1000,
            )
            assert i.protection_status is prot
            assert i.reconciliation_health is health


def test_reservation_fields_validate():
    from atlas.domain.execution import Reservation

    Reservation(
        reservation_id="r1",
        intent_id="i1",
        remaining_open_qty=Decimal("0.01"),
        normal_loss=Decimal("10"),
        stress_loss=Decimal("25"),
        notional=Decimal("500"),
        beta_adjusted_notional=Decimal("400"),
        margin=Decimal("100"),
        es_contribution=Decimal("5"),
    )
    with pytest.raises(ValueError):
        Reservation(
            reservation_id="r2",
            intent_id="i1",
            remaining_open_qty=Decimal("-0.01"),
            normal_loss=Decimal("10"),
            stress_loss=Decimal("25"),
            notional=Decimal("500"),
            beta_adjusted_notional=Decimal("400"),
            margin=Decimal("100"),
            es_contribution=Decimal("5"),
        )
    with pytest.raises(ValueError):
        Reservation(
            reservation_id="r3",
            intent_id="i1",
            remaining_open_qty=Decimal("0.01"),
            normal_loss=Decimal("-1"),
            stress_loss=Decimal("25"),
            notional=Decimal("500"),
            beta_adjusted_notional=Decimal("400"),
            margin=Decimal("100"),
            es_contribution=Decimal("5"),
        )
