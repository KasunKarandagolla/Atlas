"""Domain enums, money, time primitives."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from atlas.domain.enums import (
    AvailabilityClass,
    CapabilityStatus,
    CommandOutcome,
    LifecycleState,
    ProtectionStatus,
    ReconciliationHealth,
)
from atlas.domain.money import (
    canonical_decimal_str,
    ensure_decimal,
    ensure_fraction,
    ensure_non_negative_decimal,
    ensure_positive_decimal,
)
from atlas.domain.time import datetime_to_ns, ensure_utc_ns, ns_to_datetime


def test_lifecycle_enum_has_all_13_states():
    expected = {
        "PLAN_APPROVED",
        "INTENT_PERSISTED",
        "SUBMITTING",
        "SUBMIT_UNKNOWN",
        "ENTRY_WORKING",
        "PARTIALLY_FILLED",
        "OPEN_UNPROTECTED",
        "OPEN_PROTECTED",
        "CANCEL_PENDING",
        "EXIT_PENDING",
        "FLAT_PENDING_RECONCILIATION",
        "CLOSED",
        "RECOVERY_REQUIRED",
    }
    assert {s.value for s in LifecycleState} == expected


def test_protection_reconciliation_command_availability_enums():
    assert {s.value for s in ProtectionStatus} == {"NONE", "UNCONFIRMED", "CONFIRMED", "BREACHED"}
    assert {s.value for s in ReconciliationHealth} == {"CURRENT", "STALE", "CONFLICTED"}
    assert {s.value for s in CommandOutcome} == {
        "UNSENT",
        "UNKNOWN",
        "DEFINITE_ACCEPT",
        "DEFINITE_REJECT",
        "RECONCILED",
    }
    assert {s.value for s in AvailabilityClass} == {
        "ACTUAL_OBSERVED",
        "RECONSTRUCTED_PUBLIC",
        "UNKNOWN",
        "REVISED_NO_VINTAGE",
    }
    assert {s.value for s in CapabilityStatus} == {
        "UNVERIFIED",
        "SUPPORTED",
        "UNSUPPORTED",
        "FAILED",
    }


def test_enums_are_strings():
    assert isinstance(LifecycleState.CLOSED.value, str)
    assert LifecycleState("CLOSED") is LifecycleState.CLOSED
    with pytest.raises(ValueError):
        LifecycleState("BOGUS")


def test_ensure_decimal_rejects_nan_inf_float():
    with pytest.raises(ValueError):
        ensure_decimal(Decimal("NaN"))
    with pytest.raises(ValueError):
        ensure_decimal(Decimal("Infinity"))
    with pytest.raises(ValueError):
        ensure_decimal(1.5)  # float rejected
    with pytest.raises(ValueError):
        ensure_decimal(True)


def test_ensure_decimal_accepts_str_int_decimal():
    assert ensure_decimal("1.5") == Decimal("1.5")
    assert ensure_decimal(5) == Decimal("5")
    assert ensure_decimal(Decimal("2.0")) == Decimal("2.0")
    with pytest.raises(ValueError):
        ensure_decimal("not-a-number")


def test_non_negative_and_positive():
    ensure_non_negative_decimal("0")
    with pytest.raises(ValueError):
        ensure_non_negative_decimal("-0.01")
    ensure_positive_decimal("0.0001")
    with pytest.raises(ValueError):
        ensure_positive_decimal("0")
    with pytest.raises(ValueError):
        ensure_positive_decimal("-1")


def test_fraction_semantics():
    # 0.001 means 0.10%
    assert ensure_fraction("0.001") == Decimal("0.001")
    with pytest.raises(ValueError):
        ensure_fraction("-0.001")
    with pytest.raises(ValueError):
        ensure_fraction("1.5")  # >1 rejected for generic risk fractions


def test_canonical_decimal_str_deterministic():
    assert canonical_decimal_str(Decimal("1.00")) == "1"
    assert canonical_decimal_str(Decimal("1.0")) == "1"
    assert canonical_decimal_str(Decimal("0.0010")) == "0.001"
    assert canonical_decimal_str(Decimal("0")) == "0"


@given(st.decimals(allow_nan=False, allow_infinity=False, places=6))
def test_canonical_decimal_roundtrip_stable(value):
    s = canonical_decimal_str(value)
    assert Decimal(s) == value


def test_time_rejects_naive_and_negative():
    with pytest.raises(ValueError):
        datetime_to_ns(datetime(2026, 1, 1, 0, 0, 0))  # naive
    with pytest.raises(ValueError):
        ensure_utc_ns(-1)
    with pytest.raises(ValueError):
        ensure_utc_ns("123")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        ensure_utc_ns(True)  # type: ignore[arg-type]


def test_time_roundtrip_aware():
    dt = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=timezone.utc)
    ns = datetime_to_ns(dt)
    assert isinstance(ns, int) and ns > 0
    back = ns_to_datetime(ns)
    assert back.tzinfo is not None
    assert back.replace(microsecond=0) == dt.replace(microsecond=0)
    # microsecond preserved at ms granularity
    assert abs((back - dt).total_seconds()) < 0.001
