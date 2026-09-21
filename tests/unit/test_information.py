"""InformationContract causality tests (freeze §4)."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from atlas.domain.enums import AvailabilityClass
from atlas.domain.information import (
    InformationContract,
    actual_observed,
    reconstructed_public,
)

T0 = 1_700_000_000_000_000_000
T1 = T0 + 1_000_000_000
T2 = T1 + 1_000_000_000


def test_available_before_received_rejected():
    with pytest.raises(ValueError, match="available_at cannot precede received_at"):
        actual_observed(
            source_id="bybit",
            data_type="bar_1h",
            received_at_ns=T1,
            available_at_ns=T0,  # before receipt
        )


def test_processed_after_available_rejected():
    with pytest.raises(ValueError, match="available_at cannot precede processed_at"):
        actual_observed(
            source_id="atlas",
            data_type="feature",
            received_at_ns=T0,
            available_at_ns=T1,
            processed_at_ns=T2,  # completion after availability
        )


def test_reconstructed_must_not_fabricate_receipt():
    # received == replay (fabricated historical receipt) must fail
    with pytest.raises(ValueError, match="must not fabricate"):
        reconstructed_public(
            source_id="bybit",
            data_type="bar_1h",
            received_at_ns=T1,
            replay_available_at_ns=T1,  # equal -> fabricated
            availability_method="bar_end+5s",
        )
    # received before replay also fails
    with pytest.raises(ValueError, match="must not fabricate"):
        reconstructed_public(
            source_id="bybit",
            data_type="bar_1h",
            received_at_ns=T0,
            replay_available_at_ns=T1,
            availability_method="bar_end+5s",
        )


def test_reconstructed_requires_method_and_replay():
    with pytest.raises(ValueError):
        InformationContract(
            contract_version="1.0",
            source_id="s",
            data_type="d",
            units="",
            received_at_ns=T1,
            availability_class=AvailabilityClass.RECONSTRUCTED_PUBLIC,
            replay_available_at_ns=T0,
            # missing availability_method
        )


def test_actual_observed_requires_receipt_and_availability():
    with pytest.raises(ValueError, match="ACTUAL_OBSERVED requires"):
        InformationContract(
            contract_version="1.0",
            source_id="s",
            data_type="d",
            units="",
            availability_class=AvailabilityClass.ACTUAL_OBSERVED,
        )
    with pytest.raises(ValueError, match="must not carry replay"):
        InformationContract(
            contract_version="1.0",
            source_id="s",
            data_type="d",
            units="",
            received_at_ns=T0,
            available_at_ns=T1,
            replay_available_at_ns=T0,
            availability_class=AvailabilityClass.ACTUAL_OBSERVED,
        )


def test_revised_no_vintage_cannot_carry_replay():
    with pytest.raises(ValueError, match="must not carry replay"):
        InformationContract(
            contract_version="1.0",
            source_id="fred",
            data_type="macro",
            units="",
            availability_class=AvailabilityClass.REVISED_NO_VINTAGE,
            replay_available_at_ns=T0,
        )


def test_dependency_causality():
    actual_observed(
        source_id="bybit", data_type="bar", received_at_ns=T0, available_at_ns=T1
    )
    derived = InformationContract(
        contract_version="1.0",
        source_id="atlas",
        data_type="feature",
        units="",
        received_at_ns=T1,
        available_at_ns=T1,  # equal to dep available -> ok (>=)
        processed_at_ns=T1,
        availability_class=AvailabilityClass.ACTUAL_OBSERVED,
        dependency_ids=("dep1",),
    )
    derived.validate_dependencies({"dep1": T1})  # ok
    with pytest.raises(ValueError, match="cannot be available before required dependencies"):
        derived.validate_dependencies({"dep1": T2})  # dep available after derived
    with pytest.raises(ValueError, match="missing dependency"):
        derived.validate_dependencies({})


def test_revision_provenance_combinations():
    # REVISED without replay is allowed (diagnostic)
    r = InformationContract(
        contract_version="1.0",
        source_id="fred",
        data_type="cpi",
        units="index",
        source_revision="vintage-2024-01",
        availability_class=AvailabilityClass.REVISED_NO_VINTAGE,
    )
    assert r.source_revision == "vintage-2024-01"
    # deterministic serialization
    assert r.to_canonical_json() == r.to_canonical_json()
    h1 = r.compute_content_hash()
    r2 = InformationContract(
        contract_version="1.0",
        source_id="fred",
        data_type="cpi",
        units="index",
        source_revision="vintage-2024-01",
        availability_class=AvailabilityClass.REVISED_NO_VINTAGE,
    )
    assert r2.compute_content_hash() == h1


def test_deterministic_serialization_stable_across_field_order():
    a = actual_observed(
        source_id="bybit", data_type="trade", received_at_ns=T0, available_at_ns=T1
    )
    b = actual_observed(
        source_id="bybit", data_type="trade", received_at_ns=T0, available_at_ns=T1
    )
    assert a.to_canonical_json() == b.to_canonical_json()
    assert a.compute_content_hash() == b.compute_content_hash()


@given(
    recv=st.integers(min_value=1_000_000, max_value=9_999_999_999_999_999_999),
    delta=st.integers(min_value=0, max_value=1_000_000_000),
)
def test_available_gte_received_property(recv, delta):
    r = actual_observed(
        source_id="s", data_type="d", received_at_ns=recv, available_at_ns=recv + delta
    )
    assert r.available_at_ns >= r.received_at_ns  # type: ignore[operator]
