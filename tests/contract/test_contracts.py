"""Contract tests: deterministic serialization across domain contracts."""

from __future__ import annotations

from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from atlas.domain.money import canonical_decimal_str


@given(st.decimals(allow_nan=False, allow_infinity=False, min_value=-1_000_000, max_value=1_000_000))
def test_canonical_decimal_deterministic(value):
    a = canonical_decimal_str(value)
    b = canonical_decimal_str(Decimal(a))
    assert a == b


def test_tradeplan_risk_capability_json_sorted():
    from atlas.domain.capability import initial_unverified_fixture
    from atlas.domain.risk import engineering_default_policy

    c = initial_unverified_fixture()
    p = engineering_default_policy(policy_effective_at_ns=123)
    # sorted keys + compact separators => deterministic
    assert '"account_identity_hash"' in c.to_canonical_json()
    assert c.contract_hash() == initial_unverified_fixture().contract_hash()
    assert p.policy_hash() == engineering_default_policy(policy_effective_at_ns=123).policy_hash()
