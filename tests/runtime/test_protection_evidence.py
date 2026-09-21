from __future__ import annotations

from decimal import Decimal

import pytest

from atlas.domain.execution import ProtectionObservation
from atlas.runtime.protection_evidence import verify_protection

T0 = 1_700_000_000_000_000_000


def _observation(*, qty: str = "0.010", at: int = T0, stop: str = "48000", trigger: str = "MarkPrice", refs: tuple[str, ...] = ("position-1",)) -> ProtectionObservation:
    return ProtectionObservation(
        position_epoch=3, desired_stop_version=7, qty=Decimal(qty), trigger_basis=trigger,
        stop_price=Decimal(stop), semantics="Full Market ReduceOnly", evidence_ids=refs, observed_at_ns=at,
    )


def test_protection_identity_and_exact_current_signed_coverage_are_preserved():
    result = verify_protection(
        _observation(), 3, Decimal("0.010"), Decimal("48000"), "MarkPrice", T0,
        2_000_000_000, account_ref="acct-hash", instrument="BTCUSDT",
        conditional_order_evidence_ids=("conditional-1",), conditional_order_view_available=True,
    )
    assert result.verified
    assert result.evidence.account_ref == "acct-hash"
    assert result.evidence.instrument == "BTCUSDT"
    assert result.evidence.observed_signed_qty == Decimal("0.010")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"qty": "0.009"}, {"qty": "-0.010"}, {"stop": "47000"},
        {"trigger": "LastPrice"}, {"refs": ("REQUIRED",)}, {"at": T0 + 3_000_000_000},
    ],
)
def test_stale_wrong_or_incomplete_protection_is_fail_closed(kwargs):
    result = verify_protection(
        _observation(**kwargs), 3, Decimal("0.010"), Decimal("48000"), "MarkPrice", T0,
        2_000_000_000, account_ref="acct-hash", instrument="BTCUSDT",
    )
    assert not result.verified
    assert result.evidence.account_ref == "acct-hash"
