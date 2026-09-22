"""Account/profile prerequisites; placeholders are representable but never pass."""

from __future__ import annotations

from dataclasses import dataclass

_PLACE = {"", "CONFIGURED", "REQUIRED", "REQUIRED_AT_INSTALL", "PENDING"}


def _ph(v: str) -> bool:
    return (
        not isinstance(v, str)
        or v.strip() in _PLACE
        or v.strip().startswith(("REQUIRED", "PENDING", "UNVERIFIED", "TEST_GATE"))
    )


@dataclass(frozen=True)
class IdentityExpectation:
    environment: str = "testnet"
    expected_venue: str = "BYBIT"
    expected_product: str = "linear"
    expected_position_mode: str = "one_way"
    expected_margin_profile: str = "isolated"
    expected_account_identity_hash: str = "REQUIRED"
    required_instruments: tuple[str, ...] = ("BTCUSDT", "ETHUSDT")


@dataclass(frozen=True)
class ObservedAccountState:
    environment: str
    venue: str
    product: str
    position_mode: str
    margin_profile: str
    account_identity_hash: str
    instruments_with_metadata: tuple[str, ...]
    private_verified: bool = False


@dataclass(frozen=True)
class PrerequisiteResult:
    ok: bool
    reasons: tuple[str, ...]


def check_identity(e: IdentityExpectation, o: ObservedAccountState) -> PrerequisiteResult:
    r = []
    for actual, expected, n in (
        (o.environment, e.environment, "environment"),
        (o.venue, e.expected_venue, "venue"),
        (o.product, e.expected_product, "product"),
        (o.position_mode, e.expected_position_mode, "position_mode"),
        (o.margin_profile, e.expected_margin_profile, "margin_profile"),
    ):
        if actual != expected:
            r.append(f"{n} mismatch")
    if _ph(e.expected_account_identity_hash) or _ph(o.account_identity_hash):
        r.append("account identity placeholder")
    elif o.account_identity_hash != e.expected_account_identity_hash:
        r.append("account identity mismatch")
    if any(x not in o.instruments_with_metadata for x in e.required_instruments):
        r.append("required instrument metadata missing")
    if not o.private_verified:
        r.append("private account verification UNVERIFIED")
    return PrerequisiteResult(not r, tuple(r))
