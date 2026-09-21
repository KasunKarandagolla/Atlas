"""Phase 1 prerequisite validators (freeze §1.8 test 1, §9.1, §9.8).

Testnet identity / account contract types: environment must be testnet for
development qualification; venue Bybit; linear; one-way; isolated margin
profile; account identity match; BTCUSDT/ETHUSDT metadata present.

Mismatch => new risk disabled. Never auto-mutate an occupied account's mode.
Private account verification requiring credentials remains TEST GATE, but the
validator + evidence model are fully implemented and fixture-tested.
"""

from __future__ import annotations

from dataclasses import dataclass


def _nonblank(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-blank string")
    return value.strip()


PLACEHOLDER_VALUES = frozenset({"CONFIGURED", "REQUIRED", "REQUIRED_AT_INSTALL", ""})


def _is_placeholder(value: str) -> bool:
    s = value.strip() if isinstance(value, str) else ""
    return not s or s in PLACEHOLDER_VALUES or s.startswith("REQUIRED")


@dataclass(frozen=True)
class IdentityExpectation:
    environment: str = "testnet"
    expected_venue: str = "BYBIT"
    expected_product: str = "linear"
    expected_position_mode: str = "one_way"
    expected_margin_profile: str = "isolated"
    expected_account_identity_hash: str = "CONFIGURED"
    required_instruments: tuple[str, ...] = ("BTCUSDT", "ETHUSDT")

    def __post_init__(self) -> None:
        if _is_placeholder(self.expected_account_identity_hash):
            raise ValueError(
                f"expected_account_identity_hash holds placeholder {self.expected_account_identity_hash!r}; "
                "production readiness requires real evidence"
            )


@dataclass(frozen=True)
class ObservedAccountState:
    """Fixture/test-supplied observation. Production private verification is TEST GATE."""

    environment: str
    venue: str
    product: str
    position_mode: str
    margin_profile: str
    account_identity_hash: str
    instruments_with_metadata: tuple[str, ...]
    private_verified: bool = False  # True only with credentialed venue proof (TEST GATE)

    def __post_init__(self) -> None:
        for f in (
            "environment",
            "venue",
            "product",
            "position_mode",
            "margin_profile",
            "account_identity_hash",
        ):
            _nonblank(getattr(self, f), f"observed.{f}")
        if _is_placeholder(self.account_identity_hash):
            raise ValueError(
                f"observed account_identity_hash holds placeholder {self.account_identity_hash!r}; "
                "production readiness requires real evidence"
            )
        if not isinstance(self.private_verified, bool):
            raise ValueError("private_verified must be bool")
        object.__setattr__(
            self, "instruments_with_metadata", tuple(self.instruments_with_metadata)
        )


@dataclass(frozen=True)
class PrerequisiteResult:
    ok: bool
    reasons: tuple[str, ...]
    private_verification: str = "UNVERIFIED_TEST_GATE"

    def __bool__(self) -> bool:
        return self.ok


def check_identity(
    expected: IdentityExpectation, observed: ObservedAccountState
) -> PrerequisiteResult:
    reasons: list[str] = []
    if observed.environment != expected.environment:
        reasons.append(f"environment {observed.environment!r} != {expected.environment!r}")
    if observed.venue != expected.expected_venue:
        reasons.append(f"venue {observed.venue!r} != {expected.expected_venue!r}")
    if observed.product != expected.expected_product:
        reasons.append(f"product {observed.product!r} != {expected.expected_product!r}")
    if observed.position_mode != expected.expected_position_mode:
        reasons.append(
            f"position_mode {observed.position_mode!r} != {expected.expected_position_mode!r}"
        )
    if observed.margin_profile != expected.expected_margin_profile:
        reasons.append(
            f"margin_profile {observed.margin_profile!r} != {expected.expected_margin_profile!r}"
        )
    if observed.account_identity_hash != expected.expected_account_identity_hash:
        reasons.append("account identity hash mismatch")
    missing = [
        s for s in expected.required_instruments if s not in observed.instruments_with_metadata
    ]
    if missing:
        reasons.append(f"missing instrument metadata: {missing}")
    if not observed.private_verified:
        reasons.append("private account verification UNVERIFIED (TEST GATE, no credentials)")
    return PrerequisiteResult(
        ok=not reasons,
        reasons=tuple(reasons),
        private_verification="VERIFIED" if observed.private_verified else "UNVERIFIED_TEST_GATE",
    )
