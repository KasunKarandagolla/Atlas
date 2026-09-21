"""Public/private connectivity health abstractions (read-only, fail closed).

Public health states: DISCONNECTED / CONNECTING / SYNCHRONIZED (=current) /
STALE / DEGRADED / FAILED. Records source/receipt/heartbeat times + staleness;
receipt of a stale exchange timestamp is never fresh.

Private streams: interface + configuration boundary only. Without credentials
nothing is invented; private verification stays UNVERIFIED / TEST GATE and the
runtime remains non-READY. Deterministic fakes live in tests, never production.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from atlas.domain.time import ensure_utc_ns


class PublicState(StrEnum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    SYNCHRONIZED = "SYNCHRONIZED"
    CURRENT = "SYNCHRONIZED"  # alias: current == synchronized
    STALE = "STALE"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"


class PrivateState(StrEnum):
    UNVERIFIED = "UNVERIFIED"
    TEST_GATE = "TEST_GATE"
    VERIFIED = "VERIFIED"


@dataclass(frozen=True)
class PublicVenueHealth:
    state: PublicState
    received_at_ns: int | None = None
    source_time_ns: int | None = None
    last_heartbeat_ns: int | None = None
    evidence_ref: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.state, PublicState):
            raise ValueError("state must be PublicState")
        for f in ("received_at_ns", "source_time_ns", "last_heartbeat_ns"):
            v = getattr(self, f)
            if v is not None:
                ensure_utc_ns(v, field=f)

    def staleness_ns(self, now_ns: int) -> int | None:
        ensure_utc_ns(now_ns, field="now_ns")
        if self.last_heartbeat_ns is None:
            return None
        diff = now_ns - self.last_heartbeat_ns
        if diff < 0:
            # Future heartbeat indicates clock conflict
            return -1
        return diff

    def source_staleness_ns(self, now_ns: int) -> int | None:
        ensure_utc_ns(now_ns, field="now_ns")
        if self.source_time_ns is None:
            return None
        diff = now_ns - self.source_time_ns
        if diff < 0:
            # Future source timestamp indicates clock conflict
            return -1
        return diff

    def receipt_after_source_ns(self) -> int | None:
        if self.received_at_ns is None or self.source_time_ns is None:
            return None
        diff = self.received_at_ns - self.source_time_ns
        if diff < 0:
            # Receipt before source indicates clock conflict
            return -1
        return diff

    def is_fresh(self, *, now_ns: int, max_staleness_ns: int, clock_uncertainty_ns: int = 0) -> bool:
        """Fresh requires SYNCHRONIZED plus recent heartbeat AND recent source.

        A recently received but stale-dated exchange timestamp is NOT fresh.
        Future/clock-conflicting timestamps are NOT fresh (fail closed).
        """
        ensure_utc_ns(now_ns, field="now_ns")
        ensure_utc_ns(max_staleness_ns, field="max_staleness_ns")
        ensure_utc_ns(clock_uncertainty_ns, field="clock_uncertainty_ns")
        if self.state != PublicState.SYNCHRONIZED:
            return False
        if self.last_heartbeat_ns is None or self.received_at_ns is None:
            return False
        # Check for future/clock-conflicting timestamps (fail closed)
        hb_diff = now_ns - self.last_heartbeat_ns
        if hb_diff < -clock_uncertainty_ns:
            # Heartbeat is in the future beyond allowed uncertainty
            return False
        if hb_diff > max_staleness_ns:
            return False
        if self.source_time_ns is not None:
            src_diff = now_ns - self.source_time_ns
            if src_diff < -clock_uncertainty_ns:
                # Source timestamp is in the future beyond allowed uncertainty
                return False
            if src_diff > max_staleness_ns:
                return False
            if self.received_at_ns < self.source_time_ns - clock_uncertainty_ns:
                # Receipt before source beyond allowed uncertainty
                return False
        return True


@dataclass(frozen=True)
class PrivateConfig:
    environment: str
    venue: str = "BYBIT"
    credentials_present: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.environment, str) or not self.environment.strip():
            raise ValueError("environment must be non-blank")
        if not isinstance(self.credentials_present, bool):
            raise ValueError("credentials_present must be bool")


@dataclass(frozen=True)
class PrivateVerification:
    state: PrivateState = PrivateState.UNVERIFIED
    evidence_ref: str = ""
    verification_timestamp_ns: int = 0
    environment: str = ""
    venue_account_identity_evidence: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.state, PrivateState):
            raise ValueError("state must be PrivateState")
        if self.state == PrivateState.VERIFIED:
            if not self.evidence_ref or not self.evidence_ref.strip():
                raise ValueError("VERIFIED state requires non-empty evidence_ref")
            if self.verification_timestamp_ns <= 0:
                raise ValueError("VERIFIED state requires positive verification_timestamp_ns")
            if not self.environment or not self.environment.strip():
                raise ValueError("VERIFIED state requires non-empty environment")
            if not self.venue_account_identity_evidence or not self.venue_account_identity_evidence.strip():
                raise ValueError("VERIFIED state requires venue_account_identity_evidence")
            # No TEST_GATE_* or UNVERIFIED_* markers allowed in VERIFIED evidence
            forbidden_markers = ("TEST_GATE", "UNVERIFIED")
            for marker in forbidden_markers:
                if marker in self.evidence_ref or marker in self.venue_account_identity_evidence:
                    raise ValueError(f"VERIFIED evidence must not contain {marker} marker")
        # UNVERIFIED and TEST_GATE may use placeholder messages
