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
        return max(0, now_ns - self.last_heartbeat_ns)

    def is_fresh(self, *, now_ns: int, max_staleness_ns: int) -> bool:
        """Fresh requires SYNCHRONIZED plus recent heartbeat AND recent source.

        A recently received but stale-dated exchange timestamp is NOT fresh.
        """
        ensure_utc_ns(now_ns, field="now_ns")
        ensure_utc_ns(max_staleness_ns, field="max_staleness_ns")
        if self.state != PublicState.SYNCHRONIZED:
            return False
        if self.last_heartbeat_ns is None or self.received_at_ns is None:
            return False
        if now_ns - self.last_heartbeat_ns > max_staleness_ns:
            return False
        if self.source_time_ns is not None:
            if now_ns - self.source_time_ns > max_staleness_ns:
                return False
            if self.received_at_ns < self.source_time_ns:
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
    evidence_ref: str = "TEST_GATE_NO_CREDENTIALS"

    def __post_init__(self) -> None:
        if not isinstance(self.state, PrivateState):
            raise ValueError("state must be PrivateState")
