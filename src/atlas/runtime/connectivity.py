"""Fail-closed public/private connectivity health models."""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from atlas.domain.time import ensure_utc_ns


class PublicState(StrEnum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    SYNCHRONIZED = "SYNCHRONIZED"
    CURRENT = "SYNCHRONIZED"
    CONNECTED = "CONNECTED"
    STALE = "STALE"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"


class PrivateState(StrEnum):
    UNVERIFIED = "UNVERIFIED"
    TEST_GATE = "TEST_GATE"
    VERIFIED = "VERIFIED"


@dataclass(frozen=True, init=False)
class PublicVenueHealth:
    """Public market-data freshness with source/receipt clock checks."""

    state: PublicState
    received_at_ns: int | None
    source_time_ns: int | None
    last_heartbeat_ns: int | None
    evidence_ref: str

    def __init__(self, state: PublicState = PublicState.DISCONNECTED,
                 received_at_ns: int | None = None,
                 source_time_ns: int | None = None,
                 last_heartbeat_ns: int | None = None,
                 evidence_ref: str = "", *,
                 last_source_time_ns: int | None = None,
                 last_receive_time_ns: int | None = None,
                 heartbeat_at_ns: int | None = None) -> None:
        if last_source_time_ns is not None:
            source_time_ns = last_source_time_ns
        if last_receive_time_ns is not None:
            received_at_ns = last_receive_time_ns
        if heartbeat_at_ns is not None:
            last_heartbeat_ns = heartbeat_at_ns
        object.__setattr__(self, "state", state)
        object.__setattr__(self, "received_at_ns", received_at_ns)
        object.__setattr__(self, "source_time_ns", source_time_ns)
        object.__setattr__(self, "last_heartbeat_ns", last_heartbeat_ns)
        object.__setattr__(self, "evidence_ref", evidence_ref)
        self.__post_init__()

    def __post_init__(self) -> None:
        if not isinstance(self.state, PublicState):
            raise ValueError("state must be PublicState")
        for field in ("received_at_ns", "source_time_ns", "last_heartbeat_ns"):
            value = getattr(self, field)
            if value is not None:
                ensure_utc_ns(value, field=field)

    @property
    def last_source_time_ns(self) -> int | None:
        return self.source_time_ns

    @property
    def last_receive_time_ns(self) -> int | None:
        return self.received_at_ns

    @property
    def heartbeat_at_ns(self) -> int | None:
        return self.last_heartbeat_ns

    def staleness_ns(self, now_ns: int) -> int | None:
        ensure_utc_ns(now_ns, field="now_ns")
        if self.last_heartbeat_ns is None:
            return None
        diff = now_ns - self.last_heartbeat_ns
        return -1 if diff < 0 else diff

    def source_staleness_ns(self, now_ns: int) -> int | None:
        ensure_utc_ns(now_ns, field="now_ns")
        if self.source_time_ns is None:
            return None
        diff = now_ns - self.source_time_ns
        return -1 if diff < 0 else diff

    def receipt_after_source_ns(self) -> int | None:
        if self.received_at_ns is None or self.source_time_ns is None:
            return None
        diff = self.received_at_ns - self.source_time_ns
        return -1 if diff < 0 else diff

    def is_fresh(self, *, now_ns: int, max_staleness_ns: int,
                 clock_uncertainty_ns: int = 0) -> bool:
        ensure_utc_ns(now_ns, field="now_ns")
        ensure_utc_ns(max_staleness_ns, field="max_staleness_ns")
        ensure_utc_ns(clock_uncertainty_ns, field="clock_uncertainty_ns")
        if self.state not in (PublicState.SYNCHRONIZED, PublicState.CONNECTED):
            return False
        if self.last_heartbeat_ns is None or self.received_at_ns is None:
            return False
        for value in (self.last_heartbeat_ns, self.received_at_ns, self.source_time_ns):
            if value is not None and value > now_ns + clock_uncertainty_ns:
                return False
        if now_ns - self.last_heartbeat_ns > max_staleness_ns:
            return False
        if self.source_time_ns is not None and now_ns - self.source_time_ns > max_staleness_ns:
            return False
        return not (self.source_time_ns is not None and self.received_at_ns < self.source_time_ns - clock_uncertainty_ns)


@dataclass(frozen=True, init=False)
class PrivateVerification:
    state: PrivateState = PrivateState.UNVERIFIED
    evidence_refs: tuple[str, ...] = ()
    verified_at_ns: int | None = None
    environment: str | None = None
    account_identity_hash: str | None = None

    def __init__(self, state: PrivateState = PrivateState.UNVERIFIED,
                 evidence_refs: tuple[str, ...] = (), verified_at_ns: int | None = None,
                 environment: str | None = None, account_identity_hash: str | None = None,
                 *, evidence_ref: str | None = None, verification_timestamp_ns: int | None = None,
                 venue_account_identity_evidence: str | None = None) -> None:
        if evidence_ref is not None and not evidence_refs:
            evidence_refs=(evidence_ref,)
        if verification_timestamp_ns is not None and verified_at_ns is None:
            verified_at_ns=verification_timestamp_ns
        if account_identity_hash is None and venue_account_identity_evidence:
            account_identity_hash=venue_account_identity_evidence
        object.__setattr__(self,"state",state);object.__setattr__(self,"evidence_refs",tuple(evidence_refs));object.__setattr__(self,"verified_at_ns",verified_at_ns);object.__setattr__(self,"environment",environment);object.__setattr__(self,"account_identity_hash",account_identity_hash)
        self.__post_init__()

    def __post_init__(self)->None:
        if not isinstance(self.state,PrivateState): raise ValueError('state must be PrivateState')
        if self.state==PrivateState.VERIFIED:
            if not self.evidence_refs or self.verified_at_ns is None or not self.environment:
                raise ValueError('VERIFIED state requires complete evidence')

    @property
    def evidence_ref(self) -> str:
        return self.evidence_refs[0] if self.evidence_refs else ""

    @property
    def verification_timestamp_ns(self) -> int | None:
        return self.verified_at_ns

    @property
    def venue_account_identity_evidence(self) -> str:
        return self.account_identity_hash or ""

    @property
    def verified(self) -> bool:
        identity=self.account_identity_hash
        return (self.state == PrivateState.VERIFIED and bool(self.evidence_refs)
                and self.verified_at_ns is not None and self.environment == "testnet"
                and bool(identity)
                and not identity.startswith(("REQUIRED", "UNVERIFIED", "TEST_GATE")) if identity else False)


@dataclass(frozen=True)
class PrivateConfig:
    environment: str
    venue: str = "BYBIT"
    credentials_present: bool = False

    def __post_init__(self) -> None:
        if not self.environment.strip(): raise ValueError("environment must be non-blank")
        if not isinstance(self.credentials_present,bool): raise ValueError("credentials_present must be bool")
