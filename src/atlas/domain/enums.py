"""Frozen string enums for ATLAS V1 (freeze §1.4, §1.5).

All enums are explicit strings. Unknown must never imply supported/passed.
"""

from __future__ import annotations

from enum import Enum


class StrEnum(str, Enum):
    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.value


class LifecycleState(StrEnum):
    PLAN_APPROVED = "PLAN_APPROVED"
    INTENT_PERSISTED = "INTENT_PERSISTED"
    SUBMITTING = "SUBMITTING"
    SUBMIT_UNKNOWN = "SUBMIT_UNKNOWN"
    ENTRY_WORKING = "ENTRY_WORKING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    OPEN_UNPROTECTED = "OPEN_UNPROTECTED"
    OPEN_PROTECTED = "OPEN_PROTECTED"
    CANCEL_PENDING = "CANCEL_PENDING"
    EXIT_PENDING = "EXIT_PENDING"
    FLAT_PENDING_RECONCILIATION = "FLAT_PENDING_RECONCILIATION"
    CLOSED = "CLOSED"
    RECOVERY_REQUIRED = "RECOVERY_REQUIRED"


class ProtectionStatus(StrEnum):
    NONE = "NONE"
    UNCONFIRMED = "UNCONFIRMED"
    CONFIRMED = "CONFIRMED"
    BREACHED = "BREACHED"


class ReconciliationHealth(StrEnum):
    CURRENT = "CURRENT"
    STALE = "STALE"
    CONFLICTED = "CONFLICTED"


class CommandOutcome(StrEnum):
    UNSENT = "UNSENT"
    UNKNOWN = "UNKNOWN"
    DEFINITE_ACCEPT = "DEFINITE_ACCEPT"
    DEFINITE_REJECT = "DEFINITE_REJECT"
    RECONCILED = "RECONCILED"


class AvailabilityClass(StrEnum):
    ACTUAL_OBSERVED = "ACTUAL_OBSERVED"
    RECONSTRUCTED_PUBLIC = "RECONSTRUCTED_PUBLIC"
    UNKNOWN = "UNKNOWN"
    REVISED_NO_VINTAGE = "REVISED_NO_VINTAGE"


class CapabilityStatus(StrEnum):
    UNVERIFIED = "UNVERIFIED"
    SUPPORTED = "SUPPORTED"
    UNSUPPORTED = "UNSUPPORTED"
    FAILED = "FAILED"


class Environment(StrEnum):
    DEVELOPMENT = "development"
    TESTNET = "testnet"
    LIVE = "live"


class HealthState(StrEnum):
    BOOT = "BOOT"
    RECOVERING = "RECOVERING"
    READY = "READY"
    DEGRADED = "DEGRADED"
    ENTRY_HALTED = "ENTRY_HALTED"
    PROTECTION_UNCERTAIN = "PROTECTION_UNCERTAIN"
    EMERGENCY_EXIT = "EMERGENCY_EXIT"


class Side(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class CommandType(StrEnum):
    SUBMIT_ENTRY = "SUBMIT_ENTRY"
    CANCEL_ENTRY = "CANCEL_ENTRY"
    SUBMIT_EXIT = "SUBMIT_EXIT"
    REPAIR_STOP = "REPAIR_STOP"
    FLATTEN = "FLATTEN"
    QUERY_RECONCILE = "QUERY_RECONCILE"
