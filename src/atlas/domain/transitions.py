"""Frozen lifecycle + command-outcome transition validators (freeze §1.5, §1.4).

Pure functions: no I/O, no inference. Protection and reconciliation remain
independent dimensions; only lifecycle is constrained here.
"""

from __future__ import annotations

from atlas.domain.enums import CommandOutcome, LifecycleState

_L = LifecycleState
_C = CommandOutcome

# Frozen §1.5 allowed next states (ordinary transitions).
# RECOVERY_REQUIRED reachable where freeze lists it; unexpected/contradictory
# evidence may route to RECOVERY_REQUIRED from states that list it.
ALLOWED_LIFECYCLE_TRANSITIONS: dict[_L, frozenset[_L]] = {
    _L.PLAN_APPROVED: frozenset({_L.INTENT_PERSISTED, _L.CLOSED}),
    _L.INTENT_PERSISTED: frozenset({_L.SUBMITTING, _L.CLOSED}),
    _L.SUBMITTING: frozenset(
        {
            _L.ENTRY_WORKING,
            _L.PARTIALLY_FILLED,
            _L.OPEN_UNPROTECTED,
            _L.OPEN_PROTECTED,
            _L.SUBMIT_UNKNOWN,
            _L.FLAT_PENDING_RECONCILIATION,
        }
    ),
    _L.SUBMIT_UNKNOWN: frozenset(
        {
            _L.ENTRY_WORKING,
            _L.PARTIALLY_FILLED,
            _L.OPEN_UNPROTECTED,
            _L.OPEN_PROTECTED,
            _L.FLAT_PENDING_RECONCILIATION,
            _L.RECOVERY_REQUIRED,
        }
    ),
    _L.ENTRY_WORKING: frozenset(
        {
            _L.PARTIALLY_FILLED,
            _L.OPEN_UNPROTECTED,
            _L.OPEN_PROTECTED,
            _L.CANCEL_PENDING,
            _L.FLAT_PENDING_RECONCILIATION,
        }
    ),
    _L.PARTIALLY_FILLED: frozenset(
        {
            _L.OPEN_UNPROTECTED,
            _L.OPEN_PROTECTED,
            _L.CANCEL_PENDING,
            _L.EXIT_PENDING,
            _L.FLAT_PENDING_RECONCILIATION,
        }
    ),
    _L.OPEN_UNPROTECTED: frozenset({_L.OPEN_PROTECTED, _L.EXIT_PENDING, _L.RECOVERY_REQUIRED}),
    _L.OPEN_PROTECTED: frozenset(
        {
            _L.EXIT_PENDING,
            _L.OPEN_UNPROTECTED,
            _L.FLAT_PENDING_RECONCILIATION,
            _L.RECOVERY_REQUIRED,
        }
    ),
    _L.CANCEL_PENDING: frozenset(
        {
            _L.ENTRY_WORKING,
            _L.PARTIALLY_FILLED,
            _L.OPEN_UNPROTECTED,
            _L.OPEN_PROTECTED,
            _L.EXIT_PENDING,
            _L.FLAT_PENDING_RECONCILIATION,
        }
    ),
    _L.EXIT_PENDING: frozenset(
        {
            _L.OPEN_UNPROTECTED,
            _L.OPEN_PROTECTED,
            _L.FLAT_PENDING_RECONCILIATION,
            _L.RECOVERY_REQUIRED,
        }
    ),
    _L.FLAT_PENDING_RECONCILIATION: frozenset(
        {
            _L.CLOSED,
            _L.OPEN_UNPROTECTED,
            _L.OPEN_PROTECTED,
            _L.RECOVERY_REQUIRED,
        }
    ),
    # CLOSED is terminal for the epoch; only contrary evidence reopens recovery.
    _L.CLOSED: frozenset({_L.RECOVERY_REQUIRED}),
    # Recovery may resolve to any fact-supported state (validated with evidence
    # at the call site); the pure graph permits all as candidates.
    _L.RECOVERY_REQUIRED: frozenset(set(_L)),
}


def is_allowed_lifecycle_transition(frm: _L, to: _L) -> bool:
    if not isinstance(frm, _L) or not isinstance(to, _L):
        raise ValueError("lifecycle transition requires LifecycleState values")
    if frm == to:
        return False  # no-op is not a transition; use version-preserving no-op path instead
    return to in ALLOWED_LIFECYCLE_TRANSITIONS[frm]


def validate_lifecycle_transition(frm: _L, to: _L) -> None:
    if not is_allowed_lifecycle_transition(frm, to):
        raise ValueError(f"illegal lifecycle transition {frm.value} -> {to.value}")


# Command outcome transitions (freeze §1.4: UNKNOWN uncertainty, no silent regress).
ALLOWED_OUTCOME_TRANSITIONS: dict[_C, frozenset[_C]] = {
    _C.UNSENT: frozenset({_C.UNKNOWN, _C.DEFINITE_REJECT}),
    _C.UNKNOWN: frozenset({_C.DEFINITE_ACCEPT, _C.DEFINITE_REJECT, _C.RECONCILED}),
    _C.DEFINITE_ACCEPT: frozenset({_C.RECONCILED}),
    _C.DEFINITE_REJECT: frozenset({_C.RECONCILED}),
    _C.RECONCILED: frozenset(),
}


def is_allowed_outcome_transition(frm: _C, to: _C) -> bool:
    if not isinstance(frm, _C) or not isinstance(to, _C):
        raise ValueError("outcome transition requires CommandOutcome values")
    if frm == to:
        return True  # idempotent no-op permitted
    return to in ALLOWED_OUTCOME_TRANSITIONS[frm]


def validate_outcome_transition(frm: _C, to: _C) -> None:
    if not is_allowed_outcome_transition(frm, to):
        raise ValueError(f"illegal command outcome transition {frm.value} -> {to.value}")
