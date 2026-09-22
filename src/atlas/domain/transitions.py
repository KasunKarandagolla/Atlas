"""Frozen lifecycle + command-outcome transition validators."""

from __future__ import annotations

from atlas.domain.enums import CommandOutcome as _C
from atlas.domain.enums import LifecycleState as _L

ALLOWED_LIFECYCLE_TRANSITIONS = {
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
            _L.RECOVERY_REQUIRED,
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
            _L.RECOVERY_REQUIRED,
        }
    ),
    _L.PARTIALLY_FILLED: frozenset(
        {
            _L.OPEN_UNPROTECTED,
            _L.OPEN_PROTECTED,
            _L.CANCEL_PENDING,
            _L.EXIT_PENDING,
            _L.FLAT_PENDING_RECONCILIATION,
            _L.RECOVERY_REQUIRED,
        }
    ),
    _L.OPEN_UNPROTECTED: frozenset({_L.OPEN_PROTECTED, _L.EXIT_PENDING, _L.RECOVERY_REQUIRED}),
    _L.OPEN_PROTECTED: frozenset(
        {_L.EXIT_PENDING, _L.OPEN_UNPROTECTED, _L.FLAT_PENDING_RECONCILIATION, _L.RECOVERY_REQUIRED}
    ),
    _L.CANCEL_PENDING: frozenset(
        {
            _L.ENTRY_WORKING,
            _L.PARTIALLY_FILLED,
            _L.OPEN_UNPROTECTED,
            _L.OPEN_PROTECTED,
            _L.EXIT_PENDING,
            _L.FLAT_PENDING_RECONCILIATION,
            _L.RECOVERY_REQUIRED,
        }
    ),
    _L.EXIT_PENDING: frozenset(
        {_L.OPEN_UNPROTECTED, _L.OPEN_PROTECTED, _L.FLAT_PENDING_RECONCILIATION, _L.RECOVERY_REQUIRED}
    ),
    _L.FLAT_PENDING_RECONCILIATION: frozenset(
        {_L.CLOSED, _L.OPEN_UNPROTECTED, _L.OPEN_PROTECTED, _L.RECOVERY_REQUIRED}
    ),
    _L.CLOSED: frozenset({_L.RECOVERY_REQUIRED}),
    _L.RECOVERY_REQUIRED: frozenset(set(_L)),
}


def is_allowed_lifecycle_transition(frm: _L, to: _L) -> bool:
    if not isinstance(frm, _L) or not isinstance(to, _L):
        raise ValueError("lifecycle transition requires LifecycleState values")
    return frm != to and to in ALLOWED_LIFECYCLE_TRANSITIONS[frm]


def validate_lifecycle_transition(frm: _L, to: _L) -> None:
    if not is_allowed_lifecycle_transition(frm, to):
        raise ValueError(f"illegal lifecycle transition {frm.value} -> {to.value}")


ALLOWED_OUTCOME_TRANSITIONS = {
    _C.UNSENT: frozenset({_C.UNKNOWN, _C.DEFINITE_REJECT}),
    _C.UNKNOWN: frozenset({_C.DEFINITE_ACCEPT, _C.DEFINITE_REJECT, _C.RECONCILED}),
    _C.DEFINITE_ACCEPT: frozenset({_C.RECONCILED}),
    _C.DEFINITE_REJECT: frozenset({_C.RECONCILED}),
    _C.RECONCILED: frozenset(),
}


def is_allowed_outcome_transition(frm: _C, to: _C) -> bool:
    if not isinstance(frm, _C) or not isinstance(to, _C):
        raise ValueError("outcome transition requires CommandOutcome values")
    return frm == to or to in ALLOWED_OUTCOME_TRANSITIONS[frm]


def validate_outcome_transition(frm: _C, to: _C) -> None:
    if not is_allowed_outcome_transition(frm, to):
        raise ValueError(f"illegal command outcome transition {frm.value} -> {to.value}")
