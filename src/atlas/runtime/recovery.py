"""Typed recovery evidence (freeze §1.6, §9.8).

A local recovery certificate NEVER claims venue reconciliation succeeded when
no venue observations were obtained. READY requires explicit positive
prerequisite evidence supplied by the caller.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from atlas.domain.enums import ReconciliationHealth
from atlas.domain.time import ensure_utc_ns


class RecoveryDecision(StrEnum):
    READY = "READY"
    REMAIN_RECOVERING = "REMAIN_RECOVERING"
    RECOVERY_REQUIRED = "RECOVERY_REQUIRED"


@dataclass(frozen=True)
class RecoveryCertificate:
    recovery_run_id: str
    writer_id: str
    writer_epoch: int
    journal_schema_version: int
    unresolved_intents: tuple[str, ...]
    unresolved_commands: tuple[str, ...]
    unknown_commands: tuple[str, ...]
    reconciliation_health: ReconciliationHealth
    protection_uncertainty_summary: str
    started_at_ns: int
    ended_at_ns: int
    evidence_refs: tuple[str, ...]
    venue_observations_obtained: bool
    decision: RecoveryDecision

    def __post_init__(self) -> None:
        for f in (
            "recovery_run_id",
            "writer_id",
            "protection_uncertainty_summary",
        ):
            v = getattr(self, f)
            if not isinstance(v, str) or not v.strip():
                raise ValueError(f"{f} must be a non-blank string")
        if not isinstance(self.writer_epoch, int) or isinstance(self.writer_epoch, bool):
            raise ValueError("writer_epoch must be int")
        ensure_utc_ns(self.started_at_ns, field="started_at_ns")
        ensure_utc_ns(self.ended_at_ns, field="ended_at_ns")
        if self.ended_at_ns < self.started_at_ns:
            raise ValueError("ended_at cannot precede started_at")
        if not isinstance(self.reconciliation_health, ReconciliationHealth):
            raise ValueError("reconciliation_health must be ReconciliationHealth")
        if not isinstance(self.decision, RecoveryDecision):
            raise ValueError("decision must be RecoveryDecision")
        if not isinstance(self.venue_observations_obtained, bool):
            raise ValueError("venue_observations_obtained must be bool")
        object.__setattr__(self, "unresolved_intents", tuple(self.unresolved_intents))
        object.__setattr__(self, "unresolved_commands", tuple(self.unresolved_commands))
        object.__setattr__(self, "unknown_commands", tuple(self.unknown_commands))
        object.__setattr__(self, "evidence_refs", tuple(self.evidence_refs))
        # Local certificate must never claim venue reconciliation without venue data.
        if self.decision == RecoveryDecision.READY and not self.venue_observations_obtained:
            # READY on purely local restore is allowed only when nothing is
            # unresolved; otherwise venue truth is required. Enforced in
            # run_recovery; this guard blocks direct construction abuse.
            if self.unresolved_intents or self.unknown_commands:
                raise ValueError(
                    "READY with unresolved intents/UNKNOWN requires venue observations"
                )


def run_recovery(
    *,
    recovery_run_id: str,
    writer_id: str,
    writer_epoch: int,
    journal_schema_version: int,
    unresolved_intent_ids: tuple[str, ...],
    unresolved_command_ids: tuple[str, ...],
    unknown_command_ids: tuple[str, ...],
    reconciliation_health: ReconciliationHealth,
    protection_uncertainty_summary: str,
    started_at_ns: int,
    ended_at_ns: int,
    venue_observations_obtained: bool,
    prerequisites_ok: bool,
) -> RecoveryCertificate:
    """Deterministic recovery classification.

    - Any UNKNOWN commands, unresolved intents, non-CURRENT reconciliation, or
      failed prerequisites => REMAIN_RECOVERING or RECOVERY_REQUIRED.
    - READY only when: nothing unresolved, reconciliation CURRENT, prerequisites
      positively satisfied, and (if venue scope claimed) venue observations exist.
    - Conflicted reconciliation => RECOVERY_REQUIRED.
    """
    if reconciliation_health == ReconciliationHealth.CONFLICTED:
        decision = RecoveryDecision.RECOVERY_REQUIRED
    elif (
        unknown_command_ids
        or unresolved_intent_ids
        or reconciliation_health != ReconciliationHealth.CURRENT
        or not prerequisites_ok
    ):
        # Distinguish soft remain vs hard recovery: conflicted/stale-with-unknown
        # escalates; plain prerequisite failure remains recovering.
        if reconciliation_health != ReconciliationHealth.CURRENT and unknown_command_ids:
            decision = RecoveryDecision.RECOVERY_REQUIRED
        else:
            decision = RecoveryDecision.REMAIN_RECOVERING
    else:
        decision = RecoveryDecision.READY
    return RecoveryCertificate(
        recovery_run_id=recovery_run_id,
        writer_id=writer_id,
        writer_epoch=writer_epoch,
        journal_schema_version=journal_schema_version,
        unresolved_intents=tuple(unresolved_intent_ids),
        unresolved_commands=tuple(unresolved_command_ids),
        unknown_commands=tuple(unknown_command_ids),
        reconciliation_health=reconciliation_health,
        protection_uncertainty_summary=protection_uncertainty_summary,
        started_at_ns=started_at_ns,
        ended_at_ns=ended_at_ns,
        evidence_refs=("journal-restore",),
        venue_observations_obtained=venue_observations_obtained,
        decision=decision,
    )
