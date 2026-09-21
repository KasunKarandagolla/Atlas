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

from .reconciliation_evidence import Completeness, QueryStatus, ReconciliationEvidenceBundle


class RecoveryDecision(StrEnum):
    READY = "READY"
    REMAIN_RECOVERING = "REMAIN_RECOVERING"
    RECOVERY_REQUIRED = "RECOVERY_REQUIRED"


@dataclass(frozen=True)
class RecoveryIncident:
    """Append-only record for an uncertainty/recovery episode."""

    incident_id: str
    recovery_run_id: str
    category: str
    status: str
    evidence_refs: tuple[str, ...]
    opened_at_ns: int
    resolved_at_ns: int | None = None

    def __post_init__(self) -> None:
        for name in ("incident_id", "recovery_run_id", "category", "status"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be non-blank")
        object.__setattr__(self, "evidence_refs", tuple(self.evidence_refs))
        if not self.evidence_refs or any(
            not isinstance(ref, str)
            or not ref.strip()
            or ref.strip().upper() in {"REQUIRED", "REQUIRED_AT_INSTALL", "PLACEHOLDER"}
            for ref in self.evidence_refs
        ):
            raise ValueError("incident evidence_refs must be non-empty")
        ensure_utc_ns(self.opened_at_ns, field="opened_at_ns")
        if self.resolved_at_ns is not None:
            ensure_utc_ns(self.resolved_at_ns, field="resolved_at_ns")
            if self.resolved_at_ns < self.opened_at_ns:
                raise ValueError("resolved_at cannot precede opened_at")


@dataclass(frozen=True)
class RecoveryProtectionEvidence:
    """Positive protection prerequisite for a READY recovery certificate."""

    certified_flat: bool
    current_protection: bool
    evidence_refs: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.certified_flat, bool) or not isinstance(self.current_protection, bool):
            raise ValueError("recovery protection flags must be bool")
        object.__setattr__(self, "evidence_refs", tuple(self.evidence_refs))
        if not (self.certified_flat or self.current_protection):
            raise ValueError("recovery requires certified flatness or current protection")
        if not self.evidence_refs or any(not isinstance(ref, str) or not ref.strip() for ref in self.evidence_refs):
            raise ValueError("recovery protection evidence_refs must be non-empty")


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
    venue_evidence_refs: tuple[str, ...]
    decision: RecoveryDecision
    protection_evidence: RecoveryProtectionEvidence | None = None

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
        object.__setattr__(self, "venue_evidence_refs", tuple(self.venue_evidence_refs))
        # Local certificate must never claim venue reconciliation without venue data.
        if self.decision == RecoveryDecision.READY:
            if not self.venue_observations_obtained:
                raise ValueError("READY requires venue_observations_obtained=True")
            if not self.venue_evidence_refs:
                raise ValueError("READY requires non-empty venue_evidence_refs")
            if self.unresolved_intents or self.unresolved_commands or self.unknown_commands:
                raise ValueError(
                    "READY with unresolved intents/commands/UNKNOWN requires venue evidence"
                )
            if self.reconciliation_health != ReconciliationHealth.CURRENT:
                raise ValueError("READY requires reconciliation_health=CURRENT")
            if self.protection_evidence is None:
                raise ValueError("READY requires certified flatness or current protection evidence")
            if not (self.protection_evidence.certified_flat or self.protection_evidence.current_protection):
                raise ValueError("READY requires positive flat/protection evidence")


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
    venue_evidence_refs: tuple[str, ...],
    prerequisites_ok: bool,
    protection_evidence: RecoveryProtectionEvidence | None = None,
    reconciliation_evidence: ReconciliationEvidenceBundle | None = None,
) -> RecoveryCertificate:
    """Deterministic recovery classification.

    - Any unresolved commands (UNSENT, UNKNOWN, DEFINITE_ACCEPT), unresolved intents,
      non-CURRENT reconciliation, or failed prerequisites => REMAIN_RECOVERING or RECOVERY_REQUIRED.
    - READY only when: nothing unresolved, reconciliation CURRENT, prerequisites
      positively satisfied, and venue observations with evidence refs exist.
    - Conflicted reconciliation => RECOVERY_REQUIRED.
    """
    evidence_refs = tuple(
        query.evidence_hash for query in reconciliation_evidence.queries
    ) if reconciliation_evidence is not None else ()
    if reconciliation_evidence is not None:
        if reconciliation_evidence.overall_status != QueryStatus.SUCCESS:
            reconciliation_health = ReconciliationHealth.STALE
        if reconciliation_evidence.overall_completeness != Completeness.COMPLETE:
            reconciliation_health = ReconciliationHealth.STALE
        if not venue_evidence_refs:
            venue_evidence_refs = evidence_refs
        venue_observations_obtained = venue_observations_obtained or bool(reconciliation_evidence.queries)

    has_unresolved = (
        unresolved_intent_ids
        or unresolved_command_ids
        or unknown_command_ids
    )
    if reconciliation_health == ReconciliationHealth.CONFLICTED:
        decision = RecoveryDecision.RECOVERY_REQUIRED
    elif has_unresolved or reconciliation_health != ReconciliationHealth.CURRENT or not prerequisites_ok:
        # Distinguish soft remain vs hard recovery: conflicted/stale-with-unknown
        # escalates; plain prerequisite failure remains recovering.
        if reconciliation_health != ReconciliationHealth.CURRENT and (unknown_command_ids or unresolved_command_ids):
            decision = RecoveryDecision.RECOVERY_REQUIRED
        else:
            decision = RecoveryDecision.REMAIN_RECOVERING
    elif protection_evidence is None or not (
        protection_evidence.certified_flat or protection_evidence.current_protection
    ):
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
        evidence_refs=("journal-restore",) + evidence_refs,
        venue_observations_obtained=venue_observations_obtained,
        venue_evidence_refs=tuple(venue_evidence_refs),
        decision=decision,
        protection_evidence=protection_evidence,
    )
