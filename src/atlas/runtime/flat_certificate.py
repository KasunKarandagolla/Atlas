"""Flat reconciliation certificate (freeze §1.6, §9.8).

A flat certificate requires evidence for:
- reconciled zero position
- terminal opening command/order certainty
- no remaining opening orders
- no residual conditional/protection orders for epoch
- deduplicated execution coverage
- query completeness
- reservation release eligibility
- current writer/account identity

Do not treat single zero-position observation as CLOSED.
Late contradictory fill/evidence must reopen recovery incident, never silently mutate history.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from atlas.domain.time import ensure_utc_ns
from atlas.runtime.fill_dedup import ExecutionEvidence
from atlas.runtime.reconciliation_evidence import Completeness, QueryStatus, ReconciliationQueryEvidence


class FlatCertificationDecision(StrEnum):
    CERTIFIED_FLAT = "CERTIFIED_FLAT"
    NOT_FLAT = "NOT_FLAT"
    INCONCLUSIVE = "INCONCLUSIVE"


@dataclass(frozen=True)
class FlatCertificate:
    """Certificate proving flat state with complete evidence chain."""

    certification_id: str
    reconciliation_run_id: str
    writer_id: str
    writer_epoch: int
    account_identity_hash: str
    instrument: str
    position_epoch: int

    # Evidence references
    zero_position_evidence: ReconciliationQueryEvidence
    opening_commands_terminal_evidence: tuple[ReconciliationQueryEvidence, ...]
    no_remaining_opening_orders_evidence: ReconciliationQueryEvidence
    no_residual_protection_orders_evidence: ReconciliationQueryEvidence
    execution_dedup_evidence: ExecutionEvidence
    economic_reconciliation_evidence: ReconciliationQueryEvidence

    # Derived state
    decision: FlatCertificationDecision
    certified_at_ns: int
    mismatch_details: tuple[str, ...]
    current_signed_position_qty: Decimal = Decimal("0")
    current_position_evidence_complete: bool = False
    terminal_opening_order_certainty: bool = False
    no_unresolved_opening_command: bool = False
    reservation_release_evidence: bool = False
    writer_identity_current: bool = False
    account_identity_current: bool = False
    required_query_windows_complete: bool = False
    late_contradictory_evidence: bool = False
    intent_id: str | None = None

    def __post_init__(self) -> None:
        for f in ("certification_id", "reconciliation_run_id", "writer_id", "account_identity_hash", "instrument"):
            v = getattr(self, f)
            if not isinstance(v, str) or not v.strip():
                raise ValueError(f"{f} must be non-blank")
        if not isinstance(self.writer_epoch, int) or isinstance(self.writer_epoch, bool) or self.writer_epoch < 0:
            raise ValueError("writer_epoch must be int >= 0")
        if not isinstance(self.position_epoch, int) or isinstance(self.position_epoch, bool) or self.position_epoch < 0:
            raise ValueError("position_epoch must be int >= 0")
        if not isinstance(self.zero_position_evidence, ReconciliationQueryEvidence):
            raise ValueError("zero_position_evidence must be ReconciliationQueryEvidence")
        if not isinstance(self.opening_commands_terminal_evidence, tuple):
            raise ValueError("opening_commands_terminal_evidence must be tuple")
        if not isinstance(self.no_remaining_opening_orders_evidence, ReconciliationQueryEvidence):
            raise ValueError("no_remaining_opening_orders_evidence must be ReconciliationQueryEvidence")
        if not isinstance(self.no_residual_protection_orders_evidence, ReconciliationQueryEvidence):
            raise ValueError("no_residual_protection_orders_evidence must be ReconciliationQueryEvidence")
        if not isinstance(self.execution_dedup_evidence, ExecutionEvidence):
            raise ValueError("execution_dedup_evidence must be ExecutionEvidence")
        if not isinstance(self.economic_reconciliation_evidence, ReconciliationQueryEvidence):
            raise ValueError("economic_reconciliation_evidence must be ReconciliationQueryEvidence")
        if not isinstance(self.decision, FlatCertificationDecision):
            raise ValueError("decision must be FlatCertificationDecision")
        ensure_utc_ns(self.certified_at_ns, field="certified_at_ns")
        if not isinstance(self.mismatch_details, tuple):
            raise ValueError("mismatch_details must be tuple")
        if not isinstance(self.current_signed_position_qty, Decimal):
            raise ValueError("current_signed_position_qty must be Decimal")
        if self.intent_id is not None and not self.intent_id.strip():
            raise ValueError("intent_id must be non-blank when supplied")
        for name in (
            "current_position_evidence_complete", "terminal_opening_order_certainty",
            "no_unresolved_opening_command", "reservation_release_evidence",
            "writer_identity_current", "account_identity_current",
            "required_query_windows_complete", "late_contradictory_evidence",
        ):
            if not isinstance(getattr(self, name), bool):
                raise ValueError(f"{name} must be bool")

    @property
    def is_flat(self) -> bool:
        return self.decision == FlatCertificationDecision.CERTIFIED_FLAT

    @property
    def can_release_reservation(self) -> bool:
        """Reservation can be released only if flat is certified."""
        return self.is_flat


def certify_flat(
    *,
    certification_id: str,
    reconciliation_run_id: str,
    writer_id: str,
    writer_epoch: int,
    account_identity_hash: str,
    instrument: str,
    position_epoch: int,
    # Evidence inputs
    zero_position_evidence: ReconciliationQueryEvidence,
    opening_commands_evidence: list[ReconciliationQueryEvidence],
    no_remaining_opening_orders_evidence: ReconciliationQueryEvidence,
    no_residual_protection_orders_evidence: ReconciliationQueryEvidence,
    execution_evidence: ExecutionEvidence,
    economic_evidence: ReconciliationQueryEvidence,
    now_ns: int,
    current_signed_position_qty: Decimal = Decimal("0"),
    current_position_evidence_complete: bool = False,
    terminal_opening_order_certainty: bool = False,
    no_unresolved_opening_command: bool = False,
    reservation_release_evidence: bool = False,
    writer_identity_current: bool = False,
    account_identity_current: bool = False,
    required_query_windows_complete: bool = False,
    late_contradictory_evidence: bool = False,
    intent_id: str | None = None,
) -> FlatCertificate:
    """Certify flat state with complete evidence chain.

    Returns FlatCertificate with decision:
    - CERTIFIED_FLAT: all evidence positively confirms flat
    - NOT_FLAT: evidence positively shows non-flat (residual position/orders)
    - INCONCLUSIVE: evidence incomplete or conflicting
    """
    mismatches: list[str] = []

    # 1. Positive current flatness, not historical fill quantity.
    if current_signed_position_qty != Decimal("0"):
        mismatches.append(f"current signed position is not zero: {current_signed_position_qty}")
    if not current_position_evidence_complete:
        mismatches.append("current position evidence is not complete/current")
    # The query is still required to be complete and interval-covered; its
    # empty result is supporting evidence, not the whole certificate.
    if not zero_position_evidence.can_certify_absence:
        if zero_position_evidence.status != QueryStatus.SUCCESS:
            mismatches.append(f"zero position query failed: {zero_position_evidence.status.value}")
        elif zero_position_evidence.completeness != Completeness.COMPLETE:
            mismatches.append(f"zero position query incomplete: {zero_position_evidence.completeness.value}")
        elif not zero_position_evidence.is_empty_result:
            mismatches.append("zero position query returned non-empty result")
        else:
            mismatches.append("zero position query retention coverage insufficient")

    # 2. All opening commands must have terminal evidence
    if not terminal_opening_order_certainty:
        mismatches.append("opening command/order terminal certainty is absent")
    if not no_unresolved_opening_command:
        mismatches.append("an opening command remains unresolved")
    for ev in opening_commands_evidence:
        if ev.status != QueryStatus.SUCCESS:
            mismatches.append(f"opening command query failed: {ev.status.value}")
        if ev.completeness != Completeness.COMPLETE:
            mismatches.append(f"opening command query incomplete: {ev.completeness.value}")
        # Check that each opening command has terminal outcome
        # (Implementation would inspect the actual records)

    # 3. No remaining opening orders
    if not no_remaining_opening_orders_evidence.can_certify_absence:
        if no_remaining_opening_orders_evidence.status != QueryStatus.SUCCESS:
            mismatches.append(f"opening orders query failed: {no_remaining_opening_orders_evidence.status.value}")
        elif no_remaining_opening_orders_evidence.completeness != Completeness.COMPLETE:
            mismatches.append(f"opening orders query incomplete: {no_remaining_opening_orders_evidence.completeness.value}")
        elif not no_remaining_opening_orders_evidence.is_empty_result:
            mismatches.append("opening orders query returned non-empty result")
        else:
            mismatches.append("opening orders query retention coverage insufficient")

    # 4. No residual protection/conditional orders for this epoch
    if not no_residual_protection_orders_evidence.can_certify_absence:
        if no_residual_protection_orders_evidence.status != QueryStatus.SUCCESS:
            mismatches.append(f"protection orders query failed: {no_residual_protection_orders_evidence.status.value}")
        elif no_residual_protection_orders_evidence.completeness != Completeness.COMPLETE:
            mismatches.append(f"protection orders query incomplete: {no_residual_protection_orders_evidence.completeness.value}")
        elif not no_residual_protection_orders_evidence.is_empty_result:
            mismatches.append("protection orders query returned non-empty result")
        else:
            mismatches.append("protection orders query retention coverage insufficient")

    # 5. Historical executions may be non-zero.  Require complete, unique
    # execution evidence instead of incorrectly requiring no historical fills.
    execution_ids = [fill.execution_id for fill in execution_evidence.fills]
    if len(execution_ids) != len(set(execution_ids)):
        mismatches.append("execution evidence contains duplicate execution IDs")
    if not execution_evidence.fills and execution_evidence.cumulative_qty != Decimal("0"):
        mismatches.append("execution cumulative quantity is inconsistent with fills")

    if not reservation_release_evidence:
        mismatches.append("reservation release eligibility is not evidenced")
    if not writer_identity_current:
        mismatches.append("writer identity is not current")
    if not account_identity_current:
        mismatches.append("account identity is not current")
    if not required_query_windows_complete:
        mismatches.append("required query windows are incomplete")
    if late_contradictory_evidence:
        mismatches.append("late contradictory evidence reopened recovery")

    # 6. Economic reconciliation (funding/fees)
    # Funding/fee posting may remain pending after execution risk is flat.
    # It is retained as evidence but is not a prerequisite for releasing the
    # execution reservation.

    # Decision
    if not mismatches:
        decision = FlatCertificationDecision.CERTIFIED_FLAT
    elif any("non-empty" in m or "non-zero" in m for m in mismatches):
        decision = FlatCertificationDecision.NOT_FLAT
    else:
        decision = FlatCertificationDecision.INCONCLUSIVE

    return FlatCertificate(
        certification_id=certification_id,
        reconciliation_run_id=reconciliation_run_id,
        writer_id=writer_id,
        writer_epoch=writer_epoch,
        account_identity_hash=account_identity_hash,
        instrument=instrument,
        position_epoch=position_epoch,
        zero_position_evidence=zero_position_evidence,
        opening_commands_terminal_evidence=tuple(opening_commands_evidence),
        no_remaining_opening_orders_evidence=no_remaining_opening_orders_evidence,
        no_residual_protection_orders_evidence=no_residual_protection_orders_evidence,
        execution_dedup_evidence=execution_evidence,
        economic_reconciliation_evidence=economic_evidence,
        decision=decision,
        certified_at_ns=now_ns,
        mismatch_details=tuple(mismatches),
        current_signed_position_qty=current_signed_position_qty,
        current_position_evidence_complete=current_position_evidence_complete,
        terminal_opening_order_certainty=terminal_opening_order_certainty,
        no_unresolved_opening_command=no_unresolved_opening_command,
        reservation_release_evidence=reservation_release_evidence,
        writer_identity_current=writer_identity_current,
        account_identity_current=account_identity_current,
        required_query_windows_complete=required_query_windows_complete,
        late_contradictory_evidence=late_contradictory_evidence,
        intent_id=intent_id,
    )
