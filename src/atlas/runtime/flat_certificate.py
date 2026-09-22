"""Artifact-backed flat certificates derived from durable journal evidence."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any

from atlas.domain.enums import CommandOutcome, CommandType
from atlas.domain.time import ensure_utc_ns

from .reconciliation_evidence import QueryType, build_reconciliation_bundle


class FlatCertificationDecision(StrEnum):
    CERTIFIED_FLAT = "CERTIFIED_FLAT"
    NOT_FLAT = "NOT_FLAT"
    INCONCLUSIVE = "INCONCLUSIVE"


@dataclass(frozen=True, init=False)
class FlatCertificate:
    certification_id: str
    reconciliation_run_id: str
    intent_id: str | None
    writer_id: str
    writer_epoch: int
    account_identity_hash: str
    instrument: str
    position_epoch: int
    decision: FlatCertificationDecision
    certified_at_ns: int
    evidence_refs: tuple[str, ...]
    mismatch_details: tuple[str, ...]
    derived_from_journal: bool
    zero_position_evidence: Any | None
    opening_commands_terminal_evidence: tuple[Any, ...]
    no_remaining_opening_orders_evidence: Any | None
    no_residual_protection_orders_evidence: Any | None
    execution_dedup_evidence: Any | None
    economic_reconciliation_evidence: Any | None
    current_signed_position_qty: Decimal
    current_position_evidence_complete: bool
    terminal_opening_order_certainty: bool
    no_unresolved_opening_command: bool
    reservation_release_evidence: bool
    writer_identity_current: bool
    account_identity_current: bool
    required_query_windows_complete: bool
    late_contradictory_evidence: bool

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        names = (
            "certification_id",
            "reconciliation_run_id",
            "intent_id",
            "writer_id",
            "writer_epoch",
            "account_identity_hash",
            "instrument",
            "position_epoch",
            "decision",
            "certified_at_ns",
            "evidence_refs",
            "mismatch_details",
        )
        if len(args) == len(names):
            values = dict(zip(names, args, strict=True))
        elif args:
            raise TypeError(f"expected {len(names)} v5 positional fields")
        else:
            values = {}
        values.update(kwargs)
        values.setdefault("intent_id", None)
        values.setdefault("evidence_refs", ())
        values.setdefault("mismatch_details", ())
        defaults = {
            "derived_from_journal": False,
            "zero_position_evidence": None,
            "opening_commands_terminal_evidence": (),
            "no_remaining_opening_orders_evidence": None,
            "no_residual_protection_orders_evidence": None,
            "execution_dedup_evidence": None,
            "economic_reconciliation_evidence": None,
            "current_signed_position_qty": Decimal("0"),
            "current_position_evidence_complete": False,
            "terminal_opening_order_certainty": False,
            "no_unresolved_opening_command": False,
            "reservation_release_evidence": False,
            "writer_identity_current": False,
            "account_identity_current": False,
            "required_query_windows_complete": False,
            "late_contradictory_evidence": False,
        }
        for name, default in defaults.items():
            values.setdefault(name, default)
        for name in (*names, *defaults):
            if name not in values:
                raise TypeError(f"missing required field: {name}")
            object.__setattr__(self, name, values[name])
        object.__setattr__(self, "evidence_refs", tuple(self.evidence_refs))
        object.__setattr__(self, "mismatch_details", tuple(self.mismatch_details))
        object.__setattr__(self, "opening_commands_terminal_evidence", tuple(self.opening_commands_terminal_evidence))
        self.__post_init__()

    def __post_init__(self) -> None:
        ensure_utc_ns(self.certified_at_ns, field="certified_at_ns")

    @property
    def is_flat(self) -> bool:
        return self.decision == FlatCertificationDecision.CERTIFIED_FLAT

    @property
    def can_release_reservation(self) -> bool:
        return self.is_flat and self.derived_from_journal

    def to_dict(self) -> dict[str, Any]:
        return {
            "certification_id": self.certification_id,
            "reconciliation_run_id": self.reconciliation_run_id,
            "intent_id": self.intent_id,
            "writer_id": self.writer_id,
            "writer_epoch": self.writer_epoch,
            "account_identity_hash": self.account_identity_hash,
            "instrument": self.instrument,
            "position_epoch": self.position_epoch,
            "decision": self.decision.value,
            "certified_at_ns": self.certified_at_ns,
            "evidence_refs": list(self.evidence_refs),
            "mismatch_details": list(self.mismatch_details),
            "derived_from_journal": self.derived_from_journal,
        }


def certify_flat_from_journal(
    *,
    journal: Any,
    certification_id: str,
    reconciliation_run_id: str,
    intent_id: str,
    current_writer_id: str,
    current_writer_epoch: int,
    account_identity_hash: str,
    instrument: str,
    position_epoch: int,
    now_ns: int,
) -> FlatCertificate:
    run = journal.load_reconciliation_run(reconciliation_run_id)
    queries = journal.load_run_queries(reconciliation_run_id)
    bundle = build_reconciliation_bundle(run, queries)
    mismatches: list[str] = []
    if not bundle.complete_for_recovery:
        mismatches.append("reconciliation run incomplete/missing required query types")
    if run.account != account_identity_hash:
        mismatches.append("account identity mismatch")
    if run.instrument != instrument:
        mismatches.append("instrument mismatch")
    if run.writer_id != current_writer_id or run.writer_epoch != current_writer_epoch:
        mismatches.append("writer identity/epoch not current")
    by_type = {query.query_type: query for query in queries}
    positions = by_type.get(QueryType.POSITIONS)
    signed_qty: Decimal | None = None
    if positions is None:
        mismatches.append("position evidence missing")
    else:
        try:
            signed_qty = Decimal(str(positions.facts.get("signed_qty")))
            if signed_qty != 0:
                mismatches.append(f"current signed position non-zero: {signed_qty}")
        except (ArithmeticError, TypeError, ValueError):
            mismatches.append("invalid signed_qty evidence")
    for query_type, label in (
        (QueryType.OPEN_ORDERS, "opening orders"),
        (QueryType.CONDITIONAL_ORDERS, "conditional/protection orders"),
    ):
        query = by_type.get(query_type)
        if query is None or not query.can_certify_absence:
            mismatches.append(f"cannot certify absence of {label}")
    for query_type in (
        QueryType.ORDER_HISTORY,
        QueryType.EXECUTION_HISTORY,
        QueryType.WALLET_BALANCE,
        QueryType.TRADING_STOP,
    ):
        query = by_type.get(query_type)
        if query is None or query.status.value != "success" or query.completeness.value != "complete":
            mismatches.append(f"{query_type.value} incomplete")
    commands = journal.load_commands_for_intent(intent_id)
    opening = [command for command in commands if command.command_type == CommandType.SUBMIT_ENTRY]
    if not opening:
        mismatches.append("no opening command evidence for intent")
    unresolved = [
        command
        for command in opening
        if command.outcome in (CommandOutcome.UNSENT, CommandOutcome.UNKNOWN, CommandOutcome.DEFINITE_ACCEPT)
    ]
    if unresolved:
        mismatches.append("opening command remains unresolved")
    decision = (
        FlatCertificationDecision.NOT_FLAT
        if any("non-zero" in mismatch for mismatch in mismatches)
        else FlatCertificationDecision.CERTIFIED_FLAT
        if not mismatches
        else FlatCertificationDecision.INCONCLUSIVE
    )
    refs = tuple(query.evidence_hash for query in queries) + tuple(command.command_id for command in opening)
    return FlatCertificate(
        certification_id=certification_id,
        reconciliation_run_id=reconciliation_run_id,
        intent_id=intent_id,
        writer_id=current_writer_id,
        writer_epoch=current_writer_epoch,
        account_identity_hash=account_identity_hash,
        instrument=instrument,
        position_epoch=position_epoch,
        decision=decision,
        certified_at_ns=now_ns,
        evidence_refs=refs,
        mismatch_details=tuple(mismatches),
        derived_from_journal=True,
        current_signed_position_qty=signed_qty or Decimal("0"),
        current_position_evidence_complete=positions is not None and not mismatches,
    )


def persist_and_release_if_flat(journal: Any, certificate: FlatCertificate) -> Any:
    return journal.release_reservation_from_flat_certificate(certificate)


def certify_flat(
    *,
    certification_id: str,
    reconciliation_run_id: str,
    writer_id: str,
    writer_epoch: int,
    account_identity_hash: str,
    instrument: str,
    position_epoch: int,
    zero_position_evidence: Any,
    opening_commands_evidence: list[Any],
    no_remaining_opening_orders_evidence: Any,
    no_residual_protection_orders_evidence: Any,
    execution_evidence: Any,
    economic_evidence: Any,
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
    """Compatibility constructor that is intentionally never release-authoritative."""
    mismatches: list[str] = []
    if current_signed_position_qty != Decimal("0"):
        mismatches.append(f"current signed position is not zero: {current_signed_position_qty}")
    if not current_position_evidence_complete:
        mismatches.append("current position evidence is not complete/current")
    if not zero_position_evidence.can_certify_absence:
        mismatches.append("zero position query cannot certify absence")
    if not terminal_opening_order_certainty:
        mismatches.append("opening command/order terminal certainty is absent")
    if not no_unresolved_opening_command:
        mismatches.append("an opening command remains unresolved")
    if not no_remaining_opening_orders_evidence.can_certify_absence:
        mismatches.append("opening orders query cannot certify absence")
    if not no_residual_protection_orders_evidence.can_certify_absence:
        mismatches.append("protection orders query cannot certify absence")
    execution_ids = [fill.execution_id for fill in execution_evidence.fills]
    if len(execution_ids) != len(set(execution_ids)):
        mismatches.append("execution evidence contains duplicate execution IDs")
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
    decision = (
        FlatCertificationDecision.NOT_FLAT
        if any("not zero" in mismatch or "non-empty" in mismatch for mismatch in mismatches)
        else FlatCertificationDecision.CERTIFIED_FLAT
        if not mismatches
        else FlatCertificationDecision.INCONCLUSIVE
    )
    refs = tuple(
        getattr(value, "evidence_hash", str(value))
        for value in (
            zero_position_evidence,
            *opening_commands_evidence,
            no_remaining_opening_orders_evidence,
            no_residual_protection_orders_evidence,
            economic_evidence,
        )
    )
    return FlatCertificate(
        certification_id=certification_id,
        reconciliation_run_id=reconciliation_run_id,
        intent_id=intent_id,
        writer_id=writer_id,
        writer_epoch=writer_epoch,
        account_identity_hash=account_identity_hash,
        instrument=instrument,
        position_epoch=position_epoch,
        decision=decision,
        certified_at_ns=now_ns,
        evidence_refs=refs,
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
    )


def record_flat_contradiction(
    journal: Any,
    *,
    certificate_id: str,
    evidence_ref: str,
    observed_at_ns: int,
) -> None:
    """Append a recovery incident without rewriting the immutable certificate."""
    from atlas.domain.enums import LifecycleState, ProtectionStatus, ReconciliationHealth

    from .recovery import RecoveryIncident

    payload = journal.load_flat_certificate_payload(certificate_id)
    if not payload:
        raise ValueError("flat certificate not found")
    incident = RecoveryIncident(
        f"{certificate_id}:contradiction:{observed_at_ns}",
        payload["reconciliation_run_id"],
        "late_contradictory_evidence",
        "RECOVERY_REQUIRED",
        (certificate_id, evidence_ref),
        observed_at_ns,
    )
    journal.append_recovery_incident(incident)
    if payload.get("intent_id"):
        intent = journal.load_intent(payload["intent_id"])
        if intent.lifecycle == LifecycleState.CLOSED:
            journal.update_intent_state(
                intent_id=intent.intent_id,
                lifecycle=LifecycleState.RECOVERY_REQUIRED,
                protection=ProtectionStatus.UNCONFIRMED,
                health=ReconciliationHealth.CONFLICTED,
                expected_version=intent.state_version,
            )
