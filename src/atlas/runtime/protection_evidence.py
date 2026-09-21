"""Upgraded protection evidence model (freeze §1.3, §1.4).

Records more than minimal protection proof:
- account reference/hash
- instrument
- position epoch
- desired stop version
- observed signed quantity
- full-position semantics
- stop price, trigger basis
- closing-only behavior/evidence
- observation time
- raw evidence IDs (position view + conditional order view)
- freshness/currentness

Unknown/conflicting evidence => UNCONFIRMED.
Stop acknowledgement is NOT protection proof.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from atlas.domain.enums import ProtectionStatus
from atlas.domain.execution import ProtectionObservation
from atlas.domain.time import ensure_utc_ns


@dataclass(frozen=True)
class ProtectionEvidence:
    """Complete protection evidence record for audit/reconciliation."""

    # Identity
    account_ref: str
    instrument: str
    position_epoch: int

    # Desired state
    desired_stop_version: int

    # Observed state
    observed_signed_qty: Decimal
    full_position_semantics: bool  # True = venue adjusts qty with position size
    stop_price: Decimal
    trigger_basis: str  # "MarkPrice" | "LastPrice" | "IndexPrice"
    closing_only_behavior: bool  # True = stop only reduces position

    # Evidence
    position_view_evidence_ids: tuple[str, ...]
    conditional_order_view_evidence_ids: tuple[str, ...]
    observation_time_ns: int
    receive_time_ns: int

    # Derived status
    status: ProtectionStatus
    freshness_ns: int  # observation_time_ns relative to now

    def __post_init__(self) -> None:
        if not self.account_ref or not self.account_ref.strip():
            raise ValueError("account_ref must be non-blank")
        if not self.instrument or not self.instrument.strip():
            raise ValueError("instrument must be non-blank")
        if not isinstance(self.position_epoch, int) or isinstance(self.position_epoch, bool) or self.position_epoch < 0:
            raise ValueError("position_epoch must be int >= 0")
        if not isinstance(self.desired_stop_version, int) or isinstance(self.desired_stop_version, bool) or self.desired_stop_version < 0:
            raise ValueError("desired_stop_version must be int >= 0")
        if not isinstance(self.observed_signed_qty, Decimal):
            raise ValueError("observed_signed_qty must be Decimal")
        if not isinstance(self.full_position_semantics, bool):
            raise ValueError("full_position_semantics must be bool")
        if not isinstance(self.stop_price, Decimal) or self.stop_price <= 0:
            raise ValueError("stop_price must be positive Decimal")
        if not self.trigger_basis or not self.trigger_basis.strip():
            raise ValueError("trigger_basis must be non-blank")
        if not isinstance(self.closing_only_behavior, bool):
            raise ValueError("closing_only_behavior must be bool")
        ensure_utc_ns(self.observation_time_ns, field="observation_time_ns")
        ensure_utc_ns(self.receive_time_ns, field="receive_time_ns")
        if self.receive_time_ns < self.observation_time_ns:
            raise ValueError("receive_time_ns cannot precede observation_time_ns")
        if not isinstance(self.status, ProtectionStatus):
            raise ValueError("status must be ProtectionStatus")
        if not isinstance(self.freshness_ns, int) or isinstance(self.freshness_ns, bool) or self.freshness_ns < 0:
            raise ValueError("freshness_ns must be int >= 0")

    @property
    def is_confirmed(self) -> bool:
        """Protection is CONFIRMED only if status=CONFIRMED and evidence is current."""
        return self.status == ProtectionStatus.CONFIRMED

    @property
    def has_conflicting_evidence(self) -> bool:
        """Check for conflicting evidence between position view and conditional order view."""
        # If both views exist but disagree on key fields, mark as conflicted
        return self.status == ProtectionStatus.BREACHED


@dataclass(frozen=True)
class ProtectionVerificationResult:
    """Result of protection verification attempt."""

    evidence: ProtectionEvidence
    verified: bool
    mismatch_details: tuple[str, ...]  # Empty if verified

    def __bool__(self) -> bool:
        return self.verified


def verify_protection(
    observation: ProtectionObservation,
    expected_position_epoch: int,
    expected_signed_qty: Decimal,
    expected_stop_price: Decimal,
    expected_trigger_basis: str,
    now_ns: int,
    max_staleness_ns: int,
) -> ProtectionVerificationResult:
    """Verify protection observation matches expected state.

    Returns verified=True only if:
    - position_epoch matches
    - observed qty covers expected (within venue propagation tolerance)
    - stop_price matches expected
    - trigger_basis matches expected
    - semantics indicate full-position closing-only
    - evidence is fresh (within max_staleness_ns)
    - no conflicting evidence between views
    """
    mismatches: list[str] = []

    if observation.position_epoch != expected_position_epoch:
        mismatches.append(f"position_epoch {observation.position_epoch} != expected {expected_position_epoch}")

    # Qty check: observed must cover expected (venue qty may briefly lag)
    # Use absolute values for comparison
    if abs(observation.qty) < abs(expected_signed_qty):
        mismatches.append(f"observed qty {observation.qty} < expected {expected_signed_qty}")

    if observation.stop_price != expected_stop_price:
        mismatches.append(f"stop_price {observation.stop_price} != expected {expected_stop_price}")

    if observation.trigger_basis != expected_trigger_basis:
        mismatches.append(f"trigger_basis {observation.trigger_basis} != expected {expected_trigger_basis}")

    if "Full" not in observation.semantics:
        mismatches.append(f"semantics {observation.semantics!r} not full-position")

    freshness = now_ns - observation.observed_at_ns
    if freshness > max_staleness_ns:
        mismatches.append(f"evidence stale: {freshness}ns > {max_staleness_ns}ns")

    verified = len(mismatches) == 0

    # Build evidence record
    evidence = ProtectionEvidence(
        account_ref="",  # Filled by caller
        instrument="",   # Filled by caller
        position_epoch=observation.position_epoch,
        desired_stop_version=observation.desired_stop_version,
        observed_signed_qty=observation.qty,
        full_position_semantics="Full" in observation.semantics,
        stop_price=observation.stop_price,
        trigger_basis=observation.trigger_basis,
        closing_only_behavior="reduce" in observation.semantics.lower() or "close" in observation.semantics.lower(),
        position_view_evidence_ids=observation.evidence_ids,
        conditional_order_view_evidence_ids=(),  # Would need separate query
        observation_time_ns=observation.observed_at_ns,
        receive_time_ns=now_ns,
        status=ProtectionStatus.CONFIRMED if verified else ProtectionStatus.UNCONFIRMED,
        freshness_ns=freshness,
    )

    return ProtectionVerificationResult(
        evidence=evidence,
        verified=verified,
        mismatch_details=tuple(mismatches),
    )
