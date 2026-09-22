"""Durable typed protection evidence; acknowledgements alone are never proof."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from atlas.domain.enums import ProtectionStatus
from atlas.domain.execution import ProtectionObservation
from atlas.domain.time import ensure_utc_ns

_PLACEHOLDERS = {"", "REQUIRED", "PLACEHOLDER", "TEST_GATE", "UNVERIFIED"}


def _valid_refs(refs: tuple[str, ...]) -> bool:
    return bool(refs) and all(
        isinstance(value, str) and value.strip() and value.strip().upper() not in _PLACEHOLDERS for value in refs
    )


@dataclass(frozen=True)
class ProtectionEvidence:
    account_ref: str
    instrument: str
    position_epoch: int
    desired_stop_version: int
    observed_signed_qty: Decimal
    full_position_semantics: bool
    stop_price: Decimal
    trigger_basis: str
    closing_only_behavior: bool
    position_view_evidence_ids: tuple[str, ...]
    conditional_order_view_evidence_ids: tuple[str, ...]
    observation_time_ns: int
    receive_time_ns: int
    status: ProtectionStatus
    market_stop_semantics: bool = False

    def __post_init__(self) -> None:
        if not self.account_ref.strip() or not self.instrument.strip():
            raise ValueError("protection identity required")
        ensure_utc_ns(self.observation_time_ns, field="observation_time_ns")
        ensure_utc_ns(self.receive_time_ns, field="receive_time_ns")
        if self.stop_price <= 0:
            raise ValueError("positive stop required")
        object.__setattr__(self, "position_view_evidence_ids", tuple(self.position_view_evidence_ids))
        object.__setattr__(self, "conditional_order_view_evidence_ids", tuple(self.conditional_order_view_evidence_ids))

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "account_ref": self.account_ref,
            "instrument": self.instrument,
            "position_epoch": self.position_epoch,
            "desired_stop_version": self.desired_stop_version,
            "observed_signed_qty": str(self.observed_signed_qty),
            "full_position_semantics": self.full_position_semantics,
            "stop_price": str(self.stop_price),
            "trigger_basis": self.trigger_basis,
            "market_stop_semantics": self.market_stop_semantics,
            "closing_only_behavior": self.closing_only_behavior,
            "position_view_evidence_ids": list(self.position_view_evidence_ids),
            "conditional_order_view_evidence_ids": list(self.conditional_order_view_evidence_ids),
            "observation_time_ns": self.observation_time_ns,
            "receive_time_ns": self.receive_time_ns,
            "status": self.status.value,
        }

    def expected_evidence_hash(self) -> str:
        return hashlib.sha256(
            json.dumps(self.canonical_payload(), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    @property
    def is_confirmed(self) -> bool:
        return self.status == ProtectionStatus.CONFIRMED


@dataclass(frozen=True)
class ProtectionVerificationResult:
    evidence: ProtectionEvidence
    verified: bool
    mismatch_details: tuple[str, ...]


def verify_protection(
    observation: ProtectionObservation,
    expected_position_epoch: int,
    expected_signed_qty: Decimal,
    expected_stop_price: Decimal,
    expected_trigger_basis: str,
    now_ns: int,
    max_staleness_ns: int,
    *,
    account_ref: str,
    instrument: str,
    receive_time_ns: int | None = None,
    conditional_order_evidence_ids: tuple[str, ...] = (),
    conditional_order_view_available: bool = False,
) -> ProtectionVerificationResult:
    mismatches: list[str] = []
    semantics = observation.semantics.lower()
    if observation.position_epoch != expected_position_epoch:
        mismatches.append("position epoch mismatch")
    if observation.qty != expected_signed_qty or expected_signed_qty == 0:
        mismatches.append("signed quantity not exact current exposure")
    if observation.stop_price != expected_stop_price:
        mismatches.append("stop price mismatch")
    if observation.trigger_basis != expected_trigger_basis or observation.trigger_basis != "MarkPrice":
        mismatches.append("trigger basis mismatch")
    has_full_market = "full" in semantics and "market" in semantics
    if not has_full_market:
        mismatches.append("not full-position market semantics")
    closing_only = "reduce" in semantics or "close" in semantics
    if not closing_only:
        mismatches.append("not closing-only semantics")
    if not _valid_refs(tuple(observation.evidence_ids)):
        mismatches.append("position evidence refs invalid")
    if conditional_order_view_available and not _valid_refs(conditional_order_evidence_ids):
        mismatches.append("conditional evidence refs invalid")
    if receive_time_ns is None:
        receive_time_ns = now_ns
    ensure_utc_ns(receive_time_ns, field="receive_time_ns")
    ensure_utc_ns(now_ns, field="now_ns")
    if receive_time_ns > now_ns:
        mismatches.append("receive time is in the future")
    age = now_ns - observation.observed_at_ns
    if age < 0:
        mismatches.append("source clock conflict/future observation")
    elif age > max_staleness_ns:
        mismatches.append("protection evidence stale")
    evidence = ProtectionEvidence(
        account_ref=account_ref,
        instrument=instrument,
        position_epoch=observation.position_epoch,
        desired_stop_version=observation.desired_stop_version,
        observed_signed_qty=observation.qty,
        full_position_semantics="full" in semantics,
        stop_price=observation.stop_price,
        trigger_basis=observation.trigger_basis,
        closing_only_behavior=closing_only,
        position_view_evidence_ids=tuple(observation.evidence_ids),
        conditional_order_view_evidence_ids=tuple(conditional_order_evidence_ids),
        observation_time_ns=observation.observed_at_ns,
        receive_time_ns=receive_time_ns,
        status=ProtectionStatus.CONFIRMED if not mismatches else ProtectionStatus.UNCONFIRMED,
        market_stop_semantics="market" in semantics,
    )
    return ProtectionVerificationResult(evidence, not mismatches, tuple(mismatches))
