"""Research-only availability checks; no historical receipt or health synthesis."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any


class EvidenceGrade(StrEnum):
    PROSPECTIVE = "ACTUAL_PROSPECTIVE_OBSERVATION"
    RECONSTRUCTED = "RECONSTRUCTED_HISTORICAL_RESEARCH"
    MODELED = "MODELED_SHADOW_EXECUTION"
    EXCHANGE = "ACTUAL_EXCHANGE_EXECUTION"
    LEGACY = "LEGACY_UNQUALIFIED"


@dataclass(frozen=True)
class CausalFact:
    ref: str
    kind: str
    event_at_ns: int
    confirmed_at_ns: int
    received_at_ns: int
    available_at_ns: int
    ingested_at_ns: int
    computed_at_ns: int
    instrument_revision: str
    source_health_ref: str
    source_hash: str
    code_hash: str
    grade: str = EvidenceGrade.PROSPECTIVE

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    def refusal(self, cutoff_ns: int, instrument_revision: str) -> str | None:
        if self.grade != EvidenceGrade.PROSPECTIVE:
            return "NOT_ORIGINAL_TIME_OBSERVATION"
        if self.instrument_revision != instrument_revision:
            return "POINT_IN_TIME_PRODUCT_REVISION_MISMATCH"
        if not all((self.ref, self.source_health_ref, self.source_hash, self.code_hash)):
            return "SOURCE_HEALTH_OR_LINEAGE_MISSING"
        if not (
            0 < self.event_at_ns <= self.confirmed_at_ns <= self.received_at_ns <= self.available_at_ns <= cutoff_ns
        ):
            return "INPUT_UNAVAILABLE_AT_CUTOFF"
        if not (
            self.received_at_ns <= self.ingested_at_ns <= cutoff_ns
            and self.confirmed_at_ns <= self.computed_at_ns <= self.available_at_ns
        ):
            return "COMPUTATION_OR_INGESTION_UNAVAILABLE_AT_CUTOFF"
        return None


def prospective_refusal(
    facts: Sequence[CausalFact],
    *,
    cutoff_ns: int,
    instrument_revision: str,
    required_kinds: Sequence[str],
    derived_cutoff_ns: int | None = None,
) -> str | None:
    if not instrument_revision or not facts:
        return "ORIGINAL_PROVENANCE_MISSING"
    for fact in facts:
        fact_cutoff = derived_cutoff_ns if fact.kind == "FEATURE" and derived_cutoff_ns is not None else cutoff_ns
        reason = fact.refusal(fact_cutoff, instrument_revision)
        if reason:
            return reason
    if not set(required_kinds) <= {fact.kind for fact in facts}:
        return "REQUIRED_ORIGINAL_SOURCE_MISSING"
    return None


def action_qualification(action: Mapping[str, Any]) -> bool:
    """Legacy/reconstructed actions cannot acquire authority via a later evaluation."""
    import json

    if not (
        action.get("evidence_grade") == EvidenceGrade.PROSPECTIVE
        and action.get("policy_id")
        and action.get("policy_hash")
        and not action.get("hard_refusal_reason")
        and action.get("opportunity_id")
        and action.get("instrument_revision")
    ):
        return False
    try:
        provenance = action.get("provenance") or {}
        if isinstance(provenance, str):
            provenance = json.loads(provenance)
        facts = [CausalFact(**f) for f in provenance["facts"]]
        decision, seal, trigger = (int(action[n]) for n in ("decision_at_ns", "sealed_at_ns", "trigger_at_ns"))
        if not (int(action["setup_at_ns"]) <= trigger <= decision <= seal <= int(action["deadline_ns"])):
            return False
        return (
            prospective_refusal(
                facts,
                cutoff_ns=decision,
                derived_cutoff_ns=seal,
                instrument_revision=action["instrument_revision"],
                required_kinds=("M15", "H1", "H4", "PRODUCT", "UNIVERSE", "FEATURE", "BBO", "EVENT_GATE", "MARK_INDEX"),
            )
            is None
        )
    except (ValueError, TypeError, KeyError):
        return False
