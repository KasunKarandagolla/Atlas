"""Immutable matured labels for chronological research; no capital authority."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any

from atlas.domain.money import canonical_decimal_str
from atlas.v2._serialization import decimal_value, nonblank, sha256_json, sha256_ref, strict_fields, timestamp
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.risk import ActualClosedPositionSourceV2

VERSION = "MATURED_OUTCOME_V2_V1"


class OutcomeProvenanceV2(StrEnum):
    ACTUAL = "ACTUAL"
    SIMULATED = "SIMULATED"
    COUNTERFACTUAL = "COUNTERFACTUAL"


class CalendarStateV2(StrEnum):
    SELECTED = "SELECTED"
    UNSELECTED = "UNSELECTED"
    REJECTED = "REJECTED"
    NO_CANDIDATE = "NO_CANDIDATE"
    NO_TRADE = "NO_TRADE"
    NOT_ESTIMABLE = "NOT_ESTIMABLE"
    EXPIRED = "EXPIRED"
    NO_FILL = "NO_FILL"
    PARTIAL_FILL = "PARTIAL_FILL"
    FULL_FILL = "FULL_FILL"


class LabelStateV2(StrEnum):
    MATURED = "MATURED"
    UNRESOLVED = "UNRESOLVED"
    CENSORED = "CENSORED"


@dataclass(frozen=True)
class MaturedOutcomeV2:
    decision_ref: str
    candidate_set_ref: str
    candidate_ref: str | None
    policy_id: str
    policy_version: str
    policy_hash: str
    action_hash: str | None
    action_artifact_ref: str | None
    action_absence_reason: str | None
    instrument_revision: str | None
    venue: str | None
    product: str | None
    decision_at_ns: int
    horizon_end_ns: int
    matured_at_ns: int
    available_at_ns: int
    label_definition: str
    label_view: str
    calendar_state: CalendarStateV2
    label_state: LabelStateV2
    provenance: OutcomeProvenanceV2
    payoff_unit: str
    quantity_unit: str
    gross_payoff: Decimal | None
    fees: Decimal | None
    funding_cashflow: Decimal | None
    net_payoff: Decimal | None
    fill_quantity: Decimal | None
    requested_quantity: Decimal | None
    mfe: Decimal | None
    mae: Decimal | None
    evidence_refs: tuple[str, ...]
    execution_evidence_ref: str | None
    extrema_evidence_ref: str | None
    actual_closed_source_ref: str | None
    evidence_resolution: str
    evidence_quality: str
    ambiguity: tuple[str, ...]
    reason: str | None = None

    def __post_init__(self) -> None:
        for name in ("decision_ref", "candidate_set_ref", "policy_hash"):
            sha256_ref(getattr(self, name), field=name)
        for name in ("candidate_ref", "action_hash", "action_artifact_ref", "instrument_revision", "execution_evidence_ref",
                     "extrema_evidence_ref", "actual_closed_source_ref"):
            value = getattr(self, name)
            if value is not None:
                sha256_ref(value, field=name)
        for name in ("policy_id", "policy_version", "label_definition", "label_view", "evidence_resolution", "evidence_quality"):
            nonblank(getattr(self, name), field=name)
        if self.label_view not in ("ACTUAL_SYSTEM", "RECONSTRUCTED_MARKET"):
            raise ValueError("unsupported label view")
        if self.payoff_unit != "USDT" or self.quantity_unit != "CONTRACTS":
            raise ValueError("unsupported payoff/quantity units for V2 linear perpetual label")
        for name in ("decision_at_ns", "horizon_end_ns", "matured_at_ns", "available_at_ns"):
            timestamp(getattr(self, name), field=name)
        if not self.decision_at_ns < self.horizon_end_ns <= self.matured_at_ns <= self.available_at_ns:
            raise ValueError("outcome chronology invalid")
        for name, enum in (("calendar_state", CalendarStateV2), ("label_state", LabelStateV2), ("provenance", OutcomeProvenanceV2)):
            object.__setattr__(self, name, enum(getattr(self, name)))
        if self.candidate_ref is None:
            if self.calendar_state != CalendarStateV2.NO_CANDIDATE or self.action_hash is not None:
                raise ValueError("only NO_CANDIDATE may lack candidate identity")
        elif self.calendar_state == CalendarStateV2.NO_CANDIDATE:
            raise ValueError("NO_CANDIDATE cannot claim a candidate")
        if self.action_hash is None:
            if self.action_artifact_ref is not None:
                raise ValueError("absent action cannot claim action artifact")
            nonblank(self.action_absence_reason or "", field="action_absence_reason")
        elif self.action_absence_reason is not None or self.candidate_ref is None or self.action_artifact_ref is None:
            raise ValueError("frozen action identity and absence reason conflict")
        if self.calendar_state in (CalendarStateV2.UNSELECTED, CalendarStateV2.REJECTED, CalendarStateV2.NO_CANDIDATE) and self.action_hash is not None:
            raise ValueError("unselected/rejected/no-candidate cannot invent a frozen action")
        if self.instrument_revision is None:
            if self.candidate_ref is not None or self.venue is not None or self.product is not None:
                raise ValueError("candidate instrument identity incomplete")
        elif not self.venue or not self.product:
            raise ValueError("venue and product required with instrument revision")
        for name in ("gross_payoff", "fees", "funding_cashflow", "net_payoff", "fill_quantity", "requested_quantity", "mfe", "mae"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, decimal_value(value, field=name))
        if self.label_state == LabelStateV2.MATURED:
            if None in (self.gross_payoff, self.fees, self.funding_cashflow, self.net_payoff):
                raise ValueError("matured monetary label requires complete components")
            assert self.gross_payoff is not None and self.fees is not None and self.funding_cashflow is not None and self.net_payoff is not None
            if self.fees < 0 or self.net_payoff != self.gross_payoff - self.fees + self.funding_cashflow:
                raise ValueError("net payoff must count fees and funding once")
            if self.action_hash is None and (self.calendar_state not in (
                    CalendarStateV2.NO_TRADE, CalendarStateV2.NO_CANDIDATE, CalendarStateV2.EXPIRED)
                    or any(x != 0 for x in (self.gross_payoff, self.fees, self.funding_cashflow, self.net_payoff))
                    or self.fill_quantity not in (None, 0)):
                raise ValueError("no-action decision cannot claim executable counterfactual payoff")
        elif any(x is not None for x in (self.gross_payoff, self.fees, self.funding_cashflow, self.net_payoff, self.fill_quantity, self.mfe, self.mae)):
            raise ValueError("unresolved/censored label cannot assert measured payoff or fill")
        if self.requested_quantity is not None and self.requested_quantity <= 0:
            raise ValueError("requested quantity must be positive")
        if (self.mfe is None) != (self.mae is None):
            raise ValueError("MFE and MAE must be measured together")
        if self.mfe is not None:
            if self.extrema_evidence_ref is None or self.mfe < 0 or self.mae is None or self.mae > 0:
                raise ValueError("extrema require signed, referenced measured evidence")
        elif self.extrema_evidence_ref is not None:
            raise ValueError("extrema evidence cannot assert unmeasured extrema")
        if self.fill_quantity is not None:
            if self.fill_quantity < 0 or self.requested_quantity is None or self.fill_quantity > self.requested_quantity:
                raise ValueError("fill quantity invalid")
            if self.calendar_state == CalendarStateV2.NO_FILL and self.fill_quantity != 0:
                raise ValueError("no-fill quantity must be zero")
            if self.calendar_state == CalendarStateV2.PARTIAL_FILL and not 0 < self.fill_quantity < self.requested_quantity:
                raise ValueError("partial-fill quantity invalid")
            if self.fill_quantity > 0 and self.execution_evidence_ref is None:
                raise ValueError("positive fill requires execution evidence")
            if self.fill_quantity > 0 and self.action_hash is None:
                raise ValueError("candidate without frozen action cannot claim executable fill")
        if self.label_state == LabelStateV2.MATURED and self.calendar_state in (
                CalendarStateV2.NO_FILL, CalendarStateV2.PARTIAL_FILL, CalendarStateV2.FULL_FILL):
            if self.execution_evidence_ref is None or self.fill_quantity is None or self.requested_quantity is None:
                raise ValueError("matured fill state requires measured execution evidence")
            if self.calendar_state == CalendarStateV2.FULL_FILL and self.fill_quantity != self.requested_quantity:
                raise ValueError("full-fill quantity must equal requested quantity")
        if self.provenance == OutcomeProvenanceV2.ACTUAL:
            if self.label_view != "ACTUAL_SYSTEM":
                raise ValueError("ACTUAL outcome requires ACTUAL_SYSTEM view")
            if self.label_state == LabelStateV2.MATURED and self.actual_closed_source_ref is None:
                raise ValueError("matured ACTUAL requires reconciled source ref")
            if self.label_state == LabelStateV2.MATURED and self.action_hash is None:
                raise ValueError("matured ACTUAL closed outcome requires frozen action")
        elif self.actual_closed_source_ref is not None or self.label_view != "RECONSTRUCTED_MARKET":
            raise ValueError("research labels cannot claim actual-system view/source")
        if self.provenance == OutcomeProvenanceV2.COUNTERFACTUAL and self.fill_quantity is not None and self.fill_quantity > 0 and self.execution_evidence_ref is None:
            raise ValueError("counterfactual market path does not prove execution")
        refs = tuple(self.evidence_refs)
        if refs != tuple(sorted(set(refs))):
            raise ValueError("evidence refs must be sorted and unique")
        for ref in refs:
            sha256_ref(ref, field="evidence_ref")
        if self.execution_evidence_ref is not None and self.execution_evidence_ref not in refs:
            raise ValueError("execution evidence must be in evidence_refs")
        if self.extrema_evidence_ref is not None and self.extrema_evidence_ref not in refs:
            raise ValueError("extrema evidence must be in evidence_refs")
        if self.actual_closed_source_ref is not None and self.actual_closed_source_ref not in refs:
            raise ValueError("actual source must be in evidence_refs")
        if self.label_state != LabelStateV2.MATURED:
            nonblank(self.reason or "", field="reason")

    def _body(self) -> dict[str, Any]:
        from atlas.v2._serialization import json_value
        return json_value({"version": VERSION, **{name: getattr(self, name) for name in self.__dataclass_fields__}})

    @property
    def content_hash(self) -> str:
        return sha256_json(self._body())

    def to_dict(self) -> dict[str, Any]:
        return {**self._body(), "content_hash": self.content_hash}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MaturedOutcomeV2:
        fields = set(cls.__dataclass_fields__) | {"version", "content_hash"}
        d = strict_fields(data, expected=fields, required=fields, name="MaturedOutcomeV2")
        if d["version"] != VERSION:
            raise ValueError("unsupported MaturedOutcomeV2 version")
        if not isinstance(d["evidence_refs"], list) or not isinstance(d["ambiguity"], list):
            raise ValueError("refs and ambiguity must be arrays")
        values = {name: d[name] for name in cls.__dataclass_fields__}
        for name in ("gross_payoff", "fees", "funding_cashflow", "net_payoff", "fill_quantity", "requested_quantity", "mfe", "mae"):
            if values[name] is not None:
                values[name] = decimal_value(values[name], field=name, wire=True)
        values["evidence_refs"] = tuple(values["evidence_refs"])
        values["ambiguity"] = tuple(values["ambiguity"])
        result = cls(**values)
        if result.content_hash != d["content_hash"]:
            raise ValueError("MaturedOutcomeV2 content_hash mismatch")
        return result


def index_matured_outcome(repo: OpsRepository, outcome: MaturedOutcomeV2) -> str:
    """Index an available label; callers retain its immutable payload separately."""
    if outcome.action_artifact_ref is not None:
        action = repo.get_artifact(outcome.action_artifact_ref)
        artifact = action.metadata.get("action_artifact") if action is not None else None
        identity = action.metadata.get("action_identity") if action is not None else None
        key = identity.get("key") if isinstance(identity, Mapping) else None
        if (action is None or action.artifact_type != "ActionArtifactV2" or action.available_at_ns > outcome.horizon_end_ns
                or not isinstance(artifact, Mapping) or not isinstance(identity, Mapping) or not isinstance(key, Mapping)
                or artifact.get("action_hash") != outcome.action_hash
                or artifact.get("candidate_ref") != outcome.candidate_ref
                or artifact.get("candidate_set_ref") != outcome.candidate_set_ref
                or identity.get("policy_id") != outcome.policy_id
                or identity.get("policy_version") != outcome.policy_version
                or identity.get("policy_hash") != outcome.policy_hash
                or key.get("contract_revision") != outcome.instrument_revision
                or key.get("venue") != outcome.venue or key.get("product") != outcome.product):
            raise ValueError("matured outcome exact frozen action identity mismatch")
    for ref in outcome.evidence_refs:
        entry = repo.get_artifact(ref)
        if entry is None or entry.available_at_ns > outcome.matured_at_ns:
            raise ValueError("outcome maturity cannot precede required evidence availability")
    if outcome.provenance == OutcomeProvenanceV2.ACTUAL and outcome.label_state == LabelStateV2.MATURED:
        entry = repo.get_artifact(outcome.actual_closed_source_ref or "")
        if entry is None or entry.artifact_type != "ActualClosedPositionSourceV2":
            raise ValueError("qualified actual closed source required")
        key = entry.metadata.get("key")
        if (not isinstance(key, Mapping) or key.get("contract_revision") != outcome.instrument_revision
                or key.get("venue") != outcome.venue or key.get("product") != outcome.product
                or entry.metadata.get("close_at_ns", outcome.horizon_end_ns + 1) > outcome.horizon_end_ns
                or entry.available_at_ns > outcome.matured_at_ns
                or outcome.net_payoff is None
                or entry.metadata.get("realized_net_pnl") != canonical_decimal_str(outcome.net_payoff)
                or sha256_json(entry.metadata) != outcome.actual_closed_source_ref):
            raise ValueError("actual close reconciliation mismatch")
    repo.register_artifact(ArtifactIndexEntryV2(outcome.content_hash, "MaturedOutcomeV2", outcome.content_hash,
        outcome.matured_at_ns, outcome.available_at_ns, {"outcome": outcome.to_dict()}))
    return outcome.content_hash


def training_eligible(outcome: MaturedOutcomeV2, cutoff_ns: int) -> bool:
    timestamp(cutoff_ns, field="training cutoff")
    return outcome.label_state == LabelStateV2.MATURED and outcome.available_at_ns <= cutoff_ns


def realized_risk_source(outcome: MaturedOutcomeV2, source: ActualClosedPositionSourceV2) -> str:
    """Return only the qualified source identity, never a research payoff proxy."""
    if (outcome.provenance != OutcomeProvenanceV2.ACTUAL or outcome.label_state != LabelStateV2.MATURED
            or outcome.actual_closed_source_ref != source.content_hash or outcome.net_payoff != source.realized_net_pnl
            or outcome.available_at_ns < source.available_at_ns or outcome.instrument_revision != source.key.contract_revision):
        raise ValueError("matured outcome is not reconciled actual risk evidence")
    return source.content_hash
