"""Bounded retrospective replay producer; never infer missing market evidence.

Minute records are supplied and sealed by an evidence publisher. Native BBO
samples alone do not establish executable depth or complete mark/last ranges.
This producer reconstructs the existing replay contract and uses its fill math.
"""
from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from atlas.science.execution_replay import ReplayMinute
from atlas.v2._serialization import canonical_json, json_value, sha256_json, sha256_ref, timestamp
from atlas.v2.contracts import CandidateActionV2
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.raw import RawObservationV2
from atlas.v2.instruments import InstrumentKeyV2, ProductContractV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.science.action import ActionArtifactV2, FrozenActionV2
from atlas.v2.science.costs import FeeScheduleV2, FundingCashflowV2, FundingScheduleV2
from atlas.v2.science.outcomes import DecisionCalendarEntryV2
from atlas.v2.science.replay import (
    ReplayAssumptionsV2,
    ReplayPathV2,
    ReplayStatusV2,
    index_replay_path,
    replay_action,
)
from atlas.v2.strategies.s1_trend import S1_POLICY
from atlas.v2.strategies.s2_breakout import S2_POLICY

MAX_REPLAY_MINUTES = 256
MAX_RAW_REFS = 1024
SOURCE_TYPE = "ActionReplaySourceEvidenceV1"


@dataclass(frozen=True)
class ActionOutcomeProductionV1:
    status: str
    decision_ref: str
    action_ref: str | None
    payoff_ref: str | None
    evidence_refs: tuple[str, ...]
    reason_code: str | None
    available_at_ns: int


@dataclass(frozen=True)
class ActionReplayLifecycleSummaryV1:
    """Sealed replay/source linkage at one deterministic lifecycle locator."""
    decision_ref: str
    action_ref: str
    payoff_ref: str
    source_evidence_ref: str
    evidence_cutoff_ns: int
    execution_status: str
    entry_at_ns: int | None
    exit_at_ns: tuple[int, ...]
    exit_reason: str
    filled_quantity: str
    remaining_quantity: str
    payoff_available_at_ns: int
    available_at_ns: int

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": "ACTION_REPLAY_LIFECYCLE_SUMMARY_V1",
            **{name: getattr(self, name) for name in self.__dataclass_fields__}})

    @property
    def artifact_ref(self) -> str:
        return sha256_json({"version": "ACTION_REPLAY_LIFECYCLE_IDENTITY_V1",
            "decision_ref": self.decision_ref, "action_ref": self.action_ref,
            "payoff_ref": self.payoff_ref, "source_evidence_ref": self.source_evidence_ref})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class ActionReplaySourceEvidenceV1:
    """Exact replay inputs, including one sealed minute record per path minute.

    Each minute record has type ActionReplayMinuteEvidenceV1 and metadata keys
    minute, raw_source_refs, source_class and bound_assumptions_ref. Its metadata
    hash is its artifact reference. No completeness assertion is synthesized.
    """
    decision_ref: str
    action_ref: str
    path_ref: str
    product_ref: str
    existing_portfolio_ref: str
    replay_assumptions_ref: str
    fee_ref: str
    funding_schedule_ref: str
    minute_source_refs: tuple[tuple[int, str], ...]
    raw_source_refs: tuple[str, ...]
    available_at_ns: int
    source_class: str = "EXACT_MINUTE"
    bound_assumptions_ref: str | None = None

    def __post_init__(self) -> None:
        for name in ("decision_ref", "action_ref", "path_ref", "product_ref",
                     "existing_portfolio_ref", "replay_assumptions_ref", "fee_ref",
                     "funding_schedule_ref"):
            sha256_ref(getattr(self, name), field=name)
        timestamp(self.available_at_ns, field="available_at_ns")
        minutes = tuple((at, ref) for at, ref in self.minute_source_refs)
        if not 1 <= len(minutes) <= MAX_REPLAY_MINUTES:
            raise ValueError("bounded minute source evidence required")
        if tuple(at for at, _ in minutes) != tuple(sorted({at for at, _ in minutes})):
            raise ValueError("minute source times must be ordered and unique")
        for at, ref in minutes:
            timestamp(at, field="minute_at_ns")
            sha256_ref(ref, field="minute_source_ref")
        raw = tuple(self.raw_source_refs)
        if not 1 <= len(raw) <= MAX_RAW_REFS or raw != tuple(sorted(set(raw))):
            raise ValueError("raw source references must be bounded, unique and ordered")
        for ref in raw:
            sha256_ref(ref, field="raw_source_ref")
        if self.source_class not in {"EXACT_MINUTE", "CONSERVATIVE_BOUND"}:
            raise ValueError("unsupported replay source class")
        if self.source_class == "CONSERVATIVE_BOUND":
            if not isinstance(self.bound_assumptions_ref, str):
                raise ValueError("conservative bounds require sealed assumptions")
            sha256_ref(self.bound_assumptions_ref, field="bound_assumptions_ref")
        elif self.bound_assumptions_ref is not None:
            raise ValueError("exact evidence cannot carry bound assumptions")
        object.__setattr__(self, "minute_source_refs", minutes)
        object.__setattr__(self, "raw_source_refs", raw)

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": "ACTION_REPLAY_SOURCE_EVIDENCE_V1",
                           **{name: getattr(self, name) for name in self.__dataclass_fields__}})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, body: Mapping[str, Any]) -> ActionReplaySourceEvidenceV1:
        fields = set(cls.__dataclass_fields__)
        if set(body) != fields | {"version"} or body["version"] != "ACTION_REPLAY_SOURCE_EVIDENCE_V1":
            raise ValueError("invalid replay source evidence wire contract")
        return cls(**{key: body[key] for key in fields})


def _entry(repo: OpsRepository, ref: str, cutoff: int, kind: str | None = None) -> ArtifactIndexEntryV2:
    item = repo.get_artifact(ref)
    if (item is None or item.content_hash != ref or item.artifact_ref != ref
            or item.available_at_ns > cutoff or (kind is not None and item.artifact_type != kind)):
        raise ValueError("replay input missing, future or contradictory")
    return item


def _body(repo: OpsRepository, ref: str, cutoff: int, kind: str,
          wrapper: str | None = None) -> Mapping[str, Any]:
    item = _entry(repo, ref, cutoff, kind)
    body = item.metadata if wrapper is None else item.metadata.get(wrapper)
    if not isinstance(body, Mapping):
        raise ValueError("replay input content mismatch")
    decoded = json_value(body)
    if kind == "CandidateActionV2":
        digest = CandidateActionV2.from_dict(decoded).content_hash
    elif kind == "ProductContractV2":
        digest = ProductContractV2.from_dict(decoded).content_hash
    else:
        digest = sha256_json(body)
    if digest != ref or ("available_at_ns" in decoded
            and decoded["available_at_ns"] != item.available_at_ns):
        raise ValueError("replay input content mismatch")
    return decoded


def _calendar(entry: ArtifactIndexEntryV2, cutoff: int) -> DecisionCalendarEntryV2:
    body = entry.metadata.get("decision_entry")
    if entry.artifact_type != "DecisionCalendarEntryV2" or not isinstance(body, Mapping):
        raise ValueError("typed decision calendar required")
    decision = DecisionCalendarEntryV2.from_dict(json_value(body))
    if (entry.artifact_ref != decision.content_hash or entry.content_hash != decision.content_hash
            or entry.created_at_ns != decision.created_at_ns
            or entry.available_at_ns != decision.available_at_ns or entry.available_at_ns > cutoff):
        raise ValueError("decision calendar content mismatch")
    return decision


def _validate_source(repo: OpsRepository, source: ActionReplaySourceEvidenceV1,
                     calendar: ArtifactIndexEntryV2, cutoff: int) -> Mapping[str, Any]:
    decision = _calendar(calendar, cutoff)
    if (source.decision_ref != decision.content_hash or source.action_ref != decision.action_artifact_ref
            or source.available_at_ns > cutoff):
        raise ValueError("source decision/action binding mismatch")
    cutoff = min(cutoff, source.available_at_ns)
    path = _body(repo, source.path_ref, cutoff, "ReplayPathV2", "path")
    for name, maximum in (("closed_15m_refs", 256), ("funding_refs", 256), ("minutes", MAX_REPLAY_MINUTES)):
        values = path.get(name)
        if not isinstance(values, list) or len(values) > maximum:
            raise ValueError("replay path supplementary evidence exceeds its declared bound")
    artifact = _body(repo, source.action_ref, cutoff, "ActionArtifactV2", "action_artifact")
    portfolio = _body(repo, source.existing_portfolio_ref, cutoff, "ExistingPortfolioPathV2", "portfolio")
    if (artifact.get("action_hash") != decision.action_hash
            or artifact.get("candidate_ref") != decision.candidate_ref
            or artifact.get("candidate_set_ref") != decision.candidate_set_ref
            or artifact.get("product_ref") != source.product_ref
            or artifact.get("policy_hash") != decision.policy_hash
            or portfolio.get("common_path_id") != path.get("common_path_id")
            or portfolio.get("scenario_manifest_ref") != path.get("scenario_manifest_ref")):
        raise ValueError("source action/product/portfolio binding mismatch")
    minutes = path.get("minutes")
    if (not isinstance(minutes, list) or not 1 <= len(minutes) <= MAX_REPLAY_MINUTES
            or tuple(row["at_ns"] for row in minutes) != tuple(at for at, _ in source.minute_source_refs)):
        raise ValueError("path minute binding mismatch")
    for ref, kind in ((source.action_ref, "ActionArtifactV2"), (source.product_ref, "ProductContractV2"),
                      (source.existing_portfolio_ref, "ExistingPortfolioPathV2"),
                      (source.replay_assumptions_ref, "ReplayAssumptionsV2"),
                      (source.fee_ref, "FeeScheduleV2"), (source.funding_schedule_ref, "FundingScheduleV2")):
        item = _entry(repo, ref, cutoff, kind)
        if kind in {"ReplayAssumptionsV2", "FeeScheduleV2", "FundingScheduleV2", "ProductContractV2"}:
            if item.available_at_ns > decision.decision_at_ns:
                raise ValueError("replay assumptions/cost/product must be predeclared")
    if source.bound_assumptions_ref is not None:
        assumptions = _entry(repo, source.bound_assumptions_ref, decision.decision_at_ns)
        if (sha256_json(assumptions.metadata) != source.bound_assumptions_ref
                or assumptions.metadata.get("scope") != "RETROSPECTIVE_ONLY"
                or not assumptions.metadata.get("bounds")):
            raise ValueError("explicit sealed bound assumptions required")
    raw_refs: set[str] = set()
    for (_, ref), minute in zip(source.minute_source_refs, minutes, strict=True):
        row = _body(repo, ref, cutoff, "ActionReplayMinuteEvidenceV1")
        if type(minute.get("available")) is not bool:
            raise ValueError("typed replay minute availability required")
        refs = row.get("raw_source_refs")
        if (canonical_json(row.get("minute")) != canonical_json(minute)
                or row.get("source_class") != source.source_class
                or row.get("bound_assumptions_ref") != source.bound_assumptions_ref
                or not isinstance(refs, list) or not refs or len(refs) > MAX_RAW_REFS):
            raise ValueError("sealed minute/raw/bound evidence mismatch")
        raw_refs.update(refs)
    if raw_refs != set(source.raw_source_refs):
        raise ValueError("minute evidence raw source binding mismatch")
    for ref in source.raw_source_refs:
        _entry(repo, ref, cutoff)
    return path


def index_action_replay_source_evidence(repo: OpsRepository, source: ActionReplaySourceEvidenceV1) -> str:
    """Validate exact indexed inputs before publishing a sealed replay source."""
    calendar = _entry(repo, source.decision_ref, source.available_at_ns, "DecisionCalendarEntryV2")
    _validate_source(repo, source, calendar, source.available_at_ns)
    repo.register_artifact(ArtifactIndexEntryV2(source.content_hash, SOURCE_TYPE, source.content_hash,
        source.available_at_ns, source.available_at_ns, {"source_evidence": source.to_dict()}))
    return source.content_hash


def _inputs(repo: OpsRepository, source: ActionReplaySourceEvidenceV1, cutoff: int,
            path_body: Mapping[str, Any]) -> dict[str, Any]:
    cutoff = min(cutoff, source.available_at_ns)
    action_item = _entry(repo, source.action_ref, cutoff, "ActionArtifactV2")
    artifact = _body(repo, source.action_ref, cutoff, "ActionArtifactV2", "action_artifact")
    identity = json_value(action_item.metadata["action_identity"])
    frozen = dict(identity)
    frozen.pop("version")
    frozen["key"] = InstrumentKeyV2.from_dict(frozen["key"])
    for field in ("quantity", "entry_reference", "entry_collar", "stop_price"):
        frozen[field] = Decimal(frozen[field])
    action = ActionArtifactV2(FrozenActionV2(**frozen), artifact["candidate_ref"],
        artifact["sizing_ref"], artifact["candidate_set_ref"], artifact["available_at_ns"])
    for ref in (action.candidate_ref, action.sizing_ref, action.candidate_set_ref,
                path_body["source_ref"], path_body["scenario_manifest_ref"]):
        _entry(repo, ref, cutoff)
    candidate = CandidateActionV2.from_dict(_body(repo, action.candidate_ref, cutoff, "CandidateActionV2", "candidate"))
    assumptions_body = dict(_body(repo, source.replay_assumptions_ref, cutoff, "ReplayAssumptionsV2"))
    assumptions = ReplayAssumptionsV2(assumptions_body["decision_to_arrival_ns"],
        assumptions_body["human_delay_ns"], assumptions_body["stop_latency_ns"],
        assumptions_body["exit_latency_ns"], Decimal(assumptions_body["participation"]),
        Decimal(assumptions_body["exit_impact"]))
    fee_body = dict(_body(repo, source.fee_ref, cutoff, "FeeScheduleV2"))
    fee_body.pop("version")
    fee_body["key"] = InstrumentKeyV2.from_dict(fee_body["key"])
    for name in ("entry_taker_rate", "exit_taker_rate"):
        fee_body[name] = Decimal(fee_body[name])
    fee = FeeScheduleV2(**fee_body)
    schedule_body = dict(_body(repo, source.funding_schedule_ref, cutoff, "FundingScheduleV2"))
    schedule_body.pop("version")
    schedule = FundingScheduleV2(**schedule_body)
    product = ProductContractV2.from_dict(_body(repo, source.product_ref, cutoff, "ProductContractV2", "product"))
    def optional_decimal(value: Any) -> Decimal | None:
        return Decimal(value) if value is not None else None
    minutes = tuple(ReplayMinute(row["at_ns"], optional_decimal(row["bid"]), optional_decimal(row["ask"]),
        optional_decimal(row["bid_depth"]), optional_decimal(row["ask_depth"]),
        Decimal(row["mark_low"]), Decimal(row["mark_high"]), Decimal(row["last_low"]),
        Decimal(row["last_high"]), row["available"]) for row in path_body["minutes"])
    funding = []
    for ref in path_body["funding_refs"]:
        body = dict(_body(repo, ref, cutoff, "FundingCashflowV2"))
        body.pop("version")
        for name in ("signed_rate", "mark_price"):
            body[name] = Decimal(body[name])
        funding.append(FundingCashflowV2(**body))
    bars = []
    for ref in path_body["closed_15m_refs"]:
        bar_body = _body(repo, ref, cutoff, "CausalBarV2")
        raw_item = _entry(repo, bar_body["record_id"], cutoff)
        raw_body = raw_item.metadata.get("raw")
        if not isinstance(raw_body, Mapping):
            raise ValueError("closed bar raw evidence unavailable")
        raw = RawObservationV2.from_dict(json_value(raw_body))
        bars.append(CausalBarV2(raw, BarIntervalV2(bar_body["interval"]), bar_body["open_at_ns"],
            bar_body["close_at_ns"], Decimal(bar_body["open"]), Decimal(bar_body["high"]),
            Decimal(bar_body["low"]), Decimal(bar_body["close"]), Decimal(bar_body["volume"]), bar_body["final"]))
    path = ReplayPathV2(path_body["common_path_id"], path_body["scenario_manifest_ref"],
        path_body["available_at_ns"], minutes, tuple(bars), tuple(funding), path_body["source_ref"])
    if path.content_hash != source.path_ref or action.content_hash != source.action_ref:
        raise ValueError("reconstructed replay identity mismatch")
    index_replay_path(repo, path)
    return {"action": action, "candidate": candidate, "path": path,
        "existing_portfolio_ref": source.existing_portfolio_ref, "assumptions": assumptions,
        "fee": fee, "schedule": schedule, "product": product}



def _lifecycle_summary(repo: OpsRepository, source: ActionReplaySourceEvidenceV1,
                       payoff_ref: str, payoff: Mapping[str, Any],
                       production_clock: Callable[[], int]) -> str:
    # The sealed source binds every replay input to this exact availability.
    # A maintenance retry may happen after the payoff publication, including
    # recovery from interruption between payoff and summary persistence. Its
    # later cycle cutoff is not the historical payoff's information cutoff.
    evidence_cutoff_ns = source.available_at_ns
    if evidence_cutoff_ns > payoff["available_at_ns"]:
        raise ValueError("lifecycle replay source postdates payoff computation")
    published_at = production_clock()
    timestamp(published_at, field="lifecycle publication")
    if published_at < payoff["available_at_ns"]:
        raise ValueError("lifecycle publication precedes payoff computation")
    summary = ActionReplayLifecycleSummaryV1(source.decision_ref, source.action_ref,
        payoff_ref, source.content_hash, evidence_cutoff_ns, payoff["status"],
        payoff["entry"]["at_ns"] if payoff["entry"] is not None else None,
        tuple(row["at_ns"] for row in payoff["exits"]), payoff["exit_reason"],
        payoff["filled_quantity"], payoff["remaining_quantity"], payoff["available_at_ns"], published_at)
    existing = repo.get_artifact(summary.artifact_ref)
    if existing is not None:
        body = existing.metadata.get("summary")
        if (existing.artifact_type != "ActionReplayLifecycleSummaryV1" or not isinstance(body, Mapping)
                or set(body) != set(summary.to_dict())
                or sha256_json(body) != existing.content_hash
                or existing.available_at_ns > published_at
                or body.get("available_at_ns") != existing.available_at_ns
                or type(body.get("evidence_cutoff_ns")) is not int
                or not source.available_at_ns <= body["evidence_cutoff_ns"] <= payoff["available_at_ns"]
                or existing.created_at_ns != existing.available_at_ns):
            raise ValueError("lifecycle receipt is contradictory or unavailable")
        expected = summary.to_dict()
        for name in expected.keys() - {"available_at_ns", "evidence_cutoff_ns"}:
            if canonical_json(body.get(name)) != canonical_json(expected[name]):
                raise ValueError("lifecycle receipt payoff/source linkage mismatch")
        return summary.artifact_ref
    repo.register_artifact(ArtifactIndexEntryV2(summary.artifact_ref, "ActionReplayLifecycleSummaryV1",
        summary.content_hash, summary.available_at_ns, summary.available_at_ns,
        {"summary": summary.to_dict(), "input_refs": [source.decision_ref, source.action_ref,
            payoff_ref, source.content_hash]}))
    return summary.artifact_ref


class RetrospectiveActionOutcomeProducerV1:
    def __init__(self, *, clock_ns: Callable[[], int] = time.time_ns,
                 evidence_resolver: Callable[..., ActionReplaySourceEvidenceV1 | None] | None = None):
        self.clock_ns = clock_ns
        self.evidence_resolver = evidence_resolver

    def __call__(self, repo: OpsRepository, indexed_calendar: ArtifactIndexEntryV2,
                 evidence_cutoff_ns: int, *, clock_ns: Callable[[], int] | None = None) -> ActionOutcomeProductionV1:
        raw_clock = clock_ns or self.clock_ns
        timestamp(evidence_cutoff_ns, field="evidence cutoff")
        previous_sample = evidence_cutoff_ns
        def production_clock() -> int:
            nonlocal previous_sample
            sampled = raw_clock()
            timestamp(sampled, field="production time")
            if sampled < previous_sample:
                raise ValueError("production clock moved backwards or precedes evidence cutoff")
            previous_sample = sampled
            return sampled
        produced_at = production_clock()
        decision_ref = indexed_calendar.artifact_ref
        action_ref: str | None = None
        refs: tuple[str, ...] = ()
        def result(status: str, reason: str | None, payoff_ref: str | None = None) -> ActionOutcomeProductionV1:
            completed_at = production_clock()
            timestamp(completed_at, field="production completion")
            if completed_at < produced_at:
                raise ValueError("production clock moved backwards")
            return ActionOutcomeProductionV1(status, decision_ref, action_ref, payoff_ref, refs, reason, completed_at)
        try:
            decision = _calendar(indexed_calendar, evidence_cutoff_ns)
            action_ref = decision.action_artifact_ref
            if action_ref is None:
                return result("UNSUPPORTED", "NO_FROZEN_ACTION")
            page = repo.artifact_entries_by_metadata_identity(SOURCE_TYPE, ("source_evidence", "decision_ref"),
                decision_ref, as_of_ns=evidence_cutoff_ns, limit=1)
            if page.has_more or page.invalid_entry_count:
                return result("NOT_ESTIMABLE", "CONFLICTING_REPLAY_SOURCE_EVIDENCE")
            source = None
            if page.entries:
                entry = page.entries[0]
                source = ActionReplaySourceEvidenceV1.from_dict(json_value(entry.metadata["source_evidence"]))
                if (source.content_hash != entry.artifact_ref or source.content_hash != entry.content_hash
                        or entry.available_at_ns != source.available_at_ns
                        or entry.created_at_ns != source.available_at_ns):
                    raise ValueError("sealed source evidence content mismatch")
            elif self.evidence_resolver is not None:
                source = self.evidence_resolver(repo, indexed_calendar, evidence_cutoff_ns)
                if source is not None:
                    if not isinstance(source, ActionReplaySourceEvidenceV1):
                        raise ValueError("typed replay source evidence required")
                    index_action_replay_source_evidence(repo, source)
            if source is None:
                return result("NOT_ESTIMABLE", "REPLAY_SOURCE_EVIDENCE_UNAVAILABLE")
            if not isinstance(source, ActionReplaySourceEvidenceV1):
                raise ValueError("typed replay source evidence required")
            path_body = _validate_source(repo, source, indexed_calendar, evidence_cutoff_ns)
            refs = tuple(sorted({source.content_hash, source.path_ref, source.action_ref,
                source.product_ref, source.existing_portfolio_ref, source.replay_assumptions_ref,
                source.fee_ref, source.funding_schedule_ref, *source.raw_source_refs,
                *(ref for _, ref in source.minute_source_refs),
                *((source.bound_assumptions_ref,) if source.bound_assumptions_ref else ())}))
            inputs = _inputs(repo, source, evidence_cutoff_ns, path_body)
            action = inputs["action"]
            if (action.action.action_hash != decision.action_hash or action.candidate_ref != decision.candidate_ref
                    or action.candidate_set_ref != decision.candidate_set_ref
                    or action.action.policy_hash != decision.policy_hash):
                raise ValueError("replay calendar/action identity mismatch")
            if action.action.policy_hash not in {S1_POLICY.policy_hash, S2_POLICY.policy_hash}:
                return result("UNSUPPORTED", "UNSUPPORTED_REPLAY_POLICY")
            existing = repo.artifact_entries_by_metadata_identity("PolicyPayoffV2", ("payoff", "action_hash"),
                action.action.action_hash, as_of_ns=produced_at, limit=1)
            if existing.has_more or existing.invalid_entry_count:
                return result("NOT_ESTIMABLE", "CONFLICTING_ACTION_PAYOFF_EVIDENCE")
            if existing.entries:
                item = existing.entries[0]
                body = _body(repo, item.artifact_ref, produced_at, "PolicyPayoffV2", "payoff")
                expected = {"action_artifact_ref": source.action_ref, "path_ref": source.path_ref,
                    "existing_portfolio_ref": source.existing_portfolio_ref,
                    "replay_assumptions_ref": source.replay_assumptions_ref, "fee_ref": source.fee_ref,
                    "funding_schedule_ref": source.funding_schedule_ref}
                if any(body.get(key) != value for key, value in expected.items()):
                    raise ValueError("existing payoff/source binding mismatch")
                receipt_ref = _lifecycle_summary(repo, source, item.artifact_ref, body,
                    production_clock)
                refs = tuple(sorted({*refs, receipt_ref}))
                if body.get("status") == ReplayStatusV2.NOT_ESTIMABLE:
                    return result("NOT_ESTIMABLE", "REPLAY_NOT_ESTIMABLE", item.artifact_ref)
                return result("EXISTING", None, item.artifact_ref)
            payoff = replay_action(repo, **inputs, replay_cutoff_ns=evidence_cutoff_ns,
                production_clock_ns=production_clock)
            receipt_ref = _lifecycle_summary(repo, source, payoff.content_hash, payoff.to_dict(),
                production_clock)
            refs = tuple(sorted({*refs, receipt_ref}))
            if payoff.status == ReplayStatusV2.NOT_ESTIMABLE:
                return result("NOT_ESTIMABLE", payoff.reasons[0] if payoff.reasons else "REPLAY_NOT_ESTIMABLE", payoff.content_hash)
            return result("PRODUCED", None, payoff.content_hash)
        except (ValueError, TypeError, KeyError, ArithmeticError):
            return result("NOT_ESTIMABLE", "REPLAY_EVIDENCE_INVALID")
