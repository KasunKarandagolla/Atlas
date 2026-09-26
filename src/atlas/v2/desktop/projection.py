"""Bounded immutable read models projected from persisted V2 evidence."""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

from .._serialization import canonical_json, sha256_json, sha256_ref, strict_fields, timestamp
from ..contracts import (
    CandidateActionV2,
    CandidateSelectionStatus,
    CandidateSetV2,
    EligibilityStatusV2,
    OpportunityWatchV2,
)
from ..data.bars import BarIntervalV2, CausalBarV2
from ..data.raw import AvailabilityClassV2, RawObservationV2
from ..instruments import UniverseContractV2
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository

DESKTOP_PROJECTION_SCHEMA_VERSION = 2
DESKTOP_PROJECTION_VERSION = "ATLAS_DESKTOP_PROJECTION_V2"
MAX_SCANNER_ROWS = 500
MAX_WATCH_ROWS = 500
MAX_EVIDENCE_ROWS = 500
MAX_CHART_BARS = 10_000
MAX_SNAPSHOT_BYTES = 900_000
SOURCE_HEALTH_MAX_AGE_NS = 60_000_000_000
_SAFE_CODE = re.compile(r"^[A-Z0-9_.:-]{1,128}$")

_EVIDENCE_TYPES = (
    "UniverseContractV2", "ProductContractV2", "FeatureArtifactV2", "PublicObservationIndexV2",
    "ScannerSelectionSourceV1", "ScannerRankEvidenceV1", "OpportunityWatchV2",
    "S1SetupEvidenceV1", "S2SetupEvidenceV1", "S2TriggerEvidenceV1",
    "CandidateSetV2", "CandidateActionV2", "SizingDecisionV2", "ActionArtifactV2",
    "M0FeatureVectorV2", "M0SupportV2", "M0CalibrationV2", "M0OODV2",
    "M0ModelFitV2", "M0ModelRunV2", "M0PredictionV2", "M0OOFResidualArchiveV2",
    "PretradeScenarioArtifactV2", "PretradeExecutionScenarioV2", "PretradeDerivedEvidenceV2",
    "ScenarioSupportV2", "InferenceSupportV2", "OutcomeDistributionV2",
    "EstimationUncertaintyV2", "ExecutionModelUncertaintyV2", "NumericalErrorV2",
    "DeterministicStressV2", "PortfolioESV2", "EvaluationArtifactV2",
    "DecisionCalendarEntryV2", "TradePlanEnvelopeV2", "MaturedOutcomeV2", "ReplayPathV2", "PolicyPayoffV2",
    "RiskPolicyV1", "RiskPolicyV2", "SizingRiskInputV2", "VenueSizingLimitsV2",
    "StressBoundV2", "AccountRiskSnapshotV2", "FeeScheduleV2",
    "VenueCapabilitySnapshotV2", "ModelForecastV2", "ModelForecastArtifactV2",
    "SyntheticIntegrationFixtureV1",
)


def _tuple_strings(value: Any, *, field: str, maximum: int = 10_000) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or len(value) > maximum:
        raise ValueError(f"{field} must be a bounded string array")
    result = tuple(value)
    if any(not isinstance(item, str) or not item for item in result):
        raise ValueError(f"{field} must contain non-empty strings")
    return result


def _text(value: Any, *, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    return default


def _code(value: Any, *, default: str = "UNAVAILABLE") -> str:
    text = _text(value, default=default)
    return text if _SAFE_CODE.fullmatch(text) else default


def _source_label(value: str) -> str:
    return "SOURCE_" + hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest()[:16]


def _strategy_label(value: Any) -> str:
    raw = _text(value, default="UNAVAILABLE")
    if raw in {"S1_MTF_TREND_PULLBACK", "S2_BREAKOUT_VOLUME"}:
        return raw
    return "STRATEGY_" + hashlib.sha256(raw.encode("utf-8", errors="replace")).hexdigest()[:16]


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _body(entry: ArtifactIndexEntryV2) -> Mapping[str, Any]:
    keys = {
        "UniverseContractV2": "universe", "FeatureArtifactV2": "feature",
        "CandidateSetV2": "candidate_set", "CandidateActionV2": "candidate",
        "SizingDecisionV2": "sizing", "ActionArtifactV2": "action_artifact",
        "EvaluationArtifactV2": "evaluation", "DecisionCalendarEntryV2": "decision_entry",
        "TradePlanEnvelopeV2": "plan", "MaturedOutcomeV2": "outcome",
        "PretradeScenarioArtifactV2": "scenario", "PretradeExecutionScenarioV2": "scenario",
    }
    body = entry.metadata.get(keys.get(entry.artifact_type, ""))
    return body if isinstance(body, Mapping) else entry.metadata


def _reason_codes(entry: ArtifactIndexEntryV2, body: Mapping[str, Any]) -> tuple[str, ...]:
    for name in ("reason_codes", "reasons", "diagnostic_reasons"):
        value = body.get(name, entry.metadata.get(name))
        if isinstance(value, (list, tuple)):
            return tuple(_code(item) for item in value)
    if entry.artifact_type == "CandidateSetV2":
        raw = body.get("candidates")
        if isinstance(raw, list):
            reasons = [row.get("rejection_reason") for row in raw if isinstance(row, Mapping)]
            return tuple(_code(item) for item in reasons if item)
    return ()


def _status(entry: ArtifactIndexEntryV2, body: Mapping[str, Any]) -> str:
    for name in (
        "decision", "admission_state", "selection_state", "selection_status", "status",
        "execution_state", "label_state", "state", "capability_status",
    ):
        value = body.get(name, entry.metadata.get(name))
        if value is not None:
            return _code(value, default="INDEXED")
    return "INDEXED"


def _artifact_refs(entry: ArtifactIndexEntryV2, body: Mapping[str, Any]) -> tuple[str, ...]:
    refs: set[str] = set()
    envelopes = body.get("envelope")
    candidates: list[Any] = [body.get("input_refs"), body.get("source_refs"), body.get("dependency_refs")]
    for key, value in body.items():
        if key.endswith("_ref") and isinstance(value, str):
            candidates.append((value,))
        elif key.endswith("_refs"):
            candidates.append(value)
    if isinstance(envelopes, Mapping):
        candidates.append(envelopes.get("input_refs"))
    for values in candidates:
        if isinstance(values, (list, tuple)):
            for ref in values:
                if isinstance(ref, str) and len(ref) == 64:
                    try:
                        refs.add(sha256_ref(ref, field="input ref"))
                    except ValueError:
                        continue
    return tuple(sorted(refs))


@dataclass(frozen=True)
class DesktopStatusV2:
    name: str
    state: str
    value: str
    observed_at_ns: int | None = None
    evidence_ref: str | None = None
    reason_code: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name, "state": self.state, "value": self.value,
            "observed_at_ns": self.observed_at_ns, "evidence_ref": self.evidence_ref,
            "reason_code": self.reason_code,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DesktopStatusV2:
        fields = {"name", "state", "value", "observed_at_ns", "evidence_ref", "reason_code"}
        item = dict(strict_fields(data, expected=fields, required=fields, name=cls.__name__))
        if item["observed_at_ns"] is not None:
            timestamp(item["observed_at_ns"], field="observed_at_ns")
        if item["evidence_ref"] is not None:
            sha256_ref(item["evidence_ref"], field="evidence_ref")
        return cls(item["name"], item["state"], item["value"], item["observed_at_ns"],
                   item["evidence_ref"], item["reason_code"])


@dataclass(frozen=True)
class DesktopScannerRowV2:
    key_json: str
    venue: str
    environment: str
    product: str
    symbol: str
    contract_revision: str
    eligibility: str
    observed_state: str
    strategy_id: str
    policy_hash: str | None
    selection_rank: int | None
    selection_state: str
    watch_state: str
    candidate_ref: str | None
    sizing_status: str
    frozen_action_ref: str | None
    evaluation_decision: str
    reason_codes: tuple[str, ...]
    expiry_ns: int | None
    evidence_complete: bool
    synthetic_fixture: bool

    def to_dict(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DesktopScannerRowV2:
        fields = set(cls.__dataclass_fields__)
        item = dict(strict_fields(data, expected=fields, required=fields, name=cls.__name__))
        item["reason_codes"] = _tuple_strings(item["reason_codes"], field="reason_codes")
        if item["observed_state"] not in {"OBSERVED", "NOT_OBSERVED", "UNAVAILABLE"}:
            raise ValueError("observed_state is not a supported display state")
        for name in ("candidate_ref", "frozen_action_ref", "policy_hash"):
            if item[name] is not None:
                sha256_ref(item[name], field=name)
        if item["expiry_ns"] is not None:
            timestamp(item["expiry_ns"], field="expiry_ns")
        if item["selection_rank"] is not None and (type(item["selection_rank"]) is not int or item["selection_rank"] < 1):
            raise ValueError("selection_rank must be positive or null")
        return cls(**item)


@dataclass(frozen=True)
class DesktopWatchRowV2:
    watch_id: str
    key_json: str
    venue: str
    product: str
    symbol: str
    strategy_id: str
    strategy_version: str
    policy_hash: str
    state: str
    created_at_ns: int
    wake_at_ns: int | None
    expires_at_ns: int
    invalidation_codes: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    current_status: str

    def to_dict(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DesktopWatchRowV2:
        fields = set(cls.__dataclass_fields__)
        item = dict(strict_fields(data, expected=fields, required=fields, name=cls.__name__))
        item["invalidation_codes"] = _tuple_strings(item["invalidation_codes"], field="invalidation_codes")
        item["evidence_refs"] = _tuple_strings(item["evidence_refs"], field="evidence_refs")
        for name in ("created_at_ns", "expires_at_ns"):
            timestamp(item[name], field=name)
        if item["wake_at_ns"] is not None:
            timestamp(item["wake_at_ns"], field="wake_at_ns")
        sha256_ref(item["policy_hash"], field="policy_hash")
        for ref in item["evidence_refs"]:
            sha256_ref(ref, field="evidence_ref")
        return cls(**item)


@dataclass(frozen=True)
class DesktopEvidenceSummaryV2:
    artifact_type: str
    schema_version: str
    content_ref: str
    content_hash: str
    provenance: str
    decision_at_ns: int | None
    event_at_ns: int | None
    available_at_ns: int
    source_health: str
    input_refs: tuple[str, ...]
    status: str
    label_target: str | None
    reason_codes: tuple[str, ...]
    synthetic_fixture: bool

    def to_dict(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DesktopEvidenceSummaryV2:
        fields = set(cls.__dataclass_fields__)
        item = dict(strict_fields(data, expected=fields, required=fields, name=cls.__name__))
        sha256_ref(item["content_ref"], field="content_ref")
        sha256_ref(item["content_hash"], field="content_hash")
        item["input_refs"] = _tuple_strings(item["input_refs"], field="input_refs")
        item["reason_codes"] = _tuple_strings(item["reason_codes"], field="reason_codes")
        if item["label_target"] is not None:
            item["label_target"] = _code(item["label_target"])
        timestamp(item["available_at_ns"], field="available_at_ns")
        for name in ("decision_at_ns", "event_at_ns"):
            if item[name] is not None:
                timestamp(item[name], field=name)
        return cls(**item)


@dataclass(frozen=True)
class DesktopOverviewV2:
    statuses: tuple[DesktopStatusV2, ...]
    decision_counts: tuple[tuple[str, int], ...]
    universe_instruments: int
    observed_instruments: int
    scanner_eligible_instruments: int
    unavailable_fields: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "statuses": [row.to_dict() for row in self.statuses],
            "decision_counts": [[key, count] for key, count in self.decision_counts],
            "universe_instruments": self.universe_instruments,
            "observed_instruments": self.observed_instruments,
            "scanner_eligible_instruments": self.scanner_eligible_instruments,
            "unavailable_fields": list(self.unavailable_fields),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DesktopOverviewV2:
        fields = {"statuses", "decision_counts", "universe_instruments", "observed_instruments",
                  "scanner_eligible_instruments", "unavailable_fields"}
        item = strict_fields(data, expected=fields, required=fields, name=cls.__name__)
        counts = item["decision_counts"]
        if not isinstance(counts, list) or any(not isinstance(x, list) or len(x) != 2 for x in counts):
            raise ValueError("decision_counts must contain name/count pairs")
        return cls(tuple(DesktopStatusV2.from_dict(x) for x in item["statuses"]),
                   tuple((str(name), int(count)) for name, count in counts),
                   item["universe_instruments"], item["observed_instruments"],
                   item["scanner_eligible_instruments"],
                   _tuple_strings(item["unavailable_fields"], field="unavailable_fields"))


@dataclass(frozen=True)
class DesktopChartBarV2:
    open_at_ns: int
    close_at_ns: int
    open: str
    high: str
    low: str
    close: str
    volume: str
    bar_ref: str
    observation_ref: str

    def to_dict(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DesktopChartBarV2:
        fields = set(cls.__dataclass_fields__)
        item = strict_fields(data, expected=fields, required=fields, name=cls.__name__)
        timestamp(item["open_at_ns"], field="open_at_ns")
        timestamp(item["close_at_ns"], field="close_at_ns")
        sha256_ref(item["bar_ref"], field="bar_ref")
        sha256_ref(item["observation_ref"], field="observation_ref")
        return cls(**item)


@dataclass(frozen=True)
class DesktopChartSeriesV2:
    schema_version: int
    projection_version: str
    key_json: str
    interval: str
    information_cutoff_ns: int
    availability_view: str
    state: str
    reason_code: str | None
    bars: tuple[DesktopChartBarV2, ...]
    synthetic_fixture: bool = False

    SCHEMA_VERSION: ClassVar[int] = DESKTOP_PROJECTION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != self.SCHEMA_VERSION or self.projection_version != DESKTOP_PROJECTION_VERSION:
            raise ValueError("unsupported desktop chart projection version")
        timestamp(self.information_cutoff_ns, field="information_cutoff_ns")
        if len(self.bars) > MAX_CHART_BARS:
            raise ValueError("chart exceeds bounded bar count")
        times = tuple(item.open_at_ns for item in self.bars)
        if times != tuple(sorted(set(times))):
            raise ValueError("chart bars must be unique and time ordered")
        if self.state == "AVAILABLE" and not self.bars:
            raise ValueError("available chart requires bars")
        if self.state != "AVAILABLE" and self.bars:
            raise ValueError("unavailable chart cannot contain fabricated bars")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version, "projection_version": self.projection_version,
            "key_json": self.key_json, "interval": self.interval,
            "information_cutoff_ns": self.information_cutoff_ns,
            "availability_view": self.availability_view, "state": self.state,
            "reason_code": self.reason_code, "bars": [row.to_dict() for row in self.bars],
            "synthetic_fixture": self.synthetic_fixture,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DesktopChartSeriesV2:
        fields = {"schema_version", "projection_version", "key_json", "interval", "information_cutoff_ns",
                  "availability_view", "state", "reason_code", "bars", "synthetic_fixture"}
        item = strict_fields(data, expected=fields, required=fields, name=cls.__name__)
        if not isinstance(item["bars"], list):
            raise ValueError("bars must be an array")
        return cls(item["schema_version"], item["projection_version"], item["key_json"], item["interval"],
                   item["information_cutoff_ns"], item["availability_view"], item["state"],
                   item["reason_code"], tuple(DesktopChartBarV2.from_dict(row) for row in item["bars"]),
                   item["synthetic_fixture"])


@dataclass(frozen=True)
class DesktopSnapshotV2:
    schema_version: int
    projection_version: str
    generated_at_ns: int
    valid_until_ns: int
    freshness_state: str
    overview: DesktopOverviewV2
    scanner_rows: tuple[DesktopScannerRowV2, ...]
    watch_rows: tuple[DesktopWatchRowV2, ...]
    evidence: tuple[DesktopEvidenceSummaryV2, ...]
    evidence_truncated: bool

    SCHEMA_VERSION: ClassVar[int] = DESKTOP_PROJECTION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != self.SCHEMA_VERSION or self.projection_version != DESKTOP_PROJECTION_VERSION:
            raise ValueError("unsupported desktop snapshot version")
        timestamp(self.generated_at_ns, field="generated_at_ns")
        timestamp(self.valid_until_ns, field="valid_until_ns")
        if self.valid_until_ns < self.generated_at_ns:
            raise ValueError("snapshot validity cannot precede generation")
        if len(self.scanner_rows) > MAX_SCANNER_ROWS or len(self.watch_rows) > MAX_WATCH_ROWS:
            raise ValueError("desktop snapshot exceeds bounded row counts")
        if len(self.evidence) > MAX_EVIDENCE_ROWS:
            raise ValueError("desktop snapshot exceeds bounded evidence count")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version, "projection_version": self.projection_version,
            "generated_at_ns": self.generated_at_ns, "valid_until_ns": self.valid_until_ns,
            "freshness_state": self.freshness_state, "overview": self.overview.to_dict(),
            "scanner_rows": [row.to_dict() for row in self.scanner_rows],
            "watch_rows": [row.to_dict() for row in self.watch_rows],
            "evidence": [row.to_dict() for row in self.evidence],
            "evidence_truncated": self.evidence_truncated,
        }

    def to_canonical_json(self) -> str:
        return canonical_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DesktopSnapshotV2:
        fields = {"schema_version", "projection_version", "generated_at_ns", "valid_until_ns", "freshness_state",
                  "overview", "scanner_rows", "watch_rows", "evidence", "evidence_truncated"}
        item = strict_fields(data, expected=fields, required=fields, name=cls.__name__)
        for name in ("scanner_rows", "watch_rows", "evidence"):
            if not isinstance(item[name], list):
                raise ValueError(f"{name} must be an array")
        return cls(item["schema_version"], item["projection_version"], item["generated_at_ns"],
                   item["valid_until_ns"], item["freshness_state"],
                   DesktopOverviewV2.from_dict(item["overview"]),
                   tuple(DesktopScannerRowV2.from_dict(row) for row in item["scanner_rows"]),
                   tuple(DesktopWatchRowV2.from_dict(row) for row in item["watch_rows"]),
                   tuple(DesktopEvidenceSummaryV2.from_dict(row) for row in item["evidence"]),
                   item["evidence_truncated"])


def _synthetic_refs(entries: Sequence[ArtifactIndexEntryV2]) -> frozenset[str]:
    refs: set[str] = set()
    for entry in entries:
        if entry.artifact_type == "SyntheticIntegrationFixtureV1" and entry.metadata.get("synthetic_fixture") is True:
            subjects = entry.metadata.get("subject_refs", ())
            if isinstance(subjects, (list, tuple)):
                refs.update(item for item in subjects if isinstance(item, str) and len(item) == 64)
        if entry.metadata.get("synthetic_fixture") is True:
            refs.add(entry.artifact_ref)
        plan_evidence = entry.metadata.get("shadow_plan_evidence")
        if isinstance(plan_evidence, Mapping) and plan_evidence.get("synthetic_fixture") is True:
            refs.add(entry.artifact_ref)
    return frozenset(refs)


def _entry_summary(entry: ArtifactIndexEntryV2, *, synthetic_refs: frozenset[str]) -> DesktopEvidenceSummaryV2:
    body = _body(entry)
    envelope = body.get("envelope")
    if not isinstance(envelope, Mapping):
        envelope = {}
    version = body.get("version", body.get("schema_version", body.get("artifact_type", "INDEXED")))
    provenance = body.get("provenance", body.get("outcome_provenance", "PERSISTED_V2_ARTIFACT"))
    source_health = body.get("source_health_ref", body.get("source_health", "UNAVAILABLE"))
    decision_at = body.get("decision_at_ns", body.get("decision_slot_ns", body.get("information_cutoff_ns")))
    event_at = body.get("event_at_ns", body.get("event_time_ns"))
    decision_at = decision_at if type(decision_at) is int else None
    event_at = event_at if type(event_at) is int else None
    source_health_text = _code(source_health, default="AVAILABLE_REF")
    provenance_text = _code(provenance, default="PERSISTED_V2_ARTIFACT")
    version_text = _code(version, default=entry.artifact_type)
    return DesktopEvidenceSummaryV2(
        entry.artifact_type, version_text, entry.artifact_ref, entry.content_hash,
        provenance_text, decision_at, event_at, entry.available_at_ns, source_health_text,
        _artifact_refs(entry, body), _status(entry, body),
        _code(body.get("outcome_target")) if body.get("outcome_target") is not None else None,
        _reason_codes(entry, body),
        entry.artifact_ref in synthetic_refs or any(ref in synthetic_refs for ref in _artifact_refs(entry, body)),
    )


def _parse_universe(entries: Sequence[ArtifactIndexEntryV2]) -> dict[str, UniverseContractV2]:
    result: dict[str, UniverseContractV2] = {}
    for entry in entries:
        if entry.artifact_type != "UniverseContractV2":
            continue
        raw = entry.metadata.get("universe")
        if isinstance(raw, Mapping):
            try:
                universe = UniverseContractV2.from_dict(_jsonable(raw))
            except (TypeError, ValueError):
                continue
            if universe.content_hash == entry.artifact_ref == entry.content_hash:
                result[entry.artifact_ref] = universe
    return result


def _parse_candidate_set(entry: ArtifactIndexEntryV2) -> CandidateSetV2 | None:
    raw = entry.metadata.get("candidate_set")
    if not isinstance(raw, Mapping):
        return None
    try:
        value = CandidateSetV2.from_dict(_jsonable(raw))
    except (TypeError, ValueError):
        return None
    return value if value.content_hash == entry.artifact_ref == entry.content_hash else None


def _parse_candidate(entry: ArtifactIndexEntryV2) -> CandidateActionV2 | None:
    raw = entry.metadata.get("candidate")
    if not isinstance(raw, Mapping):
        return None
    try:
        value = CandidateActionV2.from_dict(_jsonable(raw))
    except (TypeError, ValueError):
        return None
    return value if value.content_hash == entry.artifact_ref == entry.content_hash else None


def _project_watch(watch: OpportunityWatchV2) -> DesktopWatchRowV2:
    return DesktopWatchRowV2(
        watch.watch_id, watch.key.to_canonical_json(), watch.key.venue.value, watch.key.product.value,
        watch.key.native_symbol, _strategy_label(watch.strategy_id), _code(watch.strategy_version), watch.policy_hash,
        watch.state.value, watch.created_at_ns, watch.wake_at_ns, watch.expires_at_ns,
        watch.invalidators, watch.evidence_refs,
        "ACTIVE" if watch.state.value in {"DETECTED", "WAITING_FOR_EVENT", "READY_FOR_RECHECK", "CONFIRMED"}
        else "TERMINAL",
    )


def _project_scanner(
    repo: OpsRepository, entries: Sequence[ArtifactIndexEntryV2], watches: Sequence[OpportunityWatchV2],
    synthetic_refs: frozenset[str],
) -> tuple[tuple[DesktopScannerRowV2, ...], dict[str, tuple[str, ...]]]:
    universes = _parse_universe(entries)
    candidates: dict[str, tuple[CandidateActionV2, ArtifactIndexEntryV2]] = {}
    for entry in entries:
        if entry.artifact_type == "CandidateActionV2":
            candidate = _parse_candidate(entry)
            if candidate is not None:
                candidates[candidate.candidate_id] = (candidate, entry)
    candidate_sets = [item for item in (_parse_candidate_set(entry) for entry in entries
                     if entry.artifact_type == "CandidateSetV2") if item is not None]
    latest_sets: dict[str, CandidateSetV2] = {}
    for candidate_set in candidate_sets:
        previous = latest_sets.get(candidate_set.universe_ref)
        if previous is None or (candidate_set.envelope.available_at_ns, candidate_set.decision_event_id,
                                candidate_set.content_hash) > (previous.envelope.available_at_ns,
                                previous.decision_event_id, previous.content_hash):
            latest_sets[candidate_set.universe_ref] = candidate_set
    current_members = [(candidate_set, member) for candidate_set in latest_sets.values()
                       for member in candidate_set.candidates]

    calendar_by_candidate: dict[str, tuple[ArtifactIndexEntryV2, Mapping[str, Any]]] = {}
    for entry in entries:
        if entry.artifact_type != "DecisionCalendarEntryV2":
            continue
        body = entry.metadata.get("decision_entry")
        if not isinstance(body, Mapping) or not isinstance(body.get("candidate_ref"), str):
            continue
        prior = calendar_by_candidate.get(str(body["candidate_ref"]))
        if prior is None or (entry.available_at_ns, entry.artifact_ref) > (prior[0].available_at_ns, prior[0].artifact_ref):
            calendar_by_candidate[str(body["candidate_ref"])] = (entry, body)

    sizing_by_candidate: dict[str, tuple[ArtifactIndexEntryV2, Mapping[str, Any]]] = {}
    action_by_candidate: dict[str, tuple[ArtifactIndexEntryV2, Mapping[str, Any]]] = {}
    for entry in entries:
        if entry.artifact_type == "SizingDecisionV2":
            body = entry.metadata.get("sizing")
            if isinstance(body, Mapping) and isinstance(body.get("candidate_ref"), str):
                prior = sizing_by_candidate.get(str(body["candidate_ref"]))
                if prior is None or (entry.available_at_ns, entry.artifact_ref) > (prior[0].available_at_ns, prior[0].artifact_ref):
                    sizing_by_candidate[str(body["candidate_ref"])] = (entry, body)
        elif entry.artifact_type == "ActionArtifactV2":
            body = entry.metadata.get("action_artifact")
            if isinstance(body, Mapping) and isinstance(body.get("candidate_ref"), str):
                prior = action_by_candidate.get(str(body["candidate_ref"]))
                if prior is None or (entry.available_at_ns, entry.artifact_ref) > (prior[0].available_at_ns, prior[0].artifact_ref):
                    action_by_candidate[str(body["candidate_ref"])] = (entry, body)

    watch_by_id = {item.watch_id: item for item in watches}
    watch_for_candidate: dict[str, str] = {}
    for candidate_id, (_, entry) in candidates.items():
        watch_id = entry.metadata.get("watch_id")
        if isinstance(watch_id, str):
            watch_for_candidate[candidate_id] = watch_id

    rows: dict[str, DesktopScannerRowV2] = {}
    current_candidate_keys = {member.key.to_canonical_json() for _, member in current_members}
    latest_universe = max(universes.values(), key=lambda x: (x.decision_slot_ns, x.content_hash), default=None)
    for universe in (() if latest_universe is None else (latest_universe,)):
        for item in universe.entries:
            key_text = item.key.to_canonical_json()
            if key_text in current_candidate_keys:
                continue
            rows[key_text] = DesktopScannerRowV2(
                key_text, item.key.venue.value, item.key.environment.value, item.key.product.value,
                item.key.native_symbol, item.key.contract_revision,
                "SCANNER_ELIGIBLE" if item.scanner_eligible else "INELIGIBLE",
                "OBSERVED" if item.observed else "NOT_OBSERVED",
                "", None, None, "NO_CANDIDATE", "UNAVAILABLE", None, "UNAVAILABLE", None,
                "NO_CANDIDATE", tuple(item.reasons), None,
                bool(item.product_ref and universe.envelope.available_at_ns <= universe.decision_slot_ns),
                universe.content_hash in synthetic_refs,
            )

    for candidate_set, member in current_members:
        key_text = member.key.to_canonical_json()
        candidate_data = candidates.get(member.candidate_id)
        candidate, candidate_entry = candidate_data if candidate_data is not None else (None, None)
        if candidate is None or candidate_entry is None:
            continue
        candidate_universe = universes.get(candidate_set.universe_ref)
        universe_item = next((item for item in candidate_universe.entries if item.key == candidate.key), None) if candidate_universe else None
        policy_hash = candidate.policy_hash
        calendar_pair = calendar_by_candidate.get(candidate.content_hash)
        calendar_body = calendar_pair[1] if calendar_pair else None
        admission = _text(calendar_body.get("admission_state"), default="NOT_EVALUATED") if calendar_body else "NOT_EVALUATED"
        calendar_reasons = tuple(_code(x) for x in calendar_body.get("reason_codes", ())) if calendar_body else ()
        sizing = sizing_by_candidate.get(candidate.content_hash)
        action = action_by_candidate.get(candidate.content_hash)
        watch = watch_by_id.get(watch_for_candidate.get(candidate.candidate_id, ""))
        reason_codes = calendar_reasons or ((member.rejection_reason,) if member.rejection_reason else ())
        required = {candidate.content_hash, candidate.snapshot_hash, candidate_set.content_hash}
        if member.selection_feature_refs:
            required.update(member.selection_feature_refs)
        if action:
            required.add(action[0].artifact_ref)
        if calendar_pair:
            required.add(calendar_pair[0].artifact_ref)
        all_refs_present = all(repo.get_artifact(ref) is not None for ref in required)
        if candidate_set.selection_status == CandidateSelectionStatus.NOT_ESTIMABLE:
            selection_state = CandidateSelectionStatus.NOT_ESTIMABLE.value
        elif candidate_set.selected_candidate_id == candidate.candidate_id:
            selection_state = "SELECTED"
        elif member.eligibility_status == EligibilityStatusV2.INELIGIBLE:
            selection_state = "REJECTED"
        else:
            selection_state = "UNSELECTED"
        row = DesktopScannerRowV2(
            key_text, candidate.key.venue.value, candidate.key.environment.value,
            candidate.key.product.value, candidate.key.native_symbol, candidate.key.contract_revision,
            "SCANNER_ELIGIBLE" if universe_item and universe_item.scanner_eligible else
            "UNVERIFIED" if universe_item is None else "INELIGIBLE",
            "UNAVAILABLE" if universe_item is None else "OBSERVED" if universe_item.observed else "NOT_OBSERVED",
            _strategy_label(member.policy_id), policy_hash, member.rank, selection_state,
            watch.state.value if watch else "UNAVAILABLE", candidate.content_hash,
            _text(sizing[1].get("status")) if sizing else "UNAVAILABLE",
            action[0].artifact_ref if action else None, admission,
            reason_codes, candidate.deadline_ns, all_refs_present,
            candidate.content_hash in synthetic_refs or candidate_set.content_hash in synthetic_refs or
            (action is not None and action[0].artifact_ref in synthetic_refs),
        )
        rows[f"{key_text}|{candidate.content_hash}"] = row

    # Preserve every member of each latest CandidateSet, including rejected and unselected members.
    sorted_rows = sorted(rows.values(), key=lambda row: (row.selection_rank or 2**31, row.venue,
                         row.product, row.symbol, row.contract_revision, row.candidate_ref or ""))
    rows_tuple = tuple(sorted_rows[:MAX_SCANNER_ROWS])
    candidate_refs_by_key: dict[str, tuple[str, ...]] = {
        row.key_json: (row.candidate_ref,) if row.candidate_ref else () for row in rows_tuple
    }
    return rows_tuple, candidate_refs_by_key


def _decision_counts(entries: Sequence[ArtifactIndexEntryV2]) -> tuple[tuple[str, int], ...]:
    count: Counter[str] = Counter()
    for entry in entries:
        if entry.artifact_type != "DecisionCalendarEntryV2":
            continue
        body = entry.metadata.get("decision_entry")
        if isinstance(body, Mapping):
            state = body.get("admission_state", body.get("selection_state", "UNAVAILABLE"))
            count[_text(state)] += 1
    return tuple(sorted(count.items()))


def _project_statuses(
    repo: OpsRepository, entries: Sequence[ArtifactIndexEntryV2], *, now_ns: int,
    universe_count: int, observed_count: int, eligible_count: int,
) -> tuple[tuple[DesktopStatusV2, ...], tuple[str, ...]]:
    status: list[DesktopStatusV2] = [
        DesktopStatusV2("projection_service", "RUNNING", "read-only", now_ns),
        DesktopStatusV2("process", "UNAVAILABLE", "ENGINE_PROCESS_NOT_INDEXED"),
        DesktopStatusV2("version", "UNAVAILABLE", "RUNTIME_VERSION_NOT_INDEXED"),
        DesktopStatusV2("uptime", "UNAVAILABLE", "ENGINE_UPTIME_NOT_INDEXED"),
        DesktopStatusV2("engine_runtime", "UNVERIFIED", "UNAVAILABLE", reason_code="ENGINE_HEALTH_NOT_INDEXED"),
        DesktopStatusV2("last_successful_input", "UNAVAILABLE", "NO_INPUT_EVIDENCE"),
        DesktopStatusV2("data_lag", "UNAVAILABLE", "NO_INPUT_EVIDENCE"),
        DesktopStatusV2("queue_depth", "UNAVAILABLE", "NOT_REPORTED"),
        DesktopStatusV2("disk_health", "UNAVAILABLE", "NOT_REPORTED"),
        DesktopStatusV2("model_worker", "UNAVAILABLE", "NOT_REPORTED"),
        DesktopStatusV2("universe", "AVAILABLE" if universe_count else "UNAVAILABLE", str(universe_count)),
        DesktopStatusV2("scanner", "AVAILABLE" if eligible_count else "NO_CANDIDATE", str(eligible_count)),
        DesktopStatusV2("new_risk_allowed", "BLOCKED", "False", reason_code="CAPITAL_AUTHORITY_DISABLED"),
        DesktopStatusV2("capability", "UNVERIFIED", "UNAVAILABLE"),
        DesktopStatusV2("recovery", "UNAVAILABLE", "NOT_REPORTED"),
        DesktopStatusV2("economic_value", "NOT_ESTIMABLE", "NOT_ESTIMABLE", reason_code="REAL_LABELS_UNAVAILABLE"),
        DesktopStatusV2("continuous_high_resolution_evidence", "TEST GATE", "UNVERIFIED"),
        DesktopStatusV2("action_position_linkage", "UNVERIFIED", "TEST GATE"),
        DesktopStatusV2("venue_qualification", "UNVERIFIED", "TEST GATE"),
        DesktopStatusV2("soak_72h", "BLOCKED BY ENVIRONMENT", "BLOCKED BY ENVIRONMENT"),
        DesktopStatusV2("research_health", "UNAVAILABLE", "RESEARCH_PROCESS_HEALTH_NOT_INDEXED"),
        DesktopStatusV2("data_health", "UNAVAILABLE", "SOURCE_HEALTH_NOT_REPORTED"),
        DesktopStatusV2("capital_state", "BLOCKED", "CAPITAL_DISABLED", reason_code="CAPITAL_AUTHORITY_DISABLED"),
        DesktopStatusV2("protection_recovery", "UNAVAILABLE", "RECOVERY_HEALTH_NOT_INDEXED"),
    ]
    unavailable = {"process", "version", "uptime", "engine_runtime", "queue_depth", "disk_health",
                   "model_worker", "recovery", "research_health", "data_health", "protection_recovery"}

    observation_times = [item.available_at_ns for item in entries
                         if item.artifact_type in {"FeatureArtifactV2", "PublicObservationIndexV2"}]
    if observation_times:
        last_input = max(observation_times)
        status[5] = DesktopStatusV2("last_successful_input", "AVAILABLE", str(last_input), last_input)
        status[6] = DesktopStatusV2("data_lag", "MEASURED", str(max(0, now_ns - last_input)), last_input)
        unavailable.discard("last_successful_input")
        unavailable.discard("data_lag")

    source_records: list[DesktopStatusV2] = []
    for source_id in repo.source_health_sources():
        history = repo.source_health_history(source_id, limit=1)
        if not history:
            continue
        latest = history[-1]
        state = _code(latest.status)
        if state == "HEALTHY_CURRENT" and now_ns - latest.available_at_ns > SOURCE_HEALTH_MAX_AGE_NS:
            state = "STALE"
        details_ref = latest.details_ref if isinstance(latest.details_ref, str) and len(latest.details_ref) == 64 else None
        if details_ref is not None:
            try:
                sha256_ref(details_ref, field="details_ref")
            except ValueError:
                details_ref = None
        source_records.append(DesktopStatusV2(
            f"source:{_source_label(source_id)}", state, state, latest.available_at_ns, details_ref,
            None if state == "HEALTHY_CURRENT" else _code(f"SOURCE_{state}"),
        ))
    status.extend(sorted(source_records, key=lambda item: item.name))
    if source_records:
        unavailable.discard("data_health")
        all_healthy = all(item.state == "HEALTHY_CURRENT" for item in source_records)
        data_state = "AVAILABLE" if all_healthy else "DEGRADED"
        data_health_index = next(index for index, item in enumerate(status) if item.name == "data_health")
        status[data_health_index] = DesktopStatusV2(
            "data_health", data_state, "SOURCES_HEALTHY_CURRENT" if all_healthy else "SOURCE_HEALTH_DEGRADED",
        )

    capabilities = [item for item in entries if item.artifact_type == "VenueCapabilitySnapshotV2"]
    if capabilities:
        latest_capability = max(capabilities, key=lambda item: (item.available_at_ns, item.artifact_ref))
        body = _body(latest_capability)
        state = _status(latest_capability, body)
        status[13] = DesktopStatusV2(
            "capability", state, state, latest_capability.available_at_ns, latest_capability.artifact_ref
        )
    return tuple(status), tuple(sorted(unavailable))


def project_snapshot(
    repo: OpsRepository, *, now_ns: int | None = None, valid_for_ns: int = 2_000_000_000,
    artifact_limit: int = 2_000,
) -> DesktopSnapshotV2:
    """Project only explicit persisted artifacts into a bounded desktop snapshot."""
    now = time.time_ns() if now_ns is None else now_ns
    timestamp(now, field="now_ns")
    if type(valid_for_ns) is not int or not 100_000_000 <= valid_for_ns <= 30_000_000_000:
        raise ValueError("snapshot validity must be between 100ms and 30s")
    entries = repo.artifact_entries_by_types(_EVIDENCE_TYPES, limit=artifact_limit)
    watches = repo.list_watches(limit=MAX_WATCH_ROWS + 1)
    synthetic_refs = _synthetic_refs(entries)
    scanner, _ = _project_scanner(repo, entries, watches, synthetic_refs)
    watch_rows = tuple(_project_watch(item) for item in watches[:MAX_WATCH_ROWS])
    evidence_rows = tuple(_entry_summary(item, synthetic_refs=synthetic_refs)
                          for item in entries[-MAX_EVIDENCE_ROWS:])
    universes = _parse_universe(entries)
    latest_universe = max(universes.values(), key=lambda item: (item.decision_slot_ns, item.content_hash), default=None)
    universe_count = len(latest_universe.entries) if latest_universe else 0
    observed_count = sum(item.observed for item in latest_universe.entries) if latest_universe else 0
    eligible_count = sum(item.scanner_eligible for item in latest_universe.entries) if latest_universe else 0
    statuses, unavailable = _project_statuses(repo, entries, now_ns=now, universe_count=universe_count,
        observed_count=observed_count, eligible_count=eligible_count)
    overview = DesktopOverviewV2(statuses, _decision_counts(entries), universe_count, observed_count,
                                 eligible_count, unavailable)
    snapshot = DesktopSnapshotV2(DESKTOP_PROJECTION_SCHEMA_VERSION, DESKTOP_PROJECTION_VERSION,
        now, now + valid_for_ns, "CURRENT", overview, scanner, watch_rows, evidence_rows,
        len(entries) >= artifact_limit or len(watches) > MAX_WATCH_ROWS)
    serialized = snapshot.to_canonical_json().encode("utf-8")
    if len(serialized) > MAX_SNAPSHOT_BYTES:
        snapshot = DesktopSnapshotV2(snapshot.schema_version, snapshot.projection_version,
            snapshot.generated_at_ns, snapshot.valid_until_ns, snapshot.freshness_state,
            snapshot.overview, snapshot.scanner_rows[:250], snapshot.watch_rows[:250],
            snapshot.evidence[:100], True)
        if len(snapshot.to_canonical_json().encode("utf-8")) > MAX_SNAPSHOT_BYTES:
            raise ValueError("desktop projection exceeds transport byte bound")
    return snapshot


def _read_chart_bars_from_archive(
    repo: OpsRepository, archive_root: Path, *, key_revision: str, interval: BarIntervalV2,
    cutoff_ns: int, availability_view: AvailabilityClassV2, limit: int,
) -> tuple[tuple[DesktopChartBarV2, ...], bool]:
    import pyarrow.parquet as pq

    from ..data.bars import close_boundary_ns

    if not archive_root.exists() or not archive_root.is_dir():
        return (), False
    versions: dict[int, tuple[tuple[int, str], DesktopChartBarV2]] = {}
    examined = 0
    paths = sorted(path for path in archive_root.glob("*.parquet") if path.is_file() and not path.is_symlink())
    if len(paths) > 20_000:
        return (), True
    for path in paths:
        try:
            parquet = pq.ParquetFile(path)
            columns = {"record_id", "instrument_revision", "event_type", "available_at_ns",
                       "replay_available_at_ns", "availability_class", "raw_payload_hash",
                       "observation_json", "raw_payload_bytes", "bar_json", "archive_record_kind"}
            if not columns.issubset(set(parquet.schema.names)):
                continue
            for batch in parquet.iter_batches(columns=sorted(columns), batch_size=512):
                for row in batch.to_pylist():
                    examined += 1
                    if examined > 2_000_000:
                        return (), True
                    if (row["instrument_revision"] != key_revision or row["event_type"] != f"BAR_{interval.value}"
                            or row["archive_record_kind"] == "DUPLICATE_CONFLICT"
                            or row["availability_class"] != availability_view.value):
                        continue
                    available = (row["available_at_ns"] if availability_view == AvailabilityClassV2.ACTUAL_SYSTEM
                                 else row["replay_available_at_ns"])
                    if type(available) is not int or available > cutoff_ns:
                        continue
                    raw_bytes = row["raw_payload_bytes"]
                    if not isinstance(raw_bytes, bytes) or hashlib.sha256(raw_bytes).hexdigest() != row["raw_payload_hash"]:
                        continue
                    observation = RawObservationV2.from_dict(json.loads(row["observation_json"]))
                    raw_bar = json.loads(row["bar_json"])
                    if not isinstance(raw_bar, Mapping) or raw_bar.get("final") is not True:
                        continue
                    open_at = raw_bar.get("open_at_ns")
                    if type(open_at) is not int:
                        continue
                    close_at = close_boundary_ns(open_at, interval)
                    from decimal import Decimal

                    values = tuple(Decimal(str(raw_bar[name])) for name in ("open", "high", "low", "close", "volume"))
                    bar = CausalBarV2(
                        observation, interval, open_at, close_at, values[0], values[1], values[2], values[3],
                        values[4], raw_bar.get("final") is True,
                    )
                    if row["record_id"] != observation.record_id:
                        continue
                    ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": observation.record_id})
                    indexed = repo.get_artifact(ref)
                    if (indexed is None or indexed.artifact_type != "PublicObservationIndexV2"
                            or indexed.content_hash != observation.content_hash
                            or indexed.metadata.get("bar_content_hash") != bar.content_hash
                            or indexed.metadata.get("raw_payload_hash") != observation.raw_payload_hash):
                        continue
                    display = DesktopChartBarV2(
                        bar.open_at_ns, bar.close_at_ns, str(bar.open), str(bar.high), str(bar.low),
                        str(bar.close), str(bar.volume), bar.content_hash, ref,
                    )
                    identity = (int(available), observation.record_id)
                    old = versions.get(bar.open_at_ns)
                    if old is None or identity > old[0]:
                        versions[bar.open_at_ns] = (identity, display)
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            continue
    return _ordered_chart_bars(versions, limit), len(versions) > limit


def _ordered_chart_bars(
    values: Mapping[int, tuple[tuple[int, str], DesktopChartBarV2]], limit: int
) -> tuple[DesktopChartBarV2, ...]:
    return tuple(values[key][1] for key in sorted(values)[-limit:])


def project_chart_series(
    repo: OpsRepository, *, key_json: str, interval: str, information_cutoff_ns: int,
    archive_root: str | Path | None, availability_view: str = "ACTUAL_SYSTEM",
    limit: int = MAX_CHART_BARS,
) -> DesktopChartSeriesV2:
    """Read actual archived candles only after matching them to the immutable ops index."""
    timestamp(information_cutoff_ns, field="information_cutoff_ns")
    if type(limit) is not int or not 1 <= limit <= MAX_CHART_BARS:
        raise ValueError("chart limit is outside the supported bound")
    try:
        key = json.loads(key_json)
        from ..instruments import InstrumentKeyV2
        instrument = InstrumentKeyV2.from_dict(key)
        frame = BarIntervalV2(interval)
        view = AvailabilityClassV2(availability_view)
    except (ValueError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("chart instrument/interval/availability request is invalid") from exc
    if view not in (AvailabilityClassV2.ACTUAL_SYSTEM, AvailabilityClassV2.RECONSTRUCTED_MARKET):
        raise ValueError("chart availability view must be actual or reconstructed market")
    if archive_root is None:
        return DesktopChartSeriesV2(2, DESKTOP_PROJECTION_VERSION, instrument.to_canonical_json(), frame.value,
            information_cutoff_ns, view.value, "UNAVAILABLE", "ARCHIVE_NOT_CONFIGURED", ())
    bars, truncated = _read_chart_bars_from_archive(repo, Path(archive_root), key_revision=instrument.contract_revision,
        interval=frame, cutoff_ns=information_cutoff_ns, availability_view=view, limit=limit)
    if not bars:
        return DesktopChartSeriesV2(2, DESKTOP_PROJECTION_VERSION, instrument.to_canonical_json(), frame.value,
            information_cutoff_ns, view.value, "UNAVAILABLE",
            "ARCHIVE_SCAN_BOUND_EXCEEDED" if truncated else "NO_CAUSAL_CANDLES_AVAILABLE", ())
    synthetic_refs = _synthetic_refs(repo.artifact_entries_by_types(("SyntheticIntegrationFixtureV1",)))
    return DesktopChartSeriesV2(2, DESKTOP_PROJECTION_VERSION, instrument.to_canonical_json(), frame.value,
        information_cutoff_ns, view.value, "AVAILABLE",
        "SERIES_LIMIT_APPLIED" if truncated else None, bars,
        any(row.bar_ref in synthetic_refs or row.observation_ref in synthetic_refs for row in bars))
