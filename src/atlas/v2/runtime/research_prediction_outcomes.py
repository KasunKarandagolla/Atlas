"""Bounded maturation of declared price forecasts, with no execution labels.

The existing controller owns the repository and calls this maintenance lane.
An immutable missingness snapshot may later be followed by a supported label;
neither is an executable action outcome or a fee-adjusted economic claim.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Any

from atlas.v2._serialization import canonical_json, json_value, sha256_json, sha256_ref, strict_fields, timestamp
from atlas.v2.data.bars import BarIntervalV2
from atlas.v2.data.history import IndexedCausalBarV2, reconstruct_indexed_causal_bars_v1
from atlas.v2.data.raw import AvailabilityClassV2, indexed_availability_matches
from atlas.v2.instruments import InstrumentKeyV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository, prediction_due_lane_v1
from atlas.v2.models.baseline import BaselineInputsV2
from atlas.v2.models.protocol import ForecastArtifactV2
from atlas.v2.models.worker_protocol import WorkerRequestV2
from atlas.v2.science.outcomes import _resolve_decision_calendar_entry

RESEARCH_PREDICTION_TARGET_DEFINITION_V1 = {
    "version": "ResearchPredictionTargetDefinitionV1",
    "target": "log_return", "unit": "LOG_RETURN",
    "origin": "LAST_CAUSAL_BASELINE_INPUT_CLOSE",
    "horizon_end": "ORIGIN_CLOSE_BOUNDARY_PLUS_REQUESTED_HORIZON",
    "origin_revision": "LATEST_ORIGINAL_CUTOFF_VISIBLE_MATCHING_SEALED_INPUT",
    "horizon_revision": "FIRST_ACTUAL_FINAL_PUBLICATION",
    "interval": "15M", "availability_class": "ACTUAL_SYSTEM",
    "allowed_horizons_ns": [900_000_000_000, 3_600_000_000_000, 14_400_000_000_000],
    "missing_bar": "UNRESOLVED_NO_INTERPOLATION_RETRY_WITH_ORIGINAL_CUTOFF",
    "later_revision": "PRESERVE_FIRST_MEASURED_LABEL_AND_EXACT_SOURCE_REFERENCES",
    "evaluation": "PRICE_PREDICTION_DIAGNOSTIC_ONLY",
    "arithmetic": "DECIMAL_NATURAL_LOG_PRECISION_34",
    "authority": "ZERO",
}
RESEARCH_PREDICTION_TARGET_REF_V1 = sha256_json(RESEARCH_PREDICTION_TARGET_DEFINITION_V1)
MAINTENANCE_INTERVAL_NS_V1 = 60_000_000_000
MAX_PREDICTION_TERMINALS_PER_CYCLE_V1 = 8
MAX_PREDICTION_HORIZONS_PER_CYCLE_V1 = 8


class _PredictionPublicationPending(Exception):
    def __init__(self, available_at_ns: int):
        self.available_at_ns = available_at_ns


@dataclass(frozen=True)
class ResearchPredictionOutcomeV1:
    prediction_id: str
    run_id: str
    config_hash: str
    terminal_ref: str
    request_ref: str
    route_ref: str
    model_manifest_ref: str
    decision_calendar_ref: str | None
    action_artifact_ref: str | None
    action_hash: str | None
    target_definition_ref: str
    instrument_key: InstrumentKeyV2
    information_cutoff_ns: int
    origin_close_at_ns: int | None
    horizon_ns: int
    horizon_end_ns: int | None
    label_state: str
    reason_code: str | None
    measured_log_return: Decimal | None
    measured_origin_close: Decimal | None
    measured_horizon_close: Decimal | None
    source_refs: tuple[str, ...]
    forecast_values_ref: str | None
    measurement_ref: str | None
    available_at_ns: int

    def __post_init__(self) -> None:
        for name in ("prediction_id", "config_hash", "terminal_ref", "request_ref", "route_ref",
                     "model_manifest_ref", "target_definition_ref"):
            sha256_ref(getattr(self, name), field=name)
        for name in ("decision_calendar_ref", "action_artifact_ref", "action_hash", "forecast_values_ref", "measurement_ref"):
            value = getattr(self, name)
            if value is not None:
                sha256_ref(value, field=name)
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,96}", self.run_id):
            raise ValueError("prediction run identity must be bounded")
        if not isinstance(self.instrument_key, InstrumentKeyV2):
            raise TypeError("prediction requires its full instrument identity")
        timestamp(self.information_cutoff_ns, field="information_cutoff_ns")
        timestamp(self.available_at_ns, field="available_at_ns")
        if self.available_at_ns < self.information_cutoff_ns:
            raise ValueError("prediction label cannot predate its original cutoff")
        if type(self.horizon_ns) is not int or self.horizon_ns <= 0:
            raise ValueError("prediction horizon must be positive")
        if (self.origin_close_at_ns is None) != (self.horizon_end_ns is None):
            raise ValueError("prediction target boundaries must be present together")
        if self.origin_close_at_ns is not None:
            timestamp(self.origin_close_at_ns, field="origin_close_at_ns")
            timestamp(self.horizon_end_ns if self.horizon_end_ns is not None else 0, field="horizon_end_ns")
            if (self.origin_close_at_ns > self.information_cutoff_ns
                    or self.origin_close_at_ns % BarIntervalV2.M15.duration_ns
                    or self.horizon_end_ns != self.origin_close_at_ns + self.horizon_ns):
                raise ValueError("prediction horizon must bind its exact causal input close boundary")
        if self.label_state not in {"UNRESOLVED", "MATURED"}:
            raise ValueError("prediction label state is unsupported")
        if self.reason_code is not None and not re.fullmatch(r"[A-Z][A-Z0-9_]{0,95}", self.reason_code):
            raise ValueError("prediction reason must be sanitized")
        if self.source_refs != tuple(sorted(set(self.source_refs))) or len(self.source_refs) > 2:
            raise ValueError("prediction source refs must be bounded, sorted and unique")
        for ref in self.source_refs:
            sha256_ref(ref, field="prediction source ref")
        for name in ("measured_log_return", "measured_origin_close", "measured_horizon_close"):
            value = getattr(self, name)
            if value is not None:
                number = Decimal(value)
                if not number.is_finite():
                    raise ValueError("prediction measured values must be finite")
                object.__setattr__(self, name, number)
        if self.label_state == "MATURED":
            if (self.reason_code is not None or self.horizon_end_ns is None
                    or self.available_at_ns < self.horizon_end_ns or len(self.source_refs) != 2
                    or self.forecast_values_ref is None or self.measurement_ref is None
                    or any(getattr(self, name) is None for name in (
                        "measured_log_return", "measured_origin_close", "measured_horizon_close"))):
                raise ValueError("matured prediction requires measured values and exact source evidence")
            assert self.measured_origin_close is not None and self.measured_horizon_close is not None
            if min(self.measured_origin_close, self.measured_horizon_close) <= 0:
                raise ValueError("prediction close prices must be positive")
            if self.measured_log_return != _log_return(self.measured_origin_close, self.measured_horizon_close):
                raise ValueError("prediction measured log return does not reproduce its prices")
        elif (self.reason_code is None or self.source_refs or self.measurement_ref is not None
              or any(getattr(self, name) is not None for name in (
                  "measured_log_return", "measured_origin_close", "measured_horizon_close"))):
            raise ValueError("unresolved prediction cannot claim measured outcome values")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": "ResearchPredictionOutcomeV1", "authority": "ZERO",
                           **{name: getattr(self, name) for name in self.__dataclass_fields__},
                           "instrument_key": self.instrument_key.to_dict()})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ResearchPredictionOutcomeV1:
        names = set(cls.__dataclass_fields__)
        row = dict(strict_fields(data, expected=names | {"version", "authority"},
                                required=names | {"version", "authority"}, name=cls.__name__))
        if row.pop("version") != cls.__name__ or row.pop("authority") != "ZERO":
            raise ValueError("prediction label version or authority is invalid")
        row["instrument_key"] = InstrumentKeyV2.from_dict(row["instrument_key"])
        if not isinstance(row["source_refs"], list):
            raise ValueError("prediction source refs require a wire list")
        row["source_refs"] = tuple(row["source_refs"])
        return cls(**row)


def _log_return(origin: Decimal, horizon: Decimal) -> Decimal:
    with localcontext() as context:
        context.prec = 34
        return (horizon / origin).ln()


def _body(repo: OpsRepository, ref: str, kind: str, key: str, cutoff: int) -> Mapping[str, Any]:
    entry = repo.get_artifact(ref)
    body = entry.metadata.get(key) if entry is not None else None
    if (entry is None or entry.artifact_type != kind or entry.content_hash != ref
            or entry.artifact_ref != ref or entry.available_at_ns > cutoff
            or not isinstance(body, Mapping) or sha256_json(body) != ref):
        raise ValueError("prediction producer evidence identity or availability is invalid")
    return body


def _producer(repo: OpsRepository, terminal_ref: str, cutoff: int
              ) -> tuple[Mapping[str, Any], Mapping[str, Any], WorkerRequestV2]:
    terminal = _body(repo, terminal_ref, "ResearchModelTerminalV1", "routing", cutoff)
    seal = _body(repo, terminal["request_ref"], "ResearchModelRequestV1", "routing", cutoff)
    packet = WorkerRequestV2.from_dict(json_value(seal["worker_packet"]))
    if (packet.content_hash != seal["worker_packet_hash"]
            or packet.request.input_hash != terminal["input_hash"]
            or packet.manifest.manifest_hash != terminal["manifest_hash"]
            or packet.request.request_id != terminal["request_id"]
            or any(terminal.get(name) != seal.get(name) for name in (
                "run_id", "config_hash", "route_ref", "action_artifact_ref", "action_hash",
                "decision_calendar_ref", "decision_event_ref", "derived_input_ref"))
            or seal.get("authority") != "ZERO" or terminal.get("authority") != "ZERO"):
        raise ValueError("prediction terminal does not bind its exact request")
    registry = _body(repo, seal["registry_ref"], "ResearchModelRoutingRegistryV1", "routing", cutoff)
    if (registry.get("run_id") != terminal["run_id"] or registry.get("config_hash") != terminal["config_hash"]
            or not any(sha256_json(route) == seal["route_ref"]
                       and route.get("manifest_hash") == packet.manifest.manifest_hash
                       and route.get("provider_key") == seal.get("provider_key") for route in registry["routes"])):
        raise ValueError("prediction route differs from its immutable run registry")
    if seal.get("decision_calendar_ref") is not None:
        calendar = _resolve_decision_calendar_entry(repo, seal["decision_calendar_ref"])
        if (calendar.decision_at_ns != packet.request.information_cutoff_ns
                or calendar.action_artifact_ref != seal.get("action_artifact_ref")
                or calendar.action_hash != seal.get("action_hash") or calendar.available_at_ns > cutoff):
            raise ValueError("prediction calendar does not bind its exact action and origin")
    if seal.get("action_artifact_ref") is not None:
        action = _body(repo, seal["action_artifact_ref"], "ActionArtifactV2", "action_artifact", cutoff)
        action_entry = repo.get_artifact(seal["action_artifact_ref"])
        identity = action_entry.metadata.get("action_identity") if action_entry else None
        if (not isinstance(identity, Mapping) or sha256_json(identity) != seal["action_hash"]
                or action.get("action_hash") != seal["action_hash"]
                or canonical_json(identity.get("key")) != canonical_json(packet.request.instrument_key.to_dict())):
            raise ValueError("prediction action identity is invalid")
    return terminal, seal, packet


def validate_research_prediction_outcome_v1(repo: OpsRepository, item: ResearchPredictionOutcomeV1) -> None:
    """Validate compact evidence through a read-only repository-compatible port."""
    terminal, _seal, packet = _producer(repo, item.terminal_ref, item.available_at_ns)
    request = packet.request
    if (item.prediction_id != sha256_json({"version": "ResearchPredictionIdentityV1",
            "terminal_ref": item.terminal_ref, "target_definition_ref": item.target_definition_ref,
            "horizon_ns": item.horizon_ns}) or item.target_definition_ref != RESEARCH_PREDICTION_TARGET_REF_V1
            or item.run_id != terminal["run_id"] or item.config_hash != terminal["config_hash"]
            or item.request_ref != terminal["request_ref"] or item.route_ref != terminal["route_ref"]
            or item.model_manifest_ref != request.model_manifest_hash or item.instrument_key != request.instrument_key
            or item.information_cutoff_ns != request.information_cutoff_ns
            or item.horizon_ns not in request.requested_horizons
            or any(getattr(item, name) != terminal.get(name) for name in (
                "action_artifact_ref", "action_hash", "decision_calendar_ref", "forecast_values_ref")
                   if name != "forecast_values_ref")
            or item.forecast_values_ref != terminal.get("values_evidence_ref")):
        raise ValueError("prediction label differs from its immutable model request")
    try:
        inputs = BaselineInputsV2.from_dict(json_value(packet.inputs))
    except (ValueError, TypeError, KeyError):
        inputs = None
    if inputs is not None:
        if (inputs.content_hash != request.input_hash or inputs.instrument_key != request.instrument_key
                or inputs.information_cutoff_ns != request.information_cutoff_ns):
            raise ValueError("prediction input ABI does not bind its exact request")
        origin = inputs.closes[-1] if inputs.closes else None
        if item.origin_close_at_ns != (origin.close_at_ns if origin is not None else None):
            raise ValueError("prediction target origin differs from its sealed baseline input")
    if item.label_state != "MATURED":
        return
    declaration = _body(repo, item.target_definition_ref, "ResearchPredictionTargetDefinitionV1",
                        "target_definition", request.information_cutoff_ns)
    if canonical_json(declaration) != canonical_json(RESEARCH_PREDICTION_TARGET_DEFINITION_V1):
        raise ValueError("prediction target was not predeclared exactly")
    if not terminal.get("usable") or inputs is None or not inputs.closes:
        raise ValueError("unsupported model terminal cannot receive a measured prediction label")
    if item.horizon_end_ns is None or item.horizon_end_ns <= item.information_cutoff_ns:
        raise ValueError("prediction horizon already elapsed before its original model cutoff")
    forecast = _body(repo, terminal["forecast_ref"], "ResearchModelForecastV1", "routing", item.available_at_ns)
    typed_forecast = ForecastArtifactV2.from_dict(json_value(forecast["forecast"]))
    if (forecast.get("request_ref") != item.request_ref
            or forecast.get("values_evidence_ref") != item.forecast_values_ref
            or typed_forecast.request_id != request.request_id
            or typed_forecast.input_hash != request.input_hash
            or typed_forecast.model_manifest_hash != request.model_manifest_hash
            or typed_forecast.values_ref != item.forecast_values_ref
            or not typed_forecast.is_usable_at(request.deadline_ns)):
        raise ValueError("prediction forecast was unavailable, late or bound to another request")
    values_entry = repo.get_artifact(item.forecast_values_ref or "")
    values = values_entry.metadata.get("model_values") if values_entry is not None else None
    if (values_entry is None or values_entry.artifact_type != "ResearchModelValuesV1"
            or values_entry.content_hash != item.forecast_values_ref or not isinstance(values, Mapping)
            or sha256_json(values) != item.forecast_values_ref or values_entry.available_at_ns > item.available_at_ns
            or f"log_return:{item.horizon_ns}:mean" not in values):
        raise ValueError("prediction numerical forecast evidence is missing")
    if item.measured_origin_close != inputs.closes[-1].close:
        raise ValueError("prediction origin price changed after the causal input was sealed")
    expected_boundaries = {item.origin_close_at_ns, item.horizon_end_ns}
    actual_boundaries = set()
    for ref in item.source_refs:
        entry = repo.get_artifact(ref)
        metadata = entry.metadata if entry is not None else {}
        if (entry is None or entry.artifact_type != "PublicObservationIndexV2"
                or entry.available_at_ns > item.available_at_ns
                or metadata.get("instrument_key_json") != item.instrument_key.to_canonical_json()
                or metadata.get("instrument_revision") != item.instrument_key.contract_revision
                or metadata.get("event_type") != "BAR_15M"
                or metadata.get("availability_class") != "ACTUAL_SYSTEM"
                or metadata.get("event_at_ns") not in expected_boundaries
                or not isinstance(metadata.get("bar_content_hash"), str)):
            raise ValueError("prediction source identity, revision or availability is invalid")
        boundary = metadata["event_at_ns"]
        if boundary == item.origin_close_at_ns and entry.available_at_ns > item.information_cutoff_ns:
            raise ValueError("prediction origin source became available after the fixed model cutoff")
        actual_boundaries.add(boundary)
    if actual_boundaries != expected_boundaries:
        raise ValueError("prediction source endpoints are incomplete")
    measurement = _body(repo, item.measurement_ref or "", "ResearchPredictionMeasurementV1",
                        "measurement", item.available_at_ns)
    if (measurement.get("terminal_ref") != item.terminal_ref
            or measurement.get("prediction_id") != item.prediction_id
            or measurement.get("target_definition_ref") != item.target_definition_ref
            or tuple(measurement.get("source_refs", ())) != item.source_refs
            or measurement.get("authority") != "ZERO"):
        raise ValueError("prediction measured endpoint evidence has a different identity")
    for name, expected_boundary, expected_price in (
            ("origin_bar", item.origin_close_at_ns, item.measured_origin_close),
            ("horizon_bar", item.horizon_end_ns, item.measured_horizon_close)):
        bar = measurement[name]
        if not isinstance(bar, Mapping):
            raise ValueError("prediction measurement bar is not an object")
        source_ref = measurement[name + "_source_ref"]
        entry = repo.get_artifact(source_ref)
        if (source_ref not in item.source_refs or entry is None
                or entry.metadata.get("bar_content_hash") != sha256_json(bar)
                or entry.metadata.get("record_id") != bar.get("record_id")
                or entry.metadata.get("raw_payload_hash") != bar.get("raw_payload_hash")
                or bar.get("final") is not True
                or bar.get("close_at_ns") != expected_boundary
                or Decimal(str(bar.get("close"))) != expected_price
                or entry.available_at_ns > item.available_at_ns
                or bar.get("interval") != BarIntervalV2.M15.value
                or bar.get("instrument_revision") != item.instrument_key.contract_revision
                or entry.metadata.get("availability_class") != AvailabilityClassV2.ACTUAL_SYSTEM.value):
            raise ValueError("prediction endpoint prices do not bind their exact indexed canonical bars")


def index_research_prediction_outcome_v1(repo: OpsRepository, item: ResearchPredictionOutcomeV1) -> str:
    validate_research_prediction_outcome_v1(repo, item)
    ref = item.content_hash
    prior = repo.get_artifact(ref)
    metadata = {"prediction_outcome": item.to_dict()}
    if prior is None:
        repo.register_artifact(ArtifactIndexEntryV2(ref, "ResearchPredictionOutcomeV1", ref,
            item.available_at_ns, item.available_at_ns, metadata))
    elif prior.artifact_type != "ResearchPredictionOutcomeV1" or canonical_json(prior.metadata) != canonical_json(metadata):
        raise ValueError("prediction label conflicts with immutable persisted evidence")
    return ref


class ResearchPredictionOutcomeMaintenanceV1:
    """Indexed due horizons, bounded reads and restart-safe immutable labels."""

    def __init__(self, *, run_id: str, config_hash: str, archive_root: str | Path,
                 clock_ns: Callable[[], int] = time.time_ns,
                 bar_reader: Callable[..., tuple[IndexedCausalBarV2, ...]] = reconstruct_indexed_causal_bars_v1):
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,96}", run_id):
            raise ValueError("prediction maintenance run identity must be bounded")
        sha256_ref(config_hash, field="config_hash")
        self.run_id, self.config_hash = run_id, config_hash
        self.archive_root, self.clock_ns, self.bar_reader = Path(archive_root), clock_ns, bar_reader

    def register_target(self, repo: OpsRepository, *, available_at_ns: int) -> str:
        timestamp(available_at_ns, field="target available_at_ns")
        if repo.read_only:
            raise ValueError("prediction target publication belongs to the controller writer")
        ref = RESEARCH_PREDICTION_TARGET_REF_V1
        prior = repo.get_artifact(ref)
        metadata = {"target_definition": RESEARCH_PREDICTION_TARGET_DEFINITION_V1}
        if prior is None:
            repo.register_artifact(ArtifactIndexEntryV2(ref, "ResearchPredictionTargetDefinitionV1", ref,
                available_at_ns, available_at_ns, metadata))
        elif (prior.artifact_type != "ResearchPredictionTargetDefinitionV1"
              or prior.content_hash != ref or canonical_json(prior.metadata) != canonical_json(metadata)
              or prior.available_at_ns > available_at_ns):
            raise ValueError("prediction target definition conflicts with its immutable declaration")
        return ref

    def _read_bar(self, repo: OpsRepository, key: InstrumentKeyV2, boundary: int, cutoff: int,
                  *, first: bool) -> IndexedCausalBarV2 | None:
        entries = repo.confirmed_bar_observation_entries(key, event_type="BAR_15M", close_at_ns=boundary,
                                                        as_of_ns=cutoff, limit=128)
        bars = self.bar_reader(repo, self.archive_root, key=key, interval=BarIntervalV2.M15,
                              index_entries=entries, information_cutoff_ns=cutoff)
        if len(bars) != len(entries):
            raise ValueError("prediction exact source reconstruction is incomplete")
        for indexed in bars:
            bar = indexed.bar
            entry = repo.get_artifact(indexed.observation_index_ref)
            if (entry is None or bar.close_at_ns != boundary or not bar.final
                    or bar.interval != BarIntervalV2.M15 or bar.instrument_revision != key.contract_revision
                    or bar.raw.availability_class != AvailabilityClassV2.ACTUAL_SYSTEM
                    or bar.raw.available_at_ns > cutoff or entry.available_at_ns > cutoff
                    or not indexed_availability_matches(bar.raw.available_at_ns, entry.available_at_ns, entry.metadata)
                    or entry.content_hash != bar.raw.content_hash
                    or entry.metadata.get("instrument_key_json") != key.to_canonical_json()
                    or entry.metadata.get("bar_content_hash") != bar.content_hash):
                raise ValueError("prediction source reconstruction has wrong revision or causal availability")
        ordered = sorted(bars, key=lambda row: (row.bar.raw.available_at_ns, row.bar.raw.record_id))
        return (ordered[0] if first else ordered[-1]) if ordered else None

    def _measure(self, repo: OpsRepository, terminal_ref: str, horizon: int, cutoff: int) -> ResearchPredictionOutcomeV1:
        terminal, _seal, packet = _producer(repo, terminal_ref, cutoff)
        request = packet.request
        if terminal["run_id"] != self.run_id or terminal["config_hash"] != self.config_hash:
            raise ValueError("prediction maintenance cannot switch immutable runs")
        try:
            inputs = BaselineInputsV2.from_dict(json_value(packet.inputs))
            origin = inputs.closes[-1] if inputs.closes else None
        except (KeyError, TypeError, ValueError):
            origin = None
        prediction = ResearchPredictionOutcomeV1(
            prediction_id=sha256_json({"version": "ResearchPredictionIdentityV1", "terminal_ref": terminal_ref,
                "target_definition_ref": RESEARCH_PREDICTION_TARGET_REF_V1, "horizon_ns": horizon}),
            run_id=self.run_id, config_hash=self.config_hash, terminal_ref=terminal_ref,
            request_ref=terminal["request_ref"], route_ref=terminal["route_ref"],
            model_manifest_ref=request.model_manifest_hash, decision_calendar_ref=terminal.get("decision_calendar_ref"),
            action_artifact_ref=terminal.get("action_artifact_ref"), action_hash=terminal.get("action_hash"),
            target_definition_ref=RESEARCH_PREDICTION_TARGET_REF_V1, instrument_key=request.instrument_key,
            information_cutoff_ns=request.information_cutoff_ns, origin_close_at_ns=origin.close_at_ns if origin else None,
            horizon_ns=horizon, horizon_end_ns=origin.close_at_ns + horizon if origin else None,
            label_state="UNRESOLVED", reason_code="PREDICTION_TARGET_ABI_UNSUPPORTED",
            measured_log_return=None, measured_origin_close=None, measured_horizon_close=None,
            source_refs=(), forecast_values_ref=terminal.get("values_evidence_ref"), measurement_ref=None,
            available_at_ns=self.clock_ns())
        if origin is None or request.requested_targets != ("log_return",):
            return prediction
        if horizon not in RESEARCH_PREDICTION_TARGET_DEFINITION_V1["allowed_horizons_ns"]:
            return replace(prediction, reason_code="PREDICTION_HORIZON_UNSUPPORTED")
        if prediction.horizon_end_ns is not None and prediction.horizon_end_ns <= request.information_cutoff_ns:
            return replace(prediction, reason_code="PREDICTION_TARGET_ALREADY_ELAPSED_AT_CUTOFF")
        declaration = repo.get_artifact(RESEARCH_PREDICTION_TARGET_REF_V1)
        if declaration is None or declaration.available_at_ns > request.information_cutoff_ns:
            return replace(prediction, reason_code="PREDICTION_TARGET_NOT_PREDECLARED")
        if not terminal.get("usable"):
            return replace(prediction, reason_code="PREDICTION_MODEL_TERMINAL_UNUSABLE")
        if prediction.horizon_end_ns is not None and cutoff < prediction.horizon_end_ns:
            return replace(prediction, reason_code="PREDICTION_HORIZON_PENDING")
        try:
            start = self._read_bar(repo, request.instrument_key, origin.close_at_ns,
                                   request.information_cutoff_ns, first=False)
            if start is None:
                return replace(prediction, reason_code="PREDICTION_CAUSAL_ORIGIN_BAR_MISSING")
            if start.bar.close != origin.close:
                return replace(prediction, reason_code="PREDICTION_CAUSAL_ORIGIN_REVISION_CONFLICT")
            end = self._read_bar(repo, request.instrument_key, origin.close_at_ns + horizon, cutoff, first=True)
            if end is None:
                return replace(prediction, reason_code="PREDICTION_FINAL_HORIZON_BAR_MISSING")
            measurement = {"version": "ResearchPredictionMeasurementV1", "prediction_id": prediction.prediction_id,
                "terminal_ref": terminal_ref, "target_definition_ref": RESEARCH_PREDICTION_TARGET_REF_V1,
                "origin_bar": start.bar.to_dict(), "horizon_bar": end.bar.to_dict(),
                "origin_bar_source_ref": start.observation_index_ref,
                "horizon_bar_source_ref": end.observation_index_ref,
                "source_refs": sorted((start.observation_index_ref, end.observation_index_ref)), "authority": "ZERO"}
            measurement_ref = sha256_json(measurement)
            measured_at_ns = max(prediction.available_at_ns, self.clock_ns())
            if repo.get_artifact(measurement_ref) is None:
                repo.register_artifact(ArtifactIndexEntryV2(measurement_ref, "ResearchPredictionMeasurementV1",
                    measurement_ref, measured_at_ns, measured_at_ns, {"measurement": measurement}))
            return replace(prediction, label_state="MATURED", reason_code=None,
                measured_log_return=_log_return(start.bar.close, end.bar.close),
                measured_origin_close=start.bar.close, measured_horizon_close=end.bar.close,
                source_refs=tuple(sorted((start.observation_index_ref, end.observation_index_ref))),
                measurement_ref=measurement_ref, available_at_ns=max(measured_at_ns, self.clock_ns()))
        except (OSError, ValueError, KeyError, TypeError, ArithmeticError):
            return replace(prediction, reason_code="PREDICTION_SOURCE_INVALID_OR_REVISION_BOUND_EXCEEDED",
                           available_at_ns=max(prediction.available_at_ns, self.clock_ns()))

    def run_cycle(self, repo: OpsRepository, *, evidence_cutoff_ns: int) -> Mapping[str, Any]:
        cutoff = timestamp(evidence_cutoff_ns, field="evidence_cutoff_ns")
        terminal_lane = prediction_due_lane_v1("PREDICTION_TERMINAL", self.run_id, self.config_hash)
        outcome_lane = prediction_due_lane_v1("PREDICTION_OUTCOME", self.run_id, self.config_hash)
        if repo.read_only:
            raise ValueError("prediction maintenance uses the existing controller writer")
        page = repo.artifact_entries_by_metadata_identity("ResearchPredictionOutcomeCheckpointV1",
            ("checkpoint", "run_id"), self.run_id, as_of_ns=cutoff, limit=1)
        if page.invalid_entry_count:
            raise ValueError("prediction maintenance checkpoint index is corrupt")
        previous = page.entries[0] if page.entries else None
        checkpoint = previous.metadata.get("checkpoint") if previous else None
        if previous is not None and (not isinstance(checkpoint, Mapping)
                or sha256_json(checkpoint) != previous.content_hash
                or checkpoint.get("config_hash") != self.config_hash):
            raise ValueError("prediction maintenance checkpoint identity conflicts")
        if previous is not None and cutoff < previous.available_at_ns + MAINTENANCE_INTERVAL_NS_V1:
            return {"status": "IMPLEMENTED", "horizons_inspected": 0, "labels_written": 0,
                "reason_code": "BOUNDED_INTERVAL", "due_work": repo.due_work_pressure(
                    outcome_lane, as_of_ns=cutoff, page_limit=MAX_PREDICTION_HORIZONS_PER_CYCLE_V1)}
        discovery = repo.discover_due_work()
        failure: str | None = None
        diagnostics: list[Mapping[str, Any]] = []
        terminal_count = 0
        terminals = repo.due_work_items(terminal_lane, as_of_ns=cutoff,
            limit=MAX_PREDICTION_TERMINALS_PER_CYCLE_V1)
        remaining = MAX_PREDICTION_TERMINALS_PER_CYCLE_V1 - len(terminals)
        if remaining:
            terminals += repo.due_work_items("PREDICTION_TERMINAL", as_of_ns=cutoff, limit=remaining)
        for work in terminals:
            terminal_count += 1
            try:
                # Route legacy projection rows without judging another run's evidence.
                if work.lane == "PREDICTION_TERMINAL":
                    entry = repo.get_artifact(work.source_ref)
                    routing = entry.metadata.get("routing") if entry is not None else None
                    if (isinstance(routing, Mapping) and isinstance(routing.get("run_id"), str)
                            and isinstance(routing.get("config_hash"), str)):
                        scoped = prediction_due_lane_v1(work.lane, routing["run_id"], routing["config_hash"])
                        repo.enqueue_due_work(lane=scoped, work_id=work.work_id,
                            source_ref=work.source_ref, created_at_ns=work.created_at_ns,
                            due_at_ns=work.due_at_ns, payload=work.payload)
                        repo.retire_due_work(work.lane, work.work_id, reason_code="RUN_LANE_MIGRATED")
                        continue
                terminal, _seal, packet = _producer(repo, work.source_ref, cutoff)
                if terminal["run_id"] != self.run_id or terminal["config_hash"] != self.config_hash:
                    raise ValueError("prediction terminal belongs to another immutable run")
                horizons = packet.request.requested_horizons
                if not 1 <= len(horizons) <= 8:
                    raise ValueError("prediction horizon population exceeds its bound")
                for horizon in horizons:
                    identity = sha256_json({"version": "ResearchPredictionIdentityV1",
                        "terminal_ref": work.source_ref, "target_definition_ref": RESEARCH_PREDICTION_TARGET_REF_V1,
                        "horizon_ns": horizon})
                    repo.enqueue_due_work(lane=outcome_lane, work_id=identity,
                        source_ref=work.source_ref, created_at_ns=work.created_at_ns,
                        due_at_ns=work.due_at_ns, payload={"horizon_ns": horizon})
                repo.retire_due_work(work.lane, work.work_id, reason_code="HORIZONS_ENQUEUED")
            except (ValueError, TypeError, KeyError):
                failure = "PREDICTION_TERMINAL_LINEAGE_INVALID"
                repo.quarantine_due_work(work.lane, work.work_id, reason_code=failure)
                diagnostics.append({"source_ref": work.source_ref, "work_id": work.work_id,
                    "reason_code": failure, "disposition": "QUARANTINED"})
        writes = inspected = 0
        legacy_horizons = repo.due_work_items("PREDICTION_OUTCOME", as_of_ns=cutoff,
            limit=MAX_PREDICTION_HORIZONS_PER_CYCLE_V1)
        for work in legacy_horizons:
            try:
                entry = repo.get_artifact(work.source_ref)
                routing = entry.metadata.get("routing") if entry is not None else None
                if not isinstance(routing, Mapping):
                    raise ValueError("prediction source run identity is missing")
                scoped = prediction_due_lane_v1("PREDICTION_OUTCOME", routing["run_id"], routing["config_hash"])
                repo.enqueue_due_work(lane=scoped, work_id=work.work_id,
                    source_ref=work.source_ref, created_at_ns=work.created_at_ns,
                    due_at_ns=work.due_at_ns, payload=work.payload)
                repo.retire_due_work(work.lane, work.work_id, reason_code="RUN_LANE_MIGRATED")
            except (ValueError, TypeError, KeyError):
                failure = "PREDICTION_FORECAST_LINEAGE_INVALID"
                repo.quarantine_due_work(work.lane, work.work_id, reason_code=failure)
                diagnostics.append({"source_ref": work.source_ref, "work_id": work.work_id,
                    "reason_code": failure, "disposition": "QUARANTINED"})
        terminal_reasons = {
            "PREDICTION_TARGET_ABI_UNSUPPORTED", "PREDICTION_HORIZON_UNSUPPORTED",
            "PREDICTION_TARGET_ALREADY_ELAPSED_AT_CUTOFF", "PREDICTION_TARGET_NOT_PREDECLARED",
            "PREDICTION_MODEL_TERMINAL_UNUSABLE",
        }
        for work in repo.due_work_items(outcome_lane, as_of_ns=cutoff,
                                        limit=MAX_PREDICTION_HORIZONS_PER_CYCLE_V1):
            inspected += 1
            try:
                item, written = self._maintain_due_horizon(repo, work.source_ref, work.payload["horizon_ns"], cutoff)
                writes += written
                if item.reason_code == "PREDICTION_FORECAST_LINEAGE_INVALID":
                    repo.quarantine_due_work(work.lane, work.work_id, reason_code=item.reason_code)
                    failure = item.reason_code
                    diagnostics.append({"source_ref": work.source_ref, "work_id": work.work_id,
                        "reason_code": failure, "disposition": "QUARANTINED"})
                elif item.label_state == "MATURED" or item.reason_code in terminal_reasons:
                    repo.retire_due_work(work.lane, work.work_id,
                        reason_code="MATURED" if item.label_state == "MATURED" else item.reason_code or "UNSUPPORTED")
                else:
                    due = max(cutoff + MAINTENANCE_INTERVAL_NS_V1,
                        item.horizon_end_ns or cutoff) if item.reason_code == "PREDICTION_HORIZON_PENDING" else (
                            cutoff + MAINTENANCE_INTERVAL_NS_V1)
                    repo.reschedule_due_work(work.lane, work.work_id, due_at_ns=due,
                        reason_code=item.reason_code or "PREDICTION_UNRESOLVED")
            except _PredictionPublicationPending as pending:
                repo.reschedule_due_work(work.lane, work.work_id,
                    due_at_ns=max(cutoff + MAINTENANCE_INTERVAL_NS_V1, pending.available_at_ns),
                    reason_code="PREDICTION_PRIOR_PUBLICATION_PENDING")
            except (ValueError, TypeError, KeyError, ArithmeticError):
                failure = "PREDICTION_FORECAST_LINEAGE_INVALID"
                repo.quarantine_due_work(work.lane, work.work_id, reason_code=failure)
                diagnostics.append({"source_ref": work.source_ref, "work_id": work.work_id,
                    "reason_code": failure, "disposition": "QUARANTINED"})
        pressure = repo.due_work_pressure(outcome_lane, as_of_ns=cutoff,
                                         page_limit=MAX_PREDICTION_HORIZONS_PER_CYCLE_V1)
        published = timestamp(self.clock_ns(), field="prediction maintenance publication")
        if published < cutoff:
            raise ValueError("prediction maintenance publication clock precedes its evidence cutoff")
        next_checkpoint = {"version": "ResearchPredictionOutcomeCheckpointV1", "run_id": self.run_id,
            "config_hash": self.config_hash, "generation": checkpoint.get("generation", 0) + 1 if checkpoint else 0,
            "previous_checkpoint_ref": previous.artifact_ref if previous else None, "available_at_ns": published,
            "cursor": None, "current_terminal_ref": None, "horizon_index": 0, "sweep_cutoff_ns": cutoff,
            "method": "INDEXED_DUE_WORK_V1", "discovery": discovery, "due_work": pressure,
            "terminal_due_work": repo.due_work_pressure(terminal_lane, as_of_ns=cutoff,
                page_limit=MAX_PREDICTION_TERMINALS_PER_CYCLE_V1),
            "unscoped_terminal_due_work": repo.due_work_pressure("PREDICTION_TERMINAL", as_of_ns=cutoff,
                page_limit=MAX_PREDICTION_TERMINALS_PER_CYCLE_V1),
            "unscoped_outcome_due_work": repo.due_work_pressure("PREDICTION_OUTCOME", as_of_ns=cutoff,
                page_limit=MAX_PREDICTION_HORIZONS_PER_CYCLE_V1),
            "legacy_horizons_inspected": len(legacy_horizons),
            "diagnostics": diagnostics, "failure_code": failure, "authority": "ZERO"}
        ref = sha256_json(next_checkpoint)
        repo.register_artifact(ArtifactIndexEntryV2(ref, "ResearchPredictionOutcomeCheckpointV1", ref,
            published, published, {"checkpoint": next_checkpoint}))
        return {"status": "IMPLEMENTED", "horizons_inspected": inspected, "labels_written": writes,
            "terminals_inspected": terminal_count, "checkpoint_ref": ref, "due_work": pressure,
            "terminal_due_work": next_checkpoint["terminal_due_work"],
            "unscoped_terminal_due_work": next_checkpoint["unscoped_terminal_due_work"],
            "unscoped_outcome_due_work": next_checkpoint["unscoped_outcome_due_work"],
            "legacy_horizons_inspected": len(legacy_horizons),
            "diagnostics": diagnostics, "discovery": discovery, "failure_code": failure, "authority": "ZERO"}

    def _maintain_due_horizon(self, repo: OpsRepository, terminal_ref: str, horizon: int,
                             cutoff: int) -> tuple[ResearchPredictionOutcomeV1, int]:
        prediction_id = sha256_json({"version": "ResearchPredictionIdentityV1", "terminal_ref": terminal_ref,
            "target_definition_ref": RESEARCH_PREDICTION_TARGET_REF_V1, "horizon_ns": horizon})
        completion_key = sha256_json({"version": "ResearchPredictionCompletionIdentityV1", "prediction_id": prediction_id})
        completed = repo.get_artifact(completion_key)
        if completed is not None:
            if completed.available_at_ns > cutoff:
                raise _PredictionPublicationPending(completed.available_at_ns)
            binding = completed.metadata.get("prediction_completion")
            if (completed.artifact_type != "ResearchPredictionCompletionIdentityV1"
                    or not isinstance(binding, Mapping) or sha256_json(binding) != completed.content_hash
                    or binding.get("prediction_id") != prediction_id or binding.get("authority") != "ZERO"):
                raise ValueError("prediction completed label identity is corrupt")
            label = repo.get_artifact(binding["outcome_ref"])
            if label is None or label.artifact_type != "ResearchPredictionOutcomeV1":
                raise ValueError("prediction completed label evidence is missing")
            item = ResearchPredictionOutcomeV1.from_dict(json_value(label.metadata["prediction_outcome"]))
            validate_research_prediction_outcome_v1(repo, item)
            if (item.label_state != "MATURED" or item.prediction_id != prediction_id
                    or label.artifact_ref != item.content_hash or label.content_hash != item.content_hash
                    or label.created_at_ns != item.available_at_ns or label.available_at_ns != item.available_at_ns
                    or completed.created_at_ns != item.available_at_ns or completed.available_at_ns != item.available_at_ns):
                raise ValueError("prediction completion does not bind a measured label")
            return item, 0
        support_page = repo.artifact_entries_by_metadata_identity("ResearchPredictionOutcomeV1",
            ("prediction_outcome", "prediction_id"), prediction_id, as_of_ns=2**63 - 1, limit=1)
        if support_page.invalid_entry_count:
            raise ValueError("prediction support state index is corrupt")
        # Queue recovery checks physical publication progress without consuming
        # future label contents or remeasuring an already published horizon.
        if support_page.entries and support_page.entries[0].available_at_ns > cutoff:
            raise _PredictionPublicationPending(support_page.entries[0].available_at_ns)
        old_item = ResearchPredictionOutcomeV1.from_dict(json_value(
            support_page.entries[0].metadata["prediction_outcome"])) if support_page.entries else None
        if old_item is not None:
            validate_research_prediction_outcome_v1(repo, old_item)
            entry = support_page.entries[0]
            if (entry.artifact_ref != old_item.content_hash or entry.content_hash != old_item.content_hash
                    or entry.created_at_ns != old_item.available_at_ns or entry.available_at_ns != old_item.available_at_ns):
                raise ValueError("prediction support publication identity is corrupt")
        # A crash after label publication but before completion never remeasures
        # the horizon against a later market revision.
        item = old_item if old_item is not None and old_item.label_state == "MATURED" else self._measure(
            repo, terminal_ref, horizon, cutoff)
        current_support, previous_support = item.to_dict(), old_item.to_dict() if old_item else None
        current_support.pop("available_at_ns")
        if previous_support is not None:
            previous_support.pop("available_at_ns")
        writes = int(old_item is None or current_support != previous_support)
        if writes:
            try:
                ref = index_research_prediction_outcome_v1(repo, item)
            except (ValueError, TypeError, KeyError, ArithmeticError):
                item = replace(item, label_state="UNRESOLVED", reason_code="PREDICTION_FORECAST_LINEAGE_INVALID",
                    measured_log_return=None, measured_origin_close=None, measured_horizon_close=None,
                    source_refs=(), measurement_ref=None)
                ref = index_research_prediction_outcome_v1(repo, item)
        else:
            assert old_item is not None
            item, ref = old_item, old_item.content_hash
        if item.label_state == "MATURED":
            binding = {"version": "ResearchPredictionCompletionIdentityV1", "prediction_id": item.prediction_id,
                "outcome_ref": ref, "authority": "ZERO"}
            repo.register_artifact(ArtifactIndexEntryV2(completion_key, "ResearchPredictionCompletionIdentityV1",
                sha256_json(binding), item.available_at_ns, item.available_at_ns, {"prediction_completion": binding}))
        return item, writes
