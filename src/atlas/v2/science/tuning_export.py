"""Bounded read-only projections of the operational research evidence.

Parquet partitions and reports are disposable analysis products, never another
authority. SQLite insertion order is the incremental cursor: late outcomes and
honestly late receipts cannot disappear behind a decision-time watermark.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sqlite3
import time
import uuid
from collections import Counter
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Any

from atlas.v2._serialization import canonical_json, json_value, sha256_json, sha256_ref, timestamp
from atlas.v2.agent_intelligence.shadow_measurement import (
    ActionCriticShadowObservationV1,
    index_action_critic_shadow_observation,
)
from atlas.v2.data.health import PublicSourceHealthV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.models.baseline import BaselineInputsV2
from atlas.v2.models.protocol import ForecastArtifactV2
from atlas.v2.models.worker_protocol import WorkerRequestV2
from atlas.v2.runtime.s3_native_cadence import find_s3_m1_origin_late_gate
from atlas.v2.science.outcomes import (
    DecisionCalendarEntryV2,
    MaturedOutcomeV2,
    index_decision_calendar_entry,
    index_matured_outcome,
)
from atlas.v2.science.s3_calendar import S3DecisionCalendarMissingnessV1

VERSION = "ATLAS_TUNING_EXPORT_V1"
_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,95}$")
_SAFE_CODE = re.compile(r"^[A-Z][A-Z0-9_ /.-]{0,127}$")
_TYPES = (
    "DecisionCalendarEntryV2", "S3DecisionCalendarMissingnessV1", "MaturedOutcomeV2",
    "OpsSupervisorReceiptV1", "OpsSupervisorCycleReceiptV1", "OpsSupervisorAttemptFailureV1",
    "PublicSourceHealthV2", "PublicStreamSourceHealthEvidenceV1", "PublicStreamContinuityReportV1",
    "BybitPublicRecoveryEvidenceV1", "OutcomeMaturityCycleReportV1", "OutcomeMaturityStatusV1",
    "M0PredictionV2", "M0SupportV2", "M0CalibrationV2", "M1PredictionV2", "M1SupportV2",
    "M1CalibrationV2", "AnalogueActionValueV2", "EvaluationArtifactV2", "OpsZeroAuthorityDiagnosticV1",
    "ActionCriticShadowObservationV1", "ActionCriticShadowMaturityLinkV1",
    "S3NativeWarmupReadinessV1", "OpsRecoveryEpochV1", "ResearchResourceSampleV1", "ResearchRunTelemetryV1",
    "ResearchModelRequestV1", "ResearchModelForecastV1", "ResearchModelTerminalV1",
    "ResearchModelValuesV1", "ResearchModelRoutingRegistryV1", "ResearchModelShadowDiagnosticV1",
    "ResearchPredictionOutcomeV1",
)
_METRIC_NAMES = frozenset({
    "latency_ns", "dispatch_to_result_latency_ns", "queue_items", "queue_bytes", "high_water_items",
    "high_water_bytes", "frames_rejected", "gap_count", "observed_trade_count", "recovery_epoch",
    "duration_ns", "acquisition_duration_ns", "training_sample_count", "independent_support_count",
    "net_mean", "lcb", "predicted_value", "prediction", "mean", "effective_sample_size",
    "independent_episode_count", "net_payoff", "fees", "funding_cashflow", "gross_payoff",
    "es_before", "es_after", "cpu_percent", "rss_bytes", "handle_count", "free_disk_bytes",
    "sqlite_bytes", "wal_bytes", "archive_bytes", "deadline_missed", "overflowed", "source_current",
    "book_sequence_valid", "trade_completeness_proven", "matured_count", "pending_count",
    "unsupported_count", "outcomes_indexed", "budget_elapsed_ns", "budget_exceeded",
    "expected_net_value", "estimation_uncertainty", "numerical_conversion_error",
    "maintenance_budget_overrun_ns", "oldest_pending_age_ns", "oldest_maturable_age_ns",
    "db_bytes", "handles", "peak_rss_bytes", "process_cpu_seconds", "cpu_seconds", "threads", "disk_free_bytes",
    "queue_wait_ns", "run_elapsed_ns", "inference_started_ns", "completed_ns", "received_ns", "expires_ns",
    "measured_log_return", "predicted_log_return", "absolute_prediction_error", "squared_prediction_error",
    "quantile_interval_covered", "prediction_error",
})
_IDENTITY_NAMES = frozenset({
    "event_id", "decision_event_id", "decision_ref", "decision_calendar_ref", "candidate_ref",
    "candidate_set_ref", "action_hash", "action_artifact_ref", "policy_hash", "policy_id",
    "provider_profile_hash", "model_profile_hash", "config_hash", "origin_ref", "source_id",
    "epoch_id",
    "request_ref", "route_ref", "provider_key", "input_hash", "forecast_ref", "values_evidence_ref",
})


@dataclass(frozen=True)
class TuningRunIdentityV1:
    run_id: str
    config_hash: str
    source_sha: str
    started_at_ns: int

    def __post_init__(self) -> None:
        if not isinstance(self.run_id, str) or not _SAFE_NAME.fullmatch(self.run_id):
            raise ValueError("run identity must be a bounded filesystem-safe name")
        sha256_ref(self.config_hash, field="run config_hash")
        if not re.fullmatch(r"[0-9a-f]{40}", self.source_sha):
            raise ValueError("run source_sha must be an exact Git SHA")
        timestamp(self.started_at_ns, field="run started_at_ns")

    def to_dict(self) -> dict[str, Any]:
        return {"version": "TUNING_RUN_IDENTITY_V1", **self.__dict__}


class TuningExportBudgetExceeded(RuntimeError):
    """No manifest/checkpoint is advanced when snapshot work exceeds its budget."""


class _ValidationReader(OpsRepository):
    """Reuse existing full index validators while prohibiting every database write."""

    def register_artifact(self, entry: ArtifactIndexEntryV2) -> ArtifactIndexEntryV2:
        prior = self.get_artifact(entry.artifact_ref)
        if prior != entry:
            raise ValueError("analysis validator requires the exact already-persisted artifact")
        return prior


@contextmanager
def _export_lock(path: Path) -> Iterator[None]:
    with path.open("a+b") as handle:
        if os.name == "nt":
            import msvcrt

            windows_locks: Any = msvcrt
            if handle.seek(0, os.SEEK_END) == 0:
                handle.write(b"0")
                handle.flush()
            handle.seek(0)
            windows_locks.locking(handle.fileno(), windows_locks.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            if os.name == "nt":
                handle.seek(0)
                windows_locks.locking(handle.fileno(), windows_locks.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _immutable_json(path: Path, value: Mapping[str, Any]) -> None:
    payload = (canonical_json(value) + "\n").encode()
    if path.exists():
        if path.read_bytes() != payload:
            raise ValueError("immutable export identity conflicts with existing bytes")
        return
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _compact_values(body: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, str], list[str], list[str]]:
    """Closed field projection: never copy prompts, API keys, responses or raw payloads."""
    metrics: dict[str, Any] = {}
    identities: dict[str, str] = {}
    reasons: set[str] = set()
    refs: set[str] = set()
    pending: list[tuple[str, Mapping[str, Any], int]] = [("", body, 0)]
    visited = 0
    while pending:
        prefix, node, depth = pending.pop()
        visited += 1
        if visited > 256:
            raise ValueError("compact evidence exceeds its structural bound")
        for key, value in node.items():
            if key in _IDENTITY_NAMES and isinstance(value, str):
                if len(value) <= 128 and re.fullmatch(r"[A-Za-z0-9_-]+", value):
                    identities.setdefault(key, value)
            if key.endswith("_ref") and isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value):
                refs.add(value)
            if key in {"input_refs", "evidence_refs", "artifact_refs", "causal_input_refs"} and isinstance(value, list):
                refs.update(item for item in value if isinstance(item, str) and re.fullmatch(r"[0-9a-f]{64}", item))
            if key in {"reason_code", "terminal_reason_code", "reason", "status", "state", "terminal_status",
                       "missing_or_blocking_reason", "source_health_state", "source_health", "failure_code", "maintenance_budget_status",
                       "critic_terminal_status", "selection_state", "admission_state", "source_stage", "decision"}:
                if isinstance(value, str) and _SAFE_CODE.fullmatch(value):
                    reasons.add(value)
            if key in {"reason_codes", "reasons"} and isinstance(value, list):
                reasons.update(item for item in value if isinstance(item, str) and _SAFE_CODE.fullmatch(item))
            if key in _METRIC_NAMES:
                if (value is None or isinstance(value, (bool, int))
                        or (isinstance(value, float) and math.isfinite(value))
                        or isinstance(value, str) and re.fullmatch(r"-?\d+(?:\.\d+)?", value)):
                    metrics[prefix + key] = value
            if key == "stage_terminal_statuses" and isinstance(value, Mapping):
                for stage, state in value.items():
                    if isinstance(state, str) and _SAFE_CODE.fullmatch(state):
                        metrics[prefix + "stage_terminal_statuses." + stage] = state
            if isinstance(value, Mapping) and depth < 4:
                pending.append((prefix + str(key) + ".", value, depth + 1))
            elif key == "stages" and isinstance(value, list) and depth < 4:
                for index, stage in enumerate(value[:16]):
                    if isinstance(stage, Mapping):
                        pending.append((prefix + f"stages.{index}.", stage, depth + 1))
    return metrics, identities, sorted(reasons), sorted(refs)


def _validated_row(repository: OpsRepository, entry: ArtifactIndexEntryV2) -> dict[str, Any]:
    row: dict[str, Any] = {
        "artifact_ref": entry.artifact_ref, "artifact_type": entry.artifact_type,
        "created_at_ns": entry.created_at_ns, "available_at_ns": entry.available_at_ns,
        "row_kind": "EVIDENCE", "decision_at_ns": None, "decision_ref": None,
        "event_id": None, "candidate_set_ref": None, "candidate_ref": None, "action_hash": None,
        "policy_id": None, "policy_hash": None, "origin_ref": None,
        "selection_state": None, "admission_state": None, "outcome_target": None,
        "label_state": None, "provenance": None, "net_payoff": None,
        "provider_profile_hash": None, "model_profile_hash": None, "status": None,
        "source_stage": None, "instrument_key_json": None, "epoch_id": None,
        "request_ref": None, "route_ref": None, "provider_key": None, "input_hash": None,
        "forecast_ref": None, "values_evidence_ref": None, "model_values_json": None,
        "model_sample_counts_json": None,
        "model_missing_outputs_json": None,
        "prediction_id": None, "prediction_target_ref": None, "horizon_ns": None, "horizon_end_ns": None,
        "measured_log_return": None, "predicted_log_return": None,
    }
    body = json_value(entry.metadata)
    if entry.artifact_type.startswith("ResearchModel"):
        body = _model_projection(repository, entry, row)
    if entry.artifact_type == "ResearchPredictionOutcomeV1":
        body = _prediction_projection(repository, entry, row)
    elif entry.artifact_type == "DecisionCalendarEntryV2":
        decision = DecisionCalendarEntryV2.from_dict(body["decision_entry"])
        if index_decision_calendar_entry(repository, decision) != entry.artifact_ref:
            raise ValueError("calendar content identity mismatch")
        body = decision.to_dict()
        row.update(row_kind="DECISION", decision_ref=entry.artifact_ref,
                   decision_at_ns=decision.decision_at_ns, selection_state=decision.selection_state.value,
                   admission_state=decision.admission_state.value, source_stage=decision.source_stage.value)
        candidate_set = repository.get_artifact(decision.candidate_set_ref)
        if candidate_set is None:
            raise ValueError("decision lost its exact CandidateSet")
        row["event_id"] = candidate_set.metadata["candidate_set"]["decision_event_id"]
        if decision.candidate_ref is not None:
            candidate = repository.get_artifact(decision.candidate_ref)
            if candidate is None:
                raise ValueError("decision lost its exact candidate")
            row["instrument_key_json"] = canonical_json(candidate.metadata["candidate"]["key"])
    elif entry.artifact_type == "MaturedOutcomeV2":
        outcome = MaturedOutcomeV2.from_dict(body["outcome"])
        if index_matured_outcome(repository, outcome) != entry.artifact_ref:
            raise ValueError("outcome content identity mismatch")
        body = outcome.to_dict()
        row.update(row_kind="OUTCOME", decision_at_ns=outcome.decision_at_ns,
                   selection_state=outcome.selection_state.value, admission_state=outcome.admission_state.value,
                   outcome_target=outcome.outcome_target.value, label_state=outcome.label_state.value,
                   provenance=outcome.provenance.value,
                   net_payoff=body["net_payoff"])
        if outcome.action_artifact_ref is not None:
            action = repository.get_artifact(outcome.action_artifact_ref)
            if action is None:
                raise ValueError("outcome lost its exact action")
            row["instrument_key_json"] = canonical_json(action.metadata["action_identity"]["key"])
    elif entry.artifact_type == "S3DecisionCalendarMissingnessV1":
        missing = S3DecisionCalendarMissingnessV1.from_dict(body["missingness"])
        if (missing.content_hash != entry.artifact_ref or missing.content_hash != entry.content_hash
                or missing.available_at_ns != entry.available_at_ns or missing.created_at_ns != entry.created_at_ns):
            raise ValueError("missingness content identity mismatch")
        gate = repository.get_artifact(missing.late_gate_ref)
        if (gate is None or gate.available_at_ns > missing.available_at_ns
                or find_s3_m1_origin_late_gate((gate,), missing.instrument_key, missing.decision_slot_ns) is None):
            raise ValueError("missingness lost its supporting gate")
        body = missing.to_dict()
        row.update(row_kind="MISSINGNESS", decision_at_ns=missing.decision_slot_ns, status=missing.state,
                   instrument_key_json=canonical_json(missing.instrument_key.to_dict()))
    elif entry.artifact_type == "ActionCriticShadowObservationV1":
        observation = ActionCriticShadowObservationV1.from_dict(body["observation"])
        if index_action_critic_shadow_observation(repository, observation) != entry.artifact_ref:
            raise ValueError("critic observation identity mismatch")
        for ref in (observation.originating_receipt_ref, observation.decision_calendar_ref,
                    observation.packet_ref, observation.request_ref):
            dependency = repository.get_artifact(ref)
            if dependency is None or dependency.available_at_ns > observation.recorded_at_ns:
                raise ValueError("critic observation precedes required evidence availability")
        decision_entry = repository.get_artifact(observation.decision_calendar_ref)
        if decision_entry is None:
            raise ValueError("critic observation has no exact decision")
        decision = DecisionCalendarEntryV2.from_dict(decision_entry.metadata["decision_entry"])
        if (index_decision_calendar_entry(repository, decision) != observation.decision_calendar_ref
                or decision_entry.available_at_ns > observation.recorded_at_ns
                or decision.action_hash != observation.action_hash
                or decision.candidate_set_ref != observation.candidate_set_ref
                or decision.action_artifact_ref != observation.action_artifact_ref):
            raise ValueError("critic observation action/calendar mismatch")
        body = observation.to_dict()
        row.update(row_kind="INTELLIGENCE", status=observation.critic_terminal_status,
                   decision_ref=observation.decision_calendar_ref)
    elif entry.artifact_type == "PublicSourceHealthV2":
        health = PublicSourceHealthV2.from_dict(body["health"])
        if health.content_hash != entry.content_hash:
            raise ValueError("public health content identity mismatch")
        row["status"] = health.state.value
    elif entry.artifact_type == "ResearchRunTelemetryV1":
        telemetry = body["telemetry"]
        if (sha256_json(telemetry) != entry.content_hash or entry.artifact_ref != entry.content_hash
                or telemetry.get("schema_version") != 1 or telemetry.get("authority") != "ZERO"
                or telemetry.get("available_at_ns") != entry.available_at_ns
                or entry.created_at_ns != entry.available_at_ns):
            raise ValueError("resource telemetry identity or availability mismatch")
    metrics, identities, reasons, refs = _compact_values(body)
    for key, value in identities.items():
        target = "event_id" if key == "decision_event_id" else "decision_ref" if key == "decision_calendar_ref" else key
        if target in row:
            row[target] = value
    row["metrics_json"] = canonical_json(metrics)
    row["reason_codes"] = reasons
    row["evidence_refs"] = refs
    return row


def _prediction_projection(repository: OpsRepository, entry: ArtifactIndexEntryV2,
                           row: dict[str, Any]) -> Mapping[str, Any]:
    from atlas.v2.runtime.research_prediction_outcomes import (
        ResearchPredictionOutcomeV1,
        validate_research_prediction_outcome_v1,
    )

    outcome = ResearchPredictionOutcomeV1.from_dict(json_value(entry.metadata["prediction_outcome"]))
    validate_research_prediction_outcome_v1(repository, outcome)
    if (outcome.content_hash != entry.artifact_ref or entry.content_hash != entry.artifact_ref
            or outcome.available_at_ns != entry.available_at_ns or entry.created_at_ns != entry.available_at_ns):
        raise ValueError("prediction outcome identity or publication time mismatch")
    terminal = repository.get_artifact(outcome.terminal_ref)
    if terminal is None:
        raise ValueError("prediction outcome lost exact model terminal")
    _model_projection(repository, terminal, {})
    body = outcome.to_dict()
    row.update(row_kind="MODEL_OUTCOME", decision_at_ns=outcome.information_cutoff_ns,
               decision_ref=outcome.decision_calendar_ref, action_hash=outcome.action_hash,
               request_ref=outcome.request_ref, route_ref=outcome.route_ref,
               model_profile_hash=outcome.model_manifest_ref, label_state=outcome.label_state,
               instrument_key_json=canonical_json(outcome.instrument_key.to_dict()),
               prediction_id=outcome.prediction_id, prediction_target_ref=outcome.target_definition_ref,
               horizon_ns=outcome.horizon_ns, horizon_end_ns=outcome.horizon_end_ns,
               values_evidence_ref=outcome.forecast_values_ref,
               measured_log_return=body["measured_log_return"])
    row["_model_run_id"], row["_model_config_hash"] = outcome.run_id, outcome.config_hash
    if outcome.label_state == "MATURED" and outcome.forecast_values_ref is not None:
        values_entry = repository.get_artifact(outcome.forecast_values_ref)
        if values_entry is None:
            raise ValueError("prediction outcome lost exact numerical forecast")
        _model_projection(repository, values_entry, {})
        values = json_value(values_entry.metadata["model_values"])
        predicted = values.get(f"log_return:{outcome.horizon_ns}:mean")
        if predicted is not None:
            with localcontext() as context:
                context.prec = 34
                mean = Decimal(predicted)
                observed = Decimal(str(outcome.measured_log_return))
                error = observed - mean
                if not mean.is_finite():
                    raise ValueError("prediction mean must be finite")
                row["predicted_log_return"] = str(mean)
                body = dict(body) | {"measured_log_return": float(observed), "predicted_log_return": float(mean),
                                     "prediction_error": float(error),
                                     "absolute_prediction_error": float(abs(error)),
                                     "squared_prediction_error": float(error * error)}
                if any(not math.isfinite(body[name]) for name in (
                    "measured_log_return", "predicted_log_return", "prediction_error",
                    "absolute_prediction_error", "squared_prediction_error")):
                    raise ValueError("prediction diagnostic exceeds finite numerical reporting range")
                low, high = (values.get(f"log_return:{outcome.horizon_ns}:q{quantile}")
                             for quantile in ("0.05", "0.95"))
                if low is not None and high is not None:
                    lower, upper = Decimal(low), Decimal(high)
                    if not lower.is_finite() or not upper.is_finite() or lower > upper:
                        raise ValueError("prediction quantile interval is invalid")
                    body["quantile_interval_covered"] = int(lower <= observed <= upper)
    return body


def _model_projection(repository: OpsRepository, entry: ArtifactIndexEntryV2,
                      row: dict[str, Any]) -> Mapping[str, Any]:
    """Validate exact model identities and export only closed diagnostic fields."""
    body = json_value(entry.metadata.get("routing"))
    if entry.artifact_type == "ResearchModelValuesV1":
        values = json_value(entry.metadata.get("model_values"))
        if (not isinstance(values, dict) or sha256_json(values) != entry.artifact_ref
                or entry.content_hash != entry.artifact_ref or entry.metadata.get("authority") != "ZERO"
                or len(canonical_json(values).encode()) > 65_536):
            raise ValueError("model numerical values identity mismatch")
        # The installed statistical lane has a fixed target/horizon grammar.
        # Arbitrary provider text is never copied into the analysis dataset.
        projected = {}
        for key, value in values.items():
            if (isinstance(key, str) and re.fullmatch(r"log_return:\d+:(?:mean|q0(?:\.\d+)?|q1)", key)
                    and isinstance(value, str) and re.fullmatch(r"-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?", value)
                    and math.isfinite(float(value))):
                projected[key] = value
        row.update(row_kind="MODEL_VALUES", values_evidence_ref=entry.artifact_ref,
                   model_values_json=canonical_json(projected))
        return {"values_evidence_ref": entry.artifact_ref}
    if (not isinstance(body, dict) or body.get("version") != entry.artifact_type
            or body.get("authority") != "ZERO" or sha256_json(body) != entry.content_hash
            or entry.content_hash != entry.artifact_ref or entry.created_at_ns != entry.available_at_ns):
        raise ValueError("model routing content identity mismatch")
    if "run_id" in body:
        row["_model_run_id"], row["_model_config_hash"] = body["run_id"], body["config_hash"]
    if entry.artifact_type == "ResearchModelRequestV1":
        packet = WorkerRequestV2.from_dict(body["worker_packet"])
        if packet.content_hash != body["worker_packet_hash"]:
            raise ValueError("model request packet hash mismatch")
        registry = repository.get_artifact(body["registry_ref"])
        registry_body = registry.metadata.get("routing") if registry is not None else None
        if (registry is None or registry.artifact_type != "ResearchModelRoutingRegistryV1"
                or not isinstance(registry_body, Mapping) or sha256_json(registry_body) != registry.content_hash
                or registry.artifact_ref != registry.content_hash or registry.available_at_ns > entry.available_at_ns
                or registry_body.get("run_id") != body["run_id"]
                or registry_body.get("config_hash") != body["config_hash"]
                or not any(sha256_json(route) == body["route_ref"]
                           and route.get("provider_key") == body["provider_key"]
                           and route.get("manifest_hash") == packet.manifest.manifest_hash
                           for route in registry_body.get("routes", ()))):
            raise ValueError("model request lost its immutable declared routing registry")
        for ref in packet.request.input_artifact_refs:
            dependency = repository.get_artifact(ref)
            if dependency is None or dependency.available_at_ns > packet.request.information_cutoff_ns:
                raise ValueError("model request input exceeds its fixed market cutoff")
        derived_ref = body.get("derived_input_ref")
        if derived_ref is not None:
            snapshot = repository.get_artifact(derived_ref)
            inputs = BaselineInputsV2.from_dict(packet.inputs)
            if (snapshot is None or snapshot.artifact_type != "BaselineInputsV2"
                    or snapshot.available_at_ns > entry.available_at_ns
                    or snapshot.content_hash != inputs.content_hash or derived_ref != inputs.content_hash
                    or inputs.content_hash != packet.request.input_hash
                    or canonical_json(snapshot.metadata.get("baseline_inputs")) != canonical_json(inputs.to_dict())):
                raise ValueError("model request derived publication snapshot is invalid")
        action_ref = body.get("action_artifact_ref")
        if action_ref is not None:
            action = repository.get_artifact(action_ref)
            action_body = action.metadata.get("action_artifact") if action is not None else None
            action_identity = action.metadata.get("action_identity") if action is not None else None
            if (action is None or action.artifact_type != "ActionArtifactV2"
                    or action.available_at_ns > entry.available_at_ns
                    or not isinstance(action_body, Mapping) or not isinstance(action_identity, Mapping)
                    or sha256_json(action_body) != action_ref or action.content_hash != action_ref
                    or sha256_json(action_identity) != body.get("action_hash")
                    or action_body.get("action_hash") != body.get("action_hash")
                    or canonical_json(action_identity.get("key")) != canonical_json(packet.request.instrument_key.to_dict())):
                raise ValueError("model request action binding is invalid")
        decision_ref = body.get("decision_calendar_ref")
        if decision_ref is not None:
            decision_entry = repository.get_artifact(decision_ref)
            if decision_entry is None or decision_entry.available_at_ns > entry.available_at_ns:
                raise ValueError("model request calendar unavailable at dispatch")
            decision = DecisionCalendarEntryV2.from_dict(json_value(decision_entry.metadata["decision_entry"]))
            if (index_decision_calendar_entry(repository, decision) != decision_ref
                    or decision.decision_at_ns != packet.request.information_cutoff_ns
                    or action_ref is not None and decision.action_artifact_ref != action_ref):
                raise ValueError("model request decision binding is invalid")
            candidate_set = repository.get_artifact(decision.candidate_set_ref)
            event = repository.get_artifact(body["decision_event_ref"])
            event_body = event.metadata.get("event") if event is not None else None
            if (candidate_set is None or event is None or event.artifact_type != "OpsDecisionEventSourceV1"
                    or event.available_at_ns > entry.available_at_ns or not isinstance(event_body, Mapping)
                    or sha256_json(event_body) != event.content_hash or event.artifact_ref != event.content_hash
                    or event_body.get("event_id") != candidate_set.metadata["candidate_set"]["decision_event_id"]
                    or event_body.get("information_cutoff_ns") != packet.request.information_cutoff_ns
                    or event_body.get("deadline_ns") != packet.request.deadline_ns):
                raise ValueError("model request event or original deadline binding is invalid")
        elif action_ref is None:
            raise ValueError("model request has neither an exact action nor calendar")
        row.update(row_kind="MODEL_REQUEST", request_ref=entry.artifact_ref,
                   decision_at_ns=packet.request.information_cutoff_ns,
                   instrument_key_json=canonical_json(packet.request.instrument_key.to_dict()),
                   model_profile_hash=packet.manifest.manifest_hash, input_hash=packet.request.input_hash)
        # Derived packets remain referenced in the operational store. Do not
        # duplicate closes, prompts, or arbitrary framework inputs into Parquet.
        return {key: value for key, value in body.items() if key not in {"worker_packet"}} | {
            "input_refs": list(packet.request.input_artifact_refs)}
    if entry.artifact_type == "ResearchModelForecastV1":
        forecast = ForecastArtifactV2.from_dict(body["forecast"])
        request_entry = repository.get_artifact(body["request_ref"])
        if request_entry is None or request_entry.artifact_type != "ResearchModelRequestV1":
            raise ValueError("model forecast lost its sealed request")
        _model_projection(repository, request_entry, {})
        packet = WorkerRequestV2.from_dict(json_value(request_entry.metadata["routing"]["worker_packet"]))
        sealed = request_entry.metadata["routing"]
        row["_model_run_id"], row["_model_config_hash"] = sealed["run_id"], sealed["config_hash"]
        if (request_entry.available_at_ns > forecast.inference_started_ns
                or forecast.request_id != packet.request.request_id
                or forecast.model_manifest_hash != packet.manifest.manifest_hash
                or forecast.input_hash != packet.request.input_hash
                or forecast.received_ns > entry.available_at_ns):
            raise ValueError("model forecast identity or chronology mismatch")
        values_ref = body["values_evidence_ref"]
        if values_ref is not None:
            values = repository.get_artifact(values_ref)
            if (values is None or values.artifact_type != "ResearchModelValuesV1"
                    or values_ref != forecast.values_ref or values.available_at_ns > entry.available_at_ns):
                raise ValueError("model forecast lost its exact numerical values")
        row.update(row_kind="MODEL_FORECAST", forecast_ref=entry.artifact_ref,
                   model_profile_hash=forecast.model_manifest_hash, input_hash=forecast.input_hash,
                   status=forecast.status.value)
        sample_counts = forecast.resource_metrics.to_dict().get("sample_counts", {})
        if not isinstance(sample_counts, dict):
            raise ValueError("model sample counts must be a bounded typed object")
        projected_counts = {key: count for key, count in sample_counts.items()
                            if isinstance(key, str) and re.fullmatch(r"log_return:\d+", key)
                            and isinstance(count, int) and not isinstance(count, bool) and 0 <= count <= 4096}
        row["model_sample_counts_json"] = canonical_json(projected_counts)
        missing_outputs = [reason for reason in forecast.missing_outputs
                           if re.fullmatch(r"log_return:\d+:[A-Z][A-Z0-9_]{0,95}", reason)]
        row["model_missing_outputs_json"] = canonical_json(missing_outputs)
        return {"request_ref": body["request_ref"], "values_evidence_ref": values_ref,
                "decision_calendar_ref": sealed["decision_calendar_ref"],
                "decision_event_ref": sealed["decision_event_ref"], "action_hash": sealed["action_hash"],
                "route_ref": sealed["route_ref"], "provider_key": sealed["provider_key"],
                "inference_started_ns": forecast.inference_started_ns, "completed_ns": forecast.completed_ns,
                "received_ns": forecast.received_ns, "expires_ns": forecast.expires_ns,
                "reason_codes": sorted({reason.rsplit(":", 1)[-1] for reason in missing_outputs})}
    if entry.artifact_type == "ResearchModelTerminalV1":
        request_entry = repository.get_artifact(body["request_ref"])
        if request_entry is None or request_entry.artifact_type != "ResearchModelRequestV1":
            raise ValueError("model terminal lost its exact request")
        _model_projection(repository, request_entry, {})
        sealed = json_value(request_entry.metadata["routing"])
        packet = WorkerRequestV2.from_dict(sealed["worker_packet"])
        if (body["request_id"] != packet.request.request_id or body["input_hash"] != packet.request.input_hash
                or body["manifest_hash"] != packet.manifest.manifest_hash
                or any(body.get(key) != sealed.get(key) for key in
                       ("run_id", "config_hash", "route_ref", "action_artifact_ref", "action_hash",
                        "decision_calendar_ref", "decision_event_ref"))):
            raise ValueError("model terminal binding mismatch")
        row.update(row_kind="MODEL_TERMINAL", model_profile_hash=packet.manifest.manifest_hash,
                   provider_key=sealed["provider_key"], status=body["state"],
                   decision_at_ns=packet.request.information_cutoff_ns)
    elif entry.artifact_type == "ResearchModelShadowDiagnosticV1":
        receipt = repository.get_artifact(body["receipt_ref"])
        receipt_body = receipt.metadata.get("receipt") if receipt is not None else None
        if (receipt is None or receipt.artifact_type != "OpsSupervisorReceiptV1"
                or not isinstance(receipt_body, Mapping) or sha256_json(receipt_body) != receipt.artifact_ref
                or receipt.content_hash != receipt.artifact_ref or receipt.available_at_ns > entry.available_at_ns
                or body["available_at_ns"] != entry.available_at_ns
                or receipt_body["decision_event"]["information_cutoff_ns"] != body["information_cutoff_ns"]
                or receipt_body["decision_event"]["deadline_ns"] != body["original_deadline_ns"]):
            raise ValueError("model missingness lost its exact originating receipt")
        row.update(row_kind="MODEL_MISSINGNESS", status=body["status"],
                   decision_at_ns=body["information_cutoff_ns"])
    else:
        row.update(row_kind="MODEL_REGISTRY" if entry.artifact_type == "ResearchModelRoutingRegistryV1"
                   else "MODEL_MISSINGNESS", status=body.get("status"))
    return body


def _schema() -> Any:
    import pyarrow as pa

    integer_names = {"created_at_ns", "available_at_ns", "decision_at_ns", "source_rowid", "horizon_ns", "horizon_end_ns"}
    list_names = {"reason_codes", "evidence_refs"}
    names = (
        "run_id", "config_hash", "source_sha", "source_rowid", "artifact_ref", "artifact_type",
        "created_at_ns", "available_at_ns", "row_kind", "decision_at_ns", "decision_ref", "event_id",
        "candidate_set_ref", "candidate_ref", "action_hash", "policy_id", "policy_hash", "origin_ref",
        "selection_state", "admission_state", "outcome_target", "label_state", "provenance", "net_payoff",
        "provider_profile_hash", "model_profile_hash", "status", "source_stage", "instrument_key_json", "epoch_id",
        "metrics_json", "reason_codes", "evidence_refs",
        "request_ref", "route_ref", "provider_key", "input_hash", "forecast_ref", "values_evidence_ref",
        "model_values_json",
        "model_sample_counts_json",
        "model_missing_outputs_json",
        "prediction_id", "prediction_target_ref", "horizon_ns", "horizon_end_ns",
        "measured_log_return", "predicted_log_return",
    )
    return pa.schema([(name, pa.int64() if name in integer_names else pa.list_(pa.string())
                       if name in list_names else pa.string()) for name in names])


def export_tuning_snapshot(
    database_path: str | Path,
    output_root: str | Path,
    identity: TuningRunIdentityV1,
    *,
    cutoff_ns: int,
    max_rows: int = 100_000,
    batch_size: int = 128,
    max_snapshot_seconds: float = 10.0,
) -> dict[str, Any]:
    """Append a bounded immutable analysis partition; restart reuses the last manifest.

    Each run owns a dedicated operational database. Caller supplies its immutable
    run identity; sharing one database between configurations is prohibited by the
    launcher. ``has_more`` requires another call before a final report is complete.
    Only manifested files are dataset members; interrupted temporary/orphan files
    are never read as evidence. No provider requests or writable DB is opened.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    timestamp(cutoff_ns, field="export cutoff_ns")
    if cutoff_ns < identity.started_at_ns:
        raise ValueError("export cutoff precedes run start")
    if (type(max_rows) is not int or not 1 <= max_rows <= 100_000
            or type(batch_size) is not int or not 1 <= batch_size <= 256
            or not math.isfinite(max_snapshot_seconds) or not 0 < max_snapshot_seconds <= 30):
        raise ValueError("invalid bounded export configuration")
    root = Path(output_root) / identity.run_id
    root.mkdir(parents=True, exist_ok=True)
    with _export_lock(root / "export.lock"):
        _immutable_json(root / "run-identity.json", identity.to_dict())
        manifests = root / "manifests"
        manifests.mkdir(exist_ok=True)
        partitions = root / "partitions"
        partitions.mkdir(exist_ok=True)
        head_path = root / "head.json"
        previous: dict[str, Any] | None = None
        if head_path.exists():
            head = json.loads(head_path.read_text())
            manifest_name = head["manifest_sha256"]
            sha256_ref(manifest_name, field="manifest_sha256")
            previous = json.loads((manifests / f"{manifest_name}.json").read_text())
            if sha256_json(previous) != manifest_name or previous["run_identity"] != identity.to_dict():
                raise ValueError("export checkpoint hash or run identity mismatch")
            if cutoff_ns < previous["cutoff_ns"]:
                raise ValueError("export cutoff cannot regress")
            partition_hash = previous["partition_sha256"]
            sha256_ref(partition_hash, field="partition_sha256")
            if _file_hash(partitions / f"{partition_hash}.parquet") != partition_hash:
                raise ValueError("export checkpoint partition checksum mismatch")
        after = previous["through_rowid"] if previous is not None else 0
        counts: Counter[str] = Counter(previous["counts"] if previous is not None else {})
        failures: Counter[str] = Counter(previous["validation_failures"] if previous is not None else {})
        metric_summaries = dict(previous["metric_summaries"] if previous is not None else {})
        temporary = partitions / f".{uuid.uuid4().hex}.parquet.tmp"
        writer = pq.ParquetWriter(temporary, _schema(), compression="zstd")
        started = time.monotonic()
        rows_written = 0
        through = after
        has_more = False
        blocked_future = False
        previous_digest = previous["source_record_digest"] if previous is not None else "0" * 64
        digest = hashlib.sha256(bytes.fromhex(previous_digest))
        try:
            with _ValidationReader(database_path, read_only=True) as repository, repository.read_snapshot():
                connection = repository._connection
                connection.set_progress_handler(
                    lambda: int(time.monotonic() - started > max_snapshot_seconds), 10_000,
                )
                marks = ",".join("?" for _ in _TYPES)
                cursor = connection.execute(
                    "SELECT rowid AS source_rowid,artifact_ref,artifact_type,content_hash,created_at_ns,available_at_ns,"
                    "CASE WHEN length(metadata_json)<=CASE WHEN artifact_type='ResearchModelRequestV1' "
                    "THEN 1114112 ELSE 131072 END THEN metadata_json ELSE '{}' END AS metadata_json,"
                    "length(metadata_json)>CASE WHEN artifact_type='ResearchModelRequestV1' THEN 1114112 "
                    "ELSE 131072 END AS metadata_overflow FROM artifact_index WHERE rowid>? "
                    f"AND artifact_type IN ({marks}) ORDER BY rowid LIMIT ?",
                    (after, *_TYPES, max_rows + 1),
                )
                stop = False
                while not stop:
                    raw_rows = cursor.fetchmany(batch_size)
                    if not raw_rows:
                        break
                    batch = []
                    for raw in raw_rows:
                        if rows_written >= max_rows:
                            has_more, stop = True, True
                            break
                        if raw["available_at_ns"] > cutoff_ns:
                            blocked_future, stop = True, True
                            break
                        if time.monotonic() - started > max_snapshot_seconds:
                            raise TuningExportBudgetExceeded("read snapshot exceeded its fixed time budget")
                        through = raw["source_rowid"]
                        try:
                            if raw["metadata_overflow"]:
                                raise ValueError("compact export refuses oversized evidence metadata")
                            entry = ArtifactIndexEntryV2._from_storage_row(raw)
                            if entry.artifact_type == "ResearchRunTelemetryV1":
                                telemetry = entry.metadata.get("telemetry", {})
                                if telemetry.get("run_id") != identity.run_id or telemetry.get("config_hash") != identity.config_hash:
                                    raise ValueError("resource telemetry belongs to another immutable run")
                            row = _validated_row(repository, entry)
                            if "_model_run_id" in row:
                                if (row.pop("_model_run_id") != identity.run_id
                                        or row.pop("_model_config_hash") != identity.config_hash):
                                    raise ValueError("model evidence belongs to another immutable run")
                        except (ValueError, KeyError, TypeError, ArithmeticError, json.JSONDecodeError):
                            failures[raw["artifact_type"]] += 1
                            row = {"artifact_ref": raw["artifact_ref"], "artifact_type": raw["artifact_type"],
                                   "created_at_ns": raw["created_at_ns"], "available_at_ns": raw["available_at_ns"],
                                   "row_kind": "INVALID", "status": "TEST GATE", "metrics_json": "{}",
                                   "reason_codes": ["INDEXED_EVIDENCE_FAILED_VALIDATION"], "evidence_refs": []}
                        row.update(run_id=identity.run_id, config_hash=identity.config_hash,
                                   source_sha=identity.source_sha, source_rowid=through)
                        batch.append(row)
                        rows_written += 1
                        counts["row_kind:" + row["row_kind"]] += 1
                        for name in ("selection_state", "admission_state", "source_stage", "status", "policy_id"):
                            if row.get(name) is not None:
                                counts[name + ":" + str(row[name])] += 1
                        if row["row_kind"].startswith("MODEL_"):
                            for name in ("route_ref", "provider_key", "model_profile_hash"):
                                if row.get(name) is not None:
                                    counts["model:" + row["row_kind"] + ":" + name + ":" + str(row[name])] += 1
                        for reason in row["reason_codes"]:
                            counts["reason:" + reason] += 1
                        for name, value in json.loads(row["metrics_json"]).items():
                            key = row["artifact_type"] + ":" + name
                            if row["row_kind"] == "MODEL_OUTCOME":
                                key = ":".join((row["artifact_type"], row["prediction_target_ref"],
                                                row["model_profile_hash"], row["route_ref"],
                                                str(row["horizon_ns"]), name))
                            if (isinstance(value, (int, float)) and not isinstance(value, bool)
                                    and math.isfinite(float(value))):
                                summary = metric_summaries.setdefault(key, {"count": 0, "sum": 0, "min": value,
                                                                           "max": value})
                                summary["count"] += 1
                                summary["sum"] += value
                                summary["min"] = min(summary["min"], value)
                                summary["max"] = max(summary["max"], value)
                            elif isinstance(value, str) and _SAFE_CODE.fullmatch(value):
                                counts["metric_state:" + key + ":" + value] += 1
                        digest.update(canonical_json(row).encode())
                    if batch:
                        writer.write_table(pa.Table.from_pylist(batch, schema=_schema()))
                cursor.close()
                connection.set_progress_handler(None, 0)
            writer.close()
            with temporary.open("rb") as sealed_partition:
                os.fsync(sealed_partition.fileno())
            file_hash = _file_hash(temporary)
            target = partitions / f"{file_hash}.parquet"
            if target.exists():
                if _file_hash(target) != file_hash:
                    raise ValueError("existing partition checksum mismatch")
                temporary.unlink()
            else:
                os.replace(temporary, target)
            if (through == after and previous is not None and cutoff_ns == previous["cutoff_ns"]
                    and has_more == previous["has_more"]
                    and blocked_future == previous["blocked_future_evidence"]):
                return previous
            manifest: dict[str, Any] = {
                "version": VERSION, "run_identity": identity.to_dict(), "cutoff_ns": cutoff_ns,
                "after_rowid": after, "through_rowid": through, "rows_written": rows_written,
                "has_more": has_more, "blocked_future_evidence": blocked_future,
                "previous_manifest_sha256": sha256_json(previous) if previous is not None else None,
                "source_record_digest": digest.hexdigest(), "partition_sha256": file_hash,
                "partition": f"partitions/{file_hash}.parquet", "counts": dict(sorted(counts.items())),
                "validation_failures": dict(sorted(failures.items())),
                "metric_summaries": dict(sorted(metric_summaries.items())),
                "report": {
                    "status": "TEST GATE" if failures or has_more or blocked_future else "TESTED",
                    "scope": "READ_ONLY_OFFLINE_EVIDENCE_PROJECTION",
                    "calendar_rows": counts["row_kind:DECISION"],
                    "missing_origin_rows": counts["row_kind:MISSINGNESS"],
                    "outcome_rows": counts["row_kind:OUTCOME"],
                    "intelligence_rows": counts["row_kind:INTELLIGENCE"],
                    "model_request_rows": counts["row_kind:MODEL_REQUEST"],
                    "model_forecast_rows": counts["row_kind:MODEL_FORECAST"],
                    "model_terminal_rows": counts["row_kind:MODEL_TERMINAL"],
                    "model_missingness_rows": counts["row_kind:MODEL_MISSINGNESS"],
                    "model_values_rows": counts["row_kind:MODEL_VALUES"],
                    "model_outcome_rows": counts["row_kind:MODEL_OUTCOME"],
                    "model_stage_profile_counts": {key: value for key, value in sorted(counts.items())
                                                   if key.startswith("model:")},
                    "source_and_pipeline_states": {key: value for key, value in sorted(counts.items())
                                                   if key.startswith(("status:", "metric_state:", "reason:"))},
                    "recorded_resource_and_latency_summaries": metric_summaries,
                    "invalid_rows": counts["row_kind:INVALID"],
                    "denominator_definition": "CALENDAR_ENTRY_ROWS_BY_STAGE_WITH_MISSING_ORIGINS_SEPARATE",
                    "opportunity_denominator": "NOT_ESTIMABLE_UNTIL_ORIGIN_AND_STAGE_RECONCILIATION",
                    "independence": "CONFIGURATIONS_SHARE_MARKET_HISTORY_AND_ARE_NOT_INDEPENDENT_SAMPLES",
                    "unsupported_metrics": [
                        "economic_significance_without_prospective_duration_regimes_dependence_and_support",
                        "prediction_calibration_without_typed_prediction_target_and_exact_matured_labels",
                        "per_stage_latency_without_recorded_computation_timestamps",
                        "expected_origins_missing_from_the_operational_calendar",
                    ],
                    "capital": "DISABLED", "assisted_execution": "DISABLED", "authority": "ZERO",
                },
            }
            manifest_hash = sha256_json(manifest)
            _immutable_json(manifests / f"{manifest_hash}.json", manifest)
            _immutable_json(root / f"report-{manifest_hash}.json", manifest["report"])
            head_tmp = root / f".head.{uuid.uuid4().hex}.tmp"
            _immutable_json(head_tmp, {"manifest_sha256": manifest_hash})
            os.replace(head_tmp, head_path)
            return manifest
        except sqlite3.OperationalError as error:
            if time.monotonic() - started > max_snapshot_seconds:
                raise TuningExportBudgetExceeded("read snapshot exceeded its fixed time budget") from error
            raise
        finally:
            writer.close()
            temporary.unlink(missing_ok=True)
