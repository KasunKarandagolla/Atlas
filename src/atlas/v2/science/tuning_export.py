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
import threading
import time
import uuid
from collections import Counter
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Any

from atlas.domain.money import canonical_decimal_str
from atlas.v2._serialization import canonical_json, decimal_value, json_value, sha256_json, sha256_ref, timestamp
from atlas.v2.agent_intelligence.shadow_measurement import (
    ActionCriticShadowObservationV1,
    index_action_critic_shadow_observation,
)
from atlas.v2.data.health import PublicSourceHealthV2
from atlas.v2.data.public_evidence_checkpoint import (
    BOOK_CHECKPOINT_TYPE,
    CONTINUITY_CHECKPOINT_TYPE,
    validate_book_checkpoint,
    validate_continuity_checkpoint,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.models.baseline import BaselineInputsV2
from atlas.v2.models.protocol import ForecastArtifactV2
from atlas.v2.models.worker_protocol import WorkerRequestV2
from atlas.v2.runtime.s3_native_cadence import find_s3_m1_origin_late_gate, s3_m1_event_id
from atlas.v2.science.outcomes import (
    DecisionCalendarEntryV2,
    MaturedOutcomeV2,
    index_decision_calendar_entry,
    index_matured_outcome,
)
from atlas.v2.science.s3_calendar import S3DecisionCalendarMissingnessV1

VERSION = "ATLAS_TUNING_EXPORT_V1"
MAX_ANALYSIS_PARTITIONS = 128
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
    "M15OriginAccountingRecordV1", "M15OpportunityMissingnessV1",
    "FeatureArtifactV2", "OpsSupervisorStageCheckpointV1",
    "DerivedComputationChronologyV1", "ResearchPrerequisiteInventoryV1",
    "ActionReplayLifecycleSummaryV1", "ActionReplaySourceEvidenceV1",
    "OpsActiveWorkPressureV1", "ActiveTrainingWorkPressureV1",
    "PublicBarGapRepairPageV1",
    "PublicContextCycleReportV1", "NewsEventV2",
    CONTINUITY_CHECKPOINT_TYPE, BOOK_CHECKPOINT_TYPE,
    "EconomicSourceManifestV1", "OpsEconomicEvidenceResolutionV1",
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
    "limit", "observed_count", "observed_count_is_lower_bound", "invalid_rows",
    "invalid_entry_count", "has_more", "consumer_eligible", "entry_to_exit_duration_ns",
    "frozen_sizing_margin", "net_margin_roi", "computation_duration_ns", "publication_latency_ns",
    "confidence", "duplicate_count", "inflight_count", "completed_slot_count",
    "ready", "processed_bar_count", "last_close_at_ns", "max_rows_per_cycle",
    "verified_close_at_ns", "target_close_at_ns", "max_source_rows_per_cycle",
    "backlog", "cursor_available_at_ns", "quarantined_count", "retired_count",
    "due_count_lower_bound", "due_page_overflow", "oldest_due_age_ns",
    "scenario_count", "max_manifest_rows", "observed_row_count",
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
            if key in {"reason_codes", "reasons", "missing_reasons"} and isinstance(value, list):
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
        "accounting_kind": None,
        "feature_ref": None, "feature_schema": None, "features_json": None, "regimes_json": None,
        "action_artifact_ref": None, "expected_net_value": None, "method_id": None,
        "method_config_hash": None, "stage_completed_at_ns": None, "stage_order": None,
        "source_event_at_ns": None, "received_at_ns": None, "information_cutoff_ns": None,
        "registered_routes_json": None,
        "computation_started_ns": None, "computation_finished_ns": None,
        "computation_duration_ns": None, "publication_latency_ns": None,
        "entry_at_ns": None, "exit_at_ns": None, "entry_to_exit_duration_ns": None,
        "fees": None, "funding_cashflow": None, "frozen_sizing_margin": None, "net_margin_roi": None,
    }
    body = json_value(entry.metadata)
    if entry.artifact_type == CONTINUITY_CHECKPOINT_TYPE:
        validate_continuity_checkpoint(repository, entry)
    elif entry.artifact_type == BOOK_CHECKPOINT_TYPE:
        validate_book_checkpoint(repository, entry, as_of_ns=entry.available_at_ns)
    elif entry.artifact_type == "PublicStreamContinuityReportV1":
        report = body["report"]
        if "storage_version" in body:
            from ..data.public_evidence_checkpoint import resolve_report_transport

            transport = resolve_report_transport(repository, body, available_at_ns=entry.available_at_ns)
            if entry.created_at_ns != body["computation_started_ns"]:
                raise ValueError("compact report computation start differs from indexed chronology")
            body = {"report": report, "state_ref": body["state_ref"],
                    "source_health_ref": body["source_health_ref"], "transport": transport}
        if (sha256_json({"artifact_type": entry.artifact_type, "report": report}) != entry.content_hash
                or entry.artifact_ref != entry.content_hash or report["as_of_ns"] > entry.available_at_ns
                or (report["as_of_ns"] != entry.available_at_ns
                    and entry.metadata.get("storage_version") != "PUBLIC_CONTINUITY_REPORT_INDEX_V2")):
            raise ValueError("continuity report content or chronology mismatch")
        state_entry = repository.get_artifact(body["state_ref"])
        if state_entry is None or state_entry.available_at_ns > entry.available_at_ns:
            raise ValueError("continuity report state missing or future")
        state = validate_continuity_checkpoint(repository, state_entry)
        if (state.instrument.to_dict() != report["instrument"] or state.channel != report["channel"]
                or state.epoch_id != report["epoch_id"] or state.current_recovery_ref != report["current_recovery_ref"]
                or state.gap_count != report["gap_count"]):
            raise ValueError("continuity report state identity mismatch")
        bbo = report["latest_valid_bbo"]
        if bbo is not None and state_entry.artifact_type == CONTINUITY_CHECKPOINT_TYPE:
            refs = bbo["input_refs"]
            if not isinstance(refs, list) or len(refs) != 2 or report["source_health_ref"] not in refs:
                raise ValueError("compact continuity BBO lineage missing")
            anchors = [repository.get_artifact(ref) for ref in refs if ref != report["source_health_ref"]]
            if len(anchors) != 1 or anchors[0] is None:
                raise ValueError("compact continuity book checkpoint missing")
            checkpoint = validate_book_checkpoint(repository, anchors[0], as_of_ns=entry.available_at_ns)
            if (checkpoint["bbo"] != [bbo["bid_price"], bbo["ask_price"]]
                    or checkpoint["received_at_ns"] != bbo["received_at_ns"]
                    or checkpoint["instrument"] != report["instrument"] or checkpoint["channel"] != report["channel"]
                    or checkpoint["epoch_id"] != report["epoch_id"] or checkpoint["metadata_ref"] != report["metadata_ref"]
                    or bbo["data_age_ns"] != report["as_of_ns"] - bbo["received_at_ns"]):
                raise ValueError("compact continuity BBO differs from exact book checkpoint")
    if entry.artifact_type.startswith("ResearchModel"):
        body = _model_projection(repository, entry, row)
    if entry.artifact_type == "DerivedComputationChronologyV1":
        body = _chronology_projection(repository, entry, row)
    elif entry.artifact_type == "ResearchPrerequisiteInventoryV1":
        body = _prerequisite_projection(repository, entry, row)
    elif entry.artifact_type == "EconomicSourceManifestV1":
        from atlas.v2.runtime.economic_sources import validate_economic_source_manifest

        manifest = validate_economic_source_manifest(repository, entry, cutoff_ns=entry.available_at_ns)
        body = manifest.to_dict()
        row.update(row_kind="ECONOMIC_CONFIGURATION", method_id=manifest.version,
            method_config_hash=manifest.content_hash, policy_hash=manifest.policy_hash,
            information_cutoff_ns=manifest.available_at_ns, status="DECLARED")
    elif entry.artifact_type == "OpsEconomicEvidenceResolutionV1":
        from atlas.v2.chronology import causal_artifact

        resolution = body.get("resolution")
        if (not isinstance(resolution, Mapping) or sha256_json(resolution) != entry.content_hash
                or resolution.get("authority") != "ZERO"):
            raise ValueError("economic resolution content or authority mismatch")
        if resolution.get("version") == "OPS_DECLARED_ECONOMIC_RESOLUTION_V1":
            if (resolution.get("available_at_ns") != entry.available_at_ns or not causal_artifact(
                    repository, entry.artifact_ref, cutoff_ns=resolution["market_information_cutoff_ns"],
                    consumer_at_ns=entry.available_at_ns, deadline_ns=resolution["consumer_deadline_ns"])):
                raise ValueError("economic resolution derived binding chronology invalid")
            row.update(method_config_hash=resolution["source_manifest_ref"],
                information_cutoff_ns=resolution["market_information_cutoff_ns"])
        elif (resolution.get("version") != "OPS_ECONOMIC_EVIDENCE_RESOLUTION_V1"
                or entry.artifact_ref != entry.content_hash):
            raise ValueError("unsupported economic resolution")
        body = resolution
        row.update(row_kind="ECONOMIC_BINDING", event_id=resolution["event_id"],
            candidate_ref=resolution["candidate_ref"], candidate_set_ref=resolution["candidate_set_ref"],
            action_artifact_ref=resolution["action_artifact_ref"], action_hash=resolution["action_hash"],
            method_id=resolution["version"], status="BOUND")
    elif entry.artifact_type == "ActionReplayLifecycleSummaryV1":
        body = _lifecycle_projection(repository, entry, row)
    elif entry.artifact_type == "ActionReplaySourceEvidenceV1":
        from atlas.v2.runtime.action_outcome_producer import ActionReplaySourceEvidenceV1, _validate_source

        source = ActionReplaySourceEvidenceV1.from_dict(body["source_evidence"])
        if (source.content_hash != entry.artifact_ref or source.content_hash != entry.content_hash
                or source.available_at_ns != entry.available_at_ns or entry.created_at_ns != entry.available_at_ns):
            raise ValueError("replay source evidence identity or publication mismatch")
        calendar = repository.get_artifact(source.decision_ref)
        if calendar is None:
            raise ValueError("replay source calendar unavailable")
        _validate_source(repository, source, calendar, source.available_at_ns)
        body = source.to_dict()
        row.update(row_kind="REPLAY_SOURCE", decision_ref=source.decision_ref, action_artifact_ref=source.action_ref)
    elif entry.artifact_type in {"OpsActiveWorkPressureV1", "ActiveTrainingWorkPressureV1"}:
        pressure = body["pressure"]
        if (not isinstance(pressure, Mapping) or pressure.get("version") != entry.artifact_type
                or pressure.get("authority") != "ZERO" or sha256_json(pressure) != entry.content_hash
                or entry.artifact_ref != entry.content_hash or entry.created_at_ns != entry.available_at_ns):
            raise ValueError("active pressure identity or publication mismatch")
        body = pressure
        row.update(row_kind="PRESSURE", status="AVAILABLE" if pressure.get("ready") is True else "NOT_ESTIMABLE")
        lane = pressure.get("lane")
        interval = pressure.get("interval", pressure.get("event_type"))
        row["source_stage"] = (str(lane) + ":" + str(interval)
                               if lane is not None and interval is not None else lane)
        row["instrument_key_json"] = pressure.get("instrument_key_json")
    elif entry.artifact_type == "PublicBarGapRepairPageV1":
        proof = body["repair"]
        if (not isinstance(proof, Mapping) or proof.get("version") != entry.artifact_type
                or proof.get("authority") != "ZERO" or sha256_json(proof) != entry.content_hash
                or entry.artifact_ref != entry.content_hash
                or proof.get("available_at_ns") != entry.available_at_ns
                or entry.created_at_ns != entry.available_at_ns):
            raise ValueError("bar repair proof identity or publication mismatch")
        body = proof
        row.update(row_kind="PRESSURE", status="AVAILABLE" if proof.get("reason_code") ==
                   "CONFIRMED_BAR_GAP_REPAIRED" else "NOT_ESTIMABLE")
    elif entry.artifact_type == "ResearchPredictionOutcomeV1":
        body = _prediction_projection(repository, entry, row)
    elif entry.artifact_type in {"M15OriginAccountingRecordV1", "M15OpportunityMissingnessV1"}:
        body = _m15_projection(repository, entry, row)
    elif entry.artifact_type == "FeatureArtifactV2":
        body = _feature_projection(repository, entry, row)
    elif entry.artifact_type in {"M0PredictionV2", "M1PredictionV2"}:
        body = _action_prediction_projection(repository, entry, row)
    elif entry.artifact_type == "OpsSupervisorStageCheckpointV1":
        body = _stage_projection(repository, entry, row)
    elif entry.artifact_type == "OpsSupervisorReceiptV1":
        body = _receipt_projection(repository, entry, row)
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
            _decision_feature_context(repository, candidate, row)
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
                   instrument_key_json=canonical_json(missing.instrument_key.to_dict()),
                   event_id=s3_m1_event_id(missing.instrument_key, missing.decision_slot_ns))
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


def _computation_times(body: Mapping[str, Any], row: dict[str, Any]) -> None:
    cutoff, start, finish, published = (timestamp(body[name], field=name) for name in (
        "information_cutoff_ns", "computation_started_ns", "computation_finished_ns", "available_at_ns"))
    market_cutoff = timestamp(body.get("market_information_cutoff_ns", cutoff), field="market_information_cutoff_ns")
    if not market_cutoff <= cutoff <= start <= finish <= published:
        raise ValueError("computation projection chronology invalid")
    row.update(information_cutoff_ns=cutoff, computation_started_ns=start, computation_finished_ns=finish,
        computation_duration_ns=finish - start, publication_latency_ns=published - market_cutoff)


def _chronology_projection(repository: OpsRepository, entry: ArtifactIndexEntryV2,
                           row: dict[str, Any]) -> Mapping[str, Any]:
    from atlas.v2.chronology import (
        DERIVED_TYPES,
        MAX_DEPENDENCIES,
        RECEIPT_FIELDS,
        _declared_inputs,
        causal_artifact,
        chronology_ref,
    )
    from atlas.v2.chronology import VERSION as chronology_version

    body = json_value(entry.metadata["chronology"])
    target = repository.get_artifact(body["artifact_ref"])
    if (set(body) != RECEIPT_FIELDS or body.get("version") != chronology_version
            or entry.artifact_ref != chronology_ref(body["artifact_ref"])
            or sha256_json(body) != entry.content_hash or target is None
            or target.content_hash != body["artifact_content_hash"] or target.artifact_type != body["artifact_type"]
            or target.artifact_type not in DERIVED_TYPES
            or entry.available_at_ns != body["available_at_ns"] or entry.created_at_ns != entry.available_at_ns
            or target.available_at_ns != entry.available_at_ns or body.get("authority") != "ZERO"
            or body.get("consumer_eligible") != (entry.available_at_ns <= body["consumer_deadline_ns"])):
        raise ValueError("computation receipt identity, artifact or publication mismatch")
    _computation_times(body, row)
    refs = body.get("input_refs")
    cache: dict[str, ArtifactIndexEntryV2 | None] = {}
    budget = [0]
    if (not isinstance(refs, list) or len(refs) > MAX_DEPENDENCIES or not set(_declared_inputs(target)).issubset(refs)
            or any(not causal_artifact(repository, ref,
            cutoff_ns=body["market_information_cutoff_ns"], consumer_at_ns=body["computation_started_ns"],
            deadline_ns=body["consumer_deadline_ns"], _cache=cache, _budget=budget) for ref in refs)):
        raise ValueError("computation receipt dependency unavailable or noncausal")
    actual_cutoff = body["market_information_cutoff_ns"]
    for ref in refs:
        dependency = cache.get(ref)
        if dependency is None:
            raise ValueError("computation receipt dependency unavailable")
        actual_cutoff = max(actual_cutoff, dependency.available_at_ns)
    if actual_cutoff != body["information_cutoff_ns"]:
        raise ValueError("computation receipt input cutoff mismatch")
    row.update(row_kind="COMPUTATION", source_stage=body["artifact_type"],
        status="AVAILABLE" if body["consumer_eligible"] else "NOT_ESTIMABLE")
    return body


def _prerequisite_projection(repository: OpsRepository, entry: ArtifactIndexEntryV2,
                             row: dict[str, Any]) -> Mapping[str, Any]:
    from atlas.v2.runtime.research_prerequisites import _publication, prerequisite_identity_ref

    publication = _publication(entry)
    body = json_value(entry.metadata["prerequisites"])
    if entry.artifact_ref != prerequisite_identity_ref(body["event_id"], body["product_ref"]):
        raise ValueError("prerequisite inventory locator mismatch")
    for ref in body["input_refs"]:
        dependency = repository.get_artifact(ref)
        if dependency is None or dependency.available_at_ns > body["information_cutoff_ns"]:
            raise ValueError("prerequisite input unavailable at market cutoff")
    gate = repository.get_artifact(publication.event_gate_ref)
    from atlas.v2.contracts import ArtifactEnvelope
    from atlas.v2.news.events import EventSafetyGateV2

    gate_body = gate.metadata.get("gate") if gate else None
    if (gate is None or gate.artifact_type != EventSafetyGateV2.ARTIFACT_TYPE
            or gate.available_at_ns != entry.available_at_ns or not isinstance(gate_body, Mapping)
            or not isinstance(gate_body.get("envelope"), Mapping)):
        raise ValueError("prerequisite event gate publication mismatch")
    envelope = ArtifactEnvelope.from_dict(json_value(gate_body["envelope"]))
    preimage = {**gate_body, "envelope": envelope.to_dict(include_hash=False)}
    if (sha256_json({"artifact_type": EventSafetyGateV2.ARTIFACT_TYPE, "artifact": preimage}) != gate.content_hash
            or envelope.content_hash != publication.event_gate_ref or gate.content_hash != publication.event_gate_ref
            or envelope.available_at_ns != gate.available_at_ns or envelope.created_at_ns != gate.created_at_ns
            or gate_body.get("cutoff_ns") != body["information_cutoff_ns"]):
        raise ValueError("prerequisite event gate content or market cutoff mismatch")
    _computation_times(body, row)
    row.update(row_kind="PREREQUISITES", event_id=body["event_id"], status=publication.status)
    return body


def _lifecycle_projection(repository: OpsRepository, entry: ArtifactIndexEntryV2,
                          row: dict[str, Any]) -> Mapping[str, Any]:
    from atlas.v2.runtime.action_outcome_producer import ActionReplayLifecycleSummaryV1

    body = json_value(entry.metadata["summary"])
    fields = ActionReplayLifecycleSummaryV1.__dataclass_fields__
    if set(body) != set(fields) | {"version"} or body.get("version") != "ACTION_REPLAY_LIFECYCLE_SUMMARY_V1":
        raise ValueError("lifecycle summary schema mismatch")
    summary = ActionReplayLifecycleSummaryV1(**{name: body[name] for name in fields})
    if (summary.content_hash != entry.content_hash or summary.artifact_ref != entry.artifact_ref
            or summary.available_at_ns != entry.available_at_ns or entry.created_at_ns != entry.available_at_ns
            or not summary.evidence_cutoff_ns <= summary.payoff_available_at_ns <= summary.available_at_ns):
        raise ValueError("lifecycle summary identity, locator or publication mismatch")
    required_refs = {summary.decision_ref, summary.action_ref, summary.payoff_ref, summary.source_evidence_ref}
    if set(entry.metadata.get("input_refs", ())) != required_refs:
        raise ValueError("lifecycle summary exact input refs mismatch")
    linked: dict[str, ArtifactIndexEntryV2] = {}
    for ref, kind in ((summary.decision_ref, "DecisionCalendarEntryV2"), (summary.action_ref, "ActionArtifactV2"),
            (summary.payoff_ref, "PolicyPayoffV2"), (summary.source_evidence_ref, "ActionReplaySourceEvidenceV1")):
        item = repository.get_artifact(ref)
        if item is None or item.artifact_type != kind or item.available_at_ns > summary.available_at_ns:
            raise ValueError("lifecycle required source unavailable")
        linked[kind] = item
    _validated_row(repository, linked["ActionReplaySourceEvidenceV1"])
    decision_entry = linked["DecisionCalendarEntryV2"]
    decision = DecisionCalendarEntryV2.from_dict(json_value(decision_entry.metadata["decision_entry"]))
    if (decision.content_hash != decision_entry.content_hash or decision.content_hash != summary.decision_ref
            or decision.available_at_ns != decision_entry.available_at_ns):
        raise ValueError("lifecycle calendar identity or availability mismatch")
    action_entry = linked["ActionArtifactV2"]
    action, identity = action_entry.metadata["action_artifact"], action_entry.metadata["action_identity"]
    source = linked["ActionReplaySourceEvidenceV1"].metadata["source_evidence"]
    payoff_entry = linked["PolicyPayoffV2"]
    payoff = json_value(payoff_entry.metadata["payoff"])
    if (sha256_json(action) != summary.action_ref or action_entry.content_hash != summary.action_ref
            or action["available_at_ns"] != action_entry.available_at_ns or sha256_json(identity) != action["action_hash"]
            or decision.action_artifact_ref != summary.action_ref or decision.action_hash != action["action_hash"]
            or decision.candidate_ref != action["candidate_ref"] or decision.candidate_set_ref != action["candidate_set_ref"]
            or source["decision_ref"] != summary.decision_ref or source["action_ref"] != summary.action_ref
            or sha256_json(payoff) != summary.payoff_ref or payoff_entry.content_hash != summary.payoff_ref
            or payoff["version"] != "S1_S2_EXECUTION_REPLAY_V1" or payoff["action_hash"] != decision.action_hash
            or payoff["action_artifact_ref"] != summary.action_ref
            or payoff["available_at_ns"] != summary.payoff_available_at_ns
            or payoff_entry.available_at_ns != summary.payoff_available_at_ns
            or payoff["path_ref"] != source["path_ref"]
            or any(payoff[name] != source[name] for name in (
                "existing_portfolio_ref", "replay_assumptions_ref", "fee_ref", "funding_schedule_ref"))
            or source["available_at_ns"] > summary.evidence_cutoff_ns):
        raise ValueError("lifecycle payoff/calendar/action/source linkage mismatch")
    fills = payoff["exits"]
    opening = payoff["entry"]
    payoff_inputs = payoff_entry.metadata.get("input_refs")
    if (not isinstance(payoff_inputs, (tuple, list)) or len(payoff_inputs) > 2048
            or not {summary.action_ref, source["path_ref"]}.issubset(payoff_inputs)):
        raise ValueError("lifecycle payoff required action/path references absent or unbounded")
    for ref in payoff_inputs:
        dependency = repository.get_artifact(ref)
        if dependency is None or dependency.available_at_ns > payoff_entry.available_at_ns:
            raise ValueError("lifecycle payoff dependency unavailable")
    if (summary.execution_status != payoff["status"] or summary.exit_reason != payoff["exit_reason"]
            or summary.entry_at_ns != (opening["at_ns"] if opening else None)
            or list(summary.exit_at_ns) != [fill["at_ns"] for fill in fills]
            or summary.filled_quantity != payoff["filled_quantity"]
            or summary.remaining_quantity != payoff["remaining_quantity"]):
        raise ValueError("lifecycle fill summary differs from exact payoff")
    def amount(value: Any) -> Decimal:
        return decimal_value(value, field="lifecycle monetary value", wire=True)

    net = amount(payoff["payoff"]) if payoff["payoff"] is not None else None
    fees = funding = None
    closed = payoff["status"] in {"NO_FILL", "PARTIAL_FILL", "FULL_FILL"} and not payoff["reasons"]
    if closed:
        filled = amount(payoff["filled_quantity"])
        requested = amount(identity["quantity"])
        if (amount(payoff["remaining_quantity"]) != 0 or net is None or not 0 <= filled <= requested
                or (payoff["status"] == "FULL_FILL" and filled != requested)
                or (payoff["status"] == "PARTIAL_FILL" and not 0 < filled < requested)):
            raise ValueError("lifecycle claimed closed payoff without resolved economics")
        if opening is None:
            if payoff["status"] != "NO_FILL" or fills or payoff["funding_cashflows"] or filled != 0 or net != 0:
                raise ValueError("lifecycle no-fill economics mismatch")
            fees = funding = Decimal(0)
        else:
            for fill in [opening, *fills]:
                if (amount(fill["quantity"]) <= 0 or amount(fill["price"]) <= 0 or amount(fill["fee"]) < 0
                        or not decision.decision_at_ns <= timestamp(fill["at_ns"], field="lifecycle fill") <= payoff_entry.available_at_ns):
                    raise ValueError("lifecycle fill monetary value or availability invalid")
            if (amount(opening["quantity"]) != filled or sum((amount(fill["quantity"]) for fill in fills), Decimal(0)) != filled
                    or any(fill["at_ns"] < opening["at_ns"] for fill in fills)):
                raise ValueError("lifecycle closed fill quantity or chronology mismatch")
            fees = amount(opening["fee"]) + sum((amount(fill["fee"]) for fill in fills), Decimal(0))
            path_entry = repository.get_artifact(source["path_ref"])
            if path_entry is None:
                raise ValueError("lifecycle funding path unavailable")
            for fund in payoff["funding_cashflows"]:
                if (not isinstance(fund, list) or len(fund) != 2
                        or fund[0] not in path_entry.metadata["path"].get("funding_refs", ())):
                    raise ValueError("lifecycle funding source mismatch")
            funding = sum((amount(fund[1]) for fund in payoff["funding_cashflows"]), Decimal(0))
            product = repository.get_artifact(source["product_ref"])
            if product is None:
                raise ValueError("lifecycle product multiplier unavailable")
            multiplier = amount(product.metadata["product"]["base_units_per_contract"])
            direction = {"LONG": Decimal(1), "SHORT": Decimal(-1)}[identity["side"]]
            gross = direction * multiplier * (sum((amount(fill["quantity"]) * amount(fill["price"]) for fill in fills),
                Decimal(0)) - filled * amount(opening["price"]))
            if fees < 0 or net != gross - fees + funding:
                raise ValueError("lifecycle net payoff component mismatch")
    sizing_entry = repository.get_artifact(action["sizing_ref"])
    sizing = sizing_entry.metadata.get("sizing") if sizing_entry else None
    if (sizing_entry is None or sizing_entry.artifact_type != "SizingDecisionV2" or not isinstance(sizing, Mapping)
            or sha256_json(sizing) != action["sizing_ref"] or sizing_entry.content_hash != action["sizing_ref"]
            or sizing["available_at_ns"] != sizing_entry.available_at_ns or sizing_entry.available_at_ns > action_entry.available_at_ns
            or sizing["candidate_ref"] != decision.candidate_ref or sizing["candidate_set_ref"] != decision.candidate_set_ref
            or sizing["product_ref"] != identity["product_ref"]
            or sizing["risk_policy_hash"] != identity["risk_policy_hash"]
            or sizing["risk_policy_v2_hash"] != identity["risk_policy_v2_hash"]
            or sizing["quantity"] != identity["quantity"] or sizing["status"] != "SIZED"):
        raise ValueError("lifecycle frozen sizing linkage mismatch")
    margin = amount(sizing["margin"]) if sizing.get("margin") is not None else None
    if margin is not None and margin < 0:
        raise ValueError("lifecycle frozen margin invalid")
    last_exit = max(summary.exit_at_ns) if summary.exit_at_ns else None
    duration = last_exit - summary.entry_at_ns if closed and last_exit is not None and summary.entry_at_ns is not None else None
    roi = net / margin if closed and net is not None and margin is not None and margin > 0 else None
    compact = {**body, "fees": canonical_decimal_str(fees) if fees is not None else None,
        "funding_cashflow": canonical_decimal_str(funding) if funding is not None else None,
        "net_payoff": canonical_decimal_str(net) if net is not None else None,
        "frozen_sizing_margin": canonical_decimal_str(margin) if margin is not None else None,
        "net_margin_roi": canonical_decimal_str(roi) if roi is not None else None,
        "entry_to_exit_duration_ns": duration, "authority": "ZERO"}
    row.update(row_kind="ACTION_LIFECYCLE", decision_ref=summary.decision_ref, decision_at_ns=decision.decision_at_ns,
        action_artifact_ref=summary.action_ref, action_hash=decision.action_hash, candidate_ref=decision.candidate_ref,
        candidate_set_ref=decision.candidate_set_ref, policy_id=decision.policy_id, policy_hash=decision.policy_hash,
        instrument_key_json=canonical_json(identity["key"]), status=summary.execution_status, provenance="SIMULATED",
        entry_at_ns=summary.entry_at_ns, exit_at_ns=last_exit, entry_to_exit_duration_ns=duration,
        net_payoff=compact["net_payoff"], fees=compact["fees"], funding_cashflow=compact["funding_cashflow"],
        frozen_sizing_margin=compact["frozen_sizing_margin"], net_margin_roi=compact["net_margin_roi"])
    return compact


def _feature_projection(repository: OpsRepository, entry: ArtifactIndexEntryV2,
                        row: dict[str, Any]) -> Mapping[str, Any]:
    from atlas.v2.chronology import causal_artifact
    from atlas.v2.contracts import FeatureArtifactV2

    feature = FeatureArtifactV2.from_dict(json_value(entry.metadata["feature"]))
    if (feature.content_hash != entry.artifact_ref or entry.content_hash != entry.artifact_ref
            or feature.envelope.created_at_ns != entry.created_at_ns
            or feature.envelope.available_at_ns != entry.available_at_ns or len(feature.values) > 256):
        raise ValueError("feature identity, publication chronology or width mismatch")
    # The envelope can include derived dependencies published after the market
    # cutoff. Their actual publication must precede this feature's availability.
    cache: dict[str, ArtifactIndexEntryV2 | None] = {}
    budget = [0]
    for ref in feature.envelope.input_refs:
        dependency = repository.get_artifact(ref)
        if dependency is None or not causal_artifact(repository, ref,
                cutoff_ns=feature.information_cutoff_ns, consumer_at_ns=entry.available_at_ns,
                deadline_ns=entry.available_at_ns, _cache=cache, _budget=budget):
            raise ValueError("feature lost a reconstructable causal input")
    values = {name: value.to_dict() for name, value in feature.values.items()
              if re.fullmatch(r"[A-Za-z0-9_.-]{1,96}", name)}
    if len(values) != len(feature.values):
        raise ValueError("feature identifier exceeds the compact projection contract")
    row.update(row_kind="FEATURE", feature_ref=entry.artifact_ref,
               feature_schema=feature.feature_set_version, decision_at_ns=feature.information_cutoff_ns,
               instrument_key_json=feature.key.to_canonical_json(),
               features_json=canonical_json(values),
               regimes_json=canonical_json({name: value for name, value in values.items()
                                             if name.startswith("regime.")}))
    return feature.to_dict()


def _decision_feature_context(repository: OpsRepository, candidate_entry: ArtifactIndexEntryV2,
                             row: dict[str, Any]) -> None:
    from atlas.v2.contracts import CandidateActionV2

    candidate = CandidateActionV2.from_dict(json_value(candidate_entry.metadata["candidate"]))
    if candidate.content_hash != candidate_entry.artifact_ref or candidate_entry.content_hash != candidate.content_hash:
        raise ValueError("decision candidate identity mismatch")
    feature_entry = repository.get_artifact(candidate.snapshot_hash)
    row["feature_ref"] = candidate.snapshot_hash
    if feature_entry is None or feature_entry.artifact_type != "FeatureArtifactV2":
        return  # An absent feature is never a zero or a synthetic regime.
    context: dict[str, Any] = {}
    _feature_projection(repository, feature_entry, context)
    if (context["instrument_key_json"] != candidate.key.to_canonical_json()
            or context["decision_at_ns"] > candidate.decision_at_ns
            or feature_entry.available_at_ns > candidate_entry.available_at_ns):
        raise ValueError("decision feature context identity or chronology mismatch")
    for name in ("feature_schema", "regimes_json"):
        row[name] = context[name]


def _action_prediction_projection(repository: OpsRepository, entry: ArtifactIndexEntryV2,
                                  row: dict[str, Any]) -> Mapping[str, Any]:
    from atlas.v2.science.m0 import M0_CONFIG_VERSION, M0_FEATURE_SCHEMA_VERSION, M0_MODEL_VERSION, M0PredictionV2
    from atlas.v2.science.m1 import (
        M1_FEATURE_POLICY_HASH,
        M1_MODEL_FIT_VERSION,
        M1_POLICY_HASH,
        M1_POLICY_ID,
        M1_PREDICTION_VERSION,
        M1PredictionV2,
    )

    body = json_value(entry.metadata["prediction"])
    if entry.artifact_type == "M0PredictionV2":
        body = M0PredictionV2.from_dict(body).to_dict()
        cutoff = body["training_cutoff_ns"]
        model_ref, model_kind = body["model_ref"], "M0ModelFitV2"
    else:
        if (body.get("version") != M1_PREDICTION_VERSION
                or set(body) != set(M1PredictionV2.__dataclass_fields__) | {"version"}):
            raise ValueError("unsupported M1 prediction version")
        parsed = {name: body[name] for name in M1PredictionV2.__dataclass_fields__}
        for name in ("training_row_refs", "reasons"):
            parsed[name] = tuple(parsed[name])
        parsed["expected_net_value"] = (Decimal(parsed["expected_net_value"])
                                        if parsed["expected_net_value"] is not None else None)
        if canonical_json(M1PredictionV2(**parsed).to_dict()) != canonical_json(body):
            raise ValueError("M1 prediction schema mismatch")
        cutoff = body["information_cutoff_ns"]
        model_ref, model_kind = body["model_fit_ref"], "M1ModelFitV2"
    timestamp(cutoff, field="action prediction cutoff_ns")
    if (sha256_json(body) != entry.artifact_ref or entry.content_hash != entry.artifact_ref
            or body["available_at_ns"] != entry.available_at_ns or cutoff > entry.available_at_ns):
        raise ValueError("action prediction content or chronology mismatch")
    action = repository.get_artifact(body["action_artifact_ref"])
    if (action is None or action.artifact_type != "ActionArtifactV2"
            or sha256_json(action.metadata["action_artifact"]) != action.artifact_ref
            or action.content_hash != action.artifact_ref
            or sha256_json(action.metadata["action_identity"]) != body["action_hash"]
            or action.metadata["action_artifact"]["action_hash"] != body["action_hash"]
            or action.available_at_ns > entry.available_at_ns):
        raise ValueError("prediction lost its exact frozen action")
    model = repository.get_artifact(model_ref)
    model_body = model.metadata.get("model_fit") if model is not None else None
    if (model is None or model.artifact_type != model_kind or not isinstance(model_body, Mapping)
            or sha256_json(model_body) != model_ref or model.content_hash != model_ref
            or model.available_at_ns > entry.available_at_ns):
        raise ValueError("prediction lost its immutable fitted method")
    prefix = "M0" if entry.artifact_type == "M0PredictionV2" else "M1"
    dependencies = {"support_ref": (prefix + "SupportV2", "support"),
                    "calibration_ref": (prefix + "CalibrationV2", "calibration"),
                    "oof_archive_ref": ("M0OOFResidualArchiveV2" if prefix == "M0" else "M1OOFArchiveV2",
                                        "oof_archive" if prefix == "M0" else "archive"),
                    "ood_ref": (prefix + "OODV2", "ood"),
                    "feature_vector_ref": (prefix + "FeatureVectorV2", "feature_vector")}
    for name, (kind, key) in dependencies.items():
        dependency = repository.get_artifact(body[name])
        dependency_body = dependency.metadata.get(key) if dependency is not None else None
        if (dependency is None or dependency.artifact_type != kind
                or dependency.available_at_ns > entry.available_at_ns
                or not isinstance(dependency_body, Mapping)
                or sha256_json(dependency_body) != body[name] or dependency.content_hash != body[name]
                or ("action_hash" in dependency_body and dependency_body["action_hash"] != body["action_hash"])):
            raise ValueError("prediction dependency identity or availability mismatch")
        if name == "feature_vector_ref" and (
                dependency_body["action_artifact_ref"] != action.artifact_ref
                or dependency_body["information_cutoff_ns"] != cutoff
                or dependency_body["candidate_ref"] != action.metadata["action_artifact"]["candidate_ref"]):
            raise ValueError("prediction feature vector lost its exact action/cutoff")
    value = body["expected_net_value"]
    if value is not None and (not isinstance(value, str) or not Decimal(value).is_finite()):
        raise ValueError("action prediction value is not a finite exact decimal")
    if body["status"] not in {"AVAILABLE", "NOT_ESTIMABLE"} or (body["status"] == "AVAILABLE") != (value is not None):
        raise ValueError("prediction support status conflicts with its value")
    if entry.artifact_type == "M0PredictionV2":
        if (model_body["version"] != M0_MODEL_VERSION
                or model_body["config_version"] != M0_CONFIG_VERSION
                or model_body["feature_schema_version"] != M0_FEATURE_SCHEMA_VERSION
                or model_body["current_action_hash"] != body["action_hash"]
                or model_body["current_action_artifact_ref"] != action.artifact_ref
                or model_body["current_feature_vector_ref"] != body["feature_vector_ref"]
                or model_body["training_cutoff_ns"] != cutoff):
            raise ValueError("M0 fitted method lost its exact action/cutoff")
        method_id = model_body["version"]
        config = {name: json_value(model_body.get(name)) for name in
                  ("version", "config_version", "feature_schema_version", "hyperparameters", "support_config")}
        config_hash = sha256_json(config)
    else:
        if (body["candidate_ref"] != action.metadata["action_artifact"]["candidate_ref"]
                or body["candidate_set_ref"] != action.metadata["action_artifact"]["candidate_set_ref"]
                or model_body["version"] != M1_MODEL_FIT_VERSION
                or model_body["feature_policy_hash"] != M1_FEATURE_POLICY_HASH
                or model_body["model_policy_hash"] != M1_POLICY_HASH
                or model_body["compatibility_key"] != body["compatibility_key"]
                or model_body["fit_cutoff_ns"] != cutoff
                or model_body["available_at_ns"] != model.available_at_ns):
            raise ValueError("M1 method or exact candidate identity mismatch")
        method_id = M1_POLICY_ID
        config_hash = sha256_json({name: json_value(model_body.get(name)) for name in (
            "model_policy_hash", "feature_policy_hash", "selected_parameters", "dependency_lock_hash",
            "lightgbm_version", "seed", "thread_count", "objective", "validation_metric", "tie_break")})
    row.update(row_kind="ACTION_PREDICTION", action_artifact_ref=action.artifact_ref,
               action_hash=body["action_hash"], candidate_ref=action.metadata["action_artifact"]["candidate_ref"],
               candidate_set_ref=action.metadata["action_artifact"]["candidate_set_ref"],
               policy_hash=action.metadata["action_identity"]["policy_hash"],
               instrument_key_json=canonical_json(action.metadata["action_identity"]["key"]),
               decision_at_ns=cutoff, expected_net_value=value, status=body["status"],
               method_id=method_id, method_config_hash=config_hash, model_profile_hash=model_ref)
    return body


def _stage_projection(repository: OpsRepository, entry: ArtifactIndexEntryV2,
                      row: dict[str, Any]) -> Mapping[str, Any]:
    from atlas.v2.runtime.ops_supervisor import PIPELINE_STAGE_ORDER, OpsStageResultV1, OpsSupervisorV2

    body = json_value(entry.metadata["stage_result"])
    stage = OpsStageResultV1.from_dict(body)
    event_id = entry.metadata["event_id"]
    sha256_ref(event_id, field="checkpoint event_id")
    if (canonical_json(body) != canonical_json(stage.to_dict())
            or entry.artifact_ref != OpsSupervisorV2._checkpoint_ref(event_id, stage.stage)
            or stage.content_hash != entry.content_hash or stage.completed_at_ns != entry.available_at_ns
            or entry.created_at_ns != entry.available_at_ns):
        raise ValueError("stage checkpoint identity or completion chronology mismatch")
    for ref in stage.artifact_refs:
        dependency = repository.get_artifact(ref)
        if dependency is None or dependency.available_at_ns > stage.completed_at_ns:
            raise ValueError("stage completion precedes its output")
    row.update(row_kind="PIPELINE_STAGE", event_id=event_id, source_stage=stage.stage.value,
               stage_completed_at_ns=stage.completed_at_ns, stage_order=PIPELINE_STAGE_ORDER.index(stage.stage),
               action_hash=stage.bound_action_hash, status=stage.status.value)
    return stage.to_dict()


def _receipt_projection(repository: OpsRepository, entry: ArtifactIndexEntryV2,
                      row: dict[str, Any]) -> Mapping[str, Any]:
    from atlas.v2.runtime.ops_supervisor import _receipt_from_dict
    from atlas.v2.runtime.production import decision_event_from_dict

    body = json_value(entry.metadata["receipt"])
    try:
        restored = _receipt_from_dict(body)
    except RuntimeError as error:
        raise ValueError("receipt schema mismatch") from error
    if canonical_json(restored.to_dict()) != canonical_json(body):
        raise ValueError("receipt schema mismatch")
    event = decision_event_from_dict(body["decision_event"])
    source = repository.get_artifact(event.content_hash)
    content_hash = sha256_json(body)
    receipt_ref = sha256_json({"artifact_type": "OpsSupervisorReceiptV1", "content_hash": content_hash})
    if (receipt_ref != entry.artifact_ref or entry.content_hash != content_hash
            or body["created_at_ns"] != entry.available_at_ns or entry.created_at_ns != entry.available_at_ns
            or source is None or source.artifact_type != "OpsDecisionEventSourceV1"
            or source.content_hash != event.content_hash
            or canonical_json(source.metadata["event"]) != canonical_json(event.to_dict())
            or source.available_at_ns > entry.available_at_ns or body["agent_mode"] != "DISABLED"
            or body["capital_enabled"] is not False or body["assisted_enabled"] is not False
            or body["created_at_ns"] < event.information_cutoff_ns):
        raise ValueError("receipt exact source identity, chronology or authority mismatch")
    row.update(row_kind="PIPELINE_RECEIPT", event_id=event.event_id,
               source_event_at_ns=event.source_event_at_ns, received_at_ns=event.received_at_ns,
               information_cutoff_ns=event.information_cutoff_ns, decision_at_ns=event.information_cutoff_ns,
               status=body["terminal_status"])
    return body


def _m15_projection(repository: OpsRepository, entry: ArtifactIndexEntryV2,
                    row: dict[str, Any]) -> Mapping[str, Any]:
    from atlas.v2.runtime.production import decision_event_from_dict
    from atlas.v2.science.m15_origin_accounting import M15OpportunityMissingnessV1, M15OriginAccountingRecordV1

    record = (M15OriginAccountingRecordV1.from_dict(json_value(entry.metadata["accounting"]))
              if entry.artifact_type == M15OriginAccountingRecordV1.VERSION
              else M15OpportunityMissingnessV1.from_dict(json_value(entry.metadata["missingness"])))
    if (entry.artifact_ref != record.content_hash or entry.content_hash != record.content_hash
            or entry.available_at_ns != record.observed_at_ns or entry.created_at_ns != record.observed_at_ns):
        raise ValueError("M15 origin accounting hash or observation chronology mismatch")
    source = repository.get_artifact(record.observation_index_ref)
    if (source is None or source.artifact_type != "PublicObservationIndexV2"
            or source.available_at_ns > record.observed_at_ns
            or source.metadata.get("bar_content_hash") != record.bar_ref
            or source.metadata.get("instrument_key_json") != record.instrument_key.to_canonical_json()
            or source.metadata.get("instrument_revision") != record.instrument_key.contract_revision
            or source.metadata.get("availability_class") != "ACTUAL_SYSTEM"
            or source.metadata.get("event_type") != "BAR_15M"
            or source.metadata.get("event_at_ns") != record.close_at_ns
            or source.artifact_ref != sha256_json({"artifact_type": "PublicObservationIndexV2",
                                                   "record_id": source.metadata.get("record_id")})):
        raise ValueError("M15 origin lost its exact instrument/raw-bar evidence")
    row.update(origin_ref=record.origin_ref, decision_at_ns=record.close_at_ns,
               instrument_key_json=canonical_json(record.instrument_key.to_dict()))
    if isinstance(record, M15OpportunityMissingnessV1):
        if source.available_at_ns != record.source_available_at_ns:
            raise ValueError("M15 missingness source availability mismatch")
        row.update(row_kind="MISSINGNESS", status=record.status)
    else:
        terminal = repository.get_artifact(record.accounting_ref)
        if (terminal is None or terminal.artifact_type != record.accounting_kind
                or terminal.available_at_ns > record.observed_at_ns):
            raise ValueError("M15 origin accounting lost its exact terminal evidence")
        if record.accounting_kind == M15OpportunityMissingnessV1.VERSION:
            missing_row: dict[str, Any] = {}
            _m15_projection(repository, terminal, missing_row)
            if missing_row["origin_ref"] != record.origin_ref:
                raise ValueError("M15 accounting/missingness origin mismatch")
        else:
            event = decision_event_from_dict(json_value(terminal.metadata["event"]))
            trigger = repository.get_artifact(event.trigger_ref)
            trigger_body = trigger.metadata.get("trigger") if trigger is not None else None
            if (event.content_hash != terminal.artifact_ref or terminal.content_hash != terminal.artifact_ref
                    or event.source_event_at_ns != record.close_at_ns
                    or trigger is None or trigger.artifact_type != "OpsPublicFinalBarTriggerV1"
                    or not isinstance(trigger_body, Mapping) or sha256_json(trigger_body) != trigger.artifact_ref
                    or trigger.content_hash != trigger.artifact_ref or trigger.available_at_ns > terminal.available_at_ns
                    or trigger_body.get("source_observation_ref") != record.observation_index_ref
                    or trigger_body.get("bar_ref") != record.bar_ref):
                raise ValueError("M15 accounting/event source binding mismatch")
            row["event_id"] = event.event_id
        row.update(row_kind="ORIGIN", accounting_kind=record.accounting_kind)
    return record.to_dict()


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
    if entry.artifact_type == "ResearchModelRoutingRegistryV1":
        from atlas.v2.models.research_routing import ResearchModelRouteV1

        routes = body["routes"]
        if not isinstance(routes, list) or len(routes) > 16:
            raise ValueError("registered model route list exceeds its bound")
        projected_routes = []
        for wire in routes:
            route = ResearchModelRouteV1(wire["route_id"], wire["provider_key"], wire["manifest_hash"], wire["settings"])
            if canonical_json(route.to_dict()) != canonical_json(wire):
                raise ValueError("registered model route schema mismatch")
            model = repository.get_model_manifest(route.manifest_hash)
            if model is None or model.manifest_hash != route.manifest_hash:
                raise ValueError("registered route lost its exact manifest")
            projected_routes.append({"route_ref": route.content_hash, "provider_key": route.provider_key,
                                     "manifest_hash": route.manifest_hash})
        row.update(row_kind="MODEL_REGISTRY", registered_routes_json=canonical_json(projected_routes))
        return body
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

    integer_names = {"created_at_ns", "available_at_ns", "decision_at_ns", "source_rowid", "horizon_ns", "horizon_end_ns",
                     "stage_completed_at_ns", "stage_order", "source_event_at_ns", "received_at_ns", "information_cutoff_ns",
                     "computation_started_ns", "computation_finished_ns", "computation_duration_ns", "publication_latency_ns",
                     "entry_at_ns", "exit_at_ns", "entry_to_exit_duration_ns"}
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
        "accounting_kind",
        "feature_ref", "feature_schema", "features_json", "regimes_json", "action_artifact_ref",
        "expected_net_value", "method_id", "method_config_hash", "stage_completed_at_ns", "stage_order",
        "source_event_at_ns", "received_at_ns", "information_cutoff_ns", "registered_routes_json",
        "computation_started_ns", "computation_finished_ns", "computation_duration_ns", "publication_latency_ns",
        "entry_at_ns", "exit_at_ns", "entry_to_exit_duration_ns", "fees", "funding_cashflow",
        "frozen_sizing_margin", "net_margin_roi",
    )
    return pa.schema([(name, pa.int64() if name in integer_names else pa.list_(pa.string())
                       if name in list_names else pa.string()) for name in names])


def _reconciled_report(root: Path, previous: Mapping[str, Any] | None, partition: Path,
                       identity: TuningRunIdentityV1, *, seconds: float) -> dict[str, Any]:
    """Bounded disposable analytics over checksummed, manifested rows only.

    Origin/event identities reconcile stages; they do not estimate independent
    observations. Unregistered expected origins cannot be inferred from silence.
    """
    import duckdb

    started = time.monotonic()
    paths = [str(partition)]
    seen: set[str] = set()
    prior = previous
    windowed = False
    while prior is not None:
        if len(paths) >= MAX_ANALYSIS_PARTITIONS or time.monotonic() - started >= seconds / 2:
            windowed = True
            break
        digest = sha256_json(prior)
        if digest in seen or prior["run_identity"] != identity.to_dict():
            raise ValueError("analysis manifest chain identity or cycle mismatch")
        seen.add(digest)
        checksum = prior["partition_sha256"]
        sha256_ref(checksum, field="analysis partition_sha256")
        path = root / "partitions" / f"{checksum}.parquet"
        if _file_hash(path) != checksum:
            raise ValueError("analysis partition checksum mismatch")
        paths.append(str(path))
        predecessor = prior["previous_manifest_sha256"]
        if predecessor is None:
            break
        sha256_ref(predecessor, field="analysis previous_manifest_sha256")
        prior = json.loads((root / "manifests" / f"{predecessor}.json").read_text())
        if sha256_json(prior) != predecessor:
            raise ValueError("analysis manifest predecessor checksum mismatch")
    remaining = seconds - (time.monotonic() - started)
    if remaining <= 0:
        return {"status": "TEST GATE", "reason": "ANALYSIS_HISTORY_OR_TIME_BUDGET_EXCEEDED"}
    # No operational connection or persistent analytics database is opened.
    connection = duckdb.connect(":memory:", config={"memory_limit": "128MB", "threads": "1",
                                                  "max_temp_directory_size": "0B"})
    timer = threading.Timer(remaining, connection.interrupt)
    timer.daemon = True
    timer.start()
    truncated = False

    def groups(sql: str) -> list[dict[str, Any]]:
        nonlocal truncated
        cursor = connection.execute(sql + " LIMIT 257")
        names = [column[0] for column in cursor.description]
        values = cursor.fetchall()
        truncated |= len(values) > 256
        return [dict(zip(names, row, strict=True)) for row in values[:256]]

    try:
        connection.from_parquet(paths, union_by_name=True).create_view("rows")
        window = connection.execute("""SELECT min(source_rowid),max(source_rowid),min(available_at_ns),
            max(available_at_ns),count(*) FROM rows""").fetchone()
        connection.execute("""CREATE VIEW origin_events AS
            SELECT DISTINCT event_id, origin_ref FROM rows
            WHERE row_kind='ORIGIN' AND event_id IS NOT NULL""")
        connection.execute("""CREATE VIEW decisions AS
            SELECT r.*, coalesce(o.origin_ref, r.event_id) AS opportunity_ref
            FROM rows r LEFT JOIN origin_events o ON r.event_id=o.event_id
            WHERE r.row_kind='DECISION'""")
        summary = connection.execute("""WITH opportunities AS (
            SELECT opportunity_ref AS ref FROM decisions
            UNION SELECT origin_ref FROM rows WHERE row_kind='ORIGIN'
            UNION SELECT coalesce(event_id, origin_ref) FROM rows WHERE row_kind='MISSINGNESS'
        ) SELECT count(*) FILTER (WHERE ref IS NOT NULL), count(*) FILTER (WHERE ref IS NULL)
          FROM opportunities""").fetchone()
        if summary is None:
            raise ValueError("analysis opportunity aggregate did not return a row")
        stages = groups("""SELECT policy_id, policy_hash, source_stage, selection_state, admission_state,
            count(*) AS calendar_rows, count(DISTINCT opportunity_ref) AS distinct_recorded_origins,
            count(DISTINCT candidate_ref) AS distinct_candidates,
            count(DISTINCT action_hash) AS distinct_actions
            FROM decisions GROUP BY ALL ORDER BY policy_id, policy_hash, source_stage, selection_state, admission_state""")
        coverage = groups("""SELECT d.policy_id, d.policy_hash, d.source_stage, d.selection_state,
            d.admission_state, count(DISTINCT d.decision_ref) AS calendar_entries,
            count(DISTINCT o.decision_ref) AS entries_with_any_outcome,
            count(DISTINCT o.decision_ref) FILTER (WHERE o.label_state='MATURED') AS entries_with_matured_outcome,
            count(DISTINCT o.decision_ref) FILTER (WHERE o.label_state IN ('UNRESOLVED','CENSORED'))
                AS entries_with_unresolved_or_censored_outcome
            FROM decisions d LEFT JOIN rows o ON o.row_kind='OUTCOME' AND o.decision_ref=d.decision_ref
            GROUP BY ALL ORDER BY d.policy_id, d.policy_hash, d.source_stage, d.selection_state, d.admission_state""")
        outcome_labels = groups("""SELECT outcome_target, provenance, label_state,
            count(*) AS label_rows, count(DISTINCT decision_ref) AS distinct_decisions,
            count(DISTINCT action_hash) AS distinct_actions FROM rows WHERE row_kind='OUTCOME'
            GROUP BY ALL ORDER BY outcome_target, provenance, label_state""")
        predictions = groups("""SELECT prediction_target_ref, horizon_ns, model_profile_hash, route_ref,
            instrument_key_json, label_state, count(*) AS label_rows,
            count(DISTINCT request_ref) AS distinct_requests,
            count(DISTINCT decision_ref) AS distinct_decisions,
            count(predicted_log_return) FILTER (WHERE label_state='MATURED' AND measured_log_return IS NOT NULL)
                AS paired_prediction_labels,
            avg(abs(try_cast(measured_log_return AS DOUBLE)-try_cast(predicted_log_return AS DOUBLE)))
                FILTER (WHERE label_state='MATURED') AS mean_absolute_log_return_error,
            sqrt(avg(pow(try_cast(measured_log_return AS DOUBLE)-try_cast(predicted_log_return AS DOUBLE),2))
                FILTER (WHERE label_state='MATURED')) AS root_mean_squared_log_return_error,
            avg(try_cast(measured_log_return AS DOUBLE)-try_cast(predicted_log_return AS DOUBLE))
                FILTER (WHERE label_state='MATURED') AS mean_log_return_error,
            count(json_extract_string(metrics_json,'$.quantile_interval_covered'))
                FILTER (WHERE label_state='MATURED') AS interval_label_pairs,
            avg(try_cast(json_extract_string(metrics_json,'$.quantile_interval_covered') AS DOUBLE))
                FILTER (WHERE label_state='MATURED') AS observed_90pct_interval_coverage
            FROM rows WHERE row_kind='MODEL_OUTCOME' GROUP BY ALL
            ORDER BY prediction_target_ref,horizon_ns,model_profile_hash,route_ref,instrument_key_json,label_state""")
        origins = groups("""SELECT artifact_type, accounting_kind, status,
            count(*) AS record_rows, count(DISTINCT origin_ref) AS distinct_recorded_origins
            FROM rows WHERE row_kind IN ('ORIGIN','MISSINGNESS') GROUP BY ALL
            ORDER BY artifact_type,accounting_kind,status""")
        regimes = groups("""SELECT d.policy_id,d.policy_hash,d.source_stage,d.selection_state,d.admission_state,
            d.feature_schema,d.regimes_json,count(DISTINCT d.decision_ref) AS calendar_rows,
            count(DISTINCT d.opportunity_ref) AS distinct_recorded_origins,
            count(DISTINCT o.decision_ref) FILTER (WHERE o.label_state='MATURED') AS matured_decisions
            FROM decisions d LEFT JOIN rows o ON o.row_kind='OUTCOME' AND o.decision_ref=d.decision_ref
            GROUP BY ALL ORDER BY d.policy_id,d.policy_hash,d.source_stage,d.regimes_json""")
        features = groups("""SELECT r.instrument_key_json,r.feature_schema,
            floor(r.decision_at_ns/21600000000000)::BIGINT AS utc_six_hour_bucket,
            f.key AS feature_id,json_extract_string(f.value,'$.unit') AS unit,
            count(*) AS recorded_snapshots,
            count(*) FILTER (WHERE json_extract_string(f.value,'$.value') IS NULL) AS missing_snapshots,
            avg(try_cast(json_extract_string(f.value,'$.value') AS DOUBLE)) AS observed_mean,
            stddev_pop(try_cast(json_extract_string(f.value,'$.value') AS DOUBLE)) AS observed_stddev,
            min(try_cast(json_extract_string(f.value,'$.value') AS DOUBLE)) AS observed_min,
            max(try_cast(json_extract_string(f.value,'$.value') AS DOUBLE)) AS observed_max
            FROM rows r,json_each(r.features_json) f WHERE r.row_kind='FEATURE'
            GROUP BY ALL ORDER BY r.instrument_key_json,r.feature_schema,utc_six_hour_bucket,feature_id""")
        feature_missingness = groups("""SELECT r.feature_schema,f.key AS feature_id,
            json_extract_string(f.value,'$.missing_reason') AS missing_reason,count(*) AS snapshots
            FROM rows r,json_each(r.features_json) f WHERE r.row_kind='FEATURE'
            AND json_extract_string(f.value,'$.value') IS NULL
            GROUP BY ALL ORDER BY r.feature_schema,feature_id,missing_reason""")
        disagreements = groups("""WITH p AS (
            SELECT *,row_number() OVER (PARTITION BY action_artifact_ref,action_hash,artifact_type
                ORDER BY available_at_ns DESC,artifact_ref DESC) AS position
            FROM rows WHERE row_kind='ACTION_PREDICTION'), pairs AS (
            SELECT coalesce(a.action_artifact_ref,b.action_artifact_ref) AS action_artifact_ref,
                coalesce(a.action_hash,b.action_hash) AS action_hash,
                coalesce(a.policy_hash,b.policy_hash) AS policy_hash,
                coalesce(a.instrument_key_json,b.instrument_key_json) AS instrument_key_json,
                a.method_config_hash AS m0_config_hash,b.method_config_hash AS m1_config_hash,
                a.status AS m0_status,b.status AS m1_status,
                try_cast(a.expected_net_value AS DOUBLE) AS m0_value,
                try_cast(b.expected_net_value AS DOUBLE) AS m1_value
            FROM (SELECT * FROM p WHERE artifact_type='M0PredictionV2' AND position=1) a
            FULL OUTER JOIN (SELECT * FROM p WHERE artifact_type='M1PredictionV2' AND position=1) b
                ON a.action_artifact_ref=b.action_artifact_ref AND a.action_hash=b.action_hash)
            SELECT policy_hash,instrument_key_json,m0_config_hash,m1_config_hash,m0_status,m1_status,
                count(*) AS exact_action_rows,
                count(*) FILTER (WHERE m0_value IS NOT NULL AND m1_value IS NOT NULL) AS paired_estimates,
                avg(abs(m0_value-m1_value)) AS mean_absolute_disagreement,
                count(*) FILTER (WHERE (m0_value<0 AND m1_value>0) OR (m0_value>0 AND m1_value<0))
                    AS opposed_sign_estimates
            FROM pairs GROUP BY ALL ORDER BY policy_hash,instrument_key_json,m0_config_hash,m1_config_hash""")
        stage_latency = groups("""WITH timed AS (
            SELECT s.*,lag(stage_completed_at_ns) OVER (PARTITION BY event_id ORDER BY stage_order)
                AS previous_completion,lag(stage_order) OVER (PARTITION BY event_id ORDER BY stage_order)
                AS previous_stage_order FROM rows s WHERE row_kind='PIPELINE_STAGE')
            SELECT t.source_stage,t.status,count(*) AS checkpoints,
                count(*) FILTER (WHERE previous_stage_order=t.stage_order-1
                    AND t.stage_completed_at_ns>=previous_completion) AS consecutive_completion_pairs,
                avg(t.stage_completed_at_ns-previous_completion) FILTER
                    (WHERE previous_stage_order=t.stage_order-1
                        AND t.stage_completed_at_ns>=previous_completion) AS mean_completion_interval_ns,
                max(t.stage_completed_at_ns-previous_completion) FILTER
                    (WHERE previous_stage_order=t.stage_order-1
                        AND t.stage_completed_at_ns>=previous_completion) AS max_completion_interval_ns,
                count(*) FILTER (WHERE t.stage_order>0 AND (previous_stage_order IS NULL
                    OR previous_stage_order<t.stage_order-1)) AS missing_predecessor_checkpoints,
                count(*) FILTER (WHERE t.stage_completed_at_ns<previous_completion) AS clock_regressions,
                avg(t.stage_completed_at_ns-r.information_cutoff_ns) AS mean_cutoff_to_completion_ns
            FROM timed t LEFT JOIN rows r ON r.row_kind='PIPELINE_RECEIPT' AND t.event_id=r.event_id
            GROUP BY ALL ORDER BY t.source_stage,t.status""")
        receipt_latency = groups("""SELECT status,count(*) AS receipts,
            avg(received_at_ns-source_event_at_ns) AS mean_source_to_receipt_ns,
            avg(available_at_ns-information_cutoff_ns) AS mean_cutoff_to_terminal_ns,
            max(available_at_ns-information_cutoff_ns) AS max_cutoff_to_terminal_ns
            FROM rows WHERE row_kind='PIPELINE_RECEIPT' GROUP BY ALL ORDER BY status""")
        methods = groups("""SELECT method_id,method_config_hash,policy_hash,instrument_key_json,status,
            count(*) AS prediction_rows,count(DISTINCT action_artifact_ref) AS distinct_exact_actions,
            count(expected_net_value) AS estimable_predictions,avg(try_cast(expected_net_value AS DOUBLE))
                AS mean_recorded_estimate FROM rows WHERE row_kind='ACTION_PREDICTION'
            GROUP BY ALL ORDER BY method_id,method_config_hash,policy_hash,instrument_key_json,status""")
        computations = groups("""SELECT artifact_type,source_stage,status,count(*) AS recorded_computations,
            avg(computation_duration_ns) AS mean_computation_duration_ns,
            avg(publication_latency_ns) AS mean_publication_latency_ns,
            max(publication_latency_ns) AS max_publication_latency_ns
            FROM rows WHERE row_kind IN ('COMPUTATION','PREREQUISITES')
            GROUP BY ALL ORDER BY artifact_type,source_stage,status""")
        lifecycles = groups("""SELECT status,provenance,count(*) AS lifecycle_rows,
            count(DISTINCT action_artifact_ref) AS distinct_exact_actions,
            count(net_payoff) AS resolved_payoffs,avg(try_cast(net_payoff AS DOUBLE)) AS mean_recorded_net_payoff,
            avg(try_cast(fees AS DOUBLE)) AS mean_recorded_fees,
            avg(try_cast(funding_cashflow AS DOUBLE)) AS mean_recorded_funding_cashflow,
            avg(entry_to_exit_duration_ns) AS mean_entry_to_exit_duration_ns,
            count(net_margin_roi) AS supported_margin_roi_rows,
            avg(try_cast(net_margin_roi AS DOUBLE)) AS mean_recorded_net_margin_roi
            FROM rows WHERE row_kind='ACTION_LIFECYCLE' GROUP BY ALL ORDER BY status,provenance""")
        pressure = groups("""SELECT artifact_type,source_stage,instrument_key_json,status,reason_codes,
            count(*) AS pressure_rows FROM rows WHERE row_kind='PRESSURE'
            GROUP BY ALL ORDER BY artifact_type,source_stage,reason_codes""")
        prerequisites = groups("""SELECT status,reason_codes,count(*) AS inventory_rows
            FROM rows WHERE row_kind='PREREQUISITES' GROUP BY ALL ORDER BY status,reason_codes""")
        routes = groups("""WITH declared AS (
            SELECT DISTINCT json_extract_string(route.value,'$.route_ref') AS route_ref,
                json_extract_string(route.value,'$.provider_key') AS provider_key,
                json_extract_string(route.value,'$.manifest_hash') AS model_profile_hash
            FROM rows r,json_each(r.registered_routes_json) route WHERE r.row_kind='MODEL_REGISTRY')
            SELECT d.route_ref,d.provider_key,d.model_profile_hash,
                count(*) FILTER (WHERE r.row_kind='MODEL_REQUEST') AS request_rows,
                count(*) FILTER (WHERE r.row_kind='MODEL_TERMINAL') AS terminal_rows,
                count(*) FILTER (WHERE r.row_kind='MODEL_OUTCOME' AND r.label_state='MATURED') AS matured_labels
            FROM declared d LEFT JOIN rows r ON r.route_ref=d.route_ref
            GROUP BY ALL ORDER BY d.route_ref""")
        reconciliation = connection.execute("""SELECT
            (SELECT count(*) FROM rows o WHERE o.row_kind='OUTCOME' AND NOT EXISTS
                (SELECT 1 FROM decisions d WHERE d.decision_ref=o.decision_ref)) AS outcomes_without_exported_calendar,
            (SELECT count(*) FROM rows o WHERE o.row_kind='ORIGIN' AND o.event_id IS NOT NULL AND NOT EXISTS
                (SELECT 1 FROM decisions d WHERE d.event_id=o.event_id)) AS registered_events_without_exported_calendar,
            (SELECT count(*) FROM (SELECT event_id FROM origin_events GROUP BY event_id
                HAVING count(DISTINCT origin_ref)>1)) AS conflicting_event_origin_bindings""").fetchone()
        if reconciliation is None:
            raise ValueError("analysis reconciliation aggregate did not return a row")
        return {"status": "TEST GATE" if truncated or reconciliation[2] or windowed else "TESTED",
                "scope": "VALIDATED_RECENT_PARTITION_WINDOW" if windowed else "VALIDATED_RECORDED_ORIGINS_ONLY",
                "history_omitted": windowed, "partition_count": len(paths),
                "partition_limit": MAX_ANALYSIS_PARTITIONS, "window_source_rowid_from": window[0] if window else None,
                "window_source_rowid_through": window[1] if window else None,
                "window_available_at_from_ns": window[2] if window else None,
                "window_available_at_through_ns": window[3] if window else None,
                "window_rows": window[4] if window else 0, "group_limit": 256,
                "group_limit_exceeded": truncated, "distinct_recorded_opportunities": summary[0],
                "unidentified_opportunity_rows": summary[1], "stage_funnel": stages,
                "origin_accounting": origins, "outcome_coverage_by_calendar_stage": coverage,
                "outcome_labels_by_target_and_provenance": outcome_labels,
                "prediction_diagnostics_by_target_horizon_route_instrument": predictions,
                "regime_context_by_calendar_stage": regimes,
                "descriptive_feature_stability_by_six_hour_bucket": features,
                "feature_missingness": feature_missingness,
                "m0_m1_exact_action_disagreement": disagreements,
                "pipeline_stage_completion_latency": stage_latency,
                "pipeline_receipt_latency": receipt_latency,
                "registered_action_method_comparisons": methods,
                "derived_computation_publication_latency": computations,
                "action_replay_lifecycle_diagnostics": lifecycles,
                "action_margin_roi_scope": "SIMULATED_EXACT_FROZEN_SIZING_MARGIN_DESCRIPTIVE_ONLY_NO_LEVERAGE_RULE",
                "active_work_pressure": pressure, "research_prerequisite_missingness": prerequisites,
                "registered_model_route_coverage": routes,
                "stage_latency_definition": "CHECKPOINT_COMPLETION_INTERVALS_INCLUDE_QUEUE_AND_OTHER_WORK_NOT_ISOLATED_COMPUTE",
                "feature_stability_claim": "DESCRIPTIVE_RECORDED_VALUES_ONLY_NOT_DRIFT_SIGNIFICANCE",
                "outcomes_without_exported_calendar": reconciliation[0],
                "registered_events_without_exported_calendar": reconciliation[1],
                "conflicting_event_origin_bindings": reconciliation[2],
                "expected_unregistered_origin_count": None,
                "expected_origin_scope": "REGISTERED_ORIGINS_ONLY_NO_INSTRUMENT_SLOT_EXPECTATION_CONTRACT",
                "economic_significance": "NOT ESTIMABLE",
                "independence": "SHARED_MARKET_HISTORY_AND_OVERLAPPING_LABELS_REQUIRE_DEPENDENCE_AWARE_INFERENCE"}
    except duckdb.Error:
        return {"status": "TEST GATE", "reason": "ANALYSIS_QUERY_OR_RESOURCE_BUDGET_FAILED"}
    finally:
        timer.cancel()
        timer.join()
        connection.close()


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
            with temporary.open("r+b") as sealed_partition:
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
            reconciliation = _reconciled_report(
                root, previous, target, identity,
                seconds=max(0.0, max_snapshot_seconds - (time.monotonic() - started)),
            )
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
                    "status": "TEST GATE" if failures or has_more or blocked_future
                    or reconciliation["status"] != "TESTED" else "TESTED",
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
                    "denominator_definition": "DISTINCT_VALIDATED_RECORDED_ORIGINS_WITH_CALENDAR_STAGES_SEPARATE",
                    "opportunity_denominator": reconciliation.get("distinct_recorded_opportunities"),
                    "recorded_opportunity_reconciliation": reconciliation,
                    "independence": "CONFIGURATIONS_SHARE_MARKET_HISTORY_AND_ARE_NOT_INDEPENDENT_SAMPLES",
                    "unsupported_metrics": [
                        "economic_significance_without_prospective_duration_regimes_dependence_and_support",
                        "prediction_calibration_without_typed_prediction_target_and_exact_matured_labels",
                        "isolated_per_stage_compute_latency_without_recorded_start_and_end_timestamps",
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
