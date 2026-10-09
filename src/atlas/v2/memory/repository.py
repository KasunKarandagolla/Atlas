"""Transactional repository for restart-safe opportunity memory.

This database is owned by one local ``atlas-ops`` writer. It has no venue or
capital mutation API and is intentionally separate from V1 live-control data.
"""

from __future__ import annotations

import json
import re
import sqlite3
import threading
import time
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from .._serialization import FrozenMap, canonical_json, nonblank, sha256_json, sha256_ref, timestamp
from ..contracts import OpportunityWatchV2, WatchStateV2
from ..models.protocol import ModelManifestV2
from .compressed_metadata import decode_metadata_json, encode_metadata_json
from .schema import OPS_SCHEMA_NAMESPACE, OPS_SCHEMA_VERSION, initialize, validate_read_only
from .writer_lock import OpsWriterLock

_ACTIVE_STATES = (
    WatchStateV2.DETECTED.value,
    WatchStateV2.WAITING_FOR_EVENT.value,
    WatchStateV2.READY_FOR_RECHECK.value,
    WatchStateV2.CONFIRMED.value,
)

if TYPE_CHECKING:
    from ..instruments import InstrumentKeyV2


class StaleWatchVersion(RuntimeError):
    """Raised when a caller attempts a transition from a stale snapshot."""


class DedupeConflict(RuntimeError):
    """Raised when a previously used event identity has different content."""


@dataclass(frozen=True)
class TransitionResultV2:
    watch: OpportunityWatchV2
    inserted: bool


@dataclass(frozen=True)
class OutboxItemV2:
    outbox_id: str
    watch_id: str
    event_id: str
    state_version: int
    dedupe_key: str
    payload: Mapping[str, Any]
    payload_hash: str
    created_at_ns: int
    handled_at_ns: int | None
    handling_ref: str | None


@dataclass(frozen=True)
class SourceHealthV2:
    source_id: str
    observed_at_ns: int
    available_at_ns: int
    status: str
    details_ref: str | None = None

    def __post_init__(self) -> None:
        nonblank(self.source_id, field="source_id")
        nonblank(self.status, field="status")
        timestamp(self.observed_at_ns, field="observed_at_ns")
        timestamp(self.available_at_ns, field="available_at_ns")
        if self.available_at_ns < self.observed_at_ns:
            raise ValueError("source health cannot be available before observation")
        if self.details_ref is not None:
            nonblank(self.details_ref, field="details_ref")

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "observed_at_ns": self.observed_at_ns,
            "available_at_ns": self.available_at_ns,
            "status": self.status,
            "details_ref": self.details_ref,
        }


@dataclass(frozen=True)
class ArtifactIndexEntryV2:
    artifact_ref: str
    artifact_type: str
    content_hash: str
    created_at_ns: int
    available_at_ns: int
    metadata: Mapping[str, Any]

    @classmethod
    def _from_storage_row(cls, row: sqlite3.Row, *,
                          decoded_metadata: Mapping[str, Any] | None = None) -> ArtifactIndexEntryV2:
        """Rebuild an immutable entry from the repository's canonical JSON row."""
        artifact_ref = row["artifact_ref"]
        artifact_type = row["artifact_type"]
        content_hash = row["content_hash"]
        created_at_ns = row["created_at_ns"]
        available_at_ns = row["available_at_ns"]
        sha256_ref(artifact_ref, field="artifact_ref")
        nonblank(artifact_type, field="artifact_type")
        sha256_ref(content_hash, field="content_hash")
        timestamp(created_at_ns, field="created_at_ns")
        timestamp(available_at_ns, field="available_at_ns")
        if available_at_ns < created_at_ns:
            raise ValueError("artifact available_at_ns cannot precede created_at_ns")
        metadata = (decoded_metadata if decoded_metadata is not None else
                    decode_metadata_json(artifact_type, row["metadata_json"]))
        if not isinstance(metadata, Mapping):
            raise ValueError("persisted artifact metadata must be a JSON object")
        entry = object.__new__(cls)
        object.__setattr__(entry, "artifact_ref", artifact_ref)
        object.__setattr__(entry, "artifact_type", artifact_type)
        object.__setattr__(entry, "content_hash", content_hash)
        object.__setattr__(entry, "created_at_ns", created_at_ns)
        object.__setattr__(entry, "available_at_ns", available_at_ns)
        # Storage JSON has already been normalized by its strict decoder. Use
        # the fused immutable conversion without revisiting scalar type cases
        # through the general-purpose domain-value constructor.
        frozen = (metadata if isinstance(metadata, FrozenMap) else
                  FrozenMap.from_json(metadata) if decoded_metadata is None else FrozenMap(metadata))
        object.__setattr__(entry, "metadata", frozen)
        return entry

    def __post_init__(self) -> None:
        sha256_ref(self.artifact_ref, field="artifact_ref")
        nonblank(self.artifact_type, field="artifact_type")
        sha256_ref(self.content_hash, field="content_hash")
        timestamp(self.created_at_ns, field="created_at_ns")
        timestamp(self.available_at_ns, field="available_at_ns")
        if self.available_at_ns < self.created_at_ns:
            raise ValueError("artifact available_at_ns cannot precede created_at_ns")
        object.__setattr__(self, "metadata", FrozenMap.from_json(self.metadata))


@dataclass(frozen=True)
class ArtifactIndexPageV2:
    """One bounded page from a stable artifact-index keyset traversal."""

    entries: tuple[ArtifactIndexEntryV2, ...]
    next_cursor: tuple[int, str] | None
    invalid_entry_count: int = 0
    raw_keys: tuple[tuple[int, str], ...] = ()


@dataclass(frozen=True)
class ArtifactMetadataIdentityPageV1:
    """Bounded exact identity lookup within one typed artifact metadata path."""

    entries: tuple[ArtifactIndexEntryV2, ...]
    next_cursor: tuple[int, str] | None
    has_more: bool
    invalid_entry_count: int = 0


@dataclass(frozen=True)
class LatestArtifactPageV1:
    """Newest available typed evidence with explicit bounded-read overflow."""

    entries: tuple[ArtifactIndexEntryV2, ...]
    has_more: bool
    invalid_entry_count: int = 0


@dataclass(frozen=True)
class NativeM1OriginObservationPageV1:
    """Bounded page of exact ACTUAL_SYSTEM final-M1 source origins.

    The page is keyed by full InstrumentKeyV2 and ordered by the immutable
    M1 close origin (`event_at_ns`). Multiple source revisions for one close
    produce one representative source entry, chosen by earliest availability
    and then artifact ref.
    """

    entries: tuple[ArtifactIndexEntryV2, ...]
    has_more: bool
    last_close_at_ns: int | None


@dataclass(frozen=True)
class PendingDecisionEventPageV1:
    """Bounded oldest-first page of event handoffs without durable receipts."""

    entries: tuple[ArtifactIndexEntryV2, ...]
    has_more: bool
    invalid_entry_count: int = 0


@dataclass(frozen=True)
class DueWorkItemV1:
    lane: str
    work_id: str
    source_ref: str
    created_at_ns: int
    due_at_ns: int
    payload: Mapping[str, Any]
    attempts: int


_DUE_SOURCE_LANES = {
    "DecisionCalendarEntryV2": "ACTION_OUTCOME",
    "ResearchModelTerminalV1": "PREDICTION_TERMINAL",
    "OpsDecisionEventSourceV1": "OPS_EVENT",
    "ResearchBasketForecastV2": "S8_BASKET_OUTCOME_V1",
    "OpsSupervisorReceiptV1": "FROZEN_ACTION_COMPARISON_V1",
}


def prediction_due_lane_v1(lane: str, run_id: str, config_hash: str) -> str:
    """Keep the scheduling projection isolated by immutable producer run."""
    if lane not in {"PREDICTION_TERMINAL", "PREDICTION_OUTCOME"}:
        raise ValueError("unsupported prediction due-work lane")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,96}", run_id):
        raise ValueError("prediction due-work run identity must be bounded")
    sha256_ref(config_hash, field="config_hash")
    return f"{lane}:{sha256_json({'run_id': run_id, 'config_hash': config_hash})}"


def _due_source_lane(artifact_type: str, metadata: Mapping[str, Any]) -> str | None:
    lane = _DUE_SOURCE_LANES.get(artifact_type)
    if lane == "FROZEN_ACTION_COMPARISON_V1":
        receipt = metadata.get("receipt")
        if isinstance(receipt, Mapping) and receipt.get("action_ref") is None:
            # A complete no-action receipt remains the decision denominator;
            # there is no exact action for a downstream model comparison.
            return None
    routing = metadata.get("routing")
    if lane == "PREDICTION_TERMINAL" and isinstance(routing, Mapping):
        run_id, config_hash = routing.get("run_id"), routing.get("config_hash")
        if isinstance(run_id, str) and isinstance(config_hash, str):
            try:
                return prediction_due_lane_v1(lane, run_id, config_hash)
            except ValueError:
                pass
    return lane


_ARTIFACT_METADATA_IDENTITY_EXPRESSIONS: dict[tuple[str, tuple[str, ...]], str] = {
    ("CandidateSetDecisionIndexV1", ("decision_event_id",)):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.decision_event_id') END",
    ("CandidateSetV2", ("candidate_set", "decision_event_id")):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.candidate_set.decision_event_id') END",
    ("NewsEventV2", ("event", "source_url")):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.event.source_url') END",
    ("NewsEventV2", ("event", "duplicate_group")):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.event.duplicate_group') END",
    ("EventAlertV2", ("duplicate_group",)):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.duplicate_group') END",
    ("PublicStreamContinuityReportV1", ("report", "channel")):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.report.channel') END",
    ("NewsEventV2", ("event", "source_id")):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.event.source_id') END",
    ("ActionReplaySourceEvidenceV1", ("source_evidence", "decision_ref")):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.source_evidence.decision_ref') END",
    ("MaturedOutcomeV2", ("outcome", "decision_ref")):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.outcome.decision_ref') END",
    ("ActualActionPositionBindingV2", ("binding", "action_hash")):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.binding.action_hash') END",
    ("PolicyPayoffV2", ("payoff", "action_hash")):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.payoff.action_hash') END",
    ("DiagnosticTargetEvidenceV2", ("diagnostic", "decision_ref")):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.diagnostic.decision_ref') END",
    ("OutcomeMaturityStatusV1", ("status", "decision_ref")):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.status.decision_ref') END",
    ("OpsDecisionEventSourceV1", ("native_m1_origin", "origin_ref")):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.native_m1_origin.origin_ref') END",
    ("OpsDecisionEventSourceV1", ("event", "event_id")):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.event.event_id') END",
    ("OpsDecisionEventSourceV1", ("trigger_record_id",)):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.trigger_record_id') END",
    ("OpsSupervisorReceiptIdentityV1", ("event_id",)):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.event_id') END",
    ("OpsPublicAcquisitionDeadlineGateV1", ("native_m1_origin_ref",)):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.native_m1_origin_ref') END",
    ("OpsPublicSourceReconciliationV1", ("reconciliation", "source_id")):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.reconciliation.source_id') END",
    ("S3NativeM1OriginAccountingCheckpointV1", ("instrument_key_json",)):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.instrument_key_json') END",
    ("M15OriginAccountingCheckpointV1", ("instrument_key_json",)):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.instrument_key_json') END",
    ("S4FeatureArtifactV2", ("instrument_key_json",)):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.instrument_key_json') END",
    ("OfficialCalendarCoverageEvidenceV1", ("source_id",)):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.source_id') END",
    ("EventExtractionValidationReceiptV1", ("receipt", "request_ref")):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.receipt.request_ref') END",
    ("ScheduledEventV2", ("evidence", "event_id")):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.evidence.event_id') END",
    ("M15OriginAccountingRecordV1", ("m15_origin_ref",)):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.m15_origin_ref') END",
    ("ResearchPredictionOutcomeCheckpointV1", ("checkpoint", "run_id")):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.checkpoint.run_id') END",
    ("ResearchPredictionOutcomeV1", ("prediction_outcome", "prediction_id")):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.prediction_outcome.prediction_id') END",
    ("PublicSourceHealthV2", ("health", "source_id")):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.health.source_id') END",
    ("ScannerRankEvidenceV1", ("candidate_id",)):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.candidate_id') END",
    ("ScannerRankEvidenceV1", ("decision_event_id",)):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.decision_event_id') END",
    ("EventSafetyGateV2", ("cutoff_ns",)):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.cutoff_ns') END",
    ("ResearchPrerequisiteInventoryV1", ("prerequisites", "event_id")):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.prerequisites.event_id') END",
    ("ProductContractV2", ("product", "key", "contract_revision")):
        "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.product.key.contract_revision') END",
}


def _archive_json_expression(name: str) -> str:
    # Only internal, fixed field names are passed here; never caller SQL.
    return f"CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.{name}') END"


def _public_archive_json_expression(name: str) -> str:
    """Metadata expression for public rows, with legacy identity hydration."""
    if name == "instrument_key_json":
        return (
            "CASE WHEN json_valid(metadata_json) THEN COALESCE("
            "json_extract(metadata_json,'$.instrument_key_ref'),"
            "ATLAS_PUBLIC_KEY_REF(json_extract(metadata_json,'$.instrument_key_json'))) END"
        )
    return _archive_json_expression(name)


def _public_identity_ref(key_json: str) -> str:
    from ..data.compact_observation_index import instrument_identity_ref

    return instrument_identity_ref(key_json)


def _public_metadata_query_expression(metadata_tables_enabled: bool, name: str) -> str:
    """Match compact indexes after migration and legacy indexes on old readers."""
    return (_public_archive_json_expression(name) if metadata_tables_enabled
            else _archive_json_expression(name))


def _public_metadata_query_index(metadata_tables_enabled: bool, name: str) -> str:
    return name + "_v2" if metadata_tables_enabled else name


def _public_metadata_query_key(metadata_tables_enabled: bool, key_json: str) -> str:
    return _public_identity_ref(key_json) if metadata_tables_enabled else key_json


_RECEIPT_REPLAY_CANDIDATE_LIMIT = 100_000
_RECEIPT_EVENT_TYPE_LIMIT = 16
_RECEIPT_QUERY_INDEXES = tuple(
    f"CREATE INDEX IF NOT EXISTS public_exact_receipt_{scope}_{view}_lookup_v2 ON artifact_index ("
    + ",".join(_public_archive_json_expression(name) for name in (
        ("instrument_revision", "instrument_key_json", "event_type", "availability_class")
        if scope == "key" else ("instrument_revision", "event_type", "availability_class")))
    + "," + ("available_at_ns" if view == "actual" else _public_archive_json_expression("replay_available_at_ns"))
    + " DESC," + _public_archive_json_expression("record_id") + " DESC,artifact_ref DESC) "
    "WHERE artifact_type='PublicObservationIndexV2'"
    for scope in ("key", "revision") for view in ("actual", "replay")
)


_ARCHIVE_QUERY_INDEXES = (
    "CREATE INDEX IF NOT EXISTS s3_residual_context_window ON artifact_index ("
    "json_extract(metadata_json,'$.residual.key'),json_extract(metadata_json,'$.residual.close_at_ns') DESC,"
    "artifact_ref) WHERE artifact_type='S3ResidualObservationV2'",
    "CREATE INDEX IF NOT EXISTS s3_vwap_context_window ON artifact_index ("
    "json_extract(metadata_json,'$.vwap.key'),json_extract(metadata_json,'$.vwap.information_cutoff_ns') DESC,"
    "artifact_ref) WHERE artifact_type='S3TradeVwapSnapshotV2'",
    "CREATE INDEX IF NOT EXISTS public_origin_discovery_lookup_v2 ON artifact_index ("
    + ",".join(_public_archive_json_expression(name) for name in (
        "instrument_key_json", "event_type", "availability_class"))
    + ",available_at_ns,artifact_ref) WHERE artifact_type='PublicObservationIndexV2'",
    "CREATE INDEX IF NOT EXISTS artifact_type_insertion_lookup ON artifact_index (artifact_type)",
    "CREATE INDEX IF NOT EXISTS artifact_latest_available ON artifact_index "
    "(artifact_type,available_at_ns DESC,artifact_ref DESC)",
    "CREATE INDEX IF NOT EXISTS artifact_type_created_order ON artifact_index "
    "(artifact_type,created_at_ns DESC,artifact_ref DESC,available_at_ns)",
    "CREATE INDEX IF NOT EXISTS watch_created_order ON watch "
    "(json_extract(payload_json,'$.created_at_ns') DESC,watch_id DESC)",
    "CREATE INDEX IF NOT EXISTS watch_transition_time_order ON watch_transition "
    "(transition_at_ns,watch_id,state_version)",
    "CREATE INDEX IF NOT EXISTS native_m1_checkpoint_exact_generation ON artifact_index ("
    + _archive_json_expression("instrument_key_json") + ",CAST("
    + _archive_json_expression("checkpoint.generation") + " AS INTEGER) DESC,artifact_ref DESC) "
    "WHERE artifact_type='S3NativeM1OriginAccountingCheckpointV1'",
    "CREATE INDEX IF NOT EXISTS m15_checkpoint_exact_generation ON artifact_index ("
    + _archive_json_expression("instrument_key_json") + ",CAST("
    + _archive_json_expression("checkpoint.generation") + " AS INTEGER) DESC,artifact_ref DESC) "
    "WHERE artifact_type='M15OriginAccountingCheckpointV1'",
    "CREATE INDEX IF NOT EXISTS source_health_state_lookup ON source_health "
    "(source_id,status,observed_at_ns)",
    "CREATE INDEX IF NOT EXISTS source_health_gap_lookup ON source_health "
    "(source_id,observed_at_ns DESC) WHERE status<>'HEALTHY_CURRENT'",
    "CREATE INDEX IF NOT EXISTS source_health_available_lookup ON source_health "
    "(source_id,available_at_ns DESC,observed_at_ns DESC)",
    "CREATE INDEX IF NOT EXISTS m15_accounting_origin_lookup ON artifact_index ("
    + _archive_json_expression("m15_origin_ref")
    + ",available_at_ns,created_at_ns DESC,artifact_ref DESC) "
    "WHERE artifact_type='M15OriginAccountingRecordV1'",
    "CREATE INDEX IF NOT EXISTS m15_checkpoint_key_generation_lookup ON artifact_index ("
    + _archive_json_expression("instrument_key_json")
    + ",CAST(CASE WHEN json_valid(metadata_json) THEN "
    "json_extract(metadata_json, '$.checkpoint.generation') END AS INTEGER) DESC,artifact_ref DESC) "
    "WHERE artifact_type='M15OriginAccountingCheckpointV1'",
    "CREATE INDEX IF NOT EXISTS research_prediction_checkpoint_run_lookup ON artifact_index ("
    "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.checkpoint.run_id') END,"
    "created_at_ns DESC,artifact_ref DESC,available_at_ns) "
    "WHERE artifact_type='ResearchPredictionOutcomeCheckpointV1'",
    "CREATE INDEX IF NOT EXISTS research_prediction_outcome_identity_lookup ON artifact_index ("
    "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, "
    "'$.prediction_outcome.prediction_id') END,created_at_ns DESC,artifact_ref DESC,available_at_ns) "
    "WHERE artifact_type='ResearchPredictionOutcomeV1'",
    "CREATE INDEX IF NOT EXISTS decision_event_trigger_record_lookup ON artifact_index ("
    + _archive_json_expression("trigger_record_id")
    + ",available_at_ns,created_at_ns DESC,artifact_ref DESC) "
    "WHERE artifact_type='OpsDecisionEventSourceV1'",
    "CREATE INDEX IF NOT EXISTS public_archive_history_lookup_v2 ON artifact_index ("
    + ",".join(_public_archive_json_expression(name) for name in (
        "instrument_key_json", "instrument_revision", "event_type", "availability_class", "event_at_ns"))
    + ",available_at_ns,artifact_ref) WHERE artifact_type='PublicObservationIndexV2'",
    "CREATE INDEX IF NOT EXISTS l2_archive_restart_lookup ON artifact_index ("
    + ",".join(_archive_json_expression(name) for name in ("instrument_hash", "source_id", "channel"))
    + ",available_at_ns DESC,artifact_ref DESC) WHERE artifact_type='L2FrameArchiveCheckpointV2'",
    "CREATE INDEX IF NOT EXISTS public_stream_trade_event_window ON artifact_index ("
    + ",".join(_archive_json_expression(name) for name in (
        "instrument_key_json", "instrument_revision", "source_id", "event_type"))
    + "," + _archive_json_expression("event_at_ns")
    + " DESC,available_at_ns DESC,artifact_ref DESC) WHERE artifact_type='PublicStreamTradeObservationIndexV1'",
    "CREATE INDEX IF NOT EXISTS public_stream_generic_source_lookup ON artifact_index ("
    + ",".join(_archive_json_expression(name) for name in (
        "instrument_revision", "source_id"))
    + ",available_at_ns DESC,artifact_ref DESC) WHERE artifact_type='PublicObservationIndexV2'",
    "CREATE INDEX IF NOT EXISTS public_stream_invalidation_window ON artifact_index ("
    + ",".join(_archive_json_expression(f"observation.{name}") for name in (
        "instrument", "source_id", "channel"))
    + ",available_at_ns,artifact_ref) WHERE artifact_type='PublicStreamContinuityEventV1'",
) + tuple(
    f"CREATE INDEX IF NOT EXISTS public_stream_continuity_head_{version}_progress ON artifact_index ("
    + ",".join(_archive_json_expression(f"{path}.{name}") for name in (
        "instrument.native_symbol", "channel", "source_id"))
    + ",available_at_ns DESC," + ",".join(
        f"COALESCE({_archive_json_expression(f'{path}.{name}')},0) DESC"
        for name in ("last_available_at_ns", "recovery_epoch", "observed_trade_count",
                     "last_transport_receipt_at_ns", "gap_count"))
    + f",artifact_ref DESC) WHERE artifact_type='{kind}'"
    for version, kind, path in (
        ("v1", "PublicStreamContinuityStateV1", "state"),
        ("v2", "PublicStreamContinuityCheckpointV2", "checkpoint.state"),
    )
) + _RECEIPT_QUERY_INDEXES

_LATEST_METADATA_INDEXES = tuple(
    f"CREATE INDEX IF NOT EXISTS latest_metadata_{sha256_json([artifact_type, list(path)])[:16]} "
    f"ON artifact_index ({expression},available_at_ns DESC,artifact_ref DESC) "
    f"WHERE artifact_type='{artifact_type}'"
    for (artifact_type, path), expression in _ARTIFACT_METADATA_IDENTITY_EXPRESSIONS.items()
)

_METADATA_IDENTITY_RAW_LIMIT = 1024
_CREATED_METADATA_INDEXES = tuple(
    f"CREATE INDEX IF NOT EXISTS created_metadata_{sha256_json([artifact_type, list(path)])[:16]} "
    f"ON artifact_index ({expression},created_at_ns DESC,artifact_ref DESC,available_at_ns) "
    f"WHERE artifact_type='{artifact_type}'"
    for (artifact_type, path), expression in _ARTIFACT_METADATA_IDENTITY_EXPRESSIONS.items()
)

@dataclass(frozen=True)
class RestartSnapshotV2:
    active_watches: tuple[OpportunityWatchV2, ...]
    required_events: tuple[tuple[str, str], ...]


class OpsRepository:
    """A small local SQLite repository; callers provide every durable ID."""

    def __init__(self, path: str | Path, *, read_only: bool = False) -> None:
        raw_path = str(path)
        if raw_path.startswith("file:") or "://" in raw_path:
            raise ValueError("atlas-ops requires a local SQLite path, not a shared/network URI")
        if raw_path == "":
            raise ValueError("database path must be non-empty")
        self.path = raw_path
        self.read_only = read_only
        self._lock = threading.RLock()
        self._savepoint_counter = 0
        self._public_extent_writer: Any = None
        self._public_index_cache: OrderedDict[Any, Any] = OrderedDict()
        self._public_index_decode_depth = 0
        self._public_locator_enabled = False
        self._public_metadata_enabled = False
        self._public_metadata_cache: OrderedDict[Any, Any] = OrderedDict()
        self._persistence_metrics: dict[str, Any] = {"version": "OPS_PERSISTENCE_TIMING_V1",
            "transaction_count": 0, "active_phase": "IDLE", "authority": "ZERO"}
        self._writer_lease = None if read_only or raw_path == ":memory:" else OpsWriterLock(raw_path)
        if self._writer_lease is not None:
            self._writer_lease.acquire()
        try:
            self._open_connection()
        except BaseException:
            connection = getattr(self, "_connection", None)
            if connection is not None:
                connection.close()
            if self._writer_lease is not None:
                self._writer_lease.close()
            raise

    def _open_connection(self) -> None:
        read_only = self.read_only
        if read_only:
            if self.path == ":memory:":
                raise ValueError("read-only atlas-ops access requires an existing local database file")
            absolute = Path(self.path).resolve().as_posix()
            encoded_path = quote(absolute, safe="/:\\")
            uri = f"file:{encoded_path}?mode=ro"
            self._connection = sqlite3.connect(uri, uri=True, isolation_level=None, check_same_thread=False)
        else:
            self._connection = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        from ..data.compact_observation_index import instrument_identity_ref

        def public_key_ref(value: Any) -> str | None:
            if not isinstance(value, str):
                return None
            try:
                return instrument_identity_ref(value)
            except (TypeError, ValueError):
                return None

        self._connection.create_function("ATLAS_PUBLIC_KEY_REF", 1, public_key_ref, deterministic=True)
        self._connection.execute("PRAGMA foreign_keys=ON")
        if not read_only:
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA synchronous=FULL")
        else:
            self._connection.execute("PRAGMA query_only=ON")
        if self._connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
            self._connection.close()
            raise RuntimeError("SQLite foreign keys could not be enabled")
        if not read_only and self._connection.execute("PRAGMA journal_mode").fetchone()[0].lower() != "wal":
            self._connection.close()
            raise RuntimeError("atlas-ops SQLite WAL mode is unavailable")
        if not read_only and self._connection.execute("PRAGMA synchronous").fetchone()[0] != 2:
            self._connection.close()
            raise RuntimeError("atlas-ops SQLite synchronous=FULL is unavailable")
        try:
            validate_read_only(self._connection) if read_only else initialize(self._connection)
            from ..data.compact_observation_index import BLOCK_DDL, IDENTITY_DDL, LOCATOR_DDL
            from ..data.compact_public_index import LOCATOR_DDL as STREAM_LOCATOR_DDL
            from ..data.compact_public_index import LOCATOR_TABLE as STREAM_LOCATOR_TABLE

            if not read_only:
                # Readers must see either complete legacy access paths or the
                # complete v2 representation, including all query indexes.
                self._connection.execute("BEGIN IMMEDIATE")
                for statement in (*STREAM_LOCATOR_DDL, *IDENTITY_DDL, *BLOCK_DDL, *LOCATOR_DDL):
                    self._connection.execute(statement)
                block_columns = {row["name"] for row in self._connection.execute(
                    "PRAGMA table_info(public_observation_metadata_block_v1)")}
                if "revision" not in block_columns:
                    self._connection.execute("ALTER TABLE public_observation_metadata_block_v1 "
                                             "ADD COLUMN revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0)")
                self._connection.execute(
                    "CREATE TRIGGER IF NOT EXISTS public_observation_metadata_block_revision_v1 "
                    "AFTER UPDATE OF compressed_metadata ON public_observation_metadata_block_v1 "
                    "WHEN NEW.compressed_metadata IS NOT OLD.compressed_metadata "
                    "BEGIN UPDATE public_observation_metadata_block_v1 SET revision=OLD.revision+1 "
                    "WHERE block_ref=NEW.block_ref; END")
                # The previous wide indexes are rebuildable. Drop them once the
                # digest-keyed v2 indexes are available; otherwise a broad
                # public universe would keep paying their full-JSON write cost.
                for name in (
                    "public_native_m1_source_window_lookup", "public_origin_discovery_lookup",
                    "public_origin_window_lookup", "public_archive_history_lookup",
                    "public_archive_receipt_lookup",
                    *(f"public_exact_receipt_{scope}_{view}_lookup"
                      for scope in ("key", "revision") for view in ("actual", "replay")),
                ):
                    self._connection.execute(f"DROP INDEX IF EXISTS {name}")
            self._public_locator_enabled = self._connection.execute(
                "SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?", (STREAM_LOCATOR_TABLE,)).fetchone() is not None
            self._public_metadata_enabled = all(self._connection.execute(
                "SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?", (name,)).fetchone() is not None
                for name in ("public_instrument_identity_v1", "public_observation_metadata_block_v1",
                             "public_observation_metadata_locator_v1"))
            if not read_only:
                # These are rebuildable access indexes over accepted artifact
                # rows, not new evidence tables or a new schema authority.
                for statement in (*_ARCHIVE_QUERY_INDEXES, *_LATEST_METADATA_INDEXES, *_CREATED_METADATA_INDEXES):
                    self._connection.execute(statement)
                self._connection.execute("CREATE INDEX IF NOT EXISTS scheduled_event_time_v1 "
                    "ON artifact_index(json_extract(metadata_json,'$.evidence.scheduled_at_ns'),available_at_ns) "
                    "WHERE artifact_type='ScheduledEventV2' AND json_valid(metadata_json)")
                self._connection.commit()
        except BaseException:
            self._connection.close()
            raise

    @property
    def schema_version(self) -> int:
        return OPS_SCHEMA_VERSION

    @contextmanager
    def read_snapshot(self):
        """Hold one consistent, read-only SQLite snapshot across bounded queries.

        SQLite establishes the snapshot on the first read after ``BEGIN``. In
        WAL mode, concurrent writers may continue while this connection sees
        the same committed view. The transaction is always rolled back because
        this context exists only to delimit the read snapshot.
        """
        if not self.read_only:
            raise ValueError("read snapshots require an OpsRepository opened read-only")
        with self._lock:
            if self._connection.in_transaction:
                raise RuntimeError("cannot nest an OpsRepository read snapshot")
            self._connection.execute("BEGIN")
            try:
                yield self
            finally:
                self._connection.execute("ROLLBACK")

    @property
    def schema_namespace(self) -> str:
        return OPS_SCHEMA_NAMESPACE

    def close(self) -> None:
        with self._lock:
            try:
                self._connection.close()
            finally:
                if self._writer_lease is not None:
                    self._writer_lease.close()

    def checkpoint(self) -> tuple[int, int, int]:
        """Attempt passive WAL maintenance without waiting for active readers.

        Return SQLite's busy flag, WAL frame count and checkpointed frame count.
        Call only at a writer cycle boundary; incomplete checkpoints are health
        observations, never a reason to discard evidence or interrupt readers.
        """
        if self.read_only:
            raise RuntimeError("read-only atlas-ops repository cannot checkpoint WAL")
        with self._lock:
            if self._connection.in_transaction:
                raise RuntimeError("cannot checkpoint WAL inside an active transaction")
            started = time.monotonic_ns()
            self._persistence_phase("PASSIVE_CHECKPOINT", started)
            try:
                row = self._connection.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
            finally:
                self._persistence_metrics = {**self._persistence_metrics, "active_phase": "IDLE",
                    "checkpoint_duration_ns": time.monotonic_ns() - started,
                    "checkpoint_observed_at_ns": time.time_ns()}
            if row is None or len(row) != 3:
                raise RuntimeError("SQLite returned an invalid WAL checkpoint result")
            prior = self._persistence_metrics
            progressed = (int(row[2]) >= int(row[1]) or int(row[2]) > prior.get("checkpointed_frames", 0)
                          or int(row[1]) < prior.get("wal_frames", 0))
            self._persistence_metrics = {**prior,
                "checkpoint_progress_at_ns": time.time_ns() if progressed else prior.get("checkpoint_progress_at_ns"),
                "checkpoint_busy": int(row[0]), "wal_frames": int(row[1]), "checkpointed_frames": int(row[2])}
            return int(row[0]), int(row[1]), int(row[2])

    def persistence_metrics(self) -> dict[str, Any]:
        """Bounded diagnostics readable without a DB call or the writer lock.

        The sole writer replaces this scalar record atomically. A watchdog can
        therefore observe a pending commit while that writer is blocked.
        """
        return dict(self._persistence_metrics)

    def _persistence_phase(self, phase: str, started: int) -> None:
        self._persistence_metrics = {**self._persistence_metrics, "active_phase": phase,
            "phase_started_monotonic_ns": started, "phase_observed_at_ns": time.time_ns()}

    def __enter__(self) -> OpsRepository:
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()

    def _transaction(self):
        if self.read_only:
            raise RuntimeError("read-only atlas-ops repository cannot mutate evidence")

        class Transaction:
            def __init__(self, repository: OpsRepository) -> None:
                self.repository = repository
                self.savepoint: str | None = None
                self.begin_duration_ns = 0
                self.body_started_ns = 0

            def __enter__(self) -> sqlite3.Connection:
                self.repository._lock.acquire()
                try:
                    if self.repository._connection.in_transaction:
                        self.repository._savepoint_counter += 1
                        self.savepoint = f"atlas_composition_{self.repository._savepoint_counter}"
                        self.repository._connection.execute(f"SAVEPOINT {self.savepoint}")
                    else:
                        started = time.monotonic_ns()
                        self.repository._persistence_phase("SQLITE_BEGIN", started)
                        self.repository._connection.execute("BEGIN IMMEDIATE")
                        self.begin_duration_ns = time.monotonic_ns() - started
                        self.body_started_ns = time.monotonic_ns()
                        self.repository._persistence_phase("ROW_AND_ARCHIVE_WORK", self.body_started_ns)
                except BaseException:
                    prior = self.repository._persistence_metrics
                    if self.savepoint is None and prior["active_phase"] == "SQLITE_BEGIN":
                        self.repository._persistence_metrics = {**prior, "active_phase": "IDLE",
                            "begin_duration_ns": time.monotonic_ns() - prior["phase_started_monotonic_ns"],
                            "begin_failed": True, "transaction_failed": True,
                            "transaction_observed_at_ns": time.time_ns()}
                    self.repository._lock.release()
                    raise
                return self.repository._connection

            def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
                body_duration = time.monotonic_ns() - self.body_started_ns
                commit_started = time.monotonic_ns()
                commit_duration = rollback_duration = 0
                commit_failed = False
                try:
                    if self.savepoint is not None:
                        if exc_type:
                            self.repository._connection.execute(f"ROLLBACK TO SAVEPOINT {self.savepoint}")
                        self.repository._connection.execute(f"RELEASE SAVEPOINT {self.savepoint}")
                    elif exc_type:
                        self.repository._persistence_phase("SQLITE_ROLLBACK", commit_started)
                        try:
                            self.repository._connection.rollback()
                        finally:
                            rollback_duration = time.monotonic_ns() - commit_started
                    else:
                        self.repository._persistence_phase("SQLITE_COMMIT", commit_started)
                        try:
                            self.repository._connection.commit()
                        except BaseException:
                            commit_failed = True
                            commit_duration = time.monotonic_ns() - commit_started
                            rollback_started = time.monotonic_ns()
                            self.repository._persistence_phase("SQLITE_ROLLBACK", rollback_started)
                            try:
                                self.repository._connection.rollback()
                            finally:
                                rollback_duration = time.monotonic_ns() - rollback_started
                            raise
                        else:
                            commit_duration = time.monotonic_ns() - commit_started
                finally:
                    if self.savepoint is None:
                        prior = self.repository._persistence_metrics
                        self.repository._persistence_metrics = {**prior, "active_phase": "IDLE",
                            "transaction_count": prior["transaction_count"] + 1,
                            "begin_failed": False,
                            "begin_duration_ns": self.begin_duration_ns, "row_and_archive_duration_ns": body_duration,
                            "commit_duration_ns": commit_duration, "rollback_duration_ns": rollback_duration,
                            "transaction_failed": bool(exc_type) or commit_failed,
                            "max_commit_duration_ns": max(prior.get("max_commit_duration_ns", 0), commit_duration),
                            "transaction_observed_at_ns": time.time_ns()}
                    self.repository._lock.release()

        return Transaction(self)

    @contextmanager
    def atomic_composition(self):
        """Commit controller watch transitions and evidence as one composition.

        Existing mutation methods use nested savepoints; an escaping exception
        rolls back every operation in this context, including due projections.
        """
        with self._transaction():
            yield self

    @staticmethod
    def _watch_from_row(row: sqlite3.Row) -> OpportunityWatchV2:
        watch = OpportunityWatchV2.from_dict(json.loads(row["payload_json"]))
        if sha256_json(watch.to_dict()) != row["payload_hash"]:
            raise RuntimeError("stored watch payload hash mismatch")
        return watch

    @staticmethod
    def _watch_payload(watch: OpportunityWatchV2) -> tuple[str, str]:
        payload = canonical_json(watch.to_dict())
        return payload, sha256_json(watch.to_dict())

    def create_watch(self, watch: OpportunityWatchV2) -> OpportunityWatchV2:
        if not isinstance(watch, OpportunityWatchV2):
            raise ValueError("watch must be OpportunityWatchV2")
        if watch.state != WatchStateV2.DETECTED or watch.state_version != 0 or watch.last_event_id is not None:
            raise ValueError("new watches must begin at DETECTED state_version 0 without a prior event")
        payload, digest = self._watch_payload(watch)
        with self._transaction() as connection:
            existing = connection.execute("SELECT * FROM watch WHERE watch_id=?", (watch.watch_id,)).fetchone()
            if existing is not None:
                stored = self._watch_from_row(existing)
                if existing["payload_hash"] != digest:
                    raise ValueError("watch_id already exists with different immutable initial content")
                return stored
            connection.execute(
                """INSERT INTO watch(watch_id,state,state_version,expires_at_ns,required_next_event,
                   last_event_id,last_evaluated_at_ns,payload_json,payload_hash)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (
                    watch.watch_id,
                    watch.state.value,
                    watch.state_version,
                    watch.expires_at_ns,
                    watch.required_next_event,
                    watch.last_event_id,
                    watch.last_evaluated_at_ns,
                    payload,
                    digest,
                ),
            )
        return watch

    def get_watch(self, watch_id: str) -> OpportunityWatchV2 | None:
        nonblank(watch_id, field="watch_id")
        with self._lock:
            row = self._connection.execute("SELECT * FROM watch WHERE watch_id=?", (watch_id,)).fetchone()
        return None if row is None else self._watch_from_row(row)

    def list_active_watches(self, *, limit: int | None = None) -> tuple[OpportunityWatchV2, ...]:
        if limit is not None and (type(limit) is not int or not 1 <= limit <= 4096):
            raise ValueError("active watch read limit must be between 1 and 4096")
        marks = ",".join("?" for _ in _ACTIVE_STATES)
        with self._lock:
            if limit is None:
                rows = self._connection.execute(
                    f"SELECT * FROM watch WHERE state IN ({marks}) ORDER BY expires_at_ns, watch_id",
                    _ACTIVE_STATES).fetchall()
            else:
                # A multi-state IN plus global ordering sorts all active watches
                # before applying LIMIT. Merge separately bounded equality seeks.
                rows = []
                for state in _ACTIVE_STATES:
                    rows.extend(self._connection.execute(
                        "SELECT * FROM watch INDEXED BY watch_active_order WHERE state=? "
                        "ORDER BY expires_at_ns,watch_id LIMIT ?", (state, limit)).fetchall())
                rows.sort(key=lambda row: (row["expires_at_ns"], row["watch_id"]))
                rows = rows[:limit]
        return tuple(self._watch_from_row(row) for row in rows)

    def list_watches(self, *, limit: int | None = None) -> tuple[OpportunityWatchV2, ...]:
        """Return every persisted watch, including terminal lifecycle states."""
        if limit is not None and (type(limit) is not int or not 1 <= limit <= 10_000):
            raise ValueError("watch read limit must be between 1 and 10000")
        with self._lock:
            if limit is None:
                rows = self._connection.execute(
                    "SELECT * FROM watch ORDER BY json_extract(payload_json, '$.created_at_ns'), watch_id"
                ).fetchall()
            else:
                rows = self._connection.execute(
                    "SELECT * FROM watch ORDER BY json_extract(payload_json, '$.created_at_ns') DESC, "
                    "watch_id DESC LIMIT ?",
                    (limit,),
                ).fetchall()[::-1]
        return tuple(self._watch_from_row(row) for row in rows)

    def watch_transition_history(self, *, limit: int = 10_000) -> tuple[Mapping[str, Any], ...]:
        """Read a stable bounded view of immutable watch lifecycle transitions."""
        if type(limit) is not int or not 1 <= limit <= 10_000:
            raise ValueError("watch transition read limit must be between 1 and 10000")
        with self._lock:
            rows = self._connection.execute(
                "SELECT payload_json FROM watch_transition ORDER BY transition_at_ns,watch_id,state_version LIMIT ?",
                (limit,),
            ).fetchall()
        result = tuple(json.loads(row["payload_json"]) for row in rows)
        if any(not isinstance(item, Mapping) for item in result):
            raise RuntimeError("stored watch transition history contains a non-object row")
        return result

    def transition_watch(
        self,
        watch_id: str,
        *,
        expected_state_version: int,
        event_id: str,
        event_at_ns: int,
        transition_at_ns: int,
        target_state: WatchStateV2,
        outbox_id: str,
        required_next_event: str | None = None,
        wake_at_ns: int | None = None,
        reason: str | None = None,
        handoff_receipt: str | None = None,
    ) -> TransitionResultV2:
        nonblank(watch_id, field="watch_id")
        nonblank(event_id, field="event_id")
        nonblank(outbox_id, field="outbox_id")
        if type(expected_state_version) is not int or expected_state_version < 0:
            raise ValueError("expected_state_version must be nonnegative")
        event_at_ns = timestamp(event_at_ns, field="event_at_ns")
        transition_at_ns = timestamp(transition_at_ns, field="transition_at_ns")
        target_state = WatchStateV2(target_state)
        payload_obj = {
            "watch_id": watch_id,
            "expected_state_version": expected_state_version,
            "event_id": event_id,
            "event_at_ns": event_at_ns,
            "transition_at_ns": transition_at_ns,
            "target_state": target_state.value,
            "outbox_id": outbox_id,
            "required_next_event": required_next_event,
            "wake_at_ns": wake_at_ns,
            "reason": reason,
            "handoff_receipt": handoff_receipt,
        }
        transition_json = canonical_json(payload_obj)
        transition_hash = sha256_json(payload_obj)
        target_version = expected_state_version + 1
        dedupe_key = canonical_json([watch_id, event_id, target_version])
        with self._transaction() as connection:
            duplicate = connection.execute(
                "SELECT payload_hash,result_watch_json FROM watch_transition WHERE watch_id=? AND event_id=? AND state_version=?",
                (watch_id, event_id, target_version),
            ).fetchone()
            if duplicate is not None:
                if duplicate["payload_hash"] != transition_hash:
                    raise DedupeConflict("event identity was already applied with different transition content")
                return TransitionResultV2(
                    OpportunityWatchV2.from_dict(json.loads(duplicate["result_watch_json"])), False
                )
            row = connection.execute("SELECT * FROM watch WHERE watch_id=?", (watch_id,)).fetchone()
            if row is None:
                raise KeyError(f"unknown watch_id: {watch_id}")
            current = self._watch_from_row(row)
            if current.state_version != expected_state_version:
                raise StaleWatchVersion(
                    f"watch state_version is {current.state_version}, expected {expected_state_version}"
                )
            updated = current.transition_to(
                target_state,
                event_id=event_id,
                event_at_ns=event_at_ns,
                transition_at_ns=transition_at_ns,
                required_next_event=required_next_event,
                wake_at_ns=wake_at_ns,
                reason=reason,
                handoff_receipt=handoff_receipt,
            )
            updated_json, updated_hash = self._watch_payload(updated)
            connection.execute(
                """UPDATE watch SET state=?,state_version=?,expires_at_ns=?,required_next_event=?,last_event_id=?,
                   last_evaluated_at_ns=?,payload_json=?,payload_hash=? WHERE watch_id=? AND state_version=?""",
                (
                    updated.state.value,
                    updated.state_version,
                    updated.expires_at_ns,
                    updated.required_next_event,
                    updated.last_event_id,
                    updated.last_evaluated_at_ns,
                    updated_json,
                    updated_hash,
                    watch_id,
                    expected_state_version,
                ),
            )
            connection.execute(
                """INSERT INTO watch_transition(watch_id,event_id,state_version,from_state,to_state,event_at_ns,
                   transition_at_ns,payload_json,payload_hash,result_watch_json) VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (
                    watch_id,
                    event_id,
                    updated.state_version,
                    current.state.value,
                    updated.state.value,
                    event_at_ns,
                    transition_at_ns,
                    transition_json,
                    transition_hash,
                    updated_json,
                ),
            )
            outbox_payload = {
                "event_type": "OPPORTUNITY_WATCH_TRANSITION",
                "watch_id": watch_id,
                "event_id": event_id,
                "state_version": updated.state_version,
                "state": updated.state.value,
                "watch_hash": updated_hash,
            }
            outbox_json = canonical_json(outbox_payload)
            self._before_outbox_insert(updated)
            connection.execute(
                """INSERT INTO ops_outbox(outbox_id,watch_id,event_id,state_version,dedupe_key,payload_json,
                   payload_hash,created_at_ns) VALUES(?,?,?,?,?,?,?,?)""",
                (
                    outbox_id,
                    watch_id,
                    event_id,
                    updated.state_version,
                    dedupe_key,
                    outbox_json,
                    sha256_json(outbox_payload),
                    transition_at_ns,
                ),
            )
        return TransitionResultV2(updated, True)

    def _before_outbox_insert(self, watch: OpportunityWatchV2) -> None:
        """Narrow overridable fault-injection seam used to prove transaction atomicity."""

    def pending_outbox(self, *, limit: int = 100) -> tuple[OutboxItemV2, ...]:
        if type(limit) is not int or limit <= 0:
            raise ValueError("limit must be a positive integer")
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM ops_outbox WHERE handled_at_ns IS NULL ORDER BY created_at_ns,outbox_id LIMIT ?",
                (limit,),
            ).fetchall()
        return tuple(self._outbox_from_row(row) for row in rows)

    @staticmethod
    def _outbox_from_row(row: sqlite3.Row) -> OutboxItemV2:
        payload = json.loads(row["payload_json"])
        if sha256_json(payload) != row["payload_hash"]:
            raise RuntimeError("stored outbox payload hash mismatch")
        return OutboxItemV2(
            row["outbox_id"],
            row["watch_id"],
            row["event_id"],
            row["state_version"],
            row["dedupe_key"],
            payload,
            row["payload_hash"],
            row["created_at_ns"],
            row["handled_at_ns"],
            row["handling_ref"],
        )

    def record_outbox_handling(self, outbox_id: str, *, handled_at_ns: int, handling_ref: str) -> OutboxItemV2:
        nonblank(outbox_id, field="outbox_id")
        nonblank(handling_ref, field="handling_ref")
        timestamp(handled_at_ns, field="handled_at_ns")
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM ops_outbox WHERE outbox_id=?", (outbox_id,)).fetchone()
            if row is None:
                raise KeyError(f"unknown outbox_id: {outbox_id}")
            if row["handled_at_ns"] is not None and (
                row["handled_at_ns"] != handled_at_ns or row["handling_ref"] != handling_ref
            ):
                raise DedupeConflict("outbox item already has a different handling record")
            connection.execute(
                "UPDATE ops_outbox SET handled_at_ns=?,handling_ref=? WHERE outbox_id=?",
                (handled_at_ns, handling_ref, outbox_id),
            )
            updated = connection.execute("SELECT * FROM ops_outbox WHERE outbox_id=?", (outbox_id,)).fetchone()
        return self._outbox_from_row(updated)

    def record_source_health(self, record: SourceHealthV2) -> SourceHealthV2:
        payload = record.to_dict()
        payload_json = canonical_json(payload)
        digest = sha256_json(payload)
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT payload_hash FROM source_health WHERE source_id=? AND observed_at_ns=?",
                (record.source_id, record.observed_at_ns),
            ).fetchone()
            if existing is not None and existing["payload_hash"] != digest:
                raise ValueError("source health observation conflicts at an existing source/time")
            connection.execute(
                """INSERT OR IGNORE INTO source_health(source_id,observed_at_ns,available_at_ns,status,details_ref,
                   payload_json,payload_hash) VALUES(?,?,?,?,?,?,?)""",
                (
                    record.source_id,
                    record.observed_at_ns,
                    record.available_at_ns,
                    record.status,
                    record.details_ref,
                    payload_json,
                    digest,
                ),
            )
        return record

    def source_health_history(self, source_id: str, *, limit: int | None = None) -> tuple[SourceHealthV2, ...]:
        nonblank(source_id, field="source_id")
        if limit is not None and (type(limit) is not int or not 1 <= limit <= 10_000):
            raise ValueError("source health read limit must be between 1 and 10000")
        with self._lock:
            if limit is None:
                rows = self._connection.execute(
                    "SELECT payload_json,payload_hash FROM source_health WHERE source_id=? ORDER BY observed_at_ns",
                    (source_id,),
                ).fetchall()
            else:
                rows = self._connection.execute(
                    "SELECT payload_json,payload_hash FROM source_health WHERE source_id=? "
                    "ORDER BY observed_at_ns DESC LIMIT ?",
                    (source_id, limit),
                ).fetchall()[::-1]
        result: list[SourceHealthV2] = []
        for row in rows:
            payload = json.loads(row["payload_json"])
            if sha256_json(payload) != row["payload_hash"]:
                raise RuntimeError("stored source health hash mismatch")
            result.append(SourceHealthV2(**payload))
        return tuple(result)

    def source_health_sources(self, *, limit: int = 128) -> tuple[str, ...]:
        """Seek distinct source successors without scanning health history."""
        if type(limit) is not int or not 1 <= limit <= 2_000:
            raise ValueError("source health source-ID bound must be between 1 and 2000")
        values: list[str] = []
        previous = ""
        with self._lock:
            for index in range(limit + 1):
                comparison = ">=" if index == 0 else ">"
                row = self._connection.execute(
                    "SELECT source_id FROM source_health WHERE source_id" + comparison
                    + "? ORDER BY source_id LIMIT 1", (previous,),
                ).fetchone()
                if row is None:
                    break
                source_id = row["source_id"]
                if not isinstance(source_id, str) or not source_id.strip():
                    raise ValueError("source health source-ID inventory is invalid")
                values.append(source_id)
                previous = source_id
        if len(values) > limit:
            raise ValueError("source health source-ID set exceeded its deterministic bound")
        return tuple(values)

    def source_had_unhealthy_after_healthy(self, source_id: str) -> bool:
        """Read the sticky recovery requirement without loading health history.

        A later healthy observation cannot erase a prior gap. The earliest
        healthy point and latest non-healthy point are sufficient to test the
        chronological invariant. Availability never rescues earlier health.
        """
        nonblank(source_id, field="source_id")
        with self._lock:
            first = self._connection.execute(
                "SELECT observed_at_ns FROM source_health WHERE source_id=? "
                "AND status='HEALTHY_CURRENT' ORDER BY observed_at_ns LIMIT 1",
                (source_id,),
            ).fetchone()
            if first is None:
                return False
            last = self._connection.execute(
                "SELECT observed_at_ns FROM source_health WHERE source_id=? "
                "AND status<>'HEALTHY_CURRENT' ORDER BY observed_at_ns DESC LIMIT 1",
                (source_id,),
            ).fetchone()
        return last is not None and int(last[0]) > int(first[0])

    def latest_source_health_at(self, source_id: str, *, as_of_ns: int) -> SourceHealthV2 | None:
        """Return the latest exact source health available at a causal cutoff."""
        nonblank(source_id, field="source_id")
        cutoff = timestamp(as_of_ns, field="as_of_ns")
        with self._lock:
            row = self._connection.execute(
                "SELECT payload_json,payload_hash FROM source_health WHERE source_id=? "
                "AND available_at_ns<=? AND observed_at_ns<=? "
                "ORDER BY available_at_ns DESC,observed_at_ns DESC LIMIT 1",
                (source_id, cutoff, cutoff),
            ).fetchone()
        if row is None:
            return None
        payload = json.loads(row["payload_json"])
        if sha256_json(payload) != row["payload_hash"]:
            raise RuntimeError("stored source health hash mismatch")
        return SourceHealthV2(**payload)

    def register_model_manifest(self, manifest: ModelManifestV2) -> str:
        manifest_hash = manifest.manifest_hash
        manifest_json = manifest.to_canonical_json()
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT manifest_json FROM model_registry WHERE manifest_hash=?", (manifest_hash,)
            ).fetchone()
            if row is not None and row["manifest_json"] != manifest_json:
                raise ValueError("model manifest hash collision/conflicting immutable content")
            connection.execute(
                "INSERT OR IGNORE INTO model_registry(manifest_hash,provider,checkpoint_id,promotion_status,manifest_json) VALUES(?,?,?,?,?)",
                (
                    manifest_hash,
                    manifest.provider,
                    manifest.checkpoint_id,
                    manifest.promotion_status.value,
                    manifest_json,
                ),
            )
        return manifest_hash

    def get_model_manifest(self, manifest_hash: str) -> ModelManifestV2 | None:
        sha256_ref(manifest_hash, field="manifest_hash")
        with self._lock:
            row = self._connection.execute(
                "SELECT manifest_json FROM model_registry WHERE manifest_hash=?", (manifest_hash,)
            ).fetchone()
        if row is None:
            return None
        manifest = ModelManifestV2.from_dict(json.loads(row["manifest_json"]))
        if manifest.manifest_hash != manifest_hash:
            raise RuntimeError("stored model manifest hash mismatch")
        return manifest

    def register_artifact(self, entry: ArtifactIndexEntryV2) -> ArtifactIndexEntryV2:
        return self.register_artifacts((entry,))[0]

    def register_artifacts(self, entries: Sequence[ArtifactIndexEntryV2]) -> tuple[ArtifactIndexEntryV2, ...]:
        """Atomically register a deterministic batch of immutable artifact rows."""
        batch = tuple(entries)
        if any(not isinstance(entry, ArtifactIndexEntryV2) for entry in batch):
            raise ValueError("artifact batch must contain ArtifactIndexEntryV2 entries")
        from ..data.public_archive_extents import extent_ref

        compact = tuple(entry for entry in batch
            if entry.artifact_type in ("PublicStreamFrameIndexV1", "PublicStreamTradeObservationIndexV1")
            and isinstance(entry.metadata.get("archive_chunk_id"), str)
            and re.fullmatch(r"[0-9a-f]{64}", str(entry.metadata["archive_chunk_id"])) is not None
            and self.get_artifact(extent_ref("ops-l2-frames" if entry.artifact_type == "PublicStreamFrameIndexV1"
                else "ops-observations", str(entry.metadata["archive_chunk_id"]))) is not None)
        if compact:
            selected = {entry.artifact_ref for entry in compact}
            with self.atomic_composition():
                self.register_public_archive_entries(compact)
                self.register_artifacts(tuple(entry for entry in batch if entry.artifact_ref not in selected))
            return batch
        original_batch = batch
        if self._public_locator_enabled and batch:
            known = self.get_artifact_metadata_by_refs(tuple(entry.artifact_ref for entry in batch))
            aliases = set()
            for entry in batch:
                # A compact locator must never be shadowed by a generic row
                # with the same ref and different immutable domain content.
                prior = known.get(entry.artifact_ref)
                if prior is not None and prior["artifact_type"] in (
                        "PublicStreamFrameIndexV1", "PublicStreamTradeObservationIndexV1",
                        "PublicStreamSourceHealthV1"):
                    exact = self.get_artifact(entry.artifact_ref)
                    if exact != entry:
                        raise ValueError("artifact identity conflicts with immutable public evidence")
                    with self._lock:
                        alias = self._connection.execute(
                            "SELECT 1 FROM public_stream_archive_locator_v1 WHERE artifact_ref=?",
                            (bytes.fromhex(entry.artifact_ref),)).fetchone()
                    if alias is not None:
                        aliases.add(entry.artifact_ref)
            batch = tuple(entry for entry in batch if entry.artifact_ref not in aliases)
        compact_public: dict[str, tuple[str, str, str]] = {}
        from ..data.compact_observation_index import compact_projection

        encoded: dict[str, tuple[ArtifactIndexEntryV2, str, str]] = {}
        for entry in batch:
            metadata_json = canonical_json(entry.metadata)
            storage_json = metadata_json
            if entry.artifact_type == "PublicObservationIndexV2":
                if "instrument_key_ref" in entry.metadata:
                    raise ValueError("public observation metadata uses a reserved compact identity field")
                # Legacy callers may use a different valid artifact identity.
                # Keep those rows plain rather than imposing a new domain rule.
                prepared = (compact_projection(entry.metadata) if entry.artifact_ref == sha256_json(
                    {"artifact_type": "PublicObservationIndexV2",
                     "record_id": entry.metadata.get("record_id")}) else None)
                if prepared is not None:
                    storage_json, key_ref = prepared
                    compact_public[entry.artifact_ref] = (
                        storage_json, key_ref, str(entry.metadata["instrument_key_json"]))
            previous = encoded.get(entry.artifact_ref)
            if previous is not None:
                prior_entry, prior_json, prior_storage = previous
                if (
                    prior_entry.artifact_type,
                    prior_entry.content_hash,
                    prior_entry.created_at_ns,
                    prior_entry.available_at_ns,
                    prior_json,
                    prior_storage,
                ) != (
                    entry.artifact_type,
                    entry.content_hash,
                    entry.created_at_ns,
                    entry.available_at_ns,
                    metadata_json,
                    storage_json,
                ):
                    raise ValueError("artifact batch repeats an identity with different immutable content")
            else:
                encoded[entry.artifact_ref] = (entry, metadata_json, storage_json)
        with self._transaction() as connection:
            existing: dict[str, sqlite3.Row] = {}
            refs = tuple(encoded)
            for offset in range(0, len(refs), 500):
                ref_batch = refs[offset : offset + 500]
                if not ref_batch:
                    continue
                marks = ",".join("?" for _ in ref_batch)
                rows = connection.execute(
                    f"SELECT artifact_ref,artifact_type,content_hash,created_at_ns,available_at_ns,metadata_json "
                    f"FROM artifact_index WHERE artifact_ref IN ({marks})",
                    ref_batch,
                ).fetchall()
                existing.update((row["artifact_ref"], row) for row in rows)
            from ..data.compact_observation_index import encode_blocks, register_identity

            new_public = [(entry.artifact_ref, entry.metadata)
                          for entry, _domain, _storage in encoded.values()
                          if entry.artifact_ref not in existing and entry.artifact_ref in compact_public]
            for block in encode_blocks(new_public):
                prior = connection.execute(
                    "SELECT entry_count,uncompressed_bytes,codec,compressed_metadata "
                    "FROM public_observation_metadata_block_v1 WHERE block_ref=?", (block["block_ref"],),
                ).fetchone()
                candidate = (block["entry_count"], block["uncompressed_bytes"], "zlib-v1",
                             block["compressed_metadata"])
                if prior is None:
                    connection.execute(
                        "INSERT INTO public_observation_metadata_block_v1 "
                        "(block_ref,entry_count,uncompressed_bytes,codec,compressed_metadata) VALUES(?,?,?,?,?)",
                        (block["block_ref"], *candidate),
                    )
                elif (prior["entry_count"], prior["uncompressed_bytes"], prior["codec"],
                      bytes(prior["compressed_metadata"])) != candidate:
                    raise ValueError("public metadata block identity conflicts with stored content")
                locators = block["locators"]
                marks = ",".join("?" for _ in locators)
                prior_locators = {bytes(row["artifact_ref"]): row for row in connection.execute(
                    "SELECT artifact_ref,block_ref,ordinal FROM public_observation_metadata_locator_v1 "
                    f"WHERE artifact_ref IN ({marks})", tuple(row[0] for row in locators))}
                missing_locators = []
                identities: dict[str, str] = {}
                for artifact_ref, block_ref, ordinal in locators:
                    prior_locator = prior_locators.get(artifact_ref)
                    if prior_locator is not None:
                        if (bytes(prior_locator["block_ref"]) != block_ref
                                or prior_locator["ordinal"] != ordinal):
                            raise ValueError("public observation metadata locator identity conflicts")
                    else:
                        missing_locators.append((artifact_ref, block_ref, ordinal))
                    key_ref, identity_json = compact_public[artifact_ref.hex()][1:]
                    previous_key = identities.setdefault(key_ref, identity_json)
                    if previous_key != identity_json:
                        raise ValueError("public instrument identity reference collision")
                connection.executemany(
                    "INSERT INTO public_observation_metadata_locator_v1 VALUES(?,?,?)", missing_locators)
                for key_ref, identity_json in identities.items():
                    register_identity(connection, key_ref=key_ref, canonical_key_json=identity_json)

            for entry, metadata_json, storage_json in encoded.values():
                row = existing.get(entry.artifact_ref)
                if row is not None:
                    stored_tuple = (
                        row["artifact_type"],
                        row["content_hash"],
                        row["created_at_ns"],
                        row["available_at_ns"],
                        canonical_json(self.artifact_entry_from_storage_row(row).metadata),
                    )
                    requested_tuple = (
                        entry.artifact_type,
                        entry.content_hash,
                        entry.created_at_ns,
                        entry.available_at_ns,
                        metadata_json,
                    )
                    if stored_tuple != requested_tuple:
                        raise ValueError("artifact_ref already indexes different immutable content")
                    continue
                connection.execute(
                    "INSERT INTO artifact_index(artifact_ref,artifact_type,content_hash,created_at_ns,available_at_ns,metadata_json) VALUES(?,?,?,?,?,?)",
                    (
                        entry.artifact_ref,
                        entry.artifact_type,
                        entry.content_hash,
                        entry.created_at_ns,
                        entry.available_at_ns,
                        encode_metadata_json(entry.artifact_type, storage_json),
                    ),
                )
                lane = _due_source_lane(entry.artifact_type, entry.metadata)
                if lane is not None:
                    self._schedule_artifact_origin(connection, entry, lane)
                if entry.artifact_type == "OpsSupervisorReceiptIdentityV1":
                    event_id = entry.metadata.get("event_id")
                    if isinstance(event_id, str):
                        updated = connection.execute(
                            "UPDATE due_work SET state='RETIRED',reason_code='RECEIPTED' "
                            "WHERE lane='OPS_EVENT' AND work_id=? AND state='PENDING'", (event_id,))
                        if updated.rowcount:
                            connection.execute("UPDATE due_work_pressure SET pending_count=pending_count-1,"
                                               "retired_count=retired_count+1 WHERE lane='OPS_EVENT'")
                if entry.artifact_type == "PublicCollectorCursorV2":
                    self._update_collector_head(connection, entry)
                if entry.artifact_type == "PublicObservationIndexV2":
                    close = entry.metadata.get("event_at_ns")
                    event_type = entry.metadata.get("event_type")
                    key_json = entry.metadata.get("instrument_key_json")
                    if (isinstance(event_type, str) and event_type.startswith("BAR_")
                            and isinstance(key_json, str) and type(close) is int
                            and entry.metadata.get("bar_content_hash") is not None):
                        connection.execute(
                            "UPDATE active_history_head SET dirty_available_at_ns="
                            "CASE WHEN dirty_available_at_ns IS NULL THEN ? "
                            "ELSE MIN(dirty_available_at_ns,?) END "
                            "WHERE instrument_key_json=? AND interval=? AND scan_close_at_ns>=?",
                            (entry.available_at_ns, entry.available_at_ns, key_json,
                             event_type.removeprefix("BAR_"), close))
                        connection.execute(
                            "UPDATE native_origin_window_head SET complete=0,cursor_available_ns="
                            "available_from_ns-1,cursor_ref='' WHERE instrument_key_json=? AND event_type=? "
                            "AND ? BETWEEN available_from_ns AND available_through_ns "
                            "AND (?,?)<=(cursor_available_ns,cursor_ref)",
                            (key_json,event_type,entry.available_at_ns,entry.available_at_ns,entry.artifact_ref))
        return original_batch

    def active_history_head(self, key: InstrumentKeyV2, interval: str) -> dict[str, Any] | None:
        """Read one bounded rebuildable indicator checkpoint in the single-writer store."""
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM active_history_head WHERE instrument_key_json=? AND interval=?",
                (key.to_canonical_json(), interval)).fetchone()
        return dict(row) if row is not None else None

    def s3_context_window_entries(self, key: InstrumentKeyV2, artifact_type: str, *,
                                  cutoff_ns: int) -> tuple[ArtifactIndexEntryV2,...]:
        """Seek only the frozen seven-day AR context plus its 120-minute warmup."""
        specs = {"S3ResidualObservationV2":("residual","close_at_ns","s3_residual_context_window"),
                 "S3TradeVwapSnapshotV2":("vwap","information_cutoff_ns","s3_vwap_context_window")}
        if artifact_type not in specs:
            raise ValueError("unsupported S3 context artifact type")
        timestamp(cutoff_ns,field="cutoff_ns")
        wrapper,time_field,index = specs[artifact_type]
        earliest = max(0,cutoff_ns-(7*1440+120)*60_000_000_000)
        with self._lock:
            rows = self._connection.execute(
                f"SELECT * FROM artifact_index INDEXED BY {index} WHERE artifact_type='{artifact_type}' "
                f"AND json_extract(metadata_json,'$.{wrapper}.key')=? "
                f"AND json_extract(metadata_json,'$.{wrapper}.{time_field}') BETWEEN ? AND ? "
                f"ORDER BY json_extract(metadata_json,'$.{wrapper}.{time_field}') DESC,artifact_ref LIMIT 16385",
                (key.to_canonical_json(),earliest,cutoff_ns)).fetchall()
        if len(rows)>16384:
            if not self.read_only:
                pressure = {"version": "OpsActiveWorkPressureV1", "lane": "S3_CONTEXT",
                    "artifact_type": artifact_type, "instrument_key_json": key.to_canonical_json(),
                    "information_cutoff_ns": cutoff_ns, "limit": 16384, "observed_count": len(rows),
                    "observed_count_is_lower_bound": True,
                    "reason": "S3_CONTEXT_WINDOW_POPULATION_OVERFLOW", "authority": "ZERO"}
                ref = sha256_json(pressure)
                if self.get_artifact(ref) is None:
                    published = max(time.time_ns(),cutoff_ns)
                    self.register_artifact(ArtifactIndexEntryV2(ref,"OpsActiveWorkPressureV1",ref,
                        published,published,{"pressure":pressure}))
            raise ValueError("S3_CONTEXT_WINDOW_POPULATION_OVERFLOW")
        return tuple(self.artifact_entry_from_storage_row(row) for row in rows
                     if row["available_at_ns"]<=cutoff_ns)

    def latest_healthy_source_before(self, source_id: str, *, before_ns: int) -> SourceHealthV2 | None:
        """Seek the latest certified healthy boundary preceding a repair attempt."""
        nonblank(source_id, field="source_id")
        timestamp(before_ns, field="before_ns")
        with self._lock:
            row = self._connection.execute(
                "SELECT payload_json,payload_hash FROM source_health INDEXED BY source_health_state_lookup "
                "WHERE source_id=? AND status='HEALTHY_CURRENT' AND observed_at_ns<? "
                "ORDER BY observed_at_ns DESC LIMIT 1", (source_id,before_ns)).fetchone()
        if row is None:
            return None
        body = json.loads(row["payload_json"])
        if sha256_json(body)!=row["payload_hash"]:
            raise RuntimeError("stored healthy-boundary payload hash mismatch")
        return SourceHealthV2(**body)

    def public_bar_repair_head(self, key: InstrumentKeyV2, interval: str) -> dict[str,Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM public_bar_repair_head WHERE instrument_key_json=? AND interval=?",
                (key.to_canonical_json(),interval)).fetchone()
        return dict(row) if row is not None else None

    def save_public_bar_repair_head(self, key: InstrumentKeyV2, interval: str, *,
                                    started_ns: int, close_ns: int, certificate_ref: str,
                                    available_at_ns: int) -> None:
        for name,value in (("started_ns",started_ns),("close_ns",close_ns),("available_at_ns",available_at_ns)):
            timestamp(value,field=name)
        sha256_ref(certificate_ref,field="certificate_ref")
        with self._transaction() as connection:
            connection.execute("INSERT INTO public_bar_repair_head VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(instrument_key_json,interval) DO UPDATE SET "
                "recovery_started_at_ns=excluded.recovery_started_at_ns,"
                "verified_close_at_ns=excluded.verified_close_at_ns,"
                "certificate_ref=excluded.certificate_ref,available_at_ns=excluded.available_at_ns",
                (key.to_canonical_json(),interval,started_ns,close_ns,certificate_ref,available_at_ns))

    def save_active_history_head(self, key: InstrumentKeyV2, interval: str, *,
                                state: Mapping[str, Any] | None, state_ref: str | None,
                                scan_close_at_ns: int, cutoff_ns: int, available_at_ns: int) -> None:
        """Replace only the bounded cache; immutable checkpoint/raw evidence remains retained."""
        for name, value in (("scan_close_at_ns", scan_close_at_ns), ("cutoff_ns", cutoff_ns),
                            ("available_at_ns", available_at_ns)):
            timestamp(value, field=name)
        if state_ref is not None:
            sha256_ref(state_ref, field="state_ref")
        with self._transaction() as connection:
            connection.execute(
                "INSERT INTO active_history_head VALUES(?,?,?,?,?,?,?,NULL) "
                "ON CONFLICT(instrument_key_json,interval) DO UPDATE SET "
                "state_json=excluded.state_json,state_ref=excluded.state_ref,"
                "scan_close_at_ns=excluded.scan_close_at_ns,cutoff_ns=excluded.cutoff_ns,"
                "available_at_ns=excluded.available_at_ns,"
                "dirty_available_at_ns=CASE WHEN active_history_head.dirty_available_at_ns>? "
                "THEN active_history_head.dirty_available_at_ns ELSE NULL END",
                (key.to_canonical_json(), interval, canonical_json(state) if state is not None else None,
                 state_ref, scan_close_at_ns, cutoff_ns, available_at_ns, cutoff_ns))

    def active_history_source_page(self, key: InstrumentKeyV2, interval: str, *,
                                   after_close_at_ns: int, cutoff_ns: int,
                                   limit: int = 128) -> tuple[tuple[ArtifactIndexEntryV2, ...], int, bool]:
        """Seek a fixed row page, then include every visible revision at those exact origins.

        The first seek includes unavailable revisions so its work cannot grow
        with rejected future rows. Source close bounds and the final revision
        inventory remain explicit; overflow refuses the whole page.
        """
        if interval not in {"1M", "15M", "1H", "4H"} or type(limit) is not int or not 1 <= limit <= 128:
            raise ValueError("active history page has unsupported interval/budget")
        timestamp(after_close_at_ns, field="after_close_at_ns")
        timestamp(cutoff_ns, field="cutoff_ns")
        prefix = " AND ".join(_public_metadata_query_expression(self._public_metadata_enabled, name) + "=?" for name in (
            "instrument_key_json", "instrument_revision", "event_type", "availability_class"))
        close = _archive_json_expression("event_at_ns")
        params = (_public_metadata_query_key(self._public_metadata_enabled, key.to_canonical_json()), key.contract_revision,
                  "BAR_" + interval, "ACTUAL_SYSTEM")
        with self._lock:
            origins = self._connection.execute(
                "SELECT " + close + " AS close_at_ns FROM artifact_index "
                "INDEXED BY " + _public_metadata_query_index(
                    self._public_metadata_enabled, "public_archive_history_lookup") + " "
                "WHERE artifact_type='PublicObservationIndexV2' AND " + prefix
                + " AND " + close + ">? AND " + close + "<=? ORDER BY " + close
                + ",available_at_ns,artifact_ref LIMIT ?",
                (*params, after_close_at_ns, cutoff_ns, limit + 1)).fetchall()
            if not origins:
                return (), after_close_at_ns, False
            closes = tuple(sorted({int(row[0]) for row in origins[:limit]}))
            rows = self._connection.execute(
                "SELECT * FROM artifact_index INDEXED BY " + _public_metadata_query_index(
                    self._public_metadata_enabled, "public_archive_history_lookup") + " "
                "WHERE artifact_type='PublicObservationIndexV2' AND "
                + prefix + " AND " + close + " IN (" + ",".join("?" for _ in closes)
                + ") LIMIT ?",
                (*params, *closes, limit + 1)).fetchall()
        if len(rows) > limit:
            raise ValueError("ACTIVE_HISTORY_REVISION_PAGE_OVERFLOW")
        if any(row["available_at_ns"] > cutoff_ns for row in rows):
            raise ValueError("ACTIVE_HISTORY_FUTURE_REVISION_PENDING")
        return (tuple(self.artifact_entry_from_storage_row(row) for row in rows),
                closes[-1], len(origins) > limit)

    @staticmethod
    def _update_collector_head(connection: sqlite3.Connection, entry: ArtifactIndexEntryV2) -> None:
        source, channel, sequence = (entry.metadata.get(name) for name in (
            "source_id", "channel", "high_water_sequence"))
        if (not isinstance(source, str) or not source.strip() or not isinstance(channel, str)
                or not channel.strip() or type(sequence) is not int or sequence < 0):
            raise ValueError("collector checkpoint head identity is invalid")
        connection.execute("INSERT INTO collector_cursor_head VALUES(?,?,?,?,?) "
            "ON CONFLICT(source_id,channel) DO UPDATE SET artifact_ref=excluded.artifact_ref,"
            "high_water_sequence=excluded.high_water_sequence,created_at_ns=excluded.created_at_ns "
            "WHERE (excluded.high_water_sequence,excluded.created_at_ns,excluded.artifact_ref)>"
            "(collector_cursor_head.high_water_sequence,collector_cursor_head.created_at_ns,"
            "collector_cursor_head.artifact_ref)", (source, channel, entry.artifact_ref, sequence, entry.created_at_ns))

    def collector_cursor_heads(self, *, limit: int = 128) -> tuple[ArtifactIndexEntryV2, ...]:
        """Read one materialized immutable checkpoint per stream, with bounded migration."""
        if type(limit) is not int or not 1 <= limit <= 128:
            raise ValueError("collector stream inventory exceeds its bound")
        with self._transaction() as connection:
            marker = connection.execute("SELECT last_rowid FROM due_work_discovery "
                "WHERE projection_id='COLLECTOR_HEADS_V1'").fetchone()
            cursor = int(marker[0]) if marker else 0
            if cursor >= 0:
                legacy = connection.execute("SELECT rowid AS cursor_rowid,* FROM artifact_index "
                    "WHERE artifact_type='PublicCollectorCursorV2' AND rowid>? ORDER BY rowid LIMIT 129",
                    (cursor,)).fetchall()
                for row in legacy[:128]:
                    self._update_collector_head(connection, self.artifact_entry_from_storage_row(row))
                cursor = int(legacy[127]["cursor_rowid"]) if len(legacy) > 128 else -1
                connection.execute("INSERT INTO due_work_discovery VALUES('COLLECTOR_HEADS_V1',?) "
                    "ON CONFLICT(projection_id) DO UPDATE SET last_rowid=excluded.last_rowid", (cursor,))
            rows = connection.execute("SELECT a.* FROM collector_cursor_head h "
                "JOIN artifact_index a ON a.artifact_ref=h.artifact_ref "
                "ORDER BY h.source_id,h.channel LIMIT ?", (limit + 1,)).fetchall()
        if cursor >= 0:
            raise ValueError("collector checkpoint migration pending; bounded restart must continue")
        if len(rows) > limit:
            raise ValueError("collector stream head inventory overflow")
        return tuple(self.artifact_entry_from_storage_row(row) for row in rows)

    @staticmethod
    def _schedule_artifact_origin(connection: sqlite3.Connection, entry: ArtifactIndexEntryV2,
                                  lane: str) -> None:
        work_id = entry.artifact_ref
        if lane == "S8_BASKET_OUTCOME_V1":
            basket = entry.metadata.get("basket")
            if not isinstance(basket, Mapping):
                raise ValueError("S8 forecast scheduling requires its sealed basket")
            decision = basket.get("decision_at_ns")
            if type(decision) is not int:
                raise ValueError("S8 forecast scheduling requires an exact decision timestamp")
            timestamp(decision, field="basket.decision_at_ns")
            if (entry.artifact_ref != sha256_json(basket) or basket.get("capital_authority") != "ZERO"
                    or basket.get("trade_plan_allowed") is not False):
                raise ValueError("S8 forecast scheduling identity or authority mismatch")
            maturity = decision + 4 * 3_600_000_000_000
            work_id = sha256_json({"version": "S8BasketOutcomeIdentityV1", "forecast_ref": entry.artifact_ref})
            OpsRepository._enqueue_due_work(connection, lane=lane, work_id=work_id,
                source_ref=entry.artifact_ref, created_at_ns=entry.created_at_ns, due_at_ns=maturity,
                payload={"forecast_ref": entry.artifact_ref, "decision_at_ns": decision,
                         "maturity_at_ns": maturity})
            return
        if lane == "OPS_EVENT":
            body = entry.metadata.get("event")
            if isinstance(body, Mapping) and isinstance(body.get("event_id"), str):
                work_id = body["event_id"]
                receipt_ref = sha256_json({"artifact_type": "OpsSupervisorReceiptIdentityV1",
                                          "event_id": work_id})
                if connection.execute("SELECT 1 FROM artifact_index WHERE artifact_ref=?", (receipt_ref,)).fetchone():
                    return
        OpsRepository._enqueue_due_work(connection, lane=lane, work_id=work_id,
            source_ref=entry.artifact_ref, created_at_ns=entry.created_at_ns,
            due_at_ns=entry.available_at_ns, payload={})

    @staticmethod
    def _enqueue_due_work(connection: sqlite3.Connection, *, lane: str, work_id: str,
                          source_ref: str, created_at_ns: int, due_at_ns: int,
                          payload: Mapping[str, Any]) -> None:
        encoded = canonical_json(payload)
        prior = connection.execute(
            "SELECT source_ref,created_at_ns,payload_json FROM due_work WHERE lane=? AND work_id=?",
            (lane, work_id),
        ).fetchone()
        if prior is not None:
            if tuple(prior) != (source_ref, created_at_ns, encoded):
                raise ValueError("due-work immutable source identity conflicts")
            return
        connection.execute(
            "INSERT INTO due_work(lane,work_id,source_ref,created_at_ns,due_at_ns,payload_json) VALUES(?,?,?,?,?,?)",
            (lane, work_id, source_ref, created_at_ns, due_at_ns, encoded),
        )
        connection.execute(
            "INSERT INTO due_work_pressure(lane,pending_count) VALUES(?,1) "
            "ON CONFLICT(lane) DO UPDATE SET pending_count=pending_count+1", (lane,),
        )

    def enqueue_due_work(self, *, lane: str, work_id: str, source_ref: str,
                         created_at_ns: int, due_at_ns: int, payload: Mapping[str, Any]) -> None:
        """Schedule exact immutable evidence using the existing controller writer."""
        for name, value in (("lane", lane), ("work_id", work_id), ("source_ref", source_ref)):
            nonblank(value, field=name)
            if len(value) > 128:
                raise ValueError("due-work identities must be bounded")
        timestamp(created_at_ns, field="created_at_ns")
        timestamp(due_at_ns, field="due_at_ns")
        if len(canonical_json(payload)) > 4096:
            raise ValueError("due-work payload exceeds its bound")
        with self._transaction() as connection:
            self._enqueue_due_work(connection, lane=lane, work_id=work_id, source_ref=source_ref,
                created_at_ns=created_at_ns, due_at_ns=due_at_ns, payload=payload)

    def discover_due_work(self, *, limit: int = 64) -> Mapping[str, Any]:
        """Catch up a migrated store by insertion rowid, never wrapping history.

        New artifacts enqueue atomically, so this bounded migration lane cannot
        delay newly published opportunities behind an old inventory.
        """
        if type(limit) is not int or not 1 <= limit <= 512:
            raise ValueError("due-work discovery limit must be between 1 and 512")
        with self._transaction() as connection:
            previous = connection.execute(
                "SELECT last_rowid FROM due_work_discovery WHERE projection_id='ORIGINS_V1'"
            ).fetchone()
            cursor = int(previous[0]) if previous else 0
            rows = connection.execute(
                "SELECT rowid AS origin_rowid,artifact_ref,artifact_type,created_at_ns,available_at_ns,metadata_json "
                "FROM artifact_index WHERE rowid>? ORDER BY rowid LIMIT ?", (cursor, limit),
            ).fetchall()
            for row in rows:
                lane = _DUE_SOURCE_LANES.get(row["artifact_type"])
                if lane is not None:
                    metadata: Mapping[str, Any] = {}
                    try:
                        parsed = self.artifact_entry_from_storage_row(row).metadata
                        if isinstance(parsed, Mapping):
                            metadata = parsed
                            resolved_lane = _due_source_lane(row["artifact_type"], metadata)
                            if resolved_lane is None:
                                continue
                            lane = resolved_lane
                    except (TypeError, ValueError):
                        pass  # Keep malformed origins reachable for quarantine.
                    try:
                        entry = ArtifactIndexEntryV2(row["artifact_ref"], row["artifact_type"],
                            row["artifact_ref"], row["created_at_ns"], row["available_at_ns"], metadata)
                        self._schedule_artifact_origin(connection, entry, lane)
                    except (TypeError, ValueError):
                        self._enqueue_due_work(connection, lane=lane, work_id=row["artifact_ref"],
                            source_ref=row["artifact_ref"], created_at_ns=row["created_at_ns"],
                            due_at_ns=row["available_at_ns"], payload={})
            if rows:
                cursor = int(rows[-1]["origin_rowid"])
            connection.execute(
                "INSERT INTO due_work_discovery(projection_id,last_rowid) VALUES('ORIGINS_V1',?) "
                "ON CONFLICT(projection_id) DO UPDATE SET last_rowid=excluded.last_rowid", (cursor,),
            )
            remaining = connection.execute(
                "SELECT 1 FROM artifact_index WHERE rowid>? LIMIT 1", (cursor,),
            ).fetchone() is not None
        return {"rows_inspected": len(rows), "last_rowid": cursor, "has_more": remaining}

    def due_work_items(self, lane: str, *, as_of_ns: int, limit: int = 8) -> tuple[DueWorkItemV1, ...]:
        nonblank(lane, field="lane")
        timestamp(as_of_ns, field="as_of_ns")
        if type(limit) is not int or not 1 <= limit <= 64:
            raise ValueError("due-work page limit must be between 1 and 64")
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM due_work WHERE lane=? AND state='PENDING' AND due_at_ns<=? "
                "ORDER BY due_at_ns,work_id LIMIT ?", (lane, as_of_ns, limit),
            ).fetchall()
        return tuple(DueWorkItemV1(row["lane"], row["work_id"], row["source_ref"],
            row["created_at_ns"], row["due_at_ns"], json.loads(row["payload_json"]), row["attempts"])
            for row in rows)

    def due_artifact_page(self, lane: str, *, as_of_ns: int, limit: int = 8) -> ArtifactIndexPageV2:
        """Resolve only the bounded due selection, accounting malformed sources."""
        items = self.due_work_items(lane, as_of_ns=as_of_ns, limit=limit)
        entries: list[ArtifactIndexEntryV2] = []
        raw_keys: list[tuple[int, str]] = []
        invalid = 0
        for item in items:
            raw_keys.append((item.created_at_ns, item.source_ref))
            try:
                entry = self.get_artifact(item.source_ref)
                if entry is None or entry.created_at_ns != item.created_at_ns or entry.available_at_ns > as_of_ns:
                    raise ValueError("due source identity or availability is invalid")
                entries.append(entry)
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                invalid += 1
        raw_keys.sort(reverse=True)
        return ArtifactIndexPageV2(tuple(entries), raw_keys[-1] if raw_keys else None, invalid, tuple(raw_keys))

    def reschedule_due_work(self, lane: str, work_id: str, *, due_at_ns: int, reason_code: str) -> None:
        timestamp(due_at_ns, field="due_at_ns")
        nonblank(reason_code, field="reason_code")
        with self._transaction() as connection:
            result = connection.execute(
                "UPDATE due_work SET due_at_ns=?,attempts=attempts+1,reason_code=? "
                "WHERE lane=? AND work_id=? AND state='PENDING'", (due_at_ns, reason_code, lane, work_id),
            )
            if result.rowcount != 1:
                raise ValueError("due-work reschedule requires an active exact identity")

    def retire_due_work(self, lane: str, work_id: str, *, reason_code: str) -> None:
        self._finish_due_work(lane, work_id, reason_code=reason_code, state="RETIRED")

    def quarantine_due_work(self, lane: str, work_id: str, *, reason_code: str) -> None:
        """Preserve an exact malformed source and diagnostic without hot retries."""
        self._finish_due_work(lane, work_id, reason_code=reason_code, state="QUARANTINED")

    def _finish_due_work(self, lane: str, work_id: str, *, reason_code: str, state: str) -> None:
        nonblank(reason_code, field="reason_code")
        with self._transaction() as connection:
            result = connection.execute(
                "UPDATE due_work SET state=?,reason_code=? WHERE lane=? AND work_id=? AND state='PENDING'",
                (state, reason_code, lane, work_id),
            )
            if result.rowcount:
                connection.execute(
                    "UPDATE due_work_pressure SET pending_count=pending_count-1,retired_count=retired_count+1 "
                    "WHERE lane=?", (lane,),
                )
                if state == "QUARANTINED":
                    connection.execute(
                        "INSERT INTO due_work_quarantine_pressure(lane,quarantined_count) VALUES(?,1) "
                        "ON CONFLICT(lane) DO UPDATE SET quarantined_count=quarantined_count+1", (lane,),
                    )

    def due_work_pressure(self, lane: str, *, as_of_ns: int, page_limit: int = 8) -> Mapping[str, Any]:
        """Constant-size counters and indexed oldest/overflow observations."""
        nonblank(lane, field="lane")
        timestamp(as_of_ns, field="as_of_ns")
        if type(page_limit) is not int or not 1 <= page_limit <= 64:
            raise ValueError("due-work pressure page limit must be between 1 and 64")
        with self._lock:
            counts = self._connection.execute(
                "SELECT pending_count,retired_count FROM due_work_pressure WHERE lane=?", (lane,),
            ).fetchone()
            quarantine = self._connection.execute(
                "SELECT quarantined_count FROM due_work_quarantine_pressure WHERE lane=?", (lane,),
            ).fetchone()
            oldest = self._connection.execute(
                "SELECT created_at_ns FROM due_work WHERE lane=? AND state='PENDING' "
                "ORDER BY created_at_ns,work_id LIMIT 1", (lane,),
            ).fetchone()
            ready = self._connection.execute(
                "SELECT due_at_ns FROM due_work WHERE lane=? AND state='PENDING' AND due_at_ns<=? "
                "ORDER BY due_at_ns,work_id LIMIT ?", (lane, as_of_ns, page_limit + 1),
            ).fetchall()
        return {"version": "DueWorkPressureV1", "lane": lane,
            "pending_count": int(counts[0]) if counts else 0,
            "retired_count": int(counts[1]) if counts else 0,
            "quarantined_count": int(quarantine[0]) if quarantine else 0,
            "due_count_lower_bound": len(ready), "due_page_overflow": len(ready) > page_limit,
            "oldest_pending_age_ns": max(0, as_of_ns - int(oldest[0])) if oldest else None,
            "oldest_due_age_ns": max(0, as_of_ns - int(ready[0][0])) if ready else None}

    def register_public_archive_entries(self, entries: Sequence[ArtifactIndexEntryV2],
                                        *, archive_chunk_id: str | None = None) -> None:
        """Materialize compact access locators over already durable public Arrow evidence."""
        from ..data.compact_public_index import encode_entries

        with self._transaction() as connection:
            encoded = encode_entries(self, entries, archive_chunk_id=archive_chunk_id)
            for row in encoded:
                legacy = connection.execute("SELECT * FROM artifact_index WHERE artifact_ref=?", (row[0].hex(),)).fetchone()
                if legacy is not None:
                    requested = next(entry for entry in entries if entry.artifact_ref == row[0].hex())
                    if self.artifact_entry_from_storage_row(legacy) != requested:
                        raise ValueError("compact public locator conflicts with legacy artifact")
                    continue
                prior = connection.execute("SELECT * FROM public_stream_archive_locator_v1 WHERE artifact_ref=?", (row[0],)).fetchone()
                if prior is not None:
                    if tuple(prior) != row:
                        raise ValueError("compact public locator identity conflicts")
                    continue
                connection.execute("INSERT INTO public_stream_archive_locator_v1 VALUES(?,?,?,?,?,?,?,?,?,?,?)", row)

    def get_artifact(self, artifact_ref: str) -> ArtifactIndexEntryV2 | None:
        sha256_ref(artifact_ref, field="artifact_ref")
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM artifact_index WHERE artifact_ref=?", (artifact_ref,)
            ).fetchone()
        if row is None:
            if not self._public_locator_enabled:
                return None
            with self._lock:
                compact = self._connection.execute(
                    "SELECT * FROM public_stream_archive_locator_v1 WHERE artifact_ref=?",
                    (bytes.fromhex(artifact_ref),)).fetchone()
            if compact is None:
                return None
            return self._decode_public_locator(compact)
        return self.artifact_entry_from_storage_row(row)

    def get_artifact_header(self, artifact_ref: str) -> Mapping[str, Any] | None:
        """Read an artifact's small durable identity without hydrating its payload."""
        sha256_ref(artifact_ref, field="artifact_ref")
        with self._lock:
            row = self._connection.execute(
                "SELECT artifact_type,content_hash,available_at_ns FROM artifact_index WHERE artifact_ref=?",
                (artifact_ref,),
            ).fetchone()
        if row is None:
            return None
        return {"artifact_ref": artifact_ref, "artifact_type": row[0],
                "content_hash": row[1], "available_at_ns": row[2]}

    def _decode_public_locator(self, row: sqlite3.Row) -> ArtifactIndexEntryV2:
        from ..data.compact_public_index import decode_row

        with self._lock:
            if self._public_index_decode_depth >= 2:
                raise ValueError("compact public locator dependency recursion exceeded its bound")
            self._public_index_decode_depth += 1
            try:
                return decode_row(self, row)
            finally:
                self._public_index_decode_depth -= 1

    def artifact_entry_from_storage_row(self, row: sqlite3.Row) -> ArtifactIndexEntryV2:
        """Hydrate repository storage encodings at the immutable row boundary."""
        if row["artifact_type"] == "PublicObservationIndexV2":
            projection = json.loads(row["metadata_json"])
            if isinstance(projection, Mapping) and "instrument_key_ref" in projection:
                if not self._public_metadata_enabled:
                    raise ValueError("compact public metadata tables are unavailable")
                from ..data.compact_observation_index import decode_metadata

                with self._lock:
                    metadata = decode_metadata(self._connection, artifact_ref=row["artifact_ref"],
                        projection_json=row["metadata_json"], cache=self._public_metadata_cache)
                if metadata is None:
                    raise ValueError("compact public observation metadata could not be resolved")
                return ArtifactIndexEntryV2._from_storage_row(row, decoded_metadata=metadata)
        return ArtifactIndexEntryV2._from_storage_row(row)

    def get_artifact_metadata_by_refs(self, artifact_refs: Sequence[str]) -> dict[str, Mapping[str, Any]]:
        """Read exact artifact index columns by ref without rebuilding full domain entries."""
        requested = tuple(artifact_refs)
        if any(not isinstance(ref, str) for ref in requested):
            raise ValueError("artifact reference batch must contain strings")
        refs = tuple(sorted(set(requested)))
        if len(refs) > 100_000:
            raise ValueError("artifact reference batch exceeds its bound")
        for ref in refs:
            sha256_ref(ref, field="artifact_ref")
        entries: dict[str, Mapping[str, Any]] = {}
        with self._lock:
            storage_rows: list[sqlite3.Row] = []
            for offset in range(0, len(refs), 500):
                batch = refs[offset : offset + 500]
                if not batch:
                    continue
                marks = ",".join("?" for _ in batch)
                rows = self._connection.execute(
                    f"SELECT * FROM artifact_index WHERE artifact_ref IN ({marks})",
                    batch,
                ).fetchall()
                storage_rows.extend(rows)
            # Global reference hashes interleave unrelated compressed blocks.
            # Decode each block together, across SQL lookup pages, so a batch
            # larger than the eight-block cache does not repeatedly inflate
            # hundreds of rows for every requested reference. Each row still
            # passes the full locator/projection/hash/identity validation.
            for row in self._storage_rows_in_decode_order(storage_rows):
                entry = self.artifact_entry_from_storage_row(row)
                entries[row["artifact_ref"]] = {
                    "artifact_type": entry.artifact_type,
                    "content_hash": entry.content_hash,
                    "available_at_ns": entry.available_at_ns,
                    "metadata": entry.metadata,
                }
        if self._public_locator_enabled:
            missing = tuple(ref for ref in refs if ref not in entries)
            with self._lock:
                for offset in range(0, len(missing), 500):
                    batch = missing[offset:offset + 500]
                    if not batch:
                        continue
                    rows = self._connection.execute(
                        "SELECT * FROM public_stream_archive_locator_v1 WHERE artifact_ref IN ("
                        + ",".join("?" for _ in batch) + ")", tuple(bytes.fromhex(ref) for ref in batch)).fetchall()
                    for row in rows:
                        entry = self._decode_public_locator(row)
                        entries[entry.artifact_ref] = {"artifact_type": entry.artifact_type,
                            "content_hash": entry.content_hash, "available_at_ns": entry.available_at_ns,
                            "metadata": entry.metadata}
        return {ref: entries[ref] for ref in refs if ref in entries}

    def _storage_rows_in_decode_order(self, rows: Sequence[sqlite3.Row]) -> list[sqlite3.Row]:
        """Group a caller's already bounded row population by immutable block.

        Caller-visible pagination and evidence order are restored after decode.
        Malformed row refs stay in the population for the normal row validator;
        this lookup optimization must not erase invalid evidence accounting.
        """
        block_order: dict[str, tuple[bytes, int]] = {}
        if self._public_metadata_enabled:
            public_refs = []
            for row in rows:
                if row["artifact_type"] != "PublicObservationIndexV2":
                    continue
                try:
                    ref = bytes.fromhex(row["artifact_ref"])
                except (TypeError, ValueError):
                    continue
                if len(ref) == 32:
                    public_refs.append(ref)
            with self._lock:
                for offset in range(0, len(public_refs), 500):
                    batch = public_refs[offset:offset + 500]
                    locators = self._connection.execute(
                        "SELECT artifact_ref,block_ref,ordinal FROM public_observation_metadata_locator_v1 "
                        "WHERE artifact_ref IN (" + ",".join("?" for _ in batch) + ")", batch).fetchall()
                    block_order.update((bytes(row["artifact_ref"]).hex(),
                        (bytes(row["block_ref"]), row["ordinal"])) for row in locators)
        return sorted(rows, key=lambda row: (*block_order.get(row["artifact_ref"], (b"", -1)),
            str(row["artifact_ref"])))

    def _artifact_entries_from_storage_rows(self, rows: Sequence[sqlite3.Row]) -> tuple[ArtifactIndexEntryV2, ...]:
        decoded = {row["artifact_ref"]: self.artifact_entry_from_storage_row(row)
            for row in self._storage_rows_in_decode_order(rows)}
        return tuple(decoded[row["artifact_ref"]] for row in rows)

    def _valid_artifact_entries_from_storage_rows(
        self, rows: Sequence[sqlite3.Row],
    ) -> tuple[tuple[ArtifactIndexEntryV2, ...], int]:
        """Decode a bounded result in block order and retain its invalid-row count."""
        decoded: dict[str, ArtifactIndexEntryV2] = {}
        invalid = 0
        for row in self._storage_rows_in_decode_order(rows):
            try:
                decoded[row["artifact_ref"]] = self.artifact_entry_from_storage_row(row)
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                invalid += 1
        return tuple(decoded[row["artifact_ref"]] for row in rows if row["artifact_ref"] in decoded), invalid

    def artifact_entries(self, artifact_type: str) -> tuple[ArtifactIndexEntryV2, ...]:
        """Read a small typed artifact-index namespace without adding a warehouse table."""
        nonblank(artifact_type, field="artifact_type")
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM artifact_index WHERE artifact_type=? ORDER BY created_at_ns,artifact_ref",
                (artifact_type,),
            ).fetchall()
        result = self._artifact_entries_from_storage_rows(rows)
        from ..data.compact_public_index import KINDS

        if self._public_locator_enabled and artifact_type in KINDS:
            with self._lock:
                compact = self._connection.execute("SELECT * FROM public_stream_archive_locator_v1 WHERE kind=? "
                    "ORDER BY created_at_ns,artifact_ref", (KINDS[artifact_type],)).fetchall()
            result = (*result, *(self._decode_public_locator(row) for row in compact))
        return result

    def latest_artifact_entries(
        self, artifact_type: str, *, as_of_ns: int, limit: int,
        metadata_path: tuple[str, ...] | None = None, identity_value: str | int | None = None,
    ) -> LatestArtifactPageV1:
        """Read at most ``limit + 1`` indexed newest rows, reporting malformed rows.

        Exact metadata filtering uses the same registered identity expressions
        as maturation lookups. Callers must handle overflow explicitly.
        """
        nonblank(artifact_type, field="artifact_type")
        cutoff = timestamp(as_of_ns, field="as_of_ns")
        if type(limit) is not int or not 1 <= limit <= 4096:
            raise ValueError("latest artifact page size must be between 1 and 4096")
        if (metadata_path is None) != (identity_value is None):
            raise ValueError("latest artifact metadata path and identity must be supplied together")
        params: tuple[Any, ...]
        if metadata_path is not None:
            if (not isinstance(metadata_path, tuple) or not metadata_path or any(
                    not isinstance(item, str) or not item or not item.isascii()
                    or not item.replace("_", "a").isalnum() for item in metadata_path)):
                raise ValueError("latest artifact metadata path must contain ASCII identifier components")
            expression = _ARTIFACT_METADATA_IDENTITY_EXPRESSIONS.get((artifact_type, metadata_path))
            if expression is None:
                raise ValueError("metadata identity path has no bounded artifact index")
            if isinstance(identity_value, str):
                nonblank(identity_value, field="identity_value")
            elif type(identity_value) is not int or not -(2**63) <= identity_value < 2**63:
                raise ValueError("metadata identity must be a string or a SQLite integer")
            query = ("SELECT * FROM artifact_index WHERE "
                f"artifact_type='{artifact_type}' AND {expression}=? AND available_at_ns<=? "
                "ORDER BY available_at_ns DESC,artifact_ref DESC LIMIT ?")
            params = (identity_value, cutoff, limit + 1)
        else:
            query = ("SELECT * FROM artifact_index WHERE artifact_type=? AND available_at_ns<=? "
                "ORDER BY available_at_ns DESC,artifact_ref DESC LIMIT ?")
            params = (artifact_type, cutoff, limit + 1)
        with self._lock:
            rows = self._connection.execute(query, params).fetchall()
        entries, invalid = self._valid_artifact_entries_from_storage_rows(rows[:limit])
        return LatestArtifactPageV1(entries, len(rows) > limit, invalid)

    def scheduled_event_window(self, *, cutoff_ns: int, limit: int = 64) -> LatestArtifactPageV1:
        """Indexed blackout interval with latest known revisions for each event."""
        timestamp(cutoff_ns, field="scheduled calendar cutoff")
        if type(limit) is not int or not 1 <= limit <= 256:
            raise ValueError("scheduled calendar population bound invalid")
        start, end = max(0, cutoff_ns - 15 * 60 * 1_000_000_000), cutoff_ns + 30 * 60 * 1_000_000_000
        with self._lock:
            rows = self._connection.execute("SELECT * FROM artifact_index WHERE artifact_type='ScheduledEventV2' "
                "AND json_valid(metadata_json) AND json_extract(metadata_json,'$.evidence.scheduled_at_ns')>=? "
                "AND json_extract(metadata_json,'$.evidence.scheduled_at_ns')<=? AND available_at_ns<=? "
                "ORDER BY available_at_ns DESC,artifact_ref DESC LIMIT ?", (start, end, cutoff_ns, limit + 1)).fetchall()
        entries: dict[str, ArtifactIndexEntryV2] = {}
        invalid = 0
        for row in rows[:limit]:
            try:
                entry = self.artifact_entry_from_storage_row(row)
                event_id = entry.metadata["evidence"]["event_id"]
                if not isinstance(event_id, str):
                    raise ValueError("scheduled event identity missing")
                if event_id in entries:
                    continue
                page = self.latest_artifact_entries("ScheduledEventV2", as_of_ns=cutoff_ns, limit=1,
                    metadata_path=("evidence", "event_id"), identity_value=event_id)
                if page.invalid_entry_count or not page.entries:
                    raise ValueError("scheduled event revision unavailable")
                latest = page.entries[0]
                scheduled = latest.metadata["evidence"]["scheduled_at_ns"]
                if type(scheduled) is not int:
                    raise ValueError("scheduled event time missing")
                if start <= scheduled <= end:
                    entries[event_id] = latest
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                invalid += 1
        return LatestArtifactPageV1(tuple(sorted(entries.values(), key=lambda entry: entry.artifact_ref)),
                                    len(rows) > limit, invalid)

    def public_archive_history_entries(
        self,
        *,
        instrument_revision: str,
        event_types: tuple[str, ...],
        information_cutoff_ns: int,
        limit: int,
        instrument_key_json: str | None = None,
        availability_class: str | None = None,
    ) -> tuple[ArtifactIndexEntryV2, ...]:
        """Select bounded source locators before opening any raw archive file.

        Bar queries include every cutoff-visible revision of the most recent
        ``limit`` source close origins. Receipt queries select the most recent
        exact observations by availability and record identity. A fixed result
        budget fails explicitly rather than truncating revisions silently.
        """
        sha256_ref(instrument_revision, field="instrument_revision")
        cutoff = timestamp(information_cutoff_ns, field="information_cutoff_ns")
        kinds = tuple(sorted(set(event_types)))
        if not kinds or any(not isinstance(kind, str) or not kind.strip() for kind in kinds):
            raise ValueError("archive source lookup requires event types")
        if type(limit) is not int or not 1 <= limit <= 100_000:
            raise ValueError("archive source lookup limit exceeds its bound")
        if instrument_key_json is not None:
            nonblank(instrument_key_json, field="instrument_key_json")
        if availability_class is not None:
            if availability_class not in {"ACTUAL_SYSTEM", "RECONSTRUCTED_MARKET"}:
                raise ValueError("archive source lookup availability view is invalid")
        effective = ("available_at_ns" if availability_class != "RECONSTRUCTED_MARKET"
                     else _archive_json_expression("replay_available_at_ns"))
        if instrument_key_json is None or not all(kind.startswith("BAR_") for kind in kinds):
            if len(kinds) > _RECEIPT_EVENT_TYPE_LIMIT:
                raise ValueError("archive receipt event-type population exceeds its bound (16)")
            classes = ((availability_class,) if availability_class is not None else
                       ("ACTUAL_SYSTEM", "RECONSTRUCTED_MARKET"))
            scope = "key" if instrument_key_json is not None else "revision"
            replay = availability_class == "RECONSTRUCTED_MARKET"
            view = "replay" if replay else "actual"
            # A multi-event IN query can choose the broad revision/receipt
            # index and walk every rejected historical observation. Seek each
            # exact event/class lane before merging its bounded top rows.
            rows_by_ref: dict[str, sqlite3.Row] = {}
            with self._lock:
                for kind in kinds:
                    for source_class in classes:
                        names = ["instrument_revision"]
                        values: list[Any] = [instrument_revision]
                        if instrument_key_json is not None:
                            names.append("instrument_key_json")
                            values.append(_public_metadata_query_key(self._public_metadata_enabled, instrument_key_json))
                        names.extend(("event_type", "availability_class"))
                        values.extend((kind, source_class))
                        lane = " AND ".join(_public_metadata_query_expression(
                            self._public_metadata_enabled, name) + "=?" for name in names)
                        order_time = effective if replay else "available_at_ns"
                        budget = _RECEIPT_REPLAY_CANDIDATE_LIMIT + 1 if replay else limit
                        query = ("SELECT * FROM artifact_index INDEXED BY "
                                 f"public_exact_receipt_{scope}_{view}_lookup"
                                 + ("_v2" if self._public_metadata_enabled else "") + " "
                                 "WHERE artifact_type='PublicObservationIndexV2' AND " + lane
                                 + " AND " + effective + "<=? ORDER BY " + order_time + " DESC,"
                                 + _archive_json_expression("record_id") + " DESC,artifact_ref DESC LIMIT ?")
                        lane_rows = self._connection.execute(query, (*values, cutoff, budget)).fetchall()
                        # Reconstructed replay eligibility and actual receipt
                        # ordering differ. Preserve the existing ordering only
                        # when the entire eligible population fits the explicit
                        # work budget; never silently sample the replay prefix.
                        if replay and len(lane_rows) > _RECEIPT_REPLAY_CANDIDATE_LIMIT:
                            raise ValueError("archive reconstructed receipt population exceeds its bound (100000)")
                        rows_by_ref.update((row["artifact_ref"], row) for row in lane_rows)
                        if replay and len(rows_by_ref) > _RECEIPT_REPLAY_CANDIDATE_LIMIT:
                            raise ValueError("archive reconstructed receipt population exceeds its bound (100000)")
            rows = sorted(rows_by_ref.values(), key=lambda row: (
                int(row["available_at_ns"]),
                str(json.loads(row["metadata_json"]).get("record_id", "")),
                str(row["artifact_ref"])), reverse=True)[:limit]
            return tuple(self.artifact_entry_from_storage_row(row) for row in rows)
        else:
            close = _archive_json_expression("event_at_ns")
            if len(kinds) != 1:
                raise ValueError("bounded causal bar history requires one exact interval")
            # Seek distinct close origins directly. GROUP BY over a materialized
            # retained-history CTE made a small output limit conceal a full scan.
            base = " AND ".join(_public_metadata_query_expression(
                self._public_metadata_enabled, name)+"=?" for name in (
                "instrument_key_json","instrument_revision","event_type","availability_class"))
            exact = (_public_metadata_query_key(self._public_metadata_enabled, instrument_key_json),instrument_revision,
                     kinds[0],availability_class or "ACTUAL_SYSTEM")
            entries: list[ArtifactIndexEntryV2] = []
            cursor_close = cutoff+1
            with self._lock:
                for _ in range(limit):
                    origin = self._connection.execute(
                        "SELECT " + close + " FROM artifact_index INDEXED BY " + _public_metadata_query_index(
                            self._public_metadata_enabled, "public_archive_history_lookup") + " "
                        "WHERE artifact_type='PublicObservationIndexV2' AND " + base + " AND "
                        + close + "<? ORDER BY " + close + " DESC LIMIT 1",
                        (*exact,cursor_close)).fetchone()
                    if origin is None:
                        break
                    cursor_close = int(origin[0])
                    revisions = self._connection.execute(
                        "SELECT * FROM artifact_index INDEXED BY " + _public_metadata_query_index(
                            self._public_metadata_enabled, "public_archive_history_lookup") + " "
                        "WHERE artifact_type='PublicObservationIndexV2' AND " + base + " AND "
                        + close + "=? AND " + effective + "<=? LIMIT 129",
                        (*exact,cursor_close,cutoff)).fetchall()
                    if len(revisions)>128:
                        raise ValueError("archive exact-origin revision work bound exceeded (128)")
                    entries.extend(self.artifact_entry_from_storage_row(row) for row in revisions)
                    if len(entries)>100_000:
                        raise ValueError("archive source lookup exceeded its revision-row bound (100000)")
            return tuple(entries)
    def confirmed_bar_observation_entries(
        self, instrument_key: InstrumentKeyV2, *, event_type: str,
        close_at_ns: int, as_of_ns: int, limit: int = 128,
    ) -> tuple[ArtifactIndexEntryV2, ...]:
        """All bounded exact-close actual source revisions, earliest receipt first.

        Finality is verified from archived typed bar bytes by the reconstructor.
        Overflow fails explicitly; a latest-history window cannot hide a target.
        """
        from ..instruments import InstrumentKeyV2

        if not isinstance(instrument_key, InstrumentKeyV2):
            raise ValueError("exact bar lookup requires full InstrumentKeyV2")
        intervals = {"BAR_1M": 60_000_000_000, "BAR_15M": 900_000_000_000}
        if event_type not in intervals:
            raise ValueError("exact bar lookup only supports M1 and M15 sources")
        close = timestamp(close_at_ns, field="close_at_ns")
        cutoff = timestamp(as_of_ns, field="as_of_ns")
        if close % intervals[event_type]:
            raise ValueError("exact bar close must align to its interval")
        if type(limit) is not int or not 1 <= limit <= 128:
            raise ValueError("exact bar source revision bound must be between 1 and 128")
        query = ("SELECT * FROM artifact_index WHERE artifact_type='PublicObservationIndexV2' "
                 "AND " + _public_metadata_query_expression(
                     self._public_metadata_enabled, "instrument_key_json") + "=? AND "
                 + _archive_json_expression("instrument_revision") + "=? AND "
                 + _archive_json_expression("event_type") + "=? AND "
                 + _archive_json_expression("availability_class") + "='ACTUAL_SYSTEM' AND "
                 + _archive_json_expression("event_at_ns") + "=? AND "
                 + _archive_json_expression("bar_content_hash") + " IS NOT NULL "
                 "AND available_at_ns<=? ORDER BY available_at_ns,"
                 + _archive_json_expression("record_id") + ",artifact_ref LIMIT ?")
        with self._lock:
            rows = self._connection.execute(query, (_public_metadata_query_key(
                self._public_metadata_enabled, instrument_key.to_canonical_json()),
                instrument_key.contract_revision, event_type, close, cutoff, limit + 1)).fetchall()
        if len(rows) > limit:
            raise ValueError("exact bar source revision lookup exceeded its explicit bound")
        return tuple(self.artifact_entry_from_storage_row(row) for row in rows)

    def public_stream_trade_entries(
        self, instrument_key: InstrumentKeyV2, *, source_id: str,
        event_from_ns: int, cutoff_ns: int, limit: int = 512,
    ) -> tuple[ArtifactIndexEntryV2, ...]:
        """Seek exact current-window trade locators, exposing overflow as an extra row.

        Callers must reject ``len(result)>limit``; the returned suffix is not a
        sampled trade population and cannot certify completeness or UTC VWAP.
        Raw archive files are opened only after this bounded source selection.
        """
        from ..instruments import InstrumentKeyV2

        if not isinstance(instrument_key, InstrumentKeyV2):
            raise ValueError("stream trade lookup requires a complete instrument key")
        nonblank(source_id, field="source_id")
        floor = timestamp(event_from_ns, field="event_from_ns")
        cutoff = timestamp(cutoff_ns, field="cutoff_ns")
        if floor > cutoff or type(limit) is not int or not 1 <= limit <= 100_000:
            raise ValueError("stream trade window or population limit is invalid")
        fields = tuple(_archive_json_expression(name) for name in (
            "instrument_key_json", "instrument_revision", "source_id", "event_type"))
        event_time = _archive_json_expression("event_at_ns")
        query = ("SELECT * FROM artifact_index INDEXED BY public_stream_trade_event_window "
            "WHERE artifact_type='PublicStreamTradeObservationIndexV1' AND "
            + " AND ".join(field + "=?" for field in fields)
            + " AND " + event_time + ">=? AND " + event_time + "<=? AND available_at_ns<=?"
            + " ORDER BY " + event_time + " DESC,available_at_ns DESC,artifact_ref DESC LIMIT ?")
        with self._lock:
            rows = self._connection.execute(query, (instrument_key.to_canonical_json(),
                instrument_key.contract_revision, source_id, "TRADE", floor, cutoff, cutoff, limit + 1)).fetchall()
        result = tuple(self.artifact_entry_from_storage_row(row) for row in rows)
        if self._public_locator_enabled:
            from ..data.compact_public_index import feed_ref

            identity = bytes.fromhex(feed_ref(instrument_key.to_canonical_json(), source_id))
            with self._lock:
                compact = self._connection.execute(
                    "SELECT * FROM public_stream_archive_locator_v1 INDEXED BY public_stream_archive_trade_window_v1 "
                    "WHERE kind=2 AND feed_ref=? AND event_at_ns>=? AND event_at_ns<=? AND available_at_ns<=? "
                    "ORDER BY event_at_ns DESC,available_at_ns DESC,artifact_ref DESC LIMIT ?",
                    (identity, floor, cutoff, cutoff, limit + 1)).fetchall()
            result = (*result, *(self._decode_public_locator(row) for row in compact))
        return tuple(sorted(result, key=lambda entry: (entry.metadata.get("event_at_ns", 0),
            entry.available_at_ns, entry.artifact_ref), reverse=True)[:limit + 1])

    def generic_public_stream_observation_exists(
        self, instrument_key: InstrumentKeyV2, *, source_id: str, cutoff_ns: int,
    ) -> bool:
        """Detect wrong generic indexing for this WS source/revision in one seek."""
        from ..instruments import InstrumentKeyV2

        if not isinstance(instrument_key, InstrumentKeyV2):
            raise ValueError("stream generic-index check requires a complete instrument key")
        nonblank(source_id, field="source_id")
        cutoff = timestamp(cutoff_ns, field="cutoff_ns")
        with self._lock:
            row = self._connection.execute(
                "SELECT 1 FROM artifact_index INDEXED BY public_stream_generic_source_lookup "
                "WHERE artifact_type='PublicObservationIndexV2' AND "
                + _archive_json_expression("instrument_revision") + "=? AND "
                + _archive_json_expression("source_id") + "=? AND available_at_ns<=? "
                "ORDER BY available_at_ns DESC,artifact_ref DESC LIMIT 1",
                (instrument_key.contract_revision, source_id, cutoff)).fetchone()
        return row is not None

    def public_stream_continuity_invalidations(
        self, instrument_key: InstrumentKeyV2, *, source_id: str, channel: str,
        after_ns: int, through_ns: int,
    ) -> tuple[ArtifactIndexEntryV2, ...]:
        """Read at most one exact source gap/recovery in the consumer's interval."""
        from ..instruments import InstrumentKeyV2

        if not isinstance(instrument_key, InstrumentKeyV2):
            raise ValueError("stream invalidation lookup requires a complete instrument key")
        nonblank(source_id, field="source_id")
        nonblank(channel, field="channel")
        after = timestamp(after_ns, field="after_ns")
        through = timestamp(through_ns, field="through_ns")
        if after > through:
            raise ValueError("stream invalidation interval is reversed")
        fields = tuple(_archive_json_expression(f"observation.{name}") for name in (
            "instrument", "source_id", "channel"))
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM artifact_index INDEXED BY public_stream_invalidation_window "
                "WHERE artifact_type='PublicStreamContinuityEventV1' AND "
                + " AND ".join(field + "=?" for field in fields)
                + " AND available_at_ns>? AND available_at_ns<=? ORDER BY available_at_ns,artifact_ref LIMIT 1",
                (instrument_key.to_canonical_json(), source_id, channel, after, through)).fetchall()
        return tuple(self.artifact_entry_from_storage_row(row) for row in rows)

    def latest_stream_continuity_entries(self, *, as_of_ns: int) -> tuple[ArtifactIndexEntryV2, ...]:
        """Seek at most eight public continuity heads without decoding their history.

        The installed public composition owns exactly the BTC/ETH Bybit book
        and trade channels. Keep one compact checkpoint and one legacy state
        per channel so callers can validate the exact bodies and migrate a
        previously recorded run without loading thousands of full caches.
        A malformed selected body remains an error; this lookup does not turn
        an invalid latest checkpoint into permission to reuse older evidence.
        """
        cutoff = timestamp(as_of_ns, field="as_of_ns")
        rows: list[sqlite3.Row] = []
        with self._lock:
            for version, kind, path in (
                ("v1", "PublicStreamContinuityStateV1", "state"),
                ("v2", "PublicStreamContinuityCheckpointV2", "checkpoint.state"),
            ):
                fields = tuple(_archive_json_expression(f"{path}.{name}") for name in (
                    "instrument.native_symbol", "channel", "source_id"))
                progress = tuple(f"COALESCE({_archive_json_expression(f'{path}.{name}')},0) DESC"
                    for name in ("last_available_at_ns", "recovery_epoch", "observed_trade_count",
                                 "last_transport_receipt_at_ns", "gap_count"))
                query = (
                    f"SELECT * FROM artifact_index INDEXED BY public_stream_continuity_head_{version}_progress "
                    f"WHERE artifact_type='{kind}' AND "
                    + " AND ".join(field + "=?" for field in fields)
                    + " AND available_at_ns<=? ORDER BY available_at_ns DESC,"
                    + ",".join(progress) + ",artifact_ref DESC LIMIT 1"
                )
                for symbol in ("BTCUSDT", "ETHUSDT"):
                    for prefix in ("orderbook.50.", "publicTrade."):
                        row = self._connection.execute(query, (
                            symbol, prefix + symbol, "BYBIT_PUBLIC_WS", cutoff,
                        )).fetchone()
                        if row is not None:
                            rows.append(row)
        return tuple(self.artifact_entry_from_storage_row(row) for row in rows)

    def l2_archive_restart_checkpoints(self, *, limit: int = 1_024) -> tuple[ArtifactIndexEntryV2, ...]:
        """Return one latest checkpoint per stream without loading frame history."""
        if type(limit) is not int or not 1 <= limit <= 10_000:
            raise ValueError("L2 restart stream limit exceeds its bound")
        fields = tuple(_archive_json_expression(name) for name in ("instrument_hash", "source_id", "channel"))
        # Seek the next distinct stream, then its latest receipt. A ROW_NUMBER
        # partition used to visit every retained frame before applying LIMIT.
        rows: list[sqlite3.Row] = []
        previous = ("", "", "")
        with self._lock:
            for position in range(limit + 1):
                identity = None
                # SQLite expression indexes do not turn a tuple comparison into
                # a complete range seek. Seek each hierarchy level explicitly
                # so finding the end of one stream never walks its frame history.
                levels = (-1,) if position == 0 else (2, 1, 0)
                for level in levels:
                    conditions = [] if level < 0 else [field + "=?" for field in fields[:level]]
                    if level >= 0:
                        conditions.append(fields[level] + ">?")
                    successor = "" if not conditions else " AND " + " AND ".join(conditions)
                    identity = self._connection.execute(
                        "SELECT " + ",".join(fields) + " FROM artifact_index INDEXED BY l2_archive_restart_lookup "
                        "WHERE artifact_type='L2FrameArchiveCheckpointV2'" + successor
                        + " ORDER BY " + ",".join(fields[max(level, 0):]) + " LIMIT 1",
                        () if level < 0 else previous[:level + 1]).fetchone()
                    if identity is not None:
                        break
                if identity is None:
                    break
                previous = tuple(identity)
                if any(not isinstance(value, str) or not value.strip() for value in previous):
                    raise ValueError("L2 restart stream identity is invalid")
                row = self._connection.execute(
                    "SELECT * FROM artifact_index INDEXED BY l2_archive_restart_lookup "
                    "WHERE artifact_type='L2FrameArchiveCheckpointV2' AND "
                    + " AND ".join(field + "=?" for field in fields)
                    + " ORDER BY available_at_ns DESC,artifact_ref DESC LIMIT 1", previous).fetchone()
                if row is not None:
                    rows.append(row)
        if len(rows) > limit:
            raise ValueError("L2 restart exceeded its distinct-stream bound")
        rows.sort(key=lambda row: row["artifact_ref"])
        return tuple(self.artifact_entry_from_storage_row(row) for row in rows)

    def _typed_creation_rows(self, types: tuple[str, ...], *, cutoff: int | None,
            after: tuple[int, str] | None, limit: int) -> list[sqlite3.Row]:
        """Bound raw candidates before availability filtering and global merge."""
        if len(types) > 128:
            raise ValueError("typed artifact namespace population exceeds its bound (128)")
        maximum = 16_384
        collected: list[sqlite3.Row] = []
        with self._lock:
            for kind in types:
                lane_cursor = after
                examined = 0
                visible: list[sqlite3.Row] = []
                while len(visible) < limit:
                    cursor = " AND (created_at_ns,artifact_ref)<(?,?)" if lane_cursor is not None else ""
                    batch = min(limit + 1, maximum - examined + 1)
                    raw = self._connection.execute(
                        "SELECT * FROM artifact_index INDEXED BY artifact_type_created_order "
                        "WHERE artifact_type=?" + cursor + " ORDER BY created_at_ns DESC,artifact_ref DESC LIMIT ?",
                        (kind, *((lane_cursor[0], lane_cursor[1]) if lane_cursor is not None else ()), batch)).fetchall()
                    for row in raw:
                        examined += 1
                        if examined > maximum:
                            raise ValueError("TYPED_ARTIFACT_CAUSAL_WINDOW_OVERFLOW")
                        if cutoff is None or row["available_at_ns"] <= cutoff:
                            visible.append(row)
                            if len(visible) == limit:
                                break
                    if len(raw) < batch or len(visible) >= limit:
                        break
                    lane_cursor = (raw[-1]["created_at_ns"], raw[-1]["artifact_ref"])
                collected.extend(visible[:limit])
        collected.sort(key=lambda row: (row["created_at_ns"], row["artifact_ref"]), reverse=True)
        return collected[:limit]

    def artifact_entries_by_types(
        self,
        artifact_types: tuple[str, ...],
        *,
        limit: int = 2_000,
        available_before_ns: int | None = None,
    ) -> tuple[ArtifactIndexEntryV2, ...]:
        """Read bounded typed artifact namespaces in stable index order."""
        types = tuple(sorted(set(artifact_types)))
        if not types or any(not isinstance(item, str) or not item.strip() for item in types):
            raise ValueError("at least one non-empty artifact type is required")
        if type(limit) is not int or not 1 <= limit <= 10_000:
            raise ValueError("artifact read limit must be between 1 and 10000")
        cutoff = (
            timestamp(available_before_ns, field="available_before_ns") if available_before_ns is not None else None
        )
        rows = self._typed_creation_rows(types, cutoff=cutoff, after=None, limit=limit)
        rows.reverse()
        return self._artifact_entries_from_storage_rows(rows)

    def artifact_entries_by_types_page(
        self,
        artifact_types: Sequence[str],
        *,
        as_of_ns: int,
        after: tuple[int, str] | None = None,
        limit: int = 500,
    ) -> ArtifactIndexPageV2:
        """Read one deterministic page, filtering availability before paging.

        Pages are ordered descending by ``(created_at_ns, artifact_ref)``.
        The reference makes equal timestamps unambiguous. ``after`` is the
        final raw row key from the preceding page, including when that row is
        malformed, so callers can account for invalid rows without looping.
        Use inside :meth:`read_snapshot` when multiple pages must describe one
        consistent inventory.
        """
        types = tuple(sorted(set(artifact_types)))
        if not types or any(not isinstance(item, str) or not item.strip() for item in types):
            raise ValueError("at least one non-empty artifact type is required")
        cutoff = timestamp(as_of_ns, field="as_of_ns")
        if type(limit) is not int or not 1 <= limit <= 2_000:
            raise ValueError("artifact page size must be between 1 and 2000")
        if after is not None:
            if (not isinstance(after, tuple) or len(after) != 2 or type(after[0]) is not int
                    or not isinstance(after[1], str)):
                raise ValueError("artifact cursor must be a (created_at_ns, artifact_ref) pair")
            timestamp(after[0], field="cursor.created_at_ns")
            # Raw-key pagination must also cross malformed blank refs exactly.
        rows = self._typed_creation_rows(types, cutoff=cutoff, after=after, limit=limit)
        entries: list[ArtifactIndexEntryV2] = []
        invalid = 0
        row_order = {row["artifact_ref"]: index for index, row in enumerate(rows)}
        for row in self._storage_rows_in_decode_order(rows):
            try:
                entries.append(self.artifact_entry_from_storage_row(row))
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                invalid += 1
        entries.sort(key=lambda entry: row_order[entry.artifact_ref])
        next_cursor = (int(rows[-1]["created_at_ns"]), str(rows[-1]["artifact_ref"])) if rows else None
        raw_keys = tuple((int(row["created_at_ns"]), str(row["artifact_ref"])) for row in rows)
        return ArtifactIndexPageV2(tuple(entries), next_cursor, invalid, raw_keys)

    def native_m1_origin_observation_page(
        self, instrument_key: InstrumentKeyV2, *, available_from_ns: int,
        available_through_ns: int, after_close_at_ns: int | None = None, limit: int = 4,
    ) -> NativeM1OriginObservationPageV1:
        """Compatibility port for exact native M1 source origins."""
        return self.native_bar_origin_observation_page(
            instrument_key, event_type="BAR_1M", available_from_ns=available_from_ns,
            available_through_ns=available_through_ns, after_close_at_ns=after_close_at_ns, limit=limit,
        )

    def m15_origin_observation_page(
        self, instrument_key: InstrumentKeyV2, *, available_from_ns: int,
        available_through_ns: int, after_close_at_ns: int | None = None, limit: int = 4,
        min_close_at_ns: int = 0,
    ) -> NativeM1OriginObservationPageV1:
        """Exact native M15 origin page; late older bars remain discoverable."""
        return self.native_bar_origin_observation_page(
            instrument_key, event_type="BAR_15M", available_from_ns=available_from_ns,
            available_through_ns=available_through_ns, after_close_at_ns=after_close_at_ns, limit=limit,
            min_close_at_ns=min_close_at_ns,
        )

    def native_bar_origin_observation_page(
        self,
        instrument_key: InstrumentKeyV2,
        *,
        event_type: str,
        available_from_ns: int,
        available_through_ns: int,
        after_close_at_ns: int | None = None,
        limit: int = 4,
        min_close_at_ns: int = 0,
    ) -> NativeM1OriginObservationPageV1:
        """Read a bounded oldest-close-first page of exact native M1 or M15 bars.

        The inclusive availability bounds define one frozen source discovery
        window. A one-timestamp overlap makes same-clock source-index writes
        restart safe; durable event/gate state makes the overlap idempotent.
        The caller persists that window
        in its revision-bound checkpoint and only moves the close cursor after
        the corresponding event/gate records are durable. A later source
        window starts with no close cursor, so delayed older bars are still
        discovered without rescanning already completed source history.

        Only controller-indexed public observations with a non-null bar ref,
        full exact instrument identity, the declared bar interval, and
        ACTUAL_SYSTEM availability are returned. `event_at_ns` is the canonical
        source close. Duplicate revisions at one close
        collapse to the earliest available indexed observation.
        """
        # Local import avoids making the memory package depend on runtime code.
        from ..data.raw import AvailabilityClassV2
        from ..instruments import InstrumentKeyV2

        if not isinstance(instrument_key, InstrumentKeyV2):
            raise ValueError("native bar source paging requires a full InstrumentKeyV2")
        available_from = timestamp(available_from_ns, field="available_from_ns")
        available_through = timestamp(available_through_ns, field="available_through_ns")
        min_close = timestamp(min_close_at_ns, field="min_close_at_ns")
        if available_from > available_through:
            raise ValueError("native bar source availability window is reversed")
        if type(limit) is not int or not 1 <= limit <= 2_000:
            raise ValueError("native bar source page limit must be between 1 and 2000")
        if after_close_at_ns is not None:
            timestamp(after_close_at_ns, field="after_close_at_ns")

        intervals = {"BAR_1M": 60_000_000_000, "BAR_15M": 900_000_000_000}
        if event_type not in intervals:
            raise ValueError("native origin paging only supports M1 and M15 bars")
        duration_ns = intervals[event_type]
        if after_close_at_ns is not None and after_close_at_ns % duration_ns:
            raise ValueError("native close cursor must align to its source interval")
        key_json = instrument_key.to_canonical_json()
        key_expr = _public_metadata_query_expression(self._public_metadata_enabled, "instrument_key_json")
        event_expr = "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.event_type') END"
        availability_expr = (
            "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.availability_class') END"
        )
        window_ref = sha256_json({"version": "NativeOriginWindowProjectionV1", "key": key_json,
            "event_type": event_type, "from_ns": available_from, "through_ns": available_through})
        with self._transaction() as connection:
            head = connection.execute("SELECT * FROM native_origin_window_head "
                "WHERE instrument_key_json=? AND event_type=?", (key_json,event_type)).fetchone()
            if head is None or head["window_ref"] != window_ref:
                connection.execute("INSERT INTO native_origin_window_head VALUES(?,?,?,?,?,?,?,0) "
                    "ON CONFLICT(instrument_key_json,event_type) DO UPDATE SET window_ref=excluded.window_ref,"
                    "available_from_ns=excluded.available_from_ns,available_through_ns=excluded.available_through_ns,"
                    "cursor_available_ns=excluded.cursor_available_ns,cursor_ref='',complete=0",
                    (key_json,event_type,window_ref,available_from,available_through,available_from-1,""))
                cursor_available, cursor_ref, complete = available_from-1, "", False
            else:
                cursor_available, cursor_ref, complete = head["cursor_available_ns"],head["cursor_ref"],bool(head["complete"])
            if not complete:
                source_rows = connection.execute(
                    "SELECT * FROM artifact_index INDEXED BY " + _public_metadata_query_index(
                        self._public_metadata_enabled, "public_origin_discovery_lookup") + " "
                    "WHERE artifact_type='PublicObservationIndexV2' AND " + key_expr + "=? AND "
                    + event_expr + "=? AND " + availability_expr + "=? "
                    "AND available_at_ns>=? AND available_at_ns<=? "
                    "AND (available_at_ns,artifact_ref)>(?,?) "
                    "ORDER BY available_at_ns,artifact_ref LIMIT 129",
                    (_public_metadata_query_key(self._public_metadata_enabled, key_json),event_type,
                     AvailabilityClassV2.ACTUAL_SYSTEM.value,
                     available_from,available_through,cursor_available,cursor_ref)).fetchall()
                for row in source_rows[:128]:
                    body = json.loads(row["metadata_json"])
                    close_value = body.get("event_at_ns")
                    if (type(close_value) is int and close_value>=0 and close_value % duration_ns==0
                            and body.get("bar_content_hash") is not None):
                        connection.execute("INSERT INTO native_origin_window VALUES(?,?,?,?) "
                            "ON CONFLICT(window_ref,close_at_ns) DO UPDATE SET "
                            "available_at_ns=excluded.available_at_ns,artifact_ref=excluded.artifact_ref "
                            "WHERE (excluded.available_at_ns,excluded.artifact_ref)<"
                            "(native_origin_window.available_at_ns,native_origin_window.artifact_ref)",
                            (window_ref,close_value,row["available_at_ns"],row["artifact_ref"]))
                if source_rows:
                    last = source_rows[min(len(source_rows),128)-1]
                    cursor_available,cursor_ref = last["available_at_ns"],last["artifact_ref"]
                complete = len(source_rows)<=128
                connection.execute("UPDATE native_origin_window_head SET cursor_available_ns=?,"
                    "cursor_ref=?,complete=? WHERE instrument_key_json=? AND event_type=?",
                    (cursor_available,cursor_ref,int(complete),key_json,event_type))
            if not complete:
                pressure = {"version": "OpsActiveWorkPressureV1", "lane": "NATIVE_ORIGIN_DISCOVERY",
                    "instrument_key_json": key_json, "event_type": event_type,
                    "window_ref": window_ref, "cursor_available_at_ns": cursor_available,
                    "cursor_ref": cursor_ref, "max_rows_per_cycle": 128, "backlog": True, "authority": "ZERO"}
                ref = sha256_json(pressure)
                published = max(time.time_ns(),available_through)
                self.register_artifact(ArtifactIndexEntryV2(ref,"OpsActiveWorkPressureV1",ref,
                    published,published,{"pressure":pressure}))
                return NativeM1OriginObservationPageV1((),True,after_close_at_ns or 0)
            rows = connection.execute(
                "SELECT a.* FROM native_origin_window w CROSS JOIN artifact_index a ON a.artifact_ref=w.artifact_ref "
                "WHERE w.window_ref=? AND w.close_at_ns>=? "
                "ORDER BY w.close_at_ns LIMIT ?",
                (window_ref,max(min_close,after_close_at_ns+1 if after_close_at_ns is not None else 0),limit+1)).fetchall()

        has_more = len(rows) > limit
        selected_rows = rows[:limit]
        entries: list[ArtifactIndexEntryV2] = []
        for row in selected_rows:
            entry = self.artifact_entry_from_storage_row(row)
            metadata = entry.metadata
            close_at_ns = metadata.get("event_at_ns")
            if (entry.artifact_type != "PublicObservationIndexV2"
                    or metadata.get("instrument_key_json") != key_json
                    or metadata.get("event_type") != event_type
                    or metadata.get("availability_class") != AvailabilityClassV2.ACTUAL_SYSTEM.value
                    or type(close_at_ns) is not int or close_at_ns % duration_ns
                    or metadata.get("bar_content_hash") is None
                    or not available_from <= entry.available_at_ns <= available_through):
                raise ValueError("native bar source page contains conflicting indexed metadata")
            sha256_ref(str(metadata["bar_content_hash"]), field="bar_content_hash")
            entries.append(entry)
        close_values = [int(entry.metadata["event_at_ns"]) for entry in entries]
        if close_values != sorted(set(close_values)):
            raise ValueError("native bar source page is not unique and oldest-close-first")
        return NativeM1OriginObservationPageV1(
            tuple(entries), has_more, close_values[-1] if close_values else None,
        )

    def public_observation_source_ids(self, *, limit: int = 128) -> tuple[str, ...]:
        """Seek distinct source successors using the exact source-ID index."""
        if type(limit) is not int or not 1 <= limit <= 2_000:
            raise ValueError("public observation source-ID bound must be between 1 and 2000")
        expression = _archive_json_expression("source_id")
        values: list[str] = []
        previous = ""
        with self._lock:
            for index in range(limit + 1):
                comparison = ">=" if index == 0 else ">"
                row = self._connection.execute(
                    "SELECT " + expression + " AS source_id,json_type(metadata_json,'$.source_id') AS source_type "
                    "FROM artifact_index INDEXED BY public_observation_source_id_lookup "
                    "WHERE artifact_type='PublicObservationIndexV2' AND "
                    + expression + comparison + "? ORDER BY " + expression + " LIMIT 1", (previous,),
                ).fetchone()
                if row is None:
                    break
                source_id = row["source_id"]
                if row["source_type"] != "text" or not isinstance(source_id, str) or not source_id.strip():
                    raise ValueError("public observation source-ID inventory is invalid")
                values.append(source_id)
                previous = source_id
        if len(values) > limit:
            raise ValueError("public observation source-ID set exceeded its deterministic bound")
        return tuple(values)

    def public_reconciliation_source_ids(self, *, limit: int = 128) -> tuple[str, ...]:
        """Seek distinct reconciliation successors through its source-ID index."""
        if type(limit) is not int or not 1 <= limit <= 2_000:
            raise ValueError("reconciliation source-ID bound must be between 1 and 2000")
        expression = _archive_json_expression("reconciliation.source_id")
        values: list[str] = []
        previous = ""
        with self._lock:
            for index in range(limit + 1):
                comparison = ">=" if index == 0 else ">"
                row = self._connection.execute(
                    "SELECT " + expression + " AS source_id,"
                    "json_type(metadata_json,'$.reconciliation.source_id') AS source_type FROM artifact_index "
                    "INDEXED BY public_reconciliation_source_id_lookup "
                    "WHERE artifact_type='OpsPublicSourceReconciliationV1' AND "
                    + expression + comparison + "? ORDER BY " + expression + " LIMIT 1", (previous,),
                ).fetchone()
                if row is None:
                    break
                source_id = row["source_id"]
                if row["source_type"] != "text" or not isinstance(source_id, str) or not source_id.strip():
                    raise ValueError("reconciliation source-ID inventory is invalid")
                values.append(source_id)
                previous = source_id
        if len(values) > limit:
            raise ValueError("reconciliation source-ID set exceeded its deterministic bound")
        return tuple(values)

    def pending_decision_event_page(
        self,
        *,
        as_of_ns: int,
        limit: int = 1_024,
    ) -> PendingDecisionEventPageV1:
        """Read bounded unreceipted event handoffs in oldest-availability order."""
        cutoff = timestamp(as_of_ns, field="as_of_ns")
        if type(limit) is not int or not 1 <= limit <= 2_000:
            raise ValueError("pending event page size must be between 1 and 2000")
        if not self.read_only:
            self.discover_due_work(limit=64)
        query = """SELECT e.* FROM due_work AS d
                   JOIN artifact_index AS e ON e.artifact_ref=d.source_ref
                   WHERE d.lane='OPS_EVENT' AND d.state='PENDING' AND d.due_at_ns<=?
                   ORDER BY d.due_at_ns,d.work_id LIMIT ?"""
        with self._lock:
            rows = self._connection.execute(query, (cutoff, limit + 1)).fetchall()
        has_more = len(rows) > limit
        selected = rows[:limit]
        entries: list[ArtifactIndexEntryV2] = []
        invalid = 0
        for row in selected:
            try:
                entries.append(self.artifact_entry_from_storage_row(row))
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                invalid += 1
        return PendingDecisionEventPageV1(tuple(entries), has_more, invalid)

    def latest_native_m1_origin_accounting_checkpoint(
        self,
        instrument_key: InstrumentKeyV2,
    ) -> ArtifactIndexEntryV2 | None:
        """Read the latest exact-key checkpoint in constant bounded work."""
        from ..instruments import InstrumentKeyV2

        if not isinstance(instrument_key, InstrumentKeyV2):
            raise ValueError("native M1 checkpoint lookup requires full InstrumentKeyV2")
        with self._lock:
            rows = self._connection.execute(
                """SELECT * FROM artifact_index INDEXED BY native_m1_checkpoint_exact_generation
                   WHERE artifact_type='S3NativeM1OriginAccountingCheckpointV1'
                     AND CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.instrument_key_json') END=?
                   ORDER BY CAST(CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.checkpoint.generation') END AS INTEGER) DESC,
                            artifact_ref DESC
                   LIMIT 2""",
                (instrument_key.to_canonical_json(),),
            ).fetchall()
        if not rows:
            return None
        entries = tuple(self.artifact_entry_from_storage_row(row) for row in rows)
        if len(entries) > 1:
            top = entries[0].metadata.get("checkpoint")
            next_body = entries[1].metadata.get("checkpoint")
            if (not isinstance(top, Mapping) or not isinstance(next_body, Mapping)
                    or top.get("generation") == next_body.get("generation")):
                raise ValueError("native M1 checkpoint generation has conflicting durable rows")
        return entries[0]

    def latest_m15_origin_accounting_checkpoint(
        self, instrument_key: InstrumentKeyV2,
    ) -> ArtifactIndexEntryV2 | None:
        """Read the newest exact-revision M15 checkpoint with a bounded indexed query."""
        from ..instruments import InstrumentKeyV2
        from ..science.m15_origin_accounting import M15OriginAccountingCheckpointV1

        if not isinstance(instrument_key, InstrumentKeyV2):
            raise ValueError("M15 checkpoint lookup requires full InstrumentKeyV2")
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM artifact_index INDEXED BY m15_checkpoint_exact_generation WHERE artifact_type='M15OriginAccountingCheckpointV1' "
                "AND CASE WHEN json_valid(metadata_json) THEN "
                "json_extract(metadata_json, '$.instrument_key_json') END=? "
                "ORDER BY CAST(CASE WHEN json_valid(metadata_json) THEN "
                "json_extract(metadata_json, '$.checkpoint.generation') END AS INTEGER) DESC,"
                "artifact_ref DESC LIMIT 2",
                (instrument_key.to_canonical_json(),),
            ).fetchall()
        if not rows:
            return None
        entries = tuple(self.artifact_entry_from_storage_row(row) for row in rows)
        records: list[M15OriginAccountingCheckpointV1] = []
        for entry in entries:
            body = entry.metadata.get("checkpoint")
            if not isinstance(body, Mapping):
                raise ValueError("M15 checkpoint requires a typed durable payload")
            record = M15OriginAccountingCheckpointV1.from_dict(body)
            if (record.instrument_key != instrument_key or entry.artifact_ref != record.content_hash
                    or entry.content_hash != record.content_hash
                    or entry.available_at_ns != record.observed_at_ns):
                raise ValueError("M15 checkpoint identity conflicts with its durable index")
            records.append(record)
        if len(records) > 1 and records[0].generation == records[1].generation:
            raise ValueError("M15 checkpoint generation has conflicting durable rows")
        return entries[0]

    def artifact_entries_by_metadata_identity(
        self,
        artifact_type: str,
        metadata_path: Sequence[str],
        identity_value: str,
        *,
        as_of_ns: int,
        after: tuple[int, str] | None = None,
        limit: int = 32,
    ) -> ArtifactMetadataIdentityPageV1:
        """Return a bounded exact metadata identity match set, causally available by ``as_of_ns``.

        JSON path components are restricted to ASCII identifiers so the generated
        SQLite JSON path cannot change query structure. ``has_more`` exposes
        overflow; callers resolving immutable evidence should fail closed rather
        than choose among a truncated set. Creation-order paging examines at
        most 1024 exact-identity rows; an excessive unavailable prefix refuses
        the query rather than hiding a retained-history scan behind LIMIT.
        """
        nonblank(artifact_type, field="artifact_type")
        path = tuple(metadata_path)
        if not path or any(
            not isinstance(item, str)
            or not item
            or not item.isascii()
            or not item.replace("_", "a").isalnum()
            for item in path
        ):
            raise ValueError("metadata path must contain ASCII identifier components")
        nonblank(identity_value, field="identity_value")
        cutoff = timestamp(as_of_ns, field="as_of_ns")
        if type(limit) is not int or not 1 <= limit <= 512:
            raise ValueError("metadata identity page size must be between 1 and 512")
        if after is not None:
            if (not isinstance(after, tuple) or len(after) != 2 or type(after[0]) is not int
                    or not isinstance(after[1], str)):
                raise ValueError("metadata identity cursor must be a (created_at_ns, artifact_ref) pair")
            timestamp(after[0], field="cursor.created_at_ns")
            sha256_ref(after[1], field="cursor.artifact_ref")
        identity_expression = _ARTIFACT_METADATA_IDENTITY_EXPRESSIONS.get((artifact_type, path))
        if identity_expression is None:
            raise ValueError("metadata identity path has no bounded artifact index")
        cursor_clause = " AND (created_at_ns,artifact_ref)<(?,?)" if after is not None else ""
        params: tuple[Any, ...] = (
            identity_value,
            cutoff,
            *((after[0], after[1]) if after is not None else ()),
            _METADATA_IDENTITY_RAW_LIMIT + 1,
        )
        index = "created_metadata_" + sha256_json([artifact_type, list(path)])[:16]
        with self._lock:
            rows = self._connection.execute(
                f"SELECT * FROM artifact_index INDEXED BY {index} WHERE "
                f"artifact_type='{artifact_type}' AND {identity_expression}=? AND created_at_ns<=?"
                f"{cursor_clause} ORDER BY created_at_ns DESC,artifact_ref DESC LIMIT ?",
                params,
            ).fetchall()
        visible_rows = [row for row in rows[:_METADATA_IDENTITY_RAW_LIMIT] if row["available_at_ns"] <= cutoff]
        if len(rows) > _METADATA_IDENTITY_RAW_LIMIT and len(visible_rows) <= limit:
            raise ValueError("METADATA_IDENTITY_CAUSAL_WINDOW_OVERFLOW")
        has_more = len(visible_rows) > limit
        selected_rows = visible_rows[:limit]
        entries: list[ArtifactIndexEntryV2] = []
        invalid = 0
        for row in selected_rows:
            try:
                entries.append(self.artifact_entry_from_storage_row(row))
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                invalid += 1
        next_cursor = (
            (int(selected_rows[-1]["created_at_ns"]), str(selected_rows[-1]["artifact_ref"]))
            if selected_rows else None
        )
        return ArtifactMetadataIdentityPageV1(tuple(entries), next_cursor, has_more, invalid)

    def recover_active_watches(
        self,
        *,
        now_ns: int,
        expiry_ids: Mapping[str, tuple[str, str]] | None = None,
    ) -> RestartSnapshotV2:
        """Expire due watches atomically, then expose active subscriptions in stable order.

        ``expiry_ids`` maps each due watch to caller-supplied ``(event_id,
        outbox_id)``. Supplying stable IDs makes retry after restart idempotent.
        """
        timestamp(now_ns, field="now_ns")
        ids = dict(expiry_ids or {})
        due = tuple(watch for watch in self.list_active_watches() if now_ns >= watch.expires_at_ns)
        for watch in due:
            if watch.watch_id not in ids:
                raise ValueError(f"caller must supply deterministic expiry IDs for {watch.watch_id}")
        for watch in due:
            event_id, outbox_id = ids[watch.watch_id]
            self.transition_watch(
                watch.watch_id,
                expected_state_version=watch.state_version,
                event_id=event_id,
                event_at_ns=now_ns,
                transition_at_ns=now_ns,
                target_state=WatchStateV2.EXPIRED,
                outbox_id=outbox_id,
            )
        active = self.list_active_watches()
        required = tuple((watch.watch_id, watch.required_next_event) for watch in active)
        return RestartSnapshotV2(active, required)
