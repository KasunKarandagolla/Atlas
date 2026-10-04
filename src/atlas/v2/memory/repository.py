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
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from .._serialization import FrozenMap, canonical_json, nonblank, sha256_json, sha256_ref, timestamp
from ..contracts import OpportunityWatchV2, WatchStateV2
from ..models.protocol import ModelManifestV2
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
    def _from_storage_row(cls, row: sqlite3.Row) -> ArtifactIndexEntryV2:
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
        metadata = json.loads(row["metadata_json"])
        if not isinstance(metadata, Mapping):
            raise ValueError("persisted artifact metadata must be a JSON object")
        entry = object.__new__(cls)
        object.__setattr__(entry, "artifact_ref", artifact_ref)
        object.__setattr__(entry, "artifact_type", artifact_type)
        object.__setattr__(entry, "content_hash", content_hash)
        object.__setattr__(entry, "created_at_ns", created_at_ns)
        object.__setattr__(entry, "available_at_ns", available_at_ns)
        object.__setattr__(entry, "metadata", FrozenMap(metadata))
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


_RECEIPT_REPLAY_CANDIDATE_LIMIT = 100_000
_RECEIPT_EVENT_TYPE_LIMIT = 16
_RECEIPT_QUERY_INDEXES = tuple(
    f"CREATE INDEX IF NOT EXISTS public_exact_receipt_{scope}_{view}_lookup ON artifact_index ("
    + ",".join(_archive_json_expression(name) for name in (
        ("instrument_revision", "instrument_key_json", "event_type", "availability_class")
        if scope == "key" else ("instrument_revision", "event_type", "availability_class")))
    + "," + ("available_at_ns" if view == "actual" else _archive_json_expression("replay_available_at_ns"))
    + " DESC," + _archive_json_expression("record_id") + " DESC,artifact_ref DESC) "
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
    "CREATE INDEX IF NOT EXISTS public_origin_discovery_lookup ON artifact_index ("
    + ",".join(_archive_json_expression(name) for name in (
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
    "CREATE INDEX IF NOT EXISTS public_origin_window_lookup ON artifact_index ("
    + ",".join(_archive_json_expression(name) for name in (
        "instrument_key_json", "event_type", "availability_class"))
    + ",available_at_ns," + _archive_json_expression("event_at_ns")
    + ",artifact_ref) WHERE artifact_type='PublicObservationIndexV2'",
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
    "CREATE INDEX IF NOT EXISTS public_archive_history_lookup ON artifact_index ("
    + ",".join(_archive_json_expression(name) for name in (
        "instrument_key_json", "instrument_revision", "event_type", "availability_class", "event_at_ns"))
    + ",available_at_ns,artifact_ref) WHERE artifact_type='PublicObservationIndexV2'",
    "CREATE INDEX IF NOT EXISTS public_archive_receipt_lookup ON artifact_index ("
    + _archive_json_expression("instrument_revision")
    + ",available_at_ns DESC,artifact_ref DESC) WHERE artifact_type='PublicObservationIndexV2'",
    "CREATE INDEX IF NOT EXISTS l2_archive_restart_lookup ON artifact_index ("
    + ",".join(_archive_json_expression(name) for name in ("instrument_hash", "source_id", "channel"))
    + ",available_at_ns DESC,artifact_ref DESC) WHERE artifact_type='L2FrameArchiveCheckpointV2'",
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
            if not read_only:
                # These are rebuildable access indexes over accepted artifact
                # rows, not new evidence tables or a new schema authority.
                for statement in (*_ARCHIVE_QUERY_INDEXES, *_LATEST_METADATA_INDEXES, *_CREATED_METADATA_INDEXES):
                    self._connection.execute(statement)
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
            row = self._connection.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
            if row is None or len(row) != 3:
                raise RuntimeError("SQLite returned an invalid WAL checkpoint result")
            return int(row[0]), int(row[1]), int(row[2])

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

            def __enter__(self) -> sqlite3.Connection:
                self.repository._lock.acquire()
                try:
                    if self.repository._connection.in_transaction:
                        self.repository._savepoint_counter += 1
                        self.savepoint = f"atlas_composition_{self.repository._savepoint_counter}"
                        self.repository._connection.execute(f"SAVEPOINT {self.savepoint}")
                    else:
                        self.repository._connection.execute("BEGIN IMMEDIATE")
                except BaseException:
                    self.repository._lock.release()
                    raise
                return self.repository._connection

            def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
                try:
                    if self.savepoint is not None:
                        if exc_type:
                            self.repository._connection.execute(f"ROLLBACK TO SAVEPOINT {self.savepoint}")
                        self.repository._connection.execute(f"RELEASE SAVEPOINT {self.savepoint}")
                    elif exc_type:
                        self.repository._connection.rollback()
                    else:
                        try:
                            self.repository._connection.commit()
                        except BaseException:
                            self.repository._connection.rollback()
                            raise
                finally:
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
        encoded: dict[str, tuple[ArtifactIndexEntryV2, str]] = {}
        for entry in batch:
            metadata_json = canonical_json(entry.metadata)
            previous = encoded.get(entry.artifact_ref)
            if previous is not None:
                prior_entry, prior_json = previous
                if (
                    prior_entry.artifact_type,
                    prior_entry.content_hash,
                    prior_entry.created_at_ns,
                    prior_entry.available_at_ns,
                    prior_json,
                ) != (
                    entry.artifact_type,
                    entry.content_hash,
                    entry.created_at_ns,
                    entry.available_at_ns,
                    metadata_json,
                ):
                    raise ValueError("artifact batch repeats an identity with different immutable content")
            else:
                encoded[entry.artifact_ref] = (entry, metadata_json)
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
            for entry, metadata_json in encoded.values():
                row = existing.get(entry.artifact_ref)
                if row is not None:
                    stored_tuple = (
                        row["artifact_type"],
                        row["content_hash"],
                        row["created_at_ns"],
                        row["available_at_ns"],
                        row["metadata_json"],
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
                        metadata_json,
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
        return batch

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
        return tuple(ArtifactIndexEntryV2._from_storage_row(row) for row in rows
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
        prefix = " AND ".join(_archive_json_expression(name) + "=?" for name in (
            "instrument_key_json", "instrument_revision", "event_type", "availability_class"))
        close = _archive_json_expression("event_at_ns")
        params = (key.to_canonical_json(), key.contract_revision, "BAR_" + interval, "ACTUAL_SYSTEM")
        with self._lock:
            origins = self._connection.execute(
                "SELECT " + close + " AS close_at_ns FROM artifact_index "
                "INDEXED BY public_archive_history_lookup "
                "WHERE artifact_type='PublicObservationIndexV2' AND " + prefix
                + " AND " + close + ">? AND " + close + "<=? ORDER BY " + close
                + ",available_at_ns,artifact_ref LIMIT ?",
                (*params, after_close_at_ns, cutoff_ns, limit + 1)).fetchall()
            if not origins:
                return (), after_close_at_ns, False
            closes = tuple(sorted({int(row[0]) for row in origins[:limit]}))
            rows = self._connection.execute(
                "SELECT * FROM artifact_index INDEXED BY public_archive_history_lookup "
                "WHERE artifact_type='PublicObservationIndexV2' AND "
                + prefix + " AND " + close + " IN (" + ",".join("?" for _ in closes)
                + ") LIMIT ?",
                (*params, *closes, limit + 1)).fetchall()
        if len(rows) > limit:
            raise ValueError("ACTIVE_HISTORY_REVISION_PAGE_OVERFLOW")
        if any(row["available_at_ns"] > cutoff_ns for row in rows):
            raise ValueError("ACTIVE_HISTORY_FUTURE_REVISION_PENDING")
        return (tuple(ArtifactIndexEntryV2._from_storage_row(row) for row in rows),
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
                    self._update_collector_head(connection, ArtifactIndexEntryV2._from_storage_row(row))
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
        return tuple(ArtifactIndexEntryV2._from_storage_row(row) for row in rows)

    @staticmethod
    def _schedule_artifact_origin(connection: sqlite3.Connection, entry: ArtifactIndexEntryV2,
                                  lane: str) -> None:
        work_id = entry.artifact_ref
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
                        parsed = json.loads(row["metadata_json"])
                        if isinstance(parsed, Mapping):
                            metadata = parsed
                            lane = _due_source_lane(row["artifact_type"], metadata) or lane
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

    def get_artifact(self, artifact_ref: str) -> ArtifactIndexEntryV2 | None:
        sha256_ref(artifact_ref, field="artifact_ref")
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM artifact_index WHERE artifact_ref=?", (artifact_ref,)
            ).fetchone()
        if row is None:
            return None
        return ArtifactIndexEntryV2(
            row["artifact_ref"],
            row["artifact_type"],
            row["content_hash"],
            row["created_at_ns"],
            row["available_at_ns"],
            json.loads(row["metadata_json"]),
        )

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
            for offset in range(0, len(refs), 500):
                batch = refs[offset : offset + 500]
                if not batch:
                    continue
                marks = ",".join("?" for _ in batch)
                rows = self._connection.execute(
                    f"SELECT * FROM artifact_index WHERE artifact_ref IN ({marks})",
                    batch,
                ).fetchall()
                for row in rows:
                    entries[row["artifact_ref"]] = {
                        "artifact_type": row["artifact_type"],
                        "content_hash": row["content_hash"],
                        "available_at_ns": row["available_at_ns"],
                        "metadata": json.loads(row["metadata_json"]),
                    }
        return entries

    def artifact_entries(self, artifact_type: str) -> tuple[ArtifactIndexEntryV2, ...]:
        """Read a small typed artifact-index namespace without adding a warehouse table."""
        nonblank(artifact_type, field="artifact_type")
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM artifact_index WHERE artifact_type=? ORDER BY created_at_ns,artifact_ref",
                (artifact_type,),
            ).fetchall()
        return tuple(
            ArtifactIndexEntryV2._from_storage_row(row)
            for row in rows
        )

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
        entries: list[ArtifactIndexEntryV2] = []
        invalid = 0
        for row in rows[:limit]:
            try:
                entries.append(ArtifactIndexEntryV2._from_storage_row(row))
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                invalid += 1
        return LatestArtifactPageV1(tuple(entries), len(rows) > limit, invalid)

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
                            values.append(instrument_key_json)
                        names.extend(("event_type", "availability_class"))
                        values.extend((kind, source_class))
                        lane = " AND ".join(_archive_json_expression(name) + "=?" for name in names)
                        order_time = effective if replay else "available_at_ns"
                        budget = _RECEIPT_REPLAY_CANDIDATE_LIMIT + 1 if replay else limit
                        query = ("SELECT * FROM artifact_index INDEXED BY "
                                 f"public_exact_receipt_{scope}_{view}_lookup "
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
            return tuple(ArtifactIndexEntryV2._from_storage_row(row) for row in rows)
        else:
            close = _archive_json_expression("event_at_ns")
            if len(kinds) != 1:
                raise ValueError("bounded causal bar history requires one exact interval")
            # Seek distinct close origins directly. GROUP BY over a materialized
            # retained-history CTE made a small output limit conceal a full scan.
            base = " AND ".join(_archive_json_expression(name)+"=?" for name in (
                "instrument_key_json","instrument_revision","event_type","availability_class"))
            exact = (instrument_key_json,instrument_revision,kinds[0],availability_class or "ACTUAL_SYSTEM")
            entries: list[ArtifactIndexEntryV2] = []
            cursor_close = cutoff+1
            with self._lock:
                for _ in range(limit):
                    origin = self._connection.execute(
                        "SELECT " + close + " FROM artifact_index INDEXED BY public_archive_history_lookup "
                        "WHERE artifact_type='PublicObservationIndexV2' AND " + base + " AND "
                        + close + "<? ORDER BY " + close + " DESC LIMIT 1",
                        (*exact,cursor_close)).fetchone()
                    if origin is None:
                        break
                    cursor_close = int(origin[0])
                    revisions = self._connection.execute(
                        "SELECT * FROM artifact_index INDEXED BY public_archive_history_lookup "
                        "WHERE artifact_type='PublicObservationIndexV2' AND " + base + " AND "
                        + close + "=? AND " + effective + "<=? LIMIT 129",
                        (*exact,cursor_close,cutoff)).fetchall()
                    if len(revisions)>128:
                        raise ValueError("archive exact-origin revision work bound exceeded (128)")
                    entries.extend(ArtifactIndexEntryV2._from_storage_row(row) for row in revisions)
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
                 "AND " + _archive_json_expression("instrument_key_json") + "=? AND "
                 + _archive_json_expression("instrument_revision") + "=? AND "
                 + _archive_json_expression("event_type") + "=? AND "
                 + _archive_json_expression("availability_class") + "='ACTUAL_SYSTEM' AND "
                 + _archive_json_expression("event_at_ns") + "=? AND "
                 + _archive_json_expression("bar_content_hash") + " IS NOT NULL "
                 "AND available_at_ns<=? ORDER BY available_at_ns,"
                 + _archive_json_expression("record_id") + ",artifact_ref LIMIT ?")
        with self._lock:
            rows = self._connection.execute(query, (instrument_key.to_canonical_json(),
                instrument_key.contract_revision, event_type, close, cutoff, limit + 1)).fetchall()
        if len(rows) > limit:
            raise ValueError("exact bar source revision lookup exceeded its explicit bound")
        return tuple(ArtifactIndexEntryV2._from_storage_row(row) for row in rows)

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
        return tuple(ArtifactIndexEntryV2._from_storage_row(row) for row in rows)

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
        return tuple(
            ArtifactIndexEntryV2._from_storage_row(row)
            for row in rows
        )

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
        for row in rows:
            try:
                entries.append(ArtifactIndexEntryV2._from_storage_row(row))
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                invalid += 1
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
        key_expr = "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.instrument_key_json') END"
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
                    "SELECT * FROM artifact_index INDEXED BY public_origin_discovery_lookup "
                    "WHERE artifact_type='PublicObservationIndexV2' AND " + key_expr + "=? AND "
                    + event_expr + "=? AND " + availability_expr + "=? "
                    "AND available_at_ns>=? AND available_at_ns<=? "
                    "AND (available_at_ns,artifact_ref)>(?,?) "
                    "ORDER BY available_at_ns,artifact_ref LIMIT 129",
                    (key_json,event_type,AvailabilityClassV2.ACTUAL_SYSTEM.value,
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
            entry = ArtifactIndexEntryV2._from_storage_row(row)
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
                entries.append(ArtifactIndexEntryV2._from_storage_row(row))
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
        entries = tuple(ArtifactIndexEntryV2._from_storage_row(row) for row in rows)
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
        entries = tuple(ArtifactIndexEntryV2._from_storage_row(row) for row in rows)
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
                entries.append(ArtifactIndexEntryV2._from_storage_row(row))
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
