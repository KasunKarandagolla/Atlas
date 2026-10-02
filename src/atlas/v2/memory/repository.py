"""Transactional repository for restart-safe opportunity memory.

This database is owned by one local ``atlas-ops`` writer. It has no venue or
capital mutation API and is intentionally separate from V1 live-control data.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from .._serialization import FrozenMap, canonical_json, json_value, nonblank, sha256_json, sha256_ref, timestamp
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
        object.__setattr__(self, "metadata", FrozenMap(json_value(self.metadata)))


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


_ARTIFACT_METADATA_IDENTITY_EXPRESSIONS: dict[tuple[str, tuple[str, ...]], str] = {
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
}


def _archive_json_expression(name: str) -> str:
    # Only internal, fixed field names are passed here; never caller SQL.
    return f"CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.{name}') END"


_ARCHIVE_QUERY_INDEXES = (
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
                for statement in _ARCHIVE_QUERY_INDEXES:
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

            def __enter__(self) -> sqlite3.Connection:
                self.repository._lock.acquire()
                try:
                    self.repository._connection.execute("BEGIN IMMEDIATE")
                except BaseException:
                    self.repository._lock.release()
                    raise
                return self.repository._connection

            def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
                try:
                    if exc_type:
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

    def list_active_watches(self) -> tuple[OpportunityWatchV2, ...]:
        marks = ",".join("?" for _ in _ACTIVE_STATES)
        with self._lock:
            rows = self._connection.execute(
                f"SELECT * FROM watch WHERE state IN ({marks}) ORDER BY expires_at_ns, watch_id",
                _ACTIVE_STATES,
            ).fetchall()
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

    def source_health_sources(self) -> tuple[str, ...]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT DISTINCT source_id FROM source_health ORDER BY source_id"
            ).fetchall()
        return tuple(row[0] for row in rows)

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
        return batch

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
        clauses = ["artifact_type='PublicObservationIndexV2'",
                   _archive_json_expression("instrument_revision") + "=?",
                   _archive_json_expression("event_type") + " IN (" + ",".join("?" for _ in kinds) + ")"]
        params: list[Any] = [instrument_revision, *kinds]
        if instrument_key_json is not None:
            nonblank(instrument_key_json, field="instrument_key_json")
            clauses.append(_archive_json_expression("instrument_key_json") + "=?")
            params.append(instrument_key_json)
        if availability_class is not None:
            if availability_class not in {"ACTUAL_SYSTEM", "RECONSTRUCTED_MARKET"}:
                raise ValueError("archive source lookup availability view is invalid")
            clauses.append(_archive_json_expression("availability_class") + "=?")
            params.append(availability_class)
        effective = ("available_at_ns" if availability_class != "RECONSTRUCTED_MARKET"
                     else _archive_json_expression("replay_available_at_ns"))
        clauses.append(effective + "<=?")
        params.append(cutoff)
        if instrument_key_json is None or not all(kind.startswith("BAR_") for kind in kinds):
            query = ("SELECT * FROM artifact_index WHERE " + " AND ".join(clauses)
                     + " ORDER BY available_at_ns DESC," + _archive_json_expression("record_id")
                     + " DESC LIMIT ?")
            params.append(limit)
        else:
            close = _archive_json_expression("event_at_ns")
            clauses.append(_archive_json_expression("bar_content_hash") + " IS NOT NULL")
            clauses.append("json_type(CASE WHEN json_valid(metadata_json) THEN metadata_json ELSE '{}' END,"
                           "'$.event_at_ns')='integer'")
            query = ("WITH eligible AS (SELECT * FROM artifact_index WHERE " + " AND ".join(clauses)
                     + "), origins AS (SELECT " + close + " AS close_at_ns FROM eligible GROUP BY "
                     + close + " ORDER BY close_at_ns DESC LIMIT ?) SELECT * FROM eligible WHERE "
                     + close + " IN (SELECT close_at_ns FROM origins) ORDER BY " + close + " DESC,"
                     + effective + " DESC," + _archive_json_expression("record_id") + " DESC LIMIT 100001")
            params.append(limit)
        with self._lock:
            rows = self._connection.execute(query, tuple(params)).fetchall()
        if len(rows) > 100_000:
            raise ValueError("archive source lookup exceeded its revision-row bound (100000)")
        return tuple(ArtifactIndexEntryV2._from_storage_row(row) for row in rows)

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
        partition = ",".join(fields)
        query = ("WITH ranked AS (SELECT artifact_ref,ROW_NUMBER() OVER (PARTITION BY " + partition
                 + " ORDER BY available_at_ns DESC,artifact_ref DESC) AS stream_rank FROM artifact_index "
                 "WHERE artifact_type='L2FrameArchiveCheckpointV2') SELECT a.* FROM ranked r "
                 "JOIN artifact_index a ON a.artifact_ref=r.artifact_ref WHERE r.stream_rank=1 "
                 "ORDER BY a.artifact_ref LIMIT ?")
        with self._lock:
            rows = self._connection.execute(query, (limit + 1,)).fetchall()
        if len(rows) > limit:
            raise ValueError("L2 restart exceeded its distinct-stream bound")
        return tuple(ArtifactIndexEntryV2._from_storage_row(row) for row in rows)

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
        marks = ",".join("?" for _ in types)
        availability_filter = " AND available_at_ns<=?" if cutoff is not None else ""
        params: tuple[Any, ...] = (*types, *((cutoff,) if cutoff is not None else ()), limit)
        with self._lock:
            rows = self._connection.execute(
                f"SELECT * FROM artifact_index WHERE artifact_type IN ({marks}){availability_filter} "
                "ORDER BY created_at_ns DESC, artifact_ref DESC LIMIT ?",
                params,
            ).fetchall()
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
        marks = ",".join("?" for _ in types)
        cursor_clause = " AND (created_at_ns,artifact_ref)<(?,?)" if after is not None else ""
        params: tuple[Any, ...] = (
            *types, cutoff, *((after[0], after[1]) if after is not None else ()), limit,
        )
        with self._lock:
            rows = self._connection.execute(
                f"SELECT * FROM artifact_index WHERE artifact_type IN ({marks}) "
                f"AND available_at_ns<=?{cursor_clause} "
                "ORDER BY created_at_ns DESC,artifact_ref DESC LIMIT ?",
                params,
            ).fetchall()
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
        close_expr = "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.event_at_ns') END"
        key_expr = "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.instrument_key_json') END"
        event_expr = "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.event_type') END"
        availability_expr = (
            "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.availability_class') END"
        )
        bar_ref_expr = "CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.bar_content_hash') END"
        close_filter = f" AND {close_expr}>?" if after_close_at_ns is not None else ""
        params: tuple[Any, ...] = (
            key_json,
            event_type,
            AvailabilityClassV2.ACTUAL_SYSTEM.value,
            available_from,
            available_through,
            min_close,
            *((after_close_at_ns,) if after_close_at_ns is not None else ()),
            limit + 1,
            limit + 1,
        )
        query = f"""
            WITH eligible AS (
                SELECT artifact_ref,
                       CAST({close_expr} AS INTEGER) AS close_at_ns,
                       available_at_ns
                FROM artifact_index
                WHERE artifact_type='PublicObservationIndexV2'
                  AND {key_expr}=?
                  AND {event_expr}=?
                  AND {availability_expr}=?
                  AND {bar_ref_expr} IS NOT NULL
                  AND json_type(
                        CASE WHEN json_valid(metadata_json) THEN metadata_json ELSE '{{}}' END,
                        '$.event_at_ns'
                  )='integer'
                  AND (CAST({close_expr} AS INTEGER) % {duration_ns})=0
                  AND available_at_ns>=? AND available_at_ns<=?
                  AND CAST({close_expr} AS INTEGER)>=?
                  {close_filter}
            ), first_revision AS (
                SELECT close_at_ns, MIN(available_at_ns) AS first_available_at_ns
                FROM eligible
                GROUP BY close_at_ns
            ), representative AS (
                SELECT e.close_at_ns, MIN(e.artifact_ref) AS artifact_ref
                FROM eligible e
                JOIN first_revision f
                  ON f.close_at_ns=e.close_at_ns
                 AND f.first_available_at_ns=e.available_at_ns
                GROUP BY e.close_at_ns
                ORDER BY e.close_at_ns ASC
                LIMIT ?
            ), selected AS (
                SELECT close_at_ns, artifact_ref
                FROM representative
                ORDER BY close_at_ns ASC
                LIMIT ?
            )
            SELECT a.*
            FROM selected s
            JOIN artifact_index a ON a.artifact_ref=s.artifact_ref
            ORDER BY s.close_at_ns ASC
        """
        with self._lock:
            rows = self._connection.execute(query, params).fetchall()

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
        """Return a bounded distinct source-ID set without enumerating observation history."""
        if type(limit) is not int or not 1 <= limit <= 2_000:
            raise ValueError("public observation source-ID bound must be between 1 and 2000")
        with self._lock:
            rows = self._connection.execute(
                """SELECT DISTINCT json_extract(metadata_json, '$.source_id') AS source_id
                   FROM artifact_index
                   WHERE artifact_type='PublicObservationIndexV2'
                     AND json_valid(metadata_json)
                     AND json_type(metadata_json, '$.source_id')='text'
                   ORDER BY source_id LIMIT ?""",
                (limit + 1,),
            ).fetchall()
        if len(rows) > limit:
            raise ValueError("public observation source-ID set exceeded its deterministic bound")
        values = tuple(str(row["source_id"]) for row in rows)
        if any(not value.strip() for value in values) or values != tuple(sorted(set(values))):
            raise ValueError("public observation source-ID inventory is invalid or ambiguous")
        return values

    def public_reconciliation_source_ids(self, *, limit: int = 128) -> tuple[str, ...]:
        """Return a bounded distinct reconciliation source-ID set."""
        if type(limit) is not int or not 1 <= limit <= 2_000:
            raise ValueError("reconciliation source-ID bound must be between 1 and 2000")
        with self._lock:
            rows = self._connection.execute(
                """SELECT DISTINCT json_extract(metadata_json, '$.reconciliation.source_id') AS source_id
                   FROM artifact_index
                   WHERE artifact_type='OpsPublicSourceReconciliationV1'
                     AND json_valid(metadata_json)
                     AND json_type(metadata_json, '$.reconciliation.source_id')='text'
                   ORDER BY source_id LIMIT ?""",
                (limit + 1,),
            ).fetchall()
        if len(rows) > limit:
            raise ValueError("reconciliation source-ID set exceeded its deterministic bound")
        values = tuple(str(row["source_id"]) for row in rows)
        if any(not value.strip() for value in values) or values != tuple(sorted(set(values))):
            raise ValueError("reconciliation source-ID inventory is invalid or ambiguous")
        return values

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
        query = """SELECT e.* FROM artifact_index AS e
                   WHERE e.artifact_type='OpsDecisionEventSourceV1'
                     AND e.available_at_ns<=?
                     AND json_valid(e.metadata_json)
                     AND json_type(e.metadata_json, '$.event.event_id')='text'
                     AND NOT EXISTS (
                         SELECT 1 FROM artifact_index AS r
                         WHERE r.artifact_type='OpsSupervisorReceiptIdentityV1'
                           AND json_valid(r.metadata_json)
                           AND json_extract(r.metadata_json, '$.event_id')=
                               json_extract(e.metadata_json, '$.event.event_id')
                     )
                   ORDER BY e.available_at_ns, e.created_at_ns, e.artifact_ref
                   LIMIT ?"""
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
                """SELECT * FROM artifact_index
                   WHERE artifact_type='S3NativeM1OriginAccountingCheckpointV1'
                     AND json_valid(metadata_json)
                     AND json_extract(metadata_json, '$.instrument_key_json')=?
                   ORDER BY CAST(json_extract(metadata_json, '$.checkpoint.generation') AS INTEGER) DESC,
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
                "SELECT * FROM artifact_index WHERE artifact_type='M15OriginAccountingCheckpointV1' "
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
        than choose among a truncated set.
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
            limit + 1,
        )
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM artifact_index WHERE "
                f"artifact_type='{artifact_type}' AND {identity_expression}=? AND available_at_ns<=?"
                f"{cursor_clause} ORDER BY created_at_ns DESC,artifact_ref DESC LIMIT ?",
                params,
            ).fetchall()
        has_more = len(rows) > limit
        selected_rows = rows[:limit]
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
