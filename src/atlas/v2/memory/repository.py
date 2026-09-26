"""Transactional repository for restart-safe opportunity memory.

This database is owned by one local ``atlas-ops`` writer. It has no venue or
capital mutation API and is intentionally separate from V1 live-control data.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

from .._serialization import FrozenMap, canonical_json, nonblank, sha256_json, sha256_ref, timestamp
from ..contracts import OpportunityWatchV2, WatchStateV2
from ..models.protocol import ModelManifestV2
from .schema import OPS_SCHEMA_NAMESPACE, OPS_SCHEMA_VERSION, initialize, validate_read_only

_ACTIVE_STATES = (
    WatchStateV2.DETECTED.value,
    WatchStateV2.WAITING_FOR_EVENT.value,
    WatchStateV2.READY_FOR_RECHECK.value,
    WatchStateV2.CONFIRMED.value,
)


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

    def __post_init__(self) -> None:
        sha256_ref(self.artifact_ref, field="artifact_ref")
        nonblank(self.artifact_type, field="artifact_type")
        sha256_ref(self.content_hash, field="content_hash")
        timestamp(self.created_at_ns, field="created_at_ns")
        timestamp(self.available_at_ns, field="available_at_ns")
        if self.available_at_ns < self.created_at_ns:
            raise ValueError("artifact available_at_ns cannot precede created_at_ns")
        canonical_json(self.metadata)
        object.__setattr__(self, "metadata", FrozenMap(json.loads(canonical_json(self.metadata))))


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
        except BaseException:
            self._connection.close()
            raise

    @property
    def schema_version(self) -> int:
        return OPS_SCHEMA_VERSION

    @property
    def schema_namespace(self) -> str:
        return OPS_SCHEMA_NAMESPACE

    def close(self) -> None:
        with self._lock:
            self._connection.close()

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
                (watch.watch_id, watch.state.value, watch.state_version, watch.expires_at_ns,
                 watch.required_next_event, watch.last_event_id, watch.last_evaluated_at_ns, payload, digest),
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
                    "watch_id DESC LIMIT ?", (limit,)
                ).fetchall()[::-1]
        return tuple(self._watch_from_row(row) for row in rows)

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
                return TransitionResultV2(OpportunityWatchV2.from_dict(json.loads(duplicate["result_watch_json"])), False)
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
                (updated.state.value, updated.state_version, updated.expires_at_ns, updated.required_next_event,
                 updated.last_event_id, updated.last_evaluated_at_ns, updated_json, updated_hash,
                 watch_id, expected_state_version),
            )
            connection.execute(
                """INSERT INTO watch_transition(watch_id,event_id,state_version,from_state,to_state,event_at_ns,
                   transition_at_ns,payload_json,payload_hash,result_watch_json) VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (watch_id, event_id, updated.state_version, current.state.value, updated.state.value,
                 event_at_ns, transition_at_ns, transition_json, transition_hash, updated_json),
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
                (outbox_id, watch_id, event_id, updated.state_version, dedupe_key, outbox_json,
                 sha256_json(outbox_payload), transition_at_ns),
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
            row["outbox_id"], row["watch_id"], row["event_id"], row["state_version"],
            row["dedupe_key"], payload, row["payload_hash"], row["created_at_ns"],
            row["handled_at_ns"], row["handling_ref"],
        )

    def record_outbox_handling(self, outbox_id: str, *, handled_at_ns: int, handling_ref: str) -> OutboxItemV2:
        nonblank(outbox_id, field="outbox_id")
        nonblank(handling_ref, field="handling_ref")
        timestamp(handled_at_ns, field="handled_at_ns")
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM ops_outbox WHERE outbox_id=?", (outbox_id,)).fetchone()
            if row is None:
                raise KeyError(f"unknown outbox_id: {outbox_id}")
            if row["handled_at_ns"] is not None and (row["handled_at_ns"] != handled_at_ns or row["handling_ref"] != handling_ref):
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
                (record.source_id, record.observed_at_ns, record.available_at_ns, record.status,
                 record.details_ref, payload_json, digest),
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
                    "ORDER BY observed_at_ns DESC LIMIT ?", (source_id, limit),
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
            rows = self._connection.execute("SELECT DISTINCT source_id FROM source_health ORDER BY source_id").fetchall()
        return tuple(row[0] for row in rows)

    def register_model_manifest(self, manifest: ModelManifestV2) -> str:
        manifest_hash = manifest.manifest_hash
        manifest_json = manifest.to_canonical_json()
        with self._transaction() as connection:
            row = connection.execute("SELECT manifest_json FROM model_registry WHERE manifest_hash=?", (manifest_hash,)).fetchone()
            if row is not None and row["manifest_json"] != manifest_json:
                raise ValueError("model manifest hash collision/conflicting immutable content")
            connection.execute(
                "INSERT OR IGNORE INTO model_registry(manifest_hash,provider,checkpoint_id,promotion_status,manifest_json) VALUES(?,?,?,?,?)",
                (manifest_hash, manifest.provider, manifest.checkpoint_id, manifest.promotion_status.value, manifest_json),
            )
        return manifest_hash

    def get_model_manifest(self, manifest_hash: str) -> ModelManifestV2 | None:
        sha256_ref(manifest_hash, field="manifest_hash")
        with self._lock:
            row = self._connection.execute("SELECT manifest_json FROM model_registry WHERE manifest_hash=?", (manifest_hash,)).fetchone()
        if row is None:
            return None
        manifest = ModelManifestV2.from_dict(json.loads(row["manifest_json"]))
        if manifest.manifest_hash != manifest_hash:
            raise RuntimeError("stored model manifest hash mismatch")
        return manifest

    def register_artifact(self, entry: ArtifactIndexEntryV2) -> ArtifactIndexEntryV2:
        metadata_json = canonical_json(entry.metadata)
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM artifact_index WHERE artifact_ref=?", (entry.artifact_ref,)).fetchone()
            if row is not None:
                stored_tuple = (row["artifact_type"], row["content_hash"], row["created_at_ns"], row["available_at_ns"], row["metadata_json"])
                requested_tuple = (entry.artifact_type, entry.content_hash, entry.created_at_ns, entry.available_at_ns, metadata_json)
                if stored_tuple != requested_tuple:
                    raise ValueError("artifact_ref already indexes different immutable content")
                return entry
            connection.execute(
                "INSERT INTO artifact_index(artifact_ref,artifact_type,content_hash,created_at_ns,available_at_ns,metadata_json) VALUES(?,?,?,?,?,?)",
                (entry.artifact_ref, entry.artifact_type, entry.content_hash, entry.created_at_ns,
                 entry.available_at_ns, metadata_json),
            )
        return entry

    def get_artifact(self, artifact_ref: str) -> ArtifactIndexEntryV2 | None:
        sha256_ref(artifact_ref, field="artifact_ref")
        with self._lock:
            row = self._connection.execute("SELECT * FROM artifact_index WHERE artifact_ref=?", (artifact_ref,)).fetchone()
        if row is None:
            return None
        return ArtifactIndexEntryV2(
            row["artifact_ref"], row["artifact_type"], row["content_hash"], row["created_at_ns"],
            row["available_at_ns"], json.loads(row["metadata_json"]),
        )

    def artifact_entries(self, artifact_type: str) -> tuple[ArtifactIndexEntryV2, ...]:
        """Read a small typed artifact-index namespace without adding a warehouse table."""
        nonblank(artifact_type, field="artifact_type")
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM artifact_index WHERE artifact_type=? ORDER BY created_at_ns,artifact_ref",
                (artifact_type,),
            ).fetchall()
        return tuple(
            ArtifactIndexEntryV2(
                row["artifact_ref"], row["artifact_type"], row["content_hash"], row["created_at_ns"],
                row["available_at_ns"], json.loads(row["metadata_json"]),
            )
            for row in rows
        )

    def artifact_entries_by_types(
        self, artifact_types: tuple[str, ...], *, limit: int = 2_000
    ) -> tuple[ArtifactIndexEntryV2, ...]:
        """Read bounded typed artifact namespaces in stable index order."""
        types = tuple(sorted(set(artifact_types)))
        if not types or any(not isinstance(item, str) or not item.strip() for item in types):
            raise ValueError("at least one non-empty artifact type is required")
        if type(limit) is not int or not 1 <= limit <= 10_000:
            raise ValueError("artifact read limit must be between 1 and 10000")
        marks = ",".join("?" for _ in types)
        with self._lock:
            rows = self._connection.execute(
                f"SELECT * FROM artifact_index WHERE artifact_type IN ({marks}) "
                "ORDER BY created_at_ns DESC, artifact_ref DESC LIMIT ?",
                (*types, limit),
            ).fetchall()
        rows.reverse()
        return tuple(
            ArtifactIndexEntryV2(
                row["artifact_ref"], row["artifact_type"], row["content_hash"], row["created_at_ns"],
                row["available_at_ns"], json.loads(row["metadata_json"]),
            )
            for row in rows
        )

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
