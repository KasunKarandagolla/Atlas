"""Small restart-aware public collector orchestration; no broker or credentials."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass

from .._serialization import sha256_json, timestamp
from ..instruments import InstrumentKeyV2, InstrumentRegistryV2
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository, RestartSnapshotV2
from .bars import CausalBarV2
from .health import PublicSourceHealthV2, PublicSourceStateV2, SourceHealthTrackerV2
from .history import ArchiveRecordKindV2, ImportedObservationV2, ParquetObservationArchiveV2
from .raw import AppendResultV2, AppendStatusV2, RawObservationStoreV2, RawObservationV2
from .subscriptions import SubscriptionPlanV2, restore_subscription_plan
from .universe import ComputeTierV2


class SequenceGapV2(RuntimeError):
    def __init__(self, source_id: str, channel: str, previous: int, incoming: int) -> None:
        super().__init__(f"{source_id}/{channel} sequence gap: expected {previous + 1}, received {incoming}")
        self.source_id = source_id
        self.channel = channel
        self.previous = previous
        self.incoming = incoming


@dataclass(frozen=True)
class CollectorIngestResultV2:
    append: AppendResultV2
    sequence_gap: SequenceGapV2 | None = None
    persistent_conflict: bool = False


@dataclass(frozen=True)
class ConflictingDuplicateV2:
    record_id: str
    existing_payload_hash: str
    incoming_payload_hash: str
    observed_at_ns: int
    quarantine_chunk_id: str


@dataclass(frozen=True)
class CollectorRestartV2:
    watches: RestartSnapshotV2
    subscriptions: SubscriptionPlanV2


class BoundedBackoffV2:
    def __init__(self, *, initial_ms: int = 250, max_ms: int = 30_000) -> None:
        if initial_ms <= 0 or max_ms < initial_ms:
            raise ValueError("bounded backoff configuration is invalid")
        self.initial_ms = initial_ms
        self.max_ms = max_ms

    def delay_ms(self, attempt: int) -> int:
        if type(attempt) is not int or attempt < 0:
            raise ValueError("attempt must be a nonnegative integer")
        return min(self.max_ms, self.initial_ms * (2 ** min(attempt, 30)))


class PublicCollectorV2:
    """One-process public ingestion coordinator over typed venue translators."""

    def __init__(
        self,
        *,
        repository: OpsRepository,
        registry: InstrumentRegistryV2,
        clock_ns: Callable[[], int],
        archive: ParquetObservationArchiveV2 | None = None,
        backoff: BoundedBackoffV2 | None = None,
    ) -> None:
        self.repository = repository
        self.registry = registry
        self.clock_ns = clock_ns
        self.archive = archive
        self.backoff = backoff or BoundedBackoffV2()
        self.store = RawObservationStoreV2()
        self.health = SourceHealthTrackerV2()
        self._last_sequence: dict[tuple[str, str], int] = {}
        self._pending_archive: list[ImportedObservationV2] = []
        self._pending_instrument_keys: dict[str, InstrumentKeyV2] = {}
        self._health_state: dict[str, PublicSourceStateV2] = {}
        self._cursor_hashes: dict[tuple[str, str], dict[str, str]] = {}
        self._conflicts: list[ConflictingDuplicateV2] = []
        self._bar_hashes: dict[str, str | None] = {}
        self._restore_health()
        self._restore_cursors()

    def _restore_health(self) -> None:
        for source_id in self.repository.source_health_sources():
            history = self.repository.source_health_history(source_id)
            if not history:
                continue
            latest = history[-1]
            try:
                state = PublicSourceStateV2(latest.status)
            except ValueError:
                continue
            self._health_state[source_id] = state
            self.health.append(
                PublicSourceHealthV2(
                    source_id,
                    latest.observed_at_ns,
                    latest.available_at_ns,
                    state,
                    latest.details_ref or sha256_json(latest.to_dict()),
                    "restored append-only source health state",
                )
            )
            if state == PublicSourceStateV2.HEALTHY_CURRENT:
                self._record_health(
                    source_id,
                    PublicSourceStateV2.INCOMPLETE_SNAPSHOT,
                    at_ns=max(self.clock_ns(), latest.observed_at_ns + 1),
                    details="collector restarted; reconnect overlap and gap repair are required",
                )

    def _restore_cursors(self) -> None:
        for entry in self.repository.artifact_entries("PublicCollectorCursorV2"):
            metadata = dict(entry.metadata)
            source_id = str(metadata.get("source_id", ""))
            channel = str(metadata.get("channel", ""))
            sequence = metadata.get("high_water_sequence")
            if source_id and channel and isinstance(sequence, int):
                cursor = (source_id, channel)
                self._last_sequence[cursor] = max(self._last_sequence.get(cursor, sequence), sequence)
                hashes = metadata.get("recent_payload_hashes", {})
                if isinstance(hashes, Mapping):
                    self._cursor_hashes[cursor] = {
                        str(record_id): str(payload_hash) for record_id, payload_hash in hashes.items()
                    }

    def _record_health(
        self, source_id: str, state: PublicSourceStateV2, *, at_ns: int, details: str
    ) -> PublicSourceHealthV2:
        timestamp(at_ns, field="health observation time")
        prior = self._health_state.get(source_id)
        if prior == state:
            latest = self.health.latest(source_id)
            if latest is not None:
                return latest
        latest = self.health.latest(source_id)
        if latest is not None and at_ns < latest.observed_at_ns:
            raise ValueError("source health observation cannot precede the most recent source state")
        if latest is not None and at_ns == latest.observed_at_ns:
            at_ns += 1
        record = PublicSourceHealthV2(
            source_id,
            at_ns,
            at_ns,
            state,
            sha256_json({"source_id": source_id, "state": state.value, "at_ns": at_ns, "details": details}),
            details,
        )
        self.health.append(record)
        self.repository.record_source_health(record.to_ops_record())
        self._health_state[source_id] = state
        return record

    def ingest(
        self,
        observation: RawObservationV2,
        *,
        raw_payload: bytes | str,
        instrument_key: InstrumentKeyV2 | None = None,
        bar: CausalBarV2 | None = None,
        sequence_channel: str | None = None,
        sequence_is_contiguous: bool = False,
    ) -> CollectorIngestResultV2:
        if instrument_key is None:
            instrument_key = self.registry.resolve_key_for_revision(observation.instrument_revision)
        elif (
            not isinstance(instrument_key, InstrumentKeyV2)
            or instrument_key.contract_revision != observation.instrument_revision
            or not any(item.key == instrument_key for item in self.registry.contracts())
        ):
            raise ValueError("explicit collector instrument key is not registered for the observation revision")
        payload_bytes = raw_payload.encode("utf-8") if isinstance(raw_payload, str) else raw_payload
        if (
            not isinstance(payload_bytes, bytes)
            or hashlib.sha256(payload_bytes).hexdigest() != observation.raw_payload_hash
        ):
            raise ValueError("archived raw payload bytes must match RawObservationV2.raw_payload_hash")
        if bar is not None and (bar.raw.content_hash != observation.content_hash or not bar.final):
            raise ValueError("collector bars must be final and bound to the exact raw observation")
        index_ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": observation.record_id})
        persisted_entry = self.repository.get_artifact(index_ref)
        persistent_hash: str | None = None
        if persisted_entry is not None:
            indexed = dict(persisted_entry.metadata)
            persistent_hash = str(indexed.get("raw_payload_hash", ""))
            persistent_matches = (
                persisted_entry.artifact_type == "PublicObservationIndexV2"
                and indexed.get("record_id") == observation.record_id
                and indexed.get("instrument_revision") == observation.instrument_revision
                and indexed.get("instrument_key_json") in (None, instrument_key.to_canonical_json())
                and indexed.get("event_at_ns") == observation.event_at_ns
                and indexed.get("published_at_ns") == observation.published_at_ns
                and indexed.get("translation_version") == observation.translation_version
                and indexed.get("revision_of") == observation.revision_of
                and tuple(indexed.get("quality_flags", ())) == observation.quality_flags
                and indexed.get("availability_class") == observation.availability_class.value
                and indexed.get("replay_available_at_ns") == observation.replay_available_at_ns
                and indexed.get("raw_payload_hash") == observation.raw_payload_hash
                and indexed.get("bar_content_hash") == (bar.content_hash if bar is not None else None)
            )
            append = AppendResultV2(
                AppendStatusV2.DUPLICATE if persistent_matches else AppendStatusV2.CONFLICT_QUARANTINED,
                observation,
                observation,
            )
            persistent_conflict = not persistent_matches
        else:
            append = self.store.append(observation)
            persistent_conflict = False
            if append.status == AppendStatusV2.DUPLICATE and self._bar_hashes.get(observation.record_id) != (
                bar.content_hash if bar is not None else None
            ):
                append = AppendResultV2(AppendStatusV2.CONFLICT_QUARANTINED, append.stored, observation)
        if append.status == AppendStatusV2.CONFLICT_QUARANTINED or persistent_conflict:
            prior_hash = persistent_hash
            if prior_hash is None:
                existing = self.store.get(observation.record_id)
                prior_hash = existing.raw_payload_hash if existing is not None else observation.raw_payload_hash
            if self.archive is None:
                raise RuntimeError(
                    "Parquet observation archive is required to durably quarantine a conflicting duplicate"
                )
            quarantine_chunk_id = sha256_json(
                {
                    "artifact_type": "PublicDuplicateConflictV2",
                    "record_id": observation.record_id,
                    "existing_payload_hash": prior_hash,
                    "incoming_payload_hash": observation.raw_payload_hash,
                }
            )
            self.archive.write_observation_chunk(
                quarantine_chunk_id,
                (
                    ImportedObservationV2(
                        1,
                        observation,
                        payload_bytes,
                        bar,
                        record_kind=ArchiveRecordKindV2.DUPLICATE_CONFLICT,
                    ),
                ),
            )
            conflict = ConflictingDuplicateV2(
                observation.record_id,
                prior_hash,
                observation.raw_payload_hash,
                self.clock_ns(),
                quarantine_chunk_id,
            )
            self._conflicts.append(conflict)
            metadata = {
                "record_id": conflict.record_id,
                "existing_payload_hash": conflict.existing_payload_hash,
                "incoming_payload_hash": conflict.incoming_payload_hash,
                "observed_at_ns": conflict.observed_at_ns,
                "quarantine_chunk_id": conflict.quarantine_chunk_id,
            }
            conflict_hash = sha256_json(metadata)
            self.repository.register_artifact(
                ArtifactIndexEntryV2(
                    conflict_hash,
                    "PublicDuplicateConflictV2",
                    conflict_hash,
                    conflict.observed_at_ns,
                    conflict.observed_at_ns,
                    metadata,
                )
            )
            self._record_health(
                observation.source_id,
                PublicSourceStateV2.SEQUENCE_GAP_CONFLICT,
                at_ns=conflict.observed_at_ns,
                details="conflicting duplicate event identity quarantined",
            )
            return CollectorIngestResultV2(append, persistent_conflict=persistent_conflict)
        if append.status == AppendStatusV2.DUPLICATE:
            return CollectorIngestResultV2(append)
        self._pending_archive.append(
            ImportedObservationV2(len(self._pending_archive) + 1, observation, payload_bytes, bar)
        )
        self._pending_instrument_keys[observation.record_id] = instrument_key
        self._bar_hashes[observation.record_id] = bar.content_hash if bar is not None else None

        gap: SequenceGapV2 | None = None
        if sequence_channel is not None and sequence_is_contiguous and observation.sequence is not None:
            try:
                sequence = int(observation.sequence)
            except (ValueError, TypeError):
                sequence = -1
            if sequence >= 0:
                cursor = (observation.source_id, sequence_channel)
                previous = self._last_sequence.get(cursor)
                if previous is not None and sequence > previous + 1:
                    gap = SequenceGapV2(observation.source_id, sequence_channel, previous, sequence)
                    self._record_health(
                        observation.source_id,
                        PublicSourceStateV2.SEQUENCE_GAP_CONFLICT,
                        at_ns=self.clock_ns(),
                        details=str(gap),
                    )
                elif previous is not None and sequence < previous:
                    gap = SequenceGapV2(observation.source_id, sequence_channel, previous, sequence)
                    self._record_health(
                        observation.source_id,
                        PublicSourceStateV2.INCOMPLETE_SNAPSHOT,
                        at_ns=self.clock_ns(),
                        details="out-of-order contiguous sequence requires source reconciliation",
                    )
                self._last_sequence[cursor] = max(previous or sequence, sequence)
                self._cursor_hashes.setdefault(cursor, {})[observation.record_id] = observation.raw_payload_hash
        if gap is None and self._health_state.get(observation.source_id) in (None, PublicSourceStateV2.HEALTHY_CURRENT):
            self._record_health(
                observation.source_id,
                PublicSourceStateV2.HEALTHY_CURRENT,
                at_ns=self.clock_ns(),
                details="public observation translated and accepted",
            )
        return CollectorIngestResultV2(append, gap)

    def flush_archive(self) -> str | None:
        if not self._pending_archive:
            return None
        if self.archive is None:
            raise RuntimeError("Parquet observation archive is required to flush collected public data")
        items = tuple(self._pending_archive)
        chunk_id = sha256_json(
            {
                "record_content_hashes": [item.observation.content_hash for item in items],
                "payload_hashes": [item.observation.raw_payload_hash for item in items],
                "bar_content_hashes": [item.bar.content_hash if item.bar is not None else None for item in items],
            }
        )
        path = self.archive.write_observation_chunk(chunk_id, items)
        index_entries: list[ArtifactIndexEntryV2] = []
        for item in items:
            observation = item.observation
            index_ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": observation.record_id})
            index_entries.append(
                ArtifactIndexEntryV2(
                    index_ref,
                    "PublicObservationIndexV2",
                    observation.content_hash,
                    observation.received_at_ns,
                    observation.available_at_ns,
                    {
                        "record_id": observation.record_id,
                        "instrument_revision": observation.instrument_revision,
                        "instrument_key_json": self._pending_instrument_keys[observation.record_id].to_canonical_json(),
                        "event_at_ns": observation.event_at_ns,
                        "published_at_ns": observation.published_at_ns,
                        "translation_version": observation.translation_version,
                        "revision_of": observation.revision_of,
                        "quality_flags": list(observation.quality_flags),
                        "availability_class": observation.availability_class.value,
                        "replay_available_at_ns": observation.replay_available_at_ns,
                        "raw_payload_hash": observation.raw_payload_hash,
                        "bar_content_hash": item.bar.content_hash if item.bar is not None else None,
                    },
                )
            )
        self.repository.register_artifacts(index_entries)
        self._pending_archive.clear()
        self._pending_instrument_keys.clear()
        return str(path)

    def quarantined_conflicts(self) -> tuple[ConflictingDuplicateV2, ...]:
        return tuple(self._conflicts)

    def on_disconnect(self, source_id: str, *, at_ns: int) -> PublicSourceHealthV2:
        return self._record_health(
            source_id, PublicSourceStateV2.DISCONNECTED, at_ns=at_ns, details="public transport disconnected"
        )

    def on_stale(self, source_id: str, *, at_ns: int) -> PublicSourceHealthV2:
        return self._record_health(
            source_id, PublicSourceStateV2.STALE, at_ns=at_ns, details="public source exceeded its freshness bound"
        )

    def on_rate_limited(self, source_id: str, *, at_ns: int) -> PublicSourceHealthV2:
        return self._record_health(
            source_id,
            PublicSourceStateV2.DEGRADED_RATE_LIMITED,
            at_ns=at_ns,
            details="public source rate limited; data eligibility is suspended",
        )

    def mark_incomplete_snapshot(self, source_id: str, *, at_ns: int, details: str) -> PublicSourceHealthV2:
        return self._record_health(source_id, PublicSourceStateV2.INCOMPLETE_SNAPSHOT, at_ns=at_ns, details=details)

    def begin_reconnect(self, source_id: str, *, attempt: int, at_ns: int) -> tuple[PublicSourceHealthV2, int]:
        record = self._record_health(
            source_id, PublicSourceStateV2.RECONNECTING, at_ns=at_ns, details="bounded public reconnect in progress"
        )
        return record, self.backoff.delay_ms(attempt)

    def reconnected(self, source_id: str, *, at_ns: int) -> PublicSourceHealthV2:
        return self._record_health(
            source_id,
            PublicSourceStateV2.INCOMPLETE_SNAPSHOT,
            at_ns=at_ns,
            details="reconnected; overlap/gap repair is required before eligibility",
        )

    def reconcile_after_reconnect(
        self, source_id: str, *, at_ns: int, complete_snapshot: bool, missed_interval_repaired: bool
    ) -> PublicSourceHealthV2:
        healthy = complete_snapshot and missed_interval_repaired
        state = PublicSourceStateV2.HEALTHY_CURRENT if healthy else PublicSourceStateV2.INCOMPLETE_SNAPSHOT
        detail = "overlap snapshot and missed interval verified" if healthy else "reconnect evidence remains incomplete"
        return self._record_health(source_id, state, at_ns=at_ns, details=detail)

    def restore_subscriptions(
        self,
        tiers: Mapping[InstrumentKeyV2, ComputeTierV2],
        *,
        now_ns: int,
        expiry_ids: Mapping[str, tuple[str, str]] | None = None,
        open_positions: Iterable[InstrumentKeyV2] = (),
    ) -> CollectorRestartV2:
        snapshot, plan = restore_subscription_plan(
            self.repository,
            tiers,
            now_ns=now_ns,
            expiry_ids=expiry_ids,
            open_positions=open_positions,
        )
        return CollectorRestartV2(snapshot, plan)

    def checkpoint_cursors(self, *, at_ns: int, recent_limit: int = 512) -> tuple[str, ...]:
        timestamp(at_ns, field="checkpoint at")
        if self._pending_archive:
            raise RuntimeError("public observations must be durably archived before cursor checkpoint")
        if recent_limit <= 0:
            raise ValueError("recent_limit must be positive")
        refs: list[str] = []
        for (source_id, channel), high_water in sorted(self._last_sequence.items()):
            recent_hashes = dict(sorted(self._cursor_hashes.get((source_id, channel), {}).items())[-recent_limit:])
            metadata = {
                "source_id": source_id,
                "channel": channel,
                "high_water_sequence": high_water,
                "checkpoint_at_ns": at_ns,
                "recent_payload_hashes": recent_hashes,
            }
            content_hash = sha256_json(metadata)
            artifact_ref = sha256_json({"artifact_type": "PublicCollectorCursorV2", "metadata": metadata})
            entry = ArtifactIndexEntryV2(artifact_ref, "PublicCollectorCursorV2", content_hash, at_ns, at_ns, metadata)
            self.repository.register_artifact(entry)
            refs.append(artifact_ref)
        return tuple(refs)
