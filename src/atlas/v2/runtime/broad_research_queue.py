"""Durable one-slot queue for exact cutoff-bound broad research inputs.

The queue stores prepared active-history states as immutable snapshots. It does
not restore live order books: those are process-local observations and S4 is
unavailable after restart until a new live sequence is observed.
"""
from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .._serialization import json_value, sha256_json, sha256_ref, timestamp
from ..chronology import causal_artifact, chronology_ref, record_computation
from ..data.active_history import ActiveCausalHistoryStateV1
from ..data.bars import BarIntervalV2
from ..instruments import InstrumentKeyV2, UniverseContractV2
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository
from .active_history import ActiveHistoryPageV1
from .full_strategy_surface import (
    MAX_BROAD_SOURCE_REFS,
    MAX_PREPARED_HISTORY_KEYS,
    MAX_S4_SEQUENCE_BOOKS,
    MAX_TOTAL_HISTORY_BARS,
)
from .ops_supervisor import OpsDecisionEventV1

LANE_BROAD_RESEARCH_V1 = "BROAD_RESEARCH"
SNAPSHOT_TYPE_V1 = "BroadPreparedHistorySnapshotV1"
HISTORY_SNAPSHOT_TYPE_V1 = "BroadPreparedHistoryStateV1"
DEFERRED_TYPE_V1 = "BroadResearchDeferredV1"
COMPLETION_TYPE_V1 = "BroadResearchCompletionV1"
MAX_HISTORY_INTERVALS_PER_KEY_V1 = len(BarIntervalV2)
MAX_COMPOSITION_REFS_V1 = MAX_BROAD_SOURCE_REFS
MAX_S4_FEATURE_REFS_V1 = MAX_S4_SEQUENCE_BOOKS


@dataclass(frozen=True)
class BroadResearchQueueResultV1:
    status: str
    snapshot_ref: str | None
    deferred_ref: str | None
    reason_code: str


@dataclass(frozen=True)
class LoadedBroadResearchSnapshotV1:
    snapshot_ref: str
    event_id: str
    event_ref: str
    information_cutoff_ns: int
    universe_ref: str
    composition_refs: tuple[str, ...]
    histories: Mapping[str, Mapping[BarIntervalV2, ActiveHistoryPageV1]]
    s4_feature_refs: Mapping[str, str]
    s4_features: Mapping[str, Any]
    live_books_available: bool = False
    live_book_reason: str = "LIVE_BOOKS_UNAVAILABLE_AFTER_RESTART"
    s4_missing_reason: str | None = None


def _ref(value: Any, *, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a SHA-256 ref")
    sha256_ref(value, field=name)
    return value


def _validate_causal_refs(repository: OpsRepository, refs: Sequence[str], *,
                          cutoff_ns: int, consumer_at_ns: int) -> None:
    cache: dict[str, Any] = {}
    validated: set[tuple[str, int, int, int]] = set()
    budget = [0]
    for ref in refs:
        if not causal_artifact(repository, ref, cutoff_ns=cutoff_ns,
                consumer_at_ns=consumer_at_ns, deadline_ns=consumer_at_ns,
                _cache=cache, _validated=validated, _budget=budget):
            raise ValueError("BROAD_RESEARCH_INPUT_REF_IDENTITY_OR_CHRONOLOGY_INVALID")


def _composition_identity(repository: OpsRepository, refs: Sequence[str], *, observed_at_ns: int
                          ) -> list[dict[str, Any]]:
    identities = []
    for ref in refs:
        entry = repository.get_artifact(ref)
        if entry is None or entry.available_at_ns > observed_at_ns:
            raise ValueError("BROAD_RESEARCH_COMPOSITION_REF_IDENTITY_OR_CHRONOLOGY_INVALID")
        identities.append({"ref": ref, "artifact_type": entry.artifact_type,
            "content_hash": entry.content_hash, "created_at_ns": entry.created_at_ns,
            "available_at_ns": entry.available_at_ns,
            "metadata_hash": sha256_json(json_value(entry.metadata))})
    return identities


def _validate_universe(repository: OpsRepository, ref: str, *, cutoff_ns: int,
                       consumer_at_ns: int) -> UniverseContractV2:
    entry = repository.get_artifact(ref)
    wire = json_value(entry.metadata.get("universe")) if entry is not None else None
    if (entry is None or entry.artifact_type != "UniverseContractV2"
            or entry.artifact_ref != ref or entry.content_hash != ref
            or entry.available_at_ns > consumer_at_ns or not isinstance(wire, Mapping)):
        raise ValueError("BROAD_RESEARCH_UNIVERSE_IDENTITY_OR_CHRONOLOGY_INVALID")
    universe = UniverseContractV2.from_dict(wire)
    if (universe.content_hash != ref or not causal_artifact(repository, ref,
            cutoff_ns=cutoff_ns, consumer_at_ns=consumer_at_ns, deadline_ns=consumer_at_ns)):
        raise ValueError("BROAD_RESEARCH_UNIVERSE_CAUSAL_RECEIPT_INVALID")
    return universe


def _defer_if_occupied(repository: OpsRepository, *, event: OpsDecisionEventV1,
                       universe_ref: str, observed_at_ns: int) -> BroadResearchQueueResultV1 | None:
    """Cheap capacity path, before walking any prepared history or causal DAG."""
    with repository.atomic_composition():
        pending = repository.due_work_items(LANE_BROAD_RESEARCH_V1,
            as_of_ns=2**63 - 1, limit=2)
        if not pending:
            return None
        same_identity = False
        if len(pending) == 1:
            existing = repository.get_artifact(pending[0].source_ref)
            existing_body = (json_value(existing.metadata.get("snapshot"))
                if existing is not None and existing.artifact_type == SNAPSHOT_TYPE_V1 else None)
            if isinstance(existing_body, Mapping):
                same_identity = (existing_body.get("event_id") == event.event_id
                    and existing_body.get("event_ref") == event.content_hash
                    and existing_body.get("information_cutoff_ns") == event.information_cutoff_ns
                    and existing_body.get("universe_ref") == universe_ref)
        if same_identity:
            return None
        deferred = {"version": DEFERRED_TYPE_V1, "event_id": event.event_id,
            "event_ref": event.content_hash, "cutoff_ns": event.information_cutoff_ns,
            "universe_ref": universe_ref, "reason": "RESEARCH_SLOT_CAPACITY",
            "authority": "ZERO", "capital_enabled": False, "assisted_enabled": False}
        deferred_ref = sha256_json(deferred)
        old = repository.get_artifact(deferred_ref)
        if old is None:
            repository.register_artifact(ArtifactIndexEntryV2(deferred_ref, DEFERRED_TYPE_V1,
                deferred_ref, observed_at_ns, observed_at_ns, {"deferred": deferred}))
        elif (old.artifact_type != DEFERRED_TYPE_V1 or old.content_hash != deferred_ref
                or sha256_json(old.metadata.get("deferred")) != deferred_ref
                or old.created_at_ns != old.available_at_ns):
            raise ValueError("BROAD_RESEARCH_DEFERRED_IDENTITY_CONFLICT")
        return BroadResearchQueueResultV1("DEFERRED", None, deferred_ref,
            "RESEARCH_SLOT_CAPACITY")


def _validated_s4_feature(repository: OpsRepository, *, key: InstrumentKeyV2, ref: str,
                          cutoff_ns: int, as_of_ns: int) -> Any:
    from ..chronology import VERSION as CHRONOLOGY_VERSION
    from ..data.microstructure import S4FeatureArtifactV2

    entry = repository.get_artifact(ref)
    feature_body = json_value(entry.metadata.get("feature")) if entry is not None else None
    receipt = repository.get_artifact(chronology_ref(ref))
    receipt_body = json_value(receipt.metadata.get("chronology")) if receipt is not None else None
    if (entry is None or entry.artifact_type != "S4FeatureArtifactV2"
            or entry.artifact_ref != ref or entry.content_hash != ref
            or entry.available_at_ns > as_of_ns or not isinstance(feature_body, Mapping)
            or receipt is None or receipt.artifact_type != CHRONOLOGY_VERSION
            or receipt.available_at_ns != entry.available_at_ns
            or not isinstance(receipt_body, Mapping)
            or receipt.content_hash != sha256_json(receipt_body)
            or not causal_artifact(repository, ref, cutoff_ns=cutoff_ns,
                consumer_at_ns=as_of_ns, deadline_ns=as_of_ns)):
        raise ValueError("BROAD_RESEARCH_S4_FEATURE_IDENTITY_OR_CHRONOLOGY_INVALID")
    feature = S4FeatureArtifactV2.from_dict(dict(feature_body))
    if feature.content_hash != ref or feature.instrument != key or feature.cutoff_ns != cutoff_ns:
        raise ValueError("BROAD_RESEARCH_S4_FEATURE_CUTOFF_OR_KEY_INVALID")
    return feature


def _serialize_histories(
    prepared_histories: Mapping[str, Mapping[BarIntervalV2, ActiveHistoryPageV1]], *,
    cutoff_ns: int,
) -> tuple[list[dict[str, Any]], int, tuple[ArtifactIndexEntryV2, ...]]:
    if not isinstance(prepared_histories, Mapping) or len(prepared_histories) > MAX_PREPARED_HISTORY_KEYS:
        raise ValueError("prepared history key population exceeds its fixed bound")
    rows: list[dict[str, Any]] = []
    total_bars = 0
    seen_keys: set[str] = set()
    history_artifacts: dict[str, ArtifactIndexEntryV2] = {}
    for key_json, pages in sorted(prepared_histories.items()):
        if not isinstance(key_json, str) or len(key_json) > 2048 or not isinstance(pages, Mapping):
            raise ValueError("prepared history key or interval map is invalid")
        key = InstrumentKeyV2.from_dict(json.loads(key_json))
        if key.to_canonical_json() != key_json or key_json in seen_keys:
            raise ValueError("prepared history key identity is not canonical and unique")
        seen_keys.add(key_json)
        if len(pages) > MAX_HISTORY_INTERVALS_PER_KEY_V1:
            raise ValueError("prepared history interval population exceeds its fixed bound")
        for raw_interval, page in sorted(pages.items(), key=lambda item: BarIntervalV2(item[0]).value):
            interval = BarIntervalV2(raw_interval)
            if not isinstance(page, ActiveHistoryPageV1) or type(page.ready) is not bool:
                raise ValueError("prepared history page is not typed")
            if not isinstance(page.reason_code, str) or not page.reason_code or len(page.reason_code) > 128:
                raise ValueError("prepared history page reason is invalid")
            state_wire = None
            state_snapshot_ref = None
            if page.state is not None:
                state = page.state
                if not isinstance(state, ActiveCausalHistoryStateV1):
                    raise ValueError("prepared history state is not typed")
                # Round-trip through the strict wire decoder before retaining it.
                state_wire = state.to_dict()
                parsed = ActiveCausalHistoryStateV1.from_dict(state_wire)
                if (parsed.content_hash != state.content_hash or parsed.key != key
                        or parsed.interval != interval or parsed.max_source_available_at_ns > cutoff_ns
                        or any(item.bar.raw.available_at_ns > cutoff_ns for item in parsed.tail)):
                    raise ValueError("prepared history state identity or cutoff chronology is invalid")
                if page.ready is False and not page.reason_code:
                    raise ValueError("unready history page requires an explicit reason")
                total_bars += len(parsed.tail)
                source_refs = tuple(sorted(set(
                    [item.observation_index_ref for item in parsed.tail]
                    + ([parsed.previous_state_ref] if parsed.previous_state_ref else []))))
                history_body = {"version": HISTORY_SNAPSHOT_TYPE_V1,
                    "state_ref": parsed.content_hash, "state": state_wire,
                    "authority": "ZERO"}
                state_snapshot_ref = sha256_json(history_body)
                history_artifacts[state_snapshot_ref] = ArtifactIndexEntryV2(
                    state_snapshot_ref, HISTORY_SNAPSHOT_TYPE_V1, state_snapshot_ref,
                    0, 0, {"history_snapshot": history_body, "input_refs": list(source_refs)})
            elif page.ready:
                raise ValueError("ready history page must retain its exact state")
            rows.append({"key_json": key_json, "interval": interval.value,
                         "ready": page.ready, "reason_code": page.reason_code,
                         "state_ref": state_snapshot_ref,
                         "state_hash": parsed.content_hash if page.state is not None else None})
    if total_bars > MAX_TOTAL_HISTORY_BARS:
        raise ValueError("prepared history bar population exceeds its fixed bound")
    return rows, total_bars, tuple(history_artifacts[ref] for ref in sorted(history_artifacts))


def enqueue_prepared_history_snapshot(
    repository: OpsRepository,
    *,
    event: OpsDecisionEventV1,
    universe_ref: str,
    composition_refs: Sequence[str],
    prepared_histories: Mapping[str, Mapping[BarIntervalV2, ActiveHistoryPageV1]],
    observed_at_ns: int,
    s4_feature_refs: Mapping[str, str] | None = None,
) -> BroadResearchQueueResultV1:
    """Atomically save an exact prepared snapshot and enqueue its one-slot work."""
    if repository.read_only:
        raise ValueError("broad research queue requires the sole repository writer")
    if not isinstance(event, OpsDecisionEventV1):
        raise ValueError("broad research queue requires the exact typed decision event")
    cutoff = timestamp(event.information_cutoff_ns, field="research cutoff")
    observed = timestamp(observed_at_ns, field="observed_at_ns")
    if observed < event.available_at_ns:
        raise ValueError("research snapshot observation cannot precede event availability")
    universe = _ref(universe_ref, name="universe_ref")
    early_deferred = _defer_if_occupied(repository, event=event, universe_ref=universe,
        observed_at_ns=observed)
    if early_deferred is not None:
        return early_deferred
    refs = tuple(sorted({_ref(item, name="composition_ref") for item in composition_refs}))
    if not refs or len(refs) > MAX_COMPOSITION_REFS_V1:
        raise ValueError("research composition refs must be nonempty and bounded")
    causal_refs = tuple(sorted({event.trigger_ref, *event.causal_input_refs}))
    if len(causal_refs) + len(refs) + 1 > MAX_COMPOSITION_REFS_V1:
        raise ValueError("research source ref population exceeds its fixed bound")
    _validate_causal_refs(repository, causal_refs, cutoff_ns=cutoff,
        consumer_at_ns=observed)
    _validate_universe(repository, universe, cutoff_ns=cutoff, consumer_at_ns=observed)
    composition_identity = _composition_identity(repository, refs, observed_at_ns=observed)
    history_rows, total_bars, history_artifacts = _serialize_histories(
        prepared_histories, cutoff_ns=cutoff)
    feature_refs = dict(s4_feature_refs or {})
    if len(feature_refs) > MAX_S4_FEATURE_REFS_V1:
        raise ValueError("prepared S4 feature population exceeds its fixed bound")
    prepared_keys = set(prepared_histories)
    if not set(feature_refs).issubset(prepared_keys):
        raise ValueError("prepared S4 features must belong to exact prepared universe keys")
    feature_refs = {key: _ref(ref, name="s4_feature_ref")
        for key, ref in sorted(feature_refs.items())}
    for key_json, feature_ref in feature_refs.items():
        key = InstrumentKeyV2.from_dict(json.loads(key_json))
        _validated_s4_feature(repository, key=key, ref=feature_ref, cutoff_ns=cutoff,
            as_of_ns=observed)
    body = {
        "version": SNAPSHOT_TYPE_V1,
        "event_id": event.event_id,
        "event_ref": event.content_hash,
        "event_wire": event.to_dict(),
        "information_cutoff_ns": cutoff,
        "event_available_at_ns": event.available_at_ns,
        "universe_ref": universe,
        "composition_refs": composition_identity,
        "s4_feature_refs": feature_refs,
        "s4_features_available": bool(feature_refs),
        "history_key_count": len(prepared_histories),
        "history_keys": sorted(prepared_histories),
        "history_state_count": sum(row["state_ref"] is not None for row in history_rows),
        "total_history_bars": total_bars,
        "histories": history_rows,
        "live_books_included": False,
        "capital_enabled": False,
        "assisted_enabled": False,
        "authority": "ZERO",
    }
    snapshot_ref = sha256_json(body)
    work_id = snapshot_ref
    with repository.atomic_composition():
        pending = repository.due_work_items(LANE_BROAD_RESEARCH_V1, as_of_ns=2**63 - 1, limit=2)
        same = next((item for item in pending if item.work_id == work_id), None)
        existing_snapshot = repository.get_artifact(snapshot_ref)
        if existing_snapshot is not None and same is None and not pending:
            if (existing_snapshot.artifact_type != SNAPSHOT_TYPE_V1
                    or existing_snapshot.content_hash != snapshot_ref
                    or sha256_json(existing_snapshot.metadata.get("snapshot")) != snapshot_ref):
                raise ValueError("BROAD_RESEARCH_SNAPSHOT_IMMUTABLE_IDENTITY_CONFLICT")
            return BroadResearchQueueResultV1("ALREADY_TERMINAL", snapshot_ref, None,
                "SNAPSHOT_ALREADY_RETIRED_OR_QUARANTINED")
        if same is None and pending:
            deferred = {"version": DEFERRED_TYPE_V1, "event_id": event.event_id,
                "event_ref": event.content_hash, "cutoff_ns": cutoff, "universe_ref": universe,
                "reason": "RESEARCH_SLOT_CAPACITY", "authority": "ZERO",
                "capital_enabled": False, "assisted_enabled": False}
            deferred_ref = sha256_json(deferred)
            old = repository.get_artifact(deferred_ref)
            if old is None:
                repository.register_artifact(ArtifactIndexEntryV2(deferred_ref, DEFERRED_TYPE_V1,
                    deferred_ref, observed, observed, {"deferred": deferred}))
            elif (old.artifact_type != DEFERRED_TYPE_V1 or old.content_hash != deferred_ref
                    or sha256_json(old.metadata.get("deferred")) != deferred_ref
                    or old.created_at_ns != old.available_at_ns):
                raise ValueError("BROAD_RESEARCH_DEFERRED_IDENTITY_CONFLICT")
            return BroadResearchQueueResultV1("DEFERRED", None, deferred_ref,
                "RESEARCH_SLOT_CAPACITY")
        for history_artifact in history_artifacts:
            existing_history = repository.get_artifact(history_artifact.artifact_ref)
            if existing_history is None:
                published = ArtifactIndexEntryV2(history_artifact.artifact_ref,
                    history_artifact.artifact_type, history_artifact.content_hash,
                    observed, observed, history_artifact.metadata)
                repository.register_artifact(published)
                input_refs = history_artifact.metadata["input_refs"]
                record_computation(repository, artifact_ref=history_artifact.artifact_ref,
                    information_cutoff_ns=cutoff, started_ns=observed, finished_ns=observed,
                    available_ns=observed, input_refs=tuple(input_refs),
                    deadline_ns=max(event.deadline_ns, observed))
            elif (existing_history.artifact_type != HISTORY_SNAPSHOT_TYPE_V1
                    or existing_history.content_hash != history_artifact.content_hash
                    or sha256_json(existing_history.metadata.get("history_snapshot"))
                    != history_artifact.artifact_ref):
                raise ValueError("BROAD_RESEARCH_HISTORY_SNAPSHOT_IMMUTABLE_IDENTITY_CONFLICT")
            elif not causal_artifact(repository, history_artifact.artifact_ref,
                    cutoff_ns=cutoff, consumer_at_ns=observed,
                    deadline_ns=max(event.deadline_ns, observed)):
                raise ValueError("BROAD_RESEARCH_HISTORY_SNAPSHOT_PRIOR_RECEIPT_INVALID")
        if existing_snapshot is None:
            published = ArtifactIndexEntryV2(snapshot_ref, SNAPSHOT_TYPE_V1, snapshot_ref,
                observed, observed, {"snapshot": body})
            repository.register_artifact(published)
        elif (existing_snapshot.artifact_type != SNAPSHOT_TYPE_V1
              or existing_snapshot.content_hash != snapshot_ref
              or sha256_json(existing_snapshot.metadata.get("snapshot")) != snapshot_ref):
            raise ValueError("BROAD_RESEARCH_SNAPSHOT_IMMUTABLE_IDENTITY_CONFLICT")
        queue_created_at = existing_snapshot.created_at_ns if existing_snapshot is not None else observed
        repository.enqueue_due_work(lane=LANE_BROAD_RESEARCH_V1, work_id=work_id,
            source_ref=snapshot_ref, created_at_ns=queue_created_at,
            due_at_ns=queue_created_at, payload={"snapshot_ref": snapshot_ref,
                "event_id": event.event_id, "cutoff_ns": cutoff})
    return BroadResearchQueueResultV1("QUEUED", snapshot_ref, None, "QUEUED")


def load_due_prepared_history_snapshot(
    repository: OpsRepository, *, as_of_ns: int, limit: int = 1,
) -> tuple[LoadedBroadResearchSnapshotV1, ...]:
    """Load only the stored cutoff snapshot; never consult mutable history heads."""
    cutoff_now = timestamp(as_of_ns, field="as_of_ns")
    items = repository.due_work_items(LANE_BROAD_RESEARCH_V1, as_of_ns=cutoff_now, limit=limit)
    loaded: list[LoadedBroadResearchSnapshotV1] = []
    for item in items:
        entry = repository.get_artifact(item.source_ref)
        if (entry is None or entry.artifact_ref != item.source_ref
                or entry.artifact_type != SNAPSHOT_TYPE_V1 or entry.content_hash != item.source_ref
                or entry.created_at_ns != item.created_at_ns or entry.available_at_ns > cutoff_now):
            raise ValueError("BROAD_RESEARCH_SNAPSHOT_IDENTITY_OR_CHRONOLOGY_INVALID")
        body = entry.metadata.get("snapshot")
        if not isinstance(body, Mapping):
            raise ValueError("BROAD_RESEARCH_SNAPSHOT_WIRE_INVALID")
        body = json_value(body)
        if body.get("version") != SNAPSHOT_TYPE_V1 or sha256_json(body) != item.source_ref:
            raise ValueError("BROAD_RESEARCH_SNAPSHOT_CHECKSUM_INVALID")
        if (body.get("authority") != "ZERO" or body.get("capital_enabled") is not False
                or body.get("assisted_enabled") is not False or body.get("live_books_included") is not False):
            raise ValueError("BROAD_RESEARCH_SNAPSHOT_AUTHORITY_INVALID")
        wire = body.get("event_wire")
        if not isinstance(wire, Mapping):
            raise ValueError("BROAD_RESEARCH_EVENT_WIRE_INVALID")
        event_fields = {key: value for key, value in wire.items() if key != "schema_version"}
        event = OpsDecisionEventV1(**{**event_fields, "causal_input_refs": tuple(wire["causal_input_refs"])})
        if (event.content_hash != body.get("event_ref") or event.event_id != body.get("event_id")
                or event.information_cutoff_ns != body.get("information_cutoff_ns")
                or event.available_at_ns != body.get("event_available_at_ns")):
            raise ValueError("BROAD_RESEARCH_EVENT_IDENTITY_OR_CHRONOLOGY_INVALID")
        rows = body.get("histories")
        if not isinstance(rows, list) or len(rows) > MAX_PREPARED_HISTORY_KEYS * MAX_HISTORY_INTERVALS_PER_KEY_V1:
            raise ValueError("BROAD_RESEARCH_HISTORY_ROWS_EXCEED_BOUND")
        raw_history_keys = body.get("history_keys")
        if (not isinstance(raw_history_keys, list) or len(raw_history_keys) > MAX_PREPARED_HISTORY_KEYS
                or len(raw_history_keys) != len(set(raw_history_keys))):
            raise ValueError("BROAD_RESEARCH_HISTORY_KEYS_INVALID")
        histories: dict[str, dict[BarIntervalV2, ActiveHistoryPageV1]] = {}
        for key_json in raw_history_keys:
            if not isinstance(key_json, str):
                raise ValueError("BROAD_RESEARCH_HISTORY_KEY_INVALID")
            key = InstrumentKeyV2.from_dict(json.loads(key_json))
            if key.to_canonical_json() != key_json:
                raise ValueError("BROAD_RESEARCH_HISTORY_KEY_INVALID")
            histories[key_json] = {}
        total_bars = 0
        state_count = 0
        for row in rows:
            if not isinstance(row, Mapping) or set(row) != {
                    "key_json", "interval", "ready", "reason_code", "state_ref", "state_hash"}:
                raise ValueError("BROAD_RESEARCH_HISTORY_ROW_INVALID")
            key_json = row["key_json"]
            key = InstrumentKeyV2.from_dict(json.loads(key_json))
            if key.to_canonical_json() != key_json:
                raise ValueError("BROAD_RESEARCH_HISTORY_KEY_INVALID")
            interval = BarIntervalV2(row["interval"])
            if key_json not in histories:
                raise ValueError("BROAD_RESEARCH_HISTORY_ROW_KEY_UNDECLARED")
            pages = histories[key_json]
            if interval in pages:
                raise ValueError("BROAD_RESEARCH_HISTORY_INTERVAL_DUPLICATE")
            state = None
            if row["state_ref"] is not None:
                state_ref = _ref(row["state_ref"], name="history_snapshot_ref")
                history_entry = repository.get_artifact(state_ref)
                if (history_entry is None or history_entry.artifact_type != HISTORY_SNAPSHOT_TYPE_V1
                    or history_entry.content_hash != state_ref
                    or history_entry.available_at_ns > cutoff_now
                    or not causal_artifact(repository, state_ref,
                        cutoff_ns=event.information_cutoff_ns, consumer_at_ns=cutoff_now,
                        deadline_ns=cutoff_now)):
                    raise ValueError("BROAD_RESEARCH_HISTORY_STATE_INVALID")
                history_body = json_value(history_entry.metadata.get("history_snapshot"))
                if (not isinstance(history_body, Mapping)
                        or history_body.get("version") != HISTORY_SNAPSHOT_TYPE_V1
                        or history_body.get("authority") != "ZERO"
                        or sha256_json(history_body) != state_ref
                        or history_body.get("state_ref") != row["state_hash"]):
                    raise ValueError("BROAD_RESEARCH_HISTORY_STATE_CHECKSUM_INVALID")
                state_body = history_body.get("state")
                if not isinstance(state_body, Mapping):
                    raise ValueError("BROAD_RESEARCH_HISTORY_STATE_WIRE_INVALID")
                state = ActiveCausalHistoryStateV1.from_dict(state_body)
                if (state.key != key or state.interval != interval
                        or state.content_hash != row["state_hash"]
                        or state.max_source_available_at_ns > event.information_cutoff_ns
                        or any(tail.bar.raw.available_at_ns > event.information_cutoff_ns for tail in state.tail)):
                    raise ValueError("BROAD_RESEARCH_HISTORY_STATE_IDENTITY_OR_CUTOFF_INVALID")
                total_bars += len(state.tail)
                state_count += 1
            elif row["state_hash"] is not None:
                raise ValueError("BROAD_RESEARCH_HISTORY_HASH_WITHOUT_SNAPSHOT")
            if row["ready"] is True and state is None:
                raise ValueError("BROAD_RESEARCH_READY_HISTORY_STATE_MISSING")
            if (type(row["ready"]) is not bool or not isinstance(row["reason_code"], str)
                    or not row["reason_code"] or len(row["reason_code"]) > 128):
                raise ValueError("BROAD_RESEARCH_HISTORY_STATUS_INVALID")
            pages[interval] = ActiveHistoryPageV1(state, row["ready"], row["reason_code"])
        if (len(histories) != body.get("history_key_count")
                or state_count != body.get("history_state_count")
                or total_bars != body.get("total_history_bars")
                or total_bars > MAX_TOTAL_HISTORY_BARS):
            raise ValueError("BROAD_RESEARCH_HISTORY_TOTALS_INVALID")
        composition_identity = body.get("composition_refs", ())
        universe = _ref(body.get("universe_ref"), name="universe_ref")
        if (not isinstance(composition_identity, list) or not composition_identity
                or len(composition_identity) > MAX_COMPOSITION_REFS_V1):
            raise ValueError("BROAD_RESEARCH_COMPOSITION_REFS_INVALID")
        composition_refs = []
        for identity in composition_identity:
            if not isinstance(identity, Mapping) or set(identity) != {
                    "ref", "artifact_type", "content_hash", "created_at_ns", "available_at_ns",
                    "metadata_hash"}:
                raise ValueError("BROAD_RESEARCH_COMPOSITION_IDENTITY_INVALID")
            ref = _ref(identity["ref"], name="composition_ref")
            composition_entry = repository.get_artifact(ref)
            if (composition_entry is None or composition_entry.artifact_type != identity["artifact_type"]
                    or composition_entry.content_hash != identity["content_hash"]
                    or composition_entry.created_at_ns != identity["created_at_ns"]
                    or composition_entry.available_at_ns != identity["available_at_ns"]
                    or sha256_json(json_value(composition_entry.metadata)) != identity["metadata_hash"]
                    or composition_entry.available_at_ns > cutoff_now):
                raise ValueError("BROAD_RESEARCH_COMPOSITION_IDENTITY_OR_CHRONOLOGY_INVALID")
            composition_refs.append(ref)
        if composition_refs != sorted(set(composition_refs)):
            raise ValueError("BROAD_RESEARCH_COMPOSITION_REFS_INVALID")
        event_refs = tuple(sorted({event.trigger_ref, *event.causal_input_refs}))
        _validate_causal_refs(repository, event_refs, cutoff_ns=event.information_cutoff_ns,
            consumer_at_ns=cutoff_now)
        _validate_universe(repository, universe, cutoff_ns=event.information_cutoff_ns,
            consumer_at_ns=cutoff_now)
        raw_feature_refs = body.get("s4_feature_refs", {})
        if not isinstance(raw_feature_refs, Mapping) or len(raw_feature_refs) > MAX_S4_FEATURE_REFS_V1:
            raise ValueError("BROAD_RESEARCH_S4_FEATURE_REFS_INVALID")
        if body.get("s4_features_available") is not bool(raw_feature_refs):
            raise ValueError("BROAD_RESEARCH_S4_FEATURE_AVAILABILITY_FLAG_INVALID")
        feature_refs: dict[str, str] = {}
        features: dict[str, Any] = {}
        if raw_feature_refs:
            for key_json, raw_ref in raw_feature_refs.items():
                key = InstrumentKeyV2.from_dict(json.loads(key_json))
                if key.to_canonical_json() != key_json or key_json not in histories:
                    raise ValueError("BROAD_RESEARCH_S4_FEATURE_KEY_INVALID")
                feature_ref = _ref(raw_ref, name="s4_feature_ref")
                feature = _validated_s4_feature(repository, key=key, ref=feature_ref,
                    cutoff_ns=event.information_cutoff_ns, as_of_ns=cutoff_now)
                feature_refs[key_json] = feature_ref
                features[key_json] = feature
        loaded.append(LoadedBroadResearchSnapshotV1(item.source_ref, event.event_id,
            event.content_hash, event.information_cutoff_ns, universe,
            tuple(composition_refs), histories,
            feature_refs, features, s4_missing_reason=(None if features else
                "S4_NOT_ESTIMABLE_NO_CUTOFF_FEATURE_SNAPSHOT")))
    return tuple(loaded)


def complete_prepared_history_snapshot(
    repository: OpsRepository,
    *,
    snapshot_ref: str,
    completed_at_ns: int,
    result_ref: str | None = None,
) -> str:
    """Atomically publish completion evidence and retire exactly its due item."""
    ref = _ref(snapshot_ref, name="snapshot_ref")
    completed = timestamp(completed_at_ns, field="completed_at_ns")
    result = _ref(result_ref, name="result_ref") if result_ref is not None else None
    if result is not None:
        result_entry = repository.get_artifact(result)
        if result_entry is None or result_entry.available_at_ns > completed:
            raise ValueError("BROAD_RESEARCH_COMPLETION_RESULT_IDENTITY_OR_CHRONOLOGY_INVALID")
    snapshot = repository.get_artifact(ref)
    if snapshot is None or snapshot.artifact_type != SNAPSHOT_TYPE_V1 or snapshot.content_hash != ref:
        raise ValueError("BROAD_RESEARCH_COMPLETION_SOURCE_INVALID")
    identity = {"version": COMPLETION_TYPE_V1, "snapshot_ref": ref,
        "capital_enabled": False, "assisted_enabled": False, "authority": "ZERO"}
    completion_ref = sha256_json(identity)
    body = {**identity, "result_ref": result, "completed_at_ns": completed}
    with repository.atomic_composition():
        existing = repository.get_artifact(completion_ref)
        if existing is None:
            repository.register_artifact(ArtifactIndexEntryV2(completion_ref, COMPLETION_TYPE_V1,
                sha256_json(body), completed, completed, {"completion": body}))
        else:
            existing_body = json_value(existing.metadata.get("completion"))
            if (existing.artifact_type != COMPLETION_TYPE_V1
                    or not isinstance(existing_body, Mapping)
                    or existing.content_hash != sha256_json(existing_body)
                    or {key: existing_body.get(key) for key in identity} != identity
                    or existing_body.get("result_ref") != result
                    or type(existing_body.get("completed_at_ns")) is not int
                    or existing.created_at_ns != existing_body.get("completed_at_ns")
                    or existing.available_at_ns != existing_body.get("completed_at_ns")):
                raise ValueError("BROAD_RESEARCH_COMPLETION_IDENTITY_CONFLICT")
            timestamp(existing_body["completed_at_ns"], field="completed_at_ns")
        repository.retire_due_work(LANE_BROAD_RESEARCH_V1, ref, reason_code="RESEARCH_COMPLETED")
    return completion_ref
