"""One bounded exact-prefix maintenance page, persisted by the existing ops writer.

The mutable head is a rebuildable cache. Its immutable certificates bind exact
raw deltas, the original recursive seeds and the full ordered retained tail.
Previous certificates must already be published by the next market cutoff:
this prevents active validation from walking an ever-longer parent chain.
"""
from __future__ import annotations

import json
import time
from collections import OrderedDict
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from weakref import WeakKeyDictionary

from .._serialization import sha256_json
from ..chronology import record_computation, sample
from ..data.active_history import ActiveCausalHistoryStateV1, advance
from ..data.bars import BarIntervalV2
from ..data.history import IndexedCausalBarV2, reconstruct_native_bars_from_index_page
from ..instruments import InstrumentKeyV2
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository

# These objects are disposable acceleration only. Exact durable JSON and its
# checksum still identify every entry, and recovery validates the wire again.
MAX_VERIFIED_HISTORY_HEADS_V1 = 8
HISTORY_DECODE_SERVICE_ITEMS_V1 = 32


@dataclass(frozen=True)
class _VerifiedHead:
    state_json: str
    state_ref: str
    state: ActiveCausalHistoryStateV1


_VERIFIED_HEADS: WeakKeyDictionary[OpsRepository, OrderedDict[tuple[str, str], _VerifiedHead]] = WeakKeyDictionary()


def _remember_head(repository: OpsRepository, head: Any, state: ActiveCausalHistoryStateV1) -> None:
    if head is None or head["state_json"] is None or head["state_ref"] != state.content_hash:
        raise ValueError("ACTIVE_HISTORY_HEAD_IDENTITY_CONFLICT")
    identity = (state.key.to_canonical_json(), state.interval.value)
    cache = _VERIFIED_HEADS.setdefault(repository, OrderedDict())
    cache[identity] = _VerifiedHead(head["state_json"], head["state_ref"], state)
    cache.move_to_end(identity)
    while len(cache) > MAX_VERIFIED_HISTORY_HEADS_V1:
        cache.popitem(last=False)


class _ServicedWireTail(list[Any]):
    """Keep the strict decoder unchanged while yielding between source items."""

    def __init__(self, items: list[Any], service: Callable[[], None]) -> None:
        super().__init__(items)
        self._service = service

    def __iter__(self) -> Iterator[Any]:
        for position, item in enumerate(super().__iter__()):
            if position % HISTORY_DECODE_SERVICE_ITEMS_V1 == 0:
                self._service()
            yield item
        self._service()


def _decode_head(repository: OpsRepository, head: Any, *, key: InstrumentKeyV2,
                 interval: BarIntervalV2, service: Callable[[], None] | None) -> ActiveCausalHistoryStateV1 | None:
    if head is None or head["state_json"] is None:
        return None
    identity = (key.to_canonical_json(), interval.value)
    cache = _VERIFIED_HEADS.setdefault(repository, OrderedDict())
    previous = cache.get(identity)
    if (previous is not None and previous.state_ref == head["state_ref"]
            and previous.state_json == head["state_json"]):
        cache.move_to_end(identity)
        return previous.state
    wire = json.loads(head["state_json"])
    if service is not None and isinstance(wire, dict) and isinstance(wire.get("tail"), list):
        # The callback is supplied only at a safe writer boundary, never while
        # the caller has an outer composition transaction open. It does not
        # change this captured immutable wire or any of its strict validators.
        wire["tail"] = _ServicedWireTail(wire["tail"], service)
    state = ActiveCausalHistoryStateV1.from_dict(wire)
    if state.key != key or state.interval != interval or state.content_hash != head["state_ref"]:
        raise ValueError("ACTIVE_HISTORY_HEAD_IDENTITY_CONFLICT")
    _remember_head(repository, head, state)
    return state


@dataclass(frozen=True)
class ActiveHistoryPageV1:
    state: ActiveCausalHistoryStateV1 | None
    ready: bool
    reason_code: str

    @property
    def bars(self) -> tuple[IndexedCausalBarV2, ...]:
        if not self.ready or self.state is None:
            return ()
        return tuple(IndexedCausalBarV2(item.bar, item.observation_index_ref) for item in self.state.tail)


def maintain_history(repository: OpsRepository, archive_root: Path, *, key: InstrumentKeyV2,
                     interval: BarIntervalV2, cutoff_ns: int,
                     clock_ns: Callable[[], int], deadline_ns: int,
                     service: Callable[[], None] | None = None) -> ActiveHistoryPageV1:
    """Advance at most 128 source rows once; never sweep or silently reseed history."""
    head = repository.active_history_head(key, interval.value)
    state = _decode_head(repository, head, key=key, interval=interval, service=service)
    if state is not None and (head is None or state.key != key or state.interval != interval
                              or state.content_hash != head["state_ref"]):
        raise ValueError("ACTIVE_HISTORY_HEAD_IDENTITY_CONFLICT")
    if state is not None:
        certificate = repository.get_artifact(state.content_hash)
        if (certificate is None or certificate.artifact_type != "ActiveCausalHistoryStateV1"
                or certificate.artifact_ref != certificate.content_hash
                or sha256_json(certificate.metadata.get("history")) != state.content_hash
                or head is None or certificate.available_at_ns != head["available_at_ns"]):
            raise ValueError("ACTIVE_HISTORY_CHECKPOINT_IDENTITY_CONFLICT")
    cursor = int(head["scan_close_at_ns"]) if head is not None else 0
    rebuild = (head is not None and head["dirty_available_at_ns"] is not None
               and head["dirty_available_at_ns"] <= cutoff_ns)
    if rebuild:
        state, cursor = None, 0
    reason = "EXACT_PREFIX_AVAILABLE"
    ready = False
    started = sample(clock_ns, floor_ns=cutoff_ns)
    # A cache built from later source revisions cannot answer an earlier cutoff.
    if state is not None and state.max_source_available_at_ns > cutoff_ns:
        reason = "HISTORICAL_CUTOFF_REQUIRES_EXACT_REBUILD"
    elif head is not None and not rebuild and head["available_at_ns"] > cutoff_ns:
        # At most one advancement at a given cutoff. Same-cutoff consumers may
        # reuse its causal receipt, but may not extend it into a parent walk.
        try:
            rows, _cursor, more = repository.active_history_source_page(
                key, interval.value, after_close_at_ns=cursor, cutoff_ns=cutoff_ns)
            ready = (state is not None and not rows and not more and head["cutoff_ns"] == cutoff_ns)
            reason = "EXACT_PREFIX_AVAILABLE" if ready else "PRIOR_CHECKPOINT_PUBLICATION_PENDING"
        except ValueError as exc:
            reason = str(exc) if str(exc).startswith("ACTIVE_HISTORY_") else "EXACT_PREFIX_SOURCE_UNSUPPORTED"
    else:
        try:
            if service is not None:
                service()
            rows, new_cursor, more = repository.active_history_source_page(
                key, interval.value, after_close_at_ns=cursor, cutoff_ns=cutoff_ns)
            indexed = reconstruct_native_bars_from_index_page(repository, archive_root,
                key=key, interval=interval, index_entries=rows, max_origins=128, service=service)
            if service is not None:
                service()
            selected: dict[int, IndexedCausalBarV2] = {}
            for item in indexed:
                previous = selected.get(item.bar.close_at_ns)
                if previous is None or (item.bar.raw.available_at_ns, item.bar.raw.record_id) > (
                        previous.bar.raw.available_at_ns, previous.bar.raw.record_id):
                    selected[item.bar.close_at_ns] = item
            new_bars = tuple(selected[close] for close in sorted(selected))
            next_state = advance(state, new_bars, key=key, interval=interval) if new_bars else state
            if service is not None:
                service()
            available = int(head["available_at_ns"]) if head is not None and not rebuild else started
            with repository.atomic_composition():
                if next_state is not None and next_state is not state:
                    repository.register_artifacts(tuple(ArtifactIndexEntryV2(
                        item.bar.content_hash, "CausalBarV2", item.bar.content_hash,
                        item.bar.raw.available_at_ns, item.bar.raw.available_at_ns,
                        {"bar": item.bar.to_dict(), "source_observation_ref": item.observation_index_ref})
                        for item in new_bars))
                    existing = repository.get_artifact(next_state.content_hash)
                    if existing is None:
                        finished = sample(clock_ns, floor_ns=started)
                        available = sample(clock_ns, floor_ns=finished)
                        body = next_state.to_dict()
                        body.pop("tail")
                        body.pop("content_hash")
                        repository.register_artifact(ArtifactIndexEntryV2(
                            next_state.content_hash, "ActiveCausalHistoryStateV1", next_state.content_hash,
                            finished, available, {"history": body, "input_refs": next_state.input_refs}))
                        record_computation(repository, artifact_ref=next_state.content_hash,
                            information_cutoff_ns=cutoff_ns, started_ns=started, finished_ns=finished,
                            available_ns=available, input_refs=next_state.input_refs, deadline_ns=deadline_ns)
                    else:
                        if (existing.artifact_type != "ActiveCausalHistoryStateV1"
                                or existing.content_hash != next_state.content_hash
                                or sha256_json(existing.metadata["history"]) != next_state.content_hash):
                            raise ValueError("ACTIVE_HISTORY_CHECKPOINT_IDENTITY_CONFLICT")
                        available = existing.available_at_ns
                if (head is None or rebuild or next_state is not state or new_cursor != cursor):
                    repository.save_active_history_head(key, interval.value,
                        state=next_state.to_dict() if next_state is not None else None,
                        state_ref=next_state.content_hash if next_state is not None else None,
                        scan_close_at_ns=new_cursor, cutoff_ns=cutoff_ns, available_at_ns=available)
            if next_state is not None:
                # This state was constructed and validated on the same writer.
                # Read back the exact persisted JSON before retaining it. If an
                # outer transaction later rolls back, its old JSON will miss.
                _remember_head(repository, repository.active_history_head(key, interval.value), next_state)
            state, ready = next_state, not more and next_state is not None
            reason = ("EXACT_PREFIX_AVAILABLE" if ready else "EXACT_PREFIX_SOURCE_MISSING"
                      if next_state is None and not more else
                      "SOURCE_REVISION_REBUILD_PENDING" if rebuild else "HISTORY_BOOTSTRAP_BACKLOG")
        except (ValueError, OSError) as exc:
            # Typed failures are retained below; never replace a failed prefix
            # with a truncated latest-history seed.
            reason = (str(exc) if str(exc).startswith("ACTIVE_HISTORY_")
                      else "EXACT_PREFIX_SOURCE_UNSUPPORTED")
    pressure = {"version": "OpsActiveWorkPressureV1", "lane": "ACTIVE_HISTORY",
        "instrument_key_json": key.to_canonical_json(), "interval": interval.value,
        "information_cutoff_ns": cutoff_ns, "ready": ready, "reason_code": reason,
        "processed_bar_count": state.total_count if state is not None else 0,
        "last_close_at_ns": state.last_close_at_ns if state is not None else None,
        "max_rows_per_cycle": 128, "authority": "ZERO"}
    ref = sha256_json(pressure)
    if repository.get_artifact(ref) is None:
        at = sample(clock_ns, floor_ns=started)
        repository.register_artifact(ArtifactIndexEntryV2(
            ref, "OpsActiveWorkPressureV1", ref, at, at, {"pressure": pressure}))
    return ActiveHistoryPageV1(state, ready, reason)


class ActiveHistoryMaintenanceV1:
    """Warm one exact key/frame prefix per turn, independently of scan cadence.

    The installed ops writer calls this between decision cycles. A 500 ms
    cadence and a 128-product ceiling bound discovery and each advance; the
    durable heads, rather than this disposable round-robin cursor, own progress.
    """

    def __init__(self, archive_root: Path, *, clock_ns: Callable[[], int] = time.time_ns) -> None:
        self.archive_root = archive_root
        self.clock_ns = clock_ns
        self._cursor = 0
        self._next_cutoff_ns = 0

    def run_cycle(self, repository: OpsRepository, *, cutoff_ns: int,
                  service: Callable[[], None] | None = None) -> ActiveHistoryPageV1 | None:
        if cutoff_ns < self._next_cutoff_ns:
            return None
        self._next_cutoff_ns = cutoff_ns + 500_000_000
        # Share the production inventory bound and its persisted overflow proof.
        from .production import _causal_products

        products = _causal_products(repository, cutoff_ns=cutoff_ns)
        frames = (BarIntervalV2.M15, BarIntervalV2.H1, BarIntervalV2.H4, BarIntervalV2.M1)
        lanes = tuple((product.key, frame) for product in products for frame in frames)
        if not lanes:
            return None
        key, interval = lanes[self._cursor % len(lanes)]
        self._cursor += 1
        return maintain_history(repository, self.archive_root, key=key, interval=interval,
            cutoff_ns=cutoff_ns, clock_ns=self.clock_ns, deadline_ns=cutoff_ns + 250_000_000,
            service=service)
