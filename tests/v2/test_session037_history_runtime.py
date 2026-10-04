"""Persisted exact-prefix maintenance remains bounded, causal and restartable."""

from dataclasses import replace

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.chronology import causal_artifact, chronology_ref
from atlas.v2.data.active_history import advance
from atlas.v2.data.bars import BarIntervalV2
from atlas.v2.data.history import IndexedCausalBarV2
from atlas.v2.data.raw import AvailabilityClassV2, RawObservationV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime import active_history as runtime

from .test_session014_core import KEY, bar


def _indexed(index, interval=BarIntervalV2.M15, *, available=None, original=None):
    item = bar(index, interval=interval, close=str(100 + index / 10 + (1 if original is not None else 0)))
    if available is not None or original is not None:
        at = item.close_at_ns if available is None else available
        raw = RawObservationV2.build(instrument_revision=KEY.contract_revision,
            source_id=item.raw.source_id, event_type=item.raw.event_type,
            event_at_ns=item.close_at_ns, received_at_ns=at, ingested_at_ns=at,
            available_at_ns=at, translation_version="history-runtime-fixture-v1",
            revision_of=original.bar.raw.record_id if original is not None else None,
            payload={"index": index, "revision": original is not None})
        item = replace(item, raw=raw)
    ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": item.raw.record_id})
    return IndexedCausalBarV2(item, ref)


def _register(repo, indexed):
    repo.register_artifacts(tuple(ArtifactIndexEntryV2(item.observation_index_ref,
        "PublicObservationIndexV2", item.bar.raw.content_hash,
        item.bar.raw.received_at_ns, item.bar.raw.available_at_ns,
        {"instrument_key_json": KEY.to_canonical_json(), "instrument_revision": KEY.contract_revision,
         "event_type": item.bar.raw.event_type, "event_at_ns": item.bar.close_at_ns,
         "availability_class": AvailabilityClassV2.ACTUAL_SYSTEM.value,
         "bar_content_hash": item.bar.content_hash, "record_id": item.bar.raw.record_id,
         "source_id": item.bar.raw.source_id}) for item in indexed))


@pytest.fixture
def archive(monkeypatch):
    indexed = {}
    calls = []

    def reconstruct(repository, _root, *, key, interval, index_entries, max_origins, service=None):
        assert key == KEY and max_origins == 128
        assert len(index_entries) <= 128
        calls.append(tuple(entry.artifact_ref for entry in index_entries))
        result = tuple(indexed[entry.artifact_ref] for entry in index_entries)
        assert all(item.bar.interval == interval for item in result)
        assert all(repository.get_artifact(item.observation_index_ref) is not None for item in result)
        if service is not None:
            service()
        return result

    monkeypatch.setattr(runtime, "reconstruct_native_bars_from_index_page", reconstruct)
    return indexed, calls


def _maintain(repo, root, cutoff, interval=BarIntervalV2.M15):
    return runtime.maintain_history(repo, root, key=KEY, interval=interval, cutoff_ns=cutoff,
        clock_ns=lambda: cutoff + 10, deadline_ns=cutoff + 100)


def test_bounded_bootstrap_same_cutoff_publication_and_restart_preserve_exact_seeds(tmp_path, archive):
    sources = tuple(_indexed(i) for i in range(150))
    indexed, calls = archive
    indexed.update((item.observation_index_ref, item) for item in sources)
    cutoff = sources[-1].bar.close_at_ns
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        _register(repo, sources)
        first = _maintain(repo, tmp_path, cutoff)
        assert not first.ready and first.reason_code == "HISTORY_BOOTSTRAP_BACKLOG"
        assert first.state.total_count == 128 and len(calls[0]) == 128
        waiting = _maintain(repo, tmp_path, cutoff)
        assert not waiting.ready and waiting.reason_code == "PRIOR_CHECKPOINT_PUBLICATION_PENDING"
        assert waiting.state == first.state and len(calls) == 1
        first_ref = first.state.content_hash
        assert not causal_artifact(repo, first_ref, cutoff_ns=cutoff,
            consumer_at_ns=cutoff, deadline_ns=cutoff + 100)
        assert causal_artifact(repo, first_ref, cutoff_ns=cutoff,
            consumer_at_ns=cutoff + 10, deadline_ns=cutoff + 100)
        receipt = repo.get_artifact(chronology_ref(first_ref))
        assert receipt.metadata["chronology"]["information_cutoff_ns"] == cutoff
    with OpsRepository(path) as repo:
        complete = _maintain(repo, tmp_path, cutoff + 20)
        assert complete.ready and complete.state.total_count == 150 and len(calls[-1]) == 22
        expected = advance(None, sources[:128], key=KEY, interval=BarIntervalV2.M15)
        expected = advance(expected, sources[128:], key=KEY, interval=BarIntervalV2.M15)
        assert complete.state == expected
        assert complete.state.previous_state_ref == first_ref
        head = repo.active_history_head(KEY, BarIntervalV2.M15.value)
        assert head["state_ref"] == expected.content_hash
        checkpoint = repo.get_artifact(expected.content_hash)
        assert sha256_json(checkpoint.metadata["history"]) == expected.content_hash
        repeated = _maintain(repo, tmp_path, cutoff + 20)
        assert repeated.ready and repeated.state == complete.state and len(calls) == 2


def test_late_nonappend_revision_rebuilds_from_original_seed_in_bounded_pages(tmp_path, archive):
    sources = tuple(_indexed(i) for i in range(140))
    indexed, calls = archive
    indexed.update((item.observation_index_ref, item) for item in sources)
    cutoff = sources[-1].bar.close_at_ns
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _register(repo, sources)
        _maintain(repo, tmp_path, cutoff)
        original = _maintain(repo, tmp_path, cutoff + 20)
        revision = _indexed(3, available=cutoff + 40, original=sources[3])
        indexed[revision.observation_index_ref] = revision
        _register(repo, (revision,))
        rebuilt = _maintain(repo, tmp_path, cutoff + 50)
        assert not rebuilt.ready and rebuilt.reason_code == "SOURCE_REVISION_REBUILD_PENDING"
        assert rebuilt.state.total_count == 127 and len(calls[-1]) == 128
        assert rebuilt.state.previous_state_ref is None
        assert rebuilt.state.content_hash != original.state.content_hash
        complete = _maintain(repo, tmp_path, cutoff + 70)
        assert complete.ready and complete.state.total_count == 140
        selected = sources[:3] + (revision,) + sources[4:]
        expected = advance(None, selected[:127], key=KEY, interval=BarIntervalV2.M15)
        expected = advance(expected, selected[127:], key=KEY, interval=BarIntervalV2.M15)
        assert complete.state == expected
        assert len(calls[-1]) == 13


def test_historical_cutoff_never_reuses_a_newer_prefix(tmp_path, archive):
    sources = tuple(_indexed(i) for i in range(30))
    archive[0].update((item.observation_index_ref, item) for item in sources)
    cutoff = sources[-1].bar.close_at_ns
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _register(repo, sources)
        complete = _maintain(repo, tmp_path, cutoff)
        head_before = repo.active_history_head(KEY, BarIntervalV2.M15.value)
        refused = _maintain(repo, tmp_path, cutoff - BarIntervalV2.M15.duration_ns)
        assert not refused.ready and refused.bars == ()
        assert refused.reason_code == "HISTORICAL_CUTOFF_REQUIRES_EXACT_REBUILD"
        assert refused.state == complete.state
        assert repo.active_history_head(KEY, BarIntervalV2.M15.value) == head_before


def test_future_revision_is_pending_and_cannot_be_dropped_from_origin_inventory(tmp_path, archive):
    source = _indexed(0)
    cutoff = source.bar.close_at_ns + 100
    future = _indexed(0, available=cutoff + 100, original=source)
    archive[0].update((item.observation_index_ref, item) for item in (source, future))
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _register(repo, (source, future))
        pending = _maintain(repo, tmp_path, cutoff)
        assert not pending.ready and pending.state is None
        assert pending.reason_code == "ACTIVE_HISTORY_FUTURE_REVISION_PENDING"
        assert archive[1] == []
        ready = _maintain(repo, tmp_path, cutoff + 100)
        assert ready.ready and ready.state.total_count == 1
        assert ready.state.tail[-1].bar.raw.record_id == future.bar.raw.record_id


def test_revision_overflow_refuses_the_whole_page_and_retains_pressure(tmp_path, archive):
    sources = [_indexed(0)]
    for i in range(128):
        sources.append(_indexed(0, available=sources[0].bar.close_at_ns + i + 1, original=sources[-1]))
    archive[0].update((item.observation_index_ref, item) for item in sources)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _register(repo, sources)
        result = _maintain(repo, tmp_path, sources[-1].bar.raw.available_at_ns)
        assert not result.ready and result.state is None
        assert result.reason_code == "ACTIVE_HISTORY_REVISION_PAGE_OVERFLOW"
        assert archive[1] == []
        pressure = repo.artifact_entries("OpsActiveWorkPressureV1")
        assert len(pressure) == 1
        assert pressure[0].metadata["pressure"]["reason_code"] == result.reason_code


def test_same_cutoff_revision_failure_remains_visible_without_raising(tmp_path, archive):
    source = _indexed(0)
    archive[0][source.observation_index_ref] = source
    cutoff = BarIntervalV2.M15.duration_ns * 3
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _register(repo, (source,))
        _maintain(repo, tmp_path, cutoff)
        next_origin = _indexed(1, available=cutoff + 100)
        archive[0][next_origin.observation_index_ref] = next_origin
        _register(repo, (next_origin,))
        # Include the next close in the same-cutoff seek while publication of
        # this newly appended source remains later than the information cutoff.
        result = _maintain(repo, tmp_path, cutoff)
        assert not result.ready and result.reason_code == "ACTIVE_HISTORY_FUTURE_REVISION_PENDING"


@pytest.mark.parametrize("interval", (BarIntervalV2.M1, BarIntervalV2.H1, BarIntervalV2.H4))
def test_missing_interval_is_an_empty_bounded_result(tmp_path, archive, interval):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        result = _maintain(repo, tmp_path, 1, interval)
        assert result.state is None and result.bars == ()
        assert archive[1] == [()]


def test_installed_maintenance_warms_each_frame_without_waiting_for_a_scan_and_resumes(tmp_path, archive):
    from .test_session017_risk import risk_case

    frames = (BarIntervalV2.M15, BarIntervalV2.H1, BarIntervalV2.H4, BarIntervalV2.M1)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        sources = tuple(_indexed(0, interval) for interval in frames)
        archive[0].update((item.observation_index_ref, item) for item in sources)
        _register(repo, sources)
        cutoff = max(case.product.available_at_ns, *(item.bar.raw.available_at_ns for item in sources))
        maintenance = runtime.ActiveHistoryMaintenanceV1(tmp_path, clock_ns=lambda: cutoff + 10)
        first = maintenance.run_cycle(repo, cutoff_ns=cutoff)
        assert first.ready and first.state.interval == frames[0]
        assert maintenance.run_cycle(repo, cutoff_ns=cutoff + 1) is None
        for frame in frames[1:]:
            cutoff += 500_000_000
            result = maintenance.run_cycle(repo, cutoff_ns=cutoff)
            assert result.ready and result.state.interval == frame and result.state.total_count == 1
        heads = tuple(repo.active_history_head(KEY, frame.value) for frame in frames)
    cutoff += 500_000_000
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        restarted = runtime.ActiveHistoryMaintenanceV1(tmp_path, clock_ns=lambda: cutoff + 10)
        resumed = restarted.run_cycle(repo, cutoff_ns=cutoff)
        assert resumed.ready and resumed.state.content_hash == heads[0]["state_ref"]
        assert all(len(page) <= 128 for page in archive[1])
