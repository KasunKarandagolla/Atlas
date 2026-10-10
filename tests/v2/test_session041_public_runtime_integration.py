"""Deterministic integration through the broad production port and runtime."""

from __future__ import annotations

import asyncio
import builtins
import hashlib
import json
import threading
import time
import zlib
from decimal import Decimal
from pathlib import Path

import pytest

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.data.broad_public_source import BroadPublicSnapshotV2, PublicInputRecordV2
from atlas.v2.data.durable_public_capture import SealedPublicTransportV1
from atlas.v2.data.microstructure_archive import L2FrameArchiveV2
from atlas.v2.data.public_microstructure_ws import CapturedPublicFrameV2
from atlas.v2.data.raw import RawObservationV2
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.broad_public_runtime import BroadPublicRuntimeV2
from atlas.v2.runtime.broad_universe import active_products, full_universe, latest_workset
from atlas.v2.runtime.production import BroadProductionOpsCyclePortV2

NOW = 1_800_000_000_000_000_000


def _product(symbol: str, venue: VenueV2) -> ProductContractV2:
    revision = sha256_json({"venue": venue.value, "symbol": symbol, "revision": 1})
    key = InstrumentKeyV2(venue, EnvironmentV2.MAINNET,
        ProductTypeV2.LINEAR_PERPETUAL, symbol, symbol.removesuffix("USDT"),
        "USDT", "USDT", revision)
    return ProductContractV2(key, NOW, NOW, NOW, Decimal("1"), Decimal("0.01"),
        Decimal("0.001"), Decimal("0.001"), TradingStatusV2.TRADING, revision)


class _DeterministicBybitSource:
    enabled_venues = (VenueV2.BYBIT, VenueV2.BINANCE)
    required_source_ids = ("BYBIT_PUBLIC_V2", "BINANCE_PUBLIC_V2")

    def __init__(self, products: tuple[ProductContractV2, ...], at_ns: int) -> None:
        self.current_products = products
        self.at_ns = at_ns
        self._state = {"schema_version": 2, "rotation_cursor": 0,
            "enabled_venues": ["BYBIT"], "bar_cursors_ms": {}, "bar_pages": {},
            "last_cheap_refresh_at_ns": None}
        self.enrichment_keys = ()

    def bootstrap_products(self, *, now_ns: int) -> tuple[ProductContractV2, ...]:
        assert now_ns >= self.at_ns
        return self.current_products

    def begin_collection_cycle(self, *, now_ns: int) -> None:
        assert now_ns >= self.at_ns

    def acquire_snapshot(self, *, now_ns: int) -> BroadPublicSnapshotV2:
        assert now_ns >= self.at_ns
        records = []
        for product in self.current_products:
            source_id = f"{product.key.venue.value}_PUBLIC_V2"
            event_type = ("TICKER_MARK_INDEX_FUNDING_OI" if product.key.venue == VenueV2.BYBIT
                          else "TICKER_24H")
            payload = canonical_json({
                "symbol": product.key.native_symbol,
                "lastPrice": "100.01",
                "indexPrice": "100",
                "markPrice": "100",
                "bid1Price": "99.99",
                "ask1Price": "100.01",
                "turnover24h": "20000000",
            }).encode()
            raw = RawObservationV2.build(
                instrument_revision=product.key.contract_revision,
                source_id=source_id,
                event_type=event_type,
                event_at_ns=self.at_ns,
                received_at_ns=self.at_ns,
                ingested_at_ns=self.at_ns,
                available_at_ns=self.at_ns,
                payload=payload,
                translation_version="SESSION041_PUBLIC_RUNTIME_INTEGRATION_V1",
                sequence=f"ticker:{self.at_ns}:{product.key.venue.value}:{product.key.native_symbol}",
            )
            records.append(PublicInputRecordV2(raw, payload, product.key))
        metadata = {venue.value: {"market_status": "BULK_MARKET_COMPLETE",
            "metadata_received_at_ns": NOW} for venue in self.enabled_venues}
        manifest = {"schema_version": 1, "enabled_venues": ["BYBIT", "BINANCE"],
            "acquisition_due": True, "metadata": metadata,
            "source_snapshot_id": sha256_json(metadata)}
        return BroadPublicSnapshotV2(tuple(records), True, None, None, self.at_ns,
            self.at_ns, 1, 1, 0, 0, 0, manifest)

    def export_state(self) -> dict:
        self._state["last_cheap_refresh_at_ns"] = self.at_ns
        return dict(self._state)

    def restore_state(self, state) -> None:
        self._state = dict(state)

    def set_enrichment_keys(self, keys) -> None:
        self.enrichment_keys = tuple(sorted(keys, key=lambda key: key.to_canonical_json()))

    def close(self) -> None:
        pass


def _idle_stream():
    async def frames():
        await asyncio.Event().wait()
        if False:
            yield None

    return frames


def _port(products: tuple[ProductContractV2, ...], at_ns: int):
    runtime = BroadPublicRuntimeV2(
        clock_ns=lambda: at_ns,
        stream_factories={"BYBIT": _idle_stream(), "BINANCE_MARKET": _idle_stream()},
    )
    source = _DeterministicBybitSource(products, at_ns)
    return BroadProductionOpsCyclePortV2(public_source=source,
        broad_runtime=runtime, clock_ns=lambda: at_ns), runtime, source


def test_broad_runtime_status_summarizes_phase_timing_counters():
    runtime = BroadPublicRuntimeV2()
    runtime._record_phase_duration_ns("worker_wait", 12)
    runtime._record_phase_duration_ns("worker_wait", 5)
    runtime._record_phase_duration_ns("worker_execution", 4)

    status = runtime.status()
    assert status.phase_timings_ns["worker_wait"] == {
        "count": 2, "sum_ns": 17, "max_ns": 12,
    }
    assert status.phase_timings_ns["worker_execution"] == {
        "count": 1, "sum_ns": 4, "max_ns": 4,
    }
    assert status.phase_timings_ns["input_write"] == {
        "count": 0, "sum_ns": 0, "max_ns": 0,
    }
    assert status.phase_timings_ns["archive_extent_rename"] == {
        "count": 0, "sum_ns": 0, "max_ns": 0,
    }
    assert runtime.progress_snapshot()["stream_phase_timings_ns"] == status.phase_timings_ns


def test_candidate_state_copies_each_distinct_feed_once(monkeypatch):
    product = _product("BTCUSDT", VenueV2.BYBIT)
    runtime = BroadPublicRuntimeV2()
    runtime.plan = type("Plan", (), {"key_for_frame": lambda _self, _frame: product.key})()
    tracker_key = (product.key, "publicTrade.BTCUSDT")
    runtime._sequence_books[product.key] = object()
    runtime._trackers[tracker_key] = object()
    frames = tuple(CapturedPublicFrameV2(
        VenueV2.BYBIT, "BYBIT_PUBLIC_WS_BROAD_V2", "publicTrade.BTCUSDT",
        b"{}", hashlib.sha256(b"{}").hexdigest(), NOW + index, NOW + index, 1,
    ) for index in range(3))
    copied: list[object] = []
    monkeypatch.setattr("atlas.v2.runtime.broad_public_runtime.copy.deepcopy",
                        lambda value: copied.append(value) or value)

    books, trackers, epochs = runtime._candidate_state_for_frames(frames)

    assert books[product.key] is runtime._sequence_books[product.key]
    assert trackers[tracker_key] is runtime._trackers[tracker_key]
    assert epochs == {}
    assert copied == [runtime._sequence_books[product.key], runtime._trackers[tracker_key]]


def test_broad_production_port_integrates_acquisition_stream_workset_and_restart(tmp_path):
    # Identical native symbols remain distinct cross-venue product identities.
    products = (_product("BTCUSDT", VenueV2.BYBIT), _product("BTCUSDT", VenueV2.BINANCE))
    path = tmp_path / "ops.sqlite"

    port, runtime, source = _port(products, NOW)
    with OpsRepository(path) as repository:
        recovery = port.recover(repository, now_ns=NOW)
        assert runtime.plan is not None
        assert set(runtime.plan.keys) == {product.key for product in products}
        cycle = port.collect(repository, now_ns=NOW, recovery=recovery)
        assert cycle is not None

        workset = latest_workset(repository, cutoff_ns=NOW)
        universe = full_universe(repository, cutoff_ns=NOW)
        assert workset is not None and universe is not None
        assert {row.key for row in universe.entries} == {p.key for p in products}
        assert len(active_products(repository, cutoff_ns=NOW) or ()) <= 24
        receipt = repository.latest_artifact_entries(
            "BroadPublicAcquisitionReceiptV2", as_of_ns=NOW, limit=1).entries[0]
        source_refs = receipt.metadata["receipt"]["source_observation_refs"]
        assert len(source_refs["BYBIT_PUBLIC_V2"]) == 1
        assert len(source_refs["BINANCE_PUBLIC_V2"]) == 1
        assert source.enrichment_keys == tuple(sorted(
            source.enrichment_keys, key=lambda key: key.to_canonical_json()))

        port.service_public_stream(repository)
        assert runtime._service_calls >= 1
        stream_status = runtime.status()
        assert stream_status.handoff.max_queue_items == 512
        assert stream_status.handoff.max_queue_bytes == 16_000_000
        assert stream_status.capture["max_pending_batches"] == 64

        expected_universe_ref = universe.content_hash
        expected_workset_ref = sha256_json(workset)
        scheduler_state = source.export_state()
        port.finish_public_capture(repository)
    port.close()

    restarted, restarted_runtime, restarted_source = _port(products, NOW + 10_000_000_000)
    with OpsRepository(path) as repository:
        recovery = restarted.recover(repository, now_ns=NOW + 10_000_000_000)
        assert recovery.required_source_ids == ("BINANCE_PUBLIC_V2", "BYBIT_PUBLIC_V2")
        assert restarted_source._state == scheduler_state
        assert restarted_runtime.plan is not None
        universe = full_universe(repository, cutoff_ns=NOW + 10_000_000_000)
        workset = latest_workset(repository, cutoff_ns=NOW + 10_000_000_000)
        assert universe is not None and universe.content_hash == expected_universe_ref
        assert workset is not None and sha256_json(workset) == expected_workset_ref
        assert {row.key for row in universe.entries} == {p.key for p in products}
        assert len(active_products(repository, cutoff_ns=NOW + 10_000_000_000) or ()) <= 24
        assert repository.latest_artifact_entries(
            "BroadPublicAcquisitionReceiptV2", as_of_ns=NOW + 10_000_000_000, limit=1).entries
        restarted.finish_public_capture(repository)
    restarted.close()


def test_preparation_completion_is_nonblocking_and_publication_stays_writer_owned(tmp_path, monkeypatch):
    products = (_product("BTCUSDT", VenueV2.BYBIT), _product("BTCUSDT", VenueV2.BINANCE))
    port, runtime, _source = _port(products, NOW)
    port_closed = False
    try:
        with OpsRepository(tmp_path / "async-adoption.sqlite") as repository:
            port.recover(repository, now_ns=NOW)
            assert runtime.source is not None and runtime.capture is not None
            lane = runtime.source._lanes["BYBIT"]._handoff
            expected_payloads = {}
            for index in range(2):
                at_ns = NOW - 1_000_000_000 + index + 1
                at_ms = at_ns // 1_000_000
                raw = json.dumps({"topic": "publicTrade.BTCUSDT", "type": "snapshot",
                    "ts": at_ms, "data": [{"T": at_ms, "s": "BTCUSDT", "S": "Buy",
                        "v": "0.01", "p": "100", "i": f"async-{index}"}]},
                    separators=(",", ":")).encode()
                frame = CapturedPublicFrameV2(
                    VenueV2.BYBIT, "BYBIT_PUBLIC_WS_BROAD_V2", "publicTrade.BTCUSDT",
                    raw, hashlib.sha256(raw).hexdigest(), at_ns, at_ns, 1,
                )
                assert lane.offer(frame)
                expected_payloads[frame.raw_payload_hash] = raw

            deadline = time.monotonic() + 3.0
            while (runtime.capture.status().capture["pending_batches"] == 0
                   and time.monotonic() < deadline):
                time.sleep(0.005)
            capture_status = runtime.capture.status()
            assert capture_status.capture["pending_batches"] > 0, (
                f"terminal={capture_status.capture['terminal_error']!r}; "
                f"received={capture_status.handoff.frames_received}; "
                f"queued={capture_status.handoff.queue_items}; "
                f"captured={capture_status.capture['captured_frames']}"
            )

            worker = runtime._preparation_worker
            assert worker is not None
            original_submit = worker.submit
            submitted_manifests = []

            def inspect_v3_submit(request):
                manifest = json.loads(Path(request.input_path).read_text())
                assert request.preparation_version == "PUBLIC_EVIDENCE_PREPARATION_V3"
                assert set(manifest) == {"frames", "transport_binding"}
                assert all("raw_payload_b64" not in item["frame"] for item in manifest["frames"])
                assert all(item["frame"]["transport_ordinal"] == item["frame_ordinal"]
                           for item in manifest["frames"])
                submitted_manifests.append(manifest)
                original_submit(request)

            monkeypatch.setattr(worker, "submit", inspect_v3_submit)
            runtime.service(repository, now_ns=NOW + 10)
            pending = runtime._pending_adoption
            assert pending is not None
            descriptor = pending.sealed
            original_poll = worker.poll
            monkeypatch.setattr(worker, "poll", lambda: None)
            started = time.monotonic_ns()
            runtime.service(repository, now_ns=NOW + 11)
            parse_poll_duration = time.monotonic_ns() - started

            assert parse_poll_duration < 100_000_000
            assert runtime._pending_adoption is pending
            assert runtime.status().capture["pending_frames"] == descriptor.frame_count
            assert runtime.status().capture["adoption_pending_batches"] == 1
            assert repository.get_artifact(descriptor.batch.artifact_ref) is None
            assert runtime._service_frames == 0
            assert runtime.sequence_books == {}

            monkeypatch.setattr(worker, "poll", original_poll)
            deadline = time.monotonic() + 5.0
            while runtime._pending_adoption is not None and time.monotonic() < deadline:
                runtime.service(repository, now_ns=NOW + 12)
                time.sleep(0.001)
            assert runtime._pending_adoption is None
            assert submitted_manifests
            assert repository.public_adoption_stages_v2(
                runtime._run_id or "", descriptor.batch.artifact_ref,
            ) == (None, ())
            port.finish_public_capture(repository)
            status = runtime.status()
            assert status.capture["pending_frames"] == 0
            assert status.capture["captured_frames"] == 2
            assert status.capture["delivered_frames"] == 2
            assert runtime._service_frames == 2
            assert repository.get_artifact(descriptor.batch.artifact_ref) == descriptor.batch
            assert runtime._archive is not None
            checkpoints = repository.artifact_entries("L2FrameArchiveCheckpointV3")
            assert len(checkpoints) == 1
            view_ref = str(checkpoints[0].metadata["archive_extent_ref"])
            view_descriptor = repository.get_artifact(view_ref)
            assert view_descriptor is not None
            assert view_descriptor.artifact_type == "PublicArchiveFrameViewV1"
            rows = runtime._archive.read_chunk(str(checkpoints[0].metadata["chunk_id"]))
            assert {row["raw_payload_hash"]: row["raw_payload_bytes"] for row in rows} == expected_payloads
            publication = repository._connection.execute(
                "SELECT publication_id FROM public_evidence_locator_v2 WHERE artifact_ref=?", (view_ref,),
            ).fetchone()
            assert publication is not None
            assert repository._connection.execute(
                "SELECT 1 FROM public_evidence_publication_observation_v2 WHERE publication_id=?",
                (publication[0],),
            ).fetchone() is not None
            continuity_entries = repository.artifact_entries("BroadPublicStreamContinuityV3")
            assert len(continuity_entries) == 4
            assert all(repository.get_artifact(item.artifact_ref) == item for item in continuity_entries)
            lane_health = tuple(
                item for item in repository.artifact_entries("PublicSourceHealthV2")
                if "lane" in item.metadata
            )
            assert lane_health
            assert all(repository.effective_available_at_ns(item.artifact_ref) is not None
                       for item in lane_health)
            compact_row = repository._connection.execute(
                "SELECT b.codec,b.payload FROM public_evidence_locator_v2 l "
                "JOIN public_evidence_block_v2 b ON b.block_ref=l.block_ref "
                "WHERE l.artifact_ref=?", (continuity_entries[0].artifact_ref,),
            ).fetchone()
            assert compact_row is not None
            compact_payload = (zlib.decompress(compact_row["payload"])
                               if compact_row["codec"] == "zlib-1" else bytes(compact_row["payload"]))
            assert "contexts" in json.loads(compact_payload)
            operational = repository.artifact_entries("PublicStreamOperationalCheckpointV1")
            assert len(operational) == 1
            assert len(operational[0].metadata["checkpoint"]["state"]["trade_identity_cache"]) == 2
            lifecycle = json.loads((tmp_path / "public-capture-lifecycle-v1.json").read_text())
            assert lifecycle["state"] == "CLEAN"
        port.close()
        port_closed = True
        restarted, restarted_runtime, _source = _port(products, NOW + 10_000_000_000)
        try:
            with OpsRepository(tmp_path / "async-adoption.sqlite") as repository:
                restarted_runtime.recover(repository, run_root=tmp_path, products=products, tiers={},
                    now_ns=NOW + 10_000_000_000, benchmark_keys=(products[0].key, products[1].key))
                assert set(restarted_runtime._trackers) == {
                    (products[0].key, "publicTrade.BTCUSDT"),
                }
                assert set(restarted_runtime._connection_epochs.values()) == {-1}
                restarted.finish_public_capture(repository)
        finally:
            restarted.close()
    finally:
        if not port_closed:
            port.close()


def test_alt_only_inventory_keeps_deterministic_stream_seed_after_publication(tmp_path):
    products = (_product("ALT01USDT", VenueV2.BYBIT),
                _product("ALT02USDT", VenueV2.BINANCE))
    port, runtime, _source = _port(products, NOW)
    try:
        with OpsRepository(tmp_path / "alt-only.sqlite") as repository:
            recovery = port.recover(repository, now_ns=NOW)
            assert runtime.plan is not None
            assert set(runtime.plan.keys) == {product.key for product in products}

            cycle = port.collect(repository, now_ns=NOW, recovery=recovery)

            assert cycle is not None
            assert runtime.plan is not None
            assert set(runtime.plan.keys) == {product.key for product in products}
            assert len(runtime.plan.keys) == 2
            port.finish_public_capture(repository)
    finally:
        port.close()


def test_broad_runtime_capture_progresses_during_sqlite_commit_stall(tmp_path):
    offered_count = 160
    products = (
        _product("BTCUSDT", VenueV2.BYBIT),
        _product("ETHUSDT", VenueV2.BINANCE),
        *tuple(_product(f"X{index:03d}USDT", VenueV2.BYBIT) for index in range(62)),
    )
    port, runtime, source = _port(products, NOW)
    interpreted: list[CapturedPublicFrameV2] = []
    runtime.interpret_frames = lambda _repo, _plan, frames, _now: interpreted.extend(frames)
    path = tmp_path / "broad-stall.sqlite"

    with OpsRepository(path) as repository:
        port.recover(repository, now_ns=NOW)
        snapshot = source.acquire_snapshot(now_ns=NOW)
        port._register_refreshed_stream_products(repository, snapshot)
        assert runtime.source is not None
        bybit_lane = runtime.source._lanes["BYBIT"]
        handoff = bybit_lane._handoff
        producer_started = threading.Event()
        producer_done = threading.Event()
        producer_errors: list[BaseException] = []
        offered_frames: list[CapturedPublicFrameV2] = []
        stall_started = threading.Event()
        stall_duration: list[float] = []
        captured_during_stall: list[int] = []
        stalled = False

        def producer() -> None:
            try:
                producer_started.set()
                if not stall_started.wait(2.0):
                    raise AssertionError("SQLite commit stall was not reached")
                for index in range(offered_count):
                    at_ns = NOW - 1_000_000_000 + index + 1
                    raw = json.dumps({"topic": "publicTrade.BTCUSDT", "type": "snapshot",
                        "ts": at_ns, "data": [{"T": at_ns, "s": "BTCUSDT", "S": "Buy",
                            "v": "0.01", "p": "100", "i": f"trade-{index}"}]},
                        separators=(",", ":")).encode()
                    item = CapturedPublicFrameV2(
                        VenueV2.BYBIT,
                        "BYBIT_PUBLIC_WS_BROAD_V2",
                        "publicTrade.BTCUSDT",
                        raw,
                        hashlib.sha256(raw).hexdigest(),
                        at_ns,
                        at_ns,
                        1,
                    )
                    if not handoff.offer(item):
                        raise AssertionError("broad Bybit handoff rejected a frame")
                    offered_frames.append(item)
            except BaseException as exc:
                producer_errors.append(exc)
            finally:
                producer_done.set()

        original_connection = repository._connection

        class StalledConnection:
            def __getattr__(self, name):
                return getattr(original_connection, name)

            def commit(self):
                nonlocal stalled
                if not stalled:
                    stalled = True
                    stall_started.set()
                    before = time.monotonic()
                    assert runtime.capture is not None
                    captured = 0
                    deadline = before + 3.0
                    while time.monotonic() < deadline:
                        captured = int(runtime.capture.status().capture["captured_frames"])
                        if captured and time.monotonic() - before >= 0.5:
                            break
                        time.sleep(0.01)
                    captured_during_stall.append(captured)
                    stall_duration.append(time.monotonic() - before)
                return original_connection.commit()

        repository._connection = StalledConnection()
        producer_thread = threading.Thread(target=producer, name="s41-broad-stall-producer")
        producer_thread.start()
        assert producer_started.wait(1.0)
        try:
            port._persist_broad_public_snapshot(repository, snapshot, now_ns=NOW)
            producer_thread.join(timeout=2.0)
            assert producer_done.is_set()
            assert not producer_errors
            assert stall_started.is_set() and stall_duration and stall_duration[0] >= 0.45
            assert captured_during_stall and captured_during_stall[0] > 0

            # The independent raw-capture worker must continue draining the
            # actual bounded source queue while the SQLite controller sleeps.
            status_during_or_after_stall = runtime.status()
            assert status_during_or_after_stall.capture["captured_frames"] == offered_count
            assert status_during_or_after_stall.handoff.frames_received == offered_count
            assert status_during_or_after_stall.handoff.frames_drained == offered_count
            assert status_during_or_after_stall.handoff.frames_rejected == 0
            assert not status_during_or_after_stall.handoff.overflowed
            assert status_during_or_after_stall.handoff.high_water_items <= (
                status_during_or_after_stall.handoff.max_queue_items
            )
            assert status_during_or_after_stall.handoff.high_water_bytes <= (
                status_during_or_after_stall.handoff.max_queue_bytes
            )
            assert status_during_or_after_stall.capture["high_water_batches"] <= (
                status_during_or_after_stall.capture["max_pending_batches"]
            )

            port.finish_public_capture(repository)
            final_status = runtime.status()
            assert final_status.capture["pending_frames"] == 0
            assert final_status.capture["pending_batches"] == 0
            assert final_status.capture["terminal_error"] is None
            assert final_status.handoff.queue_items == 0
            assert final_status.handoff.frames_received == offered_count
            assert final_status.handoff.frames_drained == offered_count
            assert runtime._service_frames == offered_count
            assert len(offered_frames) == offered_count
            assert interpreted == offered_frames
            assert runtime.status().max_service_gap_ns >= 450_000_000
            lifecycle = json.loads((tmp_path / "public-capture-lifecycle-v1.json").read_text())
            assert lifecycle["state"] == "CLEAN"
        finally:
            repository._connection = original_connection
            if producer_thread.is_alive():
                producer_thread.join(timeout=2.0)
            port.close()

    # A new controller must bind the clean lifecycle to the final indexed
    # receipt and re-read the exact retained extent before it starts capture.
    restarted, restarted_runtime, _ = _port(products, NOW + 10_000_000_000)
    with OpsRepository(path) as repository:
        restarted_runtime.recover(
            repository,
            run_root=tmp_path,
            products=products,
            tiers={},
            now_ns=NOW + 10_000_000_000,
            benchmark_keys=(products[0].key, products[1].key),
        )
        assert restarted_runtime.status().capture["terminal_error"] is None
        # This fixture replaces the interpreter with a frame collector above,
        # so it intentionally creates no continuity checkpoint to restore.
        assert restarted_runtime._trackers == {}
        assert restarted_runtime._connection_epochs == {}
        restarted_runtime.finish(repository)
        restarted_runtime.close()


@pytest.mark.parametrize("read_stall_seconds", [0.1, 0.5, 1.0])
def test_reconciliation_services_real_broad_stream_between_pages_under_read_stalls(
    tmp_path, monkeypatch, read_stall_seconds: float,
):
    products = (_product("BTCUSDT", VenueV2.BYBIT), _product("BTCUSDT", VenueV2.BINANCE))
    port, runtime, _source = _port(products, NOW)
    service_starts: list[int] = []
    service_durations: list[int] = []
    read_stalls: list[tuple[int, int]] = []
    producer_errors: list[BaseException] = []
    actual_service_starts: list[int] = []
    actual_service_ends: list[int] = []
    adoption_durations_ns: list[int] = []
    interpretation_durations_ns: list[int] = []
    archive_write_durations_ns: list[int] = []
    extent_write_phase_metrics: list[dict[str, int | str]] = []
    allow_burst = (threading.Event(), threading.Event())
    burst_frames = 2
    frame_ordinal = 0
    path = tmp_path / f"paged-reconciliation-{int(read_stall_seconds * 1000)}.sqlite"

    def offer_burst(handoffs, count: int) -> None:
        nonlocal frame_ordinal
        for _ in range(count):
            frame_ordinal += 1
            received = NOW - 1_000_000_000 + frame_ordinal
            if frame_ordinal % 2:
                event_ms = received // 1_000_000
                body = {"topic": "publicTrade.BTCUSDT", "type": "snapshot", "ts": event_ms,
                    "data": [{"T": event_ms, "s": "BTCUSDT", "S": "Buy", "v": "0.01",
                        "p": "100.01", "i": f"s41-paged-{frame_ordinal}"}]}
                venue, source_id, channel, lane_name = (VenueV2.BYBIT,
                    "BYBIT_PUBLIC_WS_BROAD_V2", "publicTrade.BTCUSDT", "BYBIT")
            else:
                body = {"stream": "btcusdt@aggTrade", "data": {
                    "e": "aggTrade", "E": received // 1_000_000, "s": "BTCUSDT",
                    "a": frame_ordinal, "p": "100.01", "q": "0.01", "f": frame_ordinal,
                    "l": frame_ordinal, "T": received // 1_000_000, "m": False}}
                venue, source_id, channel, lane_name = (VenueV2.BINANCE,
                    "BINANCE_MARKET_PUBLIC_WS_BROAD_V2", "btcusdt@aggTrade", "BINANCE_MARKET")
            payload = json.dumps(body, separators=(",", ":")).encode()
            frame = CapturedPublicFrameV2(venue, source_id, channel, payload,
                hashlib.sha256(payload).hexdigest(), received, received, 1)
            assert handoffs[lane_name].offer(frame)

    with OpsRepository(path) as repository:
        port.recover(repository, now_ns=NOW)
        assert runtime.source is not None and runtime.capture is not None
        handoffs = {name: lane._handoff for name, lane in runtime.source._lanes.items()}
        capture = runtime.capture
        original_runtime_service = runtime.service
        original_adopt = SealedPublicTransportV1.adopt
        original_interpret = runtime._interpret_frames
        original_archive_write = L2FrameArchiveV2.write_chunks

        def measured_adopt(sealed, repository):
            started = time.perf_counter_ns()
            try:
                return original_adopt(sealed, repository)
            finally:
                adoption_durations_ns.append(time.perf_counter_ns() - started)

        def measured_interpret(repository, frames, *, now_ns):
            started = time.perf_counter_ns()
            try:
                return original_interpret(repository, frames, now_ns=now_ns)
            finally:
                interpretation_durations_ns.append(time.perf_counter_ns() - started)

        def measured_archive_write(archive, frames):
            started = time.perf_counter_ns()
            try:
                original_import = builtins.__import__

                def reject_parquet_import(name, *args, **kwargs):
                    if name == "pyarrow.parquet":
                        raise AssertionError("compact live archive must not import legacy Parquet writer")
                    return original_import(name, *args, **kwargs)

                with monkeypatch.context() as import_guard:
                    import_guard.setattr(builtins, "__import__", reject_parquet_import)
                    return original_archive_write(archive, frames)
            finally:
                archive_write_durations_ns.append(time.perf_counter_ns() - started)
                writer = archive.repository._public_extent_writer
                if writer is not None:
                    extent_write_phase_metrics.append(dict(writer.metrics))

        monkeypatch.setattr(SealedPublicTransportV1, "adopt", measured_adopt)
        monkeypatch.setattr(runtime, "_interpret_frames", measured_interpret)
        monkeypatch.setattr(L2FrameArchiveV2, "write_chunks", measured_archive_write)

        def measured_runtime_service(repository, *, now_ns):
            actual_service_starts.append(time.perf_counter_ns())
            try:
                return original_runtime_service(repository, now_ns=now_ns)
            finally:
                actual_service_ends.append(time.perf_counter_ns())

        monkeypatch.setattr(runtime, "service", measured_runtime_service)
        offer_burst(handoffs, burst_frames)
        before = capture.status().capture["pending_batches"]
        deadline = time.monotonic() + 5
        while capture.status().capture["pending_batches"] <= before and time.monotonic() < deadline:
            time.sleep(0.01)
        assert capture.status().capture["pending_batches"] > before

        # 257 immutable refs force three collector pages. Delays are injected
        # only for those exact page reads; stream service runs on the real
        # supervisor path between them, on the same sole repository writer.
        refs = []
        entries = []
        for index in range(257):
            record_id = f"s41-reconnect-observation-{index}"
            ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": record_id})
            refs.append(ref)
            entries.append(ArtifactIndexEntryV2(ref, "PublicObservationIndexV2",
                sha256_json({"observation": record_id}), NOW + 1, NOW + 1,
                {"record_id": record_id, "source_id": "BYBIT_PUBLIC_V2"}))
        repository.register_artifacts(tuple(entries))
        ordered_refs = tuple(sorted(refs))
        pages = {ordered_refs[offset:offset + 128] for offset in range(0, len(ordered_refs), 128)}
        original_lookup = repository.get_artifact_metadata_by_refs

        def stalled_lookup(artifact_refs):
            requested = tuple(artifact_refs)
            if requested in pages:
                started = time.perf_counter_ns()
                time.sleep(read_stall_seconds)
                read_stalls.append((started, time.perf_counter_ns()))
            return original_lookup(artifact_refs)

        monkeypatch.setattr(repository, "get_artifact_metadata_by_refs", stalled_lookup)
        collector_state = port._collector_recovery
        assert collector_state is not None
        collector = collector_state.collector

        def producer() -> None:
            try:
                for event in allow_burst:
                    if not event.wait(30):
                        raise AssertionError("next broad stream burst was not released by page service")
                    offer_burst(handoffs, burst_frames)
            except BaseException as exc:
                producer_errors.append(exc)

        producer_thread = threading.Thread(target=producer, name="s41-reconcile-stream-producer")
        producer_thread.start()

        def service() -> None:
            assert not repository._connection.in_transaction
            expected_captured = burst_frames * (len(service_starts) + 1)
            deadline = time.monotonic() + 10
            while (capture.status().capture["captured_frames"] < expected_captured
                   and time.monotonic() < deadline):
                time.sleep(0.0005)
            assert capture.status().capture["captured_frames"] >= expected_captured
            started = time.perf_counter_ns()
            port.service_public_stream(repository)
            service_durations.append(time.perf_counter_ns() - started)
            service_starts.append(started)
            if len(service_starts) <= len(allow_burst):
                allow_burst[len(service_starts) - 1].set()

        health = collector.reconcile_after_reconnect(
            "BYBIT_PUBLIC_V2", at_ns=NOW + 10_000_000_000,
            complete_snapshot=True, missed_interval_repaired=True,
            snapshot_refs=ordered_refs,
            recovery_epoch_ref=collector.required_recovery_epoch_ref,
            service_callback=service,
        )
        producer_thread.join(timeout=5)
        assert not producer_thread.is_alive() and not producer_errors
        assert health.state.value == "HEALTHY_CURRENT"
        assert len(service_starts) == len(read_stalls) == 3
        assert len(service_durations) == 3
        assert sum(lane.snapshot().frames_rejected for lane in handoffs.values()) == 0
        assert {name: lane.snapshot().frames_drained for name, lane in handoffs.items()} == {
            "BYBIT": burst_frames * 3 // 2, "BINANCE_MARKET": burst_frames * 3 // 2,
        }
        assert capture.status().capture["high_water_batches"] < capture.status().capture["max_pending_batches"]

        # Measure actual BroadPublicRuntime entries, not merely callbacks. Once
        # the prior service ends, the next service is bounded by any injected
        # page stall, the 200ms capture/service budget, and clock resolution.
        assert len(actual_service_starts) >= 2
        assert len(actual_service_ends) == len(actual_service_starts)
        service_gaps_ns = []
        service_gap_limits_ns = []
        clock_allowance_ns = int(2 * time.get_clock_info("perf_counter").resolution * 1_000_000_000)
        for index in range(1, len(actual_service_starts)):
            left, right = actual_service_ends[index - 1], actual_service_starts[index]
            overlap_ns = sum(max(0, min(right, end) - max(left, start))
                for start, end in read_stalls if end > left and start < right)
            gap_ns = right - left
            allowed_ns = overlap_ns + 200_000_000 + clock_allowance_ns
            service_gaps_ns.append(gap_ns)
            service_gap_limits_ns.append(allowed_ns)
            assert gap_ns <= allowed_ns, {
                "post_service_gap_ns": gap_ns,
                "read_stall_overlap_ns": overlap_ns,
                "actual_service_durations_ns": [end - start for start, end in zip(
                    actual_service_starts, actual_service_ends, strict=True)],
                "actual_service_count": len(actual_service_starts),
                "runtime_reported_max_gap_ns": runtime._max_service_gap_ns,
            }
        port.finish_public_capture(repository)
        assert runtime._service_frames == burst_frames * 3
        final = runtime.status()
        assert final.capture["pending_batches"] == 0
        assert final.handoff.queue_items == 0
        assert final.capture["terminal_error"] is None
        print("SESSION041_REAL_QUEUE_SERVICE_METRICS=" + json.dumps({
            "injected_read_stall_seconds": read_stall_seconds,
            "service_entries": len(actual_service_starts),
            "service_durations_ns": [end - start for start, end in zip(
                actual_service_starts, actual_service_ends, strict=True)],
            "adoption_durations_ns": adoption_durations_ns,
            "interpretation_durations_ns": interpretation_durations_ns,
            "archive_write_durations_ns": archive_write_durations_ns,
            "extent_write_phase_metrics": extent_write_phase_metrics,
            "post_service_gaps_ns": service_gaps_ns,
            "post_service_gap_limits_ns": service_gap_limits_ns,
            "captured_frames": final.capture["captured_frames"],
            "service_frames": runtime._service_frames,
            "queue_high_water_items": final.handoff.high_water_items,
            "queue_high_water_bytes": final.handoff.high_water_bytes,
            "pending_batch_high_water": final.capture["high_water_batches"],
            "pending_batch_limit": final.capture["max_pending_batches"],
            "persistence_metrics": repository.persistence_metrics(),
            "extent_writer_metrics": (repository._public_extent_writer.metrics
                                       if repository._public_extent_writer is not None else None),
        }, sort_keys=True))
    port.close()
