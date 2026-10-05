"""Scheduling regressions plus an explicitly opt-in real-time capacity probe.

Simulated receipt clocks establish FIFO, headroom and evidence semantics, not
hardware throughput. The real-time probe uses real SQLite/Parquet and a producer
which runs independently of the writer; it must be run separately to establish
that its declared workload is sustainable on the tested host.
"""

from __future__ import annotations

import hashlib
import json
import os
import runpy
import threading
import time
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pyarrow.parquet as pq
import pytest

from atlas.v2.data.active_history import advance
from atlas.v2.data.bars import BarIntervalV2
from atlas.v2.data.bybit_source import BybitPublicSnapshotV1
from atlas.v2.data.public_archive_extents import read_extent
from atlas.v2.data.public_microstructure_ws import (
    BoundedPublicFrameHandoffV2,
    CapturedPublicFrameV2,
    bybit_btc_eth_linear_topics,
)
from atlas.v2.instruments import VenueV2
from atlas.v2.runtime import active_history, production
from atlas.v2.runtime.ops_supervisor import OpsCycleBatchV1, OpsSupervisorV2
from atlas.v2.runtime.serviced_acquisition import ServicedPublicAcquisitionV1

from .test_session037_active_history import KEY, indexed

# Reuse the established S32 metadata fixture.
_S32 = runpy.run_path(str(Path(__file__).with_name("test_session032_public_stream_integration.py")))
NOW_NS = int(_S32["NOW_NS"])


class FixturePublicSource:
    required_source_ids: tuple[str, ...] = ()

    def __init__(self) -> None:
        self.products = tuple(_S32["_contract"](symbol) for symbol in ("BTCUSDT", "ETHUSDT"))

    def bootstrap_products(self, *, now_ns: int) -> Any:
        self.products = tuple(replace(product, observed_at_ns=now_ns, available_at_ns=now_ns)
                              for product in self.products)
        return self.products

    def collect(self, repository: Any, collector: Any, *, now_ns: int, recovery: Any) -> OpsCycleBatchV1:
        del recovery
        assert collector.repository is repository
        return OpsCycleBatchV1((), (), (), (), True, now_ns)


def _frame(channel: str, payload: dict[str, Any], *, received_at_ns: int) -> CapturedPublicFrameV2:
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return CapturedPublicFrameV2(VenueV2.BYBIT, production.BYBIT_PUBLIC_WS_SOURCE_ID_V1,
                                 channel, raw, hashlib.sha256(raw).hexdigest(),
                                 received_at_ns, received_at_ns, 1)


class QueueStream:
    venue = VenueV2.BYBIT
    topics = bybit_btc_eth_linear_topics()

    def __init__(self, clock_ns: Any) -> None:
        self.clock_ns = clock_ns
        self.handoff = BoundedPublicFrameHandoffV2(venue=self.venue, topics=self.topics)
        self.closed = False

    def start(self) -> None:
        self.handoff.observe_connected(self.clock_ns())

    def drain(self, *, max_items: int) -> tuple[CapturedPublicFrameV2, ...]:
        return self.handoff.drain(max_items=max_items)

    def status(self) -> SimpleNamespace:
        return SimpleNamespace(state="RUNNING", attempt_count=1, reconnect_count=0,
                               last_error_code=None, handoff=self.handoff.snapshot())

    def close(self) -> None:
        self.closed = True
        self.handoff.close(self.clock_ns())


class MixedWorkload:
    """120 L2 and 40 one-record trade frames per second at 160 total fps."""

    def __init__(self) -> None:
        self.ordinal = 0
        self.book_counts = {"BTCUSDT": 0, "ETHUSDT": 0}

    def frame(self, received_at_ns: int) -> CapturedPublicFrameV2:
        self.ordinal += 1
        symbol = "BTCUSDT" if self.ordinal % 2 else "ETHUSDT"
        event_ms = received_at_ns // 1_000_000
        # Every group of eight contains six books and two trades, one of each
        # trade channel; each book carries one changed bid and ask level.
        if self.ordinal % 8 in (0, 7):
            return _frame(f"publicTrade.{symbol}", {
                "topic": f"publicTrade.{symbol}", "ts": event_ms,
                "data": [{"T": event_ms, "s": symbol, "S": "Buy", "v": "0.25",
                          "p": "100.5", "i": f"s38-{self.ordinal}"}],
            }, received_at_ns=received_at_ns)
        self.book_counts[symbol] += 1
        sequence = self.book_counts[symbol]
        bids = [[str(Decimal("100") - Decimal(i) / 100), "2"] for i in range(50)] if sequence == 1 else [["100", "2"]]
        asks = [[str(Decimal("101") + Decimal(i) / 100), "3"] for i in range(50)] if sequence == 1 else [["101", "3"]]
        return _frame(f"orderbook.50.{symbol}", {
            "topic": f"orderbook.50.{symbol}",
            "type": "snapshot" if sequence == 1 else "delta",
            "ts": event_ms, "cts": event_ms,
            "data": {"s": symbol, "u": sequence, "seq": sequence,
                     "b": bids, "a": asks},
        }, received_at_ns=received_at_ns)


def transport_rows(repository: Any, root: Any) -> list[dict[str, Any]]:
    entries = (*repository.artifact_entries("PublicStreamTransportBatchV1"),
               *repository.artifact_entries("PublicStreamTransportBatchV2"))
    entries = sorted(entries, key=lambda entry: entry.metadata["batch"].get("first_received_at_ns")
        if entry.artifact_type == "PublicStreamTransportBatchV2" else entry.metadata["batch"]["frames"][0]["received_at_ns"])
    return [row for entry in entries for row in (
        read_extent(repository, entry.metadata["batch"]["archive_extent_ref"])
        if entry.artifact_type == "PublicStreamTransportBatchV2" else pq.read_table(
            root / "ops-public-transport" / entry.metadata["batch"]["archive_path_name"])
    ).to_pylist()]


def assert_current_reports(repository: Any, *, book_warm: bool = False) -> None:
    reports = repository.artifact_entries("PublicStreamContinuityReportV1")
    latest: dict[str, Any] = {}
    for entry in sorted(reports, key=lambda entry: entry.available_at_ns):
        latest[entry.metadata["report"]["channel"]] = entry.metadata["report"]
    assert set(latest) == set(bybit_btc_eth_linear_topics())
    assert all(report["transport_received"] and report["source_current"] for report in latest.values())
    assert all(report["metadata_current"] and report["gap_count"] == 0 for report in latest.values())
    for channel, report in latest.items():
        if channel.startswith("orderbook."):
            if book_warm:
                assert report["book_sequence_valid"]
            else:
                assert report["bbo_stale_or_unavailable_reason"] == "NOT_ESTIMABLE_BOOK_STATE_WARMING"
    # A current transport is not fabricated public-trade completeness.
    assert all(not report["trade_completeness_proven"] for channel, report in latest.items()
               if channel.startswith("publicTrade."))


def test_old_one_drain_per_five_second_rest_cycle_exhausts_real_handoff() -> None:
    handoff = BoundedPublicFrameHandoffV2(venue=VenueV2.BYBIT, topics=bybit_btc_eth_linear_topics())
    workload = MixedWorkload()
    # Old composition drained once before REST and could next drain only after
    # about five seconds. At 160 fps it loses a frame before that next drain.
    assert handoff.drain(max_items=32) == ()
    for ordinal in range(513):
        frame = workload.frame(NOW_NS + ordinal * 6_250_000)
        assert handoff.offer(frame) is (ordinal < 512)
    status = handoff.snapshot()
    assert status.high_water_items == status.max_queue_items == 512
    assert status.overflowed and status.frames_rejected == 1
    assert status.last_error_code == "FRAME_QUEUE_OVERFLOW"
    assert (frame.received_at_ns - NOW_NS) < 5_000_000_000


def test_periodic_service_preserves_scheduled_mixed_stream_and_real_archives(tmp_path) -> None:
    clock = [NOW_NS]
    stream = QueueStream(lambda: clock[0])
    port = production.create_bybit_public_ws_port(
        public_source=FixturePublicSource(), public_stream_source=stream, clock_ns=lambda: clock[0],
    )
    expected: list[CapturedPublicFrameV2] = []
    workload = MixedWorkload()
    with OpsSupervisorV2(tmp_path / "ops.sqlite", port, clock_ns=lambda: clock[0]) as supervisor:
        supervisor.run_once()
        repository = supervisor.repository
        assert repository is not None
        # Begin with retained backlog, then continue arrivals. Headroom must
        # recover while the producer remains active, not just after it stops.
        for ordinal in range(64):
            frame = workload.frame(clock[0] + ordinal + 1)
            expected.append(frame)
            assert stream.handoff.offer(frame)
        clock[0] += 64
        backlog = 64
        for _ in range(12):
            clock[0] += 100_000_000
            for index in range(16):
                frame = workload.frame(clock[0] - (15 - index) * 6_250_000)
                expected.append(frame)
                assert stream.handoff.offer(frame)
            port.service_public_stream(repository)
            remaining = stream.handoff.snapshot().queue_items
            # Up to one sub-batch may accumulate for <=200ms, while retained
            # backlog must decrease and never grow with elapsed runtime.
            assert remaining <= max(16, backlog - 16)
            backlog = remaining
        status = stream.handoff.snapshot()
        clock[0] += 200_000_000
        port._collect_public_stream_evidence(repository, now_ns=clock[0])
        status = stream.handoff.snapshot()
        assert not status.overflowed and status.frames_rejected == 0
        assert status.high_water_items == 80 and status.frames_drained == len(expected)
        assert status.queue_items == 0
        rows = transport_rows(repository, tmp_path)
        assert [row["raw_payload_bytes"] for row in rows] == [frame.raw_payload_bytes for frame in expected]
        assert [row["received_at_ns"] for row in rows] == [frame.received_at_ns for frame in expected]
        assert_current_reports(repository)


def test_deliberate_overload_remains_visible_and_fails_closed(tmp_path) -> None:
    clock = [NOW_NS]
    stream = QueueStream(lambda: clock[0])
    port = production.create_bybit_public_ws_port(
        public_source=FixturePublicSource(), public_stream_source=stream, clock_ns=lambda: clock[0],
    )
    with OpsSupervisorV2(tmp_path / "ops.sqlite", port, clock_ns=lambda: clock[0]) as supervisor:
        supervisor.run_once()
        repository = supervisor.repository
        assert repository is not None
        workload = MixedWorkload()
        accepted = [workload.frame(NOW_NS + ordinal + 1) for ordinal in range(512)]
        for frame in accepted:
            assert stream.handoff.offer(frame)
        assert not stream.handoff.offer(workload.frame(NOW_NS + 513))
        clock[0] += 100_000_000
        port.service_public_stream(repository)
        reports = repository.artifact_entries("PublicStreamContinuityReportV1")
        assert reports
        assert all(not entry.metadata["report"]["source_current"] for entry in reports)
        assert any("QUEUE_OVERFLOW_LOCAL_DATA_LOSS" in entry.metadata["report"]["gap_reason_codes"]
                   for entry in reports)
        assert stream.handoff.snapshot().overflowed
        assert stream.handoff.snapshot().frames_rejected == 1
        rows = transport_rows(repository, tmp_path)
        assert [row["raw_payload_bytes"] for row in rows] == [frame.raw_payload_bytes for frame in accepted[:len(rows)]]


@pytest.mark.skipif(os.environ.get("ATLAS_S38_REALTIME_STREAM") != "1",
                    reason="explicit actual-wall capacity probe, separate from deterministic regression")
def test_actual_wall_mixed_stream_with_five_second_rest_wait(tmp_path, monkeypatch) -> None:
    """Declared-rate real producer; hardware timing is measured, never simulated."""
    seconds = int(os.environ.get("ATLAS_S38_REALTIME_SECONDS", "15"))
    assert 6 <= seconds <= 300
    frame_count = seconds * 160
    class SlowREST:
        def acquire_snapshot(self, *, now_ns: int) -> BybitPublicSnapshotV1:
            del now_ns
            time.sleep(5)
            return BybitPublicSnapshotV1((), False, "INCOMPLETE", "OFFLINE_PROBE", 0, time.time_ns())

    stream = QueueStream(time.time_ns)
    port = production.create_bybit_public_ws_port(
        public_source=FixturePublicSource(), public_stream_source=stream, clock_ns=time.time_ns,
    )
    expected: list[CapturedPublicFrameV2] = []
    stop = threading.Event()
    helper = ServicedPublicAcquisitionV1(SlowREST(), clock_ns=time.time_ns)
    producer_lateness: list[float] = []
    storage_samples: list[dict[str, Any]] = []
    writer_threads: set[int] = set()
    # Restore a genuinely serialized history while arrivals continue. This is
    # the formerly synchronous strict decoding seam, not a simulated delay.
    history = None
    for offset in range(0, 1200, 128):
        history = advance(history, tuple(indexed(i) for i in range(offset, min(offset + 128, 1200))),
                          key=KEY, interval=BarIntervalV2.M15)
    assert history is not None
    history_head = {"state_json": json.dumps(history.to_dict()), "state_ref": history.content_hash}
    history_restored = False
    with OpsSupervisorV2(tmp_path / "ops.sqlite", port, clock_ns=time.time_ns) as supervisor:
        supervisor.run_once()
        repository = supervisor.repository
        assert repository is not None
        from atlas.v2.product import resource_sample

        def sample_storage(elapsed_s: float) -> None:
            storage_samples.append({"elapsed_s": elapsed_s,
                "sqlite_bytes": Path(repository.path).stat().st_size,
                "wal_bytes": Path(str(repository.path) + "-wal").stat().st_size,
                "raw_archive_bytes": sum(path.stat().st_size for path in tmp_path.rglob("*")
                    if path.suffix in (".arrow", ".parquet")),
                "book_frames": [len(book._frames) for book in port._stream_books.values() if book is not None],
                "book_levels": [[len(book.bids), len(book.asks)]
                    for book in port._stream_books.values() if book is not None],
                "resource": resource_sample(tmp_path)})

        sample_storage(0)
        original_register = repository.register_artifact

        def observed_register(*args: Any, **kwargs: Any) -> Any:
            writer_threads.add(threading.get_ident())
            return original_register(*args, **kwargs)

        monkeypatch.setattr(repository, "register_artifact", observed_register)

        def produce() -> None:
            workload = MixedWorkload()
            start = time.monotonic()
            for ordinal in range(frame_count):
                target = start + ordinal / 160
                if stop.wait(max(0, target - time.monotonic())):
                    return
                producer_lateness.append(max(0, time.monotonic() - target))
                frame = workload.frame(time.time_ns())
                expected.append(frame)
                if not stream.handoff.offer(frame):
                    return

        producer = threading.Thread(target=produce, name="s38-fake-public-producer", daemon=True)
        offered_started_ns = time.time_ns()
        producer.start()
        started = time.monotonic()
        try:
            helper.acquire(now_ns=time.time_ns(), service=lambda: port.service_public_stream(repository))
            next_rest = time.monotonic() + 1
            while producer.is_alive() and not stream.handoff.snapshot().overflowed:
                if time.monotonic() - started >= len(storage_samples) * 30:
                    sample_storage(time.monotonic() - started)
                if not history_restored and time.monotonic() - started >= min(10, seconds / 2):
                    restored = active_history._decode_head(repository, history_head, key=KEY,
                        interval=BarIntervalV2.M15, service=lambda: port.service_public_stream(repository))
                    assert restored is not None and restored.content_hash == history.content_hash
                    history_restored = True
                if time.monotonic() >= next_rest and time.monotonic() - started < seconds - 6:
                    helper.acquire(now_ns=time.time_ns(), service=lambda: port.service_public_stream(repository))
                    next_rest = time.monotonic() + 1
                port.service_public_stream(repository)
                # Match the installed supervisor's 20ms idle polling. A busy
                # spin would consume a core and test an unrelated scheduler.
                time.sleep(0.02)
            producer.join(timeout=1)
            deadline = time.monotonic() + 5
            while stream.handoff.snapshot().queue_items and time.monotonic() < deadline:
                port.service_public_stream(repository)
                time.sleep(0.02)
            status = stream.handoff.snapshot()
            elapsed = time.monotonic() - started
            sample_storage(elapsed)
            print(json.dumps({"frames": len(expected), "elapsed_s": elapsed,
                   "high_water_items": status.high_water_items, "rejected": status.frames_rejected,
                   "max_producer_lateness_s": max(producer_lateness, default=0),
                   "acquisition": helper.status(), "history_rows_restored": 1200 if history_restored else 0,
                   "storage_samples": storage_samples}, sort_keys=True))
            assert not status.overflowed, "actual writer throughput did not sustain the declared 160 fps"
            assert len(expected) == status.frames_drained == frame_count
            assert status.queue_items == 0 and status.frames_rejected == 0
            assert max(producer_lateness, default=0) < 1.0, "host did not deliver the declared workload on time"
            assert writer_threads == {threading.get_ident()}
            assert history_restored
            rows = transport_rows(repository, tmp_path)
            assert [row["raw_payload_bytes"] for row in rows] == [frame.raw_payload_bytes for frame in expected]
            assert_current_reports(repository, book_warm=seconds >= 32)
            # Initial reports correctly lack receipt evidence. Once every
            # channel has arrived, inspect the whole run, not just its end.
            current_reports = [entry.metadata["report"] for entry in
                repository.artifact_entries("PublicStreamContinuityReportV1")
                if entry.available_at_ns >= offered_started_ns + 1_000_000_000]
            assert current_reports and all(report["source_current"] and report["transport_received"]
                                           for report in current_reports)
        finally:
            stop.set()
            producer.join(timeout=1)
            helper.close(timeout_s=0.1)


def test_failed_interpretation_retains_raw_and_does_not_flood_idle_writer(tmp_path, monkeypatch) -> None:
    clock = [NOW_NS]
    stream = QueueStream(lambda: clock[0])
    port = production.create_bybit_public_ws_port(
        public_source=FixturePublicSource(), public_stream_source=stream, clock_ns=lambda: clock[0],
    )
    with OpsSupervisorV2(tmp_path / "ops.sqlite", port, clock_ns=lambda: clock[0]) as supervisor:
        supervisor.run_once()
        repository = supervisor.repository
        assert repository is not None
        frame = MixedWorkload().frame(NOW_NS + 1)
        assert stream.handoff.offer(frame)
        clock[0] += 250_000_000

        def fail_interpretation(*args: Any, **kwargs: Any) -> None:
            raise ValueError("offline interpretation fault")

        monkeypatch.setattr(port, "_persist_public_stream_batch", fail_interpretation)
        supervisor.service_public_stream()
        assert port._stream_ingestion_failed and stream.closed
        assert transport_rows(repository, tmp_path)[0]["raw_payload_bytes"] == frame.raw_payload_bytes
        failures = repository.artifact_entries("OpsPublicStreamServiceFailureV1")
        assert len(failures) == 1
        for _ in range(20):
            clock[0] += 20_000_000
            supervisor.service_public_stream()
        assert repository.artifact_entries("OpsPublicStreamServiceFailureV1") == failures
        with pytest.raises(RuntimeError, match="RESTART"):
            port._collect_public_stream_evidence(repository, now_ns=clock[0])
