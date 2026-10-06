"""Accelerated retention regression using the installed public composition.

The receipt clock advances 128 and 614.4 seconds while 20,480 and 24,576
real archived frames are processed. This measures storage/retention semantics, not hardware throughput
or a 48-hour live qualification; S38's independent actual-wall probe remains
the capacity test. Samples retain raw archive and operational growth separately.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import replace
from pathlib import Path

import pytest

from atlas.v2._serialization import canonical_json
from atlas.v2.data.microstructure import LIVE_BOOK_MAX_FRAMES, LIVE_BOOK_MAX_LEVELS
from atlas.v2.data.public_evidence_checkpoint import (
    BOOK_CHECKPOINT_TYPE,
    CONTINUITY_CHECKPOINT_TYPE,
    decode_continuity_checkpoint,
)
from atlas.v2.data.public_stream_continuity import MAX_OBSERVATION_REPLAY_CACHE, MAX_TRADE_ID_CACHE
from atlas.v2.product import resource_sample
from atlas.v2.runtime import production
from atlas.v2.runtime.ops_supervisor import OpsSupervisorV2
from atlas.v2.science.tuning_export import TuningRunIdentityV1, export_tuning_snapshot

from .test_s38_sustained_public_stream import (
    NOW_NS,
    FixturePublicSource,
    MixedWorkload,
    QueueStream,
    assert_current_reports,
    transport_rows,
)

FRAME_COUNT = 20_480
FRAME_INTERVAL_NS = 6_250_000
BATCH_SIZE = 128


DeepWorkload = MixedWorkload


class RestartPublicSource(FixturePublicSource):
    def bootstrap_products(self, *, now_ns):
        # Genuine Bybit metadata translation uses receipt as effective time.
        # Keep the registry's exact key/effective-time conflict check intact.
        self.products = tuple(replace(product, effective_at_ns=now_ns)
                              for product in super().bootstrap_products(now_ns=now_ns))
        return self.products


def _size(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


def _sample(repository, port, root, frame_count, interval):
    rows = repository._connection.execute(
        "SELECT artifact_type,count(*),sum(length(metadata_json)),max(length(metadata_json)) "
        "FROM artifact_index GROUP BY artifact_type",
    ).fetchall()
    checkpoint = repository.checkpoint()
    return {
        "frames": frame_count,
        "virtual_elapsed_ns": frame_count * interval,
        "artifact_types": {row[0]: {"count": row[1], "metadata_bytes": row[2],
                                     "max_metadata_bytes": row[3]} for row in rows},
        "compact_locator_counts": {str(row[0]): row[1] for row in repository._connection.execute(
            "SELECT kind,count(*) FROM public_stream_archive_locator_v1 GROUP BY kind")},
        "decoded_cache_chunks": len(repository._public_index_cache),
        "decoded_cache_arrow_bytes": sum(value[1] for value in repository._public_index_cache.values()),
        "archive_segments": len(tuple(root.rglob("*.arrow"))),
        "sqlite_bytes": _size(root / "ops.sqlite"),
        "wal_bytes": _size(root / "ops.sqlite-wal"),
        "wal_checkpoint": list(checkpoint),
        "raw_archive_bytes": sum(path.stat().st_size for path in root.rglob("*") if path.suffix in (".parquet", ".arrow")),
        "book_frames": [len(book._frames) for book in port._stream_books.values() if book is not None],
        "book_seen": [len(book._seen) for book in port._stream_books.values() if book is not None],
        "book_levels": [[len(book.bids), len(book.asks)]
                        for book in port._stream_books.values() if book is not None],
        "trade_cache_items": [len(tracker.state.trade_identity_cache)
                              for tracker in port._stream_trackers.values()],
        "replay_cache_items": [len(tracker.state.observation_replay_cache)
                               for tracker in port._stream_trackers.values()],
        "process_resource": resource_sample(root),
    }


@pytest.mark.parametrize("frame_count,interval", [(FRAME_COUNT, FRAME_INTERVAL_NS), (24_576, 25_000_000)],
                         ids=["160fps-128seconds", "40fps-614seconds"])
def test_sustained_archived_stream_keeps_compact_lineage_and_linear_operational_growth(
        tmp_path, monkeypatch, frame_count, interval):
    clock = [NOW_NS]
    stream = QueueStream(lambda: clock[0])
    port = production.create_bybit_public_ws_port(
        public_source=RestartPublicSource(), public_stream_source=stream, clock_ns=lambda: clock[0],
    )
    workload = DeepWorkload()
    samples = []
    writer_threads = set()
    identity = TuningRunIdentityV1("s39-accelerated-storage", "a" * 64, "b" * 40, NOW_NS)
    with OpsSupervisorV2(tmp_path / "ops.sqlite", port, clock_ns=lambda: clock[0]) as supervisor:
        supervisor.run_once()
        repository = supervisor.repository
        assert repository is not None
        original = repository.register_artifacts

        def observed_register(entries):
            writer_threads.add(threading.get_ident())
            return original(entries)

        monkeypatch.setattr(repository, "register_artifacts", observed_register)
        for start in range(0, frame_count, BATCH_SIZE):
            for ordinal in range(start, start + BATCH_SIZE):
                frame = workload.frame(NOW_NS + (ordinal + 1) * interval)
                assert stream.handoff.offer(frame)
            clock[0] = NOW_NS + (start + BATCH_SIZE) * interval
            # Repeated service leaves no batch pending even on a slow test
            # machine; this is the deterministic semantic workload, not a
            # claim that this host can sustain 160 actual-wall fps.
            for _ in range(4):
                port.service_public_stream(repository)
                if stream.handoff.snapshot().queue_items == 0:
                    break
            assert stream.handoff.snapshot().queue_items == 0
            if (start + BATCH_SIZE) % (frame_count // 4) == 0:
                samples.append(_sample(repository, port, tmp_path, start + BATCH_SIZE, interval))

        clock[0] += 250_000_000
        port._collect_public_stream_evidence(repository, now_ns=clock[0])
        status = stream.handoff.snapshot()
        assert status.frames_drained == frame_count
        assert status.high_water_items == BATCH_SIZE
        assert status.frames_rejected == 0 and not status.overflowed
        assert writer_threads == {threading.get_ident()}
        assert_current_reports(repository, book_warm=True)

        reports = repository.artifact_entries("PublicStreamContinuityReportV1")
        assert len(reports) > 250
        assert max(len(canonical_json(entry.metadata).encode()) for entry in reports) < 16_384
        bbo_refs = [entry.metadata["report"]["latest_valid_bbo"]["input_refs"]
                    for entry in reports if entry.metadata["report"]["latest_valid_bbo"] is not None]
        assert bbo_refs and all(len(refs) == 2 for refs in bbo_refs)
        for entry in repository.artifact_entries(CONTINUITY_CHECKPOINT_TYPE):
            assert len(canonical_json(entry.metadata).encode()) < 8192
            state = decode_continuity_checkpoint(entry)
            assert not state.trade_identity_cache and not state.observation_replay_cache
        assert not repository.artifact_entries("PublicStreamContinuityStateV1")
        assert set(port._stream_pending_book_transports) <= {key for key in port._stream_trackers
                                                            if key[2].startswith("orderbook.")}
        assert all(book is None for key, book in port._stream_books.items() if key[2].startswith("publicTrade."))
        for entry in repository.artifact_entries(BOOK_CHECKPOINT_TYPE):
            assert len(canonical_json(entry.metadata).encode()) < 8192
            assert len(entry.metadata["checkpoint"]["archive_checkpoint_refs"]) <= 32
        for sample in samples:
            assert max(sample["book_frames"]) <= LIVE_BOOK_MAX_FRAMES
            assert max(sample["book_seen"]) <= LIVE_BOOK_MAX_FRAMES
            assert max(max(levels) for levels in sample["book_levels"]) <= LIVE_BOOK_MAX_LEVELS
            assert max(sample["trade_cache_items"]) <= MAX_TRADE_ID_CACHE
            assert max(sample["replay_cache_items"]) <= MAX_OBSERVATION_REPLAY_CACHE
            assert sample["wal_checkpoint"][0] == 0
            assert sample["decoded_cache_chunks"] <= 8
            assert sample["decoded_cache_arrow_bytes"] <= 32 * 1024 * 1024
            assert all(levels == [50, 50] for levels in sample["book_levels"])
        # Full trade caches and book windows must have been reached. The final
        # windows plateau rather than retain the entire current epoch.
        assert max(samples[-1]["trade_cache_items"]) == MAX_TRADE_ID_CACHE
        assert max(samples[-1]["book_frames"]) < 2000
        assert samples[-1]["book_frames"] == samples[-2]["book_frames"]
        assert samples[-1]["raw_archive_bytes"] > samples[0]["raw_archive_bytes"] > 0
        metadata_totals = [sum(value["metadata_bytes"] for value in sample["artifact_types"].values())
                           for sample in samples]
        increments = [b - a for a, b in zip(metadata_totals, metadata_totals[1:], strict=False)]
        assert min(increments) > 0
        assert max(increments) < min(increments) * 1.2
        continuity_totals = [sample["artifact_types"]["PublicStreamContinuityReportV1"]["metadata_bytes"]
                            for sample in samples]
        assert (continuity_totals[-1] - continuity_totals[-2]
                < (continuity_totals[1] - continuity_totals[0]) * 1.2)

        rows = transport_rows(repository, tmp_path)
        assert len(rows) == frame_count
        replay_workload = DeepWorkload()
        for ordinal, row in enumerate(rows):
            receipt = NOW_NS + (ordinal + 1) * interval
            expected = replay_workload.frame(receipt)
            assert row["raw_payload_bytes"] == expected.raw_payload_bytes
            assert row["received_at_ns"] == expected.received_at_ns
        for book in port._stream_books.values():
            if book is not None:
                old = book.feature(cutoff_ns=NOW_NS + 1_000_000_000)
                assert old.bbo is None
                assert old.missing_reason == "NOT_ESTIMABLE_REQUIRES_IMMUTABLE_BOOK_REPLAY"

        before_changes = repository._connection.total_changes
        pages = []
        through = 0
        for _ in range(64):
            started = time.monotonic()
            # S40 seals an exact validated prefix rather than holding a long
            # reader open. Consume every page at the unchanged product budget.
            exported = export_tuning_snapshot(repository.path, tmp_path / "reports", identity,
                                              cutoff_ns=clock[0])
            assert exported["after_rowid"] == through
            through = exported["through_rowid"]
            pages.append({"seconds": time.monotonic() - started, "rows": exported["rows_written"],
                          "through_rowid": through, "budget_yielded": exported["budget_yielded"]})
            assert exported["snapshot_budget_seconds"] == 10
            assert not exported["validation_failures"]
            assert not exported["blocked_future_evidence"]
            if not exported["has_more"]:
                break
        assert not exported["has_more"]
        assert repository._connection.total_changes == before_changes
        metrics = {"scope": "ACCELERATED_OFFLINE_STORAGE_SEMANTICS", "frames": frame_count,
                   "virtual_elapsed_ns": frame_count * interval,
                   "queue_high_water_items": status.high_water_items, "frames_rejected": status.frames_rejected,
                   "samples": samples, "export_pages": pages,
                   "validation_failures": exported["validation_failures"]}
        (tmp_path / "s39-storage-metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
        print(json.dumps(metrics, sort_keys=True))


def test_compact_restart_retains_real_overload_and_requires_new_book_snapshot(tmp_path):
    clock = [NOW_NS]
    database = tmp_path / "ops.sqlite"
    stream = QueueStream(lambda: clock[0])
    port = production.create_bybit_public_ws_port(
        public_source=RestartPublicSource(), public_stream_source=stream, clock_ns=lambda: clock[0],
    )
    with OpsSupervisorV2(database, port, clock_ns=lambda: clock[0]) as supervisor:
        supervisor.run_once()
        repository = supervisor.repository
        assert repository is not None
        workload = MixedWorkload()
        for ordinal in range(512):
            assert stream.handoff.offer(workload.frame(NOW_NS + ordinal + 1))
        assert not stream.handoff.offer(workload.frame(NOW_NS + 513))
        clock[0] += 100_000_000
        port.service_public_stream(repository)
        reports = repository.artifact_entries("PublicStreamContinuityReportV1")
        assert any("QUEUE_OVERFLOW_LOCAL_DATA_LOSS" in entry.metadata["report"]["gap_reason_codes"]
                   for entry in reports)
        assert repository.artifact_entries(CONTINUITY_CHECKPOINT_TYPE)

    clock[0] += 1_000_000_000
    restarted_stream = QueueStream(lambda: clock[0])
    restarted_port = production.create_bybit_public_ws_port(
        public_source=RestartPublicSource(), public_stream_source=restarted_stream, clock_ns=lambda: clock[0],
    )
    with OpsSupervisorV2(database, restarted_port, clock_ns=lambda: clock[0]) as supervisor:
        supervisor.run_once()
        repository = supervisor.repository
        assert repository is not None
        latest = [entry.metadata["report"] for entry in repository.artifact_entries("PublicStreamContinuityReportV1")
                  if entry.available_at_ns >= clock[0]]
        assert latest
        assert all(not report["book_sequence_valid"] and not report["trade_completeness_proven"]
                   for report in latest)
        assert all(report["gap_count"] > 0 for report in latest)
        assert any("QUEUE_OVERFLOW_LOCAL_DATA_LOSS" in report["gap_reason_codes"] for report in latest)
        assert all(report["latest_valid_bbo"] is None for report in latest)
