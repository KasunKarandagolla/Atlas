"""Independent FIFO raw replay of compact book lineage, including intra-batch resets."""
import json
from dataclasses import replace

import pytest

from atlas.v2._serialization import json_value, sha256_json
from atlas.v2.data.health import PublicSourceHealthV2
from atlas.v2.data.microstructure import L2DeltaV2, L2SequenceFaultV2, L2SnapshotV2, SequenceValidBookV2
from atlas.v2.data.public_archive_extents import read_extent
from atlas.v2.data.public_evidence_checkpoint import validate_book_checkpoint
from atlas.v2.data.public_microstructure_ws import CapturedPublicFrameV2, parse_bybit_orderbook_frame
from atlas.v2.instruments import InstrumentKeyV2, VenueV2
from atlas.v2.runtime import production
from atlas.v2.runtime.ops_supervisor import OpsSupervisorV2

from .test_s38_sustained_public_stream import NOW_NS, MixedWorkload, QueueStream, _frame
from .test_session039_long_run_public_evidence import RestartPublicSource


def test_continuity_projection_matches_full_s4_qualification_without_flow_work(monkeypatch):
    from atlas.v2.data import microstructure

    from .test_s38_sustained_public_stream import _S32

    key = _S32["_contract"]("BTCUSDT").key
    book = SequenceValidBookV2(instrument=key, source_id="BYBIT_PUBLIC_WS",
                              channel="orderbook.50.BTCUSDT", sequence_semantics="BYBIT_U")

    def compare(cut):
        feature = book.feature(cutoff_ns=cut)
        view = book.continuity_view(cutoff_ns=cut)
        assert (view.sequence_state, view.bbo, view.data_age_ns, view.missing_reason, view.input_refs) == (
            feature.sequence_state, feature.bbo, feature.data_age_ns, feature.missing_reason, feature.input_refs)

    compare(NOW_NS)  # cold
    workload = MixedWorkload()
    for i in range(6400):
        at = NOW_NS + i * 6_250_000
        frame = workload.frame(at)
        if frame.channel != book.channel:
            continue
        event = parse_bybit_orderbook_frame(frame, instrument=key, declared_depth=50,
            source_health="HEALTHY_CURRENT", source_health_ref="a" * 64, processed_at_ns=at)
        if isinstance(event, L2SnapshotV2):
            book.apply_snapshot(event)
        else:
            book.apply_delta(event)
        book.compact_live_state(as_of_ns=at)
        if i == 0:
            compare(at)  # warming
    cut = NOW_NS + 6400 * 6_250_000
    assert len(book._frames) > 1800 and len(book.bids) == 50
    compare(cut)  # mature, current

    def forbid_metrics(*args, **kwargs):
        raise AssertionError("continuity must not compute S4 flow metrics")

    with monkeypatch.context() as patch:
        patch.setattr(microstructure, "_replenishment_proxy", forbid_metrics)
        patch.setattr(book, "_windows", forbid_metrics)
        patch.setattr(book, "_ofi_windows", forbid_metrics)
        assert book.continuity_view(cutoff_ns=cut).bbo is not None
    compare(NOW_NS)  # old cutoff requires retained raw replay
    compare(cut + 2_000_000_000)  # stale
    book.disconnect(cut + 3_000_000_000)
    compare(cut + 3_000_000_000)
    book.reconnect(cut + 4_000_000_000)
    compare(cut + 4_000_000_000)


def replay_checkpoint(repository, ref):
    """Audit retained raw prefixes; intentionally separate from the live writer.

    Historical audit is bounded explicitly by a caller budget; the active path
    validates only immediate links. Replay uses FIFO transport, not typed chunk
    sorting, so a reset with a lower update ID cannot change causal order.
    """
    chain = []
    visited = set()
    while ref is not None:
        assert len(chain) < 4096 and ref not in visited
        visited.add(ref)
        entry = repository.get_artifact(ref)
        assert entry is not None
        body = validate_book_checkpoint(repository, entry, as_of_ns=entry.available_at_ns)
        chain.append(body)
        ref = body["prior_checkpoint_ref"]
    first = chain[-1]
    book = SequenceValidBookV2(instrument=InstrumentKeyV2.from_dict(first["instrument"]),
                               source_id=first["source_id"], channel=first["channel"], sequence_semantics="BYBIT_U")
    last_epoch = None
    seen_transports = set()
    for checkpoint in reversed(chain):
        controls = [json_value(repository.get_artifact(ref).metadata["observation"])
                    for ref in checkpoint["control_event_refs"]]
        applied_controls = set()

        def apply_control(index, *, before_frame=False, controls=controls, applied_controls=applied_controls):
            nonlocal book, last_epoch
            if index in applied_controls:
                return
            applied_controls.add(index)
            observation = controls[index]
            kind = observation["kind"]
            if kind == "METADATA_REVISION_CHANGE":
                book = SequenceValidBookV2(instrument=book.instrument, source_id=book.source_id,
                    channel=book.channel, sequence_semantics=book.sequence_semantics)
            if kind in ("RECONNECT", "CONTROLLER_RESTART", "METADATA_REVISION_CHANGE"):
                book.reconnect(observation["observed_at_ns"])
                last_epoch = observation["epoch_id"]
            elif kind in ("DISCONNECT", "QUEUE_OVERFLOW", "CONNECTION_ERROR"):
                book.disconnect(observation["available_at_ns"] if before_frame else observation["observed_at_ns"])

        for index, observation in enumerate(controls):
            if observation["kind"] in ("CONTROLLER_RESTART", "METADATA_REVISION_CHANGE"):
                apply_control(index, before_frame=True)
        for typed_ref in checkpoint["archive_checkpoint_refs"]:
            typed = repository.get_artifact(typed_ref)
            rows = read_extent(repository, typed.metadata["archive_extent_ref"]).to_pylist()
            metadata_rows = [{key: value for key, value in row.items()
                              if key not in ("raw_payload_bytes", "instrument_json")} for row in rows]
            assert sha256_json({"archive_type": "L2RawFrameChunkV2", "frames": metadata_rows}) == typed.content_hash
        for transport_ref in checkpoint["transport_batch_refs"]:
            assert transport_ref not in seen_transports
            seen_transports.add(transport_ref)
            transport = repository.get_artifact(transport_ref)
            rows = read_extent(repository, transport.metadata["batch"]["archive_extent_ref"]).to_pylist()
            health_entries = [repository.get_artifact(ref) for ref in checkpoint["frame_health_refs"]]
            headers = [{key: value for key, value in row.items() if key != "raw_payload_bytes"} for row in rows]
            assert sha256_json({"version": "PublicStreamTransportBatchV1", "frames": headers}) == transport.metadata["batch"]["chunk_id"]
            for ordinal, row in enumerate(rows):
                assert row["fifo_index"] == ordinal
                if row["channel"] != checkpoint["channel"]:
                    continue
                matching = [entry for entry in health_entries
                            if entry.metadata["transport"]["transport_batch_ref"] == transport_ref
                            and entry.metadata["transport"]["connection_attempt"] == row["connection_epoch"]]
                assert len(matching) == 1
                health_entry = matching[0]
                health = PublicSourceHealthV2.from_dict(json_value(health_entry.metadata["health"]))
                epoch = health_entry.metadata["transport"]["epoch_id"]
                for index, observation in enumerate(controls):
                    if observation["kind"] == "RECONNECT" and observation["epoch_id"] == epoch:
                        for pending in range(index + 1):
                            apply_control(pending, before_frame=True)
                if last_epoch is not None and epoch != last_epoch:
                    book.reconnect(health.available_at_ns)
                last_epoch = epoch
                frame = CapturedPublicFrameV2(VenueV2(row["venue"]), row["source_id"], row["channel"],
                    row["raw_payload_bytes"], row["raw_payload_hash"], row["received_at_ns"],
                    row["available_at_ns"], row["connection_epoch"])
                event = parse_bybit_orderbook_frame(frame, instrument=book.instrument, declared_depth=50,
                    source_health=health.state.value, source_health_ref=health.content_hash,
                    processed_at_ns=health.available_at_ns)
                if isinstance(event, L2SnapshotV2):
                    book.apply_snapshot(event)
                elif isinstance(event, L2DeltaV2):
                    book.apply_delta(event)
                elif isinstance(event, L2SequenceFaultV2):
                    book.apply_fault(event)
            book.compact_live_state(as_of_ns=health.available_at_ns)
        for index in range(len(controls)):
            apply_control(index)
        # Empty publications still compact at their source collection cutoff.
        book.compact_live_state(as_of_ns=checkpoint["as_of_ns"])
        assert sha256_json(book.evidence_state()) == checkpoint["book_state_hash"]
        feature = book.feature(cutoff_ns=checkpoint["as_of_ns"])
        assert (list(feature.bbo) if feature.bbo else None) == checkpoint["bbo"]
    return book


@pytest.mark.parametrize("reset", [False, True])
def test_exact_bbo_and_terminal_state_replay_from_retained_fifo_lineage(tmp_path, reset):
    clock = [NOW_NS]
    stream = QueueStream(lambda: clock[0])
    port = production.create_bybit_public_ws_port(public_source=RestartPublicSource(),
        public_stream_source=stream, clock_ns=lambda: clock[0])
    with OpsSupervisorV2(tmp_path / "ops.sqlite", port, clock_ns=lambda: clock[0]) as supervisor:
        supervisor.run_once()
        repository = supervisor.repository
        workload = MixedWorkload()
        # A long-enough book warmup and many links; the reset deliberately
        # lowers update IDs within one batch where typed-archive sorting differs.
        for batch in range(45):
            for offset in range(32):
                receipt = NOW_NS + (batch * 32 + offset + 1) * 25_000_000
                frame = workload.frame(receipt)
                if reset and batch == 25 and offset == 16:
                    payload = json.loads(frame.raw_payload_bytes)
                    if payload.get("type") is not None:
                        payload["type"] = "snapshot"
                        payload["data"]["u"] = 1
                        frame = _frame(frame.channel, payload, received_at_ns=receipt)
                assert stream.handoff.offer(frame)
            clock[0] = NOW_NS + (batch + 1) * 800_000_000
            port._collect_public_stream_evidence(repository, now_ns=clock[0])
        for ref in port._stream_book_checkpoint_refs.values():
            replayed = replay_checkpoint(repository, ref)
            assert len(replayed._frames) <= 4096
        trades = repository.artifact_entries("PublicStreamTradeObservationIndexV1")
        assert trades
        for entry in trades:
            assert repository.get_artifact(entry.artifact_ref) == entry
        frames = repository.artifact_entries("PublicStreamFrameIndexV1")
        assert frames and repository.get_artifact(frames[0].artifact_ref) == frames[0]
        assert not repository._connection.execute("SELECT 1 FROM artifact_index WHERE artifact_type IN "
            "('PublicStreamTradeObservationIndexV1','PublicStreamFrameIndexV1') LIMIT 1").fetchone()
        assert repository.public_stream_trade_entries(replayed.instrument, source_id=production.BYBIT_PUBLIC_WS_SOURCE_ID_V1,
            event_from_ns=NOW_NS, cutoff_ns=clock[0], limit=1)


def test_compact_report_transport_is_exact_and_invalid_reference_fails_closed(tmp_path):
    from atlas.v2.data.public_evidence_checkpoint import resolve_report_transport
    from atlas.v2.memory.repository import ArtifactIndexEntryV2
    from atlas.v2.science.tuning_export import _validated_row

    clock = [NOW_NS]
    stream = QueueStream(lambda: clock[0])
    port = production.create_bybit_public_ws_port(public_source=RestartPublicSource(),
        public_stream_source=stream, clock_ns=lambda: clock[0])
    with OpsSupervisorV2(tmp_path / "ops.sqlite", port, clock_ns=lambda: clock[0]) as supervisor:
        supervisor.run_once()
        repository = supervisor.repository
        for entry in repository.artifact_entries("PublicStreamContinuityReportV1"):
            transport = resolve_report_transport(repository, entry.metadata, available_at_ns=entry.available_at_ns)
            row = _validated_row(repository, entry)
            assert transport["queue_capacity_items"] == 512
            assert json.loads(row["metrics_json"])["transport.high_water_items"] == transport["high_water_items"]
            bad_metadata = json_value(entry.metadata)
            bad_metadata["transport_ref"] = "f" * 64
            corrupted = replace(entry, metadata=bad_metadata)
            with pytest.raises(ValueError, match="transport binding"):
                _validated_row(repository, corrupted)
        workload = MixedWorkload()
        assert stream.handoff.offer(workload.frame(NOW_NS + 1))
        clock[0] += 100_000_000
        port._collect_public_stream_evidence(repository, now_ns=clock[0])
        frame = repository.artifact_entries("PublicStreamFrameIndexV1")[0]
        repository.register_artifact(frame)  # exact duplicate stays idempotent
        with pytest.raises(ValueError, match="identity conflicts"):
            repository.register_artifact(ArtifactIndexEntryV2(frame.artifact_ref, "WrongType", frame.content_hash,
                frame.created_at_ns, frame.available_at_ns, frame.metadata))


def test_book_checkpoint_publication_follows_real_computation_clock(tmp_path):
    from atlas.v2.data.public_evidence_checkpoint import (
        decode_continuity_checkpoint,
        persist_book_checkpoint,
        persist_continuity_checkpoint,
    )
    from atlas.v2.data.public_stream_continuity import PublicStreamContinuityTrackerV1
    from atlas.v2.memory.repository import OpsRepository

    ticks = iter([NOW_NS + 10, NOW_NS + 80])
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        product = RestartPublicSource().bootstrap_products(now_ns=NOW_NS)[0]
        book = SequenceValidBookV2(instrument=product.key, source_id=production.BYBIT_PUBLIC_WS_SOURCE_ID_V1,
            channel="orderbook.50." + product.key.native_symbol, sequence_semantics="BYBIT_U")
        ref = persist_book_checkpoint(repository, book, epoch_id="cold", metadata_ref=product.metadata_ref,
            as_of_ns=NOW_NS, prior_ref=None, archive_refs=(), transport_refs=(), frame_health_refs=(),
            clock_ns=lambda: next(ticks))
        entry = repository.get_artifact(ref)
        assert entry.created_at_ns == NOW_NS + 10
        assert entry.available_at_ns == NOW_NS + 80
        with pytest.raises(ValueError, match="chronology"):
            validate_book_checkpoint(repository, entry, as_of_ns=NOW_NS + 79)
        body = validate_book_checkpoint(repository, entry, as_of_ns=NOW_NS + 80)
        assert body["as_of_ns"] == NOW_NS
        assert body["computation_finished_ns"] == NOW_NS + 80
        tracker = PublicStreamContinuityTrackerV1(instrument=product.key, source_id=book.source_id,
            channel=book.channel, metadata_ref=product.metadata_ref, epoch_id="cold")
        ticks = iter([NOW_NS + 100, NOW_NS + 200])
        continuity_ref = persist_continuity_checkpoint(repository, tracker.to_state(), available_at_ns=NOW_NS,
            prior_ref=None, transport_ref=None, clock_ns=lambda: next(ticks))
        continuity = repository.get_artifact(continuity_ref)
        assert continuity.created_at_ns == NOW_NS + 100 and continuity.available_at_ns == NOW_NS + 200
        assert decode_continuity_checkpoint(continuity).instrument == product.key


def test_disconnect_and_new_connection_snapshot_replay_preserves_recovery(tmp_path):
    class ReconnectingStream(QueueStream):
        attempt = 1

        def status(self):
            status = super().status()
            status.attempt_count = self.attempt
            status.reconnect_count = self.attempt - 1
            return status

    clock = [NOW_NS]
    stream = ReconnectingStream(lambda: clock[0])
    port = production.create_bybit_public_ws_port(public_source=RestartPublicSource(),
        public_stream_source=stream, clock_ns=lambda: clock[0])
    with OpsSupervisorV2(tmp_path / "ops.sqlite", port, clock_ns=lambda: clock[0]) as supervisor:
        supervisor.run_once()
        workload = MixedWorkload()
        for offset in range(32):
            assert stream.handoff.offer(workload.frame(NOW_NS + offset + 1))
        clock[0] += 100_000_000
        port._collect_public_stream_evidence(supervisor.repository, now_ns=clock[0])
        clock[0] += 100_000_000
        stream.handoff.observe_disconnected(clock[0])
        port._collect_public_stream_evidence(supervisor.repository, now_ns=clock[0])
        for ref in port._stream_book_checkpoint_refs.values():
            assert replay_checkpoint(supervisor.repository, ref).last_received_at_ns is None

        clock[0] += 100_000_000
        stream.attempt = 2
        stream.handoff.observe_connected(clock[0])
        # New connection begins with genuine fresh snapshots, not old deltas.
        workload = MixedWorkload()
        for offset in range(32):
            assert stream.handoff.offer(replace(workload.frame(clock[0] + offset + 1), connection_epoch=2))
        clock[0] += 100_000_000
        port._collect_public_stream_evidence(supervisor.repository, now_ns=clock[0])
        for ref in port._stream_book_checkpoint_refs.values():
            replayed = replay_checkpoint(supervisor.repository, ref)
            assert replayed.last_received_at_ns is not None
            assert replayed.feature(cutoff_ns=clock[0]).missing_reason is not None  # warmup remains required


def test_live_book_capacity_overload_keeps_raw_evidence_and_fails_closed(tmp_path, monkeypatch):
    import atlas.v2.data.microstructure as microstructure

    from .test_s38_sustained_public_stream import transport_rows

    monkeypatch.setattr(microstructure, "LIVE_BOOK_MAX_FRAMES", 8)
    clock = [NOW_NS]
    stream = QueueStream(lambda: clock[0])
    port = production.create_bybit_public_ws_port(public_source=RestartPublicSource(),
        public_stream_source=stream, clock_ns=lambda: clock[0])
    with OpsSupervisorV2(tmp_path / "ops.sqlite", port, clock_ns=lambda: clock[0]) as supervisor:
        supervisor.run_once()
        workload = MixedWorkload()
        for offset in range(32):
            assert stream.handoff.offer(workload.frame(NOW_NS + offset + 1))
        clock[0] += 100_000_000
        port._collect_public_stream_evidence(supervisor.repository, now_ns=clock[0])
        assert len(transport_rows(supervisor.repository, tmp_path)) == 32
        assert stream.handoff.snapshot().frames_rejected == 0
        for book in port._stream_books.values():
            if book is not None:
                assert not book._frames and not book.bids and not book.asks
                assert book.sequence_state.reason == "LIVE_BOOK_CAPACITY_REQUIRES_SNAPSHOT_RECOVERY"
                assert book.feature(cutoff_ns=clock[0]).bbo is None
        for ref in port._stream_book_checkpoint_refs.values():
            replay_checkpoint(supervisor.repository, ref)
