from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from types import SimpleNamespace

from atlas.v2._serialization import sha256_json
from atlas.v2.data.public_archive_extents import read_public_chunk
from atlas.v2.data.public_microstructure_ws import (
    CapturedPublicFrameV2,
    bybit_btc_eth_linear_topics,
)
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2
from atlas.v2.runtime import production
from atlas.v2.runtime.ops_supervisor import OpsCycleBatchV1, OpsSupervisorV2

BASE_NS = 1_750_000_000_000_000_000
NOW_NS = BASE_NS + 10_000_000


def _contract(symbol: str) -> ProductContractV2:
    key = InstrumentKeyV2(
        VenueV2.BYBIT, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
        symbol, symbol.removesuffix("USDT"), "USDT", "USDT", sha256_json(["s32", symbol]),
    )
    return ProductContractV2(
        key, BASE_NS, BASE_NS, BASE_NS,
        Decimal("1"), Decimal("0.1"), Decimal("0.001"), Decimal("0.001"),
        TradingStatusV2.TRADING, sha256_json(["metadata", symbol]),
    )


def _frame(channel: str, payload: dict, *, received_at_ns: int = BASE_NS + 5_000_000) -> CapturedPublicFrameV2:
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return CapturedPublicFrameV2(
        VenueV2.BYBIT, production.BYBIT_PUBLIC_WS_SOURCE_ID_V1, channel,
        raw, hashlib.sha256(raw).hexdigest(), received_at_ns, received_at_ns, 1,
    )


def _frames() -> tuple[CapturedPublicFrameV2, ...]:
    result = []
    event_ms = BASE_NS // 1_000_000
    for symbol, seq in (("BTCUSDT", 100), ("ETHUSDT", 200)):
        result.append(_frame(
            f"orderbook.50.{symbol}",
            {"topic": f"orderbook.50.{symbol}", "type": "snapshot", "ts": event_ms,
             "cts": event_ms, "data": {"s": symbol, "u": seq, "seq": seq,
                                       "b": [["100", "2"]], "a": [["101", "3"]]}},
        ))
        result.append(_frame(
            f"publicTrade.{symbol}",
            {"topic": f"publicTrade.{symbol}", "type": "snapshot", "ts": event_ms,
             "data": [{"T": event_ms, "s": symbol, "S": "Buy", "v": "0.25",
                       "p": "100.5", "i": f"trade-{symbol}-1"}]},
        ))
    return tuple(result)


class _FakePublicSource:
    required_source_ids: tuple[str, ...] = ()

    def __init__(self) -> None:
        self.products = (_contract("BTCUSDT"), _contract("ETHUSDT"))
        self.repositories = []

    def bootstrap_products(self, *, now_ns: int):
        assert now_ns == NOW_NS or now_ns == NOW_NS + 1
        return self.products

    def collect(self, repository, collector, *, now_ns: int, recovery):
        self.repositories.append(repository)
        assert collector.repository is repository
        return OpsCycleBatchV1((), (), (), (), True, now_ns)


class _FakeStreamSource:
    venue = VenueV2.BYBIT
    topics = bybit_btc_eth_linear_topics()

    def __init__(self, frames: tuple[CapturedPublicFrameV2, ...], *, repeat: bool = False) -> None:
        self.frames = frames
        self.repeat = repeat
        self.started = False
        self.closed = False
        self.drained = False

    def start(self) -> None:
        self.started = True

    def drain(self, *, max_items: int):
        assert 0 < max_items <= 32
        if self.drained and not self.repeat:
            return ()
        self.drained = True
        return self.frames[:max_items]

    def status(self):
        handoff = SimpleNamespace(
            connected=True,
            disconnect_count=0,
            last_disconnect_at_ns=None,
            last_heartbeat_at_ns=NOW_NS,
            last_activity_at_ns=NOW_NS,
            queue_items=0,
            queue_bytes=0,
            max_queue_items=512,
            max_queue_bytes=16_000_000,
            high_water_items=len(self.frames),
            high_water_bytes=sum(len(frame.raw_payload_bytes) for frame in self.frames),
            overflowed=False,
            backpressure=False,
            last_error_code=None,
        )
        return SimpleNamespace(
            state="RUNNING", attempt_count=1, reconnect_count=0, last_error_code=None, handoff=handoff,
        )

    def close(self) -> None:
        self.closed = True


def test_opt_in_stream_archives_two_symbols_under_supervisor_writer_and_stays_unqualified(tmp_path) -> None:
    database = tmp_path / "ops.sqlite"
    source = _FakePublicSource()
    stream = _FakeStreamSource(_frames(), repeat=True)
    cycle_now = [NOW_NS]
    port = production.create_bybit_public_ws_port(
        public_source=source, public_stream_source=stream, clock_ns=lambda: cycle_now[0],
    )
    with OpsSupervisorV2(database, port, clock_ns=lambda: cycle_now[0]) as supervisor:
        run = supervisor.run_once()
        assert run.cycle.source_health_state == "UNKNOWN"
        repository = supervisor.repository
        assert repository is not None
        assert source.repositories == [repository]
        assert stream.started
        assert repository.source_health_sources() == ()
        assert port._collector_recovery is not None
        assert port._collector_recovery.collector.store.records() == ()

        stream_trade_indexes = repository.artifact_entries("PublicStreamTradeObservationIndexV1")
        assert len(stream_trade_indexes) == 2
        assert {entry.metadata["source_id"] for entry in stream_trade_indexes} == {
            production.BYBIT_PUBLIC_WS_SOURCE_ID_V1,
        }
        assert not any(
            entry.metadata.get("source_id") == production.BYBIT_PUBLIC_WS_SOURCE_ID_V1
            for entry in repository.artifact_entries("PublicObservationIndexV2")
        )

        reports = [entry.metadata["report"] for entry in repository.artifact_entries("PublicStreamContinuityReportV1")]
        trade_reports = {report["channel"]: report for report in reports
                         if report["channel"].startswith("publicTrade.")}
        book_reports = {report["channel"]: report for report in reports
                        if report["channel"].startswith("orderbook.")}
        assert set(trade_reports) == {"publicTrade.BTCUSDT", "publicTrade.ETHUSDT"}
        assert set(book_reports) == {"orderbook.50.BTCUSDT", "orderbook.50.ETHUSDT"}
        for report in trade_reports.values():
            assert report["transport_received"]
            assert report["source_current"]
            assert report["observed_trade_evidence"]
            assert report["observed_trade_count"] == 1
            assert report["trade_completeness_proven"] is False
            assert report["strategy_input_qualified"] is False
            assert report["trade_recovery_semantics"]
        for report in book_reports.values():
            assert report["transport_received"]
            assert report["source_current"]
            assert report["book_sequence_valid"] is False  # one fresh snapshot has not met the existing warmup.
            assert report["latest_valid_bbo"] is None

        frame_indexes = repository.artifact_entries("PublicStreamFrameIndexV1")
        assert len(frame_indexes) == 4
        assert {entry.metadata["channel"] for entry in frame_indexes} == set(bybit_btc_eth_linear_topics())
        raw_frame_hashes = {entry.metadata["raw_payload_hash"] for entry in frame_indexes}
        assert raw_frame_hashes == {frame.raw_payload_hash for frame in _frames()}

        archive_root = tmp_path / "ops-observations"
        raw_rows = []
        for entry in stream_trade_indexes:
            rows = read_public_chunk(repository, archive_root, entry.metadata["archive_chunk_id"]).to_pylist()
            raw_rows.extend(rows)
        for row in raw_rows:
            assert row["source_id"] == production.BYBIT_PUBLIC_WS_SOURCE_ID_V1
            assert row["published_at_ns"] is None
            assert row["received_at_ns"] == BASE_NS + 5_000_000
            assert row["ingested_at_ns"] == NOW_NS
            assert row["available_at_ns"] == NOW_NS
            assert hashlib.sha256(row["raw_payload_bytes"]).hexdigest() == row["raw_payload_hash"]

        # Re-reporting unchanged continuity state at a later cutoff is idempotent.
        cycle_now[0] = NOW_NS + 1
        supervisor.run_once()
        assert source.repositories == [repository, repository]
        assert len(repository.artifact_entries("PublicStreamTradeObservationIndexV1")) == 2
        assert len(repository.artifact_entries("PublicStreamFrameIndexV1")) == 4
        assert port._collector_recovery.collector.store.records() == ()

    assert stream.closed


def test_controller_restart_records_unrepaired_trade_gap_and_requires_new_book_snapshot(tmp_path) -> None:
    database = tmp_path / "restart.sqlite"
    first_source = _FakePublicSource()
    first_stream = _FakeStreamSource(_frames())
    with OpsSupervisorV2(
        database,
        production.create_bybit_public_ws_port(
            public_source=first_source, public_stream_source=first_stream, clock_ns=lambda: NOW_NS,
        ),
        clock_ns=lambda: NOW_NS,
    ) as first:
        first.run_once()

    restart_stream = _FakeStreamSource(())
    with OpsSupervisorV2(
        database,
        production.create_bybit_public_ws_port(
            public_source=_FakePublicSource(), public_stream_source=restart_stream,
            clock_ns=lambda: NOW_NS + 1,
        ),
        clock_ns=lambda: NOW_NS + 1,
    ) as restarted:
        restarted.run_once()
        repository = restarted.repository
        assert repository is not None
        reports = [entry.metadata["report"] for entry in repository.artifact_entries("PublicStreamContinuityReportV1")]
        latest = {}
        for report in reports:
            instrument = report["instrument"]
            if report["as_of_ns"] <= NOW_NS + 1:
                identity = (instrument["native_symbol"], report["channel"])
                prior = latest.get(identity)
                if prior is None or report["as_of_ns"] >= prior["as_of_ns"]:
                    latest[identity] = report
        btc_trade = latest[("BTCUSDT", "publicTrade.BTCUSDT")]
        btc_book = latest[("BTCUSDT", "orderbook.50.BTCUSDT")]
        assert btc_trade["observed_trade_evidence"]
        assert btc_trade["trade_completeness_proven"] is False
        assert btc_trade["strategy_input_qualified"] is False
        assert "CONTROLLER_RESTART_UNACKNOWLEDGED_BUFFER_INTERVAL" in btc_trade["gap_reason_codes"]
        assert btc_book["book_sequence_valid"] is False
        assert btc_book["latest_valid_bbo"] is None
        assert any(
            entry.metadata["decision"]["classification"] == "RECOVERY_EPOCH_STARTED"
            for entry in repository.artifact_entries("PublicStreamContinuityEventV1")
        )


def test_point_in_time_contract_revision_rebinds_channel_identity_fail_closed(tmp_path) -> None:
    database = tmp_path / "metadata-revision.sqlite"
    source = _FakePublicSource()
    stream = _FakeStreamSource(_frames())
    cycle_now = [NOW_NS]
    port = production.create_bybit_public_ws_port(
        public_source=source, public_stream_source=stream, clock_ns=lambda: cycle_now[0],
    )
    with OpsSupervisorV2(database, port, clock_ns=lambda: cycle_now[0]) as supervisor:
        supervisor.run_once()
        repository = supervisor.repository
        recovery = port._collector_recovery
        assert repository is not None and recovery is not None

        prior_product = source.products[0]
        revised_key = InstrumentKeyV2(
            VenueV2.BYBIT, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
            "BTCUSDT", "BTC", "USDT", "USDT", sha256_json("s32-btc-revision-two"),
        )
        revised = ProductContractV2(
            revised_key, NOW_NS + 1, NOW_NS + 1, NOW_NS + 1,
            prior_product.base_units_per_contract, prior_product.tick_size,
            prior_product.qty_step, prior_product.min_qty, TradingStatusV2.TRADING,
            sha256_json("s32-btc-metadata-two"),
        )
        recovery.collector.registry.register(revised)
        repository.register_artifact(
            ArtifactIndexEntryV2(
                revised.content_hash, "ProductContractV2", revised.content_hash,
                revised.observed_at_ns, revised.available_at_ns, {"product": revised.to_dict()},
            )
        )
        event_ms = (NOW_NS + 2) // 1_000_000
        revised_trade = _frame(
            "publicTrade.BTCUSDT",
            {"topic": "publicTrade.BTCUSDT", "ts": event_ms,
             "data": [{"T": event_ms, "s": "BTCUSDT", "S": "Sell", "v": "0.1",
                       "p": "100.25", "i": "trade-BTCUSDT-revision-two"}]},
            received_at_ns=NOW_NS + 2,
        )
        stream.frames = (revised_trade,)
        stream.drained = False
        cycle_now[0] = NOW_NS + 3
        supervisor.run_once()

        latest = [
            entry.metadata["report"]
            for entry in repository.artifact_entries("PublicStreamContinuityReportV1")
            if entry.metadata["report"]["channel"] == "publicTrade.BTCUSDT"
        ]
        report = max(latest, key=lambda item: item["as_of_ns"])
        assert report["contract_revision"] == revised_key.contract_revision
        assert report["metadata_ref"] == revised.metadata_ref
        assert report["metadata_current"]
        assert report["observed_trade_count"] == 1
        assert report["trade_completeness_proven"] is False
        assert report["strategy_input_qualified"] is False
        assert "METADATA_REVISION_CHANGED_REQUIRES_FRESH_FEED_STATE" in report["gap_reason_codes"]
        assert len(port._stream_trackers) == 4

        suspended = ProductContractV2(
            revised_key, NOW_NS + 4, NOW_NS + 4, NOW_NS + 4,
            prior_product.base_units_per_contract, prior_product.tick_size,
            prior_product.qty_step, prior_product.min_qty, TradingStatusV2.SUSPENDED,
            sha256_json("s32-btc-metadata-suspended"),
        )
        recovery.collector.registry.register(suspended)
        repository.register_artifact(
            ArtifactIndexEntryV2(
                suspended.content_hash, "ProductContractV2", suspended.content_hash,
                suspended.observed_at_ns, suspended.available_at_ns, {"product": suspended.to_dict()},
            )
        )
        suspended_event_ms = (NOW_NS + 5) // 1_000_000
        stream.frames = (_frame(
            "publicTrade.BTCUSDT",
            {"topic": "publicTrade.BTCUSDT", "ts": suspended_event_ms,
             "data": [{"T": suspended_event_ms, "s": "BTCUSDT", "S": "Buy", "v": "0.1",
                       "p": "100.5", "i": "trade-BTCUSDT-suspended"}]},
            received_at_ns=NOW_NS + 5,
        ),)
        stream.drained = False
        cycle_now[0] = NOW_NS + 6
        supervisor.run_once()
        suspended_reports = [
            entry.metadata["report"]
            for entry in repository.artifact_entries("PublicStreamContinuityReportV1")
            if entry.metadata["report"]["channel"] == "publicTrade.BTCUSDT"
        ]
        status_report = max(suspended_reports, key=lambda item: item["as_of_ns"])
        assert status_report["metadata_status"] == "SUSPENDED"
        assert status_report["metadata_current"] is False
        assert status_report["strategy_input_qualified"] is False
        assert "METADATA_REVISION_CHANGED_REQUIRES_FRESH_FEED_STATE" in status_report["gap_reason_codes"]


def test_restart_after_queued_unpersisted_frames_records_an_uncertain_interval(tmp_path) -> None:
    database = tmp_path / "queued-restart.sqlite"
    first_stream = _FakeStreamSource(_frames())
    first_port = production.create_bybit_public_ws_port(
        public_source=_FakePublicSource(), public_stream_source=first_stream, clock_ns=lambda: NOW_NS,
    )
    with OpsSupervisorV2(database, first_port, clock_ns=lambda: NOW_NS) as first:
        repository = first._ensure_open()
        first_port.recover(repository, now_ns=NOW_NS)
        assert first_stream.started
        assert not first_stream.drained

    restart_stream = _FakeStreamSource(())
    with OpsSupervisorV2(
        database,
        production.create_bybit_public_ws_port(
            public_source=_FakePublicSource(), public_stream_source=restart_stream,
            clock_ns=lambda: NOW_NS + 1,
        ),
        clock_ns=lambda: NOW_NS + 1,
    ) as restarted:
        restarted.run_once()
        repository = restarted.repository
        assert repository is not None
        reports = [entry.metadata["report"] for entry in repository.artifact_entries("PublicStreamContinuityReportV1")]
        btc_trade = next(
            report for report in reports
            if report["instrument"]["native_symbol"] == "BTCUSDT"
            and report["channel"] == "publicTrade.BTCUSDT"
        )
        btc_book = next(
            report for report in reports
            if report["instrument"]["native_symbol"] == "BTCUSDT"
            and report["channel"] == "orderbook.50.BTCUSDT"
        )
        assert btc_trade["observed_trade_evidence"] is False
        assert btc_trade["trade_completeness_proven"] is False
        assert "CONTROLLER_RESTART_UNACKNOWLEDGED_BUFFER_INTERVAL" in btc_trade["gap_reason_codes"]
        assert btc_book["book_sequence_valid"] is False
        assert btc_book["latest_valid_bbo"] is None
