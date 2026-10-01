from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from atlas.v2.data.microstructure_archive import L2FrameArchiveV2, L2RawFrameV2
from atlas.v2.data.public_microstructure_ws import (
    BoundedPublicFrameHandoffV2,
    CapturedPublicFrameV2,
    bybit_btc_eth_linear_topics,
)
from atlas.v2.data.session035_public_host_qualification import (
    WriterLockBusy,
    adjudicate_observed_interval,
    build_bybit_trade_completeness_assessment,
    classify_filesystem_path,
    classify_host,
    compare_trade_overlap,
    controller_writer_lock,
    run_public_host_source_qualification,
)
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    ProductTypeV2,
    VenueV2,
)
from atlas.v2.memory.repository import OpsRepository

TOPICS = bybit_btc_eth_linear_topics()


def _frame(channel: str, body: dict[str, object], received_at_ns: int) -> CapturedPublicFrameV2:
    raw = json.dumps(body, separators=(",", ":")).encode()
    return CapturedPublicFrameV2(
        VenueV2.BYBIT,
        "BYBIT_PUBLIC_WS",
        channel,
        raw,
        hashlib.sha256(raw).hexdigest(),
        received_at_ns,
        received_at_ns + 1,
        1,
    )


def _instrument(symbol: str) -> InstrumentKeyV2:
    base = symbol.removesuffix("USDT")
    return InstrumentKeyV2(
        VenueV2.BYBIT,
        EnvironmentV2.MAINNET,
        ProductTypeV2.LINEAR_PERPETUAL,
        symbol,
        base,
        "USDT",
        "USDT",
        f"test-{base.lower()}-contract-revision",
    )


def _trade_frame(symbol: str, trade_id: str, seq: int, event_ms: int) -> tuple[bytes, str]:
    topic = f"publicTrade.{symbol}"
    raw = json.dumps({
        "topic": topic,
        "type": "snapshot",
        "ts": event_ms,
        "data": [{"i": trade_id, "T": event_ms, "S": "Buy", "p": "100.00", "v": "0.25", "seq": seq}],
    }, separators=(",", ":")).encode()
    return raw, topic


def test_default_runner_is_offline_and_records_real_sqlite_modes(tmp_path: Path) -> None:
    root = tmp_path / "qualification"
    result = run_public_host_source_qualification(data_root=root)
    assert result["network_calls"] == 0
    assert result["real_public_smoke_opt_in"] is False
    assert result["real_public_smoke"]["status"] == "UNVERIFIED"
    assert result["host"]["sqlite"]["journal_mode"] == "WAL"
    assert result["host"]["sqlite"]["synchronous"] == "FULL"
    assert result["host"]["sqlite"]["runtime_version"]


def test_nonempty_data_root_fails_closed_as_test_gate(tmp_path: Path) -> None:
    root = tmp_path / "atlas-session035-nonempty"
    root.mkdir()
    marker = root / "owner-data.bin"
    marker.write_bytes(b"preserve")
    result = run_public_host_source_qualification(data_root=root, real_public_smoke=True)
    assert result["status"] == "TEST GATE"
    assert result["network_calls"] == 0
    assert result["reason_code"] == "ValueError"
    assert marker.read_bytes() == b"preserve"


def test_real_smoke_requires_explicit_flag(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import atlas.v2.data.session035_public_host_qualification as qualification

    calls: list[int] = []

    def fake_smoke(*_args, **kwargs):
        calls.append(kwargs["duration_seconds"])
        return {"status": "TESTED", "duration_seconds": 1}

    monkeypatch.setattr(qualification, "_real_public_smoke", fake_smoke)
    offline = run_public_host_source_qualification(data_root=tmp_path / "offline")
    assert calls == [] and offline["network_calls"] == 0
    opted_in = run_public_host_source_qualification(
        data_root=tmp_path / "atlas-session035-opt-in", real_public_smoke=True, duration_seconds=20,
    )
    assert calls == [20]
    assert opted_in["real_public_smoke_opt_in"] is True
    assert opted_in["real_public_smoke"]["status"] == "TESTED"


def test_opted_in_fake_smoke_returns_bounded_sanitized_counters(tmp_path: Path) -> None:
    from types import SimpleNamespace

    from atlas.v2.data.public_stream_source import PublicStreamSourceStatusV2

    handoff = BoundedPublicFrameHandoffV2(venue=VenueV2.BYBIT, topics=TOPICS)
    handoff.observe_connected(1)
    handoff.offer(_frame("CONTROL", {"op": "subscribe", "success": True}, 2))
    topic = "publicTrade.BTCUSDT"
    handoff.offer(_frame(topic, {"topic": topic, "data": [{"i": "one"}]}, 3))

    class Stream:
        attempt_count = 1

        def status(self):
            return PublicStreamSourceStatusV2("RUNNING", 1, 0, None, handoff.snapshot())

    stream = Stream()

    class Supervisor:
        def run_once(self):
            handoff.drain()
            return SimpleNamespace(event_receipts=())

        def close(self):
            return None

    clock = {"mono": 0, "utc": 1_800_000_000_000_000_000}

    def monotonic_ns() -> int:
        return clock["mono"]

    def clock_ns() -> int:
        clock["utc"] += 1
        return clock["utc"]

    def sleep_fn(seconds: float) -> None:
        clock["mono"] += int(seconds * 1_000_000_000)

    def runtime_builder(**_kwargs):
        return SimpleNamespace(public_source=None), stream, Supervisor()

    result = run_public_host_source_qualification(
        data_root=tmp_path / "atlas-session035-fake-smoke",
        real_public_smoke=True,
        duration_seconds=20,
        clock_ns=clock_ns,
        monotonic_ns=monotonic_ns,
        sleep_fn=sleep_fn,
        runtime_builder=runtime_builder,
    )
    smoke = result["real_public_smoke"]
    assert smoke["status"] == "TESTED"
    assert smoke["connection_attempts"] == 1
    assert smoke["connection_epochs"] == [1]
    assert smoke["queue"]["high_water_items"] <= smoke["queue"]["capacity_items"]
    assert smoke["qualification"]["trade_completeness"] == "NOT ESTIMABLE"
    assert result["network_calls"] == {
        "bybit_public_rest_gets": 0,
        "bybit_public_websocket_connection_attempts": 1,
        "credentialed_calls": 0,
        "account_calls": 0,
        "order_calls": 0,
        "provider_model_calls": 0,
    }
    serialized = json.dumps(result, sort_keys=True)
    assert "owner-data" not in serialized and "raw_payload_bytes" not in serialized


def test_runner_does_not_read_or_emit_environment_secrets(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import atlas.v2.data.session035_public_host_qualification as qualification

    class ForbiddenEnvironment:
        def __getitem__(self, _key):
            raise AssertionError("environment value read")

        def get(self, *_args, **_kwargs):
            raise AssertionError("environment value read")

        def __iter__(self):
            raise AssertionError("environment enumerated")

        def items(self):
            raise AssertionError("environment enumerated")

    with monkeypatch.context() as scoped:
        scoped.setattr(qualification.os, "environ", ForbiddenEnvironment())
        result = run_public_host_source_qualification(data_root=tmp_path / "private")
    encoded = json.dumps(result, sort_keys=True)
    assert result["network_calls"] == 0
    assert "API_KEY" not in encoded and "TOKEN" not in encoded and "PRIVATE_IP" not in encoded
    assert result["host"]["paths"]["path_absolute_values_emitted"] is False


def test_non_linux_host_reports_environment_block(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import atlas.v2.data.session035_public_host_qualification as qualification

    monkeypatch.setattr(qualification.sys, "platform", "win32")
    result = run_public_host_source_qualification(data_root=tmp_path / "unsupported")
    assert classify_host("win32", "Windows") == ("UNSUPPORTED", "BLOCKED BY ENVIRONMENT")
    assert result["host"]["host_status"] == "BLOCKED BY ENVIRONMENT"
    assert result["network_calls"] == 0
    assert not (tmp_path / "unsupported").exists()


def test_windows_mounted_path_is_detected_without_rewriting() -> None:
    path = "/mnt/c/atlas/session035"
    before = path
    assert classify_filesystem_path(path, host_class="WSL") == "WINDOWS_MOUNT"
    assert path == before


def test_runner_gates_windows_mount_without_creating_or_rewriting_data_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import atlas.v2.data.session035_public_host_qualification as qualification

    root = tmp_path / "atlas-session035-windows-mount"
    monkeypatch.setattr(
        qualification, "classify_filesystem_path", lambda _path, *, host_class: "WINDOWS_MOUNT",
    )
    result = run_public_host_source_qualification(data_root=root, real_public_smoke=True)
    assert result["host"]["host_status"] == "TEST GATE"
    assert result["network_calls"] == 0
    assert result["real_public_smoke"]["status"] == "BLOCKED BY ENVIRONMENT"
    assert not root.exists()


def test_linux_guest_path_is_accepted_for_wsl_host(tmp_path: Path) -> None:
    assert classify_filesystem_path(tmp_path, host_class="WSL") == "LINUX_GUEST_FILESYSTEM"


def test_wal_and_full_synchronous_settings_are_verified(tmp_path: Path) -> None:
    result = run_public_host_source_qualification(data_root=tmp_path / "sqlite-check")
    assert result["host"]["single_writer_lock"] == {"acquired": True, "status": "TESTED"}
    assert result["host"]["sqlite"]["journal_mode"] == "WAL"
    assert result["host"]["sqlite"]["synchronous"] == "FULL"


def test_single_writer_conflict_fails_closed_without_network(tmp_path: Path) -> None:
    root = tmp_path / "atlas-session035-one-writer"
    root.mkdir()
    with controller_writer_lock(root / "controller.lock"):
        with pytest.raises(WriterLockBusy), controller_writer_lock(root / "controller.lock"):
            pass
        result = run_public_host_source_qualification(data_root=root, real_public_smoke=True)
    assert result["network_calls"] == 0
    assert result["real_public_smoke"]["status"] == "TEST GATE"
    assert result["real_public_smoke"]["reason_code"] == "SINGLE_WRITER_LOCK_BUSY"


def test_successful_observation_is_bounded_and_keeps_completeness_unproven() -> None:
    handoff = BoundedPublicFrameHandoffV2(
        venue=VenueV2.BYBIT,
        topics=TOPICS,
        max_queue_items=2,
        max_queue_bytes=4_096,
        max_drain_items=1,
    )
    handoff.observe_connected(1)
    handoff.offer(_frame("CONTROL", {"op": "subscribe", "success": True}, 2))
    trade_topic = "publicTrade.BTCUSDT"
    handoff.offer(_frame(trade_topic, {"topic": trade_topic, "data": [{"i": "one"}]}, 3))
    state = handoff.snapshot()
    assert state.successful_subscription_ack_count == 1
    assert state.subscription_ack_topics == TOPICS
    assert state.frames_received == 1 and state.frame_bytes_received > 0
    assert state.first_frame_received_at_ns == 3 == state.last_frame_received_at_ns
    assert state.high_water_items <= state.max_queue_items
    assert state.high_water_bytes <= state.max_queue_bytes
    gates = adjudicate_observed_interval(
        subscription_acknowledged=True,
        frame_count=state.frames_received,
        trade_count=1,
        book_sequence_valid=True,
    )
    assert gates["public_websocket_transport"] == "TESTED"
    assert gates["trade_completeness"] == "NOT ESTIMABLE"
    assert gates["trade_completeness_proven"] is False


def test_disconnect_produces_test_gate() -> None:
    gates = adjudicate_observed_interval(
        subscription_acknowledged=True, frame_count=5, trade_count=2, book_sequence_valid=True,
        disconnect_count=1,
    )
    assert gates["public_websocket_transport"] == "TEST GATE"
    assert gates["book_continuity"] == "TEST GATE"
    assert gates["observed_trade_evidence"] == "TEST GATE"


def test_queue_overflow_produces_test_gate() -> None:
    topic = "publicTrade.BTCUSDT"
    handoff = BoundedPublicFrameHandoffV2(
        venue=VenueV2.BYBIT, topics=TOPICS, max_queue_items=1, max_queue_bytes=4_096,
    )
    assert handoff.offer(_frame(topic, {"topic": topic, "data": []}, 10))
    assert not handoff.offer(_frame(topic, {"topic": topic, "data": []}, 11))
    assert handoff.snapshot().overflowed
    gates = adjudicate_observed_interval(
        subscription_acknowledged=True, frame_count=1, trade_count=1, book_sequence_valid=True,
        queue_overflow=True,
    )
    assert gates["public_websocket_transport"] == "TEST GATE"
    assert gates["book_continuity"] == "TEST GATE"
    assert gates["trade_completeness"] == "NOT ESTIMABLE"


def test_conflicting_trade_id_produces_test_gate() -> None:
    gates = adjudicate_observed_interval(
        subscription_acknowledged=True, frame_count=9, trade_count=7, book_sequence_valid=True,
        conflicting_trade_ids=1,
    )
    assert gates["public_websocket_transport"] == "TEST GATE"
    assert gates["observed_trade_evidence"] == "TEST GATE"
    assert gates["trade_completeness"] == "NOT ESTIMABLE"


def test_healthy_transport_alone_never_proves_trade_completeness() -> None:
    gates = adjudicate_observed_interval(
        subscription_acknowledged=True, frame_count=10, trade_count=10, book_sequence_valid=True,
    )
    assert gates["public_websocket_transport"] == "TESTED"
    assert gates["book_continuity"] == "TESTED"
    assert gates["observed_trade_evidence"] == "TESTED"
    assert gates["trade_completeness_proven"] is False
    assert gates["trade_completeness"] == "NOT ESTIMABLE"


def test_increasing_seq_alone_never_proves_trade_completeness() -> None:
    observed_seq = (100, 101, 102)
    assert observed_seq == tuple(sorted(observed_seq))
    gates = adjudicate_observed_interval(
        subscription_acknowledged=True, frame_count=3, trade_count=3, book_sequence_valid=True,
    )
    assert gates["trade_completeness_proven"] is False
    assert gates["trade_completeness"] == "NOT ESTIMABLE"


def test_same_seq_across_trade_messages_is_counted_as_documented_behavior(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    root.mkdir()
    key = _instrument("BTCUSDT")
    with OpsRepository(root / "ops.sqlite") as repository:
        archive = L2FrameArchiveV2(root / "ops-l2-frames", repository)
        for trade_id, event_ms in (("trade-1", 1_700_000_000_000), ("trade-2", 1_700_000_000_001)):
            raw, topic = _trade_frame("BTCUSDT", trade_id, 9001, event_ms)
            archive.write_chunk((L2RawFrameV2(
                key, "BYBIT_PUBLIC_WS", topic, f"TRADE_FRAME_{trade_id}", raw,
                hashlib.sha256(raw).hexdigest(), event_ms * 1_000_000,
                event_ms * 1_000_000, event_ms * 1_000_000, None, None, None,
                "BYBIT_TRADE_ID_IS_IDENTITY_NOT_REPLAY_CURSOR", "HEALTHY_CURRENT", "ACTUAL_SYSTEM",
            ),))
    from atlas.v2.data.session035_public_host_qualification import _archive_trade_rows

    rows, stats = _archive_trade_rows(root)
    assert len(rows["BTCUSDT"]) == 2
    assert stats["same_seq_multiple_messages"] == 1
    assert stats["duplicate_ids"] == 0
    assert stats["diagnostic_scan_truncated"] is False


def test_recent_rest_overlap_match_does_not_prove_completeness() -> None:
    content = {"price": "100", "size": "1", "event_time_ns": 10, "side": "Buy", "seq": "3"}
    overlap = compare_trade_overlap(
        {"BTCUSDT": {"a": {**content, "seq": None}}, "ETHUSDT": {}},
        {"BTCUSDT": {"a": content}, "ETHUSDT": {}},
    )
    assert overlap["BTCUSDT"]["statuses"] == ["MATCHED"]
    assert overlap["BTCUSDT"]["completeness_proven"] is False


def test_rest_trade_diagnostic_is_capped_at_one_request_per_symbol() -> None:
    from atlas.v2.data.session035_public_host_qualification import _SingleRecentTradeReader

    class Reader:
        def recent_trades(self, symbol: str, *, limit: int) -> tuple[str, int]:
            return symbol, limit

    counts = {"BTCUSDT": 0, "ETHUSDT": 0}
    reader = _SingleRecentTradeReader(Reader(), counts)
    assert reader.recent_trades("BTCUSDT", limit=100) == ("BTCUSDT", 100)
    assert counts == {"BTCUSDT": 1, "ETHUSDT": 0}
    with pytest.raises(RuntimeError):
        reader.recent_trades("BTCUSDT", limit=100)
    with pytest.raises(RuntimeError):
        reader.recent_trades("SOLUSDT", limit=100)


def test_rest_mismatch_remains_explicit() -> None:
    comparison = compare_trade_overlap(
        {"BTCUSDT": {"ws-only": {"price": "1", "event_time_ns": 10}}, "ETHUSDT": {}},
        {"BTCUSDT": {"rest-only": {"price": "2", "event_time_ns": 10}}, "ETHUSDT": {}},
    )
    assert set(comparison["BTCUSDT"]["statuses"]) == {"MISSING_FROM_WS", "MISSING_FROM_REST"}
    assert comparison["BTCUSDT"]["completeness_proven"] is False


def test_rest_overlap_ignores_websocket_trades_outside_recent_suffix_time_window() -> None:
    matched = {"price": "100", "size": "1", "event_time_ns": 20, "side": "Buy", "seq": None}
    comparison = compare_trade_overlap(
        {"BTCUSDT": {
            "old-ws": {**matched, "event_time_ns": 1},
            "shared": matched,
        }, "ETHUSDT": {}},
        {"BTCUSDT": {
            "shared": {**matched, "seq": "99"},
            "suffix-only": {**matched, "event_time_ns": 21},
        }, "ETHUSDT": {}},
    )
    assert comparison["BTCUSDT"]["shared_event_time_window"] == [20, 20]
    assert comparison["BTCUSDT"]["ws_ids_in_shared_event_time_window"] == 1
    assert comparison["BTCUSDT"]["rest_ids_in_shared_event_time_window"] == 1
    assert comparison["BTCUSDT"]["statuses"] == ["MATCHED"]


def test_rest_overlap_without_shared_event_time_window_is_not_estimable() -> None:
    comparison = compare_trade_overlap(
        {"BTCUSDT": {"ws": {"event_time_ns": 10}}, "ETHUSDT": {}},
        {"BTCUSDT": {"rest": {"event_time_ns": 20}}, "ETHUSDT": {}},
    )
    assert comparison["BTCUSDT"]["statuses"] == ["NOT ESTIMABLE"]
    assert comparison["BTCUSDT"]["completeness_proven"] is False


def test_no_repair_cursor_keeps_completeness_not_estimable() -> None:
    assessment = build_bybit_trade_completeness_assessment(
        (_instrument("BTCUSDT"), _instrument("ETHUSDT")), assessed_at_ns=1_800_000_000_000_000_000,
    )
    assert assessment["rest_repair_semantics"]["query_cursor"] is False
    assert assessment["historical_archive_review"]["official_recent_trade_docs_link_to_downloadable_archive"] is True
    assert assessment["historical_archive_review"]["qualifies_as_exact_repair_source"] is False
    assert {item["channel"] for item in assessment["channels"]} == {
        "publicTrade.BTCUSDT", "publicTrade.ETHUSDT",
    }
    assert assessment["trade_completeness_status"] == "NOT ESTIMABLE"
    assert assessment["gate_status"] == "TEST GATE"
    assert assessment["completeness_proven"] is False


def test_s3_readiness_stays_false_with_frozen_thresholds() -> None:
    assessment = build_bybit_trade_completeness_assessment(
        (_instrument("BTCUSDT"), _instrument("ETHUSDT")), assessed_at_ns=1_800_000_000_000_000_000,
    )
    assert assessment["s3_warmup_contract"] == {
        "required_contiguous_m1_observations": 10_081,
        "strictly_preceding_residual_observations": 120,
        "trade_derived_vwap_required": True,
        "fresh_bbo_max_age_ns": 1_000_000_000,
        "readiness": "NOT ESTIMABLE",
    }


def test_capital_and_assisted_remain_disabled() -> None:
    assessment = build_bybit_trade_completeness_assessment(
        (_instrument("BTCUSDT"), _instrument("ETHUSDT")), assessed_at_ns=1_800_000_000_000_000_000,
    )
    assert assessment["capital_enabled"] is False
    assert assessment["assisted_enabled"] is False
    assert assessment["authority"] == "ZERO"


def test_public_http_transport_disables_ambient_proxy_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    import atlas.v2.data.public_http as public_http

    seen_handlers: list[object] = []

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self, _limit: int) -> bytes:
            return b"{}"

    class DirectOpener:
        def open(self, _request, *, timeout: float):
            assert timeout == 1.0
            return Response()

    def fake_build_opener(*handlers):
        seen_handlers.extend(handlers)
        return DirectOpener()

    monkeypatch.setattr(public_http, "build_opener", fake_build_opener)
    status, body = public_http._stdlib_get("https://api.bybit.com/v5/market/time", 1.0)
    assert status == 200 and body == b"{}"
    assert len(seen_handlers) == 1
    assert isinstance(seen_handlers[0], public_http.ProxyHandler)
    assert seen_handlers[0].proxies == {}
