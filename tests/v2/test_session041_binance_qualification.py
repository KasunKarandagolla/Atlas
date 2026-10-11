from __future__ import annotations

import asyncio
import hashlib
import json
import os
import stat
import threading
import time
from collections import deque
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.durable_public_capture import (
    _PAYLOAD_SIZE_BUCKETS,
    DurablePublicCaptureV1,
)
from atlas.v2.data.public_microstructure_ws import CapturedPublicFrameV2
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime import binance_qualification as qualification


def _host_probe(selected_path: Path, identity_sha256: str = "a" * 64) -> dict:
    from atlas.v2.runtime.storage_preflight import PREFLIGHT_CONTRACT_V1, StoragePreflightLimitsV1

    body = {
        "schema_version": 1,
        "version": PREFLIGHT_CONTRACT_V1,
        "selected_path": str(selected_path),
        "identity_sha256": identity_sha256,
        "started_at_ns": 1,
        "completed_at_ns": 2,
        "elapsed_ns": 1,
        "status": "TESTED",
        "allowed": True,
        "reasons": [],
        "limits": StoragePreflightLimitsV1().as_dict(),
        "measurements": [],
        "facts": {},
        "probe_mode": "HOST_PATH",
        "authority": "ZERO",
        "capital_enabled": False,
        "assisted_enabled": False,
        "live_source_qualification": "TEST GATE",
        "endurance_qualification": "TEST GATE",
    }
    return {**body, "content_hash": sha256_json(body)}


def _run_with_qualification_marker(tmp_path: Path) -> Path:
    from atlas.v2.product import create_run

    selected = tmp_path / "selected-data"
    selected.mkdir(parents=True)
    run_root = selected / "s41-qualification-only"
    config = qualification.qualification_config(run_root)
    old_umask = os.umask(0o077)
    try:
        run = create_run(run_root, config)
        manifest = json.loads((run / "run.json").read_text(encoding="utf-8"))
        supervisor_token = "qualification-test-supervisor-token"
        host_identity = sha256_json({
            "version": "BINANCE_QUALIFICATION_HOST_PATH_IDENTITY_V1",
            "source_sha": manifest["source_sha"],
            "profile_hash": qualification.profile_contract_hash(),
            "selected_path_sha256": hashlib.sha256(str(selected.resolve()).encode()).hexdigest(),
        })
        probe = _host_probe(selected.resolve(), host_identity)
        binding = qualification._profile_binding(run, selected_path=selected.resolve(),
            host_probe=probe, manifest=manifest, supervisor_token=supervisor_token)
        qualification._write_immutable(run / qualification.PROFILE_BINDING_FILE, binding)
    finally:
        os.umask(old_umask)
    return run


def test_profile_is_immutable_binance_only_and_has_no_authority(tmp_path):
    config = qualification.qualification_config((tmp_path / "data").resolve())
    qualification.validate_qualification_config(config)
    assert qualification.profile_contract()["purpose"] == "QUALIFICATION_ONLY"
    assert qualification.profile_contract()["public_venues"] == ["BINANCE"]
    assert qualification.profile_contract()["execution_profile"] == "DISABLED"
    assert qualification.profile_contract()["provider_profile"] == "DISABLED"
    assert qualification.profile_contract()["capacity_certificate_issued"] is False
    assert qualification.profile_contract()["capital_enabled"] is False
    assert qualification.profile_contract()["assisted_enabled"] is False

    rejected = (
        replace(config, public_venues=("BINANCE", "BYBIT")),
        replace(config, selected_execution_venue="BYBIT"),
        replace(config, credential_ref="ref_owner_credential"),
        replace(config, capability_profile_ref="ref_capability"),
        replace(config, provider_profile="deepseek-v41-action-critic-v1"),
        replace(config, report_interval_seconds=120),
    )
    for candidate in rejected:
        with pytest.raises(ValueError, match="QUALIFICATION_CONFIG_SCOPE_MISMATCH"):
            qualification.validate_qualification_config(candidate)


def test_child_environment_drops_credentials_and_provider_secrets(monkeypatch):
    monkeypatch.setenv("BINANCE_API_KEY", "test-only-value")
    monkeypatch.setenv("DEEPSEEK_API_TOKEN", "test-only-value")
    monkeypatch.setenv("HTTPS_PROXY", "https://proxy.invalid")
    monkeypatch.setenv("PYTHONPATH", "/tmp/untrusted-pythonpath")
    monkeypatch.setenv("ATLAS_DIAGNOSTIC_SAFE_SETTING", "preserved")
    environment = qualification._sanitized_environment()
    assert "BINANCE_API_KEY" not in environment
    assert "DEEPSEEK_API_TOKEN" not in environment
    assert "HTTPS_PROXY" not in environment
    assert "PYTHONPATH" not in environment
    assert environment["ATLAS_DIAGNOSTIC_SAFE_SETTING"] == "preserved"


def test_selected_path_rejects_symlink_source_tree_and_root(tmp_path):
    link = tmp_path / "link"
    link.symlink_to(tmp_path / "target", target_is_directory=True)
    with pytest.raises(ValueError, match="CANONICAL|SYMLINK"):
        qualification.validate_selected_path(link)
    with pytest.raises(ValueError, match="SOURCE_TREE"):
        qualification.validate_selected_path(Path(__file__).resolve().parents[2])
    with pytest.raises(ValueError, match="NONROOT"):
        qualification.validate_selected_path(Path("/"))


def test_binance_factory_never_constructs_bybit_or_authenticated_clients(monkeypatch):
    import atlas.v2.data.broad_public_source as broad_source
    from atlas.v2.runtime.production import create_broad_public_port

    constructed: list[str] = []

    def forbidden_bybit():
        raise AssertionError("inactive Bybit reader was constructed")

    class BinancePublicFixture:
        def __init__(self):
            constructed.append("BINANCE_PUBLIC")

    monkeypatch.setattr(broad_source, "BybitPublicReaderV2", forbidden_bybit)
    monkeypatch.setattr(broad_source, "BinanceUsdMPublicReaderV2", BinancePublicFixture)
    import atlas.v2.runtime.production as production

    monkeypatch.setattr(production, "BroadProductionOpsCyclePortV2",
        lambda **kwargs: SimpleNamespace(public_source=kwargs["public_source"]))
    port = create_broad_public_port(enabled_venues=(VenueV2.BINANCE,))
    qualification._validate_port_scope(port)
    assert port.public_source.bybit_reader is None
    assert port.public_source.required_source_ids == (qualification.BINANCE_REST_SOURCE_ID,)
    assert port.public_source.enabled_venues == (VenueV2.BINANCE,)
    assert constructed == ["BINANCE_PUBLIC", "BINANCE_PUBLIC"]
    assert "bybit" not in port.public_source.required_source_ids[0].lower()


def test_due_cycle_and_queue_loss_gates_fail_closed():
    base = {"acquisition_due": True, "complete": True, "pending": False,
            "scheduled_offset_ns": 0, "completed_offset_ns": 99}
    assert qualification._due_cycle_failure(base, window_end_offset_ns=100) is None
    assert qualification._due_cycle_failure({**base, "complete": False}, window_end_offset_ns=100) == \
        "INCOMPLETE_DUE_POLL_CYCLE"
    assert qualification._due_cycle_failure({"status": "MISSED_DUE_SLOT"},
        window_end_offset_ns=100) == "INCOMPLETE_DUE_POLL_ACCOUNTING"
    assert qualification._due_cycle_failure({**base, "completed_offset_ns": 101},
        window_end_offset_ns=100) == "DUE_POLL_COMPLETED_OUTSIDE_MEASUREMENT_WINDOW"
    assert qualification._due_cycle_failure({**base, "scheduled_offset_ns": 0,
        "completed_offset_ns": qualification.POLL_INTERVAL_NS + 1},
        window_end_offset_ns=qualification.POLL_INTERVAL_NS + 2) == "DUE_POLL_CADENCE_EXCEEDED"
    assert qualification._next_due_poll_ns(100, 100 + 5 * qualification.POLL_INTERVAL_NS) == (
        100 + 5 * qualification.POLL_INTERVAL_NS)
    assert qualification._next_due_poll_ns(100, 100 + 5 * qualification.POLL_INTERVAL_NS + 1) == (
        100 + 6 * qualification.POLL_INTERVAL_NS)

    class DuePort:
        last_acquisition_snapshot = None
        public_source = SimpleNamespace(required_source_ids=(qualification.BINANCE_REST_SOURCE_ID,))
        public_stream_source = SimpleNamespace(plan=None)

        def collect(self, _repository, *, now_ns, recovery):
            self.last_acquisition_snapshot = SimpleNamespace(
                source_snapshot={"enabled_venues": ["BINANCE"], "complete": True},
                complete=True, request_count=3, successful_request_count=3, records=())
            return SimpleNamespace(events=())

    cycle = qualification._snapshot_cycle(DuePort(), object(), object(),
        scheduled_ns=100, window_start_ns=1)
    assert cycle["acquisition_due"] is True


def test_capture_payload_histogram_keeps_oversize_bucket_and_exact_window():
    capture = object.__new__(DurablePublicCaptureV1)
    capture._lock = threading.Lock()
    capture._capture_payload_metrics = True
    capture._capture_timeline_truncated_before_ns = None
    first = [0] * (len(_PAYLOAD_SIZE_BUCKETS) + 1)
    first[-1] = 2
    second = [0] * (len(_PAYLOAD_SIZE_BUCKETS) + 1)
    second[0] = 3
    capture._payload_size_batch_timeline = deque(((10, tuple(first)), (20, tuple(second))))
    distribution = capture.payload_size_distribution_between_monotonic_ns(10, 20)
    assert distribution[f"gt_{_PAYLOAD_SIZE_BUCKETS[-1]}"] == 0
    assert distribution[f"le_{_PAYLOAD_SIZE_BUCKETS[0]}"] == 3
    distribution = capture.payload_size_distribution_between_monotonic_ns(0, 10)
    assert distribution[f"gt_{_PAYLOAD_SIZE_BUCKETS[-1]}"] == 2
    in_window = capture.payload_size_distribution_in_window(10, 20)
    assert in_window[f"gt_{_PAYLOAD_SIZE_BUCKETS[-1]}"] == 2
    assert in_window[f"le_{_PAYLOAD_SIZE_BUCKETS[0]}"] == 0


def test_capture_payload_metrics_are_opt_in_for_qualification_only():
    from atlas.v2.runtime.broad_public_runtime import BroadPublicRuntimeV2

    handoff = SimpleNamespace(max_queue_items=512, max_drain_items=128)

    class EmptySource:
        venue = "BINANCE"
        topics = ()

        @staticmethod
        def status():
            return SimpleNamespace(state="RUNNING", attempt_count=0, reconnect_count=0,
                last_error_code=None, handoff=handoff)

    ordinary_status = DurablePublicCaptureV1(EmptySource()).status().capture
    assert "payload_bytes_written" not in ordinary_status
    assert "payload_size_histogram" not in ordinary_status
    diagnostic_status = DurablePublicCaptureV1(EmptySource(), capture_payload_metrics=True).status().capture
    assert diagnostic_status["payload_bytes_written"] == 0
    assert "payload_size_histogram" in diagnostic_status

    ordinary_capture = object.__new__(DurablePublicCaptureV1)
    ordinary_capture._capture_payload_metrics = False
    with pytest.raises(RuntimeError, match="were not enabled"):
        ordinary_capture.captured_payload_bytes_at_monotonic_ns(10)
    ordinary_runtime = BroadPublicRuntimeV2()
    assert ordinary_runtime.capture_payload_metrics is False
    diagnostic_runtime = BroadPublicRuntimeV2(capture_payload_metrics=True)
    assert diagnostic_runtime.capture_payload_metrics is True
    with pytest.raises(ValueError, match="must be boolean"):
        BroadPublicRuntimeV2(capture_payload_metrics=1)


def test_final_backlog_gate_detects_growth_and_requires_coverage():
    stable = [
        {"window_offset_ns": second * 1_000_000_000, "queue_items": 4,
         "capture": {"pending_frames": 3}}
        for second in (90, 100, 119)
    ]
    assert qualification._final_backlog_summary(stable)["slope_frames_per_second"] == 0
    growing = [dict(sample, queue_items=sample["queue_items"] + index * 5)
        for index, sample in enumerate(stable)]
    assert qualification._final_backlog_summary(growing)["slope_frames_per_second"] > 0
    assert qualification._final_backlog_summary(stable[:1])["measurement_complete"] is False

    runtime = {"free_disk_bytes": 10_000, "queue_items": 1, "queue_bytes": 10,
        "queue_loss_latched": True, "frames_rejected": 1, "max_service_gap_ns": 1,
        "queue_max_items": 512, "queue_max_bytes": 16_000_000,
        "capture": {"batch_frame_limit": 16}, "stream_ingestion_failed": False}
    assert qualification._check_runtime_bounds(runtime, minimum_free_disk_bytes=1) == \
        "IRREVERSIBLE_PUBLIC_FRAME_LOSS"
    runtime["queue_loss_latched"] = False
    runtime["frames_rejected"] = 0
    runtime["max_service_gap_ns"] = qualification.STREAM_SERVICE_GAP_LIMIT_NS + 1
    assert qualification._check_runtime_bounds(runtime, minimum_free_disk_bytes=1) == \
        "FROZEN_STREAM_SERVICE_GAP_EXCEEDED"
    runtime["max_service_gap_ns"] = 1
    runtime["lanes"] = {"BINANCE_MARKET": {"connected": True}}
    runtime["source_health"] = {"BINANCE_MARKET": "HEALTHY_CURRENT"}
    runtime["stream_unresolved_gap"] = False
    assert qualification._check_runtime_bounds(runtime, minimum_free_disk_bytes=1,
        require_healthy_source=True) is None
    runtime["stream_unresolved_gap"] = True
    assert qualification._check_runtime_bounds(runtime, minimum_free_disk_bytes=1,
        require_healthy_source=True) == "PUBLIC_STREAM_SOURCE_UNHEALTHY_OR_GAPPED"
    runtime["stream_unresolved_gap"] = False
    runtime["lanes"]["BINANCE_MARKET"]["connected"] = False
    assert qualification._check_runtime_bounds(runtime, minimum_free_disk_bytes=1,
        require_healthy_source=True) == "BINANCE_STREAM_LANE_DISCONNECTED"
    runtime["lanes"]["BINANCE_MARKET"]["connected"] = True
    runtime["queue_max_items"] = 1024
    assert qualification._check_runtime_bounds(runtime, minimum_free_disk_bytes=1) == \
        "FROZEN_CAPTURE_OR_HANDOFF_CONFIGURATION_MISMATCH"


def test_report_slots_missed_beyond_one_cadence_fail_closed():
    assert qualification._report_slot_failure(scheduled_ns=1, attempted_ns=2) is None
    assert qualification._report_slot_failure(scheduled_ns=1,
        attempted_ns=1 + qualification.REPORT_INTERVAL_NS - 1) is None
    assert qualification._report_slot_failure(scheduled_ns=1,
        attempted_ns=1 + qualification.REPORT_INTERVAL_NS) == "MISSED_DUE_REPORT_SLOT"
    with pytest.raises(ValueError, match="CLOCK_INVALID"):
        qualification._report_slot_failure(scheduled_ns=2, attempted_ns=1)


def test_completed_measurement_requires_real_durable_public_payload():
    assert qualification._measured_traffic_failure(
        window_complete=False, frame_count=0, payload_bytes=0) is None
    assert qualification._measured_traffic_failure(
        window_complete=True, frame_count=1, payload_bytes=100) is None
    assert qualification._measured_traffic_failure(
        window_complete=True, frame_count=0, payload_bytes=0) == "NO_MEASURED_BINANCE_PUBLIC_FRAMES"
    with pytest.raises(ValueError, match="COUNTER_INVALID"):
        qualification._measured_traffic_failure(window_complete=True, frame_count=-1, payload_bytes=0)


def test_stream_manifest_enforces_reviewed_population_caps():
    class FakeKey:
        @staticmethod
        def to_dict():
            return {"venue": "BINANCE"}

    plan = SimpleNamespace(plan_id="plan", keys=tuple(FakeKey() for _ in range(17)),
        identities=(), lane_topics={}, source_refs=())
    port = SimpleNamespace(public_stream_source=SimpleNamespace(plan=plan))
    with pytest.raises(ValueError, match="PROFILE_CAP"):
        qualification._stream_plan_manifest(port)


def test_capture_payload_metric_gate_fails_closed_when_not_enabled():
    port = SimpleNamespace(public_stream_source=SimpleNamespace(
        capture=SimpleNamespace(_capture=SimpleNamespace(_capture_payload_metrics=False))))
    with pytest.raises(ValueError, match="CAPTURE_PAYLOAD_METRICS_DISABLED"):
        qualification._validate_capture_metrics_enabled(port)


def test_rate_samples_use_actual_monotonic_counter_deltas():
    rates = qualification._sampled_rates((
        {"sampled_at_monotonic_ns": 0, "window_offset_ns": 0, "frames_received": 2,
         "captured_frames": 1, "captured_payload_bytes": 100},
        {"sampled_at_monotonic_ns": 2_000_000_000, "window_offset_ns": 2_000_000_000,
         "frames_received": 10, "captured_frames": 8, "captured_payload_bytes": 500},
    ))
    assert rates["source_frames_per_second"][0]["rate"] == 4
    assert rates["durably_captured_frames_per_second"][0]["rate"] == 3.5
    assert rates["durable_payload_bytes_per_second"][0]["rate"] == 200


def test_qualification_traffic_keeps_lane_channel_rates_and_exact_burst_peaks():
    from atlas.v2.data.qualification_telemetry import QualificationTrafficWindowV1

    start = 100
    end = start + 3_000_000_000
    traffic = QualificationTrafficWindowV1()
    traffic.begin_window(start, end)
    traffic.record(at_ns=start + 10, lane="BINANCE_MARKET", channel="btcusdt@aggTrade", payload_bytes=5)
    traffic.record(at_ns=start + 20, lane="BINANCE_MARKET", channel="btcusdt@aggTrade", payload_bytes=7)
    traffic.record(at_ns=start + 1_000_000_010, lane="BINANCE_MARKET",
        channel="btcusdt@aggTrade", payload_bytes=11)
    traffic.record(at_ns=start + 1_000_000_020, lane="BINANCE_DEPTH",
        channel="btcusdt@depth@100ms", payload_bytes=3)
    summary = qualification._traffic_window_summary(traffic.snapshot(), window_start_ns=start,
        window_end_ns=end, expected_identities=(
            ("BINANCE_MARKET", "btcusdt@aggTrade"), ("BINANCE_DEPTH", "btcusdt@depth@100ms")))
    assert summary["complete"] is True
    assert summary["lane_rates"]["BINANCE_MARKET"]["frames"] == 3
    assert summary["lane_rates"]["BINANCE_DEPTH"]["frames"] == 1
    market = summary["lane_rates"]["BINANCE_MARKET"]["channels"]["btcusdt@aggTrade"]
    assert market["payload_bytes"] == 23
    assert market["mean_payload_bytes"] == pytest.approx(23 / 3)
    peak = summary["one_second_peaks"]["all_lanes"]
    assert peak["frames"] == {"second": 0, "value": 2}
    assert peak["payload_bytes"] == {"second": 1, "value": 14}
    market_peaks = summary["one_second_peaks"]["lanes"]["BINANCE_MARKET"]
    assert market_peaks["frames"] == {"second": 0, "value": 2}
    assert market_peaks["payload_bytes"] == {"second": 0, "value": 12}
    assert summary["peak_bucket_basis"] == "fixed one-second buckets anchored at window_start_monotonic_ns"


def test_qualification_traffic_empty_and_truncated_windows_are_explicit():
    from atlas.v2.data.qualification_telemetry import QualificationTrafficWindowV1

    start, end = 500, 500 + 2_000_000_000
    traffic = QualificationTrafficWindowV1()
    traffic.begin_window(start, end)
    empty = qualification._traffic_window_summary(traffic.snapshot(), window_start_ns=start,
        window_end_ns=end, expected_identities=(("BINANCE_MARKET", "btcusdt@aggTrade"),))
    identity = empty["identity_rates"][0]
    assert identity["observed"] is False
    assert identity["frames_per_second"] == 0
    assert identity["mean_payload_bytes"] is None
    traffic._history_truncated = True
    with pytest.raises(ValueError, match="WINDOW_MISSING_OR_TRUNCATED"):
        qualification._traffic_window_summary(traffic.snapshot(), window_start_ns=start,
            window_end_ns=end, expected_identities=())


def _resource_fixture(at_ns: int, *, cpu_ticks: int = 10) -> dict:
    return {"sampled_at_monotonic_ns": at_ns, "process_cpu_seconds": cpu_ticks / 10,
        "process_rss_bytes": 1000, "process_peak_rss_bytes": 1200,
        "process_io_bytes": {"read_bytes": 100, "write_bytes": 200 + cpu_ticks},
        "worker": {"cpu_ticks": cpu_ticks, "cpu_ticks_per_second": 100,
            "worker_instance_ref": "a" * 64, "rss_bytes": 500, "read_bytes": 20,
            "write_bytes": 30 + cpu_ticks},
        "host_cpu_counters": {"total_ticks": at_ns, "idle_ticks": at_ns // 2},
        "host_resources": {"host_memory_available_bytes": 4096, "cgroup_memory": {"current_bytes": 2048},
            "process_affinity_cpu_count": 2},
        "system_psi": {
            "cpu": {"some": {"avg10": 0.1, "avg60": 0.1, "avg300": 0.1, "total": 1}},
            "memory": {"some": {"avg10": 0.1, "avg60": 0.1, "avg300": 0.1, "total": 1},
                "full": {"avg10": 0.0, "avg60": 0.0, "avg300": 0.0, "total": 0}},
            "io": {"some": {"avg10": 0.2, "avg60": 0.2, "avg300": 0.2, "total": 2},
                "full": {"avg10": 0.0, "avg60": 0.0, "avg300": 0.0, "total": 0}},
        },
        "free_disk_bytes": 10_000, "sqlite_bytes": 500, "wal_bytes": 100}


def test_resource_summary_exposes_missing_one_second_buckets_and_worker_metrics():
    start, end = 1_000_000_000, 4_000_000_000
    incomplete = [
        {"resource": _resource_fixture(start + 200_000_000)},
        {"resource": _resource_fixture(start + 2_200_000_000, cpu_ticks=20)},
        {"resource": _resource_fixture(end + 10_000_000, cpu_ticks=21)},
    ]
    missing = qualification._resource_summary(incomplete, window_complete=True,
        window_start_ns=start, window_end_ns=end)
    assert missing["measurement_window_complete"] is False
    assert missing["missing_second_buckets"] == [1]
    assert missing["max_sample_gap_ns"] is not None

    complete = [{"resource": _resource_fixture(start + offset, cpu_ticks=10 + index * 10)}
        for index, offset in enumerate((200_000_000, 1_200_000_000, 2_200_000_000))]
    complete.append({"resource": _resource_fixture(end + 10_000_000, cpu_ticks=41),
        "window_boundary_sample": True})
    summary = qualification._resource_summary(complete, window_complete=True,
        window_start_ns=start, window_end_ns=end)
    assert summary["measurement_window_complete"] is True
    assert summary["missing_second_buckets"] == []
    assert summary["worker_rss_max_bytes"] == 500
    assert summary["worker_instance_stable"] is True
    assert summary["sqlite_max_bytes"] == 500
    assert summary["wal_max_bytes"] == 100
    assert summary["sample_timestamps_monotonic_ns"] == [
        start + 200_000_000, start + 1_200_000_000, start + 2_200_000_000, end + 10_000_000]
    incomplete_pressure = [dict(item, resource=dict(item["resource"],
        system_psi={"cpu": {}, "memory": {}, "io": {}})) for item in complete]
    missing_pressure = qualification._resource_summary(incomplete_pressure, window_complete=True,
        window_start_ns=start, window_end_ns=end)
    assert missing_pressure["measurement_window_complete"] is False
    assert any("system_psi" in item["missing"] for item in missing_pressure["missing_sensor_fields"])


def test_diagnostic_failure_receipt_carries_missing_measurement_reasons(tmp_path):
    start, end = 10_000_000_000, 11_000_000_000
    measured, failure = qualification._qualification_measurements({}, stream_plan={"lane_topics": {}},
        repository_metrics={}, resource_summary={"measurement_window_complete": False},
        window_start_ns=start, window_end_ns=end)
    assert failure is not None
    failed_body = {"schema_version": 1,
        "status": "DIAGNOSTIC_FAILED", "reason": failure,
        "measured_window_seconds": 1, "measurement_window_complete": False,
        "diagnostic_measurements": measured}
    receipt = qualification._write_receipt(tmp_path, failed_body)
    assert receipt["status"] == "DIAGNOSTIC_FAILED"
    assert receipt["reason"].startswith("QUALIFICATION_TELEMETRY_INCOMPLETE_")
    assert receipt["diagnostic_measurements"]["missing_measurements"]
    receipt_path = tmp_path / qualification.RECEIPT_FILE
    original_bytes = receipt_path.read_bytes()
    assert qualification._write_receipt(tmp_path, failed_body) == receipt
    with pytest.raises(ValueError, match="QUALIFICATION_RECEIPT_IMMUTABLE_CONFLICT"):
        qualification._write_receipt(tmp_path, {**failed_body, "reason": "different failure"})
    assert receipt_path.read_bytes() == original_bytes
    with pytest.raises(ValueError, match="QUALIFICATION_COMPLETE_RECEIPT_TELEMETRY_INCOMPLETE"):
        qualification._write_receipt(tmp_path, {"schema_version": 1,
            "status": "DIAGNOSTIC_COMPLETE", "measured_window_seconds": 120,
            "measurement_window_complete": True, "diagnostic_measurements": measured})


def test_trade_geometry_distinguishes_zero_activity_from_missing_evidence():
    indexed = {"complete": True, "identity_rates": [
        {"lane": "BINANCE_MARKET", "channel": "btcusdt@aggTrade", "frames": 0}]}
    summary = qualification._trade_geometry_summary(indexed, ())
    assert summary["complete"] is True
    assert summary["measurement"] == "NO_INDEXED_FRAMES"
    assert summary["indexed_frames"] == 0
    assert summary["trade_rows"] == 0
    assert summary["mean_trades_per_indexed_frame"] is None
    assert summary["by_identity"][0]["measurement"] == "NO_INDEXED_FRAMES"

    incomplete_index = qualification._trade_geometry_summary({"complete": False,
        "identity_rates": []}, ())
    assert incomplete_index["complete"] is False
    assert incomplete_index["measurement"] == "INCOMPLETE"
    assert incomplete_index["missing_identities"] == [{"reason": "INDEXED_TRAFFIC_INCOMPLETE"}]


def test_trade_geometry_accounts_for_partially_populated_lanes():
    indexed = {"complete": True, "identity_rates": [
        {"lane": "BYBIT", "channel": "publicTrade.BTCUSDT", "frames": 2},
        {"lane": "BINANCE_MARKET", "channel": "btcusdt@aggTrade", "frames": 0}]}
    summary = qualification._trade_geometry_summary(indexed, ({
        "lane": "BYBIT", "channel": "publicTrade.BTCUSDT", "parsed_frames": 2,
        "trade_rows": 3, "frames_with_trades": 2,
        "min_trades_per_frame": 1, "max_trades_per_frame": 2,
    },))
    assert summary["complete"] is True
    assert summary["measurement"] == "COMPLETE"
    assert summary["indexed_frames"] == 2
    assert summary["mean_trades_per_indexed_frame"] == 1.5
    by_identity = {(item["lane"], item["channel"]): item for item in summary["by_identity"]}
    assert by_identity[("BINANCE_MARKET", "btcusdt@aggTrade")]["measurement"] == "NO_INDEXED_FRAMES"
    assert by_identity[("BYBIT", "publicTrade.BTCUSDT")]["trade_rows"] == 3


def test_missing_trade_geometry_and_empty_stream_cannot_complete_receipt(tmp_path):
    indexed = {"complete": True, "identity_rates": [
        {"lane": "BINANCE_MARKET", "channel": "btcusdt@aggTrade", "frames": 1}]}
    missing = qualification._trade_geometry_summary(indexed, ())
    assert missing["complete"] is False
    assert missing["measurement"] == "INCOMPLETE"
    assert missing["missing_identities"] == [{"lane": "BINANCE_MARKET",
        "channel": "btcusdt@aggTrade", "indexed_frames": 1, "parsed_frames": None}]

    zero = qualification._trade_geometry_summary({"complete": True, "identity_rates": [
        {"lane": "BINANCE_MARKET", "channel": "btcusdt@aggTrade", "frames": 0}]}, ())
    measurements = {"complete": True, "missing_measurements": [],
        "source_traffic": {"complete": True}, "durable_capture_traffic": {"complete": True},
        "indexed_traffic": {"complete": True}, "trade_geometry": zero,
        "payload_geometry": {"measurement": "COMPLETE"},
        "resource_sampling": {"measurement_window_complete": True}}
    with pytest.raises(ValueError, match="QUALIFICATION_COMPLETE_RECEIPT_TELEMETRY_INCOMPLETE"):
        qualification._write_receipt(tmp_path, {"schema_version": 1,
            "status": "DIAGNOSTIC_COMPLETE", "measured_window_seconds": 120,
            "measurement_window_complete": True, "diagnostic_measurements": measurements})
    nonempty = {**measurements, "trade_geometry": {"complete": True,
        "measurement": "COMPLETE", "indexed_frames": 1}}
    accepted = qualification._write_receipt(tmp_path / "nonempty", {"schema_version": 1,
        "status": "DIAGNOSTIC_COMPLETE", "measured_window_seconds": 120,
        "measurement_window_complete": True, "diagnostic_measurements": nonempty})
    assert accepted["status"] == "DIAGNOSTIC_COMPLETE"


def test_repository_reports_window_scoped_sqlite_commit_timing(tmp_path):
    start = time.monotonic_ns()
    end = start + 30_000_000_000
    with OpsRepository(tmp_path / "timing.sqlite") as repository:
        repository.begin_persistence_timing_window(start_ns=start, end_ns=end)
        ref = sha256_json({"fixture": "timing-window-commit"})
        entry = ArtifactIndexEntryV2(ref, "QualificationTimingFixtureV1", ref,
            time.time_ns(), time.time_ns(), {"fixture": "timing-window-commit"})
        with repository.atomic_composition():
            repository.register_artifact(entry)
        timing = repository.persistence_timing_window()
    assert timing["transaction_count"] == 1
    assert timing["successful_transaction_count"] == 1
    assert timing["commit_attempt_count"] == 1
    assert timing["commit_duration_sum_ns"] >= 0
    assert timing["row_and_archive_duration_sum_ns"] >= 0


def test_phase_window_preserves_worker_phase_semantics_without_summing():
    from atlas.v2.data.qualification_telemetry import QualificationPhaseWindowV1

    phases = QualificationPhaseWindowV1(("worker_execution", "seal_worker_execution"))
    phases.begin_window(100, 1_000_000_100)
    phases.record(at_ns=200, phase="worker_execution", duration_ns=700)
    phases.record(at_ns=200, phase="seal_worker_execution", duration_ns=700)
    phases.record(at_ns=1_000_000_100, phase="worker_execution", duration_ns=900)
    result = phases.snapshot()
    assert result["phases"]["worker_execution"] == {"count": 1, "sum_ns": 700, "max_ns": 700}
    assert result["phases"]["seal_worker_execution"] == {"count": 1, "sum_ns": 700, "max_ns": 700}


def test_causal_reader_excludes_unobserved_and_future_publication(tmp_path):
    ref = sha256_json({"qualification": "causal-publication"})
    instrument = {"venue": "BINANCE", "product": "LINEAR_PERPETUAL", "environment": "MAINNET",
        "native_symbol": "BTCUSDT", "contract_revision": "r1"}
    source_id, channel, metadata_ref = "BINANCE_MARKET_PUBLIC_WS_BROAD_V2", "aggTrade.BTCUSDT", "b" * 64
    feed_ref = sha256_json({"instrument": instrument, "source_id": source_id,
        "channel": channel, "metadata_ref": metadata_ref})
    entry = ArtifactIndexEntryV2(ref, "PublicStreamSourceHealthEvidenceV1", ref, 100, 100,
        {"observation": {"instrument": instrument, "source_id": source_id,
            "channel": channel, "metadata_ref": metadata_ref, "available_at_ns": 100,
            "trade_id": "trade-1", "trade_payload_hash": "c" * 64}})
    publication_id = sha256_json({"publication": "qualification-causal"})
    trade = {"feed_ref": feed_ref, "trade_id": "trade-1", "payload_hash": "c" * 64,
        "artifact_ref": ref, "available_at_ns": 100}
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        with repository._transaction() as connection:
            repository.publish_stream_slice_v1(connection, publication_id=publication_id,
                run_id="qualification-run", descriptor_hash="d" * 64, slice_id="complete",
                logical_ready_at_ns=300, entries=(entry,), trade_identities=(trade,))
        assert repository.effective_available_at_ns(ref) is None
        assert repository.latest_artifact_entries(
            entry.artifact_type, as_of_ns=500, limit=4).entries == ()
        assert repository.unobserved_publications_v2("qualification-run") == (publication_id,)
        observed_at = repository.observe_publication_v2(publication_id, observed_at_ns=350)
        assert observed_at == 350
        assert repository.effective_available_at_ns(ref) == 350
        assert repository.latest_artifact_entries(
            entry.artifact_type, as_of_ns=349, limit=4).entries == ()
        assert repository.latest_artifact_entries(
            entry.artifact_type, as_of_ns=350, limit=4).entries == (entry,)
        assert repository.unobserved_publications_v2("qualification-run") == ()


def test_causal_read_requires_every_active_stream_checkpoint():
    now_ns = time.time_ns()
    revision = sha256_json({"fixture": "checkpoint-set"})
    key = InstrumentKeyV2(VenueV2.BINANCE, EnvironmentV2.MAINNET,
        ProductTypeV2.LINEAR_PERPETUAL, "BTCUSDT", "BTC", "USDT", "USDT", revision)
    product = SimpleNamespace(key=key, metadata_ref="a" * 64)
    identities = (
        SimpleNamespace(venue=VenueV2.BINANCE, key=key, channel="aggTrade.BTCUSDT"),
        SimpleNamespace(venue=VenueV2.BINANCE, key=key, channel="BTCUSDT@depth@100ms"),
    )
    plan = SimpleNamespace(identities=identities)
    port = SimpleNamespace(public_stream_source=SimpleNamespace(plan=plan),
        public_source=SimpleNamespace(current_products=(product,)))
    first_feed = sha256_json({"instrument": key.to_dict(),
        "source_id": "BINANCE_MARKET_PUBLIC_WS_BROAD_V2", "channel": identities[0].channel,
        "metadata_ref": product.metadata_ref})

    class IncompleteRepository:
        @staticmethod
        def latest_public_stream_operational_checkpoints_v1(*, run_id, feed_refs, as_of_ns):
            assert run_id == "run" and len(feed_refs) == 2 and as_of_ns == now_ns
            return {first_feed: SimpleNamespace(artifact_ref="checkpoint-ref")}

        @staticmethod
        def effective_available_at_ns(_ref):
            return now_ns - 1

    assert qualification._causal_checkpoint_read(IncompleteRepository(), run_id="run", port=port,
        cutoff_ns=now_ns) == ()
    with pytest.raises(ValueError, match="CHECKPOINT_INCOMPLETE"):
        qualification._causal_checkpoint_read(IncompleteRepository(), run_id="run", port=port,
            cutoff_ns=now_ns, require_complete=True)


def test_real_binance_stream_runtime_commits_and_reads_only_observed_fixture(tmp_path):
    from atlas.v2.runtime.broad_public_runtime import BroadPublicRuntimeV2

    now_ns = time.time_ns()
    revision = sha256_json({"fixture": "binance-btc-revision"})
    key = InstrumentKeyV2(VenueV2.BINANCE, EnvironmentV2.MAINNET,
        ProductTypeV2.LINEAR_PERPETUAL, "BTCUSDT", "BTC", "USDT", "USDT", revision)
    product = ProductContractV2(key, now_ns, now_ns, now_ns, Decimal("0.001"), Decimal("0.01"),
        Decimal("0.001"), Decimal("0.001"), TradingStatusV2.TRADING, revision)

    async def frames():
        trade_id = 1
        while True:
            received = time.time_ns()
            payload = json.dumps({"stream": "btcusdt@aggTrade", "data": {
                "e": "aggTrade", "E": received // 1_000_000,
                "s": "BTCUSDT", "a": trade_id, "p": "100.01", "q": "0.01",
                "T": received // 1_000_000, "m": False}}, separators=(",", ":")).encode()
            yield CapturedPublicFrameV2(VenueV2.BINANCE, "BINANCE_MARKET_PUBLIC_WS_BROAD_V2",
                "btcusdt@aggTrade", payload, hashlib.sha256(payload).hexdigest(), received, received, 1)
            trade_id += 1
            await asyncio.sleep(0.025)

    runtime = BroadPublicRuntimeV2(stream_factories={"BINANCE_MARKET": frames},
        capture_payload_metrics=True)
    run_root = tmp_path / "fixture-run"
    run_root.mkdir()
    try:
        with OpsRepository(run_root / "ops.sqlite") as repository:
            runtime.recover(repository, run_root=run_root, products=(product,), tiers={},
                now_ns=now_ns, benchmark_keys=(key,))
            assert runtime.plan is not None
            assert {identity.venue for identity in runtime.plan.identities} == {VenueV2.BINANCE}
            metric_start = time.monotonic_ns() + 100_000_000
            runtime.begin_qualification_window(metric_start, metric_start + 10_000_000_000)
            time.sleep(max(0, metric_start - time.monotonic_ns()) / 1_000_000_000)
            deadline = time.monotonic() + 5
            while runtime.status().indexed_frames == 0 and time.monotonic() < deadline:
                runtime.service(repository, now_ns=time.time_ns())
                time.sleep(0.01)
            assert runtime.status().indexed_frames > 0
            runtime.finish(repository)
            status = runtime.status()
            assert status.handoff.venue == "BROAD"
            assert status.handoff.loss_latched is False
            assert status.handoff.frames_rejected == 0
            assert status.capture["pending_frames"] == 0
            assert status.indexed_frames == status.capture["captured_frames"]
            assert repository.unobserved_publications_v2(run_root.name) == ()
            diagnostic_metrics = runtime.qualification_metrics_snapshot()
            source_frames = sum(row["frames"] for lane in diagnostic_metrics["source"].values()
                for row in lane["identities"])
            capture_frames = sum(row["frames"] for row in diagnostic_metrics["capture"]["traffic"]["identities"])
            indexed_frames = sum(row["frames"] for row in diagnostic_metrics["index"]["identities"])
            assert source_frames > 0
            assert capture_frames > 0
            assert indexed_frames > 0
            trade_geometry = diagnostic_metrics["trade_geometry"]
            assert trade_geometry
            assert sum(int(item["trade_rows"] or 0) for item in trade_geometry) == sum(
                int(item["parsed_frames"] or 0) for item in trade_geometry)
            assert diagnostic_metrics["phase_timings_ns"]["window_started"] is True
            assert diagnostic_metrics["phase_timings_ns"]["phases"]["public_commit"]["count"] > 0
            archive_metrics = diagnostic_metrics["capture"]["archive"]
            assert archive_metrics["operation_count"] > 0
            assert archive_metrics["extent_count"] > 0
            assert archive_metrics["archive_bytes_written"] > 0
            assert archive_metrics["fsync_ns"] >= 0
            worker_resource = runtime.qualification_worker_resource_sample()
            assert worker_resource is not None
            assert worker_resource["cpu_ticks_per_second"] > 0
            assert worker_resource["rss_bytes"] > 0
            source_id = "BINANCE_MARKET_PUBLIC_WS_BROAD_V2"
            feed_ref = sha256_json({"instrument": key.to_dict(), "source_id": source_id,
                "channel": "btcusdt@aggTrade", "metadata_ref": product.metadata_ref})
            cutoff = time.time_ns()
            checkpoints = repository.latest_public_stream_operational_checkpoints_v1(
                run_id=run_root.name, feed_refs=(feed_ref,), as_of_ns=cutoff)
            assert feed_ref in checkpoints
            entry = checkpoints[feed_ref]
            assert repository.effective_available_at_ns(entry.artifact_ref) <= cutoff
    finally:
        runtime.close()


def test_ordinary_v2_start_rejects_before_public_factory_when_capacity_missing(tmp_path, monkeypatch):
    import atlas.v2.product as product
    import atlas.v2.runtime.production as production

    run_root = tmp_path / "normal-data"
    config = qualification.qualification_config(run_root)
    run = product.create_run(run_root, config)
    assert product._run_config(json.loads((run / "run.json").read_text())["configuration"]) == config
    monkeypatch.setattr(product, "preflight_run", lambda _run: {"allowed": False})
    constructed = False

    def forbidden_factory(**_kwargs):
        nonlocal constructed
        constructed = True
        raise AssertionError("public source must not be constructed before capacity admission")

    monkeypatch.setattr(production, "create_broad_public_port", forbidden_factory)
    with pytest.raises(RuntimeError, match="OWNER_STORAGE_PREFLIGHT_REJECTED"):
        product.run_component(run)
    assert not constructed
    from atlas.v2.runtime import storage_preflight

    assert storage_preflight.S41_MAX_BREADTH_BYTES_PER_CYCLE_V2 is None


def test_qualification_marker_blocks_ordinary_run_and_restart(tmp_path, monkeypatch):
    import atlas.v2.product as product

    run = _run_with_qualification_marker(tmp_path)
    monkeypatch.setattr(product, "preflight_run", lambda _run: pytest.fail("preflight must not admit diagnostic run"))
    with pytest.raises(RuntimeError, match="QUALIFICATION_ONLY_RUN_CANNOT_ENTER_ORDINARY_STARTUP"):
        product.run_component(run)
    with pytest.raises(RuntimeError, match="QUALIFICATION_ONLY_RUN_CANNOT_ENTER_ORDINARY_STARTUP"):
        product.launch_run(run)
    with OpsRepository(run / "ops.sqlite"):
        pass
    with pytest.raises(ValueError, match="CANNOT_RESUME"):
        qualification._assert_fresh_run(run)


def test_cancellation_before_measurement_emits_failed_receipt_without_decisions(tmp_path, monkeypatch):
    import atlas.v2.runtime.storage_preflight as storage_preflight

    run = _run_with_qualification_marker(tmp_path)
    monkeypatch.setattr(storage_preflight, "qualify_storage_path",
        lambda *_args, **_kwargs: SimpleNamespace(as_dict=lambda: {**_host_probe(
            Path(json.loads((run / qualification.PROFILE_BINDING_FILE).read_text())["selected_path"])),
            "content_hash": "e" * 64}))

    class EmptyPublicSource:
        enabled_venues = (VenueV2.BINANCE,)
        required_source_ids = (qualification.BINANCE_REST_SOURCE_ID,)
        bybit_reader = None
        current_products = ()

        @staticmethod
        def close():
            return None

    class EmptyRuntime:
        plan = None
        capture = SimpleNamespace(
            _capture=SimpleNamespace(_capture_payload_metrics=True),
            status=lambda: SimpleNamespace(capture={}))

        @staticmethod
        def status():
            handoff = SimpleNamespace(
                venue="BROAD", queue_items=0, queue_bytes=0, max_queue_items=512,
                max_queue_bytes=16_000_000, high_water_items=0, high_water_bytes=0,
                frames_received=0, frames_drained=0, frames_rejected=0, loss_latched=False,
            )
            return SimpleNamespace(state="CREATED", handoff=handoff, capture={
                "captured_frames": 0, "delivered_frames": 0, "pending_frames": 0,
                "terminal_error": None}, indexed_frames=0, max_service_gap_ns=0, lanes={})

        @staticmethod
        def progress_snapshot():
            return {"stream_source_states": {}, "stream_unresolved_gap": False,
                "stream_ingestion_failed": False}

        @staticmethod
        def close():
            return None

    class EmptyPort:
        public_source = EmptyPublicSource()
        public_stream_source = EmptyRuntime()
        last_acquisition_snapshot = None

        @staticmethod
        def recover(_repository, *, now_ns):
            return SimpleNamespace()

        @staticmethod
        def finish_public_capture(_repository):
            return None

    receipt = qualification.run_qualification_component(run,
        supervisor_token="qualification-test-supervisor-token", stop_requested=lambda: True,
        public_port_factory=lambda **_kwargs: EmptyPort())
    assert receipt["status"] == "DIAGNOSTIC_FAILED"
    assert receipt["reason"] == "CANCELLED_DURING_WARMUP"
    assert receipt["capacity_certificate_issued"] is False
    assert receipt["production_admission"] is False
    assert not (run / "status.json").exists()
    assert (run / "qualification-start.json").is_file()
    with pytest.raises(ValueError, match="CANNOT_RESUME"):
        qualification.run_qualification_component(run,
            supervisor_token="qualification-test-supervisor-token", stop_requested=lambda: True,
            public_port_factory=lambda **_kwargs: EmptyPort())


def test_diagnostic_child_requires_supervisor_token(tmp_path):
    run = _run_with_qualification_marker(tmp_path)
    with pytest.raises(ValueError, match="SUPERVISOR_TOKEN_INVALID"):
        qualification.run_qualification_component(run, stop_requested=lambda: True,
            public_port_factory=lambda **_kwargs: pytest.fail("unauthorized child started"))


def test_parent_rejects_unsafe_path_before_host_probe_or_child(tmp_path, monkeypatch):
    monkeypatch.setattr(qualification, "run_host_path_probe",
        lambda *_args, **_kwargs: pytest.fail("unsafe path must stop before probe"))
    receipt, status = qualification.commission_binance_diagnostic(Path("/"))
    assert status == 2
    assert receipt["status"] == "DIAGNOSTIC_FAILED"
    assert receipt["capacity_certificate_issued"] is False
    assert receipt["capital_enabled"] is False


def test_supervisor_binds_one_time_child_and_private_run_tree(tmp_path, monkeypatch):
    import subprocess

    import atlas.v2.product as product

    selected = tmp_path / "selected-data"
    captured: dict[str, object] = {}
    build = product.build_identity()

    def approved_path_probe(path: Path, *, identity_sha256: str) -> dict:
        return _host_probe(path.resolve(), identity_sha256)

    def blocked_child(*_args, **kwargs):
        captured.update(kwargs)
        raise OSError("test child launch stop")

    monkeypatch.setattr(qualification, "run_host_path_probe", approved_path_probe)
    monkeypatch.setattr(product, "build_identity", lambda: build)
    monkeypatch.setattr(subprocess, "Popen", blocked_child)
    receipt, exit_code = qualification.commission_binance_diagnostic(selected)
    assert exit_code == 2
    assert receipt["status"] == "DIAGNOSTIC_FAILED"
    assert receipt["capacity_certificate_issued"] is False
    run_candidates = tuple((selected / "s41-qualification-only").glob("*/runs/*"))
    assert len(run_candidates) == 1, receipt
    run = run_candidates[0]
    for path in (run, *run.rglob("*")):
        info = path.lstat()
        assert not stat.S_ISLNK(info.st_mode)
        assert info.st_uid == os.geteuid()
        assert stat.S_IMODE(info.st_mode) & 0o077 == 0
    binding = json.loads((run / qualification.PROFILE_BINDING_FILE).read_text())
    child_token = captured["env"]["ATLAS_QUALIFICATION_PARENT_TOKEN"]
    assert hashlib.sha256(child_token.encode("ascii")).hexdigest() == binding["supervisor_token_sha256"]
    assert child_token not in json.dumps(binding)
    assert (run / qualification.RECEIPT_FILE).is_file()
