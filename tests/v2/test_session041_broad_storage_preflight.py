"""The S41 broad-workload storage estimate is additive to the frozen S40 probe."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from atlas.v2 import product
from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.memory.writer_lock import OpsWriterAlreadyActive, OpsWriterLock
from atlas.v2.runtime.storage_preflight import (
    S41_BROAD_REFRESH_CADENCE_SECONDS_V2,
    S41_BROAD_RETENTION_HOURS_V2,
    S41_MAX_ACTIVE_PRODUCTS_V2,
    StoragePreflightLimitsV1,
    assess_broad_storage_capacity_v2,
)

IDENTITY = "a" * 64


def test_missing_measurement_fails_closed_with_a_bounded_broad_workload_envelope():
    result = assess_broad_storage_capacity_v2(identity_sha256=IDENTITY,
        selected_path="/tmp/atlas-data", observed_at_ns=123,
        enabled_venues=("BYBIT", "BINANCE"), observed_free_bytes=10**15,
        owner_free_disk_reserve_bytes=1_073_741_824)
    body = result.as_dict()
    assert result.allowed is False and result.status == "TEST GATE"
    assert result.capacity_qualified is False
    assert result.measured_max_cycle_bytes is None
    assert result.projected_workload_bytes is None and result.required_free_bytes is None
    assert result.reasons == ("BROAD_WORKLOAD_CAPACITY_MEASUREMENT_UNAVAILABLE",)
    assert body["selected_max_products"] == S41_MAX_ACTIVE_PRODUCTS_V2 == 4096
    assert body["refresh_cadence_seconds"] == S41_BROAD_REFRESH_CADENCE_SECONDS_V2 == 10
    assert body["retention_hours"] == S41_BROAD_RETENTION_HOURS_V2 == 48
    assert body["projection_class"] == "CONSERVATIVE_OFFLINE_ENVELOPE_NOT_ENDURANCE_QUALIFICATION"
    assert body["authority"] == "ZERO" and body["capital_enabled"] is body["assisted_enabled"] is False
    assert body["content_hash"] == sha256_json({key: value for key, value in body.items()
        if key != "content_hash"})


def test_measured_envelope_scales_for_enabled_venue_count_and_rejects_low_headroom():
    projected_one_venue = ((1_000_000 * 3 + 1) // 2) * 17_280
    projected_two_venues = ((2_000_000 * 3 + 1) // 2) * 17_280
    one = assess_broad_storage_capacity_v2(identity_sha256=IDENTITY,
        selected_path="/tmp/atlas-data", observed_at_ns=123,
        enabled_venues=("BYBIT",), observed_free_bytes=projected_one_venue * 2,
        owner_free_disk_reserve_bytes=100, measured_max_cycle_bytes=2_000_000)
    two = assess_broad_storage_capacity_v2(identity_sha256=IDENTITY,
        selected_path="/tmp/atlas-data", observed_at_ns=123,
        enabled_venues=("BYBIT", "BINANCE"), observed_free_bytes=projected_two_venues * 2,
        owner_free_disk_reserve_bytes=100, measured_max_cycle_bytes=2_000_000)
    assert one.projected_workload_bytes == projected_one_venue
    assert two.projected_workload_bytes == projected_two_venues
    assert one.selected_max_products == 2048 and two.selected_max_products == 4096
    assert one.required_free_bytes == projected_one_venue * 5 // 4 + 100
    assert one.allowed and two.allowed and not one.capacity_qualified

    insufficient = assess_broad_storage_capacity_v2(identity_sha256=IDENTITY,
        selected_path="/tmp/atlas-data", observed_at_ns=123,
        enabled_venues=("BYBIT", "BINANCE"), observed_free_bytes=projected_two_venues,
        owner_free_disk_reserve_bytes=100, measured_max_cycle_bytes=2_000_000)
    assert not insufficient.allowed and insufficient.status == "TEST GATE"
    assert "BROAD_WORKLOAD_DISK_HEADROOM_INSUFFICIENT" in insufficient.reasons


def test_v2_product_preflight_preserves_host_contract_and_gates_unmeasured_breadth(
        tmp_path, monkeypatch):
    monkeypatch.setattr(product, "build_identity", lambda: {
        "source_sha": "a" * 40, "version": "2.0.41.0", "runtime_lock_sha256": "b" * 64})
    config = product.ResearchRunConfigV2(data_root=str(tmp_path.resolve()),
        public_venues=("BYBIT", "BINANCE"))
    run = product.create_run(tmp_path, config)
    manifest = product.load_run(run)
    host = {
        "schema_version": 1, "version": "StoragePreflightResultV1",
        "selected_path": str(run.resolve()), "identity_sha256": manifest["content_hash"],
        "started_at_ns": 1, "completed_at_ns": 1, "elapsed_ns": 0,
        "status": "TESTED", "allowed": True, "reasons": [],
        "limits": StoragePreflightLimitsV1().as_dict(), "measurements": [],
        "facts": {"free_disk_bytes": 10**15}, "probe_mode": "HOST_PATH", "authority": "ZERO",
        "capital_enabled": False, "assisted_enabled": False,
        "live_source_qualification": "TEST GATE", "endurance_qualification": "TEST GATE",
    }
    host["content_hash"] = sha256_json({key: value for key, value in host.items()
        if key != "content_hash"})

    def child(command, **options):
        assert command[command.index("--component") + 1] == "preflight"
        with pytest.raises(OpsWriterAlreadyActive):
            OpsWriterLock(run / "ops.sqlite").acquire()
        return SimpleNamespace(returncode=0, stdout=json.dumps(host).encode())

    monkeypatch.setattr(product.subprocess, "run", child)
    result = product.preflight_run(run)
    assert result["version"] == "OwnerPreflightResultV2"
    assert result["allowed"] is False and result["status"] == "TEST GATE"
    assert result["host_preflight"] == host
    assert result["broad_capacity"]["identity_sha256"] == manifest["content_hash"]
    assert result["broad_capacity"]["selected_path"] == str(run.resolve())
    assert result["broad_capacity"]["observed_at_ns"] == host["completed_at_ns"]
    assert "BROAD_WORKLOAD_CAPACITY_MEASUREMENT_UNAVAILABLE" in result["reasons"]
    assert result["content_hash"] == sha256_json({key: value for key, value in result.items()
        if key != "content_hash"})
    assert json.loads((run / "preflight.json").read_text()) == result
    raw_host_results = [path for path in (run / "preflight-results").glob("*.json")
        if not path.name.endswith((".broad-capacity.json", ".owner.json"))]
    assert len(raw_host_results) == 1
    assert json.loads(raw_host_results[0].read_text()) == host
    capacity_results = tuple((run / "preflight-results").glob("*.broad-capacity.json"))
    assert len(capacity_results) == 1
    assert json.loads(capacity_results[0].read_text()) == result["broad_capacity"]
    owner_results = tuple((run / "preflight-results").glob("*.owner.json"))
    assert len(owner_results) == 1 and json.loads(owner_results[0].read_text()) == result
    assert result["broad_capacity_ref"] == result["broad_capacity"]["content_hash"]
    assert not (run / "ops.sqlite").exists()


def test_v2_runtime_resource_reserve_uses_current_projection_and_gates_corruption(
        tmp_path, monkeypatch):
    monkeypatch.setattr(product, "build_identity", lambda: {
        "source_sha": "a" * 40, "version": "2.0.41.0", "runtime_lock_sha256": "b" * 64})
    config = product.ResearchRunConfigV2(data_root=str(tmp_path.resolve()),
        public_venues=("BYBIT", "BINANCE"))
    run = product.create_run(tmp_path, config)
    manifest = product.load_run(run)
    host = {
        "schema_version": 1, "version": "StoragePreflightResultV1",
        "selected_path": str(run.resolve()), "identity_sha256": manifest["content_hash"],
        "started_at_ns": 1, "completed_at_ns": 2, "elapsed_ns": 1,
        "status": "TESTED", "allowed": True, "reasons": [],
        "limits": StoragePreflightLimitsV1().as_dict(), "measurements": [],
        "facts": {"free_disk_bytes": 10**15}, "probe_mode": "HOST_PATH", "authority": "ZERO",
        "capital_enabled": False, "assisted_enabled": False,
        "live_source_qualification": "TEST GATE", "endurance_qualification": "TEST GATE",
    }
    host["content_hash"] = sha256_json({key: value for key, value in host.items()
        if key != "content_hash"})

    from atlas.v2.runtime import storage_preflight

    monkeypatch.setattr(storage_preflight, "S41_MAX_BREADTH_BYTES_PER_CYCLE_V2", 2_000_000)
    monkeypatch.setattr(product.subprocess, "run", lambda *args, **kwargs:
        SimpleNamespace(returncode=0, stdout=json.dumps(host).encode()))
    result = product.preflight_run(run)
    assert result["allowed"] is True
    reserve = result["broad_capacity"]["required_free_bytes"]
    fields = product.owner_storage_reserve_fields(run, manifest, config)
    assert fields["broad_capacity_status"] == "TESTED"
    assert fields["disk_reserve_bytes"] >= reserve
    assert fields["disk_reserve_bytes"] > StoragePreflightLimitsV1().minimum_free_bytes
    assert fields["broad_capacity_required_free_bytes"] == reserve

    tampered = json.loads(json.dumps(result))
    tampered["broad_capacity"]["required_free_bytes"] = 1
    broad_body = {key: value for key, value in tampered["broad_capacity"].items()
        if key != "content_hash"}
    tampered["broad_capacity"]["content_hash"] = sha256_json(broad_body)
    tampered["broad_capacity_ref"] = tampered["broad_capacity"]["content_hash"]
    owner_body = {key: value for key, value in tampered.items() if key != "content_hash"}
    tampered["content_hash"] = sha256_json(owner_body)
    (run / "preflight.json").write_text(json.dumps(tampered), encoding="utf-8")
    invalid = product.owner_storage_reserve_fields(run, manifest, config)
    assert invalid["broad_capacity_status"] == "TEST GATE"
    assert invalid["broad_capacity_reason"] == "BROAD_CAPACITY_SIDECAR_MISMATCH"
    assert invalid["disk_reserve_bytes"] is None
    assert invalid["resource_pressure"] is True


def test_recent_alert_product_view_marks_bounded_page_truncation(tmp_path, monkeypatch):
    monkeypatch.setattr(product, "build_identity", lambda: {
        "source_sha": "a" * 40, "version": "2.0.41.0", "runtime_lock_sha256": "b" * 64})
    run = product.create_run(tmp_path, product.ResearchRunConfigV1())
    with OpsRepository(run / "ops.sqlite") as repository:
        for index in range(2):
            ref = sha256_json({"event-alert-fixture": index})
            event_ref = sha256_json({"event-fixture": index})
            entry = ArtifactIndexEntryV2(ref, "EventAlertV2", ref, index + 1, index + 1,
                {"alert": {"event_ref": event_ref, "relevance": "WATCH"}})
            repository.register_artifact(entry)
    alerts = product.recent_event_alerts_for_run(run, limit=1)
    assert len(alerts) == 2
    assert alerts[0]["relevance"] == "WATCH"
    assert alerts[1]["relevance"] == "RECENT_ALERT_LIST_TRUNCATED"
