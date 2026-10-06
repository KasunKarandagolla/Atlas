"""Host rejection is measurable, temporary-only and does not authorize a run."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.runtime import storage_preflight as preflight

IDENTITY = "a" * 64


def _limits():
    return preflight.StoragePreflightLimitsV1(sustained_batches=2, burst_batches=2)


def _hooks(**changes):
    return replace(preflight.StoragePreflightHooksV1(sleep=lambda _seconds: None,
        free_bytes=lambda _path: 100_000_000_000, monotonic_ns=lambda: 0), **changes)


def _assert_clean(path):
    assert not list(path.glob(".atlas-preflight-*"))


def test_real_temp_store_replays_reopens_exports_and_does_not_touch_existing_run(tmp_path):
    selected = tmp_path / "selected path Ω"
    selected.mkdir()
    existing = selected / "preserved-owner-run"
    existing.mkdir()
    with OpsRepository(existing / "ops.sqlite"):
        pass
    preserved = (existing / "ops.sqlite").read_bytes()
    result = preflight.qualify_storage_path(selected, identity_sha256=IDENTITY, limits=_limits(), hooks=_hooks())
    assert result.allowed, result.reasons
    assert result.status == "TESTED" and result.probe_mode == "FAULT_INJECTION"
    assert result.facts["sqlite_journal_mode"] == "wal"
    assert result.facts["sqlite_synchronous"] == 2
    assert result.facts["generated_frame_count"] == 256
    assert result.facts["generated_archive_extents"] == 4
    assert result.facts["generated_index_rows"] == 261
    assert result.facts["sole_writer_lease_enforced"] is True
    assert result.facts["sqlite_integrity"] == "ok"
    assert result.facts["pinned_reader_checkpoint"][1] > result.facts["pinned_reader_checkpoint"][2]
    assert result.facts["wal_checkpoint"][1] == result.facts["wal_checkpoint"][2]
    assert result.facts["raw_archive_bytes"] > 0
    assert result.facts["real_run_database_opened"] is False
    assert result.facts["probe_removed"] is True
    assert len(result.facts["export_smoke"]["validation_failures"]) == 0
    assert result.facts["export_smoke"]["has_more"] is False
    assert (existing / "ops.sqlite").read_bytes() == preserved
    body = result.as_dict()
    assert body["content_hash"] == sha256_json({key: value for key, value in body.items() if key != "content_hash"})
    assert body["authority"] == "ZERO"
    assert body["capital_enabled"] is body["assisted_enabled"] is False
    assert body["live_source_qualification"] == body["endurance_qualification"] == "TEST GATE"
    with pytest.raises(FrozenInstanceError):
        result.status = "IMPLEMENTED"
    with pytest.raises(TypeError):
        result.facts["probe_removed"] = False
    _assert_clean(selected)


def test_limits_derive_queue_stall_rate_and_disk_reserve_without_48h_claim():
    limits = preflight.StoragePreflightLimitsV1()
    assert limits.maximum_operation_ns == 800_000_000
    assert limits.maximum_mean_service_ns == 200_000_000
    assert limits.minimum_free_bytes == 62_930_192_327
    assert limits.minimum_free_bytes > preflight.S39_MEASURED_48H_PROJECTION_BYTES
    assert limits.as_dict()["projection_class"] == "MEASURED_S39_SHORT_RUN_ESTIMATE_NOT_GUARANTEED"
    for changes in ({"queue_capacity_frames": 513}, {"safety_margin": 1}, {"total_budget_seconds": 61},
                    {"sustained_batches": 65}, {"burst_batches": 0}, {"batch_frames": True}):
        with pytest.raises(ValueError):
            preflight.StoragePreflightLimitsV1(**changes)


def test_low_disk_blocks_before_probe_or_dependency_work(tmp_path):
    dependencies = []
    result = preflight.qualify_storage_path(tmp_path, identity_sha256=IDENTITY, limits=_limits(),
        hooks=_hooks(free_bytes=lambda _path: 23_000_000_000,
                     dependencies=lambda: dependencies.append("must-not-run")))
    assert not result.allowed and result.status == "TEST GATE"
    assert result.reasons == ("INSUFFICIENT_DISK_HEADROOM",)
    assert result.facts["free_disk_bytes"] == 23_000_000_000
    assert not dependencies
    _assert_clean(tmp_path)


@pytest.mark.parametrize("phase,exception", [
    ("PATH_CREATE", PermissionError),
    ("FILESYSTEM_WRITE_FLUSH_READ_RENAME_DELETE", OSError),
    ("RUNTIME_DEPENDENCIES", ImportError),
    ("STRICT_READONLY_EXPORT", RuntimeError),
    ("SQLITE_READONLY_REOPEN", OSError),
])
def test_predictable_failures_reject_with_exact_safe_reason_and_clean_up(tmp_path, phase, exception):
    def inject(name):
        if name == phase:
            raise exception("sensitive exception detail must not enter result")

    result = preflight.qualify_storage_path(tmp_path, identity_sha256=IDENTITY, limits=_limits(),
                                            hooks=_hooks(phase=inject))
    assert result.reasons == (f"{phase}_FAILED_{exception.__name__}",)
    assert not result.allowed and result.status == "TEST GATE"
    assert "sensitive" not in str(result.as_dict())
    _assert_clean(tmp_path)


@pytest.mark.parametrize("pragma,reason", [
    ("PRAGMA journal_mode=DELETE", "SQLITE_WAL_UNAVAILABLE"),
    ("PRAGMA synchronous=NORMAL", "SQLITE_FULL_DURABILITY_UNAVAILABLE"),
])
def test_unsupported_wal_or_durability_semantics_block_start(tmp_path, pragma, reason):
    result = preflight.qualify_storage_path(tmp_path, identity_sha256=IDENTITY, limits=_limits(),
        hooks=_hooks(inspect_sqlite=lambda connection: connection.execute(pragma)))
    assert result.reasons == (reason,)
    assert not result.allowed
    _assert_clean(tmp_path)


class _Clock:
    def __init__(self):
        self.value = 0

    def now(self):
        return self.value


@pytest.mark.parametrize("phase", ["ARCHIVE_WRITE_FSYNC", "SQLITE_TRANSACTION_COMMIT"])
def test_measured_persistence_stall_exceeding_queue_headroom_rejects(tmp_path, phase):
    clock = _Clock()

    def inject(name):
        if name == phase:
            clock.value += 1_000_000_000

    result = preflight.qualify_storage_path(tmp_path, identity_sha256=IDENTITY, limits=_limits(),
        hooks=_hooks(phase=inject, monotonic_ns=clock.now))
    assert result.reasons == (f"{phase}_EXCEEDS_QUEUE_HEADROOM",)
    measurement = next(item for item in result.measurements if item.operation == phase)
    assert measurement.maximum_ns == 1_000_000_000
    assert not result.allowed
    _assert_clean(tmp_path)


def test_average_service_incapability_rejects_before_owner_live_run(tmp_path):
    clock = _Clock()

    def inject(name):
        if name == "ARCHIVE_WRITE_FSYNC":
            clock.value += 150_000_000
        if name == "SQLITE_TRANSACTION_COMMIT":
            clock.value += 150_000_000

    result = preflight.qualify_storage_path(tmp_path, identity_sha256=IDENTITY, limits=_limits(),
        hooks=_hooks(phase=inject, monotonic_ns=clock.now))
    assert result.reasons == ("SUSTAINED_BATCH_SERVICE_CANNOT_SUSTAIN_DECLARED_RATE",
                             "BURST_BATCH_SERVICE_CANNOT_SUSTAIN_DECLARED_RATE")
    assert not result.allowed
    assert next(item for item in result.measurements if item.operation == "ARCHIVE_WRITE_FSYNC").maximum_ns < 800_000_000
    _assert_clean(tmp_path)


def test_report_smoke_enforces_half_of_existing_fixed_budget(tmp_path):
    clock = _Clock()

    def inject(name):
        if name == "STRICT_READONLY_EXPORT":
            clock.value += 5_000_000_001

    result = preflight.qualify_storage_path(tmp_path, identity_sha256=IDENTITY, limits=_limits(),
        hooks=_hooks(phase=inject, monotonic_ns=clock.now))
    assert result.reasons == ("EXPORT_SMOKE_EXCEEDS_FIXED_BUDGET_SAFETY_MARGIN",)
    assert not result.allowed
    _assert_clean(tmp_path)


def test_probe_deadline_is_not_extended_by_later_success(tmp_path):
    clock = _Clock()

    def inject(name):
        if name == "RUNTIME_DEPENDENCIES":
            clock.value += 31_000_000_000

    result = preflight.qualify_storage_path(tmp_path, identity_sha256=IDENTITY, limits=_limits(),
        hooks=_hooks(phase=inject, monotonic_ns=clock.now))
    assert result.reasons == ("PREFLIGHT_TIME_BUDGET_EXCEEDED",)
    assert not result.allowed
    _assert_clean(tmp_path)


def test_unc_network_path_is_rejected_before_creation():
    from pathlib import Path

    result = preflight.qualify_storage_path(Path("//atlas-invalid-server/share"), identity_sha256=IDENTITY)
    assert result.reasons == ("NETWORK_SHARED_WRITABLE_SQLITE_UNSUPPORTED",)
    assert not result.allowed


def test_mapped_or_mounted_remote_path_is_not_accepted_as_local(tmp_path, monkeypatch):
    def remote(_path):
        raise preflight._ProbeRejected("NETWORK_SHARED_WRITABLE_SQLITE_UNSUPPORTED")

    monkeypatch.setattr(preflight, "_local_filesystem_type", remote)
    result = preflight.qualify_storage_path(tmp_path, identity_sha256=IDENTITY, hooks=_hooks())
    assert result.reasons == ("NETWORK_SHARED_WRITABLE_SQLITE_UNSUPPORTED",)
    assert not result.allowed
    _assert_clean(tmp_path)


def test_regressed_clock_cannot_produce_an_allowed_preflight(tmp_path):
    clock = _Clock()

    def inject(name):
        if name == "DISK_HEADROOM":
            clock.value -= 1

    result = preflight.qualify_storage_path(tmp_path, identity_sha256=IDENTITY,
        hooks=_hooks(phase=inject, monotonic_ns=clock.now))
    assert "MONOTONIC_CLOCK_REGRESSION" in result.reasons
    assert "PREFLIGHT_CLOCK_REGRESSION" in result.reasons
    assert not result.allowed
    _assert_clean(tmp_path)


def test_real_readonly_export_validation_failure_is_not_accepted(tmp_path):
    def export(_database, _destination, _identity):
        raise ValueError("strict validation rejected generated evidence")

    result = preflight.qualify_storage_path(tmp_path, identity_sha256=IDENTITY, limits=_limits(), hooks=_hooks(export=export))
    assert result.reasons == ("STRICT_READONLY_EXPORT_FAILED_ValueError",)
    assert not result.allowed
    _assert_clean(tmp_path)
