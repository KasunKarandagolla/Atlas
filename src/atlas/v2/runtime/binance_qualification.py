"""Bounded, public-only Binance commissioning diagnostic.

This path is deliberately separate from ordinary V2 admission. It composes
the existing broad public port, capture, sole writer, causal repository reads
and read-only exporter, but never runs the strategy/admission supervisor and
never creates a capacity measurement.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import shutil
import signal
import stat
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, cast

from .._serialization import canonical_json, sha256_json

QUALIFICATION_PROFILE_VERSION = "S41_BINANCE_SINGLE_VENUE_QUALIFICATION_V1"
QUALIFICATION_PURPOSE = "QUALIFICATION_ONLY"
REVIEWED_DESIGN_SHA = "dfe6bd4e21698454982f0e5e04ad93c2ef6f51ef"
REVIEWED_PROPOSAL_SHA256 = "505351b2f4b172c4d9cbd6dbac0c0792f62729ac55ebf3e6a28ff1297186a0a8"
BINANCE_REST_SOURCE_ID = "BINANCE_USDM_PUBLIC_HTTP"
MEASURED_SECONDS = 120
WARMUP_DEADLINE_SECONDS = 60
POLL_INTERVAL_NS = 10_000_000_000
REPORT_INTERVAL_NS = 60_000_000_000
MAX_SUPERVISED_SECONDS = 300
HOST_PATH_WORKER_DEADLINE_SECONDS = 35
QUEUE_ITEMS_LIMIT = 512
QUEUE_BYTES_LIMIT = 16_000_000
STREAM_SERVICE_GAP_LIMIT_NS = 1_500_000_000
RESOURCE_SAMPLE_INTERVAL_NS = 1_000_000_000
NANOSECONDS_PER_SECOND = RESOURCE_SAMPLE_INTERVAL_NS
MAX_STREAM_INSTRUMENTS = 16
MAX_DEPTH_INSTRUMENTS = 8
MAX_STREAM_TOPICS = 32
PROFILE_BINDING_FILE = "qualification-profile.json"
RECEIPT_FILE = "binance-public-diagnostic-receipt.json"


def profile_contract() -> dict[str, Any]:
    """Return the immutable diagnostic scope; this is not a capacity profile."""
    return {
        "version": QUALIFICATION_PROFILE_VERSION,
        "purpose": QUALIFICATION_PURPOSE,
        "public_venues": ["BINANCE"],
        "selected_execution_venue": "BINANCE",
        "execution_profile": "DISABLED",
        "provider_profile": "DISABLED",
        "account_scope_ref": None,
        "capability_profile_ref": None,
        "credential_ref": None,
        "owner_pair_configuration_hash": None,
        "measured_seconds": MEASURED_SECONDS,
        "warmup_deadline_seconds": WARMUP_DEADLINE_SECONDS,
        "broad_poll_interval_ns": POLL_INTERVAL_NS,
        "report_interval_ns": REPORT_INTERVAL_NS,
        "capture_batch_frames": 16,
        "handoff_max_items": QUEUE_ITEMS_LIMIT,
        "handoff_max_bytes": QUEUE_BYTES_LIMIT,
        "maximum_stream_service_gap_ns": STREAM_SERVICE_GAP_LIMIT_NS,
        "maximum_stream_instruments": MAX_STREAM_INSTRUMENTS,
        "maximum_depth_instruments": MAX_DEPTH_INSTRUMENTS,
        "maximum_stream_topics": MAX_STREAM_TOPICS,
        "raw_first": True,
        "sqlite_wal_mode": "WAL",
        "sqlite_synchronous": "FULL",
        "coordinating_acceptance": "DIAGNOSTIC IMPLEMENTATION ONLY",
        "reviewed_design_sha": REVIEWED_DESIGN_SHA,
        "reviewed_proposal_sha256": REVIEWED_PROPOSAL_SHA256,
        "capacity_certificate_issued": False,
        "production_admission": False,
        "capital_enabled": False,
        "assisted_enabled": False,
        "economics": "NOT ESTIMABLE",
    }


def profile_contract_hash() -> str:
    return sha256_json(profile_contract())


def qualification_config(data_root: Path) -> Any:
    """Construct the one permitted V2 run configuration for this diagnostic."""
    from ..product import ResearchRunConfigV2

    root = data_root.expanduser().resolve()
    return ResearchRunConfigV2(
        data_root=str(root), public_venues=("BINANCE",), selected_execution_venue="BINANCE",
        execution_environment="DEMO", product_scope="LINEAR_PERPETUAL", scope="BOUNDED_UNIVERSE_V2",
        resource_profile_id="bounded-v2", account_scope_ref=None, capability_profile_ref=None,
        credential_ref=None, execution_profile="DISABLED", execution_instrument_symbol=None,
        owner_pair_configuration_hash=None, official_fomc_source_profile_json=None,
        report_interval_seconds=60, provider_profile="DISABLED",
    )


def validate_qualification_config(config: Any) -> None:
    from ..product import ResearchRunConfigV2

    if not isinstance(config, ResearchRunConfigV2):
        raise ValueError("QUALIFICATION_CONFIG_TYPE_INVALID")
    expected = qualification_config(Path(config.data_root))
    if config != expected:
        raise ValueError("QUALIFICATION_CONFIG_SCOPE_MISMATCH")


def validate_selected_path(path: Path) -> Path:
    """Reject ambiguous, network, repository-local and non-Linux targets."""
    if not sys.platform.startswith("linux"):
        raise ValueError("QUALIFICATION_REQUIRES_LINUX")
    expanded = path.expanduser()
    if not expanded.is_absolute() or str(expanded).startswith(("//", "\\\\")):
        raise ValueError("QUALIFICATION_PATH_MUST_BE_LOCAL_AND_ABSOLUTE")
    resolved = expanded.resolve()
    if str(resolved) != str(expanded) or resolved == Path(resolved.anchor):
        raise ValueError("QUALIFICATION_PATH_MUST_BE_CANONICAL_NONROOT")
    if expanded.exists() and expanded.is_symlink():
        raise ValueError("QUALIFICATION_PATH_SYMLINK_REJECTED")
    from ..resources import resource_file

    repository_root = resource_file(".").resolve()
    if resolved == repository_root or resolved.is_relative_to(repository_root):
        raise ValueError("QUALIFICATION_PATH_INSIDE_SOURCE_TREE")
    return resolved


def _sanitized_environment() -> dict[str, str]:
    unsafe_launch_environment = {
        "all_proxy", "http_proxy", "https_proxy", "no_proxy", "ld_preload", "ld_library_path",
        "dyld_insert_libraries", "dyld_library_path", "pythonhome", "pythonpath",
        "pythonstartup", "pythoninspect", "pythonuserbase", "qt_plugin_path",
        "qt_qpa_platform_plugin_path",
    }
    return {key: value for key, value in os.environ.items()
            if key.lower() not in unsafe_launch_environment
            and not any(word in key.upper() for word in ("KEY", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL"))}


def _host_path_command(selected_path: Path, identity_sha256: str) -> list[str]:
    from ..product import resource_file

    if getattr(sys, "frozen", False):
        return [sys.executable, "--component", "qualification-host-path",
                "--selected-path", str(selected_path), "--identity-sha256", identity_sha256]
    return [sys.executable, str(resource_file("src/atlas_product_entry.py")),
            "--component", "qualification-host-path", "--selected-path", str(selected_path),
            "--identity-sha256", identity_sha256]


def _validate_host_path_result(raw: bytes, *, returncode: int, selected_path: Path,
                               identity_sha256: str) -> dict[str, Any]:
    from ..runtime.storage_preflight import PREFLIGHT_CONTRACT_V1, StoragePreflightLimitsV1

    if len(raw) > 1_000_000:
        raise ValueError("QUALIFICATION_HOST_PATH_RESULT_OVERSIZED")
    result = json.loads(raw)
    expected = {"schema_version", "version", "selected_path", "identity_sha256", "started_at_ns",
        "completed_at_ns", "elapsed_ns", "status", "allowed", "reasons", "limits", "measurements",
        "facts", "probe_mode", "authority", "capital_enabled", "assisted_enabled",
        "live_source_qualification", "endurance_qualification", "content_hash"}
    if (not isinstance(result, dict) or set(result) != expected or result["schema_version"] != 1
            or result["version"] != PREFLIGHT_CONTRACT_V1
            or result["selected_path"] != str(selected_path)
            or result["identity_sha256"] != identity_sha256 or result["probe_mode"] != "HOST_PATH"
            or result["limits"] != StoragePreflightLimitsV1().as_dict()
            or result["authority"] != "ZERO" or result["capital_enabled"] is not False
            or result["assisted_enabled"] is not False
            or result["live_source_qualification"] != "TEST GATE"
            or result["endurance_qualification"] != "TEST GATE"
            or type(result["allowed"]) is not bool
            or result["allowed"] != (result["status"] == "TESTED" and not result["reasons"])
            or any(type(result[key]) is not int or result[key] < 0
                   for key in ("started_at_ns", "completed_at_ns", "elapsed_ns"))
            or result["completed_at_ns"] < result["started_at_ns"]
            or sha256_json({key: value for key, value in result.items() if key != "content_hash"})
            != result["content_hash"]
            or returncode != (0 if result["allowed"] else 2)):
        raise ValueError("QUALIFICATION_HOST_PATH_RESULT_BINDING_FAILED")
    return result


def run_host_path_probe(selected_path: Path, *, identity_sha256: str) -> dict[str, Any]:
    completed = subprocess.run(
        _host_path_command(selected_path, identity_sha256), cwd=Path.cwd(),
        env=_sanitized_environment(), stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        timeout=HOST_PATH_WORKER_DEADLINE_SECONDS, check=False,
    )
    return _validate_host_path_result(completed.stdout, returncode=completed.returncode,
        selected_path=selected_path, identity_sha256=identity_sha256)


def _write_immutable(path: Path, body: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (canonical_json(body) + "\n").encode("utf-8")
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _write_receipt(run: Path, body: dict[str, Any]) -> dict[str, Any]:
    if body.get("status") == "DIAGNOSTIC_COMPLETE":
        measurements = body.get("diagnostic_measurements")
        if (not isinstance(measurements, Mapping) or measurements.get("complete") is not True
                or measurements.get("missing_measurements") != []
                or any(not isinstance(measurements.get(name), Mapping)
                       or measurements[name].get("complete") is not True
                       for name in ("source_traffic", "durable_capture_traffic", "indexed_traffic"))
                or not isinstance(measurements.get("trade_geometry"), Mapping)
                or measurements["trade_geometry"].get("complete") is not True
                or measurements["trade_geometry"].get("measurement") != "COMPLETE"
                or type(measurements["trade_geometry"].get("indexed_frames")) is not int
                or measurements["trade_geometry"]["indexed_frames"] <= 0
                or not isinstance(measurements.get("payload_geometry"), Mapping)
                or measurements["payload_geometry"].get("measurement") != "COMPLETE"
                or not isinstance(measurements.get("resource_sampling"), Mapping)
                or measurements["resource_sampling"].get("measurement_window_complete") is not True
                or body.get("measurement_window_complete") is not True
                or body.get("measured_window_seconds") != MEASURED_SECONDS):
            raise ValueError("QUALIFICATION_COMPLETE_RECEIPT_TELEMETRY_INCOMPLETE")
    receipt_body = {**body, "version": "BINANCE_PUBLIC_DIAGNOSTIC_RECEIPT_V1",
        "authority": "ZERO", "capacity_certificate_issued": False,
        "production_admission": False, "capital_enabled": False,
        "assisted_enabled": False, "economics": "NOT ESTIMABLE"}
    receipt = {**receipt_body, "content_hash": sha256_json(receipt_body)}
    target = run / RECEIPT_FILE
    if target.exists():
        existing = json.loads(target.read_text(encoding="utf-8"))
        if existing != receipt:
            raise ValueError("QUALIFICATION_RECEIPT_IMMUTABLE_CONFLICT")
        return existing
    _write_immutable(target, receipt)
    return receipt


def _profile_binding(run: Path, *, selected_path: Path, host_probe: Mapping[str, Any],
                     manifest: Mapping[str, Any], supervisor_token: str) -> dict[str, Any]:
    from ..product import build_identity

    try:
        device = os.stat(selected_path).st_dev
        run_device = os.stat(run).st_dev
    except OSError as exc:
        raise ValueError("QUALIFICATION_DEVICE_IDENTITY_UNAVAILABLE") from exc
    if device != run_device:
        raise ValueError("QUALIFICATION_RUN_ON_DIFFERENT_DEVICE")
    build = build_identity()
    body = {
        "version": "BINANCE_QUALIFICATION_PROFILE_BINDING_V1",
        "purpose": QUALIFICATION_PURPOSE,
        "profile_version": QUALIFICATION_PROFILE_VERSION,
        "profile_hash": profile_contract_hash(),
        "coordinating_acceptance": "DIAGNOSTIC IMPLEMENTATION ONLY",
        "reviewed_design_sha": REVIEWED_DESIGN_SHA,
        "reviewed_proposal_sha256": REVIEWED_PROPOSAL_SHA256,
        "run_id": manifest["run_id"], "run_manifest_hash": manifest["content_hash"],
        "configuration_hash": manifest["config_hash"], "source_sha": manifest["source_sha"],
        "build_identity_hash": sha256_json(build),
        "selected_path": str(selected_path),
        "selected_path_sha256": hashlib.sha256(str(selected_path).encode()).hexdigest(),
        "device_id_sha256": hashlib.sha256(str(device).encode()).hexdigest(),
        "supervisor_token_sha256": hashlib.sha256(supervisor_token.encode("ascii")).hexdigest(),
        "host_path_probe": dict(host_probe), "host_path_probe_ref": host_probe["content_hash"],
        "profile": profile_contract(),
    }
    return {**body, "content_hash": sha256_json(body)}


def _validate_profile_binding(run: Path, manifest: Mapping[str, Any]) -> dict[str, Any]:
    marker = run / PROFILE_BINDING_FILE
    if marker.is_symlink() or not marker.is_file() or marker.stat().st_size > 1_000_000:
        raise ValueError("QUALIFICATION_PROFILE_BINDING_MISSING_OR_OVERSIZED")
    body = json.loads(marker.read_text(encoding="utf-8"))
    if not isinstance(body, dict) or body.get("content_hash") != sha256_json(
            {key: value for key, value in body.items() if key != "content_hash"}):
        raise ValueError("QUALIFICATION_PROFILE_BINDING_HASH_FAILED")
    config = manifest["configuration"]
    from ..product import _run_config

    validate_qualification_config(_run_config(config))
    root = Path(body.get("selected_path", ""))
    host_probe = body.get("host_path_probe")
    if not isinstance(host_probe, Mapping):
        raise ValueError("QUALIFICATION_HOST_PATH_PROBE_MISSING")
    selected_path_sha256 = hashlib.sha256(str(root).encode()).hexdigest()
    expected_host_identity = sha256_json({
        "version": "BINANCE_QUALIFICATION_HOST_PATH_IDENTITY_V1",
        "source_sha": manifest["source_sha"],
        "profile_hash": profile_contract_hash(),
        "selected_path_sha256": selected_path_sha256,
    })
    try:
        device_id_sha256 = hashlib.sha256(str(os.stat(root).st_dev).encode()).hexdigest()
    except OSError as exc:
        raise ValueError("QUALIFICATION_DEVICE_IDENTITY_UNAVAILABLE") from exc
    host_probe = _validate_host_path_result(
        (canonical_json(dict(host_probe)) + "\n").encode("utf-8"),
        returncode=0,
        selected_path=root,
        identity_sha256=expected_host_identity,
    )
    if (body.get("version") != "BINANCE_QUALIFICATION_PROFILE_BINDING_V1"
            or body.get("purpose") != QUALIFICATION_PURPOSE
            or body.get("profile_version") != QUALIFICATION_PROFILE_VERSION
            or body.get("profile_hash") != profile_contract_hash()
            or body.get("profile") != profile_contract()
            or body.get("coordinating_acceptance") != "DIAGNOSTIC IMPLEMENTATION ONLY"
            or body.get("reviewed_design_sha") != REVIEWED_DESIGN_SHA
            or body.get("reviewed_proposal_sha256") != REVIEWED_PROPOSAL_SHA256
            or body.get("run_id") != manifest["run_id"]
            or body.get("run_manifest_hash") != manifest["content_hash"]
            or body.get("configuration_hash") != manifest["config_hash"]
            or body.get("source_sha") != manifest["source_sha"]
            or body.get("build_identity_hash") != manifest["build_identity_hash"]
            or body.get("selected_path_sha256") != selected_path_sha256
            or body.get("device_id_sha256") != device_id_sha256
            or not isinstance(body.get("supervisor_token_sha256"), str)
            or len(body["supervisor_token_sha256"]) != 64
            or any(char not in "0123456789abcdef" for char in body["supervisor_token_sha256"])
            or body.get("host_path_probe_ref") != host_probe.get("content_hash")
            or host_probe.get("identity_sha256") != expected_host_identity
            or host_probe.get("allowed") is not True
            or host_probe.get("probe_mode") != "HOST_PATH"
            or host_probe.get("selected_path") != str(root)
            or Path(config["data_root"]) != run.parent.parent
            or not run.resolve().is_relative_to(root)):
        raise ValueError("QUALIFICATION_PROFILE_BINDING_SCOPE_MISMATCH")
    return body


def _assert_fresh_run(run: Path) -> None:
    allowed_files = {"run.json", PROFILE_BINDING_FILE}
    allowed_directories = {"reports", "epochs", "launches"}
    if not (run / "run.json").is_file() or (run / "ops.sqlite").exists():
        raise ValueError("QUALIFICATION_RUN_NOT_FRESH_CANNOT_RESUME")
    for child in run.iterdir():
        if child.is_symlink():
            raise ValueError("QUALIFICATION_RUN_SYMLINK_REJECTED")
        if child.is_file() and child.name not in allowed_files:
            raise ValueError("QUALIFICATION_RUN_CONTAINS_UNEXPECTED_EVIDENCE")
        if child.is_dir():
            if child.name not in allowed_directories or any(child.iterdir()):
                raise ValueError("QUALIFICATION_RUN_CONTAINS_PRIOR_STATE")
        elif not child.is_file():
            raise ValueError("QUALIFICATION_RUN_CONTAINS_UNSUPPORTED_ENTRY")


def _assert_owner_only_run(run: Path) -> None:
    for path in (run, *run.iterdir()):
        item = path.lstat()
        if (stat.S_ISLNK(item.st_mode) or item.st_uid != os.geteuid()
                or stat.S_IMODE(item.st_mode) & 0o077):
            raise ValueError("QUALIFICATION_RUN_NOT_OWNER_ONLY")


def _assert_empty_repository(repository: Any) -> None:
    with repository._lock:
        artifact_count = repository._connection.execute("SELECT COUNT(*) FROM artifact_index").fetchone()[0]
        due_count = repository._connection.execute("SELECT COUNT(*) FROM due_work").fetchone()[0]
    if artifact_count or due_count or repository.list_active_watches(limit=1):
        raise ValueError("QUALIFICATION_RUN_HAS_PRIOR_ARTIFACT_WATCH_OR_DUE_WORK")


def _validate_port_scope(port: Any) -> None:
    from ..instruments import VenueV2

    source = port.public_source
    if (tuple(source.enabled_venues) != (VenueV2.BINANCE,)
            or tuple(source.required_source_ids) != (BINANCE_REST_SOURCE_ID,)
            or source.bybit_reader is not None):
        raise ValueError("QUALIFICATION_BYBIT_SOURCE_SCOPE_VIOLATION")


def _validate_capture_metrics_enabled(port: Any) -> None:
    capture = getattr(port.public_stream_source, "capture", None)
    durable_capture = getattr(capture, "_capture", None)
    if getattr(durable_capture, "_capture_payload_metrics", False) is not True:
        raise ValueError("QUALIFICATION_CAPTURE_PAYLOAD_METRICS_DISABLED")


def _causal_checkpoint_read(repository: Any, *, run_id: str, port: Any,
                            cutoff_ns: int, require_complete: bool = False) -> tuple[str, ...]:
    from .._serialization import sha256_json

    runtime = port.public_stream_source
    plan = runtime.plan
    products = {product.key: product for product in port.public_source.current_products}
    if plan is None:
        return ()
    feed_refs: list[str] = []
    for identity in plan.identities:
        if identity.venue.value != "BINANCE":
            raise ValueError("QUALIFICATION_NON_BINANCE_STREAM_IDENTITY")
        product = products.get(identity.key)
        if product is None:
            raise ValueError("QUALIFICATION_STREAM_PRODUCT_REVISION_MISSING")
        source_id = ("BINANCE_DEPTH_PUBLIC_WS_BROAD_V2" if "@depth" in identity.channel
                     else "BINANCE_MARKET_PUBLIC_WS_BROAD_V2")
        feed_refs.append(sha256_json({"instrument": identity.key.to_dict(), "source_id": source_id,
            "channel": identity.channel, "metadata_ref": product.metadata_ref}))
    entries = repository.latest_public_stream_operational_checkpoints_v1(
        run_id=run_id, feed_refs=tuple(feed_refs), as_of_ns=cutoff_ns)
    if set(entries) != set(feed_refs):
        if require_complete:
            raise ValueError("QUALIFICATION_STREAM_CHECKPOINT_INCOMPLETE")
        return ()
    visible = []
    for entry in entries.values():
        effective = repository.effective_available_at_ns(entry.artifact_ref)
        if effective is None or effective > cutoff_ns:
            raise ValueError("QUALIFICATION_UNOBSERVED_OR_FUTURE_CHECKPOINT_VISIBLE")
        visible.append(entry.artifact_ref)
    return tuple(sorted(visible))


def _stream_plan_manifest(port: Any) -> dict[str, Any]:
    plan = port.public_stream_source.plan
    if plan is None:
        return {"plan_id": None, "identities": [], "lane_topics": {}}
    identities = []
    for identity in plan.identities:
        if identity.venue.value != "BINANCE":
            raise ValueError("QUALIFICATION_NON_BINANCE_STREAM_IDENTITY")
        identities.append({"venue": identity.venue.value, "channel": identity.channel,
            "instrument": identity.key.to_dict()})
    topics = {name: list(values) for name, values in sorted(plan.lane_topics.items())}
    if any(not name.startswith("BINANCE") for name in topics):
        raise ValueError("QUALIFICATION_NON_BINANCE_STREAM_TOPIC")
    result = {"plan_id": plan.plan_id, "source_refs": list(plan.source_refs),
        "instruments": [key.to_dict() for key in plan.keys], "identities": identities,
        "lane_topics": topics, "depth_subscription_count": sum("@depth" in item["channel"]
            for item in identities), "topic_count": sum(len(items) for items in topics.values())}
    if (len(plan.keys) > MAX_STREAM_INSTRUMENTS
            or result["depth_subscription_count"] > MAX_DEPTH_INSTRUMENTS
            or result["topic_count"] > MAX_STREAM_TOPICS):
        raise ValueError("QUALIFICATION_STREAM_PLAN_EXCEEDS_REVIEWED_PROFILE_CAP")
    return result


def _source_selection_manifest(repository: Any, port: Any, *, cutoff_ns: int) -> dict[str, Any]:
    from ..instruments import VenueV2
    from ..runtime.broad_universe import latest_workset

    products = tuple(port.public_source.current_products)
    if any(product.key.venue != VenueV2.BINANCE for product in products):
        raise ValueError("QUALIFICATION_NON_BINANCE_PRODUCT_IN_SELECTION")
    workset = latest_workset(repository, cutoff_ns=cutoff_ns)
    if workset is None:
        raise ValueError("QUALIFICATION_CAUSAL_WORKSET_UNAVAILABLE")
    tiers: dict[str, int] = {}
    for value in workset["tiers"].values():
        key = str(value)
        tiers[key] = tiers.get(key, 0) + 1
    watches = repository.list_active_watches(limit=513)
    if len(watches) > 512:
        raise ValueError("QUALIFICATION_ACTIVE_WATCH_POPULATION_OVERFLOW")
    watch_manifest = [{"watch_id": item.watch_id, "key": item.key.to_dict(),
        "state_version": item.state_version} for item in watches]
    return {
        "workset_ref": sha256_json(dict(workset)),
        "workset_source_ref": workset["source_ref"],
        "universe_ref": workset["universe_ref"],
        "product_count": len(workset["product_refs"]),
        "product_refs_sha256": sha256_json(list(workset["product_refs"])),
        "tier_counts": dict(sorted(tiers.items())),
        "research_cohort_venue": workset["research_cohort_venue"],
        "research_cohort_keys": list(workset["research_cohort_keys"]),
        "active_product_refs": list(workset["active_product_refs"]),
        "selected_count": workset["selected_count"],
        "unselected_observed_count": workset["unselected_observed_count"],
        "active_watches": watch_manifest,
        "stream_plan": _stream_plan_manifest(port),
        "cutoff_ns": cutoff_ns,
    }


def _read_psi() -> dict[str, Any]:
    result: dict[str, Any] = {}
    root = Path("/proc/pressure")
    for resource_name in ("cpu", "memory", "io"):
        path = root / resource_name
        try:
            lines = path.read_text(encoding="ascii").splitlines()
            parsed: dict[str, dict[str, float | int]] = {}
            for line in lines:
                pieces = line.split()
                values: dict[str, float | int] = {}
                for piece in pieces[1:]:
                    key, value = piece.split("=", 1)
                    values[key] = int(value) if key == "total" else float(value)
                parsed[pieces[0]] = values
            result[resource_name] = parsed
        except (OSError, ValueError, IndexError):
            result[resource_name] = None
    return result


def _rss_bytes() -> int | None:
    try:
        fields = Path("/proc/self/statm").read_text(encoding="ascii").split()
        return int(fields[1]) * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError):
        return None


def _proc_io_bytes(path: Path) -> dict[str, int] | None:
    try:
        fields = {}
        for line in path.read_text(encoding="ascii").splitlines():
            name, raw = line.split(":", 1)
            fields[name] = int(raw.strip())
        return {name: fields[name] for name in ("read_bytes", "write_bytes")}
    except (OSError, ValueError, KeyError):
        return None


def _host_cpu_counters() -> dict[str, int] | None:
    try:
        line = next(item for item in Path("/proc/stat").read_text(encoding="ascii").splitlines()
                    if item.startswith("cpu "))
        values = [int(value) for value in line.split()[1:9]]
        if len(values) < 4:
            return None
        return {"total_ticks": sum(values), "idle_ticks": values[3] + (values[4] if len(values) > 4 else 0)}
    except (OSError, ValueError, StopIteration):
        return None


def _host_resource_facts() -> dict[str, Any]:
    meminfo: dict[str, int] = {}
    try:
        for line in Path("/proc/meminfo").read_text(encoding="ascii").splitlines():
            name, raw, *_ = line.replace(":", "").split()
            meminfo[name] = int(raw) * 1024
    except (OSError, ValueError):
        meminfo = {}
    try:
        affinity_count: int | None = len(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        affinity_count = os.cpu_count()
    cgroup_root = Path("/sys/fs/cgroup")
    try:
        for line in Path("/proc/self/cgroup").read_text(encoding="ascii").splitlines():
            hierarchy, controllers, relative = line.split(":", 2)
            if hierarchy == "0" and not controllers:
                candidate = (cgroup_root / relative.lstrip("/")).resolve()
                if candidate.is_relative_to(cgroup_root.resolve()):
                    cgroup_root = candidate
                break
    except (OSError, ValueError):
        pass
    cpu_quota: float | None = None
    try:
        quota, period = (cgroup_root / "cpu.max").read_text(encoding="ascii").split()
        if quota != "max" and int(period) > 0:
            cpu_quota = int(quota) / int(period)
    except (OSError, ValueError):
        pass
    cgroup_memory: dict[str, int | None] = {}
    for key, filename in (("current_bytes", "memory.current"), ("maximum_bytes", "memory.max")):
        try:
            raw = (cgroup_root / filename).read_text(encoding="ascii").strip()
            cgroup_memory[key] = None if raw == "max" else int(raw)
        except (OSError, ValueError):
            cgroup_memory[key] = None
    return {
        "logical_cpu_count": os.cpu_count(),
        "process_affinity_cpu_count": affinity_count,
        "cgroup_cpu_quota": cpu_quota,
        "host_memory_total_bytes": meminfo.get("MemTotal"),
        "host_memory_available_bytes": meminfo.get("MemAvailable"),
        "host_swap_total_bytes": meminfo.get("SwapTotal"),
        "host_swap_free_bytes": meminfo.get("SwapFree"),
        "cgroup_memory": cgroup_memory,
    }


def _psi_sample_complete(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return False
    for resource_name, row_names in (("cpu", ("some",)),
                                    ("memory", ("some", "full")),
                                    ("io", ("some", "full"))):
        rows = value.get(resource_name)
        if not isinstance(rows, Mapping):
            return False
        for row_name in row_names:
            row = rows.get(row_name)
            if (not isinstance(row, Mapping)
                    or any(type(row.get(key)) not in (int, float)
                           for key in ("avg10", "avg60", "avg300", "total"))):
                return False
    return True


def _resource_telemetry(run: Path, *, port: Any) -> dict[str, Any]:
    import resource

    db = run / "ops.sqlite"
    wal = run / "ops.sqlite-wal"
    stream_runtime = port.public_stream_source
    worker_sample = stream_runtime.qualification_worker_resource_sample()
    return {
        "sampled_at_monotonic_ns": time.monotonic_ns(),
        "process_cpu_seconds": time.process_time(),
        "process_rss_bytes": _rss_bytes(),
        "process_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        "process_io_bytes": _proc_io_bytes(Path("/proc/self/io")),
        "worker": worker_sample,
        "host_cpu_counters": _host_cpu_counters(),
        "host_resources": _host_resource_facts(),
        "system_psi": _read_psi(),
        "free_disk_bytes": shutil.disk_usage(run).free,
        "sqlite_bytes": db.stat().st_size if db.exists() else 0,
        "wal_bytes": wal.stat().st_size if wal.exists() else 0,
    }


def _resource_summary(samples: Sequence[Mapping[str, Any]], *, window_complete: bool,
                      window_start_ns: int | None = None,
                      window_end_ns: int | None = None) -> dict[str, Any]:
    all_resources = [item["resource"] for item in samples if isinstance(item.get("resource"), Mapping)]
    resources = [item for item in all_resources if type(item.get("sampled_at_monotonic_ns")) is int
                 and (window_start_ns is None or item["sampled_at_monotonic_ns"] >= window_start_ns)
                 and (window_end_ns is None or item["sampled_at_monotonic_ns"] < window_end_ns)]
    endpoint = next((item for item in reversed(all_resources)
                     if window_end_ns is not None
                     and type(item.get("sampled_at_monotonic_ns")) is int
                     and item["sampled_at_monotonic_ns"] >= window_end_ns), None)
    if not resources:
        return {"sample_count": 0, "measurement_window_complete": False,
                "missing_second_buckets": list(range(MEASURED_SECONDS)),
                "max_sample_gap_ns": None, "boundary_sample_present": endpoint is not None}
    resources.sort(key=lambda item: item["sampled_at_monotonic_ns"])
    first = resources[0]
    last = endpoint or resources[-1]
    duration_ns = ((window_end_ns - window_start_ns) if window_start_ns is not None
                   and window_end_ns is not None else None)
    expected_buckets = ((duration_ns + RESOURCE_SAMPLE_INTERVAL_NS - 1) // RESOURCE_SAMPLE_INTERVAL_NS
                        if duration_ns is not None else 0)
    bucket_counts: dict[int, int] = {}
    if window_start_ns is not None and window_end_ns is not None:
        for item in resources:
            bucket = (item["sampled_at_monotonic_ns"] - window_start_ns) // RESOURCE_SAMPLE_INTERVAL_NS
            bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
    missing_buckets = [index for index in range(expected_buckets) if bucket_counts.get(index, 0) == 0]
    gap_points = [item["sampled_at_monotonic_ns"] for item in resources]
    if window_start_ns is not None:
        gap_points.insert(0, window_start_ns)
    if endpoint is not None:
        gap_points.append(endpoint["sampled_at_monotonic_ns"])
    elif window_end_ns is not None:
        gap_points.append(window_end_ns)
    storage_samples = (*resources, *((endpoint,) if endpoint is not None else ()))
    max_gap_ns = max((right - left for left, right in zip(gap_points, gap_points[1:], strict=False)),
                     default=None)
    missing_sensor_fields: list[dict[str, Any]] = []
    required_samples = (*resources, *((endpoint,) if endpoint is not None else ()))
    for index, item in enumerate(required_samples):
        absent = []
        if type(item.get("process_cpu_seconds")) not in (int, float):
            absent.append("process_cpu_seconds")
        if type(item.get("process_rss_bytes")) is not int:
            absent.append("process_rss_bytes")
        process_io = item.get("process_io_bytes")
        if not isinstance(process_io, Mapping) or any(type(process_io.get(name)) is not int
                for name in ("read_bytes", "write_bytes")):
            absent.append("process_io_bytes")
        worker = item.get("worker")
        if (not isinstance(worker, Mapping) or any(type(worker.get(name)) is not int
                for name in ("cpu_ticks", "cpu_ticks_per_second", "rss_bytes", "read_bytes", "write_bytes"))
                or not isinstance(worker.get("worker_instance_ref"), str)):
            absent.append("worker_process_counters")
        cpu = item.get("host_cpu_counters")
        if not isinstance(cpu, Mapping) or any(type(cpu.get(name)) is not int
                for name in ("total_ticks", "idle_ticks")):
            absent.append("host_cpu_counters")
        host_facts = item.get("host_resources")
        if not isinstance(host_facts, Mapping) or type(host_facts.get("host_memory_available_bytes")) is not int:
            absent.append("host_memory_available_bytes")
        psi = item.get("system_psi")
        if not _psi_sample_complete(psi):
            absent.append("system_psi")
        if any(type(item.get(name)) is not int for name in ("free_disk_bytes", "sqlite_bytes", "wal_bytes")):
            absent.append("filesystem_sqlite_wal_counters")
        if absent:
            missing_sensor_fields.append({"sample_index": index, "missing": absent})
    worker_instance_refs = {item["worker"].get("worker_instance_ref") for item in required_samples
        if isinstance(item.get("worker"), Mapping)}
    worker_instance_stable = (len(worker_instance_refs) == 1 and len(required_samples) > 0
        and all(isinstance(item.get("worker"), Mapping) for item in required_samples))
    if not worker_instance_stable:
        missing_sensor_fields.append({"missing": ["worker_instance_changed_or_unavailable"]})
    rss_values: list[int] = []
    for item in resources:
        value = item.get("process_rss_bytes")
        if isinstance(value, int):
            rss_values.append(value)
    host_memory_available: list[int] = []
    cgroup_memory_current: list[int] = []
    for item in resources:
        host = item.get("host_resources")
        if not isinstance(host, Mapping):
            continue
        available = host.get("host_memory_available_bytes")
        if isinstance(available, int):
            host_memory_available.append(available)
        cgroup = host.get("cgroup_memory")
        if isinstance(cgroup, Mapping):
            current = cgroup.get("current_bytes")
            if isinstance(current, int):
                cgroup_memory_current.append(current)
    span_seconds = max(0.001, (last["sampled_at_monotonic_ns"]
        - first["sampled_at_monotonic_ns"]) / 1_000_000_000)
    cpu_capacity: float = 1.0
    host = last.get("host_resources")
    if isinstance(host, Mapping):
        configured_capacity = host.get("cgroup_cpu_quota") or host.get("process_affinity_cpu_count")
        if isinstance(configured_capacity, (int, float)) and configured_capacity > 0:
            cpu_capacity = float(configured_capacity)
    cpu_seconds = (max(0.0, last["process_cpu_seconds"] - first["process_cpu_seconds"])
                   if type(last.get("process_cpu_seconds")) in (int, float)
                   and type(first.get("process_cpu_seconds")) in (int, float) else None)
    worker_first, worker_last = first.get("worker"), last.get("worker")
    worker_cpu_seconds = None
    worker_write_bytes = None
    if isinstance(worker_first, Mapping) and isinstance(worker_last, Mapping):
        tick_hz = worker_first.get("cpu_ticks_per_second")
        if (type(tick_hz) is int and tick_hz > 0 and worker_last.get("cpu_ticks_per_second") == tick_hz
                and type(worker_first.get("cpu_ticks")) is int and type(worker_last.get("cpu_ticks")) is int):
            worker_cpu_seconds = max(0, worker_last["cpu_ticks"] - worker_first["cpu_ticks"]) / tick_hz
        if type(worker_first.get("write_bytes")) is int and type(worker_last.get("write_bytes")) is int:
            worker_write_bytes = max(0, worker_last["write_bytes"] - worker_first["write_bytes"])
    process_io_first, process_io_last = first.get("process_io_bytes"), last.get("process_io_bytes")
    process_write_bytes = None
    if (isinstance(process_io_first, Mapping) and isinstance(process_io_last, Mapping)
            and type(process_io_first.get("write_bytes")) is int
            and type(process_io_last.get("write_bytes")) is int):
        process_write_bytes = max(0, process_io_last["write_bytes"] - process_io_first["write_bytes"])
    host_cpu_first, host_cpu_last = first.get("host_cpu_counters"), last.get("host_cpu_counters")
    host_cpu_busy_percent = None
    if isinstance(host_cpu_first, Mapping) and isinstance(host_cpu_last, Mapping):
        total_delta = host_cpu_last.get("total_ticks", 0) - host_cpu_first.get("total_ticks", 0)
        idle_delta = host_cpu_last.get("idle_ticks", 0) - host_cpu_first.get("idle_ticks", 0)
        if type(total_delta) is int and total_delta > 0 and type(idle_delta) is int:
            host_cpu_busy_percent = max(0.0, min(100.0, (total_delta - idle_delta) / total_delta * 100.0))
    return {
        "sample_count": len(resources),
        "boundary_sample_present": endpoint is not None,
        "boundary_sample_at_monotonic_ns": endpoint.get("sampled_at_monotonic_ns") if endpoint else None,
        "measurement_window_complete": (window_complete and not missing_buckets and not missing_sensor_fields
            and max_gap_ns is not None and max_gap_ns <= 1_500_000_000 and endpoint is not None),
        "expected_second_buckets": expected_buckets,
        "observed_second_buckets": len(bucket_counts),
        "missing_second_buckets": missing_buckets,
        "sample_bucket_counts": {str(key): value for key, value in sorted(bucket_counts.items())},
        "sample_timestamps_monotonic_ns": [item["sampled_at_monotonic_ns"] for item in required_samples],
        "sample_gaps_ns": [right - left for left, right in zip(gap_points, gap_points[1:], strict=False)],
        "max_sample_gap_ns": max_gap_ns,
        "missing_sensor_fields": missing_sensor_fields,
        "sample_span_ns": max(0, last["sampled_at_monotonic_ns"]
            - first["sampled_at_monotonic_ns"]),
        "process_cpu_seconds_delta": cpu_seconds,
        "process_cpu_percent_of_allocated_capacity": (cpu_seconds / span_seconds / cpu_capacity * 100.0
            if cpu_seconds is not None else None),
        "worker_cpu_seconds_delta": worker_cpu_seconds,
        "worker_cpu_percent_of_allocated_capacity": (worker_cpu_seconds / span_seconds
            / cpu_capacity * 100.0 if worker_cpu_seconds is not None else None),
        "host_cpu_busy_percent": host_cpu_busy_percent,
        "process_rss_max_bytes": max(rss_values) if rss_values else None,
        "process_peak_rss_max_bytes": max((item["process_peak_rss_bytes"] for item in resources
            if type(item.get("process_peak_rss_bytes")) is int), default=None),
        "worker_rss_max_bytes": max((item["worker"].get("rss_bytes") for item in resources
            if isinstance(item.get("worker"), Mapping)
            and type(item["worker"].get("rss_bytes")) is int), default=None),
        "worker_instance_stable": worker_instance_stable,
        "process_write_bytes_delta": process_write_bytes,
        "worker_write_bytes_delta": worker_write_bytes,
        "host_memory_available_min_bytes": min(host_memory_available) if host_memory_available else None,
        "cgroup_memory_current_max_bytes": max(cgroup_memory_current) if cgroup_memory_current else None,
        "free_disk_start_bytes": first["free_disk_bytes"],
        "free_disk_end_bytes": last["free_disk_bytes"],
        "free_disk_min_bytes": min(item["free_disk_bytes"] for item in resources),
        "sqlite_start_bytes": first["sqlite_bytes"], "sqlite_end_bytes": last["sqlite_bytes"],
        "sqlite_growth_bytes": last["sqlite_bytes"] - first["sqlite_bytes"],
        "sqlite_max_bytes": max(item["sqlite_bytes"] for item in storage_samples),
        "wal_start_bytes": first["wal_bytes"], "wal_end_bytes": last["wal_bytes"],
        "wal_growth_bytes": last["wal_bytes"] - first["wal_bytes"],
        "wal_max_bytes": max(item["wal_bytes"] for item in storage_samples),
        "system_psi_samples": [item["system_psi"] for item in resources],
        "resource_observation_scope": {
            "process": "ATLAS diagnostic process CPU/RSS/process I/O",
            "worker": "existing public evidence preparation child CPU/RSS/process I/O",
            "host": "host aggregate CPU and memory counters; PSI is host-wide",
            "filesystem": "selected data filesystem free bytes plus ops.sqlite/WAL growth",
        },
    }


def _final_backlog_summary(samples: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    points: list[tuple[float, int]] = []
    for item in samples:
        offset = item.get("window_offset_ns")
        capture = item.get("capture")
        if type(offset) is int and offset >= 90_000_000_000 and isinstance(capture, Mapping):
            pending = item.get("queue_items", 0) + capture.get("pending_frames", 0)
            if type(pending) is int:
                points.append((offset / 1_000_000_000, pending))
    if len(points) < 2:
        return {"sample_count": len(points), "slope_frames_per_second": None,
            "measurement_complete": False}
    mean_time = sum(item[0] for item in points) / len(points)
    mean_pending = sum(item[1] for item in points) / len(points)
    time_variance = sum((item[0] - mean_time) ** 2 for item in points)
    slope = (sum((at - mean_time) * (pending - mean_pending) for at, pending in points)
        / time_variance if time_variance > 0 else None)
    return {
        "sample_count": len(points),
        "first_pending_frames": points[0][1],
        "last_pending_frames": points[-1][1],
        "maximum_pending_frames": max(item[1] for item in points),
        "slope_frames_per_second": slope,
        "measurement_complete": slope is not None,
    }


def _runtime_telemetry(port: Any, *, run: Path) -> dict[str, Any]:
    status = port.public_stream_source.status()
    handoff = status.handoff
    capture = dict(status.capture)
    lanes = {}
    for name, lane in status.lanes.items():
        if not name.startswith("BINANCE") or lane.handoff.venue.value != "BINANCE":
            raise ValueError("QUALIFICATION_NON_BINANCE_LANE_ACTIVE")
        lanes[name] = {"state": lane.state, "connected": lane.handoff.connected,
            "topics": list(lane.handoff.topics),
            "queue_items": lane.handoff.queue_items, "queue_bytes": lane.handoff.queue_bytes,
            "high_water_items": lane.handoff.high_water_items,
            "high_water_bytes": lane.handoff.high_water_bytes,
            "frames_received": lane.handoff.frames_received,
            "frames_rejected": lane.handoff.frames_rejected,
            "disconnect_count": lane.handoff.disconnect_count}
    progress = port.public_stream_source.progress_snapshot()
    return {
        "sampled_at_ns": time.time_ns(), "sampled_at_monotonic_ns": time.monotonic_ns(),
        "free_disk_bytes": shutil.disk_usage(run).free,
        "stream_state": status.state,
        "source_ids": list(port.public_source.required_source_ids),
        "enabled_venues": [venue.value for venue in port.public_source.enabled_venues],
        "bybit_reader_active": port.public_source.bybit_reader is not None,
        "frames_received": handoff.frames_received, "frames_drained": handoff.frames_drained,
        "frames_rejected": handoff.frames_rejected, "queue_items": handoff.queue_items,
        "queue_bytes": handoff.queue_bytes, "queue_max_items": handoff.max_queue_items,
        "queue_max_bytes": handoff.max_queue_bytes, "queue_high_water_items": handoff.high_water_items,
        "queue_high_water_bytes": handoff.high_water_bytes, "queue_loss_latched": handoff.loss_latched,
        "capture": capture, "captured_frames": int(capture.get("captured_frames", 0)),
        "indexed_frames": status.indexed_frames,
        "indexed_frames_by_lane": dict(status.indexed_frames_by_lane),
        "phase_timings_ns": status.phase_timings_ns,
        "captured_payload_bytes": int(capture.get("payload_bytes_written", 0)),
        "max_service_gap_ns": int(status.max_service_gap_ns),
        "source_health": progress["stream_source_states"],
        "stream_unresolved_gap": progress["stream_unresolved_gap"],
        "stream_ingestion_failed": progress["stream_ingestion_failed"],
        "lanes": lanes,
    }


def _check_runtime_bounds(sample: Mapping[str, Any], *, minimum_free_disk_bytes: int,
                          require_healthy_source: bool = False) -> str | None:
    if sample["free_disk_bytes"] < minimum_free_disk_bytes:
        return "EXISTING_FREE_DISK_FLOOR_REACHED"
    if sample["queue_items"] > QUEUE_ITEMS_LIMIT or sample["queue_bytes"] > QUEUE_BYTES_LIMIT:
        return "FROZEN_HANDOFF_BOUND_EXCEEDED"
    capture = sample.get("capture")
    if (sample.get("queue_max_items") != QUEUE_ITEMS_LIMIT
            or sample.get("queue_max_bytes") != QUEUE_BYTES_LIMIT
            or not isinstance(capture, Mapping) or capture.get("batch_frame_limit") != 16):
        return "FROZEN_CAPTURE_OR_HANDOFF_CONFIGURATION_MISMATCH"
    if sample["queue_loss_latched"] or sample["frames_rejected"]:
        return "IRREVERSIBLE_PUBLIC_FRAME_LOSS"
    if sample["max_service_gap_ns"] > STREAM_SERVICE_GAP_LIMIT_NS:
        return "FROZEN_STREAM_SERVICE_GAP_EXCEEDED"
    if isinstance(capture, Mapping) and capture.get("terminal_error"):
        return "DURABLE_CAPTURE_FAILED"
    if sample["stream_ingestion_failed"]:
        return "PUBLIC_STREAM_INGESTION_FAILED"
    if require_healthy_source and (
            sample["stream_unresolved_gap"] or not sample["source_health"]
            or any(value != "HEALTHY_CURRENT" for value in sample["source_health"].values())):
        return "PUBLIC_STREAM_SOURCE_UNHEALTHY_OR_GAPPED"
    lanes = sample.get("lanes")
    if require_healthy_source and (not isinstance(lanes, Mapping) or not lanes
            or any(not isinstance(lane, Mapping) or lane.get("connected") is not True
                for lane in lanes.values())):
        return "BINANCE_STREAM_LANE_DISCONNECTED"
    return None


def _due_cycle_failure(cycle: Mapping[str, Any], *, window_end_offset_ns: int) -> str | None:
    if cycle.get("status") == "MISSED_DUE_SLOT":
        return "INCOMPLETE_DUE_POLL_ACCOUNTING"
    if (cycle.get("acquisition_due") is not True or cycle.get("complete") is not True
            or cycle.get("pending") is True):
        return "INCOMPLETE_DUE_POLL_CYCLE"
    if type(cycle.get("completed_offset_ns")) is not int or cycle["completed_offset_ns"] > window_end_offset_ns:
        return "DUE_POLL_COMPLETED_OUTSIDE_MEASUREMENT_WINDOW"
    scheduled = cycle.get("scheduled_offset_ns")
    if (type(scheduled) is not int
            or cycle["completed_offset_ns"] > scheduled + POLL_INTERVAL_NS):
        return "DUE_POLL_CADENCE_EXCEEDED"
    return None


def _next_due_poll_ns(first_due_ns: int, window_start_ns: int) -> int:
    """Find the first source-cadence boundary inside or after the measured window start."""
    if first_due_ns < 0 or window_start_ns < first_due_ns:
        raise ValueError("QUALIFICATION_DUE_POLL_CLOCK_INVALID")
    due_ns = first_due_ns + POLL_INTERVAL_NS
    while due_ns < window_start_ns:
        due_ns += POLL_INTERVAL_NS
    return due_ns


def _report_slot_failure(*, scheduled_ns: int, attempted_ns: int) -> str | None:
    if attempted_ns < scheduled_ns:
        raise ValueError("QUALIFICATION_REPORT_SLOT_CLOCK_INVALID")
    if attempted_ns - scheduled_ns >= REPORT_INTERVAL_NS:
        return "MISSED_DUE_REPORT_SLOT"
    return None


def _measured_traffic_failure(*, window_complete: bool, frame_count: int, payload_bytes: int) -> str | None:
    if frame_count < 0 or payload_bytes < 0:
        raise ValueError("QUALIFICATION_TRAFFIC_COUNTER_INVALID")
    if window_complete and (frame_count == 0 or payload_bytes == 0):
        return "NO_MEASURED_BINANCE_PUBLIC_FRAMES"
    return None


def _sampled_rates(samples: Sequence[Mapping[str, Any]]) -> dict[str, list[dict[str, float | int | None]]]:
    rates: dict[str, list[dict[str, float | int | None]]] = {
        "source_frames_per_second": [],
        "durably_captured_frames_per_second": [],
        "durable_payload_bytes_per_second": [],
    }
    rate_samples = [item for item in samples if type(item.get("sampled_at_monotonic_ns")) is int
        and all(type(item.get(name)) is int for name in ("frames_received", "captured_frames",
                                                         "captured_payload_bytes"))]
    for left, right in zip(rate_samples[:-1], rate_samples[1:], strict=True):
        elapsed_ns = right["sampled_at_monotonic_ns"] - left["sampled_at_monotonic_ns"]
        if elapsed_ns <= 0:
            continue
        elapsed_seconds = elapsed_ns / 1_000_000_000
        for key, counter in (("source_frames_per_second", "frames_received"),
                ("durably_captured_frames_per_second", "captured_frames"),
                ("durable_payload_bytes_per_second", "captured_payload_bytes")):
            delta = right[counter] - left[counter]
            if delta < 0:
                raise ValueError("QUALIFICATION_MONOTONIC_CAPTURE_COUNTER_REGRESSED")
            rates[key].append({"start_offset_ns": left.get("window_offset_ns"),
                "end_offset_ns": right.get("window_offset_ns"), "rate": delta / elapsed_seconds})
    return rates


def _rate_summary(rates: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for name, samples in rates.items():
        values = sorted(float(item["rate"]) for item in samples)
        if not values:
            summary[name] = {"sample_count": 0, "mean": None, "p95": None, "maximum": None}
            continue
        p95_index = max(0, (95 * len(values) + 99) // 100 - 1)
        summary[name] = {"sample_count": len(values), "mean": sum(values) / len(values),
            "p95": values[p95_index], "maximum": values[-1]}
    return summary


def _combine_traffic_windows(values: Mapping[str, Any] | Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    snapshots: list[Mapping[str, Any]] = []
    if isinstance(values, Mapping):
        candidates = [values] if "window_started" in values else list(values.values())
    else:
        candidates = list(values)
    if not candidates or any(not isinstance(item, Mapping) for item in candidates):
        return {"window_started": False, "history_complete": False}
    snapshots.extend(cast(Mapping[str, Any], item) for item in candidates)
    first = snapshots[0]
    start, end = first.get("window_start_monotonic_ns"), first.get("window_end_monotonic_ns")
    if (first.get("window_started") is not True or type(start) is not int or type(end) is not int
            or any(item.get("window_started") is not True
                   or item.get("window_start_monotonic_ns") != start
                   or item.get("window_end_monotonic_ns") != end for item in snapshots)):
        return {"window_started": False, "history_complete": False}
    totals: dict[tuple[str, str], dict[str, Any]] = {}
    buckets: dict[int, dict[tuple[str, str], dict[str, Any]]] = {}
    history_complete = True
    for snapshot in snapshots:
        history_complete = history_complete and snapshot.get("history_complete") is True
        for row in snapshot.get("identities", ()):
            if not isinstance(row, Mapping):
                history_complete = False
                continue
            lane, channel = row.get("lane"), row.get("channel")
            if not isinstance(lane, str) or not lane or not isinstance(channel, str) or not channel:
                history_complete = False
                continue
            key = (lane, channel)
            current = totals.get(key)
            if current is None:
                totals[key] = dict(row)
            else:
                current["frames"] += row.get("frames", 0)
                current["payload_bytes"] += row.get("payload_bytes", 0)
                minima = [item for item in (current.get("payload_min_bytes"), row.get("payload_min_bytes"))
                          if type(item) is int]
                maxima = [item for item in (current.get("payload_max_bytes"), row.get("payload_max_bytes"))
                          if type(item) is int]
                current["payload_min_bytes"] = min(minima) if minima else None
                current["payload_max_bytes"] = max(maxima) if maxima else None
        for bucket in snapshot.get("buckets", ()):
            second = bucket.get("second")
            if type(second) is not int or second < 0:
                history_complete = False
                continue
            target = buckets.setdefault(second, {})
            for row in bucket.get("identities", ()):
                if not isinstance(row, Mapping):
                    history_complete = False
                    continue
                lane, channel = row.get("lane"), row.get("channel")
                if not isinstance(lane, str) or not lane or not isinstance(channel, str) or not channel:
                    history_complete = False
                    continue
                key = (lane, channel)
                current = target.get(key)
                if current is None:
                    target[key] = dict(row)
                else:
                    current["frames"] += row.get("frames", 0)
                    current["payload_bytes"] += row.get("payload_bytes", 0)
                    minima = [item for item in (current.get("payload_min_bytes"), row.get("payload_min_bytes"))
                              if type(item) is int]
                    maxima = [item for item in (current.get("payload_max_bytes"), row.get("payload_max_bytes"))
                              if type(item) is int]
                    current["payload_min_bytes"] = min(minima) if minima else None
                    current["payload_max_bytes"] = max(maxima) if maxima else None
    expected_buckets = (end - start + RESOURCE_SAMPLE_INTERVAL_NS - 1) // RESOURCE_SAMPLE_INTERVAL_NS
    return {"window_started": True, "history_complete": history_complete,
        "window_start_monotonic_ns": start, "window_end_monotonic_ns": end,
        "expected_bucket_count": expected_buckets,
        "identities": [{**row, "lane": key[0], "channel": key[1]}
            for key, row in sorted(totals.items())],
        "buckets": [{"second": second, "identities": [
            {**row, "lane": key[0], "channel": key[1]} for key, row in sorted(identity_rows.items())]}
            for second, identity_rows in sorted(buckets.items())]}


def _traffic_window_summary(snapshot: Mapping[str, Any], *, window_start_ns: int,
                             window_end_ns: int, expected_identities: Sequence[tuple[str, str]]) -> dict[str, Any]:
    if (snapshot.get("window_started") is not True or snapshot.get("history_complete") is not True
            or snapshot.get("window_start_monotonic_ns") != window_start_ns
            or snapshot.get("window_end_monotonic_ns") != window_end_ns):
        raise ValueError("QUALIFICATION_TRAFFIC_WINDOW_MISSING_OR_TRUNCATED")
    expected_buckets = (window_end_ns - window_start_ns + RESOURCE_SAMPLE_INTERVAL_NS - 1) // RESOURCE_SAMPLE_INTERVAL_NS
    if snapshot.get("expected_bucket_count") != expected_buckets:
        raise ValueError("QUALIFICATION_TRAFFIC_BUCKET_COUNT_INVALID")
    totals: dict[tuple[str, str], Mapping[str, Any]] = {}
    for row in snapshot.get("identities", ()):
        if not isinstance(row, Mapping):
            raise ValueError("QUALIFICATION_TRAFFIC_IDENTITY_INVALID")
        lane, channel = row.get("lane"), row.get("channel")
        if (not isinstance(lane, str) or not lane or not isinstance(channel, str) or not channel
                or any(type(row.get(name)) is not int or row[name] < 0
                    for name in ("frames", "payload_bytes"))):
            raise ValueError("QUALIFICATION_TRAFFIC_IDENTITY_INVALID")
        key = (lane, channel)
        if key in totals:
            raise ValueError("QUALIFICATION_TRAFFIC_IDENTITY_INVALID")
        totals[key] = row
    bucket_values: dict[int, dict[tuple[str, str], Mapping[str, Any]]] = {}
    for bucket in snapshot.get("buckets", ()):
        if not isinstance(bucket, Mapping) or type(bucket.get("second")) is not int:
            raise ValueError("QUALIFICATION_TRAFFIC_BUCKET_INVALID")
        second = bucket["second"]
        if not 0 <= second < expected_buckets or second in bucket_values:
            raise ValueError("QUALIFICATION_TRAFFIC_BUCKET_INVALID")
        rows: dict[tuple[str, str], Mapping[str, Any]] = {}
        for row in bucket.get("identities", ()):
            if not isinstance(row, Mapping):
                raise ValueError("QUALIFICATION_TRAFFIC_BUCKET_IDENTITY_INVALID")
            lane_name, channel = row.get("lane"), row.get("channel")
            if (not isinstance(lane_name, str) or not lane_name
                    or not isinstance(channel, str) or not channel
                    or type(row.get("frames")) is not int or row["frames"] < 0
                    or type(row.get("payload_bytes")) is not int or row["payload_bytes"] < 0):
                raise ValueError("QUALIFICATION_TRAFFIC_BUCKET_IDENTITY_INVALID")
            key = (lane_name, channel)
            if key in rows:
                raise ValueError("QUALIFICATION_TRAFFIC_BUCKET_IDENTITY_INVALID")
            rows[key] = row
        bucket_values[second] = rows
    summed: dict[tuple[str, str], list[int]] = {}
    peaks_by_lane: dict[str, dict[str, dict[str, int]]] = {}
    peaks_by_identity: dict[tuple[str, str], dict[str, dict[str, int]]] = {}
    overall_peak = {"frames": {"second": 0, "value": 0},
        "payload_bytes": {"second": 0, "value": 0}}
    for second in range(expected_buckets):
        rows = bucket_values.get(second, {})
        lane_values: dict[str, list[int]] = {}
        all_frames = all_bytes = 0
        for key, row in rows.items():
            pair = summed.setdefault(key, [0, 0])
            pair[0] += row["frames"]
            pair[1] += row["payload_bytes"]
            lane_total = lane_values.setdefault(key[0], [0, 0])
            lane_total[0] += row["frames"]
            lane_total[1] += row["payload_bytes"]
            all_frames += row["frames"]
            all_bytes += row["payload_bytes"]
            identity_peak = peaks_by_identity.setdefault(key, {
                "frames": {"second": 0, "value": 0},
                "payload_bytes": {"second": 0, "value": 0},
            })
            if row["frames"] > identity_peak["frames"]["value"]:
                identity_peak["frames"] = {"second": second, "value": row["frames"]}
            if row["payload_bytes"] > identity_peak["payload_bytes"]["value"]:
                identity_peak["payload_bytes"] = {"second": second, "value": row["payload_bytes"]}
        if all_frames > overall_peak["frames"]["value"]:
            overall_peak["frames"] = {"second": second, "value": all_frames}
        if all_bytes > overall_peak["payload_bytes"]["value"]:
            overall_peak["payload_bytes"] = {"second": second, "value": all_bytes}
        for lane, (frames, payload_bytes) in lane_values.items():
            lane_peak = peaks_by_lane.setdefault(lane, {
                "frames": {"second": 0, "value": 0},
                "payload_bytes": {"second": 0, "value": 0},
            })
            if frames > lane_peak["frames"]["value"]:
                lane_peak["frames"] = {"second": second, "value": frames}
            if payload_bytes > lane_peak["payload_bytes"]["value"]:
                lane_peak["payload_bytes"] = {"second": second, "value": payload_bytes}
    for key in set(summed) | set(totals):
        total = totals.get(key, {})
        if [total.get("frames", 0), total.get("payload_bytes", 0)] != summed.get(key, [0, 0]):
            raise ValueError("QUALIFICATION_TRAFFIC_BUCKET_TOTAL_MISMATCH")
    duration_seconds = (window_end_ns - window_start_ns) / NANOSECONDS_PER_SECOND
    identities: list[dict[str, Any]] = []
    for lane, channel in sorted(set(expected_identities) | set(totals)):
        row = totals.get((lane, channel), {})
        frames = int(row.get("frames", 0))
        payload_bytes = int(row.get("payload_bytes", 0))
        identities.append({"lane": lane, "channel": channel, "frames": frames,
            "payload_bytes": payload_bytes, "frames_per_second": frames / duration_seconds,
            "payload_bytes_per_second": payload_bytes / duration_seconds,
            "payload_min_bytes": row.get("payload_min_bytes"), "payload_max_bytes": row.get("payload_max_bytes"),
            "mean_payload_bytes": payload_bytes / frames if frames else None,
            "observed": frames > 0})
    for lane, channel in expected_identities:
        peaks_by_identity.setdefault((lane, channel), {
            "frames": {"second": 0, "value": 0},
            "payload_bytes": {"second": 0, "value": 0},
        })
        peaks_by_lane.setdefault(lane, {
            "frames": {"second": 0, "value": 0},
            "payload_bytes": {"second": 0, "value": 0},
        })
    lanes: dict[str, dict[str, Any]] = {}
    for item in identities:
        lane_summary = lanes.setdefault(item["lane"], {"frames": 0, "payload_bytes": 0, "channels": {}})
        lane_summary["frames"] += item["frames"]
        lane_summary["payload_bytes"] += item["payload_bytes"]
        lane_summary["channels"][item["channel"]] = item
    for lane_summary in lanes.values():
        lane_summary["frames_per_second"] = lane_summary["frames"] / duration_seconds
        lane_summary["payload_bytes_per_second"] = lane_summary["payload_bytes"] / duration_seconds
    return {"window_start_monotonic_ns": window_start_ns, "window_end_monotonic_ns": window_end_ns,
        "complete": True, "measured_seconds": duration_seconds,
        "identity_rates": identities, "lane_rates": lanes,
        "one_second_peaks": {"all_lanes": overall_peak, "lanes": peaks_by_lane,
            "identities": [{"lane": key[0], "channel": key[1], **value}
                for key, value in sorted(peaks_by_identity.items())]},
        "peak_bucket_basis": "fixed one-second buckets anchored at window_start_monotonic_ns"}


def _trade_geometry_summary(indexed_summary: Mapping[str, Any],
                            trade_geometry: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if indexed_summary.get("complete") is not True:
        return {"complete": False, "measurement": "INCOMPLETE",
            "missing_identities": [{"reason": "INDEXED_TRAFFIC_INCOMPLETE"}],
            "by_identity": [], "trade_rows": 0, "indexed_frames": 0,
            "mean_trades_per_indexed_frame": None}
    indexed = {(item["lane"], item["channel"]): item["frames"]
               for item in indexed_summary.get("identity_rates", ())}
    measured = {(item.get("lane"), item.get("channel")): item for item in trade_geometry
                if isinstance(item, Mapping)}
    missing = []
    rows = []
    for identity, frame_count in sorted(indexed.items()):
        item = measured.get(identity)
        if frame_count == 0 and item is None:
            rows.append({"lane": identity[0], "channel": identity[1], "indexed_frames": 0,
                "parsed_frames": 0, "trade_rows": 0, "frames_with_trades": 0,
                "mean_trades_per_frame": None, "min_trades_per_frame": None,
                "max_trades_per_frame": None, "measurement": "NO_INDEXED_FRAMES"})
            continue
        if item is None or item.get("parsed_frames") != frame_count:
            missing.append({"lane": identity[0], "channel": identity[1],
                "indexed_frames": frame_count, "parsed_frames": item.get("parsed_frames") if item else None})
            continue
        trade_rows = item.get("trade_rows")
        if type(trade_rows) is not int or trade_rows < 0:
            missing.append({"lane": identity[0], "channel": identity[1], "reason": "TRADE_COUNT_MISSING"})
            continue
        rows.append({"lane": identity[0], "channel": identity[1], "indexed_frames": frame_count,
            "parsed_frames": item["parsed_frames"], "trade_rows": trade_rows,
            "frames_with_trades": item.get("frames_with_trades"),
            "mean_trades_per_frame": trade_rows / frame_count if frame_count else None,
            "min_trades_per_frame": item.get("min_trades_per_frame"),
            "max_trades_per_frame": item.get("max_trades_per_frame")})
    trade_rows = sum(int(item["trade_rows"]) for item in rows)
    indexed_frames = sum(int(item["indexed_frames"]) for item in rows)
    complete = not missing
    measurement = ("INCOMPLETE" if not complete else
        "COMPLETE" if indexed_frames > 0 else "NO_INDEXED_FRAMES")
    return {"complete": complete, "measurement": measurement,
        "missing_identities": missing, "by_identity": rows,
        "trade_rows": trade_rows, "indexed_frames": indexed_frames,
        "mean_trades_per_indexed_frame": (trade_rows / indexed_frames if indexed_frames else None)}


def _qualification_measurements(runtime_metrics: Mapping[str, Any], *,
                                stream_plan: Mapping[str, Any], repository_metrics: Mapping[str, Any],
                                resource_summary: Mapping[str, Any], window_start_ns: int,
                                window_end_ns: int) -> tuple[dict[str, Any], str | None]:
    expected_identities = [(lane, topic) for lane, topics in stream_plan.get("lane_topics", {}).items()
                           for topic in topics]
    failures: list[str] = []
    source_snapshot = _combine_traffic_windows(runtime_metrics.get("source", {}))
    capture_bundle = runtime_metrics.get("capture", {})
    capture_snapshot = capture_bundle.get("traffic", {}) if isinstance(capture_bundle, Mapping) else {}
    index_snapshot = runtime_metrics.get("index", {})
    traffic: dict[str, Any] = {}
    for name, snapshot in (("source", source_snapshot), ("capture", capture_snapshot), ("index", index_snapshot)):
        try:
            traffic[name] = _traffic_window_summary(snapshot, window_start_ns=window_start_ns,
                window_end_ns=window_end_ns, expected_identities=expected_identities)
        except (TypeError, ValueError, KeyError):
            failures.append(f"{name.upper()}_TRAFFIC_WINDOW_INCOMPLETE")
            traffic[name] = {"complete": False, "window_start_monotonic_ns": window_start_ns,
                "window_end_monotonic_ns": window_end_ns, "identity_rates": [], "lane_rates": {},
                "one_second_peaks": None}
    trade_geometry = _trade_geometry_summary(traffic["index"], runtime_metrics.get("trade_geometry", ()))
    if not trade_geometry["complete"]:
        failures.append("TRADE_ROWS_PER_INDEXED_FRAME_INCOMPLETE")
    archive = capture_bundle.get("archive") if isinstance(capture_bundle, Mapping) else None
    archive_fields = ("operation_count", "extent_count", "archive_bytes_written", "arrow_encode_ns",
        "compression_ns", "write_ns", "fsync_ns", "rename_ns", "directory_sync_ns", "total_ns", "max_fsync_ns")
    if (not isinstance(archive, Mapping) or archive.get("history_complete") is not True
            or archive.get("window_start_monotonic_ns") != window_start_ns
            or archive.get("window_end_monotonic_ns") != window_end_ns
            or any(type(archive.get(name)) is not int or archive[name] < 0 for name in archive_fields)
            or archive.get("operation_count", 0) == 0 or archive.get("archive_bytes_written", 0) == 0):
        failures.append("RAW_ARCHIVE_TIMING_OR_BYTES_INCOMPLETE")
    persistence_fields = ("transaction_count", "successful_transaction_count", "failed_transaction_count",
        "commit_attempt_count", "commit_duration_sum_ns", "commit_duration_max_ns",
        "begin_duration_sum_ns", "begin_duration_max_ns", "row_and_archive_duration_sum_ns",
        "row_and_archive_duration_max_ns", "rollback_duration_sum_ns", "rollback_duration_max_ns")
    if (repository_metrics.get("window_started") is not True
            or repository_metrics.get("history_complete") is not True
            or repository_metrics.get("window_start_monotonic_ns") != window_start_ns
            or repository_metrics.get("window_end_monotonic_ns") != window_end_ns
            or any(type(repository_metrics.get(name)) is not int or repository_metrics[name] < 0
                   for name in persistence_fields)
            or repository_metrics.get("commit_attempt_count", 0) == 0):
        failures.append("SQLITE_TRANSACTION_TIMING_INCOMPLETE")
    phase_window = runtime_metrics.get("phase_timings_ns")
    phases = phase_window.get("phases") if isinstance(phase_window, Mapping) else None
    if (not isinstance(phase_window, Mapping) or phase_window.get("window_started") is not True
            or phase_window.get("history_complete") is not True
            or phase_window.get("window_start_monotonic_ns") != window_start_ns
            or phase_window.get("window_end_monotonic_ns") != window_end_ns
            or not isinstance(phases, Mapping) or not phases
            or any(not isinstance(value, Mapping)
                   or any(type(value.get(key)) is not int or value[key] < 0
                          for key in ("count", "sum_ns", "max_ns")) for value in phases.values())):
        failures.append("WRITER_WORKER_PHASE_TIMING_INCOMPLETE")
    if resource_summary.get("measurement_window_complete") is not True:
        failures.append("RESOURCE_SAMPLE_COVERAGE_INCOMPLETE")
    measurements = {
        "complete": not failures,
        "window_start_monotonic_ns": window_start_ns,
        "window_end_monotonic_ns": window_end_ns,
        "source_traffic": traffic["source"],
        "durable_capture_traffic": traffic["capture"],
        "indexed_traffic": traffic["index"],
        "trade_geometry": trade_geometry,
        "raw_archive_timing": dict(archive) if isinstance(archive, Mapping) else None,
        "sqlite_transaction_timing": dict(repository_metrics),
        "writer_worker_phase_timings_ns": dict(phases) if isinstance(phases, Mapping) else None,
        "phase_timing_semantics": "Phase-local durations overlap across nested and worker activity; do not sum them as elapsed wall time.",
        "resource_sampling": dict(resource_summary),
        "missing_measurements": failures,
    }
    return measurements, ("QUALIFICATION_TELEMETRY_INCOMPLETE_" + failures[0]
        if failures else None)


def _snapshot_cycle(port: Any, repository: Any, recovery: Any, *, scheduled_ns: int,
                    window_start_ns: int) -> dict[str, Any]:
    cycle_now_ns = time.time_ns()
    cycle_started = time.monotonic_ns()
    batch = port.collect(
        repository,
        now_ns=cycle_now_ns,
        recovery=recovery,
    )
    completed = time.monotonic_ns()
    snapshot = port.last_acquisition_snapshot
    if snapshot is None:
        raise ValueError("QUALIFICATION_DUE_CYCLE_SNAPSHOT_MISSING")
    sources = tuple(port.public_source.required_source_ids)
    if (sources != (BINANCE_REST_SOURCE_ID,)
            or snapshot.source_snapshot.get("enabled_venues") != ["BINANCE"]):
        raise ValueError("QUALIFICATION_DUE_CYCLE_SOURCE_SCOPE_FAILED")
    return {
        "scheduled_offset_ns": max(0, scheduled_ns - window_start_ns),
        "started_offset_ns": max(0, cycle_started - window_start_ns),
        "completed_offset_ns": max(0, completed - window_start_ns),
        # A due BroadPublicSourceV2 snapshot omits this optional field;
        # non-due cache reuse explicitly records False.
        "acquisition_due": snapshot.source_snapshot.get("acquisition_due", True) is True,
        "complete": snapshot.complete is True,
        "pending": snapshot.source_snapshot.get("pending", False) is True,
        "request_count": snapshot.request_count,
        "successful_request_count": snapshot.successful_request_count,
        "record_count": len(snapshot.records),
        "source_snapshot_id": snapshot.source_snapshot.get("source_snapshot_id"),
        "batch_event_count": len(batch.events),
        "stream_plan": _stream_plan_manifest(port),
    }


def _report_completion(worker: Any, *, completed: list[dict[str, Any]], report_slots: list[dict[str, Any]],
                      repository: Any, run_id: str, port: Any, now_ns: int) -> None:
    result = worker.poll()
    if result is not None:
        exported = result.result
        refs = _causal_checkpoint_read(repository, run_id=run_id, port=port,
            cutoff_ns=result.completed_at_ns)
        overlap = any(
            (available := repository.effective_available_at_ns(ref)) is not None
            and result.started_at_ns <= available <= result.completed_at_ns
            for ref in refs)
        active_slot = next((item for item in reversed(report_slots)
            if item.get("started") and "publication_overlap" not in item), None)
        if active_slot is not None:
            active_slot["completed_at_ns"] = result.completed_at_ns
            active_slot["publication_overlap"] = overlap
            active_slot["completion_observed_at_ns"] = now_ns
        completed.append({"state": result.status, "started_at_ns": result.started_at_ns,
            "completed_at_ns": result.completed_at_ns, "error_type": result.error_type,
            "publication_overlap": overlap,
            "export_result": exported, "export_result_hash": sha256_json(exported)
                if isinstance(exported, Mapping) else None,
            "observed_at_ns": now_ns})


def run_qualification_component(run: Path, *, supervisor_token: str | None = None,
                               stop_requested: Callable[[], bool] = lambda: False,
                               public_port_factory: Callable[..., Any] | None = None) -> dict[str, Any]:
    """Run one non-resumable supervised live diagnostic; never issue capacity."""
    from ..instruments import VenueV2
    from ..memory.repository import OpsRepository
    from ..product import ResearchRunConfigV2, _run_config, export_run, load_run
    from ..runtime.production import create_broad_public_port
    from ..runtime.read_only_report_worker import ReadOnlyReportWorkerV1
    from ..runtime.storage_preflight import qualify_storage_path

    run = run.resolve(strict=True)
    manifest = load_run(run)
    binding = _validate_profile_binding(run, manifest)
    if (not isinstance(supervisor_token, str) or not supervisor_token
            or not secrets.compare_digest(hashlib.sha256(supervisor_token.encode("ascii")).hexdigest(),
                                          binding["supervisor_token_sha256"])):
        raise ValueError("QUALIFICATION_SUPERVISOR_TOKEN_INVALID")
    _assert_fresh_run(run)
    _assert_owner_only_run(run)
    _write_immutable(run / "qualification-start.json", {
        "version": "BINANCE_QUALIFICATION_SINGLE_START_V1", "run_id": manifest["run_id"],
        "profile_binding_hash": binding["content_hash"], "started_at_ns": time.time_ns(),
    })
    config = _run_config(manifest["configuration"])
    if not isinstance(config, ResearchRunConfigV2):
        raise ValueError("QUALIFICATION_REQUIRES_V2_CONFIG")
    selected_path = validate_selected_path(Path(binding["selected_path"]))
    if os.stat(run).st_dev != os.stat(selected_path).st_dev:
        raise ValueError("QUALIFICATION_RUN_DEVICE_MISMATCH")
    child_probe = qualify_storage_path(run, identity_sha256=manifest["content_hash"]).as_dict()
    if not child_probe["allowed"]:
        probe_failure_receipt = _write_receipt(run, {"schema_version": 1, "status": "DIAGNOSTIC_FAILED",
            "reason": "QUALIFICATION_RUN_PATH_PROBE_REJECTED", "profile_version": QUALIFICATION_PROFILE_VERSION,
            "profile_hash": profile_contract_hash(), "run_id": manifest["run_id"],
            "source_sha": manifest["source_sha"], "run_path_probe_ref": child_probe["content_hash"],
            "measured_window_seconds": 0, "capacity_qualification": "TEST GATE"})
        return probe_failure_receipt

    repository = OpsRepository(run / "ops.sqlite")
    port: Any | None = None
    report_worker = None
    report_results: list[dict[str, Any]] = []
    receipt: dict[str, Any] | None = None
    cycle_records: list[dict[str, Any]] = []
    report_slots: list[dict[str, Any]] = []
    telemetry: list[dict[str, Any]] = []
    failure_reason: str | None = None
    acquisition_worker_closed = False
    readiness_started = time.monotonic_ns()
    window_start_ns: int | None = None
    window_end_ns: int | None = None
    window_completed = False
    last_active_health: Mapping[str, Any] = {}
    checkpoint_refs: tuple[str, ...] = ()
    readiness_at_ns: int | None = None
    readiness_elapsed_ns = 0
    try:
        _assert_empty_repository(repository)
        # This factory receives a one-element enum tuple. It constructs only
        # public readers; no provider, account, order or capital object exists.
        port_factory = public_port_factory or create_broad_public_port
        port = cast(Any, port_factory(enabled_venues=(VenueV2.BINANCE,), capture_payload_metrics=True))
        _validate_port_scope(port)
        recovery = port.recover(repository, now_ns=time.time_ns())
        _validate_capture_metrics_enabled(port)
        _stream_plan_manifest(port)
        port._qualification_repository = repository
        port._qualification_recovery = recovery
        report_worker = ReadOnlyReportWorkerV1(lambda: export_run(run))
        if (port.public_source.current_products
                and any(product.key.venue != VenueV2.BINANCE for product in port.public_source.current_products)):
            raise ValueError("QUALIFICATION_BOOTSTRAP_CONTAINS_NON_BINANCE_PRODUCT")

        ready = False
        first_due_start_ns: int | None = None
        next_warmup_collect_ns = readiness_started
        while time.monotonic_ns() - readiness_started < WARMUP_DEADLINE_SECONDS * 1_000_000_000:
            if stop_requested():
                failure_reason = "CANCELLED_DURING_WARMUP"
                break
            if port.public_stream_source.plan is None or port.public_stream_source.capture is None:
                raise ValueError("QUALIFICATION_STREAM_PLAN_UNAVAILABLE")
            port.service_public_stream(repository)
            snapshot = port.last_acquisition_snapshot
            if (snapshot is None or not snapshot.complete) and time.monotonic_ns() >= next_warmup_collect_ns:
                cycle_now_ns = time.time_ns()
                cycle_started = time.monotonic_ns()
                port.collect(repository, now_ns=cycle_now_ns, recovery=recovery)
                snapshot = port.last_acquisition_snapshot
                if (snapshot is not None
                        and snapshot.source_snapshot.get("acquisition_due", True) is True):
                    first_due_start_ns = cycle_started
                    next_warmup_collect_ns = cycle_started + POLL_INTERVAL_NS
                if snapshot is not None and not snapshot.complete:
                    next_warmup_collect_ns = max(next_warmup_collect_ns, cycle_started + POLL_INTERVAL_NS)
            runtime_sample = _runtime_telemetry(port, run=run)
            reason = _check_runtime_bounds(runtime_sample,
                minimum_free_disk_bytes=config.minimum_free_disk_bytes)
            if reason:
                failure_reason = reason
                break
            if (runtime_sample["source_health"]
                    and not runtime_sample["stream_unresolved_gap"]
                    and runtime_sample["lanes"]
                    and all(lane["connected"] is True for lane in runtime_sample["lanes"].values())
                    and all(value == "HEALTHY_CURRENT" for value in runtime_sample["source_health"].values())):
                checkpoint_refs = _causal_checkpoint_read(repository, run_id=manifest["run_id"],
                    port=port, cutoff_ns=time.time_ns())
                if checkpoint_refs and snapshot is not None and snapshot.complete:
                    ready = True
                    readiness_at_ns = time.time_ns()
                    readiness_elapsed_ns = max(0, time.monotonic_ns() - readiness_started)
                    last_active_health = dict(runtime_sample["source_health"])
                    break
            time.sleep(0.05)
        if failure_reason is None and not ready:
            failure_reason = "SOURCE_READINESS_OR_WARMUP_DEADLINE_EXCEEDED"
        if not ready:
            readiness_elapsed_ns = max(0, time.monotonic_ns() - readiness_started)
        if failure_reason is None:
            assert first_due_start_ns is not None
            # Arm every opt-in recorder before the exact half-open window begins.
            # Traffic and completion events before this future boundary are excluded.
            window_start_ns = time.monotonic_ns() + 100_000_000
            window_end_ns = window_start_ns + MEASURED_SECONDS * 1_000_000_000
            next_poll_ns = _next_due_poll_ns(first_due_start_ns, window_start_ns)
            next_report_ns = window_start_ns + REPORT_INTERVAL_NS
            next_sample_ns = window_start_ns
            capture_layer = port.public_stream_source.capture
            port.public_stream_source.begin_qualification_window(window_start_ns, window_end_ns)
            repository.begin_persistence_timing_window(start_ns=window_start_ns, end_ns=window_end_ns)
            window_disconnect_counts = {name: lane["disconnect_count"]
                for name, lane in _runtime_telemetry(port, run=run)["lanes"].items()}
            time.sleep(max(0, window_start_ns - time.monotonic_ns()) / NANOSECONDS_PER_SECOND)
            while time.monotonic_ns() < window_end_ns:
                now_mono = time.monotonic_ns()
                if stop_requested():
                    failure_reason = "CANCELLED_DURING_MEASUREMENT"
                    break
                port.service_public_stream(repository)
                runtime_sample = _runtime_telemetry(port, run=run)
                last_active_health = dict(runtime_sample["source_health"])
                if (set(runtime_sample["lanes"]) != set(window_disconnect_counts)
                        or any(lane["disconnect_count"] != window_disconnect_counts.get(name)
                            for name, lane in runtime_sample["lanes"].items())):
                    failure_reason = "BINANCE_STREAM_DISCONNECTED_DURING_MEASUREMENT"
                    break
                reason = _check_runtime_bounds(runtime_sample,
                    minimum_free_disk_bytes=config.minimum_free_disk_bytes, require_healthy_source=True)
                if reason:
                    failure_reason = reason
                    break
                if now_mono >= next_poll_ns and next_poll_ns < window_end_ns:
                    # Record missed slots explicitly; never collapse missed
                    # 10-second refresh work into a later synthetic success.
                    if now_mono - next_poll_ns >= POLL_INTERVAL_NS:
                        cycle_records.append({"scheduled_offset_ns": next_poll_ns - window_start_ns,
                            "status": "MISSED_DUE_SLOT"})
                        failure_reason = "INCOMPLETE_DUE_POLL_ACCOUNTING"
                        break
                    cycle = _snapshot_cycle(port, repository, recovery, scheduled_ns=next_poll_ns,
                        window_start_ns=window_start_ns)
                    cycle_records.append(cycle)
                    failure_reason = _due_cycle_failure(cycle,
                        window_end_offset_ns=MEASURED_SECONDS * 1_000_000_000)
                    if failure_reason:
                        break
                    next_poll_ns += POLL_INTERVAL_NS
                report_attempted_ns = time.monotonic_ns()
                if report_attempted_ns >= next_report_ns and next_report_ns < window_end_ns:
                    missed = _report_slot_failure(scheduled_ns=next_report_ns, attempted_ns=report_attempted_ns)
                    if missed:
                        report_slots.append({"scheduled_offset_ns": next_report_ns - window_start_ns,
                            "status": missed})
                        failure_reason = "INCOMPLETE_REPORT_SLOT_ACCOUNTING"
                        break
                    slot: dict[str, Any] = {"scheduled_offset_ns": next_report_ns - window_start_ns,
                        "started_at_ns": time.time_ns(),
                        "started_offset_ns": report_attempted_ns - window_start_ns,
                        "started": report_worker.start()}
                    if not slot["started"]:
                        slot["status"] = "REPORT_SLOT_UNAVAILABLE"
                        failure_reason = "INCOMPLETE_REPORT_SLOT_ACCOUNTING"
                    report_slots.append(slot)
                    next_report_ns += REPORT_INTERVAL_NS
                    if failure_reason:
                        break
                now_wall = time.time_ns()
                _report_completion(report_worker, completed=report_results, report_slots=report_slots,
                    repository=repository, run_id=manifest["run_id"], port=port, now_ns=now_wall)
                sample_at_ns = runtime_sample["sampled_at_monotonic_ns"]
                if sample_at_ns >= next_sample_ns:
                    system = _resource_telemetry(run, port=port)
                    resource_at_ns = system["sampled_at_monotonic_ns"]
                    runtime_sample["resource"] = system
                    runtime_sample["window_offset_ns"] = max(0, resource_at_ns - window_start_ns)
                    telemetry.append(runtime_sample)
                    next_sample_ns = (window_start_ns
                        + ((resource_at_ns - window_start_ns) // RESOURCE_SAMPLE_INTERVAL_NS + 1)
                        * RESOURCE_SAMPLE_INTERVAL_NS)
                wait_ns = min(50_000_000, max(1_000_000, window_end_ns - time.monotonic_ns()))
                time.sleep(wait_ns / 1_000_000_000)
            window_completed = time.monotonic_ns() >= window_end_ns
            if window_completed and failure_reason is None:
                recorded_poll_offsets = {item.get("scheduled_offset_ns") for item in cycle_records}
                while next_poll_ns < window_end_ns:
                    offset = next_poll_ns - window_start_ns
                    if offset not in recorded_poll_offsets:
                        cycle_records.append({"scheduled_offset_ns": offset, "status": "MISSED_DUE_SLOT"})
                        failure_reason = "INCOMPLETE_DUE_POLL_ACCOUNTING"
                    next_poll_ns += POLL_INTERVAL_NS
                recorded_report_offsets = {item.get("scheduled_offset_ns") for item in report_slots}
                while next_report_ns < window_end_ns:
                    offset = next_report_ns - window_start_ns
                    if offset not in recorded_report_offsets:
                        report_slots.append({"scheduled_offset_ns": offset, "status": "MISSED_DUE_SLOT"})
                        failure_reason = failure_reason or "INCOMPLETE_REPORT_SLOT_ACCOUNTING"
                    next_report_ns += REPORT_INTERVAL_NS
            if not window_completed and failure_reason is None:
                failure_reason = "MEASUREMENT_WINDOW_DID_NOT_REACH_120_SECONDS"
            measurement_runtime_metrics = port.public_stream_source.qualification_metrics_snapshot()
            if window_completed and not any(
                    isinstance(item.get("resource"), Mapping)
                    and item["resource"].get("sampled_at_monotonic_ns", 0) >= window_end_ns
                    for item in telemetry):
                boundary_resource = _resource_telemetry(run, port=port)
                telemetry.append({"resource": boundary_resource, "window_boundary_sample": True})
        # Stop acquisition and drain every accepted frame through the existing
        # raw-first capture, sole writer, commit and post-commit observer.
        if port is not None:
            port.finish_public_capture(repository)
            final_runtime = _runtime_telemetry(port, run=run)
            status = port.public_stream_source.status()
            handoff = status.handoff
            capture = status.capture
            if (handoff.queue_items or handoff.queue_bytes or capture.get("pending_frames")
                    or capture.get("terminal_error") or handoff.loss_latched or handoff.frames_rejected
                    or handoff.frames_received != capture.get("captured_frames")
                    or capture.get("captured_frames") != capture.get("delivered_frames")
                    or capture.get("delivered_frames") != status.indexed_frames):
                failure_reason = failure_reason or "CLEAN_DRAIN_OR_FRAME_ACCOUNTING_FAILED"
            unobserved = repository.unobserved_publications_v2(manifest["run_id"])
            if unobserved:
                failure_reason = failure_reason or "UNOBSERVED_PUBLICATION_REMAINS"
            cutoff_ns = time.time_ns()
            try:
                final_refs = _causal_checkpoint_read(repository, run_id=manifest["run_id"],
                    port=port, cutoff_ns=cutoff_ns, require_complete=True)
            except ValueError:
                final_refs = ()
                failure_reason = failure_reason or "CUTOFF_VISIBLE_STREAM_CHECKPOINT_SET_INCOMPLETE"
            if not final_refs:
                failure_reason = failure_reason or "NO_CUTOFF_VISIBLE_POSTCOMMIT_CHECKPOINT"
            try:
                source_selection = _source_selection_manifest(repository, port, cutoff_ns=cutoff_ns)
            except Exception as exc:
                source_selection = {"error_type": type(exc).__name__}
                failure_reason = failure_reason or "SOURCE_SELECTION_MANIFEST_UNAVAILABLE"
            if final_runtime["max_service_gap_ns"] > STREAM_SERVICE_GAP_LIMIT_NS:
                failure_reason = failure_reason or "FROZEN_STREAM_SERVICE_GAP_EXCEEDED"
            # The public source is stopped and accepted data is drained before
            # joining a potentially slow read-only export. The reporter ran
            # concurrently during the live window; its tail cannot starve the
            # single-writer stream service path.
            if report_worker is not None:
                close_ok = report_worker.close(timeout_s=30.0)
                _report_completion(report_worker, completed=report_results, report_slots=report_slots,
                    repository=repository, run_id=manifest["run_id"], port=port, now_ns=time.time_ns())
                expected_reports = sum(bool(slot.get("started")) for slot in report_slots)
                if (not close_ok or len(report_results) != expected_reports) and failure_reason is None:
                    failure_reason = "REPORT_EXPORT_DID_NOT_COMPLETE"
                if (any(slot.get("started") and slot.get("publication_overlap") is not True
                        for slot in report_slots) and failure_reason is None):
                    failure_reason = "REPORT_EXPORT_PUBLICATION_OVERLAP_NOT_PROVEN"
                for completion in report_results:
                    exported = completion.get("export_result")
                    if (completion["state"] != "IMPLEMENTED" or not isinstance(exported, Mapping)
                            or exported.get("blocked_future_evidence") is True
                            or exported.get("has_more") is True
                            or any(value for value in exported.get("validation_failures", {}).values())):
                        failure_reason = failure_reason or "REPORT_EXPORT_CAUSAL_VALIDATION_FAILED"
            acquisition_worker = getattr(port, "_serviced_acquisition", None)
            acquisition_worker_closed = (acquisition_worker.close(timeout_s=0.1)
                if acquisition_worker is not None else True)
            if not acquisition_worker_closed:
                failure_reason = failure_reason or "PUBLIC_REST_WORKER_DID_NOT_STOP_CLEANLY"
            final_backlog = _final_backlog_summary(telemetry)
            if window_completed and (not final_backlog["measurement_complete"]
                    or final_backlog["slope_frames_per_second"] > 0) and failure_reason is None:
                failure_reason = "FINAL_30_SECOND_BACKLOG_NOT_STABLE"
            capture_obj = getattr(port.public_stream_source, "capture", None)
            capture_detail = capture_obj.status().capture if capture_obj is not None else {}
            payload_bytes = int(capture_detail.get("payload_bytes_written", 0))
            payload_frames = int(capture_detail.get("captured_frames", 0))
            if window_start_ns is None or window_end_ns is None:
                actual_window_end_ns = None
            elif window_completed:
                actual_window_end_ns = window_end_ns
            else:
                actual_window_end_ns = min(time.monotonic_ns(), window_end_ns)
            measured_seconds = ((actual_window_end_ns - window_start_ns) / 1_000_000_000
                if actual_window_end_ns is not None and window_start_ns is not None else 0)
            capture_traffic_snapshot = measurement_runtime_metrics.get("capture", {}).get("traffic", {})
            captured_identities = (capture_traffic_snapshot.get("identities", ())
                if isinstance(capture_traffic_snapshot, Mapping) else ())
            measured_capture_frames = sum(int(item.get("frames", 0)) for item in captured_identities
                if isinstance(item, Mapping))
            measured_capture_bytes = sum(int(item.get("payload_bytes", 0)) for item in captured_identities
                if isinstance(item, Mapping))
            measured_payload_histogram = (capture_layer.payload_size_distribution_in_window(
                window_start_ns, window_end_ns) if actual_window_end_ns is not None else {})
            traffic_failure = _measured_traffic_failure(window_complete=window_completed,
                frame_count=measured_capture_frames, payload_bytes=measured_capture_bytes)
            failure_reason = failure_reason or traffic_failure
            observed_rates = _sampled_rates(telemetry)
            resource_window = _resource_summary(telemetry, window_complete=window_completed,
                window_start_ns=window_start_ns, window_end_ns=window_end_ns)
            if window_start_ns is not None and window_end_ns is not None:
                stream_plan = source_selection.get("stream_plan", {})
                measurement_telemetry, telemetry_failure = _qualification_measurements(
                    measurement_runtime_metrics, stream_plan=stream_plan,
                    repository_metrics=repository.persistence_timing_window(),
                    resource_summary=resource_window, window_start_ns=window_start_ns,
                    window_end_ns=window_end_ns)
                failure_reason = failure_reason or telemetry_failure
                histogram_frames = sum(value for value in measured_payload_histogram.values()
                                       if type(value) is int)
                histogram_complete = histogram_frames == measured_capture_frames
                measurement_telemetry["payload_geometry"] = {
                    "measurement": "COMPLETE" if histogram_complete else "MISSING_OR_TRUNCATED",
                    "captured_frames": measured_capture_frames,
                    "histogram_frames": histogram_frames,
                    "mean_payload_bytes": (measured_capture_bytes / measured_capture_frames
                        if histogram_complete and measured_capture_frames else None),
                    "payload_size_histogram": measured_payload_histogram,
                    "timestamp_basis": "durable capture seal completion monotonic timestamp",
                }
                if not histogram_complete:
                    measurement_telemetry["missing_measurements"].append("DURABLE_PAYLOAD_GEOMETRY_INCOMPLETE")
                    failure_reason = failure_reason or "QUALIFICATION_TELEMETRY_INCOMPLETE_DURABLE_PAYLOAD_GEOMETRY"
            else:
                measurement_telemetry = {"missing_measurements": ["MEASUREMENT_WINDOW_NOT_STARTED"]}
                failure_reason = failure_reason or "QUALIFICATION_TELEMETRY_INCOMPLETE_MEASUREMENT_WINDOW"
            body = {
                "schema_version": 1,
                "status": "DIAGNOSTIC_FAILED" if failure_reason else "DIAGNOSTIC_COMPLETE",
                "reason": failure_reason,
                "profile_version": QUALIFICATION_PROFILE_VERSION,
                "profile_hash": binding["profile_hash"],
                "reviewed_design_sha": binding["reviewed_design_sha"],
                "reviewed_proposal_sha256": binding["reviewed_proposal_sha256"],
                "run_id": manifest["run_id"], "run_manifest_hash": manifest["content_hash"],
                "configuration_hash": manifest["config_hash"], "source_sha": manifest["source_sha"],
                "build_identity_hash": binding["build_identity_hash"],
                "selected_path_sha256": binding["selected_path_sha256"],
                "device_id_sha256": binding["device_id_sha256"],
                "host_path_probe_ref": binding["host_path_probe_ref"],
                "run_path_probe_ref": child_probe["content_hash"],
                "ready_at_ns": readiness_at_ns,
                "readiness_elapsed_ns": readiness_elapsed_ns,
                "public_rest_worker_closed": acquisition_worker_closed,
                "measured_window_seconds": measured_seconds,
                "measurement_window_complete": window_completed,
                "window_start_monotonic_ns": window_start_ns,
                "window_end_monotonic_ns": actual_window_end_ns,
                "window_definition": "[start,start+120s); finalization is outside measured window",
                "due_poll_cycles": cycle_records, "report_slots": report_slots,
                "report_completions": [{key: value for key, value in item.items() if key != "export_result"}
                    for item in report_results],
                "report_validation": [{"state": item["state"], "error_type": item["error_type"],
                    "export_result_hash": item.get("export_result_hash"),
                    "manifest_sha256": item["export_result"].get("manifest_sha256")
                        if isinstance(item.get("export_result"), Mapping) else None,
                    "partition_sha256": item["export_result"].get("partition_sha256")
                        if isinstance(item.get("export_result"), Mapping) else None,
                    "has_more": item["export_result"].get("has_more") if isinstance(item.get("export_result"), Mapping) else None,
                    "blocked_future_evidence": item["export_result"].get("blocked_future_evidence")
                        if isinstance(item.get("export_result"), Mapping) else None,
                    "validation_failures": item["export_result"].get("validation_failures")
                        if isinstance(item.get("export_result"), Mapping) else None}
                    for item in report_results],
                "runtime_samples": telemetry,
                "resource_summary": resource_window,
                "diagnostic_measurements": measurement_telemetry,
                "final_30_second_backlog": final_backlog,
                "final_runtime": final_runtime,
                "venue_isolation_evidence": {
                    "enabled_venues": final_runtime["enabled_venues"],
                    "public_source_ids": final_runtime["source_ids"],
                    "bybit_reader_active": final_runtime["bybit_reader_active"],
                    "stream_lanes": sorted(final_runtime["lanes"]),
                    "stream_identities": source_selection.get("stream_plan", {}).get("identities", []),
                },
                "last_active_source_health": dict(last_active_health),
                "causal_checkpoint_refs": list(final_refs if not failure_reason else checkpoint_refs),
                "source_selection": source_selection,
                "unobserved_publication_count": len(repository.unobserved_publications_v2(manifest["run_id"])),
                "captured_payload_bytes": payload_bytes, "captured_payload_frames": payload_frames,
                "measured_durable_frames": measured_capture_frames,
                "measured_durable_frames_per_second": (measured_capture_frames / measured_seconds
                    if measured_seconds > 0 else 0),
                "measured_durable_payload_bytes": measured_capture_bytes,
                "measured_durable_payload_bytes_per_second": (measured_capture_bytes / measured_seconds
                    if measured_seconds > 0 else 0),
                "measured_payload_size_histogram": measured_payload_histogram,
                "observed_source_frame_rates_fps": [
                    {"start_offset_ns": left.get("window_offset_ns"),
                     "end_offset_ns": right.get("window_offset_ns"),
                     "frames_per_second": ((right["frames_received"] - left["frames_received"])
                        / ((right["sampled_at_monotonic_ns"] - left["sampled_at_monotonic_ns"])
                           / 1_000_000_000))}
                    for left, right in zip(
                        [item for item in telemetry if type(item.get("frames_received")) is int],
                        [item for item in telemetry if type(item.get("frames_received")) is int][1:],
                        strict=False,
                    )
                    if right["sampled_at_monotonic_ns"] > left["sampled_at_monotonic_ns"]],
                "observed_rates": observed_rates,
                "observed_rate_summary": _rate_summary(observed_rates),
                "final_frame_counts": {"received": handoff.frames_received,
                    "captured": capture.get("captured_frames", 0), "delivered": capture.get("delivered_frames", 0),
                    "indexed": status.indexed_frames, "rejected": handoff.frames_rejected},
                "clean_drain": not bool(handoff.queue_items or handoff.queue_bytes or capture.get("pending_frames")
                    or capture.get("terminal_error")),
                "capacity_qualification": "TEST GATE",
            }
            receipt = _write_receipt(run, body)
    except BaseException as exc:
        failure_reason = failure_reason or ("CANCELLED" if isinstance(exc, KeyboardInterrupt) else
            "DIAGNOSTIC_COMPONENT_FAILED_" + type(exc).__name__)
        if report_worker is not None:
            report_worker.close(timeout_s=1.0)
        if port is not None:
            try:
                lane = port.public_stream_source
                lane.request_pressure_stop()
                port.finish_public_capture(repository)
            except Exception:
                pass
        if receipt is None:
            receipt = _write_receipt(run, {"schema_version": 1, "status": "DIAGNOSTIC_FAILED",
                "reason": failure_reason, "profile_version": QUALIFICATION_PROFILE_VERSION,
                "profile_hash": profile_contract_hash(), "run_id": manifest.get("run_id"),
                "source_sha": manifest.get("source_sha"), "measured_window_seconds": 0,
                "due_poll_cycles": cycle_records, "report_slots": report_slots,
                "capacity_qualification": "TEST GATE"})
    finally:
        if report_worker is not None:
            report_worker.close(timeout_s=1.0)
        if port is not None:
            acquisition_worker = getattr(port, "_serviced_acquisition", None)
            if acquisition_worker is not None:
                if not acquisition_worker_closed:
                    try:
                        acquisition_worker.close(timeout_s=0)
                    except Exception:
                        pass
            else:
                try:
                    port.public_source.close()
                except Exception:
                    pass
            try:
                port.public_stream_source.close()
            except Exception:
                pass
        repository.close()
    assert receipt is not None
    return receipt


def _component_command(component: str, run: Path) -> list[str]:
    from ..product import resource_file

    if getattr(sys, "frozen", False):
        return [sys.executable, "--component", component, "--run-root", str(run)]
    return [sys.executable, str(resource_file("src/atlas_product_entry.py")),
            "--component", component, "--run-root", str(run)]


def _read_receipt(run: Path, *, expected_run_id: str, expected_source_sha: str,
                  expected_profile_hash: str) -> dict[str, Any] | None:
    path = run / RECEIPT_FILE
    if not path.is_file() or path.is_symlink() or path.stat().st_size > 4_000_000:
        return None
    body = json.loads(path.read_text(encoding="utf-8"))
    if (not isinstance(body, dict) or body.get("content_hash") != sha256_json(
            {key: value for key, value in body.items() if key != "content_hash"})
            or body.get("version") != "BINANCE_PUBLIC_DIAGNOSTIC_RECEIPT_V1"
            or body.get("run_id") != expected_run_id
            or body.get("source_sha") != expected_source_sha
            or body.get("profile_hash") != expected_profile_hash
            or body.get("capacity_certificate_issued") is not False
            or body.get("production_admission") is not False
            or body.get("capital_enabled") is not False
            or body.get("assisted_enabled") is not False
            or body.get("authority") != "ZERO"):
        return None
    return body


def commission_binance_diagnostic(selected_data_root: Path, *,
                                  stop_requested: Callable[[], bool] = lambda: False) -> tuple[dict[str, Any], int]:
    """Supervise one bounded qualification-only child on the selected device."""
    previous_umask = os.umask(0o077)
    try:
        return _commission_binance_diagnostic(selected_data_root, stop_requested=stop_requested)
    finally:
        os.umask(previous_umask)


def _commission_binance_diagnostic(selected_data_root: Path, *,
                                   stop_requested: Callable[[], bool]) -> tuple[dict[str, Any], int]:
    from ..product import build_identity, create_run

    source_sha: str | None = None
    selected_path_sha256 = hashlib.sha256(
        str(selected_data_root.expanduser()).encode()).hexdigest()
    host_path_probe_ref: str | None = None
    try:
        selected_path = validate_selected_path(selected_data_root)
        selected_path_sha256 = hashlib.sha256(str(selected_path).encode()).hexdigest()
        build = build_identity()
        source_sha = build["source_sha"]
        identity = sha256_json({"version": "BINANCE_QUALIFICATION_HOST_PATH_IDENTITY_V1",
            "source_sha": build["source_sha"], "profile_hash": profile_contract_hash(),
            "selected_path_sha256": selected_path_sha256})
        host_probe = run_host_path_probe(selected_path, identity_sha256=identity)
        host_path_probe_ref = host_probe["content_hash"]
        if not host_probe["allowed"]:
            raise ValueError("QUALIFICATION_SELECTED_DEVICE_PATH_PROBE_REJECTED")
        if stop_requested():
            raise ValueError("QUALIFICATION_CANCELLED_BEFORE_RUN_CREATION")
        qualification_home = selected_path / "s41-qualification-only"
        if qualification_home.is_symlink():
            raise ValueError("QUALIFICATION_HOME_SYMLINK_REJECTED")
        qualification_home.mkdir(mode=0o700, parents=True, exist_ok=True)
        home_stat = qualification_home.stat()
        if home_stat.st_uid != os.geteuid() or stat.S_IMODE(home_stat.st_mode) & 0o077:
            raise ValueError("QUALIFICATION_HOME_NOT_OWNER_ONLY")
        if (not qualification_home.resolve().is_relative_to(selected_path)
                or os.stat(qualification_home).st_dev != os.stat(selected_path).st_dev):
            raise ValueError("QUALIFICATION_HOME_DEVICE_OR_PATH_MISMATCH")
        qualification_root = qualification_home / uuid.uuid4().hex
        config = qualification_config(qualification_root)
        validate_qualification_config(config)
        run = create_run(qualification_root, config)
        manifest = json.loads((run / "run.json").read_text(encoding="utf-8"))
        supervisor_token = secrets.token_urlsafe(32)
        binding = _profile_binding(run, selected_path=selected_path, host_probe=host_probe,
            manifest=manifest, supervisor_token=supervisor_token)
        _write_immutable(run / PROFILE_BINDING_FILE, binding)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError, json.JSONDecodeError) as exc:
        body = {"schema_version": 1, "status": "DIAGNOSTIC_FAILED",
            "reason": "QUALIFICATION_SUPERVISOR_PREFLIGHT_" + type(exc).__name__,
            "profile_version": QUALIFICATION_PROFILE_VERSION, "profile_hash": profile_contract_hash(),
            "source_sha": source_sha, "selected_path_sha256": selected_path_sha256,
            "host_path_probe_ref": host_path_probe_ref,
            "measured_window_seconds": 0, "capacity_qualification": "TEST GATE"}
        preflight_receipt = {**body, "version": "BINANCE_PUBLIC_DIAGNOSTIC_RECEIPT_V1", "authority": "ZERO",
            "capacity_certificate_issued": False, "production_admission": False,
            "capital_enabled": False, "assisted_enabled": False, "economics": "NOT ESTIMABLE"}
        return {**preflight_receipt, "content_hash": sha256_json(preflight_receipt)}, 2

    environment = _sanitized_environment()
    environment["ATLAS_QUALIFICATION_PARENT_TOKEN"] = supervisor_token
    if not getattr(sys, "frozen", False):
        from ..product import resource_file
        environment["PYTHONPATH"] = str(resource_file("src"))
    try:
        if stop_requested():
            cancel_receipt = _write_receipt(run, {"schema_version": 1, "status": "DIAGNOSTIC_FAILED",
                "reason": "QUALIFICATION_CANCELLED_BEFORE_CHILD_START",
                "profile_version": QUALIFICATION_PROFILE_VERSION, "profile_hash": binding["profile_hash"],
                "run_id": manifest["run_id"], "source_sha": manifest["source_sha"],
                "measured_window_seconds": 0, "capacity_qualification": "TEST GATE"})
            return cancel_receipt, 2
        process = subprocess.Popen(_component_command("qualification-diagnostic", run), cwd=run,
            env=environment, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True)
        deadline = time.monotonic() + MAX_SUPERVISED_SECONDS
        while process.poll() is None and time.monotonic() < deadline and not stop_requested():
            time.sleep(0.1)
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)
        child_receipt = _read_receipt(run, expected_run_id=manifest["run_id"],
            expected_source_sha=manifest["source_sha"], expected_profile_hash=binding["profile_hash"])
        if child_receipt is None:
            child_receipt = _write_receipt(run, {"schema_version": 1, "status": "DIAGNOSTIC_FAILED",
                "reason": "QUALIFICATION_CHILD_TIMEOUT_OR_CRASH", "profile_version": QUALIFICATION_PROFILE_VERSION,
                "profile_hash": binding["profile_hash"], "run_id": manifest["run_id"],
                "source_sha": manifest["source_sha"], "measured_window_seconds": 0,
                "capacity_qualification": "TEST GATE"})
        return child_receipt, 0 if (process.returncode == 0
            and child_receipt.get("status") == "DIAGNOSTIC_COMPLETE") else 2
    except OSError as exc:
        child_failure_receipt = _write_receipt(run, {"schema_version": 1, "status": "DIAGNOSTIC_FAILED",
            "reason": "QUALIFICATION_CHILD_START_FAILED_" + type(exc).__name__,
            "profile_version": QUALIFICATION_PROFILE_VERSION, "profile_hash": binding["profile_hash"],
            "run_id": manifest["run_id"], "source_sha": manifest["source_sha"],
            "measured_window_seconds": 0, "capacity_qualification": "TEST GATE"})
        return child_failure_receipt, 2
