"""Offline G1 qualification from a source checkout. Never issues a host certificate.

Only deterministic transport and existing max-breadth fixtures are used. The
worker removes credentials, denies socket/DNS and subprocess launches, uses the
existing capture/receipt/sole Ops writer, and deletes temporary stores. Run this
as an unprivileged source-only process; the Python fence is tested isolation,
not an OS sandbox against malicious native extensions.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import math
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import asdict, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from atlas.v2._serialization import sha256_json  # noqa: E402

VERSION = "G1_OFFLINE_PUBLIC_OBSERVATION_V1"
FRAMES = 512
PROFILE = {
    "frames": FRAMES,
    "normal_fps": 160,
    "adverse_fps": 320,
    "adverse_controller_stall_ns": 250_000_000,
    "capture_batch_frames": 64,
    "queue_items": 512,
    "queue_bytes": 16_000_000,
    "broad_cadence_ns": 10_000_000_000,
    "native_deadline_ns": 5_000_000_000,
    "maximum_breadth_products": 4096,
    "breadth_cycles": 3,
    "source_grade": "SYNTHETIC_TRANSPORT_FIXTURE",
    "network": "DISABLED",
}


def distribution(values: list[int]) -> dict[str, Any]:
    if not values or any(type(x) is not int or x < 0 for x in values):
        raise ValueError("G1_INCOMPLETE_LATENCY_SAMPLES")
    ordered = sorted(values)

    def q(fraction):
        return ordered[math.ceil(len(values) * fraction) - 1]

    return {
        "count": len(values),
        "minimum_ns": ordered[0],
        "mean_ns": sum(values) // len(values),
        "p50_ns": q(0.5),
        "p95_ns": q(0.95),
        "p99_ns": q(0.99),
        "maximum_ns": ordered[-1],
        "samples_ns": values,
    }


def _fence() -> dict[str, int]:
    attempts = {"network_denied": 0, "subprocess_denied": 0}
    for key in list(os.environ):
        if any(word in key.upper() for word in ("KEY", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL")):
            os.environ.pop(key)

    def audit(event, _args):
        if event in {"socket.connect", "socket.getaddrinfo", "socket.gethostbyname", "socket.sendto"}:
            attempts["network_denied"] += 1
            raise PermissionError("G1_OFFLINE_NETWORK_DISABLED")
        if event in {"subprocess.Popen", "os.system", "os.exec", "os.posix_spawn"}:
            attempts["subprocess_denied"] += 1
            raise PermissionError("G1_CHILD_PROCESS_DISABLED")

    sys.addaudithook(audit)
    return attempts


def source_hashes() -> dict[str, str]:
    paths = [Path(__file__), ROOT / "tests/v2/test_session041_scale_review.py"]
    paths.extend(sorted((ROOT / "src/atlas").rglob("*.py")))
    paths.extend([ROOT / "pyproject.toml", ROOT / "requirements-lock.txt"])
    return {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}


class ResourceSampler:
    """Sample all process descendants, memory/CPU/disk, and scheduler wake delay."""

    def __init__(self, root: Path):
        self.root = root
        self.samples: list[dict[str, int]] = []
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _sample(self, due: int):
        pids = [os.getpid()]
        rss = pss = private = peak = 0
        for pid in pids:
            proc = Path("/proc") / str(pid)
            # This worker may not launch children; count unexpected descendants.
            for task in (proc / "task").iterdir():
                children = task / "children"
                if children.exists():
                    pids.extend(x for x in map(int, children.read_text().split()) if x not in pids)
            fields = {}
            for line in (proc / "status").read_text().splitlines():
                name, _, raw = line.partition(":")
                if name in {"VmRSS", "VmHWM"}:
                    fields[name] = int(raw.split()[0]) * 1024
            rss += fields["VmRSS"]
            peak += fields["VmHWM"]
            for line in (proc / "smaps_rollup").read_text().splitlines():
                name, _, raw = line.partition(":")
                if name in {"Pss", "Private_Clean", "Private_Dirty"}:
                    value = int(raw.split()[0]) * 1024
                    pss += value if name == "Pss" else 0
                    private += value if name.startswith("Private_") else 0
        from atlas.v2.runtime.binance_qualification import _host_resource_facts

        host = _host_resource_facts()
        cgroup = host["cgroup_memory"]
        available = host["host_memory_available_bytes"]
        if cgroup["maximum_bytes"] is not None:
            available = min(available, max(0, cgroup["maximum_bytes"] - cgroup["current_bytes"]))
        now = time.monotonic_ns()
        files = []
        for path in self.root.rglob("*"):
            try:
                if path.is_file():
                    files.append((path.name, path.stat().st_size))
            except FileNotFoundError:
                continue
        self.samples.append(
            {
                "offset_ns": now - self.started,
                "scheduler_delay_ns": max(0, now - due),
                "process_tree_count": len(pids),
                "rss_bytes": rss,
                "pss_bytes": pss,
                "private_bytes": private,
                "process_tree_hwm_bytes": peak,
                "available_memory_bytes": available,
                "cpu_ns": time.process_time_ns(),
                "wal_bytes": sum(size for name, size in files if name.endswith("-wal")),
                "temporary_bytes": sum(size for _name, size in files),
                "free_disk_bytes": shutil.disk_usage(self.root).free,
            }
        )

    def _run(self):
        due = self.started
        try:
            while not self.stopped.is_set():
                self._sample(due)
                due += 50_000_000
                self.stopped.wait(max(0, due - time.monotonic_ns()) / 1e9)
        except Exception as error:
            self.error = type(error).__name__

    def __enter__(self):
        self.error = None
        self.started = time.monotonic_ns()
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stopped.set()
        self.thread.join(timeout=2)
        self._sample(time.monotonic_ns())

    def result(self):
        if self.error or len(self.samples) < 2:
            raise ValueError("G1_RESOURCE_SAMPLING_INCOMPLETE")
        return {
            "sample_interval_ns": 50_000_000,
            "sample_count": len(self.samples),
            "peak_rss_bytes": max(x["rss_bytes"] for x in self.samples),
            "process_tree_hwm_bytes": max(x["process_tree_hwm_bytes"] for x in self.samples),
            "peak_pss_bytes": max(x["pss_bytes"] for x in self.samples),
            "peak_private_bytes": max(x["private_bytes"] for x in self.samples),
            "minimum_available_memory_bytes": min(x["available_memory_bytes"] for x in self.samples),
            "peak_wal_bytes": max(x["wal_bytes"] for x in self.samples),
            "peak_temporary_bytes": max(x["temporary_bytes"] for x in self.samples),
            "cpu_seconds": (self.samples[-1]["cpu_ns"] - self.samples[0]["cpu_ns"]) / 1e9,
            "scheduler_delay": distribution([x["scheduler_delay_ns"] for x in self.samples]),
            "samples": self.samples,
        }


def capture_probe(root: Path, profile: str) -> dict[str, Any]:
    from atlas.v2.data.durable_public_capture import DurablePublicCaptureV1
    from atlas.v2.data.public_archive_extents import read_extent
    from atlas.v2.data.public_microstructure_ws import (
        BoundedPublicFrameHandoffV2,
        CapturedPublicFrameV2,
        bybit_btc_eth_linear_topics,
    )
    from atlas.v2.instruments import VenueV2
    from atlas.v2.memory.repository import OpsRepository

    class FixtureSource:
        venue = VenueV2.BYBIT
        topics = bybit_btc_eth_linear_topics()

        def __init__(self):
            self.handoff = BoundedPublicFrameHandoffV2(venue=self.venue, topics=self.topics)

        def start(self):
            self.handoff.observe_connected(time.time_ns())

        def close(self):
            self.handoff.close(time.time_ns())

        def drain(self, *, max_items):
            return self.handoff.drain(max_items=max_items)

        def status(self):
            return SimpleNamespace(
                state="RUNNING",
                attempt_count=1,
                reconnect_count=0,
                last_error_code=None,
                handoff=self.handoff.snapshot(),
            )

    root.mkdir()
    source = FixtureSource()
    capture = DurablePublicCaptureV1(source, capture_payload_metrics=True)
    capture.configure_capture(root)
    expected = []
    producer_errors = []
    fps = PROFILE["normal_fps"] if profile == "normal" else PROFILE["adverse_fps"]

    def produce():
        started = time.monotonic_ns()
        try:
            for sequence in range(FRAMES):
                due = started + sequence * 1_000_000_000 // fps
                time.sleep(max(0, due - time.monotonic_ns()) / 1e9)
                received = time.time_ns()
                raw = json.dumps(
                    {
                        "topic": "publicTrade.BTCUSDT",
                        "ts": received // 1_000_000,
                        "data": [
                            {
                                "T": received // 1_000_000,
                                "s": "BTCUSDT",
                                "S": "Buy",
                                "v": "0.01",
                                "p": "100.5",
                                "i": f"g1-{sequence}",
                            }
                        ],
                    },
                    separators=(",", ":"),
                ).encode()
                frame = CapturedPublicFrameV2(
                    VenueV2.BYBIT,
                    "BYBIT_PUBLIC_WS_BTC_ETH_LINEAR_V1",
                    "publicTrade.BTCUSDT",
                    raw,
                    hashlib.sha256(raw).hexdigest(),
                    received,
                    received,
                    1,
                )
                expected.append(frame)
                if not source.handoff.offer(frame):
                    raise RuntimeError("G1_FIXTURE_QUEUE_REJECTED")
        except Exception as error:
            producer_errors.append(type(error).__name__)

    latencies: dict[str, list[int]] = {
        "receipt_to_controller_ready": [],
        "adoption": [],
        "readback": [],
        "sqlite_commit": [],
        "archive_fsync": [],
        "receipt_fsync": [],
    }
    descriptors = []
    started = time.monotonic_ns()
    capture.start()
    producer = threading.Thread(target=produce, daemon=True)
    producer.start()
    try:
        with OpsRepository(root / "ops.sqlite") as repository:
            while producer.is_alive() or capture.status().pending_frames or source.status().handoff.queue_items:
                if time.monotonic_ns() - started > 20_000_000_000 or producer_errors:
                    raise RuntimeError("G1_CAPTURE_WORKLOAD_INCOMPLETE")
                batch = capture.drain_sealed_transport()
                if batch is None:
                    if capture.status().state == "FAILED":
                        raise RuntimeError("G1_CAPTURE_TERMINAL_FAILURE")
                    time.sleep(0.001)
                    continue
                if profile == "adverse" and not descriptors:
                    time.sleep(PROFILE["adverse_controller_stall_ns"] / 1e9)
                before = time.monotonic_ns()
                frames = batch.adopt(repository)
                latencies["adoption"].append(time.monotonic_ns() - before)
                latencies["sqlite_commit"].append(repository.persistence_metrics()["commit_duration_ns"])
                now = time.time_ns()
                latencies["receipt_to_controller_ready"].extend(now - frame.received_at_ns for frame in frames)
                before = time.monotonic_ns()
                assert read_extent(repository, batch.extent.artifact_ref).num_rows == len(frames)
                latencies["readback"].append(time.monotonic_ns() - before)
                descriptors.append(batch)
            producer.join(timeout=1)
            capture.close()
            status = capture.status()
            assert status.capture["captured_frames"] == status.capture["delivered_frames"] == FRAMES
            assert not status.handoff.frames_rejected and not producer_errors
            assert repository._connection.execute("PRAGMA synchronous").fetchone()[0] == 2
            assert repository._connection.execute("PRAGMA quick_check").fetchone()[0] == "ok"
            # Actual last-batch archive telemetry; counts explicitly show it is
            # one sample, not a distribution over every archive operation.
            latencies["archive_fsync"].append(capture._writer.metrics["fsync_ns"])
        # Each reopen verifies every immutable raw extent and FIFO identity.
        for _ in range(2):
            with OpsRepository(root / "ops.sqlite", read_only=True) as reader:
                restored = [frame for batch in descriptors for frame in batch.read_unpublished(root)]
                assert restored == expected
                assert all(reader.get_artifact(batch.batch.artifact_ref) for batch in descriptors)
        # Receipt fsync timing is collected by the full preflight separately.
        del latencies["receipt_fsync"]
        elapsed = time.monotonic_ns() - started
        raw_bytes = sum(len(frame.raw_payload_bytes) for frame in expected)
        return {
            "profile": profile,
            "frames": FRAMES,
            "raw_bytes": raw_bytes,
            "decoded_transport_bytes": sum(x.extent.metadata["extent"]["decoded_bytes"] for x in descriptors),
            "archive_bytes": sum(p.stat().st_size for p in (root / "ops-public-extents").glob("*.arrow")),
            "receipt_bytes": capture._journal.bytes_written,
            "elapsed_ns": elapsed,
            "capture_frames_per_second": FRAMES / (elapsed / 1e9),
            "throughput_scope": "FULL_FIXTURE_STARTUP_PRODUCER_DRAIN_REOPEN",
            "latencies": {name: distribution(values) for name, values in latencies.items()},
            "population": {
                "offered": len(expected),
                "sealed": status.capture["captured_frames"],
                "adopted": status.capture["delivered_frames"],
                "recovered_readback": len(restored),
                "rejected": status.handoff.frames_rejected,
                "lost": 0,
                "duplicate_transport": 0,
                "unavailable": 0,
            },
            "handoff": asdict(status.handoff),
            "capture": status.capture,
            "restart_readback_passes": 2,
            "payload_population_hash": sha256_json([f.raw_payload_hash for f in expected]),
        }
    finally:
        capture.close()
        producer.join(timeout=2)


def breadth_probe(root: Path) -> dict[str, Any]:
    # Reuse the already reviewed production-owner measurement fixture rather
    # than write another market population/collector/persistence architecture.
    sys.path.insert(0, str(ROOT / "tests"))
    from v2.test_session041_scale_review import measure_bounded_cycles

    started = time.monotonic_ns()
    result = measure_bounded_cycles(root, per_venue=2048, cycles=3)
    samples = result["samples"]
    previous_archive = 0
    for sample in samples:
        archive = sample["archive_cumulative_bytes"]
        sample["persisted_cycle_growth_bytes"] = sample["db_growth_bytes"] + archive - previous_archive
        previous_archive = archive
    result["maximum_cycle_bytes"] = max(x["persisted_cycle_growth_bytes"] for x in samples)
    result["elapsed_ns"] = time.monotonic_ns() - started
    result["measured_scope"] = "DB_LOGICAL_GROWTH_PLUS_PARQUET_FILE_GROWTH_NOT_PHYSICAL_WRITE_IO"
    result["initial_product_metadata_bytes"] = result["db_baseline_bytes"]
    result["capacity_qualified"] = False
    result["sample_count"] = 3
    result["adoption_latency"] = distribution([round(x["adoption_seconds"] * 1e9) for x in samples])
    result["workset_latency"] = distribution([round(x["workset_publication_seconds"] * 1e9) for x in samples])
    from atlas.v2.runtime.storage_preflight import assess_broad_storage_capacity_v2

    result["projection"] = assess_broad_storage_capacity_v2(
        identity_sha256=sha256_json(PROFILE),
        selected_path="TEMPORARY_G1_PROBE",
        observed_at_ns=time.time_ns(),
        enabled_venues=("BYBIT", "BINANCE"),
        observed_free_bytes=shutil.disk_usage(root).free,
        owner_free_disk_reserve_bytes=0,
        measured_max_cycle_bytes=result["maximum_cycle_bytes"],
    ).as_dict()
    return result


def validate_result(body: dict[str, Any]) -> None:
    """Reject incomplete/inconsistent records. A rehash is not trusted provenance.

    This validator cannot authenticate hostile user-supplied timing claims;
    only the guarded local worker creates the execution result. No import of a
    result file, even a consistent one, grants capacity/host/source authority.
    """
    if (
        body.get("version") != VERSION
        or body.get("profile") != PROFILE
        or body.get("profile_hash") != sha256_json(PROFILE)
        or body.get("capacity_qualified") is not False
        or body.get("actual_host_qualified") is not False
        or body.get("network") != "DISABLED"
        or body.get("authority") != "ZERO"
        or body.get("status") != "TEST GATE"
        or body.get("content_hash") != sha256_json({k: v for k, v in body.items() if k != "content_hash"})
    ):
        raise ValueError("G1_RECORD_BINDING_INVALID")
    if len(body["capture"]) != 2:
        raise ValueError("G1_CAPTURE_PROFILES_INCOMPLETE")
    if body["maximum_breadth_requested"] != (body["breadth"] is not None):
        raise ValueError("G1_REQUESTED_BREADTH_MISSING")
    for metric in body["storage_latencies"].values():
        if metric != distribution(metric["samples_ns"]):
            raise ValueError("G1_STORAGE_LATENCY_INVALID")
    if not body["storage_latencies"]:
        raise ValueError("G1_STORAGE_LATENCY_MISSING")
    for result in body["capture"]:
        if result["population"] != {
            "offered": FRAMES,
            "sealed": FRAMES,
            "adopted": FRAMES,
            "recovered_readback": FRAMES,
            "rejected": 0,
            "lost": 0,
            "duplicate_transport": 0,
            "unavailable": 0,
        }:
            raise ValueError("G1_CAPTURE_POPULATION_INCOMPLETE")
        if result["restart_readback_passes"] != 2:
            raise ValueError("G1_RESTART_INCOMPLETE")
        for metric in result["latencies"].values():
            if metric != distribution(metric["samples_ns"]):
                raise ValueError("G1_LATENCY_SUMMARY_INVALID")
    if {r["profile"] for r in body["capture"]} != {"normal", "adverse"}:
        raise ValueError("G1_CAPTURE_PROFILES_INCOMPLETE")
    resources = body["resources"]
    if resources["sample_count"] < 2 or len(resources["samples"]) != resources["sample_count"]:
        raise ValueError("G1_RESOURCES_INCOMPLETE")
    required_resources = {
        "offset_ns",
        "scheduler_delay_ns",
        "process_tree_count",
        "rss_bytes",
        "pss_bytes",
        "private_bytes",
        "process_tree_hwm_bytes",
        "available_memory_bytes",
        "cpu_ns",
        "wal_bytes",
        "temporary_bytes",
        "free_disk_bytes",
    }
    if any(
        set(sample) != required_resources or any(type(v) is not int or v < 0 for v in sample.values())
        for sample in resources["samples"]
    ):
        raise ValueError("G1_RESOURCES_INCOMPLETE")
    for summary, field in [
        ("peak_rss_bytes", "rss_bytes"),
        ("process_tree_hwm_bytes", "process_tree_hwm_bytes"),
        ("peak_pss_bytes", "pss_bytes"),
        ("peak_private_bytes", "private_bytes"),
        ("peak_wal_bytes", "wal_bytes"),
        ("peak_temporary_bytes", "temporary_bytes"),
    ]:
        if resources[summary] != max(x[field] for x in resources["samples"]):
            raise ValueError("G1_RESOURCE_SUMMARY_INVALID")
    if body["breadth"] is not None:
        broad = body["breadth"]
        integrity = broad["integrity"]
        if (
            broad["population"] != 4096
            or broad["cycles"] != 3
            or broad["sample_count"] != 3
            or len(broad["samples"]) != 3
            or integrity["exact_raw_rows_verified"] != 24576
            or integrity["exact_index_metadata_verified_after_restart"] != 24576
            or integrity["cutoff_worksets_verified_after_restart"] != 3
            or any(x["record_count"] != 8192 for x in broad["samples"])
            or broad["maximum_cycle_bytes"] != max(x["persisted_cycle_growth_bytes"] for x in broad["samples"])
        ):
            raise ValueError("G1_BREADTH_MEASUREMENT_INCOMPLETE")
    if body["isolation"] != {"network_denied": 0, "subprocess_denied": 0}:
        raise ValueError("G1_UNEXPECTED_SIDE_EFFECT_ATTEMPT")


def worker(storage_path: Path, *, maximum_breadth: bool, environment: str) -> dict[str, Any]:
    if not sys.platform.startswith("linux"):
        raise ValueError("G1_RESOURCE_HARNESS_REQUIRES_LINUX")
    isolation = _fence()
    from atlas.v2.runtime.binance_qualification import validate_selected_path
    from atlas.v2.runtime.storage_preflight import (
        StoragePreflightHooksV1,
        qualify_storage_path,
    )

    storage_path = validate_selected_path(storage_path)
    storage_path.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".atlas-g1-", dir=storage_path) as temporary:
        root = Path(temporary)
        with ResourceSampler(root) as resources:
            host = qualify_storage_path(root, identity_sha256=sha256_json(PROFILE)).as_dict()
            # The same service workload can execute despite environment gates.
            # Explicit hooks never qualify capacity, dependencies, or HOST_PATH.
            diagnostic = qualify_storage_path(
                root,
                identity_sha256=sha256_json(PROFILE),
                hooks=replace(
                    StoragePreflightHooksV1(),
                    free_bytes=lambda _: 10**12,
                    dependencies=lambda: {"scope": "STORAGE_ONLY_NOT_DESKTOP_RUNTIME"},
                ),
            ).as_dict()
            capture = [capture_probe(root / name, name) for name in ("normal", "adverse")]
            broad = breadth_probe(root / "breadth") if maximum_breadth else None
        result = {
            "version": VERSION,
            "profile": PROFILE,
            "profile_hash": sha256_json(PROFILE),
            "environment": environment,
            "host": {
                "platform": platform.system(),
                "machine": platform.machine(),
                "python": platform.python_version(),
                "cpu_count": os.cpu_count(),
            },
            "dependencies": {name: importlib.metadata.version(name) for name in ("pyarrow", "duckdb", "websockets")},
            "source_hashes": source_hashes(),
            "host_preflight": host,
            "diagnostic_preflight": diagnostic,
            "diagnostic_overrides": [
                "free_bytes=1000000000000 (injected, not observed)",
                "storage-only dependencies; full runtime smoke NOT qualified",
            ],
            "capture": capture,
            "breadth": broad,
            "maximum_breadth_requested": maximum_breadth,
            "storage_latencies": {
                name: distribution(values) for name, values in diagnostic["facts"]["latency_samples_ns"].items()
            },
            "resources": resources.result(),
            "network": "DISABLED",
            "isolation": isolation,
            "authority": "ZERO",
            "capacity_qualified": False,
            "actual_host_qualified": False,
            "native_timing": {
                "status": "UNVERIFIED",
                "deadline_ns": 5_000_000_000,
                "missing_segments": [
                    "maintained context",
                    "native features",
                    "strategy decision-ready",
                    "genuine P1 provenance",
                    "complete typed Brief",
                    "Agent B/provider",
                ],
                "capture_latency_is_not_native_decision_latency": True,
            },
            "status": "TEST GATE",
        }
    # Do not publish ephemeral host paths or a temporary directory identity.
    for record in (result["host_preflight"], result["diagnostic_preflight"]):
        record["selected_path"] = "TEMPORARY_G1_PROBE"
        record["content_hash"] = sha256_json({k: v for k, v in record.items() if k != "content_hash"})
    result["temporary_probe_removed"] = not root.exists()
    result["content_hash"] = sha256_json(result)
    validate_result(result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--storage-path", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--maximum-breadth", action="store_true")
    parser.add_argument(
        "--environment",
        choices=("CLOUD DEVELOPMENT MEASUREMENTS", "TARGET HOST MEASUREMENTS"),
        default="CLOUD DEVELOPMENT MEASUREMENTS",
    )
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output must be a new file")
    if args.worker:
        try:
            result = worker(args.storage_path, maximum_breadth=args.maximum_breadth, environment=args.environment)
        except Exception as error:
            result = {
                "version": VERSION,
                "status": "TEST GATE",
                "failure_type": type(error).__name__,
                "capacity_qualified": False,
                "actual_host_qualified": False,
                "authority": "ZERO",
            }
        args.output.write_text(json.dumps(result, indent=2) + "\n")
        return 0 if "content_hash" in result else 2
    # Parent permits only this exact source-only child, no external command.
    environment = {
        k: v
        for k, v in os.environ.items()
        if not any(w in k.upper() for w in ("KEY", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL"))
    }
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--storage-path",
        str(args.storage_path),
        "--output",
        str(args.output),
        "--environment",
        args.environment,
    ]
    if args.maximum_breadth:
        command.append("--maximum-breadth")
    try:
        child = subprocess.run(command, env=environment, timeout=240, check=False, capture_output=True)
    except subprocess.TimeoutExpired:
        print("TEST GATE: bounded worker exceeded 240 seconds; no qualification issued")
        return 2
    if child.returncode:
        print("TEST GATE: worker failed; inspect sanitized output")
        return 2
    validate_result(json.loads(args.output.read_text()))
    print("TEST GATE: offline measurements complete; actual host/source/native timing unqualified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
