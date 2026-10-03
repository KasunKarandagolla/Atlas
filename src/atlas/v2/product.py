"""Non-capital installed run lifecycle, configuration and diagnostic surface.

The ops supervisor remains the only research writer. The launcher never opens
a writable research database or imports the capital runtime.
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import heapq
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast

from atlas import __version__

from ._serialization import canonical_json, sha256_json, sha256_ref
from .resources import resource_file

RUN_PROFILE = "PUBLIC_BYBIT_BASELINE_V1"
MAX_CONFIG_BYTES = 16_384


def development_gate() -> dict[str, Any]:
    path = resource_file("docs/v2/ATLAS_FINAL_DEVELOPMENT_CLOSURE_V1.json")
    if not path.is_file():
        return {"verdict": "DEVELOPMENT NOT READY", "development_complete": False,
                "status": "UNVERIFIED"}
    gate = _read_json(path, maximum_bytes=131_072)
    return {"verdict": gate["verdict"], "development_complete": gate["development_complete"],
            "status": gate["status"]}


def _read_json(path: Path, *, maximum_bytes: int = MAX_CONFIG_BYTES) -> dict[str, Any]:
    with path.open("rb") as handle:
        raw = handle.read(maximum_bytes + 1)
    if len(raw) > maximum_bytes:
        raise ValueError("product configuration exceeds its bound")
    body = json.loads(raw)
    if not isinstance(body, dict):
        raise ValueError("product configuration must be an object")
    return body


def _publish(path: Path, body: dict[str, Any], *, immutable: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (canonical_json(body) + "\n").encode()
    if immutable:
        with path.open("xb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        return
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def build_identity() -> dict[str, Any]:
    path = (Path(sys.executable).resolve().parent / "build-manifest.json"
            if getattr(sys, "frozen", False) else resource_file("build-manifest.json"))
    if not path.exists():
        if getattr(sys, "frozen", False):
            raise ValueError("installed build manifest is missing")
        # A development invocation also needs an exact source identity for
        # causal exports. This lookup is never used by the installed product.
        source = subprocess.check_output(["git", "rev-parse", "HEAD"],
                                         cwd=resource_file("."), timeout=5, text=True).strip()
        return {"version": __version__, "source_sha": source, "runtime_lock_sha256": None,
                "development_build": True}
    manifest = _read_json(path, maximum_bytes=8_000_000)
    manifest_source = manifest.get("source_sha")
    if not isinstance(manifest_source, str) or len(manifest_source) != 40 or any(c not in "0123456789abcdef" for c in manifest_source):
        raise ValueError("installed source identity is invalid")
    if manifest.get("capital_enabled") is not False or manifest.get("assisted_enabled") is not False:
        raise ValueError("installed authority manifest is invalid")
    # The payload inventory is verified by the package harness. Immutable run
    # identity binds its tree hash without duplicating the full inventory.
    return {key: manifest[key] for key in ("schema_version", "source_sha", "version",
            "runtime_lock_sha256", "dependency_locks", "payload_tree_sha256",
            "capital_enabled", "assisted_enabled", "python_version", "runtime_platform")}


@dataclass(frozen=True)
class ResearchRunConfigV1:
    profile: str = RUN_PROFILE
    report_interval_seconds: int = 21_600
    minimum_free_disk_bytes: int = 1_073_741_824
    provider_profile: str = "DISABLED"

    def __post_init__(self) -> None:
        if self.profile != RUN_PROFILE:
            raise ValueError("unregistered run profile")
        if (type(self.report_interval_seconds) is not int
                or not 60 <= self.report_interval_seconds <= 86_400):
            raise ValueError("report interval must be between one minute and one day")
        if type(self.minimum_free_disk_bytes) is not int or self.minimum_free_disk_bytes < 268_435_456:
            raise ValueError("research run requires a declared disk reserve")
        # Provider activation is deliberately explicit at the integrated broker
        # boundary; a stored key alone must never authorize paid inference.
        if self.provider_profile not in {"DISABLED", "deepseek-v41-action-critic-v1"}:
            raise ValueError("unregistered provider dispatch profile")

    @property
    def content_hash(self) -> str:
        return sha256_json({"schema_version": 1, **asdict(self)})


def default_data_root() -> Path:
    if os.name == "nt":
        base = os.environ.get("LOCALAPPDATA")
        if not base:
            raise RuntimeError("Windows application-data location is unavailable")
        return Path(base) / "Atlas"
    return Path.home() / ".local" / "share" / "atlas"


def create_run(data_root: Path, config: ResearchRunConfigV1) -> Path:
    root = data_root.expanduser().resolve()
    if getattr(sys, "frozen", False) and root.is_relative_to(Path(sys.executable).parent.resolve()):
        raise ValueError("research data must be outside the installation directory")
    run_id = uuid.uuid4().hex
    run = root / "runs" / run_id
    run.mkdir(parents=True, exist_ok=False)
    for name in ("reports", "epochs", "launches"):
        (run / name).mkdir()
    build = build_identity()
    body = {"schema_version": 1, "run_id": run_id, "started_at_ns": time.time_ns(),
            "source_sha": build["source_sha"], "build_identity_hash": sha256_json(build),
            "configuration": asdict(config), "config_hash": config.content_hash,
            "provider_configuration": provider_configuration(config.provider_profile),
            "research_model_configuration": research_model_configuration(build),
            "capital_enabled": False, "assisted_enabled": False, "holdout_access": False}
    body["content_hash"] = sha256_json(body)
    _publish(run / "run.json", body, immutable=True)
    return run


def load_run(run: Path, *, require_current_build: bool = True) -> dict[str, Any]:
    from .models.protocol import ModelManifestV2

    body = _read_json(run / "run.json")
    fields = {"schema_version", "run_id", "started_at_ns", "source_sha", "build_identity_hash",
              "configuration", "config_hash", "provider_configuration", "research_model_configuration",
              "capital_enabled", "assisted_enabled", "holdout_access", "content_hash"}
    if set(body) != fields or body["schema_version"] != 1:
        raise ValueError("unknown run manifest contract")
    expected = sha256_json({k: v for k, v in body.items() if k != "content_hash"})
    config = ResearchRunConfigV1(**body["configuration"])
    if (body["content_hash"] != expected or body["config_hash"] != config.content_hash
            or body["capital_enabled"] is not False or body["assisted_enabled"] is not False
            or body["holdout_access"] is not False or body["run_id"] != run.name
            or type(body["started_at_ns"]) is not int or body["started_at_ns"] <= 0):
        raise ValueError("run identity/configuration drift")
    source = body["source_sha"]
    if (not isinstance(source, str) or len(source) != 40
            or any(c not in "0123456789abcdef" for c in source)):
        raise ValueError("run source identity is invalid")
    sha256_ref(body["build_identity_hash"], field="build_identity_hash")
    if body["provider_configuration"] != provider_configuration(config.provider_profile):
        raise ValueError("registered provider configuration drift")
    model = ModelManifestV2.from_dict(body["research_model_configuration"]["manifest"])
    original = {"source_sha": source, "runtime_lock_sha256": model.environment_lock_hash}
    if body["research_model_configuration"] != research_model_configuration(original):
        raise ValueError("registered research model configuration drift")
    if require_current_build:
        current = build_identity()
        if body["build_identity_hash"] != sha256_json(current) or source != current["source_sha"]:
            raise ValueError("resume requires the exact original build; create a new run after upgrade")
        if body["research_model_configuration"] != research_model_configuration(current):
            raise ValueError("registered research model configuration drift")
    return body


def research_model_configuration(build: dict[str, Any]) -> dict[str, Any]:
    from .models.research_routing import statistical_research_route_v1

    environment_lock = build.get("runtime_lock_sha256")
    if environment_lock is None:
        environment_lock = hashlib.sha256(resource_file("requirements-lock.txt").read_bytes()).hexdigest()
    route, manifest = statistical_research_route_v1(source_sha=build["source_sha"],
        environment_lock_hash=environment_lock)
    return {"route": route.to_dict(), "manifest": manifest.to_dict(), "authority": "ZERO"}


class _ResearchShadows:
    """Ordered observation callbacks on the supervisor-owned writer."""

    def __init__(self, *callbacks: Any) -> None:
        self.callbacks = callbacks

    def __call__(self, receipt: Any, receipt_ref: str, repository: Any) -> None:
        for callback in self.callbacks:
            callback(receipt, receipt_ref, repository)

    def drain_completed(self, *, max_items: int, repository: Any) -> None:
        for callback in self.callbacks:
            drain = getattr(callback, "drain_completed", None)
            if callable(drain):
                drain(max_items=max_items, repository=repository)


def provider_configuration(profile: str) -> dict[str, Any] | None:
    if profile == "DISABLED":
        return None
    if profile != "deepseek-v41-action-critic-v1":
        raise ValueError("unregistered provider configuration")
    from .agent_intelligence.budget import DeepSeekPriceScheduleV1
    from .agent_intelligence.profile import deepseek_v41_flash_action_critic_profile

    schedule = DeepSeekPriceScheduleV1.load(resource_file(
        "configs/agent_intelligence/provider_pricing_deepseek_v41_flash_v1.json"))
    return deepseek_v41_flash_action_critic_profile(price_schedule=schedule,
        agent_lock_path=resource_file("requirements-agent-lock.txt")).to_dict()


class WindowsSecretStore:
    """Current-user DPAPI; no plaintext or cross-platform insecure fallback."""

    def __init__(self, root: Path) -> None:
        self.root = root

    @staticmethod
    def _crypt(raw: bytes, *, decrypt: bool) -> bytes:
        if os.name != "nt":
            raise RuntimeError("Windows protected secrets backend unavailable")
        from ctypes import wintypes

        class Blob(ctypes.Structure):
            _fields_ = [("size", wintypes.DWORD), ("data", ctypes.POINTER(ctypes.c_ubyte))]

        buffer = ctypes.create_string_buffer(raw)
        incoming = Blob(len(raw), ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ubyte)))
        outgoing = Blob()
        library = cast(Any, ctypes).WinDLL("crypt32", use_last_error=True)
        kernel = cast(Any, ctypes).WinDLL("kernel32", use_last_error=True)
        operation = library.CryptUnprotectData if decrypt else library.CryptProtectData
        operation.argtypes = [ctypes.POINTER(Blob), ctypes.c_void_p, ctypes.c_void_p,
                              ctypes.c_void_p, ctypes.c_void_p, wintypes.DWORD, ctypes.POINTER(Blob)]
        operation.restype = wintypes.BOOL
        kernel.LocalFree.argtypes = [ctypes.c_void_p]
        kernel.LocalFree.restype = ctypes.c_void_p
        if not operation(ctypes.byref(incoming), None, None, None, None, 1, ctypes.byref(outgoing)):
            raise RuntimeError("Windows protected secret operation failed")
        try:
            return ctypes.string_at(outgoing.data, outgoing.size)
        finally:
            kernel.LocalFree(outgoing.data)

    def put_provider_key(self, profile: str, key: str) -> None:
        if profile != "deepseek-v41-action-critic-v1" or not 1 <= len(key) <= 4096:
            raise ValueError("unsupported provider secret profile")
        encrypted = self._crypt(key.encode(), decrypt=False)
        self.root.mkdir(parents=True, exist_ok=True)
        target = self.root / "deepseek-action-critic.dpapi"
        temporary = target.with_name(target.name + "." + uuid.uuid4().hex + ".tmp")
        try:
            with temporary.open("xb") as handle:
                handle.write(encrypted)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)

    def get_provider_key(self, profile: str) -> str:
        """Called only by the isolated credential broker component."""
        if profile != "deepseek-v41-action-critic-v1":
            raise ValueError("unsupported provider secret profile")
        with (self.root / "deepseek-action-critic.dpapi").open("rb") as handle:
            cipher = handle.read(65_537)
        if not cipher or len(cipher) > 65_536:
            raise ValueError("invalid protected provider key size")
        key = self._crypt(cipher, decrypt=True).decode("utf-8")
        if not 1 <= len(key) <= 4096:
            raise ValueError("invalid protected provider key size")
        return key


def export_run(run: Path) -> Any:
    from .science.tuning_export import TuningRunIdentityV1, export_tuning_snapshot

    manifest = load_run(run, require_current_build=False)
    identity = TuningRunIdentityV1(manifest["run_id"], manifest["config_hash"],
                                  manifest["source_sha"], manifest["started_at_ns"])
    cutoff = time.time_ns()
    # A fixed cutoff across bounded pages prevents report draining from
    # following an ever-growing live stream. Remaining work stays explicit.
    for _ in range(8):
        result = export_tuning_snapshot(run / "ops.sqlite", run / "reports", identity, cutoff_ns=cutoff)
        if not result["has_more"] or result["blocked_future_evidence"]:
            return result
    return result


def runtime_dependency_smoke() -> dict[str, str]:
    """Load the installed native dependencies without network or capital."""
    import duckdb
    import lightgbm
    import pyarrow
    from PySide6.QtCore import qVersion
    from PySide6.QtWidgets import QApplication

    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    application = QApplication.instance() or QApplication(["ATLAS offline fixture"])
    with duckdb.connect(":memory:") as connection:
        if connection.execute("SELECT 1").fetchone() != (1,):
            raise RuntimeError("analytics runtime failed its deterministic fixture")
    if pyarrow.table({"fixture": [1]}).num_rows != 1 or application is None:
        raise RuntimeError("desktop/columnar runtime fixture failed")
    return {"qt": qVersion(), "pyarrow": str(getattr(pyarrow, "__version__", "unknown")),
            "duckdb": str(getattr(duckdb, "__version__", "unknown")),
            "lightgbm": str(getattr(lightgbm, "__version__", "unknown"))}


def _component_command(component: str, run: Path) -> list[str]:
    if getattr(sys, "frozen", False):
        return [sys.executable, "--component", component, "--run-root", str(run)]
    return [sys.executable, str(resource_file("src/atlas_product_entry.py")),
            "--component", component, "--run-root", str(run)]


def launch_run(run: Path) -> subprocess.Popen[bytes]:
    load_run(run)
    # No API key or capability is carried on a command line or inherited from
    # ambient developer environments by this public-only process.
    environment = {k: v for k, v in os.environ.items()
                   if not any(word in k.upper() for word in ("KEY", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL"))}
    if not getattr(sys, "frozen", False):
        environment["PYTHONPATH"] = str(resource_file("src"))
    options: dict[str, Any] = {"start_new_session": True} if os.name != "nt" else {
        "creationflags": cast(Any, subprocess).CREATE_NEW_PROCESS_GROUP | cast(Any, subprocess).DETACHED_PROCESS}
    launch_id = uuid.uuid4().hex
    _publish(run / "launches" / (launch_id + "-request.json"),
             {"launch_id": launch_id, "run_id": run.name, "requested_at_ns": time.time_ns()}, immutable=True)
    try:
        return subprocess.Popen([*_component_command("ops", run), "--launch-id", launch_id], cwd=run, env=environment,
                                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, **options)
    except OSError as exc:
        _publish(run / "launches" / (launch_id + "-result.json"),
                 {"launch_id": launch_id, "run_id": run.name, "status": "TEST GATE",
                  "reason": "PROCESS_START_FAILED", "error_type": type(exc).__name__}, immutable=True)
        raise


def request_stop(run: Path) -> None:
    manifest = load_run(run)
    status = _read_json(run / "status.json")
    if status.get("run_id") != manifest["run_id"] or not isinstance(status.get("epoch_id"), str):
        raise ValueError("no exact active research epoch to stop")
    _publish(run / "stop.request", {"run_id": manifest["run_id"], "epoch_id": status["epoch_id"]})


def resource_sample(run: Path) -> dict[str, Any]:
    sample: dict[str, Any] = {"cpu_seconds": time.process_time(), "threads": threading.active_count(),
                              "disk_free_bytes": shutil.disk_usage(run).free,
                              "rss_bytes": None, "handles": None}
    if os.name == "nt":
        from ctypes import wintypes

        class MemoryCounters(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD), ("faults", wintypes.DWORD),
                        *[(name, ctypes.c_size_t) for name in ("peak_working_set", "working_set",
                          "peak_paged", "paged", "peak_nonpaged", "nonpaged", "pagefile", "peak_pagefile")]]

        kernel = cast(Any, ctypes).WinDLL("kernel32", use_last_error=True)
        psapi = cast(Any, ctypes).WinDLL("psapi", use_last_error=True)
        kernel.GetCurrentProcess.restype = wintypes.HANDLE
        process = kernel.GetCurrentProcess()
        psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(MemoryCounters), wintypes.DWORD]
        psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
        kernel.GetProcessHandleCount.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
        kernel.GetProcessHandleCount.restype = wintypes.BOOL
        counters = MemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        handles = wintypes.DWORD()
        if psapi.GetProcessMemoryInfo(process, ctypes.byref(counters), counters.cb):
            sample["rss_bytes"] = counters.working_set
        if kernel.GetProcessHandleCount(process, ctypes.byref(handles)):
            sample["handles"] = handles.value
    else:
        import resource

        sample["peak_rss_bytes"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == "darwin" else 1024)
        # Peak RSS is kept distinct from current working set.
    return sample


def _load_broker_context(run: Path, path: Path) -> dict[str, Any]:
    if path.is_symlink() or path.resolve().parent != (run / "epochs").resolve():
        raise ValueError("broker context must be inside this run's epoch directory")
    with path.open("rb") as handle:
        cipher = handle.read(65_537)
    if not cipher or len(cipher) > 65_536:
        raise ValueError("protected broker context exceeds its bound")
    context = json.loads(WindowsSecretStore._crypt(cipher, decrypt=True))
    fields = {"schema_version", "run_id", "epoch_id", "pipe_name", "authentication_key", "signing_key", "owner_process"}
    if not isinstance(context, dict) or set(context) != fields or context["schema_version"] != 2:
        raise ValueError("invalid broker context contract")
    from .agent_intelligence.windows_broker import PIPE_NAME_RE

    owner = context["owner_process"]
    if (not isinstance(owner, dict) or set(owner) != {"pid", "creation_filetime_ticks"}
            or type(owner["pid"]) is not int or not 0 < owner["pid"] <= 0xFFFFFFFF
            or type(owner["creation_filetime_ticks"]) is not int
            or not 0 < owner["creation_filetime_ticks"] <= 0xFFFFFFFFFFFFFFFF
            or not isinstance(context["pipe_name"], str)
            or PIPE_NAME_RE.fullmatch(context["pipe_name"]) is None):
        raise ValueError("invalid broker owner or endpoint identity")
    epoch = context["epoch_id"]
    if (context["run_id"] != run.name or not isinstance(epoch, str) or len(epoch) != 32
            or any(c not in "0123456789abcdef" for c in epoch)
            or path.name != epoch + "-broker.dpapi"):
        raise ValueError("broker context does not bind this run and epoch")
    for field in ("authentication_key", "signing_key"):
        value = context[field]
        if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
            raise ValueError("invalid protected broker capability key")
    return context


def run_critic_broker(run: Path, context_path: Path, *, stop_requested: Callable[[], bool]) -> int:
    """The only installed component allowed to retrieve a provider credential."""
    from .agent_intelligence.windows_broker import WindowsOwnerProcessV1

    manifest = load_run(run)
    if manifest["configuration"]["provider_profile"] != "deepseek-v41-action-critic-v1":
        raise ValueError("this run did not authorize the fixed critic provider")
    context = _load_broker_context(run, context_path)
    with WindowsOwnerProcessV1(context["owner_process"]) as owner:
        return _serve_critic_broker(run, context, stop_requested=lambda: stop_requested() or not owner.alive())


def _serve_critic_broker(run: Path, context: dict[str, Any], *, stop_requested: Callable[[], bool]) -> int:
    from .agent_intelligence.broker import InferenceBroker
    from .agent_intelligence.budget import DeepSeekPriceScheduleV1
    from .agent_intelligence.profile import deepseek_v41_flash_action_critic_profile
    from .agent_intelligence.provider import DeepSeekResponsesActionAssessmentProvider
    from .agent_intelligence.windows_broker import WindowsActionCriticBrokerServer

    if stop_requested():
        return 0
    schedule = DeepSeekPriceScheduleV1.load(resource_file(
        "configs/agent_intelligence/provider_pricing_deepseek_v41_flash_v1.json"))
    profile = deepseek_v41_flash_action_critic_profile(price_schedule=schedule,
        agent_lock_path=resource_file("requirements-agent-lock.txt"))
    credential = WindowsSecretStore(default_data_root() / "secrets").get_provider_key(
        "deepseek-v41-action-critic-v1")
    provider = DeepSeekResponsesActionAssessmentProvider(credential)
    broker = InferenceBroker(provider, signing_key=bytes.fromhex(context["signing_key"]),
                             model_profile=profile, price_schedule_hash=schedule.content_hash)
    server = WindowsActionCriticBrokerServer(context["pipe_name"], broker,
        authentication_key=bytes.fromhex(context["authentication_key"]))
    status_path = run / "epochs" / (context["epoch_id"] + "-broker-status.json")
    stop_path = run / "epochs" / (context["epoch_id"] + "-broker-stop.json")
    try:
        server.start()
        while not stop_requested():
            health = server.health()
            _publish(status_path, {"run_id": run.name, "epoch_id": context["epoch_id"],
                                  "profile_hash": profile.content_hash, "pid": os.getpid(),
                                  "observed_at_ns": time.time_ns(), "health": health,
                                  "provider_conformance": "TEST GATE", "authority": "ZERO"})
            if stop_path.exists() and _read_json(stop_path) == {"run_id": run.name, "epoch_id": context["epoch_id"]}:
                break
            if health.get("status") == "TEST GATE":
                return 2
            time.sleep(0.25)
        return 0
    finally:
        server.close()


@dataclass
class _InstalledCriticRuntime:
    process: subprocess.Popen[bytes]
    shadow: Any
    run: Path
    epoch_id: str

    def close(self) -> None:
        try:
            self.shadow.close()
        finally:
            try:
                _publish(self.run / "epochs" / (self.epoch_id + "-broker-stop.json"),
                         {"run_id": self.run.name, "epoch_id": self.epoch_id})
            finally:
                _stop_child(self.process)


def _stop_child(process: subprocess.Popen[bytes]) -> None:
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def _start_installed_critic(run: Path, epoch_id: str) -> _InstalledCriticRuntime:
    from .agent_intelligence.windows_broker import WindowsActionCriticClientPort, current_owner_identity
    from .runtime.action_critic_shadow import create_action_assessment_shadow

    context: dict[str, Any] = {"schema_version": 2, "run_id": run.name, "epoch_id": epoch_id,
               "owner_process": current_owner_identity(),
               "pipe_name": "\\\\.\\pipe\\AtlasCritic-" + str(uuid.uuid4()),
               "authentication_key": os.urandom(32).hex(), "signing_key": os.urandom(32).hex()}
    context_path = run / "epochs" / (epoch_id + "-broker.dpapi")
    cipher = WindowsSecretStore._crypt(canonical_json(context).encode(), decrypt=False)
    with context_path.open("xb") as handle:
        handle.write(cipher)
        handle.flush()
        os.fsync(handle.fileno())
    environment = {k: v for k, v in os.environ.items()
                   if not any(word in k.upper() for word in ("KEY", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL"))}
    if not getattr(sys, "frozen", False):
        environment["PYTHONPATH"] = str(resource_file("src"))
    process = subprocess.Popen([*_component_command("critic-broker", run), "--broker-context", str(context_path)],
                               cwd=run, env=environment, stdin=subprocess.DEVNULL,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        deadline = time.monotonic() + 15
        status_path = run / "epochs" / (epoch_id + "-broker-status.json")
        while not status_path.is_file():
            if process.poll() is not None or time.monotonic() >= deadline:
                raise RuntimeError("configured critic broker did not become ready")
            time.sleep(0.05)
        status = _read_json(status_path)
        if (status.get("run_id") != run.name or status.get("epoch_id") != epoch_id
                or status.get("health", {}).get("status") != "IMPLEMENTED"):
            raise ValueError("broker readiness identity mismatch")
        port = WindowsActionCriticClientPort(context["pipe_name"],
            authentication_key=bytes.fromhex(context["authentication_key"]))
        shadow = create_action_assessment_shadow(str(run / "ops.sqlite"), None,
            capability_signing_key=bytes.fromhex(context["signing_key"]), broker_port=port)
        return _InstalledCriticRuntime(process, shadow, run, epoch_id)
    except BaseException:
        process.terminate()
        _stop_child(process)
        raise


def run_component(run: Path, *, smoke: bool = False, stop_requested: Callable[[], bool] = lambda: False) -> int:
    from .runtime.active_history import ActiveHistoryMaintenanceV1
    from .runtime.ops_supervisor import OpsSupervisorV2
    from .runtime.production import create_bybit_public_ws_port, create_production_port
    from .runtime.public_context import PublicContextMaintenanceV1
    from .runtime.research_model_shadow import StatisticalResearchShadowV1
    from .runtime.research_prediction_outcomes import ResearchPredictionOutcomeMaintenanceV1

    manifest = load_run(run)
    config = ResearchRunConfigV1(**manifest["configuration"])
    port = create_production_port() if smoke else create_bybit_public_ws_port()
    port.minimum_m15_origin_close_at_ns = manifest["started_at_ns"]
    from .runtime.production import IndexedPublicCycleSourceV1

    if isinstance(port.public_source, IndexedPublicCycleSourceV1):
        port.public_source.minimum_m15_origin_close_at_ns = manifest["started_at_ns"]
    epoch_id = uuid.uuid4().hex
    started = time.time_ns()
    state: dict[str, Any] = {"schema_version": 1, "run_id": manifest["run_id"], "epoch_id": epoch_id,
                             "started_at_ns": started, "pid": os.getpid(), "status": "IMPLEMENTED",
                             "capital_enabled": False, "assisted_enabled": False,
                             "provider_health": "DISABLED", "live_qualification": "TEST GATE"}
    result_code = 0
    # Acquire the repository writer before publishing an active epoch. A second
    # process cannot conceal or overwrite the status of the existing writer.
    prediction_maintenance = ResearchPredictionOutcomeMaintenanceV1(
        run_id=manifest["run_id"], config_hash=manifest["config_hash"],
        archive_root=run / "ops-observations")
    target_registered = False
    public_context: PublicContextMaintenanceV1 | None = None
    history_maintenance = ActiveHistoryMaintenanceV1(run / "ops-observations")

    def post_cycle(repo: Any, at_ns: int) -> Any:
        nonlocal target_registered
        if public_context is not None:
            public_context.run_cycle(repo, information_cutoff_ns=at_ns)
        if not smoke:
            history_maintenance.run_cycle(repo, cutoff_ns=at_ns)
        if not target_registered:
            prediction_maintenance.register_target(repo, available_at_ns=manifest["started_at_ns"])
            target_registered = True
        prediction_maintenance.run_cycle(repo, evidence_cutoff_ns=at_ns)
        from .runtime.outcome_maturity import run_outcome_maturity_cycle
        return run_outcome_maturity_cycle(repo, evidence_cutoff_ns=at_ns,
            production_clock_ns=time.time_ns, maintenance_budget_ns=200_000_000)

    with OpsSupervisorV2(run / "ops.sqlite", port, post_cycle_maintenance=post_cycle,
                         outcome_maintenance_budget_ns=250_000_000) as supervisor:
        _publish(run / "epochs" / (epoch_id + "-start.json"), state, immutable=True)
        last_report = time.monotonic()
        last_telemetry = float("-inf")
        critic: _InstalledCriticRuntime | None = None
        try:
            if not smoke:
                public_context = PublicContextMaintenanceV1()
            statistical = StatisticalResearchShadowV1(run_id=manifest["run_id"],
                config_hash=manifest["config_hash"], source_sha=manifest["source_sha"],
                environment_lock_hash=manifest["research_model_configuration"]["manifest"]["environment_lock_hash"],
                archive_root=run / "ops-observations")
            supervisor.post_receipt_shadow = _ResearchShadows(statistical)
            if supervisor.repository is not None and not target_registered:
                prediction_maintenance.register_target(supervisor.repository,
                    available_at_ns=manifest["started_at_ns"])
                target_registered = True
            if config.provider_profile != "DISABLED":
                critic = _start_installed_critic(run, epoch_id)
                supervisor.post_receipt_shadow = _ResearchShadows(statistical, critic.shadow)
                state["provider_health"] = "IMPLEMENTED"
            while True:
                if stop_requested():
                    state.update(status="IMPLEMENTED", reason="PROCESS_STOP_REQUESTED")
                    break
                if critic is not None and critic.process.poll() is not None:
                    state.update(status="TEST GATE", reason="CONFIGURED_PROVIDER_BROKER_LOST",
                                 provider_health="TEST GATE")
                    result_code = 2
                    break
                if shutil.disk_usage(run).free < config.minimum_free_disk_bytes:
                    state.update(status="TEST GATE", reason="DISK_RESERVE_EXHAUSTED")
                    result_code = 2
                    break
                stop_file = run / "stop.request"
                if stop_file.exists():
                    stop = _read_json(stop_file)
                    if stop == {"run_id": manifest["run_id"], "epoch_id": epoch_id}:
                        state.update(status="IMPLEMENTED", reason="OWNER_STOP")
                        break
                result = supervisor.run_once()
                state.update(observed_at_ns=time.time_ns(), source_health=result.cycle.source_health_state,
                             last_cycle_ref=result.cycle.content_hash, decision_count=len(result.event_receipts),
                             failure_types=list(result.cycle.failure_types), recovery_epoch=epoch_id,
                             disk_free_bytes=shutil.disk_usage(run).free,
                             db_bytes=(run / "ops.sqlite").stat().st_size,
                             wal_bytes=(run / "ops.sqlite-wal").stat().st_size if (run / "ops.sqlite-wal").exists() else 0)
                _publish(run / "status.json", state)
                if smoke:
                    state.update(status="TESTED", reason="OFFLINE_COMPOSITION_FIXTURE_ONLY")
                    break
                if time.monotonic() - last_report >= config.report_interval_seconds:
                    export_run(run)
                    last_report = time.monotonic()
                if supervisor.repository is not None and time.monotonic() - last_telemetry >= 60:
                    from .memory.repository import ArtifactIndexEntryV2

                    at_ns = time.time_ns()
                    telemetry = {"schema_version": 1, "run_id": manifest["run_id"],
                                 "config_hash": manifest["config_hash"], "epoch_id": epoch_id,
                                 "available_at_ns": at_ns, "source_health": result.cycle.source_health_state,
                                 "cycle_ref": result.cycle.content_hash, **resource_sample(run),
                                 "db_bytes": state["db_bytes"], "wal_bytes": state["wal_bytes"],
                                 "authority": "ZERO"}
                    ref = sha256_json(telemetry)
                    supervisor.repository.register_artifact(ArtifactIndexEntryV2(
                        ref, "ResearchRunTelemetryV1", ref, at_ns, at_ns, {"telemetry": telemetry}))
                    supervisor.repository.checkpoint()
                    last_telemetry = time.monotonic()
                time.sleep(1)
        except Exception as exc:
            # Persist only a closed error class, never exception/provider text.
            state.update(status="TEST GATE", reason="RUNTIME_COMPONENT_FAILED", error_type=type(exc).__name__)
            result_code = 2
        finally:
            if public_context is not None:
                public_context.close()
            if critic is not None:
                try:
                    critic.close()
                except Exception as exc:
                    state.update(status="TEST GATE", reason="PROVIDER_SHUTDOWN_FAILED", error_type=type(exc).__name__)
                    result_code = 2
            state.update(stopped_at_ns=time.time_ns(), observed_at_ns=time.time_ns())
            _publish(run / "status.json", state)
            _publish(run / "epochs" / (epoch_id + "-stop.json"), state, immutable=True)
    try:
        export_run(run)
    except Exception as exc:
        _publish(run / "report-failure.json", {"status": "TEST GATE", "reason": "REPORT_UNAVAILABLE",
                                               "error_type": type(exc).__name__,
                                               "run_id": manifest["run_id"]})
        result_code = 2
    return result_code


def _refresh_run_selector(selector: Any, data_root: Path, *, preferred_run: Path | None = None) -> None:
    preferred = str(preferred_run) if preferred_run is not None else selector.currentData()

    def records():
        for index, path in enumerate((data_root.expanduser() / "runs").glob("*/run.json")):
            if index >= 4096:
                raise ValueError("run inventory exceeds its browse budget; select another data folder")
            try:
                body = load_run(path.parent, require_current_build=False)
                yield body["started_at_ns"], body["run_id"], str(path.parent)
            except (OSError, ValueError, TypeError, KeyError):
                continue

    entries = heapq.nlargest(128, records())
    if preferred and all(item[2] != preferred for item in entries):
        path = Path(preferred)
        if path.parent == data_root.expanduser() / "runs":
            body = load_run(path, require_current_build=False)
            entries = [(body["started_at_ns"], body["run_id"], str(path)), *entries[:127]]
    selector.clear()
    for _created_at, run_id, path in entries:
        selector.addItem(run_id, path)
    selected_index = selector.findData(preferred)
    if selected_index >= 0:
        selector.setCurrentIndex(selected_index)


def _runtime_status_text(state: dict[str, Any], *, now_ns: int) -> str:
    heartbeat = state.get("observed_at_ns")
    stopped = state.get("stopped_at_ns") is not None
    runtime_status = state.get("status", "UNVERIFIED")
    reason = state.get("reason", "No live qualification claimed")
    if type(heartbeat) is not int or heartbeat <= 0:
        runtime_status, reason, age = "TEST GATE", "RUNTIME_HEARTBEAT_UNAVAILABLE", None
    elif now_ns < heartbeat:
        runtime_status, reason, age = "TEST GATE", "RUNTIME_CLOCK_REGRESSION", None
    else:
        age = (now_ns - heartbeat) // 1_000_000_000
        if not stopped and age > 30:
            runtime_status, reason = "TEST GATE", "RUNTIME_HEARTBEAT_STALE"
    activity = "Stopped" if stopped else "Heartbeat unavailable" if age is None else f"Heartbeat age {age}s"
    return (f"Run {state['run_id']}\nRuntime: {runtime_status} / {activity}\n"
            f"Data: {state.get('source_health', 'UNVERIFIED')}\n"
            f"Provider: {state.get('provider_health', 'DISABLED')}\n"
            f"Decisions in last cycle: {state.get('decision_count', 'UNVERIFIED')}\n"
            f"Disk free: {state.get('disk_free_bytes', 'UNVERIFIED')} bytes; "
            f"DB/WAL: {state.get('db_bytes', 'UNVERIFIED')}/{state.get('wal_bytes', 'UNVERIFIED')} bytes\n"
            f"Reason: {reason}\nCapital disabled.")


def desktop() -> int:
    from concurrent.futures import ThreadPoolExecutor

    from PySide6.QtCore import QTimer
    from PySide6.QtWidgets import (
        QApplication,
        QComboBox,
        QFileDialog,
        QInputDialog,
        QLabel,
        QLineEdit,
        QMessageBox,
        QPushButton,
        QVBoxLayout,
        QWidget,
    )

    app = QApplication(sys.argv[:1])
    window = QWidget()
    window.setWindowTitle("ATLAS — Public research")
    layout = QVBoxLayout(window)
    root_edit = QLineEdit(str(default_data_root()))
    layout.addWidget(QLabel("Research data location"))
    layout.addWidget(root_edit)
    browse = QPushButton("Choose folder")
    layout.addWidget(browse)
    runs = QComboBox()
    layout.addWidget(QLabel("Research run — public Bybit baseline"))
    layout.addWidget(runs)
    provider = QComboBox()
    provider.addItem("Intelligence disabled", "DISABLED")
    provider.addItem("Optional DeepSeek V4.1 Flash action critic", "deepseek-v41-action-critic-v1")
    layout.addWidget(QLabel("Intelligence configuration for new runs"))
    layout.addWidget(provider)
    status_label = QLabel("Capital disabled. Public collection requires no API key.")
    status_label.setWordWrap(True)
    layout.addWidget(status_label)
    layout.addWidget(QLabel(development_gate()["verdict"]))
    buttons = {name: QPushButton(name) for name in ("Create run", "Start / resume", "Stop", "Export report")}
    for button in buttons.values():
        layout.addWidget(button)
    secret_button = QPushButton("Store optional DeepSeek key securely")
    layout.addWidget(secret_button)
    layout.addWidget(QLabel("Provider use requires an explicitly selected run configuration and protected key. Live provider conformance remains a TEST GATE."))
    reports = ThreadPoolExecutor(max_workers=1, thread_name_prefix="atlas-report")
    pending_report: Any = None

    def refresh_runs(preferred_run: Path | None = None) -> None:
        try:
            _refresh_run_selector(runs, Path(root_edit.text()), preferred_run=preferred_run)
        except (OSError, ValueError, TypeError, KeyError):
            status_label.setText("Run inventory: TEST GATE. Select a data folder with a supported run inventory.")

    def selected() -> Path:
        value = runs.currentData()
        if not value:
            raise ValueError("create or select a research run first")
        return Path(value)

    def action(name: str) -> None:
        nonlocal pending_report
        try:
            if name == "Create run":
                created = create_run(Path(root_edit.text()), ResearchRunConfigV1(provider_profile=provider.currentData()))
                refresh_runs(created)
            elif name == "Start / resume":
                launch_run(selected())
            elif name == "Stop":
                request_stop(selected())
            elif name == "Export report":
                if pending_report is None:
                    pending_report = reports.submit(export_run, selected())
                    buttons[name].setEnabled(False)
        except Exception as exc:
            QMessageBox.warning(window, "ATLAS", "Operation failed: " + type(exc).__name__)

    def choose_folder() -> None:
        chosen = QFileDialog.getExistingDirectory(window, "Research data location", root_edit.text())
        if chosen:
            root_edit.setText(chosen)
            refresh_runs()

    def store_secret() -> None:
        value, accepted = QInputDialog.getText(window, "Optional provider key", "DeepSeek API key",
                                               QLineEdit.EchoMode.Password)
        if accepted and value:
            try:
                WindowsSecretStore(default_data_root() / "secrets").put_provider_key(
                    "deepseek-v41-action-critic-v1", value)
                QMessageBox.information(window, "ATLAS", "Key stored with Windows protection. Select the optional critic when creating a run to enable it.")
            except Exception as exc:
                QMessageBox.warning(window, "ATLAS", "Secret storage failed: " + type(exc).__name__)

    def refresh_status() -> None:
        nonlocal pending_report
        if pending_report is not None and pending_report.done():
            try:
                report = pending_report.result()
                detail = ("Additional evidence remains to export." if report["has_more"]
                          else "Evidence beyond the report cutoff is pending." if report["blocked_future_evidence"]
                          else "Report exported to the run's reports folder.")
                QMessageBox.information(window, "ATLAS", report["report"]["status"] + ": " + detail)
            except Exception as exc:
                QMessageBox.warning(window, "ATLAS", "Report failed: " + type(exc).__name__)
            pending_report = None
            buttons["Export report"].setEnabled(True)
        try:
            status = _read_json(selected() / "status.json")
            status_label.setText(_runtime_status_text(status, now_ns=time.time_ns()))
        except (OSError, ValueError, TypeError, KeyError):
            status_label.setText("No runtime status. Create/start a public run. Capital disabled.")

    for name, button in buttons.items():
        button.clicked.connect(lambda checked=False, name=name: action(name))
    browse.clicked.connect(choose_folder)
    secret_button.clicked.connect(store_secret)
    root_edit.editingFinished.connect(refresh_runs)
    refresh_runs()
    timer = QTimer(window)
    timer.timeout.connect(refresh_status)
    timer.start(2000)
    window.resize(600, 500)
    window.show()
    try:
        return app.exec()
    finally:
        reports.shutdown(wait=False, cancel_futures=True)


def main() -> int:
    parser = argparse.ArgumentParser(prog="ATLAS")
    parser.add_argument("--diagnostics", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--broker-smoke", action="store_true")
    parser.add_argument("--data-root", type=Path)
    parser.add_argument("--run-root", type=Path)
    parser.add_argument("--launch-id")
    parser.add_argument("--broker-context", type=Path)
    parser.add_argument("--component", choices=("ops", "desktop", "critic-broker"))
    args = parser.parse_args()
    if args.broker_smoke:
        from .agent_intelligence.windows_broker import native_windows_critic_broker_smoke_v1

        print(json.dumps(native_windows_critic_broker_smoke_v1()))
        return 0
    if args.diagnostics:
        build = build_identity()
        print(json.dumps({"build": build, "version": build["version"], "source_sha": build["source_sha"], "capital_enabled": False,
                          "assisted_enabled": False, "public_requires_secret": False,
                          "windows_secret_backend": "IMPLEMENTED" if os.name == "nt" else "BLOCKED BY ENVIRONMENT",
                          "provider_dispatch": "EXPLICIT_RUN_CONFIGURATION", "live_qualification": "TEST GATE",
                          "development_gate": development_gate()}))
        return 0
    if args.smoke:
        if args.data_root is None:
            parser.error("offline smoke requires --data-root")
        dependencies = runtime_dependency_smoke()
        run = create_run(args.data_root, ResearchRunConfigV1())
        result = run_component(run, smoke=True)
        state = _read_json(run / "status.json")
        report_failure = _read_json(run / "report-failure.json") if (run / "report-failure.json").exists() else None
        print(json.dumps({"status": "TESTED" if result == 0 else "TEST GATE",
                          "reason": state.get("reason"), "run_id": run.name,
                          "capital_enabled": False, "assisted_enabled": False,
                          "live_qualification": "TEST GATE", "runtime_dependencies": dependencies,
                          "report_failure": report_failure}))
        return result
    if args.component in {"ops", "critic-broker"}:
        if args.run_root is None:
            parser.error("runtime component requires --run-root")
        if args.component == "critic-broker" and args.broker_context is None:
            parser.error("critic broker requires its protected epoch context")
        if args.launch_id is not None and (len(args.launch_id) != 32
                                          or any(c not in "0123456789abcdef" for c in args.launch_id)):
            parser.error("launch identity must be an exact generated identifier")
        stop = threading.Event()
        previous_handlers = {kind: signal.signal(kind, lambda signum, frame: stop.set())
                             for kind in (signal.SIGINT, signal.SIGTERM)}
        try:
            if args.component == "critic-broker":
                result = run_critic_broker(args.run_root, args.broker_context, stop_requested=stop.is_set)
            else:
                result = run_component(args.run_root, stop_requested=stop.is_set)
            reason = "COMPONENT_STOPPED" if result == 0 else "COMPONENT_FAILED"
            error_type = None
        except Exception as exc:
            result, reason, error_type = 2, "COMPONENT_START_OR_WRITER_FAILED", type(exc).__name__
        finally:
            for kind, handler in previous_handlers.items():
                signal.signal(kind, handler)
        if args.launch_id is not None:
            _publish(args.run_root / "launches" / (args.launch_id + "-result.json"),
                     {"launch_id": args.launch_id, "run_id": args.run_root.name,
                      "status": "IMPLEMENTED" if result == 0 else "TEST GATE", "reason": reason,
                      "error_type": error_type, "stopped_at_ns": time.time_ns()}, immutable=True)
        return result
    return desktop()
