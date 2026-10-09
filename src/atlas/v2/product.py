"""Non-capital installed run lifecycle, configuration and diagnostic surface.

The ops supervisor remains the only research writer. The launcher never opens
a writable research database or imports the capital runtime.
"""

from __future__ import annotations

import argparse
import asyncio
import ctypes
import hashlib
import heapq
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast

from atlas import __version__

from ._serialization import canonical_json, sha256_json, sha256_ref
from .resources import resource_file

RUN_PROFILE = "PUBLIC_BYBIT_BASELINE_V1"
S7_EVENT_EXTRACTION_PROFILE = "openai-gpt6-astra-s7-event-extraction-v1"
PROVIDER_PROFILES = {"DISABLED", "deepseek-v41-action-critic-v1", S7_EVENT_EXTRACTION_PROFILE}
V1_PROVIDER_PROFILES = {"DISABLED", "deepseek-v41-action-critic-v1"}
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
    report_interval_seconds: int = 60
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
        if not isinstance(self.provider_profile, str) or self.provider_profile not in V1_PROVIDER_PROFILES:
            raise ValueError("unregistered provider dispatch profile")

    @property
    def content_hash(self) -> str:
        return sha256_json({"schema_version": 1, **asdict(self)})


_OPAQUE_REFERENCE_RE = re.compile(r"(?:ref_[A-Za-z0-9_-]{1,120}|sha256:[0-9a-f]{64})\Z")


@dataclass(frozen=True)
class ResearchRunConfigV2:
    """Owner-declared, non-capital run selection frozen before preflight.

    V2 configuration records the selected public collection and intended demo
    or test execution identity. It does not create an authenticated venue
    client or grant execution authority.
    """

    schema_version: int = 2
    data_root: str = ""
    public_venues: tuple[str, ...] = ("BYBIT",)
    selected_execution_venue: str = "BYBIT"
    execution_environment: str = "DEMO"
    product_scope: str = "LINEAR_PERPETUAL"
    scope: str = "BOUNDED_UNIVERSE_V2"
    resource_profile_id: str = "bounded-v2"
    account_scope_ref: str | None = None
    capability_profile_ref: str | None = None
    credential_ref: str | None = None
    execution_profile: str = "DISABLED"
    execution_instrument_symbol: str | None = None
    owner_pair_configuration_hash: str | None = None
    official_fomc_source_profile_json: str | None = None
    report_interval_seconds: int = 60
    minimum_free_disk_bytes: int = 1_073_741_824
    provider_profile: str = "DISABLED"

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != 2:
            raise ValueError("unsupported run configuration version")
        if not isinstance(self.data_root, str) or not self.data_root:
            raise ValueError("V2 run configuration requires its selected data root")
        selected_root = Path(self.data_root).expanduser().resolve()
        if str(selected_root) != self.data_root:
            raise ValueError("V2 data root must be canonical before run creation")
        if isinstance(self.public_venues, list):
            object.__setattr__(self, "public_venues", tuple(self.public_venues))
        if (not isinstance(self.public_venues, tuple) or not self.public_venues
                or len(self.public_venues) > 2
                or any(not isinstance(venue, str) or venue not in {"BYBIT", "BINANCE"}
                       for venue in self.public_venues)
                or len(set(self.public_venues)) != len(self.public_venues)):
            raise ValueError("public venues must be a unique registered venue tuple")
        if (not isinstance(self.selected_execution_venue, str)
                or self.selected_execution_venue not in {"BYBIT", "BINANCE"}):
            raise ValueError("unsupported selected execution venue")
        if (not isinstance(self.execution_environment, str)
                or self.execution_environment not in {"DEMO", "TESTNET"}):
            raise ValueError("execution environment must be DEMO or TESTNET")
        if (self.product_scope != "LINEAR_PERPETUAL" or self.scope != "BOUNDED_UNIVERSE_V2"
                or not isinstance(self.product_scope, str) or not isinstance(self.scope, str)):
            raise ValueError("unsupported product or bounded universe scope")
        if not isinstance(self.resource_profile_id, str) or self.resource_profile_id != "bounded-v2":
            raise ValueError("unsupported bounded resource profile")
        if (type(self.report_interval_seconds) is not int
                or not 60 <= self.report_interval_seconds <= 86_400):
            raise ValueError("report interval must be between one minute and one day")
        if type(self.minimum_free_disk_bytes) is not int or self.minimum_free_disk_bytes < 268_435_456:
            raise ValueError("research run requires a declared disk reserve")
        if (not isinstance(self.provider_profile, str)
                or self.provider_profile not in PROVIDER_PROFILES):
            raise ValueError("unregistered provider dispatch profile")
        for field_name in ("account_scope_ref", "capability_profile_ref", "credential_ref"):
            value = getattr(self, field_name)
            if value is not None and (not isinstance(value, str) or _OPAQUE_REFERENCE_RE.fullmatch(value) is None):
                raise ValueError(f"{field_name} must be an opaque registered reference")
        if self.execution_profile not in {"DISABLED", "DEMO_READ_ONLY_QUALIFICATION", "DEMO_NATIVE_OMS_QUALIFICATION"}:
            raise ValueError("unsupported demo qualification profile")
        if self.owner_pair_configuration_hash is not None:
            sha256_ref(self.owner_pair_configuration_hash, field="owner_pair_configuration_hash")
        if self.official_fomc_source_profile_json is not None:
            from .runtime.official_calendar import OfficialFomcSourceProfileV1

            encoded = self.official_fomc_source_profile_json
            if not isinstance(encoded, str) or len(encoded.encode("utf-8")) > 8192:
                raise ValueError("official FOMC source profile exceeds its configuration bound")
            profile = OfficialFomcSourceProfileV1.from_dict(json.loads(encoded))
            if canonical_json(profile.to_dict()) != encoded:
                raise ValueError("official FOMC source profile must use canonical JSON")
        if self.execution_instrument_symbol is not None and (
            not isinstance(self.execution_instrument_symbol, str)
            or re.fullmatch(r"[A-Z0-9_]{1,32}", self.execution_instrument_symbol) is None
        ):
            raise ValueError("invalid demo instrument symbol")
        if self.execution_profile != "DISABLED" and (
            any(getattr(self, name) is None for name in ("account_scope_ref", "capability_profile_ref", "credential_ref"))
            or self.execution_profile == "DEMO_NATIVE_OMS_QUALIFICATION" and self.execution_instrument_symbol is None
        ):
            raise ValueError("demo qualification requires explicit bound run references and native instrument")

    @property
    def content_hash(self) -> str:
        return sha256_json(asdict(self))


RunConfig = ResearchRunConfigV1 | ResearchRunConfigV2


def canonical_data_root(data_root: Path) -> str:
    return str(data_root.expanduser().resolve())


def _run_config(value: dict[str, Any]) -> RunConfig:
    if value.get("schema_version") == 2:
        return ResearchRunConfigV2(**value)
    return ResearchRunConfigV1(**value)


def _selected_run_configuration_text(
    run_id: str,
    config: RunConfig,
    *,
    provider_key_status: str,
    demo_credential_status: str,
) -> str:
    """Render immutable run identity without exposing protected secret values."""
    if isinstance(config, ResearchRunConfigV2):
        identity = (
            f"public venues: {' + '.join(config.public_venues)}; "
            f"authenticated execution: {config.selected_execution_venue} / {config.execution_environment}; "
            f"execution profile: {config.execution_profile}; "
            f"native instrument: {config.execution_instrument_symbol or 'NOT SET'}; "
            f"account scope ref: {config.account_scope_ref or 'NOT BOUND'}; "
            f"capability profile ref: {config.capability_profile_ref or 'NOT BOUND'}; "
            f"provider profile: {config.provider_profile}; provider key: {provider_key_status}; "
            f"demo/test credential: {demo_credential_status}"
        )
    else:
        identity = (
            "public venues: BYBIT (legacy V1); authenticated execution: NOT CONFIGURED; "
            f"provider profile: {config.provider_profile}; provider key: {provider_key_status}; "
            "demo/test credential: NOT BOUND"
        )
    return (f"Selected immutable run {run_id} — {identity}. "
            "Capital: OFF; assisted execution: OFF. Secret values are never displayed. "
            "The controls above set choices for a new run and do not change this run.")


def secret_store_for_run(manifest: dict[str, Any]) -> WindowsSecretStore:
    config = _run_config(manifest["configuration"])
    root = Path(config.data_root) if isinstance(config, ResearchRunConfigV2) else default_data_root()
    return WindowsSecretStore(root / "secrets")


def demo_qualification_for_run(run: Path) -> Any:
    """Create the declared demo host; no credential read or connection yet."""
    from atlas.runtime.selected_demo import SelectedDemoQualification

    manifest = load_run(run)
    config = _run_config(manifest["configuration"])
    if not isinstance(config, ResearchRunConfigV2):
        raise ValueError("demo qualification requires a V2 run")
    return SelectedDemoQualification(run=run, configuration=config,
        secret_store=secret_store_for_run(manifest), lease_root=default_data_root() / "control")


def check_demo_account_for_run(run: Path) -> dict[str, Any]:
    host = demo_qualification_for_run(run)
    try:
        result = host.open()
        _publish(run / "demo-qualification-status.json", result)
        return result
    finally:
        host.close()


def run_selected_demo_component(run: Path, *, stop_requested: Callable[[], bool] = lambda: False,
                                host_factory: Callable[[Path], Any] = demo_qualification_for_run) -> int:
    """Explicit selected demo host; public startup never invokes this process."""
    manifest = load_run(run)
    config = _run_config(manifest["configuration"])
    if not isinstance(config, ResearchRunConfigV2) or config.execution_profile != "DEMO_NATIVE_OMS_QUALIFICATION":
        raise ValueError("select native demo qualification in a new immutable run")
    host = host_factory(run)
    epoch = uuid.uuid4().hex
    started = time.time_ns()
    result = 0

    async def serve() -> None:
        nonlocal result
        host.open()
        if host.node is None:
            raise ValueError("native demo host unavailable")
        task = asyncio.create_task(host.node.run())
        try:
            await asyncio.sleep(0)
            while not task.done():
                state = host.status() | {"epoch_id": epoch, "run_id": manifest["run_id"],
                    "started_at_ns": started, "observed_at_ns": time.time_ns(),
                    "status": "IMPLEMENTED", "reason": "SELECTED_DEMO_OMS_ACTIVE",
                    "runtime_failure": host.node.last_failure_code}
                _publish(run / "demo-runtime-status.json", state)
                stop_path = run / "demo-stop.request"
                requested = _read_json(stop_path) if stop_path.exists() else {}
                if stop_requested() or requested == {"run_id": manifest["run_id"], "epoch_id": epoch}:
                    break
                if host.node.last_failure_code or host.node.queue_overflow:
                    result = 2
                    break
                await asyncio.sleep(0.2)
            if task.done():
                await task
        finally:
            await host.node.stop()
            await task

    try:
        asyncio.run(serve())
    except Exception as exc:
        result = 2
        _publish(run / "demo-runtime-failure.json", {"status": "TEST GATE",
            "reason": "SELECTED_DEMO_RUNTIME_FAILED", "error_type": type(exc).__name__,
            "epoch_id": epoch, "observed_at_ns": time.time_ns()})
    finally:
        # A failed stop must keep its account lease held until the process ends.
        # SelectedDemoQualification.close refuses live native/read workers.
        host.close()
        _publish(run / "demo-runtime-status.json", {"status": "IMPLEMENTED" if result == 0 else "TEST GATE",
            "reason": "SELECTED_DEMO_STOPPED" if result == 0 else "SELECTED_DEMO_RUNTIME_FAILED",
            "run_id": manifest["run_id"], "config_hash": manifest["config_hash"], "epoch_id": epoch,
            "observed_at_ns": time.time_ns(), "stopped_at_ns": time.time_ns(),
            "capital_enabled": False, "assisted_enabled": False})
    return result


def launch_selected_demo(run: Path) -> subprocess.Popen[bytes]:
    manifest = load_run(run)
    config = _run_config(manifest["configuration"])
    if not isinstance(config, ResearchRunConfigV2) or config.execution_profile != "DEMO_NATIVE_OMS_QUALIFICATION":
        raise ValueError("select native demo qualification in a new run")
    environment = {key: value for key, value in os.environ.items()
                   if not any(word in key.upper() for word in ("KEY", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL"))}
    if not getattr(sys, "frozen", False):
        environment["PYTHONPATH"] = str(resource_file("src"))
    options: dict[str, Any] = {"start_new_session": True} if os.name != "nt" else {
        "creationflags": cast(Any, subprocess).CREATE_NEW_PROCESS_GROUP | cast(Any, subprocess).DETACHED_PROCESS}
    return subprocess.Popen(_component_command("demo-oms", run), cwd=run, env=environment,
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, **options)


def request_demo_stop(run: Path) -> None:
    manifest = load_run(run)
    status = _read_json(run / "demo-runtime-status.json")
    if status.get("run_id") != manifest["run_id"] or not isinstance(status.get("epoch_id"), str):
        raise ValueError("no exact active demo epoch to stop")
    _publish(run / "demo-stop.request", {"run_id": manifest["run_id"], "epoch_id": status["epoch_id"]})


def default_data_root() -> Path:
    if os.name == "nt":
        base = os.environ.get("LOCALAPPDATA")
        if not base:
            raise RuntimeError("Windows application-data location is unavailable")
        return Path(base) / "Atlas"
    return Path.home() / ".local" / "share" / "atlas"


def create_run(data_root: Path, config: RunConfig, *, owner_pair_configuration_path: Path | None = None) -> Path:
    root = data_root.expanduser().resolve()
    if isinstance(config, ResearchRunConfigV2) and config.data_root != str(root):
        raise ValueError("run data root differs from its immutable V2 configuration")
    if getattr(sys, "frozen", False) and root.is_relative_to(Path(sys.executable).parent.resolve()):
        raise ValueError("research data must be outside the installation directory")
    pair_configuration = None
    if owner_pair_configuration_path is not None:
        from .runtime.owner_pairs import read_owner_pair_configuration

        pair_configuration = read_owner_pair_configuration(owner_pair_configuration_path)
    expected_pair_hash = config.owner_pair_configuration_hash if isinstance(config, ResearchRunConfigV2) else None
    if (pair_configuration.content_hash if pair_configuration is not None else None) != expected_pair_hash:
        raise ValueError("owner pair configuration differs from the immutable run configuration")
    run_id = uuid.uuid4().hex
    run = root / "runs" / run_id
    run.mkdir(parents=True, exist_ok=False)
    for name in ("reports", "epochs", "launches"):
        (run / name).mkdir()
    if pair_configuration is not None:
        _publish(run / "owner-pairs.json", pair_configuration.to_dict(), immutable=True)
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
    config = _run_config(body["configuration"])
    if (body["content_hash"] != expected or body["config_hash"] != config.content_hash
            or body["capital_enabled"] is not False or body["assisted_enabled"] is not False
            or body["holdout_access"] is not False or body["run_id"] != run.name
            or type(body["started_at_ns"]) is not int or body["started_at_ns"] <= 0):
        raise ValueError("run identity/configuration drift")
    if isinstance(config, ResearchRunConfigV2):
        configured_root = Path(config.data_root).expanduser().resolve()
        if str(configured_root) != config.data_root or configured_root != run.resolve().parent.parent:
            raise ValueError("V2 run cannot be relocated from its immutable data root")
        pair_path = run / "owner-pairs.json"
        if config.owner_pair_configuration_hash is not None:
            from .runtime.owner_pairs import read_owner_pair_configuration

            if read_owner_pair_configuration(pair_path).content_hash != config.owner_pair_configuration_hash:
                raise ValueError("immutable owner pair configuration drift")
        elif pair_path.exists() or pair_path.is_symlink():
            raise ValueError("unregistered owner pair configuration")
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
    if profile == S7_EVENT_EXTRACTION_PROFILE:
        from .agent_intelligence.event_extraction_transport import EVENT_EXTRACTION_PROFILE_V1

        return dict(EVENT_EXTRACTION_PROFILE_V1)
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
    def _provider_key_name(profile: str) -> str:
        names = {
            "deepseek-v41-action-critic-v1": "deepseek-action-critic.dpapi",
            S7_EVENT_EXTRACTION_PROFILE: "openai-s7-event-extraction.dpapi",
        }
        try:
            return names[profile]
        except (KeyError, TypeError):
            raise ValueError("unsupported provider secret profile") from None

    def has_provider_key(self, profile: str) -> bool:
        path = self.root / self._provider_key_name(profile)
        return path.is_file() and not path.is_symlink()

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
        if not 1 <= len(key) <= 4096:
            raise ValueError("unsupported provider secret profile")
        name = self._provider_key_name(profile)
        encrypted = self._crypt(key.encode(), decrypt=False)
        self.root.mkdir(parents=True, exist_ok=True)
        target = self.root / name
        temporary = target.with_name(target.name + "." + uuid.uuid4().hex + ".tmp")
        try:
            with temporary.open("xb") as handle:
                handle.write(encrypted)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _venue_credentials_name(reference: str, venue: str, environment: str) -> str:
        if _OPAQUE_REFERENCE_RE.fullmatch(reference) is None:
            raise ValueError("credential reference must be opaque")
        if venue not in {"BYBIT", "BINANCE"} or environment not in {"DEMO", "TESTNET"}:
            raise ValueError("unsupported demo credential binding")
        binding = f"{venue}:{environment}:{reference}".encode()
        return "venue-" + hashlib.sha256(binding).hexdigest() + ".dpapi"

    def put_demo_exchange_credentials(
        self, reference: str, venue: str, environment: str, api_key: str, api_secret: str,
    ) -> None:
        """Store demo/test credentials under DPAPI without enabling venue calls."""
        if not 1 <= len(api_key) <= 4096 or not 1 <= len(api_secret) <= 4096:
            raise ValueError("demo credential fields are outside their bound")
        name = self._venue_credentials_name(reference, venue, environment)
        if os.name != "nt":
            raise RuntimeError("Windows protected secrets backend unavailable")
        body = {"schema_version": 1, "reference": reference, "venue": venue,
                "environment": environment, "api_key": api_key, "api_secret": api_secret}
        encrypted = self._crypt(canonical_json(body).encode(), decrypt=False)
        self.root.mkdir(parents=True, exist_ok=True)
        target = self.root / name
        temporary = target.with_name(target.name + "." + uuid.uuid4().hex + ".tmp")
        try:
            with temporary.open("xb") as handle:
                handle.write(encrypted)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)

    def has_demo_exchange_credentials(self, reference: str, venue: str, environment: str) -> bool:
        """Report whether a bound protected record exists without disclosing it."""
        name = self._venue_credentials_name(reference, venue, environment)
        path = self.root / name
        return path.is_file() and not path.is_symlink()

    def get_demo_exchange_credentials(self, reference: str, venue: str, environment: str) -> Any:
        """Retrieve one exact protected binding for the isolated demo component."""
        from atlas.runtime.binance_demo import DemoCredential

        name = self._venue_credentials_name(reference, venue, environment)
        target = self.root / name
        if target.is_symlink():
            raise ValueError("protected credential record cannot be a symlink")
        with target.open("rb") as handle:
            cipher = handle.read(65_537)
        if not cipher or len(cipher) > 65_536:
            raise ValueError("invalid protected credential size")
        plaintext = self._crypt(cipher, decrypt=True)
        if not plaintext or len(plaintext) > 32_768:
            raise ValueError("invalid protected credential payload size")
        try:
            body = json.loads(plaintext)
            expected = {"schema_version", "reference", "venue", "environment", "api_key", "api_secret"}
            if (not isinstance(body, dict) or set(body) != expected or body["schema_version"] != 1
                    or body["reference"] != reference or body["venue"] != venue or body["environment"] != environment):
                raise ValueError("credential binding mismatch")
            return DemoCredential(body["api_key"], body["api_secret"])
        except (ValueError, TypeError, KeyError):
            raise ValueError("protected credential binding invalid") from None

    def get_provider_key(self, profile: str) -> str:
        """Called only by the isolated credential broker component."""
        name = self._provider_key_name(profile)
        with (self.root / name).open("rb") as handle:
            cipher = handle.read(65_537)
        if not cipher or len(cipher) > 65_536:
            raise ValueError("invalid protected provider key size")
        key = self._crypt(cipher, decrypt=True).decode("utf-8")
        if not 1 <= len(key) <= 4096:
            raise ValueError("invalid protected provider key size")
        return key


def recent_event_alerts_for_run(run: Path, *, limit: int = 8) -> tuple[dict[str, Any], ...]:
    """Return a bounded read-only view of recent zero-authority public alerts."""
    if type(limit) is not int or not 1 <= limit <= 20:
        raise ValueError("recent alert limit must be between 1 and 20")
    load_run(run, require_current_build=False)
    database = run / "ops.sqlite"
    if not database.is_file() or database.is_symlink():
        return ()
    from .memory.repository import OpsRepository

    repository = OpsRepository(database, read_only=True)
    try:
        page = repository.latest_artifact_entries("EventAlertV2", as_of_ns=time.time_ns(), limit=limit)
        alerts: list[dict[str, Any]] = []
        for entry in page.entries:
            metadata = entry.metadata.get("alert")
            if not isinstance(metadata, Mapping):
                continue
            event_ref = metadata.get("event_ref")
            event = repository.get_artifact(event_ref) if isinstance(event_ref, str) else None
            event_body = event.metadata.get("event") if event is not None else None
            alerts.append({
                "alert_ref": entry.artifact_ref,
                "available_at_ns": entry.available_at_ns,
                "relevance": str(metadata.get("relevance", "UNVERIFIED"))[:80],
                "event_type": (str(event_body.get("event_type", "UNKNOWN"))[:48]
                    if isinstance(event_body, Mapping) else "UNKNOWN"),
                "severity": (str(event_body.get("severity", "UNKNOWN"))[:24]
                    if isinstance(event_body, Mapping) else "UNKNOWN"),
            })
        if page.invalid_entry_count:
            alerts.append({"alert_ref": "", "available_at_ns": 0, "relevance": "INVALID_ALERT_RECORDS",
                           "event_type": "UNKNOWN", "severity": "UNKNOWN"})
        if page.has_more:
            alerts.append({"alert_ref": "", "available_at_ns": 0, "relevance": "RECENT_ALERT_LIST_TRUNCATED",
                           "event_type": "UNKNOWN", "severity": "UNKNOWN"})
        return tuple(alerts)
    finally:
        repository.close()


def export_run(run: Path, *, maximum_snapshots: int = 8) -> Any:
    from .science.tuning_export import TuningRunIdentityV1, export_tuning_snapshot

    if type(maximum_snapshots) is not int or not 1 <= maximum_snapshots <= 8:
        raise ValueError("report snapshot count exceeds its bounded envelope")
    manifest = load_run(run, require_current_build=False)
    identity = TuningRunIdentityV1(manifest["run_id"], manifest["config_hash"],
                                  manifest["source_sha"], manifest["started_at_ns"])
    cutoff = time.time_ns()
    request_id = uuid.uuid4().hex
    status = {"version": "OWNER_REPORT_OPERATION_V1", "run_id": manifest["run_id"],
              "config_hash": manifest["config_hash"], "request_id": request_id,
              "started_at_ns": cutoff, "state": "RUNNING", "authority": "ZERO"}
    _publish(run / "report-status.json", status)
    # A fixed cutoff across bounded pages prevents report draining from
    # following an ever-growing live stream. Remaining work stays explicit.
    try:
        for _ in range(maximum_snapshots):
            result = export_tuning_snapshot(run / "ops.sqlite", run / "reports", identity, cutoff_ns=cutoff)
            if not result["has_more"] or result["blocked_future_evidence"]:
                break
        status.update(state="SUCCEEDED", completed_at_ns=time.time_ns(),
                      manifest_ref=sha256_json(result), has_more=result["has_more"],
                      validation_failures=sum(result["validation_failures"].values()))
        return result
    except Exception as error:
        status.update(state="FAILED", completed_at_ns=time.time_ns(), error_type=type(error).__name__)
        raise
    finally:
        _publish(run / "report-operations" / (request_id + ".json"), status, immutable=True)
        _publish(run / "report-status.json", status)


def preflight_run(run: Path) -> dict[str, Any]:
    """Bound the storage probe in a separate process before opening the writer.

    A hung filesystem call cannot be interrupted safely in a Python thread.
    This child owns only temporary probe data and never the actual run store.
    """
    manifest = load_run(run)
    attempt_id = uuid.uuid4().hex
    destination = run / "preflight-results" / (attempt_id + ".json")
    environment = {key: value for key, value in os.environ.items()
                   if not any(word in key.upper() for word in ("KEY", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL"))}
    environment["QT_QPA_PLATFORM"] = "offscreen"
    if not getattr(sys, "frozen", False):
        environment["PYTHONPATH"] = str(resource_file("src"))
    from .memory.writer_lock import OpsWriterLock

    lease = OpsWriterLock(run / "ops.sqlite")
    lease.acquire()
    try:
        try:
            completed = subprocess.run([*_component_command("preflight", run)], cwd=run,
                env=environment, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL, timeout=45, check=False)
            if len(completed.stdout) > MAX_CONFIG_BYTES:
                raise ValueError("preflight worker result exceeded its bound")
            result = json.loads(completed.stdout)
            body = {key: value for key, value in result.items() if key != "content_hash"}
            from .runtime.storage_preflight import PREFLIGHT_CONTRACT_V1, StoragePreflightLimitsV1

            expected_fields = {"schema_version", "version", "selected_path", "identity_sha256", "started_at_ns",
                "completed_at_ns", "elapsed_ns", "status", "allowed", "reasons", "limits", "measurements",
                "facts", "probe_mode", "authority", "capital_enabled", "assisted_enabled",
                "live_source_qualification", "endurance_qualification", "content_hash"}
            if (set(result) != expected_fields or type(result["schema_version"]) is not int
                    or result["schema_version"] != 1 or result["version"] != PREFLIGHT_CONTRACT_V1
                    or type(result["allowed"]) is not bool or result["capital_enabled"] is not False
                    or result["assisted_enabled"] is not False
                    or result["live_source_qualification"] != "TEST GATE"
                    or result["endurance_qualification"] != "TEST GATE"
                    or result["status"] not in {"TESTED", "TEST GATE"}
                    or result["limits"] != StoragePreflightLimitsV1().as_dict()
                    or not isinstance(result["facts"], dict) or not isinstance(result["measurements"], list)
                    or not isinstance(result["reasons"], list)
                    or any(not isinstance(reason, str) or not reason or len(reason) > 256
                           for reason in result["reasons"])
                    or result["allowed"] != (result["status"] == "TESTED" and not result["reasons"])
                    or any(type(result[field]) is not int or result[field] < 0
                           for field in ("started_at_ns", "completed_at_ns", "elapsed_ns"))
                    or result["completed_at_ns"] < result["started_at_ns"]
                    or completed.returncode not in {0, 2}):
                raise ValueError("preflight worker schema or disabled-authority mismatch")
            if (sha256_json(body) != result["content_hash"] or result["identity_sha256"] != manifest["content_hash"]
                    or result["selected_path"] != str(run.resolve()) or result["probe_mode"] != "HOST_PATH"
                    or result["authority"] != "ZERO" or result["allowed"] != (completed.returncode == 0)):
                raise ValueError("preflight worker binding or result mismatch")
        except Exception as error:
            reason = ("PREFLIGHT_HOST_WORKER_DEADLINE_EXCEEDED" if isinstance(error, subprocess.TimeoutExpired)
                      else "PREFLIGHT_HOST_WORKER_FAILED_" + type(error).__name__)
            body = {"version": "OWNER_PREFLIGHT_PROCESS_FAILURE_V1", "run_id": manifest["run_id"],
                    "identity_sha256": manifest["content_hash"], "selected_path": str(run.resolve()),
                    "status": "TEST GATE", "allowed": False, "reasons": [reason],
                    "observed_at_ns": time.time_ns(), "authority": "ZERO"}
            result = {**body, "content_hash": sha256_json(body)}
        host_result = result
        _publish(destination, host_result, immutable=True)
        config = _run_config(manifest["configuration"])
        if isinstance(config, ResearchRunConfigV2):
            from .runtime.storage_preflight import (
                S41_MAX_BREADTH_BYTES_PER_CYCLE_V2,
                assess_broad_storage_capacity_v2,
            )

            facts = host_result.get("facts", {})
            free_bytes = facts.get("free_disk_bytes") if isinstance(facts, dict) else None
            broad_capacity = assess_broad_storage_capacity_v2(
                identity_sha256=manifest["content_hash"],
                selected_path=str(run.resolve()),
                observed_at_ns=host_result.get("completed_at_ns", host_result.get("observed_at_ns", 0)),
                enabled_venues=config.public_venues,
                observed_free_bytes=free_bytes if type(free_bytes) is int else None,
                owner_free_disk_reserve_bytes=config.minimum_free_disk_bytes,
                measured_max_cycle_bytes=S41_MAX_BREADTH_BYTES_PER_CYCLE_V2)
            broad_capacity_body = broad_capacity.as_dict()
            _publish(run / "preflight-results" / (attempt_id + ".broad-capacity.json"),
                     broad_capacity_body, immutable=True)
            reasons = list(host_result.get("reasons", []))
            reasons.extend(reason for reason in broad_capacity_body["reasons"] if reason not in reasons)
            allowed = host_result.get("allowed") is True and broad_capacity_body["allowed"] is True
            wrapper = {
                "schema_version": 2,
                "version": "OwnerPreflightResultV2",
                "run_id": manifest["run_id"],
                "identity_sha256": manifest["content_hash"],
                "selected_path": str(run.resolve()),
                "attempt_id": attempt_id,
                "host_preflight_ref": host_result["content_hash"],
                "host_preflight": host_result,
                "broad_capacity_ref": broad_capacity_body["content_hash"],
                "broad_capacity": broad_capacity_body,
                "status": "TESTED" if allowed else "TEST GATE",
                "allowed": allowed,
                "reasons": reasons,
                "authority": "ZERO",
                "capital_enabled": False,
                "assisted_enabled": False,
            }
            result = {**wrapper, "content_hash": sha256_json(wrapper)}
            _publish(run / "preflight-results" / (attempt_id + ".owner.json"), result, immutable=True)
        _publish(run / "preflight.json", result)
        return result
    finally:
        lease.close()


def owner_storage_reserve_fields(run: Path, manifest: dict[str, Any], config: RunConfig) -> dict[str, Any]:
    """Bind live disk warnings to the current immutable run's preflight.

    V1 keeps the S40 reserve unchanged. V2 uses the durable broad-workload
    estimate; missing, stale, corrupt, or mismatched evidence becomes an
    explicit TEST GATE and resource pressure, never a smaller reserve.
    """
    from .runtime.storage_preflight import StoragePreflightLimitsV1

    v1_reserve = max(config.minimum_free_disk_bytes, StoragePreflightLimitsV1().minimum_free_bytes)
    if not isinstance(config, ResearchRunConfigV2):
        return {"disk_reserve_bytes": v1_reserve, "broad_capacity_status": "NOT_APPLICABLE"}

    def failed(code: str) -> dict[str, Any]:
        return {"disk_reserve_bytes": None, "broad_capacity_status": "TEST GATE",
                "broad_capacity_reason": code, "resource_pressure": True}

    try:
        if manifest.get("config_hash") != config.content_hash:
            return failed("BROAD_CAPACITY_RUN_CONFIG_MISMATCH")
        owner = _read_json(run / "preflight.json", maximum_bytes=MAX_CONFIG_BYTES)
        if not isinstance(owner, dict):
            return failed("BROAD_CAPACITY_PREFLIGHT_INVALID_OR_NOT_ALLOWED")
        owner_body = {key: value for key, value in owner.items() if key != "content_hash"}
        if (owner.get("version") != "OwnerPreflightResultV2"
                or owner.get("schema_version") != 2
                or sha256_json(owner_body) != owner.get("content_hash")
                or owner.get("run_id") != manifest["run_id"]
                or owner.get("identity_sha256") != manifest["content_hash"]
                or owner.get("selected_path") != str(run.resolve())
                or owner.get("allowed") is not True or owner.get("status") != "TESTED"
                or owner.get("authority") != "ZERO" or owner.get("capital_enabled") is not False
                or owner.get("assisted_enabled") is not False):
            return failed("BROAD_CAPACITY_PREFLIGHT_INVALID_OR_NOT_ALLOWED")
        attempt_id = owner.get("attempt_id")
        if not isinstance(attempt_id, str) or re.fullmatch(r"[0-9a-f]{32}", attempt_id) is None:
            return failed("BROAD_CAPACITY_PREFLIGHT_ATTEMPT_INVALID")
        host = owner.get("host_preflight")
        broad = owner.get("broad_capacity")
        if not isinstance(host, dict) or not isinstance(broad, dict):
            return failed("BROAD_CAPACITY_PREFLIGHT_EVIDENCE_MISSING")
        host_facts = host.get("facts")
        if not isinstance(host_facts, dict):
            return failed("BROAD_CAPACITY_HOST_PREFLIGHT_FACTS_INVALID")
        host_body = {key: value for key, value in host.items() if key != "content_hash"}
        broad_body = {key: value for key, value in broad.items() if key != "content_hash"}
        if (sha256_json(host_body) != host.get("content_hash")
                or owner.get("host_preflight_ref") != host.get("content_hash")
                or host.get("identity_sha256") != manifest["content_hash"]
                or host.get("selected_path") != str(run.resolve())
                or host.get("allowed") is not True or host.get("status") != "TESTED"):
            return failed("BROAD_CAPACITY_HOST_PREFLIGHT_BINDING_INVALID")
        if (sha256_json(broad_body) != broad.get("content_hash")
                or owner.get("broad_capacity_ref") != broad.get("content_hash")
                or broad.get("identity_sha256") != manifest["content_hash"]
                or broad.get("selected_path") != str(run.resolve())
                or broad.get("enabled_venues") != list(config.public_venues)
                or broad.get("observed_free_bytes") != host_facts.get("free_disk_bytes")
                or broad.get("allowed") is not True or broad.get("status") != "TESTED"
                or broad.get("capacity_qualified") is not False
                or broad.get("authority") != "ZERO" or broad.get("capital_enabled") is not False
                or broad.get("assisted_enabled") is not False):
            return failed("BROAD_CAPACITY_ASSESSMENT_BINDING_INVALID")
        files = run / "preflight-results"
        archived_host = _read_json(files / f"{attempt_id}.json", maximum_bytes=MAX_CONFIG_BYTES)
        archived_broad = _read_json(files / f"{attempt_id}.broad-capacity.json", maximum_bytes=MAX_CONFIG_BYTES)
        archived_owner = _read_json(files / f"{attempt_id}.owner.json", maximum_bytes=MAX_CONFIG_BYTES)
        if archived_host != host or archived_broad != broad or archived_owner != owner:
            return failed("BROAD_CAPACITY_SIDECAR_MISMATCH")
        required = broad.get("required_free_bytes")
        projected = broad.get("projected_workload_bytes")
        if (type(required) is not int or required < config.minimum_free_disk_bytes
                or type(projected) is not int or projected <= 0):
            return failed("BROAD_CAPACITY_RESERVE_MISSING")
        return {"disk_reserve_bytes": max(v1_reserve, required),
                "broad_capacity_status": "TESTED", "broad_capacity_reason": None,
                "broad_capacity_required_free_bytes": required,
                "broad_capacity_projection_bytes": projected,
                "resource_pressure": False}
    except (OSError, ValueError, TypeError, KeyError):
        return failed("BROAD_CAPACITY_PREFLIGHT_UNREADABLE")


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
    manifest = load_run(run)
    from .runtime.live_health import QualificationLatchV1

    if QualificationLatchV1(run, run_id=manifest["run_id"], config_hash=manifest["config_hash"]).read() is not None:
        raise RuntimeError("RUN_QUALIFICATION_FAILED_CREATE_NEW_RUN")
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


def _load_event_extraction_broker_context(run: Path, path: Path) -> dict[str, Any]:
    if path.is_symlink() or path.resolve().parent != (run / "epochs").resolve():
        raise ValueError("event extraction broker context must be inside this run's epoch directory")
    with path.open("rb") as handle:
        cipher = handle.read(65_537)
    if not cipher or len(cipher) > 65_536:
        raise ValueError("protected event extraction context exceeds its bound")
    context = json.loads(WindowsSecretStore._crypt(cipher, decrypt=True))
    fields = {"schema_version", "run_id", "epoch_id", "pipe_name", "authentication_key",
              "signing_key", "owner_process"}
    if not isinstance(context, dict) or set(context) != fields or context["schema_version"] != 1:
        raise ValueError("invalid event extraction broker context contract")
    from .agent_intelligence.event_extraction_transport import _pipe_name

    owner = context["owner_process"]
    if (not isinstance(owner, dict) or set(owner) != {"pid", "creation_filetime_ticks"}
            or type(owner["pid"]) is not int or not 0 < owner["pid"] <= 0xFFFFFFFF
            or type(owner["creation_filetime_ticks"]) is not int
            or not 0 < owner["creation_filetime_ticks"] <= 0xFFFFFFFFFFFFFFFF):
        raise ValueError("invalid event extraction owner identity")
    _pipe_name(context["pipe_name"])
    epoch = context["epoch_id"]
    if (context["run_id"] != run.name or not isinstance(epoch, str) or len(epoch) != 32
            or any(c not in "0123456789abcdef" for c in epoch)
            or path.name != epoch + "-event-extraction-broker.dpapi"):
        raise ValueError("event extraction context does not bind this run and epoch")
    for field in ("authentication_key", "signing_key"):
        value = context[field]
        if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
            raise ValueError("invalid protected event extraction capability key")
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


def run_event_extraction_broker(run: Path, context_path: Path, *,
                                stop_requested: Callable[[], bool]) -> int:
    """Dedicated S7 child; this component alone reads the OpenAI API key."""
    from .agent_intelligence.windows_broker import WindowsOwnerProcessV1

    manifest = load_run(run)
    if manifest["configuration"]["provider_profile"] != S7_EVENT_EXTRACTION_PROFILE:
        raise ValueError("this run did not authorize S7 event extraction")
    context = _load_event_extraction_broker_context(run, context_path)
    with WindowsOwnerProcessV1(context["owner_process"]) as owner:
        return _serve_event_extraction_broker(
            run, context, stop_requested=lambda: stop_requested() or not owner.alive())


def _serve_event_extraction_broker(run: Path, context: dict[str, Any], *,
                                   stop_requested: Callable[[], bool]) -> int:
    from .agent_intelligence.event_extraction_transport import (
        EVENT_EXTRACTION_PROFILE_HASH_V1,
        EventExtractionBroker,
        OpenAIResponsesEventExtractionProvider,
        WindowsEventExtractionBrokerServer,
    )

    if stop_requested():
        return 0
    key = secret_store_for_run(load_run(run)).get_provider_key(S7_EVENT_EXTRACTION_PROFILE)
    client = _openai_event_extraction_client(key)
    del key
    broker = EventExtractionBroker(OpenAIResponsesEventExtractionProvider(client),
        signing_key=bytes.fromhex(context["signing_key"]))
    server = WindowsEventExtractionBrokerServer(context["pipe_name"], broker,
        authentication_key=bytes.fromhex(context["authentication_key"]))
    status_path = run / "epochs" / (context["epoch_id"] + "-event-extraction-broker-status.json")
    stop_path = run / "epochs" / (context["epoch_id"] + "-event-extraction-broker-stop.json")
    try:
        server.start()
        while not stop_requested():
            _publish(status_path, {"run_id": run.name, "epoch_id": context["epoch_id"],
                "profile_hash": EVENT_EXTRACTION_PROFILE_HASH_V1, "pid": os.getpid(),
                "observed_at_ns": time.time_ns(), "health": server.health(),
                "provider_conformance": "TEST GATE", "authority": "ZERO"})
            if stop_path.exists() and _read_json(stop_path) == {
                    "run_id": run.name, "epoch_id": context["epoch_id"]}:
                break
            time.sleep(0.25)
        return 0
    finally:
        server.close()
        client.close()


def _openai_event_extraction_client(api_key: str) -> Any:
    """Construct a no-retry Responses client that ignores ambient proxy variables."""
    import httpx
    from openai import OpenAI

    http_client = httpx.Client(trust_env=False)
    return OpenAI(api_key=api_key, max_retries=0, timeout=35.0, http_client=http_client)


def _provider_child_environment() -> dict[str, str]:
    """Remove credentials and ambient proxy routing from isolated provider children."""
    return {key: value for key, value in os.environ.items()
        if not any(word in key.upper() for word in (
            "KEY", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL", "_PROXY"))}


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
    credential = secret_store_for_run(load_run(run)).get_provider_key("deepseek-v41-action-critic-v1")
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


@dataclass
class _InstalledEventExtractionRuntime:
    process: subprocess.Popen[bytes]
    provider: Any
    run: Path
    epoch_id: str

    def close(self) -> None:
        try:
            _publish(self.run / "epochs" / (self.epoch_id + "-event-extraction-broker-stop.json"),
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


def _start_installed_critic(run: Path, epoch_id: str, *, service: Callable[[], None] = lambda: None) -> _InstalledCriticRuntime:
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
            service()
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


def _start_installed_event_extraction(run: Path, epoch_id: str, *,
                                      service: Callable[[], None] = lambda: None) -> _InstalledEventExtractionRuntime:
    from .agent_intelligence.event_extraction_transport import (
        EventExtractionBrokerClientProvider,
        WindowsEventExtractionClientPort,
    )
    from .agent_intelligence.windows_broker import current_owner_identity

    context: dict[str, Any] = {"schema_version": 1, "run_id": run.name, "epoch_id": epoch_id,
        "owner_process": current_owner_identity(),
        "pipe_name": "\\\\.\\pipe\\AtlasEventExtract-" + str(uuid.uuid4()),
        "authentication_key": os.urandom(32).hex(), "signing_key": os.urandom(32).hex()}
    context_path = run / "epochs" / (epoch_id + "-event-extraction-broker.dpapi")
    cipher = WindowsSecretStore._crypt(canonical_json(context).encode(), decrypt=False)
    with context_path.open("xb") as handle:
        handle.write(cipher)
        handle.flush()
        os.fsync(handle.fileno())
    environment = _provider_child_environment()
    if not getattr(sys, "frozen", False):
        environment["PYTHONPATH"] = str(resource_file("src"))
    process = subprocess.Popen([*_component_command("event-extraction-broker", run),
        "--broker-context", str(context_path)], cwd=run, env=environment,
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        from .agent_intelligence.event_extraction_transport import EVENT_EXTRACTION_PROFILE_HASH_V1

        deadline = time.monotonic() + 15
        status_path = run / "epochs" / (epoch_id + "-event-extraction-broker-status.json")
        while not status_path.is_file():
            service()
            if process.poll() is not None or time.monotonic() >= deadline:
                raise RuntimeError("configured S7 event extraction broker did not become ready")
            time.sleep(0.05)
        status = _read_json(status_path)
        if (status.get("run_id") != run.name or status.get("epoch_id") != epoch_id
                or status.get("profile_hash") != EVENT_EXTRACTION_PROFILE_HASH_V1
                or status.get("health", {}).get("closed") is not False):
            raise ValueError("event extraction broker readiness identity mismatch")
        port = WindowsEventExtractionClientPort(context["pipe_name"],
            authentication_key=bytes.fromhex(context["authentication_key"]))
        provider = EventExtractionBrokerClientProvider(port,
            signing_key=bytes.fromhex(context["signing_key"]))
        return _InstalledEventExtractionRuntime(process, provider, run, epoch_id)
    except BaseException:
        process.terminate()
        _stop_child(process)
        raise


def run_component(run: Path, *, smoke: bool = False, stop_requested: Callable[[], bool] = lambda: False) -> int:
    from .runtime.active_history import ActiveHistoryMaintenanceV1
    from .runtime.official_calendar import OfficialCalendarMaintenanceV1
    from .runtime.ops_supervisor import OpsSupervisorV2
    from .runtime.production import create_bybit_public_ws_port, create_production_port
    from .runtime.public_context import PublicContextMaintenanceV1
    from .runtime.read_only_report_worker import ReadOnlyReportWorkerV1
    from .runtime.research_model_shadow import StatisticalResearchShadowV1
    from .runtime.research_prediction_outcomes import ResearchPredictionOutcomeMaintenanceV1

    manifest = load_run(run)
    config = _run_config(manifest["configuration"])
    # Startup inventory precedes network capture; live monitoring never walks
    # an elapsed-runtime-growing evidence tree. Failure to inventory is visible.
    inventory_started = time.monotonic()
    baseline_footprint = 0
    baseline_database = baseline_wal = 0
    inventory_complete = True
    for index, path in enumerate(run.rglob("*")):
        if index >= 32_768 or time.monotonic() - inventory_started > 2:
            inventory_complete = False
            break
        if path.is_file() and not path.is_symlink():
            size = path.stat().st_size
            baseline_footprint += size
            if path.name == "ops.sqlite":
                baseline_database = size
            elif path.name == "ops.sqlite-wal":
                baseline_wal = size
    if smoke:
        port = create_production_port()
    elif isinstance(config, ResearchRunConfigV2):
        from .instruments import VenueV2
        from .runtime.production import create_broad_public_port

        port = create_broad_public_port(enabled_venues=tuple(VenueV2(venue) for venue in config.public_venues))
    else:
        port = create_bybit_public_ws_port()
    if not smoke and (isinstance(config, ResearchRunConfigV2) or port.public_stream_source is not None):
        qualified = preflight_run(run)
        if not qualified["allowed"]:
            raise RuntimeError("OWNER_STORAGE_PREFLIGHT_REJECTED")
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
    event_extraction: _InstalledEventExtractionRuntime | None = None
    official_calendar: OfficialCalendarMaintenanceV1 | None = None
    history_maintenance = ActiveHistoryMaintenanceV1(run / "ops-observations")
    # One incremental snapshot per periodic job keeps the reader lifetime and
    # clean-stop join independent of the total campaign backlog. Manual/final
    # export can drain up to eight snapshots with a fixed cutoff.
    report_worker = ReadOnlyReportWorkerV1(lambda: export_run(run, maximum_snapshots=1))
    final_report_available = True
    health_monitor: Any = None

    def progress_snapshot() -> dict[str, Any]:
        broad_progress = getattr(port.public_stream_source, "progress_snapshot", None)
        if callable(broad_progress):
            result = broad_progress()
            from .runtime.live_health import terminal_database_integrity_failure_v1

            result["database_integrity_failed"] = bool(state.get("database_integrity_failed"))
            if terminal_database_integrity_failure_v1(state.get("failure_types", ())):
                result["database_integrity_failed"] = True
            return result
        reports = tuple(port._owner_stream_reports.values())
        at = time.time_ns()
        current = bool(reports) and all(report.source_current and
            at - report.as_of_ns <= 3_000_000_000 for report in reports)
        from .runtime.live_health import terminal_database_integrity_failure_v1

        return {"observed_at_ns": port._stream_last_service_at_ns or state.get("observed_at_ns", started),
                "stream_source_state": "HEALTHY_CURRENT" if current else "INCOMPLETE_SNAPSHOT",
                "stream_recovery_required": not reports or any(
                    not report.book_sequence_valid for report in reports if report.channel.startswith("orderbook.")),
                "stream_unresolved_gap": any(report.gap_count for report in reports),
                "stream_service_duration_seconds": port._stream_last_service_duration_ns / 1e9,
                "stream_service_gap_seconds": (max(0, at - port._stream_last_service_at_ns) / 1e9
                    if port._stream_last_service_at_ns is not None else None),
                "stream_ingestion_failed": port._stream_ingestion_failed,
                "database_integrity_failed": (bool(state.get("database_integrity_failed"))
                    or terminal_database_integrity_failure_v1(state.get("failure_types", ())))}

    def monitored_resources() -> dict[str, Any]:
        values = resource_sample(run)
        values["db_bytes"] = (run / "ops.sqlite").stat().st_size
        wal = run / "ops.sqlite-wal"
        values["wal_bytes"] = wal.stat().st_size if wal.exists() else 0
        source = port.public_stream_source
        capture = getattr(source.status(), "capture", {}) if source is not None else {}
        archive = supervisor.repository._public_extent_writer if supervisor.repository is not None else None
        values["current_footprint_bytes"] = (max(0, baseline_footprint - baseline_database - baseline_wal)
            + values["db_bytes"] + values["wal_bytes"] + capture.get("archive_bytes_written", 0)
            + capture.get("receipt_bytes_written", 0) + getattr(archive, "bytes_written", 0)
            if inventory_complete else None)
        values["footprint_scope"] = "STARTUP_INVENTORY_PLUS_CURRENT_DB_WAL_ARCHIVE_AND_RECEIPT_COUNTERS"
        values["resource_pressure"] = not inventory_complete
        storage_fields = owner_storage_reserve_fields(run, manifest, config)
        values["disk_reserve_bytes"] = storage_fields["disk_reserve_bytes"]
        values["broad_capacity_status"] = storage_fields["broad_capacity_status"]
        values["broad_capacity_reason"] = storage_fields.get("broad_capacity_reason")
        if storage_fields.get("broad_capacity_required_free_bytes") is not None:
            values["broad_capacity_required_free_bytes"] = storage_fields["broad_capacity_required_free_bytes"]
            values["broad_capacity_projection_bytes"] = storage_fields["broad_capacity_projection_bytes"]
        # A missing/corrupt V2 estimate is an explicit attention state; it can
        # never silently fall back to the smaller S39 reserve.
        values["resource_pressure"] = bool(values["resource_pressure"] or storage_fields.get("resource_pressure"))
        return values

    def monitored_report() -> dict[str, Any]:
        path = run / "report-status.json"
        if not path.exists():
            return {"state": "IDLE"}
        body = _read_json(path)
        if body.get("run_id") != manifest["run_id"] or body.get("config_hash") != manifest["config_hash"]:
            raise ValueError("report status belongs to another run")
        return body

    def publish_runtime_telemetry(repo: Any, *, final: bool = False) -> None:
        from .memory.repository import ArtifactIndexEntryV2

        at_ns = time.time_ns()
        telemetry = {"schema_version": 1, "run_id": manifest["run_id"],
                     "config_hash": manifest["config_hash"], "epoch_id": epoch_id,
                     "available_at_ns": at_ns, "source_health": state.get("source_health", "UNVERIFIED"),
                     "cycle_ref": state.get("last_cycle_ref"), **resource_sample(run),
                     "db_bytes": (run / "ops.sqlite").stat().st_size,
                     "wal_bytes": (run / "ops.sqlite-wal").stat().st_size
                         if (run / "ops.sqlite-wal").exists() else 0,
                     "owner_live_health": health_monitor.latest if health_monitor is not None else None,
                     "persistence": repo.persistence_metrics(), "final_observation": final, "authority": "ZERO"}
        ref = sha256_json(telemetry)
        repo.register_artifact(ArtifactIndexEntryV2(
            ref, "ResearchRunTelemetryV1", ref, at_ns, at_ns, {"telemetry": telemetry}))

    def post_cycle(repo: Any, at_ns: int) -> Any:
        nonlocal target_registered
        if public_context is not None:
            public_context.run_cycle(repo, information_cutoff_ns=at_ns)
            supervisor.service_public_stream()
        if official_calendar is not None:
            calendar_status = official_calendar.run_cycle(repo, information_cutoff_ns=at_ns)
            state["official_calendar"] = {key: value for key, value in calendar_status.items()
                if key not in {"coverage", "events"}}
            supervisor.service_public_stream()
        if not smoke:
            history_maintenance.run_cycle(repo, cutoff_ns=at_ns, service=supervisor.service_public_stream)
            supervisor.service_public_stream()
        if not target_registered:
            prediction_maintenance.register_target(repo, available_at_ns=manifest["started_at_ns"])
            target_registered = True
        prediction_maintenance.run_cycle(repo, evidence_cutoff_ns=at_ns)
        supervisor.service_public_stream()
        from .runtime.frozen_action_comparison import FrozenActionComparisonMaintenanceV1

        FrozenActionComparisonMaintenanceV1(repo).run_one()
        supervisor.service_public_stream()
        research_maintenance = getattr(port, "run_research_maintenance", None)
        if callable(research_maintenance):
            research_maintenance(repo, cutoff_ns=at_ns)
            supervisor.service_public_stream()
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
            if port.public_stream_source is not None and supervisor.repository is not None:
                from .runtime.owner_health_monitor import OwnerHealthMonitorV1

                health_monitor = OwnerHealthMonitorV1(run, run_id=manifest["run_id"], config_hash=manifest["config_hash"],
                    source=port.public_stream_source, progress=progress_snapshot,
                    persistence=supervisor.repository.persistence_metrics, resources=monitored_resources,
                    report=monitored_report, publish=_publish)
                health_monitor.start()
            if not smoke:
                if config.provider_profile == S7_EVENT_EXTRACTION_PROFILE:
                    event_extraction = _start_installed_event_extraction(
                        run, epoch_id, service=supervisor.service_public_stream)
                    state["provider_health"] = "IMPLEMENTED"
                public_context = PublicContextMaintenanceV1(
                    event_extraction_provider=(event_extraction.provider
                        if event_extraction is not None else None))
                from .runtime.official_calendar import OfficialFomcSourceProfileV1

                fomc_profile = (OfficialFomcSourceProfileV1.from_dict(json.loads(config.official_fomc_source_profile_json))
                    if isinstance(config, ResearchRunConfigV2) and config.official_fomc_source_profile_json is not None else None)
                official_calendar = OfficialCalendarMaintenanceV1(fomc_source_profile=fomc_profile)
            if isinstance(config, ResearchRunConfigV2) and config.owner_pair_configuration_hash is not None:
                from .runtime.full_strategy_surface import register_s8_pair_catalog
                from .runtime.owner_pairs import read_owner_pair_configuration

                pair_configuration = read_owner_pair_configuration(run / "owner-pairs.json")
                if pair_configuration.content_hash != config.owner_pair_configuration_hash:
                    raise ValueError("immutable owner pair configuration drift")
                if supervisor.repository is None:
                    raise ValueError("owner pair configuration requires the sole evidence writer")
                existing_catalog = supervisor.repository.latest_artifact_entries(
                    "S8PairOwnerCatalogV1", as_of_ns=time.time_ns(), limit=1)
                if existing_catalog.invalid_entry_count:
                    raise ValueError("owner pair catalog is invalid")
                if existing_catalog.entries:
                    registered = existing_catalog.entries[0]
                    catalog = registered.metadata.get("catalog")
                    expected_pairs = [pair.to_dict() for pair in sorted(pair_configuration.pairs,
                        key=lambda pair: pair.pair_id)]
                    if (not isinstance(catalog, Mapping)
                            or catalog.get("owner_id") != pair_configuration.owner_id
                            or canonical_json(catalog.get("pairs")) != canonical_json(expected_pairs)
                            or catalog.get("authority") != "RESEARCH_CONFIGURATION_ONLY"
                            or catalog.get("capital_authority") != "ZERO"
                            or sha256_json(catalog) != registered.content_hash
                            or registered.artifact_ref != registered.content_hash):
                        raise ValueError("registered owner pair catalog differs from the immutable run")
                else:
                    register_s8_pair_catalog(supervisor.repository, owner_id=pair_configuration.owner_id,
                        pairs=pair_configuration.pairs, available_at_ns=time.time_ns())
            statistical = StatisticalResearchShadowV1(run_id=manifest["run_id"],
                config_hash=manifest["config_hash"], source_sha=manifest["source_sha"],
                environment_lock_hash=manifest["research_model_configuration"]["manifest"]["environment_lock_hash"],
                archive_root=run / "ops-observations")
            supervisor.post_receipt_shadow = _ResearchShadows(statistical)
            if supervisor.repository is not None and not target_registered:
                prediction_maintenance.register_target(supervisor.repository,
                    available_at_ns=manifest["started_at_ns"])
                target_registered = True
            if config.provider_profile == "deepseek-v41-action-critic-v1":
                try:
                    critic = _start_installed_critic(run, epoch_id, service=supervisor.service_public_stream)
                    supervisor.post_receipt_shadow = _ResearchShadows(statistical, critic.shadow)
                    state["provider_health"] = "IMPLEMENTED"
                except Exception as error:
                    state.update(provider_health="TEST GATE", provider_reason="CONFIGURED_PROVIDER_STARTUP_FAILED",
                                 provider_error_type=type(error).__name__)
            while True:
                if stop_requested():
                    state.update(status="IMPLEMENTED", reason="PROCESS_STOP_REQUESTED")
                    break
                if critic is not None and critic.process.poll() is not None:
                    state.update(provider_reason="CONFIGURED_PROVIDER_BROKER_LOST", provider_health="TEST GATE")
                    # Keep collecting public evidence; configured critic
                    # failures remain explicit terminal side-branch evidence.
                    # No alternate provider/model is selected.
                    supervisor.post_receipt_shadow = _ResearchShadows(statistical)
                    if supervisor.repository is not None:
                        critic.shadow.abandon_pending(repository=supervisor.repository)
                    critic.shadow.close()
                    critic = None
                if event_extraction is not None and event_extraction.process.poll() is not None:
                    state.update(provider_reason="S7_EVENT_EXTRACTION_BROKER_LOST", provider_health="TEST GATE")
                    event_extraction.close()
                    event_extraction = None
                if health_monitor is not None and health_monitor.latest is not None and (
                        health_monitor.latest["assessment"]["qualification_failed"]):
                    state.update(status="TEST GATE", reason="RUN_QUALIFICATION_FAILED_STOP_AND_EXPORT")
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
                from .runtime.live_health import terminal_database_integrity_failure_v1

                state["database_integrity_failed"] = (bool(state.get("database_integrity_failed"))
                    or terminal_database_integrity_failure_v1(result.cycle.failure_types))
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
                completed_report = report_worker.poll()
                if completed_report is not None:
                    state["report_status"] = completed_report.status
                    state["report_error_type"] = completed_report.error_type
                if time.monotonic() - last_report >= config.report_interval_seconds and report_worker.start():
                    last_report = time.monotonic()
                if supervisor.repository is not None and time.monotonic() - last_telemetry >= 60:
                    publish_runtime_telemetry(supervisor.repository)
                    checkpoint = supervisor.repository.checkpoint()
                    state["wal_checkpoint"] = list(checkpoint)
                    last_telemetry = time.monotonic()
                # Service forward evidence during the existing idle allowance.
                # This remains the supervisor's sole writer thread.
                idle_until = time.monotonic() + 1.0
                while time.monotonic() < idle_until and not stop_requested():
                    supervisor.service_public_stream()
                    time.sleep(min(0.02, max(0.0, idle_until - time.monotonic())))
        except Exception as exc:
            # Persist only a closed error class, never exception/provider text.
            from .runtime.live_health import database_exception_is_integrity_failure_v1

            state["database_integrity_failed"] = (bool(state.get("database_integrity_failed"))
                or database_exception_is_integrity_failure_v1(exc))
            state.update(status="TEST GATE", reason="RUNTIME_COMPONENT_FAILED", error_type=type(exc).__name__)
            result_code = 2
        finally:
            if supervisor.repository is not None:
                try:
                    port.finish_public_capture(supervisor.repository)
                except Exception as error:
                    port._stream_ingestion_failed = True
                    from .runtime.live_health import database_exception_is_integrity_failure_v1

                    state["database_integrity_failed"] = (bool(state.get("database_integrity_failed"))
                        or database_exception_is_integrity_failure_v1(error))
                    state.update(status="TEST GATE", reason="PUBLIC_CAPTURE_SHUTDOWN_FAILED", error_type=type(error).__name__)
                    result_code = 2
            if health_monitor is not None:
                try:
                    health_monitor.close()
                except Exception as error:
                    state.update(status="TEST GATE", reason="OWNER_HEALTH_MONITOR_SHUTDOWN_FAILED",
                                 error_type=type(error).__name__)
                    result_code = 2
            final_report_available = report_worker.close(timeout_s=10.0)
            if public_context is not None:
                public_context.close()
            if official_calendar is not None:
                official_calendar.close()
            if critic is not None:
                try:
                    try:
                        if supervisor.repository is not None:
                            critic.shadow.abandon_pending(repository=supervisor.repository)
                    finally:
                        critic.close()
                except Exception as exc:
                    state.update(status="TEST GATE", reason="PROVIDER_SHUTDOWN_FAILED", error_type=type(exc).__name__)
                    result_code = 2
            if event_extraction is not None:
                try:
                    event_extraction.close()
                except Exception as exc:
                    state.update(status="TEST GATE", reason="S7_EVENT_EXTRACTION_SHUTDOWN_FAILED",
                                 error_type=type(exc).__name__)
                    result_code = 2
            if supervisor.repository is not None:
                try:
                    # A terminal capture fault may occur between minute-scale
                    # samples. Preserve the final typed watchdog/latch binding
                    # in the tuning store after capture stops, on the sole writer.
                    publish_runtime_telemetry(supervisor.repository, final=True)
                except Exception as exc:
                    state.update(status="TEST GATE", reason="FINAL_RUNTIME_TELEMETRY_FAILED",
                                 error_type=type(exc).__name__)
                    result_code = 2
            state.update(stopped_at_ns=time.time_ns(), observed_at_ns=time.time_ns())
            _publish(run / "status.json", state)
            _publish(run / "epochs" / (epoch_id + "-stop.json"), state, immutable=True)
    try:
        if final_report_available:
            export_run(run)
        else:
            _publish(run / "report-failure.json", {"status": "TEST GATE",
                "reason": "READ_ONLY_REPORT_STILL_RUNNING_AT_SHUTDOWN", "run_id": manifest["run_id"]})
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
            f"Current public HTTP/context: {state.get('source_health', 'UNVERIFIED')}\n"
            f"Provider: {state.get('provider_health', 'DISABLED')}\n"
            f"Decisions in last cycle: {state.get('decision_count', 'UNVERIFIED')}\n"
            f"Disk free: {state.get('disk_free_bytes', 'UNVERIFIED')} bytes; "
            f"DB/WAL: {state.get('db_bytes', 'UNVERIFIED')}/{state.get('wal_bytes', 'UNVERIFIED')} bytes\n"
            f"Reason: {reason}\nCapital disabled.")


def _owner_operator_details(assessment: dict[str, Any], *, now_ns: int,
                            facts: dict[str, Any] | None = None,
                            source_states: dict[str, Any] | None = None,
                            runtime: dict[str, Any] | None = None) -> list[str]:
    lines: list[str] = []
    if isinstance(facts, dict):
        observed = facts.get("observed_at_ns")
        if type(observed) is int and observed <= now_ns:
            age = (now_ns - observed) // 1_000_000_000
            lines.append(f"Process/watchdog: responding; health snapshot {age}s old")
        else:
            lines.append("Process/watchdog: health heartbeat unavailable or clock conflict")
        failed = bool(assessment.get("qualification_failed"))
        lines.append("Run validity: FAILED and latched" if failed else
                     "Run validity: no permanent failure observed; live qualification is still pending")
        if isinstance(source_states, dict) and source_states:
            lanes = [f"{name} {state}" for name, state in sorted(source_states.items())[:8]
                     if isinstance(name, str) and isinstance(state, str)]
            if lanes:
                lines.append("Public data: " + "; ".join(lanes))
        else:
            lines.append("Public data: " + str(facts.get("source_state", "UNAVAILABLE")))
        queue = facts.get("queue_items")
        queue_cap = facts.get("queue_capacity")
        queue_bytes = facts.get("queue_bytes")
        queue_byte_cap = facts.get("queue_capacity_bytes")
        queue_text = (f"{queue}/{queue_cap} items" if type(queue) is int and type(queue_cap) is int
                      else "queue unavailable")
        if type(queue_bytes) is int and type(queue_byte_cap) is int:
            queue_text += f", {queue_bytes}/{queue_byte_cap} bytes"
        pending, pending_cap = facts.get("capture_pending_batches"), facts.get("capture_max_pending_batches")
        if type(pending) is int and type(pending_cap) is int:
            queue_text += f", capture {pending}/{pending_cap} batches"
        lines.append("Queue/capture: " + queue_text)
        free, reserve, wal = facts.get("free_disk_bytes"), facts.get("disk_reserve_bytes"), facts.get("wal_bytes")
        storage = (f"free {free} bytes" if type(free) is int else "free space unavailable")
        if type(reserve) is int:
            storage += f", reserve estimate {reserve} bytes"
        if type(wal) is int:
            storage += f", WAL {wal} bytes"
        lines.append("Storage: " + storage)
        report = facts.get("report_state", "UNKNOWN")
        validation_failures = facts.get("evidence_validation_failures", 0)
        lines.append(f"Report/export: {report}; validation failures {validation_failures}")
        reasons = assessment.get("reasons", [])
        if isinstance(reasons, list) and reasons:
            lines.append("Signals: " + ", ".join(str(reason)[:64] for reason in reasons[:12]))
    else:
        lines.extend(("Process/watchdog: health heartbeat unavailable",
                      "Run validity: health evidence unavailable"))
    if isinstance(runtime, dict):
        provider = runtime.get("provider_health", "UNKNOWN")
        if isinstance(provider, str):
            detail = runtime.get("provider_reason")
            provider_text = provider[:48]
            if isinstance(detail, str) and detail.isascii() and detail.replace("_", "").isalnum():
                provider_text += f" ({detail[:64]})"
            lines.append("Provider/critic: " + provider_text)
    return lines[:9]


def owner_live_indicator(run: Path, *, now_ns: int) -> dict[str, Any]:
    """Read-only desktop assessment; current HTTP health cannot clear a latch."""
    from .runtime.live_health import (
        LiveHealthFactsV1,
        LiveHealthPolicyV1,
        QualificationLatchError,
        QualificationLatchV1,
        assess_live_health,
    )

    identity = load_run(run, require_current_build=False)
    from .data.health import PublicSourceStateV2

    try:
        runtime = _read_json(run / "status.json", maximum_bytes=65_536)
    except (OSError, ValueError, TypeError):
        runtime = None

    def attach_details(result: dict[str, Any], *, facts: dict[str, Any] | None = None,
                       source_states: dict[str, Any] | None = None) -> dict[str, Any]:
        result["operator_details"] = _owner_operator_details(
            result, now_ns=now_ns, facts=facts, source_states=source_states, runtime=runtime)
        return result

    # The durable run failure outranks a missing, stale or damaged UI snapshot.
    # Desktop assessment has no latch publication or SQLite write authority.
    fallback = LiveHealthFactsV1(identity["run_id"], identity["config_hash"], now_ns, None, None,
        "CREATED", False, PublicSourceStateV2.INCOMPLETE_SNAPSHOT, True, 0, 512, 0)
    try:
        latch = QualificationLatchV1(run, run_id=identity["run_id"], config_hash=identity["config_hash"]).read()
    except QualificationLatchError as error:
        return attach_details(assess_live_health(fallback, latch_error=str(error), at_ns=now_ns).to_dict())
    if latch is not None:
        return attach_details(assess_live_health(latch.facts, latch=latch, at_ns=now_ns).to_dict(),
                              facts=latch.facts.to_dict())
    try:
        projection = _read_json(run / "live-health.json", maximum_bytes=65_536)
    except (OSError, ValueError):
        assessment = assess_live_health(fallback, at_ns=now_ns).to_dict()
        preflight = run / "preflight.json"
        if preflight.exists():
            result = _read_json(preflight)
            if result.get("allowed") is False and result.get("identity_sha256") == identity["content_hash"]:
                assessment["guidance"] = "Run preflight failed: " + ", ".join(result.get("reasons", [])[:3])
        return attach_details(assessment)
    facts = LiveHealthFactsV1.from_dict(projection["facts"])
    if facts.run_id != identity["run_id"] or facts.config_hash != identity["config_hash"]:
        raise ValueError("operator health projection belongs to another immutable run")
    policy_body = dict(projection["policy"])
    if policy_body.pop("schema_version") != 1:
        raise ValueError("operator health policy version unsupported")
    policy = LiveHealthPolicyV1(**policy_body)
    assessment = assess_live_health(facts, policy=policy, at_ns=now_ns).to_dict()
    source_states = projection.get("source_states", {})
    if not isinstance(source_states, dict):
        source_states = {}
    return attach_details(assessment, facts=facts.to_dict(), source_states=source_states)


def desktop(*, smoke: bool = False, data_root: Path | None = None) -> int:
    from concurrent.futures import ThreadPoolExecutor

    from PySide6.QtCore import QTimer
    from PySide6.QtWidgets import (
        QApplication,
        QCheckBox,
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
    root_edit = QLineEdit(str(data_root if data_root is not None else default_data_root()))
    layout.addWidget(QLabel("Research data location"))
    layout.addWidget(root_edit)
    browse = QPushButton("Choose folder")
    layout.addWidget(browse)
    public_bybit = QCheckBox("Collect public Bybit")
    public_bybit.setChecked(True)
    public_binance = QCheckBox("Collect public Binance")
    layout.addWidget(public_bybit)
    layout.addWidget(public_binance)
    execution_venue = QComboBox()
    execution_venue.addItem("Bybit", "BYBIT")
    execution_venue.addItem("Binance", "BINANCE")
    layout.addWidget(QLabel("Selected demo/test execution venue"))
    layout.addWidget(execution_venue)
    execution_environment = QComboBox()
    execution_environment.addItem("Demo", "DEMO")
    execution_environment.addItem("Testnet", "TESTNET")
    layout.addWidget(QLabel("Execution environment"))
    layout.addWidget(execution_environment)
    execution_profile = QComboBox()
    execution_profile.addItem("Disabled", "DISABLED")
    execution_profile.addItem("Demo/test account checks", "DEMO_READ_ONLY_QUALIFICATION")
    execution_profile.addItem("Demo/test native OMS qualification", "DEMO_NATIVE_OMS_QUALIFICATION")
    layout.addWidget(QLabel("Demo/test qualification profile for a new run"))
    layout.addWidget(execution_profile)
    execution_symbol = QLineEdit()
    layout.addWidget(QLabel("Native demo/test symbol (required for OMS qualification)"))
    layout.addWidget(execution_symbol)
    account_scope = QLineEdit()
    capability_ref = QLineEdit()
    credential_ref = QLineEdit()
    for label, edit in (("Opaque account-scope reference (optional)", account_scope),
                        ("Opaque capability-profile reference (optional)", capability_ref),
                        ("Opaque demo credential reference (optional)", credential_ref)):
        layout.addWidget(QLabel(label))
        layout.addWidget(edit)
    runs = QComboBox()
    layout.addWidget(QLabel("Research runs"))
    layout.addWidget(runs)
    provider = QComboBox()
    provider.addItem("Intelligence disabled", "DISABLED")
    provider.addItem("Optional DeepSeek V4.1 Flash action critic", "deepseek-v41-action-critic-v1")
    provider.addItem("Optional OpenAI GPT-6 Astra S7 event extraction", S7_EVENT_EXTRACTION_PROFILE)
    layout.addWidget(QLabel("Intelligence configuration for new runs"))
    layout.addWidget(provider)
    owner_pairs_edit = QLineEdit()
    owner_pairs_edit.setReadOnly(True)
    owner_pairs_button = QPushButton("Select optional S8 research pair configuration")
    layout.addWidget(owner_pairs_button)
    layout.addWidget(owner_pairs_edit)
    calendar_profile_edit = QLineEdit()
    calendar_profile_edit.setReadOnly(True)
    calendar_profile_button = QPushButton("Select optional official FOMC source profile")
    layout.addWidget(calendar_profile_button)
    layout.addWidget(calendar_profile_edit)
    status_label = QLabel("Capital and assisted execution disabled. Public collection requires no venue credentials.")
    status_label.setWordWrap(True)
    layout.addWidget(status_label)
    health_label = QLabel("ATTENTION — live-test health evidence is unavailable.")
    health_label.setWordWrap(True)
    health_label.setStyleSheet("font-weight: bold; font-size: 16px; color: #ad6500;")
    layout.addWidget(health_label)
    health_details_label = QLabel("Per-source, queue, storage, report and provider details appear when health telemetry is available.")
    health_details_label.setWordWrap(True)
    layout.addWidget(health_details_label)
    layout.addWidget(QLabel(development_gate()["verdict"]))
    buttons = {name: QPushButton(name) for name in ("Create run", "Storage preflight", "Start / resume", "Stop", "Export report")}
    for button in buttons.values():
        layout.addWidget(button)
    secret_button = QPushButton("Store selected provider key securely")
    layout.addWidget(secret_button)
    venue_secret_button = QPushButton("Configure demo/test credentials securely")
    layout.addWidget(venue_secret_button)
    credential_status_label = QLabel("Demo/test credential status: NOT CONFIGURED")
    layout.addWidget(credential_status_label)
    selected_run_configuration_label = QLabel(
        "Selected immutable run configuration: no run selected. The controls above set choices for a new run.")
    selected_run_configuration_label.setWordWrap(True)
    layout.addWidget(selected_run_configuration_label)
    alerts_label = QLabel("Recent public alerts: no selected run")
    alerts_label.setWordWrap(True)
    layout.addWidget(alerts_label)
    demo_check_button = QPushButton("Check selected demo/test account")
    layout.addWidget(demo_check_button)
    demo_start_button = QPushButton("Start selected demo/test OMS")
    demo_stop_button = QPushButton("Stop selected demo/test OMS")
    layout.addWidget(demo_start_button)
    layout.addWidget(demo_stop_button)
    layout.addWidget(QLabel("Provider use requires an explicitly selected run configuration and protected key. Live provider conformance remains a TEST GATE."))
    reports = ThreadPoolExecutor(max_workers=1, thread_name_prefix="atlas-report")
    pending_report: Any = None
    pending_preflight: Any = None
    pending_demo_check: Any = None
    last_selected_details_refresh = float("-inf")

    def refresh_credential_status() -> None:
        reference = credential_ref.text().strip()
        if not reference or _OPAQUE_REFERENCE_RE.fullmatch(reference) is None:
            credential_status_label.setText("Demo/test credential status: NOT CONFIGURED")
            return
        try:
            store = WindowsSecretStore(Path(root_edit.text()).expanduser().resolve() / "secrets")
            present = store.has_demo_exchange_credentials(
                reference, execution_venue.currentData(), execution_environment.currentData())
        except (OSError, ValueError):
            present = False
        state = "PROTECTED RECORD PRESENT" if present else "NOT CONFIGURED"
        credential_status_label.setText("Demo/test credential status: " + state)

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

    def refresh_selected_run_details() -> None:
        nonlocal last_selected_details_refresh
        if time.monotonic() - last_selected_details_refresh < 15:
            return
        last_selected_details_refresh = time.monotonic()
        try:
            run = selected()
            manifest = load_run(run, require_current_build=False)
            config = _run_config(manifest["configuration"])
            store = secret_store_for_run(manifest)
            provider_profile = config.provider_profile
            provider_key = ("NOT REQUIRED" if provider_profile == "DISABLED" else
                "PROTECTED RECORD PRESENT" if store.has_provider_key(provider_profile) else "NOT CONFIGURED")
            if isinstance(config, ResearchRunConfigV2) and config.credential_ref:
                venue_key = store.has_demo_exchange_credentials(config.credential_ref,
                    config.selected_execution_venue, config.execution_environment)
                demo_credential = "PROTECTED RECORD PRESENT" if venue_key else "NOT CONFIGURED"
            else:
                demo_credential = "NOT BOUND"
            selected_run_configuration_label.setText(_selected_run_configuration_text(
                manifest["run_id"], config, provider_key_status=provider_key,
                demo_credential_status=demo_credential,
            ))
            alerts = recent_event_alerts_for_run(run, limit=8)
            if not alerts:
                alerts_label.setText("Recent public alerts: none recorded. Alerts have zero trading authority.")
            else:
                lines = []
                for alert in alerts:
                    if not alert["alert_ref"]:
                        lines.append(str(alert["relevance"]))
                        continue
                    available = time.strftime("%Y-%m-%d %H:%M:%S UTC",
                        time.gmtime(alert["available_at_ns"] / 1_000_000_000))
                    lines.append(f"{available} — {alert['severity']} {alert['event_type']} — "
                                 f"{alert['relevance']} — {alert['alert_ref'][:16]}")
                alerts_label.setText("Recent public alerts (zero authority):\n" + "\n".join(lines))
        except (OSError, ValueError, TypeError, KeyError):
            selected_run_configuration_label.setText("Selected run configuration/credential status is unavailable.")
            alerts_label.setText("Recent public alerts are unavailable. Alerts have zero trading authority.")

    def action(name: str) -> None:
        nonlocal pending_report, pending_preflight
        try:
            if name == "Create run":
                pair_path = Path(owner_pairs_edit.text()) if owner_pairs_edit.text() else None
                pair_hash = None
                if pair_path is not None:
                    from .runtime.owner_pairs import read_owner_pair_configuration

                    pair_hash = read_owner_pair_configuration(pair_path).content_hash
                public_venues = tuple(venue for venue, checked in (
                    ("BYBIT", public_bybit.isChecked()), ("BINANCE", public_binance.isChecked())) if checked)
                config = ResearchRunConfigV2(
                    data_root=canonical_data_root(Path(root_edit.text())), public_venues=public_venues,
                    selected_execution_venue=execution_venue.currentData(),
                    execution_environment=execution_environment.currentData(),
                    account_scope_ref=account_scope.text().strip() or None,
                    capability_profile_ref=capability_ref.text().strip() or None,
                    credential_ref=credential_ref.text().strip() or None,
                    execution_profile=execution_profile.currentData(),
                    execution_instrument_symbol=execution_symbol.text().strip() or None,
                    provider_profile=provider.currentData(),
                    owner_pair_configuration_hash=pair_hash,
                    official_fomc_source_profile_json=calendar_profile_edit.property("canonical_profile"),
                )
                created = create_run(Path(root_edit.text()), config, owner_pair_configuration_path=pair_path)
                refresh_runs(created)
            elif name == "Start / resume":
                launch_run(selected())
            elif name == "Storage preflight":
                if pending_preflight is None:
                    pending_preflight = reports.submit(preflight_run, selected())
                    buttons[name].setEnabled(False)
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

    def choose_owner_pairs() -> None:
        chosen, _ = QFileDialog.getOpenFileName(window, "S8 research pair configuration", "", "JSON files (*.json)")
        if chosen:
            try:
                from .runtime.owner_pairs import read_owner_pair_configuration

                parsed = read_owner_pair_configuration(chosen)
                owner_pairs_edit.setText(chosen)
                owner_pairs_edit.setToolTip(f"{parsed.owner_id}: {len(parsed.pairs)} research pairs; {parsed.content_hash}")
            except (OSError, ValueError):
                QMessageBox.warning(window, "ATLAS", "Invalid S8 research pair configuration")

    owner_pairs_button.clicked.connect(choose_owner_pairs)

    def choose_calendar_profile() -> None:
        chosen, _ = QFileDialog.getOpenFileName(window, "Official FOMC source profile", "", "JSON files (*.json)")
        if chosen:
            try:
                from .runtime.official_calendar import OfficialFomcSourceProfileV1

                selected_path = Path(chosen)
                if selected_path.is_symlink() or not selected_path.is_file():
                    raise ValueError("source profile requires a regular local file")
                with selected_path.open("rb") as handle:
                    raw = handle.read(8193)
                if len(raw) > 8192:
                    raise ValueError("source profile exceeds its configuration bound")
                profile = OfficialFomcSourceProfileV1.from_dict(json.loads(raw.decode("utf-8")))
                calendar_profile_edit.setText(chosen)
                calendar_profile_edit.setProperty("canonical_profile", canonical_json(profile.to_dict()))
                calendar_profile_edit.setToolTip(f"{profile.parser}; {profile.content_hash}; external source proof required")
            except (OSError, ValueError):
                QMessageBox.warning(window, "ATLAS", "Invalid official FOMC source profile")

    calendar_profile_button.clicked.connect(choose_calendar_profile)

    def store_secret() -> None:
        nonlocal last_selected_details_refresh
        profile = provider.currentData()
        if profile == "DISABLED":
            QMessageBox.information(window, "ATLAS", "Select a provider profile before storing its key.")
            return
        provider_name = "OpenAI" if profile == S7_EVENT_EXTRACTION_PROFILE else "DeepSeek"
        value, accepted = QInputDialog.getText(window, "Optional provider key", provider_name + " API key",
                                               QLineEdit.EchoMode.Password)
        if accepted and value:
            try:
                WindowsSecretStore(Path(root_edit.text()).expanduser().resolve() / "secrets").put_provider_key(
                    profile, value)
                last_selected_details_refresh = float("-inf")
                refresh_selected_run_details()
                QMessageBox.information(window, "ATLAS", "Key stored with Windows protection in the selected data root. No key value is displayed.")
            except Exception as exc:
                QMessageBox.warning(window, "ATLAS", "Secret storage failed: " + type(exc).__name__)
            finally:
                value = ""

    def store_venue_credentials() -> None:
        reference = credential_ref.text().strip()
        if not reference:
            QMessageBox.warning(window, "ATLAS", "Enter an opaque credential reference before configuring demo/test credentials.")
            return
        key, key_accepted = QInputDialog.getText(window, "Demo/test credential", "API key",
                                                 QLineEdit.EchoMode.Password)
        if not key_accepted or not key:
            return
        secret, secret_accepted = QInputDialog.getText(window, "Demo/test credential", "API secret",
                                                       QLineEdit.EchoMode.Password)
        if not secret_accepted or not secret:
            key = ""
            return
        try:
            venue = execution_venue.currentData()
            environment = execution_environment.currentData()
            WindowsSecretStore(Path(root_edit.text()).expanduser().resolve() / "secrets").put_demo_exchange_credentials(
                reference, venue, environment, key, secret)
            refresh_credential_status()
            QMessageBox.information(window, "ATLAS", "Demo/test credential is protected for this venue, environment, and reference. No venue authentication or order call is performed.")
        except Exception as exc:
            QMessageBox.warning(window, "ATLAS", "Credential storage failed: " + type(exc).__name__)
        finally:
            key = ""
            secret = ""

    def refresh_status() -> None:
        nonlocal pending_report, pending_preflight, pending_demo_check
        if pending_demo_check is not None and pending_demo_check.done():
            try:
                result = pending_demo_check.result()
                QMessageBox.information(window, "ATLAS demo/test account", result["account_profile"] + ": "
                    + ", ".join(result["account_profile_reasons"]) + "\nCapital and assisted execution disabled.")
            except Exception as exc:
                QMessageBox.warning(window, "ATLAS demo/test account", "Account check failed: " + type(exc).__name__)
            pending_demo_check = None
            demo_check_button.setEnabled(True)
        if pending_preflight is not None and pending_preflight.done():
            try:
                result = pending_preflight.result()
                detail = "Storage preflight allows startup; live qualification remains a TEST GATE." if result["allowed"] else "; ".join(result["reasons"])
                QMessageBox.information(window, "ATLAS preflight", result["status"] + ": " + detail)
            except Exception as error:
                QMessageBox.warning(window, "ATLAS preflight", "TEST GATE: " + type(error).__name__)
            pending_preflight = None
            buttons["Storage preflight"].setEnabled(True)
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
        try:
            guidance = owner_live_indicator(selected(), now_ns=time.time_ns())
            health_label.setText(guidance["action"] + " — " + guidance["guidance"])
            health_details_label.setText("\n".join(guidance.get("operator_details", ())))
            colour = {"GREEN": "#167326", "AMBER": "#ad6500", "RED": "#ba1515"}[guidance["colour"]]
            health_label.setStyleSheet("font-weight: bold; font-size: 16px; color: " + colour + ";")
        except (OSError, ValueError, TypeError, KeyError):
            health_label.setText("ATTENTION — live-test health evidence is unavailable. Inspect preflight/startup status.")
            health_label.setStyleSheet("font-weight: bold; font-size: 16px; color: #ad6500;")
            health_details_label.setText("Process health, source lanes, queue/storage pressure, report and provider details are unavailable.")
        refresh_selected_run_details()

    for name, button in buttons.items():
        button.clicked.connect(lambda checked=False, name=name: action(name))
    browse.clicked.connect(choose_folder)
    secret_button.clicked.connect(store_secret)
    venue_secret_button.clicked.connect(store_venue_credentials)

    def check_selected_demo() -> None:
        nonlocal pending_demo_check
        try:
            if pending_demo_check is None:
                pending_demo_check = reports.submit(check_demo_account_for_run, selected())
                demo_check_button.setEnabled(False)
        except Exception as exc:
            QMessageBox.warning(window, "ATLAS demo/test account", "Account check failed: " + type(exc).__name__)

    demo_check_button.clicked.connect(check_selected_demo)
    def selected_run_changed(_index: int = -1) -> None:
        nonlocal last_selected_details_refresh
        last_selected_details_refresh = float("-inf")
        refresh_selected_run_details()

    runs.currentIndexChanged.connect(selected_run_changed)

    def demo_action(start: bool) -> None:
        try:
            if start:
                launch_selected_demo(selected())
            else:
                request_demo_stop(selected())
        except Exception as exc:
            QMessageBox.warning(window, "ATLAS demo/test OMS", "Operation failed: " + type(exc).__name__)

    demo_start_button.clicked.connect(lambda: demo_action(True))
    demo_stop_button.clicked.connect(lambda: demo_action(False))
    root_edit.editingFinished.connect(refresh_runs)
    root_edit.editingFinished.connect(refresh_credential_status)
    credential_ref.editingFinished.connect(refresh_credential_status)
    execution_venue.currentIndexChanged.connect(refresh_credential_status)
    execution_environment.currentIndexChanged.connect(refresh_credential_status)
    refresh_credential_status()
    refresh_runs()
    timer = QTimer(window)
    timer.timeout.connect(refresh_status)
    timer.start(2000)
    window.resize(600, 500)
    window.show()
    smoke_result: dict[str, Any] = {}
    if smoke:
        def finish_desktop_smoke() -> None:
            refresh_status()
            smoke_result.update(status="TESTED", check="DESKTOP_STARTUP_AND_OWNER_GUIDANCE",
                window_visible=window.isVisible(), preflight_control_present="Storage preflight" in buttons,
                health_indicator_present=bool(health_label.text()), live_run_started=False,
                capital_enabled=False, assisted_enabled=False)
            window.close()
            app.quit()

        QTimer.singleShot(250, finish_desktop_smoke)
    try:
        result = app.exec()
        if smoke:
            if not smoke_result or not all(smoke_result[key] for key in (
                    "window_visible", "preflight_control_present", "health_indicator_present")):
                raise ValueError("desktop startup/owner-guidance smoke did not complete")
            print(json.dumps(smoke_result))
        return result
    finally:
        reports.shutdown(wait=False, cancel_futures=True)


def main() -> int:
    parser = argparse.ArgumentParser(prog="ATLAS")
    parser.add_argument("--diagnostics", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--broker-smoke", action="store_true")
    parser.add_argument("--desktop-smoke", action="store_true")
    parser.add_argument("--data-root", type=Path)
    parser.add_argument("--run-root", type=Path)
    parser.add_argument("--launch-id")
    parser.add_argument("--broker-context", type=Path)
    parser.add_argument("--component", choices=("ops", "desktop", "critic-broker",
        "event-extraction-broker", "preflight", "demo-oms"))
    args = parser.parse_args()
    if args.desktop_smoke:
        if args.data_root is None:
            parser.error("desktop smoke requires temporary --data-root")
        return desktop(smoke=True, data_root=args.data_root)
    if args.component == "preflight":
        if args.run_root is None:
            parser.error("preflight requires --run-root")
        from .runtime.storage_preflight import qualify_storage_path

        manifest = load_run(args.run_root)
        qualification = qualify_storage_path(args.run_root, identity_sha256=manifest["content_hash"])
        print(json.dumps(qualification.as_dict()))
        return 0 if qualification.allowed else 2
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
    if args.component in {"ops", "critic-broker", "event-extraction-broker", "demo-oms"}:
        if args.run_root is None:
            parser.error("runtime component requires --run-root")
        if args.component == "critic-broker" and args.broker_context is None:
            parser.error("critic broker requires its protected epoch context")
        if args.component == "event-extraction-broker" and args.broker_context is None:
            parser.error("event extraction broker requires its protected epoch context")
        if args.launch_id is not None and (len(args.launch_id) != 32
                                          or any(c not in "0123456789abcdef" for c in args.launch_id)):
            parser.error("launch identity must be an exact generated identifier")
        stop = threading.Event()
        previous_handlers = {kind: signal.signal(kind, lambda signum, frame: stop.set())
                             for kind in (signal.SIGINT, signal.SIGTERM)}
        try:
            if args.component == "critic-broker":
                result = run_critic_broker(args.run_root, args.broker_context, stop_requested=stop.is_set)
            elif args.component == "event-extraction-broker":
                result = run_event_extraction_broker(args.run_root, args.broker_context,
                                                     stop_requested=stop.is_set)
            elif args.component == "demo-oms":
                result = run_selected_demo_component(args.run_root, stop_requested=stop.is_set)
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
