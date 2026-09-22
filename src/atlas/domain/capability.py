"""Typed ATLAS V1 capability contract (freeze §1.1)."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

from .enums import CapabilityStatus

CONTRACT_VERSION = "1.0"
REQUIRED_CAPABILITY_FIELDS = (
    "entry_ioc_with_attached_full_mark_market_stop",
    "native_stop_visible_and_resizes_on_partial_fill",
    "reduce_only_wire_and_matching_enforcement",
    "ambiguous_submit_not_treated_as_definite_rejection",
    "external_native_stop_fill_reconciliation",
    "native_position_stop_read_and_repair_port",
)
_PLACEHOLDERS = frozenset({"", "REQUIRED", "REQUIRED_AT_INSTALL", "CONFIGURED", "PENDING", "UNVERIFIED", "TEST_GATE"})
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _nb(v: str, n: str) -> str:
    if not isinstance(v, str) or not v.strip():
        raise ValueError(f"{n} must be non-blank")
    return v.strip()


def is_placeholder(v: str) -> bool:
    s = v.strip() if isinstance(v, str) else ""
    return s in _PLACEHOLDERS or s.startswith(("REQUIRED", "PENDING", "UNVERIFIED", "TEST_GATE"))


@dataclass(frozen=True)
class RuntimeInfo:
    distribution: str
    version: str
    source_commit: str
    installed_artifact_sha256: str
    dependency_lock_sha256: str
    python_platform_abi: str

    def __post_init__(self):
        for f in self.__dataclass_fields__:
            _nb(getattr(self, f), f"runtime.{f}")

    def to_dict(self) -> dict[str, Any]:
        return {f: getattr(self, f) for f in self.__dataclass_fields__}


@dataclass(frozen=True)
class VenueInfo:
    environment: str
    account_identity_hash: str
    account_generation_and_margin_mode: str
    product: str
    position_mode: str
    supported_symbols: tuple[str, ...]

    def __post_init__(self):
        for f in (
            "environment",
            "account_identity_hash",
            "account_generation_and_margin_mode",
            "product",
            "position_mode",
        ):
            _nb(getattr(self, f), f"venue.{f}")
        if not self.supported_symbols:
            raise ValueError("supported_symbols required")
        object.__setattr__(self, "supported_symbols", tuple(self.supported_symbols))

    def to_dict(self) -> dict[str, Any]:
        return {
            "environment": self.environment,
            "account_identity_hash": self.account_identity_hash,
            "account_generation_and_margin_mode": self.account_generation_and_margin_mode,
            "product": self.product,
            "position_mode": self.position_mode,
            "symbols": list(self.supported_symbols),
        }


@dataclass(frozen=True)
class Capabilities:
    entry_ioc_with_attached_full_mark_market_stop: CapabilityStatus
    native_stop_visible_and_resizes_on_partial_fill: CapabilityStatus
    reduce_only_wire_and_matching_enforcement: CapabilityStatus
    ambiguous_submit_not_treated_as_definite_rejection: CapabilityStatus
    external_native_stop_fill_reconciliation: CapabilityStatus
    native_position_stop_read_and_repair_port: CapabilityStatus

    def __post_init__(self):
        for f in REQUIRED_CAPABILITY_FIELDS:
            if not isinstance(getattr(self, f), CapabilityStatus):
                raise ValueError(f"capabilities.{f} must be CapabilityStatus")

    def to_dict(self) -> dict[str, str]:
        return {f: getattr(self, f).value for f in REQUIRED_CAPABILITY_FIELDS}

    def all_supported(self) -> bool:
        return all(getattr(self, f) == CapabilityStatus.SUPPORTED for f in REQUIRED_CAPABILITY_FIELDS)

    def blocking_reasons(self) -> list[str]:
        return [
            f"{f}={getattr(self, f).value} (must be SUPPORTED)"
            for f in REQUIRED_CAPABILITY_FIELDS
            if getattr(self, f) != CapabilityStatus.SUPPORTED
        ]


@dataclass(frozen=True)
class CapabilityContract:
    contract_version: str
    runtime: RuntimeInfo
    venue: VenueInfo
    capabilities: Capabilities
    assisted_enabled: bool = False

    def __post_init__(self):
        _nb(self.contract_version, "contract_version")
        blockers = self.assisted_blockers() if self.assisted_enabled else []
        if blockers:
            raise ValueError("assisted_enabled=true while blockers remain: " + "; ".join(blockers))

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "runtime": self.runtime.to_dict(),
            "venue": self.venue.to_dict(),
            "capabilities": self.capabilities.to_dict(),
            "assisted_enabled": self.assisted_enabled,
        }

    def to_canonical_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    def contract_hash(self) -> str:
        return hashlib.sha256(self.to_canonical_json().encode()).hexdigest()

    def assisted_blockers(self) -> list[str]:
        r = self.runtime
        v = self.venue
        reasons = self.capabilities.blocking_reasons()
        for n, val in (
            ("runtime.installed_artifact_sha256", r.installed_artifact_sha256),
            ("runtime.dependency_lock_sha256", r.dependency_lock_sha256),
            ("runtime.python_platform_abi", r.python_platform_abi),
            ("venue.account_identity_hash", v.account_identity_hash),
            ("venue.account_generation_and_margin_mode", v.account_generation_and_margin_mode),
        ):
            if is_placeholder(val):
                reasons.append(f"{n} holds placeholder {val!r}")
        if r.distribution != "nautilus_trader":
            reasons.append("runtime.distribution must be nautilus_trader")
        if r.version != "2.0.0rc5":
            reasons.append("runtime.version must be 2.0.0rc5")
        if r.source_commit != "1b0a49d2792a9432a3aca3fcb617ce7a630d905e":
            reasons.append("runtime.source_commit mismatch")
        for n, val in (
            ("runtime.installed_artifact_sha256", r.installed_artifact_sha256),
            ("runtime.dependency_lock_sha256", r.dependency_lock_sha256),
        ):
            if not is_placeholder(val) and not _SHA256_RE.fullmatch(val):
                reasons.append(f"{n} invalid SHA256")
        if r.python_platform_abi != "cpython-312-x86_64-linux-gnu":
            reasons.append("python/platform ABI mismatch")
        if v.environment != "testnet":
            reasons.append("venue.environment must be testnet")
        if v.product != "linear":
            reasons.append("venue.product must be linear")
        if v.position_mode != "one_way":
            reasons.append("venue.position_mode must be one_way")
        if tuple(v.supported_symbols) != ("BTCUSDT", "ETHUSDT"):
            reasons.append("venue symbols must be BTCUSDT, ETHUSDT")
        if (
            not is_placeholder(v.account_generation_and_margin_mode)
            and "isolated" not in v.account_generation_and_margin_mode.lower()
        ):
            reasons.append("account margin profile not isolated-compatible")
        return reasons

    validate_for_assisted = assisted_blockers


def initial_unverified_fixture(**overrides: Any) -> CapabilityContract:
    d: dict[str, Any] = {
        "environment": "testnet",
        "account_identity_hash": "REQUIRED",
        "account_generation_and_margin_mode": "REQUIRED",
        "distribution": "nautilus_trader",
        "version": "2.0.0rc5",
        "source_commit": "1b0a49d2792a9432a3aca3fcb617ce7a630d905e",
        "installed_artifact_sha256": "eab45fafd2312deda1236554c49a9798bfc76bc8465af864878e2f70189ebebe",
        "dependency_lock_sha256": "0d7b5cc6129aab127f07a1b5bce4b7794eec5c48db0f99451eba675031ca2b39",
        "python_platform_abi": "cpython-312-x86_64-linux-gnu",
        "product": "linear",
        "position_mode": "one_way",
        "supported_symbols": ("BTCUSDT", "ETHUSDT"),
    }
    d.update(overrides)
    caps = Capabilities(**dict.fromkeys(REQUIRED_CAPABILITY_FIELDS, CapabilityStatus.UNVERIFIED))
    return CapabilityContract(
        CONTRACT_VERSION,
        RuntimeInfo(
            d["distribution"],
            d["version"],
            d["source_commit"],
            d["installed_artifact_sha256"],
            d["dependency_lock_sha256"],
            d["python_platform_abi"],
        ),
        VenueInfo(
            d["environment"],
            d["account_identity_hash"],
            d["account_generation_and_margin_mode"],
            d["product"],
            d["position_mode"],
            tuple(d["supported_symbols"]),
        ),
        caps,
        False,
    )


def capability_contract_from_manifest(data: dict[str, Any]) -> CapabilityContract:
    r, v, c = data["runtime"], data["venue"], data["capabilities"]
    raw = data.get("assisted_enabled", False)
    if not isinstance(raw, bool):
        raise ValueError("assisted_enabled must be a real YAML boolean")
    try:
        caps = Capabilities(**{f: CapabilityStatus(str(c[f])) for f in REQUIRED_CAPABILITY_FIELDS})
    except ValueError as exc:
        raise ValueError(f"unknown status: {exc}") from exc
    symbols = v.get("symbols", v.get("supported_symbols"))
    return CapabilityContract(
        str(data.get("contract_version", CONTRACT_VERSION)),
        RuntimeInfo(
            str(r["distribution"]),
            str(r["version"]),
            str(r["source_commit"]),
            str(r["installed_artifact_sha256"]),
            str(r["dependency_lock_sha256"]),
            str(r["python_platform_abi"]),
        ),
        VenueInfo(
            str(v["environment"]),
            str(v["account_identity_hash"]),
            str(v["account_generation_and_margin_mode"]),
            str(v["product"]),
            str(v["position_mode"]),
            tuple(map(str, symbols)),
        ),
        caps,
        raw,
    )
