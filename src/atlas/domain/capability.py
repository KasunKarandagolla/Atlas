"""CapabilityContract (freeze §1.1).

- Typed, immutable, versioned.
- Capability values are explicit CapabilityStatus, never bare booleans.
- Unknown never implies supported.
- Assisted execution cannot be represented as enabled while required capabilities
  remain unverified/unsupported/failed.
"""

from __future__ import annotations

import hashlib
import json
import re as _re
from dataclasses import dataclass
from typing import Any

from .enums import CapabilityStatus

CONTRACT_VERSION = "1.0"

REQUIRED_CAPABILITY_FIELDS: tuple[str, ...] = (
    "entry_ioc_with_attached_full_mark_market_stop",
    "native_stop_visible_and_resizes_on_partial_fill",
    "reduce_only_wire_and_matching_enforcement",
    "ambiguous_submit_not_treated_as_definite_rejection",
    "external_native_stop_fill_reconciliation",
    "native_position_stop_read_and_repair_port",
)


PLACEHOLDER_VALUES = frozenset({"REQUIRED", "REQUIRED_AT_INSTALL"})

_SHA256_RE = _re.compile(r"^[0-9a-f]{64}$")


def _is_placeholder(value: str) -> bool:
    s = value.strip()
    return not s or s in PLACEHOLDER_VALUES or s.startswith("REQUIRED")


def _require_real(value: str, field_name: str) -> str:
    if _is_placeholder(value):
        raise ValueError(f"{field_name} still holds placeholder {value!r}; assisted impossible")
    return value


def _require_nonblank(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-blank string")
    return value.strip()


@dataclass(frozen=True)
class RuntimeInfo:
    distribution: str
    version: str
    source_commit: str
    installed_artifact_sha256: str
    dependency_lock_sha256: str
    python_platform_abi: str

    def __post_init__(self) -> None:
        for f in (
            "distribution",
            "version",
            "source_commit",
            "installed_artifact_sha256",
            "dependency_lock_sha256",
            "python_platform_abi",
        ):
            _require_nonblank(getattr(self, f), f"runtime.{f}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "distribution": self.distribution,
            "version": self.version,
            "source_commit": self.source_commit,
            "installed_artifact_sha256": self.installed_artifact_sha256,
            "dependency_lock_sha256": self.dependency_lock_sha256,
            "python_platform_abi": self.python_platform_abi,
        }


@dataclass(frozen=True)
class VenueInfo:
    environment: str
    account_identity_hash: str
    account_generation_and_margin_mode: str
    product: str
    position_mode: str
    supported_symbols: tuple[str, ...]

    def __post_init__(self) -> None:
        for f in (
            "environment",
            "account_identity_hash",
            "account_generation_and_margin_mode",
            "product",
            "position_mode",
        ):
            _require_nonblank(getattr(self, f), f"venue.{f}")
        if not self.supported_symbols:
            raise ValueError("venue.supported_symbols must be non-empty")
        for s in self.supported_symbols:
            _require_nonblank(s, "venue.supported_symbols[]")
        # Freeze boundary: v1 supports only listed linear one-way symbols; enforce shape, not venue truth.
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

    def __post_init__(self) -> None:
        for f in REQUIRED_CAPABILITY_FIELDS:
            v = getattr(self, f)
            if not isinstance(v, CapabilityStatus):
                raise ValueError(f"capabilities.{f} must be CapabilityStatus, got {v!r}")

    def to_dict(self) -> dict[str, Any]:
        return {f: getattr(self, f).value for f in REQUIRED_CAPABILITY_FIELDS}

    def all_supported(self) -> bool:
        return all(getattr(self, f) == CapabilityStatus.SUPPORTED for f in REQUIRED_CAPABILITY_FIELDS)

    def blocking_reasons(self) -> list[str]:
        out: list[str] = []
        for f in REQUIRED_CAPABILITY_FIELDS:
            v = getattr(self, f)
            if v != CapabilityStatus.SUPPORTED:
                out.append(f"{f}={v.value} (must be SUPPORTED for assisted execution)")
        return out


@dataclass(frozen=True)
class CapabilityContract:
    contract_version: str
    runtime: RuntimeInfo
    venue: VenueInfo
    capabilities: Capabilities
    assisted_enabled: bool = False

    def __post_init__(self) -> None:
        _require_nonblank(self.contract_version, "contract_version")
        if not isinstance(self.runtime, RuntimeInfo):
            raise ValueError("runtime must be RuntimeInfo")
        if not isinstance(self.venue, VenueInfo):
            raise ValueError("venue must be VenueInfo")
        if not isinstance(self.capabilities, Capabilities):
            raise ValueError("capabilities must be Capabilities")
        if not isinstance(self.assisted_enabled, bool):
            raise ValueError("assisted_enabled must be bool")
        if self.assisted_enabled:
            blockers = self.assisted_blockers()
            if blockers:
                raise ValueError(
                    "assisted_enabled=true blocked: " + "; ".join(blockers)
                )

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
        return hashlib.sha256(self.to_canonical_json().encode("utf-8")).hexdigest()

    def validate_for_assisted(self) -> list[str]:
        """Return structured blocking reasons (empty means eligible on capability grounds)."""
        return self.assisted_blockers()

    def assisted_blockers(self) -> list[str]:
        """Capability blockers PLUS placeholder/identity evidence blockers.

        Assisted execution is impossible while install/account identity fields
        still contain placeholders (REQUIRED / REQUIRED_AT_INSTALL).
        """
        reasons: list[str] = list(self.capabilities.blocking_reasons())
        r = self.runtime
        v = self.venue
        for field_name, val in (
            ("runtime.installed_artifact_sha256", r.installed_artifact_sha256),
            ("runtime.dependency_lock_sha256", r.dependency_lock_sha256),
            ("runtime.python_platform_abi", r.python_platform_abi),
            ("venue.account_identity_hash", v.account_identity_hash),
            ("venue.account_generation_and_margin_mode", v.account_generation_and_margin_mode),
            ("venue.environment", v.environment),
            ("venue.product", v.product),
            ("venue.position_mode", v.position_mode),
        ):
            if _is_placeholder(val):
                reasons.append(f"{field_name} holds placeholder {val!r}")
        for field_name, val in (
            ("runtime.installed_artifact_sha256", r.installed_artifact_sha256),
            ("runtime.dependency_lock_sha256", r.dependency_lock_sha256),
        ):
            if not _is_placeholder(val) and not _SHA256_RE.match(val.strip()):
                reasons.append(f"{field_name} must be 64 lowercase hex SHA256")
        return reasons


def initial_unverified_fixture(
    *,
    environment: str = "testnet",
    account_identity_hash: str = "REQUIRED",
    account_generation_and_margin_mode: str = "REQUIRED",
    distribution: str = "nautilus_trader",
    version: str = "2.0.0rc5",
    source_commit: str = "1b0a49d2792a9432a3aca3fcb617ce7a630d905e",
    installed_artifact_sha256: str = "REQUIRED_AT_INSTALL",
    dependency_lock_sha256: str = "REQUIRED_AT_INSTALL",
    python_platform_abi: str = "REQUIRED_AT_INSTALL",
    product: str = "linear",
    position_mode: str = "one_way",
    supported_symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT"),
) -> CapabilityContract:
    """Initial fixture: all exchange capabilities UNVERIFIED, assisted disabled."""
    caps = Capabilities(**dict.fromkeys(REQUIRED_CAPABILITY_FIELDS, CapabilityStatus.UNVERIFIED))  # type: ignore[arg-type]
    return CapabilityContract(
        contract_version=CONTRACT_VERSION,
        runtime=RuntimeInfo(
            distribution=distribution,
            version=version,
            source_commit=source_commit,
            installed_artifact_sha256=installed_artifact_sha256,
            dependency_lock_sha256=dependency_lock_sha256,
            python_platform_abi=python_platform_abi,
        ),
        venue=VenueInfo(
            environment=environment,
            account_identity_hash=account_identity_hash,
            account_generation_and_margin_mode=account_generation_and_margin_mode,
            product=product,
            position_mode=position_mode,
            supported_symbols=supported_symbols,
        ),
        capabilities=caps,
        assisted_enabled=False,
    )


def capability_contract_from_manifest(data: dict[str, Any]) -> CapabilityContract:
    """Parse docs/capability/bybit-v1.yaml style manifest into CapabilityContract."""
    try:
        runtime = data["runtime"]
        venue = data["venue"]
        caps = data["capabilities"]
        raw_assisted = data.get("assisted_enabled", False)
    except KeyError as exc:
        raise ValueError(f"manifest missing section: {exc}") from exc
    if not isinstance(raw_assisted, bool):
        raise ValueError(
            f"assisted_enabled must be a real YAML boolean, got {raw_assisted!r}"
        )
    assisted = raw_assisted

    def _status(name: str, raw: Any) -> CapabilityStatus:
        try:
            return CapabilityStatus(str(raw))
        except ValueError as exc:
            raise ValueError(f"capabilities.{name}: unknown status {raw!r}") from exc

    # Accept both 'symbols' and 'supported_symbols' keys for manifest ergonomics.
    symbols = venue.get("symbols", venue.get("supported_symbols"))
    if symbols is None:
        raise ValueError("venue.symbols (or supported_symbols) is required")

    capabilities = Capabilities(
        **{f: _status(f, caps[f]) for f in REQUIRED_CAPABILITY_FIELDS}  # type: ignore[arg-type]
    )
    # This will raise if manifest lacks a required capability (KeyError -> ValueError).
    return CapabilityContract(
        contract_version=str(data.get("contract_version", CONTRACT_VERSION)),
        runtime=RuntimeInfo(
            distribution=str(runtime["distribution"]),
            version=str(runtime["version"]),
            source_commit=str(runtime["source_commit"]),
            installed_artifact_sha256=str(runtime["installed_artifact_sha256"]),
            dependency_lock_sha256=str(runtime["dependency_lock_sha256"]),
            python_platform_abi=str(runtime["python_platform_abi"]),
        ),
        venue=VenueInfo(
            environment=str(venue["environment"]),
            account_identity_hash=str(venue["account_identity_hash"]),
            account_generation_and_margin_mode=str(
                venue["account_generation_and_margin_mode"]
            ),
            product=str(venue["product"]),
            position_mode=str(venue["position_mode"]),
            supported_symbols=tuple(str(s) for s in symbols),
        ),
        capabilities=capabilities,
        assisted_enabled=assisted,
    )
