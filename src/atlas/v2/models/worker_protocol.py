"""Versioned minimal worker IPC and provider failure types."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .._serialization import canonical_json, sha256_json, strict_fields
from .protocol import ForecastArtifactV2, ModelManifestV2, ModelRequestV2


class ProviderStateV2(StrEnum):
    AVAILABLE = "AVAILABLE"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    STALE_DISCARDED = "STALE_DISCARDED"
    QUEUE_FULL = "QUEUE_FULL"


class ModelProviderError(RuntimeError):
    def __init__(self, code: str, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable


@dataclass(frozen=True)
class WorkerRequestV2:
    request: ModelRequestV2
    manifest: ModelManifestV2
    inputs: Mapping[str, Any]

    SCHEMA_VERSION = 1

    def __post_init__(self) -> None:
        if self.manifest.manifest_hash != self.request.model_manifest_hash:
            raise ValueError("worker request manifest does not match the immutable request")
        if not isinstance(self.inputs, Mapping):
            raise ValueError("worker inputs must be an object")
        _reject_sensitive_fields(self.request.to_dict(), field_name="request")
        _reject_sensitive_fields(self.manifest.to_dict(), field_name="manifest")
        _reject_sensitive_fields(self.inputs)

    def to_dict(self) -> dict[str, Any]:
        # The allowlisted fields intentionally exclude paths, environment, account or execution objects.
        return {
            "schema_version": self.SCHEMA_VERSION,
            "request": self.request.to_dict(),
            "manifest": self.manifest.to_dict(),
            "inputs": dict(self.inputs),
        }

    @property
    def content_hash(self) -> str:
        return sha256_json({"worker_request_type": "WorkerRequestV2", "request": self.to_dict()})

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> WorkerRequestV2:
        fields = {"schema_version", "request", "manifest", "inputs"}
        value = strict_fields(data, expected=fields, required=fields, name=cls.__name__)
        if type(value["schema_version"]) is not int or value["schema_version"] != cls.SCHEMA_VERSION:
            raise ValueError("unsupported WorkerRequestV2 schema_version")
        if not isinstance(value["inputs"], Mapping):
            raise ValueError("worker inputs must be an object")
        return cls(ModelRequestV2.from_dict(value["request"]), ModelManifestV2.from_dict(value["manifest"]), value["inputs"])


_PROHIBITED_INPUT_FRAGMENTS = (
    "apikey", "apisecret", "secret", "privatetoken", "accountid", "accountidentity",
    "livecontroldb", "livecontrolpath", "databasepath", "dbpath", "orderapi", "riskpolicy",
    "executionclient", "tradingcredential", "exchangecredential",
)


def _reject_sensitive_fields(value: Any, *, field_name: str = "inputs") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
            if any(fragment in normalized for fragment in _PROHIBITED_INPUT_FRAGMENTS):
                raise ValueError(f"worker protocol forbids sensitive field {field_name}.{key}")
            _reject_sensitive_fields(child, field_name=f"{field_name}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            _reject_sensitive_fields(child, field_name=f"{field_name}[{index}]")
    elif isinstance(value, str):
        lowered = value.lower()
        is_absolute_path = value.startswith(("/", "\\\\")) or re.match(r"^[a-z]:[\\\\/]", lowered) is not None
        is_storage_path = lowered.endswith((".sqlite", ".sqlite3", ".db")) or ".sqlite/" in lowered
        if is_absolute_path or is_storage_path or lowered.startswith("file://"):
            raise ValueError(f"worker protocol forbids filesystem path data in {field_name}")


def strict_worker_response(data: Any) -> ForecastArtifactV2:
    if not isinstance(data, Mapping):
        raise ValueError("worker response must be a JSON object")
    return ForecastArtifactV2.from_dict(data)


def canonical_worker_request(request: WorkerRequestV2) -> bytes:
    return canonical_json(request.to_dict()).encode("utf-8")
