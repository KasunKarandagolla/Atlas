"""Strict deterministic primitives shared by the additive V2 contracts."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any

from atlas.domain.money import canonical_decimal_str, ensure_decimal
from atlas.domain.time import ensure_utc_ns

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True, init=False)
class FrozenMap(Mapping[str, Any]):
    """A recursively immutable, key-sorted JSON object."""

    _items: tuple[tuple[str, Any], ...]

    def __init__(self, values: Mapping[str, Any] | Iterator[tuple[str, Any]] | None = None) -> None:
        items = values.items() if isinstance(values, Mapping) else (values or ())
        normalized: list[tuple[str, Any]] = []
        seen: set[str] = set()
        for key, value in items:
            if not isinstance(key, str) or not key:
                raise ValueError("object keys must be non-empty strings")
            if key in seen:
                raise ValueError(f"duplicate object key: {key}")
            seen.add(key)
            normalized.append((key, freeze_json(value)))
        object.__setattr__(self, "_items", tuple(sorted(normalized, key=lambda pair: pair[0])))

    def __getitem__(self, key: str) -> Any:
        for existing, value in self._items:
            if existing == key:
                return value
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        return (key for key, _ in self._items)

    def __len__(self) -> int:
        return len(self._items)

    def __hash__(self) -> int:
        return hash(self._items)

    def to_dict(self) -> dict[str, Any]:
        return {key: json_value(value) for key, value in self._items}


def freeze_json(value: Any, *, field: str = "value") -> Any:
    if value is None or isinstance(value, (str, bool, int, Decimal, Enum)):
        if isinstance(value, Decimal) and not value.is_finite():
            raise ValueError(f"{field}: non-finite Decimal is not allowed")
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{field}: non-finite float is not allowed")
        return value
    if isinstance(value, Mapping):
        return FrozenMap(value)
    if isinstance(value, (tuple, list)):
        return tuple(freeze_json(item, field=field) for item in value)
    if hasattr(value, "to_dict") and getattr(getattr(value, "__dataclass_params__", None), "frozen", False):
        return value
    raise ValueError(f"{field}: unsupported JSON value type {type(value).__name__}")


def json_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        return canonical_decimal_str(value)
    if isinstance(value, FrozenMap):
        return value.to_dict()
    if isinstance(value, tuple):
        return [json_value(item) for item in value]
    if isinstance(value, list):
        return [json_value(item) for item in value]
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise ValueError("JSON object keys must be strings")
        return {key: json_value(item) for key, item in value.items()}
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("NaN/Infinity is not authoritative JSON")
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if hasattr(value, "to_dict"):
        return json_value(value.to_dict())
    raise ValueError(f"unsupported canonical JSON type {type(value).__name__}")


def canonical_json(value: Any) -> str:
    return json.dumps(json_value(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def artifact_wire(envelope: Any, body: Mapping[str, Any], *, artifact_type: str, include_hash: bool = True) -> dict[str, Any]:
    result = {"envelope": envelope.to_dict(include_hash=False)}
    result.update(json_value(body))
    if include_hash:
        if not envelope.content_hash:
            raise ValueError("artifact is not sealed")
        result["envelope"]["content_hash"] = envelope.content_hash
    return result


def seal_envelope(envelope: Any, body: Mapping[str, Any], *, artifact_type: str) -> Any:
    from dataclasses import replace

    preimage = {"artifact_type": artifact_type, "artifact": artifact_wire(envelope, body, artifact_type=artifact_type, include_hash=False)}
    digest = sha256_json(preimage)
    if not envelope.content_hash:
        return replace(envelope, content_hash=digest)
    if envelope.content_hash != digest:
        raise ValueError(f"{artifact_type} content_hash mismatch")
    return envelope


def nonblank(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


def sha256_ref(value: str, *, field: str) -> str:
    value = nonblank(value, field=field)
    if not SHA256_RE.fullmatch(value):
        raise ValueError(f"{field} must be a lowercase SHA-256 hex digest")
    return value


def timestamp(value: int, *, field: str) -> int:
    return ensure_utc_ns(value, field=field)


def decimal_value(value: Decimal | int | str, *, field: str, wire: bool = False) -> Decimal:
    if wire and not isinstance(value, str):
        raise ValueError(f"{field} wire value must be a canonical decimal string")
    result = ensure_decimal(value, field=field)
    if wire and canonical_decimal_str(result) != value:
        raise ValueError(f"{field} wire value is not canonical")
    return result


def strict_fields(data: Any, *, expected: set[str], required: set[str], name: str) -> Mapping[str, Any]:
    if not isinstance(data, Mapping) or any(not isinstance(key, str) for key in data):
        raise ValueError(f"{name} must be a string-keyed object")
    unknown = set(data) - expected
    missing = required - set(data)
    if unknown:
        raise ValueError(f"{name} contains unknown fields: {sorted(unknown)}")
    if missing:
        raise ValueError(f"{name} is missing fields: {sorted(missing)}")
    return data


def string_tuple(value: Any, *, field: str, sorted_unique: bool = False) -> tuple[str, ...]:
    if not isinstance(value, (tuple, list)):
        raise ValueError(f"{field} must be an array of strings")
    result = tuple(nonblank(item, field=field) for item in value)
    if sorted_unique and result != tuple(sorted(set(result))):
        raise ValueError(f"{field} must be sorted and unique")
    return result
