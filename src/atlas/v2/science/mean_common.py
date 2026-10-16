"""Strict additive offline artifact codec; no runtime or capital authority.

Decimal cash/price/quantity serialize as canonical strings. Arrays are tuples,
objects are FrozenMap, unknown versions/fields and nonfinite values fail closed.
"""

from __future__ import annotations

import json
import math
import types
from collections.abc import Mapping
from dataclasses import fields
from decimal import Decimal
from typing import Any, ClassVar, Union, get_args, get_origin, get_type_hints

from atlas.v2._serialization import FrozenMap, decimal_value, json_value, sha256_json, sha256_ref, strict_fields

ZERO = Decimal(0)
DAY_NS = 86_400_000_000_000
MEAN_TARGET = "EXACT_ACTION_INCREMENTAL_TRADING_CASH_USDT_COMMON_24H_V1"


def _value(value: Any, kind: Any, *, wire: bool) -> Any:
    origin, args = get_origin(kind), get_args(kind)
    if origin in (Union, types.UnionType):
        for option in args:
            try:
                return _value(value, option, wire=wire)
            except (ValueError, TypeError):
                pass
        raise ValueError("invalid union field")
    if kind is type(None):
        if value is not None:
            raise ValueError("expected null")
    elif origin is tuple:
        if not isinstance(value, (list, tuple)) or (not wire and type(value) is not tuple):
            raise ValueError("immutable tuple required")
        if len(args) == 2 and args[1] is Ellipsis:
            return tuple(_value(x, args[0], wire=wire) for x in value)
        if len(value) != len(args):
            raise ValueError("tuple width mismatch")
        return tuple(_value(x, t, wire=wire) for x, t in zip(value, args, strict=True))
    elif kind is Decimal:
        if not wire and not isinstance(value, Decimal):
            raise ValueError("Decimal required")
        return decimal_value(value, field="decimal", wire=wire)
    elif kind is FrozenMap:
        if not isinstance(value, Mapping):
            raise ValueError("immutable object required")
        return FrozenMap(value)
    elif kind in (str, int, bool, float):
        if type(value) is not kind or (kind is float and not math.isfinite(value)):
            raise ValueError(f"finite exact {kind.__name__} required")
    elif wire and hasattr(kind, "from_dict"):
        return kind.from_dict(json_value(value))
    elif not isinstance(value, kind):
        raise ValueError(f"invalid field type {kind}")
    return value


class OfflineArtifact:
    VERSION: ClassVar[str]

    def __post_init__(self) -> None:
        hints = get_type_hints(type(self))
        for f in fields(self):  # type: ignore[arg-type]
            value = _value(getattr(self, f.name), hints[f.name], wire=False)
            object.__setattr__(self, f.name, value)
            if f.name.endswith(("_ref", "_hash")) and value is not None:
                sha256_ref(value, field=f.name)
            if f.name.endswith(("_refs", "_hashes")):
                for ref in value:
                    sha256_ref(ref, field=f.name)
            if f.name.endswith("_ns") and value is not None:
                times = value if isinstance(value, tuple) else (value,)
                if any(t < 0 for t in times):
                    raise ValueError("negative timestamp or duration")

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.VERSION, **{f.name: json_value(getattr(self, f.name)) for f in fields(self)}}  # type: ignore[arg-type]

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Any:
        names = {f.name for f in fields(cls)}  # type: ignore[arg-type]
        d = strict_fields(data, expected=names | {"version"}, required=names | {"version"}, name=cls.__name__)
        if d["version"] != cls.VERSION:
            raise ValueError("unsupported offline contract version")
        hints = get_type_hints(cls)
        return cls(**{k: _value(d[k], hints[k], wire=True) for k in names})

    @classmethod
    def from_json(cls, payload: str, *, maximum_bytes: int = 4_194_304) -> Any:
        if not isinstance(payload, str) or len(payload.encode("utf-8")) > maximum_bytes:
            raise ValueError("offline artifact wire byte cap")

        def unique_object(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError("duplicate JSON field")
                result[key] = value
            return result

        return cls.from_dict(json.loads(payload, object_pairs_hook=unique_object))

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def positive(*values: Decimal) -> None:
    if any(not isinstance(v, Decimal) or not v.is_finite() or v <= 0 for v in values):
        raise ValueError("positive finite Decimal required")


def nonnegative(*values: Decimal) -> None:
    if any(not isinstance(v, Decimal) or not v.is_finite() or v < 0 for v in values):
        raise ValueError("nonnegative finite Decimal required")


def unique(values: tuple[Any, ...]) -> None:
    if len(set(values)) != len(values):
        raise ValueError("duplicate identity")
