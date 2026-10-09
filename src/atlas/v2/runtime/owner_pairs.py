"""Strict, bounded adapter for owner-supplied S8 pair configuration.

This module parses declarations only. It never imports executable owner code,
discovers instruments, or grants a trading/capital authority.
"""

from __future__ import annotations

import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from atlas.v2._serialization import sha256_json, strict_fields
from atlas.v2.instruments import InstrumentKeyV2
from atlas.v2.strategies.s8_pairs import S8PairDefinitionV2

OWNER_PAIR_CONFIGURATION_VERSION = "S8_OWNER_PAIR_CONFIGURATION_V1"
MAX_OWNER_PAIR_COUNT = 64
MAX_OWNER_PAIR_CONFIGURATION_BYTES = 131_072


@dataclass(frozen=True)
class OwnerPairConfigurationV1:
    """Validated declarations and the hash of their canonical JSON body."""

    owner_id: str
    pairs: tuple[S8PairDefinitionV2, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.owner_id, str) or not self.owner_id.strip():
            raise ValueError("owner_id must be a non-empty string")
        if not self.pairs or len(self.pairs) > MAX_OWNER_PAIR_COUNT:
            raise ValueError("owner pair configuration requires 1 to 64 pairs")
        if any(not isinstance(pair, S8PairDefinitionV2) for pair in self.pairs):
            raise ValueError("pairs must contain S8PairDefinitionV2 declarations")
        if len({pair.pair_id for pair in self.pairs}) != len(self.pairs):
            raise ValueError("owner pair IDs must be unique")

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": OWNER_PAIR_CONFIGURATION_VERSION,
            "owner_id": self.owner_id,
            "pairs": [pair.to_dict() for pair in self.pairs],
        }

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number is not allowed: {value}")


def _reject_floats(value: Any, *, field: str = "configuration") -> None:
    if isinstance(value, float):
        raise ValueError(f"{field} must not contain floating-point values")
    if isinstance(value, dict):
        for key, item in value.items():
            _reject_floats(item, field=f"{field}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_floats(item, field=f"{field}[{index}]")


def _read_bounded_regular_file(path: Path) -> bytes:
    # O_NOFOLLOW and fstat keep symlink and replacement races from escaping the
    # declared file boundary. The initial lstat gives a clear error for links.
    try:
        if stat.S_ISLNK(path.lstat().st_mode):
            raise ValueError("owner pair configuration may not be a symlink")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(path, flags)
    except OSError as exc:
        raise ValueError(f"cannot open owner pair configuration: {exc}") from exc
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("owner pair configuration must be a regular file")
        if info.st_size > MAX_OWNER_PAIR_CONFIGURATION_BYTES:
            raise ValueError("owner pair configuration exceeds 131072 bytes")
        chunks: list[bytes] = []
        remaining = MAX_OWNER_PAIR_CONFIGURATION_BYTES + 1
        while remaining:
            chunk = os.read(fd, min(16_384, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        if len(raw) > MAX_OWNER_PAIR_CONFIGURATION_BYTES:
            raise ValueError("owner pair configuration exceeds 131072 bytes")
        return raw
    finally:
        os.close(fd)


def _parse_pair(value: Any, index: int) -> S8PairDefinitionV2:
    name = f"pairs[{index}]"
    fields = {"version", "pair_id", "economic_pair_definition", "key_a", "key_b",
        "hedge_fit", "residual_definition"}
    body = strict_fields(value, expected=fields, required=fields, name=name)
    for field in ("version", "pair_id", "economic_pair_definition", "hedge_fit", "residual_definition"):
        if not isinstance(body[field], str):
            raise ValueError(f"{name}.{field} must be a string")
    if body["version"] != "S8_PAIR_DEFINITION_V1":
        raise ValueError(f"{name}.version is unsupported")
    key_a = InstrumentKeyV2.from_dict(body["key_a"])
    key_b = InstrumentKeyV2.from_dict(body["key_b"])
    pair = S8PairDefinitionV2(body["pair_id"], body["economic_pair_definition"],
        key_a, key_b, body["hedge_fit"], body["residual_definition"], body["version"])
    # Ensure parsing and serialization preserve precisely the supported schema.
    if pair.to_dict() != dict(body):
        raise ValueError(f"{name} is not in canonical S8 pair form")
    return pair


def read_owner_pair_configuration(path: str | os.PathLike[str]) -> OwnerPairConfigurationV1:
    """Read strict UTF-8 JSON containing exact, existing S8 pair declarations."""
    raw = _read_bounded_regular_file(Path(path))
    try:
        text = raw.decode("utf-8", errors="strict")
        data = json.loads(text, object_pairs_hook=_no_duplicate_keys,
            parse_constant=_reject_constant)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("owner pair configuration must be valid UTF-8 JSON") from exc
    _reject_floats(data)
    top = strict_fields(data, expected={"version", "owner_id", "pairs"},
        required={"version", "owner_id", "pairs"}, name="owner pair configuration")
    if top["version"] != OWNER_PAIR_CONFIGURATION_VERSION:
        raise ValueError("unsupported owner pair configuration version")
    owner_id = top["owner_id"]
    if not isinstance(owner_id, str) or not owner_id.strip():
        raise ValueError("owner_id must be a non-empty string")
    values = top["pairs"]
    if not isinstance(values, list) or not values or len(values) > MAX_OWNER_PAIR_COUNT:
        raise ValueError("owner pair configuration requires 1 to 64 pairs")
    pairs = tuple(_parse_pair(value, index) for index, value in enumerate(values))
    result = OwnerPairConfigurationV1(owner_id, pairs)
    return result
