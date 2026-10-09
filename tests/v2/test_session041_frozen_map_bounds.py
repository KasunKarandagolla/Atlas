"""Large immutable universe maps keep lookup work logarithmic."""
from __future__ import annotations

import pytest

from atlas.v2._serialization import FrozenMap, canonical_json


class CountedKey(str):
    comparisons = 0

    def __lt__(self, other):
        type(self).comparisons += 1
        return super().__lt__(other)

    def __gt__(self, other):
        type(self).comparisons += 1
        return super().__gt__(other)


def test_full_universe_lookup_has_logarithmic_comparison_bound():
    data = {CountedKey(f"instrument-{number:05d}"): number for number in range(4096)}
    frozen = FrozenMap(data)
    for number in (0, 1, 1023, 2048, 4095):
        CountedKey.comparisons = 0
        assert frozen[f"instrument-{number:05d}"] == number
        assert CountedKey.comparisons <= 26
    CountedKey.comparisons = 0
    assert frozen.get("instrument-99999") is None
    assert CountedKey.comparisons <= 26


def test_lookup_and_refreezing_preserve_canonical_wire_and_ownership():
    source = {"z": [1, {"a": "δ"}], "a": {"child": [True, None]}}
    frozen = FrozenMap(source)
    assert canonical_json(frozen) == canonical_json(source)
    assert FrozenMap(frozen) == FrozenMap.from_json(frozen) == frozen
    source["z"].append(9)
    assert frozen["z"] == (1, FrozenMap({"a": "δ"}))
    with pytest.raises(KeyError):
        frozen[1]
    with pytest.raises(KeyError):
        frozen["missing"]
