"""Synchronized residual-block archive and non-circular sampling."""

from __future__ import annotations

import random
from collections.abc import Sequence
from dataclasses import dataclass

BLOCK_CANDIDATES = (24, 48, 72)
MIN_OOF_HOURS = 60 * 24


@dataclass(frozen=True)
class JointResidualHour:
    at_ns: int
    btc_residual: float
    eth_residual: float
    btc_forecast: float
    eth_forecast: float
    btc_sigma: float
    eth_sigma: float
    btc_z: float
    eth_z: float
    complete: bool = True
    calendar_identity: str = ""
    universe_identity: str = "BTCUSDT_ETHUSDT_V1"


def eligible_starts(hours: Sequence[JointResidualHour], length: int) -> tuple[int, ...]:
    if length not in BLOCK_CANDIDATES:
        raise ValueError("unfrozen block length")
    starts = []
    hour_ns = 3_600_000_000_000
    for start in range(0, len(hours) - length + 1):
        segment = hours[start : start + length]
        if all(x.complete for x in segment) and all(
            b.at_ns == a.at_ns + hour_ns for a, b in zip(segment, segment[1:], strict=False)
        ):
            starts.append(start)
    return tuple(starts)


def sample_blocks(hours: Sequence[JointResidualHour], *, length: int, horizon_hours: int, paths: int, seed: int) -> tuple[tuple[JointResidualHour, ...], ...]:
    starts = eligible_starts(hours, length)
    if not starts:
        raise ValueError("NOT_ESTIMABLE: no contiguous synchronized blocks")
    rng = random.Random(seed)
    result = []
    for _ in range(paths):
        path: list[JointResidualHour] = []
        while len(path) < horizon_hours:
            start = starts[rng.randrange(len(starts))]
            path.extend(hours[start : start + length])
        result.append(tuple(path[:horizon_hours]))
    return tuple(result)


def select_block_length(validation_energy_scores: dict[int, float], *, effectively_independent_blocks: dict[int, int]) -> int:
    for length in BLOCK_CANDIDATES:
        if length not in validation_energy_scores or effectively_independent_blocks.get(length, 0) < 2:
            raise ValueError("NOT_ESTIMABLE: inadequate block validation support")
    best = min(validation_energy_scores.values())
    return max(length for length, score in validation_energy_scores.items() if score <= best + 1e-12)
