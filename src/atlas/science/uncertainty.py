"""Outer conditional-mean uncertainty definitions (separate from path dispersion)."""

from __future__ import annotations

import math
import random
from collections.abc import Sequence
from statistics import fmean

BOOTSTRAP_REPLICATES = 200
BOOTSTRAP_INNER_PATHS = 512
FAMILY_ERROR = 0.05
INSTRUMENT_DELTA = 0.025


def lcb_order_statistic(means: Sequence[float], delta: float = INSTRUMENT_DELTA) -> float:
    if not means or not 0 < delta < 1:
        raise ValueError("nonempty means and valid delta required")
    ordered = sorted(means)
    # Freeze: 1-indexed ceil(B*delta), converted to a zero-indexed list position.
    index = max(0, math.ceil(len(ordered) * delta) - 1)
    return ordered[index]


def path_mean(pnls: Sequence[float]) -> float:
    if not pnls:
        raise ValueError("paths required")
    return fmean(pnls)


def block_bootstrap_indices(length: int, *, block_length: int, count: int, seed: int) -> tuple[tuple[int, ...], ...]:
    """Non-circular chronological block bootstrap index sets, with explicit RNG."""
    if length < block_length or block_length < 1 or count < 1:
        raise ValueError("valid block bootstrap dimensions required")
    rng = random.Random(seed)
    output = []
    for _ in range(count):
        indices: list[int] = []
        while len(indices) < length:
            start = rng.randrange(length - block_length + 1)
            indices.extend(range(start, start + block_length))
        output.append(tuple(indices[:length]))
    return tuple(output)


def bootstrap_lcb(
    replicate_means: Sequence[float], *, delta: float = INSTRUMENT_DELTA,
    seed_a_means: Sequence[float] | None = None, seed_b_means: Sequence[float] | None = None,
    numerical_boundary: float = 0.0,
) -> tuple[float, bool]:
    """Return frozen LCB and whether independent inner-seed noise can flip it."""
    lcb = lcb_order_statistic(replicate_means, delta)
    unstable = False
    if seed_a_means is not None and seed_b_means is not None:
        unstable = (lcb_order_statistic(seed_a_means, delta) > numerical_boundary) != (lcb_order_statistic(seed_b_means, delta) > numerical_boundary)
    return lcb, unstable
