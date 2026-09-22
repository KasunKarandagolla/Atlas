"""Outer conditional-mean uncertainty definitions (separate from path dispersion)."""

from __future__ import annotations

import math
import random
from collections.abc import Callable, Sequence
from dataclasses import dataclass
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


@dataclass(frozen=True)
class ReplicateEvaluation:
    mean_pnl: float
    action_hash: str
    scaler_refit: bool
    ridge_reselected: bool
    chronological_oof_rebuilt: bool
    costs_reestimated: bool


@dataclass(frozen=True)
class OuterBootstrapResult:
    replicate_means: tuple[float, ...]
    lcb: float
    numerical_unstable: bool
    status: str
    replicate_seed: int
    inner_seed_a: int
    inner_seed_b: int


def outer_expected_mean_bootstrap[T](
    history: Sequence[T], *, block_length: int, action_hash: str,
    evaluator: Callable[[tuple[T, ...], int, int], ReplicateEvaluation],
    replicates: int = BOOTSTRAP_REPLICATES, inner_paths: int = BOOTSTRAP_INNER_PATHS,
    replicate_seed: int = 1, inner_seed_a: int = 2, inner_seed_b: int = 3,
    delta: float = INSTRUMENT_DELTA,
) -> OuterBootstrapResult:
    """Run refit/OOF/cost/action replicates and the two-inner-seed stability check."""
    index_sets = block_bootstrap_indices(len(history), block_length=block_length, count=replicates, seed=replicate_seed)
    means_a: list[float] = []
    means_b: list[float] = []
    for indices in index_sets:
        sample = tuple(history[index] for index in indices)
        a = evaluator(sample, inner_seed_a, inner_paths)
        b = evaluator(sample, inner_seed_b, inner_paths)
        for result in (a, b):
            if result.action_hash != action_hash:
                raise ValueError("bootstrap changed immutable current action")
            if not (result.scaler_refit and result.ridge_reselected and result.chronological_oof_rebuilt and result.costs_reestimated):
                raise ValueError("bootstrap replicate omitted frozen refit/OOF/cost stage")
        means_a.append(a.mean_pnl)
        means_b.append(b.mean_pnl)
    lcb, unstable = bootstrap_lcb(means_a, delta=delta, seed_a_means=means_a, seed_b_means=means_b)
    return OuterBootstrapResult(tuple(means_a), lcb, unstable,
                                "NO_TRADE_NUMERICAL" if unstable else "ESTIMATED",
                                replicate_seed, inner_seed_a, inner_seed_b)
