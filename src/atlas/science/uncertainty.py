"""Outer conditional-mean uncertainty definitions (separate from path dispersion)."""

from __future__ import annotations

import math
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
