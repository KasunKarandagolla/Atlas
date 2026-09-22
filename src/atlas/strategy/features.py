"""Exact finite-window feature calculation for CRYPTO_TREND_24H_V1."""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal

HOUR_NS = 3_600_000_000_000
WINDOW_CLOSES = 721
WINDOW_RETURNS = 720
EWMA_SEED_COUNT = 48
EWMA_HALF_LIFE_HOURS = 48
VOLATILITY_FLOOR = 0.0001


class FeatureWindowError(ValueError):
    """The causal close window cannot define the frozen feature."""


@dataclass(frozen=True, order=True)
class HourlyClose:
    end_at_ns: int
    close: Decimal
    available_at_ns: int
    record_id: str = ""

    def __post_init__(self) -> None:
        if self.end_at_ns < 0 or self.available_at_ns < 0:
            raise FeatureWindowError("timestamps must be UTC nanoseconds")
        if self.end_at_ns % HOUR_NS:
            raise FeatureWindowError("hourly close must end on an UTC hour")
        if self.close <= 0:
            raise FeatureWindowError("non-positive close")


@dataclass(frozen=True)
class FeatureValues:
    end_at_ns: int
    returns: tuple[float, ...]
    variance: float
    sigma: float
    z: float


def validate_closes(closes: Iterable[HourlyClose], *, decision_at_ns: int | None = None) -> tuple[HourlyClose, ...]:
    """Validate exactly one causal, contiguous 721-close window.

    Conflicting duplicate timestamps are rejected rather than silently choosing a
    revised record.  An identical duplicate is also rejected: it makes source
    identity ambiguous and is not a 721-record window.
    """
    result = tuple(sorted(closes, key=lambda x: x.end_at_ns))
    if len(result) != WINDOW_CLOSES:
        raise FeatureWindowError(f"requires exactly {WINDOW_CLOSES} closes")
    previous: HourlyClose | None = None
    for item in result:
        if decision_at_ns is not None and item.available_at_ns > decision_at_ns:
            raise FeatureWindowError("future/unavailable close")
        if previous is not None:
            if item.end_at_ns == previous.end_at_ns:
                raise FeatureWindowError("duplicate close")
            if item.end_at_ns != previous.end_at_ns + HOUR_NS:
                raise FeatureWindowError("missing hourly close")
        previous = item
    return result


def returns_from_closes(closes: Iterable[HourlyClose], *, decision_at_ns: int | None = None) -> tuple[float, ...]:
    window = validate_closes(closes, decision_at_ns=decision_at_ns)
    return tuple(math.log(float(b.close / a.close)) for a, b in zip(window, window[1:], strict=False))


def finite_window_variance(returns: Iterable[float]) -> float:
    values = tuple(returns)
    if len(values) != WINDOW_RETURNS:
        raise FeatureWindowError(f"requires exactly {WINDOW_RETURNS} returns")
    if not all(math.isfinite(x) for x in values):
        raise FeatureWindowError("non-finite return")
    v = sum(x * x for x in values[:EWMA_SEED_COUNT]) / EWMA_SEED_COUNT
    decay = 2 ** (-1 / EWMA_HALF_LIFE_HOURS)
    for r in values[EWMA_SEED_COUNT:]:
        v = decay * v + (1.0 - decay) * r * r
    return v


def finite_window_sigma(returns: Iterable[float]) -> float:
    return math.sqrt(max(finite_window_variance(returns), 1e-8))


def feature_values(closes: Iterable[HourlyClose], *, decision_at_ns: int | None = None) -> FeatureValues:
    window = validate_closes(closes, decision_at_ns=decision_at_ns)
    returns = returns_from_closes(window, decision_at_ns=decision_at_ns)
    variance = finite_window_variance(returns)
    sigma = math.sqrt(max(variance, 1e-8))
    z = math.log(float(window[-1].close / window[-25].close)) / (math.sqrt(24) * sigma)
    return FeatureValues(window[-1].end_at_ns, returns, variance, sigma, z)
