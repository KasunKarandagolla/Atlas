"""Synchronized residual-block archive and non-circular sampling."""

from __future__ import annotations

import math
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
    # Immutable synchronized raw/replay evidence.  Empty means genuinely absent,
    # never synthesized candle/depth/fill information.
    btc_last_ohlc: tuple[tuple[float, float, float, float], ...] = ()
    eth_last_ohlc: tuple[tuple[float, float, float, float], ...] = ()
    btc_mark_ohlc: tuple[tuple[float, float, float, float], ...] = ()
    eth_mark_ohlc: tuple[tuple[float, float, float, float], ...] = ()
    btc_index_ohlc: tuple[tuple[float, float, float, float], ...] = ()
    eth_index_ohlc: tuple[tuple[float, float, float, float], ...] = ()
    source_class: str = ""
    funding_publication_at_ns: int | None = None
    funding_settlement_at_ns: int | None = None
    execution_missing: bool = True
    btc_feature_ref: str = ""
    eth_feature_ref: str = ""
    opening_gaps: tuple[float, ...] = ()
    excursions: tuple[float, ...] = ()
    spread_depth_observations: tuple[dict[str, str], ...] = ()
    latency_fill_observations: tuple[dict[str, str], ...] = ()
    funding_observations: tuple[dict[str, str], ...] = ()
    availability_class: str = ""
    replay_mode: str = ""
    evidence_hashes: tuple[str, ...] = ()
    minute_replay_complete: bool = False

    def __post_init__(self) -> None:
        values = (self.btc_residual, self.eth_residual, self.btc_forecast, self.eth_forecast,
                  self.btc_sigma, self.eth_sigma, self.btc_z, self.eth_z)
        if not all(math.isfinite(x) for x in values) or self.btc_sigma <= 0 or self.eth_sigma <= 0:
            raise ValueError("finite residual/forecast/features and positive sigmas required")
        minute_series = (self.btc_last_ohlc, self.eth_last_ohlc, self.btc_mark_ohlc,
                         self.eth_mark_ohlc, self.btc_index_ohlc, self.eth_index_ohlc)
        if self.minute_replay_complete and any(len(series) != 60 for series in minute_series):
            raise ValueError("complete minute replay requires exactly 60 last/mark/index bars per instrument")
        if any(series and len(series) != 60 for series in minute_series):
            raise ValueError("minute evidence must be absent or exactly 60 bars")


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


def _energy_score(samples: Sequence[tuple[float, float]], observation: tuple[float, float]) -> float:
    """Standard finite-sample multivariate energy score (deterministic O(n²))."""
    if not samples:
        raise ValueError("samples required")
    def distance(a: tuple[float, float], b: tuple[float, float]) -> float:
        return ((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2) ** 0.5
    first = sum(distance(x, observation) for x in samples) / len(samples)
    second = sum(distance(a, b) for a in samples for b in samples) / (2 * len(samples) ** 2)
    return first - second


def chronological_energy_scores(
    training: Sequence[JointResidualHour], validation: Sequence[JointResidualHour], *, training_btc_sigma: float, training_eth_sigma: float,
) -> dict[int, float]:
    """Frozen 4h/24h equal-weight joint-return validation objective.

    Each candidate's empirical scenarios are contiguous training blocks.  Cost
    and barrier observations are intentionally not included in this objective.
    """
    if training_btc_sigma <= 0 or training_eth_sigma <= 0:
        raise ValueError("positive training volatility required")
    scores: dict[int, float] = {}
    for length in BLOCK_CANDIDATES:
        starts = eligible_starts(training, length)
        if len(starts) < 2:
            raise ValueError("NOT_ESTIMABLE: insufficient effectively independent block history")
        component_scores = []
        for horizon in (4, 24):
            if len(validation) < horizon or length < horizon:
                raise ValueError("NOT_ESTIMABLE: validation horizon unavailable")
            samples = [
                (sum(x.btc_sigma * (x.btc_forecast + x.btc_residual) for x in training[s : s + horizon]) / training_btc_sigma,
                 sum(x.eth_sigma * (x.eth_forecast + x.eth_residual) for x in training[s : s + horizon]) / training_eth_sigma)
                for s in starts
            ]
            for begin in range(0, len(validation) - horizon + 1, horizon):
                actual = (
                    sum(x.btc_sigma * (x.btc_forecast + x.btc_residual) for x in validation[begin : begin + horizon]) / training_btc_sigma,
                    sum(x.eth_sigma * (x.eth_forecast + x.eth_residual) for x in validation[begin : begin + horizon]) / training_eth_sigma,
                )
                component_scores.append(_energy_score(samples, actual))
        scores[length] = sum(component_scores) / len(component_scores)
    return scores
