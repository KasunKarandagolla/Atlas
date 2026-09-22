"""Deterministic scanner ranking and past-only correlation clusters."""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence

from .models import CheapScanObservation, RankBand, RankedObservation

CORRELATION_CLUSTER_THRESHOLD = 0.70


def _pearson(left: Sequence[float], right: Sequence[float]) -> float:
    count = min(len(left), len(right))
    if count < 2:
        return 0.0
    a = tuple(float(x) for x in left[-count:])
    b = tuple(float(x) for x in right[-count:])
    if not all(math.isfinite(x) for x in a + b):
        return 0.0
    mean_a = sum(a) / count
    mean_b = sum(b) / count
    covariance = sum((x - mean_a) * (y - mean_b) for x, y in zip(a, b, strict=True))
    variance_a = sum((x - mean_a) ** 2 for x in a)
    variance_b = sum((y - mean_b) ** 2 for y in b)
    if variance_a <= 0 or variance_b <= 0:
        return 0.0
    return covariance / math.sqrt(variance_a * variance_b)


def correlation_clusters(return_histories: Mapping[str, Sequence[float]],
                         *, threshold: float = CORRELATION_CLUSTER_THRESHOLD) -> dict[str, str]:
    """Assign stable clusters using only each instrument's past return history."""
    if threshold < -1 or threshold > 1:
        raise ValueError("correlation threshold must be in [-1, 1]")
    representatives: list[str] = []
    clusters: dict[str, str] = {}
    for instrument in sorted(return_histories):
        history = return_histories[instrument]
        assigned: str | None = None
        for index, representative in enumerate(representatives):
            if _pearson(history, return_histories[representative]) >= threshold:
                assigned = f"cluster-{index + 1:02d}"
                break
        if assigned is None:
            representative = instrument
            representatives.append(representative)
            assigned = f"cluster-{len(representatives):02d}"
        clusters[instrument] = assigned
    return clusters


def rank_band(rank: int) -> RankBand:
    if rank < 1:
        raise ValueError("rank must be positive")
    if rank <= 3:
        return RankBand.TOP_3
    if rank <= 10:
        return RankBand.B1
    if rank <= 20:
        return RankBand.B2
    return RankBand.B3


def rank_observations(observations: Iterable[CheapScanObservation], *, universe_hash: str,
                      return_histories: Mapping[str, Sequence[float]] | None = None,
                      ) -> tuple[RankedObservation, ...]:
    """Sort by descending cheap score and canonical instrument ID tie-break."""
    materialized = tuple(observations)
    if not materialized:
        return ()
    slots = {observation.slot_at_ns for observation in materialized}
    if len(slots) != 1:
        raise ValueError("ranking is per scanner slot")
    histories = return_histories or {observation.instrument: () for observation in materialized}
    clusters = correlation_clusters(histories) if histories else {}
    ordered = sorted(materialized, key=lambda item: (-item.score, item.instrument))
    return tuple(RankedObservation(
        slot_at_ns=observation.slot_at_ns,
        instrument=observation.instrument,
        cheap_score=observation.score,
        cheap_observation_hash=observation.hash(),
        universe_hash=universe_hash,
        rank=index,
        tie_break_key=observation.instrument,
        rank_band=rank_band(index),
        correlation_cluster=clusters.get(observation.instrument, "cluster-00"),
    ) for index, observation in enumerate(ordered, start=1))
