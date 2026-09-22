"""Frozen one-minute empirical bridge configuration and deterministic construction."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

from .residual_blocks import JointResidualHour, sample_blocks

PRODUCTION_PATHS = 2_048
HORIZON_HOURS = 24
MINUTES_PER_HOUR = 60


@dataclass(frozen=True)
class MinuteOHLC:
    open: float
    high: float
    low: float
    close: float
    mark_close: float | None = None
    index_close: float | None = None

    def __post_init__(self) -> None:
        if self.high < max(self.open, self.close) or self.low > min(self.open, self.close) or self.low <= 0:
            raise ValueError("OHLC inequalities violated")


def bridge_return(archive_close: float, archive_previous_close: float, archive_sigma: float, archive_mu: float, current_sigma: float, current_mu: float) -> float:
    if archive_close <= 0 or archive_previous_close <= 0 or archive_sigma <= 0 or current_sigma <= 0:
        raise ValueError("positive prices/sigmas required")
    e = math.log(archive_close / archive_previous_close) / archive_sigma - archive_mu / 60
    return current_sigma * (current_mu / 60 + e)


def bridge_minute(previous: float, archive: MinuteOHLC, archive_previous_close: float, archive_sigma: float, archive_mu: float, current_sigma: float, current_mu: float) -> MinuteOHLC:
    ret = bridge_return(archive.close, archive_previous_close, archive_sigma, archive_mu, current_sigma, current_mu)
    close = previous * math.exp(ret)
    scale = current_sigma / archive_sigma
    # Preserve the signed log excursions around the archive open/close envelope.
    high_exc = max(0.0, math.log(archive.high / max(archive.open, archive.close))) * scale
    low_exc = max(0.0, math.log(min(archive.open, archive.close) / archive.low)) * scale
    return MinuteOHLC(previous, max(previous, close) * math.exp(high_exc), min(previous, close) * math.exp(-low_exc), close)


def joint_paths(hours: Sequence[JointResidualHour], *, block_length: int = 24, paths: int = PRODUCTION_PATHS, seed: int = 0) -> tuple[tuple[JointResidualHour, ...], ...]:
    return sample_blocks(hours, length=block_length, horizon_hours=HORIZON_HOURS, paths=paths, seed=seed)
