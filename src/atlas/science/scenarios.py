"""Frozen one-minute empirical bridge for synchronized last/mark/index paths.

The bridge rescales *historical* archived minute microstructure onto the current
causal snapshot.  It never synthesizes missing microstructure: an hour whose
last/mark/index minute evidence is absent is ``NOT_ESTIMABLE``, and mark/index
minutes are always paired with the same sampled BTC/ETH residual block as the
last minutes rather than being resampled independently.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

from .residual_blocks import JointResidualHour, sample_blocks

PRODUCTION_PATHS = 2_048
HORIZON_HOURS = 24
MINUTES_PER_HOUR = 60
RECONSTRUCTION_TOLERANCE = 1e-9
INSTRUMENTS = ("BTCUSDT", "ETHUSDT")


class ScenarioNotEstimable(ValueError):
    """The frozen scenario cannot be valued from the supplied causal evidence."""


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
        if not all(math.isfinite(x) for x in (self.open, self.high, self.low, self.close)):
            raise ValueError("finite OHLC required")


@dataclass(frozen=True)
class SynchronizedMinute:
    """Paired last/mark/index minute with its historical normalized innovations."""

    last: MinuteOHLC
    mark: MinuteOHLC
    index: MinuteOHLC
    at_ns: int = 0
    return_innovation: float = 0.0
    opening_gap: float = 0.0
    high_excursion: float = 0.0
    low_excursion: float = 0.0
    mark_basis: float = 0.0
    index_basis: float = 0.0


@dataclass(frozen=True)
class SynchronizedPrices:
    last: float
    mark: float
    index: float

    def __post_init__(self) -> None:
        if min(self.last, self.mark, self.index) <= 0:
            raise ValueError("positive causal anchor prices required")


@dataclass(frozen=True)
class BridgeSupport:
    min_volatility_ratio: float = 0.5
    max_volatility_ratio: float = 2.0
    max_abs_basis_log: float = 0.02
    barrier_calibrated: bool = True
    reconstruction_tolerance: float = RECONSTRUCTION_TOLERANCE


@dataclass(frozen=True)
class JointMinutePaths:
    """One sampled residual block replayed as paired BTC/ETH minute paths."""

    btc: tuple[SynchronizedMinute, ...]
    eth: tuple[SynchronizedMinute, ...]


def bridge_return(archive_close: float, archive_previous_close: float, archive_sigma: float, archive_mu: float,
                  current_sigma: float, current_mu: float) -> float:
    if archive_close <= 0 or archive_previous_close <= 0 or archive_sigma <= 0 or current_sigma <= 0:
        raise ValueError("positive prices/sigmas required")
    e = math.log(archive_close / archive_previous_close) / archive_sigma - archive_mu / 60
    return current_sigma * (current_mu / 60 + e)


def bridge_minute(previous: float, archive: MinuteOHLC, archive_previous_close: float, archive_sigma: float,
                  archive_mu: float, current_sigma: float, current_mu: float) -> MinuteOHLC:
    ret = bridge_return(archive.close, archive_previous_close, archive_sigma, archive_mu, current_sigma, current_mu)
    close = previous * math.exp(ret)
    scale = current_sigma / archive_sigma
    # Preserve the signed log excursions around the archive open/close envelope.
    high_exc = max(0.0, math.log(archive.high / max(archive.open, archive.close))) * scale
    low_exc = max(0.0, math.log(min(archive.open, archive.close) / archive.low)) * scale
    return MinuteOHLC(previous, max(previous, close) * math.exp(high_exc), min(previous, close) * math.exp(-low_exc), close)


def _scale_innovations(innovations: Sequence[float], target: float) -> tuple[list[float], float]:
    if not innovations:
        raise ScenarioNotEstimable("broken OHLC reconstruction")
    total = math.fsum(innovations)
    factor = target / total if abs(total) > 1e-12 else 0.0
    if not 0.25 <= factor <= 4.0:
        # A sign flip or an extreme rescale would distort/invert the archived
        # intrabar shape; the hour level is instead shifted rigidly.
        drift = (target - total) / len(innovations)
        return [x + drift for x in innovations], 1.0
    return [x * factor for x in innovations], factor


def reconstruct_minute_series(previous_close: float, bars: Sequence[MinuteOHLC], *, target_return: float,
                              excursion_scale: float) -> tuple[tuple[MinuteOHLC, ...], tuple[float, ...], tuple[float, ...]]:
    """Rescale an archived 60-bar hour onto ``target_return`` without clipping."""
    if len(bars) != MINUTES_PER_HOUR:
        raise ScenarioNotEstimable("incomplete minute archive")
    if previous_close <= 0 or not math.isfinite(target_return):
        raise ScenarioNotEstimable("invalid bridge anchor/return")
    innovations: list[float] = []
    gaps: list[float] = []
    intrabar: list[float] = []
    for index, bar in enumerate(bars):
        # The first simulated bar opens exactly at the causal anchor.
        gap = 0.0 if index == 0 else math.log(bar.open / bars[index - 1].close)
        inner = math.log(bar.close / bar.open)
        gaps.append(gap)
        intrabar.append(inner)
        innovations.append(gap + inner)
    scaled, _ = _scale_innovations(innovations, target_return)
    output: list[MinuteOHLC] = []
    scaled_gaps: list[float] = []
    previous = previous_close
    for index, bar in enumerate(bars):
        inner = scaled[index] - gaps[index]
        open_price = previous * math.exp(gaps[index])
        close_price = open_price * math.exp(inner)
        high_exc = max(0.0, math.log(bar.high / max(bar.open, bar.close))) * excursion_scale
        low_exc = max(0.0, math.log(min(bar.open, bar.close) / bar.low)) * excursion_scale
        output.append(MinuteOHLC(open_price, max(open_price, close_price) * math.exp(high_exc),
                                 min(open_price, close_price) * math.exp(-low_exc), close_price))
        scaled_gaps.append(gaps[index])
        previous = close_price
    return tuple(output), tuple(scaled_gaps), tuple(scaled)


def _hour_return(hour: JointResidualHour, instrument: str) -> tuple[float, float]:
    if instrument == "BTCUSDT":
        sigma, forecast, residual = hour.btc_sigma, hour.btc_forecast, hour.btc_residual
    elif instrument == "ETHUSDT":
        sigma, forecast, residual = hour.eth_sigma, hour.eth_forecast, hour.eth_residual
    else:
        raise ValueError("frozen V1 universe is BTCUSDT/ETHUSDT")
    return sigma * (forecast + residual), sigma


def _minute_bars(hour: JointResidualHour, instrument: str, series: str) -> tuple[tuple[float, float, float, float], ...]:
    if instrument == "BTCUSDT":
        return getattr(hour, f"btc_{series}_ohlc")
    return getattr(hour, f"eth_{series}_ohlc")


def bridge_hour(hour: JointResidualHour, instrument: str, *, previous: SynchronizedPrices,
                archive_sigma: float, support: BridgeSupport,
                start_at_ns: int | None = None) -> tuple[SynchronizedMinute, ...]:
    """Replay one archived hour of one instrument as 60 synchronized minutes."""
    if archive_sigma <= 0:
        raise ScenarioNotEstimable("missing archive volatility")
    if not support.barrier_calibrated:
        raise ScenarioNotEstimable("poor barrier calibration")
    target_return, hour_sigma = _hour_return(hour, instrument)
    ratio = hour_sigma / archive_sigma
    if not support.min_volatility_ratio <= ratio <= support.max_volatility_ratio:
        raise ScenarioNotEstimable("unsupported current volatility")
    series: dict[str, tuple[MinuteOHLC, ...]] = {}
    for name in ("last", "mark", "index"):
        raw = _minute_bars(hour, instrument, name)
        if len(raw) != MINUTES_PER_HOUR:
            raise ScenarioNotEstimable("insufficient mark/index archive support")
        series[name] = tuple(MinuteOHLC(*bar) for bar in raw)
    paths: dict[str, tuple[MinuteOHLC, ...]] = {}
    for name, anchor in (("last", previous.last), ("mark", previous.mark), ("index", previous.index)):
        bars, _, _ = reconstruct_minute_series(anchor, series[name], target_return=target_return,
                                               excursion_scale=ratio)
        paths[name] = bars
    minute_ns = 60_000_000_000
    output: list[SynchronizedMinute] = []
    previous_last = previous.last
    for index in range(MINUTES_PER_HOUR):
        last_bar = paths["last"][index]
        mark_bar = paths["mark"][index]
        index_bar = paths["index"][index]
        innovation = math.log(last_bar.close / previous_last)
        gap = math.log(last_bar.open / previous_last)
        high_exc = math.log(last_bar.high / max(last_bar.open, last_bar.close))
        low_exc = math.log(min(last_bar.open, last_bar.close) / last_bar.low)
        mark_basis = math.log(mark_bar.close / last_bar.close)
        index_basis = math.log(index_bar.close / last_bar.close)
        if abs(mark_basis) > support.max_abs_basis_log or abs(index_basis) > support.max_abs_basis_log:
            raise ScenarioNotEstimable("excess basis drift")
        output.append(SynchronizedMinute(last_bar, mark_bar, index_bar,
                                         at_ns=(hour.at_ns if start_at_ns is None else start_at_ns) + index * minute_ns,
                                         return_innovation=innovation, opening_gap=gap,
                                         high_excursion=high_exc, low_excursion=low_exc,
                                         mark_basis=mark_basis, index_basis=index_basis))
        previous_last = last_bar.close
    realised = math.log(output[-1].last.close / previous.last)
    if abs(realised - target_return) > support.reconstruction_tolerance:
        raise ScenarioNotEstimable("broken OHLC reconstruction")
    return tuple(output)


DEFAULT_BRIDGE_SUPPORT = BridgeSupport()


def joint_minute_paths(block: Sequence[JointResidualHour], *, initial: dict[str, SynchronizedPrices],
                       archive_sigma: dict[str, float],
                       support: BridgeSupport = DEFAULT_BRIDGE_SUPPORT) -> JointMinutePaths:
    """Bridge one sampled BTC/ETH block; mark/index stay paired with the same block."""
    if not initial or set(initial) - set(INSTRUMENTS) or set(initial) != set(archive_sigma):
        raise ScenarioNotEstimable("frozen instrument set requires matching causal anchors")
    built: dict[str, list[SynchronizedMinute]] = {"BTCUSDT": [], "ETHUSDT": []}
    current = dict(initial)
    base_at_ns = block[0].at_ns if block else 0
    for minute, hour in enumerate(block):
        for instrument in initial:
            minutes = bridge_hour(hour, instrument, previous=current[instrument],
                                  archive_sigma=archive_sigma[instrument], support=support,
                                  start_at_ns=base_at_ns + minute * MINUTES_PER_HOUR * 60_000_000_000)
            built[instrument].extend(minutes)
            final = minutes[-1]
            current[instrument] = SynchronizedPrices(final.last.close, final.mark.close, final.index.close)
    return JointMinutePaths(tuple(built["BTCUSDT"]), tuple(built["ETHUSDT"]))


def joint_paths(hours: Sequence[JointResidualHour], *, block_length: int = 24, paths: int = PRODUCTION_PATHS,
                seed: int = 0) -> tuple[tuple[JointResidualHour, ...], ...]:
    return sample_blocks(hours, length=block_length, horizon_hours=HORIZON_HOURS, paths=paths, seed=seed)
