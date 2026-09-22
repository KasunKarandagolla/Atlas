"""Frozen current-state minute bridge for synchronized last/mark/index paths.

The bridge applies the **current decision model state** to historical sampled
minute innovations::

    epsilon_j    = archive_minute_return_j - mean_archive_minute_return
    r*_j         = current_sigma * (current_mu / 60 + epsilon_j / archive_sigma
                                    + archive_residual / 60)

which is algebraically the frozen bridge
``epsilon = archive_return / archive_sigma - archive_mu / 60`` followed by
``r*_minute = current_sigma * (current_mu / 60 + epsilon)``.  The archived hour
return is therefore never replayed unchanged: the simulated path always depends
on the current decision model state (``current_sigma``, ``current_mu``).

Mark and index minutes stay paired with the same sampled block and preserve the
sampled relative basis behaviour.  Missing microstructure is never synthesized.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal

from atlas.strategy.features import HOUR_NS, HourlyClose, feature_values

from .huber_mean import HuberRidgeModel
from .residual_blocks import JointResidualHour, sample_blocks

PRODUCTION_PATHS = 2_048
HORIZON_HOURS = 24
MINUTES_PER_HOUR = 60
MINUTE_NS = 60_000_000_000
RECONSTRUCTION_TOLERANCE = 1e-6
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
    """Paired last/mark/index minute with its simulated normalized innovations."""

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
class CurrentModelState:
    """The current decision model state for one instrument."""

    sigma: float
    mu: float

    def __post_init__(self) -> None:
        if not math.isfinite(self.sigma) or self.sigma <= 0 or not math.isfinite(self.mu):
            raise ValueError("finite positive current sigma and finite current mu required")


@dataclass(frozen=True)
class BridgeSupport:
    min_volatility_ratio: float = 0.5
    max_volatility_ratio: float = 2.0
    max_abs_basis_log: float = 0.02
    barrier_calibrated: bool = True
    reconstruction_tolerance: float = RECONSTRUCTION_TOLERANCE


DEFAULT_BRIDGE_SUPPORT = BridgeSupport()


@dataclass(frozen=True)
class JointMinutePaths:
    """One sampled residual block replayed as paired BTC/ETH minute paths."""

    btc: tuple[SynchronizedMinute, ...]
    eth: tuple[SynchronizedMinute, ...]


def bridge_return(archive_close: float, archive_previous_close: float, archive_sigma: float, archive_mu: float,
                  current_sigma: float, current_mu: float) -> float:
    """The frozen single-minute bridge formula (kept as the low-level primitive)."""
    if archive_close <= 0 or archive_previous_close <= 0 or archive_sigma <= 0 or current_sigma <= 0:
        raise ValueError("positive prices/sigmas required")
    epsilon = math.log(archive_close / archive_previous_close) / archive_sigma - archive_mu / MINUTES_PER_HOUR
    return current_sigma * (current_mu / MINUTES_PER_HOUR + epsilon)


def bridge_minute(previous: float, archive: MinuteOHLC, archive_previous_close: float, archive_sigma: float,
                  archive_mu: float, current_sigma: float, current_mu: float) -> MinuteOHLC:
    ret = bridge_return(archive.close, archive_previous_close, archive_sigma, archive_mu, current_sigma, current_mu)
    close = previous * math.exp(ret)
    scale = current_sigma / archive_sigma
    high_exc = max(0.0, math.log(archive.high / max(archive.open, archive.close))) * scale
    low_exc = max(0.0, math.log(min(archive.open, archive.close) / archive.low)) * scale
    return MinuteOHLC(previous, max(previous, close) * math.exp(high_exc), min(previous, close) * math.exp(-low_exc), close)


def _instrument_state(hour: JointResidualHour, instrument: str) -> tuple[float, float, float]:
    if instrument == "BTCUSDT":
        return hour.btc_sigma, hour.btc_forecast, hour.btc_residual
    if instrument == "ETHUSDT":
        return hour.eth_sigma, hour.eth_forecast, hour.eth_residual
    raise ValueError("frozen V1 universe is BTCUSDT/ETHUSDT")


def _minute_bars(hour: JointResidualHour, instrument: str, series: str) -> tuple[tuple[float, float, float, float], ...]:
    prefix = "btc" if instrument == "BTCUSDT" else "eth"
    return getattr(hour, f"{prefix}_{series}_ohlc")


def _series_time_return(bars: Sequence[MinuteOHLC]) -> float:
    return math.log(bars[-1].close / bars[0].open)


def _simulate_series(anchor: float, bars: Sequence[MinuteOHLC], *, archive_sigma: float, residual: float,
                     current: CurrentModelState, excursion_scale: float) -> tuple[tuple[MinuteOHLC, ...], tuple[float, ...]]:
    """Apply the frozen current-state bridge to one archived minute series."""
    hour_return = _series_time_return(bars)
    mean_return = hour_return / MINUTES_PER_HOUR
    simulated: list[MinuteOHLC] = []
    innovations: list[float] = []
    previous = anchor
    archive_previous = bars[0].open
    for bar in bars:
        raw = math.log(bar.close / archive_previous)
        innovation = raw - mean_return
        ret = current.sigma * (current.mu / MINUTES_PER_HOUR + innovation / archive_sigma + residual / MINUTES_PER_HOUR)
        close = previous * math.exp(ret)
        high_exc = max(0.0, math.log(bar.high / max(bar.open, bar.close))) * excursion_scale
        low_exc = max(0.0, math.log(min(bar.open, bar.close) / bar.low)) * excursion_scale
        simulated.append(MinuteOHLC(previous, max(previous, close) * math.exp(high_exc),
                                    min(previous, close) * math.exp(-low_exc), close))
        innovations.append(innovation)
        previous = close
        archive_previous = bar.close
    return tuple(simulated), tuple(innovations)


def bridge_hour(hour: JointResidualHour, instrument: str, *, previous: SynchronizedPrices,
                current: CurrentModelState, support: BridgeSupport = DEFAULT_BRIDGE_SUPPORT,
                start_at_ns: int | None = None) -> tuple[SynchronizedMinute, ...]:
    """Replay one archived hour of one instrument as 60 current-state minutes."""
    archive_sigma, archive_mu, residual = _instrument_state(hour, instrument)
    if archive_sigma <= 0:
        raise ScenarioNotEstimable("missing archive volatility")
    if not support.barrier_calibrated:
        raise ScenarioNotEstimable("poor barrier calibration")
    ratio = current.sigma / archive_sigma
    if not support.min_volatility_ratio <= ratio <= support.max_volatility_ratio:
        raise ScenarioNotEstimable("unsupported current volatility")
    raw: dict[str, tuple[MinuteOHLC, ...]] = {}
    for name in ("last", "mark", "index"):
        bars = _minute_bars(hour, instrument, name)
        if len(bars) != MINUTES_PER_HOUR:
            raise ScenarioNotEstimable("insufficient mark/index archive support")
        raw[name] = tuple(MinuteOHLC(*bar) for bar in bars)
    realised = _series_time_return(raw["last"]) / archive_sigma
    if abs(realised - (archive_mu + residual)) > support.reconstruction_tolerance:
        raise ScenarioNotEstimable("incoherent archived hour evidence")
    simulated: dict[str, tuple[MinuteOHLC, ...]] = {}
    for name, anchor in (("last", previous.last), ("mark", previous.mark), ("index", previous.index)):
        simulated[name], _ = _simulate_series(anchor, raw[name], archive_sigma=archive_sigma, residual=residual,
                                              current=current, excursion_scale=ratio)
    base_at_ns = hour.at_ns if start_at_ns is None else start_at_ns
    output: list[SynchronizedMinute] = []
    previous_last = previous.last
    for index in range(MINUTES_PER_HOUR):
        last_bar = simulated["last"][index]
        mark_bar = simulated["mark"][index]
        index_bar = simulated["index"][index]
        mark_basis = math.log(mark_bar.close / last_bar.close)
        index_basis = math.log(index_bar.close / last_bar.close)
        if abs(mark_basis) > support.max_abs_basis_log or abs(index_basis) > support.max_abs_basis_log:
            raise ScenarioNotEstimable("excess basis drift")
        output.append(SynchronizedMinute(last_bar, mark_bar, index_bar, at_ns=base_at_ns + index * MINUTE_NS,
                                         return_innovation=math.log(last_bar.close / previous_last),
                                         opening_gap=math.log(last_bar.open / previous_last),
                                         high_excursion=math.log(last_bar.high / max(last_bar.open, last_bar.close)),
                                         low_excursion=math.log(min(last_bar.open, last_bar.close) / last_bar.low),
                                         mark_basis=mark_basis, index_basis=index_basis))
        previous_last = last_bar.close
    return tuple(output)


def simulated_feature_state(closes: Sequence[HourlyClose], model: HuberRidgeModel, instrument: str) -> CurrentModelState:
    """Current sigma^*/mu^* from the frozen finite-window feature engine."""
    values = feature_values(closes)
    return CurrentModelState(sigma=values.sigma, mu=model.forecast(instrument, values.z))


def append_simulated_close(closes: Sequence[HourlyClose], close: float, *, record_id: str) -> tuple[HourlyClose, ...]:
    """Evolve the causal 721-close window with one simulated completed hour."""
    last = closes[-1]
    appended = HourlyClose(last.end_at_ns + HOUR_NS, Decimal(str(close)), last.end_at_ns + HOUR_NS, record_id)
    return tuple(closes[1:]) + (appended,)


def joint_minute_paths(block: Sequence[JointResidualHour], *, initial: dict[str, SynchronizedPrices],
                       causal_closes: dict[str, tuple[HourlyClose, ...]], model: HuberRidgeModel,
                       support: BridgeSupport = DEFAULT_BRIDGE_SUPPORT) -> JointMinutePaths:
    """Bridge one sampled BTC/ETH block with the simulated feature state evolving hourly.

    Model coefficients stay frozen; the 721-close window, EWMA sigma and momentum z
    are recomputed from the simulated hourly closes using the frozen feature engine.
    """
    if not block:
        raise ScenarioNotEstimable("empty sampled block")
    if not initial or set(initial) - set(INSTRUMENTS) or set(initial) != set(causal_closes):
        raise ScenarioNotEstimable("frozen instrument set requires matching causal anchors and close windows")
    built: dict[str, list[SynchronizedMinute]] = {"BTCUSDT": [], "ETHUSDT": []}
    prices = dict(initial)
    windows = {instrument: tuple(causal_closes[instrument]) for instrument in initial}
    base_at_ns = block[0].at_ns
    for index, hour in enumerate(block):
        states = {instrument: simulated_feature_state(windows[instrument], model, instrument) for instrument in initial}
        for instrument in initial:
            minutes = bridge_hour(hour, instrument, previous=prices[instrument], current=states[instrument],
                                  support=support, start_at_ns=base_at_ns + index * HOUR_NS)
            built[instrument].extend(minutes)
            final = minutes[-1]
            windows[instrument] = append_simulated_close(windows[instrument], final.last.close,
                                                         record_id=f"sim-{instrument}-{index}")
            prices[instrument] = SynchronizedPrices(final.last.close, final.mark.close, final.index.close)
    return JointMinutePaths(tuple(built["BTCUSDT"]), tuple(built["ETHUSDT"]))


def joint_paths(hours: Sequence[JointResidualHour], *, block_length: int = 24, paths: int = PRODUCTION_PATHS,
                seed: int = 0) -> tuple[tuple[JointResidualHour, ...], ...]:
    return sample_blocks(hours, length=block_length, horizon_hours=HORIZON_HOURS, paths=paths, seed=seed)
