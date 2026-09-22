"""Deterministic synthetic Phase-4 market fixture (plumbing proof, not profitability).

The fixture builds ~155 days of synchronized BTC/ETH hourly closes plus a
60-day joint residual archive carrying paired last/mark/index minute evidence.
Everything is deterministic and cached for the session; nothing here is a
profitability claim and no exchange is contacted.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from atlas.science.huber_mean import DAY_NS, HOUR_NS, MeanObservation
from atlas.science.oof import OOFArchive
from atlas.science.residual_blocks import BlockSelectionResult, JointResidualHour
from atlas.strategy.features import HourlyClose, finite_window_variance

EPOCH_NS = int(datetime(2025, 1, 6, tzinfo=UTC).timestamp() * 1_000_000_000)  # Monday 00:00 UTC
TOTAL_DAYS = 179
FIRST_REFIT_DAY = 91
BLOCK_FIT_DAY = 175  # Monday
EVALUATION_DAY = 176
RETENTION_DAYS = 91
INSTRUMENTS = ("BTCUSDT", "ETHUSDT")
MINUTE_NS = 60_000_000_000
MINUTES_PER_HOUR = 60
BASE_PRICE = {"BTCUSDT": 100.0, "ETHUSDT": 50.0}
BASE_HOURLY_VOL = 0.0025


def day_ns(day: int) -> int:
    return EPOCH_NS + day * DAY_NS


def hour_ns(hour: int) -> int:
    return EPOCH_NS + hour * HOUR_NS


@dataclass(frozen=True)
class HourlyFeatures:
    z: tuple[float, ...]
    sigma: tuple[float, ...]
    next_return: tuple[float, ...]


@dataclass(frozen=True)
class SyntheticMarket:
    closes: dict[str, tuple[HourlyClose, ...]]
    features: dict[str, HourlyFeatures]
    returns: dict[str, tuple[float, ...]]

    def observations(self, instrument: str) -> list[MeanObservation]:
        values = self.features[instrument]
        return [MeanObservation(hour_ns(index), instrument, values.z[index], values.sigma[index], values.next_return[index])
                for index in range(len(values.z))]

    def window_ending(self, instrument: str, end_at_ns: int) -> tuple[HourlyClose, ...]:
        closes = self.closes[instrument]
        stop = end_at_ns // HOUR_NS - EPOCH_NS // HOUR_NS
        return closes[stop - 720 : stop + 1]


_PATTERN_CACHE: dict[tuple[str, str], tuple[tuple[float, float, float, float], ...]] = {}
SHAPE_PHASE = {
    "last": 0.0,
    "mark": 0.7,
    "index": 1.3,
}


def minute_pattern(instrument: str, kind: str) -> tuple[tuple[float, float, float, float], ...]:
    """One deterministic 60-bar shape per (instrument, series); shared, never synthesized."""
    key = (instrument, kind)
    cached = _PATTERN_CACHE.get(key)
    if cached is not None:
        return cached
    rng = random.Random(f"{instrument}:{kind}")
    bars: list[tuple[float, float, float, float]] = []
    price = 1.0
    for _ in range(MINUTES_PER_HOUR):
        drift = rng.gauss(0, 0.0002)
        open_price = price
        close_price = open_price * math.exp(drift)
        high = max(open_price, close_price) * math.exp(abs(rng.gauss(0, 0.0001)))
        low = min(open_price, close_price) * math.exp(-abs(rng.gauss(0, 0.0001)))
        bars.append((open_price, high, low, close_price))
        price = close_price
    _PATTERN_CACHE[key] = tuple(bars)
    return _PATTERN_CACHE[key]


def hour_minute_bars(hour: int, kind: str, hour_return: float, *, level: float = 1.0,
                     instrument: str = "BTCUSDT") -> tuple[tuple[float, float, float, float], ...]:
    """Deterministic 60-minute bars whose total log return is exactly ``hour_return``.

    The per-minute innovation is centered on the hourly return, so the archived
    minute evidence and the recorded hourly residual describe the same hour.
    """
    phase = SHAPE_PHASE[kind] + (hour % 17) / 17.0
    shape = [0.0006 * math.sin(2 * math.pi * (index / 17.0 + phase)) for index in range(MINUTES_PER_HOUR)]
    mean_shape = sum(shape) / MINUTES_PER_HOUR
    bars: list[tuple[float, float, float, float]] = []
    price = level
    for index in range(MINUTES_PER_HOUR):
        ret = hour_return / MINUTES_PER_HOUR + (shape[index] - mean_shape)
        open_price = price
        close_price = open_price * math.exp(ret)
        excursion = 0.0002 * (1.0 + abs(math.sin(index / 5.0 + phase)))
        bars.append((open_price, max(open_price, close_price) * math.exp(excursion),
                     min(open_price, close_price) * math.exp(-excursion), close_price))
        price = close_price
    del instrument  # the instrument only selects the call site; shapes are shared
    return tuple(bars)


def _build_hourly(seed: int) -> SyntheticMarket:
    """Chronological synthetic series whose next hour depends only on the past.

    The realized standardized return is deliberately light-tailed so the frozen
    Huber IRLS converges quickly in tests; the Huber regime itself is covered by
    the dedicated model tests.
    """
    closes: dict[str, tuple[HourlyClose, ...]] = {}
    features: dict[str, HourlyFeatures] = {}
    returns_by_instrument: dict[str, tuple[float, ...]] = {}
    hours = TOTAL_DAYS * 24
    for offset, instrument in enumerate(INSTRUMENTS):
        rng = random.Random(seed + offset)
        series: list[HourlyClose] = [HourlyClose(hour_ns(0), Decimal(str(BASE_PRICE[instrument])), hour_ns(0), f"{instrument}-0")]
        rets: list[float] = [0.0]
        zs: list[float] = [0.0]
        sigmas: list[float] = [0.0035]
        for hour in range(1, hours):
            previous = series[-1]
            # Saturate the momentum feedback so the synthetic process stays stable.
            # Fixed return scale avoids a self-referential volatility feedback loop;
            # sigma/z are still computed by the frozen finite-window estimator below.
            realized = BASE_HOURLY_VOL * (0.3 * math.tanh(zs[-1] / 4.0) + 0.25 * rng.gauss(0, 1)) if hour >= 25 else 0.0
            rets.append(realized)
            price = float(previous.close) * math.exp(realized)
            series.append(HourlyClose(hour_ns(hour), Decimal(str(round(price, 8))), hour_ns(hour), f"{instrument}-{hour}"))
            if hour >= 720:
                window = rets[hour - 719 : hour + 1]
                sigma = math.sqrt(max(finite_window_variance(window), 1e-8))
                z = math.log(price / float(series[hour - 24].close)) / (math.sqrt(24) * sigma)
            else:
                sigma, z = 0.0035, 0.0
            zs.append(z)
            sigmas.append(sigma)
        # next_return[t] is the realized return of hour t+1 standardized at t.
        next_returns = [rets[hour + 1] if hour + 1 < hours else 0.0 for hour in range(hours)]
        closes[instrument] = tuple(series)
        features[instrument] = HourlyFeatures(tuple(zs), tuple(sigmas), tuple(next_returns))
        returns_by_instrument[instrument] = tuple(rets)
    return SyntheticMarket(closes, features, returns_by_instrument)


def _hour_calendar_identity(at_ns: int) -> str:
    return f"cal-{at_ns // DAY_NS}"


def residual_hour(market: SyntheticMarket, hour: int, forecast: dict[str, float]) -> JointResidualHour:
    at_ns = hour_ns(hour)
    btc = market.features["BTCUSDT"]
    eth = market.features["ETHUSDT"]
    btc_return = btc.next_return[hour]
    eth_return = eth.next_return[hour]
    observations = (
        {"instrument": "BTCUSDT", "spread_bp": "1.2", "taker_fee_bp": "5.5", "depth_notional": "250000"},
        {"instrument": "ETHUSDT", "spread_bp": "1.6", "taker_fee_bp": "5.5", "depth_notional": "120000"},
    )
    latency = (
        {"instrument": "BTCUSDT", "entry_latency_ms": "120", "fill_ratio": "1.0"},
        {"instrument": "ETHUSDT", "entry_latency_ms": "140", "fill_ratio": "1.0"},
    )
    settles = hour % 8 == 0
    funding = tuple({"instrument": instrument, "rate": "0.00001", "mark": "100"}
                    for instrument in INSTRUMENTS) if settles else ()
    return JointResidualHour(
        at_ns=at_ns,
        btc_residual=btc_return / btc.sigma[hour] - forecast["BTCUSDT"],
        eth_residual=eth_return / eth.sigma[hour] - forecast["ETHUSDT"],
        btc_forecast=forecast["BTCUSDT"], eth_forecast=forecast["ETHUSDT"],
        btc_sigma=btc.sigma[hour], eth_sigma=eth.sigma[hour],
        btc_z=btc.z[hour], eth_z=eth.z[hour],
        # Shared, deterministic minute shapes: the bridge re-anchors them to the
        # current causal snapshot, so absolute archive level is irrelevant here.
        btc_last_ohlc=hour_minute_bars(hour, "last", btc_return, instrument="BTCUSDT"),
        eth_last_ohlc=hour_minute_bars(hour, "last", eth_return, instrument="ETHUSDT"),
        btc_mark_ohlc=minute_pattern("BTCUSDT", "mark"),
        eth_mark_ohlc=minute_pattern("ETHUSDT", "mark"),
        btc_index_ohlc=minute_pattern("BTCUSDT", "index"),
        eth_index_ohlc=minute_pattern("ETHUSDT", "index"),
        source_class="SYNTHETIC_FIXTURE",
        funding_publication_at_ns=at_ns - 3_600_000_000_000 if settles else None,
        funding_settlement_at_ns=at_ns if settles else None,
        execution_missing=False,
        btc_feature_ref=f"btc-feature-{hour}", eth_feature_ref=f"eth-feature-{hour}",
        opening_gaps=(0.0,), excursions=(0.0001,),
        spread_depth_observations=observations,
        latency_fill_observations=latency,
        funding_observations=funding,
        availability_class="RECONSTRUCTED_MARKET", replay_mode="MINUTE_REPLAY",
        evidence_hashes=(f"hash-{hour}",),
        calendar_identity=_hour_calendar_identity(at_ns), universe_identity="BTCUSDT_ETHUSDT_V1",
        minute_replay_complete=True,
    )


@dataclass(frozen=True)
class Phase4Fixture:
    market: SyntheticMarket
    oof: OOFArchive
    hours: tuple[JointResidualHour, ...]
    start_hour: int
    selected_block: int
    energy_scores: dict[int, float]
    training_sigma: dict[str, float]
    block_selection: BlockSelectionResult


_FIXTURE_CACHE: dict[int, Phase4Fixture] = {}


def build_fixture(seed: int = 11) -> Phase4Fixture:
    cached = _FIXTURE_CACHE.get(seed)
    if cached is not None:
        return cached
    from atlas.science.huber_mean import weekly_refit
    from atlas.science.residual_blocks import select_block_length_frozen

    market = _build_hourly(seed)
    rows = market.observations("BTCUSDT") + market.observations("ETHUSDT")
    archive = OOFArchive()
    start_hour = FIRST_REFIT_DAY * 24
    evaluation_hour = EVALUATION_DAY * 24
    forecast: dict[str, float] = {"BTCUSDT": 0.0, "ETHUSDT": 0.0}
    current_model = None
    hours: list[JointResidualHour] = []
    for hour in range(start_hour, evaluation_hour + 24):
        if hour % (7 * 24) == 0:
            # Frozen cadence (Monday refit) over the retained 90+ day training window.
            retained = [row for row in rows if row.label_at_ns > hour_ns(hour) - RETENTION_DAYS * DAY_NS]
            current_model = weekly_refit(retained, hour_ns(hour))
        assert current_model is not None
        model = current_model.model
        forecast = {instrument: model.forecast(instrument, market.features[instrument].z[hour])
                    for instrument in INSTRUMENTS}
        hours.append(residual_hour(market, hour, forecast))
        for instrument in INSTRUMENTS:
            observation = MeanObservation(hour_ns(hour), instrument, market.features[instrument].z[hour],
                                          market.features[instrument].sigma[hour],
                                          market.features[instrument].next_return[hour])
            archive.forecast(observation, model)
            archive.mature(instrument, hour_ns(hour), market.features[instrument].next_return[hour],
                           hour_ns(hour + 1))
    archived = tuple(hours)
    training_sigma = {
        "BTCUSDT": math.sqrt(sum(h.btc_sigma**2 for h in archived) / len(archived)),
        "ETHUSDT": math.sqrt(sum(h.eth_sigma**2 for h in archived) / len(archived)),
    }
    # Production orchestration: the frozen three 7-day validation windows ending at
    # the last Monday refit, with non-overlapping candidate scenarios.
    selection: BlockSelectionResult = select_block_length_frozen(archived, fit_at_ns=day_ns(BLOCK_FIT_DAY),
                                           training_btc_sigma=training_sigma["BTCUSDT"],
                                           training_eth_sigma=training_sigma["ETHUSDT"])
    fixture = Phase4Fixture(market, archive, archived, start_hour, selection.selected_block,
                            selection.energy_scores, training_sigma, selection)
    _FIXTURE_CACHE[seed] = fixture
    return fixture
