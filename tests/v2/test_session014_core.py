"""Hand-calculated and prefix checks for Session-014 pure feature families."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.raw import RawObservationV2
from atlas.v2.features.candles import CausalTrade, candle_geometry, utc_day_vwap
from atlas.v2.features.regime import classify_regime
from atlas.v2.features.structure import (
    confirmed_legs,
    confirmed_swings,
    fibonacci,
    morphology,
    structure_events,
    support_resistance,
)
from atlas.v2.features.technical import adx, atr, ema, rsi, technical_series
from atlas.v2.instruments import EnvironmentV2, InstrumentKeyV2, ProductTypeV2, VenueV2
from atlas.v2.math.core import (
    KalmanState,
    common_paths,
    empirical_residuals,
    ewma_variance,
    har_forecast,
    kalman_update,
    realized_variance,
    robust_slope,
)

NS = 1_000_000_000
KEY = InstrumentKeyV2(VenueV2.BYBIT, EnvironmentV2.TESTNET, ProductTypeV2.LINEAR_PERPETUAL,
                      "BTCUSDT", "bitcoin", "USDT", "USDT", "rev-014")


def bar(index: int, *, interval: BarIntervalV2 = BarIntervalV2.M15, close: str = "100",
        high: str | None = None, low: str | None = None, volume: str = "10",
        key: InstrumentKeyV2 = KEY) -> CausalBarV2:
    close_ns = (index + 1) * interval.duration_ns
    price = Decimal(close)
    high_value = Decimal(high) if high is not None else price + Decimal("0.3")
    low_value = Decimal(low) if low is not None else price - Decimal("0.3")
    raw = RawObservationV2.build(instrument_revision=key.content_hash, source_id="fixture-public",
        event_type=f"BAR_{interval.value}", received_at_ns=close_ns, ingested_at_ns=close_ns,
        available_at_ns=close_ns, event_at_ns=close_ns, translation_version="fixture-v1",
        payload={"i": index, "close": close, "high": str(high_value), "low": str(low_value)})
    return CausalBarV2(raw, interval, index * interval.duration_ns, close_ns,
                       price, high_value, low_value, price, Decimal(volume), True)


def test_hand_calculated_math() -> None:
    assert ema([1, 2, 3, 4], 3) == pytest.approx((None, None, 2, 3))
    bars = tuple(bar(i, high="101", low="99") for i in range(5))
    assert atr(bars, 3)[-1] == pytest.approx(2)
    assert rsi([1, 2, 3, 4], 2)[-1] == pytest.approx(100)
    assert realized_variance((1, 2, 3)) == pytest.approx(14)
    assert ewma_variance((1, 2), decay=0.5) == pytest.approx(2.5)
    assert robust_slope((1, 3, 5, 7), lookback=4) == pytest.approx(2)
    state = kalman_update(KalmanState(0, 1, 0), 2, process_variance=0, observation_variance=1)
    assert (state.estimate, state.variance, state.observation_count) == pytest.approx((1, .5, 1))
    assert empirical_residuals((("b", 2, 2.0), ("a", 1, -1.0)), cutoff_ns=2, minimum=2).values == (-1, 2)
    assert empirical_residuals((("a", 1, 1.0),), cutoff_ns=1).status == "NOT_ESTIMABLE"
    assert common_paths(seed=7, experiment_ref="e", decision_ref="d", count=4) == common_paths(seed=7, experiment_ref="e", decision_ref="d", count=4)
    assert har_forecast(tuple(float(i) for i in range(10)), origin=10) is None  # singular design fails closed
    history = [1.0, 3.0, 2.0]
    for _ in range(12):
        history.append(1 + .2 * history[-1] + .3 * sum(history[-2:]) / 2 + .4 * sum(history[-3:]) / 3)
    expected = 1 + .2 * history[-1] + .3 * sum(history[-2:]) / 2 + .4 * sum(history[-3:]) / 3
    assert har_forecast(tuple(history), origin=len(history)) == pytest.approx(expected, abs=1e-8)
    assert har_forecast(tuple(history) + (999.0,), origin=len(history)) == pytest.approx(expected, abs=1e-8)
    assert empirical_residuals((("a", 1, 1.0), ("future", 10, 9.0)), cutoff_ns=1).values == (1.0,)


def test_prefix_future_tail_all_standard_features() -> None:
    bars = tuple(bar(i, close=str(100 + i * 0.1)) for i in range(60))
    tail = tuple(bar(i, close=str(300 + i)) for i in range(60, 75))
    assert technical_series(bars)[-1] == technical_series(bars + tail)[59]
    assert atr(bars)[-1] == atr(bars + tail)[59]
    assert rsi([float(x.close) for x in bars])[-1] == rsi([float(x.close) for x in bars + tail])[59]
    assert adx(bars)[-1] == adx(bars + tail)[59]
    assert candle_geometry(bars[-1], atr=0.6, previous=bars[-2], volume_history=bars[-21:-1]) == candle_geometry(bars[-1], atr=0.6, previous=bars[-2], volume_history=(bars + tail)[-36:-15])
    assert robust_slope(tuple(float(x.close) for x in bars)) == robust_slope(tuple(float(x.close) for x in bars + tail)[:60])
    first = KalmanState(0, 1, 0)
    prefix_state = first
    for item in bars:
        prefix_state = kalman_update(prefix_state, float(item.close), process_variance=.1, observation_variance=1)
    full_state = first
    at_prefix = first
    for i, item in enumerate(bars + tail):
        full_state = kalman_update(full_state, float(item.close), process_variance=.1, observation_variance=1)
        if i == len(bars) - 1:
            at_prefix = full_state
    assert prefix_state == at_prefix


def test_vwap_and_regime_missingness() -> None:
    trades = (CausalTrade(KEY, Decimal("10"), Decimal("2"), 1, 1, "a"), CausalTrade(KEY, Decimal("20"), Decimal("1"), 2, 2, "b"),
              CausalTrade(KEY, Decimal("100"), Decimal("1"), 3, 30, "future"))
    assert utc_day_vwap(trades, key=KEY, cutoff_ns=2) == (Decimal(40) / 3, ("a", "b"))
    assert utc_day_vwap(trades, key=KEY, cutoff_ns=2) == utc_day_vwap(trades + (CausalTrade(KEY, Decimal(1), Decimal(1), 4, 4, "tail"),), key=KEY, cutoff_ns=2)
    state = classify_regime(ema20=2, ema50=1, close=3, realized_variance=.2, spread_bps=None,
                            funding=None, event_gate=None, feature_ref="feature")
    assert state.trend_state.state == "UP"
    assert state.liquidity_state.state == state.crowding_state.state == state.event_state.state == "UNKNOWN"
    assert state.unknown_or_ood_state.state == "UNKNOWN"


def test_smc_confirmation_and_prefix() -> None:
    highs = [101, 102, 105, 103, 102, 104, 106, 103, 102, 108]
    lows = [99, 98, 97, 98, 99, 98, 97, 98, 99, 100]
    bars = tuple(bar(i, close="100", high=str(highs[i]), low=str(lows[i])) for i in range(len(highs)))
    assert not any(s.pivot_ref == bars[2].content_hash for s in confirmed_swings(bars[:4]))
    confirmed = [s for s in confirmed_swings(bars[:5]) if s.pivot_ref == bars[2].content_hash and s.kind == "HIGH"]
    assert len(confirmed) == 1
    assert confirmed[0].pivot_at_ns == bars[2].close_at_ns
    assert confirmed[0].confirmed_at_ns == bars[4].close_at_ns
    assert confirmed_swings(bars[:7]) == tuple(s for s in confirmed_swings(bars) if s.confirmed_at_ns <= bars[6].close_at_ns)
    atr_values = (1.0,) * len(bars)
    assert structure_events(bars[:7], atr_values[:7]) == tuple(e for e in structure_events(bars, atr_values) if e.confirmed_at_ns <= bars[6].close_at_ns)
    assert support_resistance(bars[:7], atr_values[:7]) == tuple(z for z in support_resistance(bars, atr_values) if z.updated_at_ns <= bars[6].close_at_ns)
    legs = confirmed_legs(confirmed_swings(bars))
    if legs:
        assert set(fibonacci(legs[-1])) == {"0.382", "0.5", "0.618"}
        assert morphology(legs[:1]) == morphology(legs[:1] + legs[1:]) if len(legs) == 1 else morphology(legs[:1]) != morphology(legs)
        prefix_legs = confirmed_legs(confirmed_swings(bars[:7]))
        assert prefix_legs == tuple(leg for leg in legs if leg.confirmed_at_ns <= bars[6].close_at_ns)
        if prefix_legs:
            assert fibonacci(prefix_legs[-1]) == fibonacci(legs[len(prefix_legs) - 1])
            assert morphology(prefix_legs) == morphology(legs[:len(prefix_legs)])


def test_bos_choch_fvg_sweep_orderblock_timing() -> None:
    rows = [
        ("100", "101", "99"), ("100", "102", "98"), ("103", "105", "102"),
        ("100", "103", "98"), ("100", "102", "99"), ("106", "106.3", "105.7"),
        ("100", "101", "96"), ("100", "102", "98"), ("100", "101", "99"),
        ("95", "96", "94"), ("100", "107", "99"),
    ]
    bars = [bar(i, close=close, high=high, low=low) for i, (close, high, low) in enumerate(rows)]
    bars[4] = replace(bars[4], open=Decimal("101"))
    bars_tuple = tuple(bars)
    assert not any(e.kind == "FVG_BULL" and e.bar_ref == bars[2].content_hash for e in structure_events(bars_tuple[:2], (1.0,) * 2))
    at_third = structure_events(bars_tuple[:3], (1.0,) * 3)
    assert any(e.kind == "FVG_BULL" and e.bar_ref == bars[2].content_hash for e in at_third)
    assert not any(e.kind.startswith("BOS") for e in structure_events(bars_tuple[:5], (1.0,) * 5))
    at_bos = structure_events(bars_tuple[:6], (1.0,) * 6)
    bos = [e for e in at_bos if e.kind == "BOS_UP" and e.bar_ref == bars[5].content_hash]
    assert len(bos) == 1 and bos[0].swing_ref == bars[2].content_hash
    block = [e for e in at_bos if e.kind == "ORDER_BLOCK_CANDIDATE_UP"]
    assert len(block) == 1 and block[0].related_ref == bars[4].content_hash
    assert not any(e.kind.startswith("ORDER_BLOCK") for e in structure_events(bars_tuple[:5], (1.0,) * 5))
    assert not any(e.kind == "CHOCH_DOWN" for e in structure_events(bars_tuple[:9], (1.0,) * 9))
    full = structure_events(bars_tuple, (1.0,) * len(bars_tuple))
    assert any(e.kind == "CHOCH_DOWN" and e.bar_ref == bars[9].content_hash for e in full)
    assert any(e.kind == "SWEEP_UP" and e.bar_ref == bars[10].content_hash for e in full)
    assert at_bos == tuple(e for e in full if e.confirmed_at_ns <= bars[5].close_at_ns)


@settings(max_examples=8, deadline=None)
@given(st.lists(st.floats(min_value=90, max_value=110, allow_nan=False, allow_infinity=False), min_size=1, max_size=4))
def test_arbitrary_future_tail_does_not_relabel_prefix(tail_closes: list[float]) -> None:
    prefix = tuple(bar(i, close=str(100 + (i % 5) * .2)) for i in range(55))
    tail = tuple(bar(55 + i, close=str(value)) for i, value in enumerate(tail_closes))
    all_bars = prefix + tail
    assert technical_series(prefix)[-1] == technical_series(all_bars)[54]
    swings = confirmed_swings(prefix)
    assert swings == tuple(x for x in confirmed_swings(all_bars) if x.confirmed_at_ns <= prefix[-1].close_at_ns)
    atr_prefix = atr(prefix)
    events = structure_events(prefix, atr_prefix)
    all_events = structure_events(all_bars, atr(all_bars))
    assert events == tuple(x for x in all_events if x.confirmed_at_ns <= prefix[-1].close_at_ns)
    zones = support_resistance(prefix, atr_prefix)
    assert zones == tuple(x for x in support_resistance(all_bars, atr(all_bars)) if x.updated_at_ns <= prefix[-1].close_at_ns)
    legs = confirmed_legs(swings)
    all_legs = confirmed_legs(confirmed_swings(all_bars))
    assert legs == all_legs[:len(legs)]
    assert morphology(legs) == morphology(all_legs[:len(legs)])
