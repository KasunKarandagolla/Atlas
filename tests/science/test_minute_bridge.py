"""§2: the frozen CURRENT-STATE minute bridge (never a replay of the archived return)."""

from __future__ import annotations

import math
from dataclasses import replace

import pytest

from atlas.science.huber_mean import HOUR_NS
from atlas.science.residual_blocks import JointResidualHour
from atlas.science.scenarios import (
    BridgeSupport,
    CurrentModelState,
    JointMinutePaths,
    MinuteOHLC,
    ScenarioNotEstimable,
    SynchronizedPrices,
    bridge_hour,
    bridge_minute,
    bridge_return,
    joint_minute_paths,
)

BASE = 1_700_000_000_000_000_000
MINUTES_PER_HOUR = 60


def minute_series(hour_return: float, *, phase: float = 0.0) -> tuple[tuple[float, float, float, float], ...]:
    shape = [0.0006 * math.sin(2 * math.pi * (index / 17.0 + phase)) for index in range(MINUTES_PER_HOUR)]
    mean = sum(shape) / MINUTES_PER_HOUR
    bars = []
    price = 1.0
    for index in range(MINUTES_PER_HOUR):
        ret = hour_return / MINUTES_PER_HOUR + (shape[index] - mean)
        open_price = price
        close = open_price * math.exp(ret)
        excursion = 0.0001 * (1 + abs(math.sin(index / 5.0 + phase)))
        bars.append((open_price, max(open_price, close) * math.exp(excursion),
                     min(open_price, close) * math.exp(-excursion), close))
        price = close
    return tuple(bars)


def hour(*, sigma: float = 0.01, mu: float = 0.2, residual: float = 0.3, phase: float = 0.0,
         mark_scale: float = 1.0001, index_scale: float = 0.99995, **overrides: object) -> JointResidualHour:
    hour_return = sigma * (mu + residual)
    kwargs: dict[str, object] = {
        "at_ns": BASE, "btc_residual": residual, "eth_residual": residual, "btc_forecast": mu, "eth_forecast": mu,
        "btc_sigma": sigma, "eth_sigma": sigma, "btc_z": 0.1, "eth_z": 0.1,
        "btc_last_ohlc": minute_series(hour_return, phase=phase),
        "eth_last_ohlc": minute_series(hour_return, phase=phase + 0.3),
        "btc_mark_ohlc": minute_series(hour_return, phase=phase + 0.1),
        "eth_mark_ohlc": minute_series(hour_return, phase=phase + 0.4),
        "btc_index_ohlc": minute_series(hour_return, phase=phase + 0.2),
        "eth_index_ohlc": minute_series(hour_return, phase=phase + 0.5),
        "execution_missing": False, "minute_replay_complete": True,
    }
    kwargs.update(overrides)
    return JointResidualHour(**kwargs)  # type: ignore[arg-type]


def anchor(price: float = 100.0) -> SynchronizedPrices:
    return SynchronizedPrices(price, price, price)


def test_current_mu_changes_the_simulated_path_for_the_same_archive_block():
    value = hour()
    low = bridge_hour(value, "BTCUSDT", previous=anchor(), current=CurrentModelState(0.01, 0.0))
    high = bridge_hour(value, "BTCUSDT", previous=anchor(), current=CurrentModelState(0.01, 0.8))
    assert [minute.last.close for minute in low] != [minute.last.close for minute in high]
    assert math.log(high[-1].last.close / 100.0) > math.log(low[-1].last.close / 100.0)


def test_current_sigma_changes_the_simulated_path_for_the_same_archive_block():
    value = hour()
    small = bridge_hour(value, "BTCUSDT", previous=anchor(), current=CurrentModelState(0.008, 0.2))
    large = bridge_hour(value, "BTCUSDT", previous=anchor(), current=CurrentModelState(0.014, 0.2))
    assert [minute.last.close for minute in small] != [minute.last.close for minute in large]
    assert abs(math.log(large[-1].last.close / 100.0)) > abs(math.log(small[-1].last.close / 100.0))
    with pytest.raises(ScenarioNotEstimable, match="unsupported current volatility"):
        bridge_hour(value, "BTCUSDT", previous=anchor(), current=CurrentModelState(0.05, 0.2))


def test_archive_mu_residual_relationship_reconstructs_correctly():
    for sigma, mu, residual in ((0.01, 0.2, 0.3), (0.004, -0.1, 0.25), (0.02, 0.0, -0.4)):
        value = hour(sigma=sigma, mu=mu, residual=residual)
        minutes = bridge_hour(value, "BTCUSDT", previous=anchor(),
                              current=CurrentModelState(sigma, mu))
        # current state == archive state reproduces the archived hour return exactly.
        assert math.log(minutes[-1].last.close / 100.0) == pytest.approx(sigma * (mu + residual), rel=1e-12)
    # A residual that disagrees with the archived minute returns is incoherent evidence.
    with pytest.raises(ScenarioNotEstimable, match="incoherent archived hour evidence"):
        bridge_hour(replace(hour(), btc_residual=0.9), "BTCUSDT", previous=anchor(),
                    current=CurrentModelState(0.01, 0.2))


def test_bridge_matches_the_frozen_per_minute_formula():
    value = hour(sigma=0.01, mu=0.2, residual=0.3)
    current = CurrentModelState(0.012, 0.5)
    minutes = bridge_hour(value, "BTCUSDT", previous=anchor(), current=current)
    bars = value.btc_last_ohlc
    previous = 100.0
    expected: list[float] = []
    per_minute: list[float] = []
    archive_previous = bars[0][0]
    for bar in bars:
        predicted = bridge_minute(previous, MinuteOHLC(*bar), archive_previous, 0.01, 0.2, current.sigma, current.mu)
        expected.append(predicted.close)
        per_minute.append(bridge_return(bar[3], archive_previous, 0.01, 0.2, current.sigma, current.mu))
        previous = predicted.close
        archive_previous = bar[3]
    assert [minute.last.close for minute in minutes] == pytest.approx(expected, rel=1e-12)
    simulated = [math.log(minute.last.close / (100.0 if index == 0 else minutes[index - 1].last.close))
                 for index, minute in enumerate(minutes)]
    assert simulated == pytest.approx(per_minute, rel=1e-12)


def test_mark_index_remain_paired_with_last_and_keep_basis_bounded():
    value = hour()
    minutes = bridge_hour(value, "BTCUSDT", previous=anchor(), current=CurrentModelState(0.01, 0.2))
    for minute in minutes:
        for bar in (minute.last, minute.mark, minute.index):
            assert bar.high >= max(bar.open, bar.close)
            assert bar.low <= min(bar.open, bar.close)
        assert abs(minute.mark_basis) <= 0.02 and abs(minute.index_basis) <= 0.02
    assert minutes[-1].last.close == pytest.approx(minutes[-1].mark.close, rel=0.02)
    with pytest.raises(ScenarioNotEstimable, match="excess basis drift"):
        bridge_hour(value, "BTCUSDT", previous=SynchronizedPrices(100.0, 105.0, 100.0),
                    current=CurrentModelState(0.01, 0.2))


def test_missing_or_incoherent_evidence_is_not_estimable():
    with pytest.raises(ScenarioNotEstimable, match="insufficient mark/index archive support"):
        bridge_hour(hour(btc_mark_ohlc=(), minute_replay_complete=False), "BTCUSDT", previous=anchor(),
                    current=CurrentModelState(0.01, 0.2))
    with pytest.raises(ScenarioNotEstimable, match="poor barrier calibration"):
        bridge_hour(hour(), "BTCUSDT", previous=anchor(), current=CurrentModelState(0.01, 0.2),
                    support=BridgeSupport(barrier_calibrated=False))
    with pytest.raises(ValueError, match="OHLC inequalities violated"):
        MinuteOHLC(1.0, 0.5, 0.9, 1.0)
    with pytest.raises(ValueError, match="finite positive current sigma"):
        CurrentModelState(0.0, 0.1)


def test_joint_paths_stay_paired_with_the_same_sampled_block():
    block = (hour(), hour(at_ns=BASE + HOUR_NS, phase=0.4))
    paths = joint_minute_paths(block, initial={"BTCUSDT": anchor(100.0), "ETHUSDT": anchor(50.0)},
                               current={"BTCUSDT": CurrentModelState(0.01, 0.2),
                                        "ETHUSDT": CurrentModelState(0.01, -0.1)})
    assert isinstance(paths, JointMinutePaths)
    assert len(paths.btc) == len(paths.eth) == 120
    assert all(a.at_ns == b.at_ns for a, b in zip(paths.btc, paths.eth, strict=True))
    assert paths.btc[0].last.open == pytest.approx(100.0)
    assert paths.eth[0].last.open == pytest.approx(50.0)
    assert paths.btc[60].at_ns == BASE + HOUR_NS
    with pytest.raises(ScenarioNotEstimable, match="matching causal anchors and current state"):
        joint_minute_paths(block, initial={"BTCUSDT": anchor()}, current={})
