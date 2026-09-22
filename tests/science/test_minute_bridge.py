"""§6: synchronized last/mark/index minute bridge, anchored and never clipped."""

from __future__ import annotations

import math

import pytest

from atlas.science.huber_mean import HOUR_NS
from atlas.science.residual_blocks import JointResidualHour
from atlas.science.scenarios import (
    BridgeSupport,
    JointMinutePaths,
    MinuteOHLC,
    ScenarioNotEstimable,
    SynchronizedPrices,
    bridge_hour,
    joint_minute_paths,
    reconstruct_minute_series,
)

BASE = 1_700_000_000_000_000_000


def series(seed: float = 0.0, *, scale: float = 1.0) -> tuple[tuple[float, float, float, float], ...]:
    bars = []
    price = scale
    for index in range(60):
        drift = 0.0002 * math.sin(index / 5.0 + seed)
        open_price = price
        close = open_price * math.exp(drift)
        bars.append((open_price, max(open_price, close) * math.exp(0.0001), min(open_price, close) * math.exp(-0.0001), close))
        price = close
    return tuple(bars)


def hour(**overrides: object) -> JointResidualHour:
    kwargs: dict[str, object] = {
        "at_ns": BASE, "btc_residual": 0.0, "eth_residual": 0.0, "btc_forecast": 0.0, "eth_forecast": 0.0,
        "btc_sigma": 0.01, "eth_sigma": 0.01, "btc_z": 0.1, "eth_z": 0.1,
        "btc_last_ohlc": series(0.0), "eth_last_ohlc": series(0.5),
        "btc_mark_ohlc": series(0.1, scale=1.00005), "eth_mark_ohlc": series(0.6, scale=1.00005),
        "btc_index_ohlc": series(0.2, scale=0.99995), "eth_index_ohlc": series(0.7, scale=0.99995),
        "execution_missing": False, "minute_replay_complete": True,
    }
    kwargs.update(overrides)
    return JointResidualHour(**kwargs)  # type: ignore[arg-type]


def test_bridge_preserves_ohlc_and_reproduces_the_frozen_hour_return():
    value = hour(btc_forecast=0.2, btc_residual=0.3)
    minutes = bridge_hour(value, "BTCUSDT", previous=SynchronizedPrices(100.0, 100.02, 99.99),
                          archive_sigma=0.01, support=BridgeSupport())
    assert len(minutes) == 60
    for minute in minutes:
        for bar in (minute.last, minute.mark, minute.index):
            assert bar.high >= max(bar.open, bar.close)
            assert bar.low <= min(bar.open, bar.close)
    realized = math.log(minutes[-1].last.close / 100.0)
    assert realized == pytest.approx(0.01 * (0.2 + 0.3), rel=1e-9)
    assert minutes[0].at_ns == BASE
    assert minutes[-1].at_ns == BASE + 59 * 60_000_000_000
    assert abs(minutes[-1].mark_basis) <= 0.02 and abs(minutes[-1].index_basis) <= 0.02


def test_bridge_anchors_to_the_current_causal_snapshot():
    value = hour()
    minutes = bridge_hour(value, "BTCUSDT", previous=SynchronizedPrices(500.0, 500.01, 499.99),
                          archive_sigma=0.01, support=BridgeSupport())
    assert minutes[0].last.open == pytest.approx(500.0)
    assert minutes[0].mark.open == pytest.approx(500.01)
    assert minutes[0].index.open == pytest.approx(499.99)


def test_missing_or_incoherent_evidence_is_not_estimable_and_never_clipped():
    with pytest.raises(ScenarioNotEstimable, match="insufficient mark/index archive support"):
        bridge_hour(hour(btc_mark_ohlc=(), minute_replay_complete=False), "BTCUSDT",
                    previous=SynchronizedPrices(100.0, 100.0, 100.0),
                    archive_sigma=0.01, support=BridgeSupport())
    with pytest.raises(ScenarioNotEstimable, match="unsupported current volatility"):
        bridge_hour(hour(btc_sigma=0.5), "BTCUSDT", previous=SynchronizedPrices(100.0, 100.0, 100.0),
                    archive_sigma=0.01, support=BridgeSupport())
    with pytest.raises(ScenarioNotEstimable, match="poor barrier calibration"):
        bridge_hour(hour(), "BTCUSDT", previous=SynchronizedPrices(100.0, 100.0, 100.0), archive_sigma=0.01,
                    support=BridgeSupport(barrier_calibrated=False))
    with pytest.raises(ScenarioNotEstimable, match="excess basis drift"):
        bridge_hour(hour(), "BTCUSDT", previous=SynchronizedPrices(100.0, 105.0, 100.0), archive_sigma=0.01,
                    support=BridgeSupport())


def test_broken_reconstruction_is_not_estimable_rather_than_clipped():
    flat = tuple((1.0, 1.0, 1.0, 1.0) for _ in range(60))
    archive = tuple(MinuteOHLC(*bar) for bar in flat)
    rebuilt, _, _ = reconstruct_minute_series(1.0, archive, target_return=0.01, excursion_scale=1.0)
    assert math.log(rebuilt[-1].close / 1.0) == pytest.approx(0.01, rel=1e-9)
    assert rebuilt[-1].high >= max(rebuilt[-1].open, rebuilt[-1].close)
    with pytest.raises(ValueError, match="OHLC inequalities violated"):
        MinuteOHLC(1.0, 0.5, 0.9, 1.0)
    with pytest.raises(ScenarioNotEstimable, match="incomplete minute archive"):
        reconstruct_minute_series(1.0, archive[:10], target_return=0.01, excursion_scale=1.0)


def test_joint_paths_keep_mark_index_paired_with_the_same_sampled_block():
    block = (hour(), hour(at_ns=BASE + HOUR_NS))
    paths = joint_minute_paths(block, initial={"BTCUSDT": SynchronizedPrices(100.0, 100.0, 100.0),
                                               "ETHUSDT": SynchronizedPrices(50.0, 50.0, 50.0)},
                               archive_sigma={"BTCUSDT": 0.01, "ETHUSDT": 0.01})
    assert isinstance(paths, JointMinutePaths)
    assert len(paths.btc) == len(paths.eth) == 120
    assert all(a.at_ns == b.at_ns for a, b in zip(paths.btc, paths.eth, strict=True))
    assert paths.btc[0].last.open == pytest.approx(100.0)
    assert paths.eth[0].last.open == pytest.approx(50.0)
    with pytest.raises(ScenarioNotEstimable, match="matching causal anchors"):
        joint_minute_paths(block, initial={"BTCUSDT": SynchronizedPrices(100.0, 100.0, 100.0)}, archive_sigma={})
