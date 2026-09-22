"""§5: the synchronized joint residual archive must be replay-sufficient."""

from __future__ import annotations

import pytest

from atlas.science.huber_mean import HOUR_NS
from atlas.science.residual_blocks import JointResidualHour, eligible_starts, sample_blocks

BASE = 1_700_000_000_000_000_000


def bars(close: float = 1.0) -> tuple[tuple[float, float, float, float], ...]:
    return tuple((close, close * 1.001, close * 0.999, close) for _ in range(60))


def hour(index: int, **overrides: object) -> JointResidualHour:
    kwargs: dict[str, object] = {
        "at_ns": BASE + index * HOUR_NS,
        "btc_residual": 0.1, "eth_residual": -0.1, "btc_forecast": 0.05, "eth_forecast": -0.02,
        "btc_sigma": 0.01, "eth_sigma": 0.02, "btc_z": 0.7, "eth_z": -0.3,
        "btc_last_ohlc": bars(), "eth_last_ohlc": bars(),
        "btc_mark_ohlc": bars(), "eth_mark_ohlc": bars(),
        "btc_index_ohlc": bars(), "eth_index_ohlc": bars(),
        "execution_missing": False, "minute_replay_complete": True,
        "calendar_identity": "cal-1", "universe_identity": "BTCUSDT_ETHUSDT_V1",
        "replay_mode": "MINUTE_REPLAY", "availability_class": "RECONSTRUCTED_MARKET",
        "evidence_hashes": ("h1",), "funding_publication_at_ns": BASE, "funding_settlement_at_ns": BASE + 1,
        "spread_depth_observations": ({"spread_bp": "1.0"},),
        "latency_fill_observations": ({"fill_ratio": "1.0"},),
        "funding_observations": ({"rate": "0.0001"},),
        "opening_gaps": (0.0,), "excursions": (0.0001,),
        "btc_feature_ref": "btc-f", "eth_feature_ref": "eth-f",
    }
    kwargs.update(overrides)
    return JointResidualHour(**kwargs)  # type: ignore[arg-type]


def test_archive_binds_every_replay_field_per_synchronized_hour():
    value = hour(0)
    assert value.btc_forecast == 0.05 and value.eth_residual == -0.1
    assert value.calendar_identity == "cal-1" and value.universe_identity == "BTCUSDT_ETHUSDT_V1"
    assert value.availability_class == "RECONSTRUCTED_MARKET" and value.replay_mode == "MINUTE_REPLAY"
    assert value.evidence_hashes == ("h1",)
    assert value.spread_depth_observations and value.latency_fill_observations and value.funding_observations
    assert value.funding_publication_at_ns is not None and value.funding_settlement_at_ns is not None
    assert value.execution_missing is False and value.minute_replay_complete is True
    for series in (value.btc_last_ohlc, value.eth_last_ohlc, value.btc_mark_ohlc, value.eth_mark_ohlc,
                   value.btc_index_ohlc, value.eth_index_ohlc):
        assert len(series) == 60
        for open_price, high, low, close in series:
            assert high >= max(open_price, close) and low <= min(open_price, close)


def test_complete_minute_replay_requires_exactly_sixty_bars():
    with pytest.raises(ValueError, match="exactly 60"):
        hour(0, btc_mark_ohlc=bars()[:59])
    with pytest.raises(ValueError, match="absent or exactly 60"):
        hour(0, minute_replay_complete=False, btc_index_ohlc=bars()[:10])
    absent = hour(0, minute_replay_complete=False, btc_mark_ohlc=(), eth_mark_ohlc=(), btc_index_ohlc=(),
                  eth_index_ohlc=())
    assert absent.minute_replay_complete is False
    assert absent.btc_mark_ohlc == ()


def test_missing_microstructure_is_never_synthesized():
    value = hour(0, btc_mark_ohlc=(), eth_mark_ohlc=(), btc_index_ohlc=(), eth_index_ohlc=(),
                 execution_missing=True, minute_replay_complete=False, spread_depth_observations=(),
                 latency_fill_observations=())
    assert value.execution_missing is True
    assert value.spread_depth_observations == () and value.latency_fill_observations == ()


def test_non_finite_or_nonpositive_statistics_are_rejected():
    for overrides in ({"btc_residual": float("nan")}, {"eth_forecast": float("inf")}, {"btc_sigma": 0.0},
                      {"eth_sigma": -0.01}):
        with pytest.raises(ValueError, match="finite residual/forecast/features"):
            hour(0, **overrides)


def test_eligible_starts_require_contiguous_complete_hours():
    hours = tuple(hour(i) for i in range(80))
    assert eligible_starts(hours, 72) == tuple(range(9))
    assert eligible_starts(hours, 24) == tuple(range(57))
    broken = list(hours)
    broken[10] = hour(10, complete=False)
    assert all(not (start <= 10 < start + 24) for start in eligible_starts(tuple(broken), 24))
    gapped = list(hours)
    gapped[5] = hour(5, at_ns=BASE + 9 * HOUR_NS)
    # No 24-hour window may straddle the missing hour; windows from index 6 on are intact.
    assert min(eligible_starts(tuple(gapped), 24)) == 6
    with pytest.raises(ValueError, match="unfrozen block length"):
        eligible_starts(hours, 25)


def test_block_sampling_is_deterministic_and_bounded():
    hours = tuple(hour(i) for i in range(120))
    first = sample_blocks(hours, length=24, horizon_hours=25, paths=3, seed=5)
    second = sample_blocks(hours, length=24, horizon_hours=25, paths=3, seed=5)
    assert first == second and len(first) == 3
    assert all(len(path) == 25 for path in first)
    with pytest.raises(ValueError, match="NOT_ESTIMABLE"):
        sample_blocks((), length=24, horizon_hours=24, paths=1, seed=0)
