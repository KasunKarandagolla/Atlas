"""Focused R1 regressions for interval qualification and daily coverage."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.bars import BarIntervalV2
from atlas.v2.features.candles import CausalTrade, prior_utc_day_range, utc_day_vwap
from atlas.v2.features.joins import JoinedBars
from atlas.v2.features.pipeline import feature_snapshot

from .test_session014_core import KEY, bar

NS = 1_000_000_000
DAY_NS = 86_400 * NS


def _joined(*, h4=None, h1=None, m15=None) -> JoinedBars:
    frames = {
        BarIntervalV2.H4: tuple(bar(i, interval=BarIntervalV2.H4) for i in range(55)),
        BarIntervalV2.H1: tuple(bar(i, interval=BarIntervalV2.H1) for i in range(55)),
        BarIntervalV2.M15: tuple(bar(i) for i in range(55)),
    }
    if h4 is not None:
        frames[BarIntervalV2.H4] = h4
    if h1 is not None:
        frames[BarIntervalV2.H1] = h1
    if m15 is not None:
        frames[BarIntervalV2.M15] = m15
    cutoff = max(item.close_at_ns for group in frames.values() for item in group)
    return JoinedBars(
        KEY,
        cutoff,
        frames[BarIntervalV2.H4],
        frames[BarIntervalV2.H1],
        frames[BarIntervalV2.M15],
        "AVAILABLE",
        None,
        sha256_json({"health": cutoff}),
    )


@pytest.mark.parametrize(
    ("interval", "feature", "gap_at"),
    ((BarIntervalV2.H4, "h4.ema50", 20), (BarIntervalV2.H1, "h1.ema50", 49), (BarIntervalV2.M15, "m15.atr14", 20)),
)
def test_gap_makes_time_dependent_frame_feature_unavailable(interval, feature, gap_at) -> None:
    complete = tuple(bar(i, interval=interval) for i in range(56))
    gapped = complete[:gap_at] + complete[gap_at + 1 :]
    kwargs = {"h4": None, "h1": None, "m15": None}
    kwargs[{BarIntervalV2.H4: "h4", BarIntervalV2.H1: "h1", BarIntervalV2.M15: "m15"}[interval]] = gapped
    artifact = feature_snapshot(_joined(**kwargs))
    value = artifact.values[feature]
    assert value.value is None
    assert value.missing_reason == "INTERVAL_COVERAGE_GAP"


def test_h4_24_hour_gap_near_ema_warmup_is_unqualified() -> None:
    complete = tuple(bar(i, interval=BarIntervalV2.H4) for i in range(56))
    gapped = complete[:20] + complete[26:]
    artifact = feature_snapshot(_joined(h4=gapped))
    assert len(gapped) == 50
    assert artifact.values["h4.ema50"].value is None
    assert artifact.values["h4.ema50"].missing_reason == "INTERVAL_COVERAGE_GAP"


def test_filling_gap_restores_qualified_prefix_without_reseeding() -> None:
    complete = tuple(bar(i, interval=BarIntervalV2.H4) for i in range(56))
    gapped = complete[:20] + complete[21:]
    before = feature_snapshot(_joined(h4=gapped))
    after = feature_snapshot(_joined(h4=complete))
    assert before.values["h4.ema50"].value is None
    assert after.values["h4.ema50"].value is not None


def test_exact_prefix_gap_outside_feature_context_withholds_recursive_features() -> None:
    from atlas.v2.data.active_history import advance
    from atlas.v2.features.pipeline import feature_snapshot

    from .test_session037_active_history import indexed
    from .test_session037_feature_prefix import _case

    join, states, _frames = _case()
    h4_inputs = tuple(indexed(i, BarIntervalV2.H4) for i in range(310))
    gapped_inputs = h4_inputs[:120] + h4_inputs[121:]
    gapped_h4 = None
    for start in range(0, len(gapped_inputs), 128):
        gapped_h4 = advance(
            gapped_h4,
            gapped_inputs[start : start + 128],
            key=KEY,
            interval=BarIntervalV2.H4,
        )
    assert gapped_h4 is not None
    gapped_join = replace(join, h4=gapped_h4.bars[-100:])
    history_states = {**states, BarIntervalV2.H4: gapped_h4}

    artifact = feature_snapshot(gapped_join, exact_histories=history_states)
    for name in ("h4.ema20", "h4.ema50", "h4.atr14"):
        assert artifact.values[name].value is None
        assert artifact.values[name].missing_reason == "INTERVAL_COVERAGE_GAP"


def test_prior_day_range_requires_all_unique_m15_slots() -> None:
    complete = tuple(bar(i) for i in range(96, 192))
    cutoff = 2 * DAY_NS
    high, low, refs = prior_utc_day_range(complete, cutoff_ns=cutoff)
    assert high == max(item.high for item in complete)
    assert low == min(item.low for item in complete)
    assert len(refs) == 96

    incomplete = complete[:37] + complete[38:]
    high, low, _ = prior_utc_day_range(incomplete, cutoff_ns=cutoff)
    assert high is None and low is None


def test_feature_snapshot_labels_sparse_prior_day_as_incomplete() -> None:
    h4 = tuple(bar(i, interval=BarIntervalV2.H4) for i in range(12))
    h1 = tuple(bar(i, interval=BarIntervalV2.H1) for i in range(48))
    m15 = tuple(bar(i) for i in range(96, 191))
    artifact = feature_snapshot(_joined(h4=h4, h1=h1, m15=m15))
    value = artifact.values["location.prior_day_high"]
    assert value.value is None
    assert value.missing_reason == "PRIOR_DAY_INCOMPLETE_COVERAGE"


def test_utc_day_vwap_deduplicates_exact_native_trade_replay() -> None:
    trades = (
        CausalTrade(KEY, Decimal("10"), Decimal("1"), 100, 100, "source-a", "native-1"),
        CausalTrade(KEY, Decimal("10"), Decimal("1"), 100, 100, "source-replay", "native-1"),
        CausalTrade(KEY, Decimal("20"), Decimal("1"), 200, 200, "source-b", "native-2"),
    )
    value, refs = utc_day_vwap(trades, key=KEY, cutoff_ns=1_000)
    assert value == Decimal("15")
    assert refs == ("source-a", "source-replay", "source-b")


def test_utc_day_vwap_rejects_conflicting_native_trade_identity() -> None:
    trades = (
        CausalTrade(KEY, Decimal("10"), Decimal("1"), 100, 100, "source-a", "native-1"),
        CausalTrade(KEY, Decimal("11"), Decimal("1"), 100, 100, "source-replay", "native-1"),
    )
    with pytest.raises(ValueError, match="CONFLICTING_TRADE_IDENTITY"):
        utc_day_vwap(trades, key=KEY, cutoff_ns=1_000)


def test_utc_day_vwap_rejects_source_ref_rebound_to_another_native_trade() -> None:
    trades = (
        CausalTrade(KEY, Decimal("10"), Decimal("1"), 100, 100, "same-source", "native-1"),
        CausalTrade(KEY, Decimal("10"), Decimal("1"), 100, 100, "same-source", "native-2"),
    )
    with pytest.raises(ValueError, match="CONFLICTING_TRADE_IDENTITY"):
        utc_day_vwap(trades, key=KEY, cutoff_ns=1_000)


def test_feature_snapshot_labels_conflicting_native_trade_identity() -> None:
    joined = _joined()
    cutoff = joined.cutoff_ns
    trades = (
        CausalTrade(KEY, Decimal("10"), Decimal("1"), cutoff - 2, cutoff - 2, "source-a", "native-1"),
        CausalTrade(KEY, Decimal("11"), Decimal("1"), cutoff - 1, cutoff - 1, "source-b", "native-1"),
    )
    artifact = feature_snapshot(joined, trades=trades)
    partial = artifact.values["location.utc_day_trade_vwap_partial_to_cutoff"]
    assert partial.value is None
    assert partial.missing_reason == "TRADE_IDENTITY_CONFLICT"
    assert artifact.values["location.utc_day_trade_vwap"].value is None


def test_utc_day_vwap_keeps_future_duplicate_out_of_cutoff_identity() -> None:
    trades = (
        CausalTrade(KEY, Decimal("10"), Decimal("1"), 100, 100, "source-a", "native-1"),
        CausalTrade(KEY, Decimal("99"), Decimal("1"), 101, 1_001, "future", "native-1"),
    )
    assert utc_day_vwap(trades, key=KEY, cutoff_ns=1_000) == (Decimal("10"), ("source-a",))


def test_feature_artifact_labels_vwap_partial_and_withholds_unverified_full_day() -> None:
    from atlas.v2.features.candles import CausalTrade

    joined = _joined()
    cutoff = joined.cutoff_ns
    trade = CausalTrade(
        KEY, Decimal("10"), Decimal("2"), cutoff - 1, cutoff - 1, sha256_json({"trade": "one"}), "native-one"
    )
    artifact = feature_snapshot(joined, trades=(trade,))
    assert artifact.values["location.utc_day_trade_vwap_partial_to_cutoff"].value == Decimal("10")
    legacy_full_day = artifact.values["location.utc_day_trade_vwap"]
    assert legacy_full_day.value is None
    assert legacy_full_day.missing_reason == "FULL_UTC_DAY_COVERAGE_UNVERIFIED"
    full_day = artifact.values["location.utc_day_trade_vwap_complete_day"]
    assert full_day.value is None
    assert full_day.missing_reason == "FULL_UTC_DAY_COVERAGE_UNVERIFIED"


def test_s1_refuses_gap_qualified_history_before_watch_creation(tmp_path, monkeypatch) -> None:
    from atlas.v2.memory.repository import OpsRepository
    from atlas.v2.strategies import s1_trend
    from atlas.v2.strategies.s1_trend import S1ShadowCoordinator

    from .test_session014_s1 import SETUP, asof_join, clear, store_for_side, universe_at

    joined = asof_join(store_for_side("LONG"), KEY, cutoff_ns=SETUP)
    gapped = replace(joined, h4=joined.h4[:20] + joined.h4[21:])
    feature = feature_snapshot(gapped)
    monkeypatch.setattr(s1_trend, "_setup", lambda *_args, **_kwargs: pytest.fail("gap reached setup"))
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        result = S1ShadowCoordinator(repository).create_watch(
            gapped, feature, event_gate=clear(SETUP), universe=universe_at(SETUP)
        )
    assert result.status == "NOT_ESTIMABLE"
    assert result.reason == "BAR_INTERVAL_COVERAGE_GAP"


def test_s1_refuses_legacy_feature_artifact_without_coverage_version(tmp_path) -> None:
    from atlas.v2.memory.repository import OpsRepository
    from atlas.v2.strategies.s1_trend import S1ShadowCoordinator

    from .test_session014_s1 import SETUP, asof_join, clear, store_for_side, universe_at

    joined = asof_join(store_for_side("LONG"), KEY, cutoff_ns=SETUP)
    feature = feature_snapshot(joined)
    legacy = replace(
        feature, feature_set_version="INTRADAY_CORE_V1", envelope=replace(feature.envelope, content_hash="")
    )
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        result = S1ShadowCoordinator(repository).create_watch(
            joined, legacy, event_gate=clear(SETUP), universe=universe_at(SETUP)
        )
    assert result.status == "NOT_ESTIMABLE"
    assert result.reason == "FEATURE_INTERVAL_COVERAGE_UNQUALIFIED"


def test_s1_refuses_conflicting_partial_vwap_identity(tmp_path) -> None:
    from atlas.v2.memory.repository import OpsRepository
    from atlas.v2.strategies.s1_trend import S1ShadowCoordinator

    from .test_session014_s1 import SETUP, asof_join, clear, store_for_side, universe_at

    joined = asof_join(store_for_side("LONG"), KEY, cutoff_ns=SETUP)
    trades = (
        CausalTrade(KEY, Decimal("10"), Decimal("1"), SETUP - 2, SETUP - 2, "source-a", "native-conflict"),
        CausalTrade(KEY, Decimal("11"), Decimal("1"), SETUP - 1, SETUP - 1, "source-b", "native-conflict"),
    )
    feature = feature_snapshot(joined, trades=trades)
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        result = S1ShadowCoordinator(repository).create_watch(
            joined, feature, event_gate=clear(SETUP), universe=universe_at(SETUP)
        )
    assert result.status == "NOT_ESTIMABLE"
    assert result.reason == "TRADE_IDENTITY_CONFLICT"
