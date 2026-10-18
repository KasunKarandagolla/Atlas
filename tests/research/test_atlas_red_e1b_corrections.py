"""D1–D3 producer regressions; original accepted artifacts stay unchanged."""

from __future__ import annotations

import csv
import gzip
import json
import math
import os
import pathlib
import sys
from types import SimpleNamespace

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "docs/research/atlas-red-e1b"))
import build_research_panel as producer  # noqa: E402


def hourly(year, count=280, initial=100.0, increment=0.02):
    begin = producer.ns(year, 1, 1)
    rows = []
    for i in range(count):
        p = initial + increment * i
        rows.append(
            {
                "open_ns": begin + i * producer.HOUR,
                "close_ns": begin + (i + 1) * producer.HOUR,
                "open": str(p),
                "high": str(p + 1),
                "low": str(p - 1),
                "close": str(p),
                "volume": "1",
                "archive_sha256": "a" * 64,
                "row_index": i + 1,
            }
        )
    return producer.make_bars("TESTUSDT", "1h", rows)


def state(h1, h4, cutoff):
    return producer.s1_cached_state(
        "TESTUSDT",
        h1,
        h4,
        cutoff,
        list(producer.ema([float(b.close) for b in h1], 20)),
        list(producer.ema([float(b.close) for b in h1], 50)),
        list(producer.ema([float(b.close) for b in h4], 20)),
        list(producer.ema([float(b.close) for b in h4], 50)),
        producer.gap_prefix(h1, producer.HOUR),
        producer.gap_prefix(h4, producer.H4),
    )


@pytest.mark.parametrize("frame", ["H1", "H4"])
def test_d1_rejects_stale_native_terminal(frame):
    h1 = hourly(2025)
    h4 = producer.h4_bars_from_h1("TESTUSDT", h1)
    cutoff = h1[-1].close_at_ns
    assert state(h1, h4, cutoff)["state"] in ("SETUP", "NO_SETUP")
    if frame == "H1":
        h1 = h1[:-1]
        producer.BAR_INDEX[id(h1)] = [b.close_at_ns for b in h1]
    else:
        h4 = h4[:-1]
        producer.BAR_INDEX[id(h4)] = [b.close_at_ns for b in h4]
    assert state(h1, h4, cutoff)["state"] == "SOURCE_MISSING"


def test_d2_explicit_origins_do_not_carry_disconnected_ema():
    a, b = hourly(2024, initial=10.0), hourly(2025, initial=200.0)
    origin_a, origin_b = producer.ns(2024, 1, 1), producer.ns(2025, 1, 1)
    actual = producer.research_prefix_ema(a + b, 50, (origin_a, origin_b))
    assert actual[: len(a)] == list(producer.ema([float(x.close) for x in a], 50))
    assert actual[len(a) :] == list(producer.ema([float(x.close) for x in b], 50))
    assert actual[len(a)] is None


def test_d2_calendar_year_does_not_itself_reset_a_contiguous_history():
    h1 = hourly(2024, count=9000)
    expected = list(producer.ema([float(b.close) for b in h1], 50))
    assert producer.research_prefix_ema(h1, 50, (producer.ns(2024, 1, 1),)) == expected


def test_d2_gap_inside_declared_prefix_does_not_silently_restart():
    h1 = hourly(2025)
    del h1[30]
    producer.BAR_INDEX[id(h1)] = [b.close_at_ns for b in h1]
    h4 = producer.h4_bars_from_h1("TESTUSDT", h1)
    assert state(h1, h4, h1[-1].close_at_ns)["state"] == "SOURCE_MISSING"


@pytest.mark.parametrize("value", [0.0, 0, None])
def test_d3_optional_numeric_preserves_zero_and_missing(value):
    got = producer.research_number(value)
    assert got == ("" if value is None else 0.0)


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_d3_nonfinite_is_not_zero(value):
    with pytest.raises(ValueError, match="NONFINITE"):
        producer.research_number(value)


def test_d2_declared_seed_matches_pure_production_flat_equal_case():
    from atlas.v2.strategies.s1_trend import _regime

    h1 = hourly(2025, initial=100.0, increment=0.0)
    h4 = producer.h4_bars_from_h1("TESTUSDT", h1)
    assert _regime(SimpleNamespace(h1=h1, h4=h4)) is None
    values = producer.research_prefix_ema(h4, 50, (producer.ns(2025, 1, 1),))
    assert values[-1] == 100.0


@pytest.mark.parametrize("value", [0.0, None])
def test_d3_actual_builder_anchor_serialization(tmp_path, monkeypatch, value):
    monkeypatch.setattr(producer, "COHORT", ["BTCUSDT", "ETHUSDT"])
    monkeypatch.setattr(producer, "WINDOWS", (("A", 2024, 3, 1, 2024, 3, 1),))
    begin = producer.ns(2024, 3, 1)
    monkeypatch.setattr(producer, "decision_cutoffs", lambda *_: (begin,))
    monkeypatch.setattr(producer, "parse_raw", lambda *_: {})

    def no_setup(*_):
        return {"state": "NO_SETUP", "reason": "", "h1_last_return": value}

    monkeypatch.setattr(producer, "s1_cached_state", no_setup)
    producer.build(tmp_path, tmp_path, [])
    with gzip.open(tmp_path / "DIAGNOSTIC_FEATURE_PANEL.csv.gz", "rt") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 2
    for row in rows:
        for anchor in ("btc", "eth"):
            assert row[anchor + "_h1_return"] == ("" if value is None else "0.0")


def test_d1_actual_builder_physical_h1_identity_is_independent_of_context(tmp_path, monkeypatch):
    monkeypatch.setattr(producer, "COHORT", ["BTCUSDT"])
    monkeypatch.setattr(producer, "WINDOWS", (("A", 2024, 3, 1, 2024, 3, 1),))
    begin = producer.ns(2024, 3, 1)
    monkeypatch.setattr(producer, "decision_cutoffs", lambda *_: (begin,))
    raw = []
    for i, end in enumerate((begin - producer.HOUR, begin), 1):
        raw.append(
            {
                "open_ns": end - producer.HOUR,
                "close_ns": end,
                "open": "100",
                "high": "101",
                "low": "99",
                "close": "100",
                "volume": "1",
                "archive_sha256": "a" * 64,
                "row_index": i,
                "archive_url": "fixture",
                "archive_bytes": 1,
            }
        )
    monkeypatch.setattr(producer, "parse_raw", lambda *_: {"BTCUSDT": {"1h": raw}})
    monkeypatch.setattr(
        producer,
        "s1_cached_state",
        lambda *_: {
            "state": "SOURCE_MISSING",
            "reason": "TEST_CONTEXT_MISSING",
            "h1_last_close_ns": begin - producer.HOUR,
        },
    )
    producer.build(tmp_path, tmp_path, [])
    with gzip.open(tmp_path / "DIAGNOSTIC_FEATURE_PANEL.csv.gz", "rt") as f:
        row = next(csv.DictReader(f))
    assert row["h1_row_index"] == "2"
    assert row["h1_archive_sha256"] == "a" * 64
    assert row["h1_source_status"] == "OBSERVED"
    assert row["s1_precursor_state"] == "SOURCE_MISSING"


def real_h1(symbol):
    cache = os.environ.get("ATLAS_SOL_CSV_CACHE")
    if not cache:
        pytest.skip("external verified CSV cache not supplied")
    manifest = [
        r
        for r in map(json.loads, (producer.HERE / "SOURCE_MANIFEST.jsonl").read_text().splitlines())
        if r.get("status") == "VERIFIED" and r["symbol"] == symbol and r["interval"] == "1h"
    ]
    raw = producer.parse_raw(pathlib.Path(cache), manifest)[symbol]["1h"]
    h1 = producer.make_bars(symbol, "1h", raw)
    return h1, producer.h4_bars_from_h1(symbol, h1)


def real_state(symbol, cutoff):
    h1, h4 = real_h1(symbol)
    origins = (producer.ns(2024, 1, 1), producer.ns(2025, 1, 1))
    result = producer.s1_cached_state(
        symbol,
        h1,
        h4,
        cutoff,
        producer.research_prefix_ema(h1, 20, origins),
        producer.research_prefix_ema(h1, 50, origins),
        producer.research_prefix_ema(h4, 20, origins),
        producer.research_prefix_ema(h4, 50, origins),
        producer.gap_prefix(h1, producer.HOUR),
        producer.gap_prefix(h4, producer.H4),
    )
    return result, h1, h4


def test_d2_real_ftm_declared_prefix_matches_pure_production():
    from atlas.v2.strategies.s1_trend import _regime, _setup

    cutoff = producer.ns(2025, 3, 1)
    state, h1, h4 = real_state("FTMUSDT", cutoff)
    begin = producer.ns(2025, 1, 1)
    join = SimpleNamespace(
        h1=tuple(b for b in h1 if begin < b.close_at_ns <= cutoff),
        h4=tuple(b for b in h4 if begin < b.close_at_ns <= cutoff),
    )
    side = _regime(join)
    assert state["h4_trend"] == ("UP" if side and side.value == "LONG" else "DOWN" if side else "NEUTRAL")
    assert (state["state"] == "SETUP") == (_setup(join) is not None)


def test_d1_real_eos_first_missing_h1_has_no_context_identity():
    cutoff = 1747825200000000000
    state, h1, _ = real_state("EOSUSDT", cutoff)
    assert state["state"] == "SOURCE_MISSING"
    assert "h1_last_close_ns" not in state and "s1_ref" not in state
    assert max(b.close_at_ns for b in h1 if b.close_at_ns <= cutoff) == cutoff - producer.HOUR


@pytest.mark.parametrize(
    "symbol,cutoff",
    [("ETHUSDT", 1715814000000000000), ("ETHUSDT", 1746126000000000000), ("BTCUSDT", 1748224800000000000)],
)
def test_d3_real_observed_zero_anchors(symbol, cutoff):
    state, _, _ = real_state(symbol, cutoff)
    assert state["h1_last_return"] == 0.0
    assert producer.research_number(state["h1_last_return"]) == 0.0


def test_production_history_retains_seed_across_gap_without_restart():
    from v2.test_session014_core import bar
    from v2.test_session037_strategy_prefix import state_for

    from atlas.v2.data.bars import BarIntervalV2
    from atlas.v2.features.technical import interval_coverage_complete

    bars = tuple(bar(i, interval=BarIntervalV2.H1, close=str(100 + i)) for i in range(70) if i != 30)
    state = state_for(bars, BarIntervalV2.H1)
    assert state.total_count == 69
    assert state.ema50 == list(producer.ema([float(b.close) for b in bars], 50))[-1]
    assert not interval_coverage_complete(state.bars)
