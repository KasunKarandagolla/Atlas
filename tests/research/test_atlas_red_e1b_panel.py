"""Offline validation for the bounded R-E1B archive and math adapter."""
from __future__ import annotations

import csv
import gzip
import hashlib
import io
import pathlib
import sys
import zipfile
from statistics import covariance, stdev

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "docs/research/atlas-red-e1b"))

import acquire_archives as archives  # noqa: E402
import build_research_panel as panel  # noqa: E402
import summarize_research_outputs as summary  # noqa: E402


def csv_zip(symbol: str, interval: str, month: str, rows: list[list[str]]) -> bytes:
    name = f"{symbol}-{interval}-{month}.csv"
    output = io.StringIO()
    w = csv.writer(output)
    w.writerow(["open_time", "open", "high", "low", "close", "volume", "close_time",
                "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume", "ignore"])
    w.writerows(rows)
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr(name, output.getvalue())
    return mem.getvalue()


def test_archive_checksum_duplicate_and_gap_checks(tmp_path, monkeypatch) -> None:
    symbol, interval, month = "BTCUSDT", "1h", "2024-01"
    start = 1_704_067_200_000
    def row(t, p):
        return [str(t), str(p), str(p + 1), str(p - 1), str(p), "10", str(t + 3_599_999), "100", "2", "5", "50", "0"]
    data = csv_zip(symbol, interval, month, [row(start, 100), row(start + 3_600_000, 101), row(start + 10_800_000, 102)])
    digest = hashlib.sha256(data).hexdigest()
    name = f"{symbol}-{interval}-{month}.zip"
    monkeypatch.setattr(archives, "get", lambda url: ((digest + "  " + name + "\n").encode(), 1) if url.endswith("CHECKSUM") else (data, 1))
    result = archives.one((symbol, interval, month), tmp_path)
    assert result["status"] == "VERIFIED"
    assert result["downloaded_sha256"] == digest
    assert result["rows"] == 3
    assert result["gap_count"] == 1

    duplicate = csv_zip(symbol, interval, month, [row(start, 100), row(start, 101)])
    duplicate_hash = hashlib.sha256(duplicate).hexdigest()
    monkeypatch.setattr(archives, "get", lambda url: ((duplicate_hash + "  " + name + "\n").encode(), 1) if url.endswith("CHECKSUM") else (duplicate, 1))
    result = archives.one((symbol, interval, month), tmp_path)
    assert result["status"] == "FAILED_VALIDATION_OR_FETCH"
    assert "DUPLICATE_OR_OUT_OF_ORDER" in str(result["reason"])

    monkeypatch.setattr(archives, "get", lambda url: (("0" * 64 + "  " + name + "\n").encode(), 1) if url.endswith("CHECKSUM") else (data, 1))
    result = archives.one((symbol, interval, month), tmp_path)
    assert result["status"] == "FAILED_VALIDATION_OR_FETCH"
    assert result["reason"] == "ValueError: PUBLISHED_CHECKSUM_MISMATCH"


def test_s6_price_adapter_matches_existing_repository_return_and_rank_math() -> None:
    start = 1_704_067_200_000
    def synthetic(symbol: str, scale: float) -> list[dict[str, object]]:
        rows = []
        price = 100.0
        for i in range(800):
            price *= 1 + scale * (0.001 * ((i % 13) - 6) + 0.0002 * (i % 7))
            op = (start + i * 3_600_000) * 1_000_000
            rows.append({"open_ns": op, "close_ns": op + panel.HOUR, "open": str(price),
                         "high": str(price * 1.001), "low": str(price * 0.999), "close": str(price), "volume": "1"})
        return rows
    btc = panel.make_bars("BTCUSDT", "1h", synthetic("BTCUSDT", 1.0))
    asset = panel.make_bars("ETHUSDT", "1h", synthetic("ETHUSDT", 1.7))
    peer = panel.make_bars("BNBUSDT", "1h", synthetic("BNBUSDT", 0.8))
    h1 = {"BTCUSDT": btc, "ETHUSDT": asset, "BNBUSDT": peer}
    h4 = {s: panel.h4_bars_from_h1(s, bars) for s, bars in h1.items()}
    aligned = panel.aligned_return_series(h1)
    cutoff = btc[-1].close_at_ns
    scores, breadth, _ = panel.s6_scores(["BTCUSDT", "ETHUSDT", "BNBUSDT"], h4, aligned, cutoff)
    assert breadth == 2
    assert scores["ETHUSDT"]["state"] == "S6_FIXED_COHORT_RANK_DIAGNOSTIC"

    btc_times, btc_returns = panel.returns(btc)
    asset_times, asset_returns = panel.returns(asset)
    btc_rows, asset_rows = dict(zip(btc_times, btc_returns, strict=True)), dict(zip(asset_times, asset_returns, strict=True))
    end, begin = cutoff - cutoff % panel.HOUR, cutoff - cutoff % panel.HOUR - 30 * panel.DAY
    times = sorted(t for t in btc_rows.keys() & asset_rows.keys() if begin < t <= end)
    x = [asset_rows[t] for t in times]
    y = [btc_rows[t] for t in times]
    assert len(times) == 720
    beta = covariance(x, y) / (stdev(y) ** 2)
    residuals = [a - beta * b for a, b in zip(x, y, strict=True)]
    direct_score = sum(residuals[-4:]) / stdev(residuals)
    assert abs(float(scores["ETHUSDT"]["score"]) - direct_score) < 1e-8
    h4_tail = panel.contiguous_tail(h4["ETHUSDT"], cutoff, panel.H4, 50)
    closes = [float(b.close) for b in h4_tail]
    e20, e50 = panel.ema(closes, 20)[-1], panel.ema(closes, 50)[-1]
    expected_trend = "UP" if closes[-1] > e50 and e20 > e50 else "DOWN" if closes[-1] < e50 and e20 < e50 else "NEUTRAL"
    assert scores["ETHUSDT"]["own_4h_trend"] == expected_trend


def test_fixed_cohort_is_immutable() -> None:
    assert len(panel.COHORT) == 35
    assert panel.COHORT_SHA256 == "b51f4a360e60bbb62fae982e4120ff3681b12a13ef8cd4a0aff01d761f6f03b5"


def test_march_boundary_decision_uses_only_completed_seed_bars() -> None:
    boundary = panel.ns(2024, 3, 1)
    assert boundary in panel.decision_cutoffs(boundary, boundary + panel.M15)
    assert list(panel.native_slot_opens(boundary, boundary + panel.M15, panel.M15)) == [boundary - panel.M15]
    # The M15 observation available at 00:00 closed at the boundary and opened
    # at 23:45 the prior day; the next H1 close is deliberately present as a
    # future row and must not appear in the as-of prefix.
    m15_seed = panel.make_bars("BTCUSDT", "15m", [{
        "open_ns": boundary - panel.M15, "close_ns": boundary,
        "open": "100", "high": "102", "low": "99", "close": "101", "volume": "1",
    }])
    h1_seed = panel.make_bars("BTCUSDT", "1h", [
        {"open_ns": boundary - panel.HOUR, "close_ns": boundary,
         "open": "100", "high": "102", "low": "99", "close": "101", "volume": "1"},
        {"open_ns": boundary, "close_ns": boundary + panel.HOUR,
         "open": "101", "high": "104", "low": "100", "close": "103", "volume": "1"},
    ])
    assert m15_seed[0].close_at_ns == boundary
    completed = panel.contiguous_tail(h1_seed, boundary, panel.HOUR, 1)
    assert len(completed) == 1
    assert completed[0].close_at_ns == boundary
    assert completed[0].close_at_ns <= boundary < h1_seed[1].close_at_ns


def test_cached_s1_precursor_matches_repository_functions() -> None:
    start = panel.ns(2024, 1, 1)
    rows = []
    price = 100.0
    for i in range(400):
        price *= 1.001 + (0.004 if i % 17 == 0 else -0.0004 if i % 11 == 0 else 0)
        op = start + i * panel.HOUR
        rows.append({"open_ns": op, "close_ns": op + panel.HOUR, "open": str(price),
                     "high": str(price * 1.002), "low": str(price * 0.996 if i % 17 == 0 else price * 0.999),
                     "close": str(price), "volume": "1"})
    h1 = panel.make_bars("ETHUSDT", "1h", rows)
    h4 = panel.h4_bars_from_h1("ETHUSDT", h1)
    h1_close, h4_close = [float(b.close) for b in h1], [float(b.close) for b in h4]
    h1e20, h1e50 = list(panel.ema(h1_close, 20)), list(panel.ema(h1_close, 50))
    h4e20, h4e50 = list(panel.ema(h4_close, 20)), list(panel.ema(h4_close, 50))
    h1g, h4g = panel.gap_prefix(h1, panel.HOUR), panel.gap_prefix(h4, panel.H4)
    for i in range(250, len(h1), 13):
        cutoff = h1[i].close_at_ns
        state = panel.s1_cached_state("ETHUSDT", h1, h4, cutoff, h1e20, h1e50, h4e20, h4e50, h1g, h4g)
        h1prefix = panel.contiguous_prefix(h1, cutoff, panel.HOUR, start)
        h4prefix = panel.contiguous_prefix(h4, cutoff, panel.H4, start)
        joined = panel.JoinedBars(panel.key_for("ETHUSDT"), cutoff, h4prefix, h1prefix, (), "AVAILABLE", None, None)
        regime = panel._regime(joined)
        setup = panel._setup(joined)
        assert state["regime"] == (regime.value if regime else "NEUTRAL")
        assert state["state"] == ("SETUP" if setup else "NO_SETUP")
        if setup:
            assert state["side"] == setup[0].value
            assert state["setup_extreme"] == str(setup[1])


def test_cached_s1_and_m15_trigger_match_verified_real_prefix() -> None:
    fixture = ROOT / "tests/research/fixtures"

    def read_fixture(name: str) -> list[dict[str, object]]:
        with gzip.open(fixture / name, "rt", encoding="utf-8", newline="") as f:
            rows = []
            for row in csv.DictReader(f):
                rows.append({"open_ns": int(row["open_time"]) * 1_000_000,
                             "close_ns": (int(row["close_time"]) + 1) * 1_000_000,
                             "open": row["open"], "high": row["high"], "low": row["low"],
                             "close": row["close"], "volume": row["volume"]})
            return rows

    h1 = panel.make_bars("ETHUSDT", "1h", read_fixture("ETHUSDT-1h-2024-window-prefix.csv.gz"))
    h4 = panel.h4_bars_from_h1("ETHUSDT", h1)
    m15 = panel.make_bars("ETHUSDT", "15m", read_fixture("ETHUSDT-15m-2024-window-prefix.csv.gz"))
    m15_by_close = {bar.close_at_ns: bar for bar in m15}
    h1_close, h4_close = [float(b.close) for b in h1], [float(b.close) for b in h4]
    cached = (list(panel.ema(h1_close, 20)), list(panel.ema(h1_close, 50)),
              list(panel.ema(h4_close, 20)), list(panel.ema(h4_close, 50)),
              panel.gap_prefix(h1, panel.HOUR), panel.gap_prefix(h4, panel.H4))

    def compare(cutoff: int) -> tuple[dict[str, object], tuple[object, ...] | None]:
        state = panel.s1_cached_state("ETHUSDT", h1, h4, cutoff, *cached)
        prefix1 = panel.contiguous_prefix(h1, cutoff, panel.HOUR, panel.ns(2024, 1, 1))
        prefix4 = panel.contiguous_prefix(h4, cutoff, panel.H4, panel.ns(2024, 1, 1))
        if not prefix1 or not prefix4:
            assert state["state"] == "SOURCE_MISSING"
            return state, None
        if state["state"] in ("SOURCE_MISSING", "INSUFFICIENT_WARMUP_OR_GAP"):
            return state, None
        joined = panel.JoinedBars(panel.key_for("ETHUSDT"), cutoff, prefix4, prefix1, (), "AVAILABLE", None, None)
        regime, setup = panel._regime(joined), panel._setup(joined)
        assert state["regime"] == (regime.value if regime else "NEUTRAL")
        assert state["state"] == ("SETUP" if setup else "NO_SETUP")
        if setup:
            assert (state["side"], state["setup_extreme"]) == (setup[0].value, str(setup[1]))
        return state, setup

    # An early prefix is not rankable as a precursor; the UTC boundary has a
    # real completed M15 seed and a NO_SETUP state; later real states include
    # an S1 setup whose quarter-hour bar trigger is independently reproducible.
    warmup, _ = compare(h1[10].close_at_ns)
    assert warmup["state"] == "SOURCE_MISSING"
    boundary = panel.ns(2024, 3, 1)
    boundary_state, _ = compare(boundary)
    assert boundary_state["state"] == "NO_SETUP"
    assert m15_by_close[boundary].close_at_ns == boundary
    setup_cutoff = 1_709_290_800_000_000_000
    setup_state, setup = compare(setup_cutoff)
    assert setup_state["state"] == "SETUP" and setup is not None
    trigger_cutoff = 1_709_293_500_000_000_000
    trigger_bar, previous_bar = m15_by_close[trigger_cutoff], m15_by_close[trigger_cutoff - panel.M15]
    trigger = (setup[0].value == "LONG" and trigger_bar.close > previous_bar.high) or (
        setup[0].value == "SHORT" and trigger_bar.close < previous_bar.low)
    assert trigger


def test_r1_hour_boundary_keeps_originating_setup_and_unique_first_trigger() -> None:
    setup = {"state": "SETUP", "side": "LONG", "setup_extreme": "90", "s1_ref": "a" * 64}
    setup_cutoff = 1_710_000_000_000_000_000
    long_origin, first_event = panel.s1_setup_origin("ETHUSDT", setup_cutoff, setup,
        {"state": "NO_SETUP"}, {})
    assert first_event == 1
    repeated_setup = {**setup, "s1_ref": "c" * 64}
    carried_origin, repeated_event = panel.s1_setup_origin("ETHUSDT", setup_cutoff + panel.HOUR,
        repeated_setup, setup, long_origin)
    assert repeated_event == 0
    assert carried_origin == long_origin
    current_short = {"state": "SETUP", "side": "SHORT", "setup_extreme": "110", "s1_ref": "b" * 64}
    short_origin, short_event = panel.s1_setup_origin("ETHUSDT", setup_cutoff + panel.HOUR,
        current_short, setup, long_origin)
    assert short_event == 1
    source = panel.s1_trigger_source("ETHUSDT", setup_cutoff + panel.HOUR,
                                     setup_cutoff + panel.HOUR, short_origin, carried_origin)
    assert source["side"] == "LONG" and source["setup_extreme"] == "90"
    assert panel.s1_trigger_source("ETHUSDT", setup_cutoff + panel.HOUR, setup_cutoff + panel.HOUR,
                                   {}, carried_origin)["side"] == "LONG"
    changed_extreme, changed_extreme_event = panel.s1_setup_origin("ETHUSDT", setup_cutoff + 2 * panel.HOUR,
        {**setup, "setup_extreme": "91"}, setup, long_origin)
    assert changed_extreme_event == 1 and changed_extreme["setup_cutoff_ns"] == setup_cutoff + 2 * panel.HOUR
    assert changed_extreme["setup_id"] != long_origin["setup_id"]
    # At 13:00 a newly computed SHORT setup is descriptive only; the 12:00
    # LONG setup owns that final trigger opportunity.
    predicate = panel.s1_trigger_predicate("LONG", "101", "100", "100", "99")
    first = panel.s1_first_trigger(source, predicate, set(), setup_cutoff + panel.HOUR)
    repeated = panel.s1_first_trigger(source, predicate, {source["setup_id"]}, setup_cutoff + panel.HOUR + panel.M15)
    assert source["setup_id"] and source["side"] == "LONG"
    assert first["first_trigger_event"] == 1
    assert first["first_trigger_event_id"]
    assert repeated["first_trigger_event_id"] == ""
    assert repeated["first_trigger_event"] == 0
    assert repeated["raw_trigger_predicate"] == 1
    assert [panel.s1_setup_active(source, setup_cutoff + i * panel.M15) for i in range(1, 5)] == [True] * 4
    assert not panel.s1_setup_active(source, setup_cutoff + 5 * panel.M15)
    new_sequence, new_event = panel.s1_setup_origin("ETHUSDT", setup_cutoff + 2 * panel.HOUR,
        setup, {"state": "NO_SETUP"}, {})
    assert new_event == 1 and new_sequence["setup_id"] != source["setup_id"]


def test_r1_missing_or_noncontiguous_previous_m15_is_not_no_trigger() -> None:
    result = panel.s1_trigger_observation(
        active=True, setup={"setup_id": "setup-1", "side": "LONG"},
        current_present=True, previous_present=False, contiguous=False,
        current_close="101", previous_high=None, current_low="99", previous_low=None)
    assert result["state"] == "SOURCE_MISSING"
    assert result["reason"] == "PREVIOUS_M15_MISSING_OR_NONCONTIGUOUS"
    assert result["raw_trigger_predicate"] == 0
    current_gap = panel.s1_trigger_observation(
        active=False, setup={}, current_present=False, previous_present=True, contiguous=True,
        current_close="0", previous_high="1", current_low="0", previous_low="1")
    assert current_gap["state"] == "SOURCE_MISSING"
    assert current_gap["reason"] == "CURRENT_M15_MISSING"


def test_r1_pre_window_trigger_denominator_uses_only_verified_prior_bars() -> None:
    setup_cutoff = panel.ns(2024, 3, 1)
    setup = panel.s1_setup_identity("ETHUSDT", setup_cutoff,
        {"state": "SETUP", "side": "LONG", "setup_extreme": "90", "s1_ref": "d" * 64})
    rows = [
        {"open_ns": setup_cutoff - panel.M15, "close_ns": setup_cutoff, "open": "100", "high": "100",
         "low": "99", "close": "100", "volume": "1"},
        {"open_ns": setup_cutoff, "close_ns": setup_cutoff + panel.M15, "open": "100", "high": "102",
         "low": "99", "close": "101", "volume": "1"},
    ]
    bars = panel.make_bars("ETHUSDT", "15m", rows)
    by_close = {bar.close_at_ns: bar for bar in bars}
    assert panel.s1_prior_trigger_history(setup, setup_cutoff + 2 * panel.M15, by_close) == "TRIGGERED_BEFORE_WINDOW"
    assert panel.s1_prior_trigger_history(setup, setup_cutoff + panel.M15, by_close) == "NO_PRIOR_MATCH"
    assert panel.s1_prior_trigger_history(setup, setup_cutoff + 2 * panel.M15,
        {bars[1].close_at_ns: bars[1]}) == "PRIOR_TRIGGER_HISTORY_MISSING"
    unknown_first = panel.s1_first_trigger(setup, 1, set(), setup_cutoff + 2 * panel.M15,
                                           {str(setup["setup_id"])})
    assert unknown_first["first_trigger_event"] == 0
    assert unknown_first["first_trigger_reason"] == "PRIOR_TRIGGER_HISTORY_MISSING"


def test_r2_research_bars_separate_unknown_actual_receipt_from_replay_clock() -> None:
    close = panel.ns(2024, 3, 1)
    rows = [{"open_ns": close - panel.M15, "close_ns": close, "open": "100", "high": "102",
             "low": "99", "close": "101", "volume": "1", "archive_sha256": "a" * 64,
             "row_index": 7}]
    bar = panel.make_bars("BTCUSDT", "15m", rows)[0]
    rows[0]["retrieved_at_utc"] = "later-run-time-is-not-part-of-stable-source-identity"
    rebuilt = panel.make_bars("BTCUSDT", "15m", rows)[0]
    assert bar.actual_receipt_at_ns is None
    assert bar.replay_available_at_ns == close + 5_000_000_000
    assert bar.stable_source_row_id == "a" * 64 + ":7"
    assert bar.stable_source_row_id == rebuilt.stable_source_row_id
    assert bar.content_hash == rebuilt.content_hash
    assert not hasattr(bar, "raw")
    assert panel.bar_visible_at(bar, close) is False
    assert panel.bar_visible_at(bar, close + 5_000_000_000) is True


def test_r2_replay_sensitivities_and_feature_processing_are_explicit() -> None:
    close = panel.ns(2024, 3, 1)
    assert panel.replay_available_at(close, 5) == close + 5_000_000_000
    assert panel.replay_available_at(close, 30) == close + 30_000_000_000
    assert panel.replay_available_at(close, 60) == close + 60_000_000_000
    assert panel.derived_replay_available_at([close + 5_000_000_000]) == close + 6_000_000_000


def _build_active_trigger_gap_case(tmp_path, monkeypatch, side: str, *, scenario: str = "missing_before",
                                   append_future: bool = False):
    setup_cutoff = panel.ns(2024, 3, 1) + 12 * panel.HOUR
    opportunities = [setup_cutoff + i * panel.M15 for i in range(1, 5)]
    monkeypatch.setattr(panel, "COHORT", ("BTCUSDT",))
    monkeypatch.setattr(panel, "WINDOWS", (("A", 2024, 3, 1, 2024, 3, 1),))
    monkeypatch.setattr(panel, "decision_cutoffs", lambda _begin, _final: iter(opportunities))
    monkeypatch.setattr(panel, "write_manifest", lambda *_args: None)
    monkeypatch.setattr(panel, "slot_calendar", lambda *_args: None)

    def row(cutoff: int, high: str, low: str, close: str, row_index: int) -> dict[str, object]:
        return {"open_ns": cutoff - panel.M15, "close_ns": cutoff, "open": close,
                "high": high, "low": low, "close": close, "volume": "1",
                "archive_url": "https://data.binance.vision/synthetic-test-row",
                "archive_sha256": f"{row_index:064x}", "row_index": row_index}

    if scenario == "missing_before" and side == "LONG":
        observed = [row(opportunities[1], "100", "99", "100", 2),
                    row(opportunities[2], "102", "99", "101", 3),
                    row(opportunities[3], "104", "100", "103", 4)]
    elif scenario == "missing_before":
        observed = [row(opportunities[1], "101", "100", "100", 2),
                    row(opportunities[2], "101", "98", "99", 3),
                    row(opportunities[3], "100", "96", "97", 4)]
    elif scenario == "known_first_then_gap" and side == "LONG":
        observed = [row(setup_cutoff, "100", "99", "100", 1),
                    row(opportunities[0], "101", "99", "101", 2),
                    row(opportunities[2], "104", "100", "103", 4),
                    row(opportunities[3], "106", "101", "105", 5)]
    elif scenario == "known_first_then_gap":
        observed = [row(setup_cutoff, "101", "100", "100", 1),
                    row(opportunities[0], "101", "99", "99", 2),
                    row(opportunities[2], "100", "96", "97", 4),
                    row(opportunities[3], "99", "94", "95", 5)]
    elif scenario == "observed_nonmatch_until_match" and side == "LONG":
        observed = [row(setup_cutoff, "100", "99", "100", 1),
                    row(opportunities[0], "100", "99", "100", 2),
                    row(opportunities[1], "100", "99", "100", 3),
                    row(opportunities[2], "100", "99", "100", 4),
                    row(opportunities[3], "102", "99", "101", 5)]
    elif scenario == "observed_nonmatch_until_match":
        observed = [row(setup_cutoff, "101", "100", "100", 1),
                    row(opportunities[0], "101", "100", "100", 2),
                    row(opportunities[1], "101", "100", "100", 3),
                    row(opportunities[2], "101", "100", "100", 4),
                    row(opportunities[3], "101", "98", "99", 5)]
    else:
        raise ValueError(f"unsupported scenario {scenario}")
    if append_future:
        observed.append(row(opportunities[3] + panel.M15, "110", "90", "105", 5))
    monkeypatch.setattr(panel, "parse_raw", lambda *_args: {"BTCUSDT": {"1h": [], "15m": observed}})
    monkeypatch.setattr(panel, "aligned_return_series", lambda *_args: {})
    monkeypatch.setattr(panel, "s6_scores", lambda keys, *_args: (
        {symbol: {"state": "NOT_RANKABLE", "reason": "TEST_FIXTURE"} for symbol in keys}, 0, "TEST_FIXTURE"))

    def fake_s1_state(symbol, _h1, _h4, cutoff, *_args):
        is_setup = cutoff == setup_cutoff
        state = {"state": "SETUP" if is_setup else "NO_SETUP",
                 "side": side if is_setup else "", "setup_extreme": "100" if is_setup else "",
                 "s1_ref": "c" * 64 if is_setup else "", "reason": "",
                 "h1_last_close_ns": None, "h1_last_close": "", "h1_last_return": None,
                 "h1_ema20": None, "h1_ema50": None, "h4_trend": "UNAVAILABLE"}
        assert symbol == "BTCUSDT"
        return state

    monkeypatch.setattr(panel, "s1_cached_state", fake_s1_state)
    built = panel.build(tmp_path / f"out-{side}", tmp_path / f"scratch-{side}", [])
    output = panel.HERE / "DIAGNOSTIC_FEATURE_PANEL.csv.gz"
    output = tmp_path / f"out-{side}" / output.name
    with gzip.open(output, "rt", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 4
    assert built["panel_sha256"] == hashlib.sha256(output.read_bytes()).hexdigest()
    return rows, output.read_bytes()


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_r1_producer_censors_later_match_after_missing_active_m15_opportunities(
    tmp_path, monkeypatch, side: str
) -> None:
    rows, _ = _build_active_trigger_gap_case(tmp_path, monkeypatch, side)
    assert [row["s1_trigger_state"] for row in rows[:2]] == ["SOURCE_MISSING", "SOURCE_MISSING"]
    assert [row["s1_trigger_source_missing_reason"] for row in rows[:2]] == [
        "CURRENT_M15_MISSING", "PREVIOUS_M15_MISSING_OR_NONCONTIGUOUS"]
    assert [row["s1_raw_trigger_predicate"] for row in rows[:2]] == ["0", "0"]
    assert [row["s1_raw_trigger_predicate"] for row in rows[2:]] == ["1", "1"]
    assert not any(row["s1_first_trigger_event"] == "1" for row in rows)
    assert rows[2]["s1_first_trigger_reason"] in {
        "ACTIVE_TRIGGER_HISTORY_CENSORED", "PREWINDOW_AND_ACTIVE_HISTORY_CENSORED"}


def test_r1_producer_missing_gap_does_not_retroactively_censor_known_first(tmp_path, monkeypatch) -> None:
    rows, _ = _build_active_trigger_gap_case(tmp_path, monkeypatch, "LONG", scenario="known_first_then_gap")
    assert rows[0]["s1_first_trigger_event"] == "1"
    assert [row["s1_trigger_state"] for row in rows[1:3]] == ["SOURCE_MISSING", "SOURCE_MISSING"]
    assert rows[3]["s1_raw_trigger_predicate"] == "1"
    assert rows[3]["s1_first_trigger_event"] == "0"
    assert rows[3]["s1_trigger_episode_status"] == "TRIGGER_ALREADY_ESTABLISHED"


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_r1_observed_nonmatches_then_known_first_event(tmp_path, monkeypatch, side: str) -> None:
    rows, _ = _build_active_trigger_gap_case(
        tmp_path, monkeypatch, side, scenario="observed_nonmatch_until_match")
    assert [row["s1_trigger_state"] for row in rows] == ["NO_TRIGGER"] * 3 + ["BAR_TRIGGER"]
    assert [row["s1_first_trigger_event"] for row in rows] == ["0", "0", "0", "1"]


def test_r1_future_rows_do_not_change_first_event_or_identity(tmp_path, monkeypatch) -> None:
    with monkeypatch.context() as scoped:
        rows, output = _build_active_trigger_gap_case(tmp_path, scoped, "LONG")
    with monkeypatch.context() as scoped:
        future_rows, future_output = _build_active_trigger_gap_case(
            tmp_path, scoped, "LONG", append_future=True)
    assert rows == future_rows
    assert output == future_output


def test_r1_censoring_is_scoped_to_setup_identity() -> None:
    setup_a = {"setup_id": "setup-a", "side": "LONG"}
    setup_b = {"setup_id": "setup-b", "side": "LONG"}
    unknown = {"setup-a"}
    censored = panel.s1_first_trigger(setup_a, 1, set(), 4, unknown)
    verified = panel.s1_first_trigger(setup_b, 1, set(), 4, unknown)
    assert censored["first_trigger_event"] == 0
    assert censored["trigger_episode_status"] == "CENSORED_PREWINDOW"
    assert verified["first_trigger_event"] == 1
    assert verified["trigger_episode_status"] == "VERIFIED_FIRST_TRIGGER"


def test_r1_setup_and_m15_denominators_reconcile_by_exclusive_outcome() -> None:
    begin, final, m15 = 1_000, 1_060, 15

    def row(setup_id: str, origin: int, cutoff: int, *, state: str, raw: int = 0,
            status: str = "OBSERVED_NO_MATCH", pre: str = "NO_PRIOR_MATCH",
            first: int = 0, missing_reason: str = "",
            active: bool = True) -> dict[str, str]:
        return {
            "window": "A", "symbol": "BTCUSDT", "decision_cutoff_utc_ns": str(cutoff),
            "s1_raw_trigger_predicate": str(raw), "s1_trigger_state": state,
            "s1_trigger_window_active": "1" if active else "0", "s1_trigger_setup_id": setup_id,
            "s1_trigger_setup_cutoff_utc_ns": str(origin), "s1_trigger_setup_side": "LONG",
            "s1_trigger_setup_extreme": "100", "s1_trigger_setup_ref": f"ref-{setup_id}",
            "s1_trigger_pre_window_history_status": pre, "s1_trigger_episode_status": status,
            "s1_trigger_source_missing_reason": missing_reason,
            "s1_first_trigger_event": str(first),
            "s1_first_trigger_event_id": hashlib.sha256(f"{setup_id}:{cutoff}".encode()).hexdigest()
            if first else "",
            "s1_setup_event_id": "", "s1_setup_event_cutoff_utc_ns": "",
            "s1_setup_event_side": "", "s1_setup_event_extreme": "", "s1_setup_event_ref": "",
        }

    rows = []
    # Fully observed nonmatches before one verified first predicate.
    rows += [row("known-first", 985, cutoff, state="BAR_TRIGGER" if i == 4 else "NO_TRIGGER", raw=int(i == 4),
                  status="VERIFIED_FIRST_TRIGGER" if i == 4 else "OBSERVED_NO_MATCH",
                  first=int(i == 4))
             for i, cutoff in enumerate((1_000, 1_015, 1_030, 1_045), start=1)]
    # Two missing active opportunities, then visible positive matches remain censored.
    rows += [row("active-gap", 985, cutoff, state="SOURCE_MISSING" if i <= 2 else "BAR_TRIGGER",
                  raw=int(i >= 3), status="CENSORED_ACTIVE", missing_reason="CURRENT_M15_MISSING"
                  if i == 1 else "PREVIOUS_M15_MISSING_OR_NONCONTIGUOUS" if i == 2 else "")
             for i, cutoff in enumerate((1_000, 1_015, 1_030, 1_045), start=1)]
    # Missing pre-window history is an independent censor reason.
    rows += [row("prior-gap", 970, cutoff, state="BAR_TRIGGER" if i <= 3 else "NOT_IN_TRIGGER_WINDOW",
                  raw=int(i <= 3), active=i <= 3,
                  status="CENSORED_PREWINDOW", pre="PRIOR_TRIGGER_HISTORY_MISSING")
             for i, cutoff in enumerate((1_000, 1_015, 1_030, 1_045), start=1)]
    # A trigger verified before the window stays established across a missing row.
    rows += [row("prior-trigger", 970, cutoff,
                  state="SOURCE_MISSING" if i == 1 else "NOT_IN_TRIGGER_WINDOW" if i == 4 else "NO_TRIGGER",
                  active=i <= 3, status="TRIGGERED_BEFORE_WINDOW", pre="TRIGGERED_BEFORE_WINDOW",
                  missing_reason="CURRENT_M15_MISSING" if i == 1 else "")
             for i, cutoff in enumerate((1_000, 1_015, 1_030, 1_045), start=1)]
    # Four fully observed nonmatches are a complete no-match setup.
    rows += [row("no-match", 985, cutoff, state="NO_TRIGGER")
             for cutoff in (1_000, 1_015, 1_030, 1_045)]
    # A setup near the window end has opportunities outside the available window.
    rows.append(row("window-end", 1_045, 1_045, state="NOT_IN_TRIGGER_WINDOW", status="NO_MATCH", active=False))

    result = summary.reconcile_s1_trigger_rows(rows, {"A": (begin, final)}, m15)["A:BTCUSDT"]
    assert result["m15_row_counts"]["raw_positive_predicates"] == 6
    assert result["m15_row_counts"]["observed_positive_predicates_after_censorship"] == 5
    assert result["m15_row_counts"]["source_missing_trigger_rows_by_reason"] == {
        "CURRENT_M15_MISSING": 2, "PREVIOUS_M15_MISSING_OR_NONCONTIGUOUS": 1}
    assert result["setup_counts"] == {
        "unique_setup_identities": 6,
        "verified_first_trigger_events": 1,
        "censored_by_missing_pre_window_history": 1,
        "censored_by_missing_active_window_observation": 1,
        "censored_by_either_missing_history": 2,
        "complete_observation_no_match": 1,
        "previously_established_trigger": 1,
        "exclusive_outcome_partition": {
            "censored": 2, "complete_observed_no_match": 1, "incomplete_or_unresolved": 1,
            "triggered_before_window": 1, "verified_first_in_window": 1,
        },
        "exclusive_outcome_partition_total": 6,
        "exclusive_outcome_partition_reconciles": True,
    }

    duplicate_first_rows = [dict(item) for item in rows]
    repeated_first = dict(next(item for item in rows if item["s1_first_trigger_event"] == "1"))
    repeated_first["decision_cutoff_utc_ns"] = "1050"
    repeated_first["s1_first_trigger_event_id"] = hashlib.sha256(
        f"{repeated_first['s1_trigger_setup_id']}:1050".encode()).hexdigest()
    duplicate_first_rows.append(repeated_first)
    with pytest.raises(ValueError, match="MULTIPLE_VERIFIED_FIRST_EVENTS_PER_SETUP"):
        summary.reconcile_s1_trigger_rows(duplicate_first_rows, {"A": (begin, final)}, m15)

    censored_first_rows = [dict(item) for item in rows]
    false_first = next(item for item in censored_first_rows if item["s1_trigger_setup_id"] == "active-gap"
                       and item["s1_raw_trigger_predicate"] == "1")
    false_first["s1_first_trigger_event"] = "1"
    false_first["s1_first_trigger_event_id"] = hashlib.sha256(
        f"{false_first['s1_trigger_setup_id']}:{false_first['decision_cutoff_utc_ns']}".encode()).hexdigest()
    false_first["s1_trigger_episode_status"] = "VERIFIED_FIRST_TRIGGER"
    with pytest.raises(ValueError, match="CENSORED_SETUP_HAS_VERIFIED_FIRST_EVENT"):
        summary.reconcile_s1_trigger_rows(censored_first_rows, {"A": (begin, final)}, m15)


def test_research_summary_preserves_prior_checkpoint_parser_attempt_facts() -> None:
    assert summary.PRIOR_CHECKPOINT["commit"] == "ab645e792fe6593d9fb206954585b2996673f96c"
    assert summary.PRIOR_CHECKPOINT["dataset_manifest_sha256"] == (
        "f22f7e16ab8fd9d92b84b8df51cf9d6e662d1ffc03465acdcc5d9e7c8aa89e0e")
    assert summary.PRIOR_CHECKPOINT["verified_archives"] == 757
    assert summary.PRIOR_CHECKPOINT["official_404_objects"] == 13
    assert summary.PRIOR_CHECKPOINT["parser_rejected_attempt_rows"] == 420
    assert summary.PRIOR_CHECKPOINT["parser_rejected_attempt_download_bytes"] == "NOT_CAPTURED_BY_REJECTED_PARSER"


def test_clean_clone_rebuild_attestation_requires_empty_scratch_and_output() -> None:
    base = {
        "producer_commit_sha": "04f05ab641457ab6deee8456da2c2a77d84f5149",
        "scratch_empty": True,
        "output_empty": True,
        "official_retrieval_and_checksum_verified": True,
    }
    assert summary.clean_clone_rebuild_qualified(**base)
    assert not summary.clean_clone_rebuild_qualified(**{**base, "scratch_empty": False})
    assert not summary.clean_clone_rebuild_qualified(**{**base, "output_empty": False})
    assert not summary.clean_clone_rebuild_qualified(
        **{**base, "official_retrieval_and_checksum_verified": False})
    assert not summary.clean_clone_rebuild_qualified(**{**base, "producer_commit_sha": None})
