"""Independent research oracles; no outcomes, orders, or capital decisions."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import math
import os
import pathlib
import sys
import zipfile
from decimal import Decimal, localcontext
from types import SimpleNamespace

import numpy as np
import pytest

RESEARCH = pathlib.Path(__file__).resolve().parents[2] / "docs/research/atlas-red-e1b"
sys.path.insert(0, str(RESEARCH))
import independent_sol_reproduction as oracle  # noqa: E402
import verify_sol_sources as sources  # noqa: E402


def bar(end, close="100", *, high="102", low="98", interval="15m"):
    return oracle.Bar(end, interval, (close, high, low, close, "1"), "synthetic", "synthetic_archive", 1)


def origin(side="LONG", born=0, ref="r"):
    return oracle.Origin("TESTUSDT", born, side, "99", ref)


def test_ema_seed_and_recurrence():
    assert oracle.independent_ema([1.0, 2.0, 3.0, 4.0, 5.0], 3) == [None, None, 2.0, 3.0, 4.0]
    assert oracle.independent_ema([1.0], 3) == [None]


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_four_native_opportunities_first_then_repeat_and_expiry(side):
    m = oracle.M15
    close = "103" if side == "LONG" else "97"
    bars = {0: bar(0), **{i * m: bar(i * m, close) for i in range(1, 6)}}
    ep = oracle.Episodes(0, bars)
    setup = origin(side)
    first = ep.observe(setup, m)
    assert first["s1_first_trigger_event"] == 1
    for i in (2, 3, 4):
        row = ep.observe(setup, i * m)
        assert row["s1_trigger_window_active"] == 1
        assert row["s1_first_trigger_event"] == 0
    assert ep.observe(setup, 5 * m)["s1_trigger_window_active"] == 0
    assert ep.partition()["exclusive_partition"] == {"verified_first_in_window": 1}


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_missing_active_current_and_prior_cannot_create_later_first(side):
    m = oracle.M15
    close = "103" if side == "LONG" else "97"
    bars = {0: bar(0), 2 * m: bar(2 * m, close), 3 * m: bar(3 * m, close)}
    ledger = oracle.Episodes(0, bars)
    setup = origin(side)
    a, b, c = [ledger.observe(setup, t) for t in (m, 2 * m, 3 * m)]
    assert a["s1_trigger_source_missing_reason"] == "CURRENT_M15_MISSING"
    assert b["s1_trigger_source_missing_reason"] == "PREVIOUS_M15_MISSING_OR_NONCONTIGUOUS"
    assert c["s1_raw_trigger_predicate"] == 1
    assert c["s1_first_trigger_event"] == 0
    assert c["s1_trigger_episode_status"] == "CENSORED_ACTIVE"


def test_pre_window_unknown_cannot_be_repaired_by_later_positive():
    m = oracle.M15
    bars = {0: bar(0), 2 * m: bar(2 * m, "103"), 3 * m: bar(3 * m, "103"), 4 * m: bar(4 * m, "103")}
    ledger = oracle.Episodes(4 * m, bars)
    row = ledger.observe(origin(), 4 * m)
    assert row["s1_raw_trigger_predicate"] == 1
    assert row["s1_first_trigger_event"] == 0
    assert row["s1_trigger_episode_status"] == "CENSORED_PREWINDOW"


def test_complete_nonmatches_include_observed_pre_window_opportunities():
    m = oracle.M15
    bars = {i * m: bar(i * m) for i in range(5)}
    ledger = oracle.Episodes(4 * m, bars)
    assert ledger.observe(origin(), 4 * m)["s1_first_trigger_event"] == 0
    assert ledger.partition()["exclusive_partition"] == {"complete_observed_no_match": 1}


def test_end_of_window_is_unresolved_without_later_opportunities():
    ledger = oracle.Episodes(0, {0: bar(0)})
    ledger.get(origin())
    assert ledger.partition()["exclusive_partition"] == {"incomplete_or_unresolved": 1}


def test_oracle_does_not_import_producer_mathematics():
    import ast

    tree = ast.parse((RESEARCH / "independent_sol_reproduction.py").read_text())
    modules = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            modules.append(node.module or "")
        elif isinstance(node, ast.Import):
            modules.extend(item.name for item in node.names)
    assert not any("build_research_panel" in m or "acquire_archives" in m or "summarize_research" in m for m in modules)
    assert [m for m in modules if m.startswith("atlas.")] == ["atlas.v2.strategies.s1_trend"]


def test_established_first_survives_gap_and_identity_isolation():
    m = oracle.M15
    bars = {0: bar(0), m: bar(m, "103"), 3 * m: bar(3 * m), 4 * m: bar(4 * m, "103")}
    ledger = oracle.Episodes(0, bars)
    a = origin()
    assert ledger.observe(a, m)["s1_first_trigger_event"] == 1
    ledger.observe(a, 2 * m)
    assert ledger.observe(a, 4 * m)["s1_trigger_episode_status"] == "TRIGGER_ALREADY_ESTABLISHED"
    b = origin(born=3 * m, ref="different")
    assert ledger.observe(b, 4 * m)["s1_first_trigger_event"] == 1


def test_exact_h1_boundary_owns_prior_setup_fourth_opportunity():
    h = oracle.HOUR
    old, new = origin(born=0), origin(born=h, ref="new")
    prefix = SimpleNamespace(at=lambda *_: {"s1_precursor_state": "NO_SETUP"})
    bars = {h - oracle.M15: bar(h - oracle.M15), h: bar(h, "103")}
    ledger = oracle.Episodes(h, bars)
    # Supply complete earlier observed nonmatches, preserving causal ownership.
    for i in (0, 1, 2):
        bars[i * oracle.M15] = bar(i * oracle.M15)
    row = oracle.s1_row(prefix, "primary", h, {0: old, h: new}, {h: 1}, ledger)
    assert row["s1_setup_event_id"] == new.identity
    assert row["s1_trigger_setup_id"] == old.identity
    assert row["s1_first_trigger_event"] == 1


def test_completed_prefix_rejects_gap_stale_tail_and_future():
    h = oracle.HOUR
    bars = [bar(i * h, interval="1h") for i in range(1, 5)]
    assert len(oracle.completed_prefix(bars, 3 * h, h, 0)) == 3
    assert oracle.completed_prefix(bars[:2], 3 * h, h, 0) == []
    assert oracle.completed_prefix([bars[0], bars[2]], 3 * h, h, 0) == []
    assert oracle.completed_prefix(bars + [bar(100 * h, interval="1h")], 3 * h, h, 0) == bars[:3]


@pytest.mark.parametrize("fault", ["future", "duplicate", "wrong_symbol", "unknown_window", "exclusive_end"])
def test_native_calendar_rejects_invalid_or_future_keys(fault):
    cohort = ["BTCUSDT", "ETHUSDT"]
    begin = oracle.timestamp(2024, 3, 1)
    row = {"window": "A", "symbol": "BTCUSDT", "decision_cutoff_utc_ns": str(begin)}
    assert oracle.panel_calendar_key(row, cohort, 0) == begin
    index = 0
    if fault == "future":
        row["decision_cutoff_utc_ns"] = str(begin + oracle.M15)
    elif fault == "duplicate":
        index = 2
    elif fault == "wrong_symbol":
        row["symbol"] = "ETHUSDT"
    elif fault == "unknown_window":
        row["window"] = "C"
    else:
        row["decision_cutoff_utc_ns"] = str(oracle.timestamp(2024, 7, 1))
        index = 11712 * 2
    with pytest.raises(ValueError):
        oracle.panel_calendar_key(row, cohort, index)


def synthetic_h1(year, count=500, reverse=False):
    start = oracle.timestamp(year, 1, 1)
    result = []
    for i in range(1, count + 1):
        price = 100 + (-1 if reverse else 1) * i * 0.04
        result.append(bar(start + i * oracle.HOUR, str(price), high=str(price + 1), low=str(price - 1), interval="1h"))
    return result


@pytest.mark.parametrize("reverse", [False, True])
def test_s1_independent_synthetic_regime_setup_matches_pure_production(reverse):
    from atlas.v2.strategies.s1_trend import _regime, _setup

    bars = synthetic_h1(2024, reverse=reverse)
    prefix = oracle.S1Prefixes("TESTUSDT", bars)
    cutoff = bars[-1].end
    expected = prefix.at(cutoff)

    def convert(b):
        return SimpleNamespace(
            close=Decimal(b.prices[3]), high=Decimal(b.prices[1]), low=Decimal(b.prices[2]), content_hash=b.fingerprint
        )

    join = SimpleNamespace(h1=tuple(map(convert, bars)), h4=tuple(map(convert, prefix.h4)))
    regime = _regime(join)
    assert regime.value == ("SHORT" if reverse else "LONG")
    setup = _setup(join)
    assert (setup is not None) == (expected["s1_precursor_state"] == "SETUP")
    if setup:
        assert setup[0].value == expected["s1_side"]
        assert str(setup[1]) == expected["extreme"]


def test_s1_future_and_previous_year_cannot_change_primary_prefix():
    old, current = synthetic_h1(2024), synthetic_h1(2025)
    t = current[300].end
    fresh = oracle.S1Prefixes("TESTUSDT", current).at(t)
    joined = oracle.S1Prefixes("TESTUSDT", old + current).at(t)
    truncated = oracle.S1Prefixes("TESTUSDT", current[:301]).at(t)
    assert fresh == joined == truncated
    stale = oracle.S1Prefixes("TESTUSDT", current[:300]).at(t)
    assert stale["s1_precursor_state"] == "SOURCE_MISSING"


def test_s6_centered_float_against_50_digit_decimal_oracle():
    x = np.array([0.001 * math.sin(i * 0.23) + 0.0002 * math.cos(i * 0.11) for i in range(720)])
    y = np.array([0.0007 * math.sin(i * 0.23) + 0.0003 * math.cos(i * 0.07) for i in range(720)])
    actual = oracle.s6_moments(x, y)
    with localcontext() as ctx:
        ctx.prec = 50
        a, b = [[Decimal.from_float(float(n)) for n in z] for z in (x, y)]
        am, bm = sum(a) / 720, sum(b) / 720
        beta = sum((v - am) * (w - bm) for v, w in zip(a, b, strict=True)) / sum((w - bm) ** 2 for w in b)
        residual = [v - beta * w for v, w in zip(a, b, strict=True)]
        mean = sum(residual) / 720
        vol = (sum((r - mean) ** 2 for r in residual) / 719).sqrt()
        score = sum(residual[-4:]) / vol
    for got, expected in zip(actual, (beta, vol, score), strict=True):
        assert abs(got - float(expected)) <= 2e-14 * max(1.0, abs(float(expected)))


@pytest.mark.parametrize("kind", ["short", "nan", "zero_btc", "zero_residual"])
def test_s6_rejects_incomplete_and_unrankable(kind):
    y = np.arange(720, dtype=float) / 1000
    x = y.copy()
    if kind == "short":
        x = x[:-1]
    elif kind == "nan":
        x[10] = np.nan
    elif kind == "zero_btc":
        y[:] = 0
    with pytest.raises(ValueError):
        oracle.s6_moments(x, y)


def test_rank_tie_decile_and_empty():
    assert oracle.rank_scores({}) == {}
    scores = {f"A{i:02}USDT": 1.0 for i in reversed(range(21))}
    ranks = oracle.rank_scores(scores)
    assert ranks["A00USDT"] == (1, "TOP")
    assert ranks["A02USDT"] == (3, "TOP")
    assert ranks["A03USDT"] == (4, "MIDDLE")
    assert ranks["A20USDT"] == (21, "BOTTOM")


@pytest.mark.parametrize("lag", [5, 30, 60])
def test_replay_readiness_strictly_after_last_visible_dependency(lag):
    t = oracle.timestamp(2024, 3, 1)
    assert oracle.replay_readiness([t - oracle.HOUR, t], lag) == t + (lag + 1) * oracle.SECOND


def test_comparator_exposes_false_first_and_numeric_error():
    c = oracle.Comparisons()
    row = {
        "window": "A",
        "symbol": "TESTUSDT",
        "decision_cutoff_utc_ns": "0",
        "s1_first_trigger_event": "1",
        "s6_score": "1.0001",
    }
    assert c.compare("s1_primary", row, {"s1_first_trigger_event": 0})
    assert c.compare("s6", row, {"s6_score": 1.0})
    assert c.report()["rows_with_mismatch"] == {"s1_primary:A": 1, "s6:A": 1}
    assert c.compare("anchor_context", {**row, "btc_h1_return": ""}, {"btc_h1_return": 0.0})
    assert not c.compare("anchor_context", {**row, "btc_h1_return": "0"}, {"btc_h1_return": 0.0})


@pytest.mark.parametrize("corrupt", ["none", "sha", "csv", "member"])
def test_independent_zip_binding_rejects_forgery(corrupt):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("fixture.csv", b"real source\n")
    payload = buf.getvalue()
    sha = hashlib.sha256(payload).hexdigest()
    row = {"published_sha256": sha, "downloaded_sha256": sha, "member": "fixture.csv"}
    cached = b"real source\n"
    if corrupt == "sha":
        row["published_sha256"] = "0" * 64
    if corrupt == "csv":
        cached = b"fabricated\n"
    if corrupt == "member":
        row["member"] = "other.csv"
    if corrupt == "none":
        assert sources.verify_member(payload, row, cached)["csv_sha256"] == hashlib.sha256(cached).hexdigest()
    else:
        with pytest.raises(ValueError):
            sources.verify_member(payload, row, cached)


def test_real_checksum_backed_eth_prefix_against_pure_production():
    from atlas.v2.strategies.s1_trend import _regime, _setup

    cache = os.environ.get("ATLAS_SOL_CSV_CACHE")
    if not cache:
        pytest.skip("external checksum-backed CSV cache not provided")
    manifest = [json.loads(line) for line in (RESEARCH / "SOURCE_MANIFEST.jsonl").read_text().splitlines()]
    proofs = {
        r["url"]: r
        for r in map(json.loads, (RESEARCH / "SOL_SOURCE_CHECKS.jsonl").read_text().splitlines())
        if r["status"] == "INDEPENDENT_ZIP_CHECKSUM_AND_CSV_MATCH"
    }
    bars = oracle.load_bars("ETHUSDT", "1h", manifest, pathlib.Path(cache), proofs)
    prefix = oracle.S1Prefixes("ETHUSDT", bars)
    tested = 0
    for year in (2024, 2025):
        for offset in (0, 1, 4, 24, 720, 1500):
            t = oracle.timestamp(year, 3, 1) + offset * oracle.HOUR
            h1 = oracle.completed_prefix(bars, t, oracle.HOUR, oracle.timestamp(year, 1, 1))
            h4 = oracle.aggregate_h4(h1)

            def convert(b):
                return SimpleNamespace(
                    close=Decimal(b.prices[3]),
                    high=Decimal(b.prices[1]),
                    low=Decimal(b.prices[2]),
                    content_hash=b.fingerprint,
                )

            join = SimpleNamespace(h1=tuple(map(convert, h1)), h4=tuple(map(convert, h4)))
            state = prefix.at(t)
            regime = _regime(join)
            setup = _setup(join)
            assert state["h4_trend"] == ("UP" if regime and regime.value == "LONG" else "DOWN" if regime else "NEUTRAL")
            assert (setup is not None) == (state["s1_precursor_state"] == "SETUP")
            if setup:
                assert (setup[0].value, str(setup[1])) == (state["s1_side"], state["extreme"])
            tested += 1
    assert tested == 12


def test_pinned_archive_and_cohort_identity():
    for name, sha in oracle.PINS.items():
        if name != "DIAGNOSTIC_FEATURE_PANEL.csv.gz":
            assert oracle.sha_file(RESEARCH / name) == sha
    with gzip.open(RESEARCH / "COMPACT_REAL_MULTI_SYMBOL_SAMPLE.csv.gz", "rt") as f:
        assert sum(1 for _ in f) == 6721


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_strict_trigger_equality_is_no_match(side):
    m = oracle.M15
    close = "102" if side == "LONG" else "98"
    state, _, raw = oracle.observed_predicate(origin(side), m, {0: bar(0), m: bar(m, close)})
    assert state == "NO_TRIGGER" and raw == 0


def test_rank_tie_key_matches_declared_canonical_instrument_key():
    import build_research_panel as subject

    symbols = json.loads((RESEARCH / "DATASET_MANIFEST.json").read_text())["cohort_symbols"]
    expected = sorted(symbols, key=lambda s: subject.key_for(s).to_canonical_json())
    actual = list(oracle.rank_scores(dict.fromkeys(reversed(symbols), 1.0)))
    assert actual == expected


def test_real_all_s6_720_input_windows_and_latest_four(tmp_path):
    """Producer is the subject here; the oracle module never imports it."""
    import bisect

    import build_research_panel as subject

    cache = os.environ.get("ATLAS_SOL_CSV_CACHE")
    if not cache:
        pytest.skip("external checksum-backed CSV cache not provided")
    manifest = [json.loads(line) for line in (RESEARCH / "SOURCE_MANIFEST.jsonl").read_text().splitlines()]
    proofs = {
        r["url"]: r
        for r in map(json.loads, (RESEARCH / "SOL_SOURCE_CHECKS.jsonl").read_text().splitlines())
        if r["status"] == "INDEPENDENT_ZIP_CHECKSUM_AND_CSV_MATCH"
    }
    symbols = json.loads((RESEARCH / "DATASET_MANIFEST.json").read_text())["cohort_symbols"]
    all_bars = {s: oracle.load_bars(s, "1h", manifest, pathlib.Path(cache), proofs) for s in symbols}
    subject_bars = {
        s: [SimpleNamespace(close_at_ns=b.end, close=Decimal(b.prices[3])) for b in bars]
        for s, bars in all_bars.items()
    }
    aligned = subject.aligned_return_series(subject_bars)
    totals = {
        "complete_input_windows": 0,
        "incomplete_input_windows": 0,
        "return_values_compared": 0,
        "max_absolute_log_return_error": 0.0,
        "latest_four_values_compared": 0,
    }
    for year in (2024, 2025):
        first, begin, final = (oracle.timestamp(year, m, 1) for m in (1, 3, 7))
        grids = {}
        for s, bars in all_bars.items():
            prices = np.full((final - first) // oracle.HOUR + 1, np.nan)
            for b in bars:
                if first < b.end <= final:
                    prices[(b.end - first) // oracle.HOUR] = b.close
            grids[s] = np.log(prices[1:] / prices[:-1])
        for s in symbols:
            if s == "BTCUSDT":
                continue
            times, x, y, *_ = aligned[s]
            for t in range(begin, final, oracle.HOUR):
                i = (t - first) // oracle.HOUR
                a, b = grids[s][i - 720 : i], grids["BTCUSDT"][i - 720 : i]
                left, right = bisect.bisect_right(times, t - 720 * oracle.HOUR), bisect.bisect_right(times, t)
                subject_rankable = right - left == 720 and all(
                    v - u == oracle.HOUR for u, v in zip(times[left:right], times[left + 1 : right], strict=False)
                )
                independent_rankable = bool(np.isfinite(a).all() and np.isfinite(b).all())
                assert subject_rankable == independent_rankable
                totals["complete_input_windows" if independent_rankable else "incomplete_input_windows"] += 1
                if not independent_rankable:
                    continue
                for actual, expected in ((np.array(x[left:right]), a), (np.array(y[left:right]), b)):
                    error = float(np.max(np.abs(actual - expected)))
                    assert error <= 1e-14
                    totals["max_absolute_log_return_error"] = max(totals["max_absolute_log_return_error"], error)
                    totals["return_values_compared"] += 720
                    totals["latest_four_values_compared"] += 4
                    assert np.allclose(actual[-4:], expected[-4:], rtol=0, atol=1e-14)
    assert totals["complete_input_windows"] + totals["incomplete_input_windows"] == 5856 * 34
    target = os.environ.get("ATLAS_SOL_INPUT_RESULTS")
    if target:
        pathlib.Path(target).write_text(json.dumps(totals, sort_keys=True, indent=2) + "\n")
