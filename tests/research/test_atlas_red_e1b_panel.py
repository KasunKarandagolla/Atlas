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

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "docs/research/atlas-red-e1b"))

import acquire_archives as archives  # noqa: E402
import build_research_panel as panel  # noqa: E402
from atlas.v2.strategies.s6_cross_section import _four_hour_context, _hourly_returns  # noqa: E402


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
    row = lambda t, p: [str(t), str(p), str(p + 1), str(p - 1), str(p), "10", str(t + 3_599_999), "100", "2", "5", "50", "0"]
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

    key = panel.key_for("BTCUSDT")
    btc_rows = _hourly_returns(btc, key=key, cutoff_ns=cutoff, replay_view="RECONSTRUCTED_MARKET")
    asset_rows = _hourly_returns(asset, key=panel.key_for("ETHUSDT"), cutoff_ns=cutoff, replay_view="RECONSTRUCTED_MARKET")
    end, begin = cutoff - cutoff % panel.HOUR, cutoff - cutoff % panel.HOUR - 30 * panel.DAY
    times = sorted(t for t in btc_rows.keys() & asset_rows.keys() if begin < t <= end)
    x = [asset_rows[t][0] for t in times]
    y = [btc_rows[t][0] for t in times]
    assert len(times) == 720
    beta = covariance(x, y) / (stdev(y) ** 2)
    residuals = [a - beta * b for a, b in zip(x, y, strict=True)]
    direct_score = sum(residuals[-4:]) / stdev(residuals)
    assert abs(float(scores["ETHUSDT"]["score"]) - direct_score) < 1e-8
    expected_trend, _ = _four_hour_context(h4["ETHUSDT"], panel.key_for("ETHUSDT"), cutoff, "RECONSTRUCTED_MARKET")
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
