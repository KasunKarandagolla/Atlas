#!/usr/bin/env python3
"""Regenerate R-E1B checksum-backed Binance cohort diagnostics.

The command downloads official monthly USD-M klines anonymously, verifies each
published sibling checksum, then emits only compressed compact diagnostics and
manifests. It never computes forward labels or outcomes.
"""
from __future__ import annotations

import bisect
import csv
import gzip
import hashlib
import io
import json
import math
import pathlib
import sys
import contextlib
from collections import defaultdict
from decimal import Decimal

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parents[2]
sys.path.insert(0, str(REPO / "src"))
from atlas.v2.contracts import V2Side  # noqa: E402
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2  # noqa: E402
from atlas.v2.data.raw import AvailabilityClassV2, RawObservationV2  # noqa: E402
from atlas.v2.features.joins import JoinedBars  # noqa: E402
from atlas.v2.features.technical import ema  # noqa: E402
from atlas.v2.instruments import EnvironmentV2, InstrumentKeyV2, ProductTypeV2, VenueV2  # noqa: E402
from atlas.v2.strategies.s1_trend import S1_POLICY, _regime, _setup  # noqa: E402
from atlas.v2.strategies.s6_cross_section import S6_POLICY  # noqa: E402

from acquire_archives import COHORT, MONTHS, COHORT_SHA256, run  # noqa: E402

HOUR = BarIntervalV2.H1.duration_ns
M15 = BarIntervalV2.M15.duration_ns
H4 = BarIntervalV2.H4.duration_ns
DAY = 86_400_000_000_000
WINDOWS = (("A", 2024, 3, 1, 2024, 6, 30), ("B", 2025, 3, 1, 2025, 6, 30))
INTERVALS = {"1h": BarIntervalV2.H1, "15m": BarIntervalV2.M15}
SIX_POLICY_ID = "S6_CROSS_SECTIONAL_RELATIVE_STRENGTH"
SIX_POLICY_VERSION = "1.0.0-shadow-research"
SIX_POLICY_HASH = S6_POLICY.policy_hash
BAR_INDEX: dict[int, list[int]] = {}


def ns(y: int, m: int, d: int) -> int:
    import datetime as dt
    value = dt.datetime(y, m, 1, tzinfo=dt.UTC) + dt.timedelta(days=d - 1)
    return int(value.timestamp() * 1e9)


def decision_cutoffs(begin: int, final: int) -> range:
    """Decision times are interval closes, including the first UTC boundary."""
    return range(begin, final, M15)


def native_slot_opens(begin: int, final: int, duration: int) -> range:
    """Native slots whose close/decision cutoff falls inside [begin, final)."""
    return range(begin - duration, final - duration, duration)


@contextlib.contextmanager
def deterministic_gzip_text(path: pathlib.Path):
    raw = path.open("wb")
    compressed = gzip.GzipFile(filename="", mode="wb", compresslevel=9, fileobj=raw, mtime=0)
    text = io.TextIOWrapper(compressed, encoding="utf-8", newline="")
    try:
        yield text
    finally:
        text.close()
        raw.close()


def parse_raw(scratch: pathlib.Path, manifests: list[dict[str, object]]) -> dict[str, dict[str, list[dict[str, object]]]]:
    source: dict[str, dict[str, list[dict[str, object]]]] = defaultdict(lambda: defaultdict(list))
    by_name = {(r.get("symbol"), r.get("interval"), r.get("month")): r for r in manifests}
    for row in manifests:
        if row.get("status") != "VERIFIED":
            continue
        path = scratch / str(row["csv_scratch"])
        with path.open(encoding="utf-8", newline="") as f:
            for row_index, fields in enumerate(csv.reader(f)):
                if row_index == 0 and fields and fields[0] == "open_time":
                    continue
                op = int(fields[0]) * 1_000_000
                close = int(fields[6]) * 1_000_000 + 1_000_000
                source[str(row["symbol"])][str(row["interval"])].append({
                    "open_ns": op, "close_ns": close, "open": fields[1], "high": fields[2],
                    "low": fields[3], "close": fields[4], "volume": fields[5],
                    "row_index": row_index - (1 if fields[0] == "open_time" else 0),
                    "month": row["month"], "archive_sha256": row["downloaded_sha256"],
                    "archive_url": row["url"], "archive_bytes": row["archive_bytes"],
                })
    for symbol in source:
        for interval in source[symbol]:
            source[symbol][interval].sort(key=lambda x: int(x["open_ns"]))
            opens = [int(x["open_ns"]) for x in source[symbol][interval]]
            if len(opens) != len(set(opens)):
                raise ValueError(f"CROSS_ARCHIVE_DUPLICATE_SLOT:{symbol}:{interval}")
    return source


def key_for(symbol: str) -> InstrumentKeyV2:
    revision = hashlib.sha256(("BINANCE_UM_RESEARCH_UNKNOWN_REVISION:" + symbol).encode()).hexdigest()
    base = symbol.removesuffix("USDT")
    return InstrumentKeyV2(VenueV2.BINANCE, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
                           symbol, base, "USDT", "USDT", revision)


def make_bars(symbol: str, interval_name: str, rows: list[dict[str, object]]) -> list[CausalBarV2]:
    interval = INTERVALS[interval_name]
    result = []
    for row in rows:
        op, close = int(row["open_ns"]), int(row["close_ns"])
        payload = {k: row[k] for k in ("open", "high", "low", "close", "volume")}
        raw = RawObservationV2.build(
            instrument_revision=key_for(symbol).contract_revision, source_id="BINANCE_DATA_VISION_USDM_MONTHLY_KLINES",
            event_type=f"BAR_{interval.value}", event_at_ns=close, received_at_ns=close,
            ingested_at_ns=close, available_at_ns=close, replay_available_at_ns=close,
            availability_class=AvailabilityClassV2.RECONSTRUCTED_MARKET,
            translation_version="R-E1B_BINANCE_KLINES_12COL_V1", payload=payload)
        result.append(CausalBarV2(raw, interval, op, close, *(Decimal(str(row[k])) for k in ("open", "high", "low", "close", "volume")), True))
    BAR_INDEX[id(result)] = [bar.close_at_ns for bar in result]
    return result


def h4_bars_from_h1(symbol: str, h1: list[CausalBarV2]) -> list[CausalBarV2]:
    by_open = {b.open_at_ns: b for b in h1}
    opens = sorted(by_open)
    result = []
    groups = defaultdict(list)
    for bar in h1:
        groups[(bar.open_at_ns // H4) * H4].append(bar)
    for op, group in sorted(groups.items()):
        group.sort(key=lambda b: b.open_at_ns)
        if len(group) != 4 or [b.open_at_ns for b in group] != [op + i * HOUR for i in range(4)]:
            continue
        close = op + H4
        fields = {"open": str(group[0].open), "high": str(max(x.high for x in group)),
                  "low": str(min(x.low for x in group)), "close": str(group[-1].close),
                  "volume": str(sum((x.volume for x in group), Decimal(0)))}
        raw = RawObservationV2.build(
            instrument_revision=key_for(symbol).contract_revision, source_id="RECONSTRUCTED_FROM_BINANCE_USDM_H1",
            event_type="BAR_4H", event_at_ns=close, received_at_ns=close, ingested_at_ns=close,
            available_at_ns=close, replay_available_at_ns=close,
            availability_class=AvailabilityClassV2.RECONSTRUCTED_MARKET,
            translation_version="R-E1B_EXACT_UTC_H1_TO_H4_V1", payload=fields)
        result.append(CausalBarV2(raw, BarIntervalV2.H4, op, close,
                                  *(Decimal(fields[k]) for k in ("open", "high", "low", "close", "volume")), True))
    BAR_INDEX[id(result)] = [bar.close_at_ns for bar in result]
    return result


def contiguous_tail(bars: list[CausalBarV2], cutoff: int, duration: int, wanted: int) -> tuple[CausalBarV2, ...]:
    times = BAR_INDEX.get(id(bars))
    if times is None:
        times = [b.close_at_ns for b in bars]
        BAR_INDEX[id(bars)] = times
    end = bisect.bisect_right(times, cutoff)
    if end < wanted:
        return ()
    tail = bars[end - wanted:end]
    if any(b.open_at_ns != a.open_at_ns + duration for a, b in zip(tail, tail[1:])):
        return ()
    return tuple(tail)


def contiguous_prefix(bars: list[CausalBarV2], cutoff: int, duration: int, begin: int) -> tuple[CausalBarV2, ...]:
    times = BAR_INDEX.get(id(bars))
    if times is None:
        times = [b.close_at_ns for b in bars]
        BAR_INDEX[id(bars)] = times
    left = bisect.bisect_left(times, begin + duration)
    right = bisect.bisect_right(times, cutoff)
    if right <= left:
        return ()
    prefix_bars = bars[left:right]
    if prefix_bars[0].open_at_ns != begin or any(b.open_at_ns != a.open_at_ns + duration for a, b in zip(prefix_bars, prefix_bars[1:])):
        return ()
    return tuple(prefix_bars)


def gap_prefix(bars: list[CausalBarV2], duration: int) -> list[int]:
    counts = [0] * len(bars)
    for i in range(1, len(bars)):
        counts[i] = counts[i - 1] + int(bars[i].open_at_ns != bars[i - 1].open_at_ns + duration)
    return counts


def s1_cached_state(symbol: str, h1: list[CausalBarV2], h4: list[CausalBarV2], cutoff: int,
                    h1_ema20: list[float | None], h1_ema50: list[float | None],
                    h4_ema20: list[float | None], h4_ema50: list[float | None],
                    h1_gaps: list[int], h4_gaps: list[int]) -> dict[str, object]:
    """Pointwise equivalent of the repository S1 pure precursor on a verified prefix."""
    import datetime as dt
    year_begin = ns(dt.datetime.fromtimestamp(cutoff / 1e9, dt.UTC).year, 1, 1)
    h1_times = BAR_INDEX[id(h1)]
    h4_times = BAR_INDEX[id(h4)]
    h1_left = bisect.bisect_left(h1_times, year_begin + HOUR)
    h4_left = bisect.bisect_left(h4_times, year_begin + H4)
    h1_right = bisect.bisect_right(h1_times, cutoff)
    h4_right = bisect.bisect_right(h4_times, cutoff)
    if (h1_right - h1_left < 50 or h4_right - h4_left < 50
            or h1[h1_left].open_at_ns != year_begin or h4[h4_left].open_at_ns != year_begin
            or (h1_right - h1_left > 1 and h1_gaps[h1_right - 1] != h1_gaps[h1_left])
            or (h4_right - h4_left > 1 and h4_gaps[h4_right - 1] != h4_gaps[h4_left])):
        return {"state": "SOURCE_MISSING", "reason": "H1_OR_RECONSTRUCTED_H4_PREFIX_HAS_GAP_OR_INSUFFICIENT_WARMUP"}
    i1, i4 = h1_right - 1, h4_right - 1
    e20_4, e50_4 = h4_ema20[i4], h4_ema50[i4]
    if e20_4 is None or e50_4 is None:
        return {"state": "INSUFFICIENT_WARMUP_OR_GAP", "reason": "NEED_50_CONTIGUOUS_H1_AND_H4_BARS"}
    close4 = float(h4[i4].close)
    side = V2Side.LONG if close4 > e50_4 and e20_4 > e50_4 else V2Side.SHORT if close4 < e50_4 and e20_4 < e50_4 else None
    setup = None
    if side is not None and i1 >= 2:
        indices = range(i1 - 2, i1 + 1)
        e20, e50 = [h1_ema20[i] for i in indices], [h1_ema50[i] for i in indices]
        bars = h1[i1 - 2:i1 + 1]
        if all(value is not None for value in (*e20, *e50)):
            e20f = [float(value) for value in e20 if value is not None]
            e50f = [float(value) for value in e50 if value is not None]
            if side == V2Side.LONG:
                touched = any(float(bar.low) <= level for bar, level in zip(bars, e20f, strict=True))
                reclaimed = float(bars[-1].close) > e20f[-1] and float(bars[-1].close) > e50f[-1]
                extreme = min(bar.low for bar in bars)
            else:
                touched = any(float(bar.high) >= level for bar, level in zip(bars, e20f, strict=True))
                reclaimed = float(bars[-1].close) < e20f[-1] and float(bars[-1].close) < e50f[-1]
                extreme = max(bar.high for bar in bars)
            if touched and reclaimed:
                setup = (side, extreme, tuple(bar.content_hash for bar in bars))
    s1ref = hashlib.sha256(("|".join([b.content_hash for b in h1[max(0, i1 - 49):i1 + 1]] +
                                      [b.content_hash for b in h4[max(0, i4 - 49):i4 + 1]])).encode()).hexdigest()
    return {"state": "SETUP" if setup else "NO_SETUP", "regime": side.value if side else "NEUTRAL",
            "side": setup[0].value if setup else "", "setup_extreme": str(setup[1]) if setup else "",
            "s1_ref": s1ref, "h1_ema20": h1_ema20[i1], "h1_ema50": h1_ema50[i1],
            "h1_last_close": str(h1[i1].close), "h1_last_close_ns": h1[i1].close_at_ns,
            "h1_last_return": math.log(float(h1[i1].close) / float(h1[i1 - 1].close))
            if h1[i1].close_at_ns - h1[i1 - 1].close_at_ns == HOUR else None,
            "h4_trend": "UP" if side == V2Side.LONG else "DOWN" if side == V2Side.SHORT else "NEUTRAL"}


def returns(bars: list[CausalBarV2]) -> tuple[list[int], list[float]]:
    times, values = [], []
    for prev, cur in zip(bars, bars[1:]):
        if cur.close_at_ns - prev.close_at_ns == HOUR:
            times.append(cur.close_at_ns)
            values.append(math.log(float(cur.close) / float(prev.close)))
    return times, values


def prefix(values: list[float]) -> tuple[list[float], list[float]]:
    sums, squares = [0.0], [0.0]
    for value in values:
        sums.append(sums[-1] + value)
        squares.append(squares[-1] + value * value)
    return sums, squares


def aligned_return_series(hourly: dict[str, list[CausalBarV2]]) -> dict[str, tuple[list[int], list[float], list[float], list[float], list[float], list[float], list[float], list[float]]]:
    btc_times, btc_values = returns(hourly.get("BTCUSDT", []))
    btc_by_t = dict(zip(btc_times, btc_values, strict=True))
    result = {}
    for symbol, bars in hourly.items():
        if symbol == "BTCUSDT":
            continue
        times, values = returns(bars)
        by_t = dict(zip(times, values, strict=True))
        common = sorted(btc_by_t.keys() & by_t.keys())
        x, y = [by_t[t] for t in common], [btc_by_t[t] for t in common]
        sx, x2 = prefix(x); sy, y2 = prefix(y)
        xy = [0.0]
        for a, b in zip(x, y, strict=True): xy.append(xy[-1] + a * b)
        result[symbol] = (common, x, y, sx, x2, sy, y2, xy)
    return result


def s6_scores(keys: list[str], h4s: dict[str, list[CausalBarV2]], aligned: dict[str, tuple[list[int], list[float], list[float], list[float], list[float], list[float], list[float], list[float]]], cutoff: int) -> tuple[dict[str, dict[str, object]], int, str]:
    end = cutoff - cutoff % HOUR
    start = end - 30 * DAY
    out: dict[str, dict[str, object]] = {}
    out["BTCUSDT"] = {"state": "BTC_REFERENCE_ANCHOR", "reason": "BTC_PROXY_EXCLUDED_FROM_PEER_RANK"}
    scored = []
    for symbol in keys:
        if symbol == "BTCUSDT":
            continue
        common, xall, yall, sxall, x2all, syall, y2all, xyall = aligned.get(symbol, ([], [], [], [], [], [], [], []))
        left, right = bisect.bisect_right(common, start), bisect.bisect_right(common, end)
        if right - left != 720 or any(b - a != HOUR for a, b in zip(common[left:right], common[left + 1:right])):
            out[symbol] = {"state": "NOT_RANKABLE", "reason": "NEED_720_CONTIGUOUS_MATCHED_H1_RETURNS"}
            continue
        x, y = xall[left:right], yall[left:right]
        sx, x2 = sxall[right] - sxall[left], x2all[right] - x2all[left]
        sy, y2 = syall[right] - syall[left], y2all[right] - y2all[left]
        xy = xyall[right] - xyall[left]
        n = 720
        mean_x, mean_y = sx / n, sy / n
        variance_y = (y2 - n * mean_y * mean_y) / (n - 1)
        covariance = (xy - sx * sy / n) / (n - 1)
        if variance_y <= 0 or not math.isfinite(variance_y):
            out[symbol] = {"state": "NOT_RANKABLE", "reason": "BTC_PROXY_ZERO_OR_INVALID_VARIANCE"}
            continue
        beta = covariance / variance_y
        residual_var = (x2 + beta * beta * y2 - 2 * beta * xy - n * (mean_x - beta * mean_y) ** 2) / (n - 1)
        volatility = math.sqrt(max(0.0, residual_var))
        if volatility <= 0 or not math.isfinite(volatility):
            out[symbol] = {"state": "NOT_RANKABLE", "reason": "RESIDUAL_VOLATILITY_ZERO_OR_INVALID"}
            continue
        latest = [x[-i] - beta * y[-i] for i in (4, 3, 2, 1)]
        score = math.fsum(latest) / volatility
        h4_tail = contiguous_tail(h4s.get(symbol, []), end, H4, 50)
        trend = "UNKNOWN"
        if h4_tail:
            closes = [float(b.close) for b in h4_tail]
            e20, e50 = ema(closes, 20)[-1], ema(closes, 50)[-1]
            if closes[-1] > e50 and e20 > e50:
                trend = "UP"
            elif closes[-1] < e50 and e20 < e50:
                trend = "DOWN"
            else:
                trend = "NEUTRAL"
        out[symbol] = {"state": "S6_FIXED_COHORT_RANK_DIAGNOSTIC", "beta_btc": beta,
                       "residual_volatility": volatility, "latest_four_residual_sum": math.fsum(latest),
                       "score": score, "own_4h_trend": trend,
                       "h1_ref_start": common[left], "h1_ref_end": common[right - 1]}
        scored.append((symbol, score))
    ranked = sorted(scored, key=lambda item: (-item[1], key_for(item[0]).to_canonical_json()))
    decile_size = max(1, math.ceil(len(ranked) * 0.1)) if ranked else 0
    for i, (symbol, _) in enumerate(ranked, start=1):
        out[symbol]["rank"] = i
        out[symbol]["decile"] = "TOP" if i <= decile_size else "BOTTOM" if i > len(ranked) - decile_size else "MIDDLE"
    breadth_reason = "OBSERVED_FIXED_COHORT_PRICE_SERIES_ONLY; EXCHANGE_ELIGIBILITY_UNVERIFIED"
    return out, len(ranked), breadth_reason


def read_manifest(path: pathlib.Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def write_manifest(outdir: pathlib.Path, manifests: list[dict[str, object]], rejected_path: pathlib.Path | None,
                   scratch: pathlib.Path) -> None:
    path = outdir / "SOURCE_MANIFEST.jsonl"
    with path.open("w", encoding="utf-8") as f:
        sample = {"record_type": "PRIOR_VERIFIED_SAMPLE", "status": "RETAINED_FROM_E1",
                  "path": "docs/research/atlas-red-e1/SAMPLE_TRACE.json",
                  "sha256": "e93d020d0e49fd523a9036c46a800265f2c2dc78886523876dc54347c3803a02",
                  "rows": 24, "source_archive_sha256": "09a24635f090c9b2ab17f4a61aa3c4d262a7ccc88e96ff0199ce66a54a8ab66b"}
        f.write(json.dumps(sample, sort_keys=True, separators=(",", ":")) + "\n")
        if rejected_path and rejected_path.exists():
            for row in read_manifest(rejected_path):
                row.update({"status": "PARSER_REJECTED_PRECEDING_ATTEMPT", "attempt": 1,
                            "downloaded_bytes": "NOT_CAPTURED_BY_REJECTED_PARSER",
                            "observed_csv_header": ["open_time", "open", "high", "low", "close", "volume", "close_time",
                                                    "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume", "ignore"],
                            "reason": "Initial parser rejected the CSV header before checksum/byte metadata was recorded; later fetch of the same URL checksum-verified the exact observed 12-column Binance USD-M header recorded here."})
                f.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
        for row in manifests:
            row["attempt"] = 2 if rejected_path and row.get("interval") == "1h" else 1
            if row.get("status") == "VERIFIED":
                csv_path = scratch / str(row["csv_scratch"])
                import datetime as dt
                row["csv_extraction_completed_at_utc"] = dt.datetime.fromtimestamp(csv_path.stat().st_mtime, dt.UTC).isoformat()
                row["http_download_completed_at_utc"] = "NOT_CAPTURED_SEPARATELY; extraction follows successful HTTP download and validation"
                row["timestamp_unit"] = "milliseconds UTC epoch"
                row["timestamp_resolution"] = "1 millisecond source; normalized bar boundaries are exact UTC intervals"
            f.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")


def slot_calendar(outdir: pathlib.Path, source: dict[str, dict[str, list[dict[str, object]]]], manifest: list[dict[str, object]]) -> None:
    archive_status = {(r["symbol"], r["interval"], r["month"]): r for r in manifest}
    observed = {}
    for symbol in COHORT:
        for interval in ("1h", "15m"):
            observed[(symbol, interval)] = {int(r["open_ns"]): r for r in source.get(symbol, {}).get(interval, [])}
    path = outdir / "ASOF_COVERAGE.csv.gz"
    fields = ("window", "symbol", "interval", "slot_open_utc_ns", "decision_cutoff_utc_ns", "expected_status", "reason",
              "archive_url", "archive_sha256", "archive_member_row_index", "source_timestamp_grade")
    with deterministic_gzip_text(path) as gz:
        w = csv.DictWriter(gz, fieldnames=fields)
        w.writeheader()
        for label, sy, sm, sd, ey, em, ed in WINDOWS:
            begin, final = ns(sy, sm, sd), ns(ey, em, ed + 1)
            for symbol in COHORT:
                for interval, duration in (("1h", HOUR), ("15m", M15)):
                    # The window is a decision-cutoff calendar, so each expected
                    # closed bar opens one native interval before its cutoff.
                    bars = observed[(symbol, interval)]
                    for op in native_slot_opens(begin, final, duration):
                        cutoff = op + duration
                        row = bars.get(op)
                        if row:
                            status, reason = "OBSERVED", ""
                            url, sha, index = row["archive_url"], row["archive_sha256"], row["row_index"]
                        else:
                            month = __import__("datetime").datetime.fromtimestamp(op / 1e9, __import__("datetime").UTC).strftime("%Y-%m")
                            arc = archive_status.get((symbol, interval, month), {})
                            status = "SOURCE_MISSING"
                            reason = ("OFFICIAL_ARCHIVE_404" if arc.get("status") == "MISSING_OBJECT_404" else
                                      "NATIVE_SLOT_GAP" if arc.get("status") == "VERIFIED" else "ARCHIVE_STATUS_UNAVAILABLE")
                            url, sha, index = arc.get("url", ""), "", ""
                        w.writerow({"window": label, "symbol": symbol, "interval": interval,
                                    "slot_open_utc_ns": op, "decision_cutoff_utc_ns": cutoff,
                                    "expected_status": status, "reason": reason,
                                    "archive_url": url, "archive_sha256": sha,
                                    "archive_member_row_index": index,
                                    "source_timestamp_grade": "RECONSTRUCTED_MARKET; original receipt unavailable" if row else "UNAVAILABLE"})


def build(outdir: pathlib.Path, scratch: pathlib.Path, manifests: list[dict[str, object]], rejected_path: pathlib.Path | None = None) -> dict[str, object]:
    outdir.mkdir(parents=True, exist_ok=True)
    write_manifest(outdir, manifests, rejected_path, scratch)
    raw = parse_raw(scratch, manifests)
    source: dict[str, dict[str, list[dict[str, object]]]] = raw
    slot_calendar(outdir, source, manifests)
    bars_h1: dict[str, list[CausalBarV2]] = {}
    bars_m15: dict[str, list[CausalBarV2]] = {}
    bars_h4: dict[str, list[CausalBarV2]] = {}
    s1_emas: dict[str, tuple[list[float | None], list[float | None], list[float | None], list[float | None], list[int], list[int]]] = {}
    for symbol in COHORT:
        bars_h1[symbol] = make_bars(symbol, "1h", raw.get(symbol, {}).get("1h", []))
        bars_m15[symbol] = make_bars(symbol, "15m", raw.get(symbol, {}).get("15m", []))
        bars_h4[symbol] = h4_bars_from_h1(symbol, bars_h1[symbol])
        h1_closes = [float(b.close) for b in bars_h1[symbol]]
        h4_closes = [float(b.close) for b in bars_h4[symbol]]
        s1_emas[symbol] = (list(ema(h1_closes, 20)), list(ema(h1_closes, 50)),
                           list(ema(h4_closes, 20)), list(ema(h4_closes, 50)),
                           gap_prefix(bars_h1[symbol], HOUR), gap_prefix(bars_h4[symbol], H4))
    aligned = aligned_return_series(bars_h1)
    window_bounds = {label: (ns(sy, sm, sd), ns(ey, em, ed + 1)) for label, sy, sm, sd, ey, em, ed in WINDOWS}
    archive_status = {(r.get("symbol"), r.get("interval"), r.get("month")): r for r in manifests}

    def s1_states_at(hcut: int) -> dict[str, dict[str, object]]:
        states = {}
        for symbol in COHORT:
            h1e20, h1e50, h4e20, h4e50, h1gaps, h4gaps = s1_emas[symbol]
            states[symbol] = s1_cached_state(symbol, bars_h1[symbol], bars_h4[symbol], hcut,
                                             h1e20, h1e50, h4e20, h4e50, h1gaps, h4gaps)
        return states

    fields = ("window", "symbol", "decision_cutoff_utc_ns", "native_interval", "source_timestamp_grade",
              "source_status", "source_missing_reason", "h1_archive_sha256", "h1_row_index", "m15_archive_sha256", "m15_row_index",
              "h1_source_status", "h1_source_missing_reason",
              "h1_close", "h1_return", "h1_ema20", "h1_ema50", "h4_trend", "s1_precursor_state", "s1_setup_event",
              "s1_trigger_state", "s1_reason", "s1_side", "s1_setup_ref", "s6_state", "s6_score", "s6_rank",
              "s6_decile", "s6_beta_btc", "s6_residual_volatility", "s6_own_4h_trend", "s6_rankable_breadth",
              "s6_exclusion_reason", "btc_h1_return", "eth_h1_return", "btc_h4_trend", "eth_h4_trend",
              "eligibility_grade", "s1_diagnostic_grade", "s1_policy_id", "s1_policy_version", "s1_policy_hash",
              "s6_policy_id", "s6_policy_version", "s6_policy_hash", "s6_partial_method", "raw_source_refs")
    panel_path = outdir / "DIAGNOSTIC_FEATURE_PANEL.csv.gz"
    row_counts = defaultdict(int)
    source_maps = {}
    bar_maps = {}
    for symbol in COHORT:
        source_maps[symbol] = {interval: {int(r["close_ns"]): r for r in source.get(symbol, {}).get(interval, [])}
                               for interval in ("1h", "15m")}
        bar_maps[symbol] = {interval: {b.close_at_ns: b for b in bars}
                            for interval, bars in (("1h", bars_h1[symbol]), ("15m", bars_m15[symbol]))}
    with deterministic_gzip_text(panel_path) as gz:
        w = csv.DictWriter(gz, fieldnames=fields)
        w.writeheader()
        for label, (begin, final) in window_bounds.items():
            cached_hcut = None
            current_s1: dict[str, dict[str, object]] = {}
            previous_s1: dict[str, dict[str, object]] = {}
            current_s6: tuple[dict[str, dict[str, object]], int, str] = ({}, 0, "")
            for cutoff in decision_cutoffs(begin, final):
                hcut = cutoff - cutoff % HOUR
                if hcut != cached_hcut:
                    previous_s1 = current_s1 if cached_hcut is not None else s1_states_at(hcut - HOUR)
                    current_s1 = s1_states_at(hcut)
                    current_s6 = s6_scores(COHORT, bars_h4, aligned, hcut)
                    cached_hcut = hcut
                s6state, breadth, breadth_note = current_s6
                for symbol in COHORT:
                    m15ref = source_maps[symbol]["15m"].get(cutoff)
                    m15 = bar_maps[symbol]["15m"].get(cutoff) if m15ref else None
                    s1 = current_s1[symbol]
                    trigger_setup = previous_s1.get(symbol, s1) if cutoff == hcut else s1
                    s1_active = trigger_setup["state"] == "SETUP" and cutoff > (hcut - HOUR if cutoff == hcut else hcut) and cutoff <= (hcut - HOUR if cutoff == hcut else hcut) + 4 * M15
                    prev_m15 = None
                    if m15:
                        prev_row = source_maps[symbol]["15m"].get(cutoff - M15)
                        if prev_row:
                            prev_m15 = bar_maps[symbol]["15m"].get(cutoff - M15)
                    trigger = False
                    if s1_active and m15 and prev_m15:
                        trigger = (trigger_setup.get("side") == V2Side.LONG.value and m15.close > prev_m15.high) or (trigger_setup.get("side") == V2Side.SHORT.value and m15.close < prev_m15.low)
                    slot_ok = bool(m15)
                    score = s6state.get(symbol, {})
                    latest_h1_ns = s1.get("h1_last_close_ns")
                    h1ref = source_maps[symbol]["1h"].get(latest_h1_ns) if latest_h1_ns else None
                    import datetime as dt
                    h1_month = dt.datetime.fromtimestamp((hcut - HOUR) / 1e9, dt.UTC).strftime("%Y-%m")
                    h1_arc = archive_status.get((symbol, "1h", h1_month), {})
                    m15_month = dt.datetime.fromtimestamp((cutoff - M15) / 1e9, dt.UTC).strftime("%Y-%m")
                    m15_arc = archive_status.get((symbol, "15m", m15_month), {})
                    h1_missing = ("" if h1ref else "OFFICIAL_ARCHIVE_404" if h1_arc.get("status") == "MISSING_OBJECT_404"
                                  else "NATIVE_SLOT_GAP" if h1_arc.get("status") == "VERIFIED" else "ARCHIVE_STATUS_UNAVAILABLE")
                    m15_missing = ("" if m15ref else "OFFICIAL_ARCHIVE_404" if m15_arc.get("status") == "MISSING_OBJECT_404"
                                   else "NATIVE_SLOT_GAP" if m15_arc.get("status") == "VERIFIED" else "ARCHIVE_STATUS_UNAVAILABLE")
                    e20, e50 = s1.get("h1_ema20"), s1.get("h1_ema50")
                    row = {"window": label, "symbol": symbol, "decision_cutoff_utc_ns": cutoff,
                           "native_interval": "15m", "source_timestamp_grade": "RECONSTRUCTED_MARKET; no genuine historical receipt",
                           "source_status": "OBSERVED" if slot_ok else "SOURCE_MISSING",
                           "source_missing_reason": m15_missing,
                           "h1_source_status": "OBSERVED" if h1ref else "SOURCE_MISSING",
                           "h1_source_missing_reason": h1_missing,
                           "h1_archive_sha256": h1ref.get("archive_sha256", "") if h1ref else "",
                           "h1_row_index": h1ref.get("row_index", "") if h1ref else "",
                           "m15_archive_sha256": m15ref.get("archive_sha256", "") if m15ref else "",
                           "m15_row_index": m15ref.get("row_index", "") if m15ref else "",
                           "h1_close": s1.get("h1_last_close", ""),
                           "h1_return": s1.get("h1_last_return", "") if s1.get("h1_last_return") is not None else "",
                           "h1_ema20": e20 if e20 is not None else "", "h1_ema50": e50 if e50 is not None else "",
                           "h4_trend": s1.get("h4_trend", "UNAVAILABLE"),
                           "s1_precursor_state": s1["state"], "s1_setup_event": int(s1["state"] == "SETUP" and cutoff == hcut),
                           "s1_trigger_state": "SOURCE_MISSING" if not slot_ok else "BAR_TRIGGER" if trigger else "NO_TRIGGER" if s1_active else "NOT_IN_TRIGGER_WINDOW",
                           "s1_reason": s1.get("reason", ""), "s1_side": s1.get("side", ""), "s1_setup_ref": s1.get("s1_ref", ""),
                           "s1_diagnostic_grade": "S1_BAR_PRECURSOR_DIAGNOSTIC" if s1["state"] in ("SETUP", "NO_SETUP") else s1["state"],
                           "s6_state": score.get("state", "NOT_RANKABLE"), "s6_score": score.get("score", ""),
                           "s6_rank": score.get("rank", ""), "s6_decile": score.get("decile", ""),
                           "s6_beta_btc": score.get("beta_btc", ""), "s6_residual_volatility": score.get("residual_volatility", ""),
                           "s6_own_4h_trend": score.get("own_4h_trend", ""), "s6_rankable_breadth": breadth,
                           "s6_exclusion_reason": score.get("reason", breadth_note),
                           "btc_h1_return": current_s1.get("BTCUSDT", {}).get("h1_last_return") or "",
                           "eth_h1_return": current_s1.get("ETHUSDT", {}).get("h1_last_return") or "",
                           "btc_h4_trend": current_s1.get("BTCUSDT", {}).get("h4_trend", "UNAVAILABLE"),
                           "eth_h4_trend": current_s1.get("ETHUSDT", {}).get("h4_trend", "UNAVAILABLE"),
                           "eligibility_grade": "UNIVERSE_UNVERIFIED; fixed convenience cohort only",
                           "s1_policy_id": S1_POLICY.policy_id, "s1_policy_version": S1_POLICY.version,
                           "s1_policy_hash": S1_POLICY.policy_hash, "s6_policy_id": SIX_POLICY_ID,
                           "s6_policy_version": SIX_POLICY_VERSION, "s6_policy_hash": SIX_POLICY_HASH,
                           "s6_partial_method": "PRICE_ONLY_FIXED_COHORT_ADAPTER; funding, liquidity, historical listing/filter proof and actual execution absent",
                           "raw_source_refs": json.dumps({"h1": h1ref.get("archive_sha256") if h1ref else None,
                                                          "m15": m15ref.get("archive_sha256") if m15ref else None,
                                                          "s1_prefix": s1.get("s1_ref"), "s6_window_start_ns": score.get("h1_ref_start"),
                                                          "s6_window_end_ns": score.get("h1_ref_end")}, separators=(",", ":"))}
                    w.writerow(row)
                    row_counts[(label, "slots")] += 1
                    row_counts[(label, "s1_setup")] += int(row["s1_setup_event"])
                    row_counts[(label, "s1_trigger")] += int(trigger)
                    row_counts[(label, "s1_no_setup")] += int(s1["state"] == "NO_SETUP")
                    row_counts[(label, "s1_unavailable")] += int(s1["state"] == "INSUFFICIENT_WARMUP_OR_GAP")
                    row_counts[(label, "s6_rankable")] += int(score.get("state") == "S6_FIXED_COHORT_RANK_DIAGNOSTIC")
                    row_counts[(label, "s6_unavailable")] += int(score.get("state") != "S6_FIXED_COHORT_RANK_DIAGNOSTIC")
                    row_counts[(label, "missing_m15")] += int(not slot_ok)
                    if row_counts[(label, "slots")] % 10_000 == 0:
                        gz.flush()
                        print(json.dumps({"progress_window": label, "rows": row_counts[(label, "slots")],
                                          "missing_m15": row_counts[(label, "missing_m15")],
                                          "s1_setups": row_counts[(label, "s1_setup")],
                                          "s6_rankable": row_counts[(label, "s6_rankable")]}), file=sys.stderr, flush=True)
    return {"cohort_sha256": COHORT_SHA256, "panel_path": str(panel_path), "counts": {f"{a}:{b}": v for (a, b), v in row_counts.items()},
            "panel_sha256": hashlib.sha256(panel_path.read_bytes()).hexdigest(), "panel_bytes": panel_path.stat().st_size}


def main() -> None:
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--scratch", type=pathlib.Path, default=pathlib.Path("work/e1b-cache"))
    p.add_argument("--outdir", type=pathlib.Path, default=HERE)
    p.add_argument("--offline-manifests", nargs=2, type=pathlib.Path, help="verified H1 and M15 manifest JSONL, reuse cached raw CSVs")
    p.add_argument("--rejected-attempt", type=pathlib.Path)
    args = p.parse_args()
    args.scratch.mkdir(parents=True, exist_ok=True)
    if args.offline_manifests:
        manifests = read_manifest(args.offline_manifests[0]) + read_manifest(args.offline_manifests[1])
    else:
        h1_months = MONTHS
        # February monthly files provide the exact prior 15m candle for March 1's first trigger check.
        m15_months = [m for m in MONTHS if m[5:] != "01"]
        manifests = run("1h", COHORT, h1_months, args.scratch) + run("15m", COHORT, m15_months, args.scratch)
        with (args.scratch / "all-manifests.jsonl").open("w", encoding="utf-8") as f:
            for row in manifests: f.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
    result = build(args.outdir, args.scratch, manifests, args.rejected_attempt)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
