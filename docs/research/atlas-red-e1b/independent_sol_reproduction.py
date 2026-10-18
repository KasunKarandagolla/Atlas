#!/usr/bin/env python3
"""Read-only independent E1B mathematics, source chronology and calendar audit.

Never imports the E1B producer, acquisition helpers, summarizer, or production
mathematics. Production PolicySpec identities alone are reused as contracts.
Primary S1 uses complete UTC year prefixes with independent EMA recurrence.
An explicitly secondary observed-history calculation diagnoses producer seeding
and stale-tail behavior. Neither calculation emits an action or an outcome.
"""

from __future__ import annotations

import argparse
import bisect
import collections
import contextlib
import csv
import gzip
import hashlib
import json
import math
import pathlib
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

import numpy as np

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT / "src"))
from atlas.v2.strategies.s1_trend import S1_POLICY  # noqa: E402 -- identity only

HOUR = 3_600_000_000_000
M15 = HOUR // 4
H4 = 4 * HOUR
SECOND = 1_000_000_000
REVIEWED = "d339d845a417e36b9a83c83b4f366e74fd207474"
PINS = {
    "DIAGNOSTIC_FEATURE_PANEL.csv.gz": "68ee994b983f47de86cec2781c9469f6e992d03a03728517978790b571c9d682",
    "ASOF_COVERAGE.csv.gz": "eb74a6eb0379524f07eec8c2462e21a3cfa78c5be66cd01972e8c735f8b45a61",
    "COMPACT_REAL_MULTI_SYMBOL_SAMPLE.csv.gz": "f678dce748d7f7ac4402067088ce9b970e6fe7b2ba02b4c607f31537b73e2608",
    "SOURCE_MANIFEST.jsonl": "c64a12c3aa5e4c500f0b431fe65c14fc07e45f18bc30100b6298f539f4c80d7b",
    "DATASET_MANIFEST.json": "41e4a5c8975610bae7a9e9c288ab4114e2981da19814a0df1871a8ab3b18221a",
}
COHORT_SHA = "b51f4a360e60bbb62fae982e4120ff3681b12a13ef8cd4a0aff01d761f6f03b5"
# Frozen before real comparisons. Centered S6 arithmetic avoids prefix-sum
# cancellation. Bounds cover float64 reduction/serialization, not a seed change.
TOLERANCES = {
    "s6_score": (1e-9, 1e-9),
    "s6_beta_btc": (1e-11, 1e-10),
    "s6_residual_volatility": (1e-12, 1e-10),
    "h1_ema20": (1e-10, 2e-12),
    "h1_ema50": (1e-10, 2e-12),
    "h1_return": (1e-14, 1e-12),
    "btc_h1_return": (1e-14, 1e-12),
    "eth_h1_return": (1e-14, 1e-12),
}


def timestamp(year: int, month: int, day: int) -> int:
    return int(datetime(year, month, day, tzinfo=UTC).timestamp()) * SECOND


def sha_file(path: pathlib.Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def hash_value(value: object) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


@dataclass(slots=True)
class Bar:
    end: int
    interval: str
    prices: tuple[str, str, str, str, str]
    source: str
    archive: str
    row: int
    fingerprint: str = field(init=False)

    def __post_init__(self) -> None:
        duration = {"1h": HOUR, "15m": M15, "4h": H4}[self.interval]
        self.fingerprint = hash_value(
            dict(
                interval={"1h": "1H", "15m": "15M", "4h": "4H"}[self.interval],
                open_at_ns=self.end - duration,
                close_at_ns=self.end,
                **dict(zip(("open", "high", "low", "close", "volume"), self.prices, strict=True)),
                source_row_id=self.source,
            )
        )

    @property
    def close(self) -> float:
        return float(self.prices[3])


def load_bars(
    symbol: str, interval: str, manifest: list[dict], cache: pathlib.Path, proofs: dict[str, dict]
) -> list[Bar]:
    result = []
    duration = HOUR if interval == "1h" else M15
    for item in manifest:
        if item.get("status") != "VERIFIED" or item["symbol"] != symbol or item["interval"] != interval:
            continue
        path = cache / item["csv_scratch"]
        if sha_file(path) != proofs[item["url"]]["csv_sha256"]:
            raise ValueError("SOURCE_CSV_PROOF_MISMATCH:" + path.name)
        with path.open(newline="") as handle:
            for index, columns in enumerate(csv.reader(handle)):
                if index == 0 and columns[0] == "open_time":
                    continue
                if len(columns) != 12:
                    raise ValueError("SOURCE_SCHEMA")
                start, end = int(columns[0]) * 1_000_000, (int(columns[6]) + 1) * 1_000_000
                if end - start != duration or start % duration:
                    raise ValueError("SOURCE_CLOCK")
                values = tuple(columns[1:6])
                numbers = tuple(Decimal(v) for v in values)
                if any(not n.is_finite() for n in numbers) or min(numbers[:4]) <= 0 or numbers[4] < 0:
                    raise ValueError("SOURCE_PRICE")
                if numbers[2] > min(numbers[0], numbers[3]) or numbers[1] < max(numbers[0], numbers[3]):
                    raise ValueError("SOURCE_OHLC")
                result.append(
                    Bar(end, interval, values, f"{item['downloaded_sha256']}:{index}", item["downloaded_sha256"], index)
                )
    result.sort(key=lambda b: b.end)
    if any(a.end >= b.end for a, b in zip(result, result[1:], strict=False)):
        raise ValueError("DUPLICATE_OR_OUT_OF_ORDER_SOURCE")
    return result


def aggregate_h4(hourly: list[Bar]) -> list[Bar]:
    by_end = {b.end: b for b in hourly}
    result = []
    for end in sorted(t for t in by_end if t % H4 == 0):
        group = [by_end.get(end - j * HOUR) for j in (3, 2, 1, 0)]
        if any(b is None for b in group):
            continue
        values = (
            group[0].prices[0],
            str(max(Decimal(b.prices[1]) for b in group)),
            str(min(Decimal(b.prices[2]) for b in group)),
            group[-1].prices[3],
            str(sum((Decimal(b.prices[4]) for b in group), Decimal(0))),
        )
        source = hashlib.sha256("|".join(b.source for b in group).encode()).hexdigest()
        result.append(Bar(end, "4h", values, source, "H4_FROM_FOUR_EXACT_H1", -1))
    return result


def independent_ema(values: list[float], period: int) -> list[float | None]:
    out: list[float | None] = [None] * len(values)
    if len(values) >= period:
        state = math.fsum(values[:period]) / period
        out[period - 1] = state
        alpha = 2.0 / (period + 1)
        for i in range(period, len(values)):
            state = state + alpha * (values[i] - state)
            out[i] = state
    return out


def completed_prefix(
    bars: list[Bar], cutoff: int, duration: int, begin: int, *, require_current: bool = True
) -> list[Bar]:
    if cutoff < begin:
        return []
    selected = [b for b in bars if begin < b.end <= cutoff]
    end = cutoff - cutoff % duration
    if (
        not selected
        or selected[0].end != begin + duration
        or any(b.end - a.end != duration for a, b in zip(selected, selected[1:], strict=False))
        or (require_current and selected[-1].end != end)
    ):
        return []
    return selected


class S1Prefixes:
    def __init__(self, symbol: str, hourly: list[Bar]):
        self.symbol, self.h1, self.h4 = symbol, hourly, aggregate_h4(hourly)
        self.times = {1: [b.end for b in self.h1], 4: [b.end for b in self.h4]}
        self.by_end = {b.end: b for b in hourly}
        self.ema = {}
        self.gaps = {}
        for interval, bars in ((1, self.h1), (4, self.h4)):
            gaps = [0]
            for a, b in zip(bars, bars[1:], strict=False):
                gaps.append(gaps[-1] + int(b.end - a.end != interval * HOUR))
            self.gaps[interval] = gaps if bars else []
            values = [b.close for b in bars]
            for period in (20, 50):
                self.ema[interval, period, "observed"] = independent_ema(values, period)
                fresh = [None] * len(bars)
                for year in (2024, 2025):
                    positions = [
                        i for i, b in enumerate(bars) if timestamp(year, 1, 1) < b.end <= timestamp(year, 7, 1)
                    ]
                    vals = independent_ema([values[i] for i in positions], period)
                    for i, v in zip(positions, vals, strict=True):
                        fresh[i] = v
                self.ema[interval, period, "primary"] = fresh
        self.cache = {}

    def at(self, cutoff: int, mode: str = "primary") -> dict:
        key = (cutoff, mode)
        if key in self.cache:
            return self.cache[key]
        year = datetime.fromtimestamp(cutoff // SECOND, UTC).year
        begin = timestamp(year, 1, 1)
        indices = {}
        valid = True
        for interval, bars in ((1, self.h1), (4, self.h4)):
            times = self.times[interval]
            left = bisect.bisect_left(times, begin + interval * HOUR)
            right = bisect.bisect_right(times, cutoff)
            indices[interval] = right - 1
            if (
                right - left < 50
                or bars[left].end != begin + interval * HOUR
                or self.gaps[interval][right - 1] != self.gaps[interval][left]
                or (mode == "primary" and bars[right - 1].end != cutoff - cutoff % (interval * HOUR))
            ):
                valid = False
        if not valid:
            result = {
                "s1_precursor_state": "SOURCE_MISSING",
                "s1_reason": "H1_OR_RECONSTRUCTED_H4_PREFIX_HAS_GAP_OR_INSUFFICIENT_WARMUP",
            }
        else:
            i, j = indices[1], indices[4]
            e20 = self.ema[4, 20, mode][j]
            e50 = self.ema[4, 50, mode][j]
            c = self.h4[j].close
            direction = "LONG" if c > e50 and e20 > e50 else "SHORT" if c < e50 and e20 < e50 else ""
            trio = self.h1[i - 2 : i + 1]
            ma20 = self.ema[1, 20, mode][i - 2 : i + 1]
            ma50 = self.ema[1, 50, mode][i - 2 : i + 1]
            touched = bool(direction) and any(
                (float(b.prices[2]) <= m if direction == "LONG" else float(b.prices[1]) >= m)
                for b, m in zip(trio, ma20, strict=True)
            )
            reclaim = bool(direction) and (
                trio[-1].close > max(ma20[-1], ma50[-1])
                if direction == "LONG"
                else trio[-1].close < min(ma20[-1], ma50[-1])
            )
            extreme = (
                str(
                    (min if direction == "LONG" else max)(
                        Decimal(b.prices[2 if direction == "LONG" else 1]) for b in trio
                    )
                )
                if touched and reclaim
                else ""
            )
            ref = hashlib.sha256(
                "|".join(b.fingerprint for b in self.h1[i - 49 : i + 1] + self.h4[j - 49 : j + 1]).encode()
            ).hexdigest()
            result = {
                "s1_precursor_state": "SETUP" if extreme else "NO_SETUP",
                "s1_reason": "",
                "s1_side": direction if extreme else "",
                "extreme": extreme,
                "s1_setup_ref": ref,
                "h1_close": self.h1[i].prices[3],
                "h1_ema20": ma20[-1],
                "h1_ema50": ma50[-1],
                "h4_trend": "UP" if direction == "LONG" else "DOWN" if direction == "SHORT" else "NEUTRAL",
                "h1_return": math.log(self.h1[i].close / self.h1[i - 1].close),
                "h1_bar": self.h1[i],
            }
        self.cache[key] = result
        return result

    def origins(self, year: int, mode: str) -> tuple[dict, dict]:
        origins, events = {}, {}
        prior = None
        for at in range(timestamp(year, 1, 1), timestamp(year, 7, 1), HOUR):
            state = self.at(at, mode)
            origin = None
            new = 0
            if state["s1_precursor_state"] == "SETUP":
                if prior and prior.side == state["s1_side"] and prior.extreme == state["extreme"]:
                    origin = prior
                else:
                    origin = Origin(self.symbol, at, state["s1_side"], state["extreme"], state["s1_setup_ref"])
                    new = 1
            origins[at] = origin
            events[at] = new
            prior = origin
        return origins, events


@dataclass(frozen=True, slots=True)
class Origin:
    symbol: str
    born: int
    side: str
    extreme: str
    ref: str
    identity: str = field(init=False)

    def __post_init__(self):
        payload = [
            self.symbol,
            S1_POLICY.policy_id,
            S1_POLICY.version,
            S1_POLICY.policy_hash,
            self.born,
            self.side,
            self.extreme,
            self.ref,
        ]
        object.__setattr__(
            self, "identity", hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest()
        )


def observed_predicate(origin: Origin, cutoff: int, by_end: dict[int, Bar]) -> tuple[str, str, int]:
    cur, prev = by_end.get(cutoff), by_end.get(cutoff - M15)
    if cur is None:
        return "SOURCE_MISSING", "CURRENT_M15_MISSING", 0
    if not origin or not origin.born < cutoff <= origin.born + HOUR:
        return "NOT_IN_TRIGGER_WINDOW", "", 0
    if prev is None:
        return "SOURCE_MISSING", "PREVIOUS_M15_MISSING_OR_NONCONTIGUOUS", 0
    match = (
        Decimal(cur.prices[3]) > Decimal(prev.prices[1])
        if origin.side == "LONG"
        else Decimal(cur.prices[3]) < Decimal(prev.prices[2])
    )
    return ("BAR_TRIGGER" if match else "NO_TRIGGER"), "", int(match)


class Episodes:
    def __init__(self, begin: int, bars: dict[int, Bar]):
        self.begin, self.bars, self.items = begin, bars, {}

    def get(self, origin: Origin) -> dict:
        if origin.identity not in self.items:
            ep = {
                "origin": origin,
                "pre_unknown": False,
                "active_unknown": False,
                "first": None,
                "before": False,
                "observed": {},
            }
            for cutoff in range(origin.born + M15, min(self.begin, origin.born + HOUR + M15), M15):
                state, _, raw = observed_predicate(origin, cutoff, self.bars)
                ep["observed"][cutoff] = None if state == "SOURCE_MISSING" else raw
                if state == "SOURCE_MISSING" and ep["first"] is None:
                    ep["pre_unknown"] = True
                elif raw and ep["first"] is None and not ep["pre_unknown"]:
                    ep["first"] = cutoff
                    ep["before"] = True
            self.items[origin.identity] = ep
        return self.items[origin.identity]

    def observe(self, origin: Origin | None, cutoff: int) -> dict:
        state, missing, raw = observed_predicate(origin, cutoff, self.bars)
        active = bool(origin and origin.born < cutoff <= origin.born + HOUR)
        ep = self.get(origin) if origin else None
        first = False
        if ep:
            if active:
                ep["observed"][cutoff] = None if state == "SOURCE_MISSING" else raw
                if state == "SOURCE_MISSING" and ep["first"] is None:
                    ep["active_unknown"] = True
            if raw and ep["first"] is None and not ep["pre_unknown"] and not ep["active_unknown"]:
                first = True
                ep["first"] = cutoff
        if not ep:
            status = "NO_SETUP"
            reason = "NO_MATCH"
        elif first:
            status = "VERIFIED_FIRST_TRIGGER"
            reason = "FIRST_MATCH_FOR_SETUP"
        elif ep["first"] is not None:
            status = "TRIGGERED_BEFORE_WINDOW" if ep["before"] else "TRIGGER_ALREADY_ESTABLISHED"
            reason = "REPEATED_MATCH_FOR_ALREADY_TRIGGERED_SETUP" if raw else "FIRST_ALREADY_ESTABLISHED"
        elif ep["pre_unknown"] and ep["active_unknown"]:
            status = "CENSORED_PREWINDOW_AND_ACTIVE"
            reason = "PREWINDOW_AND_ACTIVE_HISTORY_CENSORED"
        elif ep["pre_unknown"]:
            status = "CENSORED_PREWINDOW"
            reason = "PRIOR_TRIGGER_HISTORY_MISSING"
        elif ep["active_unknown"]:
            status = "CENSORED_ACTIVE"
            reason = "ACTIVE_TRIGGER_HISTORY_CENSORED"
        else:
            status = "OBSERVED_NO_MATCH"
            reason = "NO_MATCH"
        return {
            "s1_trigger_state": state,
            "s1_trigger_source_missing_reason": missing,
            "s1_raw_trigger_predicate": raw,
            "s1_first_trigger_event": int(first),
            "s1_first_trigger_event_id": hashlib.sha256(f"{origin.identity}:{cutoff}".encode()).hexdigest()
            if first
            else "",
            "s1_first_trigger_reason": reason,
            "s1_trigger_episode_status": status,
            "s1_trigger_window_active": int(active),
            "s1_trigger_pre_window_history_status": (
                "TRIGGERED_BEFORE_WINDOW"
                if ep["before"]
                else "PRIOR_TRIGGER_HISTORY_MISSING"
                if ep["pre_unknown"]
                else "NO_PRIOR_MATCH"
            )
            if ep
            else "NO_SETUP",
        }

    def partition(self) -> dict:
        counts = collections.Counter()
        for ep in self.items.values():
            if ep["before"]:
                outcome = "triggered_before_window"
            elif ep["first"] is not None:
                outcome = "verified_first_in_window"
            elif ep["pre_unknown"] or ep["active_unknown"]:
                outcome = "censored"
            elif len(ep["observed"]) == 4 and all(v == 0 for v in ep["observed"].values()):
                outcome = "complete_observed_no_match"
            else:
                outcome = "incomplete_or_unresolved"
            counts[outcome] += 1
        return {"unique_setup_identities": len(self.items), "exclusive_partition": dict(sorted(counts.items()))}


def s1_row(prefix: S1Prefixes, mode: str, cutoff: int, origins: dict, events: dict, episodes: Episodes) -> dict:
    hour = cutoff - cutoff % HOUR
    state = prefix.at(hour, mode)
    current = origins.get(hour)
    owner = origins.get(hour - HOUR) if cutoff == hour else current
    event = bool(current and events[hour] and cutoff == hour)
    if event:
        episodes.get(current)
    expected = {
        k: state.get(k, "UNAVAILABLE" if k == "h4_trend" else "")
        for k in (
            "s1_precursor_state",
            "s1_reason",
            "s1_side",
            "s1_setup_ref",
            "h1_close",
            "h1_return",
            "h1_ema20",
            "h1_ema50",
            "h4_trend",
        )
    }
    for group, origin in (("s1_setup_event", current if event else None), ("s1_trigger_setup", owner)):
        for suffix, attribute in (
            ("id", "identity"),
            ("cutoff_utc_ns", "born"),
            ("side", "side"),
            ("ref", "ref"),
            ("extreme", "extreme"),
        ):
            expected[f"{group}_{suffix}"] = getattr(origin, attribute) if origin else ""
    expected["s1_setup_event"] = int(event)
    expected.update(episodes.observe(owner, cutoff))
    return expected


def s6_moments(x: np.ndarray, y: np.ndarray) -> tuple[float, float, float]:
    if len(x) != 720 or len(y) != 720 or not np.isfinite(x).all() or not np.isfinite(y).all():
        raise ValueError("INCOMPLETE_S6_WINDOW")
    dx, dy = x - x.mean(), y - y.mean()
    denominator = float(np.dot(dy, dy))
    if denominator <= 0:
        raise ValueError("BTC_PROXY_ZERO_OR_INVALID_VARIANCE")
    beta = float(np.dot(dx, dy) / denominator)
    residual = x - beta * y
    vol = float(np.std(residual, ddof=1))
    if vol <= 0:
        raise ValueError("RESIDUAL_VOLATILITY_ZERO_OR_INVALID")
    return beta, vol, math.fsum(float(v) for v in residual[-4:]) / vol


def rank_scores(scores: dict[str, float]) -> dict[str, tuple[int, str]]:
    ordered = sorted(scores, key=lambda s: (-scores[s], s.removesuffix("USDT")))
    n = max(1, math.ceil(len(ordered) / 10)) if ordered else 0
    return {
        s: (i, "TOP" if i <= n else "BOTTOM" if i > len(ordered) - n else "MIDDLE") for i, s in enumerate(ordered, 1)
    }


def s6_grid(cohort: list[str], manifest: list[dict], cache: pathlib.Path, proofs: dict) -> tuple[dict, dict]:
    grids = {}
    input_digest = hashlib.sha256()
    boundary = {}
    for label, year in (("A", 2024), ("B", 2025)):
        first, begin, final = timestamp(year, 1, 1), timestamp(year, 3, 1), timestamp(year, 7, 1)
        count = (final - first) // HOUR
        prices = {}
        h4s = {}
        for symbol in cohort:
            bars = load_bars(symbol, "1h", manifest, cache, proofs)
            grid = np.full(count + 1, np.nan)
            for b in bars:
                if first < b.end <= final:
                    grid[(b.end - first) // HOUR] = b.close
            prices[symbol] = grid
            h4s[symbol] = aggregate_h4(bars)
        h4_times = {s: [b.end for b in bars] for s, bars in h4s.items()}
        returns = {s: np.log(p[1:] / p[:-1]) for s, p in prices.items()}
        # index i in returns is the bar whose end is first+(i+1)*HOUR.
        for hour in range((begin - first) // HOUR, (final - first) // HOUR):
            cutoff = first + hour * HOUR
            rows = {}
            scored = {}
            y = returns["BTCUSDT"][hour - 720 : hour]
            for symbol in cohort:
                if symbol == "BTCUSDT":
                    rows[symbol] = {
                        "s6_state": "BTC_REFERENCE_ANCHOR",
                        "s6_exclusion_reason": "BTC_PROXY_EXCLUDED_FROM_PEER_RANK",
                    }
                    continue
                x = returns[symbol][hour - 720 : hour]
                row = {"s6_state": "NOT_RANKABLE", "s6_exclusion_reason": "NEED_720_CONTIGUOUS_MATCHED_H1_RETURNS"}
                if len(x) == 720 and np.isfinite(x).all() and np.isfinite(y).all():
                    try:
                        beta, vol, score = s6_moments(x, y)
                    except ValueError as exc:
                        row["s6_exclusion_reason"] = str(exc)
                    else:
                        right = bisect.bisect_right(h4_times[symbol], cutoff)
                        closed = h4s[symbol][max(0, right - 50) : right]
                        trend = "UNKNOWN"
                        if len(closed) == 50 and all(
                            b.end - a.end == H4 for a, b in zip(closed, closed[1:], strict=False)
                        ):
                            values = [b.close for b in closed]
                            e20 = independent_ema(values, 20)[-1]
                            e50 = math.fsum(values) / 50
                            trend = (
                                "UP"
                                if values[-1] > e50 and e20 > e50
                                else "DOWN"
                                if values[-1] < e50 and e20 < e50
                                else "NEUTRAL"
                            )
                        row = {
                            "s6_state": "S6_FIXED_COHORT_RANK_DIAGNOSTIC",
                            "s6_exclusion_reason": "OBSERVED_FIXED_COHORT_PRICE_SERIES_ONLY; EXCHANGE_ELIGIBILITY_UNVERIFIED",
                            "s6_beta_btc": beta,
                            "s6_residual_volatility": vol,
                            "s6_score": score,
                            "s6_own_4h_trend": trend,
                        }
                        scored[symbol] = score
                        input_digest.update(
                            label.encode()
                            + symbol.encode()
                            + str(cutoff).encode()
                            + x.astype("<f8").tobytes()
                            + y.astype("<f8").tobytes()
                        )
                rows[symbol] = row
            ranks = rank_scores(scored)
            for symbol, row in rows.items():
                for key in (
                    "s6_beta_btc",
                    "s6_residual_volatility",
                    "s6_score",
                    "s6_own_4h_trend",
                    "s6_rank",
                    "s6_decile",
                ):
                    row.setdefault(key, "")
                row["s6_rankable_breadth"] = len(scored)
                if symbol in ranks:
                    row["s6_rank"], row["s6_decile"] = ranks[symbol]
            grids[label, cutoff] = rows
        boundary[label] = {
            "first_decision_utc": datetime.fromtimestamp(begin // SECOND, UTC).isoformat(),
            "m15_seed_open_utc": datetime.fromtimestamp((begin - M15) // SECOND, UTC).isoformat(),
            "h1_seed_open_utc": datetime.fromtimestamp((begin - HOUR) // SECOND, UTC).isoformat(),
            "h4_seed_open_utc": datetime.fromtimestamp((begin - H4) // SECOND, UTC).isoformat(),
        }
    return grids, {"all_rankable_720_return_inputs_sha256": input_digest.hexdigest(), "boundary": boundary}


class Comparisons:
    def __init__(self):
        self.fields = collections.Counter()
        self.checked_rows = collections.Counter()
        self.checked_fields = collections.Counter()
        self.symbol_mismatches = collections.Counter()
        self.row_counts = collections.Counter()
        self.examples = []
        self.numeric = {}
        self.example_counts = collections.Counter()

    def compare(self, group: str, actual: dict, expected: dict, *, category: str = "") -> bool:
        bad = False
        self.checked_rows[group + ":" + actual["window"]] += 1
        for key, value in expected.items():
            self.checked_fields[group + ":" + actual["window"] + ":" + key] += 1
            got = actual.get(key, "")
            equal = str(got) == str(value)
            if key in TOLERANCES and value != "" and got != "":
                try:
                    x, y = float(got), float(value)
                    error = abs(x - y)
                    relative = error / max(abs(y), 1e-300)
                    stats = self.numeric.setdefault(
                        group + ":" + key, {"max_absolute_error": 0.0, "max_relative_error": 0.0, "comparisons": 0}
                    )
                    stats["comparisons"] += 1
                    stats["max_absolute_error"] = max(stats["max_absolute_error"], error)
                    stats["max_relative_error"] = max(stats["max_relative_error"], relative)
                    absolute, rtol = TOLERANCES[key]
                    equal = math.isfinite(x) and math.isfinite(y) and error <= absolute + rtol * abs(y)
                except (ValueError, OverflowError):
                    equal = False
            if equal:
                continue
            bad = True
            label = actual["window"]
            self.fields[group + ":" + label + ":" + key] += 1
            example_key = (group, label, key)
            if self.example_counts[example_key] < 2 and len(self.examples) < 500:
                self.example_counts[example_key] += 1
                self.examples.append(
                    {
                        "group": group,
                        "window": label,
                        "symbol": actual["symbol"],
                        "decision_cutoff_utc_ns": actual["decision_cutoff_utc_ns"],
                        "field": key,
                        "panel": got,
                        "independent": value,
                        "classification": category
                        or (
                            "missing_data_incomplete_terminal_prefix"
                            if group == "s1_primary" and actual["symbol"] == "EOSUSDT"
                            else "producer_mathematics_noncontiguous_ema_seed"
                            if group == "s1_primary" and actual["symbol"] == "FTMUSDT"
                            else "producer_mathematics_or_identity"
                            if group.startswith("s1")
                            else "numerical_or_ranking"
                        ),
                        "panel_h1_archive_sha256": actual.get("h1_archive_sha256", ""),
                        "panel_m15_archive_sha256": actual.get("m15_archive_sha256", ""),
                        "coverage_archive_sha256": actual.get("archive_sha256", ""),
                        "independent_anchor_source_refs": actual.get("independent_anchor_source_refs", {}),
                    }
                )
        if bad:
            self.row_counts[group + ":" + actual["window"]] += 1
            self.symbol_mismatches[group + ":" + actual["window"] + ":" + actual["symbol"]] += 1
        return bad

    def report(self):
        return {
            "field_mismatches": dict(sorted(self.fields.items())),
            "checked_rows": dict(sorted(self.checked_rows.items())),
            "checked_fields": dict(sorted(self.checked_fields.items())),
            "symbol_rows_with_mismatch": dict(sorted(self.symbol_mismatches.items())),
            "rows_with_mismatch": dict(sorted(self.row_counts.items())),
            "numerical_errors": dict(sorted(self.numeric.items())),
            "bounded_examples": len(self.examples),
        }


def replay_readiness(dependency_closes: list[int], lag: int) -> int:
    if not dependency_closes or lag not in (5, 30, 60):
        raise ValueError("REPLAY_DEPENDENCIES_OR_LAG")
    return max(dependency_closes) + (lag + 1) * SECOND


def panel_calendar_key(row: dict, cohort: list[str], index: int) -> int:
    if row["window"] not in ("A", "B"):
        raise ValueError("PANEL_UNKNOWN_WINDOW")
    year = 2024 if row["window"] == "A" else 2025
    begin, final = timestamp(year, 3, 1), timestamp(year, 7, 1)
    cutoff = int(row["decision_cutoff_utc_ns"])
    if (
        row["symbol"] != cohort[index % len(cohort)]
        or cutoff != begin + (index // len(cohort)) * M15
        or cutoff >= final
    ):
        raise ValueError("PANEL_CALENDAR_ORDER_OR_KEY")
    return cutoff


def main_run(
    panel: pathlib.Path, cache: pathlib.Path, proof_file: pathlib.Path, scratch: pathlib.Path, outdir: pathlib.Path
) -> dict:
    artifacts = {}
    for name, expected in PINS.items():
        path = panel if name == panel.name else HERE / name
        actual = sha_file(path)
        if actual != expected:
            raise ValueError("PINNED_ARTIFACT_MISMATCH:" + name)
        artifacts[name] = actual
    dataset = json.loads((HERE / "DATASET_MANIFEST.json").read_text())
    cohort = dataset["cohort_symbols"]
    if hashlib.sha256(("\n".join(cohort) + "\n").encode()).hexdigest() != COHORT_SHA:
        raise ValueError("COHORT_CHANGED")
    manifest = [json.loads(line) for line in (HERE / "SOURCE_MANIFEST.jsonl").read_text().splitlines()]
    proof_rows = [json.loads(line) for line in proof_file.read_text().splitlines()]
    proofs = {r["url"]: r for r in proof_rows if r["status"] == "INDEPENDENT_ZIP_CHECKSUM_AND_CSV_MATCH"}
    if len(proofs) != 757:
        raise ValueError("INDEPENDENT_SOURCE_PROOF_INCOMPLETE")
    scratch.mkdir(parents=True, exist_ok=True)
    outdir.mkdir(parents=True, exist_ok=True)
    s6, source_notes = s6_grid(cohort, manifest, cache, proofs)
    anchors = {s: S1Prefixes(s, load_bars(s, "1h", manifest, cache, proofs)) for s in ("BTCUSDT", "ETHUSDT")}
    comparisons = Comparisons()
    counts = collections.Counter()
    timing = collections.Counter()
    cross = collections.Counter()
    partitions = {}
    writers = {}
    key_hash = hashlib.sha256()
    panel_episodes = {}
    with contextlib.ExitStack() as stack, gzip.open(panel, "rt", newline="") as handle:
        reader = csv.DictReader(handle)
        fields = reader.fieldnames
        sample_handle = stack.enter_context(
            gzip.open(HERE / "COMPACT_REAL_MULTI_SYMBOL_SAMPLE.csv.gz", "rt", newline="")
        )
        sample_rows = iter(csv.DictReader(sample_handle))
        sample_matches = 0
        for symbol in cohort:
            h = stack.enter_context(gzip.open(scratch / (symbol + ".csv.gz"), "wt", compresslevel=1, newline=""))
            writers[symbol] = csv.DictWriter(h, fieldnames=fields)
            writers[symbol].writeheader()
        window_indices = collections.Counter()
        for row in reader:
            label = row["window"]
            year = 2024 if label == "A" else 2025
            begin = timestamp(year, 3, 1)
            i = window_indices[label]
            cutoff = panel_calendar_key(row, cohort, i)
            window_indices[label] += 1
            counts[label + ":panel_rows"] += 1
            if cutoff < begin + 24 * HOUR:
                if row != next(sample_rows):
                    raise ValueError("COMPACT_SAMPLE_FULL_PANEL_JOIN_MISMATCH")
                sample_matches += 1
            key_hash.update(f"{label}:{row['symbol']}:{cutoff}\n".encode())
            writers[row["symbol"]].writerow(row)
            expected = s6[label, cutoff - cutoff % HOUR][row["symbol"]]
            comparisons.compare("s6", row, expected)
            context = {}
            anchor_refs = {}
            for anchor, name in (("BTCUSDT", "btc"), ("ETHUSDT", "eth")):
                state = anchors[anchor].at(cutoff - cutoff % HOUR, "primary")
                context[name + "_h1_return"] = state.get("h1_return", "")
                context[name + "_h4_trend"] = state.get("h4_trend", "UNAVAILABLE")
                bar = state.get("h1_bar")
                anchor_refs[anchor] = (
                    {"archive_sha256": bar.archive, "member_row_index": bar.row, "close_ns": bar.end} if bar else {}
                )
            comparisons.compare(
                "anchor_context",
                {**row, "independent_anchor_source_refs": anchor_refs},
                context,
                category="producer_zero_return_serialization",
            )
            if cutoff % HOUR == 0:
                counts[label + ":s6_hourly_symbol_rows"] += 1
            counts[label + ":first_events"] += int(row["s1_first_trigger_event"])
            counts[label + ":raw_positive_predicates"] += int(row["s1_raw_trigger_predicate"])
            counts[label + ":missing_current_m15"] += int(row["source_status"] == "SOURCE_MISSING")
            for sid in (row["s1_setup_event_id"], row["s1_trigger_setup_id"]):
                if sid:
                    panel_episodes.setdefault(
                        (label, row["symbol"], sid),
                        {
                            "first": False,
                            "censored": False,
                            "before": False,
                            "opps": {},
                            "origin": int(
                                row["s1_trigger_setup_cutoff_utc_ns"]
                                if sid == row["s1_trigger_setup_id"]
                                else row["s1_setup_event_cutoff_utc_ns"]
                            ),
                            "prior_complete": False,
                        },
                    )
            sid = row["s1_trigger_setup_id"]
            if sid:
                ep = panel_episodes[label, row["symbol"], sid]
                ep["first"] |= row["s1_first_trigger_event"] == "1"
                ep["censored"] |= row["s1_trigger_episode_status"].startswith("CENSORED")
                ep["before"] |= row["s1_trigger_episode_status"] == "TRIGGERED_BEFORE_WINDOW"
                ep["prior_complete"] |= row["s1_trigger_pre_window_history_status"] == "NO_PRIOR_MATCH"
                if row["s1_trigger_window_active"] == "1":
                    ep["opps"][cutoff] = (
                        None if row["s1_trigger_state"] == "SOURCE_MISSING" else int(row["s1_raw_trigger_predicate"])
                    )
            if row["s1_first_trigger_event"] == "1":
                cross[label + ":" + row["s6_state"] + ":" + (row["s6_decile"] or "NONE")] += 1
            observed = row["source_status"] == "OBSERVED"
            timing[label + ":first_events_not_visible_at_exact_close"] += int(row["s1_first_trigger_event"])
            timing[label + ":observed_current_m15"] += int(observed)
            comparisons.compare(
                "chronology",
                row,
                {
                    "actual_receipt_status": "UNKNOWN_NOT_CAPTURED_AT_ACQUISITION",
                    "actual_receipt_at_utc_ns": "",
                    "m15_event_at_utc_ns": cutoff if observed else "",
                    "m15_replay_available_at_utc_ns": cutoff + 5 * SECOND if observed else "",
                    "feature_replay_available_at_utc_ns": cutoff + 6 * SECOND if observed else "",
                    "replay_visibility_at_cutoff": "NOT_VISIBLE_UNDER_CLOSE_PLUS_5S",
                },
                category="timing",
            )
            for lag in (5, 30, 60):
                timing[f"{label}:lag_{lag}:raw_unavailable_at_exact_close"] += int(observed)
                timing[f"{label}:lag_{lag}:derived_unavailable_at_raw_ready"] += int(observed)
        if next(sample_rows, None) is not None or sample_matches != 6720:
            raise ValueError("COMPACT_SAMPLE_DENOMINATOR")
    for label in ("A", "B"):
        if window_indices[label] != 409920:
            raise ValueError("PANEL_DENOMINATOR")
    for (label, symbol, _), ep in panel_episodes.items():
        key = label + ":" + symbol
        c = partitions.setdefault(key, collections.Counter())
        outcome = (
            "triggered_before_window"
            if ep["before"]
            else "verified_first_in_window"
            if ep["first"]
            else "censored"
            if ep["censored"]
            else "complete_observed_no_match"
            if ep["prior_complete"]
            and all(
                (t < timestamp(2024 if label == "A" else 2025, 3, 1)) or ep["opps"].get(t) == 0
                for t in (ep["origin"] + j * M15 for j in range(1, 5))
            )
            else "incomplete_or_unresolved"
        )
        c[outcome] += 1
    calendar_counts = collections.Counter()
    coverage_statuses = collections.Counter()
    coverage_hash = hashlib.sha256()
    archive_map = {
        (r["symbol"], r["interval"], r["month"]): r
        for r in manifest
        if r.get("status") in ("VERIFIED", "MISSING_OBJECT_404")
    }
    with contextlib.ExitStack() as stack, gzip.open(HERE / "ASOF_COVERAGE.csv.gz", "rt", newline="") as handle:
        reader = csv.DictReader(handle)
        writers = {}
        for symbol in cohort:
            h = stack.enter_context(gzip.open(scratch / (symbol + ".coverage.gz"), "wt", compresslevel=1, newline=""))
            writers[symbol] = csv.DictWriter(h, fieldnames=reader.fieldnames)
            writers[symbol].writeheader()
        for row in reader:
            label, symbol, interval = row["window"], row["symbol"], row["interval"]
            if label not in ("A", "B") or symbol not in cohort or interval not in ("1h", "15m"):
                raise ValueError("COVERAGE_UNKNOWN_KEY")
            duration = HOUR if interval == "1h" else M15
            year = 2024 if label == "A" else 2025
            begin = timestamp(year, 3, 1)
            cutoff = int(row["decision_cutoff_utc_ns"])
            key = (label, symbol, interval)
            n = calendar_counts[key]
            if (
                cutoff != begin + n * duration
                or cutoff >= timestamp(year, 7, 1)
                or int(row["slot_open_utc_ns"]) != cutoff - duration
            ):
                raise ValueError("COVERAGE_CALENDAR")
            calendar_counts[key] += 1
            coverage_statuses[":".join((label, interval, row["expected_status"], row["reason"]))] += 1
            coverage_hash.update(canonical(row).encode())
            writers[symbol].writerow(row)
    if (
        len(calendar_counts) != 140
        or sum(calendar_counts.values()) != 1024800
        or any(n != (2928 if i == "1h" else 11712) for (_, _, i), n in calendar_counts.items())
    ):
        raise ValueError("COVERAGE_DENOMINATOR")
    primary, compat = {}, {}
    source_audit = collections.Counter()
    boundary_proofs = []
    discrepancy_context = []
    discrepancy_symbols = set()
    flat_zero_volume = collections.Counter()
    for symbol in cohort:
        h1 = load_bars(symbol, "1h", manifest, cache, proofs)
        m15 = load_bars(symbol, "15m", manifest, cache, proofs)
        m15map = {b.end: b for b in m15}
        h1map = {b.end: b for b in h1}
        with gzip.open(scratch / (symbol + ".coverage.gz"), "rt", newline="") as handle:
            for row in csv.DictReader(handle):
                interval = row["interval"]
                cutoff = int(row["decision_cutoff_utc_ns"])
                bar = (h1map if interval == "1h" else m15map).get(cutoff)
                month = datetime.fromtimestamp((cutoff - (HOUR if interval == "1h" else M15)) // SECOND, UTC).strftime(
                    "%Y-%m"
                )
                arc = archive_map.get((symbol, interval, month), {})
                reason = (
                    ""
                    if bar
                    else (
                        "OFFICIAL_ARCHIVE_404"
                        if arc.get("status") == "MISSING_OBJECT_404"
                        else "NATIVE_SLOT_GAP"
                        if arc.get("status") == "VERIFIED"
                        else "ARCHIVE_STATUS_UNAVAILABLE"
                    )
                )
                comparisons.compare(
                    "coverage_source_" + interval,
                    row,
                    {
                        "expected_status": "OBSERVED" if bar else "SOURCE_MISSING",
                        "reason": reason,
                        "archive_url": arc.get("url", ""),
                        "archive_sha256": bar.archive if bar else "",
                        "archive_member_row_index": bar.row if bar else "",
                        "source_timestamp_grade": "RECONSTRUCTED_MARKET; original receipt unavailable"
                        if bar
                        else "UNAVAILABLE",
                    },
                    category="source_identity_or_missing_data",
                )
        prefix = S1Prefixes(symbol, h1)
        for bar in h1:
            for label, year in (("A", 2024), ("B", 2025)):
                if timestamp(year, 3, 1) <= bar.end < timestamp(year, 7, 1):
                    if len({Decimal(v) for v in bar.prices[:4]}) == 1 and Decimal(bar.prices[4]) == 0:
                        flat_zero_volume[label + ":" + symbol] += 1
        series = {}
        ledgers = {}
        for label, year in (("A", 2024), ("B", 2025)):
            for mode in ("primary", "observed"):
                series[label, mode] = prefix.origins(year, mode)
                ledgers[label, mode] = Episodes(timestamp(year, 3, 1), m15map)
        with gzip.open(scratch / (symbol + ".csv.gz"), "rt", newline="") as handle:
            for row in csv.DictReader(handle):
                label = row["window"]
                cutoff = int(row["decision_cutoff_utc_ns"])
                hour = cutoff - cutoff % HOUR
                for mode in ("primary", "observed"):
                    origins, events = series[label, mode]
                    expected = s1_row(prefix, mode, cutoff, origins, events, ledgers[label, mode])
                    bad = comparisons.compare("s1_" + mode, row, expected)
                    if bad and mode == "primary" and symbol not in discrepancy_symbols:
                        discrepancy_symbols.add(symbol)
                        context = {"symbol": symbol, "window": label, "cutoff_ns": cutoff, "modes": {}}
                        for name in ("primary", "observed"):
                            details = {}
                            for interval, bars in ((1, prefix.h1), (4, prefix.h4)):
                                i = bisect.bisect_right(prefix.times[interval], hour) - 1
                                details[str(interval) + "h"] = {
                                    "latest_end_ns": bars[i].end,
                                    "close": bars[i].prices[3],
                                    "ema20": prefix.ema[interval, 20, name][i],
                                    "ema50": prefix.ema[interval, 50, name][i],
                                    "stable_source_row_id": bars[i].source,
                                    "archive_sha256": bars[i].archive,
                                }
                            context["modes"][name] = details
                        discrepancy_context.append(context)
                source = prefix.by_end.get(hour)

                def missing_reason(interval, end, duration, exists, symbol=symbol):
                    if exists:
                        return ""
                    month = datetime.fromtimestamp((end - duration) // SECOND, UTC).strftime("%Y-%m")
                    status = archive_map.get((symbol, interval, month), {}).get("status")
                    return (
                        "OFFICIAL_ARCHIVE_404"
                        if status == "MISSING_OBJECT_404"
                        else "NATIVE_SLOT_GAP"
                        if status == "VERIFIED"
                        else "ARCHIVE_STATUS_UNAVAILABLE"
                    )

                comparisons.compare(
                    "physical_h1_source",
                    row,
                    {
                        "h1_source_status": "OBSERVED" if source else "SOURCE_MISSING",
                        "h1_source_missing_reason": missing_reason("1h", hour, HOUR, source),
                        "h1_archive_sha256": source.archive if source else "",
                        "h1_row_index": source.row if source else "",
                    },
                    category="missing_data_or_stale_source_identity",
                )
                current = m15map.get(cutoff)
                comparisons.compare(
                    "physical_m15_source",
                    row,
                    {
                        "source_status": "OBSERVED" if current else "SOURCE_MISSING",
                        "source_missing_reason": missing_reason("15m", cutoff, M15, current),
                        "m15_archive_sha256": current.archive if current else "",
                        "m15_row_index": current.row if current else "",
                    },
                    category="source_identity",
                )
                valid = prefix.at(hour, "primary")["s1_precursor_state"] != "SOURCE_MISSING"
                source_audit[label + ":feature_ready_populated_with_incomplete_required_s1_prefix"] += int(
                    not valid and bool(row["feature_replay_available_at_utc_ns"])
                )
                if cutoff == timestamp(2024 if label == "A" else 2025, 3, 1):
                    boundary_proofs.append(
                        {
                            "window": label,
                            "symbol": symbol,
                            "m15_seed_source": current.source if current else None,
                            "h1_source": source.source if source else None,
                            "first_m15_native_open_ns": cutoff - M15,
                            "h1_end_ns": source.end if source else None,
                            "primary_s1_state": prefix.at(hour, "primary")["s1_precursor_state"],
                            "s6_price_state": s6[label, hour][symbol]["s6_state"],
                        }
                    )
        for label in ("A", "B"):
            primary[label + ":" + symbol] = ledgers[label, "primary"].partition()
            compat[label + ":" + symbol] = ledgers[label, "observed"].partition()
        print(json.dumps({"independent_symbol_completed": symbol}), flush=True)
    report = {
        "schema": "ATLAS_E1B_INDEPENDENT_SCIENCE_V1",
        "reviewed_e1b_sha": REVIEWED,
        "model_effort_attestation": "Owner-selected GPT-6.1 Sol; independent runtime identity/effort unavailable; no subagents",
        "pinned_artifacts": artifacts,
        "cohort_sha256": COHORT_SHA,
        "source_proof_sha256": sha_file(proof_file),
        "source_proof_objects": len(proof_rows),
        "source_archive_count": len(proofs),
        "tolerances_absolute_relative": TOLERANCES,
        "oracle_code_sha256": sha_file(pathlib.Path(__file__)),
        "counts": dict(sorted(counts.items())),
        "comparisons": comparisons.report(),
        "panel_key_sha256": key_hash.hexdigest(),
        "coverage_row_sha256": coverage_hash.hexdigest(),
        "coverage_rows": sum(calendar_counts.values()),
        "coverage_groups": len(calendar_counts),
        "coverage_status_counts": dict(sorted(coverage_statuses.items())),
        "panel_setup_partitions": {k: dict(sorted(v.items())) for k, v in sorted(partitions.items())},
        "primary_complete_year_prefix_setup_partitions": primary,
        "secondary_observed_history_setup_partitions": compat,
        "source_audit_counts": dict(sorted(source_audit.items())),
        "discrepancy_context": discrepancy_context,
        "flat_zero_volume_h1_counts": dict(sorted(flat_zero_volume.items())),
        "chronology_counts": dict(sorted(timing.items())),
        "first_event_s6_contemporaneous_crosstab": dict(sorted(cross.items())),
        "s6_input_checks": source_notes,
        "first_boundary_proofs": boundary_proofs,
        "economic_status": "NOT ESTIMABLE; no outcome labels, forward returns, costs, actions, fills or programme returns generated",
    }
    (outdir / "SOL_REPRODUCTION_RESULTS.json").write_text(
        json.dumps(report, sort_keys=True, indent=2, allow_nan=False) + "\n"
    )
    (outdir / "SOL_MISMATCH_EXAMPLES.jsonl").write_text("".join(canonical(r) + "\n" for r in comparisons.examples))
    return report


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--panel", type=pathlib.Path, required=True)
    p.add_argument("--csv-cache", type=pathlib.Path, required=True)
    p.add_argument("--source-proof", type=pathlib.Path, required=True)
    p.add_argument("--scratch", type=pathlib.Path, required=True)
    p.add_argument("--outdir", type=pathlib.Path, required=True)
    a = p.parse_args()
    r = main_run(a.panel, a.csv_cache, a.source_proof, a.scratch, a.outdir)
    print(canonical({"counts": r["counts"], "comparisons": r["comparisons"]}))
