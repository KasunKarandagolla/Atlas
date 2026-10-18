"""Cutoff-bounded source adapter and accepted finite-prefix S1/S6 mathematics.

Only immutable PrefixView and Snapshot values enter diagnostic decisions.
No decision function receives a Market, source path or outcome interface.
"""

from __future__ import annotations

import bisect
import csv
import functools
import json
import math
from dataclasses import dataclass, field
from decimal import Decimal
from types import MappingProxyType

import common

# common establishes the pinned repository reader path before these imports.
# isort: split
import build_research_panel as accepted
from independent_sol_reproduction import Episodes, Origin


@dataclass(frozen=True, slots=True)
class NativeBar:
    interval: object
    open_at_ns: int
    close_at_ns: int
    prices: tuple[str, ...]
    stable_source_row_id: str
    content_hash: str = field(init=False)

    def __post_init__(self):
        payload = dict(
            interval=self.interval.value,
            open_at_ns=self.open_at_ns,
            close_at_ns=self.close_at_ns,
            **dict(zip(("open", "high", "low", "close", "volume"), self.prices, strict=True)),
            source_row_id=self.stable_source_row_id,
        )
        object.__setattr__(self, "content_hash", common.digest(payload))

    @property
    def end(self):
        return self.close_at_ns

    @property
    def source(self):
        return self.stable_source_row_id

    @property
    def open(self):
        return Decimal(self.prices[0])

    @property
    def high(self):
        return Decimal(self.prices[1])

    @property
    def low(self):
        return Decimal(self.prices[2])

    @property
    def close(self):
        return Decimal(self.prices[3])

    @property
    def volume(self):
        return Decimal(self.prices[4])


@functools.lru_cache(maxsize=1)
def source_rows():
    prior = [json.loads(line) for line in (common.OLD / "SOURCE_MANIFEST.jsonl").read_text().splitlines()]
    future_path = common.HERE / "SOURCE_OBJECTS.jsonl"
    extra = [json.loads(line) for line in future_path.read_text().splitlines()] if future_path.exists() else []
    proofs = {
        r["url"]: r
        for r in map(json.loads, (common.OLD / "SOL_SOURCE_CHECKS.jsonl").read_text().splitlines())
        if "csv_sha256" in r
    }
    for r in prior:
        if r.get("status") == "VERIFIED":
            r["csv_sha256"] = proofs[r["url"]]["csv_sha256"]
    return [(r, "old") for r in prior] + [(r, "new") for r in extra]


def load_native(symbol, interval, year, months, old_cache, new_cache):
    result = []
    for r, location in source_rows():
        if (
            r.get("status") != "VERIFIED"
            or r["symbol"] != symbol
            or r["interval"] != interval
            or r["month"] not in [f"{year}-{m:02}" for m in months]
        ):
            continue
        path = (old_cache if location == "old" else new_cache) / r["csv_scratch"]
        if common.sha(path) != r["csv_sha256"]:
            raise ValueError("SOURCE_CSV_PROOF_MISMATCH")
        duration = common.HOUR if interval == "1h" else common.M15
        with path.open(newline="") as f:
            for index, row in enumerate(csv.reader(f)):
                if index == 0 and row[0] == "open_time":
                    continue
                begin, end = int(row[0]) * 1_000_000, (int(row[6]) + 1) * 1_000_000
                if end - begin != duration or begin % duration:
                    raise ValueError("SOURCE_NATIVE_CLOCK")
                result.append(
                    NativeBar(
                        accepted.INTERVALS[interval],
                        begin,
                        end,
                        tuple(row[1:6]),
                        f"{r['downloaded_sha256']}:{index}",
                    )
                )
    result.sort(key=lambda bar: bar.end)
    if any(a.end >= b.end for a, b in zip(result, result[1:], strict=False)):
        raise ValueError("SOURCE_DUPLICATE_OR_ORDER")
    return result


class BoundedBars:
    """Read capability: get cannot expose any future/effectively unavailable bar."""

    __slots__ = ("__mapping", "cutoff", "decision_ns", "lag")

    def __init__(self, mapping, cutoff, decision_ns, lag=5):
        if lag not in (5, 30, 60):
            raise ValueError("UNREGISTERED_LAG")
        self.__mapping, self.cutoff, self.decision_ns, self.lag = mapping, cutoff, decision_ns, lag

    def get(self, at, default=None):
        if at > self.cutoff:
            raise ValueError("FUTURE_PRICE_ACCESS")
        bar = self.__mapping.get(at)
        if bar is not None and at + self.lag * common.SECOND > self.decision_ns:
            raise ValueError("BAR_NOT_REPLAY_VISIBLE")
        return bar if bar is not None else default


@dataclass(frozen=True)
class PrefixView:
    cutoff_ns: int
    ready_ns: int
    hourly: tuple
    current: NativeBar | None
    previous: NativeBar | None

    def __post_init__(self):
        dependencies = [b.end for b in self.hourly]
        dependencies += [b.end for b in (self.current, self.previous) if b is not None]
        if any(t > self.cutoff_ns or t + 6 * common.SECOND > self.ready_ns for t in dependencies):
            raise ValueError("FUTURE_OR_UNREADY_PREFIX")

    def bar(self, at):
        if at > self.cutoff_ns:
            raise ValueError("FUTURE_PRICE_ACCESS")
        return next((b for b in (*self.hourly, self.current, self.previous) if b is not None and b.end == at), None)


@dataclass(frozen=True)
class Snapshot:
    symbol: str
    prefix: PrefixView
    state: object
    s6: object
    btc: object
    eth: object
    source_ref: str


class Market:
    """Source orchestration owner; never passed into diagnostic decision functions."""

    def __init__(self, year, last_month, m15_months, old_cache, new_cache):
        self.year = year
        self.symbols = common.protocol()["cohort"]
        self.source_binding = common.digest(
            {
                "prior_source_manifest_sha256": common.sha(common.OLD / "SOURCE_MANIFEST.jsonl"),
                "prior_checksum_proof_sha256": common.sha(common.OLD / "SOL_SOURCE_CHECKS.jsonl"),
                "new_source_manifest_sha256": common.sha(common.HERE / "SOURCE_OBJECTS.jsonl")
                if last_month > 6
                else None,
                "prefix_origin": f"{year}-01-01T00:00:00Z",
                "S6_dependency_rule": "720 exact matched completed H1 returns and 50 closed H4; cutoff bounded",
            }
        )
        self.h1, self.m15, self.h4, self.ema = {}, {}, {}, {}
        self.h1maps, self.m15maps, self.h1times = {}, {}, {}
        begin = common.ns(f"{year}-01-01T00:00:00Z")
        for symbol in self.symbols:
            h1 = load_native(symbol, "1h", year, range(1, last_month + 1), old_cache, new_cache)
            m15 = load_native(symbol, "15m", year, m15_months, old_cache, new_cache)
            h4 = accepted.h4_bars_from_h1(symbol, h1)
            self.h1[symbol], self.m15[symbol], self.h4[symbol] = h1, m15, h4
            accepted.BAR_INDEX[id(h1)] = [b.end for b in h1]
            self.h1times[symbol] = [b.end for b in h1]
            self.h1maps[symbol] = {b.end: b for b in h1}
            self.m15maps[symbol] = {b.end: b for b in m15}
            self.ema[symbol] = (
                accepted.research_prefix_ema(h1, 20, (begin,)),
                accepted.research_prefix_ema(h1, 50, (begin,)),
                accepted.research_prefix_ema(h4, 20, (begin,)),
                accepted.research_prefix_ema(h4, 50, (begin,)),
                accepted.gap_prefix(h1, common.HOUR),
                accepted.gap_prefix(h4, 4 * common.HOUR),
            )
        self.aligned = accepted.aligned_return_series(self.h1)
        self.state = functools.lru_cache(maxsize=512)(self._state)
        self.s6 = functools.lru_cache(maxsize=2)(self._s6)

    def _state(self, symbol, at):
        return accepted.s1_cached_state(symbol, self.h1[symbol], self.h4[symbol], at, *self.ema[symbol])

    def _s6(self, at):
        return accepted.s6_scores(self.symbols, self.h4, self.aligned, at)

    def snapshot(self, symbol, at):
        hcut = at - at % common.HOUR
        times = self.h1times[symbol]
        right = bisect.bisect_right(times, hcut)
        hourly = tuple(self.h1[symbol][max(0, right - 25) : right])
        view = PrefixView(
            at,
            at + 6 * common.SECOND,
            hourly,
            self.m15maps[symbol].get(at),
            self.m15maps[symbol].get(at - common.M15),
        )
        states, _, _ = self.s6(hcut)
        s1 = self.state(symbol, hcut)
        source = common.digest(
            {
                "cutoff": at,
                "symbol": symbol,
                "h1": [b.source for b in hourly],
                "m15": [b.source for b in (view.current, view.previous) if b],
                "s1": s1.get("s1_ref"),
                "S6_context_cutoff": hcut,
                "all_archive_dependency_binding": self.source_binding,
                "s6_h1_ref_start": states[symbol].get("h1_ref_start"),
                "s6_h1_ref_end": states[symbol].get("h1_ref_end"),
            }
        )
        return Snapshot(
            symbol,
            view,
            MappingProxyType(s1),
            MappingProxyType(states[symbol]),
            MappingProxyType(self.state("BTCUSDT", hcut)),
            MappingProxyType(self.state("ETHUSDT", hcut)),
            source,
        )


BASE_FEATURES = (
    "side_long",
    "episode_age_m15",
    "signed_own_h1_log_return",
    "m15_close_position",
    "signed_distance_from_setup_extreme",
    "m15_log_range",
    "own_24h_return_volatility",
)
CONTEXT_FEATURES = (
    "aligned_s6_score",
    "aligned_extreme_decile",
    "signed_btc_h1_return",
    "signed_eth_h1_return",
    "aligned_btc_h4_trend",
    "aligned_eth_h4_trend",
)


def feature_values(snapshot, side, born, extreme):
    """Pure: only frozen cutoff-known values, no source/outcome read capability."""
    view = snapshot.prefix
    if side not in ("LONG", "SHORT") or not born < view.cutoff_ns <= born + common.HOUR:
        raise ValueError("INVALID_FIRST_EVENT_ORIGIN")
    if snapshot.state.get("state") not in ("SETUP", "NO_SETUP"):
        raise ValueError("SOURCE_PREFIX_UNQUALIFIED")
    bars = view.hourly
    if len(bars) != 25 or bars[-1].end != view.cutoff_ns - view.cutoff_ns % common.HOUR:
        raise ValueError("NEED_25_CURRENT_H1_CLOSES")
    if any(b.end - a.end != common.HOUR for a, b in zip(bars, bars[1:], strict=False)):
        raise ValueError("INCOMPLETE_24H_RETURN_PREFIX")
    cur = view.current
    if cur is None or view.previous is None:
        raise ValueError("MISSING_M15_FEATURE_SOURCE")
    sign = 1 if side == "LONG" else -1
    returns = [math.log(float(b.close / a.close)) for a, b in zip(bars, bars[1:], strict=False)]
    mean = math.fsum(returns) / len(returns)
    volatility = math.sqrt(math.fsum((r - mean) ** 2 for r in returns) / (len(returns) - 1))
    spread = cur.high - cur.low
    base = (
        float(side == "LONG"),
        (view.cutoff_ns - born) / common.M15,
        sign * returns[-1],
        float((cur.close - cur.low) / spread) if spread else 0.5,
        sign * float(cur.close / Decimal(str(extreme)) - 1),
        math.log(float(cur.high / cur.low)),
        volatility,
    )
    rank = snapshot.s6
    state = rank.get("state")
    if state != "S6_FIXED_COHORT_RANK_DIAGNOSTIC":
        return base, None
    expected = "TOP" if side == "LONG" else "BOTTOM"
    opposite = "BOTTOM" if side == "LONG" else "TOP"
    values = [snapshot.btc.get("h1_last_return"), snapshot.eth.get("h1_last_return")]
    if any(x is None for x in values):
        return base, None
    trend = {"UP": 1, "DOWN": -1, "NEUTRAL": 0}
    if any(anchor.get("h4_trend") not in trend for anchor in (snapshot.btc, snapshot.eth)):
        return base, None
    context = (
        sign * float(rank["score"]),
        float(1 if rank["decile"] == expected else -1 if rank["decile"] == opposite else 0),
        sign * float(values[0]),
        sign * float(values[1]),
        float(sign * trend[snapshot.btc["h4_trend"]]),
        float(sign * trend[snapshot.eth["h4_trend"]]),
    )
    if any(not math.isfinite(x) for x in (*base, *context)):
        raise ValueError("NONFINITE_FEATURE")
    return base, context


def setup_origin(symbol, setup):
    if not setup.get("setup_id"):
        return None
    result = Origin(symbol, int(setup["setup_cutoff_ns"]), setup["side"], setup["setup_extreme"], setup["setup_ref"])
    if result.identity != setup["setup_id"]:
        raise ValueError("SETUP_IDENTITY_DRIFT")
    return result


def replay_events(market, begin, final, calendar_writer=None):
    """One full expected calendar, preserving H1 boundary ownership/censoring."""
    initial_hcut = begin - begin % common.HOUR - common.HOUR
    state, current, previous = {}, {}, {}
    engines = {}
    for s in market.symbols:
        state[s] = market.state(s, initial_hcut)
        current[s] = accepted.s1_origin_at(s, initial_hcut, lambda at, sym=s: market.state(sym, at))
        previous[s] = current[s]
        engines[s] = Episodes(begin, BoundedBars(market.m15maps[s], begin, begin + 6 * common.SECOND))
    last_hcut = initial_hcut
    for at in range(begin, final, common.M15):
        hcut = at - at % common.HOUR
        if hcut != last_hcut:
            for s in market.symbols:
                previous[s] = current[s]
                new = market.state(s, hcut)
                current[s], _ = accepted.s1_setup_origin(s, hcut, new, state[s], current[s])
                state[s] = new
            last_hcut = hcut
        for s in market.symbols:
            snapshot = market.snapshot(s, at)
            owner = previous[s] if at == hcut else current[s]
            orig = setup_origin(s, owner)
            engines[s].bars = BoundedBars(market.m15maps[s], at, at + 6 * common.SECOND)
            trigger = engines[s].observe(orig, at)
            row = {
                "symbol": s,
                "cutoff_ns": at,
                "ready_ns": at + 6 * common.SECOND,
                "source_current_m15": "OBSERVED" if snapshot.prefix.current else "SOURCE_MISSING",
                "s1_precursor": state[s]["state"],
                "setup_id": orig.identity if orig else "",
                "trigger_status": trigger["s1_trigger_episode_status"],
                "first_event_id": trigger["s1_first_trigger_event_id"],
                "s6_state": snapshot.s6["state"],
                "source_ref": snapshot.source_ref,
                "frozen_policy": "BLOCKED_TIMING_AND_REQUIRED_INPUTS",
            }
            if calendar_writer:
                calendar_writer.writerow(row)
            if trigger["s1_first_trigger_event"]:
                yield snapshot, orig, row
