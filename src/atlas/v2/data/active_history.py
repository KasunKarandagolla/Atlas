"""Bounded exact recursive indicator state over an append-only causal bar prefix.

This pure module owns no persistence, clocks, scheduling or trading authority.
A caller must select the exact full instrument key and cutoff-visible source
revisions before advancing. Non-append arrivals require an explicit rebuild;
retaining a finite tail never changes the original recursive indicator seeds.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from functools import cached_property
from types import MappingProxyType
from typing import Any

from .._serialization import decimal_value, sha256_json, sha256_ref, strict_fields, timestamp
from ..instruments import InstrumentKeyV2
from .bars import BarIntervalV2, CausalBarV2
from .history import IndexedCausalBarV2
from .raw import AvailabilityClassV2, RawObservationV2

ALGORITHM_VERSION = "EXACT_PREFIX_EMA_WILDER_ATR_V1"
STATE_VERSION = "ActiveCausalHistoryStateV1"
MAX_ADVANCE_BARS = 128
MAX_INPUT_REFS = 2 * MAX_ADVANCE_BARS + 1
DAY_NS = 86_400_000_000_000
TAIL_LIMITS = MappingProxyType({BarIntervalV2.M15: 2902, BarIntervalV2.H1: 256,
                                BarIntervalV2.H4: 256, BarIntervalV2.M1: 10081})


class HistoryInvalidationRequired(ValueError):
    """The current prefix cannot be extended without revising prior evidence."""

    def __init__(self, close_at_ns: int) -> None:
        self.close_at_ns = close_at_ns
        self.reason_code = "NON_APPEND_SOURCE_REVISION_REQUIRES_REBUILD"
        super().__init__(self.reason_code)


def _number(value: Any, name: str) -> float:
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError(f"{name} must be a finite number")
    return float(value)


def _optional_number(value: Any, name: str) -> float | None:
    return None if value is None else _number(value, name)


@dataclass(frozen=True)
class HistoryTailItemV1:
    bar: CausalBarV2
    observation_index_ref: str
    ema20: float | None
    ema50: float | None
    atr14: float | None

    def __post_init__(self) -> None:
        if not isinstance(self.bar, CausalBarV2) or type(self.bar.final) is not bool or not self.bar.final:
            raise ValueError("history state requires typed final bars")
        if self.bar.raw.availability_class != AvailabilityClassV2.ACTUAL_SYSTEM:
            raise ValueError("history state only supports ACTUAL_SYSTEM sources")
        sha256_ref(self.observation_index_ref, field="observation_index_ref")
        expected = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": self.bar.raw.record_id})
        if self.observation_index_ref != expected:
            raise ValueError("history observation index locator is not canonical for its exact raw source")
        for field in ("ema20", "ema50", "atr14"):
            object.__setattr__(self, field, _optional_number(getattr(self, field), field))
        if any(value is not None and value <= 0 for value in (self.ema20, self.ema50)):
            raise ValueError("EMA must be positive when estimable")
        if self.atr14 is not None and self.atr14 < 0:
            raise ValueError("ATR cannot be negative")

    @cached_property
    def content_hash(self) -> str:
        """Bind every immutable bar, receipt, locator and indicator field."""
        return sha256_json(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {"bar": self.bar.to_dict(), "raw": self.bar.raw.to_dict(),
                "bar_ref": self.bar.content_hash, "observation_index_ref": self.observation_index_ref,
                "ema20": self.ema20, "ema50": self.ema50, "atr14": self.atr14}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> HistoryTailItemV1:
        fields = {"bar", "raw", "bar_ref", "observation_index_ref", "ema20", "ema50", "atr14"}
        row = strict_fields(value, expected=fields, required=fields, name=cls.__name__)
        bar_fields = {"record_id", "raw_payload_hash", "instrument_revision", "interval", "open_at_ns",
                      "close_at_ns", "open", "high", "low", "close", "volume", "final"}
        body = strict_fields(row["bar"], expected=bar_fields, required=bar_fields, name="history bar")
        raw = RawObservationV2.from_dict(row["raw"])
        if (body["record_id"] != raw.record_id or body["raw_payload_hash"] != raw.raw_payload_hash
                or body["instrument_revision"] != raw.instrument_revision):
            raise ValueError("history bar conflicts with exact raw observation")
        if any(not isinstance(body[field], str) for field in ("open", "high", "low", "close", "volume")):
            raise ValueError("history OHLCV wire values must be decimal strings")
        bar = CausalBarV2(raw, BarIntervalV2(body["interval"]), body["open_at_ns"], body["close_at_ns"],
                         decimal_value(body["open"], field="open"), decimal_value(body["high"], field="high"),
                         decimal_value(body["low"], field="low"), decimal_value(body["close"], field="close"),
                         decimal_value(body["volume"], field="volume"), body["final"])
        if bar.content_hash != row["bar_ref"]:
            raise ValueError("history bar checksum mismatch")
        return cls(bar, row["observation_index_ref"], row["ema20"], row["ema50"], row["atr14"])


@dataclass(frozen=True)
class ActiveCausalHistoryStateV1:
    key: InstrumentKeyV2
    interval: BarIntervalV2
    source_id: str
    total_count: int
    observed_unique_utc_close_days: int
    last_utc_close_day: int
    max_source_available_at_ns: int
    seed_closes: tuple[float, ...]
    seed_true_ranges: tuple[float, ...]
    ema20: float | None
    ema50: float | None
    atr14: float | None
    tail: tuple[HistoryTailItemV1, ...]
    previous_state_ref: str | None
    input_refs: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.key, InstrumentKeyV2):
            raise ValueError("history state requires full InstrumentKeyV2")
        object.__setattr__(self, "interval", BarIntervalV2(self.interval))
        if self.interval not in TAIL_LIMITS:
            raise ValueError("history interval has no registered tail budget")
        if not isinstance(self.source_id, str) or not self.source_id.strip():
            raise ValueError("history source_id must be explicit")
        for field in ("total_count", "observed_unique_utc_close_days", "last_utc_close_day"):
            if type(getattr(self, field)) is not int or getattr(self, field) < (0 if field == "last_utc_close_day" else 1):
                raise ValueError(f"{field} has an invalid count")
        timestamp(self.max_source_available_at_ns, field="max_source_available_at_ns")
        if self.observed_unique_utc_close_days > self.total_count:
            raise ValueError("observed days exceed actual bar count")
        if (not isinstance(self.seed_closes, tuple) or len(self.seed_closes) != min(self.total_count, 50)
                or not isinstance(self.seed_true_ranges, tuple)
                or len(self.seed_true_ranges) != min(self.total_count, 14)):
            raise ValueError("history seeds must retain exactly the bounded initial observations")
        object.__setattr__(self, "seed_closes", tuple(_number(x, "seed_close") for x in self.seed_closes))
        object.__setattr__(self, "seed_true_ranges", tuple(_number(x, "seed_TR") for x in self.seed_true_ranges))
        if any(x <= 0 for x in self.seed_closes):
            raise ValueError("seed closes must be positive")
        if any(x < 0 for x in self.seed_true_ranges):
            raise ValueError("true ranges cannot be negative")
        if not isinstance(self.tail, tuple) or len(self.tail) != min(self.total_count, TAIL_LIMITS[self.interval]):
            raise ValueError("history tail violates its registered fixed budget")
        for position, item in enumerate(self.tail, self.total_count - len(self.tail) + 1):
            if not isinstance(item, HistoryTailItemV1):
                raise ValueError("history tail must contain typed items")
            for field, period in (("ema20", 20), ("ema50", 50), ("atr14", 14)):
                if (getattr(item, field) is None) != (position < period):
                    raise ValueError("history tail indicator warmup does not match original prefix")
            if (item.bar.instrument_revision != self.key.contract_revision or item.bar.interval != self.interval
                    or item.bar.raw.source_id != self.source_id
                    or item.bar.raw.available_at_ns > self.max_source_available_at_ns):
                raise ValueError("history tail violates exact source identity")
        if any(b.bar.close_at_ns <= a.bar.close_at_ns for a, b in zip(self.tail, self.tail[1:], strict=False)):
            raise ValueError("history tail must be strictly chronological")
        if self.last_utc_close_day != self.tail[-1].bar.close_at_ns // DAY_NS:
            raise ValueError("history last-day cursor conflicts with its final bar")
        visible_days = len({item.bar.close_at_ns // DAY_NS for item in self.tail})
        if (visible_days > self.observed_unique_utc_close_days
                or (self.total_count == len(self.tail) and visible_days != self.observed_unique_utc_close_days)):
            raise ValueError("history day count omits visible source days")
        for field, period in (("ema20", 20), ("ema50", 50), ("atr14", 14)):
            value = _optional_number(getattr(self, field), field)
            if (value is None) != (self.total_count < period) or value != getattr(self.tail[-1], field):
                raise ValueError("history final indicator/warmup state conflicts with tail")
            object.__setattr__(self, field, value)
        if self.previous_state_ref is not None:
            sha256_ref(self.previous_state_ref, field="previous_state_ref")
        if (not isinstance(self.input_refs, tuple) or not 1 <= len(self.input_refs) <= MAX_INPUT_REFS
                or self.input_refs != tuple(sorted(set(self.input_refs)))):
            raise ValueError("history input manifest must be bounded, sorted and unique")
        for ref in self.input_refs:
            sha256_ref(ref, field="input_ref")
        if self.previous_state_ref is not None and self.previous_state_ref not in self.input_refs:
            raise ValueError("history manifest omits exact previous checkpoint")
        if not {self.tail[-1].bar.content_hash, self.tail[-1].observation_index_ref}.issubset(self.input_refs):
            raise ValueError("history manifest omits newest exact bar/source references")

    @property
    def last_close_at_ns(self) -> int:
        return self.tail[-1].bar.close_at_ns

    @property
    def last_bar_ref(self) -> str:
        return self.tail[-1].bar.content_hash

    @property
    def last_observation_index_ref(self) -> str:
        return self.tail[-1].observation_index_ref

    @property
    def bars(self) -> tuple[CausalBarV2, ...]:
        return tuple(item.bar for item in self.tail)

    def _body(self) -> dict[str, Any]:
        return {"version": STATE_VERSION, "algorithm_version": ALGORITHM_VERSION, "authority": "ZERO",
                "availability_class": AvailabilityClassV2.ACTUAL_SYSTEM.value,
                "key": self.key.to_dict(), "interval": self.interval.value, "source_id": self.source_id,
                "total_count": self.total_count, "observed_unique_utc_close_days": self.observed_unique_utc_close_days,
                "last_utc_close_day": self.last_utc_close_day,
                "max_source_available_at_ns": self.max_source_available_at_ns,
                "seed_closes": list(self.seed_closes), "seed_true_ranges": list(self.seed_true_ranges),
                "ema20": self.ema20, "ema50": self.ema50, "atr14": self.atr14,
                # Ordered digests cover each item's complete serialized body,
                # including middle-tail source chronology and indicator values.
                # Immutable item digests are reused between bounded advances.
                "tail_hash": sha256_json([item.content_hash for item in self.tail]),
                "tail_count": len(self.tail), "tail_first_bar_ref": self.tail[0].bar.content_hash,
                "tail_last_bar_ref": self.tail[-1].bar.content_hash,
                "previous_state_ref": self.previous_state_ref,
                "input_refs": list(self.input_refs)}

    @cached_property
    def content_hash(self) -> str:
        return sha256_json(self._body())

    def to_dict(self) -> dict[str, Any]:
        return {**self._body(), "tail": [item.to_dict() for item in self.tail], "content_hash": self.content_hash}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ActiveCausalHistoryStateV1:
        fields = {"version", "algorithm_version", "authority", "availability_class", "key", "interval",
                  "source_id", "total_count", "observed_unique_utc_close_days", "last_utc_close_day",
                  "max_source_available_at_ns", "seed_closes", "seed_true_ranges", "ema20", "ema50", "atr14",
                  "tail_hash", "tail_count", "tail_first_bar_ref", "tail_last_bar_ref", "tail",
                  "previous_state_ref", "input_refs", "content_hash"}
        row = strict_fields(value, expected=fields, required=fields, name=STATE_VERSION)
        if (row["version"] != STATE_VERSION or row["algorithm_version"] != ALGORITHM_VERSION
                or row["authority"] != "ZERO" or row["availability_class"] != AvailabilityClassV2.ACTUAL_SYSTEM.value):
            raise ValueError("unsupported active history identity/authority")
        for name, limit in (("seed_closes", 50), ("seed_true_ranges", 14), ("input_refs", MAX_INPUT_REFS)):
            if not isinstance(row[name], list) or len(row[name]) > limit:
                raise ValueError("history wire array exceeds fixed bound")
        interval = BarIntervalV2(row["interval"])
        if (interval not in TAIL_LIMITS or not isinstance(row["tail"], list)
                or not 1 <= len(row["tail"]) <= TAIL_LIMITS[interval]):
            raise ValueError("history wire tail exceeds fixed bound")
        if any(not isinstance(item, Mapping) for item in row["tail"]):
            raise ValueError("history wire tail requires object items")
        if (type(row["tail_count"]) is not int or row["tail_count"] != len(row["tail"])
                or row["tail_first_bar_ref"] != row["tail"][0].get("bar_ref")
                or row["tail_last_bar_ref"] != row["tail"][-1].get("bar_ref")):
            raise ValueError("history wire tail identity summary conflicts")
        sha256_ref(row["content_hash"], field="content_hash")
        identity = {key: val for key, val in row.items() if key not in {"tail", "content_hash"}}
        if sha256_json(identity) != row["content_hash"]:
            raise ValueError("history state checksum mismatch")
        tail = tuple(HistoryTailItemV1.from_dict(item) for item in row["tail"])
        if sha256_json([item.content_hash for item in tail]) != row["tail_hash"]:
            raise ValueError("history tail checksum mismatch")
        return cls(InstrumentKeyV2.from_dict(row["key"]), interval, row["source_id"], row["total_count"],
                   row["observed_unique_utc_close_days"], row["last_utc_close_day"],
                   row["max_source_available_at_ns"], tuple(row["seed_closes"]), tuple(row["seed_true_ranges"]),
                   row["ema20"], row["ema50"], row["atr14"],
                   tail,
                   row["previous_state_ref"], tuple(row["input_refs"]))


def advance(state: ActiveCausalHistoryStateV1 | None, bars: tuple[IndexedCausalBarV2, ...], *,
            key: InstrumentKeyV2, interval: BarIntervalV2) -> ActiveCausalHistoryStateV1:
    """Advance at most 128 already-selected causal bars; gaps are retained honestly.

    A same-close duplicate or any older revision is refused, including an exact
    duplicate. Discovery must deduplicate before advancement; this function must
    never hide a revision behind a cached seed or silently change source identity.
    """
    frame = BarIntervalV2(interval)
    if not isinstance(key, InstrumentKeyV2) or frame not in TAIL_LIMITS:
        raise ValueError("history advancement requires exact key and supported interval")
    if not isinstance(bars, tuple) or len(bars) > MAX_ADVANCE_BARS:
        raise ValueError("history advancement exceeds 128-bar work budget")
    if state is not None and not isinstance(state, ActiveCausalHistoryStateV1):
        raise ValueError("history advancement requires a typed previous state")
    if state is not None and (state.key != key or state.interval != frame):
        raise ValueError("history advancement full-key/interval mismatch")
    if not bars:
        if state is None:
            raise ValueError("history bootstrap requires at least one causal source")
        return state
    if any(not isinstance(item, IndexedCausalBarV2) or not isinstance(item.bar, CausalBarV2) for item in bars):
        raise ValueError("history advancement requires exact indexed causal bars")
    source_id = state.source_id if state else bars[0].bar.raw.source_id
    count = state.total_count if state else 0
    days = state.observed_unique_utc_close_days if state else 0
    last_day = state.last_utc_close_day if state else None
    max_available = state.max_source_available_at_ns if state else 0
    closes = list(state.seed_closes) if state else []
    true_ranges = list(state.seed_true_ranges) if state else []
    e20, e50, a14 = (state.ema20, state.ema50, state.atr14) if state else (None, None, None)
    tail = list(state.tail) if state else []
    previous_close = float(tail[-1].bar.close) if tail else None
    last_close = tail[-1].bar.close_at_ns if tail else None
    refs = {state.content_hash} if state else set()
    for indexed in bars:
        if not isinstance(indexed, IndexedCausalBarV2):
            raise ValueError("history advancement requires exact indexed causal bars")
        bar = indexed.bar
        # Validate before doing any arithmetic or advancing an immutable cursor.
        HistoryTailItemV1(bar, indexed.observation_index_ref, None, None, None)
        if (bar.instrument_revision != key.contract_revision or bar.interval != frame
                or bar.raw.source_id != source_id):
            raise ValueError("history advancement exact source identity mismatch")
        if last_close is not None and bar.close_at_ns <= last_close:
            raise HistoryInvalidationRequired(bar.close_at_ns)
        close = float(bar.close)
        if not math.isfinite(close):
            raise ValueError("indicator close must be finite")
        previous = close if previous_close is None else previous_close
        tr = max(float(bar.high - bar.low), abs(float(bar.high) - previous), abs(float(bar.low) - previous))
        if not math.isfinite(tr):
            raise ValueError("indicator true range must be finite")
        count += 1
        if count <= 50:
            closes.append(close)
        if count <= 14:
            true_ranges.append(tr)
        if count == 20:
            e20 = math.fsum(closes[:20]) / 20
        elif count > 20:
            assert e20 is not None
            e20 += (2 / (20 + 1)) * (close - e20)
        if count == 50:
            e50 = math.fsum(closes) / 50
        elif count > 50:
            assert e50 is not None
            e50 += (2 / (50 + 1)) * (close - e50)
        if count == 14:
            a14 = math.fsum(true_ranges) / 14
        elif count > 14:
            assert a14 is not None
            a14 = ((14 - 1) * a14 + tr) / 14
        day = bar.close_at_ns // DAY_NS
        if day != last_day:
            days += 1
        last_day = day
        max_available = max(max_available, bar.raw.available_at_ns)
        tail.append(HistoryTailItemV1(bar, indexed.observation_index_ref, e20, e50, a14))
        previous_close, last_close = close, bar.close_at_ns
        refs.update((bar.content_hash, indexed.observation_index_ref))
    assert last_day is not None
    return ActiveCausalHistoryStateV1(key=key, interval=frame, source_id=source_id,
        total_count=count, observed_unique_utc_close_days=days, last_utc_close_day=last_day,
        max_source_available_at_ns=max_available,
        seed_closes=tuple(closes), seed_true_ranges=tuple(true_ranges), ema20=e20, ema50=e50, atr14=a14,
        tail=tuple(tail[-TAIL_LIMITS[frame]:]),
        previous_state_ref=state.content_hash if state else None, input_refs=tuple(sorted(refs)))
