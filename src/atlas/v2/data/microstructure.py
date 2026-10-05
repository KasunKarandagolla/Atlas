"""Sequence-valid public L2/trade evidence and causal S4 research features.

This module consumes already captured raw frames.  It does not pretend that a
REST snapshot or candle stream is a live order book.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any, ClassVar, cast

from .._serialization import canonical_json, decimal_value, nonblank, sha256_json, sha256_ref, strict_fields, timestamp
from ..instruments import InstrumentKeyV2
from .capabilities import (
    FeedCoverageEvidenceV2,
    FeedCoverageStateV2,
    capability_for_public_channel_v2,
    default_evidence_capability_matrix_v2,
)

S4_FEATURE_VERSION = "S4_MICROSTRUCTURE_FEATURE_V1"
S4_ABSORPTION_VERSION = "S4_ABSORPTION_SHADOW_V1"
S4_EXECUTION_CONTEXT_VERSION = "S4_EXECUTION_QUALITY_CONTEXT_V1"
DEFAULT_BOOK_WARMUP_NS = 30_000_000_000
DEFAULT_BOOK_STALE_NS = 1_000_000_000
LIVE_BOOK_WINDOW_NS = 30_000_000_000
LIVE_BOOK_MAX_FRAMES = 4096
LIVE_BOOK_MAX_LEVELS = 4096
S4_FEATURE_POLICY_SPEC = {
    "book_sequence_required": True, "gap_action": "NOT_ESTIMABLE_UNTIL_FRESH_SNAPSHOT_BRIDGE_AND_WARMUP",
    "windows_seconds": [1, 5, 30], "window_left_boundary_baseline": True,
    "ofi_windows": True, "flow_price_response_windows": True,
    "source_channel_capability_row_required": True, "trade_coverage_matrix_hash_exact": True,
    "actual_receipt_only_book_and_trade_evidence": True,
    "binance_rest_snapshot_requires_buffered_update_bridge": True,
    "binance_snapshot": "REST lastUpdateId = L",
    "binance_stale_buffered_events": "discard only buffered events where u < L; u == L remains eligible",
    "binance_snapshot_bridge": "first processed event requires U <= L <= u",
    "binance_subsequent_updates": (
        "require pu == previous accepted u; any mismatch immediately enters GAP_DETECTED, independent of feature warmup"
    ),
    "binance_gap_recovery": "fresh REST snapshot, new valid bridge, then declared S4 warmup",
    "decision_view": "ACTUAL_RECEIPT",
    "future_markout_role": "OUTCOME_ONLY", "baseline_contract": "INTRADAY_CORE_V1_UNCHANGED",
}
S4_FEATURE_POLICY_HASH = sha256_json({"policy_id": S4_FEATURE_VERSION, "spec": S4_FEATURE_POLICY_SPEC})
S4_ABSORPTION_POLICY_HASH = sha256_json({"policy_id": S4_ABSORPTION_VERSION, "spec": {
    "prior_only_expected_response": True, "minimum_prior_samples": 3,
    "flow_response_window_seconds": 30,
    "unusual_flow_z_default": "1", "residual_minimum_default": "0",
    "opposing_persistence_required": True, "exact_action": "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT",
    "threshold_status": "ENGINEERING_RESEARCH_DEFAULT_UNQUALIFIED",
}})
S4_EXECUTION_CONTEXT_POLICY_HASH = sha256_json({"policy_id": S4_EXECUTION_CONTEXT_VERSION, "spec": {
    "inputs": ["valid_sequence_book", "spread", "depth_bands", "data_age"], "selector_influence": "ZERO",
}})


class BookStateV2(StrEnum):
    COLD = "COLD"
    WARMING = "WARMING"
    VALID = "VALID"
    GAP_DETECTED = "GAP_DETECTED"
    SNAPSHOT_RECOVERY = "SNAPSHOT_RECOVERY"
    INVALID = "INVALID"


class BinanceDepthSyncPhaseV2(StrEnum):
    """Snapshot synchronization phase, independent of S4 feature warmup."""

    NOT_APPLICABLE = "NOT_APPLICABLE"
    AWAITING_SNAPSHOT_BRIDGE = "AWAITING_SNAPSHOT_BRIDGE"
    BRIDGE_COMPLETE = "BRIDGE_COMPLETE"


class AvailabilityViewV2(StrEnum):
    ACTUAL_RECEIPT = "ACTUAL_RECEIPT"
    RECONSTRUCTED_MARKET = "RECONSTRUCTED_MARKET"


@dataclass(frozen=True)
class BookLevelV2:
    price: Decimal
    quantity: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "price", decimal_value(self.price, field="price"))
        object.__setattr__(self, "quantity", decimal_value(self.quantity, field="quantity"))
        if self.price <= 0 or self.quantity < 0:
            raise ValueError("book price must be positive and quantity nonnegative")

    def to_dict(self) -> dict[str, str]:
        return {"price": str(self.price), "quantity": str(self.quantity)}


def _levels(values: Iterable[BookLevelV2 | tuple[Decimal | str, Decimal | str]], *, bids: bool) -> tuple[BookLevelV2, ...]:
    result = tuple(value if isinstance(value, BookLevelV2) else
                   BookLevelV2(cast(Decimal, value[0]), cast(Decimal, value[1]))
                   for value in values)
    prices = tuple(level.price for level in result)
    if len(prices) != len(set(prices)):
        raise ValueError("book prices must be unique")
    if bids and prices != tuple(sorted(prices, reverse=True)):
        raise ValueError("bids must be sorted descending")
    if not bids and prices != tuple(sorted(prices)):
        raise ValueError("asks must be sorted ascending")
    return result


@dataclass(frozen=True)
class L2SnapshotV2:
    instrument: InstrumentKeyV2
    source_id: str
    channel: str
    sequence_semantics: str
    last_update_id: int
    event_at_ns: int | None
    received_at_ns: int
    available_at_ns: int
    bids: tuple[BookLevelV2, ...]
    asks: tuple[BookLevelV2, ...]
    raw_content_ref: str
    source_health: str
    declared_depth: int
    availability_class: str = "ACTUAL_SYSTEM"
    snapshot_token: str | None = None
    source_health_ref: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.instrument, InstrumentKeyV2):
            raise ValueError("full InstrumentKeyV2 is required")
        for name in ("source_id", "channel", "sequence_semantics", "source_health", "availability_class"):
            nonblank(getattr(self, name), field=name)
        if type(self.last_update_id) is not int or self.last_update_id < 0:
            raise ValueError("snapshot update id must be a nonnegative integer")
        for name in ("event_at_ns", "received_at_ns", "available_at_ns"):
            value = getattr(self, name)
            if value is not None:
                timestamp(value, field=name)
        if self.available_at_ns < self.received_at_ns:
            raise ValueError("available time cannot precede actual receipt")
        if self.event_at_ns is not None and self.event_at_ns > self.available_at_ns:
            # Event time may exceed local availability during clock skew; retain but don't reject.
            pass
        object.__setattr__(self, "bids", _levels(self.bids, bids=True))
        object.__setattr__(self, "asks", _levels(self.asks, bids=False))
        sha256_ref(self.raw_content_ref, field="raw_content_ref")
        if type(self.declared_depth) is not int or self.declared_depth <= 0:
            raise ValueError("declared depth must be positive")
        if not self.bids or not self.asks or self.bids[0].price >= self.asks[0].price:
            raise ValueError("snapshot must have a non-crossed two-sided book")
        if self.snapshot_token is not None:
            nonblank(self.snapshot_token, field="snapshot_token")
        if self.source_health_ref is not None:
            sha256_ref(self.source_health_ref, field="source_health_ref")

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, "instrument": self.instrument.to_dict(), "source_id": self.source_id,
                "channel": self.channel, "sequence_semantics": self.sequence_semantics,
                "last_update_id": self.last_update_id, "event_at_ns": self.event_at_ns,
                "received_at_ns": self.received_at_ns, "available_at_ns": self.available_at_ns,
                "bids": [item.to_dict() for item in self.bids], "asks": [item.to_dict() for item in self.asks],
                "raw_content_ref": self.raw_content_ref, "source_health": self.source_health,
                "declared_depth": self.declared_depth, "availability_class": self.availability_class,
                "snapshot_token": self.snapshot_token, "source_health_ref": self.source_health_ref}


@dataclass(frozen=True)
class L2DeltaV2:
    instrument: InstrumentKeyV2
    source_id: str
    channel: str
    sequence_semantics: str
    first_update_id: int | None
    last_update_id: int
    previous_update_id: int | None
    event_at_ns: int | None
    received_at_ns: int
    available_at_ns: int
    bids: tuple[BookLevelV2, ...]
    asks: tuple[BookLevelV2, ...]
    raw_content_ref: str
    source_health: str
    availability_class: str = "ACTUAL_SYSTEM"
    reset: bool = False
    source_health_ref: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.instrument, InstrumentKeyV2):
            raise ValueError("full InstrumentKeyV2 is required")
        for name in ("source_id", "channel", "sequence_semantics", "source_health", "availability_class"):
            nonblank(getattr(self, name), field=name)
        for name in ("first_update_id", "last_update_id", "previous_update_id"):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError(f"{name} must be a nonnegative integer or null")
        if type(self.last_update_id) is not int:
            raise ValueError("last_update_id is required")
        for name in ("event_at_ns", "received_at_ns", "available_at_ns"):
            value = getattr(self, name)
            if value is not None:
                timestamp(value, field=name)
        if self.available_at_ns < self.received_at_ns:
            raise ValueError("available time cannot precede actual receipt")
        object.__setattr__(self, "bids", _levels(self.bids, bids=True))
        object.__setattr__(self, "asks", _levels(self.asks, bids=False))
        sha256_ref(self.raw_content_ref, field="raw_content_ref")
        if self.source_health_ref is not None:
            sha256_ref(self.source_health_ref, field="source_health_ref")

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, "instrument": self.instrument.to_dict(), "source_id": self.source_id,
                "channel": self.channel, "sequence_semantics": self.sequence_semantics,
                "first_update_id": self.first_update_id, "last_update_id": self.last_update_id,
                "previous_update_id": self.previous_update_id, "event_at_ns": self.event_at_ns,
                "received_at_ns": self.received_at_ns, "available_at_ns": self.available_at_ns,
                "bids": [item.to_dict() for item in self.bids], "asks": [item.to_dict() for item in self.asks],
                "raw_content_ref": self.raw_content_ref, "source_health": self.source_health,
                "availability_class": self.availability_class, "reset": self.reset,
                "source_health_ref": self.source_health_ref}


@dataclass(frozen=True)
class L2SequenceFaultV2:
    instrument: InstrumentKeyV2
    source_id: str
    channel: str
    fault: str
    event_at_ns: int | None
    received_at_ns: int
    available_at_ns: int
    raw_content_ref: str
    source_health: str
    source_health_ref: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.instrument, InstrumentKeyV2):
            raise ValueError("sequence fault requires full instrument identity")
        for name in ("source_id", "channel", "fault", "source_health"):
            nonblank(getattr(self, name), field=name)
        for name in ("event_at_ns", "received_at_ns", "available_at_ns"):
            value = getattr(self, name)
            if value is not None:
                timestamp(value, field=name)
        if self.available_at_ns < self.received_at_ns:
            raise ValueError("fault availability cannot precede receipt")
        sha256_ref(self.raw_content_ref, field="raw_content_ref")
        if self.source_health_ref is not None:
            sha256_ref(self.source_health_ref, field="source_health_ref")


@dataclass(frozen=True)
class BboV2:
    bid_price: Decimal
    bid_quantity: Decimal
    ask_price: Decimal
    ask_quantity: Decimal

    @classmethod
    def from_book(cls, bids: tuple[BookLevelV2, ...], asks: tuple[BookLevelV2, ...]) -> BboV2:
        return cls(bids[0].price, bids[0].quantity, asks[0].price, asks[0].quantity)

    @property
    def mid(self) -> Decimal:
        return (self.bid_price + self.ask_price) / Decimal(2)

    @property
    def spread(self) -> Decimal:
        return self.ask_price - self.bid_price

    @property
    def microprice(self) -> Decimal:
        denominator = self.bid_quantity + self.ask_quantity
        return self.mid if denominator == 0 else (
            self.ask_price * self.bid_quantity + self.bid_price * self.ask_quantity
        ) / denominator


BookFrameStateV2 = tuple[
    int, int, str, BboV2, tuple[BookLevelV2, ...], tuple[BookLevelV2, ...], int | None
]


@dataclass(frozen=True)
class AggressiveTradeV2:
    instrument: InstrumentKeyV2
    source_id: str
    channel: str
    trade_id: str
    aggressor_side: str | None
    price: Decimal
    quantity: Decimal
    event_at_ns: int | None
    received_at_ns: int
    available_at_ns: int
    raw_content_ref: str
    source_health: str
    side_convention: str
    availability_class: str = "ACTUAL_SYSTEM"
    source_health_ref: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.instrument, InstrumentKeyV2):
            raise ValueError("full InstrumentKeyV2 is required")
        for name in ("source_id", "channel", "trade_id", "source_health", "side_convention", "availability_class"):
            nonblank(getattr(self, name), field=name)
        if self.aggressor_side not in {"BUY", "SELL", None}:
            raise ValueError("aggressor_side must be qualified BUY, SELL or unavailable")
        object.__setattr__(self, "price", decimal_value(self.price, field="price"))
        object.__setattr__(self, "quantity", decimal_value(self.quantity, field="quantity"))
        if self.price <= 0 or self.quantity < 0:
            raise ValueError("trade price must be positive and quantity nonnegative")
        for name in ("event_at_ns", "received_at_ns", "available_at_ns"):
            value = getattr(self, name)
            if value is not None:
                timestamp(value, field=name)
        if self.available_at_ns < self.received_at_ns:
            raise ValueError("available time cannot precede actual receipt")
        sha256_ref(self.raw_content_ref, field="raw_content_ref")
        if self.source_health_ref is not None:
            sha256_ref(self.source_health_ref, field="source_health_ref")

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, "instrument": self.instrument.to_dict(), "source_id": self.source_id,
                "channel": self.channel, "trade_id": self.trade_id, "aggressor_side": self.aggressor_side, "price": str(self.price),
                "quantity": str(self.quantity), "event_at_ns": self.event_at_ns,
                "received_at_ns": self.received_at_ns, "available_at_ns": self.available_at_ns,
                "raw_content_ref": self.raw_content_ref, "source_health": self.source_health,
                "side_convention": self.side_convention, "availability_class": self.availability_class,
                "source_health_ref": self.source_health_ref}


@dataclass(frozen=True)
class SequenceStateV2:
    state: BookStateV2
    last_update_id: int | None
    recovery_epoch: int
    state_changed_at_ns: int
    reason: str | None = None


class SequenceValidBookV2:
    """Deterministic L2 state machine with explicit snapshot recovery and warmup."""

    def __init__(self, *, instrument: InstrumentKeyV2, source_id: str, channel: str,
                 sequence_semantics: str, warmup_ns: int = DEFAULT_BOOK_WARMUP_NS,
                 stale_ns: int = DEFAULT_BOOK_STALE_NS, declared_cadence_ns: int | None = None) -> None:
        if warmup_ns < 0 or stale_ns <= 0:
            raise ValueError("warmup and stale limits are invalid")
        self.instrument = instrument
        self.source_id = nonblank(source_id, field="source_id")
        self.channel = nonblank(channel, field="channel")
        self.sequence_semantics = nonblank(sequence_semantics, field="sequence_semantics")
        self._capability_matrix = default_evidence_capability_matrix_v2()
        self._book_capability = capability_for_public_channel_v2(self._capability_matrix, instrument, channel)
        expected_sequence = "BYBIT_U" if instrument.venue.value == "BYBIT" else "BINANCE_U_PU"
        self._capability_error: str | None = None
        if (self._book_capability is None
                or "sequence-valid displayed depth context" not in " ".join(self._book_capability.permitted_uses)):
            self._capability_error = "NOT_ESTIMABLE_BOOK_CHANNEL_CAPABILITY_MISSING"
        elif sequence_semantics != expected_sequence:
            self._capability_error = "NOT_ESTIMABLE_BOOK_SEQUENCE_CAPABILITY_MISMATCH"
        self.warmup_ns = warmup_ns
        self.stale_ns = stale_ns
        if declared_cadence_ns is not None and (type(declared_cadence_ns) is not int or declared_cadence_ns <= 0):
            raise ValueError("declared cadence must be a positive integer nanosecond interval")
        self.declared_cadence_ns = declared_cadence_ns
        self.state = BookStateV2.COLD
        self._binance_sync_phase = (
            BinanceDepthSyncPhaseV2.AWAITING_SNAPSHOT_BRIDGE
            if sequence_semantics == "BINANCE_U_PU" else BinanceDepthSyncPhaseV2.NOT_APPLICABLE
        )
        self.last_update_id: int | None = None
        self.epoch = 0
        self.state_changed_at_ns = 0
        self.valid_since_ns: int | None = None
        self.last_received_at_ns: int | None = None
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self._seen: dict[int, str] = {}
        self._frames: list[BookFrameStateV2] = []
        self._frame_epochs: list[int] = []
        self._source_health = "UNKNOWN"
        self._source_health_ref: str | None = None
        self._reason: str | None = None
        self._declared_depth = 0
        self._state_events: list[tuple[int, BookStateV2, int, str, str | None, int | None, str | None]] = []
        self._live_retained_from_ns: int | None = None

    def evidence_state(self) -> dict[str, Any]:
        """Exact terminal state digest preimage; caches are not book facts."""
        return {"instrument": self.instrument.to_dict(), "source_id": self.source_id,
                "channel": self.channel, "sequence_semantics": self.sequence_semantics,
                "epoch": self.epoch, "state": self.state.value, "reason": self._reason,
                "last_update_id": self.last_update_id, "last_received_at_ns": self.last_received_at_ns,
                "valid_since_ns": self.valid_since_ns, "state_changed_at_ns": self.state_changed_at_ns,
                "source_health": self._source_health, "source_health_ref": self._source_health_ref,
                "declared_depth": self._declared_depth, "warmup_ns": self.warmup_ns,
                "stale_ns": self.stale_ns, "binance_sync_phase": self._binance_sync_phase.value,
                "bids": [[str(p), str(q)] for p, q in sorted(self.bids.items(), reverse=True)],
                "asks": [[str(p), str(q)] for p, q in sorted(self.asks.items())]}

    def compact_live_state(self, *, as_of_ns: int) -> None:
        """Bound operational windows after immutable raw archive publication.

        Preserve the left-boundary baseline for all declared 1/5/30s features.
        Older cutoffs require raw replay; excess traffic fails closed rather
        than silently yielding truncated feature windows.
        """
        floor = timestamp(as_of_ns, field="live book cutoff") - LIVE_BOOK_WINDOW_NS
        prefix = [i for i, row in enumerate(self._frames) if row[0] <= floor]
        start = prefix[-1] if prefix else 0
        self._frames = self._frames[start:]
        self._frame_epochs = self._frame_epochs[start:]
        events = [i for i, row in enumerate(self._state_events) if row[0] <= floor]
        self._state_events = self._state_events[events[-1] if events else 0:]
        self._live_retained_from_ns = max(self._live_retained_from_ns or 0, max(0, floor))
        if len(self._frames) > LIVE_BOOK_MAX_FRAMES or len(self.bids) > LIVE_BOOK_MAX_LEVELS or len(self.asks) > LIVE_BOOK_MAX_LEVELS:
            self._invalidate(BookStateV2.INVALID, "LIVE_BOOK_CAPACITY_REQUIRES_SNAPSHOT_RECOVERY", as_of_ns)
            self._frames.clear()
            self._frame_epochs.clear()
            self.bids.clear()
            self.asks.clear()
        self._state_events = self._state_events[-(LIVE_BOOK_MAX_FRAMES + 1):]
        if len(self._seen) > LIVE_BOOK_MAX_FRAMES:
            self._seen = dict(tuple(self._seen.items())[-LIVE_BOOK_MAX_FRAMES:])

    @property
    def sequence_state(self) -> SequenceStateV2:
        return SequenceStateV2(self.state, self.last_update_id, self.epoch, self.state_changed_at_ns,
                               self._reason)

    def _invalidate(self, state: BookStateV2, reason: str, at_ns: int) -> None:
        # Receipt chronology is authoritative. A malformed frame may carry a
        # regressed timestamp, but it cannot insert a state transition into the
        # already observed past and rewrite a prior cutoff.
        if self._state_events:
            at_ns = max(at_ns, self._state_events[-1][0])
        self.state = state
        self.state_changed_at_ns = at_ns
        self.valid_since_ns = None
        self._reason = reason
        self._seen.clear()
        if self.sequence_semantics == "BINANCE_U_PU":
            self._binance_sync_phase = BinanceDepthSyncPhaseV2.AWAITING_SNAPSHOT_BRIDGE
        self._state_events.append((at_ns, state, self.epoch, self._source_health, reason, self.valid_since_ns,
                                   self._source_health_ref))

    def disconnect(self, at_ns: int) -> SequenceStateV2:
        timestamp(at_ns, field="disconnect time")
        self._invalidate(BookStateV2.INVALID, "DISCONNECT_REQUIRES_NEW_SNAPSHOT", at_ns)
        self.last_received_at_ns = None
        return self.sequence_state

    def reconnect(self, at_ns: int) -> SequenceStateV2:
        timestamp(at_ns, field="reconnect time")
        self._invalidate(BookStateV2.SNAPSHOT_RECOVERY, "RECONNECT_REQUIRES_SNAPSHOT_RECONCILIATION", at_ns)
        return self.sequence_state

    def mark_stale(self, now_ns: int) -> SequenceStateV2:
        timestamp(now_ns, field="staleness check time")
        if self.last_received_at_ns is not None and now_ns - self.last_received_at_ns > self.stale_ns:
            self._invalidate(BookStateV2.INVALID, "STALE_BOOK", now_ns)
        return self.sequence_state

    def apply_fault(self, fault: L2SequenceFaultV2) -> SequenceStateV2:
        if fault.instrument != self.instrument or fault.source_id != self.source_id or fault.channel != self.channel:
            self._invalidate(BookStateV2.INVALID, "WRONG_INSTRUMENT_OR_SOURCE", fault.available_at_ns)
            return self.sequence_state
        self._source_health = fault.source_health
        self._source_health_ref = fault.source_health_ref
        self._invalidate(BookStateV2.GAP_DETECTED, f"SEQUENCE_FAULT_{fault.fault}", fault.available_at_ns)
        return self.sequence_state

    def apply_snapshot(self, snapshot: L2SnapshotV2, *,
                       buffered_deltas: tuple[L2DeltaV2, ...] = ()) -> SequenceStateV2:
        snapshot_capability = capability_for_public_channel_v2(
            self._capability_matrix, snapshot.instrument, snapshot.channel,
        )
        snapshot_is_stream = snapshot.source_id == self.source_id and snapshot.channel == self.channel
        snapshot_is_declared_rest_seed = bool(
            snapshot_capability is not None
            and "snapshot initialization input" in " ".join(snapshot_capability.permitted_uses)
            and snapshot.instrument == self.instrument
        )
        if (snapshot.instrument != self.instrument
                or not (snapshot_is_stream or snapshot_is_declared_rest_seed)):
            self._invalidate(BookStateV2.INVALID, "WRONG_INSTRUMENT_OR_SOURCE", snapshot.available_at_ns)
            return self.sequence_state
        if self._capability_error is not None:
            self._invalidate(BookStateV2.INVALID, self._capability_error, snapshot.available_at_ns)
            return self.sequence_state
        if self._frames and (snapshot.available_at_ns < self._frames[-1][0]
                             or self.last_received_at_ns is not None and snapshot.received_at_ns < self.last_received_at_ns):
            self._invalidate(BookStateV2.INVALID, "OUT_OF_ORDER_SNAPSHOT_RECEIPT", snapshot.available_at_ns)
            return self.sequence_state
        self._source_health = snapshot.source_health
        self._source_health_ref = snapshot.source_health_ref
        if snapshot.sequence_semantics != self.sequence_semantics:
            self._invalidate(BookStateV2.INVALID, "WRONG_SEQUENCE_SEMANTICS", snapshot.available_at_ns)
            return self.sequence_state
        if snapshot_is_declared_rest_seed and snapshot_capability is not None:
            if "snapshot initialization input" not in " ".join(snapshot_capability.permitted_uses):
                self._invalidate(BookStateV2.INVALID, "NOT_ESTIMABLE_SNAPSHOT_CAPABILITY_MISSING",
                                 snapshot.available_at_ns)
                return self.sequence_state
        if snapshot.source_health != "HEALTHY_CURRENT" or snapshot.source_health_ref is None:
            self._invalidate(BookStateV2.INVALID, "SOURCE_NOT_HEALTHY_CURRENT", snapshot.available_at_ns)
            return self.sequence_state
        if snapshot.availability_class != "ACTUAL_SYSTEM":
            self._invalidate(BookStateV2.INVALID, "NOT_ESTIMABLE_RECONSTRUCTED_BOOK_AVAILABILITY",
                             snapshot.available_at_ns)
            return self.sequence_state
        if self.state not in {BookStateV2.COLD, BookStateV2.GAP_DETECTED, BookStateV2.INVALID,
                              BookStateV2.SNAPSHOT_RECOVERY, BookStateV2.WARMING, BookStateV2.VALID}:
            self._invalidate(BookStateV2.INVALID, "SNAPSHOT_REJECTED", snapshot.available_at_ns)
            return self.sequence_state
        self.epoch += 1
        self.bids = {x.price: x.quantity for x in snapshot.bids if x.quantity > 0}
        self.asks = {x.price: x.quantity for x in snapshot.asks if x.quantity > 0}
        self.last_update_id = snapshot.last_update_id
        self.state = BookStateV2.WARMING
        self.state_changed_at_ns = snapshot.available_at_ns
        self._binance_sync_phase = (
            BinanceDepthSyncPhaseV2.AWAITING_SNAPSHOT_BRIDGE
            if self.sequence_semantics == "BINANCE_U_PU" else BinanceDepthSyncPhaseV2.NOT_APPLICABLE
        )
        # A Binance snapshot is not a synchronized book until a diff event
        # bridges its lastUpdateId. Start feature warmup only at that bridge.
        self.valid_since_ns = (None if self.sequence_semantics == "BINANCE_U_PU"
                               else snapshot.available_at_ns)
        self.last_received_at_ns = snapshot.received_at_ns
        self._declared_depth = snapshot.declared_depth
        self._seen.clear()
        self._reason = None
        self._append_frame(snapshot.available_at_ns, snapshot.received_at_ns, snapshot.raw_content_ref,
                           snapshot.event_at_ns)
        self._state_events.append((snapshot.available_at_ns, self.state, self.epoch, self._source_health,
                                   None, self.valid_since_ns, self._source_health_ref))
        if buffered_deltas:
            if not snapshot_is_declared_rest_seed or any(
                delta.received_at_ns > snapshot.received_at_ns
                or delta.available_at_ns > snapshot.available_at_ns
                for delta in buffered_deltas
            ):
                self._invalidate(BookStateV2.INVALID, "INVALID_SNAPSHOT_BUFFER_RECONCILIATION",
                                 snapshot.available_at_ns)
                return self.sequence_state
            for delta in buffered_deltas:
                self._apply_delta(delta, reconciled_at_ns=snapshot.available_at_ns)
                if self.state in {BookStateV2.GAP_DETECTED, BookStateV2.INVALID}:
                    break
        return self.sequence_state

    def _append_frame(self, available_at_ns: int, received_at_ns: int, ref: str,
                      event_at_ns: int | None) -> None:
        bids = tuple(BookLevelV2(p, q) for p, q in sorted(self.bids.items(), reverse=True)[:self._declared_depth])
        asks = tuple(BookLevelV2(p, q) for p, q in sorted(self.asks.items())[:self._declared_depth])
        if not bids or not asks or bids[0].price >= asks[0].price:
            self._invalidate(BookStateV2.INVALID, "EMPTY_OR_CROSSED_BOOK", available_at_ns)
            return
        self._frames.append((available_at_ns, received_at_ns, ref, BboV2.from_book(bids, asks),
                             bids, asks, event_at_ns))
        self._frame_epochs.append(self.epoch)

    def apply_delta(self, delta: L2DeltaV2) -> SequenceStateV2:
        return self._apply_delta(delta, reconciled_at_ns=None)

    def _apply_delta(self, delta: L2DeltaV2, *, reconciled_at_ns: int | None) -> SequenceStateV2:
        processed_at = (timestamp(reconciled_at_ns, field="snapshot_reconciliation_time")
                        if reconciled_at_ns is not None else delta.available_at_ns)
        if processed_at < delta.available_at_ns:
            self._invalidate(BookStateV2.INVALID, "RECONCILIATION_PRECEDES_DELTA_AVAILABILITY", processed_at)
            return self.sequence_state
        if delta.instrument != self.instrument or delta.source_id != self.source_id or delta.channel != self.channel:
            self._invalidate(BookStateV2.INVALID, "WRONG_INSTRUMENT_OR_SOURCE", processed_at)
            return self.sequence_state
        self._source_health = delta.source_health
        self._source_health_ref = delta.source_health_ref
        if delta.source_health != "HEALTHY_CURRENT" or delta.source_health_ref is None:
            self._invalidate(BookStateV2.INVALID, "SOURCE_NOT_HEALTHY_CURRENT", processed_at)
            return self.sequence_state
        if delta.availability_class != "ACTUAL_SYSTEM":
            self._invalidate(BookStateV2.INVALID, "NOT_ESTIMABLE_RECONSTRUCTED_BOOK_AVAILABILITY", processed_at)
            return self.sequence_state
        if (reconciled_at_ns is None and self.last_received_at_ns is not None
                and delta.received_at_ns - self.last_received_at_ns > self.stale_ns):
            self._invalidate(BookStateV2.INVALID, "STALE_FEED_REQUIRES_SNAPSHOT_RECOVERY", processed_at)
            return self.sequence_state
        if delta.sequence_semantics != self.sequence_semantics:
            self._invalidate(BookStateV2.INVALID, "WRONG_SEQUENCE_SEMANTICS", processed_at)
            return self.sequence_state
        if self.state not in {BookStateV2.WARMING, BookStateV2.VALID} or self.last_update_id is None:
            self._invalidate(BookStateV2.GAP_DETECTED, "DELTA_WITHOUT_RECONCILED_SNAPSHOT", processed_at)
            return self.sequence_state
        if delta.reset:
            self._invalidate(BookStateV2.GAP_DETECTED, "EXCHANGE_RESET_REQUIRES_NEW_SNAPSHOT", processed_at)
            return self.sequence_state
        prior_hash = self._seen.get(delta.last_update_id)
        if prior_hash is not None:
            if prior_hash == delta.raw_content_ref:
                return self.sequence_state
            self._invalidate(BookStateV2.GAP_DETECTED, "CONFLICTING_DUPLICATE_DELTA", processed_at)
            return self.sequence_state
        mode = self.sequence_semantics
        previous = self.last_update_id
        if (self._frames and (processed_at < self._frames[-1][0]
                             or reconciled_at_ns is None and self.last_received_at_ns is not None
                             and delta.received_at_ns < self.last_received_at_ns)):
            self._invalidate(BookStateV2.GAP_DETECTED, "OUT_OF_ORDER_RECEIPT_OR_AVAILABILITY", processed_at)
            return self.sequence_state
        if delta.last_update_id < previous:
            if (self.sequence_semantics == "BINANCE_U_PU"
                    and self._binance_sync_phase == BinanceDepthSyncPhaseV2.AWAITING_SNAPSHOT_BRIDGE):
                # USD-M synchronization discards buffered events ending before
                # snapshot L. An event ending exactly at L is still eligible
                # to be the first overlapping bridge.
                return self.sequence_state
            self._invalidate(BookStateV2.GAP_DETECTED, "OUT_OF_ORDER_DELTA", processed_at)
            return self.sequence_state
        if (delta.last_update_id == previous
                and not (self.sequence_semantics == "BINANCE_U_PU"
                         and self._binance_sync_phase == BinanceDepthSyncPhaseV2.AWAITING_SNAPSHOT_BRIDGE)):
            self._invalidate(BookStateV2.GAP_DETECTED, "OUT_OF_ORDER_DELTA", processed_at)
            return self.sequence_state
        if mode == "BINANCE_U_PU":
            if self._binance_sync_phase == BinanceDepthSyncPhaseV2.AWAITING_SNAPSHOT_BRIDGE:
                # Events ending before the snapshot ID were already discarded
                # above. The first processed event must overlap snapshot L.
                contiguous = (delta.first_update_id is not None
                              and delta.first_update_id <= previous <= delta.last_update_id)
            else:
                contiguous = delta.previous_update_id == previous
        elif mode in {"BYBIT_U", "CONTIGUOUS_UPDATE_ID"}:
            contiguous = delta.last_update_id == previous + 1
        else:
            self._invalidate(BookStateV2.GAP_DETECTED, "UNKNOWN_SEQUENCE_SEMANTICS", processed_at)
            return self.sequence_state
        if not contiguous:
            self._invalidate(BookStateV2.GAP_DETECTED, "SEQUENCE_GAP_OR_FAILED_SNAPSHOT_BRIDGE", processed_at)
            return self.sequence_state
        for side, levels in ((self.bids, delta.bids), (self.asks, delta.asks)):
            for level in levels:
                if level.quantity == 0:
                    side.pop(level.price, None)
                else:
                    side[level.price] = level.quantity
        self.last_update_id = delta.last_update_id
        self._seen[delta.last_update_id] = delta.raw_content_ref
        self.last_received_at_ns = max(self.last_received_at_ns or delta.received_at_ns, delta.received_at_ns)
        if mode == "BINANCE_U_PU" and self._binance_sync_phase == BinanceDepthSyncPhaseV2.AWAITING_SNAPSHOT_BRIDGE:
            self._binance_sync_phase = BinanceDepthSyncPhaseV2.BRIDGE_COMPLETE
            self.valid_since_ns = processed_at
        self._append_frame(processed_at, delta.received_at_ns, delta.raw_content_ref,
                           delta.event_at_ns)
        if self.state != BookStateV2.INVALID and self.valid_since_ns is not None:
            if delta.available_at_ns - self.valid_since_ns >= self.warmup_ns:
                self.state = BookStateV2.VALID
            else:
                self.state = BookStateV2.WARMING
        self._state_events.append((processed_at, self.state, self.epoch, self._source_health,
                                   getattr(self, "_reason", None), self.valid_since_ns,
                                   self._source_health_ref))
        return self.sequence_state

    def current_book(self, *, at_ns: int) -> tuple[tuple[BookLevelV2, ...], tuple[BookLevelV2, ...]] | None:
        self.mark_stale(at_ns)
        if self.state != BookStateV2.VALID or not self.bids or not self.asks:
            return None
        return (
            tuple(BookLevelV2(p, q) for p, q in sorted(self.bids.items(), reverse=True)[:self._declared_depth]),
            tuple(BookLevelV2(p, q) for p, q in sorted(self.asks.items())[:self._declared_depth]),
        )

    def feature(self, *, cutoff_ns: int, availability_view: AvailabilityViewV2 = AvailabilityViewV2.ACTUAL_RECEIPT,
                trades: tuple[AggressiveTradeV2, ...] = (), depth_bands_bps: tuple[Decimal, ...] = (Decimal("5"),),
                trade_coverage: FeedCoverageEvidenceV2 | None = None) -> S4FeatureArtifactV2:
        cutoff = timestamp(cutoff_ns, field="cutoff_ns")
        view = AvailabilityViewV2(availability_view)
        trade_coverage_state = trade_coverage.state.value if trade_coverage is not None else FeedCoverageStateV2.NOT_ESTIMABLE.value
        trade_coverage_ref = trade_coverage.content_hash if trade_coverage is not None else None
        coverage_supports_cutoff = bool(
            trade_coverage is not None
            and trade_coverage.instrument == self.instrument
            and trade_coverage.capability_matrix_ref == self._capability_matrix.content_hash
            and trade_coverage.available_at_ns <= cutoff
            and trade_coverage.covered_from_ns <= cutoff - 30_000_000_000
            and trade_coverage.covered_through_ns >= cutoff
        )
        trade_capability = (capability_for_public_channel_v2(
            self._capability_matrix, self.instrument, trade_coverage.channel,
        ) if trade_coverage is not None else None)
        trade_capability_ok = bool(
            trade_capability is not None
            and trade_capability.aggressor_side_convention is not None
            and "signed aggressive" in " ".join(trade_capability.permitted_uses)
        )
        if not trade_capability_ok:
            coverage_supports_cutoff = False
            if trade_coverage is not None:
                trade_coverage_state = FeedCoverageStateV2.NOT_ESTIMABLE.value
        if trade_coverage is not None and trade_coverage_state == FeedCoverageStateV2.QUALIFIED.value and not coverage_supports_cutoff:
            trade_coverage_state = FeedCoverageStateV2.NOT_ESTIMABLE.value
        if view == AvailabilityViewV2.RECONSTRUCTED_MARKET:
            return S4FeatureArtifactV2(
                cutoff_ns=cutoff, availability_view=view, instrument=self.instrument,
                source_id=self.source_id, channel=self.channel, producer_version=S4_FEATURE_VERSION,
                sequence_state=BookStateV2.INVALID, recovery_epoch=self.epoch,
                source_health=self._source_health, source_health_ref=self._source_health_ref,
                capability_matrix_ref=default_evidence_capability_matrix_v2().content_hash,
                declared_cadence_ns=self.declared_cadence_ns, trade_coverage_state=trade_coverage_state,
                trade_coverage_ref=trade_coverage_ref, trade_source_ids=(), trade_side_conventions=(),
                input_refs=(), bbo=None, spread=None, mid=None, microprice=None, depth_bands=(),
                depth_imbalance=(), ofi=None, ofi_windows=(), signed_trade_windows=(),
                flow_price_response_windows=(), price_response_windows=(),
                displayed_liquidity_changes=None, persistence_proxy=None, replenishment_proxy=None,
                opposing_liquidity_side="NOT_ESTIMABLE", opposing_liquidity_persistence=None,
                missing_reason="NOT_ESTIMABLE_RECONSTRUCTED_S4_AVAILABILITY_UNSUPPORTED",
                data_age_ns=None, alignment_uncertainty_ns=None,
            )
        state_rows = [row for row in self._state_events if row[0] <= cutoff]
        asof_state, asof_epoch, asof_health, asof_reason, valid_since, asof_health_ref = (
            (state_rows[-1][1], state_rows[-1][2], state_rows[-1][3], state_rows[-1][4], state_rows[-1][5], state_rows[-1][6])
            if state_rows else (BookStateV2.COLD, 0, "UNKNOWN", "NO_SNAPSHOT", None, None)
        )
        eligible = [frame for frame, epoch in zip(self._frames, self._frame_epochs, strict=True)
                    if frame[0] <= cutoff and epoch == asof_epoch]
        if self._live_retained_from_ns is not None and cutoff < self._live_retained_from_ns:
            asof_state, asof_reason = BookStateV2.INVALID, "NOT_ESTIMABLE_REQUIRES_IMMUTABLE_BOOK_REPLAY"
            eligible = []
        health_refs = {row[6] for row in state_rows if row[2] == asof_epoch and row[6] is not None}
        refs = tuple(sorted({frame[2] for frame in eligible} | health_refs))
        if asof_state == BookStateV2.WARMING and valid_since is not None and cutoff - valid_since >= self.warmup_ns:
            if eligible and cutoff - eligible[-1][0] <= self.stale_ns and asof_health == "HEALTHY_CURRENT":
                asof_state = BookStateV2.VALID
        if eligible and cutoff - eligible[-1][0] > self.stale_ns:
            asof_state, asof_reason = BookStateV2.INVALID, "NOT_ESTIMABLE_STALE_BOOK"
        if asof_state != BookStateV2.VALID:
            return S4FeatureArtifactV2(
                cutoff_ns=cutoff, availability_view=view, instrument=self.instrument,
                source_id=self.source_id, channel=self.channel, producer_version=S4_FEATURE_VERSION,
                sequence_state=asof_state, recovery_epoch=asof_epoch, source_health=asof_health,
                source_health_ref=asof_health_ref,
                capability_matrix_ref=default_evidence_capability_matrix_v2().content_hash,
                declared_cadence_ns=self.declared_cadence_ns, trade_coverage_state=trade_coverage_state,
                trade_coverage_ref=trade_coverage_ref, trade_source_ids=(), trade_side_conventions=(),
                input_refs=refs, bbo=None, spread=None, mid=None, microprice=None,
                depth_bands=(), depth_imbalance=(), ofi=None, signed_trade_windows=(),
                ofi_windows=(), flow_price_response_windows=(),
                price_response_windows=(), displayed_liquidity_changes=None, persistence_proxy=None,
                replenishment_proxy=None,
                opposing_liquidity_side="NOT_ESTIMABLE", opposing_liquidity_persistence=None,
                missing_reason=asof_reason or f"NOT_ESTIMABLE_BOOK_STATE_{asof_state.value}",
                data_age_ns=max(0, cutoff - eligible[-1][1]) if eligible else None,
                alignment_uncertainty_ns=self._alignment_uncertainty_ns(eligible),
            )
        if not eligible:
            reason = "NOT_ESTIMABLE_NO_CUTOFF_KNOWN_BOOK"
            current = None
        else:
            current = eligible[-1]
            if cutoff - current[0] > self.stale_ns:
                reason = "NOT_ESTIMABLE_STALE_BOOK"
                current = None
            else:
                reason = None
        if current is None:
            return S4FeatureArtifactV2(
                cutoff_ns=cutoff, availability_view=view, instrument=self.instrument,
                source_id=self.source_id, channel=self.channel, producer_version=S4_FEATURE_VERSION,
                sequence_state=asof_state, recovery_epoch=asof_epoch, source_health=asof_health,
                source_health_ref=asof_health_ref,
                capability_matrix_ref=default_evidence_capability_matrix_v2().content_hash,
                declared_cadence_ns=self.declared_cadence_ns, trade_coverage_state=trade_coverage_state,
                trade_coverage_ref=trade_coverage_ref, trade_source_ids=(), trade_side_conventions=(),
                input_refs=refs, bbo=None, spread=None, mid=None, microprice=None,
                depth_bands=(), depth_imbalance=(), ofi=None, signed_trade_windows=(),
                ofi_windows=(), flow_price_response_windows=(),
                price_response_windows=(), displayed_liquidity_changes=None, persistence_proxy=None,
                replenishment_proxy=None,
                opposing_liquidity_side="NOT_ESTIMABLE", opposing_liquidity_persistence=None,
                missing_reason=reason or "NOT_ESTIMABLE_NO_CUTOFF_KNOWN_BOOK",
                data_age_ns=max(0, cutoff - eligible[-1][1]) if eligible else None,
                alignment_uncertainty_ns=self._alignment_uncertainty_ns(eligible),
            )
        _, _, _, bbo, bids, asks, _ = current
        band_values: list[tuple[str, str, str]] = []
        imbalance_values: list[tuple[str, str]] = []
        for band in depth_bands_bps:
            band = decimal_value(band, field="depth band")
            if band <= 0:
                raise ValueError("depth bands must be positive")
            bid_depth = sum((x.quantity for x in bids if (bbo.mid - x.price) / bbo.mid * 10000 <= band), Decimal(0))
            ask_depth = sum((x.quantity for x in asks if (x.price - bbo.mid) / bbo.mid * 10000 <= band), Decimal(0))
            band_values.append((str(band), str(bid_depth), str(ask_depth)))
            total_depth = bid_depth + ask_depth
            imbalance_values.append((str(band), str((bid_depth - ask_depth) / total_depth) if total_depth else "NOT_ESTIMABLE"))
        window_features = self._windows(eligible, cutoff)
        ofi_windows = self._ofi_windows(eligible, cutoff, window_features)
        expected_side_convention = ({
            "BYBIT": "BYBIT_S_IS_TAKER_SIDE",
            "BINANCE": "BINANCE_m_TRUE_BUYER_MAKER_SELLER_AGGRESSOR",
        }.get(self.instrument.venue.value))
        eligible_trades = [t for t in trades if t.instrument == self.instrument and t.available_at_ns <= cutoff
                           and t.source_health == "HEALTHY_CURRENT" and t.source_health_ref is not None
                           and t.aggressor_side is not None and t.availability_class == "ACTUAL_SYSTEM"
                           and t.side_convention == expected_side_convention
                           and (trade_coverage is None or
                                (t.source_id == trade_coverage.source_id and t.channel == trade_coverage.channel))]
        trade_features: list[tuple[str, str, str, str]] = []
        for seconds in (1, 5, 30):
            window_start = cutoff - seconds * 1_000_000_000
            sample = [t for t in eligible_trades if window_start < t.available_at_ns <= cutoff]
            buy = sum((t.quantity for t in sample if t.aggressor_side == "BUY"), Decimal(0))
            sell = sum((t.quantity for t in sample if t.aggressor_side == "SELL"), Decimal(0))
            total = buy + sell
            value = (str((buy - sell) / total) if total else "0") if sample and coverage_supports_cutoff and trade_coverage_state == FeedCoverageStateV2.QUALIFIED.value else None
            trade_features.append((str(seconds), str(buy) if sample else "NOT_ESTIMABLE",
                                   str(sell) if sample else "NOT_ESTIMABLE",
                                   value if value is not None else "NOT_ESTIMABLE_TRADE_COVERAGE_UNKNOWN"))
        response_by_window = {row[0]: row for row in window_features}
        flow_response_windows = []
        for seconds in (1, 5, 30):
            window_start = cutoff - seconds * 1_000_000_000
            sample = [t for t in eligible_trades if window_start < t.available_at_ns <= cutoff]
            net = sum((t.quantity if t.aggressor_side == "BUY" else -t.quantity for t in sample), Decimal(0))
            response = response_by_window[str(seconds)]
            if not coverage_supports_cutoff or trade_coverage_state != FeedCoverageStateV2.QUALIFIED.value:
                flow_response_windows.append((str(seconds), "NOT_ESTIMABLE", response[1],
                                              "NOT_ESTIMABLE_TRADE_COVERAGE_UNKNOWN"))
            elif not sample:
                flow_response_windows.append((str(seconds), "0", response[1],
                                              "NOT_ESTIMABLE_NO_AGGRESSIVE_TRADES"))
            elif response[2] != "ESTIMABLE":
                flow_response_windows.append((str(seconds), str(net), "NOT_ESTIMABLE",
                                              "NOT_ESTIMABLE_BOOK_RESPONSE_WINDOW"))
            else:
                flow_response_windows.append((str(seconds), str(net), response[1], "ESTIMABLE"))
        # OFI and displayed changes are computed from consecutive cutoff-known book frames.
        latest_pair = (eligible[-2], eligible[-1]) if len(eligible) >= 2 else None
        ofi = _ofi(latest_pair[0][3], latest_pair[1][3]) if latest_pair else None
        recent_start = cutoff - 30_000_000_000
        recent_frames = [row for row in eligible if row[0] >= recent_start]
        additions, removals, persistence = _display_changes(recent_frames)
        replenishment = _replenishment_proxy(recent_frames)
        flow_30 = next((row for row in flow_response_windows if row[0] == "30"), None)
        opposing_side, opposing_persistence = "NOT_ESTIMABLE", None
        thirty_second_response = next((row for row in window_features if row[0] == "30"), None)
        if (flow_30 is not None and flow_30[3] == "ESTIMABLE"
                and thirty_second_response is not None and thirty_second_response[2] == "ESTIMABLE"):
            signed_net_flow = Decimal(flow_30[1])
            if signed_net_flow != 0 and len(recent_frames) >= 2:
                opposing_side = "ASK" if signed_net_flow > 0 else "BID"
                opposing_persistence = _opposing_display_persistence(recent_frames, opposing_side)
        feature_refs = tuple(sorted(set(refs) | {t.raw_content_ref for t in eligible_trades}
                                     | {t.source_health_ref for t in eligible_trades if t.source_health_ref}
                                     | ({trade_coverage_ref} if trade_coverage_ref else set())
                                     | (set(trade_coverage.input_refs) if trade_coverage else set())
                                     | ({trade_coverage.source_health_ref} if trade_coverage else set())))
        return S4FeatureArtifactV2(
            cutoff_ns=cutoff, availability_view=view, instrument=self.instrument,
            source_id=self.source_id, channel=self.channel, producer_version=S4_FEATURE_VERSION,
            sequence_state=asof_state, recovery_epoch=asof_epoch, source_health=asof_health,
            source_health_ref=asof_health_ref,
            capability_matrix_ref=default_evidence_capability_matrix_v2().content_hash,
            declared_cadence_ns=self.declared_cadence_ns, trade_coverage_state=trade_coverage_state,
            trade_coverage_ref=trade_coverage_ref,
            trade_source_ids=tuple(sorted({t.source_id for t in eligible_trades})),
            trade_side_conventions=tuple(sorted({t.side_convention for t in eligible_trades})),
            input_refs=feature_refs, bbo=(str(bbo.bid_price), str(bbo.ask_price)),
            spread=str(bbo.spread), mid=str(bbo.mid), microprice=str(bbo.microprice),
            depth_bands=tuple(band_values), depth_imbalance=tuple(imbalance_values), ofi=ofi,
            ofi_windows=ofi_windows, signed_trade_windows=tuple(trade_features),
            flow_price_response_windows=tuple(flow_response_windows), price_response_windows=window_features,
            displayed_liquidity_changes=(str(additions), str(removals)),
            persistence_proxy=str(persistence), replenishment_proxy=str(replenishment), missing_reason=None,
            opposing_liquidity_side=opposing_side,
            opposing_liquidity_persistence=str(opposing_persistence) if opposing_persistence is not None else None,
            data_age_ns=max(0, cutoff - current[1]),
            alignment_uncertainty_ns=self._alignment_uncertainty_ns(eligible),
        )

    def _windows(self, eligible: list[BookFrameStateV2], cutoff: int) -> tuple[tuple[str, str, str], ...]:
        result = []
        for seconds in (1, 5, 30):
            start = cutoff - seconds * 1_000_000_000
            baseline = [x for x in eligible if x[0] <= start]
            rows = [x for x in eligible if start < x[0] <= cutoff]
            if self.declared_cadence_ns is None:
                result.append((str(seconds), "NOT_ESTIMABLE", "DECLARED_CADENCE_UNKNOWN"))
                continue
            expected_updates = math.ceil(seconds * 1_000_000_000 / self.declared_cadence_ns)
            minimum_updates = max(1, math.ceil(expected_updates / 2))
            max_gap = min(self.stale_ns, self.declared_cadence_ns * 3)
            path = ([baseline[-1]] if baseline else []) + rows
            support = (bool(baseline) and len(rows) >= minimum_updates
                       and start - baseline[-1][0] <= max_gap and rows[0][0] - start <= max_gap
                       and cutoff - rows[-1][0] <= max_gap
                       and all(right[0] - left[0] <= max_gap
                               for left, right in zip(path, path[1:], strict=False)))
            if not support:
                result.append((str(seconds), "NOT_ESTIMABLE", "INSUFFICIENT_CADENCE_OR_COVERAGE"))
            else:
                delta_mid = rows[-1][3].mid - baseline[-1][3].mid
                result.append((str(seconds), str(delta_mid), "ESTIMABLE"))
        return tuple(result)

    def _alignment_uncertainty_ns(self, rows: list[BookFrameStateV2]) -> int | None:
        aligned = [abs(row[1] - row[6]) for row in rows if row[6] is not None]
        if not aligned:
            return None
        return max(aligned[-30:])

    def _ofi_windows(self, eligible: list[BookFrameStateV2], cutoff: int,
                     price_windows: tuple[tuple[str, str, str], ...]) -> tuple[tuple[str, str, str], ...]:
        result = []
        by_window = {row[0]: row[2] for row in price_windows}
        for seconds in (1, 5, 30):
            start = cutoff - seconds * 1_000_000_000
            baseline = [row for row in eligible if row[0] <= start]
            rows = [row for row in eligible if start < row[0] <= cutoff]
            if by_window[str(seconds)] != "ESTIMABLE" or not baseline or not rows:
                result.append((str(seconds), "NOT_ESTIMABLE", "INSUFFICIENT_CADENCE_OR_COVERAGE"))
                continue
            path = [baseline[-1], *rows]
            aggregate = sum((_ofi(left[3], right[3]) for left, right in zip(path, path[1:], strict=False)), Decimal(0))
            result.append((str(seconds), str(aggregate), "ESTIMABLE"))
        return tuple(result)


def _ofi(previous: BboV2, current: BboV2) -> Decimal:
    bid = (current.bid_quantity - previous.bid_quantity if current.bid_price == previous.bid_price
           else current.bid_quantity if current.bid_price > previous.bid_price else -previous.bid_quantity)
    ask = (-(current.ask_quantity - previous.ask_quantity) if current.ask_price == previous.ask_price
           else -current.ask_quantity if current.ask_price < previous.ask_price else previous.ask_quantity)
    return bid + ask


def _display_changes(frames: list[BookFrameStateV2]) -> tuple[Decimal, Decimal, Decimal]:
    if len(frames) < 2:
        return Decimal(0), Decimal(0), Decimal(0)
    before, after = frames[-2][4] + frames[-2][5], frames[-1][4] + frames[-1][5]
    old = {(x.price): x.quantity for x in before}
    new = {(x.price): x.quantity for x in after}
    additions = sum((max(Decimal(0), q - old.get(p, Decimal(0))) for p, q in new.items()), Decimal(0))
    removals = sum((max(Decimal(0), q - new.get(p, Decimal(0))) for p, q in old.items()), Decimal(0))
    shared_persistence = sum((min(old[p], q) for p, q in new.items() if p in old), Decimal(0))
    return additions, removals, shared_persistence


def _replenishment_proxy(frames: list[BookFrameStateV2]) -> Decimal:
    """Count causal displayed quantity restored at a price after an earlier reduction."""
    restored = Decimal(0)
    pending: dict[Decimal, Decimal] = {}
    for before, after in zip(frames, frames[1:], strict=False):
        old_levels = {x.price: x.quantity for x in before[4] + before[5]}
        new_levels = {x.price: x.quantity for x in after[4] + after[5]}
        for price in set(old_levels) | set(new_levels):
            prior = old_levels.get(price, Decimal(0))
            current = new_levels.get(price, Decimal(0))
            if current < prior:
                pending[price] = pending.get(price, Decimal(0)) + prior - current
            elif current > prior and pending.get(price, Decimal(0)) > 0:
                amount = min(current - prior, pending[price])
                restored += amount
                pending[price] -= amount
    return restored


def _opposing_display_persistence(frames: list[BookFrameStateV2], side_name: str) -> Decimal:
    """Average shared displayed quantity on the side opposing net signed trades."""
    if side_name not in {"BID", "ASK"} or len(frames) < 2:
        return Decimal(0)
    total = Decimal(0)
    count = 0
    for before, after in zip(frames, frames[1:], strict=False):
        old_levels = before[4] if side_name == "BID" else before[5]
        new_levels = after[4] if side_name == "BID" else after[5]
        old = {x.price: x.quantity for x in old_levels}
        new = {x.price: x.quantity for x in new_levels}
        total += sum((min(old[price], new[price]) for price in old.keys() & new.keys()), Decimal(0))
        count += 1
    return total / Decimal(count) if count else Decimal(0)


@dataclass(frozen=True)
class S4MarkoutOutcomeV2:
    feature_ref: str
    feature_cutoff_ns: int
    horizon_ns: int
    matured_at_ns: int
    signed_flow_side: str
    future_mid: Decimal
    adverse_markout: Decimal

    def __post_init__(self) -> None:
        sha256_ref(self.feature_ref, field="feature_ref")
        timestamp(self.feature_cutoff_ns, field="feature_cutoff_ns")
        timestamp(self.matured_at_ns, field="matured_at_ns")
        if type(self.horizon_ns) is not int or self.horizon_ns <= 0:
            raise ValueError("markout horizon must be positive")
        if self.signed_flow_side not in {"BUY", "SELL"}:
            raise ValueError("markout side must be BUY or SELL")
        object.__setattr__(self, "future_mid", decimal_value(self.future_mid, field="future_mid"))
        object.__setattr__(self, "adverse_markout", decimal_value(self.adverse_markout, field="adverse_markout"))
        if self.matured_at_ns < self.feature_cutoff_ns + self.horizon_ns:
            raise ValueError("markout outcome cannot mature before its future horizon")

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, "feature_ref": self.feature_ref,
                "feature_cutoff_ns": self.feature_cutoff_ns, "horizon_ns": self.horizon_ns,
                "matured_at_ns": self.matured_at_ns, "signed_flow_side": self.signed_flow_side,
                "future_mid": str(self.future_mid), "adverse_markout": str(self.adverse_markout),
                "role": "MATURED_OUTCOME_ONLY"}


@dataclass(frozen=True)
class S4FeatureArtifactV2:
    cutoff_ns: int
    availability_view: AvailabilityViewV2
    instrument: InstrumentKeyV2
    source_id: str
    channel: str
    producer_version: str
    sequence_state: BookStateV2
    recovery_epoch: int
    source_health: str
    source_health_ref: str | None
    capability_matrix_ref: str
    declared_cadence_ns: int | None
    trade_coverage_state: str
    trade_coverage_ref: str | None
    trade_source_ids: tuple[str, ...]
    trade_side_conventions: tuple[str, ...]
    input_refs: tuple[str, ...]
    bbo: tuple[str, str] | None
    spread: str | None
    mid: str | None
    microprice: str | None
    depth_bands: tuple[tuple[str, str, str], ...]
    depth_imbalance: tuple[tuple[str, str], ...]
    ofi: Decimal | None
    ofi_windows: tuple[tuple[str, str, str], ...]
    signed_trade_windows: tuple[tuple[str, str, str, str], ...]
    flow_price_response_windows: tuple[tuple[str, str, str, str], ...]
    price_response_windows: tuple[tuple[str, str, str], ...]
    displayed_liquidity_changes: tuple[str, str] | None
    persistence_proxy: str | None
    replenishment_proxy: str | None
    opposing_liquidity_side: str
    opposing_liquidity_persistence: str | None
    missing_reason: str | None
    data_age_ns: int | None
    alignment_uncertainty_ns: int | None

    SCHEMA_VERSION: ClassVar[int] = 1

    def __post_init__(self) -> None:
        timestamp(self.cutoff_ns, field="cutoff_ns")
        object.__setattr__(self, "availability_view", AvailabilityViewV2(self.availability_view))
        object.__setattr__(self, "sequence_state", BookStateV2(self.sequence_state))
        if not isinstance(self.instrument, InstrumentKeyV2):
            raise ValueError("feature requires full instrument key")
        for name in ("source_id", "channel", "producer_version", "source_health"):
            nonblank(getattr(self, name), field=name)
        nonblank(self.opposing_liquidity_side, field="opposing_liquidity_side")
        if self.opposing_liquidity_persistence is not None:
            object.__setattr__(self, "opposing_liquidity_persistence",
                               str(decimal_value(self.opposing_liquidity_persistence,
                                                 field="opposing_liquidity_persistence")))
        if self.source_health_ref is not None:
            sha256_ref(self.source_health_ref, field="source_health_ref")
        sha256_ref(self.capability_matrix_ref, field="capability_matrix_ref")
        if self.declared_cadence_ns is not None and self.declared_cadence_ns <= 0:
            raise ValueError("declared cadence must be positive or absent")
        nonblank(self.trade_coverage_state, field="trade_coverage_state")
        if self.trade_coverage_ref is not None:
            sha256_ref(self.trade_coverage_ref, field="trade_coverage_ref")
        if self.trade_coverage_state == "QUALIFIED" and self.trade_coverage_ref is None:
            raise ValueError("qualified trade coverage must have an exact reference")
        for value in self.trade_source_ids + self.trade_side_conventions:
            nonblank(value, field="trade source/convention")
        if type(self.recovery_epoch) is not int or self.recovery_epoch < 0:
            raise ValueError("recovery epoch must be nonnegative")
        for ref in self.input_refs:
            sha256_ref(ref, field="input_ref")
        if self.ofi is not None:
            object.__setattr__(self, "ofi", decimal_value(self.ofi, field="ofi"))
        if self.missing_reason is not None:
            nonblank(self.missing_reason, field="missing_reason")

    @property
    def estimable(self) -> bool:
        return self.missing_reason is None and self.sequence_state == BookStateV2.VALID

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": self.SCHEMA_VERSION, "cutoff_ns": self.cutoff_ns,
                "availability_view": self.availability_view.value, "instrument": self.instrument.to_dict(),
                "source_id": self.source_id, "channel": self.channel, "producer_version": self.producer_version,
                "producer_policy_hash": S4_FEATURE_POLICY_HASH,
                "sequence_state": self.sequence_state.value, "recovery_epoch": self.recovery_epoch,
                "source_health": self.source_health, "source_health_ref": self.source_health_ref,
                "capability_matrix_ref": self.capability_matrix_ref,
                "declared_cadence_ns": self.declared_cadence_ns,
                "trade_coverage_state": self.trade_coverage_state,
                "trade_coverage_ref": self.trade_coverage_ref,
                "trade_source_ids": list(self.trade_source_ids),
                "trade_side_conventions": list(self.trade_side_conventions),
                "input_refs": list(self.input_refs),
                "bbo": list(self.bbo) if self.bbo else None, "spread": self.spread, "mid": self.mid,
                "microprice": self.microprice, "depth_bands": [list(x) for x in self.depth_bands],
                "depth_imbalance": [list(x) for x in self.depth_imbalance],
                "ofi": str(self.ofi) if self.ofi is not None else None,
                "ofi_windows": [list(x) for x in self.ofi_windows],
                "signed_trade_windows": [list(x) for x in self.signed_trade_windows],
                "flow_price_response_windows": [list(x) for x in self.flow_price_response_windows],
                "price_response_windows": [list(x) for x in self.price_response_windows],
                "displayed_liquidity_changes": list(self.displayed_liquidity_changes) if self.displayed_liquidity_changes else None,
                "persistence_proxy": self.persistence_proxy,
                "replenishment_proxy": self.replenishment_proxy, "missing_reason": self.missing_reason,
                "opposing_liquidity_side": self.opposing_liquidity_side,
                "opposing_liquidity_persistence": self.opposing_liquidity_persistence,
                "data_age_ns": self.data_age_ns, "alignment_uncertainty_ns": self.alignment_uncertainty_ns,
                "role": "DECISION_TIME_FEATURES_ONLY"}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> S4FeatureArtifactV2:
        expected = {"schema_version", "cutoff_ns", "availability_view", "instrument", "source_id", "channel",
            "producer_version", "producer_policy_hash", "sequence_state", "recovery_epoch", "source_health",
            "source_health_ref", "capability_matrix_ref", "declared_cadence_ns", "trade_coverage_state",
            "trade_coverage_ref", "trade_source_ids", "trade_side_conventions", "input_refs", "bbo", "spread",
            "mid", "microprice", "depth_bands", "depth_imbalance", "ofi", "ofi_windows", "signed_trade_windows",
            "flow_price_response_windows", "price_response_windows", "displayed_liquidity_changes",
            "persistence_proxy", "replenishment_proxy", "opposing_liquidity_side", "opposing_liquidity_persistence",
            "missing_reason", "data_age_ns", "alignment_uncertainty_ns", "role"}
        value = strict_fields(data, expected=expected, required=expected, name="S4FeatureArtifactV2")
        if value["schema_version"] != cls.SCHEMA_VERSION or value["producer_policy_hash"] != S4_FEATURE_POLICY_HASH:
            raise ValueError("unsupported S4 feature schema or producer policy")
        return cls(
            cutoff_ns=value["cutoff_ns"], availability_view=AvailabilityViewV2(value["availability_view"]),
            instrument=InstrumentKeyV2.from_dict(value["instrument"]), source_id=value["source_id"],
            channel=value["channel"], producer_version=value["producer_version"],
            sequence_state=BookStateV2(value["sequence_state"]), recovery_epoch=value["recovery_epoch"],
            source_health=value["source_health"], source_health_ref=value["source_health_ref"],
            capability_matrix_ref=value["capability_matrix_ref"], declared_cadence_ns=value["declared_cadence_ns"],
            trade_coverage_state=value["trade_coverage_state"], trade_coverage_ref=value["trade_coverage_ref"],
            trade_source_ids=tuple(value["trade_source_ids"]), trade_side_conventions=tuple(value["trade_side_conventions"]),
            input_refs=tuple(value["input_refs"]), bbo=tuple(value["bbo"]) if value["bbo"] is not None else None,
            spread=value["spread"], mid=value["mid"], microprice=value["microprice"],
            depth_bands=tuple(tuple(row) for row in value["depth_bands"]),
            depth_imbalance=tuple(tuple(row) for row in value["depth_imbalance"]),
            ofi=Decimal(value["ofi"]) if value["ofi"] is not None else None,
            ofi_windows=tuple(tuple(row) for row in value["ofi_windows"]),
            signed_trade_windows=tuple(tuple(row) for row in value["signed_trade_windows"]),
            flow_price_response_windows=tuple(tuple(row) for row in value["flow_price_response_windows"]),
            price_response_windows=tuple(tuple(row) for row in value["price_response_windows"]),
            displayed_liquidity_changes=tuple(value["displayed_liquidity_changes"])
                if value["displayed_liquidity_changes"] is not None else None,
            persistence_proxy=value["persistence_proxy"], replenishment_proxy=value["replenishment_proxy"],
            opposing_liquidity_side=value["opposing_liquidity_side"],
            opposing_liquidity_persistence=value["opposing_liquidity_persistence"],
            missing_reason=value["missing_reason"], data_age_ns=value["data_age_ns"],
            alignment_uncertainty_ns=value["alignment_uncertainty_ns"],
        )

    @property
    def content_hash(self) -> str:
        return sha256_json({"artifact_type": "S4FeatureArtifactV2", "artifact": self.to_dict()})

    def to_canonical_json(self) -> str:
        return canonical_json(self.to_dict())


@dataclass(frozen=True)
class S4AbsorptionHypothesisV2:
    feature_ref: str
    cutoff_ns: int
    state: str
    signed_flow: Decimal | None
    expected_response: Decimal | None
    response_residual: Decimal | None
    opposing_liquidity_persistence: Decimal | None
    evidence_refs: tuple[str, ...]
    model_version: str
    fit_window_ref: str | None
    threshold_status: str
    exact_action_status: str = "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT"

    def __post_init__(self) -> None:
        sha256_ref(self.feature_ref, field="feature_ref")
        timestamp(self.cutoff_ns, field="cutoff_ns")
        if self.state not in {"ABSORPTION_HYPOTHESIS", "NO_ABSORPTION_HYPOTHESIS", "NOT_ESTIMABLE"}:
            raise ValueError("invalid absorption state")
        for name in ("signed_flow", "expected_response", "response_residual", "opposing_liquidity_persistence"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, decimal_value(value, field=name))
        for ref in self.evidence_refs:
            sha256_ref(ref, field="evidence_ref")
        nonblank(self.model_version, field="model_version")
        if self.fit_window_ref is not None:
            sha256_ref(self.fit_window_ref, field="fit_window_ref")
        nonblank(self.threshold_status, field="threshold_status")

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, "policy_id": S4_ABSORPTION_VERSION,
                "policy_hash": S4_ABSORPTION_POLICY_HASH, "feature_ref": self.feature_ref,
                "cutoff_ns": self.cutoff_ns, "state": self.state,
                "signed_flow": str(self.signed_flow) if self.signed_flow is not None else None,
                "expected_response": str(self.expected_response) if self.expected_response is not None else None,
                "response_residual": str(self.response_residual) if self.response_residual is not None else None,
                "opposing_liquidity_persistence": str(self.opposing_liquidity_persistence) if self.opposing_liquidity_persistence is not None else None,
                "evidence_refs": list(self.evidence_refs),
                "model_version": self.model_version, "fit_window_ref": self.fit_window_ref,
                "threshold_status": self.threshold_status, "exact_action_status": self.exact_action_status}

    @property
    def content_hash(self) -> str:
        return sha256_json({"artifact_type": "S4AbsorptionHypothesisV2", "artifact": self.to_dict()})


@dataclass(frozen=True)
class S4ExpectedResponseBaselineV2:
    fit_cutoff_ns: int
    fit_window_start_ns: int
    fit_window_end_ns: int
    input_refs: tuple[str, ...]
    intercept: Decimal
    slope: Decimal
    flow_mean: Decimal
    flow_std: Decimal
    model_version: str = "S4_EXPECTED_RESPONSE_PRIOR_ONLY_V1"

    def __post_init__(self) -> None:
        timestamp(self.fit_cutoff_ns, field="fit_cutoff_ns")
        timestamp(self.fit_window_start_ns, field="fit_window_start_ns")
        timestamp(self.fit_window_end_ns, field="fit_window_end_ns")
        if len(self.input_refs) < 3:
            raise ValueError("expected-response fit requires at least three prior observations")
        if self.fit_window_end_ns < self.fit_window_start_ns or self.fit_window_end_ns >= self.fit_cutoff_ns:
            raise ValueError("expected-response fit window must be chronological and strictly prior to cutoff")
        for ref in self.input_refs:
            sha256_ref(ref, field="fit_input_ref")
        for name in ("intercept", "slope", "flow_mean", "flow_std"):
            object.__setattr__(self, name, decimal_value(getattr(self, name), field=name))
        if self.flow_std <= 0:
            raise ValueError("prior flow standard deviation must be positive")

    @property
    def content_hash(self) -> str:
        return sha256_json({"artifact_type": "S4ExpectedResponseBaselineV2",
                            "baseline": {"fit_cutoff_ns": self.fit_cutoff_ns,
                                         "input_refs": list(self.input_refs),
                                         "fit_window_start_ns": self.fit_window_start_ns,
                                         "fit_window_end_ns": self.fit_window_end_ns,
                                         "intercept": str(self.intercept), "slope": str(self.slope),
                                         "flow_mean": str(self.flow_mean), "flow_std": str(self.flow_std),
                                         "model_version": self.model_version}})


def fit_s4_expected_response_baseline(*, history: tuple[tuple[int, Decimal, Decimal, str], ...],
                                      fit_cutoff_ns: int) -> S4ExpectedResponseBaselineV2 | None:
    """Fit chronological expected response only from records known before fit cutoff."""
    cutoff = timestamp(fit_cutoff_ns, field="fit_cutoff_ns")
    eligible = sorted((row for row in history if row[0] < cutoff), key=lambda row: (row[0], row[3]))
    if len(eligible) < 3:
        return None
    flows = [abs(decimal_value(row[1], field="signed_flow")) for row in eligible]
    responses = [abs(decimal_value(row[2], field="actual_response")) for row in eligible]
    mean = sum(flows, Decimal(0)) / Decimal(len(flows))
    variance = sum(((value - mean) ** 2 for value in flows), Decimal(0)) / Decimal(len(flows))
    std = variance.sqrt()
    if std == 0:
        return None
    response_mean = sum(responses, Decimal(0)) / Decimal(len(responses))
    denom = sum(((value - mean) ** 2 for value in flows), Decimal(0))
    if denom == 0:
        return None
    slope = sum(((flow - mean) * (response - response_mean)
                 for flow, response in zip(flows, responses, strict=True)), Decimal(0)) / denom
    intercept = response_mean - slope * mean
    refs = tuple(row[3] for row in eligible)
    return S4ExpectedResponseBaselineV2(cutoff, eligible[0][0], eligible[-1][0],
                                       refs, intercept, slope, mean, std)


def estimate_s4_absorption(*, feature: S4FeatureArtifactV2,
                           baseline: S4ExpectedResponseBaselineV2 | None,
                           unusual_flow_z_threshold: Decimal = Decimal("1"),
                           residual_threshold: Decimal = Decimal("0"),
                           window_seconds: int = 30) -> S4AbsorptionHypothesisV2:
    """Derive absorption only from bound feature values and a prior-only fit."""
    feature_ref = feature.content_hash
    flow_row = next((row for row in feature.flow_price_response_windows
                     if row[0] == str(window_seconds) and row[3] == "ESTIMABLE"), None)
    response_row = next((row for row in feature.price_response_windows
                         if row[0] == str(window_seconds) and row[2] == "ESTIMABLE"), None)
    evidence_refs = (feature_ref, baseline.content_hash if baseline else None)
    if (not feature.estimable or feature.trade_coverage_state != FeedCoverageStateV2.QUALIFIED.value
            or flow_row is None or response_row is None
            or feature.opposing_liquidity_persistence is None
            or baseline is None or baseline.fit_cutoff_ns >= feature.cutoff_ns
            or window_seconds != 30):
        return S4AbsorptionHypothesisV2(feature_ref, feature.cutoff_ns, "NOT_ESTIMABLE", None, None, None, None, (),
                                        "S4_EXPECTED_RESPONSE_PRIOR_ONLY_V1", None,
                                        "ENGINEERING_RESEARCH_DEFAULT_UNQUALIFIED")
    flow = decimal_value(flow_row[1], field="signed_flow")
    actual = decimal_value(response_row[1], field="actual_response")
    persistence = decimal_value(feature.opposing_liquidity_persistence, field="opposing_liquidity_persistence")
    if feature.opposing_liquidity_side != ("ASK" if flow > 0 else "BID" if flow < 0 else "NOT_ESTIMABLE"):
        return S4AbsorptionHypothesisV2(feature_ref, feature.cutoff_ns, "NOT_ESTIMABLE", None, None, None, None, (),
                                        baseline.model_version, baseline.content_hash,
                                        "OPPOSING_SIDE_NOT_BOUND_TO_NET_TRADE_DIRECTION")
    expected = baseline.intercept + baseline.slope * abs(flow)
    residual = expected - abs(actual)
    surprise = (abs(flow) - baseline.flow_mean) / baseline.flow_std
    threshold = decimal_value(unusual_flow_z_threshold, field="unusual_flow_z_threshold")
    residual_min = decimal_value(residual_threshold, field="residual_threshold")
    state = "ABSORPTION_HYPOTHESIS" if surprise >= threshold and residual >= residual_min and persistence > 0 else "NO_ABSORPTION_HYPOTHESIS"
    typed_refs = tuple(str(ref) for ref in evidence_refs if ref is not None)
    return S4AbsorptionHypothesisV2(feature_ref, feature.cutoff_ns, state, flow, expected, residual, persistence,
                                    typed_refs, baseline.model_version, baseline.content_hash,
                                    "ENGINEERING_RESEARCH_DEFAULT_UNQUALIFIED")


def s4_execution_quality_context(feature: S4FeatureArtifactV2) -> dict[str, Any]:
    return {"version": S4_EXECUTION_CONTEXT_VERSION, "policy_hash": S4_EXECUTION_CONTEXT_POLICY_HASH,
            "feature_ref": feature.content_hash,
            "cutoff_ns": feature.cutoff_ns, "state": "ESTIMABLE" if feature.estimable else "NOT_ESTIMABLE",
            "spread": feature.spread, "depth_bands": [list(x) for x in feature.depth_bands],
            "data_age_ns": feature.data_age_ns, "book_state": feature.sequence_state.value,
            "missing_reason": feature.missing_reason, "selector_influence": "ZERO"}
