"""Bounded, fail-closed continuity evidence for public forward streams.

This module records observations and classifies them. It deliberately leaves
book application to :class:`SequenceValidBookV2` and durable persistence to
the supervisor-owned writer. No observation in this module proves complete
historical trade coverage.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from enum import StrEnum
from functools import cached_property
from typing import Any, ClassVar

from .._serialization import nonblank, sha256_json, sha256_ref, strict_fields, timestamp
from ..instruments import InstrumentKeyV2, ProductContractV2, TradingStatusV2
from .capabilities import capability_for_public_channel_v2, default_evidence_capability_matrix_v2
from .health import PublicSourceHealthV2, PublicSourceStateV2
from .microstructure import (
    AggressiveTradeV2,
    BookStateV2,
    L2DeltaV2,
    L2SequenceFaultV2,
    L2SnapshotV2,
    SequenceValidBookV2,
)
from .public_microstructure_ws import CapturedPublicFrameV2

PUBLIC_STREAM_CONTINUITY_VERSION = "PUBLIC_STREAM_CONTINUITY_V1"
MAX_TRADE_ID_CACHE = 2048
MAX_OBSERVATION_REPLAY_CACHE = 2048
MAX_GAP_REASON_CODES = 64
MAX_SERIALIZED_CACHE_ITEMS = max(MAX_TRADE_ID_CACHE, MAX_OBSERVATION_REPLAY_CACHE)


class PublicStreamObservationKindV1(StrEnum):
    FRAME_RECEIVED = "FRAME_RECEIVED"
    TRADE_OBSERVED = "TRADE_OBSERVED"
    DISCONNECT = "DISCONNECT"
    RECONNECT = "RECONNECT"
    SOURCE_RESTART = "SOURCE_RESTART"
    CONTROLLER_RESTART = "CONTROLLER_RESTART"
    HEARTBEAT_TIMEOUT = "HEARTBEAT_TIMEOUT"
    CONNECTION_ERROR = "CONNECTION_ERROR"
    QUEUE_OVERFLOW = "QUEUE_OVERFLOW"
    MALFORMED_FRAME = "MALFORMED_FRAME"
    BOOK_SNAPSHOT = "BOOK_SNAPSHOT"
    BOOK_DELTA = "BOOK_DELTA"
    BOOK_RESET = "BOOK_RESET"
    BOOK_SEQUENCE_FAULT = "BOOK_SEQUENCE_FAULT"
    METADATA_REVISION_CHANGE = "METADATA_REVISION_CHANGE"


class PublicStreamClassificationV1(StrEnum):
    FRAME_ACCEPTED = "FRAME_ACCEPTED"
    TRADE_ACCEPTED = "TRADE_ACCEPTED"
    DUPLICATE_OBSERVATION = "DUPLICATE_OBSERVATION"
    DUPLICATE_TRADE_IDENTICAL = "DUPLICATE_TRADE_IDENTICAL"
    CONFLICTING_TRADE_ID = "CONFLICTING_TRADE_ID"
    TRADE_ID_HISTORY_UNAVAILABLE = "TRADE_ID_HISTORY_UNAVAILABLE"
    OUT_OF_ORDER_TRADE_EVENT_TIME = "OUT_OF_ORDER_TRADE_EVENT_TIME"
    OUT_OF_ORDER_TRADE_RECEIPT_TIME = "OUT_OF_ORDER_TRADE_RECEIPT_TIME"
    OUT_OF_ORDER_RECEIPT_TIME = "OUT_OF_ORDER_RECEIPT_TIME"
    GAP_RECORDED = "GAP_RECORDED"
    RECOVERY_EPOCH_STARTED = "RECOVERY_EPOCH_STARTED"
    BOOK_EVIDENCE_RECORDED = "BOOK_EVIDENCE_RECORDED"
    METADATA_REVISION_STARTED = "METADATA_REVISION_STARTED"
    OUT_OF_ORDER_AVAILABILITY = "OUT_OF_ORDER_AVAILABILITY"
    STALE_EPOCH_OBSERVATION = "STALE_EPOCH_OBSERVATION"


_EPOCH_START_KINDS = {
    PublicStreamObservationKindV1.RECONNECT,
    PublicStreamObservationKindV1.SOURCE_RESTART,
    PublicStreamObservationKindV1.CONTROLLER_RESTART,
}
_GAP_KINDS = {
    PublicStreamObservationKindV1.DISCONNECT,
    PublicStreamObservationKindV1.HEARTBEAT_TIMEOUT,
    PublicStreamObservationKindV1.CONNECTION_ERROR,
    PublicStreamObservationKindV1.QUEUE_OVERFLOW,
    PublicStreamObservationKindV1.MALFORMED_FRAME,
    PublicStreamObservationKindV1.BOOK_RESET,
    PublicStreamObservationKindV1.BOOK_SEQUENCE_FAULT,
    PublicStreamObservationKindV1.METADATA_REVISION_CHANGE,
}
_TRADE_KINDS = {PublicStreamObservationKindV1.TRADE_OBSERVED}


def _validate_source_identity(instrument: InstrumentKeyV2, source_id: str, channel: str,
                              metadata_ref: str, epoch_id: str) -> None:
    if not isinstance(instrument, InstrumentKeyV2):
        raise ValueError("public stream evidence requires a full instrument identity")
    for name, value in (("source_id", source_id), ("channel", channel),
                        ("metadata_ref", metadata_ref), ("epoch_id", epoch_id)):
        nonblank(value, field=name)


@dataclass(frozen=True)
class PublicStreamObservationV1:
    instrument: InstrumentKeyV2
    source_id: str
    channel: str
    metadata_ref: str
    epoch_id: str
    kind: PublicStreamObservationKindV1
    observed_at_ns: int
    available_at_ns: int
    receipt_at_ns: int | None = None
    event_at_ns: int | None = None
    evidence_ref: str | None = None
    trade_id: str | None = None
    trade_payload_hash: str | None = None
    trade_payload_hash_basis: str | None = None
    sequence_id: int | None = None
    source_health_ref: str | None = None
    source_health_epoch_id: str | None = None
    reason_code: str | None = None

    SCHEMA_VERSION: ClassVar[int] = 1

    def __post_init__(self) -> None:
        _validate_source_identity(self.instrument, self.source_id, self.channel,
                                  self.metadata_ref, self.epoch_id)
        object.__setattr__(self, "kind", PublicStreamObservationKindV1(self.kind))
        timestamp(self.observed_at_ns, field="observed_at_ns")
        timestamp(self.available_at_ns, field="available_at_ns")
        if self.available_at_ns < self.observed_at_ns:
            raise ValueError("observation availability cannot precede observation")
        for name in ("receipt_at_ns", "event_at_ns"):
            value = getattr(self, name)
            if value is not None:
                timestamp(value, field=name)
        if self.receipt_at_ns is not None and self.receipt_at_ns < self.observed_at_ns:
            raise ValueError("frame receipt cannot precede local observation")
        if self.evidence_ref is not None:
            sha256_ref(self.evidence_ref, field="evidence_ref")
        if self.source_health_ref is not None:
            sha256_ref(self.source_health_ref, field="source_health_ref")
        if self.trade_payload_hash is not None:
            sha256_ref(self.trade_payload_hash, field="trade_payload_hash")
        if self.trade_id is not None:
            nonblank(self.trade_id, field="trade_id")
        if (self.trade_id is None) != (self.trade_payload_hash is None):
            raise ValueError("trade identity and payload hash must appear together")
        if (self.trade_id is None) != (self.trade_payload_hash_basis is None):
            raise ValueError("trade identity and payload hash basis must appear together")
        if self.trade_payload_hash_basis is not None:
            nonblank(self.trade_payload_hash_basis, field="trade_payload_hash_basis")
        if self.sequence_id is not None and (type(self.sequence_id) is not int or self.sequence_id < 0):
            raise ValueError("sequence_id must be a nonnegative integer or absent")
        if self.source_health_epoch_id is not None:
            nonblank(self.source_health_epoch_id, field="source_health_epoch_id")
        if self.reason_code is not None:
            nonblank(self.reason_code, field="reason_code")

    @classmethod
    def from_frame(cls, frame: CapturedPublicFrameV2, *, instrument: InstrumentKeyV2,
                   metadata_ref: str, epoch_id: str, source_health_ref: str | None = None,
                   source_health_epoch_id: str | None = None,
                   persisted_at_ns: int | None = None) -> PublicStreamObservationV1:
        _check_frame_identity(frame, instrument)
        available = max(frame.available_at_ns, timestamp(
            frame.available_at_ns if persisted_at_ns is None else persisted_at_ns,
            field="persisted_at_ns",
        ))
        return cls(instrument, frame.source_id, frame.channel, metadata_ref, epoch_id,
                   PublicStreamObservationKindV1.FRAME_RECEIVED, frame.received_at_ns,
                   available, frame.received_at_ns, evidence_ref=frame.raw_payload_hash,
                   source_health_ref=source_health_ref, source_health_epoch_id=source_health_epoch_id)

    @classmethod
    def from_trade(cls, trade: AggressiveTradeV2, *, metadata_ref: str, epoch_id: str,
                   source_health_epoch_id: str | None = None,
                   persisted_at_ns: int | None = None,
                   exact_trade_payload_hash: str | None = None) -> PublicStreamObservationV1:
        if trade.instrument.venue.value != "BYBIT":
            raise ValueError("S32 Bybit trade identity semantics cannot be reused for another venue")
        if trade.side_convention != "BYBIT_S_IS_TAKER_SIDE":
            raise ValueError("Bybit public trade requires the declared S taker-side convention")
        if trade.channel != f"publicTrade.{trade.instrument.native_symbol}":
            raise ValueError("Bybit trade topic does not match the instrument identity")
        available = max(trade.available_at_ns, timestamp(
            trade.available_at_ns if persisted_at_ns is None else persisted_at_ns,
            field="persisted_at_ns",
        ))
        if exact_trade_payload_hash is None:
            payload_hash = sha256_json({
                "instrument": trade.instrument.to_dict(),
                "trade_id_i": trade.trade_id,
                "aggressor_side_from_S": trade.aggressor_side,
                "price_p": str(trade.price),
                "quantity_v": str(trade.quantity),
                "matched_time_T_ns": trade.event_at_ns,
                "side_convention": trade.side_convention,
            })
            payload_hash_basis = "NORMALIZED_PARSED_FIELDS_FALLBACK"
        else:
            payload_hash = sha256_ref(exact_trade_payload_hash, field="exact_trade_payload_hash")
            payload_hash_basis = "CANONICALIZED_PER_TRADE_RAW_ROW_BYTES"
        return cls(trade.instrument, trade.source_id, trade.channel, metadata_ref, epoch_id,
                   PublicStreamObservationKindV1.TRADE_OBSERVED, trade.received_at_ns, available,
                   trade.received_at_ns, trade.event_at_ns, trade.raw_content_ref, trade.trade_id,
                   payload_hash, payload_hash_basis, source_health_ref=trade.source_health_ref,
                   source_health_epoch_id=source_health_epoch_id)

    @classmethod
    def transport(cls, *, instrument: InstrumentKeyV2, source_id: str, channel: str,
                  metadata_ref: str, epoch_id: str, kind: PublicStreamObservationKindV1 | str,
                  observed_at_ns: int, available_at_ns: int | None = None,
                  reason_code: str | None = None,
                  source_health_ref: str | None = None,
                  source_health_epoch_id: str | None = None) -> PublicStreamObservationV1:
        event_kind = PublicStreamObservationKindV1(kind)
        allowed = _GAP_KINDS | _EPOCH_START_KINDS
        if event_kind not in allowed:
            raise ValueError("transport event must be a declared disconnect, epoch or gap event")
        return cls(instrument, source_id, channel, metadata_ref, epoch_id, event_kind,
                   observed_at_ns, observed_at_ns if available_at_ns is None else available_at_ns,
                   evidence_ref=None, source_health_ref=source_health_ref,
                   source_health_epoch_id=source_health_epoch_id,
                   reason_code=reason_code or _default_reason(event_kind))

    @classmethod
    def from_book_event(cls, event: L2SnapshotV2 | L2DeltaV2 | L2SequenceFaultV2, *,
                        metadata_ref: str, epoch_id: str,
                        persisted_at_ns: int | None = None) -> PublicStreamObservationV1:
        capability = capability_for_public_channel_v2(
            default_evidence_capability_matrix_v2(), event.instrument, event.channel,
        )
        if (capability is None
                or "sequence-valid displayed depth" not in " ".join(capability.permitted_uses)):
            raise ValueError("book observation topic does not match a declared L2 channel")
        available = max(event.available_at_ns, timestamp(
            event.available_at_ns if persisted_at_ns is None else persisted_at_ns,
            field="persisted_at_ns",
        ))
        if isinstance(event, L2SequenceFaultV2):
            kind = PublicStreamObservationKindV1.BOOK_SEQUENCE_FAULT
            sequence_id = None
            reason = f"SEQUENCE_FAULT_{event.fault}"
        elif isinstance(event, L2SnapshotV2):
            kind = PublicStreamObservationKindV1.BOOK_SNAPSHOT
            sequence_id = event.last_update_id
            reason = None
        elif event.reset:
            kind = PublicStreamObservationKindV1.BOOK_RESET
            sequence_id = event.last_update_id
            reason = "EXCHANGE_RESET_REQUIRES_NEW_SNAPSHOT"
        else:
            kind = PublicStreamObservationKindV1.BOOK_DELTA
            sequence_id = event.last_update_id
            reason = None
        return cls(event.instrument, event.source_id, event.channel, metadata_ref, epoch_id,
                   kind, event.received_at_ns, available, event.received_at_ns,
                   event.event_at_ns, event.raw_content_ref, sequence_id=sequence_id,
                   source_health_ref=event.source_health_ref, reason_code=reason)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.SCHEMA_VERSION,
            "instrument": self.instrument.to_dict(),
            "source_id": self.source_id,
            "channel": self.channel,
            "metadata_ref": self.metadata_ref,
            "epoch_id": self.epoch_id,
            "kind": self.kind.value,
            "observed_at_ns": self.observed_at_ns,
            "available_at_ns": self.available_at_ns,
            "receipt_at_ns": self.receipt_at_ns,
            "event_at_ns": self.event_at_ns,
            "evidence_ref": self.evidence_ref,
            "trade_id": self.trade_id,
            "trade_payload_hash": self.trade_payload_hash,
            "trade_payload_hash_basis": self.trade_payload_hash_basis,
            "sequence_id": self.sequence_id,
            "source_health_ref": self.source_health_ref,
            "source_health_epoch_id": self.source_health_epoch_id,
            "reason_code": self.reason_code,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PublicStreamObservationV1:
        fields = {"schema_version", "instrument", "source_id", "channel", "metadata_ref", "epoch_id",
                  "kind", "observed_at_ns", "available_at_ns", "receipt_at_ns", "event_at_ns",
                  "evidence_ref", "trade_id", "trade_payload_hash", "trade_payload_hash_basis",
                  "sequence_id", "source_health_ref",
                  "source_health_epoch_id", "reason_code"}
        value = strict_fields(data, expected=fields, required=fields, name=cls.__name__)
        if type(value["schema_version"]) is not int or value["schema_version"] != cls.SCHEMA_VERSION:
            raise ValueError("unsupported public stream observation schema")
        return cls(InstrumentKeyV2.from_dict(value["instrument"]), value["source_id"], value["channel"],
                   value["metadata_ref"], value["epoch_id"], PublicStreamObservationKindV1(value["kind"]),
                   value["observed_at_ns"], value["available_at_ns"], value["receipt_at_ns"],
                   value["event_at_ns"], value["evidence_ref"], value["trade_id"],
                   value["trade_payload_hash"], value["trade_payload_hash_basis"], value["sequence_id"],
                   value["source_health_ref"],
                   value["source_health_epoch_id"], value["reason_code"])

    @cached_property
    def content_hash(self) -> str:
        return sha256_json({"artifact_type": "PublicStreamObservationV1", "observation": self.to_dict()})

    @cached_property
    def idempotency_ref(self) -> str:
        """Identity of a receipt for replay checks, excluding later persistence time."""
        return sha256_json({
            "artifact_type": "PublicStreamObservationIdentityV1",
            "instrument": self.instrument.to_dict(),
            "source_id": self.source_id,
            "channel": self.channel,
            "metadata_ref": self.metadata_ref,
            "epoch_id": self.epoch_id,
            "kind": self.kind.value,
            "observed_at_ns": self.observed_at_ns,
            "receipt_at_ns": self.receipt_at_ns,
            "event_at_ns": self.event_at_ns,
            "evidence_ref": self.evidence_ref,
            "trade_id": self.trade_id,
            "trade_payload_hash": self.trade_payload_hash,
            "sequence_id": self.sequence_id,
            "reason_code": self.reason_code,
        })


def _check_frame_identity(frame: CapturedPublicFrameV2, instrument: InstrumentKeyV2) -> None:
    if frame.venue != instrument.venue:
        raise ValueError("public frame venue does not match full instrument identity")
    capability = capability_for_public_channel_v2(default_evidence_capability_matrix_v2(), instrument, frame.channel)
    if capability is None:
        raise ValueError("public frame channel is absent from the exact capability matrix")
    try:
        payload = json.loads(frame.raw_payload_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("malformed frame must be recorded as MALFORMED_FRAME, not FRAME_RECEIVED") from exc
    if not isinstance(payload, dict):
        raise ValueError("public frame JSON body must be an object")
    topic = payload.get("topic", payload.get("stream"))
    if topic != frame.channel:
        raise ValueError("public frame topic does not exactly match its declared channel")


def _default_reason(kind: PublicStreamObservationKindV1) -> str:
    return {
        PublicStreamObservationKindV1.DISCONNECT: "DISCONNECT_TRADE_INTERVAL_UNREPAIRABLE",
        PublicStreamObservationKindV1.RECONNECT: "RECONNECT_TRADE_INTERVAL_UNREPAIRABLE",
        PublicStreamObservationKindV1.SOURCE_RESTART: "SOURCE_RESTART_TRADE_INTERVAL_UNREPAIRABLE",
        PublicStreamObservationKindV1.CONTROLLER_RESTART: "CONTROLLER_RESTART_UNACKNOWLEDGED_BUFFER_INTERVAL",
        PublicStreamObservationKindV1.HEARTBEAT_TIMEOUT: "HEARTBEAT_TIMEOUT_INTERVAL_UNREPAIRABLE",
        PublicStreamObservationKindV1.CONNECTION_ERROR: "CONNECTION_ERROR_INTERVAL_UNREPAIRABLE",
        PublicStreamObservationKindV1.QUEUE_OVERFLOW: "QUEUE_OVERFLOW_LOCAL_DATA_LOSS",
        PublicStreamObservationKindV1.MALFORMED_FRAME: "MALFORMED_PUBLIC_FRAME_UNPARSEABLE",
        PublicStreamObservationKindV1.BOOK_RESET: "EXCHANGE_RESET_REQUIRES_NEW_SNAPSHOT",
        PublicStreamObservationKindV1.BOOK_SEQUENCE_FAULT: "BOOK_SEQUENCE_FAULT_REQUIRES_NEW_SNAPSHOT",
        PublicStreamObservationKindV1.METADATA_REVISION_CHANGE: "METADATA_REVISION_CHANGED_REQUIRES_FRESH_FEED_STATE",
    }.get(kind, "UNSPECIFIED_STREAM_GAP")


class _ValidatedTradeCache(tuple[tuple[str, str], ...]):
    """Immutable internal cache; validate new rows once, including on restore."""

    def append_row(self, row: tuple[str, str]) -> _ValidatedTradeCache:
        nonblank(row[0], field="trade_id")
        sha256_ref(row[1], field="trade_payload_hash")
        return tuple.__new__(_ValidatedTradeCache, (*self, row)[-MAX_TRADE_ID_CACHE:])


class _ValidatedReplayCache(tuple[str, ...]):
    def append_ref(self, ref: str) -> _ValidatedReplayCache:
        sha256_ref(ref, field="observation_hash")
        return tuple.__new__(_ValidatedReplayCache, (*self, ref)[-MAX_OBSERVATION_REPLAY_CACHE:])


@dataclass(frozen=True)
class PublicStreamContinuityStateV1:
    instrument: InstrumentKeyV2
    source_id: str
    channel: str
    metadata_ref: str
    epoch_id: str
    recovery_epoch: int
    prior_recovery_ref: str | None
    current_recovery_ref: str
    last_available_at_ns: int | None = None
    last_transport_receipt_at_ns: int | None = None
    last_trade_receipt_at_ns: int | None = None
    last_trade_event_at_ns: int | None = None
    observed_trade_count: int = 0
    trade_identity_cache: tuple[tuple[str, str], ...] = ()
    trade_identity_cache_complete: bool = True
    observation_replay_cache: tuple[str, ...] = ()
    gap_reason_codes: tuple[str, ...] = ()
    gap_count: int = 0
    transport_disconnected: bool = False

    SCHEMA_VERSION: ClassVar[int] = 1

    def __post_init__(self) -> None:
        # Own immutable copies before caching this state's exact identity. Wire
        # decoders already supply tuples; direct callers may supply arrays.
        trade_cache_validated = isinstance(self.trade_identity_cache, _ValidatedTradeCache)
        replay_cache_validated = isinstance(self.observation_replay_cache, _ValidatedReplayCache)
        if not trade_cache_validated:
            object.__setattr__(self, "trade_identity_cache", tuple(tuple(row) for row in self.trade_identity_cache))
        if not replay_cache_validated:
            object.__setattr__(self, "observation_replay_cache", tuple(self.observation_replay_cache))
        object.__setattr__(self, "gap_reason_codes", tuple(self.gap_reason_codes))
        _validate_source_identity(self.instrument, self.source_id, self.channel,
                                  self.metadata_ref, self.epoch_id)
        if type(self.recovery_epoch) is not int or self.recovery_epoch < 0:
            raise ValueError("recovery_epoch must be nonnegative")
        if type(self.observed_trade_count) is not int or self.observed_trade_count < 0:
            raise ValueError("observed_trade_count must be nonnegative")
        if type(self.gap_count) is not int or self.gap_count < 0:
            raise ValueError("gap_count must be nonnegative")
        for name in ("last_available_at_ns", "last_transport_receipt_at_ns",
                     "last_trade_receipt_at_ns", "last_trade_event_at_ns"):
            value = getattr(self, name)
            if value is not None:
                timestamp(value, field=name)
        if self.prior_recovery_ref is not None:
            sha256_ref(self.prior_recovery_ref, field="prior_recovery_ref")
        sha256_ref(self.current_recovery_ref, field="current_recovery_ref")
        if len(self.trade_identity_cache) > MAX_TRADE_ID_CACHE:
            raise ValueError("trade identity cache exceeds strict bound")
        if len(self.observation_replay_cache) > MAX_OBSERVATION_REPLAY_CACHE:
            raise ValueError("observation replay cache exceeds strict bound")
        if not trade_cache_validated:
            for trade_id, payload_hash in self.trade_identity_cache:
                nonblank(trade_id, field="trade_id")
                sha256_ref(payload_hash, field="trade_payload_hash")
            if len({trade_id for trade_id, _ in self.trade_identity_cache}) != len(self.trade_identity_cache):
                raise ValueError("trade identity cache keys must be unique")
            object.__setattr__(self, "trade_identity_cache", tuple.__new__(_ValidatedTradeCache, self.trade_identity_cache))
        if not replay_cache_validated:
            for ref in self.observation_replay_cache:
                sha256_ref(ref, field="observation_hash")
            object.__setattr__(self, "observation_replay_cache", tuple.__new__(_ValidatedReplayCache, self.observation_replay_cache))
        if len(self.gap_reason_codes) > MAX_GAP_REASON_CODES:
            raise ValueError("gap reason list exceeds strict bound")
        for reason in self.gap_reason_codes:
            nonblank(reason, field="gap_reason_code")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.SCHEMA_VERSION,
            "continuity_version": PUBLIC_STREAM_CONTINUITY_VERSION,
            "instrument": self.instrument.to_dict(),
            "source_id": self.source_id,
            "channel": self.channel,
            "metadata_ref": self.metadata_ref,
            "epoch_id": self.epoch_id,
            "recovery_epoch": self.recovery_epoch,
            "prior_recovery_ref": self.prior_recovery_ref,
            "current_recovery_ref": self.current_recovery_ref,
            "last_available_at_ns": self.last_available_at_ns,
            "last_transport_receipt_at_ns": self.last_transport_receipt_at_ns,
            "last_trade_receipt_at_ns": self.last_trade_receipt_at_ns,
            "last_trade_event_at_ns": self.last_trade_event_at_ns,
            "observed_trade_count": self.observed_trade_count,
            "trade_identity_cache": [list(row) for row in self.trade_identity_cache],
            "trade_identity_cache_complete": self.trade_identity_cache_complete,
            "observation_replay_cache": list(self.observation_replay_cache),
            "gap_reason_codes": list(self.gap_reason_codes),
            "gap_count": self.gap_count,
            "transport_disconnected": self.transport_disconnected,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PublicStreamContinuityStateV1:
        fields = {"schema_version", "continuity_version", "instrument", "source_id", "channel", "metadata_ref",
                  "epoch_id", "recovery_epoch", "prior_recovery_ref", "current_recovery_ref",
                  "last_available_at_ns", "last_transport_receipt_at_ns", "last_trade_receipt_at_ns",
                  "last_trade_event_at_ns", "observed_trade_count", "trade_identity_cache",
                  "trade_identity_cache_complete", "observation_replay_cache", "gap_reason_codes",
                  "gap_count", "transport_disconnected"}
        value = strict_fields(data, expected=fields, required=fields, name=cls.__name__)
        if (type(value["schema_version"]) is not int or value["schema_version"] != cls.SCHEMA_VERSION
                or value["continuity_version"] != PUBLIC_STREAM_CONTINUITY_VERSION):
            raise ValueError("unsupported public stream continuity state version")
        return cls(
            InstrumentKeyV2.from_dict(value["instrument"]), value["source_id"], value["channel"],
            value["metadata_ref"], value["epoch_id"], value["recovery_epoch"],
            value["prior_recovery_ref"], value["current_recovery_ref"],
            value["last_available_at_ns"], value["last_transport_receipt_at_ns"],
            value["last_trade_receipt_at_ns"], value["last_trade_event_at_ns"],
            value["observed_trade_count"], tuple(tuple(row) for row in value["trade_identity_cache"]),
            value["trade_identity_cache_complete"], tuple(value["observation_replay_cache"]),
            tuple(value["gap_reason_codes"]), value["gap_count"], value["transport_disconnected"],
        )

    @cached_property
    def content_hash(self) -> str:
        return sha256_json({"artifact_type": "PublicStreamContinuityStateV1", "state": self.to_dict()})


@dataclass(frozen=True)
class PublicStreamContinuityDecisionV1:
    observation_ref: str
    classification: PublicStreamClassificationV1
    reason_code: str | None
    state_ref: str

    def __post_init__(self) -> None:
        sha256_ref(self.observation_ref, field="observation_ref")
        sha256_ref(self.state_ref, field="state_ref")
        object.__setattr__(self, "classification", PublicStreamClassificationV1(self.classification))
        if self.reason_code is not None:
            nonblank(self.reason_code, field="reason_code")

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, "observation_ref": self.observation_ref,
                "classification": self.classification.value, "reason_code": self.reason_code,
                "state_ref": self.state_ref}

    @property
    def content_hash(self) -> str:
        return sha256_json({"artifact_type": "PublicStreamContinuityDecisionV1", "decision": self.to_dict()})


@dataclass(frozen=True)
class _DeferredContinuityDecision:
    """Operational transition; seal the existing wire contract only on use.

    The captured state is immutable. Ordinary transitions need classification
    only; faults, external callers and serializers still receive exact hashes.
    """

    observation: PublicStreamObservationV1 | str
    classification: PublicStreamClassificationV1
    reason_code: str | None
    state: PublicStreamContinuityStateV1

    def seal(self, *, state_ref: str | None = None) -> PublicStreamContinuityDecisionV1:
        ref = self.observation if isinstance(self.observation, str) else self.observation.content_hash
        resolved_state_ref = self.state.content_hash if state_ref is None else sha256_ref(
            state_ref, field="state_ref")
        return PublicStreamContinuityDecisionV1(ref, self.classification,
                                               self.reason_code, resolved_state_ref)

    def to_dict(self) -> dict[str, Any]:
        return self.seal().to_dict()


class PublicStreamContinuityTrackerV1:
    """Apply one immutable observation at a time to a strictly bounded state."""

    def __init__(self, *, instrument: InstrumentKeyV2, source_id: str, channel: str,
                 metadata_ref: str, epoch_id: str, state: PublicStreamContinuityStateV1 | None = None,
                 prior_recovery_ref: str | None = None) -> None:
        _validate_source_identity(instrument, source_id, channel, metadata_ref, epoch_id)
        if capability_for_public_channel_v2(default_evidence_capability_matrix_v2(), instrument, channel) is None:
            raise ValueError("tracker channel is absent from the exact public capability matrix")
        if state is None:
            initial_ref = sha256_json({"continuity_version": PUBLIC_STREAM_CONTINUITY_VERSION,
                                       "instrument": instrument.to_dict(), "source_id": source_id,
                                       "channel": channel, "metadata_ref": metadata_ref,
                                       "epoch_id": epoch_id, "prior_recovery_ref": prior_recovery_ref})
            self._state = PublicStreamContinuityStateV1(
                instrument, source_id, channel, metadata_ref, epoch_id, 0,
                prior_recovery_ref, initial_ref,
            )
        else:
            if (state.instrument, state.source_id, state.channel, state.metadata_ref, state.epoch_id) != (
                instrument, source_id, channel, metadata_ref, epoch_id
            ):
                raise ValueError("restored tracker state identity does not match requested source revision/epoch")
            self._state = state
        self._trade_identity_lookup = dict(self._state.trade_identity_cache)
        self._slice_trade_cache: list[tuple[str, str]] | None = None
        self._slice_replay_cache: list[str] | None = None
        self._slice_trade_cache_complete: bool | None = None

    @property
    def state(self) -> PublicStreamContinuityStateV1:
        return self._state

    def to_state(self) -> PublicStreamContinuityStateV1:
        return self._state

    @classmethod
    def from_state(cls, state: PublicStreamContinuityStateV1) -> PublicStreamContinuityTrackerV1:
        return cls(instrument=state.instrument, source_id=state.source_id, channel=state.channel,
                   metadata_ref=state.metadata_ref, epoch_id=state.epoch_id, state=state)

    def rebind_metadata(self, *, instrument: InstrumentKeyV2, metadata_ref: str,
                        epoch_id: str, observed_at_ns: int) -> tuple[PublicStreamContinuityTrackerV1,
                                                                    PublicStreamObservationV1]:
        """Start a new empty revision partition while retaining only ancestry."""
        timestamp(observed_at_ns, field="observed_at_ns")
        new_tracker = PublicStreamContinuityTrackerV1(
            instrument=instrument, source_id=self._state.source_id, channel=self._state.channel,
            metadata_ref=metadata_ref, epoch_id=epoch_id,
            prior_recovery_ref=self._state.current_recovery_ref,
        )
        observation = PublicStreamObservationV1.transport(
            instrument=instrument, source_id=self._state.source_id, channel=self._state.channel,
            metadata_ref=metadata_ref, epoch_id=epoch_id,
            kind=PublicStreamObservationKindV1.METADATA_REVISION_CHANGE,
            observed_at_ns=observed_at_ns, reason_code="METADATA_REVISION_CHANGED_REQUIRES_FRESH_FEED_STATE",
        )
        # Mark this first new partition with the prior state hash and revision reason.
        new_tracker._state = replace(new_tracker._state, prior_recovery_ref=self._state.current_recovery_ref)
        return new_tracker, observation

    def apply(self, observation: PublicStreamObservationV1, *,
              durable_prior_payload_hash: str | None = None,
              durable_lookup_complete: bool = False,
              durable_identity_conflicted: bool = False) -> PublicStreamContinuityDecisionV1:
        return self.apply_deferred(observation, durable_prior_payload_hash=durable_prior_payload_hash,
                                   durable_lookup_complete=durable_lookup_complete,
                                   durable_identity_conflicted=durable_identity_conflicted).seal()

    def apply_deferred(self, observation: PublicStreamObservationV1, *,
                       durable_prior_payload_hash: str | None = None,
                       durable_lookup_complete: bool = False,
                       durable_identity_conflicted: bool = False) -> _DeferredContinuityDecision:
        state = self._state
        if (observation.instrument, observation.source_id, observation.channel, observation.metadata_ref) != (
            state.instrument, state.source_id, state.channel, state.metadata_ref
        ):
            raise ValueError("observation does not match tracker instrument/source/channel/metadata revision")
        if durable_prior_payload_hash is not None:
            sha256_ref(durable_prior_payload_hash, field="durable_prior_payload_hash")
        if type(durable_lookup_complete) is not bool:
            raise ValueError("durable lookup completion must be explicit boolean")
        if type(durable_identity_conflicted) is not bool:
            raise ValueError("durable trade identity conflict must be explicit boolean")
        event_hash = observation.idempotency_ref
        replay_cache = (self._slice_replay_cache if self._slice_replay_cache is not None
                        else state.observation_replay_cache)
        if event_hash in replay_cache:
            return _DeferredContinuityDecision(
                event_hash, PublicStreamClassificationV1.DUPLICATE_OBSERVATION,
                "IDEMPOTENT_OBSERVATION_REPLAY", state,
            )
        if observation.epoch_id != state.epoch_id:
            if observation.kind not in _EPOCH_START_KINDS:
                return _DeferredContinuityDecision(
                    event_hash, PublicStreamClassificationV1.STALE_EPOCH_OBSERVATION,
                    "OBSERVATION_EPOCH_DOES_NOT_MATCH_ACTIVE_STREAM", state,
                )
            if observation.available_at_ns < (state.last_available_at_ns or 0):
                return _DeferredContinuityDecision(
                    event_hash, PublicStreamClassificationV1.OUT_OF_ORDER_AVAILABILITY,
                    "EPOCH_TRANSITION_ARRIVED_BEFORE_CURRENT_STATE", state,
                )
            self._start_epoch(observation, event_hash)
            return self._decision(observation, PublicStreamClassificationV1.RECOVERY_EPOCH_STARTED,
                                  observation.reason_code)
        if observation.available_at_ns < (state.last_available_at_ns or 0):
            return _DeferredContinuityDecision(
                event_hash, PublicStreamClassificationV1.OUT_OF_ORDER_AVAILABILITY,
                "OBSERVATION_AVAILABILITY_PRECEDES_ACTIVE_STATE", state,
            )

        self._remember_observation(event_hash)
        state = self._state
        if observation.kind in _TRADE_KINDS:
            decision = self._apply_trade(observation, durable_prior_payload_hash, durable_lookup_complete,
                                         durable_identity_conflicted)
            return decision
        if observation.kind in _GAP_KINDS:
            self._add_gap(observation.reason_code or _default_reason(observation.kind))
            state = self._state
            self._state = replace(
                state,
                last_available_at_ns=observation.available_at_ns,
                transport_disconnected=(observation.kind in {
                    PublicStreamObservationKindV1.DISCONNECT,
                    PublicStreamObservationKindV1.HEARTBEAT_TIMEOUT,
                    PublicStreamObservationKindV1.CONNECTION_ERROR,
                }) or state.transport_disconnected,
            )
            return self._decision(observation, PublicStreamClassificationV1.GAP_RECORDED,
                                  observation.reason_code or _default_reason(observation.kind))
        if observation.kind == PublicStreamObservationKindV1.FRAME_RECEIVED:
            receipt_out_of_order = bool(
                observation.receipt_at_ns is not None
                and self._state.last_transport_receipt_at_ns is not None
                and observation.receipt_at_ns < self._state.last_transport_receipt_at_ns
            )
            if receipt_out_of_order:
                self._add_gap("OUT_OF_ORDER_RECEIPT_TIME")
            previous_receipt = self._state.last_transport_receipt_at_ns
            new_receipt = observation.receipt_at_ns
            self._state = replace(self._state, last_available_at_ns=observation.available_at_ns,
                                  last_transport_receipt_at_ns=(
                                      max(previous_receipt, new_receipt)
                                      if previous_receipt is not None and new_receipt is not None
                                      else new_receipt or previous_receipt
                                  ),
                                  transport_disconnected=False)
            return self._decision(
                observation,
                PublicStreamClassificationV1.OUT_OF_ORDER_RECEIPT_TIME if receipt_out_of_order
                else PublicStreamClassificationV1.FRAME_ACCEPTED,
                "FRAME_RECEIPT_TIME_REGRESSED" if receipt_out_of_order else None,
            )
        if observation.kind in {PublicStreamObservationKindV1.BOOK_SNAPSHOT,
                                 PublicStreamObservationKindV1.BOOK_DELTA}:
            receipt_out_of_order = bool(
                observation.receipt_at_ns is not None
                and self._state.last_transport_receipt_at_ns is not None
                and observation.receipt_at_ns < self._state.last_transport_receipt_at_ns
            )
            if receipt_out_of_order:
                self._add_gap("OUT_OF_ORDER_RECEIPT_TIME")
            previous_receipt = self._state.last_transport_receipt_at_ns
            new_receipt = observation.receipt_at_ns
            self._state = replace(self._state, last_available_at_ns=observation.available_at_ns,
                                  last_transport_receipt_at_ns=(
                                      max(previous_receipt, new_receipt)
                                      if previous_receipt is not None and new_receipt is not None
                                      else new_receipt or previous_receipt
                                  ),
                                  transport_disconnected=False)
            return self._decision(
                observation,
                PublicStreamClassificationV1.OUT_OF_ORDER_RECEIPT_TIME if receipt_out_of_order
                else PublicStreamClassificationV1.BOOK_EVIDENCE_RECORDED,
                "BOOK_RECEIPT_TIME_REGRESSED" if receipt_out_of_order else None,
            )
        raise ValueError(f"observation kind is not valid in the active continuity epoch: {observation.kind}")

    def apply_slice_v2(
        self,
        observations: Sequence[PublicStreamObservationV1],
        *,
        durable_identities: Mapping[str, tuple[str | None, bool, bool]] | None = None,
    ) -> tuple[_DeferredContinuityDecision, ...]:
        """Apply an ordered writer-owned slice while retaining V1 transition semantics.

        Each durable identity value is (prior payload hash, lookup complete,
        sticky conflict). The bounded kernel owns its tracker and never moves
        classification, chronology, or trade authority to a preparation worker.
        """
        if not isinstance(observations, (tuple, list)) or not 1 <= len(observations) <= 64:
            raise ValueError("public continuity slice must contain 1..64 ordered observations")
        identities = durable_identities or {}
        if not isinstance(identities, Mapping):
            raise ValueError("public continuity durable identity binding is malformed")
        for observation in observations:
            if not isinstance(observation, PublicStreamObservationV1):
                raise ValueError("public continuity slice contains an untyped observation")
            if (observation.instrument, observation.source_id, observation.channel, observation.metadata_ref) != (
                self._state.instrument, self._state.source_id, self._state.channel, self._state.metadata_ref
            ):
                raise ValueError("public continuity slice crosses a feed or product revision")
        if self._slice_trade_cache is not None or self._slice_replay_cache is not None:
            raise RuntimeError("public continuity writer slice cannot be nested")
        original_state = self._state
        original_lookup = self._trade_identity_lookup.copy()
        self._slice_trade_cache = list(original_state.trade_identity_cache)
        self._slice_replay_cache = list(original_state.observation_replay_cache)
        self._slice_trade_cache_complete = original_state.trade_identity_cache_complete
        self._state = replace(original_state, trade_identity_cache=(), observation_replay_cache=())
        output: list[_DeferredContinuityDecision] = []
        try:
            for observation in observations:
                binding = identities.get(observation.trade_id or "", (None, False, False))
                if (not isinstance(binding, tuple) or len(binding) != 3
                        or type(binding[1]) is not bool or type(binding[2]) is not bool):
                    raise ValueError("public continuity slice durable identity binding is malformed")
                output.append(self.apply_deferred(
                    observation, durable_prior_payload_hash=binding[0],
                    durable_lookup_complete=binding[1],
                    durable_identity_conflicted=binding[2],
                ))
            self._state = replace(
                self._state,
                trade_identity_cache=tuple.__new__(_ValidatedTradeCache, tuple(self._slice_trade_cache)),
                trade_identity_cache_complete=bool(self._slice_trade_cache_complete),
                observation_replay_cache=tuple.__new__(_ValidatedReplayCache, tuple(self._slice_replay_cache)),
            )
            return tuple(output)
        except BaseException:
            self._state = original_state
            self._trade_identity_lookup = original_lookup
            raise
        finally:
            self._slice_trade_cache = None
            self._slice_replay_cache = None
            self._slice_trade_cache_complete = None

    def _start_epoch(self, observation: PublicStreamObservationV1, event_hash: str) -> None:
        old = self._state
        reason = observation.reason_code or _default_reason(observation.kind)
        recovery_ref = sha256_json({"continuity_version": PUBLIC_STREAM_CONTINUITY_VERSION,
                                    "previous_recovery_ref": old.current_recovery_ref,
                                    "epoch_id": observation.epoch_id,
                                    "transition_kind": observation.kind.value,
                                    "transition_at_ns": observation.observed_at_ns,
                                    "transition_ref": event_hash})
        prior_cache = old.trade_identity_cache
        if self._slice_trade_cache is not None:
            prior_cache = ()
            assert self._slice_replay_cache is not None
            self._slice_replay_cache.clear()
            self._slice_replay_cache.append(event_hash)
        # Trade IDs are source identities and are useful across reconnects;
        # retain the bounded cache, but never interpret it as history proof.
        state = replace(
            old, epoch_id=observation.epoch_id, recovery_epoch=old.recovery_epoch + 1,
            prior_recovery_ref=old.current_recovery_ref, current_recovery_ref=recovery_ref,
            last_available_at_ns=observation.available_at_ns,
            last_transport_receipt_at_ns=None,
            observation_replay_cache=(),
            gap_count=old.gap_count + 1,
            gap_reason_codes=_append_bounded(old.gap_reason_codes, reason, MAX_GAP_REASON_CODES),
            transport_disconnected=False,
            trade_identity_cache=prior_cache,
        )
        if self._slice_replay_cache is None:
            state = replace(state, observation_replay_cache=(event_hash,))
        self._state = state

    def _apply_trade(self, observation: PublicStreamObservationV1,
                     durable_prior_payload_hash: str | None,
                     durable_lookup_complete: bool,
                     durable_identity_conflicted: bool = False) -> _DeferredContinuityDecision:
        state = self._state
        trade_id = observation.trade_id
        payload_hash = observation.trade_payload_hash
        if trade_id is None or payload_hash is None:
            raise ValueError("trade observation requires exact venue identity and payload hash")
        cached = self._trade_identity_lookup.get(trade_id)
        if durable_identity_conflicted:
            self._add_gap("CONFLICTING_TRADE_ID_PAYLOAD")
            self._set_trade_receipts(observation)
            return self._decision(observation, PublicStreamClassificationV1.CONFLICTING_TRADE_ID,
                                  "TRADE_ID_HAS_PRIOR_CONFLICTING_DURABLE_PAYLOAD")
        known = cached if cached is not None else durable_prior_payload_hash
        receipt_out_of_order = bool(
            observation.receipt_at_ns is not None
            and state.last_transport_receipt_at_ns is not None
            and observation.receipt_at_ns < state.last_transport_receipt_at_ns
        )
        if known is not None:
            classification = (PublicStreamClassificationV1.DUPLICATE_TRADE_IDENTICAL
                              if known == payload_hash else PublicStreamClassificationV1.CONFLICTING_TRADE_ID)
            reason = ("TRADE_ID_ALREADY_ARCHIVED_IDENTICAL_PAYLOAD" if known == payload_hash
                      else "TRADE_ID_REUSED_WITH_CONFLICTING_PAYLOAD")
            if classification == PublicStreamClassificationV1.CONFLICTING_TRADE_ID:
                self._add_gap("CONFLICTING_TRADE_ID_PAYLOAD")
            if receipt_out_of_order:
                self._add_gap("OUT_OF_ORDER_RECEIPT_TIME")
            self._set_trade_receipts(observation)
            if receipt_out_of_order:
                reason = f"{reason};RECEIPT_ORDER_REGRESSED"
            return self._decision(observation, classification, reason)
        cache_complete = (self._slice_trade_cache_complete
                          if self._slice_trade_cache_complete is not None
                          else state.trade_identity_cache_complete)
        if not cache_complete and not durable_lookup_complete:
            self._add_gap("TRADE_IDENTITY_LOOKUP_REQUIRED_AFTER_BOUNDED_CACHE_EVICTION")
            if receipt_out_of_order:
                self._add_gap("OUT_OF_ORDER_RECEIPT_TIME")
            self._set_trade_receipts(observation)
            return self._decision(observation, PublicStreamClassificationV1.TRADE_ID_HISTORY_UNAVAILABLE,
                                  "DURABLE_TRADE_ID_LOOKUP_REQUIRED")
        event_out_of_order = bool(
            observation.event_at_ns is not None and state.last_trade_event_at_ns is not None
            and observation.event_at_ns < state.last_trade_event_at_ns
        )
        if receipt_out_of_order:
            self._add_gap("OUT_OF_ORDER_RECEIPT_TIME")
            state = self._state
        if self._slice_trade_cache is None:
            cache = _ValidatedTradeCache(state.trade_identity_cache).append_row((trade_id, payload_hash))
            cache_complete = state.trade_identity_cache_complete
        else:
            cache_rows = self._slice_trade_cache
            cache = ()
            if len(cache_rows) == MAX_TRADE_ID_CACHE:
                evicted_id, _ = cache_rows.pop(0)
                self._trade_identity_lookup.pop(evicted_id, None)
                self._slice_trade_cache_complete = False
            nonblank(trade_id, field="trade_id")
            sha256_ref(payload_hash, field="trade_payload_hash")
            cache_rows.append((trade_id, payload_hash))
            cache_complete = bool(self._slice_trade_cache_complete)
        if self._slice_trade_cache is None and len(state.trade_identity_cache) == MAX_TRADE_ID_CACHE:
            evicted_id, _ = state.trade_identity_cache[0]
            self._trade_identity_lookup.pop(evicted_id, None)
            cache_complete = False
        self._trade_identity_lookup[trade_id] = payload_hash
        self._state = replace(
            state,
            last_available_at_ns=observation.available_at_ns,
            last_transport_receipt_at_ns=self._latest_receipt(state.last_transport_receipt_at_ns,
                                                                observation.receipt_at_ns),
            last_trade_receipt_at_ns=self._latest_receipt(state.last_trade_receipt_at_ns,
                                                           observation.receipt_at_ns),
            last_trade_event_at_ns=(max(state.last_trade_event_at_ns, observation.event_at_ns)
                                    if state.last_trade_event_at_ns is not None and observation.event_at_ns is not None
                                    else observation.event_at_ns or state.last_trade_event_at_ns),
            observed_trade_count=state.observed_trade_count + 1,
            trade_identity_cache=cache,
            trade_identity_cache_complete=cache_complete,
        )
        if event_out_of_order:
            return self._decision(observation, PublicStreamClassificationV1.OUT_OF_ORDER_TRADE_EVENT_TIME,
                                  "TRADE_EVENT_TIME_REGRESSED_RELATIVE_TO_PRIOR_RECEIPT")
        if receipt_out_of_order:
            return self._decision(observation, PublicStreamClassificationV1.OUT_OF_ORDER_TRADE_RECEIPT_TIME,
                                  "TRADE_RECEIPT_TIME_REGRESSED")
        return self._decision(observation, PublicStreamClassificationV1.TRADE_ACCEPTED, None)

    @staticmethod
    def _latest_receipt(previous: int | None, current: int | None) -> int | None:
        if previous is None:
            return current
        if current is None:
            return previous
        return max(previous, current)

    def _set_trade_receipts(self, observation: PublicStreamObservationV1) -> None:
        state = self._state
        self._state = replace(
            state,
            last_available_at_ns=observation.available_at_ns,
            last_transport_receipt_at_ns=self._latest_receipt(state.last_transport_receipt_at_ns,
                                                                observation.receipt_at_ns),
            last_trade_receipt_at_ns=self._latest_receipt(state.last_trade_receipt_at_ns,
                                                           observation.receipt_at_ns),
        )

    def _remember_observation(self, event_hash: str) -> None:
        if self._slice_replay_cache is not None:
            sha256_ref(event_hash, field="observation_hash")
            self._slice_replay_cache.append(event_hash)
            if len(self._slice_replay_cache) > MAX_OBSERVATION_REPLAY_CACHE:
                del self._slice_replay_cache[:-MAX_OBSERVATION_REPLAY_CACHE]
            return
        cache = _ValidatedReplayCache(self._state.observation_replay_cache).append_ref(event_hash)
        self._state = replace(self._state,
                              observation_replay_cache=cache)

    def has_cached_trade_identity(self, trade_id: str) -> bool:
        """Report whether the bounded in-memory identity cache contains this ID."""
        nonblank(trade_id, field="trade_id")
        return trade_id in self._trade_identity_lookup

    def _add_gap(self, reason: str) -> None:
        state = self._state
        self._state = replace(state, gap_count=state.gap_count + 1,
                              gap_reason_codes=_append_bounded(state.gap_reason_codes, reason,
                                                               MAX_GAP_REASON_CODES))

    def _decision(self, observation: PublicStreamObservationV1,
                  classification: PublicStreamClassificationV1,
                  reason_code: str | None) -> _DeferredContinuityDecision:
        return _DeferredContinuityDecision(observation, classification,
                                           reason_code, self._state)


def _append_bounded(values: tuple[str, ...], value: str, maximum: int) -> tuple[str, ...]:
    if value in values:
        return values
    result = (*values, value)
    if len(result) <= maximum:
        return result
    return ("GAP_REASON_HISTORY_TRUNCATED", *result[-(maximum - 1):])


@dataclass(frozen=True)
class LatestValidBboEvidenceV1:
    bid_price: str
    ask_price: str
    received_at_ns: int
    data_age_ns: int
    input_refs: tuple[str, ...]

    def __post_init__(self) -> None:
        timestamp(self.received_at_ns, field="received_at_ns")
        if self.data_age_ns < 0:
            raise ValueError("BBO data age cannot be negative")
        for ref in self.input_refs:
            sha256_ref(ref, field="input_ref")

    def to_dict(self) -> dict[str, Any]:
        return {"bid_price": self.bid_price, "ask_price": self.ask_price,
                "received_at_ns": self.received_at_ns, "data_age_ns": self.data_age_ns,
                "input_refs": list(self.input_refs)}


@dataclass(frozen=True)
class PublicStreamContinuityReportV1:
    instrument: InstrumentKeyV2
    source_id: str
    channel: str
    metadata_ref: str
    contract_revision: str
    metadata_status: str
    metadata_current: bool
    epoch_id: str
    recovery_epoch: int
    prior_recovery_ref: str | None
    current_recovery_ref: str
    as_of_ns: int
    transport_received: bool
    last_transport_receipt_at_ns: int | None
    source_current: bool
    source_health_ref: str | None
    source_health_epoch_match: bool
    book_sequence_valid: bool | None
    observed_trade_evidence: bool
    observed_trade_count: int
    last_trade_receipt_at_ns: int | None
    trade_completeness_proven: bool
    strategy_input_qualified: bool
    latest_valid_bbo: LatestValidBboEvidenceV1 | None
    bbo_stale_or_unavailable_reason: str | None
    gap_count: int
    gap_reason_codes: tuple[str, ...]
    trade_id_semantics: str
    trade_side_semantics: str
    trade_recovery_semantics: str
    sequence_semantics: str | None
    capability_matrix_ref: str
    capability_row_ref: str | None
    capability_status: str
    coverage_censoring_limitations: str
    gap_reset_reconnect_behavior: str
    repair_capability: str
    source_health_requirement: str
    permitted_uses: tuple[str, ...]
    explicitly_unsupported_uses: tuple[str, ...]
    reasons: tuple[str, ...]

    SCHEMA_VERSION: ClassVar[int] = 1

    def __post_init__(self) -> None:
        timestamp(self.as_of_ns, field="as_of_ns")
        sha256_ref(self.current_recovery_ref, field="current_recovery_ref")
        if self.prior_recovery_ref is not None:
            sha256_ref(self.prior_recovery_ref, field="prior_recovery_ref")
        sha256_ref(self.capability_matrix_ref, field="capability_matrix_ref")
        if self.capability_row_ref is not None:
            sha256_ref(self.capability_row_ref, field="capability_row_ref")
        if self.source_health_ref is not None:
            sha256_ref(self.source_health_ref, field="source_health_ref")
        object.__setattr__(self, "gap_reason_codes", tuple(self.gap_reason_codes))
        object.__setattr__(self, "permitted_uses", tuple(self.permitted_uses))
        object.__setattr__(self, "explicitly_unsupported_uses", tuple(self.explicitly_unsupported_uses))
        object.__setattr__(self, "reasons", tuple(self.reasons))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.SCHEMA_VERSION,
            "report_version": PUBLIC_STREAM_CONTINUITY_VERSION,
            "instrument": self.instrument.to_dict(), "source_id": self.source_id,
            "channel": self.channel, "metadata_ref": self.metadata_ref,
            "contract_revision": self.contract_revision, "metadata_status": self.metadata_status,
            "metadata_current": self.metadata_current, "epoch_id": self.epoch_id,
            "recovery_epoch": self.recovery_epoch, "prior_recovery_ref": self.prior_recovery_ref,
            "current_recovery_ref": self.current_recovery_ref, "as_of_ns": self.as_of_ns,
            "transport_received": self.transport_received,
            "last_transport_receipt_at_ns": self.last_transport_receipt_at_ns,
            "source_current": self.source_current, "source_health_ref": self.source_health_ref,
            "source_health_epoch_match": self.source_health_epoch_match,
            "book_sequence_valid": self.book_sequence_valid,
            "observed_trade_evidence": self.observed_trade_evidence,
            "observed_trade_count": self.observed_trade_count,
            "last_trade_receipt_at_ns": self.last_trade_receipt_at_ns,
            "trade_completeness_proven": self.trade_completeness_proven,
            "strategy_input_qualified": self.strategy_input_qualified,
            "latest_valid_bbo": self.latest_valid_bbo.to_dict() if self.latest_valid_bbo else None,
            "bbo_stale_or_unavailable_reason": self.bbo_stale_or_unavailable_reason,
            "gap_count": self.gap_count, "gap_reason_codes": list(self.gap_reason_codes),
            "trade_id_semantics": self.trade_id_semantics,
            "trade_side_semantics": self.trade_side_semantics,
            "trade_recovery_semantics": self.trade_recovery_semantics,
            "sequence_semantics": self.sequence_semantics,
            "capability_matrix_ref": self.capability_matrix_ref,
            "capability_row_ref": self.capability_row_ref,
            "capability_status": self.capability_status,
            "coverage_censoring_limitations": self.coverage_censoring_limitations,
            "gap_reset_reconnect_behavior": self.gap_reset_reconnect_behavior,
            "repair_capability": self.repair_capability,
            "source_health_requirement": self.source_health_requirement,
            "permitted_uses": list(self.permitted_uses),
            "explicitly_unsupported_uses": list(self.explicitly_unsupported_uses),
            "reasons": list(self.reasons),
        }

    @property
    def content_hash(self) -> str:
        return sha256_json({"artifact_type": "PublicStreamContinuityReportV1", "report": self.to_dict()})


def build_public_stream_continuity_report(
    tracker: PublicStreamContinuityTrackerV1,
    *,
    as_of_ns: int,
    source_health: PublicSourceHealthV2 | None,
    source_health_epoch_id: str | None,
    metadata: ProductContractV2 | None,
    max_source_health_age_ns: int,
    max_metadata_age_ns: int,
    book: SequenceValidBookV2 | None = None,
    book_metadata_ref: str | None = None,
    book_lineage_ref: str | None = None,
) -> PublicStreamContinuityReportV1:
    """Produce an as-of capability report from one bounded tracker snapshot.

    Callers must supply the state snapshot that was current at ``as_of_ns``;
    later observations are rejected to preserve causal availability.
    """
    cutoff = timestamp(as_of_ns, field="as_of_ns")
    if max_source_health_age_ns <= 0 or max_metadata_age_ns <= 0:
        raise ValueError("health and metadata freshness limits must be positive")
    state = tracker.state
    if state.last_available_at_ns is not None and state.last_available_at_ns > cutoff:
        raise ValueError("tracker state contains evidence after the requested as-of cutoff")
    matrix = default_evidence_capability_matrix_v2()
    capability = capability_for_public_channel_v2(matrix, state.instrument, state.channel)
    matrix_ref = matrix.content_hash
    row_ref = sha256_json(capability.to_dict()) if capability is not None else None
    health_ref = source_health.content_hash if source_health is not None else None
    health_epoch_match = bool(source_health_epoch_id is not None and source_health_epoch_id == state.epoch_id)
    transport_received = bool(
        state.last_transport_receipt_at_ns is not None
        and state.last_transport_receipt_at_ns <= cutoff
        and cutoff - state.last_transport_receipt_at_ns <= max_source_health_age_ns
    )
    health_current = bool(
        source_health is not None
        and source_health.source_id == state.source_id
        and source_health.state == PublicSourceStateV2.HEALTHY_CURRENT
        and source_health.available_at_ns <= cutoff
        and source_health.observed_at_ns <= cutoff
        and cutoff - source_health.observed_at_ns <= max_source_health_age_ns
        and health_epoch_match
        and transport_received
        and not state.transport_disconnected
    )
    metadata_ref_matches = bool(metadata is not None and metadata.key == state.instrument
                                and metadata.metadata_ref == state.metadata_ref)
    metadata_current = bool(
        metadata_ref_matches and metadata is not None
        and metadata.effective_at_ns <= cutoff
        and metadata.available_at_ns <= cutoff
        and metadata.observed_at_ns <= cutoff
        and cutoff - metadata.observed_at_ns <= max_metadata_age_ns
        and metadata.trading_status == TradingStatusV2.TRADING
    )
    observed_trades = bool(state.observed_trade_count > 0 and state.last_trade_receipt_at_ns is not None
                           and state.last_trade_receipt_at_ns <= cutoff)
    book_sequence_valid: bool | None = None
    latest_bbo: LatestValidBboEvidenceV1 | None = None
    bbo_reason: str | None = None
    reasons: list[str] = []
    if not metadata_ref_matches:
        reasons.append("METADATA_REVISION_MISMATCH_OR_UNAVAILABLE")
    elif not metadata_current:
        if metadata is None:
            reasons.append("METADATA_NOT_SUPPLIED")
        elif metadata.trading_status != TradingStatusV2.TRADING:
            reasons.append(f"METADATA_STATUS_{metadata.trading_status.value}")
        else:
            reasons.append("METADATA_STALE_OR_NOT_YET_AVAILABLE")
    if not health_current:
        if source_health is None:
            reasons.append("SOURCE_HEALTH_NOT_SUPPLIED")
        elif source_health.source_id != state.source_id:
            reasons.append("SOURCE_HEALTH_SOURCE_ID_MISMATCH")
        elif not health_epoch_match:
            reasons.append("SOURCE_HEALTH_EPOCH_MISMATCH")
        else:
            reasons.append("SOURCE_NOT_CURRENT_OR_STALE")
    if state.channel.startswith("orderbook."):
        book_sequence_valid = False
        if book is None:
            bbo_reason = "MISSING_SEQUENCE_BOOK_STATE"
        elif (book.instrument, book.source_id, book.channel) != (state.instrument, state.source_id, state.channel):
            bbo_reason = "BOOK_STATE_IDENTITY_MISMATCH"
        elif book_metadata_ref != state.metadata_ref:
            bbo_reason = "BOOK_METADATA_REVISION_MISMATCH"
        elif not metadata_current:
            bbo_reason = "METADATA_STALE_OR_INELIGIBLE"
        else:
            feature = (book.continuity_view(cutoff_ns=cutoff) if book_lineage_ref is not None
                       else book.feature(cutoff_ns=cutoff))
            book_sequence_valid = feature.sequence_state == BookStateV2.VALID and feature.bbo is not None
            if book_sequence_valid and feature.bbo is not None and feature.data_age_ns is not None:
                latest_bbo = LatestValidBboEvidenceV1(
                    feature.bbo[0], feature.bbo[1], cutoff - feature.data_age_ns,
                    feature.data_age_ns, ((book_lineage_ref, source_health.content_hash)
                        if book_lineage_ref is not None and source_health is not None else feature.input_refs),
                )
            else:
                bbo_reason = feature.missing_reason or f"BOOK_SEQUENCE_{feature.sequence_state.value}"
        if book_sequence_valid is not True:
            reasons.append(bbo_reason or "BOOK_SEQUENCE_NOT_VALID")
    elif state.channel.startswith("publicTrade."):
        book_sequence_valid = None
        bbo_reason = "NOT_AN_ORDERBOOK_CHANNEL"
    else:
        book_sequence_valid = None
        bbo_reason = "NO_BOOK_SEMANTICS_FOR_CHANNEL"
    if not transport_received:
        reasons.append("NO_RECENT_FRAME_RECEIPT")
    if state.channel.startswith("publicTrade."):
        reasons.append("TRADE_COMPLETENESS_UNSUPPORTED_NO_DECLARED_CURSOR_OR_HISTORY_REPAIR")
    reasons.append("POLICY_SPECIFIC_STRATEGY_GATES_NOT_EVALUATED")
    if state.gap_count:
        reasons.extend(state.gap_reason_codes)
    if capability is None:
        trade_id_semantics = "NO_EXACT_CAPABILITY_ROW"
        trade_side_semantics = "UNQUALIFIED"
        trade_recovery_semantics = "UNQUALIFIED"
        sequence_semantics = None
        capability_status = "NO_EXACT_CAPABILITY_ROW"
        coverage_limitations = "No exact capability row; no inferred use is permitted."
        gap_recovery = "UNQUALIFIED"
        repair_capability = "UNQUALIFIED"
        health_requirement = "UNQUALIFIED"
        permitted_uses: tuple[str, ...] = ()
        unsupported_uses: tuple[str, ...] = ("all uses until exact source capability is declared",)
    else:
        if state.channel.startswith("publicTrade.") or "aggTrade" in state.channel:
            trade_id_semantics = capability.sequence_update_semantics or "UNDECLARED"
            trade_side_semantics = capability.aggressor_side_convention or "UNDECLARED"
            trade_recovery_semantics = capability.repair_capability
        else:
            trade_id_semantics = "NOT_APPLICABLE_TO_THIS_CHANNEL"
            trade_side_semantics = "NOT_APPLICABLE_TO_THIS_CHANNEL"
            trade_recovery_semantics = "NOT_APPLICABLE_TO_THIS_CHANNEL"
        sequence_semantics = capability.sequence_update_semantics
        capability_status = capability.status.value
        coverage_limitations = capability.coverage_censoring_limitations
        gap_recovery = capability.gap_reset_reconnect_behavior
        repair_capability = capability.repair_capability
        health_requirement = capability.source_health_requirement
        permitted_uses = capability.permitted_uses
        unsupported_uses = capability.explicitly_unsupported_uses
    if state.instrument.venue.value == "BYBIT" and state.channel.startswith("publicTrade."):
        # The Bybit i field is an idempotency identity, not a replay cursor.
        # Bybit seq can be shared across grouped records; no history repair is declared.
        trade_id_semantics = (
            "i is a trade identity; exact duplicate comparison uses canonical per-trade row hash when supplied; "
            "seq may be shared across grouped records"
        )
        trade_side_semantics = "S is taker side (Buy/Sell); parser convention BYBIT_S_IS_TAKER_SIDE"
        trade_recovery_semantics = "no declared cursor or historical repair; disconnect/reconnect intervals remain unsupported"
    return PublicStreamContinuityReportV1(
        state.instrument, state.source_id, state.channel, state.metadata_ref,
        state.instrument.contract_revision,
        metadata.trading_status.value if metadata is not None else "UNKNOWN",
        metadata_current, state.epoch_id, state.recovery_epoch, state.prior_recovery_ref,
        state.current_recovery_ref, cutoff, transport_received, state.last_transport_receipt_at_ns,
        health_current, health_ref, health_epoch_match, book_sequence_valid,
        observed_trades, state.observed_trade_count, state.last_trade_receipt_at_ns,
        False, False, latest_bbo, bbo_reason, state.gap_count,
        state.gap_reason_codes, trade_id_semantics, trade_side_semantics,
        trade_recovery_semantics, sequence_semantics, matrix_ref, row_ref,
        capability_status, coverage_limitations, gap_recovery, repair_capability,
        health_requirement, permitted_uses, unsupported_uses, tuple(dict.fromkeys(reasons)),
    )
