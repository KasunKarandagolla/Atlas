from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.health import PublicSourceHealthV2, PublicSourceStateV2
from atlas.v2.data.microstructure import (
    AggressiveTradeV2,
    BookLevelV2,
    BookStateV2,
    L2DeltaV2,
    L2SnapshotV2,
    SequenceValidBookV2,
)
from atlas.v2.data.public_microstructure_ws import CapturedPublicFrameV2
from atlas.v2.data.public_stream_continuity import (
    PublicStreamClassificationV1,
    PublicStreamContinuityTrackerV1,
    PublicStreamObservationKindV1,
    PublicStreamObservationV1,
    build_public_stream_continuity_report,
)
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)

T0 = 1_750_000_000_000_000_000
SOURCE = "BYBIT_LINEAR_PUBLIC_WS"
TRADE_CHANNEL = "publicTrade.BTCUSDT"
BOOK_CHANNEL = "orderbook.50.BTCUSDT"
META_REF = "metadata-revision-one"
HEALTH_REF = sha256_json({"session032": "healthy"})
TRADE_EPOCH = "connection-1"


def ref(value: object) -> str:
    return sha256_json({"s32_test": value})


def key(*, revision: str = "a" * 64, symbol: str = "BTCUSDT") -> InstrumentKeyV2:
    return InstrumentKeyV2(VenueV2.BYBIT, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
                           symbol, symbol.removesuffix("USDT"), "USDT", "USDT", revision)


def contract(instrument: InstrumentKeyV2 | None = None, *, metadata_ref: str = META_REF,
             observed_at_ns: int = T0, status: TradingStatusV2 = TradingStatusV2.TRADING) -> ProductContractV2:
    return ProductContractV2(
        instrument or key(), T0, observed_at_ns, observed_at_ns,
        Decimal("1"), Decimal("0.1"), Decimal("0.001"), Decimal("0.001"),
        status, metadata_ref,
    )


def health(source_id: str = SOURCE, *, at_ns: int = T0,
           state: PublicSourceStateV2 = PublicSourceStateV2.HEALTHY_CURRENT) -> PublicSourceHealthV2:
    return PublicSourceHealthV2(source_id, at_ns, at_ns, state, f"health-{at_ns}", "fixture")


def tracker(*, instrument: InstrumentKeyV2 | None = None, source_id: str = SOURCE,
            channel: str = TRADE_CHANNEL, metadata_ref: str = META_REF,
            epoch_id: str = TRADE_EPOCH) -> PublicStreamContinuityTrackerV1:
    return PublicStreamContinuityTrackerV1(
        instrument=instrument or key(), source_id=source_id, channel=channel,
        metadata_ref=metadata_ref, epoch_id=epoch_id,
    )


def trade(trade_id: str, *, received: int = T0, event: int | None = None,
          price: str = "100", quantity: str = "2", side: str | None = "BUY",
          instrument: InstrumentKeyV2 | None = None, source_id: str = SOURCE,
          channel: str = TRADE_CHANNEL) -> AggressiveTradeV2:
    return AggressiveTradeV2(
        instrument or key(), source_id, channel, trade_id, side, Decimal(price), Decimal(quantity),
        received if event is None else event, received, received, ref([trade_id, received, price]),
        "HEALTHY_CURRENT", "BYBIT_S_IS_TAKER_SIDE", source_health_ref=HEALTH_REF,
    )


def trade_observation(trade_id: str, *, received: int = T0, event: int | None = None,
                      price: str = "100", epoch_id: str = TRADE_EPOCH,
                      instrument: InstrumentKeyV2 | None = None,
                      metadata_ref: str = META_REF,
                      exact_payload_hash: str | None = None) -> PublicStreamObservationV1:
    return PublicStreamObservationV1.from_trade(
        trade(trade_id, received=received, event=event, price=price, instrument=instrument),
        metadata_ref=metadata_ref, epoch_id=epoch_id,
        exact_trade_payload_hash=exact_payload_hash,
    )


def frame_observation(*, at_ns: int = T0, channel: str = TRADE_CHANNEL,
                      epoch_id: str = TRADE_EPOCH, instrument: InstrumentKeyV2 | None = None,
                      source_id: str = SOURCE, metadata_ref: str = META_REF,
                      persisted_at_ns: int | None = None) -> PublicStreamObservationV1:
    instrument = instrument or key()
    raw = json.dumps({"topic": channel, "data": []}, separators=(",", ":")).encode()
    frame = CapturedPublicFrameV2(instrument.venue, source_id, channel, raw,
                                  hashlib.sha256(raw).hexdigest(), at_ns, at_ns)
    return PublicStreamObservationV1.from_frame(
        frame, instrument=instrument, metadata_ref=metadata_ref, epoch_id=epoch_id,
        persisted_at_ns=persisted_at_ns,
    )


def report(current: PublicStreamContinuityTrackerV1, *, as_of_ns: int = T0,
           source_health: PublicSourceHealthV2 | None = None,
           source_health_epoch_id: str | None = TRADE_EPOCH,
           metadata: ProductContractV2 | None = None,
           book: SequenceValidBookV2 | None = None,
           book_metadata_ref: str | None = None):
    return build_public_stream_continuity_report(
        current, as_of_ns=as_of_ns,
        source_health=source_health or health(at_ns=as_of_ns),
        source_health_epoch_id=source_health_epoch_id,
        metadata=metadata or contract(current.state.instrument, metadata_ref=current.state.metadata_ref),
        max_source_health_age_ns=20_000_000_000,
        max_metadata_age_ns=60_000_000_000,
        book=book, book_metadata_ref=book_metadata_ref,
    )


def test_trade_id_exact_duplicate_and_conflicting_payload_are_distinct() -> None:
    current = tracker()
    exact_hash = ref("canonicalized-raw-row")
    first = trade_observation("trade-1", received=T0, exact_payload_hash=exact_hash)
    assert current.apply(first).classification == PublicStreamClassificationV1.TRADE_ACCEPTED
    assert first.trade_payload_hash_basis == "CANONICALIZED_PER_TRADE_RAW_ROW_BYTES"
    exact_replay = current.apply(first)
    assert exact_replay.classification == PublicStreamClassificationV1.DUPLICATE_OBSERVATION
    identical_id = trade_observation("trade-1", received=T0 + 1, event=T0,
                                     exact_payload_hash=exact_hash)
    assert current.apply(identical_id).classification == PublicStreamClassificationV1.DUPLICATE_TRADE_IDENTICAL
    conflicting_id = trade_observation("trade-1", received=T0 + 2, price="101",
                                       exact_payload_hash=ref("different-canonical-row"))
    decision = current.apply(conflicting_id)
    assert decision.classification == PublicStreamClassificationV1.CONFLICTING_TRADE_ID
    assert "CONFLICTING_TRADE_ID_PAYLOAD" in current.state.gap_reason_codes
    assert current.state.observed_trade_count == 1


def test_out_of_order_event_time_does_not_rewrite_receipt_order() -> None:
    current = tracker()
    current.apply(trade_observation("trade-1", received=T0, event=T0 + 10))
    decision = current.apply(trade_observation("trade-2", received=T0 + 20, event=T0 + 5))
    assert decision.classification == PublicStreamClassificationV1.OUT_OF_ORDER_TRADE_EVENT_TIME
    assert current.state.last_trade_receipt_at_ns == T0 + 20
    assert current.state.last_trade_event_at_ns == T0 + 10
    assert current.state.observed_trade_count == 2


def test_out_of_order_frame_receipt_is_classified_without_regressing_latest_receipt() -> None:
    current = tracker()
    current.apply(frame_observation(at_ns=T0 + 10))
    delayed = frame_observation(at_ns=T0 + 5, persisted_at_ns=T0 + 11)
    decision = current.apply(delayed)
    assert decision.classification == PublicStreamClassificationV1.OUT_OF_ORDER_RECEIPT_TIME
    assert current.state.last_transport_receipt_at_ns == T0 + 10
    assert "OUT_OF_ORDER_RECEIPT_TIME" in current.state.gap_reason_codes


def test_replayed_frame_is_idempotent_when_durable_availability_moves_later() -> None:
    current = tracker()
    first = frame_observation(at_ns=T0)
    current.apply(first)
    state_ref = current.state.content_hash
    replay = frame_observation(at_ns=T0, persisted_at_ns=T0 + 10)
    replay = replace(replay, source_health_ref=ref("later-health-projection"))
    decision = current.apply(replay)
    assert decision.classification == PublicStreamClassificationV1.DUPLICATE_OBSERVATION
    assert current.state.content_hash == state_ref


def test_malformed_frame_is_a_durable_gap_observation() -> None:
    current = tracker()
    malformed = PublicStreamObservationV1.transport(
        instrument=key(), source_id=SOURCE, channel=TRADE_CHANNEL, metadata_ref=META_REF,
        epoch_id=TRADE_EPOCH, kind=PublicStreamObservationKindV1.MALFORMED_FRAME,
        observed_at_ns=T0, reason_code="MALFORMED_PUBLIC_FRAME_UNPARSEABLE",
    )
    decision = current.apply(malformed)
    assert decision.classification == PublicStreamClassificationV1.GAP_RECORDED
    assert "MALFORMED_PUBLIC_FRAME_UNPARSEABLE" in current.state.gap_reason_codes


def test_queue_overflow_remains_an_unresolved_gap_after_new_frames() -> None:
    current = tracker()
    overflow = PublicStreamObservationV1.transport(
        instrument=key(), source_id=SOURCE, channel=TRADE_CHANNEL, metadata_ref=META_REF,
        epoch_id=TRADE_EPOCH, kind=PublicStreamObservationKindV1.QUEUE_OVERFLOW,
        observed_at_ns=T0,
    )
    current.apply(overflow)
    current.apply(frame_observation(at_ns=T0 + 1))
    value = report(current, as_of_ns=T0 + 1, source_health=health(at_ns=T0 + 1))
    assert value.transport_received
    assert value.gap_count == 1
    assert "QUEUE_OVERFLOW_LOCAL_DATA_LOSS" in value.gap_reason_codes
    assert value.trade_completeness_proven is False


def test_changed_or_stale_contract_metadata_fails_closed() -> None:
    current = tracker()
    current.apply(frame_observation())
    suspended = report(current, metadata=contract(status=TradingStatusV2.SUSPENDED))
    assert suspended.metadata_current is False
    assert "METADATA_STATUS_SUSPENDED" in suspended.reasons
    stale = report(current, as_of_ns=T0 + 60_000_000_001,
                   metadata=contract(observed_at_ns=T0),
                   source_health=health(at_ns=T0 + 60_000_000_001))
    assert stale.metadata_current is False
    assert "METADATA_STALE_OR_NOT_YET_AVAILABLE" in stale.reasons


def test_frame_topic_must_match_exact_symbol_and_channel() -> None:
    instrument = key()
    raw = json.dumps({"topic": "publicTrade.ETHUSDT", "data": []}, separators=(",", ":")).encode()
    frame = CapturedPublicFrameV2(
        VenueV2.BYBIT, SOURCE, TRADE_CHANNEL, raw, hashlib.sha256(raw).hexdigest(), T0, T0,
    )
    with pytest.raises(ValueError, match="topic does not exactly match"):
        PublicStreamObservationV1.from_frame(
            frame, instrument=instrument, metadata_ref=META_REF, epoch_id=TRADE_EPOCH,
        )


def test_disconnect_reconnect_creates_epoch_and_unrepairable_trade_gap() -> None:
    current = tracker()
    current.apply(trade_observation("trade-1"))
    disconnect = PublicStreamObservationV1.transport(
        instrument=key(), source_id=SOURCE, channel=TRADE_CHANNEL, metadata_ref=META_REF,
        epoch_id=TRADE_EPOCH, kind=PublicStreamObservationKindV1.DISCONNECT,
        observed_at_ns=T0 + 1,
    )
    current.apply(disconnect)
    reconnect = PublicStreamObservationV1.transport(
        instrument=key(), source_id=SOURCE, channel=TRADE_CHANNEL, metadata_ref=META_REF,
        epoch_id="connection-2", kind=PublicStreamObservationKindV1.RECONNECT,
        observed_at_ns=T0 + 2,
    )
    decision = current.apply(reconnect)
    assert decision.classification == PublicStreamClassificationV1.RECOVERY_EPOCH_STARTED
    assert current.state.recovery_epoch == 1
    assert current.state.prior_recovery_ref is not None
    assert "RECONNECT_TRADE_INTERVAL_UNREPAIRABLE" in current.state.gap_reason_codes
    assert current.state.last_transport_receipt_at_ns is None
    current.apply(frame_observation(at_ns=T0 + 3, epoch_id="connection-2"))
    fresh_health = health(at_ns=T0 + 3)
    current_report = report(current, as_of_ns=T0 + 3, source_health=fresh_health,
                            source_health_epoch_id="connection-2")
    assert current_report.source_current
    assert current_report.observed_trade_evidence
    assert current_report.trade_completeness_proven is False
    assert current_report.strategy_input_qualified is False
    assert current_report.gap_count >= 2


def test_book_reset_and_delta_without_snapshot_stay_unqualified() -> None:
    book_current = tracker(channel=BOOK_CHANNEL)
    reset_delta = L2DeltaV2(
        key(), SOURCE, BOOK_CHANNEL, "BYBIT_U", None, 1, None,
        T0, T0, T0,
        (BookLevelV2(Decimal("100"), Decimal("1")),),
        (BookLevelV2(Decimal("102"), Decimal("1")),),
        ref("reset-delta"), "HEALTHY_CURRENT", reset=True, source_health_ref=HEALTH_REF,
    )
    sequence = SequenceValidBookV2(instrument=key(), source_id=SOURCE, channel=BOOK_CHANNEL,
                                   sequence_semantics="BYBIT_U", warmup_ns=0, stale_ns=100)
    assert sequence.apply_delta(reset_delta).state == BookStateV2.GAP_DETECTED
    reset_obs = PublicStreamObservationV1.from_book_event(reset_delta, metadata_ref=META_REF,
                                                          epoch_id=TRADE_EPOCH)
    book_current.apply(reset_obs)
    book_current.apply(frame_observation(channel=BOOK_CHANNEL))
    value = report(book_current, source_health=health(), book=sequence, book_metadata_ref=META_REF)
    assert value.book_sequence_valid is False
    assert value.latest_valid_bbo is None
    assert value.bbo_stale_or_unavailable_reason is not None
    assert "EXCHANGE_RESET_REQUIRES_NEW_SNAPSHOT" in value.gap_reason_codes


def test_sequence_valid_book_bbo_expires_at_asof_cutoff() -> None:
    current = tracker(channel=BOOK_CHANNEL)
    source_book = SequenceValidBookV2(instrument=key(), source_id=SOURCE, channel=BOOK_CHANNEL,
                                      sequence_semantics="BYBIT_U", warmup_ns=0, stale_ns=5)
    snapshot = L2SnapshotV2(
        key(), SOURCE, BOOK_CHANNEL, "BYBIT_U", 100, T0, T0, T0,
        (BookLevelV2(Decimal("100"), Decimal("2")),),
        (BookLevelV2(Decimal("102"), Decimal("3")),), ref("snapshot"),
        "HEALTHY_CURRENT", 50, source_health_ref=HEALTH_REF,
    )
    source_book.apply_snapshot(snapshot)
    current.apply(PublicStreamObservationV1.from_book_event(snapshot, metadata_ref=META_REF,
                                                            epoch_id=TRADE_EPOCH))
    fresh = report(current, book=source_book, book_metadata_ref=META_REF, as_of_ns=T0 + 1,
                   source_health=health(at_ns=T0 + 1))
    assert fresh.book_sequence_valid is True
    assert fresh.latest_valid_bbo is not None
    assert fresh.latest_valid_bbo.received_at_ns == T0
    stale = report(current, book=source_book, book_metadata_ref=META_REF, as_of_ns=T0 + 6,
                   source_health=health(at_ns=T0 + 6))
    assert stale.book_sequence_valid is False
    assert stale.latest_valid_bbo is None
    assert stale.bbo_stale_or_unavailable_reason == "NOT_ESTIMABLE_STALE_BOOK"


def test_metadata_rebind_clears_prior_identity_and_binds_new_revision() -> None:
    current = tracker()
    current.apply(trade_observation("trade-old"))
    prior_recovery = current.state.current_recovery_ref
    new_key = key(revision="b" * 64)
    rebound, metadata_event = current.rebind_metadata(
        instrument=new_key, metadata_ref="metadata-revision-two", epoch_id="connection-2",
        observed_at_ns=T0 + 10,
    )
    decision = rebound.apply(metadata_event)
    assert decision.classification == PublicStreamClassificationV1.GAP_RECORDED
    assert rebound.state.observed_trade_count == 0
    assert rebound.state.trade_identity_cache == ()
    assert rebound.state.prior_recovery_ref == prior_recovery
    with pytest.raises(ValueError, match="metadata revision"):
        rebound.apply(trade_observation("trade-old", instrument=key(), metadata_ref=META_REF))
    value = report(rebound, as_of_ns=T0 + 10,
                   source_health=health(at_ns=T0 + 10), source_health_epoch_id="connection-2",
                   metadata=contract(new_key, metadata_ref="metadata-revision-two", observed_at_ns=T0 + 10))
    assert value.contract_revision == "b" * 64
    assert value.observed_trade_evidence is False
    assert value.metadata_current


def test_rest_health_or_wrong_epoch_never_certifies_current_websocket_source() -> None:
    current = tracker()
    current.apply(frame_observation())
    rest_health = health("BYBIT_REST", at_ns=T0)
    rest_value = report(current, source_health=rest_health)
    assert rest_value.transport_received
    assert rest_value.source_current is False
    assert "SOURCE_HEALTH_SOURCE_ID_MISMATCH" in rest_value.reasons
    epoch_value = report(current, source_health=health(), source_health_epoch_id="old-epoch")
    assert epoch_value.source_health_epoch_match is False
    assert epoch_value.source_current is False
    assert "SOURCE_HEALTH_EPOCH_MISMATCH" in epoch_value.reasons
    assert epoch_value.trade_completeness_proven is False


def test_trade_completeness_never_promoted_and_report_hash_is_asof_deterministic() -> None:
    current = tracker()
    current.apply(frame_observation())
    current.apply(trade_observation("trade-1"))
    first = report(current)
    second = report(current)
    assert first.content_hash == second.content_hash
    assert first.trade_completeness_proven is False
    assert first.strategy_input_qualified is False
    assert "no declared cursor or historical repair" in first.trade_recovery_semantics
    with pytest.raises(ValueError, match="after the requested as-of cutoff"):
        report(current, as_of_ns=T0 - 1)


def test_tracker_state_serialization_and_durable_prior_identity_lookup() -> None:
    current = tracker()
    first = trade_observation("trade-1")
    current.apply(first)
    restored = PublicStreamContinuityTrackerV1.from_state(
        type(current.state).from_dict(current.state.to_dict())
    )
    duplicate = trade_observation("trade-1", received=T0 + 1, event=T0)
    decision = restored.apply(duplicate, durable_prior_payload_hash=first.trade_payload_hash)
    assert decision.classification == PublicStreamClassificationV1.DUPLICATE_TRADE_IDENTICAL
    assert restored.state.content_hash == type(restored.state).from_dict(restored.to_state().to_dict()).content_hash
