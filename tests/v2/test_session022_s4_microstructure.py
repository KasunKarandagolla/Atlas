from __future__ import annotations

import hashlib
import json
from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from atlas.v2._serialization import sha256_json
from atlas.v2.data.capabilities import (
    FeedCoverageEvidenceV2,
    FeedCoverageStateV2,
    default_evidence_capability_matrix_v2,
)
from atlas.v2.data.microstructure import (
    S4_FEATURE_POLICY_HASH,
    S4_FEATURE_POLICY_SPEC,
    S4_FEATURE_VERSION,
    AggressiveTradeV2,
    BinanceDepthSyncPhaseV2,
    BookLevelV2,
    BookStateV2,
    L2DeltaV2,
    L2SequenceFaultV2,
    L2SnapshotV2,
    S4MarkoutOutcomeV2,
    SequenceValidBookV2,
    estimate_s4_absorption,
    fit_s4_expected_response_baseline,
    s4_execution_quality_context,
)
from atlas.v2.data.public_microstructure_ws import (
    CapturedPublicFrameV2,
    parse_binance_aggtrade,
    parse_binance_depth_frame,
    parse_binance_rest_snapshot,
    parse_bybit_orderbook_frame,
    parse_bybit_trades,
)
from atlas.v2.instruments import EnvironmentV2, InstrumentKeyV2, ProductTypeV2, VenueV2

T0 = 1_750_000_000_000_000_000
SOURCE = "BYBIT_TEST_PUBLIC_WS"
CHANNEL = "orderbook.50.BTCUSDT"
BINANCE_SOURCE = "BINANCE_PUBLIC_WS"
BINANCE_CHANNEL = "btcusdt@depth@100ms"
HEALTH_REF = sha256_json({"test_health": "healthy"})


def key(venue: VenueV2 = VenueV2.BYBIT, symbol: str = "BTCUSDT") -> InstrumentKeyV2:
    return InstrumentKeyV2(venue, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
                           symbol, symbol.removesuffix("USDT"), "USDT", "USDT", "a" * 64)


def ref(value: object) -> str:
    return sha256_json({"session022_test": value})


def snapshot(at: int, *, seq: int = 100, instrument: InstrumentKeyV2 | None = None,
             source: str = SOURCE, channel: str = CHANNEL, health: str = "HEALTHY_CURRENT",
             bid: str = "100", bid_qty: str = "10", ask: str = "102", ask_qty: str = "10",
             health_ref: str | None = HEALTH_REF, raw: str | None = None) -> L2SnapshotV2:
    return L2SnapshotV2(
        instrument or key(), source, channel, "BYBIT_U", seq, at, at, at,
        (BookLevelV2(Decimal(bid), Decimal(bid_qty)),),
        (BookLevelV2(Decimal(ask), Decimal(ask_qty)),), ref(raw or ["snapshot", seq, at]),
        health, 50, source_health_ref=health_ref,
    )


def delta(at: int, seq: int, *, bid: str = "100", bid_qty: str = "10", ask: str = "102",
          ask_qty: str = "10", instrument: InstrumentKeyV2 | None = None,
          source: str = SOURCE, channel: str = CHANNEL, health: str = "HEALTHY_CURRENT",
          health_ref: str | None = HEALTH_REF, raw: str | None = None,
          first: int | None = None, previous: int | None = None) -> L2DeltaV2:
    return L2DeltaV2(
        instrument or key(), source, channel, "BYBIT_U", first, seq, previous, at, at, at,
        (BookLevelV2(Decimal(bid), Decimal(bid_qty)),),
        (BookLevelV2(Decimal(ask), Decimal(ask_qty)),), ref(raw or ["delta", seq, at, bid_qty, ask_qty]),
        health, source_health_ref=health_ref,
    )


def book(*, warmup: int = 0, stale: int = 1_000_000_000,
         cadence: int | None = 1_000_000_000) -> SequenceValidBookV2:
    return SequenceValidBookV2(instrument=key(), source_id=SOURCE, channel=CHANNEL,
                               sequence_semantics="BYBIT_U", warmup_ns=warmup,
                               stale_ns=stale, declared_cadence_ns=cadence)


def binance_book(*, warmup: int = 0, stale: int = 10_000) -> SequenceValidBookV2:
    return SequenceValidBookV2(
        instrument=key(VenueV2.BINANCE), source_id=BINANCE_SOURCE, channel=BINANCE_CHANNEL,
        sequence_semantics="BINANCE_U_PU", warmup_ns=warmup, stale_ns=stale,
        declared_cadence_ns=100,
    )


def binance_snapshot(last_update_id: int, *, received: int = T0, available: int | None = None) -> L2SnapshotV2:
    return L2SnapshotV2(
        key(VenueV2.BINANCE), "BINANCE_REST", "USD-M depth snapshot REST", "BINANCE_U_PU",
        last_update_id, None, received, received if available is None else available,
        (BookLevelV2(Decimal("99"), Decimal("10")),),
        (BookLevelV2(Decimal("101"), Decimal("10")),), ref(["binance-snapshot", last_update_id]),
        "HEALTHY_CURRENT", 1, source_health_ref=HEALTH_REF,
    )


def binance_delta(first: int, last: int, previous: int, at: int, *,
                  received: int | None = None, available: int | None = None,
                  raw: str | None = None, bid_qty: str = "12") -> L2DeltaV2:
    receipt = at if received is None else received
    availability = receipt if available is None else available
    return L2DeltaV2(
        key(VenueV2.BINANCE), BINANCE_SOURCE, BINANCE_CHANNEL, "BINANCE_U_PU",
        first, last, previous, receipt, receipt, availability,
        (BookLevelV2(Decimal("99"), Decimal(bid_qty)),), (),
        ref(raw or ["binance-delta", first, last, previous, receipt, bid_qty]),
        "HEALTHY_CURRENT", source_health_ref=HEALTH_REF,
    )


def frame(venue: VenueV2, payload: dict, *, channel: str, source: str, at: int = T0) -> CapturedPublicFrameV2:
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return CapturedPublicFrameV2(venue, source, channel, raw, hashlib.sha256(raw).hexdigest(), at, at)


def qualified_feature(*, cutoff_ns: int = T0, trade_quantity: str = "1"):
    start = cutoff_ns - 30_000_000_000
    b = book(stale=2_000_000_000, cadence=1_000_000_000)
    b.apply_snapshot(snapshot(start))
    for i in range(1, 31):
        b.apply_delta(delta(start + i * 1_000_000_000, 100 + i))
    coverage = FeedCoverageEvidenceV2(
        key(), SOURCE, "publicTrade.BTCUSDT", start, cutoff_ns, cutoff_ns,
        1_000_000_000, 31, 0, 1_000_000_000, FeedCoverageStateV2.QUALIFIED,
        HEALTH_REF, default_evidence_capability_matrix_v2().content_hash,
        (ref(["coverage", cutoff_ns]),),
    )
    trades = tuple(AggressiveTradeV2(
        key(), SOURCE, coverage.channel, f"trade-{i}", "BUY", Decimal("100"), Decimal(trade_quantity),
        cutoff_ns - (30 - i) * 1_000_000_000, cutoff_ns - (30 - i) * 1_000_000_000,
        cutoff_ns - (30 - i) * 1_000_000_000, ref(["trade", cutoff_ns, i]), "HEALTHY_CURRENT",
        "BYBIT_S_IS_TAKER_SIDE", source_health_ref=HEALTH_REF,
    ) for i in range(1, 31))
    return b.feature(cutoff_ns=cutoff_ns, trades=trades, trade_coverage=coverage)


def test_s4_feature_policy_hash_describes_current_binance_sync_contract() -> None:
    specification = json.dumps(S4_FEATURE_POLICY_SPEC, sort_keys=True)
    assert "lastUpdateId + 1" not in specification
    assert "L+1" not in specification
    assert "u <= lastUpdateId ignored" not in specification
    assert S4_FEATURE_POLICY_SPEC["gap_action"] == "NOT_ESTIMABLE_UNTIL_FRESH_SNAPSHOT_BRIDGE_AND_WARMUP"
    assert S4_FEATURE_POLICY_SPEC["binance_snapshot"] == "REST lastUpdateId = L"
    assert S4_FEATURE_POLICY_SPEC["binance_stale_buffered_events"] == (
        "discard only buffered events where u < L; u == L remains eligible"
    )
    assert S4_FEATURE_POLICY_SPEC["binance_snapshot_bridge"] == "first processed event requires U <= L <= u"
    assert S4_FEATURE_POLICY_SPEC["binance_subsequent_updates"] == (
        "require pu == previous accepted u; any mismatch immediately enters GAP_DETECTED, independent of feature warmup"
    )
    assert S4_FEATURE_POLICY_SPEC["binance_gap_recovery"] == (
        "fresh REST snapshot, new valid bridge, then declared S4 warmup"
    )
    assert sha256_json({
        "policy_id": S4_FEATURE_VERSION,
        "spec": S4_FEATURE_POLICY_SPEC,
    }) == S4_FEATURE_POLICY_HASH
    assert S4_FEATURE_POLICY_HASH != "2708c4a7091cb9c8bab75b388371acbaac905db9f229e9e375f656edd7dffcdf"
    assert qualified_feature().to_dict()["producer_policy_hash"] == S4_FEATURE_POLICY_HASH


def test_instrument_identity_is_full_and_venue_specific() -> None:
    bybit = key(VenueV2.BYBIT)
    binance = key(VenueV2.BINANCE)
    b = book()
    assert b.apply_snapshot(snapshot(T0, instrument=bybit)).state == BookStateV2.WARMING
    wrong = delta(T0 + 1, 101, instrument=binance)
    assert b.apply_delta(wrong).state == BookStateV2.INVALID


def test_valid_snapshot_delta_duplicate_is_idempotent_and_conflict_quarantines_state() -> None:
    b = book()
    assert b.apply_snapshot(snapshot(T0)).state == BookStateV2.WARMING
    change = delta(T0 + 100, 101, bid_qty="11", raw="same-bytes")
    assert b.apply_delta(change).state == BookStateV2.VALID
    state = b.sequence_state
    assert b.apply_delta(change) == state
    assert b.last_update_id == 101
    conflict = delta(T0 + 200, 101, bid_qty="12", raw="different-bytes")
    after = b.apply_delta(conflict)
    assert after.state == BookStateV2.GAP_DETECTED
    assert after.reason == "CONFLICTING_DUPLICATE_DELTA"
    assert not b.feature(cutoff_ns=T0 + 200).estimable


def test_missing_sequence_fault_gap_out_of_order_and_disconnect_fail_closed() -> None:
    b = book()
    fault = L2SequenceFaultV2(key(), SOURCE, CHANNEL, "MISSING_UPDATE_ID", T0, T0, T0,
                              ref("malformed-frame"), "HEALTHY_CURRENT", HEALTH_REF)
    assert b.apply_fault(fault).state == BookStateV2.GAP_DETECTED
    b = book()
    b.apply_snapshot(snapshot(T0))
    assert b.apply_delta(delta(T0 + 10, 102)).state == BookStateV2.GAP_DETECTED
    assert b.sequence_state.reason == "SEQUENCE_GAP_OR_FAILED_SNAPSHOT_BRIDGE"
    b.apply_snapshot(snapshot(T0 + 20, seq=200))
    assert b.apply_delta(delta(T0 + 30, 201)).state == BookStateV2.VALID
    assert b.apply_delta(delta(T0 + 40, 199)).state == BookStateV2.GAP_DETECTED
    b.disconnect(T0 + 50)
    assert b.sequence_state.state == BookStateV2.INVALID
    assert b.reconnect(T0 + 60).state == BookStateV2.SNAPSHOT_RECOVERY


def test_sequence_gap_requires_explicit_snapshot_and_declared_warmup() -> None:
    b = book(warmup=20, stale=100)
    b.apply_snapshot(snapshot(T0))
    assert b.apply_delta(delta(T0 + 10, 101)).state == BookStateV2.WARMING
    assert b.feature(cutoff_ns=T0 + 10).missing_reason == "NOT_ESTIMABLE_BOOK_STATE_WARMING"
    b.apply_delta(delta(T0 + 20, 103))
    assert b.sequence_state.state == BookStateV2.GAP_DETECTED
    assert b.feature(cutoff_ns=T0 + 20).sequence_state == BookStateV2.GAP_DETECTED
    b.apply_snapshot(snapshot(T0 + 30, seq=200))
    assert b.feature(cutoff_ns=T0 + 30).sequence_state == BookStateV2.WARMING
    b.apply_delta(delta(T0 + 50, 201))
    assert b.feature(cutoff_ns=T0 + 50).sequence_state == BookStateV2.VALID


@pytest.mark.parametrize(
    ("first", "last"),
    (
        (8, 10),
        (9, 11),
    ),
)
def test_binance_initial_bridge_overlaps_snapshot_id(first: int, last: int) -> None:
    b = binance_book(warmup=1_000)
    b.apply_snapshot(binance_snapshot(10))
    assert b._binance_sync_phase == BinanceDepthSyncPhaseV2.AWAITING_SNAPSHOT_BRIDGE

    bridge = binance_delta(first, last, 9_999, T0 + 10)
    assert b.apply_delta(bridge).state == BookStateV2.WARMING
    assert b.last_update_id == last
    assert b._binance_sync_phase == BinanceDepthSyncPhaseV2.BRIDGE_COMPLETE
    assert ref(["binance-delta", first, last, 9_999, T0 + 10, "12"]) in b.feature(
        cutoff_ns=T0 + 10
    ).input_refs


def test_binance_initial_bridge_does_not_accept_an_event_only_covering_l_plus_one() -> None:
    b = binance_book(warmup=1_000)
    b.apply_snapshot(binance_snapshot(10))

    assert b.apply_delta(binance_delta(11, 12, 10, T0 + 10)).state == BookStateV2.GAP_DETECTED
    assert b.sequence_state.reason == "SEQUENCE_GAP_OR_FAILED_SNAPSHOT_BRIDGE"


def test_binance_wrong_pu_fails_while_warming_even_when_range_covers_next_id() -> None:
    b = binance_book(warmup=1_000)
    b.apply_snapshot(binance_snapshot(10))
    assert b.apply_delta(binance_delta(9, 11, 0, T0 + 10)).state == BookStateV2.WARMING

    # U/u covers updates after 11, but pu must name the last accepted event (u=11).
    wrong = binance_delta(12, 13, 10, T0 + 20)
    assert b.apply_delta(wrong).state == BookStateV2.GAP_DETECTED
    assert b.sequence_state.reason == "SEQUENCE_GAP_OR_FAILED_SNAPSHOT_BRIDGE"
    assert not b.feature(cutoff_ns=T0 + 20).estimable
    assert b.apply_delta(binance_delta(12, 13, 11, T0 + 30)).state == BookStateV2.GAP_DETECTED


def test_binance_correct_second_pu_succeeds_while_still_warming() -> None:
    b = binance_book(warmup=1_000)
    b.apply_snapshot(binance_snapshot(10))
    assert b.apply_delta(binance_delta(9, 11, 0, T0 + 10)).state == BookStateV2.WARMING

    second = binance_delta(12, 13, 11, T0 + 20)
    assert b.apply_delta(second).state == BookStateV2.WARMING
    assert b.last_update_id == 13
    assert b._binance_sync_phase == BinanceDepthSyncPhaseV2.BRIDGE_COMPLETE


def test_binance_buffer_discards_stale_events_and_chains_every_event_after_bridge() -> None:
    b = binance_book(warmup=1_000)
    snapshot_receipt = T0 + 100
    stale = binance_delta(7, 9, 7, T0 + 80, raw="pre-snapshot-stale")
    bridge = binance_delta(8, 10, 7, T0 + 90, raw="snapshot-bridge")
    chained = binance_delta(11, 12, 10, T0 + 95, raw="buffered-chain")
    state = b.apply_snapshot(
        binance_snapshot(10, received=snapshot_receipt),
        buffered_deltas=(stale, bridge, chained),
    )
    assert state.state == BookStateV2.WARMING
    assert b.last_update_id == 12
    assert b._binance_sync_phase == BinanceDepthSyncPhaseV2.BRIDGE_COMPLETE
    assert ref("pre-snapshot-stale") not in b.feature(cutoff_ns=snapshot_receipt).input_refs
    assert {ref("snapshot-bridge"), ref("buffered-chain")} <= set(
        b.feature(cutoff_ns=snapshot_receipt).input_refs
    )

    bad_chain = binance_delta(15, 16, 13, T0 + 110, raw="bad-buffer-chain")
    # The range covers 15, but pu does not equal the prior accepted u (14).
    assert b.apply_delta(bad_chain).state == BookStateV2.GAP_DETECTED


@pytest.mark.parametrize(
    ("first", "last", "next_first", "next_last"),
    (
        (8, 10, 11, 12),
        (9, 11, 12, 13),
    ),
)
def test_binance_next_pu_must_match_each_valid_bridge_endpoint(
    first: int, last: int, next_first: int, next_last: int,
) -> None:
    b = binance_book(warmup=1_000)
    b.apply_snapshot(binance_snapshot(10))
    assert b.apply_delta(binance_delta(first, last, 0, T0 + 10)).state == BookStateV2.WARMING

    assert b.apply_delta(binance_delta(next_first, next_last, last, T0 + 20)).state == BookStateV2.WARMING
    assert b.last_update_id == next_last


@pytest.mark.parametrize(("first", "last", "next_first"), ((8, 10, 11), (9, 11, 12)))
def test_binance_next_event_rejects_wrong_pu_for_both_bridge_endpoints(
    first: int, last: int, next_first: int,
) -> None:
    b = binance_book(warmup=1_000)
    b.apply_snapshot(binance_snapshot(10))
    assert b.apply_delta(binance_delta(first, last, 0, T0 + 10)).state == BookStateV2.WARMING

    assert b.apply_delta(binance_delta(next_first, next_first, last - 1, T0 + 20)).state == BookStateV2.GAP_DETECTED


def test_binance_gap_requires_new_snapshot_bridge_and_warmup_without_rewriting_earlier_feature() -> None:
    b = binance_book(warmup=10)
    b.apply_snapshot(binance_snapshot(10))
    assert b.apply_delta(binance_delta(8, 10, 0, T0 + 10)).state == BookStateV2.WARMING
    assert b.apply_delta(binance_delta(11, 11, 10, T0 + 20)).state == BookStateV2.VALID
    earlier = b.feature(cutoff_ns=T0 + 20).to_canonical_json()

    fault = binance_delta(12, 13, 9, T0 + 30)
    assert b.apply_delta(fault).state == BookStateV2.GAP_DETECTED
    assert b.feature(cutoff_ns=T0 + 30).sequence_state == BookStateV2.GAP_DETECTED
    assert b.apply_delta(binance_delta(16, 17, 15, T0 + 40)).state == BookStateV2.GAP_DETECTED

    b.apply_snapshot(binance_snapshot(20, received=T0 + 50))
    assert b._binance_sync_phase == BinanceDepthSyncPhaseV2.AWAITING_SNAPSHOT_BRIDGE
    assert b.feature(cutoff_ns=T0 + 50).sequence_state == BookStateV2.WARMING
    assert b.apply_delta(binance_delta(19, 20, 0, T0 + 60)).state == BookStateV2.WARMING
    assert b.apply_delta(binance_delta(21, 21, 20, T0 + 70)).state == BookStateV2.VALID
    assert b.last_update_id == 21
    assert b.feature(cutoff_ns=T0 + 20).to_canonical_json() == earlier


def test_unhealthy_wrong_source_health_and_stale_book_invalidate() -> None:
    b = book(stale=10)
    assert b.apply_snapshot(snapshot(T0, health="STALE", health_ref=HEALTH_REF)).state == BookStateV2.INVALID
    assert b.sequence_state.reason == "SOURCE_NOT_HEALTHY_CURRENT"
    b = book(stale=10)
    b.apply_snapshot(snapshot(T0))
    assert b.apply_delta(delta(T0 + 11, 101)).state == BookStateV2.INVALID
    assert b.sequence_state.reason == "STALE_FEED_REQUIRES_SNAPSHOT_RECOVERY"
    b = book()
    assert b.apply_snapshot(snapshot(T0, source="WRONG_SOURCE")).state == BookStateV2.INVALID


def test_future_delta_and_later_snapshot_recovery_do_not_rewrite_earlier_features() -> None:
    b = book(stale=100_000)
    b.apply_snapshot(snapshot(T0))
    b.apply_delta(delta(T0 + 10, 101, bid_qty="11"))
    earlier = b.feature(cutoff_ns=T0 + 10).to_canonical_json()
    b.apply_delta(delta(T0 + 20, 102, bid_qty="12"))
    assert b.feature(cutoff_ns=T0 + 10).to_canonical_json() == earlier
    b.apply_delta(delta(T0 + 30, 104))  # creates a future gap
    assert b.feature(cutoff_ns=T0 + 10).to_canonical_json() == earlier
    assert not b.feature(cutoff_ns=T0 + 30).estimable


def test_ofi_microprice_depth_band_and_recovery_epoch_hand_calculation() -> None:
    b = book(stale=1000, cadence=10)
    b.apply_snapshot(snapshot(T0))
    b.apply_delta(delta(T0 + 10, 101, bid_qty="12", ask_qty="8"))
    feature = b.feature(cutoff_ns=T0 + 10, depth_bands_bps=(Decimal("200"),))
    assert feature.ofi == Decimal("4")
    assert feature.microprice == "101.2"
    assert feature.depth_bands == (("200", "12", "8"),)
    assert feature.depth_imbalance == (("200", "0.2"),)
    assert feature.sequence_state == BookStateV2.VALID
    assert feature.recovery_epoch == 1
    assert feature.source_health_ref == HEALTH_REF
    assert feature.capability_matrix_ref == default_evidence_capability_matrix_v2().content_hash


def test_declared_one_five_thirty_second_windows_require_observed_support() -> None:
    b = book(stale=2_000_000_000, cadence=1_000_000_000)
    b.apply_snapshot(snapshot(T0))
    for offset in range(1, 31):
        b.apply_delta(delta(T0 + offset * 1_000_000_000, 100 + offset,
                            bid_qty=str(10 + offset % 2)))
    feature = b.feature(cutoff_ns=T0 + 30_000_000_000)
    assert [int(x[0]) for x in feature.price_response_windows] == [1, 5, 30]
    assert all(row[2] == "ESTIMABLE" for row in feature.price_response_windows)
    assert all(row[2] == "ESTIMABLE" for row in feature.ofi_windows)
    assert feature.ofi_windows[0][1] == "-1"
    no_cadence = book(stale=1000, cadence=None)
    no_cadence.apply_snapshot(snapshot(T0))
    no_cadence.apply_delta(delta(T0 + 10, 101))
    rows = no_cadence.feature(cutoff_ns=T0 + 10).price_response_windows
    assert all(row[2] == "DECLARED_CADENCE_UNKNOWN" for row in rows)


def test_trade_aggressor_conventions_are_explicit_and_trade_flow_needs_coverage() -> None:
    bybit_frame = frame(VenueV2.BYBIT, {"topic": "publicTrade.BTCUSDT", "data": [
        {"i": "t1", "S": "Sell", "p": "100", "v": "2", "T": 1750000000000},
    ]}, channel="publicTrade.BTCUSDT", source=SOURCE)
    bybit_trade, = parse_bybit_trades(bybit_frame, instrument=key())
    assert bybit_trade.aggressor_side == "SELL"
    assert bybit_trade.side_convention == "BYBIT_S_IS_TAKER_SIDE"
    assert bybit_trade.channel == "publicTrade.BTCUSDT"
    b = book(stale=1000, cadence=10)
    b.apply_snapshot(snapshot(T0))
    b.apply_delta(delta(T0 + 10, 101))
    feature = b.feature(cutoff_ns=T0 + 10, trades=(bybit_trade,))
    assert feature.trade_coverage_state == FeedCoverageStateV2.NOT_ESTIMABLE.value
    assert all(row[3] == "NOT_ESTIMABLE_TRADE_COVERAGE_UNKNOWN" for row in feature.signed_trade_windows)
    binance_frame = frame(VenueV2.BINANCE, {"stream": "btcusdt@aggTrade", "data": {
        "a": 5, "p": "100", "q": "3", "T": 1750000000000, "m": True,
    }}, channel="btcusdt@aggTrade", source="BINANCE_TEST")
    assert parse_binance_aggtrade(binance_frame, instrument=key(VenueV2.BINANCE)).aggressor_side == "SELL"


def test_unknown_trade_channel_or_matrix_hash_fails_coverage_closed() -> None:
    from dataclasses import replace

    start = T0 - 30_000_000_000
    b = book(stale=2_000_000_000, cadence=1_000_000_000)
    b.apply_snapshot(snapshot(start))
    for i in range(1, 31):
        b.apply_delta(delta(start + i * 1_000_000_000, 100 + i))
    coverage = FeedCoverageEvidenceV2(
        key(), SOURCE, "publicTrade.BTCUSDT", start, T0, T0,
        1_000_000_000, 31, 0, 1_000_000_000, FeedCoverageStateV2.QUALIFIED,
        HEALTH_REF, default_evidence_capability_matrix_v2().content_hash, (ref("coverage"),),
    )
    invalid_coverages = (
        replace(coverage, capability_matrix_ref=ref("unknown-matrix")),
        replace(coverage, channel="unlisted.public.channel"),
    )
    for unsupported in invalid_coverages:
        feature = b.feature(cutoff_ns=T0, trade_coverage=unsupported)
        assert feature.estimable
        assert feature.trade_coverage_state == FeedCoverageStateV2.NOT_ESTIMABLE.value
        assert all(row[3] == "NOT_ESTIMABLE_TRADE_COVERAGE_UNKNOWN"
                   for row in feature.signed_trade_windows)


def test_replenishment_persistence_and_execution_context_are_explicit() -> None:
    b = book(stale=1000, cadence=10)
    b.apply_snapshot(snapshot(T0, bid_qty="10"))
    b.apply_delta(delta(T0 + 10, 101, bid_qty="5"))
    b.apply_delta(delta(T0 + 20, 102, bid_qty="10"))
    feature = b.feature(cutoff_ns=T0 + 20)
    assert feature.persistence_proxy == "15"
    assert feature.replenishment_proxy == "5"
    context = s4_execution_quality_context(feature)
    assert context["version"] == "S4_EXECUTION_QUALITY_CONTEXT_V1"
    assert context["selector_influence"] == "ZERO"


def test_prior_only_expected_response_fit_and_absorption_cases() -> None:
    prior_refs = [ref(f"prior-{i}") for i in range(4)]
    history = tuple((T0 - 100 + i, Decimal(i + 1), Decimal(i + 1) / 10, prior_refs[i]) for i in range(3))
    # A future training point is excluded by the fit cutoff.
    history += ((T0 + 20, Decimal("100"), Decimal("100"), prior_refs[3]),)
    baseline = fit_s4_expected_response_baseline(history=history, fit_cutoff_ns=T0 - 1)
    assert baseline is not None
    assert len(baseline.input_refs) == 3
    assert baseline.fit_window_start_ns == T0 - 100
    assert baseline.fit_window_end_ns == T0 - 98
    assert ref("future-flow") not in baseline.input_refs
    current = qualified_feature()
    assert current.trade_coverage_state == "QUALIFIED"
    assert current.flow_price_response_windows[-1][3] == "ESTIMABLE"
    assert current.ofi_windows[-1][2] == "ESTIMABLE"
    positive = estimate_s4_absorption(feature=current, baseline=baseline)
    assert positive.state == "ABSORPTION_HYPOTHESIS"
    assert positive.exact_action_status == "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT"
    assert positive.signed_flow == Decimal("30")
    assert positive.opposing_liquidity_persistence == Decimal("10")

    moved = book(stale=2_000_000_000, cadence=1_000_000_000)
    moved.apply_snapshot(snapshot(T0 - 30_000_000_000))
    for i in range(1, 30):
        moved.apply_delta(delta(T0 - (30 - i) * 1_000_000_000, 100 + i))
    moved.apply_delta(L2DeltaV2(
        key(), SOURCE, CHANNEL, "BYBIT_U", None, 130, None, T0, T0, T0,
        (BookLevelV2(Decimal("100"), Decimal(0)), BookLevelV2(Decimal("90"), Decimal("10"))),
        (BookLevelV2(Decimal("102"), Decimal("10")),), ref("large-adverse-price-response"),
        "HEALTHY_CURRENT", source_health_ref=HEALTH_REF,
    ))
    current_trades = tuple(AggressiveTradeV2(
        key(), SOURCE, "publicTrade.BTCUSDT", f"trade-{i}", "BUY", Decimal("100"), Decimal("1"),
        T0 - (30 - i) * 1_000_000_000, T0 - (30 - i) * 1_000_000_000,
        T0 - (30 - i) * 1_000_000_000, ref(["trade-current", i]), "HEALTHY_CURRENT",
        "BYBIT_S_IS_TAKER_SIDE", source_health_ref=HEALTH_REF,
    ) for i in range(1, 31))
    coverage = FeedCoverageEvidenceV2(
        key(), SOURCE, "publicTrade.BTCUSDT", T0 - 30_000_000_000, T0, T0,
        1_000_000_000, 31, 0, 1_000_000_000, FeedCoverageStateV2.QUALIFIED,
        HEALTH_REF, default_evidence_capability_matrix_v2().content_hash, (ref("coverage-current"),),
    )
    negative_feature = moved.feature(cutoff_ns=T0, trades=current_trades, trade_coverage=coverage)
    negative = estimate_s4_absorption(feature=negative_feature, baseline=baseline)
    assert negative.state == "NO_ABSORPTION_HYPOTHESIS"


def test_flow_response_observation_cannot_override_bound_feature_values() -> None:
    from atlas.v2.features.context import S4FlowResponseObservationV2

    feature = qualified_feature()
    flow_row = next(row for row in feature.flow_price_response_windows if row[0] == "30")
    response_row = next(row for row in feature.price_response_windows if row[0] == "30")
    typed = S4FlowResponseObservationV2(
        feature, feature.cutoff_ns, Decimal(flow_row[1]), Decimal(response_row[1]), feature.content_hash,
    )
    assert typed.feature.content_hash == feature.content_hash
    with pytest.raises(ValueError, match="bind qualified"):
        S4FlowResponseObservationV2(
            feature, feature.cutoff_ns, Decimal(flow_row[1]) + 1, Decimal(response_row[1]), feature.content_hash,
        )


def test_feature_hash_is_cutoff_bound_and_future_markout_is_separate_outcome() -> None:
    b = book(stale=1000, cadence=10)
    b.apply_snapshot(snapshot(T0))
    b.apply_delta(delta(T0 + 10, 101))
    feature = b.feature(cutoff_ns=T0 + 10)
    b.apply_delta(delta(T0 + 20, 102))
    assert b.feature(cutoff_ns=T0 + 10).content_hash == feature.content_hash
    outcome = S4MarkoutOutcomeV2(feature.content_hash, feature.cutoff_ns, 10,
                                 feature.cutoff_ns + 10, "BUY", Decimal("101"), Decimal("-1"))
    assert outcome.to_dict()["role"] == "MATURED_OUTCOME_ONLY"
    assert "matured_markout" not in feature.to_dict()


@given(st.lists(st.integers(min_value=1, max_value=3), min_size=1, max_size=30))
def test_property_valid_sequence_never_moves_update_id_backwards(increments: list[int]) -> None:
    b = book(stale=1000, cadence=1)
    b.apply_snapshot(snapshot(T0))
    current = 100
    for i, step in enumerate(increments, start=1):
        # Any skipped ID creates a gap and no later delta can silently restore validity.
        next_id = current + step
        result = b.apply_delta(delta(T0 + i, next_id))
        if step != 1:
            assert result.state == BookStateV2.GAP_DETECTED
            assert b.apply_delta(delta(T0 + i + 1, next_id + 1)).state == BookStateV2.GAP_DETECTED
            return
        assert result.last_update_id == next_id
        current = next_id


def test_bybit_and_binance_public_parsers_keep_exact_sequence_and_timestamps() -> None:
    bybit = frame(VenueV2.BYBIT, {"topic": CHANNEL, "type": "snapshot", "ts": 1750000000000,
                                  "data": {"u": 7, "seq": 9, "b": [["100", "2"]], "a": [["102", "3"]]}},
                  channel=CHANNEL, source=SOURCE)
    parsed = parse_bybit_orderbook_frame(bybit, instrument=key(), source_health="HEALTHY_CURRENT",
                                         source_health_ref=HEALTH_REF, processed_at_ns=T0 + 5)
    assert isinstance(parsed, L2SnapshotV2)
    assert parsed.last_update_id == 7 and parsed.event_at_ns == 1_750_000_000_000_000_000
    assert parsed.received_at_ns == T0 and parsed.available_at_ns == T0 + 5
    bybit_cts = frame(VenueV2.BYBIT, {"topic": CHANNEL, "type": "snapshot", "ts": 1750000000000,
                                      "data": {"u": 8, "seq": 10, "b": [["100", "2"]],
                                               "a": [["102", "3"]]}, "cts": 1750000000001},
                      channel=CHANNEL, source=SOURCE)
    assert parse_bybit_orderbook_frame(bybit_cts, instrument=key()).event_at_ns == 1_750_000_000_001_000_000
    binance_raw = {"stream": "btcusdt@depth@100ms", "data": {
        "E": 1750000000000, "U": 7, "u": 10, "pu": 6,
        "b": [["100", "3"]], "a": [["102", "4"]],
    }}
    binance = frame(VenueV2.BINANCE, binance_raw, channel="btcusdt@depth@100ms", source="BINANCE_TEST")
    parsed_delta = parse_binance_depth_frame(binance, instrument=key(VenueV2.BINANCE),
                                             source_health="HEALTHY_CURRENT", source_health_ref=HEALTH_REF,
                                             processed_at_ns=T0 + 2)
    assert isinstance(parsed_delta, L2DeltaV2)
    assert parsed_delta.sequence_semantics == "BINANCE_U_PU"
    assert (parsed_delta.first_update_id, parsed_delta.last_update_id, parsed_delta.previous_update_id) == (7, 10, 6)
    assert parsed_delta.received_at_ns == T0 and parsed_delta.available_at_ns == T0 + 2
    snapshot_body = json.dumps({"lastUpdateId": 7, "bids": [["100", "2"]], "asks": [["102", "3"]]}).encode()
    snap = parse_binance_rest_snapshot(snapshot_body, instrument=key(VenueV2.BINANCE),
                                       source_id="BINANCE_REST", channel="USD-M depth snapshot REST",
                                       received_at_ns=T0, available_at_ns=T0, declared_depth=100,
                                       source_health="HEALTHY_CURRENT", source_health_ref=HEALTH_REF,
                                       processed_at_ns=T0)
    book_binance = SequenceValidBookV2(instrument=key(VenueV2.BINANCE), source_id="BINANCE_TEST",
                                       channel="btcusdt@depth@100ms", sequence_semantics="BINANCE_U_PU",
                                       warmup_ns=0, stale_ns=100)
    book_binance.apply_snapshot(snap)
    assert book_binance.apply_delta(parsed_delta).state == BookStateV2.VALID


def test_archive_round_trip_raw_bytes_restart_state_and_conflict_quarantine(tmp_path) -> None:
    from dataclasses import replace

    from atlas.v2.data.microstructure_archive import L2FrameArchiveV2
    from atlas.v2.data.public_microstructure_ws import raw_archive_record
    from atlas.v2.memory.repository import OpsRepository

    captured = frame(VenueV2.BYBIT, {"topic": CHANNEL, "type": "delta", "data": {"u": 101}},
                     channel=CHANNEL, source=SOURCE)
    record = raw_archive_record(
        captured, instrument=key(), frame_type="L2_DELTA", sequence_semantics="BYBIT_U",
        first_update_id=None, last_update_id=101, event_at_ns=T0,
        source_health="HEALTHY_CURRENT", source_health_ref=HEALTH_REF,
    )
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        archive = L2FrameArchiveV2(tmp_path / "archive", repository)
        chunk_id, path = archive.write_chunk((record,))
        row, = archive.read_chunk(chunk_id)
        assert path.exists()
        assert row["raw_payload_bytes"] == captured.raw_payload_bytes
        assert row["raw_payload_hash"] == captured.raw_payload_hash
        assert row["received_at_ns"] == captured.received_at_ns
        assert row["available_at_ns"] == captured.available_at_ns
        assert row["last_update_id"] == 101
        cursor, = archive.restart_cursors()
        assert cursor.high_water_update_id == 101
        assert cursor.state_after_restart == "SNAPSHOT_RECOVERY"
        assert cursor.source_health_after_restart == "INCOMPLETE_SNAPSHOT"
        same_chunk, _ = archive.write_chunk((record,))
        assert same_chunk == chunk_id
        conflicting_bytes = captured.raw_payload_bytes + b" "
        conflicting_hash = hashlib.sha256(conflicting_bytes).hexdigest()
        conflicting = replace(record, raw_payload_bytes=conflicting_bytes,
                               raw_payload_hash=conflicting_hash, received_at_ns=T0 + 1,
                               available_at_ns=T0 + 1)
        _, quarantine_path = archive.write_conflict_quarantine(record, conflicting)
        assert quarantine_path.exists()
        assert len(repository.artifact_entries("L2FrameArchiveCheckpointV2")) == 1
        assert len(repository.artifact_entries("L2ConflictQuarantineV2")) == 1


def test_public_socket_routes_are_split_allowlisted_and_credential_free() -> None:
    import inspect
    from urllib.parse import parse_qs, urlsplit

    from atlas.v2.data.public_microstructure_ws import (
        _topic_allowed,
        _venue_url,
        capture_public_frames,
    )

    bybit = _venue_url(VenueV2.BYBIT, ("orderbook.50.BTCUSDT", "publicTrade.BTCUSDT"))
    binance_depth = _venue_url(VenueV2.BINANCE, ("btcusdt@depth@100ms",))
    binance_trade = _venue_url(VenueV2.BINANCE, ("btcusdt@aggTrade",))
    assert bybit == "wss://stream.bybit.com/v5/public/linear"
    assert urlsplit(binance_depth).path == "/public/stream"
    assert parse_qs(urlsplit(binance_depth).query)["streams"] == ["btcusdt@depth@100ms"]
    assert urlsplit(binance_trade).path == "/market/stream"
    assert parse_qs(urlsplit(binance_trade).query)["streams"] == ["btcusdt@aggTrade"]
    assert "wss://fstream.binance.com/stream?" not in binance_depth
    assert "wss://fstream.binance.com/stream?" not in binance_trade
    with pytest.raises(ValueError, match="separate WebSocket routes"):
        _venue_url(VenueV2.BINANCE, ("btcusdt@depth@100ms", "btcusdt@aggTrade"))

    assert not _topic_allowed(VenueV2.BYBIT, "private/order.create")
    assert not _topic_allowed(VenueV2.BINANCE, "btcusdt@bookTicker")
    assert not _topic_allowed(VenueV2.BINANCE, "btcusdt@userData")
    with pytest.raises(ValueError, match="allowlisted"):
        _venue_url(VenueV2.BYBIT, ("private/order.create",))
    with pytest.raises(ValueError, match="allowlisted"):
        _venue_url(VenueV2.BINANCE, ("btcusdt@orderTradeUpdate",))

    credential_parameters = {"headers", "additional_headers", "api_key", "apiKey", "token", "credentials"}
    assert not credential_parameters.intersection(inspect.signature(capture_public_frames).parameters)
    transport_source = inspect.getsource(capture_public_frames)
    assert "additional_headers" not in transport_source
    assert "api_key" not in transport_source and "credentials" not in transport_source
