from __future__ import annotations

from decimal import Decimal

import pytest

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.health import PublicSourceHealthV2, PublicSourceStateV2
from atlas.v2.data.history import ImportedObservationV2, ParquetObservationArchiveV2
from atlas.v2.data.microstructure import BookLevelV2, L2SnapshotV2, SequenceValidBookV2
from atlas.v2.data.public_stream_continuity import PublicStreamContinuityStateV1
from atlas.v2.data.raw import AvailabilityClassV2, RawObservationV2
from atlas.v2.data.s3_forward_evidence import (
    BYBIT_PUBLIC_WS_SOURCE_ID_V1,
    CONTINUITY_REPORT_TYPE_V1,
    CONTINUITY_STATE_TYPE_V1,
    GENERIC_OBSERVATION_INDEX_TYPE_V2,
    MAX_REPORTED_GAP_OPENS,
    STREAM_HEALTH_TYPE_V1,
    TRADE_INDEX_TYPE_V1,
    S3ForwardTradeEvidenceV1,
    S3NativeComputationContextV1,
    evaluate_s3_warmup_readiness,
    quote_from_valid_continuity_report,
    reconstruct_s3_stream_trade_evidence,
    sequence_valid_s3_quote,
)
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.strategies.s1_trend import EventGate, EventState, ExecutableQuote
from atlas.v2.strategies.s3_mean_reversion import (
    AR_OBSERVATION_COUNT,
    CausalTradeV2,
    ResidualObservationV2,
    TradeVwapSnapshotV2,
    residual_observation,
)

T0 = 1_750_000_000_000_000_000
M1_NS = BarIntervalV2.M1.duration_ns


def _product(revision: str | None = None) -> ProductContractV2:
    key = InstrumentKeyV2(
        VenueV2.BYBIT, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
        "BTCUSDT", "BTC", "USDT", "USDT", revision or sha256_json("s34-forward-key"),
    )
    return ProductContractV2(
        key, T0, T0, T0, Decimal("1"), Decimal("0.1"), Decimal("0.001"),
        Decimal("0.001"), TradingStatusV2.TRADING, sha256_json("s34-metadata"),
    )


def _report_context(
    repo: OpsRepository,
    product: ProductContractV2,
    *,
    channel: str,
    as_of_ns: int,
    latest_valid_bbo: dict | None = None,
    book_sequence_valid: bool | None = None,
    observed_trade_count: int = 1,
    last_trade_receipt_at_ns: int | None = None,
) -> tuple[str, PublicSourceHealthV2, PublicStreamContinuityStateV1]:
    source_id = BYBIT_PUBLIC_WS_SOURCE_ID_V1
    epoch_id = "s34-test-epoch"
    recovery_ref = sha256_json({"recovery": [product.key.to_dict(), channel]})
    health = PublicSourceHealthV2(
        source_id, as_of_ns, as_of_ns, PublicSourceStateV2.HEALTHY_CURRENT,
        sha256_json({"transition": [channel, as_of_ns]}), "offline S34 current stream fixture",
    )
    transport = {
        "source_id": source_id, "instrument": product.key.to_dict(), "channel": channel,
        "metadata_ref": product.metadata_ref, "epoch_id": epoch_id,
    }
    repo.register_artifact(ArtifactIndexEntryV2(
        health.content_hash, STREAM_HEALTH_TYPE_V1, health.content_hash,
        as_of_ns, as_of_ns, {"health": health.to_dict(), "transport": transport},
    ))
    state = PublicStreamContinuityStateV1(
        product.key, source_id, channel, product.metadata_ref, epoch_id, 1, None, recovery_ref,
        as_of_ns, as_of_ns - 20_000_000, last_trade_receipt_at_ns,
        last_trade_receipt_at_ns, observed_trade_count,
        gap_count=0, transport_disconnected=False,
    )
    repo.register_artifact(ArtifactIndexEntryV2(
        state.content_hash, CONTINUITY_STATE_TYPE_V1, state.content_hash,
        as_of_ns, as_of_ns, {"state": state.to_dict()},
    ))
    report = {
        "schema_version": 1,
        "report_version": "PUBLIC_STREAM_CONTINUITY_V1",
        "instrument": product.key.to_dict(),
        "source_id": source_id,
        "channel": channel,
        "metadata_ref": product.metadata_ref,
        "contract_revision": product.key.contract_revision,
        "metadata_status": "TRADING",
        "metadata_current": True,
        "epoch_id": epoch_id,
        "recovery_epoch": 1,
        "prior_recovery_ref": None,
        "current_recovery_ref": recovery_ref,
        "as_of_ns": as_of_ns,
        "transport_received": True,
        "last_transport_receipt_at_ns": as_of_ns - 20_000_000,
        "source_current": True,
        "source_health_ref": health.content_hash,
        "source_health_epoch_match": True,
        "book_sequence_valid": book_sequence_valid,
        "observed_trade_evidence": observed_trade_count > 0,
        "observed_trade_count": observed_trade_count,
        "last_trade_receipt_at_ns": last_trade_receipt_at_ns,
        "trade_completeness_proven": False,
        "strategy_input_qualified": False,
        "latest_valid_bbo": (
            {**latest_valid_bbo,
             "input_refs": sorted(set(latest_valid_bbo.get("input_refs", ())) | {health.content_hash})}
            if latest_valid_bbo else None
        ),
        "bbo_stale_or_unavailable_reason": None if latest_valid_bbo else "BOOK_WARMING",
        "gap_count": 0,
        "gap_reason_codes": [],
        "trade_id_semantics": (
            "i is a trade identity; exact duplicate comparison uses canonical per-trade row hash when supplied; "
            "seq may be shared across grouped records"
        ),
        "trade_side_semantics": "S is taker side (Buy/Sell); parser convention BYBIT_S_IS_TAKER_SIDE",
        "trade_recovery_semantics": (
            "no declared cursor or historical repair; disconnect/reconnect intervals remain unsupported"
        ),
        "sequence_semantics": "BYBIT_U" if channel.startswith("orderbook.") else None,
        "capability_matrix_ref": sha256_json("capability matrix"),
        "capability_row_ref": sha256_json("capability row"),
        "capability_status": "UNVERIFIED",
        "coverage_censoring_limitations": "coverage unknown",
        "gap_reset_reconnect_behavior": "disconnect loses events",
        "repair_capability": "no historical repair declared",
        "source_health_requirement": "HEALTHY_CURRENT",
        "permitted_uses": ["signed aggressive flow when qualified"],
        "explicitly_unsupported_uses": ["complete historical trade coverage"],
        "reasons": ["TRADE_COMPLETENESS_UNSUPPORTED_NO_DECLARED_CURSOR_OR_HISTORY_REPAIR"],
    }
    report_ref = sha256_json({"artifact_type": CONTINUITY_REPORT_TYPE_V1, "report": report})
    repo.register_artifact(ArtifactIndexEntryV2(
        report_ref, CONTINUITY_REPORT_TYPE_V1, report_ref, as_of_ns, as_of_ns,
        {"report": report, "state_ref": state.content_hash, "source_health_ref": health.content_hash,
        "transport": transport},
    ))
    return report_ref, health, state


def _ws_trade(
    repo: OpsRepository,
    archive_root,
    product: ProductContractV2,
    *,
    cutoff_ns: int,
    event_at_ns: int = T0,
    available_at_ns: int = T0 + 100_000_000,
    received_at_ns: int = T0 + 50_000_000,
    index_type: str = TRADE_INDEX_TYPE_V1,
    key_json: str | None = None,
    revision: str | None = None,
) -> str:
    row = {
        "T": event_at_ns // 1_000_000,
        "s": product.key.native_symbol,
        "S": "Buy",
        "v": "0.25",
        "p": "100.5",
        "i": "trade-s34-1",
    }
    raw_bytes = canonical_json(row).encode("utf-8")
    observation = RawObservationV2.build(
        instrument_revision=revision or product.key.contract_revision,
        source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1,
        event_type="TRADE", event_at_ns=event_at_ns, published_at_ns=None,
        received_at_ns=received_at_ns, ingested_at_ns=available_at_ns,
        available_at_ns=available_at_ns, translation_version="bybit-public-ws-trade-v1",
        payload=raw_bytes, quality_flags=("TRADE_COMPLETENESS_UNPROVEN",), sequence="trade-s34-1",
    )
    chunk = "s34-trade-chunk"
    ParquetObservationArchiveV2(archive_root).write_observation_chunk(
        chunk, (ImportedObservationV2(1, observation, raw_bytes),),
    )
    index_ref = sha256_json({"artifact_type": index_type, "record_id": observation.record_id})
    metadata = {
        "record_id": observation.record_id,
        "source_id": observation.source_id,
        "event_type": observation.event_type,
        "instrument_revision": observation.instrument_revision,
        "instrument_key_json": key_json or product.key.to_canonical_json(),
        "event_at_ns": observation.event_at_ns,
        "published_at_ns": observation.published_at_ns,
        "translation_version": observation.translation_version,
        "revision_of": observation.revision_of,
        "quality_flags": list(observation.quality_flags),
        "availability_class": observation.availability_class.value,
        "replay_available_at_ns": observation.replay_available_at_ns,
        "raw_payload_hash": observation.raw_payload_hash,
        "bar_content_hash": None,
        "archive_chunk_id": chunk,
    }
    repo.register_artifact(ArtifactIndexEntryV2(
        index_ref, index_type, observation.content_hash,
        observation.received_at_ns, observation.available_at_ns, metadata,
    ))
    return index_ref


def test_ws_trade_reconstructs_exact_identity_but_never_proves_completeness(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        product = _product()
        cutoff = T0 + 1_000_000_000
        report_ref, health, _ = _report_context(
            repo, product, channel="publicTrade.BTCUSDT", as_of_ns=cutoff,
            last_trade_receipt_at_ns=T0 + 50_000_000,
        )
        _ws_trade(repo, tmp_path / "archive", product, cutoff_ns=cutoff)

        result = reconstruct_s3_stream_trade_evidence(
            repo, tmp_path / "archive", product=product, cutoff_ns=cutoff,
            continuity_report_ref=report_ref,
        )

        assert isinstance(result, S3ForwardTradeEvidenceV1)
        assert len(result.trades) == result.observed_trade_count == 1
        trade = result.trades[0]
        assert isinstance(trade, CausalTradeV2)
        assert (trade.key, trade.trade_id, trade.source_id, trade.aggressor_side) == (
            product.key, "trade-s34-1", BYBIT_PUBLIC_WS_SOURCE_ID_V1, "BUY",
        )
        assert trade.availability_class == AvailabilityClassV2.ACTUAL_SYSTEM
        assert trade.raw_observation_ref == result.trade_refs[0]
        assert result.source_health_ref == health.content_hash
        assert result.trade_completeness_proven is False
        assert result.status == "TEST GATE"
        assert "BYBIT_TRADE_COMPLETENESS_UNPROVEN" in result.reason_codes


def test_forward_trade_timing_keeps_source_cutoff_and_records_real_production(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        product = _product()
        cutoff = T0 + 1_000_000_000
        report_ref, health, _state = _report_context(
            repo, product, channel="publicTrade.BTCUSDT", as_of_ns=cutoff,
            last_trade_receipt_at_ns=T0 + 50_000_000,
        )
        _ws_trade(repo, tmp_path / "archive", product, cutoff_ns=cutoff)
        context = S3NativeComputationContextV1(
            cutoff, cutoff + 10, cutoff + 20, cutoff + 100,
        )

        source_view = reconstruct_s3_stream_trade_evidence(
            repo, tmp_path / "archive", product=product, cutoff_ns=cutoff,
            continuity_report_ref=report_ref,
        )
        result = source_view.with_computation_context(context, repository=repo)
        body = result.to_dict()

        assert result.cutoff_ns == cutoff
        assert body["computation_context"]["evidence_cutoff_ns"] == cutoff
        assert body["computation_context"]["computation_started_ns"] == cutoff + 10
        assert body["computation_context"]["computation_finished_ns"] == cutoff + 20
        assert body["computation_context"]["produced_at_ns"] == cutoff + 20
        assert body["computation_context"]["consumer_deadline_ns"] == cutoff + 100
        assert body["continuity_report_as_of_ns"] == cutoff
        assert body["source_health_observed_at_ns"] == health.observed_at_ns == cutoff
        assert body["source_health_available_at_ns"] == health.available_at_ns == cutoff
        assert body["trades"][0]["received_at_ns"] <= cutoff
        assert body["trades"][0]["available_at_ns"] <= cutoff
        assert result.trade_completeness_proven is False


def test_later_source_health_report_is_not_admitted_at_fixed_trade_cutoff(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        product = _product()
        cutoff = T0 + 500_000_000
        later_report_ref, _health, _state = _report_context(
            repo, product, channel="publicTrade.BTCUSDT", as_of_ns=cutoff + 1,
            last_trade_receipt_at_ns=T0 + 50_000_000,
        )
        _ws_trade(repo, tmp_path / "archive", product, cutoff_ns=cutoff,
                  available_at_ns=cutoff)
        context = S3NativeComputationContextV1(
            cutoff, cutoff + 5, cutoff + 10, cutoff + 50,
        )

        result = reconstruct_s3_stream_trade_evidence(
            repo, tmp_path / "archive", product=product, cutoff_ns=cutoff,
            continuity_report_ref=later_report_ref, computation_context=context,
        )

        assert result.trades == ()
        assert result.status == "NOT_ESTIMABLE"
        assert result.computation_context == context
        assert result.continuity_report_as_of_ns is None
        assert result.source_health_ref is None


def test_context_roundtrip_and_fixed_deadline_validation():
    cutoff = T0 + 1_000
    context = S3NativeComputationContextV1(cutoff, cutoff + 1, cutoff + 2, cutoff + 10)

    assert S3NativeComputationContextV1.from_dict(context.to_dict()) == context
    with pytest.raises(ValueError, match="fixed causal deadline"):
        S3NativeComputationContextV1(cutoff, cutoff + 1, cutoff + 11, cutoff + 10)


def test_wrong_generic_index_type_does_not_reconstruct_stream_trade(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        product = _product()
        cutoff = T0 + 1_000_000_000
        report_ref, _health, _state = _report_context(
            repo, product, channel="publicTrade.BTCUSDT", as_of_ns=cutoff,
            last_trade_receipt_at_ns=T0 + 50_000_000,
        )
        _ws_trade(repo, tmp_path / "archive", product, cutoff_ns=cutoff,
                  index_type=GENERIC_OBSERVATION_INDEX_TYPE_V2)

        result = reconstruct_s3_stream_trade_evidence(
            repo, tmp_path / "archive", product=product, cutoff_ns=cutoff,
            continuity_report_ref=report_ref,
        )

        assert result.trades == ()
        assert result.status == "NOT_ESTIMABLE"
        assert result.reason_codes == ("S3_WS_TRADE_INDEX_TYPE_INVALID",)


def test_wrong_key_index_and_wrong_revision_fail_closed(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        product = _product()
        cutoff = T0 + 1_000_000_000
        report_ref, _health, _state = _report_context(
            repo, product, channel="publicTrade.BTCUSDT", as_of_ns=cutoff,
            last_trade_receipt_at_ns=T0 + 50_000_000,
        )
        _ws_trade(repo, tmp_path / "archive", product, cutoff_ns=cutoff,
                  key_json=_product(sha256_json("other-revision")).key.to_canonical_json())

        result = reconstruct_s3_stream_trade_evidence(
            repo, tmp_path / "archive", product=product, cutoff_ns=cutoff,
            continuity_report_ref=report_ref,
        )

        assert result.trades == ()
        assert result.status == "NOT_ESTIMABLE"
        assert result.reason_codes == ("NO_EXACT_CUTOFF_AVAILABLE_WS_TRADES",)


def test_wrong_instrument_revision_is_not_admitted(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        product = _product()
        cutoff = T0 + 1_000_000_000
        report_ref, _health, _state = _report_context(
            repo, product, channel="publicTrade.BTCUSDT", as_of_ns=cutoff,
            last_trade_receipt_at_ns=T0 + 50_000_000,
        )
        _ws_trade(repo, tmp_path / "archive", product, cutoff_ns=cutoff,
                  revision=sha256_json("wrong-instrument-revision"))

        result = reconstruct_s3_stream_trade_evidence(
            repo, tmp_path / "archive", product=product, cutoff_ns=cutoff,
            continuity_report_ref=report_ref,
        )

        assert result.trades == ()
        assert result.status == "NOT_ESTIMABLE"


def test_conflicting_archived_payload_for_one_trade_identity_fails_closed(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        product = _product()
        cutoff = T0 + 1_000_000_000
        report_ref, _health, _state = _report_context(
            repo, product, channel="publicTrade.BTCUSDT", as_of_ns=cutoff,
            last_trade_receipt_at_ns=T0 + 50_000_000,
        )
        _ws_trade(repo, tmp_path / "archive", product, cutoff_ns=cutoff)
        conflicting_payload = canonical_json({
            "T": T0 // 1_000_000, "s": "BTCUSDT", "S": "Sell", "v": "0.25",
            "p": "99.5", "i": "trade-s34-1",
        }).encode("utf-8")
        conflicting_observation = RawObservationV2.build(
            instrument_revision=product.key.contract_revision,
            source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1, event_type="TRADE", event_at_ns=T0,
            published_at_ns=None, received_at_ns=T0 + 60_000_000, ingested_at_ns=T0 + 70_000_000,
            available_at_ns=T0 + 70_000_000, translation_version="bybit-public-ws-trade-v1",
            payload=conflicting_payload, quality_flags=("TRADE_COMPLETENESS_UNPROVEN",),
            sequence="trade-s34-1",
        )
        ParquetObservationArchiveV2(tmp_path / "archive").write_observation_chunk(
            "s34-conflicting-trade-chunk",
            (ImportedObservationV2(1, conflicting_observation, conflicting_payload),),
        )

        result = reconstruct_s3_stream_trade_evidence(
            repo, tmp_path / "archive", product=product, cutoff_ns=cutoff,
            continuity_report_ref=report_ref,
        )

        assert result.trades == ()
        assert result.reason_codes == ("S3_TRADE_ARCHIVE_PAYLOAD_CONFLICT",)


def test_healthy_stream_report_and_current_health_still_do_not_prove_completeness(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        product = _product()
        cutoff = T0 + 1_000_000_000
        report_ref, health, state = _report_context(
            repo, product, channel="publicTrade.BTCUSDT", as_of_ns=cutoff,
            last_trade_receipt_at_ns=T0 + 50_000_000,
        )
        _ws_trade(repo, tmp_path / "archive", product, cutoff_ns=cutoff)

        result = reconstruct_s3_stream_trade_evidence(
            repo, tmp_path / "archive", product=product, cutoff_ns=cutoff,
            continuity_report_ref=report_ref,
        )

        assert health.state == PublicSourceStateV2.HEALTHY_CURRENT
        assert state.gap_count == 0
        assert result.status == "TEST GATE"
        assert result.trade_completeness_proven is False


def test_trade_disconnect_after_report_remains_a_cutoff_gap(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        product = _product()
        cutoff = T0 + 1_000_000_000
        report_ref, _health, _state = _report_context(
            repo, product, channel="publicTrade.BTCUSDT", as_of_ns=cutoff - 100_000_000,
            last_trade_receipt_at_ns=T0 + 50_000_000,
        )
        _ws_trade(repo, tmp_path / "archive", product, cutoff_ns=cutoff)
        gap_body = {
            "instrument": product.key.to_dict(),
            "source_id": BYBIT_PUBLIC_WS_SOURCE_ID_V1,
            "channel": "publicTrade.BTCUSDT",
            "decision": {"classification": "RECOVERY_EPOCH_STARTED"},
        }
        gap_ref = sha256_json({"artifact_type": "PublicStreamContinuityEventV1", "body": gap_body})
        repo.register_artifact(ArtifactIndexEntryV2(
            gap_ref, "PublicStreamContinuityEventV1", gap_ref,
            cutoff - 50_000_000, cutoff - 50_000_000, {"observation": gap_body},
        ))

        result = reconstruct_s3_stream_trade_evidence(
            repo, tmp_path / "archive", product=product, cutoff_ns=cutoff,
            continuity_report_ref=report_ref,
        )

        assert result.trades == ()
        assert result.status == "NOT_ESTIMABLE"
        assert result.reason_codes == ("S3_TRADE_CONTINUITY_GAP_AT_CUTOFF",)


def test_late_ws_trade_is_excluded_at_cutoff(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        product = _product()
        cutoff = T0 + 500_000_000
        report_ref, _health, _state = _report_context(
            repo, product, channel="publicTrade.BTCUSDT", as_of_ns=cutoff,
            last_trade_receipt_at_ns=T0 + 50_000_000,
        )
        _ws_trade(repo, tmp_path / "archive", product, cutoff_ns=cutoff,
                  available_at_ns=cutoff + 1)

        result = reconstruct_s3_stream_trade_evidence(
            repo, tmp_path / "archive", product=product, cutoff_ns=cutoff,
            continuity_report_ref=report_ref,
        )

        assert result.trades == ()
        assert "NO_EXACT_CUTOFF_AVAILABLE_WS_TRADES" in result.reason_codes


def test_valid_persisted_sequence_report_bridges_fresh_bbo_and_roundtrips(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        product = _product()
        channel = "orderbook.50.BTCUSDT"
        as_of = T0 + 300_000_000
        received = T0 + 250_000_000
        raw_book_ref = sha256_json("s34-exact-book-frame")
        frame_body = {
            "record_id": sha256_json("s34-frame-record"),
            "instrument": product.key.to_dict(),
            "instrument_hash": product.key.content_hash,
            "source_id": BYBIT_PUBLIC_WS_SOURCE_ID_V1,
            "channel": channel,
            "frame_type": "SNAPSHOT",
            "event_at_ns": received,
            "received_at_ns": received,
            "available_at_ns": received,
            "raw_payload_hash": raw_book_ref,
            "archive_chunk_id": "s34-book-chunk",
            "sequence_semantics": "BYBIT_U",
            "authority": "ZERO",
        }
        frame_ref = sha256_json({"artifact_type": "PublicStreamFrameIndexV1",
                                 "record_id": frame_body["record_id"]})
        repo.register_artifact(ArtifactIndexEntryV2(
            frame_ref, "PublicStreamFrameIndexV1", sha256_json(frame_body),
            received, received, frame_body,
        ))
        report_ref, _health, _state = _report_context(
            repo, product, channel=channel, as_of_ns=as_of,
            latest_valid_bbo={
                "bid_price": "100", "ask_price": "101", "received_at_ns": received,
                "data_age_ns": as_of - received,
                "input_refs": [raw_book_ref],
            },
            book_sequence_valid=True, observed_trade_count=0,
        )

        source_cutoff = as_of + 100_000_000
        context = S3NativeComputationContextV1(
            source_cutoff, source_cutoff + 10, source_cutoff + 20, source_cutoff + 100,
        )
        bridge = quote_from_valid_continuity_report(
            repo, product, cutoff_ns=source_cutoff, continuity_report_ref=report_ref,
        ).with_computation_context(context, repository=repo)
        roundtrip = type(bridge).from_dict(bridge.to_dict())

        assert bridge.status == "AVAILABLE"
        assert bridge.continuity_report_ref == report_ref
        assert bridge.bbo_age_ns == 150_000_000
        assert bridge.observed_at_ns == received
        assert bridge.available_at_ns == as_of
        assert bridge.computation_context == context
        assert bridge.to_dict()["source_available_at_ns"] == as_of
        assert bridge.to_dict()["continuity_report_as_of_ns"] == as_of
        assert bridge.to_dict()["source_health_observed_at_ns"] == as_of
        assert bridge.to_dict()["produced_at_ns"] == source_cutoff + 20
        assert bridge.to_dict()["produced_at_ns"] > bridge.cutoff_ns
        assert bridge.quote is not None
        assert bridge.quote.valid_at(source_cutoff, 1_000_000_000)
        assert report_ref in bridge.input_refs
        assert roundtrip.to_dict() == bridge.to_dict()


def test_bbo_report_cannot_admit_a_quote_received_after_the_fixed_cutoff(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        product = _product()
        channel = "orderbook.50.BTCUSDT"
        cutoff = T0 + 1_000_000_000
        report_ref, _health, _state = _report_context(
            repo, product, channel=channel, as_of_ns=cutoff,
            latest_valid_bbo={
                "bid_price": "100", "ask_price": "101", "received_at_ns": cutoff + 1,
                "data_age_ns": 0, "input_refs": [sha256_json("future-book-frame")],
            },
            book_sequence_valid=True, observed_trade_count=0,
        )
        context = S3NativeComputationContextV1(cutoff, cutoff + 5, cutoff + 10, cutoff + 50)

        result = quote_from_valid_continuity_report(
            repo, product, cutoff_ns=cutoff, continuity_report_ref=report_ref,
        ).with_computation_context(context, repository=repo)

        body = result.to_dict()
        assert result.status == "NOT_ESTIMABLE"
        assert result.quote is None
        assert result.cutoff_ns == cutoff
        assert result.observed_at_ns is None
        assert result.available_at_ns is None
        assert body["computation_context"]["produced_at_ns"] == cutoff + 10
        assert body["computation_context"]["evidence_cutoff_ns"] == cutoff


def test_stale_or_invalid_book_report_never_supplies_s3_quote(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        product = _product()
        channel = "orderbook.50.BTCUSDT"
        as_of = T0 + 2_000_000_000
        received = T0
        report_ref, _health, _state = _report_context(
            repo, product, channel=channel, as_of_ns=as_of,
            latest_valid_bbo={
                "bid_price": "100", "ask_price": "101", "received_at_ns": received,
                "data_age_ns": as_of - received, "input_refs": [sha256_json("old-book")],
            },
            book_sequence_valid=True, observed_trade_count=0,
        )

        result = quote_from_valid_continuity_report(
            repo, product, cutoff_ns=as_of, continuity_report_ref=report_ref,
        )

        assert result.status == "NOT_ESTIMABLE"
        assert result.quote is None
        assert result.reason_code == "BOOK_BBO_OLDER_THAN_S3_MAX_AGE"


def test_existing_sequence_valid_book_bridges_to_s3_quote_with_exact_support_refs(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        product = _product()
        channel = "orderbook.50.BTCUSDT"
        as_of = T0 + 300_000_000
        received = T0 + 250_000_000
        source_id = BYBIT_PUBLIC_WS_SOURCE_ID_V1
        health = PublicSourceHealthV2(
            source_id, as_of, as_of, PublicSourceStateV2.HEALTHY_CURRENT,
            sha256_json({"transition": [channel, as_of]}), "offline S34 current stream fixture",
        )
        raw_book_ref = sha256_json("s34-sequence-valid-book-frame")
        book = SequenceValidBookV2(
            instrument=product.key, source_id=source_id, channel=channel,
            sequence_semantics="BYBIT_U", warmup_ns=0, stale_ns=1_000_000_000,
            declared_cadence_ns=100_000_000,
        )
        snapshot = L2SnapshotV2(
            product.key, source_id, channel, "BYBIT_U", 100, received, received, as_of,
            (BookLevelV2(Decimal("100"), Decimal("10")),),
            (BookLevelV2(Decimal("101"), Decimal("10")),), raw_book_ref,
            PublicSourceStateV2.HEALTHY_CURRENT.value, 50, source_health_ref=health.content_hash,
        )
        book.apply_snapshot(snapshot)
        feature = book.feature(cutoff_ns=as_of, availability_view="ACTUAL_RECEIPT")
        assert feature.bbo is not None and feature.data_age_ns is not None
        frame_body = {
            "record_id": sha256_json("s34-sequence-valid-book-record"),
            "instrument": product.key.to_dict(),
            "instrument_hash": product.key.content_hash,
            "source_id": source_id,
            "channel": channel,
            "frame_type": "SNAPSHOT",
            "event_at_ns": received,
            "received_at_ns": received,
            "available_at_ns": as_of,
            "raw_payload_hash": raw_book_ref,
            "archive_chunk_id": "s34-sequence-valid-book-chunk",
            "sequence_semantics": "BYBIT_U",
            "authority": "ZERO",
        }
        frame_ref = sha256_json({
            "artifact_type": "PublicStreamFrameIndexV1", "record_id": frame_body["record_id"],
        })
        repo.register_artifact(ArtifactIndexEntryV2(
            frame_ref, "PublicStreamFrameIndexV1", sha256_json(frame_body),
            as_of, as_of, frame_body,
        ))
        report_ref, report_health, _state = _report_context(
            repo, product, channel=channel, as_of_ns=as_of,
            latest_valid_bbo={
                "bid_price": feature.bbo[0], "ask_price": feature.bbo[1],
                "received_at_ns": as_of - feature.data_age_ns,
                "data_age_ns": feature.data_age_ns, "input_refs": list(feature.input_refs),
            },
            book_sequence_valid=True, observed_trade_count=0,
        )
        assert report_health.content_hash == health.content_hash

        bridge = sequence_valid_s3_quote(
            repo, book, product=product, cutoff_ns=as_of, continuity_report_ref=report_ref,
        )

        assert bridge.status == "AVAILABLE"
        assert bridge.quote is not None
        assert bridge.quote.bid == Decimal("100") and bridge.quote.ask == Decimal("101")
        assert bridge.quote.valid_at(as_of, 1_000_000_000)
        assert {frame_ref, health.content_hash, report_ref} <= set(bridge.input_refs)


def _m1_bar(product: ProductContractV2, open_ns: int, *, reconstructed: bool = False) -> CausalBarV2:
    availability = AvailabilityClassV2.RECONSTRUCTED_MARKET if reconstructed else AvailabilityClassV2.ACTUAL_SYSTEM
    raw = RawObservationV2.build(
        instrument_revision=product.key.contract_revision, source_id="BYBIT_PUBLIC_HTTP",
        event_type="BAR_1M", event_at_ns=open_ns + M1_NS, received_at_ns=open_ns + M1_NS,
        ingested_at_ns=open_ns + M1_NS, available_at_ns=open_ns + M1_NS,
        translation_version="s34-readiness-fixture", payload={"open": open_ns},
        availability_class=availability,
        replay_available_at_ns=(open_ns + M1_NS if reconstructed else None),
    )
    close = Decimal("100")
    return CausalBarV2(raw, BarIntervalV2.M1, open_ns, open_ns + M1_NS,
                       close, close, close, close, Decimal("0"), True)


def _readiness(product, bars, *, view="ACTUAL_SYSTEM"):
    cutoff = bars[-1].close_at_ns if bars else T0
    health = PublicSourceHealthV2(
        "BYBIT_PUBLIC_HTTP", cutoff, cutoff, PublicSourceStateV2.HEALTHY_CURRENT,
        sha256_json("readiness-health"), "fixture",
    )
    quote = ExecutableQuote(product.key, Decimal("99"), Decimal("101"), cutoff - 10,
                            cutoff - 5, sha256_json("readiness-bbo"))
    gate = EventGate(EventState.CLEAR, cutoff, sha256_json("readiness-gate"), "fixture")
    return evaluate_s3_warmup_readiness(
        key=product.key, cutoff_ns=cutoff, bars=bars, residuals=(), trade_vwaps=(), trades=(),
        bar_source_health=health, trade_source_health=health, quote=quote, event_gate=gate,
        point_in_time_universe_eligible=True, availability_view=view, recovery_epoch=1,
    )


def test_readiness_reports_production_after_cutoff_without_changing_readiness_authority():
    product = _product()
    bar = _m1_bar(product, T0 - T0 % M1_NS)
    source_cutoff = bar.close_at_ns
    context = S3NativeComputationContextV1(
        source_cutoff, source_cutoff + 10, source_cutoff + 20, source_cutoff + 100,
    )

    readiness = _readiness(product, (bar,)).with_computation_context(context)
    body = readiness.to_dict()

    assert readiness.cutoff_ns == source_cutoff
    assert body["computation_context"]["evidence_cutoff_ns"] == source_cutoff
    assert body["computation_context"]["computation_started_ns"] == source_cutoff + 10
    assert body["produced_at_ns"] == source_cutoff + 20
    assert body["produced_at_ns"] > readiness.cutoff_ns
    assert readiness.status == "NOT_ESTIMABLE"
    assert readiness.gate_status == "TEST GATE"
    assert readiness.trade_completeness_proven is False


def test_warmup_keeps_exact_10081_bar_requirement_and_hard_completeness_gate():
    product = _product()
    first_open = T0 - T0 % M1_NS - (AR_OBSERVATION_COUNT - 1) * M1_NS
    short = tuple(_m1_bar(product, first_open + index * M1_NS) for index in range(AR_OBSERVATION_COUNT - 1))
    exact = tuple(_m1_bar(product, first_open + index * M1_NS) for index in range(AR_OBSERVATION_COUNT))

    short_readiness = _readiness(product, short)
    exact_readiness = _readiness(product, exact)

    assert short_readiness.required_m1_bars == 10_081
    assert short_readiness.observed_contiguous_m1_bars == 10_080
    assert exact_readiness.observed_contiguous_m1_bars == 10_081
    assert exact_readiness.status == "NOT_ESTIMABLE"
    assert exact_readiness.gate_status == "TEST GATE"
    assert exact_readiness.trade_completeness_proven is False
    assert exact_readiness.to_dict()["source_recovery_epochs"] == {"trade": 1, "book": None}
    assert "TEST_GATE_BYBIT_TRADE_COMPLETENESS_UNPROVEN" in exact_readiness.reason_codes
    assert exact_readiness.to_dict()["required_preceding_standardization_residuals"] == 120


def test_warmup_gap_and_reconstructed_availability_remain_explicit():
    product = _product()
    first_open = T0 - T0 % M1_NS - (AR_OBSERVATION_COUNT + 2) * M1_NS
    bars = tuple(
        _m1_bar(product, first_open + index * M1_NS, reconstructed=True)
        for index in range(AR_OBSERVATION_COUNT + 2) if index != AR_OBSERVATION_COUNT // 2
    )

    reconstructed = _readiness(product, bars, view="RECONSTRUCTED_MARKET")
    actual = _readiness(product, bars)

    assert reconstructed.availability_view == "RECONSTRUCTED_MARKET"
    assert reconstructed.observed_contiguous_m1_bars < AR_OBSERVATION_COUNT
    assert reconstructed.gaps_open_at_ns
    assert "M1_HISTORY_GAP_OR_CONFLICT" in reconstructed.reason_codes
    assert "RECONSTRUCTED_MARKET_CANNOT_QUALIFY_ACTUAL_SYSTEM_WARMUP" in reconstructed.reason_codes
    assert actual.observed_contiguous_m1_bars == 0
    assert actual.status == "NOT_ESTIMABLE"


def test_multi_year_gap_reports_bounded_missing_open_list_and_exact_count():
    product = _product()
    first_open = T0 - T0 % M1_NS
    missing_count = MAX_REPORTED_GAP_OPENS + 100_000
    bars = (
        _m1_bar(product, first_open),
        _m1_bar(product, first_open + (missing_count + 1) * M1_NS),
    )

    readiness = _readiness(product, bars)

    assert readiness.gap_count == missing_count
    assert len(readiness.gaps_open_at_ns) == MAX_REPORTED_GAP_OPENS
    assert readiness.gaps_truncated is True
    assert "M1_HISTORY_GAP_OR_CONFLICT" in readiness.reason_codes


def test_120_residual_rows_are_insufficient_but_121_supply_standardization_only():
    product = _product()
    first_open = T0 - T0 % M1_NS - 120 * M1_NS
    bars = tuple(_m1_bar(product, first_open + index * M1_NS) for index in range(121))
    health = PublicSourceHealthV2(
        "BYBIT_PUBLIC_HTTP", bars[-1].close_at_ns, bars[-1].close_at_ns,
        PublicSourceStateV2.HEALTHY_CURRENT, sha256_json("residual-health"), "fixture",
    )
    trade_ref = sha256_json("residual-trade")
    vwaps = tuple(
        TradeVwapSnapshotV2(
            product.key, (bar.close_at_ns // 86_400_000_000_000) * 86_400_000_000_000,
            bar.close_at_ns, bar.close_at_ns, Decimal("100"), (trade_ref,),
            health.content_hash, "ACTUAL_SYSTEM",
        )
        for bar in bars
    )
    residuals: tuple[ResidualObservationV2, ...] = tuple(
        residual_observation(bar, vwap) for bar, vwap in zip(bars, vwaps, strict=True)
    )
    cutoff = bars[-1].close_at_ns
    quote = ExecutableQuote(product.key, Decimal("99"), Decimal("101"), cutoff - 10,
                            cutoff - 5, sha256_json("residual-bbo"))
    gate = EventGate(EventState.CLEAR, cutoff, sha256_json("residual-gate"), "fixture")

    readiness = evaluate_s3_warmup_readiness(
        key=product.key, cutoff_ns=cutoff, bars=bars, residuals=residuals, trade_vwaps=vwaps,
        trades=(CausalTradeV2(product.key, trade_ref, "BYBIT_PUBLIC_HTTP", "trade-residual",
                               cutoff - 1, cutoff - 1, cutoff - 1, Decimal("100"), Decimal("1")),),
        bar_source_health=health, trade_source_health=health, quote=quote, event_gate=gate,
        point_in_time_universe_eligible=True,
    )

    assert readiness.valid_residual_count == 121
    assert readiness.contiguous_residual_count == 121
    assert readiness.to_dict()["valid_preceding_standardization_residuals"] == 120
    assert readiness.status == "NOT_ESTIMABLE"
    assert "INSUFFICIENT_EXACT_TRADE_VWAP_RESIDUALS" in readiness.reason_codes
