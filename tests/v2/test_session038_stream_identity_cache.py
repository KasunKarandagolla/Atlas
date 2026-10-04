"""Exact stream identities survive memoization, replacement and caller mutation."""

from __future__ import annotations

import hashlib
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal
from enum import Enum
from typing import Any, cast

import pytest

from atlas.v2._serialization import FrozenMap, json_value, sha256_json
from atlas.v2.data.capabilities import default_evidence_capability_matrix_v2
from atlas.v2.data.microstructure_archive import L2RawFrameV2
from atlas.v2.data.public_stream_continuity import (
    PublicStreamContinuityStateV1,
    PublicStreamContinuityTrackerV1,
    PublicStreamObservationKindV1,
    PublicStreamObservationV1,
)
from atlas.v2.instruments import EnvironmentV2, InstrumentKeyV2, ProductTypeV2, VenueV2


def key() -> InstrumentKeyV2:
    return InstrumentKeyV2(
        VenueV2.BYBIT, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
        "BTCUSDT", "BTC", "USDT", "USDT", sha256_json("revision"),
    )


def state() -> PublicStreamContinuityStateV1:
    return PublicStreamContinuityTrackerV1(
        instrument=key(), source_id="BYBIT_PUBLIC_WS", channel="publicTrade.BTCUSDT",
        metadata_ref=sha256_json("metadata"), epoch_id="run:connection:1",
    ).state


def test_instrument_and_raw_frame_identities_keep_exact_preimages_after_replacement() -> None:
    instrument = key()
    expected = sha256_json({"contract_type": "InstrumentKeyV2", "key": instrument.to_dict()})
    assert instrument.content_hash == expected == instrument.content_hash
    revised = replace(instrument, contract_revision=sha256_json("revision-two"))
    assert revised.content_hash != expected
    assert revised.content_hash == sha256_json({"contract_type": "InstrumentKeyV2", "key": revised.to_dict()})
    raw = b'{"topic":"orderbook.50.BTCUSDT"}'
    frame = L2RawFrameV2(
        instrument, "BYBIT_PUBLIC_WS", "orderbook.50.BTCUSDT", "DELTA", raw,
        hashlib.sha256(raw).hexdigest(), 90, 100, 120, None, 7, None,
        "BYBIT_U", "HEALTHY_CURRENT", "ACTUAL_SYSTEM",
    )
    expected_frame = sha256_json({
        "instrument": instrument.to_dict(), "source_id": frame.source_id,
        "channel": frame.channel, "frame_type": frame.frame_type,
        "sequence": 7, "event_at_ns": None,
    })
    assert frame.record_id == expected_frame == frame.record_id
    later = replace(frame, last_update_id=8)
    assert later.record_id != frame.record_id
    assert later.record_id == sha256_json({
        "instrument": instrument.to_dict(), "source_id": frame.source_id,
        "channel": frame.channel, "frame_type": frame.frame_type,
        "sequence": 8, "event_at_ns": None,
    })


def test_observation_cache_preserves_receipt_identity_separate_from_publication() -> None:
    observation = PublicStreamObservationV1.transport(
        instrument=key(), source_id="BYBIT_PUBLIC_WS", channel="publicTrade.BTCUSDT",
        metadata_ref=sha256_json("metadata"), epoch_id="run:connection:2",
        kind=PublicStreamObservationKindV1.RECONNECT, observed_at_ns=100, available_at_ns=120,
    )
    wire = observation.to_dict()
    expected = sha256_json({"artifact_type": "PublicStreamObservationV1", "observation": wire})
    identity = {name: value for name, value in wire.items() if name in {
        "instrument", "source_id", "channel", "metadata_ref", "epoch_id", "kind",
        "observed_at_ns", "receipt_at_ns", "event_at_ns", "evidence_ref", "trade_id",
        "trade_payload_hash", "sequence_id", "reason_code",
    }}
    identity["artifact_type"] = "PublicStreamObservationIdentityV1"
    expected_identity = sha256_json(identity)
    assert observation.content_hash == expected == observation.content_hash
    assert observation.idempotency_ref == expected_identity == observation.idempotency_ref
    later = replace(observation, available_at_ns=140)
    assert later.content_hash != observation.content_hash
    assert later.idempotency_ref == observation.idempotency_ref
    assert PublicStreamObservationV1.from_dict(wire).content_hash == expected


def test_state_copies_caller_arrays_before_memoizing_exact_hash() -> None:
    identities = [["trade-one", sha256_json("payload")]]
    replays = [sha256_json("observation")]
    reasons = ["DISCONNECT_TRADE_INTERVAL_UNREPAIRABLE"]
    # Direct construction is public too; caller-owned arrays must never make a
    # cached frozen identity mutable after validation.
    cached = replace(state(), trade_identity_cache=cast(Any, identities),
                     observation_replay_cache=cast(Any, replays), gap_reason_codes=cast(Any, reasons))
    expected = sha256_json({"artifact_type": "PublicStreamContinuityStateV1", "state": cached.to_dict()})
    assert cached.content_hash == expected
    identities[0][1] = sha256_json("changed")
    replays.append(sha256_json("later"))
    reasons.clear()
    assert cached.content_hash == expected
    assert sha256_json({"artifact_type": "PublicStreamContinuityStateV1", "state": cached.to_dict()}) == expected
    advanced = replace(cached, observed_trade_count=1)
    assert advanced.content_hash != expected
    assert PublicStreamContinuityStateV1.from_dict(cached.to_dict()).content_hash == expected
    hostile = cached.to_dict()
    hostile["trade_identity_cache"][0][1] = []
    with pytest.raises(ValueError):
        PublicStreamContinuityStateV1.from_dict(hostile)


def test_default_capability_cache_is_immutable_and_does_not_share_mutable_wire() -> None:
    matrix = default_evidence_capability_matrix_v2()
    assert default_evidence_capability_matrix_v2() is matrix
    expected = sha256_json({"artifact_type": "EvidenceCapabilityMatrixV2", "matrix": matrix.to_dict()})
    assert matrix.content_hash == expected == matrix.content_hash
    wire = matrix.to_dict()
    wire["rows"].clear()
    assert matrix.rows
    assert matrix.content_hash == expected
    assert sha256_json({"artifact_type": "EvidenceCapabilityMatrixV2", "matrix": matrix.to_dict()}) == expected
    revised = replace(matrix, version="research-identity-test")
    assert revised.content_hash != expected
    with pytest.raises(FrozenInstanceError):
        matrix.version = "mutated"  # type: ignore[misc]


def test_deferred_transition_keeps_exact_state_and_requires_durable_lookup_after_eviction() -> None:
    from .test_session032_public_stream_continuity import tracker, trade_observation

    original = tracker()
    # A bounded cache eviction is not evidence that a newly observed ID was
    # already seen. Only a completed authoritative index query can disambiguate.
    original._state = replace(original.state, trade_identity_cache_complete=False)
    denied = original.apply(trade_observation("new-one"))
    assert denied.classification.value == "TRADE_ID_HISTORY_UNAVAILABLE"
    restored = PublicStreamContinuityTrackerV1.from_state(original.state)
    observation = trade_observation("new-two")
    deferred = restored.apply_deferred(observation, durable_lookup_complete=True)
    assert deferred.classification.value == "TRADE_ACCEPTED"
    captured = deferred.seal()
    wire = restored.state.to_dict()
    assert captured.state_ref == sha256_json({"artifact_type": "PublicStreamContinuityStateV1", "state": wire})
    restored.apply(trade_observation("new-three"), durable_lookup_complete=True)
    assert deferred.seal() == captured
    assert restored.state.observed_trade_count == 2
    duplicate = restored.apply(observation, durable_prior_payload_hash=observation.trade_payload_hash,
                               durable_lookup_complete=True)
    assert duplicate.classification.value == "DUPLICATE_OBSERVATION"
    assert restored.state.observed_trade_count == 2


def test_fused_canonical_metadata_preserves_values_identity_and_caller_ownership() -> None:
    class Number(Enum):
        ONE = 1

    source: dict[str, Any] = {"decimal": Decimal("1.20"), "enum": Number.ONE,
              "nested": [{"values": [1, None, True, "x"]}], "map": FrozenMap({"d": Decimal("2.5")})}
    expected = FrozenMap(json_value(source))
    actual = FrozenMap.from_json(source)
    assert actual == expected
    assert sha256_json(actual) == sha256_json(expected)
    source["nested"][0]["values"].clear()
    assert actual == expected
    for bad in ({"x": float("nan")}, {"x": float("inf")}, {"x": object()}, {1: "value"}):
        with pytest.raises(ValueError):
            FrozenMap.from_json(cast(Any, bad))
