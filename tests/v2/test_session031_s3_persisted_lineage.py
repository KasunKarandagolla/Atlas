"""Adversarial typed-artifact checks for production S3 readiness provenance."""

from __future__ import annotations

import math
import random
from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.health import PublicSourceHealthV2, PublicSourceStateV2
from atlas.v2.data.raw import RawObservationV2
from atlas.v2.instruments import EnvironmentV2, InstrumentKeyV2, ProductTypeV2, VenueV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2
from atlas.v2.science.session031_readiness import S3_REQUIRED_M1_BARS, s3_cadence_report_v1
from atlas.v2.science.session031_s3_provenance import validate_s3_persisted_lineage_v1
from atlas.v2.strategies.s1_trend import EventGate, EventState, ExecutableQuote
from atlas.v2.strategies.s3_mean_reversion import (
    CausalTradeV2,
    ResidualObservationV2,
    TradeVwapSnapshotV2,
    residual_observation,
)

M1_NS = BarIntervalV2.M1.duration_ns
CUTOFF = 1_800_000_000_000_000_000
SOURCE_ID = "BYBIT_PUBLIC_HTTP"
KEY = InstrumentKeyV2(
    VenueV2.BYBIT, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
    "BTCUSDT", "BTC", "USDT", "USDT", sha256_json("s31-s3-provenance-key"),
)
OTHER_KEY = replace(KEY, native_symbol="ETHUSDT", base_asset_id="ETH",
                    contract_revision=sha256_json("s31-s3-other-key"))


def _entry(ref: str, artifact_type: str, at_ns: int, field: str, payload: dict) -> ArtifactIndexEntryV2:
    return ArtifactIndexEntryV2(ref, artifact_type, ref, at_ns, at_ns, {field: payload})


@pytest.fixture(scope="module")
def persisted_window():
    """Exact typed links for a full numerical window; REST continuity stays unproven."""
    rng = random.Random(31_031)
    residual = 0.0
    bars: list[CausalBarV2] = []
    trades: list[CausalTradeV2] = []
    health: list[PublicSourceHealthV2] = []
    vwaps: list[ArtifactIndexEntryV2] = []
    residual_entries: list[ArtifactIndexEntryV2] = []
    first_close = CUTOFF - (S3_REQUIRED_M1_BARS - 1) * M1_NS
    for index in range(S3_REQUIRED_M1_BARS):
        close_at = first_close + index * M1_NS
        residual = 0.97 * residual + rng.gauss(0.0, 0.00015)
        close = Decimal(str(math.exp(residual) * 100.0))
        raw = RawObservationV2.build(
            instrument_revision=KEY.contract_revision, source_id=SOURCE_ID, event_type="BAR_1M",
            event_at_ns=close_at, received_at_ns=close_at, ingested_at_ns=close_at,
            available_at_ns=close_at, translation_version="s31-s3-test-v1",
            payload={"index": index, "close": str(close)},
        )
        bar = CausalBarV2(
            raw, BarIntervalV2.M1, close_at - M1_NS, close_at,
            close, max(close, Decimal("100.1")), min(close, Decimal("99.9")), close,
            Decimal("1"), True,
        )
        health_row = PublicSourceHealthV2(
            SOURCE_ID, close_at, close_at, PublicSourceStateV2.HEALTHY_CURRENT,
            sha256_json({"health-transition": index}), "typed current source-health fixture",
        )
        trade_ref = sha256_json({"persisted-public-trade-index-ref": index})
        trade = CausalTradeV2(
            KEY, trade_ref, SOURCE_ID, f"trade-{index}", close_at, close_at, close_at,
            Decimal("100"), Decimal("1"), "BUY",
        )
        vwap = TradeVwapSnapshotV2(
            KEY, close_at - close_at % 86_400_000_000_000, close_at, close_at,
            Decimal("100"), (trade_ref,), health_row.content_hash, "ACTUAL_SYSTEM",
        )
        item = residual_observation(bar, vwap)
        bars.append(bar)
        health.append(health_row)
        trades.append(trade)
        vwaps.append(_entry(vwap.content_hash, "S3TradeVwapSnapshotV2", close_at, "vwap", vwap.to_dict()))
        residual_entries.append(_entry(item.content_hash, "S3ResidualObservationV2", close_at,
                                       "residual", item.to_dict()))
    quote = ExecutableQuote(
        KEY, Decimal("99.9"), Decimal("100.1"), CUTOFF - 500_000_000,
        CUTOFF - 400_000_000, sha256_json("persisted fresh BBO"),
    )
    gate = EventGate(EventState.CLEAR, CUTOFF, sha256_json("persisted event gate"), "s31-fixture")
    return {
        "bars": tuple(bars), "trades": tuple(trades), "health": tuple(health),
        "vwaps": tuple(vwaps), "residuals": tuple(residual_entries), "quote": quote, "gate": gate,
    }


def _validate(data, *, cutoff=CUTOFF, **changes):
    values = {
        "key": KEY, "cutoff_ns": cutoff, "bars": data["bars"],
        "vwap_entries": data["vwaps"], "residual_entries": data["residuals"],
        "trades": data["trades"], "source_health": data["health"],
        "quote": data["quote"], "event_gate": data["gate"],
    }
    values.update(changes)
    return validate_s3_persisted_lineage_v1(**values)


def test_exact_persisted_lineage_is_recognized_but_recent_rest_is_not_continuity_proof(persisted_window):
    duplicate = replace(
        persisted_window["trades"][-1],
        raw_observation_ref=sha256_json("same-trade-identity-recaptured"),
    )
    report = _validate(persisted_window, trades=(*persisted_window["trades"], duplicate)).to_dict()

    assert report["lineage_status"] == "VALIDATED"
    assert report["numerical_arrays_status"] == "SUFFICIENT"
    assert report["validated_observations"] == 10_081
    assert report["expected_observations"] == 10_081
    assert report["strictly_preceding_residuals"] == 120
    assert report["duplicate_trade_identity_count"] == 1
    assert report["trade_continuity_status"] == "UNVERIFIABLE_BOUNDED_REST_RECENT_TRADE_HISTORY"
    assert report["qualification_status"] == "TEST GATE"
    assert report["processing_at_ns"] is None
    assert "S3_TRADE_CONTINUITY_UNPROVEN_BY_BOUNDED_REST_SNAPSHOTS" in report["reason_codes"]


def test_made_up_vwap_reference_does_not_validate_a_correct_count(persisted_window):
    entries = list(persisted_window["residuals"])
    last = entries[-1]
    item = ResidualObservationV2(
        KEY, last.metadata["residual"]["bar_ref"], sha256_json("made-up-vwap-ref"),
        last.metadata["residual"]["close_at_ns"], last.metadata["residual"]["available_at_ns"],
        last.metadata["residual"]["residual"], "ACTUAL_SYSTEM",
    )
    entries[-1] = _entry(item.content_hash, "S3ResidualObservationV2", item.available_at_ns,
                          "residual", item.to_dict())

    report = _validate(persisted_window, residual_entries=tuple(entries)).to_dict()

    assert report["validated_observations"] == 10_080
    assert report["lineage_status"] == "INVALID"
    assert report["qualification_status"] == "NOT ESTIMABLE"
    assert "S3_RESIDUAL_TRADE_VWAP_REFERENCE_MISSING" in report["reason_codes"]


def test_future_available_residual_is_excluded_as_of_cutoff(persisted_window):
    entries = list(persisted_window["residuals"])
    original = entries[-1].metadata["residual"]
    future = ResidualObservationV2(
        KEY, original["bar_ref"], original["vwap_ref"], original["close_at_ns"], CUTOFF + 1,
        original["residual"], "ACTUAL_SYSTEM",
    )
    entries[-1] = _entry(future.content_hash, "S3ResidualObservationV2", CUTOFF + 1,
                          "residual", future.to_dict())

    report = _validate(persisted_window, residual_entries=tuple(entries)).to_dict()

    assert report["validated_observations"] == 10_080
    assert report["qualification_status"] == "NOT ESTIMABLE"
    assert "S3_PERSISTED_RESIDUAL_WINDOW_INCOMPLETE_OR_MISALIGNED" in report["reason_codes"]


def test_noncontiguous_or_duplicate_m1_lineage_fails(persisted_window):
    missing = persisted_window["residuals"][:5_000] + persisted_window["residuals"][5_001:]
    missing_report = _validate(persisted_window, residual_entries=missing).to_dict()
    duplicate_report = _validate(persisted_window, bars=(*persisted_window["bars"],
                                                         persisted_window["bars"][-1])).to_dict()

    assert missing_report["qualification_status"] == "NOT ESTIMABLE"
    assert "S3_PERSISTED_RESIDUAL_WINDOW_INCOMPLETE_OR_MISALIGNED" in missing_report["reason_codes"]
    assert duplicate_report["lineage_status"] == "INVALID"
    assert "S3_DUPLICATE_OR_CONFLICTING_M1_BAR_ORIGIN" in duplicate_report["reason_codes"]


def test_mismatched_instrument_revision_cannot_substitute_for_vwap_lineage(persisted_window):
    entries = list(persisted_window["vwaps"])
    last = entries[-1]
    old = TradeVwapSnapshotV2.from_dict(last.metadata["vwap"])
    wrong = replace(old, key=OTHER_KEY)
    entries[-1] = _entry(wrong.content_hash, "S3TradeVwapSnapshotV2", wrong.available_at_ns,
                          "vwap", wrong.to_dict())

    report = _validate(persisted_window, vwap_entries=tuple(entries)).to_dict()

    assert report["validated_observations"] == 10_080
    assert report["lineage_status"] == "INVALID"
    assert "S3_RESIDUAL_TRADE_VWAP_REFERENCE_MISSING" in report["reason_codes"]


def test_one_second_bbo_and_current_source_health_gates_fail_closed(persisted_window):
    stale = replace(persisted_window["quote"], observed_at_ns=CUTOFF - 1_000_000_001,
                    available_at_ns=CUTOFF - 1_000_000_001)
    stale_report = _validate(persisted_window, quote=stale).to_dict()
    limited = PublicSourceHealthV2(
        SOURCE_ID, CUTOFF + 1, CUTOFF + 1, PublicSourceStateV2.DEGRADED_RATE_LIMITED,
        sha256_json("rate-limited-transition"), "fixture rate limit",
    )
    health_report = _validate(persisted_window, cutoff=CUTOFF + 1,
                              source_health=(*persisted_window["health"], limited)).to_dict()

    assert stale_report["qualification_status"] == "NOT ESTIMABLE"
    assert "S3_FRESH_EXECUTABLE_BBO_MISSING_OR_OLDER_THAN_ONE_SECOND" in stale_report["reason_codes"]
    assert health_report["qualification_status"] == "NOT ESTIMABLE"
    assert "S3_REQUIRED_CURRENT_TRADE_SOURCE_HEALTH_MISSING" in health_report["reason_codes"]


def test_blocked_event_and_conflicting_trade_identity_fail(persisted_window):
    blocked = replace(persisted_window["gate"], state=EventState.BLOCKED)
    trade = persisted_window["trades"][-1]
    conflict = replace(trade, raw_observation_ref=sha256_json("conflicting-trade-ref"), price=Decimal("101"))
    report = _validate(persisted_window, event_gate=blocked,
                       trades=(*persisted_window["trades"], conflict)).to_dict()

    assert report["lineage_status"] == "INVALID"
    assert report["qualification_status"] == "NOT ESTIMABLE"
    assert "S3_CONFLICTING_TRADE_IDENTITY" in report["reason_codes"]
    assert "S3_RELEVANT_EVENT_GATE_BLOCKED" in report["reason_codes"]


def test_m15_handoff_never_counts_as_native_m1_s3_coverage():
    report = s3_cadence_report_v1(
        expected_native_m1_origins=(60_000_000_000, 120_000_000_000),
        actual_production_handoff_origins=(900_000_000_000,),
    )

    assert report["required_native_s3_cadence_ns"] == M1_NS
    assert report["actual_production_cadence_ns"] == BarIntervalV2.M15.duration_ns
    assert report["missing_or_uncovered_origins"] == [60_000_000_000, 120_000_000_000]
    assert report["native_one_minute_coverage"] == "TEST GATE"
