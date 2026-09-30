"""Offline Session-031 public-shadow intake, readiness, identity and report replicas."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2, close_boundary_ns, translate_final_bar
from atlas.v2.data.bybit_source import CAMPAIGN_INTERVALS, BybitPublicCycleSourceV1
from atlas.v2.data.collector import PublicCollectorV2
from atlas.v2.data.history import ParquetObservationArchiveV2
from atlas.v2.data.raw import AppendStatusV2, RawObservationV2
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    InstrumentRegistryV2,
    ProductTypeV2,
    VenueV2,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime import production
from atlas.v2.runtime.ops_supervisor import OpsSupervisorV2
from atlas.v2.science.session031_campaign import (
    AGENT_FREEZE_SHA256,
    S23_EXPERIMENT_ID,
    S23_EXPERIMENT_REF,
    S23_MAXIMUM_ATTEMPTS,
    S23_MULTIPLICITY_FAMILY_ID,
    S23_MULTIPLICITY_REF,
    S23_PARAMETER_SEARCH_BUDGET,
    S23_PRIOR_ATTEMPTS,
    S30_START_SHA,
    V1_FREEZE_SHA256,
    V2_FREEZE_SHA256,
    CampaignLaneV1,
    CampaignManifestV1,
    default_lane_identities,
)
from atlas.v2.science.session031_preflight import (
    build_public_shadow_preflight_v1,
    render_preflight_summary,
)
from atlas.v2.science.session031_readiness import (
    StrategyEvidenceSnapshotV1,
    evaluate_strategy_readiness_v1,
    s3_cadence_report_v1,
)
from atlas.v2.strategies.s1_trend import ExecutableQuote

CUTOFF = 1_800_000_000_000_000
KEY = InstrumentKeyV2(VenueV2.BYBIT, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
                      "BTCUSDT", "BTC", "USDT", "USDT", sha256_json("session031-key"))
_NS_PER_MS = 1_000_000


class _FakeBybitReader:
    """In-memory response fixture; this test never calls the network."""

    def __init__(self, now_ns: int) -> None:
        self.now_ns = now_ns
        self.calls: list[str] = []

    @staticmethod
    def _response(payload: Any, received_at_ns: int):
        return SimpleNamespace(payload=payload, received_at_ns=received_at_ns)

    def instruments(self, *, limit: int = 1000, cursor: str | None = None):
        del limit, cursor
        self.calls.append("instruments")
        rows = []
        for symbol, base in (("BTCUSDT", "BTC"), ("ETHUSDT", "ETH")):
            rows.append({
                "symbol": symbol, "baseCoin": base, "quoteCoin": "USDT", "settleCoin": "USDT",
                "contractType": "LinearPerpetual", "status": "Trading", "launchTime": "0",
                "deliveryTime": "0", "priceFilter": {"tickSize": "0.1"},
                "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001",
                                  "minNotionalValue": "5", "maxOrderQty": "1000"},
            })
        return self._response({"retCode": 0, "result": {"list": rows}}, self.now_ns)

    def server_time_ns(self) -> int:
        self.calls.append("server_time")
        return self.now_ns + 1

    def klines(self, symbol: str, interval: BarIntervalV2, *, limit: int = 200):
        del limit
        frame = BarIntervalV2(interval)
        self.calls.append(f"kline:{symbol}:{frame.value}")
        latest_open = self.now_ns - frame.duration_ns
        rows = []
        for opened in (latest_open, latest_open - frame.duration_ns):
            rows.append([str(opened // _NS_PER_MS), "100", "102", "99", "101", "3", "303"])
        return self._response({"retCode": 0, "result": {"list": rows}}, self.now_ns)

    def ticker(self, symbol: str):
        self.calls.append(f"ticker:{symbol}")
        row = {"symbol": symbol, "ts": str(self.now_ns // _NS_PER_MS),
               "bid1Price": "100", "ask1Price": "100.1", "bid1Size": "2", "ask1Size": "2",
               "markPrice": "100.05", "indexPrice": "100.04", "fundingRate": "0"}
        return self._response({"retCode": 0, "result": {"list": [row]}}, self.now_ns)

    def recent_trades(self, symbol: str, *, limit: int = 100):
        del limit
        self.calls.append(f"trades:{symbol}")
        row = {"symbol": symbol, "execId": f"trade-{symbol}",
               "time": str((self.now_ns - 1_000_000) // _NS_PER_MS), "price": "100", "size": "0.1",
               "side": "Buy"}
        return self._response({"retCode": 0, "result": {"list": [row]}}, self.now_ns)


class _FakeClock:
    def __init__(self, now_ns: int) -> None:
        self.now_ns = now_ns

    def __call__(self) -> int:
        return self.now_ns


def _snapshot(**changes: Any) -> StrategyEvidenceSnapshotV1:
    initial = StrategyEvidenceSnapshotV1(
        KEY, CUTOFF, {}, False, False, None, None, None, False,
        False, False, False, False, 0, 0, (), (), (),
    )
    return replace(initial, **changes)


def _bar(interval: BarIntervalV2, opened: int, *, received: int | None = None) -> CausalBarV2:
    closed = close_boundary_ns(opened, interval)
    raw = RawObservationV2.build(
        instrument_revision=KEY.contract_revision, source_id="fixture-public", event_type=f"BAR_{interval.value}",
        event_at_ns=closed, received_at_ns=received if received is not None else closed,
        ingested_at_ns=received if received is not None else closed,
        available_at_ns=max(closed, received) if received is not None else closed,
        translation_version="fault-fixture-v1", payload={"open": str(opened), "close": str(closed)},
        sequence=str(opened),
    )
    return CausalBarV2(raw, interval, opened, closed, Decimal("100"), Decimal("101"), Decimal("99"),
                       Decimal("100"), Decimal("1"), True)


def test_default_production_remains_archive_only_and_network_free():
    port = production.create_production_port()
    assert type(port.public_source) is production.IndexedPublicCycleSourceV1
    assert type(port.inputs_provider) is production.IndexedProductionEventInputsV1
    assert type(production.create_bybit_public_port().public_source) is BybitPublicCycleSourceV1


def test_fake_bybit_source_flows_through_collector_archive_index_supervisor_and_calendar(tmp_path):
    reader = _FakeBybitReader(CUTOFF)
    source = BybitPublicCycleSourceV1(reader, clock_ns=_FakeClock(CUTOFF))
    port = production.ProductionOpsCyclePortV1(public_source=source)
    db = tmp_path / "ops.sqlite"
    with OpsSupervisorV2(db, port, clock_ns=_FakeClock(CUTOFF)) as supervisor:
        result = supervisor.run_once()
        assert supervisor.repository is not None
        repository = supervisor.repository
        assert repository.artifact_entries("ProductContractV2")
        assert repository.artifact_entries("PublicObservationIndexV2")
        assert repository.artifact_entries("OpsPublicSourceReconciliationV1")
        assert repository.artifact_entries("OpsSupervisorReceiptV1")
        assert repository.artifact_entries("DecisionCalendarEntryV2")
        assert tuple((tmp_path / "ops-observations").glob("*.parquet"))
        assert result.cycle.source_health_state == "HEALTHY_CURRENT"
    assert len([call for call in reader.calls if call.startswith("kline:")]) == 8
    assert not any("order" in call.lower() or "account" in call.lower() for call in reader.calls)


def test_source_health_reconciliation_cannot_precede_public_receipts(tmp_path):
    reader = _FakeBybitReader(CUTOFF + 100)
    port = production.ProductionOpsCyclePortV1(
        public_source=BybitPublicCycleSourceV1(reader, clock_ns=_FakeClock(CUTOFF + 100)),
    )
    with OpsSupervisorV2(tmp_path / "future-receipts.sqlite", port, clock_ns=_FakeClock(CUTOFF)) as supervisor:
        result = supervisor.run_once()
        assert supervisor.repository is not None
        health = supervisor.repository.source_health_history("BYBIT_PUBLIC_HTTP")[-1]
        assert health.available_at_ns >= CUTOFF + 100
        assert result.cycle.source_health_state != "HEALTHY_CURRENT"
        assert not result.cycle.event_receipt_refs
        assert not supervisor.repository.artifact_entries("OpsDecisionEventSourceV1")


def test_metadata_bootstrap_failure_keeps_supervisor_cycle_fail_closed(tmp_path):
    from atlas.v2.data.public_http import PublicDataError

    class UnavailableMetadata(_FakeBybitReader):
        def instruments(self, *, limit: int = 1000, cursor: str | None = None):
            del limit, cursor
            self.calls.append("instruments")
            raise PublicDataError("deterministic offline timeout")

    source = BybitPublicCycleSourceV1(UnavailableMetadata(CUTOFF), clock_ns=_FakeClock(CUTOFF))
    port = production.ProductionOpsCyclePortV1(public_source=source)
    with OpsSupervisorV2(tmp_path / "metadata-timeout.sqlite", port, clock_ns=_FakeClock(CUTOFF)) as supervisor:
        result = supervisor.run_once()
        assert supervisor.repository is not None
        health = supervisor.repository.source_health_history("BYBIT_PUBLIC_HTTP")[-1]
        assert health.status == "INCOMPLETE_SNAPSHOT"
        assert result.cycle.source_health_state != "HEALTHY_CURRENT"
        assert not result.cycle.failure_types
        assert not supervisor.repository.artifact_entries("OpsDecisionEventSourceV1")


def test_incomplete_bybit_snapshot_cannot_qualify_source_or_create_a_handoff(tmp_path):
    class MissingTrades(_FakeBybitReader):
        def recent_trades(self, symbol: str, *, limit: int = 100):
            del limit
            self.calls.append(f"trades:{symbol}")
            return self._response({"retCode": 0, "result": {"list": []}}, self.now_ns)

    port = production.ProductionOpsCyclePortV1(
        public_source=BybitPublicCycleSourceV1(MissingTrades(CUTOFF), clock_ns=_FakeClock(CUTOFF)),
    )
    with OpsSupervisorV2(tmp_path / "ops.sqlite", port, clock_ns=_FakeClock(CUTOFF)) as supervisor:
        result = supervisor.run_once()
        assert supervisor.repository is not None
        health = supervisor.repository.source_health_history("BYBIT_PUBLIC_HTTP")[-1]
        assert health.status == "INCOMPLETE_SNAPSHOT"
        assert not supervisor.repository.artifact_entries("OpsPublicSourceReconciliationV1")
        assert not supervisor.repository.artifact_entries("OpsDecisionEventSourceV1")
        assert result.cycle.source_health_state != "HEALTHY_CURRENT"


def test_bybit_request_budget_is_bounded_and_intervals_match_frozen_inputs():
    assert CAMPAIGN_INTERVALS == (BarIntervalV2.M1, BarIntervalV2.M15, BarIntervalV2.H1, BarIntervalV2.H4)
    assert len(CAMPAIGN_INTERVALS) * 2 + 2 * 2 + 1 == 13  # 8 klines, 2 tickers, 2 trade reads, server time


def test_real_receipt_time_cannot_be_backdated():
    with pytest.raises(ValueError, match="available_at_ns cannot precede"):
        RawObservationV2.build(
            instrument_revision=KEY.contract_revision, source_id="fixture-public", event_type="TRADE",
            event_at_ns=CUTOFF - 10, received_at_ns=CUTOFF, ingested_at_ns=CUTOFF,
            available_at_ns=CUTOFF - 1, translation_version="fixture-v1", payload={"trade": 1},
        )


def test_unconfirmed_bar_is_not_constructed_as_final():
    raw = RawObservationV2.build(
        instrument_revision=KEY.contract_revision, source_id="fixture-public", event_type="BAR_M1",
        event_at_ns=CUTOFF, received_at_ns=CUTOFF, ingested_at_ns=CUTOFF, available_at_ns=CUTOFF,
        translation_version="fixture-v1", payload={"bar": 1}, quality_flags=("FORMING",),
    )
    assert translate_final_bar(raw=raw, interval=BarIntervalV2.M1, open_at_ns=CUTOFF - 60_000_000_000,
                               values={"open": "1", "high": "1", "low": "1", "close": "1"},
                               final=False) is None


def test_readiness_fails_closed_on_missing_s1_context_and_required_health():
    report = evaluate_strategy_readiness_v1(_snapshot())
    assert report["sleeves"]["S1"]["status"] == "NOT_ESTIMABLE"
    assert "S1_CONFIRMED_4H_EMA_HISTORY_MISSING" in report["sleeves"]["S1"]["reason_codes"]
    assert "S1_MARK_INDEX_MISSING_OR_STALE" in report["sleeves"]["S1"]["reason_codes"]
    assert report["candidate_activity_created"] is False


def test_s2_requires_2901_contiguous_comparison_bars_and_context():
    rows = (_bar(BarIntervalV2.M15, CUTOFF - 30 * M15), _bar(BarIntervalV2.M15, CUTOFF - 15 * M15))
    report = evaluate_strategy_readiness_v1(_snapshot(bars={BarIntervalV2.M15: rows}))
    reasons = report["sleeves"]["S2"]["reason_codes"]
    assert "S2_INSUFFICIENT_2901_CONTIGUOUS_15M_BARS" in reasons
    assert "S2_CONFIRMED_H1_CONTEXT_MISSING" in reasons
    assert "S2_CONFIRMED_H4_CONTEXT_MISSING" in reasons


M15 = BarIntervalV2.M15.duration_ns


def test_s2_fragmented_history_is_not_silently_shrunk():
    rows = (_bar(BarIntervalV2.M15, CUTOFF - 90 * M15), _bar(BarIntervalV2.M15, CUTOFF - 60 * M15))
    reasons = evaluate_strategy_readiness_v1(
        _snapshot(bars={BarIntervalV2.M15: rows})
    )["sleeves"]["S2"]["reason_codes"]
    assert "S2_INSUFFICIENT_2901_CONTIGUOUS_15M_BARS" in reasons
    assert "S2_FRAGMENTED_15M_COMPARISON_HISTORY" in reasons


def test_s3_missing_trade_vwap_residual_and_one_second_bbo_stays_not_estimable():
    stale = ExecutableQuote(KEY, Decimal("99"), Decimal("100"), CUTOFF - 1_000_000_001,
                            CUTOFF - 1_000_000_001, sha256_json("stale-bbo"))
    report = evaluate_strategy_readiness_v1(_snapshot(fresh_quote=stale))
    reasons = report["sleeves"]["S3"]["reason_codes"]
    assert "S3_PUBLIC_TRADE_EVIDENCE_MISSING" in reasons
    assert "S3_HISTORICAL_TRADE_DERIVED_VWAP_HISTORY_MISSING" in reasons
    assert "S3_CAUSAL_RESIDUAL_AR_WINDOW_INCOMPLETE" in reasons
    assert "S3_BBO_EXCEEDS_ONE_SECOND_AGE_OR_MISSING" in reasons


def test_s3_can_be_ready_from_complete_causal_m1_and_trade_vwap_evidence():
    count = 10_081
    interval_ns = BarIntervalV2.M1.duration_ns
    bars = tuple(_bar(BarIntervalV2.M1, CUTOFF - count * interval_ns + i * interval_ns)
                 for i in range(count))
    residuals: list[float] = []
    value = 0.0
    for i in range(count):
        noise = (((i * 1_103_515_245 + 12_345) & 0x7FFF_FFFF) / 0x7FFF_FFFF - 0.5) * 0.001
        value = 0.97 * value + noise
        residuals.append(value)
    quote = ExecutableQuote(KEY, Decimal("100"), Decimal("100.1"), CUTOFF, CUTOFF,
                            sha256_json("fresh-one-second-bbo"))
    from atlas.v2.strategies.s1_trend import EventGate, EventState

    gate = EventGate(EventState.CLEAR, CUTOFF, sha256_json("valid-event-gate"), "fixture-v1")
    snapshot = _snapshot(
        bars={BarIntervalV2.M1: bars,
              BarIntervalV2.M15: (_bar(BarIntervalV2.M15, CUTOFF - M15),),
              BarIntervalV2.H4: (_bar(BarIntervalV2.H4, CUTOFF - BarIntervalV2.H4.duration_ns),)},
        source_health_current=True, trade_source_health_current=True,
        fresh_quote=quote, event_gate=gate, point_in_time_universe_eligible=True,
        public_trade_count=1, trade_vwap_count=count,
        residual_values=tuple(residuals),
        residual_refs=tuple(f"residual-{i}" for i in range(count)),
        trade_refs=("actual-trade-fixture-ref",),
        residual_close_times_ns=tuple(bar.close_at_ns for bar in bars),
        residual_vwap_refs=tuple(f"trade-vwap-{i}" for i in range(count)),
    )

    result = evaluate_strategy_readiness_v1(snapshot)

    assert result["sleeves"]["S3"]["status"] == "READY"
    assert result["sleeves"]["S3"]["reason_codes"] == []
    assert result["s3_ar_diagnostic"]["half_life_minutes"] is not None
    assert result["candidate_activity_created"] is False


def test_s3_cadence_report_keeps_m1_origins_separate_from_m15_handoffs():
    report = s3_cadence_report_v1(expected_native_m1_origins=(1, 2, 3, 4),
                                  actual_production_handoff_origins=(1,), source_gap_origins=(2,),
                                  warmup_incomplete_origins=(3,))
    assert report["required_native_s3_cadence_ns"] == BarIntervalV2.M1.duration_ns
    assert report["actual_production_cadence_ns"] == BarIntervalV2.M15.duration_ns
    assert report["missing_or_uncovered_origins"] == [2, 3, 4]
    assert report["gaps_by_cause"]["genuine_source_gaps"] == [2]
    assert report["gaps_by_cause"]["warmup_incomplete_gaps"] == [3]
    assert report["production_cadence_changed"] is False


def test_campaign_manifest_and_lane_provenance_are_immutable_zero_authority():
    manifest = CampaignManifestV1(
        "ATLAS_PUBLIC_SHADOW_S31", 1, S30_START_SHA, V1_FREEZE_SHA256, V2_FREEZE_SHA256,
        AGENT_FREEZE_SHA256, (("requirements-lock.txt", sha256_json("locked")),),
        (("selection", sha256_json("baseline-selection")),), S23_EXPERIMENT_ID, S23_EXPERIMENT_REF,
        S23_MAXIMUM_ATTEMPTS, S23_PARAMETER_SEARCH_BUDGET, S23_PRIOR_ATTEMPTS,
        S23_MULTIPLICITY_FAMILY_ID, S23_MULTIPLICITY_REF, "LINUX_OR_WSL_UNVERIFIED",
        "UTC_SYSTEM_CLOCK_NTP_UNVERIFIED", ("BYBIT_PUBLIC_HTTP",), ("BTCUSDT", "ETHUSDT"),
        "ALL_EXPECTED_DECISION_ORIGINS_INCLUDING_MISSING_AND_REJECTED", ("S1/S2/S3_CAUSAL_INPUTS",),
        (("max_public_http_timeout_ms", "1250"),), ("READ_ONLY_PUBLIC_GET", "SHADOW_DECISION"),
        "NOT_LAUNCHED", ("S31_ENGINEERING_CHECKPOINT_ONLY",), (), "PROHIBITED_UNASSIGNED_UNTOUCHED",
        False, False,
    )
    assert manifest.to_dict()["authority"] == "METADATA_ONLY_ZERO_STRATEGY_MODEL_EXECUTION_AUTHORITY"
    assert len(manifest.discovery_prior_attempts) == 17
    assert manifest.discovery_maximum_attempts - len(manifest.discovery_prior_attempts) == 0
    with pytest.raises(ValueError, match="cannot rename, reset, omit"):
        replace(manifest, discovery_prior_attempts=manifest.discovery_prior_attempts[:-1])
    lanes = default_lane_identities()
    assert {lane.lane for lane in lanes} == set(CampaignLaneV1)
    assert all(lane.execution_authority == "ZERO" and not lane.final_holdout_access for lane in lanes)
    assert len(manifest.content_hash) == 64


def test_read_only_preflight_has_stable_identity_and_non_authoritative_statuses(tmp_path):
    db = tmp_path / "empty-ops.sqlite"
    with OpsRepository(db):
        pass
    before = db.stat().st_mtime_ns
    with OpsRepository(db, read_only=True) as repository:
        report1 = build_public_shadow_preflight_v1(repository, as_of_ns=CUTOFF)
        report2 = build_public_shadow_preflight_v1(repository, as_of_ns=CUTOFF)
    assert report1["report_identity"] == report2["report_identity"]
    assert report1["reports"]["public_ingestion"]["status"] == "TEST GATE"
    assert report1["reports"]["strategy_and_funnel"]["status"] == "TEST GATE"
    assert report1["reports"]["recovery"]["status"] == "TEST GATE"
    assert report1["reports"]["strategy_and_funnel"]["expected_s3_native_m1_origins"][
        "native_one_minute_coverage"
    ] == "UNVERIFIED"
    assert report1["reports"]["resources"]["host_resource_budget_qualification"] == "UNVERIFIED"
    assert report1["reports"]["science_and_research"]["final_holdout"] == "UNASSIGNED / UNTOUCHED"
    assert report1["reports"]["hidden_critic"]["decision_influence"] is False
    assert report1["reports"]["hidden_critic"]["admission_influence"] is False
    assert "PRODUCTION_MATURED_OUTCOME_PRODUCER_NOT_QUALIFIED" in render_preflight_summary(report1)
    assert db.stat().st_mtime_ns == before


def test_preflight_does_not_count_science_evidence_unavailable_at_cutoff(tmp_path):
    db = tmp_path / "as-of-ops.sqlite"
    future_ref = sha256_json("future-calibration")
    with OpsRepository(db) as repository:
        repository.register_artifact(ArtifactIndexEntryV2(
            future_ref, "M0CalibrationV2", future_ref, CUTOFF + 1, CUTOFF + 1,
            {"future_fixture": True},
        ))
    with OpsRepository(db, read_only=True) as repository:
        report = build_public_shadow_preflight_v1(repository, as_of_ns=CUTOFF)
    assert report["reports"]["science_and_research"]["available_artifact_counts"]["M0CalibrationV2"] == 0


def test_disconnect_reconnect_requires_exact_snapshot_refs_and_gap_repair(tmp_path):
    from atlas.v2.data.health import PublicSourceStateV2
    from atlas.v2.instruments import ProductContractV2, TradingStatusV2

    key = KEY
    product = ProductContractV2(key, CUTOFF, CUTOFF, CUTOFF, Decimal("1"), Decimal("0.1"),
                                Decimal("0.01"), Decimal("0.01"), TradingStatusV2.TRADING,
                                sha256_json("recovery-product"))
    registry = InstrumentRegistryV2()
    registry.register(product)
    with OpsRepository(tmp_path / "recovery.sqlite") as repository:
        collector = PublicCollectorV2(repository=repository, registry=registry, clock_ns=lambda: CUTOFF + 10,
                                      archive=ParquetObservationArchiveV2(tmp_path / "recovery-archive"))
        observation = RawObservationV2.build(
            instrument_revision=key.contract_revision, source_id="recover-me", event_type="TRADE",
            event_at_ns=CUTOFF, received_at_ns=CUTOFF, ingested_at_ns=CUTOFF, available_at_ns=CUTOFF,
            translation_version="recovery-fixture-v1", payload={"id": "one"}, sequence="one",
        )
        collector.ingest(observation, raw_payload=canonical_json({"id": "one"}), instrument_key=key)
        collector.flush_archive()
        ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": observation.record_id})
        collector.on_disconnect("recover-me", at_ns=CUTOFF + 11)
        collector.reconnected("recover-me", at_ns=CUTOFF + 12)
        incomplete = collector.reconcile_after_reconnect(
            "recover-me", at_ns=CUTOFF + 13, complete_snapshot=True, missed_interval_repaired=False,
        )
        assert incomplete.state == PublicSourceStateV2.INCOMPLETE_SNAPSHOT
        assert not repository.artifact_entries("OpsPublicSourceReconciliationV1")
        recovered = collector.reconcile_after_reconnect(
            "recover-me", at_ns=CUTOFF + 14, complete_snapshot=True, missed_interval_repaired=True,
            snapshot_refs=(ref,),
        )
        assert recovered.state == PublicSourceStateV2.HEALTHY_CURRENT
        assert repository.artifact_entries("OpsPublicSourceReconciliationV1")


def test_restart_replay_is_idempotent_for_persisted_public_observation(tmp_path):
    from atlas.v2.instruments import ProductContractV2, TradingStatusV2

    key = KEY
    registry = InstrumentRegistryV2()
    registry.register(ProductContractV2(key, CUTOFF, CUTOFF, CUTOFF, Decimal("1"), Decimal("0.1"),
                                        Decimal("0.01"), Decimal("0.01"), TradingStatusV2.TRADING,
                                        sha256_json("replay-product")))
    db, archive_path = tmp_path / "restart.sqlite", tmp_path / "restart-archive"
    observation = RawObservationV2.build(
        instrument_revision=key.contract_revision, source_id="restart-source", event_type="TRADE",
        event_at_ns=CUTOFF, received_at_ns=CUTOFF, ingested_at_ns=CUTOFF, available_at_ns=CUTOFF,
        translation_version="restart-v1", payload={"id": "stable"}, sequence="stable",
    )
    payload = canonical_json({"id": "stable"})
    with OpsRepository(db) as repository:
        first = PublicCollectorV2(repository=repository, registry=registry, clock_ns=lambda: CUTOFF,
                                  archive=ParquetObservationArchiveV2(archive_path))
        assert first.ingest(observation, raw_payload=payload, instrument_key=key).append.status == AppendStatusV2.INSERTED
        first.flush_archive()
    with OpsRepository(db) as repository:
        restarted = PublicCollectorV2(repository=repository, registry=registry, clock_ns=lambda: CUTOFF + 10,
                                      archive=ParquetObservationArchiveV2(archive_path))
        before = len(repository.artifact_entries("PublicObservationIndexV2"))
        result = restarted.ingest(observation, raw_payload=payload, instrument_key=key)
        restarted.flush_archive()
        assert result.append.status == AppendStatusV2.DUPLICATE
        assert len(repository.artifact_entries("PublicObservationIndexV2")) == before


def test_read_only_wal_reader_coexists_with_baseline_writer(tmp_path):
    db = tmp_path / "wal-pressure.sqlite"
    with OpsRepository(db) as writer, OpsRepository(db, read_only=True) as reader:
        before = reader.artifact_entries("PreflightFaultReplicaV1")
        ref = sha256_json("wal-reader-evidence")
        writer.register_artifact(ArtifactIndexEntryV2(ref, "PreflightFaultReplicaV1", ref,
                                                       CUTOFF, CUTOFF, {"fixture": "temporary"}))
        after = reader.artifact_entries("PreflightFaultReplicaV1")
        assert before == ()
        assert len(after) == 1
        with pytest.raises(RuntimeError, match="read-only"):
            reader.register_artifact(ArtifactIndexEntryV2(ref, "PreflightFaultReplicaV1", ref,
                                                            CUTOFF, CUTOFF, {"fixture": "forbidden"}))


def test_preflight_rejects_writable_repository_and_missing_values_stay_unavailable(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository, pytest.raises(ValueError, match="opened read-only"):
        build_public_shadow_preflight_v1(repository, as_of_ns=CUTOFF)
    result = evaluate_strategy_readiness_v1(_snapshot())
    assert result["s3_ar_diagnostic"]["half_life_minutes"] is None
    assert result["economic_value_claim"] == "NOT ESTIMABLE"


def test_future_available_observation_is_not_admitted_at_cutoff():
    future = _bar(BarIntervalV2.M1, CUTOFF, received=CUTOFF + 1)
    report = evaluate_strategy_readiness_v1(_snapshot(bars={BarIntervalV2.M1: (future,)}))
    assert report["sleeves"]["S3"]["observed_counts"]["confirmed_1m_contiguous"] == 0


def test_campaign_lane_provenance_rejects_final_holdout_access():
    lanes = default_lane_identities()
    replica = next(item for item in lanes if item.lane == CampaignLaneV1.FAULT_REPLICA)
    with pytest.raises(ValueError, match="cannot authorize"):
        replace(replica, final_holdout_access=True)


def test_campaign_identity_cannot_change_the_frozen_s30_start():
    with pytest.raises(ValueError, match="accepted Session-030"):
        CampaignManifestV1(
            "bad", 1, "0" * 64, V1_FREEZE_SHA256, V2_FREEZE_SHA256, AGENT_FREEZE_SHA256,
            (("lock", sha256_json("x")),), (("baseline", sha256_json("y")),),
            S23_EXPERIMENT_ID, S23_EXPERIMENT_REF, S23_MAXIMUM_ATTEMPTS, S23_PARAMETER_SEARCH_BUDGET,
            S23_PRIOR_ATTEMPTS, S23_MULTIPLICITY_FAMILY_ID, S23_MULTIPLICITY_REF, "LINUX",
            "UTC", ("BYBIT",), ("BTCUSDT", "ETHUSDT"), "ALL_ORIGINS", ("CAUSAL",),
            (("rss", "bounded"),), ("PUBLIC_READ",), "NOT_LAUNCHED", (), (), "UNTOUCHED", False, False,
        )


def test_current_bybit_source_never_constructs_repository_or_makes_mutating_calls():
    reader = _FakeBybitReader(CUTOFF)
    source = BybitPublicCycleSourceV1(reader, clock_ns=_FakeClock(CUTOFF))
    assert not hasattr(source, "repository")
    assert not any("order" in name.lower() or "account" in name.lower() for name in dir(reader))


def test_offline_fault_identity_rejects_conflicting_duplicate_payload(tmp_path):
    # A copied temporary store and archive exercise the real conflict quarantine path.
    from atlas.v2.data.history import ParquetObservationArchiveV2

    key = KEY
    registry = InstrumentRegistryV2()
    from atlas.v2.instruments import ProductContractV2, TradingStatusV2

    product = ProductContractV2(key, CUTOFF, CUTOFF, CUTOFF, Decimal("1"), Decimal("0.1"),
                                Decimal("0.01"), Decimal("0.01"), TradingStatusV2.TRADING,
                                sha256_json("metadata"))
    registry.register(product)
    db = tmp_path / "fault-replica.sqlite"
    with OpsRepository(db) as repository:
        collector = PublicCollectorV2(repository=repository, registry=registry, clock_ns=lambda: CUTOFF,
                                      archive=ParquetObservationArchiveV2(tmp_path / "fault-archive"))
        first = RawObservationV2.build(
            instrument_revision=key.contract_revision, source_id="fault-source", event_type="FAULT_OBS",
            event_at_ns=CUTOFF, received_at_ns=CUTOFF, ingested_at_ns=CUTOFF, available_at_ns=CUTOFF,
            translation_version="fault-v1", payload={"value": 1},
        )
        conflict = RawObservationV2.build(
            instrument_revision=key.contract_revision, source_id="fault-source", event_type="FAULT_OBS",
            event_at_ns=CUTOFF, received_at_ns=CUTOFF, ingested_at_ns=CUTOFF, available_at_ns=CUTOFF,
            translation_version="fault-v1", payload={"value": 2},
        )
        collector.ingest(first, raw_payload=canonical_json({"value": 1}), instrument_key=key)
        result = collector.ingest(conflict, raw_payload=canonical_json({"value": 2}), instrument_key=key)
        assert result.persistent_conflict or result.append.status.value == "CONFLICT_QUARANTINED"
        assert repository.artifact_entries("PublicDuplicateConflictV2")
