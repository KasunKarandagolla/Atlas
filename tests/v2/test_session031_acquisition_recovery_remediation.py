"""Adversarial offline tests for Session-031 public acquisition and recovery."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from atlas.v2._serialization import sha256_json
from atlas.v2.data.bars import BarIntervalV2
from atlas.v2.data.bybit import SOURCE_ID
from atlas.v2.data.bybit_source import (
    MAX_ACQUISITION_DURATION_NS,
    MAX_METADATA_CACHE_AGE_NS,
    MAX_REQUESTS_PER_SNAPSHOT,
    MAX_TRADE_ROWS,
    BybitPublicCycleSourceV1,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime import production
from atlas.v2.runtime.ops_supervisor import (
    OpsCycleBatchV1,
    OpsDecisionEventV1,
    OpsRecoverySnapshotV1,
    OpsSourceStateV1,
    OpsSupervisorV2,
    OpsTerminalStatusV1,
)

START_NS = 1_800_000_000_000_000
NS_PER_MS = 1_000_000


class AdvancingClock:
    def __init__(self, now_ns: int = START_NS) -> None:
        self.now_ns = now_ns
        self.monotonic = 0

    def __call__(self) -> int:
        return self.now_ns

    def tick(self, elapsed_ns: int = 100_000_000) -> int:
        self.now_ns += elapsed_ns
        self.monotonic += elapsed_ns
        return self.now_ns

    def monotonic_ns(self) -> int:
        return self.monotonic


class AdvancingBybitReader:
    """A public-only response fake with independently advancing receipt time."""

    def __init__(self, clock: AdvancingClock, *, latency_ns: int = 100_000_000,
                 trades_per_symbol: int = 1) -> None:
        self.clock = clock
        self.latency_ns = latency_ns
        self.trades_per_symbol = trades_per_symbol
        self.calls: list[str] = []
        self.request_starts_ns: list[int] = []
        self.response_receipts_ns: list[int] = []
        self.rate_limit_next = False
        self.disconnect_next = False
        self.timeout_server_time_count = 0
        self.malformed_next = False
        self.server_time = clock.now_ns

    def _response(self, payload: Any):
        received = self.clock.tick(self.latency_ns)
        self.response_receipts_ns.append(received)
        return SimpleNamespace(payload=payload, received_at_ns=received)

    @staticmethod
    def _products() -> list[dict[str, Any]]:
        rows = []
        for symbol, base in (("BTCUSDT", "BTC"), ("ETHUSDT", "ETH")):
            rows.append({
                "symbol": symbol, "baseCoin": base, "quoteCoin": "USDT", "settleCoin": "USDT",
                "contractType": "LinearPerpetual", "status": "Trading", "launchTime": "0",
                "deliveryTime": "0", "priceFilter": {"tickSize": "0.1"},
                "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001",
                                  "minNotionalValue": "5", "maxOrderQty": "1000"},
            })
        return rows

    def instruments(self, *, limit: int = 1000, cursor: str | None = None):
        del limit, cursor
        self.calls.append("instruments")
        self.request_starts_ns.append(self.clock.now_ns)
        return self._response({"retCode": 0, "result": {"list": self._products()}})

    def server_time_ns(self) -> int:
        self.calls.append("server_time")
        self.request_starts_ns.append(self.clock.now_ns)
        self.server_time = self.clock.tick(self.latency_ns)
        self.response_receipts_ns.append(self.server_time)
        if self.timeout_server_time_count:
            self.timeout_server_time_count -= 1
            from atlas.v2.data.public_http import PublicDataError
            raise PublicDataError("offline deterministic timeout")
        return self.server_time

    def klines(self, symbol: str, interval: BarIntervalV2, *, limit: int = 200):
        del limit
        self.calls.append(f"kline:{symbol}:{BarIntervalV2(interval).value}")
        self.request_starts_ns.append(self.clock.now_ns)
        if self.malformed_next:
            self.malformed_next = False
            return self._response({"retCode": 0, "result": {"list": [["bad"]]}})
        duration = BarIntervalV2(interval).duration_ns
        latest_open = (self.server_time // duration) * duration - duration
        rows = [[str(opened // NS_PER_MS), "100", "102", "99", "101", "3", "303"]
                for opened in (latest_open, latest_open - duration)]
        return self._response({"retCode": 0, "result": {"list": rows}})

    def ticker(self, symbol: str):
        self.calls.append(f"ticker:{symbol}")
        self.request_starts_ns.append(self.clock.now_ns)
        if self.rate_limit_next:
            self.rate_limit_next = False
            from atlas.v2.data.public_http import PublicDataError
            self.clock.tick(self.latency_ns)
            raise PublicDataError("offline deterministic rate limit", status_code=429)
        if self.disconnect_next:
            self.disconnect_next = False
            from atlas.v2.data.public_http import PublicDataError
            self.clock.tick(self.latency_ns)
            raise PublicDataError("offline deterministic disconnect")
        received = self.clock.tick(self.latency_ns)
        row = {
            "symbol": symbol, "ts": str(received // NS_PER_MS), "bid1Price": "100",
            "ask1Price": "100.1", "bid1Size": "2", "ask1Size": "2",
            "markPrice": "100.05", "indexPrice": "100.04", "fundingRate": "0",
        }
        return SimpleNamespace(payload={"retCode": 0, "result": {"list": [row]}}, received_at_ns=received)

    def recent_trades(self, symbol: str, *, limit: int = 100):
        del limit
        self.calls.append(f"trades:{symbol}")
        self.request_starts_ns.append(self.clock.now_ns)
        rows = [
            {
                "symbol": symbol, "execId": f"trade-{symbol}-{self.clock.now_ns}-{index}",
                "time": str((self.clock.now_ns - (index + 1) * NS_PER_MS) // NS_PER_MS),
                "price": "100", "size": "0.1", "side": "Buy",
            }
            for index in range(self.trades_per_symbol)
        ]
        return self._response({"retCode": 0, "result": {"list": rows}})


def _port(clock: AdvancingClock, reader: AdvancingBybitReader) -> production.ProductionOpsCyclePortV1:
    return production.ProductionOpsCyclePortV1(
        public_source=BybitPublicCycleSourceV1(
            reader, clock_ns=lambda: clock.tick(50_000_000), monotonic_ns=clock.monotonic_ns,
        ),
    )


def test_advancing_receipts_are_deferred_to_the_next_causal_cycle(tmp_path):
    clock = AdvancingClock()
    reader = AdvancingBybitReader(clock)
    port = _port(clock, reader)
    with OpsSupervisorV2(tmp_path / "advancing.sqlite", port, clock_ns=clock) as supervisor:
        first = supervisor.run_once()
        assert supervisor.repository is not None
        repository = supervisor.repository
        indexed = repository.artifact_entries("PublicObservationIndexV2")
        assert indexed
        first_start = START_NS
        assert min(entry.available_at_ns for entry in indexed) > first_start
        # The receipt, controller ingestion, and availability boundaries remain distinct.
        assert reader.request_starts_ns[1] > first_start
        assert reader.response_receipts_ns[0] > reader.request_starts_ns[0]
        first_receipt = reader.response_receipts_ns[-1]
        first_available = min(entry.available_at_ns for entry in indexed)
        assert first_receipt < first_available
        for entry in indexed:
            raw = port._collector_recovery.collector.store.get(entry.metadata["record_id"])
            assert raw is not None
            assert raw.available_at_ns >= raw.received_at_ns
            assert raw.available_at_ns > first_start
            assert raw.ingested_at_ns == raw.available_at_ns
            assert entry.available_at_ns == raw.available_at_ns
        assert first.cycle.source_health_state != "HEALTHY_CURRENT"
        assert not first.cycle.event_receipt_refs
        assert not first.cycle.failure_types

        second_start = clock.now_ns
        second = supervisor.run_once()
        assert second.cycle.source_health_state == "HEALTHY_CURRENT"
        assert second.cycle.event_receipt_refs
        assert repository.artifact_entries("DecisionCalendarEntryV2")
        assert not second.cycle.failure_types
        assert reader.calls.count("instruments") == 1
        health = repository.source_health_history("BYBIT_PUBLIC_HTTP")[-1]
        assert health.available_at_ns <= second_start
        reconciliations = repository.artifact_entries("OpsPublicSourceReconciliationV1")
        matching = next(entry for entry in reconciliations if entry.available_at_ns == health.available_at_ns)
        body = matching.metadata["reconciliation"]
        assert body["recovery_epoch_ref"] == port._collector_recovery.recovery_epoch_ref
        epoch = repository.get_artifact(body["recovery_epoch_ref"])
        assert epoch is not None and epoch.available_at_ns <= health.available_at_ns


def test_bounded_request_profile_and_acquisition_deadline_are_deterministic():
    nominal_clock = AdvancingClock()
    nominal_reader = AdvancingBybitReader(nominal_clock)
    nominal_source = BybitPublicCycleSourceV1(
        nominal_reader, clock_ns=nominal_clock, monotonic_ns=nominal_clock.monotonic_ns,
    )
    nominal_source.bootstrap_products(now_ns=nominal_clock.now_ns)
    nominal = nominal_source.acquire_snapshot(now_ns=nominal_clock.now_ns)
    assert nominal.complete
    assert nominal.request_count == nominal.successful_request_count == MAX_REQUESTS_PER_SNAPSHOT == 13
    assert nominal.bootstrap_request_count == nominal.successful_bootstrap_request_count == 1
    assert nominal.request_count + nominal.bootstrap_request_count == 14
    assert nominal.acquisition_duration_ns == 1_400_000_000
    assert nominal.acquisition_duration_ns < MAX_ACQUISITION_DURATION_NS
    assert sum(call.startswith("kline:") for call in nominal_reader.calls) == 8
    assert sum(call.startswith("ticker:") for call in nominal_reader.calls) == 2
    assert sum(call.startswith("trades:") for call in nominal_reader.calls) == 2
    nominal_clock.tick(MAX_METADATA_CACHE_AGE_NS + 1)
    nominal_source.bootstrap_products(now_ns=nominal_clock.now_ns)
    assert nominal_reader.calls.count("instruments") == 2

    slow_clock = AdvancingClock()
    slow_reader = AdvancingBybitReader(slow_clock, latency_ns=1_250_000_000)
    slow_source = BybitPublicCycleSourceV1(
        slow_reader, clock_ns=slow_clock, monotonic_ns=slow_clock.monotonic_ns,
    )
    slow_source.bootstrap_products(now_ns=slow_clock.now_ns)
    slow = slow_source.acquire_snapshot(now_ns=slow_clock.now_ns)
    assert not slow.complete
    assert slow.failure_reason == "BYBIT_PUBLIC_ACQUISITION_BUDGET_EXCEEDED"
    assert slow.request_count < MAX_REQUESTS_PER_SNAPSHOT
    assert slow.acquisition_duration_ns <= MAX_ACQUISITION_DURATION_NS
    assert slow.bootstrap_request_count == 1
    assert len([call for call in slow_reader.calls if call != "instruments"]) == slow.request_count


def test_malformed_and_partial_snapshots_remain_bounded_and_fail_closed():
    clock = AdvancingClock()
    reader = AdvancingBybitReader(clock)
    source = BybitPublicCycleSourceV1(reader, clock_ns=clock, monotonic_ns=clock.monotonic_ns)
    source.bootstrap_products(now_ns=clock.now_ns)
    reader.malformed_next = True
    snapshot = source.acquire_snapshot(now_ns=clock.now_ns)
    assert not snapshot.complete
    assert snapshot.failure_kind == "MALFORMED"
    assert len(snapshot.records) <= 1_810
    assert snapshot.request_count <= MAX_REQUESTS_PER_SNAPSHOT


def test_repeated_transport_timeouts_do_not_retry_or_expand_request_budget():
    clock = AdvancingClock()
    reader = AdvancingBybitReader(clock, latency_ns=1_250_000_000)
    source = BybitPublicCycleSourceV1(reader, clock_ns=clock, monotonic_ns=clock.monotonic_ns)
    reader.timeout_server_time_count = 3
    for attempt in range(3):
        source.bootstrap_products(now_ns=clock.now_ns)
        snapshot = source.acquire_snapshot(now_ns=clock.now_ns)
        assert not snapshot.complete
        assert snapshot.failure_kind == "DISCONNECTED"
        assert snapshot.request_count == 1
        expected_duration = 2_500_000_000 if attempt == 0 else 1_250_000_000
        assert snapshot.acquisition_duration_ns == expected_duration
    reader.latency_ns = 100_000_000
    source.bootstrap_products(now_ns=clock.now_ns)
    recovered = source.acquire_snapshot(now_ns=clock.now_ns)
    assert recovered.complete
    assert recovered.request_count == MAX_REQUESTS_PER_SNAPSHOT


def test_rate_limit_and_disconnect_fail_closed_and_recovery_does_not_claim_trade_repair(tmp_path):
    for failure_mode in ("rate_limit_next", "disconnect_next"):
        path = tmp_path / f"{failure_mode}.sqlite"
        clock = AdvancingClock()
        reader = AdvancingBybitReader(clock, trades_per_symbol=MAX_TRADE_ROWS)
        port = _port(clock, reader)
        with OpsSupervisorV2(path, port, clock_ns=clock) as supervisor:
            first = supervisor.run_once()
            assert first.cycle.source_health_state != "HEALTHY_CURRENT"
            failure = supervisor.run_once()
            assert supervisor.repository is not None
            setattr(reader, failure_mode, True)
            failed_call = supervisor.run_once()
            states = [item.status for item in supervisor.repository.source_health_history("BYBIT_PUBLIC_HTTP")]
            expected = "DEGRADED_RATE_LIMITED" if failure_mode == "rate_limit_next" else "DISCONNECTED"
            assert expected in states
            assert failed_call.cycle.source_health_state != "HEALTHY_CURRENT"
            assert failed_call.cycle.event_receipt_refs == ()
            current_receipts = len(supervisor.repository.artifact_entries("OpsSupervisorReceiptV1"))
            assert failed_call.cycle.failure_types == ()
            assert failure.cycle.failure_types == ()

            recovery_cycle = supervisor.run_once()
            evidence_entries = supervisor.repository.artifact_entries("BybitPublicRecoveryEvidenceV1")
            assert evidence_entries
            latest_evidence_entry = max(evidence_entries, key=lambda item: item.available_at_ns)
            latest_evidence = latest_evidence_entry.metadata["recovery_evidence"]
            assert latest_evidence["observed_trade_records"] == 2 * MAX_TRADE_ROWS
            assert latest_evidence["bar_gaps_repaired"] is True
            assert latest_evidence["trade_continuity_proven"] is False
            assert latest_evidence["trade_gap_status"] == "UNVERIFIABLE"
            assert latest_evidence["s3_historical_vwap_coverage"] == "TEST GATE"
            assert "BYBIT_RECENT_TRADE_WINDOW_DOES_NOT_PROVE_TRADE_CONTINUITY" in latest_evidence["reason_codes"]
            assert recovery_cycle.cycle.source_health_state != "HEALTHY_CURRENT"
            assert len(supervisor.repository.artifact_entries("OpsSupervisorReceiptV1")) == current_receipts


def test_stale_market_observation_is_not_replayed_as_timely(tmp_path):
    clock = AdvancingClock()
    reader = AdvancingBybitReader(clock)
    port = _port(clock, reader)
    with OpsSupervisorV2(tmp_path / "stale.sqlite", port, clock_ns=clock) as supervisor:
        first = supervisor.run_once()
        assert not first.cycle.event_receipt_refs
        clock.tick(5_000_000_001)
        stale = supervisor.run_once()
        assert stale.cycle.source_health_state == "HEALTHY_CURRENT"
        assert not stale.cycle.event_receipt_refs
        assert not supervisor.repository.artifact_entries("OpsDecisionEventSourceV1")


def test_final_m15_bar_that_expires_during_acquisition_never_becomes_a_handoff(tmp_path):
    m15_ns = BarIntervalV2.M15.duration_ns
    bar_close_ns = ((START_NS // m15_ns) + 1) * m15_ns
    clock = AdvancingClock(bar_close_ns + 3_800_000_000)
    reader = AdvancingBybitReader(clock, latency_ns=100_000_000)
    port = _port(clock, reader)
    with OpsSupervisorV2(tmp_path / "stale-during-acquisition.sqlite", port, clock_ns=clock) as supervisor:
        first = supervisor.run_once()
        assert supervisor.repository is not None
        indexed_m15 = [
            entry for entry in supervisor.repository.artifact_entries("PublicObservationIndexV2")
            if entry.metadata.get("event_type") == "BAR_15M"
            and entry.metadata.get("event_at_ns") == bar_close_ns
        ]
        assert len(indexed_m15) == 2
        assert all(entry.created_at_ns < bar_close_ns + 5_000_000_000 for entry in indexed_m15)
        assert all(entry.available_at_ns > bar_close_ns + 5_000_000_000 for entry in indexed_m15)
        assert not first.cycle.event_receipt_refs

        stale = supervisor.run_once()
        assert stale.cycle.source_health_state == "HEALTHY_CURRENT"
        assert not stale.cycle.event_receipt_refs
        assert not supervisor.repository.artifact_entries("OpsDecisionEventSourceV1")


def test_restart_keeps_epoch_bound_and_does_not_duplicate_event_or_receipt(tmp_path):
    path = tmp_path / "restart.sqlite"
    clock = AdvancingClock()
    reader = AdvancingBybitReader(clock)
    first_port = _port(clock, reader)
    with OpsSupervisorV2(path, first_port, clock_ns=clock) as supervisor:
        waiting = supervisor.run_once()
        assert not waiting.cycle.event_receipt_refs
        ready = supervisor.run_once()
        assert ready.cycle.event_receipt_refs
        assert supervisor.repository is not None
        repository = supervisor.repository
        prior_event_refs = {
            entry.artifact_ref for entry in repository.artifact_entries("OpsDecisionEventSourceV1")
        }
        prior_receipt_refs = {
            entry.artifact_ref for entry in repository.artifact_entries("OpsSupervisorReceiptV1")
        }
        first_epoch = first_port._collector_recovery.recovery_epoch_ref

    restarted_port = _port(clock, reader)
    with OpsSupervisorV2(path, restarted_port, clock_ns=clock) as restarted:
        result = restarted.run_once()
        assert restarted.repository is not None
        assert result.cycle.source_health_state != "HEALTHY_CURRENT"
        assert {
            entry.artifact_ref for entry in restarted.repository.artifact_entries("OpsDecisionEventSourceV1")
        } == prior_event_refs
        assert {
            entry.artifact_ref for entry in restarted.repository.artifact_entries("OpsSupervisorReceiptV1")
        } == prior_receipt_refs
        epoch = restarted.repository.get_artifact(restarted_port._collector_recovery.recovery_epoch_ref)
        assert epoch is not None
        epoch_body = epoch.metadata["recovery_epoch"]
        assert epoch_body["epoch_index"] == 2
        assert epoch_body["previous_epoch_ref"] == first_epoch
        recovery = max(
            restarted.repository.artifact_entries("BybitPublicRecoveryEvidenceV1"),
            key=lambda item: item.available_at_ns,
        ).metadata["recovery_evidence"]
        assert recovery["recovery_required"] is True
        assert recovery["trade_continuity_proven"] is False
        assert result.cycle.event_receipt_refs == ()


def test_elapsed_decision_deadline_produces_expired_receipt(tmp_path):
    expired_at = START_NS
    event = OpsDecisionEventV1(
        sha256_json("expired-event"), "CONFIRMED_15M_CLOSE", SOURCE_ID,
        sha256_json("expired-trigger"), expired_at, None, expired_at, expired_at,
        expired_at, expired_at, (sha256_json("expired-trigger"),),
    )

    class ExpiredEventPort:
        def recover(self, repository: OpsRepository, *, now_ns: int):
            del repository
            state = OpsSourceStateV1(SOURCE_ID, "HEALTHY_CURRENT", expired_at, expired_at)
            return OpsRecoverySnapshotV1((SOURCE_ID,), (state,), (), None, True, now_ns)

        def collect(self, repository: OpsRepository, *, now_ns: int, recovery: OpsRecoverySnapshotV1):
            del recovery
            repository.register_artifact(ArtifactIndexEntryV2(
                event.trigger_ref, "FixtureTriggerV1", event.trigger_ref,
                expired_at, expired_at, {"trigger": event.trigger_ref},
            ))
            state = OpsSourceStateV1(SOURCE_ID, "HEALTHY_CURRENT", expired_at, expired_at)
            return OpsCycleBatchV1((event,), (state,), (SOURCE_ID,), (), True, now_ns)

        def process_event(self, *args, **kwargs):
            raise AssertionError("expired event must not enter the deterministic pipeline")

    result = None
    with OpsSupervisorV2(
        tmp_path / "expired-event.sqlite", ExpiredEventPort(), clock_ns=lambda: expired_at + 1,
    ) as supervisor:
        result = supervisor.run_once()
    assert result is not None and len(result.event_receipts) == 1
    receipt = result.event_receipts[0]
    assert receipt.result.terminal_status.value == "EXPIRED"
    assert receipt.result.missing_reason == "DECISION_DEADLINE_EXPIRED_BEFORE_RECOVERY_REPLAY"


def test_deadline_crossed_during_acquisition_is_deferred_then_recorded_expired(tmp_path):
    clock = AdvancingClock()
    reader = AdvancingBybitReader(clock)
    port = _port(clock, reader)
    with OpsSupervisorV2(tmp_path / "acquisition-deadline.sqlite", port, clock_ns=clock) as supervisor:
        supervisor.run_once()
        assert supervisor.repository is not None
        repository = supervisor.repository
        eligible_at = clock.now_ns
        trigger_ref = sha256_json("immutable-preexisting-trigger")
        repository.register_artifact(ArtifactIndexEntryV2(
            trigger_ref, "FixtureTriggerV1", trigger_ref,
            eligible_at, eligible_at, {"trigger": "persisted-before-acquisition"},
        ))
        event = OpsDecisionEventV1(
            sha256_json("decision-whose-deadline-is-during-acquisition"),
            "CONFIRMED_15M_CLOSE", SOURCE_ID, trigger_ref,
            eligible_at, eligible_at, eligible_at, eligible_at, eligible_at,
            eligible_at + 500_000_000, (trigger_ref,),
        )
        repository.register_artifact(ArtifactIndexEntryV2(
            event.content_hash, "OpsDecisionEventSourceV1", event.content_hash,
            eligible_at, eligible_at, {"event": event.to_dict(), "trigger_record_id": "fixture"},
        ))

        crossing = supervisor.run_once()
        assert clock.now_ns > event.deadline_ns
        assert all(item.event.event_id != event.event_id for item in crossing.event_receipts)
        gates = repository.artifact_entries("OpsPublicAcquisitionDeadlineGateV1")
        assert any(event.event_id in item.metadata["deadline_gate"]["event_ids"] for item in gates)
        assert event.deadline_ns == eligible_at + 500_000_000

        expired = supervisor.run_once()
        matching = [item for item in expired.event_receipts if item.event.event_id == event.event_id]
        assert len(matching) == 1
        assert matching[0].result.terminal_status == OpsTerminalStatusV1.EXPIRED
        assert matching[0].event.deadline_ns == event.deadline_ns
