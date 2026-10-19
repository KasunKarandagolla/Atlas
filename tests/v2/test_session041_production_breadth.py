"""Integrated broad inventory, causal freshness and retained REST-worker gates."""

from __future__ import annotations

import json
import threading
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import pytest

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.broad_public_source import BroadPublicSnapshotV2, PublicInputRecordV2
from atlas.v2.data.raw import RawObservationV2
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.broad_serviced_acquisition import BroadServicedPublicAcquisitionV2
from atlas.v2.runtime.broad_universe import active_products, full_universe, latest_workset, publish_broad_workset
from atlas.v2.runtime.production import BroadProductionOpsCyclePortV2

NOW = 1_800_000_000_000_000_000


def product(index: int, venue: VenueV2 = VenueV2.BYBIT) -> ProductContractV2:
    symbol = f"ALT{index}USDT"
    revision = sha256_json({"symbol": symbol, "venue": venue.value})
    key = InstrumentKeyV2(venue, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
                          symbol, f"ALT{index}", "USDT", "USDT", revision)
    return ProductContractV2(key, NOW, NOW, NOW, Decimal("1"), Decimal("0.01"),
        Decimal("0.001"), Decimal("0.001"), TradingStatusV2.TRADING, revision)


def quote(p: ProductContractV2, at: int, *, event: str = "TICKER_24H", values=None,
          source_id: str | None = None) -> PublicInputRecordV2:
    row = {"symbol": p.key.native_symbol, "bidPrice": "99.99", "askPrice": "100.01",
           "quoteVolume": "20000000"} if values is None else values
    payload = canonical_json(row).encode()
    source = source_id or (
        "BYBIT_PUBLIC_V2" if p.key.venue == VenueV2.BYBIT else "BINANCE_USDM_PUBLIC_V2"
    )
    raw = RawObservationV2.build(instrument_revision=p.key.contract_revision, source_id=source,
        event_type=event, event_at_ns=at, published_at_ns=None, received_at_ns=at,
        ingested_at_ns=at, available_at_ns=at, payload=payload,
        translation_version="SESSION041_CAUSAL_FIXTURE", sequence=str(at))
    return PublicInputRecordV2(raw, payload, p.key)


def snapshot(products=(), records=(), at=NOW) -> BroadPublicSnapshotV2:
    return BroadPublicSnapshotV2(tuple(records), True, None, None,
        max((item.observation.received_at_ns for item in records), default=0), at,
        0, 0, 0, 0, 0, {"metadata": {"BYBIT": {"received_at_ns": at},
            "BINANCE": {"received_at_ns": at},
            "enrichment": {"scheduled_keys": [p.key.to_canonical_json() for p in products[:2]]}}})


def publish(repo, products, records=(), at=NOW, *, service=None):
    repo.register_artifacts(tuple(ArtifactIndexEntryV2(p.content_hash, "ProductContractV2", p.content_hash,
        p.observed_at_ns, p.available_at_ns, {"product": p.to_dict()}) for p in products))
    body = {"cutoff": at, "products": [p.content_hash for p in products],
            "records": [item.observation.content_hash for item in records]}
    ref = sha256_json(body)
    repo.register_artifact(ArtifactIndexEntryV2(ref, "FixtureAcquisitionV2", ref, at, at, body))
    return publish_broad_workset(repo, products=tuple(products), snapshot=snapshot(products, records, at),
        available_at_ns=at, acquisition_ref=ref,
        service=service,
        source_state={"BYBIT_PUBLIC_V2": "HEALTHY_CURRENT", "BINANCE_USDM_PUBLIC_V2": "HEALTHY_CURRENT"})


def test_broad_publication_services_sole_writer_without_changing_selection(tmp_path):
    products = [product(index, venue) for venue in VenueV2 for index in range(40)]
    records = [quote(p, NOW) for p in products]
    with OpsRepository(tmp_path / "plain.sqlite") as repo:
        expected = publish(repo, products, records)
    with OpsRepository(tmp_path / "serviced.sqlite") as repo:
        calls = []

        def service():
            assert not repo._connection.in_transaction
            calls.append(threading.get_ident())
            ref = sha256_json({"stream_service_fixture": len(calls)})
            repo.register_artifact(ArtifactIndexEntryV2(ref, "StreamServiceFixtureV1", ref,
                NOW, NOW, {"call": len(calls)}))

        actual = publish(repo, products, records, service=service)
        assert len(calls) >= 12
        assert set(calls) == {threading.get_ident()}
        assert canonical_json(actual) == canonical_json(expected)
        assert len(repo.artifact_entries("StreamServiceFixtureV1")) == len(calls)
        assert len(full_universe(repo, cutoff_ns=NOW).entries) == len(products)


def test_broad_publication_service_failure_cannot_publish_complete_workset(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        def failed_service():
            raise RuntimeError("STREAM_SERVICE_FAILED")

        with pytest.raises(RuntimeError, match="STREAM_SERVICE_FAILED"):
            publish(repo, [product(1)], service=failed_service)
        assert latest_workset(repo, cutoff_ns=NOW) is None


def test_full_inventory_is_distinct_bounded_and_restart_reproduces_workset(tmp_path):
    products = [product(index, venue) for venue in VenueV2 for index in range(2048)]
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        body = publish(repo, products)
        universe = full_universe(repo, cutoff_ns=NOW)
        assert universe is not None and len(universe.entries) == 4096
        assert len({entry.key for entry in universe.entries}) == 4096
        assert {entry.key.venue for entry in universe.entries} == set(VenueV2)
        assert body["selected_count"] <= 24
        assert len(active_products(repo, cutoff_ns=NOW)) <= 24
        assert not any(entry.capital_eligible for entry in universe.entries)
        assert not any(entry.scanner_eligible for entry in universe.entries)
    with OpsRepository(path) as restarted:
        assert json.loads(canonical_json(latest_workset(restarted, cutoff_ns=NOW))) == body
        assert len(full_universe(restarted, cutoff_ns=NOW).entries) == 4096


def test_full_universe_cache_is_bound_to_the_cutoff_visible_workset(tmp_path):
    old, new = product(7), replace(product(7), observed_at_ns=NOW + 1,
        available_at_ns=NOW + 1, effective_at_ns=NOW + 1,
        trading_status=TradingStatusV2.SUSPENDED)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        publish(repo, [old])
        first = full_universe(repo, cutoff_ns=NOW)
        assert first is not None

        original_get_artifact = repo.get_artifact

        def reject_rehydration(ref):
            if ref == first.content_hash:
                raise AssertionError("same-generation universe should use its exact typed cache")
            return original_get_artifact(ref)

        repo.get_artifact = reject_rehydration
        assert full_universe(repo, cutoff_ns=NOW) is first
        repo.get_artifact = original_get_artifact
        with OpsRepository(repo.path, read_only=True) as reader:
            assert full_universe(reader, cutoff_ns=NOW) is first

        publish(repo, [new], at=NOW + 1)
        current = full_universe(repo, cutoff_ns=NOW + 1)
        historical = full_universe(repo, cutoff_ns=NOW)
        assert current is not None and current.content_hash != first.content_hash
        assert "PRODUCT_SUSPENDED" in current.entries[0].reasons
        assert historical is not None and historical.content_hash == first.content_hash
        assert "PRODUCT_SUSPENDED" not in historical.entries[0].reasons


def test_new_mark_receipt_cannot_refresh_old_quote_fields(tmp_path):
    p = product(1)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        old = publish(repo, [p], [quote(p, NOW)])
        at = NOW + 31_000_000_000
        current = publish(repo, [p], [quote(p, at, event="MARK_INDEX_CURRENT_FUNDING",
            values={"symbol": p.key.native_symbol, "markPrice": "100", "indexPrice": "100"})], at)
        universe = full_universe(repo, cutoff_ns=at)
        reasons = universe.entries[0].reasons
        assert "CURRENT_BID_STALE_OR_UNAVAILABLE" in reasons
        assert "CURRENT_ASK_STALE_OR_UNAVAILABLE" in reasons
        assert current["prior_workset_ref"] == sha256_json(old)
        assert current["cheap_quotes"][p.key.to_canonical_json()]["_field_receipts"]["bidPrice"] == NOW


def test_suspension_is_current_and_old_state_is_still_replayable(tmp_path):
    p = product(2)
    suspended = replace(p, trading_status=TradingStatusV2.SUSPENDED,
        observed_at_ns=NOW + 1, available_at_ns=NOW + 1, effective_at_ns=NOW + 1)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        old = publish(repo, [p])
        publish(repo, [suspended], at=NOW + 1)
        assert "PRODUCT_SUSPENDED" in full_universe(repo, cutoff_ns=NOW + 1).entries[0].reasons
        assert canonical_json(latest_workset(repo, cutoff_ns=NOW)) == canonical_json(old)
        assert "PRODUCT_SUSPENDED" not in full_universe(repo, cutoff_ns=NOW).entries[0].reasons


class ControlledBroadSource:
    enabled_venues = (VenueV2.BYBIT, VenueV2.BINANCE)

    def __init__(self):
        self.started = threading.Event()
        self.release = threading.Event()
        self.result = snapshot(at=NOW)
        self.calls = 0

    def begin_collection_cycle(self, *, now_ns):
        pass

    def acquire_snapshot(self, *, now_ns):
        self.calls += 1
        self.started.set()
        self.release.wait()
        return self.result


def test_broad_pending_read_services_writer_and_keeps_one_original_completion():
    source = ControlledBroadSource()
    helper = BroadServicedPublicAcquisitionV2(source, max_wait_s=0.03, clock_ns=lambda: NOW + 100)
    threads = []
    try:
        for _ in range(3):
            result = helper.acquire(now_ns=NOW, service=lambda: threads.append(threading.get_ident()))
            assert result.source_snapshot["pending"] and not result.records
        assert source.calls == 1
        assert set(threads) == {threading.get_ident()}
        source.release.set()
        assert helper.acquire(now_ns=NOW + 100, service=lambda: None) is source.result
        assert helper.status()["consumed_count"] == 1
    finally:
        source.release.set()
        helper.close()


class InventorySource:
    enabled_venues = (VenueV2.BYBIT, VenueV2.BINANCE)
    required_source_ids = ("BINANCE_USDM_PUBLIC_V2", "BYBIT_PUBLIC_V2")

    def __init__(self, products):
        self.current_products = tuple(products)

    def bootstrap_products(self, *, now_ns):
        return self.current_products

    def acquire_snapshot(self, *, now_ns):
        return snapshot(self.current_products, at=now_ns)

    def export_state(self):
        return {"version": "INVENTORY_SOURCE_TEST_V1"}


class FakeBroadRuntime:
    def __init__(self):
        self.capture = None
        self.recover_calls = []
        self.service_calls = 0
        self.reconfigure_calls = []

    def recover(self, repo, **kwargs):
        self.capture = SimpleNamespace()
        self.recover_calls.append(kwargs)

    def service(self, repo, *, now_ns):
        assert not repo._connection.in_transaction
        self.service_calls += 1

    def reconfigure(self, repo, **kwargs):
        assert not repo._connection.in_transaction
        self.reconfigure_calls.append(kwargs)

    def close(self):
        pass


def test_runtime_recovery_preserves_nonbenchmark_dual_venue_inventory(tmp_path):
    products = [product(i, venue) for venue in VenueV2 for i in range(75)]
    runtime = FakeBroadRuntime()
    port = BroadProductionOpsCyclePortV2(public_source=InventorySource(products),
        broad_runtime=runtime, clock_ns=lambda: NOW)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        recovered = port.recover(repo, now_ns=NOW)
        contracts = port._collector_recovery.collector.registry.contracts()
        assert len(contracts) == 150
        assert recovered.required_source_ids == InventorySource.required_source_ids
        assert runtime.recover_calls[0]["products"] == tuple(products)
        assert len(runtime.recover_calls[0]["benchmark_keys"]) <= 2
    port.close()


def test_conserve_effort_records_optional_deferral_without_pausing_outcomes(tmp_path, monkeypatch):
    source = InventorySource((product(3),))
    port = BroadProductionOpsCyclePortV2(
        public_source=source,
        broad_runtime=FakeBroadRuntime(),
        clock_ns=lambda: NOW,
        discovery_effort="CONSERVE",
    )
    outcome_calls = []
    monkeypatch.setattr(
        port._s8_outcome_producer,
        "run_cycle",
        lambda repository, **kwargs: outcome_calls.append(kwargs),
    )
    snapshot_ref = sha256_json({"prepared_snapshot": "for-effort-test"})
    with OpsRepository(tmp_path / "effort.sqlite") as repo:
        repo.enqueue_due_work(
            lane="BROAD_RESEARCH",
            work_id="effort-slot-1",
            source_ref=snapshot_ref,
            created_at_ns=NOW - 1,
            due_at_ns=NOW - 1,
            payload={"snapshot_ref": snapshot_ref},
        )
        first = port.run_research_maintenance(repo, cutoff_ns=NOW)
        second = port.run_research_maintenance(repo, cutoff_ns=NOW)
        pending = repo.due_work_items("BROAD_RESEARCH", as_of_ns=NOW, limit=1)
        records = repo.artifact_entries("BroadResearchDeferredV1")
        assert first == second == records[0].artifact_ref
        assert len(records) == 1
        assert records[0].metadata["deferred"]["reason"] == "OWNER_DISCOVERY_EFFORT_CONSERVE"
        assert len(pending) == 1 and pending[0].work_id == "effort-slot-1"
        assert len(outcome_calls) == 2
    port.close()


def test_collection_path_services_stream_and_commits_broad_workset(tmp_path):
    products = [product(1, VenueV2.BYBIT), product(1, VenueV2.BINANCE)]
    runtime = FakeBroadRuntime()
    port = BroadProductionOpsCyclePortV2(public_source=InventorySource(products),
        broad_runtime=runtime, clock_ns=lambda: NOW)
    try:
        with OpsRepository(tmp_path / "ops.sqlite") as repo:
            recovery = port.recover(repo, now_ns=NOW)
            batch = port.collect(repo, now_ns=NOW, recovery=recovery)
            assert batch is not None
            assert port._collection_calls == 1
            assert runtime.service_calls > 0
            assert runtime.reconfigure_calls
            assert len(full_universe(repo, cutoff_ns=NOW).entries) == len(products)
    finally:
        port.close()


def test_population_overflow_is_an_explicit_gate(tmp_path):
    products = [product(i) for i in range(4097)]
    with OpsRepository(tmp_path / "ops.sqlite") as repo, pytest.raises(
        ValueError, match="BROAD_PRODUCT_POPULATION_INVALID",
    ):
        publish_broad_workset(repo, products=tuple(products), snapshot=snapshot(at=NOW),
            available_at_ns=NOW, acquisition_ref="a" * 64, source_state={})


def test_workset_publication_is_actual_and_preserves_its_source_cutoff(tmp_path):
    p = product(5)
    clock = [NOW]

    def advancing_clock():
        clock[0] += 100
        return clock[0]

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        publish(repo, [p])
        body = publish_broad_workset(repo, products=(p,), snapshot=snapshot((p,), at=NOW),
            available_at_ns=NOW, acquisition_ref="a" * 64, source_state={}, clock_ns=advancing_clock)
        assert body["source_cutoff_ns"] == NOW
        assert body["available_at_ns"] == NOW + 300
        assert latest_workset(repo, cutoff_ns=NOW)["available_at_ns"] == NOW
        actual = latest_workset(repo, cutoff_ns=clock[0])
        assert canonical_json(actual) == canonical_json(body)
        universe = full_universe(repo, cutoff_ns=clock[0])
        assert universe.decision_slot_ns == NOW + 200
        assert universe.envelope.available_at_ns == NOW + 200
        assert universe.envelope.created_at_ns == NOW + 200


def test_workset_publication_rejects_regressing_clock(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo, pytest.raises(ValueError, match="CLOCK_REGRESSED"):
        publish_broad_workset(repo, products=(), snapshot=snapshot(at=NOW),
            available_at_ns=NOW, acquisition_ref="a" * 64, source_state={}, clock_ns=lambda: NOW - 1)


def test_same_venue_research_cohort_has_twenty_keys_within_global_history_bound(tmp_path):
    products = [product(index, venue) for venue in VenueV2 for index in range(25)]
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        body = publish(repo, products)
        assert len(body["research_cohort_keys"]) == 20
        keys = [InstrumentKeyV2.from_dict(json.loads(key)) for key in body["research_cohort_keys"]]
        assert {key.venue.value for key in keys} == {body["research_cohort_venue"]}
        assert len(active_products(repo, cutoff_ns=NOW)) <= 24
        assert set(keys).issubset({item.key for item in active_products(repo, cutoff_ns=NOW)})
        assert len(full_universe(repo, cutoff_ns=NOW).entries) == 50


def _completed_bybit_bar(p: ProductContractV2, received_at_ns: int, *, high: str = "102",
                         close: str = "101") -> PublicInputRecordV2:
    interval = BarIntervalV2.M1
    open_at_ns = (NOW // interval.duration_ns - 1) * interval.duration_ns
    close_at_ns = open_at_ns + interval.duration_ns
    payload = canonical_json({"open": "100", "high": high, "low": "99",
        "close": close, "volume": "7"}).encode()
    raw = RawObservationV2.build(instrument_revision=p.key.contract_revision,
        source_id="BYBIT_PUBLIC_HTTP", event_type="BAR_1M", event_at_ns=close_at_ns,
        published_at_ns=None, received_at_ns=received_at_ns, ingested_at_ns=received_at_ns,
        available_at_ns=received_at_ns, payload=payload, translation_version="bybit-public-v1",
        sequence="completed-bar-duplicate-reproducer")
    bar = CausalBarV2(raw, interval, open_at_ns, close_at_ns, Decimal("100"), Decimal(high),
        Decimal("99"), Decimal(close), Decimal("7"), True)
    return PublicInputRecordV2(raw, payload, p.key, bar)


def _bybit_snapshot(record: PublicInputRecordV2, at_ns: int) -> BroadPublicSnapshotV2:
    return BroadPublicSnapshotV2((record,), True, None, None, record.observation.received_at_ns,
        at_ns, 1, 1, 0, 0, 0, {"metadata": {"BYBIT": {
            "market_status": "BULK_MARKET_COMPLETE", "received_at_ns": at_ns}}})


def _ticker_record(p: ProductContractV2, received_at_ns: int, *, bid: str) -> PublicInputRecordV2:
    payload = canonical_json({"bidPrice": bid, "askPrice": "101", "quoteVolume": "20000000"}).encode()
    raw = RawObservationV2.build(instrument_revision=p.key.contract_revision, source_id="BYBIT_PUBLIC_HTTP",
        event_type="TICKER_24H", event_at_ns=NOW, published_at_ns=None,
        received_at_ns=received_at_ns, ingested_at_ns=received_at_ns, available_at_ns=received_at_ns,
        payload=payload, translation_version="bybit-public-v1", sequence="ticker-conflict-reproducer")
    return PublicInputRecordV2(raw, payload, p.key)


class SingleVenueBarSource:
    enabled_venues = (VenueV2.BYBIT,)
    required_source_ids = ("BYBIT_PUBLIC_HTTP",)

    def __init__(self, p: ProductContractV2):
        self.current_products = (p,)
        self._state = {"version": "SINGLE_VENUE_COMPLETED_BAR_REPRODUCER_V1"}

    def bootstrap_products(self, *, now_ns):
        return self.current_products

    def acquire_snapshot(self, *, now_ns):
        return _bybit_snapshot(_completed_bybit_bar(self.current_products[0], now_ns), now_ns)

    def export_state(self):
        return self._state

    def restore_state(self, state):
        if state.get("version") != "SINGLE_VENUE_COMPLETED_BAR_REPRODUCER_V1":
            raise ValueError("unexpected completed-bar source state")
        self._state = state


def _bar_port(p: ProductContractV2, clock: list[int]) -> BroadProductionOpsCyclePortV2:
    return BroadProductionOpsCyclePortV2(public_source=SingleVenueBarSource(p),
        broad_runtime=FakeBroadRuntime(), clock_ns=lambda: clock[0])


def test_repeated_durable_completed_bar_reconciles_on_actual_production_path(tmp_path):
    p = product(941, VenueV2.BYBIT)
    path = tmp_path / "completed-bar.sqlite"
    clock = [NOW]
    port = _bar_port(p, clock)
    try:
        with OpsRepository(path) as repo:
            port.recover(repo, now_ns=NOW)
            first_record = _completed_bybit_bar(p, NOW)
            assert port._persist_broad_public_snapshot(repo,
                _bybit_snapshot(first_record, NOW), now_ns=NOW)
            first_health = repo.latest_source_health_at("BYBIT_PUBLIC_HTTP", as_of_ns=NOW)
            assert first_health is not None and first_health.status == "HEALTHY_CURRENT"
            expected_ref = sha256_json({"artifact_type": "PublicObservationIndexV2",
                "record_id": first_record.observation.record_id})
            first_index = repo.get_artifact(expected_ref)
            assert first_index is not None

            clock[0] = NOW + 10_000_000_000
            duplicate = _completed_bybit_bar(p, clock[0])
            assert duplicate.observation.record_id == first_record.observation.record_id
            assert duplicate.observation.raw_payload_hash == first_record.observation.raw_payload_hash
            print("completed_bar_duplicate_reproducer", {
                "source_id": duplicate.observation.source_id,
                "record_id": duplicate.observation.record_id,
                "index_ref": expected_ref,
                "raw_payload_hash": duplicate.observation.raw_payload_hash,
                "first_health": first_health.status,
                "first_received_at_ns": first_record.observation.received_at_ns,
                "duplicate_received_at_ns": duplicate.observation.received_at_ns,
            })
            assert port._persist_broad_public_snapshot(repo,
                _bybit_snapshot(duplicate, clock[0]), now_ns=clock[0])
            duplicate_index = repo.get_artifact(expected_ref)
            assert duplicate_index == first_index
            receipt = repo.latest_artifact_entries("BroadPublicAcquisitionReceiptV2",
                as_of_ns=clock[0], limit=1).entries[0].metadata["receipt"]
            assert receipt["source_health_current"]
            assert list(receipt["source_observation_refs"]["BYBIT_PUBLIC_HTTP"]) == [expected_ref]
            duplicate_receipts = repo.artifact_entries("BroadPublicDuplicateReceiptV2")
            assert len(duplicate_receipts) == 1
            duplicate_receipt = duplicate_receipts[0].metadata["receipt"]
            assert duplicate_receipt["original_ref"] == expected_ref
            assert duplicate_receipt["source_id"] == "BYBIT_PUBLIC_HTTP"
            assert duplicate_receipt["received_at_ns"] == duplicate.observation.received_at_ns
            assert duplicate_receipt["available_at_ns"] == clock[0]
            second_health = repo.latest_source_health_at("BYBIT_PUBLIC_HTTP", as_of_ns=clock[0])
            assert second_health is not None and second_health.status == "HEALTHY_CURRENT"
    finally:
        port.close()

    # A fresh collector after process restart must read the same immutable index
    # and complete reconciliation without trying to archive the duplicate again.
    clock[0] = NOW + 20_000_000_000
    restarted = _bar_port(p, clock)
    try:
        with OpsRepository(path) as repo:
            restarted.recover(repo, now_ns=clock[0])
            repeated = _completed_bybit_bar(p, clock[0])
            assert not restarted._persist_broad_public_snapshot(repo,
                _bybit_snapshot(repeated, clock[0]), now_ns=clock[0])
            assert repo.get_artifact(expected_ref) == first_index
            assert len(repo.artifact_entries("BroadPublicDuplicateReceiptV2")) == 2
            receipt = repo.latest_artifact_entries("BroadPublicAcquisitionReceiptV2",
                as_of_ns=clock[0], limit=1).entries[0].metadata["receipt"]
            assert not receipt["source_health_current"]
            assert receipt["source_ingestion_complete"]["BYBIT_PUBLIC_HTTP"]
            assert list(receipt["source_observation_refs"]["BYBIT_PUBLIC_HTTP"]) == [expected_ref]
            health = repo.latest_source_health_at("BYBIT_PUBLIC_HTTP", as_of_ns=clock[0])
            assert health is not None and health.status == "INCOMPLETE_SNAPSHOT"
    finally:
        restarted.close()


def test_corrected_completed_bar_appends_distinct_causal_revision(tmp_path):
    p = product(942, VenueV2.BYBIT)
    clock = [NOW]
    port = _bar_port(p, clock)
    try:
        with OpsRepository(tmp_path / "bar-correction.sqlite") as repo:
            port.recover(repo, now_ns=NOW)
            original = _completed_bybit_bar(p, NOW)
            assert port._persist_broad_public_snapshot(repo, _bybit_snapshot(original, NOW), now_ns=NOW)
            clock[0] = NOW + 10_000_000_000
            corrected = _completed_bybit_bar(p, clock[0], high="103", close="102")
            assert corrected.observation.record_id == original.observation.record_id
            assert corrected.observation.raw_payload_hash != original.observation.raw_payload_hash
            assert port._persist_broad_public_snapshot(repo,
                _bybit_snapshot(corrected, clock[0]), now_ns=clock[0])
            revision_id = RawObservationV2.build(
                instrument_revision=p.key.contract_revision, source_id=corrected.observation.source_id,
                event_type=corrected.observation.event_type, event_at_ns=corrected.observation.event_at_ns,
                published_at_ns=corrected.observation.published_at_ns,
                received_at_ns=corrected.observation.received_at_ns,
                ingested_at_ns=corrected.observation.ingested_at_ns,
                available_at_ns=corrected.observation.available_at_ns, payload=corrected.raw_payload,
                translation_version=corrected.observation.translation_version,
                sequence=corrected.observation.sequence, revision_of=original.observation.record_id,
                quality_flags=corrected.observation.quality_flags,
                availability_class=corrected.observation.availability_class,
            )
            revision_ref = sha256_json({"artifact_type": "PublicObservationIndexV2",
                "record_id": revision_id.record_id})
            indexed = repo.get_artifact(revision_ref)
            assert indexed is not None and indexed.metadata["revision_of"] == original.observation.record_id
            assert indexed.metadata["record_id"] != original.observation.record_id
            assert repo.get_artifact(sha256_json({"artifact_type": "PublicObservationIndexV2",
                "record_id": original.observation.record_id})) is not None
            receipt = repo.latest_artifact_entries("BroadPublicAcquisitionReceiptV2",
                as_of_ns=clock[0], limit=1).entries[0].metadata["receipt"]
            assert list(receipt["source_observation_refs"]["BYBIT_PUBLIC_HTTP"]) == [revision_ref]
            assert receipt["source_health_current"]
    finally:
        port.close()


def test_conflicting_nonbar_duplicate_payload_is_quarantined_and_incomplete(tmp_path):
    p = product(943, VenueV2.BYBIT)
    clock = [NOW]
    port = _bar_port(p, clock)
    try:
        with OpsRepository(tmp_path / "ticker-conflict.sqlite") as repo:
            port.recover(repo, now_ns=NOW)
            first = _ticker_record(p, NOW, bid="100")
            assert port._persist_broad_public_snapshot(repo, _bybit_snapshot(first, NOW), now_ns=NOW)
            clock[0] = NOW + 10_000_000_000
            conflict = _ticker_record(p, clock[0], bid="99")
            assert conflict.observation.record_id == first.observation.record_id
            assert conflict.observation.raw_payload_hash != first.observation.raw_payload_hash
            assert not port._persist_broad_public_snapshot(repo,
                _bybit_snapshot(conflict, clock[0]), now_ns=clock[0])
            conflicts = repo.artifact_entries("PublicDuplicateConflictV2")
            assert len(conflicts) == 1
            assert conflicts[0].metadata["record_id"] == first.observation.record_id
            receipt = repo.latest_artifact_entries("BroadPublicAcquisitionReceiptV2",
                as_of_ns=clock[0], limit=1).entries[0].metadata["receipt"]
            assert list(receipt["rejected_record_indexes"]) == [0]
            assert not receipt["source_health_current"]
            health = repo.latest_source_health_at("BYBIT_PUBLIC_HTTP", as_of_ns=clock[0])
            assert health is not None and health.status == "INCOMPLETE_SNAPSHOT"
    finally:
        port.close()


@pytest.mark.parametrize("corruption,expected_reason", [
    ("source", "DURABLE_DUPLICATE_INDEX_MISMATCH"),
    ("availability", "DURABLE_DUPLICATE_INDEX_UNAVAILABLE"),
    ("bar-content-hash", "DURABLE_DUPLICATE_INDEX_MISMATCH"),
])
def test_completed_bar_duplicate_rejects_mismatched_or_future_durable_index(
    tmp_path, monkeypatch, corruption, expected_reason,
):
    p = product(944, VenueV2.BYBIT)
    clock = [NOW]
    port = _bar_port(p, clock)
    try:
        with OpsRepository(tmp_path / f"bar-invalid-index-{corruption}.sqlite") as repo:
            port.recover(repo, now_ns=NOW)
            first = _completed_bybit_bar(p, NOW)
            assert port._persist_broad_public_snapshot(repo, _bybit_snapshot(first, NOW), now_ns=NOW)
            ref = sha256_json({"artifact_type": "PublicObservationIndexV2",
                "record_id": first.observation.record_id})
            original_lookup = repo.get_artifact_metadata_by_refs
            clock[0] = NOW + 10_000_000_000

            def altered_lookup(refs):
                found = original_lookup(refs)
                if ref not in found:
                    return found
                result = dict(found)
                entry = dict(result[ref])
                if corruption == "source":
                    entry["metadata"] = {**entry["metadata"], "source_id": "OTHER_PUBLIC_SOURCE"}
                elif corruption == "availability":
                    entry["available_at_ns"] = clock[0] + 1
                else:
                    entry["metadata"] = {**entry["metadata"], "bar_content_hash": "e" * 64}
                result[ref] = entry
                return result

            monkeypatch.setattr(repo, "get_artifact_metadata_by_refs", altered_lookup)
            duplicate = _completed_bybit_bar(p, clock[0])
            assert not port._persist_broad_public_snapshot(repo,
                _bybit_snapshot(duplicate, clock[0]), now_ns=clock[0])
            monkeypatch.setattr(repo, "get_artifact_metadata_by_refs", original_lookup)
            receipt = repo.latest_artifact_entries("BroadPublicAcquisitionReceiptV2",
                as_of_ns=clock[0], limit=1).entries[0].metadata["receipt"]
            assert list(receipt["rejected_record_indexes"]) == [0]
            assert list(receipt["source_observation_refs"]["BYBIT_PUBLIC_HTTP"]) == []
            assert not receipt["source_health_current"]
            rejection = repo.artifact_entries("BroadPublicAdoptionRejectionV1")[-1]
            assert rejection.metadata["rejection"]["reason"] == expected_reason
            assert repo.artifact_entries("BroadPublicDuplicateReceiptV2") == ()
            health = repo.latest_source_health_at("BYBIT_PUBLIC_HTTP", as_of_ns=clock[0])
            assert health is not None and health.status == "INCOMPLETE_SNAPSHOT"
    finally:
        port.close()


def test_completed_bar_duplicate_rejects_incorrect_persisted_payload_hash(tmp_path, monkeypatch):
    p = product(945, VenueV2.BYBIT)
    clock = [NOW]
    port = _bar_port(p, clock)
    try:
        with OpsRepository(tmp_path / "bar-invalid-payload-hash.sqlite") as repo:
            port.recover(repo, now_ns=NOW)
            original = _completed_bybit_bar(p, NOW)
            assert port._persist_broad_public_snapshot(repo, _bybit_snapshot(original, NOW), now_ns=NOW)

            clock[0] = NOW + 10_000_000_000
            corrected = _completed_bybit_bar(p, clock[0], high="103", close="102")
            assert port._persist_broad_public_snapshot(repo,
                _bybit_snapshot(corrected, clock[0]), now_ns=clock[0])
            corrected_revision = RawObservationV2.build(
                instrument_revision=p.key.contract_revision, source_id=corrected.observation.source_id,
                event_type=corrected.observation.event_type, event_at_ns=corrected.observation.event_at_ns,
                published_at_ns=corrected.observation.published_at_ns,
                received_at_ns=corrected.observation.received_at_ns,
                ingested_at_ns=corrected.observation.ingested_at_ns,
                available_at_ns=corrected.observation.available_at_ns, payload=corrected.raw_payload,
                translation_version=corrected.observation.translation_version,
                sequence=corrected.observation.sequence, revision_of=original.observation.record_id,
                quality_flags=corrected.observation.quality_flags,
                availability_class=corrected.observation.availability_class,
            )
            revision_ref = sha256_json({"artifact_type": "PublicObservationIndexV2",
                "record_id": corrected_revision.record_id})
            original_lookup = repo.get_artifact_metadata_by_refs
            clock[0] = NOW + 20_000_000_000

            def altered_lookup(refs):
                found = original_lookup(refs)
                if revision_ref not in found:
                    return found
                result = dict(found)
                entry = dict(result[revision_ref])
                entry["metadata"] = {**entry["metadata"], "raw_payload_hash": "f" * 64}
                result[revision_ref] = entry
                return result

            monkeypatch.setattr(repo, "get_artifact_metadata_by_refs", altered_lookup)
            repeated_correction = _completed_bybit_bar(p, clock[0], high="103", close="102")
            assert not port._persist_broad_public_snapshot(repo,
                _bybit_snapshot(repeated_correction, clock[0]), now_ns=clock[0])
            monkeypatch.setattr(repo, "get_artifact_metadata_by_refs", original_lookup)
            assert repo.get_artifact(revision_ref).metadata["raw_payload_hash"] == corrected.observation.raw_payload_hash
            rejection = repo.artifact_entries("BroadPublicAdoptionRejectionV1")[-1]
            assert rejection.metadata["rejection"]["reason"] == "DURABLE_DUPLICATE_INDEX_MISMATCH"
            assert repo.artifact_entries("BroadPublicDuplicateReceiptV2") == ()
            receipt = repo.latest_artifact_entries("BroadPublicAcquisitionReceiptV2",
                as_of_ns=clock[0], limit=1).entries[0].metadata["receipt"]
            assert list(receipt["rejected_record_indexes"]) == [0]
            assert not receipt["source_health_current"]
    finally:
        port.close()


def test_adoption_waits_for_durable_archive_before_proving_current_source_health(tmp_path):
    from atlas.v2.runtime.production import ProductionOpsCyclePortV1

    class HttpInventorySource(InventorySource):
        required_source_ids = ("BINANCE_USDM_PUBLIC_HTTP", "BYBIT_PUBLIC_HTTP")

    p = product(2)
    runtime = FakeBroadRuntime()
    port = BroadProductionOpsCyclePortV2(public_source=HttpInventorySource((p,)),
                                       broad_runtime=runtime, clock_ns=lambda: NOW)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        port.recover(repo, now_ns=NOW)
        # BroadPublicCycleSourceV2 publishes the HTTP adapter source identities,
        # so this adoption fixture must use the same production identity.
        acquired = replace(snapshot((p,), (quote(p, NOW, source_id="BYBIT_PUBLIC_HTTP"),)), source_snapshot={
            "metadata": {"BYBIT": {"market_status": "BULK_MARKET_COMPLETE"},
                         "BINANCE": {"market_status": "BULK_MARKET_COMPLETE"}}})
        assert not ProductionOpsCyclePortV1._persist_broad_public_snapshot(port, repo, acquired, now_ns=NOW)
        receipt = repo.latest_artifact_entries("BroadPublicAcquisitionReceiptV2", as_of_ns=NOW, limit=1).entries[0]
        refs = receipt.metadata["receipt"]["source_observation_refs"]["BYBIT_PUBLIC_HTTP"]
        assert len(refs) == 1
        assert repo.get_artifact(refs[0]).metadata["archive_chunk_id"]
        assert repo.latest_source_health_at("BYBIT_PUBLIC_HTTP", as_of_ns=NOW).status == "HEALTHY_CURRENT"
        assert repo.latest_source_health_at("BINANCE_USDM_PUBLIC_HTTP", as_of_ns=NOW).status == "INCOMPLETE_SNAPSHOT"
    port.close()


def test_future_market_event_is_rejected_without_rescuing_publication_time(tmp_path):
    from atlas.v2.runtime.production import ProductionOpsCyclePortV1

    p = product(3)
    runtime = FakeBroadRuntime()
    port = BroadProductionOpsCyclePortV2(public_source=InventorySource((p,)),
                                       broad_runtime=runtime, clock_ns=lambda: NOW)
    original = quote(p, NOW)
    raw = RawObservationV2.build(instrument_revision=p.key.contract_revision, source_id="BYBIT_PUBLIC_V2",
        event_type="TICKER_24H", event_at_ns=NOW + 100, received_at_ns=NOW,
        ingested_at_ns=NOW, available_at_ns=NOW, payload=original.raw_payload, translation_version="FUTURE_FIXTURE")
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        port.recover(repo, now_ns=NOW)
        acquired = snapshot((p,), (replace(original, observation=raw),))
        assert not ProductionOpsCyclePortV1._persist_broad_public_snapshot(port, repo, acquired, now_ns=NOW)
        rejected = repo.latest_artifact_entries("BroadPublicAdoptionRejectionV1", as_of_ns=NOW, limit=1).entries[0]
        assert rejected.available_at_ns == NOW
        assert rejected.metadata["rejection"]["reason"] == "FUTURE_SOURCE_CHRONOLOGY"
        assert repo.get_artifact(sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": raw.record_id})) is None
    port.close()


def test_archive_completion_controls_public_index_visibility(tmp_path):
    from atlas.v2.data.raw import indexed_availability_matches
    from atlas.v2.runtime.production import ProductionOpsCyclePortV1

    p = product(8)
    clock = [NOW]
    port = BroadProductionOpsCyclePortV2(public_source=InventorySource((p,)),
        broad_runtime=FakeBroadRuntime(), clock_ns=lambda: clock[0])
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        port.recover(repo, now_ns=NOW)
        assert port._collector_recovery is not None
        collector = port._collector_recovery.collector
        assert collector is not None and collector.archive is not None
        original_write = collector.archive.write_observation_chunk

        def delayed_write(*args, **kwargs):
            result = original_write(*args, **kwargs)
            clock[0] = NOW + 500_000_000
            return result

        collector.archive.write_observation_chunk = delayed_write
        record = quote(p, NOW)
        acquired = snapshot((p,), (record,))
        ProductionOpsCyclePortV1._persist_broad_public_snapshot(port, repo, acquired, now_ns=NOW)
        ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": record.observation.record_id})
        entry = repo.get_artifact(ref)
        assert entry is not None and entry.available_at_ns == clock[0]
        assert entry.content_hash == record.observation.content_hash
        assert indexed_availability_matches(NOW, entry.available_at_ns, entry.metadata)
        assert not repo.latest_artifact_entries("PublicObservationIndexV2", as_of_ns=NOW, limit=1).entries
        assert repo.latest_artifact_entries("PublicObservationIndexV2", as_of_ns=clock[0], limit=1).entries
        assert repo.latest_source_health_at("BYBIT_PUBLIC_V2", as_of_ns=clock[0]).available_at_ns >= entry.available_at_ns
    port.close()
