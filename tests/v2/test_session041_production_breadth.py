"""Integrated broad inventory, causal freshness and retained REST-worker gates."""

from __future__ import annotations

import json
import threading
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import pytest

from atlas.v2._serialization import canonical_json, sha256_json
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


def quote(p: ProductContractV2, at: int, *, event: str = "TICKER_24H", values=None) -> PublicInputRecordV2:
    row = {"symbol": p.key.native_symbol, "bidPrice": "99.99", "askPrice": "100.01",
           "quoteVolume": "20000000"} if values is None else values
    payload = canonical_json(row).encode()
    source = "BYBIT_PUBLIC_V2" if p.key.venue == VenueV2.BYBIT else "BINANCE_USDM_PUBLIC_V2"
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


def test_adoption_waits_for_durable_archive_before_proving_current_source_health(tmp_path):
    from atlas.v2.runtime.production import ProductionOpsCyclePortV1

    p = product(2)
    runtime = FakeBroadRuntime()
    port = BroadProductionOpsCyclePortV2(public_source=InventorySource((p,)),
                                       broad_runtime=runtime, clock_ns=lambda: NOW)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        port.recover(repo, now_ns=NOW)
        acquired = replace(snapshot((p,), (quote(p, NOW),)), source_snapshot={
            "metadata": {"BYBIT": {"market_status": "BULK_MARKET_COMPLETE"},
                         "BINANCE": {"market_status": "BULK_MARKET_COMPLETE"}}})
        assert not ProductionOpsCyclePortV1._persist_broad_public_snapshot(port, repo, acquired, now_ns=NOW)
        receipt = repo.latest_artifact_entries("BroadPublicAcquisitionReceiptV2", as_of_ns=NOW, limit=1).entries[0]
        refs = receipt.metadata["receipt"]["source_observation_refs"]["BYBIT_PUBLIC_V2"]
        assert len(refs) == 1
        assert repo.get_artifact(refs[0]).metadata["archive_chunk_id"]
        assert repo.latest_source_health_at("BYBIT_PUBLIC_V2", as_of_ns=NOW).status == "HEALTHY_CURRENT"
        assert repo.latest_source_health_at("BINANCE_USDM_PUBLIC_V2", as_of_ns=NOW).status == "INCOMPLETE_SNAPSHOT"
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
