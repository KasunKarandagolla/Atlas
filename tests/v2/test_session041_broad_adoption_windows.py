"""Broad adoption retains every receipt across batching and stream publication."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from types import SimpleNamespace

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.collector import PublicCollectorV2
from atlas.v2.data.history import ParquetObservationArchiveV2
from atlas.v2.instruments import InstrumentRegistryV2, VenueV2
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.runtime.production import ProductionOpsCyclePortV1

from .test_session041_scale_review import NOW, SOURCES, _products, _records, _snapshot


@pytest.mark.parametrize("conflict", [False, True])
def test_adoption_retains_all_rows_and_reconciles_identity_published_by_stream_service(tmp_path, conflict):
    products = _products(160)
    records = _records(products, NOW)
    registry = InstrumentRegistryV2()
    for product in products:
        registry.register(product)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        collector = PublicCollectorV2(repository=repo, registry=registry, clock_ns=lambda: NOW,
            archive=ParquetObservationArchiveV2(tmp_path / "observations"))
        collector.publish_indexes_after_archive = True
        service_calls = []

        def service(_repo):
            service_calls.append(len(service_calls))
            if len(service_calls) != 1:
                return
            incoming = records[100]
            payload = incoming.raw_payload + b" " if conflict else incoming.raw_payload
            raw = replace(incoming.observation,
                raw_payload_hash=hashlib.sha256(payload).hexdigest())
            collector.ingest(raw, raw_payload=payload, instrument_key=incoming.instrument_key,
                retain_in_memory=False, update_source_health=False)
            collector.flush_archive()

        port = SimpleNamespace(_collector_recovery=SimpleNamespace(collector=collector),
            public_source=SimpleNamespace(enabled_venues=tuple(VenueV2), required_source_ids=SOURCES),
            clock_ns=lambda: NOW, service_public_stream=service)
        ready = ProductionOpsCyclePortV1._persist_broad_public_snapshot(
            port, repo, _snapshot(records, NOW), now_ns=NOW)
        receipt = repo.latest_artifact_entries("BroadPublicAcquisitionReceiptV2",
            as_of_ns=NOW, limit=1).entries[0].metadata["receipt"]
        assert ready is (not conflict)
        assert len(service_calls) > 1
        assert list(receipt["rejected_record_indexes"]) == ([100] if conflict else [])
        refs = {ref for values in receipt["source_observation_refs"].values() for ref in values}
        expected = {sha256_json({"artifact_type": "PublicObservationIndexV2",
            "record_id": record.observation.record_id}) for index, record in enumerate(records)
            if not conflict or index != 100}
        assert refs == expected
        assert len(repo.get_artifact_metadata_by_refs(tuple(refs))) == len(expected)
        conflicting = repo.artifact_entries("PublicDuplicateConflictV2")
        assert len(conflicting) == int(conflict)
        if conflict:
            assert conflicting[0].metadata["record_id"] == records[100].observation.record_id
