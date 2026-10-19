"""Bounded exact reconciliation reads yield to the sole-writer stream lane."""

from __future__ import annotations

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.collector import MAX_RECONCILIATION_REFS_PER_PAGE_V1, PublicCollectorV2
from atlas.v2.instruments import InstrumentRegistryV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository

NOW = 1_800_000_000_000_000_000
SOURCE = "BYBIT_PUBLIC_V2"


def test_reconnect_reconciliation_services_between_exact_ref_pages(tmp_path) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        collector = PublicCollectorV2(repository=repository, registry=InstrumentRegistryV2(),
            clock_ns=lambda: NOW)
        refs = []
        entries = []
        for index in range(MAX_RECONCILIATION_REFS_PER_PAGE_V1 * 2 + 1):
            record_id = f"reconciliation-record-{index}"
            ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": record_id})
            refs.append(ref)
            entries.append(ArtifactIndexEntryV2(ref, "PublicObservationIndexV2",
                sha256_json({"observation": record_id}), NOW - 1, NOW - 1,
                {"record_id": record_id, "source_id": SOURCE}))
        repository.register_artifacts(tuple(entries))
        calls = []

        def service() -> None:
            assert not repository._connection.in_transaction
            calls.append(len(calls) + 1)
            ref = sha256_json({"service_checkpoint": calls[-1]})
            repository.register_artifact(ArtifactIndexEntryV2(ref, "StreamServiceCheckpointV1",
                ref, NOW, NOW, {"call": calls[-1]}))

        health = collector.reconcile_after_reconnect(SOURCE, at_ns=NOW,
            complete_snapshot=True, missed_interval_repaired=True,
            snapshot_refs=tuple(sorted(refs)), service_callback=service)

        assert health.state.value == "HEALTHY_CURRENT"
        assert len(calls) == 3
        receipt = repository.latest_artifact_entries("OpsPublicSourceReconciliationV1",
            as_of_ns=NOW, limit=1).entries[0]
        assert receipt.metadata["reconciliation"]["evidence_refs"] == tuple(sorted(refs))


def test_reconnect_reconciliation_reuses_exact_verified_index_entries_without_refetch(
    tmp_path, monkeypatch,
) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        collector = PublicCollectorV2(repository=repository, registry=InstrumentRegistryV2(),
            clock_ns=lambda: NOW)
        refs = []
        for index in range(MAX_RECONCILIATION_REFS_PER_PAGE_V1 * 2 + 1):
            record_id = f"verified-reconciliation-record-{index}"
            ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": record_id})
            refs.append(ref)
            repository.register_artifact(ArtifactIndexEntryV2(ref, "PublicObservationIndexV2",
                sha256_json({"observation": record_id}), NOW - 1, NOW - 1,
                {"record_id": record_id, "source_id": SOURCE}))
        exact_refs = tuple(sorted(refs))
        verified = repository.get_artifact_metadata_by_refs(exact_refs)
        assert set(verified) == set(exact_refs)
        original_lookup = repository.get_artifact_metadata_by_refs

        def guarded_lookup(requested_refs):
            assert not set(requested_refs).intersection(exact_refs), (
                "verified exact observation indexes must be reused within this cycle"
            )
            return original_lookup(requested_refs)

        monkeypatch.setattr(repository, "get_artifact_metadata_by_refs", guarded_lookup)
        callbacks = []

        health = collector.reconcile_after_reconnect(SOURCE, at_ns=NOW,
            complete_snapshot=True, missed_interval_repaired=True, snapshot_refs=exact_refs,
            _verified_snapshot_index_entries=verified,
            service_callback=lambda: callbacks.append(len(callbacks) + 1))

        assert health.state.value == "HEALTHY_CURRENT"
        assert callbacks == [1, 2, 3]
        monkeypatch.setattr(repository, "get_artifact_metadata_by_refs", original_lookup)
        receipt = repository.latest_artifact_entries("OpsPublicSourceReconciliationV1",
            as_of_ns=NOW, limit=1).entries[0]
        assert receipt.metadata["reconciliation"]["evidence_refs"] == exact_refs


def test_reconnect_reconciliation_revalidates_reused_entries_and_rejects_mismatched_source(
    tmp_path, monkeypatch,
) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        collector = PublicCollectorV2(repository=repository, registry=InstrumentRegistryV2(),
            clock_ns=lambda: NOW)
        refs = []
        for index in range(2):
            record_id = f"mismatched-reconciliation-record-{index}"
            ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": record_id})
            refs.append(ref)
            repository.register_artifact(ArtifactIndexEntryV2(ref, "PublicObservationIndexV2",
                sha256_json({"observation": record_id}), NOW - 1, NOW - 1,
                {"record_id": record_id, "source_id": SOURCE}))
        exact_refs = tuple(sorted(refs))
        verified = repository.get_artifact_metadata_by_refs(exact_refs)
        wrong_ref = exact_refs[0]
        wrong_entry = dict(verified[wrong_ref])
        wrong_entry["metadata"] = {**wrong_entry["metadata"], "source_id": "OTHER_SOURCE"}
        verified[wrong_ref] = wrong_entry
        original_lookup = repository.get_artifact_metadata_by_refs

        def unexpected_lookup(_refs):
            raise AssertionError("reconciliation must validate the supplied values directly")

        monkeypatch.setattr(repository, "get_artifact_metadata_by_refs", unexpected_lookup)
        with pytest.raises(ValueError, match="exact available source observation refs"):
            collector.reconcile_after_reconnect(SOURCE, at_ns=NOW,
                complete_snapshot=True, missed_interval_repaired=True, snapshot_refs=exact_refs,
                _verified_snapshot_index_entries=verified)
        monkeypatch.setattr(repository, "get_artifact_metadata_by_refs", original_lookup)
        assert repository.artifact_entries("OpsPublicSourceReconciliationV1") == ()
