"""Bounded exact reconciliation reads yield to the sole-writer stream lane."""

from __future__ import annotations

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
