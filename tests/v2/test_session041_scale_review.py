"""Bounded synthetic breadth measurements; no endpoint or endurance qualification."""
from __future__ import annotations

import json
import time
import zlib
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.data.broad_public_source import BroadPublicSnapshotV2, PublicInputRecordV2
from atlas.v2.data.collector import PublicCollectorV2
from atlas.v2.data.history import ParquetObservationArchiveV2
from atlas.v2.data.raw import RawObservationV2
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    InstrumentRegistryV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.broad_universe import full_universe, latest_workset, publish_broad_workset
from atlas.v2.runtime.production import ProductionOpsCyclePortV1

NOW = 1_800_000_000_000_000_000
STEP = 10_000_000_000
SOURCES = ("BINANCE_USDM_PUBLIC_V2", "BYBIT_PUBLIC_V2")


def _products(per_venue):
    result = []
    for venue in VenueV2:
        for index in range(per_venue):
            symbol = f"SCALE{index}USDT"
            revision = sha256_json({"symbol": symbol, "venue": venue.value})
            key = InstrumentKeyV2(venue, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
                symbol, f"SCALE{index}", "USDT", "USDT", revision)
            result.append(ProductContractV2(key, NOW, NOW, NOW, Decimal("1"), Decimal("0.01"),
                Decimal("0.001"), Decimal("0.001"), TradingStatusV2.TRADING, revision))
    return tuple(result)


def _records(products, at, *, future=False):
    records = []
    for product in products:
        symbol = product.key.native_symbol
        if product.key.venue == VenueV2.BYBIT:
            rows = (("TICKER_MARK_INDEX_FUNDING_OI", {
                "symbol": symbol, "lastPrice": "100.01", "indexPrice": "100", "markPrice": "100",
                "bid1Price": "99.99", "bid1Size": "142.250", "ask1Price": "100.01",
                "ask1Size": "162.125", "turnover24h": "20000000", "volume24h": "200000",
                "openInterest": "42000", "openInterestValue": "4200000",
                "fundingRate": "0.0001", "nextFundingTime": "1800007200000"}),)
            source = "BYBIT_PUBLIC_V2"
        else:
            rows = (("BOOK_TICKER", {"symbol": symbol, "bidPrice": "99.99", "bidQty": "142.250",
                    "askPrice": "100.01", "askQty": "162.125", "time": at // 1_000_000}),
                ("MARK_INDEX_CURRENT_FUNDING", {"symbol": symbol, "markPrice": "100",
                    "indexPrice": "100", "estimatedSettlePrice": "100", "lastFundingRate": "0.0001",
                    "interestRate": "0.0001", "nextFundingTime": 1800007200000,
                    "time": at // 1_000_000}),
                ("TICKER_24H", {"symbol": symbol, "lastPrice": "100.01", "priceChange": "1.00",
                    "priceChangePercent": "1.01", "weightedAvgPrice": "99.7", "openPrice": "99.01",
                    "highPrice": "101", "lowPrice": "98", "volume": "200000",
                    "quoteVolume": "20000000", "openTime": 1799913600000,
                    "closeTime": at // 1_000_000, "firstId": 120000, "lastId": 125000, "count": 5001}))
            source = "BINANCE_USDM_PUBLIC_V2"
        for event, row in rows:
            payload = canonical_json(row).encode()
            raw = RawObservationV2.build(instrument_revision=product.key.contract_revision,
                source_id=source, event_type=event, event_at_ns=at + 1 if future else at,
                received_at_ns=at, ingested_at_ns=at, available_at_ns=at, payload=payload,
                translation_version="SESSION041_SYNTHETIC_SCALE_V1", sequence=f"bulk-receipt:{at}")
            records.append(PublicInputRecordV2(raw, payload, product.key))
    return tuple(records)


def _snapshot(records, at):
    manifest = {"acquisition_due": True, "metadata": {
        venue.value: {"market_status": "BULK_MARKET_COMPLETE", "metadata_received_at_ns": NOW}
        for venue in VenueV2}, "synthetic_fixture": True}
    manifest["source_snapshot_id"] = sha256_json(manifest)
    return BroadPublicSnapshotV2(records, True, None, None, at, at, 4, 4, 0, 0, 0, manifest)


def measure_bounded_cycles(root: Path, *, per_venue=128, cycles=2):
    """Real SQLite/collector/Parquet/workset path; fake source and fixed receipts."""
    root.mkdir(parents=True, exist_ok=True)
    products = _products(per_venue)
    at = [NOW]
    registry = InstrumentRegistryV2()
    for product in products:
        registry.register(product)
    metrics = {"population": len(products), "cycles": cycles, "cadence_seconds": 10,
        "source_shape": "one Bybit ticker and three Binance bulk market rows per symbol",
        "excluded": ["network", "streams", "enrichment", "scheduler state", "reports", "decision work"],
        "samples": []}
    with OpsRepository(root / "ops.sqlite") as repo:
        repo.register_artifacts(tuple(ArtifactIndexEntryV2(p.content_hash, "ProductContractV2", p.content_hash,
            NOW, NOW, {"product": p.to_dict()}) for p in products))
        collector = PublicCollectorV2(repository=repo, registry=registry, clock_ns=lambda: at[0],
            archive=ParquetObservationArchiveV2(root / "ops-observations",
                compact_stream_repository=repo, clock_ns=lambda: at[0]))
        collector.publish_indexes_after_archive = True
        port = SimpleNamespace(_collector_recovery=SimpleNamespace(collector=collector),
            public_source=SimpleNamespace(enabled_venues=tuple(VenueV2), required_source_ids=SOURCES),
            clock_ns=lambda: at[0], service_public_stream=lambda _repo: None)
        def db_bytes():
            return (repo._connection.execute("PRAGMA page_count").fetchone()[0]
                * repo._connection.execute("PRAGMA page_size").fetchone()[0])
        def storage_write_bytes():
            return int(next(line.split(":", 1)[1] for line in Path("/proc/self/io").read_text().splitlines()
                if line.startswith("write_bytes:")))
        baseline = db_bytes()
        write_baseline = storage_write_bytes()
        previous_writes = write_baseline
        previous = baseline
        hashes = []
        expected_raw = {}
        expected_index = {}
        for cycle in range(cycles):
            at[0] = NOW + cycle * STEP
            records = _records(products, at[0])
            for record in records:
                observation = record.observation
                assert observation.record_id not in expected_raw
                expected_raw[observation.record_id] = (canonical_json(observation.to_dict()), record.raw_payload)
            acquired = _snapshot(records, at[0])
            started = time.perf_counter()
            assert ProductionOpsCyclePortV1._persist_broad_public_snapshot(port, repo, acquired, now_ns=at[0])
            adoption_seconds = time.perf_counter() - started
            receipt = repo.latest_artifact_entries("BroadPublicAcquisitionReceiptV2",
                as_of_ns=at[0], limit=1).entries[0]
            exact_refs = [ref for values in receipt.metadata["receipt"]["source_observation_refs"].values()
                for ref in values]
            assert len(exact_refs) == 4 * per_venue
            assert set(exact_refs) == {sha256_json({"artifact_type": "PublicObservationIndexV2",
                "record_id": record.observation.record_id}) for record in records}
            exact_entries = repo.get_artifact_metadata_by_refs(exact_refs)
            assert len(exact_entries) == len(records)
            for record in records:
                ref = sha256_json({"artifact_type": "PublicObservationIndexV2",
                    "record_id": record.observation.record_id})
                entry = repo.get_artifact(ref)
                assert entry is not None and entry.content_hash == record.observation.content_hash
                assert entry.available_at_ns == at[0]
                assert canonical_json(exact_entries[ref]["metadata"]) == canonical_json(entry.metadata)
                expected_index[ref] = canonical_json(entry.metadata)
            assert receipt.metadata["receipt"]["rejected_record_indexes"] == ()
            started = time.perf_counter()
            body = publish_broad_workset(repo, products=products, snapshot=acquired,
                available_at_ns=at[0], acquisition_ref=receipt.artifact_ref,
                source_state=dict.fromkeys(SOURCES, "HEALTHY_CURRENT"), clock_ns=lambda: at[0])
            publication_seconds = time.perf_counter() - started
            universe = full_universe(repo, cutoff_ns=at[0])
            assert len(universe.entries) == len(products)
            assert body["selected_count"] <= 24
            assert not any(entry.scanner_eligible or entry.capital_eligible for entry in universe.entries)
            hashes.append(sha256_json(body))
            rows = repo._connection.execute("SELECT artifact_type,count(*),sum(length(CAST(metadata_json AS BLOB))) "
                "FROM artifact_index GROUP BY artifact_type").fetchall()
            metadata = {row[0]: {"rows": row[1], "bytes": row[2]} for row in rows}
            encoded = repo._connection.execute("SELECT artifact_type,metadata_json FROM artifact_index "
                "WHERE available_at_ns=? AND artifact_type != 'ProductContractV2'", (at[0],)).fetchall()
            compressed = {}
            for kind in sorted({row[0] for row in encoded}):
                payload = ("[" + ",".join(row[1] for row in encoded if row[0] == kind) + "]").encode()
                compact_payload = zlib.compress(payload, level=1)
                assert zlib.decompress(compact_payload) == payload
                compressed[kind] = len(compact_payload)
            current = db_bytes()
            current_writes = storage_write_bytes()
            metrics["samples"].append({"cycle": cycle + 1, "record_count": len(records),
                "raw_payload_bytes": sum(len(record.raw_payload) for record in records),
                "db_logical_bytes": current, "db_growth_bytes": current - previous,
                "process_storage_write_bytes_cycle": current_writes - previous_writes,
                "wal_sample_bytes": (root / "ops.sqlite-wal").stat().st_size,
                "archive_cumulative_bytes": sum(path.stat().st_size for path in (root / "ops-observations").glob("*.parquet")),
                "metadata_by_type_cumulative": metadata, "zlib_metadata_bytes_by_type_cycle": compressed,
                "adoption_seconds": adoption_seconds, "workset_publication_seconds": publication_seconds})
            previous = current
            previous_writes = current_writes
        assert repo._connection.execute("SELECT count(*) FROM artifact_index WHERE artifact_type='PublicObservationIndexV2'").fetchone()[0] == cycles * 4 * per_venue
        assert repo._connection.execute("SELECT count(*) FROM artifact_index WHERE artifact_type='UniverseObservationV2'").fetchone()[0] == cycles * 2 * per_venue
        for cycle, ref in enumerate(hashes):
            assert sha256_json(latest_workset(repo, cutoff_ns=NOW + cycle * STEP)) == ref
        metrics["db_baseline_bytes"] = baseline
        metrics["process_storage_write_bytes_total"] = storage_write_bytes() - write_baseline
        metrics["dbstat_bytes"] = {row[0]: row[1] for row in repo._connection.execute(
            "SELECT name,sum(pgsize) FROM dbstat GROUP BY name")}
        expected_artifacts = {row["artifact_ref"]: (row["artifact_type"], row["content_hash"],
            row["created_at_ns"], row["available_at_ns"], row["metadata_json"])
            for row in repo._connection.execute("SELECT * FROM artifact_index")}
        repo._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    with OpsRepository(root / "ops.sqlite", read_only=True) as restarted:
        for cycle, ref in enumerate(hashes):
            assert sha256_json(latest_workset(restarted, cutoff_ns=NOW + cycle * STEP)) == ref
            assert len(full_universe(restarted, cutoff_ns=NOW + cycle * STEP).entries) == len(products)
            observed = restarted.artifact_entries_by_types(("PublicObservationIndexV2",),
                limit=10_000, available_before_ns=NOW + cycle * STEP)
            assert len(observed) == (cycle + 1) * 4 * per_venue
            assert all(entry.available_at_ns <= NOW + cycle * STEP for entry in observed)
        for ref, metadata in expected_index.items():
            entry = restarted.get_artifact(ref)
            assert entry is not None and canonical_json(entry.metadata) == metadata
        for ref, expected in expected_artifacts.items():
            entry = restarted.get_artifact(ref)
            assert entry is not None
            assert (entry.artifact_type, entry.content_hash, entry.created_at_ns,
                entry.available_at_ns, canonical_json(entry.metadata)) == expected
    import pyarrow.parquet as pq

    archived = {}
    for path in (root / "ops-observations").glob("*.parquet"):
        for row in pq.read_table(path, columns=["record_id", "observation_json", "raw_payload_bytes"]).to_pylist():
            assert row["record_id"] not in archived
            archived[row["record_id"]] = (canonical_json(json.loads(row["observation_json"])), row["raw_payload_bytes"])
    assert archived == expected_raw
    assert all(json.loads(raw[0])["sequence"] == f"bulk-receipt:{json.loads(raw[0])['received_at_ns']}"
        for raw in archived.values())
    metrics["integrity"] = {"exact_raw_rows_verified": len(archived),
        "exact_index_metadata_verified_after_restart": len(expected_index),
        "all_artifact_domain_metadata_verified_after_restart": len(expected_artifacts),
        "source_receipt_refs_verified": len(expected_index),
        "cutoff_worksets_verified_after_restart": len(hashes),
        "compression_roundtrip": "EXACT_CANONICAL_BYTES"}
    metrics["db_final_file_bytes"] = (root / "ops.sqlite").stat().st_size
    metrics["archive_file_count"] = len(tuple((root / "ops-observations").glob("*.parquet")))
    return metrics


def test_two_representative_bulk_cycles_retain_all_receipts_and_cutoffs(tmp_path):
    metrics = measure_bounded_cycles(tmp_path)
    print("SESSION041_SCALE_METRICS=" + json.dumps(metrics, sort_keys=True))
    assert metrics["population"] == 256
    assert all(sample["record_count"] == 512 for sample in metrics["samples"])
    assert metrics["archive_file_count"] == 2
    assert metrics["samples"][1]["db_growth_bytes"] > 0


def test_future_row_rejection_keeps_exact_rejection_identity(tmp_path):
    products = _products(1)
    registry = InstrumentRegistryV2()
    for product in products:
        registry.register(product)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        collector = PublicCollectorV2(repository=repo, registry=registry, clock_ns=lambda: NOW,
            archive=ParquetObservationArchiveV2(tmp_path / "ops-observations"))
        port = SimpleNamespace(_collector_recovery=SimpleNamespace(collector=collector),
            public_source=SimpleNamespace(enabled_venues=tuple(VenueV2), required_source_ids=SOURCES),
            clock_ns=lambda: NOW, service_public_stream=lambda _repo: None)
        records = _records(products, NOW, future=True)
        assert not ProductionOpsCyclePortV1._persist_broad_public_snapshot(port, repo,
            _snapshot(records, NOW), now_ns=NOW)
        receipt = repo.latest_artifact_entries("BroadPublicAcquisitionReceiptV2", as_of_ns=NOW, limit=1).entries[0]
        assert list(receipt.metadata["receipt"]["rejected_record_indexes"]) == list(range(4))
        assert not any(receipt.metadata["receipt"]["source_observation_refs"].values())
        rejections = repo.latest_artifact_entries("BroadPublicAdoptionRejectionV1", as_of_ns=NOW, limit=4).entries
        assert {entry.metadata["rejection"]["observation_hash"] for entry in rejections} == {
            record.observation.content_hash for record in records}
        assert all(entry.available_at_ns == NOW for entry in rejections)
