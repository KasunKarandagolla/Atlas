"""Migration, block publication, and corruption checks for public indexes."""
from __future__ import annotations

import json
import sqlite3
import zlib
from decimal import Decimal

import pytest

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.data.collector import PublicCollectorV2
from atlas.v2.data.raw import AvailabilityClassV2, RawObservationV2
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    InstrumentRegistryV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)
from atlas.v2.memory import repository as repository_module
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository

NOW = 1_800_000_000_000_000_000
SOURCE = "BINANCE_USDM_PUBLIC_V2"


def _key(symbol: str = "BTCUSDT") -> InstrumentKeyV2:
    revision = sha256_json({"symbol": symbol, "revision": "migration-test"})
    return InstrumentKeyV2(VenueV2.BINANCE, EnvironmentV2.MAINNET,
        ProductTypeV2.LINEAR_PERPETUAL, symbol, symbol.removesuffix("USDT"),
        "USDT", "USDT", revision)


def _entry(*, record: str = "trade-1", event_type: str = "TRADE",
           event_at: int = NOW + 1, available_at: int = NOW + 3,
           key: InstrumentKeyV2 | None = None) -> ArtifactIndexEntryV2:
    key = key or _key()
    record_id = sha256_json({"domain_record": record})
    metadata = {
        "record_id": record_id, "source_id": SOURCE, "event_type": event_type,
        "instrument_revision": key.contract_revision, "event_at_ns": event_at,
        "published_at_ns": event_at + 1, "received_at_ns": available_at,
        "ingested_at_ns": available_at, "available_at_ns": available_at,
        "index_published_at_ns": available_at, "raw_available_at_ns": available_at,
        "translation_version": "BLOCK_MIGRATION_TEST_V1", "revision_of": None,
        "quality_flags": [], "availability_class": "ACTUAL_SYSTEM",
        "replay_available_at_ns": None, "raw_payload_hash": sha256_json({"payload": record}),
        "bar_content_hash": sha256_json({"bar": record}) if event_type.startswith("BAR_") else None,
        "archive_chunk_id": "a" * 64, "instrument_key_json": key.to_canonical_json(),
    }
    ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": record_id})
    return ArtifactIndexEntryV2(ref, "PublicObservationIndexV2", sha256_json(metadata),
        available_at, available_at, metadata)


def _full_json_seed(path, entries):
    """Create rows and legacy public indexes, then remove the S41 tables."""
    repository = OpsRepository(path)
    connection = repository._connection
    for entry in entries:
        connection.execute("INSERT INTO artifact_index VALUES(?,?,?,?,?,?)", (
            entry.artifact_ref, entry.artifact_type, entry.content_hash,
            entry.created_at_ns, entry.available_at_ns, canonical_json(entry.metadata)))
    for index in (
        "public_exact_receipt_key_actual_lookup",
        "public_archive_history_lookup",
    ):
        connection.execute(f"DROP INDEX IF EXISTS {index}")
    connection.execute(
        "CREATE INDEX public_exact_receipt_key_actual_lookup ON artifact_index ("
        "json_extract(metadata_json,'$.instrument_revision'),"
        "json_extract(metadata_json,'$.instrument_key_json'),"
        "json_extract(metadata_json,'$.event_type'),"
        "json_extract(metadata_json,'$.availability_class'),available_at_ns DESC,"
        "json_extract(metadata_json,'$.record_id') DESC,artifact_ref DESC) "
        "WHERE artifact_type='PublicObservationIndexV2'")
    connection.execute(
        "CREATE INDEX public_archive_history_lookup ON artifact_index ("
        "json_extract(metadata_json,'$.instrument_key_json'),"
        "json_extract(metadata_json,'$.instrument_revision'),"
        "json_extract(metadata_json,'$.event_type'),"
        "json_extract(metadata_json,'$.availability_class'),"
        "json_extract(metadata_json,'$.event_at_ns'),available_at_ns,artifact_ref) "
        "WHERE artifact_type='PublicObservationIndexV2'")
    for name in ("public_instrument_identity_v1", "public_observation_metadata_block_v1",
                 "public_observation_metadata_locator_v1"):
        connection.execute(f"DROP TABLE IF EXISTS {name}")
    repository.close()


def test_legacy_full_json_read_only_queries_work_before_writable_migration(tmp_path):
    key = _key()
    trade = _entry(record="receipt", event_type="TRADE", key=key)
    bar = _entry(record="bar", event_type="BAR_1m", event_at=NOW + 2,
                 available_at=NOW + 4, key=key)
    path = tmp_path / "legacy.sqlite"
    _full_json_seed(path, (trade, bar))

    # A legacy full-JSON database remains queryable before any writable open.
    with OpsRepository(path, read_only=True) as legacy:
        assert legacy._public_metadata_enabled is False
        receipt = legacy.public_archive_history_entries(
            instrument_revision=key.contract_revision, event_types=("TRADE",),
            information_cutoff_ns=NOW + 3, limit=10,
            instrument_key_json=key.to_canonical_json(), availability_class="ACTUAL_SYSTEM")
        assert tuple(item.artifact_ref for item in receipt) == (trade.artifact_ref,)
        bars = legacy.public_archive_history_entries(
            instrument_revision=key.contract_revision, event_types=("BAR_1m",),
            information_cutoff_ns=NOW + 5, limit=10,
            instrument_key_json=key.to_canonical_json(), availability_class="ACTUAL_SYSTEM")
        assert tuple(item.artifact_ref for item in bars) == (bar.artifact_ref,)
        assert bars[0].metadata["instrument_key_json"] == key.to_canonical_json()

    with OpsRepository(path) as migrated:
        assert migrated._public_metadata_enabled is True
        assert migrated.get_artifact(trade.artifact_ref) == trade
        assert migrated.get_artifact(bar.artifact_ref) == bar


def test_failed_writable_migration_rolls_back_indexes_and_releases_writer_lease(
        tmp_path, monkeypatch):
    key = _key()
    trade = _entry(record="migration-failure", key=key)
    path = tmp_path / "interrupted-migration.sqlite"
    _full_json_seed(path, (trade,))
    with sqlite3.connect(path) as connection:
        # Remove the v2 query indexes so this is an actual pre-migration
        # full-JSON database with only its legacy query paths available.
        connection.execute("DROP INDEX IF EXISTS public_exact_receipt_key_actual_lookup_v2")
        connection.execute("DROP INDEX IF EXISTS public_archive_history_lookup_v2")

    original_indexes = repository_module._ARCHIVE_QUERY_INDEXES
    monkeypatch.setattr(repository_module, "_ARCHIVE_QUERY_INDEXES",
                        (*original_indexes, "CREATE INDEX intentionally_invalid_migration_sql"))
    with pytest.raises(sqlite3.OperationalError, match="incomplete input"):
        OpsRepository(path)

    with sqlite3.connect(path) as connection:
        names = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type='table'")}
        indexes = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type='index'")}
    assert not ({"public_instrument_identity_v1", "public_observation_metadata_block_v1",
                 "public_observation_metadata_locator_v1"} & names)
    assert "public_exact_receipt_key_actual_lookup" in indexes
    assert "public_archive_history_lookup" in indexes

    # Read-only legacy access remains available after the failed migration.
    with OpsRepository(path, read_only=True) as legacy:
        assert legacy._public_metadata_enabled is False
        receipt = legacy.public_archive_history_entries(
            instrument_revision=key.contract_revision, event_types=("TRADE",),
            information_cutoff_ns=NOW + 5, limit=10,
            instrument_key_json=key.to_canonical_json(), availability_class="ACTUAL_SYSTEM")
        assert tuple(item.artifact_ref for item in receipt) == (trade.artifact_ref,)

    # A normal writable reopen succeeds immediately. This also proves the
    # failed constructor released its OS writer lease.
    monkeypatch.setattr(repository_module, "_ARCHIVE_QUERY_INDEXES", original_indexes)
    with OpsRepository(path) as migrated:
        assert migrated._public_metadata_enabled is True
        assert migrated.get_artifact(trade.artifact_ref) == trade


def test_repeated_writable_reopen_keeps_v2_indexes_and_schema_version_stable(tmp_path):
    path = tmp_path / "indexes.sqlite"
    with OpsRepository(path) as repository:
        version = repository._connection.execute("PRAGMA schema_version").fetchone()[0]
        names = tuple(row[0] for row in repository._connection.execute(
            "SELECT name FROM sqlite_schema WHERE type='index' AND name LIKE '%_v2' ORDER BY name"))
        snapshot = tuple(repository._connection.execute(
            "SELECT name,rootpage,sql FROM sqlite_schema WHERE type='index' AND name LIKE '%_v2' ORDER BY name"))
    assert names
    for _ in range(2):
        with OpsRepository(path) as reopened:
            assert reopened._connection.execute("PRAGMA schema_version").fetchone()[0] == version
            assert tuple(reopened._connection.execute(
                "SELECT name,rootpage,sql FROM sqlite_schema WHERE type='index' AND name LIKE '%_v2' ORDER BY name")) == snapshot


def test_failed_block_locator_artifact_publication_rolls_back_all_rows(tmp_path):
    repository = OpsRepository(tmp_path / "atomic.sqlite")
    entries = (_entry(record="atomic-1"), _entry(record="atomic-2", event_at=NOW + 2))
    try:
        repository._connection.execute(
            "CREATE TRIGGER reject_public_artifact BEFORE INSERT ON artifact_index "
            "WHEN NEW.artifact_type='PublicObservationIndexV2' BEGIN SELECT RAISE(ABORT,'injected'); END")
        with pytest.raises(sqlite3.IntegrityError, match="injected"):
            repository.register_artifacts(entries)
        for table in ("public_observation_metadata_block_v1",
                      "public_observation_metadata_locator_v1", "public_instrument_identity_v1"):
            assert repository._connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0
        assert repository._connection.execute(
            "SELECT count(*) FROM artifact_index WHERE artifact_type='PublicObservationIndexV2'").fetchone()[0] == 0
    finally:
        repository.close()


@pytest.mark.parametrize("damage", ["missing_locator", "invalid_ordinal", "invalid_hash",
                                     "missing_identity", "invalid_identity_hash", "oversized_blob"])
def test_compact_metadata_corruption_fails_closed(tmp_path, damage):
    repository = OpsRepository(tmp_path / f"corrupt-{damage}.sqlite")
    entry = _entry(record=damage)
    try:
        repository.register_artifact(entry)
        connection = repository._connection
        locator = connection.execute(
            "SELECT block_ref,ordinal FROM public_observation_metadata_locator_v1 WHERE artifact_ref=?",
            (bytes.fromhex(entry.artifact_ref),)).fetchone()
        key_ref = bytes.fromhex(sha256_json({
            "version": "PublicInstrumentIdentityRefV1",
            "instrument_key_json": entry.metadata["instrument_key_json"]}))
        if damage == "missing_locator":
            connection.execute("DELETE FROM public_observation_metadata_locator_v1 WHERE artifact_ref=?",
                               (bytes.fromhex(entry.artifact_ref),))
        elif damage == "invalid_ordinal":
            connection.execute("PRAGMA ignore_check_constraints=ON")
            connection.execute("UPDATE public_observation_metadata_locator_v1 SET ordinal=512 WHERE artifact_ref=?",
                               (bytes.fromhex(entry.artifact_ref),))
        elif damage == "invalid_hash":
            row = connection.execute("SELECT compressed_metadata FROM public_observation_metadata_block_v1 "
                                     "WHERE block_ref=?", (locator["block_ref"],)).fetchone()
            body = json.loads(zlib.decompress(row["compressed_metadata"]))
            body["entries"][0]["metadata"]["source_id"] = "tampered"
            corrupted = canonical_json(body).encode()
            connection.execute("UPDATE public_observation_metadata_block_v1 SET uncompressed_bytes=?,"
                               "compressed_metadata=? WHERE block_ref=?",
                               (len(corrupted), zlib.compress(corrupted), locator["block_ref"]))
        elif damage == "missing_identity":
            connection.execute("DELETE FROM public_instrument_identity_v1 WHERE instrument_key_ref=?", (key_ref,))
        elif damage == "invalid_identity_hash":
            connection.execute("UPDATE public_instrument_identity_v1 SET key_sha256=? WHERE instrument_key_ref=?",
                               (b"x" * 32, key_ref))
        elif damage == "oversized_blob":
            connection.execute("PRAGMA ignore_check_constraints=ON")
            connection.execute("UPDATE public_observation_metadata_block_v1 SET compressed_metadata=? "
                               "WHERE block_ref=?", (b"x" * (4 * 1024 * 1024 + 65537), locator["block_ref"]))
        with pytest.raises((ValueError, TypeError, sqlite3.DatabaseError)):
            repository.get_artifact(entry.artifact_ref)
    finally:
        repository.close()


def test_collector_prefetch_rejects_missing_identity(tmp_path):
    repository = OpsRepository(tmp_path / "prefetch.sqlite")
    key = _key()
    product = ProductContractV2(key, NOW, NOW, NOW, Decimal("1"), Decimal("0.01"),
        Decimal("0.001"), Decimal("0.001"), TradingStatusV2.TRADING, key.contract_revision)
    registry = InstrumentRegistryV2()
    registry.register(product)
    collector = PublicCollectorV2(repository=repository, registry=registry, clock_ns=lambda: NOW + 10)
    payload = b'{"price":"100"}'
    observation = RawObservationV2.build(instrument_revision=key.contract_revision, source_id=SOURCE,
        event_type="TRADE", event_at_ns=NOW + 1, published_at_ns=NOW + 2,
        received_at_ns=NOW + 3, ingested_at_ns=NOW + 4, available_at_ns=NOW + 4,
        payload=payload, translation_version="BLOCK_MIGRATION_TEST_V1", sequence="prefetch-1",
        availability_class=AvailabilityClassV2.ACTUAL_SYSTEM)
    try:
        with pytest.raises(ValueError, match="does not cover the current identity"):
            collector.ingest(observation, raw_payload=payload, instrument_key=key,
                update_source_health=False, retain_in_memory=False,
                persisted_public_index_rows_by_ref={})
    finally:
        repository.close()
