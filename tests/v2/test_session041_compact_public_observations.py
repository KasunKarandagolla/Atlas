"""Lossless storage and query contract for compact public observation indexes.

Fixtures deliberately use the production collector and Parquet archive path,
with fixed synthetic inputs as in ``test_session041_scale_review``.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.data.collector import PublicCollectorV2
from atlas.v2.data.history import ParquetObservationArchiveV2
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
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository

NOW = 1_800_000_000_000_000_000
STEP = 10_000_000_000
EVENTS = ("TRADE", "AGG_TRADE")
SOURCE = "BINANCE_USDM_PUBLIC_V2"


def _product(symbol: str, *, venue: VenueV2 = VenueV2.BINANCE,
             revision_salt: str = "r0") -> ProductContractV2:
    revision = sha256_json({"symbol": symbol, "venue": venue.value, "revision": revision_salt})
    key = InstrumentKeyV2(venue, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
        symbol, symbol.removesuffix("USDT"), "USDT", "USDT", revision)
    return ProductContractV2(key, NOW, NOW, NOW, Decimal("1"), Decimal("0.01"),
        Decimal("0.001"), Decimal("0.001"), TradingStatusV2.TRADING, revision)


def _archive_fixture(root, specs):
    """Archive (key, type, event time, actual receipt, replay time, class) rows."""
    products = {product.key.to_canonical_json(): product for product in {
        spec[0] for spec in specs
    }}
    registry = InstrumentRegistryV2()
    for product in products.values():
        registry.register(product)
    clock = [max(spec[3] for spec in specs) + 5]
    repository = OpsRepository(root / "ops.sqlite")
    collector = PublicCollectorV2(repository=repository, registry=registry,
        clock_ns=lambda: clock[0], archive=ParquetObservationArchiveV2(
            root / "ops-observations", compact_stream_repository=repository,
            clock_ns=lambda: clock[0]))
    collector.publish_indexes_after_archive = True
    expected = {}
    for index, (product, event_type, event_at, received_at, replay_at, availability) in enumerate(specs):
        payload = canonical_json({"symbol": product.key.native_symbol, "n": index,
            "price": "100.0100", "opaque": "δ" * 40}).encode("utf-8")
        observation = RawObservationV2.build(instrument_revision=product.key.contract_revision,
            source_id=SOURCE, event_type=event_type, event_at_ns=event_at,
            published_at_ns=event_at + 1, received_at_ns=received_at,
            ingested_at_ns=received_at + 1, available_at_ns=received_at + 1,
            payload=payload, translation_version="SESSION041_COMPACT_PUBLIC_TEST_V1",
            sequence=f"receipt-{index}", availability_class=availability,
            replay_available_at_ns=replay_at if availability == AvailabilityClassV2.RECONSTRUCTED_MARKET else None)
        accepted = collector.ingest(observation, raw_payload=payload, instrument_key=product.key,
            update_source_health=False, retain_in_memory=False)
        assert accepted.append.status.value == "INSERTED"
        ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": observation.record_id})
        expected[ref] = (observation, product.key, payload)
    archive_path = collector.flush_archive()
    assert archive_path is not None
    chunk_id = Path(archive_path).stem
    # This is the effective index publication time written by the collector.
    expected_entries = {}
    for ref, (observation, key, _payload) in expected.items():
        entry = repository.get_artifact(ref)
        assert entry is not None
        expected_entries[ref] = entry
        assert entry.metadata["archive_chunk_id"] == chunk_id
        assert entry.metadata["index_published_at_ns"] == clock[0]
        assert entry.metadata["raw_available_at_ns"] == observation.available_at_ns
        assert entry.available_at_ns == clock[0]
        assert entry.content_hash == observation.content_hash
        assert entry.metadata["instrument_key_json"] == key.to_canonical_json()
    return repository, expected_entries, expected


def _canonical_entry(entry):
    return (entry.artifact_ref, entry.artifact_type, entry.content_hash,
        entry.created_at_ns, entry.available_at_ns, canonical_json(entry.metadata))


def test_compact_archive_index_round_trips_all_repository_reads_and_restart(tmp_path):
    product = _product("BTCUSDT")
    specs = [
        (product, "TRADE", NOW + 1, NOW + 100, None, AvailabilityClassV2.ACTUAL_SYSTEM),
        (product, "AGG_TRADE", NOW + 2, NOW + 100, None, AvailabilityClassV2.ACTUAL_SYSTEM),
        (product, "TRADE", NOW + 3, NOW + 200, None, AvailabilityClassV2.ACTUAL_SYSTEM),
        (product, "TRADE", NOW + 4, NOW + 300, NOW + 50,
            AvailabilityClassV2.RECONSTRUCTED_MARKET),
    ]
    repository, expected_entries, _ = _archive_fixture(tmp_path, specs)
    expected = tuple(sorted(expected_entries.values(),
        key=lambda entry: (entry.created_at_ns, entry.artifact_ref)))
    refs = tuple(entry.artifact_ref for entry in expected)
    try:
        # Public rows keep only a narrow query projection in the generic
        # index; exact domain metadata lives in bounded shared blocks.
        raw_rows = repository._connection.execute(
            "SELECT metadata_json FROM artifact_index WHERE artifact_type='PublicObservationIndexV2'"
        ).fetchall()
        assert len(raw_rows) == len(expected)
        for raw_row in raw_rows:
            projection = json.loads(raw_row["metadata_json"])
            assert "instrument_key_json" not in projection
            assert len(projection["instrument_key_ref"]) == 64
            assert set(projection) <= {"record_id", "source_id", "event_type", "instrument_revision",
                "event_at_ns", "availability_class", "replay_available_at_ns", "raw_payload_hash",
                "bar_content_hash", "archive_chunk_id", "instrument_key_ref"}
        assert repository._connection.execute(
            "SELECT count(*) FROM public_observation_metadata_locator_v1"
        ).fetchone()[0] == len(expected)
        assert repository._connection.execute(
            "SELECT count(*) FROM public_observation_metadata_block_v1"
        ).fetchone()[0] == 1
        assert tuple(map(_canonical_entry, repository.artifact_entries(
            "PublicObservationIndexV2"))) == tuple(map(_canonical_entry, expected))
        typed = repository.artifact_entries_by_types(("PublicObservationIndexV2",),
            available_before_ns=NOW + 1_000)
        assert {entry.artifact_ref: _canonical_entry(entry) for entry in typed} == {
            entry.artifact_ref: _canonical_entry(entry) for entry in expected}
        assert {ref: _canonical_entry(repository.get_artifact(ref)) for ref in refs} == {
            entry.artifact_ref: _canonical_entry(entry) for entry in expected}
        batch = repository.get_artifact_metadata_by_refs((*refs, "f" * 64))
        assert set(batch) == set(refs)
        for entry in expected:
            decoded = batch[entry.artifact_ref]
            assert (decoded["artifact_type"], decoded["content_hash"], decoded["available_at_ns"],
                canonical_json(decoded["metadata"])) == (entry.artifact_type, entry.content_hash,
                entry.available_at_ns, canonical_json(entry.metadata))
        # Reopening is the repository restart boundary used by this contract.
        path = repository.path
        repository.close()
        with OpsRepository(path, read_only=True) as restarted:
            assert tuple(map(_canonical_entry, restarted.artifact_entries(
                "PublicObservationIndexV2"))) == tuple(map(_canonical_entry, expected))
            assert {ref: _canonical_entry(restarted.get_artifact(ref)) for ref in refs} == {
                entry.artifact_ref: _canonical_entry(entry) for entry in expected}
            all_metadata = restarted.get_artifact_metadata_by_refs(refs)
            assert all(canonical_json(all_metadata[ref]["metadata"]) ==
                canonical_json(expected_entries[ref].metadata) for ref in refs)
    finally:
        repository.close()


def test_legacy_plain_observation_rows_remain_readable_with_compact_rows(tmp_path):
    product = _product("ETHUSDT")
    repository, compact_entries, _ = _archive_fixture(tmp_path, [
        (product, "TRADE", NOW + 1, NOW + 100, None, AvailabilityClassV2.ACTUAL_SYSTEM),
    ])
    try:
        record_id = sha256_json({"legacy": "session041", "record": 1})
        ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": record_id})
        metadata = {"record_id": record_id, "source_id": SOURCE, "event_type": "TRADE",
            "instrument_revision": product.key.contract_revision,
            "instrument_key_json": product.key.to_canonical_json(), "event_at_ns": NOW + 10,
            "published_at_ns": None, "translation_version": "LEGACY_PLAIN_V1", "revision_of": None,
            "quality_flags": [], "availability_class": "ACTUAL_SYSTEM",
            "replay_available_at_ns": None, "raw_payload_hash": hashlib.sha256(b"legacy").hexdigest(),
            "bar_content_hash": None, "archive_chunk_id": "e" * 64}
        legacy = ArtifactIndexEntryV2(ref, "PublicObservationIndexV2", sha256_json(metadata),
            NOW + 20, NOW + 20, metadata)
        # Seed an actual pre-compaction storage row to prove mixed-history
        # compatibility rather than passing it through the new encoder.
        repository._connection.execute(
            "INSERT INTO artifact_index VALUES(?,?,?,?,?,?)",
            (ref, legacy.artifact_type, legacy.content_hash, legacy.created_at_ns,
             legacy.available_at_ns, canonical_json(metadata)),
        )
        assert repository.get_artifact(ref) == legacy
        compact_ref = next(iter(compact_entries))
        assert repository.get_artifact(compact_ref) is not None
        listed = repository.artifact_entries("PublicObservationIndexV2")
        assert {entry.artifact_ref for entry in listed} == {ref, *compact_entries.keys()}
        listed_types = repository.artifact_entries_by_types(("PublicObservationIndexV2",),
            available_before_ns=NOW + 1_000)
        assert {entry.artifact_ref for entry in listed_types} == {ref, *compact_entries.keys()}
        batch = repository.get_artifact_metadata_by_refs((ref, *compact_entries.keys()))
        assert canonical_json(batch[ref]["metadata"]) == canonical_json(metadata)
    finally:
        repository.close()


def test_compact_observation_corruption_fails_closed(tmp_path):
    product = _product("SOLUSDT")
    repository, entries, _ = _archive_fixture(tmp_path, [
        (product, "TRADE", NOW + 1, NOW + 100, None, AvailabilityClassV2.ACTUAL_SYSTEM),
    ])
    entry = next(iter(entries.values()))
    try:
        # Damage the compact metadata digest without touching the archive;
        # reads must report corruption,
        # never synthesize a plausible row or fall back to an empty result.
        locator = repository._connection.execute(
            "SELECT block_ref FROM public_observation_metadata_locator_v1 WHERE artifact_ref=?",
            (bytes.fromhex(entry.artifact_ref),),
        ).fetchone()
        assert locator is not None
        repository._connection.execute(
            "UPDATE public_observation_metadata_block_v1 SET compressed_metadata=? WHERE block_ref=?",
            (b"damaged", locator["block_ref"]))
        with pytest.raises((ValueError, OSError, sqlite3.DatabaseError)):
            repository.get_artifact(entry.artifact_ref)
        with pytest.raises((ValueError, OSError, sqlite3.DatabaseError)):
            repository.get_artifact_metadata_by_refs((entry.artifact_ref,))
        with pytest.raises((ValueError, OSError, sqlite3.DatabaseError)):
            repository.artifact_entries("PublicObservationIndexV2")
    finally:
        repository.close()


def test_compact_history_queries_preserve_identity_cutoff_replay_and_ties(tmp_path):
    btc = _product("BTCUSDT")
    btc_other_revision = _product("BTCUSDT", revision_salt="r1")
    eth = _product("ETHUSDT")
    specs = [
        (btc, "TRADE", NOW + 20, NOW + 100, None, AvailabilityClassV2.ACTUAL_SYSTEM),
        (btc, "AGG_TRADE", NOW + 21, NOW + 100, None, AvailabilityClassV2.ACTUAL_SYSTEM),
        (btc, "TRADE", NOW + 22, NOW + 101, None, AvailabilityClassV2.ACTUAL_SYSTEM),
        (btc, "TRADE", NOW + 23, NOW + 102, NOW + 50,
            AvailabilityClassV2.RECONSTRUCTED_MARKET),
        (btc, "TRADE", NOW + 24, NOW + 103, NOW + 150,
            AvailabilityClassV2.RECONSTRUCTED_MARKET),
        (btc_other_revision, "TRADE", NOW + 25, NOW + 100, None, AvailabilityClassV2.ACTUAL_SYSTEM),
        (eth, "TRADE", NOW + 26, NOW + 100, None, AvailabilityClassV2.ACTUAL_SYSTEM),
    ]
    repository, entries, observations = _archive_fixture(tmp_path, specs)
    try:
        assert repository.public_archive_history_entries(
            instrument_revision=btc.key.contract_revision, event_types=EVENTS,
            information_cutoff_ns=NOW + 107, limit=10,
            instrument_key_json=btc.key.to_canonical_json(), availability_class="ACTUAL_SYSTEM") == ()
        actual = repository.public_archive_history_entries(
            instrument_revision=btc.key.contract_revision, event_types=EVENTS,
            information_cutoff_ns=NOW + 108, limit=10,
            instrument_key_json=btc.key.to_canonical_json(), availability_class="ACTUAL_SYSTEM")
        # Equal receipt times are ordered by descending record identity, then
        # artifact ref; the later observation is excluded at this cutoff.
        candidates = [entry for entry in entries.values()
            if entry.metadata["instrument_key_json"] == btc.key.to_canonical_json()
            and entry.metadata["availability_class"] == "ACTUAL_SYSTEM"
            and entry.metadata["event_type"] in EVENTS and entry.available_at_ns <= NOW + 108]
        expected = tuple(sorted(candidates,
            key=lambda entry: (entry.available_at_ns, entry.metadata["record_id"], entry.artifact_ref),
            reverse=True))
        assert tuple(map(_canonical_entry, actual)) == tuple(map(_canonical_entry, expected))
        assert all(item.metadata["instrument_revision"] == btc.key.contract_revision for item in actual)
        assert all(item.metadata["instrument_key_json"] == btc.key.to_canonical_json() for item in actual)

        reconstructed = repository.public_archive_history_entries(
            instrument_revision=btc.key.contract_revision, event_types=("TRADE",),
            information_cutoff_ns=NOW + 108, limit=10,
            instrument_key_json=btc.key.to_canonical_json(),
            availability_class="RECONSTRUCTED_MARKET")
        eligible = [entry for entry in entries.values()
            if entry.metadata["instrument_key_json"] == btc.key.to_canonical_json()
            and entry.metadata["availability_class"] == "RECONSTRUCTED_MARKET"
            and entry.metadata["event_type"] == "TRADE"
            and entry.metadata["replay_available_at_ns"] <= NOW + 108]
        expected_replay = tuple(sorted(eligible,
            key=lambda entry: (entry.available_at_ns, entry.metadata["record_id"], entry.artifact_ref),
            reverse=True))
        assert tuple(map(_canonical_entry, reconstructed)) == tuple(map(_canonical_entry, expected_replay))
        assert len(reconstructed) == 1
        assert reconstructed[0].metadata["replay_available_at_ns"] == NOW + 50

        # A missing key broadens only within the requested exact revision;
        # another contract revision and another instrument remain excluded.
        revision_scope = repository.public_archive_history_entries(
            instrument_revision=btc.key.contract_revision, event_types=("TRADE",),
            information_cutoff_ns=NOW + 108, limit=10, availability_class="ACTUAL_SYSTEM")
        assert {item.metadata["instrument_revision"] for item in revision_scope} == {
            btc.key.contract_revision}
        assert {item.metadata["instrument_key_json"] for item in revision_scope} == {
            btc.key.to_canonical_json()}
    finally:
        repository.close()
