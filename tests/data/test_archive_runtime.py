from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

from atlas.data.archive import DuckDBResearchCatalog, ParquetArchive
from atlas.data.models import DataKind, make_record
from atlas.domain.enums import AvailabilityClass
from atlas.persistence.sqlite import SQLiteJournal

T0 = 1_700_000_000_000_000_000


def record(record_id: str, instrument: str, close: str):
    return make_record(
        record_id=record_id,
        source_id="BYBIT_PUBLIC",
        venue="BYBIT",
        instrument=instrument,
        data_kind=DataKind.BAR_1M_LAST,
        payload={"close": close, "symbol": instrument},
        received_at_ns=T0 + 1_000,
        processed_at_ns=T0 + 2_000,
        available_at_ns=T0 + 2_000,
        data_ingested_at_ns=T0 + 1_000,
        recorded_at_ns=T0 + 2_000,
        evidence_ref=f"raw:{record_id}",
        source_event_at_ns=T0,
        bar_start_ns=T0 - 60_000_000_000,
        bar_end_ns=T0,
        availability_class=AvailabilityClass.ACTUAL_OBSERVED,
        dependency_ids=("instrument-metadata",),
    )


def test_parquet_is_real_partitioned_idempotent_and_append_only(tmp_path: Path):
    archive = ParquetArchive(tmp_path / "archive")
    btc = record("btc-1", "BTCUSDT", "50000")
    eth = record("eth-1", "ETHUSDT", "3000")

    first = archive.write_batch([btc])
    duplicate = archive.write_batch([btc])
    other = archive.write_batch([eth])
    assert first.created and not duplicate.created and other.created
    assert "instrument=BTCUSDT" in first.path
    assert "instrument=ETHUSDT" in other.path
    assert first.content_hash == duplicate.content_hash
    before = hashlib.sha256(Path(first.path).read_bytes()).hexdigest()

    conflicting = record("btc-1", "BTCUSDT", "50001")
    try:
        archive.write_batch([conflicting])
    except ValueError as exc:
        assert "conflicting logical identity" in str(exc)
    else:
        raise AssertionError("conflicting logical identity was accepted")
    assert hashlib.sha256(Path(first.path).read_bytes()).hexdigest() == before

    conflicting_partition = record("btc-1", "ETHUSDT", "3000")
    try:
        archive.write_batch([conflicting_partition])
    except ValueError as exc:
        assert "conflicting logical identity" in str(exc)
    else:
        raise AssertionError("cross-partition logical identity was accepted")


def test_parquet_arrow_schema_and_canonical_json_round_trip(tmp_path: Path):
    archive = ParquetArchive(tmp_path / "archive")
    item = record("btc-1", "BTCUSDT", "50000")
    written = archive.write_batch([item])
    rows = archive.read_rows(written.path)
    assert rows[0]["record_id"] == "btc-1"
    assert rows[0]["payload"] == {"close": "50000", "symbol": "BTCUSDT"}
    assert rows[0]["dependency_ids"] == ("instrument-metadata",)
    assert isinstance(rows[0]["received_at_ns"], int)
    assert (
        json.dumps(rows[0]["payload"], sort_keys=True, separators=(",", ":")) == '{"close":"50000","symbol":"BTCUSDT"}'
    )
    assert rows[0]["record_fingerprint"] == item.record_fingerprint


def test_archive_rejects_causal_identity_conflicts_not_just_content_conflicts(tmp_path: Path):
    archive = ParquetArchive(tmp_path / "archive")
    item = record("causal-1", "BTCUSDT", "50000")
    written = archive.write_batch([item])
    before = hashlib.sha256(Path(written.path).read_bytes()).hexdigest()
    candidates = (
        replace(
            item,
            received_at_ns=T0 + 2_000,
            processed_at_ns=T0 + 3_000,
            available_at_ns=T0 + 3_000,
            data_ingested_at_ns=T0 + 2_000,
            recorded_at_ns=T0 + 3_000,
        ),
        replace(item, available_at_ns=T0 + 4_000),
        replace(
            item,
            availability_class=AvailabilityClass.RECONSTRUCTED_PUBLIC,
            availability_lower_ns=T0 + 500,
            availability_upper_ns=T0 + 500,
            replay_available_at_ns=T0 + 500,
            availability_method="rule@v1:hash",
        ),
        replace(item, dependency_ids=("other-dependency",)),
        replace(item, evidence_ref="different-evidence"),
        replace(item, pipeline_version="phase3-v2"),
    )
    for candidate in candidates:
        try:
            archive.write_batch([candidate])
        except ValueError as exc:
            assert "conflicting logical identity" in str(exc)
        else:
            raise AssertionError("causal identity conflict was accepted")
    assert hashlib.sha256(Path(written.path).read_bytes()).hexdigest() == before


def test_duckdb_reads_only_archive_and_does_not_mutate_sqlite(tmp_path: Path):
    archive = ParquetArchive(tmp_path / "archive")
    archive.write_batch([record("btc-1", "BTCUSDT", "50000")])
    archive.write_batch([record("eth-1", "ETHUSDT", "3000")])
    glob = str(tmp_path / "archive" / "data_kind=*" / "venue=BYBIT" / "instrument=*" / "date=*" / "part-*.parquet")
    catalog = DuckDBResearchCatalog()
    btc = catalog.query_parquet(glob, "instrument = 'BTCUSDT' AND data_kind = 'bar_1m_last'")
    eth = catalog.query_parquet(glob, "instrument = 'ETHUSDT' AND data_kind = 'bar_1m_last'")
    assert len(btc) == len(eth) == 1

    journal = SQLiteJournal(tmp_path / "authoritative.db")
    before = journal.count("schema_metadata")
    catalog.query_parquet(glob, "data_kind = 'bar_1m_last'")
    assert journal.count("schema_metadata") == before
    journal.close()
