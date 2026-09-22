from __future__ import annotations

import json
import sqlite3

import pytest

from atlas.domain.enums import ReconciliationHealth
from atlas.persistence.migrations import LEGACY_RUNTIME_INSTANCE_ID, bootstrap, current_version
from atlas.persistence.sqlite import SQLiteJournal
from atlas.runtime.recovery import RecoveryCertificate, RecoveryDecision

T0 = 1_800_000_000_000_000_000


def _create_v4_database(path, *, complete: bool = True) -> None:
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE schema_metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    connection.execute("INSERT INTO schema_metadata VALUES('schema_version', '4')")
    connection.execute(
        """
        CREATE TABLE recovery_certificates(
            recovery_run_id TEXT PRIMARY KEY,
            writer_id TEXT NOT NULL,
            writer_epoch INTEGER NOT NULL,
            journal_schema_version INTEGER NOT NULL,
            unresolved_intents_json TEXT NOT NULL,
            unresolved_commands_json TEXT NOT NULL,
            unknown_commands_json TEXT NOT NULL,
            reconciliation_health TEXT NOT NULL,
            protection_uncertainty_summary TEXT NOT NULL,
            started_at_ns INTEGER NOT NULL,
            ended_at_ns INTEGER NOT NULL,
            evidence_refs_json TEXT NOT NULL,
            venue_observations_obtained INTEGER NOT NULL,
            venue_evidence_refs_json TEXT NOT NULL,
            decision TEXT NOT NULL,
            protection_certified_flat INTEGER,
            protection_current INTEGER,
            protection_evidence_refs_json TEXT
        )
        """
    )
    if complete:
        connection.execute(
            """
            INSERT INTO recovery_certificates VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "legacy-recovery",
                "writer-v4",
                7,
                4,
                json.dumps(["intent-old"]),
                json.dumps(["command-old"]),
                json.dumps(["command-old"]),
                "STALE",
                "legacy uncertainty",
                T0,
                T0 + 10,
                json.dumps(["legacy-query-hash"]),
                1,
                json.dumps(["legacy-venue-evidence"]),
                "REMAIN_RECOVERING",
                0,
                1,
                json.dumps(["legacy-protection-query"]),
            ),
        )
    connection.commit()
    connection.close()


def test_v4_recovery_certificates_migrate_and_remain_usable_after_restart(tmp_path):
    path = tmp_path / "v4.db"
    _create_v4_database(path)

    journal = SQLiteJournal(path)
    assert journal.schema_version() == 5
    historical = journal.load_recovery_certificate("legacy-recovery")
    assert historical is not None
    assert historical.runtime_instance_id == LEGACY_RUNTIME_INSTANCE_ID
    assert historical.compatibility_metadata["legacy_schema"] == 4
    assert historical.compatibility_metadata["historical_evidence_authoritative"] is False
    assert historical.compatibility_metadata["legacy_v4"]["writer_id"] == "writer-v4"
    assert historical.protection_evidence is not None
    assert historical.protection_evidence.current_protection is True

    appended = RecoveryCertificate(
        recovery_run_id="new-v5-recovery",
        runtime_instance_id="runtime-v5",
        writer_id="writer-v5",
        writer_epoch=8,
        journal_schema_version=5,
        unresolved_intents=(),
        unresolved_commands=(),
        unknown_commands=(),
        reconciliation_health=ReconciliationHealth.STALE,
        started_at_ns=T0 + 20,
        ended_at_ns=T0 + 21,
        evidence_refs=("v5-evidence",),
        venue_evidence_refs=("v5-venue",),
        decision=RecoveryDecision.REMAIN_RECOVERING,
    )
    journal.append_recovery_certificate(appended)
    assert journal.load_recovery_certificate("new-v5-recovery") == appended
    journal.close()

    reopened = SQLiteJournal(path)
    assert reopened.load_recovery_certificate("legacy-recovery").runtime_instance_id == LEGACY_RUNTIME_INSTANCE_ID
    assert reopened.load_recovery_certificate("new-v5-recovery") == appended
    reopened.close()


def test_v4_migration_rolls_back_before_schema_metadata_update(tmp_path):
    path = tmp_path / "broken-v4.db"
    _create_v4_database(path, complete=False)
    connection = sqlite3.connect(path)
    connection.execute("ALTER TABLE recovery_certificates RENAME TO recovery_certificates_v4_bad")
    connection.execute("CREATE TABLE recovery_certificates(recovery_run_id TEXT PRIMARY KEY, writer_id TEXT NOT NULL)")
    connection.commit()
    with pytest.raises(RuntimeError, match="missing columns"):
        bootstrap(connection)
    assert current_version(connection) == 4
    assert (
        connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='recovery_certificates'"
        ).fetchone()
        is not None
    )
    connection.close()
