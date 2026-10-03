"""Collector restart reads bounded stream heads and preserves durable identity."""

from __future__ import annotations

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.collector import PublicCollectorV2
from atlas.v2.instruments import InstrumentRegistryV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository


def _cursor(sequence: int, at: int, *, source: str = "PUBLIC", channel: str = "trades") -> ArtifactIndexEntryV2:
    metadata = {"source_id": source, "channel": channel, "high_water_sequence": sequence,
                "checkpoint_at_ns": at,
                "recent_payload_hashes": {sha256_json({"record": sequence}): sha256_json({"payload": sequence})}}
    return ArtifactIndexEntryV2(
        sha256_json({"artifact_type": "PublicCollectorCursorV2", "metadata": metadata}),
        "PublicCollectorCursorV2", sha256_json(metadata), at, at, metadata,
    )


def _restore(repository: OpsRepository) -> PublicCollectorV2:
    return PublicCollectorV2(repository=repository, registry=InstrumentRegistryV2(), clock_ns=lambda: 10_000)


def test_many_historical_checkpoints_restore_one_exact_head_without_history_scan(tmp_path, monkeypatch) -> None:
    path = tmp_path / "ops.sqlite"
    entries = tuple(_cursor(sequence, sequence + 1) for sequence in range(1000))
    with OpsRepository(path) as repository:
        repository.register_artifacts(entries)
    with OpsRepository(path) as repository:
        monkeypatch.setattr(repository, "artifact_entries", lambda *_args: pytest.fail("unbounded history scan"))
        restored = _restore(repository)
        assert restored._last_sequence == {("PUBLIC", "trades"): 999}
        assert restored._cursor_hashes[("PUBLIC", "trades")] == dict(entries[-1].metadata["recent_payload_hashes"])


def test_later_lower_high_water_does_not_regress_restored_sequence_or_hashes(tmp_path) -> None:
    highest = _cursor(50, 100)
    later_lower = _cursor(20, 200)
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        repository.register_artifacts((highest, later_lower))
        restored = _restore(repository)
        assert restored._last_sequence == {("PUBLIC", "trades"): 50}
        assert restored._cursor_hashes[("PUBLIC", "trades")] == dict(highest.metadata["recent_payload_hashes"])


def test_corrupt_selected_cursor_fails_closed(tmp_path) -> None:
    entry = _cursor(50, 100)
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        repository.register_artifact(entry)
        repository._connection.execute("UPDATE artifact_index SET content_hash=? WHERE artifact_ref=?",
                                       ("f" * 64, entry.artifact_ref))
        with pytest.raises(ValueError, match="cursor"):
            _restore(repository)


def test_stream_cardinality_overflow_refuses_recovery_without_truncation(tmp_path) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        repository.register_artifacts(tuple(_cursor(1, 100, channel=f"stream-{index}") for index in range(129)))
        with pytest.raises(ValueError):
            _restore(repository)
