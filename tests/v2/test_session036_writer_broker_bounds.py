"""Single-writer lifetime and bounded local broker fault boundaries."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, cast

import pytest

from atlas.v2.agent_intelligence.broker import (
    MAX_BROKER_CONNECTIONS,
    InferenceBroker,
    InferenceBrokerServer,
    _read_frame,
    _write_frame,
)
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.memory.writer_lock import OpsWriterAlreadyActive, OpsWriterLock


def _child(path: Path, *, crash: bool = False) -> subprocess.CompletedProcess[str]:
    source_root = str(Path(__file__).resolve().parents[2] / "src")
    program = """
import os, sys
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.memory.writer_lock import OpsWriterAlreadyActive
try:
    repository = OpsRepository(sys.argv[1])
except OpsWriterAlreadyActive:
    raise SystemExit(23)
if sys.argv[2] == 'crash':
    os._exit(7)
repository.close()
"""
    return subprocess.run([sys.executable, "-c", program, str(path), "crash" if crash else "close"],
        env={**os.environ, "PYTHONPATH": source_root}, capture_output=True, text=True, timeout=10, check=False)


def test_repository_excludes_another_process_and_reopens_after_release_and_crash(tmp_path: Path) -> None:
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path):
        with pytest.raises(OpsWriterAlreadyActive):
            OpsRepository(path)
        with OpsRepository(path, read_only=True):
            assert _child(path).returncode == 23
    assert _child(path).returncode == 0
    assert _child(path, crash=True).returncode == 7
    with OpsRepository(path):
        assert _child(path).returncode == 23
    assert path.with_name(path.name + ".writer.lock").exists()


def test_initialization_failure_releases_writer_lease(tmp_path: Path) -> None:
    path = tmp_path / "ops.sqlite"
    path.write_bytes(b"not a database")
    with pytest.raises(Exception, match="not a database"):
        OpsRepository(path)
    path.unlink()
    with OpsRepository(path):
        assert _child(path).returncode == 23


def test_writer_rejects_network_paths_and_hard_link_database_aliases(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="network share"):
        OpsWriterLock(r"\\server\share\ops.sqlite")
    path = tmp_path / "ops.sqlite"
    alias = tmp_path / "alias.sqlite"
    with OpsRepository(path):
        pass
    try:
        os.link(path, alias)
    except OSError:
        pytest.skip("filesystem does not support hard links")
    with pytest.raises(ValueError, match="hard-link aliases"):
        OpsRepository(alias)
    with pytest.raises(ValueError, match="hard-link aliases"):
        OpsRepository(path)


def test_passive_wal_checkpoint_is_writer_only_and_preserves_reader_snapshot(tmp_path: Path) -> None:
    from atlas.v2.memory.repository import ArtifactIndexEntryV2

    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as writer, OpsRepository(path, read_only=True) as reader:
        with pytest.raises(RuntimeError, match="read-only"):
            reader.checkpoint()
        with writer._transaction(), pytest.raises(RuntimeError, match="active transaction"):
            writer.checkpoint()
        with reader.read_snapshot():
            assert reader.get_artifact("a" * 64) is None
            writer.register_artifact(ArtifactIndexEntryV2("a" * 64, "Fixture", "a" * 64, 1, 1, {}))
            busy, frames, checkpointed = writer.checkpoint()
            assert busy in {0, 1} and frames >= checkpointed >= 0
            assert reader.get_artifact("a" * 64) is None
        assert writer.checkpoint()[0] == 0
        assert reader.get_artifact("a" * 64) is not None


def _wait_until(predicate: Any) -> None:
    deadline = time.monotonic() + 3
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError("broker did not reach the expected connection state")
        time.sleep(0.01)


def test_broker_saturation_is_explicit_and_handlers_release_on_disconnect(tmp_path: Path) -> None:
    if not hasattr(socket, "AF_UNIX"):
        pytest.skip("Unix broker transport is not present on this platform")
    endpoint = tmp_path / "bounded.sock"
    server = InferenceBrokerServer(endpoint, cast(InferenceBroker, object()))
    clients: list[socket.socket] = []
    server.start()
    try:
        duplicate = InferenceBrokerServer(endpoint, cast(InferenceBroker, object()))
        with pytest.raises(RuntimeError, match="already exists"):
            duplicate.start()
        duplicate.close()
        assert endpoint.exists()
        for _ in range(MAX_BROKER_CONNECTIONS):
            connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            connection.settimeout(2)
            connection.connect(str(endpoint))
            clients.append(connection)
        _wait_until(lambda: len(server._connections) == MAX_BROKER_CONNECTIONS)
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as excess:
            excess.settimeout(2)
            excess.connect(str(endpoint))
            assert json.loads(_read_frame(excess)) == {
                "protocol_version": 1, "request_id": None, "ok": False, "error": "BROKER_SATURATED"}
        assert len(server._connections) == MAX_BROKER_CONNECTIONS
        clients.pop().close()
        _wait_until(lambda: len(server._connections) == MAX_BROKER_CONNECTIONS - 1)
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as replacement:
            replacement.settimeout(2)
            replacement.connect(str(endpoint))
            _write_frame(replacement, {"command": "unsupported"})
            assert json.loads(_read_frame(replacement))["error"] == "BROKER_OPERATION_UNSUPPORTED"
    finally:
        server.close()
        for connection in clients:
            connection.close()
    _wait_until(lambda: not server._connections)


def test_broker_closed_connection_before_handler_start_releases_slot(tmp_path: Path) -> None:
    server = InferenceBrokerServer(tmp_path / "closed.sock", cast(InferenceBroker, object()))
    connection = socket.socket()
    assert server._handler_slots.acquire(blocking=False)
    server._connections.add(connection)
    connection.close()
    server._handle(connection)
    assert not server._connections
    # Every slot must be available after the startup/close race.
    for _ in range(MAX_BROKER_CONNECTIONS):
        assert server._handler_slots.acquire(blocking=False)
    assert not server._handler_slots.acquire(blocking=False)


def test_cursor_cache_eviction_keeps_durable_duplicate_and_conflict_detection(tmp_path: Path) -> None:
    from atlas.v2._serialization import canonical_json
    from atlas.v2.data.collector import MAX_RECENT_CURSOR_HASHES_V2, PublicCollectorV2
    from atlas.v2.data.history import ParquetObservationArchiveV2
    from atlas.v2.data.raw import AppendStatusV2
    from atlas.v2.instruments import InstrumentRegistryV2
    from tests.v2.test_data_runtime import product, raw

    contract = product()
    registry = InstrumentRegistryV2()
    registry.register(contract)
    path = tmp_path / "ops.sqlite"
    first = raw(sequence="0", event_at=10, received=100, payload={"id": 0})
    with OpsRepository(path) as repository:
        archive = ParquetObservationArchiveV2(tmp_path / "archive")
        collector = PublicCollectorV2(repository=repository, registry=registry, clock_ns=lambda: 1000,
            archive=archive)
        for sequence in range(MAX_RECENT_CURSOR_HASHES_V2 + 8):
            observation = raw(sequence=str(sequence), event_at=10, received=100, payload={"id": sequence})
            collector.ingest(observation, raw_payload=canonical_json({"id": sequence}),
                sequence_channel="trades", sequence_is_contiguous=True, retain_in_memory=False)
            assert len(collector._cursor_hashes[(observation.source_id, "trades")]) <= MAX_RECENT_CURSOR_HASHES_V2
        collector.flush_archive()
        collector.checkpoint_cursors(at_ns=1001)
    with OpsRepository(path) as repository:
        restored = PublicCollectorV2(repository=repository, registry=registry, clock_ns=lambda: 2000,
            archive=archive)
        assert len(restored._cursor_hashes[(first.source_id, "trades")]) == MAX_RECENT_CURSOR_HASHES_V2
        assert restored.ingest(first, raw_payload=canonical_json({"id": 0}),
            retain_in_memory=False).append.status == AppendStatusV2.DUPLICATE
        conflicting = raw(sequence="0", event_at=10, received=100, payload={"id": "changed"})
        result = restored.ingest(conflicting, raw_payload=canonical_json({"id": "changed"}), retain_in_memory=False)
        assert result.append.status == AppendStatusV2.CONFLICT_QUARANTINED
        assert result.persistent_conflict
