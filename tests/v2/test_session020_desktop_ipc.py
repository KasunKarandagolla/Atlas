"""Session-020 projection contract, loopback IPC, and desktop process-safety checks."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import socket
import struct
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

from atlas import __version__ as ATLAS_VERSION
from atlas.desktop.models import current_freshness, scanner_display
from atlas.v2._serialization import sha256_json
from atlas.v2.desktop.ipc import (
    MAX_IPC_MESSAGE_BYTES,
    READ_ONLY_COMMANDS,
    ProjectionClient,
    ProjectionService,
    load_or_create_token,
    read_token,
)
from atlas.v2.desktop.projection import DesktopScannerRowV2, _entry_summary, project_chart_series, project_snapshot
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository

from .test_session014_core import KEY


def _receive_exact(sock: socket.socket, count: int) -> bytes:
    data = bytearray()
    while len(data) < count:
        part = sock.recv(count - len(data))
        if not part:
            raise RuntimeError("unexpected IPC EOF in test")
        data.extend(part)
    return bytes(data)


def _raw_request(host: str, port: int, payload: bytes, *, announced_size: int | None = None) -> dict:
    with socket.create_connection((host, port), timeout=2) as sock:
        sock.settimeout(2)
        sock.sendall(struct.pack("!I", len(payload) if announced_size is None else announced_size) + payload)
        size = struct.unpack("!I", _receive_exact(sock, 4))[0]
        return json.loads(_receive_exact(sock, size))


def _request(token: str, kind: str, *, version: int = 2) -> bytes:
    return json.dumps({
        "protocol_version": version,
        "request_id": str(uuid.uuid4()),
        "request_type": kind,
        "session_token": token,
        "params": {},
    }, separators=(",", ":")).encode()


def _initialized_db(path: Path) -> None:
    with OpsRepository(path):
        pass


def test_projection_marks_stale_and_missing_chart_explicitly(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        snapshot = project_snapshot(repo, now_ns=1_900_000_000_000_000_000)
        assert snapshot.to_canonical_json() == project_snapshot(
            repo, now_ns=1_900_000_000_000_000_000
        ).to_canonical_json()
        assert current_freshness(snapshot, now_ns=snapshot.valid_until_ns + 1) == "STALE"
        chart = project_chart_series(repo, key_json=KEY.to_canonical_json(), interval="1H",
            information_cutoff_ns=snapshot.generated_at_ns, archive_root=None)
        assert chart.state == "UNAVAILABLE"
        assert chart.reason_code == "ARCHIVE_NOT_CONFIGURED"
        states = {item.state for item in snapshot.overview.statuses}
        assert {"NOT_ESTIMABLE", "UNVERIFIED", "TEST GATE", "BLOCKED BY ENVIRONMENT"}.issubset(states)
        decision_states = ("NO_TRADE", "NOT_ESTIMABLE", "UNVERIFIED", "TEST GATE", "BLOCKED BY ENVIRONMENT")
        for decision_state in decision_states:
            row = DesktopScannerRowV2("{}", "BYBIT", "TESTNET", "PERP", "BTCUSDT", "r1",
                "SCANNER_ELIGIBLE", "OBSERVED", "S1", None, 1, "SELECTED", "UNAVAILABLE", None,
                "UNAVAILABLE", None, decision_state, (), None, False, False)
            assert scanner_display(row)[10] == decision_state
        assert scanner_display(row)[4] == "OBSERVED"
        assert snapshot.to_dict()["schema_version"] == 2
        assert snapshot.to_dict()["projection_version"] == "ATLAS_DESKTOP_PROJECTION_V2"


def test_matured_executable_and_diagnostic_targets_remain_distinct():
    summaries = []
    for target in ("EXECUTABLE_ACTION_VALUE", "NON_EXECUTABLE_DIAGNOSTIC"):
        body = {"version": "MATURED_OUTCOME_V2_V3", "outcome_target": target,
                "label_state": "MATURED", "provenance": "SIMULATED"}
        ref = sha256_json(body)
        entry = ArtifactIndexEntryV2(ref, "MaturedOutcomeV2", ref, 1, 2, {"outcome": body})
        summaries.append(_entry_summary(entry, synthetic_refs=frozenset()))
    assert [row.label_target for row in summaries] == ["EXECUTABLE_ACTION_VALUE", "NON_EXECUTABLE_DIAGNOSTIC"]
    assert summaries[0].status == summaries[1].status == "MATURED"
    assert summaries[0].to_dict() != summaries[1].to_dict()


def test_protocol_rejects_bad_large_unknown_and_future_requests(tmp_path):
    db = tmp_path / "ops.sqlite"
    _initialized_db(db)
    token = secrets.token_urlsafe(48)
    with ProjectionService(db, token) as service:
        service.start()
        host, port = service.address
        assert host == "127.0.0.1"
        malformed = _raw_request(host, port, b"{")
        assert malformed["error"]["code"] == "MALFORMED_JSON"
        oversized = _raw_request(host, port, b"", announced_size=MAX_IPC_MESSAGE_BYTES + 1)
        assert oversized["error"]["code"] == "MESSAGE_TOO_LARGE"
        wrong_version = _raw_request(host, port, _request(token, "ping", version=3))
        assert wrong_version["error"]["code"] == "UNSUPPORTED_PROTOCOL_VERSION"
        unknown = _raw_request(host, port, _request(token, "modify_risk_policy"))
        assert unknown["error"]["code"] == "UNKNOWN_REQUEST"
        client = ProjectionClient(host, port, token)
        ping = client.request("ping")
        assert ping["state"] == "READY"
        assert ping["service"] == "atlas-v2-projection"
        assert ping["database_mode"] == "READ_ONLY"
        assert ping["loopback_only"] is True
        assert ping["release_classification"] == "SHADOW_RELEASED"
        assert ping["capital_enabled"] is False
        assert type(ping["uptime_seconds"]) is int
        assert ping["requests_served"] >= 1
        health = client.request("health")
        diagnostics = health["diagnostics"]
        assert diagnostics["process"] == "atlas-v2-projection"
        assert diagnostics["version"] == ATLAS_VERSION
        assert diagnostics["ipc_protocol_version"] == 2
        assert diagnostics["release_classification"] == "SHADOW_RELEASED"
        assert diagnostics["economics_status"] == "NOT ESTIMABLE"
        assert diagnostics["soak_status"] == "BLOCKED BY ENVIRONMENT"
        assert diagnostics["capital_enabled"] is False
        assert diagnostics["requests_served"] >= ping["requests_served"]
        status_names = {item["name"] for item in health["statuses"]}
        assert {"data_lag", "queue_depth", "model_worker", "capability", "recovery", "new_risk_allowed"} <= status_names
        assert frozenset(
            {"ping", "health", "snapshot", "overview", "scanner", "watches", "evidence", "chart"}
        ) == READ_ONLY_COMMANDS
        with pytest.raises(ValueError):
            ProjectionClient("0.0.0.0", port, token)
        with pytest.raises(ValueError):
            ProjectionService(db, token, host="0.0.0.0")
        with pytest.raises(ValueError):
            ProjectionClient(host, port, token).request("place_order")


def test_ipc_token_file_is_private_and_regular(tmp_path):
    path = tmp_path / "ipc.token"
    token = load_or_create_token(path)
    assert read_token(path) == token
    if os.name != "nt":
        assert path.stat().st_mode & 0o077 == 0
        path.chmod(0o644)
        with pytest.raises(ValueError, match="permissions"):
            read_token(path)
        path.unlink()
        path.symlink_to(tmp_path / "elsewhere")
        with pytest.raises(ValueError, match="regular file"):
            read_token(path)


def test_desktop_crash_disconnect_reconnect_and_evidence_immutability(tmp_path):
    db = tmp_path / "ops.sqlite"
    _initialized_db(db)
    token = secrets.token_urlsafe(48)
    initial_db_hash = hashlib.sha256(db.read_bytes()).hexdigest()
    service = ProjectionService(db, token)
    service.start()
    host, port = service.address
    child_code = """
import os
from PySide6.QtWidgets import QApplication
from atlas.desktop.app import AtlasDesktop
from atlas.v2.desktop.ipc import ProjectionClient
app = QApplication([])
window = AtlasDesktop(ProjectionClient(os.environ['ATLAS_TEST_HOST'], int(os.environ['ATLAS_TEST_PORT']), os.environ['ATLAS_TEST_TOKEN']))
window.refresh()
names = [window.tabs.tabText(i) for i in range(window.tabs.count())]
assert names == ['Overview', 'Scanner', 'Chart', 'Watches', 'Evidence'], names
os.write(1, b'READ_ONLY_VIEWS_READY\\n')
os._exit(37)
"""
    environment = os.environ.copy()
    environment.update({
        "ATLAS_TEST_HOST": host,
        "ATLAS_TEST_PORT": str(port),
        "ATLAS_TEST_TOKEN": token,
        "QT_QPA_PLATFORM": "offscreen",
        "PYTHONPATH": os.pathsep.join((str(Path.cwd() / "src"), environment.get("PYTHONPATH", ""))),
    })
    process = subprocess.Popen([sys.executable, "-c", child_code], env=environment,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        stdout, stderr = process.communicate(timeout=30)
        assert b"READ_ONLY_VIEWS_READY" in stdout, (
            "desktop did not reach its view-ready marker: " + stderr.decode("utf-8", "replace")[-1000:]
        )
        assert process.returncode == 37
        assert service.is_alive
        client = ProjectionClient(host, port, token)
        snapshot = client.request("snapshot")
        assert snapshot["projection_version"] == "ATLAS_DESKTOP_PROJECTION_V2"
        assert token not in json.dumps(snapshot)
        assert "SHADOW_FAKE_ACCOUNT" not in json.dumps(snapshot)
        assert hashlib.sha256(db.read_bytes()).hexdigest() == initial_db_hash
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        if process.stdout is not None:
            process.stdout.close()
        if process.stderr is not None:
            process.stderr.close()
        service.close()
    # A fresh client reconnects to the still-running service between desktop sessions.
    with ProjectionService(db, token) as reconnected:
        reconnected.start()
        assert ProjectionClient(*reconnected.address, token).request("ping")["state"] == "READY"


def test_desktop_reports_clean_projection_unavailable_state(monkeypatch):
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    from atlas.desktop.app import AtlasDesktop

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        host, port = listener.getsockname()
    app = QApplication.instance() or QApplication([])
    window = AtlasDesktop(ProjectionClient(host, port, secrets.token_urlsafe(48)))
    try:
        assert window.banner.text() == "Projection service unavailable · SERVICE_UNAVAILABLE · reconnecting"
    finally:
        window.timer.stop()
        window.close()
        app.processEvents()
