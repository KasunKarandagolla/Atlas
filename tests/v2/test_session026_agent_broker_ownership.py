"""Structural/runtime architecture assertions for the Session-026 single writer."""

from __future__ import annotations

import ast
import inspect
import sqlite3
import sys
from pathlib import Path
from typing import Any

import pytest

import atlas.v2.agent_intelligence.broker as broker_module
import atlas.v2.agent_intelligence.provider as provider_module
from atlas.v2.agent_intelligence.persistence import AGENT_NAMESPACE_WRITER_OWNER

ROOT = Path(__file__).resolve().parents[2]


def test_broker_runtime_module_has_no_writable_persistence_dependency() -> None:
    source_path = ROOT / "src/atlas/v2/agent_intelligence/broker.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    imported: set[str] = set()
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.Name):
            names.add(node.id)
    assert not any(name == "sqlite3" or name.endswith(".persistence") for name in imported)
    assert not {"OpsRepository", "AgentJobRepository"}.intersection(names)
    assert "--ops-db" not in source_path.read_text(encoding="utf-8")
    assert "ATLAS_OPS_DB" not in source_path.read_text(encoding="utf-8")
    assert "authorize_dispatch" not in inspect.signature(broker_module.InferenceBroker).parameters
    assert "authorize_dispatch" not in inspect.signature(broker_module.InferenceBroker.__init__).parameters


def test_agent_namespace_writer_owner_is_controller_only() -> None:
    assert AGENT_NAMESPACE_WRITER_OWNER == "atlas-ops/controller"
    persistence_consumers = []
    for source_path in (ROOT / "src/atlas").rglob("*.py"):
        if source_path.name != "persistence.py" and "AgentJobRepository" in source_path.read_text(encoding="utf-8"):
            persistence_consumers.append(source_path.relative_to(ROOT).as_posix())
    assert persistence_consumers == ["src/atlas/v2/agent_intelligence/controller.py"]


def test_broker_entry_runs_without_ops_database_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    created: dict[str, Any] = {}

    class FakeProvider:
        def __init__(self, api_key: str, *, model_id: str) -> None:
            created["provider_config"] = (api_key, model_id)

        def propose(self, *_args: Any) -> None:
            raise AssertionError("broker entry test does not dispatch inference")

    class FakeServer:
        def __init__(self, socket_path: str, broker: Any) -> None:
            created["socket_path"] = socket_path
            created["broker"] = broker

        def start(self) -> None:
            raise KeyboardInterrupt

        def close(self) -> None:
            created["closed"] = True

    def forbidden_sqlite_open(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("inference broker attempted to open SQLite")

    monkeypatch.setattr(provider_module, "PydanticAIResearchProposalProvider", FakeProvider)
    monkeypatch.setattr(broker_module, "InferenceBrokerServer", FakeServer)
    monkeypatch.setattr(sqlite3, "connect", forbidden_sqlite_open)
    monkeypatch.delenv("ATLAS_OPS_DB", raising=False)
    monkeypatch.setenv("ATLAS_AGENT_BROKER_SOCKET", str(tmp_path / "broker.sock"))
    monkeypatch.setenv("ATLAS_AGENT_CAPABILITY_KEY", "42" * 32)
    monkeypatch.setenv("OPENAI_API_KEY", "offline-test-placeholder-not-a-provider-key")
    monkeypatch.setattr(sys, "argv", ["atlas-agent-broker"])

    assert broker_module.broker_main() == 0
    assert created["provider_config"] == ("offline-test-placeholder-not-a-provider-key", "gpt-6-astra")
    assert created["closed"] is True
    assert not hasattr(created["broker"], "_jobs")
    assert not hasattr(created["broker"], "_authorize_dispatch")


def test_broker_cli_rejects_legacy_ops_db_argument(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["atlas-agent-broker", "--ops-db", "/tmp/ops.sqlite"])
    with pytest.raises(SystemExit) as error:
        broker_module.broker_main()
    assert error.value.code == 2


def test_worker_runtime_module_has_no_database_network_or_provider_secret_access() -> None:
    source_path = ROOT / "src/atlas/v2/agent_intelligence/worker.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    imported: set[str] = set()
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.Name):
            names.add(node.id)
    assert not any(name == "sqlite3" or name.endswith(".persistence") for name in imported)
    assert not {"OpsRepository", "AgentJobRepository", "OPENAI_API_KEY"}.intersection(names)
    assert "--share-net" not in source_path.read_text(encoding="utf-8")
