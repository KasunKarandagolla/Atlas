"""The owner demo host is explicit, scoped and never generates an order."""
from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from atlas.v2 import product


@pytest.fixture
def build(monkeypatch):
    monkeypatch.setattr(product, "build_identity", lambda: {
        "source_sha": "a" * 40, "version": "2.0.40.0", "runtime_lock_sha256": "b" * 64})


def native_run(tmp_path):
    return product.create_run(tmp_path, product.ResearchRunConfigV2(
        data_root=str(tmp_path.resolve()), selected_execution_venue="BINANCE",
        execution_profile="DEMO_NATIVE_OMS_QUALIFICATION", execution_instrument_symbol="SOLUSDT",
        account_scope_ref="ref_owner", credential_ref="ref_credential", capability_profile_ref="ref_capability"))


def test_explicit_demo_host_stops_and_joins_before_releasing_account_lease(tmp_path, build):
    run = native_run(tmp_path)
    calls = []
    completed = asyncio.Event()

    async def native_start():
        calls.append("native_start")
        await completed.wait()

    async def native_stop():
        calls.append("native_stop")
        completed.set()

    host = SimpleNamespace(node=SimpleNamespace(run=native_start, stop=native_stop,
        last_failure_code=None, queue_overflow=False), open=lambda: calls.append("open"),
        close=lambda: calls.append("close"), status=lambda: {
            "capital_enabled": False, "assisted_enabled": False, "opening_gate": "TEST GATE"})
    assert product.run_selected_demo_component(run, host_factory=lambda _: host,
                                               stop_requested=lambda: True) == 0
    assert calls == ["open", "native_start", "native_stop", "close"]
    state = json.loads((run / "demo-runtime-status.json").read_text())
    assert state["reason"] == "SELECTED_DEMO_STOPPED"
    assert state["capital_enabled"] is state["assisted_enabled"] is False
    assert not (run / "ops.sqlite").exists()


def test_demo_host_requires_explicit_immutable_profile_before_construction(tmp_path, build):
    run = product.create_run(tmp_path, product.ResearchRunConfigV2(data_root=str(tmp_path.resolve())))
    factory = Mock()
    with pytest.raises(ValueError, match="select native demo"):
        product.run_selected_demo_component(run, host_factory=factory)
    factory.assert_not_called()


def test_demo_launcher_does_not_pass_ambient_secrets_or_switch_venues(tmp_path, build, monkeypatch):
    run = native_run(tmp_path)
    monkeypatch.setenv("BINANCE_SECRET", "fixture-never-forwarded")
    monkeypatch.setenv("BYBIT_API_KEY", "fixture-never-forwarded")
    process = Mock()
    launch = Mock(return_value=process)
    monkeypatch.setattr(product.subprocess, "Popen", launch)
    assert product.launch_selected_demo(run) is process
    args, options = launch.call_args
    assert args[0][args[0].index("--component") + 1] == "demo-oms"
    assert args[0][-1] == str(run)
    assert "BINANCE_SECRET" not in options["env"] and "BYBIT_API_KEY" not in options["env"]
    assert options["stdout"] == options["stderr"] == product.subprocess.DEVNULL
