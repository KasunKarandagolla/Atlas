from __future__ import annotations

import ast
from pathlib import Path

import atlas.runtime.phase4_v2 as phase4_v2
from atlas.v2.desktop.ipc import READ_ONLY_COMMANDS
from atlas.v2.models.local_process import LocalProcessProvider


def test_v2_science_and_bridge_have_no_direct_order_transport():
    roots = (Path("src/atlas/v2/strategies"), Path("src/atlas/v2/science"))
    forbidden_calls = {"submit_order", "place_order", "cancel_order", "replace_order"}
    forbidden_imports = (
        "nautilus_trader.adapters",
        "atlas.runtime.nautilus_boundary",
        "atlas.runtime.assisted_control",
    )
    for root in roots:
        for path in root.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            imports = [node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
            imports.extend(
                alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names
            )
            assert not any(name and name.startswith(forbidden_imports) for name in imports), str(path)
            assert not any(
                isinstance(node, ast.Attribute) and node.attr in forbidden_calls for node in ast.walk(tree)
            ), str(path)

    assert not any(callable(value) and name in forbidden_calls for name, value in vars(phase4_v2).items())


def test_desktop_and_research_worker_have_no_order_or_credential_authority():
    assert (
        frozenset(
            {
                "ping",
                "health",
                "snapshot",
                "overview",
                "scanner",
                "watches",
                "evidence",
                "chart",
            }
        )
        == READ_ONLY_COMMANDS
    )
    supplied = {
        "PATH": "/usr/bin:/bin",
        "BYBIT_TESTNET_API_KEY": "FAKE-TEST-KEY",
        "BYBIT_TESTNET_API_SECRET": "FAKE-TEST-SECRET",
        "BINANCE_TESTNET_API_KEY": "FAKE-TEST-KEY",
        "BINANCE_TESTNET_API_SECRET": "FAKE-TEST-SECRET",
        "ATLAS_LIVE_CONTROL_DB": "/tmp/fake-live.sqlite",
    }
    child_environment = LocalProcessProvider.allowlisted_environment(supplied)
    assert set(child_environment) == {"PATH"}
    assert not any("API_KEY" in name or "API_SECRET" in name for name in child_environment)


def test_phase4_bridge_keeps_normal_orders_on_existing_nautilus_boundary():
    source = Path("src/atlas/runtime/phase4_v2.py").read_text(encoding="utf-8")
    assert "submit_order" not in source and "place_order" not in source
    assert "requests." not in source and "httpx." not in source
