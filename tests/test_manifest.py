from pathlib import Path

import yaml


def test_manifest_remains_unverified_and_assisted_disabled():
    d = yaml.safe_load(Path("docs/capability/bybit-v1.yaml").read_text())
    assert d["assisted_enabled"] is False
    assert set(d["capabilities"].values()) == {"UNVERIFIED"}
    assert d["venue"]["environment"] == "testnet"
