"""Bybit capability manifest parser test: maps into CapabilityContract, stays UNVERIFIED."""

from __future__ import annotations

from pathlib import Path

import yaml

from atlas.domain.capability import capability_contract_from_manifest
from atlas.domain.enums import CapabilityStatus


def test_bybit_manifest_maps_and_stays_unverified():
    path = Path(__file__).resolve().parents[2] / "docs" / "capability" / "bybit-v1.yaml"
    assert path.exists(), f"manifest missing: {path}"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    contract = capability_contract_from_manifest(data)
    assert contract.runtime.version == "2.0.0rc5"
    assert contract.runtime.source_commit == "1b0a49d2792a9432a3aca3fcb617ce7a630d905e"
    assert set(contract.venue.supported_symbols) == {"BTCUSDT", "ETHUSDT"}
    assert contract.venue.product == "linear"
    assert contract.venue.position_mode == "one_way"
    assert contract.assisted_enabled is False
    # All exchange behaviors untested -> UNVERIFIED
    for name, status in contract.capabilities.to_dict().items():
        assert status == CapabilityStatus.UNVERIFIED.value, name
    assert contract.to_dict()["venue"]["symbols"] == ["BTCUSDT", "ETHUSDT"]
