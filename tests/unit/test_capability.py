"""CapabilityContract tests: status validation, assisted gate, manifest mapping."""

from __future__ import annotations

import pytest

from atlas.domain.capability import (
    REQUIRED_CAPABILITY_FIELDS,
    Capabilities,
    CapabilityContract,
    capability_contract_from_manifest,
    initial_unverified_fixture,
)
from atlas.domain.enums import CapabilityStatus


def test_initial_fixture_all_unverified_and_no_assisted():
    c = initial_unverified_fixture()
    for f in REQUIRED_CAPABILITY_FIELDS:
        assert getattr(c.capabilities, f) == CapabilityStatus.UNVERIFIED
    assert c.assisted_enabled is False
    # Unknown never implies supported (capability blockers + placeholder blockers)
    assert c.validate_for_assisted() != []
    assert len(c.validate_for_assisted()) >= len(REQUIRED_CAPABILITY_FIELDS)
    assert any("entry_ioc_with_attached_full_mark_market_stop" in r for r in c.validate_for_assisted())


def test_assisted_enabled_rejected_while_unverified():
    caps = Capabilities(**dict.fromkeys(REQUIRED_CAPABILITY_FIELDS, CapabilityStatus.UNVERIFIED))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="assisted_enabled"):
        from atlas.domain.capability import RuntimeInfo, VenueInfo

        CapabilityContract(
            contract_version="1.0",
            runtime=RuntimeInfo(
                distribution="nautilus_trader",
                version="2.0.0rc5",
                source_commit="abc",
                installed_artifact_sha256="h1",
                dependency_lock_sha256="h2",
                python_platform_abi="py",
            ),
            venue=VenueInfo(
                environment="testnet",
                account_identity_hash="h",
                account_generation_and_margin_mode="m",
                product="linear",
                position_mode="one_way",
                supported_symbols=("BTCUSDT",),
            ),
            capabilities=caps,
            assisted_enabled=True,
        )


def test_capability_rejects_bare_booleans():
    with pytest.raises(ValueError):
        Capabilities(
            entry_ioc_with_attached_full_mark_market_stop=True,  # type: ignore[arg-type]
            native_stop_visible_and_resizes_on_partial_fill=CapabilityStatus.SUPPORTED,
            reduce_only_wire_and_matching_enforcement=CapabilityStatus.SUPPORTED,
            ambiguous_submit_not_treated_as_definite_rejection=CapabilityStatus.SUPPORTED,
            external_native_stop_fill_reconciliation=CapabilityStatus.SUPPORTED,
            native_position_stop_read_and_repair_port=CapabilityStatus.SUPPORTED,
        )


def test_all_supported_allows_assisted_and_hash_deterministic():
    import dataclasses

    from atlas.domain.capability import RuntimeInfo, VenueInfo

    caps = Capabilities(**dict.fromkeys(REQUIRED_CAPABILITY_FIELDS, CapabilityStatus.SUPPORTED))  # type: ignore[arg-type]
    real = initial_unverified_fixture(
        installed_artifact_sha256="a" * 64,
        dependency_lock_sha256="b" * 64,
        python_platform_abi="cpython-312-x86_64-linux-gnu",
        account_identity_hash="acct-hash-001",
        account_generation_and_margin_mode="gen1-isolated",
    )
    c2 = dataclasses.replace(real, capabilities=caps, assisted_enabled=True)
    assert c2.validate_for_assisted() == []
    assert c2.contract_hash() == c2.contract_hash()
    # Any single FAILED/UNSUPPORTED blocks
    for f in REQUIRED_CAPABILITY_FIELDS:
        d = dict.fromkeys(REQUIRED_CAPABILITY_FIELDS, CapabilityStatus.SUPPORTED)
        d[f] = CapabilityStatus.FAILED
        bad = Capabilities(**d)  # type: ignore[arg-type]
        c3 = dataclasses.replace(real, capabilities=bad)
        assert any(f in r for r in c3.validate_for_assisted())
    # All SUPPORTED but placeholder identity still blocks (FIX7)
    c4 = dataclasses.replace(
        initial_unverified_fixture(), capabilities=caps, assisted_enabled=False
    )
    assert any("placeholder" in r for r in c4.validate_for_assisted())
    with pytest.raises(ValueError, match="placeholder"):
        dataclasses.replace(c4, assisted_enabled=True)

    _ = (RuntimeInfo, VenueInfo)


def test_manifest_missing_capability_fails():
    with pytest.raises((ValueError, KeyError)):
        capability_contract_from_manifest(
            {
                "runtime": {
                    "distribution": "nautilus_trader",
                    "version": "2.0.0rc5",
                    "source_commit": "x",
                    "installed_artifact_sha256": "a",
                    "dependency_lock_sha256": "b",
                    "python_platform_abi": "c",
                },
                "venue": {
                    "environment": "testnet",
                    "account_identity_hash": "h",
                    "account_generation_and_margin_mode": "m",
                    "product": "linear",
                    "position_mode": "one_way",
                    "symbols": ["BTCUSDT"],
                },
                "capabilities": {"entry_ioc_with_attached_full_mark_market_stop": "SUPPORTED"},
                "assisted_enabled": False,
            }
        )


def test_unknown_status_string_fails():
    with pytest.raises(ValueError, match="unknown status"):
        capability_contract_from_manifest(
            {
                "contract_version": "1.0",
                "runtime": {
                    "distribution": "nautilus_trader",
                    "version": "2.0.0rc5",
                    "source_commit": "x",
                    "installed_artifact_sha256": "a",
                    "dependency_lock_sha256": "b",
                    "python_platform_abi": "c",
                },
                "venue": {
                    "environment": "testnet",
                    "account_identity_hash": "h",
                    "account_generation_and_margin_mode": "m",
                    "product": "linear",
                    "position_mode": "one_way",
                    "symbols": ["BTCUSDT"],
                },
                "capabilities": dict.fromkeys(REQUIRED_CAPABILITY_FIELDS, "MAYBE"),
                "assisted_enabled": False,
            }
        )
