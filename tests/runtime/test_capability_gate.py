from __future__ import annotations

from dataclasses import replace

from atlas.domain.capability import Capabilities, CapabilityStatus, initial_unverified_fixture
from atlas.runtime.coordinator import _check_capability_gate


def _qualified_contract():
    base = initial_unverified_fixture(
        account_identity_hash="a" * 64,
        account_generation_and_margin_mode="one-way-isolated-generation",
        installed_artifact_sha256="b" * 64,
        dependency_lock_sha256="c" * 64,
    )
    capabilities = Capabilities(
        **dict.fromkeys(base.capabilities.__dataclass_fields__, CapabilityStatus.SUPPORTED),  # type: ignore[arg-type]
    )
    return replace(base, capabilities=capabilities, assisted_enabled=True)


def test_integrated_gate_binds_hash_and_exact_contract():
    contract = _qualified_contract()
    allowed, reasons = _check_capability_gate(contract, contract.contract_hash(), True, True, 1)
    assert allowed
    assert reasons == []
    wrong, reasons = _check_capability_gate(contract, "0" * 64, True, True, 1)
    assert not wrong
    assert any("bind" in reason for reason in reasons)


def test_current_unverified_contract_can_never_pass_gate():
    contract = initial_unverified_fixture()
    allowed, reasons = _check_capability_gate(contract, contract.contract_hash(), True, True, 1)
    assert not allowed
    assert all(getattr(contract.capabilities, field).value == "UNVERIFIED" for field in contract.capabilities.__dataclass_fields__)
    assert any("SUPPORTED" in reason for reason in reasons)
