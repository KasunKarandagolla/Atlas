from __future__ import annotations

import pytest

from atlas.domain.capability import initial_unverified_fixture
from atlas.runtime.capability_ledger import CapabilityEvidenceLedger
from atlas.runtime.connectivity import PrivateVerification, PublicVenueHealth
from atlas.runtime.prerequisites import IdentityExpectation, ObservedAccountState
from atlas.runtime.safe_runtime import SafeRuntime, SafeRuntimeConfig


def cfg(tmp_path):
    c = initial_unverified_fixture()
    return SafeRuntimeConfig(
        str(tmp_path / "j.db"),
        str(tmp_path / "w.lock"),
        str(tmp_path / "status.json"),
        c.contract_hash(),
        False,
        False,
        IdentityExpectation(),
        ObservedAccountState(
            "testnet", "BYBIT", "linear", "one_way", "isolated", "REQUIRED", ("BTCUSDT", "ETHUSDT"), False
        ),
        PublicVenueHealth(),
        PrivateVerification(),
        5_000_000_000,
        capability_contract=c,
    )


def test_runtime_identity_stable_recovery_cycle_identity_distinct(tmp_path):
    r = SafeRuntime(cfg(tmp_path))
    first = r.start()
    journal = r.journal
    rid = r.runtime_instance_id
    second = r.tick()
    assert r.runtime_instance_id == rid
    assert first.certificate.runtime_instance_id == second.certificate.runtime_instance_id == rid
    assert first.certificate.recovery_run_id != second.certificate.recovery_run_id
    assert r.journal is journal and journal.is_open and r.writer_held
    assert not first.new_risk_allowed and not second.new_risk_allowed
    r.shutdown()
    assert r.journal is None and not r.writer_held


def test_capability_evidence_requires_real_refs_and_profile_hash():
    ledger = CapabilityEvidenceLedger()
    name = ledger.REQUIRED_CAPABILITIES[0]
    with pytest.raises(ValueError):
        ledger.record_offline_test(name, "run", (), 1, True)
    with pytest.raises(ValueError):
        ledger.record_testnet_gate(name, "run", ("evidence",), 1, True, target_profile_hash="bad")
    assert ledger.all_unverified()
