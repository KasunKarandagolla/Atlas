from __future__ import annotations

from atlas.domain.capability import initial_unverified_fixture
from atlas.runtime.connectivity import PrivateVerification, PublicState, PublicVenueHealth
from atlas.runtime.prerequisites import IdentityExpectation, ObservedAccountState
from atlas.runtime.safe_runtime import SafeRuntime, SafeRuntimeConfig


def _config(tmp_path):
    contract = initial_unverified_fixture()
    return SafeRuntimeConfig(
        journal_path=str(tmp_path / "journal.db"),
        lock_path=str(tmp_path / "writer.lock"),
        status_path=str(tmp_path / "status.json"),
        capability_hash=contract.contract_hash(),
        all_qualified=False,
        assisted_enabled=False,
        capability_contract=contract,
        identity_expected=IdentityExpectation(expected_account_identity_hash="REQUIRED"),
        identity_observed=ObservedAccountState(
            environment="testnet",
            venue="BYBIT",
            product="linear",
            position_mode="one_way",
            margin_profile="isolated",
            account_identity_hash="REQUIRED",
            instruments_with_metadata=("BTCUSDT", "ETHUSDT"),
            private_verified=False,
        ),
        public_health=PublicVenueHealth(state=PublicState.DISCONNECTED),
        private_verification=PrivateVerification(),
        max_public_staleness_ns=5_000_000_000,
        tick_interval_ns=1,
    )


def test_runtime_retains_writer_and_exact_journal_until_shutdown(tmp_path):
    runtime = SafeRuntime(_config(tmp_path))
    first = runtime.start()
    journal = runtime.journal
    instance_id = first.certificate.runtime_instance_id
    assert journal is not None and journal.is_open
    assert runtime.writer_held
    second = runtime.tick()
    assert runtime.journal is journal
    assert journal.is_open
    assert runtime.writer_held
    assert second.certificate.runtime_instance_id == instance_id
    assert second.certificate.recovery_run_id != first.certificate.recovery_run_id
    assert second.new_risk_allowed is False
    assert second.certificate.ended_at_ns >= first.certificate.ended_at_ns
    runtime.shutdown()
    assert runtime.journal is None
    assert not runtime.writer_held
