"""Writer ownership + runtime identity/health tests."""

from __future__ import annotations

import pytest

from atlas.domain.enums import (
    Environment,
    HealthState,
    ProtectionStatus,
    ReconciliationHealth,
)
from atlas.runtime.health import HealthSnapshot, new_risk_allowed
from atlas.runtime.identity import RuntimeIdentity
from atlas.runtime.writer_lock import WriterAlreadyActive, WriterLock


def test_duplicate_local_writer_denied(tmp_path):
    lock = tmp_path / "writer.lock"
    w1 = WriterLock(lock)
    o1 = w1.acquire()
    assert o1.writer_epoch >= 1
    w2 = WriterLock(lock)
    with pytest.raises(WriterAlreadyActive):
        w2.acquire()
    # Same-process re-acquire also denied
    with pytest.raises(WriterAlreadyActive):
        w1.acquire()
    w1.release()
    # After release, a new writer can acquire with monotonic epoch
    w3 = WriterLock(lock)
    o3 = w3.acquire()
    assert o3.writer_epoch >= o1.writer_epoch
    w3.release()


def test_writer_lock_docs_do_not_fence_remote():

    import atlas.runtime.writer_lock as mod

    assert "does NOT fence another" in mod.__doc__


def test_recovery_state_blocks_new_risk():
    snap = HealthSnapshot(
        state=HealthState.RECOVERING,
        writer_owned=True,
        account_matched=True,
        data_current=True,
        reconciliation=ReconciliationHealth.CURRENT,
        protection=ProtectionStatus.CONFIRMED,
        has_open_exposure=False,
        drawdown_stop_active=False,
    )
    d = new_risk_allowed(snap)
    assert not d.allowed


def test_stale_conflicted_blocks_new_risk():
    for health in (ReconciliationHealth.STALE, ReconciliationHealth.CONFLICTED):
        snap = HealthSnapshot(
            state=HealthState.READY,
            writer_owned=True,
            account_matched=True,
            data_current=True,
            reconciliation=health,
            protection=ProtectionStatus.CONFIRMED,
            has_open_exposure=False,
            drawdown_stop_active=False,
        )
        assert not new_risk_allowed(snap).allowed


def test_unconfirmed_protection_blocks_new_risk_with_exposure():
    for prot in (
        ProtectionStatus.NONE,
        ProtectionStatus.UNCONFIRMED,
        ProtectionStatus.BREACHED,
    ):
        snap = HealthSnapshot(
            state=HealthState.READY,
            writer_owned=True,
            account_matched=True,
            data_current=True,
            reconciliation=ReconciliationHealth.CURRENT,
            protection=prot,
            has_open_exposure=True,
            drawdown_stop_active=False,
        )
        dec = new_risk_allowed(snap)
        assert not dec.allowed, prot


def test_healthy_ready_allows_local_predicate():
    snap = HealthSnapshot(
        state=HealthState.READY,
        writer_owned=True,
        account_matched=True,
        data_current=True,
        reconciliation=ReconciliationHealth.CURRENT,
        protection=ProtectionStatus.CONFIRMED,
        has_open_exposure=True,
        drawdown_stop_active=False,
    )
    assert new_risk_allowed(snap).allowed
    # Flat with NONE protection is fine (no exposure to protect)
    flat = HealthSnapshot(
        state=HealthState.READY,
        writer_owned=True,
        account_matched=True,
        data_current=True,
        reconciliation=ReconciliationHealth.CURRENT,
        protection=ProtectionStatus.NONE,
        has_open_exposure=False,
        drawdown_stop_active=False,
    )
    assert new_risk_allowed(flat).allowed


def test_writer_unowned_blocks_even_if_ready():
    snap = HealthSnapshot(
        state=HealthState.READY,
        writer_owned=False,
        account_matched=True,
        data_current=True,
        reconciliation=ReconciliationHealth.CURRENT,
        protection=ProtectionStatus.NONE,
        has_open_exposure=False,
        drawdown_stop_active=False,
    )
    assert not new_risk_allowed(snap).allowed


def test_identity_typed():
    ident = RuntimeIdentity(
        environment=Environment.TESTNET,
        expected_venue="BYBIT",
        expected_product="linear",
        expected_position_mode="one_way",
        expected_margin_mode="isolated",
        account_identity_hash="abc123",
    )
    assert ident.to_dict()["environment"] == "testnet"
    with pytest.raises(ValueError):
        RuntimeIdentity(
            environment="testnet",  # type: ignore[arg-type]
            expected_venue="BYBIT",
            expected_product="linear",
            expected_position_mode="one_way",
            expected_margin_mode="isolated",
            account_identity_hash="abc123",
        )
