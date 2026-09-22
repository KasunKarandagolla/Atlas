from __future__ import annotations

from decimal import Decimal

import pytest
from conftest import T0, completed_run, q

from atlas.domain.enums import ProtectionStatus
from atlas.persistence.sqlite import PersistenceError
from atlas.runtime.flat_certificate import FlatCertificate, FlatCertificationDecision
from atlas.runtime.protection_evidence import ProtectionEvidence
from atlas.runtime.reconciliation_evidence import (
    DEFAULT_EXECUTION_RISK_QUERIES,
    QueryType,
    ReconciliationRun,
    ReconciliationRunState,
)
from atlas.runtime.recovery import RecoveryDecision, recover_from_persisted_run


def test_preconstructed_certified_flat_cannot_be_persisted_or_authorize_recovery(journal):
    completed_run(journal)
    fake = FlatCertificate(
        certification_id="fake-flat",
        reconciliation_run_id="run-1",
        intent_id="intent-1",
        writer_id="writer",
        writer_epoch=1,
        account_identity_hash="acct",
        instrument="BTCUSDT",
        position_epoch=0,
        decision=FlatCertificationDecision.CERTIFIED_FLAT,
        certified_at_ns=T0 + 20,
        evidence_refs=(),
        mismatch_details=(),
    )
    with pytest.raises(PersistenceError, match="cannot authorize release"):
        journal.append_flat_certificate(fake)
    assert journal.count("flat_certificates") == 0
    certificate = recover_from_persisted_run(
        journal=journal,
        recovery_run_id="recovery-fake-flat",
        reconciliation_run_id="run-1",
        runtime_instance_id="runtime",
        writer_id="writer",
        writer_epoch=1,
        unresolved_intent_ids=(),
        unresolved_command_ids=(),
        unknown_command_ids=(),
        account="acct",
        instrument="BTCUSDT",
        position_epoch=0,
        started_at_ns=T0,
        ended_at_ns=T0 + 21,
        prerequisites_ok=True,
        flat_certificate_id="fake-flat",
    )
    assert certificate.decision != RecoveryDecision.READY


def test_fake_confirmed_protection_is_not_ready_after_restart(journal):
    completed_run(journal)
    fake = ProtectionEvidence(
        account_ref="acct",
        instrument="BTCUSDT",
        position_epoch=0,
        desired_stop_version=1,
        observed_signed_qty=Decimal("0"),
        full_position_semantics=True,
        stop_price=Decimal("48000"),
        trigger_basis="MarkPrice",
        closing_only_behavior=True,
        position_view_evidence_ids=("caller-position-view",),
        conditional_order_view_evidence_ids=("caller-conditional-view",),
        observation_time_ns=T0,
        receive_time_ns=T0 + 1,
        status=ProtectionStatus.CONFIRMED,
        market_stop_semantics=True,
    )
    journal.append_protection_evidence("fake-protection", fake, fake.expected_evidence_hash())
    certificate = recover_from_persisted_run(
        journal=journal,
        recovery_run_id="recovery-fake-protection",
        reconciliation_run_id="run-1",
        runtime_instance_id="runtime",
        writer_id="writer",
        writer_epoch=1,
        unresolved_intent_ids=(),
        unresolved_command_ids=(),
        unknown_command_ids=(),
        account="acct",
        instrument="BTCUSDT",
        position_epoch=0,
        started_at_ns=T0,
        ended_at_ns=T0 + 2,
        prerequisites_ok=True,
        protection_evidence_id="fake-protection",
    )
    assert certificate.decision != RecoveryDecision.READY


def test_reduced_query_set_and_caller_complete_run_are_rejected():
    with pytest.raises(ValueError, match="omits mandatory"):
        ReconciliationRun(
            "reduced",
            "acct",
            "BTCUSDT",
            "writer",
            1,
            "runtime",
            T0,
            None,
            (QueryType.POSITIONS,),
            ReconciliationRunState.OPEN,
        )
    with pytest.raises(ValueError, match="must be OPEN"):
        ReconciliationRun(
            "complete",
            "acct",
            "BTCUSDT",
            "writer",
            1,
            "runtime",
            T0,
            T0 + 1,
            DEFAULT_EXECUTION_RISK_QUERIES,
            ReconciliationRunState.COMPLETE,
        )


def test_unbound_hash_and_residual_open_order_fail_closed(journal):
    evidence = q(QueryType.POSITIONS, "hash-test")
    object.__setattr__(evidence, "evidence_hash", "a" * 64)
    with pytest.raises(PersistenceError, match="hash"):
        journal.append_reconciliation_query_evidence(evidence)

    completed_run(journal, run_id="residual")
    residual = q(QueryType.OPEN_ORDERS, "zz-residual-open", records=1)
    journal.append_reconciliation_query_evidence(residual)
    journal.bind_query_to_run("residual", residual.query_id)
    certificate = recover_from_persisted_run(
        journal=journal,
        recovery_run_id="recovery-residual",
        reconciliation_run_id="residual",
        runtime_instance_id="runtime",
        writer_id="writer",
        writer_epoch=1,
        unresolved_intent_ids=(),
        unresolved_command_ids=(),
        unknown_command_ids=(),
        account="acct",
        instrument="BTCUSDT",
        position_epoch=0,
        started_at_ns=T0,
        ended_at_ns=T0 + 51,
        prerequisites_ok=True,
        flat_certificate_id="missing-flat",
    )
    assert certificate.decision == RecoveryDecision.RECOVERY_REQUIRED
