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
        observed_signed_qty=Decimal("0.01"),
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


def test_zero_quantity_confirmed_protection_cannot_be_persisted(journal):
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
        position_view_evidence_ids=("position-view",),
        conditional_order_view_evidence_ids=("conditional-view",),
        observation_time_ns=T0,
        receive_time_ns=T0 + 1,
        status=ProtectionStatus.CONFIRMED,
        market_stop_semantics=True,
    )
    with pytest.raises(PersistenceError, match="flat position"):
        journal.append_protection_evidence("flat-protection", fake, fake.expected_evidence_hash())


def _protection_run(journal, *, positive: bool) -> tuple[str, dict[str, str]]:
    run = ReconciliationRun(
        "protection-run",
        "acct",
        "BTCUSDT",
        "writer",
        1,
        "runtime",
        T0,
        None,
        DEFAULT_EXECUTION_RISK_QUERIES,
        ReconciliationRunState.OPEN,
    )
    journal.create_reconciliation_run(run)
    ids: dict[str, str] = {}
    for query_type in DEFAULT_EXECUTION_RISK_QUERIES:
        facts: dict[str, object] = {}
        records = 0
        if query_type == QueryType.POSITIONS:
            facts = {"signed_qty": "0.01", "position_epoch": 0}
            records = 1
        elif query_type == QueryType.CONDITIONAL_ORDERS and positive:
            facts = {
                "is_current_protection": True,
                "native_stop_visible": True,
                "protection_representation": "conditional_order",
                "protection_order_ids": ["native-stop-1"],
            }
            records = 1
        elif query_type == QueryType.TRADING_STOP and positive:
            facts = {
                "position_epoch": 0,
                "signed_qty": "0.01",
                "desired_stop_version": 1,
                "stop_price": "48000",
                "trigger_basis": "MarkPrice",
                "market_stop_semantics": True,
                "full_position_semantics": True,
                "closing_only_behavior": True,
                "native_stop_visible": True,
            }
            records = 1
        evidence = q(query_type, f"protection-{query_type.value}", records=records, facts=facts)
        journal.append_reconciliation_query_evidence(evidence)
        journal.bind_query_to_run(run.run_id, evidence.query_id)
        ids[query_type.value] = evidence.query_id
    journal.complete_reconciliation_run(run.run_id, T0 + 10)
    return run.run_id, ids


def test_complete_run_with_empty_protection_views_cannot_authorize_ready(journal):
    run_id, ids = _protection_run(journal, positive=False)
    fake = ProtectionEvidence(
        account_ref="acct",
        instrument="BTCUSDT",
        position_epoch=0,
        desired_stop_version=1,
        observed_signed_qty=Decimal("0.01"),
        full_position_semantics=True,
        stop_price=Decimal("48000"),
        trigger_basis="MarkPrice",
        closing_only_behavior=True,
        position_view_evidence_ids=(ids[QueryType.POSITIONS.value],),
        conditional_order_view_evidence_ids=(ids[QueryType.CONDITIONAL_ORDERS.value],),
        observation_time_ns=T0 + 1,
        receive_time_ns=T0 + 2,
        status=ProtectionStatus.CONFIRMED,
        market_stop_semantics=True,
    )
    journal.append_protection_evidence("empty-protection", fake, fake.expected_evidence_hash())
    certificate = recover_from_persisted_run(
        journal=journal,
        recovery_run_id="recovery-empty-protection",
        reconciliation_run_id=run_id,
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
        ended_at_ns=T0 + 20,
        prerequisites_ok=True,
        protection_evidence_id="empty-protection",
    )
    assert certificate.decision != RecoveryDecision.READY


def test_positive_run_bound_protection_facts_can_authorize_ready_offline(journal):
    run_id, ids = _protection_run(journal, positive=True)
    evidence = ProtectionEvidence(
        account_ref="acct",
        instrument="BTCUSDT",
        position_epoch=0,
        desired_stop_version=1,
        observed_signed_qty=Decimal("0.01"),
        full_position_semantics=True,
        stop_price=Decimal("48000"),
        trigger_basis="MarkPrice",
        closing_only_behavior=True,
        position_view_evidence_ids=(ids[QueryType.POSITIONS.value],),
        conditional_order_view_evidence_ids=(ids[QueryType.CONDITIONAL_ORDERS.value],),
        observation_time_ns=T0 + 1,
        receive_time_ns=T0 + 2,
        status=ProtectionStatus.CONFIRMED,
        market_stop_semantics=True,
    )
    journal.append_protection_evidence("positive-protection", evidence, evidence.expected_evidence_hash())
    certificate = recover_from_persisted_run(
        journal=journal,
        recovery_run_id="recovery-positive-protection",
        reconciliation_run_id=run_id,
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
        ended_at_ns=T0 + 20,
        prerequisites_ok=True,
        protection_evidence_id="positive-protection",
    )
    assert certificate.decision == RecoveryDecision.READY


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

    residual = q(QueryType.OPEN_ORDERS, "zz-residual-open", records=1)
    completed_run(journal, run_id="residual", extra_queries=(residual,))
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
