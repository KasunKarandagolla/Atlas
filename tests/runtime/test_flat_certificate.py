from __future__ import annotations

from decimal import Decimal

from atlas.runtime.fill_dedup import FillRecord, build_execution_evidence
from atlas.runtime.flat_certificate import FlatCertificationDecision, certify_flat
from atlas.runtime.reconciliation_evidence import (
    Completeness,
    QueryStatus,
    QueryType,
    ReconciliationQueryEvidence,
    compute_evidence_hash,
)
from tests.support.fault_injection import DeterministicVenueSimulator

T0 = 1_700_000_000_000_000_000


def _query(kind: QueryType, query_id: str, *, records: int = 0) -> ReconciliationQueryEvidence:
    return ReconciliationQueryEvidence(
        query_id=query_id, query_type=kind, account="acct-hash", instrument="BTCUSDT",
        requested_interval_start_ns=T0 - 10, requested_interval_end_ns=T0 + 10,
        pagination_cursors=(query_id,), pages_observed=1, total_records_returned=records,
        completeness=Completeness.COMPLETE, status=QueryStatus.SUCCESS, source_time_ns=T0, receipt_time_ns=T0 + 1,
        request_ids=(query_id,), retention_coverage_start_ns=T0 - 10, retention_coverage_end_ns=T0 + 10,
        evidence_hash=compute_evidence_hash({"query": query_id}), error_message=None,
    )


def test_flat_certificate_allows_historical_fills_when_current_position_is_flat():
    fill = FillRecord(
        execution_id="exec-open", order_id="order-1", client_order_id="a" * 32, intent_id="intent-1",
        instrument="BTCUSDT", side="Buy", qty=Decimal("0.010"), price=Decimal("49000"), fee=Decimal("0.1"),
        fee_currency="USDT", trade_time_ns=T0, receive_time_ns=T0 + 1, source="rest_query", raw_hash="raw",
    )
    evidence = build_execution_evidence("intent-1", "a" * 32, [fill], [], T0 + 2)
    certificate = certify_flat(
        certification_id="cert-1", reconciliation_run_id="recon-1", writer_id="writer-1", writer_epoch=1,
        account_identity_hash="acct-hash", instrument="BTCUSDT", position_epoch=0,
        zero_position_evidence=_query(QueryType.POSITIONS, "position"), opening_commands_evidence=[_query(QueryType.ORDER_HISTORY, "opening")],
        no_remaining_opening_orders_evidence=_query(QueryType.OPEN_ORDERS, "open"),
        no_residual_protection_orders_evidence=_query(QueryType.CONDITIONAL_ORDERS, "conditional"),
        execution_evidence=evidence, economic_evidence=_query(QueryType.TRANSACTION_LOG, "economic", records=1), now_ns=T0 + 3,
        current_signed_position_qty=Decimal("0"), current_position_evidence_complete=True,
        terminal_opening_order_certainty=True, no_unresolved_opening_command=True,
        reservation_release_evidence=True, writer_identity_current=True, account_identity_current=True,
        required_query_windows_complete=True,
    )
    assert certificate.decision == FlatCertificationDecision.CERTIFIED_FLAT
    assert certificate.can_release_reservation


def test_late_contradictory_evidence_reopens_recovery():
    certificate = certify_flat(
        certification_id="cert-2", reconciliation_run_id="recon-2", writer_id="writer-1", writer_epoch=1,
        account_identity_hash="acct-hash", instrument="BTCUSDT", position_epoch=0,
        zero_position_evidence=_query(QueryType.POSITIONS, "position"), opening_commands_evidence=[_query(QueryType.ORDER_HISTORY, "opening")],
        no_remaining_opening_orders_evidence=_query(QueryType.OPEN_ORDERS, "open"),
        no_residual_protection_orders_evidence=_query(QueryType.CONDITIONAL_ORDERS, "conditional"),
        execution_evidence=build_execution_evidence("intent-1", "a" * 32, [], [], T0 + 2),
        economic_evidence=_query(QueryType.TRANSACTION_LOG, "economic"), now_ns=T0 + 3,
        current_position_evidence_complete=True, terminal_opening_order_certainty=True,
        no_unresolved_opening_command=True, reservation_release_evidence=True,
        writer_identity_current=True, account_identity_current=True, required_query_windows_complete=True,
        late_contradictory_evidence=True,
    )
    assert certificate.decision != FlatCertificationDecision.CERTIFIED_FLAT


def test_reservation_release_consumes_valid_flat_certificate():
    simulator = DeterministicVenueSimulator()
    simulator._open()
    try:
        certificate = certify_flat(
            certification_id="cert-release", reconciliation_run_id="recon-release", writer_id="writer-1", writer_epoch=1,
            account_identity_hash="acct-hash", instrument="BTCUSDT", position_epoch=0,
            zero_position_evidence=simulator._query(QueryType.POSITIONS, query_id="position-release"),
            opening_commands_evidence=[simulator._query(QueryType.ORDER_HISTORY, query_id="opening-release")],
            no_remaining_opening_orders_evidence=simulator._query(QueryType.OPEN_ORDERS, query_id="open-release"),
            no_residual_protection_orders_evidence=simulator._query(QueryType.CONDITIONAL_ORDERS, query_id="conditional-release"),
            execution_evidence=build_execution_evidence(
                "intent-fault", simulator.journal.load_intent("intent-fault").client_order_id, [], [], T0 + 2
            ),
            economic_evidence=simulator._query(QueryType.TRANSACTION_LOG, query_id="economic-release"), now_ns=T0 + 3,
            current_position_evidence_complete=True, terminal_opening_order_certainty=True,
            no_unresolved_opening_command=True, reservation_release_evidence=True,
            writer_identity_current=True, account_identity_current=True, required_query_windows_complete=True,
            intent_id="intent-fault",
        )
        released = simulator.journal.release_reservation_from_flat_certificate(certificate)
        assert released.remaining_open_qty == Decimal("0")
        assert simulator.journal.load_reservation("intent-fault").remaining_open_qty == Decimal("0")
    finally:
        simulator._close()
