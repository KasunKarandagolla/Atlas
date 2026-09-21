from __future__ import annotations

from decimal import Decimal

import pytest

from atlas.persistence.sqlite import PersistenceError, SQLiteJournal
from atlas.runtime.fill_dedup import FillDeduplicator, FillRecord, OrderStatusRecord

T0 = 1_700_000_000_000_000_000


def _fill(execution_id: str, qty: str = "0.010") -> FillRecord:
    return FillRecord(
        execution_id=execution_id, order_id="order-1", client_order_id="a" * 32,
        intent_id="intent-1", instrument="BTCUSDT", side="Buy", qty=Decimal(qty),
        price=Decimal("49000"), fee=Decimal("0.1"), fee_currency="USDT",
        trade_time_ns=T0, receive_time_ns=T0 + 1, source="rest_query", raw_hash=execution_id,
    )


def _status(qty: str) -> OrderStatusRecord:
    return OrderStatusRecord(
        order_id="order-1", client_order_id="a" * 32, intent_id="intent-1", status="Filled",
        cum_exec_qty=Decimal(qty), cum_exec_fee=Decimal("0.1"),
        cum_exec_value=Decimal(qty) * Decimal("49000"), avg_exec_price=Decimal("49000"),
        receive_time_ns=T0 + 2, source="private_stream", raw_hash="status-1",
    )


def test_execution_ids_and_statuses_persist_across_restart(tmp_path):
    path = tmp_path / "evidence.db"
    journal = SQLiteJournal(path)
    dedup = FillDeduplicator(journal)
    assert dedup.try_record_fill(_fill("exec-1")).accepted
    dedup.record_status(_status("0.010"))
    journal.close()

    restarted = SQLiteJournal(path)
    restored = FillDeduplicator(restarted)
    duplicate = restored.try_record_fill(_fill("exec-1"))
    assert not duplicate.accepted
    assert restored.get_cumulative_qty("intent-1") == Decimal("0.010")
    assert len(restarted.load_order_status_observations(intent_id="intent-1")) == 1
    restarted.close()


def test_conflicting_duplicate_execution_is_quarantined(tmp_path):
    journal = SQLiteJournal(tmp_path / "evidence.db")
    dedup = FillDeduplicator(journal)
    assert dedup.try_record_fill(_fill("exec-1", "0.010")).accepted
    with pytest.raises((PersistenceError, ValueError), match="conflict"):
        dedup.try_record_fill(_fill("exec-1", "0.011"))
    assert journal.count("execution_evidence") == 1
    journal.close()


def test_order_status_does_not_create_fill_or_regress_economics(tmp_path):
    journal = SQLiteJournal(tmp_path / "evidence.db")
    dedup = FillDeduplicator(journal)
    dedup.record_status(_status("0.010"))
    dedup.record_status(_status("0.000"))
    assert dedup.get_cumulative_qty("intent-1") == Decimal("0")
    journal.close()
