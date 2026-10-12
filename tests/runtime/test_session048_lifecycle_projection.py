"""Journal-backed diagnostic projection; no execution authority is produced."""

from contextlib import closing
from dataclasses import replace
from decimal import Decimal

import pytest
from conftest import T0, add_intent, q

from atlas.domain.enums import CommandOutcome, CommandType
from atlas.domain.execution import make_command
from atlas.runtime.binance_demo import (
    BinanceDemoIdentity,
    external_client_order_id,
    verify_binance_protection,
)
from atlas.runtime.position_lifecycle import project_position_lifecycle
from atlas.runtime.reconciliation_evidence import DEFAULT_EXECUTION_RISK_QUERIES, QueryType, ReconciliationRun
from atlas.v2.strategies.s1_trend import ExecutableQuote
from tests.runtime.test_session041_binance_demo import product


def capture(journal, *, qty="0", contingent=0, fault=None, unknown=False, run_id="s48-run", verified_stop=False):
    p = product()
    identity = BinanceDemoIdentity("owner-demo", "binance-demo-key")
    intent = add_intent(journal)
    command = make_command(
        command_id="entry",
        intent_id=intent.intent_id,
        command_type=CommandType.SUBMIT_ENTRY,
        payload_dict={
            "client_order_id": intent.client_order_id,
            "identity_hash": identity.content_hash,
            "instrument_ref": p.content_hash,
            "symbol": "SOLUSDT",
        },
        expected_state_version=0,
        created_at_ns=T0,
    )
    journal.persist_command(command)
    journal.mark_send_started("entry", T0 + 1)
    journal.update_command_outcome("entry", CommandOutcome.UNKNOWN if unknown else CommandOutcome.RECONCILED)
    if verified_stop:
        stop_client_id = "a" * 32
        stop = make_command(
            command_id="repair-stop",
            intent_id=intent.intent_id,
            command_type=CommandType.REPAIR_STOP,
            payload_dict={
                "identity_hash": identity.content_hash,
                "instrument_ref": p.content_hash,
                "symbol": "SOLUSDT",
                "client_order_id": stop_client_id,
                "side": "SELL",
                "quantity": str(abs(Decimal(qty))),
                "price": "100",
                "stop": "90",
                "reduce_only": True,
            },
            expected_state_version=0,
            created_at_ns=T0,
        )
        journal.persist_command(stop)
        journal.mark_send_started(stop.command_id, T0 + 1)
        journal.update_command_outcome(stop.command_id, CommandOutcome.RECONCILED)
        raw_stop = {
            "symbol": "SOLUSDT",
            "algoId": 202,
            "algoType": "CONDITIONAL",
            "clientAlgoId": external_client_order_id(stop_client_id),
            "algoStatus": "NEW",
            "orderType": "STOP_MARKET",
            "workingType": "MARK_PRICE",
            "positionSide": "BOTH",
            "side": "SELL" if Decimal(qty) > 0 else "BUY",
            "closePosition": True,
            "triggerPrice": "90",
            "reduceOnly": False,
            "quantity": "0",
        }
        stop_readback = verify_binance_protection(
            raw_stop,
            symbol="SOLUSDT",
            client_order_id=stop_client_id,
            stop=Decimal("90"),
            signed_position=Decimal(qty),
            now_ns=T0 + 1,
            received_at_ns=T0 + 1,
        )
        protection_fact = {
            "command_id": stop.command_id,
            "verified": stop_readback.verified,
            "reason": stop_readback.reason,
            "observed_at_ns": stop_readback.observed_at_ns,
            "position_epoch": intent.position_epoch,
            "signed_position": qty,
            "algo_order_id": stop_readback.algo_order_id,
            "stop": "90",
        }
    else:
        protection_fact = None
    journal.create_reconciliation_run(
        ReconciliationRun(run_id, identity.account_scope_ref, "SOLUSDT", "writer", 1, "runtime", T0, None)
    )
    for kind in DEFAULT_EXECUTION_RISK_QUERIES:
        facts = {
            "execution_key": p.key.to_dict(),
            "instrument_ref": p.content_hash,
            "venue_identity_hash": identity.content_hash,
            "environment": "DEMO",
            "receipt_source": "SIGNED_BINANCE_DEMO_READER",
            "response_hashes": ["a" * 64],
        }
        records = (1 if verified_stop else contingent) if kind == QueryType.CONDITIONAL_ORDERS else 0
        if kind == QueryType.POSITIONS:
            facts.update(signed_qty=qty, position_epoch=0)
            if fault == "contradictory":
                facts["signed_qty"] = "NaN"
        if kind == QueryType.TRADING_STOP:
            facts["protection_readbacks"] = [protection_fact] if protection_fact else []
        if fault == "venue":
            facts["execution_key"] = replace(p.key, contract_revision="wrong").to_dict()
        e = q(
            kind,
            kind.value,
            account=identity.account_scope_ref,
            instrument="SOLUSDT",
            facts=facts,
            records=records,
            complete=fault != "incomplete",
            receipt_time_ns=T0 + 1,
        )
        journal.append_reconciliation_query_evidence(e)
        journal.bind_query_to_run(run_id, e.query_id)
    if fault != "incomplete":
        journal.complete_reconciliation_run(run_id, T0 + 10)
    quote = ExecutableQuote(p.key, Decimal("119"), Decimal("120"), T0, T0, "fixture-quote")
    return intent, p, identity, quote


def project(journal, inputs, *, reconciliation_run_id="s48-run", **changes):
    intent, p, identity, quote = inputs
    args = {
        "journal": journal,
        "reconciliation_run_id": reconciliation_run_id,
        "intent_id": intent.intent_id,
        "identity": identity,
        "product": p,
        "writer_id": "writer",
        "writer_epoch": 1,
        "now_ns": T0 + 11,
        "quote": quote,
        "funding_schedule_ref": p.funding_schedule_ref,
    }
    return project_position_lifecycle(**(args | changes))


def test_latest_complete_absent_stop_downgrades_previously_verified_protection(journal):
    inputs = capture(journal, qty="1", run_id="s49-protected", verified_stop=True)
    protected = project(journal, inputs, reconciliation_run_id="s49-protected")
    assert protected.state == "OPEN_PROTECTED" and protected.protection_verified

    intent, p, identity, _quote = inputs
    run_id = "s49-latest-stop-absent"
    journal.create_reconciliation_run(
        ReconciliationRun(run_id, identity.account_scope_ref, "SOLUSDT", "writer", 1, "runtime", T0 + 20, None)
    )
    for kind in DEFAULT_EXECUTION_RISK_QUERIES:
        facts = {
            "execution_key": p.key.to_dict(),
            "instrument_ref": p.content_hash,
            "venue_identity_hash": identity.content_hash,
            "environment": "DEMO",
            "receipt_source": "SIGNED_BINANCE_DEMO_READER",
            "response_hashes": ["b" * 64],
        }
        if kind == QueryType.POSITIONS:
            facts.update(signed_qty="1", position_epoch=intent.position_epoch)
        elif kind == QueryType.TRADING_STOP:
            facts["protection_readbacks"] = []
        evidence = q(
            kind,
            f"latest-{kind.value}",
            account=identity.account_scope_ref,
            instrument="SOLUSDT",
            facts=facts,
            records=0,
            complete=True,
            receipt_time_ns=T0 + 21,
        )
        journal.append_reconciliation_query_evidence(evidence)
        journal.bind_query_to_run(run_id, evidence.query_id)
    journal.complete_reconciliation_run(run_id, T0 + 22)

    latest = project(journal, inputs, reconciliation_run_id=run_id, now_ns=T0 + 23)
    assert latest.state == "OPEN_UNPROTECTED"
    assert not latest.protection_verified


def test_flat_risk_evidence_is_not_mature_economic_evidence(journal):
    result = project(journal, capture(journal))
    assert result.state == "FLAT_PENDING_ECONOMICS"
    assert result.signed_quantity == Decimal("0")
    assert not result.can_mature and not result.can_admit_opening and not result.can_release_reservation
    assert result.economic_status == "NOT ESTIMABLE"


@pytest.mark.parametrize("fault", ["venue", "incomplete", "contradictory"])
def test_invalid_local_readback_is_unknown(journal, fault):
    assert project(journal, capture(journal, fault=fault)).state == "UNKNOWN"


@pytest.mark.parametrize("fault", ["epoch", "writer", "foreign_quote", "no_quote", "stale_quote", "funding", "product"])
def test_local_scope_context_failures_never_become_executable(journal, fault):
    inputs = capture(journal, qty="1")
    _, p, identity, quote = inputs
    from atlas.v2.instruments import VenueV2

    changes = {
        "epoch": {"writer_epoch": 2},
        "writer": {"writer_id": "old"},
        "foreign_quote": {"quote": replace(quote, key=replace(p.key, venue=VenueV2.BYBIT))},
        "no_quote": {"quote": None},
        "stale_quote": {"quote": replace(quote, observed_at_ns=T0 - 6_000_000_000)},
        "funding": {"funding_schedule_ref": "b" * 64},
        "product": {"product": replace(p, base_units_per_contract=Decimal("10"))},
    }[fault]
    result = project(journal, inputs, **changes)
    assert result.reasons and not result.can_admit_opening
    if fault in ("epoch", "writer", "product"):
        assert result.state == "UNKNOWN"
    else:
        assert result.state == "OPEN_UNPROTECTED"


def test_unknown_send_and_late_exposure_remain_unknown_without_duplicate_opening(journal):
    inputs = capture(journal, qty="0.25", unknown=True)
    before = journal.count("commands")
    result = project(journal, inputs)
    assert result.state == "UNKNOWN" and result.signed_quantity == Decimal("0.25")
    assert journal.count("commands") == before


def test_flatness_with_orphan_conditional_order_is_not_terminal(journal):
    result = project(journal, capture(journal, contingent=1))
    assert result.state == "UNKNOWN"
    assert "OUTSTANDING_CONTINGENT_ORDERS" in result.reasons


def test_projection_restarts_and_never_invents_stop_or_cash_evidence(tmp_path):
    from atlas.persistence.sqlite import SQLiteJournal

    path = tmp_path / "journal.db"
    with closing(SQLiteJournal(path)) as journal:
        inputs = capture(journal, qty="0.3")
        first = project(journal, inputs)
        assert first.state == "OPEN_UNPROTECTED"
    with closing(SQLiteJournal(path)) as journal:
        assert project(journal, inputs) == first


def test_real_recovery_capture_adds_source_product_identity_without_authority(journal):
    from atlas.runtime.binance_reconciliation import capture_binance_recovery_cycle
    from tests.runtime.test_session041_binance_reconciliation import HistoryReader, _recovery_responses

    p = product()
    p = replace(p, key=replace(p.key, native_symbol="BTCUSDT"))
    identity = BinanceDemoIdentity("owner-demo", "binance-demo-key")
    reader = HistoryReader(identity, _recovery_responses(), T0)
    result = capture_binance_recovery_cycle(
        reader,
        journal,
        symbol="BTCUSDT",
        writer_id="writer",
        writer_epoch=1,
        runtime_instance_id="runtime",
        position_epoch=0,
        history_start_ns=T0 - 1_000_000_000,
        assert_writer=lambda: None,
        assert_quiescent=lambda: None,
        product=p,
    )
    for query in journal.load_run_queries(result.reconciliation_run_id):
        assert query.facts["execution_key"] == p.key.to_dict()
        assert query.facts["instrument_ref"] == p.content_hash
    assert not result.complete_for_recovery  # no actual source qualification supplied


def test_foreign_product_is_rejected_before_recovery_transport(journal):
    from atlas.runtime.binance_reconciliation import capture_binance_recovery_cycle
    from atlas.v2.instruments import VenueV2
    from tests.runtime.test_session041_binance_reconciliation import HistoryReader, _recovery_responses

    p = product()
    p = replace(p, key=replace(p.key, venue=VenueV2.BYBIT))
    reader = HistoryReader(BinanceDemoIdentity("owner-demo", "key"), _recovery_responses(), T0)
    with pytest.raises(ValueError, match="product mismatch"):
        capture_binance_recovery_cycle(
            reader,
            journal,
            symbol="SOLUSDT",
            writer_id="writer",
            writer_epoch=1,
            runtime_instance_id="runtime",
            position_epoch=0,
            history_start_ns=T0 - 1_000_000_000,
            assert_writer=lambda: None,
            assert_quiescent=lambda: None,
            product=p,
        )
    assert not reader.calls


@pytest.mark.parametrize("event_type", ["MARGIN_CHANGE", "COLLATERAL_TRANSFER", "LIQUIDATION", "ADL"])
def test_external_economic_incident_does_not_make_flat_exposure_mature(journal, event_type):
    from atlas.domain.execution import EconomicEvent

    inputs = capture(journal)
    identity = inputs[2]
    event = EconomicEvent(
        identity.account_scope_ref,
        "incident",
        "USDT",
        Decimal("-5"),
        T0 + 2,
        T0 + 3,
        event_type,
        "explicit-offline-fixture",
    )
    journal.append_economic_event(event)
    result = project(journal, inputs)
    assert result.economic_status == "NOT ESTIMABLE" and not result.can_mature
    assert journal.load_economic_event(identity.account_scope_ref, "incident") == event
    assert journal.count("reservation_release_events") == 0


def test_disk_write_failure_preserves_uncertain_intent_and_prevents_effect(journal, monkeypatch):
    import sqlite3

    from atlas.persistence.sqlite import PersistenceError

    inputs = capture(journal, qty="0.25", unknown=True)
    original = journal._tx
    from contextlib import contextmanager

    @contextmanager
    def failed_tx():
        raise sqlite3.OperationalError("offline simulated disk full")
        yield  # pragma: no cover

    monkeypatch.setattr(journal, "_tx", failed_tx)
    command = make_command(
        command_id="never-sent",
        intent_id=inputs[0].intent_id,
        command_type=CommandType.FLATTEN,
        payload_dict={"reduce_only": True},
        expected_state_version=0,
        created_at_ns=T0 + 10,
    )
    with pytest.raises(PersistenceError):
        journal.persist_command(command)
    monkeypatch.setattr(journal, "_tx", original)
    assert project(journal, inputs).state == "UNKNOWN"
    assert journal.count("commands") == 1


def test_external_conditional_order_with_exposure_is_unknown_and_never_deleted(journal):
    inputs = capture(journal, qty="1", contingent=1)
    before = journal.count("commands")
    result = project(journal, inputs)
    assert result.state == "UNKNOWN" and "EXTERNAL_OR_UNRESOLVED_ORDERS" in result.reasons
    assert journal.count("commands") == before


def test_selected_runtime_consumer_is_read_only_and_fences_review_twice(journal):
    from types import SimpleNamespace
    from unittest.mock import Mock

    from atlas.runtime.selected_demo import SelectedDemoQualification

    inputs = capture(journal, qty="0.5", unknown=True)
    runtime = object.__new__(SelectedDemoQualification)
    runtime.identity = inputs[2]
    runtime.product = inputs[1]
    runtime.journal = journal
    runtime.ownership = SimpleNamespace(writer_id="writer", writer_epoch=1)
    runtime.assert_writer = Mock()
    before = journal.count("commands")
    result = runtime.project_lifecycle(
        reconciliation_run_id="s48-run",
        intent_id=inputs[0].intent_id,
        now_ns=T0 + 11,
        quote=inputs[3],
        funding_schedule_ref=None,
    )
    assert result.state == "UNKNOWN" and not result.can_admit_opening
    assert runtime.assert_writer.call_count == 2 and journal.count("commands") == before
