import hashlib
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from types import SimpleNamespace

import pytest
from support.assisted_control_fixture import make_plan

from atlas.domain.enums import CommandType, LifecycleState, ProtectionStatus, ReconciliationHealth
from atlas.domain.execution import EconomicEvent, Intent, Reservation, make_command
from atlas.persistence.sqlite import SQLiteJournal
from atlas.runtime.binance_demo import BinanceDemoIdentity, external_client_order_id
from atlas.runtime.binance_reconciliation import (
    BinanceRecoveryCapabilityLedgerV1,
    capture_account_snapshot,
    capture_binance_recovery_cycle,
    normalize_binance_income,
    normalize_binance_order_row,
    reconcile_binance_execution_history,
    record_binance_income,
    record_binance_order_status,
    record_binance_trades,
)


class StubReader:
    def __init__(self, identity, responses, now_ns):
        self.identity = identity
        self.responses = iter(responses)
        self.clock_ns = lambda: now_ns

    def read_with_receipt(self, path):
        payload = next(self.responses)
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return SimpleNamespace(
            identity_hash=self.identity.content_hash,
            endpoint=path,
            received_at_ns=self.clock_ns(),
            raw_payload_hash=hashlib.sha256(canonical.encode()).hexdigest(),
            payload=payload,
        )


def _persist_trade_intent(journal, client_id="00000000000000000000000000000001"):
    make_plan(journal, plan_id="plan")
    intent = Intent("intent-1", 0, "plan", "v1", client_id, 1, LifecycleState.SUBMITTING,
                    ProtectionStatus.UNCONFIRMED, ReconciliationHealth.STALE, 1_800_000_000_000_000_000)
    reservation = Reservation("reservation-1", "intent-1", Decimal("1"), Decimal("0"), Decimal("0"),
                              Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"))
    journal.create_intent_with_reservation(intent, reservation)
    journal.persist_command(make_command(
        command_id="cmd-1", intent_id="intent-1", command_type=CommandType.SUBMIT_EXIT,
        payload_dict={"client_order_id": client_id, "symbol": "BTCUSDT",
                      "identity_hash": BinanceDemoIdentity("scope-a", "cred-a").content_hash},
        expected_state_version=0, created_at_ns=1_800_000_000_000_000_000,
    ))


def test_account_snapshot_binds_modes_identity_age_and_unverified_scope():
    now = 1_800_000_000_000_000_000
    identity = BinanceDemoIdentity("scope-a", "cred-a")
    reader = StubReader(
        identity,
        [
            {"canTrade": True, "multiAssetsMargin": False},
            {"dualSidePosition": False, "multiAssetsMargin": False, "canTrade": True},
            [{"symbol": "BTCUSDT", "positionSide": "BOTH", "marginType": "isolated", "isolated": True,
              "positionAmt": "0", "updateTime": 0}],
            [{"symbol": "BTCUSDT", "marginType": "ISOLATED"}],
            [{"asset": "USDT", "accountAlias": ""}],
        ],
        now,
    )
    snapshot = capture_account_snapshot(reader, now_ns=now)
    assert snapshot.identity_hash == identity.content_hash
    assert snapshot.account_scope_ref == "scope-a"
    assert snapshot.eligible is False
    assert snapshot.account_fingerprint is None
    assert "ACCOUNT_FINGERPRINT_UNVERIFIED" in snapshot.reasons
    assert snapshot.positions[0].source_hash


def test_account_snapshot_rejects_stale_or_cross_mode_position():
    now = 1_800_000_000_000_000_000
    identity = BinanceDemoIdentity("scope-a", "cred-a")
    reader = StubReader(
        identity,
        [
            {"canTrade": True, "multiAssetsMargin": False, "uid": 17},
            {"dualSidePosition": False, "multiAssetsMargin": False, "canTrade": True},
            [{"symbol": "BTCUSDT", "positionSide": "LONG", "marginType": "cross", "isolated": False,
              "positionAmt": "1", "updateTime": now // 1_000_000 - 3_000}],
            [{"symbol": "BTCUSDT", "marginType": "CROSSED"}],
            [{"asset": "USDT", "accountAlias": ""}],
        ],
        now,
    )
    snapshot = capture_account_snapshot(reader, now_ns=now)
    assert snapshot.account_fingerprint_status == "VERIFIED_STABLE_UID"
    assert snapshot.eligible is False
    assert "POSITION_NOT_ISOLATED_ONE_WAY" in snapshot.reasons


def test_native_decoder_normalizes_regular_and_algo_ids_and_rejects_wrong_prefix():
    client_id = "00000000000000000000000000000001"
    wire = external_client_order_id(client_id)
    regular = normalize_binance_order_row(
        {"orderId": 101, "clientOrderId": wire}, received_at_ns=1_800_000_000_000_000_000
    )
    algo = normalize_binance_order_row(
        {"algoId": 202, "clientAlgoId": wire}, received_at_ns=1_800_000_000_000_000_000
    )
    assert regular.as_dict()["clientOrderId_local"] == client_id
    assert algo.as_dict()["clientAlgoId_local"] == client_id
    with pytest.raises(ValueError):
        normalize_binance_order_row(
            {"clientAlgoId": "x-not-the-pinned-prefix-0000000000000000000000"},
            received_at_ns=1_800_000_000_000_000_000,
        )


def test_trade_rows_are_deduplicated_by_exchange_execution_id(tmp_path):
    journal = SQLiteJournal(tmp_path / "binance.sqlite")
    identity = BinanceDemoIdentity("scope-a", "cred-a")
    client_id = "00000000000000000000000000000001"
    received = 1_800_000_000_000_000_000
    rows = [{"id": 77, "orderId": 101, "symbol": "BTCUSDT", "side": "BUY", "qty": "0.2",
             "price": "50000", "commission": "0.1", "commissionAsset": "USDT",
             "time": received // 1_000_000 - 5}]
    _persist_trade_intent(journal, client_id)
    first = record_binance_trades(
        journal, identity, rows, received_at_ns=received,
        client_id_by_order_id={("BTCUSDT", "101"): client_id}, intent_id_by_client_id={client_id: "intent-1"},
    )
    second = record_binance_trades(
        journal, identity, rows, received_at_ns=received + 100,
        client_id_by_order_id={("BTCUSDT", "101"): client_id}, intent_id_by_client_id={client_id: "intent-1"},
    )
    assert len(first) == 1 and second == ()
    assert journal.load_execution_evidence()[0].execution_id == f"BINANCE:{identity.content_hash}:BTCUSDT:77"
    assert first[0].qty == Decimal("0.2")


def test_cancel_observation_preserves_cumulative_fill_quantity(tmp_path):
    journal = SQLiteJournal(tmp_path / "binance.sqlite")
    client_id = "00000000000000000000000000000001"
    _persist_trade_intent(journal, client_id)
    status = record_binance_order_status(
        journal,
        {"symbol": "BTCUSDT", "orderId": 101, "clientOrderId": external_client_order_id(client_id), "status": "CANCELED",
         "executedQty": "0.3", "cumQuote": "15000", "avgPrice": "50000"},
        received_at_ns=1_800_000_000_000_000_000,
        intent_id="intent-1",
        identity=BinanceDemoIdentity("scope-a", "cred-a"),
    )
    assert status.status == "CANCELED"
    assert status.cum_exec_qty == Decimal("0.3")
    assert journal.load_order_status_observations()[0].cum_exec_qty == Decimal("0.3")


def test_income_rows_preserve_cost_funding_currency_and_source_hash():
    identity = BinanceDemoIdentity("scope-a", "cred-a")
    received = 1_800_000_000_000_000_000
    event_rows = normalize_binance_income(
        identity,
        [{"tranId": 55, "incomeType": "FUNDING_FEE", "asset": "USDT", "income": "-0.17",
          "time": received // 1_000_000 - 20}],
        received_at_ns=received,
    )
    event, source = event_rows[0]
    assert event.venue_transaction_id == "55"
    assert event.amount == Decimal("-0.17")
    assert event.currency == "USDT" and event.event_type == "FUNDING_FEE"
    assert event.revision == source.source_hash


def test_conflicting_duplicate_trade_and_income_rows_fail_closed(tmp_path):
    identity = BinanceDemoIdentity("scope-a", "cred-a")
    now = 1_800_000_000_000_000_000
    trade = {"id": 8, "orderId": 101, "symbol": "BTCUSDT", "side": "BUY", "qty": "0.2",
             "price": "50000", "commission": "0.1", "commissionAsset": "USDT",
             "time": now // 1_000_000 - 5}
    with pytest.raises(ValueError, match="conflicting Binance trade"):
        record_binance_trades(
            SQLiteJournal(tmp_path / "conflict.sqlite"), identity,
            [trade, {**trade, "qty": "0.3"}], received_at_ns=now,
            client_id_by_order_id={("BTCUSDT", "101"): "00000000000000000000000000000001"},
            intent_id_by_client_id={"00000000000000000000000000000001": "intent-1"},
        )
    income = {"tranId": 55, "incomeType": "FUNDING_FEE", "asset": "USDT", "income": "-0.17",
              "time": now // 1_000_000 - 20}
    with pytest.raises(ValueError, match="conflicting Binance income"):
        normalize_binance_income(identity, [income, {**income, "income": "-0.18"}], received_at_ns=now)


def test_income_persistence_is_idempotent_and_conflicts_are_audited(tmp_path):
    journal = SQLiteJournal(tmp_path / "income.sqlite")
    identity = BinanceDemoIdentity("scope-a", "cred-a")
    now = 1_800_000_000_000_000_000
    row = {"tranId": 55, "incomeType": "FUNDING_FEE", "asset": "USDT", "income": "-0.17",
           "time": now // 1_000_000 - 20}
    first = record_binance_income(journal, identity, [row], received_at_ns=now)
    replay = record_binance_income(journal, identity, [row], received_at_ns=now + 50)
    assert first == replay
    assert journal.load_economic_event("scope-a", "55") == first[0]
    assert journal.count("observations") == 2
    from atlas.persistence.sqlite import PersistenceError

    with pytest.raises(PersistenceError, match="conflicting economic event"):
        journal.append_economic_event(EconomicEvent(
            account="scope-a", venue_transaction_id="55", currency="USDT", amount=Decimal("-0.18"),
            effective_time_ns=first[0].effective_time_ns, received_at_ns=now,
            event_type="FUNDING_FEE", revision="changed",
        ))


def test_conflicting_income_keeps_transport_receipt_before_quarantine(tmp_path):
    journal = SQLiteJournal(tmp_path / "conflicting-income.sqlite")
    identity = BinanceDemoIdentity("scope-a", "cred-a")
    now = 1_800_000_000_000_000_000
    original = {"tranId": 55, "incomeType": "FUNDING_FEE", "asset": "USDT", "income": "-0.17",
                "time": now // 1_000_000 - 20}
    conflicting = {**original, "income": "-0.18"}
    record_binance_income(journal, identity, [original], received_at_ns=now)

    from atlas.persistence.sqlite import PersistenceError

    with pytest.raises(PersistenceError, match="conflicting economic event"):
        record_binance_income(journal, identity, [conflicting], received_at_ns=now + 1)

    expected_hashes = {
        normalize_binance_income(identity, [row], received_at_ns=now)[0][1].source_hash
        for row in (original, conflicting)
    }
    persisted_hashes = {
        row[0]
        for row in journal._conn.execute(
            "SELECT raw_hash FROM observations WHERE source='BINANCE_DEMO_INCOME'"
        )
    }
    assert persisted_hashes == expected_hashes
    assert journal.count("economic_events") == 1


def test_concurrent_income_replay_reuses_one_economic_row(tmp_path, monkeypatch):
    journal = SQLiteJournal(tmp_path / "concurrent-income.sqlite")
    identity = BinanceDemoIdentity("scope-a", "cred-a")
    now = 1_800_000_000_000_000_000
    row = {"tranId": 55, "incomeType": "FUNDING_FEE", "asset": "USDT", "income": "-0.17",
           "time": now // 1_000_000 - 20}
    normalized_barrier = threading.Barrier(2)
    import atlas.runtime.binance_reconciliation as reconciliation

    original_normalizer = reconciliation.normalize_binance_income
    original_loader = journal.load_economic_event

    def synchronized_normalizer(*args, **kwargs):
        result = original_normalizer(*args, **kwargs)
        normalized_barrier.wait(timeout=2)
        return result

    def delayed_loader(account, transaction_id):
        prior = original_loader(account, transaction_id)
        if prior is None:
            time.sleep(0.05)
        return prior

    monkeypatch.setattr(reconciliation, "normalize_binance_income", synchronized_normalizer)
    monkeypatch.setattr(journal, "load_economic_event", delayed_loader)
    with ThreadPoolExecutor(max_workers=2) as pool:
        calls = [
            pool.submit(record_binance_income, journal, identity, [row], received_at_ns=now + offset)
            for offset in (0, 1)
        ]
        results = [call.result(timeout=5) for call in calls]

    assert results[0] == results[1]
    assert journal.count("economic_events") == 1
    assert journal.count("observations") == 2


@pytest.mark.parametrize("field,value", [("id", None), ("id", True), ("orderId", []), ("qty", True),
                                         ("buyer", 1), ("commissionAsset", None)])
def test_trade_identity_and_numeric_fields_reject_coercion(tmp_path, field, value):
    journal = SQLiteJournal(tmp_path / "typed.sqlite")
    identity = BinanceDemoIdentity("scope-a", "cred-a")
    client_id = "00000000000000000000000000000001"
    _persist_trade_intent(journal, client_id)
    now = 1_800_000_000_000_000_000
    row = {"id": 77, "orderId": 101, "symbol": "BTCUSDT", "buyer": True, "qty": "0.2",
           "price": "50000", "commission": "0.1", "commissionAsset": "USDT",
           "time": now // 1_000_000 - 5, field: value}
    with pytest.raises(ValueError):
        record_binance_trades(journal, identity, [row], received_at_ns=now,
                             client_id_by_order_id={("BTCUSDT", "101"): client_id},
                             intent_id_by_client_id={client_id: "intent-1"})
    assert journal.load_execution_evidence() == []


def test_regular_cancel_missing_cumulative_quantity_is_not_zero_fill_proof(journal):
    client_id = "00000000000000000000000000000001"
    _persist_trade_intent(journal, client_id)
    with pytest.raises(ValueError, match="cumulative quantity missing"):
        record_binance_order_status(journal, {"symbol": "BTCUSDT", "orderId": 101,
            "clientOrderId": external_client_order_id(client_id), "status": "CANCELED"},
            received_at_ns=1_800_000_000_000_000_000, intent_id="intent-1",
            identity=BinanceDemoIdentity("scope-a", "cred-a"))
    assert journal.load_order_status_observations() == []


def test_documented_symbol_config_and_balance_alias_are_fresh_independent_evidence():
    now = 1_800_000_000_000_000_000
    identity = BinanceDemoIdentity("scope-a", "cred-a")
    responses = [
        {"totalWalletBalance": "100", "assets": [], "positions": []},
        {"dualSidePosition": False, "multiAssetsMargin": False, "canTrade": True},
        [{"symbol": "BTCUSDT", "positionSide": "BOTH", "positionAmt": "1", "isolatedMargin": "25"}],
        [{"symbol": "BTCUSDT", "marginType": "ISOLATED", "isAutoAddMargin": "false"}],
        [{"asset": "USDT", "accountAlias": "opaqueAlias", "balance": "100"}],
    ]
    snapshot = capture_account_snapshot(StubReader(identity, responses, now), now_ns=now)
    assert snapshot.eligible and snapshot.account_fingerprint_status == "VERIFIED_ACCOUNT_ALIAS_RECEIPT"
    assert snapshot.symbol_configs[0].as_dict()["marginType"] == "ISOLATED"
    assert "opaqueAlias" not in repr(snapshot) and "accountAlias" not in snapshot.balances[0].as_dict()
    changed = capture_account_snapshot(StubReader(identity, responses, now), now_ns=now,
                                      expected_account_fingerprint="a" * 64)
    assert not changed.eligible and "ACCOUNT_FINGERPRINT_CHANGED" in changed.reasons


def test_invented_position_flags_do_not_replace_real_symbol_config():
    now = 1_800_000_000_000_000_000
    identity = BinanceDemoIdentity("scope-a", "cred-a")
    snapshot = capture_account_snapshot(StubReader(identity, [
        {"canTrade": True, "multiAssetsMargin": False},
        {"dualSidePosition": False, "multiAssetsMargin": False, "canTrade": True},
        [{"symbol": "BTCUSDT", "positionSide": "BOTH", "positionAmt": "1",
          "marginType": "isolated", "isolated": True}],
        [], [{"asset": "USDT", "accountAlias": "opaqueAlias"}],
    ], now), now_ns=now)
    assert not snapshot.eligible and "POSITION_NOT_ISOLATED_ONE_WAY" in snapshot.reasons


class HistoryReader(StubReader):
    def __init__(self, identity, responses, now_ns):
        super().__init__(identity, responses, now_ns)
        self.calls = []

    def read_with_receipt(self, path, params=None):
        self.calls.append((path, params))
        response = super().read_with_receipt(path)
        if isinstance(response.payload, Exception):
            raise response.payload
        return response


def test_scoped_signed_history_persists_fill_status_income_without_resolving_unknown(journal):
    now = 1_800_000_000_000_000_000
    identity = BinanceDemoIdentity("scope-a", "cred-a")
    client_id = "00000000000000000000000000000001"
    _persist_trade_intent(journal, client_id)
    journal.mark_send_started("cmd-1", now)
    reader = HistoryReader(identity, [
        {"symbol": "BTCUSDT", "orderId": 101, "clientOrderId": external_client_order_id(client_id),
         "status": "CANCELED", "executedQty": "0.2", "avgPrice": "50000", "cumQuote": "10000"},
        {"symbol": "BTCUSDT", "algoId": 202, "clientAlgoId": external_client_order_id(client_id),
         "algoStatus": "NEW", "orderType": "STOP_MARKET"},
        [{"id": 77, "orderId": 101, "symbol": "BTCUSDT", "side": "BUY", "qty": "0.2",
          "price": "50000", "commission": "0.1", "commissionAsset": "USDT", "time": now // 1_000_000}],
        [{"symbol": "BTCUSDT", "tranId": 55, "incomeType": "FUNDING_FEE", "asset": "USDT",
          "income": "-0.1", "time": now // 1_000_000}],
    ], now)
    result = reconcile_binance_execution_history(reader, journal, max_intents=1)
    assert result.processed_command_ids == ("cmd-1",) and len(result.query_ids) == 4
    assert not result.complete_for_recovery and result.reasons == ()
    assert journal.load_command("cmd-1").outcome.value == "UNKNOWN"
    assert journal.load_execution_evidence()[0].qty == Decimal("0.2")
    assert journal.load_economic_event(identity.account_scope_ref, "55").amount == Decimal("-0.1")
    evidence = journal.load_reconciliation_query_evidence()
    assert all(item.hash_binds_payload and not item.can_certify_absence for item in evidence)
    assert all(item.facts["opening_protection_qualified"] is False for item in evidence)
    assert all("symbol" in params or path.endswith("algoOrder") for path, params in reader.calls)


def test_missing_lookup_and_budget_refusal_are_persisted_incomplete_without_negative_proof(journal):
    from atlas.runtime.binance_demo import DemoReadError

    now = 1_800_000_000_000_000_000
    identity = BinanceDemoIdentity("scope-a", "cred-a")
    _persist_trade_intent(journal)
    journal.mark_send_started("cmd-1", now)

    class RefusingReader:
        def clock_ns(self):
            return now

        def __init__(self):
            self.identity = identity

        def read_with_receipt(self, *_args):
            raise DemoReadError("signature=must-never-appear")

    result = reconcile_binance_execution_history(RefusingReader(), journal, max_intents=1)
    assert len(result.query_ids) == 4 and result.reasons == ("BINANCE_HISTORY_READ_OR_ASSOCIATION_INCOMPLETE",)
    evidence = journal.load_reconciliation_query_evidence()
    assert all(not item.can_certify_absence and item.pages_observed == 0 for item in evidence)
    assert "must-never-appear" not in repr(evidence)
    assert journal.load_command("cmd-1").outcome.value == "UNKNOWN"


def test_bounded_history_pass_does_not_query_unsent_commands_and_rotates_cursor(journal):
    now = 1_800_000_000_000_000_000
    identity = BinanceDemoIdentity("scope-a", "cred-a")
    _persist_trade_intent(journal)
    reader = HistoryReader(identity, [], now)
    result = reconcile_binance_execution_history(reader, journal, max_intents=1)
    assert result.processed_command_ids == () and result.query_ids == ()
    assert result.next_command_id == "cmd-1" and reader.calls == []
    for maximum in (0, 33, True):
        with pytest.raises(ValueError, match="intent bound"):
            reconcile_binance_execution_history(reader, journal, max_intents=maximum)


def _recovery_responses(*, regular_history=None, trades=None, open_algos=None, position_amount="0"):
    positions = [] if position_amount == "0" else [{
        "symbol": "BTCUSDT", "positionSide": "BOTH", "positionAmt": position_amount,
        "marginType": "ISOLATED", "isolated": True,
    }]
    return [regular_history or [], [], trades or [], [], [],
        {"totalWalletBalance": "100", "assets": [], "positions": []},
        {"canTrade": True, "dualSidePosition": False, "multiAssetsMargin": False},
        positions, [{"symbol": "BTCUSDT", "marginType": "ISOLATED"}],
        [{"asset": "USDT", "accountAlias": "simulated-account-alias", "balance": "100"}],
        open_algos or [],
    ]


def _persist_repair_stop(journal, identity, now):
    client_id = "00000000000000000000000000000002"
    make_plan(journal, plan_id="repair-plan")
    intent = Intent("repair-intent", 1, "repair-plan", "v1", client_id, 1,
                    LifecycleState.SUBMITTING, ProtectionStatus.UNCONFIRMED,
                    ReconciliationHealth.STALE, now - 1)
    reservation = Reservation("repair-reservation", intent.intent_id, Decimal("1"), Decimal("0"),
                              Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"))
    journal.create_intent_with_reservation(intent, reservation)
    command = make_command(
        command_id="repair-stop-command", intent_id=intent.intent_id,
        command_type=CommandType.REPAIR_STOP,
        payload_dict={"identity_hash": identity.content_hash, "instrument_ref": "a" * 64,
                      "symbol": "BTCUSDT", "client_order_id": client_id, "side": "SELL",
                      "quantity": "1", "price": None, "stop": "50000", "reduce_only": True},
        expected_state_version=0, created_at_ns=now - 1,
    )
    journal.persist_command(command)
    journal.mark_send_started(command.command_id, now)
    return client_id


def _simulated_source_qualification(journal, identity, now, *, state="PASSED_TESTNET", environment=None):
    """Temporary test-only external qualification simulation, never live evidence."""
    from atlas.runtime.capability_ledger import CapabilityEvidence, EvidenceState, QualificationRecord
    from atlas.runtime.reconciliation_evidence import (
        Completeness,
        QueryScope,
        QueryStatus,
        QueryType,
        make_query_evidence,
    )

    refs = []
    for query_type, endpoints in (
        (QueryType.ORDER_HISTORY, ["/fapi/v1/allOrders", "/fapi/v1/allAlgoOrders"]),
        (QueryType.EXECUTION_HISTORY, ["/fapi/v1/userTrades"]),
        (QueryType.TRANSACTION_LOG, ["/fapi/v1/income"]),
    ):
        query = make_query_evidence(
            query_id=f"simulated-source-{query_type.value}", query_type=query_type,
            scope=QueryScope.ACCOUNT if query_type == QueryType.TRANSACTION_LOG else QueryScope.INSTRUMENT,
            account=identity.account_scope_ref,
            instrument=None if query_type == QueryType.TRANSACTION_LOG else "BTCUSDT-PERP.BINANCE",
            requested_interval_start_ns=now - 1_000_000_000, requested_interval_end_ns=now,
            pagination_cursors=(), pages_observed=1, total_records_returned=0,
            completeness=Completeness.COMPLETE, status=QueryStatus.SUCCESS, source_time_ns=None,
            receipt_time_ns=now, request_ids=(), retention_segments=((now - 1_000_000_000, now),),
            facts={"capability_name": BinanceRecoveryCapabilityLedgerV1.NAME,
                   "venue_identity_hash": identity.content_hash, "environment": identity.environment,
                   "receipt_source": "SIGNED_BINANCE_DEMO_READER", "simulation_only": True,
                   "response_hashes": ["f" * 64], "endpoints": endpoints}, error_message=None,
        )
        journal.append_reconciliation_query_evidence(query)
        refs.append(f"query:{query.query_id}:{query.evidence_hash}")
    refs = tuple(refs)
    journal.append_capability_evidence(CapabilityEvidence(
        BinanceRecoveryCapabilityLedgerV1.NAME, EvidenceState(state), "simulation-only", refs, now,
        environment or identity.environment, "TEST FIXTURE ONLY: simulated external source qualification",
        identity.content_hash,
    ))
    journal.append_capability_qualification(QualificationRecord(
        "simulation-qualification", BinanceRecoveryCapabilityLedgerV1.NAME, EvidenceState.TEST_GATE_TESTNET,
        EvidenceState.PASSED_TESTNET, "simulation-only", refs, "simulation-test-fixture", now,
        identity.content_hash,
    ))


def _capture_cycle(journal, reader, now):
    return capture_binance_recovery_cycle(reader, journal, symbol="BTCUSDT", writer_id="writer-fixture",
        writer_epoch=1, runtime_instance_id="simulation-runtime", position_epoch=1,
        history_start_ns=now - 1_000_000_000, assert_writer=lambda: None, assert_quiescent=lambda: None)


def test_unqualified_recovery_retains_history_gate_and_zero_authority_flat_sidecar(journal):
    from atlas.runtime.binance_demo import opening_gate_reason

    now = 1_800_000_000_000_000_000
    identity = BinanceDemoIdentity("scope-a", "cred-a")
    result = _capture_cycle(journal, HistoryReader(identity, _recovery_responses(), now), now)
    assert not result.complete_for_recovery
    assert "BINANCE_RECOVERY_SOURCE_RETENTION_UNQUALIFIED" in result.reasons
    assert result.flat_observation is not None
    assert result.flat_observation.capital_enabled is False
    assert result.flat_observation.can_release_reservation is False
    assert result.flat_observation.can_admit_opening is False
    assert opening_gate_reason().startswith("TEST GATE:")
    run_queries = journal.load_run_queries(result.reconciliation_run_id)
    assert all(query.hash_binds_payload for query in run_queries)
    assert all(not query.can_certify_absence for query in run_queries
               if query.query_type.value in {"order_history", "execution_history"})


def test_simulated_qualified_recovery_captures_complete_flat_inputs_without_capital(journal):
    now = 1_800_000_000_000_000_000
    identity = BinanceDemoIdentity("scope-a", "cred-a")
    _simulated_source_qualification(journal, identity, now)
    reader = HistoryReader(identity, _recovery_responses(), now)
    result = _capture_cycle(journal, reader, now)
    assert result.complete_for_recovery and result.reasons == ()
    assert result.flat_observation is not None and len(result.flat_observation.source_capability_refs) == 3
    assert not result.flat_observation.capital_enabled
    open_requests = [(path, params) for path, params in reader.calls if path.endswith(("openOrders", "openAlgoOrders"))]
    assert all(params is None for _path, params in open_requests)


@pytest.mark.parametrize("state,environment", [("TESTED_OFFLINE", None), ("PASSED_TESTNET", "TESTNET")])
def test_offline_or_wrong_environment_capability_cannot_complete_actual_recovery(journal, state, environment):
    now = 1_800_000_000_000_000_000
    identity = BinanceDemoIdentity("scope-a", "cred-a")
    _simulated_source_qualification(journal, identity, now, state=state, environment=environment)
    result = _capture_cycle(journal, HistoryReader(identity, _recovery_responses(), now), now)
    assert not result.complete_for_recovery
    assert "BINANCE_RECOVERY_SOURCE_RETENTION_UNQUALIFIED" in result.reasons


def test_complete_inputs_preserve_partial_fill_cancel_race_unknown_command_and_reservation(journal):
    now = 1_800_000_000_000_000_000
    identity = BinanceDemoIdentity("scope-a", "cred-a")
    client_id = "00000000000000000000000000000001"
    _persist_trade_intent(journal, client_id)
    journal.mark_send_started("cmd-1", now)
    _simulated_source_qualification(journal, identity, now)
    order = {"symbol": "BTCUSDT", "orderId": 101, "clientOrderId": external_client_order_id(client_id),
             "status": "CANCELED", "executedQty": "0.2", "avgPrice": "50000", "cumQuote": "10000"}
    trade = {"id": 77, "orderId": 101, "symbol": "BTCUSDT", "side": "BUY", "qty": "0.2",
             "price": "50000", "commission": "0.1", "commissionAsset": "USDT", "time": now // 1_000_000}
    result = _capture_cycle(journal, HistoryReader(identity, _recovery_responses(
        regular_history=[order], trades=[trade]), now), now)
    assert result.complete_for_recovery and result.flat_observation is None
    assert journal.load_command("cmd-1").outcome.value == "UNKNOWN"
    assert journal.load_execution_evidence()[0].qty == Decimal("0.2")
    assert journal.load_order_status_observations()[0].cum_exec_qty == Decimal("0.2")
    assert journal.count("reservations") == 1


def test_orphan_conditional_on_other_symbol_blocks_current_flat_sidecar(journal):
    now = 1_800_000_000_000_000_000
    identity = BinanceDemoIdentity("scope-a", "cred-a")
    _simulated_source_qualification(journal, identity, now)
    orphan = {"symbol": "ETHUSDT", "algoId": 909, "clientAlgoId": "external-stop", "algoStatus": "NEW"}
    result = _capture_cycle(journal, HistoryReader(identity, _recovery_responses(open_algos=[orphan]), now), now)
    assert result.complete_for_recovery and result.flat_observation is None
    conditional = next(query for query in journal.load_run_queries(result.reconciliation_run_id)
                       if query.query_type.value == "conditional_orders")
    assert conditional.total_records_returned == 1
    assert conditional.facts["is_current_protection"] is False


@pytest.mark.parametrize("trigger,verified", [("50000", True), ("49000", False)])
def test_recovery_binds_repair_stop_readback_to_current_position_and_command(journal, trigger, verified):
    now = 1_800_000_000_000_000_000
    identity = BinanceDemoIdentity("scope-a", "cred-a")
    client_id = _persist_repair_stop(journal, identity, now)
    _simulated_source_qualification(journal, identity, now)
    row = {"symbol": "BTCUSDT", "algoId": 909, "clientAlgoId": external_client_order_id(client_id),
           "algoStatus": "NEW", "orderType": "STOP_MARKET", "workingType": "MARK_PRICE",
           "positionSide": "BOTH", "side": "SELL", "closePosition": True,
           "triggerPrice": trigger}
    result = _capture_cycle(journal, HistoryReader(identity, _recovery_responses(
        open_algos=[row], position_amount="1"), now), now)
    trading_stop = next(query for query in journal.load_run_queries(result.reconciliation_run_id)
                        if query.query_type.value == "trading_stop")
    readback, = trading_stop.facts["protection_readbacks"]
    assert readback["verified"] is verified
    assert readback["command_id"] == "repair-stop-command"
    assert readback.get("algo_order_id") == ("909" if verified else None)
    assert journal.load_command("repair-stop-command").outcome.value == "UNKNOWN"
    assert journal.count("reservations") == 1
    assert ("BINANCE_REPAIR_STOP_READBACK_UNCONFIRMED" in result.reasons) is (not verified)


def test_recovery_does_not_verify_stop_against_stale_position_receipt(journal):
    now = 1_800_000_000_000_000_000
    identity = BinanceDemoIdentity("scope-a", "cred-a")
    client_id = _persist_repair_stop(journal, identity, now)
    _simulated_source_qualification(journal, identity, now)
    row = {"symbol": "BTCUSDT", "algoId": 909, "clientAlgoId": external_client_order_id(client_id),
           "algoStatus": "NEW", "orderType": "STOP_MARKET", "workingType": "MARK_PRICE",
           "positionSide": "BOTH", "side": "SELL", "closePosition": True, "triggerPrice": "50000"}

    class DelayedAlgoReader(HistoryReader):
        def __init__(self):
            super().__init__(identity, _recovery_responses(open_algos=[row], position_amount="1"), now)
            self.current = now
            self.clock_ns = lambda: self.current

        def read_with_receipt(self, path, params=None):
            if path == "/fapi/v1/openAlgoOrders":
                self.current += 3_000_000_000
            return super().read_with_receipt(path, params)

    result = _capture_cycle(journal, DelayedAlgoReader(), now)
    trading_stop = next(query for query in journal.load_run_queries(result.reconciliation_run_id)
                        if query.query_type.value == "trading_stop")
    readback, = trading_stop.facts["protection_readbacks"]
    assert not readback["verified"]
    assert readback["reason"] == "REPAIR_POSITION_RECEIPT_STALE"


def test_real_signed_reader_paces_recovery_and_keeps_every_current_receipt_fresh(journal):
    from urllib.parse import urlsplit

    from atlas.runtime.binance_demo import BinanceDemoReader, DemoCredential

    now = 1_800_000_000_000_000_000
    identity = BinanceDemoIdentity("scope-a", "cred-a")
    _simulated_source_qualification(journal, identity, now)
    current = [now]
    payloads = iter(_recovery_responses())
    requests = []
    sleeps = []

    class Response:
        def __init__(self, request):
            self.request = request
            self.body = json.dumps(next(payloads)).encode()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def geturl(self):
            return self.request.full_url

        def read(self, _limit):
            current[0] += 20_000_000
            return self.body

    class Opener:
        def open(self, request, *, timeout):
            assert request.method == "GET" and timeout == 2.0
            requests.append((urlsplit(request.full_url).path, current[0]))
            return Response(request)

    def sleep(seconds):
        sleeps.append(seconds)
        current[0] += round(seconds * 1_000_000_000)

    reader = BinanceDemoReader(identity=identity, credential=DemoCredential("fixture-key", "fixture-secret"),
        clock_ns=lambda: current[0], monotonic_ns=lambda: current[0], sleep=sleep, opener=Opener())
    result = _capture_cycle(journal, reader, now)
    assert result.complete_for_recovery and result.reasons == ()
    assert len(requests) == 11 and sleeps
    assert all(seconds <= 0.1 for seconds in sleeps)
    assert sum(weight for _at, weight in reader._requests) == 150
    for at, _weight in reader._requests:
        assert sum(weight for other, weight in reader._requests if 0 <= at - other < 1_000_000_000) <= 60
    queries = journal.load_run_queries(result.reconciliation_run_id)
    historical = [q for q in queries if not q.facts["account_wide_current_view"]]
    latest = [q for q in queries if q.facts["account_wide_current_view"]]
    assert current[0] - historical[0].receipt_time_ns > 2_000_000_000
    assert all(0 <= current[0] - q.receipt_time_ns <= 2_000_000_000 for q in latest)
    assert result.flat_observation is not None and not result.flat_observation.capital_enabled


def test_slow_final_current_read_blocks_completion_and_flat_sidecar(journal):
    now = 1_800_000_000_000_000_000
    identity = BinanceDemoIdentity("scope-a", "cred-a")
    _simulated_source_qualification(journal, identity, now)

    class SlowReader(HistoryReader):
        def __init__(self):
            super().__init__(identity, _recovery_responses(), now)
            self.current = now
            self.clock_ns = lambda: self.current

        def read_with_receipt(self, path, params=None):
            if path == "/fapi/v1/openAlgoOrders":
                self.current += 2_000_000_001
            receipt = super().read_with_receipt(path, params)
            receipt.received_at_ns = self.current
            return receipt

    result = _capture_cycle(journal, SlowReader(), now)
    assert not result.complete_for_recovery and result.flat_observation is None
    assert "BINANCE_RECOVERY_CYCLE_RECEIPTS_STALE" in result.reasons


def test_quiescence_revocation_during_budget_wait_refuses_all_later_reads(journal):
    from atlas.persistence.sqlite import PersistenceError

    now = 1_800_000_000_000_000_000
    identity = BinanceDemoIdentity("scope-a", "cred-a")
    active = [True]

    class RevokedReader(HistoryReader):
        def wait_for_read_budget(self, path, params, *, max_wait_ns, assert_active):
            active[0] = False
            assert_active()

    reader = RevokedReader(identity, _recovery_responses(), now)

    def quiescent():
        if not active[0]:
            raise PersistenceError("quiescence revoked")

    with pytest.raises(PersistenceError, match="quiescence revoked"):
        capture_binance_recovery_cycle(reader, journal, symbol="BTCUSDT", writer_id="writer",
            writer_epoch=1, runtime_instance_id="runtime", position_epoch=1,
            history_start_ns=now - 1_000_000_000, assert_writer=lambda: None, assert_quiescent=quiescent)
    assert reader.calls == []
