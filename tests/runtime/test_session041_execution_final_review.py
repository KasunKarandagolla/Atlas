"""Independent offline review of S41 execution safety boundaries."""
from __future__ import annotations

import hashlib
import io
import json
import queue
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from conftest import T0, add_intent

from atlas.domain.enums import CommandOutcome, CommandType
from atlas.persistence.sqlite import PersistenceError
from atlas.runtime.binance_demo import (
    BinanceDemoIdentity,
    BinanceDemoReader,
    DemoCredential,
    DemoReadError,
    dispatch_persisted_demo_command,
)
from atlas.runtime.binance_demo import (
    opening_gate_reason as binance_opening_gate,
)
from atlas.runtime.binance_native import BinanceNativeEvent, BinanceNativeNode
from atlas.runtime.binance_reconciliation import record_binance_order_status, record_binance_trades
from atlas.runtime.bybit_demo import (
    BybitDemoIdentity,
    BybitDemoReader,
    BybitDemoReadError,
    BybitDemoReadReceipt,
    capture_bybit_account_snapshot,
    dispatch_persisted_bybit_command,
)
from atlas.runtime.bybit_demo import (
    opening_gate_reason as bybit_opening_gate,
)


@pytest.mark.parametrize("dispatcher,gate", [
    (dispatch_persisted_demo_command, binance_opening_gate),
    (dispatch_persisted_bybit_command, bybit_opening_gate),
])
def test_opening_cannot_mark_send_or_reach_effect(journal, dispatcher, gate):
    intent = add_intent(journal)
    command = journal.prepare_dispatch(
        intent_id=intent.intent_id, expected_state_version=0, expected_reservation_version=1,
        command_id="review-open", command_type=CommandType.SUBMIT_ENTRY,
        payload_dict={"client_order_id": intent.client_order_id}, created_at_ns=T0,
    )
    port = Mock()
    with pytest.raises(PersistenceError, match="UNQUALIFIED"):
        dispatcher(journal=journal, command_id=command.command_id, port=port,
                   now_ns=T0 + 1, writer_epoch=1, assert_writer=lambda: None)
    assert gate().startswith("TEST GATE:")
    assert journal.load_command(command.command_id).outcome == CommandOutcome.UNSENT
    assert journal.load_command(command.command_id).send_started_at_ns is None
    port.dispatch.assert_not_called()


@pytest.mark.parametrize("dispatcher", [dispatch_persisted_demo_command, dispatch_persisted_bybit_command])
@pytest.mark.parametrize("failure", [None, TimeoutError, ValueError])
def test_ack_loss_duplicate_error_and_local_enqueue_never_authorize_resend(journal, dispatcher, failure):
    intent = add_intent(journal)
    before = journal.load_reservation(intent.intent_id)
    command = journal.prepare_dispatch(
        intent_id=intent.intent_id, expected_state_version=0, expected_reservation_version=1,
        command_id="review-close", command_type=CommandType.SUBMIT_EXIT,
        payload_dict={"client_order_id": intent.client_order_id}, created_at_ns=T0,
    )
    seen = []

    def effect(command, *, now_ns):
        durable = journal.load_command(command.command_id)
        assert durable.outcome == CommandOutcome.UNKNOWN
        assert durable.send_started_at_ns == now_ns
        seen.append(command.command_id)
        if failure:
            raise failure("fixture sensitive transport detail")

    args = {"journal": journal, "command_id": command.command_id,
            "port": SimpleNamespace(dispatch=effect), "now_ns": T0 + 1,
            "writer_epoch": 1, "assert_writer": lambda: None}
    assert dispatcher(**args).outcome == CommandOutcome.UNKNOWN
    with pytest.raises(PersistenceError):
        dispatcher(**args)
    assert seen == [command.command_id]
    assert journal.load_reservation(intent.intent_id).remaining_open_qty == before.remaining_open_qty


def _receipt(identity_hash, endpoint, result):
    canonical = json.dumps({"retCode": 0, "result": result}, sort_keys=True)
    digest = hashlib.sha256(canonical.encode()).hexdigest()
    return BybitDemoReadReceipt(identity_hash, endpoint, T0, digest, digest, canonical, T0)


@pytest.mark.parametrize("corruption", ["identity", "endpoint"])
def test_bybit_current_profile_rejects_receipts_from_foreign_scope(corruption):
    identity = BybitDemoIdentity("review-scope", "review-reference")
    credential = DemoCredential("offline-review-key", "offline-review-secret")

    class Reader:
        _credential = credential
        clock_ns = staticmethod(lambda: T0)

        def read_with_receipt(self, path, params=None):
            result = ({"unifiedMarginStatus": 5, "marginMode": "ISOLATED_MARGIN"}
                      if path == "/v5/account/info" else {"list": [{"accountType": "UNIFIED"}]})
            return _receipt("f" * 64 if corruption == "identity" else identity.content_hash,
                            "/v5/order/history" if corruption == "endpoint" else path, result)

        def read_pages(self, path, params=None):
            return (_receipt(identity.content_hash, path, {"list": [
                {"symbol": "SOLUSDT", "positionIdx": 0, "size": "1", "side": "Buy"}]}),)

    reader = Reader()
    reader.identity = identity
    try:
        snapshot = capture_bybit_account_snapshot(reader, native_symbol="SOLUSDT")
    except (ValueError, BybitDemoReadError):
        return
    assert not snapshot.eligible
    assert not snapshot.profile_qualified


@pytest.mark.parametrize("foreign_field", ["identity_hash", "instrument_ref"])
def test_binance_native_fill_rejects_foreign_durable_account_or_product(journal, foreign_field):
    intent = add_intent(journal)
    identity = BinanceDemoIdentity("review-scope", "review-reference")
    product_ref = "e" * 64
    values = {"client_order_id": intent.client_order_id, "symbol": "SOLUSDT",
              "identity_hash": identity.content_hash, "instrument_ref": product_ref}
    values[foreign_field] = "f" * 64
    journal.prepare_dispatch(
        intent_id=intent.intent_id, expected_state_version=0, expected_reservation_version=1,
        command_id="review-close", command_type=CommandType.SUBMIT_EXIT,
        payload_dict=values, created_at_ns=T0,
    )
    # Exercise the event persistence boundary without constructing a venue node.
    runtime = BinanceNativeNode.__new__(BinanceNativeNode)
    runtime.identity = identity
    runtime.journal = journal
    runtime.assert_writer = lambda: None
    runtime._product = SimpleNamespace(key=SimpleNamespace(native_symbol="SOLUSDT"), content_hash=product_ref)
    runtime.event_queue = queue.Queue()
    runtime.stopped = False
    runtime.last_failure_code = None
    runtime.event_queue.put(BinanceNativeEvent(
        "OrderFilled", intent.client_order_id, "101", "SOLUSDT-PERP.BINANCE", "FILLED",
        "0.01", "120", T0 + 1, trade_id="77", side="Buy", fee="0.01",
        fee_currency="USDT", trade_time_ns=T0, cumulative_quantity="0.01", average_price="120",
    ))
    runtime.drain_events_to_journal()
    assert runtime.stopped
    assert journal.load_execution_evidence() == []
    assert journal.load_order_status_observations() == []
    assert journal.load_reservation(intent.intent_id).remaining_open_qty == Decimal("0.01")


def test_binance_rest_fill_rejects_foreign_durable_account(journal):
    intent = add_intent(journal)
    identity = BinanceDemoIdentity("review-scope", "review-reference")
    journal.prepare_dispatch(
        intent_id=intent.intent_id, expected_state_version=0, expected_reservation_version=1,
        command_id="review-close", command_type=CommandType.SUBMIT_EXIT,
        payload_dict={"client_order_id": intent.client_order_id, "symbol": "SOLUSDT",
                      "identity_hash": "f" * 64, "instrument_ref": "e" * 64}, created_at_ns=T0,
    )
    row = {"id": 77, "orderId": 101, "symbol": "SOLUSDT", "side": "BUY",
           "qty": "0.01", "price": "120", "commission": "0.01",
           "commissionAsset": "USDT", "time": T0 // 1_000_000}
    with pytest.raises(ValueError):
        record_binance_trades(
            journal, identity, [row], received_at_ns=T0 + 1,
            client_id_by_order_id={("SOLUSDT", "101"): intent.client_order_id},
            intent_id_by_client_id={intent.client_order_id: intent.intent_id},
        )
    assert journal.load_execution_evidence() == []


@pytest.mark.parametrize("foreign_first", [False, True])
def test_binance_rest_batch_validates_all_accounts_before_first_write(journal, foreign_first):
    identity = BinanceDemoIdentity("review-scope", "review-reference")
    intents = [add_intent(journal, "review-valid"), add_intent(journal, "review-foreign")]
    rows = []
    associations = {}
    for index, intent in enumerate(intents):
        journal.prepare_dispatch(
            intent_id=intent.intent_id, expected_state_version=0, expected_reservation_version=1,
            command_id="review-close-" + str(index), command_type=CommandType.SUBMIT_EXIT,
            payload_dict={"client_order_id": intent.client_order_id, "symbol": "SOLUSDT",
                          "identity_hash": identity.content_hash if index == 0 else "f" * 64,
                          "instrument_ref": "e" * 64}, created_at_ns=T0,
        )
        rows.append({"id": 77 + index, "orderId": 101 + index, "symbol": "SOLUSDT",
                     "side": "BUY", "qty": "0.01", "price": "120", "commission": "0.01",
                     "commissionAsset": "USDT", "time": T0 // 1_000_000})
        associations[("SOLUSDT", str(101 + index))] = intent.client_order_id
    if foreign_first:
        rows.reverse()
    with pytest.raises(ValueError):
        record_binance_trades(
            journal, identity, rows, received_at_ns=T0 + 1,
            client_id_by_order_id=associations,
            intent_id_by_client_id={intent.client_order_id: intent.intent_id for intent in intents},
        )
    assert journal.load_execution_evidence() == []


def test_binance_rest_order_status_rejects_foreign_durable_account_before_write(journal):
    intent = add_intent(journal)
    identity = BinanceDemoIdentity("review-scope", "review-reference")
    journal.prepare_dispatch(
        intent_id=intent.intent_id, expected_state_version=0, expected_reservation_version=1,
        command_id="review-close", command_type=CommandType.SUBMIT_EXIT,
        payload_dict={"client_order_id": intent.client_order_id, "symbol": "SOLUSDT",
                      "identity_hash": "f" * 64, "instrument_ref": "e" * 64}, created_at_ns=T0,
    )
    with pytest.raises(ValueError):
        record_binance_order_status(
            journal, {"orderId": 101, "clientOrderId": intent.client_order_id,
                      "symbol": "SOLUSDT", "status": "CANCELED", "executedQty": "0"},
            received_at_ns=T0 + 1, intent_id=intent.intent_id, identity=identity,
        )
    assert journal.load_order_status_observations() == []


@pytest.mark.parametrize("reader_type,identity_type,error_type,path", [
    (BinanceDemoReader, BinanceDemoIdentity, DemoReadError, "/fapi/v1/order"),
    (BybitDemoReader, BybitDemoIdentity, BybitDemoReadError, "/v5/account/info"),
])
@pytest.mark.parametrize("environment", ["DEMO", "TESTNET"])
def test_authenticated_reader_uses_get_exact_environment_and_redacts_server_echo(
    reader_type, identity_type, error_type, path, environment,
):
    credential = DemoCredential("offline-review-key", "offline-review-secret")
    seen = []

    class Response(io.BytesIO):
        def geturl(self):
            return seen[0].full_url

    class Opener:
        def open(self, request, *, timeout):
            seen.append(request)
            body = {"retCode": 0, "result": {}, "server_echo": credential.api_secret}
            return Response(json.dumps(body).encode())

    reader = reader_type(identity=identity_type("scope", "reference", environment),
                         credential=credential, clock_ns=lambda: T0, opener=Opener())
    with pytest.raises(error_type) as caught:
        reader.read_with_receipt(path)
    assert seen[0].method == "GET"
    assert seen[0].full_url.startswith(reader._base_url + path)
    assert ("testnet" in reader._base_url) == (environment == "TESTNET")
    assert credential.api_key not in str(caught.value)
    assert credential.api_secret not in str(caught.value)
    assert "offline-review" not in repr(credential)
