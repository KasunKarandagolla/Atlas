from __future__ import annotations

import hashlib
import json
from decimal import Decimal

from conftest import T0, add_intent

from atlas.domain.enums import CommandType
from atlas.domain.execution import make_command
from atlas.runtime.bybit_demo import (
    BybitDemoIdentity,
    BybitDemoReadReceipt,
    BybitDemoSnapshot,
    persist_bybit_product_contract_snapshot,
    reconcile_bybit_commands,
)
from atlas.v2._serialization import sha256_json
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)


def _receipt(identity_hash: str, endpoint: str, result: dict, at_ns: int) -> BybitDemoReadReceipt:
    body = json.dumps({"retCode": 0, "result": result}, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(body.encode()).hexdigest()
    return BybitDemoReadReceipt(identity_hash, endpoint, at_ns, digest, digest, body, at_ns - 1)


def _snapshot(identity_hash: str) -> BybitDemoSnapshot:
    info = _receipt(identity_hash, "/v5/account/info", {"unifiedMarginStatus": 3,
        "marginMode": "ISOLATED_MARGIN"}, T0 + 1)
    wallet = _receipt(identity_hash, "/v5/account/wallet-balance", {"list": [
        {"accountType": "UNIFIED"}]}, T0 + 2)
    position = _receipt(identity_hash, "/v5/position/list", {"list": [
        {"symbol": "SOLUSDT", "positionIdx": 0, "size": "0", "side": ""}]}, T0 + 3)
    return BybitDemoSnapshot(identity_hash, T0 + 1, True, (), "f" * 64,
        "BYBIT_CURRENT_PROFILE_QUALIFIED", (info, wallet, position), positions=(position,),
        profile_qualified=True, native_symbol="SOLUSDT")


def _product(step: str = "0.01") -> ProductContractV2:
    metadata = {"symbol": "SOLUSDT", "contractType": "LinearPerpetual", "quoteCoin": "USDT",
        "settleCoin": "USDT", "baseCoin": "SOL", "status": "Trading",
        "priceFilter": {"tickSize": "0.01"},
        "lotSizeFilter": {"qtyStep": step, "minOrderQty": "0.01", "maxOrderQty": "100"}}
    revision = sha256_json(metadata)
    key = InstrumentKeyV2(VenueV2.BYBIT, EnvironmentV2.DEMO, ProductTypeV2.LINEAR_PERPETUAL,
        "SOLUSDT", "SOL", "USDT", "USDT", revision)
    return ProductContractV2(key, T0, T0, T0, Decimal("1"), Decimal("0.01"), Decimal(step),
        Decimal("0.01"), TradingStatusV2.TRADING, revision, max_qty=Decimal("100"))


def _metadata_receipt(identity_hash: str, step: str = "0.01") -> BybitDemoReadReceipt:
    payload = {"symbol": "SOLUSDT", "contractType": "LinearPerpetual", "quoteCoin": "USDT",
        "settleCoin": "USDT", "baseCoin": "SOL", "status": "Trading",
        "priceFilter": {"tickSize": "0.01"},
        "lotSizeFilter": {"qtyStep": step, "minOrderQty": "0.01", "maxOrderQty": "100"}}
    return _receipt(identity_hash, "/v5/market/instruments-info", {"list": [payload]}, T0 + 4)


def _sent_command(journal, identity: BybitDemoIdentity, product: ProductContractV2):
    intent = add_intent(journal, "bybit-recovery-intent")
    command = make_command(command_id="bybit-recovery-command", intent_id=intent.intent_id,
        command_type=CommandType.SUBMIT_EXIT,
        payload_dict={"identity_hash": identity.content_hash, "instrument_ref": product.content_hash,
            "symbol": "SOLUSDT", "client_order_id": intent.client_order_id,
            "side": "SELL", "quantity": "0.01", "price": None, "stop": None,
            "reduce_only": True}, expected_state_version=0, created_at_ns=T0)
    journal.persist_command(command)
    journal.mark_send_started(command.command_id, T0 + 1)
    return intent, command


class Reader:
    def __init__(self, identity: BybitDemoIdentity, *, order_row=None, execution_rows=(),
                 wrong_link=False, fail_execution=False, open_order_rows=(),
                 realtime_order_rows=(), history_order_rows=None):
        self.identity = identity
        self.clock_ns = lambda: T0 + 20_000_000
        self.order_row = order_row
        self.execution_rows = execution_rows
        self.wrong_link = wrong_link
        self.fail_execution = fail_execution
        self.open_order_rows = tuple(open_order_rows)
        self.realtime_order_rows = tuple(realtime_order_rows)
        self.history_order_rows = (tuple(history_order_rows) if history_order_rows is not None
                                   else (() if order_row is None else (order_row,)))

    def read_pages(self, path, params=None):
        params = dict(params or {})
        if path == "/v5/order/realtime":
            rows = list(self.realtime_order_rows if "orderLinkId" in params else self.open_order_rows)
        elif path == "/v5/order/history":
            rows = [dict(row) for row in self.history_order_rows]
            if self.wrong_link and rows:
                rows[0]["orderLinkId"] = "e" * 32
        elif path == "/v5/execution/list":
            if self.fail_execution:
                raise RuntimeError("private fixture detail")
            rows = list(self.execution_rows)
        else:
            raise AssertionError(path)
        receipt = _receipt(self.identity.content_hash, path, {"list": rows}, T0 + 10_000_000)
        return (receipt,)


def _reconcile(journal, reader, identity, product):
    persist_bybit_product_contract_snapshot(journal, identity=identity,
        snapshot=_snapshot(identity.content_hash), product=product,
        instrument_metadata_receipt=_metadata_receipt(identity.content_hash, str(product.qty_step)),
        assert_writer=lambda: None)
    return reconcile_bybit_commands(reader, journal, native_symbol="SOLUSDT",
        snapshot=_snapshot(identity.content_hash), product=product,
        instrument_metadata_receipt=_metadata_receipt(identity.content_hash, str(product.qty_step)),
        assert_writer=lambda: None)


def test_bybit_restart_recovery_persists_exact_order_state_without_resolving_unknown(journal):
    identity = BybitDemoIdentity("scope-recovery", "credential-recovery")
    product = _product()
    intent, command = _sent_command(journal, identity, product)
    order = {"symbol": "SOLUSDT", "orderId": "venue-order-1",
        "orderLinkId": intent.client_order_id, "orderStatus": "Cancelled",
        "createdTime": str(T0 // 1_000_000 + 2), "updatedTime": str(T0 // 1_000_000 + 3),
        "cumExecQty": "0", "cumExecFee": "0", "cumExecValue": "0", "avgPrice": "0"}

    result = _reconcile(journal, Reader(identity, order_row=order), identity, product)

    assert result.ready and result.observed_commands == (command.command_id,)
    assert journal.load_command(command.command_id).outcome.value == "UNKNOWN"
    assert journal.load_order_status_observations(intent_id=intent.intent_id)[-1].status == "Cancelled"
    assert {q.query_type.value for q in journal.load_reconciliation_query_evidence()} >= {
        "positions", "wallet_balance", "open_orders", "order_history", "execution_history"}
    assert all(q.facts.get("venue_identity_hash") == identity.content_hash
               for q in journal.load_reconciliation_query_evidence())
    assert any(q.facts.get("endpoints") == ["/v5/order/history"]
               for q in journal.load_reconciliation_query_evidence())


def test_bybit_missing_or_foreign_order_link_keeps_unknown_and_not_ready(journal):
    identity = BybitDemoIdentity("scope-recovery", "credential-recovery")
    product = _product()
    intent, command = _sent_command(journal, identity, product)
    order = {"symbol": "SOLUSDT", "orderId": "venue-order-1",
        "orderLinkId": intent.client_order_id, "orderStatus": "Cancelled",
        "createdTime": str(T0 // 1_000_000 + 2), "updatedTime": str(T0 // 1_000_000 + 3),
        "cumExecQty": "0", "cumExecFee": "0", "cumExecValue": "0", "avgPrice": "0"}

    missing = _reconcile(journal, Reader(identity), identity, product)
    assert not missing.ready and journal.load_command(command.command_id).outcome.value == "UNKNOWN"
    assert not journal.load_order_status_observations(intent_id=intent.intent_id)

    foreign = _reconcile(journal, Reader(identity, order_row=order, wrong_link=True), identity, product)
    assert not foreign.ready and journal.load_command(command.command_id).outcome.value == "UNKNOWN"
    assert not journal.load_order_status_observations(intent_id=intent.intent_id)
    assert "BYBIT_UNKNOWN_COMMAND_HAS_NO_PROVEN_ORDER_STATE" in missing.reasons


def test_bybit_unaccounted_selected_open_order_keeps_reconciliation_not_ready(journal):
    identity = BybitDemoIdentity("scope-recovery", "credential-recovery")
    product = _product()
    result = _reconcile(journal, Reader(identity, open_order_rows=({
        "symbol": "SOLUSDT", "orderId": "manual-order-1", "orderLinkId": "e" * 32,
        "orderStatus": "New",
    },)), identity, product)

    assert not result.ready
    assert "BYBIT_OPEN_ORDER_OWNER_UNRESOLVED" in result.reasons


def test_bybit_selected_open_order_with_one_durable_owner_is_accounted(journal):
    identity = BybitDemoIdentity("scope-recovery", "credential-recovery")
    product = _product()
    intent, command = _sent_command(journal, identity, product)
    order = {"symbol": "SOLUSDT", "orderId": "venue-order-1",
        "orderLinkId": intent.client_order_id, "orderStatus": "New",
        "createdTime": str(T0 // 1_000_000 + 2), "updatedTime": str(T0 // 1_000_000 + 3),
        "cumExecQty": "0", "cumExecFee": "0", "cumExecValue": "0", "avgPrice": "0"}

    result = _reconcile(journal, Reader(identity, open_order_rows=(order,),
        realtime_order_rows=(order,), history_order_rows=(order,)), identity, product)

    assert result.ready and result.observed_commands == (command.command_id,)
    assert journal.load_command(command.command_id).outcome.value == "UNKNOWN"


def test_bybit_conflicting_realtime_and_history_state_is_ambiguous(journal):
    identity = BybitDemoIdentity("scope-recovery", "credential-recovery")
    product = _product()
    intent, command = _sent_command(journal, identity, product)
    realtime = {"symbol": "SOLUSDT", "orderId": "venue-order-1",
        "orderLinkId": intent.client_order_id, "orderStatus": "New",
        "createdTime": str(T0 // 1_000_000 + 2), "updatedTime": str(T0 // 1_000_000 + 3),
        "cumExecQty": "0", "cumExecFee": "0", "cumExecValue": "0", "avgPrice": "0"}
    history = realtime | {"orderStatus": "Cancelled", "updatedTime": str(T0 // 1_000_000 + 4)}

    result = _reconcile(journal, Reader(identity, realtime_order_rows=(realtime,),
        history_order_rows=(history,)), identity, product)

    assert not result.ready
    assert "BYBIT_COMMAND_ORDER_ID_AMBIGUOUS" in result.reasons
    assert journal.load_command(command.command_id).outcome.value == "UNKNOWN"
    assert not journal.load_order_status_observations(intent_id=intent.intent_id)


def test_bybit_multiple_order_ids_for_one_client_id_are_ambiguous(journal):
    identity = BybitDemoIdentity("scope-recovery", "credential-recovery")
    product = _product()
    intent, command = _sent_command(journal, identity, product)
    first = {"symbol": "SOLUSDT", "orderId": "venue-order-1",
        "orderLinkId": intent.client_order_id, "orderStatus": "Cancelled",
        "createdTime": str(T0 // 1_000_000 + 2), "updatedTime": str(T0 // 1_000_000 + 3),
        "cumExecQty": "0", "cumExecFee": "0", "cumExecValue": "0", "avgPrice": "0"}
    second = first | {"orderId": "venue-order-2"}

    result = _reconcile(journal, Reader(identity, history_order_rows=(first, second)), identity, product)

    assert not result.ready
    assert "BYBIT_COMMAND_ORDER_ID_AMBIGUOUS" in result.reasons
    assert journal.load_command(command.command_id).outcome.value == "UNKNOWN"
    assert not journal.load_order_status_observations(intent_id=intent.intent_id)


def test_bybit_execution_failure_does_not_mark_reconciliation_ready(journal):
    identity = BybitDemoIdentity("scope-recovery", "credential-recovery")
    product = _product()
    intent, command = _sent_command(journal, identity, product)
    order = {"symbol": "SOLUSDT", "orderId": "venue-order-1",
        "orderLinkId": intent.client_order_id, "orderStatus": "Filled",
        "createdTime": str(T0 // 1_000_000 + 2), "updatedTime": str(T0 // 1_000_000 + 3),
        "cumExecQty": "0.01", "cumExecFee": "0.001", "cumExecValue": "1.2", "avgPrice": "120"}

    result = _reconcile(journal, Reader(identity, order_row=order, fail_execution=True), identity, product)

    assert not result.ready
    assert journal.load_command(command.command_id).outcome.value == "UNKNOWN"
    assert "BYBIT_COMMAND_EXECUTION_HISTORY_INCOMPLETE" in result.reasons
    assert any(q.status.value == "failed" and q.query_type.value == "execution_history"
               for q in journal.load_reconciliation_query_evidence())


def test_bybit_recovery_rejects_changed_signed_instrument_revision(journal):
    identity = BybitDemoIdentity("scope-recovery", "credential-recovery")
    original = _product()
    intent, command = _sent_command(journal, identity, original)
    order = {"symbol": "SOLUSDT", "orderId": "venue-order-1",
        "orderLinkId": intent.client_order_id, "orderStatus": "Cancelled",
        "createdTime": str(T0 // 1_000_000 + 2), "updatedTime": str(T0 // 1_000_000 + 3),
        "cumExecQty": "0", "cumExecFee": "0", "cumExecValue": "0", "avgPrice": "0"}

    result = _reconcile(journal, Reader(identity, order_row=order), identity, _product("0.02"))

    assert not result.ready
    assert journal.load_command(command.command_id).outcome.value == "UNKNOWN"
    assert "BYBIT_DURABLE_INSTRUMENT_REVISION_UNRESOLVED" in result.reasons
    assert not journal.load_order_status_observations(intent_id=intent.intent_id)


def test_bybit_recovery_replay_deduplicates_exact_execution_identity(journal):
    identity = BybitDemoIdentity("scope-recovery", "credential-recovery")
    product = _product()
    intent, command = _sent_command(journal, identity, product)
    order = {"symbol": "SOLUSDT", "orderId": "venue-order-1",
        "orderLinkId": intent.client_order_id, "orderStatus": "Filled",
        "createdTime": str(T0 // 1_000_000 + 2), "updatedTime": str(T0 // 1_000_000 + 3),
        "cumExecQty": "0.01", "cumExecFee": "0.001", "cumExecValue": "1.2", "avgPrice": "120"}
    execution = {"symbol": "SOLUSDT", "orderId": "venue-order-1",
        "orderLinkId": intent.client_order_id, "execId": "exec-1", "side": "Sell",
        "execQty": "0.01", "execPrice": "120", "execFee": "0.001", "feeCurrency": "USDT",
        "execTime": str(T0 // 1_000_000 + 4)}
    reader = Reader(identity, order_row=order, execution_rows=(execution,))

    first = _reconcile(journal, reader, identity, product)
    second = _reconcile(journal, reader, identity, product)

    assert first.ready and second.ready
    assert journal.load_command(command.command_id).outcome.value == "UNKNOWN"
    fills = journal.load_execution_evidence(intent_id=intent.intent_id)
    assert len(fills) == 1 and fills[0].execution_id.endswith(":exec-1")
    assert fills[0].trade_time_ns == (T0 // 1_000_000 + 4) * 1_000_000
