"""Exact demo risk-reduction readiness, independent of historical recovery.

The proof admits one existing durable command through the normal Nautilus OMS.
It cannot admit entry, resolve UNKNOWN, qualify protection on entry, or release
a reservation. Native state changes invalidate the proof before another effect.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any

from atlas.domain.enums import CommandOutcome, CommandType, LifecycleState
from atlas.domain.execution import Command, Intent, validate_client_order_id
from atlas.persistence.sqlite import PersistenceError, SQLiteJournal
from atlas.runtime.binance_demo import (
    MAX_STATE_AGE_NS,
    BinanceDemoIdentity,
    local_client_order_id,
    verify_binance_protection,
)
from atlas.runtime.binance_reconciliation import BinanceAccountSnapshot
from atlas.runtime.reconciliation_evidence import (
    Completeness,
    QueryScope,
    QueryStatus,
    QueryType,
    make_query_evidence,
)
from atlas.v2.instruments import ProductContractV2

ALLOWED_REDUCTIONS = frozenset({CommandType.CANCEL_ENTRY, CommandType.SUBMIT_EXIT,
                               CommandType.FLATTEN, CommandType.REPAIR_STOP})
PROFILE = "BINANCE_DEMO_COMMAND_READINESS_V1"
LIFECYCLE_NAMESPACE = "BINANCE_DEMO_ORDER_LIFECYCLE_V1"
_LIFECYCLES = {
    CommandType.CANCEL_ENTRY: frozenset({LifecycleState.CANCEL_PENDING}),
    CommandType.SUBMIT_EXIT: frozenset({LifecycleState.EXIT_PENDING}),
    CommandType.FLATTEN: frozenset({LifecycleState.EXIT_PENDING}),
    CommandType.REPAIR_STOP: frozenset({LifecycleState.OPEN_UNPROTECTED,
                                      LifecycleState.OPEN_PROTECTED, LifecycleState.PARTIALLY_FILLED}),
}


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def binance_command_client_order_id(identity: BinanceDemoIdentity, *, intent_id: str,
                                    command_id: str, writer_epoch: int) -> str:
    if not intent_id or not command_id or type(writer_epoch) is not int or writer_epoch < 0:
        raise ValueError("invalid Binance order lifecycle identity")
    return _hash([LIFECYCLE_NAMESPACE, identity.content_hash, intent_id, command_id, writer_epoch])[:32]


def validate_command_order_identity(identity: BinanceDemoIdentity, command: Command, intent: Intent) -> str:
    payload = json.loads(command.payload)
    client_id = validate_client_order_id(payload.get("client_order_id"))
    expected = intent.client_order_id if command.command_type == CommandType.CANCEL_ENTRY else \
        binance_command_client_order_id(identity, intent_id=intent.intent_id,
                                       command_id=command.command_id, writer_epoch=intent.writer_epoch)
    if command.command_type not in ALLOWED_REDUCTIONS or client_id != expected:
        raise PersistenceError("Binance command order lifecycle identity mismatch")
    if command.command_type != CommandType.CANCEL_ENTRY and client_id == intent.client_order_id:
        raise PersistenceError("Binance submitted order reuses its parent identity")
    return client_id


def resolve_binance_order_intent(journal: SQLiteJournal, identity: BinanceDemoIdentity,
                                  client_order_id: str) -> Intent | None:
    intent = journal.load_intent_by_client_order_id(client_order_id)
    if intent is not None:
        return intent
    command = journal.load_command_by_client_order_id(client_order_id)
    if command is None:
        return None
    intent = journal.load_intent(command.intent_id)
    validate_command_order_identity(identity, command, intent)
    if json.loads(command.payload).get("identity_hash") != identity.content_hash:
        raise PersistenceError("Binance child order belongs to a different account")
    return intent


@dataclass(frozen=True)
class BinanceCommandReadinessProofV1:
    query_id: str
    evidence_hash: str
    command_id: str
    intent_id: str
    command_type: str
    exact_payload_hash: str
    expected_state_version: int
    intent_state_version: int
    intent_lifecycle: str
    writer_epoch: int
    position_epoch: int
    identity_hash: str
    instrument_ref: str
    symbol: str
    client_order_id: str
    parent_client_order_id: str
    snapshot_hash: str
    signed_quantity: str
    received_at_ns: int
    expires_at_ns: int
    native_generation: int

    def facts(self) -> dict[str, Any]:
        fields = asdict(self)
        fields.pop("query_id")
        fields.pop("evidence_hash")
        return fields


def _current_command(journal: SQLiteJournal, identity: BinanceDemoIdentity, product: ProductContractV2,
                     command_id: str, writer_epoch: int, *, send_started: bool = False) -> tuple[Command, Intent]:
    intents = journal.load_unresolved_intents(limit=2)
    command = journal.load_command(command_id)
    if len(intents) != 1 or intents[0].intent_id != command.intent_id:
        raise PersistenceError("Binance reduction requires one exact active intent")
    intent = intents[0]
    if command.command_type not in ALLOWED_REDUCTIONS:
        raise PersistenceError("Binance readiness refuses opening or unknown commands")
    if (intent.state_version != command.expected_state_version + 1
            or intent.lifecycle not in _LIFECYCLES[command.command_type]):
        raise PersistenceError("Binance command state version or lifecycle is stale")
    if send_started:
        if command.outcome != CommandOutcome.UNKNOWN or command.send_started_at_ns is None:
            raise PersistenceError("Binance command effect phase is invalid")
    elif command.outcome != CommandOutcome.UNSENT or command.send_started_at_ns is not None:
        raise PersistenceError("Binance readiness cannot replay a sent or UNKNOWN command")
    commands = journal.load_commands_for_intent(intent.intent_id, limit=33)
    if len(commands) > 32 or not commands or commands[-1].command_id != command.command_id:
        raise PersistenceError("Binance reduction command is superseded or unbounded")
    active = [item for item in commands if item.outcome not in (CommandOutcome.RECONCILED, CommandOutcome.DEFINITE_REJECT)]
    if len(active) != 1 or active[0].command_id != command.command_id:
        raise PersistenceError("Binance reduction has unresolved conflicting commands")
    payload = json.loads(command.payload)
    if (payload.get("identity_hash") != identity.content_hash or payload.get("instrument_ref") != product.content_hash
            or payload.get("symbol") != product.key.native_symbol):
        raise PersistenceError("Binance readiness command account or product mismatch")
    plan = journal.load_trade_plan(intent.plan_id)
    if (plan.version != intent.plan_version or plan.market.upper() not in {"BINANCE", "BINANCE_DEMO", "BINANCE_TESTNET"}
            or plan.instrument != product.key.native_symbol):
        raise PersistenceError("Binance readiness immutable plan scope mismatch")
    entry_side = "BUY" if plan.side.value == "LONG" else "SELL"
    expected_side = entry_side if command.command_type == CommandType.CANCEL_ENTRY else (
        "SELL" if plan.side.value == "LONG" else "BUY")
    if payload.get("side") != expected_side or Decimal(payload.get("stop", "NaN")) != plan.stop:
        raise PersistenceError("Binance readiness action differs from the approved plan")
    if command.command_type == CommandType.REPAIR_STOP and plan.stop_trigger_basis != "MarkPrice":
        raise PersistenceError("Binance repair stop trigger differs from the approved plan")
    client_id = validate_command_order_identity(identity, command, intent)
    statuses = journal.load_order_status_observations(intent_id=intent.intent_id, limit=256)
    if len(statuses) == 256:
        raise PersistenceError("Binance current order status bound exhausted")
    if command.command_type != CommandType.CANCEL_ENTRY and (
            any(item.client_order_id == client_id for item in statuses)
            or journal.load_execution_evidence(client_order_id=client_id)):
        raise PersistenceError("Binance new order lifecycle already has execution evidence")
    if command.command_type == CommandType.CANCEL_ENTRY and any(
            item.client_order_id == client_id and item.status in {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}
            for item in statuses):
        raise PersistenceError("Binance cancellation target has terminal evidence")
    return command, intent


def build_binance_command_readiness(
    reader: Any, journal: SQLiteJournal, *, identity: BinanceDemoIdentity,
    product: ProductContractV2, snapshot: BinanceAccountSnapshot, writer_epoch: int,
    native_generation: int, assert_writer: Callable[[], None],
) -> BinanceCommandReadinessProofV1 | None:
    """Read two selected-symbol open views and persist one exact readiness proof."""
    assert_writer()
    commands = journal.load_unresolved_commands(limit=32)
    if not commands:
        return None
    if len(commands) == 32 and journal.load_unresolved_commands(limit=1, after_command_id=commands[-1].command_id):
        raise PersistenceError("Binance active command bound exhausted")
    if len(commands) != 1:
        raise PersistenceError("Binance reduction has unresolved conflicting commands")
    command, intent = _current_command(journal, identity, product, commands[0].command_id, writer_epoch)
    if (not isinstance(snapshot, BinanceAccountSnapshot) or snapshot.identity_hash != identity.content_hash
            or not snapshot.eligible or snapshot.account_fingerprint is None):
        raise PersistenceError("Binance readiness account profile unqualified")
    payload = json.loads(command.payload)
    plan = journal.load_trade_plan(intent.plan_id)
    symbol = product.key.native_symbol
    positions = [row.as_dict() for row in snapshot.positions if row.as_dict().get("symbol") == symbol]
    if len(positions) != 1 or positions[0].get("positionSide") != "BOTH":
        raise PersistenceError("Binance readiness position identity is missing or ambiguous")
    signed = Decimal(positions[0]["positionAmt"])
    if not signed.is_finite():
        raise PersistenceError("Binance readiness position amount invalid")
    if any(Decimal(row.as_dict()["positionAmt"]) != 0 for row in snapshot.positions
           if row.as_dict().get("symbol") != symbol):
        raise PersistenceError("Binance readiness has a position outside its active intent")
    configs = [row.as_dict() for row in snapshot.symbol_configs if row.as_dict().get("symbol") == symbol]
    if len(configs) != 1 or configs[0].get("marginType") != "ISOLATED":
        raise PersistenceError("Binance readiness isolated symbol profile missing")
    quantity = Decimal(payload["quantity"])
    if not quantity.is_finite() or quantity <= 0:
        raise PersistenceError("Binance readiness quantity invalid")
    if command.command_type != CommandType.CANCEL_ENTRY:
        if (payload.get("reduce_only") is not True or signed == 0 or quantity > abs(signed)
                or payload.get("side") != ("SELL" if signed > 0 else "BUY")):
            raise PersistenceError("Binance readiness is not an exact reduction")
        if (signed > 0) != (plan.side.value == "LONG"):
            raise PersistenceError("Binance position side differs from its approved plan")
        if command.command_type == CommandType.REPAIR_STOP and quantity != abs(signed):
            raise PersistenceError("Binance readiness full position repair quantity mismatch")
    receipts = []
    views = []
    for path in ("/fapi/v1/openOrders", "/fapi/v1/openAlgoOrders"):
        assert_writer()
        receipt = reader.read_with_receipt(path, {"symbol": symbol})
        if receipt.identity_hash != identity.content_hash or receipt.endpoint != path:
            raise PersistenceError("Binance readiness open view identity mismatch")
        rows = receipt.payload
        if not isinstance(rows, list) or len(rows) > 64:
            raise PersistenceError("Binance readiness open view exceeded its bound")
        if any(not isinstance(row, dict) or row.get("symbol") != symbol for row in rows):
            raise PersistenceError("Binance readiness open view symbol mismatch")
        receipts.append(receipt)
        views.append(rows)
    now_ns = reader.clock_ns()
    timestamps = [snapshot.captured_at_ns, *(row.received_at_ns for row in (
        snapshot.account, snapshot.dual_side, snapshot.multi_assets, *snapshot.positions,
        *snapshot.symbol_configs, *snapshot.balances)), *(receipt.received_at_ns for receipt in receipts)]
    if any(type(stamp) is not int or not 0 <= now_ns - stamp <= MAX_STATE_AGE_NS for stamp in timestamps):
        raise PersistenceError("Binance readiness input receipts are stale")
    regular, algos = views
    if command.command_type == CommandType.CANCEL_ENTRY:
        if (len(regular) != 1 or local_client_order_id(regular[0].get("clientOrderId")) != intent.client_order_id
                or regular[0].get("status") not in {"NEW", "PARTIALLY_FILLED"}
                or regular[0].get("side") != payload.get("side")):
            raise PersistenceError("Binance readiness cancel target is absent or conflicting")
    elif regular:
        raise PersistenceError("Binance readiness has conflicting regular open orders")
    for row in algos:
        client_id = local_client_order_id(row.get("clientAlgoId"))
        prior = journal.load_command_by_client_order_id(client_id)
        if (prior is None or prior.intent_id != intent.intent_id or prior.command_type != CommandType.REPAIR_STOP
                or prior.outcome != CommandOutcome.RECONCILED):
            raise PersistenceError("Binance readiness has orphan or unresolved protection")
        validate_command_order_identity(identity, prior, intent)
        previous = json.loads(prior.payload)
        if (previous.get("identity_hash") != identity.content_hash
                or previous.get("instrument_ref") != product.content_hash
                or previous.get("symbol") != symbol):
            raise PersistenceError("Binance existing protection has a conflicting account or product")
        if not verify_binance_protection(row, symbol=symbol, client_order_id=client_id,
                stop=Decimal(previous["stop"]), signed_position=signed, now_ns=now_ns,
                received_at_ns=receipts[1].received_at_ns).verified:
            raise PersistenceError("Binance readiness existing protection is conflicting")
    if len(algos) > 1 or command.command_type == CommandType.REPAIR_STOP and algos:
        raise PersistenceError("Binance readiness refuses duplicate protection lifecycle")
    client_id = validate_command_order_identity(identity, command, intent)
    if command.command_type != CommandType.CANCEL_ENTRY:
        associated = journal.load_command_by_client_order_id(client_id)
        if (associated is None or associated.command_id != command.command_id
                or journal.load_intent_by_client_order_id(client_id) is not None):
            raise PersistenceError("Binance readiness submitted client identity collided")
    assert_writer()
    with journal._transaction_lock:
        _current_command(journal, identity, product, command.command_id, writer_epoch)
        proof = BinanceCommandReadinessProofV1("", "", command.command_id, intent.intent_id,
            command.command_type.value, command.exact_payload_hash, command.expected_state_version,
            intent.state_version, intent.lifecycle.value, writer_epoch, intent.position_epoch,
            identity.content_hash, product.content_hash, symbol, client_id, intent.client_order_id,
            _hash(asdict(snapshot)), str(signed), now_ns, min(timestamps) + MAX_STATE_AGE_NS, native_generation)
        query = make_query_evidence(query_id=uuid.uuid4().hex, query_type=QueryType.OPEN_ORDERS,
            scope=QueryScope.INSTRUMENT, account=identity.account_scope_ref, instrument=f"{symbol}-PERP.BINANCE",
            requested_interval_start_ns=None, requested_interval_end_ns=None, pagination_cursors=(),
            pages_observed=2, total_records_returned=len(regular) + len(algos), completeness=Completeness.COMPLETE,
            status=QueryStatus.SUCCESS, source_time_ns=None, receipt_time_ns=now_ns, request_ids=(),
            retention_segments=(), facts={"profile": PROFILE, "proof": proof.facts(),
                "response_hashes": [receipt.raw_payload_hash for receipt in receipts],
                "regular_open_orders": regular, "algo_open_orders": algos,
                "capital_enabled": False, "assisted_enabled": False,
                "opening_protection_qualified": False, "complete_for_recovery": False}, error_message=None)
        journal.append_reconciliation_query_evidence(query)
    return BinanceCommandReadinessProofV1(query.query_id, query.evidence_hash, **proof.facts())


def validate_binance_command_readiness(
    proof: BinanceCommandReadinessProofV1 | None, journal: SQLiteJournal, *, identity: BinanceDemoIdentity,
    product: ProductContractV2, snapshot: BinanceAccountSnapshot, command_id: str, writer_epoch: int,
    native_generation: int, now_ns: int, send_started: bool = False,
) -> None:
    if (not isinstance(proof, BinanceCommandReadinessProofV1) or proof.command_id != command_id
            or type(now_ns) is not int or not proof.received_at_ns <= now_ns <= proof.expires_at_ns
            or proof.native_generation != native_generation or proof.writer_epoch != writer_epoch
            or proof.identity_hash != identity.content_hash or proof.instrument_ref != product.content_hash):
        raise PersistenceError("Binance command readiness is missing, expired or superseded")
    queries = journal.load_reconciliation_query_evidence(proof.query_id)
    if len(queries) != 1:
        raise PersistenceError("Binance command readiness persisted proof missing")
    query = queries[0]
    if (not query.hash_binds_payload or query.evidence_hash != proof.evidence_hash
            or query.facts.get("profile") != PROFILE or query.facts.get("proof") != proof.facts()
            or query.status != QueryStatus.SUCCESS or query.completeness != Completeness.COMPLETE
            or query.account != identity.account_scope_ref or query.instrument != f"{proof.symbol}-PERP.BINANCE"
            or query.facts.get("capital_enabled") is not False or query.facts.get("assisted_enabled") is not False
            or query.facts.get("opening_protection_qualified") is not False
            or query.facts.get("complete_for_recovery") is not False):
        raise PersistenceError("Binance command readiness persisted proof is invalid")
    command, intent = _current_command(journal, identity, product, command_id, writer_epoch, send_started=send_started)
    if (command.exact_payload_hash != proof.exact_payload_hash or command.expected_state_version != proof.expected_state_version
            or intent.state_version != proof.intent_state_version or intent.position_epoch != proof.position_epoch
            or intent.lifecycle.value != proof.intent_lifecycle or intent.intent_id != proof.intent_id
            or intent.client_order_id != proof.parent_client_order_id or _hash(asdict(snapshot)) != proof.snapshot_hash):
        raise PersistenceError("Binance command readiness current state binding changed")
