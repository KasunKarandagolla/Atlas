"""Identity-bound, read-only Binance USD-M account and execution evidence.

This module only reads through ``BinanceDemoReader`` and appends observations
through the existing SQLite journal APIs. It does not submit or retry orders.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

from atlas.domain.execution import EconomicEvent, Observation, validate_client_order_id
from atlas.domain.time import ensure_utc_ns
from atlas.persistence.sqlite import PersistenceError, SQLiteJournal
from atlas.runtime.binance_demo import (
    MAX_STATE_AGE_NS,
    BinanceDemoIdentity,
    BinanceDemoReader,
    BinanceDemoReadReceipt,
    external_client_order_id,
    local_client_order_id,
)
from atlas.runtime.capability_ledger import EvidenceState
from atlas.runtime.fill_dedup import FillRecord, OrderStatusRecord
from atlas.runtime.reconciliation_evidence import (
    Completeness,
    QueryScope,
    QueryStatus,
    QueryType,
    ReconciliationRun,
    build_reconciliation_bundle,
    make_query_evidence,
)


def _canonical_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(k, str) for k in value):
        raise ValueError(f"invalid Binance {name} response")
    return value


def _decimal(value: Any, name: str, *, nonnegative: bool = False) -> Decimal:
    if not isinstance(value, (str, int, Decimal)) or isinstance(value, bool):
        raise ValueError(f"invalid Binance {name}")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError(f"invalid Binance {name}") from None
    if not parsed.is_finite() or (nonnegative and parsed < 0):
        raise ValueError(f"invalid Binance {name}")
    return parsed


def _id(value: Any, name: str) -> str:
    if (not isinstance(value, (str, int)) or isinstance(value, bool)
            or not re.fullmatch(r"[0-9]{1,64}", str(value)) or int(value) <= 0):
        raise ValueError(f"invalid Binance {name}")
    return str(value)


def _symbol(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Z0-9_]{1,32}", value):
        raise ValueError("invalid Binance symbol")
    return value


def _fill_identity(fill: FillRecord) -> tuple[Any, ...]:
    return (fill.execution_id, fill.order_id, fill.client_order_id, fill.intent_id, fill.instrument,
            fill.side, fill.qty, fill.price, fill.fee, fill.fee_currency, fill.trade_time_ns)


@dataclass(frozen=True)
class BinanceObservedRow:
    """A frozen broker row plus the response receipt and canonical hash."""

    row: tuple[tuple[str, Any], ...]
    received_at_ns: int
    source_hash: str

    def as_dict(self) -> dict[str, Any]:
        return dict(self.row)


@dataclass(frozen=True)
class BinanceAccountSnapshot:
    identity_hash: str
    account_scope_ref: str
    credential_ref: str
    environment: str
    product: str
    captured_at_ns: int
    account: BinanceObservedRow
    dual_side: BinanceObservedRow
    multi_assets: BinanceObservedRow
    positions: tuple[BinanceObservedRow, ...]
    account_fingerprint: str | None
    account_fingerprint_status: str
    eligible: bool
    reasons: tuple[str, ...]
    symbol_configs: tuple[BinanceObservedRow, ...] = ()
    balances: tuple[BinanceObservedRow, ...] = ()


def _observed(value: Any, received_at_ns: int, source_hash: str | None = None) -> BinanceObservedRow:
    if isinstance(value, Mapping):
        row = tuple(sorted((key, _freeze(item)) for key, item in value.items()))
    else:
        raise ValueError("expected Binance object row")
    return BinanceObservedRow(row, received_at_ns, source_hash or _canonical_hash(value))


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return tuple(sorted((str(key), _freeze(item)) for key, item in value.items()))
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def _validate_intent_symbol(journal: SQLiteJournal, intent_id: str, client_id: str, symbol: str,
                            *, identity_hash: str | None = None) -> None:
    """Require the broker symbol to match a command durably bound to the intent."""
    commands = journal.load_commands_for_intent(intent_id)
    symbols: set[str] = set()
    identities: set[str | None] = set()
    for command in commands:
        try:
            payload = json.loads(command.payload)
        except (TypeError, ValueError):
            continue
        if isinstance(payload, dict) and payload.get("client_order_id") == client_id:
            identities.add(payload.get("identity_hash"))
            candidate = payload.get("symbol")
            if isinstance(candidate, str) and candidate:
                symbols.add(candidate)
    if symbols != {symbol}:
        raise ValueError("Binance symbol does not match durable intent command")
    if identity_hash is not None and identities != {identity_hash}:
        raise ValueError("Binance account identity does not match durable intent command")


def normalize_binance_order_row(raw: Mapping[str, Any], *, received_at_ns: int) -> BinanceObservedRow:
    """Bind regular/algo wire IDs to exact ATLAS IDs with Nautilus' decoder."""
    row = dict(_mapping(raw, "order"))
    ensure_utc_ns(received_at_ns, field="received_at_ns")
    local_ids: set[str] = set()
    for field_name in ("clientOrderId", "origClientOrderId", "clientAlgoId"):
        value = row.get(field_name)
        if value is not None:
            decoded = local_client_order_id(value)
            local_ids.add(decoded)
            row[f"{field_name}_local"] = decoded
    if len(local_ids) > 1:
        raise ValueError("Binance order client identity fields disagree")
    return _observed(row, received_at_ns)


def record_binance_order_status(
    journal: SQLiteJournal,
    raw: Mapping[str, Any],
    *,
    received_at_ns: int,
    intent_id: str,
    identity: BinanceDemoIdentity,
) -> OrderStatusRecord:
    """Append an order observation; cancellation alone never implies no fills."""
    row = _mapping(raw, "order")
    ensure_utc_ns(received_at_ns, field="received_at_ns")
    local_ids: set[str] = set()
    for field_name in ("clientOrderId", "origClientOrderId", "clientAlgoId"):
        if row.get(field_name) is not None:
            local_ids.add(local_client_order_id(row[field_name]))
    if len(local_ids) != 1:
        raise ValueError("Binance order client identity missing")
    local_id = next(iter(local_ids))
    intent = journal.load_intent(intent_id)
    if intent.client_order_id != local_id:
        raise ValueError("Binance order does not match durable intent identity")
    symbol = _symbol(row.get("symbol"))
    _validate_intent_symbol(journal, intent_id, local_id, symbol, identity_hash=identity.content_hash)
    exchange_order_id = _id(row.get("orderId", row.get("algoId")), "order id")
    if "orderId" in row and "executedQty" not in row:
        raise ValueError("Binance regular order cumulative quantity missing")
    cumulative_qty = _decimal(row.get("executedQty", "0"), "cumulative executed quantity", nonnegative=True)
    cum_value = _decimal(row.get("cumQuote", "0"), "cumulative quote value", nonnegative=True)
    average = _decimal(row.get("avgPrice", "0"), "average fill price", nonnegative=True)
    if average == 0 and cumulative_qty > 0:
        average = cum_value / cumulative_qty if cum_value > 0 else Decimal("0")
    status = row.get("status", row.get("algoStatus", ""))
    if not isinstance(status, str):
        raise ValueError("Binance order status missing or unknown")
    if status not in {"NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED", "REJECTED", "EXPIRED",
                      "EXPIRED_IN_MATCH", "FINISHED"}:
        raise ValueError("Binance order status missing or unknown")
    if row.get("algoStatus") == "FINISHED" and row.get("status") is None:
        status = "UNKNOWN"
    if cumulative_qty > 0 and average <= 0:
        status = "UNKNOWN"
    observed = OrderStatusRecord(
        order_id=exchange_order_id,
        client_order_id=local_id,
        intent_id=intent_id,
        status=status,
        cum_exec_qty=cumulative_qty,
        cum_exec_fee=Decimal("0"),
        cum_exec_value=cum_value,
        avg_exec_price=average if average > 0 else None,
        receive_time_ns=received_at_ns,
        source="BINANCE_DEMO_ORDER_LOOKUP",
        raw_hash=_canonical_hash(raw),
    )
    journal.append_order_status_observation(observed)
    return observed


class _BinanceReadPort(Protocol):
    identity: BinanceDemoIdentity
    clock_ns: Callable[[], int]

    def read_with_receipt(self, path: str, params: Mapping[str, str | int] | None = None) -> BinanceDemoReadReceipt: ...


def capture_account_snapshot(
    reader: _BinanceReadPort,
    *,
    now_ns: int | None = None,
    expected_account_fingerprint: str | None = None,
) -> BinanceAccountSnapshot:
    """Read account/mode/positions, binding every response to configured identity.

    Each response has an exact receipt timestamp. Every component must be no
    more than two seconds old at the final capture clock. Position ``updateTime``
    is a state-change time and is not used as a freshness proxy.
    """
    identity = reader.identity
    if now_ns is not None:
        ensure_utc_ns(now_ns, field="now_ns")
    receipts: list[BinanceDemoReadReceipt] = []
    for path in ("/fapi/v3/account", "/fapi/v1/accountConfig", "/fapi/v3/positionRisk",
                 "/fapi/v1/symbolConfig", "/fapi/v3/balance"):
        receipt = reader.read_with_receipt(path)
        ensure_utc_ns(receipt.received_at_ns, field="received_at_ns")
        if receipt.identity_hash != identity.content_hash or receipt.endpoint != path:
            raise ValueError("Binance account snapshot receipt identity mismatch")
        if receipts and receipt.received_at_ns < receipts[-1].received_at_ns:
            raise ValueError("Binance account snapshot receipt clock moved backwards")
        receipts.append(receipt)
    capture_time_ns = reader.clock_ns()
    ensure_utc_ns(capture_time_ns, field="capture_time_ns")
    if now_ns is not None and capture_time_ns < now_ns:
        raise ValueError("Binance account snapshot clock moved backwards")
    if any(not 0 <= capture_time_ns - receipt.received_at_ns <= MAX_STATE_AGE_NS for receipt in receipts):
        raise ValueError("Binance account snapshot responses are stale")
    account_raw, config_raw, positions_raw, configs_raw, balances_raw = (receipt.payload for receipt in receipts)

    account = _mapping(account_raw, "account")
    dual = _mapping(config_raw, "account configuration")
    multi = dual
    if not isinstance(positions_raw, list) or any(not isinstance(row, Mapping) for row in positions_raw):
        raise ValueError("invalid Binance positions response")
    if not isinstance(configs_raw, list) or any(not isinstance(row, Mapping) for row in configs_raw):
        raise ValueError("invalid Binance symbol configuration response")
    if not isinstance(balances_raw, list) or any(not isinstance(row, Mapping) for row in balances_raw):
        raise ValueError("invalid Binance balances response")
    frozen_positions = tuple(_observed(row, receipts[2].received_at_ns, receipts[2].raw_payload_hash)
                             for row in positions_raw)
    frozen_configs = tuple(_observed(_mapping(row, "symbol configuration"), receipts[3].received_at_ns,
                                    receipts[3].raw_payload_hash) for row in configs_raw)
    config_by_symbol: dict[str, Mapping[str, Any]] = {}
    for row in configs_raw:
        symbol = _symbol(row.get("symbol"))
        if symbol in config_by_symbol:
            raise ValueError("duplicate Binance symbol configuration")
        config_by_symbol[symbol] = row
    reasons: list[str] = []
    if dual.get("canTrade") is not True or ("canTrade" in account and account["canTrade"] is not True):
        reasons.append("ACCOUNT_CANNOT_TRADE")
    if dual.get("dualSidePosition") is not False:
        reasons.append("ACCOUNT_NOT_ONE_WAY")
    if (multi.get("multiAssetsMargin") is not False
            or ("multiAssetsMargin" in account and account["multiAssetsMargin"] is not False)):
        reasons.append("ACCOUNT_NOT_SINGLE_ASSET_MARGIN")
    for position in frozen_positions:
        row = position.as_dict()
        position_symbol = row.get("symbol")
        config = config_by_symbol.get(position_symbol) if isinstance(position_symbol, str) else None
        if (config is None or config.get("marginType") != "ISOLATED"
                or row.get("positionSide") != "BOTH"):
            reasons.append("POSITION_NOT_ISOLATED_ONE_WAY")
            break
        try:
            _decimal(row.get("positionAmt"), "position amount")
        except ValueError:
            reasons.append("POSITION_AMOUNT_INVALID")
            break
    uid = account.get("uid")
    fingerprint: str | None = None
    fingerprint_status = "UNVERIFIED_ACCOUNT_FINGERPRINT"
    if (isinstance(uid, (str, int)) and not isinstance(uid, bool)
            and re.fullmatch(r"[0-9]{1,64}", str(uid)) and int(uid) > 0):
        fingerprint = hashlib.sha256(f"binance-usdm-uid:{uid}".encode()).hexdigest()
        fingerprint_status = "VERIFIED_STABLE_UID"
    else:
        usdt_balances = [row for row in balances_raw if row.get("asset") == "USDT"]
        alias = usdt_balances[0].get("accountAlias") if len(usdt_balances) == 1 else None
        if (isinstance(alias, str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", alias)
                and all(row.get("accountAlias") == alias for row in balances_raw)):
            fingerprint = hashlib.sha256(f"binance-usdm-account-alias:{alias}".encode()).hexdigest()
            fingerprint_status = "VERIFIED_ACCOUNT_ALIAS_RECEIPT"
        else:
            reasons.append("ACCOUNT_FINGERPRINT_UNVERIFIED")
    if expected_account_fingerprint is not None and fingerprint != expected_account_fingerprint:
        reasons.append("ACCOUNT_FINGERPRINT_CHANGED")
    # Retain exact response hashes and timestamps without exposing the account alias.
    frozen_balances = tuple(_observed({key: value for key, value in row.items() if key != "accountAlias"},
                                     receipts[4].received_at_ns, receipts[4].raw_payload_hash)
                            for row in balances_raw)
    return BinanceAccountSnapshot(
        identity.content_hash,
        identity.account_scope_ref,
        identity.credential_ref,
        identity.environment,
        identity.product,
        capture_time_ns,
        _observed({key: value for key, value in account.items() if key != "uid"},
                  receipts[0].received_at_ns, receipts[0].raw_payload_hash),
        _observed(dual, receipts[1].received_at_ns, receipts[1].raw_payload_hash),
        _observed(multi, receipts[1].received_at_ns, receipts[1].raw_payload_hash),
        frozen_positions,
        fingerprint,
        fingerprint_status,
        not reasons,
        tuple(dict.fromkeys(reasons)),
        frozen_configs,
        frozen_balances,
    )


def record_binance_trades(
    journal: SQLiteJournal,
    identity: BinanceDemoIdentity,
    rows: Sequence[Mapping[str, Any]],
    *,
    received_at_ns: int,
    client_id_by_order_id: Mapping[tuple[str, str], str],
    intent_id_by_client_id: Mapping[str, str],
) -> tuple[FillRecord, ...]:
    """Normalize and deduplicate USD-M trades through durable fill evidence.

    Binance trade rows do not carry the client order identity. The caller must
    supply that association from separately reconciled order history; missing
    or conflicting associations fail closed.
    """
    ensure_utc_ns(received_at_ns, field="received_at_ns")
    seen: dict[str, str] = {}
    unique_rows: list[tuple[Mapping[str, Any], str, str, str, str]] = []
    for raw in rows:
        row = _mapping(raw, "trade")
        trade_id = _id(row.get("id"), "trade id")
        exchange_order_id = _id(row.get("orderId"), "order id")
        symbol = _symbol(row.get("symbol"))
        execution_id = f"BINANCE:{identity.content_hash}:{symbol}:{trade_id}"
        source_hash = _canonical_hash(raw)
        if execution_id in seen:
            if seen[execution_id] != source_hash:
                raise ValueError("conflicting Binance trade rows share an execution id")
            continue
        seen[execution_id] = source_hash
        unique_rows.append((row, trade_id, exchange_order_id, symbol, source_hash))
    normalized: list[FillRecord] = []
    for row, _trade_id, exchange_order_id, symbol, source_hash in unique_rows:
        client_id = validate_client_order_id(client_id_by_order_id[(symbol, exchange_order_id)])
        intent_id = intent_id_by_client_id[client_id]
        if not intent_id.strip():
            raise ValueError("trade intent association missing")
        intent = journal.load_intent(intent_id)
        if intent.client_order_id != client_id:
            raise ValueError("Binance trade does not match durable intent identity")
        _validate_intent_symbol(journal, intent_id, client_id, symbol, identity_hash=identity.content_hash)
        side_value = row.get("side")
        if side_value not in ("BUY", "SELL"):
            buyer = row.get("buyer")
            if type(buyer) is not bool:
                raise ValueError("Binance trade side missing")
            side_value = "BUY" if buyer else "SELL"
        time_ms = row.get("time")
        if not isinstance(time_ms, int) or isinstance(time_ms, bool) or time_ms <= 0:
            raise ValueError("Binance trade time missing")
        fee_currency = row.get("commissionAsset")
        if not isinstance(fee_currency, str) or not re.fullmatch(r"[A-Z0-9]{1,16}", fee_currency):
            raise ValueError("Binance trade fee currency missing")
        fill = FillRecord(
            execution_id=execution_id,
            order_id=exchange_order_id,
            client_order_id=client_id,
            intent_id=intent_id,
            instrument=f"{symbol}-PERP.BINANCE",
            side="Buy" if side_value == "BUY" else "Sell",
            qty=_decimal(row.get("qty"), "trade quantity", nonnegative=True),
            price=_decimal(row.get("price"), "trade price", nonnegative=True),
            fee=_decimal(row.get("commission", "0"), "trade commission", nonnegative=True),
            fee_currency=fee_currency,
            trade_time_ns=time_ms * 1_000_000,
            receive_time_ns=received_at_ns,
            source="BINANCE_DEMO_USER_TRADES",
            raw_hash=source_hash,
        )
        normalized.append(fill)
    # Serialize dedup with native callbacks; receipt/source/hash may differ for
    # the same immutable broker execution observed through both transports.
    with journal._transaction_lock:
        prior_by_id = {fill.execution_id: fill for client_id in {row.client_order_id for row in normalized}
                       for fill in journal.load_execution_evidence(client_order_id=client_id)}
        for fill in normalized:
            prior = prior_by_id.get(fill.execution_id)
            if prior is not None and _fill_identity(prior) != _fill_identity(fill):
                raise ValueError("conflicting Binance trade durable execution payload")
        accepted: list[FillRecord] = []
        for fill in normalized:
            if fill.execution_id in prior_by_id:
                continue
            if journal.append_execution_evidence(fill):
                accepted.append(fill)
        return tuple(accepted)


def normalize_binance_income(
    identity: BinanceDemoIdentity,
    rows: Sequence[Mapping[str, Any]],
    *,
    received_at_ns: int,
) -> tuple[tuple[EconomicEvent, BinanceObservedRow], ...]:
    """Normalize funding, commissions and other income rows with source hashes."""
    ensure_utc_ns(received_at_ns, field="received_at_ns")
    result: list[tuple[EconomicEvent, BinanceObservedRow]] = []
    seen: dict[str, str] = {}
    for raw in rows:
        row = _mapping(raw, "income")
        transaction_id = _id(row.get("tranId"), "income transaction id")
        event_type = row.get("incomeType")
        currency = row.get("asset")
        time_ms = row.get("time")
        if (not isinstance(event_type, str) or not re.fullmatch(r"[A-Z_]{1,64}", event_type)
                or not isinstance(currency, str) or not re.fullmatch(r"[A-Z0-9]{1,16}", currency)):
            raise ValueError("Binance income identity missing")
        if not isinstance(time_ms, int) or isinstance(time_ms, bool) or time_ms <= 0:
            raise ValueError("Binance income time missing")
        source_hash = _canonical_hash(raw)
        if transaction_id in seen:
            if seen[transaction_id] != source_hash:
                raise ValueError("conflicting Binance income rows share a transaction id")
            continue
        seen[transaction_id] = source_hash
        event = EconomicEvent(
            account=identity.account_scope_ref,
            venue_transaction_id=transaction_id,
            currency=currency,
            amount=_decimal(row.get("income"), "income amount"),
            effective_time_ns=time_ms * 1_000_000,
            received_at_ns=received_at_ns,
            event_type=event_type,
            revision=source_hash,
        )
        result.append((event, _observed(raw, received_at_ns)))
    return tuple(result)


def record_binance_income(
    journal: SQLiteJournal,
    identity: BinanceDemoIdentity,
    rows: Sequence[Mapping[str, Any]],
    *,
    received_at_ns: int,
) -> tuple[EconomicEvent, ...]:
    """Persist exact typed income/funding/cost rows idempotently."""
    recorded: list[EconomicEvent] = []
    for event, source in normalize_binance_income(identity, rows, received_at_ns=received_at_ns):
        prior = journal.load_economic_event(event.account, event.venue_transaction_id)
        if prior is None:
            journal.append_economic_event(event)
        elif (prior.currency, prior.amount, prior.effective_time_ns, prior.event_type, prior.revision) != (
            event.currency, event.amount, event.effective_time_ns, event.event_type, event.revision,
        ):
            # Delegate conflict diagnostics and preservation to the journal.
            journal.append_economic_event(event)
        journal.append_observation(Observation(
            observation_id=uuid.uuid4().hex,
            source="BINANCE_DEMO_INCOME",
            venue_identity=identity.content_hash,
            source_time_ns=event.effective_time_ns,
            receive_time_ns=received_at_ns,
            raw_hash=source.source_hash,
            completeness="EXACT_RESPONSE_ROW",
        ))
        recorded.append(prior if prior is not None else event)
    return tuple(recorded)


@dataclass(frozen=True)
class BinanceExecutionHistoryPass:
    processed_command_ids: tuple[str, ...]
    query_ids: tuple[str, ...]
    next_command_id: str | None
    reasons: tuple[str, ...]
    complete_for_recovery: bool = False
    next_query_offset: int = 0


def reconcile_binance_execution_history(
    reader: BinanceDemoReader,
    journal: SQLiteJournal,
    *,
    max_intents: int = 4,
    after_command_id: str | None = None,
    history_window_ns: int = 7 * 24 * 3600 * 1_000_000_000,
    max_queries: int = 4,
    query_offset: int = 0,
    assert_writer: Callable[[], None] = lambda: None,
) -> BinanceExecutionHistoryPass:
    """Collect one bounded diagnostic history pass through the signed read port.

    Singleton order/algo lookups and one bounded history page cannot certify
    absence or complete account recovery. Every failed/missing association is
    persisted as incomplete evidence; UNKNOWN sends and reservations remain.
    A caller owns the rotating cursor and cadence under the shared read budget.
    """
    if type(max_intents) is not int or not 1 <= max_intents <= 32:
        raise ValueError("invalid Binance history intent bound")
    if type(history_window_ns) is not int or not 0 < history_window_ns <= 7 * 24 * 3600 * 1_000_000_000:
        raise ValueError("invalid Binance history interval bound")
    if (type(max_queries) is not int or not 1 <= max_queries <= 4
            or type(query_offset) is not int or not 0 <= query_offset < 4
            or (max_queries != 4 or query_offset != 0) and max_intents != 1):
        raise ValueError("invalid Binance history query cursor bound")
    assert_writer()
    commands = journal.load_unresolved_commands(limit=max_intents, after_command_id=after_command_id)
    if not commands and after_command_id is not None:
        commands = journal.load_unresolved_commands(limit=max_intents)
    processed: list[str] = []
    query_ids: list[str] = []
    reasons: list[str] = []
    next_query_offset = 0
    deferred = False
    now_ns = ensure_utc_ns(reader.clock_ns(), field="history_clock_ns")
    for command in commands:
        if command.send_started_at_ns is None:
            continue
        processed.append(command.command_id)
        payload = json.loads(command.payload)
        if payload.get("identity_hash") != reader.identity.content_hash:
            raise ValueError("Binance history durable command account identity mismatch")
        symbol = _symbol(payload.get("symbol"))
        client_id = validate_client_order_id(payload.get("client_order_id"))
        intent = journal.load_intent(command.intent_id)
        if intent.client_order_id != client_id:
            raise ValueError("Binance history durable command identity mismatch")
        start_ns = max(0, now_ns - history_window_ns, command.created_at_ns - 1_000_000_000)
        start_ms, end_ms = start_ns // 1_000_000, now_ns // 1_000_000
        order_associations: dict[tuple[str, str], str] = {}
        # A rotated execution-only pass can reuse exact durable order/client
        # associations from prior reads or native events. Missing algo child
        # associations remain incomplete rather than being inferred.
        if query_offset > 0:
            for observed in journal.load_order_status_observations(intent_id=intent.intent_id):
                if observed.client_order_id == client_id:
                    order_associations[(symbol, observed.order_id)] = client_id
        # Persist each query independently, including read refusal and page limits.
        requests: tuple[tuple[str, QueryType, dict[str, str | int]], ...] = (
            ("/fapi/v1/order", QueryType.ORDER_HISTORY,
             {"symbol": symbol, "origClientOrderId": external_client_order_id(client_id)}),
            ("/fapi/v1/algoOrder", QueryType.CONDITIONAL_ORDERS,
             {"clientAlgoId": external_client_order_id(client_id)}),
            ("/fapi/v1/userTrades", QueryType.EXECUTION_HISTORY,
             {"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": 100}),
            ("/fapi/v1/income", QueryType.TRANSACTION_LOG,
             {"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": 100}),
        )
        for index in range(query_offset, min(4, query_offset + max_queries)):
            path, query_type, params = requests[index]
            assert_writer()
            query_id = uuid.uuid4().hex
            receipt = None
            records = 0
            status = QueryStatus.FAILED
            completeness = Completeness.UNKNOWN
            failure = None
            facts: dict[str, Any] = {
                "venue_identity_hash": reader.identity.content_hash, "endpoint": path,
                "command_id": command.command_id, "client_order_id": client_id,
                "symbol_filter": symbol, "absence_certified": False,
                "opening_protection_qualified": False,
            }
            try:
                budget_delay = getattr(reader, "read_budget_delay_ns", None)
                if budget_delay is not None and budget_delay(path, params) > 0:
                    deferred = True
                    next_query_offset = index
                    status = QueryStatus.RATE_LIMITED
                    failure = "BINANCE_HISTORY_BUDGET_DEFERRED"
                    reasons.append(failure)
                else:
                    next_query_offset = (index + 1) % 4
                if deferred:
                    raise _HistoryBudgetDeferred
                receipt = reader.read_with_receipt(path, params)
                assert_writer()
                ensure_utc_ns(receipt.received_at_ns, field="history_received_at_ns")
                if receipt.identity_hash != reader.identity.content_hash or receipt.endpoint != path:
                    raise ValueError("Binance history receipt identity mismatch")
                facts["response_hash"] = receipt.raw_payload_hash
                rows = receipt.payload
                if query_type in (QueryType.ORDER_HISTORY, QueryType.CONDITIONAL_ORDERS):
                    row = _mapping(rows, "history order")
                    if _symbol(row.get("symbol")) != symbol:
                        raise ValueError("Binance history symbol mismatch")
                    normalized = normalize_binance_order_row(row, received_at_ns=receipt.received_at_ns).as_dict()
                    local_ids = {normalized[field] for field in (
                        "clientOrderId_local", "origClientOrderId_local", "clientAlgoId_local",
                    ) if field in normalized}
                    if local_ids != {client_id}:
                        raise ValueError("Binance history client association mismatch")
                    record_binance_order_status(journal, row, received_at_ns=receipt.received_at_ns,
                                                intent_id=intent.intent_id, identity=reader.identity)
                    records = 1
                    facts["typed_order_row_hash"] = _canonical_hash(row)
                    if query_type == QueryType.ORDER_HISTORY:
                        order_associations[(symbol, _id(row.get("orderId"), "order id"))] = client_id
                    else:
                        # A triggered algo may have created an independent regular child.
                        if row.get("actualOrderId") not in (None, "", "0", 0):
                            order_associations[(symbol, _id(row["actualOrderId"], "algo child order id"))] = client_id
                    completeness = Completeness.INCOMPLETE_TRUNCATED
                else:
                    if not isinstance(rows, list) or len(rows) > 100:
                        raise ValueError("Binance history page invalid")
                    for row in rows:
                        row = _mapping(row, "history row")
                        if _symbol(row.get("symbol")) != symbol:
                            raise ValueError("Binance history row symbol mismatch")
                        row_time = row.get("time")
                        if type(row_time) is not int or not start_ms <= row_time <= end_ms:
                            raise ValueError("Binance history row outside requested interval")
                    records = len(rows)
                    if query_type == QueryType.EXECUTION_HISTORY:
                        bound_rows = [row for row in rows
                                      if (symbol, _id(row.get("orderId"), "trade order id")) in order_associations]
                        record_binance_trades(journal, reader.identity, bound_rows,
                            received_at_ns=receipt.received_at_ns, client_id_by_order_id=order_associations,
                            intent_id_by_client_id={client_id: intent.intent_id})
                        facts["unassociated_trade_rows"] = len(rows) - len(bound_rows)
                        completeness = (Completeness.INCOMPLETE_PAGINATED if len(rows) == 100
                                        else Completeness.INCOMPLETE_TRUNCATED if len(bound_rows) != len(rows)
                                        else Completeness.COMPLETE)
                    else:
                        record_binance_income(journal, reader.identity, rows, received_at_ns=receipt.received_at_ns)
                        # A symbol-filtered page is not account-wide transaction coverage.
                        completeness = (Completeness.INCOMPLETE_PAGINATED if len(rows) == 100
                                        else Completeness.INCOMPLETE_TRUNCATED)
                status = QueryStatus.SUCCESS
            except _HistoryBudgetDeferred:
                pass
            except Exception:
                # Exception text can contain a signed URL, server body or secret.
                failure = "BINANCE_HISTORY_READ_OR_ASSOCIATION_INCOMPLETE"
                reasons.append(failure)
            assert_writer()
            evidence = make_query_evidence(
                query_id=query_id, query_type=query_type,
                scope=QueryScope.ACCOUNT if query_type == QueryType.TRANSACTION_LOG else QueryScope.INSTRUMENT,
                account=reader.identity.account_scope_ref,
                instrument=None if query_type == QueryType.TRANSACTION_LOG else f"{symbol}-PERP.BINANCE",
                requested_interval_start_ns=start_ms * 1_000_000,
                requested_interval_end_ns=end_ms * 1_000_000,
                pagination_cursors=(), pages_observed=1 if receipt is not None else 0,
                total_records_returned=records, completeness=completeness, status=status,
                source_time_ns=None, receipt_time_ns=reader.clock_ns() if receipt is None else receipt.received_at_ns,
                request_ids=(), retention_segments=(), facts=facts, error_message=failure,
            )
            journal.append_reconciliation_query_evidence(evidence)
            query_ids.append(query_id)
            if deferred:
                break
        if deferred:
            break
    return BinanceExecutionHistoryPass(tuple(processed), tuple(query_ids),
                                       after_command_id if deferred or next_query_offset else
                                       commands[-1].command_id if commands else None,
                                       tuple(sorted(set(reasons))), next_query_offset=next_query_offset)


class _HistoryBudgetDeferred(Exception):
    """Local scheduling signal, never a venue absence or failed signed GET."""


class BinanceRecoveryCapabilityLedgerV1:
    """Consume separately qualified Binance source coverage without capital authority."""

    NAME = "binance_recovery_history_coverage_v1"

    def __init__(self, journal: SQLiteJournal, identity: BinanceDemoIdentity) -> None:
        self.journal = journal
        self.identity = identity

    def qualified_sources(self, *, now_ns: int) -> tuple[Any, ...]:
        evidence = self.journal.load_latest_capability_evidence().get(self.NAME)
        qualification = self.journal.load_latest_capability_qualification(self.NAME)
        if (evidence is None or qualification is None or evidence.state != EvidenceState.PASSED_TESTNET
                or evidence.environment != self.identity.environment
                or evidence.target_profile_hash != self.identity.content_hash
                or qualification.target_profile_hash != self.identity.content_hash
                or qualification.new_state != EvidenceState.PASSED_TESTNET
                or qualification.test_run_id != evidence.test_run_id
                or qualification.evidence_refs != evidence.evidence_refs
                or not qualification.qualified_by.strip()
                or not 0 <= qualification.qualified_at_ns <= now_ns
                or evidence.test_timestamp_ns is None or not 0 <= evidence.test_timestamp_ns <= now_ns):
            return ()
        result = []
        for ref in evidence.evidence_refs:
            try:
                prefix, query_id, digest = ref.split(":")
                if prefix != "query":
                    return ()
                source = self.journal.load_reconciliation_query_evidence(query_id)[0]
                facts = source.facts
                if (not source.hash_binds_payload or source.evidence_hash != digest
                        or source.account != self.identity.account_scope_ref
                        or source.status != QueryStatus.SUCCESS or source.completeness != Completeness.COMPLETE
                        or not source.has_retention_coverage
                        or facts.get("capability_name") != self.NAME
                        or facts.get("venue_identity_hash") != self.identity.content_hash
                        or facts.get("environment") != self.identity.environment
                        or facts.get("receipt_source") != "SIGNED_BINANCE_DEMO_READER"
                        or not isinstance(facts.get("endpoints"), list)
                        or not 1 <= len(facts["endpoints"]) <= 8
                        or any(not isinstance(endpoint, str) for endpoint in facts["endpoints"])
                        or source.receipt_time_ns > qualification.qualified_at_ns
                        or not isinstance(facts.get("response_hashes"), list)
                        or not facts["response_hashes"]
                        or any(not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value)
                               for value in facts["response_hashes"])):
                    return ()
                result.append(source)
            except (PersistenceError, ValueError, TypeError, IndexError, KeyError):
                return ()
        return tuple(result)

    def covers(self, *, query_type: QueryType, instrument: str | None, endpoints: tuple[str, ...], start_ns: int,
               end_ns: int, now_ns: int) -> tuple[str, ...]:
        sources = self.qualified_sources(now_ns=now_ns)
        for source in sources:
            if (source.query_type == query_type
                    and source.instrument == instrument
                    and set(endpoints) <= set(source.facts.get("endpoints", ()))
                    and any(start <= start_ns and end >= end_ns for start, end in source.retention_segments)):
                return (f"query:{source.query_id}:{source.evidence_hash}",)
        return ()


@dataclass(frozen=True)
class BinanceDemoFlatObservationV1:
    identity_hash: str
    account_fingerprint: str
    reconciliation_run_id: str
    input_query_refs: tuple[str, ...]
    source_capability_refs: tuple[str, ...]
    observed_at_ns: int
    profile: str = field(default="BINANCE_DEMO_FLAT_OBSERVATION_V1", init=False)
    capital_enabled: bool = field(default=False, init=False)
    assisted_enabled: bool = field(default=False, init=False)
    can_release_reservation: bool = field(default=False, init=False)
    can_admit_opening: bool = field(default=False, init=False)

    @property
    def content_hash(self) -> str:
        return _canonical_hash(asdict(self))


@dataclass(frozen=True)
class BinanceRecoveryCycleV1:
    reconciliation_run_id: str
    query_ids: tuple[str, ...]
    complete_for_recovery: bool
    flat_observation: BinanceDemoFlatObservationV1 | None
    reasons: tuple[str, ...]


def capture_binance_recovery_cycle(
    reader: BinanceDemoReader,
    journal: SQLiteJournal,
    *,
    symbol: str,
    writer_id: str,
    writer_epoch: int,
    runtime_instance_id: str,
    position_epoch: int,
    history_start_ns: int,
    expected_account_fingerprint: str | None = None,
    max_commands: int = 32,
    page_limit: int = 100,
    assert_writer: Callable[[], None],
    assert_quiescent: Callable[[], None],
) -> BinanceRecoveryCycleV1:
    """Capture a bounded Binance recovery run using the frozen generic evidence model.

    The selected host must be quiescent throughout this cycle. Full account open
    views detect orders and orphan protection on other symbols. One history page
    per endpoint is intentionally incomplete at its limit. Historical COMPLETE
    requires separately qualified, persisted Binance source coverage; offline
    capability evidence cannot provide it. No commands or reservations change.
    """
    symbol = _symbol(symbol)
    deadline = time.monotonic_ns() + 30_000_000_000
    started_ns = ensure_utc_ns(reader.clock_ns(), field="recovery_started_ns")
    ensure_utc_ns(history_start_ns, field="history_start_ns")
    if not 0 <= started_ns - history_start_ns <= 7 * 24 * 3600 * 1_000_000_000:
        raise ValueError("Binance recovery history window exceeded")
    if type(max_commands) is not int or not 1 <= max_commands <= 32:
        raise ValueError("Binance recovery command bound invalid")
    if type(page_limit) is not int or not 1 <= page_limit <= 100:
        raise ValueError("Binance recovery page bound invalid")
    assert_writer()
    assert_quiescent()
    identity = reader.identity
    instrument = f"{symbol}-PERP.BINANCE"
    run_id = uuid.uuid4().hex
    journal.create_reconciliation_run(ReconciliationRun(
        run_id=run_id, account=identity.account_scope_ref, instrument=instrument, writer_id=writer_id,
        writer_epoch=writer_epoch, runtime_instance_id=runtime_instance_id,
        started_at_ns=started_ns, completed_at_ns=None,
    ))
    queries: list[Any] = []
    reasons: list[str] = []
    capability = BinanceRecoveryCapabilityLedgerV1(journal, identity)
    start_ms, end_ms = history_start_ns // 1_000_000, started_ns // 1_000_000
    interval_start, interval_end = start_ms * 1_000_000, end_ms * 1_000_000
    commands = journal.load_unresolved_commands(limit=max_commands)
    overflow = bool(commands and journal.load_unresolved_commands(limit=1, after_command_id=commands[-1].command_id))
    scoped_intents: dict[str, str] = {}
    for command in commands:
        payload = json.loads(command.payload)
        if payload.get("identity_hash") != identity.content_hash or payload.get("symbol") != symbol:
            reasons.append("BINANCE_RECOVERY_COMMAND_SCOPE_UNRESOLVED")
            continue
        client_id = validate_client_order_id(payload.get("client_order_id"))
        intent = journal.load_intent(command.intent_id)
        if intent.client_order_id != client_id or command.created_at_ns < history_start_ns:
            reasons.append("BINANCE_RECOVERY_COMMAND_HISTORY_UNCOVERED")
        else:
            scoped_intents[client_id] = intent.intent_id
    if overflow:
        reasons.append("BINANCE_RECOVERY_COMMAND_BOUND_EXHAUSTED")

    def persist(query_type: QueryType, *, receipts: Sequence[Any] = (), records: int = 0,
                completeness: Completeness = Completeness.UNKNOWN,
                facts: Mapping[str, Any] | None = None, failure: str | None = None,
                current_view: bool = False, endpoints: tuple[str, ...] = ()) -> Any:
        assert_writer()
        assert_quiescent()
        now_ns = reader.clock_ns()
        refs = capability.covers(query_type=query_type,
                                instrument=None if query_type == QueryType.TRANSACTION_LOG else instrument,
                                endpoints=endpoints, start_ns=interval_start,
                                end_ns=interval_end, now_ns=now_ns) if endpoints else ()
        if not current_view and completeness == Completeness.COMPLETE and not refs:
            completeness = Completeness.INCOMPLETE_RETENTION_LIMIT
            reasons.append("BINANCE_RECOVERY_SOURCE_RETENTION_UNQUALIFIED")
        # Current full snapshot coverage is one actual receipt instant. Historical
        # coverage is never assigned to a current wallet/position/open-order read.
        receipt_ns = max((receipt.received_at_ns for receipt in receipts), default=now_ns)
        query_start, query_end = (receipt_ns, receipt_ns) if current_view else (interval_start, interval_end)
        evidence = make_query_evidence(
            query_id=uuid.uuid4().hex, query_type=query_type,
            scope=QueryScope.ACCOUNT if query_type in (QueryType.WALLET_BALANCE, QueryType.TRANSACTION_LOG)
            else QueryScope.INSTRUMENT, account=identity.account_scope_ref,
            instrument=None if query_type in (QueryType.WALLET_BALANCE, QueryType.TRANSACTION_LOG) else instrument,
            requested_interval_start_ns=query_start, requested_interval_end_ns=query_end,
            pagination_cursors=(), pages_observed=len(receipts), total_records_returned=records,
            completeness=completeness, status=QueryStatus.SUCCESS if failure is None else QueryStatus.FAILED,
            source_time_ns=None, receipt_time_ns=receipt_ns, request_ids=(),
            retention_segments=((query_start, query_end),) if completeness == Completeness.COMPLETE and (current_view or refs) else (),
            facts={"venue_identity_hash": identity.content_hash, "environment": identity.environment,
                   "receipt_source": "SIGNED_BINANCE_DEMO_READER", "account_wide_current_view": current_view,
                   "source_capability_refs": list(refs), "response_hashes": [receipt.raw_payload_hash for receipt in receipts],
                   **dict(facts or {})}, error_message=failure,
        )
        journal.append_reconciliation_query_evidence(evidence)
        journal.bind_query_to_run(run_id, evidence.query_id)
        queries.append(evidence)
        return evidence

    def read(path: str, params: Mapping[str, Any] | None = None) -> Any:
        def active() -> None:
            assert_writer()
            assert_quiescent()
            if time.monotonic_ns() >= deadline:
                raise ValueError("Binance recovery duration bound exhausted")

        active()
        wait = getattr(reader, "wait_for_read_budget", None)
        if wait is not None:
            wait(path, params, max_wait_ns=2_000_000_000, assert_active=active)
        receipt = reader.read_with_receipt(path, params)
        active()
        if receipt.identity_hash != identity.content_hash or receipt.endpoint != path:
            raise ValueError("Binance recovery receipt identity mismatch")
        return receipt

    order_associations: dict[tuple[str, str], str] = {}
    order_history_receipts: list[Any] = []
    history_records = 0
    history_complete = True
    for path in ("/fapi/v1/allOrders", "/fapi/v1/allAlgoOrders"):
        try:
            receipt = read(path, {"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": page_limit})
            order_history_receipts.append(receipt)
            rows = receipt.payload
            if not isinstance(rows, list) or len(rows) > page_limit:
                raise ValueError("Binance recovery history page invalid")
            history_records += len(rows)
            if len(rows) == page_limit:
                history_complete = False
                reasons.append("BINANCE_RECOVERY_HISTORY_PAGE_BOUND_EXHAUSTED")
            for row in rows:
                if _symbol(row.get("symbol")) != symbol:
                    raise ValueError("Binance recovery order history symbol mismatch")
                normalized = normalize_binance_order_row(row, received_at_ns=receipt.received_at_ns).as_dict()
                local_ids = {normalized[field] for field in ("clientOrderId_local", "clientAlgoId_local", "origClientOrderId_local") if field in normalized}
                if len(local_ids) != 1:
                    history_complete = False
                    continue
                local_id = next(iter(local_ids))
                history_intent = journal.load_intent_by_client_order_id(local_id)
                if history_intent is None:
                    history_complete = False
                    continue
                record_binance_order_status(journal, row, received_at_ns=receipt.received_at_ns,
                    intent_id=history_intent.intent_id, identity=reader.identity)
                scoped_intents[local_id] = history_intent.intent_id
                if path.endswith("allOrders"):
                    order_associations[(symbol, _id(row.get("orderId"), "history order id"))] = local_id
                elif row.get("actualOrderId") not in (None, "", 0, "0"):
                    order_associations[(symbol, _id(row["actualOrderId"], "history algo child id"))] = local_id
        except Exception:
            history_complete = False
            reasons.append("BINANCE_RECOVERY_ORDER_HISTORY_INCOMPLETE")
    persist(QueryType.ORDER_HISTORY, receipts=order_history_receipts, records=history_records,
            completeness=Completeness.COMPLETE if history_complete and len(order_history_receipts) == 2 and not overflow else Completeness.INCOMPLETE_TRUNCATED,
            endpoints=("/fapi/v1/allOrders", "/fapi/v1/allAlgoOrders"),
            facts={"unresolved_opening_command": any(command.outcome.value == "UNKNOWN" for command in commands)})

    for path, query_type in (("/fapi/v1/userTrades", QueryType.EXECUTION_HISTORY),
                             ("/fapi/v1/income", QueryType.TRANSACTION_LOG)):
        try:
            params: dict[str, str | int] = {"startTime": start_ms, "endTime": end_ms, "limit": page_limit}
            if query_type == QueryType.EXECUTION_HISTORY:
                params["symbol"] = symbol
            receipt = read(path, params)
            rows = receipt.payload
            if not isinstance(rows, list) or len(rows) > page_limit:
                raise ValueError("Binance recovery economic page invalid")
            for row in rows:
                row = _mapping(row, "recovery execution/economic row")
                timestamp = row.get("time")
                if type(timestamp) is not int or not start_ms <= timestamp <= end_ms:
                    raise ValueError("Binance recovery row outside requested interval")
            complete = len(rows) < page_limit
            if query_type == QueryType.EXECUTION_HISTORY:
                bound = [row for row in rows if _symbol(row.get("symbol")) == symbol
                         and (symbol, _id(row.get("orderId"), "recovery trade order id")) in order_associations]
                record_binance_trades(journal, identity, bound, received_at_ns=receipt.received_at_ns,
                                      client_id_by_order_id=order_associations, intent_id_by_client_id=scoped_intents)
                complete = complete and len(bound) == len(rows)
            else:
                record_binance_income(journal, identity, rows, received_at_ns=receipt.received_at_ns)
            persist(query_type, receipts=(receipt,), records=len(rows),
                    completeness=Completeness.COMPLETE if complete else Completeness.INCOMPLETE_PAGINATED,
                    endpoints=(path,))
        except Exception:
            reasons.append("BINANCE_RECOVERY_EXECUTION_OR_INCOME_INCOMPLETE")
            persist(query_type, failure="BINANCE_RECOVERY_EXECUTION_OR_INCOME_INCOMPLETE")

    snapshot = None

    def capture_profile() -> None:
        nonlocal snapshot
        try:
            # Preserve the exact signed receipt sequence used by the profile snapshot.
            receipts: list[Any] = []

            class SnapshotReader:
                def __init__(self) -> None:
                    self.identity = reader.identity
                    # Keep injected functions on the instance: a class-level
                    # function would be bound as a method and change its signature.
                    self.clock_ns = reader.clock_ns

                def read_with_receipt(self, path: str, params: Mapping[str, str | int] | None = None) -> Any:
                    receipt = read(path)
                    receipts.append(receipt)
                    return receipt

            snapshot = capture_account_snapshot(SnapshotReader(), expected_account_fingerprint=expected_account_fingerprint)
            positions = [row.as_dict() for row in snapshot.positions]
            selected_positions = [row for row in positions if row.get("symbol") == symbol]
            if any(row.get("positionSide") != "BOTH" for row in positions):
                raise ValueError("Binance recovery position mode unresolved")
            selected_qty = sum((_decimal(row.get("positionAmt"), "recovery position quantity") for row in selected_positions), Decimal("0"))
            global_qty = sum((abs(_decimal(row.get("positionAmt"), "recovery position quantity")) for row in positions), Decimal("0"))
            if not snapshot.eligible:
                reasons.extend(snapshot.reasons)
            persist(QueryType.POSITIONS, receipts=receipts, records=len(selected_positions), current_view=True,
                    completeness=Completeness.COMPLETE if snapshot.eligible else Completeness.UNKNOWN,
                    facts={"signed_qty": str(selected_qty), "position_epoch": position_epoch,
                           "account_absolute_position_qty": str(global_qty),
                           "unexpected_remaining_open_orders": global_qty != 0})
            persist(QueryType.WALLET_BALANCE, receipts=receipts, records=len(snapshot.balances), current_view=True,
                    completeness=Completeness.COMPLETE if snapshot.eligible and snapshot.balances else Completeness.UNKNOWN,
                    facts={"account_fingerprint": snapshot.account_fingerprint})
        except Exception:
            reasons.append("BINANCE_RECOVERY_PROFILE_INCOMPLETE")
            for query_type in (QueryType.POSITIONS, QueryType.WALLET_BALANCE):
                if not any(query.query_type == query_type for query in queries):
                    persist(query_type, failure="BINANCE_RECOVERY_PROFILE_INCOMPLETE")

    open_rows: dict[QueryType, list[Any] | None] = {}
    open_receipts: dict[QueryType, Any] = {}
    for path, query_type in (("/fapi/v1/openOrders", QueryType.OPEN_ORDERS),
                             ("/fapi/v1/openAlgoOrders", QueryType.CONDITIONAL_ORDERS)):
        if query_type == QueryType.CONDITIONAL_ORDERS:
            capture_profile()
        try:
            receipt = read(path)  # Deliberately all account symbols.
            rows = receipt.payload
            if not isinstance(rows, list) or len(rows) > 1000:
                raise ValueError("Binance account open view bound exceeded")
            for row in rows:
                _mapping(row, "account open order")
                _symbol(row.get("symbol"))
                _id(row.get("orderId", row.get("algoId")), "account open order id")
            open_rows[query_type] = rows
            open_receipts[query_type] = receipt
            persist(query_type, receipts=(receipt,), records=len(rows), current_view=True,
                    completeness=Completeness.COMPLETE,
                    facts={"remaining_open_orders": len(rows), "residual_conditional_orders": len(rows) if query_type == QueryType.CONDITIONAL_ORDERS else 0,
                           "is_current_protection": False})
        except Exception:
            open_rows[query_type] = None
            reasons.append("BINANCE_RECOVERY_ACCOUNT_OPEN_VIEW_INCOMPLETE")
            persist(query_type, failure="BINANCE_RECOVERY_ACCOUNT_OPEN_VIEW_INCOMPLETE")

    # Full conditional account view is the native protection representation. It
    # never claims equivalence to an attached full-position stop on entry.
    conditional = open_rows.get(QueryType.CONDITIONAL_ORDERS)
    conditional_receipt = open_receipts.get(QueryType.CONDITIONAL_ORDERS)
    persist(QueryType.TRADING_STOP, receipts=() if conditional_receipt is None else (conditional_receipt,),
            records=len(conditional or ()), current_view=True,
            completeness=Completeness.COMPLETE if conditional is not None else Completeness.UNKNOWN,
            facts={"native_stop_visible": bool(conditional), "protection_representation": "conditional_order",
                   "opening_protection_qualified": False, "residual_conditional_orders": len(conditional or ())})

    ended_ns = reader.clock_ns()
    assert_writer()
    assert_quiescent()
    current_receipts = [receipt.received_at_ns for receipt in open_receipts.values()]
    if snapshot is not None:
        current_receipts.extend(row.received_at_ns for row in (snapshot.account, snapshot.dual_side,
            snapshot.multi_assets, *snapshot.positions, *snapshot.symbol_configs, *snapshot.balances))
    if any(not 0 <= ended_ns - receipt_ns <= MAX_STATE_AGE_NS for receipt_ns in current_receipts):
        reasons.append("BINANCE_RECOVERY_CYCLE_RECEIPTS_STALE")
    # Completion remains a generic frozen journal operation; this adapter cannot
    # remove mandatory query types, produce capital authority or auto-resolve sends.
    if not reasons and all(query.status == QueryStatus.SUCCESS and query.completeness == Completeness.COMPLETE
                           for query in queries):
        journal.complete_reconciliation_run(run_id, ended_ns)
    run = journal.load_reconciliation_run(run_id)
    bundle = build_reconciliation_bundle(run, tuple(queries))
    current = {query.query_type: query for query in queries}
    flat = None
    required_current = (QueryType.POSITIONS, QueryType.WALLET_BALANCE, QueryType.OPEN_ORDERS,
                        QueryType.CONDITIONAL_ORDERS, QueryType.TRADING_STOP)
    if (snapshot is not None and snapshot.eligible and snapshot.account_fingerprint is not None
            and not commands and not overflow and not journal.has_unresolved_intents()
            and "BINANCE_RECOVERY_CYCLE_RECEIPTS_STALE" not in reasons
            and all(current[query_type].status == QueryStatus.SUCCESS
                    and current[query_type].completeness == Completeness.COMPLETE for query_type in required_current)
            and all(0 <= ended_ns - current[query_type].receipt_time_ns <= MAX_STATE_AGE_NS for query_type in required_current)
            and Decimal(current[QueryType.POSITIONS].facts.get("account_absolute_position_qty", "NaN")) == 0
            and current[QueryType.OPEN_ORDERS].total_records_returned == 0
            and current[QueryType.CONDITIONAL_ORDERS].total_records_returned == 0):
        source_refs = tuple(sorted({ref for query in queries for ref in query.facts.get("source_capability_refs", ())}))
        flat = BinanceDemoFlatObservationV1(identity.content_hash, snapshot.account_fingerprint, run_id,
            tuple(f"query:{query.query_id}:{query.evidence_hash}" for query in queries), source_refs, ended_ns)
        journal.append_observation(Observation(uuid.uuid4().hex, flat.profile, identity.content_hash,
            None, ended_ns, flat.content_hash, completeness="CURRENT_FLAT_ACCOUNT_OBSERVATION_ZERO_AUTHORITY"))
    return BinanceRecoveryCycleV1(run_id, tuple(query.query_id for query in queries),
                                 bundle.complete_for_recovery, flat, tuple(sorted(set(reasons))))
