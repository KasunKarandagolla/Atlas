"""Bounded authenticated Bybit demo reads and pinned Nautilus configuration.

This module provides read-only account qualification and protection readback.
Ordinary order submission remains in Nautilus; this REST reader has no write
method and cannot grant production or assisted authority.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any

from atlas.runtime.binance_demo import DemoCredential
from atlas.runtime.nautilus_boundary import PINNED_VERSION, verify_installation

DEMO_REST = "https://api-demo.bybit.com"
TESTNET_REST = "https://api-testnet.bybit.com"
MAX_READ_BYTES = 2 * 1024 * 1024
MAX_PAGE_ROWS = 100
PAGE_LIMITS = {
    "/v5/order/realtime": 50,
    "/v5/order/history": 50,
    "/v5/account/transaction-log": 50,
}
MAX_TOTAL_ROWS = 1000
MAX_PAGES = 10
MAX_READ_AGE_NS = 2_000_000_000
RECV_WINDOW_MS = 5000
_REF = re.compile(r"[A-Za-z0-9_.-]{1,96}\Z")
_SYMBOL = re.compile(r"[A-Z0-9]{2,32}\Z")


@dataclass(frozen=True)
class BybitDemoIdentity:
    account_scope_ref: str
    credential_ref: str
    environment: str = "DEMO"
    venue: str = "BYBIT"
    product: str = "LINEAR"
    position_mode: str = "ONE_WAY"
    margin_mode: str = "ISOLATED"

    def __post_init__(self) -> None:
        if (self.environment not in {"DEMO", "TESTNET"} or self.venue != "BYBIT"
                or self.product != "LINEAR" or self.position_mode != "ONE_WAY"
                or self.margin_mode != "ISOLATED"):
            raise ValueError("unsupported Bybit demo identity")
        for name in ("account_scope_ref", "credential_ref"):
            if not _REF.fullmatch(getattr(self, name)):
                raise ValueError("invalid opaque demo reference")

    @property
    def content_hash(self) -> str:
        body = json.dumps(self.__dict__, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(body.encode()).hexdigest()


def build_bybit_demo_config(identity: BybitDemoIdentity, credential: DemoCredential) -> Any:
    """Construct pinned native Bybit execution configuration without I/O."""
    if not verify_installation().installed:
        raise ValueError(f"Nautilus {PINNED_VERSION} required")
    from nautilus_trader.adapters.bybit import (
        BybitEnvironment,
        BybitExecutionClientConfig,
        BybitProductType,
    )
    from nautilus_trader.model import AccountId

    env = BybitEnvironment.DEMO if identity.environment == "DEMO" else BybitEnvironment.TESTNET
    config = BybitExecutionClientConfig(
        account_id=AccountId(f"BYBIT-{identity.account_scope_ref}"),
        product_types=[BybitProductType.LINEAR],
        environment=env,
        api_key=credential.api_key,
        api_secret=credential.api_secret,
        # rc5 applies a configured margin mode via a write during connect.
        # Observe and qualify the existing mode; never mutate it on connect.
        margin_mode=None,
        max_retries=0,
        retry_delay_initial_ms=0,
        retry_delay_max_ms=0,
        heartbeat_interval_secs=10,
        recv_window_ms=RECV_WINDOW_MS,
    )
    if config.max_retries != 0 or config.environment != env:
        raise ValueError("pinned Bybit execution configuration did not retain fail-closed profile")
    return config


class BybitDemoReadError(RuntimeError):
    """Sanitized reader error. URLs, credentials and server text are discarded."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str,
                         headers: Any, newurl: str) -> None:
        return None


@dataclass(frozen=True)
class BybitDemoReadReceipt:
    identity_hash: str
    endpoint: str
    received_at_ns: int
    raw_payload_hash: str
    canonical_payload_hash: str
    canonical_payload: str = field(repr=False)
    requested_at_ns: int = 0

    @property
    def payload(self) -> Any:
        return json.loads(self.canonical_payload)


class BybitDemoReader:
    """Strict GET-only Bybit V5 reader with URL, time and payload bounds."""

    PATHS = frozenset({
        "/v5/account/info", "/v5/account/wallet-balance", "/v5/position/list",
        "/v5/market/instruments-info",
        "/v5/order/realtime", "/v5/order/history", "/v5/execution/list",
        "/v5/position/closed-pnl", "/v5/account/transaction-log",
    })
    NON_PAGED_PATHS = frozenset({"/v5/account/info", "/v5/account/wallet-balance", "/v5/market/instruments-info"})

    def __init__(self, *, identity: BybitDemoIdentity, credential: DemoCredential,
                 clock_ns: Callable[[], int] = time.time_ns, opener: Any = None) -> None:
        self.identity = identity
        self._credential = credential
        self._base_url = DEMO_REST if identity.environment == "DEMO" else TESTNET_REST
        self.clock_ns = clock_ns
        self._opener = opener or urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect)

    def read_with_receipt(self, path: str,
                          params: Mapping[str, str | int] | None = None) -> BybitDemoReadReceipt:
        if path not in self.PATHS:
            raise ValueError("endpoint is not an allowlisted Bybit demo read")
        values = dict(params or {})
        if any(not isinstance(k, str) or not isinstance(v, (str, int)) or isinstance(v, bool)
               for k, v in values.items()):
            raise ValueError("invalid reconciliation parameters")
        if (len(values) > 12 or any(len(str(v)) > 128 for v in values.values())
                or {"apiKey", "apiSecret", "sign", "signature", "timestamp", "recvWindow"} & values.keys()):
            raise ValueError("reconciliation parameter budget exceeded")
        if path == "/v5/market/instruments-info" and (
                set(values) != {"category", "symbol"} or values.get("category") != "linear"
                or not isinstance(values.get("symbol"), str) or not _SYMBOL.fullmatch(str(values["symbol"]))):
            raise ValueError("Bybit metadata requires exact selected linear symbol")
        max_limit = PAGE_LIMITS.get(path, MAX_PAGE_ROWS)
        if "limit" in values and not 1 <= int(values["limit"]) <= max_limit:
            raise ValueError("reconciliation page limit exceeded")
        if "cursor" in values and (not isinstance(values["cursor"], str) or len(values["cursor"]) > 512):
            raise ValueError("reconciliation cursor budget exceeded")
        requested_at_ns = self.clock_ns()
        timestamp_ms = requested_at_ns // 1_000_000
        if timestamp_ms <= 0:
            raise BybitDemoReadError("BYBIT_READ_CLOCK_INVALID")
        query = urllib.parse.urlencode(sorted(values.items()))
        pre_sign = f"{timestamp_ms}{self._credential.api_key}{RECV_WINDOW_MS}{query}"
        signature = hmac.new(self._credential.api_secret.encode(), pre_sign.encode(), hashlib.sha256).hexdigest()
        url = f"{self._base_url}{path}" + (f"?{query}" if query else "")
        request = urllib.request.Request(url, headers={
            "X-BAPI-API-KEY": self._credential.api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-TIMESTAMP": str(timestamp_ms),
            "X-BAPI-RECV-WINDOW": str(RECV_WINDOW_MS),
            "Content-Type": "application/json",
        }, method="GET")
        try:
            with self._opener.open(request, timeout=2.0) as response:
                if response.geturl().split("?", 1)[0] != f"{self._base_url}{path}":
                    raise BybitDemoReadError("BYBIT_ENDPOINT_IDENTITY_FAILED")
                data = response.read(MAX_READ_BYTES + 1)
                received_at_ns = self.clock_ns()
            if len(data) > MAX_READ_BYTES:
                raise BybitDemoReadError("BYBIT_RESPONSE_BUDGET_EXCEEDED")
            body = json.loads(data)
            if not isinstance(body, dict) or body.get("retCode") != 0 or "result" not in body:
                raise BybitDemoReadError("BYBIT_LOOKUP_UNRESOLVED")
            canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False)
            if (self._credential.api_key in canonical or self._credential.api_secret in canonical
                    or received_at_ns < requested_at_ns):
                raise BybitDemoReadError("BYBIT_RECEIPT_IDENTITY_OR_CLOCK_FAILED")
            return BybitDemoReadReceipt(
                self.identity.content_hash, path, received_at_ns,
                hashlib.sha256(data).hexdigest(), hashlib.sha256(canonical.encode()).hexdigest(), canonical, requested_at_ns,
            )
        except BybitDemoReadError:
            raise
        except (urllib.error.URLError, OSError, ValueError, TypeError):
            raise BybitDemoReadError("BYBIT_READ_UNAVAILABLE_OR_MALFORMED") from None

    def read_pages(self, path: str, params: Mapping[str, str | int] | None = None
                   ) -> tuple[BybitDemoReadReceipt, ...]:
        """Fetch bounded cursor pages; fail closed on repeated/overlong pagination."""
        if path not in self.PATHS or path in self.NON_PAGED_PATHS:
            raise ValueError("endpoint does not support bounded list pagination")
        query = dict(params or {})
        query.setdefault("limit", PAGE_LIMITS.get(path, MAX_PAGE_ROWS))
        receipts: list[BybitDemoReadReceipt] = []
        cursors: set[str] = set()
        rows = 0
        for _ in range(MAX_PAGES):
            receipt = self.read_with_receipt(path, query)
            receipts.append(receipt)
            result = receipt.payload["result"]
            if not isinstance(result, dict) or not isinstance(result.get("list", []), list):
                raise BybitDemoReadError("BYBIT_LIST_RESPONSE_INVALID")
            rows += len(result.get("list", []))
            if rows > MAX_TOTAL_ROWS:
                raise BybitDemoReadError("BYBIT_ROW_BUDGET_EXCEEDED")
            cursor = result.get("nextPageCursor")
            if not cursor:
                return tuple(receipts)
            if not isinstance(cursor, str) or len(cursor) > 512 or cursor in cursors:
                raise BybitDemoReadError("BYBIT_PAGINATION_UNRESOLVED")
            cursors.add(cursor)
            query["cursor"] = cursor
        raise BybitDemoReadError("BYBIT_PAGE_BUDGET_EXCEEDED")


@dataclass(frozen=True)
class BybitDemoSnapshot:
    identity_hash: str
    captured_at_ns: int
    eligible: bool
    reasons: tuple[str, ...]
    account_fingerprint: str | None
    status: str
    receipts: tuple[BybitDemoReadReceipt, ...] = field(repr=False)
    account_fingerprint_status: str = "DECLARED_SCOPE_CREDENTIAL_BINDING"
    account_identity_status: str = "UNVERIFIED_ACCOUNT_IDENTITY"
    positions: tuple[BybitDemoReadReceipt, ...] = field(default=(), repr=False)
    profile_qualified: bool = False
    native_symbol: str | None = None


def _result(receipt: BybitDemoReadReceipt) -> Mapping[str, Any]:
    value = receipt.payload.get("result")
    return value if isinstance(value, dict) else {}


def capture_bybit_account_snapshot(reader: BybitDemoReader,
                                   expected_account_fingerprint: str | None = None, *,
                                   native_symbol: str = "BTCUSDT") -> BybitDemoSnapshot:
    """Fresh current profile only; credential binding is not verified account identity.

    Query the selected symbol explicitly: settleCoin queries omit flat positions,
    so an empty account-wide position list cannot establish its position mode.
    """
    if not isinstance(native_symbol, str) or not _SYMBOL.fullmatch(native_symbol):
        raise ValueError("invalid selected Bybit symbol")
    receipts: list[BybitDemoReadReceipt] = []
    reasons: list[str] = []
    positions: tuple[BybitDemoReadReceipt, ...] = ()
    try:
        info = reader.read_with_receipt("/v5/account/info")
        receipts.append(info)
        wallet = reader.read_with_receipt("/v5/account/wallet-balance", {"accountType": "UNIFIED"})
        receipts.append(wallet)
        positions = reader.read_pages("/v5/position/list", {"category": "linear", "symbol": native_symbol})
        receipts.extend(positions)
    except (BybitDemoReadError, ValueError):
        return BybitDemoSnapshot(reader.identity.content_hash, reader.clock_ns(), False,
                                 ("authenticated Bybit current profile incomplete",), None,
                                 "BYBIT_READ_UNRESOLVED", tuple(receipts), positions=positions,
                                 native_symbol=native_symbol)
    info_result = _result(info)
    expected_endpoints = ("/v5/account/info", "/v5/account/wallet-balance", *(
        "/v5/position/list" for _ in positions))
    if any(receipt.identity_hash != reader.identity.content_hash or receipt.endpoint != endpoint
           for receipt, endpoint in zip(receipts, expected_endpoints, strict=True)):
        reasons.append("current profile receipt identity or endpoint mismatch")
    # /account/info has no UID. This binds operator-declared scope and credential,
    # not a broker account identity. /user/query-api is not DEMO allowlisted and
    # can contain credential fields; it is deliberately not called or persisted.
    fingerprint = hashlib.sha256(
        f"{reader.identity.account_scope_ref}:{hashlib.sha256(reader._credential.api_key.encode()).hexdigest()}"
        .encode()
    ).hexdigest()
    if expected_account_fingerprint is not None and fingerprint != expected_account_fingerprint:
        reasons.append("declared scope credential binding mismatch")
    status_value = info_result.get("unifiedMarginStatus")
    if type(status_value) is not int or status_value not in {3, 4, 5, 6}:
        reasons.append("unified account mode unsupported")
    if info_result.get("marginMode") != "ISOLATED_MARGIN":
        reasons.append("account margin mode is not isolated")
    wallet_rows = _result(wallet).get("list")
    if (not isinstance(wallet_rows, list) or len(wallet_rows) != 1
            or not isinstance(wallet_rows[0], dict) or wallet_rows[0].get("accountType") != "UNIFIED"):
        reasons.append("unified wallet profile incomplete")
    position_rows = [row for receipt in positions for row in _result(receipt).get("list", [])]
    if (len(position_rows) != 1 or not isinstance(position_rows[0], dict)
            or position_rows[0].get("symbol") != native_symbol
            or type(position_rows[0].get("positionIdx")) is not int
            or position_rows[0].get("positionIdx") != 0):
        reasons.append("selected symbol one-way mode unconfirmed")
    else:
        try:
            row = position_rows[0]
            size = Decimal(row.get("size", ""))
            if (not size.is_finite() or size < 0
                    or row.get("side") not in ({"", "Buy", "Sell"} if size == 0 else {"Buy", "Sell"})):
                reasons.append("position quantity or side invalid")
        except (InvalidOperation, TypeError, ValueError):
            reasons.append("position quantity or side invalid")
    captured_at_ns = min(receipt.received_at_ns for receipt in receipts)
    now_ns = reader.clock_ns()
    if any(not 0 <= now_ns - receipt.received_at_ns <= MAX_READ_AGE_NS for receipt in receipts):
        reasons.append("current profile exceeded freshness bound")
    status = "BYBIT_CURRENT_PROFILE_QUALIFIED" if not reasons else "BYBIT_DEMO_UNQUALIFIED"
    return BybitDemoSnapshot(reader.identity.content_hash, captured_at_ns, not reasons,
                             tuple(reasons), fingerprint, status, tuple(receipts), positions=positions,
                             profile_qualified=not reasons, native_symbol=native_symbol)


@dataclass(frozen=True)
class BybitHistoricalDiagnostics:
    identity_hash: str
    start_ms: int
    end_ms: int
    receipts: tuple[BybitDemoReadReceipt, ...] = field(repr=False)
    status: str = "BOUNDED_DIAGNOSTICS_NOT_RECOVERY_COMPLETENESS"


def capture_bybit_historical_diagnostics(reader: BybitDemoReader, *, native_symbol: str,
                                        start_ms: int, end_ms: int) -> BybitHistoricalDiagnostics:
    """Caller-cadenced, bounded seven-day diagnostic window, never current profile.

    Missing/retained-away records never settle an UNKNOWN send or free capital.
    The caller must schedule diagnostics separately from the 1.5-second profile.
    """
    if (not isinstance(native_symbol, str) or not _SYMBOL.fullmatch(native_symbol)
            or type(start_ms) is not int or type(end_ms) is not int
            or not 0 < start_ms <= end_ms <= reader.clock_ns() // 1_000_000
            or end_ms - start_ms > 7 * 24 * 3600 * 1000):
        raise ValueError("invalid bounded Bybit diagnostic window")
    receipts: list[BybitDemoReadReceipt] = []
    for path in ("/v5/order/history", "/v5/execution/list", "/v5/position/closed-pnl",
                 "/v5/account/transaction-log"):
        params: dict[str, str | int] = {"category": "linear", "symbol": native_symbol, "startTime": start_ms, "endTime": end_ms}
        if path == "/v5/account/transaction-log":
            params["accountType"] = "UNIFIED"
            params["type"] = "SETTLEMENT"
        receipts.extend(reader.read_pages(path, params))
    return BybitHistoricalDiagnostics(reader.identity.content_hash, start_ms, end_ms, tuple(receipts))


@dataclass(frozen=True)
class BybitProtectionReadback:
    verified: bool
    reason: str
    observed_at_ns: int
    position_idx: int = 0
    stop_loss: str | None = None


def verify_bybit_protection(position: Mapping[str, Any], *, symbol: str,
                            expected_signed_qty: Decimal, stop_price: Decimal,
                            now_ns: int, received_at_ns: int,
                            stop_order: Mapping[str, Any] | None = None,
                            stop_order_received_at_ns: int | None = None) -> BybitProtectionReadback:
    """Require both current position and exact active Full stop-order evidence.

    Position tpslMode is deprecated/always Full and position/list does not expose
    slTriggerBy. A synthetic position field alone cannot verify MarkPrice.
    """
    try:
        signed_qty = Decimal(str(position.get("size", "0")))
        stop = Decimal(str(position.get("stopLoss", "0")))
        valid = (
            _SYMBOL.fullmatch(symbol) is not None and position.get("symbol") == symbol
            and position.get("positionIdx") == 0 and position.get("tpslMode") == "Full"
            and isinstance(stop_order, Mapping)
            and stop_order.get("symbol") == symbol and stop_order.get("positionIdx") == 0
            and stop_order.get("stopOrderType") == "StopLoss"
            and stop_order.get("orderType") == "Market" and stop_order.get("triggerBy") == "MarkPrice"
            and stop_order.get("tpslMode") == "Full"
            and stop_order.get("orderStatus") == "Untriggered"
            and stop_order.get("reduceOnly") is True and stop_order.get("closeOnTrigger") is True
            and stop_order.get("side") == ("Sell" if expected_signed_qty > 0 else "Buy")
            and Decimal(str(stop_order.get("triggerPrice", "0"))) == stop_price
            and Decimal(str(stop_order.get("qty", "0"))) == abs(expected_signed_qty)
            and type(stop_order_received_at_ns) is int
            and 0 <= now_ns - stop_order_received_at_ns <= MAX_READ_AGE_NS
            and position.get("side") == ("Buy" if expected_signed_qty > 0 else "Sell")
            and signed_qty == abs(expected_signed_qty) and stop == stop_price
            and expected_signed_qty != 0 and stop_price.is_finite() and stop_price > 0
            and 0 <= now_ns - received_at_ns <= MAX_READ_AGE_NS
        )
    except (InvalidOperation, TypeError, ValueError):
        valid = False
        stop = Decimal(0)
    return BybitProtectionReadback(bool(valid),
        "VERIFIED_FULL_MARK_PRICE_STOP" if valid else "PROTECTION_UNCONFIRMED",
        received_at_ns, 0, str(stop) if valid else None)


def opening_gate_reason() -> str:
    return "TEST GATE: BYBIT_FULL_MARK_PRICE_FILL_TIME_READBACK_UNQUALIFIED"


@dataclass(frozen=True)
class BybitMarketFilters:
    """Observed Bybit market quantity filter; ordinary lot size is insufficient."""
    product_ref: str
    observed_at_ns: int
    min_qty: Decimal
    max_qty: Decimal
    qty_step: Decimal
    source_hash: str

    def __post_init__(self) -> None:
        if (not re.fullmatch(r"[0-9a-f]{64}", self.product_ref)
                or not re.fullmatch(r"[0-9a-f]{64}", self.source_hash)
                or type(self.observed_at_ns) is not int or self.observed_at_ns <= 0
                or any(not value.is_finite() or value <= 0
                       for value in (self.min_qty, self.max_qty, self.qty_step))
                or self.max_qty < self.min_qty):
            raise ValueError("invalid observed Bybit market filters")

    @classmethod
    def from_metadata(cls, row: Mapping[str, Any], *, product: Any,
                      received_at_ns: int) -> BybitMarketFilters:
        if row.get("symbol") != product.key.native_symbol or row.get("status") != "Trading":
            raise ValueError("Bybit market filter product identity mismatch")
        lot = row.get("lotSizeFilter")
        if not isinstance(lot, Mapping):
            raise ValueError("missing Bybit market lot filter")
        raw = json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return cls(product.content_hash, received_at_ns, Decimal(lot["minOrderQty"]),
                   Decimal(lot["maxMktOrderQty"]), Decimal(lot["qtyStep"]),
                   hashlib.sha256(raw.encode()).hexdigest())

    def validate(self, *, product_ref: str, quantity: Decimal, now_ns: int) -> None:
        if (self.product_ref != product_ref or not quantity.is_finite()
                or not 0 <= now_ns - self.observed_at_ns <= 3_600_000_000_000
                or not self.min_qty <= quantity <= self.max_qty or quantity % self.qty_step != 0):
            raise ValueError("Bybit market quantity filter or freshness failed")


class BybitNautilusDemoPort:
    """Exact native IOC/market reductions and cancellation; no REST ordinary orders.

    Attached Full MarkPrice entry params can be compiled offline. Opening effects
    and stop repairs stay closed until native fill-time protection/readback is
    qualified; an independent conditional order is not an equivalent full stop.
    """

    def __init__(self, identity: BybitDemoIdentity, host: Any, product: Any, *,
                 account_snapshot_getter: Callable[[], Any] | None = None,
                 market_filters: BybitMarketFilters | None = None) -> None:
        if (product.key.venue.value != "BYBIT" or product.key.environment.value != identity.environment
                or product.key.product.value != "LINEAR_PERPETUAL"):
            raise ValueError("Bybit selected linear demo product identity required")
        self.identity, self.host, self.product = identity, host, product
        self.account_snapshot_getter = account_snapshot_getter
        self.market_filters = market_filters

    def _exact_payload(self, command: Any, now_ns: int) -> Mapping[str, Any]:
        from atlas.domain.execution import validate_client_order_id
        payload = json.loads(command.payload)
        fields = {"identity_hash", "instrument_ref", "symbol", "client_order_id", "side",
                  "quantity", "price", "stop", "reduce_only"}
        if (not isinstance(payload, dict) or set(payload) != fields
                or payload["identity_hash"] != self.identity.content_hash
                or payload["instrument_ref"] != self.product.content_hash
                or payload["symbol"] != self.product.key.native_symbol
                or self.product.trading_status.value != "TRADING"
                or max(self.product.available_at_ns, self.product.effective_at_ns) > now_ns
                or not 0 <= now_ns - self.product.observed_at_ns <= 3_600_000_000_000):
            raise ValueError("Bybit exact command identity or product freshness failed")
        validate_client_order_id(payload["client_order_id"])
        quantity = Decimal(payload["quantity"])
        if (payload["side"] not in {"BUY", "SELL"} or not quantity.is_finite() or quantity <= 0
                or quantity % self.product.qty_step != 0 or quantity < self.product.min_qty
                or self.product.max_qty is not None and quantity > self.product.max_qty):
            raise ValueError("Bybit exact quantity or side filter failed")
        return payload

    def _account_preflight(self, payload: Mapping[str, Any], now_ns: int, *, cancel: bool = False) -> None:
        if self.account_snapshot_getter is None:
            raise ValueError("BYBIT_CURRENT_PROFILE_UNAVAILABLE")
        snapshot = self.account_snapshot_getter()
        if (not isinstance(snapshot, BybitDemoSnapshot) or snapshot.identity_hash != self.identity.content_hash
                or not snapshot.profile_qualified or not snapshot.eligible
                or snapshot.native_symbol != self.product.key.native_symbol
                or not snapshot.account_fingerprint
                or snapshot.account_fingerprint_status != "DECLARED_SCOPE_CREDENTIAL_BINDING"
                or not 0 <= now_ns - snapshot.captured_at_ns <= MAX_READ_AGE_NS
                or any(not 0 <= now_ns - receipt.received_at_ns <= MAX_READ_AGE_NS
                       for receipt in snapshot.receipts)):
            raise ValueError("BYBIT_CURRENT_PROFILE_STALE_OR_UNQUALIFIED")
        if cancel:
            return
        rows = [row for receipt in snapshot.positions for row in _result(receipt).get("list", [])]
        if len(rows) != 1 or rows[0].get("symbol") != payload["symbol"] or rows[0].get("positionIdx") != 0:
            raise ValueError("BYBIT_REDUCTION_POSITION_IDENTITY_UNCONFIRMED")
        size = Decimal(rows[0].get("size", ""))
        if (not size.is_finite() or size <= 0 or Decimal(payload["quantity"]) > size
                or payload["side"] != ({"Buy": "SELL", "Sell": "BUY"}.get(rows[0].get("side")))):
            raise ValueError("BYBIT_REDUCTION_POSITION_SCOPE_FAILED")

    def compile_entry_ioc(self, command: Any, *, now_ns: int) -> tuple[Any, dict[str, Any]]:
        """Build exact limit IOC and SDK-native attached stop params without an effect."""
        from nautilus_trader.model import ClientOrderId, InstrumentId, OrderSide, Price, Quantity, TimeInForce

        from atlas.domain.enums import CommandType
        payload = self._exact_payload(command, now_ns)
        if command.command_type != CommandType.SUBMIT_ENTRY or payload["reduce_only"] is not False:
            raise ValueError("Bybit offline entry command required")
        quantity, price, stop = (Decimal(payload[name]) for name in ("quantity", "price", "stop"))
        side = OrderSide.BUY if payload["side"] == "BUY" else OrderSide.SELL
        if (any(not value.is_finite() or value <= 0 for value in (price, stop))
                or price % self.product.tick_size != 0 or stop % self.product.tick_size != 0
                or self.product.min_notional is not None and quantity * price < self.product.min_notional
                or (payload["side"] == "BUY" and stop >= price)
                or (payload["side"] == "SELL" and stop <= price)):
            raise ValueError("Bybit offline entry price or stop filter failed")
        order = self.host.order_factory.limit(
            instrument_id=InstrumentId.from_str(f"{payload['symbol']}-LINEAR.BYBIT"), order_side=side,
            quantity=Quantity.from_str(payload["quantity"]), price=Price.from_str(payload["price"]),
            client_order_id=ClientOrderId(payload["client_order_id"]), time_in_force=TimeInForce.IOC,
            reduce_only=False)
        # Keys checked against pinned rc5 common/parse.rs and execution.rs.
        params = {"position_idx": 0, "stop_loss": payload["stop"], "sl_trigger_by": "MarkPrice",
                  "sl_order_type": "Market", "tpsl_mode": "Full"}
        return order, params

    def dispatch(self, command: Any, *, now_ns: int) -> None:
        from nautilus_trader.model import ClientOrderId, InstrumentId, OrderSide, Price, Quantity, TimeInForce

        from atlas.domain.enums import CommandType
        if command.command_type == CommandType.SUBMIT_ENTRY:
            raise ValueError(opening_gate_reason())
        if command.command_type == CommandType.REPAIR_STOP:
            raise ValueError("BYBIT_FULL_POSITION_PROTECTION_PORT_UNQUALIFIED")
        payload = self._exact_payload(command, now_ns)
        self._account_preflight(payload, now_ns, cancel=command.command_type == CommandType.CANCEL_ENTRY)
        if command.command_type == CommandType.CANCEL_ENTRY:
            order = self.host.find_order(payload["client_order_id"])
            if (order is None or str(order.client_order_id) != payload["client_order_id"]
                    or str(order.instrument_id) != f"{payload['symbol']}-LINEAR.BYBIT"):
                raise ValueError("Bybit OMS cancel identity requires reconciliation")
            self.host.cancel_order(order)
            return
        if command.command_type not in {CommandType.SUBMIT_EXIT, CommandType.FLATTEN} or payload["reduce_only"] is not True:
            raise ValueError("Bybit ordinary effect must be an exact reduction")
        common = {"instrument_id": InstrumentId.from_str(f"{payload['symbol']}-LINEAR.BYBIT"),
                  "order_side": OrderSide.BUY if payload["side"] == "BUY" else OrderSide.SELL,
                  "quantity": Quantity.from_str(payload["quantity"]), "reduce_only": True,
                  "client_order_id": ClientOrderId(payload["client_order_id"])}
        if payload["price"] is None:
            if not isinstance(self.market_filters, BybitMarketFilters):
                raise ValueError("BYBIT_MARKET_FILTER_EVIDENCE_UNAVAILABLE")
            self.market_filters.validate(product_ref=self.product.content_hash,
                                         quantity=Decimal(payload["quantity"]), now_ns=now_ns)
            order = self.host.order_factory.market(**common, time_in_force=TimeInForce.IOC)
        else:
            price = Decimal(payload["price"])
            if (not price.is_finite() or price <= 0 or price % self.product.tick_size != 0
                    or self.product.min_notional is not None and price * Decimal(payload["quantity"]) < self.product.min_notional):
                raise ValueError("Bybit reduction limit price filter failed")
            order = self.host.order_factory.limit(**common, price=Price.from_str(payload["price"]),
                                                  time_in_force=TimeInForce.IOC)
        self.host.submit_order(order, params={"position_idx": 0})


def dispatch_persisted_bybit_command(*, journal: Any, command_id: str, port: Any,
                                     now_ns: int, writer_epoch: int,
                                     assert_writer: Callable[[], None]) -> Any:
    """Keep a single durable ID and UNKNOWN across loss of asynchronous ACK."""
    from atlas.domain.enums import CommandOutcome, CommandType
    from atlas.persistence.sqlite import PersistenceError
    assert_writer()
    command = journal.load_command(command_id)
    intent = journal.load_intent(command.intent_id)
    if intent.writer_epoch != writer_epoch:
        raise PersistenceError("stale Bybit intent writer")
    payload = json.loads(command.payload)
    if payload.get("client_order_id") != intent.client_order_id:
        raise PersistenceError("Bybit command does not bind durable client identity")
    if command.send_started_at_ns is not None or command.outcome != CommandOutcome.UNSENT:
        raise PersistenceError("Bybit UNKNOWN command requires reconciliation; replay refused")
    if command.command_type == CommandType.SUBMIT_ENTRY:
        raise PersistenceError(opening_gate_reason())
    if command.command_type == CommandType.REPAIR_STOP:
        raise PersistenceError("BYBIT_FULL_POSITION_PROTECTION_PORT_UNQUALIFIED")
    journal.mark_send_started(command_id, now_ns)
    assert_writer()
    try:
        port.dispatch(journal.load_command(command_id), now_ns=now_ns)
    except Exception:
        return journal.load_command(command_id)
    # Native strategy enqueue is not a broker acknowledgment.
    return journal.load_command(command_id)
