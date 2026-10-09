"""Pinned Binance demo mechanics, with a deliberately closed opening gate.

Nautilus owns the OMS and every order effect.  The signed reader supplements
reconciliation only.  This module never grants capital or assisted authority.
Binance's independent conditional order is not an attached Bybit full stop.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

from atlas.domain.enums import CommandOutcome, CommandType
from atlas.domain.execution import Command, validate_client_order_id
from atlas.persistence.sqlite import PersistenceError, SQLiteJournal
from atlas.runtime.nautilus_boundary import PINNED_VERSION, verify_installation
from atlas.v2.instruments import ProductContractV2, TradingStatusV2, VenueV2

DEMO_REST = "https://demo-fapi.binance.com"
TESTNET_REST = "https://testnet.binancefuture.com"
PROFILE_VERSION = "BINANCE_USDM_MARK_CLOSE_POSITION_V2"
MAX_READ_BYTES = 2 * 1024 * 1024
MAX_STATE_AGE_NS = 2_000_000_000
_REF = re.compile(r"[A-Za-z0-9_.-]{1,96}\Z")
_BROKER_PREFIX = "x-aHRE4BCj-u"
_BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"


def local_client_order_id(value: str) -> str:
    """Use the pinned adapter decoder, including its legacy-ID semantics."""
    from nautilus_trader.adapters.binance import decode_binance_futures_client_order_id

    if not isinstance(value, str) or len(value) > 36:
        raise ValueError("invalid external Binance client identity")
    try:
        decoded = str(decode_binance_futures_client_order_id(value))
        return validate_client_order_id(decoded)
    except (ValueError, TypeError):
        raise ValueError("external Binance client identity is not an ATLAS identity") from None


def external_client_order_id(value: str) -> str:
    """Encode only the frozen 32-hex ATLAS identity; verify with the real decoder.

    Nautilus rc5 uses this exact reversible UUID branch for regular and algo
    orders. Internal identities remain unchanged in the durable ATLAS journal.
    """
    local = validate_client_order_id(value)
    number = int(local, 16)
    digits = ["0"] * 22
    for index in range(21, -1, -1):
        number, digit = divmod(number, 62)
        digits[index] = _BASE62[digit]
    wire = _BROKER_PREFIX + "".join(digits)
    if number or local_client_order_id(wire) != local:
        raise ValueError("pinned Binance identity encoding failed")
    return wire


@dataclass(frozen=True)
class BinanceDemoIdentity:
    account_scope_ref: str
    credential_ref: str
    environment: str = "DEMO"
    venue: str = "BINANCE"
    product: str = "USD_M_USDT_LINEAR_PERPETUAL"
    position_mode: str = "ONE_WAY"
    margin_mode: str = "ISOLATED"

    def __post_init__(self) -> None:
        if (self.environment not in {"DEMO", "TESTNET"} or self.venue != "BINANCE"
                or self.product != "USD_M_USDT_LINEAR_PERPETUAL"
                or self.position_mode != "ONE_WAY" or self.margin_mode != "ISOLATED"):
            raise ValueError("unsupported Binance demo identity")
        for name in ("account_scope_ref", "credential_ref"):
            if not _REF.fullmatch(getattr(self, name)):
                raise ValueError("invalid opaque demo reference")

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(json.dumps(self.__dict__, sort_keys=True).encode()).hexdigest()


@dataclass(frozen=True, repr=False)
class DemoCredential:
    api_key: str = field(repr=False)
    api_secret: str = field(repr=False)

    def __post_init__(self) -> None:
        if any(not isinstance(v, str) or not v or len(v) > 4096 or any(c.isspace() for c in v)
               for v in (self.api_key, self.api_secret)):
            raise ValueError("invalid demo credential")

    def __repr__(self) -> str:
        return "DemoCredential(<protected>)"


def build_binance_demo_config(identity: BinanceDemoIdentity, credential: DemoCredential) -> Any:
    """Return a pinned native client config; construction performs no I/O."""
    if not verify_installation().installed:
        raise ValueError(f"Nautilus {PINNED_VERSION} required")
    from nautilus_trader.adapters.binance import (
        BinanceEnvironment,
        BinanceExecutionClientConfig,
        BinanceProductType,
    )
    from nautilus_trader.model import AccountId, OmsType

    environment = BinanceEnvironment.DEMO if identity.environment == "DEMO" else BinanceEnvironment.TESTNET
    return BinanceExecutionClientConfig(
        account_id=AccountId(f"BINANCE-{identity.account_scope_ref}"),
        product_type=BinanceProductType.USD_M,
        environment=environment,
        oms_type=OmsType.NETTING,
        api_key=credential.api_key,
        api_secret=credential.api_secret,
        recv_window_ms=5000,
        max_retries=0,
        use_ws_trading=False,
        treat_expired_as_canceled=False,
    )


class DemoReadError(RuntimeError):
    """Sanitized failure: signed URLs, headers and server text are never retained."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str,
                         headers: Any, newurl: str) -> None:
        return None


@dataclass(frozen=True)
class BinanceDemoReadReceipt:
    identity_hash: str
    endpoint: str
    received_at_ns: int
    raw_payload_hash: str
    canonical_payload_hash: str
    canonical_payload: str = field(repr=False)

    @property
    def payload(self) -> Any:
        return json.loads(self.canonical_payload)


class BinanceDemoReader:
    """Read-only signed USD-M lookups with exact endpoint and resource bounds."""

    PATHS = frozenset({
        "/fapi/v3/account", "/fapi/v3/positionRisk", "/fapi/v1/positionSide/dual",
        "/fapi/v1/multiAssetsMargin", "/fapi/v1/order", "/fapi/v1/openOrders",
        "/fapi/v1/allOrders", "/fapi/v1/userTrades", "/fapi/v1/income",
        "/fapi/v1/algoOrder", "/fapi/v1/openAlgoOrders", "/fapi/v1/allAlgoOrders",
        "/fapi/v1/symbolConfig",
        "/fapi/v3/balance",
        "/fapi/v1/exchangeInfo",
        "/fapi/v1/accountConfig",
    })

    def __init__(self, *, identity: BinanceDemoIdentity, credential: DemoCredential,
                 clock_ns: Callable[[], int] = time.time_ns, opener: Any = None,
                 sleep: Callable[[float], None] = time.sleep,
                 monotonic_ns: Callable[[], int] = time.monotonic_ns) -> None:
        self.identity = identity
        self._base_url = DEMO_REST if identity.environment == "DEMO" else TESTNET_REST
        self._credential = credential
        self.clock_ns = clock_ns
        self._opener = opener or urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect)
        self._budget_lock = threading.Lock()
        self._requests: deque[tuple[int, int]] = deque(maxlen=1500)
        self._sleep = sleep
        self._monotonic_ns = monotonic_ns

    @staticmethod
    def read_weight(path: str, params: Mapping[str, Any] | None = None) -> int:
        if path not in BinanceDemoReader.PATHS:
            raise ValueError("endpoint is not a demo reconciliation read")
        if path in {"/fapi/v1/openOrders", "/fapi/v1/openAlgoOrders"}:
            return 1 if (params or {}).get("symbol") else 40
        return {"/fapi/v1/positionSide/dual": 30, "/fapi/v1/multiAssetsMargin": 30,
                "/fapi/v1/income": 30}.get(path, 5)

    def read_budget_delay_ns(self, path: str, params: Mapping[str, Any] | None = None) -> int:
        """Return the earliest shared-budget delay without reserving or doing I/O."""
        weight = self.read_weight(path, params)
        at_ns = self.clock_ns() // 1_000_000 * 1_000_000
        with self._budget_lock:
            if self._requests and at_ns < self._requests[-1][0]:
                raise DemoReadError("DEMO_READ_BUDGET_CLOCK_REGRESSED")
            requests = tuple(self._requests)
            candidates = sorted({at_ns, *(at + span for at, _ in requests
                                          for span in (1_000_000_000, 60_000_000_000)
                                          if at + span > at_ns)})
            for candidate in candidates:
                recent = [(at, used) for at, used in requests if candidate - at < 60_000_000_000]
                if (len(recent) < 1500 and sum(used for _, used in recent) + weight <= 1500
                        and sum(used for at, used in recent if candidate - at < 1_000_000_000) + weight <= 60):
                    return candidate - at_ns
        raise DemoReadError("DEMO_READ_REQUEST_BUDGET_EXCEEDED")

    def wait_for_read_budget(self, path: str, params: Mapping[str, Any] | None = None,
                             *, max_wait_ns: int = 2_000_000_000,
                             assert_active: Callable[[], None] = lambda: None) -> None:
        """Bounded explicit pacing for a quiescent read coordinator.

        Ordinary reads remain fail-fast. Concurrent readers can consume budget
        after preflight; the actual read still reserves atomically before I/O.
        """
        if type(max_wait_ns) is not int or not 0 <= max_wait_ns <= 5_000_000_000:
            raise ValueError("invalid demo read budget wait bound")
        deadline = self._monotonic_ns() + max_wait_ns
        for _ in range(64):
            assert_active()
            delay = self.read_budget_delay_ns(path, params)
            if delay == 0:
                return
            remaining = deadline - self._monotonic_ns()
            if remaining <= 0 or delay > remaining:
                raise DemoReadError("DEMO_READ_BUDGET_WAIT_EXHAUSTED")
            # Recheck the writer/quiescence fence at least every 100 ms.
            self._sleep(min(delay, remaining, 100_000_000) / 1_000_000_000)
        raise DemoReadError("DEMO_READ_BUDGET_WAIT_EXHAUSTED")

    def _reserve_read_budget(self, path: str, at_ns: int,
                             params: Mapping[str, Any] | None = None) -> None:
        # Reserve before I/O: failures and late responses still consume weight.
        weight = self.read_weight(path, params)
        with self._budget_lock:
            if self._requests and at_ns < self._requests[-1][0]:
                raise DemoReadError("DEMO_READ_BUDGET_CLOCK_REGRESSED")
            while self._requests and at_ns - self._requests[0][0] >= 60_000_000_000:
                self._requests.popleft()
            if (sum(item[1] for item in self._requests) + weight > 1500
                    or sum(item[1] for item in self._requests if at_ns - item[0] < 1_000_000_000) + weight > 60
                    or len(self._requests) >= 1500):
                raise DemoReadError("DEMO_READ_REQUEST_BUDGET_EXCEEDED")
            self._requests.append((at_ns, weight))

    def read(self, path: str, params: Mapping[str, str | int] | None = None) -> Any:
        return self.read_with_receipt(path, params).payload

    def read_with_receipt(self, path: str,
                          params: Mapping[str, str | int] | None = None) -> BinanceDemoReadReceipt:
        if path not in self.PATHS:
            raise ValueError("endpoint is not a demo reconciliation read")
        values = dict(params or {})
        if {"signature", "timestamp", "recvWindow", "apiKey", "apiSecret"} & values.keys():
            raise ValueError("authentication parameters are broker-owned")
        if any(not isinstance(k, str) or not isinstance(v, (str, int)) or isinstance(v, bool)
               for k, v in values.items()):
            raise ValueError("invalid reconciliation parameters")
        if len(values) > 12 or any(len(str(v)) > 128 for v in values.values()):
            raise ValueError("reconciliation parameter budget exceeded")
        if "limit" in values and not 1 <= int(values["limit"]) <= 1000:
            raise ValueError("reconciliation page limit exceeded")
        if "startTime" in values and "endTime" in values:
            span = int(values["endTime"]) - int(values["startTime"])
            if not 0 <= span <= 7 * 24 * 3600 * 1000:
                raise ValueError("reconciliation history window exceeded")
        for name in ("origClientOrderId", "clientAlgoId"):
            if name in values:
                values[name] = external_client_order_id(local_client_order_id(str(values[name])))
        timestamp_ms = self.clock_ns() // 1_000_000
        if timestamp_ms <= 0:
            raise ValueError("invalid reconciliation clock")
        self._reserve_read_budget(path, timestamp_ms * 1_000_000, values)
        values.update(timestamp=timestamp_ms, recvWindow=5000)
        query = urllib.parse.urlencode(sorted(values.items()))
        signature = hmac.new(self._credential.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        request = urllib.request.Request(
            f"{self._base_url}{path}?{query}&signature={signature}",
            headers={"X-MBX-APIKEY": self._credential.api_key}, method="GET",
        )
        try:
            with self._opener.open(request, timeout=2.0) as response:
                if response.geturl().split("?", 1)[0] != f"{self._base_url}{path}":
                    raise DemoReadError("DEMO_ENDPOINT_IDENTITY_FAILED")
                data = response.read(MAX_READ_BYTES + 1)
                received_at_ns = self.clock_ns()
            if len(data) > MAX_READ_BYTES:
                raise DemoReadError("DEMO_RESPONSE_BUDGET_EXCEEDED")
            body = json.loads(data)
            if not isinstance(body, (dict, list)):
                raise DemoReadError("DEMO_MALFORMED_RESPONSE")
            if isinstance(body, dict) and isinstance(body.get("code"), int) and body["code"] < 0:
                raise DemoReadError("DEMO_LOOKUP_UNRESOLVED")
            canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False)
            if (self._credential.api_key in canonical or self._credential.api_secret in canonical
                    or received_at_ns < timestamp_ms * 1_000_000):
                raise DemoReadError("DEMO_RECEIPT_IDENTITY_OR_CLOCK_FAILED")
            return BinanceDemoReadReceipt(self.identity.content_hash, path, received_at_ns,
                hashlib.sha256(data).hexdigest(), hashlib.sha256(canonical.encode()).hexdigest(), canonical)
        except DemoReadError:
            raise
        except (urllib.error.URLError, OSError, ValueError, TypeError):
            raise DemoReadError("DEMO_READ_UNAVAILABLE_OR_MALFORMED") from None


@dataclass(frozen=True)
class BinanceProtectionReadback:
    verified: bool
    reason: str
    observed_at_ns: int
    algo_order_id: str | None = None


def verify_binance_protection(row: Mapping[str, Any], *, symbol: str, client_order_id: str,
                              stop: Decimal, signed_position: Decimal, now_ns: int,
                              received_at_ns: int) -> BinanceProtectionReadback:
    """Independent conditional readback, never evidence of atomic entry protection."""
    validate_client_order_id(client_order_id)
    if (signed_position == 0 or not signed_position.is_finite() or not stop.is_finite() or stop <= 0
            or not 0 <= now_ns - received_at_ns <= MAX_STATE_AGE_NS):
        return BinanceProtectionReadback(False, "STALE_OR_INVALID_POSITION_STOP", received_at_ns)
    try:
        valid = (
            row.get("symbol") == symbol and local_client_order_id(str(row.get("clientAlgoId"))) == client_order_id
            and row.get("orderType", row.get("type")) == "STOP_MARKET"
            and row.get("workingType") == "MARK_PRICE" and row.get("positionSide") == "BOTH"
            and row.get("side") == ("SELL" if signed_position > 0 else "BUY")
            and row.get("closePosition") in (True, "true")
            and row.get("algoStatus", row.get("status")) == "NEW"
            and Decimal(str(row.get("triggerPrice", row.get("stopPrice")))) == stop
            and row.get("algoId") is not None
        )
    except (InvalidOperation, ValueError, TypeError):
        valid = False
    return BinanceProtectionReadback(
        bool(valid), "VERIFIED_CURRENT_CONDITIONAL" if valid else "PROTECTION_UNCONFIRMED",
        received_at_ns, str(row["algoId"]) if valid else None,
    )


def opening_gate_reason() -> str:
    # No mutable boolean or synthetic fixture can approve this invariant.
    return "TEST GATE: BINANCE_FILL_TIME_PROTECTION_EQUIVALENCE_UNQUALIFIED"


def classify_negative_order_lookup(*, sent_started: bool, lookup_found: bool,
                                   history_complete: bool) -> CommandOutcome:
    """Absence, including complete retained history, never proves an effect was unsent."""
    if sent_started or lookup_found or not history_complete:
        return CommandOutcome.UNKNOWN
    return CommandOutcome.UNSENT


class NautilusDemoHost(Protocol):
    order_factory: Any

    def submit_order(self, order: Any, *, position_id: Any | None = None,
                     params: dict[str, Any] | None = None) -> None: ...
    def resolve_position_id(self, *, instrument_id: Any, account_id: str,
                            expected_signed_quantity: Decimal) -> Any: ...
    def cancel_order(self, order: Any) -> None: ...
    def find_order(self, client_order_id: str) -> Any: ...


@dataclass(frozen=True)
class BinanceMarketFilters:
    """Exact MARKET_LOT_SIZE receipt, distinct from the normal lot filter."""
    product_ref: str
    observed_at_ns: int
    min_qty: Decimal
    max_qty: Decimal
    qty_step: Decimal
    source_hash: str

    def __post_init__(self) -> None:
        if (not re.fullmatch(r"[0-9a-f]{64}", self.product_ref)
                or not re.fullmatch(r"[0-9a-f]{64}", self.source_hash)
                or type(self.observed_at_ns) is not int or self.observed_at_ns < 0
                or any(not value.is_finite() or value < 0
                       for value in (self.min_qty, self.max_qty, self.qty_step))
                or self.max_qty < self.min_qty or self.max_qty == 0):
            raise ValueError("invalid observed Binance market filters")

    @classmethod
    def from_metadata(cls, row: Mapping[str, Any], *, product: ProductContractV2,
                      received_at_ns: int) -> BinanceMarketFilters:
        if row.get("symbol") != product.key.native_symbol or row.get("status") != "TRADING":
            raise ValueError("market filter product scope mismatch")
        filters = row.get("filters")
        if not isinstance(filters, list):
            raise ValueError("missing market filters")
        matches = [item for item in filters if isinstance(item, Mapping)
                   and item.get("filterType") == "MARKET_LOT_SIZE"]
        if len(matches) != 1:
            raise ValueError("missing or ambiguous market lot filter")
        raw = json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return cls(product.content_hash, received_at_ns, Decimal(matches[0]["minQty"]),
            Decimal(matches[0]["maxQty"]), Decimal(matches[0]["stepSize"]), hashlib.sha256(raw.encode()).hexdigest())

    def validate(self, *, product_ref: str, quantity: Decimal, now_ns: int) -> None:
        if (self.product_ref != product_ref or not 0 <= now_ns - self.observed_at_ns <= 3_600_000_000_000
                or not self.min_qty <= quantity <= self.max_qty
                or (self.qty_step != 0 and quantity % self.qty_step != 0)):
            raise ValueError("market quantity filter or freshness failed")


class BinanceNautilusDemoPort:
    """Compile durable exact payloads into pinned OMS commands; no direct REST orders."""

    def __init__(self, identity: BinanceDemoIdentity, host: NautilusDemoHost,
                 product: ProductContractV2, *,
                 account_snapshot_getter: Callable[[], Any] | None = None,
                 market_filters: BinanceMarketFilters | None = None) -> None:
        if (product.key.venue != VenueV2.BINANCE
                or product.key.environment.value != identity.environment):
            raise ValueError("Binance demo product identity required")
        self.identity, self.host, self.product = identity, host, product
        self.account_snapshot_getter = account_snapshot_getter
        self.market_filters = market_filters

    def _account_preflight(self, command: Command, payload: Mapping[str, Any], *, now_ns: int) -> Decimal | None:
        from .binance_reconciliation import BinanceAccountSnapshot

        if self.account_snapshot_getter is None:
            raise ValueError("DEMO_ACCOUNT_PROFILE_NOT_VERIFIED")
        account = self.account_snapshot_getter()
        if (not isinstance(account, BinanceAccountSnapshot)
                or account.identity_hash != self.identity.content_hash or not account.eligible
                or not 0 <= now_ns - account.captured_at_ns <= MAX_STATE_AGE_NS
                or any(not 0 <= now_ns - row.received_at_ns <= MAX_STATE_AGE_NS
                       for row in (account.account, account.dual_side, account.multi_assets))):
            raise ValueError("DEMO_ACCOUNT_PROFILE_STALE_OR_UNQUALIFIED")
        if command.command_type == CommandType.CANCEL_ENTRY:
            return None
        configs = [row for row in account.symbol_configs
                   if row.as_dict().get("symbol") == self.product.key.native_symbol]
        if (len(configs) != 1 or configs[0].as_dict().get("marginType") != "ISOLATED"
                or not 0 <= now_ns - configs[0].received_at_ns <= MAX_STATE_AGE_NS):
            raise ValueError("DEMO_SYMBOL_MARGIN_PROFILE_UNQUALIFIED")
        positions = [row.as_dict() for row in account.positions
                     if row.as_dict().get("symbol") == self.product.key.native_symbol]
        if any(not 0 <= now_ns - row.received_at_ns <= MAX_STATE_AGE_NS for row in account.positions):
            raise ValueError("DEMO_POSITION_RECEIPT_STALE")
        if len(positions) != 1:
            raise ValueError("DEMO_POSITION_IDENTITY_NOT_RECONCILED")
        position = positions[0]
        signed = Decimal(position["positionAmt"])
        quantity = Decimal(payload["quantity"])
        if (position.get("positionSide") != "BOTH"
                or not signed.is_finite() or signed == 0 or quantity > abs(signed)
                or payload["side"] != ("SELL" if signed > 0 else "BUY")):
            raise ValueError("DEMO_REDUCTION_POSITION_SCOPE_FAILED")
        if command.command_type == CommandType.REPAIR_STOP and quantity != abs(signed):
            raise ValueError("DEMO_FULL_POSITION_STOP_QUANTITY_MISMATCH")
        return signed

    def compile_entry_ioc(self, command: Command, *, now_ns: int) -> Any:
        """Offline exact IOC compilation; this method has no submit operation.

        Opening dispatch remains closed until fill-time protection equivalence
        has an explicitly accepted qualification profile. Compilation cannot
        change direction, quantity, limit or protective stop to rescue value.
        """
        from nautilus_trader.model import ClientOrderId, InstrumentId, OrderSide, Price, Quantity, TimeInForce

        payload = json.loads(command.payload)
        fields = {"identity_hash", "instrument_ref", "symbol", "client_order_id", "side",
                  "quantity", "price", "stop", "reduce_only"}
        if (command.command_type != CommandType.SUBMIT_ENTRY or not isinstance(payload, Mapping)
                or set(payload) != fields or payload["identity_hash"] != self.identity.content_hash
                or payload["instrument_ref"] != self.product.content_hash
                or payload["symbol"] != self.product.key.native_symbol or payload["reduce_only"] is not False
                or self.product.trading_status != TradingStatusV2.TRADING
                or not max(self.product.effective_at_ns, self.product.available_at_ns) <= now_ns
                or not 0 <= now_ns - self.product.observed_at_ns <= 3_600_000_000_000):
            raise ValueError("offline entry identity or product contract failed")
        local = validate_client_order_id(payload["client_order_id"])
        quantity, price, stop = (Decimal(payload[name]) for name in ("quantity", "price", "stop"))
        side = {"BUY": OrderSide.BUY, "SELL": OrderSide.SELL}.get(payload["side"])
        if (side is None or any(not item.is_finite() or item <= 0 for item in (quantity, price, stop))
                or quantity % self.product.qty_step != 0 or quantity < self.product.min_qty
                or self.product.max_qty is not None and quantity > self.product.max_qty
                or price % self.product.tick_size != 0 or stop % self.product.tick_size != 0
                or self.product.min_notional is not None and quantity * price < self.product.min_notional
                or (side == OrderSide.BUY and stop >= price) or (side == OrderSide.SELL and stop <= price)):
            raise ValueError("offline entry exact-action filters failed")
        return self.host.order_factory.limit(
            instrument_id=InstrumentId.from_str(f"{payload['symbol']}-PERP.BINANCE"), order_side=side,
            quantity=Quantity.from_str(payload["quantity"]), price=Price.from_str(payload["price"]),
            client_order_id=ClientOrderId(local), time_in_force=TimeInForce.IOC, reduce_only=False)

    def dispatch(self, command: Command, *, now_ns: int,
                 effect_fence: Callable[[], None] | None = None) -> None:
        from nautilus_trader.model import (
            ClientOrderId,
            InstrumentId,
            OrderSide,
            Price,
            Quantity,
            TimeInForce,
            TriggerType,
        )

        payload = json.loads(command.payload)
        required = {"identity_hash", "instrument_ref", "symbol", "client_order_id", "side",
                    "quantity", "price", "stop", "reduce_only"}
        if not isinstance(payload, dict) or set(payload) != required:
            raise ValueError("invalid exact demo command")
        if (payload["identity_hash"] != self.identity.content_hash
                or payload["instrument_ref"] != self.product.content_hash
                or payload["symbol"] != self.product.key.native_symbol
                or self.product.trading_status != TradingStatusV2.TRADING
                or max(self.product.available_at_ns, self.product.effective_at_ns) > now_ns
                or now_ns - self.product.observed_at_ns > 3_600_000_000_000):
            raise ValueError("demo command identity or product freshness failed")
        client_id = validate_client_order_id(payload["client_order_id"])
        side = {"BUY": OrderSide.BUY, "SELL": OrderSide.SELL}.get(payload["side"])
        quantity = Decimal(payload["quantity"])
        is_repair_stop = command.command_type == CommandType.REPAIR_STOP
        if (side is None or not quantity.is_finite() or quantity <= 0
                or quantity % self.product.qty_step != 0
                or (not is_repair_stop and quantity < self.product.min_qty)
                or (not is_repair_stop and self.product.max_qty is not None
                    and quantity > self.product.max_qty)):
            raise ValueError("demo side or quantity filter failed")
        if command.command_type == CommandType.SUBMIT_ENTRY:
            raise ValueError(opening_gate_reason())
        if effect_fence is None:
            raise ValueError("Binance external effects require a persisted readiness fence")
        signed_position = self._account_preflight(command, payload, now_ns=now_ns)
        if command.command_type == CommandType.CANCEL_ENTRY:
            order = self.host.find_order(client_id)
            if (order is None or str(order.client_order_id) != client_id
                    or str(order.instrument_id) != f"{payload['symbol']}-PERP.BINANCE"):
                raise ValueError("missing OMS order requires reconciliation")
            effect_fence()
            self.host.cancel_order(order)
            return
        if payload["reduce_only"] is not True:
            raise ValueError("demo risk-reduction command must be reduce-only")
        if self.host.find_order(client_id) is not None:
            raise ValueError("DEMO_SUBMITTED_ORDER_ID_COLLISION")
        common = {"instrument_id": InstrumentId.from_str(f"{payload['symbol']}-PERP.BINANCE"),
                      "order_side": side, "quantity": Quantity.from_str(payload["quantity"]),
                      "reduce_only": True, "client_order_id": ClientOrderId(client_id)}
        if signed_position is None:
            raise ValueError("DEMO_REDUCTION_POSITION_UNRESOLVED")
        position_id = self.host.resolve_position_id(
            instrument_id=common["instrument_id"],
            account_id=f"BINANCE-{self.identity.account_scope_ref}",
            expected_signed_quantity=signed_position,
        )
        if command.command_type in {CommandType.FLATTEN, CommandType.SUBMIT_EXIT}:
            if payload["price"] is None:
                if self.market_filters is None:
                    raise ValueError("DEMO_MARKET_FILTER_EVIDENCE_UNAVAILABLE")
                self.market_filters.validate(product_ref=self.product.content_hash,
                    quantity=quantity, now_ns=now_ns)
                order = self.host.order_factory.market(**common)
            else:
                price = Decimal(payload["price"])
                if not price.is_finite() or price <= 0 or price % self.product.tick_size != 0:
                    raise ValueError("demo exit price filter failed")
                order = self.host.order_factory.limit(
                    **common, price=Price.from_str(payload["price"]), time_in_force=TimeInForce.IOC)
            effect_fence()
            self.host.submit_order(order, position_id=position_id)
        elif command.command_type == CommandType.REPAIR_STOP:
            stop = Decimal(payload["stop"])
            if not stop.is_finite() or stop <= 0 or stop % self.product.tick_size != 0:
                raise ValueError("demo stop price filter failed")
            order = self.host.order_factory.stop_market(
                **common, trigger_price=Price.from_str(payload["stop"]), trigger_type=TriggerType.MARK_PRICE)
            if signed_position is None:
                raise ValueError("DEMO_REPAIR_POSITION_UNRESOLVED")
            effect_fence()
            self.host.submit_order(order, position_id=position_id, params={"close_position": True})
        else:
            raise ValueError("demo command unsupported; reconciliation is read-only")


def dispatch_persisted_demo_command(*, journal: SQLiteJournal, command_id: str,
                                    port: BinanceNautilusDemoPort, now_ns: int,
                                    writer_epoch: int, assert_writer: Callable[[], None],
                                    readiness_proof: Any = None,
                                    snapshot_getter: Callable[[], Any] | None = None,
                                    generation_getter: Callable[[], int] | None = None,
                                    clock_ns: Callable[[], int] | None = None) -> Command:
    """Persist UNKNOWN before external effects and never replay an uncertain send."""
    assert_writer()
    command = journal.load_command(command_id)
    if command.send_started_at_ns is not None or command.outcome != CommandOutcome.UNSENT:
        raise PersistenceError("demo command requires reconciliation; redispatch refused")
    # Opening qualification is checked before writing send-started; there is
    # neither an opening effect nor a misleading UNKNOWN for a known refusal.
    if command.command_type == CommandType.SUBMIT_ENTRY:
        raise PersistenceError(opening_gate_reason())
    from atlas.runtime.binance_readiness import validate_binance_command_readiness

    def fence(*, send_started: bool) -> None:
        assert_writer()
        if snapshot_getter is None or generation_getter is None or clock_ns is None:
            raise PersistenceError("Binance command readiness context missing")
        validate_binance_command_readiness(readiness_proof, journal,
            identity=port.identity, product=port.product, snapshot=snapshot_getter(),
            command_id=command_id, writer_epoch=writer_epoch,
            native_generation=generation_getter(), now_ns=clock_ns(), send_started=send_started)

    with journal._transaction_lock:
        fence(send_started=False)
        journal.mark_send_started(command_id, now_ns)
        fence(send_started=True)
        try:
            port.dispatch(journal.load_command(command_id), now_ns=clock_ns(),
                          effect_fence=lambda: fence(send_started=True))
        except Exception:
            # The asynchronous OMS call is uncertain until actual readback.
            # Retain UNKNOWN and the reservation; do not retry automatically.
            return journal.load_command(command_id)
    # Strategy acceptance is an asynchronous enqueue, not a venue ACK.
    return journal.load_command(command_id)
