"""Strict, request-generation-only boundary for Binance USD-M Futures Demo.

DEMO_ONLY has its own identity and artifact paths. This module has no live
hostname, order sender, state writer, or fallback route. Existing Atlas public
market transport, risk, protection, and recovery services remain authoritative.
"""

from __future__ import annotations

import hashlib
import hmac
import re
import urllib.parse
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import Any, Mapping

from atlas.v2._serialization import sha256_json

BINANCE_FUTURES_DEMO_REST = "https://demo-fapi.binance.com"
BINANCE_FUTURES_DEMO_WS = "wss://demo-fstream.binance.com"
DEMO_ONLY_ENVIRONMENT = "DEMO_ONLY"
DEMO_READ_PATHS = frozenset({
    "/fapi/v3/account", "/fapi/v3/balance", "/fapi/v1/positionRisk",
    "/fapi/v1/positionSide/dual", "/fapi/v1/multiAssetsMargin", "/fapi/v1/symbolConfig",
    "/fapi/v1/exchangeInfo", "/fapi/v1/accountConfig", "/fapi/v1/order", "/fapi/v1/openOrders",
    "/fapi/v1/allOrders", "/fapi/v1/userTrades", "/fapi/v1/income", "/fapi/v1/algoOrder",
    "/fapi/v1/openAlgoOrders", "/fapi/v1/allAlgoOrders",
})
LIVE_CREDENTIAL_ENV_NAMES = frozenset({
    "BINANCE_API_KEY", "BINANCE_SECRET_KEY", "BINANCE_API_SECRET", "BINANCE_LIVE_API_KEY",
    "BINANCE_LIVE_API_SECRET", "BINANCE_FUTURES_API_KEY", "BINANCE_FUTURES_API_SECRET",
    "BINANCE_TESTNET_API_KEY", "BINANCE_TESTNET_API_SECRET", "ATLAS_BINANCE_DEMO_API_KEY",
    "ATLAS_BINANCE_DEMO_API_SECRET",
})
_DEMO_ENV_ALLOWED_NAMES = frozenset({
    "ATLAS_BINANCE_FUTURES_ENV", "ATLAS_BINANCE_FUTURES_DATA_ROOT",
    "BINANCE_FUTURES_DEMO_API_KEY", "BINANCE_FUTURES_DEMO_API_SECRET",
})
_OPAQUE_REF = re.compile(r"[A-Za-z0-9_.-]{1,96}\Z")
_SYMBOL = re.compile(r"[A-Z0-9]{2,32}\Z")
_ORDER_ID = re.compile(r"[A-Za-z0-9._:-]{1,36}\Z")


@dataclass(frozen=True)
class DemoOnlyIdentity:
    account_scope_ref: str
    credential_ref: str
    environment: str = DEMO_ONLY_ENVIRONMENT
    venue: str = "BINANCE"
    product: str = "USD_M_USDT_LINEAR_PERPETUAL"
    position_mode: str = "ONE_WAY"
    margin_mode: str = "ISOLATED"

    def __post_init__(self) -> None:
        if (self.environment != DEMO_ONLY_ENVIRONMENT or self.venue != "BINANCE"
                or self.product != "USD_M_USDT_LINEAR_PERPETUAL"
                or self.position_mode != "ONE_WAY" or self.margin_mode != "ISOLATED"):
            raise ValueError("unsupported DEMO_ONLY identity")
        if (not isinstance(self.account_scope_ref, str) or not _OPAQUE_REF.fullmatch(self.account_scope_ref)
                or not isinstance(self.credential_ref, str) or not _OPAQUE_REF.fullmatch(self.credential_ref)):
            raise ValueError("invalid opaque DEMO_ONLY reference")

    @property
    def content_hash(self) -> str:
        return sha256_json({"version": "BINANCE_FUTURES_DEMO_ONLY_IDENTITY_V1",
                            "account_scope_ref": self.account_scope_ref,
                            "credential_ref": self.credential_ref,
                            "environment": self.environment, "venue": self.venue, "product": self.product,
                            "position_mode": self.position_mode, "margin_mode": self.margin_mode})


@dataclass(frozen=True, repr=False, init=False)
class DemoOnlyCredentials:
    api_key: str = field(repr=False)
    api_secret: str = field(repr=False)
    source: str = field(repr=False)

    def __init__(self, api_key: str, api_secret: str) -> None:
        for value in (api_key, api_secret):
            if not isinstance(value, str) or not value or len(value) > 4096 or any(char.isspace() for char in value):
                raise ValueError("invalid DEMO_ONLY credentials")
        object.__setattr__(self, "api_key", api_key)
        object.__setattr__(self, "api_secret", api_secret)
        object.__setattr__(self, "source", "FIXTURE")

    def __repr__(self) -> str:
        return "DemoOnlyCredentials(<protected>)"

    @classmethod
    def from_environment(cls, environ: Mapping[str, str]) -> DemoOnlyCredentials:
        if environ.get("ATLAS_BINANCE_FUTURES_ENV") != DEMO_ONLY_ENVIRONMENT:
            raise ValueError("DEMO_ONLY environment identity is required")
        unapproved_binance_secrets = any(
            value and ("BINANCE" in name and ("KEY" in name or "SECRET" in name)
                       and name not in _DEMO_ENV_ALLOWED_NAMES)
            for name, value in environ.items()
        )
        endpoint_override = any(
            value and "BINANCE" in name and any(part in name for part in ("ENDPOINT", "HOST", "URL"))
            for name, value in environ.items()
        )
        conflicting_environment = any(
            value and "BINANCE" in name and any(part in name for part in ("LIVE", "TESTNET", "SHADOW"))
            for name, value in environ.items()
        )
        if any(environ.get(name) for name in LIVE_CREDENTIAL_ENV_NAMES) or unapproved_binance_secrets:
            raise ValueError("live credential environment variable is present")
        if endpoint_override or conflicting_environment:
            raise ValueError("Binance endpoint override is forbidden")
        key = environ.get("BINANCE_FUTURES_DEMO_API_KEY")
        secret = environ.get("BINANCE_FUTURES_DEMO_API_SECRET")
        if not key or not secret:
            raise ValueError("DEMO_ONLY credentials are unavailable")
        credentials = cls(key, secret)
        object.__setattr__(credentials, "source", "BINANCE_FUTURES_DEMO_ENV")
        return credentials


@dataclass(frozen=True, repr=False)
class DemoSignedReadRequestV1:
    method: str
    full_url: str = field(repr=False)
    api_key: str = field(repr=False)
    environment: str = DEMO_ONLY_ENVIRONMENT

    def __repr__(self) -> str:
        return "DemoSignedReadRequestV1(method='GET', endpoint=<DEMO_ONLY protected>)"

    def get_header(self, name: str) -> str | None:
        return self.api_key if name.lower() == "x-mbx-apikey" else None


@dataclass(frozen=True)
class DemoOrderRequestV1:
    environment: str
    symbol: str
    side: str
    quantity: str
    client_order_id: str
    reduce_only: bool
    order_type: str = "MARKET"
    venue_effect: str = "REQUEST_GENERATION_ONLY"

    def __post_init__(self) -> None:
        if self.environment != DEMO_ONLY_ENVIRONMENT or self.venue_effect != "REQUEST_GENERATION_ONLY":
            raise ValueError("order request is outside the DEMO_ONLY offline boundary")
        if (not isinstance(self.symbol, str) or not _SYMBOL.fullmatch(self.symbol)
                or not isinstance(self.side, str) or self.side not in {"BUY", "SELL"}):
            raise ValueError("invalid demo order instrument or side")
        try:
            quantity = Decimal(self.quantity)
        except (InvalidOperation, TypeError) as exc:
            raise ValueError("invalid demo order quantity") from exc
        if not quantity.is_finite() or quantity <= 0:
            raise ValueError("invalid demo order quantity")
        if not _ORDER_ID.fullmatch(self.client_order_id) or type(self.reduce_only) is not bool:
            raise ValueError("invalid demo order identity or reduction flag")
        if self.order_type not in {"MARKET", "LIMIT", "STOP_MARKET"}:
            raise ValueError("unsupported demo order type")

    @property
    def content_hash(self) -> str:
        return sha256_json({"version": "BINANCE_FUTURES_DEMO_ORDER_REQUEST_V1",
                            "environment": self.environment, "symbol": self.symbol, "side": self.side,
                            "quantity": self.quantity, "client_order_id": self.client_order_id,
                            "reduce_only": self.reduce_only, "order_type": self.order_type,
                            "venue_effect": self.venue_effect})


def build_demo_order_request(*, symbol: str, side: str, quantity: str, client_order_id: str,
                             reduce_only: bool) -> DemoOrderRequestV1:
    """Build a typed demo order fixture; no network or order submission occurs."""
    return DemoOrderRequestV1(DEMO_ONLY_ENVIRONMENT, symbol, side, quantity, client_order_id, reduce_only)


class DemoOrderState(StrEnum):
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    EXPIRED_IN_MATCH = "EXPIRED_IN_MATCH"


class DemoTransportFailure(StrEnum):
    RATE_LIMITED = "RATE_LIMITED"
    INVALID_SIGNATURE = "INVALID_SIGNATURE"
    TIMESTAMP_OUTSIDE_WINDOW = "TIMESTAMP_OUTSIDE_WINDOW"
    AUTHENTICATION_REJECTED = "AUTHENTICATION_REJECTED"
    REQUEST_REJECTED = "REQUEST_REJECTED"
    UNSAFE_REDIRECT = "UNSAFE_REDIRECT"
    VENUE_UNAVAILABLE = "VENUE_UNAVAILABLE"
    HTTP_FAILURE = "HTTP_FAILURE"


def normalize_demo_transport_failure(status_code: int, payload: Mapping[str, Any] | None = None) -> DemoTransportFailure:
    """Map Demo HTTP/API errors to safe codes without retaining venue text."""
    if type(status_code) is not int or not 100 <= status_code <= 599:
        raise ValueError("invalid Demo HTTP status")
    if 300 <= status_code <= 399:
        return DemoTransportFailure.UNSAFE_REDIRECT
    if status_code < 300:
        raise ValueError("successful Demo HTTP status is not a transport failure")
    code = payload.get("code") if isinstance(payload, Mapping) else None
    if type(code) is int:
        if code == -1022:
            return DemoTransportFailure.INVALID_SIGNATURE
        if code == -1021:
            return DemoTransportFailure.TIMESTAMP_OUTSIDE_WINDOW
        if code in {-2014, -2015}:
            return DemoTransportFailure.AUTHENTICATION_REJECTED
    if status_code in {418, 429}:
        return DemoTransportFailure.RATE_LIMITED
    if 500 <= status_code <= 599:
        return DemoTransportFailure.VENUE_UNAVAILABLE
    if status_code >= 400:
        return DemoTransportFailure.REQUEST_REJECTED
    return DemoTransportFailure.HTTP_FAILURE


def normalize_demo_order_state(response: Mapping[str, Any]) -> DemoOrderState:
    value = response.get("status")
    if not isinstance(value, str) or not value:
        raise ValueError("required order status is missing")
    try:
        return DemoOrderState(value)
    except ValueError as exc:
        raise ValueError("unknown order state") from exc


def _demo_decimal(value: Any, field_name: str, *, positive: bool = False,
                  nonnegative: bool = False) -> Decimal:
    if not isinstance(value, (str, int, Decimal)) or isinstance(value, bool):
        raise ValueError(f"invalid Demo {field_name}")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError(f"invalid Demo {field_name}") from None
    if (not parsed.is_finite() or (positive and parsed <= 0)
            or (nonnegative and parsed < 0)):
        raise ValueError(f"invalid Demo {field_name}")
    return parsed


def _demo_id(value: Any, field_name: str) -> str:
    if (not isinstance(value, (str, int)) or isinstance(value, bool)
            or not re.fullmatch(r"[0-9]{1,64}", str(value)) or int(value) <= 0):
        raise ValueError(f"invalid Demo {field_name}")
    return str(value)


@dataclass(frozen=True)
class DemoBalanceV1:
    asset: str
    wallet_balance: Decimal
    available_balance: Decimal


@dataclass(frozen=True)
class DemoPositionV1:
    symbol: str
    position_amount: Decimal
    entry_price: Decimal
    unrealized_profit: Decimal


@dataclass(frozen=True)
class DemoAccountSnapshotV1:
    environment: str
    identity_hash: str
    captured_at_ns: int
    can_trade: bool
    balances: tuple[DemoBalanceV1, ...]
    positions: tuple[DemoPositionV1, ...]


def normalize_demo_account(identity: DemoOnlyIdentity, payload: Mapping[str, Any], *,
                           captured_at_ns: int) -> DemoAccountSnapshotV1:
    if identity.environment != DEMO_ONLY_ENVIRONMENT or type(captured_at_ns) is not int or captured_at_ns <= 0:
        raise ValueError("invalid DEMO_ONLY account identity or capture time")
    if not isinstance(payload, Mapping) or type(payload.get("canTrade")) is not bool:
        raise ValueError("invalid Demo account response")
    raw_balances, raw_positions = payload.get("assets"), payload.get("positions")
    if (not isinstance(raw_balances, list) or not isinstance(raw_positions, list)
            or len(raw_balances) > 256 or len(raw_positions) > 512):
        raise ValueError("invalid Demo account rows")
    balances = tuple(
        DemoBalanceV1(
            row["asset"],
            _demo_decimal(row.get("walletBalance"), "wallet balance", nonnegative=True),
            _demo_decimal(row.get("availableBalance"), "available balance"),
        )
        for row in raw_balances
        if isinstance(row, Mapping) and isinstance(row.get("asset"), str) and row["asset"]
    )
    positions = tuple(
        DemoPositionV1(
            row["symbol"],
            _demo_decimal(row.get("positionAmt"), "position amount"),
            _demo_decimal(row.get("entryPrice"), "entry price", nonnegative=True),
            _demo_decimal(row.get("unRealizedProfit"), "unrealized profit"),
        )
        for row in raw_positions
        if isinstance(row, Mapping) and isinstance(row.get("symbol"), str) and _SYMBOL.fullmatch(row["symbol"])
    )
    if len(balances) != len(raw_balances) or len(positions) != len(raw_positions):
        raise ValueError("invalid Demo account row identity")
    return DemoAccountSnapshotV1(DEMO_ONLY_ENVIRONMENT, identity.content_hash, captured_at_ns,
                                 payload["canTrade"], balances, positions)


@dataclass(frozen=True)
class DemoInstrumentV1:
    environment: str
    identity_hash: str
    symbol: str
    status: str
    contract_type: str
    quote_asset: str
    market_min_quantity: Decimal
    market_max_quantity: Decimal
    market_quantity_step: Decimal


def normalize_demo_instrument(identity: DemoOnlyIdentity, payload: Mapping[str, Any]) -> DemoInstrumentV1:
    if identity.environment != DEMO_ONLY_ENVIRONMENT or not isinstance(payload, Mapping):
        raise ValueError("invalid DEMO_ONLY instrument response")
    symbol, status = payload.get("symbol"), payload.get("status")
    filters = payload.get("filters")
    if (not isinstance(symbol, str) or not _SYMBOL.fullmatch(symbol) or not isinstance(status, str)
            or payload.get("contractType") != "PERPETUAL" or not isinstance(payload.get("quoteAsset"), str)
            or not isinstance(filters, list)):
        raise ValueError("invalid Demo instrument identity")
    market_filters = [row for row in filters if isinstance(row, Mapping)
                      and row.get("filterType") == "MARKET_LOT_SIZE"]
    if len(market_filters) != 1:
        raise ValueError("Demo market lot size filter is missing or ambiguous")
    market_filter = market_filters[0]
    minimum = _demo_decimal(market_filter.get("minQty"), "market minimum quantity", nonnegative=True)
    maximum = _demo_decimal(market_filter.get("maxQty"), "market maximum quantity", positive=True)
    step = _demo_decimal(market_filter.get("stepSize"), "market quantity step", positive=True)
    if maximum < minimum:
        raise ValueError("invalid Demo market quantity range")
    return DemoInstrumentV1(DEMO_ONLY_ENVIRONMENT, identity.content_hash, symbol, status,
                            "PERPETUAL", payload["quoteAsset"], minimum, maximum, step)


@dataclass(frozen=True)
class DemoSymbolMarginV1:
    symbol: str
    margin_type: str
    auto_add_margin: bool
    leverage: int
    max_notional_value: Decimal


@dataclass(frozen=True)
class DemoMarginSnapshotV1:
    environment: str
    identity_hash: str
    multi_assets_margin: bool
    symbols: tuple[DemoSymbolMarginV1, ...]


def normalize_demo_margin(identity: DemoOnlyIdentity, multi_assets: Mapping[str, Any],
                          symbol_configs: list[Mapping[str, Any]]) -> DemoMarginSnapshotV1:
    if (identity.environment != DEMO_ONLY_ENVIRONMENT or not isinstance(multi_assets, Mapping)
            or type(multi_assets.get("multiAssetsMargin")) is not bool or not isinstance(symbol_configs, list)
            or len(symbol_configs) > 512):
        raise ValueError("invalid Demo margin response")
    configs: list[DemoSymbolMarginV1] = []
    for row in symbol_configs:
        if not isinstance(row, Mapping):
            raise ValueError("invalid Demo symbol margin row")
        auto_add = row.get("isAutoAddMargin")
        if auto_add in ("true", "false"):
            auto_add = auto_add == "true"
        leverage = row.get("leverage")
        symbol, margin_type = row.get("symbol"), row.get("marginType")
        if (not isinstance(symbol, str) or not _SYMBOL.fullmatch(symbol)
                or not isinstance(margin_type, str) or margin_type not in {"isolated", "cross"}
                or type(auto_add) is not bool
                or type(leverage) is not int or not 1 <= leverage <= 125):
            raise ValueError("invalid Demo symbol margin identity")
        configs.append(DemoSymbolMarginV1(
            symbol, margin_type, auto_add, leverage,
            _demo_decimal(row.get("maxNotionalValue"), "maximum notional", positive=True),
        ))
    return DemoMarginSnapshotV1(DEMO_ONLY_ENVIRONMENT, identity.content_hash,
                                multi_assets["multiAssetsMargin"], tuple(configs))


@dataclass(frozen=True)
class DemoOrderSnapshotV1:
    environment: str
    identity_hash: str
    order_id: str
    client_order_id: str
    symbol: str
    side: str
    order_type: str
    status: DemoOrderState
    original_quantity: Decimal
    executed_quantity: Decimal


def normalize_demo_order(identity: DemoOnlyIdentity, payload: Mapping[str, Any]) -> DemoOrderSnapshotV1:
    if identity.environment != DEMO_ONLY_ENVIRONMENT or not isinstance(payload, Mapping):
        raise ValueError("invalid DEMO_ONLY order response")
    order_id = _demo_id(payload.get("orderId"), "order id")
    client_id, symbol, side, order_type = (payload.get("clientOrderId"), payload.get("symbol"),
                                          payload.get("side"), payload.get("type"))
    if (not isinstance(client_id, str) or not _ORDER_ID.fullmatch(client_id)
            or not isinstance(symbol, str) or not _SYMBOL.fullmatch(symbol)
            or not isinstance(side, str) or side not in {"BUY", "SELL"}
            or not isinstance(order_type, str) or not order_type):
        raise ValueError("invalid Demo order identity")
    original = _demo_decimal(payload.get("origQty"), "original order quantity", positive=True)
    executed = _demo_decimal(payload.get("executedQty"), "executed order quantity", nonnegative=True)
    if executed > original:
        raise ValueError("Demo executed quantity exceeds original order quantity")
    return DemoOrderSnapshotV1(DEMO_ONLY_ENVIRONMENT, identity.content_hash, order_id, client_id,
                               symbol, side, order_type, normalize_demo_order_state(payload), original, executed)


@dataclass(frozen=True)
class DemoFillV1:
    execution_id: str
    order_id: str
    symbol: str
    side: str
    quantity: Decimal
    price: Decimal
    commission: Decimal
    commission_asset: str
    trade_time_ns: int


def normalize_demo_fills(identity: DemoOnlyIdentity, payload: list[Mapping[str, Any]]) -> tuple[DemoFillV1, ...]:
    if identity.environment != DEMO_ONLY_ENVIRONMENT or not isinstance(payload, list) or len(payload) > 1000:
        raise ValueError("invalid DEMO_ONLY fill response")
    result: list[DemoFillV1] = []
    for row in payload:
        if not isinstance(row, Mapping):
            raise ValueError("invalid Demo fill row")
        symbol, side, asset = row.get("symbol"), row.get("side"), row.get("commissionAsset")
        trade_ms = row.get("time")
        if (not isinstance(symbol, str) or not _SYMBOL.fullmatch(symbol)
                or not isinstance(side, str) or side not in {"BUY", "SELL"}
                or not isinstance(asset, str) or not asset or type(trade_ms) is not int or trade_ms <= 0):
            raise ValueError("invalid Demo fill identity")
        result.append(DemoFillV1(
            _demo_id(row.get("id"), "execution id"), _demo_id(row.get("orderId"), "order id"),
            symbol, side, _demo_decimal(row.get("qty"), "fill quantity", positive=True),
            _demo_decimal(row.get("price"), "fill price", positive=True),
            _demo_decimal(row.get("commission"), "fill commission", nonnegative=True),
            asset, trade_ms * 1_000_000,
        ))
    return tuple(result)


@dataclass(frozen=True)
class DemoProtectionObservationV1:
    environment: str
    identity_hash: str
    algo_id: str
    symbol: str
    side: str
    status: str
    order_type: str
    working_type: str
    trigger_price: Decimal
    is_close_position: bool

    @property
    def is_stop_market(self) -> bool:
        return self.order_type == "STOP_MARKET" and self.working_type == "MARK_PRICE"


def normalize_demo_protection(identity: DemoOnlyIdentity, payload: Mapping[str, Any]) -> DemoProtectionObservationV1:
    if identity.environment != DEMO_ONLY_ENVIRONMENT or not isinstance(payload, Mapping):
        raise ValueError("invalid DEMO_ONLY protection response")
    status = payload.get("algoStatus")
    if (not isinstance(status, str)
            or status not in {"NEW", "TRIGGERED", "FINISHED", "CANCELED", "REJECTED", "EXPIRED"}):
        raise ValueError("unknown Demo protective order state")
    symbol, side = payload.get("symbol"), payload.get("side")
    close_position = payload.get("closePosition")
    if close_position in ("true", "false"):
        close_position = close_position == "true"
    if (not isinstance(symbol, str) or not _SYMBOL.fullmatch(symbol)
            or not isinstance(side, str) or side not in {"BUY", "SELL"}
            or type(close_position) is not bool):
        raise ValueError("invalid Demo protective order identity")
    order_type, working_type = payload.get("orderType"), payload.get("workingType")
    if not isinstance(order_type, str) or not isinstance(working_type, str):
        raise ValueError("Demo protective order semantics are missing")
    return DemoProtectionObservationV1(
        DEMO_ONLY_ENVIRONMENT, identity.content_hash, _demo_id(payload.get("algoId"), "algo id"),
        symbol, side, status, order_type, working_type,
        _demo_decimal(payload.get("triggerPrice"), "protection trigger", positive=True), close_position,
    )


@dataclass(frozen=True)
class DemoOnlyPaths:
    root: Path
    database: Path
    logs: Path
    orders: Path
    account_snapshots: Path

    @classmethod
    def under(cls, data_root: str | Path) -> DemoOnlyPaths:
        base = Path(data_root)
        if base.is_symlink():
            raise ValueError("DEMO_ONLY data root cannot be a symlink")
        root = base.resolve() / "DEMO_ONLY" / "binance-futures"
        if root.exists() and root.is_symlink():
            raise ValueError("DEMO_ONLY state directory cannot be a symlink")
        result = cls(
            root, root / "binance-futures-demo.sqlite3", root / "logs",
            root / "orders", root / "account-snapshots",
        )
        paths = (result.database, result.logs, result.orders, result.account_snapshots)
        if any(path.parent != root for path in paths):
            raise ValueError("DEMO_ONLY state isolation failed")
        if any(path.exists() and path.is_symlink() for path in paths):
            raise ValueError("DEMO_ONLY state path cannot be a symlink")
        return result


class DemoOnlyAdapter:
    """Offline request builder. There is intentionally no ``send`` or submit method."""

    __slots__ = ("_identity", "_credentials", "_clock_ms")

    def __init__(self, *, identity: DemoOnlyIdentity, credentials: DemoOnlyCredentials,
                 base_url: str = BINANCE_FUTURES_DEMO_REST, clock_ms: Any,
                 allow_fixture_credentials: bool = False) -> None:
        if not isinstance(identity, DemoOnlyIdentity) or identity.environment != DEMO_ONLY_ENVIRONMENT:
            raise ValueError("DEMO_ONLY identity is required")
        if not isinstance(credentials, DemoOnlyCredentials):
            raise ValueError("DEMO_ONLY credentials are required")
        if credentials.source != "BINANCE_FUTURES_DEMO_ENV" and allow_fixture_credentials is not True:
            raise ValueError("fixture credentials are disabled outside explicit tests")
        if base_url != BINANCE_FUTURES_DEMO_REST:
            raise ValueError("DEMO_ONLY endpoint override is forbidden")
        self._identity = identity
        self._credentials = credentials
        self._clock_ms = clock_ms

    @property
    def base_url(self) -> str:
        return BINANCE_FUTURES_DEMO_REST

    @property
    def websocket_url(self) -> str:
        return BINANCE_FUTURES_DEMO_WS

    @property
    def identity(self) -> DemoOnlyIdentity:
        return self._identity

    def __repr__(self) -> str:
        return "DemoOnlyAdapter(environment='DEMO_ONLY', endpoint=<fixed>, credentials=<protected>)"

    def request(self, method: str, path: str, params: Mapping[str, Any]) -> DemoSignedReadRequestV1:
        if method != "GET":
            raise ValueError("DEMO_ONLY request builder is GET only")
        if not isinstance(path, str) or path not in DEMO_READ_PATHS:
            raise ValueError("path is not a DEMO_ONLY read endpoint")
        if not isinstance(params, Mapping):
            raise ValueError("demo read parameters must be an object")
        query_params: dict[str, str] = {}
        for key, value in params.items():
            if not isinstance(key, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9]{0,63}", key):
                raise ValueError("invalid demo read parameter")
            if key in {"signature", "timestamp"}:
                raise ValueError("signed demo parameters are adapter-owned")
            if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
                raise ValueError("invalid demo read parameter value")
            rendered = str(value)
            if not rendered or len(rendered) > 128:
                raise ValueError("invalid demo read parameter value")
            query_params[key] = rendered
        recv_window = int(query_params.get("recvWindow", "5000"))
        if not 1 <= recv_window <= 60_000:
            raise ValueError("invalid demo recvWindow")
        timestamp_ms = self._clock_ms()
        if type(timestamp_ms) is not int or timestamp_ms < 0:
            raise ValueError("invalid demo timestamp")
        query_params["recvWindow"] = str(recv_window)
        query_params["timestamp"] = str(timestamp_ms)
        unsigned = urllib.parse.urlencode(sorted(query_params.items()))
        signature = hmac.new(self._credentials.api_secret.encode("utf-8"), unsigned.encode("utf-8"),
                             hashlib.sha256).hexdigest()
        url = f"{BINANCE_FUTURES_DEMO_REST}{path}?{unsigned}&signature={signature}"
        return DemoSignedReadRequestV1("GET", url, self._credentials.api_key)

    def build_signed_read(self, path: str, params: Mapping[str, Any] | None = None) -> DemoSignedReadRequestV1:
        return self.request("GET", path, params or {})
