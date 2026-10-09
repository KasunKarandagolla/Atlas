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
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any

from atlas.runtime.binance_demo import DemoCredential
from atlas.runtime.nautilus_boundary import PINNED_VERSION, verify_installation
from atlas.v2._serialization import sha256_json
from atlas.v2.instruments import ProductContractV2, ProductTypeV2, VenueV2

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
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_BYBIT_ORDER_STATUSES = frozenset({"Created", "New", "Rejected", "PartiallyFilled",
    "PartiallyFilledCanceled", "Filled", "Cancelled", "Untriggered", "Triggered", "Deactivated"})


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
class BybitCommandReconciliationPass:
    """Durable bounded evidence pass; it never changes a command outcome."""

    ready: bool
    observed_commands: tuple[str, ...]
    query_ids: tuple[str, ...]
    reasons: tuple[str, ...]
    next_command_id: str | None = None
    has_more_commands: bool = False


def persist_bybit_product_contract_snapshot(journal: Any, *, identity: BybitDemoIdentity,
                                           snapshot: BybitDemoSnapshot, product: ProductContractV2,
                                           instrument_metadata_receipt: BybitDemoReadReceipt,
                                           assert_writer: Callable[[], None]) -> str:
    """Bind a durable command product hash to its signed instrument revision."""
    from atlas.runtime.reconciliation_evidence import (
        Completeness,
        QueryScope,
        QueryStatus,
        QueryType,
        make_query_evidence,
    )

    if (snapshot.identity_hash != identity.content_hash or not snapshot.eligible
            or not snapshot.profile_qualified or snapshot.native_symbol != product.key.native_symbol
            or product.key.venue is not VenueV2.BYBIT
            or product.key.environment.value != identity.environment
            or product.key.product is not ProductTypeV2.LINEAR_PERPETUAL
            or instrument_metadata_receipt.identity_hash != identity.content_hash
            or instrument_metadata_receipt.endpoint != "/v5/market/instruments-info"):
        raise ValueError("Bybit product contract snapshot scope invalid")
    metadata_rows = instrument_metadata_receipt.payload.get("result", {}).get("list")
    matched = [row for row in metadata_rows if isinstance(row, Mapping)
               and row.get("symbol") == product.key.native_symbol] if isinstance(metadata_rows, list) else []
    if (len(matched) != 1 or sha256_json(dict(matched[0])) != product.key.contract_revision
            or product.key.contract_revision != product.metadata_ref):
        raise ValueError("Bybit signed metadata does not bind product contract snapshot")
    assert_writer()
    receipt_ns = max((receipt.received_at_ns for receipt in snapshot.positions), default=snapshot.captured_at_ns)
    evidence = make_query_evidence(
        query_id=uuid.uuid4().hex, query_type=QueryType.POSITIONS, scope=QueryScope.INSTRUMENT,
        account=identity.account_scope_ref, instrument=f"{product.key.native_symbol}-LINEAR.BYBIT",
        requested_interval_start_ns=receipt_ns, requested_interval_end_ns=receipt_ns,
        pagination_cursors=(), pages_observed=len(snapshot.positions),
        total_records_returned=sum(len(_result(receipt).get("list", [])) for receipt in snapshot.positions),
        completeness=Completeness.COMPLETE,
        status=QueryStatus.SUCCESS, source_time_ns=None, receipt_time_ns=receipt_ns,
        request_ids=(), retention_segments=((receipt_ns, receipt_ns),),
        facts={"venue_identity_hash": identity.content_hash, "environment": identity.environment,
            "endpoints": [receipt.endpoint for receipt in snapshot.positions],
            "receipt_hashes": [receipt.raw_payload_hash for receipt in snapshot.positions],
            "metadata_endpoint": instrument_metadata_receipt.endpoint,
            "metadata_receipt_hash": instrument_metadata_receipt.raw_payload_hash,
            "product_contract_snapshot": product.to_dict(),
            "product_contract_hash": product.content_hash,
            "instrument_key_hash": product.key.content_hash},
        error_message=None,
    )
    journal.append_reconciliation_query_evidence(evidence)
    assert_writer()
    return evidence.query_id


def reconcile_bybit_commands(reader: BybitDemoReader, journal: Any, *, native_symbol: str,
                             snapshot: BybitDemoSnapshot, product: ProductContractV2,
                             instrument_metadata_receipt: BybitDemoReadReceipt,
                             assert_writer: Callable[[], None], max_commands: int = 1,
                             after_command_id: str | None = None
                             ) -> BybitCommandReconciliationPass:
    """Persist selected-account and exact command evidence before readiness.

    A missing orderLinkId match is never treated as proof that an UNKNOWN send
    had no effect. The command stays UNKNOWN and the pass remains not ready.
    This is selected-instrument evidence, not a full-account recovery
    certificate; it grants no new execution authority.
    """
    from atlas.domain.enums import CommandOutcome, CommandType
    from atlas.domain.execution import Observation, validate_client_order_id
    from atlas.persistence.sqlite import PersistenceError
    from atlas.runtime.fill_dedup import FillRecord, OrderStatusRecord
    from atlas.runtime.reconciliation_evidence import (
        Completeness,
        QueryScope,
        QueryStatus,
        QueryType,
        make_query_evidence,
    )

    if (type(max_commands) is not int or not 1 <= max_commands <= 32
            or not isinstance(native_symbol, str) or not _SYMBOL.fullmatch(native_symbol)
            or snapshot.identity_hash != reader.identity.content_hash
            or not snapshot.eligible or not snapshot.profile_qualified
            or snapshot.native_symbol != native_symbol
            or product.key.venue is not VenueV2.BYBIT
            or product.key.environment.value != reader.identity.environment
            or product.key.product is not ProductTypeV2.LINEAR_PERPETUAL
            or product.key.native_symbol != native_symbol
            or instrument_metadata_receipt.identity_hash != reader.identity.content_hash
            or instrument_metadata_receipt.endpoint != "/v5/market/instruments-info"):
        raise PersistenceError("Bybit recovery prerequisites are not qualified")
    metadata_rows = instrument_metadata_receipt.payload.get("result", {}).get("list")
    matched_metadata = [row for row in metadata_rows if isinstance(row, Mapping)
                        and row.get("symbol") == native_symbol] if isinstance(metadata_rows, list) else []
    if (len(matched_metadata) != 1
            or sha256_json(dict(matched_metadata[0])) != product.key.contract_revision
            or product.key.contract_revision != product.metadata_ref):
        raise PersistenceError("Bybit signed metadata does not bind selected product contract")

    query_ids: list[str] = []
    reasons: list[str] = []
    instrument = f"{native_symbol}-LINEAR.BYBIT"

    def record_query(query_type: Any, *, scope: Any, receipts: tuple[BybitDemoReadReceipt, ...],
                     records: int, account: str, instrument_ref: str | None,
                     request_ids: tuple[str, ...] = (), start_ns: int | None = None,
                     end_ns: int | None = None, facts: Mapping[str, Any] | None = None,
                     failed: bool = False) -> None:
        assert_writer()
        receipt_ns = max((r.received_at_ns for r in receipts), default=reader.clock_ns())
        endpoint_hashes = [r.raw_payload_hash for r in receipts]
        evidence = make_query_evidence(
            query_id=hashlib.sha256(uuid.uuid4().bytes).hexdigest()[:32],
            query_type=query_type, scope=scope, account=account, instrument=instrument_ref,
            requested_interval_start_ns=start_ns, requested_interval_end_ns=end_ns,
            pagination_cursors=(), pages_observed=len(receipts), total_records_returned=records,
            completeness=Completeness.UNKNOWN if failed else Completeness.COMPLETE,
            status=QueryStatus.FAILED if failed else QueryStatus.SUCCESS,
            source_time_ns=None, receipt_time_ns=receipt_ns, request_ids=request_ids,
            retention_segments=(), facts={"venue_identity_hash": reader.identity.content_hash,
                "environment": reader.identity.environment, "receipt_hashes": endpoint_hashes,
                "endpoints": sorted({r.endpoint for r in receipts}),
                "receipt_times_ns": [r.received_at_ns for r in receipts],
                **dict(facts or {})},
            error_message="BYBIT_RECONCILIATION_SOURCE_INCOMPLETE" if failed else None,
        )
        journal.append_reconciliation_query_evidence(evidence)
        query_ids.append(evidence.query_id)

    def validate_receipts(path: str, receipts: tuple[BybitDemoReadReceipt, ...]) -> list[Mapping[str, Any]]:
        rows: list[Mapping[str, Any]] = []
        if not receipts:
            raise ValueError("empty Bybit receipt sequence")
        for receipt in receipts:
            assert_writer()
            if (receipt.identity_hash != reader.identity.content_hash or receipt.endpoint != path
                    or type(receipt.received_at_ns) is not int
                    or not 0 < receipt.received_at_ns <= reader.clock_ns()):
                raise ValueError("Bybit recovery receipt scope or chronology mismatch")
            result = receipt.payload.get("result")
            page_rows = result.get("list") if isinstance(result, dict) else None
            if not isinstance(page_rows, list):
                raise ValueError("Bybit recovery list response invalid")
            rows.extend(page_rows)
        return rows

    # Persist current selected-account profile chronology before asking the
    # native runtime to dispatch any queued command.
    try:
        expected_profile_paths = {"/v5/account/info", "/v5/account/wallet-balance", "/v5/position/list"}
        profile_receipts = tuple(snapshot.receipts)
        if not profile_receipts or any(r.identity_hash != reader.identity.content_hash
                                       or r.endpoint not in expected_profile_paths for r in profile_receipts):
            raise ValueError("Bybit current profile receipt scope mismatch")
        wallet = tuple(r for r in profile_receipts if r.endpoint == "/v5/account/wallet-balance")
        positions = tuple(r for r in profile_receipts if r.endpoint == "/v5/position/list")
        if len(wallet) != 1 or not positions:
            raise ValueError("Bybit current profile source set incomplete")
        for receipt in profile_receipts:
            assert_writer()
            source = "BYBIT_RECOVERY_PROFILE_" + receipt.endpoint.rsplit("/", 1)[-1].upper()
            journal.append_observation(Observation(
                uuid.uuid4().hex, source, reader.identity.content_hash,
                None, receipt.received_at_ns, receipt.raw_payload_hash, None, None,
                "EXACT_AUTHENTICATED_READ_RECEIPT"))
        wallet_rows = validate_receipts("/v5/account/wallet-balance", wallet)
        position_rows = validate_receipts("/v5/position/list", positions)
        record_query(QueryType.WALLET_BALANCE, scope=QueryScope.ACCOUNT, receipts=wallet,
                     records=len(wallet_rows), account=reader.identity.account_scope_ref,
                     instrument_ref=None, facts={"account_profile_qualified": True})
        record_query(QueryType.POSITIONS, scope=QueryScope.INSTRUMENT, receipts=positions,
                     records=len(position_rows), account=reader.identity.account_scope_ref,
                     instrument_ref=instrument, facts={"selected_symbol": native_symbol,
                         "product_contract_snapshot": product.to_dict(),
                         "product_contract_hash": product.content_hash,
                         "instrument_key_hash": product.key.content_hash,
                         "metadata_endpoint": instrument_metadata_receipt.endpoint,
                         "metadata_receipt_hash": instrument_metadata_receipt.raw_payload_hash})
    except Exception:
        reasons.append("BYBIT_CURRENT_PROFILE_EVIDENCE_INCOMPLETE")

    # Current open orders are a separate exact selected-symbol observation.
    try:
        open_receipts = reader.read_pages("/v5/order/realtime", {"category": "linear", "symbol": native_symbol})
        open_rows = validate_receipts("/v5/order/realtime", open_receipts)
        if any(not isinstance(row, Mapping) or row.get("symbol") != native_symbol for row in open_rows):
            raise ValueError("Bybit open-order scope mismatch")
        record_query(QueryType.OPEN_ORDERS, scope=QueryScope.INSTRUMENT, receipts=open_receipts,
                     records=len(open_rows), account=reader.identity.account_scope_ref,
                     instrument_ref=instrument, facts={"all_pages_observed": True})
    except Exception:
        open_rows = []
        reasons.append("BYBIT_OPEN_ORDERS_INCOMPLETE")
        record_query(QueryType.OPEN_ORDERS, scope=QueryScope.INSTRUMENT, receipts=(), records=0,
                     account=reader.identity.account_scope_ref, instrument_ref=instrument, failed=True)

    commands = journal.load_unresolved_commands(limit=max_commands, after_command_id=after_command_id)
    if not commands and after_command_id is not None:
        commands = journal.load_unresolved_commands(limit=max_commands)
    overflow = bool(commands and journal.load_unresolved_commands(limit=1,
        after_command_id=commands[-1].command_id))
    next_command_id = (None if not commands else commands[-1].command_id)
    # Older commands may refer to a point-in-time ProductContractV2 whose
    # observation timestamps differ from today's read. Recover the stored
    # immutable snapshot and compare its stable InstrumentKey/revision to the
    # current signed metadata translation.
    saved_contract_keys: dict[str, str] = {}
    for evidence in journal.load_reconciliation_query_evidence():
        facts = evidence.facts
        raw_contract = facts.get("product_contract_snapshot")
        ref = facts.get("product_contract_hash")
        if (evidence.query_type is not QueryType.POSITIONS
                or evidence.account != reader.identity.account_scope_ref
                or facts.get("venue_identity_hash") != reader.identity.content_hash
                or facts.get("metadata_endpoint") != "/v5/market/instruments-info"
                or not isinstance(raw_contract, Mapping) or not isinstance(ref, str)):
            continue
        try:
            prior_product = ProductContractV2.from_dict(dict(raw_contract))
            if (prior_product.content_hash == ref and facts.get("instrument_key_hash") == prior_product.key.content_hash
                    and prior_product.key.venue is VenueV2.BYBIT
                    and prior_product.key.environment.value == reader.identity.environment
                    and prior_product.key.product is ProductTypeV2.LINEAR_PERPETUAL
                    and prior_product.key.native_symbol == native_symbol):
                saved_contract_keys[ref] = prior_product.key.content_hash
        except (TypeError, ValueError):
            continue

    # Every live selected-symbol order must be attributable to exactly one
    # durable order-producing command. A qualified account profile alone does
    # not account for manual or stale-writer orders.
    open_orders_by_client_id: dict[str, set[str]] = {}
    for row in open_rows:
        try:
            if not isinstance(row, Mapping):
                raise ValueError("Bybit open order row is not an object")
            client_id = validate_client_order_id(row.get("orderLinkId"))
            order_id = row.get("orderId")
            if not isinstance(order_id, str) or not order_id:
                raise ValueError("Bybit open order identity is incomplete")
            open_orders_by_client_id.setdefault(client_id, set()).add(order_id)
            intent = journal.load_intent_by_client_order_id(client_id)
            if intent is None or intent.client_order_id != client_id:
                raise ValueError("Bybit open order has no durable intent")
            candidates = []
            for owner_command in journal.load_commands_for_intent(intent.intent_id):
                owner_payload = json.loads(owner_command.payload)
                if (owner_payload.get("client_order_id") == client_id
                        and owner_command.command_type in {
                            CommandType.SUBMIT_ENTRY, CommandType.SUBMIT_EXIT, CommandType.FLATTEN,
                        }):
                    candidates.append((owner_command, owner_payload))
            if len(candidates) != 1:
                raise ValueError("Bybit open order durable owner is ambiguous")
            owner_command, owner_payload = candidates[0]
            if (owner_command.send_started_at_ns is None
                    or owner_command.outcome in {CommandOutcome.DEFINITE_REJECT, CommandOutcome.RECONCILED}
                    or owner_payload.get("identity_hash") != reader.identity.content_hash
                    or owner_payload.get("symbol") != native_symbol
                    or saved_contract_keys.get(owner_payload.get("instrument_ref")) != product.key.content_hash):
                raise ValueError("Bybit open order durable owner scope is unresolved")
        except Exception:
            reasons.append("BYBIT_OPEN_ORDER_OWNER_UNRESOLVED")
    if any(len(order_ids) != 1 for order_ids in open_orders_by_client_id.values()):
        reasons.append("BYBIT_OPEN_ORDER_CLIENT_ID_AMBIGUOUS")

    observed: list[str] = []
    prior_fills: dict[str, Any] = {}
    for command in commands:
        if command.send_started_at_ns is None or command.outcome in {
                CommandOutcome.DEFINITE_REJECT, CommandOutcome.RECONCILED}:
            continue
        assert_writer()
        intent = journal.load_intent(command.intent_id)
        payload = json.loads(command.payload)
        client_id = validate_client_order_id(payload.get("client_order_id"))
        instrument_ref = payload.get("instrument_ref")
        if (client_id != intent.client_order_id or payload.get("symbol") != native_symbol
                or payload.get("identity_hash") != reader.identity.content_hash
                or not isinstance(instrument_ref, str) or not _SHA256.fullmatch(instrument_ref)):
            reasons.append("BYBIT_DURABLE_COMMAND_SCOPE_UNRESOLVED")
            continue
        if saved_contract_keys.get(instrument_ref) != product.key.content_hash:
            reasons.append("BYBIT_DURABLE_INSTRUMENT_REVISION_UNRESOLVED")
            continue
        sibling_scopes = set()
        for sibling in journal.load_commands_for_intent(intent.intent_id):
            sibling_payload = json.loads(sibling.payload)
            if sibling_payload.get("client_order_id") == client_id:
                sibling_scopes.add((sibling_payload.get("symbol"), sibling_payload.get("instrument_ref"),
                                    sibling_payload.get("identity_hash")))
        if sibling_scopes != {(native_symbol, instrument_ref, reader.identity.content_hash)}:
            reasons.append("BYBIT_DURABLE_COMMAND_SCOPE_AMBIGUOUS")
            continue
        for prior_fill in journal.load_execution_evidence(client_order_id=client_id):
            prior_fills[prior_fill.execution_id] = prior_fill

        matched_orders: dict[str, Mapping[str, Any]] = {}
        order_receipt_times: dict[str, int] = {}
        command_query_failed = False
        command_order_ambiguous = False
        for path, query_type in (("/v5/order/realtime", QueryType.OPEN_ORDERS),
                                 ("/v5/order/history", QueryType.ORDER_HISTORY)):
            receipts: tuple[BybitDemoReadReceipt, ...] = ()
            try:
                receipts = reader.read_pages(path, {"category": "linear", "symbol": native_symbol,
                                                     "orderLinkId": client_id})
                rows = validate_receipts(path, receipts)
                scoped = []
                for row in rows:
                    if not isinstance(row, Mapping) or row.get("symbol") != native_symbol:
                        raise ValueError("Bybit order row symbol mismatch")
                    if row.get("orderLinkId") != client_id:
                        raise ValueError("Bybit order row link identity mismatch")
                    order_id = row.get("orderId")
                    status = row.get("orderStatus")
                    if not isinstance(order_id, str) or not order_id or status not in _BYBIT_ORDER_STATUSES:
                        raise ValueError("Bybit order state unresolved")
                    created_ms = row.get("createdTime")
                    updated_ms = row.get("updatedTime")
                    if (type(created_ms) not in {int, str} or not str(created_ms).isdigit()
                            or type(updated_ms) not in {int, str} or not str(updated_ms).isdigit()):
                        raise ValueError("Bybit order chronology unavailable")
                    created_ns = int(str(created_ms)) * 1_000_000
                    updated_ns = int(str(updated_ms)) * 1_000_000
                    if (created_ns < command.send_started_at_ns - MAX_READ_AGE_NS
                            or updated_ns < created_ns
                            or updated_ns > max(r.received_at_ns for r in receipts)):
                        raise ValueError("Bybit order chronology outside authenticated receipt")
                    prior_order = matched_orders.get(order_id)
                    if prior_order is not None:
                        # Realtime and history can race. Do not let one source
                        # silently overwrite a conflicting economic order state.
                        state_fields = (
                            "symbol", "orderLinkId", "orderStatus", "side", "qty", "reduceOnly",
                            "positionIdx", "cumExecQty", "cumExecFee", "cumExecValue", "avgPrice",
                        )
                        if any(prior_order.get(name) != row.get(name) for name in state_fields):
                            command_order_ambiguous = True
                        else:
                            prior_updated = int(str(prior_order["updatedTime"]))
                            current_updated = int(str(row["updatedTime"]))
                            if current_updated > prior_updated:
                                matched_orders[order_id] = row
                    else:
                        matched_orders[order_id] = row
                    order_receipt_times[order_id] = max(order_receipt_times.get(order_id, 0),
                                                        max(r.received_at_ns for r in receipts))
                    scoped.append(row)
                record_query(query_type, scope=QueryScope.INSTRUMENT, receipts=receipts,
                    records=len(scoped), account=reader.identity.account_scope_ref,
                    instrument_ref=instrument, request_ids=(client_id,),
                    start_ns=command.created_at_ns, end_ns=max(command.created_at_ns, reader.clock_ns()),
                    facts={"client_order_id": client_id, "exact_order_link_id": True,
                           "order_ids": sorted(matched_orders)})
            except Exception:
                command_query_failed = True
                reasons.append("BYBIT_COMMAND_ORDER_HISTORY_INCOMPLETE")
                record_query(query_type, scope=QueryScope.INSTRUMENT, receipts=receipts, records=0,
                    account=reader.identity.account_scope_ref, instrument_ref=instrument,
                    request_ids=(client_id,), start_ns=command.created_at_ns,
                    end_ns=max(command.created_at_ns, reader.clock_ns()), failed=True,
                    facts={"client_order_id": client_id})

        if len(matched_orders) > 1:
            command_order_ambiguous = True
        if command_order_ambiguous:
            reasons.append("BYBIT_COMMAND_ORDER_ID_AMBIGUOUS")
            continue

        if not matched_orders:
            # Bybit can return an empty exact lookup for both active and retained
            # history. Absence cannot prove the send had no effect.
            reasons.append("BYBIT_UNKNOWN_COMMAND_HAS_NO_PROVEN_ORDER_STATE")
            continue

        order_execs: dict[str, list[Mapping[str, Any]]] = {order_id: [] for order_id in matched_orders}
        execution_receipt_times: dict[str, int] = {}
        for order_id, order in matched_orders.items():
            try:
                receipts = reader.read_pages("/v5/execution/list", {"category": "linear",
                    "symbol": native_symbol, "orderId": order_id})
                rows = validate_receipts("/v5/execution/list", receipts)
                execution_receipt_times[order_id] = max(r.received_at_ns for r in receipts)
                for row in rows:
                    if (not isinstance(row, Mapping) or row.get("symbol") != native_symbol
                            or row.get("orderId") != order_id or row.get("orderLinkId") != client_id):
                        raise ValueError("Bybit execution association mismatch")
                    order_execs[order_id].append(row)
                record_query(QueryType.EXECUTION_HISTORY, scope=QueryScope.INSTRUMENT,
                    receipts=receipts, records=len(rows), account=reader.identity.account_scope_ref,
                    instrument_ref=instrument, request_ids=(order_id, client_id),
                    start_ns=command.created_at_ns, end_ns=max(command.created_at_ns, reader.clock_ns()),
                    facts={"client_order_id": client_id, "order_id": order_id,
                           "all_pages_observed": True})
            except Exception:
                command_query_failed = True
                reasons.append("BYBIT_COMMAND_EXECUTION_HISTORY_INCOMPLETE")
                record_query(QueryType.EXECUTION_HISTORY, scope=QueryScope.INSTRUMENT, receipts=(), records=0,
                    account=reader.identity.account_scope_ref, instrument_ref=instrument,
                    request_ids=(order_id, client_id), start_ns=command.created_at_ns,
                    end_ns=max(command.created_at_ns, reader.clock_ns()), failed=True,
                    facts={"client_order_id": client_id, "order_id": order_id})
                continue

            for row in order_execs[order_id]:
                try:
                    exec_id = row.get("execId")
                    side = row.get("side")
                    qty = Decimal(str(row.get("execQty")))
                    price = Decimal(str(row.get("execPrice")))
                    fee = Decimal(str(row.get("execFee", "0")))
                    fee_currency = row.get("feeCurrency")
                    exec_time_ms = row.get("execTime")
                    if (not isinstance(exec_id, str) or not exec_id or side not in {"Buy", "Sell"}
                            or not qty.is_finite() or qty <= 0 or not price.is_finite() or price <= 0
                            or not fee.is_finite() or fee < 0 or not isinstance(fee_currency, str)
                            or not fee_currency or type(exec_time_ms) not in {int, str}
                            or not str(exec_time_ms).isdigit()):
                        raise ValueError("Bybit execution row incomplete")
                    trade_time_ns = int(str(exec_time_ms)) * 1_000_000
                    received_at_ns = execution_receipt_times[order_id]
                    if (command.created_at_ns > trade_time_ns or trade_time_ns > received_at_ns
                            or trade_time_ns > execution_receipt_times[order_id]):
                        raise ValueError("Bybit execution chronology outside command bounds")
                    raw_hash = hashlib.sha256(json.dumps(row, sort_keys=True, separators=(",", ":"),
                                               allow_nan=False).encode()).hexdigest()
                    fill = FillRecord(f"BYBIT:{reader.identity.content_hash}:{native_symbol}:{exec_id}",
                        order_id, client_id, intent.intent_id, instrument, side, qty, price, fee,
                        fee_currency, trade_time_ns, received_at_ns,
                        "BYBIT_RECONCILED_EXECUTION", raw_hash)
                    prior = prior_fills.get(fill.execution_id)
                    if prior is None:
                        journal.append_execution_evidence(fill)
                        prior_fills[fill.execution_id] = fill
                    elif (prior.order_id, prior.client_order_id, prior.intent_id, prior.instrument,
                          prior.side, prior.qty, prior.price, prior.fee, prior.fee_currency,
                          prior.trade_time_ns) != (fill.order_id, fill.client_order_id, fill.intent_id,
                          fill.instrument, fill.side, fill.qty, fill.price, fill.fee,
                          fill.fee_currency, fill.trade_time_ns):
                        raise ValueError("conflicting Bybit durable execution identity")
                except Exception:
                    command_query_failed = True
                    reasons.append("BYBIT_EXECUTION_ROW_UNRESOLVED")

            for order_id, order in matched_orders.items():
                try:
                    def amount(row: Mapping[str, Any], name: str) -> Decimal:
                        value = Decimal(str(row.get(name, "0") or "0"))
                        if not value.is_finite() or value < 0:
                            raise ValueError("Bybit cumulative order values invalid")
                        return value
                    raw_hash = hashlib.sha256(json.dumps(order, sort_keys=True, separators=(",", ":"),
                                               allow_nan=False).encode()).hexdigest()
                    average = amount(order, "avgPrice")
                    journal.append_order_status_observation(OrderStatusRecord(
                        order_id, client_id, intent.intent_id, str(order["orderStatus"]),
                        amount(order, "cumExecQty"), amount(order, "cumExecFee"), amount(order, "cumExecValue"),
                        average if average > 0 else None, order_receipt_times[order_id],
                        "BYBIT_RECONCILED_ORDER_HISTORY", raw_hash))
                except Exception:
                    command_query_failed = True
                    reasons.append("BYBIT_ORDER_STATUS_UNRESOLVED")

        # A positive cumulative quantity without execution rows is unresolved;
        # the result is never inferred from order status alone.
        for order_id, order in matched_orders.items():
            try:
                cumulative = Decimal(str(order.get("cumExecQty", "0") or "0"))
                executed = sum((Decimal(str(row["execQty"])) for row in order_execs[order_id]), Decimal("0"))
                if not cumulative.is_finite() or cumulative != executed:
                    command_query_failed = True
                    reasons.append("BYBIT_EXECUTION_HISTORY_DOES_NOT_COVER_ORDER_CUMULATIVE")
            except Exception:
                command_query_failed = True
                reasons.append("BYBIT_ORDER_CUMULATIVE_UNRESOLVED")
        if not command_query_failed:
            observed.append(command.command_id)

    assert_writer()
    # A one-command page can be locally complete while later commands remain.
    # The owner aggregates page success and exposes readiness only at end of
    # cycle; this helper never promotes command outcomes.
    ready = not reasons and not overflow
    return BybitCommandReconciliationPass(ready, tuple(observed), tuple(query_ids),
                                         tuple(sorted(set(reasons))), next_command_id, overflow)


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
