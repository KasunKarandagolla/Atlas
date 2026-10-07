"""Bounded multi-venue public metadata and market observation acquisition.

This is an additive source for broad V2 observation. The legacy Bybit
BTC/ETH campaign source remains unchanged.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, cast

from .._serialization import canonical_json, sha256_json, timestamp
from ..instruments import EnvironmentV2, InstrumentKeyV2, ProductContractV2, VenueV2
from .bars import BarIntervalV2, CausalBarV2, translate_final_bar
from .binance import SOURCE_ID as BINANCE_SOURCE_ID
from .binance import BinanceUsdMPublicReaderV2, translate_agg_trades, translate_exchange_info
from .binance import translate_funding_history as translate_binance_funding
from .binance import translate_kline as translate_binance_kline
from .binance import translate_open_interest as translate_binance_oi
from .binance import translate_open_interest_history as translate_binance_oi_history
from .bybit import SOURCE_ID as BYBIT_SOURCE_ID
from .bybit import BybitPublicReaderV2
from .bybit import _result_rows as bybit_rows
from .bybit import translate_funding_history as translate_bybit_funding
from .bybit import translate_instrument_info as translate_bybit_products
from .bybit import translate_kline as translate_bybit_kline
from .bybit import translate_open_interest_history as translate_bybit_oi_history
from .bybit import translate_recent_trades as translate_bybit_trades
from .public_http import PublicDataError
from .raw import RawObservationV2

MAX_ACTIVE_CONTRACTS_V2 = 4096
MAX_CONTRACTS_PER_VENUE_V2 = 2048
MAX_METADATA_PAGES_V2 = 16
MAX_PAGE_ROWS_V2 = 1000
MAX_REQUESTS_PER_SNAPSHOT_V2 = 32
MAX_RECORDS_PER_SNAPSHOT_V2 = 16384
MAX_ACQUISITION_DURATION_NS_V2 = 5_000_000_000
MAX_METADATA_AGE_NS_V2 = 3_600_000_000_000
CHEAP_REFRESH_INTERVAL_NS_V2 = 10_000_000_000
MAX_BAR_ROWS_V2 = 200
MAX_ROTATING_ENRICHMENT_KEYS_V2 = 2
_RICH_INTERVALS = (BarIntervalV2.M1, BarIntervalV2.M15, BarIntervalV2.H1, BarIntervalV2.H4)


@dataclass(frozen=True)
class PublicInputRecordV2:
    observation: RawObservationV2
    raw_payload: bytes
    instrument_key: InstrumentKeyV2
    bar: CausalBarV2 | None = None

    def __post_init__(self) -> None:
        if not self.raw_payload:
            raise ValueError("public input record requires bounded raw payload bytes")
        if self.observation.instrument_revision != self.instrument_key.contract_revision:
            raise ValueError("public input record revision differs from its observation")
        if self.bar is not None and (not self.bar.final or self.bar.raw.record_id != self.observation.record_id):
            raise ValueError("public input bar must be final and bind the exact observation")


@dataclass(frozen=True)
class BroadPublicSnapshotV2:
    records: tuple[PublicInputRecordV2, ...]
    complete: bool
    failure_kind: str | None
    failure_reason: str | None
    latest_received_at_ns: int
    observed_at_ns: int
    request_count: int
    successful_request_count: int
    acquisition_duration_ns: int
    bootstrap_request_count: int
    successful_bootstrap_request_count: int
    source_snapshot: Mapping[str, Any]

    def __post_init__(self) -> None:
        if len(self.records) > MAX_RECORDS_PER_SNAPSHOT_V2:
            raise ValueError("broad public snapshot exceeded its fixed record bound")
        if self.complete != (self.failure_kind is None):
            raise ValueError("complete snapshot cannot carry a failure kind")
        if self.failure_kind not in (None, "INCOMPLETE", "MALFORMED", "DISCONNECTED", "RATE_LIMITED", "BUDGET_EXCEEDED"):
            raise ValueError("unsupported broad public snapshot failure kind")
        if type(self.request_count) is not int or not 0 <= self.request_count <= MAX_REQUESTS_PER_SNAPSHOT_V2:
            raise ValueError("snapshot request count exceeds its fixed budget")
        if not 0 <= self.successful_request_count <= self.request_count:
            raise ValueError("successful request count is invalid")
        if not 0 <= self.successful_bootstrap_request_count <= self.bootstrap_request_count <= MAX_METADATA_PAGES_V2 * 2:
            raise ValueError("bootstrap request accounting is invalid")
        if self.latest_received_at_ns > self.observed_at_ns:
            raise ValueError("snapshot observation time precedes the latest receipt")
        object.__setattr__(self, "records", tuple(self.records))
        object.__setattr__(self, "source_snapshot", _deep_freeze(self.source_snapshot))


class BroadPublicCycleSourceV2:
    """Credential-free broad public acquisition with explicit global budgets."""

    def __init__(
        self,
        *,
        enabled_venues: tuple[VenueV2, ...] = (VenueV2.BYBIT, VenueV2.BINANCE),
        bybit_reader: BybitPublicReaderV2 | None = None,
        binance_reader: BinanceUsdMPublicReaderV2 | None = None,
        binance_depth_reader: BinanceUsdMPublicReaderV2 | None = None,
        clock_ns: Callable[[], int] = time.time_ns,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        max_active_contracts: int = MAX_ACTIVE_CONTRACTS_V2,
        max_contracts_per_venue: int = MAX_CONTRACTS_PER_VENUE_V2,
        max_acquisition_duration_ns: int = MAX_ACQUISITION_DURATION_NS_V2,
        max_requests: int = MAX_REQUESTS_PER_SNAPSHOT_V2,
    ) -> None:
        venue_values = tuple(VenueV2(value) for value in enabled_venues)
        if not venue_values or len(set(venue_values)) != len(venue_values):
            raise ValueError("enabled venues must be nonempty and unique")
        venues = tuple(sorted(venue_values, key=lambda item: item.value))
        if max_active_contracts <= 0 or max_active_contracts > MAX_ACTIVE_CONTRACTS_V2:
            raise ValueError("active contract bound must be between 1 and the frozen maximum")
        if max_contracts_per_venue <= 0 or max_contracts_per_venue > MAX_CONTRACTS_PER_VENUE_V2:
            raise ValueError("per-venue contract bound must be between 1 and the frozen maximum")
        if max_acquisition_duration_ns <= 0 or max_acquisition_duration_ns > MAX_ACQUISITION_DURATION_NS_V2:
            raise ValueError("acquisition duration exceeds the five-second fixed maximum")
        if max_requests <= 0 or max_requests > MAX_REQUESTS_PER_SNAPSHOT_V2:
            raise ValueError("request count exceeds the fixed maximum")
        self.enabled_venues = venues
        self.bybit_reader = bybit_reader if VenueV2.BYBIT in venues else None
        self.binance_reader = binance_reader if VenueV2.BINANCE in venues else None
        if VenueV2.BYBIT in venues and self.bybit_reader is None:
            self.bybit_reader = BybitPublicReaderV2()
        if VenueV2.BINANCE in venues and self.binance_reader is None:
            self.binance_reader = BinanceUsdMPublicReaderV2()
        # A REST bridge runs concurrently with ordinary acquisition. Its
        # reusable HTTPS connection must have a separate transport owner.
        self.binance_depth_reader = (binance_depth_reader or BinanceUsdMPublicReaderV2()
                                     if VenueV2.BINANCE in venues else None)
        self.clock_ns = clock_ns
        self.monotonic_ns = monotonic_ns
        self.max_active_contracts = max_active_contracts
        self.max_contracts_per_venue = max_contracts_per_venue
        self.max_acquisition_duration_ns = max_acquisition_duration_ns
        self.max_requests = max_requests
        self._products: tuple[ProductContractV2, ...] = ()
        self._metadata_received_at: dict[VenueV2, int] = {}
        self._metadata_rows: dict[VenueV2, tuple[Mapping[str, Any], ...]] = {}
        self._metadata_row_receipts: dict[VenueV2, dict[str, int]] = {}
        self._metadata_manifest: dict[str, dict[str, Any]] = {}
        self._bootstrap_start_mono: int | None = None
        self._bootstrap_request_at_ns = 0
        self._bootstrap_requests = 0
        self._bootstrap_successes = 0
        self._bootstrap_failure: str | None = None
        self._cycle_start_mono: int | None = None
        self._rotation = 0
        self._enrichment_keys: tuple[InstrumentKeyV2, ...] = ()
        self._bar_cursors_ms: dict[str, int] = {}
        self._bar_pages: dict[str, int] = {}
        self._last_snapshot: BroadPublicSnapshotV2 | None = None
        self._last_cheap_refresh_at_ns: int | None = None
        self._cycle_due: bool | None = None
        self._cycle_metadata_requests = 0

    @property
    def required_source_ids(self) -> tuple[str, ...]:
        return tuple(sorted(
            source for venue, source in ((VenueV2.BYBIT, BYBIT_SOURCE_ID), (VenueV2.BINANCE, BINANCE_SOURCE_ID))
            if venue in self.enabled_venues
        ))

    @property
    def current_products(self) -> tuple[ProductContractV2, ...]:
        return self._products

    def metadata_received_at_ns(self, venue: VenueV2) -> int | None:
        return self._metadata_received_at.get(VenueV2(venue))

    def acquire_depth_snapshot(self, key: InstrumentKeyV2, now_ns: int) -> tuple[bytes, int]:
        """Read one bounded unauthenticated Binance book snapshot for the WS bridge."""
        if key.venue != VenueV2.BINANCE or VenueV2.BINANCE not in self.enabled_venues:
            raise ValueError("Binance depth snapshot key is outside the enabled venue scope")
        if key not in {product.key for product in self._products if product.trading_status.value == "TRADING"}:
            raise ValueError("Binance depth snapshot requires a current active product revision")
        timestamp(now_ns, field="depth snapshot request time")
        reader = self.binance_depth_reader
        if reader is None:
            raise RuntimeError("BINANCE_PUBLIC_READER_UNAVAILABLE")
        response = self._read_response(
            lambda: reader.client.get("/fapi/v1/depth", {"symbol": key.native_symbol, "limit": 1000}),
            request_at_ns=now_ns,
        )
        if response.received_at_ns > now_ns + 5_000_000_000:
            raise ValueError("BINANCE_DEPTH_SNAPSHOT_RECEIPT_FUTURE")
        return response.raw_body, response.received_at_ns

    @property
    def current_source_snapshot(self) -> Mapping[str, Any]:
        return self._last_snapshot.source_snapshot if self._last_snapshot is not None else MappingProxyType({})

    def export_state(self) -> dict[str, Any]:
        """Return the small durable scheduler cursor for caller-owned persistence."""
        return {"schema_version": 2, "rotation_cursor": self._rotation,
                "enabled_venues": [venue.value for venue in self.enabled_venues],
                "bar_cursors_ms": dict(sorted(self._bar_cursors_ms.items())),
                "bar_pages": dict(sorted(self._bar_pages.items())),
                "last_cheap_refresh_at_ns": self._last_cheap_refresh_at_ns}

    def set_enrichment_keys(self, keys: tuple[InstrumentKeyV2, ...]) -> None:
        """Bind the deterministic finite history workset without a symbol allowlist."""
        if len(keys) > 24 or len(set(keys)) != len(keys):
            raise ValueError("BROAD_ENRICHMENT_WORKSET_OVERFLOW")
        self._enrichment_keys = tuple(sorted(keys, key=lambda key: key.to_canonical_json()))

    def restore_state(self, state: Mapping[str, Any]) -> None:
        if (set(state) != {"schema_version", "rotation_cursor", "enabled_venues", "bar_cursors_ms", "bar_pages",
                           "last_cheap_refresh_at_ns"}
                or state.get("schema_version") != 2
                or state.get("enabled_venues") != [venue.value for venue in self.enabled_venues]
                or type(state.get("rotation_cursor")) is not int or state["rotation_cursor"] < 0
                or not isinstance(state.get("bar_cursors_ms"), Mapping)
                or not isinstance(state.get("bar_pages"), Mapping)
                or state.get("last_cheap_refresh_at_ns") is not None
                and (type(state["last_cheap_refresh_at_ns"]) is not int or state["last_cheap_refresh_at_ns"] < 0)):
            raise ValueError("broad public scheduler state identity is invalid")
        cursors = dict(state["bar_cursors_ms"])
        pages = dict(state["bar_pages"])
        if any(not isinstance(key, str) or type(value) is not int or value < 0 for key, value in cursors.items()):
            raise ValueError("broad public bar cursor identity is invalid")
        if any(not isinstance(key, str) or type(value) is not int or value < 0 for key, value in pages.items()):
            raise ValueError("broad public bar page accounting is invalid")
        if (len(cursors) > MAX_ACTIVE_CONTRACTS_V2 * len(_RICH_INTERVALS)
                or len(pages) > MAX_ACTIVE_CONTRACTS_V2 * len(_RICH_INTERVALS)
                or set(pages) != set(cursors)):
            raise ValueError("broad public bar cursor population exceeds its fixed bound")
        self._rotation = state["rotation_cursor"]
        self._bar_cursors_ms = cursors
        self._bar_pages = pages
        self._last_cheap_refresh_at_ns = state["last_cheap_refresh_at_ns"]

    def _expired(self, venue: VenueV2, now_ns: int) -> bool:
        receipt = self._metadata_received_at.get(venue)
        return receipt is None or now_ns < receipt or now_ns - receipt > MAX_METADATA_AGE_NS_V2

    def bootstrap_products(self, *, now_ns: int) -> tuple[ProductContractV2, ...]:
        if type(now_ns) is not int or now_ns < 0:
            raise ValueError("now_ns must be a nonnegative timestamp")
        self._bootstrap_start_mono = self.monotonic_ns()
        self._bootstrap_request_at_ns = now_ns
        self._bootstrap_requests = 0
        self._bootstrap_successes = 0
        self._bootstrap_failure = None
        products_by_key: dict[str, ProductContractV2] = {}
        manifests: dict[str, dict[str, Any]] = {}
        received_updates = dict(self._metadata_received_at)
        row_updates = dict(self._metadata_rows)
        row_receipt_updates = dict(self._metadata_row_receipts)
        attempted_venues: set[VenueV2] = set()
        previous_products = {(p.key.venue, p.key.native_symbol): p for p in self._products}
        try:
            for venue in self.enabled_venues:
                if not self._expired(venue, now_ns):
                    for item in self._products:
                        if item.key.venue == venue:
                            products_by_key[item.key.to_canonical_json()] = item
                    manifests[venue.value] = dict(self._metadata_manifest.get(venue.value, {}))
                    continue
                attempted_venues.add(venue)
                venue_products, rows, received, manifest, row_receipts = self._bootstrap_venue(venue)
                # A network receipt legitimately follows the request start.
                # Bind it to the actual completion clock, preserving its own
                # later contract availability for the controller to adopt.
                completed_at_ns = self.clock_ns()
                if type(completed_at_ns) is not int or completed_at_ns < now_ns:
                    raise ValueError("PUBLIC_RESPONSE_CLOCK_REGRESSED")
                if received > completed_at_ns:
                    raise ValueError(f"{venue.value}_METADATA_RECEIPT_AFTER_COMPLETION")
                previous_symbols = {
                    item.key.native_symbol for item in self._products
                    if item.key.venue == venue and item.trading_status.value == "TRADING"
                }
                enumerated_symbols = {item.key.native_symbol for item in venue_products}
                disappeared = sorted(previous_symbols - enumerated_symbols)
                if disappeared:
                    # An omitted active contract can mean delisting, a partial
                    # exchange response, or a changed eligibility rule. Keep
                    # the last known population and fail closed until the
                    # caller explicitly resolves that metadata discontinuity.
                    raise ValueError(f"{venue.value}_ACTIVE_METADATA_PRODUCTS_MISSING:{','.join(disappeared)}")
                received_updates[venue] = received
                row_updates[venue] = tuple(rows)
                row_receipt_updates[venue] = row_receipts
                manifests[venue.value] = dict(manifest)
                for item in venue_products:
                    previous = previous_products.get((venue, item.key.native_symbol))
                    # An unchanged metadata payload is a fresh receipt of the same contract revision.
                    # Keep the original immutable contract identity; receipt freshness is separate evidence.
                    if previous is not None and previous.key.contract_revision == item.key.contract_revision:
                        item = previous
                    products_by_key[item.key.to_canonical_json()] = item
            if len(products_by_key) > self.max_active_contracts:
                raise ValueError("ACTIVE_CONTRACT_POPULATION_OVERFLOW")
            counts: dict[str, int] = {}
            for product in products_by_key.values():
                counts[product.key.venue.value] = counts.get(product.key.venue.value, 0) + 1
            if any(count > self.max_contracts_per_venue for count in counts.values()):
                raise ValueError("PER_VENUE_CONTRACT_POPULATION_OVERFLOW")
            self._products = tuple(sorted(products_by_key.values(), key=lambda item: item.key.to_canonical_json()))
            self._metadata_received_at = received_updates
            self._metadata_rows = row_updates
            self._metadata_row_receipts = row_receipt_updates
            self._metadata_manifest = manifests
            active_series = {f"{p.key.venue.value}:{p.key.native_symbol}:{interval.value}:{p.key.contract_revision}"
                             for p in self._products if p.trading_status.value == "TRADING"
                             for interval in _RICH_INTERVALS}
            self._bar_cursors_ms = {key: value for key, value in self._bar_cursors_ms.items() if key in active_series}
            self._bar_pages = {key: value for key, value in self._bar_pages.items() if key in active_series}
            return self._products
        except Exception as exc:
            self._bootstrap_failure = f"{type(exc).__name__}:{exc}"
            for venue in attempted_venues:
                self._metadata_received_at.pop(venue, None)
                self._metadata_rows.pop(venue, None)
                self._metadata_row_receipts.pop(venue, None)
                manifest = self._metadata_manifest.setdefault(venue.value, {})
                manifest["page_complete"] = False
                manifest["refresh_failure"] = self._bootstrap_failure
            raise

    def _request(self, fn: Callable[[], Any], *, counter: str) -> Any:
        if self._bootstrap_requests >= self.max_requests:
            raise TimeoutError("METADATA_BOOTSTRAP_REQUEST_BUDGET_EXCEEDED")
        if (self._bootstrap_start_mono is not None
                and self.monotonic_ns() - self._bootstrap_start_mono >= self.max_acquisition_duration_ns):
            raise TimeoutError("METADATA_BOOTSTRAP_TIME_BUDGET_EXCEEDED")
        self._bootstrap_requests += 1
        result = self._read_response(fn, request_at_ns=self._bootstrap_request_at_ns)
        if self.monotonic_ns() - cast(int, self._bootstrap_start_mono) >= self.max_acquisition_duration_ns:
            raise TimeoutError("METADATA_BOOTSTRAP_TIME_BUDGET_EXCEEDED")
        self._bootstrap_successes += 1
        return result

    def _read_response(self, fn: Callable[[], Any], *, request_at_ns: int) -> Any:
        before_ns = self.clock_ns()
        if type(before_ns) is not int or before_ns < request_at_ns:
            raise ValueError("PUBLIC_RESPONSE_CLOCK_REGRESSED")
        result = fn()
        completed_at_ns = self.clock_ns()
        if type(completed_at_ns) is not int or completed_at_ns < before_ns:
            raise ValueError("PUBLIC_RESPONSE_CLOCK_REGRESSED")
        received_at_ns = getattr(result, "received_at_ns", None)
        if received_at_ns is not None and (
                type(received_at_ns) is not int or not 0 <= received_at_ns <= completed_at_ns):
            raise ValueError("PUBLIC_RESPONSE_RECEIPT_AFTER_COMPLETION_OR_INVALID")
        return result

    def _bootstrap_venue(self, venue: VenueV2):
        if venue == VenueV2.BYBIT:
            bybit_reader: Any = self.bybit_reader
            assert bybit_reader is not None
            cursor: str | None = None
            seen_cursors: set[str] = set()
            bybit_rows_all: list[Mapping[str, Any]] = []
            row_receipts: dict[str, int] = {}
            pages = 0
            received_at = 0
            while True:
                if pages >= MAX_METADATA_PAGES_V2 or self._bootstrap_requests >= MAX_METADATA_PAGES_V2 * len(self.enabled_venues):
                    raise ValueError("BYBIT_METADATA_PAGE_BUDGET_EXHAUSTED")
                page_cursor = cursor
                response = self._request(
                    lambda c=page_cursor, r=bybit_reader: r.instruments(limit=MAX_PAGE_ROWS_V2, cursor=c),  # type: ignore[misc]
                    counter="bootstrap",
                )
                received_at = max(received_at, response.received_at_ns)
                page = bybit_rows(response.payload, name="instrument-info")
                if len(page) > MAX_PAGE_ROWS_V2:
                    raise ValueError("BYBIT_METADATA_PAGE_ROW_BOUND_EXCEEDED")
                bybit_rows_all.extend(page)
                row_receipts.update({str(row.get("symbol", "")): response.received_at_ns for row in page})
                pages += 1
                result = response.payload.get("result", {})
                next_cursor = result.get("nextPageCursor") if isinstance(result, Mapping) else None
                if not next_cursor:
                    break
                next_cursor = str(next_cursor)
                if next_cursor in seen_cursors or next_cursor == cursor:
                    raise ValueError("BYBIT_METADATA_CURSOR_LOOP")
                seen_cursors.add(next_cursor)
                cursor = next_cursor
            raw_payload = {"retCode": 0, "result": {"list": bybit_rows_all}}
            products = translate_bybit_products(raw_payload, environment=EnvironmentV2.MAINNET,
                                                observed_at_ns=received_at, available_at_ns=received_at)
            if len({item.key.native_symbol for item in products}) != len(products):
                raise ValueError("BYBIT_METADATA_DUPLICATE_NATIVE_SYMBOL")
            manifest = self._metadata_manifest_for_rows(venue, bybit_rows_all, pages, received_at)
            return products, bybit_rows_all, received_at, manifest, row_receipts
        binance_reader: Any = self.binance_reader
        assert binance_reader is not None
        response = self._request(binance_reader.exchange_info, counter="bootstrap")
        if not isinstance(response.payload, Mapping) or len(response.payload.get("symbols", ())) > MAX_ACTIVE_CONTRACTS_V2 * 4:
            raise ValueError("BINANCE_METADATA_ROW_BOUND_EXCEEDED")
        raw_rows = response.payload.get("symbols")
        if not isinstance(raw_rows, list):
            raise ValueError("Binance exchangeInfo.symbols must be an array")
        binance_rows: list[Mapping[str, Any]] = raw_rows
        products = translate_exchange_info(response.payload, environment=EnvironmentV2.MAINNET,
                                           observed_at_ns=response.received_at_ns, available_at_ns=response.received_at_ns)
        if len({item.key.native_symbol for item in products}) != len(products):
            raise ValueError("BINANCE_METADATA_DUPLICATE_NATIVE_SYMBOL")
        manifest = self._metadata_manifest_for_rows(venue, binance_rows, 1, response.received_at_ns)
        return products, binance_rows, response.received_at_ns, manifest, {
            str(row.get("symbol", "")): response.received_at_ns for row in binance_rows}

    @staticmethod
    def _metadata_manifest_for_rows(venue: VenueV2, rows: Sequence[Mapping[str, Any]], pages: int, received_at: int):
        status_counts: dict[str, int] = {}
        accepted_contracts = 0
        rejected_non_usdt_or_nonperpetual = 0
        excluded_rows: list[dict[str, Any]] = []
        inactive_rows: list[dict[str, Any]] = []
        for row in rows:
            status = str(row.get("status", "UNKNOWN"))
            status_counts[status] = status_counts.get(status, 0) + 1
            if venue == VenueV2.BYBIT:
                is_candidate = (row.get("contractType") in ("LinearPerpetual", "LinearPerpetualContract")
                                and row.get("quoteCoin") == "USDT" and row.get("settleCoin") == "USDT")
            else:
                is_candidate = (row.get("contractType") == "PERPETUAL" and row.get("quoteAsset") == "USDT"
                                and row.get("marginAsset", "USDT") == "USDT")
            if is_candidate:
                accepted_contracts += 1
                active_status = "Trading" if venue == VenueV2.BYBIT else "TRADING"
                if status != active_status:
                    inactive_rows.append({"row": dict(row), "reason": "NON_TRADING_STATUS"})
            else:
                rejected_non_usdt_or_nonperpetual += 1
                excluded_rows.append({"row": dict(row), "reason": "NOT_USDT_LINEAR_PERPETUAL"})
        return {
            "venue": venue.value, "metadata_received_at_ns": received_at, "pages": pages,
            "page_complete": True, "rows_seen": len(rows), "status_counts": dict(sorted(status_counts.items())),
            "usdt_linear_rows": accepted_contracts, "rejected_non_target_rows": rejected_non_usdt_or_nonperpetual,
            "inactive_candidate_rows": inactive_rows, "excluded_non_target_rows": excluded_rows,
            "metadata_hash": sha256_json([dict(row) for row in rows]),
        }

    def begin_collection_cycle(self, *, now_ns: int) -> None:
        self._cycle_start_mono = self.monotonic_ns()
        self._cycle_metadata_requests = 0
        self._cycle_due = (
            self._last_snapshot is None
            or self._last_cheap_refresh_at_ns is None
            or now_ns < self._last_cheap_refresh_at_ns
            or now_ns - self._last_cheap_refresh_at_ns >= CHEAP_REFRESH_INTERVAL_NS_V2
        )
        if not self._cycle_due:
            return
        if any(self._expired(venue, now_ns) for venue in self.enabled_venues):
            try:
                self.bootstrap_products(now_ns=now_ns)
            except (PublicDataError, KeyError, TypeError, ValueError, ArithmeticError, TimeoutError):
                pass
            self._cycle_metadata_requests = self._bootstrap_requests

    def acquire_snapshot(self, *, now_ns: int) -> BroadPublicSnapshotV2:
        started = self._cycle_start_mono if self._cycle_start_mono is not None else self.monotonic_ns()
        metadata_cutoff_ns = self.clock_ns()
        if type(metadata_cutoff_ns) is not int or metadata_cutoff_ns < now_ns:
            raise ValueError("PUBLIC_RESPONSE_CLOCK_REGRESSED")
        if self._cycle_due is False:
            stale = [v.value for v in self.enabled_venues if self._expired(v, metadata_cutoff_ns)]
            cached_failure = (self._last_snapshot.failure_kind if self._last_snapshot is not None else "INCOMPLETE")
            cached_failure_reason = (self._last_snapshot.failure_reason if self._last_snapshot is not None
                              else "NO_PRIOR_ACQUISITION")
            if stale or self._bootstrap_failure is not None:
                cached_failure, cached_failure_reason = "INCOMPLETE", "METADATA_BOOTSTRAP_UNAVAILABLE_OR_STALE"
            manifest = {
                "schema_version": 1,
                "enabled_venues": [venue.value for venue in self.enabled_venues],
                "acquisition_due": False,
                "reuse_snapshot_id": self._last_snapshot.source_snapshot.get("source_snapshot_id")
                    if self._last_snapshot is not None else None,
                "metadata_stale": stale,
                "metadata": dict(self._metadata_manifest),
                "complete": cached_failure is None,
                "market_record_count": 0,
            }
            manifest["source_snapshot_id"] = sha256_json(manifest)
            prior_received = self._last_snapshot.latest_received_at_ns if self._last_snapshot is not None else now_ns
            return BroadPublicSnapshotV2(
                (), cached_failure is None, cached_failure, cached_failure_reason,
                min(prior_received, metadata_cutoff_ns), metadata_cutoff_ns, 0, 0, 0,
                self._bootstrap_requests, self._bootstrap_successes, manifest,
            )
        requests = 0
        successes = 0
        records: list[PublicInputRecordV2] = []
        latest = max(self._metadata_received_at.values(), default=0)
        failure: str | None = None
        failure_reason: str | None = None
        venue_manifest: dict[str, Any] = {k: dict(v) for k, v in self._metadata_manifest.items()}
        for venue in self.enabled_venues:
            entry = venue_manifest.setdefault(venue.value, {})
            entry["metadata_current_for_cutoff"] = not self._expired(venue, metadata_cutoff_ns)
            entry["market_status"] = "NOT_ATTEMPTED"
            entry["market_missingness"] = ["BULK_MARKET", "BARS", "TRADES", "FUNDING_HISTORY", "OPEN_INTEREST"]
        if self._bootstrap_failure is not None or not self._products or any(
                self._expired(v, metadata_cutoff_ns) for v in self.enabled_venues):
            failure = "INCOMPLETE"
            failure_reason = "METADATA_BOOTSTRAP_UNAVAILABLE_OR_STALE"
        def request(call: Callable[[], Any]):
            nonlocal requests, successes
            if (requests + self._cycle_metadata_requests >= self.max_requests
                    or self.monotonic_ns() - started >= self.max_acquisition_duration_ns):
                raise TimeoutError("ACQUISITION_BUDGET_EXCEEDED")
            requests += 1
            value = self._read_response(call, request_at_ns=now_ns)
            if self.monotonic_ns() - started >= self.max_acquisition_duration_ns:
                raise TimeoutError("ACQUISITION_BUDGET_EXCEEDED")
            successes += 1
            return value

        def record_venue_failure(venue: VenueV2, exc: Exception) -> None:
            nonlocal failure, failure_reason
            if isinstance(exc, TimeoutError):
                kind, reason = "BUDGET_EXCEEDED", "ACQUISITION_REQUEST_OR_TIME_BUDGET"
            elif isinstance(exc, PublicDataError):
                kind = "RATE_LIMITED" if exc.rate_limited else "DISCONNECTED"
                reason = "PUBLIC_ENDPOINT_RATE_LIMITED" if exc.rate_limited else "PUBLIC_ENDPOINT_UNAVAILABLE"
            else:
                kind, reason = "MALFORMED", "PUBLIC_RESPONSE_OR_TRANSLATION_INVALID"
            failure = failure or kind
            failure_reason = failure_reason or reason
            entry = venue_manifest.setdefault(venue.value, {})
            entry["market_status"] = "INCOMPLETE"
            entry["market_failure_kind"] = kind
            entry["market_missingness"] = ["BULK_MARKET", "BARS", "TRADES", "FUNDING_HISTORY", "OPEN_INTEREST"]
        try:
            # Persist exact metadata rows as evidence for every status, including suspended/prelisting rows.
            by_product = {(p.key.venue, p.key.native_symbol): p for p in self._products}
            for venue in self.enabled_venues:
                source_id = BYBIT_SOURCE_ID if venue == VenueV2.BYBIT else BINANCE_SOURCE_ID
                for row in self._metadata_rows.get(venue, ()):
                    symbol = str(row.get("symbol", ""))
                    product = by_product.get((venue, symbol))
                    if product is None:
                        continue
                    received = self._metadata_row_receipts[venue][symbol]
                    raw = RawObservationV2.build(
                        instrument_revision=product.key.contract_revision, source_id=source_id,
                        event_type="PRODUCT_METADATA", event_at_ns=None, received_at_ns=received,
                        ingested_at_ns=received, available_at_ns=received,
                        payload=row, translation_version="broad-public-v2",
                        sequence=f"{product.key.contract_revision}:{received}",
                    )
                    records.append(PublicInputRecordV2(raw, canonical_json(row).encode(), product.key))
            # Cheap quote/mark/funding evidence is fetched in bounded venue bulk calls.
            for venue in self.enabled_venues:
                if venue == VenueV2.BYBIT:
                    bybit_endpoint: Any = self.bybit_reader
                    try:
                        response = request(lambda r=bybit_endpoint: r.client.get("/v5/market/tickers", {"category": "linear"}))  # type: ignore[misc]
                    except (TimeoutError, PublicDataError, KeyError, TypeError, ValueError, ArithmeticError) as exc:
                        record_venue_failure(venue, exc)
                        continue
                    bybit_ticker_rows = bybit_rows(response.payload, name="tickers")
                    bybit_ticker_rows = self._bounded_symbol_rows(bybit_ticker_rows, venue)
                    source_id = BYBIT_SOURCE_ID
                    for row in bybit_ticker_rows:
                        product = by_product.get((venue, str(row.get("symbol", ""))))
                        if product is None:
                            continue
                        raw = RawObservationV2.build(
                            instrument_revision=product.key.contract_revision, source_id=source_id,
                            event_type="TICKER_MARK_INDEX_FUNDING_OI", event_at_ns=_ms_ns(row.get("ts")),
                            received_at_ns=response.received_at_ns, ingested_at_ns=response.received_at_ns,
                            available_at_ns=response.received_at_ns, payload=row,
                            translation_version="bybit-public-v1",
                            # A bulk poll is an actual new observation even if
                            # the venue repeats unchanged values/event time.
                            sequence=f"bulk-receipt:{response.received_at_ns}",
                        )
                        records.append(PublicInputRecordV2(raw, canonical_json(row).encode(), product.key))
                    latest = max(latest, response.received_at_ns)
                    venue_manifest.setdefault(venue.value, {})["ticker_rows_received"] = len(bybit_ticker_rows)
                    expected = {symbol for (v, symbol), p in by_product.items()
                                if v == venue and p.trading_status.value == "TRADING"}
                    received_symbols = {str(row.get("symbol", "")) for row in bybit_ticker_rows}
                    venue_manifest[venue.value]["ticker_missing_symbols"] = sorted(expected - received_symbols)
                else:
                    binance_endpoint: Any = self.binance_reader
                    try:
                        book = request(lambda r=binance_endpoint: r.client.get("/fapi/v1/ticker/bookTicker"))  # type: ignore[misc]
                        premium = request(lambda r=binance_endpoint: r.client.get("/fapi/v1/premiumIndex"))  # type: ignore[misc]
                        turnover = request(lambda r=binance_endpoint: r.client.get("/fapi/v1/ticker/24hr"))  # type: ignore[misc]
                    except (TimeoutError, PublicDataError, KeyError, TypeError, ValueError, ArithmeticError) as exc:
                        record_venue_failure(venue, exc)
                        continue
                    book_rows = self._bounded_symbol_rows(book.payload if isinstance(book.payload, list) else [book.payload], venue)
                    premium_rows = self._bounded_symbol_rows(premium.payload if isinstance(premium.payload, list) else [premium.payload], venue)
                    turnover_rows = self._bounded_symbol_rows(turnover.payload if isinstance(turnover.payload, list) else [turnover.payload], venue)
                    for response, rows, event in ((book, book_rows, "BOOK_TICKER"),
                            (premium, premium_rows, "MARK_INDEX_CURRENT_FUNDING"),
                            (turnover, turnover_rows, "TICKER_24H")):
                        for row in rows:
                            product = by_product.get((venue, str(row.get("symbol", row.get("s", "")))))
                            if product is None:
                                continue
                            raw = RawObservationV2.build(
                                instrument_revision=product.key.contract_revision, source_id=BINANCE_SOURCE_ID,
                                event_type=event, event_at_ns=_ms_ns(row.get("E", row.get("time", row.get("closeTime")))),
                                received_at_ns=response.received_at_ns, ingested_at_ns=response.received_at_ns,
                                available_at_ns=response.received_at_ns, payload=row,
                                translation_version="binance-usdm-public-v1",
                                sequence=f"bulk-receipt:{response.received_at_ns}:{row.get('u', '')}",
                            )
                            records.append(PublicInputRecordV2(raw, canonical_json(row).encode(), product.key))
                        latest = max(latest, response.received_at_ns)
                    venue_manifest.setdefault(venue.value, {})["book_ticker_rows_received"] = len(book_rows)
                    venue_manifest.setdefault(venue.value, {})["premium_index_rows_received"] = len(premium_rows)
                    venue_manifest[venue.value]["ticker_24h_rows_received"] = len(turnover_rows)
                    expected = {symbol for (v, symbol), p in by_product.items()
                                if v == venue and p.trading_status.value == "TRADING"}
                    venue_manifest[venue.value]["book_ticker_missing_symbols"] = sorted(
                        expected - {str(row.get("symbol", row.get("s", ""))) for row in book_rows})
                    venue_manifest[venue.value]["premium_index_missing_symbols"] = sorted(
                        expected - {str(row.get("symbol", "")) for row in premium_rows})
                    venue_manifest[venue.value]["ticker_24h_missing_symbols"] = sorted(
                        expected - {str(row.get("symbol", "")) for row in turnover_rows})
                missing_fields = ("ticker_missing_symbols", "book_ticker_missing_symbols",
                                  "premium_index_missing_symbols", "ticker_24h_missing_symbols")
                if any(venue_manifest[venue.value].get(field) for field in missing_fields):
                    record_venue_failure(venue, ValueError("ACTIVE_BULK_MARKET_PRODUCTS_MISSING"))
                    continue
                venue_manifest[venue.value]["market_status"] = "BULK_MARKET_COMPLETE"
                venue_manifest[venue.value]["market_missingness"] = ["BARS", "TRADES", "FUNDING_HISTORY", "OPEN_INTEREST"]
            # Bounded native-cadence and derivative enrichment rotates across the complete
            # active instrument population. Deferred rows remain explicit; none are fabricated.
            ordered_products = sorted((p for p in self._products if p.trading_status.value == "TRADING"),
                                      key=lambda p: p.key.to_canonical_json())
            rotation_start = self._rotation
            if ordered_products:
                focused = [product for product in ordered_products if product.key in self._enrichment_keys]
                selected = []
                if focused:
                    # One focused key and one broad exploration key keeps
                    # bootstrap moving for every observed instrument.
                    selected.append(focused[self._rotation % len(focused)])
                for offset in range(len(ordered_products)):
                    candidate = ordered_products[(self._rotation + offset) % len(ordered_products)]
                    if candidate not in selected:
                        selected.append(candidate)
                    if len(selected) >= min(MAX_ROTATING_ENRICHMENT_KEYS_V2, len(ordered_products)):
                        break
                self._rotation += 1 if focused else len(selected)
                server_times: dict[VenueV2, int] = {}
                for venue in self.enabled_venues:
                    server_endpoint: Any = self.bybit_reader if venue == VenueV2.BYBIT else self.binance_reader
                    server_times[venue] = request(server_endpoint.server_time_ns)
                completed_rich: list[str] = []
                unavailable: list[dict[str, str]] = []
                for product in selected:
                    venue = product.key.venue
                    enrichment_endpoint: Any = self.bybit_reader if venue == VenueV2.BYBIT else self.binance_reader
                    rich_record_start = len(records)
                    for interval in _RICH_INTERVALS:
                        if requests >= self.max_requests:
                            unavailable.append({"key": product.key.to_canonical_json(), "reason": "REQUEST_BUDGET"})
                            break
                        series_id = f"{venue.value}:{product.key.native_symbol}:{interval.value}:{product.key.contract_revision}"
                        cursor_ms = self._bar_cursors_ms.get(series_id)
                        # Current cadence acquisition continues alongside
                        # bounded historical paging, including after restore.
                        page_ends = (None, cursor_ms) if cursor_ms is not None and cursor_ms > 0 else (None,)
                        for end_ms in page_ends:
                            response = request(lambda r=enrichment_endpoint, v=venue, p=product, i=interval, c=end_ms:  # type: ignore[misc]
                                               _request_kline_page(r, v, p.key.native_symbol, i, c))
                            page_records, oldest_open_ms = self._translate_bar_page(
                                response, product, interval, server_times[venue],
                            )
                            latest = max(latest, response.received_at_ns)
                            records.extend(page_records)
                            if oldest_open_ms is not None and (end_ms is not None or cursor_ms is None):
                                next_cursor = max(0, oldest_open_ms - 1)
                                if end_ms is not None and next_cursor >= end_ms:
                                    raise ValueError("BAR_BACKFILL_CURSOR_DID_NOT_ADVANCE")
                                self._bar_cursors_ms[series_id] = next_cursor
                                self._bar_pages[series_id] = self._bar_pages.get(series_id, 0) + 1
                    if requests >= self.max_requests:
                        unavailable.append({"key": product.key.to_canonical_json(), "reason": "REQUEST_BUDGET"})
                        continue
                    if venue == VenueV2.BYBIT:
                        trade_response = request(lambda r=enrichment_endpoint, p=product: r.recent_trades(p.key.native_symbol, limit=100))  # type: ignore[misc]
                        trade_body = self._bounded_history_rows(
                            bybit_rows(trade_response.payload, name="recent-trade"))
                        observations = translate_bybit_trades(trade_body, key=product.key,
                                                              received_at_ns=trade_response.received_at_ns)
                        payloads = trade_body
                        funding_response = request(lambda r=enrichment_endpoint, p=product: r.funding_history(p.key.native_symbol, limit=100))  # type: ignore[misc]
                        funding_rows = self._bounded_history_rows(
                            bybit_rows(funding_response.payload, name="funding-history"))
                        observations += translate_bybit_funding(funding_rows, key=product.key,
                                                                 received_at_ns=funding_response.received_at_ns)
                        oi_response = request(lambda r=enrichment_endpoint, p=product: r.open_interest_history(p.key.native_symbol, interval="5min", limit=100))  # type: ignore[misc]
                        oi_rows = self._bounded_history_rows(
                            bybit_rows(oi_response.payload, name="open-interest"))
                        observations += translate_bybit_oi_history(oi_rows, key=product.key,
                                                                    received_at_ns=oi_response.received_at_ns)
                        for row, raw in zip(payloads, observations[:len(payloads)], strict=True):
                            records.append(PublicInputRecordV2(raw, canonical_json(row).encode(), product.key))
                        offset = len(payloads)
                        for group in (funding_rows, oi_rows):
                            for row, raw in zip(group, observations[offset:offset + len(group)], strict=True):
                                records.append(PublicInputRecordV2(raw, canonical_json(row).encode(), product.key))
                            offset += len(group)
                        latest = max(latest, trade_response.received_at_ns, funding_response.received_at_ns, oi_response.received_at_ns)
                    else:
                        trade_response = request(lambda r=enrichment_endpoint, p=product: r.aggregate_trades(p.key.native_symbol, limit=100))  # type: ignore[misc]
                        trade_rows = trade_response.payload
                        if not isinstance(trade_rows, list) or len(trade_rows) > 100:
                            raise ValueError("Binance aggregate trade response exceeded its row limit")
                        trade_observations = translate_agg_trades(trade_rows, key=product.key,
                                                                    received_at_ns=trade_response.received_at_ns)
                        records.extend(PublicInputRecordV2(raw, canonical_json(row).encode(), product.key)
                                       for row, raw in zip(trade_rows, trade_observations, strict=True))
                        funding_response = request(lambda r=enrichment_endpoint, p=product: r.funding_history(p.key.native_symbol, limit=100))  # type: ignore[misc]
                        funding_rows = self._bounded_history_rows(funding_response.payload)
                        funding_observations = translate_binance_funding(funding_rows, key=product.key,
                                                                           received_at_ns=funding_response.received_at_ns)
                        records.extend(PublicInputRecordV2(raw, canonical_json(row).encode(), product.key)
                                       for row, raw in zip(funding_rows, funding_observations, strict=True))
                        oi_response = request(lambda r=enrichment_endpoint, p=product: r.open_interest(p.key.native_symbol))  # type: ignore[misc]
                        oi_observation = translate_binance_oi(oi_response.payload, key=product.key,
                                                              received_at_ns=oi_response.received_at_ns)
                        oi_observation = RawObservationV2.build(
                            instrument_revision=oi_observation.instrument_revision,
                            source_id=oi_observation.source_id, event_type=oi_observation.event_type,
                            event_at_ns=oi_observation.event_at_ns, received_at_ns=oi_observation.received_at_ns,
                            ingested_at_ns=oi_observation.ingested_at_ns,
                            available_at_ns=oi_observation.available_at_ns,
                            payload=oi_response.payload, translation_version=oi_observation.translation_version,
                            sequence=f"snapshot-receipt:{oi_response.received_at_ns}",
                        )
                        records.append(PublicInputRecordV2(oi_observation, canonical_json(oi_response.payload).encode(), product.key))
                        oi_hist_response = request(lambda r=enrichment_endpoint, p=product: r.open_interest_history(p.key.native_symbol, period="5m", limit=100))  # type: ignore[misc]
                        oi_hist_rows = self._bounded_history_rows(oi_hist_response.payload)
                        oi_hist_observations = translate_binance_oi_history(oi_hist_rows, key=product.key,
                                                                             received_at_ns=oi_hist_response.received_at_ns)
                        records.extend(PublicInputRecordV2(raw, canonical_json(row).encode(), product.key)
                                       for row, raw in zip(oi_hist_rows, oi_hist_observations, strict=True))
                        latest = max(latest, trade_response.received_at_ns, funding_response.received_at_ns,
                                     oi_response.received_at_ns, oi_hist_response.received_at_ns)
                    if len(records) > rich_record_start:
                        completed_rich.append(product.key.to_canonical_json())
                venue_manifest["enrichment"] = {
                    "scheduled_keys": [p.key.to_canonical_json() for p in selected],
                    "completed_keys": completed_rich, "unavailable": unavailable,
                    "rotating_cursor_before": rotation_start, "rotating_cursor_after": self._rotation,
                    "deferred_active_keys": max(0, len(ordered_products) - len(selected)),
                    "bar_intervals": [interval.value for interval in _RICH_INTERVALS],
                    "bar_cursors_ms": dict(sorted(self._bar_cursors_ms.items())),
                    "bar_pages": dict(sorted(self._bar_pages.items())),
                    "per_key_history_complete": False,
                    "history_limitation": "bounded_pages_with_restored_oldest_timestamp_cursors; continuity_and_30d_completeness_not_yet_qualified",
                }
            if len(records) > MAX_RECORDS_PER_SNAPSHOT_V2:
                raise ValueError("SNAPSHOT_RECORD_BOUND_EXCEEDED")
        except TimeoutError:
            if all(venue_manifest.get(v.value, {}).get("market_status") == "BULK_MARKET_COMPLETE"
                   for v in self.enabled_venues):
                venue_manifest["enrichment"] = {
                    "status": "DEFERRED_BUDGET",
                    "missingness": ["BARS", "TRADES", "FUNDING_HISTORY", "OPEN_INTEREST"],
                    "reason": "ACQUISITION_REQUEST_OR_TIME_BUDGET",
                }
            else:
                failure, failure_reason = "BUDGET_EXCEEDED", "ACQUISITION_REQUEST_OR_TIME_BUDGET"
        except PublicDataError as exc:
            if all(venue_manifest.get(v.value, {}).get("market_status") == "BULK_MARKET_COMPLETE"
                   for v in self.enabled_venues):
                venue_manifest["enrichment"] = {
                    "status": "DEFERRED_ENDPOINT_FAILURE",
                    "missingness": ["BARS", "TRADES", "FUNDING_HISTORY", "OPEN_INTEREST"],
                    "reason": "PUBLIC_ENDPOINT_RATE_LIMITED" if exc.rate_limited else "PUBLIC_ENDPOINT_UNAVAILABLE",
                }
            else:
                failure = "RATE_LIMITED" if exc.rate_limited else "DISCONNECTED"
                failure_reason = "PUBLIC_ENDPOINT_RATE_LIMITED" if exc.rate_limited else "PUBLIC_ENDPOINT_UNAVAILABLE"
        except (KeyError, TypeError, ValueError, ArithmeticError):
            if all(venue_manifest.get(v.value, {}).get("market_status") == "BULK_MARKET_COMPLETE"
                   for v in self.enabled_venues):
                venue_manifest["enrichment"] = {
                    "status": "DEFERRED_MALFORMED_RESPONSE",
                    "missingness": ["BARS", "TRADES", "FUNDING_HISTORY", "OPEN_INTEREST"],
                    "reason": "ENRICHMENT_RESPONSE_OR_TRANSLATION_INVALID",
                }
            else:
                failure, failure_reason = "MALFORMED", "PUBLIC_RESPONSE_OR_TRANSLATION_INVALID"
        if failure is not None:
            for venue in self.enabled_venues:
                entry = venue_manifest.setdefault(venue.value, {})
                if entry.get("market_status") == "NOT_ATTEMPTED":
                    entry["market_status"] = "INCOMPLETE"
                    entry["market_failure_kind"] = failure
                    entry["market_missingness"] = ["BULK_MARKET", "BARS", "TRADES", "FUNDING_HISTORY", "OPEN_INTEREST"]
        completed_at_ns = self.clock_ns()
        if type(completed_at_ns) is not int or completed_at_ns < max(metadata_cutoff_ns, latest):
            raise ValueError("PUBLIC_RESPONSE_CLOCK_REGRESSED")
        complete = failure is None
        manifest = {
            "schema_version": 1,
            "enabled_venues": [v.value for v in self.enabled_venues], "metadata": venue_manifest,
            "metadata_stale": [v.value for v in self.enabled_venues if self._expired(v, metadata_cutoff_ns)],
            "active_contract_count": len(self._products), "market_record_count": len(records),
            "cycle_metadata_request_count": self._cycle_metadata_requests,
            "complete": complete,
            "failure_kind": failure, "failure_reason": failure_reason,
        }
        manifest["source_snapshot_id"] = sha256_json(manifest)
        result = BroadPublicSnapshotV2(
            tuple(records), complete, failure, failure_reason, latest, completed_at_ns, requests, successes,
            max(0, self.monotonic_ns() - started), self._bootstrap_requests, self._bootstrap_successes, manifest,
        )
        self._last_snapshot = result
        self._last_cheap_refresh_at_ns = now_ns
        return result

    @staticmethod
    def _translate_bar_page(response: Any, product: ProductContractV2, interval: BarIntervalV2,
                            server_time_ns: int) -> tuple[tuple[PublicInputRecordV2, ...], int | None]:
        bar_rows: Any = response.payload
        if product.key.venue == VenueV2.BYBIT:
            if not isinstance(response.payload, Mapping) or response.payload.get("retCode") != 0:
                raise ValueError("Bybit kline request returned a nonzero retCode")
            result_body = response.payload.get("result")
            bar_rows = result_body.get("list", []) if isinstance(result_body, Mapping) else []
        if not isinstance(bar_rows, list) or len(bar_rows) > MAX_BAR_ROWS_V2:
            raise ValueError("venue kline payload exceeded its bounded row limit")
        records: list[PublicInputRecordV2] = []
        translate = translate_bybit_kline if product.key.venue == VenueV2.BYBIT else translate_binance_kline
        for row in bar_rows:
            raw, values, open_ns, final = translate(row, key=product.key, interval=interval,
                received_at_ns=response.received_at_ns, server_time_ns=server_time_ns)
            if final:
                bar = translate_final_bar(raw=raw, interval=interval, open_at_ns=open_ns, values=values, final=True)
                if bar is not None:
                    records.append(PublicInputRecordV2(raw, canonical_json(list(row)).encode(), product.key, bar))
        oldest = min(int(row[0]) for row in bar_rows) if bar_rows else None
        return tuple(records), oldest

    def _bounded_symbol_rows(self, payload: Any, venue: VenueV2) -> list[Mapping[str, Any]]:
        if not isinstance(payload, list):
            raise ValueError("bulk market payload must be an array")
        if len(payload) > self.max_active_contracts * 2:
            raise ValueError("bulk market response exceeded its fixed row bound")
        if any(not isinstance(row, Mapping) for row in payload):
            raise ValueError(f"{venue.value}_BULK_MARKET_ROW_MALFORMED")
        symbols = [str(row.get("symbol", row.get("s", ""))) for row in payload]
        if not all(symbols) or len(set(symbols)) != len(symbols):
            raise ValueError(f"{venue.value}_BULK_MARKET_SYMBOL_IDENTITY_INVALID")
        return list(payload)

    @staticmethod
    def _bounded_history_rows(payload: Any) -> list[Mapping[str, Any]]:
        if (not isinstance(payload, list) or len(payload) > 100
                or any(not isinstance(row, Mapping) for row in payload)):
            raise ValueError("PUBLIC_HISTORY_RESPONSE_ROW_BOUND_OR_TYPE_INVALID")
        return payload


def _ms_ns(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value) * 1_000_000


def _deep_freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _deep_freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_deep_freeze(item) for item in value)
    return value


def _request_kline_page(reader: Any, venue: VenueV2, symbol: str, interval: BarIntervalV2,
                        cursor_end_ms: int | None) -> Any:
    if cursor_end_ms is None:
        return reader.klines(symbol, interval, limit=MAX_BAR_ROWS_V2)
    if venue == VenueV2.BYBIT:
        interval_text = {BarIntervalV2.M1: "1", BarIntervalV2.M15: "15",
                         BarIntervalV2.H1: "60", BarIntervalV2.H4: "240"}[interval]
        return reader.client.get("/v5/market/kline", {
            "category": "linear", "symbol": symbol, "interval": interval_text,
            "limit": MAX_BAR_ROWS_V2, "end": cursor_end_ms,
        })
    interval_text = {BarIntervalV2.M1: "1m", BarIntervalV2.M15: "15m",
                     BarIntervalV2.H1: "1h", BarIntervalV2.H4: "4h"}[interval]
    return reader.client.get("/fapi/v1/klines", {
        "symbol": symbol, "interval": interval_text, "limit": MAX_BAR_ROWS_V2,
        "endTime": cursor_end_ms,
    })
