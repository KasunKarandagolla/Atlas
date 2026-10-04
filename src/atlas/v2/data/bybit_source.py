"""Opt-in, bounded Bybit public market intake for the existing production collector."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from typing import Any

from .._serialization import canonical_json
from ..instruments import EnvironmentV2, InstrumentKeyV2, ProductContractV2
from .bars import BarIntervalV2, CausalBarV2, translate_final_bar
from .bybit import (
    SOURCE_ID,
    BybitPublicReaderV2,
    _result_rows,
    translate_instrument_info,
    translate_kline,
    translate_recent_trades,
    translate_ticker,
)
from .public_http import PublicDataError, PublicHttpClientV2, PublicHttpsSessionV1, PublicVenueV2
from .raw import RawObservationV2

CAMPAIGN_SYMBOLS = ("BTCUSDT", "ETHUSDT")
CAMPAIGN_INTERVALS = (BarIntervalV2.M1, BarIntervalV2.M15, BarIntervalV2.H1, BarIntervalV2.H4)
MAX_KLINE_ROWS = 200
MAX_TRADE_ROWS = 100
MAX_SNAPSHOT_RECORDS = 1_810
MAX_REQUESTS_PER_SNAPSHOT = 13
MAX_ACQUISITION_DURATION_NS = 5_000_000_000
MAX_METADATA_CACHE_AGE_NS = 3_600_000_000_000


class _AcquisitionBudgetExceeded(RuntimeError):
    pass


def _kline_rows(payload: Any) -> list[Sequence[Any]]:
    if not isinstance(payload, Mapping) or payload.get("retCode") != 0:
        raise ValueError("Bybit kline response failed its typed status check")
    result = payload.get("result")
    rows = result.get("list") if isinstance(result, Mapping) else None
    if not isinstance(rows, list) or any(not isinstance(row, (list, tuple)) for row in rows):
        raise ValueError("Bybit kline result.list must contain bounded positional rows")
    return rows


@dataclass(frozen=True)
class _MetadataCapture:
    products: tuple[ProductContractV2, ...]
    rows_by_symbol: Mapping[str, Mapping[str, Any]]
    received_at_ns: int


@dataclass(frozen=True)
class BybitPublicInputRecordV1:
    observation: RawObservationV2
    raw_payload: bytes
    instrument_key: InstrumentKeyV2
    bar: CausalBarV2 | None

    def __post_init__(self) -> None:
        if not self.raw_payload:
            raise ValueError("public acquisition records require their bounded raw payload")
        if self.observation.instrument_revision != self.instrument_key.contract_revision:
            raise ValueError("public acquisition record instrument revision differs from its observation")
        if self.bar is not None and self.bar.raw.record_id != self.observation.record_id:
            raise ValueError("public acquisition bar must bind its original causal observation")


@dataclass(frozen=True)
class BybitPublicSnapshotV1:
    records: tuple[BybitPublicInputRecordV1, ...]
    complete: bool
    failure_kind: str | None
    failure_reason: str | None
    latest_received_at_ns: int
    observed_at_ns: int
    request_count: int = 0
    successful_request_count: int = 0
    acquisition_duration_ns: int = 0
    bootstrap_request_count: int = 0
    successful_bootstrap_request_count: int = 0

    def __post_init__(self) -> None:
        if len(self.records) > MAX_SNAPSHOT_RECORDS:
            raise ValueError("public snapshot exceeded its fixed record bound")
        if self.failure_kind not in {None, "INCOMPLETE", "MALFORMED", "DISCONNECTED", "RATE_LIMITED"}:
            raise ValueError("unsupported public snapshot failure kind")
        if self.complete != (self.failure_kind is None):
            raise ValueError("complete public snapshots cannot carry a failure kind")
        if self.latest_received_at_ns < max(
            (item.observation.received_at_ns for item in self.records), default=0,
        ):
            raise ValueError("snapshot receipt boundary cannot precede an acquired record")
        if self.observed_at_ns < self.latest_received_at_ns:
            raise ValueError("snapshot observation time cannot precede its latest actual receipt")
        if (type(self.request_count) is not int or not 0 <= self.request_count <= MAX_REQUESTS_PER_SNAPSHOT
                or type(self.successful_request_count) is not int
                or not 0 <= self.successful_request_count <= self.request_count
                or type(self.bootstrap_request_count) is not int or self.bootstrap_request_count not in (0, 1)
                or type(self.successful_bootstrap_request_count) is not int
                or not 0 <= self.successful_bootstrap_request_count <= self.bootstrap_request_count
                or type(self.acquisition_duration_ns) is not int or self.acquisition_duration_ns < 0):
            raise ValueError("public snapshot request accounting is outside its fixed budget")


class BybitPublicCycleSourceV1:
    """Credential-free public REST adapter returning immutable records only.

    Every REST response has a hard transport timeout and the request count per cycle is
    fixed. A response is not enough to recover source health: the source verifies the
    returned confirmed-bar snapshot. The production controller owns all collector and
    repository interactions after this adapter returns its bounded snapshot.
    """

    def __init__(
        self,
        reader: BybitPublicReaderV2 | None = None,
        *,
        clock_ns: Callable[[], int] = time.time_ns,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        max_acquisition_duration_ns: int = MAX_ACQUISITION_DURATION_NS,
    ) -> None:
        if reader is None:
            reader = BybitPublicReaderV2(PublicHttpClientV2(PublicVenueV2.BYBIT, timeout_s=1.25,
                getter=PublicHttpsSessionV1(PublicVenueV2.BYBIT)))
        if type(max_acquisition_duration_ns) is not int or not 0 < max_acquisition_duration_ns <= MAX_ACQUISITION_DURATION_NS:
            raise ValueError("Bybit acquisition budget must be positive and no greater than five seconds")
        self.reader = reader
        self.clock_ns = clock_ns
        self.monotonic_ns = monotonic_ns
        self.max_acquisition_duration_ns = max_acquisition_duration_ns
        self._metadata: _MetadataCapture | None = None
        self._cycle_started_monotonic_ns: int | None = None
        self._bootstrap_request_count = 0
        self._successful_bootstrap_request_count = 0
        self._bootstrap_failed = False
        self._bootstrap_cycle_pending = False

    @property
    def current_products(self) -> tuple[ProductContractV2, ...]:
        """Return the exact metadata revision backing the current bounded snapshot.

        The supervisor-owned controller uses this after acquisition to bind
        stream frames to a point-in-time contract revision. The source itself
        remains read-only and does not mutate the repository or registry.
        """
        return self._metadata.products if self._metadata is not None else ()

    def close(self) -> None:
        """Called only after the acquisition worker completes, never during I/O."""
        close = getattr(getattr(getattr(self.reader, "client", None), "getter", None), "close", None)
        if callable(close):
            close()

    @property
    def required_source_ids(self) -> tuple[str, ...]:
        return (SOURCE_ID,)

    def bootstrap_products(self, *, now_ns: int) -> tuple[ProductContractV2, ...]:
        """Read metadata before collector construction; persistence stays with the port."""
        self._cycle_started_monotonic_ns = self.monotonic_ns()
        self._bootstrap_request_count = 0
        self._successful_bootstrap_request_count = 0
        self._bootstrap_failed = False
        self._bootstrap_cycle_pending = True
        if (self._metadata is not None and 0 <= now_ns - self._metadata.received_at_ns
                <= MAX_METADATA_CACHE_AGE_NS):
            return self._metadata.products

        self._bootstrap_request_count = 1
        client = getattr(self.reader, "client", None)
        prior_timeout = getattr(client, "timeout_s", None)
        remaining_ns = self.max_acquisition_duration_ns
        if remaining_ns <= 0:
            self._bootstrap_failed = True
            raise ValueError("Bybit metadata bootstrap exceeded its cycle acquisition budget")
        if client is not None and isinstance(prior_timeout, (int, float)) and prior_timeout > 0:
            client.timeout_s = min(float(prior_timeout), remaining_ns / 1_000_000_000)
        try:
            response = self.reader.instruments(limit=1000)
            if self.monotonic_ns() - self._cycle_started_monotonic_ns > self.max_acquisition_duration_ns:
                raise ValueError("Bybit metadata bootstrap exceeded its cycle acquisition budget")
            rows = _result_rows(response.payload, name="instrument-info")
            if len(rows) > 1000:
                raise ValueError("Bybit metadata page exceeded its requested row bound")
            selected = [row for row in rows if row.get("symbol") in CAMPAIGN_SYMBOLS]
            if {str(row.get("symbol")) for row in selected} != set(CAMPAIGN_SYMBOLS):
                raise ValueError("Bybit metadata did not contain both configured USDT-linear instruments")
            payload = {"retCode": 0, "result": {"list": selected}}
            products = translate_instrument_info(
                payload, environment=EnvironmentV2.MAINNET,
                observed_at_ns=response.received_at_ns, available_at_ns=response.received_at_ns,
            )
            by_symbol = {product.key.native_symbol: product for product in products}
            if set(by_symbol) != set(CAMPAIGN_SYMBOLS):
                raise ValueError("Bybit metadata did not resolve both configured linear perpetuals")
            self._metadata = _MetadataCapture(
                tuple(by_symbol[symbol] for symbol in CAMPAIGN_SYMBOLS),
                {str(row["symbol"]): row for row in selected}, response.received_at_ns,
            )
            self._successful_bootstrap_request_count = 1
            return self._metadata.products
        except Exception:
            self._bootstrap_failed = True
            raise
        finally:
            if client is not None and isinstance(prior_timeout, (int, float)) and prior_timeout > 0:
                client.timeout_s = prior_timeout

    def begin_collection_cycle(self, *, now_ns: int) -> None:
        """Start the shared five-second budget for one supervisor collection."""
        if self._bootstrap_cycle_pending:
            self._bootstrap_cycle_pending = False
            return
        self._cycle_started_monotonic_ns = self.monotonic_ns()
        self._bootstrap_request_count = 0
        self._successful_bootstrap_request_count = 0
        self._bootstrap_failed = False
        if (self._metadata is None or now_ns < self._metadata.received_at_ns
                or now_ns - self._metadata.received_at_ns > MAX_METADATA_CACHE_AGE_NS):
            try:
                self.bootstrap_products(now_ns=now_ns)
            except (PublicDataError, KeyError, TypeError, ValueError, ArithmeticError):
                self._bootstrap_failed = True
            finally:
                self._bootstrap_cycle_pending = False

    def acquire_snapshot(self, *, now_ns: int) -> BybitPublicSnapshotV1:
        """Acquire and validate a bounded snapshot without repository or collector access."""
        if self._cycle_started_monotonic_ns is None:
            self._cycle_started_monotonic_ns = self.monotonic_ns()
        acquisition_started_ns = self._cycle_started_monotonic_ns
        if self._metadata is None or self._bootstrap_failed:
            observed_at_ns = max(now_ns, self.clock_ns())
            duration_ns = max(0, self.monotonic_ns() - acquisition_started_ns)
            return BybitPublicSnapshotV1(
                (), False, "INCOMPLETE", "BYBIT_METADATA_BOOTSTRAP_UNAVAILABLE", now_ns, observed_at_ns,
                0, 0, duration_ns, self._bootstrap_request_count,
                self._successful_bootstrap_request_count,
            )

        translated: list[BybitPublicInputRecordV1] = []
        snapshot_complete = True
        failure_kind: str | None = None
        failure_reason: str | None = None
        latest_received_at_ns = now_ns
        request_count = 0
        successful_request_count = 0

        def request(call: Callable[[], Any]) -> Any:
            nonlocal request_count, successful_request_count
            elapsed_ns = max(0, self.monotonic_ns() - acquisition_started_ns)
            remaining_ns = self.max_acquisition_duration_ns - elapsed_ns
            if remaining_ns <= 0 or request_count >= MAX_REQUESTS_PER_SNAPSHOT:
                raise _AcquisitionBudgetExceeded
            request_count += 1
            client = getattr(self.reader, "client", None)
            prior_timeout = getattr(client, "timeout_s", None)
            if client is not None and isinstance(prior_timeout, (int, float)) and prior_timeout > 0:
                client.timeout_s = min(float(prior_timeout), remaining_ns / 1_000_000_000)
            try:
                result = call()
            finally:
                if client is not None and isinstance(prior_timeout, (int, float)) and prior_timeout > 0:
                    client.timeout_s = prior_timeout
            if self.monotonic_ns() - acquisition_started_ns > self.max_acquisition_duration_ns:
                raise _AcquisitionBudgetExceeded
            successful_request_count += 1
            return result

        try:
            server_time_ns = request(self.reader.server_time_ns)
            for product in self._metadata.products:
                key = product.key
                metadata_row = self._metadata.rows_by_symbol[key.native_symbol]
                metadata_raw = RawObservationV2.build(
                    instrument_revision=key.contract_revision, source_id=SOURCE_ID,
                    event_type="PRODUCT_METADATA", event_at_ns=None, received_at_ns=self._metadata.received_at_ns,
                    ingested_at_ns=self._metadata.received_at_ns, available_at_ns=self._metadata.received_at_ns,
                    translation_version="bybit-public-v1", payload=metadata_row,
                    sequence=key.contract_revision,
                )
                translated.append(BybitPublicInputRecordV1(
                    metadata_raw, canonical_json(metadata_row).encode(), key, None,
                ))
                for interval in CAMPAIGN_INTERVALS:
                    response = request(partial(
                        self.reader.klines, key.native_symbol, interval, limit=MAX_KLINE_ROWS,
                    ))
                    latest_received_at_ns = max(latest_received_at_ns, response.received_at_ns)
                    rows = _kline_rows(response.payload)
                    if len(rows) > MAX_KLINE_ROWS:
                        raise ValueError("Bybit kline response exceeded its requested row bound")
                    if not rows:
                        snapshot_complete = False
                    final_bars: list[CausalBarV2] = []
                    for row in rows:
                        raw, values, open_at_ns, final = translate_kline(
                            row, key=key, interval=interval, received_at_ns=response.received_at_ns,
                            server_time_ns=server_time_ns,
                        )
                        if not final:
                            continue
                        bar = translate_final_bar(
                            raw=raw, interval=interval, open_at_ns=open_at_ns, values=values, final=final,
                        )
                        if bar is None:
                            continue
                        final_bars.append(bar)
                        translated.append(BybitPublicInputRecordV1(raw, canonical_json(row).encode(), key, bar))
                    final_bars.sort(key=lambda item: item.open_at_ns)
                    if len({bar.open_at_ns for bar in final_bars}) != len(final_bars):
                        snapshot_complete = False
                    if any(
                        right.open_at_ns - left.open_at_ns != interval.duration_ns
                        for left, right in zip(final_bars, final_bars[1:], strict=False)
                    ):
                        snapshot_complete = False
                ticker_response = request(partial(self.reader.ticker, key.native_symbol))
                latest_received_at_ns = max(latest_received_at_ns, ticker_response.received_at_ns)
                ticker_rows = _result_rows(ticker_response.payload, name="ticker")
                if len(ticker_rows) > 10:
                    raise ValueError("Bybit ticker response exceeded its bounded row count")
                ticker_row = next((row for row in ticker_rows if row.get("symbol") == key.native_symbol), None)
                if ticker_row is None:
                    snapshot_complete = False
                else:
                    raw_ticker = translate_ticker(ticker_row, key=key, received_at_ns=ticker_response.received_at_ns)
                    translated.append(BybitPublicInputRecordV1(
                        raw_ticker, canonical_json(ticker_row).encode(), key, None,
                    ))
                trade_response = request(partial(
                    self.reader.recent_trades, key.native_symbol, limit=MAX_TRADE_ROWS,
                ))
                latest_received_at_ns = max(latest_received_at_ns, trade_response.received_at_ns)
                trade_rows = _result_rows(trade_response.payload, name="recent-trade")
                if len(trade_rows) > MAX_TRADE_ROWS:
                    raise ValueError("Bybit recent-trades response exceeded its requested row bound")
                if not trade_rows:
                    snapshot_complete = False
                translated_trades = translate_recent_trades(
                    trade_rows, key=key, received_at_ns=trade_response.received_at_ns,
                )
                for trade_row, raw_trade in zip(trade_rows, translated_trades, strict=True):
                    translated.append(BybitPublicInputRecordV1(raw_trade, canonical_json(trade_row).encode(), key, None))
        except _AcquisitionBudgetExceeded:
            snapshot_complete = False
            failure_kind = "INCOMPLETE"
            failure_reason = "BYBIT_PUBLIC_ACQUISITION_BUDGET_EXCEEDED"
        except PublicDataError as exc:
            snapshot_complete = False
            failure_kind = "RATE_LIMITED" if exc.rate_limited else "DISCONNECTED"
            failure_reason = "BYBIT_PUBLIC_RATE_LIMITED" if exc.rate_limited else "BYBIT_PUBLIC_TRANSPORT_FAILURE"
        except (KeyError, TypeError, ValueError, ArithmeticError):
            snapshot_complete = False
            failure_kind = "MALFORMED"
            failure_reason = "BYBIT_PUBLIC_RESPONSE_MALFORMED"

        if len(translated) > MAX_SNAPSHOT_RECORDS:
            translated = translated[:MAX_SNAPSHOT_RECORDS]
            snapshot_complete = False
            failure_kind = "MALFORMED"
            failure_reason = "BYBIT_PUBLIC_SNAPSHOT_RECORD_LIMIT_EXCEEDED"
        if not snapshot_complete and failure_kind is None:
            failure_kind = "INCOMPLETE"
            failure_reason = "BYBIT_PUBLIC_SNAPSHOT_INCOMPLETE"
        acquisition_duration_ns = max(0, self.monotonic_ns() - acquisition_started_ns)
        if acquisition_duration_ns > self.max_acquisition_duration_ns:
            snapshot_complete = False
            failure_kind = "INCOMPLETE"
            failure_reason = "BYBIT_PUBLIC_ACQUISITION_BUDGET_EXCEEDED"
        observed_at_ns = max(now_ns, latest_received_at_ns, self.clock_ns())
        return BybitPublicSnapshotV1(
            tuple(translated), snapshot_complete, failure_kind, failure_reason,
            latest_received_at_ns, observed_at_ns, request_count, successful_request_count,
            acquisition_duration_ns, self._bootstrap_request_count,
            self._successful_bootstrap_request_count,
        )


def create_bybit_public_source() -> BybitPublicCycleSourceV1:
    """Construct the source used by the explicit production-port adapter factory."""
    return BybitPublicCycleSourceV1()
