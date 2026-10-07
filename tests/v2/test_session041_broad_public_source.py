from __future__ import annotations

from dataclasses import dataclass

import pytest

from atlas.v2.data.broad_public_source import BroadPublicCycleSourceV2
from atlas.v2.data.public_http import PublicDataError
from atlas.v2.instruments import VenueV2

NOW = 1_800_000_000_000_000_000


@dataclass
class Response:
    payload: object
    received_at_ns: int = NOW


def _bybit_row(symbol: str, base: str, status: str = "Trading"):
    return {
        "symbol": symbol, "baseCoin": base, "quoteCoin": "USDT", "settleCoin": "USDT",
        "contractType": "LinearPerpetual", "status": status, "launchTime": "1700000000000",
        "priceFilter": {"tickSize": "0.01"},
        "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001", "maxOrderQty": "1000"},
    }


def _binance_row(symbol: str, base: str, status: str = "TRADING"):
    return {
        "symbol": symbol, "baseAsset": base, "quoteAsset": "USDT", "marginAsset": "USDT",
        "contractType": "PERPETUAL", "status": status, "onboardDate": 1700000000000,
        "deliveryDate": 0,
        "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
            {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001", "maxQty": "1000"},
        ],
    }


class FakeBybitReader:
    def __init__(self, *, cursor_loop: bool = False):
        self.pages = {
            None: [_bybit_row("BTCUSDT", "BTC", "Trading")],
            "p2": [_bybit_row("ETHUSDT", "ETH", "Suspended")],
        }
        self.cursor_loop = cursor_loop
        self.client = self
        self.calls = []

    def instruments(self, *, limit, cursor=None):
        self.calls.append(("instruments", limit, cursor))
        next_cursor = "p2" if cursor is None else ("p2" if self.cursor_loop else "")
        return Response({"retCode": 0, "result": {"list": self.pages.get(cursor, []), "nextPageCursor": next_cursor}})

    def get(self, path, params):
        self.calls.append((path, params))
        if path.endswith("/tickers"):
            return Response({"retCode": 0, "result": {"list": [
                {"symbol": "BTCUSDT", "ts": str(NOW // 1_000_000), "bid1Price": "99", "ask1Price": "101"},
                {"symbol": "ETHUSDT", "ts": str(NOW // 1_000_000), "bid1Price": "9", "ask1Price": "11"},
            ]}})
        if path.endswith("/kline"):
            step = int(params["interval"]) * 60_000
            open_ms = int(params["end"]) // step * step
            return Response({"retCode": 0, "result": {"list": [[str(open_ms), "10", "11", "9", "10", "100", "1000"]]}})
        raise AssertionError(path)

    def klines(self, symbol, interval, *, limit):
        self.calls.append(("klines", symbol, interval, limit))
        return Response({"retCode": 0, "result": {"list": [["1799996400000", "10", "11", "9", "10", "100", "1000"]]}})

    def recent_trades(self, symbol, *, limit):
        return Response({"retCode": 0, "result": {"list": []}})

    def funding_history(self, symbol, *, limit):
        return Response({"retCode": 0, "result": {"list": []}})

    def open_interest_history(self, symbol, *, interval, limit):
        return Response({"retCode": 0, "result": {"list": []}})

    def server_time_ns(self):
        return NOW


class FakeBinanceClient:
    def __init__(self):
        self.calls = []

    def get(self, path, params=None):
        self.calls.append(path)
        if path.endswith("bookTicker"):
            return Response([{"symbol": "BTCUSDT", "bidPrice": "99", "askPrice": "101", "u": 1}])
        if path.endswith("premiumIndex"):
            return Response([{"symbol": "BTCUSDT", "markPrice": "100", "indexPrice": "100", "lastFundingRate": "0.0001", "time": NOW // 1_000_000}])
        if path.endswith("24hr"):
            return Response([{"symbol": "BTCUSDT", "quoteVolume": "20000000", "closeTime": NOW // 1_000_000}])
        raise AssertionError(path)


class FakeBinanceReader:
    def __init__(self):
        self.client = FakeBinanceClient()

    def exchange_info(self):
        return Response({"symbols": [_binance_row("BTCUSDT", "BTC"), _binance_row("ALTUSDT", "ALT", "BREAK") ]})

    def klines(self, symbol, interval, *, limit):
        duration_ms = interval.duration_ns // 1_000_000
        open_ms = (1799996400000 // duration_ms) * duration_ms
        return Response([[open_ms, "10", "11", "9", "10", "100", open_ms + duration_ms - 1]])

    def aggregate_trades(self, symbol, *, limit):
        return Response([])

    def funding_history(self, symbol, *, limit):
        return Response([])

    def open_interest(self, symbol):
        return Response({"symbol": symbol, "openInterest": "100", "time": NOW // 1_000_000})

    def open_interest_history(self, symbol, *, period, limit):
        return Response([])

    def server_time_ns(self):
        return NOW


def test_broad_source_paginates_and_keeps_both_venues_and_nontrading_products():
    bybit = FakeBybitReader()
    binance = FakeBinanceReader()
    source = BroadPublicCycleSourceV2(clock_ns=lambda: NOW,
        enabled_venues=(VenueV2.BINANCE, VenueV2.BYBIT),
        bybit_reader=bybit, binance_reader=binance,
    )
    products = source.bootstrap_products(now_ns=NOW)
    assert len(products) == 4
    assert {p.key.venue for p in products} == {VenueV2.BINANCE, VenueV2.BYBIT}
    same_symbol = [p.key for p in products if p.key.native_symbol == "BTCUSDT"]
    assert len(same_symbol) == 2 and same_symbol[0] != same_symbol[1]
    assert any(p.key.native_symbol == "ETHUSDT" and p.trading_status.value == "SUSPENDED" for p in products)
    assert bybit.calls[:2] == [("instruments", 1000, None), ("instruments", 1000, "p2")]
    assert source._metadata_manifest["BINANCE"]["status_counts"] == {"BREAK": 1, "TRADING": 1}


def test_broad_source_cursor_loop_fails_closed_without_partial_products():
    source = BroadPublicCycleSourceV2(clock_ns=lambda: NOW,
        enabled_venues=(VenueV2.BYBIT,), bybit_reader=FakeBybitReader(cursor_loop=True),
    )
    with pytest.raises(ValueError, match="CURSOR_LOOP"):
        source.bootstrap_products(now_ns=NOW)
    assert source.current_products == ()


def test_broad_source_snapshot_is_bounded_and_exposes_complete_manifest():
    source = BroadPublicCycleSourceV2(clock_ns=lambda: NOW,
        enabled_venues=(VenueV2.BYBIT, VenueV2.BINANCE),
        bybit_reader=FakeBybitReader(), binance_reader=FakeBinanceReader(),
    )
    source.bootstrap_products(now_ns=NOW)
    source.begin_collection_cycle(now_ns=NOW)
    snapshot = source.acquire_snapshot(now_ns=NOW)
    assert snapshot.request_count <= source.max_requests
    assert snapshot.successful_request_count == snapshot.request_count
    assert len(snapshot.records) <= 16384
    assert snapshot.source_snapshot["active_contract_count"] == 4
    assert snapshot.source_snapshot["metadata"]["BYBIT"]["pages"] == 2
    assert {item.instrument_key.venue for item in snapshot.records} == {VenueV2.BYBIT, VenueV2.BINANCE}


def test_stale_metadata_is_marked_incomplete_and_never_hidden():
    source = BroadPublicCycleSourceV2(clock_ns=lambda: NOW, enabled_venues=(VenueV2.BYBIT,), bybit_reader=FakeBybitReader())
    source.bootstrap_products(now_ns=NOW)
    source._metadata_received_at[VenueV2.BYBIT] = NOW - 3_600_000_000_001
    snapshot = source.acquire_snapshot(now_ns=NOW)
    assert not snapshot.complete
    assert "BYBIT" in snapshot.source_snapshot["metadata_stale"]


def test_future_metadata_receipt_is_rejected_at_actual_completion():
    source = BroadPublicCycleSourceV2(clock_ns=lambda: NOW - 1, enabled_venues=(VenueV2.BYBIT,), bybit_reader=FakeBybitReader())
    with pytest.raises(ValueError, match="RECEIPT_AFTER_COMPLETION"):
        source.bootstrap_products(now_ns=NOW - 1)
    assert source.current_products == ()


def test_refresh_missing_previously_active_contract_fails_closed_and_retains_last_population():
    reader = FakeBybitReader()
    source = BroadPublicCycleSourceV2(clock_ns=lambda: NOW, enabled_venues=(VenueV2.BYBIT,), bybit_reader=reader)
    prior = source.bootstrap_products(now_ns=NOW)
    reader.pages[None] = []
    reader.pages["p2"] = [_bybit_row("ETHUSDT", "ETH", "Suspended")]
    source.clock_ns = lambda: NOW + 3_600_000_000_001
    with pytest.raises(ValueError, match="ACTIVE_METADATA_PRODUCTS_MISSING"):
        source.bootstrap_products(now_ns=NOW + 3_600_000_000_001)
    assert source.current_products == prior
    assert source.metadata_received_at_ns(VenueV2.BYBIT) is None
    assert source._metadata_rows.get(VenueV2.BYBIT) is None
    assert source._metadata_manifest["BYBIT"]["page_complete"] is False


def test_repeated_bulk_snapshot_preserves_distinct_actual_receipts():
    bybit = FakeBybitReader()
    source = BroadPublicCycleSourceV2(clock_ns=lambda: NOW, enabled_venues=(VenueV2.BYBIT,), bybit_reader=bybit)
    source.bootstrap_products(now_ns=NOW)
    first = source.acquire_snapshot(now_ns=NOW)
    # Identical market values and venue event timestamps can be observed again
    # at a new actual receipt; the two observations need distinct identities.
    bybit.get = lambda path, params: Response({"retCode": 0, "result": {"list": [
        {"symbol": "BTCUSDT", "ts": str(NOW // 1_000_000), "bid1Price": "99", "ask1Price": "101"},
        {"symbol": "ETHUSDT", "ts": str(NOW // 1_000_000), "bid1Price": "9", "ask1Price": "11"},
    ]}}, NOW + 1)
    source.clock_ns = lambda: NOW + 1
    second = source.acquire_snapshot(now_ns=NOW + 1)
    first_rows = [row.observation for row in first.records if row.observation.event_type.startswith("TICKER_")]
    second_rows = [row.observation for row in second.records if row.observation.event_type.startswith("TICKER_")]
    assert len(first_rows) == len(second_rows) == 2
    assert {row.record_id for row in first_rows}.isdisjoint({row.record_id for row in second_rows})
    assert {row.received_at_ns for row in second_rows} == {NOW + 1}


def test_enrichment_budget_deferral_does_not_invalidate_complete_cheap_venue_snapshot():
    source = BroadPublicCycleSourceV2(clock_ns=lambda: NOW,
        enabled_venues=(VenueV2.BYBIT, VenueV2.BINANCE), bybit_reader=FakeBybitReader(),
        binance_reader=FakeBinanceReader(), max_requests=4,
    )
    source.bootstrap_products(now_ns=NOW)
    snapshot = source.acquire_snapshot(now_ns=NOW)
    assert snapshot.complete
    assert snapshot.source_snapshot["metadata"]["BYBIT"]["market_status"] == "BULK_MARKET_COMPLETE"
    assert snapshot.source_snapshot["metadata"]["BINANCE"]["market_status"] == "BULK_MARKET_COMPLETE"
    assert snapshot.source_snapshot["metadata"]["enrichment"]["status"] == "DEFERRED_BUDGET"


def test_cheap_endpoint_failure_is_scoped_to_its_venue():
    bybit = FakeBybitReader()
    bybit.get = lambda _path, _params: (_ for _ in ()).throw(PublicDataError("fixture unavailable"))
    source = BroadPublicCycleSourceV2(clock_ns=lambda: NOW,
        enabled_venues=(VenueV2.BYBIT, VenueV2.BINANCE), bybit_reader=bybit,
        binance_reader=FakeBinanceReader(),
    )
    source.bootstrap_products(now_ns=NOW)
    snapshot = source.acquire_snapshot(now_ns=NOW)
    metadata = snapshot.source_snapshot["metadata"]
    assert not snapshot.complete
    assert metadata["BYBIT"]["market_status"] == "INCOMPLETE"
    assert metadata["BINANCE"]["market_status"] == "BULK_MARKET_COMPLETE"


def test_cheap_refresh_cadence_returns_no_new_observation_claims_between_due_cycles():
    reader = FakeBybitReader()
    source = BroadPublicCycleSourceV2(clock_ns=lambda: NOW, enabled_venues=(VenueV2.BYBIT,), bybit_reader=reader)
    source.bootstrap_products(now_ns=NOW)
    source.begin_collection_cycle(now_ns=NOW)
    first = source.acquire_snapshot(now_ns=NOW)
    calls_after_due = len(reader.calls)
    source.clock_ns = lambda: NOW + 1
    source.begin_collection_cycle(now_ns=NOW + 1)
    cached = source.acquire_snapshot(now_ns=NOW + 1)
    assert first.complete and first.records
    assert cached.complete and cached.records == ()
    assert cached.request_count == 0
    assert cached.source_snapshot["acquisition_due"] is False
    assert len(reader.calls) == calls_after_due


def test_bulk_missing_active_symbol_cannot_claim_complete_market():
    reader = FakeBybitReader()
    reader.get = lambda *_args: Response({"retCode": 0, "result": {"list": []}})
    source = BroadPublicCycleSourceV2(clock_ns=lambda: NOW, enabled_venues=(VenueV2.BYBIT,), bybit_reader=reader)
    source.bootstrap_products(now_ns=NOW)
    snapshot = source.acquire_snapshot(now_ns=NOW)
    assert not snapshot.complete
    assert snapshot.source_snapshot["metadata"]["BYBIT"]["ticker_missing_symbols"] == ("BTCUSDT",)


def test_cadence_reuse_preserves_prior_failure_and_metadata_cache_failure():
    reader = FakeBybitReader()
    reader.get = lambda *_args: (_ for _ in ()).throw(PublicDataError("offline fixture"))
    source = BroadPublicCycleSourceV2(clock_ns=lambda: NOW, enabled_venues=(VenueV2.BYBIT,), bybit_reader=reader)
    source.bootstrap_products(now_ns=NOW)
    source.begin_collection_cycle(now_ns=NOW)
    failed = source.acquire_snapshot(now_ns=NOW)
    source.clock_ns = lambda: NOW + 1
    source.begin_collection_cycle(now_ns=NOW + 1)
    reused = source.acquire_snapshot(now_ns=NOW + 1)
    assert not failed.complete and not reused.complete
    assert reused.failure_kind == failed.failure_kind
    assert reused.source_snapshot["metadata"]["BYBIT"]["page_complete"] is True


def test_metadata_refresh_and_market_acquisition_share_request_budget():
    source = BroadPublicCycleSourceV2(clock_ns=lambda: NOW, enabled_venues=(VenueV2.BYBIT,),
        bybit_reader=FakeBybitReader(), max_requests=3)
    source.begin_collection_cycle(now_ns=NOW)
    snapshot = source.acquire_snapshot(now_ns=NOW)
    assert snapshot.request_count + snapshot.source_snapshot["cycle_metadata_request_count"] == 3
    assert snapshot.complete


def test_bootstrap_partial_venue_failure_cannot_leave_successful_partial_cache():
    bybit = FakeBybitReader(cursor_loop=True)
    source = BroadPublicCycleSourceV2(clock_ns=lambda: NOW, enabled_venues=(VenueV2.BYBIT, VenueV2.BINANCE),
        bybit_reader=bybit, binance_reader=FakeBinanceReader())
    with pytest.raises(ValueError, match="CURSOR_LOOP"):
        source.bootstrap_products(now_ns=NOW)
    assert source.current_products == ()
    assert source.metadata_received_at_ns(VenueV2.BINANCE) is None
    assert source._metadata_rows == {}


def test_depth_snapshot_has_independent_http_transport_owner():
    reader = FakeBinanceReader()
    source = BroadPublicCycleSourceV2(clock_ns=lambda: NOW, enabled_venues=(VenueV2.BINANCE,), binance_reader=reader)
    assert source.binance_depth_reader is not source.binance_reader
    assert source.binance_depth_reader.client is not source.binance_reader.client


def test_post_request_timeout_does_not_certify_a_late_market_response():
    tick = [0]
    reader = FakeBybitReader()
    source = BroadPublicCycleSourceV2(clock_ns=lambda: NOW, enabled_venues=(VenueV2.BYBIT,), bybit_reader=reader,
                                    monotonic_ns=lambda: tick[0])
    source.bootstrap_products(now_ns=NOW)
    original = reader.get
    def slow_get(*args):
        response = original(*args)
        tick[0] = 5_000_000_001
        return response
    reader.get = slow_get
    snapshot = source.acquire_snapshot(now_ns=NOW)
    assert not snapshot.complete
    assert snapshot.failure_kind == "BUDGET_EXCEEDED"


def test_binance_turnover_is_actual_bulk_observation_with_exact_payload():
    source = BroadPublicCycleSourceV2(clock_ns=lambda: NOW, enabled_venues=(VenueV2.BINANCE,), binance_reader=FakeBinanceReader())
    source.bootstrap_products(now_ns=NOW)
    snapshot = source.acquire_snapshot(now_ns=NOW)
    turnover, = [item for item in snapshot.records if item.observation.event_type == "TICKER_24H"]
    assert turnover.observation.received_at_ns == NOW
    assert b'"quoteVolume":"20000000"' in turnover.raw_payload
    assert snapshot.source_snapshot["metadata"]["BINANCE"]["ticker_24h_missing_symbols"] == ()


def test_history_backfill_keeps_refreshing_current_native_pages_after_restore():
    reader = FakeBybitReader()
    source = BroadPublicCycleSourceV2(clock_ns=lambda: NOW, enabled_venues=(VenueV2.BYBIT,), bybit_reader=reader)
    source.bootstrap_products(now_ns=NOW)
    source.acquire_snapshot(now_ns=NOW)
    state = source.export_state()
    restored_reader = FakeBybitReader()
    restored = BroadPublicCycleSourceV2(clock_ns=lambda: NOW, enabled_venues=(VenueV2.BYBIT,), bybit_reader=restored_reader)
    restored.restore_state(state)
    restored.bootstrap_products(now_ns=NOW)
    restored.clock_ns = lambda: NOW + 10_000_000_000
    snapshot = restored.acquire_snapshot(now_ns=NOW + 10_000_000_000)
    assert snapshot.complete
    assert sum(call[0] == "klines" for call in restored_reader.calls) == 4
    assert sum(call[0].endswith("/kline") for call in restored_reader.calls) == 4
    assert snapshot.request_count <= 32
    assert len(snapshot.records) <= 16384
    assert restored._bar_cursors_ms != source._bar_cursors_ms


def test_metadata_receipts_after_request_start_are_adopted_at_actual_completion():
    clock = [NOW]
    reader = FakeBybitReader()
    original = reader.instruments

    def advancing_response(**kwargs):
        response = original(**kwargs)
        clock[0] += 1_000_000
        response.received_at_ns = clock[0]
        return response

    reader.instruments = advancing_response
    source = BroadPublicCycleSourceV2(enabled_venues=(VenueV2.BYBIT,), bybit_reader=reader,
                                    clock_ns=lambda: clock[0])
    source.begin_collection_cycle(now_ns=NOW)
    products = source.current_products
    assert len(products) == 2
    assert clock[0] > NOW
    assert {p.available_at_ns for p in products} == {clock[0]}
    assert source.metadata_received_at_ns(VenueV2.BYBIT) == clock[0]
    snapshot = source.acquire_snapshot(now_ns=NOW)
    assert snapshot.complete
    metadata = [item for item in snapshot.records if item.observation.event_type == "PRODUCT_METADATA"]
    assert {item.instrument_key.native_symbol: item.observation.received_at_ns for item in metadata} == {
        "BTCUSDT": NOW + 1_000_000, "ETHUSDT": NOW + 2_000_000}
    assert snapshot.observed_at_ns >= clock[0]


def test_metadata_clock_regression_invalidates_the_cache():
    clock = [NOW]
    reader = FakeBybitReader()
    original = reader.instruments

    def regressing_response(**kwargs):
        response = original(**kwargs)
        clock[0] -= 1
        return response

    reader.instruments = regressing_response
    source = BroadPublicCycleSourceV2(enabled_venues=(VenueV2.BYBIT,), bybit_reader=reader,
                                    clock_ns=lambda: clock[0])
    with pytest.raises(ValueError, match="CLOCK_REGRESSED"):
        source.bootstrap_products(now_ns=NOW)
    assert source.current_products == ()
    assert source.metadata_received_at_ns(VenueV2.BYBIT) is None


def test_market_receipt_after_actual_completion_cannot_become_healthy():
    reader = FakeBybitReader()
    source = BroadPublicCycleSourceV2(enabled_venues=(VenueV2.BYBIT,), bybit_reader=reader,
                                    clock_ns=lambda: NOW)
    source.bootstrap_products(now_ns=NOW)
    original = reader.get

    def future_response(*args):
        response = original(*args)
        response.received_at_ns = NOW + 1
        return response

    reader.get = future_response
    snapshot = source.acquire_snapshot(now_ns=NOW)
    assert not snapshot.complete
    assert snapshot.source_snapshot["metadata"]["BYBIT"]["market_status"] == "INCOMPLETE"
    assert not any(item.observation.event_type.startswith("TICKER_") for item in snapshot.records)


def test_failed_market_read_does_not_fabricate_a_current_source_receipt():
    reader = FakeBybitReader()
    source = BroadPublicCycleSourceV2(enabled_venues=(VenueV2.BYBIT,), bybit_reader=reader,
                                    clock_ns=lambda: NOW)
    source.bootstrap_products(now_ns=NOW)
    source.clock_ns = lambda: NOW + 1_000_000_000
    reader.get = lambda *_args: (_ for _ in ()).throw(PublicDataError("fixture offline"))
    snapshot = source.acquire_snapshot(now_ns=NOW + 1_000_000_000)
    assert not snapshot.complete
    assert snapshot.latest_received_at_ns == NOW
    assert snapshot.observed_at_ns == NOW + 1_000_000_000


@pytest.mark.parametrize("venue,lane", [(VenueV2.BYBIT, "trades"), (VenueV2.BYBIT, "funding"),
    (VenueV2.BYBIT, "oi"), (VenueV2.BINANCE, "funding"), (VenueV2.BINANCE, "oi")])
def test_derivative_pages_are_bounded_before_translation(venue, lane):
    reader = FakeBybitReader() if venue == VenueV2.BYBIT else FakeBinanceReader()
    payload = [{} for _ in range(101)]
    response = Response({"retCode": 0, "result": {"list": payload}} if venue == VenueV2.BYBIT else payload)
    method = {"trades": "recent_trades", "funding": "funding_history", "oi": "open_interest_history"}[lane]
    setattr(reader, method, lambda *_args, **_kwargs: response)
    kwargs = {"bybit_reader": reader} if venue == VenueV2.BYBIT else {"binance_reader": reader}
    source = BroadPublicCycleSourceV2(enabled_venues=(venue,), clock_ns=lambda: NOW, **kwargs)
    source.bootstrap_products(now_ns=NOW)
    snapshot = source.acquire_snapshot(now_ns=NOW)
    assert snapshot.complete
    assert snapshot.source_snapshot["metadata"]["enrichment"]["status"] == "DEFERRED_MALFORMED_RESPONSE"
    assert len(snapshot.records) <= 16384
