from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import pytest

from atlas.v2._serialization import canonical_json
from atlas.v2.contracts import OpportunityWatchV2, WatchStateV2
from atlas.v2.data.bars import BarIntervalV2, CausalBarStoreV2, close_boundary_ns, translate_final_bar
from atlas.v2.data.binance import (
    translate_agg_trades,
    translate_book_ticker,
    translate_exchange_info,
    translate_premium_index,
)
from atlas.v2.data.binance import (
    translate_funding_history as translate_binance_funding,
)
from atlas.v2.data.binance import translate_kline as translate_binance_kline
from atlas.v2.data.binance import (
    translate_open_interest as translate_binance_oi,
)
from atlas.v2.data.binance import (
    translate_open_interest_history as translate_binance_oi_history,
)
from atlas.v2.data.bybit import (
    translate_funding_history as translate_bybit_funding,
)
from atlas.v2.data.bybit import (
    translate_instrument_info,
    translate_recent_trades,
    translate_ticker,
)
from atlas.v2.data.bybit import translate_kline as translate_bybit_kline
from atlas.v2.data.bybit import (
    translate_open_interest_history as translate_bybit_oi_history,
)
from atlas.v2.data.collector import PublicCollectorV2, SequenceGapV2
from atlas.v2.data.health import PublicSourceStateV2
from atlas.v2.data.history import HistoricalImporterV2, ImportQuarantinedV2, ParquetObservationArchiveV2
from atlas.v2.data.public_http import PublicHttpClientV2, PublicVenueV2
from atlas.v2.data.raw import AppendStatusV2, AvailabilityClassV2, RawObservationStoreV2, RawObservationV2
from atlas.v2.data.subscriptions import SubscriptionPlanV2, build_subscription_plan, restore_subscription_plan
from atlas.v2.data.universe import (
    ComputeTierV2,
    DynamicUniverseRuntimeV2,
    UniverseObservationV2,
    subscription_channels_for_tier,
)
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    InstrumentRegistryV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)
from atlas.v2.memory.repository import OpsRepository

H = "a" * 64
H2 = "b" * 64
NS = 1_000_000_000


def product(venue: VenueV2 = VenueV2.BYBIT, *, status: TradingStatusV2 = TradingStatusV2.TRADING,
            effective: int = 1_000, available: int = 1_000, symbol: str = "BTCUSDT") -> ProductContractV2:
    revision = H if venue == VenueV2.BYBIT else H2
    key = InstrumentKeyV2(venue, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
                          symbol, "BTC", "USDT", "USDT", revision)
    return ProductContractV2(key, effective, effective, available, Decimal("1"), Decimal("0.1"),
                             Decimal("0.001"), Decimal("0.001"), status, revision)


def raw(*, sequence: str | int | None = "1", payload: Any = None, received: int = 100, revision_of: str | None = None,
        source: str = "BYBIT_PUBLIC_HTTP", event_type: str = "TRADE", event_at: int = 50,
        instrument_revision: str = H, availability: AvailabilityClassV2 = AvailabilityClassV2.ACTUAL_SYSTEM,
        replay_at: int | None = None) -> RawObservationV2:
    return RawObservationV2.build(
        instrument_revision=instrument_revision,
        source_id=source,
        event_type=event_type,
        event_at_ns=event_at,
        received_at_ns=received,
        ingested_at_ns=received,
        available_at_ns=received,
        payload={"value": 1} if payload is None else payload,
        translation_version="fixture-v1",
        sequence=sequence,
        revision_of=revision_of,
        availability_class=availability,
        replay_available_at_ns=replay_at,
    )


def test_raw_observation_wire_is_strict_and_append_identity_is_idempotent() -> None:
    first = raw()
    assert RawObservationV2.from_dict(first.to_dict()) == first
    assert first.received_at_ns == 100 and first.record_id == raw(received=300).record_id
    with pytest.raises(ValueError, match="unknown fields"):
        RawObservationV2.from_dict({**first.to_dict(), "extra": True})
    with pytest.raises(ValueError, match="schema_version"):
        RawObservationV2.from_dict({**first.to_dict(), "schema_version": 99})
    store = RawObservationStoreV2()
    assert store.append(first).status == AppendStatusV2.INSERTED
    assert store.append(raw(received=300)).status == AppendStatusV2.DUPLICATE
    assert store.append(raw(payload={"value": 2})).status == AppendStatusV2.CONFLICT_QUARANTINED
    assert len(store.quarantined_conflicts()) == 1


def test_health_and_subscription_plan_schemas_fail_closed() -> None:
    from atlas.v2.data.health import PublicSourceHealthV2

    health = PublicSourceHealthV2("source", 10, 10, PublicSourceStateV2.HEALTHY_CURRENT, H, "fixture")
    assert PublicSourceHealthV2.from_dict(health.to_dict()) == health
    with pytest.raises(ValueError, match="schema_version"):
        PublicSourceHealthV2.from_dict({**health.to_dict(), "schema_version": 2})
    contract = product()
    plan = build_subscription_plan({contract.key: ComputeTierV2.TIER_2}, created_at_ns=100)
    assert SubscriptionPlanV2.from_dict(plan.to_dict()) == plan
    with pytest.raises(ValueError, match="schema_version"):
        SubscriptionPlanV2.from_dict({**plan.to_dict(), "schema_version": 2})
    with pytest.raises(ValueError, match="unknown fields"):
        SubscriptionPlanV2.from_dict({**plan.to_dict(), "future_plan_option": True})
    assert {channel.value for channel in plan.specs[0].channels} >= {"KLINE_15M", "BOOK_TICKER", "TRADES"}


def test_import_keeps_actual_receipt_and_separate_reconstructed_availability(tmp_path) -> None:
    path = tmp_path / "bars.jsonl"
    event = 900 * NS
    value = {
        "event_type": "BAR_15M",
        "event_at_ns": event,
        "sequence": str(event - 900 * NS),
        "payload": {
            "open_at_ns": event - 900 * NS, "open": "100", "high": "102", "low": "99",
            "close": "101", "volume": "5", "final": True,
        },
    }
    path.write_text(json.dumps(value) + "\n", encoding="utf-8")
    imported_at = 10_000 * NS
    importer = HistoricalImporterV2(clock_ns=lambda: imported_at)
    batch = importer.import_jsonl(
        path, source_id="LOCAL_PUBLIC_FILE", instrument_revision=H, imported_at_ns=imported_at,
        replay_lag_ns=2 * NS,
    )[0]
    observation = batch.observations[0].observation
    assert observation.received_at_ns == imported_at
    assert observation.available_at_ns == imported_at
    assert observation.availability_class == AvailabilityClassV2.RECONSTRUCTED_MARKET
    assert observation.replay_available_at_ns == event + 2 * NS
    assert len(batch.bars) == 1 and batch.bars[0].final
    archive = ParquetObservationArchiveV2(tmp_path / "parquet")
    archive_file = archive.write_batch(batch)
    import pyarrow.parquet as pq
    row = pq.read_table(archive_file).to_pylist()[0]
    assert row["source_file_sha256"] == batch.file_sha256
    assert row["source_chunk_id"] == batch.chunk_id
    assert row["received_at_ns"] == imported_at
    assert row["archive_record_kind"] == "HISTORICAL_IMPORT"
    assert row["raw_payload_bytes"] == (json.dumps(value) + "\n").strip().encode("utf-8")
    assert batch.chunk_id == importer.import_jsonl(
        path, source_id="LOCAL_PUBLIC_FILE", instrument_revision=H, imported_at_ns=imported_at,
        replay_lag_ns=2 * NS,
    )[0].chunk_id


def test_import_rejects_malformed_flags_and_forming_historical_bars(tmp_path) -> None:
    path = tmp_path / "bad.jsonl"
    path.write_text(json.dumps({"event_type": "TRADE", "event_at_ns": 10, "payload": {}, "quality_flags": "bad"}) + "\n")
    with pytest.raises(ImportQuarantinedV2, match="quality_flags"):
        HistoricalImporterV2(clock_ns=lambda: 100).import_jsonl(
            path, source_id="FILE", instrument_revision=H, imported_at_ns=100, replay_lag_ns=0
        )
    path.write_text(json.dumps({
        "event_type": "BAR_1H", "event_at_ns": 3_600 * NS,
        "payload": {"open_at_ns": 0, "open": "1", "high": "1", "low": "1", "close": "1", "volume": "0", "final": False},
    }) + "\n")
    with pytest.raises(ImportQuarantinedV2, match="not final"):
        HistoricalImporterV2(clock_ns=lambda: 4_000 * NS).import_jsonl(
            path, source_id="FILE", instrument_revision=H, imported_at_ns=4_000 * NS, replay_lag_ns=0
        )


def test_import_rejects_duplicate_keys_and_nonstandard_json_numbers(tmp_path) -> None:
    path = tmp_path / "ambiguous.jsonl"
    importer = HistoricalImporterV2(clock_ns=lambda: 100)
    for line in (
        '{"event_type":"TRADE","event_type":"BAR_15M","event_at_ns":10,"payload":{}}',
        '{"event_type":"TRADE","event_at_ns":10,"payload":{"price":NaN}}',
    ):
        path.write_text(line + "\n", encoding="utf-8")
        with pytest.raises(ImportQuarantinedV2, match="corrupt or unsupported JSONL row"):
            importer.import_jsonl(path, source_id="FILE", instrument_revision=H, imported_at_ns=100, replay_lag_ns=0)


def test_historical_import_rejects_backdated_receipt_timestamp(tmp_path) -> None:
    path = tmp_path / "history.jsonl"
    path.write_text(json.dumps({"event_type": "TRADE", "event_at_ns": 10, "payload": {"p": "1"}}) + "\n")
    with pytest.raises(ImportQuarantinedV2, match="not close to the current ATLAS clock"):
        HistoricalImporterV2(clock_ns=lambda: 10_000 * NS).import_jsonl(
            path, source_id="FILE", instrument_revision=H, imported_at_ns=10, replay_lag_ns=0
        )


def test_venue_metadata_and_kline_fixtures_bind_distinct_full_instruments() -> None:
    bybit_metadata = {"retCode": 0, "result": {"list": [{
        "symbol": "BTCUSDT", "baseCoin": "BTC", "quoteCoin": "USDT", "settleCoin": "USDT",
        "contractType": "LinearPerpetual", "status": "Trading", "launchTime": "1000",
        "deliveryTime": "0", "priceFilter": {"tickSize": "0.1"},
        "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001", "minNotionalValue": "5"},
    }]}}
    binance_metadata = {"symbols": [{
        "symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT", "marginAsset": "USDT",
        "contractType": "PERPETUAL", "status": "TRADING", "onboardDate": 1000, "deliveryDate": 0,
        "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
            {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001", "maxQty": "1000"},
            {"filterType": "MIN_NOTIONAL", "notional": "5"},
        ],
    }]}
    bybit = translate_instrument_info(bybit_metadata, environment=EnvironmentV2.MAINNET,
                                      observed_at_ns=2_000, available_at_ns=2_001)[0]
    binance = translate_exchange_info(binance_metadata, environment=EnvironmentV2.MAINNET,
                                      observed_at_ns=2_000, available_at_ns=2_001)[0]
    assert bybit.key != binance.key
    assert bybit.key.native_symbol == binance.key.native_symbol == "BTCUSDT"
    assert bybit.key.venue == VenueV2.BYBIT and binance.key.venue == VenueV2.BINANCE
    assert bybit.key.contract_revision == bybit.metadata_ref

    open_ms = 0
    bybit_row = [str(open_ms), "100", "102", "99", "101", "5", "500"]
    binance_row = [open_ms, "100", "102", "99", "101", "5", 899_999, "500", 10, "2", "200", "0"]
    received = 901 * NS
    b_raw, b_values, b_open, b_final = translate_bybit_kline(
        bybit_row, key=bybit.key, interval=BarIntervalV2.M15, received_at_ns=received, server_time_ns=900 * NS
    )
    n_raw, n_values, n_open, n_final = translate_binance_kline(
        binance_row, key=binance.key, interval=BarIntervalV2.M15, received_at_ns=received, server_time_ns=900 * NS
    )
    assert b_final and n_final and b_open == n_open == 0
    assert b_raw.instrument_revision == bybit.key.contract_revision
    assert n_raw.instrument_revision == binance.key.contract_revision
    assert translate_final_bar(raw=b_raw, interval=BarIntervalV2.M15, open_at_ns=b_open, values=b_values, final=b_final)
    assert translate_final_bar(raw=n_raw, interval=BarIntervalV2.M15, open_at_ns=n_open, values=n_values, final=n_final)
    bybit_trade = translate_recent_trades(
        ({"symbol": "BTCUSDT", "execId": "exec-1", "time": "2000", "price": "101"},),
        key=bybit.key, received_at_ns=received,
    )[0]
    bybit_funding = translate_bybit_funding(
        ({"symbol": "BTCUSDT", "fundingRate": "0.001", "fundingRateTimestamp": "3000"},),
        key=bybit.key, received_at_ns=received,
    )[0]
    bybit_oi = translate_bybit_oi_history(
        ({"symbol": "BTCUSDT", "openInterest": "100", "timestamp": "4000"},),
        key=bybit.key, received_at_ns=received,
    )[0]
    assert bybit_trade.event_type == "TRADE" and bybit_trade.sequence == "exec-1"
    assert bybit_funding.event_type == "FUNDING_HISTORY"
    assert "PUBLICATION_TIME_UNKNOWN" in bybit_oi.quality_flags

    binance_trade = translate_agg_trades(
        ({"s": "BTCUSDT", "a": 42, "T": 2_000, "p": "101", "q": "1"},),
        key=binance.key, received_at_ns=received,
    )[0]
    binance_book = translate_book_ticker(
        {"symbol": "BTCUSDT", "u": 9, "E": 2_000, "b": "100", "a": "101", "B": "2", "A": "3"},
        key=binance.key, received_at_ns=received,
    )
    binance_mark = translate_premium_index(
        {"symbol": "BTCUSDT", "time": 2_000, "markPrice": "100", "indexPrice": "99", "lastFundingRate": "0.001"},
        key=binance.key, received_at_ns=received,
    )
    binance_funding = translate_binance_funding(
        ({"symbol": "BTCUSDT", "fundingTime": 3_000, "fundingRate": "0.001"},),
        key=binance.key, received_at_ns=received,
    )[0]
    binance_oi = translate_binance_oi(
        {"symbol": "BTCUSDT", "time": 2_000, "openInterest": "100"},
        key=binance.key, received_at_ns=received,
    )
    binance_oi_history = translate_binance_oi_history(
        ({"symbol": "BTCUSDT", "timestamp": 4_000, "sumOpenInterest": "100"},),
        key=binance.key, received_at_ns=received,
    )[0]
    assert binance_trade.event_type == "AGG_TRADE" and binance_trade.sequence == "42"
    assert binance_book.event_type == "BOOK_TICKER" and binance_book.sequence == "9"
    assert binance_mark.event_type == "MARK_INDEX_CURRENT_FUNDING"
    assert binance_funding.event_type == "FUNDING_HISTORY"
    assert binance_oi.event_type == "OPEN_INTEREST_CURRENT"
    assert "PUBLICATION_TIME_UNKNOWN" in binance_oi_history.quality_flags
    with pytest.raises(ValueError, match="Bybit linear-perpetual"):
        translate_ticker({"symbol": "BTCUSDT"}, key=binance.key, received_at_ns=received)


@pytest.mark.parametrize(
    ("interval", "duration"),
    [(BarIntervalV2.M15, 900), (BarIntervalV2.H1, 3_600), (BarIntervalV2.H4, 14_400)],
)
def test_utc_half_open_bar_boundaries_and_forming_exclusion(interval, duration) -> None:
    assert close_boundary_ns(0, interval) == duration * NS
    assert close_boundary_ns(duration * NS, interval) == (duration * 2) * NS
    forming = raw(event_type=f"BAR_{interval.value}", event_at=duration * NS, received=duration * NS - 1)
    assert translate_final_bar(
        raw=forming, interval=interval, open_at_ns=0,
        values={"open": "10", "high": "11", "low": "9", "close": "10", "volume": "1"}, final=False,
    ) is None


def test_public_snapshot_interval_gap_detection_fails_closed() -> None:
    from atlas.v2.data.qualification import _count_interval_gaps

    assert _count_interval_gaps((0, 900 * NS, 1_800 * NS), BarIntervalV2.M15) == 0
    assert _count_interval_gaps((0, 900 * NS, 2_700 * NS), BarIntervalV2.M15) == 1
    assert _count_interval_gaps((0, 0), BarIntervalV2.M15) == 1


def test_corrected_bar_appends_revision_without_rewriting_earlier_actual_cutoff() -> None:
    first_raw = raw(sequence="0", event_type="BAR_15M", event_at=900 * NS, received=901 * NS)
    first = translate_final_bar(
        raw=first_raw, interval=BarIntervalV2.M15, open_at_ns=0,
        values={"open": "10", "high": "11", "low": "9", "close": "10", "volume": "1"}, final=True,
    )
    assert first is not None
    correction_raw = raw(sequence="0", event_type="BAR_15M", event_at=900 * NS, received=1_000 * NS,
                         payload={"corrected": True}, revision_of=first_raw.record_id)
    correction = translate_final_bar(
        raw=correction_raw, interval=BarIntervalV2.M15, open_at_ns=0,
        values={"open": "10", "high": "12", "low": "9", "close": "11", "volume": "2"}, final=True,
    )
    assert correction is not None and correction.raw.record_id != first_raw.record_id
    store = CausalBarStoreV2()
    assert store.append(first)
    assert store.append(correction)
    assert store.as_of(H, BarIntervalV2.M15, information_cutoff_ns=950 * NS)[0].close == Decimal("10")
    assert store.as_of(H, BarIntervalV2.M15, information_cutoff_ns=1_100 * NS)[0].close == Decimal("11")
    before = store.as_of(H, BarIntervalV2.M15, information_cutoff_ns=950 * NS)[0].content_hash
    future_raw = raw(sequence="900000000000", event_type="BAR_15M", event_at=1_800 * NS, received=1_900 * NS)
    future = translate_final_bar(
        raw=future_raw, interval=BarIntervalV2.M15, open_at_ns=900 * NS,
        values={"open": "11", "high": "12", "low": "10", "close": "11", "volume": "1"}, final=True,
    )
    assert future is not None
    store.append(future)
    assert store.as_of(H, BarIntervalV2.M15, information_cutoff_ns=950 * NS)[0].content_hash == before


def test_historical_bar_as_of_uses_replay_availability_not_import_time() -> None:
    store = CausalBarStoreV2()
    first_raw = raw(sequence="0", event_type="BAR_15M", event_at=900 * NS, received=10_000 * NS,
                    availability=AvailabilityClassV2.RECONSTRUCTED_MARKET, replay_at=901 * NS)
    first = translate_final_bar(
        raw=first_raw, interval=BarIntervalV2.M15, open_at_ns=0,
        values={"open": "10", "high": "11", "low": "9", "close": "10", "volume": "1"}, final=True,
    )
    assert first is not None
    assert store.append(first)
    assert not store.as_of(H, BarIntervalV2.M15, information_cutoff_ns=900 * NS,
                           availability_class=AvailabilityClassV2.RECONSTRUCTED_MARKET)
    assert not store.as_of(H, BarIntervalV2.M15, information_cutoff_ns=11_000 * NS,
                           availability_class=AvailabilityClassV2.ACTUAL_SYSTEM)
    assert store.as_of(H, BarIntervalV2.M15, information_cutoff_ns=901 * NS,
                       availability_class=AvailabilityClassV2.RECONSTRUCTED_MARKET)


def test_dynamic_universe_point_in_time_tiers_and_suspended_evidence() -> None:
    good = product(VenueV2.BYBIT)
    suspended = product(VenueV2.BINANCE, status=TradingStatusV2.SUSPENDED)
    later = product(VenueV2.BINANCE, effective=2_000, available=2_000, symbol="ETHUSDT")
    future_health = UniverseObservationV2(
        product(VenueV2.BINANCE, symbol="SOLUSDT"), 40, True, Decimal("30000000"), Decimal("2"),
        PublicSourceStateV2.HEALTHY_CURRENT, 1_500, {}, source_health_available_at_ns=1_900,
    )
    observations = (
        UniverseObservationV2(good, 40, True, Decimal("20000000"), Decimal("4"),
                              PublicSourceStateV2.HEALTHY_CURRENT, 1_500, {"strategy-x": 30}),
        UniverseObservationV2(suspended, 80, True, Decimal("50000000"), Decimal("1"),
                              PublicSourceStateV2.HEALTHY_CURRENT, 1_500, {}),
        UniverseObservationV2(later, 40, True, Decimal("30000000"), Decimal("2"),
                              PublicSourceStateV2.HEALTHY_CURRENT, 2_500, {}),
        future_health,
    )
    runtime = DynamicUniverseRuntimeV2()
    result = runtime.build_snapshot(observations, decision_slot_ns=3_000, information_cutoff_ns=1_800,
                                    created_at_ns=1_800, selection_policy_hash=H)
    assert len(result.universe.entries) == 3
    entries = {(entry.key.venue, entry.key.native_symbol): entry for entry in result.universe.entries}
    assert entries[(VenueV2.BYBIT, "BTCUSDT")].scanner_eligible
    assert entries[(VenueV2.BYBIT, "BTCUSDT")].capital_eligible is False
    assert result.tiers[good.key] == ComputeTierV2.TIER_3  # The eligible top five.
    tier_map: Any = result.tiers
    with pytest.raises(TypeError):
        tier_map[good.key] = ComputeTierV2.TIER_0
    assert result.tiers[suspended.key] == ComputeTierV2.TIER_0
    assert not entries[(VenueV2.BINANCE, "BTCUSDT")].data_eligible
    assert entries[(VenueV2.BINANCE, "BTCUSDT")].observed
    future_entry = entries[(VenueV2.BINANCE, "SOLUSDT")]
    assert not future_entry.data_eligible and not future_entry.scanner_eligible
    assert "SOURCE_HEALTH_NOT_AVAILABLE_AT_CUTOFF" in future_entry.reasons
    assert result.universe.content_hash == runtime.build_snapshot(
        observations, decision_slot_ns=3_000, information_cutoff_ns=1_800,
        created_at_ns=1_800, selection_policy_hash=H,
    ).universe.content_hash
    assert good.content_hash in result.universe.envelope.input_refs
    assert observations[0].content_hash in result.universe.envelope.input_refs
    assert subscription_channels_for_tier(ComputeTierV2.TIER_4) == ()


def test_dynamic_universe_classifies_all_compute_tiers_with_deterministic_ties() -> None:
    observations = tuple(
        UniverseObservationV2(
            product(symbol=f"X{index:02d}USDT"),
            40,
            True,
            Decimal("20000000"),
            Decimal("3"),
            PublicSourceStateV2.HEALTHY_CURRENT,
            1_500,
            {},
            active_watch=index == 21,
        )
        for index in range(22)
    )
    runtime = DynamicUniverseRuntimeV2()
    result = runtime.build_snapshot(
        observations,
        decision_slot_ns=3_000,
        information_cutoff_ns=1_800,
        created_at_ns=1_800,
        selection_policy_hash=H,
    )
    ordered = sorted((item.product.key for item in observations), key=lambda key: key.to_canonical_json())
    assert [result.tiers[key] for key in ordered] == (
        [ComputeTierV2.TIER_3] * 5
        + [ComputeTierV2.TIER_2] * 15
        + [ComputeTierV2.TIER_1]
        + [ComputeTierV2.TIER_3]
    )
    assert all(not entry.capital_eligible for entry in result.universe.entries)


def test_collector_conflict_gap_health_restart_and_watch_subscriptions(tmp_path) -> None:
    db = tmp_path / "ops.sqlite"
    contract = product()
    registry = InstrumentRegistryV2()
    registry.register(contract)
    now = [1_000]
    repository = OpsRepository(db)
    collector = PublicCollectorV2(
        repository=repository,
        registry=registry,
        clock_ns=lambda: now[0],
        archive=ParquetObservationArchiveV2(tmp_path / "parquet"),
    )
    first = raw(sequence="10", event_at=10, received=100, payload={"id": 10})
    assert collector.ingest(first, raw_payload=canonical_json({"id": 10}), sequence_channel="trades", sequence_is_contiguous=True).append.status == AppendStatusV2.INSERTED
    no_sequence = raw(sequence=None, event_type="BOOK_TICKER", source="BYBIT_PUBLIC_HTTP", event_at=11, received=100,
                      payload={"id": "book"})
    assert collector.ingest(no_sequence, raw_payload=canonical_json({"id": "book"})).append.status == AppendStatusV2.INSERTED
    assert repository.artifact_entries("PublicObservationIndexV2") == ()
    with pytest.raises(ValueError, match="must match RawObservationV2.raw_payload_hash"):
        collector.ingest(first, raw_payload=b"different bytes")
    assert collector.flush_archive() is not None
    assert not collector._pending_archive
    assert len(repository.artifact_entries("PublicObservationIndexV2")) == 2
    import pyarrow.parquet as pq
    parquet_rows = pq.read_table(next((tmp_path / "parquet").glob("*.parquet"))).to_pylist()
    assert {row["raw_payload_bytes"] for row in parquet_rows} == {b'{"id":10}', b'{"id":"book"}'}
    duplicate = collector.ingest(first, raw_payload=canonical_json({"id": 10}), sequence_channel="trades", sequence_is_contiguous=True)
    assert duplicate.append.status == AppendStatusV2.DUPLICATE
    gap = collector.ingest(raw(sequence="13", event_at=13, received=101, payload={"id": 13}), raw_payload=canonical_json({"id": 13}),
                           sequence_channel="trades", sequence_is_contiguous=True)
    assert isinstance(gap.sequence_gap, SequenceGapV2)
    assert collector.flush_archive() is not None
    conflict = collector.ingest(raw(sequence="13", event_at=13, received=102, payload={"id": "changed"}),
                                raw_payload=canonical_json({"id": "changed"}), sequence_channel="trades", sequence_is_contiguous=True)
    assert conflict.persistent_conflict is True
    assert conflict.append.status == AppendStatusV2.CONFLICT_QUARANTINED
    assert len(collector.quarantined_conflicts()) == 1
    quarantined_path = tmp_path / "parquet" / f"{collector.quarantined_conflicts()[0].quarantine_chunk_id}.parquet"
    quarantined_row = pq.read_table(quarantined_path).to_pylist()[0]
    assert quarantined_row["archive_record_kind"] == "DUPLICATE_CONFLICT"
    assert quarantined_row["raw_payload_bytes"] == b'{"id":"changed"}'
    gap_health = collector.health.latest("BYBIT_PUBLIC_HTTP")
    assert gap_health is not None and gap_health.state == PublicSourceStateV2.SEQUENCE_GAP_CONFLICT
    now[0] = 1_100
    collector.on_disconnect("BYBIT_PUBLIC_HTTP", at_ns=1_100)
    collector.begin_reconnect("BYBIT_PUBLIC_HTTP", attempt=2, at_ns=1_101)
    collector.reconnected("BYBIT_PUBLIC_HTTP", at_ns=1_102)
    incomplete_health = collector.health.latest("BYBIT_PUBLIC_HTTP")
    assert incomplete_health is not None and incomplete_health.state == PublicSourceStateV2.INCOMPLETE_SNAPSHOT
    collector.reconcile_after_reconnect("BYBIT_PUBLIC_HTTP", at_ns=1_103,
                                        complete_snapshot=True, missed_interval_repaired=True)
    current_health = collector.health.latest("BYBIT_PUBLIC_HTTP")
    assert current_health is not None and current_health.state == PublicSourceStateV2.HEALTHY_CURRENT
    collector.checkpoint_cursors(at_ns=1_104)
    watch = OpportunityWatchV2("watch-1", contract.key, "strategy", "1", H, WatchStateV2.DETECTED,
                               0, 0, 0, H2, (), "BOOK_TICKER", 10_000, 0)
    repository.create_watch(watch)
    before_restart = restore_subscription_plan(repository, {contract.key: ComputeTierV2.TIER_1}, now_ns=1_105)[1]
    repository.close()
    reopened = OpsRepository(db)
    restarted = PublicCollectorV2(repository=reopened, registry=registry, clock_ns=lambda: 1_200)
    restart = restarted.restore_subscriptions({}, now_ns=1_200)
    assert restart.watches.active_watches == (watch,)
    assert restart.subscriptions.plan_id == before_restart.plan_id
    assert any(channel.value == "BOOK_TICKER" for channel in restart.subscriptions.specs[0].channels)
    assert restarted._last_sequence[("BYBIT_PUBLIC_HTTP", "trades")] == 13
    restored_health = restarted.health.latest("BYBIT_PUBLIC_HTTP")
    assert restored_health is not None and restored_health.state == PublicSourceStateV2.INCOMPLETE_SNAPSHOT
    restarted.reconcile_after_reconnect(
        "BYBIT_PUBLIC_HTTP", at_ns=1_201, complete_snapshot=True, missed_interval_repaired=True
    )
    restored_health = restarted.health.latest("BYBIT_PUBLIC_HTTP")
    assert restored_health is not None and restored_health.state == PublicSourceStateV2.HEALTHY_CURRENT
    # Re-ingestion after restart is idempotent and does not enqueue a second archive artifact.
    assert restarted.ingest(first, raw_payload=canonical_json({"id": 10})).append.status == AppendStatusV2.DUPLICATE
    assert restarted.ingest(no_sequence, raw_payload=canonical_json({"id": "book"})).append.status == AppendStatusV2.DUPLICATE
    assert not restarted._pending_archive
    reopened.close()


def test_collector_without_archive_refuses_to_discard_pending_observations(tmp_path) -> None:
    registry = InstrumentRegistryV2()
    registry.register(product())
    repository = OpsRepository(tmp_path / "ops.sqlite")
    collector = PublicCollectorV2(repository=repository, registry=registry, clock_ns=lambda: 100)
    collector.ingest(raw(), raw_payload=canonical_json({"value": 1}))
    with pytest.raises(RuntimeError, match="Parquet observation archive is required"):
        collector.flush_archive()
    assert len(collector._pending_archive) == 1
    repository.close()


def test_collector_records_stale_and_rate_limited_source_health(tmp_path) -> None:
    registry = InstrumentRegistryV2()
    registry.register(product())
    repository = OpsRepository(tmp_path / "ops.sqlite")
    collector = PublicCollectorV2(repository=repository, registry=registry, clock_ns=lambda: 200)
    collector.on_stale("BYBIT_PUBLIC_HTTP", at_ns=100)
    collector.on_rate_limited("BYBIT_PUBLIC_HTTP", at_ns=150)
    history = repository.source_health_history("BYBIT_PUBLIC_HTTP")
    assert tuple(item.status for item in history) == ("STALE", "DEGRADED_RATE_LIMITED")
    assert all(left.observed_at_ns < right.observed_at_ns for left, right in zip(history[:-1], history[1:], strict=True))
    repository.close()


def test_public_http_reader_rejects_non_public_and_mutating_paths() -> None:
    calls: list[str] = []

    def getter(url: str, timeout: float) -> tuple[int, bytes]:
        del timeout
        calls.append(url)
        return 200, b"{}"

    client = PublicHttpClientV2(PublicVenueV2.BINANCE, getter=getter)
    with pytest.raises(ValueError, match="not in the BINANCE public GET allowlist"):
        client.get("/fapi/v1/order", {"symbol": "BTCUSDT"})
    with pytest.raises(ValueError, match="query parameters are not in the public endpoint allowlist"):
        client.get("/fapi/v1/klines", {"symbol": "BTCUSDT", "apiKey": "must-not-be-sent"})
    assert calls == []
