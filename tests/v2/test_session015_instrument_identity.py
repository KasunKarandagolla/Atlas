"""Public adapter revision identity at the Phase-2 feature boundary."""

from __future__ import annotations

from dataclasses import replace

import pytest

from atlas.v2.data.bars import BarIntervalV2, CausalBarStoreV2, translate_final_bar
from atlas.v2.data.binance import SOURCE_ID as BINANCE_SOURCE_ID
from atlas.v2.data.binance import translate_exchange_info
from atlas.v2.data.binance import translate_kline as translate_binance_kline
from atlas.v2.data.bybit import SOURCE_ID as BYBIT_SOURCE_ID
from atlas.v2.data.bybit import translate_instrument_info
from atlas.v2.data.bybit import translate_kline as translate_bybit_kline
from atlas.v2.data.health import PublicSourceHealthV2, PublicSourceStateV2
from atlas.v2.features.joins import asof_join
from atlas.v2.features.pipeline import feature_snapshot
from atlas.v2.instruments import EnvironmentV2, InstrumentKeyV2, VenueV2

NS = 1_000_000_000
HOUR = 3_600 * NS


def _bybit_key(*, tick_size: str = "0.1", observed_at_ns: int = 1_000) -> InstrumentKeyV2:
    metadata = {"retCode": 0, "result": {"list": [{
        "symbol": "BTCUSDT", "baseCoin": "BTC", "quoteCoin": "USDT", "settleCoin": "USDT",
        "contractType": "LinearPerpetual", "status": "Trading", "launchTime": "1000",
        "deliveryTime": "0", "priceFilter": {"tickSize": tick_size},
        "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001", "minNotionalValue": "5"},
    }]}}
    contract = translate_instrument_info(metadata, environment=EnvironmentV2.MAINNET,
                                         observed_at_ns=observed_at_ns, available_at_ns=observed_at_ns + 1)[0]
    assert contract.key.contract_revision == contract.metadata_ref
    assert contract.effective_at_ns == observed_at_ns and contract.available_at_ns == observed_at_ns + 1
    return contract.key


def _binance_key(*, tick_size: str = "0.1", observed_at_ns: int = 1_000) -> InstrumentKeyV2:
    metadata = {"symbols": [{
        "symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT", "marginAsset": "USDT",
        "contractType": "PERPETUAL", "status": "TRADING", "onboardDate": 1000, "deliveryDate": 0,
        "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": tick_size},
            {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001", "maxQty": "1000"},
            {"filterType": "MIN_NOTIONAL", "notional": "5"},
        ],
    }]}
    contract = translate_exchange_info(metadata, environment=EnvironmentV2.MAINNET,
                                       observed_at_ns=observed_at_ns, available_at_ns=observed_at_ns + 1)[0]
    assert contract.key.contract_revision == contract.metadata_ref
    assert contract.effective_at_ns == observed_at_ns and contract.available_at_ns == observed_at_ns + 1
    return contract.key


def _append_final_triplet(store: CausalBarStoreV2, key: InstrumentKeyV2, *, close_at_ns: int) -> None:
    received = close_at_ns + NS
    for interval in (BarIntervalV2.H4, BarIntervalV2.H1, BarIntervalV2.M15):
        open_ns = close_at_ns - interval.duration_ns
        if key.venue == VenueV2.BYBIT:
            row: list[str | int] = [str(open_ns // 1_000_000), "100", "102", "99", "101", "5", "500"]
            raw, values, bar_open, final = translate_bybit_kline(
                row, key=key, interval=interval, received_at_ns=received, server_time_ns=received,
            )
        else:
            row = [open_ns // 1_000_000, "100", "102", "99", "101", "5",
                   close_at_ns // 1_000_000 - 1, "500", 10, "2", "200", "0"]
            raw, values, bar_open, final = translate_binance_kline(
                row, key=key, interval=interval, received_at_ns=received, server_time_ns=received,
            )
        bar = translate_final_bar(raw=raw, interval=interval, open_at_ns=bar_open, values=values, final=final)
        assert bar is not None and bar.instrument_revision == key.contract_revision
        assert store.append(bar)


def _health(key: InstrumentKeyV2, cutoff_ns: int) -> PublicSourceHealthV2:
    source_id = BYBIT_SOURCE_ID if key.venue == VenueV2.BYBIT else BINANCE_SOURCE_ID
    return PublicSourceHealthV2(source_id, cutoff_ns, cutoff_ns,
                                PublicSourceStateV2.HEALTHY_CURRENT, "healthy", "fixture")


@pytest.mark.parametrize("venue", [VenueV2.BYBIT, VenueV2.BINANCE])
def test_public_adapter_bars_feed_phase2_join_and_feature_snapshot(venue: VenueV2) -> None:
    key = _bybit_key() if venue == VenueV2.BYBIT else _binance_key()
    assert key.contract_revision != key.content_hash
    store = CausalBarStoreV2()
    close = 4 * HOUR
    _append_final_triplet(store, key, close_at_ns=close)
    cutoff = close + NS
    joined = asof_join(store, key, cutoff_ns=cutoff, source_health=_health(key, cutoff))
    assert joined.status == "AVAILABLE" and joined.reason is None
    assert all(bar.instrument_revision == key.contract_revision for bar in joined.h4 + joined.h1 + joined.m15)
    artifact = feature_snapshot(joined)
    assert artifact.key == key and artifact.source_health_ref == joined.source_health_ref
    assert {bar.content_hash for bar in joined.h4 + joined.h1 + joined.m15} <= set(artifact.envelope.input_refs)
    assert artifact.content_hash == feature_snapshot(joined).content_hash


def test_wrong_revision_and_venue_fail_closed_with_same_ticker() -> None:
    bybit = _bybit_key()
    binance = _binance_key()
    assert bybit.native_symbol == binance.native_symbol == "BTCUSDT"
    assert bybit.contract_revision != binance.contract_revision
    store = CausalBarStoreV2()
    close = 4 * HOUR
    cutoff = close + NS
    _append_final_triplet(store, bybit, close_at_ns=close)
    cross_venue = asof_join(store, binance, cutoff_ns=cutoff, source_health=_health(binance, cutoff))
    assert cross_venue.status == "NOT_ESTIMABLE" and cross_venue.reason == "MISSING_4H_1H_15M"
    _append_final_triplet(store, binance, close_at_ns=close)
    for key in (bybit, binance):
        joined = asof_join(store, key, cutoff_ns=cutoff, source_health=_health(key, cutoff))
        assert joined.status == "AVAILABLE"
        assert {bar.raw.source_id for bar in joined.h4 + joined.h1 + joined.m15} == {
            BYBIT_SOURCE_ID if key.venue == VenueV2.BYBIT else BINANCE_SOURCE_ID,
        }
        assert all(bar.instrument_revision == key.contract_revision for bar in joined.h4 + joined.h1 + joined.m15)
        wrong = replace(key, contract_revision="f" * 64)
        missing = asof_join(store, wrong, cutoff_ns=cutoff, source_health=_health(wrong, cutoff))
        assert missing.status == "NOT_ESTIMABLE" and missing.reason == "MISSING_4H_1H_15M"
        assert missing.h4 == missing.h1 == missing.m15 == ()
        with pytest.raises(ValueError, match="instrument revision mismatch"):
            feature_snapshot(replace(joined, key=wrong))


def test_product_revision_transition_preserves_old_evidence() -> None:
    key_a = _bybit_key(tick_size="0.1")
    key_b = _bybit_key(tick_size="0.2", observed_at_ns=4 * HOUR + NS)
    assert key_a.native_symbol == key_b.native_symbol
    assert key_a.contract_revision != key_b.contract_revision
    store = CausalBarStoreV2()
    _append_final_triplet(store, key_a, close_at_ns=4 * HOUR)
    early_cutoff = 4 * HOUR + NS
    old_join = asof_join(store, key_a, cutoff_ns=early_cutoff, source_health=_health(key_a, early_cutoff))
    old_feature = feature_snapshot(old_join)
    assert old_join.status == "AVAILABLE"
    before_b = asof_join(store, key_b, cutoff_ns=early_cutoff, source_health=_health(key_b, early_cutoff))
    assert before_b.status == "NOT_ESTIMABLE" and before_b.reason == "MISSING_4H_1H_15M"
    _append_final_triplet(store, key_b, close_at_ns=8 * HOUR)
    later_cutoff = 8 * HOUR + NS
    a_later = asof_join(store, key_a, cutoff_ns=later_cutoff, source_health=_health(key_a, later_cutoff))
    b_later = asof_join(store, key_b, cutoff_ns=later_cutoff, source_health=_health(key_b, later_cutoff))
    assert a_later.status == b_later.status == "AVAILABLE"
    assert {bar.content_hash for bar in a_later.h4 + a_later.h1 + a_later.m15} == {
        bar.content_hash for bar in old_join.h4 + old_join.h1 + old_join.m15
    }
    assert all(bar.instrument_revision == key_a.contract_revision for bar in a_later.h4 + a_later.h1 + a_later.m15)
    assert all(bar.instrument_revision == key_b.contract_revision for bar in b_later.h4 + b_later.h1 + b_later.m15)
    assert {bar.content_hash for bar in a_later.h4 + a_later.h1 + a_later.m15}.isdisjoint(
        bar.content_hash for bar in b_later.h4 + b_later.h1 + b_later.m15
    )
    assert feature_snapshot(asof_join(store, key_a, cutoff_ns=early_cutoff,
                                     source_health=_health(key_a, early_cutoff))).content_hash == old_feature.content_hash
