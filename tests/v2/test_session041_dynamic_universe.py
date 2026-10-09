from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.health import PublicSourceStateV2
from atlas.v2.data.subscriptions import build_subscription_plan
from atlas.v2.data.universe import (
    ComputeTierV2,
    DynamicUniverseRuntimeV2,
    UniverseObservationV2,
)
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)

CUTOFF = 1_800


def _observation(index: int, *, venue: VenueV2 = VenueV2.BYBIT, available: int = 1000,
                 turnover: int = 20_000_000, metadata_received: int | None = None):
    symbol = f"C{index:03d}USDT"
    revision = sha256_json({"symbol": symbol, "venue": venue.value, "available": available})
    key = InstrumentKeyV2(venue, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
                          symbol, symbol[:-4], "USDT", "USDT", revision)
    product = ProductContractV2(key, available, available, available, Decimal("1"), Decimal("0.01"),
                                Decimal("0.001"), Decimal("0.001"), TradingStatusV2.TRADING, revision)
    return UniverseObservationV2(product, 40, True, Decimal(turnover), Decimal("4"),
                                 PublicSourceStateV2.HEALTHY_CURRENT, 1500, {},
                                 product_metadata_received_at_ns=metadata_received)


def _build(items, **kwargs):
    return DynamicUniverseRuntimeV2(**kwargs).build_snapshot(
        tuple(items), decision_slot_ns=CUTOFF, information_cutoff_ns=CUTOFF,
        created_at_ns=CUTOFF, selection_policy_hash="a" * 64,
    )


def test_broad_profile_rejects_stale_metadata_but_default_profile_remains_compatible():
    item = _observation(1, available=100)
    legacy = _build([item])
    guarded = _build([item], max_product_age_ns=500)
    assert legacy.universe.entries[0].data_eligible
    assert not guarded.universe.entries[0].data_eligible
    assert "PRODUCT_METADATA_STALE" in guarded.universe.entries[0].reasons


def test_fresh_metadata_receipt_does_not_require_fabricating_a_contract_revision():
    item = _observation(1, available=100, metadata_received=CUTOFF)
    result = _build([item], max_product_age_ns=500)
    assert result.universe.entries[0].data_eligible
    assert item.product.available_at_ns == 100


def test_known_probability_exploration_is_stable_outside_primary_shortlist():
    items = [_observation(i) for i in range(10)]
    first = DynamicUniverseRuntimeV2().build_snapshot(
        tuple(items), decision_slot_ns=CUTOFF, information_cutoff_ns=CUTOFF,
        created_at_ns=CUTOFF, selection_policy_hash="b" * 64,
        top_tier_2=2, exploration_count=2,
    )
    second = DynamicUniverseRuntimeV2().build_snapshot(
        tuple(reversed(items)), decision_slot_ns=CUTOFF, information_cutoff_ns=CUTOFF,
        created_at_ns=CUTOFF, selection_policy_hash="b" * 64,
        top_tier_2=2, exploration_count=2,
    )
    assert first.universe.content_hash == second.universe.content_hash
    assert first.exploration == second.exploration
    assert len(first.exploration) == 2
    assert all(item.inclusion_probability == Decimal("0.25") for item in first.exploration)
    assert all(item.key not in {x.product.key for x in items[:2]} for item in first.exploration)


def test_broad_universe_population_bounds_fail_closed_per_venue_and_globally():
    with pytest.raises(ValueError, match="ACTIVE_UNIVERSE_POPULATION_OVERFLOW"):
        _build([_observation(i) for i in range(5)], max_active_observations=4)
    mixed = [_observation(i, venue=VenueV2.BYBIT) for i in range(3)]
    mixed += [_observation(i, venue=VenueV2.BINANCE) for i in range(3)]
    with pytest.raises(ValueError, match="PER_VENUE_UNIVERSE_POPULATION_OVERFLOW"):
        _build(mixed, max_observations_per_venue=2)


def test_broad_subscription_plan_has_bounded_tier3_fanout():
    items = [_observation(i) for i in range(65)]
    tiers = {item.product.key: ComputeTierV2.TIER_3 for item in items}
    with pytest.raises(ValueError, match="TIER3_SUBSCRIPTION_POPULATION_OVERFLOW"):
        build_subscription_plan(tiers, created_at_ns=CUTOFF)


def test_s3_native_cadence_promotion_stays_within_global_tier3_cap():
    items = []
    for index in range(12):
        item = _observation(index)
        items.append(UniverseObservationV2(
            item.product, item.observed_days, item.required_bars_present,
            item.trailing_24h_quote_turnover_usd, item.spread_bps, item.source_health,
            item.received_at_ns, {"S3_VWAP_STAT_MEAN_REVERSION": 7},
            product_metadata_received_at_ns=item.product_metadata_received_at_ns,
        ))
    result = DynamicUniverseRuntimeV2().build_snapshot(
        tuple(items), decision_slot_ns=CUTOFF, information_cutoff_ns=CUTOFF,
        created_at_ns=CUTOFF, selection_policy_hash="c" * 64,
        top_tier_2=2, top_tier_3=2,
    )
    assert sum(tier == ComputeTierV2.TIER_3 for tier in result.tiers.values()) == 8


@pytest.mark.parametrize("watch_count", [4, 8])
def test_active_watches_reserve_tier3_slots_before_ranked_scanner_population(watch_count):
    items = tuple(replace(_observation(index), active_watch=index >= 12 - watch_count)
                  for index in range(12))
    watched = {item.product.key for item in items if item.active_watch}
    result = DynamicUniverseRuntimeV2().build_snapshot(
        items, decision_slot_ns=CUTOFF, information_cutoff_ns=CUTOFF,
        created_at_ns=CUTOFF, selection_policy_hash="d" * 64, top_tier_3=5,
    )
    tier3 = {key for key, tier in result.tiers.items() if tier == ComputeTierV2.TIER_3}
    ranked = sorted((item for item in items if item.product.key not in watched),
        key=lambda item: (-item.trailing_24h_quote_turnover_usd, item.product.key.to_canonical_json()))

    assert len(tier3) == 8
    assert watched.issubset(tier3)
    assert {item.product.key for item in ranked[:8 - watch_count]}.issubset(tier3)


def test_policy_eligibility_uses_observed_per_policy_history_when_supplied():
    item = _observation(50)
    item = UniverseObservationV2(
        item.product, item.observed_days, item.required_bars_present,
        item.trailing_24h_quote_turnover_usd, item.spread_bps, item.source_health,
        item.received_at_ns,
        {"S1": 30, "S3_VWAP_STAT_MEAN_REVERSION": 7, "S6": 30},
        product_metadata_received_at_ns=item.product_metadata_received_at_ns,
        observed_policy_history_days={"S1": 0, "S3_VWAP_STAT_MEAN_REVERSION": 7, "S6": 0},
    )
    result = _build([item])
    eligibility = result.universe.entries[0].strategy_eligibility
    assert eligibility["S1"].status.value == "NOT_ESTIMABLE"
    assert eligibility["S3_VWAP_STAT_MEAN_REVERSION"].status.value == "ELIGIBLE"
    assert eligibility["S6"].status.value == "NOT_ESTIMABLE"
