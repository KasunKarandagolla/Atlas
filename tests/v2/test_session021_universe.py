"""Session-021 sleeve eligibility remains point-in-time and non-capital."""

from __future__ import annotations

from decimal import Decimal

from atlas.v2._serialization import sha256_json
from atlas.v2.contracts import EligibilityStatusV2
from atlas.v2.data.health import PublicSourceStateV2
from atlas.v2.data.universe import (
    ComputeTierV2,
    DynamicUniverseRuntimeV2,
    UniverseObservationV2,
    session021_strategy_history_days_v2,
)
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)
from atlas.v2.selection import SELECTION_POLICY_HASH

CUTOFF = 1_800


def _product(symbol: str, *, available: int = 1_000,
             status: TradingStatusV2 = TradingStatusV2.TRADING) -> ProductContractV2:
    base = symbol.removesuffix("USDT")
    revision = sha256_json({"symbol": symbol, "available": available, "status": status.value})
    key = InstrumentKeyV2(VenueV2.BYBIT, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
                          symbol, base, "USDT", "USDT", revision)
    return ProductContractV2(key, available, available, available, Decimal("1"), Decimal("0.1"),
                             Decimal("0.001"), Decimal("0.001"), status, revision)


def _observation(symbol: str, days: int, *, product: ProductContractV2 | None = None,
                 health: PublicSourceStateV2 = PublicSourceStateV2.HEALTHY_CURRENT,
                 health_available: int = 1_400) -> UniverseObservationV2:
    return UniverseObservationV2(
        product or _product(symbol), days, True, Decimal("5000000"), Decimal("3"), health,
        1_500, session021_strategy_history_days_v2(), source_health_available_at_ns=health_available,
    )


def _build(observations: tuple[UniverseObservationV2, ...]):
    return DynamicUniverseRuntimeV2().build_snapshot(
        observations, decision_slot_ns=CUTOFF, information_cutoff_ns=CUTOFF,
        created_at_ns=CUTOFF, selection_policy_hash=SELECTION_POLICY_HASH,
    )


def test_s3_s6_history_eligibility_is_policy_specific_and_noncapital() -> None:
    ready = _observation("BTCUSDT", 7)
    short = _observation("ETHUSDT", 6)
    degraded = _observation("SOLUSDT", 30, health=PublicSourceStateV2.STALE)
    result = _build((ready, short, degraded))
    entries = {item.key: item for item in result.universe.entries}

    assert entries[ready.product.key].strategy_eligibility["S3_VWAP_STAT_MEAN_REVERSION"].status == EligibilityStatusV2.ELIGIBLE
    assert entries[ready.product.key].strategy_eligibility["S6_CROSS_SECTIONAL_RELATIVE_STRENGTH"].status == EligibilityStatusV2.NOT_ESTIMABLE
    assert entries[short.product.key].strategy_eligibility["S3_VWAP_STAT_MEAN_REVERSION"].status == EligibilityStatusV2.NOT_ESTIMABLE
    assert not entries[ready.product.key].scanner_eligible
    assert not entries[ready.product.key].capital_eligible
    assert result.tiers[ready.product.key] == ComputeTierV2.TIER_3
    assert entries[degraded.product.key].strategy_eligibility["S3_VWAP_STAT_MEAN_REVERSION"].status == EligibilityStatusV2.NOT_ESTIMABLE
    assert not entries[degraded.product.key].data_eligible
    assert all(not item.capital_eligible for item in result.universe.entries)


def test_future_listing_and_status_revision_do_not_change_earlier_universe() -> None:
    active = _observation("BTCUSDT", 30)
    future_listing = _observation("NEWUSDT", 30, product=_product("NEWUSDT", available=CUTOFF + 1))
    future_suspension = _observation(
        "ETHUSDT", 30, product=_product("ETHUSDT", available=CUTOFF + 2, status=TradingStatusV2.SUSPENDED),
    )
    earlier = _build((active,))
    with_future_tail = _build((active, future_listing, future_suspension))
    assert with_future_tail.universe.content_hash == earlier.universe.content_hash
    assert tuple(entry.key for entry in with_future_tail.universe.entries) == (active.product.key,)
