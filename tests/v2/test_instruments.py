from decimal import Decimal

import pytest

from atlas.v2._serialization import FrozenMap, sha256_json
from atlas.v2.contracts import ArtifactEnvelope
from atlas.v2.instruments import (
    EligibilityStatusV2,
    EnvironmentV2,
    InstrumentKeyV2,
    InstrumentRegistryV2,
    ProductContractV2,
    ProductTypeV2,
    StrategyEligibilityV2,
    TradingStatusV2,
    UniverseContractV2,
    UniverseEntryV2,
    VenueV2,
)


def key(venue: VenueV2 = VenueV2.BYBIT, revision: str = "r1") -> InstrumentKeyV2:
    return InstrumentKeyV2(venue, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL, "BTCUSDT", "BTC", "USDT", "USDT", revision)


def product(k: InstrumentKeyV2, *, effective: int = 10, observed: int = 20, available: int = 30, status: TradingStatusV2 = TradingStatusV2.TRADING) -> ProductContractV2:
    return ProductContractV2(k, effective, observed, available, Decimal("1"), Decimal("0.1"), Decimal("0.001"), Decimal("0.001"), status, "metadata-ref")


def test_identity_contains_venue_revision_and_strict_schema() -> None:
    bybit = key()
    binance = key(VenueV2.BINANCE)
    rev = key(revision="r2")
    assert bybit != binance and bybit != rev
    assert InstrumentKeyV2.from_dict(bybit.to_dict()) == bybit
    with pytest.raises(ValueError, match="unknown fields"):
        InstrumentKeyV2.from_dict({**bybit.to_dict(), "ticker": "BTCUSDT"})
    with pytest.raises(ValueError, match="unsupported"):
        InstrumentKeyV2.from_dict({**bybit.to_dict(), "schema_version": 2})
    with pytest.raises(ValueError, match="USDT"):
        InstrumentKeyV2(VenueV2.BYBIT, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL, "BTCUSDT", "BTC", "USD", "USDT", "r1")


def test_registry_is_point_in_time_append_only_and_idempotent() -> None:
    registry = InstrumentRegistryV2()
    early = product(key(), effective=10, observed=20, available=30)
    later = product(key(), effective=40, observed=45, available=50, status=TradingStatusV2.DELISTED)
    assert registry.register(early) == early
    assert registry.register(early) == early
    registry.register(later)
    assert registry.resolve_as_of(key(), decision_slot_ns=35, information_cutoff_ns=35) == early
    assert registry.resolve_as_of(key(), decision_slot_ns=60, information_cutoff_ns=49) == early
    assert registry.resolve_as_of(key(), decision_slot_ns=60, information_cutoff_ns=50) == later
    assert registry.get_by_ref(early.content_hash) == early
    assert ProductContractV2.from_dict(early.to_dict()) == early
    assert early.to_dict()["tick_size"] == "0.1"
    assert len(registry.contracts()) == 2
    conflict = product(key(), effective=10, observed=21, available=31)
    with pytest.raises(ValueError, match="conflicting"):
        registry.register(conflict)
    with pytest.raises(ValueError, match="full InstrumentKeyV2"):
        registry.resolve_as_of("BTCUSDT", decision_slot_ns=35, information_cutoff_ns=35)  # type: ignore[arg-type]


def test_universe_keeps_ineligible_entries_and_hashes_membership() -> None:
    first = product(key())
    entry = UniverseEntryV2(
        key(), first.content_hash, True, True, True, False, False,
        FrozenMap({"policy-a": StrategyEligibilityV2(EligibilityStatusV2.INELIGIBLE, "WARMUP")}),
        ("NO_CAPITAL_AUTHORITY",),
    )
    registry = InstrumentRegistryV2()
    registry.register(first)
    universe = UniverseContractV2(
        ArtifactEnvelope(1, "universe", 31, 32, "registry/1", (first.content_hash,)),
        "universe/1", 40, sha256_json("selection"), (entry,),
    )
    assert universe.entries[0].observed and not universe.entries[0].capital_eligible
    assert universe.entries[0].strategy_eligibility["policy-a"].status != EligibilityStatusV2.ELIGIBLE
    assert UniverseContractV2.from_dict(universe.to_dict()).content_hash == universe.content_hash
    universe.validate_as_of(registry, information_cutoff_ns=35)
    with pytest.raises(ValueError, match="not available"):
        universe.validate_as_of(registry, information_cutoff_ns=31)
    changed = UniverseContractV2(
        ArtifactEnvelope(1, "universe-2", 31, 32, "registry/1", (first.content_hash,)),
        "universe/2", 40, sha256_json("selection"), (),
    )
    assert changed.content_hash != universe.content_hash


def test_universe_rejects_future_product_revision_and_registry_never_ticker_joins() -> None:
    registry = InstrumentRegistryV2()
    future = product(key(), effective=10, observed=50, available=60)
    registry.register(future)
    entry = UniverseEntryV2(key(), future.content_hash, True, True, False, False, False, FrozenMap(), ())
    with pytest.raises(ValueError, match="unavailable"):
        registry.validate_universe_as_of((entry,), decision_slot_ns=40, information_cutoff_ns=40)
