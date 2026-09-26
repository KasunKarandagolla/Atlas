"""Point-in-time dynamic universe screening and compute tiers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import IntEnum, StrEnum
from types import MappingProxyType

from .._serialization import FrozenMap, decimal_value, sha256_json, sha256_ref, timestamp
from ..contracts import ArtifactEnvelope, EligibilityStatusV2
from ..instruments import (
    InstrumentKeyV2,
    ProductContractV2,
    StrategyEligibilityV2,
    TradingStatusV2,
    UniverseContractV2,
    UniverseEntryV2,
)
from .health import PublicSourceStateV2


class ComputeTierV2(IntEnum):
    TIER_0 = 0
    TIER_1 = 1
    TIER_2 = 2
    TIER_3 = 3
    TIER_4 = 4


class SubscriptionChannelV2(StrEnum):
    INSTRUMENT_INFO = "INSTRUMENT_INFO"
    KLINE_15M = "KLINE_15M"
    KLINE_1H = "KLINE_1H"
    KLINE_4H = "KLINE_4H"
    TRADES = "TRADES"
    BOOK_TICKER = "BOOK_TICKER"
    MARK_INDEX = "MARK_INDEX"
    FUNDING = "FUNDING"
    OPEN_INTEREST = "OPEN_INTEREST"


@dataclass(frozen=True)
class UniverseObservationV2:
    product: ProductContractV2
    observed_days: int
    required_bars_present: bool
    trailing_24h_quote_turnover_usd: Decimal
    spread_bps: Decimal
    source_health: PublicSourceStateV2
    received_at_ns: int
    strategy_history_days: Mapping[str, int]
    source_health_available_at_ns: int | None = None
    open_position: bool = False
    active_watch: bool = False

    def __post_init__(self) -> None:
        if type(self.observed_days) is not int or self.observed_days < 0:
            raise ValueError("observed_days cannot be negative")
        turnover = decimal_value(self.trailing_24h_quote_turnover_usd, field="trailing_24h_quote_turnover_usd")
        spread = decimal_value(self.spread_bps, field="spread_bps")
        if turnover < 0 or spread < 0:
            raise ValueError("turnover and spread must be nonnegative")
        if (
            type(self.required_bars_present) is not bool
            or type(self.open_position) is not bool
            or type(self.active_watch) is not bool
        ):
            raise ValueError("bar presence, open position and active watch must be explicit booleans")
        timestamp(self.received_at_ns, field="received_at_ns")
        health_available = (
            self.received_at_ns if self.source_health_available_at_ns is None else self.source_health_available_at_ns
        )
        timestamp(health_available, field="source_health_available_at_ns")
        object.__setattr__(self, "source_health_available_at_ns", health_available)
        object.__setattr__(self, "source_health", PublicSourceStateV2(self.source_health))
        object.__setattr__(self, "trailing_24h_quote_turnover_usd", turnover)
        object.__setattr__(self, "spread_bps", spread)
        if any(not policy or type(days) is not int or days < 0 for policy, days in self.strategy_history_days.items()):
            raise ValueError("strategy history requirements must use non-empty policy IDs and nonnegative days")
        object.__setattr__(self, "strategy_history_days", MappingProxyType(dict(self.strategy_history_days)))

    @property
    def content_hash(self) -> str:
        return sha256_json(
            {
                "artifact_type": "UniverseObservationV2",
                "product_ref": self.product.content_hash,
                "observed_days": self.observed_days,
                "required_bars_present": self.required_bars_present,
                "trailing_24h_quote_turnover_usd": str(self.trailing_24h_quote_turnover_usd),
                "spread_bps": str(self.spread_bps),
                "source_health": self.source_health.value,
                "source_health_available_at_ns": self.source_health_available_at_ns,
                "received_at_ns": self.received_at_ns,
                "strategy_history_days": dict(self.strategy_history_days),
                "open_position": self.open_position,
                "active_watch": self.active_watch,
            }
        )


@dataclass(frozen=True)
class UniverseRuntimeResultV2:
    universe: UniverseContractV2
    tiers: Mapping[InstrumentKeyV2, ComputeTierV2]

    def __post_init__(self) -> None:
        frozen = {key: ComputeTierV2(value) for key, value in self.tiers.items()}
        object.__setattr__(self, "tiers", MappingProxyType(frozen))


class DynamicUniverseRuntimeV2:
    """Applies frozen engineering defaults; never infers Tier 4 capital eligibility."""

    def __init__(
        self,
        *,
        min_observed_days: int = 30,
        min_turnover_usd: Decimal = Decimal("10000000"),
        max_spread_bps: Decimal = Decimal("10"),
    ) -> None:
        if min_observed_days < 0 or min_turnover_usd < 0 or max_spread_bps < 0:
            raise ValueError("universe screening thresholds must be nonnegative")
        self.min_observed_days = min_observed_days
        self.min_turnover_usd = min_turnover_usd
        self.max_spread_bps = max_spread_bps

    def build_snapshot(
        self,
        observations: tuple[UniverseObservationV2, ...],
        *,
        decision_slot_ns: int,
        information_cutoff_ns: int,
        created_at_ns: int,
        selection_policy_hash: str,
        top_tier_2: int = 20,
        top_tier_3: int = 5,
        input_refs: Sequence[str] = (),
    ) -> UniverseRuntimeResultV2:
        timestamp(decision_slot_ns, field="decision_slot_ns")
        timestamp(information_cutoff_ns, field="information_cutoff_ns")
        timestamp(created_at_ns, field="created_at_ns")
        if information_cutoff_ns > decision_slot_ns or created_at_ns > information_cutoff_ns:
            raise ValueError("universe snapshot must be available at its information cutoff")
        if top_tier_2 < 0 or top_tier_3 < 0:
            raise ValueError("compute tier sizes must be nonnegative")
        causal_refs = tuple(input_refs)
        if causal_refs != tuple(sorted(set(causal_refs))):
            raise ValueError("universe input refs must be sorted and unique")
        for ref in causal_refs:
            sha256_ref(ref, field="universe input ref")
        entries: list[UniverseEntryV2] = []
        tier0: list[UniverseObservationV2] = []
        scanner: list[UniverseObservationV2] = []
        for item in observations:
            product = item.product
            if product.available_at_ns > information_cutoff_ns or product.effective_at_ns > decision_slot_ns:
                continue
            if item.received_at_ns > information_cutoff_ns:
                continue
            tier0.append(item)
            eligibility_reasons: list[str] = []
            if product.trading_status != TradingStatusV2.TRADING:
                eligibility_reasons.append(f"PRODUCT_{product.trading_status.value}")
            if not item.required_bars_present:
                eligibility_reasons.append("REQUIRED_BARS_MISSING")
            if item.source_health_available_at_ns is None or item.source_health_available_at_ns > information_cutoff_ns:
                eligibility_reasons.append("SOURCE_HEALTH_NOT_AVAILABLE_AT_CUTOFF")
            if item.source_health != PublicSourceStateV2.HEALTHY_CURRENT:
                eligibility_reasons.append(f"SOURCE_{item.source_health.value}")
            data_eligible = not eligibility_reasons
            scanner_reasons = list(eligibility_reasons)
            if item.observed_days < self.min_observed_days:
                scanner_reasons.append("INSUFFICIENT_OBSERVED_DAYS")
            if item.trailing_24h_quote_turnover_usd < self.min_turnover_usd:
                scanner_reasons.append("TURNOVER_BELOW_ENGINEERING_DEFAULT")
            if item.spread_bps > self.max_spread_bps:
                scanner_reasons.append("SPREAD_ABOVE_ENGINEERING_DEFAULT")
            scanner_eligible = data_eligible and not (
                item.observed_days < self.min_observed_days
                or item.trailing_24h_quote_turnover_usd < self.min_turnover_usd
                or item.spread_bps > self.max_spread_bps
            )
            if scanner_eligible:
                scanner.append(item)
            strategies = {
                policy_id: StrategyEligibilityV2(
                    EligibilityStatusV2.ELIGIBLE
                    if item.observed_days >= required_days and data_eligible
                    else EligibilityStatusV2.NOT_ESTIMABLE,
                    None
                    if item.observed_days >= required_days and data_eligible
                    else "POLICY_HISTORY_OR_DATA_REQUIREMENT_NOT_MET",
                )
                for policy_id, required_days in item.strategy_history_days.items()
            }
            entries.append(
                UniverseEntryV2(
                    product.key,
                    product.content_hash,
                    True,
                    data_eligible,
                    scanner_eligible,
                    False,
                    False,
                    FrozenMap(strategies),
                    tuple(sorted(set(scanner_reasons + ["CAPITAL_ELIGIBILITY_NOT_QUALIFIED"]))),
                )
            )
        scanner.sort(
            key=lambda item: (
                -item.trailing_24h_quote_turnover_usd,
                item.product.key.to_canonical_json(),
            )
        )
        tier2_keys = {item.product.key for item in scanner[:top_tier_2]}
        tier3_primary = scanner[:top_tier_3]
        tier3_keys = {item.product.key for item in tier3_primary}
        tier3_keys.update(item.product.key for item in tier0 if item.open_position or item.active_watch)
        tiers: dict[InstrumentKeyV2, ComputeTierV2] = {}
        for item in tier0:
            key = item.product.key
            tiers[key] = (
                ComputeTierV2.TIER_3
                if key in tier3_keys
                else ComputeTierV2.TIER_2
                if key in tier2_keys
                else ComputeTierV2.TIER_1
                if any(entry.key == key and entry.scanner_eligible for entry in entries)
                else ComputeTierV2.TIER_0
            )
        artifact_refs = tuple(
            sorted(
                {reference for item in tier0 for reference in (item.product.content_hash, item.content_hash)}
                | set(causal_refs)
            )
        )
        envelope = ArtifactEnvelope(
            1,
            f"universe-{decision_slot_ns}-{sha256_json(artifact_refs)[:16]}",
            created_at_ns,
            created_at_ns,
            "dynamic-universe-v1",
            artifact_refs,
        )
        universe = UniverseContractV2(
            envelope,
            "dynamic-universe-v1",
            decision_slot_ns,
            selection_policy_hash,
            tuple(sorted(entries, key=lambda entry: entry.key.to_canonical_json())),
        )
        return UniverseRuntimeResultV2(universe, tiers)


def subscription_channels_for_tier(tier: ComputeTierV2) -> tuple[SubscriptionChannelV2, ...]:
    tier = ComputeTierV2(tier)
    by_tier = {
        ComputeTierV2.TIER_0: (SubscriptionChannelV2.INSTRUMENT_INFO,),
        ComputeTierV2.TIER_1: (
            SubscriptionChannelV2.KLINE_15M,
            SubscriptionChannelV2.KLINE_1H,
            SubscriptionChannelV2.KLINE_4H,
            SubscriptionChannelV2.BOOK_TICKER,
            SubscriptionChannelV2.MARK_INDEX,
            SubscriptionChannelV2.FUNDING,
            SubscriptionChannelV2.OPEN_INTEREST,
        ),
        ComputeTierV2.TIER_2: (
            SubscriptionChannelV2.KLINE_15M,
            SubscriptionChannelV2.KLINE_1H,
            SubscriptionChannelV2.KLINE_4H,
            SubscriptionChannelV2.BOOK_TICKER,
            SubscriptionChannelV2.MARK_INDEX,
            SubscriptionChannelV2.FUNDING,
            SubscriptionChannelV2.OPEN_INTEREST,
            SubscriptionChannelV2.TRADES,
        ),
        ComputeTierV2.TIER_3: (
            SubscriptionChannelV2.KLINE_15M,
            SubscriptionChannelV2.KLINE_1H,
            SubscriptionChannelV2.KLINE_4H,
            SubscriptionChannelV2.BOOK_TICKER,
            SubscriptionChannelV2.MARK_INDEX,
            SubscriptionChannelV2.FUNDING,
            SubscriptionChannelV2.OPEN_INTEREST,
            SubscriptionChannelV2.TRADES,
        ),
        ComputeTierV2.TIER_4: (),
    }
    return by_tier[tier]
