"""Point-in-time dynamic universe screening and compute tiers."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
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

MAX_UNIVERSE_OBSERVATIONS_V2 = 4096
MAX_UNIVERSE_PER_VENUE_V2 = 2048
MAX_UNIVERSE_TIER3_V2 = 8


class ComputeTierV2(IntEnum):
    TIER_0 = 0
    TIER_1 = 1
    TIER_2 = 2
    TIER_3 = 3
    TIER_4 = 4


class SubscriptionChannelV2(StrEnum):
    INSTRUMENT_INFO = "INSTRUMENT_INFO"
    KLINE_1M = "KLINE_1M"
    KLINE_5M = "KLINE_5M"
    KLINE_15M = "KLINE_15M"
    KLINE_1H = "KLINE_1H"
    KLINE_4H = "KLINE_4H"
    TRADES = "TRADES"
    BOOK_TICKER = "BOOK_TICKER"
    MARK_INDEX = "MARK_INDEX"
    FUNDING = "FUNDING"
    OPEN_INTEREST = "OPEN_INTEREST"


def session021_strategy_history_days_v2() -> dict[str, int]:
    """Return the point-in-time observed-history minimums for S3 and S6.

    Callers pass this additive map in ``UniverseObservationV2``. Keeping it
    opt-in preserves the accepted S1/S2 universe artifact and selection inputs.
    The policies separately validate exact bars, trades, health and liquidity.
    """
    return {
        "S3_VWAP_STAT_MEAN_REVERSION": 7,
        "S6_CROSS_SECTIONAL_RELATIVE_STRENGTH": 30,
    }


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
    product_metadata_received_at_ns: int | None = None
    observed_policy_history_days: Mapping[str, int] | None = None

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
        if self.product_metadata_received_at_ns is not None:
            timestamp(self.product_metadata_received_at_ns, field="product_metadata_received_at_ns")
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
        if self.observed_policy_history_days is not None:
            if any(not policy or type(days) is not int or days < 0
                   for policy, days in self.observed_policy_history_days.items()):
                raise ValueError("observed policy history must use non-empty policy IDs and nonnegative days")
            object.__setattr__(self, "observed_policy_history_days",
                               MappingProxyType(dict(self.observed_policy_history_days)))

    @property
    def content_hash(self) -> str:
        body = {
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
        if self.product_metadata_received_at_ns is not None:
            body["product_metadata_received_at_ns"] = self.product_metadata_received_at_ns
        if self.observed_policy_history_days is not None:
            body["observed_policy_history_days"] = dict(self.observed_policy_history_days)
        return sha256_json(body)


@dataclass(frozen=True)
class UniverseRuntimeResultV2:
    universe: UniverseContractV2
    tiers: Mapping[InstrumentKeyV2, ComputeTierV2]
    exploration: tuple[UniverseExplorationEntryV2, ...] = ()

    def __post_init__(self) -> None:
        frozen = {key: ComputeTierV2(value) for key, value in self.tiers.items()}
        object.__setattr__(self, "tiers", MappingProxyType(frozen))
        object.__setattr__(self, "exploration", tuple(self.exploration))


@dataclass(frozen=True)
class UniverseExplorationEntryV2:
    key: InstrumentKeyV2
    inclusion_probability: Decimal
    sample_rank: str


class DynamicUniverseRuntimeV2:
    """Applies frozen engineering defaults; never infers Tier 4 capital eligibility."""

    def __init__(
        self,
        *,
        min_observed_days: int = 30,
        min_turnover_usd: Decimal = Decimal("10000000"),
        max_spread_bps: Decimal = Decimal("10"),
        max_product_age_ns: int | None = None,
        max_active_observations: int = MAX_UNIVERSE_OBSERVATIONS_V2,
        max_observations_per_venue: int = MAX_UNIVERSE_PER_VENUE_V2,
    ) -> None:
        if min_observed_days < 0 or min_turnover_usd < 0 or max_spread_bps < 0:
            raise ValueError("universe screening thresholds must be nonnegative")
        if max_product_age_ns is not None and max_product_age_ns <= 0:
            raise ValueError("product metadata maximum age must be positive")
        if not 0 < max_active_observations <= MAX_UNIVERSE_OBSERVATIONS_V2:
            raise ValueError("active universe bound exceeds the fixed maximum")
        if not 0 < max_observations_per_venue <= MAX_UNIVERSE_PER_VENUE_V2:
            raise ValueError("per-venue universe bound exceeds the fixed maximum")
        self.min_observed_days = min_observed_days
        self.min_turnover_usd = min_turnover_usd
        self.max_spread_bps = max_spread_bps
        self.max_product_age_ns = max_product_age_ns
        self.max_active_observations = max_active_observations
        self.max_observations_per_venue = max_observations_per_venue

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
        publication_at_ns: int | None = None,
        exploration_count: int = 0,
        service: Callable[[], None] | None = None,
    ) -> UniverseRuntimeResultV2:
        timestamp(decision_slot_ns, field="decision_slot_ns")
        timestamp(information_cutoff_ns, field="information_cutoff_ns")
        timestamp(created_at_ns, field="created_at_ns")
        if information_cutoff_ns > decision_slot_ns or (publication_at_ns is None and created_at_ns > information_cutoff_ns):
            raise ValueError("universe snapshot must be available at its information cutoff")
        if publication_at_ns is not None and not information_cutoff_ns <= created_at_ns <= publication_at_ns:
            raise ValueError("universe derived publication chronology invalid")
        if top_tier_2 < 0 or top_tier_3 < 0:
            raise ValueError("compute tier sizes must be nonnegative")
        if type(exploration_count) is not int or exploration_count < 0:
            raise ValueError("exploration_count must be a nonnegative integer")
        if len(observations) > self.max_active_observations:
            raise ValueError("ACTIVE_UNIVERSE_POPULATION_OVERFLOW")
        venue_counts: dict[object, int] = {}
        seen_keys: set[InstrumentKeyV2] = set()
        for index, item in enumerate(observations):
            if service is not None and index % 32 == 0:
                service()
            key = item.product.key
            if key in seen_keys:
                raise ValueError("universe observations must contain each full instrument identity once")
            seen_keys.add(key)
            venue_counts[key.venue] = venue_counts.get(key.venue, 0) + 1
        if any(value > self.max_observations_per_venue for value in venue_counts.values()):
            raise ValueError("PER_VENUE_UNIVERSE_POPULATION_OVERFLOW")
        causal_refs = tuple(input_refs)
        if causal_refs != tuple(sorted(set(causal_refs))):
            raise ValueError("universe input refs must be sorted and unique")
        for ref in causal_refs:
            sha256_ref(ref, field="universe input ref")
        entries: list[UniverseEntryV2] = []
        entry_by_key: dict[InstrumentKeyV2, UniverseEntryV2] = {}
        tier0: list[UniverseObservationV2] = []
        scanner: list[UniverseObservationV2] = []
        for index, item in enumerate(observations):
            if service is not None and index % 32 == 0:
                service()
            product = item.product
            if product.available_at_ns > information_cutoff_ns or product.effective_at_ns > decision_slot_ns:
                continue
            if item.received_at_ns > information_cutoff_ns:
                continue
            tier0.append(item)
            eligibility_reasons: list[str] = []
            metadata_received = item.product_metadata_received_at_ns
            if metadata_received is None:
                metadata_received = product.available_at_ns
            if self.max_product_age_ns is not None and (
                metadata_received > information_cutoff_ns
                or information_cutoff_ns - metadata_received > self.max_product_age_ns
            ):
                eligibility_reasons.append("PRODUCT_METADATA_STALE")
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
                    if (item.observed_policy_history_days.get(policy_id, 0)
                        if item.observed_policy_history_days is not None else item.observed_days) >= required_days
                    and data_eligible
                    else EligibilityStatusV2.NOT_ESTIMABLE,
                    None
                    if (item.observed_policy_history_days.get(policy_id, 0)
                        if item.observed_policy_history_days is not None else item.observed_days) >= required_days
                    and data_eligible
                    else "POLICY_HISTORY_OR_DATA_REQUIREMENT_NOT_MET",
                )
                for policy_id, required_days in item.strategy_history_days.items()
            }
            entry = UniverseEntryV2(
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
            entries.append(entry)
            entry_by_key[entry.key] = entry
        scanner.sort(
            key=lambda item: (
                -item.trailing_24h_quote_turnover_usd,
                item.product.key.to_canonical_json(),
            )
        )
        tier2_keys = {item.product.key for item in scanner[:top_tier_2]}
        exploration_population = scanner[top_tier_2:]
        exploration_count = min(exploration_count, len(exploration_population))
        exploration_ranked = sorted(
            exploration_population,
            key=lambda item: sha256_json({
                "decision_slot_ns": decision_slot_ns,
                "selection_policy_hash": selection_policy_hash,
                "key": item.product.key.to_dict(),
            }),
        )
        exploration_probability = (
            Decimal(exploration_count) / Decimal(len(exploration_population))
            if exploration_population and exploration_count else Decimal("0")
        )
        exploration = tuple(
            UniverseExplorationEntryV2(
                item.product.key,
                exploration_probability,
                sha256_json({
                    "decision_slot_ns": decision_slot_ns,
                    "selection_policy_hash": selection_policy_hash,
                    "key": item.product.key.to_dict(),
                }),
            )
            for item in exploration_ranked[:exploration_count]
        )
        tier3_mandatory_keys = {
            item.product.key for item in tier0 if item.open_position or item.active_watch
        }
        if len(tier3_mandatory_keys) > MAX_UNIVERSE_TIER3_V2:
            raise ValueError("TIER3_UNIVERSE_POPULATION_OVERFLOW")
        # Reserve deep-analysis capacity for active watches and positions first.
        # Scanner-ranked opportunities fill only the remaining slots, preserving
        # the global TIER_3 bound when mandatory work overlaps poorly with rank.
        primary_capacity = max(0, MAX_UNIVERSE_TIER3_V2 - len(tier3_mandatory_keys))
        ranked_primary_keys = [
            item.product.key for item in scanner[:top_tier_3]
            if item.product.key not in tier3_mandatory_keys
        ]
        tier3_keys = set(tier3_mandatory_keys) | set(ranked_primary_keys[:primary_capacity])
        # Explicitly opted-in S3 history requirements receive 1M observations.
        # With the default S1/S2-only map this adds no subscriptions or tier changes.
        s3_policy = "S3_VWAP_STAT_MEAN_REVERSION"
        s3_candidates = sorted(
            (item for item in tier0
             if item.strategy_history_days.get(s3_policy, 0) >= 7
             and (entry := entry_by_key[item.product.key]).strategy_eligibility.get(s3_policy) is not None
             and entry.strategy_eligibility[s3_policy].status == EligibilityStatusV2.ELIGIBLE
             and item.product.key not in tier3_keys),
            key=lambda item: (-item.trailing_24h_quote_turnover_usd, item.product.key.to_canonical_json()),
        )
        s3_capacity = max(0, MAX_UNIVERSE_TIER3_V2 - len(tier3_keys))
        tier3_keys.update(item.product.key for item in s3_candidates[:s3_capacity])
        if len(tier3_keys) > MAX_UNIVERSE_TIER3_V2:
            raise ValueError("TIER3_UNIVERSE_POPULATION_OVERFLOW")
        scanner_keys = {item.product.key for item in scanner}
        tiers: dict[InstrumentKeyV2, ComputeTierV2] = {}
        for index, item in enumerate(tier0):
            if service is not None and index % 32 == 0:
                service()
            key = item.product.key
            tiers[key] = (
                ComputeTierV2.TIER_3
                if key in tier3_keys
                else ComputeTierV2.TIER_2
                if key in tier2_keys
                else ComputeTierV2.TIER_1
                if key in scanner_keys
                else ComputeTierV2.TIER_0
            )
        references = set(causal_refs)
        for index, item in enumerate(tier0):
            if service is not None and index % 32 == 0:
                service()
            references.update((item.product.content_hash, item.content_hash))
        artifact_refs = tuple(sorted(references))
        envelope = ArtifactEnvelope(
            1,
            f"universe-{decision_slot_ns}-{sha256_json(artifact_refs)[:16]}",
            created_at_ns,
            created_at_ns if publication_at_ns is None else publication_at_ns,
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
        return UniverseRuntimeResultV2(universe, tiers, exploration)


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
            SubscriptionChannelV2.KLINE_1M,
            SubscriptionChannelV2.KLINE_5M,
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
