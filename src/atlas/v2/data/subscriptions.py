"""Deterministic tiered subscription plans with watch restoration."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from .._serialization import canonical_json, sha256_json, strict_fields, string_tuple, timestamp
from ..contracts import OpportunityWatchV2
from ..instruments import InstrumentKeyV2
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository, RestartSnapshotV2
from .universe import ComputeTierV2, SubscriptionChannelV2, subscription_channels_for_tier

_SUBSCRIPTION_SCHEMA_VERSION = 1

_WATCH_EVENT_CHANNELS = {
    "BAR_CLOSE_15M": SubscriptionChannelV2.KLINE_15M,
    "BAR_CLOSE_1H": SubscriptionChannelV2.KLINE_1H,
    "BAR_CLOSE_4H": SubscriptionChannelV2.KLINE_4H,
    "BOOK_TICKER": SubscriptionChannelV2.BOOK_TICKER,
    "TRADE": SubscriptionChannelV2.TRADES,
    "MARK_INDEX": SubscriptionChannelV2.MARK_INDEX,
    "FUNDING": SubscriptionChannelV2.FUNDING,
    "OPEN_INTEREST": SubscriptionChannelV2.OPEN_INTEREST,
}


@dataclass(frozen=True)
class SubscriptionSpecV2:
    key: InstrumentKeyV2
    tier: ComputeTierV2
    channels: tuple[SubscriptionChannelV2, ...]
    watch_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "tier", ComputeTierV2(self.tier))
        channels = tuple(SubscriptionChannelV2(channel) for channel in self.channels)
        if channels != tuple(sorted(set(channels), key=lambda item: item.value)):
            raise ValueError("subscription channels must be sorted and unique")
        object.__setattr__(self, "channels", channels)
        object.__setattr__(self, "watch_ids", string_tuple(self.watch_ids, field="watch_ids", sorted_unique=True))

    def to_dict(self) -> dict[str, object]:
        return {
            "key": self.key.to_dict(),
            "tier": int(self.tier),
            "channels": [channel.value for channel in self.channels],
            "watch_ids": list(self.watch_ids),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SubscriptionSpecV2:
        fields = {"key", "tier", "channels", "watch_ids"}
        value = strict_fields(data, expected=fields, required=fields, name=cls.__name__)
        if not isinstance(value["channels"], list) or not isinstance(value["watch_ids"], list):
            raise ValueError("subscription channels and watch_ids must be arrays")
        return cls(
            InstrumentKeyV2.from_dict(value["key"]),
            ComputeTierV2(value["tier"]),
            tuple(SubscriptionChannelV2(channel) for channel in value["channels"]),
            tuple(value["watch_ids"]),
        )


@dataclass(frozen=True)
class SubscriptionPlanV2:
    created_at_ns: int
    specs: tuple[SubscriptionSpecV2, ...]
    plan_id: str

    def __post_init__(self) -> None:
        timestamp(self.created_at_ns, field="created_at_ns")
        specs = tuple(self.specs)
        keys = tuple(item.key.to_canonical_json() for item in specs)
        if keys != tuple(sorted(set(keys))):
            raise ValueError("subscription plan specs must be sorted and unique")
        body = {"schema_version": _SUBSCRIPTION_SCHEMA_VERSION, "specs": [item.to_dict() for item in specs]}
        expected_id = sha256_json({"artifact_type": "SubscriptionPlanV2", "plan": body})
        if self.plan_id != expected_id:
            raise ValueError("subscription plan identity does not match its immutable specs")
        object.__setattr__(self, "specs", specs)

    @classmethod
    def build(cls, created_at_ns: int, specs: Iterable[SubscriptionSpecV2]) -> SubscriptionPlanV2:
        timestamp(created_at_ns, field="created_at_ns")
        ordered = tuple(sorted(specs, key=lambda item: item.key.to_canonical_json()))
        if len({item.key for item in ordered}) != len(ordered):
            raise ValueError("subscription plan must contain each full instrument identity once")
        body = {"schema_version": _SUBSCRIPTION_SCHEMA_VERSION, "specs": [item.to_dict() for item in ordered]}
        return cls(created_at_ns, ordered, sha256_json({"artifact_type": "SubscriptionPlanV2", "plan": body}))

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": _SUBSCRIPTION_SCHEMA_VERSION,
            "created_at_ns": self.created_at_ns,
            "specs": [item.to_dict() for item in self.specs],
            "plan_id": self.plan_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SubscriptionPlanV2:
        fields = {"schema_version", "created_at_ns", "specs", "plan_id"}
        value = strict_fields(data, expected=fields, required=fields, name=cls.__name__)
        if type(value["schema_version"]) is not int or value["schema_version"] != _SUBSCRIPTION_SCHEMA_VERSION:
            raise ValueError("unsupported SubscriptionPlanV2 schema_version")
        if not isinstance(value["specs"], list):
            raise ValueError("subscription specs must be an array")
        return cls(
            value["created_at_ns"],
            tuple(SubscriptionSpecV2.from_dict(item) for item in value["specs"]),
            value["plan_id"],
        )


def build_subscription_plan(
    tiers: Mapping[InstrumentKeyV2, ComputeTierV2],
    *,
    active_watches: Iterable[OpportunityWatchV2] = (),
    open_positions: Iterable[InstrumentKeyV2] = (),
    created_at_ns: int,
) -> SubscriptionPlanV2:
    watch_by_key: dict[InstrumentKeyV2, list[OpportunityWatchV2]] = {}
    for watch in active_watches:
        watch_by_key.setdefault(watch.key, []).append(watch)
    positions = set(open_positions)
    keys = set(tiers) | set(watch_by_key) | positions
    specs: list[SubscriptionSpecV2] = []
    for key in sorted(keys, key=lambda item: item.to_canonical_json()):
        tier = ComputeTierV2.TIER_3 if key in positions or key in watch_by_key else ComputeTierV2(tiers.get(key, ComputeTierV2.TIER_0))
        channels = set(subscription_channels_for_tier(tier))
        watches = sorted(watch_by_key.get(key, ()), key=lambda item: item.watch_id)
        for watch in watches:
            required = _WATCH_EVENT_CHANNELS.get(watch.required_next_event)
            if required is None:
                raise ValueError(f"active watch requires unsupported subscription event {watch.required_next_event!r}")
            channels.add(required)
        if key in positions:
            channels.update((SubscriptionChannelV2.BOOK_TICKER, SubscriptionChannelV2.MARK_INDEX))
        specs.append(
            SubscriptionSpecV2(
                key,
                tier,
                tuple(sorted(channels, key=lambda item: item.value)),
                tuple(watch.watch_id for watch in watches),
            )
        )
    return SubscriptionPlanV2.build(created_at_ns, specs)


def restore_subscription_plan(
    repository: OpsRepository,
    tiers: Mapping[InstrumentKeyV2, ComputeTierV2],
    *,
    now_ns: int,
    expiry_ids: Mapping[str, tuple[str, str]] | None = None,
    open_positions: Iterable[InstrumentKeyV2] = (),
) -> tuple[RestartSnapshotV2, SubscriptionPlanV2]:
    snapshot = repository.recover_active_watches(now_ns=now_ns, expiry_ids=expiry_ids)
    plan = build_subscription_plan(tiers, active_watches=snapshot.active_watches, open_positions=open_positions, created_at_ns=now_ns)
    existing = repository.get_artifact(plan.plan_id)
    if existing is None:
        repository.register_artifact(
            ArtifactIndexEntryV2(
                plan.plan_id, "SubscriptionPlanV2", plan.plan_id, now_ns, now_ns,
                {"specs": [item.to_dict() for item in plan.specs]},
            )
        )
    elif canonical_json(existing.metadata) != canonical_json({"specs": [item.to_dict() for item in plan.specs]}):
        raise ValueError("subscription plan identity conflicts with persisted content")
    return snapshot, plan
