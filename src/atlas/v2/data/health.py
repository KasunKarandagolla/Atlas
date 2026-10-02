"""Typed append-only public-source health observations."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .._serialization import nonblank, sha256_json, strict_fields, timestamp
from ..memory.repository import SourceHealthV2 as OpsSourceHealthV2


class PublicSourceStateV2(StrEnum):
    HEALTHY_CURRENT = "HEALTHY_CURRENT"
    STALE = "STALE"
    DISCONNECTED = "DISCONNECTED"
    RECONNECTING = "RECONNECTING"
    INCOMPLETE_SNAPSHOT = "INCOMPLETE_SNAPSHOT"
    SEQUENCE_GAP_CONFLICT = "SEQUENCE_GAP_CONFLICT"
    DEGRADED_RATE_LIMITED = "DEGRADED_RATE_LIMITED"


@dataclass(frozen=True)
class PublicSourceHealthV2:
    source_id: str
    observed_at_ns: int
    available_at_ns: int
    state: PublicSourceStateV2
    transition_id: str
    details: str

    SCHEMA_VERSION = 1

    def __post_init__(self) -> None:
        nonblank(self.source_id, field="source_id")
        timestamp(self.observed_at_ns, field="observed_at_ns")
        timestamp(self.available_at_ns, field="available_at_ns")
        if self.available_at_ns < self.observed_at_ns:
            raise ValueError("source health cannot be available before it is observed")
        try:
            object.__setattr__(self, "state", PublicSourceStateV2(self.state))
        except (ValueError, TypeError) as exc:
            raise ValueError("unknown public source health state") from exc
        nonblank(self.transition_id, field="transition_id")
        nonblank(self.details, field="details")

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @property
    def data_eligible(self) -> bool:
        return self.state == PublicSourceStateV2.HEALTHY_CURRENT

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.SCHEMA_VERSION,
            "source_id": self.source_id,
            "observed_at_ns": self.observed_at_ns,
            "available_at_ns": self.available_at_ns,
            "state": self.state.value,
            "transition_id": self.transition_id,
            "details": self.details,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PublicSourceHealthV2:
        fields = {"schema_version", "source_id", "observed_at_ns", "available_at_ns", "state", "transition_id", "details"}
        value = strict_fields(data, expected=fields, required=fields, name=cls.__name__)
        if type(value["schema_version"]) is not int or value["schema_version"] != cls.SCHEMA_VERSION:
            raise ValueError("unsupported PublicSourceHealthV2 schema_version")
        return cls(
            value["source_id"], value["observed_at_ns"], value["available_at_ns"],
            PublicSourceStateV2(value["state"]), value["transition_id"], value["details"],
        )

    def to_ops_record(self) -> OpsSourceHealthV2:
        return OpsSourceHealthV2(self.source_id, self.observed_at_ns, self.available_at_ns, self.state.value, self.content_hash)


class SourceHealthTrackerV2:
    """Bounded recent projection; the repository retains authoritative history."""

    def __init__(self, *, max_history_per_source: int = 2048) -> None:
        if type(max_history_per_source) is not int or max_history_per_source < 1:
            raise ValueError("source health cache bound must be positive")
        self.max_history_per_source = max_history_per_source
        self._history: dict[str, list[PublicSourceHealthV2]] = {}
        self._healthy_seen: set[str] = set()
        self._unhealthy_after_healthy: set[str] = set()

    def seed_prior_gap(self, source_id: str) -> None:
        self._healthy_seen.add(source_id)
        self._unhealthy_after_healthy.add(source_id)

    def had_unhealthy_after_healthy(self, source_id: str) -> bool:
        return source_id in self._unhealthy_after_healthy

    def append(self, observation: PublicSourceHealthV2) -> bool:
        history = self._history.setdefault(observation.source_id, [])
        if history and observation.observed_at_ns < history[-1].observed_at_ns:
            raise ValueError("source health observations must be chronological")
        if history and observation.observed_at_ns == history[-1].observed_at_ns:
            if observation.content_hash == history[-1].content_hash:
                return False
            raise ValueError("conflicting source health at the same observation time")
        history.append(observation)
        if observation.data_eligible:
            self._healthy_seen.add(observation.source_id)
        elif observation.source_id in self._healthy_seen:
            self._unhealthy_after_healthy.add(observation.source_id)
        if len(history) > self.max_history_per_source:
            del history[0]
        return True

    def latest(self, source_id: str) -> PublicSourceHealthV2 | None:
        history = self._history.get(source_id, [])
        return history[-1] if history else None

    def history(self, source_id: str) -> tuple[PublicSourceHealthV2, ...]:
        return tuple(self._history.get(source_id, ()))
