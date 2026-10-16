"""Prospective optional effort and verified event-rescreen contract mathematics.

No production clock/source scheduler. Mandatory observation and native clocks
are invariant; P4 executes accepted future requests using the existing owners.
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import ClassVar

from atlas.v2._serialization import sha256_json
from atlas.v2.science.mean_common import OfflineArtifact

CHEAP_REFRESH_NS = 10_000_000_000


@dataclass(frozen=True)
class EffortContractV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_PROSPECTIVE_EFFORT_CONTRACT_V1"
    requested: str
    effective: str
    changed_at_ns: int
    source_ref: str
    native_clock_refs: tuple[str, ...]
    mandatory_observation_ns: int = CHEAP_REFRESH_NS
    mandatory_outcomes: bool = True
    mandatory_safety: bool = True

    def __post_init__(self) -> None:
        super().__post_init__()
        order = ("LOW", "NORMAL", "HIGH")
        if (
            self.requested not in order
            or self.effective not in order
            or order.index(self.effective) > order.index(self.requested)
            or self.mandatory_observation_ns != CHEAP_REFRESH_NS
            or not self.mandatory_outcomes
            or not self.mandatory_safety
            or not self.native_clock_refs
        ):
            raise ValueError("optional effort cannot change mandatory work")

    @property
    def optional_refill_multiplier(self) -> Decimal:
        return {"LOW": Decimal(".25"), "NORMAL": Decimal("1"), "HIGH": Decimal("2")}[self.effective]


@dataclass(frozen=True)
class VerifiedRescreenV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_VERIFIED_FUTURE_RESCREEN_V1"
    event_ref: str
    source_ref: str
    asset_ref: str
    mapping_ref: str
    setup_episode_ref: str
    original_trigger_ns: int
    source_available_ns: int
    accepted_at_ns: int
    due_at_ns: int
    expires_at_ns: int
    event_valid_until_ns: int
    parent_job_ref: str
    verified: bool
    recursion_depth: int = 0

    def __post_init__(self) -> None:
        super().__post_init__()
        if (
            not self.verified
            or self.recursion_depth != 0
            or not self.original_trigger_ns
            <= self.source_available_ns
            <= self.accepted_at_ns
            < self.due_at_ns
            < self.expires_at_ns
            or self.due_at_ns < self.accepted_at_ns + CHEAP_REFRESH_NS
            or self.expires_at_ns > min(self.event_valid_until_ns, self.accepted_at_ns + 60_000_000_000)
            or self.accepted_at_ns - self.source_available_ns > 600_000_000_000
        ):
            raise ValueError("unverified/stale/recursive/backdated future rescreen")

    @property
    def dedupe_key(self) -> str:
        return sha256_json({"event": self.event_ref, "asset": self.asset_ref, "setup": self.setup_episode_ref})


def admit_rescreen(
    request: VerifiedRescreenV1,
    *,
    prior_keys: tuple[str, ...],
    asset_requests_last_hour: int,
    global_requests_last_hour: int,
    asset_last_accepted_ns: int | None,
) -> bool:
    return (
        request.dedupe_key not in prior_keys
        and 0 <= asset_requests_last_hour < 6
        and 0 <= global_requests_last_hour < 30
        and (asset_last_accepted_ns is None or request.accepted_at_ns - asset_last_accepted_ns >= 60_000_000_000)
    )
