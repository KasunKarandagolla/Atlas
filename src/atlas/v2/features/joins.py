"""Full-key, revision-bound as-of joins for confirmed 4H/1H/15M bars."""

from __future__ import annotations

from dataclasses import dataclass

from atlas.v2.data.bars import BarIntervalV2, CausalBarStoreV2, CausalBarV2
from atlas.v2.data.health import PublicSourceHealthV2
from atlas.v2.data.raw import AvailabilityClassV2
from atlas.v2.instruments import InstrumentKeyV2


@dataclass(frozen=True)
class JoinedBars:
    key: InstrumentKeyV2
    cutoff_ns: int
    h4: tuple[CausalBarV2, ...]
    h1: tuple[CausalBarV2, ...]
    m15: tuple[CausalBarV2, ...]
    status: str
    reason: str | None
    source_health_ref: str | None


def asof_join(store: CausalBarStoreV2, key: InstrumentKeyV2, *, cutoff_ns: int,
              trigger_ref: str | None = None,
              view: AvailabilityClassV2 = AvailabilityClassV2.ACTUAL_SYSTEM,
              source_health: PublicSourceHealthV2 | None = None) -> JoinedBars:
    def select(interval: BarIntervalV2) -> tuple[CausalBarV2, ...]:
        return tuple(bar for bar in store.as_of(key.content_hash, interval, information_cutoff_ns=cutoff_ns, availability_class=view)
                     if bar.final and bar.close_at_ns <= cutoff_ns)

    h4, h1, m15 = select(BarIntervalV2.H4), select(BarIntervalV2.H1), select(BarIntervalV2.M15)
    health_ref = source_health.content_hash if source_health is not None and source_health.available_at_ns <= cutoff_ns else None
    if trigger_ref is not None and (not m15 or m15[-1].content_hash != trigger_ref):
        return JoinedBars(key, cutoff_ns, h4, h1, m15, "NOT_ESTIMABLE", "TRIGGER_NOT_CONFIRMED_AT_CUTOFF", health_ref)
    if source_health is None or health_ref is None:
        return JoinedBars(key, cutoff_ns, h4, h1, m15, "NOT_ESTIMABLE", "SOURCE_HEALTH_UNKNOWN", health_ref)
    if any(bar.raw.source_id != source_health.source_id for bar in h4 + h1 + m15):
        return JoinedBars(key, cutoff_ns, h4, h1, m15, "NOT_ESTIMABLE", "SOURCE_HEALTH_SOURCE_MISMATCH", health_ref)
    if not source_health.data_eligible:
        return JoinedBars(key, cutoff_ns, h4, h1, m15, "NOT_ESTIMABLE", "SOURCE_HEALTH_" + source_health.state.value, health_ref)
    missing = [name for name, bars in (("4H", h4), ("1H", h1), ("15M", m15)) if not bars]
    return JoinedBars(key, cutoff_ns, h4, h1, m15, "AVAILABLE" if not missing else "NOT_ESTIMABLE",
                      "MISSING_" + "_".join(missing) if missing else None, health_ref)
