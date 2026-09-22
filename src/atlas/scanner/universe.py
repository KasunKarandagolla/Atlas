"""Point-in-time universe snapshots; historical versions are immutable."""

from __future__ import annotations

from collections.abc import Sequence

from .models import EligibilityStatus, UniverseEntry, UniverseSnapshot


def build_universe_snapshot(*, snapshot_id: str, observed_at_ns: int, available_at_ns: int, venue: str,
                            version: str, entries: Sequence[UniverseEntry], source_ref: str,
                            effective_at_ns: int | None = None, ) -> UniverseSnapshot:
    """Freeze a universe as known at ``available_at_ns``.

    ``effective_at_ns`` is an optional common policy cutoff.  When supplied it
    can only tighten availability, never make a future snapshot usable early.
    """
    if available_at_ns < observed_at_ns:
        raise ValueError("universe snapshot availability precedes observation")
    if effective_at_ns is not None and effective_at_ns < available_at_ns:
        raise ValueError("universe effective time precedes availability")
    if not entries:
        raise ValueError("universe snapshot requires at least one instrument")
    ordered = tuple(sorted(entries, key=lambda entry: entry.instrument))
    for entry in ordered:
        if entry.observed_at_ns > observed_at_ns or entry.available_at_ns > available_at_ns:
            raise ValueError("universe entry is not available at the snapshot time")
    return UniverseSnapshot(snapshot_id, observed_at_ns, available_at_ns, venue, version, ordered, source_ref)


def eligible_instruments(snapshot: UniverseSnapshot) -> tuple[str, ...]:
    return tuple(entry.instrument for entry in snapshot.entries
                 if entry.eligibility_status is EligibilityStatus.ELIGIBLE)
