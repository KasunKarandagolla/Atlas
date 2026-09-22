"""Causal replay views."""

from __future__ import annotations

from .models import MarketRecord, ReplayMode


def available_for_decision(records: list[MarketRecord], decision_at_ns: int, mode: ReplayMode) -> list[MarketRecord]:
    out = []
    for r in records:
        t = r.available_at_ns if mode == ReplayMode.ACTUAL_SYSTEM else r.replay_available_at_ns
        if t is not None and t <= decision_at_ns:
            out.append(r)
    return sorted(out, key=lambda r: ((r.source_event_at_ns or 0), r.record_id))
