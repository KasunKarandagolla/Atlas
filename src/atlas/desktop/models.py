"""Headless view formatting helpers; these never recompute strategy evidence."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from atlas.v2.desktop.projection import DesktopSnapshotV2


def display_time(value_ns: int | None) -> str:
    if value_ns is None:
        return "UNAVAILABLE"
    return datetime.fromtimestamp(value_ns / 1_000_000_000, tz=UTC).isoformat(timespec="seconds")


def current_freshness(snapshot: DesktopSnapshotV2, *, now_ns: int) -> str:
    return "CURRENT" if now_ns <= snapshot.valid_until_ns else "STALE"


def scanner_display(row: Any) -> tuple[str, ...]:
    return (
        row.venue, row.product, row.symbol, row.eligibility, row.observed_state,
        f"{row.strategy_id or 'UNAVAILABLE'} / {row.policy_hash or 'UNAVAILABLE'}",
        str(row.selection_rank) if row.selection_rank is not None else "UNRANKED",
        row.selection_state, row.watch_state, row.sizing_status, row.evaluation_decision,
        ", ".join(row.reason_codes) or "NONE", display_time(row.expiry_ns),
        "COMPLETE" if row.evidence_complete else "INCOMPLETE",
        "SYNTHETIC FIXTURE" if row.synthetic_fixture else "REAL EVIDENCE",
    )
