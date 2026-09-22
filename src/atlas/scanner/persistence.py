"""Append-only Phase-5 research artifact families."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

from atlas.science.research_archive import ResearchArtifactArchive

UNIVERSE_SNAPSHOT = "universe_snapshot"
CHEAP_SCAN = "cheap_scan"
SCANNER_RANK = "scanner_rank"
SCANNER_SELECTION = "scanner_selection"
EXPLORATION_SELECTION = "exploration_selection"
WARMUP_STATUS = "warmup_status"
SCANNER_DECISION_CALENDAR = "scanner_decision_calendar"
SCANNER_HEALTH = "scanner_health"
SCANNER_ALERT = "scanner_alert"
BLINDSPOT_METRICS = "blindspot_metrics"
SCANNER_REVISION_COMPARISON = "scanner_revision_comparison"


def persist_scanner_artifacts(archive: ResearchArtifactArchive, *, universe: Any,
                              cheap_observations: Iterable[Any], ranked: Iterable[Any],
                              selections: Iterable[Any], exploration: Any, warmups: Iterable[Any],
                              health: Any, alerts: Iterable[Any], blindspots: Any,
                              ) -> dict[str, tuple[Path, ...]]:
    """Persist one scan slot using the existing content-addressed archive."""
    written: dict[str, tuple[Path, ...]] = {}

    def record(artifact_type: str, values: Iterable[Any]) -> None:
        paths = tuple(archive.append(artifact_type, value) for value in values)
        if paths:
            written[artifact_type] = paths

    record(UNIVERSE_SNAPSHOT, (universe,))
    record(CHEAP_SCAN, cheap_observations)
    record(SCANNER_RANK, ranked)
    record(SCANNER_SELECTION, selections)
    record(EXPLORATION_SELECTION, (exploration,))
    record(WARMUP_STATUS, warmups)
    record(SCANNER_HEALTH, (health,))
    record(SCANNER_ALERT, alerts)
    record(BLINDSPOT_METRICS, (blindspots,))
    return written


def persist_scanner_revision_comparison(archive: ResearchArtifactArchive, comparison: Any) -> Path:
    """Persist a paired full-calendar revision comparison in the same archive."""
    return archive.append(SCANNER_REVISION_COMPARISON, comparison)
