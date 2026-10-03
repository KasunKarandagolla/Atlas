"""Operational work ceilings; never choose a smaller training population.

Retained evidence and the accepted estimator policies are unchanged. A fit
whose complete input exceeds these ceilings is refused and leaves a structured
pressure receipt, rather than silently fitting a newest-page subset.
"""

from __future__ import annotations

import time
from collections.abc import Sequence
from typing import Any

from atlas.v2._serialization import sha256_json, timestamp
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository

ACTIVE_TRAINING_BOUNDS_VERSION = "ActiveTrainingWorkBoundsV1"
RAW_ENTRY_LIMIT = 4096
FIT_ROW_LIMIT = 512
PRESSURE_VERSION = "ActiveTrainingWorkPressureV1"


class TrainingPressureError(ValueError):
    """The entire operation was refused; ``pressure`` describes its bound."""

    def __init__(self, pressure: dict[str, Any], receipt_ref: str | None = None) -> None:
        self.pressure = pressure
        self.receipt_ref = receipt_ref
        super().__init__(f"{pressure['reason']}: complete training operation exceeds or cannot verify its bounded input")


def _refuse(repo: OpsRepository | None, *, artifact_type: str, cutoff_ns: int | None,
            reason: str, limit: int, observed_count: int, invalid_rows: int = 0) -> None:
    body = {"version": PRESSURE_VERSION, "bounds_version": ACTIVE_TRAINING_BOUNDS_VERSION,
        "artifact_type": artifact_type, "information_cutoff_ns": cutoff_ns,
        "reason": reason, "limit": limit, "observed_count": observed_count,
        "observed_count_is_lower_bound": reason == "RAW_POPULATION_OVERFLOW",
        "invalid_rows": invalid_rows, "status": "NOT_ESTIMABLE",
        "population_selection": "WHOLE_OPERATION_REFUSED_NO_SUBSET", "authority": "ZERO"}
    ref = sha256_json(body)
    if repo is not None and repo.get_artifact(ref) is None:
        published = time.time_ns()
        repo.register_artifact(ArtifactIndexEntryV2(ref, PRESSURE_VERSION, ref, published, published,
            {"pressure": body}))
    raise TrainingPressureError(body, ref if repo is not None else None)


def bounded_artifact_entries(repo: OpsRepository, artifact_type: str, cutoff_ns: int
        ) -> tuple[ArtifactIndexEntryV2, ...]:
    cutoff = timestamp(cutoff_ns, field="active training cutoff")
    page = repo.latest_artifact_entries(artifact_type, as_of_ns=cutoff, limit=RAW_ENTRY_LIMIT)
    if page.has_more or page.invalid_entry_count:
        _refuse(repo, artifact_type=artifact_type, cutoff_ns=cutoff,
            reason="RAW_POPULATION_OVERFLOW" if page.has_more else "INVALID_INDEX_ROWS",
            limit=RAW_ENTRY_LIMIT, observed_count=len(page.entries) + page.invalid_entry_count + int(page.has_more),
            invalid_rows=page.invalid_entry_count)
    return page.entries


def bounded_training_entries(repo: OpsRepository, cutoff_ns: int) -> tuple[ArtifactIndexEntryV2, ...]:
    return bounded_artifact_entries(repo, "MaturedOutcomeV2", cutoff_ns)


def require_row_budget(repo: OpsRepository | None, rows: Sequence[Any], *, cutoff_ns: int | None = None) -> None:
    if cutoff_ns is not None:
        timestamp(cutoff_ns, field="active fitting cutoff")
    if len(rows) > FIT_ROW_LIMIT:
        _refuse(repo, artifact_type="ActionValueTrainingRows", cutoff_ns=cutoff_ns,
            reason="FIT_POPULATION_OVERFLOW", limit=FIT_ROW_LIMIT, observed_count=len(rows))
