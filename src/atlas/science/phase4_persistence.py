"""Append-only typed research persistence for every Phase-4 artifact kind.

DuckDB/Parquet stay research-only: nothing here writes authoritative live
SQLite state, and no failed/inconclusive experiment variant is overwritten.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .research_archive import ResearchArtifactArchive

WEEKLY_FIT = "weekly_fit_manifest"
OOF_FORECAST = "oof_forecast"
MATURED_RESIDUAL = "matured_residual"
JOINT_RESIDUAL_BLOCK = "joint_residual_block"
BLOCK_SELECTION = "block_selection_result"
SCENARIO_CONFIG = "scenario_configuration"
BOOTSTRAP_RESULT = "bootstrap_result"
STRESS_RESULT = "stress_result"
EVALUATION = "b0_a0_evaluation"
TRADE_PLAN_EVIDENCE = "trade_plan_evaluation_evidence"
DECISION_CALENDAR = "decision_calendar_record"
EXPERIMENT_VARIANT = "experiment_variant"

EXPERIMENT_STATUSES = ("COMPLETED", "INCONCLUSIVE", "FAILED")


@dataclass(frozen=True)
class ExperimentVariant:
    """A refused/failed/inconclusive variant retained for later review."""

    experiment_id: str
    status: str
    reason: str
    not_estimable_reasons: tuple[str, ...] = ()
    parameters: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.status not in EXPERIMENT_STATUSES:
            raise ValueError("experiment status must be COMPLETED/INCONCLUSIVE/FAILED")


@dataclass(frozen=True)
class Phase4ArtifactSet:
    experiment_id: str
    weekly_fit: Any | None = None
    oof_forecasts: tuple[Any, ...] = ()
    matured_residuals: tuple[Any, ...] = ()
    joint_residual_blocks: tuple[Any, ...] = ()
    block_selection: Any | None = None
    scenario_configuration: Any | None = None
    bootstrap: Any | None = None
    stress_results: tuple[Any, ...] = ()
    evaluation: Any | None = None
    trade_plan_evidence: Any | None = None
    decision_calendar_record: Any | None = None
    variants: tuple[ExperimentVariant, ...] = ()


def persist_phase4_artifacts(archive: ResearchArtifactArchive, artifacts: Phase4ArtifactSet) -> dict[str, tuple[Path, ...]]:
    """Write every supplied artifact under its content hash; never overwrite."""
    written: dict[str, tuple[Path, ...]] = {}

    def record(artifact_type: str, values: Iterable[Any]) -> None:
        paths = tuple(archive.append(artifact_type, value) for value in values)
        if paths:
            written[artifact_type] = paths

    if artifacts.weekly_fit is not None:
        record(WEEKLY_FIT, (artifacts.weekly_fit,))
    record(OOF_FORECAST, artifacts.oof_forecasts)
    record(MATURED_RESIDUAL, artifacts.matured_residuals)
    record(JOINT_RESIDUAL_BLOCK, artifacts.joint_residual_blocks)
    if artifacts.block_selection is not None:
        record(BLOCK_SELECTION, (artifacts.block_selection,))
    if artifacts.scenario_configuration is not None:
        record(SCENARIO_CONFIG, (artifacts.scenario_configuration,))
    if artifacts.bootstrap is not None:
        record(BOOTSTRAP_RESULT, (artifacts.bootstrap,))
    record(STRESS_RESULT, artifacts.stress_results)
    if artifacts.evaluation is not None:
        record(EVALUATION, (artifacts.evaluation,))
    if artifacts.trade_plan_evidence is not None:
        record(TRADE_PLAN_EVIDENCE, (artifacts.trade_plan_evidence,))
    if artifacts.decision_calendar_record is not None:
        record(DECISION_CALENDAR, (artifacts.decision_calendar_record,))
    record(EXPERIMENT_VARIANT, artifacts.variants)
    return written


def experiment_id(archive: ResearchArtifactArchive, value: str) -> Path:
    """Content-addressed experiment pointer; append-only like every artifact."""
    return archive.append(EXPERIMENT_VARIANT, ExperimentVariant(value, "COMPLETED", "pointer"))
