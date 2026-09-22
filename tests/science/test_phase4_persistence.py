"""§16: immutable Parquet research persistence for every Phase-4 artifact kind."""

from __future__ import annotations

import pytest
from support.phase4_factory import SLOT, bootstrap, complete_stress_input, decision_input

from atlas.science.decision_calendar import record_from_evaluation
from atlas.science.phase4_engine import evaluate_phase4
from atlas.science.phase4_persistence import (
    BLOCK_SELECTION,
    BOOTSTRAP_RESULT,
    DECISION_CALENDAR,
    EVALUATION,
    EXPERIMENT_VARIANT,
    JOINT_RESIDUAL_BLOCK,
    MATURED_RESIDUAL,
    OOF_FORECAST,
    SCENARIO_CONFIG,
    STRESS_RESULT,
    TRADE_PLAN_EVIDENCE,
    WEEKLY_FIT,
    ExperimentVariant,
    Phase4ArtifactSet,
    persist_phase4_artifacts,
)
from atlas.science.research_archive import ResearchArtifactArchive
from atlas.science.stresses import evaluate_stress_suite


def artifact_set() -> Phase4ArtifactSet:
    evaluation = evaluate_phase4(decision_input())
    stress = evaluate_stress_suite(complete_stress_input())
    record = record_from_evaluation(slot_id="slot-1", strategy_version="1.0", availability_cutoff_ns=SLOT + 1,
                                    evaluation=evaluation, slot_at_ns=SLOT, instrument="BTCUSDT",
                                    feature_snapshot_hash="snap", signal="LONG")
    return Phase4ArtifactSet(
        experiment_id="exp-1",
        weekly_fit={"selected_ridge": 0.1, "fit_at_ns": SLOT},
        oof_forecasts=({"instrument": "BTCUSDT", "forecast": 0.1},),
        matured_residuals=({"instrument": "BTCUSDT", "residual": -0.02},),
        joint_residual_blocks=({"at_ns": SLOT, "btc_residual": -0.02},),
        block_selection={"selected_block": 72, "scores": {24: 0.3, 48: 0.2, 72: 0.1}},
        scenario_configuration={"paths": 8, "seed": 1, "support": "RECONSTRUCTED_MARKET"},
        bootstrap=bootstrap(),
        stress_results=tuple({"case": result.case.name.value, "status": result.status.value} for result in stress),
        evaluation=evaluation,
        trade_plan_evidence=evaluation.evidence if evaluation.trade_plan else None,
        decision_calendar_record=record,
        variants=(ExperimentVariant("exp-1", "FAILED", "ridge selection refused", ("insufficient OOF support",),
                                    {"ridge": 10.0}),),
    )


def test_every_phase4_artifact_kind_is_persisted_append_only(tmp_path) -> None:
    archive = ResearchArtifactArchive(tmp_path)
    written = persist_phase4_artifacts(archive, artifact_set())
    assert set(written) >= {WEEKLY_FIT, OOF_FORECAST, MATURED_RESIDUAL, JOINT_RESIDUAL_BLOCK, BLOCK_SELECTION,
                            SCENARIO_CONFIG, BOOTSTRAP_RESULT, STRESS_RESULT, EVALUATION, TRADE_PLAN_EVIDENCE,
                            DECISION_CALENDAR, EXPERIMENT_VARIANT}
    for kind, paths in written.items():
        assert paths and all(path.exists() for path in paths), kind
        assert all(path.parent.name == kind for path in paths)
    first = written[EVALUATION][0]
    before = sorted(path.name for path in (tmp_path / EVALUATION).glob("*.parquet"))
    persist_phase4_artifacts(archive, artifact_set())
    after = sorted(path.name for path in (tmp_path / EVALUATION).glob("*.parquet"))
    assert before == after
    assert written[EVALUATION][0] == first


def test_failed_and_inconclusive_variants_are_never_overwritten(tmp_path) -> None:
    archive = ResearchArtifactArchive(tmp_path)
    failed = ExperimentVariant("exp-2", "FAILED", "invented venue mechanics refused", ("missing margin tier",),
                               {"stress": "MAINTENANCE_MARGIN_TIER"})
    inconclusive = ExperimentVariant("exp-2", "INCONCLUSIVE", "seed instability", ("NO_TRADE_NUMERICAL",), {})
    first = persist_phase4_artifacts(archive, Phase4ArtifactSet("exp-2", variants=(failed,)))[EXPERIMENT_VARIANT]
    second = persist_phase4_artifacts(archive, Phase4ArtifactSet("exp-2", variants=(inconclusive,)))[EXPERIMENT_VARIANT]
    assert first != second
    assert len(list((tmp_path / EXPERIMENT_VARIANT).glob("*.parquet"))) == 2
    with pytest.raises(ValueError, match="COMPLETED/INCONCLUSIVE/FAILED"):
        ExperimentVariant("exp-2", "UNKNOWN", "bad status")


def test_research_archive_rejects_unsafe_artifact_types(tmp_path) -> None:
    archive = ResearchArtifactArchive(tmp_path)
    for bad in ("", "../escape", "a/b"):
        try:
            archive.append(bad, {"x": 1})
        except ValueError:
            continue
        raise AssertionError(f"unsafe artifact type accepted: {bad!r}")
