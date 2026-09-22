from decimal import Decimal

from atlas.domain.risk import engineering_default_policy
from atlas.risk.drawdown import DrawdownEvidence, DrawdownState, next_drawdown_state
from atlas.science.funding import forecast_funding
from atlas.science.manifest import phase4_manifest
from atlas.science.research_archive import ResearchArtifactArchive
from atlas.science.uncertainty import block_bootstrap_indices, bootstrap_lcb


def test_manifest_binds_authority_relevant_values():
    common = {"selected_ridge": .1, "fit_at_ns": 1, "training_interval": (0, 1), "validation_folds": ((0, 1),),
              "selected_block": 24, "risk_policy_hash": "a", "availability_mode": "RECONSTRUCTED_MARKET"}
    one = phase4_manifest(seeds=(1,), **common)
    two = phase4_manifest(seeds=(2,), **common)
    assert one.hash() != two.hash()
    assert one.values["scenario_paths"] == 2048
    assert one.values["bootstrap_count"] == 200


def test_research_archive_is_append_only(tmp_path):
    archive = ResearchArtifactArchive(tmp_path)
    first = archive.append("weekly_fit", {"fit": 1, "failed": False})
    assert first == archive.append("weekly_fit", {"failed": False, "fit": 1})
    assert first.exists() and len(list(first.parent.glob("*.parquet"))) == 1


def test_funding_forecast_is_causal_and_bootstrap_rng_is_explicit():
    forecast = forecast_funding(next_settlement_at_ns=1, latest_predicted_rate=Decimal(".01"), latest_settled_rate=None,
                                historical_settlement_changes=(Decimal(".001"),), horizon_settlements=2)
    assert forecast.rates == (Decimal(".01"), Decimal(".011")) and forecast.downgrade is None
    assert block_bootstrap_indices(10, block_length=2, count=2, seed=4) == block_bootstrap_indices(10, block_length=2, count=2, seed=4)
    assert bootstrap_lcb([1.0] * 200)[0] == 1.0


def test_drawdown_hysteresis_requires_review_and_reconciliation():
    p = engineering_default_policy()
    stopped = next_drawdown_state(p, DrawdownState.NORMAL, Decimal(".10"))
    assert stopped is DrawdownState.STOPPED
    assert next_drawdown_state(p, stopped, Decimal(".07")) is DrawdownState.STOPPED
    assert next_drawdown_state(p, stopped, Decimal(".07"), DrawdownEvidence(True, True)) is DrawdownState.REDUCED
    assert next_drawdown_state(p, DrawdownState.REDUCED, Decimal(".039")) is DrawdownState.NORMAL
