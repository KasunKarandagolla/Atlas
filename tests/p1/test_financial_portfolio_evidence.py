"""Common-background qualification reuses the existing portfolio evidence owner."""

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2.risk import SnapshotExposureV1
from atlas.v2.science.mean_evaluation import index_mean_artifact, read_mean_artifact
from atlas.v2.science.mean_scenario_evidence import (
    ScenarioObservationV1,
    ScenarioProbabilityCalibrationV1,
    ScenarioSourceV1,
    qualify_scenarios,
    verify_scenario_evidence,
)
from atlas.v2.science.portfolio import ExistingPortfolioPathV2, PortfolioPathComponentV2, index_existing_portfolio_path

from .conftest import T


@pytest.mark.parametrize("fault", [None, "missing_path", "wrong_path", "wrong_payoff", "invented_profit"])
def test_common_background_existing_owner_full_coverage_and_identity(case, fault):
    inp = list(case.inputs.values())[1]
    source = read_mean_artifact(case.repo, inp.scenarios.source_ref, ScenarioSourceV1, consumer_ns=T)
    calibration = read_mean_artifact(
        case.repo, inp.scenarios.calibration_ref, ScenarioProbabilityCalibrationV1, consumer_ns=T
    )
    observation = read_mean_artifact(case.repo, calibration.training_refs[0], ScenarioObservationV1, consumer_ns=T)
    exposure = SnapshotExposureV1(
        "synthetic-position",
        inp.product.key.content_hash,
        "ACTUAL",
        Decimal("1"),
        Decimal("1"),
        Decimal("100"),
        Decimal("50"),
        Decimal("100"),
        True,
        Decimal("50"),
        (Decimal("-10"),),
        observation.content_hash,
    )
    snapshot = replace(case.snapshot, exposures=(exposure,))
    index_mean_artifact(case.repo, snapshot, available_at_ns=snapshot.available_at_ns)
    path_id = inp.scenarios.paths[0].common_path_id if fault != "wrong_path" else source.scope_ref
    component = PortfolioPathComponentV2(
        "OPEN", Decimal("1") if fault in ("wrong_payoff", "invented_profit") else Decimal("0"), observation.content_hash
    )
    background = ExistingPortfolioPathV2(path_id, source.scope_ref, T, T - 4, snapshot.content_hash, (component,))
    index_existing_portfolio_path(case.repo, background)
    source = replace(
        source,
        account_snapshot_ref=snapshot.content_hash,
        background_path_refs=() if fault == "missing_path" else (background.content_hash,),
    )
    index_mean_artifact(case.repo, source, available_at_ns=source.available_at_ns)
    calibration = replace(calibration, source_ref=source.content_hash)
    index_mean_artifact(case.repo, calibration, available_at_ns=calibration.available_at_ns)
    scenarios = replace(
        inp.scenarios,
        source_ref=source.content_hash,
        calibration_ref=calibration.content_hash,
        account_snapshot_ref=snapshot.content_hash,
    )
    if fault == "invented_profit":
        source = replace(
            source, templates=tuple(replace(t, background_pnl_cash=Decimal("1")) for t in source.templates)
        )
        index_mean_artifact(case.repo, source, available_at_ns=source.available_at_ns)
        calibration = replace(calibration, source_ref=source.content_hash)
        index_mean_artifact(case.repo, calibration, available_at_ns=calibration.available_at_ns)
        scenarios = replace(
            scenarios,
            source_ref=source.content_hash,
            calibration_ref=calibration.content_hash,
            paths=tuple(replace(p, background_pnl_cash=Decimal("1")) for p in scenarios.paths),
        )
    if fault:
        with pytest.raises(ValueError, match="BACKGROUND"):
            qualify_scenarios(
                case.repo,
                scenarios,
                policy=case.policy,
                snapshot=snapshot,
                scenario_es_cash=Decimal("0"),
                available_at_ns=T - 1,
                expires_at_ns=T + 1000,
            )
        return
    scenarios = qualify_scenarios(
        case.repo,
        scenarios,
        policy=case.policy,
        snapshot=snapshot,
        scenario_es_cash=Decimal("0"),
        available_at_ns=T - 1,
        expires_at_ns=T + 1000,
    )
    assert (
        verify_scenario_evidence(
            case.repo,
            scenarios,
            product_ref=inp.product.content_hash,
            policy=case.policy,
            snapshot=snapshot,
            compatibility_ref=inp.model.compatibility_ref,
            cutoff_ns=T,
            now_ns=T + 10,
        ).tail_observations
        == 5
    )


@pytest.mark.parametrize("fault", ["missing_training", "future_training"])
def test_serving_model_training_membership_is_verified_separately_from_fold_models(case, fault):
    from atlas.v2.science.mean_calibration import (
        MeanOutcomeEvidenceV1,
        calibrate_verified_selected_policy,
        verify_mean_calibration,
    )

    from .conftest import H
    from .test_financial_evidence import a_chain, read

    inp, proof, protocol, residuals = a_chain(case)
    if fault == "missing_training":
        ref = H("invented-serving-training-outcome")
    else:
        evidence = read(case, inp.model.training_refs[-1], MeanOutcomeEvidenceV1)
        evidence = replace(evidence, available_at_ns=T + 1)
        index_mean_artifact(case.repo, evidence, available_at_ns=evidence.available_at_ns)
        ref = evidence.content_hash
    model = replace(inp.model, training_refs=inp.model.training_refs + (ref,))
    index_mean_artifact(case.repo, model, available_at_ns=model.available_at_ns)
    proof = replace(proof, serving_model_ref=model.content_hash)
    index_mean_artifact(case.repo, proof, available_at_ns=proof.available_at_ns)
    calibration = replace(inp.calibration, model_ref=model.content_hash, qualification_ref=proof.content_hash)
    index_mean_artifact(case.repo, calibration, available_at_ns=calibration.available_at_ns)
    with pytest.raises(ValueError):
        verify_mean_calibration(
            case.repo,
            calibration,
            model=model,
            policy_ref=case.policy.content_hash,
            product_ref=inp.product.content_hash,
            cutoff_ns=T,
            now_ns=T + 10,
        )
    with pytest.raises(ValueError):
        calibrate_verified_selected_policy(
            case.repo,
            model=model,
            protocol=protocol,
            residuals=residuals,
            cutoff_ns=inp.calibration.available_at_ns,
            expires_at_ns=inp.calibration.expires_at_ns,
        )
