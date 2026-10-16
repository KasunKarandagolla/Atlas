from dataclasses import replace
from decimal import Decimal as D

import pytest

from atlas.v2._serialization import FrozenMap
from atlas.v2.risk import size_independent_candidate
from atlas.v2.science.action import freeze_independent_action
from atlas.v2.science.costs import value_mean_cash_path
from atlas.v2.science.mean_evaluation import independent_mean_features, index_mean_artifact
from atlas.v2.science.mean_scenarios import MeanMarketPointV1, replay_independent_action
from atlas.v2.science.outcomes import mean_training_row_from_cash
from atlas.v2.strategies.s2_breakout import S2_POLICY
from atlas.v2.strategies.s3_mean_reversion import S3_POLICY

from .conftest import H, T
from .test_mean_evaluation import run
from .test_mean_risk_cash import prepare


@pytest.mark.parametrize(
    "policy,setup",
    [
        (S2_POLICY, FrozenMap({"range_low": "99", "range_high": "100", "trigger_bar_close_ns": T})),
        (S3_POLICY, FrozenMap({"vwap_target": "101"})),
    ],
)
def test_registered_s2_failed_break_s3_target_and_missing_policy_evidence(case, policy, setup):
    inp = list(case.inputs.values())[0]
    candidate = replace(
        inp.candidate,
        envelope=replace(inp.candidate.envelope, content_hash=""),
        policy_hash=policy.policy_hash,
        horizon_end_ns=T + policy.max_hold_ns,
    )
    sizing = size_independent_candidate(
        candidate=candidate,
        product=inp.product,
        fee=inp.fees,
        snapshot=case.snapshot,
        profile=inp.risk,
        cutoff_ns=T,
        now_ns=T + 1,
    )
    args = {
        "candidate": candidate,
        "population_ref": case.population.content_hash,
        "sizing": sizing,
        "product": inp.product,
        "policy": policy,
        "permission_ref": case.snapshot.permission_ref,
        "funding_ref": inp.funding.content_hash,
        "execution_ref": inp.scenarios.execution_ref,
        "model_ref": inp.model.content_hash,
        "source_refs": case.population.candidates[0].source_refs,
    }
    with pytest.raises(ValueError):
        freeze_independent_action(**args)
    action = freeze_independent_action(**args, setup_parameters=setup)
    if policy is S2_POLICY:
        point = MeanMarketPointV1(
            T + 900_000_000_000, D("100"), D("99.99"), D("100.01"), D("100"), D("100"), H("closed-bar"), D("99.5")
        )
    else:
        point = MeanMarketPointV1(T + 100, D("101.1"), D("101.09"), D("101.11"), D("100"), D("100"), H("target-hit"))
    points = (inp.scenarios.paths[0].points[0], point, replace(point, at_ns=action.horizon_end_ns))
    scenarios = replace(inp.scenarios, paths=(replace(inp.scenarios.paths[0], points=points),))
    path = replay_independent_action(action, scenarios, quantity_step=inp.product.qty_step)[0]
    assert path.fills[-1].at_ns == point.at_ns < action.horizon_end_ns
    if policy is S2_POLICY:
        badpoints = (points[0], replace(point, closed_m15_price=None), points[-1])
        with pytest.raises(ValueError, match="CLOSED_BAR"):
            replay_independent_action(
                action,
                replace(scenarios, paths=(replace(scenarios.paths[0], points=badpoints),)),
                quantity_step=inp.product.qty_step,
            )


def test_new_outcome_producer_maturity_cash_and_execution_provenance(case):
    inp, _, action = prepare(case, 2)
    path = replay_independent_action(action, inp.scenarios, quantity_step=inp.product.qty_step)[0]
    features = independent_mean_features(action, origin_rank=2, context=inp.context, consumer_ns=T + 10)
    args = {
        "action": action,
        "path": path,
        "fee": inp.fees,
        "funding_schedule": inp.funding,
        "product": inp.product,
        "compatibility_ref": inp.context.compatibility_ref,
        "features": features,
        "source_grade": "QUALIFIED_MODELLED",
    }
    with pytest.raises(ValueError, match="IMMATURE"):
        mean_training_row_from_cash(**args, label_available_at_ns=T + 1)
    row = mean_training_row_from_cash(**args, label_available_at_ns=action.terminal_ns + 1)
    value = value_mean_cash_path(
        action=action,
        path=path,
        fee=inp.fees,
        funding_schedule=inp.funding,
        base_units_per_contract=inp.product.base_units_per_contract,
    )
    assert row.target_cash == value.trading_cash and row.provenance == "CAUSAL_REPLAY"
    assert row.execution_state == "FULL_FILL"
    assert not hasattr(row, "realized_risk_authority")


def test_derived_context_can_finish_after_prefix_but_new_raw_fact_cannot(case):
    ref = case.population.candidates[1].candidate_ref
    inp = case.inputs[ref]
    context = replace(inp.context, available_at_ns=T + 1, raw_source_available_at_ns=T)
    case.inputs[ref] = replace(inp, context=context)
    index_mean_artifact(case.repo, context, available_at_ns=T + 1)
    assert any(e.candidate_ref == ref for e in run(case).members)
    context = replace(context, raw_source_available_at_ns=T + 1)
    case.inputs[ref] = replace(inp, context=context)
    index_mean_artifact(case.repo, context, available_at_ns=T + 1)
    assert not any(e.candidate_ref == ref for e in run(case).members)


def test_unsupported_policy_and_original_horizon_refused(case):
    inp, _, _ = prepare(case)
    altered = replace(inp.policy, policy_hash="", version="unregistered")
    candidate = replace(
        inp.candidate, envelope=replace(inp.candidate.envelope, content_hash=""), policy_hash=altered.policy_hash
    )
    size = size_independent_candidate(
        candidate=candidate,
        product=inp.product,
        fee=inp.fees,
        snapshot=case.snapshot,
        profile=inp.risk,
        cutoff_ns=T,
        now_ns=T + 1,
    )
    with pytest.raises(ValueError, match="UNSUPPORTED"):
        freeze_independent_action(
            candidate=candidate,
            population_ref=case.population.content_hash,
            sizing=size,
            product=inp.product,
            policy=altered,
            permission_ref=case.snapshot.permission_ref,
            funding_ref=inp.funding.content_hash,
            execution_ref=inp.scenarios.execution_ref,
            model_ref=inp.model.content_hash,
            source_refs=case.population.candidates[0].source_refs,
        )
