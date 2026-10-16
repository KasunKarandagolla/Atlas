from dataclasses import replace
from decimal import Decimal as D

import pytest

from atlas.v2.contracts import CandidateSetV2, EligibleSetV1
from atlas.v2.science.evaluation_service import run_mean_action_evaluation as run_independent_mean_evaluation
from atlas.v2.science.mean_evaluation import index_mean_artifact, read_mean_artifact


def run(case, **kw):
    return run_independent_mean_evaluation(
        case.repo,
        population=case.population,
        snapshot=case.snapshot,
        policy=case.policy,
        inputs=case.inputs,
        clock_ns=case.clock,
        **kw,
    )


def test_a1_a2_a4_connected_independent_negative_first_fourth_and_missing(case, monkeypatch):
    monkeypatch.setattr(
        CandidateSetV2,
        "selected_candidate_id",
        property(lambda self: (_ for _ in ()).throw(AssertionError("selected-first"))),
        raising=False,
    )
    result = run(case)
    assert len(result.work) == 4
    assert [w.status for w in result.work] == ["EVALUATED", "EVALUATED", "UNAVAILABLE", "EVALUATED"]
    assert [e.mean_cash for e in result.evaluations] == [D("-2"), D("5"), D("9")]
    assert len(result.members) == 2
    assert result.members[-1].candidate_ref == case.population.candidates[3].candidate_ref
    assert result.evaluations[0].status == "INELIGIBLE"
    assert read_mean_artifact(case.repo, result.content_hash, EligibleSetV1, consumer_ns=case.clock()) == result
    for kind in ("Order", "Approval", "Reservation", "TradePlanEnvelopeV2"):
        assert case.repo.artifact_entries(kind) == ()


def test_population_duplicates_rejected_and_order_preserved(case):
    p = case.population
    with pytest.raises(ValueError, match="duplicate"):
        replace(p, candidates=(p.candidates[0], p.candidates[0]))
    changed = replace(p, candidates=p.candidates[::-1])
    assert changed.content_hash != p.content_hash
    case.population = changed
    index_mean_artifact(case.repo, changed, available_at_ns=p.cutoff_ns)
    result = run(case)
    assert tuple(w.candidate_ref for w in result.work) == tuple(c.candidate_ref for c in changed.candidates)


@pytest.mark.parametrize("budget", ["cpu", "deadline"])
def test_resource_and_deadline_population_integrity(case, budget):
    if budget == "cpu":
        case.population = replace(case.population, cpu_budget_ns=1)
    else:
        case.clock = lambda: case.population.deadline_ns
    index_mean_artifact(case.repo, case.population, available_at_ns=case.population.cutoff_ns)
    result = run(case)
    expected = "NOT_EVALUATED_RESOURCE" if budget == "cpu" else "NOT_EVALUATED_DEADLINE"
    assert all(w.status == expected for w in result.work)
    assert len(result.work) == 4 and not result.members


def test_hot_path_no_fit_search_or_framework_import(case, monkeypatch):
    import builtins

    import atlas.v2.science.m0 as old
    import atlas.v2.science.mean_models as new

    def forbidden(*a, **k):
        raise AssertionError("fit/heavy import in hot path")

    monkeypatch.setattr(old, "fit_m0", forbidden)
    for name in ("fit_m0_mean", "fit_m1_mean", "fit_empirical_mean"):
        monkeypatch.setattr(new, name, forbidden)
    original = builtins.__import__

    def guarded(name, *a, **k):
        if name.split(".")[0] in ("lightgbm", "numpy", "scipy", "torch"):
            forbidden()
        return original(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", guarded)
    assert len(run(case).members) == 2


def rebind_policy(case, policy):
    case.policy = policy
    case.population = replace(case.population, evaluation_policy_ref=policy.content_hash)
    index_mean_artifact(case.repo, policy, available_at_ns=case.population.cutoff_ns)
    index_mean_artifact(case.repo, case.population, available_at_ns=case.population.cutoff_ns)
    for ref, inp in tuple(case.inputs.items()):
        changes = {"calibration": replace(inp.calibration, policy_ref=policy.content_hash)}
        for name in ("execution_bias", "numerical", "incremental_programme"):
            changes[name] = replace(getattr(inp, name), policy_ref=policy.content_hash)
        case.inputs[ref] = replace(inp, **changes)
        for obj in changes.values():
            index_mean_artifact(
                case.repo, obj, available_at_ns=getattr(obj, "available_at_ns", case.population.cutoff_ns)
            )


def test_cash_allowances_are_distinct_and_charged_once(case):
    ref = case.population.candidates[1].candidate_ref
    inp = case.inputs[ref]
    changed = replace(
        inp,
        calibration=replace(inp.calibration, allowance_cash=D("1")),
        execution_bias=replace(inp.execution_bias, cash=D(".5")),
        numerical=replace(inp.numerical, cash=D(".25")),
        incremental_programme=replace(inp.incremental_programme, cash=D("1")),
    )
    case.inputs[ref] = changed
    for obj in (changed.calibration, changed.execution_bias, changed.numerical, changed.incremental_programme):
        index_mean_artifact(case.repo, obj, available_at_ns=obj.available_at_ns)
    evaluation = next(e for e in run(case).evaluations if e.candidate_ref == ref)
    assert evaluation.mean_cash == 5 and evaluation.score_cash == D("2.25")


def test_required_es_unsupported_refuses_then_common_cash_increment_works(case):
    rebind_policy(case, replace(case.policy, require_es=True, risk_lambda=D("1")))
    for ref, inp in tuple(case.inputs.items()):
        case.inputs[ref] = replace(inp, scenarios=replace(inp.scenarios, es_qualified=False))
        index_mean_artifact(case.repo, case.inputs[ref].scenarios, available_at_ns=case.population.cutoff_ns)
    result = run(case)
    assert not result.members and all("ES_NOT_ESTIMABLE" in e.reasons for e in result.evaluations)
    for ref, inp in tuple(case.inputs.items()):
        case.inputs[ref] = replace(inp, scenarios=replace(inp.scenarios, es_qualified=True))
        index_mean_artifact(case.repo, case.inputs[ref].scenarios, available_at_ns=case.population.cutoff_ns)
    result = run(case)
    assert result.members and all(e.risk_increment_cash < 0 for e in result.members)
    assert all(e.j_cash == e.score_cash - e.risk_increment_cash for e in result.members)


def test_actual_legacy_candidate_and_product_readers_are_consumed(case):
    for inp in case.inputs.values():
        assert "candidate" in case.repo.get_artifact(inp.candidate.content_hash).metadata
        assert "product" in case.repo.get_artifact(inp.product.content_hash).metadata
    assert len(run(case).members) == 2


def test_adaptive_proxy_exploration_and_measured_trace(case):
    from atlas.v2.science.mean_evaluation import evaluation_work_priority

    rows = case.population.candidates
    fourth = replace(rows[3], exploration=True)
    case.population = replace(case.population, candidates=(*rows[:3], fourth))
    index_mean_artifact(case.repo, case.population, available_at_ns=case.population.cutoff_ns)
    result = run(case)
    assert result.work[3].attempt_index == 3
    proxy = replace(fourth, proxy_cash=D("20"), proxy_support_ref=fourth.source_refs[0])
    assert evaluation_work_priority(proxy, best_cash=D("19"), cpu_budget_ns=100) < evaluation_work_priority(
        proxy, best_cash=D("5"), cpu_budget_ns=100
    )
    assert result.cpu_used_ns > 0 and result.wall_used_ns > 0
