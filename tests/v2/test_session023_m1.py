"""Actual LightGBM and adversarial action-value chronology regressions."""

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.science.m0 import action_features
from atlas.v2.science.m1 import (
    DAY_NS,
    HOUR_NS,
    M1_FEATURE_ORDER,
    M1_PARAMETER_GRID,
    M1_POLICY_HASH,
    M1TrainingRowV2,
    _transform,
    build_m1_training_rows,
    build_walk_forward_chronology,
    choose_parameters,
    chronological_oof,
    fit_lightgbm_fixture,
    fit_m1,
    predict_lightgbm_fixture,
    project_m0_features,
    walk_forward_oof,
)
from atlas.v2.strategies.s1_trend import S1_POLICY

from .session023_support import feature_candidate
from .test_session017_risk import CUTOFF
from .test_session018_remediation import _payoff_case


def training_row(index, *, days=1, feature=None):
    refs = {name: sha256_json([index, name]) for name in (
        "outcome", "action", "action_ref", "candidate", "set", "feature", "source", "execution")}
    decision = (index + 1) * days * DAY_NS
    values = (float(index if feature is None else feature),) + (0.0,) * (len(M1_FEATURE_ORDER) - 1)
    state = ("NO_FILL", "PARTIAL_FILL", "FULL_FILL")[index % 3]
    filled = {"NO_FILL": Decimal(0), "PARTIAL_FILL": Decimal("0.5"), "FULL_FILL": Decimal(1)}[state]
    gross = Decimal(index % 7) if filled else Decimal(0)
    return M1TrainingRowV2(refs["outcome"], refs["action"], refs["action_ref"], refs["candidate"],
        refs["set"], refs["feature"], refs["source"], sha256_json("policy"), sha256_json("compatibility"),
        "BYBIT", "LINEAR_PERPETUAL", decision, decision + HOUR_NS, decision + HOUR_NS + 1,
        values, (), gross, "SIMULATED", state, Decimal(1), filled, gross, Decimal(0), Decimal(0), refs["execution"])


def test_actual_lightgbm_deterministic_native_model_and_roundtrip():
    import lightgbm

    assert lightgbm.__version__ == "4.7.0"
    rows = tuple(training_row(i) for i in range(40))
    model, centers, scales = fit_lightgbm_fixture(rows)
    repeated, other_centers, other_scales = fit_lightgbm_fixture(rows)
    assert model == repeated and centers == other_centers and scales == other_scales
    assert "tree" in model.lower() and len(M1_PARAMETER_GRID) == 4
    value = predict_lightgbm_fixture(model, _transform(rows[-1].features, centers, scales))
    assert value == predict_lightgbm_fixture(repeated, _transform(rows[-1].features, centers, scales))
    assert len(M1_POLICY_HASH) == 64


def test_preprocessing_search_fits_only_training_and_records_exact_budget():
    train = tuple(training_row(i) for i in range(35))
    validation = tuple(training_row(40 + i, feature=1e12) for i in range(8))
    params, centers, _, results = choose_parameters(train, validation)
    assert centers[0] == 17.0 and params in M1_PARAMETER_GRID
    assert len(results) == 4 and all(status == "PASS" for _, status, _ in results)
    crossing = replace(train[-1], label_available_at_ns=validation[0].decision_at_ns + 1)
    with pytest.raises(ValueError, match="cross"):
        choose_parameters((*train[:-1], crossing), validation)


def test_all_failed_lightgbm_configurations_remain_in_search_ledger(monkeypatch):
    from atlas.v2.science import m1

    def failure(_config):
        raise ValueError("deterministic engineering failure")

    monkeypatch.setattr(m1, "_make_estimator", failure)
    with pytest.raises(m1.M1SearchFailure) as caught:
        choose_parameters(tuple(training_row(i) for i in range(35)), tuple(training_row(i) for i in range(40, 48)))
    assert len(caught.value.results) == len(M1_PARAMETER_GRID) == 4
    assert all(status == "FAILED:ValueError" for _, status, _ in caught.value.results)


def test_future_self_unmatured_and_overlap_cannot_train_or_rewrite_prior_oof():
    rows = tuple(training_row(i) for i in range(48))
    baseline = chronological_oof(rows)
    assert any(row.status == "OOF" for row in baseline)
    future = training_row(1000, feature=1e30)
    appended = chronological_oof((*rows, future))
    assert {row.outcome_ref: row for row in baseline} == {
        row.outcome_ref: row for row in appended if row.outcome_ref != future.outcome_ref}
    by_ref = {row.outcome_ref: row for row in rows}
    for predicted in baseline:
        assert predicted.outcome_ref not in predicted.training_row_refs
        assert all(by_ref[ref].label_available_at_ns < predicted.prediction_at_ns and
            by_ref[ref].horizon_end_ns <= predicted.prediction_at_ns - DAY_NS for ref in predicted.training_row_refs)
    with pytest.raises(TypeError):
        chronological_oof(rows, shuffle=True)
    with pytest.raises(ValueError, match="revised"):
        chronological_oof((*rows, replace(rows[0], features=(999.0,) + rows[0].features[1:])))
    with pytest.raises(ValueError, match="embargo"):
        chronological_oof(rows, embargo_ns=0)


def test_required_180_30_30_schedule_outer_models_and_untouched_holdout():
    rows = tuple(training_row(i) for i in range(350))
    chronology = build_walk_forward_chronology(rows, as_of_ns=351 * DAY_NS)
    assert chronology.status == "WALK_FORWARD_READY" and len(chronology.windows) == 3
    assert chronology.training_window_ns == 180 * DAY_NS
    assert chronology.validation_window_ns == chronology.outer_window_ns == chronology.advance_ns == 30 * DAY_NS
    oof, fits = walk_forward_oof(rows, chronology)
    assert len(fits) == 3 and all(fit["status"] == "TESTED" for fit in fits)
    assert chronology.holdout_state == "UNTOUCHED"
    assert not set(chronology.final_holdout_refs) & {row.outcome_ref for row in oof}
    by_ref = {row.outcome_ref: row for row in rows}
    assert all(by_ref[ref].label_available_at_ns <= row.training_cutoff_ns
        for row in oof for ref in row.training_row_refs)
    insufficient = build_walk_forward_chronology(rows[:10], as_of_ns=351 * DAY_NS)
    assert insufficient.status == "NOT_ESTIMABLE" and walk_forward_oof(rows[:10], insufficient)[0] == ()


@pytest.mark.parametrize("depth,state", [("0", "NO_FILL"), ("10", "PARTIAL_FILL"), ("100", "FULL_FILL")])
def test_honest_training_loader_preserves_fill_provenance_and_net_components(tmp_path, monkeypatch, depth, state):
    from . import test_session017_risk as risk_module

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        monkeypatch.setattr(risk_module, "candidate", lambda policy=risk_module.S1_POLICY, key=risk_module.KEY,
            **kwargs: feature_candidate(repo, policy, key, **kwargs))
        _, action, _, outcome = _payoff_case(repo, entry_depth=depth)
        from atlas.v2.science.outcomes import index_matured_outcome

        index_matured_outcome(repo, outcome)
        assert build_m1_training_rows(repo, cutoff_ns=outcome.available_at_ns - 1) == ()
        rows = build_m1_training_rows(repo, cutoff_ns=outcome.available_at_ns)
        assert len(rows) == 1 and rows[0].execution_state == state
        assert rows[0].action_hash == action.action.action_hash
        assert rows[0].target_net_value == rows[0].gross_payoff - rows[0].fees + rows[0].funding_cashflow
        assert rows[0].candidate_set_ref == outcome.candidate_set_ref
        assert build_m1_training_rows(repo, cutoff_ns=outcome.available_at_ns, exclude_action_hash=outcome.action_hash) == ()


def test_m1_exact_action_binding_no_history_and_changed_action_rejection(tmp_path):
    from atlas.v2.science.action import freeze_action

    from .session023_support import research_case
    from .test_session017_risk import size

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = research_case(repo)
        action = freeze_action(repo, candidate=case.candidate, candidate_set=case.candidate_set,
            sizing=size(repo, case), product=case.product, policy=S1_POLICY,
            v1=case.v1, v2=case.v2)
        result = fit_m1(repo, action=action, candidate=case.candidate, candidate_set=case.candidate_set,
            cutoff_ns=CUTOFF, available_at_ns=CUTOFF + 1, dependency_lock_hash=sha256_json("lock"))
        assert result.prediction.action_hash == action.action.action_hash
        assert result.prediction.status == "NOT_ESTIMABLE" and result.prediction.expected_net_value is None
        assert repo.get_artifact(result.prediction.content_hash) is not None
        vector = project_m0_features(action_features(repo, action.content_hash, cutoff_ns=CUTOFF),
            candidate_set_ref=case.candidate_set.content_hash)
        assert len(vector.values) == len(M1_FEATURE_ORDER)
        for field, value in (("quantity", action.action.quantity + 1), ("stop_price", Decimal("98")),
            ("horizon_end_ns", action.action.horizon_end_ns + HOUR_NS)):
            changed = replace(action, action=replace(action.action, **{field: value}))
            with pytest.raises(ValueError):
                fit_m1(repo, action=changed, candidate=case.candidate, candidate_set=case.candidate_set,
                    cutoff_ns=CUTOFF, available_at_ns=CUTOFF + 1, dependency_lock_hash=sha256_json("lock"))


def test_m1_final_holdout_never_rolls_into_later_training_or_resets_after_viewing(tmp_path):
    from atlas.v2.memory.repository import ArtifactIndexEntryV2
    from atlas.v2.science.discovery import DiscoveryExperimentV2, mark_holdout_spent, register_discovery_experiment
    from atlas.v2.science.m1 import reserve_m1_final_holdout

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        key = sha256_json("fixed-population")
        first = reserve_m1_final_holdout(repo, compatibility_key=key, cutoff_ns=400 * DAY_NS, available_at_ns=400 * DAY_NS + 1)
        later = reserve_m1_final_holdout(repo, compatibility_key=key, cutoff_ns=500 * DAY_NS, available_at_ns=500 * DAY_NS + 1)
        assert later == first and first[:2] == (370 * DAY_NS, 400 * DAY_NS)
        with pytest.raises(ValueError, match="future holdout"):
            reserve_m1_final_holdout(repo, compatibility_key=key, cutoff_ns=350 * DAY_NS, available_at_ns=350 * DAY_NS + 1)
        baseline = sha256_json("baseline")
        repo.register_artifact(ArtifactIndexEntryV2(baseline, "PolicyV2", baseline, 0, 0, {}))
        exp = DiscoveryExperimentV2("fixed-holdout", "family", "M1", ("candles",), ("CAUSAL",), 1, 1,
            baseline, ("whole_policy_value",), "180_30_30", "MAX_HORIZON", "family", "BUDGET",
            first[2], "UNTOUCHED", True, 500 * DAY_NS + 2)
        register_discovery_experiment(repo, exp, available_at_ns=500 * DAY_NS + 2)
        mark_holdout_spent(repo, experiment_ref=exp.content_hash, holdout_ref=first[2], attempt_id="viewed", viewed_at_ns=500 * DAY_NS + 3)
        with pytest.raises(ValueError, match="SPENT"):
            reserve_m1_final_holdout(repo, compatibility_key=key, cutoff_ns=600 * DAY_NS, available_at_ns=600 * DAY_NS + 1)
