"""Regression coverage for Session 019 review remediation evidence binding."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.science.admission import (
    ExecutionCalibrationResidualV2,
    NumericalErrorV2,
    ScenarioSupportUnitV2,
    index_numerical_convergence_run,
    index_scenario_support_unit,
    make_estimation_uncertainty,
    make_execution_uncertainty,
    make_numerical_error,
    make_scenario_support,
    scenario_support_compatibility_classes,
)
from atlas.v2.science.m0 import M0PredictionV2
from atlas.v2.science.scenario_engine import (
    JointExecutionDataV2,
    generate_pretrade_scenarios,
    index_joint_execution_data,
)

from .test_session016_candidate_selection import CUTOFF
from .test_session017_risk import risk_case
from .test_session019_scenarios import _evidence, _fixture


def _support_templates(repo, *, count: int, independent_units: int, seed: int,
        scenario_count: int, vary_payoff: bool = False):
    action, base, original, _ = _fixture(repo)
    case = risk_case(repo)
    if independent_units not in (1, count):
        raise ValueError("test helper supports shared or one-to-one episode provenance")
    sources = {item.ref: item for item in original.source_inputs}
    templates: list[JointExecutionDataV2] = []
    for i in range(independent_units):
        source = (base.source_ref if independent_units == 1 else
            _evidence(repo, "JointExecutionTemplateSourceV1", f"independent-source-{i}").ref)
        source_item = repo.get_artifact(source)
        assert source_item is not None
        if source not in sources:
            from atlas.v2.science.pretrade import CausalInputV2

            sources[source] = CausalInputV2(source, source_item.artifact_type,
                source_item.available_at_ns, source_item.available_at_ns)
        for j in range(count // independent_units):
            index = i * (count // independent_units) + j
            path_id = sha256_json({"support-template-path": index})
            points = []
            for point in base.points:
                if vary_payoff and point.funding_event_ref is not None:
                    points.append(replace(point, joint_path_id=path_id,
                        funding_event_ref=sha256_json({"support-funding-event": index}),
                        funding_cashflow_usdt=Decimal("0.3") + Decimal(index)))
                else:
                    points.append(replace(point, joint_path_id=path_id))
            item = replace(base, joint_path_id=path_id, points=tuple(points), source_ref=source)
            index_joint_execution_data(repo, item)
            templates.append(item)
    model_inputs = (original.model_input, original.calibration_input, original.execution_model_input)
    source_inputs = tuple(sorted((sources[ref] for ref in {item.ref for item in original.source_inputs} |
        {item.source_ref for item in templates}), key=lambda item: item.ref))
    scenario, _ = generate_pretrade_scenarios(repo, action=action,
        model_input=model_inputs[0], calibration_input=model_inputs[1],
        execution_model_input=model_inputs[2], source_inputs=source_inputs,
        joint_data_refs=tuple(sorted(item.content_hash for item in templates)), fee=case.fee,
        base_units_per_contract=case.product.base_units_per_contract, cutoff_ns=CUTOFF,
        created_at_ns=CUTOFF + 1, computed_at_ns=CUTOFF + 2, available_at_ns=CUTOFF + 3,
        expires_at_ns=case.candidate.deadline_ns, seed=seed, scenario_count=scenario_count,
        allow_synthetic_fixtures=True)
    policy_class, action_class = scenario_support_compatibility_classes(action)
    units = []
    for index, item in enumerate(templates):
        episode_index = index // (count // independent_units)
        if independent_units == 1:
            window_start, window_end = CUTOFF - 200, CUTOFF - 100
        else:
            window_start = CUTOFF - 500 + episode_index * 100
            window_end = window_start + 50
        unit = ScenarioSupportUnitV2(item.source_ref, window_start, window_end,
            (item.source_ref,), item_source_venue(action), action.action.product_ref,
            policy_class, action_class, original.execution_model_input.ref,
            original.calibration_input.ref, item.content_hash, CUTOFF, item.synthetic_fixture)
        units.append(unit)
    unit_refs = tuple(sorted(index_scenario_support_unit(repo, unit) for unit in units))
    support = make_scenario_support(repo, action=action, scenario=scenario,
        support_unit_refs=unit_refs)
    return action, case, base, original, templates, scenario, units, support


def item_source_venue(action):
    return action.action.key.venue


def test_thirty_templates_from_one_episode_are_one_support_unit_and_resampling_adds_none(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        action, case, base, original, templates, scenario_a, units, support_a = _support_templates(
            repo, count=30, independent_units=1, seed=101, scenario_count=500)
        assert len({unit.template_ref for unit in units}) == 30
        assert len({unit.source_episode_ref for unit in units}) == 1
        assert len({unit.source_bundle_refs for unit in units}) == 1
        assert support_a.independent_support_unit_count == 1
        assert support_a.evidence_quality == "UNSUPPORTED_OR_ENGINEERING_FIXTURE"
        scenario_b, _ = generate_pretrade_scenarios(repo, action=action,
            model_input=original.model_input, calibration_input=original.calibration_input,
            execution_model_input=original.execution_model_input, source_inputs=scenario_a.source_inputs,
            joint_data_refs=tuple(sorted(item.content_hash for item in templates)), fee=case.fee,
            base_units_per_contract=case.product.base_units_per_contract, cutoff_ns=CUTOFF,
            created_at_ns=CUTOFF + 1, computed_at_ns=CUTOFF + 2, available_at_ns=CUTOFF + 3,
            expires_at_ns=case.candidate.deadline_ns, seed=909, scenario_count=5_000,
            allow_synthetic_fixtures=True)
        support_b = make_scenario_support(repo, action=action, scenario=scenario_b,
            support_unit_refs=support_a.support_unit_refs)
        assert scenario_a.content_hash != scenario_b.content_hash
        assert support_b.independent_support_unit_count == 1
        assert support_b.independent_unit_refs == support_a.independent_unit_refs


def test_nonoverlapping_unique_source_units_increase_support_deterministically(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _, _, _, _, _, _, _, support = _support_templates(
            repo, count=3, independent_units=3, seed=11, scenario_count=300)
        assert support.independent_support_unit_count == 3
        assert support.independent_unit_refs == tuple(sorted(support.independent_unit_refs))


def test_shared_source_bundle_dedupes_template_hashes_and_rejects_forged_episode_ids(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        action, _, _, _, templates, scenario, units, _ = _support_templates(
            repo, count=3, independent_units=1, seed=21, scenario_count=30)
        assert len({item.content_hash for item in templates}) == 3
        assert len({item.source_ref for item in templates}) == 1
        forged_episode = _evidence(repo, "ScenarioSupportEpisodeFixtureV1", "different-episode")
        with pytest.raises(ValueError, match="exact template and historical source window"):
            index_scenario_support_unit(repo, replace(units[0], source_episode_ref=forged_episode.ref))
        refs = tuple(sorted(index_scenario_support_unit(repo, unit) for unit in units))
        support = make_scenario_support(repo, action=action, scenario=scenario,
            support_unit_refs=refs)
        assert support.independent_support_unit_count == 1


def test_numerical_convergence_is_reproduced_from_indexed_scenario_payoffs(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        action, data, primary, _ = _fixture(repo)
        case = risk_case(repo)

        def generate(seed: int, count: int):
            return generate_pretrade_scenarios(repo, action=action,
                model_input=primary.model_input, calibration_input=primary.calibration_input,
                execution_model_input=primary.execution_model_input, source_inputs=primary.source_inputs,
                joint_data_refs=(data.content_hash,), fee=case.fee,
                base_units_per_contract=case.product.base_units_per_contract, cutoff_ns=CUTOFF,
                created_at_ns=CUTOFF + 1, computed_at_ns=CUTOFF + 2, available_at_ns=CUTOFF + 3,
                expires_at_ns=case.candidate.deadline_ns, seed=seed, scenario_count=count,
                allow_synthetic_fixtures=True)

        run_scenario_a, payoff_a = generate(17, 100)
        run_scenario_b, payoff_b = generate(29, 1_000)
        run_scenario_c, _ = generate(31, 2_000)
        assert len(payoff_a) == len(payoff_b) == 1
        assert run_scenario_a.content_hash != run_scenario_b.content_hash
        assert run_scenario_b.content_hash != run_scenario_c.content_hash
        assert run_scenario_a.seed != run_scenario_b.seed
        run_a_ref = index_numerical_convergence_run(repo, action=action,
            scenario_ref=run_scenario_a.content_hash)
        run_b_ref = index_numerical_convergence_run(repo, action=action,
            scenario_ref=run_scenario_b.content_hash)
        run_c_ref = index_numerical_convergence_run(repo, action=action,
            scenario_ref=run_scenario_c.content_hash)
        prediction = M0PredictionV2(action.action.action_hash, action.content_hash,
            sha256_json("feature-vector"), sha256_json("model-fit"), CUTOFF, CUTOFF + 3,
            Decimal("1.25"), Decimal("0.1"), Decimal("0.0001"), sha256_json("m0-support"),
            sha256_json("oof-archive"), sha256_json("calibration"), sha256_json("ood"),
            "AVAILABLE", ())
        first = make_numerical_error(repo, action=action, scenario=primary,
            prediction=prediction, run_a_ref=run_a_ref, run_b_ref=run_b_ref)
        second = make_numerical_error(repo, action=action, scenario=primary,
            prediction=prediction, run_a_ref=run_b_ref, run_b_ref=run_c_ref)
        assert first.status == "AVAILABLE"
        assert first.estimate_a == payoff_a[0].net_payoff
        assert first.estimate_b == payoff_b[0].net_payoff
        assert first.m0_conversion_error == prediction.numerical_conversion_error
        assert first.error_bound == abs(first.estimate_a - first.estimate_b) + Decimal("0.0001")
        assert first.content_hash != second.content_hash
        with pytest.raises(TypeError):
            make_numerical_error(action_hash=action.action.action_hash,
                scenario_ref=primary.content_hash, seed_a=1, seed_b=2,
                path_count_a=100, path_count_b=1_000,
                estimate_a=Decimal(0), estimate_b=Decimal(0),
                m0_conversion_error=Decimal(0))
        with pytest.raises(ValueError):
            NumericalErrorV2.from_dict({"version": "PRETRADE_NUMERICAL_ERROR_V1"})


def test_more_paths_do_not_change_support_estimation_or_execution_uncertainty(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        action, case, _, original, templates, scenario_a, _, support_a = _support_templates(
            repo, count=3, independent_units=3, seed=0, scenario_count=20, vary_payoff=True)

        def generate(seed: int, count: int):
            return generate_pretrade_scenarios(repo, action=action,
                model_input=original.model_input, calibration_input=original.calibration_input,
                execution_model_input=original.execution_model_input, source_inputs=scenario_a.source_inputs,
                joint_data_refs=tuple(sorted(item.content_hash for item in templates)), fee=case.fee,
                base_units_per_contract=case.product.base_units_per_contract, cutoff_ns=CUTOFF,
                created_at_ns=CUTOFF + 1, computed_at_ns=CUTOFF + 2, available_at_ns=CUTOFF + 3,
                expires_at_ns=case.candidate.deadline_ns, seed=seed, scenario_count=count,
                allow_synthetic_fixtures=True)

        scenario_b, _ = generate(7, 20)
        scenario_c, _ = generate(3, 10_000)
        scenario_d, _ = generate(4, 10_000)
        support_b = make_scenario_support(repo, action=action, scenario=scenario_b,
            support_unit_refs=support_a.support_unit_refs)
        prediction = M0PredictionV2(action.action.action_hash, action.content_hash,
            sha256_json("feature"), sha256_json("model"), CUTOFF, CUTOFF + 3,
            Decimal(1), Decimal("0.2"), Decimal("0.001"), sha256_json("support"),
            sha256_json("oof"), sha256_json("calibration"), sha256_json("ood"), "AVAILABLE", ())
        estimation_a = make_estimation_uncertainty(prediction, training_refs=(sha256_json("row"),),
            confidence_multiplier=Decimal(1))
        estimation_b = make_estimation_uncertainty(prediction, training_refs=(sha256_json("row"),),
            confidence_multiplier=Decimal(1))
        residual = ExecutionCalibrationResidualV2(sha256_json("execution-observation"),
            sha256_json("compatibility"), Decimal("1"), Decimal("2"), CUTOFF - 10, CUTOFF)
        execution_a = make_execution_uncertainty(action.action.action_hash,
            original.execution_model_input.ref, (residual,), compatibility_key=residual.action_compatibility_key,
            cutoff_ns=CUTOFF, minimum_support=1)
        execution_b = make_execution_uncertainty(action.action.action_hash,
            original.execution_model_input.ref, (residual,), compatibility_key=residual.action_compatibility_key,
            cutoff_ns=CUTOFF, minimum_support=1)
        run_refs = tuple(index_numerical_convergence_run(repo, action=action,
            scenario_ref=item.content_hash) for item in (scenario_a, scenario_b, scenario_c, scenario_d))
        low_resolution = make_numerical_error(repo, action=action, scenario=scenario_a,
            prediction=prediction, run_a_ref=run_refs[0], run_b_ref=run_refs[1])
        high_resolution = make_numerical_error(repo, action=action, scenario=scenario_c,
            prediction=prediction, run_a_ref=run_refs[2], run_b_ref=run_refs[3])
        assert support_a.independent_support_unit_count == support_b.independent_support_unit_count == 3
        assert support_a.content_hash != support_b.content_hash  # scenario identity changed; support count did not
        assert estimation_a.content_hash == estimation_b.content_hash
        assert execution_a.content_hash == execution_b.content_hash
        assert low_resolution.error_bound is not None and high_resolution.error_bound is not None
        assert high_resolution.error_bound < low_resolution.error_bound


def test_admission_policy_does_not_accept_legacy_capability_boolean():
    from .test_session019_admission import _policy

    policy = _policy()
    with pytest.raises(ValueError):
        policy.from_dict(policy.to_dict() | {"venue_capability_qualified": True})
    assert "venue_capability_qualified" not in policy.to_dict()
