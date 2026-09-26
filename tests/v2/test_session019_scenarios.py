"""Explicit executable BBO, joint-path and exact action cashflow scenarios."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.risk import index_research_evidence
from atlas.v2.science.action import freeze_action
from atlas.v2.science.pretrade import CausalInputV2, ScenarioFillStateV2
from atlas.v2.science.scenario_engine import (
    PRETRADE_EXECUTION_SCENARIO_VERSION,
    SCENARIO_GENERATOR_VERSION,
    ExitReasonV2,
    JointExecutionDataV2,
    JointMarketPointV2,
    PretradeExecutionScenarioV2,
    S2ScenarioManagementV2,
    ScenarioGenerationStatusV2,
    generate_pretrade_scenarios,
    index_joint_execution_data,
    validate_pretrade_scenario_evidence,
)
from atlas.v2.strategies.s1_trend import S1_POLICY

from .test_session016_candidate_selection import CUTOFF
from .test_session017_risk import risk_case, size


def _evidence(repo, kind: str, name: str, *, at_ns: int = CUTOFF) -> CausalInputV2:
    body = {"version": kind, "fixture": name, "available_at_ns": at_ns}
    ref = sha256_json(body)
    index_research_evidence(repo, kind, ref, at_ns, body)
    return CausalInputV2(ref, kind, at_ns, at_ns)


def _fixture(repo, *, fill: bool = True, quantity_fraction: str = "1"):
    case = risk_case(repo)
    action = freeze_action(repo, candidate=case.candidate, candidate_set=case.candidate_set,
        sizing=size(repo, case), product=case.product, policy=S1_POLICY, v1=case.v1, v2=case.v2)
    model = _evidence(repo, "PretradeModelV1", "model")
    calibration = _evidence(repo, "PretradeCalibrationV1", "calibration")
    execution = _evidence(repo, "ExecutionModelV1", "execution")
    source = _evidence(repo, "JointExecutionTemplateSourceV1", "template")
    qty = action.action.quantity * Decimal(quantity_fraction) if fill else Decimal(0)
    entry_state = (ScenarioFillStateV2.FULL_FILL if qty == action.action.quantity else
                   ScenarioFillStateV2.PARTIAL_FILL if fill else ScenarioFillStateV2.NO_FILL)
    path_id = sha256_json({"path": "coherent-fixture", "fill": fill, "fraction": quantity_fraction})
    entry_at = CUTOFF + 1_000_000_000
    exit_at = action.action.horizon_end_ns
    funding_ref = sha256_json("funding-settlement-unique")
    points = (
        JointMarketPointV2(entry_at, path_id, Decimal("100.00"), Decimal("100.00"), Decimal("100.00"),
            Decimal("99.99"), Decimal("100.01"), action.action.quantity + 1, action.action.quantity + 1,
            Decimal("0.02"), None, Decimal(0)),
        JointMarketPointV2(exit_at, path_id, Decimal("101.00"), Decimal("101.00"), Decimal("100.99"),
            Decimal("100.99"), Decimal("101.01"), action.action.quantity + 1, action.action.quantity + 1,
            Decimal("0.02"), funding_ref, Decimal("0.30")),
    )
    data = JointExecutionDataV2(action.action.action_hash, action.content_hash, CUTOFF,
        sha256_json("common-scenario-set"), path_id, SCENARIO_GENERATOR_VERSION,
        PRETRADE_EXECUTION_SCENARIO_VERSION, source.ref, execution.ref, case.fee.content_hash,
        action.action.quantity, entry_state,
        qty, Decimal("100.01") if fill else None, entry_at if fill else None, 1_000_000_000 if fill else None,
        ScenarioFillStateV2.FULL_FILL if fill else ScenarioFillStateV2.NO_FILL,
        qty if fill else Decimal(0), Decimal("100.99") if fill else None,
        exit_at if fill else None, 0 if fill else None, ExitReasonV2.TIME_EXIT if fill else None,
        None, points, CUTOFF, CUTOFF, True, True)
    index_joint_execution_data(repo, data)
    scenario, payoffs = generate_pretrade_scenarios(repo, action=action, model_input=model,
        calibration_input=calibration, execution_model_input=execution, source_inputs=(source,),
        joint_data_refs=(data.content_hash,), fee=case.fee, base_units_per_contract=case.product.base_units_per_contract,
        cutoff_ns=CUTOFF, created_at_ns=CUTOFF + 1, computed_at_ns=CUTOFF + 2,
        available_at_ns=CUTOFF + 3, expires_at_ns=case.candidate.deadline_ns,
        seed=0, scenario_count=13, allow_synthetic_fixtures=True)
    return action, data, scenario, payoffs


def test_joint_v2_cashflows_use_explicit_bbo_fees_funding_and_partial_quantity(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        action, _, scenario, payoffs = _fixture(repo, quantity_fraction="0.5")
        assert scenario.synthetic_fixture is True
        assert scenario.status == ScenarioGenerationStatusV2.AVAILABLE
        assert action.action.entry_trigger_basis == S1_POLICY.trigger_basis
        assert action.action.stop_trigger_basis == "MARK_PRICE"
        assert sum((row[1] for row in scenario.rows), Decimal(0)) == Decimal(1)
        assert len(payoffs) == 1
        result = payoffs[0]
        qty = action.action.quantity / 2
        expected_entry = -(qty * Decimal("100.01"))
        expected_exit = qty * Decimal("100.99")
        expected_fees = qty * Decimal("100.01") * Decimal("0.001") + qty * Decimal("100.99") * Decimal("0.001")
        assert result.filled_quantity == qty
        assert result.entry_cashflow == expected_entry
        assert result.exit_cashflow == expected_exit
        assert result.fees == expected_fees
        assert result.funding == Decimal("0.30")
        assert result.net_payoff == expected_entry + expected_exit - expected_fees + Decimal("0.30")
        assert validate_pretrade_scenario_evidence(repo, action=action, scenario=scenario) == payoffs


def test_earlier_mark_stop_cannot_be_ignored_or_latency_rewritten(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        action, data, original, _ = _fixture(repo)
        with pytest.raises(ValueError, match="modeled latency"):
            replace(data, entry_latency_ns=0)
        assert data.entry_at_ns is not None
        stop_point = replace(data.points[0], at_ns=data.entry_at_ns + 1_000_000_000,
            mark_price=action.action.stop_price)
        ignores_stop = replace(data, points=(data.points[0], stop_point, data.points[1]))
        index_joint_execution_data(repo, ignores_stop)
        case = risk_case(repo)

        def generate(template):
            return generate_pretrade_scenarios(repo, action=action, model_input=original.model_input,
                calibration_input=original.calibration_input, execution_model_input=original.execution_model_input,
                source_inputs=original.source_inputs, joint_data_refs=(template.content_hash,), fee=case.fee,
                base_units_per_contract=case.product.base_units_per_contract, cutoff_ns=CUTOFF,
                created_at_ns=CUTOFF + 1, computed_at_ns=CUTOFF + 2, available_at_ns=CUTOFF + 3,
                expires_at_ns=case.candidate.deadline_ns, seed=1901, scenario_count=100,
                allow_synthetic_fixtures=True)

        rejected, payoffs = generate(ignores_stop)
        assert rejected.status == ScenarioGenerationStatusV2.NOT_ESTIMABLE and not payoffs
        exact_stop = replace(ignores_stop, exit_reason=ExitReasonV2.STOP,
            exit_at_ns=stop_point.at_ns, exit_latency_ns=0, exit_price=stop_point.bid_price)
        index_joint_execution_data(repo, exact_stop)
        supported, payoffs = generate(exact_stop)
        assert supported.status == ScenarioGenerationStatusV2.AVAILABLE
        assert payoffs[0].filled_quantity == action.action.quantity
        assert payoffs[0].funding == 0
        assert validate_pretrade_scenario_evidence(repo, action=action, scenario=supported) == payoffs


def test_scenario_resolution_does_not_increase_template_support(tmp_path):
    from atlas.v2.science.admission import make_scenario_support

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        action, data, first, _ = _fixture(repo)
        case = risk_case(repo)
        model = _evidence(repo, "PretradeModelV1", "support-model")
        calibration = _evidence(repo, "PretradeCalibrationV1", "support-calibration")
        execution = CausalInputV2(data.execution_model_ref, "ExecutionModelV1", CUTOFF, CUTOFF)
        source = CausalInputV2(data.source_ref, "JointExecutionTemplateSourceV1", CUTOFF, CUTOFF)
        second, _ = generate_pretrade_scenarios(repo, action=action, model_input=model,
            calibration_input=calibration, execution_model_input=execution, source_inputs=(source,),
            joint_data_refs=(data.content_hash,), fee=case.fee,
            base_units_per_contract=case.product.base_units_per_contract, cutoff_ns=CUTOFF,
            created_at_ns=CUTOFF + 1, computed_at_ns=CUTOFF + 2, available_at_ns=CUTOFF + 3,
            expires_at_ns=case.candidate.deadline_ns, seed=10, scenario_count=1000,
            allow_synthetic_fixtures=True)
        first_support = make_scenario_support(repo, action=action, scenario=first, support_unit_refs=())
        second_support = make_scenario_support(repo, action=action, scenario=second, support_unit_refs=())
        assert len(first.rows) != 0 and len(second.rows) != 0
        assert first_support.independent_support_unit_count == second_support.independent_support_unit_count == 0
        assert first_support.template_refs == second_support.template_refs == (data.content_hash,)
        assert first_support.evidence_quality == "UNSUPPORTED_OR_ENGINEERING_FIXTURE"


def test_no_fill_is_exact_zero_and_synthetic_paths_cannot_be_real_outcome_distribution(tmp_path):
    from atlas.v2.science.admission import make_outcome_distribution

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _, _, scenario, payoffs = _fixture(repo, fill=False)
        assert payoffs[0].net_payoff == Decimal(0)
        assert payoffs[0].fees == Decimal(0)
        assert payoffs[0].funding == Decimal(0)
        distribution = make_outcome_distribution(scenario, payoffs)
        assert distribution.status == "NOT_ESTIMABLE"
        assert distribution.reason == "SYNTHETIC_ENGINEERING_SCENARIO"
        assert not distribution.path_ids and distribution.expected_net_pnl is None


def test_joint_wire_rejects_unknown_versions_and_cross_path_component_swaps(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _, data, scenario, _ = _fixture(repo)
        bad_wire = data.to_dict() | {"version": "JOINT_SCENARIO_DATA_V2_V999"}
        with pytest.raises(ValueError, match="unsupported"):
            JointExecutionDataV2.from_dict(bad_wire)
        body = data.to_dict()
        body["points"][0]["joint_path_id"] = sha256_json("other-path")
        body.pop("content_hash", None)
        with pytest.raises(ValueError, match="path identity"):
            JointExecutionDataV2.from_dict(body)
        for version in ("PRETRADE_SCENARIO_ARTIFACT_V2_V4", "PRETRADE_SCENARIO_ARTIFACT_V2_V6"):
            with pytest.raises(ValueError):
                PretradeExecutionScenarioV2.from_dict({"version": version})
        assert scenario.content_hash == sha256_json(scenario._body())


def test_s2_management_wire_preserves_first_two_closed_bars_and_is_versioned():
    path = sha256_json("s2-path")
    management = S2ScenarioManagementV2(sha256_json("s2-action"), path, sha256_json("s2-setup"),
        ExitReasonV2.TIME_EXIT, None, Decimal("90"), Decimal("110"),
        ((CUTOFF + 900_000_000_000, Decimal("112")),
         (CUTOFF + 1_800_000_000_000, Decimal("115"))),
        sha256_json("s2-management-rule"), CUTOFF)
    assert S2ScenarioManagementV2.from_dict(management.to_dict()) == management
    with pytest.raises(ValueError, match="unsupported"):
        S2ScenarioManagementV2.from_dict(management.to_dict() | {"version": "S2_PRETRADE_MANAGEMENT_PATH_V1"})


def test_missing_cutoff_known_execution_support_returns_not_estimable(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        from atlas.v2.science.action import freeze_action
        from atlas.v2.strategies.s1_trend import S1_POLICY

        action = freeze_action(repo, candidate=case.candidate, candidate_set=case.candidate_set,
            sizing=size(repo, case), product=case.product, policy=S1_POLICY, v1=case.v1, v2=case.v2)
        model = _evidence(repo, "PretradeModelV1", "model")
        calibration = _evidence(repo, "PretradeCalibrationV1", "calibration")
        execution = _evidence(repo, "ExecutionModelV1", "execution")
        source = _evidence(repo, "JointExecutionTemplateSourceV1", "empty")
        scenario, payoffs = generate_pretrade_scenarios(repo, action=action, model_input=model,
            calibration_input=calibration, execution_model_input=execution, source_inputs=(source,),
            joint_data_refs=(), fee=case.fee, base_units_per_contract=case.product.base_units_per_contract,
            cutoff_ns=CUTOFF, created_at_ns=CUTOFF + 1, computed_at_ns=CUTOFF + 2,
            available_at_ns=CUTOFF + 3, expires_at_ns=case.candidate.deadline_ns,
            seed=0, scenario_count=50)
        assert scenario.status == ScenarioGenerationStatusV2.NOT_ESTIMABLE
        assert not scenario.rows and not payoffs


def test_unqualified_depth_and_future_or_retrospective_inputs_fail_closed(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        action, data, _, _ = _fixture(repo)
        # The fixture helper indexed a qualified path; the revised immutable data
        # record makes the absent depth qualification explicit.
        unsupported = replace(data, execution_depth_qualified=False)
        index_joint_execution_data(repo, unsupported)
        model = _evidence(repo, "PretradeModelV1", "model-for-unsupported")
        calibration = _evidence(repo, "PretradeCalibrationV1", "calibration-for-unsupported")
        execution = CausalInputV2(data.execution_model_ref, "ExecutionModelV1", CUTOFF, CUTOFF)
        source = CausalInputV2(data.source_ref, "JointExecutionTemplateSourceV1", CUTOFF, CUTOFF)
        case = risk_case(repo)
        scenario, payoffs = generate_pretrade_scenarios(repo, action=action, model_input=model,
            calibration_input=calibration, execution_model_input=execution, source_inputs=(source,),
            joint_data_refs=(unsupported.content_hash,), fee=case.fee,
            base_units_per_contract=case.product.base_units_per_contract, cutoff_ns=CUTOFF,
            created_at_ns=CUTOFF + 1, computed_at_ns=CUTOFF + 2, available_at_ns=CUTOFF + 3,
            expires_at_ns=case.candidate.deadline_ns, seed=0, scenario_count=20)
        assert scenario.status == ScenarioGenerationStatusV2.NOT_ESTIMABLE
        assert not payoffs


def test_s1_cannot_silently_apply_s2_failed_break_management(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        action, data, _, _ = _fixture(repo)
        invalid = replace(data, exit_reason=ExitReasonV2.FAILED_BREAK,
            s2_management_ref=sha256_json("misapplied-s2-management"))
        index_joint_execution_data(repo, invalid)
        case = risk_case(repo)
        model = _evidence(repo, "PretradeModelV1", "s1-failed-break-model")
        calibration = _evidence(repo, "PretradeCalibrationV1", "s1-failed-break-calibration")
        execution = CausalInputV2(data.execution_model_ref, "ExecutionModelV1", CUTOFF, CUTOFF)
        source = CausalInputV2(data.source_ref, "JointExecutionTemplateSourceV1", CUTOFF, CUTOFF)
        scenario, payoffs = generate_pretrade_scenarios(repo, action=action, model_input=model,
            calibration_input=calibration, execution_model_input=execution, source_inputs=(source,),
            joint_data_refs=(invalid.content_hash,), fee=case.fee,
            base_units_per_contract=case.product.base_units_per_contract, cutoff_ns=CUTOFF,
            created_at_ns=CUTOFF + 1, computed_at_ns=CUTOFF + 2, available_at_ns=CUTOFF + 3,
            expires_at_ns=case.candidate.deadline_ns, seed=3, scenario_count=10,
            allow_synthetic_fixtures=True)
        assert scenario.status == ScenarioGenerationStatusV2.NOT_ESTIMABLE
        assert scenario.reason == "SCENARIO_POLICY_OR_EXECUTION_MECHANICS_UNRESOLVED"
        assert not payoffs

        future_model = _evidence(repo, "PretradeModelV1", "future-model", at_ns=CUTOFF + 1)
        with pytest.raises(ValueError, match="not available by decision cutoff"):
            generate_pretrade_scenarios(repo, action=action, model_input=future_model,
                calibration_input=calibration, execution_model_input=execution, source_inputs=(source,),
                joint_data_refs=(), fee=case.fee, base_units_per_contract=case.product.base_units_per_contract,
                cutoff_ns=CUTOFF, created_at_ns=CUTOFF + 1, computed_at_ns=CUTOFF + 2,
                available_at_ns=CUTOFF + 3, expires_at_ns=case.candidate.deadline_ns,
                seed=1, scenario_count=20)

        with pytest.raises(ValueError, match="retrospective or future-vintage"):
            CausalInputV2(sha256_json("retrospective-replay"), "ReplayPathV2", CUTOFF, CUTOFF)
