"""Installed indexed economics consumes cutoff declarations after honest action publication."""
from __future__ import annotations

import itertools
from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import canonical_json, json_value, sha256_json
from atlas.v2.chronology import causal_artifact, chronology_ref, record_computation
from atlas.v2.contracts import FeatureArtifactV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.risk import size_selected_candidate
from atlas.v2.runtime import production
from atlas.v2.runtime.economic_sources import EconomicSourceManifestV1, index_economic_source_manifest
from atlas.v2.science.action import freeze_action
from atlas.v2.science.admission import (
    ScenarioSupportUnitV2,
    index_scenario_support_unit,
    index_venue_capability_snapshot,
    scenario_support_compatibility_classes,
)
from atlas.v2.science.evaluation_service import run_phase2_economic_evaluation
from atlas.v2.science.outcomes import AdmissionStateV2, DecisionCalendarEntryV2
from atlas.v2.science.pretrade import CausalInputV2, ScenarioFillStateV2
from atlas.v2.science.research_selection import assemble_multisleeve_research_candidate_set
from atlas.v2.science.scenario_engine import (
    PRETRADE_EXECUTION_SCENARIO_VERSION,
    SCENARIO_GENERATOR_VERSION,
    ExitReasonV2,
    JointExecutionDataV2,
    JointMarketPointV2,
    index_joint_execution_data,
    validate_pretrade_scenario_evidence,
)
from atlas.v2.strategies.s1_trend import S1_POLICY

from .session023_support import research_case
from .test_session016_candidate_selection import CUTOFF, evidence
from .test_session020_phase2_e2e import _admission_policy, _capability_for_action
from .test_session027_ops_supervisor import make_event


def _source(repo, case, kind):
    body = {"version": kind, "available_at_ns": CUTOFF, "vintage_at_ns": CUTOFF,
        "instrument_key_ref": case.product.key.content_hash, "product_ref": case.product.content_hash,
        "account_scope": case.account.account_scope, "policy_hash": S1_POLICY.policy_hash,
        "synthetic_fixture": True}
    ref = sha256_json(body)
    repo.register_artifact(ArtifactIndexEntryV2(ref, kind, ref, CUTOFF, CUTOFF, body))
    return CausalInputV2(ref, kind, CUTOFF, CUTOFF)


def _fixture(repo, *, declaration="valid", joint_template=False, synthetic_template=False, late_feature=False):
    case = research_case(repo)
    manifest = EconomicSourceManifestV1(case.product.key.content_hash, case.product.content_hash,
        case.account.account_scope, S1_POLICY.policy_hash, _admission_policy(),
        _source(repo, case, "PretradeModelV1"), _source(repo, case, "PretradeCalibrationV1"),
        _source(repo, case, "ExecutionModelV1"), 32, CUTOFF, CUTOFF)
    if joint_template:
        original_sizing = size_selected_candidate(repo, candidate_set=case.candidate_set, candidate=case.candidate,
            universe=case.universe, policy=S1_POLICY, product=case.product, v1=case.v1, v2=case.v2,
            account=case.account, exposures=case.exposures, outcomes=case.outcomes, venue=case.venue,
            stress=case.stress, fee=case.fee, cutoff_ns=CUTOFF)
        original_action = freeze_action(repo, candidate=case.candidate, candidate_set=case.candidate_set,
            sizing=original_sizing, product=case.product, policy=S1_POLICY, v1=case.v1, v2=case.v2)
        source = _source(repo, case, "JointExecutionTemplateSourceV1")
        path_id = sha256_json("session037-preregistered-contract-path")
        entry_at, exit_at = CUTOFF + 1_000_000_000, original_action.action.horizon_end_ns
        quantity = original_action.action.quantity
        points = (
            JointMarketPointV2(entry_at, path_id, Decimal("100"), Decimal("100"), Decimal("100"),
                Decimal("99.99"), Decimal("100.01"), quantity + 1, quantity + 1, Decimal("0.02"), None, Decimal(0)),
            JointMarketPointV2(exit_at, path_id, Decimal("101"), Decimal("101"), Decimal("100.99"),
                Decimal("100.99"), Decimal("101.01"), quantity + 1, quantity + 1, Decimal("0.02"), None, Decimal(0)))
        template = JointExecutionDataV2(original_action.action.action_hash, original_action.content_hash, CUTOFF,
            sha256_json("session037-preregistered-contract-scenarios"), path_id, SCENARIO_GENERATOR_VERSION,
            PRETRADE_EXECUTION_SCENARIO_VERSION, source.ref, manifest.execution_model_input.ref,
            case.fee.content_hash, quantity, ScenarioFillStateV2.FULL_FILL, quantity, Decimal("100.01"),
            entry_at, 1_000_000_000, ScenarioFillStateV2.FULL_FILL, quantity, Decimal("100.99"), exit_at,
            0, ExitReasonV2.TIME_EXIT, None, points, CUTOFF, CUTOFF, True, synthetic_template)
        index_joint_execution_data(repo, template)
        policy_class, action_class = scenario_support_compatibility_classes(original_action)
        unit = ScenarioSupportUnitV2(source.ref, CUTOFF - 200, CUTOFF - 100, (source.ref,),
            case.product.key.venue, case.product.content_hash, policy_class, action_class,
            manifest.execution_model_input.ref, manifest.calibration_input.ref, template.content_hash,
            CUTOFF, synthetic_template)
        index_scenario_support_unit(repo, unit)
        manifest = replace(manifest, source_inputs=(source,), joint_data_refs=(template.content_hash,),
                           support_unit_refs=(unit.content_hash,))
    if declaration == "future":
        manifest = replace(manifest, available_at_ns=CUTOFF + 1)
    elif declaration == "foreign":
        # Register a valid foreign policy declaration using the same public source
        # facts only when their original scope has been preserved.
        manifest = replace(manifest, account_scope="FOREIGN_ACCOUNT_SCOPE")
    if declaration not in {"missing", "foreign"}:
        index_economic_source_manifest(repo, manifest)
        if declaration == "ambiguous":
            index_economic_source_manifest(repo, replace(manifest, scenario_count=33))
    if declaration == "foreign":
        # Raw tampered declaration must be rejected by the installed validator;
        # bypass registration deliberately to test persisted foreign evidence.
        repo.register_artifact(ArtifactIndexEntryV2(manifest.content_hash, "EconomicSourceManifestV1",
            manifest.content_hash, CUTOFF, CUTOFF, {"economic_source_manifest": manifest.to_dict()}))
    event = make_event(cutoff_ns=CUTOFF, deadline_delta_ns=5_000_000_000)
    clock = itertools.count(CUTOFF + 10).__next__
    original = case.candidate
    if late_feature:
        feature = FeatureArtifactV2.from_dict(json_value(repo.get_artifact(original.snapshot_hash).metadata["feature"]))
        started, finished, published = clock(), clock(), clock()
        feature = replace(feature, envelope=replace(feature.envelope, content_hash="",
            created_at_ns=started, available_at_ns=published))
        repo.register_artifact(ArtifactIndexEntryV2(feature.content_hash, "FeatureArtifactV2", feature.content_hash,
            started, published, {"feature": feature.to_dict()}))
        record_computation(repo, artifact_ref=feature.content_hash, information_cutoff_ns=CUTOFF,
            started_ns=started, finished_ns=finished, available_ns=published,
            input_refs=feature.envelope.input_refs, deadline_ns=event.deadline_ns)
        original = replace(original, snapshot_hash=feature.content_hash,
            envelope=replace(original.envelope, content_hash="", input_refs=(feature.content_hash,)))
    at = clock()
    candidate = replace(original, envelope=replace(original.envelope, created_at_ns=at,
        available_at_ns=at, content_hash=""))
    repo.register_artifact(ArtifactIndexEntryV2(candidate.content_hash, "CandidateActionV2", candidate.content_hash,
        at, at, {"candidate": candidate.to_dict(), "feature_hash": candidate.snapshot_hash}))
    record_computation(repo, artifact_ref=candidate.content_hash, information_cutoff_ns=CUTOFF,
        started_ns=at, finished_ns=at, available_ns=at, input_refs=candidate.envelope.input_refs,
        deadline_ns=event.deadline_ns)
    rank_ref = evidence(repo, original, case.universe, 1, event=event.event_id)
    candidate_set = assemble_multisleeve_research_candidate_set(repo, universe=case.universe,
        decision_event_id=event.event_id, cutoff_ns=CUTOFF, candidates=(candidate,),
        policies={S1_POLICY.policy_hash: S1_POLICY}, scanner_evidence_refs={candidate.candidate_id: (rank_ref,)},
        clock_ns=clock, deadline_ns=event.deadline_ns)
    assert candidate_set.selected_candidate_id is not None
    assert candidate_set.selected_candidate_id == candidate.candidate_id
    sizing = size_selected_candidate(repo, candidate_set=candidate_set, candidate=candidate,
        universe=case.universe, policy=S1_POLICY, product=case.product, v1=case.v1, v2=case.v2,
        account=case.account, exposures=case.exposures, outcomes=case.outcomes, venue=case.venue,
        stress=case.stress, fee=case.fee, cutoff_ns=CUTOFF, clock_ns=clock)
    assert sizing.status.value == "SIZED"
    action = freeze_action(repo, candidate=candidate, candidate_set=candidate_set, sizing=sizing,
        product=case.product, policy=S1_POLICY, v1=case.v1, v2=case.v2, clock_ns=clock)
    index_venue_capability_snapshot(repo, _capability_for_action(action, case, CUTOFF))
    risk = production.ProductionRiskInputsV1(case.product, case.v1, case.v2, case.account,
        case.exposures, case.outcomes, case.venue, case.stress, case.fee)
    return event, case, candidate, candidate_set, sizing, action, manifest, risk, clock


def _resolve(repo, fixture, *, clock=None):
    event, _case, candidate, candidate_set, _sizing, action, _manifest, risk, default_clock = fixture
    active_clock = default_clock if clock is None else clock
    # This is exactly the installed provider; no fixture provider substitutes
    # the production economic resolution method.
    indexed = production.IndexedProductionEventInputsV1(clock_ns=active_clock)
    return indexed.resolve_economic(repo, event, candidate_set, candidate, action, risk,
        now_ns=active_clock(), clock_ns=active_clock)


def _evaluate(repo, fixture, economic):
    _event, case, candidate, candidate_set, sizing, action, _manifest, _risk, clock = fixture
    return run_phase2_economic_evaluation(repo, action=action, candidate=candidate,
        candidate_set=candidate_set, sizing=sizing, product=case.product, risk_policy=case.v1,
        risk_policy_v2=case.v2, account=case.account, fee=case.fee,
        admission_policy=economic.admission_policy, capability=economic.capability,
        model_input=economic.model_input, calibration_input=economic.calibration_input,
        execution_model_input=economic.execution_model_input, available_at_ns=economic.available_at_ns,
        scenario_seed=economic.scenario_seed, scenario_count=economic.scenario_count, clock_ns=clock,
        source_inputs=economic.source_inputs, joint_data_refs=economic.joint_data_refs,
        support_unit_refs=economic.support_unit_refs, execution_residual_refs=economic.execution_residual_refs,
        stress_input_ref=economic.stress_input_ref,
        existing_portfolio_path_refs=economic.existing_portfolio_path_refs)


@pytest.mark.parametrize("late_feature", [False, True])
def test_installed_selected_late_action_reaches_real_m0_evaluation_and_calendar(tmp_path, late_feature):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        fixture = _fixture(repo, late_feature=late_feature)
        event, case, candidate, candidate_set, sizing, action, manifest, _risk, clock = fixture
        economic, reason = _resolve(repo, fixture)
        assert reason is None and economic is not None and economic.complete
        assert economic.model_input == manifest.model_input
        assert economic.joint_data_refs == () and economic.support_unit_refs == ()
        declared = repo.get_artifact(manifest.content_hash)
        assert declared is not None and declared.available_at_ns == CUTOFF
        resolution = repo.artifact_entries("OpsEconomicEvidenceResolutionV1")
        assert len(resolution) == 1
        assert resolution[0].available_at_ns > action.available_at_ns
        assert resolution[0].metadata["resolution"]["source_manifest_ref"] == manifest.content_hash
        assert resolution[0].metadata["resolution"]["action_artifact_ref"] == action.content_hash
        for ref in (candidate.content_hash, candidate_set.content_hash, sizing.content_hash, action.content_hash):
            entry = repo.get_artifact(ref)
            assert entry is not None and CUTOFF < entry.available_at_ns < event.deadline_ns
            assert causal_artifact(repo, ref, cutoff_ns=CUTOFF, consumer_at_ns=clock(), deadline_ns=event.deadline_ns)
            receipt = repo.get_artifact(chronology_ref(ref))
            assert receipt is not None and receipt.metadata["chronology"]["market_information_cutoff_ns"] == CUTOFF
        assert not repo.artifact_entries("OpsAdmissionPolicyEvidenceV1")
        assert not repo.artifact_entries("OpsCausalInputEvidenceV1")
        assert not repo.artifact_entries("OpsEconomicScenarioConfigV1")
        result = _evaluate(repo, fixture, economic)
        assert result.prediction.status == result.evaluation.decision.value == "NOT_ESTIMABLE"
        assert result.prediction.expected_net_value is None
        assert result.scenario.rows == ()
        predictions = repo.artifact_entries("M0PredictionV2")
        assert len(predictions) == 1
        assert predictions[0].available_at_ns > action.available_at_ns > CUTOFF
        assert result.evaluation.available_at_ns >= predictions[0].available_at_ns
        evaluation_entry = repo.get_artifact(result.evaluation_ref)
        assert CUTOFF < evaluation_entry.created_at_ns <= evaluation_entry.available_at_ns
        assert evaluation_entry.available_at_ns == result.evaluation.available_at_ns
        feature_entry = repo.get_artifact(candidate.snapshot_hash)
        assert (feature_entry.available_at_ns > CUTOFF) == late_feature
        calendar = DecisionCalendarEntryV2.from_dict(json_value(repo.get_artifact(result.calendar_ref).metadata["decision_entry"]))
        assert calendar.source_artifact_ref == result.evaluation_ref
        assert calendar.action_hash == action.action.action_hash and calendar.action_artifact_ref == action.content_hash
        assert calendar.candidate_ref == candidate.content_hash and calendar.candidate_set_ref == candidate_set.content_hash
        assert calendar.selection_state.value == "SELECTED" and calendar.admission_state == AdmissionStateV2.NOT_ESTIMABLE
        assert CUTOFF < calendar.available_at_ns < candidate.deadline_ns == event.deadline_ns
        assert result.evaluation.reason_codes
        if late_feature:
            recovered = production._recover_economic_evaluation(repo, action, candidate, candidate_set,
                now_ns=clock())
            assert recovered is not None and recovered.evaluation == result.evaluation
            assert recovered.evaluation_ref == result.evaluation_ref and recovered.calendar_ref == result.calendar_ref
            assert repo.get_artifact(result.evaluation_ref) == evaluation_entry


@pytest.mark.parametrize("synthetic_template", [False, True])
def test_installed_joint_template_binding_reaches_real_scenarios_and_flat_portfolio(tmp_path, synthetic_template):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        fixture = _fixture(repo, joint_template=True, synthetic_template=synthetic_template)
        event, case, candidate, candidate_set, _sizing, action, manifest, _risk, clock = fixture
        economic, reason = _resolve(repo, fixture)
        assert reason is None and economic is not None and economic.complete
        assert economic.joint_data_refs != manifest.joint_data_refs
        raw_template = JointExecutionDataV2.from_dict(json_value(repo.get_artifact(manifest.joint_data_refs[0]).metadata["joint_execution_data"]))
        bound_template = JointExecutionDataV2.from_dict(json_value(repo.get_artifact(economic.joint_data_refs[0]).metadata["joint_execution_data"]))
        assert bound_template == replace(raw_template, action_artifact_ref=action.content_hash,
            computed_at_ns=bound_template.computed_at_ns, available_at_ns=bound_template.available_at_ns)
        assert raw_template.available_at_ns == CUTOFF < action.available_at_ns < bound_template.available_at_ns
        bound_unit = ScenarioSupportUnitV2.from_dict(json_value(repo.get_artifact(economic.support_unit_refs[0]).metadata["evidence"]))
        assert bound_unit.source_episode_ref == raw_template.source_ref
        assert bound_unit.source_window_end_ns < CUTOFF
        result = _evaluate(repo, fixture, economic)
        # This is an offline contract fixture, never prospective qualification.
        # Explicit fixture-marked templates must stay excluded in production.
        if synthetic_template:
            assert result.scenario.status.value == "NOT_ESTIMABLE"
            assert result.scenario.rows == () and result.portfolio_es.status == "NOT_ESTIMABLE"
            assert result.scenario_support.independent_support_unit_count == 0
        else:
            assert result.scenario.status.value == "AVAILABLE"
            assert validate_pretrade_scenario_evidence(repo, action=action, scenario=result.scenario) == result.payoffs
            assert result.payoffs and len(result.scenario.rows) == 1
            assert result.scenario_support.independent_support_unit_count == 1
            assert result.scenario_support.evidence_quality == "SUPPORTED"
            assert result.scenario.scenario_count == 32  # Resampling creates no extra historical episodes.
            assert not case.account.existing_exposure_refs and not case.account.pending_risk_refs
            assert result.completeness.status == "COMPLETE" and result.portfolio.status == "AVAILABLE"
            assert result.portfolio_es.status == "AVAILABLE" and result.portfolio_es.es_before_fraction == Decimal(0)
        assert result.evaluation.decision.value == "NOT_ESTIMABLE"
        assert result.prediction.expected_net_value is None
        terminal = DecisionCalendarEntryV2.from_dict(json_value(repo.get_artifact(result.calendar_ref).metadata["decision_entry"]))
        assert terminal.action_artifact_ref == action.content_hash and terminal.candidate_ref == candidate.content_hash
        assert terminal.candidate_set_ref == candidate_set.content_hash
        assert terminal.available_at_ns < candidate.deadline_ns == event.deadline_ns


@pytest.mark.parametrize("declaration", ["missing", "future", "foreign", "ambiguous"])
def test_installed_selected_action_rejects_unusable_manifest(tmp_path, declaration):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        fixture = _fixture(repo, declaration=declaration)
        result, reason = _resolve(repo, fixture)
        assert result is None and reason is not None
        assert not repo.artifact_entries("OpsEconomicEvidenceResolutionV1")
        assert not repo.artifact_entries("EvaluationArtifactV2")


def test_late_legacy_exact_candidate_wrappers_cannot_replace_cutoff_declaration(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        fixture = _fixture(repo, declaration="missing")
        event, case, candidate, candidate_set, _sizing, _action, manifest, _risk, clock = fixture
        scope = {"event_id": event.event_id, "candidate_set_ref": candidate_set.content_hash,
            "candidate_ref": candidate.content_hash, "product_ref": case.product.content_hash,
            "available_at_ns": clock()}
        production.index_ops_admission_policy_evidence(repo, admission_policy=manifest.admission_policy, **scope)
        for role, item in (("M0", manifest.model_input), ("CALIBRATION", manifest.calibration_input),
                           ("EXECUTION_MODEL", manifest.execution_model_input)):
            production.index_ops_causal_input_evidence(repo, role=role, causal_input=item, **scope)
        production.index_ops_economic_scenario_config(repo, scenario_seed=7, scenario_count=32,
            evaluation_available_at_ns=clock(), **scope)
        result, reason = _resolve(repo, fixture)
        assert result is None and reason is not None
        assert not repo.artifact_entries("OpsEconomicEvidenceResolutionV1")


def test_installed_manifest_lost_source_is_rejected_before_derived_resolution(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        fixture = _fixture(repo)
        manifest = fixture[6]
        repo._connection.execute("DELETE FROM artifact_index WHERE artifact_ref=?", (manifest.execution_model_input.ref,))
        result, reason = _resolve(repo, fixture)
        assert result is None and reason == "ECONOMIC_SOURCE_MANIFEST_INVALID_OR_OVERFLOW"
        assert not repo.artifact_entries("OpsEconomicEvidenceResolutionV1")


def test_installed_resolution_restart_reuses_exact_binding_after_late_source_change(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        fixture = _fixture(repo)
        original, reason = _resolve(repo, fixture)
        assert original is not None and reason is None
        bindings = repo.artifact_entries("OpsEconomicEvidenceResolutionV1")
        assert len(bindings) == 1
        saved = bindings[0]
        _event, _case, _candidate, _candidate_set, _sizing, _action, manifest, _risk, _clock = fixture
        late = replace(manifest, available_at_ns=CUTOFF + 1000, effective_at_ns=CUTOFF + 1000,
                       scenario_count=64)
        index_economic_source_manifest(repo, late)
    with OpsRepository(path) as reopened:
        resumed, reason = _resolve(reopened, fixture, clock=itertools.count(CUTOFF + 2000).__next__)
        assert resumed == original and reason is None
        assert reopened.artifact_entries("OpsEconomicEvidenceResolutionV1") == (saved,)
        assert canonical_json(saved.metadata) == canonical_json(reopened.get_artifact(saved.artifact_ref).metadata)


def test_installed_prerequisite_inventory_and_compact_binding_keep_declared_identity(tmp_path):
    from atlas.v2.runtime.research_prerequisites import publish_research_prerequisites
    from atlas.v2.science.tuning_export import _validated_row

    with OpsRepository(tmp_path / 'ops.sqlite') as repo:
        fixture = _fixture(repo)
        event, case, _candidate, _candidate_set, _sizing, _action, manifest, _risk, clock = fixture
        supplied = production._indexed_public_prerequisite_evidence(repo, case.product, cutoff_ns=CUTOFF)
        assert supplied['EXECUTION_MODEL'] == manifest.execution_model_input
        inventory = publish_research_prerequisites(repo, event=event, product=case.product,
            clock_ns=clock, evidence=supplied)
        role = inventory.prerequisite_statuses['EXECUTION_MODEL']
        assert role['status'] == 'AVAILABLE' and role['evidence_ref'] == manifest.execution_model_input.ref
        # A declared source is neither account qualification nor a complete event calendar.
        assert inventory.status == 'NOT_ESTIMABLE'
        economic, reason = _resolve(repo, fixture)
        assert economic is not None and reason is None
        declaration = _validated_row(repo, repo.get_artifact(manifest.content_hash))
        bound = _validated_row(repo, repo.artifact_entries('OpsEconomicEvidenceResolutionV1')[0])
        assert declaration['method_config_hash'] == bound['method_config_hash'] == manifest.content_hash
        assert bound['action_artifact_ref'] == fixture[5].content_hash
        assert bound['information_cutoff_ns'] == CUTOFF
        assert 'account_scope' not in declaration
