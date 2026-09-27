"""One synthetic Phase-3 calendar to outcome path with unchanged hard authority."""

import hashlib
import json
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

from atlas.v2._serialization import sha256_json
from atlas.v2.data.derivatives import build_s5_crowding_context
from atlas.v2.data.microstructure import SequenceValidBookV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.news.events import S7DirectionalShadowV2
from atlas.v2.science.action import freeze_action
from atlas.v2.science.admission import ADMISSION_POLICY_VERSION, AdmissionPolicyV2, VenueCapabilitySnapshotV2
from atlas.v2.science.analogue import (
    AnalogueNotEstimableError,
    build_analogue_compatibility,
    build_analogue_query,
    estimate_causal_analogue,
    not_estimable_analogue,
    persist_analogue,
)
from atlas.v2.science.audits import (
    MultiplicityVariantV2,
    build_multiplicity_audit,
    build_selection_policy_audit,
    declare_feature_family_ablation,
    persist_research_artifact,
)
from atlas.v2.science.evaluation_service import run_phase2_economic_evaluation
from atlas.v2.science.m1 import fit_m1
from atlas.v2.science.outcomes import MaturedOutcomeV2, index_matured_outcome
from atlas.v2.science.phase3 import compare_frozen_action
from atlas.v2.science.pretrade import CausalInputV2
from atlas.v2.science.research_selection import research_sleeve_audit
from atlas.v2.strategies.s1_trend import S1_POLICY
from atlas.v2.strategies.s6_cross_section import S6ShadowCoordinator
from atlas.v2.strategies.s8_pairs import build_research_basket_forecast, persist_s8_basket

from .session023_support import research_case
from .test_session014_core import KEY
from .test_session017_replay import HOUR_NS, minute, replay_context, run
from .test_session017_risk import CUTOFF, size
from .test_session021_s7 import _event, _persist_news_event
from .test_session023_discovery_s8 import basket_fixture
from .test_session023_selection_audits import calendar


def test_phase3_research_calendar_selection_frozen_models_evaluator_outcome_audits(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = research_case(repo)
        assert len(case.candidate_set.candidates) == 3
        assert case.candidate_set.selected_candidate_id == case.candidate.candidate_id
        sized = size(repo, case)
        action = freeze_action(repo, candidate=case.candidate, candidate_set=case.candidate_set,
            sizing=sized, product=case.product, policy=S1_POLICY, v1=case.v1, v2=case.v2)
        original_action = action.to_dict()
        contexts = (
            SequenceValidBookV2(instrument=KEY, source_id="SESSION023_FIXTURE", channel="orderbook.50.BTCUSDT",
                sequence_semantics="BYBIT_U", warmup_ns=0, stale_ns=1000, declared_cadence_ns=10).feature(cutoff_ns=CUTOFF),
            build_s5_crowding_context(instrument=KEY, cutoff_ns=CUTOFF),
        )
        context_refs = tuple(persist_research_artifact(repo, type(item).__name__, item.to_dict(),
            available_at_ns=CUTOFF) for item in contexts)
        attached = {"version": "SESSION023_RESEARCH_CONTEXT_ATTACHMENT_V1", "action_hash": action.action.action_hash,
            "context_refs": list(context_refs), "hard_risk_influence": "ZERO"}
        persist_research_artifact(repo, "ResearchContextAttachmentV2", attached, available_at_ns=CUTOFF)
        assert size(repo, case).to_dict() == sized.to_dict()
        s6 = S6ShadowCoordinator(repo).evaluate(universe=case.universe, cutoff_ns=CUTOFF,
            btc_proxy=KEY, hourly_bars={}, four_hour_bars={}, evidence={})
        event = _event(received=CUTOFF - 2, extracted=CUTOFF - 1)
        _persist_news_event(repo, event)
        s7 = S7DirectionalShadowV2(repo).evaluate(event=event, key=KEY, first_bar=None, second_bar=None,
            history_bars=(), beta_to_btc=None, btc_return_5m=None, spread=None, source_health=None,
            bar_source_health=None, cutoff_ns=CUTOFF)
        assert not s6.hypotheses and s7.to_dict()["exact_action_status"] == "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT"
        assert repo.artifact_entries("EventReactionArtifactV2")
        pair, hourly_a, hourly_b, seed_basket = basket_fixture()
        basket_cutoff = seed_basket.information_cutoff_ns
        basket = build_research_basket_forecast(pair, prices_a=hourly_a, prices_b=hourly_b, cutoff_ns=basket_cutoff,
            leg_a_evidence=seed_basket.leg_a_evidence, leg_b_evidence=seed_basket.leg_b_evidence)
        persist_s8_basket(repo, pair, basket, available_at_ns=basket_cutoff)
        exclusions = research_sleeve_audit(available_at_ns=CUTOFF)
        persist_research_artifact(repo, "ResearchSleeveSelectionAuditV2", exclusions.to_dict(), available_at_ns=CUTOFF)

        def causal(kind):
            body = {"kind": kind, "decision_cutoff_ns": CUTOFF, "synthetic": True}
            ref = sha256_json(body)
            repo.register_artifact(ArtifactIndexEntryV2(ref, kind, ref, CUTOFF, CUTOFF, body))
            return CausalInputV2(ref, kind, CUTOFF, CUTOFF)

        profiles = tuple(sha256_json(["session023-profile", name]) for name in ("runtime", "execution", "protection"))
        admission = AdmissionPolicyV2(ADMISSION_POLICY_VERSION, Decimal(1), 30, 30, 20, Decimal("1.96"),
            "ISOLATED", "ONE_WAY", "nautilus_trader", "2.0.0rc5", "1b0a49d2792a9432a3aca3fcb617ce7a630d905e",
            *profiles, "SESSION023_CAPABILITY_PROFILE_V1")
        capability = VenueCapabilitySnapshotV2(KEY.venue, KEY.environment, case.account.account_scope,
            action.action.product_ref, KEY.content_hash, "ISOLATED", "ONE_WAY", "nautilus_trader", "2.0.0rc5",
            "1b0a49d2792a9432a3aca3fcb617ce7a630d905e", *profiles, "SESSION023_CAPABILITY_PROFILE_V1", "UNVERIFIED", (), CUTOFF)
        evaluated = run_phase2_economic_evaluation(repo, action=action, candidate=case.candidate,
            candidate_set=case.candidate_set, sizing=sized, product=case.product, risk_policy=case.v1,
            risk_policy_v2=case.v2, account=case.account, fee=case.fee, admission_policy=admission,
            capability=capability, model_input=causal("Session023ModelFixtureV1"),
            calibration_input=causal("Session023CalibrationFixtureV1"), execution_model_input=causal("Session023ExecutionFixtureV1"),
            available_at_ns=CUTOFF + 10, scenario_seed=23023, scenario_count=100)
        challenger = fit_m1(repo, action=action, candidate=case.candidate, candidate_set=case.candidate_set,
            cutoff_ns=CUTOFF, available_at_ns=CUTOFF + 1,
            dependency_lock_hash=hashlib.sha256(Path("requirements-lock.txt").read_bytes()).hexdigest())
        names = ("action.quantity_log_notional", "value:h1.roc10")
        try:
            compat = build_analogue_compatibility(repo, action_ref=action.content_hash)
            query = build_analogue_query(repo, action_ref=action.content_hash, candidate_ref=case.candidate.content_hash,
                candidate_set_ref=case.candidate_set.content_hash, cutoff_ns=CUTOFF, compatibility=compat,
                feature_names=names, regime_id="UNKNOWN")
            analogue = estimate_causal_analogue(repo, query, (), compatibility_contracts={compat.compatibility_key: compat})
        except AnalogueNotEstimableError as error:
            analogue = not_estimable_analogue(action_ref=action.content_hash,
                candidate_ref=case.candidate.content_hash, candidate_set_ref=case.candidate_set.content_hash,
                action_hash=action.action.action_hash, information_cutoff_ns=CUTOFF, reason=error.reason)
        persist_analogue(repo, analogue, available_at_ns=CUTOFF + 1)
        comparison = compare_frozen_action(m0=evaluated.prediction, m1=challenger, analogue=analogue)
        persist_research_artifact(repo, "FrozenActionModelComparisonV2", comparison.to_dict(), available_at_ns=CUTOFF + 10)
        assert evaluated.evaluation.decision.value == "NOT_ESTIMABLE"
        assert comparison.status == challenger.prediction.status == "NOT_ESTIMABLE"
        assert action.to_dict() == original_action

        for item in case.competitors[1:]:
            calendar(repo, case.candidate_set, item, "UNSELECTED")
        context = replay_context(repo, case, action_override=action, minutes=(minute(CUTOFF, ask_depth="0"),
            minute(CUTOFF + 4 * HOUR_NS, ask_depth="0")))
        payoff = run(repo, case, context)
        assert payoff.status.value == "NO_FILL"
        outcome = MaturedOutcomeV2(decision_ref=evaluated.calendar_ref, candidate_set_ref=case.candidate_set.content_hash,
            candidate_ref=case.candidate.content_hash, policy_id=S1_POLICY.policy_id, policy_version=S1_POLICY.version,
            policy_hash=S1_POLICY.policy_hash, action_hash=action.action.action_hash, action_artifact_ref=action.content_hash,
            action_absence_reason=None, instrument_revision=KEY.contract_revision, venue=KEY.venue.value,
            product=KEY.product.value, decision_at_ns=CUTOFF, horizon_end_ns=case.candidate.horizon_end_ns,
            matured_at_ns=payoff.available_at_ns, available_at_ns=payoff.available_at_ns + 1,
            label_definition="net_action_value_v2", label_view="RECONSTRUCTED_MARKET", selection_state="SELECTED",
            admission_state="NOT_ESTIMABLE", execution_state="NO_FILL", label_state="MATURED", provenance="SIMULATED",
            payoff_unit="USDT", quantity_unit="CONTRACTS", gross_payoff=Decimal(0), fees=Decimal(0),
            funding_cashflow=Decimal(0), net_payoff=Decimal(0), fill_quantity=Decimal(0), requested_quantity=action.action.quantity,
            mfe=None, mae=None, evidence_refs=(payoff.content_hash,), execution_evidence_ref=payoff.content_hash,
            extrema_evidence_ref=None, actual_closed_source_ref=None, evidence_resolution="MINUTE", evidence_quality="REPLAY_BOUND",
            ambiguity=(), outcome_target="EXECUTABLE_ACTION_VALUE", diagnostic_value=None, diagnostic_unit=None,
            diagnostic_evidence_ref=None, actual_action_binding_ref=None)
        index_matured_outcome(repo, outcome)
        audit = build_selection_policy_audit(repo, audit_id="session023-integration", start_ns=CUTOFF - 1,
            end_ns=outcome.available_at_ns + 1)
        persist_research_artifact(repo, "SelectionPolicyAuditV2", audit.to_dict(), available_at_ns=outcome.available_at_ns + 1)
        assert audit.status == "NOT_ESTIMABLE" and dict(audit.fill_state_counts) == {"NO_FILL": 1}
        assert any(row.selection_state == "UNSELECTED" and not row.execution_states for row in audit.rows)
        variants = tuple(MultiplicityVariantV2(name, sha256_json(name), (), (), "INSUFFICIENT_GENUINE_HISTORY")
            for name in ("M0", "M1", "ANALOGUE", "MULTI_SLEEVE"))
        multiplicity = build_multiplicity_audit(family_id="session023-integration", preregistration_ref=sha256_json("design"),
            baseline_variant_id="M0", variants=variants, expected_family_member_ids=tuple(v.variant_id for v in variants))
        persist_research_artifact(repo, "MultiplicityAuditV2", multiplicity.to_dict(), available_at_ns=outcome.available_at_ns + 1)
        assert multiplicity.status == "NOT_ESTIMABLE"
        refs = {name: sha256_json(name) for name in ("baseline_policy_ref", "scanner_ref", "candidate_generation_ref",
            "selection_ref", "sizing_ref", "execution_assumptions_ref", "costs_ref", "no_fill_partial_fill_ref", "latency_ref", "gate_ref", "multiplicity_family_ref")}
        ablation = declare_feature_family_ablation(audit_id="session023-integration", family_id="session023-integration", **refs)
        persist_research_artifact(repo, "FeatureFamilyAblationAuditV2", ablation.to_dict(), available_at_ns=outcome.available_at_ns + 1)
        for kind in ("TradePlanEnvelopeV2", "OrderIntentV2", "CapitalReservationV2"):
            assert repo.artifact_entries(kind) == ()


def test_v1_writer_and_research_contract_imports_do_not_require_lightgbm():
    code = """
import builtins, sys
real_import = builtins.__import__
def guarded(name, *args, **kwargs):
    if name.split('.')[0] in {'lightgbm', 'numpy', 'scipy'}:
        raise ImportError('offline dependencies unavailable')
    return real_import(name, *args, **kwargs)
builtins.__import__ = guarded
import atlas.runtime.writer_lock
import atlas.runtime.safe_runtime
import atlas.runtime.coordinator
import atlas.runtime.__main__
import atlas.v2.science.m1
import atlas.v2.science.analogue
import atlas.v2.science.discovery
assert 'lightgbm' not in sys.modules
"""
    result = subprocess.run([sys.executable, "-c", code], env={"PYTHONPATH": "src"}, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_checked_in_research_design_matches_code_and_retained_attempts(tmp_path):
    from atlas.v2._serialization import json_value
    from atlas.v2.science.session023_report import build_session023_research_report

    stored = json.loads(Path("docs/v2/SESSION023_RESEARCH_DESIGN.json").read_text())
    with OpsRepository(tmp_path / "design.sqlite") as repo:
        regenerated = build_session023_research_report(repo, preregistered_at_ns=stored["experiment"]["preregistered_at_ns"])
        assert json_value(regenerated) == stored


def test_engineering_gate_requires_all_checks_and_never_enables_capital():
    from atlas.v2._serialization import FrozenMap
    from atlas.v2.science.phase3 import GATE_CHECKS, Phase3EngineeringGateV2

    checks = dict.fromkeys(GATE_CHECKS, True)
    checks["tier_c_passed"] = False
    gate = Phase3EngineeringGateV2("09efb89ff99e156fb39c7c0a89c5a86f639ccf47", FrozenMap(checks),
        sha256_json("engineering-evidence"), FrozenMap({}))
    assert gate.to_dict()["phase3_engineering"] == "UNVERIFIED"
    checks["tier_c_passed"] = True
    tested = Phase3EngineeringGateV2(gate.starting_sha, FrozenMap(checks), gate.validation_manifest_ref, gate.preserved_identities)
    assert tested.to_dict()["phase3_engineering"] == "TESTED"
    assert tested.to_dict()["economic_value"] == "NOT ESTIMABLE"
    assert not tested.to_dict()["capital_enabled"] and not tested.to_dict()["session024_started"]
