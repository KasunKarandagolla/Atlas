"""Negative provenance and causal-lineage regressions for Session-018 review."""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.domain.money import canonical_decimal_str
from atlas.v2._serialization import json_value, sha256_json
from atlas.v2.contracts import CandidateSelectionStatus, EligibilityStatusV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.risk import ACTUAL_CLOSE_PROVENANCE, ActualClosedPositionSourceV2, index_research_evidence
from atlas.v2.science.outcomes import (
    ActualActionPositionBindingV2,
    AdmissionStateV2,
    CandidateExpiryEvidenceV2,
    DecisionCalendarEntryV2,
    DecisionSourceStageV2,
    DiagnosticTargetEvidenceV2,
    ExecutionOutcomeStateV2,
    LabelStateV2,
    MaturedOutcomeV2,
    OutcomeProvenanceV2,
    OutcomeTargetV2,
    SelectionStateV2,
    executable_action_value_training_eligible,
    index_actual_action_position_binding,
    index_candidate_expiry_evidence,
    index_decision_calendar_entry,
    index_diagnostic_target_evidence,
    index_matured_outcome,
    matured_diagnostic_eligible,
    realized_risk_source,
)
from atlas.v2.science.pretrade import (
    JOINT_DIMENSIONS,
    CausalInputV2,
    JointScenarioDataV2,
    JointScenarioPayloadV2,
    JointScenarioPointV2,
    JointScenarioV2,
    PretradeDerivedEvidenceV2,
    PretradeScenarioArtifactV2,
    ScenarioFillStateV2,
    ScenarioStatusV2,
    index_joint_scenario_data,
    index_joint_scenario_payload,
    index_pretrade_derived_evidence,
    index_pretrade_scenario,
)
from atlas.v2.science.replay import HOUR_NS, ReplayStatusV2

from .test_session014_core import KEY
from .test_session017_replay import minute, replay_context, run
from .test_session017_risk import CUTOFF, actual_outcome, risk_case


def _index(repo, ref, kind, at, body):
    repo.register_artifact(ArtifactIndexEntryV2(ref, kind, ref, at, at, body))


def _index_fixture_candidate_set(repo, candidate_set, identity):
    _index(repo, candidate_set.content_hash, "CandidateSetV2", CUTOFF,
           {"candidate_set": candidate_set.to_dict(), "identity": identity})
    decision_index_ref = sha256_json({"artifact_type": "CandidateSetDecisionIndexV1",
        "decision_event_id": candidate_set.decision_event_id, "universe_ref": candidate_set.universe_ref,
        "selection_policy_hash": candidate_set.selection_policy_hash})
    body = {"decision_event_id": candidate_set.decision_event_id,
        "universe_ref": candidate_set.universe_ref,
        "selection_policy_hash": candidate_set.selection_policy_hash,
        "cutoff_ns": CUTOFF, "candidate_set_ref": candidate_set.content_hash}
    repo.register_artifact(ArtifactIndexEntryV2(decision_index_ref, "CandidateSetDecisionIndexV1",
        sha256_json(body), CUTOFF, CUTOFF, body))


def _payoff_case(repo, *, entry_depth="100", entry_ask="100",
                 admission_state=AdmissionStateV2.RISK_SIZED):
    case = risk_case(repo)
    context = replay_context(repo, case, minutes=(
        minute(CUTOFF, ask=entry_ask, ask_depth=entry_depth),
        minute(CUTOFF + 4 * HOUR_NS, bid="110", ask="110", mark_low="110", mark_high="110",
               last_low="110", last_high="110")))
    payoff = run(repo, case, context)
    action = context[0]
    fees = (payoff.entry.fee if payoff.entry else Decimal(0)) + sum((x.fee for x in payoff.exits), Decimal(0))
    funding = sum((cash for _, cash in payoff.funding_cashflows), Decimal(0))
    assert payoff.payoff is not None
    if admission_state == AdmissionStateV2.RISK_SIZED:
        source_stage, decision_source, decision_at = DecisionSourceStageV2.HARD_RISK, action.sizing_ref, CUTOFF
    else:
        assert admission_state in (AdmissionStateV2.NO_TRADE, AdmissionStateV2.NOT_ESTIMABLE)
        eval_body = {"version": "EVALUATION_ARTIFACT_V2_AMENDED_V1",
            "candidate_set_ref": case.candidate_set.content_hash,
            "candidate_ref": case.candidate.content_hash,
            "action_hash": action.action.action_hash, "action_artifact_ref": action.content_hash,
            "policy_hash": action.action.policy_hash,
            "decision_at_ns": CUTOFF, "decision": admission_state.value, "reason_codes": [],
            "available_at_ns": CUTOFF + 1}
        decision_source = sha256_json(eval_body)
        _index(repo, decision_source, "EvaluationArtifactV2", CUTOFF + 1,
               {"evaluation": eval_body})
        source_stage, decision_at = DecisionSourceStageV2.ECONOMIC_EVALUATION, CUTOFF + 1
    decision = DecisionCalendarEntryV2(
        candidate_set_ref=case.candidate_set.content_hash,
        candidate_ref=case.candidate.content_hash,
        policy_id=action.action.policy_id,
        policy_version=action.action.policy_version,
        policy_hash=action.action.policy_hash,
        decision_at_ns=CUTOFF,
        selection_state=SelectionStateV2.SELECTED,
        admission_state=admission_state,
        action_hash=action.action.action_hash,
        action_artifact_ref=action.content_hash,
        source_stage=source_stage,
        reason_codes=(), source_artifact_ref=decision_source,
        created_at_ns=decision_at, available_at_ns=decision_at)
    assert DecisionCalendarEntryV2.from_dict(decision.to_dict()) == decision
    decision_ref = index_decision_calendar_entry(repo, decision)
    item = MaturedOutcomeV2(
        decision_ref=decision_ref, candidate_set_ref=case.candidate_set.content_hash,
        candidate_ref=case.candidate.content_hash, policy_id=action.action.policy_id,
        policy_version=action.action.policy_version, policy_hash=action.action.policy_hash,
        action_hash=action.action.action_hash, action_artifact_ref=action.content_hash,
        action_absence_reason=None, instrument_revision=KEY.contract_revision, venue=KEY.venue.value,
        product=KEY.product.value, decision_at_ns=CUTOFF, horizon_end_ns=case.candidate.horizon_end_ns,
        matured_at_ns=payoff.available_at_ns, available_at_ns=payoff.available_at_ns + 1,
        label_definition="net_action_value_v2", label_view="RECONSTRUCTED_MARKET",
        selection_state=SelectionStateV2.SELECTED, admission_state=admission_state,
        execution_state=ExecutionOutcomeStateV2(payoff.status.value), label_state=LabelStateV2.MATURED,
        provenance=OutcomeProvenanceV2.SIMULATED, payoff_unit="USDT", quantity_unit="CONTRACTS",
        gross_payoff=payoff.payoff + fees - funding, fees=fees, funding_cashflow=funding,
        net_payoff=payoff.payoff, fill_quantity=payoff.filled_quantity,
        requested_quantity=action.action.quantity, mfe=None, mae=None,
        evidence_refs=(payoff.content_hash,), execution_evidence_ref=payoff.content_hash,
        extrema_evidence_ref=None, actual_closed_source_ref=None, evidence_resolution="MINUTE",
        evidence_quality="REPLAY_BOUND", ambiguity=(), outcome_target=OutcomeTargetV2.EXECUTABLE_ACTION_VALUE,
        diagnostic_value=None, diagnostic_unit=None, diagnostic_evidence_ref=None,
        actual_action_binding_ref=None)
    return case, action, payoff, item


def test_matured_unselected_rejected_diagnostic_and_separate_eligibility(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo, include_s2=True)
        assert case.s2_candidate is not None
        selected_id = case.candidate_set.selected_candidate_id
        candidate = case.candidate if case.candidate.candidate_id != selected_id else case.s2_candidate
        candidate_entry = next(x for x in case.candidate_set.candidates if x.candidate_id == candidate.candidate_id)
        body = {"target": "future_mid_return_v1", "candidate_ref": candidate.content_hash,
                "horizon_end_ns": CUTOFF + HOUR_NS, "value": "0.02", "unit": "FRACTION"}
        source_ref = sha256_json(body)
        index_research_evidence(repo, "CausalMarketDiagnosticV2", source_ref, CUTOFF + HOUR_NS + 1, body)
        definition = {"label_definition": "future_mid_return_v1", "unit": "FRACTION"}
        definition_ref = sha256_json(definition)
        index_research_evidence(repo, "DiagnosticTargetDefinitionV2", definition_ref, CUTOFF, definition)
        late_definition = {"label_definition": "future_mid_return_v1", "unit": "FRACTION", "revision": 2}
        late_ref = sha256_json(late_definition)
        index_research_evidence(repo, "DiagnosticTargetDefinitionV2", late_ref, CUTOFF + 1, late_definition)
        original_index = repo.get_artifact(case.candidate_set.content_hash)
        assert original_index is not None
        set_identity = dict(original_index.metadata["identity"])
        unselected_decision = None
        for state in (SelectionStateV2.UNSELECTED, SelectionStateV2.REJECTED):
            candidate_set = case.candidate_set
            if state == SelectionStateV2.REJECTED:
                rejected_entry = replace(candidate_entry, eligibility_status=EligibilityStatusV2.INELIGIBLE,
                                         rejection_reason="POLICY_INPUT_GATE")
                rejected_identity = {**set_identity, "decision_event_id": "rejected-diagnostic-fixture"}
                candidate_set = replace(candidate_set, decision_event_id="rejected-diagnostic-fixture",
                    envelope=replace(candidate_set.envelope,
                        artifact_id=sha256_json(rejected_identity), content_hash=""),
                    candidates=tuple(
                    rejected_entry if x.candidate_id == candidate.candidate_id else x
                    for x in candidate_set.candidates))
                _index_fixture_candidate_set(repo, candidate_set, rejected_identity)
            decision = DecisionCalendarEntryV2(candidate_set.content_hash, candidate.content_hash,
                candidate_entry.policy_id, "1", candidate.policy_hash, CUTOFF, state,
                AdmissionStateV2.NOT_APPLICABLE, None, None, DecisionSourceStageV2.CANDIDATE_SET,
                ("POLICY_INPUT_GATE",) if state == SelectionStateV2.REJECTED else (),
                    candidate_set.content_hash, CUTOFF, CUTOFF)
            if state == SelectionStateV2.UNSELECTED:
                unselected_decision = decision
            decision_ref = index_decision_calendar_entry(repo, decision)
            diagnostic = DiagnosticTargetEvidenceV2(decision_ref, candidate_set.content_hash,
                candidate.content_hash, "future_mid_return_v1", definition_ref, CUTOFF, CUTOFF + HOUR_NS,
                Decimal("0.02"), "FRACTION", (source_ref,), CUTOFF + HOUR_NS + 1, CUTOFF + HOUR_NS + 1)
            with pytest.raises(ValueError):
                index_diagnostic_target_evidence(repo, replace(diagnostic, target_declaration_ref=late_ref))
            index_diagnostic_target_evidence(repo, diagnostic)
            item = MaturedOutcomeV2(
                decision_ref=decision_ref, candidate_set_ref=candidate_set.content_hash,
                candidate_ref=candidate.content_hash, policy_id=candidate_entry.policy_id,
                policy_version="1", policy_hash=candidate.policy_hash,
                action_hash=None, action_artifact_ref=None, action_absence_reason="NO_FROZEN_ACTION",
                instrument_revision=KEY.contract_revision, venue=KEY.venue.value, product=KEY.product.value,
                decision_at_ns=CUTOFF, horizon_end_ns=CUTOFF + HOUR_NS,
                matured_at_ns=CUTOFF + HOUR_NS + 1, available_at_ns=CUTOFF + HOUR_NS + 2,
                label_definition="future_mid_return_v1", label_view="RECONSTRUCTED_MARKET",
                selection_state=state, admission_state=AdmissionStateV2.NOT_APPLICABLE,
                execution_state=ExecutionOutcomeStateV2.NOT_APPLICABLE,
                label_state=LabelStateV2.MATURED, provenance=OutcomeProvenanceV2.COUNTERFACTUAL,
                payoff_unit="USDT", quantity_unit="CONTRACTS", gross_payoff=None, fees=None,
                funding_cashflow=None, net_payoff=None, fill_quantity=None, requested_quantity=None,
                mfe=None, mae=None, evidence_refs=tuple(sorted((diagnostic.content_hash, source_ref))),
                execution_evidence_ref=None, extrema_evidence_ref=None, actual_closed_source_ref=None,
                evidence_resolution="FINAL_BAR", evidence_quality="CAUSAL_MARKET_ONLY", ambiguity=(),
                outcome_target=OutcomeTargetV2.NON_EXECUTABLE_DIAGNOSTIC,
                diagnostic_value=Decimal("0.02"), diagnostic_unit="FRACTION",
                diagnostic_evidence_ref=diagnostic.content_hash, actual_action_binding_ref=None)
            assert MaturedOutcomeV2.from_dict(item.to_dict()) == item
            assert index_matured_outcome(repo, item) == item.content_hash
            assert matured_diagnostic_eligible(item, item.available_at_ns)
            assert not matured_diagnostic_eligible(item, item.available_at_ns - 1)
            assert not executable_action_value_training_eligible(item, item.available_at_ns)
            assert not executable_action_value_training_eligible(item, item.available_at_ns + 1)
            with pytest.raises(ValueError):
                replace(item, fill_quantity=Decimal("1"))
            with pytest.raises(ValueError):
                replace(item, net_payoff=Decimal("1"))
            with pytest.raises(ValueError):
                index_matured_outcome(repo, replace(item, diagnostic_value=Decimal("0.03")))
        assert unselected_decision is not None
        for changes in (
            {"selection_state": SelectionStateV2.REJECTED, "reason_codes": ("POLICY_INPUT_GATE",)},
            {"selection_state": SelectionStateV2.SELECTED, "admission_state": AdmissionStateV2.NOT_EVALUATED},
            {"admission_state": AdmissionStateV2.NO_TRADE, "source_stage": DecisionSourceStageV2.HARD_RISK},
            {"admission_state": AdmissionStateV2.NOT_ESTIMABLE, "source_stage": DecisionSourceStageV2.HARD_RISK},
        ):
            with pytest.raises(ValueError):
                index_decision_calendar_entry(repo, replace(unselected_decision, **changes))
        censored = replace(item, label_state=LabelStateV2.CENSORED, diagnostic_value=None,
                           diagnostic_evidence_ref=None,
                           reason="INSUFFICIENT_MARKET_EVIDENCE")
        assert not matured_diagnostic_eligible(censored, censored.available_at_ns)
        no_candidate_identity = {**set_identity, "decision_event_id": "no-candidate-fixture",
                                 "candidate_refs": [], "attempted_scanner_evidence_refs": {},
                                 "validated_scanner_source_refs": [],
                                 "causal_input_refs": sorted((case.candidate_set.universe_ref,
                                                               case.candidate_set.selection_policy_hash))}
        empty = replace(case.candidate_set, decision_event_id="no-candidate-fixture",
                        envelope=replace(case.candidate_set.envelope,
                            artifact_id=sha256_json(no_candidate_identity), content_hash="",
                            input_refs=tuple(no_candidate_identity["causal_input_refs"])),
                        candidates=(), selected_candidate_id=None,
                        selection_status=CandidateSelectionStatus.NO_CANDIDATE)
        _index_fixture_candidate_set(repo, empty, no_candidate_identity)
        no_candidate = DecisionCalendarEntryV2(empty.content_hash, None, "CANDIDATE_SELECTION", "1",
            empty.selection_policy_hash, CUTOFF, SelectionStateV2.NO_CANDIDATE,
            AdmissionStateV2.NOT_APPLICABLE, None, None, DecisionSourceStageV2.CANDIDATE_SET,
            (), empty.content_hash, CUTOFF, CUTOFF)
        no_candidate_ref = index_decision_calendar_entry(repo, no_candidate)
        no_candidate_label = MaturedOutcomeV2(
            decision_ref=no_candidate_ref, candidate_set_ref=empty.content_hash, candidate_ref=None,
            policy_id="CANDIDATE_SELECTION", policy_version="1", policy_hash=empty.selection_policy_hash,
            action_hash=None, action_artifact_ref=None, action_absence_reason="NO_CANDIDATE",
            instrument_revision=None, venue=None, product=None, decision_at_ns=CUTOFF,
            horizon_end_ns=CUTOFF + HOUR_NS, matured_at_ns=CUTOFF + HOUR_NS,
            available_at_ns=CUTOFF + HOUR_NS, label_definition="calendar", label_view="RECONSTRUCTED_MARKET",
            selection_state=SelectionStateV2.NO_CANDIDATE, admission_state=AdmissionStateV2.NOT_APPLICABLE,
            execution_state=ExecutionOutcomeStateV2.NOT_APPLICABLE, label_state=LabelStateV2.CENSORED,
            provenance=OutcomeProvenanceV2.COUNTERFACTUAL, payoff_unit="USDT", quantity_unit="CONTRACTS",
            gross_payoff=None, fees=None, funding_cashflow=None, net_payoff=None, fill_quantity=None,
            requested_quantity=None, mfe=None, mae=None, evidence_refs=(), execution_evidence_ref=None,
            extrema_evidence_ref=None, actual_closed_source_ref=None, evidence_resolution="NONE",
            evidence_quality="NO_CANDIDATE", ambiguity=(), outcome_target=OutcomeTargetV2.NON_EXECUTABLE_DIAGNOSTIC,
            diagnostic_value=None, diagnostic_unit=None, diagnostic_evidence_ref=None,
            actual_action_binding_ref=None, reason="NO_CANDIDATE")
        assert index_matured_outcome(repo, no_candidate_label) == no_candidate_label.content_hash
        with pytest.raises(ValueError):
            MaturedOutcomeV2.from_dict({**item.to_dict(), "version": "future"})
        with pytest.raises(ValueError):
            MaturedOutcomeV2.from_dict({**item.to_dict(), "version": "MATURED_OUTCOME_V2_V2"})
        with pytest.raises(ValueError):
            MaturedOutcomeV2.from_dict({**item.to_dict(), "unknown": 1})
        with pytest.raises(ValueError):
            MaturedOutcomeV2.from_dict({**item.to_dict(), "diagnostic_value": "0.020"})
        with pytest.raises(ValueError):
            replace(item, outcome_target=OutcomeTargetV2.EXECUTABLE_ACTION_VALUE)


def test_exact_policy_payoff_fill_components_and_generic_rejection(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case, action, payoff, item = _payoff_case(repo, entry_depth="10")
        assert payoff.status == ReplayStatusV2.PARTIAL_FILL
        assert index_matured_outcome(repo, item) == item.content_hash
        assert executable_action_value_training_eligible(item, item.available_at_ns)
        assert not executable_action_value_training_eligible(item, item.available_at_ns - 1)
        assert MaturedOutcomeV2.from_dict(item.to_dict()).content_hash == item.content_hash
        with pytest.raises(ValueError):
            replace(item, matured_at_ns=item.horizon_end_ns - 1)
        with pytest.raises(ValueError):
            replace(item, available_at_ns=item.matured_at_ns - 1)
        for changed in (replace(item, decision_ref=sha256_json("another-decision")),
                        replace(item, provenance=OutcomeProvenanceV2.COUNTERFACTUAL),
                        replace(item, gross_payoff=item.gross_payoff + Decimal("1"),
                                net_payoff=item.net_payoff + Decimal("1")),
                        replace(item, evidence_refs=tuple(sorted((item.execution_evidence_ref,
                                                                   sha256_json("extra-evidence")))))):
            assert changed.content_hash != item.content_hash
        generic = sha256_json({"fixture": 1})
        _index(repo, generic, "ReplayExecutionV2", item.matured_at_ns, {"fixture": 1})
        with pytest.raises(ValueError):
            index_matured_outcome(repo, replace(item, execution_evidence_ref=generic,
                evidence_refs=(generic,)))
        with pytest.raises(ValueError):
            index_matured_outcome(repo, replace(item, provenance=OutcomeProvenanceV2.COUNTERFACTUAL,
                execution_evidence_ref=generic, evidence_refs=(generic,)))
        for change in ({"action_hash": sha256_json("other-action")},
                       {"fill_quantity": item.fill_quantity + Decimal("1")},
                       {"execution_state": ExecutionOutcomeStateV2.FULL_FILL},
                       {"gross_payoff": item.gross_payoff + Decimal("1"), "net_payoff": item.net_payoff + Decimal("1")},
                       {"fees": item.fees + Decimal("1"), "net_payoff": item.net_payoff - Decimal("1")},
                       {"funding_cashflow": Decimal("1"), "net_payoff": item.net_payoff + Decimal("1")}):
            with pytest.raises(ValueError):
                index_matured_outcome(repo, replace(item, **change))
        with pytest.raises(ValueError):
            replace(item, provenance=OutcomeProvenanceV2.COUNTERFACTUAL,
                    action_hash=None, action_artifact_ref=None, action_absence_reason="REJECTED")
        # A content-addressed frozen action B still cannot use payoff A.
        indexed_action = repo.get_artifact(action.content_hash)
        assert indexed_action is not None
        identity_b = json_value(indexed_action.metadata["action_identity"])
        identity_b["quantity"] = canonical_decimal_str(action.action.quantity + Decimal("0.1"))
        action_hash_b = sha256_json(identity_b)
        artifact_b = json_value(indexed_action.metadata["action_artifact"])
        artifact_b["action_hash"] = action_hash_b
        action_ref_b = sha256_json(artifact_b)
        _index(repo, action_ref_b, "ActionArtifactV2", CUTOFF,
               {"action_artifact": artifact_b, "action_identity": identity_b})
        other = replace(item, action_hash=action_hash_b, action_artifact_ref=action_ref_b)
        with pytest.raises(ValueError):
            index_matured_outcome(repo, other)
        assert case.candidate_set.content_hash == item.candidate_set_ref
        assert action.content_hash == payoff.action_artifact_ref


@pytest.mark.parametrize("admission", (AdmissionStateV2.NO_TRADE, AdmissionStateV2.NOT_ESTIMABLE))
@pytest.mark.parametrize(("entry_depth", "entry_ask", "expected"), (
    ("100", "101", ExecutionOutcomeStateV2.NO_FILL),
    ("10", "100", ExecutionOutcomeStateV2.PARTIAL_FILL),
    ("100", "100", ExecutionOutcomeStateV2.FULL_FILL),
))
def test_frozen_action_retains_original_admission_and_matures_counterfactual_fill(
        tmp_path, admission, entry_depth, entry_ask, expected):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _, _, payoff, item = _payoff_case(repo, entry_depth=entry_depth, entry_ask=entry_ask,
                                          admission_state=admission)
        provenance = (OutcomeProvenanceV2.COUNTERFACTUAL if expected == ExecutionOutcomeStateV2.PARTIAL_FILL
                      else OutcomeProvenanceV2.SIMULATED)
        item = replace(item, provenance=provenance)
        assert item.selection_state == SelectionStateV2.SELECTED
        assert item.admission_state == admission
        assert item.execution_state == expected
        assert payoff.status.value == expected.value
        assert item.action_hash is not None and item.action_artifact_ref is not None
        assert index_matured_outcome(repo, item) == item.content_hash
        assert executable_action_value_training_eligible(item, item.available_at_ns)
        assert not executable_action_value_training_eligible(item, item.available_at_ns - 1)


def test_decision_calendar_requires_exact_candidate_sizing_and_admission_sources(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case, action, _, outcome = _payoff_case(repo)
        decision = repo.get_artifact(outcome.decision_ref)
        assert decision is not None and decision.artifact_type == "DecisionCalendarEntryV2"
        entry_body = decision.metadata["decision_entry"]
        entry = DecisionCalendarEntryV2.from_dict(json_value(entry_body))
        assert index_decision_calendar_entry(repo, entry) == entry.content_hash
        with pytest.raises(ValueError):
            index_matured_outcome(repo, replace(outcome, decision_ref=sha256_json("arbitrary-decision-ref")))
        with pytest.raises(ValueError):
            index_matured_outcome(repo, replace(outcome, admission_state=AdmissionStateV2.NO_TRADE))

        # A candidate selected by the set cannot be relabelled UNSELECTED.
        bad_unselected = DecisionCalendarEntryV2(entry.candidate_set_ref, entry.candidate_ref,
            entry.policy_id, entry.policy_version, entry.policy_hash, entry.decision_at_ns,
            SelectionStateV2.UNSELECTED, AdmissionStateV2.NOT_APPLICABLE, None, None,
            DecisionSourceStageV2.CANDIDATE_SET, (), entry.candidate_set_ref, CUTOFF, CUTOFF)
        with pytest.raises(ValueError):
            index_decision_calendar_entry(repo, bad_unselected)

        # Hard-risk evidence must be the exact SizingDecision body referenced by the action.
        sizing = repo.get_artifact(action.sizing_ref)
        assert sizing is not None
        changed_body = json_value(sizing.metadata["sizing"])
        changed_body["status"] = "NO_TRADE"
        fake_ref = sha256_json(changed_body)
        _index(repo, fake_ref, "SizingDecisionV2", CUTOFF, {"sizing": changed_body})
        with pytest.raises(ValueError):
            index_decision_calendar_entry(repo, replace(entry, source_artifact_ref=fake_ref,
                admission_state=AdmissionStateV2.NO_TRADE, action_hash=None,
                action_artifact_ref=None, reason_codes=()))

        # A future Session-019 evaluation state cannot be supplied as an enum alone.
        unindexed_eval = sha256_json({"version": "EVALUATION_ARTIFACT_V2_AMENDED_V1"})
        future_decision = replace(entry, admission_state=AdmissionStateV2.NOT_ESTIMABLE,
            source_stage=DecisionSourceStageV2.ECONOMIC_EVALUATION,
            source_artifact_ref=unindexed_eval, reason_codes=())
        with pytest.raises(ValueError):
            index_decision_calendar_entry(repo, future_decision)


def test_candidate_expiry_is_typed_and_available_after_deadline(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        candidate = case.candidate
        expired_at = candidate.deadline_ns + 1
        evidence = CandidateExpiryEvidenceV2(case.candidate_set.content_hash, candidate.content_hash,
            candidate.policy_hash, candidate.deadline_ns, expired_at, "ACTION_DEADLINE_PASSED")
        source_ref = index_candidate_expiry_evidence(repo, evidence, available_at_ns=expired_at)
        entry = DecisionCalendarEntryV2(case.candidate_set.content_hash, candidate.content_hash,
            "S1_MTF_TREND_PULLBACK", "1", candidate.policy_hash, CUTOFF,
            SelectionStateV2.SELECTED, AdmissionStateV2.EXPIRED, None, None,
            DecisionSourceStageV2.EXPIRY, (evidence.reason_code,), source_ref, expired_at, expired_at)
        assert index_decision_calendar_entry(repo, entry) == entry.content_hash
        with pytest.raises(ValueError):
            fake = replace(evidence, expired_at_ns=candidate.deadline_ns - 1)
            index_candidate_expiry_evidence(repo, fake, available_at_ns=expired_at)
        with pytest.raises(ValueError):
            CandidateExpiryEvidenceV2.from_dict({**evidence.to_dict(), "unknown": 1})


def test_no_fill_and_unresolved_replay_cannot_mature_as_fill(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _, _, payoff, item = _payoff_case(repo, entry_ask="101")
        assert payoff.status == ReplayStatusV2.NO_FILL
        assert item.fill_quantity == 0
        index_matured_outcome(repo, item)
        with pytest.raises(ValueError):
            index_matured_outcome(repo, replace(item, execution_state=ExecutionOutcomeStateV2.PARTIAL_FILL,
                fill_quantity=Decimal("1")))
        with pytest.raises(ValueError):
            index_matured_outcome(repo, replace(item, net_payoff=Decimal("1"), gross_payoff=Decimal("1")))
        with pytest.raises(ValueError):
            replace(item, execution_state=ExecutionOutcomeStateV2.UNRESOLVED)


def _pretrade_case(repo):
    def h(label):
        return sha256_json(label)
    identity = {"version": "TEST_FROZEN_ACTION", "fixture": "pretrade-action", "quantity": "2"}
    action_hash = sha256_json(identity)
    artifact = {"action_hash": action_hash, "version": "TEST_ACTION_ARTIFACT"}
    action_ref, set_id = sha256_json(artifact), h("common-set")
    inputs = (CausalInputV2(h("model"), "ModelManifestV2", 90, 91),
              CausalInputV2(h("calibration"), "CalibrationV2", 92, 93),
              CausalInputV2(h("execution-model"), "ExecutionModelV2", 94, 95),
              CausalInputV2(h("source"), "RawObservationV2", 96, 97))
    _index(repo, action_ref, "ActionArtifactV2", 90,
           {"action_artifact": artifact, "action_identity": identity})
    for item in inputs:
        _index(repo, item.ref, item.kind, item.available_at_ns, {})
    dummy = h("dummy")
    base = PretradeScenarioArtifactV2(action_hash, action_ref, 100, "gen1", "m1", "s1",
        inputs[0], inputs[1], (inputs[3],), (dummy,), inputs[2],
        (JointScenarioV2(h("path-a"), h("row-a"), Decimal("0.25")),
         JointScenarioV2(h("path-b"), h("row-b"), Decimal("0.75"))),
        set_id, JOINT_DIMENSIONS, (dummy,), dummy, dummy, dummy, dummy,
        "NET_CASHFLOW_ONCE_V1", 101, 106, 107, 110, ScenarioStatusV2.AVAILABLE)
    rows = []
    payloads = []
    joint_data = []
    for label, weight in (("path-a", "0.25"), ("path-b", "0.75")):
        path_id = h(label)
        entry_state = ScenarioFillStateV2.NO_FILL if label == "path-a" else ScenarioFillStateV2.PARTIAL_FILL
        entry_qty = Decimal("0") if label == "path-a" else Decimal("1")
        exit_state = ScenarioFillStateV2.NOT_ATTEMPTED if label == "path-a" else ScenarioFillStateV2.FULL_FILL
        exit_qty = Decimal("0") if label == "path-a" else Decimal("1")
        point = JointScenarioPointV2(102, path_id, Decimal("100"), Decimal("100.1"), Decimal("99.9"),
            Decimal("0.2") if label == "path-a" else Decimal("0.4"),
            Decimal("10") if label == "path-a" else Decimal("30"),
            Decimal("12") if label == "path-a" else Decimal("40"),
            entry_state, Decimal("2"), entry_qty,
            2_000_000 if label == "path-a" else 4_000_000,
            3_000_000 if label == "path-a" else 6_000_000,
            exit_state, exit_qty, Decimal("-0.25") if label == "path-a" else Decimal("0.5"))
        data = JointScenarioDataV2(action_hash, action_ref, 100, set_id, path_id, "gen1", "s1",
            base.causal_input_manifest_hash, Decimal("2"), (point,), 101, 103, 103)
        data_ref = index_joint_scenario_data(repo, data)
        payload = JointScenarioPayloadV2(action_hash, action_ref, 100, set_id, h(label), "gen1", "s1",
            base.causal_input_manifest_hash, JOINT_DIMENSIONS, data_ref, 101, 103, 104)
        index_joint_scenario_payload(repo, payload)
        rows.append(JointScenarioV2(h(label), payload.content_hash, Decimal(weight)))
        payloads.append(payload)
        joint_data.append(data)
    roles = ("SUPPORT", "STRESS", "OUTCOME_DISPERSION", "ESTIMATION_UNCERTAINTY",
             "EXECUTION_UNCERTAINTY", "NUMERICAL_ERROR")
    derived = {}
    for role in roles:
        result_body = {"role": role}
        result_ref = sha256_json(result_body)
        _index(repo, result_ref, "PretradeDerivedResultV2", 104, result_body)
        item = PretradeDerivedEvidenceV2(role, action_hash, action_ref, 100, set_id,
            base.causal_input_manifest_hash, result_ref, 101, 105, 105)
        index_pretrade_derived_evidence(repo, item)
        derived[role] = item
    scenario = replace(base, rows=tuple(rows), support_refs=(derived["SUPPORT"].content_hash,),
        deterministic_stress_refs=(derived["STRESS"].content_hash,),
        outcome_dispersion_ref=derived["OUTCOME_DISPERSION"].content_hash,
        estimation_uncertainty_ref=derived["ESTIMATION_UNCERTAINTY"].content_hash,
        execution_uncertainty_ref=derived["EXECUTION_UNCERTAINTY"].content_hash,
        numerical_error_ref=derived["NUMERICAL_ERROR"].content_hash)
    return scenario, payloads, derived, joint_data


def test_pretrade_joint_manifest_set_action_and_derived_binding(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        scenario, payloads, derived, joint_data = _pretrade_case(repo)
        assert PretradeScenarioArtifactV2.from_dict(scenario.to_dict()) == scenario
        assert index_pretrade_scenario(repo, scenario) == scenario.content_hash
        assert payloads[0].available_at_ns > scenario.information_cutoff_ns
        assert derived["SUPPORT"].available_at_ns > scenario.information_cutoff_ns
        assert scenario.rows[0].probability + scenario.rows[1].probability == Decimal(1)
        assert derived["STRESS"].content_hash not in {row.joint_payload_ref for row in scenario.rows}
        for change in ({"source_inputs": (CausalInputV2(scenario.source_inputs[0].ref,
                            "RawObservationV2", 101, 101),)},
                       {"model_input": CausalInputV2(scenario.model_input.ref, "ModelManifestV2", 101, 101)},
                       {"available_at_ns": scenario.action_expires_at_ns},
                       {"joint_dimensions": ("price_mark_index",)},
                       {"deterministic_stress_refs": (scenario.rows[0].joint_payload_ref,)}):
            with pytest.raises(ValueError):
                replace(scenario, **change)
        for changed in (replace(scenario, action_hash=sha256_json("other-action")),
                        replace(scenario, information_cutoff_ns=99),
                        replace(scenario, model_version="other-model"),
                        replace(scenario, calibration_input=CausalInputV2(scenario.calibration_input.ref,
                            "CalibrationV2", 91, 93)),
                        replace(scenario, rows=(JointScenarioV2(scenario.rows[0].joint_path_id,
                            scenario.rows[0].joint_payload_ref, Decimal("0.5")),
                            JointScenarioV2(scenario.rows[1].joint_path_id,
                            scenario.rows[1].joint_payload_ref, Decimal("0.5"))))):
            assert changed.content_hash != scenario.content_hash
        for change in ({"action_hash": sha256_json("other-action")},
                       {"common_scenario_set_id": sha256_json("other-set")},
                       {"model_version": "other-model"},
                       {"calibration_input": CausalInputV2(scenario.calibration_input.ref,
                           "CalibrationV2", 91, 93)}):
            with pytest.raises(ValueError):
                index_pretrade_scenario(repo, replace(scenario, **change))
        with pytest.raises(ValueError):
            index_pretrade_scenario(repo, replace(scenario,
                rows=(JointScenarioV2(scenario.rows[0].joint_path_id, payloads[0].content_hash, Decimal("0.2")),)))
        with pytest.raises(ValueError):
            PretradeScenarioArtifactV2.from_dict({**scenario.to_dict(), "version": "future"})
        with pytest.raises(ValueError):
            PretradeScenarioArtifactV2.from_dict({**scenario.to_dict(), "version": "PRETRADE_SCENARIO_ARTIFACT_V2_V2"})
        with pytest.raises(ValueError):
            JointScenarioPayloadV2.from_dict({**payloads[0].to_dict(), "unknown": 1})
        with pytest.raises(ValueError):
            PretradeDerivedEvidenceV2.from_dict({**derived["SUPPORT"].to_dict(), "unknown": 1})
        no_est = replace(scenario, status=ScenarioStatusV2.NOT_ESTIMABLE, rows=(), support_refs=(),
            deterministic_stress_refs=(), outcome_dispersion_ref=None, estimation_uncertainty_ref=None,
            execution_uncertainty_ref=None, numerical_error_ref=None, inability_reason="NO_DEPTH_SUPPORT")
        assert no_est.rows == ()
        with pytest.raises(ValueError):
            replace(no_est, status=ScenarioStatusV2.AVAILABLE, inability_reason=None)
        with pytest.raises(ValueError):
            JointScenarioDataV2.from_dict({"version": "JOINT_SCENARIO_DATA_V2_V1", "payload": "opaque"})
        assert len(joint_data[0].points) == 1
        with pytest.raises(ValueError):
            replace(joint_data[0], points=(replace(joint_data[0].points[0], joint_path_id=joint_data[1].joint_path_id),))
        point_keys = set(joint_data[0].points[0].to_dict())
        assert {"last_price", "mark_price", "index_price", "spread", "bid_depth_contracts", "ask_depth_contracts",
                "entry_state", "entry_fill_quantity", "decision_to_execution_latency_ns", "exit_latency_ns",
                "exit_state", "exit_fill_quantity", "funding_cashflow_usdt"}.issubset(point_keys)
        for component in ("ask_depth_contracts", "funding_cashflow_usdt", "exit_latency_ns"):
            changed_point = replace(joint_data[0].points[0], **{
                component: getattr(joint_data[1].points[0], component)})
            changed_data = replace(joint_data[0], points=(changed_point,))
            assert changed_data.content_hash != joint_data[0].content_hash
            with pytest.raises(ValueError):
                repo.register_artifact(ArtifactIndexEntryV2(joint_data[0].content_hash, "JointScenarioDataV2",
                    joint_data[0].content_hash, changed_data.created_at_ns, changed_data.available_at_ns,
                    {"joint_data": changed_data.to_dict()}))
        with pytest.raises(ValueError):
            replace(joint_data[0].points[0], entry_fill_quantity=Decimal("1"))


def test_pretrade_payload_and_derived_cross_action_and_future_compute(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        scenario, payloads, derived, _ = _pretrade_case(repo)
        h = sha256_json
        bad = replace(payloads[0], action_hash=h("other-action"))
        with pytest.raises(ValueError):
            index_joint_scenario_payload(repo, bad)
        for change in ({"common_scenario_set_id": h("other-set")},
                       {"causal_input_manifest_hash": h("other-manifest")},
                       {"information_cutoff_ns": 99}):
            altered = replace(payloads[0], **change)
            with pytest.raises(ValueError):
                index_joint_scenario_payload(repo, altered)
        future = replace(payloads[0], computed_at_ns=108, available_at_ns=108)
        # A forged early index timestamp cannot hide the payload's later completion.
        _index(repo, future.content_hash, "JointScenarioPayloadV2", 102,
               {"joint_payload": future.to_dict()})
        with pytest.raises(ValueError):
            index_pretrade_scenario(repo, replace(scenario, rows=(
                JointScenarioV2(future.joint_path_id, future.content_hash, Decimal("0.25")), scenario.rows[1])))
        for role in ("SUPPORT", "STRESS", "ESTIMATION_UNCERTAINTY"):
            wrong = replace(derived[role], action_hash=h("other-action"))
            index_pretrade_derived_evidence(repo, wrong)
            field = {"SUPPORT": "support_refs", "STRESS": "deterministic_stress_refs",
                     "ESTIMATION_UNCERTAINTY": "estimation_uncertainty_ref"}[role]
            value = (wrong.content_hash,) if role in ("SUPPORT", "STRESS") else wrong.content_hash
            with pytest.raises(ValueError):
                index_pretrade_scenario(repo, replace(scenario, **{field: value}))
        wrong_cutoff = replace(derived["EXECUTION_UNCERTAINTY"], information_cutoff_ns=99)
        index_pretrade_derived_evidence(repo, wrong_cutoff)
        with pytest.raises(ValueError):
            index_pretrade_scenario(repo, replace(scenario,
                execution_uncertainty_ref=wrong_cutoff.content_hash))
        for kind in ("ReplayPathV2", "PolicyPayoffV2", "PairedPortfolioPayoffV2", "MaturedOutcomeV2"):
            with pytest.raises(ValueError):
                CausalInputV2(h(kind), kind, 90, 91)
        with pytest.raises(ValueError):
            replace(scenario, rows=(JointScenarioV2(scenario.rows[0].joint_path_id,
                scenario.rows[0].joint_payload_ref, Decimal("0.3")), scenario.rows[1]))


def test_actual_binding_exact_account_epoch_and_no_research_risk_authority(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _, action, _, sim = _payoff_case(repo)
        closed = actual_outcome(repo, sim.horizon_end_ns, sim.matured_at_ns,
                                sim.net_payoff, account_scope="SHADOW_FAKE_ACCOUNT")
        source_entry = repo.get_artifact(closed.position_ref)
        assert source_entry is not None
        source_body = source_entry.metadata
        source = ActualClosedPositionSourceV2(source_body["account_scope"], source_body["position_epoch_id"],
            KEY, source_body["close_at_ns"], Decimal(source_body["realized_net_pnl"]),
            source_body["available_at_ns"], ACTUAL_CLOSE_PROVENANCE,
            source_body["execution_source_ref"], source_body["economic_source_ref"])
        shared = {"action_hash": sim.action_hash, "action_artifact_ref": action.content_hash,
            "candidate_ref": sim.candidate_ref, "candidate_set_ref": sim.candidate_set_ref,
            "actual_closed_source_ref": source.content_hash, "account_scope": source.account_scope,
            "position_epoch_id": source.position_epoch_id}
        link_body = {**shared, "source_system": "VENUE_RECONCILED_ACTION_POSITION",
                     "execution_source_ref": source.execution_source_ref}
        link_ref = sha256_json(link_body)
        _index(repo, link_ref, "V2ActualActionPositionLinkObservationV1", sim.matured_at_ns, link_body)
        economics_body = {**shared, "source_system": "ACCOUNT_RECONCILED_ACTION_CASH",
            "economic_source_ref": source.economic_source_ref,
            "gross_payoff": canonical_decimal_str(sim.gross_payoff),
            "fees": canonical_decimal_str(sim.fees),
            "funding_cashflow": canonical_decimal_str(sim.funding_cashflow),
            "net_payoff": canonical_decimal_str(sim.net_payoff),
            "requested_quantity": canonical_decimal_str(sim.requested_quantity),
            "fill_quantity": canonical_decimal_str(sim.fill_quantity),
            "fill_status": sim.execution_state.value}
        economics_ref = sha256_json(economics_body)
        _index(repo, economics_ref, "V2ActualActionEconomicsObservationV1", sim.matured_at_ns, economics_body)
        binding = ActualActionPositionBindingV2(sim.action_hash, action.content_hash, sim.candidate_ref,
            sim.candidate_set_ref, source.content_hash, source.account_scope, source.position_epoch_id,
            link_ref, economics_ref, sim.matured_at_ns)
        index_actual_action_position_binding(repo, binding)
        actual = replace(sim, provenance=OutcomeProvenanceV2.ACTUAL, label_view="ACTUAL_SYSTEM",
            actual_closed_source_ref=source.content_hash, actual_action_binding_ref=binding.content_hash,
            execution_evidence_ref=binding.content_hash,
            evidence_refs=tuple(sorted((source.content_hash, binding.content_hash))))
        index_matured_outcome(repo, actual)
        assert realized_risk_source(repo, actual, source) == source.content_hash
        with pytest.raises(ValueError):
            realized_risk_source(repo, sim, source)
        with pytest.raises(ValueError):
            index_matured_outcome(repo, replace(actual, action_hash=sha256_json("other-action")))
        other = actual_outcome(repo, sim.horizon_end_ns, sim.matured_at_ns, sim.net_payoff,
                               account_scope="OTHER_ACCOUNT")
        with pytest.raises(ValueError):
            index_matured_outcome(repo, replace(actual, actual_closed_source_ref=other.position_ref,
                evidence_refs=tuple(sorted((binding.content_hash, other.position_ref)))))
        with pytest.raises(ValueError):
            index_actual_action_position_binding(repo, replace(binding, position_epoch_id=sha256_json("other-epoch")))
