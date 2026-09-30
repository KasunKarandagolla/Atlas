"""Focused fail-closed tests for Session-033 outcome resolution."""

from __future__ import annotations

from decimal import Decimal

import pytest

from atlas.v2._serialization import canonical_decimal_str, json_value, sha256_json
from atlas.v2.agent_intelligence.shadow_measurement import ActionCriticShadowObservationV1
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.risk import (
    ACTUAL_CLOSE_PROVENANCE,
    ActualClosedPositionSourceV2,
    index_research_evidence,
    index_risk_evidence,
)
from atlas.v2.science.action_critic_outcomes import link_action_critic_matured_outcome
from atlas.v2.science.outcome_resolution import resolve_decision_outcome
from atlas.v2.science.outcomes import (
    ActualActionPositionBindingV2,
    AdmissionStateV2,
    CandidateExpiryEvidenceV2,
    DecisionCalendarEntryV2,
    DecisionSourceStageV2,
    DiagnosticTargetEvidenceV2,
    ExecutionOutcomeStateV2,
    LabelStateV2,
    OutcomeProvenanceV2,
    OutcomeTargetV2,
    SelectionStateV2,
    index_actual_action_position_binding,
    index_candidate_expiry_evidence,
    index_decision_calendar_entry,
    index_diagnostic_target_evidence,
    index_matured_outcome,
)

from . import test_session018_remediation as s18
from .test_session016_candidate_selection import CUTOFF
from .test_session017_risk import actual_outcome, risk_case


def _calendar_index(repo: OpsRepository, ref: str) -> ArtifactIndexEntryV2:
    entry = repo.get_artifact(ref)
    assert entry is not None
    return entry


def _skip_payoff_registration(repo: OpsRepository, monkeypatch: pytest.MonkeyPatch) -> None:
    register = repo.register_artifact

    def intercept(entry: ArtifactIndexEntryV2) -> ArtifactIndexEntryV2:
        if entry.artifact_type == "PolicyPayoffV2":
            return entry
        return register(entry)

    monkeypatch.setattr(repo, "register_artifact", intercept)


def _tamper_payoff_registration(
    repo: OpsRepository,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    register = repo.register_artifact

    def intercept(entry: ArtifactIndexEntryV2) -> ArtifactIndexEntryV2:
        if entry.artifact_type != "PolicyPayoffV2":
            return register(entry)
        body = dict(json_value(entry.metadata["payoff"]))
        refs = list(entry.metadata["input_refs"])
        if mutation == "missing_fee_ref":
            body.pop("fee_ref")
        elif mutation == "missing_funding_schedule_ref":
            body.pop("funding_schedule_ref")
        elif mutation == "fee_not_in_inputs":
            refs.remove(body["fee_ref"])
        elif mutation == "funding_not_in_inputs":
            refs.remove(body["funding_schedule_ref"])
        elif mutation == "missing_replay_assumptions_ref":
            body.pop("replay_assumptions_ref")
        ref = sha256_json(body)
        altered = ArtifactIndexEntryV2(
            ref, "PolicyPayoffV2", ref, entry.created_at_ns, entry.available_at_ns,
            {"payoff": body, "input_refs": sorted(set(refs))},
        )
        return register(altered)

    monkeypatch.setattr(repo, "register_artifact", intercept)


@pytest.mark.parametrize(("depth", "expected_state"), [
    ("10", ExecutionOutcomeStateV2.PARTIAL_FILL),
    ("100", ExecutionOutcomeStateV2.FULL_FILL),
])
def test_replay_outcome_keeps_exact_fill_cost_funding_and_chronology(
    tmp_path, depth: str, expected_state: ExecutionOutcomeStateV2,
):
    with OpsRepository(tmp_path / f"outcome-{depth}.sqlite") as repo:
        _case, _action, payoff, fixture_outcome = s18._payoff_case(repo, entry_depth=depth)
        decision = _calendar_index(repo, fixture_outcome.decision_ref)
        now_ns = payoff.available_at_ns + 7

        before = resolve_decision_outcome(repo, decision, fixture_outcome.horizon_end_ns - 1)
        assert before.status == "PENDING" and before.outcome is None
        at_horizon = resolve_decision_outcome(repo, decision, fixture_outcome.horizon_end_ns)
        assert at_horizon.status == "UNRESOLVED" and at_horizon.outcome is None

        result = resolve_decision_outcome(repo, decision, now_ns)
        assert result.status == "MATURED" and result.outcome is not None, result.reason_code
        outcome = result.outcome
        assert outcome.label_state == LabelStateV2.MATURED
        assert outcome.execution_state == expected_state
        assert outcome.fill_quantity == payoff.filled_quantity
        assert outcome.requested_quantity == fixture_outcome.requested_quantity
        assert outcome.fill_quantity < outcome.requested_quantity if expected_state == ExecutionOutcomeStateV2.PARTIAL_FILL else outcome.fill_quantity == outcome.requested_quantity
        assert outcome.fees == (payoff.entry.fee if payoff.entry else Decimal(0)) + sum(
            (item.fee for item in payoff.exits), Decimal(0)
        )
        assert outcome.funding_cashflow == sum((amount for _, amount in payoff.funding_cashflows), Decimal(0))
        assert outcome.net_payoff == outcome.gross_payoff - outcome.fees + outcome.funding_cashflow
        assert outcome.mfe is None and outcome.mae is None and outcome.extrema_evidence_ref is None
        assert outcome.provenance == OutcomeProvenanceV2.SIMULATED
        assert outcome.label_view == "RECONSTRUCTED_MARKET"
        assert outcome.actual_closed_source_ref is None and outcome.actual_action_binding_ref is None
        assert outcome.available_at_ns == now_ns
        assert outcome.matured_at_ns >= max(outcome.horizon_end_ns, payoff.available_at_ns)
        assert resolve_decision_outcome(repo, decision, now_ns).outcome == outcome
        assert index_matured_outcome(repo, outcome) == outcome.content_hash


def test_supported_no_fill_requires_exact_replay_record(tmp_path, monkeypatch):
    original = s18.ReplayAssumptionsV2
    monkeypatch.setattr(
        s18,
        "ReplayAssumptionsV2",
        lambda *_args, **_kwargs: original(10_000_000_000, 0, 0, 0, Decimal("1"), Decimal("0")),
    )
    with OpsRepository(tmp_path / "supported-no-fill.sqlite") as repo:
        _case, _action, payoff, fixture_outcome = s18._payoff_case(repo)
        assert payoff.status.value == "NO_FILL" and payoff.entry is None
        result = resolve_decision_outcome(
            repo, _calendar_index(repo, fixture_outcome.decision_ref), payoff.available_at_ns + 1
        )
        assert result.status == "MATURED" and result.outcome is not None
        assert result.outcome.execution_state == ExecutionOutcomeStateV2.NO_FILL
        assert result.outcome.fill_quantity == 0
        assert result.outcome.gross_payoff == result.outcome.fees == 0
        assert result.outcome.funding_cashflow == result.outcome.net_payoff == 0
        assert index_matured_outcome(repo, result.outcome) == result.outcome.content_hash

    with OpsRepository(tmp_path / "missing-no-fill.sqlite") as repo:
        _skip_payoff_registration(repo, monkeypatch)
        _case, _action, _payoff, fixture_outcome = s18._payoff_case(repo)
        result = resolve_decision_outcome(
            repo, _calendar_index(repo, fixture_outcome.decision_ref), fixture_outcome.horizon_end_ns + 1
        )
        assert result.status == "UNRESOLVED" and result.outcome is None
        assert result.reason_code == "EXECUTION_EVIDENCE_MISSING"


@pytest.mark.parametrize("mutation", [
    "missing_fee_ref",
    "missing_funding_schedule_ref",
    "fee_not_in_inputs",
    "funding_not_in_inputs",
    "missing_replay_assumptions_ref",
])
def test_missing_cost_or_replay_evidence_never_matures(mutation, tmp_path, monkeypatch):
    with OpsRepository(tmp_path / f"missing-{mutation}.sqlite") as repo:
        _tamper_payoff_registration(repo, monkeypatch, mutation)
        _case, _action, payoff, fixture_outcome = s18._payoff_case(repo)
        result = resolve_decision_outcome(
            repo, _calendar_index(repo, fixture_outcome.decision_ref), payoff.available_at_ns + 1
        )
        assert result.status == "UNRESOLVED" and result.outcome is None


def test_conflicting_replay_paths_fail_closed(tmp_path):
    with OpsRepository(tmp_path / "conflicting-replay.sqlite") as repo:
        _case, _action, payoff, fixture_outcome = s18._payoff_case(repo)
        indexed = repo.get_artifact(payoff.content_hash)
        assert indexed is not None
        body = dict(json_value(indexed.metadata["payoff"]))
        body["exit_reason"] = "CONFLICTING_REPLAY_HISTORY"
        ref = sha256_json(body)
        repo.register_artifact(ArtifactIndexEntryV2(
            ref, "PolicyPayoffV2", ref, indexed.created_at_ns, indexed.available_at_ns,
            {"payoff": body, "input_refs": indexed.metadata["input_refs"]},
        ))
        result = resolve_decision_outcome(
            repo, _calendar_index(repo, fixture_outcome.decision_ref), payoff.available_at_ns + 1
        )
        assert result.status == "UNRESOLVED" and result.outcome is None
        assert result.reason_code == "CONFLICTING_REPLAY_EVIDENCE"


def test_actual_provenance_requires_exact_close_and_action_binding(tmp_path, monkeypatch):
    with OpsRepository(tmp_path / "actual-binding.sqlite") as repo:
        _skip_payoff_registration(repo, monkeypatch)
        case, action, _payoff, replay_outcome = s18._payoff_case(repo)
        close = actual_outcome(
            repo, replay_outcome.horizon_end_ns, replay_outcome.matured_at_ns,
            replay_outcome.net_payoff,
        )
        close_entry = repo.get_artifact(close.position_ref)
        assert close_entry is not None
        close_body = close_entry.metadata
        actual_sources = {
            "account_scope": close_body["account_scope"],
            "position_epoch_id": close_body["position_epoch_id"],
            "key": case.candidate.key.to_dict(),
            "close_at_ns": close_body["close_at_ns"],
        }
        execution_source = {**actual_sources, "source_system": "VENUE_RECONCILED_EXECUTION"}
        economic_source = {
            **actual_sources,
            "source_system": "ACCOUNT_RECONCILED_CASH",
            "realized_net_pnl": close_body["realized_net_pnl"],
        }
        execution_ref = sha256_json(execution_source)
        economic_ref = sha256_json(economic_source)
        index_research_evidence(
            repo, "V2ActualExecutionCloseObservationV1", execution_ref,
            close_body["available_at_ns"], execution_source,
        )
        index_research_evidence(
            repo, "V2ActualAccountPnlObservationV1", economic_ref,
            close_body["available_at_ns"], economic_source,
        )
        actual = ActualClosedPositionSourceV2(
            close_body["account_scope"], close_body["position_epoch_id"], case.candidate.key,
            close_body["close_at_ns"], Decimal(close_body["realized_net_pnl"]),
            close_body["available_at_ns"], ACTUAL_CLOSE_PROVENANCE,
            execution_ref, economic_ref,
        )
        index_risk_evidence(repo, actual)
        common = {
            "action_hash": action.action.action_hash,
            "action_artifact_ref": action.content_hash,
            "candidate_ref": replay_outcome.candidate_ref,
            "candidate_set_ref": replay_outcome.candidate_set_ref,
            "actual_closed_source_ref": actual.content_hash,
            "account_scope": actual.account_scope,
            "position_epoch_id": actual.position_epoch_id,
        }
        link = {**common, "source_system": "VENUE_RECONCILED_ACTION_POSITION",
                "execution_source_ref": actual.execution_source_ref}
        economics = {
            **common,
            "source_system": "ACCOUNT_RECONCILED_ACTION_CASH",
            "economic_source_ref": actual.economic_source_ref,
            "gross_payoff": canonical_decimal_str(replay_outcome.gross_payoff),
            "fees": canonical_decimal_str(replay_outcome.fees),
            "funding_cashflow": canonical_decimal_str(replay_outcome.funding_cashflow),
            "net_payoff": canonical_decimal_str(replay_outcome.net_payoff),
            "requested_quantity": canonical_decimal_str(replay_outcome.requested_quantity),
            "fill_quantity": canonical_decimal_str(replay_outcome.fill_quantity),
            "fill_status": replay_outcome.execution_state.value,
        }
        link_ref = sha256_json(link)
        economics_ref = sha256_json(economics)
        available = replay_outcome.matured_at_ns
        repo.register_artifact(ArtifactIndexEntryV2(
            link_ref, "V2ActualActionPositionLinkObservationV1", link_ref, available, available, link,
        ))
        repo.register_artifact(ArtifactIndexEntryV2(
            economics_ref, "V2ActualActionEconomicsObservationV1", economics_ref,
            available, available, economics,
        ))
        binding = ActualActionPositionBindingV2(
            replay_outcome.action_hash, action.content_hash, replay_outcome.candidate_ref,
            replay_outcome.candidate_set_ref, actual.content_hash, actual.account_scope,
            actual.position_epoch_id, link_ref, economics_ref, available,
        )
        index_actual_action_position_binding(repo, binding)
        result = resolve_decision_outcome(
            repo, _calendar_index(repo, replay_outcome.decision_ref), available + 1
        )
        assert result.status == "MATURED" and result.outcome is not None, result.reason_code
        outcome = result.outcome
        assert outcome.provenance == OutcomeProvenanceV2.ACTUAL
        assert outcome.label_view == "ACTUAL_SYSTEM"
        assert outcome.actual_closed_source_ref == actual.content_hash
        assert outcome.actual_action_binding_ref == binding.content_hash
        assert outcome.execution_evidence_ref == binding.content_hash
        assert index_matured_outcome(repo, outcome) == outcome.content_hash


def test_predeclared_diagnostic_for_unselected_candidate_is_non_executable(tmp_path):
    with OpsRepository(tmp_path / "diagnostic.sqlite") as repo:
        case = risk_case(repo, include_s2=True)
        candidate = (case.s2_candidate if case.candidate_set.selected_candidate_id == case.candidate.candidate_id
                     else case.candidate)
        assert candidate is not None
        entry = next(row for row in case.candidate_set.candidates if row.candidate_id == candidate.candidate_id)
        decision = DecisionCalendarEntryV2(
            case.candidate_set.content_hash, candidate.content_hash, entry.policy_id, "1",
            candidate.policy_hash, CUTOFF, SelectionStateV2.UNSELECTED,
            AdmissionStateV2.NOT_APPLICABLE, None, None, DecisionSourceStageV2.CANDIDATE_SET,
            (), case.candidate_set.content_hash, CUTOFF, CUTOFF,
        )
        decision_ref = index_decision_calendar_entry(repo, decision)
        declaration = {"label_definition": "future_mid_return_v1", "unit": "FRACTION"}
        declaration_ref = sha256_json(declaration)
        index_research_evidence(repo, "DiagnosticTargetDefinitionV2", declaration_ref, CUTOFF, declaration)
        source_body = {"candidate_ref": candidate.content_hash, "value": "0.02"}
        source_ref = sha256_json(source_body)
        index_research_evidence(repo, "CausalMarketDiagnosticV2", source_ref,
                                candidate.horizon_end_ns + 1, source_body)
        diagnostic = DiagnosticTargetEvidenceV2(
            decision_ref, case.candidate_set.content_hash, candidate.content_hash,
            "future_mid_return_v1", declaration_ref, CUTOFF, candidate.horizon_end_ns,
            Decimal("0.02"), "FRACTION", (source_ref,), candidate.horizon_end_ns + 1,
            candidate.horizon_end_ns + 1,
        )
        diagnostic_ref = index_diagnostic_target_evidence(repo, diagnostic)
        result = resolve_decision_outcome(repo, _calendar_index(repo, decision_ref), diagnostic.available_at_ns)
        assert result.status == "MATURED" and result.outcome is not None
        outcome = result.outcome
        assert outcome.outcome_target == OutcomeTargetV2.NON_EXECUTABLE_DIAGNOSTIC
        assert outcome.provenance == OutcomeProvenanceV2.COUNTERFACTUAL
        assert outcome.action_hash is None and outcome.execution_state == ExecutionOutcomeStateV2.NOT_APPLICABLE
        assert outcome.fill_quantity is None and outcome.net_payoff is None
        assert outcome.diagnostic_evidence_ref == diagnostic_ref
        assert index_matured_outcome(repo, outcome) == outcome.content_hash


def test_expired_decision_is_censored_only_with_exact_expiry_evidence(tmp_path):
    with OpsRepository(tmp_path / "expired.sqlite") as repo:
        case = risk_case(repo)
        candidate = case.candidate
        selected = next(row for row in case.candidate_set.candidates
                        if row.candidate_id == candidate.candidate_id)
        expiry = CandidateExpiryEvidenceV2(
            case.candidate_set.content_hash, candidate.content_hash, candidate.policy_hash,
            candidate.deadline_ns, candidate.deadline_ns, "CANDIDATE_DEADLINE_REACHED",
        )
        expiry_ref = index_candidate_expiry_evidence(repo, expiry, available_at_ns=candidate.deadline_ns)
        decision = DecisionCalendarEntryV2(
            case.candidate_set.content_hash, candidate.content_hash, selected.policy_id, "1",
            candidate.policy_hash, CUTOFF, SelectionStateV2.SELECTED, AdmissionStateV2.EXPIRED,
            None, None, DecisionSourceStageV2.EXPIRY, (expiry.reason_code,), expiry_ref,
            candidate.deadline_ns, candidate.deadline_ns,
        )
        decision_ref = index_decision_calendar_entry(repo, decision)
        result = resolve_decision_outcome(repo, _calendar_index(repo, decision_ref), candidate.horizon_end_ns + 1)
        assert result.status == "CENSORED" and result.outcome is not None
        assert result.outcome.label_state == LabelStateV2.CENSORED
        assert result.outcome.reason == "EXPIRED_BEFORE_FROZEN_ACTION"
        assert result.outcome.evidence_refs == (expiry_ref,)
        assert result.outcome.net_payoff is None and result.outcome.fill_quantity is None
        assert index_matured_outcome(repo, result.outcome) == result.outcome.content_hash


def test_critic_link_cannot_substitute_for_absent_matured_outcome(tmp_path):
    with OpsRepository(tmp_path / "critic-no-label.sqlite") as repo:
        _case, action, _payoff, fixture_outcome = s18._payoff_case(repo)
        observation = ActionCriticShadowObservationV1(
            originating_receipt_ref=sha256_json({"receipt": "frozen"}),
            decision_calendar_ref=fixture_outcome.decision_ref,
            candidate_set_ref=fixture_outcome.candidate_set_ref,
            packet_ref=sha256_json({"packet": "fixed"}),
            packet_hash=sha256_json({"packet_body": "fixed"}),
            request_id="fixture-request",
            request_ref=sha256_json({"request": "fixed"}),
            request_hash=sha256_json({"request_body": "fixed"}),
            action_artifact_ref=action.content_hash,
            action_hash=action.action.action_hash,
            critic_terminal_status="COMPLETE",
            terminal_reason_code=None,
            accepted_shadow_evidence=True,
            finding_types=(),
            dispatch_authorized_at_ns=fixture_outcome.decision_at_ns,
            result_received_at_ns=fixture_outcome.decision_at_ns + 1,
            dispatch_to_result_latency_ns=1,
            provider_profile_hash=sha256_json({"provider_profile": "fixed"}),
            model_profile_hash=sha256_json({"model_profile": "fixed"}),
            decision_influence=False,
            admission_influence=False,
            deterministic_terminal_status="NOT_ESTIMABLE",
            deterministic_admission_status=fixture_outcome.admission_state.value,
            recorded_at_ns=fixture_outcome.decision_at_ns + 2,
        )
        observation_ref = observation.content_hash
        repo.register_artifact(ArtifactIndexEntryV2(
            observation_ref, observation.VERSION, observation_ref, observation.recorded_at_ns,
            observation.recorded_at_ns, {"observation": observation.to_dict()},
        ))
        with pytest.raises(ValueError, match="indexed MaturedOutcomeV2 required"):
            link_action_critic_matured_outcome(
                repo, observation_ref, sha256_json({"missing": "outcome"}),
                as_of_ns=observation.recorded_at_ns,
            )
