"""Additive research selection, full calendar and conservative family audit."""

import inspect
from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import FrozenMap, sha256_json
from atlas.v2.contracts import CandidateSelectionStatus
from atlas.v2.instruments import StrategyEligibilityV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.science.audits import (
    ABLATION_FAMILIES,
    MultiplicityVariantV2,
    build_multiplicity_audit,
    build_selection_policy_audit,
    declare_feature_family_ablation,
)
from atlas.v2.science.outcomes import DecisionCalendarEntryV2, index_decision_calendar_entry
from atlas.v2.science.research_selection import (
    MULTI_SLEEVE_SELECTION_HASH,
    assemble_multisleeve_research_candidate_set,
    research_selection_universe,
    research_sleeve_audit,
)
from atlas.v2.selection import (
    ScannerRankEvidenceV1,
    ScannerSelectionSourceV1,
    assemble_candidate_set,
    register_scanner_rank,
    register_scanner_source,
)
from atlas.v2.strategies.s1_trend import S1_POLICY
from atlas.v2.strategies.s2_breakout import S2_POLICY
from atlas.v2.strategies.s3_mean_reversion import S3_POLICY

from .session023_support import feature_candidate, research_case
from .test_session016_candidate_selection import CUTOFF, EVENT, evidence, universe

POLICIES = {p.policy_hash: p for p in (S1_POLICY, S2_POLICY, S3_POLICY)}


def calendar(repo, candidate_set, item=None, state="SELECTED", reasons=()):
    policy = POLICIES[item.policy_hash] if item is not None else None
    row = DecisionCalendarEntryV2(candidate_set.content_hash, item.content_hash if item else None,
        policy.policy_id if policy else "MULTI_SLEEVE_RESEARCH_SELECTION_V1", policy.version if policy else "1.1.0-research",
        policy.policy_hash if policy else MULTI_SLEEVE_SELECTION_HASH, CUTOFF, state,
        "NOT_EVALUATED" if state == "SELECTED" else "NOT_APPLICABLE", None, None, "CANDIDATE_SET",
        reasons, candidate_set.content_hash, CUTOFF, CUTOFF)
    index_decision_calendar_entry(repo, row)
    return row


def test_unregistered_inclusion_probabilities_cannot_enable_ipw(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = research_case(repo)
        calendar(repo, case.candidate_set, case.candidate)
        with pytest.raises(ValueError, match="preregistered deterministic exploration"):
            build_selection_policy_audit(repo, audit_id="ipw", start_ns=CUTOFF, end_ns=CUTOFF + 1,
                exploration_probabilities={case.candidate.content_hash: Decimal("0.5")})


def test_future_same_id_candidate_cannot_rewrite_earlier_calendar_members(tmp_path):
    from .test_session016_candidate_selection import index

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = research_case(repo)
        calendar(repo, case.candidate_set, case.candidate)
        earlier = build_selection_policy_audit(repo, audit_id="immutable-calendar", start_ns=CUTOFF, end_ns=CUTOFF + 1)
        candidate = case.candidate
        future = replace(candidate, envelope=replace(candidate.envelope, content_hash="",
            created_at_ns=CUTOFF + 100, available_at_ns=CUTOFF + 100), decision_at_ns=CUTOFF + 100,
            deadline_ns=candidate.deadline_ns + 100, horizon_end_ns=candidate.horizon_end_ns + 100)
        index(repo, future)
        assert future.candidate_id == candidate.candidate_id and future.content_hash != candidate.content_hash
        later = build_selection_policy_audit(repo, audit_id="immutable-calendar", start_ns=CUTOFF, end_ns=CUTOFF + 1)
        assert later == earlier


@pytest.mark.parametrize("depth,state,provenance", [("0", "NO_FILL", "SIMULATED"),
    ("10", "PARTIAL_FILL", "COUNTERFACTUAL"), ("100", "FULL_FILL", "SIMULATED")])
def test_whole_calendar_retains_qualified_fill_states_and_terminal_no_trade(tmp_path, depth, state, provenance):
    from atlas.v2.science.outcomes import AdmissionStateV2, index_matured_outcome

    from .test_session018_remediation import _payoff_case

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _, _, _, outcome = _payoff_case(repo, entry_depth=depth, admission_state=AdmissionStateV2.NO_TRADE)
        outcome = replace(outcome, provenance=provenance)
        index_matured_outcome(repo, outcome)
        audit = build_selection_policy_audit(repo, audit_id="fill-calendar", start_ns=CUTOFF,
            end_ns=outcome.available_at_ns + 1)
        assert dict(audit.fill_state_counts) == {state: 1}
        assert dict(audit.provenance_counts) == {provenance: 1}
        assert any(row.admission_state == "NO_TRADE" and row.execution_states == (state,) for row in audit.rows)
        before = build_selection_policy_audit(repo, audit_id="before-maturity", start_ns=CUTOFF,
            end_ns=outcome.available_at_ns - 1)
        assert before.fill_state_counts == ()


def test_exact_competitors_retained_and_s1_s2_baseline_byte_hash_reproduced(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = research_case(repo)
        baseline = case.baseline_candidate_set
        identity = repo.get_artifact(baseline.content_hash).metadata["identity"]
        reproduced = assemble_candidate_set(repo, universe=case.baseline_universe, decision_event_id=EVENT,
            cutoff_ns=CUTOFF, candidates=case.competitors[:2], policies=POLICIES,
            scanner_evidence_refs=identity["attempted_scanner_evidence_refs"])
        assert baseline.to_canonical_json() == reproduced.to_canonical_json() and baseline.content_hash == reproduced.content_hash
        refs = {item.candidate_id: (evidence(repo, item, case.universe, rank, event="parity"),)
            for rank, item in enumerate(case.competitors[:2], 1)}
        research = assemble_multisleeve_research_candidate_set(repo, universe=case.universe,
            decision_event_id="parity", cutoff_ns=CUTOFF, candidates=case.competitors[:2], policies=POLICIES,
            scanner_evidence_refs=refs)
        assert research.selected_candidate_id == baseline.selected_candidate_id
        assert research.selection_policy_hash != baseline.selection_policy_hash
        assert len(case.candidate_set.candidates) == 3
        assert {member.policy_id for member in case.candidate_set.candidates} == {p.policy_id for p in POLICIES.values()}
        assert case.baseline_universe.selection_policy_hash == baseline.selection_policy_hash


def test_deterministic_ties_and_missing_competitor_retain_every_action(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        source = universe()
        expanded = replace(source, entries=(replace(source.entries[0], strategy_eligibility=FrozenMap({
            **dict(source.entries[0].strategy_eligibility), S3_POLICY.policy_id: StrategyEligibilityV2("ELIGIBLE")})),),
            envelope=replace(source.envelope, content_hash="", artifact_id="expanded"))
        u = research_selection_universe(expanded)
        items = tuple(feature_candidate(repo, policy) for policy in (S1_POLICY, S2_POLICY, S3_POLICY))
        refs = {item.candidate_id: (evidence(repo, item, u, 1),) for item in items}
        first = assemble_multisleeve_research_candidate_set(repo, universe=u, decision_event_id=EVENT,
            cutoff_ns=CUTOFF, candidates=items, policies=POLICIES, scanner_evidence_refs=refs)
        second = assemble_multisleeve_research_candidate_set(repo, universe=u, decision_event_id=EVENT,
            cutoff_ns=CUTOFF, candidates=items[::-1], policies=POLICIES, scanner_evidence_refs=refs)
        assert first.content_hash == second.content_hash
        assert first.selected_candidate_id == items[0].candidate_id
        missing = {item.candidate_id: (evidence(repo, item, u, 1, event="missing"),) for item in items[:2]}
        blocked = assemble_multisleeve_research_candidate_set(repo, universe=u, decision_event_id="missing",
            cutoff_ns=CUTOFF, candidates=items, policies=POLICIES, scanner_evidence_refs=missing)
        assert blocked.selection_status == CandidateSelectionStatus.NOT_ESTIMABLE and len(blocked.candidates) == 3


@pytest.mark.parametrize("model_kind", ["M0PredictionV2", "M1PredictionV2", "AnalogueActionValueV2", "MaturedOutcomeV2"])
def test_model_and_outcome_evidence_cannot_enter_scanner_selection_inputs(tmp_path, model_kind):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        item = feature_candidate(repo)
        u = research_selection_universe(universe())
        ref = sha256_json(model_kind)
        repo.register_artifact(ArtifactIndexEntryV2(ref, model_kind, ref, CUTOFF - 1, CUTOFF - 1, {}))
        source = ScannerSelectionSourceV1(item.candidate_id, item.key, 1, "SCANNER_V1", "1.0",
            u.content_hash, EVENT, CUTOFF, (ref,))
        source_ref = register_scanner_source(repo, source)
        rank_ref = register_scanner_rank(repo, ScannerRankEvidenceV1(item.candidate_id, 1, "SCANNER_V1",
            "1.0", u.content_hash, EVENT, CUTOFF, source_ref))
        result = assemble_multisleeve_research_candidate_set(repo, universe=u, decision_event_id=EVENT,
            cutoff_ns=CUTOFF, candidates=(item,), policies=POLICIES,
            scanner_evidence_refs={item.candidate_id: (rank_ref,)})
        assert result.selection_status == CandidateSelectionStatus.NOT_ESTIMABLE
        assert result.candidates[0].rejection_reason == "MODEL_OR_OUTCOME_EVIDENCE_FORBIDDEN_AT_SELECTION"
        assert len(result.candidates) == 1
    parameters = inspect.signature(assemble_multisleeve_research_candidate_set).parameters
    assert not any(name in parameters for name in ("m0", "m1", "analogue", "payoff", "outcomes"))


@pytest.mark.parametrize("sleeve", ["S4", "S5", "S7"])
def test_non_action_sleeves_are_named_exclusions_and_cannot_masquerade(tmp_path, sleeve):
    audit = research_sleeve_audit(CUTOFF)
    row = next(row for row in audit.sleeves if row[0] == sleeve)
    assert row[2] == "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT"
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        item = feature_candidate(repo)
        fake = replace(item, envelope=replace(item.envelope, content_hash=""), policy_hash=sha256_json(sleeve))
        repo.register_artifact(ArtifactIndexEntryV2(fake.content_hash, "CandidateActionV2", fake.content_hash,
            CUTOFF, CUTOFF, {"candidate": fake.to_dict()}))
        with pytest.raises(ValueError, match="complete action policy"):
            assemble_multisleeve_research_candidate_set(repo, universe=research_selection_universe(universe()),
                decision_event_id=EVENT, cutoff_ns=CUTOFF, candidates=(fake,),
                policies={fake.policy_hash: S1_POLICY}, scanner_evidence_refs={})


def test_s6_action_policy_is_shadow_only_and_named_in_complete_sleeve_audit():
    audit = research_sleeve_audit(CUTOFF)
    row = next(row for row in audit.sleeves if row[0] == "S6")
    assert row[1:] == ("ELIGIBLE", "Versioned shadow-only exact action policy; rank policy remains unchanged")
    assert "S6_CROSS_SECTIONAL_RELATIVE_STRENGTH" in audit.exact_action_policy_ids


def test_wrong_s3_horizon_rejected_and_full_calendar_preserves_unselected_no_candidate(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = research_case(repo)
        bad = replace(case.competitors[2], envelope=replace(case.competitors[2].envelope, content_hash=""),
            horizon_end_ns=CUTOFF + 2 * 3_600_000_000_000)
        repo.register_artifact(ArtifactIndexEntryV2(bad.content_hash, "CandidateActionV2", bad.content_hash,
            CUTOFF, CUTOFF, {"candidate": bad.to_dict()}))
        with pytest.raises(ValueError, match="horizon"):
            assemble_multisleeve_research_candidate_set(repo, universe=case.universe, decision_event_id="bad",
                cutoff_ns=CUTOFF, candidates=(bad,), policies=POLICIES, scanner_evidence_refs={})
        calendar(repo, case.candidate_set, case.candidate)
        for item in case.competitors[1:]:
            calendar(repo, case.candidate_set, item, "UNSELECTED")
        empty = assemble_multisleeve_research_candidate_set(repo, universe=case.universe, decision_event_id="empty",
            cutoff_ns=CUTOFF, candidates=(), policies=POLICIES, scanner_evidence_refs={})
        calendar(repo, empty, state="NO_CANDIDATE")
        audit = build_selection_policy_audit(repo, audit_id="whole-calendar", start_ns=CUTOFF, end_ns=CUTOFF + 1)
        assert {row.selection_state for row in audit.rows} >= {"SELECTED", "UNSELECTED", "NO_CANDIDATE"}
        assert audit.status == "NOT_ESTIMABLE" and audit.missed_value_share is None and audit.selection_lift is None
        assert audit.inverse_probability_status.startswith("NOT_ESTIMABLE")
        assert all(not row.execution_states for row in audit.rows)


def test_multiplicity_complete_family_deterministic_holm_and_insufficient_support():
    variants = (MultiplicityVariantV2("baseline", sha256_json("baseline"), (1, 2, 3), (Decimal(0),) * 3),
        MultiplicityVariantV2("challenger", sha256_json("challenger"), (1, 2, 3), (Decimal(1),) * 3),
        MultiplicityVariantV2("failed", sha256_json("failed"), (), (), "FIT_FAILED"))
    args = {"family_id": "family", "preregistration_ref": sha256_json("preregistration"),
        "baseline_variant_id": "baseline", "variants": variants,
        "expected_family_member_ids": [variant.variant_id for variant in variants], "bootstrap_replicates": 100}
    result = build_multiplicity_audit(**args)
    assert result == build_multiplicity_audit(**{**args, "variants": variants[::-1]})
    assert result.status == "NOT_ESTIMABLE" and result.effective_support == 0
    assert len(result.members) == 3 and next(row for row in result.members if row.variant_id == "failed").failure_reason == "FIT_FAILED"
    assert all(member.adjusted_p_value >= (member.raw_p_value or Decimal(0)) for member in result.members)
    with pytest.raises(ValueError, match="every"):
        build_multiplicity_audit(**{**args, "variants": variants[:2]})


def test_ablation_declares_every_family_and_preserves_whole_policy_contracts():
    names = ("baseline_policy_ref", "scanner_ref", "candidate_generation_ref", "selection_ref", "sizing_ref",
        "execution_assumptions_ref", "costs_ref", "no_fill_partial_fill_ref", "latency_ref", "gate_ref", "multiplicity_family_ref")
    kwargs = {name: sha256_json(name) for name in names}
    result = declare_feature_family_ablation(audit_id="audit", family_id="family", **kwargs)
    assert result.feature_families == ABLATION_FAMILIES and len(result.rows) == 10
    assert result.status == "NOT_ESTIMABLE"
    for row in result.rows:
        assert all(getattr(row, name) == kwargs[name] for name in names if name != "multiplicity_family_ref")
        assert row.result_ref is None
