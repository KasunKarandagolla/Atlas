"""Bounded append-only lab, holdout discipline and basket authority boundary."""

import math
from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.models.protocol import PromotionStatusV2
from atlas.v2.science.audits import MultiplicityVariantV2
from atlas.v2.science.discovery import (
    DECISION_CALENDAR_POPULATION_V1,
    DECISION_EVENT_IDENTITY_RULES_V1,
    FINAL_HOLDOUT_ASSIGNMENT_RULE,
    DiscoveryAttemptV2,
    DiscoveryExperimentV2,
    DiscoveryHoldoutPopulationV2,
    DiscoveryOutcomeViewV2,
    audit_discovery_multiplicity,
    discovery_attempt_ledger,
    holdout_spent_at,
    index_discovery_holdout_population,
    index_discovery_outcome_view,
    mark_holdout_spent,
    register_discovery_attempt,
    register_discovery_experiment,
)
from atlas.v2.science.phase3 import PromotionEvidenceV2, validate_promotion_transition
from atlas.v2.science.research_selection import assemble_multisleeve_research_candidate_set, research_selection_universe
from atlas.v2.strategies.s8_pairs import (
    FIT_HOURS,
    HOUR_NS,
    ResearchBasketForecastV2,
    S8HourlyPriceV2,
    S8LegEvidenceV2,
    S8PairDefinitionV2,
    build_research_basket_forecast,
    persist_s8_basket,
    reject_s8_single_action,
    s8_entry_side,
    s8_exit_reason,
    simulate_s8_basket_path,
    simulate_s8_synchronized_prices,
)

from .test_session014_core import KEY
from .test_session016_candidate_selection import alternate_key, universe
from .test_session017_risk import CUTOFF

DAY_NS = 24 * HOUR_NS


def experiment(repo, budget=2, parameter_budget=2, *, holdout_start_ns=None, holdout_end_ns=None,
        holdout_assigned=True, population_id="test-final-holdout"):
    baseline = sha256_json("baseline")
    repo.register_artifact(ArtifactIndexEntryV2(baseline, "PolicyV2", baseline, 0, 0, {}))
    chronology = "180D_30D_30D_THREE_OUTER_FINAL_30D_V1"
    start = (CUTOFF + 100 * DAY_NS if holdout_start_ns is None else holdout_start_ns) if holdout_assigned else None
    end = (start + 30 * DAY_NS if holdout_end_ns is None else holdout_end_ns) if holdout_assigned else None
    source_ref = None
    if holdout_assigned:
        source_body = {"version": "DISCOVERY_HOLDOUT_ASSIGNMENT_EVIDENCE_V1", "population_id": population_id,
            "population_version": 1, "experiment_id": "experiment", "family_id": "family",
            "chronology_version": chronology, "assignment_rule": FINAL_HOLDOUT_ASSIGNMENT_RULE,
            "final_holdout_start_ns": start, "final_holdout_end_ns": end,
            "decision_calendar_population": DECISION_CALENDAR_POPULATION_V1,
            "venue_product_policy_universe": [["*", "*", "*"]],
            "decision_event_identity_rules": DECISION_EVENT_IDENTITY_RULES_V1,
            "assignment_available_at_ns": 1}
        source_ref = sha256_json(source_body)
        repo.register_artifact(ArtifactIndexEntryV2(source_ref, "DiscoveryHoldoutAssignmentEvidenceV2",
            source_ref, 0, 0, {"assignment_evidence": source_body}))
    population = DiscoveryHoldoutPopulationV2(population_id, 1, "experiment", "family", chronology,
        FINAL_HOLDOUT_ASSIGNMENT_RULE, "ASSIGNED" if holdout_assigned else "NOT_ESTIMABLE", start, end,
        DECISION_CALENDAR_POPULATION_V1, (("*", "*", "*"),), DECISION_EVENT_IDENTITY_RULES_V1,
        source_ref, 1, 1 if holdout_assigned else None)
    holdout = index_discovery_holdout_population(repo, population)
    value = DiscoveryExperimentV2("experiment", "family", "numerical_challengers", ("candles",),
        ("CUT_OFF_AVAILABLE_ONLY",), budget, parameter_budget, baseline, ("whole_policy_net_value",),
        chronology, "MAX_HORIZON_PURGE_EMBARGO", "family",
        "STOP_AT_BUDGET_OR_OPERATIONAL_FAILURE", holdout, "UNTOUCHED", True, 1)
    register_discovery_experiment(repo, value, available_at_ns=1)
    return value


def split_spec(experiment, *, training=(0, 1), validation=(2, 3), outer=(4, 5), cutoff=9,
        embargo=1, horizon=1):
    return {"version": "DISCOVERY_CHRONOLOGICAL_SPLIT_V1",
        "chronology_contract": experiment.chronology, "purge_embargo_contract": experiment.purge_embargo,
        "training_start_ns": training[0], "training_end_ns": training[1],
        "validation_start_ns": validation[0], "validation_end_ns": validation[1],
        "outer_start_ns": outer[0], "outer_end_ns": outer[1], "evaluation_cutoff_ns": cutoff,
        "embargo_ns": embargo, "maximum_policy_horizon_ns": horizon, "randomized": False}


def attempt(experiment, identity="one", *, viewed=False, failure=None, refs=(), start=10, units=1,
        training_refs=None, validation_refs=None, outer_refs=None, typed_split=None):
    spec = {"version": "EXACT_EXECUTABLE_RESEARCH_SPEC_V1", "operation": "M1_LIGHTGBM_FIXED_GRID",
        "evaluation_cutoff_ns": start - 1}
    if refs or training_refs or validation_refs or outer_refs:
        spec["split_spec"] = typed_split or split_spec(experiment, cutoff=start - 1)
        spec["evaluation_cutoff_ns"] = spec["split_spec"]["evaluation_cutoff_ns"]
    return DiscoveryAttemptV2(experiment.content_hash, experiment.experiment_id, identity, 1, None,
        "1.0.0", spec, sha256_json(spec), "RESEARCH_SCRIPT", "session023-offline", {"search_units": units},
        start, start + 1, refs if training_refs is None else training_refs,
        () if validation_refs is None else validation_refs, () if outer_refs is None else outer_refs,
        None if failure else {"status": "NOT_ESTIMABLE"}, failure,
        (), viewed, experiment.final_holdout_ref)


def valid_outcome(repo, *, cutoff=CUTOFF):
    from atlas.v2.science.outcomes import index_matured_outcome

    from .test_session018_remediation import _payoff_case

    case, action, payoff, outcome = _payoff_case(repo, cutoff_ns=cutoff)
    index_matured_outcome(repo, outcome)
    return outcome


def split_for_outcome(experiment, outcome, *, outer_label=True):
    horizon = outcome.horizon_end_ns - outcome.decision_at_ns
    embargo = horizon
    if outer_label:
        return split_spec(experiment, training=(0, outcome.decision_at_ns - 3 * horizon),
            validation=(outcome.decision_at_ns - 2 * horizon, outcome.decision_at_ns - horizon),
            outer=(outcome.decision_at_ns, outcome.horizon_end_ns),
            cutoff=outcome.available_at_ns, embargo=embargo, horizon=horizon)
    return split_spec(experiment, training=(outcome.decision_at_ns - 2 * horizon, outcome.horizon_end_ns),
        validation=(outcome.horizon_end_ns + horizon, outcome.horizon_end_ns + 2 * horizon),
        outer=(outcome.horizon_end_ns + 3 * horizon, outcome.available_at_ns),
        cutoff=outcome.available_at_ns, embargo=embargo, horizon=horizon)


def test_budget_failure_and_all_attempts_are_retained_and_family_complete(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        exp = experiment(repo, budget=1, parameter_budget=1)
        failed = attempt(exp, failure="INSUFFICIENT_CHRONOLOGY")
        register_discovery_attempt(repo, exp.content_hash, failed, available_at_ns=11)
        rejected = attempt(exp, identity="overbudget")
        with pytest.raises(ValueError, match="budget"):
            register_discovery_attempt(repo, exp.content_hash, rejected, available_at_ns=11)
        ledger = discovery_attempt_ledger(repo, exp.content_hash)
        assert len(ledger) == 2 and any(row["failure_reason"] == "INSUFFICIENT_CHRONOLOGY" for row in ledger)
        assert any(row.get("rejection_reason") == "ATTEMPT_BUDGET_EXCEEDED" for row in ledger)
        assert len(repo.artifact_entries("DiscoveryAttemptV2")) == 1
        baseline = MultiplicityVariantV2("baseline", exp.baseline_policy_ref, (1,), (Decimal(0),))
        variants = tuple(MultiplicityVariantV2(row["attempt_id"], row["proposal_hash"], (), (),
            row.get("failure_reason") or row.get("rejection_reason")) for row in ledger)
        audit = audit_discovery_multiplicity(repo, experiment_ref=exp.content_hash, baseline=baseline,
            variants=variants, bootstrap_replicates=100)
        assert audit.status == "NOT_ESTIMABLE" and len(audit.members) == 3
        with pytest.raises(ValueError, match="every attempted"):
            audit_discovery_multiplicity(repo, experiment_ref=exp.content_hash, baseline=baseline,
                variants=variants[:1], bootstrap_replicates=100)


def test_durable_report_retains_all_preregistered_variants_without_economic_search(tmp_path):
    from atlas.v2.science.session023_report import build_session023_research_report

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        report = build_session023_research_report(repo, preregistered_at_ns=1)
        assert len(report["attempts"]) == report["experiment"]["maximum_attempts"] == 17
        assert all(item["failure_reason"] and not item["holdout_viewed"] for item in report["attempts"])
        assert len(report["multiplicity"]["members"]) == 18
        assert report["multiplicity"]["status"] == "NOT_ESTIMABLE"
        assert report["holdout"]["state"] == "UNTOUCHED" and not report["capital_enabled"]
        population = report["holdout"]["population"]
        assert population["assignment_status"] == "NOT_ESTIMABLE"
        assert population["final_holdout_start_ns"] is population["final_holdout_end_ns"] is None
        assert population["source_evidence_revision_ref"] is None
        assert population["assignment_rule"] == FINAL_HOLDOUT_ASSIGNMENT_RULE


def test_parameter_budget_is_independent_and_rejected_search_retained(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        exp = experiment(repo, budget=3, parameter_budget=1)
        with pytest.raises(ValueError, match="parameter search budget"):
            register_discovery_attempt(repo, exp.content_hash, attempt(exp, units=2), available_at_ns=11)
        assert not repo.artifact_entries("DiscoveryAttemptV2")
        assert discovery_attempt_ledger(repo, exp.content_hash)[0]["rejection_reason"] == "PARAMETER_SEARCH_BUDGET_EXCEEDED"


def test_viewed_holdout_is_globally_spent_and_cannot_reset_or_reuse(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        exp = experiment(repo, budget=4, parameter_budget=4, holdout_start_ns=CUTOFF)
        with pytest.raises(ValueError, match="holdout_viewed"):
            register_discovery_attempt(repo, exp.content_hash, attempt(exp, viewed=True), available_at_ns=11)
        outcome = valid_outcome(repo)
        view = DiscoveryOutcomeViewV2(exp.content_hash, outcome.content_hash, "OUTER",
            outcome.available_at_ns, exp.final_holdout_ref)
        view_ref = index_discovery_outcome_view(repo, view)
        start = outcome.available_at_ns + 2
        viewed = attempt(exp, identity="view-one", viewed=True, outer_refs=(view_ref,), start=start,
            typed_split=split_for_outcome(exp, outcome))
        register_discovery_attempt(repo, exp.content_hash, viewed, available_at_ns=start + 1)
        spent_at = holdout_spent_at(repo, exp.content_hash)
        assert spent_at == start + 1
        state = repo.artifact_entries("DiscoveryHoldoutStateV2")[0].metadata["holdout_state"]
        assert state["attempt_ref"] == viewed.content_hash and state["evidence_refs"] == (view_ref,)
        second_view = attempt(exp, identity="view-two", viewed=True, outer_refs=(view_ref,),
            start=spent_at + 2, typed_split=split_for_outcome(exp, outcome))
        with pytest.raises(ValueError, match="genuinely later future evidence|SPENT"):
            register_discovery_attempt(repo, exp.content_hash, second_view, available_at_ns=spent_at + 3)
        assert any(row["attempt_id"] == "view-two" and row.get("rejection_reason")
            for row in discovery_attempt_ledger(repo, exp.content_hash))
        raw_second_view = attempt(exp, identity="raw-second-view", viewed=True,
            outer_refs=(outcome.content_hash,), start=spent_at + 4,
            typed_split=split_for_outcome(exp, outcome))
        with pytest.raises(ValueError, match="genuinely later future evidence|SPENT"):
            register_discovery_attempt(repo, exp.content_hash, raw_second_view, available_at_ns=spent_at + 5)
        assert any(row["attempt_id"] == "raw-second-view" and row.get("rejection_reason")
            for row in discovery_attempt_ledger(repo, exp.content_hash))
        with pytest.raises(ValueError, match="SPENT|fresh future"):
            mark_holdout_spent(repo, experiment_ref=exp.content_hash, holdout_ref=exp.final_holdout_ref,
                attempt_id="reset", attempt_ref=sha256_json("reset"), evidence_refs=(view_ref,), viewed_at_ns=spent_at + 1)
        for renamed in (replace(exp, experiment_id="renamed"), replace(exp, family_id="renamed-family")):
            with pytest.raises(ValueError, match="population identity"):
                register_discovery_experiment(repo, renamed, available_at_ns=spent_at + 1)
        with pytest.raises(ValueError, match="typed population contract"):
            register_discovery_experiment(repo, replace(exp, final_holdout_ref=sha256_json("omitted-population")),
                available_at_ns=spent_at + 1)
        with pytest.raises(ValueError, match="overlaps an existing immutable population"):
            experiment(repo, holdout_start_ns=CUTOFF, population_id="renamed-family-population")
        prior_population = DiscoveryHoldoutPopulationV2.from_dict(
            repo.get_artifact(exp.final_holdout_ref).metadata["population"])
        next_start = int(prior_population.final_holdout_end_ns) + 1
        next_end = next_start + 30 * DAY_NS
        next_source_body = {"version": "DISCOVERY_HOLDOUT_ASSIGNMENT_EVIDENCE_V1",
            "population_id": "redesign-final-holdout", "population_version": 1,
            "experiment_id": "redesigned-experiment", "family_id": "redesigned-family",
            "chronology_version": prior_population.chronology_version,
            "assignment_rule": FINAL_HOLDOUT_ASSIGNMENT_RULE,
            "final_holdout_start_ns": next_start, "final_holdout_end_ns": next_end,
            "decision_calendar_population": DECISION_CALENDAR_POPULATION_V1,
            "venue_product_policy_universe": [["*", "*", "*"]],
            "decision_event_identity_rules": DECISION_EVENT_IDENTITY_RULES_V1,
            "assignment_available_at_ns": 1}
        next_source_ref = sha256_json(next_source_body)
        repo.register_artifact(ArtifactIndexEntryV2(next_source_ref, "DiscoveryHoldoutAssignmentEvidenceV2",
            next_source_ref, 0, 0, {"assignment_evidence": next_source_body}))
        next_population = DiscoveryHoldoutPopulationV2("redesign-final-holdout", 1,
            "redesigned-experiment", "redesigned-family", prior_population.chronology_version,
            FINAL_HOLDOUT_ASSIGNMENT_RULE, "ASSIGNED", next_start, next_end,
            DECISION_CALENDAR_POPULATION_V1, (("*", "*", "*"),), DECISION_EVENT_IDENTITY_RULES_V1,
            next_source_ref, 1, 1)
        next_population_ref = index_discovery_holdout_population(repo, next_population)
        redesigned = replace(exp, experiment_id="redesigned-experiment", family_id="redesigned-family",
            final_holdout_ref=next_population_ref)
        register_discovery_experiment(repo, redesigned, available_at_ns=spent_at + 6)
        stale_raw = attempt(redesigned, identity="renamed-family-old-raw-evidence", outer_refs=(outcome.content_hash,),
            start=spent_at + 8, typed_split=split_for_outcome(redesigned, outcome))
        with pytest.raises(ValueError, match="genuinely later future evidence"):
            register_discovery_attempt(repo, redesigned.content_hash, stale_raw, available_at_ns=spent_at + 9)
        assert discovery_attempt_ledger(repo, redesigned.content_hash)[0]["rejection_reason"] == \
            "SPENT_HOLDOUT_REDESIGN_REQUIRES_FRESH_FUTURE_EVIDENCE"
        with pytest.raises(ValueError, match="genuinely later future evidence"):
            register_discovery_attempt(repo, exp.content_hash,
                attempt(exp, identity="redesign", start=spent_at + 2), available_at_ns=spent_at + 3)
        population = repo.get_artifact(exp.final_holdout_ref).metadata["population"]
        minute_ns = 60 * 1_000_000_000
        fresh_cutoff = int(population["final_holdout_end_ns"]) + minute_ns
        fresh_outcome = valid_outcome(repo, cutoff=fresh_cutoff)
        fresh_split = split_for_outcome(exp, fresh_outcome)
        fresh_start = fresh_outcome.available_at_ns + 2
        redesign = attempt(exp, identity="fresh-redesign", outer_refs=(fresh_outcome.content_hash,),
            start=fresh_start, typed_split=fresh_split)
        register_discovery_attempt(repo, exp.content_hash, redesign, available_at_ns=fresh_start + 1)
        assert holdout_spent_at(repo, exp.content_hash) == spent_at
        assert any(row["attempt_id"] == "fresh-redesign" and row.get("rejection_reason") is None
            for row in discovery_attempt_ledger(repo, exp.content_hash))


def test_future_labels_cannot_enter_proposal_evaluation_and_authority_is_zero(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        exp = experiment(repo)
        future = sha256_json("future-label")
        repo.register_artifact(ArtifactIndexEntryV2(future, "MaturedOutcomeV2", future, 100, 100, {}))
        with pytest.raises(ValueError, match="MaturedOutcomeV2"):
            register_discovery_attempt(repo, exp.content_hash, attempt(exp, refs=(future,)), available_at_ns=11)
        assert discovery_attempt_ledger(repo, exp.content_hash)[0]["rejection_reason"]
        with pytest.raises(ValueError, match="authority"):
            replace(attempt(exp), capital_authority=True)
        with pytest.raises(TypeError):
            attempt(exp).proposal_spec["operation"] = "LIVE_SELF_TUNING"


def test_feature_artifact_cannot_masquerade_as_action_value_training_label(tmp_path):
    from .session023_support import feature_candidate

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        exp = experiment(repo)
        feature = feature_candidate(repo)
        invalid = attempt(exp, identity="feature-as-label", training_refs=(feature.snapshot_hash,), start=10)
        with pytest.raises(ValueError, match="MaturedOutcomeV2"):
            register_discovery_attempt(repo, exp.content_hash, invalid, available_at_ns=11)
        ledger = discovery_attempt_ledger(repo, exp.content_hash)
        assert len(ledger) == 1 and ledger[0]["rejection_reason"]


def test_unmatured_typed_outcome_cannot_enter_action_value_evaluation(tmp_path):
    from atlas.v2.science.outcomes import ExecutionOutcomeStateV2, LabelStateV2, index_matured_outcome

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        exp = experiment(repo)
        outcome = valid_outcome(repo)
        unresolved = replace(outcome, label_state=LabelStateV2.UNRESOLVED,
            execution_state=ExecutionOutcomeStateV2.UNRESOLVED, gross_payoff=None, fees=None,
            funding_cashflow=None, net_payoff=None, fill_quantity=None, reason="UNMATURED_TEST_LABEL")
        index_matured_outcome(repo, unresolved)
        invalid = attempt(exp, identity="unmatured-outcome", training_refs=(unresolved.content_hash,), start=10)
        with pytest.raises(ValueError, match="unmatured|invalid MaturedOutcomeV2"):
            register_discovery_attempt(repo, exp.content_hash, invalid, available_at_ns=11)
        assert discovery_attempt_ledger(repo, exp.content_hash)[0]["rejection_reason"]


@pytest.mark.parametrize("role", ["training", "validation"])
def test_final_holdout_cannot_appear_in_training_or_validation_refs(tmp_path, role):
    with OpsRepository(tmp_path / f"ops-{role}.sqlite") as repo:
        exp = experiment(repo, holdout_start_ns=CUTOFF)
        outcome = valid_outcome(repo, cutoff=CUTOFF)
        start = CUTOFF + 22 * DAY_NS
        # The split itself assigns this exact decision to the requested ordinary role.
        split = split_spec(exp, training=(CUTOFF - HOUR_NS, CUTOFF + 5 * DAY_NS),
            validation=(CUTOFF + 6 * DAY_NS, CUTOFF + 12 * DAY_NS),
            outer=(CUTOFF + 13 * DAY_NS, CUTOFF + 20 * DAY_NS),
            cutoff=CUTOFF + 21 * DAY_NS, embargo=1, horizon=1)
        if role == "validation":
            split = split_spec(exp, training=(CUTOFF - 10 * HOUR_NS, CUTOFF - 5 * HOUR_NS),
                validation=(CUTOFF - HOUR_NS, CUTOFF + 5 * DAY_NS),
                outer=(CUTOFF + 6 * DAY_NS, CUTOFF + 20 * DAY_NS),
                cutoff=CUTOFF + 21 * DAY_NS, embargo=1, horizon=1)
        kwargs = {f"{role}_refs": (outcome.content_hash,)}
        invalid = attempt(exp, identity=f"holdout-{role}", start=start, typed_split=split, **kwargs)
        with pytest.raises(ValueError, match="FINAL_HOLDOUT_IN_TRAINING_OR_VALIDATION"):
            register_discovery_attempt(repo, exp.content_hash, invalid, available_at_ns=start + 1)
        assert discovery_attempt_ledger(repo, exp.content_hash)[0]["rejection_reason"] == \
            "FINAL_HOLDOUT_IN_TRAINING_OR_VALIDATION"


def test_holdout_linked_outer_without_explicit_view_is_rejected_and_retained(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        exp = experiment(repo, holdout_start_ns=CUTOFF)
        outcome = valid_outcome(repo, cutoff=CUTOFF)
        view = DiscoveryOutcomeViewV2(exp.content_hash, outcome.content_hash, "OUTER",
            outcome.available_at_ns, exp.final_holdout_ref)
        ref = index_discovery_outcome_view(repo, view)
        start = outcome.available_at_ns + 2
        invalid = attempt(exp, identity="implicit-holdout", outer_refs=(ref,), start=start,
            typed_split=split_for_outcome(exp, outcome), viewed=False)
        with pytest.raises(ValueError, match="explicit holdout view"):
            register_discovery_attempt(repo, exp.content_hash, invalid, available_at_ns=start + 1)
        assert discovery_attempt_ledger(repo, exp.content_hash)[0]["rejection_reason"] == "HOLDOUT_VIEW_NOT_DECLARED"


def test_raw_final_holdout_outer_requires_explicit_view_and_spends_population(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        exp = experiment(repo, holdout_start_ns=CUTOFF)
        outcome = valid_outcome(repo, cutoff=CUTOFF)
        start = outcome.available_at_ns + 2
        split = split_for_outcome(exp, outcome)
        implicit = attempt(exp, identity="raw-implicit", outer_refs=(outcome.content_hash,), start=start,
            typed_split=split, viewed=False)
        with pytest.raises(ValueError, match="explicit holdout view"):
            register_discovery_attempt(repo, exp.content_hash, implicit, available_at_ns=start + 1)
        assert discovery_attempt_ledger(repo, exp.content_hash)[0]["rejection_reason"] == "HOLDOUT_VIEW_NOT_DECLARED"

        explicit = attempt(exp, identity="raw-explicit", outer_refs=(outcome.content_hash,), start=start + 2,
            typed_split=split, viewed=True)
        register_discovery_attempt(repo, exp.content_hash, explicit, available_at_ns=start + 3)
        assert holdout_spent_at(repo, exp.content_hash) == start + 3
        state = repo.artifact_entries("DiscoveryHoldoutStateV2")[0].metadata["holdout_state"]
        assert state["holdout_ref"] == exp.final_holdout_ref
        assert state["evidence_refs"] == (outcome.content_hash,)


def test_holdout_membership_uses_half_open_fixed_population_not_wrapper_or_revision(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        exp = experiment(repo, holdout_start_ns=CUTOFF + 1)
        before = valid_outcome(repo, cutoff=CUTOFF)
        before_start = before.available_at_ns + 2
        ordinary = attempt(exp, identity="immediately-before", outer_refs=(before.content_hash,),
            start=before_start, typed_split=split_for_outcome(exp, before), viewed=False)
        register_discovery_attempt(repo, exp.content_hash, ordinary, available_at_ns=before_start + 1)
        assert holdout_spent_at(repo, exp.content_hash) is None

    with OpsRepository(tmp_path / "ops-exact-start.sqlite") as exact_repo:
        exact_exp = experiment(exact_repo, holdout_start_ns=CUTOFF)
        at_start = valid_outcome(exact_repo, cutoff=CUTOFF)
        start = at_start.available_at_ns + 2
        raw = attempt(exact_exp, identity="exact-start", outer_refs=(at_start.content_hash,),
            start=start, typed_split=split_for_outcome(exact_exp, at_start), viewed=False)
        with pytest.raises(ValueError, match="explicit holdout view"):
            register_discovery_attempt(exact_repo, exact_exp.content_hash, raw, available_at_ns=start + 1)
        assert discovery_attempt_ledger(exact_repo, exact_exp.content_hash)[-1]["rejection_reason"] == "HOLDOUT_VIEW_NOT_DECLARED"

        revised = replace(at_start, available_at_ns=at_start.available_at_ns + 100)
        from atlas.v2.science.outcomes import index_matured_outcome
        index_matured_outcome(exact_repo, revised)
        revised_start = revised.available_at_ns + 2
        revised_raw = attempt(exact_exp, identity="revised-same-decision", outer_refs=(revised.content_hash,),
            start=revised_start, typed_split=split_for_outcome(exact_exp, revised), viewed=False)
        with pytest.raises(ValueError, match="explicit holdout view"):
            register_discovery_attempt(exact_repo, exact_exp.content_hash, revised_raw, available_at_ns=revised_start + 1)
        assert discovery_attempt_ledger(exact_repo, exact_exp.content_hash)[-1]["rejection_reason"] == "HOLDOUT_VIEW_NOT_DECLARED"

        moved = replace(at_start, decision_at_ns=CUTOFF + 1)
        with pytest.raises(ValueError, match="decision state/identity must match indexed"):
            index_matured_outcome(exact_repo, moved)

        population = DiscoveryHoldoutPopulationV2.from_dict(
            exact_repo.get_artifact(exact_exp.final_holdout_ref).metadata["population"])
        with pytest.raises(ValueError, match="version is immutable"):
            index_discovery_holdout_population(exact_repo, replace(population,
                final_holdout_start_ns=CUTOFF + 1))


def test_unassigned_preregistered_population_returns_named_not_estimable(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        exp = experiment(repo, holdout_assigned=False)
        outcome = valid_outcome(repo)
        invalid = attempt(exp, identity="unassigned-population", outer_refs=(outcome.content_hash,),
            start=outcome.available_at_ns + 2, typed_split=split_for_outcome(exp, outcome))
        with pytest.raises(ValueError, match="NOT_ESTIMABLE_FINAL_HOLDOUT_POPULATION_UNASSIGNED"):
            register_discovery_attempt(repo, exp.content_hash, invalid, available_at_ns=outcome.available_at_ns + 3)
        assert discovery_attempt_ledger(repo, exp.content_hash)[0]["rejection_reason"] == \
            "NOT_ESTIMABLE_FINAL_HOLDOUT_POPULATION_UNASSIGNED"

        original = DiscoveryHoldoutPopulationV2.from_dict(
            repo.get_artifact(exp.final_holdout_ref).metadata["population"])
        boundary = CUTOFF + 100 * DAY_NS
        source_body = {"version": "DISCOVERY_HOLDOUT_ASSIGNMENT_EVIDENCE_V1",
            "population_id": original.population_id, "population_version": 2,
            "experiment_id": original.experiment_id, "family_id": original.family_id,
            "chronology_version": original.chronology_version, "assignment_rule": original.assignment_rule,
            "final_holdout_start_ns": boundary, "final_holdout_end_ns": boundary + 30 * DAY_NS,
            "decision_calendar_population": original.decision_calendar_population,
            "venue_product_policy_universe": [["*", "*", "*"]],
            "decision_event_identity_rules": original.decision_event_identity_rules,
            "assignment_available_at_ns": 1}
        source_ref = sha256_json(source_body)
        repo.register_artifact(ArtifactIndexEntryV2(source_ref, "DiscoveryHoldoutAssignmentEvidenceV2",
            source_ref, 0, 0, {"assignment_evidence": source_body}))
        assigned = replace(original, population_version=2, assignment_status="ASSIGNED",
            final_holdout_start_ns=boundary, final_holdout_end_ns=boundary + 30 * DAY_NS,
            source_evidence_revision_ref=source_ref, assignment_available_at_ns=1)
        assigned_ref = index_discovery_holdout_population(repo, assigned)
        assert assigned_ref != exp.final_holdout_ref
        revised_experiment = replace(exp, final_holdout_ref=assigned_ref)
        register_discovery_experiment(repo, revised_experiment, available_at_ns=1)


@pytest.mark.parametrize("bad_split,reason", [
    ({"wrong_role": 0}, "wrong chronological split"),
    ({"outer_end_before_horizon": 0}, "fold boundary"),
    ({"embargo_below_horizon": 0}, "embargo is below"),
])
def test_discovery_chronology_purge_boundaries_and_embargo_are_enforced(tmp_path, bad_split, reason):
    with OpsRepository(tmp_path / f"ops-{reason.replace(' ', '-')}.sqlite") as repo:
        exp = experiment(repo)
        outcome = valid_outcome(repo)
        horizon = outcome.horizon_end_ns - outcome.decision_at_ns
        base = split_for_outcome(exp, outcome)
        if "wrong_role" in bad_split:
            base = split_for_outcome(exp, outcome)
        elif "outer_end_before_horizon" in bad_split:
            base["outer_end_ns"] = outcome.horizon_end_ns - 1
        else:
            base["embargo_ns"] = horizon - 1
        start = outcome.available_at_ns + 2
        refs = {"training_refs": (outcome.content_hash,)} if "wrong_role" in bad_split else {
            "outer_refs": (outcome.content_hash,)}
        invalid = attempt(exp, identity=f"bad-split-{reason}", start=start, typed_split=base, **refs)
        with pytest.raises(ValueError, match=reason):
            register_discovery_attempt(repo, exp.content_hash, invalid, available_at_ns=start + 1)
        assert discovery_attempt_ledger(repo, exp.content_hash)[0]["rejection_reason"]


def test_overlapping_training_labels_violate_declared_purge(tmp_path):
    from .test_session017_risk import CUTOFF, HOUR_NS

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        exp = experiment(repo)
        first = valid_outcome(repo, cutoff=CUTOFF)
        second = valid_outcome(repo, cutoff=CUTOFF + HOUR_NS)
        d0, d1 = first.decision_at_ns, second.decision_at_ns
        horizon = max(first.horizon_end_ns - d0, second.horizon_end_ns - d1)
        training_end = second.horizon_end_ns + 1
        validation_start = training_end + horizon
        validation_end = validation_start + HOUR_NS
        outer_start = validation_end + horizon
        outer_end = outer_start + HOUR_NS
        split = split_spec(exp, training=(d0 - 1, training_end),
            validation=(validation_start, validation_end), outer=(outer_start, outer_end),
            cutoff=outer_end, embargo=horizon, horizon=horizon)
        start = outer_end + 2
        invalid = attempt(exp, identity="overlapping-training-labels",
            training_refs=tuple(sorted((first.content_hash, second.content_hash))),
            start=start, typed_split=split)
        with pytest.raises(ValueError, match="overlap or violate the declared purge/embargo"):
            register_discovery_attempt(repo, exp.content_hash, invalid, available_at_ns=start + 1)
        assert discovery_attempt_ledger(repo, exp.content_hash)[0]["rejection_reason"]


def basket_fixture():
    other = alternate_key()
    pair = S8PairDefinitionV2("BTC_ETH", "BTC and ETH shared crypto exposure; hourly residual research only",
        KEY, other, "OLS_LOG_PRICE_A_ON_LOG_PRICE_B_30D_HOURLY_V1", "LOG_A_MINUS_ALPHA_MINUS_BETA_LOG_B")
    a, b = [], []
    for index in range(FIT_HOURS + 1):
        price_b = math.exp(4.0 + index * 0.0005)
        price_a = math.exp(0.2 + 1.1 * math.log(price_b) + 0.002 * math.sin(index * 0.5))
        for target, key, price in ((a, KEY, price_a), (b, other, price_b)):
            target.append(S8HourlyPriceV2(key, index * HOUR_NS, index * HOUR_NS,
                Decimal(str(price)), sha256_json([key.content_hash, index])))
    evidence_a = S8LegEvidenceV2(KEY, (a[-1].source_ref,), (), None, (), ("PARTIAL_ALLOWED",),
        ("SEQUENTIAL_DELAY_UNKNOWN",), ("ORPHAN_LEG_POSSIBLE",), FIT_HOURS * HOUR_NS)
    evidence_b = replace(evidence_a, instrument_key=other, price_refs=(b[-1].source_ref,))
    forecast = build_research_basket_forecast(pair, prices_a=a, prices_b=b, cutoff_ns=FIT_HOURS * HOUR_NS,
        leg_a_evidence=evidence_a, leg_b_evidence=evidence_b)
    return pair, a, b, forecast


def test_s8_causal_30_day_hourly_fit_future_tail_and_gaps():
    pair, a, b, forecast = basket_fixture()
    assert isinstance(forecast, ResearchBasketForecastV2) and forecast.fit_end_ns - forecast.fit_start_ns == 30 * 24 * HOUR_NS
    assert forecast.beta_frozen and forecast.economic_status == "NOT_ESTIMABLE"
    future_a = replace(a[-1], available_at_ns=forecast.information_cutoff_ns + 1, close=Decimal("1e40"), source_ref=sha256_json("future-a"))
    repeated = build_research_basket_forecast(pair, prices_a=(*a, future_a), prices_b=b,
        cutoff_ns=forecast.information_cutoff_ns, leg_a_evidence=forecast.leg_a_evidence,
        leg_b_evidence=forecast.leg_b_evidence)
    assert repeated.content_hash == forecast.content_hash
    with pytest.raises(ValueError, match="complete synchronized"):
        build_research_basket_forecast(pair, prices_a=a[1:], prices_b=b,
            cutoff_ns=forecast.information_cutoff_ns, leg_a_evidence=forecast.leg_a_evidence,
            leg_b_evidence=forecast.leg_b_evidence)


@pytest.mark.parametrize("z,side", [(2.0, None), (-2.0, None), (2.001, "SHORT_SPREAD"), (-2.001, "LONG_SPREAD")])
def test_s8_entry_threshold_is_exact(z, side):
    assert s8_entry_side(replace(basket_fixture()[3], current_z=z)) == side


@pytest.mark.parametrize("z,elapsed,reason", [(0.5, 0, None), (0.499, 0, "CONVERGENCE_ABS_Z_LT_0_5"),
    (3.5, 0, None), (3.501, 0, "STOP_ABS_Z_GT_3_5"), (2.0, 4 * HOUR_NS, "TIME_EXIT_FOUR_HOURS")])
def test_s8_exit_thresholds_exact(z, elapsed, reason):
    assert s8_exit_reason(basket_fixture()[3], z, elapsed) == reason


def test_s8_frozen_beta_both_leg_cost_funding_partial_delay_orphan_and_no_live_path(tmp_path):
    pair, a, b, forecast = basket_fixture()
    forecast = replace(forecast, current_z=2.5)
    execution = {"leg_a_fill_state": "PARTIAL_FILL", "leg_b_fill_state": "NO_FILL",
        "fee_refs": (sha256_json("a-fee"), sha256_json("b-fee")),
        "funding_refs": ((sha256_json("a-funding"),), (sha256_json("b-funding"),)),
        "sequential_delay_ns": (10, 2000), "orphan_leg_state": "LEG_A_UNHEDGED",
        "leg_fee_values": (Decimal("0.1"), Decimal(0)), "leg_funding_cashflows": (Decimal("-0.01"), Decimal(0))}
    result = simulate_s8_basket_path(forecast, z_values=(2.5, 2.0, 1.0, 0.6, 0.5),
        path_refs=tuple(sha256_json(index) for index in range(5)), **execution)
    assert result.beta_used == forecast.beta and not result.beta_refit_during_path
    assert result.exit_reason == "TIME_EXIT_FOUR_HOURS" and result.leg_a_fill_state == "PARTIAL_FILL"
    assert result.leg_fee_values == (Decimal("0.1"), Decimal(0)) and len(result.funding_refs) == 2
    computed = simulate_s8_synchronized_prices(forecast, prices_a=(a[-1],), prices_b=(b[-1],), **execution)
    assert computed.beta_used == forecast.beta and computed.leg_price_path_refs == ((a[-1].source_ref,), (b[-1].source_ref,))
    with pytest.raises(ValueError, match="frozen"):
        replace(result, beta_refit_during_path=True)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        persist_s8_basket(repo, pair, forecast, available_at_ns=forecast.information_cutoff_ns)
        with pytest.raises(TypeError, match="cannot be sized"):
            reject_s8_single_action(forecast)
        with pytest.raises(TypeError, match="single-action"):
            assemble_multisleeve_research_candidate_set(repo, universe=research_selection_universe(universe()),
                decision_event_id="basket", cutoff_ns=universe().decision_slot_ns, candidates=(forecast,),
                policies={}, scanner_evidence_refs={})
        for kind in ("CandidateActionV2", "TradePlanEnvelopeV2", "SizingDecisionV2", "Order", "Reservation"):
            assert not repo.artifact_entries(kind)


def test_promotion_ladder_no_skip_synthetic_or_historical_to_prospective():
    evidence = PromotionEvidenceV2(engineering_checks_ref=sha256_json("engineering"), manual_review_ref=sha256_json("review"))
    transition = validate_promotion_transition(PromotionStatusV2.INTEGRATED, PromotionStatusV2.ENGINEERING_PASS, evidence)
    assert transition["capital_enabled"] is False
    with pytest.raises(ValueError, match="skipping"):
        validate_promotion_transition(PromotionStatusV2.INTEGRATED, PromotionStatusV2.HISTORICAL_DIAGNOSTIC, evidence)
    with pytest.raises(ValueError, match="synthetic"):
        validate_promotion_transition(PromotionStatusV2.ENGINEERING_PASS, PromotionStatusV2.HISTORICAL_DIAGNOSTIC, evidence)
    historical = replace(evidence, synthetic=False, genuine_historical_evidence=True,
        historical_outer_refs=tuple(sha256_json(index) for index in range(3)))
    with pytest.raises(ValueError, match="prospective"):
        validate_promotion_transition(PromotionStatusV2.HISTORICAL_DIAGNOSTIC, PromotionStatusV2.PROSPECTIVE_SHADOW, historical)
    with pytest.raises(ValueError, match="automatic"):
        validate_promotion_transition(PromotionStatusV2.INTEGRATED, PromotionStatusV2.ENGINEERING_PASS, evidence, automatic=True)
