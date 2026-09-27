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
    DiscoveryAttemptV2,
    DiscoveryExperimentV2,
    audit_discovery_multiplicity,
    discovery_attempt_ledger,
    holdout_spent_at,
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


def experiment(repo, budget=2, parameter_budget=2):
    baseline, holdout = sha256_json("baseline"), sha256_json("untouched-holdout")
    for ref, kind in ((baseline, "PolicyV2"), (holdout, "UntouchedHoldoutIdentityV2")):
        repo.register_artifact(ArtifactIndexEntryV2(ref, kind, ref, 0, 0, {}))
    value = DiscoveryExperimentV2("experiment", "family", "numerical_challengers", ("candles",),
        ("CUT_OFF_AVAILABLE_ONLY",), budget, parameter_budget, baseline, ("whole_policy_net_value",),
        "180D_30D_30D_THREE_OUTER_MONTHLY_HOLDOUT", "MAX_HORIZON_PURGE_EMBARGO", "family",
        "STOP_AT_BUDGET_OR_OPERATIONAL_FAILURE", holdout, "UNTOUCHED", True, 1)
    register_discovery_experiment(repo, value, available_at_ns=1)
    return value


def attempt(experiment, identity="one", *, viewed=False, failure=None, refs=(), start=10, units=1):
    spec = {"version": "EXACT_EXECUTABLE_RESEARCH_SPEC_V1", "operation": "M1_LIGHTGBM_FIXED_GRID",
        "evaluation_cutoff_ns": start - 1}
    return DiscoveryAttemptV2(experiment.content_hash, experiment.experiment_id, identity, 1, None,
        "1.0.0", spec, sha256_json(spec), "RESEARCH_SCRIPT", "session023-offline", {"search_units": units},
        start, start + 1, refs, (), (), None if failure else {"status": "NOT_ESTIMABLE"}, failure,
        (), viewed, experiment.final_holdout_ref)


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


def test_parameter_budget_is_independent_and_rejected_search_retained(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        exp = experiment(repo, budget=3, parameter_budget=1)
        with pytest.raises(ValueError, match="parameter search budget"):
            register_discovery_attempt(repo, exp.content_hash, attempt(exp, units=2), available_at_ns=11)
        assert not repo.artifact_entries("DiscoveryAttemptV2")
        assert discovery_attempt_ledger(repo, exp.content_hash)[0]["rejection_reason"] == "PARAMETER_SEARCH_BUDGET_EXCEEDED"


def test_viewed_holdout_is_globally_spent_and_cannot_reset_or_reuse(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        exp = experiment(repo, budget=4, parameter_budget=4)
        register_discovery_attempt(repo, exp.content_hash, attempt(exp, viewed=True), available_at_ns=11)
        assert holdout_spent_at(repo, exp.content_hash) == 11
        with pytest.raises(ValueError, match="SPENT"):
            mark_holdout_spent(repo, experiment_ref=exp.content_hash, holdout_ref=exp.final_holdout_ref,
                attempt_id="reset", viewed_at_ns=12)
        with pytest.raises(ValueError, match="SPENT"):
            register_discovery_experiment(repo, replace(exp, experiment_id="renamed"), available_at_ns=12)
        with pytest.raises(ValueError, match="fresh future"):
            register_discovery_attempt(repo, exp.content_hash, attempt(exp, identity="redesign", start=20), available_at_ns=21)
        fresh = sha256_json("fresh-future-evidence")
        repo.register_artifact(ArtifactIndexEntryV2(fresh, "QualifiedHistoricalEvidenceV2", fresh, 15, 15, {}))
        redesign = attempt(exp, identity="fresh-redesign", refs=(fresh,), start=20)
        register_discovery_attempt(repo, exp.content_hash, redesign, available_at_ns=21)
        assert holdout_spent_at(repo, exp.content_hash) == 11


def test_future_labels_cannot_enter_proposal_evaluation_and_authority_is_zero(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        exp = experiment(repo)
        future = sha256_json("future-label")
        repo.register_artifact(ArtifactIndexEntryV2(future, "MaturedOutcomeV2", future, 100, 100, {}))
        with pytest.raises(ValueError, match="future"):
            register_discovery_attempt(repo, exp.content_hash, attempt(exp, refs=(future,)), available_at_ns=11)
        assert discovery_attempt_ledger(repo, exp.content_hash)[0]["rejection_reason"] == "FUTURE_OR_UNAVAILABLE_EVALUATION_LABEL"
        with pytest.raises(ValueError, match="authority"):
            replace(attempt(exp), capital_authority=True)
        with pytest.raises(TypeError):
            attempt(exp).proposal_spec["operation"] = "LIVE_SELF_TUNING"


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
