"""Offline preregistration and durable NOT ESTIMABLE Phase-3 research audit.

This design registry performs no economic search, consumes no final holdout,
and changes no active policy. Engineering fits belong to the synthetic test
suite; real whole-policy evidence is required before evaluating this family.
"""

from __future__ import annotations

from typing import Any

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.science.analogue import ANALOGUE_POLICY_BODY, ANALOGUE_POLICY_HASH
from atlas.v2.science.audits import (
    ABLATION_FAMILIES,
    MultiplicityVariantV2,
    declare_feature_family_ablation,
    persist_research_artifact,
)
from atlas.v2.science.discovery import (
    DECISION_CALENDAR_POPULATION_V1,
    DECISION_EVENT_IDENTITY_RULES_V1,
    FINAL_HOLDOUT_ASSIGNMENT_RULE,
    DiscoveryAttemptV2,
    DiscoveryExperimentV2,
    DiscoveryHoldoutPopulationV2,
    audit_discovery_multiplicity,
    discovery_attempt_ledger,
    index_discovery_holdout_population,
    register_discovery_attempt,
    register_discovery_experiment,
)
from atlas.v2.science.m0 import M0_MODEL_VERSION
from atlas.v2.science.m1 import M1_PARAMETER_GRID, M1_POLICY_BODY, M1_POLICY_HASH
from atlas.v2.science.research_selection import MULTI_SLEEVE_SELECTION_BODY, MULTI_SLEEVE_SELECTION_HASH
from atlas.v2.strategies.s8_pairs import S8_PROFILE_BODY, S8_PROFILE_HASH


def build_session023_research_report(repo: OpsRepository, *, preregistered_at_ns: int) -> dict[str, Any]:
    """Retain the entire bounded family and missing-evidence results, never tune it."""
    at = preregistered_at_ns
    policy_bodies = {"M1": M1_POLICY_BODY, "analogue": ANALOGUE_POLICY_BODY,
        "selector": MULTI_SLEEVE_SELECTION_BODY, "S8": S8_PROFILE_BODY}
    for name, body in policy_bodies.items():
        persist_research_artifact(repo, "OfflineResearchPolicyV2", body, available_at_ns=at, key=name)
    baseline_body = {"version": "SESSION023_M0_REQUIRED_BASELINE_V1", "model_version": M0_MODEL_VERSION,
        "selection_policy": "S1_S2_SCANNER_RANK_V1", "capital_authority": "ZERO"}
    baseline = persist_research_artifact(repo, "ResearchBaselinePolicyV2", baseline_body, available_at_ns=at)
    chronology = "180D_TRAIN_30D_INNER_30D_OUTER_MONTHLY_ADVANCE_THREE_OUTER_FINAL_30D_UNTOUCHED"
    population = DiscoveryHoldoutPopulationV2("SESSION023_PHASE3_FAMILY_FINAL_30D", 1,
        "SESSION023_OFFLINE_FAMILY_V1", "SESSION023_PHASE3_FAMILY_V1", chronology,
        FINAL_HOLDOUT_ASSIGNMENT_RULE, "NOT_ESTIMABLE", None, None, DECISION_CALENDAR_POPULATION_V1,
        (("*", "*", "*"),), DECISION_EVENT_IDENTITY_RULES_V1, None, at, None)
    holdout = index_discovery_holdout_population(repo, population)
    holdout_body = {"state": "UNTOUCHED", "population_ref": holdout, "population": population.to_dict(),
        "reason": "NOT_ESTIMABLE_FINAL_HOLDOUT_POPULATION_UNASSIGNED"}
    specifications = [(f"M1_CONFIG_{index + 1}", "M1_LIGHTGBM_FIXED_GRID", {"model_policy_hash": M1_POLICY_HASH,
        "parameters": dict(parameters)}) for index, parameters in enumerate(M1_PARAMETER_GRID)]
    specifications += [("ANALOGUE", "CAUSAL_ANALOGUE_FIXED_RETRIEVAL", {"policy_hash": ANALOGUE_POLICY_HASH}),
        ("MULTI_SLEEVE", "MULTI_SLEEVE_RESEARCH_SELECTION", {"policy_hash": MULTI_SLEEVE_SELECTION_HASH}),
        ("S8_BASKET", "S8_HOURLY_PAIRS_RESEARCH", {"policy_hash": S8_PROFILE_HASH})]
    specifications += [(f"ABLATION_{name.upper()}", "WHOLE_POLICY_FEATURE_ABLATION", {"feature_families": [name]})
        for name in ABLATION_FAMILIES]
    experiment = DiscoveryExperimentV2("SESSION023_OFFLINE_FAMILY_V1", "SESSION023_PHASE3_FAMILY_V1",
        "FROZEN_ACTION_CHALLENGERS_AND_WHOLE_POLICY_ABLATIONS", tuple(sorted(ABLATION_FAMILIES)),
        ("CUTOFF_AVAILABLE_HASH_BOUND", "HONEST_MATURED_EXACT_ACTION_LABELS", "NO_FABRICATED_EXECUTION"),
        len(specifications), len(specifications), baseline, ("calibration", "compute_latency", "whole_policy_net_value"),
        chronology,
        "PURGE_OVERLAPPING_LABELS_EMBARGO_AT_LEAST_MAX_HOLDING_HORIZON", "SESSION023_PHASE3_FAMILY_V1",
        "STOP_AT_17_PROPOSALS_OR_OPERATIONAL_FAILURE_REDESIGN_AFTER_HOLDOUT_NEEDS_FRESH_FUTURE_EVIDENCE",
        holdout, "UNTOUCHED", True, at)
    register_discovery_experiment(repo, experiment, available_at_ns=at)
    attempts = []
    for index, (identity, operation, fields) in enumerate(specifications):
        spec = {"version": "EXACT_EXECUTABLE_RESEARCH_SPEC_V1", "operation": operation,
            "evaluation_cutoff_ns": at, **fields}
        attempt = DiscoveryAttemptV2(experiment.content_hash, experiment.experiment_id, identity, 1, None,
            "1.0.0", spec, sha256_json(spec), "RESEARCH_SCRIPT", "SESSION023_OFFLINE_DESIGN_REGISTRY",
            {"search_units": 1}, at + index + 1, at + index + 2, (), (), (), None,
            "NOT_ESTIMABLE_GENUINE_CHRONOLOGICAL_WHOLE_POLICY_EVIDENCE_UNAVAILABLE", (), False, holdout)
        register_discovery_attempt(repo, experiment.content_hash, attempt, available_at_ns=at + index + 2)
        attempts.append(attempt)
    variant = MultiplicityVariantV2("M0_BASELINE", baseline, (), (), "GENUINE_HISTORY_UNAVAILABLE")
    challengers = tuple(MultiplicityVariantV2(item.attempt_id, item.proposal_hash, (), (), item.failure_reason)
        for item in attempts)
    multiplicity = audit_discovery_multiplicity(repo, experiment_ref=experiment.content_hash,
        baseline=variant, variants=challengers)
    multiplicity_ref = persist_research_artifact(repo, "MultiplicityAuditV2", multiplicity.to_dict(),
        available_at_ns=at + len(specifications) + 2, key="multiplicity")
    invariant_refs = {name: persist_research_artifact(repo, "WholePolicyAblationInvariantV2",
        {"version": "WHOLE_POLICY_ABLATION_INVARIANT_V1", "component": name,
        "requirement": "IDENTICAL_TO_BASELINE", "experiment_ref": experiment.content_hash},
        available_at_ns=at + len(specifications) + 2, key="invariant") for name in (
            "scanner_ref", "candidate_generation_ref", "selection_ref", "sizing_ref", "execution_assumptions_ref",
            "costs_ref", "no_fill_partial_fill_ref", "latency_ref", "gate_ref")}
    ablation = declare_feature_family_ablation(audit_id="SESSION023_ABLATION_DESIGN_V1", family_id=experiment.family_id,
        baseline_policy_ref=baseline, multiplicity_family_ref=multiplicity_ref,
        scanner_ref=invariant_refs["scanner_ref"], candidate_generation_ref=invariant_refs["candidate_generation_ref"],
        selection_ref=invariant_refs["selection_ref"], sizing_ref=invariant_refs["sizing_ref"],
        execution_assumptions_ref=invariant_refs["execution_assumptions_ref"], costs_ref=invariant_refs["costs_ref"],
        no_fill_partial_fill_ref=invariant_refs["no_fill_partial_fill_ref"], latency_ref=invariant_refs["latency_ref"],
        gate_ref=invariant_refs["gate_ref"])
    persist_research_artifact(repo, "FeatureFamilyAblationAuditV2", ablation.to_dict(),
        available_at_ns=at + len(specifications) + 2, key="ablation")
    return {"version": "SESSION023_RESEARCH_DESIGN_REPORT_V1", "experiment": experiment.to_dict(),
        "experiment_hash": experiment.content_hash, "policies": policy_bodies,
        "attempts": list(discovery_attempt_ledger(repo, experiment.content_hash)),
        "multiplicity": multiplicity.to_dict(), "multiplicity_hash": multiplicity.content_hash,
        "ablation": ablation.to_dict(), "ablation_hash": ablation.content_hash,
        "holdout": holdout_body, "economic_value": "NOT ESTIMABLE", "capital_enabled": False,
        "promotion_status": "INTEGRATED", "automatic_promotion": False, "session024_started": False}
