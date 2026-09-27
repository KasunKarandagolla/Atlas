"""Research promotion validation, frozen-action comparison and Phase-3 gate."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from atlas.v2._serialization import FrozenMap, json_value, sha256_json, sha256_ref
from atlas.v2.models.protocol import PromotionStatusV2
from atlas.v2.science.analogue import AnalogueActionValueV2
from atlas.v2.science.m0 import M0PredictionV2
from atlas.v2.science.m1 import M1RunV2

PROMOTION_LADDER = tuple(PromotionStatusV2)
PROMOTION_VERSION = "RESEARCH_PROMOTION_TRANSITION_V2_V1"
COMPARISON_VERSION = "FROZEN_ACTION_M0_M1_ANALOGUE_COMPARISON_V2_V1"
PHASE3_GATE_VERSION = "PHASE3_ENGINEERING_GATE_V2_V1"
GATE_CHECKS = (
    "s1_input_action_complete", "s2_input_action_complete", "s3_unavailable_inputs_named",
    "s4_book_gaps_invalidate_flow", "s5_coverage_uncertainty_propagates", "s6_insufficient_breadth_named",
    "s7_duplicates_do_not_duplicate_watches_candidates", "event_receipt_latency_preserved",
    "s8_outside_normal_tradeplan", "m1_chronological_oof_only", "analogue_cutoff_maturity_valid",
    "all_exact_action_competitors_retained", "non_action_sleeves_explicitly_excluded",
    "multiplicity_audit_exists", "selection_audit_exists", "discovery_failures_retained",
    "spent_holdout_immutable", "s1_s2_baseline_reproducible", "capital_authority_unchanged",
    "ablation_audit_exists", "tier_c_passed", "actual_lightgbm_tested",
)


@dataclass(frozen=True)
class PromotionEvidenceV2:
    engineering_checks_ref: str | None = None
    historical_outer_refs: tuple[str, ...] = ()
    genuine_historical_evidence: bool = False
    synthetic: bool = True
    prospective_shadow_refs: tuple[str, ...] = ()
    incremental_after_cost_review_ref: str | None = None
    calibration_stability_compute_operations_review_ref: str | None = None
    manual_review_ref: str | None = None
    holdout_state: str = "UNTOUCHED"

    def __post_init__(self) -> None:
        for value in (self.engineering_checks_ref, self.incremental_after_cost_review_ref,
            self.calibration_stability_compute_operations_review_ref, self.manual_review_ref,
            *self.historical_outer_refs, *self.prospective_shadow_refs):
            if value is not None:
                sha256_ref(value, field="promotion_evidence_ref")


def validate_promotion_transition(current: PromotionStatusV2, proposed: PromotionStatusV2,
        evidence: PromotionEvidenceV2, *, automatic: bool = False) -> FrozenMap:
    """Validate one stage only; return evidence without mutating any live authority."""
    current, proposed = PromotionStatusV2(current), PromotionStatusV2(proposed)
    if automatic or PROMOTION_LADDER.index(proposed) != PROMOTION_LADDER.index(current) + 1:
        raise ValueError("promotion requires a reviewed single-stage transition; no skipping or automatic promotion")
    if evidence.manual_review_ref is None:
        raise ValueError("promotion requires explicit review evidence, not a single metric")
    if proposed == PromotionStatusV2.ENGINEERING_PASS and evidence.engineering_checks_ref is None:
        raise ValueError("engineering promotion requires completed engineering evidence")
    if PROMOTION_LADDER.index(proposed) >= PROMOTION_LADDER.index(PromotionStatusV2.HISTORICAL_DIAGNOSTIC):
        if evidence.synthetic or not evidence.genuine_historical_evidence or len(set(evidence.historical_outer_refs)) < 3:
            raise ValueError("synthetic tests cannot supply genuine chronological historical promotion")
        if evidence.holdout_state not in {"UNTOUCHED", "SPENT"}:
            raise ValueError("promotion must retain the immutable final holdout state")
    if PROMOTION_LADDER.index(proposed) >= PROMOTION_LADDER.index(PromotionStatusV2.PROSPECTIVE_SHADOW):
        if not evidence.prospective_shadow_refs:
            raise ValueError("historical evidence cannot supply prospective shadow")
    if PROMOTION_LADDER.index(proposed) >= PROMOTION_LADDER.index(PromotionStatusV2.INCREMENTAL_VALUE_PASS):
        if not evidence.incremental_after_cost_review_ref or not evidence.calibration_stability_compute_operations_review_ref:
            raise ValueError("incremental promotion requires after-cost, calibration, stability and operational review")
    return FrozenMap({"version": PROMOTION_VERSION, "current": current.value, "proposed": proposed.value,
        "evidence": evidence.__dict__, "capital_enabled": False, "authority_mutation": False})


@dataclass(frozen=True)
class FrozenActionModelComparisonV2:
    action_hash: str
    action_artifact_ref: str
    candidate_ref: str
    candidate_set_ref: str
    decision_cutoff_ns: int
    m0_prediction_ref: str
    m1_prediction_ref: str
    analogue_ref: str
    diagnostics: FrozenMap
    status: str = "NOT_ESTIMABLE"

    def __post_init__(self) -> None:
        for name in ("action_hash", "action_artifact_ref", "candidate_ref", "candidate_set_ref",
            "m0_prediction_ref", "m1_prediction_ref", "analogue_ref"):
            sha256_ref(getattr(self, name), field=name)
        object.__setattr__(self, "diagnostics", FrozenMap(self.diagnostics))
        if self.status != "NOT_ESTIMABLE":
            raise ValueError("Session-023 comparison has no verified incremental economic evidence")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": COMPARISON_VERSION, **self.__dict__, "costs_already_in_target": True,
            "promotion_status": "INTEGRATED", "capital_authority": "ZERO", "model_voting": False,
            "model_averaging": False, "baseline": "M0", "incremental_difference_vs_m0": None,
            "incremental_uncertainty": None})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def compare_frozen_action(*, m0: M0PredictionV2, m1: M1RunV2, analogue: AnalogueActionValueV2,
        compute_ms: Mapping[str, Decimal | None] | None = None) -> FrozenActionModelComparisonV2:
    prediction = m1.prediction
    if (m0.action_hash != prediction.action_hash or m0.action_hash != analogue.query_action_hash
        or m0.action_artifact_ref != prediction.action_artifact_ref or m0.action_artifact_ref != analogue.query_action_ref
        or prediction.candidate_ref != analogue.query_candidate_ref
        or prediction.candidate_set_ref != analogue.query_candidate_set_ref
        or m0.training_cutoff_ns != prediction.information_cutoff_ns
        or prediction.information_cutoff_ns != analogue.information_cutoff_ns):
        raise ValueError("all research comparisons must evaluate the same frozen exact action and cutoff")
    return FrozenActionModelComparisonV2(m0.action_hash, m0.action_artifact_ref, prediction.candidate_ref,
        prediction.candidate_set_ref, prediction.information_cutoff_ns, m0.content_hash,
        prediction.content_hash, analogue.content_hash, FrozenMap({
            "m0": {"estimate": m0.expected_net_value, "support_ref": m0.support_ref,
                "calibration_ref": m0.calibration_ref, "ood_ref": m0.ood_ref, "status": m0.status},
            "m1": {"estimate": prediction.expected_net_value, "support": m1.support.to_dict(),
                "calibration": m1.calibration.to_dict(), "ood": m1.ood.to_dict(),
                "model_fit_ref": prediction.model_fit_ref, "compatibility_key": prediction.compatibility_key,
                "status": prediction.status},
            "analogue": {"estimate": analogue.weighted_estimate, "support": analogue.independent_support_count,
                "effective_support": analogue.effective_sample_size, "ood": analogue.ood_status,
                "compatibility_key": analogue.compatibility_key, "status": analogue.support_status,
                "independent_vote": False},
            "coverage": {"m0": m0.expected_net_value is not None, "m1": prediction.expected_net_value is not None,
                "analogue": analogue.weighted_estimate is not None},
            "operational_degradation": {"m1_missing_features": list(m1.support.missing_feature_counts),
                "analogue_missing_features": list(analogue.missing_features)},
            "compute_ms": dict(compute_ms or {"m0": None, "m1": None, "analogue": None}),
            "outer_chronology": m1.chronology.to_dict(), "economic_value": "NOT_ESTIMABLE"}))


@dataclass(frozen=True)
class Phase3EngineeringGateV2:
    starting_sha: str
    checks: FrozenMap
    validation_manifest_ref: str
    preserved_identities: FrozenMap

    def __post_init__(self) -> None:
        sha256_ref(self.validation_manifest_ref, field="validation_manifest_ref")
        object.__setattr__(self, "checks", FrozenMap(self.checks))
        object.__setattr__(self, "preserved_identities", FrozenMap(self.preserved_identities))
        if set(self.checks) != set(GATE_CHECKS) or any(type(value) is not bool for value in self.checks.values()):
            raise ValueError("Phase-3 gate must check every declared engineering requirement explicitly")

    def to_dict(self) -> dict[str, Any]:
        return {"version": PHASE3_GATE_VERSION, "starting_sha": self.starting_sha,
            "checks": self.checks.to_dict(), "validation_manifest_ref": self.validation_manifest_ref,
            "preserved_identities": self.preserved_identities.to_dict(),
            "phase3_engineering": "TESTED" if all(self.checks.values()) else "UNVERIFIED",
            "economic_value": "NOT ESTIMABLE", "live_feed_qualification": "UNVERIFIED / TEST GATE",
            "capital_enabled": False, "session024_started": False}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())
