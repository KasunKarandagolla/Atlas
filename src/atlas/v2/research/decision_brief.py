"""Versioned compact read model. No selection, sizing, valuation or dispatch effects."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

from atlas.v2._serialization import FrozenMap, canonical_json, nonblank, sha256_json, sha256_ref, timestamp
from atlas.v2.contracts import CandidateActionV2, CandidateSetV2, EligibilityStatusV2
from atlas.v2.data.bars import CausalBarV2
from atlas.v2.features.peer_context import PeerRelationshipV1
from atlas.v2.instruments import InstrumentKeyV2, UniverseContractV2
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.science.action import ActionArtifactV2
from atlas.v2.science.analogue import AnalogueActionValueV2
from atlas.v2.science.evaluation_service import Phase2EvaluationResultV2
from atlas.v2.science.m1 import M1RunV2

BRIEF_VERSION = "ATLAS_DECISION_BRIEF_RESEARCH_V1"
SCHEMA_HASH = sha256_json(
    {
        "version": BRIEF_VERSION,
        "authority": "ZERO",
        "sections": ["binding", "calendar", "market", "economics", "ml", "memory", "limits"],
        "response_options": ["SELECT_ONE", "REJECT_ALL", "ABSTAIN"],
    }
)
SECTIONS = frozenset(
    {
        "completed_candles",
        "technical",
        "structure",
        "liquidity",
        "derivatives",
        "source_health",
        "btc_eth",
        "events",
        "prior_analysis",
        "watches",
        "invalidators",
        "due_work",
        "outcomes",
    }
)
TOOLS = ("inspect_snapshot_v1", "inspect_economics_v1", "inspect_memory_v1")


@dataclass(frozen=True)
class BriefLimitsV1:
    """Explicit fixture/deployment configuration; no implicit owner spend allowance."""

    version: str
    max_rows: int
    max_bytes: int
    max_instruments: int
    max_image_refs: int
    max_projected_tokens: int

    def __post_init__(self) -> None:
        nonblank(self.version, field="limits.version")
        for name in ("max_rows", "max_bytes", "max_instruments", "max_projected_tokens"):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise ValueError(f"invalid {name}")
        if type(self.max_image_refs) is not int or self.max_image_refs < 0:
            raise ValueError("invalid max_image_refs")


@dataclass(frozen=True)
class BriefFactV1:
    section: str
    source_ref: str
    key: InstrumentKeyV2 | None
    observed_at_ns: int
    available_at_ns: int
    valid_until_ns: int
    status: str
    values: FrozenMap
    missingness: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.section not in SECTIONS or self.status not in ("AVAILABLE", "MISSING", "NOT_ESTIMABLE", "STALE"):
            raise ValueError("unknown fact section/status")
        sha256_ref(self.source_ref, field="source_ref")
        for n in ("observed_at_ns", "available_at_ns", "valid_until_ns"):
            timestamp(getattr(self, n), field=n)
        if self.observed_at_ns > self.available_at_ns or self.available_at_ns > self.valid_until_ns:
            raise ValueError("fact chronology")
        object.__setattr__(self, "values", FrozenMap(self.values))
        object.__setattr__(self, "missingness", tuple(sorted(set(self.missingness))))
        if self.status != "AVAILABLE" and not self.missingness:
            raise ValueError("unavailable fact requires missingness")

    def to_dict(self) -> dict[str, Any]:
        return {
            "section": self.section,
            "source_ref": self.source_ref,
            "key": self.key.to_dict() if self.key else None,
            "observed_at_ns": self.observed_at_ns,
            "available_at_ns": self.available_at_ns,
            "valid_until_ns": self.valid_until_ns,
            "status": self.status,
            "values": self.values.to_dict(),
            "missingness": list(self.missingness),
        }


def technical_fact(
    *,
    key: InstrumentKeyV2,
    source_ref: str,
    bars: tuple[CausalBarV2, ...],
    cutoff_ns: int,
    valid_until_ns: int,
    values: Mapping[str, Any],
) -> BriefFactV1:
    """Label gapped/unqualified TECHNICAL_V1 outputs without changing indicator math."""
    eligible = tuple(b for b in bars if b.final and b.close_at_ns <= cutoff_ns and b.raw.available_at_ns <= cutoff_ns)
    reasons = []
    if not eligible:
        reasons.append("NO_COMPLETED_BARS")
    elif any(b.instrument_revision != key.contract_revision for b in eligible):
        reasons.append("PRODUCT_REVISION")
    elif any(b.interval != eligible[0].interval for b in eligible):
        reasons.append("INTERVAL_MISMATCH")
    elif any(right.open_at_ns != left.close_at_ns for left, right in zip(eligible, eligible[1:], strict=False)):
        reasons.append("SPARSE_BAR_FEATURE_UNQUALIFIED")
    at = max((b.raw.available_at_ns for b in eligible), default=cutoff_ns)
    observed = max((b.close_at_ns for b in eligible), default=cutoff_ns)
    return BriefFactV1(
        "technical",
        source_ref,
        key,
        observed,
        at,
        valid_until_ns,
        "NOT_ESTIMABLE" if reasons else "AVAILABLE",
        FrozenMap(
            {
                "features": {} if reasons else values,
                "input_bar_refs": [b.content_hash for b in eligible],
                "feature_version": "TECHNICAL_V1",
                "qualification": "GAP_CHECK_ONLY",
            }
        ),
        tuple(reasons),
    )


@dataclass(frozen=True)
class ForecastEvidenceV1:
    source_ref: str
    model_ref: str
    checkpoint_ref: str
    calibration_ref: str
    key: InstrumentKeyV2
    target: str
    horizon_ns: int
    input_cutoff_ns: int
    available_at_ns: int
    validated_support: bool
    installed_checkpoint_verified: bool
    limitations: tuple[str, ...]
    values: FrozenMap

    def __post_init__(self) -> None:
        for name in ("source_ref", "model_ref", "checkpoint_ref", "calibration_ref"):
            sha256_ref(getattr(self, name), field=name)
        for name in ("input_cutoff_ns", "available_at_ns", "horizon_ns"):
            timestamp(getattr(self, name), field=name)
        if self.horizon_ns <= 0 or not self.limitations:
            raise ValueError("forecast requires horizon and limitations")
        object.__setattr__(self, "values", FrozenMap(self.values))


@dataclass(frozen=True)
class EconomicsEvidenceV1:
    """Only an existing exact-action result; this view never evaluates alternatives."""

    candidate: CandidateActionV2
    action: ActionArtifactV2
    result: Phase2EvaluationResultV2
    m1: M1RunV2 | None = None
    analogue: AnalogueActionValueV2 | None = None


def _indexed(repo: OpsRepository | None, ref: str, at_ns: int) -> bool:
    if repo is None:
        return False
    row = repo.get_artifact(ref)
    return (
        row is not None
        and row.available_at_ns <= at_ns
        and repo.effective_available_at_ns(ref) is not None
        and (repo.effective_available_at_ns(ref) <= at_ns)
    )


def _economic_row(e: EconomicsEvidenceV1, request: BriefRequestV1, repo: OpsRepository | None) -> dict[str, Any]:
    a, result, c = e.action, e.result, e.candidate
    ev = result.evaluation
    reasons = []
    if (
        a.candidate_ref != c.content_hash
        or a.action.key != c.key
        or a.action.side != c.side.value
        or a.candidate_set_ref != request.candidate_set.content_hash
        or ev.candidate_set_ref != a.candidate_set_ref
        or ev.action_hash != a.action.action_hash
        or ev.action_artifact_ref != a.content_hash
        or ev.candidate_ref != c.content_hash
        or ev.quantity != a.action.quantity
        or ev.universe_ref != request.universe.content_hash
        or ev.policy_hash != c.policy_hash
        or ev.selection_policy_hash != request.candidate_set.selection_policy_hash
        or a.action.product_ref != next((x.product_ref for x in request.universe.entries if x.key == c.key), None)
    ):
        reasons.append("EXACT_ACTION_IDENTITY")
    if ev.decision_at_ns != request.cutoff_ns or ev.available_at_ns > request.built_at_ns:
        reasons.append("ECONOMIC_CHRONOLOGY")
    if ev.action_expiry_ns <= request.built_at_ns or c.deadline_ns <= request.built_at_ns:
        reasons.append("ACTION_EXPIRED")
    if result.prediction.status != "AVAILABLE" or result.m0_support.evidence_quality != "SUPPORTED":
        reasons.append("M0_SUPPORT_INSUFFICIENT")
    labels = result.m0_support.training_outcome_refs
    if not labels or any(not _indexed(repo, ref, request.cutoff_ns - 1) for ref in labels):
        reasons.append("MATURE_LABELS_MISSING_OR_FUTURE")
    if result.scenario.status.value != "AVAILABLE" or result.execution.status != "AVAILABLE":
        reasons.append("COST_FILL_SUPPORT_MISSING")
    source_inputs = (
        result.scenario.model_input,
        result.scenario.calibration_input,
        result.scenario.execution_model_input,
        *result.scenario.source_inputs,
    )
    if any(not _indexed(repo, item.ref, request.cutoff_ns) for item in source_inputs):
        reasons.append("COST_MODEL_SOURCE_UNAVAILABLE")
    if result.scenario.synthetic_fixture:
        reasons.append("SYNTHETIC_SCENARIO_UNQUALIFIED")
    cohort_row = next((x for x in request.candidate_set.candidates if x.candidate_id == c.candidate_id), None)
    if cohort_row is None or cohort_row.key != c.key or cohort_row.side != c.side:
        reasons.append("CANDIDATE_PRODUCT_IDENTITY")
    refs = {
        "evaluation": ev.content_hash,
        "action": a.content_hash,
        "prediction": result.prediction.content_hash,
        "support": result.m0_support.content_hash,
        "scenario": result.scenario.content_hash,
        "estimation": result.estimation.content_hash,
        "execution": result.execution.content_hash,
        "numerical": result.numerical.content_hash,
        "stress": result.stress.content_hash,
        "portfolio_es": result.portfolio_es.content_hash,
    }
    if repo is None or any(not _indexed(repo, ref, request.built_at_ns) for ref in refs.values()):
        reasons.append("ECONOMIC_EVIDENCE_UNINDEXED")
    elif any(repo.get_artifact(refs[name]).content_hash != refs[name] for name in refs):
        reasons.append("ECONOMIC_CONTENT_IDENTITY")
    if repo is not None and _indexed(repo, ev.content_hash, request.built_at_ns):
        if canonical_json(repo.get_artifact(ev.content_hash).metadata.get("evaluation")) != canonical_json(
            ev.to_dict()
        ):
            reasons.append("EVALUATION_CONTENT_MISMATCH")
    if result.evaluation_ref != ev.content_hash or ev.m0_prediction_ref != result.prediction.content_hash:
        reasons.append("EVALUATION_BINDING")
    if not reasons and repo is not None:
        from atlas.v2.science.scenario_engine import validate_pretrade_scenario_evidence

        try:
            if validate_pretrade_scenario_evidence(repo, action=a, scenario=result.scenario) != result.payoffs:
                reasons.append("COST_PAYOFF_EVIDENCE_MISMATCH")
        except (ValueError, KeyError, TypeError):
            reasons.append("COST_FILL_EVIDENCE_MISSING")
    supplemental = _supplemental_economics(e, request, repo)
    qualified = not reasons and ev.decision.value == "CANDIDATE"
    # Full frozen evaluation retains cost/model/scenario/uncertainty/ES/risk refs;
    # invalid inputs cannot emit a numerical estimate in this view.
    return {
        "candidate_id": c.candidate_id,
        "information_cutoff_ns": request.cutoff_ns,
        "available_at_ns": max(a.available_at_ns, ev.available_at_ns),
        "action_ref": a.content_hash,
        "action_hash": a.action.action_hash,
        "status": "ASSESSED_EXACT_ACTION" if not reasons else "NOT_ESTIMABLE",
        "supported_choice": qualified,
        "missingness": sorted(set(reasons)),
        "evidence_refs": refs,
        "evaluation": ev.to_dict() if not reasons else None,
        "support": {
            "support_ref": result.m0_support.content_hash,
            "evidence_quality": result.m0_support.evidence_quality,
            "eligible_sample_count": result.m0_support.eligible_sample_count,
            "independent_support_count": result.m0_support.independent_support_count,
            "provenance_counts": result.m0_support.provenance_counts,
            "execution_state_counts": result.m0_support.execution_state_counts,
            "missing_feature_coverage": result.m0_support.missing_feature_coverage,
            "training_start_ns": result.m0_support.training_start_ns,
            "training_end_ns": result.m0_support.training_end_ns,
            "mature_label_ref_count": len(labels),
            "complete_label_refs_in_support_artifact": True,
        }
        if not reasons
        else None,
        "cost_fill_assumptions": {
            "scenario_ref": result.scenario.content_hash,
            "status": result.scenario.status.value,
            "generation_version": result.scenario.generation_version,
            "common_scenario_set_id": result.scenario.common_scenario_set_id,
            "scenario_count": result.scenario.scenario_count,
            "source_inputs": [item.to_dict() for item in source_inputs],
            "execution_model_ref": result.scenario.execution_model_input.ref,
            "path_count": len(result.payoffs),
            "fill_quantity_states": sorted({str(p.filled_quantity) for p in result.payoffs}),
            "fees_scenario_range": [
                min((p.fees for p in result.payoffs), default=None),
                max((p.fees for p in result.payoffs), default=None),
            ],
            "funding_scenario_range": [
                min((p.funding for p in result.payoffs), default=None),
                max((p.funding for p in result.payoffs), default=None),
            ],
            "path_ranges_are_not_profit_probabilities": True,
            "complete_scenario_paths_in_artifact": True,
            "scenario_support_ref": result.scenario_support.content_hash,
            "scenario_independent_support": result.scenario_support.independent_support_unit_count,
            "ood": result.ood.to_dict(),
            "stress": result.stress.to_dict(),
            "es": result.portfolio_es.to_dict(),
            "uncertainty": {
                "mean_estimation": result.estimation.uncertainty_amount,
                "execution_cost_error_q90": result.execution.absolute_cost_error_q90,
                "execution_independent_support": result.execution.independent_support_count,
            },
        }
        if not reasons
        else None,
        **supplemental,
    }


def _supplemental_economics(
    e: EconomicsEvidenceV1, request: BriefRequestV1, repo: OpsRepository | None
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "m1": {"status": "UNAVAILABLE", "reason": "NO_ELIGIBLE_M1_BINDING"},
        "analogue": {"status": "UNAVAILABLE", "reason": "NO_ELIGIBLE_ANALOGUE_BINDING"},
    }
    if e.m1 is not None:
        m, p = e.m1, e.m1.prediction
        refs = (
            p.content_hash,
            m.model_fit.content_hash,
            m.support.content_hash,
            m.calibration.content_hash,
            m.ood.content_hash,
        )
        eligible = (
            p.action_hash == e.action.action.action_hash
            and p.action_artifact_ref == e.action.content_hash
            and p.candidate_ref == e.candidate.content_hash
            and p.candidate_set_ref == request.candidate_set.content_hash
            and p.information_cutoff_ns == request.cutoff_ns
            and p.available_at_ns <= request.built_at_ns
            and p.status == "AVAILABLE"
            and m.support.status == "SUPPORTED"
            and m.calibration.status == "OOF_CALIBRATED"
            and m.ood.status == "IN_DISTRIBUTION"
            and bool(p.training_row_refs)
            and all(_indexed(repo, ref, request.built_at_ns) for ref in refs)
        )
        if eligible:
            for ref in p.training_row_refs:
                row = repo.get_artifact(ref) if repo else None
                training = row.metadata.get("training_row", {}) if row else {}
                if not training or training.get("label_available_at_ns", request.cutoff_ns) >= request.cutoff_ns:
                    eligible = False
                    break
                if not _indexed(repo, training.get("outcome_ref", ""), request.cutoff_ns - 1):
                    eligible = False
                    break
        result["m1"] = {
            "status": "QUALIFIED_DIAGNOSTIC" if eligible else "NOT_ESTIMABLE",
            "prediction_ref": p.content_hash,
            "prediction": p.to_dict() if eligible else None,
            "missingness": [] if eligible else ["M1_IDENTITY_SUPPORT_CHRONOLOGY_UNQUALIFIED"],
        }
    if e.analogue is not None:
        a = e.analogue
        eligible = (
            a.query_action_hash == e.action.action.action_hash
            and a.query_action_ref == e.action.content_hash
            and a.query_candidate_ref == e.candidate.content_hash
            and a.query_candidate_set_ref == request.candidate_set.content_hash
            and a.information_cutoff_ns == request.cutoff_ns
            and a.support_status == "SUPPORTED"
            and a.ood_status == "IN_DISTRIBUTION"
            and bool(a.neighbors)
            and _indexed(repo, a.content_hash, request.built_at_ns)
            and all(_indexed(repo, n.outcome_ref, request.cutoff_ns - 1) for n in a.neighbors)
        )
        result["analogue"] = {
            "status": "QUALIFIED_DIAGNOSTIC" if eligible else "NOT_ESTIMABLE",
            "result_ref": a.content_hash,
            "result": a.to_dict() if eligible else None,
            "missingness": [] if eligible else ["ANALOGUE_IDENTITY_SUPPORT_CHRONOLOGY_UNQUALIFIED"],
        }
    return result


@dataclass(frozen=True)
class BriefRequestV1:
    slot_id: str
    cutoff_ns: int
    deadline_ns: int
    built_at_ns: int
    candidate_set: CandidateSetV2
    universe: UniverseContractV2
    snapshot_ref: str
    research_policy_hash: str
    limits: BriefLimitsV1
    facts: tuple[BriefFactV1, ...]
    economics: tuple[EconomicsEvidenceV1, ...]
    forecasts: tuple[ForecastEvidenceV1, ...]
    peer: PeerRelationshipV1 | None = None
    image_refs: tuple[str, ...] = ()
    forecast_target: str = "CLOSE_RETURN"
    forecast_horizon_ns: int | None = None
    budget_status: str = "NO_PAID_AUTHORIZATION"

    def __post_init__(self) -> None:
        nonblank(self.slot_id, field="slot_id")
        for n in ("snapshot_ref", "research_policy_hash"):
            sha256_ref(getattr(self, n), field=n)
        for n in ("cutoff_ns", "deadline_ns", "built_at_ns"):
            timestamp(getattr(self, n), field=n)
        if not self.cutoff_ns <= self.built_at_ns or self.deadline_ns <= self.cutoff_ns:
            raise ValueError("brief chronology")
        for ref in self.image_refs:
            sha256_ref(ref, field="image_ref")
        for n in ("facts", "economics", "forecasts", "image_refs"):
            object.__setattr__(self, n, tuple(getattr(self, n)))


@dataclass(frozen=True)
class DecisionBriefV1:
    status: str
    reasons: tuple[str, ...]
    body: FrozenMap

    def __post_init__(self) -> None:
        if self.status not in ("COMPLETE", "BLOCKED", "EXPIRED"):
            raise ValueError("invalid brief status")
        object.__setattr__(self, "body", FrozenMap(self.body))
        object.__setattr__(self, "reasons", tuple(sorted(set(self.reasons))))

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": BRIEF_VERSION,
            "status": self.status,
            "reasons": list(self.reasons),
            "body": self.body.to_dict(),
        }


def build_decision_brief(request: BriefRequestV1, *, repository: OpsRepository | None = None) -> DecisionBriefV1:
    r, cs = request, request.candidate_set
    binding = {
        "slot_id": r.slot_id,
        "cutoff_ns": r.cutoff_ns,
        "deadline_ns": r.deadline_ns,
        "built_at_ns": r.built_at_ns,
        "snapshot_ref": r.snapshot_ref,
        "schema_hash": SCHEMA_HASH,
        "research_policy_hash": r.research_policy_hash,
        "selection_policy_hash": cs.selection_policy_hash,
        "candidate_set_ref": cs.content_hash,
        "universe_ref": r.universe.content_hash,
        "limits": asdict(r.limits),
        "authority": "ZERO",
        "shadow_only": True,
    }
    reasons = []
    if r.built_at_ns >= r.deadline_ns:
        return DecisionBriefV1("EXPIRED", ("DEADLINE_EXPIRED",), FrozenMap(binding))
    if r.slot_id != cs.decision_event_id:
        reasons.append("SLOT_IDENTITY")
    if cs.universe_ref != r.universe.content_hash:
        reasons.append("UNIVERSE_IDENTITY")
    if (
        cs.envelope.available_at_ns > r.built_at_ns
        or r.universe.envelope.available_at_ns > r.cutoff_ns
        or r.cutoff_ns > r.universe.decision_slot_ns
    ):
        reasons.append("COHORT_UNAVAILABLE")
    if any(f.available_at_ns > r.cutoff_ns or f.observed_at_ns > r.cutoff_ns for f in r.facts):
        reasons.append("FUTURE_FACT")
    if any(f.valid_until_ns < r.cutoff_ns for f in r.facts):
        reasons.append("STALE_CONTEXT")
    entries = {entry.key: entry for entry in r.universe.entries}
    if any(f.key is not None and f.key not in entries for f in r.facts):
        reasons.append("PRODUCT_REVISION")
    if any(not _indexed(repository, f.source_ref, r.cutoff_ns) for f in r.facts):
        reasons.append("FACT_PROVENANCE_MISSING")
    if any(not _indexed(repository, ref, r.cutoff_ns) for ref in r.image_refs):
        reasons.append("IMAGE_PROVENANCE_MISSING")
    peer = None
    if r.peer is not None:
        p = r.peer
        if p.cutoff_ns != r.cutoff_ns or p.available_at_ns > r.built_at_ns or p.valid_until_ns <= r.built_at_ns:
            reasons.append("STALE_PEER_CONTEXT")
        elif p.universe_ref != r.universe.content_hash or p.anchor_key not in entries or p.peer_key not in entries:
            reasons.append("PEER_PRODUCT_IDENTITY")
        elif not _indexed(repository, p.s6_state_ref, p.computed_at_ns):
            reasons.append("PEER_PROVENANCE_MISSING")
        else:
            peer = p.to_dict()
    ids = {c.candidate_id for c in cs.candidates}
    econ = {}
    for e in r.economics:
        if r.deadline_ns > e.candidate.deadline_ns:
            reasons.append("ORIGINAL_DEADLINE_BINDING")
        if e.candidate.candidate_id not in ids or e.candidate.candidate_id in econ:
            reasons.append("ECONOMIC_CANDIDATE_SCOPE")
            continue
        econ[e.candidate.candidate_id] = _economic_row(e, r, repository)
    calendar = []
    for c in cs.candidates:
        row = c.to_dict()
        row["screening_status"] = c.eligibility_status.value
        row["economic_status"] = econ.get(c.candidate_id, {}).get("status", "NOT_ESTIMABLE")
        row["missingness"] = econ.get(c.candidate_id, {}).get("missingness", ["EXACT_ACTION_NOT_ASSESSED"])
        row["supported_choice"] = c.eligibility_status == EligibilityStatusV2.ELIGIBLE and (
            econ.get(c.candidate_id, {}).get("supported_choice", False)
        )
        calendar.append(row)
    forecasts = []
    for f in sorted(r.forecasts, key=lambda f: (f.model_ref, f.source_ref, f.target, f.horizon_ns)):
        if f.input_cutoff_ns > r.cutoff_ns or f.available_at_ns > r.built_at_ns:
            reasons.append("FUTURE_ML_INPUT")
        manifest = repository.get_model_manifest(f.model_ref) if repository else None
        checkpoint = repository.get_artifact(f.checkpoint_ref) if repository else None
        calibration = repository.get_artifact(f.calibration_ref) if repository else None
        proof = (
            manifest is not None
            and f.checkpoint_ref in manifest.weight_sha256
            and checkpoint is not None
            and checkpoint.metadata.get("installed_weight_sha256") == f.checkpoint_ref
            and checkpoint.metadata.get("model_manifest_ref") == f.model_ref
            and calibration is not None
            and calibration.metadata.get("model_manifest_ref") == f.model_ref
            and calibration.metadata.get("target") == f.target
            and calibration.metadata.get("horizon_ns") == f.horizon_ns
            and calibration.metadata.get("status") == "SUPPORTED"
        )
        eligible = (
            proof
            and f.key in entries
            and f.input_cutoff_ns <= r.cutoff_ns
            and f.available_at_ns <= r.built_at_ns
            and f.target == r.forecast_target
            and f.horizon_ns == r.forecast_horizon_ns
            and f.validated_support
            and f.installed_checkpoint_verified
            and _indexed(repository, f.source_ref, r.built_at_ns)
            and all(
                _indexed(repository, ref, r.cutoff_ns) for ref in (f.model_ref, f.checkpoint_ref, f.calibration_ref)
            )
        )
        forecasts.append(
            {
                "source_ref": f.source_ref,
                "model_ref": f.model_ref,
                "checkpoint_ref": f.checkpoint_ref,
                "target": f.target,
                "horizon_ns": f.horizon_ns,
                "input_cutoff_ns": f.input_cutoff_ns,
                "status": "QUALIFIED_FORECAST_ONLY" if eligible else "NOT_ESTIMABLE",
                "values": f.values.to_dict() if eligible else None,
                "limitations": list(f.limitations),
                "missingness": [] if eligible else ["ML_IDENTITY_HORIZON_SUPPORT_UNQUALIFIED"],
            }
        )
    facts = [f.to_dict() for f in sorted(r.facts, key=lambda f: (f.section, f.source_ref, canonical_json(f.to_dict())))]
    present = {f.section for f in r.facts}
    body = {
        **binding,
        "calendar_denominator": 1,
        "calendar_event_id": cs.decision_event_id,
        "slot_status": cs.selection_status.value,
        "a0_selected_candidate_id": cs.selected_candidate_id,
        "calendar": calendar,
        "shortlist": [c["candidate_id"] for c in calendar if c["supported_choice"]],
        "economics": [econ[k] for k in sorted(econ)],
        "facts": facts,
        "peer_context": peer,
        "missing_sections": sorted(SECTIONS - present),
        "ml": forecasts,
        "image_refs": list(r.image_refs),
        "tool_allowlist": list(TOOLS),
        "budget_status": r.budget_status,
        "permissible_responses": ["SELECT_ONE", "REJECT_ALL", "ABSTAIN"],
        "economic_value": "NOT_ESTIMABLE",
        "source_text_trust": "UNTRUSTED_DATA",
    }
    instruments = {c.key.to_canonical_json() for c in cs.candidates}
    instruments.update(f.key.to_canonical_json() for f in r.facts if f.key)
    instruments.update(f.key.to_canonical_json() for f in r.forecasts)
    if r.peer:
        instruments.update(k.to_canonical_json() for k in (r.peer.anchor_key, r.peer.peer_key, r.peer.btc_proxy_key))
    size = len(
        canonical_json({"version": BRIEF_VERSION, "status": "COMPLETE", "reasons": [], "body": body}).encode("utf-8")
    )
    # Conservative UTF-8-byte projection, not measured provider tokens or a tokenizer claim.
    if len(calendar) + len(facts) + len(forecasts) + len(econ) > r.limits.max_rows:
        reasons.append("ROW_LIMIT")
    if len(instruments) > r.limits.max_instruments:
        reasons.append("INSTRUMENT_LIMIT")
    if len(r.image_refs) > r.limits.max_image_refs:
        reasons.append("IMAGE_LIMIT")
    if size > r.limits.max_bytes:
        reasons.append("BYTE_LIMIT")
    if size > r.limits.max_projected_tokens:
        reasons.append("TOKEN_PROJECTION_LIMIT")
    if reasons:
        # Original candidate/calendar refs stay resolvable. No partial actionable packet.
        return DecisionBriefV1(
            "BLOCKED",
            tuple(sorted(set(reasons))),
            FrozenMap(
                {
                    **binding,
                    "calendar_denominator": 1,
                    "original_candidate_count": len(cs.candidates),
                    "required_bytes": size,
                    "incomplete": True,
                }
            ),
        )
    return DecisionBriefV1("COMPLETE", (), FrozenMap(body))
