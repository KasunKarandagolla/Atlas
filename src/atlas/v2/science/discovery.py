"""Preregistered zero-authority offline proposal, attempt, and holdout ledger."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from atlas.v2._serialization import FrozenMap, canonical_json, sha256_json, sha256_ref
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository

DISCOVERY_LAB_VERSION = "BOUNDED_DISCOVERY_LAB_V2_V2"
DISCOVERY_EXPERIMENT_VERSION = "DISCOVERY_EXPERIMENT_V2_V1"
DISCOVERY_ATTEMPT_VERSION = "DISCOVERY_ATTEMPT_V2_V1"
HOLDOUT_STATE_VERSION = "DISCOVERY_HOLDOUT_STATE_V2_V2"
PROPOSER_TYPES = frozenset({"HUMAN", "OFFLINE_LLM_ADAPTER", "MECHANICAL_ABLATION", "RESEARCH_SCRIPT"})
ATTEMPT_STATES = frozenset({"PROPOSED", "STARTED", "COMPLETED", "FAILED", "ABANDONED"})
PROPOSAL_OPERATIONS = frozenset({"M1_LIGHTGBM_FIXED_GRID", "CAUSAL_ANALOGUE_FIXED_RETRIEVAL",
    "MULTI_SLEEVE_RESEARCH_SELECTION", "S8_HOURLY_PAIRS_RESEARCH", "WHOLE_POLICY_FEATURE_ABLATION"})

DISCOVERY_LAB_BODY = {
    "version": DISCOVERY_LAB_VERSION,
    "authority": "ZERO_CAPITAL_ZERO_ORDER_ZERO_RISK_MUTATION",
    "ai_proposal_boundary": {"schema_constrained_hypotheses_only": True, "credentials": False,
        "exchange_or_order_tools": False, "live_risk_mutation": False,
        "capital_authority": False, "self_promotion": False},
    "holdout": "LINEAGE_INSPECTED_EXPLICIT_VIEW_SPENDS_HOLDOUT_AND_REQUIRES_FRESH_FUTURE_EVIDENCE",
    "attempt_ledger": "APPEND_ONLY_INCLUDING_FAILURES_AND_REJECTED_VALIDATION_ATTEMPTS",
    "proposal_operations": sorted(PROPOSAL_OPERATIONS),
    "executable_spec_version": "EXACT_EXECUTABLE_RESEARCH_SPEC_V1",
    "evaluation": "OPERATION_TYPED_MATURED_EVIDENCE_DETERMINISTIC_CHRONOLOGY_PURGE_EMBARGO_AND_CUTOFF",
}
DISCOVERY_LAB_HASH = sha256_json(DISCOVERY_LAB_BODY)


@dataclass(frozen=True)
class DiscoveryExperimentV2:
    experiment_id: str
    family_id: str
    hypothesis_family: str
    allowed_feature_families: tuple[str, ...]
    availability_assumptions: tuple[str, ...]
    maximum_attempts: int
    parameter_search_budget: int
    baseline_policy_ref: str
    primary_metrics: tuple[str, ...]
    chronology: str
    purge_embargo: str
    multiplicity_family_id: str
    stop_rule: str
    final_holdout_ref: str
    holdout_state: str
    prospective_shadow_required: bool
    preregistered_at_ns: int

    def __post_init__(self) -> None:
        for name in ("experiment_id", "family_id", "hypothesis_family", "multiplicity_family_id", "stop_rule"):
            if not getattr(self, name).strip():
                raise ValueError(f"{name} is required")
        sha256_ref(self.baseline_policy_ref, field="baseline_policy_ref")
        sha256_ref(self.final_holdout_ref, field="final_holdout_ref")
        if self.maximum_attempts < 1 or self.parameter_search_budget < 1:
            raise ValueError("discovery attempt and parameter budgets must be positive")
        for name in ("allowed_feature_families", "availability_assumptions", "primary_metrics"):
            value = tuple(getattr(self, name))
            if not value or tuple(sorted(set(value))) != value:
                raise ValueError(f"{name} must be non-empty, sorted and unique")
            object.__setattr__(self, name, value)
        if self.holdout_state != "UNTOUCHED" or self.prospective_shadow_required is not True:
            raise ValueError("new discovery families require an untouched holdout and prospective shadow")
        if not self.chronology or not self.purge_embargo:
            raise ValueError("discovery chronology and purge/embargo must be preregistered")

    def to_dict(self) -> dict[str, Any]:
        return {"version": DISCOVERY_EXPERIMENT_VERSION, **{
            name: list(value) if isinstance(value, tuple) else value for name, value in self.__dict__.items()},
            "lab_contract_hash": DISCOVERY_LAB_HASH}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class DiscoveryAttemptV2:
    experiment_ref: str
    experiment_id: str
    attempt_id: str
    attempt_version: int
    previous_attempt_ref: str | None
    proposal_version: str
    proposal_spec: Mapping[str, Any]
    proposal_hash: str
    proposer_type: str
    proposer_id: str
    parameters: Mapping[str, Any]
    started_at_ns: int
    completed_at_ns: int | None
    training_refs: tuple[str, ...]
    validation_refs: tuple[str, ...]
    outer_refs: tuple[str, ...]
    result: Mapping[str, Any] | None
    failure_reason: str | None
    manual_intervention: tuple[str, ...]
    holdout_viewed: bool
    holdout_ref: str
    credentials_available: bool = False
    order_tools_available: bool = False
    risk_mutation_available: bool = False
    capital_authority: bool = False
    self_promotion: bool = False

    def __post_init__(self) -> None:
        for name in ("experiment_ref", "proposal_hash", "holdout_ref"):
            sha256_ref(getattr(self, name), field=name)
        if self.previous_attempt_ref is not None:
            sha256_ref(self.previous_attempt_ref, field="previous_attempt_ref")
        if not self.attempt_id or not self.experiment_id or not self.proposal_version or not self.proposer_id:
            raise ValueError("discovery attempt identity is incomplete")
        if self.proposer_type not in PROPOSER_TYPES or self.attempt_version < 1:
            raise ValueError("discovery proposer/version is invalid")
        if sha256_json(self.proposal_spec) != self.proposal_hash:
            raise ValueError("discovery executable proposal hash mismatch")
        if (self.proposal_spec.get("version") != "EXACT_EXECUTABLE_RESEARCH_SPEC_V1"
            or self.proposal_spec.get("operation") not in PROPOSAL_OPERATIONS
            or type(self.proposal_spec.get("evaluation_cutoff_ns")) is not int):
            raise ValueError("discovery proposals must use the bounded offline executable specification schema")
        if any((self.credentials_available, self.order_tools_available, self.risk_mutation_available,
                self.capital_authority, self.self_promotion)):
            raise ValueError("discovery lab authority boundary cannot be widened")
        for name in ("training_refs", "validation_refs", "outer_refs"):
            refs = tuple(getattr(self, name))
            if tuple(sorted(set(refs))) != refs:
                raise ValueError(f"{name} must be sorted and unique")
            for ref in refs:
                sha256_ref(ref, field=name)
        if tuple(sorted(set(self.manual_intervention))) != self.manual_intervention:
            raise ValueError("manual intervention records must be sorted and unique")
        if self.completed_at_ns is not None and self.completed_at_ns < self.started_at_ns:
            raise ValueError("discovery completion precedes start")
        if self.completed_at_ns is None and (self.result is not None or self.failure_reason is not None):
            raise ValueError("unfinished attempt cannot contain a completed result/failure")
        if self.completed_at_ns is not None and self.result is None and self.failure_reason is None:
            raise ValueError("completed attempt must retain result or failure")
        object.__setattr__(self, "proposal_spec", FrozenMap(self.proposal_spec))
        object.__setattr__(self, "parameters", FrozenMap(self.parameters))
        if self.result is not None:
            object.__setattr__(self, "result", FrozenMap(self.result))
        if self.holdout_viewed and self.completed_at_ns is None:
            raise ValueError("holdout may be marked viewed only on a completed attempt")

    @property
    def state(self) -> str:
        if self.completed_at_ns is None:
            return "STARTED"
        if self.failure_reason:
            return "FAILED"
        if self.result and self.result.get("abandoned") is True:
            return "ABANDONED"
        return "COMPLETED"

    def to_dict(self) -> dict[str, Any]:
        return {"version": DISCOVERY_ATTEMPT_VERSION, **{
            name: dict(value) if isinstance(value, Mapping) else list(value) if isinstance(value, tuple) else value
            for name, value in self.__dict__.items()}, "state": self.state}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class DiscoveryHoldoutStateV2:
    experiment_ref: str
    holdout_ref: str
    state: str
    viewed_at_ns: int
    attempt_id: str
    attempt_ref: str
    evidence_refs: tuple[str, ...]

    def __post_init__(self) -> None:
        sha256_ref(self.experiment_ref, field="experiment_ref")
        sha256_ref(self.holdout_ref, field="holdout_ref")
        sha256_ref(self.attempt_ref, field="attempt_ref")
        if self.state != "SPENT" or not self.attempt_id or not self.evidence_refs:
            raise ValueError("holdout state is append-only and may transition only to SPENT")
        if tuple(sorted(set(self.evidence_refs))) != self.evidence_refs:
            raise ValueError("holdout view evidence refs must be sorted and unique")
        for ref in self.evidence_refs:
            sha256_ref(ref, field="holdout_evidence_ref")

    def to_dict(self) -> dict[str, Any]:
        return {"version": HOLDOUT_STATE_VERSION, **self.__dict__}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class DiscoveryOutcomeViewV2:
    """Typed split assignment for an already validated immutable matured outcome."""

    experiment_ref: str
    outcome_ref: str
    split_role: str
    available_at_ns: int
    final_holdout_ref: str | None = None

    def __post_init__(self) -> None:
        for name in ("experiment_ref", "outcome_ref"):
            sha256_ref(getattr(self, name), field=name)
        if self.final_holdout_ref is not None:
            sha256_ref(self.final_holdout_ref, field="final_holdout_ref")
        if self.split_role not in {"TRAINING", "VALIDATION", "OUTER"}:
            raise ValueError("discovery outcome view split role is invalid")
        if type(self.available_at_ns) is not int or self.available_at_ns < 0:
            raise ValueError("discovery outcome view availability is invalid")
        if self.final_holdout_ref is not None and self.split_role != "OUTER":
            raise ValueError("final holdout views are permitted only as explicitly assigned outer evidence")

    def to_dict(self) -> dict[str, Any]:
        return {"version": "DISCOVERY_OUTCOME_VIEW_V1", **self.__dict__}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def index_discovery_outcome_view(repo: OpsRepository, item: DiscoveryOutcomeViewV2) -> str:
    experiment = repo.get_artifact(item.experiment_ref)
    body = experiment.metadata.get("experiment") if experiment else None
    if (experiment is None or experiment.artifact_type != "DiscoveryExperimentV2"
            or not isinstance(body, Mapping)
            or (item.final_holdout_ref is not None and item.final_holdout_ref != body.get("final_holdout_ref"))):
        raise ValueError("discovery outcome view must bind its preregistered experiment/holdout identity")
    label = _resolve_matured_label(repo, item.outcome_ref, item.available_at_ns)
    if label["available_at_ns"] > item.available_at_ns:
        raise ValueError("discovery outcome view cannot precede its matured label")
    repo.register_artifact(ArtifactIndexEntryV2(item.content_hash, "DiscoveryOutcomeViewV2", item.content_hash,
        item.available_at_ns, item.available_at_ns, {"view": item.to_dict()}))
    return item.content_hash


def register_discovery_experiment(repo: OpsRepository, experiment: DiscoveryExperimentV2,
        *, available_at_ns: int) -> str:
    if available_at_ns < experiment.preregistered_at_ns:
        raise ValueError("experiment availability precedes preregistration")
    if repo.get_artifact(experiment.baseline_policy_ref) is None or repo.get_artifact(experiment.final_holdout_ref) is None:
        raise ValueError("discovery baseline and holdout must already be indexed")
    if any(isinstance((body := entry.metadata.get("holdout_state")), Mapping)
        and body.get("holdout_ref") == experiment.final_holdout_ref for entry in repo.artifact_entries("DiscoveryHoldoutStateV2")):
        raise ValueError("a SPENT holdout cannot be reset through a new experiment identity")
    body = experiment.to_dict()
    repo.register_artifact(ArtifactIndexEntryV2(experiment.content_hash, "DiscoveryExperimentV2",
        experiment.content_hash, experiment.preregistered_at_ns, available_at_ns, {"experiment": body}))
    return experiment.content_hash


def _attempts(repo: OpsRepository, experiment_ref: str) -> tuple[Mapping[str, Any], ...]:
    found = []
    for entry in repo.artifact_entries("DiscoveryAttemptV2"):
        body = entry.metadata.get("attempt")
        if isinstance(body, Mapping) and body.get("experiment_ref") == experiment_ref:
            found.append(body)
    return tuple(found)


def holdout_spent_at(repo: OpsRepository, experiment_ref: str) -> int | None:
    experiment = repo.get_artifact(experiment_ref)
    experiment_body = experiment.metadata.get("experiment") if experiment else None
    holdout_ref = experiment_body.get("final_holdout_ref") if isinstance(experiment_body, Mapping) else None
    spent = [int(body["viewed_at_ns"]) for entry in repo.artifact_entries("DiscoveryHoldoutStateV2")
        if isinstance((body := entry.metadata.get("holdout_state")), Mapping)
        and (body.get("experiment_ref") == experiment_ref or holdout_ref is not None and body.get("holdout_ref") == holdout_ref)
        and body.get("state") == "SPENT"]
    return min(spent) if spent else None


def mark_holdout_spent(repo: OpsRepository, *, experiment_ref: str, holdout_ref: str,
        attempt_id: str, attempt_ref: str, evidence_refs: tuple[str, ...], viewed_at_ns: int) -> str:
    experiment = repo.get_artifact(experiment_ref)
    body = experiment.metadata.get("experiment") if experiment else None
    if not isinstance(body, Mapping) or body.get("final_holdout_ref") != holdout_ref or viewed_at_ns < int(body["preregistered_at_ns"]):
        raise ValueError("holdout spending must bind its preregistered immutable holdout")
    if holdout_spent_at(repo, experiment_ref) is not None:
        raise ValueError("final holdout is already SPENT and cannot be reset or viewed again")
    attempt_entry = repo.get_artifact(attempt_ref)
    attempt_body = attempt_entry.metadata.get("attempt") if attempt_entry is not None else None
    if (attempt_entry is None or attempt_entry.artifact_type != "DiscoveryAttemptV2"
            or attempt_entry.content_hash != attempt_ref or not isinstance(attempt_body, Mapping)
            or attempt_body.get("experiment_ref") != experiment_ref or attempt_body.get("attempt_id") != attempt_id
            or attempt_body.get("holdout_ref") != holdout_ref or attempt_body.get("holdout_viewed") is not True
            or attempt_body.get("completed_at_ns") != viewed_at_ns
            or not set(evidence_refs).issubset(set(attempt_body.get("outer_refs", ())))):
        raise ValueError("holdout spending requires the exact indexed completed attempt and outer evidence")
    if set(evidence_refs).difference(_holdout_linked_refs(repo, tuple(evidence_refs), holdout_ref)):
        raise ValueError("holdout spending evidence is not linked to the final holdout")
    artifact = DiscoveryHoldoutStateV2(experiment_ref, holdout_ref, "SPENT", viewed_at_ns, attempt_id,
        attempt_ref, tuple(sorted(set(evidence_refs))))
    repo.register_artifact(ArtifactIndexEntryV2(artifact.content_hash, "DiscoveryHoldoutStateV2",
        artifact.content_hash, viewed_at_ns, viewed_at_ns, {"holdout_state": artifact.to_dict()}))
    return artifact.content_hash


def _retain_rejection(repo: OpsRepository, attempt: DiscoveryAttemptV2, reason: str, at_ns: int) -> None:
    body = {**attempt.to_dict(), "rejection_reason": reason, "state": "FAILED_VALIDATION_NOT_EVALUATED"}
    ref = sha256_json(body)
    repo.register_artifact(ArtifactIndexEntryV2(ref, "DiscoveryRejectedAttemptV2", ref,
        attempt.started_at_ns, max(at_ns, attempt.started_at_ns), {"attempt": body}))


_REF_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_REFERENCE_FIELDS = frozenset({"input_ref", "input_refs", "source_ref", "source_refs", "dependency_refs",
    "evidence_ref", "evidence_refs", "outcome_ref", "outcome_refs", "decision_ref", "decision_refs",
    "experiment_ref",
    "candidate_ref", "candidate_set_ref", "action_artifact_ref", "feature_ref", "source_calendar_refs",
    "training_refs", "validation_refs", "outer_refs", "holdout_ref", "final_holdout_ref",
    "holdout_identity_ref", "attempt_ref"})


def _nested_refs(value: Any, field: str = "") -> set[str]:
    refs: set[str] = set()
    if isinstance(value, Mapping):
        for name, nested in value.items():
            refs.update(_nested_refs(nested, str(name)))
    elif isinstance(value, (list, tuple)):
        for nested in value:
            if isinstance(nested, str) and field in _REFERENCE_FIELDS and _REF_PATTERN.fullmatch(nested):
                refs.add(nested)
            else:
                refs.update(_nested_refs(nested, field))
    elif isinstance(value, str) and field in _REFERENCE_FIELDS and _REF_PATTERN.fullmatch(value):
        refs.add(value)
    return refs


def _declares_holdout(entry: Any, holdout_ref: str) -> bool:
    if entry.artifact_ref == holdout_ref:
        return True
    metadata = entry.metadata
    refs = _nested_refs(metadata)
    if holdout_ref in refs:
        return True
    def labels(value: Any, name: str = "") -> bool:
        if isinstance(value, Mapping):
            return any(labels(item, str(key).lower()) for key, item in value.items())
        if isinstance(value, (list, tuple)):
            return any(labels(item, name) for item in value)
        if isinstance(value, str) and name in {"split", "split_role", "evaluation_split", "data_split", "fold_role"}:
            return value.upper() in {"FINAL_HOLDOUT", "HOLDOUT", "FINAL_TEST_HOLDOUT"}
        return False
    return labels(metadata)


def _holdout_linked_refs(repo: OpsRepository, evaluation_refs: tuple[str, ...], holdout_ref: str) -> set[str]:
    held_out = {holdout_ref}
    linked: set[str] = set()
    pending = list(evaluation_refs)
    visited: set[str] = set()
    while pending:
        ref = pending.pop()
        if ref in visited:
            continue
        visited.add(ref)
        entry = repo.get_artifact(ref)
        if entry is None:
            continue
        if ref in held_out or _declares_holdout(entry, holdout_ref):
            linked.add(ref)
        pending.extend(_nested_refs(entry.metadata) - visited)
    return linked


def _split_specification(experiment_body: Mapping[str, Any], attempt: DiscoveryAttemptV2) -> Mapping[str, Any]:
    raw = attempt.proposal_spec.get("split_spec")
    expected = {"version", "chronology_contract", "purge_embargo_contract", "training_start_ns", "training_end_ns",
        "validation_start_ns", "validation_end_ns", "outer_start_ns", "outer_end_ns", "evaluation_cutoff_ns",
        "embargo_ns", "maximum_policy_horizon_ns", "randomized"}
    if not isinstance(raw, Mapping) or set(raw) != expected or raw.get("version") != "DISCOVERY_CHRONOLOGICAL_SPLIT_V1":
        raise ValueError("evaluation refs require a complete typed chronological split_spec")
    times = ("training_start_ns", "training_end_ns", "validation_start_ns", "validation_end_ns",
        "outer_start_ns", "outer_end_ns", "evaluation_cutoff_ns", "embargo_ns", "maximum_policy_horizon_ns")
    if any(type(raw.get(name)) is not int or raw[name] < 0 for name in times):
        raise ValueError("typed split timestamps/horizons must be nonnegative integers")
    if (raw["training_start_ns"] >= raw["training_end_ns"]
            or raw["validation_start_ns"] >= raw["validation_end_ns"]
            or raw["outer_start_ns"] >= raw["outer_end_ns"]
            or raw["training_end_ns"] > raw["validation_start_ns"]
            or raw["validation_end_ns"] > raw["outer_start_ns"]
            or raw["evaluation_cutoff_ns"] > attempt.started_at_ns
            or raw["outer_end_ns"] > raw["evaluation_cutoff_ns"]
            or raw["randomized"] is not False):
        raise ValueError("discovery split chronology is reversed, random or unavailable at attempt start")
    if (raw["chronology_contract"] != experiment_body.get("chronology")
            or raw["purge_embargo_contract"] != experiment_body.get("purge_embargo")
            or raw["evaluation_cutoff_ns"] != attempt.proposal_spec.get("evaluation_cutoff_ns")):
        raise ValueError("attempt split chronology/purge contract differs from preregistration")
    if raw["maximum_policy_horizon_ns"] <= 0 or raw["embargo_ns"] < raw["maximum_policy_horizon_ns"]:
        raise ValueError("discovery embargo is below the maximum policy holding horizon")
    return raw


def _resolve_matured_label(repo: OpsRepository, ref: str, cutoff_ns: int) -> Mapping[str, int]:
    from atlas.v2._serialization import json_value
    from atlas.v2.science.outcomes import (
        MaturedOutcomeV2,
        executable_action_value_training_eligible,
        index_matured_outcome,
    )

    entry = repo.get_artifact(ref)
    raw = entry.metadata.get("outcome") if entry is not None else None
    if entry is None or entry.artifact_type != "MaturedOutcomeV2" or not isinstance(raw, Mapping):
        raise ValueError("action-value evaluation refs must resolve to typed MaturedOutcomeV2 labels")
    outcome = MaturedOutcomeV2.from_dict(json_value(raw))
    if (entry.content_hash != ref or outcome.content_hash != ref or entry.available_at_ns != outcome.available_at_ns
            or not executable_action_value_training_eligible(outcome, cutoff_ns)):
        raise ValueError("unmatured, unavailable or invalid MaturedOutcomeV2 cannot enter discovery evaluation")
    index_matured_outcome(repo, outcome)
    if outcome.evidence_quality.upper().startswith(("SYNTHETIC", "FIXTURE")):
        raise ValueError("synthetic fixture labels cannot masquerade as historical discovery evidence")
    return {"decision_at_ns": outcome.decision_at_ns, "horizon_end_ns": outcome.horizon_end_ns,
        "available_at_ns": outcome.available_at_ns}


def _resolve_operation_evidence(repo: OpsRepository, operation: str, ref: str,
        cutoff_ns: int, *, experiment_ref: str | None = None, split_role: str | None = None) -> tuple[Mapping[str, Any], ...]:
    if operation == "S8_HOURLY_PAIRS_RESEARCH":
        raise ValueError("NOT_ESTIMABLE_S8_HONEST_TWO_LEG_MATURED_OUTCOME_CONTRACT_UNAVAILABLE")
    if operation in {"M1_LIGHTGBM_FIXED_GRID", "CAUSAL_ANALOGUE_FIXED_RETRIEVAL"}:
        entry = repo.get_artifact(ref)
        if entry is not None and entry.artifact_type == "DiscoveryOutcomeViewV2":
            view = entry.metadata.get("view")
            required = {"version", "experiment_ref", "outcome_ref", "split_role", "available_at_ns", "final_holdout_ref"}
            if (not isinstance(view, Mapping) or set(view) != required or view.get("version") != "DISCOVERY_OUTCOME_VIEW_V1"
                    or sha256_json(view) != ref or entry.content_hash != ref
                    or view.get("experiment_ref") != experiment_ref or view.get("split_role") != split_role
                    or view.get("available_at_ns") != entry.available_at_ns or entry.available_at_ns > cutoff_ns):
                raise ValueError("discovery outcome view does not bind the current experiment/split/cutoff")
            if view.get("final_holdout_ref") is not None:
                experiment = repo.get_artifact(str(view.get("experiment_ref")))
                body = experiment.metadata.get("experiment") if experiment is not None else None
                if (split_role != "OUTER" or not isinstance(body, Mapping)
                        or view.get("final_holdout_ref") != body.get("final_holdout_ref")):
                    raise ValueError("discovery outcome view final holdout identity mismatch")
            label = _resolve_matured_label(repo, str(view["outcome_ref"]), cutoff_ns)
            if label["available_at_ns"] > entry.available_at_ns:
                raise ValueError("discovery outcome view precedes its matured label")
            return ({**label, "view_split_role": view["split_role"],
                "final_holdout_ref": view.get("final_holdout_ref")},)
        return (_resolve_matured_label(repo, ref, cutoff_ns),)
    if operation == "MULTI_SLEEVE_RESEARCH_SELECTION":
        from atlas.v2.science.audits import SELECTION_AUDIT_VERSION

        entry = repo.get_artifact(ref)
        audit = entry.metadata.get("research_artifact") if entry is not None else None
        if (entry is None or entry.artifact_type != "SelectionPolicyAuditV2" or not isinstance(audit, Mapping)
                or entry.content_hash != ref or sha256_json(audit) != ref or audit.get("version") != SELECTION_AUDIT_VERSION
                or audit.get("status") != "ESTIMABLE"):
            raise ValueError("selector evaluation requires an estimable typed whole-calendar SelectionPolicyAuditV2")
        if entry.available_at_ns > cutoff_ns:
            raise ValueError("whole-calendar selector evidence is unavailable at the evaluation cutoff")
        output = []
        for row in audit.get("rows", ()):
            if not isinstance(row, Mapping) or type(row.get("decision_at_ns")) is not int:
                raise ValueError("whole-calendar selector rows are malformed")
            for outcome_ref in row.get("outcome_refs", ()):
                label = _resolve_matured_label(repo, str(outcome_ref), cutoff_ns)
                if label["decision_at_ns"] != row["decision_at_ns"]:
                    raise ValueError("whole-calendar selector outcome does not bind its decision row")
                output.append(label)
        if not output:
            raise ValueError("selector audit lacks genuine matured whole-calendar outcome evidence")
        return tuple(output)
    if operation == "WHOLE_POLICY_FEATURE_ABLATION":
        raise ValueError("NOT_ESTIMABLE_NO_TYPED_PAIRED_WHOLE_POLICY_ABLATION_EVIDENCE")
    raise ValueError("discovery operation has no registered evidence contract")


def _validate_discovery_evidence(repo: OpsRepository, experiment_body: Mapping[str, Any],
        attempt: DiscoveryAttemptV2, refs: tuple[str, ...]) -> Mapping[str, Any]:
    split = _split_specification(experiment_body, attempt)
    operation = str(attempt.proposal_spec["operation"])
    groups = (("training", attempt.training_refs, "training_start_ns", "training_end_ns", "validation_start_ns"),
        ("validation", attempt.validation_refs, "validation_start_ns", "validation_end_ns", "outer_start_ns"),
        ("outer", attempt.outer_refs, "outer_start_ns", "outer_end_ns", "evaluation_cutoff_ns"))
    observed: dict[str, list[Mapping[str, int]]] = {name: [] for name, *_ in groups}
    for name, group_refs, start_field, end_field, next_field in groups:
        for ref in group_refs:
            labels = _resolve_operation_evidence(repo, operation, ref, int(split["evaluation_cutoff_ns"]),
                experiment_ref=attempt.experiment_ref, split_role=name.upper())
            for label in labels:
                if label.get("view_split_role") is not None and label.get("view_split_role") != name.upper():
                    raise ValueError("typed discovery outcome view is assigned to the wrong split")
                start, end = int(split[start_field]), int(split[end_field])
                decision, horizon, available = label["decision_at_ns"], label["horizon_end_ns"], label["available_at_ns"]
                if not start <= decision < end:
                    raise ValueError(f"{name} label decision time belongs to the wrong chronological split")
                if horizon > end or available > int(split["evaluation_cutoff_ns"]):
                    raise ValueError(f"{name} label crosses its fold boundary or is unavailable at the evaluation cutoff")
                if next_field != "evaluation_cutoff_ns" and horizon + int(split["embargo_ns"]) > int(split[next_field]):
                    raise ValueError(f"{name} label crosses a protected split boundary after purge/embargo")
                if horizon - decision > int(split["maximum_policy_horizon_ns"]):
                    raise ValueError("label holding horizon exceeds the declared maximum policy horizon")
                observed[name].append(label)
    # Purge overlapping/correlated labels within each fold; gaps must satisfy the declared embargo.
    for name, rows in observed.items():
        ordered = sorted(rows, key=lambda item: (item["decision_at_ns"], item["horizon_end_ns"], item["available_at_ns"]))
        for left, right in zip(ordered, ordered[1:], strict=False):
            if left["horizon_end_ns"] + int(split["embargo_ns"]) > right["decision_at_ns"]:
                raise ValueError(f"{name} labels overlap or violate the declared purge/embargo")
    return split


def _rejection_code(message: str) -> str:
    if message.startswith("NOT_ESTIMABLE_"):
        return message.split(":", 1)[0]
    normalized = re.sub(r"[^A-Z0-9]+", "_", message.upper()).strip("_")
    return ("REJECTED_" + normalized)[:120]


def _register_discovery_attempt(repo: OpsRepository, experiment_ref: str, attempt: DiscoveryAttemptV2,
        *, available_at_ns: int) -> str:
    experiment_entry = repo.get_artifact(experiment_ref)
    body = experiment_entry.metadata.get("experiment") if experiment_entry else None
    if (experiment_entry is None or experiment_entry.artifact_type != "DiscoveryExperimentV2"
        or not isinstance(body, Mapping) or attempt.experiment_ref != experiment_ref
        or attempt.experiment_id != body.get("experiment_id") or attempt.holdout_ref != body.get("final_holdout_ref")):
        raise ValueError("discovery attempt does not bind its preregistered experiment")
    if attempt.proposer_type not in PROPOSER_TYPES or available_at_ns < attempt.started_at_ns:
        raise ValueError("discovery attempt proposer or chronology invalid")
    if attempt.started_at_ns < experiment_entry.available_at_ns:
        _retain_rejection(repo, attempt, "ATTEMPT_PRECEDES_PREREGISTRATION", available_at_ns)
        raise ValueError("discovery attempt cannot precede preregistration")
    declared_features = tuple(attempt.proposal_spec.get("feature_families", ()))
    if not set(declared_features).issubset(set(body["allowed_feature_families"])):
        _retain_rejection(repo, attempt, "UNDECLARED_FEATURE_FAMILY", available_at_ns)
        raise ValueError("discovery proposal features exceed the preregistered allowed families")
    prior = _attempts(repo, experiment_ref)
    attempt_versions = [row for row in prior if row.get("attempt_id") == attempt.attempt_id]
    distinct_ids = {str(row.get("attempt_id")) for row in prior}
    if not attempt_versions and len(distinct_ids) >= int(body["maximum_attempts"]):
        _retain_rejection(repo, attempt, "ATTEMPT_BUDGET_EXCEEDED", available_at_ns)
        raise ValueError("preregistered discovery search budget exceeded")
    search_units = attempt.parameters.get("search_units", 1)
    if type(search_units) is not int or search_units < 1:
        raise ValueError("discovery attempt must declare a positive integer search_units value")
    if attempt_versions:
        latest = max(attempt_versions, key=lambda row: int(row.get("attempt_version", 0)))
        if attempt.attempt_version != int(latest["attempt_version"]) + 1 or attempt.previous_attempt_ref != sha256_json(latest):
            raise ValueError("discovery attempt revisions must append to the exact prior ledger ref")
        if latest.get("state") in {"COMPLETED", "FAILED", "ABANDONED"}:
            raise ValueError("terminal discovery attempts cannot be rewritten")
        if latest.get("proposal_hash") != attempt.proposal_hash or canonical_json(latest.get("parameters")) != canonical_json(attempt.parameters):
            raise ValueError("a revised attempt cannot change its proposal or parameter budget")
    elif attempt.attempt_version != 1 or attempt.previous_attempt_ref is not None:
        raise ValueError("new discovery proposal must start at version 1")
    latest_by_id: dict[str, Mapping[str, Any]] = {}
    for row in prior:
        attempt_id = str(row.get("attempt_id", ""))
        if attempt_id and (attempt_id not in latest_by_id or
            int(row.get("attempt_version", 0)) > int(latest_by_id[attempt_id].get("attempt_version", 0))):
            latest_by_id[attempt_id] = row
    used_units = sum(int(row.get("parameters", {}).get("search_units", 1)) for row in latest_by_id.values())
    if not attempt_versions:
        used_units += search_units
    if used_units > int(body["parameter_search_budget"]):
        _retain_rejection(repo, attempt, "PARAMETER_SEARCH_BUDGET_EXCEEDED", available_at_ns)
        raise ValueError("preregistered discovery parameter search budget exceeded")
    evaluation_refs = attempt.training_refs + attempt.validation_refs + attempt.outer_refs
    split: Mapping[str, Any] | None = None
    if evaluation_refs:
        split = _validate_discovery_evidence(repo, body, attempt, evaluation_refs)
    spent_at = holdout_spent_at(repo, experiment_ref)
    if spent_at is not None and attempt.attempt_version == 1:
        if not evaluation_refs or split is None or int(split["evaluation_cutoff_ns"]) <= spent_at:
            _retain_rejection(repo, attempt, "SPENT_HOLDOUT_REDESIGN_REQUIRES_FRESH_FUTURE_EVIDENCE", available_at_ns)
            raise ValueError("a redesign after viewing the holdout requires genuinely later future evidence")
        for role, refs in (("TRAINING", attempt.training_refs), ("VALIDATION", attempt.validation_refs), ("OUTER", attempt.outer_refs)):
            for ref in refs:
                rows = _resolve_operation_evidence(repo, str(attempt.proposal_spec["operation"]), ref,
                    int(split["evaluation_cutoff_ns"]), experiment_ref=attempt.experiment_ref, split_role=role)
                if any(row["decision_at_ns"] <= spent_at or row["available_at_ns"] <= spent_at for row in rows):
                    _retain_rejection(repo, attempt, "SPENT_HOLDOUT_REDESIGN_REQUIRES_FRESH_FUTURE_EVIDENCE", available_at_ns)
                    raise ValueError("a redesign after viewing the holdout requires genuinely later future evidence")
    holdout_links = _holdout_linked_refs(repo, evaluation_refs, str(body["final_holdout_ref"]))
    ordinary_refs = attempt.training_refs + attempt.validation_refs
    if any(ref in holdout_links for ref in ordinary_refs):
        _retain_rejection(repo, attempt, "FINAL_HOLDOUT_IN_TRAINING_OR_VALIDATION", available_at_ns)
        raise ValueError("final holdout evidence cannot appear in training or validation refs")
    outer_holdout = tuple(ref for ref in attempt.outer_refs if ref in holdout_links)
    if outer_holdout and not attempt.holdout_viewed:
        _retain_rejection(repo, attempt, "HOLDOUT_VIEW_NOT_DECLARED", available_at_ns)
        raise ValueError("holdout-linked outer evidence requires an explicit holdout view")
    if attempt.holdout_viewed and (not outer_holdout or not attempt.completed_at_ns):
        _retain_rejection(repo, attempt, "HOLDOUT_VIEW_WITHOUT_HELD_OUT_EVIDENCE", available_at_ns)
        raise ValueError("holdout_viewed must bind actual final holdout evidence in outer_refs")
    if attempt.holdout_viewed and spent_at is not None:
        _retain_rejection(repo, attempt, "HOLDOUT_ALREADY_SPENT", available_at_ns)
        raise ValueError("final holdout is already SPENT and cannot be viewed again")
    if available_at_ns < experiment_entry.available_at_ns or (
        attempt.completed_at_ns is not None and available_at_ns < attempt.completed_at_ns):
        raise ValueError("discovery attempt cannot precede its preregistration")
    repo.register_artifact(ArtifactIndexEntryV2(attempt.content_hash, "DiscoveryAttemptV2",
        attempt.content_hash, attempt.started_at_ns, available_at_ns, {"attempt": attempt.to_dict()}))
    if attempt.holdout_viewed:
        if attempt.completed_at_ns is None:
            raise ValueError("holdout may be marked viewed only on a completed attempt")
        mark_holdout_spent(repo, experiment_ref=experiment_ref, holdout_ref=attempt.holdout_ref,
            attempt_id=attempt.attempt_id, attempt_ref=attempt.content_hash,
            evidence_refs=outer_holdout, viewed_at_ns=attempt.completed_at_ns)
    return attempt.content_hash


def register_discovery_attempt(repo: OpsRepository, experiment_ref: str, attempt: DiscoveryAttemptV2,
        *, available_at_ns: int) -> str:
    """Register or durably retain every accepted/rejected attempt."""
    try:
        return _register_discovery_attempt(repo, experiment_ref, attempt, available_at_ns=available_at_ns)
    except ValueError as exc:
        retained = any(isinstance((body := entry.metadata.get("attempt")), Mapping)
            and body.get("experiment_ref") == experiment_ref and body.get("attempt_id") == attempt.attempt_id
            and body.get("attempt_version") == attempt.attempt_version
            for entry in repo.artifact_entries("DiscoveryRejectedAttemptV2"))
        if not retained:
            reason = _rejection_code(str(exc))
            _retain_rejection(repo, attempt, reason, available_at_ns)
        raise


def discovery_attempt_ledger(repo: OpsRepository, experiment_ref: str) -> tuple[Mapping[str, Any], ...]:
    rejected = tuple(body for entry in repo.artifact_entries("DiscoveryRejectedAttemptV2")
        if isinstance((body := entry.metadata.get("attempt")), Mapping) and body.get("experiment_ref") == experiment_ref)
    return tuple(sorted((*_attempts(repo, experiment_ref), *rejected), key=lambda row: (
        int(row.get("started_at_ns", 0)), str(row.get("attempt_id", "")), int(row.get("attempt_version", 0)))))


def audit_discovery_multiplicity(repo: OpsRepository, *, experiment_ref: str,
        baseline: Any, variants: Any, **configuration: Any) -> Any:
    """Derive the corrected family from the retained ledger, including failed variants."""
    from atlas.v2.science.audits import build_multiplicity_audit

    experiment = repo.get_artifact(experiment_ref)
    body = experiment.metadata.get("experiment") if experiment else None
    if not isinstance(body, Mapping) or baseline.variant_hash != body.get("baseline_policy_ref"):
        raise ValueError("multiplicity must bind the preregistered discovery baseline")
    latest: dict[str, Mapping[str, Any]] = {}
    for attempt in discovery_attempt_ledger(repo, experiment_ref):
        identity = str(attempt["attempt_id"])
        if identity not in latest or int(attempt["attempt_version"]) >= int(latest[identity]["attempt_version"]):
            latest[identity] = attempt
    if {variant.variant_id for variant in variants} != set(latest):
        raise ValueError("multiplicity family must include every attempted proposal in the retained ledger")
    for variant in variants:
        attempt = latest[variant.variant_id]
        if variant.variant_hash != attempt["proposal_hash"] or (
            attempt.get("state") != "COMPLETED" and not variant.failure_reason):
            raise ValueError("multiplicity must preserve exact proposal hashes and failed/abandoned attempts")
    return build_multiplicity_audit(family_id=str(body["multiplicity_family_id"]),
        preregistration_ref=experiment_ref, baseline_variant_id=baseline.variant_id,
        variants=(baseline, *variants), expected_family_member_ids=(baseline.variant_id, *sorted(latest)),
        **configuration)
