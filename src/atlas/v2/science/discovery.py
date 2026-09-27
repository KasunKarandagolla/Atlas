"""Preregistered zero-authority offline proposal, attempt, and holdout ledger."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from atlas.v2._serialization import FrozenMap, canonical_json, sha256_json, sha256_ref
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository

DISCOVERY_LAB_VERSION = "BOUNDED_DISCOVERY_LAB_V2_V1"
DISCOVERY_EXPERIMENT_VERSION = "DISCOVERY_EXPERIMENT_V2_V1"
DISCOVERY_ATTEMPT_VERSION = "DISCOVERY_ATTEMPT_V2_V1"
HOLDOUT_STATE_VERSION = "DISCOVERY_HOLDOUT_STATE_V2_V1"
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
    "holdout": "A_VIEWED_HOLDOUT_IS_IMMUTABLY_SPENT",
    "attempt_ledger": "APPEND_ONLY_INCLUDING_FAILURES_AND_ABANDONED_VARIANTS",
    "proposal_operations": sorted(PROPOSAL_OPERATIONS),
    "executable_spec_version": "EXACT_EXECUTABLE_RESEARCH_SPEC_V1",
    "evaluation": "PREREGISTERED_CAUSAL_CUTOFF_AND_HASH_BOUND_EVIDENCE_ONLY",
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

    def __post_init__(self) -> None:
        sha256_ref(self.experiment_ref, field="experiment_ref")
        sha256_ref(self.holdout_ref, field="holdout_ref")
        if self.state != "SPENT" or not self.attempt_id:
            raise ValueError("holdout state is append-only and may transition only to SPENT")

    def to_dict(self) -> dict[str, Any]:
        return {"version": HOLDOUT_STATE_VERSION, **self.__dict__}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


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
        attempt_id: str, viewed_at_ns: int) -> str:
    experiment = repo.get_artifact(experiment_ref)
    body = experiment.metadata.get("experiment") if experiment else None
    if not isinstance(body, Mapping) or body.get("final_holdout_ref") != holdout_ref or viewed_at_ns < int(body["preregistered_at_ns"]):
        raise ValueError("holdout spending must bind its preregistered immutable holdout")
    if holdout_spent_at(repo, experiment_ref) is not None:
        raise ValueError("final holdout is already SPENT and cannot be reset or viewed again")
    artifact = DiscoveryHoldoutStateV2(experiment_ref, holdout_ref, "SPENT", viewed_at_ns, attempt_id)
    repo.register_artifact(ArtifactIndexEntryV2(artifact.content_hash, "DiscoveryHoldoutStateV2",
        artifact.content_hash, viewed_at_ns, viewed_at_ns, {"holdout_state": artifact.to_dict()}))
    return artifact.content_hash


def _retain_rejection(repo: OpsRepository, attempt: DiscoveryAttemptV2, reason: str, at_ns: int) -> None:
    body = {**attempt.to_dict(), "rejection_reason": reason, "state": "FAILED_VALIDATION_NOT_EVALUATED"}
    ref = sha256_json(body)
    repo.register_artifact(ArtifactIndexEntryV2(ref, "DiscoveryRejectedAttemptV2", ref,
        attempt.started_at_ns, max(at_ns, attempt.started_at_ns), {"attempt": body}))


def register_discovery_attempt(repo: OpsRepository, experiment_ref: str, attempt: DiscoveryAttemptV2,
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
    spent_at = holdout_spent_at(repo, experiment_ref)
    if spent_at is not None and attempt.attempt_version == 1:
        evaluation_refs = attempt.training_refs + attempt.validation_refs + attempt.outer_refs
        evidence = tuple(repo.get_artifact(ref) for ref in evaluation_refs)
        if not evidence or any(entry is None or entry.available_at_ns <= spent_at for entry in evidence):
            _retain_rejection(repo, attempt, "SPENT_HOLDOUT_REDESIGN_REQUIRES_FRESH_FUTURE_EVIDENCE", available_at_ns)
            raise ValueError("a redesign after viewing the holdout requires fresh future evidence")
    evaluation_refs = attempt.training_refs + attempt.validation_refs + attempt.outer_refs
    if evaluation_refs:
        evaluation_cutoff = attempt.proposal_spec.get("evaluation_cutoff_ns")
        if type(evaluation_cutoff) is not int or evaluation_cutoff > attempt.started_at_ns:
            raise ValueError("proposal evaluation requires a preregistered causal evidence cutoff")
        for ref in evaluation_refs:
            entry = repo.get_artifact(ref)
            if entry is None or entry.available_at_ns > evaluation_cutoff:
                _retain_rejection(repo, attempt, "FUTURE_OR_UNAVAILABLE_EVALUATION_LABEL", available_at_ns)
                raise ValueError("future or unavailable labels cannot enter discovery evaluation")
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
            attempt_id=attempt.attempt_id, viewed_at_ns=attempt.completed_at_ns)
    return attempt.content_hash


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
