"""Fixed, read-only and job-authorized research evidence surface."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from atlas.v2._serialization import canonical_json
from atlas.v2.agent_intelligence.contracts import (
    AgentEvidenceRefV1,
    ReadStatusV1,
    ResearchProposalRequestV1,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.science.discovery import DISCOVERY_LAB_HASH, DiscoveryExperimentV2, holdout_spent_at

MAX_TOOL_RESPONSE_BYTES = 24_000
MAX_TOOL_ROWS = 50
_FORBIDDEN_ARTIFACT_TYPES = frozenset({
    "DiscoveryHoldoutStateV2", "DiscoveryOutcomeViewV2", "MaturedOutcomeV2", "OutcomeArtifactV2",
    "FinalHoldoutEvidenceV1", "TradePlanEnvelopeV2", "ApprovalV2", "ReservationV2", "OrderArtifactV2",
})
_READABLE_ARTIFACT_TYPES = frozenset({
    "DiscoveryExperimentV2", "DiscoveryAttemptV2", "DiscoveryRejectedAttemptV2",
    "FeatureFamilyAblationAuditV2", "WholePolicyAblationInvariantV2", "ResearchPolicyManifestV1", "PolicyV2",
    "RiskPolicyV1", "RiskPolicyV2", "ModelManifestV2", "FeatureSchemaV1",
})


def _contains_prohibited_evidence(value: Any) -> bool:
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = "".join(char for char in str(key).lower() if char.isalnum())
            if any(token in normalized for token in ("holdoutlabel", "holdoutscore", "holdoutreturn", "futurelabel")):
                return True
            if _contains_prohibited_evidence(child):
                return True
    elif isinstance(value, (list, tuple)):
        return any(_contains_prohibited_evidence(child) for child in value)
    return False


def _attempt_exposes_final_holdout(metadata: Mapping[str, Any], holdout_ref: str) -> bool:
    attempt = metadata.get("attempt")
    if not isinstance(attempt, Mapping):
        return False
    if attempt.get("holdout_viewed") is True:
        return True
    for name in ("training_refs", "validation_refs", "outer_refs"):
        refs = attempt.get(name, ())
        if isinstance(refs, (list, tuple)) and holdout_ref in refs:
            return True
    return False


def _entry_wire(entry: ArtifactIndexEntryV2) -> dict[str, Any]:
    return {"artifact_ref": entry.artifact_ref, "artifact_type": entry.artifact_type,
            "content_hash": entry.content_hash, "created_at_ns": entry.created_at_ns,
            "available_at_ns": entry.available_at_ns, "metadata": entry.metadata}


class BoundedResearchReadService:
    """Control-plane reader. The model worker receives only these bounded values."""

    def __init__(self, repository: OpsRepository) -> None:
        if repository.read_only is False:
            # OpsRepository still exposes only typed artifact reads here; no write method is retained.
            self._repository = repository
        else:
            self._repository = repository

    def authorize_request_manifest(self, request: ResearchProposalRequestV1) -> None:
        """Prove every request ref is in one registered, untouched Discovery family."""
        experiment_entry = self._repository.get_artifact(request.experiment_ref)
        if (experiment_entry is None or experiment_entry.artifact_type != "DiscoveryExperimentV2"
                or experiment_entry.available_at_ns > request.development_cutoff_ns):
            raise ValueError("request experiment is missing, unregistered or available after its cutoff")
        raw = experiment_entry.metadata.get("experiment")
        if not isinstance(raw, Mapping):
            raise ValueError("request experiment artifact has no typed Discovery record")
        body = dict(raw)
        try:
            if body.pop("lab_contract_hash") != DISCOVERY_LAB_HASH:
                raise ValueError("request Discovery identity hash changed")
            body.pop("version")
            experiment = DiscoveryExperimentV2(**{
                **body,
                "allowed_feature_families": tuple(body["allowed_feature_families"]),
                "availability_assumptions": tuple(body["availability_assumptions"]),
                "primary_metrics": tuple(body["primary_metrics"]),
            })
        except (KeyError, TypeError) as exc:
            raise ValueError("request Discovery artifact is malformed") from exc
        if experiment.content_hash != request.experiment_ref:
            raise ValueError("request Discovery artifact content hash mismatch")
        if (request.preregistration_ref != request.experiment_ref
                or request.research_family_id != experiment.family_id
                or request.baseline_policy_ref != experiment.baseline_policy_ref
                or request.multiplicity_family != experiment.multiplicity_family_id
                or not set(request.allowed_feature_families).issubset(experiment.allowed_feature_families)
                or experiment.holdout_state != "UNTOUCHED"
                or experiment.final_holdout_ref not in request.inaccessible_holdout_identities
                or request.remaining_attempt_budget > experiment.maximum_attempts
                or request.remaining_parameter_search_budget > experiment.parameter_search_budget):
            raise ValueError("request exceeds its preregistered Discovery family or holdout permissions")
        if holdout_spent_at(self._repository, request.experiment_ref) is not None:
            raise ValueError("offline research proposer cannot access a Discovery family after final holdout spend")
        if request.development_cutoff_ns < experiment.preregistered_at_ns:
            raise ValueError("development cutoff precedes Discovery preregistration")
        if request.outcome_maturity_cutoff_ns < experiment.preregistered_at_ns:
            raise ValueError("outcome-maturity cutoff precedes Discovery preregistration")
        holdout_entry = self._repository.get_artifact(experiment.final_holdout_ref)
        population = holdout_entry.metadata.get("population") if holdout_entry is not None else None
        if (holdout_entry is None or holdout_entry.artifact_type != "DiscoveryHoldoutPopulationV2"
                or holdout_entry.content_hash != experiment.final_holdout_ref
                or not isinstance(population, Mapping) or population.get("population_id") is None):
            raise ValueError("request holdout identity is not a registered immutable population contract")
        if holdout_entry.available_at_ns > request.development_cutoff_ns:
            raise ValueError("holdout identity contract is future evidence")
        attempts = self._repository.artifact_entries_by_types(
            ("DiscoveryAttemptV2", "DiscoveryRejectedAttemptV2"), limit=10_000,
            available_before_ns=min(request.development_cutoff_ns, request.outcome_maturity_cutoff_ns))
        family_attempts = [entry for entry in attempts if isinstance(entry.metadata.get("attempt"), Mapping)
            and entry.metadata["attempt"].get("experiment_ref") == request.experiment_ref]
        known_attempt_refs = {entry.artifact_ref for entry in family_attempts}
        if set(request.attempt_history_refs) != known_attempt_refs:
            raise ValueError("request attempt history is incomplete or includes an unauthorized predecessor")
        latest_attempts: dict[str, Mapping[str, Any]] = {}
        for entry in family_attempts:
            attempt_body = entry.metadata["attempt"]
            attempt_id = str(attempt_body.get("attempt_id", ""))
            if attempt_id and (attempt_id not in latest_attempts or int(attempt_body.get("attempt_version", 0)) >
                               int(latest_attempts[attempt_id].get("attempt_version", 0))):
                latest_attempts[attempt_id] = attempt_body
        parameter_units_used = 0
        for attempt_body in latest_attempts.values():
            parameters = attempt_body.get("parameters", {})
            units = parameters.get("search_units", 1) if isinstance(parameters, Mapping) else None
            if type(units) is not int or units < 1:
                raise ValueError("Discovery attempt history has invalid parameter-search accounting")
            parameter_units_used += units
        available_discovery_attempts = experiment.maximum_attempts - len(latest_attempts)
        available_discovery_parameters = experiment.parameter_search_budget - parameter_units_used
        if (available_discovery_attempts <= 0 or available_discovery_parameters <= 0
                or request.remaining_attempt_budget > available_discovery_attempts
                or request.remaining_parameter_search_budget > available_discovery_parameters):
            raise ValueError("request exceeds remaining Discovery attempt or parameter budgets")
        authorized_refs = {request.experiment_ref, request.preregistration_ref, request.baseline_policy_ref,
                           *known_attempt_refs}
        for authorization in request.evidence_manifest:
            if authorization.artifact_ref not in authorized_refs:
                raise ValueError("request evidence ref is outside its server-authorized Discovery manifest")
            if authorization.available_through_ns > request.development_cutoff_ns:
                raise ValueError("request evidence manifest exceeds the development cutoff")
            if authorization.artifact_ref == experiment.final_holdout_ref:
                raise ValueError("final holdout identity is inaccessible to agent read tools")
            if authorization.tool_name in {"inspect_failed_discovery_variants", "inspect_authorized_ablation_results"}:
                if authorization.artifact_ref != request.experiment_ref:
                    raise ValueError("family-specific read tool must use the exact registered experiment ref")
            if authorization.cursor is not None and authorization.cursor not in known_attempt_refs:
                raise ValueError("read-tool cursor is outside the registered attempt history")
            indexed = self._repository.get_artifact(authorization.artifact_ref)
            if indexed is not None and indexed.available_at_ns > request.development_cutoff_ns:
                raise ValueError("request artifact is future evidence")
            if authorization.artifact_ref == request.baseline_policy_ref and indexed is None:
                raise ValueError("request baseline manifest is unavailable")

    def read(self, request: ResearchProposalRequestV1, authorization: AgentEvidenceRefV1,
             *, now_ns: int) -> dict[str, Any]:
        if authorization not in request.evidence_manifest:
            return self._status(ReadStatusV1.FORBIDDEN, "authorization_not_in_job_manifest")
        if now_ns >= request.absolute_deadline_ns:
            return self._status(ReadStatusV1.EXPIRED, "job_deadline_elapsed")
        if authorization.available_through_ns > request.development_cutoff_ns:
            return self._status(ReadStatusV1.FORBIDDEN, "authorization_exceeds_development_cutoff")
        try:
            result = self._execute(request, authorization)
            encoded = canonical_json(result).encode("utf-8")
            if len(encoded) > MAX_TOOL_RESPONSE_BYTES:
                return self._status(ReadStatusV1.UNAVAILABLE, "bounded_response_byte_limit")
            return result
        except Exception:
            return self._status(ReadStatusV1.UNAVAILABLE, "evidence_read_failed")

    @staticmethod
    def _status(status: ReadStatusV1, reason: str) -> dict[str, Any]:
        return {"status": status.value, "reason": reason, "rows": []}

    def _exact_entry(self, ref: str, cutoff_ns: int,
                     outcome_maturity_cutoff_ns: int | None = None) -> ArtifactIndexEntryV2 | None:
        entry = self._repository.get_artifact(ref)
        if entry is None:
            return None
        if entry.available_at_ns > cutoff_ns:
            return None
        if (outcome_maturity_cutoff_ns is not None
                and entry.artifact_type in {"DiscoveryAttemptV2", "DiscoveryRejectedAttemptV2",
                                            "FeatureFamilyAblationAuditV2", "WholePolicyAblationInvariantV2"}
                and entry.available_at_ns > outcome_maturity_cutoff_ns):
            return None
        if entry.artifact_type in _FORBIDDEN_ARTIFACT_TYPES or entry.artifact_type not in _READABLE_ARTIFACT_TYPES:
            return None
        if _contains_prohibited_evidence(entry.metadata):
            return None
        return entry

    def _final_holdout_ref(self, request: ResearchProposalRequestV1) -> str | None:
        entry = self._repository.get_artifact(request.experiment_ref)
        experiment = entry.metadata.get("experiment") if entry is not None else None
        if isinstance(experiment, Mapping) and isinstance(experiment.get("final_holdout_ref"), str):
            return str(experiment["final_holdout_ref"])
        return None

    def _execute(self, request: ResearchProposalRequestV1, auth: AgentEvidenceRefV1) -> dict[str, Any]:
        ref = auth.artifact_ref
        cutoff = request.development_cutoff_ns
        if auth.tool_name == "get_registered_artifact":
            entry = self._exact_entry(ref, cutoff, request.outcome_maturity_cutoff_ns)
            holdout_ref = self._final_holdout_ref(request)
            if entry is not None and holdout_ref is not None and _attempt_exposes_final_holdout(
                    entry.metadata, holdout_ref):
                return self._status(ReadStatusV1.FORBIDDEN, "artifact_is_linked_to_final_holdout")
            if entry is None:
                raw = self._repository.get_artifact(ref)
                if raw is None:
                    return self._status(ReadStatusV1.MISSING, "registered_artifact_missing")
                return self._status(ReadStatusV1.FORBIDDEN, "artifact_type_or_cutoff_not_authorized")
            rows = [_entry_wire(entry)]
        elif auth.tool_name == "get_registered_policy_or_model_manifest":
            entry = self._exact_entry(ref, cutoff, request.outcome_maturity_cutoff_ns)
            if entry is None:
                raw = self._repository.get_artifact(ref)
                status = ReadStatusV1.MISSING if raw is None else ReadStatusV1.FORBIDDEN
                return self._status(status, "policy_or_model_manifest_unavailable")
            if entry.artifact_type not in {"ResearchPolicyManifestV1", "RiskPolicyV1", "RiskPolicyV2", "PolicyV2",
                                           "ModelManifestV2", "FeatureSchemaV1"}:
                return self._status(ReadStatusV1.FORBIDDEN, "artifact_is_not_a_policy_or_model_manifest")
            rows = [_entry_wire(entry)]
        elif auth.tool_name == "inspect_failed_discovery_variants":
            experiment = self._exact_entry(ref, cutoff)
            if experiment is None or experiment.artifact_type != "DiscoveryExperimentV2":
                return self._status(ReadStatusV1.FORBIDDEN, "family_ref_is_not_a_registered_discovery_experiment")
            if auth.cursor is not None and self._repository.get_artifact(auth.cursor) is None:
                return self._status(ReadStatusV1.FORBIDDEN, "cursor_is_not_a_registered_artifact_ref")
            variants = self._repository.artifact_entries_by_types(
                ("DiscoveryAttemptV2", "DiscoveryRejectedAttemptV2"), limit=10_000,
                available_before_ns=min(cutoff, request.outcome_maturity_cutoff_ns))
            scoped = []
            for entry in variants:
                attempt = entry.metadata.get("attempt")
                if (isinstance(attempt, Mapping) and attempt.get("experiment_ref") == ref
                        and (auth.cursor is None or entry.artifact_ref > auth.cursor)
                        and entry.artifact_type not in _FORBIDDEN_ARTIFACT_TYPES
                        and not _contains_prohibited_evidence(entry.metadata)
                        and not _attempt_exposes_final_holdout(entry.metadata,
                            self._final_holdout_ref(request) or "")
                        and (entry.artifact_type == "DiscoveryRejectedAttemptV2"
                             or attempt.get("failure_reason") is not None
                             or attempt.get("state") in {"FAILED", "ABANDONED"})):
                    scoped.append(entry)
            scoped.sort(key=lambda item: (item.created_at_ns, item.artifact_ref))
            rows = [_entry_wire(entry) for entry in scoped[:MAX_TOOL_ROWS]]
            if not rows:
                return {"status": ReadStatusV1.PRESENT.value, "reason": "no_failed_variants_at_cutoff", "rows": []}
        elif auth.tool_name == "inspect_authorized_ablation_results":
            experiment = self._exact_entry(ref, cutoff, request.outcome_maturity_cutoff_ns)
            if experiment is None or experiment.artifact_type != "DiscoveryExperimentV2":
                return self._status(ReadStatusV1.FORBIDDEN, "experiment_ref_is_not_authorized")
            # The artifact-index contract binds ablation results directly by exact ref in the job manifest.
            ablations = self._repository.artifact_entries_by_types(
                ("FeatureFamilyAblationAuditV2", "WholePolicyAblationInvariantV2"),
                limit=MAX_TOOL_ROWS, available_before_ns=min(cutoff, request.outcome_maturity_cutoff_ns))
            rows = [_entry_wire(entry) for entry in ablations
                    if not _contains_prohibited_evidence(entry.metadata)
                    and entry.metadata.get("experiment_ref") == ref]
        else:
            return self._status(ReadStatusV1.FORBIDDEN, "unknown_tool")
        response = {"status": ReadStatusV1.PRESENT.value, "reason": "authorized_bounded_read", "rows": rows}
        if len(rows) > MAX_TOOL_ROWS or len(canonical_json(response).encode("utf-8")) > MAX_TOOL_RESPONSE_BYTES:
            return self._status(ReadStatusV1.UNAVAILABLE, "bounded_response_limit")
        return response


def collect_job_evidence(request: ResearchProposalRequestV1, service: BoundedResearchReadService,
                         *, now_ns: int) -> tuple[dict[str, Any], ...]:
    """Prefetch only manifest-authorized refs, counting every call against the fixed budget."""
    if len(request.evidence_manifest) > request.max_read_tool_calls:
        raise ValueError("request evidence manifest exceeds its read-tool budget")
    outputs = tuple({"tool_name": item.tool_name, "artifact_ref": item.artifact_ref, "cursor": item.cursor,
                     **service.read(request, item, now_ns=now_ns)} for item in request.evidence_manifest)
    return outputs
