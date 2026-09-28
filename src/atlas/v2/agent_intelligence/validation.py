"""Deterministic, all-or-nothing validation of untrusted proposal output."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from typing import Any

from atlas.v2._serialization import sha256_json, sha256_ref
from atlas.v2.agent_intelligence.contracts import (
    MAX_PROPOSAL_BYTES,
    AgentValidationReceiptV1,
    ResearchProposalRequestV1,
    ResearchProposalV1,
)

_CODE_SIGNS = re.compile(r"```|\b(?:def|class|import|exec|eval|subprocess|__import__)\s*|(?:^|\n)\s*[$>]\s")
_RESULT_CLAIMS = re.compile(
    r"\b(?:backtests?|back-testing|backtested|expected\s+(?:profit|returns?|performance)|"
    r"profitable|profitability|win[- ]rate|sharpe(?:\s+ratio)?|"
    r"(?:returns?|performance)\s+(?:improved|increased|outperformed|exceeded))\b", re.I
)
_FORBIDDEN_TEXT = re.compile(
    r"\b(?:final\s+holdout\s+(?:score|label|return|result)|holdout\s+(?:score|label|return)|"
    r"future\s+outcome\s+(?:was|is)\s+\$?[-+]?\d)\b", re.I
)
_UNAUTHORIZED_REQUEST = re.compile(
    r"\b(?:run|execute|invoke|open|browse|search|access|inspect|read|fetch|change|alter|modify|override|set|request|"
    r"provide|reveal|install|promote)\s+(?:the\s+)?(?:final\s+holdout|holdout\s+(?:labels?|scores?|returns?)|"
    r"web(?:\s+search)?|browser|shell|python|mcp|exchange\s+credentials?|api\s+keys?|risk\s*policy|capital|"
    r"orders?|reservations?|strategy|proposal|yourself|this\s+proposal|the\s+proposal)\b", re.I
)
_SECRET_OR_PATH = re.compile(r"(?:sk-[A-Za-z0-9_-]{20,}|\bBearer\s+[A-Za-z0-9._~-]{16,}|"
                             r"https?://|file://|(?:^|\s)/(?:home|root|tmp|etc)/|"
                             r"\b(?:select\s+.+\s+from|drop\s+table|insert\s+into)\b)", re.I)
_ALLOWED_FOLLOWUPS = frozenset({
    "DEVELOPMENT_WALK_FORWARD_REPLAY",
    "DEVELOPMENT_ABLATION_COMPARISON",
    "MATURITY_CUTOFF_AUDIT",
    "POINT_IN_TIME_AVAILABILITY_AUDIT",
})


def _parameter_units(rule: Any) -> int:
    if not isinstance(rule, Mapping):
        return 0
    count = int(rule.get("threshold") is not None)
    children = rule.get("children", [])
    if isinstance(children, (list, tuple)):
        count += sum(_parameter_units(child) for child in children)
    return count


def proposal_payload_schema() -> dict[str, Any]:
    """Finite public JSON schema mirrored by the optional structured-output adapter."""
    text = {"type": "string", "minLength": 1, "maxLength": 4000}
    bounded_strings = {"type": "array", "maxItems": 64, "items": {"type": "string", "minLength": 1, "maxLength": 240}}
    rule = {"type": "object", "additionalProperties": False,
            "properties": {"operator": {"type": "string"}, "feature_family": {"type": ["string", "null"]},
                           "feature_name": {"type": ["string", "null"]}, "threshold": {"type": ["string", "null"]},
                           "children": {"type": "array", "maxItems": 8, "items": {"$ref": "#/$defs/rule"}}},
            "required": ["operator", "feature_family", "feature_name", "threshold", "children"]}
    return {"type": "object", "additionalProperties": False,
            "properties": {"version": {"const": "RESEARCH_PROPOSAL_V1"},
                "research_family_id": text, "proposal_id": {"type": "string", "minLength": 1, "maxLength": 160},
                "proposal_version": {"type": "integer", "minimum": 1}, "causal_hypothesis": text,
                "proposed_rule": {"$ref": "#/$defs/rule"}, "feature_dependencies": bounded_strings,
                "evidence_refs": {"type": "array", "maxItems": 16, "items": {"type": "string", "pattern": "^[0-9a-f]{64}$"}},
                "availability_requirements": bounded_strings, "falsifier": text, "target_population": text,
                "horizon": text, "cost_semantics": text, "intended_ablation": text,
                "development_slices": bounded_strings, "known_failed_predecessors": bounded_strings,
                "proposal_lineage": bounded_strings,
                "requested_deterministic_followup_evaluation_type": {"type": "string"}},
            "required": ["version", "research_family_id", "proposal_id", "proposal_version", "causal_hypothesis",
                         "proposed_rule", "feature_dependencies", "evidence_refs", "availability_requirements",
                         "falsifier", "target_population", "horizon", "cost_semantics", "intended_ablation",
                         "development_slices", "known_failed_predecessors", "proposal_lineage",
                         "requested_deterministic_followup_evaluation_type"],
            "$defs": {"rule": rule}}


def proposal_schema_hash() -> str:
    return sha256_json({"version": "RESEARCH_PROPOSAL_SCHEMA_V1", "schema": proposal_payload_schema()})


def validate_proposal_output(
    request: ResearchProposalRequestV1,
    raw_output: str,
    *,
    now_ns: int,
    available_evidence_refs: Iterable[str],
    provider_result_hash: str | None = None,
    known_proposal_hashes: Iterable[str] = (),
) -> tuple[ResearchProposalV1 | None, AgentValidationReceiptV1]:
    """Parse and validate one complete result. No prose repair or partial acceptance."""
    reasons: list[str] = []
    proposal: ResearchProposalV1 | None = None
    proposal_hash = sha256_json({"raw_untrusted_output": raw_output})
    try:
        if now_ns >= request.absolute_deadline_ns:
            raise ValueError("DEADLINE_EXPIRED")
        if not isinstance(raw_output, str) or len(raw_output.encode("utf-8")) > MAX_PROPOSAL_BYTES:
            raise ValueError("OUTPUT_SIZE_LIMIT")
        decoded = json.loads(raw_output)
        if not isinstance(decoded, Mapping):
            raise ValueError("SCHEMA_ROOT_NOT_OBJECT")
        proposal = ResearchProposalV1.from_dict(decoded)
        proposal_hash = proposal.content_hash
        if proposal.research_family_id != request.research_family_id:
            raise ValueError("RESEARCH_FAMILY_MISMATCH")
        permitted = frozenset(request.allowed_feature_families)
        grammar = frozenset(request.allowed_operation_grammar)
        proposal.proposed_rule.validate(permitted_families=permitted, grammar=grammar)
        for dependency in proposal.feature_dependencies:
            family, separator, feature = dependency.partition("/")
            if not separator or family not in permitted or not feature or len(feature) > 96:
                raise ValueError("FEATURE_DEPENDENCY_NOT_PERMITTED")
            valid_availability = {"PREDECISION_REQUIRED", "NULL_ALLOWED_WITH_EXPLICIT_FLAG", "UNAVAILABLE_INVALIDATES"}
            if not any(item.startswith(dependency + ":") and item.split(":", 1)[1] in valid_availability
                       for item in proposal.availability_requirements):
                raise ValueError("FEATURE_AVAILABILITY_REQUIREMENT_MISSING")
        if "MISSINGNESS=EXPLICIT" not in proposal.availability_requirements:
            raise ValueError("MISSINGNESS_POLICY_NOT_EXPLICIT")
        authorized_refs = {item.artifact_ref for item in request.evidence_manifest}
        authorized_refs.update(request.attempt_history_refs)
        authorized_refs.update((request.experiment_ref, request.preregistration_ref, request.baseline_policy_ref))
        available_refs = set(available_evidence_refs)
        if not proposal.evidence_refs or not set(proposal.evidence_refs).issubset(authorized_refs):
            raise ValueError("EVIDENCE_REFERENCE_NOT_AUTHORIZED")
        if not set(proposal.evidence_refs).issubset(available_refs):
            raise ValueError("EVIDENCE_REFERENCE_UNAVAILABLE")
        if not proposal.development_slices:
            raise ValueError("DEVELOPMENT_SLICES_REQUIRED")
        for ref in proposal.development_slices:
            sha256_ref(ref, field="development_slice_ref")
        if not set(proposal.development_slices).issubset(available_refs):
            raise ValueError("DEVELOPMENT_SLICE_NOT_CUTOFF_AUTHORIZED")
        if not set(proposal.known_failed_predecessors).issubset(set(request.attempt_history_refs)):
            raise ValueError("FAILED_PREDECESSOR_NOT_AUTHORIZED")
        if not set(proposal.proposal_lineage).issubset(authorized_refs):
            raise ValueError("PROPOSAL_LINEAGE_NOT_AUTHORIZED")
        combined = "\n".join((proposal.causal_hypothesis, proposal.falsifier, proposal.target_population,
                              proposal.horizon, proposal.cost_semantics, proposal.intended_ablation,
                              proposal.requested_followup_evaluation_type,
                              *proposal.feature_dependencies, *proposal.availability_requirements,
                              *proposal.development_slices, *proposal.known_failed_predecessors,
                              *proposal.proposal_lineage))
        if _CODE_SIGNS.search(combined):
            raise ValueError("EXECUTABLE_SOURCE_NOT_ALLOWED")
        if _UNAUTHORIZED_REQUEST.search(combined):
            raise ValueError("UNAUTHORIZED_TOOL_OR_AUTHORITY_REQUEST")
        if _SECRET_OR_PATH.search(combined):
            raise ValueError("SECRET_PATH_URL_OR_QUERY_TEXT_NOT_ALLOWED")
        if _RESULT_CLAIMS.search(combined):
            raise ValueError("UNSUPPORTED_RESULT_CLAIM")
        if _FORBIDDEN_TEXT.search(combined) or any(identity in combined for identity in request.inaccessible_holdout_identities):
            raise ValueError("HOLDOUT_OR_FUTURE_OUTCOME_LEAKAGE")
        if proposal.requested_followup_evaluation_type not in _ALLOWED_FOLLOWUPS:
            raise ValueError("FOLLOWUP_EVALUATION_TYPE_NOT_AUTHORIZED")
        duplicate_hashes = set(known_proposal_hashes)
        duplicate_identity = sha256_json({"family": proposal.research_family_id, "rule": proposal.proposed_rule.to_dict(),
            "features": sorted(proposal.feature_dependencies), "population": proposal.target_population,
            "horizon": proposal.horizon, "cost": proposal.cost_semantics, "ablation": proposal.intended_ablation})
        if proposal.content_hash in duplicate_hashes or duplicate_identity in duplicate_hashes:
            raise ValueError("DUPLICATE_PROPOSAL")
        if proposal.proposal_version > request.remaining_attempt_budget:
            raise ValueError("RESEARCH_ATTEMPT_BUDGET_EXCEEDED")
        if _parameter_units(proposal.proposed_rule.to_dict()) > request.remaining_parameter_search_budget:
            raise ValueError("PARAMETER_SEARCH_BUDGET_EXCEEDED")
        reasons.append("VALID")
        status = "VALID"
    except (ValueError, TypeError, KeyError, json.JSONDecodeError, RecursionError) as exc:
        reason = str(exc) or "INVALID_OUTPUT"
        if "contains unknown fields" in reason:
            reason = "SCHEMA_UNKNOWN_FIELDS"
        elif "missing required fields" in reason or "missing fields" in reason:
            reason = "SCHEMA_REQUIRED_FIELDS"
        reasons.append(reason[:160])
        status = "INVALID"
        proposal = None
    bound_result_hash = provider_result_hash or sha256_json({"raw_untrusted_output": raw_output})
    receipt = AgentValidationReceiptV1.create(request.request_key, proposal_hash, bound_result_hash,
                                              status, reasons, now_ns)
    return proposal, receipt


def validate_request_evidence_availability(request: ResearchProposalRequestV1) -> None:
    for item in request.evidence_manifest:
        if item.available_through_ns > request.development_cutoff_ns:
            raise ValueError("evidence manifest includes future information")
        if item.artifact_ref in request.inaccessible_holdout_identities:
            raise ValueError("final holdout identity cannot be a readable artifact ref")
