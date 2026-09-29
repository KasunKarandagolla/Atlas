"""Deterministic, all-or-nothing validation of untrusted proposal output."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from typing import Any

from atlas.v2._serialization import sha256_json, sha256_ref
from atlas.v2.agent_intelligence.contracts import (
    ACTION_ASSESSMENT_FINDING_TYPES,
    ACTION_ASSESSMENT_MAX_FINDINGS,
    ACTION_ASSESSMENT_SCHEMA_VERSION,
    MAX_PROPOSAL_BYTES,
    ActionAssessmentFindingV1,
    ActionAssessmentRequestV2,
    ActionAssessmentResultV2,
    AgentValidationReceiptV1,
    ResearchProposalRequestV1,
    ResearchProposalV1,
    SealedActionAssessmentPacketV1,
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

_CRITIC_UNSAFE_TEXT = re.compile(
    r"\b(?:recommend|recommendation|suggest|should|must|please|instruct|request|browse|search|read|open|fetch|"
    r"access|use another|switch to|select another|change|alter|modify|resize|increase|decrease|move|set)\b"
    r".{0,100}\b(?:buy|sell|long|short|candidate|direction|quantity|qty|size|entry|collar|stop|leverage|"
    r"risk\s*policy|capital|order|approval|approv|reservation|reserve|venue|account|credential|api\s*key|"
    r"file|path|url|tool|browser|database|profit|confidence|conviction|trade[- ]quality)\b|"
    r"\b(?:BUY|SELL)\b|\b(?:probability\s+of\s+profit|profitability|confidence|conviction|trade[- ]quality|"
    r"profitability\s+score|confidence\s+score|conviction\s+score|trade[- ]quality\s+score)\b|"
    r"\b(?:use|call|invoke|enable|create|request)\b.{0,100}\b(?:tools?|browser|web\s+search|"
    r"database|files?|credentials?|api\s+keys?|venue|capital|orders?|reservations?)\b|"
    r"sk-[A-Za-z0-9_-]{20,}|\bBearer\s+[A-Za-z0-9._~-]{16,}|AKIA[0-9A-Z]{16}|"
    r"gh[pousr]_[A-Za-z0-9]{20,}|https?://|file://|-----BEGIN [A-Z ]+PRIVATE KEY-----|"
    r"\b(?:use|choose|prefer|keep|take|enter|go)\b.{0,100}\b(?:buy|sell|long|short|candidate|direction|"
    r"quantity|qty|size|entry|collar|stop|leverage|risk\s*policy|capital|order|approval|reservation)\b",
    re.I | re.S,
)


def action_assessment_payload_schema() -> dict[str, Any]:
    """Strict provider-facing JSON Schema for the six frozen finding types."""
    ref = {"type": "string", "pattern": "^[0-9a-f]{64}$"}
    finding = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "finding_type": {"type": "string", "enum": sorted(ACTION_ASSESSMENT_FINDING_TYPES)},
            "subject_artifact_ref": ref,
            "supporting_evidence_refs": {"type": "array", "maxItems": 16, "uniqueItems": True, "items": ref},
            "explanation": {"type": "string", "minLength": 1, "maxLength": 1_000},
            "field_paths": {"type": "array", "maxItems": 8, "uniqueItems": True,
                "items": {"type": "string", "minLength": 2, "maxLength": 160}},
        },
        "required": ["finding_type", "subject_artifact_ref", "supporting_evidence_refs", "explanation"],
    }
    return {"type": "object", "additionalProperties": False,
            "properties": {"version": {"const": ACTION_ASSESSMENT_SCHEMA_VERSION},
                           "findings": {"type": "array", "maxItems": ACTION_ASSESSMENT_MAX_FINDINGS,
                                        "items": finding}},
            "required": ["version", "findings"]}


def action_assessment_schema_hash() -> str:
    return sha256_json({"version": ACTION_ASSESSMENT_SCHEMA_VERSION, "schema": action_assessment_payload_schema()})


def _strict_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("MALFORMED_DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def _json_pointer_exists(value: Any, pointer: str) -> bool:
    if not pointer.startswith("/"):
        return False
    current = value
    for encoded in pointer[1:].split("/"):
        part = encoded.replace("~1", "/").replace("~0", "~")
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        elif isinstance(current, (list, tuple)) and part.isdecimal() and int(part) < len(current):
            current = current[int(part)]
        else:
            return False
    return True


def validate_action_assessment_output(
    request: ActionAssessmentRequestV2,
    packet: SealedActionAssessmentPacketV1,
    raw_output: str,
    *,
    now_ns: int,
) -> tuple[ActionAssessmentResultV2 | None, tuple[str, ...]]:
    """Validate untrusted critic JSON atomically; never repair or accept partial findings."""
    try:
        if type(now_ns) is not int or now_ns >= request.deadline_ns:
            raise ValueError("DEADLINE_EXPIRED")
        if (request.packet_ref != packet.packet_ref or request.packet_hash != packet.content_hash
                or request.action_hash != packet.action_hash):
            raise ValueError("SEALED_PACKET_BINDING_MISMATCH")
        if len(raw_output.encode("utf-8")) > 12_000:
            raise ValueError("OUTPUT_SIZE_LIMIT")
        decoded = json.loads(raw_output, object_pairs_hook=_strict_json_object)
        if not isinstance(decoded, Mapping):
            raise ValueError("SCHEMA_ROOT_NOT_OBJECT")
        if set(decoded) != {"version", "findings"} or decoded.get("version") != ACTION_ASSESSMENT_SCHEMA_VERSION:
            raise ValueError("SCHEMA_UNKNOWN_OR_REQUIRED_FIELDS")
        findings_raw = decoded.get("findings")
        if not isinstance(findings_raw, list) or len(findings_raw) > ACTION_ASSESSMENT_MAX_FINDINGS:
            raise ValueError("FINDINGS_MALFORMED_OR_OVERSIZED")
        summary_by_ref: dict[str, Mapping[str, Any]] = {}
        summaries = packet.summaries.to_dict()
        for item in summaries.values():
            if not isinstance(item, Mapping):
                raise ValueError("SEALED_PACKET_SUMMARY_MALFORMED")
            artifact_ref = item.get("artifact_ref")
            summary = item.get("summary")
            if isinstance(artifact_ref, str) and isinstance(summary, Mapping):
                if artifact_ref in summary_by_ref:
                    raise ValueError("SEALED_PACKET_SUMMARY_AMBIGUOUS")
                summary_by_ref[artifact_ref] = summary
        findings: list[ActionAssessmentFindingV1] = []
        for raw_finding in findings_raw:
            if not isinstance(raw_finding, Mapping):
                raise ValueError("FINDING_NOT_OBJECT")
            allowed = {"finding_type", "subject_artifact_ref", "supporting_evidence_refs", "explanation", "field_paths"}
            required = allowed - {"field_paths"}
            if set(raw_finding) - allowed:
                raise ValueError("FINDING_UNKNOWN_FIELDS")
            if not required.issubset(raw_finding):
                raise ValueError("FINDING_REQUIRED_FIELDS_MISSING")
            finding = ActionAssessmentFindingV1(
                raw_finding["finding_type"], raw_finding["subject_artifact_ref"],
                tuple(raw_finding["supporting_evidence_refs"]), raw_finding["explanation"],
                tuple(raw_finding.get("field_paths", ())),
            )
            if finding.subject_artifact_ref not in packet.artifact_refs:
                raise ValueError("FINDING_SUBJECT_REF_UNBOUND")
            if finding.subject_artifact_ref not in summary_by_ref:
                raise ValueError("FINDING_SUBJECT_SUMMARY_UNAVAILABLE")
            if not finding.supporting_evidence_refs or not set(finding.supporting_evidence_refs).issubset(
                    set(packet.artifact_refs)):
                raise ValueError("FINDING_EVIDENCE_REF_UNBOUND")
            if any(not _json_pointer_exists(summary_by_ref[finding.subject_artifact_ref], path)
                   for path in finding.field_paths):
                raise ValueError("FINDING_FIELD_PATH_UNSUPPORTED")
            if _CRITIC_UNSAFE_TEXT.search(finding.explanation):
                raise ValueError("FINDING_CONTAINS_AUTHORITY_OR_TOOL_INSTRUCTION")
            findings.append(finding)
        result = ActionAssessmentResultV2(request.request_id, packet.packet_ref, packet.content_hash,
                                          packet.action_hash, "COMPLETE", tuple(findings))
        return result, ("VALID",)
    except (ValueError, TypeError, KeyError, json.JSONDecodeError, RecursionError, AttributeError) as exc:
        reason = str(exc) or "INVALID_ACTION_ASSESSMENT"
        return None, (reason[:160],)


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
