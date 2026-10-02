"""Post-receipt packet sealing for the additive, hidden action-critic path."""

from __future__ import annotations

import json
import socket
import threading
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from atlas.v2._serialization import FrozenMap, canonical_decimal_str, canonical_json, sha256_json
from atlas.v2.agent_intelligence.broker import InferenceBrokerClient
from atlas.v2.agent_intelligence.budget import DeepSeekPriceScheduleV1
from atlas.v2.agent_intelligence.contracts import (
    ACTION_ASSESSMENT_TASK_IDENTITY,
    ActionAssessmentProviderProfileV1,
    ActionAssessmentRequestV2,
    ProviderResultV1,
    SealedActionAssessmentPacketV1,
)
from atlas.v2.agent_intelligence.controller import ActionAssessmentController, DirectActionAssessmentBrokerPort
from atlas.v2.agent_intelligence.persistence import ActionAssessmentRepository
from atlas.v2.agent_intelligence.profile import deepseek_v41_flash_action_critic_profile
from atlas.v2.agent_intelligence.shadow_measurement import (
    build_action_critic_shadow_observation,
    index_action_critic_shadow_observation,
    index_packet_and_request,
)
from atlas.v2.contracts import CandidateActionV2, CandidateSetV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.action_critic_dispatcher import (
    ActionAssessmentDispatchWorkV1,
    ActionAssessmentShadowDispatcher,
)
from atlas.v2.runtime.ops_supervisor import OpsSupervisorReceiptV1, PipelineStageV1
from atlas.v2.science.admission import AmendedEvaluationArtifactV2
from atlas.v2.science.scenario_engine import PretradeExecutionScenarioV2


class ActionAssessmentPacketUnavailable(ValueError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class SealedActionAssessmentV1:
    packet: SealedActionAssessmentPacketV1
    request: ActionAssessmentRequestV2


def _entry(repository: OpsRepository, ref: str, expected_type: str | None = None) -> ArtifactIndexEntryV2:
    entry = repository.get_artifact(ref)
    if entry is None or (expected_type is not None and entry.artifact_type != expected_type):
        raise ActionAssessmentPacketUnavailable("REQUIRED_ARTIFACT_MISSING_OR_AMBIGUOUS")
    if entry.artifact_ref != ref or not isinstance(entry.metadata, Mapping):
        raise ActionAssessmentPacketUnavailable("REQUIRED_ARTIFACT_MISSING_OR_AMBIGUOUS")
    return entry


def _typed_body(entry: ArtifactIndexEntryV2, key: str) -> Mapping[str, Any]:
    body = entry.metadata.get(key)
    envelope_hashed = entry.artifact_type in {"CandidateSetV2", "CandidateActionV2"}
    custom_content_hash = entry.artifact_type == "PretradeExecutionScenarioV2"
    if (not isinstance(body, Mapping)
            or (not envelope_hashed and not custom_content_hash and sha256_json(body) != entry.content_hash)
            or (custom_content_hash and body.get("content_hash") != entry.content_hash)):
        raise ActionAssessmentPacketUnavailable(f"REQUIRED_ARTIFACT_CONTENT_MISMATCH_{entry.artifact_type}")
    # Repository metadata is recursively frozen; typed historical contracts expect JSON arrays.
    normalized = json.loads(canonical_json(body))
    if not isinstance(normalized, Mapping):
        raise ActionAssessmentPacketUnavailable("REQUIRED_ARTIFACT_CONTENT_MISMATCH")
    return normalized


def _summary(entry: ArtifactIndexEntryV2, body: Mapping[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    selected = {name: body[name] for name in fields if name in body}
    return {"artifact_ref": entry.artifact_ref, "artifact_type": entry.artifact_type,
            "content_hash": entry.content_hash, "available_at_ns": entry.available_at_ns,
            "summary": selected}


def _receipt_stage(receipt: OpsSupervisorReceiptV1, stage: PipelineStageV1):
    return next(item for item in receipt.result.stages if item.stage == stage)


def _need_single_ref(receipt: OpsSupervisorReceiptV1, stage: PipelineStageV1) -> str:
    refs = _receipt_stage(receipt, stage).artifact_refs
    if len(refs) != 1:
        raise ActionAssessmentPacketUnavailable("EXACT_FROZEN_ACTION_OR_EVIDENCE_UNAVAILABLE")
    return refs[0]


def build_sealed_action_assessment(*, repository: OpsRepository, receipt: OpsSupervisorReceiptV1,
        receipt_ref: str, profile: ActionAssessmentProviderProfileV1) -> SealedActionAssessmentV1:
    """Resolve only exact receipt-bound artifacts and project typed, bounded summaries."""
    try:
        receipt_entry = _entry(repository, receipt_ref, "OpsSupervisorReceiptV1")
        receipt_body = receipt_entry.metadata.get("receipt")
        if (not isinstance(receipt_body, Mapping) or sha256_json(receipt_body) != receipt_entry.content_hash
                or receipt_entry.content_hash != receipt.content_hash
                or canonical_json(receipt_body) != canonical_json(receipt.to_dict())):
            raise ActionAssessmentPacketUnavailable("ORIGINATING_RECEIPT_MISMATCH")
        if receipt.agent_mode != "DISABLED" or receipt.capital_enabled or receipt.assisted_enabled:
            raise ActionAssessmentPacketUnavailable("DETERMINISTIC_RECEIPT_AUTHORITY_INVARIANT_FAILED")

        action_stage = _receipt_stage(receipt, PipelineStageV1.FROZEN_ACTION)
        action_hash = action_stage.bound_action_hash
        if action_stage.status.value != "COMPLETE" or action_hash is None:
            raise ActionAssessmentPacketUnavailable("EXACT_FROZEN_ACTION_OR_EVIDENCE_UNAVAILABLE")
        action_ref = receipt.action_ref
        evaluation_ref = receipt.evaluation_ref
        candidate_set_ref = receipt.candidate_set_ref
        if action_ref is None or evaluation_ref is None or candidate_set_ref is None:
            raise ActionAssessmentPacketUnavailable("EXACT_FROZEN_ACTION_OR_EVIDENCE_UNAVAILABLE")
        action_entry = _entry(repository, action_ref, "ActionArtifactV2")
        action_body = _typed_body(action_entry, "action_artifact")
        identity = action_entry.metadata.get("action_identity")
        if (not isinstance(identity, Mapping) or sha256_json(identity) != action_hash
                or identity.get("action_hash") not in (None, action_hash)
                or action_body.get("action_hash") != action_hash
                or action_body.get("candidate_set_ref") != candidate_set_ref
                or action_hash != action_stage.bound_action_hash):
            raise ActionAssessmentPacketUnavailable("FROZEN_ACTION_BINDING_MISMATCH")

        candidate_set_entry = _entry(repository, candidate_set_ref, "CandidateSetV2")
        candidate_set_body = _typed_body(candidate_set_entry, "candidate_set")
        candidate_set = CandidateSetV2.from_dict(candidate_set_body)
        if (candidate_set.content_hash != candidate_set_ref or candidate_set.decision_event_id != receipt.event.event_id
                or candidate_set.selection_status.value != "SELECTED"
                or candidate_set.selected_candidate_id is None
                or candidate_set.selection_policy_hash == ""
                or action_body.get("candidate_set_ref") != candidate_set_ref):
            raise ActionAssessmentPacketUnavailable("CANDIDATE_SET_BINDING_MISMATCH")
        candidate_ref = str(action_body.get("candidate_ref", ""))
        sizing_ref = str(action_body.get("sizing_ref", ""))
        candidate_entry = _entry(repository, candidate_ref, "CandidateActionV2")
        candidate_body = _typed_body(candidate_entry, "candidate")
        candidate = CandidateActionV2.from_dict(candidate_body)
        sizing_entry = _entry(repository, sizing_ref, "SizingDecisionV2")
        sizing_body = _typed_body(sizing_entry, "sizing")
        selected_row = next((item for item in candidate_set.candidates
                             if item.candidate_id == candidate_set.selected_candidate_id), None)
        if (candidate.content_hash != candidate_ref or candidate.candidate_id != candidate_set.selected_candidate_id
                or selected_row is None or selected_row.key != candidate.key or selected_row.side != candidate.side
                or sizing_body.get("selected_candidate_id") != candidate.candidate_id
                or sizing_body.get("status") != "SIZED"
                or sizing_body.get("candidate_ref") != candidate_ref
                or sizing_body.get("candidate_set_ref") != candidate_set_ref
                or sizing_body.get("quantity") != identity.get("quantity")
                or sizing_body.get("risk_policy_hash") != identity.get("risk_policy_hash")
                or sizing_body.get("risk_policy_v2_hash") != identity.get("risk_policy_v2_hash")):
            raise ActionAssessmentPacketUnavailable("SELECTED_CANDIDATE_BINDING_MISMATCH")

        evaluation_entry = _entry(repository, evaluation_ref, "EvaluationArtifactV2")
        evaluation_body = _typed_body(evaluation_entry, "evaluation")
        evaluation = AmendedEvaluationArtifactV2.from_dict(evaluation_body)
        evaluation_bindings = {
            "evaluation_ref": evaluation.content_hash == evaluation_ref,
            "action_hash": evaluation.action_hash == action_hash,
            "action_ref": evaluation.action_artifact_ref == action_ref,
            "candidate_ref": evaluation.candidate_ref == candidate_ref,
            "candidate_set_ref": evaluation.candidate_set_ref == candidate_set_ref,
            "selector_policy_hash": evaluation.selection_policy_hash == candidate_set.selection_policy_hash,
            "quantity": canonical_decimal_str(evaluation.quantity) == identity.get("quantity"),
            "policy_hash": evaluation.policy_hash == identity.get("policy_hash"),
            "risk_policy_hash": evaluation.risk_policy_hash == identity.get("risk_policy_hash"),
            "risk_policy_v2_hash": evaluation.risk_policy_v2_hash == identity.get("risk_policy_v2_hash"),
            "deadline": candidate.deadline_ns == evaluation.action_expiry_ns,
            "side": identity.get("side") == candidate.side.value,
            "key": canonical_json(identity.get("key")) == canonical_json(candidate.key.to_dict()),
            "entry_reference": identity.get("entry_reference") == canonical_decimal_str(candidate.entry_reference),
            "entry_collar": identity.get("entry_collar") == canonical_decimal_str(candidate.entry_collar),
            "stop_price": identity.get("stop_price") == canonical_decimal_str(candidate.stop_price),
        }
        if not all(evaluation_bindings.values()):
            failed = "_".join(name for name, matches in evaluation_bindings.items() if not matches)
            raise ActionAssessmentPacketUnavailable(f"ECONOMIC_EVALUATION_BINDING_MISMATCH_{failed}")

        if identity.get("policy_hash") != candidate.policy_hash:
            raise ActionAssessmentPacketUnavailable("SELECTOR_POLICY_BINDING_MISMATCH")
        if evaluation.m0_model_ref == evaluation.m0_prediction_ref:
            raise ActionAssessmentPacketUnavailable("ECONOMIC_EVALUATION_EVIDENCE_AMBIGUOUS")

        m0_model_entry = _entry(repository, evaluation.m0_model_ref, "M0ModelFitV2")
        m0_model = _typed_body(m0_model_entry, "model_fit")
        m0_prediction_entry = _entry(repository, evaluation.m0_prediction_ref, "M0PredictionV2")
        m0_prediction = _typed_body(m0_prediction_entry, "prediction")
        m0_bindings = {
            "model_action_hash": m0_model.get("current_action_hash") == action_hash,
            "model_action_ref": m0_model.get("current_action_artifact_ref") == action_ref,
            "prediction_action_hash": m0_prediction.get("action_hash") == action_hash,
            "prediction_action_ref": m0_prediction.get("action_artifact_ref") == action_ref,
            "prediction_model_ref": m0_prediction.get("model_ref") == evaluation.m0_model_ref,
        }
        if not all(m0_bindings.values()):
            failed = "_".join(name for name, matches in m0_bindings.items() if not matches)
            raise ActionAssessmentPacketUnavailable(f"M0_ACTION_BINDING_MISMATCH_{failed}")

        m1_ref = _need_single_ref(receipt, PipelineStageV1.M1_DIAGNOSTIC)
        m1_entry = _entry(repository, m1_ref)
        m1_key = "prediction" if m1_entry.artifact_type == "M1PredictionV2" else "diagnostic"
        m1_body = _typed_body(m1_entry, m1_key)
        if m1_entry.artifact_type not in {"M1PredictionV2", "OpsZeroAuthorityDiagnosticV1"}:
            raise ActionAssessmentPacketUnavailable("M1_DIAGNOSTIC_TYPE_UNSUPPORTED")
        if (m1_body.get("action_hash") != action_hash
                or m1_body.get("action_artifact_ref") != action_ref
                or (m1_body.get("kind") not in (None, "M1"))):
            raise ActionAssessmentPacketUnavailable("M1_ACTION_BINDING_MISMATCH")
        analogue_ref = _need_single_ref(receipt, PipelineStageV1.ANALOGUE_DIAGNOSTIC)
        analogue_entry = _entry(repository, analogue_ref)
        analogue_key = "analogue" if analogue_entry.artifact_type == "AnalogueActionValueV2" else "diagnostic"
        analogue_body = _typed_body(analogue_entry, analogue_key)
        if analogue_entry.artifact_type not in {"AnalogueActionValueV2", "OpsZeroAuthorityDiagnosticV1"}:
            raise ActionAssessmentPacketUnavailable("ANALOGUE_DIAGNOSTIC_TYPE_UNSUPPORTED")
        if analogue_entry.artifact_type == "AnalogueActionValueV2":
            analogue_matches = (analogue_body.get("query_action_hash") == action_hash
                and analogue_body.get("query_action_ref") == action_ref
                and analogue_body.get("query_candidate_ref") == candidate_ref
                and analogue_body.get("query_candidate_set_ref") == candidate_set_ref)
        else:
            analogue_matches = (analogue_body.get("action_hash") == action_hash
                and analogue_body.get("action_artifact_ref") == action_ref
                and analogue_body.get("kind") == "ANALOGUE")
        if not analogue_matches:
            raise ActionAssessmentPacketUnavailable("ANALOGUE_ACTION_BINDING_MISMATCH")

        exact_fields = {
            "pretrade_scenario_ref": "PretradeExecutionScenarioV2",
            "estimation_uncertainty_ref": "EstimationUncertaintyV2",
            "execution_uncertainty_ref": "ExecutionModelUncertaintyV2",
            "numerical_error_ref": "NumericalErrorV2",
            "support_ref": "InferenceSupportV2",
            "calibration_ref": "M0CalibrationV2",
            "ood_ref": "M0OODV2",
            "deterministic_stress_ref": "DeterministicStressV2",
            "portfolio_ref": "DecisionTimePortfolioScenariosV2",
        }
        resolved: dict[str, tuple[ArtifactIndexEntryV2, Mapping[str, Any]]] = {}
        for field, artifact_type in exact_fields.items():
            ref = getattr(evaluation, "existing_portfolio_ref" if field == "portfolio_ref" else field)
            item = _entry(repository, ref, artifact_type)
            metadata_key = {"pretrade_scenario_ref": "scenario", "calibration_ref": "calibration",
                            "ood_ref": "ood"}.get(field, "evidence")
            body = _typed_body(item, metadata_key)
            if body.get("action_hash") != action_hash:
                raise ActionAssessmentPacketUnavailable("SAME_ACTION_DERIVED_EVIDENCE_REQUIRED")
            if field == "pretrade_scenario_ref":
                scenario = PretradeExecutionScenarioV2.from_dict(body)
                if scenario.content_hash != ref or scenario.action_hash != action_hash \
                        or scenario.action_artifact_ref != action_ref:
                    raise ActionAssessmentPacketUnavailable("SCENARIO_ACTION_BINDING_MISMATCH")
            if field == "portfolio_ref" and (body.get("candidate_scenario_ref") != evaluation.pretrade_scenario_ref
                    or body.get("action_artifact_ref") != action_ref):
                raise ActionAssessmentPacketUnavailable("PORTFOLIO_ACTION_BINDING_MISMATCH")
            resolved[field] = (item, body)

        es_candidates = repository.artifact_entries_by_types(("PortfolioESV2",), limit=10_000)
        if len(es_candidates) >= 10_000:
            raise ActionAssessmentPacketUnavailable("PORTFOLIO_ES_EVIDENCE_AMBIGUOUS")
        matching_es: list[tuple[ArtifactIndexEntryV2, Mapping[str, Any]]] = []
        for es_entry in es_candidates:
            es_body = es_entry.metadata.get("evidence")
            if (isinstance(es_body, Mapping) and es_entry.content_hash == sha256_json(es_body)
                    and es_body.get("action_hash") == action_hash
                    and es_body.get("portfolio_scenario_ref") == evaluation.existing_portfolio_ref
                    and es_body.get("es_before_fraction") == (canonical_decimal_str(evaluation.es_before) if evaluation.es_before is not None else None)
                    and es_body.get("es_after_fraction") == (canonical_decimal_str(evaluation.es_after) if evaluation.es_after is not None else None)):
                matching_es.append((es_entry, es_body))
        if len(matching_es) != 1:
            raise ActionAssessmentPacketUnavailable("PORTFOLIO_ES_EVIDENCE_AMBIGUOUS_OR_MISSING")
        es_entry, es_body = matching_es[0]

        # S27 source-health states are immutable receipt fields without external detail refs.
        source_evidence_refs: tuple[str, ...] = ()
        post_selection_types = {"CandidateActionV2", "ActionArtifactV2", "SizingDecisionV2",
                                "EvaluationArtifactV2", "DecisionCalendarV2"}
        market_ref_entries = {ref: _entry(repository, ref) for ref in receipt.event.causal_input_refs}
        market_refs = tuple(sorted(ref for ref, entry in market_ref_entries.items()
                                   if entry.artifact_type not in post_selection_types))
        for ref in market_refs:
            entry = market_ref_entries[ref]
            if entry.available_at_ns > receipt.event.information_cutoff_ns:
                raise ActionAssessmentPacketUnavailable("FUTURE_MARKET_EVIDENCE")

        summaries: dict[str, Any] = {}
        candidate_summary_body = {name: getattr(candidate, name).value if hasattr(getattr(candidate, name), "value")
            else getattr(candidate, name) for name in ("candidate_id", "key", "policy_hash", "snapshot_hash",
            "side", "decision_at_ns", "deadline_ns", "horizon_end_ns", "entry_reference", "entry_collar",
            "stop_price", "state_version", "cost_model_ref", "quantity")}
        candidate_summary_body["key"] = candidate.key.to_dict()
        for name in ("entry_reference", "entry_collar", "stop_price", "quantity"):
            value = getattr(candidate, name)
            candidate_summary_body[name] = canonical_decimal_str(value) if value is not None else None
        artifact_map: dict[str, ArtifactIndexEntryV2] = {receipt_ref: receipt_entry,
            candidate_set_ref: candidate_set_entry, candidate_ref: candidate_entry, action_ref: action_entry,
            sizing_ref: sizing_entry,
            evaluation_ref: evaluation_entry, evaluation.m0_model_ref: m0_model_entry,
            evaluation.m0_prediction_ref: m0_prediction_entry, m1_ref: m1_entry, analogue_ref: analogue_entry,
            es_entry.artifact_ref: es_entry}
        artifact_bodies: dict[str, Mapping[str, Any]] = {
            action_ref: identity,
            candidate_ref: candidate_summary_body,
            sizing_ref: sizing_body,
                candidate_set_ref: {"selected_candidate_id": candidate_set.selected_candidate_id,
                    "selection_policy_hash": candidate_set.selection_policy_hash,
                    "selection_status": candidate_set.selection_status.value,
                    "selected_candidate_policy_id": selected_row.policy_id,
                    "selected_candidate_key": selected_row.key.to_dict(),
                    "selected_candidate_side": selected_row.side.value,
                    "selected_candidate_ref": candidate_ref},
            evaluation_ref: evaluation.to_dict(),
            evaluation.m0_model_ref: m0_model,
            evaluation.m0_prediction_ref: m0_prediction,
            m1_ref: m1_body,
            analogue_ref: analogue_body,
            es_entry.artifact_ref: es_body,
        }
        for _field, (item, body) in resolved.items():
            artifact_map[item.artifact_ref] = item
            artifact_bodies[item.artifact_ref] = body
        for ref in market_refs:
            artifact_map[ref] = market_ref_entries[ref]
        for ref in source_evidence_refs:
            artifact_map[ref] = _entry(repository, ref)

        action_summary_body = {"action_hash": action_hash, "action_identity": identity,
            "candidate_ref": action_body.get("candidate_ref"),
            "candidate_set_ref": action_body.get("candidate_set_ref"),
            "sizing_ref": action_body.get("sizing_ref")}
        summaries[action_ref] = _summary(action_entry, action_summary_body, tuple(action_summary_body.keys()))
        summaries[candidate_ref] = _summary(candidate_entry, candidate_summary_body,
            tuple(candidate_summary_body.keys()))
        summaries[sizing_ref] = _summary(sizing_entry, sizing_body, (
            "candidate_ref", "candidate_set_ref", "quantity", "risk_policy_hash", "risk_policy_v2_hash",
            "status", "reason_codes"))
        summaries[candidate_set_ref] = _summary(candidate_set_entry, artifact_bodies[candidate_set_ref],
            ("selected_candidate_id", "selection_policy_hash", "selection_status", "selected_candidate_policy_id",
             "selected_candidate_key", "selected_candidate_side", "selected_candidate_ref"))
        summaries[evaluation_ref] = _summary(evaluation_entry, evaluation.to_dict(), (
            "action_hash", "action_artifact_ref", "candidate_ref", "candidate_set_ref", "quantity", "policy_hash",
            "selection_policy_hash", "m0_model_ref", "m0_prediction_ref", "pretrade_scenario_ref",
            "deterministic_stress_ref", "existing_portfolio_ref", "expected_net_value", "expected_pnl_lcb",
            "estimation_uncertainty_ref", "execution_uncertainty_ref", "numerical_error_ref", "support_ref",
            "calibration_ref", "ood_ref", "es_before", "es_after", "decision", "reason_codes"))
        m0_model_summary = {
            "action_hash": m0_model.get("current_action_hash"),
            "action_artifact_ref": m0_model.get("current_action_artifact_ref"),
            "sample_count": m0_model.get("sample_count"),
            "status": m0_model.get("status"),
            "reasons": m0_model.get("reasons", []),
        }
        summaries[evaluation.m0_model_ref] = _summary(m0_model_entry, m0_model_summary,
            tuple(m0_model_summary.keys()))
        summaries[evaluation.m0_prediction_ref] = _summary(m0_prediction_entry, m0_prediction, (
            "action_hash", "action_artifact_ref", "model_ref", "expected_net_value", "estimation_uncertainty",
            "numerical_conversion_error", "support_ref", "calibration_ref", "ood_ref", "status", "reasons"))
        summaries[m1_ref] = _summary(m1_entry, m1_body, tuple(name for name in (
            "version", "kind", "action_hash", "action_artifact_ref", "candidate_ref", "candidate_set_ref",
            "information_cutoff_ns", "available_at_ns", "expected_net_value", "status", "reasons", "reason")
            if name in m1_body))
        summaries[analogue_ref] = _summary(analogue_entry, analogue_body, tuple(name for name in (
            "version", "kind", "query_action_hash", "query_action_ref", "query_candidate_ref",
            "query_candidate_set_ref", "information_cutoff_ns", "weighted_estimate", "support_status",
            "ood_status", "compatible_population_count", "independent_support_count", "reasons", "status", "reason")
            if name in analogue_body))
        for _field, (item, body) in resolved.items():
            fields = tuple(name for name in (
                "version", "action_hash", "action_artifact_ref", "information_cutoff_ns", "status",
                "support_status", "ood_status", "expected_net_value", "estimation_uncertainty",
                "uncertainty", "conversion_error", "scenario_count", "reason_codes", "reasons",
                "candidate_scenario_ref", "common_scenario_set_id", "unknown_exposure_present") if name in body)
            summaries[item.artifact_ref] = _summary(item, body, fields)
        summaries[es_entry.artifact_ref] = _summary(es_entry, es_body, (
            "action_hash", "portfolio_scenario_ref", "confidence", "limit_fraction", "es_before_fraction",
            "es_after_fraction", "status", "breach", "reason"))
        summaries[receipt_ref] = {"artifact_ref": receipt_ref, "artifact_type": "OpsSupervisorReceiptV1",
            "content_hash": receipt_entry.content_hash, "available_at_ns": receipt_entry.available_at_ns,
            "summary": {"source_health_state": receipt.source_health_state,
                "agent_mode": receipt.agent_mode, "capital_enabled": receipt.capital_enabled,
                "assisted_enabled": receipt.assisted_enabled,
                "source_states": [item.to_dict() for item in receipt.source_states]}}
        for ref in market_refs:
            entry = artifact_map[ref]
            if ref not in summaries:
                summaries[ref] = {"artifact_ref": ref, "artifact_type": entry.artifact_type,
                    "content_hash": entry.content_hash, "created_at_ns": entry.created_at_ns,
                    "available_at_ns": entry.available_at_ns,
                    "summary": {"event_type": receipt.event.event_type, "source_id": receipt.event.source_id,
                        "source_event_at_ns": receipt.event.source_event_at_ns,
                        "source_published_at_ns": receipt.event.source_published_at_ns,
                        "information_cutoff_ns": receipt.event.information_cutoff_ns}}
        for ref in source_evidence_refs:
            entry = artifact_map[ref]
            summaries[ref] = {"artifact_ref": ref, "artifact_type": entry.artifact_type,
                "content_hash": entry.content_hash, "available_at_ns": entry.available_at_ns,
                "summary": {"source_health_evidence": True}}

        refs = tuple(sorted(artifact_map))
        required_packet_refs = {
            "originating_receipt_ref": receipt_ref, "candidate_set_ref": candidate_set_ref,
            "selected_candidate_ref": candidate_ref, "action_artifact_ref": action_ref,
            "economic_evaluation_ref": evaluation_ref, "m0_model_ref": evaluation.m0_model_ref,
            "m0_prediction_ref": evaluation.m0_prediction_ref, "m1_diagnostic_ref": m1_ref,
            "analogue_diagnostic_ref": analogue_ref, "pretrade_scenario_ref": evaluation.pretrade_scenario_ref,
            "estimation_uncertainty_ref": evaluation.estimation_uncertainty_ref,
            "execution_uncertainty_ref": evaluation.execution_uncertainty_ref,
            "numerical_error_ref": evaluation.numerical_error_ref, "support_ref": evaluation.support_ref,
            "calibration_ref": evaluation.calibration_ref, "ood_ref": evaluation.ood_ref,
            "deterministic_stress_ref": evaluation.deterministic_stress_ref,
            "portfolio_ref": evaluation.existing_portfolio_ref, "portfolio_es_ref": es_entry.artifact_ref,
        }
        missing_required_refs = [name for name, ref in required_packet_refs.items() if ref not in refs]
        if missing_required_refs:
            raise ActionAssessmentPacketUnavailable("PACKET_REQUIRED_REFS_MISSING_" + "_".join(missing_required_refs))
        t0 = receipt.event.information_cutoff_ns
        cutoff_t = max(t0, *(artifact_map[ref].available_at_ns for ref in refs))
        deadline_d = min(receipt.event.deadline_ns, candidate.deadline_ns, evaluation.action_expiry_ns)
        if cutoff_t >= deadline_d:
            raise ActionAssessmentPacketUnavailable("SEALED_PACKET_DEADLINE_EXPIRED")
        missingness = {"source_state_details_ref": {
            item.source_id: "MISSING"
            for item in receipt.source_states},
            "m1_status": m1_body.get("status", "UNKNOWN"),
            "analogue_status": analogue_body.get("support_status", analogue_body.get("status", "UNKNOWN"))}
        packet = SealedActionAssessmentPacketV1(
            originating_receipt_ref=receipt_ref,
            decision_event_id=receipt.event.event_id,
            candidate_set_ref=candidate_set_ref,
            selected_candidate_ref=candidate_ref,
            selector_policy_hash=candidate_set.selection_policy_hash,
            action_artifact_ref=action_ref,
            action_hash=action_hash,
            economic_evaluation_ref=evaluation_ref,
            m0_model_ref=evaluation.m0_model_ref,
            m0_prediction_ref=evaluation.m0_prediction_ref,
            m1_diagnostic_ref=m1_ref,
            analogue_diagnostic_ref=analogue_ref,
            pretrade_scenario_ref=evaluation.pretrade_scenario_ref,
            estimation_uncertainty_ref=evaluation.estimation_uncertainty_ref,
            execution_uncertainty_ref=evaluation.execution_uncertainty_ref,
            numerical_error_ref=evaluation.numerical_error_ref,
            support_ref=evaluation.support_ref,
            calibration_ref=evaluation.calibration_ref,
            ood_ref=evaluation.ood_ref,
            deterministic_stress_ref=evaluation.deterministic_stress_ref,
            portfolio_ref=evaluation.existing_portfolio_ref,
            portfolio_es_ref=es_entry.artifact_ref,
            source_health_ref=receipt_ref,
            source_health_evidence_refs=source_evidence_refs,
            market_evidence_refs=market_refs,
            artifact_refs=refs,
            artifact_types=FrozenMap({ref: artifact_map[ref].artifact_type for ref in refs}),
            availability_by_ref=FrozenMap({ref: artifact_map[ref].available_at_ns for ref in refs}),
            source_cutoff_t0_ns=t0,
            sealed_cutoff_t_ns=cutoff_t,
            original_deadline_d_ns=deadline_d,
            missingness=FrozenMap(missingness),
            summaries=FrozenMap(summaries),
        )
        request_id = str(uuid.uuid5(uuid.NAMESPACE_URL,
            f"atlas-action-assessment-v2:{packet.packet_ref}:{profile.content_hash}"))
        request = ActionAssessmentRequestV2(request_id, packet.packet_ref, packet.content_hash, packet.action_hash,
            ACTION_ASSESSMENT_TASK_IDENTITY, profile.content_hash, profile.provider_binding_hash,
            profile.price_schedule_hash, profile.provider, profile.requested_model_id, profile.model_family,
            profile.revision_status, profile.endpoint, profile.prompt_hash, profile.schema_hash,
            packet.original_deadline_d_ns)
        return SealedActionAssessmentV1(packet, request)
    except ActionAssessmentPacketUnavailable:
        raise
    except Exception as exc:
        raise ActionAssessmentPacketUnavailable("SEALED_PACKET_AMBIGUOUS_OR_INVALID") from exc


class _UnixBrokerClientPort(DirectActionAssessmentBrokerPort):
    """Open one fixed local broker socket per authorized one-call task."""

    def __init__(self, socket_path: str) -> None:
        self._socket_path = socket_path

    def assess(self, *, capability: str, authorization_id: str, attempt_id: str,
               request: ActionAssessmentRequestV2,
               packet: SealedActionAssessmentPacketV1) -> ProviderResultV1:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as channel:
            channel.settimeout(35.0)
            channel.connect(self._socket_path)
            return InferenceBrokerClient(channel).assess_action_v1(capability=capability,
                authorization_id=authorization_id, attempt_id=attempt_id, request=request, packet=packet)

    def execute(self, work: ActionAssessmentDispatchWorkV1) -> ProviderResultV1:
        return self.assess(capability=work.capability, authorization_id=work.identity.authorization_id,
            attempt_id=work.identity.attempt_id, request=work.request, packet=work.packet)


class ActionAssessmentShadowCoordinator:
    """Main-writer prepare/finalize coordinator with bounded external-I/O dispatch."""

    def __init__(self, *, profile: ActionAssessmentProviderProfileV1, controller: ActionAssessmentController,
                 ledger: Any, dispatcher: ActionAssessmentShadowDispatcher | None = None,
                 now_ns: Any = time.time_ns) -> None:
        self.profile = profile
        self.controller = controller
        self.ledger = ledger
        self.dispatcher = dispatcher
        self._now_ns = now_ns
        self._writer_thread_id = threading.get_ident()
        self._works: dict[str, ActionAssessmentDispatchWorkV1] = {}
        self._closed = False

    def __call__(self, receipt: OpsSupervisorReceiptV1, receipt_ref: str,
                 repository: OpsRepository) -> None:
        self._assert_writer_thread()
        try:
            sealed = build_sealed_action_assessment(repository=repository, receipt=receipt,
                receipt_ref=receipt_ref, profile=self.profile)
        except ActionAssessmentPacketUnavailable as exc:
            self.controller.record_skip(receipt_ref, exc.code)
            return
        except Exception:
            self.controller.record_skip(receipt_ref, "SEALED_PACKET_AMBIGUOUS_OR_INVALID")
            return
        # A receipt can be replayed by the deterministic supervisor while its immutable
        # critic work is still queued, running, or awaiting controller-thread finalization.
        # That replay must not re-enter prepare(), which correctly treats any orphaned
        # durable authorization as a lost-on-restart dispatch.
        if sealed.request.request_id in self._works:
            return
        try:
            index_packet_and_request(repository, receipt=receipt, packet=sealed.packet, request=sealed.request)
        except Exception:
            try:
                self.controller.record_skip(receipt_ref, "MEASUREMENT_BINDING_UNAVAILABLE",
                    request=sealed.request, packet=sealed.packet)
                self._project_observations(repository, limit=1)
            except Exception:
                pass
            return

        dispatcher = self.dispatcher
        capacity = dispatcher.reserve_capacity() if dispatcher is not None else None
        if capacity is None:
            reason = "BROKER_UNAVAILABLE" if dispatcher is None else "DISPATCHER_CAPACITY_UNAVAILABLE"
            try:
                self.controller.record_skip(receipt_ref, reason, request=sealed.request, packet=sealed.packet)
                self._project_observations(repository, limit=1)
            except Exception:
                pass
            return
        assert dispatcher is not None
        try:
            prepared = self.controller.prepare(sealed.request, sealed.packet)
            if not isinstance(prepared, ActionAssessmentDispatchWorkV1):
                dispatcher.release_capacity(capacity)
                self._project_observations(repository, limit=1)
                return
            self._works[prepared.identity.request_id] = prepared
            if not dispatcher.submit_reserved(capacity, prepared):
                self._works.pop(prepared.identity.request_id, None)
                self.controller.abandon_authorized_work(prepared, "DISPATCH_HANDOFF_FAILED")
                self._project_observations(repository, limit=1)
        except Exception:
            dispatcher.release_capacity(capacity)
            try:
                self.controller.fail_safe(receipt_ref=receipt_ref, request=sealed.request,
                    packet=sealed.packet, reason_code="CALLBACK_FAILED")
                self._project_observations(repository, limit=1)
            except Exception:
                # Ledger or projection failure remains confined to the shadow-only branch.
                pass

    def drain_completed(self, *, max_items: int = 1, repository: OpsRepository) -> int:
        """Poll without waiting; all validation and persistence stays on the writer thread."""
        self._assert_writer_thread()
        if self.dispatcher is None or self._closed:
            self._project_observations(repository, limit=max_items)
            return 0
        completions = self.dispatcher.drain_completed(max_items=max_items)
        finalized = 0
        for completion in completions:
            work = self._works.pop(completion.identity.request_id, None)
            if work is None:
                # Unknown/stale completion identities are rejected without persistence.
                continue
            try:
                self.controller.finalize(work, completion)
                finalized += 1
            except Exception:
                # Durable dispatch remains non-redispatchable and restart recovery marks it lost.
                continue
        self._project_observations(repository, limit=max_items)
        return finalized

    def _project_observations(self, repository: OpsRepository, *, limit: int) -> int:
        rows = self.controller.pending_observation_records(limit=limit)
        projected = 0
        for row in rows:
            try:
                packet = SealedActionAssessmentPacketV1.from_dict(json.loads(str(row["packet_json"])))
                request = ActionAssessmentRequestV2.from_dict(json.loads(str(row["request_json"])))
                receipt_entry = repository.get_artifact(packet.originating_receipt_ref)
                receipt_body = receipt_entry.metadata.get("receipt") if receipt_entry is not None else None
                if not isinstance(receipt_body, Mapping):
                    continue
                from atlas.v2.runtime.ops_supervisor import _receipt_from_dict

                receipt = _receipt_from_dict(receipt_body)
                index_packet_and_request(repository, receipt=receipt, packet=packet, request=request)
                observation = build_action_critic_shadow_observation(repository, row,
                    recorded_at_ns=max(int(row["received_at_ns"]), self._now_ns()))
                ref = index_action_critic_shadow_observation(repository, observation)
                self.controller.mark_observation_projected(observation.request_id, ref, now_ns=self._now_ns())
                projected += 1
            except Exception:
                continue
        return projected

    def _assert_writer_thread(self) -> None:
        if threading.get_ident() != self._writer_thread_id:
            raise RuntimeError("critic prepare/finalize belongs to the atlas-ops controller thread")

    def close(self) -> None:
        self._closed = True
        if self.dispatcher is not None:
            self.dispatcher.close()
        if self.ledger is not None:
            self.ledger.close()


def create_action_assessment_shadow(database_path: str, socket_path: str | None, *,
                                    repository_root: str | None = None,
                                    capability_signing_key: bytes | None = None,
                                    broker_port: DirectActionAssessmentBrokerPort | None = None,
                                    ) -> ActionAssessmentShadowCoordinator:
    """Compose only the fixed local broker operation and writer-owned shadow ledger."""
    import os
    from pathlib import Path

    from atlas.v2.resources import resource_file

    root = Path(repository_root) if repository_root is not None else resource_file(".")
    schedule = DeepSeekPriceScheduleV1.load(
        root / "configs/agent_intelligence/provider_pricing_deepseek_v41_flash_v1.json")
    profile = deepseek_v41_flash_action_critic_profile(price_schedule=schedule,
        agent_lock_path=root / "requirements-agent-lock.txt")
    ledger = ActionAssessmentRepository(database_path, price_schedule=schedule)
    signing_text = os.environ.get("ATLAS_AGENT_CAPABILITY_KEY", "")
    try:
        signing_key = capability_signing_key if capability_signing_key is not None else (bytes.fromhex(signing_text) if signing_text else None)
    except ValueError:
        signing_key = None
    if signing_key is not None and len(signing_key) < 32:
        signing_key = None
    usable_socket = socket_path if socket_path and Path(socket_path).exists() else None
    io_port = broker_port if broker_port is not None else (_UnixBrokerClientPort(usable_socket) if usable_socket else None)
    dispatcher = ActionAssessmentShadowDispatcher(io_port) if io_port is not None and signing_key is not None else None
    controller = ActionAssessmentController(ledger=ledger, profile=profile,
        capability_signing_key=signing_key, provider=io_port)
    controller.recover_open_dispatches()
    return ActionAssessmentShadowCoordinator(profile=profile, controller=controller, ledger=ledger,
        dispatcher=dispatcher)


class _LazyActionAssessmentShadow:
    """Keep default atlas-ops composition untouched until explicit critic opt-in."""

    def __init__(self, database_path: str, socket_path: str | None) -> None:
        self._database_path = database_path
        self._socket_path = socket_path
        self._coordinator: ActionAssessmentShadowCoordinator | None = None
        self._init_failed = False

    def _get_coordinator(self) -> ActionAssessmentShadowCoordinator | None:
        if self._coordinator is not None or self._init_failed:
            return self._coordinator
        try:
            self._coordinator = create_action_assessment_shadow(self._database_path, self._socket_path)
        except Exception:
            # Setup failure is shadow-only; deterministic processing remains available.
            self._init_failed = True
        return self._coordinator

    def __call__(self, receipt: OpsSupervisorReceiptV1, receipt_ref: str,
                 repository: OpsRepository) -> None:
        coordinator = self._get_coordinator()
        if coordinator is not None:
            coordinator(receipt, receipt_ref, repository)

    def drain_completed(self, *, max_items: int, repository: OpsRepository) -> int:
        coordinator = self._get_coordinator()
        return coordinator.drain_completed(max_items=max_items, repository=repository) if coordinator else 0

    def close(self) -> None:
        if self._coordinator is not None:
            self._coordinator.close()
