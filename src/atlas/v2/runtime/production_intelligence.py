"""Post-receipt, zero-authority projection of persisted production evidence."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Mapping
from decimal import Decimal
from typing import Any

from atlas.v2._serialization import FrozenMap, canonical_json, json_value, sha256_json, timestamp
from atlas.v2.contracts import CandidateActionV2, CandidateSetV2
from atlas.v2.instruments import UniverseContractV2
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.research.decision_brief import (
    BriefLimitsV1,
    BriefRequestV1,
    DecisionBriefV1,
    build_decision_brief,
)
from atlas.v2.research.memory import RESEARCH_LANE, ResearchMemoryV1
from atlas.v2.runtime.ops_supervisor import OpsSupervisorReceiptV1
from atlas.v2.science.action import ActionArtifactV2, FrozenActionV2

PRODUCTION_PROJECTION_VERSION = "ATLAS_PRODUCTION_INTELLIGENCE_PROJECTION_V1"
MAX_PROJECTION_REFS = 4096
MAX_PROJECTION_BYTES = 1_000_000
BRIEF_LIMITS = BriefLimitsV1(
    "PRODUCTION_BRIEF_BOUNDS_V1",
    max_rows=256,
    max_bytes=MAX_PROJECTION_BYTES,
    max_instruments=256,
    max_image_refs=0,
    max_projected_tokens=MAX_PROJECTION_BYTES,
)
RESEARCH_POLICY_HASH = sha256_json(
    {
        "version": PRODUCTION_PROJECTION_VERSION,
        "authority": "ZERO",
        "provider_dispatch": "DISABLED",
        "fact_adapters": [],
    }
)


def _indexed(repo: OpsRepository, ref: str, *, as_of_ns: int) -> bool:
    entry = repo.get_artifact(ref)
    effective = repo.effective_available_at_ns(ref)
    return entry is not None and entry.available_at_ns <= as_of_ns and effective is not None and effective <= as_of_ns


def _blocked_brief(
    *,
    receipt: OpsSupervisorReceiptV1,
    receipt_ref: str,
    now_ns: int,
    reasons: tuple[str, ...],
    population_ref: str | None,
    candidate_set_ref: str | None = None,
    universe_ref: str | None = None,
    original_candidate_count: int | None = None,
) -> DecisionBriefV1:
    return DecisionBriefV1(
        "EXPIRED" if now_ns >= receipt.event.deadline_ns else "BLOCKED",
        reasons or ("PRODUCTION_EVIDENCE_UNAVAILABLE",),
        FrozenMap(
            {
                "slot_id": receipt.event.event_id,
                "cutoff_ns": receipt.event.information_cutoff_ns,
                "deadline_ns": receipt.event.deadline_ns,
                "built_at_ns": now_ns,
                "snapshot_ref": receipt.event.trigger_ref,
                "candidate_set_ref": candidate_set_ref,
                "universe_ref": universe_ref,
                "source_population_ref": population_ref,
                "original_candidate_count": original_candidate_count,
                "calendar_denominator": 1,
                "calendar_event_id": receipt.event.event_id,
                "a0_selected_candidate_id": None,
                "calendar": [],
                "shortlist": [],
                "economics": [],
                "facts": [],
                "missing_sections": [
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
                ],
                "ml": [],
                "image_refs": [],
                "authority": "ZERO",
                "shadow_only": True,
                "research_policy_hash": RESEARCH_POLICY_HASH,
                "receipt_ref": receipt_ref,
                "economic_value": "NOT_ESTIMABLE",
                "source_text_trust": "UNTRUSTED_DATA",
            }
        ),
    )


def _read_candidate_set(repo: OpsRepository, ref: str, receipt: OpsSupervisorReceiptV1) -> CandidateSetV2:
    entry = repo.get_artifact(ref)
    raw = entry.metadata.get("candidate_set") if entry is not None else None
    if (
        entry is None
        or entry.artifact_type != "CandidateSetV2"
        or entry.artifact_ref != ref
        or entry.content_hash != ref
        or not isinstance(raw, Mapping)
    ):
        raise ValueError("CANDIDATE_SET_INDEX_PROOF_MISSING")
    candidate_set = CandidateSetV2.from_dict(json_value(raw))
    if (
        candidate_set.content_hash != ref
        or candidate_set.decision_event_id != receipt.event.event_id
        or candidate_set.envelope.available_at_ns != entry.available_at_ns
        or entry.available_at_ns > receipt.created_at_ns
    ):
        raise ValueError("CANDIDATE_SET_IDENTITY_OR_CHRONOLOGY")
    return candidate_set


def _read_universe(
    repo: OpsRepository, candidate_set: CandidateSetV2, receipt: OpsSupervisorReceiptV1
) -> UniverseContractV2:
    entry = repo.get_artifact(candidate_set.universe_ref)
    raw = entry.metadata.get("universe") if entry is not None else None
    if (
        entry is None
        or entry.artifact_type != "UniverseContractV2"
        or entry.artifact_ref != candidate_set.universe_ref
        or entry.content_hash != candidate_set.universe_ref
        or not isinstance(raw, Mapping)
    ):
        raise ValueError("UNIVERSE_INDEX_PROOF_MISSING")
    universe = UniverseContractV2.from_dict(json_value(raw))
    if (
        universe.content_hash != candidate_set.universe_ref
        or universe.decision_slot_ns < receipt.event.information_cutoff_ns
        or universe.envelope.available_at_ns != entry.available_at_ns
        or entry.available_at_ns > receipt.event.information_cutoff_ns
    ):
        raise ValueError("UNIVERSE_IDENTITY_OR_CHRONOLOGY")
    return universe


def _candidate_by_id(repo: OpsRepository, candidate_set: CandidateSetV2, candidate_id: str) -> CandidateActionV2:
    matches: list[CandidateActionV2] = []
    for ref in candidate_set.envelope.input_refs:
        entry = repo.get_artifact(ref)
        raw = (
            entry.metadata.get("candidate")
            if entry is not None and entry.artifact_type == "CandidateActionV2"
            else None
        )
        if not isinstance(raw, Mapping):
            continue
        candidate = CandidateActionV2.from_dict(json_value(raw))
        if candidate.content_hash == ref and candidate.candidate_id == candidate_id:
            matches.append(candidate)
    if len(matches) != 1:
        raise ValueError("SELECTED_CANDIDATE_INDEX_PROOF_MISSING_OR_AMBIGUOUS")
    candidate = matches[0]
    entry = repo.get_artifact(candidate.content_hash)
    if entry is None or entry.available_at_ns > candidate.decision_at_ns:
        raise ValueError("SELECTED_CANDIDATE_CHRONOLOGY_UNAVAILABLE")
    return candidate


def _frozen_action(raw: Mapping[str, Any]) -> FrozenActionV2:
    from atlas.v2.instruments import InstrumentKeyV2

    return FrozenActionV2(
        InstrumentKeyV2.from_dict(json_value(raw["key"])),
        str(raw["side"]),
        Decimal(str(raw["quantity"])),
        str(raw["product_ref"]),
        FrozenMap(json_value(raw["entry_rule"])),
        FrozenMap(json_value(raw["collar_rule"])),
        Decimal(str(raw["entry_reference"])),
        Decimal(str(raw["entry_collar"])),
        Decimal(str(raw["stop_price"])),
        str(raw["entry_trigger_basis"]),
        str(raw["stop_trigger_basis"]),
        FrozenMap(json_value(raw["management_rule"])),
        FrozenMap(json_value(raw["time_exit_rule"])),
        int(raw["horizon_end_ns"]),
        str(raw["policy_id"]),
        str(raw["policy_version"]),
        str(raw["policy_hash"]),
        str(raw["risk_policy_hash"]),
        str(raw["risk_policy_v2_hash"]),
    )


def _read_selected_action(
    repo: OpsRepository,
    receipt: OpsSupervisorReceiptV1,
    candidate_set: CandidateSetV2,
    universe: UniverseContractV2,
    now_ns: int,
) -> tuple[CandidateActionV2 | None, dict[str, Any]]:
    action_ref = receipt.action_ref
    sizing_ref = receipt.sizing_ref
    candidate_id = candidate_set.selected_candidate_id
    if action_ref is None or sizing_ref is None or candidate_id is None:
        return None, {
            "status": "UNAVAILABLE",
            "missingness": ["A0_ACTION_OR_SIZING_NOT_PERSISTED"],
            "action_ref": action_ref,
            "sizing_ref": sizing_ref,
        }
    candidate = _candidate_by_id(repo, candidate_set, candidate_id)
    sizing_entry = repo.get_artifact(sizing_ref)
    sizing = sizing_entry.metadata.get("sizing") if sizing_entry is not None else None
    risk_input_refs = sizing.get("risk_input_refs") if isinstance(sizing, Mapping) else None
    if (
        sizing_entry is None
        or sizing_entry.artifact_type != "SizingDecisionV2"
        or not isinstance(sizing, Mapping)
        or sizing_entry.content_hash != sha256_json(sizing)
        or sizing.get("candidate_ref") != candidate.content_hash
        or sizing.get("candidate_set_ref") != candidate_set.content_hash
        or sizing.get("selected_candidate_id") != candidate_id
        or sizing.get("available_at_ns") != sizing_entry.available_at_ns
        or sizing_entry.available_at_ns > now_ns
        or sizing.get("status") != "SIZED"
        or not isinstance(risk_input_refs, (list, tuple))
        or not risk_input_refs
        or any(not _indexed(repo, ref, as_of_ns=receipt.event.information_cutoff_ns) for ref in risk_input_refs)
    ):
        return candidate, {
            "status": "NOT_QUALIFIED",
            "missingness": ["HARD_RISK_SIZING_IDENTITY_OR_STATUS"],
            "action_ref": action_ref,
            "sizing_ref": sizing_ref,
        }
    action_entry = repo.get_artifact(action_ref)
    action_meta = action_entry.metadata if action_entry is not None else {}
    raw_action = action_meta.get("action_identity")
    action_artifact_raw = action_meta.get("action_artifact")
    if (
        action_entry is None
        or action_entry.artifact_type != "ActionArtifactV2"
        or not isinstance(raw_action, Mapping)
        or not isinstance(action_artifact_raw, Mapping)
    ):
        return candidate, {
            "status": "NOT_QUALIFIED",
            "missingness": ["FROZEN_ACTION_INDEX_PROOF_MISSING"],
            "action_ref": action_ref,
            "sizing_ref": sizing_ref,
        }
    frozen = _frozen_action(raw_action)
    action = ActionArtifactV2(
        frozen,
        candidate.content_hash,
        sizing_ref,
        candidate_set.content_hash,
        action_entry.available_at_ns,
    )
    product_ref = next((entry.product_ref for entry in universe.entries if entry.key == candidate.key), None)
    product_entry = repo.get_artifact(product_ref) if product_ref is not None else None
    risk_policy = repo.get_artifact(frozen.risk_policy_hash)
    risk_policy_v2 = repo.get_artifact(frozen.risk_policy_v2_hash)
    if (
        action.content_hash != action_ref
        or action_entry.content_hash != action_ref
        or canonical_json(action.to_dict()) != canonical_json(action_artifact_raw)
        or frozen.action_hash != action_artifact_raw.get("action_hash")
        or action_entry.available_at_ns > now_ns
        or frozen.key != candidate.key
        or frozen.side != candidate.side.value
        or frozen.policy_hash != candidate.policy_hash
        or frozen.entry_reference != candidate.entry_reference
        or frozen.entry_collar != candidate.entry_collar
        or frozen.stop_price != candidate.stop_price
        or frozen.horizon_end_ns != candidate.horizon_end_ns
        or frozen.product_ref != product_ref
        or product_entry is None
        or product_entry.artifact_type != "ProductContractV2"
        or product_entry.content_hash != product_ref
        or product_entry.available_at_ns > receipt.event.information_cutoff_ns
        or risk_policy is None
        or risk_policy.artifact_type != "RiskPolicyV1"
        or risk_policy.content_hash != frozen.risk_policy_hash
        or risk_policy.available_at_ns > receipt.event.information_cutoff_ns
        or risk_policy_v2 is None
        or risk_policy_v2.artifact_type != "RiskPolicyV2"
        or risk_policy_v2.content_hash != frozen.risk_policy_v2_hash
        or risk_policy_v2.available_at_ns > receipt.event.information_cutoff_ns
        or sizing.get("quantity") is None
        or Decimal(str(sizing.get("quantity"))) != frozen.quantity
    ):
        return candidate, {
            "status": "NOT_QUALIFIED",
            "missingness": ["FROZEN_ACTION_IDENTITY_MISMATCH"],
            "action_ref": action_ref,
            "sizing_ref": sizing_ref,
        }
    return candidate, {
        "status": "QUALIFIED_PERSISTED_A0_EXACT_ACTION",
        "missingness": [],
        "action_ref": action_ref,
        "sizing_ref": sizing_ref,
        "action_hash": frozen.action_hash,
        "candidate_ref": candidate.content_hash,
        "candidate_id": candidate.candidate_id,
        "action": raw_action,
        "risk_sizing": sizing,
    }


def _validate_receipt(repo: OpsRepository, receipt: OpsSupervisorReceiptV1, receipt_ref: str) -> tuple[str, ...]:
    receipt_entry = repo.get_artifact(receipt_ref)
    if (
        receipt_entry is None
        or receipt_entry.artifact_type != "OpsSupervisorReceiptV1"
        or receipt_entry.content_hash != sha256_json(receipt.to_dict())
        or canonical_json(receipt_entry.metadata.get("receipt")) != canonical_json(receipt.to_dict())
    ):
        raise ValueError("SEALED_RECEIPT_INDEX_PROOF_MISSING")
    identity_ref = sha256_json({"artifact_type": "OpsDecisionEventIdentityV1", "event_id": receipt.event.event_id})
    trigger_identity = repo.get_artifact(identity_ref)
    if (
        trigger_identity is None
        or trigger_identity.content_hash != receipt.event.content_hash
        or canonical_json(trigger_identity.metadata.get("event")) != canonical_json(receipt.event.to_dict())
    ):
        raise ValueError("DECISION_EVENT_IDENTITY_PROOF_MISSING")
    refs = {
        receipt.event.trigger_ref,
        *receipt.event.causal_input_refs,
        *receipt.calendar_refs,
        *(
            ref
            for ref in (receipt.candidate_set_ref, receipt.sizing_ref, receipt.action_ref, receipt.evaluation_ref)
            if ref
        ),
        *(ref for stage in receipt.result.stages for ref in stage.artifact_refs),
    }
    if len(refs) > MAX_PROJECTION_REFS:
        raise ValueError("PROJECTION_REFERENCE_LIMIT")
    missing = tuple(sorted(ref for ref in refs if not _indexed(repo, ref, as_of_ns=receipt.created_at_ns)))
    return missing


def _relevant_due_work(
    repo: OpsRepository,
    receipt: OpsSupervisorReceiptV1,
    candidate_set: CandidateSetV2 | None,
    *,
    now_ns: int,
) -> tuple[str, str, tuple[str, ...], tuple[dict[str, Any], ...]]:
    rows = repo.due_work_items(RESEARCH_LANE, as_of_ns=2**63 - 1, limit=64)
    relevant: list[dict[str, Any]] = []
    refs: set[str] = set()
    unknown_sources = 0
    for row in rows:
        entry = repo.get_artifact(row.source_ref)
        if entry is None or entry.available_at_ns > now_ns:
            unknown_sources += 1
            continue
        if entry.artifact_type != "AnalysisResearchRequestV1" or entry.content_hash != sha256_json(entry.metadata):
            continue
        body = entry.metadata
        if body.get("authority") != "ZERO":
            unknown_sources += 1
            continue
        same_slot = (
            body.get("calendar_slot_id") == receipt.event.event_id
            and body.get("snapshot_ref") == receipt.event.trigger_ref
            and body.get("cutoff_ns") == receipt.event.information_cutoff_ns
            and body.get("deadline_ns") == receipt.event.deadline_ns
            and candidate_set is not None
            and body.get("candidate_set_ref") == candidate_set.content_hash
        )
        if not same_slot:
            continue
        refs.add(row.source_ref)
        relevant.append(
            {
                "lane": row.lane,
                "work_id": row.work_id,
                "request_ref": row.source_ref,
                "created_at_ns": row.created_at_ns,
                "due_at_ns": row.due_at_ns,
                "status": "DUE" if row.due_at_ns <= now_ns else "SCHEDULED",
                "authority": "ZERO",
            }
        )
    if len(rows) == 64:
        return "PAGE_LIMIT_REACHED", "DUE_WORK_PAGE_BOUND_REACHED", tuple(sorted(refs)), tuple(relevant)
    if unknown_sources:
        return "UNKNOWN", "DUE_WORK_SOURCE_UNVERIFIED", tuple(sorted(refs)), tuple(relevant)
    if relevant:
        return "MATCHED", "CAUSAL_SLOT_AND_CANDIDATE_SCOPE_VERIFIED", tuple(sorted(refs)), tuple(relevant)
    return "NONE", "NO_MATCHING_PERSISTED_RESEARCH_DUE_WORK", (), ()


def project_production_receipt(
    repo: OpsRepository,
    receipt: OpsSupervisorReceiptV1,
    receipt_ref: str,
    *,
    now_ns: int,
    clock_ns: Callable[[], int] | None = None,
) -> tuple[str, str]:
    """Persist one bounded read-only brief and memory view for an A0 receipt."""
    prior_identity = repo.get_artifact(ResearchMemoryV1.production_projection_identity(receipt_ref))
    if prior_identity is not None:
        # The memory service validates both immutable pointers and returns their original refs.
        return ResearchMemoryV1(repo).persist_production_projection(
            receipt_ref=receipt_ref,
            brief={},
            brief_status="BLOCKED",
            source_refs=(),
            source_population_ref=receipt_ref,
            now_ns=now_ns,
        )

    reasons: list[str] = []
    candidate_set: CandidateSetV2 | None = None
    universe: UniverseContractV2 | None = None
    candidate_count: int | None = None
    source_refs: tuple[str, ...] = (receipt_ref,)
    try:
        missing = _validate_receipt(repo, receipt, receipt_ref)
        if missing:
            reasons.append("INDEXED_CAUSAL_SOURCE_PROOF_MISSING")
        candidate_ref = receipt.candidate_set_ref
        if candidate_ref is None:
            reasons.append("CANDIDATE_SET_NOT_PERSISTED")
        else:
            candidate_set = _read_candidate_set(repo, candidate_ref, receipt)
            candidate_count = len(candidate_set.candidates)
            universe = _read_universe(repo, candidate_set, receipt)
            if len(candidate_set.envelope.input_refs) > MAX_PROJECTION_REFS:
                reasons.append("CANDIDATE_INPUT_REFERENCE_LIMIT")
        refs = {
            receipt_ref,
            receipt.event.trigger_ref,
            *receipt.event.causal_input_refs,
            *receipt.calendar_refs,
            *(
                ref
                for ref in (
                    receipt.candidate_set_ref,
                    receipt.sizing_ref,
                    receipt.action_ref,
                    receipt.evaluation_ref,
                )
                if ref
            ),
            *(ref for stage in receipt.result.stages for ref in stage.artifact_refs),
        }
        if candidate_set is not None:
            refs.update((candidate_set.content_hash, candidate_set.universe_ref, *candidate_set.envelope.input_refs))
            missing_candidate_inputs = tuple(
                ref
                for ref in candidate_set.envelope.input_refs
                if not _indexed(repo, ref, as_of_ns=receipt.created_at_ns)
            )
            if missing_candidate_inputs:
                reasons.append("CANDIDATE_SET_INPUT_PROOF_MISSING")
        if len(refs) > MAX_PROJECTION_REFS:
            reasons.append("PROJECTION_REFERENCE_LIMIT")
            source_refs = (receipt_ref,)
        else:
            source_refs = tuple(sorted(refs))
    except (KeyError, TypeError, ValueError, RuntimeError) as error:
        error_code = str(error)
        if error_code in {"PROJECTION_REFERENCE_LIMIT", "CANDIDATE_INPUT_REFERENCE_LIMIT"}:
            reasons.append(error_code)
        else:
            reasons.append("PERSISTED_PRODUCTION_EVIDENCE_INVALID_OR_UNAVAILABLE")

    built_at_ns = max(now_ns, receipt.created_at_ns)
    try:
        due_work_status, due_work_reason, due_work_refs, due_work_rows = _relevant_due_work(
            repo, receipt, candidate_set, now_ns=built_at_ns
        )
    except Exception:
        due_work_status, due_work_reason, due_work_refs, due_work_rows = (
            "UNKNOWN",
            "DUE_WORK_INDEX_READ_FAILED",
            (),
            (),
        )
    if len(set(source_refs) | set(due_work_refs)) <= MAX_PROJECTION_REFS:
        source_refs = tuple(sorted(set(source_refs) | set(due_work_refs)))
    elif due_work_refs:
        due_work_status = "PAGE_LIMIT_REACHED"
        due_work_reason = "PROJECTION_REFERENCE_LIMIT"
        due_work_refs = ()
    if candidate_set is not None and universe is not None:
        try:
            compiled = build_decision_brief(
                BriefRequestV1(
                    slot_id=receipt.event.event_id,
                    cutoff_ns=receipt.event.information_cutoff_ns,
                    deadline_ns=receipt.event.deadline_ns,
                    built_at_ns=built_at_ns,
                    candidate_set=candidate_set,
                    universe=universe,
                    snapshot_ref=receipt.event.trigger_ref,
                    research_policy_hash=RESEARCH_POLICY_HASH,
                    limits=BRIEF_LIMITS,
                    facts=(),
                    economics=(),
                    forecasts=(),
                ),
                repository=repo,
            )
            if compiled.status == "COMPLETE":
                reasons.append("PRODUCTION_TYPED_SECTIONS_NOT_RECONSTRUCTED")
                compiled = DecisionBriefV1(
                    "BLOCKED",
                    tuple(reasons),
                    FrozenMap(
                        {
                            **compiled.body.to_dict(),
                            "authority": "ZERO",
                            "shadow_only": True,
                            "production_evidence_status": "PARTIAL_INDEXED_EVIDENCE",
                        }
                    ),
                )
            if compiled.status == "BLOCKED":
                reasons.extend(compiled.reasons)
                if reasons:
                    compiled = DecisionBriefV1(
                        "BLOCKED",
                        tuple(sorted({*compiled.reasons, *reasons})),
                        compiled.body,
                    )
            brief = compiled
        except (KeyError, TypeError, ValueError, RuntimeError):
            reasons.append("DECISION_BRIEF_COMPILER_EVIDENCE_REJECTED")
            brief = _blocked_brief(
                receipt=receipt,
                receipt_ref=receipt_ref,
                now_ns=built_at_ns,
                reasons=tuple(sorted(set(reasons))),
                population_ref=candidate_set.content_hash,
                candidate_set_ref=candidate_set.content_hash,
                universe_ref=universe.content_hash,
                original_candidate_count=candidate_count,
            )
    else:
        brief = _blocked_brief(
            receipt=receipt,
            receipt_ref=receipt_ref,
            now_ns=built_at_ns,
            reasons=tuple(sorted(set(reasons))),
            population_ref=receipt_ref,
            candidate_set_ref=receipt.candidate_set_ref,
            original_candidate_count=candidate_count,
        )

    body = brief.body.to_dict()
    stage_population = [stage.to_dict() for stage in receipt.result.stages]
    selected_action: dict[str, Any] = {
        "status": "UNAVAILABLE",
        "missingness": ["CANDIDATE_SET_UNAVAILABLE"],
        "action_ref": receipt.action_ref,
        "sizing_ref": receipt.sizing_ref,
    }
    candidate: CandidateActionV2 | None = None
    economics: dict[str, Any] = {
        "status": "NOT_ESTIMABLE",
        "missingness": ["EXACT_ACTION_ECONOMICS_NOT_RECONSTRUCTED"],
        "evaluation_ref": receipt.evaluation_ref,
        "calendar_refs": list(receipt.calendar_refs),
    }
    if candidate_set is not None:
        try:
            candidate, selected_action = _read_selected_action(repo, receipt, candidate_set, universe, built_at_ns)
            if candidate is not None and selected_action.get("status") == "QUALIFIED_PERSISTED_A0_EXACT_ACTION":
                recovered = production_recover_economic_evaluation(
                    repo,
                    candidate,
                    selected_action,
                    candidate_set,
                    now_ns=built_at_ns,
                )
                if recovered is not None:
                    evaluation = recovered.evaluation
                    qualified = (
                        evaluation.decision.value == "CANDIDATE"
                        and evaluation.expected_net_value is not None
                        and evaluation.expected_pnl_lcb is not None
                        and evaluation.es_before is not None
                        and evaluation.es_after is not None
                        and evaluation.action_expiry_ns > built_at_ns
                    )
                    economics = {
                        "status": "QUALIFIED_EXACT_ACTION_AFTER_COST_EVALUATION" if qualified else "NOT_ESTIMABLE",
                        "missingness": [] if qualified else list(evaluation.reason_codes),
                        "evaluation_ref": recovered.evaluation_ref,
                        "calendar_ref": recovered.calendar_ref,
                        "decision": evaluation.decision.value,
                        "expected_net_value": str(evaluation.expected_net_value) if qualified else None,
                        "expected_pnl_lcb": str(evaluation.expected_pnl_lcb) if qualified else None,
                        "es_before": str(evaluation.es_before)
                        if qualified and evaluation.es_before is not None
                        else None,
                        "es_after": str(evaluation.es_after) if qualified and evaluation.es_after is not None else None,
                        "source_refs": [
                            recovered.evaluation.account_snapshot_ref,
                            recovered.evaluation.pretrade_scenario_ref,
                            recovered.evaluation.existing_portfolio_ref,
                            recovered.evaluation.deterministic_stress_ref,
                            recovered.evaluation.outcome_distribution_ref,
                            recovered.evaluation.support_ref,
                            recovered.evaluation.ood_ref,
                        ],
                    }
                else:
                    economics["missingness"] = ["EXACT_ACTION_EVALUATION_NOT_INDEXED"]
        except (KeyError, TypeError, ValueError, RuntimeError):
            selected_action = {
                "status": "NOT_QUALIFIED",
                "missingness": ["A0_ACTION_OR_SIZING_ARCHIVE_CHECK_FAILED"],
                "action_ref": receipt.action_ref,
                "sizing_ref": receipt.sizing_ref,
            }
            economics["missingness"] = ["EXACT_ACTION_ECONOMIC_ARCHIVE_CHECK_FAILED"]

    candidate_rows = candidate_set.candidates if candidate_set is not None else ()
    screening_counts = Counter(item.eligibility_status.value for item in candidate_rows)
    if economics.get("status") == "QUALIFIED_EXACT_ACTION_AFTER_COST_EVALUATION":
        economic_refs = economics.get("source_refs", ())
        if isinstance(economic_refs, (list, tuple)):
            combined_refs = set(source_refs) | set(economic_refs)
            if len(combined_refs) <= MAX_PROJECTION_REFS:
                source_refs = tuple(sorted(combined_refs))
            else:
                economics = {
                    **economics,
                    "status": "NOT_ESTIMABLE",
                    "missingness": ["PROJECTION_REFERENCE_LIMIT"],
                    "expected_net_value": None,
                    "expected_pnl_lcb": None,
                    "es_before": None,
                    "es_after": None,
                }
    slot_class = receipt.result.terminal_status.value
    stage_by_name = {item.stage.value: item for item in receipt.result.stages}
    m1_stage = stage_by_name.get("M1_DIAGNOSTIC")
    analogue_stage = stage_by_name.get("ANALOGUE_DIAGNOSTIC")
    missing_sections = list(body.get("missing_sections", ()))
    if not missing_sections:
        missing_sections = [
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
        ]
    body.update(
        {
            "production_projection_version": PRODUCTION_PROJECTION_VERSION,
            "receipt_ref": receipt_ref,
            "source_cutoff_ns": receipt.event.information_cutoff_ns,
            "computed_at_ns": built_at_ns,
            "hard_deadline_ns": receipt.event.deadline_ns,
            "source_population_ref": candidate_set.content_hash if candidate_set is not None else receipt_ref,
            "immutable_source_refs": list(source_refs),
            "source_availability": [
                {
                    "source_ref": ref,
                    "index_status": "MISSING" if (entry := repo.get_artifact(ref)) is None else "INDEXED",
                    "artifact_type": entry.artifact_type if entry is not None else None,
                    "created_at_ns": entry.created_at_ns if entry is not None else None,
                    "available_at_ns": entry.available_at_ns if entry is not None else None,
                    "effective_available_at_ns": repo.effective_available_at_ns(ref),
                }
                for ref in source_refs
            ],
            "stage_receipts": stage_population,
            "candidate_set_ref": candidate_set.content_hash if candidate_set is not None else None,
            "universe_ref": universe.content_hash if universe is not None else None,
            "product_revisions": [
                {"key": item.key.to_dict(), "product_ref": item.product_ref} for item in universe.entries
            ]
            if universe is not None
            else [],
            "source_health": {
                "state": receipt.source_health_state,
                "source_states": [item.to_dict() for item in receipt.source_states],
                "source_refs": list(stage_by_name["CAUSAL_FEATURES"].artifact_refs),
            },
            "full_screening_denominator": {
                "scope": "ONE_OPS_DECISION_EVENT",
                "whole_schedule_denominator_status": "UNAVAILABLE",
                "whole_schedule_reason": "PRODUCTION_RECEIPT_IS_SLOT_SCOPED",
                "decision_slots_in_this_receipt": 1,
                "candidate_rows": len(candidate_rows),
                "candidate_status_counts": dict(sorted(screening_counts.items())),
                "candidate_set_population_ref": candidate_set.content_hash if candidate_set is not None else None,
                "decision_calendar_refs": list(receipt.calendar_refs),
                "terminal_status": slot_class,
                "a0_selection_status": candidate_set.selection_status.value
                if candidate_set is not None
                else "UNAVAILABLE",
            },
            "a0_selected_candidate_id": candidate_set.selected_candidate_id if candidate_set is not None else None,
            "candidate_action_qualification": selected_action,
            "candidate_ranking": {
                "status": "PRELIMINARY_ONLY_NO_AFTER_COST_INFERENCE",
                "candidate_set_ref": candidate_set.content_hash if candidate_set is not None else None,
                "selection_policy_hash": candidate_set.selection_policy_hash if candidate_set is not None else None,
                "original_candidate_count": len(candidate_rows),
                "selection_status": candidate_set.selection_status.value
                if candidate_set is not None
                else "UNAVAILABLE",
            },
            "diagnostic_qualification": {
                "m1": {
                    "status": m1_stage.status.value if m1_stage is not None else "UNAVAILABLE",
                    "qualification": "NOT_ESTIMABLE_TYPED_DIAGNOSTIC_NOT_RECONSTRUCTED",
                    "evidence_refs": list(m1_stage.artifact_refs) if m1_stage is not None else [],
                    "bound_action_hash": m1_stage.bound_action_hash if m1_stage is not None else None,
                    "reason": m1_stage.reason if m1_stage is not None else "M1_STAGE_NOT_REPORTED",
                },
                "analogue": {
                    "status": analogue_stage.status.value if analogue_stage is not None else "UNAVAILABLE",
                    "qualification": "NOT_ESTIMABLE_TYPED_DIAGNOSTIC_NOT_RECONSTRUCTED",
                    "evidence_refs": list(analogue_stage.artifact_refs) if analogue_stage is not None else [],
                    "bound_action_hash": analogue_stage.bound_action_hash if analogue_stage is not None else None,
                    "reason": analogue_stage.reason if analogue_stage is not None else "ANALOGUE_STAGE_NOT_REPORTED",
                },
                "ml_forecast_status": "NOT_ESTIMABLE_NO_VERIFIED_INSTALLED_FORECAST",
                "ml_forecast_qualification": "UNQUALIFIED",
            },
            "economic_qualification": economics,
            "economic_value": economics.get("expected_pnl_lcb")
            if economics.get("status") == "QUALIFIED_EXACT_ACTION_AFTER_COST_EVALUATION"
            else "NOT_ESTIMABLE",
            "missing_sections": sorted(set(missing_sections)),
            "unavailable_section_reasons": dict.fromkeys(
                sorted(set(missing_sections)), "NO_REGISTERED_PRODUCTION_FACT_READER"
            ),
            "agent_mode": "DISABLED",
            "capital_enabled": False,
            "assisted_execution_enabled": False,
            "authority": "ZERO",
            "provider_job_status": "NOT_CREATED_NO_AUTHORITY",
            "projection_bounds": {
                "max_rows": BRIEF_LIMITS.max_rows,
                "max_bytes": BRIEF_LIMITS.max_bytes,
                "max_instruments": BRIEF_LIMITS.max_instruments,
                "max_image_refs": BRIEF_LIMITS.max_image_refs,
                "max_projected_tokens": BRIEF_LIMITS.max_projected_tokens,
                "max_references": MAX_PROJECTION_REFS,
                "token_projection_method": "UTF8_BYTE_UPPER_BOUND_NOT_TOKENIZER",
            },
            "memory_status": {
                "prior_analysis": "NOT_REUSED_NO_EXACT_ANALYSIS_MATCH",
                "due_work_status": due_work_status,
                "due_work_reason": due_work_reason,
                "due_work": list(due_work_rows),
            },
        }
    )
    if clock_ns is not None:
        final_computed_at_ns = max(built_at_ns, timestamp(clock_ns(), field="production intelligence final time"))
        if final_computed_at_ns >= receipt.event.deadline_ns:
            built_at_ns = final_computed_at_ns
            body["computed_at_ns"] = final_computed_at_ns
            body["economic_qualification"] = {
                "status": "NOT_ESTIMABLE",
                "missingness": ["HARD_DEADLINE_REACHED_DURING_PROJECTION"],
                "evaluation_ref": receipt.evaluation_ref,
                "calendar_refs": list(receipt.calendar_refs),
            }
            body["economic_value"] = "NOT_ESTIMABLE"
            brief = DecisionBriefV1(
                "EXPIRED",
                ("HARD_DEADLINE_REACHED_DURING_PROJECTION",),
                FrozenMap(body),
            )
    brief = DecisionBriefV1(brief.status, brief.reasons, FrozenMap(body))

    measured_bytes = len(canonical_json({"brief": brief.to_dict(), "projection_body": body}).encode("utf-8"))
    if measured_bytes > MAX_PROJECTION_BYTES:
        body = {
            "version": PRODUCTION_PROJECTION_VERSION,
            "slot_id": receipt.event.event_id,
            "receipt_ref": receipt_ref,
            "source_cutoff_ns": receipt.event.information_cutoff_ns,
            "computed_at_ns": built_at_ns,
            "hard_deadline_ns": receipt.event.deadline_ns,
            "source_population_ref": candidate_set.content_hash if candidate_set is not None else receipt_ref,
            "required_bytes": measured_bytes,
            "max_bytes": MAX_PROJECTION_BYTES,
            "limit_status": "BLOCKED_BYTE_LIMIT",
            "authority": "ZERO",
            "agent_mode": "DISABLED",
            "capital_enabled": False,
            "assisted_execution_enabled": False,
            "economic_value": "NOT_ESTIMABLE",
            "missing_sections": missing_sections,
        }
        brief = DecisionBriefV1("BLOCKED", ("BYTE_LIMIT",), FrozenMap(body))
    elif brief.status == "COMPLETE":
        # Production currently has no complete typed reader set for the declared
        # S47 sections. A candidate-only compiler result is never a complete packet.
        brief = DecisionBriefV1(
            "BLOCKED",
            ("PRODUCTION_TYPED_SECTIONS_NOT_RECONSTRUCTED",),
            FrozenMap({**body, "production_evidence_status": "PARTIAL_INDEXED_EVIDENCE"}),
        )

    # Add the live status after byte checking; these fields are already included in
    # the brief body and remain bounded by the same single-megabyte ceiling.
    projection_meta = {
        "brief": brief.to_dict(),
        "status": brief.status,
        "authority": "ZERO",
        "receipt_ref": receipt_ref,
        "source_population_ref": candidate_set.content_hash if candidate_set is not None else receipt_ref,
        "source_refs": list(source_refs),
        "created_at_ns": built_at_ns,
    }
    final_size = len(canonical_json(projection_meta).encode("utf-8"))
    if final_size > MAX_PROJECTION_BYTES:
        brief = DecisionBriefV1(
            "BLOCKED",
            ("BYTE_LIMIT",),
            FrozenMap(
                {
                    "slot_id": receipt.event.event_id,
                    "receipt_ref": receipt_ref,
                    "required_bytes": final_size,
                    "max_bytes": MAX_PROJECTION_BYTES,
                    "source_population_ref": candidate_set.content_hash if candidate_set is not None else receipt_ref,
                    "authority": "ZERO",
                    "agent_mode": "DISABLED",
                    "capital_enabled": False,
                    "assisted_execution_enabled": False,
                    "economic_value": "NOT_ESTIMABLE",
                    "missing_sections": missing_sections,
                }
            ),
        )
        projection_meta["brief"] = brief.to_dict()
        projection_meta["status"] = "BLOCKED"

    memory = ResearchMemoryV1(repo)
    return memory.persist_production_projection(
        receipt_ref=receipt_ref,
        brief=brief.to_dict(),
        brief_status=brief.status,
        source_refs=source_refs,
        source_population_ref=candidate_set.content_hash if candidate_set is not None else receipt_ref,
        now_ns=built_at_ns,
        due_work_status=due_work_status,
        due_work_reason=due_work_reason,
        due_work_refs=due_work_refs,
    )


def production_recover_economic_evaluation(
    repo: OpsRepository,
    candidate: CandidateActionV2,
    selected_action: dict[str, Any],
    candidate_set: CandidateSetV2,
    *,
    now_ns: int,
) -> Any:
    action_ref = selected_action.get("action_ref")
    action_entry = repo.get_artifact(str(action_ref)) if isinstance(action_ref, str) else None
    raw_action = action_entry.metadata.get("action_identity") if action_entry is not None else None
    action_artifact = action_entry.metadata.get("action_artifact") if action_entry is not None else None
    if action_entry is None or not isinstance(raw_action, Mapping) or not isinstance(action_artifact, Mapping):
        return None
    action = ActionArtifactV2(
        _frozen_action(raw_action),
        candidate.content_hash,
        str(selected_action.get("sizing_ref")),
        candidate_set.content_hash,
        action_entry.available_at_ns,
    )
    if canonical_json(action.to_dict()) != canonical_json(action_artifact):
        return None
    from atlas.v2.runtime.production import _recover_economic_evaluation

    return _recover_economic_evaluation(repo, action, candidate, candidate_set, now_ns=now_ns)
