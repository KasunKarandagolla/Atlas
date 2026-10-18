"""Real existing P1 evaluator and Brief output instrumentation, synthetic only."""

from dataclasses import replace

import pytest
from v2.test_session047_decision_brief import request

from atlas.v2._serialization import canonical_json
from atlas.v2.research.decision_brief import build_decision_brief
from atlas.v2.runtime.native_research_trace import persist_native_output
from atlas.v2.science.evaluation_service import run_mean_action_evaluation


def test_actual_p1_full_population_survives_trace_with_existing_native_risk_and_economics(case):
    result = run_mean_action_evaluation(
        case.repo,
        population=case.population,
        snapshot=case.snapshot,
        policy=case.policy,
        inputs=case.inputs,
        clock_ns=case.clock,
    )
    ref = persist_native_output(
        case.repo,
        result,
        source_ref=case.population.content_hash,
        slot_ns=case.population.slot_ns,
        evidence_grade="SYNTHETIC_OFFLINE",
    )
    entry = case.repo.get_artifact(ref)
    assert canonical_json(entry.metadata["trace"]["native_result"]) == canonical_json(result.to_dict())
    assert len(result.work) == 4 and len(result.evaluations) == 3 and len(result.members) == 2
    assert entry.metadata["trace"]["evidence_grade"] == "SYNTHETIC_OFFLINE"
    assert entry.available_at_ns > case.population.deadline_ns
    assert not case.repo.artifact_entries("Order")
    assert not case.repo.artifact_entries("TradePlanEnvelopeV2")


def test_native_p1_forged_output_not_in_owning_index_refused(case):
    result = run_mean_action_evaluation(
        case.repo,
        population=case.population,
        snapshot=case.snapshot,
        policy=case.policy,
        inputs=case.inputs,
        clock_ns=case.clock,
    )
    forged = replace(result, cpu_used_ns=result.cpu_used_ns + 1)
    with pytest.raises(ValueError):
        persist_native_output(
            case.repo,
            forged,
            source_ref=case.population.content_hash,
            slot_ns=case.population.slot_ns,
            evidence_grade="SYNTHETIC_OFFLINE",
        )


def test_native_brief_block_and_reason_survive_trace_without_agent_fallback(case):
    r = request()
    r = replace(r, built_at_ns=r.deadline_ns)
    result = build_decision_brief(r, repository=case.repo)
    assert result.status == "EXPIRED" and result.reasons == ("DEADLINE_EXPIRED",)
    ref = persist_native_output(
        case.repo,
        result,
        source_ref=case.population.content_hash,
        slot_ns=case.population.slot_ns,
        evidence_grade="SYNTHETIC_OFFLINE",
    )
    assert canonical_json(case.repo.get_artifact(ref).metadata["trace"]["native_result"]) == canonical_json(
        result.to_dict()
    )
    assert not case.repo.artifact_entries("AgentDirectiveV1")
