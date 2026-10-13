"""R4 eligibility check over the existing R3A production composition fixture."""

from __future__ import annotations

from atlas.v2._serialization import FrozenMap
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.research.agent import ProviderCapabilitiesV1, ResearchBudgetV1, ResearchJobV1
from atlas.v2.research.decision_brief import DecisionBriefV1
from atlas.v2.research.gemma_native import GEMMA_MODEL_ID
from atlas.v2.research.memory import ResearchMemoryV1

from .test_session016_candidate_selection import CUTOFF
from .test_session053_production_completion import _a0_snapshot, _production_projection


def test_actual_production_brief_gate_preserves_a0_and_refuses_blocked_gemma(tmp_path):
    body, feature, production_status = _production_projection(tmp_path, projection_budget_ns=100_000_000)
    if production_status == "DEFERRED":
        # The unchanged 80 ms projection budget may safely defer this synthetic
        # receipt; without a persisted Brief there is no Gemma dispatch input.
        with OpsRepository(tmp_path / "ops.sqlite") as repository:
            assert repository.artifact_entries("ProductionDecisionBriefProjectionV1") == ()
        return
    assert production_status == "BLOCKED"
    assert any(item["section"] == "technical" for item in body["facts"])
    assert feature.content_hash in {item["source_ref"] for item in body["facts"]}
    database = tmp_path / "ops.sqlite"
    with OpsRepository(database) as repository:
        assert repository is not None
        (receipt_identity,) = repository.artifact_entries("OpsSupervisorReceiptIdentityV1")
        receipt_ref = receipt_identity.metadata["receipt_ref"]
        receipt_entry = repository.get_artifact(receipt_ref)
        assert receipt_entry is not None
        available_now_ns = receipt_entry.available_at_ns + 1
        identity = repository.get_artifact(ResearchMemoryV1.production_projection_identity(receipt_ref))
        assert identity is not None
        (brief_entry,) = repository.artifact_entries("ProductionDecisionBriefProjectionV1")
        brief_value = brief_entry.metadata["brief"]
        brief = DecisionBriefV1(brief_value["status"], tuple(brief_value["reasons"]), FrozenMap(brief_value["body"]))
        capabilities = ProviderCapabilitiesV1(
            "GEMINI_UNVERIFIED_R4", "gemini", GEMMA_MODEL_ID, False, False, False, False,
            False, False, None, False, "UNVERIFIED", "UNVERIFIED", None,
        )
        budget = ResearchBudgetV1("R4_GATE", 1, 0, 0, 0, 4096)
        memory = ResearchMemoryV1(repository)
        unavailable_job = memory.build_production_gemma_job(
                receipt_ref, capabilities=capabilities, budget=budget, now_ns=available_now_ns,
        )
        assert unavailable_job is None
        job = ResearchJobV1(
            "r4-production-gate", brief, capabilities, budget, CUTOFF + 1,
        )
        before = _a0_snapshot(repository)
        result = memory.prepare_gemma_shadow_dispatch(
                job, receipt_ref=receipt_ref, now_ns=available_now_ns,
        )
        after = _a0_snapshot(repository)
        assert result["status"] == "LLM_NOT_CALLED"
        assert result["reason"] == "PRODUCTION_BRIEF_NOT_COMPLETE"
        assert before == after
        assert repository.artifact_entries("ProductionGemmaShadowIntentV1") == ()
