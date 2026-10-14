"""SYNTHETIC_INTEGRATION: R9 production fixture fails closed at typed-Brief gate."""

from __future__ import annotations

from atlas.v2._serialization import sha256_json
from atlas.v2.desktop.projection import project_snapshot
from atlas.v2.research.agent import ProviderCapabilitiesV1
from atlas.v2.research.gemma_native import GEMMA_MODEL_ID
from atlas.v2.research.memory import RESEARCH_LANE, ResearchMemoryV1
from atlas.v2.runtime import production, production_intelligence
from atlas.v2.runtime.ops_supervisor import OpsSupervisorV2

from .test_session016_candidate_selection import CUTOFF
from .test_session027_ops_supervisor import FakeClock
from .test_session027_production import ReconciledFixturePublicSource, StaticInputsProvider, _production_event
from .test_session055_gemma_native import research_job


def test_r9_connected_production_fixture_fails_closed_before_agent_and_demo(tmp_path):
    """Run the existing market-to-A0-to-Brief/memory composition and restart it.

    The fixture has a persisted exact action, but the production brief compiler
    cannot reconstruct every typed S47 section. The expected terminal result is
    a durable BLOCKED brief with NOT_ESTIMABLE economics, no Gemma job, no Demo
    intent, and a stable replay after restart.
    """
    database = tmp_path / "r9-connected-fixture.sqlite"
    clock = FakeClock(CUTOFF + 100)
    with production.OpsRepository(database) as repository:
        event, inputs, _case, expected_set, selected, *_ = _production_event(repository)

    port = production.ProductionOpsCyclePortV1(
        public_source=ReconciledFixturePublicSource(event),
        inputs_provider=StaticInputsProvider(event.event_id, inputs),
        clock_ns=clock,
    )
    elapsed_ticks = 0

    def deterministic_projection_clock() -> int:
        nonlocal elapsed_ticks
        elapsed_ticks += 1
        return elapsed_ticks

    with OpsSupervisorV2(
        database, port, clock_ns=clock, monotonic_ns=deterministic_projection_clock,
    ) as supervisor:
        result = supervisor.run_once()
        assert len(result.event_receipts) == 1
        (receipt,) = result.event_receipts
        assert receipt.candidate_set_ref == expected_set.content_hash
        assert receipt.action_ref is not None
        assert receipt.evaluation_ref is not None
        assert receipt.agent_mode == "DISABLED"
        assert not receipt.capital_enabled and not receipt.assisted_enabled
        assert supervisor.repository is not None

        identity = supervisor.repository.get_artifact(OpsSupervisorV2._receipt_identity_ref(event.event_id))
        assert identity is not None
        receipt_ref = identity.metadata["receipt_ref"]
        (brief_entry,) = supervisor.repository.artifact_entries("ProductionDecisionBriefProjectionV1")
        (memory_entry,) = supervisor.repository.artifact_entries("ProductionIntelligenceMemoryProjectionV1")
        assert brief_entry.metadata["status"] == "BLOCKED"
        brief = brief_entry.metadata["brief"]
        body = brief["body"]
        assert "PRODUCTION_TYPED_SECTIONS_NOT_RECONSTRUCTED" in brief["reasons"]
        assert body["economic_qualification"]["status"] == "NOT_ESTIMABLE"
        assert body["economic_value"] == "NOT_ESTIMABLE"
        assert memory_entry.metadata["brief_ref"] == brief_entry.artifact_ref
        assert memory_entry.metadata["authority"] == "ZERO"

        capabilities = ProviderCapabilitiesV1(
            "R9_FAKE_GEMMA_FIXTURE_V1", "gemini", GEMMA_MODEL_ID, True, False, True, True,
            True, True, 4000, False, "FIXTURE_ONLY", "NOT_PROBED", sha256_json("R9 fixture capability"),
        )
        template_job = research_job(capabilities=capabilities)
        memory = ResearchMemoryV1(supervisor.repository)
        assert memory.build_production_gemma_job(
            receipt_ref, capabilities=capabilities, budget=template_job.budget, now_ns=clock.now_ns,
        ) is None
        assert supervisor.repository.artifact_entries("ProductionGemmaShadowIntentV1") == ()
        assert supervisor.repository.artifact_entries("ProductionGemmaShadowResultV1") == ()
        assert supervisor.repository.artifact_entries("OrderIntentV1") == ()
        assert supervisor.repository.due_work_items(RESEARCH_LANE, as_of_ns=clock.now_ns, limit=64) == ()

        statuses = {item.name: item for item in project_snapshot(supervisor.repository, now_ns=clock.now_ns).overview.statuses}
        assert statuses["decision_brief"].state == "BLOCKED"
        assert statuses["economic_qualification"].state == "NOT_ESTIMABLE"
        assert statuses["gemma_shadow"].state == "NOT_RUN"
        assert statuses["demo_only"].state == "UNAVAILABLE"

    with production.OpsRepository(database) as restarted:
        identity = restarted.get_artifact(OpsSupervisorV2._receipt_identity_ref(event.event_id))
        replayed_receipt = OpsSupervisorV2._read_final_receipt(restarted, event.event_id)
        assert identity is not None and replayed_receipt is not None
        receipt_ref = identity.metadata["receipt_ref"]
        replay_refs = production_intelligence.project_production_receipt(
            restarted, replayed_receipt, receipt_ref, now_ns=clock.now_ns,
        )
        assert replay_refs == (brief_entry.artifact_ref, memory_entry.artifact_ref)
        restarted_memory = ResearchMemoryV1(restarted)
        assert restarted_memory.build_production_gemma_job(
            receipt_ref, capabilities=capabilities, budget=template_job.budget, now_ns=clock.now_ns,
        ) is None
        assert restarted.artifact_entries("ProductionGemmaShadowIntentV1") == ()
        assert restarted.artifact_entries("OrderIntentV1") == ()
