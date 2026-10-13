"""Production lifecycle integration for the zero-authority intelligence projection."""

from dataclasses import replace

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.desktop.projection import project_snapshot
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.research.memory import RESEARCH_LANE, ResearchMemoryV1
from atlas.v2.runtime import production, production_intelligence
from atlas.v2.runtime.ops_supervisor import OpsSupervisorV2

from .test_session016_candidate_selection import CUTOFF
from .test_session027_ops_supervisor import FakeClock
from .test_session027_production import (
    ReconciledFixturePublicSource,
    StaticInputsProvider,
    _production_event,
)


def test_normal_production_run_persists_brief_and_memory_without_dispatch(tmp_path):
    path = tmp_path / "ops.sqlite"
    clock = FakeClock(CUTOFF + 100)

    with production.OpsRepository(path) as repository:
        event, inputs, _case, expected_set, selected, *_ = _production_event(repository)
    port = production.ProductionOpsCyclePortV1(
        public_source=ReconciledFixturePublicSource(event),
        inputs_provider=StaticInputsProvider(event.event_id, inputs),
        clock_ns=clock,
    )
    with OpsSupervisorV2(path, port, clock_ns=clock) as supervisor:
        first = supervisor.run_once()
        assert supervisor.repository is not None
        (receipt,) = first.event_receipts
        assert receipt.candidate_set_ref == expected_set.content_hash
        assert receipt.result.stages[5].artifact_refs
        assert receipt.result.stages[6].artifact_refs
        assert receipt.action_ref is not None
        assert receipt.evaluation_ref is not None
        assert receipt.calendar_refs
        assert receipt.agent_mode == "DISABLED"
        assert not receipt.capital_enabled and not receipt.assisted_enabled

        briefs = supervisor.repository.artifact_entries("ProductionDecisionBriefProjectionV1")
        memories = supervisor.repository.artifact_entries("ProductionIntelligenceMemoryProjectionV1")
        assert len(briefs) == len(memories) == 1
        brief_record = briefs[0].metadata
        assert brief_record["receipt_ref"]
        assert brief_record["brief"]["body"]["candidate_set_ref"] == expected_set.content_hash
        assert brief_record["brief"]["body"]["a0_selected_candidate_id"] == selected.candidate_id
        assert brief_record["status"] == "BLOCKED"
        assert "PRODUCTION_TYPED_SECTIONS_NOT_RECONSTRUCTED" in brief_record["brief"]["reasons"]
        projection = brief_record["brief"]["body"]
        assert projection["source_cutoff_ns"] == receipt.event.information_cutoff_ns
        assert projection["computed_at_ns"] == receipt.created_at_ns
        assert projection["hard_deadline_ns"] == receipt.event.deadline_ns
        assert projection["full_screening_denominator"]["candidate_rows"] == len(expected_set.candidates)
        assert projection["full_screening_denominator"]["scope"] == "ONE_OPS_DECISION_EVENT"
        assert projection["full_screening_denominator"]["whole_schedule_denominator_status"] == "UNAVAILABLE"
        assert set(receipt.calendar_refs).issubset(set(projection["immutable_source_refs"]))
        assert receipt.evaluation_ref in projection["immutable_source_refs"]
        assert projection["economic_qualification"]["status"] == "NOT_ESTIMABLE"
        assert projection["diagnostic_qualification"]["m1"]["qualification"].startswith("NOT_ESTIMABLE")
        assert projection["diagnostic_qualification"]["analogue"]["qualification"].startswith("NOT_ESTIMABLE")
        assert projection["diagnostic_qualification"]["ml_forecast_qualification"] == "UNQUALIFIED"
        assert tuple(projection["image_refs"]) == ()
        assert projection["projection_bounds"]["max_image_refs"] == 0
        assert projection["projection_bounds"]["token_projection_method"] == "UTF8_BYTE_UPPER_BOUND_NOT_TOKENIZER"
        assert projection["memory_status"]["prior_analysis"] == "NOT_REUSED_NO_EXACT_ANALYSIS_MATCH"
        assert projection["memory_status"]["due_work_status"] == "NONE"
        assert projection["candidate_action_qualification"]["status"] == "QUALIFIED_PERSISTED_A0_EXACT_ACTION"
        assert projection["candidate_action_qualification"]["action_hash"] == receipt.result.stages[6].bound_action_hash
        assert all(item["index_status"] == "INDEXED" for item in projection["source_availability"])
        assert brief_record["authority"] == "ZERO"
        assert brief_record["brief"]["body"]["economic_value"] == "NOT_ESTIMABLE"
        assert memories[0].metadata["brief_ref"] == briefs[0].artifact_ref
        assert memories[0].metadata["authority"] == "ZERO"

        assert supervisor.repository.artifact_entries("AnalysisResearchRequestV1") == ()
        assert supervisor.repository.artifact_entries("AnalysisResearchFakeIntentV1") == ()
        assert supervisor.repository.due_work_items(RESEARCH_LANE, as_of_ns=clock.now_ns, limit=64) == ()
        desktop = project_snapshot(supervisor.repository, now_ns=clock.now_ns)
        statuses = {row.name: row for row in desktop.overview.statuses}
        assert statuses["decision_brief"].state == "BLOCKED"
        assert statuses["decision_brief"].evidence_ref == briefs[0].artifact_ref
        assert statuses["economic_qualification"].state == "NOT_ESTIMABLE"
        assert statuses["research_agent"].state == "DISABLED"
        assert statuses["gemma_shadow"].state == "NOT_RUN"
        assert statuses["demo_only"].state == "UNAVAILABLE"
        assert statuses["capital_state"].state == "BLOCKED"
        assert statuses["due_work"].state == "NONE"

        second = supervisor.run_once()
        assert len(second.event_receipts) == 1
        assert len(supervisor.repository.artifact_entries("ProductionDecisionBriefProjectionV1")) == 1
        assert len(supervisor.repository.artifact_entries("ProductionIntelligenceMemoryProjectionV1")) == 1

    with production.OpsRepository(path) as restarted_repository:
        identity = restarted_repository.get_artifact(OpsSupervisorV2._receipt_identity_ref(event.event_id))
        receipt = OpsSupervisorV2._read_final_receipt(restarted_repository, event.event_id)
        assert identity is not None and receipt is not None
        replay_refs = production_intelligence.project_production_receipt(
            restarted_repository,
            receipt,
            identity.metadata["receipt_ref"],
            now_ns=clock.now_ns,
        )
        assert replay_refs == (briefs[0].artifact_ref, memories[0].artifact_ref)
        assert len(restarted_repository.artifact_entries("ProductionDecisionBriefProjectionV1")) == 1
        assert len(restarted_repository.artifact_entries("ProductionIntelligenceMemoryProjectionV1")) == 1


def test_availability_header_batch_matches_full_index_without_decoding_body(tmp_path):
    reference = sha256_json("availability-header-fixture")
    with OpsRepository(tmp_path / "headers.sqlite") as repository:
        repository.register_artifact(ArtifactIndexEntryV2(
            reference,
            "AvailabilityHeaderFixtureV1",
            reference,
            5,
            10,
            {"fixture": "typed-body"},
        ))
        full = repository.get_artifact_metadata_by_refs((reference,))[reference]
        header = repository.get_artifact_availability_by_refs((reference,))[reference]
        absent = sha256_json("availability-header-missing")
        assert repository.get_artifact_availability_by_refs((absent,)) == {}

    assert header == {
        "artifact_type": full["artifact_type"],
        "content_hash": full["content_hash"],
        "created_at_ns": full["created_at_ns"],
        "available_at_ns": full["available_at_ns"],
        "effective_available_at_ns": full["effective_available_at_ns"],
    }


def _a0_snapshot(repository):
    kinds = (
        "OpsSupervisorReceiptV1",
        "CandidateSetV2",
        "SizingDecisionV2",
        "ActionArtifactV2",
        "EvaluationArtifactV2",
        "DecisionCalendarEntryV2",
    )
    return {
        kind: tuple(sorted((entry.artifact_ref, entry.content_hash) for entry in repository.artifact_entries(kind)))
        for kind in kinds
    }


def test_projection_failure_preserves_a0_hashes_and_can_be_disabled(tmp_path, monkeypatch):
    path = tmp_path / "failure.sqlite"
    clock = FakeClock(CUTOFF + 100)
    with production.OpsRepository(path) as repository:
        event, inputs, *_ = _production_event(repository)

    def make_port():
        return production.ProductionOpsCyclePortV1(
            public_source=ReconciledFixturePublicSource(event),
            inputs_provider=StaticInputsProvider(event.event_id, inputs),
            clock_ns=clock,
        )

    with OpsSupervisorV2(path, make_port(), clock_ns=clock, production_intelligence_enabled=False) as disabled:
        assert disabled.post_receipt_projection is None

    observed = {}

    def fail_projection(repository, *_args, **_kwargs):
        observed["before"] = _a0_snapshot(repository)
        raise OSError("injected read-only projection failure")

    monkeypatch.setattr(production_intelligence, "project_production_receipt", fail_projection)
    with OpsSupervisorV2(path, make_port(), clock_ns=clock) as failed_projection:
        result = failed_projection.run_once()
        assert len(result.event_receipts) == 1
        assert observed["before"] == _a0_snapshot(failed_projection.repository)
        assert failed_projection.repository.artifact_entries("ProductionDecisionBriefProjectionV1") == ()
        assert failed_projection.repository.artifact_entries("ProductionIntelligenceMemoryProjectionV1") == ()
        assert failed_projection.repository.artifact_entries("TradePlanEnvelopeV2") == ()
        assert failed_projection.repository.artifact_entries("OrderIntentV1") == ()


def test_deadline_reached_during_projection_expires_without_changing_receipt(tmp_path):
    path = tmp_path / "deadline.sqlite"
    clock = FakeClock(CUTOFF + 100)
    with production.OpsRepository(path) as repository:
        event, inputs, *_ = _production_event(repository)
    port = production.ProductionOpsCyclePortV1(
        public_source=ReconciledFixturePublicSource(event),
        inputs_provider=StaticInputsProvider(event.event_id, inputs),
        clock_ns=clock,
    )
    with OpsSupervisorV2(path, port, clock_ns=clock, production_intelligence_enabled=False) as supervisor:
        (receipt,) = supervisor.run_once().event_receipts
        assert supervisor.repository is not None
        before = _a0_snapshot(supervisor.repository)
        identity = supervisor.repository.get_artifact(OpsSupervisorV2._receipt_identity_ref(event.event_id))
        assert identity is not None
        production_intelligence.project_production_receipt(
            supervisor.repository,
            receipt,
            identity.metadata["receipt_ref"],
            now_ns=receipt.created_at_ns,
            clock_ns=lambda: receipt.event.deadline_ns,
        )
        (brief,) = supervisor.repository.artifact_entries("ProductionDecisionBriefProjectionV1")
        assert brief.metadata["status"] == "EXPIRED"
        assert tuple(brief.metadata["brief"]["reasons"]) == ("HARD_DEADLINE_REACHED_DURING_PROJECTION",)
        assert brief.metadata["brief"]["body"]["computed_at_ns"] == receipt.event.deadline_ns
        assert brief.metadata["brief"]["body"]["economic_qualification"]["status"] == "NOT_ESTIMABLE"
        assert _a0_snapshot(supervisor.repository) == before


@pytest.mark.parametrize("case", ("no_candidate", "unknown_economics", "risk_unavailable"))
def test_production_projection_preserves_negative_and_unknown_cases(tmp_path, case):
    path = tmp_path / f"{case}.sqlite"
    clock = FakeClock(CUTOFF + 100)
    with production.OpsRepository(path) as repository:
        if case == "no_candidate":
            event, inputs, *_ = _production_event(repository)
            inputs = replace(
                inputs,
                candidates=(),
                scanner_evidence_refs={},
                risk_inputs={},
                economic_inputs={},
                causal_feature_refs=(),
            )
            event = replace(
                event,
                event_id=sha256_json({"base_event_id": event.event_id, "fixture_case": "NO_CANDIDATE"}),
                causal_input_refs=(inputs.universe.content_hash,),
            )
        elif case == "unknown_economics":
            event, inputs, *_ = _production_event(repository, no_economic_inputs=True)
        else:
            event, inputs, *_ = _production_event(repository, future_account=True)
    port = production.ProductionOpsCyclePortV1(
        public_source=ReconciledFixturePublicSource(event),
        inputs_provider=StaticInputsProvider(event.event_id, inputs),
        clock_ns=clock,
    )
    with OpsSupervisorV2(path, port, clock_ns=clock) as supervisor:
        result = supervisor.run_once()
        assert result.event_receipts, repr(result)
        (receipt,) = result.event_receipts
        assert supervisor.repository is not None
        (brief,) = supervisor.repository.artifact_entries("ProductionDecisionBriefProjectionV1")
        body = brief.metadata["brief"]["body"]
        assert brief.metadata["status"] == "BLOCKED"
        assert body["economic_qualification"]["status"] == "NOT_ESTIMABLE"
        assert body["economic_value"] == "NOT_ESTIMABLE"
        assert body["full_screening_denominator"]["decision_calendar_refs"] == receipt.calendar_refs
        if case == "no_candidate":
            assert body["a0_selected_candidate_id"] is None
            assert body["full_screening_denominator"]["candidate_rows"] == 0
            assert receipt.action_ref is None
            assert body["candidate_action_qualification"]["status"] == "UNAVAILABLE"
        elif case == "unknown_economics":
            assert body["a0_selected_candidate_id"] is not None
            assert receipt.action_ref is not None
            assert receipt.evaluation_ref is None
            assert body["candidate_action_qualification"]["status"] == "QUALIFIED_PERSISTED_A0_EXACT_ACTION"
        else:
            assert body["a0_selected_candidate_id"] is not None
            assert receipt.action_ref is None
            assert receipt.evaluation_ref is None
            assert body["candidate_action_qualification"]["status"] == "UNAVAILABLE"
            assert receipt.result.terminal_status.value == "NOT_ESTIMABLE"


@pytest.mark.parametrize("bound", ("rows", "bytes", "tokens", "references"))
def test_production_projection_bounds_block_without_truncating_a0(tmp_path, monkeypatch, bound):
    path = tmp_path / f"bounds-{bound}.sqlite"
    clock = FakeClock(CUTOFF + 100)
    with production.OpsRepository(path) as repository:
        event, inputs, *_ = _production_event(repository)
    if bound == "rows":
        assert len(inputs.candidates) > 1
        limits = replace(production_intelligence.BRIEF_LIMITS, max_rows=len(inputs.candidates) - 1)
        monkeypatch.setattr(production_intelligence, "BRIEF_LIMITS", limits)
    elif bound == "bytes":
        limits = replace(
            production_intelligence.BRIEF_LIMITS,
            max_bytes=256,
        )
        monkeypatch.setattr(production_intelligence, "BRIEF_LIMITS", limits)
        monkeypatch.setattr(production_intelligence, "MAX_PROJECTION_BYTES", 256)
    elif bound == "tokens":
        limits = replace(production_intelligence.BRIEF_LIMITS, max_projected_tokens=256)
        monkeypatch.setattr(production_intelligence, "BRIEF_LIMITS", limits)
    else:
        monkeypatch.setattr(production_intelligence, "MAX_PROJECTION_REFS", 1)
    port = production.ProductionOpsCyclePortV1(
        public_source=ReconciledFixturePublicSource(event),
        inputs_provider=StaticInputsProvider(event.event_id, inputs),
        clock_ns=clock,
    )
    with OpsSupervisorV2(path, port, clock_ns=clock) as supervisor:
        result = supervisor.run_once()
        assert len(result.event_receipts) == 1
        assert supervisor.repository is not None
        (brief,) = supervisor.repository.artifact_entries("ProductionDecisionBriefProjectionV1")
        assert brief.metadata["status"] == "BLOCKED"
        reasons = tuple(brief.metadata["brief"]["reasons"])
        expected = {
            "rows": "ROW_LIMIT",
            "bytes": "BYTE_LIMIT",
            "tokens": "TOKEN_PROJECTION_LIMIT",
            "references": "PROJECTION_REFERENCE_LIMIT",
        }[bound]
        assert expected in reasons
        assert (
            not brief.metadata["brief"]["body"].get("economic_value")
            or brief.metadata["brief"]["body"]["economic_value"] == "NOT_ESTIMABLE"
        )
        assert supervisor.repository.artifact_entries("OrderIntentV1") == ()


def test_memory_projection_batch_rolls_back_before_commit_and_replays_after_restart(tmp_path):
    path = tmp_path / "memory.sqlite"
    source_body = {"source": "immutable fixture", "authority": "ZERO"}
    source_ref = sha256_json(source_body)
    receipt_ref = sha256_json({"receipt": "immutable fixture"})
    with OpsRepository(path) as repository:
        repository.register_artifact(
            ArtifactIndexEntryV2(source_ref, "FixtureSourceV1", source_ref, 100, 100, source_body)
        )
        memory = ResearchMemoryV1(repository)
        original_persist = memory._persist

        def fail_before_memory(ref, kind, body, *, now_ns):
            if kind == "ProductionIntelligenceMemoryProjectionV1":
                raise OSError("simulated crash before transaction commit")
            original_persist(ref, kind, body, now_ns=now_ns)

        memory._persist = fail_before_memory
        with pytest.raises(OSError):
            memory.persist_production_projection(
                receipt_ref=receipt_ref,
                brief={"status": "BLOCKED"},
                brief_status="BLOCKED",
                source_refs=(source_ref,),
                source_population_ref=source_ref,
                now_ns=200,
            )
        assert repository.artifact_entries("ProductionDecisionBriefProjectionV1") == ()
        assert repository.artifact_entries("ProductionIntelligenceMemoryProjectionV1") == ()
        assert repository.artifact_entries("ProductionIntelligenceMemoryIdentityV1") == ()

        memory._persist = original_persist
        first_refs = memory.persist_production_projection(
            receipt_ref=receipt_ref,
            brief={"status": "BLOCKED"},
            brief_status="BLOCKED",
            source_refs=(source_ref,),
            source_population_ref=source_ref,
            now_ns=200,
        )
        assert len(repository.artifact_entries("ProductionDecisionBriefProjectionV1")) == 1
        assert len(repository.artifact_entries("ProductionIntelligenceMemoryProjectionV1")) == 1

    with OpsRepository(path) as restarted_repository:
        restarted_memory = ResearchMemoryV1(restarted_repository)
        replay_refs = restarted_memory.persist_production_projection(
            receipt_ref=receipt_ref,
            brief={"status": "EXPIRED"},
            brief_status="EXPIRED",
            source_refs=(source_ref,),
            source_population_ref=source_ref,
            now_ns=300,
        )
        assert replay_refs == first_refs
        assert len(restarted_repository.artifact_entries("ProductionDecisionBriefProjectionV1")) == 1
        assert len(restarted_repository.artifact_entries("ProductionIntelligenceMemoryProjectionV1")) == 1
