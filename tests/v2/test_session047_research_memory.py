"""Durable fake dispatch, due work and exact analysis reuse through the ops writer."""

from dataclasses import replace

import pytest

from atlas.v2._serialization import FrozenMap, sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.research.agent import FakeResearchProvider
from atlas.v2.research.memory import RESEARCH_LANE, ResearchMemoryV1

from .test_session047_research_agent import job, output


def seed(repo, at=10):
    body = {"event": "BAR_CLOSED", "available_at_ns": at}
    ref = sha256_json(body)
    repo.register_artifact(ArtifactIndexEntryV2(ref, "ResearchEventFixtureV1", ref, at, at, body))
    return ref


def test_duplicate_triggers_restart_and_no_duplicate_dispatch(tmp_path):
    path = tmp_path / "ops.sqlite"
    j = job()
    with OpsRepository(path) as repo:
        memory = ResearchMemoryV1(repo)
        event = seed(repo)
        memory.schedule(j, event_ref=event, now_ns=11)
        memory.schedule(j, event_ref=event, now_ns=11)
        assert len(repo.due_work_items(RESEARCH_LANE, as_of_ns=11)) == 1
        fake = FakeResearchProvider([output()])
        result = memory.run_fake(j, fake, clock_ns=lambda: 20)
        assert result["status"] == "VALID_SHADOW"
        assert fake.calls == 1
    with OpsRepository(path) as repo:
        memory = ResearchMemoryV1(repo)
        fake = FakeResearchProvider([output()])
        assert memory.run_fake(j, fake, clock_ns=lambda: 21) == result
        assert fake.calls == 0
        assert repo.due_work_items(RESEARCH_LANE, as_of_ns=21) == ()


def test_crash_after_intent_is_unknown_and_never_retried(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        memory = ResearchMemoryV1(repo)
        j = job()
        memory.schedule(j, event_ref=seed(repo), now_ns=11)

        def crash(phase):
            if phase == "AFTER_INTENT":
                raise KeyboardInterrupt("crash fixture")

        with pytest.raises(KeyboardInterrupt):
            memory.run_fake(j, FakeResearchProvider([output()]), clock_ns=lambda: 20, fault=crash)
        fake = FakeResearchProvider([output()])
        assert memory.run_fake(j, fake, clock_ns=lambda: 21)["status"] == "UNKNOWN"
        assert fake.calls == 0
        assert memory.run_fake(j, fake, clock_ns=lambda: 22)["status"] == "UNKNOWN"


def test_atomic_trigger_rollback_and_future_source(tmp_path, monkeypatch):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        memory = ResearchMemoryV1(repo)
        j = job()
        event = seed(repo)
        original = repo.enqueue_due_work

        def fail(**kwargs):
            raise RuntimeError("crash before persistence")

        with monkeypatch.context() as patch:
            patch.setattr(repo, "enqueue_due_work", fail)
            with pytest.raises(RuntimeError):
                memory.schedule(j, event_ref=event, now_ns=11)
        assert memory.request(j.job_id) is None
        monkeypatch.setattr(repo, "enqueue_due_work", original)
        memory.schedule(j, event_ref=event, now_ns=11)
        other = replace(j, job_id="future")
        denied = memory.schedule(other, event_ref=seed(repo, at=12), now_ns=13)
        assert denied["status"] == "LLM_NOT_CALLED"
        assert denied["reason"] == "SOURCE_UNAVAILABLE_AT_CUTOFF"


def test_memo_exact_identity_invalidation_and_changed_snapshot(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        memory = ResearchMemoryV1(repo)
        j = job()
        event = seed(repo)
        memory.schedule(j, event_ref=event, now_ns=11)
        memory.run_fake(j, FakeResearchProvider([output()]), clock_ns=lambda: 20)
        next_job = replace(j, job_id="next")
        memory.schedule(next_job, event_ref=event, now_ns=21)
        fake = FakeResearchProvider([output()])
        result = memory.run_fake(next_job, fake, clock_ns=lambda: 22)
        assert result["status"] == "MEMO_REUSED"
        assert fake.calls == 0
        memory.invalidate(j, source_ref=event, cause="SOURCE_GAP", now_ns=23)
        assert memory.memo(j, now_ns=24) is None
        changed = replace(
            j, brief=replace(j.brief, body=FrozenMap(j.brief.body.to_dict() | {"snapshot_ref": sha256_json("new")}))
        )
        assert memory.memo(changed, now_ns=24) is None


@pytest.mark.parametrize(
    "phase,expected,calls",
    [
        ("BEFORE_INTENT", "VALID_SHADOW", 1),
        ("AFTER_RESPONSE", "UNKNOWN", 0),
        ("BEFORE_RESULT_COMMIT", "UNKNOWN", 0),
        ("AFTER_RESULT_COMMIT", "VALID_SHADOW", 0),
    ],
)
def test_crash_boundaries_do_not_duplicate_effect(tmp_path, phase, expected, calls):
    path = tmp_path / "ops.sqlite"
    j = job()
    with OpsRepository(path) as repo:
        memory = ResearchMemoryV1(repo)
        memory.schedule(j, event_ref=seed(repo), now_ns=11)

        def crash(at):
            if at == phase:
                raise KeyboardInterrupt(phase)

        with pytest.raises(KeyboardInterrupt):
            memory.run_fake(j, FakeResearchProvider([output()]), clock_ns=lambda: 20, fault=crash)
    with OpsRepository(path) as repo:
        fake = FakeResearchProvider([output()])
        result = ResearchMemoryV1(repo).run_fake(j, fake, clock_ns=lambda: 21)
        assert result["status"] == expected
        assert fake.calls == calls


def test_unknown_failure_expiry_budget_and_distinct_origins_are_durable(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        memory = ResearchMemoryV1(repo)
        j = job()
        event = seed(repo)
        memory.schedule(j, event_ref=event, now_ns=11)
        assert (
            memory.run_fake(j, FakeResearchProvider([RuntimeError("ambiguous")]), clock_ns=lambda: 20)["status"]
            == "UNKNOWN"
        )
        fake = FakeResearchProvider([output()])
        assert memory.run_fake(j, fake, clock_ns=lambda: 21)["status"] == "UNKNOWN"
        assert fake.calls == 0
        denied = replace(j, job_id="denied", budget=replace(j.budget, max_model_calls=0))
        memory.schedule(denied, event_ref=event, now_ns=22)
        assert memory.run_fake(denied, fake, clock_ns=lambda: 23)["status"] == "BUDGET_DENIED"
        expired = replace(j, job_id="expired")
        assert memory.schedule(expired, event_ref=event, now_ns=101)["status"] == "EXPIRED"
        assert memory.run_fake(expired, fake, clock_ns=lambda: 102)["status"] == "EXPIRED"
        assert fake.calls == 0
        assert len(repo.artifact_entries("AnalysisResearchTriggerV1")) == 3
        assert all(
            row.metadata["calendar_denominator"] == 1 for row in repo.artifact_entries("AnalysisResearchTerminalV1")
        )


def test_writer_thread_and_read_only_consumer_cannot_mutate(tmp_path):
    import threading

    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        memory = ResearchMemoryV1(repo)
        event = seed(repo)
        errors = []

        def wrong_writer():
            try:
                memory.schedule(job(), event_ref=event, now_ns=11)
            except RuntimeError as exc:
                errors.append(str(exc))

        thread = threading.Thread(target=wrong_writer)
        thread.start()
        thread.join()
        assert errors and "ops writer" in errors[0]
        assert memory.request(job().job_id) is None
    with OpsRepository(path, read_only=True) as repo, pytest.raises(RuntimeError, match="ops writer"):
        ResearchMemoryV1(repo).schedule(job(), event_ref=event, now_ns=11)


def test_late_response_is_audit_only_after_unknown_and_stale_memo_blocks_dispatch(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        memory = ResearchMemoryV1(repo)
        j = job()
        event = seed(repo)
        memory.schedule(j, event_ref=event, now_ns=11)
        unknown = memory.run_fake(j, FakeResearchProvider([RuntimeError("failure")]), clock_ns=lambda: 20)
        payload = {"job_id": j.job_id, "request_hash": j.request_hash, "decision": output()}
        ref = memory.record_late_result(j, payload, now_ns=21)
        assert ref == memory.record_late_result(j, payload, now_ns=22)
        fake = FakeResearchProvider([output()])
        assert memory.run_fake(j, fake, clock_ns=lambda: 23) == unknown
        assert fake.calls == 0
        new = replace(j, job_id="new")
        memory.schedule(new, event_ref=event, now_ns=24)
        memory.invalidate(new, source_ref=event, cause="SOURCE_GAP", now_ns=25)
        assert memory.run_fake(new, fake, clock_ns=lambda: 26)["reason"] == "SNAPSHOT_INVALIDATED"
        assert fake.calls == 0


def test_real_empty_brief_is_durable_no_call_and_preserves_original_calendar(tmp_path):
    from atlas.v2.research.agent import ResearchJobV1
    from atlas.v2.research.decision_brief import build_decision_brief

    from .test_session047_decision_brief import request

    r = request()
    original = r.candidate_set.to_canonical_json()
    brief = build_decision_brief(r)
    fixture_job = job()
    j = ResearchJobV1("real-empty-brief-fixture", brief, fixture_job.capabilities, fixture_job.budget, r.cutoff_ns + 1)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        memory = ResearchMemoryV1(repo)
        memory.schedule(j, event_ref=seed(repo, at=r.cutoff_ns), now_ns=r.cutoff_ns + 1)
        fake = FakeResearchProvider([output()])
        receipt = memory.run_fake(j, fake, clock_ns=lambda: r.cutoff_ns + 2)
        assert receipt["status"] == "LLM_NOT_CALLED"
        assert fake.calls == 0
        assert receipt["calendar_slot_id"] == r.candidate_set.decision_event_id
        assert receipt["calendar_denominator"] == 1
        assert r.candidate_set.to_canonical_json() == original


def test_future_persisted_terminal_is_not_replayed_as_available(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        memory = ResearchMemoryV1(repo)
        j = job()
        memory.schedule(j, event_ref=seed(repo), now_ns=11)
        memory.run_fake(j, FakeResearchProvider([output()]), clock_ns=lambda: 20)
        fake = FakeResearchProvider([output()])
        early = memory.run_fake(j, fake, clock_ns=lambda: 19)
        assert early["status"] == "UNKNOWN"
        assert early["reason"] == "PERSISTED_STATE_NOT_AVAILABLE"
        assert fake.calls == 0
        assert memory.run_fake(j, fake, clock_ns=lambda: 21)["status"] == "VALID_SHADOW"


def test_distinct_invalidators_keep_each_origin_without_refreshing_sentinel(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        memory = ResearchMemoryV1(repo)
        j = job()
        event = seed(repo)
        first = memory.invalidate(j, source_ref=event, cause="SOURCE_GAP", now_ns=20)
        before = repo.get_artifact(first)
        assert memory.invalidate(j, source_ref=event, cause="WATCH_INVALIDATED", now_ns=21) == first
        memory.invalidate(j, source_ref=event, cause="WATCH_INVALIDATED", now_ns=22)
        assert repo.get_artifact(first) == before
        origins = repo.artifact_entries("AnalysisResearchInvalidationEventV1")
        assert len(origins) == 2
        assert {row.metadata["cause"] for row in origins} == {"SOURCE_GAP", "WATCH_INVALIDATED"}


def test_uncertain_dispatch_cannot_be_retried_under_a_new_job_id(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        memory = ResearchMemoryV1(repo)
        j = job()
        event = seed(repo)
        memory.schedule(j, event_ref=event, now_ns=11)
        assert (
            memory.run_fake(j, FakeResearchProvider([RuntimeError("uncertain")]), clock_ns=lambda: 20)["status"]
            == "UNKNOWN"
        )
        alias = replace(j, job_id="alias")
        memory.schedule(alias, event_ref=event, now_ns=21)
        fake = FakeResearchProvider([output(job_id="alias")])
        assert memory.run_fake(alias, fake, clock_ns=lambda: 22)["status"] == "UNKNOWN"
        assert fake.calls == 0


@pytest.mark.parametrize("change", ["rebuilt_time", "budget"])
def test_scope_dedupe_survives_rebuild_time_and_budget_changes(tmp_path, change):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        memory = ResearchMemoryV1(repo)
        j = job()
        event = seed(repo)
        memory.schedule(j, event_ref=event, now_ns=11)
        memory.run_fake(j, FakeResearchProvider([RuntimeError("uncertain")]), clock_ns=lambda: 20)
        alias = replace(j, job_id="alias")
        if change == "rebuilt_time":
            alias = replace(alias, brief=replace(j.brief, body=j.brief.body.to_dict() | {"built_at_ns": 21}))
        else:
            alias = replace(alias, budget=replace(j.budget, max_tool_bytes=20000))
        memory.schedule(alias, event_ref=event, now_ns=21)
        fake = FakeResearchProvider([output(job_id="alias")])
        assert memory.run_fake(alias, fake, clock_ns=lambda: 22)["status"] == "UNKNOWN"
        assert fake.calls == 0


def test_scoped_memo_cannot_reuse_future_origin_terminal(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        memory = ResearchMemoryV1(repo)
        j = job()
        event = seed(repo)
        memory.schedule(j, event_ref=event, now_ns=11)
        alias = replace(j, job_id="alias")
        memory.schedule(alias, event_ref=event, now_ns=12)
        times = iter((11, 11, 11, 11, 20, 20))
        memory.run_fake(j, FakeResearchProvider([output()]), clock_ns=lambda: next(times))
        fake = FakeResearchProvider([output(job_id="alias")])
        assert memory.run_fake(alias, fake, clock_ns=lambda: 19)["status"] == "UNKNOWN"
        assert fake.calls == 0
