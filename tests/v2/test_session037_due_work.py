"""Active maturation uses indexed due work, never historical terminal sweeps."""


import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.contracts import WatchStateV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository, prediction_due_lane_v1
from atlas.v2.runtime import outcome_maturity as coordinator
from atlas.v2.runtime.research_prediction_outcomes import (
    MAINTENANCE_INTERVAL_NS_V1,
    ResearchPredictionOutcomeMaintenanceV1,
    index_research_prediction_outcome_v1,
)

from .test_memory import watch
from .test_session033_outcome_maturity import _calendar, _resolution, _supported_replay_fixture
from .test_session036_prediction_outcomes import _matured_fixture


def _cycle(repo, cutoff):
    return coordinator.run_outcome_maturity_cycle(repo, evidence_cutoff_ns=cutoff,
        production_clock_ns=lambda: cutoff, monotonic_ns=lambda: 0)


def test_atomic_new_origin_bypasses_bounded_migration_backlog(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        history = tuple(ArtifactIndexEntryV2(sha256_json({"history": i}), "HistoricalFixture",
            sha256_json({"history": i}), i, i, {}) for i in range(1000))
        repo.register_artifacts(history)
        decision = _calendar(repo, 1)
        discovery = repo.discover_due_work(limit=7)
        assert discovery == {"rows_inspected": 7, "last_rowid": 7, "has_more": True}
        due = repo.due_work_items("ACTION_OUTCOME", as_of_ns=decision.available_at_ns)
        assert [item.source_ref for item in due] == [decision.content_hash]
        assert repo.due_work_pressure("ACTION_OUTCOME", as_of_ns=decision.available_at_ns)["pending_count"] == 1


def test_terminal_history_is_retired_and_new_opportunity_is_immediately_due(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        for i in range(100):
            decision = _calendar(repo, i)
            repo.retire_due_work("ACTION_OUTCOME", decision.content_hash, reason_code="UNSUPPORTED")
        new = _calendar(repo, 101)
        repo.discover_due_work(limit=64)
        assert [item.source_ref for item in repo.due_work_items(
            "ACTION_OUTCOME", as_of_ns=new.available_at_ns, limit=1)] == [new.content_hash]
        assert repo.due_work_pressure("ACTION_OUTCOME", as_of_ns=new.available_at_ns)["retired_count"] == 100
    with OpsRepository(path) as repo:
        repo.discover_due_work(limit=64)
        assert repo.due_work_pressure("ACTION_OUTCOME", as_of_ns=new.available_at_ns)["pending_count"] == 1
        repo.register_artifact(repo.get_artifact(new.content_hash))
        assert repo.due_work_pressure("ACTION_OUTCOME", as_of_ns=new.available_at_ns)["pending_count"] == 1


def test_pending_horizon_is_rescheduled_and_terminal_support_not_revisited(tmp_path, monkeypatch):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        decision = _calendar(repo, 1)
        now = decision.available_at_ns + 10
        horizon = now + coordinator.OUTCOME_DUE_RETRY_INTERVAL_NS_V1 + 100
        calls = []

        def resolve(_repo, entry, cutoff, **_kwargs):
            calls.append(entry.artifact_ref)
            return _resolution(decision, "PENDING", horizon=horizon) if cutoff < horizon else (
                _resolution(decision, "UNSUPPORTED", horizon=horizon))

        monkeypatch.setattr(coordinator, "resolve_decision_outcome", resolve)
        assert _cycle(repo, now).pending_count == 1
        assert _cycle(repo, now + 1).decisions_inspected == 0
        assert _cycle(repo, horizon).unsupported_count == 1
        assert calls == [decision.content_hash, decision.content_hash]
    with OpsRepository(path) as repo:
        assert _cycle(repo, horizon + 1).decisions_inspected == 0
        assert repo.due_work_pressure("ACTION_OUTCOME", as_of_ns=horizon + 1)["retired_count"] == 1


def test_due_overflow_and_oldest_backlog_are_visible(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        decisions = [_calendar(repo, i) for i in range(12)]
        now = decisions[-1].available_at_ns + 100
        pressure = repo.due_work_pressure("ACTION_OUTCOME", as_of_ns=now, page_limit=8)
        assert pressure["pending_count"] == 12
        assert pressure["due_count_lower_bound"] == 9
        assert pressure["due_page_overflow"] is True
        assert pressure["oldest_pending_age_ns"] == now - decisions[0].created_at_ns
        assert len(repo.due_work_items("ACTION_OUTCOME", as_of_ns=now, limit=8)) == 8
        with pytest.raises(ValueError):
            repo.due_work_items("ACTION_OUTCOME", as_of_ns=now, limit=65)


def test_read_only_observer_sees_pressure_while_writer_continues(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as writer, OpsRepository(path, read_only=True) as reader:
        decision = _calendar(writer, 1)
        assert reader.due_work_pressure("ACTION_OUTCOME", as_of_ns=decision.available_at_ns)["pending_count"] == 1
        with pytest.raises(RuntimeError, match="read-only"):
            reader.retire_due_work("ACTION_OUTCOME", decision.content_hash, reason_code="UNSUPPORTED")
        writer.retire_due_work("ACTION_OUTCOME", decision.content_hash, reason_code="UNSUPPORTED")
        assert reader.due_work_pressure("ACTION_OUTCOME", as_of_ns=decision.available_at_ns)["pending_count"] == 0


def test_supported_action_outcome_retires_without_historical_validation_loop(tmp_path, monkeypatch):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _case, _action, _payoff, outcome = _supported_replay_fixture(repo)
        monkeypatch.setattr(repo, "artifact_entries", lambda *_args: pytest.fail("unbounded history read"))
        first = _cycle(repo, outcome.available_at_ns)
        assert first.outcomes_indexed == 1
        assert first.matured_count == 1
        assert first.due_work["pending_count"] == 0
        assert _cycle(repo, outcome.available_at_ns + 1).outcomes_attempted == 0


def test_prediction_crash_after_label_keeps_first_measurement_and_retires(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        maintenance, item, now = _matured_fixture(repo, tmp_path)
        # Publish immutable label/completion but leave queue progress unfinished.
        measured, writes = maintenance._maintain_due_horizon(repo, item.terminal_ref, item.horizon_ns, now[0])
        assert writes == 1
        first_ref = measured.content_hash
    with OpsRepository(path) as repo:
        maintenance = ResearchPredictionOutcomeMaintenanceV1(run_id=item.run_id, config_hash=item.config_hash,
            archive_root=tmp_path, clock_ns=lambda: now[0],
            bar_reader=lambda *_args, **_kwargs: pytest.fail("completed raw reread"))
        resumed = maintenance.run_cycle(repo, evidence_cutoff_ns=now[0])
        assert resumed["labels_written"] == 0
        assert resumed["due_work"]["pending_count"] == 0
        assert repo.get_artifact(first_ref) is not None
        now[0] += MAINTENANCE_INTERVAL_NS_V1
        assert maintenance.run_cycle(repo, evidence_cutoff_ns=now[0])["horizons_inspected"] == 0


def test_prediction_label_published_after_cutoff_defers_without_remeasurement(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        maintenance, item, now = _matured_fixture(repo, tmp_path)
        original_ref = index_research_prediction_outcome_v1(repo, item)
        maintenance.bar_reader = lambda *_args, **_kwargs: pytest.fail("published label raw reread")
        first = maintenance.run_cycle(repo, evidence_cutoff_ns=item.available_at_ns - 1)
        assert first["labels_written"] == 0 and first["due_work"]["pending_count"] == 1
        assert first["diagnostics"] == []
        assert repo.artifact_entries("ResearchPredictionCompletionIdentityV1") == ()
        now[0] += MAINTENANCE_INTERVAL_NS_V1
        resumed = maintenance.run_cycle(repo, evidence_cutoff_ns=now[0])
        assert resumed["labels_written"] == 0 and resumed["due_work"]["pending_count"] == 0
        completion = repo.artifact_entries("ResearchPredictionCompletionIdentityV1")[0]
        assert completion.metadata["prediction_completion"]["outcome_ref"] == original_ref


def test_existing_schema_migration_discovers_sources_and_never_reenqueues_retired(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        decision = _calendar(repo, 1)
        # Emulate an accepted pre-projection store without changing its evidence.
        for table in ("due_work", "due_work_pressure", "due_work_discovery"):
            repo._connection.execute(f"DROP TABLE {table}")
    with OpsRepository(path) as repo:
        assert repo.due_work_items("ACTION_OUTCOME", as_of_ns=decision.available_at_ns) == ()
        assert repo.discover_due_work(limit=1)["rows_inspected"] == 1
        assert len(repo.due_work_items("ACTION_OUTCOME", as_of_ns=decision.available_at_ns)) == 1
        repo.retire_due_work("ACTION_OUTCOME", decision.content_hash, reason_code="UNSUPPORTED")
        assert repo.discover_due_work(limit=1)["rows_inspected"] == 0
        assert repo.due_work_items("ACTION_OUTCOME", as_of_ns=decision.available_at_ns) == ()


def test_quarantine_preserves_exact_identity_reason_and_restart_pressure(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        decision = _calendar(repo, 1)
        repo.quarantine_due_work("ACTION_OUTCOME", decision.content_hash, reason_code="MALFORMED_CALENDAR_ENTRY")
        repo.quarantine_due_work("ACTION_OUTCOME", decision.content_hash, reason_code="MALFORMED_CALENDAR_ENTRY")
    with OpsRepository(path) as repo:
        repo.discover_due_work()
        pressure = repo.due_work_pressure("ACTION_OUTCOME", as_of_ns=decision.available_at_ns)
        assert pressure["pending_count"] == 0
        assert pressure["retired_count"] == pressure["quarantined_count"] == 1
        row = repo._connection.execute("SELECT source_ref,state,reason_code FROM due_work WHERE work_id=?",
            (decision.content_hash,)).fetchone()
        assert tuple(row) == (decision.content_hash, "QUARANTINED", "MALFORMED_CALENDAR_ENTRY")


def test_malformed_action_calendar_is_accounted_and_quarantined(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        ref = sha256_json({"calendar": "malformed"})
        repo.register_artifact(ArtifactIndexEntryV2(ref, "DecisionCalendarEntryV2", ref, 1, 1, {}))
        first = _cycle(repo, 10)
        assert first.invalid_calendar_entries == 1
        assert first.due_work["quarantined_count"] == 1
        assert first.status_artifacts_written == 1
        assert _cycle(repo, 11).decisions_inspected == 0


def test_malformed_prediction_terminal_is_quarantined_with_structured_diagnostic(tmp_path):
    path = tmp_path / "ops.sqlite"
    ref = sha256_json({"terminal": "malformed"})
    with OpsRepository(path) as repo:
        repo.register_artifact(ArtifactIndexEntryV2(ref, "ResearchModelTerminalV1", ref, 1, 1, {}))
        maintenance = ResearchPredictionOutcomeMaintenanceV1(run_id="run-a", config_hash="a" * 64,
            archive_root=tmp_path, clock_ns=lambda: 10)
        report = maintenance.run_cycle(repo, evidence_cutoff_ns=10)
        assert report["diagnostics"] == [{"source_ref": ref, "work_id": ref,
            "reason_code": "PREDICTION_TERMINAL_LINEAGE_INVALID", "disposition": "QUARANTINED"}]
        assert report["unscoped_terminal_due_work"]["quarantined_count"] == 1
    with OpsRepository(path) as repo:
        maintenance.clock_ns = lambda: 10 + MAINTENANCE_INTERVAL_NS_V1
        assert maintenance.run_cycle(repo, evidence_cutoff_ns=maintenance.clock_ns())["terminals_inspected"] == 0


def test_prediction_runs_do_not_inspect_or_quarantine_other_run_work(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        routing = {"run_id": "run-b", "config_hash": "b" * 64}
        ref = sha256_json(routing)
        repo.register_artifact(ArtifactIndexEntryV2(ref, "ResearchModelTerminalV1", ref, 1, 1, {"routing": routing}))
        lane = prediction_due_lane_v1("PREDICTION_TERMINAL", "run-b", "b" * 64)
        maintenance = ResearchPredictionOutcomeMaintenanceV1(run_id="run-a", config_hash="a" * 64,
            archive_root=tmp_path, clock_ns=lambda: 10)
        report = maintenance.run_cycle(repo, evidence_cutoff_ns=10)
        assert report["terminals_inspected"] == 0 and report["diagnostics"] == []
        assert repo.due_work_pressure(lane, as_of_ns=10)["pending_count"] == 1
        # A previous unscoped projection is routed to its owner, not rejected.
        repo.enqueue_due_work(lane="PREDICTION_TERMINAL", work_id=ref, source_ref=ref,
            created_at_ns=1, due_at_ns=1, payload={})
        maintenance.clock_ns = lambda: 10 + MAINTENANCE_INTERVAL_NS_V1
        report = maintenance.run_cycle(repo, evidence_cutoff_ns=maintenance.clock_ns())
        assert report["diagnostics"] == [] and report["failure_code"] is None
        assert repo.due_work_pressure(lane, as_of_ns=10)["pending_count"] == 1


def test_prediction_missing_later_endpoint_retries_at_future_due_time(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        maintenance, item, now = _matured_fixture(repo, tmp_path)
        endpoint = repo.get_artifact(item.measurement_ref).metadata["measurement"]["horizon_bar_source_ref"]
        # Model evidence exists before this final endpoint is published.
        entry = repo.get_artifact(endpoint)
        repo._connection.execute("DELETE FROM artifact_index WHERE artifact_ref=?", (endpoint,))
        first = maintenance.run_cycle(repo, evidence_cutoff_ns=now[0])
        assert first["horizons_inspected"] == 1 and first["due_work"]["pending_count"] == 1
        lane = prediction_due_lane_v1("PREDICTION_OUTCOME", item.run_id, item.config_hash)
        assert repo.due_work_items(lane, as_of_ns=now[0]) == ()
        assert first["diagnostics"] == []
        repo.register_artifact(entry)
        now[0] += MAINTENANCE_INTERVAL_NS_V1
        second = maintenance.run_cycle(repo, evidence_cutoff_ns=now[0])
        assert second["horizons_inspected"] == 1 and second["due_work"]["pending_count"] == 0
        assert second["labels_written"] == 1


def test_atomic_composition_rolls_back_watch_transition_artifacts_and_due_work(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        initial = watch()
        repo.create_watch(initial)
        with pytest.raises(RuntimeError, match="crash before seal"), repo.atomic_composition():
            repo.transition_watch("watch", expected_state_version=0, event_id="event", event_at_ns=110,
                transition_at_ns=110, target_state=WatchStateV2.WAITING_FOR_EVENT, outbox_id="outbox")
            decision = _calendar(repo, 1)
            raise RuntimeError("crash before seal")
        assert repo.get_watch("watch") == initial
        assert repo.pending_outbox() == ()
        assert repo.get_artifact(decision.content_hash) is None
        assert repo.due_work_pressure("ACTION_OUTCOME", as_of_ns=decision.available_at_ns)["pending_count"] == 0
    with OpsRepository(path) as repo:
        assert repo.get_watch("watch") == initial
        assert repo.get_artifact(decision.content_hash) is None


def test_nested_composition_failure_rolls_back_only_inner_work_and_outer_commit_is_visible(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as writer, OpsRepository(path, read_only=True) as observer:
        with writer.atomic_composition():
            first = _calendar(writer, 1)
            with pytest.raises(RuntimeError, match="inner failure"), writer.atomic_composition():
                inner = _calendar(writer, 2)
                raise RuntimeError("inner failure")
            last = _calendar(writer, 3)
            assert writer.get_artifact(inner.content_hash) is None
            assert observer.get_artifact(first.content_hash) is None
        assert observer.get_artifact(first.content_hash) is not None
        assert observer.get_artifact(last.content_hash) is not None
        assert observer.get_artifact(inner.content_hash) is None
        assert observer.due_work_pressure("ACTION_OUTCOME", as_of_ns=last.available_at_ns)["pending_count"] == 2
        with pytest.raises(RuntimeError, match="read-only"), observer.atomic_composition():
            pytest.fail("read-only composition body ran")


def test_latest_artifact_page_orders_available_evidence_and_exposes_history_overflow(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        rows = tuple(ArtifactIndexEntryV2(sha256_json({"latest": i}), "LatestFixture",
            sha256_json({"latest": i}), 0, i, {}) for i in range(1000))
        repo.register_artifacts(rows)
        page = repo.latest_artifact_entries("LatestFixture", as_of_ns=900, limit=3)
        assert [entry.available_at_ns for entry in page.entries] == [900, 899, 898]
        assert page.has_more and page.invalid_entry_count == 0
        plan = repo._connection.execute("EXPLAIN QUERY PLAN SELECT * FROM artifact_index "
            "WHERE artifact_type=? AND available_at_ns<=? ORDER BY available_at_ns DESC,artifact_ref DESC LIMIT ?",
            ("LatestFixture", 900, 4)).fetchall()
        assert any("artifact_latest_available" in row[3] for row in plan)
        assert all("TEMP B-TREE" not in row[3] for row in plan)
        malformed = page.entries[0]
        repo._connection.execute("UPDATE artifact_index SET metadata_json='{' WHERE artifact_ref=?",
            (malformed.artifact_ref,))
        invalid = repo.latest_artifact_entries("LatestFixture", as_of_ns=900, limit=3)
        assert len(invalid.entries) == 2 and invalid.invalid_entry_count == 1 and invalid.has_more
        with pytest.raises(ValueError):
            repo.latest_artifact_entries("LatestFixture", as_of_ns=900, limit=4097)


def test_latest_identity_pages_use_available_index_with_exact_string_and_integer_keys(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        for i in range(30):
            body = {"health": {"source_id": "source-a" if i % 2 else "source-b"}, "ordinal": i}
            ref = sha256_json(body)
            repo.register_artifact(ArtifactIndexEntryV2(ref, "PublicSourceHealthV2", ref, 0, i, body))
        page = repo.latest_artifact_entries("PublicSourceHealthV2", as_of_ns=25, limit=2,
            metadata_path=("health", "source_id"), identity_value="source-a")
        assert [entry.available_at_ns for entry in page.entries] == [25, 23]
        assert page.has_more
        plan = repo._connection.execute("EXPLAIN QUERY PLAN SELECT * FROM artifact_index "
            "WHERE artifact_type='PublicSourceHealthV2' AND CASE WHEN json_valid(metadata_json) "
            "THEN json_extract(metadata_json, '$.health.source_id') END=? AND available_at_ns<=? "
            "ORDER BY available_at_ns DESC,artifact_ref DESC LIMIT ?", ("source-a", 25, 3)).fetchall()
        assert any("latest_metadata_" in row[3] for row in plan)
        assert all("TEMP B-TREE" not in row[3] for row in plan)
        body = {"cutoff_ns": 20}
        ref = sha256_json(body)
        repo.register_artifact(ArtifactIndexEntryV2(ref, "EventSafetyGateV2", ref, 20, 20, body))
        integer_page = repo.latest_artifact_entries("EventSafetyGateV2", as_of_ns=20, limit=1,
            metadata_path=("cutoff_ns",), identity_value=20)
        assert integer_page.entries[0].artifact_ref == ref and not integer_page.has_more
        assert repo.latest_artifact_entries("EventSafetyGateV2", as_of_ns=20, limit=1,
            metadata_path=("cutoff_ns",), identity_value="20").entries == ()
        with pytest.raises(ValueError):
            repo.latest_artifact_entries("EventSafetyGateV2", as_of_ns=20, limit=1,
                metadata_path=("cutoff_ns",), identity_value=True)
        with pytest.raises(ValueError, match="supplied together"):
            repo.latest_artifact_entries("EventSafetyGateV2", as_of_ns=20, limit=1, identity_value=20)
