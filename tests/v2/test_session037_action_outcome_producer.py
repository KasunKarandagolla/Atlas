"""Production replay uses sealed exact evidence and the accepted replay engine."""
from dataclasses import replace

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.action_outcome_producer import (
    ActionReplaySourceEvidenceV1,
    RetrospectiveActionOutcomeProducerV1,
    index_action_replay_source_evidence,
)
from atlas.v2.science.outcomes import (
    AdmissionStateV2,
    DecisionCalendarEntryV2,
    DecisionSourceStageV2,
    SelectionStateV2,
    index_decision_calendar_entry,
)
from atlas.v2.science.replay import HOUR_NS, MINUTE_NS

from .test_session017_replay import minute, replay_context
from .test_session017_risk import CUTOFF, risk_case, source


def _fixture(repo, *, depth="100", ask="100", bound=False, minutes=None, raw_count=1):
    case = risk_case(repo)
    action, portfolio, assumptions, schedule, path = replay_context(repo, case, minutes=minutes or (
        minute(CUTOFF, ask_depth=depth, ask=ask),
        minute(CUTOFF + 4 * HOUR_NS, bid="110", ask="110", mark_low="110", mark_high="110",
               last_low="110", last_high="110"),
    ))
    calendar = DecisionCalendarEntryV2(case.candidate_set.content_hash, case.candidate.content_hash,
        action.action.policy_id, action.action.policy_version, action.action.policy_hash, CUTOFF,
        SelectionStateV2.SELECTED, AdmissionStateV2.RISK_SIZED, action.action.action_hash,
        action.content_hash, DecisionSourceStageV2.HARD_RISK, (), action.sizing_ref, CUTOFF, CUTOFF)
    index_decision_calendar_entry(repo, calendar)
    raw_refs = tuple(sorted(source(repo, f"minute-public-archive-{i}", path.available_at_ns)
                            for i in range(raw_count)))
    bound_ref = None
    if bound:
        body = {"scope": "RETROSPECTIVE_ONLY", "bounds": {"depth": "sealed conservative lower bound"}}
        bound_ref = sha256_json(body)
        repo.register_artifact(ArtifactIndexEntryV2(bound_ref, "ReplayBoundAssumptionsV1", bound_ref,
            CUTOFF, CUTOFF, body))
    source_class = "CONSERVATIVE_BOUND" if bound else "EXACT_MINUTE"
    minute_refs = []
    rows = path.to_dict()["minutes"]
    for i, row in enumerate(rows):
        row_refs = raw_refs[i::len(rows)] or raw_refs[:1]
        body = {"minute": row, "raw_source_refs": list(row_refs), "source_class": source_class,
                "bound_assumptions_ref": bound_ref}
        ref = sha256_json(body)
        repo.register_artifact(ArtifactIndexEntryV2(ref, "ActionReplayMinuteEvidenceV1", ref,
            path.available_at_ns, path.available_at_ns, body))
        minute_refs.append((row["at_ns"], ref))
    evidence = ActionReplaySourceEvidenceV1(calendar.content_hash, action.content_hash, path.content_hash,
        case.product.content_hash, portfolio.content_hash, assumptions.content_hash, case.fee.content_hash,
        schedule.content_hash, tuple(minute_refs), raw_refs, path.available_at_ns, source_class, bound_ref)
    return case, calendar, evidence


@pytest.mark.parametrize("depth,ask,status,payoff", [
    ("100", "100", "FULL_FILL", "239.855"),
    ("0.5", "100", "PARTIAL_FILL", "4.895"),
    ("0", "100", "NO_FILL", "0"),
    ("100", "101", "NO_FILL", "0"),
])
def test_producer_publishes_existing_execution_contract(tmp_path, depth, ask, status, payoff):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _, calendar, evidence = _fixture(repo, depth=depth, ask=ask)
        index_action_replay_source_evidence(repo, evidence)
        now = evidence.available_at_ns + 1
        result = RetrospectiveActionOutcomeProducerV1(clock_ns=lambda: now)(
            repo, repo.get_artifact(calendar.content_hash), evidence.available_at_ns)
        assert result.status == "PRODUCED" and result.reason_code is None
        assert result.available_at_ns == now
        body = repo.get_artifact(result.payoff_ref).metadata["payoff"]
        assert body["status"] == status and body["payoff"] == payoff
        assert body["available_at_ns"] == now
        assert evidence.content_hash in result.evidence_refs
        assert repo.artifact_entries("TradePlanEnvelopeV2") == ()


def test_restart_reuses_same_payoff_identity(tmp_path):
    db = tmp_path / "restart.sqlite"
    with OpsRepository(db) as repo:
        _, calendar, evidence = _fixture(repo)
        index_action_replay_source_evidence(repo, evidence)
        first = RetrospectiveActionOutcomeProducerV1(clock_ns=lambda: evidence.available_at_ns + 1)(
            repo, repo.get_artifact(calendar.content_hash), evidence.available_at_ns)
    with OpsRepository(db) as repo:
        repeated = RetrospectiveActionOutcomeProducerV1(clock_ns=lambda: evidence.available_at_ns + 100)(
            repo, repo.get_artifact(calendar.content_hash), evidence.available_at_ns + 1)
        assert repeated.status == "EXISTING" and repeated.payoff_ref == first.payoff_ref
        assert len(repo.artifact_entries("PolicyPayoffV2")) == 1


def test_missing_native_bbo_support_is_not_fabricated(tmp_path):
    with OpsRepository(tmp_path / "missing.sqlite") as repo:
        _, calendar, evidence = _fixture(repo, depth=None)
        producer = RetrospectiveActionOutcomeProducerV1(clock_ns=lambda: evidence.available_at_ns)
        absent = producer(repo, repo.get_artifact(calendar.content_hash), evidence.available_at_ns)
        assert absent.reason_code == "REPLAY_SOURCE_EVIDENCE_UNAVAILABLE"
        assert repo.artifact_entries("PolicyPayoffV2") == ()
        index_action_replay_source_evidence(repo, evidence)
        unsupported = producer(repo, repo.get_artifact(calendar.content_hash), evidence.available_at_ns)
        assert unsupported.status == "NOT_ESTIMABLE" and unsupported.reason_code == "MISSING_ENTRY_DEPTH"
        assert repo.get_artifact(unsupported.payoff_ref).metadata["payoff"]["payoff"] is None


def test_future_sources_stay_unavailable(tmp_path):
    with OpsRepository(tmp_path / "future.sqlite") as repo:
        _, calendar, evidence = _fixture(repo)
        index_action_replay_source_evidence(repo, evidence)
        result = RetrospectiveActionOutcomeProducerV1(clock_ns=lambda: evidence.available_at_ns)(
            repo, repo.get_artifact(calendar.content_hash), evidence.available_at_ns - 1)
        assert result.status == "NOT_ESTIMABLE" and result.payoff_ref is None
        assert repo.artifact_entries("PolicyPayoffV2") == ()


def test_sealed_minute_tamper_cannot_publish_payoff(tmp_path):
    with OpsRepository(tmp_path / "tamper.sqlite") as repo:
        _, calendar, evidence = _fixture(repo)
        index_action_replay_source_evidence(repo, evidence)
        ref = evidence.minute_source_refs[0][1]
        repo._connection.execute("UPDATE artifact_index SET metadata_json=json_set(metadata_json, '$.minute.ask_depth','9999') WHERE artifact_ref=?", (ref,))
        result = RetrospectiveActionOutcomeProducerV1(clock_ns=lambda: evidence.available_at_ns)(
            repo, repo.get_artifact(calendar.content_hash), evidence.available_at_ns)
        assert result.status == "NOT_ESTIMABLE" and result.reason_code == "REPLAY_EVIDENCE_INVALID"
        assert repo.artifact_entries("PolicyPayoffV2") == ()


def test_conflicting_sources_fail_closed_before_replay(tmp_path):
    with OpsRepository(tmp_path / "conflict.sqlite") as repo:
        _, calendar, evidence = _fixture(repo)
        index_action_replay_source_evidence(repo, evidence)
        index_action_replay_source_evidence(repo, replace(evidence, available_at_ns=evidence.available_at_ns + 1))
        result = RetrospectiveActionOutcomeProducerV1(clock_ns=lambda: evidence.available_at_ns + 2)(
            repo, repo.get_artifact(calendar.content_hash), evidence.available_at_ns + 1)
        assert result.reason_code == "CONFLICTING_REPLAY_SOURCE_EVIDENCE"
        assert repo.artifact_entries("PolicyPayoffV2") == ()


def test_explicit_conservative_bound_records_supported(tmp_path):
    with OpsRepository(tmp_path / "bound.sqlite") as repo:
        _, calendar, evidence = _fixture(repo, bound=True)
        index_action_replay_source_evidence(repo, evidence)
        result = RetrospectiveActionOutcomeProducerV1(clock_ns=lambda: evidence.available_at_ns)(
            repo, repo.get_artifact(calendar.content_hash), evidence.available_at_ns)
        assert result.status == "PRODUCED"


def test_unsealed_minute_and_unbounded_sources_rejected(tmp_path):
    with OpsRepository(tmp_path / "bounds.sqlite") as repo:
        _, _, evidence = _fixture(repo)
        with pytest.raises(ValueError):
            replace(evidence, minute_source_refs=tuple((index, evidence.minute_source_refs[0][1]) for index in range(257)))
        with pytest.raises(ValueError):
            index_action_replay_source_evidence(repo, replace(evidence, raw_source_refs=(evidence.path_ref,)))


def test_default_lookups_are_exact_and_bounded(tmp_path, monkeypatch):
    with OpsRepository(tmp_path / "bounded.sqlite") as repo:
        _, calendar, evidence = _fixture(repo)
        index_action_replay_source_evidence(repo, evidence)
        queries = []
        query = repo.artifact_entries_by_metadata_identity
        def bounded(kind, path, identity, **kwargs):
            queries.append((kind, tuple(path), identity, kwargs["limit"]))
            return query(kind, path, identity, **kwargs)
        monkeypatch.setattr(repo, "artifact_entries_by_metadata_identity", bounded)
        monkeypatch.setattr(repo, "artifact_entries", lambda *_args, **_kwargs: pytest.fail("full artifact scan"))
        result = RetrospectiveActionOutcomeProducerV1(clock_ns=lambda: evidence.available_at_ns)(
            repo, repo.get_artifact(calendar.content_hash), evidence.available_at_ns)
        assert result.status == "PRODUCED"
        assert queries == [
            ("ActionReplaySourceEvidenceV1", ("source_evidence", "decision_ref"), calendar.content_hash, 1),
            ("PolicyPayoffV2", ("payoff", "action_hash"), calendar.action_hash, 1),
        ]


def test_no_action_calendar_is_unsupported(tmp_path):
    from .test_session033_outcome_maturity import _calendar
    with OpsRepository(tmp_path / "no-action.sqlite") as repo:
        calendar = _calendar(repo, 1)
        result = RetrospectiveActionOutcomeProducerV1(clock_ns=lambda: calendar.available_at_ns)(
            repo, repo.get_artifact(calendar.content_hash), calendar.available_at_ns)
        assert result.status == "UNSUPPORTED" and result.reason_code == "NO_FROZEN_ACTION"
        assert result.payoff_ref is None


def test_future_predeclared_assumptions_fail_closed(tmp_path):
    with OpsRepository(tmp_path / "future-assumptions.sqlite") as repo:
        _, calendar, evidence = _fixture(repo)
        index_action_replay_source_evidence(repo, evidence)
        repo._connection.execute("UPDATE artifact_index SET available_at_ns=? WHERE artifact_ref=?",
            (CUTOFF + 1, evidence.replay_assumptions_ref))
        result = RetrospectiveActionOutcomeProducerV1(clock_ns=lambda: evidence.available_at_ns)(
            repo, repo.get_artifact(calendar.content_hash), evidence.available_at_ns)
        assert result.status == "NOT_ESTIMABLE" and result.payoff_ref is None
        assert repo.artifact_entries("PolicyPayoffV2") == ()


def test_advancing_clock_publishes_after_calculation_and_seals_lifecycle(tmp_path, monkeypatch):
    from atlas.v2.science import replay
    with OpsRepository(tmp_path / "advancing.sqlite") as repo:
        _, calendar, evidence = _fixture(repo)
        index_action_replay_source_evidence(repo, evidence)
        at = evidence.available_at_ns
        economic_complete = False
        samples_after_math = []
        original = replay._economic_result
        def economic_result(**kwargs):
            nonlocal economic_complete
            value = original(**kwargs)
            economic_complete = True
            return value
        def clock():
            nonlocal at
            at += 1
            samples_after_math.append(economic_complete)
            return at
        monkeypatch.setattr(replay, "_economic_result", economic_result)
        result = RetrospectiveActionOutcomeProducerV1(clock_ns=clock)(
            repo, repo.get_artifact(calendar.content_hash), evidence.available_at_ns)
        assert result.status == "PRODUCED"
        payoff = repo.get_artifact(result.payoff_ref)
        assert samples_after_math == [False, True, True, True]
        assert payoff.available_at_ns == evidence.available_at_ns + 2
        summary_entry = repo.artifact_entries("ActionReplayLifecycleSummaryV1")[0]
        summary = summary_entry.metadata["summary"]
        assert summary_entry.content_hash == sha256_json(summary)
        assert summary["source_evidence_ref"] == evidence.content_hash
        assert summary["decision_ref"] == calendar.content_hash and summary["payoff_ref"] == result.payoff_ref
        assert summary["entry_at_ns"] == CUTOFF and tuple(summary["exit_at_ns"]) == (CUTOFF + 4 * HOUR_NS,)
        assert summary["exit_reason"] == "TIME_EXIT"
        assert summary["evidence_cutoff_ns"] == evidence.available_at_ns
        assert payoff.available_at_ns < summary_entry.available_at_ns < result.available_at_ns
        restarted = RetrospectiveActionOutcomeProducerV1(clock_ns=clock)(
            repo, repo.get_artifact(calendar.content_hash), evidence.available_at_ns)
        assert restarted.status == "EXISTING" and restarted.payoff_ref == result.payoff_ref
        assert len(repo.artifact_entries("ActionReplayLifecycleSummaryV1")) == 1


def test_later_production_clock_cannot_admit_future_replay_setup(tmp_path):
    with OpsRepository(tmp_path / "future-risk.sqlite") as repo:
        _, calendar, evidence = _fixture(repo)
        index_action_replay_source_evidence(repo, evidence)
        # The source itself is cutoff-known, but an action dependency is made
        # available only between the information cutoff and production finish.
        action = repo.get_artifact(evidence.action_ref).metadata["action_artifact"]
        repo._connection.execute("UPDATE artifact_index SET available_at_ns=? WHERE artifact_ref=?",
            (evidence.available_at_ns + 5, action["sizing_ref"]))
        result = RetrospectiveActionOutcomeProducerV1(clock_ns=lambda: evidence.available_at_ns + 10)(
            repo, repo.get_artifact(calendar.content_hash), evidence.available_at_ns)
        assert result.status == "NOT_ESTIMABLE" and result.payoff_ref is None
        assert repo.artifact_entries("PolicyPayoffV2") == ()
        assert repo.artifact_entries("ActionReplayLifecycleSummaryV1") == ()


def test_default_coordinator_defers_new_payoff_then_matures_after_restart(tmp_path, monkeypatch):
    """The installed due-work path produces evidence without widening its cutoff."""
    from atlas.v2.runtime import outcome_maturity as coordinator

    path = tmp_path / "coordinator.sqlite"
    with OpsRepository(path) as repo:
        _, calendar, evidence = _fixture(repo)
        index_action_replay_source_evidence(repo, evidence)
        cutoff = evidence.available_at_ns
        # Exercise the default producer and resolver, with no injected labels.
        monkeypatch.setattr(repo, "artifact_entries", lambda *_args: pytest.fail("full history scan"))
        first = coordinator.run_outcome_maturity_cycle(repo, evidence_cutoff_ns=cutoff,
            production_clock_ns=lambda: cutoff + 1, monotonic_ns=lambda: 0)
        assert first.failure_code is None
        assert first.unresolved_count == 1 and first.outcomes_indexed == 0
        payoff_page = repo.artifact_entries_by_metadata_identity("PolicyPayoffV2",
            ("payoff", "action_hash"), calendar.action_hash, as_of_ns=cutoff + 1, limit=1)
        assert len(payoff_page.entries) == 1

        payoff_ref = payoff_page.entries[0].artifact_ref
        assert payoff_page.entries[0].available_at_ns > cutoff
        status_page = repo.artifact_entries_by_metadata_identity("OutcomeMaturityStatusV1",
            ("status", "decision_ref"), calendar.content_hash, as_of_ns=cutoff + 1, limit=1)
        assert status_page.entries[0].metadata["status"]["reason_code"] == "ACTION_PAYOFF_AWAITING_NEXT_CUTOFF"
        assert first.due_work["pending_count"] == 1

    resumed_cutoff = cutoff + coordinator.OUTCOME_DUE_RETRY_INTERVAL_NS_V1
    with OpsRepository(path) as repo:
        monkeypatch.setattr(repo, "artifact_entries", lambda *_args: pytest.fail("full history scan"))
        resumed = coordinator.run_outcome_maturity_cycle(repo, evidence_cutoff_ns=resumed_cutoff,
            production_clock_ns=lambda: resumed_cutoff + 1, monotonic_ns=lambda: 0)
        assert resumed.failure_code is None
        assert resumed.matured_count == resumed.outcomes_indexed == 1
        assert resumed.due_work["pending_count"] == 0
        matured_page = repo.artifact_entries_by_metadata_identity("MaturedOutcomeV2",
            ("outcome", "decision_ref"), calendar.content_hash, as_of_ns=resumed_cutoff + 1, limit=1)
        outcome = matured_page.entries[0].metadata["outcome"]
        assert outcome["decision_ref"] == calendar.content_hash
        assert outcome["action_hash"] == calendar.action_hash
        assert payoff_ref in outcome["evidence_refs"]
        assert outcome["outcome_target"] == "EXECUTABLE_ACTION_VALUE"
        assert outcome["provenance"] == "SIMULATED"
        assert outcome["net_payoff"] == "239.855"
        final = coordinator.run_outcome_maturity_cycle(repo, evidence_cutoff_ns=resumed_cutoff + 2,
            production_clock_ns=lambda: resumed_cutoff + 3, monotonic_ns=lambda: 0)
        assert final.outcomes_attempted == 0


def test_maximum_sealed_replay_source_fits_default_coordinator_read_budget(tmp_path):
    """Admitted 256-minute/1,024-raw support must fit the installed read budget."""
    from atlas.v2.runtime import outcome_maturity as coordinator

    with OpsRepository(tmp_path / "maximum.sqlite") as repo:
        # One outer transaction keeps this boundary fixture cheap to construct;
        # the actual coordinator still uses its normal bounded repository view.
        with repo.atomic_composition():
            _, calendar, evidence = _fixture(repo, raw_count=1024, minutes=tuple(
                minute(CUTOFF + i * MINUTE_NS, bid="110" if i >= 240 else "100",
                    ask="110" if i >= 240 else "100", mark_low="110" if i >= 240 else "100",
                    mark_high="110" if i >= 240 else "100", last_low="110" if i >= 240 else "100",
                    last_high="110" if i >= 240 else "100") for i in range(256)))
            index_action_replay_source_evidence(repo, evidence)
        cutoff = evidence.available_at_ns
        first = coordinator.run_outcome_maturity_cycle(repo, evidence_cutoff_ns=cutoff,
            production_clock_ns=lambda: cutoff + 1, monotonic_ns=lambda: 0)
        assert first.failure_code is None
        assert first.outcomes_indexed == 0 and first.unresolved_count == 1
        payoff_page = repo.artifact_entries_by_metadata_identity("PolicyPayoffV2",
            ("payoff", "action_hash"), calendar.action_hash, as_of_ns=cutoff + 1, limit=1)
        assert len(payoff_page.entries) == 1

        next_cutoff = cutoff + coordinator.OUTCOME_DUE_RETRY_INTERVAL_NS_V1
        second = coordinator.run_outcome_maturity_cycle(repo, evidence_cutoff_ns=next_cutoff,
            production_clock_ns=lambda: next_cutoff + 1, monotonic_ns=lambda: 0)
        assert second.failure_code is None
        assert second.matured_count == second.outcomes_indexed == 1
        assert second.due_work["pending_count"] == 0
