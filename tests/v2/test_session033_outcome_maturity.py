"""Focused offline regressions for the bounded S33 maturity coordinator."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

from atlas.v2._serialization import json_value, sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime import outcome_maturity as coordinator
from atlas.v2.science.outcome_resolution import OutcomeResolutionV1
from atlas.v2.science.outcomes import (
    AdmissionStateV2,
    DecisionCalendarEntryV2,
    DecisionSourceStageV2,
    MaturedOutcomeV2,
    SelectionStateV2,
)


def _calendar(repo: OpsRepository, index: int, *, at_ns: int = 1_800_000_000_000_000) -> DecisionCalendarEntryV2:
    candidate_set_ref = sha256_json({"fixture_set": index})
    decision = DecisionCalendarEntryV2(
        candidate_set_ref=candidate_set_ref,
        candidate_ref=None,
        policy_id="fixture-policy",
        policy_version="1",
        policy_hash=sha256_json({"fixture_policy": index}),
        decision_at_ns=at_ns + index,
        selection_state=SelectionStateV2.NO_CANDIDATE,
        admission_state=AdmissionStateV2.NOT_APPLICABLE,
        action_hash=None,
        action_artifact_ref=None,
        source_stage=DecisionSourceStageV2.CANDIDATE_SET,
        reason_codes=(),
        source_artifact_ref=candidate_set_ref,
        created_at_ns=at_ns + index,
        available_at_ns=at_ns + index,
    )
    repo.register_artifact(ArtifactIndexEntryV2(
        decision.content_hash, "DecisionCalendarEntryV2", decision.content_hash,
        decision.created_at_ns, decision.available_at_ns, {"decision_entry": decision.to_dict()},
    ))
    return decision


def _resolution(decision: DecisionCalendarEntryV2, status: str, *, horizon: int | None = None,
                outcome: MaturedOutcomeV2 | None = None, reason: str = "FIXTURE_STATE") -> OutcomeResolutionV1:
    return OutcomeResolutionV1(decision.content_hash, status, outcome, horizon, reason)


def _status_entries(repo: OpsRepository, decision_ref: str, *, as_of_ns: int):
    page = repo.artifact_entries_by_metadata_identity(
        "OutcomeMaturityStatusV1", ("status", "decision_ref"), decision_ref,
        as_of_ns=as_of_ns, limit=8,
    )
    assert page.invalid_entry_count == 0
    return page.entries


def _outcome_entries(repo: OpsRepository, decision_ref: str, *, as_of_ns: int):
    return repo.artifact_entries_by_metadata_identity(
        "MaturedOutcomeV2", ("outcome", "decision_ref"), decision_ref,
        as_of_ns=as_of_ns, limit=8,
    ).entries


def _supported_replay_fixture(repo: OpsRepository):
    # Reuse the accepted S18 builder so this path tests the existing replay,
    # payoff, decision-calendar and MaturedOutcomeV2 validators together.
    from tests.v2.test_session018_remediation import _payoff_case

    return _payoff_case(repo)


def test_restart_before_horizon_is_idempotent_and_status_is_deduplicated(
    tmp_path: Path, monkeypatch,
) -> None:
    path = tmp_path / "restart.sqlite"
    now = 1_800_000_000_000_000 + 10
    with OpsRepository(path) as repository:
        decision = _calendar(repository, 1)
        monkeypatch.setattr(
            coordinator, "resolve_decision_outcome",
            lambda _repo, _entry, _now: _resolution(decision, "PENDING", horizon=now + 100),
        )
        first = coordinator.run_outcome_maturity_cycle(repository, now)
        assert first.pending_count == 1
        assert first.outcomes_indexed == 0
        assert first.checkpoint_ref is not None

    with OpsRepository(path) as restarted:
        same_time = coordinator.run_outcome_maturity_cycle(restarted, now)
        assert same_time.failure_code == "CHECKPOINT_CLOCK_NOT_ADVANCED"
        coordinator.run_outcome_maturity_cycle(restarted, now + 1)  # wrap after the first page
        resumed = coordinator.run_outcome_maturity_cycle(restarted, now + 2)
        assert resumed.pending_count == 1
        assert len(_status_entries(restarted, decision.content_hash, as_of_ns=now + 2)) == 1
        assert _outcome_entries(restarted, decision.content_hash, as_of_ns=now + 2) == ()


def test_supported_matured_outcome_indexes_once_and_restart_revalidates_same_identity(
    tmp_path: Path, monkeypatch,
) -> None:
    with OpsRepository(tmp_path / "matured.sqlite") as repository:
        _case, _action, _payoff, outcome = _supported_replay_fixture(repository)
        decision_ref = outcome.decision_ref
        indexed = repository.get_artifact(decision_ref)
        assert indexed is not None
        decision = DecisionCalendarEntryV2.from_dict(json_value(indexed.metadata["decision_entry"]))
        monkeypatch.setattr(
            coordinator, "resolve_decision_outcome",
            lambda _repo, _entry, _now: _resolution(
                decision, "MATURED", horizon=outcome.horizon_end_ns, outcome=outcome,
            ),
        )
        first = coordinator.run_outcome_maturity_cycle(repository, outcome.available_at_ns)
        assert first.outcomes_attempted == 1
        assert first.outcomes_indexed == 1
        assert first.matured_count == 1
        assert first.failure_code is None

        # One empty page wraps the immutable keyset, then the original decision
        # is encountered again and the existing validator proves idempotency.
        coordinator.run_outcome_maturity_cycle(repository, outcome.available_at_ns + 1)
        repeated = coordinator.run_outcome_maturity_cycle(repository, outcome.available_at_ns + 2)
        assert repeated.outcomes_attempted == 1
        assert repeated.outcomes_indexed == 0
        assert repeated.matured_count == 1
        assert tuple(entry.artifact_ref for entry in _outcome_entries(
            repository, decision_ref, as_of_ns=outcome.available_at_ns + 2
        )) == (outcome.content_hash,)
        statuses = _status_entries(repository, decision_ref, as_of_ns=outcome.available_at_ns + 2)
        assert len(statuses) == 1


def test_conflicting_indexed_outcomes_fail_closed_without_resolver_call(
    tmp_path: Path, monkeypatch,
) -> None:
    with OpsRepository(tmp_path / "conflict.sqlite") as repository:
        _case, _action, _payoff, outcome = _supported_replay_fixture(repository)
        conflicting = replace(outcome, reason="conflicting immutable history")
        for item in (outcome, conflicting):
            repository.register_artifact(ArtifactIndexEntryV2(
                item.content_hash, "MaturedOutcomeV2", item.content_hash,
                item.matured_at_ns, item.available_at_ns, {"outcome": item.to_dict()},
            ))
        monkeypatch.setattr(
            coordinator, "resolve_decision_outcome",
            lambda *_args: (_ for _ in ()).throw(AssertionError("conflict must stop resolution")),
        )
        report = coordinator.run_outcome_maturity_cycle(repository, outcome.available_at_ns + 1)
        assert report.conflicting_decisions == 1
        assert report.unresolved_count == 1
        assert report.outcomes_indexed == 0
        assert report.failure_code is None


def test_future_available_outcome_is_not_indexed(tmp_path: Path, monkeypatch) -> None:
    with OpsRepository(tmp_path / "future.sqlite") as repository:
        _case, _action, _payoff, outcome = _supported_replay_fixture(repository)
        future_now = outcome.available_at_ns - 1
        future_outcome = replace(outcome, available_at_ns=future_now + 1)
        indexed = repository.get_artifact(outcome.decision_ref)
        assert indexed is not None
        decision = DecisionCalendarEntryV2.from_dict(json_value(indexed.metadata["decision_entry"]))
        monkeypatch.setattr(
            coordinator, "resolve_decision_outcome",
            lambda _repo, _entry, _now: _resolution(
                decision, "MATURED", horizon=future_outcome.horizon_end_ns, outcome=future_outcome,
            ),
        )
        report = coordinator.run_outcome_maturity_cycle(repository, future_now)
        assert report.outcomes_indexed == 0
        assert report.unresolved_count == 1
        assert _outcome_entries(repository, outcome.decision_ref, as_of_ns=future_now) == ()


def test_unresolved_then_matured_statuses_are_append_only(tmp_path: Path, monkeypatch) -> None:
    with OpsRepository(tmp_path / "append.sqlite") as repository:
        _case, _action, _payoff, outcome = _supported_replay_fixture(repository)
        indexed = repository.get_artifact(outcome.decision_ref)
        assert indexed is not None
        decision = DecisionCalendarEntryV2.from_dict(json_value(indexed.metadata["decision_entry"]))
        current = {"status": "UNRESOLVED"}

        def resolver(_repo: Any, _entry: Any, _now: int) -> OutcomeResolutionV1:
            if current["status"] == "UNRESOLVED":
                return _resolution(decision, "UNRESOLVED", horizon=outcome.horizon_end_ns,
                                   reason="REPLAY_EVIDENCE_LATE")
            return _resolution(decision, "MATURED", horizon=outcome.horizon_end_ns, outcome=outcome)

        monkeypatch.setattr(coordinator, "resolve_decision_outcome", resolver)
        first_time = outcome.horizon_end_ns + 1
        first = coordinator.run_outcome_maturity_cycle(repository, first_time)
        assert first.unresolved_count == 1
        assert first.maturable_count == 1
        assert first.oldest_maturable_age_ns == first_time - decision.decision_at_ns
        coordinator.run_outcome_maturity_cycle(repository, first_time + 1)  # wrap
        current["status"] = "MATURED"
        final = coordinator.run_outcome_maturity_cycle(repository, outcome.available_at_ns + 2)
        assert final.outcomes_indexed == 1
        status_entries = _status_entries(repository, outcome.decision_ref,
                                         as_of_ns=outcome.available_at_ns + 2)
        assert len(status_entries) == 2
        states = {
            entry.metadata["status"]["status"]: entry.metadata["status"]
            for entry in status_entries
        }
        assert states["UNRESOLVED"]["reason_code"] == "REPLAY_EVIDENCE_LATE"
        assert states["MATURED"]["reason_code"] == "TERMINAL_OUTCOME_SUPPORTED"
        assert states["UNRESOLVED"]["observed_at_ns"] < states["MATURED"]["observed_at_ns"]


def test_bounded_pages_process_every_decision_without_starving_older_rows(
    tmp_path: Path, monkeypatch,
) -> None:
    with OpsRepository(tmp_path / "bounds.sqlite") as repository:
        decisions = tuple(_calendar(repository, index) for index in range(40))
        seen: list[str] = []

        def resolver(_repo: Any, indexed: ArtifactIndexEntryV2, _now: int) -> OutcomeResolutionV1:
            seen.append(indexed.artifact_ref)
            decision = DecisionCalendarEntryV2.from_dict(json_value(indexed.metadata["decision_entry"]))
            return _resolution(decision, "UNSUPPORTED", reason="NO_PREDECLARED_TARGET")

        monkeypatch.setattr(coordinator, "resolve_decision_outcome", resolver)
        now = 1_800_000_000_000_100
        reports = tuple(
            coordinator.run_outcome_maturity_cycle(repository, now + offset)
            for offset in range(1, 6)
        )
        assert all(report.decisions_inspected == coordinator.DECISION_PAGE_SIZE for report in reports)
        assert all(report.outcomes_attempted == coordinator.MAX_OUTCOMES_ATTEMPTED for report in reports)
        assert all(report.unsupported_count == coordinator.MAX_OUTCOMES_ATTEMPTED for report in reports)
        assert all(report.unresolved_count == 0 for report in reports)
        assert all(report.bounded_work_exhausted for report in reports)
        assert all(report.artifact_pages_read <= coordinator.MAX_ARTIFACT_PAGES_READ for report in reports)
        assert all(report.raw_evidence_rows_inspected <= coordinator.MAX_RAW_EVIDENCE_ROWS_PER_CYCLE
                   for report in reports)
        assert all(report.replay_artifacts_resolved <= coordinator.MAX_REPLAY_ARTIFACTS_PER_CYCLE
                   for report in reports)
        assert all(report.retained_work_items <= coordinator.MAX_RETAINED_IN_MEMORY_WORK_ITEMS
                   for report in reports)
        assert set(seen) == {decision.content_hash for decision in decisions}


def test_malformed_calendar_is_retained_as_unresolved_and_never_labeled(
    tmp_path: Path, monkeypatch,
) -> None:
    with OpsRepository(tmp_path / "malformed.sqlite") as repository:
        body = {"version": "not-a-decision-entry", "future": True}
        ref = sha256_json(body)
        repository.register_artifact(ArtifactIndexEntryV2(
            ref, "DecisionCalendarEntryV2", ref, 1_800_000_000_000_000,
            1_800_000_000_000_000, {"decision_entry": body},
        ))
        monkeypatch.setattr(
            coordinator, "resolve_decision_outcome",
            lambda *_args: (_ for _ in ()).throw(AssertionError("malformed calendar must not resolve")),
        )
        now = 1_800_000_000_000_100
        report = coordinator.run_outcome_maturity_cycle(repository, now)
        assert report.invalid_calendar_entries == 1
        assert report.outcomes_attempted == 0
        assert _outcome_entries(repository, ref, as_of_ns=now) == ()
        statuses = _status_entries(repository, ref, as_of_ns=now)
        assert len(statuses) == 1
        assert statuses[0].metadata["status"]["reason_code"] == "MALFORMED_CALENDAR_ENTRY"


def test_resolver_failure_keeps_sealed_receipt_unchanged_and_sanitizes_error(
    tmp_path: Path, monkeypatch,
) -> None:
    with OpsRepository(tmp_path / "receipt.sqlite") as repository:
        decision = _calendar(repository, 1)
        receipt_body = {"sealed": "unchanged", "decision_ref": decision.content_hash}
        receipt_ref = sha256_json(receipt_body)
        repository.register_artifact(ArtifactIndexEntryV2(
            receipt_ref, "OpsSupervisorReceiptV1", receipt_ref,
            decision.created_at_ns, decision.available_at_ns, {"receipt": receipt_body},
        ))
        before = repository.get_artifact(receipt_ref)
        assert before is not None

        def fail_with_payload(*_args):
            raise ValueError("external secret payload must not be persisted")

        monkeypatch.setattr(coordinator, "resolve_decision_outcome", fail_with_payload)
        report = coordinator.run_outcome_maturity_cycle(repository, decision.available_at_ns + 5)
        after = repository.get_artifact(receipt_ref)
        assert after == before
        assert report.failure_code == "RESOLUTION_OR_INDEX_FAILURE"
        assert report.failure_type == "VALUEERROR"
        assert "secret payload" not in str(report.to_dict())
        assert _outcome_entries(repository, decision.content_hash,
                                as_of_ns=decision.available_at_ns + 5) == ()
