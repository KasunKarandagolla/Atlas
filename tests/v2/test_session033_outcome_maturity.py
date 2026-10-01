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


def _malformed_calendar_raw_row(
    repo: OpsRepository, index: int, created_at_ns: int, *, artifact_ref: str | None = None,
) -> tuple[int, str]:
    ref = artifact_ref if artifact_ref is not None else sha256_json({"malformed_calendar_raw_row": index})
    repo._connection.execute(
        "INSERT INTO artifact_index(artifact_ref,artifact_type,content_hash,created_at_ns,available_at_ns,metadata_json) "
        "VALUES(?,?,?,?,?,?)",
        (ref, "DecisionCalendarEntryV2", ref, created_at_ns, created_at_ns, "{malformed-json"),
    )
    return created_at_ns, ref


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


def _run_cycle(repo: OpsRepository, evidence_cutoff_ns: int, **kwargs: Any):
    """Keep legacy fixture timestamps deterministic and inject both clocks."""
    return coordinator.run_outcome_maturity_cycle(
        repo,
        evidence_cutoff_ns=evidence_cutoff_ns,
        production_clock_ns=kwargs.pop("production_clock_ns", lambda: evidence_cutoff_ns),
        monotonic_ns=kwargs.pop("monotonic_ns", lambda: 0),
        **kwargs,
    )


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
            lambda _repo, _entry, _now, **_kwargs: _resolution(decision, "PENDING", horizon=now + 100),
        )
        first = _run_cycle(repository, now)
        assert first.pending_count == 1
        assert first.outcomes_indexed == 0
        assert first.checkpoint_ref is not None

    with OpsRepository(path) as restarted:
        same_time = _run_cycle(restarted, now)
        assert same_time.failure_code == "CHECKPOINT_CLOCK_NOT_ADVANCED"
        _run_cycle(restarted, now + 1)  # wrap after the first page
        resumed = _run_cycle(restarted, now + 2)
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
            lambda _repo, _entry, _now, **_kwargs: _resolution(
                decision, "MATURED", horizon=outcome.horizon_end_ns, outcome=outcome,
            ),
        )
        first = _run_cycle(repository, outcome.available_at_ns)
        assert first.outcomes_attempted == 1
        assert first.outcomes_indexed == 1
        assert first.matured_count == 1
        assert first.failure_code is None

        # One empty page wraps the immutable keyset, then the original decision
        # is encountered again and the existing validator proves idempotency.
        _run_cycle(repository, outcome.available_at_ns + 1)
        repeated = _run_cycle(repository, outcome.available_at_ns + 2)
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
        report = _run_cycle(repository, outcome.available_at_ns + 1)
        assert report.conflicting_decisions == 1
        assert report.unresolved_count == 1
        assert report.outcomes_indexed == 0
        assert report.failure_code is None


def test_derived_outcome_remains_invisible_until_its_production_time(tmp_path: Path, monkeypatch) -> None:
    with OpsRepository(tmp_path / "future.sqlite") as repository:
        _case, _action, _payoff, outcome = _supported_replay_fixture(repository)
        future_now = outcome.available_at_ns - 1
        future_outcome = replace(outcome, available_at_ns=future_now + 1)
        indexed = repository.get_artifact(outcome.decision_ref)
        assert indexed is not None
        decision = DecisionCalendarEntryV2.from_dict(json_value(indexed.metadata["decision_entry"]))
        monkeypatch.setattr(
            coordinator, "resolve_decision_outcome",
            lambda _repo, _entry, _now, **_kwargs: _resolution(
                decision, "MATURED", horizon=future_outcome.horizon_end_ns, outcome=future_outcome,
            ),
        )
        report = _run_cycle(repository, future_now)
        assert report.outcomes_indexed == 1
        assert report.unresolved_count == 0
        assert _outcome_entries(repository, outcome.decision_ref, as_of_ns=future_now) == ()
        assert len(_outcome_entries(repository, outcome.decision_ref, as_of_ns=future_now + 1)) == 1


def test_unresolved_then_matured_statuses_are_append_only(tmp_path: Path, monkeypatch) -> None:
    with OpsRepository(tmp_path / "append.sqlite") as repository:
        _case, _action, _payoff, outcome = _supported_replay_fixture(repository)
        indexed = repository.get_artifact(outcome.decision_ref)
        assert indexed is not None
        decision = DecisionCalendarEntryV2.from_dict(json_value(indexed.metadata["decision_entry"]))
        current = {"status": "UNRESOLVED"}

        def resolver(_repo: Any, _entry: Any, _now: int, **_kwargs: Any) -> OutcomeResolutionV1:
            if current["status"] == "UNRESOLVED":
                return _resolution(decision, "UNRESOLVED", horizon=outcome.horizon_end_ns,
                                   reason="REPLAY_EVIDENCE_LATE")
            current_outcome = replace(outcome, available_at_ns=_now)
            return _resolution(decision, "MATURED", horizon=outcome.horizon_end_ns, outcome=current_outcome)

        monkeypatch.setattr(coordinator, "resolve_decision_outcome", resolver)
        first_time = outcome.horizon_end_ns + 1
        first = _run_cycle(repository, first_time)
        assert first.unresolved_count == 1
        assert first.maturable_count == 1
        assert first.oldest_maturable_age_ns == first_time - decision.decision_at_ns
        _run_cycle(repository, first_time + 1)  # wrap
        current["status"] = "MATURED"
        final = _run_cycle(repository, outcome.available_at_ns + 2)
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

        def resolver(
            _repo: Any, indexed: ArtifactIndexEntryV2, _now: int, **_kwargs: Any,
        ) -> OutcomeResolutionV1:
            seen.append(indexed.artifact_ref)
            decision = DecisionCalendarEntryV2.from_dict(json_value(indexed.metadata["decision_entry"]))
            return _resolution(decision, "UNSUPPORTED", reason="NO_PREDECLARED_TARGET")

        monkeypatch.setattr(coordinator, "resolve_decision_outcome", resolver)
        now = 1_800_000_000_000_100
        reports = tuple(
            _run_cycle(repository, now + offset)
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
        assert all(report.maintenance_budget_status == "WITHIN_BUDGET" for report in reports)
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
        report = _run_cycle(repository, now)
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

        def fail_with_payload(*_args, **_kwargs):
            raise ValueError("external secret payload must not be persisted")

        monkeypatch.setattr(coordinator, "resolve_decision_outcome", fail_with_payload)
        report = _run_cycle(repository, decision.available_at_ns + 5)
        after = repository.get_artifact(receipt_ref)
        assert after == before
        assert report.failure_code == "RESOLUTION_OR_INDEX_FAILURE"
        assert report.failure_type == "VALUEERROR"
        assert "secret payload" not in str(report.to_dict())
        assert _outcome_entries(repository, decision.content_hash,
                                as_of_ns=decision.available_at_ns + 5) == ()


def test_malformed_only_page_is_accounted_then_restart_reaches_older_decision(
    tmp_path: Path, monkeypatch,
) -> None:
    base = 1_800_000_000_000_000
    path = tmp_path / "malformed-only-restart.sqlite"
    with OpsRepository(path) as repository:
        older = _calendar(repository, 1, at_ns=base)
        malformed_keys = tuple(
            _malformed_calendar_raw_row(
                repository, index, base + 100 + index,
                artifact_ref="" if index == 0 else None,
            )
            for index in range(coordinator.DECISION_PAGE_SIZE)
        )
        seen: list[str] = []

        def resolver(_repo: Any, entry: ArtifactIndexEntryV2, _cutoff: int, **_kwargs: Any):
            seen.append(entry.artifact_ref)
            decision = DecisionCalendarEntryV2.from_dict(json_value(entry.metadata["decision_entry"]))
            return _resolution(decision, "UNSUPPORTED", reason="NO_PREDECLARED_TARGET")

        monkeypatch.setattr(coordinator, "resolve_decision_outcome", resolver)
        first = _run_cycle(repository, base + 1_000)
        assert first.invalid_calendar_entries == coordinator.DECISION_PAGE_SIZE
        assert first.decisions_inspected == 0
        assert first.failure_code == "MALFORMED_CALENDAR_INDEX_ROWS"
        assert set(first.invalid_calendar_raw_keys) == set(malformed_keys)
        assert first.checkpoint_ref is not None
        checkpoint_entry = repository.get_artifact(first.checkpoint_ref)
        assert checkpoint_entry is not None
        checkpoint = coordinator.OutcomeMaturityCheckpointV1.from_dict(
            json_value(checkpoint_entry.metadata["checkpoint"]),
        )
        assert set(checkpoint.accounted_invalid_raw_keys) == set(malformed_keys)
        assert checkpoint.cursor == min(malformed_keys)
        assert seen == []

    with OpsRepository(path) as restarted:
        second = _run_cycle(restarted, base + 1_001)
        assert second.decisions_inspected == 1
        assert second.unsupported_count == 1
        assert second.invalid_calendar_entries == 0
        assert seen == [older.content_hash]


def test_malformed_and_valid_calendar_rows_interleaved_without_skips(
    tmp_path: Path, monkeypatch,
) -> None:
    base = 1_800_000_000_000_000
    with OpsRepository(tmp_path / "interleaved.sqlite") as repository:
        decisions = tuple(_calendar(repository, index, at_ns=base) for index in (2, 4, 6, 8))
        malformed = tuple(
            _malformed_calendar_raw_row(repository, index, base + index)
            for index in (3, 5, 7, 9)
        )
        seen: list[str] = []

        def resolver(_repo: Any, entry: ArtifactIndexEntryV2, _cutoff: int, **_kwargs: Any):
            seen.append(entry.artifact_ref)
            decision = DecisionCalendarEntryV2.from_dict(json_value(entry.metadata["decision_entry"]))
            return _resolution(decision, "UNSUPPORTED", reason="NO_PREDECLARED_TARGET")

        monkeypatch.setattr(coordinator, "resolve_decision_outcome", resolver)
        report = _run_cycle(repository, base + 1_000)
        assert report.invalid_calendar_entries == len(malformed)
        assert report.decisions_inspected == len(decisions)
        assert report.failure_code == "MALFORMED_CALENDAR_INDEX_ROWS"
        assert set(report.invalid_calendar_raw_keys) == set(malformed)
        assert set(seen) == {decision.content_hash for decision in decisions}
        assert len(seen) == len(decisions)
        assert report.outcomes_indexed == 0


def test_elapsed_budget_expires_before_next_decision_and_restart_resumes(
    tmp_path: Path, monkeypatch,
) -> None:
    base = 1_800_000_000_000_000
    path = tmp_path / "budget-restart.sqlite"
    with OpsRepository(path) as repository:
        decisions = tuple(_calendar(repository, index, at_ns=base) for index in (1, 2, 3))
        receipt_body = {"sealed": "unchanged", "decision_refs": [item.content_hash for item in decisions]}
        receipt_ref = sha256_json(receipt_body)
        repository.register_artifact(ArtifactIndexEntryV2(
            receipt_ref, "OpsSupervisorReceiptV1", receipt_ref, base, base, {"receipt": receipt_body},
        ))
        before = repository.get_artifact(receipt_ref)
        assert before is not None
        seen: list[str] = []

        class ExpiresBeforeSecondDecision:
            returned_from_first_resolver = False
            calls_after_return = 0

            def __call__(self) -> int:
                if not self.returned_from_first_resolver:
                    return 0
                self.calls_after_return += 1
                return {1: 98, 2: 99}.get(self.calls_after_return, 100)

        mono = ExpiresBeforeSecondDecision()

        def resolver(_repo: Any, entry: ArtifactIndexEntryV2, _cutoff: int, **_kwargs: Any):
            seen.append(entry.artifact_ref)
            decision = DecisionCalendarEntryV2.from_dict(json_value(entry.metadata["decision_entry"]))
            mono.returned_from_first_resolver = True
            return _resolution(decision, "UNSUPPORTED", reason="NO_PREDECLARED_TARGET")

        monkeypatch.setattr(coordinator, "resolve_decision_outcome", resolver)
        cutoff = base + 1_000
        first = coordinator.run_outcome_maturity_cycle(
            repository, evidence_cutoff_ns=cutoff, production_clock_ns=lambda: cutoff,
            monotonic_ns=mono, maintenance_budget_ns=100,
        )
        after = repository.get_artifact(receipt_ref)
        assert after == before
        assert first.maintenance_budget_status == "MAINTENANCE_BUDGET_EXHAUSTED"
        assert first.decisions_inspected == 1
        assert first.checkpoint_ref is not None
        assert len(seen) == 1
        checkpoint_entry = repository.get_artifact(first.checkpoint_ref)
        assert checkpoint_entry is not None
        checkpoint = coordinator.OutcomeMaturityCheckpointV1.from_dict(
            json_value(checkpoint_entry.metadata["checkpoint"]),
        )
        assert checkpoint.cursor == (decisions[-1].created_at_ns, decisions[-1].content_hash)
        assert checkpoint.cursor != (decisions[0].created_at_ns, decisions[0].content_hash)

    with OpsRepository(path) as restarted:
        second_cutoff = cutoff + 1
        second = coordinator.run_outcome_maturity_cycle(
            restarted, evidence_cutoff_ns=second_cutoff, production_clock_ns=lambda: second_cutoff,
            monotonic_ns=lambda: 0,
        )
        assert second.decisions_inspected == 2
        assert set(seen) == {decision.content_hash for decision in decisions}


def test_resolver_overrun_is_reported_and_completed_result_is_kept(
    tmp_path: Path, monkeypatch,
) -> None:
    base = 1_800_000_000_000_000
    with OpsRepository(tmp_path / "budget-overrun.sqlite") as repository:
        tuple(_calendar(repository, index, at_ns=base) for index in (1, 2))
        monotonic = [0]
        seen: list[str] = []

        def resolver(_repo: Any, entry: ArtifactIndexEntryV2, _cutoff: int, **_kwargs: Any):
            seen.append(entry.artifact_ref)
            monotonic[0] = 105
            decision = DecisionCalendarEntryV2.from_dict(json_value(entry.metadata["decision_entry"]))
            return _resolution(decision, "UNSUPPORTED", reason="NO_PREDECLARED_TARGET")

        monkeypatch.setattr(coordinator, "resolve_decision_outcome", resolver)
        cutoff = base + 1_000
        report = coordinator.run_outcome_maturity_cycle(
            repository, cutoff, production_clock_ns=lambda: cutoff,
            monotonic_ns=lambda: monotonic[0], maintenance_budget_ns=100,
        )
        assert report.maintenance_budget_status == "MAINTENANCE_DEADLINE_OVERRUN"
        assert report.maintenance_budget_overrun_ns == 5
        assert report.unsupported_count == 1
        assert report.decisions_inspected == 1
        assert len(seen) == 1
        assert report.checkpoint_ref is not None
        assert repository.get_artifact(seen[0]) is not None


def test_permanently_slow_first_item_does_not_starve_older_decisions(
    tmp_path: Path, monkeypatch,
) -> None:
    base = 1_800_000_000_000_000
    with OpsRepository(tmp_path / "slow-first.sqlite") as repository:
        decisions = tuple(_calendar(repository, index, at_ns=base) for index in (1, 2, 3))
        seen: list[str] = []
        monotonic = [0]

        def resolver(_repo: Any, entry: ArtifactIndexEntryV2, _cutoff: int, **_kwargs: Any):
            seen.append(entry.artifact_ref)
            monotonic[0] += 51
            decision = DecisionCalendarEntryV2.from_dict(json_value(entry.metadata["decision_entry"]))
            return _resolution(decision, "UNSUPPORTED", reason="NO_PREDECLARED_TARGET")

        monkeypatch.setattr(coordinator, "resolve_decision_outcome", resolver)
        for offset in range(3):
            monotonic[0] = 0
            cutoff = base + 1_000 + offset
            report = coordinator.run_outcome_maturity_cycle(
                repository, cutoff, production_clock_ns=lambda cutoff=cutoff: cutoff,
                monotonic_ns=lambda: monotonic[0], maintenance_budget_ns=50,
            )
            assert report.maintenance_budget_status == "MAINTENANCE_DEADLINE_OVERRUN"
            assert report.decisions_inspected == 1
        assert len(seen) == 3
        assert set(seen) == {decision.content_hash for decision in decisions}


def test_status_checkpoint_and_report_use_production_utc_after_fixed_cutoff(
    tmp_path: Path, monkeypatch,
) -> None:
    base = 1_800_000_000_000_000
    cutoff = base + 10
    with OpsRepository(tmp_path / "production-times.sqlite") as repository:
        decision = _calendar(repository, 1, at_ns=base)

        class AdvancingUTC:
            now = cutoff + 1

            def __call__(self) -> int:
                self.now += 1
                return self.now

        utc = AdvancingUTC()
        resolver_finished: list[int] = []

        def resolver(_repo: Any, entry: ArtifactIndexEntryV2, evidence_cutoff: int, *, clock_ns):
            assert evidence_cutoff == cutoff
            resolver_finished.append(clock_ns())
            parsed = DecisionCalendarEntryV2.from_dict(json_value(entry.metadata["decision_entry"]))
            return _resolution(parsed, "UNRESOLVED", reason="EVIDENCE_PENDING")

        monkeypatch.setattr(coordinator, "resolve_decision_outcome", resolver)
        report = coordinator.run_outcome_maturity_cycle(
            repository, evidence_cutoff_ns=cutoff, production_clock_ns=utc,
            monotonic_ns=lambda: 0,
        )
        statuses = _status_entries(repository, decision.content_hash, as_of_ns=report.cycle_at_ns)
        assert len(statuses) == 1
        status = statuses[0].metadata["status"]
        assert status["observed_at_ns"] >= resolver_finished[0] > cutoff
        assert report.evidence_cutoff_ns == cutoff
        assert report.computation_started_ns > cutoff
        assert report.computation_finished_ns == report.cycle_at_ns > status["observed_at_ns"]
        assert report.checkpoint_ref is not None
        checkpoint_entry = repository.get_artifact(report.checkpoint_ref)
        assert checkpoint_entry is not None
        checkpoint = coordinator.OutcomeMaturityCheckpointV1.from_dict(
            json_value(checkpoint_entry.metadata["checkpoint"]),
        )
        assert checkpoint.written_at_ns > cutoff


def test_evidence_arriving_during_maintenance_is_deferred_to_later_cutoff(
    tmp_path: Path, monkeypatch,
) -> None:
    base = 1_800_000_000_000_000
    cutoff = base + 100
    available_during_maintenance = cutoff + 1
    with OpsRepository(tmp_path / "deferred-evidence.sqlite") as repository:
        decision = _calendar(repository, 1, at_ns=base)
        future_ref = sha256_json({"future_evidence": True})
        observed: list[bool] = []
        registered = False

        def resolver(repo: Any, entry: ArtifactIndexEntryV2, evidence_cutoff: int, **_kwargs: Any):
            nonlocal registered
            if not registered:
                repo.register_artifact(ArtifactIndexEntryV2(
                    future_ref, "FixtureFutureEvidenceV1", future_ref,
                    available_during_maintenance, available_during_maintenance, {"fixture": True},
                ))
                registered = True
            page = repo.artifact_entries_by_types_page(
                ("FixtureFutureEvidenceV1",), as_of_ns=evidence_cutoff, limit=1,
            )
            observed.append(bool(page.entries))
            parsed = DecisionCalendarEntryV2.from_dict(json_value(entry.metadata["decision_entry"]))
            return _resolution(parsed, "UNSUPPORTED", reason="NO_PREDECLARED_TARGET")

        monkeypatch.setattr(coordinator, "resolve_decision_outcome", resolver)
        first = coordinator.run_outcome_maturity_cycle(
            repository, evidence_cutoff_ns=cutoff, production_clock_ns=lambda: cutoff + 2,
            monotonic_ns=lambda: 0,
        )
        assert first.unsupported_count == 1
        assert observed == [False]
        later_cutoff = cutoff + 3
        # First cycle wraps the completed one-row cursor; the following cycle
        # revisits it under the later cutoff and can now see the evidence.
        coordinator.run_outcome_maturity_cycle(
            repository, evidence_cutoff_ns=later_cutoff, production_clock_ns=lambda: later_cutoff + 1,
            monotonic_ns=lambda: 0,
        )
        next_cutoff = later_cutoff + 2
        coordinator.run_outcome_maturity_cycle(
            repository, evidence_cutoff_ns=next_cutoff,
            production_clock_ns=lambda: next_cutoff + 1, monotonic_ns=lambda: 0,
        )
        assert observed == [False, True]
        stored = repository.get_artifact(decision.content_hash)
        assert stored is not None
        assert DecisionCalendarEntryV2.from_dict(json_value(stored.metadata["decision_entry"])) == decision
