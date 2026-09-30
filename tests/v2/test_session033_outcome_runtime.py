from __future__ import annotations

from pathlib import Path
from typing import Any

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.ops_supervisor import (
    OpsCycleBatchV1,
    OpsCyclePortV1,
    OpsDecisionEventV1,
    OpsDecisionResultV1,
    OpsRecoverySnapshotV1,
    OpsRunResultV1,
    OpsSupervisorV2,
)


class _Clock:
    def __init__(self, now_ns: int = 1_800_000_000_000_000_000) -> None:
        self.now_ns = now_ns

    def __call__(self) -> int:
        return self.now_ns


class _SingleEventPort(OpsCyclePortV1):
    def __init__(self, now_ns: int) -> None:
        self.event = OpsDecisionEventV1(
            sha256_json({"session033_event": now_ns}),
            "BAR_CLOSE_15M",
            "FIXTURE",
            sha256_json({"session033_trigger": now_ns}),
            now_ns,
            None,
            now_ns,
            now_ns,
            now_ns,
            now_ns + 1_000,
            (),
        )

    def recover(self, repository: OpsRepository, *, now_ns: int) -> OpsRecoverySnapshotV1:
        return OpsRecoverySnapshotV1((), (), (), None, True, now_ns)

    def collect(
        self, repository: OpsRepository, *, now_ns: int, recovery: OpsRecoverySnapshotV1
    ) -> OpsCycleBatchV1:
        return OpsCycleBatchV1((self.event,), (), (), (), True, now_ns)

    def process_event(
        self,
        repository: OpsRepository,
        event: OpsDecisionEventV1,
        *,
        now_ns: int,
        source_health_state: str,
        completed_stages: Any,
        checkpoint: Any,
    ) -> OpsDecisionResultV1:
        raise AssertionError("fixture source health must close the event before pipeline processing")


def test_downstream_maintenance_uses_writer_after_receipt_is_sealed(tmp_path: Path) -> None:
    now_ns = 1_800_000_000_000_000_000
    clock = _Clock(now_ns)
    seen: list[OpsRepository] = []

    def maintenance(repository: OpsRepository, attempted_at_ns: int) -> None:
        assert attempted_at_ns == now_ns
        assert len(repository.artifact_entries("OpsSupervisorCycleReceiptV1")) == 1
        assert len(repository.artifact_entries("OpsSupervisorReceiptV1")) == 1
        seen.append(repository)
        body = {"attempted_at_ns": attempted_at_ns}
        ref = sha256_json(body)
        repository.register_artifact(
            ArtifactIndexEntryV2(ref, "S33MaintenanceProbeV1", ref, attempted_at_ns, attempted_at_ns, body)
        )

    supervisor = OpsSupervisorV2(
        tmp_path / "ops.sqlite",
        _SingleEventPort(now_ns),
        clock_ns=clock,
        post_cycle_maintenance=maintenance,
    )
    with supervisor:
        result = supervisor.run_once()
        assert supervisor.repository is seen[0]
    assert len(result.event_receipts) == 1
    with OpsRepository(tmp_path / "ops.sqlite", read_only=True) as reader:
        assert len(reader.artifact_entries("S33MaintenanceProbeV1")) == 1


def test_maintenance_failure_is_sanitized_and_cannot_change_decision_receipt(tmp_path: Path) -> None:
    now_ns = 1_800_000_000_000_000_000
    secret = "external-content-that-must-not-be-persisted"

    baseline = OpsSupervisorV2(
        tmp_path / "baseline.sqlite",
        _SingleEventPort(now_ns),
        clock_ns=_Clock(now_ns),
        post_cycle_maintenance=lambda _repository, _attempted_at_ns: None,
    )
    with baseline:
        baseline_result: OpsRunResultV1 = baseline.run_once()

    def fail(_repository: OpsRepository, _attempted_at_ns: int) -> None:
        raise RuntimeError(secret)

    supervisor = OpsSupervisorV2(
        tmp_path / "ops.sqlite",
        _SingleEventPort(now_ns),
        clock_ns=_Clock(now_ns),
        post_cycle_maintenance=fail,
    )
    with supervisor:
        result: OpsRunResultV1 = supervisor.run_once()

    assert len(result.event_receipts) == 1
    assert result.cycle == baseline_result.cycle
    assert result.event_receipts == baseline_result.event_receipts
    assert result.cycle.failure_types == ()
    with OpsRepository(tmp_path / "ops.sqlite", read_only=True) as reader:
        receipt_rows = reader.artifact_entries("OpsSupervisorReceiptV1")
        failures = reader.artifact_entries("OpsOutcomeMaturityFailureV1")
        assert len(receipt_rows) == 1
        assert len(failures) == 1
        assert failures[0].metadata["failure_type"] == "RuntimeError"
        assert failures[0].metadata["reason_code"] == "OUTCOME_MATURITY_CYCLE_FAILED"
        assert secret not in str(failures[0].metadata)


def test_default_supervisor_uses_outcome_maturity_coordinator(tmp_path: Path, monkeypatch: Any) -> None:
    from atlas.v2.runtime import outcome_maturity

    seen: list[tuple[OpsRepository, int]] = []

    def maintenance(repository: OpsRepository, now_ns: int) -> object:
        seen.append((repository, now_ns))
        return outcome_maturity.OutcomeMaturityCycleReportV1(
            cycle_at_ns=now_ns,
            decisions_inspected=3,
            pending_count=1,
            unresolved_count=2,
            bounded_work_exhausted=True,
        )

    monkeypatch.setattr(outcome_maturity, "run_outcome_maturity_cycle", maintenance)
    now_ns = 1_800_000_000_000_000_000
    supervisor = OpsSupervisorV2(tmp_path / "ops.sqlite", _SingleEventPort(now_ns), clock_ns=_Clock(now_ns))
    with supervisor:
        supervisor.run_once()
        assert len(seen) == 1
        assert seen[0][0] is supervisor.repository
        assert seen[0][1] == now_ns
    with OpsRepository(tmp_path / "ops.sqlite", read_only=True) as reader:
        reports = reader.artifact_entries("OutcomeMaturityCycleReportV1")
        assert len(reports) == 1
        assert reports[0].metadata["report"]["pending_count"] == 1
        assert reports[0].metadata["report"]["unresolved_count"] == 2
        assert reports[0].available_at_ns >= reports[0].metadata["report"]["cycle_at_ns"]


def test_metadata_identity_lookup_is_exact_bounded_and_causal(tmp_path: Path) -> None:
    path = tmp_path / "identity.sqlite"
    identity = sha256_json({"action": "exact"})
    with OpsRepository(path) as repository:
        for created_at_ns, available_at_ns, selected_identity in (
            (10, 10, identity),
            (20, 20, identity),
            (30, 30, identity),
            (15, 15, sha256_json({"action": "other"})),
        ):
            body = {"payoff": {"action_hash": selected_identity}}
            ref = sha256_json({"created": created_at_ns, "body": body})
            repository.register_artifact(ArtifactIndexEntryV2(
                ref, "PolicyPayoffV2", ref, created_at_ns, available_at_ns, body
            ))

    with OpsRepository(path, read_only=True) as reader:
        first = reader.artifact_entries_by_metadata_identity(
            "PolicyPayoffV2", ("payoff", "action_hash"), identity,
            as_of_ns=20, limit=1,
        )
        assert len(first.entries) == 1 and first.has_more
        assert first.entries[0].available_at_ns == 20
        assert first.next_cursor is not None
        second = reader.artifact_entries_by_metadata_identity(
            "PolicyPayoffV2", ("payoff", "action_hash"), identity,
            as_of_ns=20, after=first.next_cursor, limit=1,
        )
        assert len(second.entries) == 1 and not second.has_more
        assert second.entries[0].available_at_ns == 10
        assert not reader.artifact_entries_by_metadata_identity(
            "PolicyPayoffV2", ("payoff", "action_hash"), identity,
            as_of_ns=9, limit=2,
        ).entries
        plan = reader._connection.execute(
            "EXPLAIN QUERY PLAN SELECT * FROM artifact_index WHERE artifact_type='PolicyPayoffV2' "
            "AND CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.payoff.action_hash') END=? "
            "AND available_at_ns<=? ORDER BY created_at_ns DESC,artifact_ref DESC LIMIT ?",
            (identity, 20, 2),
        ).fetchall()
        assert any("artifact_policy_payoff_action_lookup" in row["detail"] for row in plan)


def test_repository_restores_additive_lookup_indexes_on_existing_schema(tmp_path: Path) -> None:
    path = tmp_path / "existing-v1-schema.sqlite"
    with OpsRepository(path) as repository:
        repository._connection.execute("DROP INDEX artifact_policy_payoff_action_lookup")

    with OpsRepository(path) as repository:
        indexes = {
            row["name"] for row in repository._connection.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            ).fetchall()
        }
        assert "artifact_policy_payoff_action_lookup" in indexes
