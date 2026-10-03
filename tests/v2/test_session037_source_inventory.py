"""Source inventory work depends on source count, not retained observation count."""

from __future__ import annotations

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository, SourceHealthV2
from atlas.v2.runtime import outcome_maturity


def _seed(repository: OpsRepository, kind: str, source_ids: tuple[str, ...], repetitions: int) -> None:
    if kind == "health":
        with repository.atomic_composition():
            for source_id in source_ids:
                for at in range(repetitions):
                    repository.record_source_health(SourceHealthV2(source_id, at, at, "HEALTHY_CURRENT"))
        return
    entries = []
    for source_id in source_ids:
        for at in range(repetitions):
            metadata = ({"source_id": source_id} if kind == "observation" else
                        {"reconciliation": {"source_id": source_id}})
            digest = sha256_json({"kind": kind, "source_id": source_id, "at": at})
            entries.append(ArtifactIndexEntryV2(digest,
                "PublicObservationIndexV2" if kind == "observation" else "OpsPublicSourceReconciliationV1",
                digest, at, at, metadata))
    repository.register_artifacts(tuple(entries))


def _sources(repository: OpsRepository, kind: str, *, limit: int = 128) -> tuple[str, ...]:
    if kind == "health":
        return repository.source_health_sources(limit=limit)
    if kind == "observation":
        return repository.public_observation_source_ids(limit=limit)
    return repository.public_reconciliation_source_ids(limit=limit)


@pytest.mark.parametrize("kind", ["health", "observation", "reconciliation"])
def test_duplicate_history_does_not_increase_inventory_query_work(tmp_path, kind) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        _seed(repository, kind, ("PUBLIC_A", "PUBLIC_Z"), 1)

        def measured_inventory() -> tuple[tuple[str, ...], int, list[str]]:
            instructions = 0
            queries: list[str] = []

            def progress() -> int:
                nonlocal instructions
                instructions += 1
                return int(instructions > 1000)

            repository._connection.set_trace_callback(queries.append)
            repository._connection.set_progress_handler(progress, 1)
            try:
                values = _sources(repository, kind)
            finally:
                repository._connection.set_trace_callback(None)
                repository._connection.set_progress_handler(None, 0)
            return values, instructions, queries

        expected, small_work, _ = measured_inventory()
        _seed(repository, kind, ("PUBLIC_A", "PUBLIC_Z"), 1500)
        actual, large_work, queries = measured_inventory()
        assert actual == expected == ("PUBLIC_A", "PUBLIC_Z")
        assert large_work <= small_work + 20
        assert len(queries) == 3
        for query in queries:
            details = " ".join(str(row[3]) for row in repository._connection.execute(
                "EXPLAIN QUERY PLAN " + query).fetchall())
            assert "SEARCH" in details
            assert "USE TEMP B-TREE" not in details
            if kind != "health":
                assert f"public_{kind}_source_id_lookup" in details


@pytest.mark.parametrize("kind", ["health", "observation", "reconciliation"])
def test_inventory_overflow_is_explicit_and_restart_stable(tmp_path, kind) -> None:
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repository:
        _seed(repository, kind, ("PUBLIC_A", "PUBLIC_B", "PUBLIC_C"), 2)
        with pytest.raises(ValueError, match="exceeded"):
            _sources(repository, kind, limit=2)
        assert _sources(repository, kind, limit=3) == ("PUBLIC_A", "PUBLIC_B", "PUBLIC_C")
    with OpsRepository(path, read_only=True) as repository:
        with pytest.raises(ValueError, match="exceeded"):
            _sources(repository, kind, limit=2)
        assert _sources(repository, kind, limit=3) == ("PUBLIC_A", "PUBLIC_B", "PUBLIC_C")


@pytest.mark.parametrize("kind", ["health", "observation", "reconciliation"])
def test_inventory_bound_and_empty_store(tmp_path, kind) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        assert _sources(repository, kind) == ()
        for limit in (0, 2001, True):
            with pytest.raises(ValueError, match="bound"):
                _sources(repository, kind, limit=limit)


@pytest.mark.parametrize("kind", ["observation", "reconciliation"])
@pytest.mark.parametrize("source_id", ["", "  ", {"ambiguous": "source"}])
def test_invalid_indexed_source_is_not_silently_accepted(tmp_path, kind, source_id) -> None:
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        metadata = ({"source_id": source_id} if kind == "observation" else
                    {"reconciliation": {"source_id": source_id}})
        digest = sha256_json(metadata)
        repository.register_artifact(ArtifactIndexEntryV2(digest,
            "PublicObservationIndexV2" if kind == "observation" else "OpsPublicSourceReconciliationV1",
            digest, 1, 1, metadata))
        with pytest.raises(ValueError, match="inventory is invalid"):
            _sources(repository, kind)


def test_admitted_action_source_population_and_metadata_fit_finite_read_budget(tmp_path) -> None:
    # A maximum admitted replay source has these distinct evidence populations,
    # in addition to exact action/calendar/scenario/fee identities. Repeated
    # resolver access to immutable rows must share the same per-decision cache.
    populations = {"raw": 1024, "minute": 256, "funding": 256,
                   "closed_bar": 256, "closed_bar_raw": 256, "identity": 32}
    entries = []
    for kind, count in populations.items():
        for index in range(count):
            digest = sha256_json({"kind": kind, "index": index})
            entries.append(ArtifactIndexEntryV2(digest, "PublicEvidenceFixtureV1", digest, 1, 1, {}))
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        repository.register_artifacts(tuple(entries))
        totals = outcome_maturity._CycleReadBudget()
        budgeted = outcome_maturity._BudgetedRepository(repository, totals)
        budgeted.begin_resolver()
        for entry in entries:
            assert budgeted.get_artifact(entry.artifact_ref) == entry
        recorded_rows = totals.rows
        for entry in entries:
            assert budgeted.get_artifact(entry.artifact_ref) == entry
        assert totals.rows == recorded_rows == 2080
        with pytest.raises(outcome_maturity._ReadBudgetExceeded):
            for index in range(4096):
                budgeted.get_artifact(sha256_json({"additional_identity": index}))
