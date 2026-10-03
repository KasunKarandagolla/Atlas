"""Creation-ordered identity lookups inspect a fixed causal candidate window."""

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.memory import repository as repository_module
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository


def _entry(number, *, available=None):
    body = {"checkpoint": {"run_id": "run", "number": number}}
    ref = sha256_json(body)
    return ArtifactIndexEntryV2(ref, "ResearchPredictionOutcomeCheckpointV1", ref,
        number, number if available is None else available, body)


def _query(repo, cutoff=4000, **kwargs):
    return repo.artifact_entries_by_metadata_identity("ResearchPredictionOutcomeCheckpointV1",
        ("checkpoint", "run_id"), "run", as_of_ns=cutoff, limit=1, **kwargs)


def test_creation_order_identity_lookup_has_fixed_work_and_no_history_sort(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        repo.register_artifacts(tuple(_entry(i) for i in range(1100)))

        def measure():
            steps = 0
            queries = []

            def progress():
                nonlocal steps
                steps += 1
                return int(steps > 20000)

            repo._connection.set_progress_handler(progress, 1)
            repo._connection.set_trace_callback(queries.append)
            try:
                result = _query(repo)
            finally:
                repo._connection.set_progress_handler(None, 0)
                repo._connection.set_trace_callback(None)
            return result, steps, queries

        _, baseline, _ = measure()
        repo.register_artifacts(tuple(_entry(i) for i in range(1100, 3100)))
        result, grown, queries = measure()
        assert result.entries == (_entry(3099),) and result.has_more
        assert grown <= baseline + 30
        plan = " ".join(str(row[3]) for row in repo._connection.execute("EXPLAIN QUERY PLAN " + queries[0]))
        assert "created_metadata_" in plan and "USE TEMP B-TREE" not in plan
        assert _query(repo, after=(3099, _entry(3099).artifact_ref)).entries == (_entry(3098),)


def test_excessive_unavailable_prefix_refuses_instead_of_sweeping(tmp_path, monkeypatch):
    monkeypatch.setattr(repository_module, "_METADATA_IDENTITY_RAW_LIMIT", 4)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        repo.register_artifacts((_entry(1), *(_entry(i, available=100) for i in range(2, 7))))
        with pytest.raises(ValueError, match="METADATA_IDENTITY_CAUSAL_WINDOW_OVERFLOW"):
            _query(repo, cutoff=10)
        assert _query(repo, cutoff=100).entries == (_entry(6, available=100),)
