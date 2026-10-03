"""Exact receipt seeks stay bounded as unrelated raw observations accumulate."""

from __future__ import annotations

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.memory import repository as repository_module
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository

REVISION = "a" * 64
KEY = '{"instrument":"BTCUSDT"}'


def _entry(number: int, *, kind: str = "TRADE", key: str = KEY,
           revision: str = REVISION, source_class: str = "ACTUAL_SYSTEM",
           available: int = 100, replay: int | None = None) -> ArtifactIndexEntryV2:
    record_id = sha256_json([number, kind, key, revision, source_class, available, replay])
    metadata = {"record_id": record_id, "instrument_revision": revision,
                "instrument_key_json": key, "event_type": kind,
                "availability_class": source_class, "event_at_ns": number,
                "replay_available_at_ns": replay}
    return ArtifactIndexEntryV2(record_id, "PublicObservationIndexV2", sha256_json(metadata),
                                available, available, metadata)


def _query(repo: OpsRepository, *, key: str | None = KEY,
           source_class: str | None = "ACTUAL_SYSTEM", cutoff: int = 100,
           limit: int = 5) -> tuple[ArtifactIndexEntryV2, ...]:
    return repo.public_archive_history_entries(instrument_revision=REVISION,
        event_types=("TRADE", "AGG_TRADE"), information_cutoff_ns=cutoff,
        limit=limit, instrument_key_json=key, availability_class=source_class)


@pytest.mark.parametrize("exact_key", [KEY, None])
def test_receipt_seek_does_not_scan_unrelated_retained_observations(tmp_path, exact_key):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        wanted = tuple(_entry(i, kind="TRADE" if i % 2 else "AGG_TRADE") for i in range(10))
        repo.register_artifacts(wanted)

        def measure():
            steps = 0
            queries = []

            def progress():
                nonlocal steps
                steps += 1
                return int(steps > 2000)

            repo._connection.set_progress_handler(progress, 1)
            repo._connection.set_trace_callback(queries.append)
            try:
                rows = _query(repo, key=exact_key)
            finally:
                repo._connection.set_progress_handler(None, 0)
                repo._connection.set_trace_callback(None)
            return rows, steps, queries

        expected, baseline, _ = measure()
        repo.register_artifacts(tuple(_entry(i + 100, kind="BAR_1M") for i in range(3000)))
        actual, grown, queries = measure()
        assert actual == expected
        assert grown <= baseline + 30
        assert len(queries) == 2
        for query in queries:
            plan = " ".join(str(row[3]) for row in repo._connection.execute(
                "EXPLAIN QUERY PLAN " + query))
            assert "SEARCH" in plan and "public_exact_receipt_" in plan
            assert "USE TEMP B-TREE" not in plan


def test_receipt_merge_preserves_global_receipt_order_full_identity_and_cutoff(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        wanted = tuple(_entry(i, kind="TRADE" if i % 2 else "AGG_TRADE", available=10 + i)
                       for i in range(10))
        rejected = (_entry(100, key='{"instrument":"ETHUSDT"}', available=95),
                    _entry(101, revision="b" * 64, available=96),
                    _entry(102, available=101),
                    _entry(103, source_class="RECONSTRUCTED_MARKET", available=97))
        repo.register_artifacts((*wanted, *rejected))
        assert _query(repo) == tuple(reversed(wanted[-5:]))
        # Omitting availability class retains the original actual-receipt
        # eligibility for both classes rather than changing to replay time.
        combined = _query(repo, source_class=None)
        assert combined[0] == rejected[-1]
        assert combined[1:] == tuple(reversed(wanted[-4:]))


def test_reconstructed_receipts_keep_actual_order_with_replay_eligibility(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        wanted = (_entry(1, source_class="RECONSTRUCTED_MARKET", available=500, replay=20),
                  _entry(2, kind="AGG_TRADE", source_class="RECONSTRUCTED_MARKET", available=400, replay=30),
                  _entry(3, source_class="RECONSTRUCTED_MARKET", available=600, replay=101))
        repo.register_artifacts(wanted)
        assert _query(repo, source_class="RECONSTRUCTED_MARKET") == wanted[:2]
        assert _query(repo, key=None, source_class="RECONSTRUCTED_MARKET") == wanted[:2]


def test_reconstructed_population_overflow_refuses_whole_query(tmp_path, monkeypatch):
    monkeypatch.setattr(repository_module, "_RECEIPT_REPLAY_CANDIDATE_LIMIT", 4)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        repo.register_artifacts(tuple(_entry(i, source_class="RECONSTRUCTED_MARKET",
            available=1000 + i, replay=i) for i in range(5)))
        with pytest.raises(ValueError, match="reconstructed receipt population exceeds"):
            _query(repo, source_class="RECONSTRUCTED_MARKET", limit=1)


def test_receipt_event_type_budget_is_explicit(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo, pytest.raises(
            ValueError, match="event-type population exceeds"):
        repo.public_archive_history_entries(instrument_revision=REVISION,
            event_types=tuple(f"TYPE_{i}" for i in range(17)), information_cutoff_ns=100,
            limit=1, instrument_key_json=KEY, availability_class="ACTUAL_SYSTEM")
