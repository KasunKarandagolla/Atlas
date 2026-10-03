"""Small active lookups cannot sort retained namespaces or restart frame history."""

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository


def _entry(kind, number, body):
    ref = sha256_json([kind, number, body])
    return ArtifactIndexEntryV2(ref, kind, ref, number, number, body)


def _measure(repo, operation):
    steps = 0
    queries = []

    def progress():
        nonlocal steps
        steps += 1
        return int(steps > 40000)

    repo._connection.set_progress_handler(progress, 1)
    repo._connection.set_trace_callback(queries.append)
    try:
        result = operation()
    finally:
        repo._connection.set_progress_handler(None, 0)
        repo._connection.set_trace_callback(None)
    for query in queries:
        plan = " ".join(str(row[3]) for row in repo._connection.execute("EXPLAIN QUERY PLAN " + query))
        assert "USE TEMP B-TREE" not in plan
    return result, steps


def test_generic_typed_creation_page_and_l2_restart_do_not_grow_with_history(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        def typed():
            return repo.artifact_entries_by_types_page(("TestKind",), as_of_ns=10000, limit=1)

        def l2():
            return repo.l2_archive_restart_checkpoints(limit=2)
        for i in range(100):
            repo.register_artifacts((_entry("TestKind", i, {}), _entry("L2FrameArchiveCheckpointV2", i,
                {"instrument_hash": "instrument", "source_id": "PUBLIC", "channel": "book"})))
        _, typed_baseline = _measure(repo, typed)
        _, l2_baseline = _measure(repo, l2)
        for i in range(100, 3100):
            repo.register_artifacts((_entry("TestKind", i, {}), _entry("L2FrameArchiveCheckpointV2", i,
                {"instrument_hash": "instrument", "source_id": "PUBLIC", "channel": "book"})))
        page, typed_grown = _measure(repo, typed)
        frames, l2_grown = _measure(repo, l2)
        assert page.entries[0].created_at_ns == 3099 and frames[0].created_at_ns == 3099
        assert typed_grown <= typed_baseline + 30
        assert l2_grown <= l2_baseline + 30


def test_native_m1_checkpoint_generation_uses_exact_index_without_history_sort(tmp_path):
    from .test_session014_core import KEY

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        def add(start, end):
            repo.register_artifacts(tuple(_entry("S3NativeM1OriginAccountingCheckpointV1", i,
                {"instrument_key_json": KEY.to_canonical_json(), "checkpoint": {"generation": i}})
                for i in range(start, end)))

        def operation():
            return repo.latest_native_m1_origin_accounting_checkpoint(KEY)
        add(1, 101)
        _, baseline = _measure(repo, operation)
        add(101, 3101)
        latest, grown = _measure(repo, operation)
        assert latest.metadata["checkpoint"]["generation"] == 3100
        assert grown <= baseline + 30


def test_l2_restart_successors_cross_each_stream_identity_level(tmp_path):
    streams = (("a", "s1", "c1"), ("a", "s1", "c2"), ("a", "s2", "c1"), ("b", "s1", "c1"))
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        repo.register_artifacts(tuple(_entry("L2FrameArchiveCheckpointV2", number,
            dict(zip(("instrument_hash", "source_id", "channel"), stream, strict=True)))
            for number in range(3) for stream in streams))
        entries, _ = _measure(repo, lambda: repo.l2_archive_restart_checkpoints(limit=4))
        assert len(entries) == 4
        assert {tuple(entry.metadata[field] for field in ("instrument_hash", "source_id", "channel"))
                for entry in entries} == set(streams)
        assert all(entry.created_at_ns == 2 for entry in entries)


def test_watch_inventory_and_transitions_use_bounded_ordered_seeks(tmp_path):
    from dataclasses import replace

    from atlas.v2.contracts import WatchStateV2

    from .test_memory import watch

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        def add(start, end):
            with repo._transaction():
                for number in range(start, end):
                    name = f"watch-{number:05d}"
                    repo.create_watch(replace(watch(name, expires=20000 + number),
                        created_at_ns=100 + number, updated_at_ns=100 + number,
                        last_evaluated_at_ns=100 + number))
                    repo.transition_watch(name, expected_state_version=0, event_id=f"event-{number}",
                        event_at_ns=110 + number, transition_at_ns=110 + number,
                        target_state=WatchStateV2.WAITING_FOR_EVENT if number % 2 else WatchStateV2.INVALIDATED,
                        reason=None if number % 2 else "TEST_DECLARED_INVALIDATION",
                        outbox_id=f"outbox-{number}")

        operations = (lambda: repo.list_active_watches(limit=2), lambda: repo.list_watches(limit=2),
                      lambda: repo.watch_transition_history(limit=2))
        add(0, 100)
        baseline = [_measure(repo, operation)[1] for operation in operations]
        add(100, 3100)
        grown = [_measure(repo, operation)[1] for operation in operations]
        assert all(actual <= before + 30 for before, actual in zip(baseline, grown, strict=True))
        assert [item.watch_id for item in repo.list_active_watches(limit=2)] == ["watch-00001", "watch-00003"]
        assert [item.watch_id for item in repo.list_watches(limit=2)] == ["watch-03098", "watch-03099"]


def test_native_s3_rejection_checks_exact_event_identities_without_namespace_sweeps(tmp_path, monkeypatch):
    from .test_session034_scientific_calendar_closure import _native_fixture, _process

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _product, _bar, event, _diagnostics, now, port = _native_fixture(repo)
        repo.register_artifacts(tuple(_entry(kind, number, body)
            for number in range(3000)
            for kind, body in (("CandidateSetDecisionIndexV1", {"decision_event_id": f"unrelated-{number}"}),
                ("CandidateSetV2", {"candidate_set": {"decision_event_id": f"unrelated-{number}"}}))))

        def forbidden(_kind):
            raise AssertionError("production must not scan a retained artifact namespace")

        monkeypatch.setattr(repo, "artifact_entries", forbidden)
        first, _ = _process(repo, event, now, port)
        repeated, _ = _process(repo, event, now, port)
        assert first.terminal_status == repeated.terminal_status
        page = repo.artifact_entries_by_metadata_identity("CandidateSetV2",
            ("candidate_set", "decision_event_id"), event.event_id, as_of_ns=now, limit=2)
        assert len(page.entries) == 1 and not page.has_more
        port.close()


def test_preflight_scope_filter_cannot_hide_an_overflowing_watch_inventory(tmp_path, monkeypatch):
    from atlas.v2.science.session031_preflight import _readiness_for_product

    from .test_memory import watch
    from .test_session034_scientific_calendar_closure import _product

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        monkeypatch.setattr(repo, "list_watches", lambda **_kwargs: (watch(),) * 10000)
        monkeypatch.setattr(repo, "watch_transition_history", lambda **_kwargs:
            ({"watch_id": "different-watch", "transition_at_ns": 110},) * 10000)
        readiness = _readiness_for_product(repo, _product(), 200)
        assert readiness["watch_state"]["watch_count"] == 0
        assert readiness["watch_state"]["watch_inventory_complete"] is False
        assert readiness["watch_state"]["transition_inventory_complete"] is False
        assert "S1_WATCH_HISTORY_INVENTORY_INCOMPLETE" in readiness["sleeves"]["S1"]["reason_codes"]
