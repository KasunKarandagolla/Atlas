"""Restart reads exact public channel heads rather than retained cache snapshots."""

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository

KINDS = ("PublicStreamContinuityStateV1", "PublicStreamContinuityCheckpointV2")
IDENTITIES = tuple((symbol, prefix + symbol) for symbol in ("BTCUSDT", "ETHUSDT")
                   for prefix in ("orderbook.50.", "publicTrade."))


def test_stream_batch_commits_transport_once_before_interpretation(tmp_path, monkeypatch):
    import pytest

    from atlas.v2.runtime import production
    from atlas.v2.runtime.ops_supervisor import OpsSupervisorV2

    from .test_s38_sustained_public_stream import (
        FixturePublicSource,
        MixedWorkload,
        QueueStream,
        transport_rows,
    )
    from .test_session032_public_stream_integration import NOW_NS

    clock = [NOW_NS]
    stream = QueueStream(lambda: clock[0])
    port = production.create_bybit_public_ws_port(public_source=FixturePublicSource(),
        public_stream_source=stream, clock_ns=lambda: clock[0])
    path = tmp_path / "ops.sqlite"
    with OpsSupervisorV2(path, port, clock_ns=lambda: clock[0]) as supervisor:
        supervisor.run_once()
        repository = supervisor.repository
        workload = MixedWorkload()
        frames = tuple(workload.frame(clock[0] + number + 1) for number in range(32))
        for frame in frames:
            assert stream.handoff.offer(frame)
        clock[0] += 200_000_000
        statements = []
        repository._connection.set_trace_callback(statements.append)
        port._collect_public_stream_evidence(repository, now_ns=clock[0])
        repository._connection.set_trace_callback(None)
        assert sum(sql == "COMMIT" for sql in statements) == 2
        assert [row["raw_payload_bytes"] for row in transport_rows(repository, tmp_path)] == (
            [frame.raw_payload_bytes for frame in frames])
        # A failed typed interpretation must not roll back committed transport.
        more = tuple(workload.frame(clock[0] + number + 1) for number in range(32))
        for frame in more:
            assert stream.handoff.offer(frame)
        clock[0] += 200_000_000

        def fail_interpretation(*args, **kwargs):
            with OpsRepository(path, read_only=True) as reader:
                assert len(transport_rows(reader, tmp_path)) == 64
            raise ValueError("OFFLINE_TYPED_FAILURE")

        monkeypatch.setattr(port, "_persist_public_stream_batch", fail_interpretation)
        with pytest.raises(ValueError, match="OFFLINE_TYPED_FAILURE"):
            port._collect_public_stream_evidence(repository, now_ns=clock[0])
        assert port._stream_ingestion_failed
    with OpsRepository(path, read_only=True) as reader:
        assert [row["raw_payload_bytes"] for row in transport_rows(reader, tmp_path)] == (
            [frame.raw_payload_bytes for frame in (*frames, *more)])


def test_hourly_product_refresh_has_the_same_active_bound_as_restart(tmp_path):
    from dataclasses import replace
    from types import SimpleNamespace

    import pytest

    from atlas.v2.instruments import InstrumentRegistryV2
    from atlas.v2.runtime import production

    from .test_session032_public_stream_integration import BASE_NS, _contract

    first = _contract("BTCUSDT")
    registry = InstrumentRegistryV2()
    for offset in range(4096):
        registry.register(replace(first, effective_at_ns=BASE_NS + offset))
    source = SimpleNamespace(current_products=(first,))
    port = production.ProductionOpsCyclePortV1(public_source=source)
    port.clock_ns = lambda: BASE_NS + 8192
    port._collector_recovery = SimpleNamespace(collector=SimpleNamespace(
        registry=registry, clock_ns=port.clock_ns))
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        # An exact repeated receipt at capacity remains idempotent.
        port._register_refreshed_stream_products(repository, None)
        assert len(registry.contracts()) == 4096
        next_product = replace(first, effective_at_ns=BASE_NS + 4096)
        source.current_products = (next_product,)
        for _ in range(2):
            with pytest.raises(production.ActiveEvidenceOverflowV1):
                port._register_refreshed_stream_products(repository, None)
        assert len(registry.contracts()) == 4096
        assert repository.get_artifact(next_product.content_hash) is None
        pressure = repository.artifact_entries("OpsActiveWorkPressureV1")
        assert len(pressure) == 1
        assert pressure[0].metadata["pressure"]["limit"] == 4096
        assert pressure[0].metadata["pressure"]["authority"] == "ZERO"


def _entry(kind, number, identity, *, source="BYBIT_PUBLIC_WS", created=None):
    symbol, channel = identity
    state = {"instrument": {"native_symbol": symbol}, "channel": channel, "source_id": source}
    body = {"state": state} if kind == KINDS[0] else {"checkpoint": {"state": state}}
    ref = sha256_json([kind, number, body])
    return ArtifactIndexEntryV2(ref, kind, ref, number if created is None else created, number, body)


def _measure(repository, cutoff):
    steps = 0
    queries = []

    def progress():
        nonlocal steps
        steps += 1
        return int(steps > 4000)

    repository._connection.set_progress_handler(progress, 1)
    repository._connection.set_trace_callback(queries.append)
    try:
        result = repository.latest_stream_continuity_entries(as_of_ns=cutoff)
    finally:
        repository._connection.set_progress_handler(None, 0)
        repository._connection.set_trace_callback(None)
    assert len(queries) == 8
    for query in queries:
        plan = " ".join(str(row[3]) for row in repository._connection.execute("EXPLAIN QUERY PLAN " + query))
        assert "SEARCH artifact_index USING INDEX public_stream_continuity_head_" in plan
        assert "USE TEMP B-TREE" not in plan
        assert "SCAN artifact_index" not in plan
    return result, steps


def test_restart_head_work_does_not_grow_with_retained_or_future_snapshots(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        def add(start, end):
            repository.register_artifacts(tuple(_entry(kind, number, identity)
                for number in range(start, end) for kind in KINDS for identity in IDENTITIES))

        add(1, 11)
        baseline, before = _measure(repository, 10)
        assert len(baseline) == 8
        add(11, 2011)
        grown, after = _measure(repository, 2010)
        assert len(grown) == 8 and all(entry.available_at_ns == 2010 for entry in grown)
        past, future_steps = _measure(repository, 10)
        assert {entry.artifact_ref for entry in past} == {entry.artifact_ref for entry in baseline}
        assert after <= before + 64
        assert future_steps <= before + 64


def test_hot_book_tail_cannot_displace_other_channel_heads(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        repository.register_artifacts(tuple(_entry(kind, 1, identity)
            for kind in KINDS for identity in IDENTITIES))
        repository.register_artifacts(tuple(_entry(KINDS[0], number, IDENTITIES[0])
            for number in range(2, 10003)))
        result, _ = _measure(repository, 10002)
        assert len(result) == 8
        assert sum(entry.available_at_ns == 1 for entry in result) == 7
        assert max(entry.available_at_ns for entry in result) == 10002


def test_continuity_heads_keep_exact_scope_and_causal_availability_on_read_only_reopen(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repository:
        expected = _entry(KINDS[1], 10, IDENTITIES[0], created=1)
        future = _entry(KINDS[1], 30, IDENTITIES[0], created=2)
        other_source = _entry(KINDS[1], 20, IDENTITIES[0], source="OTHER_SOURCE")
        other_symbol = _entry(KINDS[1], 20, ("SOLUSDT", "orderbook.50.SOLUSDT"))
        other_channel = _entry(KINDS[1], 20, ("BTCUSDT", "orderbook.200.BTCUSDT"))
        repository.register_artifacts((expected, future, other_source, other_symbol, other_channel))
        with OpsRepository(path, read_only=True) as reader:
            before = repository._connection.total_changes
            result, _ = _measure(reader, 20)
            assert tuple(entry.artifact_ref for entry in result) == (expected.artifact_ref,)
            assert repository._connection.total_changes == before
            assert reader._connection.execute("PRAGMA query_only").fetchone()[0] == 1
        result, _ = _measure(repository, 30)
        assert tuple(entry.artifact_ref for entry in result) == (future.artifact_ref,)


def test_equal_publication_heads_preserve_actual_trade_and_recovery_progress(tmp_path):
    from dataclasses import replace

    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        expected = set()
        entries = []
        for kind in KINDS:
            for identity in IDENTITIES:
                base = _entry(kind, 10, identity)
                for count, epoch, gap in ((0, 0, 0), (10, 0, 0), (10, 1, 1), (10, 1, 2)):
                    state = {"instrument": {"native_symbol": identity[0]}, "channel": identity[1],
                        "source_id": "BYBIT_PUBLIC_WS", "last_available_at_ns": 10,
                        "last_transport_receipt_at_ns": 10, "observed_trade_count": count,
                        "recovery_epoch": epoch, "gap_count": gap}
                    body = {"state": state} if kind == KINDS[0] else {"checkpoint": {"state": state}}
                    ref = sha256_json([kind, body])
                    entries.append(replace(base, artifact_ref=ref, content_hash=ref, metadata=body))
                    if count == 10 and epoch == 1 and gap == 2:
                        expected.add(ref)
        repository.register_artifacts(tuple(entries))
        result, _ = _measure(repository, 10)
        assert {entry.artifact_ref for entry in result} == expected


def _trade_index(key, number, *, event_at=None, available_at=None, source="BYBIT_PUBLIC_WS"):
    available = number if available_at is None else available_at
    body = {"instrument_key_json": key.to_canonical_json(), "instrument_revision": key.contract_revision,
            "source_id": source, "event_type": "TRADE", "event_at_ns": number if event_at is None else event_at}
    ref = sha256_json(["trade-index", number, available, body])
    return ArtifactIndexEntryV2(ref, "PublicStreamTradeObservationIndexV1", ref, available, available, body)


def _indexed_work(repository, operation, index_name, *, compact=False):
    steps = 0
    queries = []

    def progress():
        nonlocal steps
        steps += 1
        return int(steps > 4000)

    repository._connection.set_progress_handler(progress, 1)
    repository._connection.set_trace_callback(queries.append)
    try:
        result = operation()
    finally:
        repository._connection.set_progress_handler(None, 0)
        repository._connection.set_trace_callback(None)
    assert len(queries) == (2 if compact else 1)
    plan = " ".join(str(row[3]) for row in repository._connection.execute("EXPLAIN QUERY PLAN " + queries[0]))
    assert "SEARCH artifact_index USING INDEX " + index_name in plan
    assert "USE TEMP B-TREE" not in plan and "SCAN artifact_index" not in plan
    if compact:
        compact_plan = " ".join(str(row[3]) for row in repository._connection.execute("EXPLAIN QUERY PLAN " + queries[1]))
        assert "SEARCH public_stream_archive_locator_v1 USING INDEX public_stream_archive_trade_window_v1" in compact_plan
        assert "USE TEMP B-TREE" not in compact_plan
    return result, steps


def test_stream_trade_locators_bound_exact_causal_window_and_expose_population_overflow(tmp_path):
    from dataclasses import replace

    from .test_session014_core import KEY

    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        valid = tuple(_trade_index(KEY, number) for number in range(10, 21))
        repository.register_artifacts((*valid, _trade_index(KEY, 21, event_at=19, available_at=30),
            _trade_index(KEY, 22, event_at=30, available_at=20),
            _trade_index(KEY, 23, event_at=19, available_at=20, source="OTHER"),
            _trade_index(replace(KEY, contract_revision="b" * 64), 24, event_at=19, available_at=20)))

        def operation():
            return repository.public_stream_trade_entries(KEY, source_id="BYBIT_PUBLIC_WS",
                event_from_ns=10, cutoff_ns=20, limit=3)

        result, before = _indexed_work(repository, operation, "public_stream_trade_event_window", compact=True)
        assert len(result) == 4  # Caller must publish overflow; these are not a sampled UTC-day VWAP.
        assert [entry.metadata["event_at_ns"] for entry in result] == [20, 19, 18, 17]
        repository.register_artifacts(tuple(_trade_index(KEY, 100 + number, event_at=1, available_at=5)
            for number in range(3000)))
        grown, after = _indexed_work(repository, operation, "public_stream_trade_event_window", compact=True)
        assert [entry.artifact_ref for entry in grown] == [entry.artifact_ref for entry in result]
        assert after <= before + 32
        complete = repository.public_stream_trade_entries(KEY, source_id="BYBIT_PUBLIC_WS",
            event_from_ns=10, cutoff_ns=20, limit=100)
        assert {entry.artifact_ref for entry in complete} == {entry.artifact_ref for entry in valid}


def test_wrong_generic_stream_index_and_invalidation_checks_seek_exact_intervals(tmp_path):
    from .test_session014_core import KEY

    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        body = {"instrument_revision": KEY.contract_revision, "source_id": "BYBIT_PUBLIC_WS"}
        generic_ref = sha256_json(["wrong-generic", body])
        repository.register_artifact(ArtifactIndexEntryV2(generic_ref, "PublicObservationIndexV2",
            generic_ref, 20, 20, body))
        assert not repository.generic_public_stream_observation_exists(KEY, source_id="BYBIT_PUBLIC_WS", cutoff_ns=19)

        def generic():
            return repository.generic_public_stream_observation_exists(KEY, source_id="BYBIT_PUBLIC_WS", cutoff_ns=20)

        result, before = _indexed_work(repository, generic, "public_stream_generic_source_lookup")
        assert result is True
        channel = "orderbook.50." + KEY.native_symbol

        def invalidation(number, *, source="BYBIT_PUBLIC_WS"):
            body = {"observation": {"instrument": KEY.to_dict(), "source_id": source, "channel": channel}}
            ref = sha256_json(["invalidation", number, body])
            return ArtifactIndexEntryV2(ref, "PublicStreamContinuityEventV1", ref, number, number, body)

        repository.register_artifacts((invalidation(10), invalidation(20), invalidation(30),
            invalidation(15, source="OTHER")))

        def gaps():
            return repository.public_stream_continuity_invalidations(KEY, source_id="BYBIT_PUBLIC_WS",
                channel=channel, after_ns=10, through_ns=20)

        found, gap_before = _indexed_work(repository, gaps, "public_stream_invalidation_window")
        assert len(found) == 1 and found[0].available_at_ns == 20
        assert repository.public_stream_continuity_invalidations(KEY, source_id="BYBIT_PUBLIC_WS",
            channel=channel, after_ns=20, through_ns=29) == ()
        repository.register_artifacts(tuple(invalidation(number) for number in range(100, 3100)))
        found, gap_after = _indexed_work(repository, gaps, "public_stream_invalidation_window")
        assert len(found) == 1 and found[0].available_at_ns == 20 and gap_after <= gap_before + 32
        _, after = _indexed_work(repository, generic, "public_stream_generic_source_lookup")
        assert after <= before + 32
