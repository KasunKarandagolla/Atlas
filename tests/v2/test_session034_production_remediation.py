from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path

import pytest

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.collector import PublicCollectorV2
from atlas.v2.data.health import PublicSourceStateV2
from atlas.v2.data.history import ImportedObservationV2, ParquetObservationArchiveV2
from atlas.v2.data.raw import AvailabilityClassV2, RawObservationV2
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    InstrumentRegistryV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime import production
from atlas.v2.runtime.ops_supervisor import (
    OpsDecisionEventV1,
    OpsRecoverySnapshotV1,
    OpsStageResultV1,
)
from atlas.v2.runtime.s3_native_cadence import (
    MAX_M1_ORIGINS_ACCOUNTED_PER_CYCLE,
    S3_M1_ACCOUNTING_CHECKPOINT_TYPE,
    S3_M1_EVENT_TYPE,
    s3_m1_event_id,
    s3_m1_origin_ref,
)

NS = 1_000_000_000
MINUTE_NS = 60 * NS
SOURCE_ID = "BYBIT_PUBLIC_WS"
FIRST_CLOSE_NS = (12 * 60 + 1) * MINUTE_NS


def _product(revision: str = "s34-native-origin-r1") -> ProductContractV2:
    key = InstrumentKeyV2(
        VenueV2.BYBIT,
        EnvironmentV2.MAINNET,
        ProductTypeV2.LINEAR_PERPETUAL,
        "BTCUSDT",
        "BTC",
        "USDT",
        "USDT",
        sha256_json({"revision": revision}),
    )
    return ProductContractV2(
        key,
        0,
        0,
        0,
        Decimal("1"),
        Decimal("0.1"),
        Decimal("0.001"),
        Decimal("0"),
        TradingStatusV2.TRADING,
        sha256_json({"metadata": revision}),
        min_notional=Decimal("1"),
        max_qty=Decimal("1000"),
    )


def _index_bars(
    repository: OpsRepository,
    archive_root: Path,
    product: ProductContractV2,
    close_times_ns: tuple[int, ...],
) -> tuple[CausalBarV2, ...]:
    imported: list[ImportedObservationV2] = []
    bars: list[CausalBarV2] = []
    for ordinal, close_at_ns in enumerate(close_times_ns, start=1):
        payload = {
            "open": "100",
            "high": "101",
            "low": "99",
            "close": "100",
            "volume": "1",
            "final": True,
        }
        payload_bytes = canonical_json(payload).encode("utf-8")
        available_at_ns = close_at_ns + 100
        raw = RawObservationV2.build(
            instrument_revision=product.key.contract_revision,
            source_id=SOURCE_ID,
            event_type="BAR_1M",
            event_at_ns=close_at_ns,
            received_at_ns=available_at_ns,
            ingested_at_ns=available_at_ns,
            available_at_ns=available_at_ns,
            translation_version="s34-native-origin-fixture-v1",
            sequence=str(close_at_ns - MINUTE_NS),
            payload=payload_bytes,
            availability_class=AvailabilityClassV2.ACTUAL_SYSTEM,
        )
        bar = CausalBarV2(
            raw,
            BarIntervalV2.M1,
            close_at_ns - MINUTE_NS,
            close_at_ns,
            Decimal("100"),
            Decimal("101"),
            Decimal("99"),
            Decimal("100"),
            Decimal("1"),
            True,
        )
        imported.append(ImportedObservationV2(ordinal, raw, payload_bytes, bar))
        bars.append(bar)

    chunk_id = sha256_json({"s34-native-origin-fixture": [bar.content_hash for bar in bars]})
    ParquetObservationArchiveV2(archive_root).write_observation_chunk(chunk_id, tuple(imported))
    entries: list[ArtifactIndexEntryV2] = []
    for bar in bars:
        observation = bar.raw
        index_ref = sha256_json({
            "artifact_type": "PublicObservationIndexV2", "record_id": observation.record_id,
        })
        entries.append(ArtifactIndexEntryV2(
            index_ref,
            "PublicObservationIndexV2",
            observation.content_hash,
            observation.received_at_ns,
            observation.available_at_ns,
            {
                "record_id": observation.record_id,
                "source_id": observation.source_id,
                "event_type": observation.event_type,
                "instrument_revision": observation.instrument_revision,
                "instrument_key_json": product.key.to_canonical_json(),
                "event_at_ns": observation.event_at_ns,
                "published_at_ns": observation.published_at_ns,
                "translation_version": observation.translation_version,
                "revision_of": observation.revision_of,
                "quality_flags": list(observation.quality_flags),
                "availability_class": observation.availability_class.value,
                "replay_available_at_ns": observation.replay_available_at_ns,
                "raw_payload_hash": observation.raw_payload_hash,
                "bar_content_hash": bar.content_hash,
                "archive_chunk_id": chunk_id,
            },
        ))
    repository.register_artifacts(entries)
    return tuple(bars)


def _setup_cycle(
    repository: OpsRepository,
    product: ProductContractV2,
    *,
    now_ns: int,
) -> PublicCollectorV2:
    registry = InstrumentRegistryV2()
    registry.register(product)
    collector = PublicCollectorV2(
        repository=repository,
        registry=registry,
        clock_ns=lambda: now_ns,
        archive=ParquetObservationArchiveV2(Path(repository.path).parent / "ops-observations"),
    )
    collector._record_health(
        SOURCE_ID,
        PublicSourceStateV2.HEALTHY_CURRENT,
        at_ns=now_ns,
        details="deterministic local origin accounting fixture",
    )
    return collector


def _collect(repository: OpsRepository, collector: PublicCollectorV2, *, now_ns: int):
    return production.IndexedPublicCycleSourceV1().collect(
        repository,
        collector,
        now_ns=now_ns,
        recovery=OpsRecoverySnapshotV1((), (), (), None, True, now_ns),
    )


def _origin_gates(repository: OpsRepository) -> dict[int, ArtifactIndexEntryV2]:
    result: dict[int, ArtifactIndexEntryV2] = {}
    for entry in repository.artifact_entries("OpsPublicAcquisitionDeadlineGateV1"):
        origin = entry.metadata.get("native_m1_origin")
        if isinstance(origin, Mapping):
            close_at_ns = origin.get("close_at_ns")
            if type(close_at_ns) is int:
                result[close_at_ns] = entry
    return result


def _origin_events(repository: OpsRepository) -> dict[int, ArtifactIndexEntryV2]:
    result: dict[int, ArtifactIndexEntryV2] = {}
    for entry in repository.artifact_entries("OpsDecisionEventSourceV1"):
        origin = entry.metadata.get("native_m1_origin")
        event = entry.metadata.get("event")
        if isinstance(origin, Mapping) and isinstance(event, Mapping):
            close_at_ns = origin.get("close_at_ns")
            if type(close_at_ns) is int:
                result[close_at_ns] = entry
    return result


def _latest_checkpoint(repository: OpsRepository) -> dict:
    entries = repository.artifact_entries(S3_M1_ACCOUNTING_CHECKPOINT_TYPE)
    assert entries
    latest = max(entries, key=lambda item: item.metadata["checkpoint"]["generation"])
    body = latest.metadata["checkpoint"]
    assert latest.artifact_ref == sha256_json(body)
    return body


def test_three_delayed_origins_are_gated_oldest_first_and_survive_restart(tmp_path, monkeypatch):
    path = tmp_path / "ops.sqlite"
    archive_root = tmp_path / "ops-observations"
    product = _product()
    closes = tuple(FIRST_CLOSE_NS + offset * MINUTE_NS for offset in range(3))
    cycle_now = closes[-1] + 10 * NS

    with OpsRepository(path) as repository:
        repository.register_artifact(ArtifactIndexEntryV2(
            product.content_hash,
            "ProductContractV2",
            product.content_hash,
            product.available_at_ns,
            product.available_at_ns,
            {"product": product.to_dict()},
        ))
        bars = _index_bars(repository, archive_root, product, closes)
        assert [bar.close_at_ns for bar in bars] == list(closes)
        page = repository.native_m1_origin_observation_page(
            product.key,
            available_from_ns=0,
            available_through_ns=cycle_now,
            limit=MAX_M1_ORIGINS_ACCOUNTED_PER_CYCLE,
        )
        assert len(page.entries) == 3, page
        from atlas.v2.data.history import reconstruct_native_m1_bars_from_index_page

        reconstructed = reconstruct_native_m1_bars_from_index_page(
            repository, archive_root, key=product.key, index_entries=page.entries,
        )
        assert len(reconstructed) == 3, reconstructed
        collector = _setup_cycle(repository, product, now_ns=cycle_now)
        monkeypatch.setattr(
            production,
            "_persist_s3_m1_origin_checkpoint",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("crash after durable gates")),
        )
        with pytest.raises(RuntimeError, match="crash after durable gates"):
            _collect(repository, collector, now_ns=cycle_now)
        gates_after_crash = _origin_gates(repository)
        assert set(gates_after_crash) == set(closes)
        assert _origin_events(repository) == {}
        assert repository.artifact_entries(S3_M1_ACCOUNTING_CHECKPOINT_TYPE) == ()

    monkeypatch.undo()
    restart_now = cycle_now + 1
    with OpsRepository(path) as repository:
        collector = _setup_cycle(repository, product, now_ns=restart_now)
        _collect(repository, collector, now_ns=restart_now + 1)
        gates_after_restart = _origin_gates(repository)
        assert set(gates_after_restart) == set(closes)
        assert {close: entry.artifact_ref for close, entry in gates_after_restart.items()} == {
            close: entry.artifact_ref for close, entry in gates_after_crash.items()
        }
        assert all(
            entry.metadata["deadline_gate"]["reason_code"]
            == "NATIVE_M1_BAR_FIRST_SEEN_AFTER_FIXED_DEADLINE"
            for entry in gates_after_restart.values()
        )
        assert all(
            entry.metadata["deadline_gate"]["deadlines_ns"] == (close + 5 * NS,)
            for close, entry in gates_after_restart.items()
        ), {close: entry.metadata["deadline_gate"]["deadlines_ns"]
            for close, entry in gates_after_restart.items()}
        assert _latest_checkpoint(repository)["last_accounted_close_at_ns"] == closes[-1]
        assert _latest_checkpoint(repository)["scan_complete"] is True

    with OpsRepository(path) as repository:
        before = {close: entry.artifact_ref for close, entry in _origin_gates(repository).items()}
        collector = _setup_cycle(repository, product, now_ns=restart_now + 2)
        _collect(repository, collector, now_ns=restart_now + 3)
        after = {close: entry.artifact_ref for close, entry in _origin_gates(repository).items()}
        assert after == before
        assert _origin_events(repository) == {}


def test_bounded_backlog_drains_oldest_first_and_new_origins_wait_for_the_next_window(tmp_path):
    path = tmp_path / "ops.sqlite"
    archive_root = tmp_path / "ops-observations"
    product = _product("s34-native-origin-backlog-r1")
    backlog_count = 2 * MAX_M1_ORIGINS_ACCOUNTED_PER_CYCLE + 3
    closes = tuple(FIRST_CLOSE_NS + offset * MINUTE_NS for offset in range(backlog_count))
    first_now = closes[-1] + 10 * NS
    later_close = closes[-1] + MINUTE_NS

    with OpsRepository(path) as repository:
        repository.register_artifact(ArtifactIndexEntryV2(
            product.content_hash,
            "ProductContractV2",
            product.content_hash,
            product.available_at_ns,
            product.available_at_ns,
            {"product": product.to_dict()},
        ))
        _index_bars(repository, archive_root, product, closes)
        collector = _setup_cycle(repository, product, now_ns=first_now)
        _collect(repository, collector, now_ns=first_now)
        first_page_gates = _origin_gates(repository)
        assert sorted(first_page_gates) == list(closes[:MAX_M1_ORIGINS_ACCOUNTED_PER_CYCLE])
        checkpoint = _latest_checkpoint(repository)
        assert checkpoint["source_scan_after_close_at_ns"] == closes[MAX_M1_ORIGINS_ACCOUNTED_PER_CYCLE - 1]
        assert checkpoint["scan_complete"] is False

    # A restart and a newly discovered later close cannot rebase the frozen
    # source window or move work ahead of the oldest unaccounted close.
    with OpsRepository(path) as repository:
        new_bar, = _index_bars(repository, archive_root, product, (later_close,))
        collector = _setup_cycle(repository, product, now_ns=first_now + 10)
        for page_number in (1, 2):
            now_ns = first_now + 12 + page_number
            _collect(repository, collector, now_ns=now_ns)
            gates = _origin_gates(repository)
            accounted = min(len(closes), (page_number + 1) * MAX_M1_ORIGINS_ACCOUNTED_PER_CYCLE)
            assert len(gates) == accounted
            assert sorted(gates) == list(closes[:accounted])
            checkpoint = _latest_checkpoint(repository)
            assert checkpoint["last_accounted_close_at_ns"] == closes[accounted - 1]
            if page_number == 1:
                assert checkpoint["scan_complete"] is False
            else:
                assert checkpoint["scan_complete"] is True

        # The later bar was not visible in the first checkpoint's frozen
        # availability window. It becomes eligible only in the following one.
        assert later_close not in _origin_gates(repository)
        later_now = new_bar.raw.available_at_ns + 1
        batch = _collect(repository, collector, now_ns=later_now)
        all_gates = _origin_gates(repository)
        all_events = _origin_events(repository)
        assert sorted(all_gates) == list(closes)
        assert set(all_events) == {later_close}
        assert all_events[later_close].metadata["event"]["event_type"] == S3_M1_EVENT_TYPE
        assert batch.events[0].event_id == s3_m1_event_id(product.key, later_close)
        assert _latest_checkpoint(repository)["last_accounted_close_at_ns"] == later_close
        assert len(all_gates) == backlog_count


def test_timely_native_origin_event_is_reused_after_event_before_checkpoint_crash(tmp_path, monkeypatch):
    path = tmp_path / "ops.sqlite"
    archive_root = tmp_path / "ops-observations"
    product = _product("s34-native-origin-timely-r1")
    close_at_ns = FIRST_CLOSE_NS + 100 * MINUTE_NS
    available_at_ns = close_at_ns + 100
    now_ns = close_at_ns + NS

    with OpsRepository(path) as repository:
        repository.register_artifact(ArtifactIndexEntryV2(
            product.content_hash,
            "ProductContractV2",
            product.content_hash,
            product.available_at_ns,
            product.available_at_ns,
            {"product": product.to_dict()},
        ))
        _index_bars(repository, archive_root, product, (close_at_ns,))
        collector = _setup_cycle(repository, product, now_ns=available_at_ns)
        original = production._persist_s3_m1_origin_checkpoint
        monkeypatch.setattr(
            production,
            "_persist_s3_m1_origin_checkpoint",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("crash after durable event")),
        )
        with pytest.raises(RuntimeError, match="crash after durable event"):
            _collect(repository, collector, now_ns=now_ns)
        events_after_crash = _origin_events(repository)
        assert set(events_after_crash) == {close_at_ns}
        event = events_after_crash[close_at_ns].metadata["event"]
        assert event["event_type"] == S3_M1_EVENT_TYPE
        assert event["event_id"] == s3_m1_event_id(product.key, close_at_ns)
        assert event["information_cutoff_ns"] == available_at_ns
        assert event["deadline_ns"] == close_at_ns + 5 * NS
        assert event["available_at_ns"] <= now_ns <= event["deadline_ns"]
        assert _origin_gates(repository) == {}

        monkeypatch.setattr(production, "_persist_s3_m1_origin_checkpoint", original)
        _collect(repository, collector, now_ns=now_ns + 10)
        events_after_retry = _origin_events(repository)
        assert {close: entry.artifact_ref for close, entry in events_after_retry.items()} == {
            close: entry.artifact_ref for close, entry in events_after_crash.items()
        }
        assert _origin_gates(repository) == {}
        assert _latest_checkpoint(repository)["last_accounted_close_at_ns"] == close_at_ns
        assert _latest_checkpoint(repository)["scan_complete"] is True
        assert s3_m1_origin_ref(product.key, close_at_ns) == (
            events_after_retry[close_at_ns].metadata["native_m1_origin"]["origin_ref"]
        )


def test_native_event_missing_exact_source_artifacts_fails_closed(tmp_path):
    close_at_ns = 1_000 * MINUTE_NS
    cutoff_ns = close_at_ns + 100
    event = OpsDecisionEventV1(
        "e" * 64,
        S3_M1_EVENT_TYPE,
        SOURCE_ID,
        "f" * 64,
        close_at_ns,
        None,
        cutoff_ns,
        cutoff_ns,
        cutoff_ns,
        close_at_ns + 5 * NS,
        (),
    )

    class DiagnosticOnlyInputs:
        def resolve(self, _repository, _event):
            return production.ProductionEventInputsV1(None, (), {}, {}, {})

    completed_at_ns = cutoff_ns + 200
    port = production.ProductionOpsCyclePortV1(
        inputs_provider=DiagnosticOnlyInputs(),  # type: ignore[arg-type]
        clock_ns=lambda: completed_at_ns,
    )
    checkpoints: list[OpsStageResultV1] = []
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        with pytest.raises(ValueError, match="does not bind its exact cutoff-visible trigger"):
            port.process_event(
                repository,
                event,
                now_ns=cutoff_ns + 100,
                source_health_state="UNKNOWN",
                completed_stages={},
                checkpoint=checkpoints.append,
            )
        assert checkpoints == []
        assert repository.artifact_entries("CandidateSetV2") == ()
        assert repository.artifact_entries("DecisionCalendarEntryV2") == ()
