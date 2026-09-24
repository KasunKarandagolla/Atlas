from __future__ import annotations

import time
from decimal import Decimal

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.contracts import OpportunityWatchV2, WatchStateV2
from atlas.v2.data.bars import BarIntervalV2, CausalBarStoreV2, translate_final_bar
from atlas.v2.data.bybit import translate_instrument_info
from atlas.v2.data.collector import PublicCollectorV2
from atlas.v2.data.health import PublicSourceStateV2
from atlas.v2.data.history import ParquetObservationArchiveV2
from atlas.v2.data.raw import RawObservationV2
from atlas.v2.data.subscriptions import restore_subscription_plan
from atlas.v2.data.universe import DynamicUniverseRuntimeV2, UniverseObservationV2
from atlas.v2.instruments import EnvironmentV2, InstrumentRegistryV2
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.models.local_process import LocalProcessProvider
from atlas.v2.models.protocol import ModelManifestV2, ModelRequestV2, PromotionStatusV2
from atlas.v2.models.provider import DeterministicFakeProvider, ModelArenaV2

HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
HASH_D = "d" * 64
NS = 1_000_000_000


def test_phase1_public_to_restart_to_isolated_forecast_plumbing(tmp_path) -> None:
    now = time.time_ns()
    metadata_fixture = {"retCode": 0, "result": {"list": [{
        "symbol": "BTCUSDT", "baseCoin": "BTC", "quoteCoin": "USDT", "settleCoin": "USDT",
        "contractType": "LinearPerpetual", "status": "Trading", "priceFilter": {"tickSize": "0.1"},
        "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001"},
    }]}}
    contract = translate_instrument_info(
        metadata_fixture, environment=EnvironmentV2.MAINNET, observed_at_ns=now, available_at_ns=now
    )[0]
    registry = InstrumentRegistryV2()
    registry.register(contract)

    repository_path = tmp_path / "ops.sqlite"
    repository = OpsRepository(repository_path)
    archive = ParquetObservationArchiveV2(tmp_path / "public-observations")
    collector = PublicCollectorV2(repository=repository, registry=registry, clock_ns=lambda: now + 1, archive=archive)
    metadata = RawObservationV2.build(
        instrument_revision=contract.key.contract_revision,
        source_id="BYBIT_PUBLIC_HTTP",
        event_type="INSTRUMENT_INFO",
        event_at_ns=now,
        received_at_ns=now,
        ingested_at_ns=now,
        available_at_ns=now,
        translation_version="bybit-public-v1",
        payload=metadata_fixture,
    )
    collector.ingest(metadata, raw_payload=canonical_json(metadata_fixture))

    close_ns = now - now % BarIntervalV2.M15.duration_ns
    bar_open_ns = close_ns - BarIntervalV2.M15.duration_ns
    bar_raw = RawObservationV2.build(
        instrument_revision=contract.key.contract_revision,
        source_id="BYBIT_PUBLIC_HTTP",
        event_type="BAR_15M",
        event_at_ns=close_ns,
        received_at_ns=now,
        ingested_at_ns=now,
        available_at_ns=now,
        translation_version="bybit-public-v1",
        sequence=str(bar_open_ns),
        payload=[str(bar_open_ns // 1_000_000), "100", "102", "99", "101", "5", "500"],
    )
    bar = translate_final_bar(
        raw=bar_raw,
        interval=BarIntervalV2.M15,
        open_at_ns=bar_open_ns,
        values={"open": "100", "high": "102", "low": "99", "close": "101", "volume": "5"},
        final=True,
    )
    assert bar is not None
    raw_bar_payload = [str(bar_open_ns // 1_000_000), "100", "102", "99", "101", "5", "500"]
    collector.ingest(bar_raw, raw_payload=canonical_json(raw_bar_payload), bar=bar)
    bars = CausalBarStoreV2()
    assert bars.append(bar)
    source_health = collector.health.latest("BYBIT_PUBLIC_HTTP")
    assert source_health is not None and source_health.state == PublicSourceStateV2.HEALTHY_CURRENT

    universe_runtime = DynamicUniverseRuntimeV2()
    universe = universe_runtime.build_snapshot(
        (UniverseObservationV2(
            contract, 35, True, Decimal("25000000"), Decimal("2"),
            PublicSourceStateV2.HEALTHY_CURRENT, now,
            {"engineering-fixture": 30}, active_watch=True,
        ),),
        decision_slot_ns=now,
        information_cutoff_ns=now,
        created_at_ns=now,
        selection_policy_hash=HASH_A,
    )
    entry = universe.universe.entries[0]
    assert entry.scanner_eligible and not entry.capital_eligible

    watch = OpportunityWatchV2(
        "active-watch", contract.key, "phase1-fixture", "1", HASH_A, WatchStateV2.DETECTED,
        0, now, now, HASH_B, (), "BAR_CLOSE_15M", now + 3_600 * NS, now,
    )
    repository.create_watch(watch)
    first_plan = restore_subscription_plan(repository, universe.tiers, now_ns=now + 2)[1]
    assert collector.flush_archive() is not None
    collector.checkpoint_cursors(at_ns=now + 3)
    repository.close()
    reopened = OpsRepository(repository_path)
    restarted = PublicCollectorV2(repository=reopened, registry=registry, clock_ns=lambda: now + 4, archive=archive)
    restart = restarted.restore_subscriptions(universe.tiers, now_ns=now + 4)
    assert restart.watches.active_watches == (watch,)
    assert restart.subscriptions.plan_id == first_plan.plan_id
    restart_health = restarted.health.latest("BYBIT_PUBLIC_HTTP")
    assert restart_health is not None and restart_health.state == PublicSourceStateV2.INCOMPLETE_SNAPSHOT
    assert restarted.ingest(bar_raw, raw_payload=canonical_json(raw_bar_payload), bar=bar).append.status.value == "DUPLICATE"
    restarted.reconcile_after_reconnect(
        "BYBIT_PUBLIC_HTTP", at_ns=now + 5, complete_snapshot=True, missed_interval_repaired=True
    )

    manifest = ModelManifestV2(
        "isolated-fixture", "fixture://test", "commit", "checkpoint", "revision", (HASH_D,),
        "code-license", "weights-license", "RESEARCH_ONLY", HASH_B, HASH_C, HASH_A,
        "cpu", "fp32", ("causal-bars.v1",), ("log-return.v1",), 100, "UNKNOWN",
        PromotionStatusV2.ENGINEERING_PASS,
    )
    inputs = {"bar": bar.to_dict(), "universe_hash": universe.universe.content_hash}
    input_hash = sha256_json(inputs)
    request = ModelRequestV2.build(
        input_artifact_refs=tuple(sorted((bar.content_hash, universe.universe.content_hash))),
        input_hash=input_hash,
        instrument_key=contract.key,
        policy_context_ref="phase1-engineering-context",
        model_manifest_hash=manifest.manifest_hash,
        information_cutoff_ns=now,
        requested_targets=("log_return",),
        requested_horizons=(BarIntervalV2.M15.duration_ns,),
        requested_quantiles=(Decimal("0.5"),),
        deadline_ns=now + 20 * NS,
        seed=123,
        resource_budget={"latency_ms": 1000, "memory_mb": 128},
    )
    arena = ModelArenaV2(max_queue_size=2, clock_ns=lambda: time.time_ns())
    arena.enqueue(request, manifest, inputs, provider_key="local-isolated")
    usable = arena.run_next(LocalProcessProvider(), provider_key="local-isolated")
    assert usable is not None and usable.usable and usable.artifact is not None

    late_request = ModelRequestV2.build(
        input_artifact_refs=(bar.content_hash,), input_hash=input_hash,
        instrument_key=contract.key, policy_context_ref="phase1-engineering-context",
        model_manifest_hash=manifest.manifest_hash, information_cutoff_ns=now,
        requested_targets=("log_return",), requested_horizons=(BarIntervalV2.M15.duration_ns,),
        requested_quantiles=(Decimal("0.5"),), deadline_ns=now + 21 * NS,
        seed=123, resource_budget={"latency_ms": 1000},
    )
    late_arena = ModelArenaV2(max_queue_size=1, clock_ns=lambda: now)
    late_arena.enqueue(late_request, manifest, inputs, provider_key="late-fixture")
    late = late_arena.run_next(
        DeterministicFakeProvider(clock_ns=lambda: now, receive_lag_ns=22 * NS), provider_key="late-fixture"
    )
    assert late is not None and not late.usable and late.artifact is not None
    assert late_arena.archive.get(late_request.request_id) == late.artifact

    # The integration ends at a research forecast artifact: ops.sqlite contains no market-data warehouse,
    # and this path has no candidate, trade plan, V1 intent, approval, reservation, or dispatch call.
    tables = {row[0] for row in reopened._connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "source_health" in tables and "artifact_index" in tables
    assert "orders" not in tables and "positions" not in tables
    assert not entry.capital_eligible
    reopened.close()
