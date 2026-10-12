"""Deterministic, opt-in close-driven M15 acquisition and causal-path checks."""

from __future__ import annotations

import asyncio
import time
from dataclasses import asdict, replace

import pytest

from atlas.v2._serialization import canonical_json, json_value, sha256_json
from atlas.v2.contracts import OpportunityWatchV2, WatchStateV2
from atlas.v2.data.bars import BarIntervalV2
from atlas.v2.data.broad_public_source import BroadPublicCycleSourceV2
from atlas.v2.data.subscriptions import build_subscription_plan
from atlas.v2.data.universe import ComputeTierV2, SubscriptionChannelV2
from atlas.v2.instruments import ProductContractV2, VenueV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.product import (
    BINANCE_CLOSE_DRIVEN_M15_PROFILE_V1,
    ResearchRunConfigV1,
    ResearchRunConfigV2,
    _run_config,
    canonical_data_root,
)
from atlas.v2.runtime import production
from atlas.v2.runtime.broad_public_runtime import BroadPublicRuntimeV2
from atlas.v2.runtime.broad_universe import latest_workset, publish_broad_workset
from atlas.v2.runtime.ops_supervisor import OpsSupervisorV2
from atlas.v2.runtime.production import BroadProductionOpsCyclePortV2
from atlas.v2.strategies.s1_trend import S1_POLICY

M15_NS = BarIntervalV2.M15.duration_ns
CLOSE_NS = 1_800_000_000_000_000
START_NS = CLOSE_NS - 1_000_000_000


class _Clock:
    def __init__(self, at_ns: int) -> None:
        self.at_ns = at_ns

    def __call__(self) -> int:
        return self.at_ns


class _Response:
    def __init__(self, payload, received_at_ns: int) -> None:
        self.payload = payload
        self.received_at_ns = received_at_ns
        self.raw_body = canonical_json(payload).encode()


def _exchange_info() -> dict:
    return {"symbols": [{
        "symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT", "marginAsset": "USDT",
        "contractType": "PERPETUAL", "status": "TRADING", "onboardDate": 1_700_000_000_000,
        "deliveryDate": 0, "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
            {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001", "maxQty": "1000"},
        ],
    }]}


class _Client:
    def __init__(self, clock: _Clock) -> None:
        self.clock = clock
        self.paths: list[str] = []

    def get(self, path: str, params=None):
        del params
        self.paths.append(path)
        at_ms = self.clock() // 1_000_000
        if path.endswith("bookTicker"):
            payload = [{"symbol": "BTCUSDT", "bidPrice": "99.99", "askPrice": "100.01",
                        "u": 77, "E": at_ms}]
        elif path.endswith("premiumIndex"):
            payload = [{"symbol": "BTCUSDT", "markPrice": "100", "indexPrice": "100",
                        "lastFundingRate": "0.0001", "time": at_ms}]
        elif path.endswith("24hr"):
            payload = [{"symbol": "BTCUSDT", "quoteVolume": "100000000", "closeTime": at_ms}]
        elif path.endswith("depth"):
            payload = {"lastUpdateId": 77, "bids": [["99.99", "1"]], "asks": [["100.01", "1"]]}
        else:
            raise AssertionError(f"unexpected public endpoint: {path}")
        return _Response(payload, self.clock())


class _BinanceReader:
    def __init__(self, clock: _Clock) -> None:
        self.clock = clock
        self.client = _Client(clock)
        self.server_time = CLOSE_NS
        self.calls: list[str] = []
        self.m15_received_at_ns: int | None = None

    def exchange_info(self):
        self.calls.append("exchange_info")
        return _Response(_exchange_info(), self.clock())

    def server_time_ns(self):
        self.calls.append("server_time")
        return self.server_time

    def book_ticker(self, symbol: str):
        del symbol
        return self.client.get("/fapi/v1/ticker/bookTicker")

    def premium_index(self, symbol: str):
        del symbol
        return self.client.get("/fapi/v1/premiumIndex")

    def klines(self, symbol: str, interval: BarIntervalV2, *, limit: int):
        assert symbol == "BTCUSDT" and 2 <= limit <= 200
        self.calls.append(f"kline:{interval.value}")
        close = (self.server_time // interval.duration_ns) * interval.duration_ns
        if interval == BarIntervalV2.M15:
            close = CLOSE_NS
        elif interval == BarIntervalV2.M1:
            # Keep this fixture focused on the M15 trigger; a simultaneously
            # closed M1 bar would legitimately create an independent S3 event.
            close = CLOSE_NS - interval.duration_ns
        open_ms = (close - interval.duration_ns) // 1_000_000
        close_ms = close // 1_000_000 - 1
        row = [open_ms, "100", "101", "99", "100.2", "1000", close_ms]
        received_at_ns = (self.m15_received_at_ns if interval == BarIntervalV2.M15
                          and self.m15_received_at_ns is not None else self.clock())
        self.clock.at_ns = max(self.clock.at_ns, received_at_ns)
        return _Response([row], received_at_ns)

    def aggregate_trades(self, symbol: str, *, limit: int):
        del symbol, limit
        return _Response([], self.clock())

    def funding_history(self, symbol: str, *, limit: int):
        del symbol, limit
        return _Response([], self.clock())

    def open_interest(self, symbol: str):
        return _Response({"symbol": symbol, "openInterest": "100", "time": self.clock() // 1_000_000},
                         self.clock())

    def open_interest_history(self, symbol: str, *, period: str, limit: int):
        del symbol, period, limit
        return _Response([], self.clock())


def _source(clock: _Clock, reader: _BinanceReader) -> BroadPublicCycleSourceV2:
    return BroadPublicCycleSourceV2(
        enabled_venues=(VenueV2.BINANCE,), binance_reader=reader, binance_depth_reader=reader,
        clock_ns=clock, close_driven_m15_profile=True,
    )


def _idle_stream():
    async def frames():
        await asyncio.Event().wait()
        if False:
            yield b""
    return frames


class _FixtureS1Inputs:
    """Supply one immutable, test-only S1 competitor to the real selector."""

    def __init__(self, product: ProductContractV2) -> None:
        self.product = product
        self.calls = 0

    def resolve(self, repository: OpsRepository, event):
        from atlas.v2.science.research_selection import research_selection_universe

        from .session023_support import feature_candidate
        from .test_session016_candidate_selection import evidence
        from .test_session016_candidate_selection import universe as fixture_universe

        self.calls += 1
        universe = fixture_universe(keys=(self.product.key,))
        entry = replace(universe.entries[0], product_ref=self.product.content_hash)
        envelope = replace(
            universe.envelope,
            created_at_ns=event.information_cutoff_ns,
            available_at_ns=event.information_cutoff_ns,
            input_refs=(self.product.content_hash,),
            content_hash="",
        )
        universe = replace(
            universe,
            envelope=envelope,
            decision_slot_ns=event.information_cutoff_ns,
            entries=(entry,),
        )
        universe = research_selection_universe(universe)
        repository.register_artifact(ArtifactIndexEntryV2(
            universe.content_hash, "UniverseContractV2", universe.content_hash,
            event.information_cutoff_ns, event.information_cutoff_ns,
            {"universe": universe.to_dict()},
        ))
        candidate = feature_candidate(
            repository, S1_POLICY, key=self.product.key,
            decision_at_ns=event.information_cutoff_ns,
            deadline_ns=event.deadline_ns,
            liquidity_ref=event.trigger_ref,
        )
        scanner_ref = evidence(
            repository, candidate, universe, 1,
            available_at_ns=event.information_cutoff_ns, event=event.event_id,
        )
        return production.ProductionEventInputsV1(
            universe, (candidate,), {candidate.candidate_id: (scanner_ref,)}, {}, {},
            causal_feature_refs=(candidate.snapshot_hash,),
            causal_source_refs=tuple(sorted({*event.causal_input_refs, self.product.content_hash})),
        )


class _ProfilePort(BroadProductionOpsCyclePortV2):
    def __init__(self, *, public_source, clock: _Clock, product: ProductContractV2) -> None:
        runtime = BroadPublicRuntimeV2(
            clock_ns=clock,
            stream_factories={"BINANCE_MARKET": _idle_stream()},
        )
        super().__init__(public_source=public_source, clock_ns=clock, broad_runtime=runtime)
        self._profile_product = product
        self.collection_elapsed_ns: int | None = None
        self.source_acquisition_ns: int | None = None

    def collect(self, repository, *, now_ns, recovery):
        started_ns = time.monotonic_ns()
        batch = super().collect(repository, now_ns=now_ns, recovery=recovery)
        self.collection_elapsed_ns = time.monotonic_ns() - started_ns
        snapshot = getattr(self.public_source, "_last_snapshot", None)
        if snapshot is not None:
            self.source_acquisition_ns = snapshot.acquisition_duration_ns
        return batch

    def recover(self, repository: OpsRepository, *, now_ns: int):
        result = super().recover(repository, now_ns=now_ns)
        self.public_source.set_close_driven_m15_keys((self._profile_product.key,))
        return result


def test_new_profile_is_versioned_opt_in_and_leaves_v1_hash_shape_unchanged(tmp_path) -> None:
    v1 = ResearchRunConfigV1()
    assert "public_acquisition_profile" not in asdict(v1)
    old_v2 = ResearchRunConfigV2(data_root=canonical_data_root(tmp_path))
    old_body = asdict(old_v2)
    old_body.pop("public_acquisition_profile")
    # Unset S49 discovery effort is omitted from the immutable legacy identity.
    old_body.pop("discovery_effort")
    assert old_v2.content_hash == sha256_json(old_body)
    assert _run_config(asdict(old_v2)).content_hash == old_v2.content_hash

    config = ResearchRunConfigV2(
        data_root=canonical_data_root(tmp_path), public_venues=("BINANCE",),
        selected_execution_venue="BINANCE", execution_profile="DISABLED", provider_profile="DISABLED",
        public_acquisition_profile=BINANCE_CLOSE_DRIVEN_M15_PROFILE_V1,
    )
    assert config.public_acquisition_profile == BINANCE_CLOSE_DRIVEN_M15_PROFILE_V1
    assert config.content_hash != old_v2.content_hash
    assert _run_config(asdict(config)).content_hash == config.content_hash

    for changes in (
        {"public_venues": ("BYBIT", "BINANCE")},
        {"public_venues": ("BYBIT",)},
        {"provider_profile": "deepseek-v41-action-critic-v1"},
        {"execution_profile": "DEMO_READ_ONLY_QUALIFICATION"},
        {"credential_ref": "ref_demo"},
        {"execution_instrument_symbol": "BTCUSDT"},
    ):
        with pytest.raises(ValueError, match="close-driven M15 profile"):
            values = {
                "data_root": canonical_data_root(tmp_path), "public_venues": ("BINANCE",),
                "selected_execution_venue": "BINANCE",
                "public_acquisition_profile": BINANCE_CLOSE_DRIVEN_M15_PROFILE_V1,
            }
            values.update(changes)
            ResearchRunConfigV2(**values)


def test_close_profile_reuses_bulk_quotes_publishes_one_exact_final_m15_and_restores(
    tmp_path, monkeypatch, record_property,
) -> None:
    from atlas.v2.data import broad_public_source

    def forbidden_bybit_construction():
        raise AssertionError("inactive Bybit reader was constructed")

    monkeypatch.setattr(broad_public_source, "BybitPublicReaderV2", forbidden_bybit_construction)
    clock = _Clock(START_NS)
    reader = _BinanceReader(clock)
    source = _source(clock, reader)
    products = source.bootstrap_products(now_ns=START_NS)
    product, = products
    source.set_close_driven_m15_keys((product.key,))

    clock.at_ns = CLOSE_NS + 250_000_000
    source.begin_collection_cycle(now_ns=clock())
    snapshot = source.acquire_snapshot(now_ns=clock())
    assert snapshot.complete
    assert snapshot.request_limit == 32 + 27
    profile = snapshot.source_snapshot["m15_close_profile"]
    assert profile["status"] == "COMPLETE"
    assert profile["completed_keys"] == (product.key.to_canonical_json(),)
    assert profile["request_count"] < 27  # bulk BBO/mark were shared with the 10-second refresh
    assert profile["results"][0]["bar_available_at_ns"] == CLOSE_NS + 250_000_000
    assert profile["results"][0]["bbo_received_at_ns"] == CLOSE_NS + 250_000_000
    assert profile["results"][0]["mark_index_received_at_ns"] == CLOSE_NS + 250_000_000
    bars = [item for item in snapshot.records if item.bar is not None
            and item.bar.interval == BarIntervalV2.M15 and item.bar.close_at_ns == CLOSE_NS]
    assert len(bars) == 1
    assert bars[0].observation.available_at_ns <= CLOSE_NS + 5_000_000_000
    assert source.bybit_reader is None
    assert all("bybit" not in path.casefold() for path in reader.client.paths)
    assert reader.calls.count("server_time") == 1
    assert source.export_state()["schema_version"] == 3
    assert snapshot.acquisition_duration_ns > 0
    assert profile["attempt_duration_ns"] > 0
    record_property("source_acquisition_duration_ns", snapshot.acquisition_duration_ns)
    record_property("close_attempt_duration_ns", profile["attempt_duration_ns"])

    restored = _source(clock, _BinanceReader(clock))
    restored.bootstrap_products(now_ns=clock())
    restored.restore_state(source.export_state())
    restored.set_close_driven_m15_keys((product.key,))
    restored.begin_collection_cycle(now_ns=clock())
    assert restored._pending_m15_close_ns is None
    assert restored.export_state()["last_m15_close_attempt_ns"] == CLOSE_NS


def test_close_profile_rejects_eligible_population_overflow_without_truncating() -> None:
    clock = _Clock(START_NS)
    source = _source(clock, _BinanceReader(clock))
    product, = source.bootstrap_products(now_ns=START_NS)
    keys = tuple(replace(product.key, native_symbol=f"S44TEST{index}USDT") for index in range(25))
    with pytest.raises(ValueError, match="exact Binance bound"):
        source.set_close_driven_m15_keys(keys)
    assert source._close_driven_m15_keys == ()


def test_close_profile_rejects_incompatible_venue_before_inactive_client_construction(monkeypatch) -> None:
    from atlas.v2.data import broad_public_source

    def forbidden_bybit_construction():
        raise AssertionError("inactive Bybit reader was constructed before profile rejection")

    monkeypatch.setattr(broad_public_source, "BybitPublicReaderV2", forbidden_bybit_construction)
    with pytest.raises(ValueError, match="Binance-only"):
        BroadPublicCycleSourceV2(
            enabled_venues=(VenueV2.BYBIT, VenueV2.BINANCE), close_driven_m15_profile=True,
        )


def test_close_profile_marks_late_final_bar_instead_of_calling_it_complete() -> None:
    clock = _Clock(CLOSE_NS + 5_100_000_000)
    reader = _BinanceReader(clock)
    reader.server_time = clock()
    source = _source(clock, reader)
    product, = source.bootstrap_products(now_ns=START_NS)
    source.set_close_driven_m15_keys((product.key,))
    started = CLOSE_NS + 4_900_000_000
    source.begin_collection_cycle(now_ns=started)
    snapshot = source.acquire_snapshot(now_ns=started)

    profile = snapshot.source_snapshot["m15_close_profile"]
    assert profile["status"] == "LATE"
    assert profile["late_keys"] == (product.key.to_canonical_json(),)
    assert profile["results"][0]["late_by_ns"] == 100_000_000
    bar, = [item.bar for item in snapshot.records if item.bar is not None
            and item.bar.interval == BarIntervalV2.M15]
    assert bar is not None and bar.raw.available_at_ns == CLOSE_NS + 5_100_000_000


def test_close_profile_broad_workset_uses_internal_interval_and_rest_health_identity(tmp_path) -> None:
    clock = _Clock(START_NS)
    source = _source(clock, _BinanceReader(clock))
    product, = source.bootstrap_products(now_ns=START_NS)
    source.set_close_driven_m15_keys((product.key,))
    clock.at_ns = CLOSE_NS + 250_000_000
    source.begin_collection_cycle(now_ns=clock())
    snapshot = source.acquire_snapshot(now_ns=clock())

    with OpsRepository(tmp_path / "workset.sqlite") as repository:
        repository.register_artifact(ArtifactIndexEntryV2(
            product.content_hash, "ProductContractV2", product.content_hash,
            product.observed_at_ns, product.available_at_ns, {"product": product.to_dict()},
        ))
        acquisition_ref = sha256_json({"profile-workset-acquisition": snapshot.source_snapshot})
        repository.register_artifact(ArtifactIndexEntryV2(
            acquisition_ref, "FixtureAcquisitionV1", acquisition_ref, clock(), clock(), {},
        ))
        publish_broad_workset(
            repository, products=(product,), snapshot=snapshot, available_at_ns=clock(),
            acquisition_ref=acquisition_ref, source_state={"BINANCE_USDM_PUBLIC_HTTP": "HEALTHY_CURRENT"},
        )
        body = latest_workset(repository, cutoff_ns=clock())
        assert body is not None
        key = product.key.to_canonical_json()
        assert body["coverage"][key]["M15"] == ((CLOSE_NS, CLOSE_NS),)
        assert body["universe_ref"]


def test_timely_profile_origin_reaches_durable_candidate_set_and_restart_is_idempotent(
    tmp_path, monkeypatch, record_property,
) -> None:
    from atlas.v2.data import public_stream_source
    from atlas.v2.strategies.s1_trend import POLICY_ID, POLICY_VERSION

    def offline_capture_public_frames(**kwargs):
        del kwargs
        return _idle_stream()()

    # The production port may reconfigure to a watched depth lane after
    # startup. Keep every possible public stream factory local and inert.
    monkeypatch.setattr(public_stream_source, "capture_public_frames", offline_capture_public_frames)

    clock = _Clock(START_NS)
    reader = _BinanceReader(clock)
    source = _source(clock, reader)
    product, = source.bootstrap_products(now_ns=START_NS)
    watch_id = sha256_json({"profile-watch": product.key.to_dict()})
    thesis_ref = sha256_json({"profile-watch-thesis": product.key.to_dict()})
    watch = OpportunityWatchV2(
        watch_id, product.key, POLICY_ID, POLICY_VERSION, S1_POLICY.policy_hash,
        WatchStateV2.DETECTED, 0, START_NS, START_NS, thesis_ref, (thesis_ref,),
        "CANDLE_CLOSED_15M", CLOSE_NS + 4 * M15_NS + 1, START_NS,
    )
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repository:
        repository.register_artifact(ArtifactIndexEntryV2(
            product.content_hash, "ProductContractV2", product.content_hash,
            product.observed_at_ns, product.available_at_ns, {"product": product.to_dict()},
        ))
        repository.create_watch(watch)
        repository.transition_watch(
            watch.watch_id, expected_state_version=0,
            event_id=sha256_json({"profile-watch-waiting": watch.watch_id}),
            event_at_ns=START_NS, transition_at_ns=START_NS,
            target_state=WatchStateV2.WAITING_FOR_EVENT,
            outbox_id=sha256_json({"profile-watch-outbox": watch.watch_id}),
            required_next_event="CANDLE_CLOSED_15M",
        )
        waiting_watch = repository.get_watch(watch.watch_id)
        assert waiting_watch is not None
        subscription_plan = build_subscription_plan(
            {product.key: ComputeTierV2.TIER_1}, active_watches=(waiting_watch,),
            created_at_ns=START_NS,
        )
        spec, = subscription_plan.specs
        assert SubscriptionChannelV2.KLINE_15M in spec.channels

    clock.at_ns = CLOSE_NS + 250_000_000
    port = _ProfilePort(public_source=source, clock=clock, product=product)
    inputs = _FixtureS1Inputs(product)
    port.inputs_provider = inputs
    try:
        with OpsSupervisorV2(path, port, clock_ns=clock) as supervisor:
            result = supervisor.run_once()
            assert port.collection_elapsed_ns is not None and port.collection_elapsed_ns > 0
            assert port.source_acquisition_ns is not None and port.source_acquisition_ns > 0
            # These intervals intentionally overlap: collection includes source
            # acquisition plus writer/index/decision work; neither is additive.
            record_property("supervisor_collection_elapsed_ns", port.collection_elapsed_ns)
            record_property("source_acquisition_duration_ns", port.source_acquisition_ns)
            assert supervisor.repository is not None
            repository = supervisor.repository
            assert result.cycle.event_ids
            metadata_evidence = repository.get_artifact(product.metadata_ref)
            assert metadata_evidence is not None
            assert metadata_evidence.artifact_type == "ProductMetadataEvidenceV1"
            events = repository.artifact_entries("OpsDecisionEventSourceV1")
            assert len(events) == 1
            event = production.decision_event_from_dict(events[0].metadata["event"])
            assert event.source_event_at_ns == CLOSE_NS
            assert event.available_at_ns <= CLOSE_NS + 5_000_000_000
            assert event.information_cutoff_ns <= CLOSE_NS + 5_000_000_000
            assert event.deadline_ns == event.information_cutoff_ns + 5_000_000_000
            assert any(
                row.metadata.get("event_at_ns") == CLOSE_NS
                and row.metadata.get("bar_content_hash") is not None
                for row in repository.artifact_entries("PublicObservationIndexV2")
            )
            candidate_sets = repository.artifact_entries("CandidateSetV2")
            assert len(candidate_sets) == 1
            candidate_set = production.CandidateSetV2.from_dict(
                json_value(candidate_sets[0].metadata["candidate_set"]))
            assert candidate_set.decision_event_id == event.event_id
            assert candidate_set.selected_candidate_id is not None
            assert len(candidate_set.candidates) == 1
            assert inputs.calls == 1
            assert result.event_receipts and not result.event_receipts[0].capital_enabled
            assert not result.event_receipts[0].assisted_enabled

            # A repeated cycle at the same close may observe the old index again,
            # but it cannot create a second event or CandidateSet.
            supervisor.run_once()
            assert len(repository.artifact_entries("CandidateSetV2")) == 1
            assert len(repository.artifact_entries("OpsDecisionEventSourceV1")) == 1
            port.finish_public_capture(repository)
    finally:
        port.close()

    restarted_source = _source(clock, _BinanceReader(clock))
    restarted_port = _ProfilePort(public_source=restarted_source, clock=clock, product=product)
    restarted_port.inputs_provider = inputs
    try:
        with OpsSupervisorV2(path, restarted_port, clock_ns=clock) as restarted:
            restarted.run_once()
            assert restarted.repository is not None
            assert len(restarted.repository.artifact_entries("CandidateSetV2")) == 1
            assert len(restarted.repository.artifact_entries("OpsDecisionEventSourceV1")) == 1
            assert inputs.calls == 1
            restarted_port.finish_public_capture(restarted.repository)
    finally:
        restarted_port.close()


def test_late_close_profile_is_durably_missing_without_event_after_restart(tmp_path, monkeypatch) -> None:
    from atlas.v2.data import public_stream_source

    def offline_capture_public_frames(**kwargs):
        del kwargs
        return _idle_stream()()

    monkeypatch.setattr(public_stream_source, "capture_public_frames", offline_capture_public_frames)
    clock = _Clock(START_NS)
    reader = _BinanceReader(clock)
    source = _source(clock, reader)
    product, = source.bootstrap_products(now_ns=START_NS)
    watch_id = sha256_json({"late-profile-watch": product.key.to_dict()})
    thesis_ref = sha256_json({"late-profile-watch-thesis": product.key.to_dict()})
    watch = OpportunityWatchV2(
        watch_id, product.key, "S1_MTF_TREND_PULLBACK", "1.0.0-research", S1_POLICY.policy_hash,
        WatchStateV2.DETECTED, 0, START_NS, START_NS, thesis_ref, (thesis_ref,),
        "CANDLE_CLOSED_15M", CLOSE_NS + 4 * M15_NS + 1, START_NS,
    )
    path = tmp_path / "late-ops.sqlite"
    with OpsRepository(path) as repository:
        repository.register_artifact(ArtifactIndexEntryV2(
            product.content_hash, "ProductContractV2", product.content_hash,
            product.observed_at_ns, product.available_at_ns, {"product": product.to_dict()},
        ))
        repository.create_watch(watch)
        repository.transition_watch(
            watch.watch_id, expected_state_version=0,
            event_id=sha256_json({"late-profile-watch-waiting": watch.watch_id}),
            event_at_ns=START_NS, transition_at_ns=START_NS,
            target_state=WatchStateV2.WAITING_FOR_EVENT,
            outbox_id=sha256_json({"late-profile-watch-outbox": watch.watch_id}),
            required_next_event="CANDLE_CLOSED_15M",
        )

    clock.at_ns = CLOSE_NS + 4_900_000_000
    reader.m15_received_at_ns = CLOSE_NS + 5_100_000_000
    port = _ProfilePort(public_source=source, clock=clock, product=product)
    try:
        with OpsSupervisorV2(path, port, clock_ns=clock) as supervisor:
            result = supervisor.run_once()
            assert result.cycle.failure_types == ()
            assert result.cycle.event_ids == ()
            assert supervisor.repository is not None
            repository = supervisor.repository
            receipt = repository.latest_artifact_entries(
                "BroadPublicAcquisitionReceiptV2", as_of_ns=clock(), limit=1).entries[0]
            profile = receipt.metadata["receipt"]["source_snapshot"]["m15_close_profile"]
            assert profile["status"] == "LATE"
            # The first controller turn sampled now_ns before the delayed
            # response became available. The next turn must account it late.
            second_result = supervisor.run_once()
            assert second_result.cycle.failure_types == ()
            assert second_result.cycle.event_ids == ()
            missing = repository.artifact_entries("M15OpportunityMissingnessV1")
            assert len(missing) == 1
            assert missing[0].metadata["missingness"]["reason_code"] == (
                "M15_ORIGIN_FIRST_AVAILABLE_AFTER_ELIGIBILITY_DEADLINE")
            assert not repository.artifact_entries("OpsDecisionEventSourceV1")
            assert not repository.artifact_entries("CandidateSetV2")
            assert any(
                row.metadata.get("event_at_ns") == CLOSE_NS
                and row.available_at_ns == CLOSE_NS + 5_100_000_000
                for row in repository.artifact_entries("PublicObservationIndexV2")
            )
    finally:
        port.close()

    restarted = _source(clock, _BinanceReader(clock))
    restarted_port = _ProfilePort(public_source=restarted, clock=clock, product=product)
    try:
        with OpsSupervisorV2(path, restarted_port, clock_ns=clock) as supervisor:
            result = supervisor.run_once()
            assert result.cycle.failure_types == ()
            assert result.cycle.event_ids == ()
            assert supervisor.repository is not None
            assert len(supervisor.repository.artifact_entries("M15OpportunityMissingnessV1")) == 1
            assert not supervisor.repository.artifact_entries("OpsDecisionEventSourceV1")
            assert not supervisor.repository.artifact_entries("CandidateSetV2")
    finally:
        restarted_port.close()
