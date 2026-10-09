"""Session-041 S4-S8 research sidecar authority and chronology checks."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.chronology import causal_artifact, chronology_ref, record_computation
from atlas.v2.data.active_history import advance
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.health import PublicSourceHealthV2, PublicSourceStateV2
from atlas.v2.data.history import ImportedObservationV2, IndexedCausalBarV2, ParquetObservationArchiveV2
from atlas.v2.data.microstructure import SequenceValidBookV2
from atlas.v2.data.raw import RawObservationV2
from atlas.v2.instruments import EnvironmentV2, UniverseContractV2, VenueV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.news.events import (
    AssetMappingStateV2,
    NewsEventV2,
    NewsSourceClassV2,
    _research_derived_evidence_available,
)
from atlas.v2.runtime.action_critic_shadow import build_sealed_action_assessment
from atlas.v2.runtime.active_history import ActiveHistoryPageV1
from atlas.v2.runtime.full_strategy_surface import (
    S7DirectionalInputV1,
    S7PopulationEvidenceV1,
    _derive_m5_from_m1,
    _fit_s7_pre_event_beta,
    _latest_s8_pair_catalog,
    _normalized_derivatives,
    _persist_native_history_bar,
    _source_visible,
    compose_full_strategy_surface,
    load_full_strategy_inputs,
    register_s8_pair_catalog,
)
from atlas.v2.strategies.s8_pairs import S8PairDefinitionV2

from . import test_session029_action_critic as critic_support
from .session023_support import research_case
from .test_session014_core import KEY
from .test_session017_risk import CUTOFF

critic_case = critic_support.frozen_case


def _archive_sources(repository, rows):
    chunk_id = sha256_json([raw.content_hash for _, raw, _, _ in rows])
    ParquetObservationArchiveV2(Path(repository.path).parent / "ops-observations").write_observation_chunk(
        chunk_id, tuple(ImportedObservationV2(index, raw, payload, bar)
            for index, (_, raw, payload, bar) in enumerate(rows, start=1)))
    refs = []
    for key, raw, _, bar in rows:
        ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": raw.record_id})
        metadata = {name: raw.to_dict()[name] for name in (
            "record_id", "source_id", "event_type", "instrument_revision", "event_at_ns", "published_at_ns",
            "translation_version", "revision_of", "quality_flags", "availability_class",
            "replay_available_at_ns", "raw_payload_hash")}
        metadata.update({"instrument_key_json": key.to_canonical_json(), "archive_chunk_id": chunk_id,
            "bar_content_hash": bar.content_hash if bar else None})
        repository.register_artifact(ArtifactIndexEntryV2(ref, "PublicObservationIndexV2", raw.content_hash,
            raw.received_at_ns, raw.available_at_ns, metadata))
        refs.append(ref)
    return tuple(refs)


def _bar(key, interval, close_at, *, offset=0, received_at=None):
    payload = canonical_json({"symbol": key.native_symbol, "close": close_at, "offset": offset}).encode()
    received = close_at if received_at is None else received_at
    raw = RawObservationV2.build(instrument_revision=key.contract_revision,
        source_id="BYBIT_PUBLIC_V2" if key.venue == VenueV2.BYBIT else "BINANCE_USDM_PUBLIC_V2",
        event_type=f"BAR_{interval.value}", event_at_ns=close_at,
        received_at_ns=received, ingested_at_ns=received, available_at_ns=received,
        translation_version="SESSION041_EXACT_FIXTURE", payload=payload,
        sequence=f"{key.native_symbol}:{close_at}")
    close = Decimal(100) + Decimal(offset)
    bar = CausalBarV2(raw, interval, close_at - interval.duration_ns, close_at,
        Decimal(100), max(Decimal(101), close), min(Decimal(99), close), close, Decimal(2), True)
    return raw, payload, bar


def test_full_surface_emits_explicit_not_estimable_roles_and_zero_authority(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        universe = research_case(repository).universe
        result = compose_full_strategy_surface(repository, universe=universe, cutoff_ns=CUTOFF,
            clock_ns=lambda: CUTOFF + 10)

        assert result.role_status == {
            "S4": "NOT_ESTIMABLE",
            "S5": "NOT_ESTIMABLE",
            "S6": "NOT_ESTIMABLE",
            "S7_DIRECTIONAL_SHADOW": "NOT_ESTIMABLE",
            "MODEL_ARENA_OPTIONAL": "NOT_ESTIMABLE",
            "S8_RESEARCH_BASKET": "NOT_ESTIMABLE",
        }
        entry = repository.get_artifact(result.content_hash)
        assert entry is not None and entry.artifact_type == "FULL_STRATEGY_RESEARCH_SURFACE_V1"
        body = entry.metadata["surface"]
        assert body["candidate_action_refs"] == ()
        assert body["authority"] == "ZERO"
        assert body["trade_plan_allowed"] is False


def test_s4_requires_indexed_feature_and_preserves_actual_feature_publication(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        universe = research_case(repository).universe
        feature = SequenceValidBookV2(instrument=KEY, source_id="SESSION041_FIXTURE",
            channel="orderbook.50.BTCUSDT", sequence_semantics="BYBIT_U", warmup_ns=0,
            stale_ns=1000, declared_cadence_ns=10).feature(cutoff_ns=CUTOFF)
        with pytest.raises(ValueError, match="missing or unavailable"):
            compose_full_strategy_surface(repository, universe=universe, cutoff_ns=CUTOFF,
                s4_features={KEY: feature}, clock_ns=lambda: CUTOFF + 10)

        repository.register_artifact(ArtifactIndexEntryV2(feature.content_hash,
            "S4FeatureArtifactV2", feature.content_hash, CUTOFF + 1, CUTOFF + 1,
            {"feature": feature.to_dict()}))
        record_computation(repository, artifact_ref=feature.content_hash, information_cutoff_ns=CUTOFF,
            started_ns=CUTOFF + 1, finished_ns=CUTOFF + 1, available_ns=CUTOFF + 1,
            input_refs=feature.input_refs, deadline_ns=CUTOFF + 1)
        result = compose_full_strategy_surface(repository, universe=universe, cutoff_ns=CUTOFF,
            s4_features={KEY: feature}, clock_ns=lambda: CUTOFF + 10)
        assert result.published_at_ns == CUTOFF + 10
        feature_entry = repository.get_artifact(feature.content_hash)
        assert feature_entry is not None and feature_entry.available_at_ns == CUTOFF + 1
        role_entry = repository.get_artifact(result.role_refs["S4"][0])
        assert role_entry is not None and role_entry.available_at_ns == CUTOFF + 10
        assert role_entry.metadata["role"]["information_cutoff_ns"] == CUTOFF


def test_universe_must_be_visible_at_information_cutoff(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        base = research_case(repository).universe
        late = UniverseContractV2(
            envelope=replace(base.envelope, content_hash="", created_at_ns=CUTOFF + 1,
                available_at_ns=CUTOFF + 1),
            universe_version=base.universe_version, decision_slot_ns=CUTOFF + 2,
            selection_policy_hash=base.selection_policy_hash, entries=base.entries,
        )
        with pytest.raises(ValueError, match="valid cutoff-sealed computation receipt"):
            compose_full_strategy_surface(repository, universe=late, cutoff_ns=CUTOFF,
                clock_ns=lambda: CUTOFF + 10)


def test_global_population_bound_counts_nested_prepared_histories(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        universe = research_case(repository).universe
        oversized = {KEY: tuple(None for _ in range(65_537))}
        with pytest.raises(ValueError, match="fixed bound"):
            compose_full_strategy_surface(repository, universe=universe, cutoff_ns=CUTOFF,
                s6_hourly_bars=oversized, clock_ns=lambda: CUTOFF + 10)


def test_s7_calendar_or_empty_context_never_generates_directional_rows(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        universe = research_case(repository).universe
        result = compose_full_strategy_surface(repository, universe=universe, cutoff_ns=CUTOFF,
            clock_ns=lambda: CUTOFF + 10)
        assert result.role_status["S7_DIRECTIONAL_SHADOW"] == "NOT_ESTIMABLE"
        assert result.missing_reasons["S7_DIRECTIONAL_SHADOW"] == (
            "NO_EXPLICIT_AUTHENTICATED_EVENT_REACTION_INPUT",
        )
        assert result.to_dict()["candidate_action_refs"] == []


def test_explicit_s7_inputs_cannot_define_the_complete_indexed_event_population(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        universe = research_case(repository).universe
        events = []
        for index in range(2):
            available = CUTOFF - 20 + index
            event_id = sha256_json({"s7-explicit-population": index})
            event = NewsEventV2(
                event_id, "FIXTURE_NEWS", NewsSourceClassV2.OFFICIAL_VENUE_PROJECT_SECURITY,
                "https://news.example.org/s7-population", sha256_json({"revision": index}),
                None, available, available, (KEY.base_asset_id,), "SECURITY_INCIDENT", "HIGH",
                Decimal("0.9"), (f"event {index}",), sha256_json("duplicate-group"),
                sha256_json("raw"), sha256_json("receipt"), sha256_json("health"),
                "UNKNOWN", None, AssetMappingStateV2.UNAMBIGUOUS, None,
            )
            repository.register_artifact(ArtifactIndexEntryV2(
                event.event_id, "NewsEventV2", event.semantic_hash, available,
                event.available_at_ns, {"event": event.to_dict()},
            ))
            events.append(event)

        explicit = S7DirectionalInputV1(events[1], KEY, None, None, (), None, None, None, None, None)
        result = compose_full_strategy_surface(repository, universe=universe, cutoff_ns=CUTOFF,
            s7_inputs=(explicit,), clock_ns=lambda: CUTOFF + 10)
        role = repository.get_artifact(result.role_refs["S7_DIRECTIONAL_SHADOW"][0])
        assert role is not None
        population = role.metadata["role"]["payload"]["population_selection"]

        assert population["status"] == "COMPLETE"
        assert population["observed_event_refs"] == (events[1].event_id, events[0].event_id)
        assert population["selected_event_refs"] == (events[1].event_id, events[0].event_id)
        assert population["directional_inference_allowed"] is True

        forged = S7PopulationEvidenceV1(CUTOFF,
            (events[1].event_id,), (events[1].event_id,))
        with pytest.raises(ValueError, match="differs from indexed source population"):
            compose_full_strategy_surface(repository, universe=universe, cutoff_ns=CUTOFF,
                s7_inputs=(explicit,), s7_population=forged, clock_ns=lambda: CUTOFF + 10)

        altered_event = replace(events[1], event_type="CALLER_ALTERED_EVENT")
        with pytest.raises(ValueError, match="outside the recorded event selection"):
            compose_full_strategy_surface(repository, universe=universe, cutoff_ns=CUTOFF,
                s7_inputs=(replace(explicit, event=altered_event),), clock_ns=lambda: CUTOFF + 10)


def test_derived_surface_waits_for_actual_universe_publication(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        base = research_case(repository).universe
        actual_publication = CUTOFF + 100
        published = UniverseContractV2(
            envelope=replace(base.envelope, content_hash="", created_at_ns=actual_publication,
                available_at_ns=actual_publication),
            universe_version=base.universe_version, decision_slot_ns=actual_publication + 100,
            selection_policy_hash=base.selection_policy_hash, entries=base.entries,
        )
        repository.register_artifact(ArtifactIndexEntryV2(
            published.content_hash, published.ARTIFACT_TYPE, published.content_hash,
            actual_publication, actual_publication, {"universe": published.to_dict()},
        ))
        from atlas.v2.chronology import record_computation

        record_computation(repository, artifact_ref=published.content_hash,
            information_cutoff_ns=CUTOFF, started_ns=actual_publication,
            finished_ns=actual_publication, available_ns=actual_publication,
            input_refs=published.envelope.input_refs, deadline_ns=published.decision_slot_ns)
        result = compose_full_strategy_surface(repository, universe=published, cutoff_ns=CUTOFF,
            clock_ns=lambda: actual_publication + 10)
        assert result.published_at_ns == actual_publication + 10
        assert result.cutoff_ns == CUTOFF


def test_s8_uses_only_exact_owner_registered_pair_definitions(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        other_key = replace(KEY, native_symbol="ETHUSDT", base_asset_id="ETH")
        pair = S8PairDefinitionV2("BTC-ETH", "BTC versus ETH linear perpetual relative value",
            KEY, other_key, "OLS_LOG_PRICE_A_ON_LOG_PRICE_B_30D_HOURLY_V1",
            "LOG_A_MINUS_ALPHA_MINUS_BETA_LOG_B")
        assert _latest_s8_pair_catalog(repository, cutoff_ns=CUTOFF) == ()

        catalog_ref = register_s8_pair_catalog(repository, owner_id="fixture-owner",
            pairs=(pair,), available_at_ns=CUTOFF - 1)
        assert repository.get_artifact(catalog_ref) is not None
        assert _latest_s8_pair_catalog(repository, cutoff_ns=CUTOFF) == (pair,)
        assert _latest_s8_pair_catalog(repository, cutoff_ns=CUTOFF - 2) == ()


def test_s7_m5_aggregation_keeps_exact_native_m1_origins_and_late_publication(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        start = CUTOFF - BarIntervalV2.M5.duration_ns
        indexed = []
        for offset in range(5):
            open_at = start + offset * BarIntervalV2.M1.duration_ns
            close_at = open_at + BarIntervalV2.M1.duration_ns
            raw = RawObservationV2.build(instrument_revision=KEY.contract_revision,
                source_id="BYBIT_PUBLIC_V2", event_type="BAR_1M", event_at_ns=close_at,
                received_at_ns=close_at, ingested_at_ns=close_at, available_at_ns=close_at,
                translation_version="SESSION041_TEST_M1", payload={"minute": offset})
            bar = CausalBarV2(raw, BarIntervalV2.M1, open_at, close_at,
                Decimal(100 + offset), Decimal(102 + offset), Decimal(99 + offset),
                Decimal(101 + offset), Decimal(2), True)
            ref = _archive_sources(repository, ((KEY, raw,
                canonical_json({"minute": offset}).encode(), bar),))[0]
            indexed.append(IndexedCausalBarV2(bar, ref))

        aggregated = _derive_m5_from_m1(repository, key=KEY, indexed_m1=tuple(indexed),
            cutoff_ns=CUTOFF, clock_ns=lambda: CUTOFF + 100,
            service_callback=None)
        assert len(aggregated) == 1
        assert aggregated[0].interval == BarIntervalV2.M5
        assert aggregated[0].open == Decimal(100)
        assert aggregated[0].close == Decimal(105)
        entry = repository.get_artifact(aggregated[0].content_hash)
        assert entry is not None and entry.artifact_type == "S7DerivedM5BarV1"
        assert entry.available_at_ns == CUTOFF + 100
        assert len(entry.metadata["input_refs"]) == 5
        receipt = repository.get_artifact(entry.metadata["aggregation_receipt_ref"])
        assert receipt is not None and receipt.available_at_ns == CUTOFF + 100
        assert receipt.metadata["receipt"]["information_cutoff_ns"] == CUTOFF


def test_s7_event_overflow_is_deterministic_explicit_and_does_not_stop_other_roles(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        universe = research_case(repository).universe
        prior_event_ref = None
        event_refs = []
        first_available = CUTOFF - 10_000
        for index in range(65):
            available_at = first_available + index
            event_id = sha256_json({"s7-retained-revision": index})
            event = NewsEventV2(
                event_id, "FIXTURE_NEWS", NewsSourceClassV2.OFFICIAL_VENUE_PROJECT_SECURITY,
                "https://news.example.org/same-item", sha256_json({"revision": index}),
                None, available_at, available_at, (KEY.base_asset_id,), "SECURITY_INCIDENT", "HIGH",
                Decimal("0.9"), (f"revision {index}",), sha256_json("same-duplicate-group"),
                sha256_json("raw"), sha256_json("receipt"), sha256_json("health"),
                "UNKNOWN", None, AssetMappingStateV2.UNAMBIGUOUS, prior_event_ref,
            )
            repository.register_artifact(ArtifactIndexEntryV2(
                event.event_id, "NewsEventV2", event.semantic_hash, available_at,
                event.available_at_ns, {"event": event.to_dict()},
            ))
            event_refs.append(event.event_id)
            prior_event_ref = event.event_id

        histories = {}
        loaded = load_full_strategy_inputs(repository, universe, CUTOFF, histories,
            lambda: CUTOFF + 10)
        loaded_again = load_full_strategy_inputs(repository, universe, CUTOFF, histories,
            lambda: CUTOFF + 10)
        population = loaded.s7_population
        assert population is not None and population.overflow
        assert population == loaded_again.s7_population
        assert len(population.selected_event_refs) == 64
        assert population.observed_event_refs == tuple(event_refs[-1::-1][:65])
        assert population.overflow_event_refs == (event_refs[0],)
        assert not population.page_has_more
        assert loaded.s7_inputs == ()

        result = compose_full_strategy_surface(repository, universe=universe, cutoff_ns=CUTOFF,
            **loaded.compose_kwargs(), clock_ns=lambda: CUTOFF + 20)
        assert result.role_status["S7_DIRECTIONAL_SHADOW"] == "NOT_ESTIMABLE"
        assert result.missing_reasons["S7_DIRECTIONAL_SHADOW"] == ("S7_EVENT_POPULATION_OVERFLOW",)
        role = repository.get_artifact(result.role_refs["S7_DIRECTIONAL_SHADOW"][0])
        assert role is not None
        selection = role.metadata["role"]["payload"]["population_selection"]
        assert selection["status"] == "OVERFLOW"
        assert selection["selected_event_refs"] == population.selected_event_refs
        assert selection["overflow_event_refs"] == (event_refs[0],)
        assert selection["directional_inference_allowed"] is False
        assert {"S4", "S5", "S6", "S8_RESEARCH_BASKET", "MODEL_ARENA_OPTIONAL"} <= set(
            result.role_status)
        assert result.to_dict()["candidate_action_refs"] == []


def test_native_public_locator_validates_raw_hash_and_exact_archive(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        raw, payload, bar = _bar(KEY, BarIntervalV2.H1, CUTOFF)
        ref = _archive_sources(repository, ((KEY, raw, payload, bar),))[0]
        assert ref != raw.content_hash
        assert _source_visible(repository, ref, CUTOFF)
        assert not _source_visible(repository, ref, CUTOFF - 1)
        wrong = sha256_json("unknown unequal-hash source")
        repository.register_artifact(ArtifactIndexEntryV2(wrong, "UnknownArtifactV1", raw.content_hash,
            CUTOFF, CUTOFF, {}))
        assert not _source_visible(repository, wrong, CUTOFF)
        path = next((tmp_path / "ops-observations").glob("*.parquet"))
        path.write_bytes(b"invalid archive")
        assert not _source_visible(repository, ref, CUTOFF)


def test_actual_loader_preserves_native_history_and_services_compose(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        universe = research_case(repository).universe
        raw, payload, bar = _bar(KEY, BarIntervalV2.H1, CUTOFF)
        ref = _archive_sources(repository, ((KEY, raw, payload, bar),))[0]
        indexed = IndexedCausalBarV2(bar, ref)
        state = advance(None, (indexed,), key=KEY, interval=BarIntervalV2.H1)
        services = []
        loaded = load_full_strategy_inputs(repository, universe, CUTOFF,
            {KEY.to_canonical_json(): {BarIntervalV2.H1: ActiveHistoryPageV1(state, True, "READY")}},
            lambda: CUTOFF + 100, service_callback=lambda: services.append(1))
        assert loaded.s6_hourly_bars[KEY] == (bar,)
        entry = repository.get_artifact(bar.content_hash)
        assert entry is not None and entry.artifact_type == "NativeStrategyHistoryBarV1"
        assert entry.available_at_ns == CUTOFF + 100
        assert entry.metadata["input_refs"] == (ref,)
        result = compose_full_strategy_surface(repository, universe=universe, cutoff_ns=CUTOFF,
            **loaded.compose_kwargs(), clock_ns=lambda: CUTOFF + 101,
            service_callback=lambda: services.append(1))
        assert result.to_dict()["capital_authority"] == "ZERO"
        assert len(services) >= 7


def test_loader_reuses_exact_production_causal_bar_index(tmp_path):
    from atlas.v2.runtime.production import _causal_bar_entry

    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        universe = research_case(repository).universe
        raw, payload, bar = _bar(KEY, BarIntervalV2.H1, CUTOFF)
        source_ref = _archive_sources(repository, ((KEY, raw, payload, bar),))[0]
        repository.register_artifact(_causal_bar_entry(bar, source_ref))
        indexed = IndexedCausalBarV2(bar, source_ref)
        state = advance(None, (indexed,), key=KEY, interval=BarIntervalV2.H1)
        loaded = load_full_strategy_inputs(repository, universe, CUTOFF,
            {KEY.to_canonical_json(): {BarIntervalV2.H1: ActiveHistoryPageV1(state, True, "READY")}},
            lambda: CUTOFF + 100)
        assert loaded.s6_hourly_bars[KEY] == (bar,)
        entry = repository.get_artifact(bar.content_hash)
        assert entry.artifact_type == "CausalBarV2"
        assert entry.available_at_ns == raw.available_at_ns
        result = compose_full_strategy_surface(repository, universe=universe, cutoff_ns=CUTOFF,
            **loaded.compose_kwargs(), clock_ns=lambda: CUTOFF + 101)
        assert result.to_dict()["capital_authority"] == "ZERO"


def test_loader_binds_btc_proxy_to_prepared_same_venue_cohort(tmp_path):
    from atlas.v2.strategies.s6_cross_section import LiquidityFundingEvidenceV2

    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        base = research_case(repository).universe
        binance_key = replace(KEY, venue=VenueV2.BINANCE)
        entries = (base.entries[0], replace(base.entries[0], key=binance_key))
        universe = replace(base, envelope=replace(base.envelope, content_hash=""),
            entries=tuple(sorted(entries, key=lambda entry: entry.key.to_canonical_json())))
        rows = tuple((key, *_bar(key, interval, CUTOFF)) for key, interval in (
            (KEY, BarIntervalV2.H1), (KEY, BarIntervalV2.H4), (binance_key, BarIntervalV2.H1)))
        refs = _archive_sources(repository, rows)
        histories = {}
        for (key, _, _, bar), ref in zip(rows, refs, strict=True):
            state = advance(None, (IndexedCausalBarV2(bar, ref),), key=key, interval=bar.interval)
            histories.setdefault(key.to_canonical_json(), {})[bar.interval] = ActiveHistoryPageV1(state, True, "READY")
        health = PublicSourceHealthV2("FIXTURE_PUBLIC", CUTOFF, CUTOFF,
            PublicSourceStateV2.HEALTHY_CURRENT, sha256_json("cohort health"), "exact cohort fixture")
        evidence = {key: LiquidityFundingEvidenceV2(key, CUTOFF, CUTOFF,
            Decimal("1"), Decimal("20000000"), Decimal(".0001"),
            sha256_json({"quote": key.to_dict()}), sha256_json({"funding": key.to_dict()}), health)
            for key in (KEY, binance_key)}
        loaded = load_full_strategy_inputs(repository, universe, CUTOFF, histories,
            lambda: CUTOFF + 10, s6_evidence=evidence, s5_contexts={})
        assert loaded.s6_btc_proxy == KEY


def test_native_m5_beta_uses_only_strict_pre_receipt_sources_and_publishes_later(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        asset = replace(KEY, native_symbol="ETHUSDT", base_asset_id="ETH")
        rows = []
        for offset in range(21):
            close_at = CUTOFF - (20 - offset) * BarIntervalV2.M5.duration_ns
            for key, multiplier in ((KEY, 1), (asset, 2)):
                raw, payload, bar = _bar(key, BarIntervalV2.M5, close_at,
                    offset=multiplier * (offset % 5 + 1))
                rows.append((key, raw, payload, bar))
        refs = _archive_sources(repository, tuple(rows))
        for (key, _, _, bar), ref in zip(rows, refs, strict=True):
            _persist_native_history_bar(repository, key=key, indexed_bar=IndexedCausalBarV2(bar, ref),
                cutoff_ns=CUTOFF, clock_ns=lambda: CUTOFF + 100)
        asset_bars = tuple(bar for key, _, _, bar in rows if key == asset)
        btc_bars = tuple(bar for key, _, _, bar in rows if key == KEY)
        beta = _fit_s7_pre_event_beta(repository, key=asset, btc_proxy=KEY,
            asset_bars=asset_bars, btc_bars=btc_bars, event_at_ns=CUTOFF,
            clock_ns=lambda: CUTOFF + 100)
        assert beta is not None and beta.estimated_through_ns == CUTOFF - BarIntervalV2.M5.duration_ns
        assert beta.available_at_ns == CUTOFF + 100
        assert _research_derived_evidence_available(repository, beta.evidence_ref,
            information_cutoff_ns=CUTOFF, published_at_ns=CUTOFF + 100)
        assert _fit_s7_pre_event_beta(repository, key=asset, btc_proxy=KEY,
            asset_bars=asset_bars[1:], btc_bars=btc_bars, event_at_ns=CUTOFF,
            clock_ns=lambda: CUTOFF + 100) is None


def test_binance_liquidity_retains_distinct_quote_turnover_and_funding_refs(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        base = research_case(repository).universe
        key = replace(KEY, venue=VenueV2.BINANCE)
        universe = replace(base, envelope=replace(base.envelope, content_hash=""),
            entries=(replace(base.entries[0], key=key),))
        rows = []
        for event, body in (("BOOK_TICKER", {"bidPrice": "99", "askPrice": "101"}),
                            ("TICKER_24H", {"quoteVolume": "20000000"}),
                            ("MARK_INDEX_CURRENT_FUNDING", {"lastFundingRate": "0.0001"})):
            payload = canonical_json(body).encode()
            raw = RawObservationV2.build(instrument_revision=key.contract_revision, source_id="BINANCE_USDM_PUBLIC_V2",
                event_type=event, event_at_ns=CUTOFF, received_at_ns=CUTOFF,
                ingested_at_ns=CUTOFF, available_at_ns=CUTOFF, payload=payload,
                translation_version="SESSION041_BINANCE_SCHEMA_FIXTURE")
            rows.append((key, raw, payload, None))
        refs = _archive_sources(repository, tuple(rows))
        health = PublicSourceHealthV2("BINANCE_USDM_PUBLIC_V2", CUTOFF, CUTOFF,
            PublicSourceStateV2.HEALTHY_CURRENT, sha256_json("health"), "exact source fixture")
        repository.register_artifact(ArtifactIndexEntryV2(health.content_hash, "PublicSourceHealthV2",
            health.content_hash, CUTOFF, CUTOFF, {"health": health.to_dict()}))
        receipt = {"authority": "ZERO", "available_at_ns": CUTOFF,
            "source_observation_refs": {"BINANCE_USDM_PUBLIC_V2": list(refs)}}
        receipt_ref = sha256_json(receipt)
        repository.register_artifact(ArtifactIndexEntryV2(receipt_ref, "BroadPublicAcquisitionReceiptV2",
            receipt_ref, CUTOFF, CUTOFF, {"receipt": receipt}))
        liquidity, _, _ = _normalized_derivatives(repository, universe=universe, cutoff_ns=CUTOFF)
        evidence = liquidity[key]
        assert evidence.liquidity_ref == refs[0]
        assert evidence.turnover_ref == refs[1]
        assert evidence.funding_ref == refs[2]
        assert evidence.available_at_ns == CUTOFF
        assert evidence.quote_turnover_24h == Decimal("20000000")
        assert "turnover_ref" not in replace(evidence, turnover_ref=None).to_dict()


def test_critic_packet_retains_exact_analogue_retrieval_receipt_and_replay(critic_case):
    path, receipt, receipt_ref, sealed, profile, _ = critic_case
    retrieval_ref = receipt.result.stages[9].artifact_refs[1]
    assert retrieval_ref in sealed.packet.artifact_refs
    assert sealed.packet.artifact_types[retrieval_ref] == "RuntimeAnalogueRetrievalReceiptV1"
    assert sealed.packet.summaries[retrieval_ref]["summary"]["action_hash"] == sealed.packet.action_hash
    with OpsRepository(path) as repository:
        replay = build_sealed_action_assessment(repository=repository, receipt=receipt,
            receipt_ref=receipt_ref, profile=profile)
        assert replay.packet.to_dict() == sealed.packet.to_dict()


@pytest.mark.parametrize("mode", ("timely_derived", "future_raw", "after_original_deadline"))
def test_critic_market_lineage_keeps_t0_and_original_deadline(critic_case, mode):
    path, receipt, _, sealed, profile, _ = critic_case
    t0 = receipt.event.information_cutoff_ns
    publication = t0 + 1 if mode != "after_original_deadline" else sealed.packet.original_deadline_d_ns + 1
    with OpsRepository(path) as repository:
        body = {"fixture": mode, "source_refs": [receipt.event.trigger_ref]}
        ref = sha256_json(body)
        kind = "UniverseObservationV2" if mode != "future_raw" else "UnknownMarketSourceV1"
        repository.register_artifact(ArtifactIndexEntryV2(ref, kind, ref,
            publication, publication, {"observation": body}))
        if mode != "future_raw":
            record_computation(repository, artifact_ref=ref, information_cutoff_ns=t0,
                started_ns=publication, finished_ns=publication, available_ns=publication,
                input_refs=(receipt.event.trigger_ref,), deadline_ns=sealed.packet.original_deadline_d_ns)
        event = replace(receipt.event, causal_input_refs=(*receipt.event.causal_input_refs, ref))
        changed = replace(receipt, event=event)
        receipt_ref = sha256_json({"fixture_receipt": changed.content_hash})
        repository.register_artifact(ArtifactIndexEntryV2(receipt_ref, "OpsSupervisorReceiptV1",
            changed.content_hash, changed.created_at_ns, changed.created_at_ns,
            {"receipt": changed.to_dict()}))
        if mode == "timely_derived":
            packet = build_sealed_action_assessment(repository=repository, receipt=changed,
                receipt_ref=receipt_ref, profile=profile).packet
            assert ref in packet.artifact_refs and ref not in packet.market_evidence_refs
            assert chronology_ref(ref) in packet.artifact_refs
            assert receipt.event.trigger_ref in packet.market_evidence_refs
            assert packet.availability_by_ref[ref] == publication
            assert packet.source_cutoff_t0_ns == t0
            assert packet.original_deadline_d_ns == sealed.packet.original_deadline_d_ns
        else:
            with pytest.raises(ValueError, match="FUTURE_MARKET_EVIDENCE"):
                build_sealed_action_assessment(repository=repository, receipt=changed,
                    receipt_ref=receipt_ref, profile=profile)


S5_KEY = replace(KEY, environment=EnvironmentV2.MAINNET)


def _s5_fixture(repository, *, continuation=True, oi=True):
    from atlas.v2.data.derivatives import (
        DerivativeAvailabilityV2,
        FundingKindV2,
        FundingObservationV2,
        OpenInterestObservationV2,
        build_s5_crowding_context,
        oi_change_15m_evidence,
    )
    from atlas.v2.data.microstructure import BookStateV2
    from atlas.v2.runtime.full_strategy_surface import _persist_s5_derived

    health_ref = sha256_json({"fixture": "exact-s5-source-health"})
    repository.register_artifact(ArtifactIndexEntryV2(health_ref, "FixtureHealth", health_ref,
        CUTOFF - 30 * BarIntervalV2.M15.duration_ns, CUTOFF - 30 * BarIntervalV2.M15.duration_ns, {}))
    feature = SequenceValidBookV2(instrument=S5_KEY, source_id="BYBIT_PUBLIC_V2",
        channel="orderbook.50.BTCUSDT", sequence_semantics="BYBIT_U", warmup_ns=0,
        stale_ns=1000, declared_cadence_ns=10).feature(cutoff_ns=CUTOFF)
    feature = replace(feature, sequence_state=BookStateV2.VALID, missing_reason=None,
        source_health="HEALTHY_CURRENT", source_health_ref=health_ref,
        trade_coverage_state="QUALIFIED", trade_coverage_ref=health_ref, input_refs=(health_ref,),
        flow_price_response_windows=(("30", "-12" if continuation else "12", "1", "ESTIMABLE"),),
        price_response_windows=(("30", "1", "ESTIMABLE"),))
    _persist_s5_derived(repository, artifact_ref=feature.content_hash, artifact_type="S4FeatureArtifactV2",
        payload={"feature": feature.to_dict(), "instrument_key_json": S5_KEY.to_canonical_json()},
        input_refs=feature.input_refs, cutoff_ns=CUTOFF, clock_ns=lambda: CUTOFF + 10)
    channel = "V5 REST ticker/funding-history/open-interest"
    funding = FundingObservationV2(S5_KEY, "BYBIT_PUBLIC_V2", FundingKindV2.CURRENT, Decimal(".0001"),
        "RATE", CUTOFF - 1, CUTOFF - 1, CUTOFF - 1, None, feature.capability_matrix_ref,
        health_ref, DerivativeAvailabilityV2.ACTUAL_RECEIPT, source_health="HEALTHY_CURRENT",
        source_health_ref=health_ref, channel=channel)
    def oi_raw_ref(event, quantity):
        ref = sha256_json({"fixture_oi_event": event, "quantity": quantity})
        repository.register_artifact(ArtifactIndexEntryV2(ref, "FixtureOIRaw", ref, event, event, {}))
        return ref

    observations = tuple(OpenInterestObservationV2(S5_KEY, "BYBIT_PUBLIC_V2", Decimal(quantity), "CONTRACTS",
        None, None, event, event, event, feature.capability_matrix_ref, oi_raw_ref(event, quantity),
        DerivativeAvailabilityV2.ACTUAL_RECEIPT, source_health="HEALTHY_CURRENT",
        source_health_ref=health_ref, channel=channel) for event, quantity in (
            (CUTOFF - BarIntervalV2.M15.duration_ns, 100), (CUTOFF, 90))) if oi else ()
    for observation in (funding, *observations):
        _persist_s5_derived(repository, artifact_ref=observation.content_hash,
            artifact_type=type(observation).__name__, payload={"observation": observation.to_dict()},
            input_refs=(health_ref, observation.raw_content_ref), cutoff_ns=CUTOFF, clock_ns=lambda: CUTOFF + 10)
    delta = oi_change_15m_evidence(observations, cutoff_ns=CUTOFF, instrument=S5_KEY)
    if delta is not None:
        _persist_s5_derived(repository, artifact_ref=delta.content_hash, artifact_type="OIChangeEvidenceV2",
            payload={"evidence": delta.to_dict()}, input_refs=delta.input_refs,
            cutoff_ns=CUTOFF, clock_ns=lambda: CUTOFF + 10)
    context = build_s5_crowding_context(instrument=S5_KEY, cutoff_ns=CUTOFF,
        funding=(funding,), open_interest=observations, liquidity_state="SEQUENCE_VALID_S4")
    rows = []
    for index in range(23):
        close_at = CUTOFF - (22 - index) * BarIntervalV2.M15.duration_ns
        offset = 0 if index < 20 else -5 if index == 20 else -6 if continuation else 0 if index == 21 else 3
        rows.append((S5_KEY, *_bar(S5_KEY, BarIntervalV2.M15, close_at, offset=offset)))
    break_at = CUTOFF - 2 * BarIntervalV2.M15.duration_ns
    h4_end = break_at - break_at % BarIntervalV2.H4.duration_ns
    for index in range(2):
        rows.append((S5_KEY, *_bar(S5_KEY, BarIntervalV2.H4,
            h4_end - (1 - index) * BarIntervalV2.H4.duration_ns, offset=1 - index)))
    raw_refs = _archive_sources(repository, tuple(rows))
    indexed = tuple(IndexedCausalBarV2(row[3], ref) for row, ref in zip(rows, raw_refs, strict=True))
    return feature, context, observations, indexed[:23], indexed[23:]


def _prepare_fixture_s5(repository, fixture, delivery):
    from atlas.v2.runtime.full_strategy_surface import _prepare_s5_diagnostics

    feature, context, observations, m15, h4 = fixture
    return _prepare_s5_diagnostics(repository, keys={S5_KEY}, cutoff_ns=CUTOFF,
        m15_indexed={S5_KEY: m15}, h4_indexed={S5_KEY: h4}, features={S5_KEY: feature},
        baselines={}, contexts={S5_KEY: context}, open_interest={S5_KEY: observations},
        clock_ns=lambda: delivery, service_callback=None)[S5_KEY]


def test_s5_actual_delivery_preserves_original_cutoff_and_restart_latency(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        fixture = _s5_fixture(repository)
        first = _prepare_fixture_s5(repository, fixture, CUTOFF + 20)
        assert first.information_cutoff_ns == CUTOFF
        assert first.delivered_at_ns == CUTOFF + 20
        assert first.continuation.state == "DELEVERAGING_EVIDENCE"
        assert first.continuation.break_evidence.available_at_ns == CUTOFF + 20
        assert first.continuation.confirmation is None
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        second = _prepare_fixture_s5(repository, fixture, CUTOFF + 30)
        assert second.continuation.state == "CONTINUATION_CONFIRMED"
        assert second.continuation.confirmation.available_at_ns == CUTOFF + 30
        assert second.continuation.break_evidence.available_at_ns == CUTOFF + 20
        assert second.continuation.confirmation_latency_ns == 10
        assert second.to_dict()["candidate_action_refs"] == []


def test_s5_funding_alone_and_unqualified_flow_cannot_confirm(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        fixture = _s5_fixture(repository, oi=False)
        result = _prepare_fixture_s5(repository, fixture, CUTOFF + 20)
        assert result.continuation.state == "NOT_ESTIMABLE"
        assert result.continuation.break_evidence is None
        assert "S5_SUPPORTED_CROWDING_STRUCTURE_LIQUIDITY_REQUIRED" in result.criteria_payload["missing_reasons"]
        feature, context, observations, m15, h4 = fixture
        unqualified = replace(feature, trade_coverage_state="UNKNOWN", trade_coverage_ref=None)
        from atlas.v2.runtime.full_strategy_surface import _persist_s5_derived

        _persist_s5_derived(repository, artifact_ref=unqualified.content_hash, artifact_type="S4FeatureArtifactV2",
            payload={"feature": unqualified.to_dict()}, input_refs=unqualified.input_refs,
            cutoff_ns=CUTOFF, clock_ns=lambda: CUTOFF + 30)
        result = _prepare_fixture_s5(repository, (unqualified, context, observations, m15, h4), CUTOFF + 30)
        assert result.continuation.confirmation is None


def test_s5_reversal_retains_all_stages_and_missing_actual_liquidation_reason(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        result = _prepare_fixture_s5(repository, _s5_fixture(repository, continuation=False), CUTOFF + 20)
        assert result.reversal.state == "NOT_ESTIMABLE_LIQUIDATION_BASELINE_UNAVAILABLE"
        assert result.reversal.missing_reason == "VENUE_SPECIFIC_PRIOR_LIQUIDATION_BASELINE_REQUIRED"
        stages = {stage.stage: stage for stage in result.reversal.stage_evidence}
        assert {"S4_FLOW_STATE", "S4_ABSORPTION", "EXHAUSTION", "RECLAIM", "FLOW_REVERSAL"} <= set(stages)
        assert stages["S4_FLOW_STATE"].available_at_ns == CUTOFF + 10
        assert stages["S4_ABSORPTION"].available_at_ns == CUTOFF + 20
        assert stages["EXHAUSTION"].available_at_ns == CUTOFF + 20


def test_s4_baseline_requires_three_qualified_prior_exact_samples(tmp_path):
    from atlas.v2.runtime.full_strategy_surface import _fit_prior_s4_baselines

    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        feature, _, _, _, _ = _s5_fixture(repository)
        assert _fit_prior_s4_baselines(repository, features={S5_KEY: feature}, cutoff_ns=CUTOFF,
            clock_ns=lambda: CUTOFF + 20, service_callback=None) == {}
        refs = []
        for offset in range(3):
            prior = replace(feature, cutoff_ns=CUTOFF - (offset + 1) * 1_000_000_000,
                flow_price_response_windows=(("30", str(offset + 1), "1", "ESTIMABLE"),))
            repository.register_artifact(ArtifactIndexEntryV2(prior.content_hash, "S4FeatureArtifactV2",
                prior.content_hash, prior.cutoff_ns, prior.cutoff_ns,
                {"feature": prior.to_dict(), "instrument_key_json": S5_KEY.to_canonical_json(),
                 "input_refs": list(prior.input_refs)}))
            refs.append(prior.content_hash)
        fitted = _fit_prior_s4_baselines(repository, features={S5_KEY: feature}, cutoff_ns=CUTOFF,
            clock_ns=lambda: CUTOFF + 20, service_callback=None)[S5_KEY]
        assert set(fitted.input_refs) == set(refs)
        assert fitted.fit_window_end_ns < fitted.fit_cutoff_ns < CUTOFF
        receipt = repository.get_artifact(chronology_ref(fitted.content_hash))
        assert receipt.metadata["chronology"]["available_at_ns"] == CUTOFF + 20
        assert set(receipt.metadata["chronology"]["input_refs"]) == set(refs)
        base = research_case(repository).universe
        universe = replace(base, envelope=replace(base.envelope, content_hash=""),
            entries=tuple(replace(entry, key=S5_KEY) if entry.key == KEY else entry for entry in base.entries))
        result = compose_full_strategy_surface(repository, universe=universe, cutoff_ns=CUTOFF,
            s4_features={S5_KEY: feature}, s4_baselines={S5_KEY: fitted}, clock_ns=lambda: CUTOFF + 30)
        assert result.published_at_ns == CUTOFF + 30
        assert result.to_dict()["candidate_action_refs"] == []


def test_s4_baseline_rejects_index_body_that_differs_from_typed_fit(tmp_path):
    from atlas.v2.data.microstructure import fit_s4_expected_response_baseline
    from atlas.v2.runtime.full_strategy_surface import _s4_baseline_body

    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        feature, _, _, _, _ = _s5_fixture(repository)
        refs = tuple(sha256_json({"prior fit sample": offset}) for offset in range(3))
        for ref in refs:
            repository.register_artifact(ArtifactIndexEntryV2(ref, "PriorFitRawFixture", ref,
                CUTOFF - 5, CUTOFF - 5, {}))
        fitted = fit_s4_expected_response_baseline(history=tuple(
            (CUTOFF - 4 + offset, Decimal(offset + 1), Decimal(offset + 2), ref)
            for offset, ref in enumerate(refs)), fit_cutoff_ns=CUTOFF - 1)
        repository.register_artifact(ArtifactIndexEntryV2(fitted.content_hash, "S4ExpectedResponseBaselineV2",
            fitted.content_hash, CUTOFF + 20, CUTOFF + 20,
            {"baseline": {**_s4_baseline_body(fitted), "slope": "999"}, "input_refs": list(refs)}))
        record_computation(repository, artifact_ref=fitted.content_hash, information_cutoff_ns=CUTOFF,
            started_ns=CUTOFF + 20, finished_ns=CUTOFF + 20, available_ns=CUTOFF + 20,
            input_refs=refs, deadline_ns=CUTOFF + 20)
        base = research_case(repository).universe
        universe = replace(base, envelope=replace(base.envelope, content_hash=""),
            entries=tuple(replace(entry, key=S5_KEY) if entry.key == KEY else entry for entry in base.entries))
        with pytest.raises(ValueError, match="not exact"):
            compose_full_strategy_surface(repository, universe=universe, cutoff_ns=CUTOFF,
                s4_features={S5_KEY: feature}, s4_baselines={S5_KEY: fitted}, clock_ns=lambda: CUTOFF + 30)


def test_prepared_s5_composes_with_actual_context_publication_and_receipt(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        base = research_case(repository).universe
        universe = replace(base, envelope=replace(base.envelope, content_hash=""),
            entries=tuple(replace(entry, key=S5_KEY) if entry.key == KEY else entry for entry in base.entries))
        fixture = _s5_fixture(repository)
        feature, context, _, _, _ = fixture
        prepared = _prepare_fixture_s5(repository, fixture, CUTOFF + 20)
        result = compose_full_strategy_surface(repository, universe=universe, cutoff_ns=CUTOFF,
            s4_features={S5_KEY: feature}, s5_contexts={S5_KEY: context},
            s5_prepared_diagnostics={S5_KEY: prepared}, clock_ns=lambda: CUTOFF + 30)
        assert result.role_status["S5"] == "AVAILABLE"
        assert result.cutoff_ns == CUTOFF
        assert result.published_at_ns == CUTOFF + 30
        context_entry = repository.get_artifact(context.content_hash)
        assert context_entry.available_at_ns == CUTOFF + 30
        receipt = repository.get_artifact(chronology_ref(context.content_hash))
        assert receipt.metadata["chronology"]["market_information_cutoff_ns"] == CUTOFF
        assert receipt.metadata["chronology"]["information_cutoff_ns"] == CUTOFF + 10
        prepared_receipt = repository.get_artifact(chronology_ref(prepared.content_hash))
        assert prepared_receipt.metadata["chronology"]["information_cutoff_ns"] == CUTOFF + 20
        assert causal_artifact(repository, context.content_hash, cutoff_ns=CUTOFF,
            consumer_at_ns=CUTOFF + 30, deadline_ns=CUTOFF + 30)
        assert result.to_dict()["candidate_action_refs"] == []


def test_prepared_s5_rejects_future_raw_source_despite_late_delivery(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        base = research_case(repository).universe
        universe = replace(base, envelope=replace(base.envelope, content_hash=""),
            entries=tuple(replace(entry, key=S5_KEY) if entry.key == KEY else entry for entry in base.entries))
        fixture = _s5_fixture(repository)
        prepared = _prepare_fixture_s5(repository, fixture, CUTOFF + 20)
        future_ref = sha256_json("future market receipt")
        repository.register_artifact(ArtifactIndexEntryV2(future_ref, "FutureMarketSourceV1", future_ref,
            CUTOFF + 1, CUTOFF + 1, {}))
        forged = replace(prepared, input_refs=(*prepared.input_refs, future_ref))
        repository.register_artifact(ArtifactIndexEntryV2(forged.content_hash, "S5PreparedDiagnosticsV1",
            forged.content_hash, CUTOFF + 20, CUTOFF + 20, {"diagnostics": forged.to_dict()}))
        with pytest.raises(ValueError, match="exact causal source receipts"):
            compose_full_strategy_surface(repository, universe=universe, cutoff_ns=CUTOFF,
                s5_prepared_diagnostics={S5_KEY: forged}, clock_ns=lambda: CUTOFF + 30)


def test_prior_prefix_research_receipt_keeps_own_cutoff_and_rejects_future_raw(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        prior_cutoff = CUTOFF - 10
        raw_ref = sha256_json("raw known at earlier prefix")
        derived_ref = sha256_json("late earlier-prefix research fit")
        repository.register_artifact(ArtifactIndexEntryV2(raw_ref, "FixtureRaw", raw_ref,
            prior_cutoff, prior_cutoff, {}))
        repository.register_artifact(ArtifactIndexEntryV2(derived_ref, "S4ExpectedResponseBaselineV2",
            derived_ref, CUTOFF + 1, CUTOFF + 1, {"input_refs": [raw_ref]}))
        record_computation(repository, artifact_ref=derived_ref, information_cutoff_ns=prior_cutoff,
            started_ns=CUTOFF + 1, finished_ns=CUTOFF + 1, available_ns=CUTOFF + 1,
            input_refs=(raw_ref,), deadline_ns=CUTOFF + 1)
        assert causal_artifact(repository, derived_ref, cutoff_ns=CUTOFF,
            consumer_at_ns=CUTOFF + 2, deadline_ns=CUTOFF + 2)
        receipt = repository.get_artifact(chronology_ref(derived_ref))
        assert receipt.metadata["chronology"]["market_information_cutoff_ns"] == prior_cutoff
        future_raw = sha256_json("raw after earlier prefix")
        invalid_derived = sha256_json("invalid earlier-prefix research fit")
        repository.register_artifact(ArtifactIndexEntryV2(future_raw, "FixtureRaw", future_raw,
            prior_cutoff + 1, prior_cutoff + 1, {}))
        repository.register_artifact(ArtifactIndexEntryV2(invalid_derived, "S4ExpectedResponseBaselineV2",
            invalid_derived, CUTOFF + 1, CUTOFF + 1, {"input_refs": [future_raw]}))
        with pytest.raises(ValueError, match="noncausal dependency"):
            record_computation(repository, artifact_ref=invalid_derived, information_cutoff_ns=prior_cutoff,
                started_ns=CUTOFF + 1, finished_ns=CUTOFF + 1, available_ns=CUTOFF + 1,
                input_refs=(future_raw,), deadline_ns=CUTOFF + 1)


def test_s6_reuses_exact_immutable_first_publication(tmp_path):
    from atlas.v2.strategies.s6_cross_section import _index

    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        ref = sha256_json("immutable S6 evidence")
        metadata = {"evidence": {"input_refs": [sha256_json("source")], "spread_bps": "1"}}
        _index(repository, ref, "S6LiquidityFundingEvidenceV2", CUTOFF + 1, metadata)
        _index(repository, ref, "S6LiquidityFundingEvidenceV2", CUTOFF + 2, metadata)
        assert repository.get_artifact(ref).available_at_ns == CUTOFF + 1
        with pytest.raises(ValueError, match="exact first publication"):
            _index(repository, ref, "S6LiquidityFundingEvidenceV2", CUTOFF + 2,
                {"evidence": {"spread_bps": "2"}})
        with pytest.raises(ValueError, match="exact first publication"):
            _index(repository, ref, "S6LiquidityFundingEvidenceV2", CUTOFF, metadata)
