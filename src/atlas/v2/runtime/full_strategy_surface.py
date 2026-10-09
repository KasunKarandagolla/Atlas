"""Zero-authority orchestration for the typed S4-S8 research surfaces.

This sidecar intentionally does not emit CandidateActionV2. The ordinary
production CandidateSet registry remains the authority boundary; callers may
persist these diagnostics without promoting research hypotheses into plans.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from decimal import Decimal
from pathlib import Path
from typing import Any

from atlas.v2._serialization import canonical_json, sha256_json, timestamp
from atlas.v2.chronology import causal_artifact, chronology_ref, record_computation
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.capabilities import default_evidence_capability_matrix_v2
from atlas.v2.data.derivatives import (
    DerivativeAvailabilityV2,
    FundingKindV2,
    FundingObservationV2,
    OpenInterestObservationV2,
    S5CrowdingContextV2,
    build_s5_crowding_context,
    oi_change_15m_evidence,
)
from atlas.v2.data.health import PublicSourceHealthV2
from atlas.v2.data.history import _open_bounded_archive_chunk_v2
from atlas.v2.data.microstructure import (
    AvailabilityViewV2,
    S4AbsorptionHypothesisV2,
    S4ExpectedResponseBaselineV2,
    S4FeatureArtifactV2,
    SequenceValidBookV2,
    estimate_s4_absorption,
    fit_s4_expected_response_baseline,
    s4_execution_quality_context,
)
from atlas.v2.data.raw import AvailabilityClassV2, RawObservationV2, indexed_availability_matches
from atlas.v2.instruments import InstrumentKeyV2, UniverseContractV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.models.protocol import ForecastArtifactV2, ModelRequestV2
from atlas.v2.news.events import (
    EventReactionArtifactV2,
    EventResolutionV2,
    MarketReturn5MV2,
    NewsEventV2,
    PreEventBetaV2,
    ReactionSpreadEvidenceV2,
    S7DirectionalShadowV2,
    _event_from_dict,
    _source_evidence_available,
)
from atlas.v2.runtime.active_history import ActiveHistoryPageV1
from atlas.v2.strategies.s5_crowding import (
    S5ContinuationArtifactV2,
    S5ContinuationResearchV2,
    S5ReversalArtifactV2,
    StageEvidenceV2,
    evaluate_s5_reversal,
)
from atlas.v2.strategies.s6_cross_section import (
    LiquidityFundingEvidenceV2,
    S6DecisionV2,
    S6ShadowCoordinator,
)
from atlas.v2.strategies.s8_pairs import (
    S8HourlyPriceV2,
    S8LegEvidenceV2,
    S8PairDefinitionV2,
    build_research_basket_forecast,
    persist_s8_basket,
)

SURFACE_VERSION = "FULL_STRATEGY_RESEARCH_SURFACE_V1"
S5_STRUCTURAL_POLICY = "S5_M15_STRUCTURAL_FLOW_RESEARCH_V1"
MAX_S5_STRUCTURAL_BARS = 23
MAX_S4_BASELINE_HISTORY = 128
MAX_S7_EVENTS = 64
MAX_S8_PAIRS = 64
MAX_SURFACE_INPUTS = 4096
MAX_SURFACE_UNIVERSE = 4096
MAX_PREPARED_HISTORY_KEYS = 24
MAX_S4_SEQUENCE_BOOKS = 8
MAX_TOTAL_HISTORY_BARS = 65_536
MAX_BROAD_SOURCE_REFS = 16_384
MAX_SELECTED_RAW_RECORDS = 4_096
S6_BULK_EVENT_TYPES = frozenset({"TICKER_MARK_INDEX_FUNDING_OI", "MARK_INDEX_CURRENT_FUNDING"})
S5_FUNDING_EVENT_TYPES = frozenset({"FUNDING", "FUNDING_HISTORY"})
S5_OI_EVENT_TYPES = frozenset({"OPEN_INTEREST", "OPEN_INTEREST_CURRENT", "OPEN_INTEREST_HISTORY"})
S6_QUOTE_EVENT_TYPES = frozenset({"BOOK_TICKER", "TICKER_24H"})


@dataclass(frozen=True)
class S5ReversalInputV1:
    key: InstrumentKeyV2
    s4: S4FeatureArtifactV2
    absorption: S4AbsorptionHypothesisV2 | None
    liquidation_window: Any | None
    liquidation_baseline: Any | None
    exhaustion: StageEvidenceV2 | None
    reclaim: StageEvidenceV2 | None
    flow_reversal: StageEvidenceV2 | None


@dataclass(frozen=True)
class S5PreparedDiagnosticsV1:
    """Typed reducer outputs delivered after the sealed market prefix."""

    key: InstrumentKeyV2
    information_cutoff_ns: int
    delivered_at_ns: int
    continuation: S5ContinuationArtifactV2
    reversal: S5ReversalArtifactV2 | None
    input_refs: tuple[str, ...]
    criteria_payload: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"version": "S5PreparedDiagnosticsV1", "key": self.key.to_dict(),
            "information_cutoff_ns": self.information_cutoff_ns,
            "delivered_at_ns": self.delivered_at_ns,
            "continuation": self.continuation.to_dict(),
            "reversal": self.reversal.to_dict() if self.reversal else None,
            "input_refs": list(self.input_refs), "criteria": dict(self.criteria_payload),
            "authority": "ZERO", "capital_authority": "ZERO", "candidate_action_refs": []}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class S7DirectionalInputV1:
    event: NewsEventV2
    key: InstrumentKeyV2
    first_bar: Any | None
    second_bar: Any | None
    history_bars: tuple[Any, ...]
    beta_to_btc: PreEventBetaV2 | None
    btc_return_5m: MarketReturn5MV2 | None
    spread: ReactionSpreadEvidenceV2 | None
    source_health: Any | None
    bar_source_health: Any | None
    resolution: EventResolutionV2 | None = None


@dataclass(frozen=True)
class S7PopulationEvidenceV1:
    """Bounded, deterministic account of the S7 event selection at one cutoff."""

    information_cutoff_ns: int
    selected_event_refs: tuple[str, ...]
    observed_event_refs: tuple[str, ...]
    overflow_event_refs: tuple[str, ...] = ()
    page_has_more: bool = False
    selection_version: str = "S7_LATEST_AVAILABLE_64_V1"

    def __post_init__(self) -> None:
        timestamp(self.information_cutoff_ns, field="S7 population cutoff")
        for name in ("selected_event_refs", "observed_event_refs", "overflow_event_refs"):
            refs = getattr(self, name)
            if len(refs) != len(set(refs)) or any(
                    not isinstance(ref, str) or len(ref) != 64 for ref in refs):
                raise ValueError("S7 population refs must be unique SHA-256 artifact refs")
        if len(self.selected_event_refs) > MAX_S7_EVENTS or len(self.observed_event_refs) > MAX_S7_EVENTS + 1:
            raise ValueError("S7 population evidence exceeds its fixed read bound")
        if not set(self.selected_event_refs).issubset(self.observed_event_refs):
            raise ValueError("S7 selected event refs must be present in the observed population")
        if not set(self.overflow_event_refs).issubset(self.observed_event_refs):
            raise ValueError("S7 overflow refs must be present in the observed population")
        if type(self.page_has_more) is not bool:
            raise ValueError("S7 population continuation state must be explicit")

    @property
    def overflow(self) -> bool:
        return bool(self.overflow_event_refs or self.page_has_more)

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.selection_version,
            "information_cutoff_ns": self.information_cutoff_ns,
            "selection_limit": MAX_S7_EVENTS,
            "status": "OVERFLOW" if self.overflow else "COMPLETE",
            "selection_order": "LATEST_AVAILABLE_AT_THEN_EVENT_REF_DESC",
            "selected_event_refs": list(self.selected_event_refs),
            "observed_event_refs": list(self.observed_event_refs),
            "overflow_event_refs": list(self.overflow_event_refs),
            "page_has_more": self.page_has_more,
            "continuation_required": self.overflow,
            "directional_inference_allowed": not self.overflow}


def _indexed_s7_population(
    repository: OpsRepository, *, cutoff_ns: int,
) -> tuple[S7PopulationEvidenceV1, dict[str, NewsEventV2]]:
    """Derive the bounded S7 denominator from indexed, cutoff-visible events."""
    cutoff = timestamp(cutoff_ns, field="S7 population cutoff")
    page = repository.latest_artifact_entries("NewsEventV2", as_of_ns=cutoff, limit=MAX_S7_EVENTS + 1)
    if page.invalid_entry_count:
        raise ValueError("indexed S7 news event population contains invalid entries")
    ordered = tuple(sorted(page.entries,
        key=lambda entry: (entry.available_at_ns, entry.artifact_ref), reverse=True))
    selected = ordered[:MAX_S7_EVENTS]
    overflow = ordered[MAX_S7_EVENTS:]
    population = S7PopulationEvidenceV1(cutoff,
        tuple(entry.artifact_ref for entry in selected),
        tuple(entry.artifact_ref for entry in ordered),
        tuple(entry.artifact_ref for entry in overflow), page.has_more)
    events: dict[str, NewsEventV2] = {}
    for entry in ordered:
        raw_event = entry.metadata.get("event")
        if not isinstance(raw_event, Mapping):
            raise ValueError("indexed S7 news event body is malformed")
        event = _event_from_dict(raw_event)
        if (event.event_id != entry.artifact_ref or event.semantic_hash != entry.content_hash
                or entry.available_at_ns != event.available_at_ns or event.available_at_ns > cutoff):
            raise ValueError("indexed S7 news event identity or chronology is invalid")
        events[entry.artifact_ref] = event
    return population, events


@dataclass(frozen=True)
class S8PairInputV1:
    pair: S8PairDefinitionV2
    prices_a: tuple[S8HourlyPriceV2, ...]
    prices_b: tuple[S8HourlyPriceV2, ...]
    leg_a_evidence: S8LegEvidenceV2
    leg_b_evidence: S8LegEvidenceV2


@dataclass(frozen=True)
class ModelForecastBindingV1:
    request: ModelRequestV2
    forecast: ForecastArtifactV2
    forecast_ref: str


@dataclass(frozen=True)
class FullStrategyInputsV1:
    """Causal prepared inputs for the production research sidecar."""

    s4_features: Mapping[InstrumentKeyV2, S4FeatureArtifactV2]
    s4_baselines: Mapping[InstrumentKeyV2, S4ExpectedResponseBaselineV2]
    s5_continuation: Mapping[InstrumentKeyV2, S5ContinuationResearchV2]
    s5_reversal: tuple[S5ReversalInputV1, ...]
    s5_contexts: Mapping[InstrumentKeyV2, S5CrowdingContextV2]
    s6_hourly_bars: Mapping[InstrumentKeyV2, tuple[Any, ...]]
    s6_four_hour_bars: Mapping[InstrumentKeyV2, tuple[Any, ...]]
    s6_evidence: Mapping[InstrumentKeyV2, LiquidityFundingEvidenceV2]
    s6_btc_proxy: InstrumentKeyV2 | None
    s7_inputs: tuple[S7DirectionalInputV1, ...]
    s8_inputs: tuple[S8PairInputV1, ...]
    model_forecasts: tuple[ModelForecastBindingV1, ...]
    s5_prepared_diagnostics: Mapping[InstrumentKeyV2, S5PreparedDiagnosticsV1] = field(default_factory=dict)
    s7_population: S7PopulationEvidenceV1 | None = None
    # S6 ranks a venue/environment/product cohort against that cohort's own
    # BTC reference. Keep the singular field for callers pinned to the older
    # one-cohort API; production loaders populate the complete tuple.
    s6_btc_proxies: tuple[InstrumentKeyV2, ...] = ()

    def compose_kwargs(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in (
            "s4_features", "s4_baselines", "s5_continuation", "s5_reversal", "s5_contexts", "s6_hourly_bars",
            "s6_four_hour_bars", "s6_evidence", "s6_btc_proxy", "s7_inputs",
            "s8_inputs", "model_forecasts", "s5_prepared_diagnostics", "s7_population",
            "s6_btc_proxies",
        )}


def register_s8_pair_catalog(
    repository: OpsRepository,
    *,
    owner_id: str,
    pairs: Sequence[S8PairDefinitionV2],
    available_at_ns: int,
) -> str:
    """Persist exact owner-supplied pair definitions; this API invents none."""
    available = timestamp(available_at_ns, field="S8 owner catalog availability")
    if not owner_id.strip() or not pairs or len(pairs) > MAX_S8_PAIRS:
        raise ValueError("S8 owner catalog requires a named owner and a bounded nonempty pair list")
    ordered = tuple(sorted(pairs, key=lambda pair: pair.pair_id))
    if len({pair.pair_id for pair in ordered}) != len(ordered):
        raise ValueError("S8 owner catalog pair IDs must be unique")
    body = {"version": "S8_PAIR_OWNER_CATALOG_V1", "owner_id": owner_id,
        "available_at_ns": available, "pairs": [pair.to_dict() for pair in ordered],
        "authority": "RESEARCH_CONFIGURATION_ONLY", "capital_authority": "ZERO"}
    catalog_ref = sha256_json(body)
    repository.register_artifact(ArtifactIndexEntryV2(catalog_ref, "S8PairOwnerCatalogV1", catalog_ref,
        available, available, {"catalog": body}))
    for pair in ordered:
        repository.register_artifact(ArtifactIndexEntryV2(pair.content_hash, "S8PairDefinitionV2",
            pair.content_hash, available, available,
            {"pair": pair.to_dict(), "owner_catalog_ref": catalog_ref}))
    return catalog_ref


def load_full_strategy_inputs(
    repository: OpsRepository,
    universe: UniverseContractV2,
    cutoff_ns: int,
    prepared_histories: Mapping[str, Mapping[BarIntervalV2, ActiveHistoryPageV1]],
    clock_ns: Callable[[], int],
    *,
    sequence_books: Mapping[InstrumentKeyV2, SequenceValidBookV2] | None = None,
    prepared_s4_features: Mapping[InstrumentKeyV2, S4FeatureArtifactV2] | None = None,
    s5_continuation: Mapping[InstrumentKeyV2, S5ContinuationResearchV2] | None = None,
    s5_reversal: Sequence[S5ReversalInputV1] = (),
    s5_contexts: Mapping[InstrumentKeyV2, S5CrowdingContextV2] | None = None,
    s6_evidence: Mapping[InstrumentKeyV2, LiquidityFundingEvidenceV2] | None = None,
    s7_inputs: Sequence[S7DirectionalInputV1] = (),
    s8_inputs: Sequence[S8PairInputV1] = (),
    s8_pair_definitions: Sequence[S8PairDefinitionV2] | None = None,
    model_forecasts: Sequence[ModelForecastBindingV1] = (),
    service_callback: Callable[[], None] | None = None,
) -> FullStrategyInputsV1:
    """Build the bounded sidecar inputs from exact production history pages.

    OHLC histories are reusable for S6 only. S4 is built only from caller-owned
    exact sequence-valid book state. Derivative rows are normalized from the
    latest exact broad-public receipt; servicing runs between bounded chunks.
    S7 still needs authenticated event reaction context, and S8 needs an
    explicitly registered pair with caller-prepared two-leg evidence.
    """
    cutoff = timestamp(cutoff_ns, field="strategy_surface.loader_cutoff_ns")
    if len(universe.entries) > MAX_SURFACE_UNIVERSE:
        raise ValueError("strategy input universe exceeds its fixed bound")
    if len(prepared_histories) > MAX_PREPARED_HISTORY_KEYS:
        raise ValueError("prepared history key population exceeds its fixed bound")
    keys = {entry.key for entry in universe.entries}
    strategy_eligible_keys = {entry.key for entry in universe.entries
        if entry.data_eligible and not entry.capital_eligible}
    hourly: dict[InstrumentKeyV2, tuple[Any, ...]] = {}
    four_hour: dict[InstrumentKeyV2, tuple[Any, ...]] = {}
    hourly_indexed: dict[InstrumentKeyV2, tuple[Any, ...]] = {}
    minute_indexed: dict[InstrumentKeyV2, tuple[Any, ...]] = {}
    five_minute_indexed: dict[InstrumentKeyV2, tuple[Any, ...]] = {}
    fifteen_minute_indexed: dict[InstrumentKeyV2, tuple[Any, ...]] = {}
    four_hour_indexed: dict[InstrumentKeyV2, tuple[Any, ...]] = {}
    total_history_bars = 0
    for entry in universe.entries:
        if service_callback is not None:
            service_callback()
        pages = prepared_histories.get(entry.key.to_canonical_json(), {})
        for interval, target in ((BarIntervalV2.H1, hourly), (BarIntervalV2.H4, four_hour)):
            page = pages.get(interval)
            if page is None or not page.ready or page.state is None:
                continue
            if page.state.key != entry.key or page.state.interval != interval:
                raise ValueError("prepared history page identity does not match its universe key")
            if (page.state.max_source_available_at_ns > cutoff
                    or any(item.bar.raw.available_at_ns > cutoff
                           or not _source_visible(repository, item.observation_index_ref, cutoff)
                           for item in page.bars)):
                raise ValueError("prepared history page contains future or unindexed source evidence")
            bars = tuple(item.bar for item in page.bars)
            if len(bars) > (721 if interval == BarIntervalV2.H1 else 181):
                raise ValueError("one prepared strategy history exceeds its fixed bound")
            total_history_bars += len(bars)
            for item in page.bars:
                _persist_native_history_bar(repository, key=entry.key, indexed_bar=item,
                    cutoff_ns=cutoff, clock_ns=clock_ns)
            target[entry.key] = bars
            if interval == BarIntervalV2.H1:
                hourly_indexed[entry.key] = tuple(page.bars)
            else:
                four_hour_indexed[entry.key] = tuple(page.bars)
        minute_page = pages.get(BarIntervalV2.M1)
        if minute_page is not None and minute_page.ready and minute_page.state is not None:
            if minute_page.state.key != entry.key or minute_page.state.interval != BarIntervalV2.M1:
                raise ValueError("prepared M1 history page identity does not match its universe key")
            if (minute_page.state.max_source_available_at_ns > cutoff
                    or len(minute_page.bars) > 721
                    or any(item.bar.raw.available_at_ns > cutoff
                           or not _source_visible(repository, item.observation_index_ref, cutoff)
                           for item in minute_page.bars)):
                raise ValueError("prepared M1 page contains future, unindexed, or excessive source history")
            total_history_bars += len(minute_page.bars)
            minute_indexed[entry.key] = tuple(minute_page.bars)
        m5_page = pages.get(BarIntervalV2.M5)
        if m5_page is not None and m5_page.ready and m5_page.state is not None:
            if m5_page.state.key != entry.key or m5_page.state.interval != BarIntervalV2.M5:
                raise ValueError("prepared M5 history page identity does not match its universe key")
            if (m5_page.state.max_source_available_at_ns > cutoff or len(m5_page.bars) > 721
                    or any(item.bar.raw.available_at_ns > cutoff
                           or not _source_visible(repository, item.observation_index_ref, cutoff)
                           for item in m5_page.bars)):
                raise ValueError("prepared M5 page contains future, unindexed, or excessive source history")
            total_history_bars += len(m5_page.bars)
            five_minute_indexed[entry.key] = tuple(m5_page.bars)
            for item in m5_page.bars:
                _persist_native_history_bar(repository, key=entry.key, indexed_bar=item,
                    cutoff_ns=cutoff, clock_ns=clock_ns)
        m15_page = pages.get(BarIntervalV2.M15)
        if m15_page is not None and m15_page.ready and m15_page.state is not None:
            if m15_page.state.key != entry.key or m15_page.state.interval != BarIntervalV2.M15:
                raise ValueError("prepared M15 history identity does not match its universe key")
            if (m15_page.state.max_source_available_at_ns > cutoff or len(m15_page.bars) > 721
                    or any(item.bar.raw.available_at_ns > cutoff
                           or not _source_visible(repository, item.observation_index_ref, cutoff)
                           for item in m15_page.bars)):
                raise ValueError("prepared M15 history contains future, unindexed, or excessive evidence")
            total_history_bars += len(m15_page.bars)
            fifteen_minute_indexed[entry.key] = tuple(m15_page.bars[-MAX_S5_STRUCTURAL_BARS:])
            for item in fifteen_minute_indexed[entry.key]:
                _persist_native_history_bar(repository, key=entry.key, indexed_bar=item,
                    cutoff_ns=cutoff, clock_ns=clock_ns)
    if total_history_bars > MAX_TOTAL_HISTORY_BARS:
        raise ValueError("prepared history bar population exceeds its global fixed bound")

    features: dict[InstrumentKeyV2, S4FeatureArtifactV2] = {}
    if len(sequence_books or {}) + len(prepared_s4_features or {}) > MAX_S4_SEQUENCE_BOOKS:
        raise ValueError("S4 sequence book population exceeds its fixed bound")
    if set(sequence_books or {}) & set(prepared_s4_features or {}):
        raise ValueError("S4 prepared feature cannot be replaced by a live book")
    for key, feature in (prepared_s4_features or {}).items():
        published = max(cutoff, timestamp(clock_ns(), field="S4 prepared feature consumer"))
        if (key not in keys or feature.instrument != key or feature.cutoff_ns != cutoff
                or not _s4_feature_indexed(repository, feature, published)
                or not causal_artifact(repository, feature.content_hash, cutoff_ns=cutoff,
                    consumer_at_ns=published, deadline_ns=published)
                or any(not _source_visible(repository, ref, cutoff) for ref in feature.input_refs)):
            raise ValueError("prepared S4 feature is not exact cutoff-bound causal evidence")
        features[key] = feature
    features.update(persist_cutoff_book_features(repository, universe=universe,
        cutoff_ns=cutoff, sequence_books=sequence_books or {}, clock_ns=clock_ns))

    loaded_s7 = tuple(s7_inputs)
    s7_population: S7PopulationEvidenceV1 | None = None
    if not loaded_s7:
        s7_population, indexed_events = _indexed_s7_population(repository, cutoff_ns=cutoff)
        by_asset: dict[str, list[InstrumentKeyV2]] = {}
        for entry in universe.entries:
            if entry.data_eligible and not entry.capital_eligible:
                by_asset.setdefault(entry.key.base_asset_id, []).append(entry.key)
        selected_events: list[S7DirectionalInputV1] = []
        for event_ref in s7_population.selected_event_refs if not s7_population.overflow else ():
            event = indexed_events[event_ref]
            for asset in event.affected_asset_ids:
                matched = by_asset.get(asset, ())
                if len(matched) == 1:
                    selected_events.append(S7DirectionalInputV1(
                        event, matched[0], None, None, (), None, None, None, None, None,
                    ))
        if len(selected_events) > MAX_S7_EVENTS:
            raise ValueError("mapped S7 news reaction population exceeds its fixed bound")
        loaded_s7 = tuple(selected_events)

    btc_candidates = sorted((entry.key for entry in universe.entries
        if entry.key.native_symbol == "BTCUSDT" and entry.data_eligible
        and not entry.capital_eligible), key=lambda key: key.to_canonical_json())
    derived_liquidity: dict[InstrumentKeyV2, LiquidityFundingEvidenceV2] = {}
    derived_contexts: dict[InstrumentKeyV2, S5CrowdingContextV2] = {}
    derived_funding_refs: dict[InstrumentKeyV2, tuple[str, ...]] = {}
    derived_oi: dict[InstrumentKeyV2, tuple[OpenInterestObservationV2, ...]] = {}
    pair_definitions = tuple(s8_pair_definitions) if s8_pair_definitions is not None else _latest_s8_pair_catalog(
        repository, cutoff_ns=cutoff,
    )
    if s6_evidence is None or s5_contexts is None or pair_definitions:
        derived_liquidity, derived_contexts, derived_funding_refs = _normalized_derivatives(
            repository, universe=universe, cutoff_ns=cutoff, service_callback=service_callback,
            clock_ns=clock_ns, sequence_features=features, oi_output=derived_oi,
        )
    if not s7_inputs and loaded_s7:
        loaded_s7 = _build_s7_inputs(repository, candidates=loaded_s7, universe=universe,
            cutoff_ns=cutoff, minute_indexed=minute_indexed, five_minute_indexed=five_minute_indexed,
            sequence_features=features, clock_ns=clock_ns, service_callback=service_callback)
    loaded_s8 = list(s8_inputs)
    if not loaded_s8 and pair_definitions:
        s4_refs = {key: feature.content_hash for key, feature in features.items()}
        available_by_key = {key: max((bar.raw.available_at_ns for bar in bars), default=cutoff)
                            for key, bars in hourly.items()}
        for pair in pair_definitions:
            if pair.key_a not in strategy_eligible_keys or pair.key_b not in strategy_eligible_keys:
                continue
            left_indexed, right_indexed = hourly_indexed.get(pair.key_a, ()), hourly_indexed.get(pair.key_b, ())
            prices_a = tuple(S8HourlyPriceV2(pair.key_a, item.bar.close_at_ns,
                item.bar.raw.available_at_ns, item.bar.close, item.observation_index_ref)
                for item in left_indexed)
            prices_b = tuple(S8HourlyPriceV2(pair.key_b, item.bar.close_at_ns,
                item.bar.raw.available_at_ns, item.bar.close, item.observation_index_ref)
                for item in right_indexed)
            leg_a = S8LegEvidenceV2(pair.key_a, tuple(sorted(row.source_ref for row in prices_a)),
                (s4_refs[pair.key_a],) if pair.key_a in s4_refs else (), None,
                derived_funding_refs.get(pair.key_a, ()), (), (), (), available_by_key.get(pair.key_a, cutoff))
            leg_b = S8LegEvidenceV2(pair.key_b, tuple(sorted(row.source_ref for row in prices_b)),
                (s4_refs[pair.key_b],) if pair.key_b in s4_refs else (), None,
                derived_funding_refs.get(pair.key_b, ()), (), (), (), available_by_key.get(pair.key_b, cutoff))
            loaded_s8.append(S8PairInputV1(pair, prices_a, prices_b, leg_a, leg_b))
    baselines = _fit_prior_s4_baselines(repository, features=features,
        cutoff_ns=cutoff, clock_ns=clock_ns, service_callback=service_callback)
    contexts = dict(s5_contexts if s5_contexts is not None else derived_contexts)
    prepared_s5 = _prepare_s5_diagnostics(repository, keys=strategy_eligible_keys,
        cutoff_ns=cutoff, m15_indexed=fifteen_minute_indexed, h4_indexed=four_hour_indexed,
        features=features, baselines=baselines, contexts=contexts, open_interest=derived_oi,
        clock_ns=clock_ns, service_callback=service_callback)
    liquidity_inputs = dict(s6_evidence if s6_evidence is not None else derived_liquidity)
    # The bounded scheduler may retain a research cohort from either venue.
    # Bind its existing BTC series by exact venue/environment/product identity.
    def proxy_coverage(proxy: InstrumentKeyV2) -> tuple[int, str]:
        cohort = {key for key in strategy_eligible_keys & set(hourly) & set(four_hour) & set(liquidity_inputs)
            if (key.venue, key.environment, key.product) == (proxy.venue, proxy.environment, proxy.product)}
        return (-len(cohort), proxy.to_canonical_json())

    # Select a deterministic proxy for every observed venue cohort. S6 then
    # evaluates each independently, so a larger Bybit cohort cannot suppress
    # Binance research (or vice versa).
    proxies_by_cohort: dict[tuple[Any, Any, Any], list[InstrumentKeyV2]] = {}
    for candidate in btc_candidates:
        cohort = (candidate.venue, candidate.environment, candidate.product)
        proxies_by_cohort.setdefault(cohort, []).append(candidate)
    btc_proxies = tuple(sorted(
        (min(candidates, key=proxy_coverage) for candidates in proxies_by_cohort.values()),
        key=lambda key: key.to_canonical_json(),
    ))
    btc_proxy = min(btc_candidates, key=proxy_coverage) if btc_candidates else None
    return FullStrategyInputsV1(
        s4_features=features,
        s4_baselines=baselines,
        s5_continuation=dict(s5_continuation or {}),
        s5_reversal=tuple(s5_reversal),
        s5_contexts=contexts,
        s5_prepared_diagnostics=prepared_s5,
        s6_hourly_bars=hourly,
        s6_four_hour_bars=four_hour,
        s6_evidence=liquidity_inputs,
        s6_btc_proxy=btc_proxy,
        s6_btc_proxies=btc_proxies,
        s7_inputs=loaded_s7,
        s8_inputs=tuple(loaded_s8),
        model_forecasts=tuple(model_forecasts),
        s7_population=s7_population,
    )


@dataclass(frozen=True)
class FullStrategySurfaceResultV1:
    content_hash: str
    cutoff_ns: int
    published_at_ns: int
    universe_ref: str
    role_status: Mapping[str, str]
    role_refs: Mapping[str, tuple[str, ...]]
    missing_reasons: Mapping[str, tuple[str, ...]]
    original_universe_receipt_ref: str | None = None
    original_decision_deadline_ns: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": SURFACE_VERSION,
            "cutoff_ns": self.cutoff_ns,
            "published_at_ns": self.published_at_ns,
            "universe_ref": self.universe_ref,
            "original_universe_receipt_ref": self.original_universe_receipt_ref,
            "original_decision_deadline_ns": self.original_decision_deadline_ns,
            "selector_influence": "ZERO",
            "role_status": dict(self.role_status),
            "role_refs": {name: list(refs) for name, refs in self.role_refs.items()},
            "missing_reasons": {name: list(reasons) for name, reasons in self.missing_reasons.items()},
            "authority": "ZERO",
            "candidate_action_refs": [],
            "capital_authority": "ZERO",
            "trade_plan_allowed": False,
        }


def _source_visible(repository: OpsRepository, ref: str, cutoff_ns: int) -> bool:
    return _source_evidence_available(repository, ref, cutoff_ns)


def _persist_native_history_bar(repository: OpsRepository, *, key: InstrumentKeyV2,
                                indexed_bar: Any, cutoff_ns: int,
                                clock_ns: Callable[[], int]) -> None:
    bar = indexed_bar.bar
    source_ref = indexed_bar.observation_index_ref
    source = repository.get_artifact(source_ref)
    if (bar.instrument_revision != key.contract_revision or not bar.final
            or bar.close_at_ns > cutoff_ns or source is None
            or source.artifact_type != "PublicObservationIndexV2"
            or source.content_hash != bar.raw.content_hash
            or not indexed_availability_matches(bar.raw.available_at_ns, source.available_at_ns, source.metadata)
            or source.metadata.get("instrument_key_json") != key.to_canonical_json()
            or source.metadata.get("bar_content_hash") != bar.content_hash
            or not _source_visible(repository, source_ref, cutoff_ns)):
        raise ValueError("native strategy history bar conflicts with its exact indexed source")
    body = {"bar": bar.to_dict(), "raw": bar.raw.to_dict(), "key": key.to_dict(),
        "input_refs": [source_ref], "information_cutoff_ns": cutoff_ns, "authority": "ZERO"}
    entry = repository.get_artifact(bar.content_hash)
    if entry is not None and entry.artifact_type == "CausalBarV2":
        if (entry.content_hash != bar.content_hash or entry.available_at_ns != source.available_at_ns
                or entry.created_at_ns != source.available_at_ns
                or canonical_json(entry.metadata) != canonical_json(
                    {"bar": bar.to_dict(), "source_observation_ref": source_ref})):
            raise ValueError("native strategy history conflicts with its existing exact causal bar")
        return
    if entry is None:
        published = max(cutoff_ns, timestamp(clock_ns(), field="native history publication"))
        repository.register_artifact(ArtifactIndexEntryV2(bar.content_hash, "NativeStrategyHistoryBarV1",
            bar.content_hash, published, published, body))
        record_computation(repository, artifact_ref=bar.content_hash, information_cutoff_ns=cutoff_ns,
            started_ns=published, finished_ns=published, available_ns=published,
            input_refs=(source_ref,), deadline_ns=published)
    elif (entry.artifact_type != "NativeStrategyHistoryBarV1"
            or entry.content_hash != bar.content_hash
            or canonical_json({name: value for name, value in entry.metadata.items()
                              if name != "information_cutoff_ns"}) != canonical_json(
                                  {name: value for name, value in body.items() if name != "information_cutoff_ns"})
            or entry.metadata.get("information_cutoff_ns", cutoff_ns + 1) > cutoff_ns):
        raise ValueError("native strategy history index conflicts with its exact derived body")


def persist_cutoff_book_features(repository: OpsRepository, *, universe: UniverseContractV2,
                                cutoff_ns: int,
                                sequence_books: Mapping[InstrumentKeyV2, SequenceValidBookV2],
                                clock_ns: Callable[[], int]) -> dict[InstrumentKeyV2, S4FeatureArtifactV2]:
    """Seal exact cutoff features while the original sequence books still exist."""
    if len(sequence_books) > MAX_S4_SEQUENCE_BOOKS:
        raise ValueError("S4 sequence book population exceeds its fixed bound")
    keys = {entry.key for entry in universe.entries}
    features = {}
    for key, book in sequence_books.items():
        if key not in keys or book.instrument != key:
            raise ValueError("S4 sequence book key does not match the bounded universe")
        started = max(cutoff_ns, timestamp(clock_ns(), field="S4 feature computation start"))
        feature = book.feature(cutoff_ns=cutoff_ns, availability_view=AvailabilityViewV2.ACTUAL_RECEIPT)
        published = max(started, timestamp(clock_ns(), field="S4 feature publication"))
        existing = repository.get_artifact(feature.content_hash)
        body = {"feature": feature.to_dict(), "instrument_key_json": key.to_canonical_json(),
            "input_refs": list(feature.input_refs)}
        if existing is None:
            repository.register_artifact(ArtifactIndexEntryV2(feature.content_hash,
                "S4FeatureArtifactV2", feature.content_hash, published, published, body))
            record_computation(repository, artifact_ref=feature.content_hash, information_cutoff_ns=cutoff_ns,
                started_ns=started, finished_ns=published, available_ns=published,
                input_refs=feature.input_refs, deadline_ns=published)
        elif (existing.artifact_type != "S4FeatureArtifactV2" or existing.content_hash != feature.content_hash
              or existing.available_at_ns < cutoff_ns or canonical_json(existing.metadata) != canonical_json(body)):
            raise ValueError("S4 feature index conflicts with its exact derived output")
        features[key] = feature
    return features


def _s4_feature_indexed(repository: OpsRepository, feature: S4FeatureArtifactV2,
                        published_by_ns: int) -> bool:
    entry = repository.get_artifact(feature.content_hash)
    return bool(entry is not None and entry.artifact_type == "S4FeatureArtifactV2"
        and entry.artifact_ref == feature.content_hash and entry.content_hash == feature.content_hash
        and entry.available_at_ns <= published_by_ns
        and canonical_json(entry.metadata.get("feature")) == canonical_json(feature.to_dict()))


def _fit_prior_s4_baselines(repository: OpsRepository, *,
                            features: Mapping[InstrumentKeyV2, S4FeatureArtifactV2], cutoff_ns: int,
                            clock_ns: Callable[[], int], service_callback: Callable[[], None] | None
                            ) -> dict[InstrumentKeyV2, S4ExpectedResponseBaselineV2]:
    baselines: dict[InstrumentKeyV2, S4ExpectedResponseBaselineV2] = {}
    if cutoff_ns < 2:
        return baselines
    for key, current in features.items():
        page = repository.latest_artifact_entries("S4FeatureArtifactV2", as_of_ns=cutoff_ns - 2,
            limit=MAX_S4_BASELINE_HISTORY, metadata_path=("instrument_key_json",),
            identity_value=key.to_canonical_json())
        if page.invalid_entry_count:
            raise ValueError("prior S4 feature index is malformed")
        if service_callback is not None:
            service_callback()
        history = []
        for entry in page.entries:
            body = entry.metadata.get("feature")
            if not isinstance(body, Mapping):
                raise ValueError("prior S4 feature body is malformed")
            feature = S4FeatureArtifactV2.from_dict(dict(body))
            if (feature.instrument != key or feature.cutoff_ns >= cutoff_ns - 1
                    or feature.content_hash != entry.content_hash or feature.content_hash != entry.artifact_ref
                    or feature.cutoff_ns > entry.available_at_ns
                    or not feature.estimable or feature.trade_coverage_state != "QUALIFIED"
                    or feature.availability_view != AvailabilityViewV2.ACTUAL_RECEIPT
                    or any(not _source_visible(repository, ref, cutoff_ns - 2) for ref in feature.input_refs)):
                continue
            flow = next((row for row in feature.flow_price_response_windows
                         if row[0] == "30" and row[3] == "ESTIMABLE"), None)
            response = next((row for row in feature.price_response_windows
                             if row[0] == "30" and row[2] == "ESTIMABLE"), None)
            if flow is not None and response is not None:
                history.append((feature.cutoff_ns, Decimal(flow[1]), Decimal(response[1]), feature.content_hash))
                if len(history) == MAX_S4_BASELINE_HISTORY:
                    break
        baseline = fit_s4_expected_response_baseline(history=tuple(history), fit_cutoff_ns=cutoff_ns - 1)
        if baseline is None or baseline.fit_cutoff_ns >= current.cutoff_ns:
            continue
        body = _s4_baseline_body(baseline)
        existing = repository.get_artifact(baseline.content_hash)
        if existing is None:
            published = max(cutoff_ns, timestamp(clock_ns(), field="prior S4 fit publication"))
            repository.register_artifact(ArtifactIndexEntryV2(baseline.content_hash,
                "S4ExpectedResponseBaselineV2", baseline.content_hash, published, published,
                {"baseline": body, "input_refs": list(baseline.input_refs), "authority": "ZERO"}))
            record_computation(repository, artifact_ref=baseline.content_hash, information_cutoff_ns=cutoff_ns,
                started_ns=published, finished_ns=published, available_ns=published,
                input_refs=baseline.input_refs, deadline_ns=published)
        elif (existing.artifact_type != "S4ExpectedResponseBaselineV2"
                or canonical_json(existing.metadata.get("baseline")) != canonical_json(body)):
            raise ValueError("prior S4 fit conflicts with its exact training window")
        baselines[key] = baseline
    return baselines


def _s4_baseline_body(baseline: S4ExpectedResponseBaselineV2) -> dict[str, Any]:
    return {"fit_cutoff_ns": baseline.fit_cutoff_ns, "fit_window_start_ns": baseline.fit_window_start_ns,
        "fit_window_end_ns": baseline.fit_window_end_ns, "input_refs": list(baseline.input_refs),
        "intercept": str(baseline.intercept), "slope": str(baseline.slope),
        "flow_mean": str(baseline.flow_mean), "flow_std": str(baseline.flow_std),
        "model_version": baseline.model_version}


def _persist_s5_derived(repository: OpsRepository, *, artifact_ref: str, artifact_type: str,
                        payload: Mapping[str, Any], input_refs: Sequence[str], cutoff_ns: int,
                        clock_ns: Callable[[], int]) -> int:
    refs = tuple(sorted(set(input_refs)))
    body = {**payload, "input_refs": list(refs), "authority": "ZERO"}
    content_hash = sha256_json(body) if artifact_type == "S5StructuralStageEvidenceV1" else artifact_ref
    existing = repository.get_artifact(artifact_ref)
    if existing is not None:
        if existing.artifact_type != artifact_type or canonical_json(existing.metadata) != canonical_json(body):
            raise ValueError("S5 derived evidence conflicts with its exact immutable body")
        return existing.available_at_ns
    published = max(cutoff_ns, timestamp(clock_ns(), field="S5 evidence delivery"))
    repository.register_artifact(ArtifactIndexEntryV2(artifact_ref, artifact_type, content_hash,
        published, published, body))
    record_computation(repository, artifact_ref=artifact_ref, information_cutoff_ns=cutoff_ns,
        started_ns=published, finished_ns=published, available_ns=published,
        input_refs=refs, deadline_ns=published)
    return published


def _s5_structural_stage(repository: OpsRepository, *, key: InstrumentKeyV2,
                         break_at_ns: int, stage: str, event_at_ns: int,
                         state: str, refs: Sequence[str], cutoff_ns: int,
                         clock_ns: Callable[[], int]) -> StageEvidenceV2:
    # One append-only stage identity per episode. Restart preserves the first
    # actual delivery rather than reconstructing evidence availability from bars.
    ref = sha256_json({"policy": S5_STRUCTURAL_POLICY, "key": key.to_dict(),
        "break_at_ns": break_at_ns, "stage": stage, "event_at_ns": event_at_ns})
    existing = repository.get_artifact(ref)
    if existing is not None:
        body = existing.metadata.get("stage")
        if (existing.artifact_type != "S5StructuralStageEvidenceV1" or not isinstance(body, Mapping)
                or body.get("stage") != stage or body.get("event_at_ns") != event_at_ns
                or body.get("state") != state or existing.metadata.get("policy") != S5_STRUCTURAL_POLICY
                or canonical_json(existing.metadata.get("key")) != canonical_json(key.to_dict())):
            raise ValueError("S5 persisted episode stage identity is malformed")
        return StageEvidenceV2(stage, event_at_ns, existing.available_at_ns, (ref,), state)
    available = _persist_s5_derived(repository, artifact_ref=ref,
        artifact_type="S5StructuralStageEvidenceV1", payload={"policy": S5_STRUCTURAL_POLICY,
            "key": key.to_dict(), "break_at_ns": break_at_ns,
            "stage": {"stage": stage, "event_at_ns": event_at_ns, "state": state}},
        input_refs=refs, cutoff_ns=cutoff_ns, clock_ns=clock_ns)
    return StageEvidenceV2(stage, event_at_ns, available, (ref,), state)


def _actual_reversal_stage_delivery(repository: OpsRepository,
                                     artifact: S5ReversalArtifactV2) -> S5ReversalArtifactV2:
    stages = []
    for stage in artifact.stage_evidence:
        if not stage.refs:
            stages.append(stage)
            continue
        indexed = repository.get_artifact(stage.refs[0])
        if indexed is None:
            raise ValueError("S5 reversal stage lacks exact indexed publication")
        available = max(stage.available_at_ns, indexed.available_at_ns)
        if available > artifact.cutoff_ns:
            raise ValueError("S5 reversal stage was delivered after reducer cutoff")
        stages.append(replace(stage, available_at_ns=available))
    return replace(artifact, stage_evidence=tuple(stages))


def _prepare_s5_diagnostics(repository: OpsRepository, *, keys: set[InstrumentKeyV2],
                            cutoff_ns: int, m15_indexed: Mapping[InstrumentKeyV2, Sequence[Any]],
                            h4_indexed: Mapping[InstrumentKeyV2, Sequence[Any]],
                            features: Mapping[InstrumentKeyV2, S4FeatureArtifactV2],
                            baselines: Mapping[InstrumentKeyV2, S4ExpectedResponseBaselineV2],
                            contexts: Mapping[InstrumentKeyV2, S5CrowdingContextV2],
                            open_interest: Mapping[InstrumentKeyV2, tuple[OpenInterestObservationV2, ...]],
                            clock_ns: Callable[[], int], service_callback: Callable[[], None] | None
                            ) -> dict[InstrumentKeyV2, S5PreparedDiagnosticsV1]:
    results = {}
    prepared_keys = keys & (set(m15_indexed) | set(features) | set(contexts))
    for key in sorted(prepared_keys, key=lambda item: item.to_canonical_json()):
        if service_callback is not None:
            service_callback()
        rows = tuple(m15_indexed.get(key, ()))[-MAX_S5_STRUCTURAL_BARS:]
        h4 = tuple(h4_indexed.get(key, ()))[-2:]
        feature, context = features.get(key), contexts.get(key)
        reducer = S5ContinuationResearchV2()
        refs: set[str] = set()
        missing: list[str] = []
        flow_row = next((row for row in feature.flow_price_response_windows
            if row[0] == "30" and row[3] == "ESTIMABLE"), None) if feature else None
        flow = Decimal(flow_row[1]) if flow_row else Decimal(0)
        liquidity_ok = bool(feature and feature.estimable and feature.trade_coverage_state == "QUALIFIED"
            and feature.availability_view == AvailabilityViewV2.ACTUAL_RECEIPT
            and feature.source_health == "HEALTHY_CURRENT" and flow_row)
        supported = bool(context and context.state == "CROWDING_CONTEXT"
            and context.evidence_quality == "SUPPORTED" and context.oi_quantity is not None
            and context.liquidity_state != "UNKNOWN" and liquidity_ok)
        contiguous = bool(len(rows) >= 21 and len(h4) == 2
            and all(right.bar.open_at_ns == left.bar.close_at_ns for left, right in zip(rows, rows[1:], strict=False))
            and h4[1].bar.open_at_ns == h4[0].bar.close_at_ns)
        if not contiguous:
            missing.append("S5_EXACT_20_PRIOR_M15_AND_TWO_H4_HISTORY_REQUIRED")
        if not liquidity_ok:
            missing.append("S5_SEQUENCE_VALID_QUALIFIED_S4_SIGNED_FLOW_REQUIRED")
        if not supported:
            missing.append("S5_SUPPORTED_CROWDING_STRUCTURE_LIQUIDITY_REQUIRED")
        selected: tuple[int, int, Decimal] | None = None
        if contiguous:
            for index in range(20, len(rows)):
                prior, broken = rows[index - 20:index], rows[index].bar
                low, high = min(item.bar.low for item in prior), max(item.bar.high for item in prior)
                direction = -1 if broken.close < low else 1 if broken.close > high else 0
                h4_before = tuple(item for item in h4 if item.bar.close_at_ns <= broken.close_at_ns)
                if (direction and len(h4_before) == 2
                        and (h4_before[-1].bar.close - h4_before[-2].bar.close) * direction > 0):
                    selected = (index, direction, low if direction < 0 else high)
                    break
        exhaustion = reclaim = reversal_flow = None
        criteria: dict[str, Any] = {"policy_version": S5_STRUCTURAL_POLICY,
            "threshold_status": "ENGINEERING_RESEARCH_DEFAULT_UNQUALIFIED",
            "prior_m15_count": 20, "m15_population_bound": MAX_S5_STRUCTURAL_BARS,
            "h4_close_count": 2, "signed_flow_window_seconds": 30,
            "funding_alone_signal_allowed": False, "missing_reasons": missing,
            "episode_expiry_m15_closes": 2, "invalidation": "REENTER_BROKEN_RANGE_OR_FLOW_LOSES_QUALIFICATION"}
        if selected is None:
            missing.append("S5_ALIGNED_M15_STRUCTURE_BREAK_UNAVAILABLE")
        else:
            index, direction, level = selected
            broken = rows[index].bar
            structural_refs = tuple(item.observation_index_ref for item in rows[index - 20:index + 1])
            structural_refs += tuple(item.observation_index_ref for item in h4)
            refs.update(structural_refs)
            criteria.update({"break_at_ns": broken.close_at_ns, "direction": direction,
                "broken_level": str(level), "expiry_at_ns": broken.close_at_ns + 2 * BarIntervalV2.M15.duration_ns})
            if supported and context is not None and feature is not None:
                refs.update(context.input_refs)
                refs.add(feature.content_hash)
                vulnerability = _s5_structural_stage(repository, key=key, break_at_ns=broken.close_at_ns,
                    stage="VULNERABILITY", event_at_ns=broken.close_at_ns, state="OBSERVED_CONTEXT",
                    refs=(*context.input_refs, *structural_refs, feature.content_hash),
                    cutoff_ns=cutoff_ns, clock_ns=clock_ns)
                break_stage = _s5_structural_stage(repository, key=key, break_at_ns=broken.close_at_ns,
                    stage="BREAK", event_at_ns=broken.close_at_ns, state="CAUSAL_STRUCTURE_FAILURE",
                    refs=structural_refs, cutoff_ns=cutoff_ns, clock_ns=clock_ns)
                reducer.vulnerabilities.append(vulnerability)
                reducer.breaks.append(break_stage)
                delta = oi_change_15m_evidence(open_interest.get(key, ()), cutoff_ns=cutoff_ns, instrument=key)
                if delta is not None and delta.change_fraction < 0 and delta.end_event_at_ns >= broken.close_at_ns:
                    endpoint = next((item for item in open_interest.get(key, ())
                        if item.raw_content_ref == delta.end_ref and item.source_id == delta.source_id
                        and item.channel == delta.channel), None)
                    if endpoint is not None:
                        available = max(cutoff_ns, timestamp(clock_ns(), field="S5 OI actual delivery"))
                        reducer.add_deleveraging(observation=endpoint, event_at_ns=endpoint.event_at_ns,
                            available_at_ns=available, oi_change_evidence=delta)
                        oi_stage = _s5_structural_stage(repository, key=key, break_at_ns=broken.close_at_ns,
                            stage="DELEVERAGING_EVIDENCE", event_at_ns=delta.end_event_at_ns,
                            state="OI_CONTRACTION_OBSERVED", refs=reducer.deleveraging_events[-1][0].refs,
                            cutoff_ns=cutoff_ns, clock_ns=clock_ns)
                        reducer.deleveraging_events[-1] = (oi_stage, reducer.deleveraging_events[-1][1])
                else:
                    missing.append("S5_QUALIFIED_SAME_SOURCE_15M_OI_CONTRACTION_REQUIRED")
                for later in rows[index + 1:]:
                    if (later.bar.close - level) * direction > 0 and flow * direction > 0:
                        available = max(cutoff_ns, timestamp(clock_ns(), field="S5 continuation delivery"))
                        reducer.confirm(event_at_ns=later.bar.close_at_ns, available_at_ns=available,
                            confirmation_ref=later.observation_index_ref)
                        if reducer.confirmations:
                            confirmed = _s5_structural_stage(repository, key=key, break_at_ns=broken.close_at_ns,
                                stage="CONTINUATION_CONFIRMED", event_at_ns=later.bar.close_at_ns,
                                state="CONFIRMED_AFTER_EVIDENCE_LATENCY",
                                refs=(later.observation_index_ref, feature.content_hash,
                                    *vulnerability.refs, *break_stage.refs,
                                    *(ref for stage, _ in reducer.deleveraging_events for ref in stage.refs)),
                                cutoff_ns=cutoff_ns, clock_ns=clock_ns)
                            reducer.confirmations[-1] = confirmed
                            break
            for offset in range(index + 1, len(rows)):
                current = rows[offset]
                if (current.bar.close - level) * direction <= 0:
                    exhaustion = _s5_structural_stage(repository, key=key, break_at_ns=broken.close_at_ns,
                        stage="EXHAUSTION", event_at_ns=current.bar.close_at_ns, state="EXHAUSTION_CONFIRMED",
                        refs=(*structural_refs, current.observation_index_ref), cutoff_ns=cutoff_ns, clock_ns=clock_ns)
                    if offset + 1 < len(rows):
                        following = rows[offset + 1]
                        reclaims = (following.bar.close > current.bar.high if direction < 0
                                    else following.bar.close < current.bar.low)
                        if reclaims:
                            reclaim = _s5_structural_stage(repository, key=key, break_at_ns=broken.close_at_ns,
                                stage="RECLAIM", event_at_ns=following.bar.close_at_ns, state="RECLAIM_CONFIRMED",
                                refs=(current.observation_index_ref, following.observation_index_ref),
                                cutoff_ns=cutoff_ns, clock_ns=clock_ns)
                            if liquidity_ok and feature is not None and flow * direction < 0:
                                reversal_flow = _s5_structural_stage(repository, key=key, break_at_ns=broken.close_at_ns,
                                    stage="FLOW_REVERSAL", event_at_ns=following.bar.close_at_ns,
                                    state="FLOW_REVERSAL_CONFIRMED", refs=(following.observation_index_ref, feature.content_hash),
                                    cutoff_ns=cutoff_ns, clock_ns=clock_ns)
                    break
        delivered = max(cutoff_ns, timestamp(clock_ns(), field="S5 prepared diagnostics delivery"))
        def delivery_clock(delivered: int = delivered) -> int:
            return delivered

        reversal = None
        if feature is not None:
            absorption = estimate_s4_absorption(feature=feature, baseline=baselines.get(key))
            absorption_refs = tuple(sorted({feature.content_hash, *absorption.evidence_refs}))
            _persist_s5_derived(repository, artifact_ref=absorption.content_hash,
                artifact_type="S4AbsorptionHypothesisV2", payload={"artifact": absorption.to_dict()},
                input_refs=absorption_refs, cutoff_ns=cutoff_ns, clock_ns=delivery_clock)
            reversal = evaluate_s5_reversal(cutoff_ns=delivered, s4=feature, absorption=absorption,
                liquidation_window=None, liquidation_baseline=None, exhaustion=exhaustion,
                reclaim=reclaim, flow_reversal=reversal_flow)
            reversal = _actual_reversal_stage_delivery(repository, reversal)
            refs.add(absorption.content_hash)
            refs.update(feature.input_refs)
            refs.add(feature.content_hash)
            if absorption.fit_window_ref is not None:
                refs.add(absorption.fit_window_ref)
        missing.append("S5_QUALIFIED_LIQUIDATION_WINDOW_AND_PRIOR_BASELINE_UNAVAILABLE")
        artifact = reducer.artifact(cutoff_ns=delivered)
        for stage in (artifact.vulnerability, artifact.break_evidence, artifact.deleveraging_evidence,
                      artifact.confirmation, exhaustion, reclaim, reversal_flow):
            if stage is not None:
                refs.update(stage.refs)
        criteria["missing_reasons"] = tuple(sorted(set(missing)))
        result = S5PreparedDiagnosticsV1(key, cutoff_ns, delivered, artifact, reversal,
            tuple(sorted(refs)), criteria)
        _persist_s5_derived(repository, artifact_ref=result.content_hash, artifact_type="S5PreparedDiagnosticsV1",
            payload={"diagnostics": result.to_dict()}, input_refs=result.input_refs,
            cutoff_ns=cutoff_ns, clock_ns=delivery_clock)
        results[key] = result
    return results


def _derive_m5_from_m1(repository: OpsRepository, *, key: InstrumentKeyV2,
                       indexed_m1: Sequence[Any], cutoff_ns: int,
                       clock_ns: Callable[[], int],
                       service_callback: Callable[[], None] | None) -> tuple[CausalBarV2, ...]:
    """Aggregate only exact contiguous five-bar M1 groups with a late receipt."""
    minute_ns = BarIntervalV2.M1.duration_ns
    five_ns = BarIntervalV2.M5.duration_ns
    by_open = {row.bar.open_at_ns: row for row in indexed_m1
        if row.bar.instrument_revision == key.contract_revision
        and row.bar.interval == BarIntervalV2.M1
        and row.bar.final and row.bar.raw.available_at_ns <= cutoff_ns
        and row.bar.close_at_ns <= cutoff_ns
        and _source_visible(repository, row.observation_index_ref, cutoff_ns)}
    result: list[CausalBarV2] = []
    for bucket in sorted({open_at - open_at % five_ns for open_at in by_open}):
        refs: list[str] = []
        components: list[CausalBarV2] = []
        for open_at in range(bucket, bucket + five_ns, minute_ns):
            row = by_open.get(open_at)
            if row is None:
                components = []
                break
            components.append(row.bar)
            refs.append(row.observation_index_ref)
        if len(components) != 5:
            continue
        sources = {bar.raw.source_id for bar in components}
        if len(sources) != 1:
            continue
        ordered_refs = tuple(sorted(refs))
        close_at = bucket + five_ns
        source_id = components[0].raw.source_id
        received = max(bar.raw.received_at_ns for bar in components)
        ingested = max(bar.raw.ingested_at_ns for bar in components)
        available = max(close_at, *(bar.raw.available_at_ns for bar in components))
        payload = {"version": "S7_NATIVE_M1_TO_M5_V1", "key": key.to_dict(),
            "open_at_ns": bucket, "close_at_ns": close_at,
            "source_refs": list(ordered_refs)}
        raw = RawObservationV2.build(instrument_revision=key.contract_revision, source_id=source_id,
            event_type="BAR_5M", event_at_ns=close_at, received_at_ns=received,
            ingested_at_ns=max(received, ingested), available_at_ns=max(available, received, ingested),
            translation_version="S7_NATIVE_M1_TO_M5_V1", payload=payload)
        bar = CausalBarV2(raw, BarIntervalV2.M5, bucket, close_at,
            components[0].open, max(row.high for row in components),
            min(row.low for row in components), components[-1].close,
            sum((row.volume for row in components), Decimal(0)), True)
        bar_entry = repository.get_artifact(bar.content_hash)
        if bar_entry is None:
            published = max(cutoff_ns, timestamp(clock_ns(), field="S7 M5 aggregation publication"))
            receipt_body = {"version": "S7_M1_TO_M5_AGGREGATION_RECEIPT_V1",
                "bar_ref": bar.content_hash, "key": key.to_dict(), "information_cutoff_ns": cutoff_ns,
                "available_at_ns": published, "input_refs": list(ordered_refs), "authority": "ZERO"}
            receipt_ref = sha256_json(receipt_body)
            repository.register_artifact(ArtifactIndexEntryV2(receipt_ref, "S7M5AggregationReceiptV1",
                receipt_ref, published, published, {"receipt": receipt_body}))
            bar_metadata = {"bar": bar.to_dict(), "aggregation_receipt_ref": receipt_ref,
                "input_refs": list(ordered_refs)}
            repository.register_artifact(ArtifactIndexEntryV2(bar.content_hash, "S7DerivedM5BarV1",
                bar.content_hash, published, published, bar_metadata))
            record_computation(repository, artifact_ref=bar.content_hash,
                information_cutoff_ns=cutoff_ns, started_ns=published, finished_ns=published,
                available_ns=published, input_refs=ordered_refs, deadline_ns=published)
        elif (bar_entry.artifact_type != "S7DerivedM5BarV1" or bar_entry.content_hash != bar.content_hash
                or not isinstance(bar_entry.metadata.get("aggregation_receipt_ref"), str)
                or canonical_json(bar_entry.metadata.get("bar")) != canonical_json(bar.to_dict())
                or tuple(bar_entry.metadata.get("input_refs", ())) != ordered_refs):
            raise ValueError("S7 derived M5 bar conflicts with its exact aggregation")
        result.append(bar)
        if service_callback is not None and len(result) % 32 == 0:
            service_callback()
    return tuple(result)


def _persist_s7_derived_evidence(repository: OpsRepository, *, artifact_type: str,
                                 information_cutoff_ns: int, available_at_ns: int,
                                 payload: Mapping[str, Any], input_refs: Sequence[str]) -> str:
    if artifact_type not in {"S7DerivedPreEventBetaV1", "S7DerivedMarketReturn5MV1",
                             "S7DerivedReactionSpreadV1"}:
        raise ValueError("unsupported S7 derived evidence type")
    refs = tuple(sorted(set(input_refs)))
    if not refs or any(not _source_visible(repository, ref, information_cutoff_ns) for ref in refs):
        raise ValueError("S7 derived evidence must retain exact cutoff-visible input refs")
    body = {"market_information_cutoff_ns": information_cutoff_ns,
        "available_at_ns": available_at_ns, "input_refs": list(refs), "payload": dict(payload),
        "authority": "ZERO"}
    ref = sha256_json({"artifact_type": artifact_type, "evidence": body})
    entry = repository.get_artifact(ref)
    metadata = {"evidence": body, "input_refs": list(refs)}
    if entry is None:
        repository.register_artifact(ArtifactIndexEntryV2(ref, artifact_type, ref,
            available_at_ns, available_at_ns, metadata))
        record_computation(repository, artifact_ref=ref,
            information_cutoff_ns=information_cutoff_ns, started_ns=available_at_ns,
            finished_ns=available_at_ns, available_ns=available_at_ns,
            input_refs=refs, deadline_ns=available_at_ns)
    elif (entry.artifact_type != artifact_type or entry.content_hash != ref
            or canonical_json(entry.metadata) != canonical_json(metadata)):
        raise ValueError("S7 derived evidence conflicts with its sealed causal inputs")
    return ref


def _s8_source_visible(repository: OpsRepository, ref: str, cutoff_ns: int,
                       published_by_ns: int | None = None) -> bool:
    if _source_visible(repository, ref, cutoff_ns):
        return True
    entry = repository.get_artifact(ref)
    feature_body = entry.metadata.get("feature") if entry is not None else None
    if (entry is None or entry.artifact_type != "S4FeatureArtifactV2"
            or entry.artifact_ref != ref or entry.content_hash != ref or not isinstance(feature_body, Mapping)):
        return False
    if entry.available_at_ns > (cutoff_ns if published_by_ns is None else published_by_ns):
        return False
    feature = S4FeatureArtifactV2.from_dict(dict(feature_body))
    return (feature.content_hash == ref and feature.cutoff_ns <= cutoff_ns
            and all(_source_visible(repository, source_ref, cutoff_ns) for source_ref in feature.input_refs))


def _native_history_visible(repository: OpsRepository, ref: str, *, cutoff_ns: int,
                            published_by_ns: int) -> bool:
    entry = repository.get_artifact(ref)
    if (entry is None or entry.artifact_type != "NativeStrategyHistoryBarV1"
            or entry.artifact_ref != ref or entry.content_hash != ref
            or entry.available_at_ns > published_by_ns):
        return False
    bar, raw_body = entry.metadata.get("bar"), entry.metadata.get("raw")
    refs = entry.metadata.get("input_refs")
    if (not isinstance(bar, Mapping) or not isinstance(raw_body, Mapping)
            or not isinstance(refs, (tuple, list)) or len(refs) != 1
            or not isinstance(refs[0], str) or sha256_json(bar) != ref
            or entry.metadata.get("authority") != "ZERO"
            or entry.metadata.get("information_cutoff_ns", cutoff_ns + 1) > cutoff_ns):
        return False
    raw = RawObservationV2.from_dict(raw_body)
    source = repository.get_artifact(refs[0])
    return bool(source is not None and source.artifact_type == "PublicObservationIndexV2"
        and source.content_hash == raw.content_hash
        and source.metadata.get("bar_content_hash") == ref
        and bar.get("record_id") == raw.record_id and bar.get("raw_payload_hash") == raw.raw_payload_hash
        and bar.get("instrument_revision") == raw.instrument_revision
        and raw.available_at_ns <= cutoff_ns and bar.get("close_at_ns", cutoff_ns + 1) <= cutoff_ns
        and bar.get("final") is True and _source_visible(repository, refs[0], cutoff_ns))


def _latest_s8_pair_catalog(repository: OpsRepository, *, cutoff_ns: int) -> tuple[S8PairDefinitionV2, ...]:
    page = repository.latest_artifact_entries("S8PairOwnerCatalogV1", as_of_ns=cutoff_ns, limit=1)
    if page.invalid_entry_count:
        raise ValueError("S8 owner pair catalog index is malformed")
    if len(page.entries) > 1:
        raise ValueError("S8 owner pair catalog requires one current registered configuration")
    if not page.entries:
        return ()
    entry = page.entries[0]
    raw = entry.metadata.get("catalog")
    if (not isinstance(raw, Mapping) or raw.get("version") != "S8_PAIR_OWNER_CATALOG_V1"
            or raw.get("available_at_ns") != entry.available_at_ns
            or raw.get("authority") != "RESEARCH_CONFIGURATION_ONLY"
            or raw.get("capital_authority") != "ZERO"
            or sha256_json(raw) != entry.content_hash):
        raise ValueError("S8 owner pair catalog body or chronology is invalid")
    raw_pairs = raw.get("pairs")
    if not isinstance(raw_pairs, (list, tuple)) or not raw_pairs or len(raw_pairs) > MAX_S8_PAIRS:
        raise ValueError("S8 owner pair catalog population is invalid")
    pairs = tuple(S8PairDefinitionV2(
        str(value["pair_id"]), str(value["economic_pair_definition"]),
        InstrumentKeyV2.from_dict(value["key_a"]), InstrumentKeyV2.from_dict(value["key_b"]),
        str(value["hedge_fit"]), str(value["residual_definition"]), str(value["version"]),
    ) for value in raw_pairs if isinstance(value, Mapping))
    if len(pairs) != len(raw_pairs) or tuple(sorted(pairs, key=lambda pair: pair.pair_id)) != pairs:
        raise ValueError("S8 owner pair definitions must be well-formed and canonical")
    for pair in pairs:
        pair_entry = repository.get_artifact(pair.content_hash)
        if (pair_entry is None or pair_entry.artifact_type != "S8PairDefinitionV2"
                or pair_entry.available_at_ns > cutoff_ns or pair_entry.content_hash != pair.content_hash
                or pair_entry.metadata.get("owner_catalog_ref") != entry.artifact_ref
                or canonical_json(pair_entry.metadata.get("pair")) != canonical_json(pair.to_dict())):
            raise ValueError("S8 pair definition is not registered to the current owner catalog")
    return pairs


def _broad_public_raw_rows(repository: OpsRepository, *, cutoff_ns: int,
                           keys: set[InstrumentKeyV2],
                           service_callback: Callable[[], None] | None = None
                           ) -> tuple[tuple[str, ArtifactIndexEntryV2,
                                                                      RawObservationV2, Mapping[str, Any]], ...]:
    """Read one exact, bounded broad-public receipt and its archived payload rows."""
    page = repository.latest_artifact_entries("BroadPublicAcquisitionReceiptV2",
        as_of_ns=cutoff_ns, limit=2)
    if page.invalid_entry_count:
        raise ValueError("broad public receipt index is malformed")
    if not page.entries:
        return ()
    receipt_entry = page.entries[0]
    receipt = receipt_entry.metadata.get("receipt")
    if (not isinstance(receipt, Mapping) or receipt.get("available_at_ns") != receipt_entry.available_at_ns
            or receipt.get("authority") != "ZERO" or receipt_entry.artifact_ref != receipt_entry.content_hash
            or sha256_json(receipt) != receipt_entry.content_hash):
        raise ValueError("broad public receipt body or chronology is invalid")
    source_refs = receipt.get("source_observation_refs")
    if not isinstance(source_refs, Mapping):
        raise ValueError("broad public receipt does not retain exact source refs")
    refs: list[str] = []
    for values in source_refs.values():
        if not isinstance(values, (tuple, list)) or any(not isinstance(ref, str) for ref in values):
            raise ValueError("broad public receipt source refs are malformed")
        refs.extend(values)
    if len(refs) > MAX_BROAD_SOURCE_REFS or len(set(refs)) != len(refs):
        raise ValueError("broad public receipt source population exceeds its fixed bound")
    indexed = repository.get_artifact_metadata_by_refs(refs)
    wanted: dict[str, tuple[ArtifactIndexEntryV2, InstrumentKeyV2]] = {}
    for index, ref in enumerate(refs, start=1):
        if service_callback is not None and index % 256 == 0:
            service_callback()
        row = indexed.get(ref)
        if not isinstance(row, Mapping) or row.get("artifact_type") != "PublicObservationIndexV2":
            raise ValueError("broad public receipt references a missing/non-public observation")
        meta = row.get("metadata")
        if not isinstance(meta, Mapping):
            raise ValueError("public observation index metadata is malformed")
        event_type = meta.get("event_type")
        if event_type not in (S6_BULK_EVENT_TYPES | S6_QUOTE_EVENT_TYPES | S5_FUNDING_EVENT_TYPES | S5_OI_EVENT_TYPES):
            continue
        key_json = meta.get("instrument_key_json")
        if not isinstance(key_json, str):
            raise ValueError("public derivative observation lacks full instrument identity")
        key = InstrumentKeyV2.from_dict(json.loads(key_json))
        if key not in keys or row.get("available_at_ns", cutoff_ns + 1) > cutoff_ns:
            continue
        chunk = meta.get("archive_chunk_id")
        record_id = meta.get("record_id")
        if (not isinstance(chunk, str) or len(chunk) != 64 or not isinstance(record_id, str)
                or sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": record_id}) != ref):
            raise ValueError("public observation archive locator identity is invalid")
        entry = repository.get_artifact(ref)
        if entry is None:
            raise ValueError("selected public derivative index is missing")
        wanted[ref] = (entry, key)
    if len(wanted) > MAX_SELECTED_RAW_RECORDS:
        raise ValueError("selected derivative source rows exceed their fixed bound")
    by_chunk: dict[str, set[str]] = {}
    for entry, _ in wanted.values():
        by_chunk.setdefault(str(entry.metadata["archive_chunk_id"]), set()).add(str(entry.metadata["record_id"]))
    found: dict[str, tuple[RawObservationV2, Mapping[str, Any]]] = {}
    archive_root = Path(repository.path).parent / "ops-observations"
    required = ("record_id", "observation_json", "raw_payload_bytes", "archive_record_kind")
    for chunk_id, selected_ids in by_chunk.items():
        path = archive_root / f"{chunk_id}.parquet"
        if path.is_symlink() or not path.is_file():
            raise ValueError("derivative archive points to a missing or unsafe chunk")
        parquet = _open_bounded_archive_chunk_v2(path)
        try:
            for batch in parquet.iter_batches(batch_size=128, columns=list(required)):
                if service_callback is not None:
                    service_callback()
                for archive_row in batch.to_pylist():
                    record_id = archive_row.get("record_id")
                    if record_id not in selected_ids:
                        continue
                    payload_bytes = archive_row.get("raw_payload_bytes")
                    raw_json = archive_row.get("observation_json")
                    if (archive_row.get("archive_record_kind") != "PUBLIC_OBSERVATION"
                            or not isinstance(payload_bytes, bytes) or not isinstance(raw_json, str)):
                        raise ValueError("selected raw derivative payload is malformed")
                    raw = RawObservationV2.from_dict(json.loads(raw_json))
                    if (raw.record_id != record_id
                            or hashlib.sha256(payload_bytes).hexdigest() != raw.raw_payload_hash):
                        raise ValueError("selected raw derivative payload hash/identity mismatch")
                    payload = json.loads(payload_bytes)
                    if not isinstance(payload, Mapping):
                        continue
                    ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": record_id})
                    if ref in found:
                        raise ValueError("derivative archive repeats a selected exact source record")
                    found[ref] = (raw, payload)
        finally:
            parquet.close()
    result: list[tuple[str, ArtifactIndexEntryV2, RawObservationV2, Mapping[str, Any]]] = []
    for ref, (entry, key) in wanted.items():
        pair = found.get(ref)
        if pair is None:
            raise ValueError("public observation archive omitted a selected exact ref")
        raw, payload = pair
        if (raw.instrument_revision != key.contract_revision
                or raw.source_id != entry.metadata.get("source_id")
                or raw.event_type != entry.metadata.get("event_type")
                or raw.raw_payload_hash != entry.metadata.get("raw_payload_hash")
                or not indexed_availability_matches(raw.available_at_ns, entry.available_at_ns, entry.metadata)
                or raw.received_at_ns != entry.created_at_ns
                or raw.availability_class != AvailabilityClassV2.ACTUAL_SYSTEM
                or any(entry.metadata.get(name) != getattr(raw, name) for name in (
                    "record_id", "instrument_revision", "event_at_ns", "published_at_ns",
                    "translation_version", "revision_of", "availability_class", "replay_available_at_ns"))
                or tuple(entry.metadata.get("quality_flags", ())) != raw.quality_flags
                or entry.content_hash != raw.content_hash):
            raise ValueError("public derivative source/index chronology mismatch")
        result.append((ref, entry, raw, {**payload, "__instrument_key_json": key.to_canonical_json()}))
    return tuple(sorted(result, key=lambda row: (row[2].available_at_ns, row[0])))


def _latest_public_health(repository: OpsRepository, source_id: str,
                          cutoff_ns: int) -> PublicSourceHealthV2 | None:
    page = repository.latest_artifact_entries("PublicSourceHealthV2", as_of_ns=cutoff_ns,
        metadata_path=("health", "source_id"), identity_value=source_id, limit=2)
    if page.invalid_entry_count:
        raise ValueError("public source health index is malformed")
    if not page.entries:
        return None
    entry = page.entries[0]
    raw = entry.metadata.get("health")
    if not isinstance(raw, Mapping):
        raise ValueError("public source health body is malformed")
    health = PublicSourceHealthV2.from_dict(raw)
    if (health.content_hash != entry.content_hash or health.content_hash != entry.artifact_ref
            or health.available_at_ns != entry.available_at_ns or health.source_id != source_id):
        raise ValueError("public source health identity does not match its exact index")
    return health


def _number(payload: Mapping[str, Any], *names: str) -> Decimal | None:
    for name in names:
        raw = payload.get(name)
        if raw in (None, ""):
            continue
        try:
            value = Decimal(str(raw))
        except Exception as exc:
            raise ValueError("raw derivative numeric field is invalid") from exc
        if not value.is_finite():
            raise ValueError("raw derivative numeric field is non-finite")
        return value
    return None


def _normalized_derivatives(repository: OpsRepository, *, universe: UniverseContractV2,
                            cutoff_ns: int,
                            service_callback: Callable[[], None] | None = None,
                            clock_ns: Callable[[], int] = time.time_ns,
                            sequence_features: Mapping[InstrumentKeyV2, S4FeatureArtifactV2] | None = None,
                            oi_output: dict[InstrumentKeyV2, tuple[OpenInterestObservationV2, ...]] | None = None,
                            ) -> tuple[dict[InstrumentKeyV2, LiquidityFundingEvidenceV2],
                                       dict[InstrumentKeyV2, S5CrowdingContextV2],
                                       dict[InstrumentKeyV2, tuple[str, ...]]]:
    keys = {entry.key for entry in universe.entries}
    rows = _broad_public_raw_rows(repository, cutoff_ns=cutoff_ns, keys=keys,
                                  service_callback=service_callback)
    by_key: dict[InstrumentKeyV2, list[tuple[str, ArtifactIndexEntryV2, RawObservationV2, Mapping[str, Any]]]] = {}
    for ref, entry, raw, payload in rows:
        key = InstrumentKeyV2.from_dict(json.loads(str(payload["__instrument_key_json"])))
        by_key.setdefault(key, []).append((ref, entry, raw, payload))
    matrix = default_evidence_capability_matrix_v2()
    liquidity: dict[InstrumentKeyV2, LiquidityFundingEvidenceV2] = {}
    contexts: dict[InstrumentKeyV2, S5CrowdingContextV2] = {}
    funding_refs: dict[InstrumentKeyV2, tuple[str, ...]] = {}
    for key, items in by_key.items():
        if service_callback is not None:
            service_callback()
        health_by_source = {source: _latest_public_health(repository, source, cutoff_ns)
                            for source in {item[2].source_id for item in items}}
        funding: list[FundingObservationV2] = []
        open_interest: list[OpenInterestObservationV2] = []
        current_quote: tuple[str, ArtifactIndexEntryV2, RawObservationV2, Mapping[str, Any]] | None = None
        current_turnover: tuple[str, ArtifactIndexEntryV2, RawObservationV2, Mapping[str, Any]] | None = None
        current_funding: tuple[str, ArtifactIndexEntryV2, RawObservationV2, Mapping[str, Any]] | None = None
        for ref, entry, raw, payload in items:
            health = health_by_source.get(raw.source_id)
            health_ref = health.content_hash if health is not None else None
            if raw.event_type == "TICKER_MARK_INDEX_FUNDING_OI":
                current_quote = current_funding = (ref, entry, raw, payload)
            elif raw.event_type == "BOOK_TICKER":
                current_quote = (ref, entry, raw, payload)
            elif raw.event_type == "MARK_INDEX_CURRENT_FUNDING":
                current_funding = (ref, entry, raw, payload)
            if raw.event_type in S6_QUOTE_EVENT_TYPES or raw.event_type == "TICKER_MARK_INDEX_FUNDING_OI":
                if _number(payload, "turnover24h", "quoteVolume") is not None:
                    current_turnover = (ref, entry, raw, payload)
                if _number(payload, "bid1Price", "bidPrice", "b") is not None:
                    current_quote = (ref, entry, raw, payload)
            if raw.event_type in S5_FUNDING_EVENT_TYPES or raw.event_type in S6_BULK_EVENT_TYPES:
                rate = _number(payload, "fundingRate", "lastFundingRate", "r")
                if rate is not None:
                    venue_channel = ("V5 REST ticker/funding-history/open-interest" if key.venue.value == "BYBIT"
                        else "USD-M REST mark/funding/open-interest")
                    # Explicit current-rate endpoints attest CURRENT only;
                    # history rows lack settlement attestation.
                    kind = (FundingKindV2.CURRENT if raw.event_type in S6_BULK_EVENT_TYPES
                            else FundingKindV2.UNQUALIFIED)
                    funding.append(FundingObservationV2(key, raw.source_id, kind, rate,
                        "VENUE_RAW_RATE", raw.event_at_ns, raw.received_at_ns, entry.available_at_ns,
                        _ms_timestamp(payload.get("nextFundingTime")), matrix.content_hash, ref,
                        DerivativeAvailabilityV2.ACTUAL_RECEIPT, raw.revision_of,
                        health.state.value if health else "UNKNOWN", health_ref, channel=venue_channel))
                    funding_refs.setdefault(key, ())
                    funding_refs[key] = tuple(sorted(set(funding_refs[key]) | {ref}))
            if raw.event_type in S5_OI_EVENT_TYPES or raw.event_type in S6_BULK_EVENT_TYPES:
                quantity = _number(payload, "openInterest", "sumOpenInterest", "openInterestAmount")
                value = _number(payload, "openInterestValue", "sumOpenInterestValue")
                mark = _number(payload, "markPrice")
                index = _number(payload, "indexPrice")
                last = _number(payload, "lastPrice")
                if quantity is not None or value is not None:
                    venue_channel = ("V5 REST ticker/funding-history/open-interest" if key.venue.value == "BYBIT"
                        else "USD-M REST mark/funding/open-interest")
                    open_interest.append(OpenInterestObservationV2(key, raw.source_id, quantity,
                        "VENUE_RAW_CONTRACT_UNITS" if quantity is not None else None, value,
                        "VENUE_RAW_QUOTE_VALUE" if value is not None else None, raw.event_at_ns,
                        raw.received_at_ns, entry.available_at_ns, matrix.content_hash, ref,
                        DerivativeAvailabilityV2.ACTUAL_RECEIPT, raw.revision_of, mark, index, last,
                        health.state.value if health else "UNKNOWN", health_ref, venue_channel))
        if current_quote is not None and current_funding is not None:
            quote_ref, quote_entry, quote_raw, quote_body = current_quote
            fund_ref, fund_entry, fund_raw, fund_body = current_funding
            health = health_by_source.get(quote_raw.source_id)
            bid = _number(quote_body, "bid1Price", "bidPrice", "b")
            ask = _number(quote_body, "ask1Price", "askPrice", "a")
            turnover = _number(current_turnover[3], "turnover24h", "quoteVolume") if current_turnover else None
            rate = _number(fund_body, "fundingRate", "lastFundingRate", "r")
            if (health is not None and current_turnover is not None
                    and bid is not None and ask is not None and bid > 0 and ask >= bid
                    and turnover is not None and rate is not None):
                midpoint = (bid + ask) / Decimal(2)
                turnover_ref, turnover_entry, turnover_raw, _ = current_turnover
                observed = max(quote_raw.received_at_ns, fund_raw.received_at_ns, turnover_raw.received_at_ns)
                available = max(quote_entry.available_at_ns, fund_entry.available_at_ns,
                    turnover_entry.available_at_ns, health.available_at_ns)
                liquidity[key] = LiquidityFundingEvidenceV2(key, observed, available,
                    (ask - bid) / midpoint * Decimal(10_000), turnover, rate,
                    quote_ref, fund_ref, health, turnover_ref if turnover_ref != quote_ref else None)
        observations: list[FundingObservationV2 | OpenInterestObservationV2] = [*funding, *open_interest]
        for observation in observations:
            observation_refs = tuple(sorted({observation.raw_content_ref,
                *([observation.source_health_ref] if observation.source_health_ref else [])}))
            _persist_s5_derived(repository, artifact_ref=observation.content_hash,
                artifact_type=type(observation).__name__, payload={"observation": observation.to_dict()},
                input_refs=observation_refs, cutoff_ns=cutoff_ns, clock_ns=clock_ns)
        delta = oi_change_15m_evidence(tuple(open_interest), cutoff_ns=cutoff_ns, instrument=key)
        if delta is not None:
            _persist_s5_derived(repository, artifact_ref=delta.content_hash, artifact_type="OIChangeEvidenceV2",
                payload={"evidence": delta.to_dict()}, input_refs=delta.input_refs,
                cutoff_ns=cutoff_ns, clock_ns=clock_ns)
        if oi_output is not None:
            oi_output[key] = tuple(open_interest)
        feature = (sequence_features or {}).get(key)
        liquidity_state = "SEQUENCE_VALID_S4" if feature is not None and feature.estimable else "UNKNOWN"
        try:
            contexts[key] = build_s5_crowding_context(instrument=key, cutoff_ns=cutoff_ns,
                funding=tuple(funding), open_interest=tuple(open_interest), liquidity_state=liquidity_state)
        except ValueError:
            continue
    return liquidity, contexts, funding_refs


def _ms_timestamp(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("venue funding timestamp is invalid") from exc
    return number * 1_000_000 if number < 10**15 else number


def _build_s7_inputs(repository: OpsRepository, *, candidates: Sequence[S7DirectionalInputV1],
                     universe: UniverseContractV2, cutoff_ns: int,
                     minute_indexed: Mapping[InstrumentKeyV2, Sequence[Any]],
                     five_minute_indexed: Mapping[InstrumentKeyV2, Sequence[Any]],
                     sequence_features: Mapping[InstrumentKeyV2, S4FeatureArtifactV2],
                     clock_ns: Callable[[], int],
                     service_callback: Callable[[], None] | None) -> tuple[S7DirectionalInputV1, ...]:
    btc_by_venue = {entry.key.venue: entry.key for entry in universe.entries
        if entry.data_eligible and not entry.capital_eligible and entry.key.native_symbol == "BTCUSDT"}
    needed = {candidate.key for candidate in candidates}
    needed.update(btc_by_venue[candidate.key.venue] for candidate in candidates
        if candidate.key.venue in btc_by_venue)
    bars_by_key: dict[InstrumentKeyV2, tuple[CausalBarV2, ...]] = {}
    for key in sorted(needed, key=lambda value: value.to_canonical_json()):
        direct = tuple(row.bar for row in five_minute_indexed.get(key, ()))
        bars_by_key[key] = direct or _derive_m5_from_m1(repository, key=key,
            indexed_m1=minute_indexed.get(key, ()), cutoff_ns=cutoff_ns,
            clock_ns=clock_ns, service_callback=service_callback)

    results: list[S7DirectionalInputV1] = []
    for candidate in candidates[:MAX_S7_EVENTS]:
        event, key = candidate.event, candidate.key
        bars = tuple(bar for bar in bars_by_key.get(key, ())
            if bar.raw.available_at_ns <= cutoff_ns and bar.close_at_ns <= cutoff_ns)
        first_index = next((index for index, bar in enumerate(bars)
            if bar.open_at_ns >= event.available_at_ns), None)
        if first_index is None:
            results.append(candidate)
            continue
        first_bar = bars[first_index]
        second_bar = bars[first_index + 1] if first_index + 1 < len(bars) else None
        history = tuple(bar for bar in bars[:first_index] if bar.close_at_ns <= first_bar.open_at_ns)[-128:]
        event_health = _latest_public_health(repository, event.source_id, cutoff_ns)
        bar_health = _latest_public_health(repository, first_bar.raw.source_id, cutoff_ns)
        btc_proxy = btc_by_venue.get(key.venue)
        beta = None
        btc_return = None
        if btc_proxy is not None:
            btc_bars = tuple(bar for bar in bars_by_key.get(btc_proxy, ())
                if bar.raw.available_at_ns <= event.received_at_ns and bar.close_at_ns < event.received_at_ns)
            asset_pre = tuple(bar for bar in bars_by_key.get(key, ())
                if bar.raw.available_at_ns <= event.received_at_ns and bar.close_at_ns < event.received_at_ns)
            beta_pair = _fit_s7_pre_event_beta(repository, key=key, btc_proxy=btc_proxy,
                asset_bars=asset_pre, btc_bars=btc_bars, event_at_ns=event.received_at_ns,
                clock_ns=clock_ns)
            beta = beta_pair
            matching_btc = next((bar for bar in bars_by_key.get(btc_proxy, ())
                if bar.close_at_ns == first_bar.close_at_ns and bar.raw.available_at_ns <= cutoff_ns), None)
            if matching_btc is not None:
                btc_refs = _m5_input_refs(repository, matching_btc.content_hash, cutoff_ns=cutoff_ns)
                btc_available = max(cutoff_ns, timestamp(clock_ns(), field="S7 BTC return publication"))
                btc_ref = _persist_s7_derived_evidence(repository,
                    artifact_type="S7DerivedMarketReturn5MV1", information_cutoff_ns=cutoff_ns,
                    available_at_ns=btc_available,
                    payload={"key": btc_proxy.to_dict(), "close_at_ns": matching_btc.close_at_ns,
                        "log_return": math.log(float(matching_btc.close) / float(matching_btc.open))},
                    input_refs=btc_refs)
                btc_return = MarketReturn5MV2(btc_proxy, matching_btc.close_at_ns, btc_available,
                    math.log(float(matching_btc.close) / float(matching_btc.open)), btc_ref)
        spread = _build_s7_spread(repository, key=key, feature=sequence_features.get(key),
            cutoff_ns=cutoff_ns, clock_ns=clock_ns)
        results.append(S7DirectionalInputV1(event, key, first_bar, second_bar, history,
            beta, btc_return, spread, event_health, bar_health, candidate.resolution))
    return tuple(results)


def _m5_input_refs(repository: OpsRepository, bar_ref: str, *, cutoff_ns: int) -> tuple[str, ...]:
    entry = repository.get_artifact(bar_ref)
    if entry is None or entry.artifact_type not in {"S7DerivedM5BarV1", "NativeStrategyHistoryBarV1"}:
        return (bar_ref,) if entry is not None and _source_visible(repository, bar_ref, cutoff_ns) else ()
    raw_refs = entry.metadata.get("input_refs")
    if not isinstance(raw_refs, (tuple, list)):
        raise ValueError("derived S7 M5 bar omitted its exact M1 origins")
    refs = tuple(sorted(set(raw_refs)))
    if not refs or any(not _source_visible(repository, ref, cutoff_ns) for ref in refs):
        raise ValueError("derived S7 M5 bar references unavailable M1 origins")
    return refs


def _fit_s7_pre_event_beta(repository: OpsRepository, *, key: InstrumentKeyV2,
                           btc_proxy: InstrumentKeyV2, asset_bars: Sequence[CausalBarV2],
                           btc_bars: Sequence[CausalBarV2], event_at_ns: int,
                           clock_ns: Callable[[], int]) -> PreEventBetaV2 | None:
    if key.venue != btc_proxy.venue:
        return None
    def prior_bars(rows: Sequence[CausalBarV2], expected: InstrumentKeyV2) -> dict[int, CausalBarV2]:
        return {bar.close_at_ns: bar for bar in rows
            if bar.instrument_revision == expected.contract_revision and bar.interval == BarIntervalV2.M5
            and bar.final and bar.close_at_ns < event_at_ns and bar.raw.available_at_ns < event_at_ns}

    asset_by_close = prior_bars(asset_bars, key)
    btc_by_close = prior_bars(btc_bars, btc_proxy)
    # Fifty M5 pairs expand to at most 500 exact native M1 origins, within
    # the sealed S7 evidence read bound of 512 references.
    times = tuple(sorted(set(asset_by_close) & set(btc_by_close)))[-50:]
    if len(times) < 20:
        return None
    x = [math.log(float(btc_by_close[at].close) / float(btc_by_close[at].open)) for at in times]
    y = [math.log(float(asset_by_close[at].close) / float(asset_by_close[at].open)) for at in times]
    mean_x, mean_y = sum(x) / len(x), sum(y) / len(y)
    variance = sum((value - mean_x) ** 2 for value in x)
    if variance <= 0:
        return None
    beta_value = sum((left - mean_x) * (right - mean_y) for left, right in zip(x, y, strict=True)) / variance
    input_refs = tuple(sorted({ref for at in times for ref in (
        *_m5_input_refs(repository, asset_by_close[at].content_hash, cutoff_ns=event_at_ns),
        *_m5_input_refs(repository, btc_by_close[at].content_hash, cutoff_ns=event_at_ns))}))
    available = max(event_at_ns, timestamp(clock_ns(), field="S7 beta publication"))
    ref = _persist_s7_derived_evidence(repository, artifact_type="S7DerivedPreEventBetaV1",
        information_cutoff_ns=event_at_ns, available_at_ns=available,
        payload={"key": key.to_dict(), "btc_proxy": btc_proxy.to_dict(),
            "beta": beta_value, "estimated_through_ns": times[-1], "matched_sample_count": len(times)},
        input_refs=input_refs)
    return PreEventBetaV2(key, btc_proxy, beta_value, times[-1], available, ref)


def _build_s7_spread(repository: OpsRepository, *, key: InstrumentKeyV2,
                     feature: S4FeatureArtifactV2 | None, cutoff_ns: int,
                     clock_ns: Callable[[], int]) -> ReactionSpreadEvidenceV2 | None:
    if (feature is None or feature.instrument != key or feature.cutoff_ns != cutoff_ns
            or not feature.estimable or feature.spread is None or feature.data_age_ns is None
            or not feature.input_refs
            or any(not _source_visible(repository, ref, cutoff_ns) for ref in feature.input_refs)):
        return None
    observed = max(0, cutoff_ns - feature.data_age_ns)
    available = max(cutoff_ns, timestamp(clock_ns(), field="S7 spread publication"))
    refs = tuple(sorted(set(feature.input_refs)))
    ref = _persist_s7_derived_evidence(repository, artifact_type="S7DerivedReactionSpreadV1",
        information_cutoff_ns=cutoff_ns, available_at_ns=available,
        payload={"key": key.to_dict(), "spread": feature.spread,
            "observed_at_ns": observed, "acceptable": True}, input_refs=refs)
    return ReactionSpreadEvidenceV2(key, True, observed, available, ref)


def _persist_role(repository: OpsRepository, *, role: str, status: str, cutoff_ns: int,
                  published_at_ns: int, payload: Mapping[str, Any], input_refs: Sequence[str],
                  missing_reasons: Sequence[str] = ()) -> str:
    ordered_refs = tuple(sorted(set(input_refs)))
    body = {
        "version": f"{role}_RESEARCH_ROLE_V1",
        "role": role,
        "status": status,
        "information_cutoff_ns": cutoff_ns,
        "available_at_ns": published_at_ns,
        "input_refs": list(ordered_refs),
        "missing_reasons": sorted(set(missing_reasons)),
        "payload": dict(payload),
        "authority": "ZERO",
        "candidate_action_ref": None,
        "trade_plan_allowed": False,
    }
    ref = sha256_json(body)
    repository.register_artifact(ArtifactIndexEntryV2(
        ref, f"{role}ResearchRoleV1", ref, published_at_ns, published_at_ns, {"role": body},
    ))
    record_computation(repository, artifact_ref=ref, information_cutoff_ns=cutoff_ns,
        started_ns=published_at_ns, finished_ns=published_at_ns, available_ns=published_at_ns,
        input_refs=ordered_refs, deadline_ns=published_at_ns)
    return ref


def compose_full_strategy_surface(
    repository: OpsRepository,
    *,
    universe: UniverseContractV2,
    cutoff_ns: int,
    s4_features: Mapping[InstrumentKeyV2, S4FeatureArtifactV2] | None = None,
    s4_baselines: Mapping[InstrumentKeyV2, S4ExpectedResponseBaselineV2] | None = None,
    s5_continuation: Mapping[InstrumentKeyV2, S5ContinuationResearchV2] | None = None,
    s5_reversal: Sequence[S5ReversalInputV1] = (),
    s5_contexts: Mapping[InstrumentKeyV2, S5CrowdingContextV2] | None = None,
    s6_hourly_bars: Mapping[InstrumentKeyV2, Sequence[Any]] | None = None,
    s6_four_hour_bars: Mapping[InstrumentKeyV2, Sequence[Any]] | None = None,
    s6_evidence: Mapping[InstrumentKeyV2, LiquidityFundingEvidenceV2] | None = None,
    s6_btc_proxy: InstrumentKeyV2 | None = None,
    s6_btc_proxies: Sequence[InstrumentKeyV2] = (),
    s7_inputs: Sequence[S7DirectionalInputV1] = (),
    s7_population: S7PopulationEvidenceV1 | None = None,
    s8_inputs: Sequence[S8PairInputV1] = (),
    model_forecasts: Sequence[ModelForecastBindingV1] = (),
    s5_prepared_diagnostics: Mapping[InstrumentKeyV2, S5PreparedDiagnosticsV1] | None = None,
    clock_ns: Callable[[], int] = time.time_ns,
    service_callback: Callable[[], None] | None = None,
) -> FullStrategySurfaceResultV1:
    """Compose causal S4-S8 research artifacts; never create candidate actions.

    Inputs are typed artifacts produced by the existing public evidence
    collectors. Every declared reference must resolve to an exact indexed
    artifact available by ``cutoff_ns``. Derived role receipts are published
    using the actual caller clock, after their calculations complete.
    """
    cutoff = timestamp(cutoff_ns, field="strategy_surface.cutoff_ns")
    if repository.read_only:
        raise ValueError("full strategy surface requires the caller-owned writable ops repository")
    composition_started = timestamp(clock_ns(), field="strategy_surface.started_at")
    if composition_started < cutoff or cutoff > universe.decision_slot_ns:
        raise ValueError("surface computation precedes cutoff or is outside the universe decision slot")
    if universe.envelope.available_at_ns > composition_started:
        raise ValueError("surface universe has not been published by actual computation start")
    if universe.envelope.available_at_ns > cutoff:
        if (repository.get_artifact(chronology_ref(universe.content_hash)) is None
                or not causal_artifact(repository, universe.content_hash, cutoff_ns=cutoff,
                    consumer_at_ns=universe.envelope.available_at_ns,
                    deadline_ns=universe.decision_slot_ns)):
            raise ValueError("late derived universe lacks a valid cutoff-sealed computation receipt")
    total_inputs = (len(universe.entries) + len(s4_features or {}) + len(s4_baselines or {})
        + len(s5_continuation or {}) + len(s5_reversal) + len(s6_hourly_bars or {})
        + len(s6_four_hour_bars or {}) + len(s6_evidence or {}) + len(s7_inputs)
        + len(s8_inputs) + len(model_forecasts))
    total_inputs += sum(len(bars) for values in (s6_hourly_bars or {}, s6_four_hour_bars or {})
                        for bars in values.values())
    total_inputs += sum(len(feature.input_refs) for feature in (s4_features or {}).values())
    total_inputs += sum(len(baseline.input_refs) for baseline in (s4_baselines or {}).values())
    total_inputs += 3 * len(s6_evidence or {})
    total_inputs += sum(sum(len(getattr(reducer, name)) for name in (
        "vulnerabilities", "breaks", "deleveraging_events", "confirmations"))
        for reducer in (s5_continuation or {}).values())
    total_inputs += sum(len(item.s4.input_refs) + sum(len(getattr(value, "refs", ()))
        for value in (item.absorption, item.liquidation_window, item.liquidation_baseline,
                      item.exhaustion, item.reclaim, item.flow_reversal) if value is not None)
        for item in s5_reversal)
    total_inputs += sum(len(item.history_bars) + 7 for item in s7_inputs)
    total_inputs += sum(len(item.prices_a) + len(item.prices_b) + 12 for item in s8_inputs)
    total_inputs += sum(len(item.request.input_artifact_refs) + 2 for item in model_forecasts)
    total_inputs += len(s5_contexts or {})
    total_inputs += sum(len(item.input_refs) + 1 for item in (s5_prepared_diagnostics or {}).values())
    if len(universe.entries) > MAX_SURFACE_UNIVERSE or total_inputs > MAX_TOTAL_HISTORY_BARS:
        raise ValueError("surface input population exceeds its fixed bound")

    statuses: dict[str, str] = {}
    refs_by_role: dict[str, tuple[str, ...]] = {}
    reasons: dict[str, tuple[str, ...]] = {}
    role_payloads: dict[str, tuple[str, Mapping[str, Any], tuple[str, ...], tuple[str, ...]]] = {}
    publication_floor = max(cutoff, universe.envelope.available_at_ns)

    def publication_time() -> int:
        nonlocal publication_floor
        publication_floor = max(publication_floor, timestamp(clock_ns(), field="strategy_surface.publication"))
        return publication_floor

    def set_role(role: str, status: str, payload: Mapping[str, Any], refs: Sequence[str],
                 missing: Sequence[str] = ()) -> None:
        if service_callback is not None:
            service_callback()
        statuses[role] = status
        reasons[role] = tuple(sorted(set(missing)))
        role_payloads[role] = (status, payload, tuple(refs), tuple(missing))

    # S4 execution context and standalone absorption hypothesis share the
    # same typed feature but remain separately labelled artifacts.
    s4_rows: list[dict[str, Any]] = []
    s4_context_refs: list[str] = []
    s4_shadow_refs: list[str] = []
    for key, feature in sorted((s4_features or {}).items(), key=lambda row: row[0].to_canonical_json()):
        if service_callback is not None:
            service_callback()
        if feature.instrument != key or feature.cutoff_ns > cutoff:
            raise ValueError("S4 feature key/cutoff does not match the research surface")
        feature_published_at = publication_time()
        if (not _s4_feature_indexed(repository, feature, feature_published_at)
                or any(not _source_visible(repository, ref, cutoff) for ref in feature.input_refs)):
            raise ValueError("S4 feature or source evidence is missing or unavailable at cutoff")
        refs = tuple(sorted(set(feature.input_refs + (feature.content_hash,))))
        context = s4_execution_quality_context(feature)
        s4_context_refs.append(_persist_role(repository, role="S4ExecutionContext",
            status="AVAILABLE" if feature.estimable else "NOT_ESTIMABLE", cutoff_ns=cutoff,
            published_at_ns=publication_time(), payload=context, input_refs=refs,
            missing_reasons=(() if feature.estimable else (feature.missing_reason or "S4_FEATURE_NOT_ESTIMABLE",))))
        baseline = (s4_baselines or {}).get(key)
        if baseline is not None:
            indexed_baseline = repository.get_artifact(baseline.content_hash)
            if (baseline.fit_cutoff_ns >= feature.cutoff_ns or indexed_baseline is None
                    or indexed_baseline.artifact_type != "S4ExpectedResponseBaselineV2"
                    or indexed_baseline.content_hash != baseline.content_hash
                    or canonical_json(indexed_baseline.metadata.get("baseline")) != canonical_json(_s4_baseline_body(baseline))
                    or any(not causal_artifact(repository, ref, cutoff_ns=cutoff,
                        consumer_at_ns=publication_floor, deadline_ns=publication_floor)
                        for ref in (*baseline.input_refs, baseline.content_hash))):
                raise ValueError("S4 expected-response baseline is not exact, prior-only or source-visible")
        absorption = estimate_s4_absorption(feature=feature, baseline=baseline)
        shadow_payload = absorption.to_dict()
        shadow_refs = tuple(sorted(set(absorption.evidence_refs + refs)))
        shadow_status = "AVAILABLE" if absorption.state in ("ABSORPTION_HYPOTHESIS", "NO_ABSORPTION_HYPOTHESIS") else "NOT_ESTIMABLE"
        s4_shadow_refs.append(_persist_role(repository, role="S4StandaloneShadow",
            status=shadow_status, cutoff_ns=cutoff, published_at_ns=publication_time(),
            payload=shadow_payload, input_refs=shadow_refs,
            missing_reasons=(() if shadow_status == "AVAILABLE" else (absorption.threshold_status,))))
        s4_rows.append({"key": key.to_dict(), "context_ref": s4_context_refs[-1],
                        "shadow_ref": s4_shadow_refs[-1], "state": absorption.state})
    set_role("S4", "AVAILABLE" if s4_rows else "NOT_ESTIMABLE", {"rows": s4_rows},
             (*s4_context_refs, *s4_shadow_refs), () if s4_rows else ("S4_TYPED_FEATURES_UNAVAILABLE",))

    # S5 reducers are independent versioned continuation policies. Reversal
    # requires a separate S4 + exhaustion + reclaim/flow-reversal input.
    continuation_refs: list[str] = []
    continuation_rows: list[dict[str, Any]] = []
    for key, reducer in sorted((s5_continuation or {}).items(), key=lambda row: row[0].to_canonical_json()):
        if service_callback is not None:
            service_callback()
        continuation_artifact = reducer.artifact(cutoff_ns=cutoff)
        input_refs = tuple(sorted({ref for stage in (continuation_artifact.vulnerability, continuation_artifact.break_evidence,
            continuation_artifact.deleveraging_evidence, continuation_artifact.confirmation) if stage is not None for ref in stage.refs}))
        if any(not _source_visible(repository, ref, cutoff) for ref in input_refs):
            raise ValueError("S5 continuation source evidence is missing or unavailable at cutoff")
        status = "AVAILABLE" if continuation_artifact.state != "NOT_ESTIMABLE" else "NOT_ESTIMABLE"
        ref = _persist_role(repository, role="S5Continuation", status=status, cutoff_ns=cutoff,
            published_at_ns=publication_time(), payload=continuation_artifact.to_dict(), input_refs=input_refs,
            missing_reasons=(() if status == "AVAILABLE" else ("S5_CONTINUATION_EVIDENCE_UNAVAILABLE",)))
        continuation_refs.append(ref)
        continuation_rows.append({"key": key.to_dict(), "artifact_ref": ref, "state": continuation_artifact.state})
    reversal_refs: list[str] = []
    reversal_rows: list[dict[str, Any]] = []
    for reversal_input in s5_reversal:
        if reversal_input.s4.instrument != reversal_input.key:
            raise ValueError("S5 reversal S4 feature key mismatch")
        reversal_delivery = publication_time()
        reversal_artifact: S5ReversalArtifactV2 = evaluate_s5_reversal(cutoff_ns=reversal_delivery, s4=reversal_input.s4,
            absorption=reversal_input.absorption, liquidation_window=reversal_input.liquidation_window,
            liquidation_baseline=reversal_input.liquidation_baseline, exhaustion=reversal_input.exhaustion,
            reclaim=reversal_input.reclaim, flow_reversal=reversal_input.flow_reversal)
        reversal_artifact = _actual_reversal_stage_delivery(repository, reversal_artifact)
        source_refs = set(reversal_input.s4.input_refs) | {reversal_input.s4.content_hash}
        for value in (reversal_input.absorption, reversal_input.liquidation_window, reversal_input.liquidation_baseline,
                      reversal_input.exhaustion, reversal_input.reclaim, reversal_input.flow_reversal):
            if value is None:
                continue
            if hasattr(value, "content_hash"):
                source_refs.add(value.content_hash)
            source_refs.update(getattr(value, "input_refs", ()))
            source_refs.update(getattr(value, "refs", ()))
        feature_derived_ref = reversal_input.s4.content_hash
        complete_refs = tuple(sorted(source_refs))
        source_refs.discard(feature_derived_ref)
        if (not _s4_feature_indexed(repository, reversal_input.s4, publication_floor)
                or any(not causal_artifact(repository, ref, cutoff_ns=cutoff,
                    consumer_at_ns=reversal_delivery, deadline_ns=reversal_delivery) for ref in source_refs)):
            raise ValueError("S5 reversal source evidence is missing or unavailable at cutoff")
        status = "AVAILABLE" if reversal_artifact.state == "POST_CASCADE_REVERSAL_CONFIRMED" else "NOT_ESTIMABLE"
        ref = _persist_role(repository, role="S5Reversal", status=status, cutoff_ns=cutoff,
            published_at_ns=publication_time(), payload=reversal_artifact.to_dict(), input_refs=complete_refs,
            missing_reasons=(() if status == "AVAILABLE" else (reversal_artifact.state,)))
        reversal_refs.append(ref)
        reversal_rows.append({"key": reversal_input.key.to_dict(), "artifact_ref": ref, "state": reversal_artifact.state})
    for key, prepared in sorted((s5_prepared_diagnostics or {}).items(), key=lambda row: row[0].to_canonical_json()):
        delivered = publication_time()
        if (prepared.key != key or prepared.information_cutoff_ns != cutoff
                or prepared.delivered_at_ns > delivered
                or prepared.continuation.cutoff_ns != prepared.delivered_at_ns
                or (prepared.reversal is not None and prepared.reversal.cutoff_ns != prepared.delivered_at_ns)):
            raise ValueError("S5 prepared diagnostics key, sealed cutoff, or actual delivery mismatch")
        entry = repository.get_artifact(prepared.content_hash)
        if (entry is None or entry.artifact_type != "S5PreparedDiagnosticsV1"
                or canonical_json(entry.metadata.get("diagnostics")) != canonical_json(prepared.to_dict())
                or any(not causal_artifact(repository, source_ref, cutoff_ns=cutoff,
                    consumer_at_ns=delivered, deadline_ns=delivered)
                    for source_ref in (*prepared.input_refs, prepared.content_hash))):
            raise ValueError("S5 prepared diagnostics lacks exact causal source receipts")
        missing = tuple(prepared.criteria_payload.get("missing_reasons", ()))
        status = "AVAILABLE" if prepared.continuation.state == "CONTINUATION_CONFIRMED" else "NOT_ESTIMABLE"
        ref = _persist_role(repository, role="S5Continuation", status=status, cutoff_ns=cutoff,
            published_at_ns=delivered, payload={**prepared.continuation.to_dict(),
                "structural_criteria": dict(prepared.criteria_payload)},
            input_refs=(*prepared.input_refs, prepared.content_hash), missing_reasons=missing)
        continuation_refs.append(ref)
        continuation_rows.append({"key": key.to_dict(), "artifact_ref": ref,
            "state": prepared.continuation.state, "information_cutoff_ns": cutoff,
            "delivered_at_ns": prepared.delivered_at_ns})
        if prepared.reversal is not None:
            reversal_status = "AVAILABLE" if prepared.reversal.state == "POST_CASCADE_REVERSAL_CONFIRMED" else "NOT_ESTIMABLE"
            ref = _persist_role(repository, role="S5Reversal", status=reversal_status, cutoff_ns=cutoff,
                published_at_ns=publication_time(), payload={**prepared.reversal.to_dict(),
                    "structural_criteria": dict(prepared.criteria_payload)},
                input_refs=(*prepared.input_refs, prepared.content_hash),
                missing_reasons=(*missing, prepared.reversal.missing_reason or prepared.reversal.state))
            reversal_refs.append(ref)
            reversal_rows.append({"key": key.to_dict(), "artifact_ref": ref,
                "state": prepared.reversal.state, "information_cutoff_ns": cutoff,
                "delivered_at_ns": prepared.delivered_at_ns})
    context_refs: list[str] = []
    context_rows: list[dict[str, Any]] = []
    for key, context_value in sorted((s5_contexts or {}).items(), key=lambda row: row[0].to_canonical_json()):
        if context_value.instrument != key or context_value.cutoff_ns > cutoff:
            raise ValueError("S5 crowding context key/cutoff does not match the research surface")
        if any(not causal_artifact(repository, ref, cutoff_ns=cutoff,
                consumer_at_ns=publication_time(), deadline_ns=publication_floor)
                for ref in context_value.input_refs):
            raise ValueError("S5 crowding context source evidence is missing or unavailable at cutoff")
        ref = context_value.content_hash
        publish_at = publication_time()
        entry = repository.get_artifact(ref)
        context_body = {"context": context_value.to_dict(),
            "input_refs": list(context_value.input_refs)}
        if entry is None:
            repository.register_artifact(ArtifactIndexEntryV2(ref, "S5CrowdingContextV2", ref,
                publish_at, publish_at, context_body))
            record_computation(repository, artifact_ref=ref, information_cutoff_ns=cutoff,
                started_ns=publish_at, finished_ns=publish_at, available_ns=publish_at,
                input_refs=context_value.input_refs, deadline_ns=publish_at)
        elif (entry.artifact_type != "S5CrowdingContextV2" or entry.content_hash != ref
                or canonical_json(entry.metadata) != canonical_json(context_body)):
            raise ValueError("S5 crowding context index conflicts with its typed input")
        context_refs.append(ref)
        context_rows.append({"key": key.to_dict(), "artifact_ref": ref, "state": context_value.state,
            "evidence_quality": context_value.evidence_quality, "liquidation_coverage": context_value.liquidation_coverage.value})
    s5_status = "AVAILABLE" if continuation_rows or reversal_rows or context_rows else "NOT_ESTIMABLE"
    s5_missing = []
    if not continuation_rows:
        s5_missing.append("S5_CONTINUATION_INPUTS_UNAVAILABLE")
    if not reversal_rows:
        s5_missing.append("S5_REVERSAL_INPUTS_UNAVAILABLE")
    if not context_rows:
        s5_missing.append("S5_CROWDING_CONTEXT_UNAVAILABLE")
    set_role("S5", s5_status,
        {"continuation": {"status": "AVAILABLE" if continuation_rows else "NOT_ESTIMABLE",
                           "rows": continuation_rows},
         "reversal": {"status": "AVAILABLE" if reversal_rows else "NOT_ESTIMABLE",
                      "rows": reversal_rows},
         "crowding_context": {"status": "AVAILABLE" if context_rows else "NOT_ESTIMABLE",
                              "rows": context_rows}},
        (*continuation_refs, *reversal_refs, *context_refs),
        s5_missing)

    # S6 remains an unsized research hypothesis. The existing coordinator
    # enforces its 20-instrument breadth, synchronized history and context.
    decisions: list[S6DecisionV2] = []
    s6_payload: Mapping[str, Any] = {"status": "NOT_ESTIMABLE", "reason": "S6_TYPED_INPUTS_UNAVAILABLE"}
    s6_missing = ["S6_TYPED_INPUTS_UNAVAILABLE"]
    requested_proxies = tuple(s6_btc_proxies) or ((s6_btc_proxy,) if s6_btc_proxy is not None else ())
    if requested_proxies and s6_hourly_bars is not None and s6_four_hour_bars is not None and s6_evidence is not None:
        cohorts = [(key.venue, key.environment, key.product) for key in requested_proxies]
        if len(set(cohorts)) != len(cohorts):
            raise ValueError("S6 requires exactly one BTC proxy per venue/environment/product cohort")
        universe_keys = {entry.key for entry in universe.entries}
        if any(key not in universe_keys or key.native_symbol != "BTCUSDT" for key in requested_proxies):
            raise ValueError("S6 BTC proxies must be exact BTCUSDT identities present in the decision universe")
        coordinator = S6ShadowCoordinator(repository)
        cohort_rows: list[dict[str, Any]] = []
        s6_ref_set: set[str] = set()
        cohort_missing: list[str] = []
        for proxy in sorted(requested_proxies, key=lambda key: key.to_canonical_json()):
            decision = coordinator.evaluate(
                universe=universe, cutoff_ns=cutoff, btc_proxy=proxy,
                hourly_bars=s6_hourly_bars, four_hour_bars=s6_four_hour_bars, evidence=s6_evidence,
                published_at_ns=publication_time(),
            )
            decisions.append(decision)
            s6_ref_set.update(decision.state.envelope.input_refs)
            cohort_rows.append({
                "venue": proxy.venue.value,
                "environment": proxy.environment.value,
                "product": proxy.product.value,
                "btc_proxy": proxy.to_dict(),
                "decision_status": decision.status,
                "reason": decision.reason,
                "state": decision.state.to_dict(),
                "hypotheses": [item.to_dict() for item in decision.hypotheses],
            })
            if decision.status != "AVAILABLE":
                cohort_missing.append(
                    f"S6_{proxy.venue.value}_{proxy.environment.value}_{proxy.product.value}:"
                    f"{decision.reason or 'NOT_ESTIMABLE'}"
                )
            for ref in decision.state.envelope.input_refs:
                if not isinstance(ref, str):
                    raise ValueError("S6 input references must be exact artifact IDs")
                if _source_visible(repository, ref, cutoff):
                    continue
                if ref == universe.content_hash and causal_artifact(repository, ref,
                        cutoff_ns=cutoff, consumer_at_ns=universe.envelope.available_at_ns,
                        deadline_ns=universe.decision_slot_ns):
                    continue
                if _native_history_visible(repository, ref, cutoff_ns=cutoff,
                        published_by_ns=decision.state.envelope.available_at_ns):
                    continue
                derived_entry = repository.get_artifact(ref)
                derived_body = derived_entry.metadata.get("evidence") if derived_entry is not None else None
                if (derived_entry is None or derived_entry.artifact_type != "S6LiquidityFundingEvidenceV2"
                        or derived_entry.available_at_ns > decision.state.envelope.available_at_ns
                        or not isinstance(derived_body, Mapping)):
                    raise ValueError("S6 actual history or liquidity/funding source is missing or unavailable at cutoff")
                liquidity_ref = derived_body.get("liquidity_ref")
                funding_ref = derived_body.get("funding_ref")
                turnover_ref = derived_body.get("turnover_ref")
                if (not isinstance(liquidity_ref, str) or not _source_visible(repository, liquidity_ref, cutoff)
                        or not isinstance(funding_ref, str) or not _source_visible(repository, funding_ref, cutoff)
                        or (turnover_ref is not None and (not isinstance(turnover_ref, str)
                            or not _source_visible(repository, turnover_ref, cutoff)))):
                    raise ValueError("S6 liquidity/funding source is missing or unavailable at cutoff")
                health_body = derived_body.get("source_health")
                if not isinstance(health_body, Mapping):
                    raise ValueError("S6 liquidity/funding source health is missing")
                source_health_ref = PublicSourceHealthV2.from_dict(health_body).content_hash
                if repository.get_artifact(chronology_ref(ref)) is None:
                    record_computation(repository, artifact_ref=ref, information_cutoff_ns=cutoff,
                        started_ns=derived_entry.available_at_ns, finished_ns=derived_entry.available_at_ns,
                        available_ns=derived_entry.available_at_ns, deadline_ns=derived_entry.available_at_ns,
                        input_refs=(liquidity_ref, funding_ref, source_health_ref,
                            *((turnover_ref,) if turnover_ref is not None else ())))
        s6_payload = {"cohorts": cohort_rows, "candidate_action_refs": [], "authority": "ZERO"}
        s6_missing = cohort_missing
    s6_refs = tuple(sorted(s6_ref_set)) if decisions else ()
    s6_available = any(decision.status == "AVAILABLE" for decision in decisions)
    set_role("S6", "AVAILABLE" if s6_available else "NOT_ESTIMABLE", s6_payload, s6_refs, s6_missing)

    # S7 directional reaction is evaluated only from an explicit typed event
    # request. Scheduled-event gates remain upstream deterministic context.
    if len(s7_inputs) > MAX_S7_EVENTS:
        raise ValueError("S7 directional event batch exceeds its fixed bound")
    indexed_population, indexed_events = _indexed_s7_population(repository, cutoff_ns=cutoff)
    if s7_population is not None and s7_population != indexed_population:
        raise ValueError("S7 population evidence differs from indexed source population")
    s7_population = indexed_population
    selected_population_refs = set(s7_population.selected_event_refs)
    if any(row.event.event_id not in selected_population_refs
            or indexed_events.get(row.event.event_id) != row.event for row in s7_inputs):
        raise ValueError("S7 directional inputs are outside the recorded event selection")
    s7_rows: list[dict[str, Any]] = []
    s7_refs: list[str] = []
    for s7_input in (() if s7_population.overflow else sorted(
            s7_inputs, key=lambda row: (row.event.available_at_ns, row.event.event_id, row.key.to_canonical_json()))):
        if service_callback is not None:
            service_callback()
        reaction_artifact: EventReactionArtifactV2 = S7DirectionalShadowV2(repository).evaluate(
            event=s7_input.event, key=s7_input.key, first_bar=s7_input.first_bar, second_bar=s7_input.second_bar,
            history_bars=s7_input.history_bars, beta_to_btc=s7_input.beta_to_btc, btc_return_5m=s7_input.btc_return_5m,
            spread=s7_input.spread, source_health=s7_input.source_health, bar_source_health=s7_input.bar_source_health,
            cutoff_ns=cutoff, resolution=s7_input.resolution,
            published_at_ns=publication_time(),
        )
        ref = reaction_artifact.content_hash
        entry = repository.get_artifact(ref)
        if entry is None:
            raise ValueError("S7 directional shadow did not persist its typed result")
        record_computation(repository, artifact_ref=ref, information_cutoff_ns=cutoff,
            started_ns=entry.available_at_ns, finished_ns=entry.available_at_ns,
            available_ns=entry.available_at_ns, input_refs=reaction_artifact.envelope.input_refs,
            deadline_ns=entry.available_at_ns)
        s7_refs.append(ref)
        s7_rows.append({"event_ref": s7_input.event.event_id, "key": s7_input.key.to_dict(), "artifact_ref": ref,
                        "status": reaction_artifact.status, "reason": reaction_artifact.reason,
                        "exact_action_status": "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT"})
    s7_missing = (("S7_EVENT_POPULATION_OVERFLOW",) if s7_population.overflow else
        () if s7_rows else ("NO_EXPLICIT_AUTHENTICATED_EVENT_REACTION_INPUT",))
    set_role("S7_DIRECTIONAL_SHADOW", "AVAILABLE" if s7_rows else "NOT_ESTIMABLE",
        {"events": s7_rows, "population_selection": s7_population.to_dict(),
         "safety_gate_authority": "SEPARATE_UPSTREAM_DETERMINISTIC_GATE"},
        (*s7_refs, *s7_population.observed_event_refs), s7_missing)

    # Model Arena forecasts are optional explanatory research evidence. An
    # absent route is an explicit NOT_ESTIMABLE; no alternate provider/model
    # is selected and forecasts cannot create strategy candidates.
    model_rows: list[dict[str, Any]] = []
    model_refs: list[str] = []
    if model_forecasts:
        for model_binding in model_forecasts:
            request, forecast = model_binding.request, model_binding.forecast
            model_input_refs = (*request.input_artifact_refs, request.policy_context_ref,
                                request.model_manifest_hash,
                                *(ref for ref in (forecast.values_ref, forecast.samples_ref) if ref))
            if (request.information_cutoff_ns > cutoff or request.input_hash != forecast.input_hash
                    or request.request_id != forecast.request_id
                    or request.model_manifest_hash != forecast.model_manifest_hash
                    or forecast.received_ns > cutoff or forecast.expires_ns < cutoff
                    or not _source_visible(repository, model_binding.forecast_ref, cutoff)
                    or any(not _source_visible(repository, ref, cutoff) for ref in model_input_refs)):
                raise ValueError("Model Arena forecast binding is invalid or unavailable at cutoff")
            model_rows.append({"request_id": request.request_id, "forecast_ref": model_binding.forecast_ref,
                "model_manifest_hash": forecast.model_manifest_hash, "status": forecast.status.value,
                "missing_outputs": list(forecast.missing_outputs), "authority": "ZERO"})
            model_refs.extend((model_binding.forecast_ref, *model_input_refs))
        set_role("MODEL_ARENA_OPTIONAL", "AVAILABLE", {"forecasts": model_rows,
            "fallback": "NONE", "candidate_action_refs": []}, model_refs)
    else:
        set_role("MODEL_ARENA_OPTIONAL", "NOT_ESTIMABLE",
            {"forecasts": [], "reason": "NO_MODEL_ARENA_ROUTE_CONFIGURED", "fallback": "NONE"},
            (), ("NO_MODEL_ARENA_ROUTE_CONFIGURED",))

    # S8 can emit only a two-leg research basket, never CandidateActionV2.
    if len(s8_inputs) > MAX_S8_PAIRS:
        raise ValueError("S8 pair batch exceeds its fixed bound")
    s8_refs: list[str] = []
    s8_rows: list[dict[str, Any]] = []
    mathematical_rows: list[dict[str, Any]] = []
    eligible_keys = {entry.key for entry in universe.entries
        if entry.data_eligible and not entry.capital_eligible}
    for pair_input in sorted(s8_inputs, key=lambda row: row.pair.content_hash):
        if service_callback is not None:
            service_callback()
        pair_entry = repository.get_artifact(pair_input.pair.content_hash)
        if (pair_entry is None or pair_entry.artifact_type != "S8PairDefinitionV2"
                or pair_entry.artifact_ref != pair_input.pair.content_hash
                or pair_entry.content_hash != pair_input.pair.content_hash
                or pair_entry.available_at_ns > cutoff
                or not isinstance(pair_entry.metadata.get("owner_catalog_ref"), str)
                or canonical_json(pair_entry.metadata.get("pair")) != canonical_json(pair_input.pair.to_dict())):
            raise ValueError("S8 pair definition must be registered and available at the market cutoff")
        catalog_entry = repository.get_artifact(str(pair_entry.metadata["owner_catalog_ref"]))
        catalog_body = catalog_entry.metadata.get("catalog") if catalog_entry is not None else None
        if (catalog_entry is None or catalog_entry.artifact_type != "S8PairOwnerCatalogV1"
                or catalog_entry.available_at_ns > cutoff or not isinstance(catalog_body, Mapping)
                or catalog_entry.artifact_ref != catalog_entry.content_hash
                or sha256_json(catalog_body) != catalog_entry.content_hash
                or catalog_body.get("authority") != "RESEARCH_CONFIGURATION_ONLY"
                or catalog_body.get("capital_authority") != "ZERO"
                or catalog_body.get("available_at_ns") != catalog_entry.available_at_ns
                or not isinstance(catalog_body.get("owner_id"), str)
                or not str(catalog_body["owner_id"]).strip()
                or not isinstance(catalog_body.get("pairs"), (tuple, list))
                or canonical_json(pair_input.pair.to_dict()) not in {
                    canonical_json(pair) for pair in catalog_body["pairs"]}):
            raise ValueError("S8 pair owner catalog is missing or unavailable at the market cutoff")
        s8_refs.extend((pair_input.pair.content_hash, catalog_entry.artifact_ref))
        if pair_input.pair.key_a not in eligible_keys or pair_input.pair.key_b not in eligible_keys:
            s8_rows.append({"pair_ref": pair_input.pair.content_hash, "status": "NOT_ESTIMABLE",
                "reason": "S8_BOTH_LEGS_NOT_DATA_ELIGIBLE", "single_action": False, "trade_plan_allowed": False})
            continue
        source_refs = {row.source_ref for row in (*pair_input.prices_a, *pair_input.prices_b)}
        source_refs.update(pair_input.leg_a_evidence.price_refs + pair_input.leg_a_evidence.book_execution_refs
            + pair_input.leg_a_evidence.funding_refs)
        source_refs.update(pair_input.leg_b_evidence.price_refs + pair_input.leg_b_evidence.book_execution_refs
            + pair_input.leg_b_evidence.funding_refs)
        source_refs.update(ref for ref in (pair_input.leg_a_evidence.fee_ref, pair_input.leg_b_evidence.fee_ref) if ref)
        if any(not _s8_source_visible(repository, ref, cutoff, publication_time()) for ref in source_refs):
            raise ValueError("S8 price or both-leg execution evidence is missing or unavailable at cutoff")
        from atlas.v2.math.relative_value import PairPoint, engle_granger, johansen_vecm, kalman_hedge_ratio

        leg_a = {row.hour_end_ns: row for row in pair_input.prices_a}
        leg_b = {row.hour_end_ns: row for row in pair_input.prices_b}
        if len(leg_a) != len(pair_input.prices_a) or len(leg_b) != len(pair_input.prices_b):
            raise ValueError("S8 mathematical diagnostics reject duplicate hourly origins")
        math_points = tuple(PairPoint(at, leg_a[at].available_at_ns, leg_b[at].available_at_ns,
            math.log(float(leg_a[at].close)), math.log(float(leg_b[at].close)),
            leg_a[at].source_ref, leg_b[at].source_ref)
            for at in sorted(set(leg_a) & set(leg_b)) if at < cutoff)[-720:]
        diagnostics = []
        for diagnostic_function in (engle_granger, kalman_hedge_ratio, johansen_vecm):
            diagnostic = diagnostic_function(math_points, cutoff_ns=cutoff, published_at_ns=cutoff)
            # Pure diagnostic calculations use the sealed prefix. Their system
            # publication timestamp is sampled after the calculation completes.
            diagnostic = replace(diagnostic, published_at_ns=publication_time())
            diagnostics.append(diagnostic.to_dict())
        mathematical_rows.append({"pair_ref": pair_input.pair.content_hash,
            "diagnostics": diagnostics, "authority": "ZERO", "selector_influence": "ZERO",
            "capital_authority": "ZERO"})
        s8_refs.extend(source_refs)
        shared_closes = {row.hour_end_ns for row in pair_input.prices_a} & {
            row.hour_end_ns for row in pair_input.prices_b}
        if len(shared_closes) < 721:
            s8_rows.append({"pair_ref": pair_input.pair.content_hash, "status": "NOT_ESTIMABLE",
                "reason": "S8_SYNCHRONIZED_721_HOURLY_BARS_UNAVAILABLE", "single_action": False,
                "trade_plan_allowed": False})
            continue
        if max(shared_closes) != cutoff:
            s8_rows.append({"pair_ref": pair_input.pair.content_hash, "status": "NOT_ESTIMABLE",
                "reason": "S8_HOURLY_HISTORY_DOES_NOT_END_AT_CUTOFF", "single_action": False,
                "trade_plan_allowed": False})
            continue
        try:
            basket_forecast = build_research_basket_forecast(pair_input.pair, prices_a=pair_input.prices_a,
                prices_b=pair_input.prices_b, cutoff_ns=cutoff, leg_a_evidence=pair_input.leg_a_evidence,
                leg_b_evidence=pair_input.leg_b_evidence)
        except ValueError:
            s8_rows.append({"pair_ref": pair_input.pair.content_hash, "status": "NOT_ESTIMABLE",
                "reason": "S8_SYNCHRONIZED_HEDGE_FIT_NOT_ESTIMABLE", "single_action": False,
                "trade_plan_allowed": False})
            continue
        if basket_forecast.information_cutoff_ns != cutoff or not basket_forecast.beta_frozen:
            raise ValueError("S8 forecast cutoff/beta-freeze contract failed")
        from .research_basket_outcomes import diagnose_s8_residuals

        diagnostic_available = publication_time()
        try:
            residual_diagnostics = diagnose_s8_residuals(
                pair_input.pair,
                basket_forecast,
                prices_a=pair_input.prices_a,
                prices_b=pair_input.prices_b,
                published_at_ns=diagnostic_available,
            )
        except ValueError:
            s8_rows.append({"pair_ref": pair_input.pair.content_hash,
                "status": "NOT_ESTIMABLE", "reason": "S8_RESIDUAL_DIAGNOSTICS_NOT_ESTIMABLE",
                "single_action": False, "trade_plan_allowed": False})
            continue
        residual_ref = residual_diagnostics.content_hash
        repository.register_artifact(ArtifactIndexEntryV2(
            residual_ref, "S8ResidualDiagnosticsV1", residual_ref,
            diagnostic_available, diagnostic_available,
            {"diagnostics": residual_diagnostics.to_dict()},
        ))
        record_computation(repository, artifact_ref=residual_ref,
            information_cutoff_ns=cutoff, started_ns=diagnostic_available,
            finished_ns=diagnostic_available, available_ns=diagnostic_available,
            input_refs=(*source_refs, pair_input.pair.content_hash, basket_forecast.content_hash),
            deadline_ns=diagnostic_available)
        if mathematical_rows and mathematical_rows[-1].get("pair_ref") == pair_input.pair.content_hash:
            mathematical_rows[-1]["residual_diagnostics_ref"] = residual_ref
            mathematical_rows[-1]["residual_diagnostics"] = residual_diagnostics.to_dict()
        s8_refs.append(residual_ref)
        publish_at = publication_time()
        ref = persist_s8_basket(repository, pair_input.pair, basket_forecast, available_at_ns=publish_at)
        record_computation(repository, artifact_ref=ref, information_cutoff_ns=cutoff,
            started_ns=publish_at, finished_ns=publish_at, available_ns=publish_at,
            input_refs=(*source_refs, pair_input.pair.content_hash, catalog_entry.artifact_ref),
            deadline_ns=publish_at)
        s8_refs.append(ref)
        s8_rows.append({"pair_ref": pair_input.pair.content_hash, "forecast_ref": ref,
                        "status": basket_forecast.economic_status, "single_action": False,
                        "trade_plan_allowed": False})
    s8_payload: dict[str, Any] = {"baskets": s8_rows, "mathematical_diagnostics": mathematical_rows,
        "candidate_action_refs": [], "trade_plan_allowed": False}
    set_role("S8_RESEARCH_BASKET", "AVAILABLE" if s8_rows else "NOT_ESTIMABLE",
        s8_payload,
        s8_refs, () if s8_rows else ("S8_PAIR_INPUTS_UNAVAILABLE",))

    published_at = publication_time()
    for role, (status, payload, input_refs, missing) in role_payloads.items():
        role_ref = _persist_role(repository, role=role, status=status, cutoff_ns=cutoff,
            published_at_ns=published_at, payload=payload, input_refs=input_refs, missing_reasons=missing)
        refs_by_role[role] = (role_ref,)
    body = {
        "version": SURFACE_VERSION,
        "cutoff_ns": cutoff,
        "published_at_ns": published_at,
        "universe_ref": universe.content_hash,
        "original_universe_receipt_ref": (chronology_ref(universe.content_hash)
            if universe.envelope.available_at_ns > cutoff else None),
        "original_decision_deadline_ns": universe.decision_slot_ns,
        "selector_influence": "ZERO",
        "role_status": statuses,
        "role_refs": {name: list(refs) for name, refs in refs_by_role.items()},
        "missing_reasons": {name: list(value) for name, value in reasons.items()},
        "authority": "ZERO", "capital_authority": "ZERO", "candidate_action_refs": [], "trade_plan_allowed": False,
    }
    surface_ref = sha256_json(body)
    repository.register_artifact(ArtifactIndexEntryV2(surface_ref, SURFACE_VERSION, surface_ref,
        published_at, published_at, {"surface": body}))
    return FullStrategySurfaceResultV1(surface_ref, cutoff, published_at, universe.content_hash,
        dict(statuses), dict(refs_by_role), dict(reasons),
        chronology_ref(universe.content_hash) if universe.envelope.available_at_ns > cutoff else None,
        universe.decision_slot_ns)
