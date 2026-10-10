"""ATLAS-owned composition of the existing non-capital V2 research APIs.

This module owns the ordering between source recovery, causal evidence, sleeve
selection, hard-risk sizing, action freezing, economic evaluation and the
decision calendar. It owns no second repository and implements no market,
selector, risk or economics rules of its own.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal
from functools import partial
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol, cast

from atlas.domain.risk import RiskPolicy

from .._serialization import canonical_json, json_value, sha256_json, sha256_ref, timestamp
from ..chronology import causal_artifact, record_computation, sample
from ..contracts import CandidateActionV2, CandidateSetV2, PolicySpecV2, V2Side
from ..data.bars import BarIntervalV2, CausalBarStoreV2
from ..data.binance import translate_agg_trades
from ..data.bybit import translate_recent_trades
from ..data.collector import MAX_RECONCILIATION_REFS_PER_PAGE_V1, PublicCollectorV2
from ..data.health import PublicSourceHealthV2, PublicSourceStateV2
from ..data.history import (
    ParquetObservationArchiveV2,
    reconstruct_causal_bars_from_archive,
    reconstruct_native_bars_from_index_page,
    reconstruct_native_m1_bars_from_index_page,
    reconstruct_public_observations_from_archive,
)
from ..data.microstructure import (
    L2DeltaV2,
    L2SequenceFaultV2,
    L2SnapshotV2,
    SequenceValidBookV2,
)
from ..data.microstructure_archive import L2FrameArchiveV2, L2RawFrameV2
from ..data.public_archive_extents import write_extent
from ..data.public_evidence_checkpoint import (
    MAX_CHUNKS_PER_CHECKPOINT,
    persist_book_checkpoint,
    persist_continuity_checkpoint,
    validate_continuity_checkpoint,
)
from ..data.public_http import PublicDataError
from ..data.public_microstructure_ws import (
    CapturedPublicFrameV2,
    bybit_btc_eth_linear_topics,
    parse_bybit_orderbook_frame,
    parse_bybit_trades,
    raw_archive_record,
)
from ..data.public_stream_continuity import (
    PublicStreamContinuityStateV1,
    PublicStreamContinuityTrackerV1,
    PublicStreamObservationKindV1,
    PublicStreamObservationV1,
    build_public_stream_continuity_report,
)
from ..data.public_stream_source import PublicStreamSourceV2
from ..data.raw import AvailabilityClassV2, RawObservationV2
from ..data.s3_forward_evidence import (
    S3ForwardTradeEvidenceV1,
    S3NativeComputationContextV1,
    S3QuoteBridgeResultV1,
    evaluate_s3_warmup_readiness,
    quote_from_valid_continuity_report,
    reconstruct_s3_stream_trade_evidence,
)
from ..data.subscriptions import SubscriptionPlanV2
from ..data.universe import ComputeTierV2, DynamicUniverseRuntimeV2, UniverseObservationV2
from ..features.candles import DAY_NS, CausalTradeAnchor, TradeLocationConfig
from ..features.joins import asof_join
from ..features.pipeline import feature_snapshot
from ..features.structure import confirmed_swings
from ..instruments import InstrumentKeyV2, InstrumentRegistryV2, ProductContractV2, UniverseContractV2, VenueV2
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository
from ..risk import (
    AccountRiskSnapshotV2,
    ClosedV2Outcome,
    ExposureKind,
    FeeScheduleV2,
    OutcomeClass,
    PossibleRiskV2,
    RiskPolicyV2,
    SizingDecisionV2,
    SizingStatus,
    StressBoundV2,
    VenueSizingLimitsV2,
    size_selected_candidate,
)
from ..science.action import ActionArtifactV2, freeze_action
from ..science.admission import (
    AdmissionPolicyV2,
    AmendedEvaluationArtifactV2,
    VenueCapabilitySnapshotV2,
    index_amended_evaluation,
)
from ..science.analogue import not_estimable_analogue, persist_analogue
from ..science.evaluation_service import Phase2EvaluationResultV2, run_phase2_economic_evaluation
from ..science.m1 import M1RunV2, fit_m1
from ..science.m15_origin_accounting import (
    M15OpportunityMissingnessV1,
    M15OriginAccountingAction,
    M15OriginAccountingCheckpointV1,
    M15OriginAccountingRecordV1,
    advance_m15_origin_checkpoint,
    m15_origin_ref,
    plan_m15_origin_accounting,
)
from ..science.outcomes import (
    AdmissionStateV2,
    DecisionCalendarEntryV2,
    DecisionSourceStageV2,
    SelectionStateV2,
    index_decision_calendar_entry,
)
from ..science.pretrade import CausalInputV2
from ..science.research_selection import (
    MULTI_SLEEVE_SELECTION_BODY,
    MULTI_SLEEVE_SELECTION_HASH,
    assemble_multisleeve_research_candidate_set,
    persist_research_sleeve_audit,
    research_selection_universe,
)
from ..science.s3_calendar import (
    ensure_native_s3_research_universe,
    persist_native_s3_not_estimable_calendar,
    persist_native_s3_not_estimable_candidate_set,
    persist_s3_late_origin_missingness,
)
from ..selection import (
    SELECTION_POLICY_HASH,
    ScannerRankEvidenceV1,
    ScannerSelectionSourceV1,
    accept_research_candidates,
    register_scanner_rank,
    register_scanner_source,
)
from ..strategies.s1_trend import (
    S1_POLICY,
    EventGate,
    EventState,
    ExecutableQuote,
    MarkIndexEvidence,
    S1ShadowCoordinator,
)
from ..strategies.s2_breakout import S2_POLICY, S2ShadowCoordinator
from ..strategies.s3_mean_reversion import (
    S3_POLICY,
    CausalTradeV2,
    ResidualObservationV2,
    S3ShadowCoordinator,
    TradeVwapSnapshotV2,
    persist_trade_vwap_v2,
    residual_observation,
    utc_day_trade_vwap,
)
from ..strategies.s6_cross_section import (
    S6_ACTION_POLICY,
    S6_POLICY,
    S6HypothesisV2,
    S6ShadowCoordinator,
    build_s6_candidate_action,
)
from .active_history import ActiveHistoryPageV1, maintain_history
from .analogue_diagnostic import run_analogue_diagnostic_v1
from .ops_supervisor import (
    PIPELINE_STAGE_ORDER,
    OpsCycleBatchV1,
    OpsDecisionEventV1,
    OpsDecisionResultV1,
    OpsRecoverySnapshotV1,
    OpsSourceStateV1,
    OpsStageResultV1,
    OpsStageStatusV1,
    OpsTerminalStatusV1,
    PipelineStageV1,
)
from .s3_native_cadence import (
    MAX_M1_ORIGINS_ACCOUNTED_PER_CYCLE,
    S3_M1_ACCOUNTING_CHECKPOINT_TYPE,
    S3_M1_DEFAULT_MAX_LATENESS_NS,
    S3_M1_EVENT_TYPE,
    S3M1BarDisposition,
    S3M1OriginAccountingAction,
    S3NativeM1OriginAccountingCheckpointV1,
    advance_s3_m1_origin_checkpoint,
    classify_s3_m1_bar,
    find_s3_m1_origin_accounting_state,
    plan_s3_m1_origin_accounting,
    s3_m1_event_id,
    s3_m1_origin_metadata,
    s3_m1_origin_ref,
)
from .trade_location_inputs import load_trade_location_inputs, persist_trade_location_input_receipt

OPS_PRODUCTION_ADAPTER_ID = "ATLAS_V2_PRODUCTION_OPS_COMPOSITION_V1"
OPS_RUNTIME_DECISION_ARTIFACT_TYPE = "OpsRuntimeDecisionEvidenceV1"
_FEATURE_CONTEXT_BARS_V1 = {"M15": 192, "H1": 100, "H4": 100}
BYBIT_PUBLIC_WS_SOURCE_ID_V1 = "BYBIT_PUBLIC_WS"
PUBLIC_STREAM_STALE_NS_V1 = 30_000_000_000
PUBLIC_STREAM_METADATA_MAX_AGE_NS_V1 = 3_600_000_000_000
PUBLIC_STREAM_MAX_FRAMES_PER_CYCLE_V1 = 32
PUBLIC_STREAM_MAX_FRAMES_PER_SERVICE_V1 = 256
BROAD_PUBLIC_ADOPTION_ARCHIVE_PAGE_ROWS_V1 = 64
BROAD_PUBLIC_PRODUCT_REGISTRATION_PAGE_ROWS_V1 = 64
BROAD_PUBLIC_ADOPTION_RECONCILIATION_PAGE_ROWS_V1 = 32
BROAD_PUBLIC_ADOPTION_STREAM_SERVICE_INTERVAL_NS_V1 = 250_000_000
PUBLIC_STREAM_MAX_TRADES_PER_FRAME_V1 = 256
_POLICIES: Mapping[str, PolicySpecV2] = MappingProxyType(
    {policy.policy_hash: policy for policy in (S1_POLICY, S2_POLICY, S3_POLICY, S6_ACTION_POLICY)}
)


@dataclass(frozen=True)
class ProductionRiskInputsV1:
    """Exact existing hard-risk inputs for one selected candidate.

    Missing fields remain ``None`` and stop the pipeline before sizing. This
    type is a transport for real evidence, not a source of defaults.
    """

    product: ProductContractV2 | None
    risk_policy_v1: RiskPolicy | None
    risk_policy_v2: RiskPolicyV2 | None
    account: AccountRiskSnapshotV2 | None
    exposures: tuple[PossibleRiskV2, ...] | None
    outcomes: tuple[ClosedV2Outcome, ...] | None
    venue: VenueSizingLimitsV2 | None
    stress: StressBoundV2 | None
    fee: FeeScheduleV2 | None

    @property
    def complete(self) -> bool:
        return all(value is not None for value in (
            self.product,
            self.risk_policy_v1,
            self.risk_policy_v2,
            self.account,
            self.exposures,
            self.outcomes,
            self.venue,
            self.stress,
            self.fee,
        ))


@dataclass(frozen=True)
class ProductionEconomicInputsV1:
    """Existing Session-019 evaluation inputs, supplied without inference."""

    admission_policy: AdmissionPolicyV2 | None
    capability: VenueCapabilitySnapshotV2 | None
    model_input: CausalInputV2 | None
    calibration_input: CausalInputV2 | None
    execution_model_input: CausalInputV2 | None
    available_at_ns: int | None
    scenario_seed: int | None
    scenario_count: int = 100
    source_inputs: tuple[CausalInputV2, ...] = ()
    joint_data_refs: tuple[str, ...] = ()
    support_unit_refs: tuple[str, ...] = ()
    execution_residual_refs: tuple[str, ...] = ()
    stress_input_ref: str | None = None
    existing_portfolio_path_refs: tuple[str, ...] = ()

    @property
    def complete(self) -> bool:
        return all(value is not None for value in (
            self.admission_policy,
            self.capability,
            self.model_input,
            self.calibration_input,
            self.execution_model_input,
            self.available_at_ns,
            self.scenario_seed,
        ))


@dataclass(frozen=True)
class ProductionEventInputsV1:
    """Causal universe and exact unsized sleeve outputs for one event."""

    # Native M1 S3 warmup produces diagnostic evidence only while public-trade
    # completeness is unproven. It intentionally has no decision universe.
    universe: UniverseContractV2 | None
    candidates: tuple[CandidateActionV2, ...]
    scanner_evidence_refs: Mapping[str, tuple[str, ...]]
    risk_inputs: Mapping[str, ProductionRiskInputsV1]
    economic_inputs: Mapping[str, ProductionEconomicInputsV1]
    causal_feature_refs: tuple[str, ...] = ()
    causal_source_refs: tuple[str, ...] = ()
    generation_missing_reasons: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        candidates = tuple(self.candidates)
        if any(not isinstance(item, CandidateActionV2) or item.policy_hash not in _POLICIES for item in candidates):
            raise ValueError("production event inputs may contain only declared exact S1-S3/S6 shadow CandidateActionV2 outputs")
        if any(item.quantity is not None for item in candidates):
            raise ValueError("production sleeve outputs must remain unsized until the existing hard-risk API")
        ids = {item.candidate_id for item in candidates}
        if len(ids) != len(candidates) or set(self.scanner_evidence_refs) - ids:
            raise ValueError("production scanner evidence must bind unique present candidates")
        features = tuple(sorted(set(self.causal_feature_refs)))
        for ref in features:
            sha256_ref(ref, field="causal feature reference")
        source_refs = tuple(sorted(set(self.causal_source_refs)))
        for ref in source_refs:
            sha256_ref(ref, field="causal source reference")
        object.__setattr__(self, "candidates", candidates)
        object.__setattr__(self, "scanner_evidence_refs", MappingProxyType({
            str(key): tuple(value) for key, value in self.scanner_evidence_refs.items()
        }))
        object.__setattr__(self, "risk_inputs", MappingProxyType(dict(self.risk_inputs)))
        object.__setattr__(self, "economic_inputs", MappingProxyType(dict(self.economic_inputs)))
        object.__setattr__(self, "causal_feature_refs", features)
        object.__setattr__(self, "causal_source_refs", source_refs)
        reasons = tuple(sorted(set(self.generation_missing_reasons)))
        if len(reasons) > 32 or any(not isinstance(reason, str) or not 1 <= len(reason) <= 192 for reason in reasons):
            raise ValueError("production generation missingness requires bounded explicit reasons")
        object.__setattr__(self, "generation_missing_reasons", reasons)


class ProductionEventInputsProviderV1(Protocol):
    """Resolve previously collected, typed causal outputs for an immutable event."""

    def resolve(self, repository: OpsRepository, event: OpsDecisionEventV1) -> ProductionEventInputsV1 | None: ...


class ProductionPublicCycleSourceV1(Protocol):
    """Public-only cycle input source; it cannot own or open an OpsRepository."""

    @property
    def required_source_ids(self) -> tuple[str, ...]: ...

    def collect(
        self,
        repository: OpsRepository,
        collector: PublicCollectorV2,
        *,
        now_ns: int,
        recovery: OpsRecoverySnapshotV1,
    ) -> OpsCycleBatchV1: ...


def _source_id_for_product(product: ProductContractV2) -> str:
    return {VenueV2.BYBIT: "BYBIT_PUBLIC_V2", VenueV2.BINANCE: "BINANCE_USDM_PUBLIC_V2"}[product.key.venue]


class IndexedPublicCycleSourceV1:
    """Build deterministic decision handoffs from reconciled archived final bars."""

    def __init__(self, *, clock_ns: Callable[[], int] = time.time_ns,
                 minimum_m15_origin_close_at_ns: int = 0,
                 scope_public_sources: bool = False) -> None:
        self.clock_ns = clock_ns
        self.scope_public_sources = scope_public_sources
        self.stream_service: Callable[[OpsRepository], None] | None = None
        self.minimum_m15_origin_close_at_ns = timestamp(minimum_m15_origin_close_at_ns,
            field="minimum_m15_origin_close_at_ns")

    def _account_m15_origins(self, repository: OpsRepository, collector: PublicCollectorV2,
                            *, now_ns: int, allow_events: bool = True) -> tuple[OpsDecisionEventV1, ...]:
        events: list[OpsDecisionEventV1] = []
        for product in collector.registry.contracts():
            latest = repository.latest_m15_origin_accounting_checkpoint(product.key)
            previous = (M15OriginAccountingCheckpointV1.from_dict(latest.metadata["checkpoint"])
                        if latest else None)
            if previous is not None and previous.observed_at_ns > now_ns:
                continue
            if previous is None:
                source_from, source_through, after_close = 0, now_ns, None
            elif previous.scan_complete:
                if now_ns <= previous.source_available_through_ns:
                    continue
                source_from, source_through, after_close = previous.source_available_through_ns, now_ns, None
            else:
                source_from, source_through = previous.source_available_from_ns, previous.source_available_through_ns
                after_close = previous.source_scan_after_close_at_ns
            page = repository.m15_origin_observation_page(product.key, available_from_ns=source_from,
                available_through_ns=source_through, after_close_at_ns=after_close,
                min_close_at_ns=self.minimum_m15_origin_close_at_ns, limit=4)
            if (previous is not None and not previous.scan_complete and not page.entries
                    and page.has_more and page.last_close_at_ns == previous.source_scan_after_close_at_ns):
                # Source discovery advanced its own durable locator, but no
                # origin was returned. Preserve the frozen accounting cursor.
                continue
            bars = reconstruct_native_bars_from_index_page(repository,
                Path(repository.path).parent / "ops-observations", key=product.key,
                interval=BarIntervalV2.M15, index_entries=page.entries, max_origins=4)
            existing: list[ArtifactIndexEntryV2] = []
            health: dict[str, PublicSourceHealthV2] = {}
            for item in bars:
                matched = repository.artifact_entries_by_metadata_identity(M15OriginAccountingRecordV1.VERSION,
                    ("m15_origin_ref",), m15_origin_ref(product.key, item.bar.close_at_ns),
                    as_of_ns=now_ns, limit=2)
                if matched.has_more:
                    raise ValueError("M15 origin accounting identity overflow")
                existing.extend(matched.entries)
                state = repository.latest_source_health_at(item.bar.raw.source_id,
                    as_of_ns=item.bar.raw.available_at_ns)
                indexed_health = repository.get_artifact(state.details_ref) if state and state.details_ref else None
                if indexed_health is not None:
                    parsed = PublicSourceHealthV2.from_dict(indexed_health.metadata["health"])
                    if (parsed.content_hash != indexed_health.content_hash
                            or parsed.content_hash != indexed_health.artifact_ref
                            or parsed.available_at_ns != indexed_health.available_at_ns):
                        raise ValueError("M15 source health identity conflicts")
                    health[parsed.content_hash] = parsed
            observed = max(now_ns, timestamp(self.clock_ns(), field="M15 origin accounting observation"))
            plans = plan_m15_origin_accounting(tuple(item.bar for item in bars), product.key,
                now_ns=observed, observation_entries=page.entries, health_entries=tuple(health.values()),
                product=product, existing_entries=tuple(existing), after_close_at_ns=after_close, limit=4)
            for plan in plans:
                if plan.action == M15OriginAccountingAction.REUSE_DURABLE_ORIGIN:
                    continue
                event: OpsDecisionEventV1 | None = None
                missingness = plan.missingness
                if plan.action == M15OriginAccountingAction.ATTEMPT_TIMELY_EVENT and not allow_events:
                    missingness = M15OpportunityMissingnessV1(
                        product.key, plan.bar.close_at_ns, plan.origin_ref, plan.bar.content_hash,
                        plan.observation_index_ref, plan.bar.raw.received_at_ns,
                        plan.bar.raw.available_at_ns, observed, plan.bar.close_at_ns + 5_000_000_000,
                        "M15_SOURCE_HEALTH_NOT_CURRENT",
                    )
                elif plan.action == M15OriginAccountingAction.ATTEMPT_TIMELY_EVENT:
                    base = _public_bar_event(repository, product, plan.bar, now_ns=now_ns)
                    generated = (IndexedProductionEventInputsV1(clock_ns=self.clock_ns,
                        stream_service=self.stream_service).resolve(repository, base)
                                 if base else None)
                    if base is not None and generated is not None and generated.universe is not None:
                        event = base
                        _persist_public_event(repository, event, plan.bar.raw.record_id, now_ns)
                        events.append(event)
                    else:
                        missingness = M15OpportunityMissingnessV1(product.key, plan.bar.close_at_ns,
                            plan.origin_ref, plan.bar.content_hash, plan.observation_index_ref,
                            plan.bar.raw.received_at_ns, plan.bar.raw.available_at_ns, observed,
                            plan.bar.close_at_ns + 5_000_000_000, "M15_EVENT_PREREQUISITE_UNAVAILABLE")
                if missingness is not None:
                    repository.register_artifact(ArtifactIndexEntryV2(missingness.content_hash, missingness.VERSION,
                        missingness.content_hash, observed, observed, {"missingness": missingness.to_dict(),
                        "m15_origin_ref": plan.origin_ref}))
                    accounting_ref, kind = missingness.content_hash, missingness.VERSION
                elif event is not None:
                    accounting_ref, kind = event.content_hash, "OpsDecisionEventSourceV1"
                else:
                    raise ValueError("M15 origin has no durable terminal accounting")
                record = M15OriginAccountingRecordV1(product.key, plan.bar.close_at_ns, plan.origin_ref,
                    plan.bar.content_hash, plan.observation_index_ref, accounting_ref, kind, observed)
                repository.register_artifact(ArtifactIndexEntryV2(record.content_hash, record.VERSION,
                    record.content_hash, observed, observed,
                    {"accounting": record.to_dict(), "m15_origin_ref": plan.origin_ref}))
            checkpoint = advance_m15_origin_checkpoint(previous, product.key, now_ns=observed,
                source_available_through_ns=source_through,
                next_close_cursor_ns=page.last_close_at_ns if page.has_more else None, has_more=page.has_more)
            repository.register_artifact(ArtifactIndexEntryV2(checkpoint.content_hash, checkpoint.VERSION,
                checkpoint.content_hash, observed, observed,
                {"checkpoint": checkpoint.to_dict(), "instrument_key_json": product.key.to_canonical_json()}))
        return tuple(events)

    @property
    def required_source_ids(self) -> tuple[str, ...]:
        return ()

    def collect(
        self,
        repository: OpsRepository,
        collector: PublicCollectorV2,
        *,
        now_ns: int,
        recovery: OpsRecoverySnapshotV1,
        allow_events: bool = True,
    ) -> OpsCycleBatchV1:
        del recovery
        archive_root = Path(repository.path).parent / "ops-observations"
        source_ids = set(self.required_source_ids) | set(repository.source_health_sources())
        source_ids.update(repository.public_reconciliation_source_ids())
        source_ids.update(repository.public_observation_source_ids())
        recovery_epoch_ref = _current_recovery_epoch_ref(repository, now_ns=now_ns)
        _reconcile_public_sources(
            repository, collector, tuple(sorted(source_ids)), now_ns=now_ns,
            recovery_epoch_ref=recovery_epoch_ref,
        )

        events: list[OpsDecisionEventV1] = []
        refs: list[str] = []
        pending_events = repository.pending_decision_event_page(as_of_ns=now_ns, limit=64)
        if pending_events.invalid_entry_count:
            raise ValueError("pending decision event page contains corrupt source handoffs")
        for entry in pending_events.entries:
            body = entry.metadata.get("event")
            if entry.available_at_ns > now_ns or not isinstance(body, Mapping):
                continue
            event = decision_event_from_dict(body)
            if (event.available_at_ns <= now_ns
                    and repository.get_artifact(_ops_receipt_identity_ref(event.event_id)) is None):
                events.append(event)
                refs.append(entry.artifact_ref)
        seen_event_ids = {item.event_id for item in events}

        health_states = {
            source_id: max(
                (item for item in collector.health.history(source_id) if item.available_at_ns <= now_ns),
                key=lambda item: (item.available_at_ns, item.content_hash),
                default=None,
            )
            for source_id in sorted(source_ids)
        }
        healthy = bool(health_states)
        for source_id, state in health_states.items():
            history = [
                item for item in collector.health.history(source_id)
                if item.available_at_ns <= now_ns and item.observed_at_ns <= now_ns
            ]
            latest_observed = max((item.observed_at_ns for item in history), default=None)
            if (state is None or latest_observed is None
                    or any(not item.data_eligible for item in history
                           if item.observed_at_ns == latest_observed)):
                healthy = False
        def source_current(source_id: str) -> bool:
            state = health_states.get(source_id)
            return state is not None and state.data_eligible

        if not healthy and not self.scope_public_sources:
            # Durable events remain queued until this recovery epoch has exact
            # source-health reconciliation evidence for every required source.
            events.clear()
            refs.clear()
            seen_event_ids.clear()
        events_allowed = allow_events and (healthy or (
            self.scope_public_sources and any(source_current(source) for source in source_ids)))
        m15_events = self._account_m15_origins(repository, collector, now_ns=now_ns, allow_events=events_allowed)
        if events_allowed:
            for event in m15_events:
                if (event.event_id not in seen_event_ids
                        and repository.get_artifact(_ops_receipt_identity_ref(event.event_id)) is None):
                    events.append(event)
                    refs.append(event.content_hash)
                    seen_event_ids.add(event.event_id)
            # Keep the accepted indexed handoff as a compatibility path for
            # already-indexed bars whose M15 origin page was sealed before the
            # new accounting checkpoint existed. The old path is bounded to
            # the newest causal bar and shares the same immutable event writer.
            for product in collector.registry.contracts():
                if self.scope_public_sources and not source_current(_source_id_for_product(product)):
                    continue
                bars = reconstruct_causal_bars_from_archive(
                    repository, archive_root, key=product.key, interval=BarIntervalV2.M15,
                    information_cutoff_ns=now_ns, availability_class=AvailabilityClassV2.ACTUAL_SYSTEM,
                    limit=128,
                )
                if not bars:
                    continue
                trigger = bars[-1].bar
                if now_ns - trigger.close_at_ns > 5_000_000_000:
                    continue
                existing_page = repository.latest_artifact_entries("OpsDecisionEventSourceV1", as_of_ns=now_ns,
                    metadata_path=("trigger_record_id",), identity_value=trigger.raw.record_id, limit=2)
                if existing_page.has_more or existing_page.invalid_entry_count or len(existing_page.entries) > 1:
                    raise ValueError("public event trigger identity ambiguous")
                existing = existing_page.entries[0] if existing_page.entries else None
                if existing is not None:
                    body = existing.metadata.get("event")
                    if isinstance(body, Mapping):
                        event = decision_event_from_dict(body)
                        if (event.event_id not in seen_event_ids
                                and repository.get_artifact(_ops_receipt_identity_ref(event.event_id)) is None):
                            events.append(event)
                            refs.append(existing.artifact_ref)
                            seen_event_ids.add(event.event_id)
                    continue
                base = _public_bar_event(repository, product, trigger, now_ns=now_ns)
                generated = (IndexedProductionEventInputsV1(clock_ns=self.clock_ns,
                    stream_service=self.stream_service).resolve(repository, base)
                             if base else None)
                if generated is None or generated.universe is None or base is None:
                    continue
                event = base
                _persist_public_event(repository, event, trigger.raw.record_id, now_ns)
                if (event.event_id not in seen_event_ids
                        and repository.get_artifact(_ops_receipt_identity_ref(event.event_id)) is None):
                    events.append(event)
                    refs.append(event.content_hash)
                    seen_event_ids.add(event.event_id)

        # Account received native origins even while current source health is
        # degraded. Timely decisions remain queued; missing/late cases persist.
        for product in collector.registry.contracts():
            previous = _load_latest_s3_m1_origin_checkpoint(repository, product.key)
            if previous is None:
                available_from_ns, available_through_ns, after_close_at_ns = 0, now_ns, None
            elif previous.scan_complete:
                if now_ns <= previous.source_available_through_ns:
                    continue
                available_from_ns = previous.source_available_through_ns
                available_through_ns, after_close_at_ns = now_ns, None
            else:
                available_from_ns = previous.source_available_from_ns
                available_through_ns = previous.source_available_through_ns
                after_close_at_ns = previous.source_scan_after_close_at_ns

            page = repository.native_m1_origin_observation_page(
                product.key,
                available_from_ns=available_from_ns,
                available_through_ns=available_through_ns,
                after_close_at_ns=after_close_at_ns,
                limit=MAX_M1_ORIGINS_ACCOUNTED_PER_CYCLE,
            )
            if (previous is not None and not previous.scan_complete and not page.entries
                    and page.has_more and page.last_close_at_ns == previous.source_scan_after_close_at_ns):
                continue
            page_bars = reconstruct_native_m1_bars_from_index_page(
                repository, archive_root, key=product.key,
                index_entries=page.entries,
                max_origins=MAX_M1_ORIGINS_ACCOUNTED_PER_CYCLE,
            )
            page_events: dict[str, ArtifactIndexEntryV2] = {}
            page_gates: dict[str, ArtifactIndexEntryV2] = {}
            for indexed_bar in page_bars:
                close_at_ns = indexed_bar.bar.close_at_ns
                event_entries, gate_entries = _native_m1_origin_state_entries(
                    repository, product.key, close_at_ns, as_of_ns=now_ns,
                )
                page_events.update((entry.artifact_ref, entry) for entry in event_entries)
                page_gates.update((entry.artifact_ref, entry) for entry in gate_entries)
            plans = plan_s3_m1_origin_accounting(
                (item.bar for item in page_bars), product.key,
                now_ns=now_ns,
                event_entries=tuple(page_events.values()),
                gate_entries=tuple(page_gates.values()),
                after_close_at_ns=after_close_at_ns,
                max_origins=MAX_M1_ORIGINS_ACCOUNTED_PER_CYCLE,
            )

            accounted_close_at_ns: int | None = None
            bars_by_close = {item.bar.close_at_ns: item.bar for item in page_bars}
            for plan in plans:
                bar = bars_by_close[plan.close_at_ns]
                if plan.action == S3M1OriginAccountingAction.CREATE_LATE_TEST_GATE:
                    _persist_late_s3_m1_origin_gate(
                        repository, product, bar, observed_at_ns=now_ns,
                    )
                elif (plan.action == S3M1OriginAccountingAction.CREATE_TIMELY_EVENT
                      and (not events_allowed or (self.scope_public_sources
                           and not source_current(_source_id_for_product(product))))):
                    _persist_late_s3_m1_origin_gate(
                        repository, product, bar, observed_at_ns=now_ns,
                        reason_code="NATIVE_M1_SOURCE_HEALTH_NOT_CURRENT",
                    )
                elif plan.action == S3M1OriginAccountingAction.CREATE_TIMELY_EVENT:
                    native_event = _native_s3_m1_public_bar_event(
                        repository, product, bar, now_ns=now_ns,
                    )
                    if native_event is None:
                        _persist_late_s3_m1_origin_gate(
                            repository, product, bar, observed_at_ns=now_ns,
                            reason_code="NATIVE_M1_ORIGIN_EVENT_PREREQUISITE_UNAVAILABLE",
                        )
                    else:
                        native_origin = s3_m1_origin_metadata(
                            product.key, bar.close_at_ns, bar_ref=bar.content_hash,
                        )["native_m1_origin"]
                        if not isinstance(native_origin, Mapping):
                            raise ValueError("native M1 origin metadata must be an object")
                        _persist_public_event(
                            repository, native_event, bar.raw.record_id, now_ns,
                            native_m1_origin=native_origin,
                        )
                        event_entry = repository.get_artifact(native_event.content_hash)
                        if (event_entry is not None and native_event.event_id not in seen_event_ids
                                and repository.get_artifact(
                                    _ops_receipt_identity_ref(native_event.event_id),
                                ) is None):
                            events.append(native_event)
                            refs.append(event_entry.artifact_ref)
                            seen_event_ids.add(native_event.event_id)
                elif plan.action == S3M1OriginAccountingAction.REUSE_TIMELY_EVENT:
                    event_entries, _ = _native_m1_origin_state_entries(
                        repository, product.key, plan.close_at_ns, as_of_ns=now_ns,
                    )
                    matching = [entry for entry in event_entries
                                if entry.artifact_ref == plan.existing_accounting_ref]
                    if len(matching) != 1:
                        raise ValueError("native M1 reused event disappeared or conflicts")
                    body = matching[0].metadata.get("event")
                    if not isinstance(body, Mapping):
                        raise ValueError("native M1 reused event lost its typed event body")
                    event = decision_event_from_dict(body)
                    if (event.available_at_ns <= now_ns and event.event_id not in seen_event_ids
                            and repository.get_artifact(_ops_receipt_identity_ref(event.event_id)) is None):
                        events.append(event)
                        refs.append(matching[0].artifact_ref)
                        seen_event_ids.add(event.event_id)
                elif plan.action == S3M1OriginAccountingAction.REUSE_LATE_TEST_GATE:
                    event_entries, gate_entries = _native_m1_origin_state_entries(
                        repository, product.key, plan.close_at_ns, as_of_ns=now_ns,
                    )
                    durable_gate = find_s3_m1_origin_accounting_state(
                        event_entries, gate_entries, product.key, plan.close_at_ns,
                    )
                    if (durable_gate is None
                            or durable_gate[0] != S3M1OriginAccountingAction.REUSE_LATE_TEST_GATE):
                        raise ValueError("native M1 reused late gate disappeared or conflicts")
                    gate_body = durable_gate[1].metadata.get("deadline_gate")
                    if not isinstance(gate_body, Mapping) or not isinstance(gate_body.get("reason_code"), str):
                        raise ValueError("native M1 reused late gate lost its exact TEST GATE reason")
                    _persist_late_s3_m1_origin_gate(
                        repository, product, bar, observed_at_ns=now_ns,
                        reason_code=gate_body["reason_code"],
                    )
                else:
                    raise ValueError("native M1 origin planner returned an unsupported action")

                event_entries, gate_entries = _native_m1_origin_state_entries(
                    repository, product.key, plan.close_at_ns, as_of_ns=now_ns,
                )
                durable = find_s3_m1_origin_accounting_state(
                    event_entries, gate_entries, product.key, plan.close_at_ns,
                )
                if durable is None:
                    raise ValueError("native M1 origin was not durably accounted before cursor advance")
                if plan.existing_accounting_ref is not None and durable[1].artifact_ref != plan.existing_accounting_ref:
                    raise ValueError("native M1 origin accounting changed during idempotent replay")
                accounted_close_at_ns = plan.close_at_ns

            if page.has_more and page.last_close_at_ns is None:
                raise ValueError("native M1 origin page claims more rows without a close cursor")
            checkpoint = advance_s3_m1_origin_checkpoint(
                previous, product.key,
                now_ns=now_ns,
                source_available_through_ns=available_through_ns,
                next_close_cursor_ns=page.last_close_at_ns if page.has_more else None,
                accounted_close_at_ns=accounted_close_at_ns,
                has_more=page.has_more,
            )
            _persist_s3_m1_origin_checkpoint(repository, checkpoint)

        events.sort(key=lambda item: (item.available_at_ns, item.information_cutoff_ns, item.event_id))
        source_ids_tuple = tuple(sorted(source_ids | {event.source_id for event in events}))
        state_rows: list[OpsSourceStateV1] = []
        for source_id in source_ids_tuple:
            health = health_states.get(source_id)
            state_rows.append(OpsSourceStateV1(
                source_id,
                health.state.value if health is not None else "UNKNOWN",
                health.observed_at_ns if health is not None else None,
                health.available_at_ns if health is not None else None,
            ))
        states = tuple(state_rows)
        reconciled = healthy
        if not reconciled and not self.scope_public_sources:
            # Keep durable decision handoffs queued while the collector is
            # replaying a reconnect gap. A restart must not turn an unfinished
            # event into a permanent source-health terminal receipt.
            events = []
        if self.scope_public_sources:
            # Retain the full current component state in the batch. The scoped
            # supervisor gate validates historical and current dependencies.
            events = [event for event in events if source_current(event.source_id)]
        return OpsCycleBatchV1(tuple(events), states, source_ids_tuple, tuple(sorted(set(refs))), reconciled, now_ns)


def _ops_receipt_identity_ref(event_id: str) -> str:
    return sha256_json({"artifact_type": "OpsSupervisorReceiptIdentityV1", "event_id": event_id})


def _persist_late_s3_m1_origin_gate(
    repository: OpsRepository,
    product: ProductContractV2,
    bar: Any,
    *,
    observed_at_ns: int,
    reason_code: str = "NATIVE_M1_BAR_FIRST_SEEN_AFTER_FIXED_DEADLINE",
) -> str:
    """Record a missed M1 origin without inventing an event cutoff."""
    origin = s3_m1_origin_metadata(product.key, bar.close_at_ns, bar_ref=bar.content_hash)["native_m1_origin"]
    if not isinstance(origin, Mapping):
        raise ValueError("native M1 late-origin metadata must be an object")
    origin_ref = origin.get("origin_ref")
    event_entries, gate_entries = _native_m1_origin_state_entries(
        repository, product.key, bar.close_at_ns, as_of_ns=observed_at_ns,
    )
    existing = find_s3_m1_origin_accounting_state(
        event_entries, gate_entries, product.key, bar.close_at_ns,
    )
    if existing is not None:
        if existing[0] == S3M1OriginAccountingAction.REUSE_TIMELY_EVENT:
            raise ValueError("native M1 origin already has a timely event and cannot gain a late gate")
        body = existing[1].metadata.get("deadline_gate")
        if not isinstance(body, Mapping) or body.get("reason_code") != reason_code:
            raise ValueError("native M1 origin already has a conflicting missed-origin gate")
        gate_ref = existing[1].artifact_ref
        persist_s3_late_origin_missingness(
            repository,
            instrument_key=product.key,
            decision_slot_ns=bar.close_at_ns,
            origin_ref=str(origin_ref),
            late_gate_ref=gate_ref,
            created_at_ns=max(observed_at_ns, existing[1].available_at_ns),
            available_at_ns=max(observed_at_ns, existing[1].available_at_ns),
        )
        return gate_ref
    body = {
        "version": "OPS_PUBLIC_ACQUISITION_DEADLINE_GATE_V1",
        "observed_at_ns": observed_at_ns,
        "eligible_cutoff_ns": bar.close_at_ns,
        "event_ids": [s3_m1_event_id(product.key, bar.close_at_ns)],
        "deadlines_ns": [bar.close_at_ns + S3_M1_DEFAULT_MAX_LATENESS_NS],
        "status": "TEST GATE",
        "reason_code": reason_code,
        "native_m1_origin_ref": origin_ref,
        "native_m1_bar_ref": bar.content_hash,
        "authority": "ZERO",
    }
    ref = sha256_json(body)
    repository.register_artifact(ArtifactIndexEntryV2(
        ref, "OpsPublicAcquisitionDeadlineGateV1", ref,
        observed_at_ns, observed_at_ns, {"deadline_gate": body,
                                        "native_m1_origin_ref": origin_ref,
                                        "native_m1_origin": dict(origin)},
    ))
    persist_s3_late_origin_missingness(
        repository,
        instrument_key=product.key,
        decision_slot_ns=bar.close_at_ns,
        origin_ref=str(origin_ref),
        late_gate_ref=ref,
        created_at_ns=observed_at_ns,
        available_at_ns=observed_at_ns,
    )
    return ref


def _native_m1_origin_state_entries(
    repository: OpsRepository,
    key: InstrumentKeyV2,
    close_at_ns: int,
    *,
    as_of_ns: int,
) -> tuple[tuple[ArtifactIndexEntryV2, ...], tuple[ArtifactIndexEntryV2, ...]]:
    origin_ref = s3_m1_origin_ref(key, close_at_ns)
    event_id = s3_m1_event_id(key, close_at_ns)
    event_rows: dict[str, ArtifactIndexEntryV2] = {}
    for path, identity in (("native_m1_origin", origin_ref), ("event_id", event_id)):
        metadata_path = (("native_m1_origin", "origin_ref") if path == "native_m1_origin"
                         else ("event", "event_id"))
        page = repository.artifact_entries_by_metadata_identity(
            "OpsDecisionEventSourceV1", metadata_path, identity,
            as_of_ns=as_of_ns, limit=2,
        )
        if page.invalid_entry_count or page.has_more:
            raise ValueError("native M1 event accounting identity is invalid or ambiguous")
        event_rows.update((entry.artifact_ref, entry) for entry in page.entries)
    gate_page = repository.artifact_entries_by_metadata_identity(
        "OpsPublicAcquisitionDeadlineGateV1", ("native_m1_origin_ref",), origin_ref,
        as_of_ns=as_of_ns, limit=2,
    )
    if gate_page.invalid_entry_count or gate_page.has_more:
        raise ValueError("native M1 late-gate accounting identity is invalid or ambiguous")
    return tuple(event_rows.values()), gate_page.entries


def _load_latest_s3_m1_origin_checkpoint(
    repository: OpsRepository,
    key: InstrumentKeyV2,
) -> S3NativeM1OriginAccountingCheckpointV1 | None:
    entry = repository.latest_native_m1_origin_accounting_checkpoint(key)
    if entry is None:
        return None
    body = entry.metadata.get("checkpoint")
    if not isinstance(body, Mapping):
        raise ValueError("native M1 origin checkpoint has no typed body")
    checkpoint = S3NativeM1OriginAccountingCheckpointV1.from_dict(body)
    if (entry.artifact_type != S3_M1_ACCOUNTING_CHECKPOINT_TYPE
            or entry.artifact_ref != checkpoint.content_hash
            or entry.content_hash != checkpoint.content_hash
            or entry.created_at_ns != checkpoint.created_at_ns
            or entry.available_at_ns != checkpoint.available_at_ns
            or checkpoint.instrument_key != key
            or entry.metadata.get("instrument_key_json") != key.to_canonical_json()):
        raise ValueError("native M1 origin checkpoint failed exact-key content validation")
    if checkpoint.generation == 1:
        if checkpoint.previous_checkpoint_ref is not None:
            raise ValueError("native M1 root checkpoint unexpectedly has a predecessor")
        return checkpoint
    previous_entry = repository.get_artifact(checkpoint.previous_checkpoint_ref or "")
    previous_body = previous_entry.metadata.get("checkpoint") if previous_entry is not None else None
    if (previous_entry is None or previous_entry.artifact_type != S3_M1_ACCOUNTING_CHECKPOINT_TYPE
            or not isinstance(previous_body, Mapping)):
        raise ValueError("native M1 checkpoint predecessor is missing")
    previous = S3NativeM1OriginAccountingCheckpointV1.from_dict(previous_body)
    if (previous.content_hash != checkpoint.previous_checkpoint_ref
            or previous_entry.content_hash != previous.content_hash
            or previous_entry.artifact_ref != previous.content_hash
            or previous.instrument_key != key
            or previous.generation + 1 != checkpoint.generation):
        raise ValueError("native M1 checkpoint predecessor hash or revision does not verify")
    if previous.scan_complete:
        if (checkpoint.source_available_from_ns < previous.source_available_through_ns
                or checkpoint.source_available_through_ns <= previous.source_available_through_ns):
            raise ValueError("native M1 completed source watermark regressed")
    elif (checkpoint.source_available_from_ns != previous.source_available_from_ns
          or checkpoint.source_available_through_ns != previous.source_available_through_ns
          or (checkpoint.source_scan_after_close_at_ns is not None
              and previous.source_scan_after_close_at_ns is not None
              and checkpoint.source_scan_after_close_at_ns <= previous.source_scan_after_close_at_ns)):
        raise ValueError("native M1 active source window or close cursor was rebased")
    if (checkpoint.last_accounted_close_at_ns is not None
            and previous.last_accounted_close_at_ns is not None
            and checkpoint.last_accounted_close_at_ns < previous.last_accounted_close_at_ns):
        raise ValueError("native M1 checkpoint accounted close regressed")
    return checkpoint


def _persist_s3_m1_origin_checkpoint(
    repository: OpsRepository,
    checkpoint: S3NativeM1OriginAccountingCheckpointV1,
) -> None:
    body = checkpoint.to_dict()
    repository.register_artifact(ArtifactIndexEntryV2(
        checkpoint.content_hash,
        S3_M1_ACCOUNTING_CHECKPOINT_TYPE,
        checkpoint.content_hash,
        checkpoint.created_at_ns,
        checkpoint.available_at_ns,
        {"checkpoint": body,
         "instrument_key_json": checkpoint.instrument_key.to_canonical_json(),
         "authority": "ZERO"},
    ))


def _new_recovery_epoch(repository: OpsRepository, *, started_at_ns: int) -> str:
    """Persist a monotone, content-addressed epoch for this supervisor recovery."""
    prior: list[tuple[int, str]] = []
    epoch_entries = _bounded_evidence(repository, "OpsRecoveryEpochV1")
    for entry in epoch_entries:
        body = entry.metadata.get("recovery_epoch")
        if (not isinstance(body, Mapping) or entry.content_hash != entry.artifact_ref
                or sha256_json(body) != entry.artifact_ref
                or body.get("version") != "OPS_RECOVERY_EPOCH_V1"
                or type(body.get("epoch_index")) is not int or body["epoch_index"] <= 0
                or type(body.get("started_at_ns")) is not int or body.get("authority") != "ZERO"
                or body.get("started_at_ns") != entry.available_at_ns):
            continue
        if entry.available_at_ns > started_at_ns:
            raise ValueError("ops recovery epoch clock moved behind persisted recovery evidence")
        prior.append((body["epoch_index"], entry.artifact_ref))
    prior.sort()
    indices = [index for index, _ref in prior]
    if len(indices) != len(set(indices)) or indices != list(range(1, len(indices) + 1)):
        raise ValueError("persisted ops recovery epoch sequence is ambiguous")
    epoch_by_ref = {entry.artifact_ref: entry for entry in epoch_entries}
    for position, (_index, ref) in enumerate(prior):
        body = epoch_by_ref[ref].metadata["recovery_epoch"]
        expected_previous = prior[position - 1][1] if position else None
        if body.get("previous_epoch_ref") != expected_previous:
            raise ValueError("persisted ops recovery epoch chain is invalid")
    epoch_index = prior[-1][0] + 1 if prior else 1
    previous_ref = prior[-1][1] if prior else None
    body = {
        "version": "OPS_RECOVERY_EPOCH_V1",
        "epoch_index": epoch_index,
        "started_at_ns": started_at_ns,
        "previous_epoch_ref": previous_ref,
        "authority": "ZERO",
    }
    ref = sha256_json(body)
    repository.register_artifact(ArtifactIndexEntryV2(
        ref, "OpsRecoveryEpochV1", ref, started_at_ns, started_at_ns,
        {"recovery_epoch": body},
    ))
    return ref


def _current_recovery_epoch_ref(repository: OpsRepository, *, now_ns: int) -> str | None:
    valid: list[tuple[int, str]] = []
    for entry in _bounded_evidence(repository, "OpsRecoveryEpochV1"):
        body = entry.metadata.get("recovery_epoch")
        if (not isinstance(body, Mapping) or entry.content_hash != entry.artifact_ref
                or sha256_json(body) != entry.artifact_ref
                or body.get("version") != "OPS_RECOVERY_EPOCH_V1"
                or type(body.get("epoch_index")) is not int
                or type(body.get("started_at_ns")) is not int or body.get("authority") != "ZERO"
                or body.get("started_at_ns") != entry.available_at_ns):
            continue
        valid.append((body["epoch_index"], entry.artifact_ref))
    if not valid:
        return None
    valid.sort()
    indices = [index for index, _ref in valid]
    if len(indices) != len(set(indices)) or indices != list(range(1, len(indices) + 1)):
        return None
    entries = {entry.artifact_ref: entry for entry in _bounded_evidence(repository, "OpsRecoveryEpochV1")}
    for position, (_index, ref) in enumerate(valid):
        entry = entries[ref]
        body = entry.metadata["recovery_epoch"]
        previous_ref = valid[position - 1][1] if position else None
        if body.get("previous_epoch_ref") != previous_ref:
            return None
    if entries[valid[-1][1]].available_at_ns > now_ns:
        return None
    return valid[-1][1]


def _reconcile_public_sources(
    repository: OpsRepository,
    collector: PublicCollectorV2,
    source_ids: tuple[str, ...],
    *,
    now_ns: int,
    recovery_epoch_ref: str | None,
) -> None:
    """Reconcile only from a persisted, exact public snapshot/gap-repair receipt."""
    for source_id in source_ids:
        current = collector.health.latest(source_id)
        if current is not None and current.data_eligible and current.available_at_ns <= now_ns:
            continue
        if current is not None and current.observed_at_ns >= now_ns:
            continue
        page = repository.artifact_entries_by_metadata_identity(
            "OpsPublicSourceReconciliationV1", ("reconciliation", "source_id"),
            source_id, as_of_ns=now_ns, limit=32,
        )
        if page.invalid_entry_count or page.has_more:
            continue
        for entry in page.entries:
            body = entry.metadata.get("reconciliation")
            if (not isinstance(body, Mapping) or entry.content_hash != entry.artifact_ref
                    or sha256_json(body) != entry.artifact_ref
                    or body.get("version") != "OPS_PUBLIC_SOURCE_RECONCILIATION_V1"
                    or body.get("source_id") != source_id
                    or recovery_epoch_ref is None
                    or body.get("recovery_epoch_ref") != recovery_epoch_ref
                    or body.get("available_at_ns") != entry.available_at_ns
                    or entry.available_at_ns > now_ns
                    or body.get("complete_snapshot") is not True
                    or body.get("missed_interval_repaired") is not True):
                continue
            refs = body.get("evidence_refs")
            if not isinstance(refs, (list, tuple)) or not refs or tuple(refs) != tuple(sorted(set(refs))):
                continue
            evidence_ok = True
            if len(refs) > 10_000:
                continue
            for ref in refs:
                item = repository.get_artifact(str(ref))
                if (item is None or item.artifact_type != "PublicObservationIndexV2"
                        or item.available_at_ns > entry.available_at_ns
                        or item.metadata.get("source_id") != source_id):
                    evidence_ok = False
                    break
            if not evidence_ok:
                continue
            # The current reconciliation observation is created by the collector
            # from the verified receipt; its timestamp is never backdated.
            at_ns = max(now_ns, (current.observed_at_ns + 1) if current is not None else now_ns)
            if at_ns > now_ns:
                continue
            collector.reconcile_after_reconnect(
                source_id, at_ns=at_ns, complete_snapshot=True, missed_interval_repaired=True,
                snapshot_refs=tuple(refs), recovery_epoch_ref=recovery_epoch_ref,
            )
            break


def _public_bar_event(
    repository: OpsRepository,
    product: ProductContractV2,
    trigger: Any,
    *,
    now_ns: int,
) -> OpsDecisionEventV1 | None:
    """Create an immutable runtime event handoff from one indexed final M15 bar."""
    index_ref = sha256_json({"artifact_type": "PublicObservationIndexV2",
                             "record_id": trigger.raw.record_id})
    indexed = repository.get_artifact(index_ref)
    if (indexed is None or indexed.artifact_type != "PublicObservationIndexV2"
            or indexed.available_at_ns > now_ns or indexed.metadata.get("bar_content_hash") != trigger.content_hash
            or indexed.metadata.get("instrument_key_json") != product.key.to_canonical_json()):
        return None
    _index_causal_bar(repository, trigger, index_ref)
    # The event cutoff must stay at the final bar's durable availability. A
    # later restart reconciliation proves the source is healthy now, but it
    # cannot be projected backward into the bar's decision-time health.
    health_entry = _latest_public_health(repository, trigger.raw.source_id, cutoff_ns=trigger.raw.available_at_ns)
    if health_entry is None:
        return None
    health_body = health_entry.metadata["health"]
    health = PublicSourceHealthV2.from_dict(health_body)
    if health.content_hash != health_entry.artifact_ref or not health.data_eligible:
        return None
    cutoff = max(trigger.raw.available_at_ns, health.available_at_ns)
    if cutoff > now_ns or cutoff - trigger.close_at_ns > 5_000_000_000:
        return None
    trigger_body = {
        "version": "OPS_PUBLIC_FINAL_BAR_TRIGGER_V1",
        "source_observation_ref": index_ref,
        "bar_ref": trigger.content_hash,
        "product_ref": product.content_hash,
        "source_id": trigger.raw.source_id,
        "source_event_at_ns": trigger.raw.event_at_ns,
        "source_published_at_ns": trigger.raw.published_at_ns,
        "received_at_ns": trigger.raw.received_at_ns,
        "available_at_ns": trigger.raw.available_at_ns,
        "information_cutoff_ns": cutoff,
        "authority": "ZERO",
    }
    trigger_ref = sha256_json(trigger_body)
    repository.register_artifact(ArtifactIndexEntryV2(
        trigger_ref, "OpsPublicFinalBarTriggerV1", trigger_ref,
        trigger.raw.available_at_ns, trigger.raw.available_at_ns, {"trigger": trigger_body},
    ))
    event_id = sha256_json({"version": "OPS_DECISION_EVENT_FROM_FINAL_BAR_V1", "trigger_ref": trigger_ref})
    return OpsDecisionEventV1(
        event_id, "CONFIRMED_15M_CLOSE", trigger.raw.source_id, trigger_ref,
        trigger.raw.event_at_ns or trigger.close_at_ns, trigger.raw.published_at_ns,
        trigger.raw.received_at_ns, trigger.raw.available_at_ns, cutoff, cutoff + 5_000_000_000,
        tuple(sorted({trigger_ref, index_ref, trigger.content_hash, product.content_hash, health.content_hash})),
    )


def _native_s3_m1_public_bar_event(
    repository: OpsRepository,
    product: ProductContractV2,
    trigger: Any,
    *,
    now_ns: int,
) -> OpsDecisionEventV1 | None:
    """Create one native S3 event from the exact, timely indexed final M1 bar."""
    if (trigger.interval != BarIntervalV2.M1 or not trigger.final
            or trigger.instrument_revision != product.key.contract_revision
            or product.effective_at_ns > trigger.close_at_ns
            or product.observed_at_ns > trigger.raw.available_at_ns
            or product.available_at_ns > trigger.raw.available_at_ns
            or classify_s3_m1_bar(trigger, now_ns=now_ns) != S3M1BarDisposition.TIMELY):
        return None
    index_ref = sha256_json({"artifact_type": "PublicObservationIndexV2",
                             "record_id": trigger.raw.record_id})
    indexed = repository.get_artifact(index_ref)
    if (indexed is None or indexed.artifact_type != "PublicObservationIndexV2"
            or indexed.available_at_ns > now_ns
            or indexed.content_hash != trigger.raw.content_hash
            or indexed.metadata.get("record_id") != trigger.raw.record_id
            or indexed.metadata.get("instrument_revision") != product.key.contract_revision
            or indexed.metadata.get("event_type") != "BAR_1M"
            or indexed.metadata.get("event_at_ns") != trigger.raw.event_at_ns
            or indexed.metadata.get("published_at_ns") != trigger.raw.published_at_ns
            or indexed.metadata.get("translation_version") != trigger.raw.translation_version
            or indexed.metadata.get("revision_of") != trigger.raw.revision_of
            or tuple(indexed.metadata.get("quality_flags", ())) != trigger.raw.quality_flags
            or indexed.metadata.get("availability_class") != trigger.raw.availability_class.value
            or indexed.metadata.get("replay_available_at_ns") != trigger.raw.replay_available_at_ns
            or indexed.metadata.get("bar_content_hash") != trigger.content_hash
            or indexed.metadata.get("instrument_key_json") != product.key.to_canonical_json()
            or indexed.metadata.get("raw_payload_hash") != trigger.raw.raw_payload_hash
            or indexed.metadata.get("availability_class") != AvailabilityClassV2.ACTUAL_SYSTEM.value):
        return None
    _index_causal_bar(repository, trigger, index_ref)

    # The M1 cutoff is fixed to source evidence available for this close. Later
    # source recovery is not allowed to move it forward to make the bar timely.
    health_entry = _latest_public_health(repository, trigger.raw.source_id, cutoff_ns=trigger.raw.available_at_ns)
    if health_entry is None:
        return None
    health_body = health_entry.metadata["health"]
    health = PublicSourceHealthV2.from_dict(health_body)
    if (health.content_hash != health_entry.artifact_ref or not health.data_eligible
            or health.observed_at_ns > trigger.raw.available_at_ns
            or health.available_at_ns > trigger.raw.available_at_ns):
        return None
    cutoff = max(trigger.raw.available_at_ns, health.available_at_ns)
    deadline = trigger.close_at_ns + S3_M1_DEFAULT_MAX_LATENESS_NS
    if cutoff > now_ns or cutoff > deadline:
        return None

    trigger_body = {
        "version": "OPS_PUBLIC_FINAL_BAR_TRIGGER_V1",
        "source_observation_ref": index_ref,
        "bar_ref": trigger.content_hash,
        "product_ref": product.content_hash,
        "source_id": trigger.raw.source_id,
        "source_event_at_ns": trigger.raw.event_at_ns,
        "source_published_at_ns": trigger.raw.published_at_ns,
        "received_at_ns": trigger.raw.received_at_ns,
        "available_at_ns": trigger.raw.available_at_ns,
        "information_cutoff_ns": cutoff,
        "authority": "ZERO",
    }
    trigger_ref = sha256_json(trigger_body)
    repository.register_artifact(ArtifactIndexEntryV2(
        trigger_ref, "OpsPublicFinalBarTriggerV1", trigger_ref,
        trigger.raw.available_at_ns, trigger.raw.available_at_ns, {"trigger": trigger_body},
    ))
    return OpsDecisionEventV1(
        s3_m1_event_id(product.key, trigger.close_at_ns), S3_M1_EVENT_TYPE,
        trigger.raw.source_id, trigger_ref,
        trigger.raw.event_at_ns or trigger.close_at_ns, trigger.raw.published_at_ns,
        trigger.raw.received_at_ns, trigger.raw.available_at_ns, cutoff, deadline,
        tuple(sorted({trigger_ref, index_ref, trigger.content_hash, product.content_hash, health.content_hash})),
    )


def _persist_public_event(
    repository: OpsRepository,
    event: OpsDecisionEventV1,
    trigger_record_id: str,
    now_ns: int,
    *,
    native_m1_origin: Mapping[str, object] | None = None,
) -> None:
    if event.information_cutoff_ns > now_ns:
        raise ValueError("public event handoff cannot be created before its information cutoff")
    repository.register_artifact(ArtifactIndexEntryV2(
        event.content_hash, "OpsDecisionEventSourceV1", event.content_hash,
        event.information_cutoff_ns, event.information_cutoff_ns,
        {"event": event.to_dict(), "trigger_record_id": trigger_record_id,
         "composition_id": OPS_PRODUCTION_ADAPTER_ID,
         **({"native_m1_origin": dict(native_m1_origin)} if native_m1_origin is not None else {})},
    ))


def _index_causal_bar(repository: OpsRepository, bar: Any, source_ref: str) -> str:
    repository.register_artifact(_causal_bar_entry(bar, source_ref, repository=repository))
    return bar.content_hash


def _causal_bar_entry(bar: Any, source_ref: str, *, repository: OpsRepository | None = None) -> ArtifactIndexEntryV2:
    # This aliases the exact bar body already present in the immutable raw
    # archive. Its first public availability includes index publication.
    available = bar.raw.available_at_ns
    if repository is not None:
        source = repository.get_artifact(source_ref)
        if source is not None:
            available = max(available, source.available_at_ns)
    return ArtifactIndexEntryV2(
        bar.content_hash, "CausalBarV2", bar.content_hash, available,
        available, {"bar": bar.to_dict(), "source_observation_ref": source_ref},
    )


def _indexed_s3_vwaps(
    repository: OpsRepository, key: InstrumentKeyV2, *, cutoff_ns: int,
) -> tuple[TradeVwapSnapshotV2, ...]:
    snapshots: list[TradeVwapSnapshotV2] = []
    for entry in repository.s3_context_window_entries(key,"S3TradeVwapSnapshotV2",cutoff_ns=cutoff_ns):
        if entry.available_at_ns > cutoff_ns:
            continue
        body = entry.metadata.get("vwap")
        if not isinstance(body, Mapping):
            continue
        try:
            snapshot = TradeVwapSnapshotV2.from_dict(body)
        except (ArithmeticError, TypeError, ValueError):
            continue
        if (entry.content_hash == snapshot.content_hash == entry.artifact_ref
                and entry.available_at_ns == snapshot.available_at_ns
                and snapshot.key == key and snapshot.information_cutoff_ns <= cutoff_ns
                and snapshot.available_at_ns <= cutoff_ns
                and snapshot.replay_view == AvailabilityClassV2.ACTUAL_SYSTEM.value):
            snapshots.append(snapshot)
    return tuple(sorted(snapshots, key=lambda item: (item.information_cutoff_ns, item.content_hash)))


def _indexed_s3_residuals(
    repository: OpsRepository, key: InstrumentKeyV2, *, cutoff_ns: int,
) -> tuple[ResidualObservationV2, ...]:
    residuals: list[ResidualObservationV2] = []
    for entry in repository.s3_context_window_entries(key,"S3ResidualObservationV2",cutoff_ns=cutoff_ns):
        if entry.available_at_ns > cutoff_ns:
            continue
        body = entry.metadata.get("residual")
        if not isinstance(body, Mapping) or body.get("schema_version") != 1:
            continue
        raw_key = body.get("key")
        if not isinstance(raw_key, Mapping):
            continue
        residual_value = body.get("residual")
        if isinstance(residual_value, bool) or not isinstance(residual_value, (int, float)):
            continue
        try:
            residual = ResidualObservationV2(
                InstrumentKeyV2.from_dict(raw_key), str(body["bar_ref"]), str(body["vwap_ref"]),
                int(body["close_at_ns"]), int(body["available_at_ns"]), float(residual_value),
                str(body["replay_view"]),
            )
        except (ArithmeticError, KeyError, TypeError, ValueError):
            continue
        if (residual.key == key and residual.content_hash == entry.artifact_ref == entry.content_hash
                and residual.available_at_ns == entry.available_at_ns
                and residual.available_at_ns <= cutoff_ns and residual.close_at_ns <= cutoff_ns):
            residuals.append(residual)
    return tuple(sorted(residuals, key=lambda item: (item.close_at_ns, item.content_hash)))


def _indexed_s3_trades(
    repository: OpsRepository,
    archive_root: Path,
    key: InstrumentKeyV2,
    *,
    cutoff_ns: int,
    health_by_source: Mapping[str, PublicSourceHealthV2],
) -> tuple[tuple[CausalTradeV2, ...], PublicSourceHealthV2 | None, tuple[str, ...]]:
    observations = reconstruct_public_observations_from_archive(
        repository, archive_root, instrument_revision=key.contract_revision,
        information_cutoff_ns=cutoff_ns, event_types=("TRADE", "AGG_TRADE"), limit=100_000, key=key,
    )
    translated: list[CausalTradeV2] = []
    for item in observations:
        observation = item.observation
        if (observation.availability_class != AvailabilityClassV2.ACTUAL_SYSTEM
                or observation.event_at_ns is None or observation.event_at_ns > cutoff_ns
                or observation.received_at_ns > cutoff_ns or observation.available_at_ns > cutoff_ns
                or (observation.published_at_ns is not None and observation.published_at_ns > cutoff_ns)):
            continue
        indexed = repository.get_artifact(item.observation_index_ref)
        if (indexed is None or indexed.metadata.get("instrument_key_json") != key.to_canonical_json()
                or indexed.metadata.get("source_id") != observation.source_id):
            continue
        try:
            payload = json.loads(item.raw_payload_bytes)
            if not isinstance(payload, Mapping):
                continue
            if key.venue.value == "BYBIT" and observation.event_type == "TRADE":
                translated_row, = translate_recent_trades(
                    (payload,), key=key, received_at_ns=observation.received_at_ns,
                    source_id=observation.source_id,
                )
                identity = payload.get("execId", payload.get("i"))
                price, quantity = Decimal(str(payload["p"])), Decimal(str(payload["v"]))
                raw_side = payload.get("S")
                aggressor = {"Buy": "BUY", "Sell": "SELL"}.get(str(raw_side))
                trade_id = str(identity)
            elif key.venue.value == "BINANCE" and observation.event_type == "AGG_TRADE":
                translated_row, = translate_agg_trades(
                    (payload,), key=key, received_at_ns=observation.received_at_ns,
                    source_id=observation.source_id,
                )
                identity = payload.get("a")
                price, quantity = Decimal(str(payload["p"])), Decimal(str(payload["q"]))
                buyer_is_maker = payload.get("m")
                aggressor = ("SELL" if buyer_is_maker else "BUY") if isinstance(buyer_is_maker, bool) else None
                trade_id = str(identity)
            else:
                continue
            if (translated_row.record_id != observation.record_id
                    or translated_row.raw_payload_hash != observation.raw_payload_hash
                    or translated_row.event_at_ns != observation.event_at_ns
                    or translated_row.sequence != observation.sequence):
                continue
            translated.append(CausalTradeV2(
                key, item.observation_index_ref, observation.source_id, trade_id,
                observation.event_at_ns, observation.received_at_ns, observation.available_at_ns,
                price, quantity, aggressor,
            ))
        except (ArithmeticError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            continue

    available_sources = {
        item.source_id for item in translated
        if item.effective_available_at(AvailabilityClassV2.ACTUAL_SYSTEM.value) is not None
    }
    current_sources = {
        source_id for source_id in available_sources
        if (source_id in health_by_source
            and health_by_source[source_id].available_at_ns <= cutoff_ns
            and health_by_source[source_id].observed_at_ns <= cutoff_ns
            and health_by_source[source_id].data_eligible)
    }
    if len(current_sources) != 1:
        return (), None, ()
    source_id = next(iter(current_sources))
    health = health_by_source[source_id]
    selected = tuple(item for item in translated if item.source_id == source_id)
    refs = tuple(sorted({item.raw_observation_ref for item in selected} | {health.content_hash}))
    return selected, health, refs


def _resolve_indexed_s3_inputs(
    repository: OpsRepository,
    archive_root: Path,
    key: InstrumentKeyV2,
    *,
    cutoff_ns: int,
    completed_1m: Sequence[Any],
    health_by_source: Mapping[str, PublicSourceHealthV2],
) -> tuple[tuple[ResidualObservationV2, ...], TradeVwapSnapshotV2 | None,
           tuple[CausalTradeV2, ...], PublicSourceHealthV2 | None, tuple[str, ...]]:
    """Rebuild S3-only typed inputs from exact indexed, cutoff-available public evidence."""
    trades, trade_health, trade_refs = _indexed_s3_trades(
        repository, archive_root, key, cutoff_ns=cutoff_ns, health_by_source=health_by_source,
    )
    snapshots = _indexed_s3_vwaps(repository, key, cutoff_ns=cutoff_ns)
    residuals = list(_indexed_s3_residuals(repository, key, cutoff_ns=cutoff_ns))
    residual_by_bar: dict[str, ResidualObservationV2] = {}
    for item in residuals:
        prior = residual_by_bar.get(item.bar_ref)
        if prior is None or (item.available_at_ns, item.content_hash) > (prior.available_at_ns, prior.content_hash):
            residual_by_bar[item.bar_ref] = item

    snapshots_by_cutoff: dict[int, list[TradeVwapSnapshotV2]] = {}
    for snapshot in snapshots:
        snapshots_by_cutoff.setdefault(snapshot.information_cutoff_ns, []).append(snapshot)
    for bar in completed_1m:
        if (not bar.final or bar.interval != BarIntervalV2.M1 or bar.instrument_revision != key.contract_revision
                or bar.close_at_ns > cutoff_ns or bar.raw.available_at_ns > cutoff_ns
                or bar.content_hash in residual_by_bar):
            continue
        matching = snapshots_by_cutoff.get(bar.close_at_ns, ())
        if len({item.content_hash for item in matching}) != 1:
            continue
        selected_snapshot = next(iter(matching), None)
        if selected_snapshot is None:
            continue
        try:
            residual = residual_observation(bar, selected_snapshot)
        except (ArithmeticError, TypeError, ValueError):
            continue
        if residual.available_at_ns <= cutoff_ns:
            existing = repository.get_artifact(residual.content_hash)
            if existing is None:
                repository.register_artifact(ArtifactIndexEntryV2(
                    residual.content_hash, "S3ResidualObservationV2", residual.content_hash,
                    residual.available_at_ns, residual.available_at_ns, {"residual": residual.to_dict()},
                ))
            residual_by_bar[bar.content_hash] = residual

    exact_snapshots = [item for item in snapshots if item.information_cutoff_ns == cutoff_ns]
    current_vwap: TradeVwapSnapshotV2 | None = None
    if len({item.content_hash for item in exact_snapshots}) == 1:
        current_vwap = exact_snapshots[0]
    elif not exact_snapshots and trades and trade_health is not None:
        current_vwap = utc_day_trade_vwap(
            trades, key=key, cutoff_ns=cutoff_ns, source_health=trade_health,
        )
        if current_vwap is not None:
            persist_trade_vwap_v2(repository, current_vwap)

    resolved = tuple(sorted(residual_by_bar.values(), key=lambda item: (item.close_at_ns, item.content_hash)))
    causal_refs = set(trade_refs)
    causal_refs.update(item.content_hash for item in resolved)
    causal_refs.update(item.vwap_ref for item in resolved)
    if current_vwap is not None:
        causal_refs.update((current_vwap.content_hash, *current_vwap.trade_refs, current_vwap.source_health_ref))
    return resolved, current_vwap, trades, trade_health, tuple(sorted(causal_refs))


@dataclass(frozen=True)
class ProductionCollectorRecoveryV1:
    collector: PublicCollectorV2
    restored_subscription_plan: SubscriptionPlanV2
    required_source_ids: tuple[str, ...]
    had_prior_source_state: bool
    recovery_epoch_ref: str


class ProductionOpsCyclePortV1:
    """Concrete ATLAS pipeline composition for the Session-027 supervisor."""

    def __init__(
        self,
        *,
        public_source: Any | None = None,
        public_stream_source: PublicStreamSourceV2 | Any | None = None,
        inputs_provider: ProductionEventInputsProviderV1 | None = None,
        crash_after_checkpoint: Callable[[PipelineStageV1], None] | None = None,
        clock_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        self.public_source = public_source or IndexedPublicCycleSourceV1()
        self.minimum_m15_origin_close_at_ns = 0
        self.public_stream_source = public_stream_source
        self.clock_ns = clock_ns
        self.inputs_provider = inputs_provider or IndexedProductionEventInputsV1(clock_ns=clock_ns)
        self.crash_after_checkpoint = crash_after_checkpoint
        self._collector_recovery: ProductionCollectorRecoveryV1 | None = None
        self._stream_archive: L2FrameArchiveV2 | None = None
        self._stream_trackers: dict[tuple[str, str, str], PublicStreamContinuityTrackerV1] = {}
        self._stream_books: dict[tuple[str, str, str], SequenceValidBookV2 | None] = {}
        self._stream_products: dict[str, ProductContractV2] = {}
        self._stream_connection_epochs: dict[tuple[str, str, str], int | None] = {}
        self._stream_disconnect_count = 0
        self._stream_overflow_seen = False
        self._stream_last_error_code: str | None = None
        self._stream_first_connection_allowed: set[tuple[str, str, str]] = set()
        self._stream_disconnect_seen: set[tuple[str, str, str]] = set()
        self._stream_run_epoch = ""
        self._stream_metadata_errors: set[str] = set()
        self._stream_ingestion_failed = False
        self._serviced_acquisition: Any | None = None
        self._stream_last_service_at_ns: int | None = None
        self._stream_last_report_at_ns: int | None = None
        self._stream_max_service_gap_ns = 0
        self._stream_max_service_duration_ns = 0
        self._stream_last_service_duration_ns = 0
        self._stream_service_frames = 0
        self._stream_service_calls = 0
        self._stream_transport_ref: str | None = None
        self._owner_stream_reports: dict[str, Any] = {}
        self._stream_checkpoint_refs: dict[tuple[str, str, str], str] = {}
        self._stream_book_checkpoint_refs: dict[tuple[str, str, str], str] = {}
        self._stream_pending_book_archives: dict[tuple[str, str, str], list[str]] = {}
        self._stream_pending_book_transports: dict[tuple[str, str, str], list[str]] = {}
        self._stream_pending_book_controls: dict[tuple[str, str, str], list[str]] = {}
        self._stream_pending_book_health: dict[tuple[str, str, str], list[str]] = {}
        self._stream_epoch_first_receipt: dict[tuple[str, str, str], tuple[int | None, int]] = {}
        self._recovery_calls = 0
        self._collection_calls = 0

    def close(self) -> None:
        """Stop the opt-in producer; persistence remains exclusively in collect/recover."""
        if self.public_stream_source is not None:
            close = getattr(self.public_stream_source, "close", None)
            if callable(close):
                close()
        if self._serviced_acquisition is not None:
            self._serviced_acquisition.close(timeout_s=0.1)
        else:
            close = getattr(self.public_source, "close", None)
            if callable(close):
                close()

    def finish_public_capture(self, repository: OpsRepository) -> None:
        """Seal and adopt the bounded final raw backlog before closing SQLite.

        This is an owner stop boundary, not a claim of current source health or
        restart continuity. A failed ingestion retains unadopted raw receipts.
        """
        if self.public_stream_source is None or not callable(
                getattr(self.public_stream_source, "drain_sealed_transport", None)):
            return
        from ..data.durable_public_capture import MAX_PENDING_CAPTURE_BATCHES

        capture = cast(Any, self.public_stream_source)
        capture.close()
        if self._stream_ingestion_failed:
            return
        deadline = time.monotonic() + 5.0
        for _ in range(MAX_PENDING_CAPTURE_BATCHES):
            if not capture.status().pending_frames:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("PUBLIC_CAPTURE_FINAL_BACKLOG_DEADLINE_EXCEEDED")
            self._collect_public_stream_evidence(repository, now_ns=sample(self.clock_ns, floor_ns=0))
        if capture.status().pending_frames:
            raise RuntimeError("PUBLIC_CAPTURE_FINAL_BACKLOG_EXCEEDED")
        mark_clean = getattr(capture, "mark_controller_capture_clean", None)
        if callable(mark_clean):
            mark_clean(repository)

    def service_public_stream(self, repository: OpsRepository) -> None:
        """Service at most four FIFO batches, checking a 50ms allowance between them.

        Called only by the supervisor writer at safe boundaries. Individual
        batch cost and missed servicing headroom are measured, never hidden.
        """
        if (self.public_stream_source is None or self._collector_recovery is None
                or self._stream_ingestion_failed):
            # The first failed interpretation already closes the source and
            # raises durable supervisor failure evidence. Collection cycles
            # continue to fail closed until restart; idle polling must not
            # repeat that terminal failure fifty times per second.
            return
        started = time.monotonic_ns()
        for _ in range(4):
            status = self.public_stream_source.status()
            now = sample(self.clock_ns, floor_ns=0)
            queued = getattr(status, "pending_frames", getattr(status.handoff, "queue_items", 0))
            if (0 < queued < PUBLIC_STREAM_MAX_FRAMES_PER_CYCLE_V1
                    and self._stream_last_service_at_ns is not None
                    and now - self._stream_last_service_at_ns < 200_000_000
                    and getattr(status, "state", None) == "RUNNING"
                    and getattr(status.handoff, "connected", False)
                    and not getattr(status.handoff, "overflowed", False)):
                # A bounded 200ms accumulation avoids tiny Parquet commits at
                # every poll. Pressure/loss bypass this accumulation immediately.
                break
            if not queued and self._stream_last_service_at_ns is not None and (
                    now - self._stream_last_service_at_ns < 1_000_000_000):
                break
            self._collect_public_stream_evidence(repository, now_ns=now, publish_report=(
                self._stream_last_report_at_ns is None
                or now - self._stream_last_report_at_ns >= 1_000_000_000))
            if not queued or time.monotonic_ns() - started >= 50_000_000:
                break

    def bind_runtime_clock(self, clock_ns: Callable[[], int]) -> None:
        """Use the supervisor's single clock for event and computation timing."""
        self.clock_ns = clock_ns
        if isinstance(self.public_source, IndexedPublicCycleSourceV1):
            self.public_source.clock_ns = clock_ns
            self.public_source.stream_service = lambda repository: self.service_public_stream(repository)
        if isinstance(self.inputs_provider, IndexedProductionEventInputsV1):
            self.inputs_provider.clock_ns = clock_ns
            self.inputs_provider.stream_service = lambda repository: self.service_public_stream(repository)

    def recover(self, repository: OpsRepository, *, now_ns: int) -> OpsRecoverySnapshotV1:
        """Restore collector cursors, active watches and subscriptions first."""
        if repository.read_only:
            raise ValueError("production ops composition requires the supervisor-owned writable repository")
        bootstrap_products = getattr(self.public_source, "bootstrap_products", None)
        if callable(bootstrap_products):
            # The public acquisition object returns immutable metadata only. This
            # supervisor-owned controller path is the sole ops.sqlite writer.
            try:
                products = bootstrap_products(now_ns=now_ns)
            except (PublicDataError, KeyError, TypeError, ValueError, ArithmeticError):
                # Metadata bootstrap failure must leave the source ineligible but
                # must not prevent the supervisor from running its deterministic
                # archive/recovery cycle. The adapter reports the missing bootstrap
                # as an incomplete snapshot during collection.
                products = ()
            for product in products:
                broad_venues = getattr(self.public_source, "enabled_venues", None)
                if broad_venues is not None:
                    if (product.key.venue not in broad_venues
                            or product.key.environment.value != "MAINNET"
                            or product.key.product.value != "LINEAR_PERPETUAL"):
                        raise ValueError("broad bootstrap returned an out-of-scope product")
                elif (product.key.venue.value != "BYBIT"
                        or product.key.environment.value != "MAINNET"
                        or product.key.product.value != "LINEAR_PERPETUAL"
                        or product.key.native_symbol not in {"BTCUSDT", "ETHUSDT"}):
                    raise ValueError("opt-in Bybit bootstrap returned an out-of-scope product")
                repository.register_artifact(ArtifactIndexEntryV2(
                    product.content_hash, "ProductContractV2", product.content_hash,
                    product.observed_at_ns, product.available_at_ns, {"product": product.to_dict()},
                ))
        recovery_epoch_ref = _new_recovery_epoch(repository, started_at_ns=now_ns)
        registry = InstrumentRegistryV2()
        for entry in _bounded_evidence(repository, "ProductContractV2"):
            body = entry.metadata.get("product")
            if isinstance(body, Mapping):
                product = ProductContractV2.from_dict(json_value(body))
                if product.content_hash != entry.artifact_ref:
                    raise ValueError("stored product identity differs from its typed production artifact")
                registry.register(product)
        archive = ParquetObservationArchiveV2(Path(repository.path).parent / "ops-observations",
            compact_stream_repository=repository, clock_ns=lambda: self.clock_ns())
        collector = PublicCollectorV2(
            repository=repository,
            registry=registry,
            clock_ns=lambda: now_ns,
            archive=archive,
            required_recovery_epoch_ref=recovery_epoch_ref,
        )
        tiers = {product.key: ComputeTierV2.TIER_1 for product in registry.contracts()}
        active_watches = _bounded_active_watches(repository)
        expiry_ids = {watch.watch_id: (
            sha256_json({"version": "OPS_RESTART_WATCH_EXPIRY_V1", "watch_id": watch.watch_id,
                         "state_version": watch.state_version, "expires_at_ns": watch.expires_at_ns}),
            sha256_json({"version": "OPS_RESTART_WATCH_EXPIRY_OUTBOX_V1", "watch_id": watch.watch_id,
                         "state_version": watch.state_version, "expires_at_ns": watch.expires_at_ns}),
        ) for watch in active_watches if watch.expires_at_ns <= now_ns}
        restart = collector.restore_subscriptions(tiers, now_ns=now_ns, expiry_ids=expiry_ids)
        source_ids = tuple(sorted(set(self.public_source.required_source_ids) | set(repository.source_health_sources())))
        states: list[OpsSourceStateV1] = []
        had_prior = False
        for source_id in source_ids:
            history = repository.source_health_history(source_id, limit=1)
            had_prior = had_prior or bool(history)
            if history:
                latest = history[-1]
                states.append(OpsSourceStateV1(source_id, latest.status, latest.observed_at_ns, latest.available_at_ns))
            else:
                states.append(OpsSourceStateV1(source_id, "UNKNOWN", now_ns, now_ns))
        self._collector_recovery = ProductionCollectorRecoveryV1(
            collector, restart.subscriptions, source_ids, had_prior, recovery_epoch_ref
        )
        if self.public_stream_source is not None:
            self._restore_public_stream_state(repository, collector, now_ns=now_ns)
            configure_capture = getattr(self.public_stream_source, "configure_capture", None)
            if callable(configure_capture):
                configure_capture(Path(repository.path).parent, capture_epoch=self._stream_run_epoch)
                recover_capture = getattr(self.public_stream_source, "recover_controller_capture", None)
                if callable(recover_capture):
                    recover_capture(repository)
            start_stream = getattr(self.public_stream_source, "start", None)
            if not callable(start_stream):
                raise ValueError("opt-in public stream source must expose its bounded start lifecycle")
            start_stream()
        self._recovery_calls += 1
        return OpsRecoverySnapshotV1(
            source_ids,
            tuple(states),
            tuple(watch.watch_id for watch in restart.watches.active_watches),
            restart.subscriptions.plan_id,
            not had_prior,
            now_ns,
        )

    def collect(
        self,
        repository: OpsRepository,
        *,
        now_ns: int,
        recovery: OpsRecoverySnapshotV1,
    ) -> OpsCycleBatchV1:
        if self._collector_recovery is None:
            raise RuntimeError("PublicCollectorV2 recovery must precede production collection")
        if self._collector_recovery.collector.repository is not repository:
            raise ValueError("production collector must reuse the supervisor-owned OpsRepository")
        if self.public_stream_source is not None:
            self._collect_public_stream_evidence(repository, now_ns=now_ns)
        acquire_snapshot = getattr(self.public_source, "acquire_snapshot", None)
        if callable(acquire_snapshot):
            # Network acquisition returns bounded immutable records. Only this
            # supervisor-owned controller persists them through the collector.
            begin_collection_cycle = getattr(self.public_source, "begin_collection_cycle", None)
            if self._serviced_acquisition is not None:
                snapshot = self._serviced_acquisition.acquire(
                    now_ns=now_ns, service=lambda: self.service_public_stream(repository))
            elif callable(begin_collection_cycle):
                begin_collection_cycle(now_ns=now_ns)
                snapshot = acquire_snapshot(now_ns=now_ns)
            else:
                snapshot = acquire_snapshot(now_ns=now_ns)
            self._register_refreshed_stream_products(repository, snapshot)
            snapshot_eligible = self._persist_public_snapshot(repository, snapshot, now_ns=now_ns)
            if not snapshot_eligible:
                # Retain opportunity accounting on a failed acquisition too.
                # Its returned decisions cannot enter this closed cycle.
                IndexedPublicCycleSourceV1(clock_ns=self.clock_ns,
                    minimum_m15_origin_close_at_ns=self.minimum_m15_origin_close_at_ns).collect(
                    repository, self._collector_recovery.collector, now_ns=now_ns, recovery=recovery,
                    allow_events=False,
                )
                self._record_late_public_events(
                    repository, eligible_at_ns=now_ns, completed_at_ns=snapshot.observed_at_ns,
                )
                # Do not run the indexed handoff builder after a failed current
                # acquisition: it persists event artifacts as a side effect.
                # The supervisor sees only the as-of source-state prefix and a
                # closed gate. Exact durable events already past deadline remain
                # eligible for their immutable EXPIRED receipt.
                expired = self._expired_public_events(repository, now_ns=now_ns)
                source_ids = tuple(sorted(
                    set(self._collector_recovery.required_source_ids)
                    | set(repository.source_health_sources())
                ))
                states = self._public_source_states_as_of(
                    self._collector_recovery.collector, source_ids, now_ns=now_ns,
                )
                batch = OpsCycleBatchV1(
                    expired, states, source_ids,
                    tuple(event.content_hash for event in expired), False, now_ns,
                )
            else:
                batch = IndexedPublicCycleSourceV1(clock_ns=self.clock_ns,
                    minimum_m15_origin_close_at_ns=self.minimum_m15_origin_close_at_ns).collect(
                    repository, self._collector_recovery.collector, now_ns=now_ns, recovery=recovery,
                )
                late_event_ids = set(self._record_late_public_events(
                    repository, eligible_at_ns=now_ns, completed_at_ns=snapshot.observed_at_ns,
                ))
                late_event_ids.update(
                    event.event_id for event in batch.events if snapshot.observed_at_ns > event.deadline_ns
                )
                late_events = tuple(
                    event for event in batch.events if event.event_id in late_event_ids
                )
                if late_events:
                    retained = tuple(event for event in batch.events if event not in late_events)
                    retained_refs = {event.content_hash for event in retained}
                    batch = replace(
                        batch,
                        events=retained,
                        evidence_refs=tuple(ref for ref in batch.evidence_refs if ref in retained_refs),
                    )
                # If acquisition carried a persisted event across its deadline,
                # the event must remain excluded while this cycle's start time
                # is still pre-deadline. On a following cycle that starts after
                # the immutable deadline, pass the exact durable event through
                # so the unchanged supervisor records EXPIRED instead of
                # repeatedly dropping it or replaying it as timely.
                expired = self._expired_public_events(repository, now_ns=now_ns)
                if expired:
                    known_ids = {event.event_id for event in batch.events}
                    additional = tuple(event for event in expired if event.event_id not in known_ids)
                    if additional:
                        events = tuple(sorted(
                            (*batch.events, *additional),
                            key=lambda item: (item.available_at_ns, item.information_cutoff_ns, item.event_id),
                        ))
                        batch = replace(
                            batch,
                            events=events,
                            evidence_refs=tuple(sorted({*batch.evidence_refs,
                                                        *(event.content_hash for event in additional)})),
                        )
        else:
            batch = self.public_source.collect(
                repository,
                self._collector_recovery.collector,
                now_ns=now_ns,
                recovery=recovery,
            )
        if batch.collected_at_ns != now_ns:
            raise ValueError("production public collector must preserve its observed cycle collection time")
        self._collection_calls += 1
        return batch

    @staticmethod
    def _stream_feed_key(
        instrument: InstrumentKeyV2, source_id: str, channel: str,
    ) -> tuple[str, str, str]:
        return instrument.content_hash, source_id, channel

    @staticmethod
    def _stream_topic_identity(channel: str) -> tuple[str, str] | None:
        if channel.startswith("orderbook.50."):
            symbol = channel.removeprefix("orderbook.50.")
        elif channel.startswith("publicTrade."):
            symbol = channel.removeprefix("publicTrade.")
        else:
            return None
        return (symbol, channel) if symbol in {"BTCUSDT", "ETHUSDT"} else None

    @staticmethod
    def _latest_stream_product(
        registry: InstrumentRegistryV2, symbol: str, *, as_of_ns: int,
    ) -> ProductContractV2 | None:
        eligible = [
            product for product in registry.contracts()
            if product.key.venue.value == "BYBIT"
            and product.key.environment.value == "MAINNET"
            and product.key.product.value == "LINEAR_PERPETUAL"
            and product.key.native_symbol == symbol
            and product.effective_at_ns <= as_of_ns
            and product.observed_at_ns <= as_of_ns
            and product.available_at_ns <= as_of_ns
        ]
        return max(
            eligible,
            key=lambda product: (product.effective_at_ns, product.available_at_ns, product.content_hash),
            default=None,
        )

    @staticmethod
    def _read_latest_stream_states(
        repository: OpsRepository, *, as_of_ns: int,
    ) -> dict[tuple[str, str], PublicStreamContinuityStateV1]:
        entries = repository.latest_stream_continuity_entries(as_of_ns=as_of_ns)
        latest: dict[
            tuple[str, str],
            tuple[int, int, int, int, int, int, str, PublicStreamContinuityStateV1],
        ] = {}
        for entry in entries:
            try:
                state = validate_continuity_checkpoint(repository, entry)
            except (ArithmeticError, KeyError, TypeError, ValueError) as exc:
                if entry.artifact_type == "PublicStreamContinuityCheckpointV2":
                    raise ValueError("latest compact continuity checkpoint failed validation") from exc
                continue
            identity = (state.instrument.native_symbol, state.channel)
            old = latest.get(identity)
            candidate = (
                state.last_available_at_ns or 0,
                state.recovery_epoch,
                state.observed_trade_count,
                state.last_transport_receipt_at_ns or 0,
                state.gap_count,
                entry.available_at_ns,
                entry.artifact_ref,
                state,
            )
            if old is None or candidate[:7] > old[:7]:
                latest[identity] = candidate
        return {identity: value[-1] for identity, value in latest.items()}

    def _persist_stream_decision(
        self,
        repository: OpsRepository,
        observation: PublicStreamObservationV1,
        decision: Any,
    ) -> None:
        # Raw frames/trades and ordinary accepted transitions are archived in
        # their bounded data archives. SQLite receives only recovery and fault
        # observations, avoiding one ops row per high-frequency market frame.
        classification = getattr(decision.classification, "value", str(decision.classification))
        if classification not in {
            "GAP_RECORDED", "RECOVERY_EPOCH_STARTED", "CONFLICTING_TRADE_ID",
            "TRADE_ID_HISTORY_UNAVAILABLE", "OUT_OF_ORDER_TRADE_EVENT_TIME",
            "OUT_OF_ORDER_TRADE_RECEIPT_TIME", "OUT_OF_ORDER_RECEIPT_TIME",
        }:
            return
        body = {"observation": observation.to_dict(), "decision": decision.to_dict()}
        ref = sha256_json({"artifact_type": "PublicStreamContinuityEventV1", "body": body})
        repository.register_artifact(ArtifactIndexEntryV2(
            ref, "PublicStreamContinuityEventV1", ref,
            observation.available_at_ns, observation.available_at_ns, body,
        ))
        if observation.channel.startswith("orderbook."):
            key = self._stream_feed_key(observation.instrument, observation.source_id, observation.channel)
            controls = self._stream_pending_book_controls.setdefault(key, [])
            controls.append(ref)
            if len(controls) > 256:
                raise ValueError("book control lineage population exceeded its bound")

    def _apply_stream_observation(
        self,
        repository: OpsRepository,
        tracker: PublicStreamContinuityTrackerV1,
        observation: PublicStreamObservationV1,
        *,
        durable_prior_payload_hash: str | None = None,
        durable_lookup_complete: bool = False,
    ) -> Any:
        decision = tracker.apply_deferred(observation, durable_prior_payload_hash=durable_prior_payload_hash,
                                           durable_lookup_complete=durable_lookup_complete)
        self._persist_stream_decision(repository, observation, decision)
        return decision

    def _restore_public_stream_state(
        self, repository: OpsRepository, collector: PublicCollectorV2, *, now_ns: int,
    ) -> None:
        if self.public_stream_source is None:
            return
        if tuple(getattr(self.public_stream_source, "topics", ())) != bybit_btc_eth_linear_topics():
            raise ValueError("S32 public stream source must use exactly the explicit four Bybit BTC/ETH topics")
        if getattr(self.public_stream_source, "venue", None) is not None and str(
            getattr(self.public_stream_source.venue, "value", self.public_stream_source.venue)
        ) != "BYBIT":
            raise ValueError("S32 public stream source must be Bybit public linear")
        recovery = cast(ProductionCollectorRecoveryV1, self._collector_recovery)
        self._stream_run_epoch = recovery.recovery_epoch_ref
        self._stream_archive = L2FrameArchiveV2(Path(repository.path).parent / "ops-l2-frames", repository,
            compact_live=True, clock_ns=lambda: self.clock_ns())
        prior_states = self._read_latest_stream_states(repository, as_of_ns=now_ns)
        prior_refs = {validate_continuity_checkpoint(repository, entry).content_hash: entry.artifact_ref
                      for entry in repository.latest_stream_continuity_entries(as_of_ns=now_ns)
                      if entry.artifact_type == "PublicStreamContinuityCheckpointV2"}
        products = {
            symbol: product
            for symbol in ("BTCUSDT", "ETHUSDT")
            if (product := self._latest_stream_product(collector.registry, symbol, as_of_ns=now_ns)) is not None
        }
        self._stream_products = products
        for product in products.values():
            for channel in (f"orderbook.50.{product.key.native_symbol}", f"publicTrade.{product.key.native_symbol}"):
                key = self._stream_feed_key(product.key, BYBIT_PUBLIC_WS_SOURCE_ID_V1, channel)
                prior = prior_states.get((product.key.native_symbol, channel))
                exact_prior = bool(
                    prior is not None
                    and prior.instrument == product.key
                    and prior.metadata_ref == product.metadata_ref
                    and prior.source_id == BYBIT_PUBLIC_WS_SOURCE_ID_V1
                )
                if exact_prior:
                    tracker = PublicStreamContinuityTrackerV1.from_state(cast(PublicStreamContinuityStateV1, prior))
                    epoch_id = f"{self._stream_run_epoch}:controller"
                    transition = PublicStreamObservationV1.transport(
                        instrument=product.key, source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1, channel=channel,
                        metadata_ref=product.metadata_ref, epoch_id=epoch_id,
                        kind=PublicStreamObservationKindV1.CONTROLLER_RESTART,
                        observed_at_ns=max(now_ns, tracker.state.last_available_at_ns or 0),
                    )
                    self._apply_stream_observation(repository, tracker, transition)
                    book = (
                        SequenceValidBookV2(
                            instrument=product.key, source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1,
                            channel=channel, sequence_semantics="BYBIT_U",
                        ) if channel.startswith("orderbook.") else None
                    )
                    if book is not None:
                        book.reconnect(max(now_ns, tracker.state.last_available_at_ns or now_ns))
                    self._stream_books[key] = book
                elif prior is not None and prior.source_id == BYBIT_PUBLIC_WS_SOURCE_ID_V1:
                    old_tracker = PublicStreamContinuityTrackerV1.from_state(prior)
                    epoch_id = f"{self._stream_run_epoch}:metadata"
                    tracker, transition = old_tracker.rebind_metadata(
                        instrument=product.key, metadata_ref=product.metadata_ref,
                        epoch_id=epoch_id, observed_at_ns=max(now_ns, prior.last_available_at_ns or 0),
                    )
                    self._apply_stream_observation(repository, tracker, transition)
                    book = (
                        SequenceValidBookV2(
                            instrument=product.key, source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1,
                            channel=channel, sequence_semantics="BYBIT_U",
                        ) if channel.startswith("orderbook.") else None
                    )
                    if book is not None:
                        book.reconnect(max(now_ns, tracker.state.last_available_at_ns or now_ns))
                    self._stream_books[key] = book
                else:
                    tracker = PublicStreamContinuityTrackerV1(
                        instrument=product.key, source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1,
                        channel=channel, metadata_ref=product.metadata_ref,
                        epoch_id=f"{self._stream_run_epoch}:pending",
                        prior_recovery_ref=prior.current_recovery_ref if prior is not None else None,
                    )
                    self._stream_books[key] = (SequenceValidBookV2(
                        instrument=product.key, source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1,
                        channel=channel, sequence_semantics="BYBIT_U",
                    ) if channel.startswith("orderbook.") else None)
                    self._stream_first_connection_allowed.add(key)
                self._stream_trackers[key] = tracker
                self._stream_connection_epochs[key] = None
                self._stream_checkpoint_refs[key] = persist_continuity_checkpoint(
                    repository, tracker.to_state(), available_at_ns=now_ns,
                    prior_ref=prior_refs.get(prior.content_hash) if prior is not None else None, transport_ref=None,
                    clock_ns=self.clock_ns)

    @staticmethod
    def _persist_stream_state(
        repository: OpsRepository,
        state: PublicStreamContinuityStateV1,
        *,
        available_at_ns: int,
    ) -> None:
        """Index one immutable continuity snapshot once, including empty restart markers."""
        ref = state.content_hash
        existing = repository.get_artifact(ref)
        if existing is not None:
            if (existing.artifact_type != "PublicStreamContinuityStateV1"
                    or existing.content_hash != ref
                    or json_value(existing.metadata.get("state")) != state.to_dict()):
                raise ValueError("stored public stream continuity state identity conflicts")
            return
        available = max(available_at_ns, state.last_available_at_ns or 0)
        repository.register_artifact(ArtifactIndexEntryV2(
            ref, "PublicStreamContinuityStateV1", ref, available, available,
            {"state": state.to_dict()},
        ))

    def _register_refreshed_stream_products(self, repository: OpsRepository, snapshot: Any) -> None:
        del snapshot
        products = getattr(self.public_source, "current_products", ())
        if callable(products):
            products = products()
        if not isinstance(products, (list, tuple)):
            return
        recovery = cast(ProductionCollectorRecoveryV1, self._collector_recovery)
        for product in products:
            if not isinstance(product, ProductContractV2):
                continue
            broad_venues = getattr(self.public_source, "enabled_venues", None)
            in_scope = (product.key.venue in broad_venues
                and product.key.environment.value == "MAINNET"
                and product.key.product.value == "LINEAR_PERPETUAL") if broad_venues is not None else (
                product.key.venue.value == "BYBIT" and product.key.environment.value == "MAINNET"
                and product.key.product.value == "LINEAR_PERPETUAL"
                and product.key.native_symbol in {"BTCUSDT", "ETHUSDT"})
            if not in_scope:
                self._stream_metadata_errors.add("OUT_OF_SCOPE_POINT_IN_TIME_METADATA")
                continue
            if product.available_at_ns > max(self.clock_ns(), recovery.collector.clock_ns()):
                # The future contract remains unavailable to this controller cutoff.
                continue
            # Recovery already applies this active-evidence budget. Apply the
            # same bound to hourly refreshes so per-frame registry lookups cannot
            # grow indefinitely during a continuous run. Retained contracts in
            # SQLite are never evicted or rewritten.
            contracts = recovery.collector.registry.contracts()
            if (len(contracts) >= 4096
                    and all(item.content_hash != product.content_hash for item in contracts)):
                error = ActiveEvidenceOverflowV1({
                    "version": "OpsActiveWorkPressureV1", "artifact_type": "ProductContractV2",
                    "evidence_cutoff_ns": product.available_at_ns, "limit": 4096,
                    "has_more": True, "invalid_entry_count": 0,
                    "reason": "ACTIVE_EVIDENCE_POPULATION_OVERFLOW", "authority": "ZERO",
                })
                error.publish(repository)
                raise error
            try:
                recovery.collector.registry.register(product)
                repository.register_artifact(ArtifactIndexEntryV2(
                    product.content_hash, "ProductContractV2", product.content_hash,
                    product.observed_at_ns, product.available_at_ns, {"product": product.to_dict()},
                ))
            except (ValueError, TypeError):
                self._stream_metadata_errors.add("CONFLICTING_OR_UNSAFE_METADATA_REVISION")

    def _connection_epoch_id(self, attempt: int | None) -> str:
        suffix = f"connection:{attempt}" if attempt is not None and attempt > 0 else "pending"
        return f"{self._stream_run_epoch}:{suffix}"

    def _ensure_stream_tracker(
        self,
        repository: OpsRepository,
        product: ProductContractV2,
        channel: str,
        *,
        epoch_number: int | None,
        available_at_ns: int,
    ) -> tuple[tuple[str, str, str], PublicStreamContinuityTrackerV1, SequenceValidBookV2 | None]:
        key = self._stream_feed_key(product.key, BYBIT_PUBLIC_WS_SOURCE_ID_V1, channel)
        tracker = self._stream_trackers.get(key)
        if tracker is None:
            previous_item = next((
                (existing_key, existing)
                for existing_key, existing in reversed(tuple(self._stream_trackers.items()))
                if existing.state.instrument.native_symbol == product.key.native_symbol
                and existing.state.channel == channel
                and existing.state.source_id == BYBIT_PUBLIC_WS_SOURCE_ID_V1
            ), None)
            epoch_id = self._connection_epoch_id(epoch_number)
            book: SequenceValidBookV2 | None
            if previous_item is not None:
                previous_key, previous = previous_item
                tracker, observation = previous.rebind_metadata(
                    instrument=product.key, metadata_ref=product.metadata_ref,
                    epoch_id=epoch_id, observed_at_ns=available_at_ns,
                )
                self._apply_stream_observation(repository, tracker, observation)
                book = (SequenceValidBookV2(
                    instrument=product.key, source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1,
                    channel=channel, sequence_semantics="BYBIT_U",
                ) if channel.startswith("orderbook.") else None)
                if book is not None:
                    book.reconnect(available_at_ns)
                self._stream_books[key] = book
                self._stream_trackers.pop(previous_key, None)
                self._stream_epoch_first_receipt.pop(previous_key, None)
                self._stream_books.pop(previous_key, None)
                self._stream_connection_epochs.pop(previous_key, None)
                self._stream_disconnect_seen.discard(previous_key)
                self._stream_first_connection_allowed.discard(previous_key)
                self._stream_checkpoint_refs.pop(previous_key, None)
                self._stream_book_checkpoint_refs.pop(previous_key, None)
                self._stream_pending_book_archives.pop(previous_key, None)
                self._stream_pending_book_transports.pop(previous_key, None)
                self._stream_pending_book_health.pop(previous_key, None)
                self._stream_pending_book_controls.pop(previous_key, None)
            else:
                tracker = PublicStreamContinuityTrackerV1(
                    instrument=product.key, source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1,
                    channel=channel, metadata_ref=product.metadata_ref, epoch_id=epoch_id,
                )
                book = (
                    SequenceValidBookV2(instrument=product.key, source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1,
                                        channel=channel, sequence_semantics="BYBIT_U")
                    if channel.startswith("orderbook.") else None
                )
                self._stream_books[key] = book
            self._stream_trackers[key] = tracker
            self._stream_connection_epochs[key] = epoch_number
            return key, tracker, self._stream_books.get(key)

        if tracker.state.metadata_ref != product.metadata_ref:
            tracker, observation = tracker.rebind_metadata(
                instrument=product.key, metadata_ref=product.metadata_ref,
                epoch_id=self._connection_epoch_id(epoch_number), observed_at_ns=available_at_ns,
            )
            self._apply_stream_observation(repository, tracker, observation)
            book = (
                SequenceValidBookV2(
                    instrument=product.key, source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1,
                    channel=channel, sequence_semantics="BYBIT_U",
                ) if channel.startswith("orderbook.") else None
            )
            if book is not None:
                book.reconnect(available_at_ns)
            self._stream_trackers[key] = tracker
            self._stream_books[key] = book
            self._stream_connection_epochs[key] = epoch_number
            return key, tracker, book

        active_number = self._stream_connection_epochs.get(key)
        if epoch_number is not None and active_number != epoch_number:
            if key in self._stream_first_connection_allowed and tracker.state.last_available_at_ns is None:
                tracker = PublicStreamContinuityTrackerV1(
                    instrument=product.key, source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1,
                    channel=channel, metadata_ref=product.metadata_ref,
                    epoch_id=self._connection_epoch_id(epoch_number),
                )
                self._stream_trackers[key] = tracker
                self._stream_first_connection_allowed.discard(key)
            else:
                transition = PublicStreamObservationV1.transport(
                    instrument=product.key, source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1, channel=channel,
                    metadata_ref=product.metadata_ref, epoch_id=self._connection_epoch_id(epoch_number),
                    kind=PublicStreamObservationKindV1.RECONNECT,
                    observed_at_ns=max(available_at_ns, tracker.state.last_available_at_ns or 0),
                )
                self._apply_stream_observation(repository, tracker, transition)
                book = self._stream_books.get(key)
                if book is not None:
                    book.reconnect(max(available_at_ns, tracker.state.last_available_at_ns or available_at_ns))
            self._stream_connection_epochs[key] = epoch_number
        return key, tracker, self._stream_books.get(key)

    def _collect_public_stream_evidence(self, repository: OpsRepository, *, now_ns: int,
                                        publish_report: bool = True) -> None:
        if self.public_stream_source is None or self._stream_archive is None:
            return
        if self._stream_ingestion_failed:
            raise RuntimeError("PUBLIC_STREAM_WRITER_RESTART_REQUIRED")
        from ..data.public_transport_archive import archive_transport_batch

        started = time.monotonic_ns()
        try:
            # Amortize archive/checkpoint cost when there is real backlog. Each
            # handoff drain retains its old 32-frame bound and FIFO ordering.
            stream_status = self.public_stream_source.status()
            queued = getattr(stream_status, "pending_frames", getattr(stream_status.handoff, "queue_items", 0))
            sealed_drain = getattr(self.public_stream_source, "drain_sealed_transport", None)
            drain_count = (min(PUBLIC_STREAM_MAX_FRAMES_PER_SERVICE_V1 // PUBLIC_STREAM_MAX_FRAMES_PER_CYCLE_V1,
                (queued + PUBLIC_STREAM_MAX_FRAMES_PER_CYCLE_V1 - 1) // PUBLIC_STREAM_MAX_FRAMES_PER_CYCLE_V1)
                if type(queued) is int and queued >= 64 else 1)
            incoming: list[CapturedPublicFrameV2] = []
            for _ in range(0 if callable(sealed_drain) else drain_count):
                batch = tuple(self.public_stream_source.drain(max_items=PUBLIC_STREAM_MAX_FRAMES_PER_CYCLE_V1))
                if len(batch) > PUBLIC_STREAM_MAX_FRAMES_PER_CYCLE_V1 or any(
                        not isinstance(frame, CapturedPublicFrameV2) for frame in batch):
                    raise ValueError("public stream source returned a frame batch outside the controller bound")
                incoming.extend(batch)
                if len(batch) < PUBLIC_STREAM_MAX_FRAMES_PER_CYCLE_V1:
                    break
            frames = tuple(incoming)
            # Commit exact transport bytes before interpretation. Unbound and
            # failed interpretation remain reconstructable after restart.
            # Extent descriptor and batch binding are one durable publication.
            # Committing each separately adds a FULL WAL sync without making
            # the transport more reproducible. Keep this commit before typed
            # interpretation, so a failed typed batch retains exact raw FIFO.
            with repository.atomic_composition():
                if callable(sealed_drain):
                    sealed = sealed_drain()
                    if sealed is not None:
                        frames = sealed.adopt(repository)
                        self._stream_transport_ref = sealed.batch.artifact_ref
                else:
                    self._stream_transport_ref = archive_transport_batch(
                        repository, frames, clock_ns=self.clock_ns, floor_ns=now_ns, compact=True)
            # One bounded batch shares a commit; individual immutable writes
            # retain their existing savepoint validation. A failed batch cannot
            # reuse mutated in-memory continuity state after rollback.
            with repository.atomic_composition():
                self._persist_public_stream_batch(repository, now_ns=now_ns, frames=frames,
                                                  publish_report=publish_report)
        except Exception:
            self._stream_ingestion_failed = True
            if self.public_stream_source is not None:
                self.public_stream_source.close()
            raise
        finally:
            self._stream_last_service_duration_ns = time.monotonic_ns() - started
            self._stream_max_service_duration_ns = max(
                self._stream_max_service_duration_ns, self._stream_last_service_duration_ns)
        at = sample(self.clock_ns, floor_ns=now_ns)
        if self._stream_last_service_at_ns is not None:
            self._stream_max_service_gap_ns = max(
                self._stream_max_service_gap_ns, at - self._stream_last_service_at_ns)
        self._stream_last_service_at_ns = at
        self._stream_service_frames += len(frames)
        self._stream_service_calls += 1

    def _persist_public_stream_batch(self, repository: OpsRepository, *, now_ns: int,
                                     frames: tuple[CapturedPublicFrameV2, ...],
                                     publish_report: bool = True) -> None:
        if self.public_stream_source is None or self._stream_archive is None:
            return
        collector = cast(ProductionCollectorRecoveryV1, self._collector_recovery).collector
        drain = getattr(self.public_stream_source, "drain", None)
        get_status = getattr(self.public_stream_source, "status", None)
        if not callable(drain) or not callable(get_status):
            raise ValueError("opt-in public stream source must expose bounded drain and status")
        status = get_status()
        prior_recoveries = {key: (tracker.state.current_recovery_ref, tracker.state.gap_count)
                            for key, tracker in self._stream_trackers.items()}
        attempt_count = getattr(status, "attempt_count", 0)
        if type(attempt_count) is not int or attempt_count < 0:
            attempt_count = 0
        handoff = getattr(status, "handoff", None)
        if handoff is None:
            raise ValueError("public stream source status is missing its bounded handoff report")
        disconnect_count = getattr(handoff, "disconnect_count", 0)
        disconnect_at = getattr(handoff, "last_disconnect_at_ns", None)
        disconnect_changed = type(disconnect_count) is int and disconnect_count > self._stream_disconnect_count
        self._stream_disconnect_seen.clear()
        ingested_at_ns = max(
            now_ns, timestamp(self.clock_ns(), field="public stream ingestion clock"),
            max((frame.available_at_ns for frame in frames), default=now_ns),
            getattr(handoff, "last_activity_at_ns", None) or 0,
        )
        registry = collector.registry
        raw_archive_groups: dict[tuple[str, str, str], list[L2RawFrameV2]] = {}
        batch_health: dict[tuple[str, str, str, int | None], tuple[PublicSourceHealthV2, str, ArtifactIndexEntryV2]] = {}
        for original_frame in frames:
            topic_identity = self._stream_topic_identity(original_frame.channel)
            if (original_frame.venue.value != "BYBIT" or original_frame.source_id != BYBIT_PUBLIC_WS_SOURCE_ID_V1
                    or topic_identity is None):
                self._persist_unbound_stream_frame(repository, original_frame, ingested_at_ns,
                                                   reason="FRAME_SOURCE_OR_TOPIC_OUTSIDE_S32_SCOPE")
                continue
            symbol, channel = topic_identity
            product = self._latest_stream_product(registry, symbol, as_of_ns=original_frame.received_at_ns)
            if product is None:
                self._persist_unbound_stream_frame(repository, original_frame, ingested_at_ns,
                                                   reason="POINT_IN_TIME_METADATA_UNAVAILABLE")
                continue
            epoch_number = original_frame.connection_epoch if original_frame.connection_epoch is not None else attempt_count
            prior_feed_key = self._stream_feed_key(product.key, BYBIT_PUBLIC_WS_SOURCE_ID_V1, channel)
            prior_tracker = self._stream_trackers.get(prior_feed_key)
            if (prior_tracker is not None and disconnect_changed
                    and prior_feed_key not in self._stream_disconnect_seen
                    and epoch_number != self._stream_connection_epochs.get(prior_feed_key)
                    and disconnect_at is not None and original_frame.received_at_ns >= disconnect_at):
                disconnect_observation = PublicStreamObservationV1.transport(
                    instrument=product.key, source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1, channel=channel,
                    metadata_ref=product.metadata_ref, epoch_id=prior_tracker.state.epoch_id,
                    kind=PublicStreamObservationKindV1.DISCONNECT,
                    observed_at_ns=max(disconnect_at, prior_tracker.state.last_available_at_ns or 0),
                    available_at_ns=ingested_at_ns,
                )
                self._apply_stream_observation(repository, prior_tracker, disconnect_observation)
                prior_book = self._stream_books.get(prior_feed_key)
                if prior_book is not None:
                    prior_book.disconnect(disconnect_observation.available_at_ns)
                self._stream_disconnect_seen.add(prior_feed_key)
            feed_key, tracker, book = self._ensure_stream_tracker(
                repository, product, channel, epoch_number=epoch_number if epoch_number > 0 else None,
                available_at_ns=ingested_at_ns,
            )
            frame = replace(original_frame, available_at_ns=max(original_frame.available_at_ns, ingested_at_ns))
            first = self._stream_epoch_first_receipt.get(feed_key)
            if first is None or first[0] != epoch_number:
                self._stream_epoch_first_receipt[feed_key] = (epoch_number, original_frame.received_at_ns)
            health_key = (product.key.content_hash, product.metadata_ref, channel, frame.connection_epoch)
            cached_health = batch_health.get(health_key)
            if cached_health is None:
                cached_health = self._stream_health_for_frame(
                    repository, tracker, product, frame, status=status, handoff=handoff,
                    attempt_count=attempt_count, available_at_ns=ingested_at_ns,
                )
                batch_health[health_key] = cached_health
            health, health_epoch_id, _ = cached_health
            event: L2SnapshotV2 | L2DeltaV2 | L2SequenceFaultV2 | None = None
            parsed_trades: tuple[Any, ...] = ()
            trade_rows: list[Mapping[str, Any]] = []
            parse_failed = False
            try:
                if channel.startswith("orderbook."):
                    event = parse_bybit_orderbook_frame(
                        frame, instrument=product.key, declared_depth=50,
                        source_health=health.state.value, source_health_ref=health.content_hash,
                        processed_at_ns=ingested_at_ns,
                    )
                else:
                    payload = json.loads(frame.raw_payload_bytes)
                    rows = payload.get("data") if isinstance(payload, Mapping) else None
                    if not isinstance(rows, list) or len(rows) > PUBLIC_STREAM_MAX_TRADES_PER_FRAME_V1:
                        raise ValueError("TRADE_FRAME_ROW_LIMIT_OR_SHAPE")
                    if any(not isinstance(row, Mapping) for row in rows):
                        raise ValueError("TRADE_FRAME_ROW_SHAPE")
                    trade_rows = list(rows)
                    parsed_trades = parse_bybit_trades(
                        frame, instrument=product.key, source_health=health.state.value,
                        source_health_ref=health.content_hash, processed_at_ns=ingested_at_ns,
                    )
                    if len(parsed_trades) != len(trade_rows):
                        raise ValueError("TRADE_TRANSLATION_COUNT_MISMATCH")
            except (ArithmeticError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                parse_failed = True
                malformed = PublicStreamObservationV1.transport(
                    instrument=product.key, source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1, channel=channel,
                    metadata_ref=product.metadata_ref, epoch_id=tracker.state.epoch_id,
                    kind=PublicStreamObservationKindV1.MALFORMED_FRAME,
                    observed_at_ns=ingested_at_ns,
                    reason_code=(str(exc) if str(exc).replace("_", "").isalnum()
                                 and len(str(exc)) <= 80 else "MALFORMED_PUBLIC_FRAME_UNPARSEABLE"),
                    source_health_ref=health.content_hash,
                    source_health_epoch_id=health_epoch_id,
                )
                self._apply_stream_observation(repository, tracker, malformed)
                event = None
                parsed_trades = ()
                trade_rows = []

            # A parsed book/trade event carries the same actual receipt and
            # advances transport freshness. Avoiding a second FRAME_RECEIVED
            # state transition halves the hot-path continuity hashing cost.
            if (not parse_failed and event is None and not parsed_trades
                    and channel.startswith("publicTrade.")):
                frame_observation = PublicStreamObservationV1.from_frame(
                    frame, instrument=product.key, metadata_ref=product.metadata_ref,
                    epoch_id=tracker.state.epoch_id, source_health_ref=health.content_hash,
                    source_health_epoch_id=health_epoch_id, persisted_at_ns=ingested_at_ns,
                )
                self._apply_stream_observation(repository, tracker, frame_observation)

            if isinstance(event, L2SequenceFaultV2):
                tracker_observation = PublicStreamObservationV1.from_book_event(
                    event, metadata_ref=product.metadata_ref, epoch_id=tracker.state.epoch_id,
                    persisted_at_ns=ingested_at_ns,
                )
                self._apply_stream_observation(repository, tracker, tracker_observation)
                if book is not None:
                    book.apply_fault(event)
                raw_frame = raw_archive_record(
                    frame, instrument=product.key, frame_type="SEQUENCE_FAULT",
                    sequence_semantics="BYBIT_U", event_at_ns=event.event_at_ns,
                    source_health=event.source_health, source_health_ref=event.source_health_ref,
                )
            elif isinstance(event, (L2SnapshotV2, L2DeltaV2)):
                tracker_observation = PublicStreamObservationV1.from_book_event(
                    event, metadata_ref=product.metadata_ref, epoch_id=tracker.state.epoch_id,
                    persisted_at_ns=ingested_at_ns,
                )
                self._apply_stream_observation(repository, tracker, tracker_observation)
                if book is not None:
                    if isinstance(event, L2SnapshotV2):
                        book.apply_snapshot(event)
                    else:
                        book.apply_delta(event)
                raw_frame = raw_archive_record(
                    frame, instrument=product.key,
                    frame_type="SNAPSHOT" if isinstance(event, L2SnapshotV2) else "DELTA",
                    sequence_semantics="BYBIT_U", last_update_id=event.last_update_id,
                    event_at_ns=event.event_at_ns, source_health=event.source_health,
                    source_health_ref=event.source_health_ref,
                )
            elif channel.startswith("publicTrade."):
                trade_event_times = [trade.event_at_ns for trade in parsed_trades if trade.event_at_ns is not None]
                raw_frame = raw_archive_record(
                    frame, instrument=product.key,
                    frame_type=f"TRADE_FRAME_{frame.raw_payload_hash[:16]}",
                    sequence_semantics="BYBIT_TRADE_ID_IS_IDENTITY_NOT_REPLAY_CURSOR",
                    event_at_ns=max(trade_event_times, default=None),
                    source_health=health.state.value, source_health_ref=health.content_hash,
                )
                for trade, row in zip(parsed_trades, trade_rows, strict=True):
                    if not trade.trade_id:
                        malformed_trade = PublicStreamObservationV1.transport(
                            instrument=product.key, source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1, channel=channel,
                            metadata_ref=product.metadata_ref, epoch_id=tracker.state.epoch_id,
                            kind=PublicStreamObservationKindV1.MALFORMED_FRAME,
                            observed_at_ns=ingested_at_ns, reason_code="BYBIT_TRADE_ID_MISSING",
                        )
                        self._apply_stream_observation(repository, tracker, malformed_trade)
                        continue
                    payload_bytes = canonical_json(row).encode("utf-8")
                    raw_observation = RawObservationV2.build(
                        instrument_revision=product.key.contract_revision,
                        source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1, event_type="TRADE",
                        event_at_ns=trade.event_at_ns, published_at_ns=None,
                        received_at_ns=frame.received_at_ns, ingested_at_ns=ingested_at_ns,
                        available_at_ns=max(frame.available_at_ns, ingested_at_ns),
                        translation_version="bybit-public-ws-trade-v1", payload=payload_bytes,
                        quality_flags=("TRADE_COMPLETENESS_UNPROVEN",),
                        sequence=str(trade.trade_id),
                    )
                    observation = PublicStreamObservationV1.from_trade(
                        trade, metadata_ref=product.metadata_ref, epoch_id=tracker.state.epoch_id,
                        persisted_at_ns=ingested_at_ns, exact_trade_payload_hash=raw_observation.raw_payload_hash,
                    )
                    index_ref = sha256_json({
                        "artifact_type": "PublicStreamTradeObservationIndexV1",
                        "record_id": raw_observation.record_id,
                    })
                    prior_entry = repository.get_artifact(index_ref)
                    prior_observation = collector.store.get(raw_observation.record_id)
                    durable_hash = (
                        str(prior_entry.metadata.get("raw_payload_hash"))
                        if prior_entry is not None and isinstance(prior_entry.metadata.get("raw_payload_hash"), str)
                        else prior_observation.raw_payload_hash if prior_observation is not None else None
                    )
                    self._apply_stream_observation(
                        repository, tracker, observation,
                        durable_prior_payload_hash=durable_hash,
                        durable_lookup_complete=True,
                    )
                    collector.ingest(
                        raw_observation, raw_payload=payload_bytes, instrument_key=product.key,
                        update_source_health=False, index_as_public_observation=False,
                        retain_in_memory=False,
                    )
            else:
                raw_frame = raw_archive_record(
                    frame, instrument=product.key,
                    frame_type=f"MALFORMED_FRAME_{frame.raw_payload_hash[:16]}",
                    sequence_semantics=("BYBIT_U" if channel.startswith("orderbook.")
                                        else "BYBIT_TRADE_ID_IS_IDENTITY_NOT_REPLAY_CURSOR"),
                    source_health="INCOMPLETE_SNAPSHOT",
                )
            raw_archive_groups.setdefault(feed_key, []).append(raw_frame)

        if batch_health:
            import pyarrow as pa

            health_entries = tuple(value[2] for value in batch_health.values())
            health_rows = [{"artifact_ref": entry.artifact_ref, "content_hash": entry.content_hash,
                            "created_at_ns": entry.created_at_ns, "available_at_ns": entry.available_at_ns,
                            "metadata_json": canonical_json(entry.metadata)} for entry in health_entries]
            health_chunk = sha256_json(health_rows)
            write_extent(repository, pa.Table.from_pylist(health_rows), namespace="ops-stream-metadata",
                         chunk_id=health_chunk, clock_ns=self.clock_ns, floor_ns=ingested_at_ns)
            repository.register_public_archive_entries(health_entries, archive_chunk_id=health_chunk)
        archive_refs = self._write_stream_frame_archives(repository, raw_archive_groups)
        for key in raw_archive_groups:
            if key[2].startswith("orderbook.") and self._stream_books.get(key) is not None and self._stream_transport_ref is not None:
                pending_transports = self._stream_pending_book_transports.setdefault(key, [])
                pending_transports.append(self._stream_transport_ref)
                if len(pending_transports) >= MAX_CHUNKS_PER_CHECKPOINT:
                    publish_report = True
                tracker = self._stream_trackers[key]
                health_refs = self._stream_pending_book_health.setdefault(key, [])
                for health_key, (frame_health, _, _) in batch_health.items():
                    if health_key[:3] == (tracker.state.instrument.content_hash, tracker.state.metadata_ref,
                                          tracker.state.channel) and frame_health.content_hash not in health_refs:
                        health_refs.append(frame_health.content_hash)
                if len(health_refs) > 128:
                    raise ValueError(f"book lineage health population exceeded its bound: {len(health_refs)}")
                if len(health_refs) >= 64:
                    publish_report = True
        for key, ref in archive_refs.items():
            if key[2].startswith("orderbook.") and self._stream_books.get(key) is not None:
                pending = self._stream_pending_book_archives.setdefault(key, [])
                pending.append(ref)
                if len(pending) >= MAX_CHUNKS_PER_CHECKPOINT:
                    publish_report = True
        for book in self._stream_books.values():
            if book is not None:
                book.compact_live_state(as_of_ns=ingested_at_ns)
        collector.flush_archive()
        # Transport can change while a batch is being archived. Reports must
        # observe that change rather than publish the pre-work status as current.
        status = get_status()
        handoff = status.handoff
        attempt_count = getattr(status, "attempt_count", 0)
        status_epoch = self._connection_epoch_id(attempt_count if attempt_count > 0 else None)
        overflowed = bool(getattr(handoff, "overflowed", False))
        connected = bool(getattr(handoff, "connected", False)) and getattr(status, "state", None) == "RUNNING"
        last_error = getattr(status, "last_error_code", None) or getattr(handoff, "last_error_code", None)
        all_keys = tuple(self._stream_trackers)
        for key in all_keys:
            tracker = self._stream_trackers[key]
            instrument = tracker.state.instrument
            channel = tracker.state.channel
            if disconnect_changed and key not in self._stream_disconnect_seen:
                has_current_epoch_frame = any(
                    frame.connection_epoch == attempt_count and frame.channel == channel
                    and (topic_identity := self._stream_topic_identity(frame.channel)) is not None
                    and topic_identity[0] == instrument.native_symbol
                    for frame in frames
                )
                if not has_current_epoch_frame or not connected:
                    disconnect_observation = PublicStreamObservationV1.transport(
                        instrument=instrument, source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1, channel=channel,
                        metadata_ref=tracker.state.metadata_ref, epoch_id=tracker.state.epoch_id,
                        kind=PublicStreamObservationKindV1.DISCONNECT,
                        observed_at_ns=max(disconnect_at or ingested_at_ns,
                                           tracker.state.last_available_at_ns or 0),
                        available_at_ns=max(ingested_at_ns, disconnect_at or 0),
                    )
                    self._apply_stream_observation(repository, tracker, disconnect_observation)
                    book = self._stream_books.get(key)
                    if book is not None:
                        book.disconnect(disconnect_observation.observed_at_ns)
                self._stream_disconnect_seen.add(key)
            if overflowed and not self._stream_overflow_seen:
                overflow_observation = PublicStreamObservationV1.transport(
                    instrument=instrument, source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1, channel=channel,
                    metadata_ref=tracker.state.metadata_ref, epoch_id=tracker.state.epoch_id,
                    kind=PublicStreamObservationKindV1.QUEUE_OVERFLOW,
                    observed_at_ns=max(ingested_at_ns, tracker.state.last_available_at_ns or 0),
                )
                self._apply_stream_observation(repository, tracker, overflow_observation)
                book = self._stream_books.get(key)
                if book is not None:
                    book.disconnect(overflow_observation.observed_at_ns)
            error_at = getattr(handoff, "last_error_at_ns", None)
            first_receipt = self._stream_epoch_first_receipt.get(key)
            error_in_active_epoch = (not connected or (type(error_at) is int
                and first_receipt is not None and error_at >= first_receipt[1]))
            if (last_error and last_error != self._stream_last_error_code and not overflowed
                    and error_in_active_epoch):
                error_observation = PublicStreamObservationV1.transport(
                    instrument=instrument, source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1, channel=channel,
                    metadata_ref=tracker.state.metadata_ref, epoch_id=tracker.state.epoch_id,
                    kind=PublicStreamObservationKindV1.CONNECTION_ERROR,
                    observed_at_ns=max(ingested_at_ns, tracker.state.last_available_at_ns or 0),
                    reason_code=(last_error if last_error.replace("_", "").isalnum()
                                 and len(last_error) <= 80 else "PUBLIC_STREAM_CONNECTION_ERROR"),
                )
                self._apply_stream_observation(repository, tracker, error_observation)
                book = self._stream_books.get(key)
                if book is not None:
                    book.disconnect(error_observation.observed_at_ns)
        self._stream_disconnect_count = disconnect_count if type(disconnect_count) is int else self._stream_disconnect_count
        self._stream_overflow_seen = self._stream_overflow_seen or overflowed
        self._stream_last_error_code = last_error

        # Raw evidence commits every batch; compact state/health checkpoints
        # publish at least once per second and immediately on a fault/recovery.
        # Restart still starts a new recovery epoch, never reuses a warm book.
        changed_recovery = any(prior_recoveries.get(key) != (
            tracker.state.current_recovery_ref, tracker.state.gap_count)
            for key, tracker in self._stream_trackers.items())
        if any(len(refs) >= 128 for refs in self._stream_pending_book_controls.values()):
            publish_report = True
        if not publish_report and not changed_recovery and not overflowed and not disconnect_changed:
            return

        report_as_of = max(ingested_at_ns, now_ns, timestamp(self.clock_ns(), field="stream report clock"))
        self._stream_last_report_at_ns = report_as_of
        for key, tracker in tuple(self._stream_trackers.items()):
            product = self._latest_stream_product(registry, tracker.state.instrument.native_symbol,
                                                  as_of_ns=report_as_of)
            channel = tracker.state.channel
            active_epoch_matches = tracker.state.epoch_id == status_epoch
            recent_receipt = bool(
                tracker.state.last_transport_receipt_at_ns is not None
                and tracker.state.last_transport_receipt_at_ns <= report_as_of
                and report_as_of - tracker.state.last_transport_receipt_at_ns <= PUBLIC_STREAM_STALE_NS_V1
            )
            if overflowed:
                state = PublicSourceStateV2.INCOMPLETE_SNAPSHOT
                details = "bounded WebSocket queue overflowed; observed local loss"
            elif not connected:
                state = PublicSourceStateV2.DISCONNECTED
                details = "public WebSocket is not connected at the controller cutoff"
            elif not active_epoch_matches or not recent_receipt:
                state = PublicSourceStateV2.STALE
                details = "channel receipt is absent, stale, or belongs to a different connection epoch"
            else:
                state = PublicSourceStateV2.HEALTHY_CURRENT
                details = "active public WebSocket epoch and channel receipt passed the local freshness bound"
            health_observed = max(
                now_ns,
                tracker.state.last_transport_receipt_at_ns or 0,
                getattr(handoff, "last_activity_at_ns", None) or 0,
            )
            health_body = {
                "version": "PUBLIC_STREAM_SOURCE_HEALTH_V1",
                "source_id": BYBIT_PUBLIC_WS_SOURCE_ID_V1,
                "instrument": tracker.state.instrument.to_dict(),
                "channel": channel,
                "metadata_ref": tracker.state.metadata_ref,
                "epoch_id": tracker.state.epoch_id,
                "observed_at_ns": health_observed,
                "available_at_ns": report_as_of,
                "state": state.value,
                "details": details,
                "connection_attempt": attempt_count,
                "reconnect_count": getattr(status, "reconnect_count", 0),
                "disconnect_count": disconnect_count,
                "last_disconnect_at_ns": disconnect_at,
                "last_heartbeat_at_ns": getattr(handoff, "last_heartbeat_at_ns", None),
                "queue_items": getattr(handoff, "queue_items", 0),
                "queue_bytes": getattr(handoff, "queue_bytes", 0),
                "queue_capacity_items": getattr(handoff, "max_queue_items", 0),
                "queue_capacity_bytes": getattr(handoff, "max_queue_bytes", 0),
                "high_water_items": getattr(handoff, "high_water_items", 0),
                "high_water_bytes": getattr(handoff, "high_water_bytes", 0),
                "overflowed": overflowed,
                "closed_rejections": getattr(handoff, "closed_rejections", 0),
                "backpressure_observed": bool(getattr(handoff, "backpressure", False)),
                "producer_state": str(getattr(status, "state", "UNKNOWN")),
                "last_error_code": last_error,
                "reason_codes": sorted(self._stream_metadata_errors),
                "service_version": "PUBLIC_STREAM_WRITER_SERVICE_V1",
                "service_frames": self._stream_service_frames,
                "service_calls": self._stream_service_calls,
                "max_service_gap_ns": self._stream_max_service_gap_ns,
                "max_service_duration_ns": self._stream_max_service_duration_ns,
                "transport_batch_ref": self._stream_transport_ref,
                **({"capture": status.capture} if hasattr(status, "capture") else {}),
            }
            health = PublicSourceHealthV2(
                BYBIT_PUBLIC_WS_SOURCE_ID_V1, health_observed, report_as_of, state,
                sha256_json(health_body), details,
            )
            repository.register_artifact(ArtifactIndexEntryV2(
                health.content_hash, "PublicStreamSourceHealthV1", health.content_hash,
                report_as_of, report_as_of, {"health": health.to_dict(), "transport": health_body},
            ))
            book = self._stream_books.get(key)
            book_ref = None
            if book is not None and channel.startswith("orderbook."):
                book_ref = persist_book_checkpoint(
                    repository, book, epoch_id=tracker.state.epoch_id,
                    metadata_ref=tracker.state.metadata_ref, as_of_ns=report_as_of,
                    prior_ref=self._stream_book_checkpoint_refs.get(key),
                    archive_refs=tuple(self._stream_pending_book_archives.get(key, ())),
                    transport_refs=tuple(self._stream_pending_book_transports.get(key, ())),
                    frame_health_refs=tuple(self._stream_pending_book_health.get(key, ())),
                    control_refs=tuple(self._stream_pending_book_controls.get(key, ())), clock_ns=self.clock_ns)
                self._stream_book_checkpoint_refs[key] = book_ref
                self._stream_pending_book_archives[key] = []
                self._stream_pending_book_transports[key] = []
                self._stream_pending_book_health[key] = []
                self._stream_pending_book_controls[key] = []
            report_started = max(report_as_of, sample(self.clock_ns, floor_ns=report_as_of))
            report = build_public_stream_continuity_report(
                tracker, as_of_ns=report_as_of, source_health=health,
                source_health_epoch_id=status_epoch if active_epoch_matches else None,
                metadata=product, max_source_health_age_ns=PUBLIC_STREAM_STALE_NS_V1,
                max_metadata_age_ns=PUBLIC_STREAM_METADATA_MAX_AGE_NS_V1,
                book=book if channel.startswith("orderbook.") else None,
                book_metadata_ref=tracker.state.metadata_ref if book is not None else None,
                book_lineage_ref=book_ref,
            )
            # Bounded immutable heads for the DB-free operator watchdog. This
            # does not replace the durable continuity evidence above/below.
            self._owner_stream_reports = {**self._owner_stream_reports, channel: report}
            state_snapshot = tracker.to_state()
            state_ref = persist_continuity_checkpoint(
                repository, state_snapshot, available_at_ns=report_as_of,
                prior_ref=self._stream_checkpoint_refs.get(key), transport_ref=self._stream_transport_ref,
                clock_ns=self.clock_ns)
            self._stream_checkpoint_refs[key] = state_ref
            report_finished = sample(self.clock_ns, floor_ns=report_started)
            repository.register_artifact(ArtifactIndexEntryV2(
                report.content_hash, "PublicStreamContinuityReportV1", report.content_hash,
                report_started, report_finished,
                {"report": report.to_dict(), "state_ref": state_ref,
                 "source_health_ref": health.content_hash,
                 "storage_version": "PUBLIC_CONTINUITY_REPORT_INDEX_V2",
                 "transport_ref": health.content_hash, "computation_started_ns": report_started,
                 "computation_finished_ns": report_finished},
            ))
        for symbol in self._stream_products:
            product = self._stream_products[symbol]
            if self._latest_stream_product(registry, symbol, as_of_ns=report_as_of) is None:
                self._persist_metadata_gate(repository, product, report_as_of,
                                            "NO_POINT_IN_TIME_METADATA_FOR_ACTIVE_CUTOFF")

    def _stream_health_for_frame(
        self,
        repository: OpsRepository,
        tracker: PublicStreamContinuityTrackerV1,
        product: ProductContractV2,
        frame: CapturedPublicFrameV2,
        *,
        status: Any,
        handoff: Any,
        attempt_count: int,
        available_at_ns: int,
    ) -> tuple[PublicSourceHealthV2, str, ArtifactIndexEntryV2]:
        frame_epoch_id = self._connection_epoch_id(frame.connection_epoch or attempt_count or None)
        active = bool(
            getattr(handoff, "connected", False)
            and getattr(status, "state", None) == "RUNNING"
            and not getattr(handoff, "overflowed", False)
            and frame.connection_epoch is not None
            and frame.connection_epoch == attempt_count
        )
        health_state = PublicSourceStateV2.HEALTHY_CURRENT if active else PublicSourceStateV2.INCOMPLETE_SNAPSHOT
        details = ("frame belongs to the active connection attempt" if active
                   else "frame connection epoch is not proven to be the active healthy attempt")
        body = {
            "version": "PUBLIC_STREAM_BATCH_FRAME_HEALTH_V2", "source_id": BYBIT_PUBLIC_WS_SOURCE_ID_V1,
            "instrument_hash": product.key.content_hash, "channel": frame.channel,
            "metadata_ref": product.metadata_ref, "epoch_id": frame_epoch_id,
            "observed_at_ns": frame.received_at_ns, "available_at_ns": available_at_ns,
            "state": health_state.value, "transport_batch_ref": self._stream_transport_ref,
            "connection_attempt": frame.connection_epoch,
            "active_attempt": attempt_count, "overflowed": bool(getattr(handoff, "overflowed", False)),
        }
        health = PublicSourceHealthV2(
            BYBIT_PUBLIC_WS_SOURCE_ID_V1, frame.received_at_ns, available_at_ns,
            health_state, sha256_json(body), details,
        )
        entry = ArtifactIndexEntryV2(
            health.content_hash, "PublicStreamSourceHealthV1", health.content_hash,
            available_at_ns, available_at_ns, {"health": health.to_dict(), "transport": body},
        )
        return health, frame_epoch_id, entry

    def _persist_unbound_stream_frame(
        self, repository: OpsRepository, frame: CapturedPublicFrameV2, available_at_ns: int, *, reason: str,
    ) -> None:
        available = max(available_at_ns, frame.available_at_ns)
        body = {
            "version": "PUBLIC_STREAM_UNBOUND_FRAME_V1", "source_id": frame.source_id,
            "venue": frame.venue.value, "channel": frame.channel,
            "raw_payload_hash": frame.raw_payload_hash,
            "received_at_ns": frame.received_at_ns, "available_at_ns": available,
            "connection_epoch": frame.connection_epoch, "reason_code": reason,
            "transport_batch_ref": self._stream_transport_ref,
            "authority": "ZERO",
        }
        ref = sha256_json(body)
        repository.register_artifact(ArtifactIndexEntryV2(
            ref, "PublicStreamUnboundFrameV1", ref, available, available, {"frame": body},
        ))

    def _persist_metadata_gate(
        self, repository: OpsRepository, product: ProductContractV2, at_ns: int, reason: str,
    ) -> None:
        available = max(at_ns, product.available_at_ns)
        body = {
            "version": "PUBLIC_STREAM_METADATA_GATE_V1", "symbol": product.key.native_symbol,
            "instrument": product.key.to_dict(), "metadata_ref": product.metadata_ref,
            "contract_revision": product.key.contract_revision, "status": product.trading_status.value,
            "observed_at_ns": product.observed_at_ns, "available_at_ns": available,
            "reason_code": reason, "qualification_status": "TEST GATE", "authority": "ZERO",
        }
        ref = sha256_json(body)
        repository.register_artifact(ArtifactIndexEntryV2(
            ref, "PublicStreamMetadataGateV1", ref, available, available, {"gate": body},
        ))

    def _write_stream_frame_archives(
        self,
        repository: OpsRepository,
        groups: Mapping[tuple[str, str, str], list[L2RawFrameV2]],
    ) -> dict[tuple[str, str, str], str]:
        checkpoints: dict[tuple[str, str, str], str] = {}
        if self._stream_archive is None:
            return checkpoints
        for _feed_key, incoming_frames in groups.items():
            unique: dict[str, L2RawFrameV2] = {}
            index_refs = {frame.record_id: sha256_json({"artifact_type": "PublicStreamFrameIndexV1",
                                                       "record_id": frame.record_id})
                          for frame in incoming_frames}
            stored_rows = repository.get_artifact_metadata_by_refs(tuple(index_refs.values()))
            for frame in incoming_frames:
                local_prior = unique.get(frame.record_id)
                index_ref = index_refs[frame.record_id]
                stored = stored_rows.get(index_ref)
                if stored is not None and (stored.get("artifact_type") != "PublicStreamFrameIndexV1"
                                           or not isinstance(stored.get("metadata"), Mapping)):
                    raise ValueError("public stream frame index identity conflict")
                prior_hash = (str(stored["metadata"].get("raw_payload_hash"))
                              if stored is not None else None)
                prior_frame: L2RawFrameV2 | None = local_prior
                if prior_frame is None and stored is not None and prior_hash != frame.raw_payload_hash:
                    chunk_id = stored["metadata"].get("archive_chunk_id")
                    if isinstance(chunk_id, str):
                        for row in self._stream_archive.read_chunk(chunk_id):
                            if row.get("record_id") == frame.record_id:
                                prior_frame = _l2_raw_frame_from_archive_row(row)
                                break
                if local_prior is not None and local_prior.raw_payload_hash == frame.raw_payload_hash:
                    continue
                if stored is not None and prior_hash == frame.raw_payload_hash:
                    continue
                if prior_frame is not None and prior_frame.raw_payload_hash != frame.raw_payload_hash:
                    self._stream_archive.write_conflict_quarantine(prior_frame, frame)
                    conflict_available = max(prior_frame.available_at_ns, frame.available_at_ns)
                    conflict = {
                        "version": "PUBLIC_STREAM_FRAME_CONFLICT_V1",
                        "record_id": frame.record_id,
                        "existing_payload_hash": prior_frame.raw_payload_hash,
                        "incoming_payload_hash": frame.raw_payload_hash,
                        "instrument_hash": frame.instrument.content_hash,
                        "source_id": frame.source_id, "channel": frame.channel,
                        "available_at_ns": conflict_available,
                        "authority": "ZERO",
                    }
                    ref = sha256_json(conflict)
                    repository.register_artifact(ArtifactIndexEntryV2(
                        ref, "PublicStreamFrameConflictV1", ref, conflict_available,
                        conflict_available, {"conflict": conflict},
                    ))
                    continue
                unique[frame.record_id] = frame
            if not unique:
                continue
            chunk_id, _path = self._stream_archive.write_chunk(tuple(unique.values()))
            checkpoints[_feed_key] = self._stream_archive.checkpoint_ref(chunk_id)
            entries = []
            for frame in unique.values():
                index_ref = sha256_json({"artifact_type": "PublicStreamFrameIndexV1",
                                         "record_id": frame.record_id})
                index_body = {
                    "record_id": frame.record_id,
                    "instrument": frame.instrument.to_dict(),
                    "instrument_hash": frame.instrument.content_hash,
                    "source_id": frame.source_id, "channel": frame.channel,
                    "frame_type": frame.frame_type,
                    "event_at_ns": frame.event_at_ns,
                    "received_at_ns": frame.received_at_ns,
                    "available_at_ns": frame.available_at_ns,
                    "raw_payload_hash": frame.raw_payload_hash,
                    "archive_chunk_id": chunk_id,
                    "sequence_semantics": frame.sequence_semantics,
                    "authority": "ZERO",
                }
                index_hash = sha256_json(index_body)
                entries.append(ArtifactIndexEntryV2(
                    index_ref, "PublicStreamFrameIndexV1", index_hash,
                    frame.available_at_ns, frame.available_at_ns, index_body,
                ))
            repository.register_artifacts(entries)
        return checkpoints

    @staticmethod
    def _public_source_states_as_of(
        collector: PublicCollectorV2,
        source_ids: tuple[str, ...],
        *,
        now_ns: int,
    ) -> tuple[OpsSourceStateV1, ...]:
        result: list[OpsSourceStateV1] = []
        for source_id in source_ids:
            eligible = [
                item for item in collector.health.history(source_id)
                if item.available_at_ns <= now_ns and item.observed_at_ns <= now_ns
            ]
            current = max(eligible, key=lambda item: (item.observed_at_ns, item.content_hash), default=None)
            result.append(OpsSourceStateV1(
                source_id,
                current.state.value if current is not None else "UNKNOWN",
                current.observed_at_ns if current is not None else None,
                current.available_at_ns if current is not None else None,
            ))
        return tuple(result)

    @staticmethod
    def _expired_public_events(
        repository: OpsRepository, *, now_ns: int,
    ) -> tuple[OpsDecisionEventV1, ...]:
        """Expose exact durable expiries even while current source health is closed."""
        result: list[OpsDecisionEventV1] = []
        for entry in repository.pending_decision_event_page(as_of_ns=now_ns, limit=64).entries:
            body = entry.metadata.get("event")
            if entry.available_at_ns > now_ns or not isinstance(body, Mapping):
                continue
            try:
                event = decision_event_from_dict(body)
            except (KeyError, TypeError, ValueError):
                continue
            if (event.deadline_ns >= now_ns or entry.artifact_ref != event.content_hash
                    or entry.content_hash != event.content_hash
                    or repository.get_artifact(_ops_receipt_identity_ref(event.event_id)) is not None):
                continue
            if any(
                (causal_entry := repository.get_artifact(ref)) is None
                or causal_entry.available_at_ns > event.information_cutoff_ns
                for ref in event.causal_input_refs
            ):
                continue
            result.append(event)
        return tuple(sorted(result, key=lambda event: (event.available_at_ns, event.information_cutoff_ns, event.event_id)))

    @staticmethod
    def _record_late_public_events(
        repository: OpsRepository,
        *,
        eligible_at_ns: int,
        completed_at_ns: int,
    ) -> tuple[str, ...]:
        late: list[OpsDecisionEventV1] = []
        for entry in repository.pending_decision_event_page(as_of_ns=eligible_at_ns, limit=64).entries:
            body = entry.metadata.get("event")
            if entry.available_at_ns > eligible_at_ns or not isinstance(body, Mapping):
                continue
            try:
                event = decision_event_from_dict(body)
            except (KeyError, TypeError, ValueError):
                continue
            if (completed_at_ns <= event.deadline_ns
                    or repository.get_artifact(_ops_receipt_identity_ref(event.event_id)) is not None):
                continue
            late.append(event)
        if not late:
            return ()
        late.sort(key=lambda event: (event.deadline_ns, event.event_id))
        body = {
            "version": "OPS_PUBLIC_ACQUISITION_DEADLINE_GATE_V1",
            "observed_at_ns": completed_at_ns,
            "eligible_cutoff_ns": eligible_at_ns,
            "event_ids": [event.event_id for event in late],
            "deadlines_ns": [event.deadline_ns for event in late],
            "status": "TEST GATE",
        }
        ref = sha256_json(body)
        repository.register_artifact(ArtifactIndexEntryV2(
            ref, "OpsPublicAcquisitionDeadlineGateV1", ref,
            completed_at_ns, completed_at_ns, {"deadline_gate": body},
        ))
        return tuple(event.event_id for event in late)

    def _persist_public_snapshot(self, repository: OpsRepository, snapshot: Any, *, now_ns: int) -> bool:
        """Controller-owned persistence and source-health reconciliation for bounded intake records."""
        if getattr(self.public_source, "enabled_venues", None) is not None:
            return self._persist_broad_public_snapshot(repository, snapshot, now_ns=now_ns)
        from ..data.bybit import SOURCE_ID
        from ..data.bybit_source import CAMPAIGN_INTERVALS

        collector = cast(ProductionCollectorRecoveryV1, self._collector_recovery).collector
        latest_receipt_ns = max(
            now_ns,
            snapshot.latest_received_at_ns,
            max((record.observation.received_at_ns for record in snapshot.records), default=now_ns),
        )
        ingestion_at_ns = max(
            latest_receipt_ns,
            snapshot.observed_at_ns,
            max((record.observation.event_at_ns or 0 for record in snapshot.records), default=0),
        )
        reconciliation_at_ns = ingestion_at_ns
        # This durable historical fact survives eviction of the recent health
        # projection and never clears unsupported exact trade completeness.
        recovery_required = collector.health.had_unhealthy_after_healthy(SOURCE_ID)

        prior_clock = collector.clock_ns
        collector.clock_ns = lambda: reconciliation_at_ns
        try:
            if snapshot.failure_kind == "RATE_LIMITED":
                collector.on_rate_limited(SOURCE_ID, at_ns=reconciliation_at_ns)
            elif snapshot.failure_kind == "DISCONNECTED":
                collector.on_disconnect(SOURCE_ID, at_ns=reconciliation_at_ns)

            ingestion_complete = snapshot.complete
            for record_number, record in enumerate(snapshot.records):
                if record_number % 16 == 0 and self._serviced_acquisition is not None:
                    # Restore the collector clock before using its stream lane.
                    collector.clock_ns = prior_clock
                    self.service_public_stream(repository)
                    ingestion_at_ns = sample(self.clock_ns, floor_ns=ingestion_at_ns)
                    reconciliation_at_ns = ingestion_at_ns
                    def fixed_reconciliation_clock(current: int = reconciliation_at_ns) -> int:
                        return current
                    collector.clock_ns = fixed_reconciliation_clock
                try:
                    # The HTTP adapter records exact receipt times. Collector
                    # validation/ingestion happens only after the complete bounded
                    # response is available, so same-cycle eligibility begins at
                    # this later, actual ingestion boundary.
                    observation = replace(
                        record.observation,
                        ingested_at_ns=max(record.observation.ingested_at_ns, ingestion_at_ns),
                        available_at_ns=max(
                            record.observation.available_at_ns,
                            ingestion_at_ns,
                            record.observation.event_at_ns or 0,
                        ),
                    )
                    bar = replace(record.bar, raw=observation) if record.bar is not None else None
                    result = collector.ingest(
                        observation, raw_payload=record.raw_payload,
                        instrument_key=record.instrument_key, bar=bar,
                    )
                    if result.persistent_conflict or result.append.status.value == "CONFLICT_QUARANTINED":
                        ingestion_complete = False
                except (ValueError, RuntimeError):
                    ingestion_complete = False
            collector.flush_archive()
        finally:
            collector.clock_ns = prior_clock

        by_symbol_events: dict[str, set[str]] = {symbol: set() for symbol in ("BTCUSDT", "ETHUSDT")}
        eligible_refs: set[str] = set()
        reconciled_snapshot_refs: set[str] = set()
        for record in snapshot.records:
            index_ref = sha256_json({
                "artifact_type": "PublicObservationIndexV2",
                "record_id": record.observation.record_id,
            })
            entry = repository.get_artifact(index_ref)
            if entry is not None and entry.available_at_ns <= reconciliation_at_ns:
                eligible_refs.add(index_ref)
                if record.observation.event_type != "TRADE":
                    reconciled_snapshot_refs.add(index_ref)
                by_symbol_events[record.instrument_key.native_symbol].add(record.observation.event_type)

        required_events = {"PRODUCT_METADATA", "TICKER_MARK_INDEX_FUNDING_OI"} | {
            f"BAR_{interval.value}" for interval in CAMPAIGN_INTERVALS
        }
        references_ready = bool(eligible_refs) and all(
            required_events.issubset(events) for events in by_symbol_events.values()
        )
        bar_repair_refs: set[str] = set()
        bar_gaps_repaired = self._public_snapshot_overlap_is_repaired(
            repository, snapshot, at_ns=reconciliation_at_ns, recovery_required=recovery_required,
            clock_ns=self.clock_ns, certificate_refs=bar_repair_refs,
        )
        reconciliation_at_ns = sample(self.clock_ns,floor_ns=reconciliation_at_ns)
        # A bounded recent-trades page has no cursor or historical backfill.
        # It can show observed trades but cannot prove that a recovery gap was
        # filled. This limitation must not prevent a separately proven repair
        # of confirmed bars and current metadata/ticker snapshots.
        trade_continuity_proven = False
        source_snapshot_reconciled = ingestion_complete and references_ready and bar_gaps_repaired
        if source_snapshot_reconciled:
            collector.reconcile_after_reconnect(
                SOURCE_ID, at_ns=reconciliation_at_ns, complete_snapshot=True, missed_interval_repaired=True,
                snapshot_refs=tuple(sorted(reconciled_snapshot_refs)),
                repair_certificate_refs=tuple(sorted(bar_repair_refs)),
                recovery_epoch_ref=collector.required_recovery_epoch_ref,
            )
        else:
            detail = snapshot.failure_reason or "BYBIT_PUBLIC_SNAPSHOT_UNRECONCILED_OR_CONFLICTED"
            collector.mark_incomplete_snapshot(SOURCE_ID, at_ns=reconciliation_at_ns, details=detail)

        trade_count = sum(record.observation.event_type == "TRADE" for record in snapshot.records)
        bar_records = [record for record in snapshot.records if record.bar is not None]
        trade_observation_refs = tuple(sorted({
            sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": record.observation.record_id})
            for record in snapshot.records if record.observation.event_type == "TRADE"
        }))
        bar_observation_refs = tuple(sorted({
            sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": record.observation.record_id})
            for record in bar_records
        }))
        bar_coverage: dict[str, int] = {}
        for record in bar_records:
            event_type = record.observation.event_type
            bar_coverage[event_type] = bar_coverage.get(event_type, 0) + 1
        recovery_evidence = {
            "version": "BYBIT_PUBLIC_RECOVERY_EVIDENCE_V1",
            "source_id": SOURCE_ID,
            "available_at_ns": reconciliation_at_ns,
            "endpoint_reachability": (
                "REACHABLE" if snapshot.successful_request_count else "UNKNOWN_OR_UNREACHABLE"
            ),
            "snapshot_complete": bool(snapshot.complete and ingestion_complete),
            "failure_kind": snapshot.failure_kind,
            "request_count": snapshot.request_count + snapshot.bootstrap_request_count,
            "successful_request_count": (snapshot.successful_request_count
                                          + snapshot.successful_bootstrap_request_count),
            "market_data_request_count": snapshot.request_count,
            "successful_market_data_request_count": snapshot.successful_request_count,
            "metadata_bootstrap_request_count": snapshot.bootstrap_request_count,
            "successful_metadata_bootstrap_request_count": snapshot.successful_bootstrap_request_count,
            "acquisition_duration_ns": snapshot.acquisition_duration_ns,
            "confirmed_bar_records_observed": len(bar_records),
            "confirmed_bar_coverage_by_type": bar_coverage,
            "confirmed_bar_observation_refs": list(bar_observation_refs),
            "bar_gaps_repaired": bar_gaps_repaired,
            "bar_repair_certificate_refs": sorted(bar_repair_refs),
            "reconciliation_scope": ["CONFIRMED_BARS", "CURRENT_PRODUCT_METADATA", "CURRENT_TICKER_SNAPSHOT"],
            "bar_snapshot_reconciled": source_snapshot_reconciled,
            "exact_trade_history_status": "NOT ESTIMABLE",
            "recovery_required": recovery_required,
            "prior_unhealthy_transition_after_healthy": recovery_required,
            "observed_trade_records": trade_count,
            "trade_observation_refs": list(trade_observation_refs),
            "trade_continuity_proven": trade_continuity_proven,
            "trade_gap_status": "UNVERIFIABLE" if recovery_required else "NO_PRIOR_GAP_IDENTIFIED",
            "s3_historical_vwap_coverage": "TEST GATE",
            "qualification_status": "TEST GATE",
            "reason_codes": ["BYBIT_RECENT_TRADE_WINDOW_DOES_NOT_PROVE_TRADE_CONTINUITY"],
        }
        evidence_ref = sha256_json(recovery_evidence)
        repository.register_artifact(ArtifactIndexEntryV2(
            evidence_ref, "BybitPublicRecoveryEvidenceV1", evidence_ref,
            reconciliation_at_ns, reconciliation_at_ns, {"recovery_evidence": recovery_evidence},
        ))
        return source_snapshot_reconciled

    def _persist_broad_public_snapshot(self, repository: OpsRepository, snapshot: Any, *, now_ns: int) -> bool:
        """Adopt both venues on the sole writer without inventing history completeness."""
        collector = cast(ProductionCollectorRecoveryV1, self._collector_recovery).collector
        publication = sample(self.clock_ns, floor_ns=now_ns)
        if max(snapshot.observed_at_ns, snapshot.latest_received_at_ns) > publication:
            raise ValueError("BROAD_PUBLIC_SNAPSHOT_FUTURE_RECEIPT")
        prior_clock = collector.clock_ns
        rejected_indexes: list[int] = []
        refs_by_source: dict[str, set[str]] = {source: set() for source in self.public_source.required_source_ids}
        pending_refs: dict[str, str] = {}
        manifest = snapshot.source_snapshot.get("metadata", {})
        accepted_by_source = {source: manifest.get(venue.value, {}).get("market_status") == "BULK_MARKET_COMPLETE"
            and venue.value not in snapshot.source_snapshot.get("metadata_stale", ())
            for venue in cast(Any, self.public_source).enabled_venues for source in (
                "BYBIT_PUBLIC_V2" if venue == VenueV2.BYBIT else "BINANCE_USDM_PUBLIC_V2",)}
        accepted = all(accepted_by_source.values())
        def adoption_clock() -> int:
            nonlocal publication
            publication = sample(self.clock_ns, floor_ns=publication)
            return publication

        collector.clock_ns = adoption_clock
        prefetched_indexes: dict[str, Mapping[str, Any] | None] = {}
        prefetched_generation = collector.archive_flush_generation
        last_stream_service_started_ns = 0
        try:
            for index, item in enumerate(snapshot.records):
                # Bound the time spent on REST universe adoption between stream
                # service turns. A fixed row count lets slow repositories leave
                # the capture handoff unattended for several seconds.
                service_started_ns = time.monotonic_ns()
                if (index == 0 or service_started_ns - last_stream_service_started_ns
                        >= BROAD_PUBLIC_ADOPTION_STREAM_SERVICE_INTERVAL_NS_V1):
                    last_stream_service_started_ns = service_started_ns
                    self.service_public_stream(repository)
                if index % BROAD_PUBLIC_ADOPTION_ARCHIVE_PAGE_ROWS_V1 == 0:
                    # Seal a small archive page before reading exact identities.
                    # Its references are prefetched only after that publication,
                    # so no in-page flush can turn a recorded absence into a
                    # durable duplicate. Service immediately after the
                    # indivisible Parquet/index write.
                    if collector.pending_archive_count:
                        collector.flush_archive()
                        self.service_public_stream(repository)
                    chunk_size = min(BROAD_PUBLIC_ADOPTION_ARCHIVE_PAGE_ROWS_V1,
                        len(snapshot.records) - index)
                    refs = tuple({sha256_json({"artifact_type": "PublicObservationIndexV2",
                        "record_id": record.observation.record_id})
                        for record in snapshot.records[index:index + chunk_size]})
                    found = repository.get_artifact_metadata_by_refs(refs)
                    prefetched_indexes = {ref: found.get(ref) for ref in refs}
                    prefetched_generation = collector.archive_flush_generation
                elif prefetched_generation != collector.archive_flush_generation:
                    # A serviced stream may have published exact indexes since
                    # the last lookup page. Refresh only the remaining portion
                    # of this bounded archive page before using absence proofs.
                    page_start = index - index % BROAD_PUBLIC_ADOPTION_ARCHIVE_PAGE_ROWS_V1
                    page_end = min(page_start + BROAD_PUBLIC_ADOPTION_ARCHIVE_PAGE_ROWS_V1,
                        len(snapshot.records))
                    refs = tuple({sha256_json({"artifact_type": "PublicObservationIndexV2",
                        "record_id": record.observation.record_id})
                        for record in snapshot.records[index:page_end]})
                    found = repository.get_artifact_metadata_by_refs(refs)
                    prefetched_indexes = {ref: found.get(ref) for ref in refs}
                    prefetched_generation = collector.archive_flush_generation
                adoption_clock()
                source_raw = item.observation
                if max(source_raw.received_at_ns, source_raw.ingested_at_ns,
                       source_raw.available_at_ns, source_raw.event_at_ns or 0,
                       source_raw.published_at_ns or 0) > publication:
                    accepted = False
                    accepted_by_source[source_raw.source_id] = False
                    rejected_indexes.append(index)
                    body = {"version": "BROAD_PUBLIC_ADOPTION_REJECTION_V1",
                        "record_id": source_raw.record_id, "observation_hash": source_raw.content_hash,
                        "source_id": source_raw.source_id, "observed_at_ns": publication,
                        "reason": "FUTURE_SOURCE_CHRONOLOGY", "authority": "ZERO"}
                    rejection_ref = sha256_json(body)
                    repository.register_artifact(ArtifactIndexEntryV2(rejection_ref,
                        "BroadPublicAdoptionRejectionV1", rejection_ref, publication, publication,
                        {"rejection": body}))
                    continue
                original_ref = sha256_json({"artifact_type": "PublicObservationIndexV2",
                                           "record_id": item.observation.record_id})
                prior = prefetched_indexes.get(original_ref)
                prior_metadata = prior.get("metadata") if prior is not None else None
                if item.bar is not None and prior_metadata is not None:
                    if prior_metadata.get("raw_payload_hash") != source_raw.raw_payload_hash:
                        # A corrected venue-final bar appends a causal revision.
                        # Its actual new receipt is retained, never backdated.
                        source_raw = RawObservationV2.build(
                            instrument_revision=source_raw.instrument_revision,
                            source_id=source_raw.source_id, event_type=source_raw.event_type,
                            event_at_ns=source_raw.event_at_ns, published_at_ns=source_raw.published_at_ns,
                            received_at_ns=source_raw.received_at_ns, ingested_at_ns=source_raw.ingested_at_ns,
                            available_at_ns=source_raw.available_at_ns, payload=item.raw_payload,
                            translation_version=source_raw.translation_version,
                            sequence=source_raw.sequence, revision_of=source_raw.record_id,
                            quality_flags=source_raw.quality_flags,
                            availability_class=source_raw.availability_class)
                    observed_ref = sha256_json({"artifact_type": "PublicObservationIndexV2",
                                              "record_id": source_raw.record_id})
                    if observed_ref not in prefetched_indexes:
                        found = repository.get_artifact_metadata_by_refs((observed_ref,))
                        prefetched_indexes[observed_ref] = found.get(observed_ref)
                    known = prefetched_indexes[observed_ref]
                    known_metadata = known.get("metadata") if known is not None else None
                    if known_metadata is not None and known_metadata.get("raw_payload_hash") == source_raw.raw_payload_hash:
                        body = {"version": "BROAD_PUBLIC_DUPLICATE_RECEIPT_V2", "original_ref": observed_ref,
                            "raw_payload_hash": source_raw.raw_payload_hash,
                            "received_at_ns": source_raw.received_at_ns, "available_at_ns": publication,
                            "source_id": source_raw.source_id, "authority": "ZERO"}
                        duplicate_ref = sha256_json(body)
                        if repository.get_artifact(duplicate_ref) is None:
                            repository.register_artifact(ArtifactIndexEntryV2(duplicate_ref,
                                "BroadPublicDuplicateReceiptV2", duplicate_ref, publication, publication,
                                {"receipt": body}))
                        refs_by_source.setdefault(source_raw.source_id, set()).add(observed_ref)
                        continue
                raw = replace(source_raw, ingested_at_ns=max(source_raw.ingested_at_ns, publication),
                    available_at_ns=max(source_raw.available_at_ns, publication))
                bar = replace(item.bar, raw=raw) if item.bar is not None else None
                try:
                    result = collector.ingest(raw, raw_payload=item.raw_payload,
                        instrument_key=item.instrument_key, bar=bar, retain_in_memory=False,
                        update_source_health=False,
                        persisted_public_index_rows_by_ref=prefetched_indexes,
                        persisted_public_index_generation=prefetched_generation)
                    if result.persistent_conflict or result.append.status.value == "CONFLICT_QUARANTINED":
                        accepted = False
                        accepted_by_source[raw.source_id] = False
                        rejected_indexes.append(index)
                    ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": raw.record_id})
                    if not result.persistent_conflict and result.append.status.value != "CONFLICT_QUARANTINED":
                        pending_refs[ref] = raw.source_id
                except (ValueError, RuntimeError):
                    accepted = False
                    accepted_by_source[raw.source_id] = False
                    rejected_indexes.append(index)
            collector.flush_archive()
            adoption_clock()
            pending_items = tuple(pending_refs.items())
            reconciliation_page_rows = min(
                MAX_RECONCILIATION_REFS_PER_PAGE_V1,
                BROAD_PUBLIC_ADOPTION_RECONCILIATION_PAGE_ROWS_V1,
            )
            for offset in range(0, len(pending_items), reconciliation_page_rows):
                # Full-universe exact-reference hydration can take seconds. Keep
                # the sole-writer stream lane moving between smaller bounded pages.
                self.service_public_stream(repository)
                page = pending_items[offset:offset + reconciliation_page_rows]
                persisted = repository.get_artifact_metadata_by_refs(tuple(ref for ref, _ in page))
                for ref, source_id in page:
                    entry = persisted.get(ref)
                    if (entry is not None and entry.get("artifact_type") == "PublicObservationIndexV2"
                            and entry["available_at_ns"] <= publication
                            and isinstance(entry.get("metadata"), Mapping)
                            and entry["metadata"].get("source_id") == source_id):
                        refs_by_source.setdefault(source_id, set()).add(ref)
                    else:
                        accepted = False
                        accepted_by_source[source_id] = False
        finally:
            collector.clock_ns = prior_clock
        # Current bulk observations prove only their declared scope. A prior
        # unhealthy interval is not erased by a metadata/ticker refresh.
        health_ready = accepted
        for source_id, snapshot_refs in sorted(refs_by_source.items()):
            prior_gap = collector.health.had_unhealthy_after_healthy(source_id)
            ready = accepted_by_source.get(source_id, False) and bool(snapshot_refs) and not prior_gap
            if ready:
                collector.reconcile_after_reconnect(source_id, at_ns=publication,
                    complete_snapshot=True, missed_interval_repaired=True,
                    snapshot_refs=tuple(sorted(snapshot_refs)), recovery_epoch_ref=collector.required_recovery_epoch_ref,
                    service_callback=lambda: self.service_public_stream(repository))
            else:
                collector.mark_incomplete_snapshot(source_id, at_ns=publication,
                    details="BROAD_PUBLIC_HISTORY_REPAIR_REQUIRED" if prior_gap else "BROAD_PUBLIC_SNAPSHOT_INCOMPLETE")
            health_ready = health_ready and ready
        body = {"version": "BROAD_PUBLIC_ACQUISITION_RECEIPT_V2", "available_at_ns": publication,
            "source_snapshot": json_value(snapshot.source_snapshot), "ingestion_complete": accepted,
            "source_health_current": health_ready, "source_observation_refs": {
                source: sorted(refs) for source, refs in sorted(refs_by_source.items())},
            "source_ingestion_complete": accepted_by_source,
            "rejected_record_indexes": rejected_indexes,
            "exact_trade_history_status": "NOT ESTIMABLE", "capital_enabled": False,
            "authority": "ZERO"}
        ref = sha256_json(body)
        repository.register_artifact(ArtifactIndexEntryV2(ref, "BroadPublicAcquisitionReceiptV2", ref,
            publication, publication, {"receipt": body}))
        return health_ready

    @staticmethod
    def _public_snapshot_overlap_is_repaired(
        repository: OpsRepository,
        snapshot: Any,
        *,
        at_ns: int,
        recovery_required: bool,
        clock_ns: Callable[[],int] = time.time_ns,
        certificate_refs: set[str] | None = None,
    ) -> bool:
        from ..data.bybit_source import CAMPAIGN_INTERVALS

        if not recovery_required:
            return True
        archive_root = Path(repository.path).parent / "ops-observations"
        keys = {record.instrument_key for record in snapshot.records}
        if not keys:
            return False
        from .bar_repair import repair_bar_interval
        for key in sorted(keys, key=lambda item: item.native_symbol):
            for interval in CAMPAIGN_INTERVALS:
                source_ids = {record.observation.source_id for record in snapshot.records
                              if record.instrument_key==key and record.bar is not None
                              and record.bar.interval==interval}
                if len(source_ids)!=1:
                    return False
                ready,repair_ref = repair_bar_interval(repository,archive_root,key=key,interval=interval,
                    source_id=next(iter(source_ids)),cutoff_ns=at_ns,clock_ns=clock_ns)
                if repair_ref is not None and certificate_refs is not None:
                    certificate_refs.add(repair_ref)
                if not ready:
                    return False
        return True

    def process_event(
        self,
        repository: OpsRepository,
        event: OpsDecisionEventV1,
        *,
        now_ns: int,
        source_health_state: str,
        completed_stages: Mapping[PipelineStageV1, OpsStageResultV1],
        checkpoint: Callable[[OpsStageResultV1], None],
    ) -> OpsDecisionResultV1:
        if event.event_type == S3_M1_EVENT_TYPE:
            inputs = self.inputs_provider.resolve(repository, event)
            if inputs is None or inputs.candidates or inputs.universe is not None:
                raise ValueError("native M1 diagnostic path produced decision-affecting inputs")
            diagnostic_refs = inputs.causal_source_refs
            diagnostic_available_at_ns = event.information_cutoff_ns
            for ref in diagnostic_refs:
                diagnostic_entry = repository.get_artifact(ref)
                if diagnostic_entry is None:
                    raise ValueError("native M1 diagnostic reference is not durably indexed")
                diagnostic_available_at_ns = max(
                    diagnostic_available_at_ns, diagnostic_entry.available_at_ns,
                )
            universe_created_at_ns = max(
                now_ns,
                diagnostic_available_at_ns,
                timestamp(self.clock_ns(), field="native S3 decision completion"),
            )
            universe_available_at_ns = max(
                universe_created_at_ns,
                timestamp(self.clock_ns(), field="native S3 universe availability"),
            )
            universe = ensure_native_s3_research_universe(
                repository,
                event,
                diagnostic_refs,
                created_at_ns=universe_created_at_ns,
                available_at_ns=universe_available_at_ns,
            )
            computation_started_ns = max(
                now_ns,
                event.information_cutoff_ns,
                universe.envelope.available_at_ns,
                diagnostic_available_at_ns,
                timestamp(self.clock_ns(), field="native S3 CandidateSet computation start"),
            )
            computation_finished_ns = max(
                computation_started_ns,
                timestamp(self.clock_ns(), field="native S3 CandidateSet computation finish"),
            )
            candidate_set_available_at_ns = max(
                computation_finished_ns,
                timestamp(self.clock_ns(), field="native S3 CandidateSet availability"),
            )
            candidate_set = persist_native_s3_not_estimable_candidate_set(
                repository,
                event,
                universe,
                diagnostic_refs,
                computation_started_ns=computation_started_ns,
                computation_finished_ns=computation_finished_ns,
                available_at_ns=candidate_set_available_at_ns,
            )
            calendar_ref = persist_native_s3_not_estimable_calendar(repository, candidate_set, event)
            native_stages: dict[PipelineStageV1, OpsStageResultV1] = {}
            for stage in PIPELINE_STAGE_ORDER:
                candidate_stage = stage == PipelineStageV1.CANDIDATE_SET
                calendar_stage = stage == PipelineStageV1.DECISION_CALENDAR
                refs = ((candidate_set.content_hash,) if candidate_stage else
                        (calendar_ref,) if calendar_stage else ())
                result = OpsStageResultV1(
                    stage,
                    OpsStageStatusV1.COMPLETE if candidate_stage else
                    OpsStageStatusV1.NOT_ESTIMABLE if calendar_stage else OpsStageStatusV1.SKIPPED,
                    refs, candidate_set.envelope.available_at_ns,
                    "BYBIT_TRADE_COMPLETENESS_UNPROVEN" if candidate_stage or calendar_stage
                    else "NATIVE_M1_S3_DIAGNOSTIC_ONLY",
                )
                checkpoint(result)
                native_stages[stage] = result
            return _result(
                native_stages, OpsTerminalStatusV1.NOT_ESTIMABLE,
                "BYBIT_TRADE_COMPLETENESS_UNPROVEN",
            )

        if source_health_state != "HEALTHY_CURRENT":
            raise ValueError("production event processing requires reconciled current public source health")
        inputs = self.inputs_provider.resolve(repository, event)
        if inputs is None:
            inputs = _empty_event_inputs(repository, event, clock_ns=self.clock_ns)
        if inputs.universe is None:
            raise ValueError("non-native-M1 production event has no decision universe")
        consumer_at = sample(self.clock_ns, floor_ns=max(now_ns, inputs.universe.envelope.available_at_ns))
        if not causal_artifact(repository, inputs.universe.content_hash,
                cutoff_ns=event.information_cutoff_ns, consumer_at_ns=consumer_at, deadline_ns=event.deadline_ns):
            raise ValueError("production universe has invalid derived chronology or missed deadline")
        if inputs.universe.decision_slot_ns < event.information_cutoff_ns:
            raise ValueError("production universe decision slot precedes its event cutoff")
        if inputs.candidates:
            required_inputs = {
                inputs.universe.content_hash,
                *(candidate.content_hash for candidate in inputs.candidates),
                *(ref for refs in inputs.scanner_evidence_refs.values() for ref in refs),
                *inputs.causal_feature_refs,
                *inputs.causal_source_refs,
            }
            sealed_inputs = _load_public_composition(repository, event)
            if sealed_inputs is None and not required_inputs.issubset(event.causal_input_refs):
                raise ValueError("candidate, universe, scanner or feature input is absent from the immutable event refs")

        stages: dict[PipelineStageV1, OpsStageResultV1] = dict(completed_stages)

        def save(
            stage: PipelineStageV1,
            status: OpsStageStatusV1,
            refs: Sequence[str] = (),
            *,
            reason: str | None = None,
            action_hash: str | None = None,
        ) -> None:
            if stage in stages:
                return
            completed_at_ns = max(now_ns, timestamp(self.clock_ns(), field="production stage completion"))
            result = OpsStageResultV1(stage, status, tuple(refs), completed_at_ns, reason, action_hash)
            checkpoint(result)
            stages[stage] = result
            if self.crash_after_checkpoint is not None:
                self.crash_after_checkpoint(stage)

        universe = inputs.universe
        if universe.selection_policy_hash != MULTI_SLEEVE_SELECTION_HASH:
            universe = research_selection_universe(universe)
        _index_universe(repository, universe)
        sleeve_audit_ref = persist_research_sleeve_audit(repository, available_at_ns=event.information_cutoff_ns)
        policies = dict(_POLICIES)
        with repository.atomic_composition():
            candidate_set = assemble_multisleeve_research_candidate_set(
                repository,
                universe=universe,
                decision_event_id=event.event_id,
                cutoff_ns=event.information_cutoff_ns,
                candidates=inputs.candidates,
                policies=policies,
                scanner_evidence_refs=inputs.scanner_evidence_refs,
                generation_missing_reasons=tuple(sorted({*inputs.generation_missing_reasons,
                    *(() if inputs.causal_feature_refs else ("CAUSAL_FEATURE_EVIDENCE_UNAVAILABLE",))})),
                clock_ns=self.clock_ns, deadline_ns=event.deadline_ns,
            )
            acceptances = accept_research_candidates(
                repository,
                candidate_set,
                inputs.candidates,
                accepted_at_ns=candidate_set.envelope.available_at_ns,
            )
        save(PipelineStageV1.UNIVERSE, OpsStageStatusV1.COMPLETE, (universe.content_hash,))
        feature_refs = tuple(ref for ref in inputs.causal_feature_refs if causal_artifact(repository, ref,
            cutoff_ns=event.information_cutoff_ns, consumer_at_ns=consumer_at, deadline_ns=event.deadline_ns))
        feature_status = OpsStageStatusV1.COMPLETE if feature_refs else OpsStageStatusV1.NOT_ESTIMABLE
        save(
            PipelineStageV1.CAUSAL_FEATURES,
            feature_status,
            feature_refs,
            reason=None if feature_refs else "CAUSAL_FEATURE_EVIDENCE_UNAVAILABLE",
        )
        save(PipelineStageV1.WATCHES_AND_SLEEVES, OpsStageStatusV1.COMPLETE, (sleeve_audit_ref,))
        save(PipelineStageV1.CANDIDATE_SET, OpsStageStatusV1.COMPLETE, (candidate_set.content_hash,))
        selection_status = {
            "SELECTED": OpsStageStatusV1.COMPLETE,
            "NO_CANDIDATE": OpsStageStatusV1.NO_CANDIDATE,
            "NOT_ESTIMABLE": OpsStageStatusV1.NOT_ESTIMABLE,
        }[candidate_set.selection_status.value]
        acceptance_refs = tuple(acceptances[item.candidate_id] for item in inputs.candidates)
        save(
            PipelineStageV1.SELECTION,
            selection_status,
            acceptance_refs,
            reason=None if selection_status == OpsStageStatusV1.COMPLETE else candidate_set.selection_status.value,
        )

        if candidate_set.selected_candidate_id is None:
            calendar_ref = _persist_selection_calendar(repository, candidate_set, event)
            save(PipelineStageV1.HARD_RISK, OpsStageStatusV1.SKIPPED, reason="NO_SELECTED_CANDIDATE")
            save(PipelineStageV1.FROZEN_ACTION, OpsStageStatusV1.SKIPPED, reason="NO_SELECTED_CANDIDATE")
            save(PipelineStageV1.ECONOMIC_EVALUATION, OpsStageStatusV1.SKIPPED, reason="NO_SELECTED_CANDIDATE")
            save(PipelineStageV1.M1_DIAGNOSTIC, OpsStageStatusV1.SKIPPED, reason="NO_FROZEN_ACTION")
            save(PipelineStageV1.ANALOGUE_DIAGNOSTIC, OpsStageStatusV1.SKIPPED, reason="NO_FROZEN_ACTION")
            save(PipelineStageV1.DECISION_CALENDAR, OpsStageStatusV1.COMPLETE, (calendar_ref,))
            terminal = (
                OpsTerminalStatusV1.NO_CANDIDATE
                if candidate_set.selection_status.value == "NO_CANDIDATE"
                else OpsTerminalStatusV1.NOT_ESTIMABLE
            )
            return _result(stages, terminal, "NO_ELIGIBLE_EXACT_ACTION_CANDIDATE" if terminal == OpsTerminalStatusV1.NO_CANDIDATE else "CANDIDATE_SELECTION_NOT_ESTIMABLE")

        selected = next(
            item for item in inputs.candidates if item.candidate_id == candidate_set.selected_candidate_id
        )
        policy = _POLICIES[selected.policy_hash]
        risk_inputs = inputs.risk_inputs.get(selected.candidate_id)
        risk_reason = _risk_input_reason(risk_inputs, event)
        if risk_reason is not None and isinstance(self.inputs_provider, IndexedProductionEventInputsV1):
            risk_inputs, resolution_reason = self.inputs_provider.resolve_risk(
                repository, event, candidate_set, selected, universe,
                now_ns=sample(self.clock_ns, floor_ns=candidate_set.envelope.available_at_ns),
            )
            risk_reason = resolution_reason or _risk_input_reason(risk_inputs, event)
        if risk_reason is not None:
            reason = risk_reason
            _index_runtime_decision(
                repository,
                candidate_set=candidate_set,
                candidate=selected,
                action=None,
                admission=AdmissionStateV2.NOT_ESTIMABLE,
                reason=reason,
                available_at_ns=now_ns,
            )
            calendar_ref = _persist_selected_calendar(repository, candidate_set, selected)
            # The blocker receipt is operational evidence created at runtime, often
            # after the event cutoff. Keep its reason on the checkpoint without
            # binding it as a causal hard-risk input.
            save(PipelineStageV1.HARD_RISK, OpsStageStatusV1.NOT_ESTIMABLE, reason=reason)
            save(PipelineStageV1.FROZEN_ACTION, OpsStageStatusV1.SKIPPED, reason="HARD_RISK_TERMINAL")
            save(PipelineStageV1.ECONOMIC_EVALUATION, OpsStageStatusV1.SKIPPED, reason="HARD_RISK_TERMINAL")
            save(PipelineStageV1.M1_DIAGNOSTIC, OpsStageStatusV1.SKIPPED, reason="NO_FROZEN_ACTION")
            save(PipelineStageV1.ANALOGUE_DIAGNOSTIC, OpsStageStatusV1.SKIPPED, reason="NO_FROZEN_ACTION")
            save(PipelineStageV1.DECISION_CALENDAR, OpsStageStatusV1.COMPLETE, (calendar_ref,))
            return _result(stages, OpsTerminalStatusV1.NOT_ESTIMABLE, reason)
        assert risk_inputs is not None and risk_inputs.complete

        with repository.atomic_composition():
            sizing = size_selected_candidate(
                repository,
                candidate_set=candidate_set,
                candidate=selected,
                universe=universe,
                policy=policy,
                product=_required(risk_inputs.product),
                v1=_required(risk_inputs.risk_policy_v1),
                v2=_required(risk_inputs.risk_policy_v2),
                account=_required(risk_inputs.account),
                exposures=_required(risk_inputs.exposures),
                outcomes=_required(risk_inputs.outcomes),
                venue=_required(risk_inputs.venue),
                stress=_required(risk_inputs.stress),
                fee=_required(risk_inputs.fee),
                cutoff_ns=event.information_cutoff_ns, clock_ns=self.clock_ns,
            )
        if sizing.status != SizingStatus.SIZED:
            admission = AdmissionStateV2.NO_TRADE if sizing.status == SizingStatus.NO_TRADE else AdmissionStateV2.NOT_ESTIMABLE
            reason_codes = tuple(sorted(set(sizing.reasons)))
            calendar_ref = _persist_sizing_calendar(repository, candidate_set, selected, sizing, admission)
            save(
                PipelineStageV1.HARD_RISK,
                OpsStageStatusV1.NO_TRADE if sizing.status == SizingStatus.NO_TRADE else OpsStageStatusV1.NOT_ESTIMABLE,
                (sizing.content_hash,),
                reason=reason_codes[0] if reason_codes else sizing.status.value,
            )
            save(PipelineStageV1.FROZEN_ACTION, OpsStageStatusV1.SKIPPED, reason="HARD_RISK_TERMINAL")
            save(PipelineStageV1.ECONOMIC_EVALUATION, OpsStageStatusV1.SKIPPED, reason="HARD_RISK_TERMINAL")
            save(PipelineStageV1.M1_DIAGNOSTIC, OpsStageStatusV1.SKIPPED, reason="NO_FROZEN_ACTION")
            save(PipelineStageV1.ANALOGUE_DIAGNOSTIC, OpsStageStatusV1.SKIPPED, reason="NO_FROZEN_ACTION")
            save(PipelineStageV1.DECISION_CALENDAR, OpsStageStatusV1.COMPLETE, (calendar_ref,))
            terminal = OpsTerminalStatusV1.NO_TRADE if sizing.status == SizingStatus.NO_TRADE else OpsTerminalStatusV1.NOT_ESTIMABLE
            return _result(stages, terminal, reason_codes[0] if reason_codes else sizing.status.value)

        # The frozen-action API is reachable only from a successful hard-risk result.
        with repository.atomic_composition():
            action = freeze_action(
                repository,
                candidate=selected,
                candidate_set=candidate_set,
                sizing=sizing,
                product=_required(risk_inputs.product),
                policy=policy,
                v1=_required(risk_inputs.risk_policy_v1),
                v2=_required(risk_inputs.risk_policy_v2), clock_ns=self.clock_ns,
            )
        save(
            PipelineStageV1.HARD_RISK,
            OpsStageStatusV1.COMPLETE,
            (sizing.content_hash,),
        )
        save(
            PipelineStageV1.FROZEN_ACTION,
            OpsStageStatusV1.COMPLETE,
            (action.content_hash,),
            action_hash=action.action.action_hash,
        )

        economic_inputs = inputs.economic_inputs.get(selected.candidate_id)
        economic_resolution_reason: str | None = None
        economic_at = sample(self.clock_ns, floor_ns=action.available_at_ns)
        if economic_inputs is None and isinstance(self.inputs_provider, IndexedProductionEventInputsV1):
            economic_inputs, economic_resolution_reason = self.inputs_provider.resolve_economic(
                repository, event, candidate_set, selected, action, risk_inputs,
                now_ns=economic_at, clock_ns=self.clock_ns,
            )
        evaluation: Phase2EvaluationResultV2 | _RecoveredEconomicEvaluationV1 | None = (
            _recover_economic_evaluation(repository, action, selected, candidate_set, now_ns=economic_at)
        )
        evaluation_reason = economic_resolution_reason or _evaluation_inputs_reason(economic_inputs, event)
        if evaluation is not None:
            evaluation_reason = None
            save(PipelineStageV1.ECONOMIC_EVALUATION,
                 {"CANDIDATE": OpsStageStatusV1.COMPLETE, "NO_TRADE": OpsStageStatusV1.NO_TRADE,
                  "NOT_ESTIMABLE": OpsStageStatusV1.NOT_ESTIMABLE}[evaluation.evaluation.decision.value],
                 (evaluation.evaluation_ref,),
                 reason=evaluation.evaluation.reason_codes[0] if evaluation.evaluation.reason_codes else None)
        elif economic_inputs is not None and economic_inputs.complete and evaluation_reason is None:
            try:
                evaluation = run_phase2_economic_evaluation(
                    repository,
                    action=action,
                    candidate=selected,
                    candidate_set=candidate_set,
                    sizing=sizing,
                    product=_required(risk_inputs.product),
                    risk_policy=_required(risk_inputs.risk_policy_v1),
                    risk_policy_v2=_required(risk_inputs.risk_policy_v2),
                    account=_required(risk_inputs.account),
                    fee=_required(risk_inputs.fee),
                    admission_policy=_required(economic_inputs.admission_policy),
                    capability=_required(economic_inputs.capability),
                    model_input=_required(economic_inputs.model_input),
                    calibration_input=_required(economic_inputs.calibration_input),
                    execution_model_input=_required(economic_inputs.execution_model_input),
                    available_at_ns=_required(economic_inputs.available_at_ns),
                    scenario_seed=_required(economic_inputs.scenario_seed),
                    scenario_count=economic_inputs.scenario_count,
                    source_inputs=economic_inputs.source_inputs,
                    joint_data_refs=economic_inputs.joint_data_refs,
                    support_unit_refs=economic_inputs.support_unit_refs,
                    execution_residual_refs=economic_inputs.execution_residual_refs,
                    stress_input_ref=economic_inputs.stress_input_ref,
                    existing_portfolio_path_refs=economic_inputs.existing_portfolio_path_refs,
                    clock_ns=self.clock_ns,
                )
            except Exception as error:
                evaluation_reason = f"ECONOMIC_EVALUATION_FAILED_{type(error).__name__}"
            if evaluation is not None:
                evaluation_status = {
                    "CANDIDATE": OpsStageStatusV1.COMPLETE,
                    "NO_TRADE": OpsStageStatusV1.NO_TRADE,
                    "NOT_ESTIMABLE": OpsStageStatusV1.NOT_ESTIMABLE,
                }[evaluation.evaluation.decision.value]
                save(
                    PipelineStageV1.ECONOMIC_EVALUATION,
                    evaluation_status,
                    (evaluation.evaluation_ref,),
                    reason=(evaluation.evaluation.reason_codes[0] if evaluation.evaluation.reason_codes else None),
                )
            else:
                save(PipelineStageV1.ECONOMIC_EVALUATION, OpsStageStatusV1.NOT_ESTIMABLE,
                     reason=evaluation_reason or "ECONOMIC_EVALUATION_UNAVAILABLE")
        else:
            evaluation_reason = evaluation_reason or "MANDATORY_ECONOMIC_EVIDENCE_UNAVAILABLE"
            save(
                PipelineStageV1.ECONOMIC_EVALUATION,
                OpsStageStatusV1.NOT_ESTIMABLE,
                reason=evaluation_reason,
            )

        diagnostic_at = max(now_ns, timestamp(self.clock_ns(), field="action diagnostic computation start"))
        m1_reason: str | None
        if diagnostic_at >= selected.deadline_ns:
            if PipelineStageV1.M1_DIAGNOSTIC not in stages:
                m1_ref, diagnostic_failure_reason = _persist_action_diagnostic_failure(
                    repository, action, kind="M1", reason="DECISION_DEADLINE_EXPIRED", available_at_ns=diagnostic_at
                )
                save(PipelineStageV1.M1_DIAGNOSTIC, OpsStageStatusV1.NOT_ESTIMABLE, (m1_ref,),
                     action_hash=action.action.action_hash, reason=diagnostic_failure_reason)
            if PipelineStageV1.ANALOGUE_DIAGNOSTIC not in stages:
                analogue = _not_estimable_analogue(repository, action, selected, candidate_set, diagnostic_at,
                                                    "NOT_ESTIMABLE_DECISION_DEADLINE_EXPIRED")
                save(PipelineStageV1.ANALOGUE_DIAGNOSTIC, OpsStageStatusV1.NOT_ESTIMABLE, (analogue,),
                     action_hash=action.action.action_hash, reason="NOT_ESTIMABLE_DECISION_DEADLINE_EXPIRED")
        else:
            if PipelineStageV1.M1_DIAGNOSTIC not in stages:
                m1_ref, m1_reason = _run_m1(repository, action, selected, candidate_set, event,
                                            diagnostic_at, dependency_lock_hash())
                save(
                    PipelineStageV1.M1_DIAGNOSTIC,
                    OpsStageStatusV1.NOT_ESTIMABLE if m1_reason else OpsStageStatusV1.COMPLETE,
                    (m1_ref,),
                    action_hash=action.action.action_hash,
                    reason=m1_reason,
                )
            if PipelineStageV1.ANALOGUE_DIAGNOSTIC not in stages:
                analogue_diagnostic = run_analogue_diagnostic_v1(
                    repository, action=action, candidate=selected, candidate_set=candidate_set,
                    cutoff_ns=event.information_cutoff_ns,
                    available_at_ns=max(diagnostic_at, timestamp(self.clock_ns(), field="analogue start")),
                    clock_ns=self.clock_ns,
                )
                save(
                    PipelineStageV1.ANALOGUE_DIAGNOSTIC,
                    OpsStageStatusV1.NOT_ESTIMABLE if analogue_diagnostic.reason else OpsStageStatusV1.COMPLETE,
                    (analogue_diagnostic.result_ref, analogue_diagnostic.retrieval_receipt_ref),
                    action_hash=action.action.action_hash,
                    reason=analogue_diagnostic.reason,
                )

        if evaluation is not None:
            calendar_ref = evaluation.calendar_ref
        else:
            _index_runtime_decision(
                repository,
                candidate_set=candidate_set,
                candidate=selected,
                action=action,
                admission=AdmissionStateV2.NOT_ESTIMABLE,
                reason=evaluation_reason or "ECONOMIC_EVIDENCE_UNAVAILABLE",
                available_at_ns=now_ns,
            )
            calendar_ref = _persist_sizing_calendar(
                repository, candidate_set, selected, sizing, AdmissionStateV2.RISK_SIZED, action=action,
            )
            # The calendar reference is the accepted scientific artifact. The
            # later runtime-only receipt remains indexed separately.
            save(PipelineStageV1.DECISION_CALENDAR, OpsStageStatusV1.NOT_ESTIMABLE,
                 (calendar_ref,), reason=evaluation_reason or "ECONOMIC_EVIDENCE_UNAVAILABLE")
        if evaluation is not None:
            save(PipelineStageV1.DECISION_CALENDAR, OpsStageStatusV1.COMPLETE, (calendar_ref,))

        if evaluation is None or evaluation.evaluation.decision.value == "NOT_ESTIMABLE":
            return _result(stages, OpsTerminalStatusV1.NOT_ESTIMABLE,
                           evaluation_reason or (evaluation.evaluation.reason_codes[0] if evaluation else "NOT_ESTIMABLE"))
        if evaluation.evaluation.decision.value == "NO_TRADE":
            return _result(stages, OpsTerminalStatusV1.NO_TRADE,
                           evaluation.evaluation.reason_codes[0] if evaluation.evaluation.reason_codes else "ECONOMIC_NO_TRADE")
        return _result(stages, OpsTerminalStatusV1.COMPLETE, None)



class ActiveEvidenceOverflowV1(ValueError):
    """The exact required population cannot be resolved within this declared work budget."""

    def __init__(self, pressure: Mapping[str, Any]) -> None:
        self.pressure = dict(pressure)
        super().__init__("ACTIVE_EVIDENCE_POPULATION_OVERFLOW")

    def publish(self, repository: OpsRepository) -> None:
        ref = sha256_json(self.pressure)
        if repository.get_artifact(ref) is None:
            at = time.time_ns()
            repository.register_artifact(ArtifactIndexEntryV2(ref, "OpsActiveWorkPressureV1", ref,
                at, at, {"pressure": self.pressure}))


def _bounded_evidence(repository: OpsRepository, artifact_type: str, *, cutoff_ns: int = 9_223_372_036_854_775_807,
                      limit: int = 4096) -> tuple[ArtifactIndexEntryV2, ...]:
    page = repository.latest_artifact_entries(artifact_type, as_of_ns=cutoff_ns, limit=limit)
    if page.has_more or page.invalid_entry_count:
        body = {"version": "OpsActiveWorkPressureV1", "artifact_type": artifact_type,
            "evidence_cutoff_ns": cutoff_ns, "limit": limit, "has_more": page.has_more,
            "invalid_entry_count": page.invalid_entry_count, "reason": "ACTIVE_EVIDENCE_POPULATION_OVERFLOW",
            "authority": "ZERO"}
        error = ActiveEvidenceOverflowV1(body)
        error.publish(repository)
        raise error
    return page.entries


def _bounded_active_watches(repository: OpsRepository) -> tuple[Any, ...]:
    watches = repository.list_active_watches(limit=513)
    if len(watches) > 512:
        error = ActiveEvidenceOverflowV1({"version": "OpsActiveWorkPressureV1",
            "artifact_type": "OpportunityWatchV2", "limit": 512, "has_more": True,
            "reason": "ACTIVE_WATCH_POPULATION_OVERFLOW", "authority": "ZERO"})
        error.publish(repository)
        raise error
    return watches


def _s6_hypothesis_for_watch(repository: OpsRepository, watch: Any, *, cutoff_ns: int) -> S6HypothesisV2 | None:
    """Decode only an exact, already-available S6 hypothesis bound to this watch."""
    entry = repository.get_artifact(watch.thesis_hash)
    body = entry.metadata.get("hypothesis") if entry is not None else None
    if (entry is None or entry.artifact_type != "S6HypothesisV2"
            or entry.artifact_ref != watch.thesis_hash or entry.content_hash != watch.thesis_hash
            or entry.available_at_ns > cutoff_ns or not isinstance(body, Mapping)):
        return None
    try:
        score, beta = body["score"], body["beta_btc"]
        cutoff = body["cutoff_ns"]
        if (isinstance(score, bool) or not isinstance(score, (int, float)) or not math.isfinite(score)
                or isinstance(beta, bool) or not isinstance(beta, (int, float)) or not math.isfinite(beta)
                or type(cutoff) is not int):
            return None
        hypothesis = S6HypothesisV2(
            hypothesis_id=str(body["hypothesis_id"]),
            key=InstrumentKeyV2.from_dict(json_value(body["key"])),
            policy_hash=str(body["policy_hash"]), state_ref=str(body["state_ref"]),
            side=V2Side(str(body["side"])), cutoff_ns=cutoff,
            score=float(score), beta_btc=float(beta), reason=str(body["reason"]),
            watch_id=str(body["watch_id"]),
            replay_view=str(body.get("replay_view", "ACTUAL_SYSTEM")),
        )
    except (KeyError, TypeError, ValueError):
        return None
    if (hypothesis.hypothesis_id != watch.thesis_hash or hypothesis.watch_id != watch.watch_id
            or hypothesis.key != watch.key or hypothesis.policy_hash != S6_POLICY.policy_hash
            or canonical_json(hypothesis.to_dict()) != canonical_json(body)):
        return None
    return hypothesis


def _latest_public_health(repository: OpsRepository, source_id: str, *, cutoff_ns: int) -> ArtifactIndexEntryV2 | None:
    page = repository.latest_artifact_entries("PublicSourceHealthV2", as_of_ns=cutoff_ns,
        metadata_path=("health", "source_id"), identity_value=source_id, limit=2)
    if page.invalid_entry_count:
        raise ValueError("public source health contains invalid indexed evidence")
    return page.entries[0] if page.entries else None

_PUBLIC_COMPOSITION_VERSION = "OpsPublicDerivedCompositionV1"


def _required_artifact(repository: OpsRepository, ref: str) -> ArtifactIndexEntryV2:
    entry = repository.get_artifact(ref)
    if entry is None:
        raise ValueError("required indexed artifact missing")
    return entry


def _public_composition_ref(event: OpsDecisionEventV1) -> str:
    return sha256_json({"version": _PUBLIC_COMPOSITION_VERSION, "event_hash": event.content_hash})


def _seal_public_composition(repository: OpsRepository, event: OpsDecisionEventV1,
                             inputs: ProductionEventInputsV1, *, clock_ns: Callable[[], int]) -> None:
    if inputs.universe is None:
        return
    body: dict[str, Any] = {"version": _PUBLIC_COMPOSITION_VERSION, "event_hash": event.content_hash,
        "event_id": event.event_id, "information_cutoff_ns": event.information_cutoff_ns,
        "universe_ref": inputs.universe.content_hash,
        "candidate_refs": [item.content_hash for item in inputs.candidates],
        "scanner_evidence_refs": {key: list(refs) for key, refs in inputs.scanner_evidence_refs.items()},
        "feature_refs": list(inputs.causal_feature_refs), "source_refs": list(inputs.causal_source_refs),
        "missing_reasons": list(inputs.generation_missing_reasons), "authority": "ZERO"}
    refs = {body["universe_ref"], *body["candidate_refs"], *body["feature_refs"], *body["source_refs"],
        *(ref for refs in inputs.scanner_evidence_refs.values() for ref in refs)}
    at = sample(clock_ns, floor_ns=max(event.information_cutoff_ns,
        *(_required_artifact(repository, ref).available_at_ns for ref in refs)))
    body["available_at_ns"] = at
    body["consumer_eligible"] = at <= event.deadline_ns
    ref = _public_composition_ref(event)
    repository.register_artifact(ArtifactIndexEntryV2(ref, _PUBLIC_COMPOSITION_VERSION,
        sha256_json(body), at, at, {"composition": body}))


def _load_public_composition(repository: OpsRepository, event: OpsDecisionEventV1) -> ProductionEventInputsV1 | None:
    entry = repository.get_artifact(_public_composition_ref(event))
    if entry is None:
        return None
    body = entry.metadata.get("composition")
    if (entry.artifact_type != _PUBLIC_COMPOSITION_VERSION or not isinstance(body, Mapping)
            or sha256_json(body) != entry.content_hash or body.get("event_hash") != event.content_hash
            or body.get("event_id") != event.event_id or body.get("information_cutoff_ns") != event.information_cutoff_ns
            or body.get("available_at_ns") != entry.available_at_ns or body.get("authority") != "ZERO"):
        raise ValueError("sealed public composition identity conflict")
    universe_entry = repository.get_artifact(str(body["universe_ref"]))
    if universe_entry is None:
        raise ValueError("sealed composition universe missing")
    universe = UniverseContractV2.from_dict(json_value(universe_entry.metadata["universe"]))
    candidates = tuple(CandidateActionV2.from_dict(json_value(_required_artifact(repository, ref).metadata["candidate"]))
        for ref in body["candidate_refs"])
    if universe.content_hash != body["universe_ref"] or [c.content_hash for c in candidates] != list(body["candidate_refs"]):
        raise ValueError("sealed composition target content conflict")
    refs = {universe.content_hash, *body["candidate_refs"], *body["feature_refs"], *body["source_refs"],
        *(ref for refs in body["scanner_evidence_refs"].values() for ref in refs)}
    # Late composition remains auditable; it is rejected separately by the consumer.
    if any(not causal_artifact(repository, ref, cutoff_ns=event.information_cutoff_ns,
            consumer_at_ns=entry.available_at_ns, deadline_ns=max(event.deadline_ns, entry.available_at_ns)) for ref in refs):
        raise ValueError("sealed composition dependency chronology is invalid")
    return ProductionEventInputsV1(universe, candidates,
        {key: tuple(refs) for key, refs in body["scanner_evidence_refs"].items()}, {}, {},
        tuple(body["feature_refs"]), tuple(body["source_refs"]), tuple(body["missing_reasons"]))


class IndexedProductionEventInputsV1:
    """Compose causal public inputs or resolve the exact artifacts already bound by an event."""

    def __init__(self, *, clock_ns: Callable[[], int] = time.time_ns,
                 stream_service: Callable[[OpsRepository], None] | None = None,
                 prepared_observer: Callable[..., None] | None = None) -> None:
        self.clock_ns = clock_ns
        self.stream_service = stream_service
        self.prepared_observer = prepared_observer

    def resolve(self, repository: OpsRepository, event: OpsDecisionEventV1) -> ProductionEventInputsV1 | None:
        if event.event_type == S3_M1_EVENT_TYPE:
            trigger = repository.get_artifact(event.trigger_ref)
            trigger_body = trigger.metadata.get("trigger") if trigger is not None else None
            if (trigger is None or trigger.artifact_type != "OpsPublicFinalBarTriggerV1"
                    or not isinstance(trigger_body, Mapping)):
                return ProductionEventInputsV1(None, (), {}, {}, {}, (), ())
            computation_started_ns = max(
                event.information_cutoff_ns,
                timestamp(self.clock_ns(), field="native S3 computation start"),
            )
            stream_service = self.stream_service
            return _compose_s3_native_diagnostics(
                repository, event, trigger_body,
                computation_started_ns=computation_started_ns,
                clock_ns=self.clock_ns,
                service=partial(stream_service, repository) if stream_service is not None else None,
            )

        sealed = _load_public_composition(repository, event)
        if sealed is not None:
            return sealed
        causal_refs = set(event.causal_input_refs)
        generation_entry = repository.get_artifact(sha256_json({
            "artifact_type": "OpsCandidateGenerationEvidenceV1", "event_id": event.event_id}))
        generation_body = generation_entry.metadata.get("generation") if generation_entry is not None else None
        if generation_entry is not None and (
                generation_entry.artifact_type != "OpsCandidateGenerationEvidenceV1"
                or not isinstance(generation_body, Mapping)
                or generation_entry.content_hash != sha256_json(generation_body)
                or generation_body.get("event_id") != event.event_id
                or generation_body.get("information_cutoff_ns") != event.information_cutoff_ns):
            raise ValueError("candidate generation evidence identity conflicts with the exact event")
        generation_missing = tuple(generation_body.get("missing_reasons", ())) if isinstance(generation_body, Mapping) else ()
        universe: UniverseContractV2 | None = None
        for ref in sorted(causal_refs):
            entry = repository.get_artifact(ref)
            body = entry.metadata.get("universe") if entry is not None and entry.artifact_type == "UniverseContractV2" else None
            if isinstance(body, Mapping):
                parsed = UniverseContractV2.from_dict(json_value(body))
                if parsed.content_hash == ref and parsed.envelope.available_at_ns <= event.information_cutoff_ns:
                    universe = parsed
                    break
        if universe is None:
            trigger = repository.get_artifact(event.trigger_ref)
            trigger_body = trigger.metadata.get("trigger") if trigger is not None else None
            if (trigger is not None and trigger.artifact_type == "OpsPublicFinalBarTriggerV1"
                    and isinstance(trigger_body, Mapping)):
                try:
                    sealed = _load_public_composition(repository, event)
                    if sealed is not None:
                        return sealed
                    stream_service = self.stream_service
                    service = partial(stream_service, repository) if stream_service is not None else None
                    prepared_products = _causal_products(
                        repository, cutoff_ns=event.information_cutoff_ns, service=service,
                    )
                    from .broad_universe import full_universe

                    prepared_broad_universe = full_universe(
                        repository, cutoff_ns=event.information_cutoff_ns, service=service,
                    )
                    prepared_histories = _prepare_public_histories(repository, event,
                        clock_ns=self.clock_ns, service=service, products=prepared_products)
                    with repository.atomic_composition():
                        sealed = _load_public_composition(repository, event)
                        if sealed is not None:
                            return sealed
                        inputs = _compose_public_event_inputs(repository, event, trigger_body, clock_ns=self.clock_ns,
                            prepared_histories=prepared_histories, prepared_products=prepared_products,
                            prepared_broad_universe=prepared_broad_universe)
                        _seal_public_composition(repository, event, inputs, clock_ns=self.clock_ns)
                        if self.prepared_observer is not None and inputs.universe is not None:
                            self.prepared_observer(event, inputs.universe, prepared_histories, repository)
                        return inputs
                except ActiveEvidenceOverflowV1 as error:
                    # Pressure is durable even when the composition rolls back.
                    error.publish(repository)
                    raise
            universe = _empty_universe(repository, event, clock_ns=self.clock_ns)

        candidates: list[CandidateActionV2] = []
        for ref in sorted(causal_refs):
            entry = repository.get_artifact(ref)
            body = entry.metadata.get("candidate") if entry is not None and entry.artifact_type == "CandidateActionV2" else None
            if not isinstance(body, Mapping):
                continue
            candidate = CandidateActionV2.from_dict(json_value(body))
            if (candidate.content_hash == ref and candidate.decision_at_ns == event.information_cutoff_ns
                    and candidate.policy_hash in _POLICIES and candidate.envelope.available_at_ns <= event.information_cutoff_ns):
                candidates.append(candidate)
        if not candidates:
            no_candidate_feature_refs = tuple(sorted(
                entry.artifact_ref for ref in causal_refs
                if (entry := repository.get_artifact(ref)) is not None
                and entry.artifact_type == "FeatureArtifactV2"
            ))
            known_refs = {universe.content_hash, *no_candidate_feature_refs}
            return ProductionEventInputsV1(
                universe, (), {}, {}, {}, no_candidate_feature_refs,
                tuple(sorted(causal_refs - known_refs)),
                generation_missing or (() if no_candidate_feature_refs else ("CAUSAL_FEATURE_EVIDENCE_UNAVAILABLE",)),
            )

        scanner_refs: dict[str, tuple[str, ...]] = {}
        feature_refs: set[str] = set()
        for candidate in candidates:
            feature_refs.add(candidate.snapshot_hash)
            ranks = tuple(sorted(
                entry.artifact_ref for entry in _bounded_evidence(repository, "ScannerRankEvidenceV1")
                if entry.available_at_ns <= event.information_cutoff_ns
                and entry.metadata.get("candidate_id") == candidate.candidate_id
                and entry.metadata.get("decision_event_id") == event.event_id
                and entry.metadata.get("universe_ref") in (universe.content_hash, *universe.envelope.input_refs)
            ))
            if ranks:
                scanner_refs[candidate.candidate_id] = ranks
        return ProductionEventInputsV1(
            universe,
            tuple(sorted(candidates, key=lambda item: item.candidate_id)),
            scanner_refs,
            {},
            {},
            tuple(sorted(feature_refs)),
            tuple(sorted(causal_refs - {universe.content_hash, *feature_refs,
                                       *(item.content_hash for item in candidates),
                                       *(ref for values in scanner_refs.values() for ref in values)})),
            generation_missing,
        )

    def resolve_risk(
        self, repository: OpsRepository, event: OpsDecisionEventV1, candidate_set: CandidateSetV2,
        candidate: CandidateActionV2, universe: UniverseContractV2, *, now_ns: int,
    ) -> tuple[ProductionRiskInputsV1 | None, str | None]:
        return _resolve_indexed_risk_inputs(
            repository, event, candidate_set, candidate, universe, now_ns=now_ns,
        )

    def resolve_economic(
        self, repository: OpsRepository, event: OpsDecisionEventV1, candidate_set: CandidateSetV2,
        candidate: CandidateActionV2, action: ActionArtifactV2, risk: ProductionRiskInputsV1,
        *, now_ns: int, clock_ns: Callable[[], int] | None = None,
    ) -> tuple[ProductionEconomicInputsV1 | None, str | None]:
        return _resolve_indexed_economic_inputs(
            repository, event, candidate_set, candidate, action, risk, now_ns=now_ns, clock_ns=clock_ns,
        )


def _s3_native_diagnostics_identity_ref(event_id: str) -> str:
    return sha256_json({
        "artifact_type": "S3NativeM1DiagnosticIdentityV1",
        "decision_event_id": event_id,
    })


def _reuse_s3_native_diagnostics(
    repository: OpsRepository,
    event: OpsDecisionEventV1,
) -> tuple[str, ...] | None:
    """Load the one atomically published diagnostic set for a fixed M1 origin."""
    identity_ref = _s3_native_diagnostics_identity_ref(event.event_id)
    identity_entry = repository.get_artifact(identity_ref)
    if identity_entry is None:
        return None
    identity = identity_entry.metadata.get("identity")
    if (identity_entry.artifact_type != "S3NativeM1DiagnosticIdentityV1"
            or not isinstance(identity, Mapping)
            or identity_entry.content_hash != sha256_json(identity)
            or identity.get("decision_event_id") != event.event_id
            or identity.get("evidence_cutoff_ns") != event.information_cutoff_ns
            or identity.get("consumer_deadline_ns") != event.deadline_ns
            or identity.get("authority") != "ZERO"):
        raise ValueError("native M1 diagnostic identity conflicts with the fixed event")
    refs = identity.get("artifact_refs")
    expected = {
        "forward_trade_evidence": "S3ForwardTradeEvidenceV1",
        "sequence_book_quote_evidence": "S3SequenceBookQuoteEvidenceV1",
        "warmup_readiness": "S3NativeWarmupReadinessV1",
    }
    if not isinstance(refs, Mapping) or set(refs) != set(expected):
        raise ValueError("native M1 diagnostic identity is incomplete")
    validated: list[str] = []
    for name, artifact_type in expected.items():
        ref = refs[name]
        if ref is None and name == "sequence_book_quote_evidence":
            continue
        if not isinstance(ref, str):
            raise ValueError("native M1 diagnostic identity contains an invalid artifact ref")
        sha256_ref(ref, field="native M1 diagnostic artifact ref")
        entry = repository.get_artifact(ref)
        if (entry is None or entry.artifact_type != artifact_type or entry.artifact_ref != ref
                or entry.content_hash != ref or entry.available_at_ns > event.deadline_ns):
            raise ValueError("native M1 diagnostic identity points to conflicting or late evidence")
        body_key = {
            "S3ForwardTradeEvidenceV1": "evidence",
            "S3SequenceBookQuoteEvidenceV1": "bridge",
            "S3NativeWarmupReadinessV1": "readiness",
        }[artifact_type]
        body = entry.metadata.get(body_key)
        if not isinstance(body, Mapping):
            raise ValueError("native M1 diagnostic artifact has no typed body")
        context_body = body.get("computation_context")
        if not isinstance(context_body, Mapping):
            raise ValueError("native M1 diagnostic artifact has no honest computation timing")
        context = S3NativeComputationContextV1.from_dict(context_body)
        if (context.evidence_cutoff_ns != event.information_cutoff_ns
                or context.consumer_deadline_ns != event.deadline_ns
                or entry.available_at_ns < context.computation_finished_ns):
            raise ValueError("native M1 diagnostic artifact violates the fixed causal timeline")
        if artifact_type == "S3ForwardTradeEvidenceV1":
            if (body.get("cutoff_ns") != event.information_cutoff_ns
                    or body.get("trade_completeness_proven") is not False
                    or sha256_json({"artifact_type": artifact_type, "evidence": dict(body)}) != ref):
                raise ValueError("native M1 forward-trade artifact failed its cutoff or content check")
        elif artifact_type == "S3SequenceBookQuoteEvidenceV1":
            bridge = S3QuoteBridgeResultV1.from_dict(body)
            if bridge.evidence_ref != ref or bridge.cutoff_ns != event.information_cutoff_ns:
                raise ValueError("native M1 sequence-book artifact failed its cutoff or content check")
        else:
            if (body.get("cutoff_ns") != event.information_cutoff_ns
                    or body.get("trade_completeness_proven") is not False
                    or sha256_json({"artifact_type": artifact_type, "readiness": dict(body)}) != ref):
                raise ValueError("native M1 readiness artifact failed its cutoff or content check")
        validated.append(ref)
    if identity_entry.available_at_ns > event.deadline_ns:
        raise ValueError("native M1 diagnostic identity was published after the fixed deadline")
    return tuple(sorted(validated))


def _compose_s3_native_diagnostics(
    repository: OpsRepository,
    event: OpsDecisionEventV1,
    trigger_body: Mapping[str, Any],
    *,
    computation_started_ns: int,
    clock_ns: Callable[[], int],
    service: Callable[[], None] | None = None,
) -> ProductionEventInputsV1:
    """Compose S3 evidence outputs at one frozen source cutoff, without candidates."""
    cached_refs = _reuse_s3_native_diagnostics(repository, event)
    if cached_refs is not None:
        return ProductionEventInputsV1(None, (), {}, {}, {}, (), cached_refs)

    cutoff_ns = event.information_cutoff_ns
    if (trigger_body.get("version") != "OPS_PUBLIC_FINAL_BAR_TRIGGER_V1"
            or trigger_body.get("information_cutoff_ns") != cutoff_ns
            or event.event_type != S3_M1_EVENT_TYPE
            or event.deadline_ns < cutoff_ns):
        return ProductionEventInputsV1(None, (), {}, {}, {}, (), ())
    for ref in event.causal_input_refs:
        entry = repository.get_artifact(ref)
        if entry is None or entry.available_at_ns > cutoff_ns:
            raise ValueError("native M1 source input exceeds the immutable evidence cutoff")

    product_ref = trigger_body.get("product_ref")
    bar_ref = trigger_body.get("bar_ref")
    if not isinstance(product_ref, str) or not isinstance(bar_ref, str):
        return ProductionEventInputsV1(None, (), {}, {}, {}, (), ())
    product_entry = repository.get_artifact(product_ref)
    product_body = product_entry.metadata.get("product") if product_entry is not None else None
    if (product_entry is None or product_entry.artifact_type != "ProductContractV2"
            or product_entry.available_at_ns > cutoff_ns or not isinstance(product_body, Mapping)):
        return ProductionEventInputsV1(None, (), {}, {}, {}, (), ())
    product = ProductContractV2.from_dict(json_value(product_body))
    if (product.content_hash != product_ref or product.effective_at_ns > event.source_event_at_ns
            or product.observed_at_ns > cutoff_ns or product.available_at_ns > cutoff_ns):
        raise ValueError("native M1 product metadata is not exact and cutoff-visible")
    if (event.event_id != s3_m1_event_id(product.key, event.source_event_at_ns)
            or event.deadline_ns != event.source_event_at_ns + S3_M1_DEFAULT_MAX_LATENESS_NS
            or trigger_body.get("source_id") != event.source_id
            or trigger_body.get("source_event_at_ns") != event.source_event_at_ns):
        raise ValueError("native M1 origin identity or fixed close deadline changed")

    archive_root = Path(repository.path).parent / "ops-observations"
    indexed_bars = reconstruct_causal_bars_from_archive(
        repository, archive_root, key=product.key, interval=BarIntervalV2.M1,
        information_cutoff_ns=cutoff_ns,
        availability_class=AvailabilityClassV2.ACTUAL_SYSTEM,
        limit=7 * 1440 + 121,
        service=service,
    )
    bars = tuple(item.bar for item in indexed_bars)
    trigger_bar = next((bar for bar in bars if bar.content_hash == bar_ref), None)
    if (trigger_bar is None or trigger_bar.close_at_ns != event.source_event_at_ns
            or trigger_bar.close_at_ns > cutoff_ns or trigger_bar.raw.available_at_ns > cutoff_ns
            or trigger_bar.raw.availability_class != AvailabilityClassV2.ACTUAL_SYSTEM):
        raise ValueError("native M1 event no longer resolves to its exact cutoff-visible final bar")

    health_entry = _latest_public_health(repository, trigger_bar.raw.source_id, cutoff_ns=cutoff_ns)
    bar_health = PublicSourceHealthV2.from_dict(health_entry.metadata["health"]) if health_entry is not None else None
    if bar_health is not None and (health_entry is None or bar_health.content_hash != health_entry.artifact_ref or bar_health.observed_at_ns > cutoff_ns):
        raise ValueError("native M1 source health chronology conflicts")

    rest_quote, _mark, rest_refs = _indexed_quote_and_mark(
        repository, archive_root, product, cutoff_ns=cutoff_ns,
    )
    bridge: S3QuoteBridgeResultV1 | None = None
    report_ref = _latest_stream_continuity_report_ref(
        repository, product, channel=f"orderbook.50.{product.key.native_symbol}", cutoff_ns=cutoff_ns,
    )
    if report_ref is not None:
        bridge = quote_from_valid_continuity_report(
            repository, product, cutoff_ns=cutoff_ns, continuity_report_ref=report_ref,
        )
    stream_quote = bridge.quote if bridge is not None else None
    quote = stream_quote or (
        rest_quote if rest_quote is not None and rest_quote.valid_at(cutoff_ns, 1_000_000_000) else None
    )
    quote_refs: set[str] = set(rest_refs)
    if bridge is not None:
        if report_ref is None:
            raise ValueError("native M1 quote bridge lost its continuity report identity")
        quote_refs.update((report_ref, bridge.evidence_ref, *bridge.input_refs))
    if quote is not None:
        quote_refs.add(quote.evidence_ref)

    trade_evidence, trade_health = _indexed_s3_forward_trade_evidence(
        repository, archive_root, product, cutoff_ns=cutoff_ns,
    )
    residuals = _indexed_s3_residuals(repository, product.key, cutoff_ns=cutoff_ns)
    vwaps = _indexed_s3_vwaps(repository, product.key, cutoff_ns=cutoff_ns)
    prerequisite = _publish_public_prerequisites(repository, event, product, clock_ns=clock_ns)
    gate_body = _required_artifact(repository, prerequisite.event_gate_ref).metadata["gate"]
    gate = EventGate(EventState(gate_body["state"]), prerequisite.available_at_ns,
        prerequisite.event_gate_ref, gate_body["gate_version"])
    gate_ref = prerequisite.event_gate_ref

    raw_trade_body = trade_evidence.to_dict()
    raw_trade_ref = sha256_json({
        "artifact_type": "S3ForwardTradeEvidenceV1", "evidence": raw_trade_body,
    })
    readiness = evaluate_s3_warmup_readiness(
        key=product.key,
        cutoff_ns=cutoff_ns,
        bars=bars,
        residuals=residuals,
        trade_vwaps=vwaps,
        trades=trade_evidence.trades,
        bar_source_health=bar_health,
        trade_source_health=trade_health,
        quote=quote,
        event_gate=gate,
        point_in_time_universe_eligible=False,
        recovery_epoch=trade_evidence.recovery_epoch if trade_evidence.recovery_epoch is not None
        else (bridge.recovery_epoch if bridge is not None else None),
        trade_recovery_epoch=trade_evidence.recovery_epoch,
        book_recovery_epoch=bridge.recovery_epoch if bridge is not None else None,
        trade_evidence_status=trade_evidence.status,
        trade_evidence_reason_codes=trade_evidence.reason_codes,
        additional_evidence_refs=tuple(sorted({raw_trade_ref, *quote_refs,
                                                *([gate_ref] if gate_ref is not None else [])})),
    )

    computation_finished_ns = max(
        computation_started_ns,
        timestamp(clock_ns(), field="native S3 diagnostic completion"),
    )
    context = S3NativeComputationContextV1(
        cutoff_ns, computation_started_ns, computation_finished_ns, event.deadline_ns,
    )
    trade_evidence = trade_evidence.with_computation_context(context, repository=repository)
    trade_body = trade_evidence.to_dict()
    trade_ref = sha256_json({"artifact_type": "S3ForwardTradeEvidenceV1", "evidence": trade_body})
    timed_bridge = bridge.with_computation_context(context, repository=repository) if bridge is not None else None
    bridge_body = timed_bridge.to_dict() if timed_bridge is not None else None
    bridge_ref = timed_bridge.evidence_ref if timed_bridge is not None else None

    readiness_refs = set(readiness.evidence_refs)
    readiness_refs.discard(raw_trade_ref)
    if bridge is not None:
        readiness_refs.discard(bridge.evidence_ref)
    readiness_refs.update((trade_ref, *rest_refs))
    if timed_bridge is not None and bridge_ref is not None:
        readiness_refs.update((bridge_ref, *timed_bridge.input_refs))
    if gate_ref is not None:
        readiness_refs.add(gate_ref)
    readiness = replace(
        readiness,
        evidence_refs=tuple(sorted(readiness_refs)),
        computation_context=context,
    )
    readiness_body = readiness.to_dict()
    readiness_ref = sha256_json({
        "artifact_type": "S3NativeWarmupReadinessV1", "readiness": readiness_body,
    })

    persisted_at_ns = max(
        computation_finished_ns,
        timestamp(clock_ns(), field="native S3 diagnostic persistence"),
    )
    if persisted_at_ns > event.deadline_ns:
        raise ValueError("native S3 diagnostic computation missed its fixed consumer deadline")
    output_entries = [
        ArtifactIndexEntryV2(
            trade_ref, "S3ForwardTradeEvidenceV1", trade_ref,
            persisted_at_ns, persisted_at_ns,
            {"evidence": trade_body, "decision_event_id": event.event_id,
             "trigger_bar_ref": trigger_bar.content_hash, "authority": "ZERO"},
        ),
        ArtifactIndexEntryV2(
            readiness_ref, "S3NativeWarmupReadinessV1", readiness_ref,
            persisted_at_ns, persisted_at_ns,
            {"readiness": readiness_body, "decision_event_id": event.event_id,
             "trigger_bar_ref": trigger_bar.content_hash,
             "forward_trade_evidence_ref": trade_ref, "authority": "ZERO"},
        ),
    ]
    if timed_bridge is not None and bridge_body is not None and bridge_ref is not None:
        output_entries.append(ArtifactIndexEntryV2(
            bridge_ref, "S3SequenceBookQuoteEvidenceV1", bridge_ref,
            persisted_at_ns, persisted_at_ns,
            {"bridge": bridge_body, "decision_event_id": event.event_id,
             "trigger_bar_ref": trigger_bar.content_hash, "authority": "ZERO"},
        ))
    identity_body = {
        "version": "S3_NATIVE_M1_DIAGNOSTIC_IDENTITY_V1",
        "decision_event_id": event.event_id,
        "evidence_cutoff_ns": cutoff_ns,
        "consumer_deadline_ns": event.deadline_ns,
        "artifact_refs": {
            "forward_trade_evidence": trade_ref,
            "sequence_book_quote_evidence": bridge_ref,
            "warmup_readiness": readiness_ref,
        },
        "authority": "ZERO",
    }
    identity_ref = _s3_native_diagnostics_identity_ref(event.event_id)
    output_entries.append(ArtifactIndexEntryV2(
        identity_ref, "S3NativeM1DiagnosticIdentityV1", sha256_json(identity_body),
        persisted_at_ns, persisted_at_ns, {"identity": identity_body},
    ))
    repository.register_artifacts(tuple(output_entries))
    refs = tuple(sorted(entry.artifact_ref for entry in output_entries
                        if entry.artifact_type != "S3NativeM1DiagnosticIdentityV1"))
    return ProductionEventInputsV1(None, (), {}, {}, {}, (), refs)


def _resolve_indexed_risk_inputs(
    repository: OpsRepository, event: OpsDecisionEventV1, candidate_set: CandidateSetV2,
    candidate: CandidateActionV2, universe: UniverseContractV2, *, now_ns: int,
) -> tuple[ProductionRiskInputsV1 | None, str | None]:
    if candidate_set.selected_candidate_id != candidate.candidate_id:
        return None, "RISK_BINDING_SELECTION_IDENTITY_MISMATCH"
    members = [item for item in universe.entries if item.key == candidate.key]
    if len(members) != 1:
        return None, "RISK_PRODUCT_IDENTITY_AMBIGUOUS"
    product_ref = members[0].product_ref
    product_entry = repository.get_artifact(product_ref)
    product_body = product_entry.metadata.get("product") if product_entry is not None else None
    product = ProductContractV2.from_dict(json_value(product_body)) if isinstance(product_body, Mapping) else None
    if (product_entry is None or product_entry.artifact_type != "ProductContractV2" or product is None
            or product.content_hash != product_ref or product.key != candidate.key
            or product_entry.available_at_ns > event.information_cutoff_ns):
        return None, "EXACT_PRODUCT_CONTRACT_UNAVAILABLE"

    policy_pairs: list[tuple[RiskPolicy, RiskPolicyV2]] = []
    v1_by_hash: dict[str, list[RiskPolicy]] = {}
    for entry in _bounded_evidence(repository, "RiskPolicyV1"):
        body = entry.metadata.get("policy")
        if entry.available_at_ns > event.information_cutoff_ns or not isinstance(body, Mapping):
            continue
        try:
            policy = _risk_policy_v1_from_dict(body)
        except (TypeError, ValueError):
            continue
        if policy.policy_hash() == entry.artifact_ref:
            v1_by_hash.setdefault(policy.policy_hash(), []).append(policy)
    for entry in _bounded_evidence(repository, "RiskPolicyV2"):
        body = entry.metadata.get("policy")
        if entry.available_at_ns > event.information_cutoff_ns or not isinstance(body, Mapping):
            continue
        try:
            policy_v2 = RiskPolicyV2.from_dict(dict(body))
        except (TypeError, ValueError):
            continue
        if policy_v2.policy_hash != entry.artifact_ref:
            continue
        for policy_v1 in v1_by_hash.get(policy_v2.base_v1_risk_policy_hash, ()):
            if (policy_v1.policy_effective_at_ns <= event.information_cutoff_ns
                    and policy_v2.effective_at_ns <= event.information_cutoff_ns):
                policy_pairs.append((policy_v1, policy_v2))
    if not policy_pairs:
        return None, "EXACT_RISK_POLICY_PAIR_UNAVAILABLE"
    newest_effective = max((item[0].policy_effective_at_ns, item[1].effective_at_ns) for item in policy_pairs)
    policy_pairs = [item for item in policy_pairs
                    if (item[0].policy_effective_at_ns, item[1].effective_at_ns) == newest_effective]
    if len({(item[0].policy_hash(), item[1].policy_hash) for item in policy_pairs}) != 1:
        return None, "AMBIGUOUS_EFFECTIVE_RISK_POLICY_PAIR"
    risk_policy_v1, risk_policy_v2 = policy_pairs[0]

    accounts: list[tuple[ArtifactIndexEntryV2, AccountRiskSnapshotV2]] = []
    for entry in _bounded_evidence(repository, "AccountRiskSnapshotV2"):
        if entry.available_at_ns > event.information_cutoff_ns:
            continue
        try:
            account = _account_risk_from_dict(entry.metadata)
        except (KeyError, TypeError, ValueError):
            continue
        if account.content_hash == entry.artifact_ref and account.operational_status == "CURRENT":
            accounts.append((entry, account))
    if not accounts:
        return None, "CURRENT_ACCOUNT_RISK_SNAPSHOT_UNAVAILABLE"
    latest_account_time = max(item.available_at_ns for item, _account in accounts)
    latest_accounts = [(entry, account) for entry, account in accounts if entry.available_at_ns == latest_account_time]
    if len({account.content_hash for _entry, account in latest_accounts}) != 1:
        return None, "AMBIGUOUS_CURRENT_ACCOUNT_RISK_SNAPSHOT"
    account = latest_accounts[0][1]

    exposures: list[PossibleRiskV2] = []
    for ref in sorted(set(account.pending_risk_refs) | set(account.existing_exposure_refs)):
        exposure_entry = repository.get_artifact(ref)
        if (exposure_entry is None or exposure_entry.artifact_type != "PossibleRiskV2"
                or exposure_entry.available_at_ns > event.information_cutoff_ns):
            return None, "ACCOUNT_EXPOSURE_REFERENCE_UNAVAILABLE"
        try:
            exposure = _possible_risk_from_dict(exposure_entry.metadata)
        except (KeyError, TypeError, ValueError):
            return None, "ACCOUNT_EXPOSURE_REFERENCE_INVALID"
        if exposure.content_hash != ref:
            return None, "ACCOUNT_EXPOSURE_IDENTITY_MISMATCH"
        exposures.append(exposure)
    outcomes: list[ClosedV2Outcome] = []
    for ref in account.closed_outcome_refs:
        outcome_entry = repository.get_artifact(ref)
        if (outcome_entry is None or outcome_entry.artifact_type != "ClosedV2Outcome"
                or outcome_entry.available_at_ns > event.information_cutoff_ns):
            return None, "ACCOUNT_OUTCOME_REFERENCE_UNAVAILABLE"
        try:
            outcome = _closed_outcome_from_dict(outcome_entry.metadata)
        except (KeyError, TypeError, ValueError):
            return None, "ACCOUNT_OUTCOME_REFERENCE_INVALID"
        if outcome.content_hash != ref:
            return None, "ACCOUNT_OUTCOME_IDENTITY_MISMATCH"
        outcomes.append(outcome)

    venue = _unique_latest_typed_risk_evidence(
        repository, "VenueSizingLimitsV2", event.information_cutoff_ns,
        lambda body: _venue_sizing_from_dict(body),
        lambda item: item.key == candidate.key and item.product_ref == product_ref,
    )
    stress = _unique_latest_typed_risk_evidence(
        repository, "StressBoundV2", event.information_cutoff_ns,
        lambda body: _stress_bound_from_dict(body), lambda item: item.key == candidate.key,
    )
    fee = _unique_latest_typed_risk_evidence(
        repository, "FeeScheduleV2", event.information_cutoff_ns,
        lambda body: _fee_schedule_from_dict(body), lambda item: item.key == candidate.key,
    )
    if venue is None or stress is None or fee is None:
        return None, "EXACT_VENUE_STRESS_OR_FEE_EVIDENCE_UNAVAILABLE_OR_AMBIGUOUS"
    result = ProductionRiskInputsV1(
        product, risk_policy_v1, risk_policy_v2, account, tuple(exposures), tuple(outcomes), venue, stress, fee,
    )
    if not result.complete:
        return None, "MANDATORY_HARD_RISK_EVIDENCE_UNAVAILABLE"
    binding = {
        "version": "OPS_RISK_EVIDENCE_RESOLUTION_V1", "event_id": event.event_id,
        "candidate_set_ref": candidate_set.content_hash, "candidate_ref": candidate.content_hash,
        "product_ref": product.content_hash, "risk_policy_v1_ref": risk_policy_v1.policy_hash(),
        "risk_policy_v2_ref": risk_policy_v2.policy_hash, "account_ref": account.content_hash,
        "risk_input_refs": sorted({product.content_hash, risk_policy_v1.policy_hash(), risk_policy_v2.policy_hash,
                                    account.content_hash, venue.content_hash, stress.content_hash, fee.content_hash,
                                    *(item.content_hash for item in exposures),
                                    *(item.content_hash for item in outcomes)}),
        "resolved_at_ns": now_ns, "authority": "ZERO",
    }
    binding_ref = sha256_json(binding)
    repository.register_artifact(ArtifactIndexEntryV2(
        binding_ref, "OpsRiskEvidenceResolutionV1", binding_ref, now_ns, now_ns,
        {"resolution": binding},
    ))
    return result, None


def _resolve_declared_economic_inputs(
    repository: OpsRepository, event: OpsDecisionEventV1, candidate_set: CandidateSetV2,
    candidate: CandidateActionV2, action: ActionArtifactV2, risk: ProductionRiskInputsV1,
    *, now_ns: int, clock_ns: Callable[[], int] | None,
) -> tuple[ProductionEconomicInputsV1 | None, str | None, bool]:
    """Bind predeclared sources after action creation; raw vintages stay at T0."""
    from ..science.scenario_engine import scenario_seed
    from .economic_binding import bind_economic_templates
    from .economic_sources import resolve_economic_source_manifest

    if risk.product is None or risk.account is None or risk.fee is None:
        return None, "MANDATORY_HARD_RISK_EVIDENCE_UNAVAILABLE", True
    manifest, reason = resolve_economic_source_manifest(repository, product=risk.product,
        account_scope=risk.account.account_scope, policy_hash=candidate.policy_hash,
        cutoff_ns=event.information_cutoff_ns)
    if manifest is None:
        return None, reason, reason != "ECONOMIC_SOURCE_MANIFEST_MISSING"
    capabilities: list[VenueCapabilitySnapshotV2] = []
    for entry in _bounded_evidence(repository, "VenueCapabilitySnapshotV2",
            cutoff_ns=event.information_cutoff_ns):
        try:
            capability = VenueCapabilitySnapshotV2.from_dict(json_value(entry.metadata["capability"]))
        except (KeyError, TypeError, ValueError):
            continue
        if (capability.content_hash == entry.content_hash == entry.artifact_ref
                and capability.available_at_ns == entry.available_at_ns
                and capability.product_ref == risk.product.content_hash
                and capability.instrument_key_ref == candidate.key.content_hash
                and capability.account_scope == risk.account.account_scope):
            capabilities.append(capability)
    if not capabilities:
        return None, "EXACT_VENUE_CAPABILITY_UNAVAILABLE", True
    latest = max(item.available_at_ns for item in capabilities)
    recent = [item for item in capabilities if item.available_at_ns == latest]
    if len({item.content_hash for item in recent}) != 1:
        return None, "AMBIGUOUS_EXACT_VENUE_CAPABILITY", True
    capability = recent[0]
    identity = {"version": "OPS_DECLARED_ECONOMIC_RESOLUTION_V1", "event_id": event.event_id,
        "candidate_set_ref": candidate_set.content_hash, "candidate_ref": candidate.content_hash,
        "action_artifact_ref": action.content_hash}
    resolution_ref = sha256_json(identity)
    prior = repository.get_artifact(resolution_ref)
    clock = clock_ns if clock_ns is not None else lambda: now_ns
    consumer_at = sample(clock, floor_ns=max(now_ns, action.available_at_ns))
    if prior is not None:
        body = prior.metadata.get("resolution")
        if (not isinstance(body, Mapping) or any(body.get(key) != value for key, value in identity.items())
                or prior.artifact_type != "OpsEconomicEvidenceResolutionV1"
                or prior.content_hash != sha256_json(body)
                or body.get("source_manifest_ref") != manifest.content_hash
                or body.get("capability_ref") != capability.content_hash
                or body.get("available_at_ns") != prior.available_at_ns
                or body.get("authority") != "ZERO"
                or not causal_artifact(repository, resolution_ref, cutoff_ns=event.information_cutoff_ns,
                    consumer_at_ns=consumer_at, deadline_ns=event.deadline_ns)):
            return None, "SEALED_ECONOMIC_RESOLUTION_CONFLICT", True
        joint_refs, support_refs = tuple(body["joint_data_refs"]), tuple(body["support_unit_refs"])
        try:
            expected_joint, expected_support = bind_economic_templates(repository, action=action,
                cutoff_ns=event.information_cutoff_ns, deadline_ns=candidate.deadline_ns,
                execution_model_ref=manifest.execution_model_input.ref, fee_ref=risk.fee.content_hash,
                joint_data_refs=manifest.joint_data_refs, support_unit_refs=manifest.support_unit_refs,
                clock_ns=clock)
        except (TypeError, ValueError, KeyError):
            return None, "SEALED_ECONOMIC_RESOLUTION_CONFLICT", True
        if (joint_refs != expected_joint or support_refs != expected_support
                or body.get("market_information_cutoff_ns") != event.information_cutoff_ns
                or body.get("consumer_deadline_ns") != candidate.deadline_ns
                or body.get("action_hash") != action.action.action_hash
                or body.get("product_ref") != risk.product.content_hash
                or body.get("account_ref") != risk.account.content_hash):
            return None, "SEALED_ECONOMIC_RESOLUTION_CONFLICT", True
        published_at = prior.available_at_ns
    else:
        if consumer_at >= min(event.deadline_ns, candidate.deadline_ns):
            return None, "ECONOMIC_EVIDENCE_UNAVAILABLE_BEFORE_ACTION_DEADLINE", True
        try:
            joint_refs, support_refs = bind_economic_templates(repository, action=action,
                cutoff_ns=event.information_cutoff_ns, deadline_ns=candidate.deadline_ns,
                execution_model_ref=manifest.execution_model_input.ref, fee_ref=risk.fee.content_hash,
                joint_data_refs=manifest.joint_data_refs, support_unit_refs=manifest.support_unit_refs,
                clock_ns=clock)
        except (TypeError, ValueError, KeyError):
            return None, "EXACT_ECONOMIC_TEMPLATE_BINDING_UNSUPPORTED", True
        input_refs = tuple(sorted({manifest.content_hash, candidate_set.content_hash, candidate.content_hash,
            action.content_hash, risk.product.content_hash, risk.account.content_hash, capability.content_hash,
            *manifest.input_refs, *joint_refs, *support_refs}))
        started = sample(clock, floor_ns=max(consumer_at,
            *(_required_artifact(repository, ref).available_at_ns for ref in input_refs)))
        finished = sample(clock, floor_ns=started)
        published_at = sample(clock, floor_ns=finished)
        if published_at >= min(event.deadline_ns, candidate.deadline_ns):
            return None, "ECONOMIC_EVIDENCE_UNAVAILABLE_BEFORE_ACTION_DEADLINE", True
        body = {**identity, "market_information_cutoff_ns": event.information_cutoff_ns,
            "consumer_deadline_ns": candidate.deadline_ns, "action_hash": action.action.action_hash,
            "source_manifest_ref": manifest.content_hash, "product_ref": risk.product.content_hash,
            "account_ref": risk.account.content_hash, "capability_ref": capability.content_hash,
            "causal_input_refs": [manifest.model_input.ref, manifest.calibration_input.ref,
                manifest.execution_model_input.ref], "joint_data_refs": list(joint_refs),
            "support_unit_refs": list(support_refs), "available_at_ns": published_at, "authority": "ZERO"}
        with repository.atomic_composition():
            repository.register_artifact(ArtifactIndexEntryV2(resolution_ref, "OpsEconomicEvidenceResolutionV1",
                sha256_json(body), started, published_at, {"resolution": body, "input_refs": list(input_refs)}))
            record_computation(repository, artifact_ref=resolution_ref,
                information_cutoff_ns=event.information_cutoff_ns, started_ns=started, finished_ns=finished,
                available_ns=published_at, input_refs=input_refs, deadline_ns=candidate.deadline_ns)
    result = ProductionEconomicInputsV1(manifest.admission_policy, capability, manifest.model_input,
        manifest.calibration_input, manifest.execution_model_input, published_at,
        scenario_seed(action.action.action_hash, event.information_cutoff_ns), manifest.scenario_count,
        manifest.source_inputs, joint_refs, support_refs, manifest.execution_residual_refs,
        manifest.stress_input_ref, manifest.existing_portfolio_path_refs)
    return result, None, True


def _resolve_indexed_economic_inputs(
    repository: OpsRepository, event: OpsDecisionEventV1, candidate_set: CandidateSetV2,
    candidate: CandidateActionV2, action: ActionArtifactV2, risk: ProductionRiskInputsV1,
    *, now_ns: int, clock_ns: Callable[[], int] | None = None,
) -> tuple[ProductionEconomicInputsV1 | None, str | None]:
    declared, declared_reason, handled = _resolve_declared_economic_inputs(
        repository, event, candidate_set, candidate, action, risk, now_ns=now_ns, clock_ns=clock_ns)
    if handled:
        return declared, declared_reason
    policy_rows: list[tuple[ArtifactIndexEntryV2, AdmissionPolicyV2]] = []
    for entry in _bounded_evidence(repository, "OpsAdmissionPolicyEvidenceV1"):
        body = entry.metadata.get("admission_policy_evidence")
        if (
            entry.available_at_ns > event.information_cutoff_ns
            or not isinstance(body, Mapping)
            or body.get("version") != "OPS_ADMISSION_POLICY_EVIDENCE_V1"
            or body.get("event_id") != event.event_id
            or body.get("candidate_set_ref") != candidate_set.content_hash
            or body.get("candidate_ref") != candidate.content_hash
            or body.get("product_ref") != (risk.product.content_hash if risk.product else None)
            or body.get("available_at_ns") != entry.available_at_ns
            or sha256_json(body) != entry.artifact_ref
            or not isinstance(body.get("admission_policy"), Mapping)
        ):
            continue
        try:
            policy = AdmissionPolicyV2.from_dict(json_value(body["admission_policy"]))
        except (TypeError, ValueError):
            continue
        if policy.content_hash == sha256_json(policy.to_dict()):
            policy_rows.append((entry, policy))
    if len(policy_rows) != 1:
        return None, "ADMISSION_POLICY_MISSING_OR_AMBIGUOUS"
    admission_policy_entry, admission_policy = policy_rows[0]

    capability_rows: list[tuple[ArtifactIndexEntryV2, VenueCapabilitySnapshotV2]] = []
    for entry in _bounded_evidence(repository, "VenueCapabilitySnapshotV2"):
        if entry.available_at_ns > event.information_cutoff_ns:
            continue
        body = entry.metadata.get("capability")
        if not isinstance(body, Mapping):
            continue
        try:
            capability = VenueCapabilitySnapshotV2.from_dict(json_value(body))
        except (TypeError, ValueError):
            continue
        if (capability.content_hash == entry.artifact_ref
                and capability.product_ref == (risk.product.content_hash if risk.product else "")
                and capability.instrument_key_ref == candidate.key.content_hash
                and risk.account is not None and capability.account_scope == risk.account.account_scope):
            capability_rows.append((entry, capability))
    if not capability_rows:
        return None, "EXACT_VENUE_CAPABILITY_UNAVAILABLE"
    latest_capability_time = max(entry.available_at_ns for entry, _cap in capability_rows)
    latest_capabilities = [(entry, cap) for entry, cap in capability_rows if entry.available_at_ns == latest_capability_time]
    if len({cap.content_hash for _entry, cap in latest_capabilities}) != 1:
        return None, "AMBIGUOUS_EXACT_VENUE_CAPABILITY"
    capability = latest_capabilities[0][1]

    inputs_by_role: dict[str, list[tuple[ArtifactIndexEntryV2, CausalInputV2]]] = {
        "M0": [], "CALIBRATION": [], "EXECUTION_MODEL": [],
    }
    for entry in _bounded_evidence(repository, "OpsCausalInputEvidenceV1"):
        if entry.available_at_ns > event.information_cutoff_ns:
            continue
        body = entry.metadata.get("input_evidence")
        if not isinstance(body, Mapping) or body.get("version") != "OPS_CAUSAL_INPUT_EVIDENCE_V1":
            continue
        if (body.get("event_id") != event.event_id
                or body.get("candidate_set_ref") != candidate_set.content_hash
                or body.get("candidate_ref") != candidate.content_hash
                or body.get("product_ref") != (risk.product.content_hash if risk.product else None)):
            continue
        role = body.get("role")
        if role not in inputs_by_role or not isinstance(body.get("input"), Mapping):
            continue
        try:
            causal_input = CausalInputV2.from_dict(body["input"])
        except (TypeError, ValueError):
            continue
        source_entry = repository.get_artifact(causal_input.ref)
        if (sha256_json(body) == entry.artifact_ref and entry.content_hash == entry.artifact_ref
                and body.get("available_at_ns") == entry.available_at_ns
                and source_entry is not None and source_entry.content_hash == causal_input.ref
                and source_entry.artifact_type == causal_input.kind
                and source_entry.available_at_ns <= causal_input.available_at_ns
                and causal_input.available_at_ns <= event.information_cutoff_ns):
            inputs_by_role[str(role)].append((entry, causal_input))
    if any(len(inputs_by_role[role]) != 1 for role in inputs_by_role):
        return None, "CAUSAL_M0_CALIBRATION_OR_EXECUTION_INPUT_MISSING_OR_AMBIGUOUS"

    configs: list[tuple[ArtifactIndexEntryV2, Mapping[str, Any]]] = []
    for entry in _bounded_evidence(repository, "OpsEconomicScenarioConfigV1"):
        body = entry.metadata.get("scenario_config")
        if (entry.available_at_ns <= event.information_cutoff_ns and isinstance(body, Mapping)
                and body.get("version") == "OPS_ECONOMIC_SCENARIO_CONFIG_V1"
                and body.get("event_id") == event.event_id
                and body.get("candidate_set_ref") == candidate_set.content_hash
                and body.get("candidate_ref") == candidate.content_hash
                and body.get("product_ref") == (risk.product.content_hash if risk.product else None)
                and body.get("available_at_ns") == entry.available_at_ns
                and sha256_json(body) == entry.artifact_ref):
            configs.append((entry, body))
    if len(configs) != 1:
        return None, "DETERMINISTIC_ECONOMIC_SCENARIO_CONFIG_MISSING_OR_AMBIGUOUS"
    config_entry, config = configs[0]
    seed, count, eval_available = (config.get("scenario_seed"), config.get("scenario_count"),
                                   config.get("evaluation_available_at_ns"))
    if (type(seed) is not int or seed < 0 or type(count) is not int or not 1 <= count <= 10_000
            or type(eval_available) is not int):
        return None, "DETERMINISTIC_ECONOMIC_SCENARIO_CONFIG_INVALID"

    by_role = {role: values[0][1] for role, values in inputs_by_role.items()}
    model, calibration, execution = (by_role["M0"], by_role["CALIBRATION"], by_role["EXECUTION_MODEL"])
    result = ProductionEconomicInputsV1(
        admission_policy, capability, model, calibration, execution, eval_available, seed, count,
    )
    binding = {
        "version": "OPS_ECONOMIC_EVIDENCE_RESOLUTION_V1", "event_id": event.event_id,
        "candidate_set_ref": candidate_set.content_hash, "candidate_ref": candidate.content_hash,
        "action_hash": action.action.action_hash, "action_artifact_ref": action.content_hash,
        "product_ref": risk.product.content_hash if risk.product else None,
        "account_ref": risk.account.content_hash if risk.account else None,
        "admission_policy_ref": admission_policy.content_hash,
        "admission_policy_evidence_ref": admission_policy_entry.artifact_ref,
        "capability_ref": capability.content_hash,
        "causal_input_refs": [model.ref, calibration.ref, execution.ref],
        "scenario_config_ref": config_entry.artifact_ref, "resolved_at_ns": now_ns,
        "authority": "ZERO",
    }
    binding_ref = sha256_json(binding)
    repository.register_artifact(ArtifactIndexEntryV2(
        binding_ref, "OpsEconomicEvidenceResolutionV1", binding_ref, now_ns, now_ns,
        {"resolution": binding},
    ))
    return result, None


def _risk_policy_v1_from_dict(body: Mapping[str, Any]) -> RiskPolicy:
    values = dict(body)
    values.pop("contract_version", None)
    decimal_fields = {
        "normal_loss_per_trade_frac", "aggregate_open_normal_loss_frac", "stress_loss_per_trade_frac",
        "portfolio_es_alpha", "portfolio_es_limit_frac", "account_gross_notional_limit",
        "instrument_notional_limit", "correlated_crypto_beta_limit", "venue_collateral_limit",
        "min_free_margin_reserve_frac", "drawdown_reduce_threshold", "drawdown_stop_threshold",
        "drawdown_reduce_recovery", "drawdown_stop_recovery", "max_contract_leverage",
        "external_capital_reference",
    }
    for name in decimal_fields:
        if values.get(name) is not None:
            values[name] = Decimal(str(values[name]))
    return RiskPolicy(**values)


def _account_risk_from_dict(body: Mapping[str, Any]) -> AccountRiskSnapshotV2:
    fields = set(AccountRiskSnapshotV2.__dataclass_fields__)
    values = {name: body[name] for name in fields}
    for name in ("eligible_equity", "margin_available", "current_margin", "drawdown",
                 "existing_open_normal_loss", "pending_reserved_normal_loss", "gross_notional",
                 "instrument_notional", "beta_notional", "venue_collateral", "existing_portfolio_es"):
        values[name] = Decimal(str(values[name]))
    for name in ("closed_outcome_refs", "pending_risk_refs", "existing_exposure_refs"):
        values[name] = tuple(values[name])
    return AccountRiskSnapshotV2(**values)


def _possible_risk_from_dict(body: Mapping[str, Any]) -> PossibleRiskV2:
    return PossibleRiskV2(
        ExposureKind(body["kind"]), InstrumentKeyV2.from_dict(body["key"]), body["available_at_ns"],
        Decimal(str(body["possible_normal_loss"])), Decimal(str(body["possible_stress_loss"])),
        Decimal(str(body["possible_notional"])), Decimal(str(body["signed_beta_notional"])),
        Decimal(str(body["possible_margin"])), Decimal(str(body["possible_venue_collateral"])),
        str(body["source_ref"]),
    )


def _closed_outcome_from_dict(body: Mapping[str, Any]) -> ClosedV2Outcome:
    return ClosedV2Outcome(body["close_at_ns"], body["available_at_ns"],
                            Decimal(str(body["realized_net_pnl"])), body["position_ref"],
                            OutcomeClass(body["outcome_class"]))


def _venue_sizing_from_dict(body: Mapping[str, Any]) -> VenueSizingLimitsV2:
    return VenueSizingLimitsV2(
        InstrumentKeyV2.from_dict(body["key"]), body["product_ref"], body["available_at_ns"],
        tuple(Decimal(str(item)) for item in body["allowed_leverages"]),
        Decimal(str(body["account_leverage_limit"])), Decimal(str(body["margin_addon_frac"])),
        body["source_ref"],
    )


def _stress_bound_from_dict(body: Mapping[str, Any]) -> StressBoundV2:
    return StressBoundV2(InstrumentKeyV2.from_dict(body["key"]), body["available_at_ns"],
                        Decimal(str(body["worst_executable_exit_price"])), body["source_ref"], body["version"])


def _fee_schedule_from_dict(body: Mapping[str, Any]) -> FeeScheduleV2:
    return FeeScheduleV2(InstrumentKeyV2.from_dict(body["key"]), body["available_at_ns"],
                         Decimal(str(body["entry_taker_rate"])), Decimal(str(body["exit_taker_rate"])),
                         body["source_ref"])


def _unique_latest_typed_risk_evidence(
    repository: OpsRepository, artifact_type: str, cutoff_ns: int, parser: Callable[[Mapping[str, Any]], Any],
    predicate: Callable[[Any], bool],
) -> Any | None:
    values: list[tuple[ArtifactIndexEntryV2, Any]] = []
    for entry in _bounded_evidence(repository, artifact_type, cutoff_ns=cutoff_ns):
        if entry.available_at_ns > cutoff_ns:
            continue
        body: Mapping[str, Any] = entry.metadata
        try:
            value = parser(body)
        except (KeyError, TypeError, ValueError):
            continue
        if value.content_hash == entry.artifact_ref and predicate(value):
            values.append((entry, value))
    if not values:
        return None
    latest = max(entry.available_at_ns for entry, _value in values)
    recent = [value for entry, value in values if entry.available_at_ns == latest]
    return recent[0] if len({value.content_hash for value in recent}) == 1 else None


def index_ops_causal_input_evidence(
    repository: OpsRepository, *, role: str, causal_input: CausalInputV2, available_at_ns: int,
    event_id: str, candidate_set_ref: str, candidate_ref: str, product_ref: str,
) -> str:
    """Index an operational role binding to an already indexed causal model artifact."""
    if role not in ("M0", "CALIBRATION", "EXECUTION_MODEL"):
        raise ValueError("unsupported runtime causal-input role")
    source = repository.get_artifact(causal_input.ref)
    if (source is None or source.content_hash != causal_input.ref or source.artifact_type != causal_input.kind
            or source.available_at_ns > causal_input.available_at_ns
            or causal_input.available_at_ns > available_at_ns):
        raise ValueError("runtime causal input must reference exact indexed event-valid evidence")
    if not event_id.strip():
        raise ValueError("runtime causal input event identity is required")
    for name, ref in (("candidate_set_ref", candidate_set_ref), ("candidate_ref", candidate_ref),
                      ("product_ref", product_ref)):
        sha256_ref(ref, field=name)
    body = {"version": "OPS_CAUSAL_INPUT_EVIDENCE_V1", "role": role,
            "event_id": event_id, "candidate_set_ref": candidate_set_ref,
            "candidate_ref": candidate_ref, "product_ref": product_ref,
            "input": causal_input.to_dict(), "available_at_ns": available_at_ns}
    ref = sha256_json(body)
    repository.register_artifact(ArtifactIndexEntryV2(
        ref, "OpsCausalInputEvidenceV1", ref, available_at_ns, available_at_ns,
        {"input_evidence": body},
    ))
    return ref


def index_ops_economic_scenario_config(
    repository: OpsRepository, *, scenario_seed: int, scenario_count: int,
    evaluation_available_at_ns: int, available_at_ns: int, event_id: str,
    candidate_set_ref: str, candidate_ref: str, product_ref: str,
) -> str:
    if (type(scenario_seed) is not int or scenario_seed < 0 or type(scenario_count) is not int
            or not 1 <= scenario_count <= 10_000 or type(evaluation_available_at_ns) is not int
            or type(available_at_ns) is not int or evaluation_available_at_ns <= available_at_ns):
        raise ValueError("runtime economic scenario configuration is invalid")
    if not event_id.strip():
        raise ValueError("runtime economic scenario event identity is required")
    for name, ref in (("candidate_set_ref", candidate_set_ref), ("candidate_ref", candidate_ref),
                      ("product_ref", product_ref)):
        sha256_ref(ref, field=name)
    body = {"version": "OPS_ECONOMIC_SCENARIO_CONFIG_V1", "event_id": event_id,
            "candidate_set_ref": candidate_set_ref, "candidate_ref": candidate_ref,
            "product_ref": product_ref, "scenario_seed": scenario_seed,
            "scenario_count": scenario_count, "evaluation_available_at_ns": evaluation_available_at_ns,
            "available_at_ns": available_at_ns, "authority": "ZERO"}
    ref = sha256_json(body)
    repository.register_artifact(ArtifactIndexEntryV2(
        ref, "OpsEconomicScenarioConfigV1", ref, available_at_ns, available_at_ns,
        {"scenario_config": body},
    ))
    return ref


def index_ops_admission_policy_evidence(
    repository: OpsRepository, *, admission_policy: AdmissionPolicyV2, available_at_ns: int,
    event_id: str, candidate_set_ref: str, candidate_ref: str, product_ref: str,
) -> str:
    """Bind an accepted static admission policy to one runtime evaluation event."""
    if not event_id.strip() or type(available_at_ns) is not int:
        raise ValueError("runtime admission-policy event identity and availability are required")
    for name, ref in (("candidate_set_ref", candidate_set_ref), ("candidate_ref", candidate_ref),
                      ("product_ref", product_ref)):
        sha256_ref(ref, field=name)
    body = {
        "version": "OPS_ADMISSION_POLICY_EVIDENCE_V1",
        "event_id": event_id,
        "candidate_set_ref": candidate_set_ref,
        "candidate_ref": candidate_ref,
        "product_ref": product_ref,
        "admission_policy": admission_policy.to_dict(),
        "available_at_ns": available_at_ns,
        "authority": "ZERO",
    }
    ref = sha256_json(body)
    repository.register_artifact(ArtifactIndexEntryV2(
        ref, "OpsAdmissionPolicyEvidenceV1", ref, available_at_ns, available_at_ns,
        {"admission_policy_evidence": body},
    ))
    return ref


def _indexed_public_prerequisite_evidence(repository: OpsRepository, product: ProductContractV2,
                                          *, cutoff_ns: int) -> dict[str, Any]:
    """Reuse exact typed facts already present; never supply public-shadow defaults."""
    supplied: dict[str, Any] = {}
    for role, kind, parser, predicate in (
        ("ACCOUNT", "AccountRiskSnapshotV2", _account_risk_from_dict,
            lambda item: item.operational_status == "CURRENT"),
        ("FEE", "FeeScheduleV2", _fee_schedule_from_dict, lambda item: item.key == product.key),
        ("STRESS", "StressBoundV2", _stress_bound_from_dict, lambda item: item.key == product.key),
        ("VENUE_SIZING", "VenueSizingLimitsV2", _venue_sizing_from_dict,
            lambda item: item.key == product.key and item.product_ref == product.content_hash),
        ("VENUE_CAPABILITY", "VenueCapabilitySnapshotV2",
            lambda body: VenueCapabilitySnapshotV2.from_dict(json_value(body["capability"])),
            lambda item: item.instrument_key_ref == product.key.content_hash
                and item.venue == product.key.venue and item.environment == product.key.environment),
    ):
        value = _unique_latest_typed_risk_evidence(repository, kind, cutoff_ns, parser, predicate)
        if value is not None:
            supplied[role] = value
    policies: dict[str, list[tuple[int, Any]]] = {"POLICY_V1": [], "POLICY_V2": []}
    for role, kind, policy_parser in (("POLICY_V1", "RiskPolicyV1", _risk_policy_v1_from_dict),
                              ("POLICY_V2", "RiskPolicyV2", RiskPolicyV2.from_dict)):
        for entry in _bounded_evidence(repository, kind, cutoff_ns=cutoff_ns):
            try:
                policy_value: Any = policy_parser(json_value(entry.metadata["policy"]))
                digest = policy_value.policy_hash() if role == "POLICY_V1" else policy_value.policy_hash
                effective = policy_value.policy_effective_at_ns if role == "POLICY_V1" else policy_value.effective_at_ns
                if digest == entry.content_hash == entry.artifact_ref and effective <= cutoff_ns:
                    policies[role].append((effective, policy_value))
            except (KeyError, TypeError, ValueError):
                continue
        if policies[role]:
            latest = max(at for at, _ in policies[role])
            recent = [policy_item for at, policy_item in policies[role] if at == latest]
            if len(recent) == 1:
                supplied[role] = recent[0]
    if "POLICY_V1" in supplied and "POLICY_V2" in supplied and (
            supplied["POLICY_V1"].policy_hash() != supplied["POLICY_V2"].base_v1_risk_policy_hash):
        supplied.pop("POLICY_V1")
        supplied.pop("POLICY_V2")
    if "ACCOUNT" in supplied and "VENUE_CAPABILITY" in supplied and (
            supplied["ACCOUNT"].account_scope != supplied["VENUE_CAPABILITY"].account_scope):
        supplied.pop("VENUE_CAPABILITY")
    from .economic_sources import declared_execution_model
    execution_model = declared_execution_model(repository, product,
        supplied["ACCOUNT"].account_scope if "ACCOUNT" in supplied else None, cutoff_ns)
    if execution_model is not None:
        supplied["EXECUTION_MODEL"] = execution_model
    return supplied


def _causal_products(repository: OpsRepository, *, cutoff_ns: int,
                     service: Callable[[], None] | None = None) -> tuple[ProductContractV2, ...]:
    """One latest effective, cutoff-visible product receipt per exact full key."""
    from .broad_universe import active_products

    broad = active_products(repository, cutoff_ns=cutoff_ns, service=service)
    if broad is not None:
        return broad
    selected: dict[str, ProductContractV2] = {}
    for entry in _bounded_evidence(repository, "ProductContractV2", cutoff_ns=cutoff_ns):
        product = ProductContractV2.from_dict(json_value(entry.metadata["product"]))
        if (product.content_hash != entry.artifact_ref or product.content_hash != entry.content_hash
                or product.available_at_ns != entry.available_at_ns):
            raise ValueError("causal product receipt identity or chronology conflict")
        if product.effective_at_ns > cutoff_ns:
            continue
        if product.listing_at_ns is not None and product.listing_at_ns > cutoff_ns:
            continue
        identity = product.key.to_canonical_json()
        previous = selected.get(identity)
        if previous is None or product.effective_at_ns > previous.effective_at_ns:
            selected[identity] = product
        elif product.effective_at_ns == previous.effective_at_ns and product.content_hash != previous.content_hash:
            raise ValueError("causal product effective revision is ambiguous")
    if len(selected) > 128:
        error = ActiveEvidenceOverflowV1({"version": "OpsActiveWorkPressureV1",
            "artifact_type": "ProductContractV2", "limit": 128, "has_more": True,
            "reason": "ACTIVE_PRODUCT_POPULATION_OVERFLOW", "authority": "ZERO"})
        error.publish(repository)
        raise error
    return tuple(selected[key] for key in sorted(selected))


def _indexed_event_prerequisites(repository: OpsRepository, *, cutoff_ns: int) -> dict[str, Any]:
    """Reevaluate exact typed facts for this cutoff/key, never reuse another gate.

    Calendar/abnormality facts have no instrument scope in their frozen wire.
    Incidents are scoped by the existing builder. Ambiguous latest facts abstain;
    whole event/incident populations exceeding 64 fail with visible pressure.
    """
    from ..news.events import (
        AbnormalityEvidenceV2,
        CalendarCoverageV2,
        OperationalIncidentV2,
        ScheduledEventV2,
    )

    result: dict[str, Any] = {}
    for name, kind, model, latest in (
        ("coverage", "CalendarCoverageV2", CalendarCoverageV2, True),
        ("abnormality", "AbnormalityEvidenceV2", AbnormalityEvidenceV2, True),
        ("scheduled_events", "ScheduledEventV2", ScheduledEventV2, False),
        ("incidents", "OperationalIncidentV2", OperationalIncidentV2, False),
    ):
        if latest:
            page = repository.latest_artifact_entries(kind, as_of_ns=cutoff_ns, limit=2)
            if page.invalid_entry_count:
                raise ValueError("indexed event prerequisite contains invalid evidence")
            entries = page.entries
            if len(entries) > 1 and entries[0].available_at_ns == entries[1].available_at_ns:
                result[name] = None
                continue
            entries = entries[:1]
        elif kind == "ScheduledEventV2":
            page = repository.scheduled_event_window(cutoff_ns=cutoff_ns)
            if page.has_more or page.invalid_entry_count:
                raise ValueError("scheduled event window exceeds its validated bound")
            entries = page.entries
        else:
            entries = _bounded_evidence(repository, kind, cutoff_ns=cutoff_ns, limit=64)
        values: list[Any] = []
        for entry in entries:
            wire = entry.metadata.get("evidence", entry.metadata)
            if not isinstance(wire, Mapping):
                raise ValueError("indexed event prerequisite wire is invalid")
            body = dict(wire)
            if body.pop("schema_version", None) != 1:
                raise ValueError("indexed event prerequisite schema is invalid")
            value = model(**body)
            if (value.content_hash != entry.artifact_ref or entry.content_hash != entry.artifact_ref
                    or value.available_at_ns != entry.available_at_ns):
                raise ValueError("indexed event prerequisite identity or chronology is invalid")
            values.append(value)
        result[name] = values[0] if latest and values else (None if latest else tuple(values))
    return result


def _publish_public_prerequisites(repository: OpsRepository, event: OpsDecisionEventV1,
        product: ProductContractV2, *, clock_ns: Callable[[], int]) -> Any:
    from .research_prerequisites import publish_research_prerequisites

    started = sample(clock_ns, floor_ns=event.information_cutoff_ns)
    publication = publish_research_prerequisites(repository, event=event, product=product, clock_ns=clock_ns,
        evidence=_indexed_public_prerequisite_evidence(repository, product, cutoff_ns=event.information_cutoff_ns),
        **_indexed_event_prerequisites(repository, cutoff_ns=event.information_cutoff_ns))
    inventory = _required_artifact(repository, publication.inventory_ref).metadata["prerequisites"]
    for ref in publication.refs:
        target = _required_artifact(repository, ref)
        # Restart preserves the original computation receipt and actual publication.
        from ..chronology import chronology_ref
        if repository.get_artifact(chronology_ref(ref)) is None:
            record_computation(repository, artifact_ref=ref, information_cutoff_ns=event.information_cutoff_ns,
                started_ns=started, finished_ns=publication.available_at_ns, available_ns=target.available_at_ns,
                input_refs=tuple(inventory["input_refs"]), deadline_ns=event.deadline_ns)
    return publication


def _prepare_public_histories(repository: OpsRepository, event: OpsDecisionEventV1, *,
                              clock_ns: Callable[[], int], service: Callable[[], None] | None,
                              products: tuple[ProductContractV2, ...],
                              ) -> dict[str, dict[BarIntervalV2, ActiveHistoryPageV1]]:
    """Prepare immutable cutoff-bound histories before watch/seal atomicity.

    Stream service publishes only later evidence, which cannot enter this
    captured event prefix. History certificates are independent evidence and
    keep their own atomic head publication on failure of later composition.
    """
    pages: dict[str, dict[BarIntervalV2, ActiveHistoryPageV1]] = {}
    for product in products:
        frames = {}
        for interval in (BarIntervalV2.M15, BarIntervalV2.H1, BarIntervalV2.H4, BarIntervalV2.M1):
            frames[interval] = maintain_history(repository, Path(repository.path).parent / "ops-observations",
                key=product.key, interval=interval, cutoff_ns=event.information_cutoff_ns,
                clock_ns=clock_ns, deadline_ns=event.deadline_ns, service=service)
            # Validate retained bar locators in small units before entering the
            # transaction owning watch changes. No market cutoff is advanced.
            bars = frames[interval].bars
            for offset in range(0, len(bars), 64):
                entries = tuple(_causal_bar_entry(item.bar, item.observation_index_ref, repository=repository)
                                for item in bars[offset:offset + 64])
                existing = repository.get_artifact_metadata_by_refs(tuple(entry.artifact_ref for entry in entries))
                missing = []
                for entry in entries:
                    prior = existing.get(entry.artifact_ref)
                    if prior is None:
                        missing.append(entry)
                    elif (prior.get("artifact_type") != "CausalBarV2"
                          or prior.get("content_hash") != entry.content_hash
                          or prior.get("available_at_ns") != entry.available_at_ns
                          or not isinstance(prior.get("metadata"), Mapping)
                          or canonical_json(prior["metadata"].get("bar"))
                          != canonical_json(entry.metadata.get("bar"))):
                        raise ValueError("causal bar ref already indexes conflicting immutable evidence")
                if missing:
                    repository.register_artifacts(tuple(missing))
                if service is not None:
                    service()
            if service is not None:
                service()
        pages[product.key.to_canonical_json()] = frames
    return pages


def _compose_public_event_inputs(
    repository: OpsRepository,
    event: OpsDecisionEventV1,
    trigger_body: Mapping[str, Any],
    *, clock_ns: Callable[[], int] = time.time_ns,
    prepared_histories: dict[str, dict[BarIntervalV2, ActiveHistoryPageV1]],
    prepared_products: tuple[ProductContractV2, ...],
    prepared_broad_universe: UniverseContractV2 | None,
) -> ProductionEventInputsV1:
    """Run each sleeve only on its frozen native cadence and archived inputs."""
    if (trigger_body.get("version") != "OPS_PUBLIC_FINAL_BAR_TRIGGER_V1"
            or trigger_body.get("information_cutoff_ns") != event.information_cutoff_ns
            or event.event_type not in {"CONFIRMED_15M_CLOSE", S3_M1_EVENT_TYPE}):
        return _empty_event_inputs(repository, event, clock_ns=clock_ns)
    composition_started = sample(clock_ns, floor_ns=event.information_cutoff_ns)
    native_m1 = event.event_type == S3_M1_EVENT_TYPE
    trigger_interval = BarIntervalV2.M1 if native_m1 else BarIntervalV2.M15
    product_entry = repository.get_artifact(str(trigger_body.get("product_ref", "")))
    product_body = product_entry.metadata.get("product") if product_entry is not None else None
    if (product_entry is None or product_entry.artifact_type != "ProductContractV2"
            or not isinstance(product_body, Mapping)):
        return _empty_event_inputs(repository, event, clock_ns=clock_ns)
    trigger_product = ProductContractV2.from_dict(json_value(product_body))
    if trigger_product.content_hash != product_entry.artifact_ref:
        return _empty_event_inputs(repository, event, clock_ns=clock_ns)

    archive_root = Path(repository.path).parent / "ops-observations"
    products = prepared_products

    histories: dict[str, dict[BarIntervalV2, tuple[Any, ...]]] = {}
    history_pages: dict[str, dict[BarIntervalV2, ActiveHistoryPageV1]] = {}
    history_missing_reasons: set[str] = set()
    store = CausalBarStoreV2()
    source_refs: set[str] = {event.trigger_ref, trigger_product.content_hash}
    latest_health: dict[str, PublicSourceHealthV2] = {}
    for source_id in repository.source_health_sources():
        health_entry = _latest_public_health(repository, source_id, cutoff_ns=event.information_cutoff_ns)
        if health_entry is not None:
            health = PublicSourceHealthV2.from_dict(health_entry.metadata["health"])
            if health.content_hash != health_entry.artifact_ref:
                raise ValueError("latest public health identity conflict")
            latest_health[source_id] = health
    source_refs.update(item.content_hash for item in latest_health.values())

    for product in products:
        frames: dict[BarIntervalV2, tuple[Any, ...]] = {}
        pages: dict[BarIntervalV2, ActiveHistoryPageV1] = {}
        for interval in (BarIntervalV2.M15, BarIntervalV2.H1, BarIntervalV2.H4, BarIntervalV2.M1):
            page = prepared_histories[product.key.to_canonical_json()][interval]
            pages[interval] = page
            # Only histories required by this native decision can block its
            # candidate generation. M1 belongs to S3; missing M1 data must not
            # suppress an otherwise complete S1/S2 M15 decision. Other products
            # retain their own universe eligibility and history diagnostics.
            if (not page.ready and product.key == trigger_product.key
                    and (native_m1 or interval != BarIntervalV2.M1)):
                history_missing_reasons.add("HISTORY:" + interval.value + ":" + page.reason_code)
            if page.ready and page.state is not None:
                source_refs.add(page.state.content_hash)
            indexed_bars = page.bars
            bars = tuple(item.bar for item in indexed_bars)
            frames[interval] = bars
            for item in indexed_bars:
                store.append(item.bar)
                source_refs.update((item.observation_index_ref, item.bar.content_hash))
        histories[product.key.to_canonical_json()] = frames
        history_pages[product.key.to_canonical_json()] = pages

    primary_frames = histories.get(trigger_product.key.to_canonical_json(), {})
    primary_trigger_bars = primary_frames.get(trigger_interval, ())
    trigger_bar = next((bar for bar in primary_trigger_bars
                        if bar.content_hash == trigger_body.get("bar_ref")), None)
    if trigger_bar is None:
        empty = _empty_event_inputs(repository, event, clock_ns=clock_ns)
        return replace(empty, generation_missing_reasons=tuple(sorted({
            *empty.generation_missing_reasons, *history_missing_reasons})))
    if (trigger_bar.interval != trigger_interval
            or (native_m1 and event.event_id != s3_m1_event_id(trigger_product.key, trigger_bar.close_at_ns))
            or trigger_bar.raw.source_id != event.source_id or trigger_bar.close_at_ns > event.information_cutoff_ns
            or trigger_bar.raw.available_at_ns > event.information_cutoff_ns):
        return _empty_event_inputs(repository, event, clock_ns=clock_ns)

    market: dict[str, tuple[PublicSourceHealthV2, EventGate | None,
                            ExecutableQuote | None, MarkIndexEvidence | None, Decimal]] = {}
    observations_for_universe: list[UniverseObservationV2] = []
    universe_observation_refs: dict[str, str] = {}
    eligible_order: list[tuple[Decimal, str]] = []
    active_watches = _bounded_active_watches(repository)
    for product in products:
        key_json = product.key.to_canonical_json()
        frames = histories.get(key_json, {})
        m15 = frames.get(BarIntervalV2.M15, ())
        h1 = frames.get(BarIntervalV2.H1, ())
        h4 = frames.get(BarIntervalV2.H4, ())
        if not m15:
            continue
        m1 = frames.get(BarIntervalV2.M1, ())
        source_bar = (
            trigger_bar if native_m1 and product.key == trigger_product.key
            else m1[-1] if native_m1 and m1 else m15[-1]
        )
        source_id = source_bar.raw.source_id
        source_health = latest_health.get(source_id)
        if source_health is None or not source_health.data_eligible:
            continue
        quote, mark, quote_refs = _indexed_quote_and_mark(
            repository, archive_root, product, cutoff_ns=event.information_cutoff_ns,
        )
        if native_m1:
            stream_quote, stream_quote_refs, _recovery_epoch, _bbo_age = _indexed_s3_stream_quote(
                repository, product, cutoff_ns=event.information_cutoff_ns, rest_quote=quote,
            )
            quote_refs = tuple(sorted({*quote_refs, *stream_quote_refs}))
            if stream_quote is not None:
                quote = stream_quote
        prerequisite = _publish_public_prerequisites(repository, event, product, clock_ns=clock_ns)
        source_refs.update(prerequisite.refs)
        published_gate = _required_artifact(repository, prerequisite.event_gate_ref).metadata["gate"]
        gate: EventGate | None = EventGate(EventState(published_gate["state"]), prerequisite.available_at_ns,
            prerequisite.event_gate_ref, published_gate["gate_version"])
        gate_ref = prerequisite.event_gate_ref
        source_refs.update((source_health.content_hash, *quote_refs))
        if gate_ref is not None:
            source_refs.add(gate_ref)
        source_refs.update(bar.content_hash for bar in (*m15, *h1, *h4))
        if quote is not None:
            source_refs.add(quote.evidence_ref)
        if mark is not None:
            source_refs.add(mark.evidence_ref)
        daily_bars = tuple(bar for bar in m15
                           if bar.close_at_ns > event.information_cutoff_ns - 24 * 60 * 60 * 1_000_000_000)
        turnover = sum((bar.close * bar.volume * product.base_units_per_contract
                        for bar in daily_bars), Decimal(0))
        quote_valid = quote is not None and quote.valid_at(event.information_cutoff_ns)
        spread = ((quote.ask - quote.bid) / ((quote.ask + quote.bid) / Decimal(2)) * Decimal(10_000)
                  if quote_valid and quote is not None else None)
        pages = history_pages[key_json]
        prefix_counts = {frame: page.state.total_count if page.ready and page.state is not None else 0
                         for frame, page in pages.items()}
        m15_state = pages[BarIntervalV2.M15].state
        observed_days = (m15_state.observed_unique_utc_close_days
                         if pages[BarIntervalV2.M15].ready and m15_state is not None else 0)
        s1_days = min(prefix_counts[BarIntervalV2.H4] // 6, prefix_counts[BarIntervalV2.H1] // 24,
                      prefix_counts[BarIntervalV2.M15] // 96)
        s2_days = prefix_counts[BarIntervalV2.M15] // 96
        s3_days = prefix_counts[BarIntervalV2.M1] // 1440
        s6_days = min(prefix_counts[BarIntervalV2.H1] // 24,
                      prefix_counts[BarIntervalV2.H4] // 6)
        # Keep the M15 universe snapshot at the same S3 watch boundary as its
        # frozen cadence, so native M1 watch timing cannot change S1/S2 inputs.
        m15_decision_close = (
            trigger_bar.close_at_ns if trigger_interval == BarIntervalV2.M15
            else event.source_event_at_ns
        )
        previous_m15_close = max(
            (bar.close_at_ns for bar in m15 if bar.close_at_ns < m15_decision_close),
            default=m15_decision_close - BarIntervalV2.M15.duration_ns,
        )
        active_watch = any(
            watch.key == product.key
            and (native_m1 or watch.policy_hash != S3_POLICY.policy_hash
                 or watch.created_at_ns <= previous_m15_close)
            for watch in active_watches
        )
        health_age_ok = (source_health.available_at_ns <= event.information_cutoff_ns
                         and source_health.observed_at_ns <= event.information_cutoff_ns)
        if quote_valid and spread is not None and health_age_ok:
            refs = tuple(sorted({product.content_hash, source_health.content_hash, *quote_refs,
                                 *(bar.content_hash for bar in (*m15, *h1, *h4)),
                                 *(page.state.content_hash for page in pages.values()
                                   if page.ready and page.state is not None)}))
            observation = UniverseObservationV2(
                product, observed_days, bool(m15 and h1 and h4), turnover, spread,
                source_health.state, event.information_cutoff_ns,
                {S1_POLICY.policy_id: 30, S2_POLICY.policy_id: 30,
                 S3_POLICY.policy_id: 7, S6_POLICY.policy_id: 30}, source_health.available_at_ns,
                open_position=False,
                active_watch=active_watch,
                observed_policy_history_days={S1_POLICY.policy_id: s1_days,
                    S2_POLICY.policy_id: s2_days, S3_POLICY.policy_id: s3_days,
                    S6_POLICY.policy_id: s6_days},
            )
            observations_for_universe.append(observation)
            observation_finished = sample(clock_ns, floor_ns=composition_started)
            observation_available = sample(clock_ns, floor_ns=observation_finished)
            observation_body = {
                "version": "UNIVERSE_OBSERVATION_INDEX_V2_V1",
                "product_ref": product.content_hash,
                "observation_hash": observation.content_hash,
                "source_refs": list(refs),
                "available_at_ns": observation_available,
            }
            observation_ref = observation.content_hash
            repository.register_artifact(ArtifactIndexEntryV2(
                observation_ref, "UniverseObservationV2", observation_ref,
                observation_finished, observation_available,
                {"observation": observation_body},
            ))
            record_computation(repository, artifact_ref=observation_ref,
                information_cutoff_ns=event.information_cutoff_ns, started_ns=composition_started,
                finished_ns=observation_finished, available_ns=observation_available, input_refs=refs,
                deadline_ns=event.deadline_ns)
            universe_observation_refs[key_json] = observation_ref
            source_refs.add(observation_ref)
            eligible_order.append((turnover, key_json))
        market[key_json] = (source_health, gate, quote, mark, product.tick_size)

    universe_started = sample(clock_ns, floor_ns=max(event.information_cutoff_ns,
        *(_required_artifact(repository, ref).available_at_ns for ref in source_refs)))
    built = DynamicUniverseRuntimeV2().build_snapshot(
        # The slot bounds consumption of this derived snapshot; the market
        # information cutoff remains fixed independently in its causal receipt.
        # Native S3 uses the same deadline-slot convention.
        tuple(observations_for_universe), decision_slot_ns=event.deadline_ns,
        information_cutoff_ns=event.information_cutoff_ns, created_at_ns=universe_started,
        publication_at_ns=universe_started, selection_policy_hash=SELECTION_POLICY_HASH,
        input_refs=tuple(sorted(source_refs)),
    )
    broad_universe = prepared_broad_universe
    if broad_universe is not None:
        entries = {item.key.to_canonical_json(): item for item in broad_universe.entries}
        entries.update({item.key.to_canonical_json(): item for item in built.universe.entries})
        refs = tuple(sorted({*built.universe.envelope.input_refs, broad_universe.content_hash,
                             *(entry.product_ref for entry in entries.values())}))
        built = replace(built, universe=replace(built.universe,
            entries=tuple(entries[key] for key in sorted(entries)),
            envelope=replace(built.universe.envelope, content_hash="", input_refs=refs)))
        source_refs.add(broad_universe.content_hash)
    universe_available = sample(clock_ns, floor_ns=universe_started)
    built = replace(built, universe=replace(built.universe, envelope=replace(built.universe.envelope,
        content_hash="", created_at_ns=universe_available, available_at_ns=universe_available)))
    _index_universe(repository, built.universe)
    record_computation(repository, artifact_ref=built.universe.content_hash,
        information_cutoff_ns=event.information_cutoff_ns, started_ns=universe_started,
        finished_ns=universe_available, available_ns=universe_available,
        input_refs=tuple(source_refs), deadline_ns=event.deadline_ns)
    research_started = sample(clock_ns, floor_ns=universe_available)
    universe = research_selection_universe(built.universe)
    research_available = sample(clock_ns, floor_ns=research_started)
    universe = replace(universe, envelope=replace(universe.envelope, content_hash="",
        created_at_ns=research_available, available_at_ns=research_available))
    repository.register_artifact(ArtifactIndexEntryV2(
        MULTI_SLEEVE_SELECTION_HASH, "ResearchSelectionPolicyV2", MULTI_SLEEVE_SELECTION_HASH,
        0, 0, MULTI_SLEEVE_SELECTION_BODY,
    ))
    _index_universe(repository, universe)
    record_computation(repository, artifact_ref=universe.content_hash,
        information_cutoff_ns=event.information_cutoff_ns, started_ns=research_started,
        finished_ns=research_available, available_ns=research_available,
        input_refs=(built.universe.content_hash, MULTI_SLEEVE_SELECTION_HASH), deadline_ns=event.deadline_ns)

    feature_refs: set[str] = set()
    generation_missing_reasons: set[str] = set(history_missing_reasons)
    candidates: dict[str, CandidateActionV2] = {}
    trigger_key_json = trigger_product.key.to_canonical_json()
    frames = histories.get(trigger_key_json, {})
    trigger_market = market.get(trigger_key_json)
    if trigger_market is None:
        trigger_health = None
        gate = quote = mark = None
        tick_size = trigger_product.tick_size
    else:
        trigger_health, gate, quote, mark, tick_size = trigger_market
    if trigger_health is not None:
        feature_trigger = trigger_bar
        if native_m1:
            feature_trigger = next((bar for bar in reversed(frames.get(BarIntervalV2.M15, ()))
                                    if bar.close_at_ns <= event.information_cutoff_ns
                                    and bar.raw.available_at_ns <= event.information_cutoff_ns), None)
        join = (
            asof_join(store, trigger_product.key, cutoff_ns=event.information_cutoff_ns,
                      trigger_ref=feature_trigger.content_hash, source_health=trigger_health)
            if feature_trigger is not None else None
        )
        if join is not None and join.status == "AVAILABLE":
            # Structure features repeatedly inspect prior confirmed swings.
            # The strategy sleeves retain the full as-of history in ``join``;
            # the shared causal snapshot receives the bounded context window
            # declared by this runtime composition.
            feature_join = replace(
                join,
                m15=join.m15[-_FEATURE_CONTEXT_BARS_V1["M15"]:],
                h1=join.h1[-_FEATURE_CONTEXT_BARS_V1["H1"]:],
                h4=join.h4[-_FEATURE_CONTEXT_BARS_V1["H4"]:],
            )
            # Freeze a deterministic causal location configuration from the
            # exact as-of product and latest confirmed M15 swing. A swing's
            # pivot event is anchored only once its confirmation bar is known.
            trade_location_config = _trade_location_config_v1(
                trigger_product, feature_join.m15, cutoff_ns=event.information_cutoff_ns,
            )
            trade_location_inputs = load_trade_location_inputs(
                repository, Path(repository.path).parent / "ops-observations",
                key=trigger_product.key, cutoff_ns=event.information_cutoff_ns,
                configuration=trade_location_config,
            )
            feature_started = sample(clock_ns, floor_ns=research_available)
            trade_location_receipt = persist_trade_location_input_receipt(
                repository, trade_location_inputs,
                created_at_ns=feature_started, available_at_ns=feature_started,
            )
            feature = feature_snapshot(feature_join, clock_ns=clock_ns,
                exact_histories={frame: page.state for frame,page in history_pages[trigger_key_json].items()
                                 if page.ready and page.state is not None},
                trades=trade_location_inputs.estimator_trades,
                trade_location=trade_location_inputs.configuration,
                trade_location_input_refs=(trade_location_receipt.content_hash,))
            source_refs.add(trade_location_receipt.content_hash)
            repository.register_artifact(ArtifactIndexEntryV2(
                feature.content_hash, "FeatureArtifactV2", feature.content_hash,
                feature.envelope.created_at_ns, feature.envelope.available_at_ns,
                {"feature": feature.to_dict()},
            ))
            record_computation(repository, artifact_ref=feature.content_hash,
                information_cutoff_ns=event.information_cutoff_ns, started_ns=feature_started,
                finished_ns=feature.envelope.created_at_ns, available_ns=feature.envelope.available_at_ns,
                input_refs=feature.envelope.input_refs, deadline_ns=event.deadline_ns)
            feature_refs.add(feature.content_hash)
            source_refs.update(feature.envelope.input_refs)
            if gate is not None:
                source_refs.add(gate.evidence_ref)
            waiting = [watch for watch in _bounded_active_watches(repository)
                       if watch.key == trigger_product.key and watch.state.value == "WAITING_FOR_EVENT"]
            if not native_m1:
                s1_waiting = [watch for watch in waiting if watch.policy_hash == S1_POLICY.policy_hash]
                s1 = S1ShadowCoordinator(repository, clock_ns=clock_ns)
                trigger_pages = history_pages[trigger_key_json]
                exact_s1 = {"h1_history": trigger_pages[BarIntervalV2.H1].state,
                            "h4_history": trigger_pages[BarIntervalV2.H4].state}
                if s1_waiting:
                    for watch in s1_waiting:
                        decision = s1.on_bar(watch.watch_id, join, feature, event_gate=gate,
                                             bbo=quote, mark_index=mark, **exact_s1)
                        if decision.status == "NOT_ESTIMABLE":
                            generation_missing_reasons.add("S1:" + decision.reason)
                        if decision.candidate is not None:
                            candidates[decision.candidate.candidate_id] = decision.candidate
                else:
                    decision = s1.create_watch(join, feature, event_gate=gate, universe=universe, **exact_s1)
                    if decision.status == "NOT_ESTIMABLE":
                        generation_missing_reasons.add("S1:" + decision.reason)
                    if decision.candidate is not None:
                        candidates[decision.candidate.candidate_id] = decision.candidate
                s2 = S2ShadowCoordinator(repository).on_trigger_close(
                    join, feature, universe=universe, bbo=quote, clock_ns=clock_ns,
                    m15_history=trigger_pages[BarIntervalV2.M15].state,
                )
                if s2.status == "NOT_ESTIMABLE":
                    generation_missing_reasons.add("S2:" + s2.reason)
                if s2.candidate is not None:
                    candidates[s2.candidate.candidate_id] = s2.candidate

                # The S6 rank/research coordinator runs in the bounded sidecar.
                # At the next M15 event, materialize only a hypothesis whose
                # single subsequent bar is this exact trigger and whose watch
                # is still live. The action policy is a separate shadow-only
                # version; legacy S6 rank/trigger evidence remains untouched.
                s6_waiting = [watch for watch in waiting if watch.policy_hash == S6_POLICY.policy_hash]
                if s6_waiting:
                    m15_bars = tuple(frames.get(BarIntervalV2.M15, ()))
                    prior_15m = next((bar for bar in reversed(m15_bars)
                        if bar.close_at_ns == trigger_bar.close_at_ns - BarIntervalV2.M15.duration_ns
                        and bar.raw.available_at_ns <= event.information_cutoff_ns), None)
                    s6 = S6ShadowCoordinator(repository)
                    for watch in s6_waiting:
                        hypothesis = _s6_hypothesis_for_watch(
                            repository, watch, cutoff_ns=event.information_cutoff_ns,
                        )
                        if hypothesis is None:
                            generation_missing_reasons.add("S6:HYPOTHESIS_MISSING_MISBOUND_OR_FUTURE")
                            continue
                        if prior_15m is None:
                            generation_missing_reasons.add("S6:IMMEDIATELY_PRIOR_M15_EVIDENCE_UNAVAILABLE")
                            continue
                        s6.confirm_trigger(
                            hypothesis_id=hypothesis.hypothesis_id,
                            previous_15m=prior_15m,
                            trigger_15m=trigger_bar,
                            cutoff_ns=event.information_cutoff_ns,
                        )
                        transitioned = repository.get_watch(watch.watch_id)
                        if transitioned is None or transitioned.state.value != "CONFIRMED":
                            continue
                        if quote is None or mark is None or gate is None:
                            generation_missing_reasons.add("S6:ACTION_BBO_MARK_OR_EVENT_GATE_UNAVAILABLE")
                            continue
                        completed_h1 = tuple(bar for bar in frames.get(BarIntervalV2.H1, ())
                            if bar.close_at_ns <= hypothesis.cutoff_ns - hypothesis.cutoff_ns % BarIntervalV2.H1.duration_ns)
                        stop_window_h1 = completed_h1[-3:]
                        cost_body = {
                            "version": "S6_SHADOW_COST_NOT_ESTIMABLE_V1",
                            "policy_hash": S6_ACTION_POLICY.policy_hash,
                            "status": "NOT_ESTIMABLE",
                            "reason": "NO_S6_SPECIFIC_QUALIFIED_ACTION_COST_CONTRACT",
                            "economic_authority": "ZERO", "capital_authority": "ZERO",
                        }
                        cost_ref = sha256_json(cost_body)
                        repository.register_artifact(ArtifactIndexEntryV2(
                            cost_ref, "S6ShadowCostNotEstimableV1", cost_ref, 0, 0,
                            {"cost": cost_body},
                        ))
                        action_now = sample(clock_ns, floor_ns=max(
                            event.information_cutoff_ns, feature.envelope.available_at_ns,
                        ))
                        s6_action_result = build_s6_candidate_action(
                            repository,
                            hypothesis=hypothesis,
                            hypothesis_ref=hypothesis.hypothesis_id,
                            decision_cutoff_ns=event.information_cutoff_ns,
                            stop_window_h1=stop_window_h1,
                            previous_15m=prior_15m,
                            trigger_15m=trigger_bar,
                            feature=feature,
                            quote=quote,
                            mark_index=mark,
                            event_gate=gate,
                            cost_model_ref=cost_ref,
                            now_ns=action_now,
                        )
                        if s6_action_result.candidate is not None:
                            # S6 actions may be computed after the sealed
                            # market cutoff as source receipts arrive. Bind the
                            # trigger eligibility and candidate to that exact
                            # cutoff before selector admission; the selector
                            # can then distinguish late derived publication
                            # from information first observed after cutoff.
                            trigger_ref = s6_action_result.trigger_ref
                            if trigger_ref is None:
                                raise ValueError("S6 candidate is missing its exact trigger eligibility artifact")
                            trigger_entry = repository.get_artifact(trigger_ref)
                            if (trigger_entry is None or trigger_entry.artifact_type != "S6ActionTriggerEligibilityV1"
                                    or trigger_entry.available_at_ns > s6_action_result.candidate.deadline_ns):
                                raise ValueError("S6 trigger eligibility was not durably published by its deadline")
                            trigger_metadata = trigger_entry.metadata
                            trigger_inputs = trigger_metadata.get("input_refs")
                            if not isinstance(trigger_inputs, (tuple, list)):
                                raise ValueError("S6 trigger eligibility is missing its sealed input references")
                            trigger_published_at = trigger_entry.available_at_ns
                            record_computation(repository, artifact_ref=trigger_ref,
                                information_cutoff_ns=event.information_cutoff_ns,
                                started_ns=trigger_published_at, finished_ns=trigger_published_at,
                                available_ns=trigger_published_at, input_refs=tuple(trigger_inputs),
                                deadline_ns=s6_action_result.candidate.deadline_ns)
                            candidate_entry = repository.get_artifact(s6_action_result.candidate.content_hash)
                            if (candidate_entry is None or candidate_entry.artifact_type != "CandidateActionV2"
                                    or candidate_entry.available_at_ns > s6_action_result.candidate.deadline_ns):
                                raise ValueError("S6 candidate action was not durably published by its deadline")
                            candidate_published_at = candidate_entry.available_at_ns
                            record_computation(repository, artifact_ref=s6_action_result.candidate.content_hash,
                                information_cutoff_ns=event.information_cutoff_ns,
                                started_ns=candidate_published_at, finished_ns=candidate_published_at,
                                available_ns=candidate_published_at,
                                input_refs=s6_action_result.candidate.envelope.input_refs,
                                deadline_ns=s6_action_result.candidate.deadline_ns)
                            candidates[s6_action_result.candidate.candidate_id] = s6_action_result.candidate
                            source_refs.update((trigger_ref, s6_action_result.candidate.content_hash))
                        elif s6_action_result.status == "NOT_ESTIMABLE":
                            generation_missing_reasons.add("S6:" + s6_action_result.reason)

            if native_m1:
                trade_evidence, stream_trade_health = _indexed_s3_forward_trade_evidence(
                    repository, archive_root, trigger_product, cutoff_ns=event.information_cutoff_ns,
                )
                trade_evidence_body = trade_evidence.to_dict()
                trade_evidence_ref = sha256_json({
                    "artifact_type": "S3ForwardTradeEvidenceV1", "evidence": trade_evidence_body,
                })
                repository.register_artifact(ArtifactIndexEntryV2(
                    trade_evidence_ref, "S3ForwardTradeEvidenceV1", trade_evidence_ref,
                    event.information_cutoff_ns, event.information_cutoff_ns,
                    {"evidence": trade_evidence_body},
                ))
                s3_quote, s3_quote_refs, quote_recovery_epoch, _quote_age = _indexed_s3_stream_quote(
                    repository, trigger_product, cutoff_ns=event.information_cutoff_ns, rest_quote=quote,
                )
                source_refs.update((*trade_evidence.trade_refs, trade_evidence_ref, *s3_quote_refs))
                if trade_evidence.continuity_report_ref is not None:
                    source_refs.add(trade_evidence.continuity_report_ref)
                if trade_evidence.source_health_ref is not None:
                    source_refs.add(trade_evidence.source_health_ref)

                readiness_residuals = _indexed_s3_residuals(
                    repository, trigger_product.key, cutoff_ns=event.information_cutoff_ns,
                )
                readiness_vwaps = _indexed_s3_vwaps(
                    repository, trigger_product.key, cutoff_ns=event.information_cutoff_ns,
                )
                trigger_universe_entries = [
                    item for item in universe.entries if item.key == trigger_product.key
                ]
                trigger_s3_eligibility = (
                    trigger_universe_entries[0].strategy_eligibility.get(S3_POLICY.policy_id)
                    if len(trigger_universe_entries) == 1 else None
                )
                readiness = evaluate_s3_warmup_readiness(
                    key=trigger_product.key, cutoff_ns=event.information_cutoff_ns,
                    bars=frames.get(BarIntervalV2.M1, ()), residuals=readiness_residuals,
                    trade_vwaps=readiness_vwaps, trades=trade_evidence.trades,
                    bar_source_health=trigger_health, trade_source_health=stream_trade_health,
                    quote=s3_quote, event_gate=gate,
                    point_in_time_universe_eligible=bool(
                        len(trigger_universe_entries) == 1
                        and trigger_universe_entries[0].data_eligible
                        and not trigger_universe_entries[0].capital_eligible
                        and trigger_s3_eligibility is not None
                        and trigger_s3_eligibility.status.value == "ELIGIBLE"
                    ),
                    recovery_epoch=(trade_evidence.recovery_epoch
                                    if trade_evidence.recovery_epoch is not None else quote_recovery_epoch),
                    trade_recovery_epoch=trade_evidence.recovery_epoch,
                    book_recovery_epoch=quote_recovery_epoch,
                    trade_evidence_status=trade_evidence.status,
                    trade_evidence_reason_codes=trade_evidence.reason_codes,
                    additional_evidence_refs=tuple(sorted({
                        trade_evidence_ref, *s3_quote_refs,
                        *([trade_evidence.continuity_report_ref]
                          if trade_evidence.continuity_report_ref is not None else []),
                    })),
                )
                readiness_body = readiness.to_dict()
                readiness_ref = sha256_json({
                    "artifact_type": "S3NativeWarmupReadinessV1", "readiness": readiness_body,
                })
                repository.register_artifact(ArtifactIndexEntryV2(
                    readiness_ref, "S3NativeWarmupReadinessV1", readiness_ref,
                    event.information_cutoff_ns, event.information_cutoff_ns,
                    {"readiness": readiness_body, "decision_event_id": event.event_id,
                     "trigger_bar_ref": trigger_bar.content_hash,
                     "forward_trade_evidence_ref": trade_evidence_ref},
                ))
                source_refs.update((readiness_ref, *readiness.evidence_refs))
                s3 = S3ShadowCoordinator(repository)
                for watch in waiting:
                    if watch.policy_hash != S3_POLICY.policy_hash:
                        continue
                    if trigger_bar.close_at_ns <= watch.created_at_ns:
                        continue
                    setup = repository.get_artifact(watch.thesis_hash)
                    setup_state = setup.metadata.get("state") if setup is not None else None
                    sigma = setup_state.get("residual_sigma") if isinstance(setup_state, Mapping) else None
                    frozen = [item for item in _indexed_s3_vwaps(
                        repository, watch.key, cutoff_ns=event.information_cutoff_ns,
                    ) if item.content_hash in watch.evidence_refs]
                    if len(frozen) != 1 or isinstance(sigma, bool) or not isinstance(sigma, (int, float)):
                        continue
                    result = s3.on_subsequent_bar(
                        watch_id=watch.watch_id, trigger=trigger_bar,
                        cutoff_ns=event.information_cutoff_ns, frozen_vwap=frozen[0],
                        residual_sigma=float(sigma), quote=s3_quote, tick_size=tick_size,
                        feature=feature, universe=universe, event_gate=gate,
                        bar_health=trigger_health,
                        trade_completeness_proven=trade_evidence.trade_completeness_proven,
                    )
                    if result.candidate is not None:
                        candidates[result.candidate.candidate_id] = result.candidate
                # The S32 report explicitly leaves complete Bybit trade coverage
                # unproven. Keep the observations diagnostic and prevent them
                # from producing VWAPs, residuals, WATCHes, or candidates.
                s3.evaluate_setup(
                    key=trigger_product.key, cutoff_ns=event.information_cutoff_ns,
                    residuals=(), current_vwap=None, trades=trade_evidence.trades,
                    completed_1m=frames.get(BarIntervalV2.M1, ()), context=join,
                    feature=feature, quote=s3_quote, tick_size=tick_size, universe=universe,
                    event_gate=gate, bar_health=trigger_health, trade_health=stream_trade_health,
                    trade_completeness_proven=trade_evidence.trade_completeness_proven,
                )

    scanner_refs: dict[str, tuple[str, ...]] = {}
    sorted_universe = sorted(eligible_order, key=lambda row: (-row[0], row[1]))
    rank_by_key = {key: index + 1 for index, (_turnover, key) in enumerate(sorted_universe)}
    for candidate in sorted(candidates.values(), key=lambda item: item.candidate_id):
        rank = rank_by_key.get(candidate.key.to_canonical_json())
        if rank is None:
            continue
        candidate_entry = repository.get_artifact(candidate.content_hash)
        feature_entry = repository.get_artifact(candidate.snapshot_hash)
        if (candidate_entry is None or feature_entry is None or candidate_entry.available_at_ns > event.deadline_ns
                or feature_entry.available_at_ns > event.deadline_ns):
            continue
        scanner_started = sample(clock_ns, floor_ns=max(candidate_entry.available_at_ns, feature_entry.available_at_ns, research_available))
        scanner_available = sample(clock_ns, floor_ns=scanner_started)
        source = ScannerSelectionSourceV1(
            candidate.candidate_id, candidate.key, rank, "PUBLIC_UNIVERSE_TURNOVER_V1", "1.0.0",
            universe.content_hash, event.event_id, scanner_available,
            (universe_observation_refs[candidate.key.to_canonical_json()],),
        )
        source_ref = register_scanner_source(repository, source)
        record_computation(repository, artifact_ref=source_ref,
            information_cutoff_ns=event.information_cutoff_ns, started_ns=scanner_started,
            finished_ns=scanner_available, available_ns=scanner_available,
            input_refs=source.input_refs, deadline_ns=event.deadline_ns)
        rank_started = sample(clock_ns, floor_ns=scanner_available)
        rank_available = sample(clock_ns, floor_ns=rank_started)
        rank_evidence = ScannerRankEvidenceV1(
            candidate.candidate_id, rank, source.scanner_policy_id, source.scanner_policy_version,
            universe.content_hash, event.event_id, rank_available, source_ref,
        )
        scanner_refs[candidate.candidate_id] = (register_scanner_rank(repository, rank_evidence),)
        record_computation(repository, artifact_ref=rank_evidence.content_hash,
            information_cutoff_ns=event.information_cutoff_ns, started_ns=rank_started,
            finished_ns=rank_available, available_ns=rank_available,
            input_refs=(source_ref,), deadline_ns=event.deadline_ns)
        source_refs.update((source_ref, rank_evidence.content_hash))

    if not feature_refs:
        generation_missing_reasons.add("CAUSAL_FEATURE_EVIDENCE_UNAVAILABLE")
    generation_body = {"version": "OpsCandidateGenerationEvidenceV1", "event_id": event.event_id,
                       "information_cutoff_ns": event.information_cutoff_ns,
                       "missing_reasons": sorted(generation_missing_reasons),
                       "candidate_refs": sorted(item.content_hash for item in candidates.values()),
                       "feature_refs": sorted(feature_refs), "authority": "ZERO"}
    generation_key = sha256_json({"artifact_type": "OpsCandidateGenerationEvidenceV1", "event_id": event.event_id})
    prior_generation = repository.get_artifact(generation_key)
    if prior_generation is not None:
        if canonical_json(prior_generation.metadata.get("generation")) != canonical_json(generation_body):
            raise ValueError("candidate generation cannot revise an already observed opportunity")
    else:
        observed_at_ns = sample(clock_ns, floor_ns=composition_started)
        repository.register_artifact(ArtifactIndexEntryV2(generation_key, "OpsCandidateGenerationEvidenceV1",
            sha256_json(generation_body), observed_at_ns, observed_at_ns, {"generation": generation_body}))
    return ProductionEventInputsV1(
        universe, tuple(sorted(candidates.values(), key=lambda item: item.candidate_id)),
        scanner_refs, {}, {}, tuple(sorted(feature_refs)), tuple(sorted(source_refs)),
        tuple(sorted(generation_missing_reasons)),
    )


def _trade_location_config_v1(
    product: ProductContractV2,
    m15_bars: Sequence[Any],
    *,
    cutoff_ns: int,
) -> TradeLocationConfig:
    """Freeze research location inputs from cutoff-known product and M15 bars."""
    day_start_ns = cutoff_ns // DAY_NS * DAY_NS
    confirmed = tuple(swing for swing in confirmed_swings(tuple(m15_bars))
                      if swing.confirmed_at_ns <= cutoff_ns)
    latest = max(confirmed, key=lambda swing: (swing.confirmed_at_ns, swing.pivot_at_ns,
                                                swing.kind, swing.pivot_ref), default=None)
    anchor = (CausalTradeAnchor(product.key, latest.pivot_at_ns,
                                latest.confirmed_at_ns, latest.confirmation_ref)
              if latest is not None else None)
    return TradeLocationConfig(anchor=anchor, profile_window_start_ns=day_start_ns, product=product)


def _latest_stream_continuity_report_ref(
    repository: OpsRepository,
    product: ProductContractV2,
    *,
    channel: str,
    cutoff_ns: int,
) -> str | None:
    candidates: list[ArtifactIndexEntryV2] = []
    page = repository.latest_artifact_entries("PublicStreamContinuityReportV1", as_of_ns=cutoff_ns,
        metadata_path=("report", "channel"), identity_value=channel, limit=2)
    if page.invalid_entry_count:
        raise ValueError("stream continuity contains invalid indexed evidence")
    for entry in page.entries:
        report = entry.metadata.get("report")
        if (entry.available_at_ns > cutoff_ns or not isinstance(report, Mapping)
                or report.get("source_id") != BYBIT_PUBLIC_WS_SOURCE_ID_V1
                or report.get("channel") != channel
                or report.get("contract_revision") != product.key.contract_revision
                or report.get("metadata_ref") != product.metadata_ref
                or (report.get("as_of_ns") != entry.available_at_ns
                    and entry.metadata.get("storage_version") != "PUBLIC_CONTINUITY_REPORT_INDEX_V2")
                or type(report.get("as_of_ns")) is not int or report["as_of_ns"] > cutoff_ns):
            continue
        try:
            if InstrumentKeyV2.from_dict(report["instrument"]) != product.key:
                continue
            if sha256_json({"artifact_type": "PublicStreamContinuityReportV1", "report": dict(report)}) != entry.artifact_ref:
                continue
        except (KeyError, TypeError, ValueError):
            continue
        candidates.append(entry)
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item.available_at_ns, item.artifact_ref)).artifact_ref


def _indexed_s3_stream_quote(
    repository: OpsRepository,
    product: ProductContractV2,
    *,
    cutoff_ns: int,
    rest_quote: ExecutableQuote | None,
) -> tuple[ExecutableQuote | None, tuple[str, ...], int | None, int | None]:
    """Read a persisted S32 sequence-book view or an already-fresh REST fallback."""
    report_ref = _latest_stream_continuity_report_ref(
        repository, product, channel=f"orderbook.50.{product.key.native_symbol}", cutoff_ns=cutoff_ns,
    )
    refs: set[str] = set()
    book_quote: ExecutableQuote | None = None
    recovery_epoch: int | None = None
    bbo_age: int | None = None
    if report_ref is not None:
        bridge = quote_from_valid_continuity_report(
            repository, product, cutoff_ns=cutoff_ns, continuity_report_ref=report_ref,
        )
        bridge_body = bridge.to_dict()
        repository.register_artifact(ArtifactIndexEntryV2(
            bridge.evidence_ref, "S3SequenceBookQuoteEvidenceV1", bridge.evidence_ref,
            cutoff_ns, cutoff_ns, {"bridge": bridge_body},
        ))
        refs.update((report_ref, bridge.evidence_ref, *bridge.input_refs))
        recovery_epoch = bridge.recovery_epoch
        bbo_age = bridge.bbo_age_ns
        book_quote = bridge.quote
    if book_quote is not None:
        return book_quote, tuple(sorted(refs)), recovery_epoch, bbo_age
    if rest_quote is not None and rest_quote.valid_at(cutoff_ns, 1_000_000_000):
        refs.add(rest_quote.evidence_ref)
        return rest_quote, tuple(sorted(refs)), recovery_epoch, cutoff_ns - rest_quote.observed_at_ns
    return None, tuple(sorted(refs)), recovery_epoch, bbo_age


def _indexed_s3_forward_trade_evidence(
    repository: OpsRepository,
    archive_root: Path,
    product: ProductContractV2,
    *,
    cutoff_ns: int,
) -> tuple[S3ForwardTradeEvidenceV1, PublicSourceHealthV2 | None]:
    report_ref = _latest_stream_continuity_report_ref(
        repository, product, channel=f"publicTrade.{product.key.native_symbol}", cutoff_ns=cutoff_ns,
    )
    if report_ref is None:
        evidence = S3ForwardTradeEvidenceV1(
            product.key, cutoff_ns, (), (), None, None, None, 0, False, "NOT_ESTIMABLE",
            ("S32_CONTINUITY_OR_CURRENT_HEALTH_UNAVAILABLE",),
        )
        return evidence, None
    evidence = reconstruct_s3_stream_trade_evidence(
        repository, archive_root, product=product, cutoff_ns=cutoff_ns,
        continuity_report_ref=report_ref, limit=512,
    )
    health: PublicSourceHealthV2 | None = None
    if evidence.source_health_ref is not None:
        entry = repository.get_artifact(evidence.source_health_ref)
        body = entry.metadata.get("health") if entry is not None else None
        if (entry is not None and entry.artifact_type == "PublicStreamSourceHealthV1"
                and entry.content_hash == evidence.source_health_ref and isinstance(body, Mapping)):
            try:
                parsed = PublicSourceHealthV2.from_dict(body)
            except (KeyError, TypeError, ValueError):
                parsed = None
            if parsed is not None and parsed.content_hash == evidence.source_health_ref:
                health = parsed
    return evidence, health


def _indexed_quote_and_mark(
    repository: OpsRepository,
    archive_root: Path,
    product: ProductContractV2,
    *,
    cutoff_ns: int,
) -> tuple[ExecutableQuote | None, MarkIndexEvidence | None, tuple[str, ...]]:
    if product.key.venue.value == "BYBIT":
        kinds: tuple[str, ...] = ("TICKER_MARK_INDEX_FUNDING_OI",)
    elif product.key.venue.value == "BINANCE":
        kinds = ("BOOK_TICKER", "MARK_INDEX_CURRENT_FUNDING")
    else:
        return None, None, ()
    observations = tuple(item for kind in kinds for item in reconstruct_public_observations_from_archive(
        repository, archive_root, instrument_revision=product.key.contract_revision,
        information_cutoff_ns=cutoff_ns, event_types=(kind,), limit=1, key=product.key,
    ))
    by_kind: dict[str, Any] = {}
    for item in observations:
        observation = item.observation
        current = by_kind.get(observation.event_type)
        if current is None or (observation.available_at_ns, observation.record_id) > (
                current.observation.available_at_ns, current.observation.record_id):
            by_kind[observation.event_type] = item
    bbo_item = by_kind.get("TICKER_MARK_INDEX_FUNDING_OI") or by_kind.get("BOOK_TICKER")
    mark_item = by_kind.get("TICKER_MARK_INDEX_FUNDING_OI") or by_kind.get("MARK_INDEX_CURRENT_FUNDING")
    quote: ExecutableQuote | None = None
    mark: MarkIndexEvidence | None = None
    refs = tuple(sorted({item.observation_index_ref for item in (bbo_item, mark_item) if item is not None}))
    if bbo_item is not None:
        try:
            raw = json.loads(bbo_item.raw_payload_bytes)
            bid = raw.get("bid1Price", raw.get("bidPrice", raw.get("b")))
            ask = raw.get("ask1Price", raw.get("askPrice", raw.get("a")))
            event_at = bbo_item.observation.event_at_ns or bbo_item.observation.received_at_ns
            if bid not in (None, "") and ask not in (None, ""):
                quote = ExecutableQuote(product.key, Decimal(str(bid)), Decimal(str(ask)),
                                        event_at, bbo_item.observation.available_at_ns,
                                        bbo_item.observation_index_ref)
        except (ValueError, TypeError, KeyError, json.JSONDecodeError):
            quote = None
    if mark_item is not None:
        try:
            raw = json.loads(mark_item.raw_payload_bytes)
            mark_price = raw.get("markPrice")
            index_price = raw.get("indexPrice")
            if mark_price not in (None, "") and index_price not in (None, ""):
                mark = MarkIndexEvidence(product.key, Decimal(str(mark_price)), Decimal(str(index_price)),
                                         mark_item.observation.available_at_ns,
                                         mark_item.observation_index_ref)
        except (ValueError, TypeError, KeyError, json.JSONDecodeError):
            mark = None
    return quote, mark, refs


def create_production_port() -> ProductionOpsCyclePortV1:
    """Built-in credential-free adapter used by the normal ``atlas-ops`` CLI."""
    return ProductionOpsCyclePortV1()


def create_bybit_public_port() -> ProductionOpsCyclePortV1:
    """Explicit opt-in public Bybit adapter; the default CLI remains archive-only."""
    from ..data.bybit_source import BybitPublicCycleSourceV1

    return ProductionOpsCyclePortV1(public_source=BybitPublicCycleSourceV1())


def create_bybit_public_ws_port(
    *,
    public_source: Any | None = None,
    public_stream_source: PublicStreamSourceV2 | Any | None = None,
    clock_ns: Callable[[], int] = time.time_ns,
) -> ProductionOpsCyclePortV1:
    """Explicit opt-in Bybit REST plus bounded WebSocket evidence adapter.

    The WebSocket producer starts only when the supervisor calls ``recover``
    on this port. The normal factory and the S31 REST-only factory are unchanged.
    """
    from ..data.bybit_source import BybitPublicCycleSourceV1

    source = public_stream_source
    if source is None:
        from ..data.durable_public_capture import DurablePublicCaptureV1

        source = DurablePublicCaptureV1(PublicStreamSourceV2(
            venue=VenueV2.BYBIT, topics=bybit_btc_eth_linear_topics(),
            source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1), clock_ns=clock_ns)
    port = ProductionOpsCyclePortV1(
        public_source=public_source or BybitPublicCycleSourceV1(),
        public_stream_source=source,
        clock_ns=clock_ns,
    )
    if public_source is None:
        from .serviced_acquisition import ServicedPublicAcquisitionV1

        port._serviced_acquisition = ServicedPublicAcquisitionV1(port.public_source, clock_ns=clock_ns)
    return port


class BroadProductionOpsCyclePortV2(ProductionOpsCyclePortV1):
    """Full metadata intake with finite research work and the S40 capture lane."""

    public_source: Any
    _serviced_acquisition: Any

    def __init__(self, *, public_source: Any, clock_ns: Callable[[], int] = time.time_ns,
                 broad_runtime: Any | None = None, capture_payload_metrics: bool = False) -> None:
        super().__init__(public_source=public_source, clock_ns=clock_ns)
        from ..data.universe import ComputeTierV2
        from .broad_public_runtime import BroadPublicRuntimeV2
        from .broad_serviced_acquisition import BroadServicedPublicAcquisitionV2

        self._broad_tier_1 = ComputeTierV2.TIER_1
        self.last_acquisition_snapshot: Any | None = None
        self._broad_lane = broad_runtime or BroadPublicRuntimeV2(clock_ns=clock_ns,
            snapshot_reader=getattr(public_source, "acquire_depth_snapshot", None),
            capture_payload_metrics=capture_payload_metrics)
        self.public_stream_source = self._broad_lane
        self._serviced_acquisition = BroadServicedPublicAcquisitionV2(public_source, clock_ns=clock_ns)
        from .research_basket_outcomes import S8ResearchBasketOutcomeProducerV1

        self._s8_outcome_producer = S8ResearchBasketOutcomeProducerV1(clock_ns=clock_ns)
        self.inputs_provider = IndexedProductionEventInputsV1(clock_ns=clock_ns,
            stream_service=self.service_public_stream, prepared_observer=self._observe_prepared_research)

    def _observe_prepared_research(self, event: OpsDecisionEventV1, universe: UniverseContractV2,
                                   histories: Mapping[str, Any], repository: OpsRepository) -> None:
        from .broad_research_queue import _defer_if_occupied, enqueue_prepared_history_snapshot
        from .full_strategy_surface import persist_cutoff_book_features

        slot_identity = {"version": "BroadResearchSlotIdentityV1",
            "information_cutoff_ns": event.information_cutoff_ns,
            "public_sources": tuple(sorted(self.public_source.required_source_ids))}
        slot_ref = sha256_json(slot_identity)
        prior = repository.get_artifact(slot_ref)
        observed = sample(self.clock_ns, floor_ns=event.available_at_ns)
        if prior is not None:
            slot = prior.metadata.get("slot")
            if (prior.artifact_type != "BroadResearchSlotIdentityV1" or not isinstance(slot, Mapping)
                    or sha256_json(slot) != prior.content_hash
                    or any(slot.get(key) != value for key, value in slot_identity.items())):
                raise ValueError("BROAD_RESEARCH_SLOT_IDENTITY_CONFLICT")
            if slot.get("event_ref") == event.content_hash:
                return
            body = {"version": "BROAD_RESEARCH_DEFERRED_V1", "event_id": event.event_id,
                "event_ref": event.content_hash, "cutoff_ns": event.information_cutoff_ns,
                "universe_ref": universe.content_hash, "slot_ref": slot_ref,
                "reason": "RESEARCH_SLOT_ALREADY_ACCOUNTED", "authority": "ZERO",
                "capital_enabled": False, "assisted_enabled": False}
            ref = sha256_json(body)
            if repository.get_artifact(ref) is None:
                repository.register_artifact(ArtifactIndexEntryV2(ref, "BroadResearchDeferredV1", ref,
                    observed, observed, {"deferred": body}))
            return
        if _defer_if_occupied(repository, event=event, universe_ref=universe.content_hash,
                observed_at_ns=observed) is not None:
            return
        books = {key: book for key, book in getattr(self._broad_lane, "sequence_books", {}).items()
                 if key.to_canonical_json() in histories}
        features = persist_cutoff_book_features(repository, universe=universe,
            cutoff_ns=event.information_cutoff_ns, sequence_books=books, clock_ns=self.clock_ns)
        observed = sample(self.clock_ns, floor_ns=observed)
        queued = enqueue_prepared_history_snapshot(repository, event=event,
            universe_ref=universe.content_hash, composition_refs=(_public_composition_ref(event),),
            prepared_histories=histories, observed_at_ns=observed,
            s4_feature_refs={key.to_canonical_json(): feature.content_hash for key, feature in features.items()})
        if queued.snapshot_ref is not None:
            slot = {**slot_identity, "event_ref": event.content_hash, "event_id": event.event_id,
                "snapshot_ref": queued.snapshot_ref, "universe_ref": universe.content_hash,
                "authority": "ZERO", "capital_enabled": False, "assisted_enabled": False}
            repository.register_artifact(ArtifactIndexEntryV2(slot_ref, "BroadResearchSlotIdentityV1",
                sha256_json(slot), observed, observed, {"slot": slot}))

    def run_research_maintenance(self, repository: OpsRepository, *, cutoff_ns: int) -> str | None:
        """Run the zero-authority broad research surface after the core cycle.

        One durable prepared M15 slot is consumed once. Its source cutoff and
        exact history/book features survive process loss.
        Due S8 outcomes are drained one bounded item per post-cycle maintenance
        turn, so outcome maturation remains live even when no new surface is
        waiting and cannot monopolize the controller writer.
        """
        self._s8_outcome_producer.run_cycle(repository, evidence_cutoff_ns=cutoff_ns, max_items=1)
        from .broad_research_queue import (
            LANE_BROAD_RESEARCH_V1,
            complete_prepared_history_snapshot,
            load_due_prepared_history_snapshot,
        )
        from .full_strategy_surface import compose_full_strategy_surface, load_full_strategy_inputs

        now = sample(self.clock_ns, floor_ns=cutoff_ns)
        pending = repository.due_work_items(LANE_BROAD_RESEARCH_V1, as_of_ns=now, limit=1)
        if not pending:
            return None
        try:
            prepared, = load_due_prepared_history_snapshot(repository, as_of_ns=now)
            universe_entry = _required_artifact(repository, prepared.universe_ref)
            universe = UniverseContractV2.from_dict(json_value(universe_entry.metadata["universe"]))
            loaded = load_full_strategy_inputs(repository, universe, prepared.information_cutoff_ns,
                prepared.histories, self.clock_ns,
                prepared_s4_features={InstrumentKeyV2.from_dict(json.loads(key)): feature
                    for key, feature in prepared.s4_features.items()},
                service_callback=lambda: self.service_public_stream(repository))
            # Input preparation can yield to capture. Publish role artifacts,
            # watches, forecasts, completion and retirement as one operation;
            # a crash cannot expose a partial research surface.
            with repository.atomic_composition():
                result = compose_full_strategy_surface(repository, universe=universe,
                    cutoff_ns=prepared.information_cutoff_ns, **loaded.compose_kwargs(),
                    clock_ns=self.clock_ns)
                complete_prepared_history_snapshot(repository, snapshot_ref=prepared.snapshot_ref,
                    completed_at_ns=sample(self.clock_ns, floor_ns=result.published_at_ns),
                    result_ref=result.content_hash)
        except (ValueError, TypeError, KeyError, ArithmeticError) as error:
            failed_at = sample(self.clock_ns, floor_ns=now)
            failure = {"version": "BroadResearchFailureV1", "snapshot_ref": pending[0].source_ref,
                "failed_at_ns": failed_at, "failure_type": type(error).__name__,
                "reason": "EXACT_RESEARCH_INPUT_OR_CONTRACT_INVALID", "status": "NOT_ESTIMABLE",
                "authority": "ZERO", "capital_enabled": False, "assisted_enabled": False}
            failure_ref = sha256_json(failure)
            with repository.atomic_composition():
                repository.register_artifact(ArtifactIndexEntryV2(failure_ref, "BroadResearchFailureV1",
                    failure_ref, failed_at, failed_at, {"failure": failure}))
                repository.quarantine_due_work(LANE_BROAD_RESEARCH_V1, pending[0].work_id,
                    reason_code="EXACT_RESEARCH_INPUT_OR_CONTRACT_INVALID")
            return failure_ref
        return result.content_hash

    def event_source_health_state(self, repository: OpsRepository, event: OpsDecisionEventV1,
                                  *, now_ns: int) -> str:
        """Gate the exact event dependencies without erasing overall ill health.

        Each dependency needs healthy evidence at its original cutoff and now.
        Later reconnection cannot fabricate historical availability. This hook
        only exists on the zero-capital broad research port.
        """
        if len(event.causal_input_refs) > 16384:
            raise ValueError("BROAD_EVENT_DEPENDENCY_OVERFLOW")
        sources = {event.source_id}
        entries = repository.get_artifact_metadata_by_refs(event.causal_input_refs)
        for ref in event.causal_input_refs:
            entry = entries.get(ref)
            if entry is None or entry["available_at_ns"] > event.information_cutoff_ns:
                return "INCOMPLETE_SNAPSHOT"
            metadata = entry["metadata"]
            for body in (metadata, metadata.get("raw"), metadata.get("observation")):
                if isinstance(body, Mapping) and isinstance(body.get("source_id"), str):
                    sources.add(body["source_id"])
        health_refs = []
        state = "HEALTHY_CURRENT"
        for source_id in sorted(sources):
            historical = repository.latest_source_health_at(source_id, as_of_ns=event.information_cutoff_ns)
            current = repository.latest_source_health_at(source_id, as_of_ns=now_ns)
            for health in (historical, current):
                if health is None:
                    state = "UNKNOWN"
                else:
                    health_refs.append(health.details_ref or sha256_json(health.to_dict()))
                    if health.status != "HEALTHY_CURRENT" and state == "HEALTHY_CURRENT":
                        state = health.status
        body = {"version": "OPS_DECISION_SOURCE_SCOPE_V2", "event_id": event.event_id,
                "event_ref": event.content_hash, "information_cutoff_ns": event.information_cutoff_ns,
                "observed_at_ns": now_ns, "required_source_ids": sorted(sources),
                "health_refs": sorted(set(health_refs)), "state": state,
                "authority": "ZERO", "capital_enabled": False}
        ref = sha256_json(body)
        repository.register_artifact(ArtifactIndexEntryV2(ref, "OpsDecisionSourceScopeV2", ref,
            now_ns, now_ns, {"source_scope": body}))
        return state

    def recover(self, repository: OpsRepository, *, now_ns: int) -> OpsRecoverySnapshotV1:
        from .broad_universe import latest_workset

        if repository.read_only:
            raise ValueError("broad recovery requires the sole ops writer")
        scheduler = repository.latest_artifact_entries("BroadPublicSchedulerStateV2", as_of_ns=now_ns, limit=1)
        if scheduler.invalid_entry_count:
            raise ValueError("BROAD_SCHEDULER_INVALID")
        if scheduler.entries:
            entry = scheduler.entries[0]
            state = entry.metadata.get("scheduler")
            if not isinstance(state, Mapping) or sha256_json(state) != entry.content_hash:
                raise ValueError("BROAD_SCHEDULER_IDENTITY_FAILED")
            self.public_source.restore_state(json_value(state))
        try:
            products = self.public_source.bootstrap_products(now_ns=now_ns)
        except (PublicDataError, KeyError, TypeError, ValueError, ArithmeticError, TimeoutError):
            products = ()
        stream_service = partial(self.service_public_stream, repository)
        if not products:
            previous = latest_workset(repository, cutoff_ns=now_ns, service=stream_service)
            if previous:
                restored_products = []
                for index, ref in enumerate(previous["product_refs"]):
                    if index % 8 == 0:
                        self.service_public_stream(repository)
                    restored_products.append(ProductContractV2.from_dict(json_value(
                        _required_artifact(repository, ref).metadata["product"])))
                products = tuple(restored_products)
        now_ns = sample(self.clock_ns, floor_ns=now_ns)
        registry = InstrumentRegistryV2()
        entries = []
        for product in products:
            if product.key.venue not in self.public_source.enabled_venues or product.key.environment.value != "MAINNET":
                raise ValueError("BROAD_PRODUCT_SCOPE_FAILED")
            if max(product.observed_at_ns, product.available_at_ns) > now_ns:
                raise ValueError("BROAD_PRODUCT_FUTURE_RECEIPT")
            registry.register(product)
            entries.append(ArtifactIndexEntryV2(product.content_hash, "ProductContractV2",
                product.content_hash, product.observed_at_ns, product.available_at_ns, {"product": product.to_dict()}))
        repository.register_artifacts(tuple(entries))
        previous_workset = latest_workset(repository, cutoff_ns=now_ns, service=stream_service)
        if previous_workset is not None:
            from .broad_universe import full_universe

            # Hydrate the immutable broad universe before starting/restarting
            # stream capture. Subsequent decision-time reads can use the
            # repository's one-generation typed cache without delaying queue
            # service under live stream pressure.
            full_universe(repository, cutoff_ns=now_ns, service=stream_service)
            setter = getattr(self.public_source, "set_enrichment_keys", None)
            if callable(setter):
                setter(tuple(ProductContractV2.from_dict(json_value(
                    _required_artifact(repository, ref).metadata["product"])).key
                    for ref in previous_workset["active_product_refs"]))
        epoch = _new_recovery_epoch(repository, started_at_ns=now_ns)
        collector = PublicCollectorV2(repository=repository, registry=registry, clock_ns=self.clock_ns,
            archive=ParquetObservationArchiveV2(Path(repository.path).parent / "ops-observations",
                compact_stream_repository=repository, clock_ns=self.clock_ns), required_recovery_epoch_ref=epoch)
        collector.publish_indexes_after_archive = True
        tiers = {product.key: self._broad_tier_1 for product in products}
        watches = _bounded_active_watches(repository)
        expiry_ids = {watch.watch_id: (
            sha256_json({"version": "BROAD_RESTART_WATCH_EXPIRY_V2", "id": watch.watch_id, "state": watch.state_version}),
            sha256_json({"version": "BROAD_RESTART_WATCH_EXPIRY_OUTBOX_V2", "id": watch.watch_id, "state": watch.state_version}))
            for watch in watches if watch.expires_at_ns <= now_ns}
        restart = collector.restore_subscriptions(tiers, now_ns=now_ns, expiry_ids=expiry_ids)
        source_ids = self.public_source.required_source_ids
        states = tuple(OpsSourceStateV1(source, "UNKNOWN", now_ns, now_ns) for source in source_ids)
        self._collector_recovery = ProductionCollectorRecoveryV1(collector, restart.subscriptions, source_ids,
            bool(scheduler.entries), epoch)
        self._stream_run_epoch = epoch
        if products:
            self._start_broad_lane(repository, products, now_ns=max(now_ns, self.clock_ns()))
        self._recovery_calls += 1
        return OpsRecoverySnapshotV1(source_ids, states,
            tuple(w.watch_id for w in restart.watches.active_watches), restart.subscriptions.plan_id,
            not scheduler.entries, now_ns)

    @staticmethod
    def _broad_stream_seed_keys(
        products: tuple[ProductContractV2, ...], *, now_ns: int,
    ) -> tuple[InstrumentKeyV2, ...]:
        active = tuple(sorted((product for product in products
            if product.trading_status.value == "TRADING"
            and product.available_at_ns <= now_ns and product.effective_at_ns <= now_ns),
            key=lambda product: product.key.to_canonical_json()))
        benchmarks = tuple(product.key for product in active
            if product.key.native_symbol in {"BTCUSDT", "ETHUSDT"})
        if benchmarks:
            return benchmarks
        # A venue may omit benchmark instruments. Keep a deterministic finite
        # public stream seed so alt-only inventories remain observable while
        # they accumulate the history needed for scanner tiers.
        return tuple(product.key for product in active[:2])

    def _start_broad_lane(self, repository: OpsRepository, products: tuple[ProductContractV2, ...], *, now_ns: int) -> None:
        if getattr(self._broad_lane, "capture", None) is not None:
            return
        benchmarks = self._broad_stream_seed_keys(products, now_ns=now_ns)
        if benchmarks:
            self._broad_lane.recover(repository, run_root=Path(repository.path).parent, products=products,
                tiers={}, now_ns=now_ns, benchmark_keys=benchmarks)

    def _register_refreshed_stream_products(self, repository: OpsRepository, snapshot: Any) -> None:
        # Replace bounded active metadata; all old revisions remain immutable in
        # SQLite. Runtime history must not grow with hourly metadata receipts.
        recovery = cast(ProductionCollectorRecoveryV1, self._collector_recovery)
        registry = InstrumentRegistryV2()
        entries: list[ArtifactIndexEntryV2] = []
        for index, product in enumerate(self.public_source.current_products):
            registry.register(product)
            entries.append(ArtifactIndexEntryV2(product.content_hash, "ProductContractV2",
                product.content_hash, product.observed_at_ns, product.available_at_ns, {"product": product.to_dict()}))
            if (index + 1) % BROAD_PUBLIC_PRODUCT_REGISTRATION_PAGE_ROWS_V1 == 0:
                repository.register_artifacts(tuple(entries))
                entries.clear()
                self.service_public_stream(repository)
        if entries:
            repository.register_artifacts(tuple(entries))
            self.service_public_stream(repository)
        recovery.collector.registry = registry
        self._start_broad_lane(repository, tuple(registry.contracts()), now_ns=max(snapshot.observed_at_ns, self.clock_ns()))

    def _persist_broad_public_snapshot(self, repository: OpsRepository, snapshot: Any, *, now_ns: int) -> bool:
        from .broad_universe import publish_broad_workset

        ready = super()._persist_broad_public_snapshot(repository, snapshot, now_ns=now_ns)
        entry = repository.latest_artifact_entries("BroadPublicAcquisitionReceiptV2",
            as_of_ns=max(snapshot.observed_at_ns, self.clock_ns()), limit=1).entries[0]
        at_ns = entry.available_at_ns
        source_states: dict[str, str] = {}
        for source in self.public_source.required_source_ids:
            health = repository.latest_source_health_at(source, as_of_ns=at_ns)
            source_states[source] = health.status if health is not None else "UNKNOWN"
        rejected = frozenset(entry.metadata["receipt"].get("rejected_record_indexes", ()))
        eligible_snapshot = replace(snapshot, records=tuple(item for index, item in enumerate(snapshot.records)
                                                            if index not in rejected))
        workset = publish_broad_workset(repository, products=self.public_source.current_products, snapshot=eligible_snapshot,
            available_at_ns=at_ns, acquisition_ref=entry.artifact_ref, source_state=source_states,
            clock_ns=self.clock_ns,
            service=lambda: self.service_public_stream(repository),
            active_watch_keys=tuple(w.key for w in _bounded_active_watches(repository)))
        at_ns = workset["available_at_ns"]
        setter = getattr(self.public_source, "set_enrichment_keys", None)
        if callable(setter):
            selected_refs = set(workset["active_product_refs"])
            setter(tuple(product.key for product in self.public_source.current_products
                         if product.content_hash in selected_refs))
        from ..data.universe import ComputeTierV2

        if getattr(self._broad_lane, "capture", None) is not None:
            current_stream_products = {product.key for product in self.public_source.current_products
                if product.trading_status.value == "TRADING" and product.available_at_ns <= at_ns
                and product.effective_at_ns <= at_ns}
            stream_tiers = {InstrumentKeyV2.from_dict(json.loads(key)): ComputeTierV2(tier)
                for key, tier in workset["tiers"].items()
                if InstrumentKeyV2.from_dict(json.loads(key)) in current_stream_products}
            stream_watches = tuple(w.key for w in _bounded_active_watches(repository)
                if w.key in current_stream_products)
            self._broad_lane.reconfigure(repository, products=self.public_source.current_products,
                tiers=stream_tiers, now_ns=at_ns,
                benchmark_keys=self._broad_stream_seed_keys(
                    self.public_source.current_products, now_ns=at_ns),
                active_watch_keys=stream_watches)
        state = self.public_source.export_state()
        ref = sha256_json(state)
        if repository.get_artifact(ref) is None:
            repository.register_artifact(ArtifactIndexEntryV2(ref, "BroadPublicSchedulerStateV2", ref,
                at_ns, at_ns, {"scheduler": state}))
        return ready

    def service_public_stream(self, repository: OpsRepository) -> None:
        started = time.monotonic_ns()
        try:
            self._broad_lane.service(repository, now_ns=sample(self.clock_ns, floor_ns=0))
            self._stream_last_service_at_ns = sample(self.clock_ns, floor_ns=0)
        except Exception:
            self._stream_ingestion_failed = True
            raise
        finally:
            self._stream_last_service_duration_ns = time.monotonic_ns() - started

    def _collect_public_stream_evidence(self, repository: OpsRepository, *, now_ns: int, **kwargs: Any) -> None:
        self.service_public_stream(repository)

    def finish_public_capture(self, repository: OpsRepository) -> None:
        self._broad_lane.finish(repository)

    def collect(self, repository: OpsRepository, *, now_ns: int, recovery: OpsRecoverySnapshotV1) -> OpsCycleBatchV1:
        self.service_public_stream(repository)
        snapshot = self._serviced_acquisition.acquire(now_ns=now_ns,
            service=lambda: self.service_public_stream(repository))
        self.last_acquisition_snapshot = snapshot
        if (not snapshot.source_snapshot.get("pending", False)
                and snapshot.source_snapshot.get("acquisition_due", True)):
            self._register_refreshed_stream_products(repository, snapshot)
            self._persist_broad_public_snapshot(repository, snapshot, now_ns=now_ns)
        elif snapshot.source_snapshot.get("pending", False):
            # The worker still owns acquisition state. Never read its mutable
            # scheduler/products or consume a fabricated empty completion.
            body = {"version": "BROAD_PUBLIC_ACQUISITION_WAIT_V2", "observed_at_ns": self.clock_ns(),
                    "reason": snapshot.failure_reason, "status": self._serviced_acquisition.status(),
                    "authority": "ZERO"}
            ref = sha256_json(body)
            repository.register_artifact(ArtifactIndexEntryV2(ref, "BroadPublicAcquisitionWaitV2", ref,
                body["observed_at_ns"], body["observed_at_ns"], {"wait": body}))
        # Recursive active histories operate only on the declared workset.
        from .broad_universe import active_products

        collector = cast(ProductionCollectorRecoveryV1, self._collector_recovery).collector
        original = collector.registry
        active_registry = InstrumentRegistryV2()
        stream_service = partial(self.service_public_stream, repository)
        for product in active_products(repository, cutoff_ns=now_ns, service=stream_service) or ():
            active_registry.register(product)
        collector.registry = active_registry
        try:
            indexed = IndexedPublicCycleSourceV1(clock_ns=self.clock_ns,
                minimum_m15_origin_close_at_ns=self.minimum_m15_origin_close_at_ns,
                scope_public_sources=True)
            indexed.stream_service = self.service_public_stream
            batch = indexed.collect(repository, collector, now_ns=now_ns, recovery=recovery)
        finally:
            collector.registry = original
        self._collection_calls += 1
        return batch


def create_broad_public_port(*, enabled_venues: tuple[VenueV2, ...], public_source: Any = None,
                             clock_ns: Callable[[], int] = time.time_ns,
                             broad_runtime: Any | None = None,
                             capture_payload_metrics: bool = False) -> BroadProductionOpsCyclePortV2:
    from ..data.broad_public_source import BroadPublicCycleSourceV2

    source = public_source or BroadPublicCycleSourceV2(enabled_venues=enabled_venues, clock_ns=clock_ns)
    if tuple(sorted(source.enabled_venues, key=lambda item: item.value)) != tuple(sorted(enabled_venues, key=lambda item: item.value)):
        raise ValueError("broad source venue configuration mismatch")
    return BroadProductionOpsCyclePortV2(public_source=source, clock_ns=clock_ns, broad_runtime=broad_runtime,
        capture_payload_metrics=capture_payload_metrics)


def _l2_raw_frame_from_archive_row(row: Mapping[str, Any]) -> L2RawFrameV2:
    """Rebuild the exact typed frame needed to quarantine a durable identity conflict."""
    instrument = row.get("instrument")
    raw_payload = row.get("raw_payload_bytes")
    if not isinstance(instrument, Mapping) or not isinstance(raw_payload, (bytes, bytearray, memoryview)):
        raise ValueError("archived raw frame row is missing its typed instrument or exact bytes")
    return L2RawFrameV2(
        instrument=InstrumentKeyV2.from_dict(instrument),
        source_id=str(row["source_id"]),
        channel=str(row["channel"]),
        frame_type=str(row["frame_type"]),
        raw_payload_bytes=bytes(raw_payload),
        raw_payload_hash=str(row["raw_payload_hash"]),
        event_at_ns=int(row["event_at_ns"]) if row.get("event_at_ns") is not None else None,
        received_at_ns=int(row["received_at_ns"]),
        available_at_ns=int(row["available_at_ns"]),
        first_update_id=int(row["first_update_id"]) if row.get("first_update_id") is not None else None,
        last_update_id=int(row["last_update_id"]) if row.get("last_update_id") is not None else None,
        previous_update_id=(int(row["previous_update_id"])
                            if row.get("previous_update_id") is not None else None),
        sequence_semantics=str(row["sequence_semantics"]),
        source_health=str(row["source_health"]),
        availability_class=str(row["availability_class"]),
        source_health_ref=(str(row["source_health_ref"])
                           if row.get("source_health_ref") is not None else None),
    )


def decision_event_from_dict(body: Mapping[str, object]) -> OpsDecisionEventV1:
    wire = cast(Mapping[str, Any], body)
    return OpsDecisionEventV1(
        str(wire["event_id"]), str(wire["event_type"]), str(wire["source_id"]), str(wire["trigger_ref"]),
        int(wire["source_event_at_ns"]),
        int(wire["source_published_at_ns"]) if wire.get("source_published_at_ns") is not None else None,
        int(wire["received_at_ns"]), int(wire["available_at_ns"]), int(wire["information_cutoff_ns"]),
        int(wire["deadline_ns"]), tuple(str(ref) for ref in wire.get("causal_input_refs", ())),
    )


def _empty_event_inputs(repository: OpsRepository, event: OpsDecisionEventV1, *,
                        clock_ns: Callable[[], int] | None = None) -> ProductionEventInputsV1:
    return ProductionEventInputsV1(_empty_universe(repository, event, clock_ns=clock_ns), (), {}, {}, {}, (), (),
                                   ("CAUSAL_EVENT_INPUTS_UNAVAILABLE",))


def _empty_universe(repository: OpsRepository, event: OpsDecisionEventV1, *,
                    clock_ns: Callable[[], int] | None = None) -> UniverseContractV2:
    started = sample(clock_ns, floor_ns=event.information_cutoff_ns) if clock_ns else event.information_cutoff_ns
    built = DynamicUniverseRuntimeV2(min_observed_days=30).build_snapshot(
        (),
        decision_slot_ns=event.deadline_ns,
        information_cutoff_ns=event.information_cutoff_ns,
        created_at_ns=started,
        publication_at_ns=started,
        selection_policy_hash=SELECTION_POLICY_HASH,
    )
    available = sample(clock_ns, floor_ns=started) if clock_ns else started
    universe = replace(built.universe, envelope=replace(built.universe.envelope,
        content_hash="", created_at_ns=available, available_at_ns=available))
    _index_universe(repository, universe)
    if clock_ns:
        record_computation(repository, artifact_ref=universe.content_hash,
            information_cutoff_ns=event.information_cutoff_ns, started_ns=started,
            finished_ns=available, available_ns=available, input_refs=(), deadline_ns=event.deadline_ns)
    research = research_selection_universe(universe)
    research_started = sample(clock_ns, floor_ns=available) if clock_ns else available
    research_available = sample(clock_ns, floor_ns=research_started) if clock_ns else research_started
    research = replace(research, envelope=replace(research.envelope, content_hash="",
        created_at_ns=research_available, available_at_ns=research_available))
    repository.register_artifact(ArtifactIndexEntryV2(
        MULTI_SLEEVE_SELECTION_HASH, "ResearchSelectionPolicyV2", MULTI_SLEEVE_SELECTION_HASH,
        0, 0, MULTI_SLEEVE_SELECTION_BODY))
    _index_universe(repository, research)
    if clock_ns:
        record_computation(repository, artifact_ref=research.content_hash,
            information_cutoff_ns=event.information_cutoff_ns, started_ns=research_started,
            finished_ns=research_available, available_ns=research_available,
            input_refs=(universe.content_hash, MULTI_SLEEVE_SELECTION_HASH), deadline_ns=event.deadline_ns)
    return research


def _index_universe(repository: OpsRepository, universe: UniverseContractV2) -> None:
    existing = repository.get_artifact(universe.content_hash)
    body = {"universe": universe.to_dict()}
    if existing is None:
        repository.register_artifact(ArtifactIndexEntryV2(
            universe.content_hash, "UniverseContractV2", universe.content_hash,
            universe.envelope.created_at_ns, universe.envelope.available_at_ns, body,
        ))
    elif existing.artifact_type != "UniverseContractV2" or canonical_json(existing.metadata) != canonical_json(body):
        raise ValueError("production universe ref already contains conflicting evidence")


def _indexed_by_cutoff(repository: OpsRepository, ref: str, event: OpsDecisionEventV1) -> bool:
    entry = repository.get_artifact(ref)
    return entry is not None and entry.available_at_ns <= event.information_cutoff_ns


def _persist_selection_calendar(
    repository: OpsRepository,
    candidate_set: CandidateSetV2,
    event: OpsDecisionEventV1,
) -> str:
    if candidate_set.selection_status.value == "NO_CANDIDATE":
        row = DecisionCalendarEntryV2(
            candidate_set.content_hash, None,
            "MULTI_SLEEVE_RESEARCH_SELECTION_V1", "1.0.0-research",
            candidate_set.selection_policy_hash, event.information_cutoff_ns,
            SelectionStateV2.NO_CANDIDATE, AdmissionStateV2.NOT_APPLICABLE,
            None, None, DecisionSourceStageV2.CANDIDATE_SET, (), candidate_set.content_hash,
            candidate_set.envelope.available_at_ns, candidate_set.envelope.available_at_ns,
        )
    else:
        candidate_entry = repository.get_artifact(candidate_set.content_hash)
        identity = candidate_entry.metadata.get("identity") if candidate_entry is not None else None
        missing_reasons = tuple(identity.get("generation_missing_reasons", ())) if isinstance(identity, Mapping) else ()
        row = DecisionCalendarEntryV2(
            candidate_set.content_hash, None,
            "MULTI_SLEEVE_RESEARCH_SELECTION_V1", "1.0.0-research",
            candidate_set.selection_policy_hash, event.information_cutoff_ns,
            SelectionStateV2.NOT_ESTIMABLE, AdmissionStateV2.NOT_APPLICABLE,
            None, None, DecisionSourceStageV2.CANDIDATE_SET, missing_reasons or ("CANDIDATE_SELECTION_NOT_ESTIMABLE",),
            candidate_set.content_hash, candidate_set.envelope.available_at_ns, candidate_set.envelope.available_at_ns,
        )
    return index_decision_calendar_entry(repository, row)


def _persist_selected_calendar(
    repository: OpsRepository,
    candidate_set: CandidateSetV2,
    candidate: CandidateActionV2,
) -> str:
    """Preserve accepted selection semantics when runtime hard-risk evidence is unavailable."""
    row = DecisionCalendarEntryV2(
        candidate_set.content_hash, candidate.content_hash,
        _POLICIES[candidate.policy_hash].policy_id, _POLICIES[candidate.policy_hash].version,
        candidate.policy_hash, candidate.decision_at_ns, SelectionStateV2.SELECTED,
        AdmissionStateV2.NOT_EVALUATED, None, None, DecisionSourceStageV2.CANDIDATE_SET,
        (), candidate_set.content_hash, candidate_set.envelope.available_at_ns,
        candidate_set.envelope.available_at_ns,
    )
    return index_decision_calendar_entry(repository, row)


def _index_runtime_decision(
    repository: OpsRepository,
    *,
    candidate_set: CandidateSetV2,
    candidate: CandidateActionV2,
    action: ActionArtifactV2 | None,
    admission: AdmissionStateV2,
    reason: str,
    available_at_ns: int,
) -> str:
    action_hash = action.action.action_hash if action is not None else None
    action_ref = action.content_hash if action is not None else None
    body = {
        "version": "OPS_RUNTIME_DECISION_EVIDENCE_V1",
        "candidate_set_ref": candidate_set.content_hash,
        "candidate_ref": candidate.content_hash,
        "policy_hash": candidate.policy_hash,
        "action_hash": action_hash,
        "action_artifact_ref": action_ref,
        "admission_state": admission.value,
        "reason_codes": [reason],
        "decision_at_ns": candidate.decision_at_ns,
        "available_at_ns": available_at_ns,
        "authority": "ZERO",
    }
    ref = sha256_json(body)
    repository.register_artifact(ArtifactIndexEntryV2(
        ref, OPS_RUNTIME_DECISION_ARTIFACT_TYPE, ref, available_at_ns, available_at_ns,
        {"runtime_decision": body},
    ))
    return ref


def _persist_sizing_calendar(
    repository: OpsRepository,
    candidate_set: CandidateSetV2,
    candidate: CandidateActionV2,
    sizing: SizingDecisionV2,
    admission: AdmissionStateV2,
    *,
    action: ActionArtifactV2 | None = None,
) -> str:
    available_at_ns = max(sizing.available_at_ns,
                          action.available_at_ns if action is not None else 0)
    row = DecisionCalendarEntryV2(
        candidate_set.content_hash, candidate.content_hash,
        _POLICIES[candidate.policy_hash].policy_id, _POLICIES[candidate.policy_hash].version,
        candidate.policy_hash, candidate.decision_at_ns, SelectionStateV2.SELECTED, admission,
        action.action.action_hash if action is not None else None,
        action.content_hash if action is not None else None,
        DecisionSourceStageV2.HARD_RISK, tuple(sorted(set(sizing.reasons)),),
        sizing.content_hash, available_at_ns, available_at_ns,
    )
    return index_decision_calendar_entry(repository, row)


def _evaluation_inputs_reason(
    inputs: ProductionEconomicInputsV1 | None,
    event: OpsDecisionEventV1,
) -> str | None:
    if inputs is None or not inputs.complete:
        return "MANDATORY_ECONOMIC_EVIDENCE_UNAVAILABLE"
    assert inputs.available_at_ns is not None and inputs.capability is not None
    assert inputs.model_input is not None
    assert inputs.calibration_input is not None
    assert inputs.execution_model_input is not None
    if inputs.available_at_ns <= event.information_cutoff_ns:
        return "ECONOMIC_EVALUATION_CANNOT_PRECEDE_ITS_DECISION_CUTOFF"
    if inputs.available_at_ns >= event.deadline_ns:
        return "ECONOMIC_EVIDENCE_UNAVAILABLE_BEFORE_ACTION_DEADLINE"
    if inputs.capability.available_at_ns > event.information_cutoff_ns:
        return "VENUE_CAPABILITY_EVIDENCE_UNAVAILABLE_AT_CUTOFF"
    if any(
        item.available_at_ns > event.information_cutoff_ns
        for item in (inputs.model_input, inputs.calibration_input, inputs.execution_model_input)
    ):
        return "FUTURE_CAUSAL_ECONOMIC_INPUT_UNAVAILABLE_AT_CUTOFF"
    return None


def _risk_input_reason(inputs: ProductionRiskInputsV1 | None, event: OpsDecisionEventV1) -> str | None:
    if inputs is None or not inputs.complete:
        return "MANDATORY_HARD_RISK_EVIDENCE_UNAVAILABLE"
    assert inputs.product is not None
    assert inputs.risk_policy_v1 is not None and inputs.risk_policy_v2 is not None
    assert inputs.account is not None and inputs.venue is not None
    assert inputs.stress is not None and inputs.fee is not None
    required_times = (
        inputs.product.available_at_ns,
        inputs.product.effective_at_ns,
        inputs.risk_policy_v1.policy_effective_at_ns,
        inputs.risk_policy_v2.effective_at_ns,
        inputs.account.available_at_ns,
        inputs.venue.available_at_ns,
        inputs.stress.available_at_ns,
        inputs.fee.available_at_ns,
        *(item.available_at_ns for item in (inputs.exposures or ())),
        *(item.available_at_ns for item in (inputs.outcomes or ())),
    )
    if any(at_ns > event.information_cutoff_ns for at_ns in required_times):
        return "FUTURE_REQUIRED_HARD_RISK_EVIDENCE"
    return None


@dataclass(frozen=True)
class _RecoveredEconomicEvaluationV1:
    evaluation: AmendedEvaluationArtifactV2
    evaluation_ref: str
    calendar_ref: str


def _recover_economic_evaluation(
    repository: OpsRepository, action: ActionArtifactV2, candidate: CandidateActionV2,
    candidate_set: CandidateSetV2, *, now_ns: int,
) -> _RecoveredEconomicEvaluationV1 | None:
    identity_ref = sha256_json({"version": "DECISION_CALENDAR_IDENTITY_V2_V1",
        "candidate_set_ref": candidate_set.content_hash, "candidate_ref": candidate.content_hash,
        "policy_hash": candidate.policy_hash})
    identity = repository.get_artifact(identity_ref)
    if identity is None:
        return None
    calendar_ref = identity.metadata.get("decision_ref")
    calendar_entry = repository.get_artifact(calendar_ref) if isinstance(calendar_ref, str) else None
    if (identity.artifact_type != "DecisionCalendarIdentityV2" or calendar_entry is None
            or calendar_entry.artifact_type != "DecisionCalendarEntryV2"
            or identity.content_hash != calendar_entry.artifact_ref
            or calendar_entry.available_at_ns > now_ns):
        raise ValueError("recovered economic calendar identity is invalid or future")
    calendar = DecisionCalendarEntryV2.from_dict(json_value(calendar_entry.metadata["decision_entry"]))
    if index_decision_calendar_entry(repository, calendar) != calendar_entry.artifact_ref:
        raise ValueError("recovered economic calendar failed its exact persisted graph")
    if calendar.source_stage != DecisionSourceStageV2.ECONOMIC_EVALUATION:
        return None
    entry = repository.get_artifact(calendar.source_artifact_ref)
    if entry is None or entry.artifact_type != "EvaluationArtifactV2":
        raise ValueError("recovered economic evaluation is missing")
    evaluation = AmendedEvaluationArtifactV2.from_dict(json_value(entry.metadata["evaluation"]))
    if (evaluation.action_hash != action.action.action_hash or evaluation.action_artifact_ref != action.content_hash
            or evaluation.candidate_ref != candidate.content_hash
            or evaluation.candidate_set_ref != candidate_set.content_hash
            or evaluation.available_at_ns != entry.available_at_ns or entry.available_at_ns > now_ns
            or index_amended_evaluation(repository, evaluation) != entry.artifact_ref):
        raise ValueError("recovered economic evaluation failed its exact persisted graph")
    return _RecoveredEconomicEvaluationV1(evaluation, entry.artifact_ref, calendar_entry.artifact_ref)


def _not_estimable_analogue(
    repository: OpsRepository,
    action: ActionArtifactV2,
    candidate: CandidateActionV2,
    candidate_set: CandidateSetV2,
    available_at_ns: int,
    reason: str,
) -> str:
    result = not_estimable_analogue(
        action_ref=action.content_hash,
        candidate_ref=candidate.content_hash,
        candidate_set_ref=candidate_set.content_hash,
        action_hash=action.action.action_hash,
        information_cutoff_ns=candidate.decision_at_ns,
        reason=reason,
    )
    return persist_analogue(repository, result, available_at_ns=available_at_ns)


def _run_m1(
    repository: OpsRepository,
    action: ActionArtifactV2,
    candidate: CandidateActionV2,
    candidate_set: CandidateSetV2,
    event: OpsDecisionEventV1,
    available_at_ns: int,
    lock_hash: str | None,
) -> tuple[str, str | None]:
    if lock_hash is None:
        ref, reason = _persist_action_diagnostic_failure(
            repository, action, kind="M1", reason="DEPENDENCY_LOCK_UNAVAILABLE", available_at_ns=available_at_ns
        )
        return ref, reason
    try:
        run: M1RunV2 = fit_m1(
            repository,
            action=action,
            candidate=candidate,
            candidate_set=candidate_set,
            cutoff_ns=event.information_cutoff_ns,
            available_at_ns=available_at_ns,
            dependency_lock_hash=lock_hash,
        )
        return run.prediction.content_hash, (run.prediction.reasons[0] if run.prediction.reasons else None)
    except Exception as error:
        return _persist_action_diagnostic_failure(
            repository, action, kind="M1", reason=f"M1_DIAGNOSTIC_UNAVAILABLE_{type(error).__name__}",
            available_at_ns=available_at_ns,
        )


def _persist_action_diagnostic_failure(
    repository: OpsRepository,
    action: ActionArtifactV2,
    *,
    kind: str,
    reason: str,
    available_at_ns: int,
) -> tuple[str, str]:
    body = {
        "version": "ZERO_AUTHORITY_DIAGNOSTIC_FAILURE_V1",
        "kind": kind,
        "action_hash": action.action.action_hash,
        "action_artifact_ref": action.content_hash,
        "available_at_ns": available_at_ns,
        "status": "NOT_ESTIMABLE",
        "reason": reason,
        "authority": "ZERO",
    }
    ref = sha256_json(body)
    repository.register_artifact(ArtifactIndexEntryV2(
        ref, "OpsZeroAuthorityDiagnosticV1", ref, available_at_ns, available_at_ns,
        {"diagnostic": body},
    ))
    return ref, reason


def _required[T](value: T | None) -> T:
    if value is None:
        raise ValueError("required production evidence disappeared after preflight")
    return value


def _result(
    stages: Mapping[PipelineStageV1, OpsStageResultV1],
    terminal: OpsTerminalStatusV1,
    reason: str | None,
) -> OpsDecisionResultV1:
    if tuple(stages) != PIPELINE_STAGE_ORDER:
        raise ValueError("production composition did not checkpoint the complete ordered pipeline")
    return OpsDecisionResultV1(tuple(stages[stage] for stage in PIPELINE_STAGE_ORDER), terminal, reason)


def dependency_lock_hash() -> str | None:
    from ..resources import resource_file

    path = resource_file("requirements-lock.txt")
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None
