"""ATLAS-owned composition of the existing non-capital V2 research APIs.

This module owns the ordering between source recovery, causal evidence, sleeve
selection, hard-risk sizing, action freezing, economic evaluation and the
decision calendar. It owns no second repository and implements no market,
selector, risk or economics rules of its own.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol, cast

from atlas.domain.risk import RiskPolicy

from .._serialization import canonical_json, json_value, sha256_json, sha256_ref, timestamp
from ..contracts import CandidateActionV2, CandidateSetV2, PolicySpecV2
from ..data.bars import BarIntervalV2, CausalBarStoreV2
from ..data.binance import translate_agg_trades
from ..data.bybit import translate_recent_trades
from ..data.collector import PublicCollectorV2
from ..data.health import PublicSourceHealthV2, PublicSourceStateV2
from ..data.history import (
    ParquetObservationArchiveV2,
    reconstruct_causal_bars_from_archive,
    reconstruct_public_observations_from_archive,
)
from ..data.microstructure import (
    L2DeltaV2,
    L2SequenceFaultV2,
    L2SnapshotV2,
    SequenceValidBookV2,
)
from ..data.microstructure_archive import L2FrameArchiveV2, L2RawFrameV2
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
from ..data.subscriptions import SubscriptionPlanV2
from ..data.universe import ComputeTierV2, DynamicUniverseRuntimeV2, UniverseObservationV2
from ..features.joins import asof_join
from ..features.pipeline import feature_snapshot
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
from ..science.admission import AdmissionPolicyV2, VenueCapabilitySnapshotV2
from ..science.analogue import not_estimable_analogue, persist_analogue
from ..science.evaluation_service import Phase2EvaluationResultV2, run_phase2_economic_evaluation
from ..science.m1 import M1RunV2, fit_m1
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

OPS_PRODUCTION_ADAPTER_ID = "ATLAS_V2_PRODUCTION_OPS_COMPOSITION_V1"
OPS_RUNTIME_DECISION_ARTIFACT_TYPE = "OpsRuntimeDecisionEvidenceV1"
_FEATURE_CONTEXT_BARS_V1 = {"M15": 60, "H1": 100, "H4": 100}
BYBIT_PUBLIC_WS_SOURCE_ID_V1 = "BYBIT_PUBLIC_WS"
PUBLIC_STREAM_STALE_NS_V1 = 30_000_000_000
PUBLIC_STREAM_METADATA_MAX_AGE_NS_V1 = 3_600_000_000_000
PUBLIC_STREAM_MAX_FRAMES_PER_CYCLE_V1 = 32
PUBLIC_STREAM_MAX_TRADES_PER_FRAME_V1 = 256
_POLICIES: Mapping[str, PolicySpecV2] = MappingProxyType(
    {policy.policy_hash: policy for policy in (S1_POLICY, S2_POLICY, S3_POLICY)}
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

    universe: UniverseContractV2
    candidates: tuple[CandidateActionV2, ...]
    scanner_evidence_refs: Mapping[str, tuple[str, ...]]
    risk_inputs: Mapping[str, ProductionRiskInputsV1]
    economic_inputs: Mapping[str, ProductionEconomicInputsV1]
    causal_feature_refs: tuple[str, ...] = ()
    causal_source_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        candidates = tuple(self.candidates)
        if any(not isinstance(item, CandidateActionV2) or item.policy_hash not in _POLICIES for item in candidates):
            raise ValueError("production event inputs may contain only exact S1-S3 CandidateActionV2 outputs")
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


class IndexedPublicCycleSourceV1:
    """Build deterministic decision handoffs from reconciled archived final bars."""

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
    ) -> OpsCycleBatchV1:
        del recovery
        archive_root = Path(repository.path).parent / "ops-observations"
        source_ids = set(self.required_source_ids) | set(repository.source_health_sources())
        source_ids.update(
            str(entry.metadata.get("source_id"))
            for entry in repository.artifact_entries("OpsPublicSourceReconciliationV1")
            if isinstance(entry.metadata.get("source_id"), str)
        )
        source_ids.update(
            str(entry.metadata.get("source_id"))
            for entry in repository.artifact_entries("PublicObservationIndexV2")
            if isinstance(entry.metadata.get("source_id"), str)
        )
        recovery_epoch_ref = _current_recovery_epoch_ref(repository, now_ns=now_ns)
        _reconcile_public_sources(
            repository, collector, tuple(sorted(source_ids)), now_ns=now_ns,
            recovery_epoch_ref=recovery_epoch_ref,
        )

        events: list[OpsDecisionEventV1] = []
        refs: list[str] = []
        for entry in repository.artifact_entries("OpsDecisionEventSourceV1"):
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
        healthy = bool(health_states) and all(
            state is not None and state.data_eligible and state.available_at_ns <= now_ns
            for state in health_states.values()
        )
        if not healthy:
            # Durable events remain queued until this recovery epoch has exact
            # source-health reconciliation evidence for every required source.
            events.clear()
            refs.clear()
            seen_event_ids.clear()
        if healthy:
            for product in collector.registry.contracts():
                bars = reconstruct_causal_bars_from_archive(
                    repository, archive_root, key=product.key, interval=BarIntervalV2.M15,
                    information_cutoff_ns=now_ns, availability_class=AvailabilityClassV2.ACTUAL_SYSTEM,
                    limit=100_000,
                )
                if not bars:
                    continue
                trigger = bars[-1].bar
                if now_ns - trigger.close_at_ns > 5_000_000_000:
                    continue
                existing = next((item for item in repository.artifact_entries("OpsDecisionEventSourceV1")
                                 if item.metadata.get("trigger_record_id") == trigger.raw.record_id), None)
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
                if base is None:
                    continue
                generated = IndexedProductionEventInputsV1().resolve(repository, base)
                if generated is None:
                    continue
                causal_refs = set(base.causal_input_refs)
                causal_refs.add(generated.universe.content_hash)
                causal_refs.update(generated.causal_feature_refs)
                causal_refs.update(generated.causal_source_refs)
                causal_refs.update(item.content_hash for item in generated.candidates)
                causal_refs.update(ref for refs_for_candidate in generated.scanner_evidence_refs.values()
                                   for ref in refs_for_candidate)
                for candidate in generated.candidates:
                    causal_refs.update(candidate.envelope.input_refs)
                event = OpsDecisionEventV1(
                    base.event_id, base.event_type, base.source_id, base.trigger_ref,
                    base.source_event_at_ns, base.source_published_at_ns, base.received_at_ns,
                    base.available_at_ns, base.information_cutoff_ns, base.deadline_ns,
                    tuple(sorted(causal_refs)),
                )
                _persist_public_event(repository, event, trigger.raw.record_id, now_ns)
                event_entry = repository.get_artifact(event.content_hash)
                if (event_entry is not None and event.event_id not in seen_event_ids
                        and repository.get_artifact(_ops_receipt_identity_ref(event.event_id)) is None):
                    events.append(event)
                    refs.append(event_entry.artifact_ref)
                    seen_event_ids.add(event.event_id)

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
        if not reconciled:
            # Keep durable decision handoffs queued while the collector is
            # replaying a reconnect gap. A restart must not turn an unfinished
            # event into a permanent source-health terminal receipt.
            events = []
        return OpsCycleBatchV1(tuple(events), states, source_ids_tuple, tuple(sorted(set(refs))), reconciled, now_ns)


def _ops_receipt_identity_ref(event_id: str) -> str:
    return sha256_json({"artifact_type": "OpsSupervisorReceiptIdentityV1", "event_id": event_id})


def _new_recovery_epoch(repository: OpsRepository, *, started_at_ns: int) -> str:
    """Persist a monotone, content-addressed epoch for this supervisor recovery."""
    prior: list[tuple[int, str]] = []
    epoch_entries = repository.artifact_entries("OpsRecoveryEpochV1")
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
    for entry in repository.artifact_entries("OpsRecoveryEpochV1"):
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
    entries = {entry.artifact_ref: entry for entry in repository.artifact_entries("OpsRecoveryEpochV1")}
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
    by_source: dict[str, list[ArtifactIndexEntryV2]] = {}
    observation_entries = {
        item.artifact_ref: item for item in repository.artifact_entries("PublicObservationIndexV2")
    }
    for entry in repository.artifact_entries("OpsPublicSourceReconciliationV1"):
        body = entry.metadata.get("reconciliation")
        if isinstance(body, Mapping):
            by_source.setdefault(str(body.get("source_id", "")), []).append(entry)
    for source_id in source_ids:
        current = collector.health.latest(source_id)
        if current is not None and current.data_eligible and current.available_at_ns <= now_ns:
            continue
        if current is not None and current.observed_at_ns >= now_ns:
            continue
        for entry in sorted(by_source.get(source_id, ()),
                            key=lambda item: (item.available_at_ns, item.artifact_ref), reverse=True):
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
            for ref in refs:
                item = observation_entries.get(str(ref))
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
    health_entries = [item for item in repository.artifact_entries("PublicSourceHealthV2")
                      if item.available_at_ns <= trigger.raw.available_at_ns
                      and isinstance(item.metadata.get("health"), Mapping)
                      and item.metadata["health"].get("source_id") == trigger.raw.source_id]
    if not health_entries:
        return None
    health_entry = max(health_entries, key=lambda item: (item.available_at_ns, item.artifact_ref))
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


def _persist_public_event(
    repository: OpsRepository,
    event: OpsDecisionEventV1,
    trigger_record_id: str,
    now_ns: int,
) -> None:
    if event.information_cutoff_ns > now_ns:
        raise ValueError("public event handoff cannot be created before its information cutoff")
    repository.register_artifact(ArtifactIndexEntryV2(
        event.content_hash, "OpsDecisionEventSourceV1", event.content_hash,
        event.information_cutoff_ns, event.information_cutoff_ns,
        {"event": event.to_dict(), "trigger_record_id": trigger_record_id,
         "composition_id": OPS_PRODUCTION_ADAPTER_ID},
    ))


def _index_causal_bar(repository: OpsRepository, bar: Any, source_ref: str) -> str:
    repository.register_artifact(_causal_bar_entry(bar, source_ref))
    return bar.content_hash


def _causal_bar_entry(bar: Any, source_ref: str) -> ArtifactIndexEntryV2:
    return ArtifactIndexEntryV2(
        bar.content_hash, "CausalBarV2", bar.content_hash, bar.raw.available_at_ns,
        bar.raw.available_at_ns, {"bar": bar.to_dict(), "source_observation_ref": source_ref},
    )


def _indexed_s3_vwaps(
    repository: OpsRepository, key: InstrumentKeyV2, *, cutoff_ns: int,
) -> tuple[TradeVwapSnapshotV2, ...]:
    snapshots: list[TradeVwapSnapshotV2] = []
    for entry in repository.artifact_entries("S3TradeVwapSnapshotV2"):
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
    for entry in repository.artifact_entries("S3ResidualObservationV2"):
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
        information_cutoff_ns=cutoff_ns, event_types=("TRADE", "AGG_TRADE"), limit=100_000,
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
        self.public_stream_source = public_stream_source
        self.inputs_provider = inputs_provider or IndexedProductionEventInputsV1()
        self.crash_after_checkpoint = crash_after_checkpoint
        self.clock_ns = clock_ns
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
        self._recovery_calls = 0
        self._collection_calls = 0

    def close(self) -> None:
        """Stop the opt-in producer; persistence remains exclusively in collect/recover."""
        if self.public_stream_source is not None:
            close = getattr(self.public_stream_source, "close", None)
            if callable(close):
                close()

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
                if (product.key.venue.value != "BYBIT"
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
        for entry in repository.artifact_entries("ProductContractV2"):
            body = entry.metadata.get("product")
            if isinstance(body, Mapping):
                product = ProductContractV2.from_dict(json_value(body))
                if product.content_hash != entry.artifact_ref:
                    raise ValueError("stored product identity differs from its typed production artifact")
                registry.register(product)
        archive = ParquetObservationArchiveV2(Path(repository.path).parent / "ops-observations")
        collector = PublicCollectorV2(
            repository=repository,
            registry=registry,
            clock_ns=lambda: now_ns,
            archive=archive,
            required_recovery_epoch_ref=recovery_epoch_ref,
        )
        tiers = {product.key: ComputeTierV2.TIER_1 for product in registry.contracts()}
        restart = collector.restore_subscriptions(tiers, now_ns=now_ns)
        source_ids = tuple(sorted(set(self.public_source.required_source_ids) | set(repository.source_health_sources())))
        states: list[OpsSourceStateV1] = []
        had_prior = False
        for source_id in source_ids:
            history = repository.source_health_history(source_id)
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
            if callable(begin_collection_cycle):
                begin_collection_cycle(now_ns=now_ns)
            snapshot = acquire_snapshot(now_ns=now_ns)
            self._register_refreshed_stream_products(repository, snapshot)
            snapshot_eligible = self._persist_public_snapshot(repository, snapshot, now_ns=now_ns)
            if not snapshot_eligible:
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
                batch = IndexedPublicCycleSourceV1().collect(
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
        entries = repository.artifact_entries_by_types(
            ("PublicStreamContinuityStateV1",), limit=10_000, available_before_ns=as_of_ns,
        )
        latest: dict[
            tuple[str, str],
            tuple[int, int, int, int, int, int, str, PublicStreamContinuityStateV1],
        ] = {}
        for entry in entries:
            body = entry.metadata.get("state")
            if not isinstance(body, Mapping):
                continue
            try:
                state = PublicStreamContinuityStateV1.from_dict(json_value(body))
            except (ArithmeticError, KeyError, TypeError, ValueError):
                continue
            if entry.artifact_ref != state.content_hash or entry.content_hash != state.content_hash:
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

    def _apply_stream_observation(
        self,
        repository: OpsRepository,
        tracker: PublicStreamContinuityTrackerV1,
        observation: PublicStreamObservationV1,
        *,
        durable_prior_payload_hash: str | None = None,
    ) -> Any:
        decision = tracker.apply(observation, durable_prior_payload_hash=durable_prior_payload_hash)
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
        self._stream_archive = L2FrameArchiveV2(Path(repository.path).parent / "ops-l2-frames", repository)
        prior_states = self._read_latest_stream_states(repository, as_of_ns=now_ns)
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
                    self._stream_books[key] = SequenceValidBookV2(
                        instrument=product.key, source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1,
                        channel=channel, sequence_semantics="BYBIT_U",
                    )
                    self._stream_first_connection_allowed.add(key)
                self._stream_trackers[key] = tracker
                self._stream_connection_epochs[key] = None
                self._persist_stream_state(repository, tracker.to_state(), available_at_ns=now_ns)

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
            if (product.key.venue.value != "BYBIT" or product.key.environment.value != "MAINNET"
                    or product.key.product.value != "LINEAR_PERPETUAL"
                    or product.key.native_symbol not in {"BTCUSDT", "ETHUSDT"}):
                self._stream_metadata_errors.add("OUT_OF_SCOPE_POINT_IN_TIME_METADATA")
                continue
            if product.available_at_ns > max(self.clock_ns(), recovery.collector.clock_ns()):
                # The future contract remains unavailable to this controller cutoff.
                continue
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
                book = SequenceValidBookV2(
                    instrument=product.key, source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1,
                    channel=channel, sequence_semantics="BYBIT_U",
                )
                book.reconnect(available_at_ns)
                self._stream_books[key] = book
                self._stream_trackers.pop(previous_key, None)
                self._stream_books.pop(previous_key, None)
                self._stream_connection_epochs.pop(previous_key, None)
                self._stream_disconnect_seen.discard(previous_key)
                self._stream_first_connection_allowed.discard(previous_key)
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

    def _collect_public_stream_evidence(self, repository: OpsRepository, *, now_ns: int) -> None:
        if self.public_stream_source is None or self._stream_archive is None:
            return
        collector = cast(ProductionCollectorRecoveryV1, self._collector_recovery).collector
        drain = getattr(self.public_stream_source, "drain", None)
        get_status = getattr(self.public_stream_source, "status", None)
        if not callable(drain) or not callable(get_status):
            raise ValueError("opt-in public stream source must expose bounded drain and status")
        frames = tuple(drain(max_items=PUBLIC_STREAM_MAX_FRAMES_PER_CYCLE_V1))
        if len(frames) > PUBLIC_STREAM_MAX_FRAMES_PER_CYCLE_V1 or any(
            not isinstance(frame, CapturedPublicFrameV2) for frame in frames
        ):
            raise ValueError("public stream source returned a frame batch outside the controller bound")
        status = get_status()
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
            health, health_epoch_id = self._stream_health_for_frame(
                repository, tracker, product, frame, status=status, handoff=handoff,
                attempt_count=attempt_count, available_at_ns=ingested_at_ns,
            )
            try:
                frame_observation = PublicStreamObservationV1.from_frame(
                    frame, instrument=product.key, metadata_ref=product.metadata_ref,
                    epoch_id=tracker.state.epoch_id, source_health_ref=health.content_hash,
                    source_health_epoch_id=health_epoch_id, persisted_at_ns=ingested_at_ns,
                )
            except ValueError:
                malformed = PublicStreamObservationV1.transport(
                    instrument=product.key, source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1, channel=channel,
                    metadata_ref=product.metadata_ref, epoch_id=tracker.state.epoch_id,
                    kind=PublicStreamObservationKindV1.MALFORMED_FRAME,
                    observed_at_ns=frame.received_at_ns, available_at_ns=ingested_at_ns,
                    reason_code="FRAME_JSON_OR_TOPIC_INVALID",
                    source_health_ref=health.content_hash,
                    source_health_epoch_id=health_epoch_id,
                )
                self._apply_stream_observation(repository, tracker, malformed)
                raw_frame = raw_archive_record(
                    frame, instrument=product.key,
                    frame_type=f"MALFORMED_FRAME_{frame.raw_payload_hash[:16]}",
                    sequence_semantics=("BYBIT_U" if channel.startswith("orderbook.")
                                        else "BYBIT_TRADE_ID_IS_IDENTITY_NOT_REPLAY_CURSOR"),
                    source_health="INCOMPLETE_SNAPSHOT", source_health_ref=health.content_hash,
                )
                raw_archive_groups.setdefault(feed_key, []).append(raw_frame)
                continue
            self._apply_stream_observation(repository, tracker, frame_observation)

            event: L2SnapshotV2 | L2DeltaV2 | L2SequenceFaultV2 | None = None
            parsed_trades: tuple[Any, ...] = ()
            trade_rows: list[Mapping[str, Any]] = []
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

        self._write_stream_frame_archives(repository, raw_archive_groups)
        collector.flush_archive()
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
            if last_error and last_error != self._stream_last_error_code and not overflowed:
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

        report_as_of = max(ingested_at_ns, now_ns, timestamp(self.clock_ns(), field="stream report clock"))
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
                "backpressure_observed": bool(getattr(handoff, "backpressure", False)),
                "producer_state": str(getattr(status, "state", "UNKNOWN")),
                "last_error_code": last_error,
                "reason_codes": sorted(self._stream_metadata_errors),
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
            report = build_public_stream_continuity_report(
                tracker, as_of_ns=report_as_of, source_health=health,
                source_health_epoch_id=status_epoch if active_epoch_matches else None,
                metadata=product, max_source_health_age_ns=PUBLIC_STREAM_STALE_NS_V1,
                max_metadata_age_ns=PUBLIC_STREAM_METADATA_MAX_AGE_NS_V1,
                book=book if channel.startswith("orderbook.") else None,
                book_metadata_ref=tracker.state.metadata_ref if book is not None else None,
            )
            state_snapshot = tracker.to_state()
            state_ref = state_snapshot.content_hash
            self._persist_stream_state(repository, state_snapshot, available_at_ns=report_as_of)
            repository.register_artifact(ArtifactIndexEntryV2(
                report.content_hash, "PublicStreamContinuityReportV1", report.content_hash,
                report_as_of, report_as_of,
                {"report": report.to_dict(), "state_ref": state_ref,
                 "source_health_ref": health.content_hash,
                 "transport": health_body},
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
    ) -> tuple[PublicSourceHealthV2, str]:
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
            "version": "PUBLIC_STREAM_FRAME_HEALTH_V1", "source_id": BYBIT_PUBLIC_WS_SOURCE_ID_V1,
            "instrument_hash": product.key.content_hash, "channel": frame.channel,
            "metadata_ref": product.metadata_ref, "epoch_id": frame_epoch_id,
            "observed_at_ns": frame.received_at_ns, "available_at_ns": available_at_ns,
            "state": health_state.value, "frame_ref": frame.raw_payload_hash,
            "connection_attempt": frame.connection_epoch,
            "active_attempt": attempt_count, "overflowed": bool(getattr(handoff, "overflowed", False)),
        }
        health = PublicSourceHealthV2(
            BYBIT_PUBLIC_WS_SOURCE_ID_V1, frame.received_at_ns, available_at_ns,
            health_state, sha256_json(body), details,
        )
        repository.register_artifact(ArtifactIndexEntryV2(
            health.content_hash, "PublicStreamSourceHealthV1", health.content_hash,
            available_at_ns, available_at_ns, {"health": health.to_dict(), "transport": body},
        ))
        return health, frame_epoch_id

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
    ) -> None:
        if self._stream_archive is None:
            return
        for _feed_key, incoming_frames in groups.items():
            unique: dict[str, L2RawFrameV2] = {}
            for frame in incoming_frames:
                local_prior = unique.get(frame.record_id)
                index_ref = sha256_json({"artifact_type": "PublicStreamFrameIndexV1",
                                         "record_id": frame.record_id})
                stored = repository.get_artifact(index_ref)
                prior_hash = (str(stored.metadata.get("raw_payload_hash"))
                              if stored is not None else None)
                prior_frame: L2RawFrameV2 | None = local_prior
                if prior_frame is None and stored is not None and prior_hash != frame.raw_payload_hash:
                    chunk_id = stored.metadata.get("archive_chunk_id")
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
        for entry in repository.artifact_entries("OpsDecisionEventSourceV1"):
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
        for entry in repository.artifact_entries("OpsDecisionEventSourceV1"):
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
        healthy_seen = False
        recovery_required = False
        for health_observation in collector.health.history(SOURCE_ID):
            if health_observation.data_eligible:
                healthy_seen = True
            elif healthy_seen:
                # A later healthy state cannot clear a previously observed
                # trade-history gap; the current bounded REST surface has no
                # evidence with which to prove that gap complete.
                recovery_required = True

        prior_clock = collector.clock_ns
        collector.clock_ns = lambda: reconciliation_at_ns
        try:
            if snapshot.failure_kind == "RATE_LIMITED":
                collector.on_rate_limited(SOURCE_ID, at_ns=reconciliation_at_ns)
            elif snapshot.failure_kind == "DISCONNECTED":
                collector.on_disconnect(SOURCE_ID, at_ns=reconciliation_at_ns)

            ingestion_complete = snapshot.complete
            for record in snapshot.records:
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
        for record in snapshot.records:
            index_ref = sha256_json({
                "artifact_type": "PublicObservationIndexV2",
                "record_id": record.observation.record_id,
            })
            entry = repository.get_artifact(index_ref)
            if entry is not None and entry.available_at_ns <= reconciliation_at_ns:
                eligible_refs.add(index_ref)
                by_symbol_events[record.instrument_key.native_symbol].add(record.observation.event_type)

        required_events = {"PRODUCT_METADATA", "TICKER_MARK_INDEX_FUNDING_OI", "TRADE"} | {
            f"BAR_{interval.value}" for interval in CAMPAIGN_INTERVALS
        }
        references_ready = bool(eligible_refs) and all(
            required_events.issubset(events) for events in by_symbol_events.values()
        )
        bar_gaps_repaired = self._public_snapshot_overlap_is_repaired(
            repository, snapshot, at_ns=reconciliation_at_ns, recovery_required=recovery_required,
        )
        # A bounded recent-trades page has no cursor or historical backfill.
        # It can show observed trades but cannot prove that a recovery gap was
        # filled. Recovery after an unhealthy state therefore remains closed.
        trade_continuity_proven = False
        repaired = bar_gaps_repaired and (not recovery_required or trade_continuity_proven)
        source_snapshot_reconciled = ingestion_complete and references_ready and repaired
        if source_snapshot_reconciled:
            collector.reconcile_after_reconnect(
                SOURCE_ID, at_ns=reconciliation_at_ns, complete_snapshot=True, missed_interval_repaired=True,
                snapshot_refs=tuple(sorted(eligible_refs)),
                recovery_epoch_ref=collector.required_recovery_epoch_ref,
            )
        else:
            detail = snapshot.failure_reason or (
                "BYBIT_RECOVERY_TRADE_HISTORY_UNVERIFIABLE"
                if recovery_required and bar_gaps_repaired
                else "BYBIT_PUBLIC_SNAPSHOT_UNRECONCILED_OR_CONFLICTED"
            )
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

    @staticmethod
    def _public_snapshot_overlap_is_repaired(
        repository: OpsRepository,
        snapshot: Any,
        *,
        at_ns: int,
        recovery_required: bool,
    ) -> bool:
        from ..data.bybit_source import CAMPAIGN_INTERVALS

        if not recovery_required:
            return True
        archive_root = Path(repository.path).parent / "ops-observations"
        keys = {record.instrument_key for record in snapshot.records}
        if not keys:
            return False
        for key in sorted(keys, key=lambda item: item.native_symbol):
            for interval in CAMPAIGN_INTERVALS:
                bars = reconstruct_causal_bars_from_archive(
                    repository, archive_root, key=key, interval=interval,
                    information_cutoff_ns=at_ns, limit=100_000,
                )
                if not bars:
                    return False
                ordered = tuple(item.bar for item in bars)
                if any(
                    right.open_at_ns - left.open_at_ns != interval.duration_ns
                    for left, right in zip(ordered, ordered[1:], strict=False)
                ):
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
        if source_health_state != "HEALTHY_CURRENT":
            raise ValueError("production event processing requires reconciled current public source health")
        inputs = self.inputs_provider.resolve(repository, event)
        if inputs is None:
            inputs = _empty_event_inputs(repository, event)
        if inputs.universe.envelope.available_at_ns > event.information_cutoff_ns:
            raise ValueError("production universe is unavailable at the event information cutoff")
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
            if not required_inputs.issubset(event.causal_input_refs):
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
            result = OpsStageResultV1(stage, status, tuple(refs), now_ns, reason, action_hash)
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
        candidate_set = assemble_multisleeve_research_candidate_set(
            repository,
            universe=universe,
            decision_event_id=event.event_id,
            cutoff_ns=event.information_cutoff_ns,
            candidates=inputs.candidates,
            policies=policies,
            scanner_evidence_refs=inputs.scanner_evidence_refs,
        )
        acceptances = accept_research_candidates(
            repository,
            candidate_set,
            inputs.candidates,
            accepted_at_ns=event.information_cutoff_ns,
        )

        save(PipelineStageV1.UNIVERSE, OpsStageStatusV1.COMPLETE, (universe.content_hash,))
        feature_refs = tuple(ref for ref in inputs.causal_feature_refs if _indexed_by_cutoff(repository, ref, event))
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
                repository, event, candidate_set, selected, universe, now_ns=now_ns,
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
            cutoff_ns=event.information_cutoff_ns,
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
        action = freeze_action(
            repository,
            candidate=selected,
            candidate_set=candidate_set,
            sizing=sizing,
            product=_required(risk_inputs.product),
            policy=policy,
            v1=_required(risk_inputs.risk_policy_v1),
            v2=_required(risk_inputs.risk_policy_v2),
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
        if economic_inputs is None and isinstance(self.inputs_provider, IndexedProductionEventInputsV1):
            economic_inputs, economic_resolution_reason = self.inputs_provider.resolve_economic(
                repository, event, candidate_set, selected, action, risk_inputs, now_ns=now_ns,
            )
        evaluation: Phase2EvaluationResultV2 | None = None
        evaluation_reason = economic_resolution_reason or _evaluation_inputs_reason(economic_inputs, event)
        if economic_inputs is not None and economic_inputs.complete and evaluation_reason is None:
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

        diagnostic_at = (
            evaluation.evaluation.available_at_ns + 1
            if evaluation is not None
            else max(event.information_cutoff_ns + 1, now_ns)
        )
        m1_reason: str | None
        if diagnostic_at >= selected.deadline_ns:
            m1_ref, diagnostic_failure_reason = _persist_action_diagnostic_failure(
                repository, action, kind="M1", reason="DECISION_DEADLINE_EXPIRED", available_at_ns=now_ns
            )
            save(PipelineStageV1.M1_DIAGNOSTIC, OpsStageStatusV1.NOT_ESTIMABLE, (m1_ref,),
                 action_hash=action.action.action_hash, reason=diagnostic_failure_reason)
            analogue = _not_estimable_analogue(repository, action, selected, candidate_set, now_ns,
                                                "NOT_ESTIMABLE_DECISION_DEADLINE_EXPIRED")
            save(PipelineStageV1.ANALOGUE_DIAGNOSTIC, OpsStageStatusV1.NOT_ESTIMABLE, (analogue,),
                 action_hash=action.action.action_hash, reason="NOT_ESTIMABLE_DECISION_DEADLINE_EXPIRED")
        else:
            m1_ref, m1_reason = _run_m1(repository, action, selected, candidate_set, event,
                                        diagnostic_at, dependency_lock_hash())
            save(
                PipelineStageV1.M1_DIAGNOSTIC,
                OpsStageStatusV1.NOT_ESTIMABLE if m1_reason else OpsStageStatusV1.COMPLETE,
                (m1_ref,),
                action_hash=action.action.action_hash,
                reason=m1_reason,
            )
            analogue_ref = _not_estimable_analogue(
                repository, action, selected, candidate_set, diagnostic_at,
                "NOT_ESTIMABLE_NO_COMPATIBLE_MATURED_ANALOGUES",
            )
            save(
                PipelineStageV1.ANALOGUE_DIAGNOSTIC,
                OpsStageStatusV1.NOT_ESTIMABLE,
                (analogue_ref,),
                action_hash=action.action.action_hash,
                reason="NOT_ESTIMABLE_NO_COMPATIBLE_MATURED_ANALOGUES",
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


class IndexedProductionEventInputsV1:
    """Compose causal public inputs or resolve the exact artifacts already bound by an event."""

    def resolve(self, repository: OpsRepository, event: OpsDecisionEventV1) -> ProductionEventInputsV1 | None:
        causal_refs = set(event.causal_input_refs)
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
                return _compose_public_event_inputs(repository, event, trigger_body)
            universe = _empty_universe(repository, event)

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
            )

        scanner_refs: dict[str, tuple[str, ...]] = {}
        feature_refs: set[str] = set()
        for candidate in candidates:
            feature_refs.add(candidate.snapshot_hash)
            ranks = tuple(sorted(
                entry.artifact_ref for entry in repository.artifact_entries("ScannerRankEvidenceV1")
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
        *, now_ns: int,
    ) -> tuple[ProductionEconomicInputsV1 | None, str | None]:
        return _resolve_indexed_economic_inputs(
            repository, event, candidate_set, candidate, action, risk, now_ns=now_ns,
        )


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
    for entry in repository.artifact_entries("RiskPolicyV1"):
        body = entry.metadata.get("policy")
        if entry.available_at_ns > event.information_cutoff_ns or not isinstance(body, Mapping):
            continue
        try:
            policy = _risk_policy_v1_from_dict(body)
        except (TypeError, ValueError):
            continue
        if policy.policy_hash() == entry.artifact_ref:
            v1_by_hash.setdefault(policy.policy_hash(), []).append(policy)
    for entry in repository.artifact_entries("RiskPolicyV2"):
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
    for entry in repository.artifact_entries("AccountRiskSnapshotV2"):
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


def _resolve_indexed_economic_inputs(
    repository: OpsRepository, event: OpsDecisionEventV1, candidate_set: CandidateSetV2,
    candidate: CandidateActionV2, action: ActionArtifactV2, risk: ProductionRiskInputsV1,
    *, now_ns: int,
) -> tuple[ProductionEconomicInputsV1 | None, str | None]:
    policy_rows: list[tuple[ArtifactIndexEntryV2, AdmissionPolicyV2]] = []
    for entry in repository.artifact_entries("OpsAdmissionPolicyEvidenceV1"):
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
    for entry in repository.artifact_entries("VenueCapabilitySnapshotV2"):
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
    for entry in repository.artifact_entries("OpsCausalInputEvidenceV1"):
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
    for entry in repository.artifact_entries("OpsEconomicScenarioConfigV1"):
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
    for entry in repository.artifact_entries(artifact_type):
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


def _compose_public_event_inputs(
    repository: OpsRepository,
    event: OpsDecisionEventV1,
    trigger_body: Mapping[str, Any],
) -> ProductionEventInputsV1:
    """Run accepted point-in-time features and S1/S2/S3 coordinators on archived evidence."""
    if (trigger_body.get("version") != "OPS_PUBLIC_FINAL_BAR_TRIGGER_V1"
            or trigger_body.get("information_cutoff_ns") != event.information_cutoff_ns):
        return _empty_event_inputs(repository, event)
    product_entry = repository.get_artifact(str(trigger_body.get("product_ref", "")))
    product_body = product_entry.metadata.get("product") if product_entry is not None else None
    if (product_entry is None or product_entry.artifact_type != "ProductContractV2"
            or not isinstance(product_body, Mapping)):
        return _empty_event_inputs(repository, event)
    trigger_product = ProductContractV2.from_dict(json_value(product_body))
    if trigger_product.content_hash != product_entry.artifact_ref:
        return _empty_event_inputs(repository, event)

    archive_root = Path(repository.path).parent / "ops-observations"
    products: list[ProductContractV2] = []
    for entry in repository.artifact_entries("ProductContractV2"):
        body = entry.metadata.get("product")
        if entry.available_at_ns <= event.information_cutoff_ns and isinstance(body, Mapping):
            product = ProductContractV2.from_dict(json_value(body))
            if product.content_hash == entry.artifact_ref:
                products.append(product)
    products.sort(key=lambda item: item.key.to_canonical_json())

    histories: dict[str, dict[BarIntervalV2, tuple[Any, ...]]] = {}
    store = CausalBarStoreV2()
    causal_bar_entries: dict[str, ArtifactIndexEntryV2] = {}
    source_refs: set[str] = {event.trigger_ref, trigger_product.content_hash}
    latest_health: dict[str, PublicSourceHealthV2] = {}
    health_entries = repository.artifact_entries("PublicSourceHealthV2")
    for entry in health_entries:
        body = entry.metadata.get("health")
        if entry.available_at_ns <= event.information_cutoff_ns and isinstance(body, Mapping):
            health = PublicSourceHealthV2.from_dict(body)
            if health.content_hash == entry.artifact_ref:
                current = latest_health.get(health.source_id)
                if current is None or (health.available_at_ns, health.content_hash) > (
                        current.available_at_ns, current.content_hash):
                    latest_health[health.source_id] = health
    source_refs.update(item.content_hash for item in latest_health.values())

    for product in products:
        frames: dict[BarIntervalV2, tuple[Any, ...]] = {}
        for interval in (BarIntervalV2.M15, BarIntervalV2.H1, BarIntervalV2.H4, BarIntervalV2.M1):
            indexed_bars = reconstruct_causal_bars_from_archive(
                repository, archive_root, key=product.key, interval=interval,
                information_cutoff_ns=event.information_cutoff_ns,
                availability_class=AvailabilityClassV2.ACTUAL_SYSTEM, limit=100_000,
            )
            bars = tuple(item.bar for item in indexed_bars)
            frames[interval] = bars
            for item in indexed_bars:
                causal_bar_entries[item.bar.content_hash] = _causal_bar_entry(
                    item.bar, item.observation_index_ref,
                )
                store.append(item.bar)
                source_refs.update((item.observation_index_ref, item.bar.content_hash))
        histories[product.key.to_canonical_json()] = frames
    if causal_bar_entries:
        existing_bars = repository.get_artifact_metadata_by_refs(tuple(causal_bar_entries))
        missing_bar_entries: list[ArtifactIndexEntryV2] = []
        for ref, entry in sorted(causal_bar_entries.items()):
            existing = existing_bars.get(ref)
            if existing is None:
                missing_bar_entries.append(entry)
                continue
            metadata = existing.get("metadata")
            if (existing.get("artifact_type") != "CausalBarV2"
                    or existing.get("content_hash") != ref
                    or existing.get("available_at_ns") != entry.available_at_ns
                    or not isinstance(metadata, Mapping)
                    or canonical_json(metadata.get("bar")) != canonical_json(entry.metadata.get("bar"))):
                raise ValueError("causal bar ref already indexes conflicting immutable evidence")
        if missing_bar_entries:
            repository.register_artifacts(tuple(missing_bar_entries))

    primary_frames = histories.get(trigger_product.key.to_canonical_json(), {})
    primary_m15 = primary_frames.get(BarIntervalV2.M15, ())
    trigger_bar = next((bar for bar in primary_m15
                        if bar.content_hash == trigger_body.get("bar_ref")), None)
    if trigger_bar is None:
        return _empty_event_inputs(repository, event)
    if (trigger_bar.raw.source_id != event.source_id or trigger_bar.close_at_ns > event.information_cutoff_ns
            or trigger_bar.raw.available_at_ns > event.information_cutoff_ns):
        return _empty_event_inputs(repository, event)

    market: dict[str, tuple[PublicSourceHealthV2, EventGate | None,
                            ExecutableQuote | None, MarkIndexEvidence | None, Decimal]] = {}
    observations_for_universe: list[UniverseObservationV2] = []
    universe_observation_refs: dict[str, str] = {}
    eligible_order: list[tuple[Decimal, str]] = []
    for product in products:
        key_json = product.key.to_canonical_json()
        frames = histories.get(key_json, {})
        m15 = frames.get(BarIntervalV2.M15, ())
        h1 = frames.get(BarIntervalV2.H1, ())
        h4 = frames.get(BarIntervalV2.H4, ())
        if not m15:
            continue
        source_id = m15[-1].raw.source_id
        source_health = latest_health.get(source_id)
        if source_health is None or not source_health.data_eligible:
            continue
        quote, mark, quote_refs = _indexed_quote_and_mark(
            repository, archive_root, product, cutoff_ns=event.information_cutoff_ns,
        )
        gate, gate_ref = _latest_event_gate(repository, event.information_cutoff_ns)
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
        observed_days = len({bar.close_at_ns // (24 * 60 * 60 * 1_000_000_000) for bar in m15})
        s1_days = min(len(h4) // 6, len(h1) // 24, len(m15) // 96)
        s2_days = len(m15) // 96
        s3_days = len(frames.get(BarIntervalV2.M1, ())) // 1440
        health_age_ok = (source_health.available_at_ns <= event.information_cutoff_ns
                         and source_health.observed_at_ns <= event.information_cutoff_ns)
        if quote_valid and spread is not None and health_age_ok:
            refs = tuple(sorted({product.content_hash, source_health.content_hash, *quote_refs,
                                 *(bar.content_hash for bar in (*m15, *h1, *h4))}))
            observation = UniverseObservationV2(
                product, observed_days, bool(m15 and h1 and h4), turnover, spread,
                source_health.state, event.information_cutoff_ns,
                {S1_POLICY.policy_id: s1_days, S2_POLICY.policy_id: s2_days,
                 S3_POLICY.policy_id: s3_days}, source_health.available_at_ns,
                open_position=False,
                active_watch=any(watch.key == product.key for watch in repository.list_active_watches()),
            )
            observations_for_universe.append(observation)
            observation_body = {
                "version": "UNIVERSE_OBSERVATION_INDEX_V2_V1",
                "product_ref": product.content_hash,
                "observation_hash": observation.content_hash,
                "source_refs": list(refs),
                "available_at_ns": event.information_cutoff_ns,
            }
            observation_ref = observation.content_hash
            repository.register_artifact(ArtifactIndexEntryV2(
                observation_ref, "UniverseObservationV2", observation_ref,
                event.information_cutoff_ns, event.information_cutoff_ns,
                {"observation": observation_body},
            ))
            universe_observation_refs[key_json] = observation_ref
            source_refs.add(observation_ref)
            eligible_order.append((turnover, key_json))
        market[key_json] = (source_health, gate, quote, mark, product.tick_size)

    built = DynamicUniverseRuntimeV2().build_snapshot(
        tuple(observations_for_universe), decision_slot_ns=event.information_cutoff_ns,
        information_cutoff_ns=event.information_cutoff_ns, created_at_ns=event.information_cutoff_ns,
        selection_policy_hash=SELECTION_POLICY_HASH,
        input_refs=tuple(sorted(source_refs)),
    )
    _index_universe(repository, built.universe)
    universe = research_selection_universe(built.universe)
    repository.register_artifact(ArtifactIndexEntryV2(
        MULTI_SLEEVE_SELECTION_HASH, "ResearchSelectionPolicyV2", MULTI_SLEEVE_SELECTION_HASH,
        0, 0, MULTI_SLEEVE_SELECTION_BODY,
    ))
    _index_universe(repository, universe)

    feature_refs: set[str] = set()
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
        join = asof_join(store, trigger_product.key, cutoff_ns=event.information_cutoff_ns,
                         trigger_ref=trigger_bar.content_hash, source_health=trigger_health)
        if join.status == "AVAILABLE":
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
            feature = feature_snapshot(feature_join)
            repository.register_artifact(ArtifactIndexEntryV2(
                feature.content_hash, "FeatureArtifactV2", feature.content_hash,
                feature.envelope.created_at_ns, feature.envelope.available_at_ns,
                {"feature": feature.to_dict()},
            ))
            feature_refs.add(feature.content_hash)
            source_refs.update(feature.envelope.input_refs)
            if gate is not None:
                source_refs.add(gate.evidence_ref)
            s3_residuals, s3_current_vwap, s3_trades, s3_trade_health, s3_refs = _resolve_indexed_s3_inputs(
                repository, archive_root, trigger_product.key,
                cutoff_ns=event.information_cutoff_ns,
                completed_1m=frames.get(BarIntervalV2.M1, ()),
                health_by_source=latest_health,
            )
            source_refs.update(s3_refs)
            waiting = [watch for watch in repository.list_active_watches()
                       if watch.key == trigger_product.key and watch.state.value == "WAITING_FOR_EVENT"]
            s1_waiting = [watch for watch in waiting if watch.policy_hash == S1_POLICY.policy_hash]
            s1 = S1ShadowCoordinator(repository)
            if s1_waiting:
                for watch in s1_waiting:
                    decision = s1.on_bar(watch.watch_id, join, feature, event_gate=gate,
                                         bbo=quote, mark_index=mark)
                    if decision.candidate is not None:
                        candidates[decision.candidate.candidate_id] = decision.candidate
            else:
                decision = s1.create_watch(join, feature, event_gate=gate, universe=universe)
                if decision.candidate is not None:
                    candidates[decision.candidate.candidate_id] = decision.candidate
            s2 = S2ShadowCoordinator(repository).on_trigger_close(
                join, feature, universe=universe, bbo=quote,
            )
            if s2.candidate is not None:
                candidates[s2.candidate.candidate_id] = s2.candidate
            s3 = S3ShadowCoordinator(repository)
            completed_m1 = tuple(
                bar for bar in frames.get(BarIntervalV2.M1, ())
                if bar.final and bar.close_at_ns <= event.information_cutoff_ns
                and bar.raw.available_at_ns <= event.information_cutoff_ns
            )
            for watch in waiting:
                if watch.policy_hash != S3_POLICY.policy_hash:
                    continue
                trigger_m1 = next(
                    (bar for bar in reversed(completed_m1) if bar.close_at_ns > watch.created_at_ns), None,
                )
                if trigger_m1 is None:
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
                    watch_id=watch.watch_id, trigger=trigger_m1,
                    cutoff_ns=event.information_cutoff_ns, frozen_vwap=frozen[0],
                    residual_sigma=float(sigma), quote=quote, tick_size=tick_size,
                    feature=feature, universe=universe, event_gate=gate,
                    bar_health=trigger_health,
                )
                if result.candidate is not None:
                    candidates[result.candidate.candidate_id] = result.candidate
            # Missing trade VWAP, 1M residuals or health remain S3's existing
            # NOT_ESTIMABLE contract; the coordinator receives only resolved evidence.
            s3.evaluate_setup(
                key=trigger_product.key, cutoff_ns=event.information_cutoff_ns,
                residuals=s3_residuals, current_vwap=s3_current_vwap, trades=s3_trades,
                completed_1m=frames.get(BarIntervalV2.M1, ()), context=join,
                feature=feature, quote=quote, tick_size=tick_size, universe=universe,
                event_gate=gate, bar_health=trigger_health, trade_health=s3_trade_health,
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
        if (candidate_entry is None or feature_entry is None or candidate_entry.available_at_ns > event.information_cutoff_ns
                or feature_entry.available_at_ns > event.information_cutoff_ns):
            continue
        source = ScannerSelectionSourceV1(
            candidate.candidate_id, candidate.key, rank, "PUBLIC_UNIVERSE_TURNOVER_V1", "1.0.0",
            universe.content_hash, event.event_id, event.information_cutoff_ns,
            (universe_observation_refs[candidate.key.to_canonical_json()],),
        )
        source_ref = register_scanner_source(repository, source)
        rank_evidence = ScannerRankEvidenceV1(
            candidate.candidate_id, rank, source.scanner_policy_id, source.scanner_policy_version,
            universe.content_hash, event.event_id, event.information_cutoff_ns, source_ref,
        )
        scanner_refs[candidate.candidate_id] = (register_scanner_rank(repository, rank_evidence),)
        source_refs.update((source_ref, rank_evidence.content_hash))

    return ProductionEventInputsV1(
        universe, tuple(sorted(candidates.values(), key=lambda item: item.candidate_id)),
        scanner_refs, {}, {}, tuple(sorted(feature_refs)), tuple(sorted(source_refs)),
    )


def _latest_event_gate(repository: OpsRepository, cutoff_ns: int) -> tuple[EventGate | None, str | None]:
    eligible: list[tuple[ArtifactIndexEntryV2, Mapping[str, Any]]] = []
    for entry in repository.artifact_entries("EventSafetyGateV2"):
        body = entry.metadata.get("gate")
        if (entry.available_at_ns <= cutoff_ns and isinstance(body, Mapping)
                and body.get("schema_version") == 2
                and type(body.get("cutoff_ns")) is int and body["cutoff_ns"] <= cutoff_ns
                and body.get("availability_view") == "ACTUAL_SYSTEM"):
            eligible.append((entry, body))
    if not eligible:
        return None, None
    entry, body = max(eligible, key=lambda item: (item[0].available_at_ns, item[0].artifact_ref))
    try:
        gate = EventGate(EventState(str(body["state"])), entry.available_at_ns,
                         entry.artifact_ref, str(body["gate_version"]))
    except (KeyError, ValueError, TypeError):
        return None, None
    return gate, entry.artifact_ref


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
    observations = reconstruct_public_observations_from_archive(
        repository, archive_root, instrument_revision=product.key.contract_revision,
        information_cutoff_ns=cutoff_ns, event_types=kinds, limit=10_000,
    )
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

    source = public_stream_source or PublicStreamSourceV2(
        venue=VenueV2.BYBIT,
        topics=bybit_btc_eth_linear_topics(),
        source_id=BYBIT_PUBLIC_WS_SOURCE_ID_V1,
    )
    return ProductionOpsCyclePortV1(
        public_source=public_source or BybitPublicCycleSourceV1(),
        public_stream_source=source,
        clock_ns=clock_ns,
    )


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


def _empty_event_inputs(repository: OpsRepository, event: OpsDecisionEventV1) -> ProductionEventInputsV1:
    return ProductionEventInputsV1(_empty_universe(repository, event), (), {}, {}, {}, ())


def _empty_universe(repository: OpsRepository, event: OpsDecisionEventV1) -> UniverseContractV2:
    built = DynamicUniverseRuntimeV2(min_observed_days=30).build_snapshot(
        (),
        decision_slot_ns=event.information_cutoff_ns,
        information_cutoff_ns=event.information_cutoff_ns,
        created_at_ns=event.information_cutoff_ns,
        selection_policy_hash=SELECTION_POLICY_HASH,
    )
    body = built.universe.to_dict()
    repository.register_artifact(ArtifactIndexEntryV2(
        built.universe.content_hash, "UniverseContractV2", built.universe.content_hash,
        event.information_cutoff_ns, event.information_cutoff_ns, {"universe": body},
    ))
    return research_selection_universe(built.universe)


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
            event.information_cutoff_ns, event.information_cutoff_ns,
        )
    else:
        row = DecisionCalendarEntryV2(
            candidate_set.content_hash, None,
            "MULTI_SLEEVE_RESEARCH_SELECTION_V1", "1.0.0-research",
            candidate_set.selection_policy_hash, event.information_cutoff_ns,
            SelectionStateV2.NOT_ESTIMABLE, AdmissionStateV2.NOT_APPLICABLE,
            None, None, DecisionSourceStageV2.CANDIDATE_SET, ("CANDIDATE_SELECTION_NOT_ESTIMABLE",),
            candidate_set.content_hash, event.information_cutoff_ns, event.information_cutoff_ns,
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
    row = DecisionCalendarEntryV2(
        candidate_set.content_hash, candidate.content_hash,
        _POLICIES[candidate.policy_hash].policy_id, _POLICIES[candidate.policy_hash].version,
        candidate.policy_hash, candidate.decision_at_ns, SelectionStateV2.SELECTED, admission,
        action.action.action_hash if action is not None else None,
        action.content_hash if action is not None else None,
        DecisionSourceStageV2.HARD_RISK, tuple(sorted(set(sizing.reasons)),),
        sizing.content_hash, sizing.available_at_ns, sizing.available_at_ns,
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
    path = Path(__file__).resolve().parents[4] / "requirements-lock.txt"
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None
