"""ATLAS-owned composition of the existing non-capital V2 research APIs.

This module owns the ordering between source recovery, causal evidence, sleeve
selection, hard-risk sizing, action freezing, economic evaluation and the
decision calendar. It owns no second repository and implements no market,
selector, risk or economics rules of its own.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol, cast

from atlas.domain.risk import RiskPolicy

from .._serialization import canonical_json, json_value, sha256_json, sha256_ref
from ..contracts import CandidateActionV2, CandidateSetV2, PolicySpecV2
from ..data.bars import BarIntervalV2, CausalBarStoreV2
from ..data.collector import PublicCollectorV2
from ..data.health import PublicSourceHealthV2
from ..data.history import (
    ParquetObservationArchiveV2,
    reconstruct_causal_bars_from_archive,
    reconstruct_public_observations_from_archive,
)
from ..data.raw import AvailabilityClassV2
from ..data.subscriptions import SubscriptionPlanV2
from ..data.universe import ComputeTierV2, DynamicUniverseRuntimeV2, UniverseObservationV2
from ..features.joins import asof_join
from ..features.pipeline import feature_snapshot
from ..instruments import InstrumentKeyV2, InstrumentRegistryV2, ProductContractV2, UniverseContractV2
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
from ..strategies.s3_mean_reversion import S3_POLICY, S3ShadowCoordinator
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
        _reconcile_public_sources(repository, collector, tuple(sorted(source_ids)), now_ns=now_ns)

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
            source_id: collector.health.latest(source_id)
            for source_id in sorted(source_ids)
        }
        healthy = bool(health_states) and all(
            state is not None and state.data_eligible and state.available_at_ns <= now_ns
            for state in health_states.values()
        )
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


def _reconcile_public_sources(
    repository: OpsRepository,
    collector: PublicCollectorV2,
    source_ids: tuple[str, ...],
    *,
    now_ns: int,
) -> None:
    """Reconcile only from a persisted, exact public snapshot/gap-repair receipt."""
    by_source: dict[str, list[ArtifactIndexEntryV2]] = {}
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
                snapshot_refs=tuple(refs),
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


@dataclass(frozen=True)
class ProductionCollectorRecoveryV1:
    collector: PublicCollectorV2
    restored_subscription_plan: SubscriptionPlanV2
    required_source_ids: tuple[str, ...]
    had_prior_source_state: bool


class ProductionOpsCyclePortV1:
    """Concrete ATLAS pipeline composition for the Session-027 supervisor."""

    def __init__(
        self,
        *,
        public_source: ProductionPublicCycleSourceV1 | None = None,
        inputs_provider: ProductionEventInputsProviderV1 | None = None,
        crash_after_checkpoint: Callable[[PipelineStageV1], None] | None = None,
    ) -> None:
        self.public_source = public_source or IndexedPublicCycleSourceV1()
        self.inputs_provider = inputs_provider or IndexedProductionEventInputsV1()
        self.crash_after_checkpoint = crash_after_checkpoint
        self._collector_recovery: ProductionCollectorRecoveryV1 | None = None
        self._recovery_calls = 0
        self._collection_calls = 0

    def recover(self, repository: OpsRepository, *, now_ns: int) -> OpsRecoverySnapshotV1:
        """Restore collector cursors, active watches and subscriptions first."""
        if repository.read_only:
            raise ValueError("production ops composition requires the supervisor-owned writable repository")
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
            collector, restart.subscriptions, source_ids, had_prior
        )
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
        repository.register_artifacts(tuple(causal_bar_entries[key] for key in sorted(causal_bar_entries)))

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
            waiting = [watch for watch in repository.list_active_watches()
                       if watch.key == trigger_product.key and watch.state.value == "WAITING_FOR_EVENT"]
            s1 = S1ShadowCoordinator(repository)
            if waiting:
                for watch in waiting:
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
            # S3 still receives its real typed causal history. Missing trade VWAP,
            # 1M residuals or health remain its own NOT_ESTIMABLE contract.
            S3ShadowCoordinator(repository).evaluate_setup(
                key=trigger_product.key, cutoff_ns=event.information_cutoff_ns,
                residuals=(), current_vwap=None, trades=(),
                completed_1m=frames.get(BarIntervalV2.M1, ()), context=join,
                feature=feature, quote=quote, tick_size=tick_size, universe=universe,
                event_gate=gate, bar_health=trigger_health, trade_health=trigger_health,
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
