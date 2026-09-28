"""ATLAS-owned composition of the existing non-capital V2 research APIs.

This module owns the ordering between source recovery, causal evidence, sleeve
selection, hard-risk sizing, action freezing, economic evaluation and the
decision calendar. It owns no second repository and implements no market,
selector, risk or economics rules of its own.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol, cast

from atlas.domain.risk import RiskPolicy

from .._serialization import canonical_json, sha256_json, sha256_ref
from ..contracts import CandidateActionV2, CandidateSetV2, PolicySpecV2
from ..data.collector import PublicCollectorV2
from ..data.history import ParquetObservationArchiveV2
from ..data.subscriptions import SubscriptionPlanV2
from ..data.universe import ComputeTierV2, DynamicUniverseRuntimeV2
from ..instruments import InstrumentRegistryV2, ProductContractV2, UniverseContractV2
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository
from ..risk import (
    AccountRiskSnapshotV2,
    ClosedV2Outcome,
    FeeScheduleV2,
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
    MULTI_SLEEVE_SELECTION_HASH,
    assemble_multisleeve_research_candidate_set,
    persist_research_sleeve_audit,
    research_selection_universe,
)
from ..selection import SELECTION_POLICY_HASH, accept_research_candidates
from ..strategies.s1_trend import S1_POLICY
from ..strategies.s2_breakout import S2_POLICY
from ..strategies.s3_mean_reversion import S3_POLICY
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
        object.__setattr__(self, "candidates", candidates)
        object.__setattr__(self, "scanner_evidence_refs", MappingProxyType({
            str(key): tuple(value) for key, value in self.scanner_evidence_refs.items()
        }))
        object.__setattr__(self, "risk_inputs", MappingProxyType(dict(self.risk_inputs)))
        object.__setattr__(self, "economic_inputs", MappingProxyType(dict(self.economic_inputs)))
        object.__setattr__(self, "causal_feature_refs", features)


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
    """Read-only event handoff for collector-owned public artifacts.

    Public collectors register ``OpsDecisionEventSourceV1`` only after their
    market input, receipt time and availability chronology are durable. The
    supervisor then applies the common source-health and event idempotency
    gates. No market evidence is synthesized when there is no such handoff.
    """

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
        events: list[OpsDecisionEventV1] = []
        refs: list[str] = []
        for entry in repository.artifact_entries("OpsDecisionEventSourceV1"):
            body = entry.metadata.get("event")
            if entry.available_at_ns > now_ns or not isinstance(body, Mapping):
                continue
            event = decision_event_from_dict(body)
            if event.available_at_ns <= now_ns:
                events.append(event)
                refs.append(entry.artifact_ref)
        events.sort(key=lambda item: (item.available_at_ns, item.information_cutoff_ns, item.event_id))
        source_ids = tuple(sorted(
            set(self.required_source_ids)
            | set(repository.source_health_sources())
            | {event.source_id for event in events}
        ))
        states = tuple(
            OpsSourceStateV1(
                source_id,
                (history[-1].status if (history := repository.source_health_history(source_id)) else "UNKNOWN"),
                (history[-1].observed_at_ns if history else None),
                (history[-1].available_at_ns if history else None),
            )
            for source_id in source_ids
        )
        reconciled = bool(source_ids) and all(
            item.state == "HEALTHY_CURRENT"
            and item.observed_at_ns is not None
            and item.available_at_ns is not None
            and item.available_at_ns <= now_ns
            for item in states
        )
        if not reconciled:
            # Keep durable decision handoffs queued while the collector is
            # replaying a reconnect gap. A restart must not turn an unfinished
            # event into a permanent source-health terminal receipt.
            events = []
        return OpsCycleBatchV1(tuple(events), states, source_ids, tuple(refs), reconciled, now_ns)


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
                product = ProductContractV2.from_dict(body)
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
        if risk_reason is not None:
            reason = risk_reason
            runtime_ref = _index_runtime_decision(
                repository,
                candidate_set=candidate_set,
                candidate=selected,
                action=None,
                admission=AdmissionStateV2.NOT_ESTIMABLE,
                reason=reason,
                available_at_ns=now_ns,
            )
            calendar_ref = _persist_runtime_calendar(
                repository, candidate_set, selected, None, AdmissionStateV2.NOT_ESTIMABLE,
                (reason,), runtime_ref, now_ns,
            )
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
        evaluation: Phase2EvaluationResultV2 | None = None
        evaluation_reason = _evaluation_inputs_reason(economic_inputs, event)
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
            runtime_ref = _index_runtime_decision(
                repository,
                candidate_set=candidate_set,
                candidate=selected,
                action=action,
                admission=AdmissionStateV2.NOT_ESTIMABLE,
                reason=evaluation_reason or "ECONOMIC_EVIDENCE_UNAVAILABLE",
                available_at_ns=now_ns,
            )
            calendar_ref = _persist_runtime_calendar(
                repository, candidate_set, selected, action, AdmissionStateV2.NOT_ESTIMABLE,
                (evaluation_reason or "ECONOMIC_EVIDENCE_UNAVAILABLE",), runtime_ref, now_ns,
            )
        save(PipelineStageV1.DECISION_CALENDAR, OpsStageStatusV1.COMPLETE, (calendar_ref,))

        if evaluation is None or evaluation.evaluation.decision.value == "NOT_ESTIMABLE":
            return _result(stages, OpsTerminalStatusV1.NOT_ESTIMABLE,
                           evaluation_reason or (evaluation.evaluation.reason_codes[0] if evaluation else "NOT_ESTIMABLE"))
        if evaluation.evaluation.decision.value == "NO_TRADE":
            return _result(stages, OpsTerminalStatusV1.NO_TRADE,
                           evaluation.evaluation.reason_codes[0] if evaluation.evaluation.reason_codes else "ECONOMIC_NO_TRADE")
        return _result(stages, OpsTerminalStatusV1.COMPLETE, None)


class IndexedProductionEventInputsV1:
    """Resolve exact event-bound CandidateAction/feature evidence from ops storage."""

    def resolve(self, repository: OpsRepository, event: OpsDecisionEventV1) -> ProductionEventInputsV1 | None:
        causal_refs = set(event.causal_input_refs)
        universe: UniverseContractV2 | None = None
        for ref in sorted(causal_refs):
            entry = repository.get_artifact(ref)
            body = entry.metadata.get("universe") if entry is not None and entry.artifact_type == "UniverseContractV2" else None
            if isinstance(body, Mapping):
                parsed = UniverseContractV2.from_dict(body)
                if parsed.content_hash == ref and parsed.envelope.available_at_ns <= event.information_cutoff_ns:
                    universe = parsed
                    break
        if universe is None:
            universe = _empty_universe(repository, event)

        candidates: list[CandidateActionV2] = []
        for ref in sorted(causal_refs):
            entry = repository.get_artifact(ref)
            body = entry.metadata.get("candidate") if entry is not None and entry.artifact_type == "CandidateActionV2" else None
            if not isinstance(body, Mapping):
                continue
            candidate = CandidateActionV2.from_dict(body)
            if (candidate.content_hash == ref and candidate.decision_at_ns == event.information_cutoff_ns
                    and candidate.policy_hash in _POLICIES and candidate.envelope.available_at_ns <= event.information_cutoff_ns):
                candidates.append(candidate)
        if not candidates:
            return ProductionEventInputsV1(universe, (), {}, {}, {}, ())

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
        )


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


def _persist_runtime_calendar(
    repository: OpsRepository,
    candidate_set: CandidateSetV2,
    candidate: CandidateActionV2,
    action: ActionArtifactV2 | None,
    admission: AdmissionStateV2,
    reasons: tuple[str, ...],
    source_ref: str,
    now_ns: int,
) -> str:
    row = DecisionCalendarEntryV2(
        candidate_set.content_hash, candidate.content_hash,
        _POLICIES[candidate.policy_hash].policy_id, _POLICIES[candidate.policy_hash].version,
        candidate.policy_hash, candidate.decision_at_ns, SelectionStateV2.SELECTED, admission,
        action.action.action_hash if action is not None else None,
        action.content_hash if action is not None else None,
        DecisionSourceStageV2.OPS_RUNTIME_GATE, tuple(sorted(set(reasons))), source_ref,
        now_ns, now_ns,
    )
    return index_decision_calendar_entry(repository, row)


def _persist_sizing_calendar(
    repository: OpsRepository,
    candidate_set: CandidateSetV2,
    candidate: CandidateActionV2,
    sizing: SizingDecisionV2,
    admission: AdmissionStateV2,
) -> str:
    row = DecisionCalendarEntryV2(
        candidate_set.content_hash, candidate.content_hash,
        _POLICIES[candidate.policy_hash].policy_id, _POLICIES[candidate.policy_hash].version,
        candidate.policy_hash, candidate.decision_at_ns, SelectionStateV2.SELECTED, admission,
        None, None, DecisionSourceStageV2.HARD_RISK, tuple(sorted(set(sizing.reasons)),),
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
