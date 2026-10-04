"""Session-027 tests for the ATLAS-owned production composition."""

from __future__ import annotations

import ast
import builtins
import importlib
import socket
import urllib.request
from collections.abc import Mapping
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from atlas.domain.risk import engineering_default_policy
from atlas.v2._serialization import FrozenMap, canonical_json, json_value, sha256_json
from atlas.v2.contracts import CandidateActionV2, CandidateSetV2
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.collector import PublicCollectorV2
from atlas.v2.data.health import PublicSourceHealthV2, PublicSourceStateV2
from atlas.v2.data.history import ParquetObservationArchiveV2
from atlas.v2.data.raw import RawObservationV2
from atlas.v2.instruments import (
    InstrumentRegistryV2,
    ProductContractV2,
    TradingStatusV2,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.risk import (
    AccountRiskSnapshotV2,
    RiskPolicyV2,
    StressBoundV2,
    VenueSizingLimitsV2,
    index_research_evidence,
    index_risk_evidence,
    index_risk_policies,
    size_selected_candidate,
)
from atlas.v2.runtime import production
from atlas.v2.runtime.ops_supervisor import (
    OpsCycleBatchV1,
    OpsDecisionEventV1,
    OpsRecoverySnapshotV1,
    OpsSourceStateV1,
    OpsSupervisorV2,
    OpsTerminalStatusV1,
    PipelineStageV1,
)
from atlas.v2.science.action import ActionArtifactV2, FrozenActionV2, freeze_action
from atlas.v2.science.admission import (
    VenueCapabilitySnapshotV2,
    VenueCapabilityStatusV2,
    index_venue_capability_snapshot,
)
from atlas.v2.science.costs import FeeScheduleV2, index_cost_evidence
from atlas.v2.science.outcomes import (
    AdmissionStateV2,
    DecisionCalendarEntryV2,
    DecisionSourceStageV2,
    SelectionStateV2,
)
from atlas.v2.science.research_selection import assemble_multisleeve_research_candidate_set
from atlas.v2.strategies.s1_trend import S1_POLICY
from atlas.v2.strategies.s2_breakout import S2_POLICY
from atlas.v2.strategies.s3_mean_reversion import S3_POLICY

from .session023_support import research_case
from .test_session014_core import KEY
from .test_session016_candidate_selection import CUTOFF, evidence
from .test_session020_phase2_e2e import (
    _admission_policy,
    _capability_for_action,
    _causal_input,
)
from .test_session027_ops_supervisor import FakeClock, make_event


class StaticInputsProvider:
    def __init__(self, event_id: str, inputs: production.ProductionEventInputsV1) -> None:
        self.event_id = event_id
        self.inputs = inputs
        self.calls = 0

    def resolve(self, repository: OpsRepository, event: OpsDecisionEventV1):
        del repository
        assert event.event_id == self.event_id
        self.calls += 1
        return self.inputs


class ReconciledFixturePublicSource:
    """Deterministic public-source seam that exercises the real collector object."""

    def __init__(self, event: OpsDecisionEventV1 | None, *, source_id: str = "PUBLIC_MARKET") -> None:
        self.event = event
        self.source_id = source_id
        self.calls: list[str] = []
        self.repositories: list[OpsRepository] = []
        self.collectors: list[PublicCollectorV2] = []
        self.recovery_snapshots: list[OpsRecoverySnapshotV1] = []
        self.trigger_refs: list[str] = []

    @property
    def required_source_ids(self) -> tuple[str, ...]:
        return (self.source_id,)

    def collect(self, repository, collector, *, now_ns, recovery):
        self.calls.append("collect")
        self.repositories.append(repository)
        self.collectors.append(collector)
        self.recovery_snapshots.append(recovery)
        assert collector.repository is repository
        health = collector.health.latest(self.source_id)
        if health is None:
            collector.reconnected(self.source_id, at_ns=now_ns - 2)
            health = self._reconcile(repository, collector, at_ns=now_ns)
        elif health.state != PublicSourceStateV2.HEALTHY_CURRENT and health.observed_at_ns < now_ns:
            health = self._reconcile(repository, collector, at_ns=now_ns)
        healthy = health.state == PublicSourceStateV2.HEALTHY_CURRENT
        events = (self.event,) if self.event is not None and healthy else ()
        if self.event is not None and events:
            body = {
                "event_id": self.event.event_id,
                "trigger": self.event.event_type,
                "cutoff_ns": self.event.information_cutoff_ns,
            }
            repository.register_artifact(_trigger_entry(self.event, body))
            self.trigger_refs.append(self.event.trigger_ref)
        state = OpsSourceStateV1(
            self.source_id,
            health.state.value,
            health.observed_at_ns,
            health.available_at_ns,
        )
        return OpsCycleBatchV1(events, (state,), (self.source_id,), (), healthy, now_ns)

    def _reconcile(self, repository, collector, *, at_ns):
        observation = RawObservationV2.build(
            instrument_revision=KEY.contract_revision, source_id=self.source_id,
            event_type="RECONNECT_SNAPSHOT_FIXTURE", event_at_ns=at_ns,
            received_at_ns=at_ns, ingested_at_ns=at_ns, available_at_ns=at_ns,
            translation_version="session027-reconnect-fixture-v1", payload={"fixture": "overlap-snapshot"},
        )
        observation_ref = sha256_json({
            "artifact_type": "PublicObservationIndexV2", "record_id": observation.record_id,
        })
        repository.register_artifact(ArtifactIndexEntryV2(
            observation_ref, "PublicObservationIndexV2", observation.content_hash,
            at_ns, at_ns,
            {"record_id": observation.record_id, "source_id": self.source_id,
             "instrument_revision": observation.instrument_revision,
             "instrument_key_json": KEY.to_canonical_json(), "event_at_ns": observation.event_at_ns,
             "published_at_ns": observation.published_at_ns,
             "raw_payload_hash": observation.raw_payload_hash},
        ))
        epoch_ref = collector.required_recovery_epoch_ref
        assert epoch_ref is not None
        return collector.reconcile_after_reconnect(
            self.source_id, at_ns=at_ns, complete_snapshot=True, missed_interval_repaired=True,
            snapshot_refs=(observation_ref,), recovery_epoch_ref=epoch_ref,
        )


def _trigger_entry(event: OpsDecisionEventV1, body: dict[str, object]) -> ArtifactIndexEntryV2:
    return ArtifactIndexEntryV2(
        event.trigger_ref,
        "DecisionTriggerFixtureV1",
        event.trigger_ref,
        event.available_at_ns,
        event.available_at_ns,
        body,
    )


def _risk_inputs(case, *, account=None, fee=None, stress=None):
    return production.ProductionRiskInputsV1(
        case.product,
        case.v1,
        case.v2,
        case.account if account is None else account,
        case.exposures,
        case.outcomes,
        case.venue,
        case.stress if stress is None else stress,
        case.fee if fee is None else fee,
    )


def _make_fixture_inputs(
    repository: OpsRepository,
    event_id: str,
    *,
    missing_account: bool = False,
    future_account: bool = False,
    future_fee: bool = False,
    future_stress: bool = False,
    future_capability: bool = False,
    no_economic_inputs: bool = False,
):
    case = research_case(repository)
    scanner_refs = {
        item.candidate_id: (evidence(repository, item, case.universe, rank, event=event_id),)
        for rank, item in enumerate(case.competitors, start=1)
    }
    candidate_set = assemble_multisleeve_research_candidate_set(
        repository,
        universe=case.universe,
        decision_event_id=event_id,
        cutoff_ns=CUTOFF,
        candidates=case.competitors,
        policies={policy.policy_hash: policy for policy in (S1_POLICY, S2_POLICY, S3_POLICY)},
        scanner_evidence_refs=scanner_refs,
    )
    selected = next(item for item in case.competitors if item.candidate_id == candidate_set.selected_candidate_id)
    if selected.candidate_id != case.candidate.candidate_id:
        raise AssertionError("deterministic fixture must select the original exact S1 action")

    account = replace(case.account, available_at_ns=CUTOFF + 1) if future_account else case.account
    fee = replace(case.fee, available_at_ns=CUTOFF + 1) if future_fee else case.fee
    stress = replace(case.stress, available_at_ns=CUTOFF + 1) if future_stress else case.stress
    risk = _risk_inputs(case, account=account, fee=fee, stress=stress)
    if missing_account:
        risk = replace(risk, account=None)
    risk_by_candidate = {selected.candidate_id: risk}

    economic_by_candidate = {}
    expected_sizing_ref = None
    expected_action_ref = None
    expected_action_hash = None
    if account is not None and not missing_account and not any((future_fee, future_stress, future_account)):
        sizing = size_selected_candidate(
            repository,
            candidate_set=candidate_set,
            candidate=selected,
            universe=case.universe,
            policy=S1_POLICY,
            product=case.product,
            v1=case.v1,
            v2=case.v2,
            account=case.account,
            exposures=case.exposures,
            outcomes=case.outcomes,
            venue=case.venue,
            stress=case.stress,
            fee=case.fee,
            cutoff_ns=CUTOFF,
            clock_ns=lambda:CUTOFF+100,
        )
        if sizing.status.value == "SIZED":
            expected_sizing_ref = sizing.content_hash
            action = freeze_action(
                repository,
                candidate=selected,
                candidate_set=candidate_set,
                sizing=sizing,
                product=case.product,
                policy=S1_POLICY,
                v1=case.v1,
                v2=case.v2,
                clock_ns=lambda:CUTOFF+100,
            )
            expected_action_ref = action.content_hash
            expected_action_hash = action.action.action_hash
            if not no_economic_inputs:
                capability = _capability_for_action(action, case, CUTOFF)
                if future_capability:
                    capability = replace(capability, available_at_ns=CUTOFF + 1)
                economic_by_candidate[selected.candidate_id] = production.ProductionEconomicInputsV1(
                    _admission_policy(),
                    capability,
                    _causal_input(repository, "Session027ProductionM0InputV1", CUTOFF),
                    _causal_input(repository, "Session027ProductionCalibrationInputV1", CUTOFF),
                    _causal_input(repository, "Session027ProductionExecutionInputV1", CUTOFF),
                    CUTOFF + 10,
                    27027,
                    100,
                )

    feature_refs = tuple(sorted({item.snapshot_hash for item in case.competitors}))
    inputs = production.ProductionEventInputsV1(
        case.universe,
        case.competitors,
        scanner_refs,
        risk_by_candidate,
        economic_by_candidate,
        feature_refs,
    )
    required_refs = {
        case.universe.content_hash,
        *(item.content_hash for item in case.competitors),
        *(ref for refs in scanner_refs.values() for ref in refs),
        *feature_refs,
    }
    return (
        inputs,
        case,
        candidate_set,
        selected,
        tuple(sorted(required_refs)),
        expected_sizing_ref,
        expected_action_ref,
        expected_action_hash,
    )


def _production_event(repository, **options):
    seed_event = make_event(cutoff_ns=CUTOFF, deadline_delta_ns=10_000_000_000)
    inputs, case, candidate_set, selected, refs, sizing_ref, action_ref, action_hash = _make_fixture_inputs(
        repository, seed_event.event_id, **options
    )
    event = replace(seed_event, causal_input_refs=refs)
    return event, inputs, case, candidate_set, selected, sizing_ref, action_ref, action_hash


def _seed_default_public_evidence(repository: OpsRepository, *, archive_root: Path) -> tuple[int, ProductContractV2]:
    """Persist local final bars and exact source receipts; do not create a candidate."""
    history_count = 2900
    m15 = BarIntervalV2.M15
    cutoff_ns = (history_count + 1) * m15.duration_ns
    metadata = {"fixture_product_metadata": "default-production-composition"}
    metadata_ref = sha256_json(metadata)
    index_research_evidence(repository, "ProductMetadataFixtureV1", metadata_ref, 0, metadata)
    product = ProductContractV2(
        KEY,
        0,
        0,
        0,
        Decimal("1"),
        Decimal("0.01"),
        Decimal("0.1"),
        Decimal("0.1"),
        TradingStatusV2.TRADING,
        metadata_ref,
        min_notional=Decimal("10"),
        max_qty=Decimal("1000"),
    )
    index_risk_evidence(repository, product)
    registry = InstrumentRegistryV2()
    registry.register(product)
    collector = PublicCollectorV2(
        repository=repository,
        registry=registry,
        clock_ns=lambda: cutoff_ns,
        archive=ParquetObservationArchiveV2(archive_root),
    )
    source_id = "fixture-public"

    def persist_bar(interval: BarIntervalV2, index: int, *, close: Decimal, high: Decimal,
                    low: Decimal, volume: Decimal) -> None:
        open_at = index * interval.duration_ns
        close_at = open_at + interval.duration_ns
        payload = {
            "open_at_ns": open_at,
            "open": str(close),
            "high": str(high),
            "low": str(low),
            "close": str(close),
            "volume": str(volume),
            "final": True,
        }
        raw = RawObservationV2.build(
            instrument_revision=KEY.contract_revision,
            source_id=source_id,
            event_type=f"BAR_{interval.value}",
            event_at_ns=close_at,
            published_at_ns=close_at,
            received_at_ns=close_at,
            ingested_at_ns=close_at,
            available_at_ns=close_at,
            translation_version="deterministic-public-fixture-v1",
            sequence=str(open_at),
            payload=payload,
        )
        bar = CausalBarV2(raw, interval, open_at, close_at, close, high, low, close, volume, True)
        collector.ingest(raw, raw_payload=canonical_json(payload), instrument_key=KEY, bar=bar)

    for index in range(history_count):
        if index < history_count - 20:
            close = Decimal("99") if index % 2 else Decimal("101")
            high, low, volume = close + Decimal("0.3"), close - Decimal("0.3"), Decimal("2000")
        else:
            close, high, low, volume = Decimal("100"), Decimal("100.3"), Decimal("99.7"), Decimal("10")
        persist_bar(m15, index, close=close, high=high, low=low, volume=volume)
    persist_bar(
        m15, history_count, close=Decimal("100.6"), high=Decimal("100.7"),
        low=Decimal("99.6"), volume=Decimal("11"),
    )
    for interval in (BarIntervalV2.H1, BarIntervalV2.H4):
        index = cutoff_ns // interval.duration_ns - 1
        persist_bar(
            interval, index, close=Decimal("100"), high=Decimal("100.3"),
            low=Decimal("99.7"), volume=Decimal("2000"),
        )

    ticker_payload = {
        "bid1Price": "99.99",
        "ask1Price": "100.01",
        "markPrice": "100",
        "indexPrice": "100",
    }
    ticker = RawObservationV2.build(
        instrument_revision=KEY.contract_revision,
        source_id=source_id,
        event_type="TICKER_MARK_INDEX_FUNDING_OI",
        event_at_ns=cutoff_ns,
        published_at_ns=cutoff_ns,
        received_at_ns=cutoff_ns,
        ingested_at_ns=cutoff_ns,
        available_at_ns=cutoff_ns,
        translation_version="deterministic-public-fixture-v1",
        payload=ticker_payload,
    )
    collector.ingest(ticker, raw_payload=canonical_json(ticker_payload), instrument_key=KEY)
    collector.flush_archive()
    health = collector.health.latest(source_id)
    assert health is not None and health.data_eligible
    observations = tuple(
        entry for entry in repository.artifact_entries("PublicObservationIndexV2")
        if entry.metadata.get("source_id") == source_id and entry.available_at_ns <= cutoff_ns
    )
    latest_observation_time = max(entry.available_at_ns for entry in observations)
    observation_refs = tuple(sorted(
        entry.artifact_ref for entry in observations if entry.available_at_ns == latest_observation_time
    ))
    assert observation_refs
    prior_epoch = _index_recovery_epoch(repository, started_at_ns=cutoff_ns - 1, epoch_index=1)
    _index_reconnect_reconciliation(
        repository, source_id=source_id, epoch_ref=prior_epoch, at_ns=cutoff_ns,
        observation_refs=observation_refs,
    )
    collector.reconcile_after_reconnect(
        source_id,
        at_ns=cutoff_ns,
        complete_snapshot=True,
        missed_interval_repaired=True,
        snapshot_refs=observation_refs,
    )
    # This fixture exercises a complete selection seam. A missing event gate
    # makes S1 NOT_ESTIMABLE and must not disappear when S2 produces a candidate.
    from atlas.v2.news.events import (
        AbnormalityEvidenceV2,
        AbnormalityStateV2,
        CalendarCoverageV2,
        EventSafetyGateBuilderV2,
    )

    calendar_ref = sha256_json({"fixture_calendar": cutoff_ns})
    abnormality_ref = sha256_json({"fixture_abnormality": cutoff_ns})
    repository.register_artifacts((
        ArtifactIndexEntryV2(calendar_ref, "CalendarSourceFixtureV2", calendar_ref,
                            cutoff_ns, cutoff_ns, {"ref": calendar_ref}),
        ArtifactIndexEntryV2(abnormality_ref, "AbnormalitySourceFixtureV2", abnormality_ref,
                            cutoff_ns, cutoff_ns, {"ref": abnormality_ref}),
    ))
    EventSafetyGateBuilderV2(repository).evaluate(
        key=KEY, cutoff_ns=cutoff_ns,
        coverage=CalendarCoverageV2("SCHEDULE_FIXTURE", cutoff_ns - BarIntervalV2.M15.duration_ns,
            cutoff_ns + BarIntervalV2.H4.duration_ns, cutoff_ns, cutoff_ns, cutoff_ns,
            True, "fixture-r1", calendar_ref, "VERIFIED"),
        scheduled_events=(), abnormality=AbnormalityEvidenceV2(
            AbnormalityStateV2.NORMAL, cutoff_ns, cutoff_ns, abnormality_ref), incidents=(),
    )
    return cutoff_ns, product


def _index_recovery_epoch(
    repository: OpsRepository, *, started_at_ns: int, epoch_index: int, previous_ref: str | None = None,
) -> str:
    body = {
        "version": "OPS_RECOVERY_EPOCH_V1", "epoch_index": epoch_index,
        "started_at_ns": started_at_ns, "previous_epoch_ref": previous_ref, "authority": "ZERO",
    }
    ref = sha256_json(body)
    repository.register_artifact(ArtifactIndexEntryV2(
        ref, "OpsRecoveryEpochV1", ref, started_at_ns, started_at_ns, {"recovery_epoch": body},
    ))
    return ref


def _index_reconnect_reconciliation(
    repository: OpsRepository, *, source_id: str, epoch_ref: str, at_ns: int,
    observation_refs: tuple[str, ...] | None = None,
) -> str:
    if observation_refs is None:
        observations = tuple(
            entry for entry in repository.artifact_entries("PublicObservationIndexV2")
            if entry.metadata.get("source_id") == source_id and entry.available_at_ns <= at_ns
        )
        latest_observation_time = max((entry.available_at_ns for entry in observations), default=None)
        refs = tuple(sorted(
            entry.artifact_ref for entry in observations if entry.available_at_ns == latest_observation_time
        ))
    else:
        refs = observation_refs
    assert refs
    body = {
        "version": "OPS_PUBLIC_SOURCE_RECONCILIATION_V1", "source_id": source_id,
        "available_at_ns": at_ns, "complete_snapshot": True,
        "missed_interval_repaired": True, "evidence_refs": list(refs),
        "recovery_epoch_ref": epoch_ref,
    }
    ref = sha256_json(body)
    repository.register_artifact(ArtifactIndexEntryV2(
        ref, "OpsPublicSourceReconciliationV1", ref, at_ns, at_ns,
        {"reconciliation": body},
    ))
    return ref


def _seed_default_s3_candidate_evidence(
    repository: OpsRepository, *, archive_root: Path, setup_cutoff_ns: int,
    trigger_receipt_delta_ns: int = 0,
) -> tuple[int, int, ProductContractV2, str, CausalBarV2]:
    """Seed public evidence plus a real S3 WATCH produced by the accepted coordinator."""
    import math

    from atlas.v2.data.bybit import translate_recent_trades, translate_ticker
    from atlas.v2.data.history import ImportedObservationV2
    from atlas.v2.news.events import (
        AbnormalityEvidenceV2,
        AbnormalityStateV2,
        CalendarCoverageV2,
        EventSafetyGateBuilderV2,
    )
    from atlas.v2.strategies.s3_mean_reversion import S3ShadowCoordinator

    from .test_session021_data_s3 import _production_s3_setup_inputs

    setup = _production_s3_setup_inputs(
        repository, cutoff_ns=setup_cutoff_ns, innovation_scale=20.0,
    )
    setup_result = S3ShadowCoordinator(repository).evaluate_setup(
        key=KEY,
        cutoff_ns=setup_cutoff_ns,
        residuals=setup["residuals"],
        current_vwap=setup["current_vwap"],
        trades=setup["trades"],
        completed_1m=setup["bars"],
        context=setup["context"],
        feature=setup["feature"],
        quote=setup["quote"],
        tick_size=setup["product"].tick_size,
        universe=setup["universe"],
        event_gate=setup["event_gate"],
        bar_health=setup["bar_health"],
        trade_health=setup["trade_health"],
        trade_completeness_proven=True,
    )
    assert setup_result.status == "WATCH" and setup_result.watch is not None
    event_cutoff_ns = setup_cutoff_ns + BarIntervalV2.M15.duration_ns
    product = setup["product"]
    archived: list[ImportedObservationV2] = []
    observation_entries: list[ArtifactIndexEntryV2] = []

    def persist_observation(observation: RawObservationV2, payload: Mapping[str, Any],
                            bar: CausalBarV2 | None = None) -> None:
        raw_bytes = canonical_json(payload).encode("utf-8")
        index_ref = sha256_json({
            "artifact_type": "PublicObservationIndexV2", "record_id": observation.record_id,
        })
        observation_entries.append(ArtifactIndexEntryV2(
            index_ref, "PublicObservationIndexV2", observation.content_hash,
            observation.received_at_ns, observation.available_at_ns,
            {
                "record_id": observation.record_id,
                "source_id": observation.source_id,
                "event_type": observation.event_type,
                "instrument_revision": observation.instrument_revision,
                "instrument_key_json": KEY.to_canonical_json(),
                "event_at_ns": observation.event_at_ns,
                "published_at_ns": observation.published_at_ns,
                "translation_version": observation.translation_version,
                "revision_of": observation.revision_of,
                "quality_flags": list(observation.quality_flags),
                "availability_class": observation.availability_class.value,
                "replay_available_at_ns": observation.replay_available_at_ns,
                "raw_payload_hash": observation.raw_payload_hash,
                "bar_content_hash": bar.content_hash if bar is not None else None,
            },
        ))
        archived.append(ImportedObservationV2(len(archived) + 1, observation, raw_bytes, bar))

    def ingest_bar(bar: CausalBarV2) -> None:
        persist_observation(bar.raw, {
            "open": str(bar.open), "high": str(bar.high),
            "low": str(bar.low), "close": str(bar.close),
        }, bar)

    m15_context = setup["context"].m15[-1]
    h4_context = setup["context"].h4[-1]
    for bar in setup["bars"]:
        ingest_bar(bar)

    def add_context_bar(interval: BarIntervalV2, close_at_ns: int, ordinal: int) -> CausalBarV2:
        if interval == BarIntervalV2.M15 and close_at_ns == m15_context.close_at_ns:
            ingest_bar(m15_context)
            return m15_context
        if interval == BarIntervalV2.H4 and close_at_ns == h4_context.close_at_ns:
            ingest_bar(h4_context)
            return h4_context
        wave = math.sin(ordinal * (0.011 if interval == BarIntervalV2.M15 else 0.19))
        close = Decimal(str(100 + 0.1 * wave))
        high, low = close + Decimal("0.5"), close - Decimal("0.5")
        open_at_ns = close_at_ns - interval.duration_ns
        payload = {"open": str(close), "high": str(high), "low": str(low), "close": str(close)}
        raw = RawObservationV2.build(
            instrument_revision=KEY.contract_revision, source_id="PUBLIC_BARS",
            event_type=f"BAR_{interval.value}", event_at_ns=close_at_ns,
            received_at_ns=close_at_ns, ingested_at_ns=close_at_ns, available_at_ns=close_at_ns,
            translation_version="session027-s3-public-bars-v1", sequence=str(open_at_ns), payload=payload,
        )
        bar = CausalBarV2(
            raw, interval, open_at_ns, close_at_ns, close, high, low, close, Decimal("1"), True,
        )
        ingest_bar(bar)
        return bar

    m15_start = setup_cutoff_ns - 30 * 24 * 60 * 60 * 1_000_000_000 + BarIntervalV2.M15.duration_ns
    for ordinal, close_at_ns in enumerate(range(m15_start, setup_cutoff_ns, BarIntervalV2.M15.duration_ns)):
        add_context_bar(BarIntervalV2.M15, close_at_ns, ordinal)
    add_context_bar(BarIntervalV2.M15, setup_cutoff_ns, 30 * 24 * 4 - 1)
    add_context_bar(BarIntervalV2.M15, event_cutoff_ns, 30 * 24 * 4)
    h1_start = setup_cutoff_ns - 30 * 24 * 60 * 60 * 1_000_000_000 + BarIntervalV2.H1.duration_ns
    for ordinal, close_at_ns in enumerate(range(h1_start, setup_cutoff_ns + 1, BarIntervalV2.H1.duration_ns)):
        add_context_bar(BarIntervalV2.H1, close_at_ns, ordinal)
    h4_start = setup_cutoff_ns - 30 * 24 * 60 * 60 * 1_000_000_000 + BarIntervalV2.H4.duration_ns
    for ordinal, close_at_ns in enumerate(range(h4_start, setup_cutoff_ns, BarIntervalV2.H4.duration_ns)):
        add_context_bar(BarIntervalV2.H4, close_at_ns, ordinal)

    trigger_bar: CausalBarV2 | None = None
    for index in range(1, 16):
        close_at_ns = setup_cutoff_ns + index * BarIntervalV2.M1.duration_ns
        received_at_ns = close_at_ns + (trigger_receipt_delta_ns if index == 15 else 0)
        close = Decimal("100.001")
        high, low = close + Decimal("0.001"), close - Decimal("0.001")
        open_at_ns = close_at_ns - BarIntervalV2.M1.duration_ns
        payload = {"open": str(close), "high": str(high), "low": str(low), "close": str(close)}
        raw = RawObservationV2.build(
            instrument_revision=KEY.contract_revision, source_id="PUBLIC_BARS",
            event_type="BAR_1M", event_at_ns=close_at_ns,
            received_at_ns=received_at_ns, ingested_at_ns=received_at_ns,
            available_at_ns=received_at_ns,
            translation_version="session027-s3-public-bars-v1", sequence=str(open_at_ns), payload=payload,
        )
        minute_bar = CausalBarV2(
            raw, BarIntervalV2.M1, open_at_ns, close_at_ns, close, high, low, close, Decimal("1"), True,
        )
        ingest_bar(minute_bar)
        if index == 15:
            trigger_bar = minute_bar

    trade_row = {
        "symbol": KEY.native_symbol, "execId": "session027-s3-current-trade",
        "p": "100", "v": "1", "time": event_cutoff_ns // 1_000_000, "S": "Buy",
    }
    trade_raw, = translate_recent_trades(
        (trade_row,), key=KEY, received_at_ns=event_cutoff_ns, source_id="PUBLIC_TRADES",
    )
    persist_observation(trade_raw, trade_row)
    quote_row = {
        "symbol": KEY.native_symbol, "ts": event_cutoff_ns // 1_000_000,
        "bid1Price": "100.001", "ask1Price": "100.002", "markPrice": "100", "indexPrice": "100",
    }
    quote_raw = translate_ticker(
        quote_row, key=KEY, received_at_ns=event_cutoff_ns, source_id="PUBLIC_QUOTE",
    )
    persist_observation(quote_raw, quote_row)
    chunk_id = sha256_json({
        "fixture": "SESSION027_DEFAULT_S3_PUBLIC_ARCHIVE_V1",
        "record_ids": [item.observation.record_id for item in archived],
    })
    ParquetObservationArchiveV2(archive_root).write_observation_chunk(chunk_id, archived)
    observation_entries = [
        replace(entry, metadata={**dict(entry.metadata), "archive_chunk_id": chunk_id})
        for entry in observation_entries
    ]
    repository.register_artifacts(tuple(observation_entries))
    for source_id in ("PUBLIC_BARS", "PUBLIC_TRADES", "PUBLIC_QUOTE"):
        health = PublicSourceHealthV2(
            source_id, event_cutoff_ns, event_cutoff_ns, PublicSourceStateV2.HEALTHY_CURRENT,
            sha256_json({"session027-s3-current-health": source_id, "cutoff": event_cutoff_ns}),
            "deterministic local S3 composition fixture",
        )
        repository.register_artifact(ArtifactIndexEntryV2(
            health.content_hash, "PublicSourceHealthV2", health.content_hash,
            event_cutoff_ns, event_cutoff_ns, {"health": health.to_dict()},
        ))
        repository.record_source_health(health.to_ops_record())

    calendar_ref = sha256_json({"session027-s3-calendar": event_cutoff_ns})
    abnormality_ref = sha256_json({"session027-s3-abnormality": event_cutoff_ns})
    repository.register_artifacts((
        ArtifactIndexEntryV2(calendar_ref, "CalendarSourceFixtureV2", calendar_ref,
                             event_cutoff_ns, event_cutoff_ns, {"ref": calendar_ref}),
        ArtifactIndexEntryV2(abnormality_ref, "AbnormalitySourceFixtureV2", abnormality_ref,
                             event_cutoff_ns, event_cutoff_ns, {"ref": abnormality_ref}),
    ))
    coverage = CalendarCoverageV2(
        "SCHEDULE_FIXTURE", event_cutoff_ns - BarIntervalV2.M15.duration_ns,
        event_cutoff_ns + BarIntervalV2.H4.duration_ns, event_cutoff_ns, event_cutoff_ns,
        event_cutoff_ns, True, "schedule-r1", calendar_ref, "VERIFIED",
    )
    abnormality = AbnormalityEvidenceV2(
        AbnormalityStateV2.NORMAL, event_cutoff_ns, event_cutoff_ns, abnormality_ref,
    )
    EventSafetyGateBuilderV2(repository).evaluate(
        key=KEY, cutoff_ns=event_cutoff_ns, coverage=coverage, scheduled_events=(),
        abnormality=abnormality, incidents=(),
    )
    assert trigger_bar is not None
    return setup_cutoff_ns, event_cutoff_ns, product, setup_result.watch.watch_id, trigger_bar


def _seed_default_risk_and_economic_evidence(
    repository: OpsRepository, *, event: OpsDecisionEventV1, candidate: CandidateActionV2,
    product: ProductContractV2, candidate_set: CandidateSetV2,
) -> None:
    cutoff = event.information_cutoff_ns
    source_refs: dict[str, str] = {}
    for label in ("risk-completeness", "risk-venue-sizing", "risk-stress", "risk-fee"):
        body = {"deterministic_typed_evidence_source": label, "cutoff_ns": cutoff}
        ref = sha256_json(body)
        index_research_evidence(repository, "ProductionTypedEvidenceSourceV1", ref, cutoff, body)
        source_refs[label] = ref

    v1 = engineering_default_policy(policy_version="OPS_DEFAULT_PATH_TEST_V1", policy_effective_at_ns=0)
    v2 = RiskPolicyV2(
        "OPS_DEFAULT_PATH_TEST_V1", 0, v1.policy_hash(), Decimal("0.05"), Decimal("0.02"), 1,
    )
    index_risk_policies(repository, v1, v2)
    account = AccountRiskSnapshotV2(
        "DEFAULT_PATH_RESEARCH_ACCOUNT",
        cutoff,
        Decimal("100000"),
        Decimal("100000"),
        Decimal("0"),
        Decimal("0"),
        Decimal("0"),
        Decimal("0"),
        Decimal("0"),
        Decimal("0"),
        Decimal("0"),
        Decimal("0"),
        Decimal("0"),
        0,
        (),
        (),
        (),
        source_refs["risk-completeness"],
    )
    venue_sizing = VenueSizingLimitsV2(
        candidate.key, product.content_hash, cutoff, (Decimal("1"), Decimal("2")),
        Decimal("2"), Decimal("0"), source_refs["risk-venue-sizing"],
    )
    stress_price = Decimal("90") if candidate.side.value == "LONG" else Decimal("110")
    stress = StressBoundV2(candidate.key, cutoff, stress_price, source_refs["risk-stress"])
    fee = FeeScheduleV2(candidate.key, cutoff, Decimal("0.001"), Decimal("0.001"), source_refs["risk-fee"])
    for item in (account, venue_sizing, stress, fee):
        if isinstance(item, FeeScheduleV2):
            index_cost_evidence(repository, item)
        else:
            index_risk_evidence(repository, item)

    admission = _admission_policy()
    production.index_ops_admission_policy_evidence(
        repository,
        admission_policy=admission,
        available_at_ns=cutoff,
        event_id=event.event_id,
        candidate_set_ref=candidate_set.content_hash,
        candidate_ref=candidate.content_hash,
        product_ref=product.content_hash,
    )
    capability = VenueCapabilitySnapshotV2(
        candidate.key.venue,
        candidate.key.environment,
        account.account_scope,
        product.content_hash,
        candidate.key.content_hash,
        admission.required_margin_mode,
        admission.required_position_mode,
        admission.required_nautilus_distribution,
        admission.required_nautilus_version,
        admission.required_nautilus_source_commit,
        admission.required_nautilus_artifact_ref,
        admission.required_execution_profile_ref,
        admission.required_protection_profile_ref,
        admission.required_qualification_version,
        VenueCapabilityStatusV2.UNVERIFIED,
        (),
        cutoff,
    )
    index_venue_capability_snapshot(repository, capability)
    roles = (
        ("M0", "Session027DefaultPathM0InputV1"),
        ("CALIBRATION", "Session027DefaultPathCalibrationInputV1"),
        ("EXECUTION_MODEL", "Session027DefaultPathExecutionInputV1"),
    )
    for role, kind in roles:
        causal = _causal_input(repository, kind, cutoff)
        production.index_ops_causal_input_evidence(
            repository,
            role=role,
            causal_input=causal,
            available_at_ns=cutoff,
            event_id=event.event_id,
            candidate_set_ref=candidate_set.content_hash,
            candidate_ref=candidate.content_hash,
            product_ref=product.content_hash,
        )
    production.index_ops_economic_scenario_config(
        repository,
        scenario_seed=27027,
        scenario_count=32,
        evaluation_available_at_ns=cutoff + 10,
        available_at_ns=cutoff,
        event_id=event.event_id,
        candidate_set_ref=candidate_set.content_hash,
        candidate_ref=candidate.content_hash,
        product_ref=product.content_hash,
    )


def _run_with_production_port(path, event, inputs, clock, *, port=None):
    source = ReconciledFixturePublicSource(event)
    adapter = port or production.ProductionOpsCyclePortV1(
        public_source=source,
        inputs_provider=StaticInputsProvider(event.event_id, inputs),
    )
    return adapter, source, OpsSupervisorV2(path, adapter, clock_ns=clock)


def test_builtin_atlas_ops_cli_imports_and_runs_without_external_adapter(tmp_path, capsys):
    assert production.OPS_PRODUCTION_ADAPTER_ID == "ATLAS_V2_PRODUCTION_OPS_COMPOSITION_V1"
    assert production.create_production_port().__class__ is production.ProductionOpsCyclePortV1
    from atlas.v2.runtime.ops_supervisor import main

    assert main(["--db", str(tmp_path / "ops.sqlite"), "--once"]) == 0
    output = capsys.readouterr().out
    assert '"recovered":true' in output


def test_default_production_composes_public_to_risk_action_and_economics_after_restart(tmp_path, monkeypatch):
    path = tmp_path / "ops.sqlite"
    archive_root = tmp_path / "ops-observations"
    with OpsRepository(path) as repository:
        cutoff_ns, product = _seed_default_public_evidence(repository, archive_root=archive_root)
        assert repository.artifact_entries("CandidateActionV2") == ()
        # The live product warms an exact retained prefix in bounded pages.
        # This decision/restart fixture starts with that prior maintenance
        # complete, preserving its original full-source indicator seeds.
        from atlas.v2.runtime.active_history import maintain_history
        for interval in (BarIntervalV2.M15,BarIntervalV2.H1,BarIntervalV2.H4):
            ready = False
            for step in range(32):
                warm_cutoff = cutoff_ns - 10_000 + step*100
                page = maintain_history(repository,archive_root,key=product.key,interval=interval,
                    cutoff_ns=warm_cutoff,clock_ns=lambda at=warm_cutoff:at+1,deadline_ns=warm_cutoff+10)
                if page.ready:
                    ready = True
                    break
            assert ready
        for _ in range(24):
            discovered = repository.m15_origin_observation_page(product.key,
                available_from_ns=0,available_through_ns=cutoff_ns+1,limit=4)
            if discovered.entries:
                break
        assert discovered.entries

    def forbid_network(*args, **kwargs):
        raise AssertionError("default production composition attempted a network request")

    monkeypatch.setattr(socket, "create_connection", forbid_network)
    monkeypatch.setattr(urllib.request, "urlopen", forbid_network)

    coordinator_calls = []
    coordinator_methods = (
        (production.S1ShadowCoordinator, "create_watch", "S1"),
        (production.S1ShadowCoordinator, "on_bar", "S1"),
        (production.S2ShadowCoordinator, "on_trigger_close", "S2"),
        (production.S3ShadowCoordinator, "evaluate_setup", "S3"),
    )
    for coordinator, method_name, label in coordinator_methods:
        original = getattr(coordinator, method_name)

        def counted(self, *args, _original=original, _label=label, **kwargs):
            coordinator_calls.append(_label)
            return _original(self, *args, **kwargs)

        monkeypatch.setattr(coordinator, method_name, counted)

    clock = FakeClock(cutoff_ns + 1)
    port = production.create_production_port()
    assert type(port.public_source) is production.IndexedPublicCycleSourceV1
    assert type(port.inputs_provider) is production.IndexedProductionEventInputsV1
    injected = []

    def crash_after_selection(stage):
        if stage == PipelineStageV1.SELECTION and not injected:
            injected.append(stage)
            raise RuntimeError("deterministic composition restart boundary")

    port.crash_after_checkpoint = crash_after_selection
    with OpsSupervisorV2(path, port, clock_ns=clock) as supervisor:
        waiting = supervisor.run_once()
        assert not waiting.event_receipts
        assert not waiting.cycle.event_ids
        assert port._collector_recovery is not None
        assert port._collector_recovery.collector.health.latest("fixture-public").state == (
            PublicSourceStateV2.INCOMPLETE_SNAPSHOT
        )
        prior_reconciliations = supervisor.repository.artifact_entries("OpsPublicSourceReconciliationV1")
        assert any(
            item.metadata["reconciliation"].get("recovery_epoch_ref") != port._collector_recovery.recovery_epoch_ref
            for item in prior_reconciliations
        )
        clock.now_ns += 1
        assert supervisor.repository is not None
        _index_reconnect_reconciliation(
            supervisor.repository, source_id="fixture-public",
            epoch_ref=port._collector_recovery.recovery_epoch_ref, at_ns=clock.now_ns,
        )
        interrupted = supervisor.run_once()
        assert not interrupted.event_receipts
        assert interrupted.cycle.event_ids
        assert interrupted.cycle.failure_types == ("RuntimeError",)
        assert supervisor.repository is not None
        repository = supervisor.repository
        assert port._collector_recovery is not None
        assert port._collector_recovery.collector.repository is repository
        assert not repository.read_only
        assert interrupted.cycle.source_health_state == "HEALTHY_CURRENT"
        event_entry, = repository.artifact_entries("OpsDecisionEventSourceV1")
        event = production.decision_event_from_dict(event_entry.metadata["event"])
        assert event.information_cutoff_ns == cutoff_ns
        assert event.source_event_at_ns == cutoff_ns
        assert event.available_at_ns == cutoff_ns
        assert event.deadline_ns > interrupted.cycle.started_at_ns
        assert "OpsPublicSourceReconciliationV1" in {
            entry.artifact_type for entry in repository.artifact_entries("OpsPublicSourceReconciliationV1")
        }
        feature_entries = repository.artifact_entries("FeatureArtifactV2")
        assert feature_entries
        from atlas.v2.chronology import causal_artifact
        assert all(event.information_cutoff_ns < entry.available_at_ns <= event.deadline_ns
                   for entry in feature_entries)
        assert all(causal_artifact(repository, entry.artifact_ref,
                   cutoff_ns=event.information_cutoff_ns, consumer_at_ns=event.deadline_ns,
                   deadline_ns=event.deadline_ns) for entry in feature_entries)
        candidate_set_checkpoint = repository.get_artifact(
            OpsSupervisorV2._checkpoint_ref(event.event_id, PipelineStageV1.CANDIDATE_SET)
        )
        selection_checkpoint = repository.get_artifact(
            OpsSupervisorV2._checkpoint_ref(event.event_id, PipelineStageV1.SELECTION)
        )
        assert candidate_set_checkpoint is not None and selection_checkpoint is not None
        candidate_set_ref = candidate_set_checkpoint.metadata["stage_result"]["artifact_refs"][0]
        candidate_set_entry = repository.get_artifact(candidate_set_ref)
        assert candidate_set_entry is not None
        candidate_set = CandidateSetV2.from_dict(json_value(candidate_set_entry.metadata["candidate_set"]))
        # The fixture intentionally has only one warm feature snapshot. The
        # newly explicit S1 warmup missingness must block selection instead of
        # silently allowing S2 to win against an unobservable competitor.
        if candidate_set.selected_candidate_id is None:
            assert candidate_set.selection_status.value == "NOT_ESTIMABLE"
            generation = repository.artifact_entries("OpsCandidateGenerationEvidenceV1")
            assert generation and "S1:EMA_WARMUP_MISSING" in generation[0].metadata["generation"]["missing_reasons"]
            assert repository.artifact_entries("ActionArtifactV2") == ()
            return
        assert candidate_set.selected_candidate_id is not None
        assert candidate_set.candidates
        candidate_ref = next(iter(candidate_set_entry.metadata["identity"]["candidate_refs"]))
        candidate_entry = repository.get_artifact(candidate_ref)
        assert candidate_entry is not None
        candidate = CandidateActionV2.from_dict(json_value(candidate_entry.metadata["candidate"]))
        assert candidate.candidate_id == candidate_set.selected_candidate_id
        assert candidate.policy_hash == S2_POLICY.policy_hash
        assert candidate.quantity is None
        assert any(
            entry.artifact_type == "FeatureArtifactV2" and entry.artifact_ref == candidate.snapshot_hash
            for entry in feature_entries
        )
        # S3 has moved to its native one-minute event. The M15 decision must
        # continue to invoke only the unchanged S1/S2 production sleeves.
        assert {"S1", "S2"}.issubset(set(coordinator_calls))
        assert "S3" not in coordinator_calls
        indexed_candidates = tuple(repository.artifact_entries("CandidateActionV2"))
        assert {entry.artifact_ref for entry in indexed_candidates} == set(
            candidate_set_entry.metadata["identity"]["candidate_refs"]
        )
        s3_states = [entry for entry in repository.artifact_entries("S3MeanReversionStateV2")
                     if entry.available_at_ns <= event.information_cutoff_ns]
        assert not s3_states
        assert not any(
            CandidateActionV2.from_dict(json_value(entry.metadata["candidate"])).policy_hash == S3_POLICY.policy_hash
            for entry in repository.artifact_entries("CandidateActionV2")
        )
        assert repository.artifact_entries("OpsSupervisorReceiptIdentityV1") == ()

    # The exact typed evidence is indexed only after the production coordinator
    # has generated and selected the candidate.
    with OpsRepository(path) as repository:
        _seed_default_risk_and_economic_evidence(
            repository, event=event, candidate=candidate, product=product, candidate_set=candidate_set,
        )
        assert repository.get_artifact(candidate.content_hash) is not None

    port.crash_after_checkpoint = None
    clock.now_ns += 1
    with OpsSupervisorV2(path, port, clock_ns=clock) as restarted:
        waiting = restarted.run_once()
        assert not waiting.event_receipts
        assert port._collector_recovery is not None
        assert port._collector_recovery.collector.health.latest("fixture-public").state == (
            PublicSourceStateV2.INCOMPLETE_SNAPSHOT
        )
        clock.now_ns += 10
        assert restarted.repository is not None
        _index_reconnect_reconciliation(
            restarted.repository, source_id="fixture-public",
            epoch_ref=port._collector_recovery.recovery_epoch_ref, at_ns=clock.now_ns,
        )
        completed = restarted.run_once()
        assert len(completed.event_receipts) == 1
        receipt = completed.event_receipts[0]
        assert receipt.event.event_id == event.event_id
        assert receipt.candidate_set_ref == candidate_set.content_hash
        assert receipt.result.terminal_status == OpsTerminalStatusV1.NOT_ESTIMABLE
        assert receipt.agent_mode == "DISABLED"
        assert not receipt.capital_enabled and not receipt.assisted_enabled
        assert receipt.sizing_ref and receipt.action_ref and receipt.evaluation_ref and receipt.calendar_refs
        assert receipt.to_dict()["action_hash"]
        repository = restarted.repository
        assert repository is not None
        assert len(repository.artifact_entries("CandidateActionV2")) == 1
        assert len(repository.artifact_entries("CandidateSetV2")) == 1
        assert len(repository.artifact_entries("SizingDecisionV2")) == 1
        assert len(repository.artifact_entries("ActionArtifactV2")) == 1
        assert len(repository.artifact_entries("EvaluationArtifactV2")) == 1
        assert len(repository.artifact_entries("DecisionCalendarEntryV2")) == 1
        assert len(repository.artifact_entries("OpsRiskEvidenceResolutionV1")) == 1
        assert len(repository.artifact_entries("OpsEconomicEvidenceResolutionV1")) == 1
        sizing = repository.get_artifact(receipt.sizing_ref)
        action = repository.get_artifact(receipt.action_ref)
        evaluation = repository.get_artifact(receipt.evaluation_ref)
        assert sizing is not None and action is not None and evaluation is not None
        sizing_body = sizing.metadata["sizing"]
        action_identity = action.metadata["action_identity"]
        assert sizing_body["status"] == "SIZED"
        assert sizing_body["quantity"] is not None
        assert action_identity["quantity"] == sizing_body["quantity"]
        assert action.metadata["action_artifact"]["action_hash"] == receipt.to_dict()["action_hash"]
        assert evaluation.metadata["evaluation"]["decision"] == "NOT_ESTIMABLE"
        calendar = repository.get_artifact(receipt.calendar_refs[0])
        assert calendar is not None and calendar.artifact_type == "DecisionCalendarEntryV2"
        assert calendar.metadata["decision_entry"]["source_stage"] == "ECONOMIC_EVALUATION"
        assert calendar.metadata["decision_entry"]["action_artifact_ref"] == receipt.action_ref
        assert repository.artifact_entries("TradePlanEnvelopeV2") == ()
        assert repository.artifact_entries("OrderIntentV2") == ()
        assert repository.artifact_entries("Approval") == ()
        assert repository.artifact_entries("ReservationSnapshotV2") == ()
        for stage in (PipelineStageV1.M1_DIAGNOSTIC, PipelineStageV1.ANALOGUE_DIAGNOSTIC):
            stage_result = next(item for item in receipt.result.stages if item.stage == stage)
            assert stage_result.bound_action_hash == receipt.to_dict()["action_hash"]
            assert stage_result.authority == "ZERO"
            diagnostic_entry = repository.get_artifact(stage_result.artifact_refs[0])
            assert diagnostic_entry is not None
            if diagnostic_entry.artifact_type == "OpsZeroAuthorityDiagnosticV1":
                assert diagnostic_entry.metadata["diagnostic"]["action_hash"] == receipt.to_dict()["action_hash"]
                assert diagnostic_entry.metadata["diagnostic"]["authority"] == "ZERO"
            elif diagnostic_entry.artifact_type == "AnalogueActionValueV2":
                assert diagnostic_entry.metadata["analogue"]["query_action_hash"] == receipt.to_dict()["action_hash"]
            else:
                assert diagnostic_entry.metadata["prediction"]["action_hash"] == receipt.to_dict()["action_hash"]
        before_restart = {
            artifact_type: tuple(entry.artifact_ref for entry in repository.artifact_entries(artifact_type))
            for artifact_type in (
                "OpsDecisionEventSourceV1", "OpsSupervisorReceiptIdentityV1", "CandidateSetV2",
                "SizingDecisionV2", "ActionArtifactV2", "EvaluationArtifactV2", "DecisionCalendarEntryV2",
            )
        }

    clock.now_ns += 1
    with OpsSupervisorV2(path, production.create_production_port(), clock_ns=clock) as replayed:
        replayed.run_once()
        assert replayed.repository is not None
        after_restart = {
            artifact_type: tuple(entry.artifact_ref for entry in replayed.repository.artifact_entries(artifact_type))
            for artifact_type in before_restart
        }
    assert after_restart == before_restart


def test_default_production_native_m1_s3_stays_closed_without_trade_completeness(tmp_path, monkeypatch):
    from atlas.v2.data.health import PublicSourceStateV2

    path = tmp_path / "s3-default.sqlite"
    archive_root = tmp_path / "ops-observations"
    setup_cutoff = 40 * 24 * 60 * 60 * 1_000_000_000 + 12 * 60 * 60 * 1_000_000_000
    with OpsRepository(path) as repository:
        _setup_cutoff, event_cutoff, product, watch_id, trigger_bar = _seed_default_s3_candidate_evidence(
            repository, archive_root=archive_root, setup_cutoff_ns=setup_cutoff,
            trigger_receipt_delta_ns=250_000_000,
        )
        watch = repository.get_watch(watch_id)
        assert watch is not None
        frozen_vwap_refs = tuple(
            entry.artifact_ref for entry in repository.artifact_entries("S3TradeVwapSnapshotV2")
            if entry.artifact_ref in watch.evidence_refs
        )
        assert len(frozen_vwap_refs) == 1
        assert repository.artifact_entries("CandidateActionV2") == ()
        event = production._native_s3_m1_public_bar_event(
            repository, product, trigger_bar, now_ns=trigger_bar.raw.available_at_ns,
        )
        assert event is not None
        assert event.source_event_at_ns < event.information_cutoff_ns
        from atlas.v2.runtime.s3_native_cadence import s3_m1_event_id

        observation_ref = sha256_json({
            "artifact_type": "PublicObservationIndexV2", "record_id": trigger_bar.raw.record_id,
        })
        source_health_entries = [
            entry for entry in repository.artifact_entries("PublicSourceHealthV2")
            if entry.metadata.get("health", {}).get("source_id") == trigger_bar.raw.source_id
            and entry.available_at_ns <= trigger_bar.raw.available_at_ns
        ]
        source_health_ref = max(
            source_health_entries, key=lambda item: (item.available_at_ns, item.artifact_ref),
        ).artifact_ref
        assert event.event_type == "CONFIRMED_1M_CLOSE"
        assert event.event_id == s3_m1_event_id(product.key, trigger_bar.close_at_ns)
        assert event.information_cutoff_ns == trigger_bar.raw.available_at_ns
        assert event.deadline_ns == trigger_bar.close_at_ns + 5_000_000_000
        assert {observation_ref, trigger_bar.content_hash, product.content_hash} <= set(event.causal_input_refs)
        assert source_health_ref in event.causal_input_refs

        # Add cutoff-visible sequence-book evidence plus later trade, BBO and
        # bar-health records. The computation may finish after T1, but none of
        # these T2 records may enter the fixed-origin result.
        from .test_session034_s3_forward_evidence import _report_context, _ws_trade

        source_cutoff = event.information_cutoff_ns
        computation_start = source_cutoff + 100
        computation_finish = source_cutoff + 200
        persisted_at = source_cutoff + 300
        assert persisted_at <= event.deadline_ns
        book_channel = f"orderbook.50.{product.key.native_symbol}"
        book_received = source_cutoff - 150_000_000
        raw_book_ref = sha256_json({"session034-advancing-clock-book": book_received})
        frame_body = {
            "record_id": sha256_json({"session034-frame": raw_book_ref}),
            "instrument": product.key.to_dict(),
            "instrument_hash": product.key.content_hash,
            "source_id": production.BYBIT_PUBLIC_WS_SOURCE_ID_V1,
            "channel": book_channel,
            "frame_type": "SNAPSHOT",
            "event_at_ns": book_received,
            "received_at_ns": book_received,
            "available_at_ns": book_received,
            "raw_payload_hash": raw_book_ref,
            "archive_chunk_id": sha256_json({"session034-frame-chunk": raw_book_ref}),
            "sequence_semantics": "BYBIT_U",
            "authority": "ZERO",
        }
        frame_ref = sha256_json({
            "artifact_type": "PublicStreamFrameIndexV1", "record_id": frame_body["record_id"],
        })
        repository.register_artifact(ArtifactIndexEntryV2(
            frame_ref, "PublicStreamFrameIndexV1", sha256_json(frame_body),
            book_received, book_received, frame_body,
        ))
        book_report_ref, book_health, _book_state = _report_context(
            repository, product, channel=book_channel, as_of_ns=source_cutoff,
            latest_valid_bbo={
                "bid_price": "100", "ask_price": "101", "received_at_ns": book_received,
                "data_age_ns": source_cutoff - book_received, "input_refs": [raw_book_ref],
            },
            book_sequence_valid=True, observed_trade_count=0,
        )
        trade_channel = f"publicTrade.{product.key.native_symbol}"
        trade_report_ref, trade_health, _trade_state = _report_context(
            repository, product, channel=trade_channel, as_of_ns=source_cutoff,
            observed_trade_count=0,
        )
        later_trade_report_ref, later_trade_health, _later_trade_state = _report_context(
            repository, product, channel=trade_channel, as_of_ns=computation_start + 1,
            observed_trade_count=1, last_trade_receipt_at_ns=computation_start + 1,
        )
        future_trade_ref = _ws_trade(
            repository, archive_root, product, cutoff_ns=computation_start + 1,
            event_at_ns=computation_start + 1, available_at_ns=computation_start + 1,
            received_at_ns=computation_start + 1,
        )
        later_book_report_ref, _later_book_health, _later_book_state = _report_context(
            repository, product, channel=book_channel, as_of_ns=computation_start + 2,
            latest_valid_bbo={
                "bid_price": "90", "ask_price": "91", "received_at_ns": computation_start + 1,
                "data_age_ns": 1, "input_refs": [sha256_json("future-bbo-frame")],
            },
            book_sequence_valid=True, observed_trade_count=0,
        )
        later_bar_health = PublicSourceHealthV2(
            trigger_bar.raw.source_id, computation_start + 3, computation_start + 3,
            PublicSourceStateV2.DISCONNECTED,
            sha256_json({"later-bar-health": computation_start + 3}),
            "after the immutable M1 evidence cutoff",
        )
        repository.register_artifact(ArtifactIndexEntryV2(
            later_bar_health.content_hash, "PublicSourceHealthV2", later_bar_health.content_hash,
            later_bar_health.available_at_ns, later_bar_health.available_at_ns,
            {"health": later_bar_health.to_dict()},
        ))

    def forbid_network(*args, **kwargs):
        raise AssertionError("default S3 composition attempted a network request")

    monkeypatch.setattr(socket, "create_connection", forbid_network)
    monkeypatch.setattr(urllib.request, "urlopen", forbid_network)
    def forbid_s1_s2(*args, **kwargs):
        raise AssertionError("native M1 event invoked an S1/S2 decision sleeve")

    monkeypatch.setattr(production.S1ShadowCoordinator, "create_watch", forbid_s1_s2)
    monkeypatch.setattr(production.S1ShadowCoordinator, "on_bar", forbid_s1_s2)
    monkeypatch.setattr(production.S2ShadowCoordinator, "on_trigger_close", forbid_s1_s2)

    class AdvancingClock:
        def __init__(self, values):
            self._values = iter(values)

        def __call__(self):
            return next(self._values)

    # Prerequisite computation/publication now has its own actual clock samples.
    clock = AdvancingClock((computation_start, computation_start, computation_start,
                            computation_start, computation_start, computation_finish, persisted_at))
    port = production.ProductionOpsCyclePortV1(clock_ns=clock)
    assert type(port.public_source) is production.IndexedPublicCycleSourceV1
    assert type(port.inputs_provider) is production.IndexedProductionEventInputsV1
    with OpsRepository(path) as repository:
        resolved = port.inputs_provider.resolve(repository, event)
        assert resolved is not None
        assert resolved.universe is None
        assert resolved.candidates == ()
        readiness = repository.artifact_entries("S3NativeWarmupReadinessV1")
        assert len(readiness) == 1
        report = readiness[-1].metadata["readiness"]
        assert report["required_m1_bars"] == 10_081
        assert report["trade_completeness_proven"] is False
        assert report["status"] == "NOT_ESTIMABLE"
        assert report["gate_status"] == "TEST GATE"
        assert report["trade_evidence_status"] == "NOT_ESTIMABLE"
        assert "TEST_GATE_BYBIT_TRADE_COMPLETENESS_UNPROVEN" in report["reason_codes"]
        assert tuple(report["trade_evidence_reason_codes"]) == (
            "NO_EXACT_CUTOFF_AVAILABLE_WS_TRADES",
        )
        trade_entries = repository.artifact_entries("S3ForwardTradeEvidenceV1")
        quote_entries = repository.artifact_entries("S3SequenceBookQuoteEvidenceV1")
        assert len(trade_entries) == len(quote_entries) == 1
        trade_entry = trade_entries[0]
        quote_entry = quote_entries[0]
        assert trade_entry.available_at_ns == persisted_at > event.information_cutoff_ns
        assert quote_entry.available_at_ns == persisted_at > event.information_cutoff_ns
        assert report["cutoff_ns"] == event.information_cutoff_ns
        assert dict(report["computation_context"]) == {
            "schema_version": 1,
            "evidence_cutoff_ns": event.information_cutoff_ns,
            "computation_started_ns": computation_start,
            "computation_finished_ns": computation_finish,
            "produced_at_ns": computation_finish,
            "consumer_deadline_ns": event.deadline_ns,
        }
        assert trade_entry.metadata["evidence"]["continuity_report_as_of_ns"] == event.information_cutoff_ns
        assert trade_entry.metadata["evidence"]["source_health_available_at_ns"] == trade_health.available_at_ns
        assert trade_entry.metadata["evidence"]["trade_refs"] == ()
        assert future_trade_ref not in trade_entry.metadata["evidence"]["trade_refs"]
        assert trade_entry.metadata["evidence"]["continuity_report_ref"] == trade_report_ref
        assert later_trade_report_ref != trade_report_ref
        assert later_trade_health.available_at_ns > event.information_cutoff_ns
        bridge = quote_entry.metadata["bridge"]
        assert bridge["continuity_report_ref"] == book_report_ref
        assert bridge["continuity_report_as_of_ns"] == event.information_cutoff_ns
        assert bridge["source_health_ref"] == book_health.content_hash
        assert bridge["observed_at_ns"] == book_received < event.information_cutoff_ns
        assert later_book_report_ref != book_report_ref
        assert later_bar_health.content_hash not in report["evidence_refs"]
        assert all(
            (entry := repository.get_artifact(ref)) is not None
            and entry.available_at_ns <= event.information_cutoff_ns
            for ref in event.causal_input_refs
        )
        timed_refs = tuple(resolved.causal_source_refs)
        assert timed_refs
        assert all(
            (entry := repository.get_artifact(ref)) is not None
            and entry.available_at_ns == persisted_at
            for ref in timed_refs
        )
        before_refs = tuple(entry.artifact_ref for entry in readiness + trade_entries + quote_entries)
        retry_resolved = production.IndexedProductionEventInputsV1(
            clock_ns=lambda: event.deadline_ns,
        ).resolve(repository, event)
        assert retry_resolved is not None
        after_refs = tuple(
            entry.artifact_ref
            for artifact_type in (
                "S3NativeWarmupReadinessV1", "S3ForwardTradeEvidenceV1",
                "S3SequenceBookQuoteEvidenceV1",
            )
            for entry in repository.artifact_entries(artifact_type)
        )
        assert set(after_refs) == set(before_refs)
        rebased_event = __import__("dataclasses").replace(
            event, information_cutoff_ns=event.information_cutoff_ns + 1,
        )
        with pytest.raises(ValueError, match="conflicts with the fixed event"):
            production.IndexedProductionEventInputsV1(
                clock_ns=lambda: event.deadline_ns,
            ).resolve(repository, rebased_event)
        assert product.content_hash in {
            entry.artifact_ref for entry in repository.artifact_entries("ProductContractV2")
        }


def test_decision_calendar_v2_v1_wire_values_round_trip_unchanged():
    assert [item.value for item in DecisionSourceStageV2] == [
        "CANDIDATE_SET", "HARD_RISK", "ECONOMIC_EVALUATION", "EXPIRY",
    ]
    row = DecisionCalendarEntryV2(
        sha256_json({"candidate_set": "accepted-wire"}),
        sha256_json({"candidate": "accepted-wire"}),
        "S2_BREAKOUT", "1.0", S2_POLICY.policy_hash, CUTOFF,
        SelectionStateV2.SELECTED, AdmissionStateV2.NOT_EVALUATED, None, None,
        DecisionSourceStageV2.CANDIDATE_SET, (), sha256_json({"source": "accepted-wire"}),
        CUTOFF, CUTOFF,
    )
    wire = row.to_dict()
    assert wire["version"] == "DECISION_CALENDAR_ENTRY_V2_V1"
    assert DecisionCalendarEntryV2.from_dict(wire) == row
    assert DecisionCalendarEntryV2.from_dict(wire).content_hash == row.content_hash


def test_indexed_default_risk_and_economic_resolvers_reject_future_and_ambiguity(tmp_path):
    path = tmp_path / "indexed-resolvers.sqlite"
    with OpsRepository(path) as repository:
        event, inputs, case, candidate_set, candidate, _, action_ref, _ = _production_event(repository)
        risk, reason = production._resolve_indexed_risk_inputs(
            repository, event, candidate_set, candidate, case.universe, now_ns=CUTOFF + 100,
        )
        assert reason is None and risk is not None and risk.complete

        future_account = replace(
            case.account,
            available_at_ns=CUTOFF + 1,
            eligible_equity=case.account.eligible_equity + Decimal("1"),
        )
        index_risk_evidence(repository, future_account)
        asof_risk, reason = production._resolve_indexed_risk_inputs(
            repository, event, candidate_set, candidate, case.universe, now_ns=CUTOFF + 100,
        )
        assert reason is None and asof_risk is not None
        assert asof_risk.account is not None and asof_risk.account.content_hash == case.account.content_hash

        action_entry = repository.get_artifact(action_ref)
        assert action_entry is not None
        action_body = json_value(action_entry.metadata["action_artifact"])
        identity = json_value(action_entry.metadata["action_identity"])
        action = ActionArtifactV2(
            FrozenActionV2(
                candidate.key, identity["side"], Decimal(identity["quantity"]), identity["product_ref"],
                FrozenMap(identity["entry_rule"]), FrozenMap(identity["collar_rule"]),
                Decimal(identity["entry_reference"]), Decimal(identity["entry_collar"]),
                Decimal(identity["stop_price"]), identity["entry_trigger_basis"],
                identity["stop_trigger_basis"], FrozenMap(identity["management_rule"]),
                FrozenMap(identity["time_exit_rule"]), identity["horizon_end_ns"],
                identity["policy_id"], identity["policy_version"], identity["policy_hash"],
                identity["risk_policy_hash"], identity["risk_policy_v2_hash"],
            ),
            action_body["candidate_ref"], action_body["sizing_ref"], action_body["candidate_set_ref"],
            action_body["available_at_ns"],
        )
        economic = inputs.economic_inputs[candidate.candidate_id]
        missing, reason = production._resolve_indexed_economic_inputs(
            repository, event, candidate_set, candidate, action, risk, now_ns=CUTOFF + 100,
        )
        assert missing is None and reason == "ADMISSION_POLICY_MISSING_OR_AMBIGUOUS"

        production.index_ops_admission_policy_evidence(
            repository,
            admission_policy=economic.admission_policy,
            available_at_ns=CUTOFF + 1,
            event_id=event.event_id,
            candidate_set_ref=candidate_set.content_hash,
            candidate_ref=candidate.content_hash,
            product_ref=case.product.content_hash,
        )
        future_policy, reason = production._resolve_indexed_economic_inputs(
            repository, event, candidate_set, candidate, action, risk, now_ns=CUTOFF + 100,
        )
        assert future_policy is None and reason == "ADMISSION_POLICY_MISSING_OR_AMBIGUOUS"

        capability = economic.capability
        assert capability is not None
        index_venue_capability_snapshot(repository, capability)
        production.index_ops_admission_policy_evidence(
            repository,
            admission_policy=economic.admission_policy,
            available_at_ns=CUTOFF,
            event_id=event.event_id,
            candidate_set_ref=candidate_set.content_hash,
            candidate_ref=candidate.content_hash,
            product_ref=case.product.content_hash,
        )
        for role, causal_input in (
            ("M0", economic.model_input),
            ("CALIBRATION", economic.calibration_input),
            ("EXECUTION_MODEL", economic.execution_model_input),
        ):
            assert causal_input is not None
            production.index_ops_causal_input_evidence(
                repository,
                role=role,
                causal_input=causal_input,
                available_at_ns=CUTOFF,
                event_id=event.event_id,
                candidate_set_ref=candidate_set.content_hash,
                candidate_ref=candidate.content_hash,
                product_ref=case.product.content_hash,
            )
        production.index_ops_economic_scenario_config(
            repository,
            scenario_seed=27027,
            scenario_count=32,
            evaluation_available_at_ns=CUTOFF + 10,
            available_at_ns=CUTOFF,
            event_id=event.event_id,
            candidate_set_ref=candidate_set.content_hash,
            candidate_ref=candidate.content_hash,
            product_ref=case.product.content_hash,
        )

        resolved, reason = production._resolve_indexed_economic_inputs(
            repository, event, candidate_set, candidate, action, risk, now_ns=CUTOFF + 100,
        )
        assert reason is None and resolved is not None and resolved.complete
        assert resolved.model_input == economic.model_input

        future_input = replace(economic.model_input, available_at_ns=CUTOFF + 1)
        production.index_ops_causal_input_evidence(
            repository,
            role="M0",
            causal_input=future_input,
            available_at_ns=CUTOFF + 1,
            event_id=event.event_id,
            candidate_set_ref=candidate_set.content_hash,
            candidate_ref=candidate.content_hash,
            product_ref=case.product.content_hash,
        )
        still_resolved, reason = production._resolve_indexed_economic_inputs(
            repository, event, candidate_set, candidate, action, risk, now_ns=CUTOFF + 100,
        )
        assert reason is None and still_resolved is not None
        assert still_resolved.model_input == economic.model_input

        duplicate_source = _causal_input(repository, "Session027AmbiguousM0InputV1", CUTOFF)
        production.index_ops_causal_input_evidence(
            repository,
            role="M0",
            causal_input=duplicate_source,
            available_at_ns=CUTOFF,
            event_id=event.event_id,
            candidate_set_ref=candidate_set.content_hash,
            candidate_ref=candidate.content_hash,
            product_ref=case.product.content_hash,
        )
        ambiguous, reason = production._resolve_indexed_economic_inputs(
            repository, event, candidate_set, candidate, action, risk, now_ns=CUTOFF + 100,
        )
        assert ambiguous is None
        assert reason == "CAUSAL_M0_CALIBRATION_OR_EXECUTION_INPUT_MISSING_OR_AMBIGUOUS"

        conflicting_account = replace(
            case.account,
            eligible_equity=case.account.eligible_equity + Decimal("2"),
        )
        index_risk_evidence(repository, conflicting_account)
        ambiguous_risk, reason = production._resolve_indexed_risk_inputs(
            repository, event, candidate_set, candidate, case.universe, now_ns=CUTOFF + 100,
        )
        assert ambiguous_risk is None
        assert reason == "AMBIGUOUS_CURRENT_ACCOUNT_RISK_SNAPSHOT"


def test_actual_adapter_recovers_before_collecting_and_uses_existing_pipeline_apis(tmp_path, monkeypatch):
    path = tmp_path / "ops.sqlite"
    source = ReconciledFixturePublicSource(None)
    port = production.ProductionOpsCyclePortV1(public_source=source)
    clock = FakeClock(CUTOFF + 100)
    with OpsSupervisorV2(path, port, clock_ns=clock) as supervisor:
        result = supervisor.run_once()
        assert supervisor.repository is not None
        assert source.repositories == [supervisor.repository]
    assert port._recovery_calls == 1
    assert port._collection_calls == 1
    assert source.calls == ["collect"]
    assert source.collectors[0].repository is source.repositories[0]
    assert source.recovery_snapshots[0].required_subscription_ref is not None

    calls: list[str] = []
    with OpsRepository(tmp_path / "api-call.sqlite") as repo:
        event, inputs, case, expected_set, selected, _, _, _ = _production_event(repo)
        source = ReconciledFixturePublicSource(event)
        provider = StaticInputsProvider(event.event_id, inputs)
        port = production.ProductionOpsCyclePortV1(
            public_source=source,
            inputs_provider=provider,
            clock_ns=lambda: CUTOFF + 100,
        )
        wrappers = (
            ("candidate_set", "assemble_multisleeve_research_candidate_set"),
            ("acceptance", "accept_research_candidates"),
            ("hard_risk", "size_selected_candidate"),
            ("action", "freeze_action"),
            ("evaluation", "run_phase2_economic_evaluation"),
            ("m1", "fit_m1"),
            ("analogue", "run_analogue_diagnostic_v1"),
            ("calendar", "index_decision_calendar_entry"),
        )
        original = {name: getattr(production, name) for _, name in wrappers}
        from atlas.v2.science import admission

        calendar_index = admission.index_decision_calendar_entry
        with monkeypatch.context() as patcher:
            def calendar_wrapper(*args, **kwargs):
                calls.append("calendar")
                return calendar_index(*args, **kwargs)

            patcher.setattr(admission, "index_decision_calendar_entry", calendar_wrapper)
            for label, name in wrappers:
                function = original[name]

                def wrapper(*args, __label=label, __function=function, **kwargs):
                    calls.append(__label)
                    return __function(*args, **kwargs)

                patcher.setattr(production, name, wrapper)
            event_ref = event.trigger_ref
            repo.register_artifact(
                _trigger_entry(event, {"event_id": event.event_id, "trigger": event.event_type,
                                                         "cutoff_ns": event.information_cutoff_ns})
            )
            recovery = port.recover(repo, now_ns=CUTOFF + 100)
            batch = port.collect(repo, now_ns=CUTOFF + 100, recovery=recovery)
            assert batch.events == (event,)
            staged: dict[PipelineStageV1, Any] = {}

            def checkpoint(item):
                staged[item.stage] = item

            result = port.process_event(
                repo,
                event,
                now_ns=CUTOFF + 100,
                source_health_state="HEALTHY_CURRENT",
                completed_stages={},
                checkpoint=checkpoint,
            )
        assert result.terminal_status in (OpsTerminalStatusV1.NOT_ESTIMABLE, OpsTerminalStatusV1.NO_TRADE)
        assert result.stages[3].artifact_refs[0] == expected_set.content_hash
        assert selected.candidate_id == expected_set.selected_candidate_id
        assert event_ref in event.causal_input_refs
        assert set(calls) >= {label for label, _ in wrappers}
        assert provider.calls == 1
        assert repo.get_artifact(expected_set.content_hash) is not None
        assert staged and len(staged) == len(production.PIPELINE_STAGE_ORDER)
        assert all(item.authority == "ZERO" for item in result.stages)


def test_public_collector_restart_subscriptions_cursor_and_health_are_used_by_adapter(tmp_path):
    from .test_memory import watch

    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        active = replace(watch("session027-production-watch", expires=CUTOFF + 10_000),
                         required_next_event="BAR_CLOSE_15M")
        repo.create_watch(active)
        cursor_metadata = {
            "source_id": "PUBLIC_MARKET",
            "channel": "KLINE_15M",
            "high_water_sequence": 42,
            "checkpoint_at_ns": CUTOFF,
            "recent_payload_hashes": {sha256_json("record-41"): sha256_json("payload-41")},
        }
        cursor_ref = sha256_json({"artifact_type": "PublicCollectorCursorV2", "metadata": cursor_metadata})
        repo.register_artifact(ArtifactIndexEntryV2(
            cursor_ref, "PublicCollectorCursorV2", sha256_json(cursor_metadata), CUTOFF, CUTOFF, cursor_metadata
        ))
    event = make_event()
    source = ReconciledFixturePublicSource(event)
    port = production.ProductionOpsCyclePortV1(public_source=source)
    clock = FakeClock(CUTOFF + 100)
    with OpsSupervisorV2(path, port, clock_ns=clock) as supervisor:
        supervisor.run_once()
        assert port._collector_recovery is not None
        collector = port._collector_recovery.collector
        assert collector._last_sequence[("PUBLIC_MARKET", "KLINE_15M")] == 42
        assert port._collector_recovery.restored_subscription_plan.plan_id
        assert "session027-production-watch" in source.recovery_snapshots[0].restored_watch_ids
        assert collector.health.latest("PUBLIC_MARKET").state == PublicSourceStateV2.HEALTHY_CURRENT

    clock.now_ns += 10
    restarted_source = ReconciledFixturePublicSource(event)
    restarted_port = production.ProductionOpsCyclePortV1(public_source=restarted_source)
    with OpsSupervisorV2(path, restarted_port, clock_ns=clock) as restarted:
        restarted.run_once()
        assert restarted_port._collector_recovery is not None
        restored = restarted_port._collector_recovery.collector.health.latest("PUBLIC_MARKET")
        assert restored is not None
        assert restored.state == PublicSourceStateV2.INCOMPLETE_SNAPSHOT
        clock.now_ns += 10
        repaired = restarted.run_once()
        assert repaired.event_receipts
        assert restarted_port._collector_recovery.collector.health.latest("PUBLIC_MARKET").state == (
            PublicSourceStateV2.HEALTHY_CURRENT
        )


def test_builtin_event_handoff_waits_for_collector_reconnect_reconciliation(tmp_path):
    event = make_event()
    path = tmp_path / "indexed-source.sqlite"
    with OpsRepository(path) as repo:
        repo.register_artifact(_trigger_entry(event, {"event_id": event.event_id}))
        repo.register_artifact(ArtifactIndexEntryV2(
            sha256_json({"event_source": event.event_id}),
            "OpsDecisionEventSourceV1",
            sha256_json({"event_source": event.event_id}),
            event.available_at_ns,
            event.available_at_ns,
            {"event": event.to_dict()},
        ))
        observation = RawObservationV2.build(
            instrument_revision=KEY.contract_revision, source_id="PUBLIC_MARKET",
            event_type="TICKER_MARK_INDEX_FUNDING_OI", event_at_ns=CUTOFF,
            received_at_ns=CUTOFF, ingested_at_ns=CUTOFF, available_at_ns=CUTOFF,
            translation_version="session027-recovery-test-v1", payload={"bid1Price": "1", "ask1Price": "2"},
        )
        observation_ref = sha256_json({
            "artifact_type": "PublicObservationIndexV2", "record_id": observation.record_id,
        })
        repo.register_artifact(ArtifactIndexEntryV2(
            observation_ref, "PublicObservationIndexV2", observation.content_hash,
            observation.available_at_ns, observation.available_at_ns,
            {"record_id": observation.record_id, "source_id": "PUBLIC_MARKET",
             "instrument_revision": KEY.contract_revision, "instrument_key_json": KEY.to_canonical_json(),
             "event_at_ns": observation.event_at_ns, "published_at_ns": None,
             "raw_payload_hash": observation.raw_payload_hash},
        ))
        old_epoch = _index_recovery_epoch(repo, started_at_ns=CUTOFF - 2, epoch_index=1)
        stale_ref = _index_reconnect_reconciliation(
            repo, source_id="PUBLIC_MARKET", epoch_ref=old_epoch, at_ns=CUTOFF - 1,
            observation_refs=(observation_ref,),
        )
        repo.record_source_health(PublicSourceHealthV2(
            "PUBLIC_MARKET",
            CUTOFF,
            CUTOFF,
            PublicSourceStateV2.HEALTHY_CURRENT,
            sha256_json("initial-public-health"),
            "fixture public source was current before restart",
        ).to_ops_record())
    clock = FakeClock(CUTOFF + 100)
    port = production.create_production_port()
    with OpsSupervisorV2(path, port, clock_ns=clock) as supervisor:
        first = supervisor.run_once()
        assert not first.event_receipts
        assert port._collector_recovery is not None
        assert port._collector_recovery.collector.health.latest("PUBLIC_MARKET").state == (
            PublicSourceStateV2.INCOMPLETE_SNAPSHOT
        )
        assert not first.cycle.event_ids
        assert port._collector_recovery.recovery_epoch_ref != old_epoch
        assert supervisor.repository is not None
        stale = supervisor.repository.get_artifact(stale_ref)
        assert stale is not None
        assert stale.metadata["reconciliation"]["recovery_epoch_ref"] == old_epoch
        clock.now_ns += 10
        with pytest.raises(ValueError, match="current recovery epoch"):
            port._collector_recovery.collector.reconcile_after_reconnect(
                "PUBLIC_MARKET", at_ns=clock.now_ns, complete_snapshot=True,
                missed_interval_repaired=True, snapshot_refs=(observation_ref,),
            )
        _index_reconnect_reconciliation(
            supervisor.repository, source_id="PUBLIC_MARKET",
            epoch_ref=port._collector_recovery.recovery_epoch_ref,
            at_ns=clock.now_ns, observation_refs=(observation_ref,),
        )
        second = supervisor.run_once()
    assert len(second.event_receipts) == 1
    assert second.event_receipts[0].result.terminal_status == OpsTerminalStatusV1.NOT_ESTIMABLE
    assert second.event_receipts[0].candidate_set_ref is not None
    with OpsRepository(path, read_only=True) as reader:
        calendar = reader.get_artifact(second.event_receipts[0].calendar_refs[0])
        assert "CAUSAL_FEATURE_EVIDENCE_UNAVAILABLE" in calendar.metadata["decision_entry"]["reason_codes"]


def test_successful_production_composition_preserves_ids_and_binds_zero_authority_diagnostics(tmp_path):
    path = tmp_path / "ops.sqlite"
    clock = FakeClock(CUTOFF + 100)
    with OpsRepository(path) as setup_repo:
        event, inputs, case, expected_set, selected, sizing_ref, action_ref, action_hash = _production_event(setup_repo)
        # The adapter will re-run these existing immutable APIs. Record artifact counts
        # after fixture construction to prove retries do not create duplicate identities.
        before = {
            kind: len(setup_repo.artifact_entries(kind))
            for kind in ("CandidateSetV2", "ActionArtifactV2", "EvaluationArtifactV2", "DecisionCalendarEntryV2")
        }
    port, source, supervisor = _run_with_production_port(path, event, inputs, clock)
    with supervisor:
        output = supervisor.run_once()
        receipt = output.event_receipts[0]
        assert supervisor.repository is not None
        repo = supervisor.repository
        after_first = {
            kind: len(repo.artifact_entries(kind))
            for kind in before
        }
        sizing = repo.get_artifact(receipt.sizing_ref or "")
        action = repo.get_artifact(receipt.action_ref or "")
        evaluation = repo.get_artifact(receipt.evaluation_ref or "")
        calendar = repo.get_artifact(receipt.calendar_refs[0])
        m1 = receipt.result.stages[8]
        analogue = receipt.result.stages[9]
        assert sizing is not None and sizing.artifact_type == "SizingDecisionV2"
        assert action is not None and action.artifact_type == "ActionArtifactV2"
        assert evaluation is not None and evaluation.artifact_type == "EvaluationArtifactV2"
        assert calendar is not None and calendar.artifact_type == "DecisionCalendarEntryV2"
        assert receipt.candidate_set_ref == expected_set.content_hash
        assert receipt.sizing_ref == sizing_ref
        assert receipt.action_ref == action_ref
        assert receipt.to_dict()["action_hash"] == action_hash == sha256_json(action.metadata["action_identity"])
        assert m1.bound_action_hash == analogue.bound_action_hash == receipt.to_dict()["action_hash"]
        assert m1.authority == analogue.authority == "ZERO"
        assert m1.status != "SKIPPED" and analogue.status != "SKIPPED"
        assert receipt.to_dict()["agent_mode"] == "DISABLED"
        assert not receipt.to_dict()["capital_enabled"] and not receipt.to_dict()["assisted_enabled"]
        assert receipt.result.terminal_status == OpsTerminalStatusV1.NOT_ESTIMABLE
        assert source.repositories[0] is repo
        assert port._recovery_calls == port._collection_calls == 1

    clock.now_ns += 10
    restarted_source = ReconciledFixturePublicSource(event)
    restarted_port = production.ProductionOpsCyclePortV1(
        public_source=restarted_source,
        inputs_provider=StaticInputsProvider(event.event_id, inputs),
    )
    with OpsSupervisorV2(path, restarted_port, clock_ns=clock) as restarted:
        restarted.run_once()
        clock.now_ns += 10
        replay_output = restarted.run_once()
        replay = replay_output.event_receipts[0]
        assert restarted.repository is not None
        after_restart = {
            kind: len(restarted.repository.artifact_entries(kind))
            for kind in before
        }
    assert replay.content_hash == receipt.content_hash
    assert after_first == after_restart


def test_missing_and_future_mandatory_evidence_terminates_without_fabricating_progress(tmp_path):
    cases = (
        {"missing_account": True},
        {"future_account": True},
        {"future_fee": True},
        {"future_stress": True},
    )
    for index, options in enumerate(cases):
        path = tmp_path / f"safe-{index}.sqlite"
        with OpsRepository(path) as setup_repo:
            event, inputs, _case, _candidate_set, _selected, _, _, _ = _production_event(setup_repo, **options)
            prior_refs = {
                kind: tuple(entry.artifact_ref for entry in setup_repo.artifact_entries(kind))
                for kind in ("ActionArtifactV2", "EvaluationArtifactV2")
            }
        clock = FakeClock(CUTOFF + 100)
        source = ReconciledFixturePublicSource(event)
        port = production.ProductionOpsCyclePortV1(
            public_source=source,
            inputs_provider=StaticInputsProvider(event.event_id, inputs),
        )
        with OpsSupervisorV2(path, port, clock_ns=clock) as supervisor:
            receipt = supervisor.run_once().event_receipts[0]
            repo = supervisor.repository
            assert repo is not None
            assert receipt.result.terminal_status == OpsTerminalStatusV1.NOT_ESTIMABLE
            assert not receipt.action_ref
            assert not receipt.evaluation_ref
            assert repo.artifact_entries("TradePlanEnvelopeV2") == ()
            assert repo.artifact_entries("OrderIntentV2") == ()
            assert repo.artifact_entries("Approval") == ()
            assert tuple(entry.artifact_ref for entry in repo.artifact_entries("ActionArtifactV2")) == prior_refs[
                "ActionArtifactV2"
            ]
            assert tuple(entry.artifact_ref for entry in repo.artifact_entries("EvaluationArtifactV2")) == prior_refs[
                "EvaluationArtifactV2"
            ]
            calendar = repo.get_artifact(receipt.calendar_refs[0])
            assert calendar is not None and calendar.artifact_type == "DecisionCalendarEntryV2"
            assert calendar.metadata["decision_entry"]["admission_state"] == "NOT_EVALUATED"
            assert calendar.metadata["decision_entry"]["source_stage"] == "CANDIDATE_SET"


def test_missing_economic_evidence_keeps_only_existing_hard_risk_action(tmp_path):
    path = tmp_path / "missing-economic.sqlite"
    with OpsRepository(path) as setup_repo:
        event, inputs, _case, expected_set, _selected, sizing_ref, action_ref, action_hash = _production_event(
            setup_repo, no_economic_inputs=True
        )
    source = ReconciledFixturePublicSource(event)
    port = production.ProductionOpsCyclePortV1(
        public_source=source,
        inputs_provider=StaticInputsProvider(event.event_id, inputs),
    )
    with OpsSupervisorV2(path, port, clock_ns=FakeClock(CUTOFF + 100)) as supervisor:
        receipt = supervisor.run_once().event_receipts[0]
        assert supervisor.repository is not None
        assert receipt.candidate_set_ref == expected_set.content_hash
        assert receipt.sizing_ref == sizing_ref
        assert receipt.action_ref == action_ref
        assert receipt.to_dict()["action_hash"] == action_hash
        assert receipt.evaluation_ref is None
        assert receipt.result.terminal_status == OpsTerminalStatusV1.NOT_ESTIMABLE
        assert supervisor.repository.artifact_entries("EvaluationArtifactV2") == ()
        calendar = supervisor.repository.get_artifact(receipt.calendar_refs[0])
        assert calendar is not None
        assert calendar.metadata["decision_entry"]["admission_state"] == "RISK_SIZED"
        assert calendar.metadata["decision_entry"]["source_stage"] == "HARD_RISK"
        assert calendar.metadata["decision_entry"]["action_artifact_ref"] == action_ref


def test_future_capability_keeps_hard_risk_action_but_blocks_evaluation(tmp_path):
    path = tmp_path / "future-capability.sqlite"
    with OpsRepository(path) as setup_repo:
        event, inputs, _case, expected_set, _selected, sizing_ref, action_ref, action_hash = _production_event(
            setup_repo, future_capability=True
        )
    source = ReconciledFixturePublicSource(event)
    port = production.ProductionOpsCyclePortV1(
        public_source=source,
        inputs_provider=StaticInputsProvider(event.event_id, inputs),
    )
    with OpsSupervisorV2(path, port, clock_ns=FakeClock(CUTOFF + 100)) as supervisor:
        receipt = supervisor.run_once().event_receipts[0]
        assert supervisor.repository is not None
        assert receipt.candidate_set_ref == expected_set.content_hash
        assert receipt.sizing_ref == sizing_ref
        assert receipt.action_ref == action_ref
        assert receipt.to_dict()["action_hash"] == action_hash
        assert receipt.evaluation_ref is None
        assert receipt.result.terminal_status == OpsTerminalStatusV1.NOT_ESTIMABLE
        assert supervisor.repository.artifact_entries("EvaluationArtifactV2") == ()


def test_injected_production_crashes_resume_existing_artifacts_in_order(tmp_path):
    for stage in (
        PipelineStageV1.UNIVERSE,
        PipelineStageV1.CANDIDATE_SET,
        PipelineStageV1.HARD_RISK,
        PipelineStageV1.ECONOMIC_EVALUATION,
    ):
        path = tmp_path / f"crash-{stage.value}.sqlite"
        with OpsRepository(path) as setup_repo:
            event, inputs, _case, _candidate_set, _selected, _, _, _ = _production_event(setup_repo)
        clock = FakeClock(CUTOFF + 100)
        failed = False

        def crash(checkpoint_stage, *, expected_stage=stage):
            nonlocal failed
            if checkpoint_stage == expected_stage and not failed:
                failed = True
                raise RuntimeError("injected production stage crash")

        source = ReconciledFixturePublicSource(event)
        port = production.ProductionOpsCyclePortV1(
            public_source=source,
            inputs_provider=StaticInputsProvider(event.event_id, inputs),
            crash_after_checkpoint=crash,
        )
        with OpsSupervisorV2(path, port, clock_ns=clock) as supervisor:
            first = supervisor.run_once()
            assert failed and not first.event_receipts
            before = {
                kind: tuple(item.artifact_ref for item in supervisor.repository.artifact_entries(kind))
                for kind in ("CandidateSetV2", "SizingDecisionV2", "ActionArtifactV2", "EvaluationArtifactV2",
                             "DecisionCalendarEntryV2")
            }
        clock.now_ns += 10
        source_after_restart = ReconciledFixturePublicSource(event)
        restarted_port = production.ProductionOpsCyclePortV1(
            public_source=source_after_restart,
            inputs_provider=StaticInputsProvider(event.event_id, inputs),
        )
        with OpsSupervisorV2(path, restarted_port, clock_ns=clock) as restarted:
            waiting = restarted.run_once()
            assert not waiting.event_receipts
            clock.now_ns += 10
            resumed = restarted.run_once()
            assert len(resumed.event_receipts) == 1
            receipt = resumed.event_receipts[0]
            after = {
                kind: tuple(item.artifact_ref for item in restarted.repository.artifact_entries(kind))
                for kind in before
            }
            checkpoints = [
                restarted.repository.get_artifact(restarted._checkpoint_ref(event.event_id, item))
                for item in production.PIPELINE_STAGE_ORDER
            ]
        for kind, prior_refs in before.items():
            assert set(prior_refs).issubset(after[kind])
            assert len(after[kind]) == len(set(after[kind]))
        assert len(after["SizingDecisionV2"]) == 1
        assert len(after["ActionArtifactV2"]) == 1
        assert len(after["EvaluationArtifactV2"]) == 1
        assert len(after["DecisionCalendarEntryV2"]) == 1
        assert all(item is not None for item in checkpoints)
        assert tuple(result.stage for result in receipt.result.stages) == production.PIPELINE_STAGE_ORDER
        assert all(result.completed_at_ns >= event.available_at_ns for result in receipt.result.stages)
        assert all(result.completed_at_ns <= clock.now_ns for result in receipt.result.stages)
        assert restarted_port._collection_calls == 2


def test_agent_dependencies_and_external_provider_calls_are_not_required_or_invoked(monkeypatch):
    production_path = Path(production.__file__)
    tree = ast.parse(production_path.read_text())
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    assert not any("agent_intelligence" in name for name in imported)
    assert not any(name.startswith(("httpx", "urllib", "websockets")) for name in imported)

    original_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        if name.startswith("atlas.v2.agent_intelligence"):
            raise AssertionError("agent packages must remain optional for ops runtime")
        return original_import(name, *args, **kwargs)

    network_calls: list[str] = []
    monkeypatch.setattr(builtins, "__import__", guarded_import)
    monkeypatch.setattr(urllib.request, "urlopen", lambda *a, **k: network_calls.append("urlopen"))
    monkeypatch.setattr(socket.socket, "connect", lambda *a, **k: network_calls.append("connect"))
    reloaded = importlib.reload(production)
    reloaded.create_production_port()
    assert network_calls == []


def test_production_composition_has_no_capital_or_live_control_boundary():
    source = Path(production.__file__).read_text()
    tree = ast.parse(source)
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    forbidden_imports = (
        "atlas.runtime",
        "atlas.v1",
        "atlas.desktop",
        "atlas.v2.agent_intelligence",
    )
    assert not any(name.startswith(forbidden_imports) for name in imported)
    forbidden_symbols = (
        "SafeRuntime",
        "OrderIntent",
        "TradePlanEnvelope",
        "ApprovalPort",
        "ProtectionPort",
        "submit_order",
        "enable_capital",
        "assisted_execution",
    )
    assert not any(symbol in source for symbol in forbidden_symbols)
    assert 'OPS_PRODUCTION_ADAPTER_ID = "ATLAS_V2_PRODUCTION_OPS_COMPOSITION_V1"' in source
    assert "agent_mode" not in source or '"DISABLED"' not in source


def test_production_inputs_reject_non_exact_or_sized_sleeve_actions(tmp_path):
    from atlas.v2.contracts import CandidateActionV2

    with OpsRepository(tmp_path / "contract.sqlite") as repo:
        case = research_case(repo)
        with pytest.raises(ValueError, match="S1-S3"):
            production.ProductionEventInputsV1(
                case.universe,
                (replace(case.candidate, policy_hash=sha256_json("S4_CONTEXT"),
                         envelope=replace(case.candidate.envelope, content_hash="")),),
                {},
                {},
                {},
            )
        assert isinstance(case.candidate, CandidateActionV2)
        with pytest.raises(ValueError, match="unsized"):
            production.ProductionEventInputsV1(
                case.universe,
                (replace(case.candidate, quantity=Decimal("1"),
                         envelope=replace(case.candidate.envelope, content_hash="")),),
                {},
                {},
                {},
            )
