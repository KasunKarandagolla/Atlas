"""Phase-5 scanner orchestration around the frozen Phase-4 evaluator."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from atlas.science.evaluation import DecisionStatus
from atlas.science.research_archive import ResearchArtifactArchive

from .alerts import AlertDelivery, AlertTransport, RecordingAlertTransport, alerts_from_rows
from .blindspots import blindspot_metrics
from .calendar import ScannerCalendar
from .cheap_scan import cheap_scan
from .handoff import not_applicable_result
from .health import scanner_health
from .models import (
    CAPITAL_ENABLED_INSTRUMENTS,
    SLOT_NS,
    BlindSpotMetrics,
    BlindSpotObservation,
    CheapScanInput,
    CheapScanObservation,
    DeadlineStatus,
    EligibilityStatus,
    ExplorationSelection,
    Phase4HandoffRequest,
    Phase4ScannerResult,
    RankedObservation,
    ScannerAlert,
    ScannerCalendarRow,
    ScannerHealth,
    ScannerPolicy,
    ScannerSelection,
    UniverseSnapshot,
    WarmupEvidence,
    WarmupState,
    WarmupStatus,
)
from .persistence import persist_scanner_artifacts
from .ranking import rank_observations
from .selection import select_candidates, select_exploration
from .warmup import evaluate_warmup


class Phase4Evaluator(Protocol):
    def __call__(self, request: Phase4HandoffRequest) -> Phase4ScannerResult: ...


@dataclass(frozen=True)
class ScanSlotResult:
    slot_at_ns: int
    universe: UniverseSnapshot
    cheap_observations: tuple[CheapScanObservation, ...]
    ranked: tuple[RankedObservation, ...]
    selections: tuple[ScannerSelection, ...]
    exploration: ExplorationSelection
    warmups: tuple[WarmupStatus, ...]
    calendar_rows: tuple[ScannerCalendarRow, ...]
    alerts: tuple[ScannerAlert, ...]
    alert_deliveries: tuple[AlertDelivery, ...]
    health: ScannerHealth
    blindspots: BlindSpotMetrics
    blind_observations: tuple[BlindSpotObservation, ...]
    artifact_paths: Mapping[str, tuple[Path, ...]]


def _not_estimable(*, request: Phase4HandoffRequest, reason: str) -> Phase4ScannerResult:
    reference = f"not-estimable:{request.slot_at_ns}:{request.instrument}:{reason}"
    return Phase4ScannerResult(request.slot_at_ns, request.instrument, DecisionStatus.NOT_ESTIMABLE,
                               reference, None, (), (reason,), reference)


def _phase4_result(*, request: Phase4HandoffRequest, warmup: WarmupStatus, evaluator: Phase4Evaluator | None,
                   top_k_selected: bool) -> Phase4ScannerResult:
    if request.instrument not in CAPITAL_ENABLED_INSTRUMENTS:
        return not_applicable_result(request=request, reason="RESEARCH_SHADOW_ONLY")
    if not top_k_selected:
        return not_applicable_result(request=request, reason="OUTSIDE_TOP_K_NO_CAPITAL_AUTHORITY")
    if warmup.state is not WarmupState.WARM_AVAILABLE:
        return _not_estimable(request=request, reason=warmup.reason)
    if evaluator is None:
        return _not_estimable(request=request, reason="PHASE4_EVALUATOR_UNAVAILABLE")
    result = evaluator(request)
    if result.instrument != request.instrument or result.slot_at_ns != request.slot_at_ns:
        raise ValueError("Phase-4 handoff returned a mismatched instrument/slot")
    return result


def _calendar_row(*, universe: UniverseSnapshot, observation: CheapScanObservation | None,
                  ranked: RankedObservation | None, selection: ScannerSelection | None,
                  exploration: ExplorationSelection, warmup: WarmupStatus, result: Phase4ScannerResult | None,
                  policy: ScannerPolicy, counterfactual_value: float | None,
                  availability_cutoff_ns: int) -> ScannerCalendarRow:
    rejection_reason = None
    not_estimable_reason = None
    if result is not None and result.status is not DecisionStatus.TRADE_CANDIDATE:
        rejection_reason = result.reasons[0] if result.reasons else result.status.value
        if result.not_estimable_reasons:
            not_estimable_reason = result.not_estimable_reasons[0]
    if (result is None or result.instrument not in CAPITAL_ENABLED_INSTRUMENTS
            or selection is None or not selection.top_k_selected):
        plan_status = "NOT_APPLICABLE"
    else:
        plan_status = result.plan_status()
    return ScannerCalendarRow(
        scan_slot_id=f"scan-{warmup.slot_at_ns}",
        scan_slot_at_ns=warmup.slot_at_ns,
        scanner_policy_version=policy.policy_version,
        universe_version=universe.version,
        universe_hash=universe.hash(),
        instrument=warmup.instrument,
        availability_cutoff_ns=availability_cutoff_ns,
        eligibility_status=EligibilityStatus.ELIGIBLE,
        exclusion_reason=None,
        cheap_snapshot_hash=observation.hash() if observation is not None else None,
        cheap_score=observation.score if observation is not None else None,
        rank=ranked.rank if ranked is not None else None,
        rank_tie_break=ranked.tie_break_key if ranked is not None else None,
        rank_band=ranked.rank_band if ranked is not None else None,
        correlation_cluster=ranked.correlation_cluster if ranked is not None else None,
        top_k_selected=selection.top_k_selected if selection is not None else False,
        deep_selected=selection.deep_selected if selection is not None else False,
        exploration_selected=exploration.instrument == warmup.instrument,
        exploration_probability=(exploration.inclusion_probability
                                 if exploration.instrument == warmup.instrument else None),
        warmup_state=warmup.state,
        model_job_enqueued_at_ns=warmup.job_enqueued_at_ns,
        model_job_started_at_ns=warmup.job_started_at_ns,
        model_job_finished_at_ns=warmup.job_finished_at_ns,
        model_deadline_status=warmup.deadline_status,
        plan_status=plan_status,
        rejection_reason=rejection_reason,
        not_estimable_reason=not_estimable_reason,
        phase4_evaluation_ref=result.evaluation_ref if result is not None else None,
        trade_plan_id=result.plan_id() if result is not None else None,
        trade_plan_hash=result.plan_hash() if result is not None else None,
        counterfactual_value=counterfactual_value,
        selection_reason=selection.selection_reason if selection is not None else None,
    )


def _excluded_row(*, universe: UniverseSnapshot, instrument: str, slot_at_ns: int,
                  policy: ScannerPolicy) -> ScannerCalendarRow:
    entry = universe.entry(instrument)
    return ScannerCalendarRow(
        scan_slot_id=f"scan-{slot_at_ns}",
        scan_slot_at_ns=slot_at_ns,
        scanner_policy_version=policy.policy_version,
        universe_version=universe.version,
        universe_hash=universe.hash(),
        instrument=instrument,
        availability_cutoff_ns=entry.available_at_ns,
        eligibility_status=entry.eligibility_status,
        exclusion_reason=entry.exclusion_reason,
        cheap_snapshot_hash=None,
        cheap_score=None,
        rank=None,
        rank_tie_break=None,
        rank_band=None,
        correlation_cluster=None,
        top_k_selected=False,
        deep_selected=False,
        exploration_selected=False,
        exploration_probability=None,
        warmup_state=WarmupState.NOT_APPLICABLE,
        model_job_enqueued_at_ns=None,
        model_job_started_at_ns=None,
        model_job_finished_at_ns=None,
        model_deadline_status=DeadlineStatus.NOT_APPLICABLE,
        plan_status="NOT_APPLICABLE",
        rejection_reason=entry.exclusion_reason,
        not_estimable_reason=None,
        phase4_evaluation_ref=None,
        trade_plan_id=None,
        trade_plan_hash=None,
        counterfactual_value=None,
    )


def run_scan_slot(*, slot_at_ns: int, universe: UniverseSnapshot, cheap_inputs: Sequence[CheapScanInput],
                  policy: ScannerPolicy, warmup_evidence: Sequence[WarmupEvidence] = (),
                  evaluator: Phase4Evaluator | None = None,
                  alert_transport: AlertTransport | None = None,
                  archive: ResearchArtifactArchive | None = None, calendar: ScannerCalendar | None = None,
                  return_histories: Mapping[str, Sequence[float]] | None = None,
                  counterfactual_values: Mapping[str, float] | None = None,
                  now_ns: int | None = None, persist: bool = True) -> ScanSlotResult:
    """Run one four-hour scanner slot without execution or capital authority."""
    if slot_at_ns % SLOT_NS:
        raise ValueError("scanner slots are four-hour UTC slots")
    if universe.available_at_ns > slot_at_ns:
        raise ValueError("universe snapshot is not available at the scanner slot")
    eligible = universe.eligible_entries()
    eligible_ids = tuple(entry.instrument for entry in eligible)
    input_map = {value.instrument: value for value in cheap_inputs}
    if len(input_map) != len(tuple(cheap_inputs)):
        raise ValueError("duplicate cheap-scan inputs")
    if set(input_map) != set(eligible_ids):
        raise ValueError("cheap scan must cover exactly the eligible universe")
    for value in input_map.values():
        if value.slot_at_ns != slot_at_ns:
            raise ValueError("cheap-scan input belongs to a different slot")

    observations = cheap_scan(tuple(input_map[instrument] for instrument in eligible_ids),
                              scanner_policy_version=policy.policy_version,
                              scorer_version=policy.cheap_scorer_version)
    histories = {entry.instrument: (return_histories or {}).get(entry.instrument, entry.causal_return_history)
                 for entry in eligible}
    ranked = rank_observations(observations, universe_hash=universe.hash(), return_histories=histories)
    selections = select_candidates(ranked, top_k=policy.top_k, deep_k=policy.deep_k)
    exploration = select_exploration(ranked, policy_version=policy.policy_version, universe_hash=universe.hash(),
                                     slot_at_ns=slot_at_ns, universe_version=universe.version)
    requested = {selection.instrument for selection in selections if selection.deep_selected}
    if exploration.instrument is not None:
        requested.add(exploration.instrument)
    evidence = {item.instrument: item for item in warmup_evidence}
    for item in evidence.values():
        if item.slot_at_ns != slot_at_ns:
            raise ValueError("warmup evidence belongs to a different slot")
    warmups = evaluate_warmup(slot_at_ns=slot_at_ns, requested_instruments=tuple(sorted(requested)),
                              evidence=evidence, deadline_at_ns=slot_at_ns + policy.warmup_deadline_ns,
                              observed_instruments=eligible_ids)
    warmup_map = {status.instrument: status for status in warmups}
    observation_map = {item.instrument: item for item in observations}
    ranked_map = {item.instrument: item for item in ranked}
    selection_map = {item.instrument: item for item in selections}
    counterfactual_values = counterfactual_values or {}

    rows: list[ScannerCalendarRow] = []
    for entry in universe.entries:
        if entry.eligibility_status is not EligibilityStatus.ELIGIBLE:
            rows.append(_excluded_row(universe=universe, instrument=entry.instrument, slot_at_ns=slot_at_ns,
                                      policy=policy))
            continue
        selection = selection_map[entry.instrument]
        warmup = warmup_map[entry.instrument]
        observation = observation_map[entry.instrument]
        request = Phase4HandoffRequest(slot_at_ns, entry.instrument, observation.availability_cutoff_ns,
                                       universe.hash(), observation.hash(), warmup.state)
        result = _phase4_result(request=request, warmup=warmup, evaluator=evaluator,
                                top_k_selected=selection.top_k_selected)
        rows.append(_calendar_row(universe=universe, observation=observation, ranked=ranked_map[entry.instrument],
                                  selection=selection, exploration=exploration, warmup=warmup, result=result,
                                  policy=policy, counterfactual_value=counterfactual_values.get(entry.instrument),
                                  availability_cutoff_ns=observation.availability_cutoff_ns))

    scanner_calendar = calendar or ScannerCalendar(archive if persist else None)
    for row in rows:
        scanner_calendar.append(row)
    scanner_calendar.assert_complete(slot_at_ns, tuple(entry.instrument for entry in universe.entries))

    blind_observations = tuple(BlindSpotObservation(
        slot_at_ns=slot_at_ns,
        instrument=item.instrument,
        rank_band=item.rank_band,
        top_k_selected=selection_map[item.instrument].top_k_selected,
        exploration_selected=exploration.instrument == item.instrument,
        inclusion_probability=(exploration.inclusion_probability if exploration.instrument == item.instrument else 0.0),
        counterfactual_value=counterfactual_values.get(item.instrument),
        warmup_available=warmup_map[item.instrument].state is WarmupState.WARM_AVAILABLE,
        deadline_met=warmup_map[item.instrument].deadline_status is DeadlineStatus.MET,
    ) for item in ranked)
    metrics = blindspot_metrics(blind_observations, tolerance=policy.blindspot_tolerance)

    finished = [status.job_finished_at_ns for status in warmups
                if status.state is WarmupState.WARM_AVAILABLE and status.job_finished_at_ns is not None]
    transport = alert_transport or RecordingAlertTransport()
    preliminary = scanner_health(now_ns=now_ns or slot_at_ns, rows=rows, alert_deliveries=(),
                                 calendar_persistence_healthy=True, research_archive_healthy=archive is not None,
                                 phase4_evaluator_available=evaluator is not None, policy=policy,
                                 universe_observed_at_ns=universe.observed_at_ns,
                                 cheap_scan_finished_at_ns=slot_at_ns,
                                 deep_data_finished_at_ns=min(finished) if finished else None,
                                 last_completed_scan_slot=slot_at_ns)
    alerts = alerts_from_rows(rows, policy=policy, created_at_ns=slot_at_ns, health=preliminary)
    deliveries = tuple(transport.deliver(alert) for alert in alerts)
    health = scanner_health(now_ns=now_ns or slot_at_ns, rows=rows, alert_deliveries=deliveries,
                            calendar_persistence_healthy=True, research_archive_healthy=archive is not None,
                            phase4_evaluator_available=evaluator is not None, policy=policy,
                            universe_observed_at_ns=universe.observed_at_ns,
                            cheap_scan_finished_at_ns=slot_at_ns,
                            deep_data_finished_at_ns=min(finished) if finished else None,
                            last_completed_scan_slot=slot_at_ns)
    paths: Mapping[str, tuple[Path, ...]] = {}
    if archive is not None and persist:
        paths = persist_scanner_artifacts(archive, universe=universe, cheap_observations=observations,
                                          ranked=ranked, selections=selections, exploration=exploration,
                                          warmups=warmups, health=health, alerts=alerts, blindspots=metrics)
    return ScanSlotResult(slot_at_ns, universe, observations, ranked, selections, exploration, warmups,
                          tuple(rows), alerts, deliveries, health, metrics, blind_observations, paths)
