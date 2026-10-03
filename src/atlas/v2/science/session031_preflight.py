"""Read-only public-shadow campaign preflight and evidence inventory reports."""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
from collections import Counter, defaultdict, deque
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from atlas.v2._serialization import canonical_json, json_value, sha256_json
from atlas.v2.contracts import ArtifactEnvelope
from atlas.v2.data.bars import BarIntervalV2
from atlas.v2.data.health import PublicSourceHealthV2
from atlas.v2.data.history import reconstruct_causal_bars_from_archive
from atlas.v2.data.raw import AvailabilityClassV2
from atlas.v2.instruments import InstrumentKeyV2, ProductContractV2, UniverseContractV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.news.events import AbnormalityStateV2, EventGateStateV2, EventSafetyGateV2
from atlas.v2.runtime.production import (
    _indexed_quote_and_mark,
    _indexed_s3_trades,
    decision_event_from_dict,
)
from atlas.v2.science.outcomes import DecisionCalendarEntryV2, MaturedOutcomeV2
from atlas.v2.science.session031_campaign import (
    S23_EXPERIMENT_ID,
    S23_EXPERIMENT_REF,
    S23_MAXIMUM_ATTEMPTS,
    S23_MULTIPLICITY_FAMILY_ID,
    S23_MULTIPLICITY_REF,
    S23_PARAMETER_SEARCH_BUDGET,
    S23_PRIOR_ATTEMPTS,
    default_lane_identities,
)
from atlas.v2.science.session031_readiness import (
    StrategyEvidenceSnapshotV1,
    evaluate_strategy_readiness_v1,
    s3_cadence_report_v1,
)
from atlas.v2.science.session031_s3_provenance import validate_s3_persisted_lineage_v1
from atlas.v2.strategies.s1_trend import EventGate

_REPORT_TYPES = (
    "PublicObservationIndexV2", "PublicSourceHealthV2", "ProductContractV2",
    "OpsDecisionEventSourceV1", "OpsSupervisorReceiptV1", "DecisionCalendarEntryV2",
    "MaturedOutcomeV2", "CandidateSetV2", "CandidateActionV2", "FeatureArtifactV2",
    "UniverseContractV2", "S3TradeVwapSnapshotV2", "S3ResidualObservationV2", "EventSafetyGateV2",
    "ActionCriticShadowObservationV1", "SealedActionAssessmentPacketV1", "ActionAssessmentRequestV2",
    "DiscoveryExperimentV2", "DiscoveryAttemptV2", "DiscoveryRejectedAttemptV2",
    "OpsPublicSourceReconciliationV1", "PublicDuplicateConflictV2", "OpsRecoveryEpochV1",
    "OpsRuntimeRestartMarkerV1", "OpsCycleReceiptV1", "OpsSupervisorStageCheckpointV1",
    "OpsRiskEvidenceResolutionV1",
    "OpsPublicFinalBarTriggerV1", "CausalBarV2", "OpsDecisionEventIdentityV1",
    "OpsSupervisorReceiptIdentityV1", "M0CalibrationV2", "M0OODV2", "M0SupportV2",
    "M1ModelFitV2", "M1LightGBMBoosterV2", "M1SupportV2", "M1CalibrationV2", "M1OODV2",
    "M1PredictionV2", "AnalogueActionValueV2", "PretradeExecutionScenarioV2",
    "FeeScheduleV2", "FundingScheduleV2", "FundingCashflowV2", "ActionCostContractV2",
    "ReplayAssumptionsV2", "StressSuiteEvidenceV2",
)

SCIENCE_ARTIFACT_SOURCES: dict[str, dict[str, Any]] = {
    "M0_calibration": {"store": "artifact_index", "types": ("M0CalibrationV2",), "supported": True},
    "M0_evaluation": {"store": "artifact_index", "types": ("M0OODV2", "M0SupportV2"), "supported": True},
    "M0_calibration_and_evaluation": {
        "store": "artifact_index", "types": ("M0CalibrationV2", "M0OODV2", "M0SupportV2"),
        "supported": True,
    },
    "M1_model_and_support": {
        "store": "artifact_index", "types": ("M1ModelFitV2", "M1LightGBMBoosterV2", "M1SupportV2",
                                                "M1CalibrationV2", "M1OODV2", "M1PredictionV2"),
        "supported": True,
    },
    "analogue_support": {"store": "artifact_index", "types": ("AnalogueActionValueV2",), "supported": True},
    "pretrade_scenarios": {"store": "artifact_index", "types": ("PretradeExecutionScenarioV2",), "supported": True},
    "execution_assumptions": {"store": "artifact_index", "types": ("ReplayAssumptionsV2", "StressSuiteEvidenceV2"),
                              "supported": True},
    "costs": {"store": "artifact_index", "types": ("FeeScheduleV2", "FundingScheduleV2", "FundingCashflowV2",
                                                       "ActionCostContractV2"), "supported": True},
    "research_calibration": {"store": None, "types": (), "supported": False,
                             "reason": "No ResearchCalibrationV2 producer or accepted typed store exists in this repository."},
    "discovery_experiments": {"store": "artifact_index", "types": ("DiscoveryExperimentV2",), "supported": True},
    "discovery_attempts_and_rejections": {
        "store": "artifact_index", "types": ("DiscoveryAttemptV2", "DiscoveryRejectedAttemptV2"),
        "supported": True,
    },
}

_INVENTORY_PAGE_SIZE = 500
_INVENTORY_MAX_PAGES = 1_000
_INVENTORY_MAX_RETAINED_DETAILS = 10_000
_DETAIL_ONLY_TYPES = frozenset({
    "PublicObservationIndexV2", "PublicSourceHealthV2", "M0CalibrationV2", "M0OODV2", "M0SupportV2",
    "M1ModelFitV2", "M1LightGBMBoosterV2", "M1SupportV2", "M1CalibrationV2", "M1OODV2", "M1PredictionV2",
    "AnalogueActionValueV2", "PretradeExecutionScenarioV2", "FeeScheduleV2", "FundingScheduleV2",
    "FundingCashflowV2", "ActionCostContractV2", "ReplayAssumptionsV2", "StressSuiteEvidenceV2",
})
_SCIENCE_PAYLOAD_KEYS = {
    "M0CalibrationV2": "calibration", "M0OODV2": "ood", "M0SupportV2": "support",
    "M1ModelFitV2": "model_fit", "M1LightGBMBoosterV2": "booster", "M1SupportV2": "support",
    "M1CalibrationV2": "calibration", "M1OODV2": "ood", "M1PredictionV2": "prediction",
    "AnalogueActionValueV2": "analogue", "PretradeExecutionScenarioV2": "scenario",
    "ActionCostContractV2": "cost_contract",
    "DiscoveryExperimentV2": "experiment", "DiscoveryAttemptV2": "attempt",
    "DiscoveryRejectedAttemptV2": "attempt",
}
_SCIENCE_DIRECT_BODY_TYPES = frozenset({
    "FeeScheduleV2", "FundingScheduleV2", "FundingCashflowV2", "ReplayAssumptionsV2", "StressSuiteEvidenceV2",
})
_SCIENCE_EXPECTED_VERSIONS = {
    "M0CalibrationV2": "M0_CHRONOLOGICAL_CALIBRATION_V1", "M0OODV2": "M0_ROBUST_OOD_V1",
    "M0SupportV2": "M0_SUPPORT_V2_V1", "M1ModelFitV2": "M1_MODEL_FIT_V2_V1",
    "M1LightGBMBoosterV2": "M1_LIGHTGBM_BOOSTER_V1", "M1SupportV2": "M1_SUPPORT_V2_V1",
    "M1CalibrationV2": "M1_CALIBRATION_V2_V1", "M1OODV2": "M1_OOD_V2_V1",
    "M1PredictionV2": "M1_PREDICTION_V2_V1", "AnalogueActionValueV2": "ANALOGUE_ACTION_VALUE_V2_V2",
    "PretradeExecutionScenarioV2": "PRETRADE_SCENARIO_ARTIFACT_V2_V5",
    "FeeScheduleV2": "V2_TAKER_FEES_V1", "FundingScheduleV2": "V2_FUNDING_SCHEDULE_V1",
    "FundingCashflowV2": "V2_SETTLED_FUNDING_V1", "ActionCostContractV2": "V2_ACTION_COST_CONTRACT_V1",
    "ReplayAssumptionsV2": "V2_REPLAY_ASSUMPTIONS_V1", "StressSuiteEvidenceV2": "V2_STRESS_SUITE_EVIDENCE_V1",
    "DiscoveryExperimentV2": "DISCOVERY_EXPERIMENT_V2_V2", "DiscoveryAttemptV2": "DISCOVERY_ATTEMPT_V2_V2",
    "DiscoveryRejectedAttemptV2": "DISCOVERY_ATTEMPT_V2_V2",
}
_SCIENCE_EXPECTED_FIELDS = {
    "M0CalibrationV2": {"version", "action_hash", "training_cutoff_ns", "oof_archive_ref",
                        "chronological_oof_count", "absolute_residual_q90", "status", "reason"},
    "M0OODV2": {"version", "action_hash", "feature_vector_ref", "training_row_refs", "robust_z_limit",
                 "maximum_absolute_robust_z", "out_of_distribution", "status"},
    "M0SupportV2": {"version", "action_hash", "information_cutoff_ns", "eligible_sample_count",
                    "independent_support_count", "compatible_policy_count", "provenance_counts",
                    "execution_state_counts", "missing_feature_coverage", "training_start_ns",
                    "training_end_ns", "training_outcome_refs", "compatibility_key", "evidence_quality"},
    "M1ModelFitV2": {"version", "model_id", "model_version", "feature_policy_hash", "feature_policy_body",
                     "model_policy_hash", "model_policy_body", "compatibility_key", "fit_cutoff_ns",
                     "available_at_ns", "training_row_refs", "scaler_fit_row_refs", "validation_row_refs",
                     "selected_parameters", "search_results", "centers", "scales", "booster_ref",
                     "lightgbm_version", "python_version", "platform_id", "dependency_lock_hash", "seed",
                     "thread_count", "objective", "validation_metric", "tie_break", "status", "reasons",
                     "final_holdout_reservation_ref"},
    "M1LightGBMBoosterV2": {"version", "model_text", "lightgbm_version", "seed", "thread_count", "deterministic"},
    "M1SupportV2": {"version", "action_hash", "cutoff_ns", "compatible_training_row_refs",
                    "independent_training_row_refs", "provenance_counts", "execution_state_counts",
                    "missing_feature_counts", "start_ns", "end_ns", "status"},
    "M1CalibrationV2": {"version", "action_hash", "cutoff_ns", "oof_archive_ref", "oof_row_refs",
                        "independent_support", "absolute_residual_q90", "status", "reason"},
    "M1OODV2": {"version", "action_hash", "feature_ref", "training_row_refs", "fitted_center", "fitted_scale",
                "maximum_absolute_robust_z", "threshold", "status"},
    "M1PredictionV2": {"version", "action_hash", "action_artifact_ref", "candidate_ref", "candidate_set_ref",
                       "information_cutoff_ns", "available_at_ns", "model_fit_ref", "feature_vector_ref",
                       "training_row_refs", "oof_archive_ref", "calibration_ref", "support_ref", "ood_ref",
                       "compatibility_key", "expected_net_value", "status", "reasons"},
    "AnalogueActionValueV2": {"version", "policy_id", "policy_hash", "policy_version", "query_action_hash",
                               "query_action_ref", "query_candidate_ref", "query_candidate_set_ref",
                               "information_cutoff_ns", "compatibility_key", "compatible_population_count",
                               "neighbor_refs", "neighbors", "weighted_estimate", "payoff_dispersion",
                               "effective_sample_size", "independent_support_count", "temporal_concentration",
                               "regime_coverage", "missing_features", "explanation_fields", "nearest_distance",
                               "ood_status", "support_status", "reasons", "scaler_training_refs",
                               "scaler_feature_names", "scaler_centers", "scaler_scales"},
    "PretradeExecutionScenarioV2": {"version", "action_hash", "action_artifact_ref", "information_cutoff_ns",
                                     "model_input", "calibration_input", "execution_model_input", "source_inputs",
                                     "template_support_refs", "source_joint_data_refs", "rows", "stress_refs",
                                     "generation_version", "common_scenario_set_id", "created_at_ns",
                                     "computed_at_ns", "available_at_ns", "expires_at_ns", "status",
                                     "synthetic_fixture", "reason", "seed", "scenario_count", "content_hash"},
    "FeeScheduleV2": {"version", "key", "available_at_ns", "entry_taker_rate", "exit_taker_rate", "source_ref"},
    "FundingScheduleV2": {"version", "available_at_ns", "expected_settlement_times_ns", "explicit_zero_funding",
                          "source_ref"},
    "FundingCashflowV2": {"version", "at_ns", "available_at_ns", "signed_rate", "mark_price", "source_ref"},
    "ActionCostContractV2": {"version", "key", "policy_hash", "available_at_ns", "fee_schedule_ref",
                             "funding_schedule_ref", "execution_assumptions_ref", "source_ref"},
    "ReplayAssumptionsV2": {"version", "decision_to_arrival_ns", "human_delay_ns", "stop_latency_ns",
                            "exit_latency_ns", "participation", "exit_impact", "minute_only_arrival",
                            "stop_ohlc_order", "same_timestamp_funding"},
    "StressSuiteEvidenceV2": {"version", "stress_input", "source_refs", "information_cutoff_ns"},
    "DiscoveryExperimentV2": {"version", "experiment_id", "family_id", "hypothesis_family",
                              "allowed_feature_families", "availability_assumptions", "maximum_attempts",
                              "parameter_search_budget", "baseline_policy_ref", "primary_metrics", "chronology",
                              "purge_embargo", "multiplicity_family_id", "stop_rule", "final_holdout_ref",
                              "holdout_state", "prospective_shadow_required", "preregistered_at_ns",
                              "lab_contract_hash"},
    "DiscoveryAttemptV2": {"version", "experiment_ref", "experiment_id", "attempt_id", "attempt_version",
                           "previous_attempt_ref", "proposal_version", "proposal_spec", "proposal_hash",
                           "proposer_type", "proposer_id", "parameters", "started_at_ns", "completed_at_ns",
                           "training_refs", "validation_refs", "outer_refs", "result", "failure_reason",
                           "manual_intervention", "holdout_viewed", "holdout_ref", "credentials_available",
                           "order_tools_available", "risk_mutation_available", "capital_authority",
                           "self_promotion", "state"},
    "DiscoveryRejectedAttemptV2": {"version", "experiment_ref", "experiment_id", "attempt_id", "attempt_version",
                                   "previous_attempt_ref", "proposal_version", "proposal_spec", "proposal_hash",
                                   "proposer_type", "proposer_id", "parameters", "started_at_ns", "completed_at_ns",
                                   "training_refs", "validation_refs", "outer_refs", "result", "failure_reason",
                                   "manual_intervention", "holdout_viewed", "holdout_ref", "credentials_available",
                                   "order_tools_available", "risk_mutation_available", "capital_authority",
                                   "self_promotion", "state"},
}


_READ_PAGE_SIZE = 500
_READ_MAX_PAGES = 200


def _scan_asof_artifacts(
    repository: OpsRepository,
    artifact_type: str,
    *,
    cutoff_ns: int,
    visit: Callable[[ArtifactIndexEntryV2], None],
) -> dict[str, Any]:
    """Visit one eligible artifact namespace in a bounded read-only keyset scan."""
    cursor: tuple[int, str] | None = None
    pages = 0
    processed = 0
    invalid = 0
    complete = False
    reason: str | None = None
    while pages < _READ_MAX_PAGES:
        page = repository.artifact_entries_by_types_page(
            (artifact_type,), as_of_ns=cutoff_ns, after=cursor, limit=_READ_PAGE_SIZE,
        )
        pages += 1
        processed += len(page.entries) + page.invalid_entry_count
        invalid += page.invalid_entry_count
        for entry in page.entries:
            visit(entry)
        if page.next_cursor is None:
            complete = True
            break
        if cursor is not None and page.next_cursor >= cursor:
            reason = "NON_ADVANCING_OR_DUPLICATE_KEYSET_CURSOR"
            break
        cursor = page.next_cursor
    else:
        reason = "PAGE_BUDGET_EXCEEDED"
    return {
        "complete": complete, "reason": reason, "records_processed": processed,
        "pages_processed": pages, "page_size": _READ_PAGE_SIZE,
        "maximum_pages": _READ_MAX_PAGES, "maximum_records": _READ_PAGE_SIZE * _READ_MAX_PAGES,
        "invalid_entry_count": invalid, "as_of_ns": cutoff_ns,
        "snapshot_consistency": "ONE_READ_ONLY_SQLITE_SNAPSHOT",
    }


def _latest_health(
    repository: OpsRepository,
    cutoff_ns: int,
    *,
    required_refs: set[str] | None = None,
) -> tuple[dict[str, PublicSourceHealthV2], dict[str, PublicSourceHealthV2], dict[str, Any]]:
    latest: dict[str, PublicSourceHealthV2] = {}
    exact: dict[str, PublicSourceHealthV2] = {}
    refs = required_refs or set()

    def visit(entry: ArtifactIndexEntryV2) -> None:
        body = entry.metadata.get("health")
        if not isinstance(body, Mapping):
            return
        try:
            item = PublicSourceHealthV2.from_dict(body)
        except (KeyError, TypeError, ValueError):
            return
        if (item.available_at_ns > cutoff_ns or item.observed_at_ns > cutoff_ns
                or item.content_hash != entry.artifact_ref or entry.content_hash != item.content_hash
                or entry.available_at_ns != item.available_at_ns):
            return
        old = latest.get(item.source_id)
        if old is None or (item.observed_at_ns, item.available_at_ns, item.content_hash) > (
                old.observed_at_ns, old.available_at_ns, old.content_hash):
            latest[item.source_id] = item
        if item.content_hash in refs:
            exact[item.content_hash] = item

    inventory = _scan_asof_artifacts(repository, "PublicSourceHealthV2", cutoff_ns=cutoff_ns, visit=visit)
    for item in latest.values():
        exact.setdefault(item.content_hash, item)
    return latest, exact, inventory


def _latest_persisted_event_gate(
    repository: OpsRepository, cutoff_ns: int,
) -> tuple[EventGate | None, str | None, dict[str, Any]]:
    latest: tuple[ArtifactIndexEntryV2, EventSafetyGateV2] | None = None

    def visit(entry: ArtifactIndexEntryV2) -> None:
        nonlocal latest
        body = entry.metadata.get("gate")
        if not isinstance(body, Mapping):
            return
        try:
            envelope_body = body.get("envelope")
            if not isinstance(envelope_body, Mapping):
                return
            gate = EventSafetyGateV2(
                ArtifactEnvelope.from_dict(json_value(envelope_body)),
                body["cutoff_ns"], EventGateStateV2(body["state"]), body["blocked"],
                tuple(body["reasons"]), body.get("scheduled_event_ref"), body.get("schedule_revision"),
                body.get("calendar_source_id"), AbnormalityStateV2(body["abnormality_state"]),
                tuple(body["incident_refs"]), body["gate_version"],
            )
        except (ArithmeticError, KeyError, TypeError, ValueError):
            return
        if (entry.artifact_ref != gate.content_hash or entry.content_hash != gate.content_hash
                or entry.available_at_ns != gate.envelope.available_at_ns
                or gate.envelope.available_at_ns > cutoff_ns or gate.cutoff_ns > cutoff_ns
                or body.get("availability_view") != "ACTUAL_SYSTEM"
                or canonical_json(gate.to_dict()) != canonical_json(body)):
            return
        if latest is None or (entry.available_at_ns, entry.artifact_ref) > (
                latest[0].available_at_ns, latest[0].artifact_ref):
            latest = entry, gate

    inventory = _scan_asof_artifacts(repository, "EventSafetyGateV2", cutoff_ns=cutoff_ns, visit=visit)
    if latest is None:
        return None, None, inventory
    gate = latest[1].to_s1_event_gate()
    return gate, latest[0].artifact_ref, inventory


def _persisted_feature_readiness(
    repository: OpsRepository, key: InstrumentKeyV2, cutoff_ns: int,
) -> tuple[bool, bool, tuple[str, ...], dict[str, Any]]:
    candidates: list[tuple[ArtifactIndexEntryV2, Mapping[str, Any]]] = []

    def visit(entry: ArtifactIndexEntryV2) -> None:
        body = entry.metadata.get("feature")
        raw_key = body.get("key") if isinstance(body, Mapping) else None
        values = body.get("values") if isinstance(body, Mapping) else None
        replay_view = body.get("replay_view") if isinstance(body, Mapping) else None
        if (not isinstance(raw_key, Mapping) or dict(raw_key) != key.to_dict()
                or replay_view != "ACTUAL_SYSTEM" or not isinstance(values, Mapping)):
            return
        candidates.append((entry, values))
    inventory = _scan_asof_artifacts(repository, "FeatureArtifactV2", cutoff_ns=cutoff_ns, visit=visit)
    if not candidates:
        return False, False, (), inventory
    entry, values = max(candidates, key=lambda row: (row[0].available_at_ns, row[0].artifact_ref))

    def present(name: str) -> bool:
        value = values.get(name)
        return isinstance(value, Mapping) and value.get("value") is not None

    s1 = present("m15.atr14") and present("m15.realized_variance20")
    s2 = present("m15.atr14") and present("m15.bollinger_width20")
    return s1, s2, (entry.artifact_ref,), inventory


def _universe_eligible(
    repository: OpsRepository, key: InstrumentKeyV2, cutoff_ns: int,
) -> tuple[bool, dict[str, Any]]:
    candidates: list[tuple[ArtifactIndexEntryV2, UniverseContractV2]] = []

    def visit(entry: ArtifactIndexEntryV2) -> None:
        body = entry.metadata.get("universe")
        if not isinstance(body, Mapping):
            return
        try:
            universe = UniverseContractV2.from_dict(json_value(body))
        except (KeyError, TypeError, ValueError):
            return
        if (entry.artifact_ref != entry.content_hash or universe.content_hash != entry.artifact_ref
                or entry.available_at_ns != universe.envelope.available_at_ns
                or universe.envelope.available_at_ns > cutoff_ns):
            return
        candidates.append((entry, universe))

    inventory = _scan_asof_artifacts(repository, "UniverseContractV2", cutoff_ns=cutoff_ns, visit=visit)
    for _entry, universe in sorted(candidates, key=lambda row: (row[0].available_at_ns, row[0].artifact_ref),
                                   reverse=True):
        match = next((item for item in universe.entries if item.key == key), None)
        if match is not None:
            eligible = bool(match.data_eligible and match.scanner_eligible and not match.capital_eligible
                            and all(match.strategy_eligibility.get(name) is not None
                                    and match.strategy_eligibility[name].status.value == "ELIGIBLE"
                                    for name in ("S1_MTF_TREND_PULLBACK", "S2_COMPRESSION_BREAKOUT",
                                                 "S3_VWAP_STAT_MEAN_REVERSION")))
            return eligible and inventory["complete"], inventory
    return False, inventory


def _indexed_instrument_artifacts(
    repository: OpsRepository,
    artifact_type: str,
    payload_key: str,
    key: InstrumentKeyV2,
    cutoff_ns: int,
) -> tuple[tuple[ArtifactIndexEntryV2, ...], dict[str, Any]]:
    entries: list[ArtifactIndexEntryV2] = []

    def visit(entry: ArtifactIndexEntryV2) -> None:
        body = entry.metadata.get(payload_key)
        raw_key = body.get("key") if isinstance(body, Mapping) else None
        if isinstance(raw_key, Mapping) and dict(raw_key) == key.to_dict():
            entries.append(entry)

    inventory = _scan_asof_artifacts(repository, artifact_type, cutoff_ns=cutoff_ns, visit=visit)
    inventory["matching_instrument_entries_retained"] = len(entries)
    return tuple(entries), inventory


def _readiness_for_product(repository: OpsRepository, product: ProductContractV2, cutoff_ns: int) -> dict[str, Any]:
    archive_root = Path(repository.path).parent / "ops-observations"
    bars: dict[BarIntervalV2, tuple[Any, ...]] = {}
    bar_refs: set[str] = set()
    for interval in (BarIntervalV2.M1, BarIntervalV2.M15, BarIntervalV2.H1, BarIntervalV2.H4):
        try:
            rows = reconstruct_causal_bars_from_archive(
                repository, archive_root, key=product.key, interval=interval,
                information_cutoff_ns=cutoff_ns, availability_class=AvailabilityClassV2.ACTUAL_SYSTEM,
                limit=100_000,
            )
        except (OSError, ValueError, RuntimeError):
            rows = ()
        bars[interval] = tuple(item.bar for item in rows)
    for interval, tail_size in ((BarIntervalV2.M1, 10_081), (BarIntervalV2.M15, 2_901),
                                (BarIntervalV2.H1, 50), (BarIntervalV2.H4, 50)):
        bar_refs.update(bar.content_hash for bar in bars[interval][-tail_size:])

    vwap_entries, vwap_inventory = _indexed_instrument_artifacts(
        repository, "S3TradeVwapSnapshotV2", "vwap", product.key, cutoff_ns,
    )
    residual_entries, residual_inventory = _indexed_instrument_artifacts(
        repository, "S3ResidualObservationV2", "residual", product.key, cutoff_ns,
    )
    vwap_health_refs = {
        str(body["source_health_ref"])
        for entry in vwap_entries
        if isinstance((body := entry.metadata.get("vwap")), Mapping)
        and isinstance(body.get("source_health_ref"), str)
    }
    health, exact_health, health_inventory = _latest_health(
        repository, cutoff_ns, required_refs=vwap_health_refs,
    )
    latest_source = next((bar.raw.source_id for bar in reversed(bars[BarIntervalV2.M15])), None)
    bar_health = health.get(latest_source or "")
    required_bar_sources = {
        bar.raw.source_id
        for interval, tail_size in ((BarIntervalV2.M1, 10_081), (BarIntervalV2.M15, 2_901),
                                    (BarIntervalV2.H1, 50), (BarIntervalV2.H4, 50))
        for bar in bars[interval][-tail_size:]
    }
    source_ok = health_inventory["complete"] and bool(required_bar_sources) and all(
        source_id in health and health[source_id].data_eligible
        and cutoff_ns - health[source_id].available_at_ns <= 60_000_000_000
        for source_id in required_bar_sources
    )
    quote, mark, quote_refs = _indexed_quote_and_mark(repository, archive_root, product, cutoff_ns=cutoff_ns)
    gate, gate_ref, event_gate_inventory = _latest_persisted_event_gate(repository, cutoff_ns)
    s1_feature, s2_feature, feature_refs, feature_inventory = _persisted_feature_readiness(
        repository, product.key, cutoff_ns,
    )
    watch_rows = repository.list_watches(limit=10_000)
    watches = [item for item in watch_rows
               if item.key == product.key and item.strategy_id == "S1_MTF_TREND_PULLBACK"
               and item.created_at_ns <= cutoff_ns]
    watch_ids = {item.watch_id for item in watches}
    transition_rows = repository.watch_transition_history(limit=10_000)
    transitions = [item for item in transition_rows
                   if item.get("watch_id") in watch_ids and item.get("transition_at_ns", cutoff_ns + 1) <= cutoff_ns]
    watch_inventory_complete = len(watch_rows) < 10_000
    transition_inventory_complete = len(transition_rows) < 10_000
    transitions_by_watch: dict[str, list[dict[str, Any]]] = {}
    for item in transitions:
        watch_id = str(item["watch_id"])
        transitions_by_watch.setdefault(watch_id, []).append(dict(item))
    active_states = {"DETECTED", "WAITING_FOR_EVENT", "READY_FOR_RECHECK", "CONFIRMED"}
    as_of_watch_states: dict[str, str] = {}
    unknown_watch_state_count = 0
    for watch in watches:
        history = transitions_by_watch.get(watch.watch_id, [])
        if history:
            as_of_watch_states[watch.watch_id] = str(history[-1].get("target_state", "UNKNOWN"))
        elif watch.state_version == 0:
            as_of_watch_states[watch.watch_id] = watch.state.value
        else:
            unknown_watch_state_count += 1
    known_active_count = sum(state in active_states for state in as_of_watch_states.values())
    active_watch_known = unknown_watch_state_count == 0
    active_watch = known_active_count > 0
    subsequent_close = any(
        type(item.get("event_at_ns")) is int
        and item["event_at_ns"] > next(watch.created_at_ns for watch in watches if watch.watch_id == item["watch_id"])
        for item in transitions
    )
    trade_count = 0
    trade_sources: set[str] = set()

    def count_public_trade(entry: ArtifactIndexEntryV2) -> None:
        nonlocal trade_count
        metadata = entry.metadata
        if (metadata.get("instrument_revision") == product.key.contract_revision
                and metadata.get("event_type") in ("TRADE", "AGG_TRADE")
                and metadata.get("availability_class") == "ACTUAL_SYSTEM"):
            trade_count += 1
            source_id = metadata.get("source_id")
            if isinstance(source_id, str):
                trade_sources.add(source_id)

    trade_inventory = _scan_asof_artifacts(
        repository, "PublicObservationIndexV2", cutoff_ns=cutoff_ns, visit=count_public_trade,
    )
    trade_health = next((health[source_id] for source_id in sorted(trade_sources)
                         if source_id in health and health[source_id].data_eligible
                         and cutoff_ns - health[source_id].available_at_ns <= 60_000_000_000), None)
    trade_health_current = health_inventory["complete"] and bool(trade_sources) and all(
        source_id in health and health[source_id].data_eligible
        and cutoff_ns - health[source_id].available_at_ns <= 60_000_000_000
        for source_id in trade_sources
    )

    vwap_refs: list[str] = []
    for entry in vwap_entries:
        body = entry.metadata.get("vwap")
        if isinstance(body, Mapping) and body.get("replay_view") == "ACTUAL_SYSTEM":
            vwap_refs.append(entry.artifact_ref)
    vwap_ref_set = set(vwap_refs)
    residuals: list[tuple[int, float, str, str]] = []
    for entry in residual_entries:
        body = entry.metadata.get("residual")
        raw_key = body.get("key") if isinstance(body, Mapping) else None
        value = body.get("residual") if isinstance(body, Mapping) else None
        close = body.get("close_at_ns") if isinstance(body, Mapping) else None
        vwap_ref = body.get("vwap_ref") if isinstance(body, Mapping) else None
        if (isinstance(raw_key, Mapping) and dict(raw_key) == product.key.to_dict() and isinstance(body, Mapping)
                and body.get("replay_view") == "ACTUAL_SYSTEM" and entry.available_at_ns <= cutoff_ns
                and isinstance(value, (int, float)) and not isinstance(value, bool)
                and type(close) is int and close <= cutoff_ns):
            residuals.append((close, float(value), entry.artifact_ref, str(vwap_ref or "")))
    residuals.sort()
    universe_eligible, universe_inventory = _universe_eligible(repository, product.key, cutoff_ns)
    snapshot = StrategyEvidenceSnapshotV1(
        product.key, cutoff_ns, bars, source_ok, trade_health_current, quote, mark, gate,
        universe_eligible, s1_feature, active_watch,
        subsequent_close, s2_feature, trade_count,
        sum(row[3] in vwap_ref_set for row in residuals),
        tuple(row[1] for row in residuals), tuple(row[2] for row in residuals),
        (),
        tuple(row[0] for row in residuals), tuple(row[3] for row in residuals if row[3] in vwap_ref_set),
    )
    report = evaluate_strategy_readiness_v1(snapshot)
    try:
        trades, _trade_health, _trade_refs = _indexed_s3_trades(
            repository, archive_root, product.key, cutoff_ns=cutoff_ns, health_by_source=health,
        )
    except (OSError, RuntimeError, ValueError):
        trades = ()
    s3_lineage = validate_s3_persisted_lineage_v1(
        key=product.key,
        cutoff_ns=cutoff_ns,
        bars=bars[BarIntervalV2.M1][-10_081:],
        vwap_entries=vwap_entries,
        residual_entries=residual_entries,
        trades=trades,
        source_health=tuple(exact_health.values()),
        quote=quote,
        event_gate=gate,
    ).to_dict()
    evidence_scans = {
        "public_trade_observations": trade_inventory,
        "source_health": health_inventory,
        "features": feature_inventory,
        "universe": universe_inventory,
        "s3_trade_vwap": vwap_inventory,
        "s3_residuals": residual_inventory,
        "event_gate": event_gate_inventory,
    }
    complete_scans = all(item["complete"] for item in evidence_scans.values())
    s3_lineage["inventory_status"] = "COMPLETE" if complete_scans else "INCOMPLETE"
    s3_lineage["inventory_scans"] = evidence_scans
    s3_reasons = set(report["sleeves"]["S3"]["reason_codes"])
    s3_reasons.add("S3_TRADE_CONTINUITY_UNPROVEN_BY_BOUNDED_REST_RECENT_TRADE_HISTORY")
    if not complete_scans:
        s3_reasons.add("S3_PERSISTED_EVIDENCE_INVENTORY_INCOMPLETE")
        s3_lineage["lineage_status"] = "INCOMPLETE"
        s3_lineage["qualification_status"] = "NOT ESTIMABLE"
        s3_lineage["reason_codes"] = sorted(set(s3_lineage["reason_codes"]) |
                                             {"S3_PERSISTED_EVIDENCE_INVENTORY_INCOMPLETE"})
    report["sleeves"]["S3"]["status"] = "NOT_ESTIMABLE"
    report["sleeves"]["S3"]["reason_codes"] = sorted(s3_reasons)
    report["sleeves"]["S3"]["observed_counts"].update({
        "persisted_lineage_observations": s3_lineage["validated_observations"],
        "persisted_lineage_expected_observations": s3_lineage["expected_observations"],
        "strictly_preceding_standardization_residuals": s3_lineage["strictly_preceding_residuals"],
        "validated_trade_vwap_artifacts": s3_lineage["validated_vwap_artifacts"],
        "observed_persisted_trade_records": s3_lineage["observed_trade_records"],
    })
    if not watch_inventory_complete or not transition_inventory_complete:
        report["sleeves"]["S1"]["status"] = "NOT_ESTIMABLE"
        report["sleeves"]["S1"]["reason_codes"] = sorted(
            set(report["sleeves"]["S1"]["reason_codes"]) | {"S1_WATCH_HISTORY_INVENTORY_INCOMPLETE"}
        )
    report["s3_persisted_lineage"] = s3_lineage
    report["evidence_refs"] = sorted(bar_refs | set(quote_refs) | set(feature_refs) | set(vwap_refs)
                                     | {row[2] for row in residuals}
                                     | ({gate_ref} if gate_ref is not None else set()))
    report["watch_state"] = {"active_watch_present": active_watch,
                              "active_watch_state_known": active_watch_known,
                              "watch_inventory_complete": watch_inventory_complete,
                              "transition_inventory_complete": transition_inventory_complete,
                              "active_watch_count_known": known_active_count,
                              "unknown_watch_state_count": unknown_watch_state_count,
                              "watch_count": len(watches), "transition_count": len(transitions),
                              "subsequent_confirmed_close_evidence": subsequent_close}
    report["source_health"] = {"bar_source_id": latest_source,
                               "bar_state": bar_health.state.value if bar_health else "UNKNOWN",
                               "trade_source_state": trade_health.state.value if trade_health else "UNKNOWN"}
    return report


def _read_only_resources(repository: OpsRepository) -> dict[str, Any]:
    sample: dict[str, Any] = {"platform": platform.system(), "python": platform.python_version(),
                              "sampling_failures": []}
    try:
        sample["cpu_load_1m"] = os.getloadavg()[0]
    except (AttributeError, OSError) as exc:
        sample["sampling_failures"].append(f"CPU:{type(exc).__name__}")
    try:
        status = Path("/proc/self/status").read_text(encoding="ascii")
        rss_line = next(line for line in status.splitlines() if line.startswith("VmHWM:"))
        sample["process_max_rss_kib"] = int(rss_line.split()[1])
    except (OSError, StopIteration, ValueError, IndexError):
        sample["process_max_rss_kib"] = None
        sample["sampling_failures"].append("RSS:UNAVAILABLE")
    try:
        usage = shutil.disk_usage(Path(repository.path).parent)
        sample["disk_bytes"] = {"total": usage.total, "used": usage.used, "free": usage.free}
        stat = repository._connection.execute("PRAGMA page_count").fetchone()
        page_size = repository._connection.execute("PRAGMA page_size").fetchone()
        journal = repository._connection.execute("PRAGMA journal_mode").fetchone()
        sync_mode = repository._connection.execute("PRAGMA synchronous").fetchone()
        sample["ops_db_bytes"] = int(stat[0]) * int(page_size[0])
        sample["sqlite_journal_mode"] = str(journal[0]).lower()
        sample["sqlite_synchronous_mode"] = int(sync_mode[0])
        sample["wal_bytes"] = Path(repository.path + "-wal").stat().st_size if Path(repository.path + "-wal").exists() else None
    except (OSError, AttributeError, TypeError, ValueError) as exc:
        sample["sampling_failures"].append(f"DISK_OR_DB:{type(exc).__name__}")
    sample["cycle_duration_ns"] = None
    sample["queue_occupancy"] = None
    sample["processing_latency_ns"] = None
    sample["resource_sampling_status"] = "UNVERIFIED" if sample["sampling_failures"] else "TESTED"
    sample["host_resource_budget_qualification"] = "UNVERIFIED"
    return sample


def _read_bounded_inventory(
    repository: OpsRepository,
    *,
    as_of_ns: int,
    page_size: int,
    max_pages: int,
    campaign_start_ns: int | None,
) -> tuple[dict[str, list[Any]], Counter[str], dict[str, Any], dict[str, Any]]:
    """Stream a stable typed snapshot, retaining only bounded detail rows."""
    by_type: dict[str, list[Any]] = defaultdict(list)
    type_counts: Counter[str] = Counter()
    retained_per_type: Counter[str] = Counter()
    observed_sources: Counter[str] = Counter()
    observed_instruments: Counter[str] = Counter()
    observed_events: Counter[str] = Counter()
    chronology_sample: deque[dict[str, Any]] = deque(maxlen=200)
    latest_bar: dict[str, tuple[int, str]] = {}
    science_valid_counts: Counter[str] = Counter()
    science_invalid_counts: Counter[str] = Counter()
    genuine_forward = 0
    reconstructed = 0
    revision_count = 0
    pages = 0
    processed = 0
    invalid = 0
    duplicate_or_order_errors = 0
    retained = 0
    cursor: tuple[int, str] | None = None
    complete = False
    reason: str | None = None

    while pages < max_pages:
        page = repository.artifact_entries_by_types_page(
            _REPORT_TYPES, as_of_ns=as_of_ns, after=cursor, limit=page_size,
        )
        pages += 1
        raw_page_count = len(page.entries) + page.invalid_entry_count
        processed += raw_page_count
        invalid += page.invalid_entry_count
        if raw_page_count == 0:
            complete = True
            break
        if page.next_cursor is None or (cursor is not None and page.next_cursor >= cursor):
            duplicate_or_order_errors += 1
            reason = "NON_ADVANCING_OR_DUPLICATE_KEYSET_CURSOR"
            break
        cursor = page.next_cursor
        for item in page.entries:
            type_counts[item.artifact_type] += 1
            if item.artifact_type in _SCIENCE_PAYLOAD_KEYS or item.artifact_type in _SCIENCE_DIRECT_BODY_TYPES:
                payload_key = _SCIENCE_PAYLOAD_KEYS.get(item.artifact_type)
                payload = item.metadata.get(payload_key) if payload_key is not None else item.metadata
                payload_hash_matches = isinstance(payload, Mapping) and sha256_json(payload) == item.content_hash
                if (item.artifact_type == "PretradeExecutionScenarioV2" and isinstance(payload, Mapping)
                        and payload.get("content_hash") == item.content_hash):
                    content_body = dict(payload)
                    content_body.pop("content_hash", None)
                    payload_hash_matches = sha256_json(content_body) == item.content_hash
                if (isinstance(payload, Mapping)
                        and payload.get("version") == _SCIENCE_EXPECTED_VERSIONS.get(item.artifact_type)
                        and set(payload) == _SCIENCE_EXPECTED_FIELDS.get(item.artifact_type, set())
                        and payload_hash_matches
                        and item.artifact_ref == item.content_hash):
                    science_valid_counts[item.artifact_type] += 1
                else:
                    science_invalid_counts[item.artifact_type] += 1
            if item.artifact_type == "PublicObservationIndexV2":
                metadata = item.metadata
                event_type = str(metadata.get("event_type", "UNKNOWN"))
                observed_sources[str(metadata.get("source_id", "UNKNOWN"))] += 1
                observed_instruments[str(metadata.get("instrument_revision", "UNKNOWN"))] += 1
                observed_events[event_type] += 1
                row = {
                    "ref": item.artifact_ref,
                    "record_id": metadata.get("record_id"),
                    "source_id": metadata.get("source_id"),
                    "event_type": metadata.get("event_type"),
                    "event_at_ns": metadata.get("event_at_ns"),
                    "published_at_ns": metadata.get("published_at_ns"),
                    "received_at_ns": item.created_at_ns,
                    "available_at_ns": item.available_at_ns,
                    "availability_class": metadata.get("availability_class"),
                    "bar_content_hash": metadata.get("bar_content_hash"),
                    "revision_of": metadata.get("revision_of"),
                    "instrument_revision": metadata.get("instrument_revision"),
                }
                chronology_sample.append(row)
                if metadata.get("revision_of") is not None:
                    revision_count += 1
                if (campaign_start_ns is not None and item.created_at_ns >= campaign_start_ns
                        and type(metadata.get("event_at_ns")) is int
                        and metadata["event_at_ns"] >= campaign_start_ns):
                    genuine_forward += 1
                if metadata.get("availability_class") == "RECONSTRUCTED_MARKET":
                    reconstructed += 1
                for interval in BarIntervalV2:
                    if (event_type == f"BAR_{interval.value}"
                            and isinstance(metadata.get("bar_content_hash"), str)):
                        candidate = (item.available_at_ns, str(metadata["bar_content_hash"]))
                        if candidate > latest_bar.get(interval.value, (-1, "")):
                            latest_bar[interval.value] = candidate
                continue
            if item.artifact_type in _DETAIL_ONLY_TYPES:
                continue
            if retained < _INVENTORY_MAX_RETAINED_DETAILS:
                by_type[item.artifact_type].append(item)
                retained_per_type[item.artifact_type] += 1
                retained += 1
    else:
        reason = "PAGE_BUDGET_EXCEEDED"

    if not complete and reason is None:
        reason = "INVENTORY_INCOMPLETE"
    overflow_types = sorted(
        name for name, count in type_counts.items()
        if name not in _DETAIL_ONLY_TYPES and retained_per_type[name] < count
    )
    observation = {
        "source_counts": observed_sources,
        "instrument_counts": observed_instruments,
        "event_counts": observed_events,
        "chronology": sorted(chronology_sample,
                             key=lambda row: (row["received_at_ns"], row["ref"]))[-200:],
        "latest_bar_ref_by_interval": {key: value[1] for key, value in latest_bar.items()},
        "genuine_forward_count": genuine_forward,
        "reconstructed_count": reconstructed,
        "revision_count": revision_count,
    }
    inventory = {
        "complete": complete,
        "reason": reason,
        "as_of_ns": as_of_ns,
        "snapshot_consistency": "ONE_READ_ONLY_SQLITE_SNAPSHOT",
        "ordering": ["created_at_ns DESC", "artifact_ref DESC"],
        "records_processed": processed,
        "pages_processed": pages,
        "page_size": page_size,
        "resource_budget": {
            "maximum_pages": max_pages,
            "maximum_records": max_pages * page_size,
            "maximum_retained_detail_entries": _INVENTORY_MAX_RETAINED_DETAILS,
        },
        "duplicate_or_order_error_count": duplicate_or_order_errors,
        "invalid_entry_count": invalid,
        "science_payload_valid_counts": dict(science_valid_counts),
        "science_payload_invalid_counts": dict(science_invalid_counts),
        "retained_detail_entries": retained,
        "detail_overflow_artifact_types": overflow_types,
        "detail_completeness": not overflow_types,
    }
    return dict(by_type), type_counts, inventory, observation


def _validate_public_handoff(
    repository: OpsRepository, entry: Any, *, as_of_ns: int,
) -> tuple[dict[str, Any] | None, str | None]:
    """Resolve an event through exact trigger, observation, bar, and product refs."""
    body = entry.metadata.get("event")
    if not isinstance(body, Mapping):
        return None, "EVENT_BODY_MISSING_OR_MALFORMED"
    try:
        event = decision_event_from_dict(body)
    except (KeyError, TypeError, ValueError):
        return None, "EVENT_CONTRACT_INVALID"
    if (event.content_hash != entry.artifact_ref or entry.content_hash != event.content_hash
            or event.available_at_ns > as_of_ns or entry.available_at_ns > as_of_ns):
        return None, "EVENT_IDENTITY_OR_AVAILABILITY_CONFLICT"
    expected_event_id = sha256_json({
        "version": "OPS_DECISION_EVENT_FROM_FINAL_BAR_V1", "trigger_ref": event.trigger_ref,
    })
    if event.event_id != expected_event_id or event.event_type != "CONFIRMED_15M_CLOSE":
        return None, "EVENT_TRIGGER_IDENTITY_MISMATCH"
    trigger_entry = repository.get_artifact(event.trigger_ref)
    trigger = trigger_entry.metadata.get("trigger") if trigger_entry is not None else None
    if (trigger_entry is None or trigger_entry.artifact_type != "OpsPublicFinalBarTriggerV1"
            or trigger_entry.artifact_ref != event.trigger_ref or trigger_entry.content_hash != event.trigger_ref
            or not isinstance(trigger, Mapping) or sha256_json(trigger) != event.trigger_ref
            or trigger.get("version") != "OPS_PUBLIC_FINAL_BAR_TRIGGER_V1"):
        return None, "TRIGGER_REFERENCE_MISSING_OR_CONFLICTING"
    record_id = entry.metadata.get("trigger_record_id")
    observation_ref = trigger.get("source_observation_ref")
    observation_entry = repository.get_artifact(str(observation_ref)) if isinstance(observation_ref, str) else None
    observation = observation_entry.metadata if observation_entry is not None else None
    if (observation_entry is None or observation_entry.artifact_type != "PublicObservationIndexV2"
            or observation_entry.artifact_ref != observation_ref or not isinstance(observation, Mapping)
            or not isinstance(record_id, str) or observation.get("record_id") != record_id
            or sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": record_id}) != observation_ref):
        return None, "SOURCE_OBSERVATION_REFERENCE_MISSING_OR_CONFLICTING"
    bar_ref = trigger.get("bar_ref")
    bar_entry = repository.get_artifact(str(bar_ref)) if isinstance(bar_ref, str) else None
    bar = bar_entry.metadata.get("bar") if bar_entry is not None else None
    if (bar_entry is None or bar_entry.artifact_type != "CausalBarV2" or bar_entry.artifact_ref != bar_ref
            or bar_entry.content_hash != bar_ref or not isinstance(bar, Mapping) or sha256_json(bar) != bar_ref):
        return None, "CONFIRMED_BAR_REFERENCE_MISSING_OR_CONFLICTING"
    product_ref = trigger.get("product_ref")
    product_entry = repository.get_artifact(str(product_ref)) if isinstance(product_ref, str) else None
    product_body = product_entry.metadata.get("product") if product_entry is not None else None
    try:
        product = ProductContractV2.from_dict(json_value(product_body)) if isinstance(product_body, Mapping) else None
    except (KeyError, TypeError, ValueError):
        product = None
    if (product is None or product_entry is None or product_entry.artifact_type != "ProductContractV2"
            or product_entry.artifact_ref != product_ref or product.content_hash != product_ref):
        return None, "PRODUCT_REFERENCE_MISSING_OR_CONFLICTING"
    try:
        key_body = json.loads(str(observation.get("instrument_key_json")))
        observation_key = InstrumentKeyV2.from_dict(key_body)
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        return None, "SOURCE_INSTRUMENT_KEY_INVALID"
    origin_ns = bar.get("close_at_ns")
    opened_ns = bar.get("open_at_ns")
    event_at_ns = observation.get("event_at_ns")
    received_ns = observation_entry.created_at_ns
    available_ns = observation_entry.available_at_ns
    chronology_ok = (
        type(origin_ns) is int and type(opened_ns) is int and bar.get("interval") == BarIntervalV2.M15.value
        and bar.get("final") is True and origin_ns == opened_ns + BarIntervalV2.M15.duration_ns
        and origin_ns % BarIntervalV2.M15.duration_ns == 0
        and bar.get("record_id") == record_id
        and bar.get("instrument_revision") == product.key.contract_revision
        and observation_key == product.key
        and observation.get("instrument_revision") == product.key.contract_revision
        and observation.get("bar_content_hash") == bar_ref
        and observation.get("availability_class") == AvailabilityClassV2.ACTUAL_SYSTEM.value
        and observation.get("source_id") == event.source_id == trigger.get("source_id")
        and event.source_event_at_ns == trigger.get("source_event_at_ns") == event_at_ns
        and event.source_published_at_ns == trigger.get("source_published_at_ns")
        and event.source_published_at_ns == observation.get("published_at_ns")
        and event.received_at_ns == trigger.get("received_at_ns") == received_ns
        and event.available_at_ns == trigger.get("available_at_ns") == available_ns
        and event.information_cutoff_ns == trigger.get("information_cutoff_ns")
        and event.information_cutoff_ns >= available_ns
        and entry.available_at_ns == event.information_cutoff_ns
        and event.information_cutoff_ns >= max(
            trigger_entry.available_at_ns, observation_entry.available_at_ns,
            bar_entry.available_at_ns, product_entry.available_at_ns,
        )
        and {event.trigger_ref, observation_ref, bar_ref, product_ref}.issubset(event.causal_input_refs)
        and origin_ns <= available_ns
        and available_ns <= as_of_ns
    )
    if not chronology_ok:
        return None, "TRIGGER_BAR_INSTRUMENT_OR_CHRONOLOGY_MISMATCH"

    identity_ref = sha256_json({"artifact_type": "OpsSupervisorReceiptIdentityV1", "event_id": event.event_id})
    identity = repository.get_artifact(identity_ref)
    receipt_ref = identity.metadata.get("receipt_ref") if identity is not None else None
    receipt_entry = repository.get_artifact(str(receipt_ref)) if isinstance(receipt_ref, str) else None
    receipt = receipt_entry.metadata.get("receipt") if receipt_entry is not None else None
    processed_at_ns = None
    terminal_status = None
    if (identity is not None and identity.artifact_type == "OpsSupervisorReceiptIdentityV1"
            and identity.metadata.get("event_id") == event.event_id and isinstance(receipt, Mapping)
            and receipt_entry is not None and receipt_entry.artifact_type == "OpsSupervisorReceiptV1"
            and receipt_entry.available_at_ns <= as_of_ns
            and receipt_entry.created_at_ns == receipt["created_at_ns"]
            and receipt_entry.available_at_ns == receipt["created_at_ns"]
            and identity.content_hash == sha256_json({"event_id": event.event_id, "receipt_ref": receipt_ref})
            and receipt_entry.artifact_ref == sha256_json({
                "artifact_type": "OpsSupervisorReceiptV1", "content_hash": receipt_entry.content_hash,
            })
            and sha256_json(receipt) == receipt_entry.content_hash
            and isinstance(receipt.get("decision_event"), Mapping)
            and canonical_json(receipt["decision_event"]) == canonical_json(event.to_dict())
            and type(receipt.get("created_at_ns")) is int):
        processed_at_ns = int(receipt["created_at_ns"])
        terminal_status = receipt.get("terminal_status")
    status = "HANDOFF_PERSISTED_UNRESOLVED"
    if processed_at_ns is not None:
        status = "TIMELY_PROCESSED" if processed_at_ns <= event.deadline_ns else "INELIGIBLE_LATE_PROCESSING"
    elif as_of_ns > event.deadline_ns:
        status = "EXPIRED_UNRESOLVED_DECISION"
    return {
        "event_ref": entry.artifact_ref,
        "event_id": event.event_id,
        "trigger_ref": event.trigger_ref,
        "source_observation_ref": observation_ref,
        "bar_ref": bar_ref,
        "product_ref": product_ref,
        "instrument": product.key.to_dict(),
        "instrument_revision": product.key.contract_revision,
        "bar_close_decision_origin_ns": origin_ns,
        "information_cutoff_ns": event.information_cutoff_ns,
        "actual_received_at_ns": event.received_at_ns,
        "actual_available_at_ns": event.available_at_ns,
        "processing_at_ns": processed_at_ns,
        "expiration_deadline_ns": event.deadline_ns,
        "decision_status": status,
        "terminal_status": terminal_status,
        "missing_source_evidence": [],
        "missing_decision_handoff": False,
        "unresolved_or_ineligible_decision": status != "TIMELY_PROCESSED",
        "replay_view": observation.get("availability_class"),
    }, None


def _decision_origin_inventory(
    repository: OpsRepository,
    *,
    by_type: dict[str, list[Any]],
    as_of_ns: int,
    campaign_start_ns: int | None,
    detail_overflow_types: set[str],
) -> dict[str, Any]:
    products: list[ProductContractV2] = []
    invalid_products = 0
    for item in by_type.get("ProductContractV2", []):
        body = item.metadata.get("product")
        try:
            product = ProductContractV2.from_dict(json_value(body)) if isinstance(body, Mapping) else None
        except (KeyError, TypeError, ValueError):
            product = None
        if product is not None and product.content_hash == item.artifact_ref == item.content_hash:
            products.append(product)
        else:
            invalid_products += 1
    products_by_key: dict[InstrumentKeyV2, ProductContractV2] = {}
    for product in sorted(products, key=lambda item: (item.available_at_ns, item.content_hash), reverse=True):
        products_by_key.setdefault(product.key, product)
    valid_handoffs: dict[tuple[InstrumentKeyV2, int], list[dict[str, Any]]] = defaultdict(list)
    invalid: list[dict[str, Any]] = []
    for item in by_type.get("OpsDecisionEventSourceV1", []):
        handoff, error = _validate_public_handoff(repository, item, as_of_ns=as_of_ns)
        if handoff is None:
            invalid.append({"event_ref": item.artifact_ref, "reason": error,
                            "source_evidence_ref": item.metadata.get("trigger_record_id")})
            continue
        key = InstrumentKeyV2.from_dict(handoff["instrument"])
        handoff_slot_key = (key, int(handoff["bar_close_decision_origin_ns"]))
        valid_handoffs[handoff_slot_key].append(handoff)

    duplicate_count = 0
    for handoffs in valid_handoffs.values():
        ordered = sorted(handoffs, key=lambda item: item["event_ref"])
        duplicate_count += max(0, len(ordered) - 1)
        for index, handoff in enumerate(ordered):
            handoff["duplicate_handoff"] = index > 0
        handoffs[:] = ordered

    per_instrument: dict[str, dict[str, Any]] = {}
    missing_slots: list[dict[str, Any]] = []
    slot_complete = True
    detail_complete = ("OpsDecisionEventSourceV1" not in detail_overflow_types
                       and "ProductContractV2" not in detail_overflow_types and invalid_products == 0)
    instrument_details_complete = "ProductContractV2" not in detail_overflow_types and invalid_products == 0
    if campaign_start_ns is not None:
        for key in sorted(products_by_key, key=lambda item: item.to_canonical_json()):
            product = products_by_key[key]
            coverage_start_ns = max(campaign_start_ns, product.effective_at_ns, product.available_at_ns)
            first_m15 = ((coverage_start_ns // BarIntervalV2.M15.duration_ns) + 1) * BarIntervalV2.M15.duration_ns
            expected_count = max(0, (as_of_ns - first_m15) // BarIntervalV2.M15.duration_ns + 1)
            if expected_count > 100_000:
                slot_complete = False
            origins = range(first_m15, min(as_of_ns + 1,
                                           first_m15 + 100_000 * BarIntervalV2.M15.duration_ns),
                            BarIntervalV2.M15.duration_ns)
            slots: list[dict[str, Any]] = []
            for origin in origins:
                matched = valid_handoffs.get((key, origin), [])
                if matched:
                    handoff = matched[0]
                    slot_decision_status = ("DUPLICATE_HANDOFF" if len(matched) > 1
                                            else handoff["decision_status"])
                    slot_row = {
                        "instrument": key.to_dict(), "bar_close_decision_origin_ns": origin,
                        "information_cutoff_ns": handoff["information_cutoff_ns"],
                        "actual_received_at_ns": handoff["actual_received_at_ns"],
                        "actual_available_at_ns": handoff["actual_available_at_ns"],
                        "processing_at_ns": handoff["processing_at_ns"],
                        "expiration_deadline_ns": handoff["expiration_deadline_ns"],
                        "source_evidence_refs": [handoff["source_observation_ref"], handoff["bar_ref"]],
                        "missing_source_evidence": [], "missing_decision_handoff": False,
                        "decision_status": slot_decision_status,
                        "unresolved_or_ineligible_decision": (len(matched) > 1
                                                              or handoff["unresolved_or_ineligible_decision"]),
                    }
                else:
                    missing = None if not detail_complete else True
                    slot_row = {
                        "instrument": key.to_dict(), "bar_close_decision_origin_ns": origin,
                        "information_cutoff_ns": None, "actual_received_at_ns": None,
                        "actual_available_at_ns": None, "processing_at_ns": None,
                        "expiration_deadline_ns": None, "source_evidence_refs": [],
                        "missing_source_evidence": (["EVENT_DETAIL_BUDGET_EXCEEDED"] if not detail_complete
                                                    else ["NO_VALID_TRIGGER_BAR_EVIDENCE"]),
                        "missing_decision_handoff": missing,
                        "decision_status": ("INCOMPLETE_EVIDENCE_INVENTORY" if not detail_complete
                                            else "MISSING_DECISION_HANDOFF"),
                        "unresolved_or_ineligible_decision": True,
                    }
                    if detail_complete:
                        missing_slots.append({"instrument": key.to_dict(),
                                              "instrument_revision": key.contract_revision,
                                              "origin_ns": origin})
                slots.append(slot_row)
            instrument_identity = sha256_json(key.to_dict())
            per_instrument[instrument_identity] = {
                "instrument_identity": instrument_identity,
                "instrument": key.to_dict(), "coverage_start_ns": coverage_start_ns,
                "first_expected_bar_close_origin_ns": first_m15,
                "expected_slot_count": len(slots), "slots": slots,
                "missing_handoff_count": (sum(row["missing_decision_handoff"] is True for row in slots)
                                          if detail_complete else None),
                "unresolved_or_ineligible_count": (sum(row["unresolved_or_ineligible_decision"] for row in slots)
                                                   if detail_complete else None),
            }
    else:
        for key in sorted({slot[0] for slot in valid_handoffs}, key=lambda item: item.to_canonical_json()):
            instrument_identity = sha256_json(key.to_dict())
            per_instrument[instrument_identity] = {
                "instrument_identity": instrument_identity,
                "instrument": key.to_dict(), "expected_slot_count": None, "slots": [],
                "missing_handoff_count": None, "unresolved_or_ineligible_count": None,
            }

    expected_m1: tuple[int, ...] = ()
    expected_m15: tuple[int, ...] = ()
    if campaign_start_ns is not None:
        first_m1 = ((campaign_start_ns // BarIntervalV2.M1.duration_ns) + 1) * BarIntervalV2.M1.duration_ns
        expected_m1_count = max(0, (as_of_ns - first_m1) // BarIntervalV2.M1.duration_ns + 1)
        if expected_m1_count > 100_000:
            slot_complete = False
        expected_m1 = tuple(range(first_m1, min(as_of_ns + 1,
                                                first_m1 + 100_000 * BarIntervalV2.M1.duration_ns),
                                    BarIntervalV2.M1.duration_ns))
        first_m15 = ((campaign_start_ns // BarIntervalV2.M15.duration_ns) + 1) * BarIntervalV2.M15.duration_ns
        expected_m15_count = max(0, (as_of_ns - first_m15) // BarIntervalV2.M15.duration_ns + 1)
        if expected_m15_count > 100_000:
            slot_complete = False
        expected_m15 = tuple(range(first_m15, min(as_of_ns + 1,
                                                  first_m15 + 100_000 * BarIntervalV2.M15.duration_ns),
                                      BarIntervalV2.M15.duration_ns))
    actual_origins = tuple(sorted({origin for _, origin in valid_handoffs}))
    return {
        "per_instrument": per_instrument,
        "invalid_product_contract_count": invalid_products,
        "instrument_details_complete": instrument_details_complete,
        "expected_m15_origin_slots": expected_m15,
        "validated_handoffs": [handoff for slot in sorted(valid_handoffs,
              key=lambda value: (value[0].to_canonical_json(), value[1]))
              for handoff in valid_handoffs[slot]],
        "invalid_handoffs": invalid,
        "duplicate_handoff_count": duplicate_count,
        "missing_slots": missing_slots,
        "expected_s3_native_m1_origins": expected_m1,
        "actual_m15_handoff_origins": actual_origins,
        "detail_inventory_complete": detail_complete and slot_complete,
        "slot_inventory_complete": slot_complete,
    }


def build_public_shadow_preflight_v1(
    repository: OpsRepository, *, as_of_ns: int, campaign_start_ns: int | None = None,
    inventory_page_size: int = _INVENTORY_PAGE_SIZE,
    inventory_max_pages: int = _INVENTORY_MAX_PAGES,
) -> dict[str, Any]:
    """Build a deterministic report from one consistent read-only snapshot."""
    if not repository.read_only:
        raise ValueError("Session-031 preflight requires an OpsRepository opened read-only")
    if type(inventory_page_size) is not int or not 1 <= inventory_page_size <= 2_000:
        raise ValueError("inventory_page_size must be between 1 and 2000")
    if type(inventory_max_pages) is not int or not 1 <= inventory_max_pages <= _INVENTORY_MAX_PAGES:
        raise ValueError(f"inventory_max_pages must be between 1 and {_INVENTORY_MAX_PAGES}")
    with repository.read_snapshot():
        return _build_public_shadow_preflight_snapshot_v1(
            repository, as_of_ns=as_of_ns, campaign_start_ns=campaign_start_ns,
            inventory_page_size=inventory_page_size, inventory_max_pages=inventory_max_pages,
        )


def _build_public_shadow_preflight_snapshot_v1(
    repository: OpsRepository, *, as_of_ns: int, campaign_start_ns: int | None,
    inventory_page_size: int, inventory_max_pages: int,
) -> dict[str, Any]:
    """Build all seven deterministic report views using a read-only repository handle."""
    if not repository.read_only:
        raise ValueError("Session-031 preflight requires an OpsRepository opened read-only")
    if type(as_of_ns) is not int or as_of_ns < 0:
        raise ValueError("preflight as_of_ns must be a nonnegative UTC timestamp")
    by_type, artifact_type_counts, inventory, observation_inventory = _read_bounded_inventory(
        repository, as_of_ns=as_of_ns, page_size=inventory_page_size, max_pages=inventory_max_pages,
        campaign_start_ns=campaign_start_ns,
    )
    truncated = not inventory["complete"]
    detail_overflow_types = set(inventory["detail_overflow_artifact_types"])
    observation_entries_count = artifact_type_counts["PublicObservationIndexV2"]
    source_counts = observation_inventory["source_counts"]
    instrument_counts = observation_inventory["instrument_counts"]
    event_counts = observation_inventory["event_counts"]
    chronology = observation_inventory["chronology"]
    health_states, _health_refs, health_inventory = _latest_health(repository, as_of_ns)
    health_report = [{"source_id": key, "state": value.state.value, "observed_at_ns": value.observed_at_ns,
                      "available_at_ns": value.available_at_ns, "evidence_ref": value.content_hash}
                     for key, value in sorted(health_states.items())]

    products: list[ProductContractV2] = []
    for item in by_type.get("ProductContractV2", []):
        body = item.metadata.get("product")
        if isinstance(body, Mapping):
            try:
                product = ProductContractV2.from_dict(json_value(body))
            except (KeyError, TypeError, ValueError):
                continue
            if product.content_hash == item.artifact_ref:
                products.append(product)
    readiness = [_readiness_for_product(repository, item, as_of_ns) for item in sorted(
        products, key=lambda row: row.key.to_canonical_json())]

    calendar: list[DecisionCalendarEntryV2] = []
    invalid_calendar_count = 0
    for item in by_type.get("DecisionCalendarEntryV2", []):
        body = item.metadata.get("decision_entry")
        if isinstance(body, Mapping):
            try:
                calendar_entry = DecisionCalendarEntryV2.from_dict(json_value(body))
            except (KeyError, TypeError, ValueError):
                invalid_calendar_count += 1
                continue
            if calendar_entry.content_hash != item.artifact_ref or item.content_hash != calendar_entry.content_hash:
                invalid_calendar_count += 1
                continue
            calendar.append(calendar_entry)
        else:
            invalid_calendar_count += 1
    matured: list[MaturedOutcomeV2] = []
    invalid_matured_count = 0
    for item in by_type.get("MaturedOutcomeV2", []):
        body = item.metadata.get("outcome")
        if isinstance(body, Mapping):
            try:
                matured_row = MaturedOutcomeV2.from_dict(json_value(body))
            except (KeyError, TypeError, ValueError):
                invalid_matured_count += 1
                continue
            if matured_row.content_hash != item.artifact_ref or item.content_hash != matured_row.content_hash:
                invalid_matured_count += 1
                continue
            matured.append(matured_row)
        else:
            invalid_matured_count += 1
    calendar_details_complete = (not truncated and "DecisionCalendarEntryV2" not in detail_overflow_types
                                 and invalid_calendar_count == 0)
    matured_details_complete = (not truncated and "MaturedOutcomeV2" not in detail_overflow_types
                                and invalid_matured_count == 0)
    matured_by_decision = {row.decision_ref for row in matured}
    origins = _decision_origin_inventory(
        repository, by_type=by_type, as_of_ns=as_of_ns, campaign_start_ns=campaign_start_ns,
        detail_overflow_types=detail_overflow_types | ({"OpsDecisionEventSourceV1"} if truncated else set()),
    )
    expected_m1 = origins["expected_s3_native_m1_origins"]
    actual_handoffs = origins["actual_m15_handoff_origins"]
    expected_m15 = origins["expected_m15_origin_slots"]
    missing_m15 = origins["missing_slots"]
    cadence = s3_cadence_report_v1(
        expected_native_m1_origins=expected_m1,
        actual_production_handoff_origins=actual_handoffs,
        replay_view="ACTUAL_SYSTEM",
    )

    decision_stage_counts = Counter(f"{row.selection_state.value}/{row.admission_state.value}" for row in calendar)
    critic_observations = by_type.get("ActionCriticShadowObservationV1", [])
    critic_statuses: Counter[str] = Counter()
    findings = 0
    for item in critic_observations:
        body = item.metadata.get("observation")
        if isinstance(body, Mapping):
            critic_statuses[str(body.get("critic_terminal_status", "UNKNOWN"))] += 1
            findings += len(body.get("finding_types", [])) if body.get("accepted_shadow_evidence") is True else 0

    science_counts: dict[str, int | None] = {}
    science_inventory: dict[str, dict[str, Any]] = {}
    valid_science_counts = inventory["science_payload_valid_counts"]
    invalid_science_counts = inventory["science_payload_invalid_counts"]
    science_inventory_complete = not truncated and inventory["invalid_entry_count"] == 0
    for label, source in SCIENCE_ARTIFACT_SOURCES.items():
        if not source["supported"]:
            count = None
            status = "UNAVAILABLE"
            reason = source["reason"]
            indexed_count = None
            valid_count = None
            invalid_count = None
        else:
            source_types = source["types"]
            indexed_count = sum(artifact_type_counts[name] for name in source_types)
            valid_count = sum(valid_science_counts.get(name, 0) for name in source_types)
            invalid_count = sum(invalid_science_counts.get(name, 0) for name in source_types)
            count = valid_count if science_inventory_complete else None
            status = "TESTED" if science_inventory_complete and invalid_count == 0 else "TEST GATE"
            reason = None if status == "TESTED" else (
                "INVENTORY_INCOMPLETE" if truncated else (
                    "MALFORMED_ARTIFACT_INDEX_ENTRY_PRESENT" if inventory["invalid_entry_count"] else
                    "MALFORMED_OR_CONFLICTING_TYPED_PAYLOADS_PRESENT"
                )
            )
        science_counts[label] = count
        science_inventory[label] = {
            "status": status, "store": source["store"], "artifact_types": list(source["types"]),
            "indexed_count": indexed_count, "validated_payload_count": valid_count,
            "invalid_payload_count": invalid_count, "count": count, "reason": reason,
        }
    science_aliases: dict[str, int | None] = {
        "M0CalibrationV2": science_counts["M0_calibration"],
        "M0OODV2": (valid_science_counts.get("M0OODV2", 0) if science_inventory_complete else None),
        "M0SupportV2": (valid_science_counts.get("M0SupportV2", 0) if science_inventory_complete else None),
        "M0CalibrationAndEvaluationV2": science_counts["M0_calibration_and_evaluation"],
        "M1ModelArtifactV2": science_counts["M1_model_and_support"],
        "AnalogueSupportV2": science_counts["analogue_support"],
        "PretradeExecutionScenarioV2": science_counts["pretrade_scenarios"],
        "ExecutionAssumptionV2": science_counts["execution_assumptions"],
        "CostObservationV2": science_counts["costs"],
        "ResearchCalibrationV2": science_counts["research_calibration"],
        "DiscoveryExperimentV2": science_counts["discovery_experiments"],
        "DiscoveryAttemptV2": (valid_science_counts.get("DiscoveryAttemptV2", 0)
                                if science_inventory_complete else None),
        "DiscoveryRejectedAttemptV2": (valid_science_counts.get("DiscoveryRejectedAttemptV2", 0)
                                       if science_inventory_complete else None),
    }
    experiments_count = science_counts["discovery_experiments"]
    attempts_count = science_counts["discovery_attempts_and_rejections"]
    resource_sample = _read_only_resources(repository)

    report: dict[str, Any] = {
        "version": "PUBLIC_SHADOW_CAMPAIGN_PREFLIGHT_V1",
        "as_of_ns": as_of_ns,
        "campaign_start_ns": campaign_start_ns,
        "read_only": True,
        "artifact_inventory": {
            **inventory,
            "eligible_artifact_type_counts": dict(sorted(artifact_type_counts.items())),
            "as_of_filter": "available_at_ns <= as_of_ns applied in SQL before keyset pagination",
            "snapshot_start_boundary": "FIRST_QUERY_AFTER_READ_ONLY_BEGIN",
            "snapshot_end_boundary": "ROLLBACK_AFTER_REPORT_BUILD",
            "detail_sample_cap": _INVENTORY_MAX_RETAINED_DETAILS,
        },
        "reports": {
            "public_ingestion": {
                "status": "TEST GATE",
                "inventory_read_status": "TEST GATE" if truncated else "TESTED",
                "sources": dict(sorted(source_counts.items())),
                "instruments_by_revision": dict(sorted(instrument_counts.items())),
                "event_coverage": dict(sorted(event_counts.items())),
                "observed_raw_receipts": observation_entries_count,
                "chronology": chronology[-200:],
                "source_health": health_report,
                "source_health_inventory": health_inventory,
                "duplicate_or_conflict_incidents": artifact_type_counts["PublicDuplicateConflictV2"],
                "reconciliation_receipts": artifact_type_counts["OpsPublicSourceReconciliationV1"],
                "last_confirmed_bar_refs_by_interval": {
                    interval.value: ([observation_inventory["latest_bar_ref_by_interval"][interval.value]]
                                     if interval.value in observation_inventory["latest_bar_ref_by_interval"] else [])
                    for interval in BarIntervalV2
                },
                "revision_observation_count": observation_inventory["revision_count"],
                "revision_observation_count_reason": None,
                "stale_observation_count": None,
                "stale_observation_reason": "No universal freshness threshold applies across bar, trade and quote feeds.",
                "coverage_limitation": "Endpoint availability is not production strategy input qualification.",
            },
            "strategy_and_funnel": {
                "status": "TEST GATE",
                "inventory_read_status": "TEST GATE" if truncated else "TESTED",
                "observation_calendar_entries": (artifact_type_counts["DecisionCalendarEntryV2"]
                                                  if calendar_details_complete else None),
                "valid_observation_calendar_entries": (len(calendar) if calendar_details_complete else None),
                "invalid_observation_calendar_entries": invalid_calendar_count,
                "calendar_denominator_complete": calendar_details_complete,
                "expected_15m_origin_slots": list(expected_m15),
                "actually_observed_15m_handoffs": list(actual_handoffs),
                "missing_or_expired_origin_slots": list(missing_m15),
                "origin_slot_inventory_complete": origins["detail_inventory_complete"],
                "per_instrument_origin_accounting": origins["per_instrument"],
                "instrument_details_complete": origins["instrument_details_complete"],
                "validated_handoffs_complete": origins["detail_inventory_complete"],
                "invalid_handoffs_complete": origins["detail_inventory_complete"],
                "invalid_product_contract_count": origins["invalid_product_contract_count"],
                "validated_handoffs": origins["validated_handoffs"],
                "invalid_handoffs": origins["invalid_handoffs"],
                "duplicate_handoff_count": origins["duplicate_handoff_count"],
                "expired_unresolved_origin_slots": [
                    {"instrument_revision": item["instrument_revision"],
                     "origin_ns": item["bar_close_decision_origin_ns"],
                     "deadline_ns": item["expiration_deadline_ns"]}
                    for item in origins["validated_handoffs"]
                    if item["decision_status"] == "EXPIRED_UNRESOLVED_DECISION"
                ],
                "missing_slots_are_absent_opportunities": False,
                "selection_admission_states": dict(sorted(decision_stage_counts.items())),
                "expected_s3_native_m1_origins": cadence,
                "expected_s3_native_m1_origins_remain_separate_from_m15_handoffs": True,
                "per_instrument_readiness": readiness,
                "watch_transitions": [
                    item for item in repository.watch_transition_history(limit=10_000)
                    if type(item.get("transition_at_ns")) is int and item["transition_at_ns"] <= as_of_ns
                ],
                "candidate_set_count": artifact_type_counts["CandidateSetV2"],
                "candidate_count": artifact_type_counts["CandidateActionV2"],
                "selected_calendar_entries": (sum(row.selection_state.value == "SELECTED" for row in calendar)
                                               if calendar_details_complete else None),
                "unselected_or_rejected_calendar_entries": sum(
                    row.selection_state.value != "SELECTED" for row in calendar
                ) if calendar_details_complete else None,
                "frozen_action_count": (sum(row.action_artifact_ref is not None for row in calendar)
                                        if calendar_details_complete else None),
                "risk_sized_calendar_entries": (sum(row.admission_state.value == "RISK_SIZED" for row in calendar)
                                                 if calendar_details_complete else None),
                "risk_input_resolution_count": artifact_type_counts["OpsRiskEvidenceResolutionV1"],
                "pipeline_stage_statuses": dict(sorted(Counter(
                    f"{(item.metadata.get('stage_result') or {}).get('stage', 'UNKNOWN')}/"
                    f"{(item.metadata.get('stage_result') or {}).get('status', 'UNKNOWN')}"
                    for item in by_type.get("OpsSupervisorStageCheckpointV1", [])
                    if isinstance(item.metadata.get("stage_result"), Mapping)
                ).items())),
                "NO_CANDIDATE_NO_TRADE_NOT_ESTIMABLE_are_valid": True,
            },
            "science_and_research": {
                "status": "NOT ESTIMABLE",
                "available_artifact_counts": science_aliases,
                "artifact_inventory": science_inventory,
                "discovery_experiment_count": experiments_count,
                "attempt_and_rejection_count": attempts_count,
                "existing_trial_history_preserved": True,
                "session023_trial_identity": {
                    "experiment_id": S23_EXPERIMENT_ID,
                    "experiment_ref": S23_EXPERIMENT_REF,
                    "maximum_attempts": S23_MAXIMUM_ATTEMPTS,
                    "parameter_search_budget": S23_PARAMETER_SEARCH_BUDGET,
                    "prior_attempts": [{"attempt_id": attempt_id, "state": state}
                                       for attempt_id, state in S23_PRIOR_ATTEMPTS],
                    "attempts_already_recorded": len(S23_PRIOR_ATTEMPTS),
                    "attempt_budget_remaining": max(0, S23_MAXIMUM_ATTEMPTS - len(S23_PRIOR_ATTEMPTS)),
                    "new_s31_attempts_authorized": 0,
                    "multiplicity_family_id": S23_MULTIPLICITY_FAMILY_ID,
                    "multiplicity_family_ref": S23_MULTIPLICITY_REF,
                },
                "final_holdout": "UNASSIGNED / UNTOUCHED",
                "multiplicity_accounting": "Use existing Discovery Lab and S23 history; no campaign reset.",
                "supported_incremental_economic_conclusion": None,
            },
            "hidden_critic": {
                "status": ("TEST GATE" if artifact_type_counts["ActionCriticShadowObservationV1"] else "UNVERIFIED"),
                "observation_count": artifact_type_counts["ActionCriticShadowObservationV1"],
                "request_artifact_count": artifact_type_counts["ActionAssessmentRequestV2"],
                "sealed_packet_count": artifact_type_counts["SealedActionAssessmentPacketV1"],
                "attempt_count": None,
                "attempt_count_reason": "Attempts and authorization rows live in the S30 controller ledger, not this ops store.",
                "dispatch_authorized_count": (sum(
                    isinstance(item.metadata.get("observation"), Mapping)
                    and item.metadata["observation"].get("dispatch_authorized_at_ns") is not None
                    for item in critic_observations
                ) if "ActionCriticShadowObservationV1" not in detail_overflow_types else None),
                "terminal_statuses": (dict(sorted(critic_statuses.items()))
                                      if "ActionCriticShadowObservationV1" not in detail_overflow_types else None),
                "provider_profile_hashes": (sorted({
                    str(item.metadata["observation"].get("provider_profile_hash"))
                    for item in critic_observations if isinstance(item.metadata.get("observation"), Mapping)
                    and isinstance(item.metadata["observation"].get("provider_profile_hash"), str)
                }) if "ActionCriticShadowObservationV1" not in detail_overflow_types else None),
                "model_profile_hashes": (sorted({
                    str(item.metadata["observation"].get("model_profile_hash"))
                    for item in critic_observations if isinstance(item.metadata.get("observation"), Mapping)
                    and isinstance(item.metadata["observation"].get("model_profile_hash"), str)
                }) if "ActionCriticShadowObservationV1" not in detail_overflow_types else None),
                "validated_finding_count": findings,
                "dispatch_authorizations": None,
                "dispatch_authorization_reason": "S30 controller-owned authorization ledger is not read from this ops store.",
                "provider_cost": None,
                "provider_cost_reason": "No provider cost is reported unless an exact persisted measurement exists.",
                "decision_influence": False,
                "admission_influence": False,
            },
            "resources": resource_sample,
            "recovery": {
                "status": "TEST GATE",
                "inventory_read_status": "TEST GATE" if truncated else "TESTED",
                "recovery_epoch_count": artifact_type_counts["OpsRecoveryEpochV1"],
                "reconciliation_receipts": artifact_type_counts["OpsPublicSourceReconciliationV1"],
                "duplicate_conflict_incidents": artifact_type_counts["PublicDuplicateConflictV2"],
                "restart_markers": artifact_type_counts["OpsRuntimeRestartMarkerV1"],
                "unexercised_fault_gates": ["LIVE_NETWORK_DISCONNECT", "POWER_INTERRUPTION", "HOST_RESTART"],
            },
            "prospective_evidence_inventory": {
                "status": "NOT ESTIMABLE" if campaign_start_ns is None else "TEST GATE",
                "campaign_start_ns": campaign_start_ns,
                "genuine_forward_observation_origins": (None if campaign_start_ns is None or truncated
                                                        else observation_inventory["genuine_forward_count"]),
                "retrospective_or_reconstructed_origins": (None if truncated
                                                           else observation_inventory["reconstructed_count"]),
                "development_contaminated_origins": None,
                "synthetic_fault_fixture_origins": None,
                "matured_outcomes": (sum(row.label_state.value == "MATURED" for row in matured)
                                     if matured_details_complete else None),
                "pending_outcomes": (sum(row.content_hash not in matured_by_decision for row in calendar)
                                     if calendar_details_complete and matured_details_complete else None),
                "pending_decisions_without_qualified_horizon": [
                    {"decision_ref": row.content_hash, "decision_at_ns": row.decision_at_ns,
                     "expected_horizon_end_ns": None,
                     "reason": "Decision calendar entry has no horizon; no production outcome producer is qualified."}
                    for row in calendar if row.content_hash not in matured_by_decision
                ] if calendar_details_complete and matured_details_complete else None,
                "censored_outcomes": (sum(row.label_state.value == "CENSORED" for row in matured)
                                      if matured_details_complete else None),
                "ambiguous_outcomes": (sum(bool(row.ambiguity) for row in matured)
                                       if matured_details_complete else None),
                "missing_cost_or_fill_evidence": (sum(
                    row.fees is None or row.fill_quantity is None or row.net_payoff is None for row in matured
                ) if matured_details_complete else None),
                "calendar_denominator": (len(calendar) if calendar_details_complete else None),
                "matured_linked_to_calendar": (sum(row.content_hash in matured_by_decision for row in calendar)
                                               if calendar_details_complete and matured_details_complete else None),
                "unexercised_live_gates": [
                    "FRESH_HOST_PUBLIC_INGESTION", "FULL_SOURCE_CADENCE", "NATIVE_S3_M1",
                    "QUALIFIED_BOOTSTRAP", "CONTINUOUS_72_HOUR_CAMPAIGN", "MATURED_OUTCOME_PRODUCER",
                ],
            },
        },
        "lane_identities": [item.to_dict() for item in default_lane_identities()],
        "promotion_ceiling": "ENGINEERING_PASS",
        "promotion_ladder": ["INTEGRATED", "ENGINEERING_PASS", "HISTORICAL_DIAGNOSTIC",
                             "PROSPECTIVE_SHADOW", "INCREMENTAL_VALUE_PASS", "DECISION_ELIGIBLE"],
        "limitations": [
            "This schema does not qualify the underlying evidence.",
            "No matured outcome producer is inferred from contracts, validators, or indexing functions.",
            "Missing values remain unavailable; they are not converted to zero or passing status.",
        ],
    }
    report["report_identity"] = sha256_json(report)
    return report


def render_preflight_summary(report: dict[str, Any]) -> str:
    sections = report["reports"]
    lines = [f"ATLAS public-shadow preflight {report['report_identity']}",
             f"As of UTC ns: {report['as_of_ns']}",
             f"Public receipts: {sections['public_ingestion']['observed_raw_receipts']}",
             f"Calendar entries: {sections['strategy_and_funnel']['observation_calendar_entries']}",
             f"Matured outcomes: {sections['prospective_evidence_inventory']['matured_outcomes']}",
             "Outcome producer: TEST GATE — PRODUCTION_MATURED_OUTCOME_PRODUCER_NOT_QUALIFIED",
             f"Decision influence: {sections['hidden_critic']['decision_influence']}; admission influence: "
             f"{sections['hidden_critic']['admission_influence']}",
             "Capital enabled: false; assisted execution enabled: false",
             "Economic value: NOT ESTIMABLE"]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, help="existing local ops.sqlite (opened read-only)")
    parser.add_argument("--as-of-ns", required=True, type=int, help="UTC information cutoff in nanoseconds")
    parser.add_argument("--campaign-start-ns", type=int)
    parser.add_argument("--summary", action="store_true", help="render the concise human-readable summary")
    args = parser.parse_args(argv)
    with OpsRepository(args.db, read_only=True) as repository:
        result = build_public_shadow_preflight_v1(repository, as_of_ns=args.as_of_ns,
                                                  campaign_start_ns=args.campaign_start_ns)
    print(render_preflight_summary(result) if args.summary else canonical_json(result))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
