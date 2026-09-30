"""Read-only public-shadow campaign preflight and evidence inventory reports."""

from __future__ import annotations

import argparse
import os
import platform
import shutil
from collections import Counter
from pathlib import Path
from typing import Any

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.data.bars import BarIntervalV2
from atlas.v2.data.health import PublicSourceHealthV2
from atlas.v2.data.history import reconstruct_causal_bars_from_archive
from atlas.v2.data.raw import AvailabilityClassV2
from atlas.v2.instruments import InstrumentKeyV2, ProductContractV2, UniverseContractV2
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.runtime.production import _indexed_quote_and_mark, _latest_event_gate
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

_REPORT_TYPES = (
    "PublicObservationIndexV2", "PublicSourceHealthV2", "ProductContractV2",
    "OpsDecisionEventSourceV1", "OpsSupervisorReceiptV1", "DecisionCalendarEntryV2",
    "MaturedOutcomeV2", "CandidateSetV2", "CandidateActionV2", "FeatureArtifactV2",
    "UniverseContractV2", "S3TradeVwapSnapshotV2", "S3ResidualObservationV2",
    "ActionCriticShadowObservationV1", "SealedActionAssessmentPacketV1", "ActionAssessmentRequestV2",
    "DiscoveryExperimentV2", "DiscoveryAttemptV2", "DiscoveryRejectedAttemptV2",
    "OpsPublicSourceReconciliationV1", "PublicDuplicateConflictV2", "OpsRecoveryEpochV1",
    "OpsRuntimeRestartMarkerV1", "OpsCycleReceiptV1", "OpsSupervisorStageCheckpointV1",
    "OpsRiskEvidenceResolutionV1",
)


def _latest_health(repository: OpsRepository, cutoff_ns: int) -> dict[str, PublicSourceHealthV2]:
    latest: dict[str, PublicSourceHealthV2] = {}
    for entry in repository.artifact_entries("PublicSourceHealthV2"):
        if entry.available_at_ns > cutoff_ns:
            continue
        body = entry.metadata.get("health")
        if not isinstance(body, dict):
            continue
        try:
            item = PublicSourceHealthV2.from_dict(body)
        except (KeyError, TypeError, ValueError):
            continue
        old = latest.get(item.source_id)
        if old is None or (item.available_at_ns, item.content_hash) > (old.available_at_ns, old.content_hash):
            latest[item.source_id] = item
    return latest


def _persisted_feature_readiness(repository: OpsRepository, key: InstrumentKeyV2, cutoff_ns: int) -> tuple[bool, bool, tuple[str, ...]]:
    candidates = []
    for entry in repository.artifact_entries("FeatureArtifactV2"):
        if entry.available_at_ns > cutoff_ns:
            continue
        body = entry.metadata.get("feature")
        raw_key = body.get("key") if isinstance(body, dict) else None
        values = body.get("values") if isinstance(body, dict) else None
        replay_view = body.get("replay_view") if isinstance(body, dict) else None
        if raw_key != key.to_dict() or replay_view != "ACTUAL_SYSTEM" or not isinstance(values, dict):
            continue
        candidates.append((entry, values))
    if not candidates:
        return False, False, ()
    entry, values = max(candidates, key=lambda row: (row[0].available_at_ns, row[0].artifact_ref))

    def present(name: str) -> bool:
        value = values.get(name)
        return isinstance(value, dict) and value.get("value") is not None

    s1 = present("m15.atr14") and present("m15.realized_variance20")
    s2 = present("m15.atr14") and present("m15.bollinger_width20")
    return s1, s2, (entry.artifact_ref,)


def _universe_eligible(repository: OpsRepository, key: InstrumentKeyV2, cutoff_ns: int) -> bool:
    for entry in sorted(repository.artifact_entries("UniverseContractV2"),
                         key=lambda item: (item.available_at_ns, item.artifact_ref), reverse=True):
        if entry.available_at_ns > cutoff_ns:
            continue
        body = entry.metadata.get("universe")
        if not isinstance(body, dict):
            continue
        try:
            universe = UniverseContractV2.from_dict(body)
        except (KeyError, TypeError, ValueError):
            continue
        match = next((item for item in universe.entries if item.key == key), None)
        if match is not None:
            return bool(match.data_eligible and match.scanner_eligible and not match.capital_eligible
                        and all(match.strategy_eligibility.get(name) is not None
                                and match.strategy_eligibility[name].status.value == "ELIGIBLE"
                                for name in ("S1_MTF_TREND_PULLBACK", "S2_COMPRESSION_BREAKOUT",
                                             "S3_VWAP_STAT_MEAN_REVERSION")))
    return False


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

    health = _latest_health(repository, cutoff_ns)
    latest_source = next((bar.raw.source_id for bar in reversed(bars[BarIntervalV2.M15])), None)
    bar_health = health.get(latest_source or "")
    required_bar_sources = {
        bar.raw.source_id
        for interval, tail_size in ((BarIntervalV2.M1, 10_081), (BarIntervalV2.M15, 2_901),
                                    (BarIntervalV2.H1, 50), (BarIntervalV2.H4, 50))
        for bar in bars[interval][-tail_size:]
    }
    source_ok = bool(required_bar_sources) and all(
        source_id in health and health[source_id].data_eligible
        and cutoff_ns - health[source_id].available_at_ns <= 60_000_000_000
        for source_id in required_bar_sources
    )
    quote, mark, quote_refs = _indexed_quote_and_mark(repository, archive_root, product, cutoff_ns=cutoff_ns)
    gate, _ = _latest_event_gate(repository, cutoff_ns)
    s1_feature, s2_feature, feature_refs = _persisted_feature_readiness(repository, product.key, cutoff_ns)
    watches = [item for item in repository.list_watches(limit=10_000)
               if item.key == product.key and item.strategy_id == "S1_MTF_TREND_PULLBACK"
               and item.created_at_ns <= cutoff_ns]
    watch_ids = {item.watch_id for item in watches}
    transitions = [item for item in repository.watch_transition_history(limit=10_000)
                   if item.get("watch_id") in watch_ids and item.get("transition_at_ns", cutoff_ns + 1) <= cutoff_ns]
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
    public_trades = [item for item in repository.artifact_entries("PublicObservationIndexV2")
                     if item.metadata.get("instrument_revision") == product.key.contract_revision
                     and item.metadata.get("event_type") in ("TRADE", "AGG_TRADE")
                     and item.metadata.get("availability_class") == "ACTUAL_SYSTEM"
                     and item.available_at_ns <= cutoff_ns]
    trade_sources = {str(item.metadata.get("source_id")) for item in public_trades}
    trade_health = next((health[source_id] for source_id in sorted(trade_sources)
                         if source_id in health and health[source_id].data_eligible
                         and cutoff_ns - health[source_id].available_at_ns <= 60_000_000_000), None)
    trade_health_current = bool(trade_sources) and all(
        source_id in health and health[source_id].data_eligible
        and cutoff_ns - health[source_id].available_at_ns <= 60_000_000_000
        for source_id in trade_sources
    )

    vwap_refs: list[str] = []
    for entry in repository.artifact_entries("S3TradeVwapSnapshotV2"):
        body = entry.metadata.get("vwap")
        raw_key = body.get("key") if isinstance(body, dict) else None
        if (raw_key == product.key.to_dict() and isinstance(body, dict)
                and body.get("replay_view") == "ACTUAL_SYSTEM" and entry.available_at_ns <= cutoff_ns):
            vwap_refs.append(entry.artifact_ref)
    vwap_ref_set = set(vwap_refs)
    residuals: list[tuple[int, float, str, str]] = []
    for entry in repository.artifact_entries("S3ResidualObservationV2"):
        body = entry.metadata.get("residual")
        raw_key = body.get("key") if isinstance(body, dict) else None
        value = body.get("residual") if isinstance(body, dict) else None
        close = body.get("close_at_ns") if isinstance(body, dict) else None
        vwap_ref = body.get("vwap_ref") if isinstance(body, dict) else None
        if (raw_key == product.key.to_dict() and isinstance(body, dict)
                and body.get("replay_view") == "ACTUAL_SYSTEM" and entry.available_at_ns <= cutoff_ns
                and isinstance(value, (int, float)) and not isinstance(value, bool)
                and type(close) is int and close <= cutoff_ns):
            residuals.append((close, float(value), entry.artifact_ref, str(vwap_ref or "")))
    residuals.sort()
    snapshot = StrategyEvidenceSnapshotV1(
        product.key, cutoff_ns, bars, source_ok, trade_health_current, quote, mark, gate,
        _universe_eligible(repository, product.key, cutoff_ns), s1_feature, active_watch,
        subsequent_close, s2_feature, len(public_trades),
        sum(row[3] in vwap_ref_set for row in residuals),
        tuple(row[1] for row in residuals), tuple(row[2] for row in residuals),
        tuple(item.artifact_ref for item in public_trades),
        tuple(row[0] for row in residuals), tuple(row[3] for row in residuals if row[3] in vwap_ref_set),
    )
    report = evaluate_strategy_readiness_v1(snapshot)
    report["evidence_refs"] = sorted(bar_refs | set(quote_refs) | set(feature_refs) | set(vwap_refs)
                                     | {row[2] for row in residuals})
    report["watch_state"] = {"active_watch_present": active_watch,
                              "active_watch_state_known": active_watch_known,
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


def build_public_shadow_preflight_v1(
    repository: OpsRepository, *, as_of_ns: int, campaign_start_ns: int | None = None,
) -> dict[str, Any]:
    """Build all seven deterministic report views using a read-only repository handle."""
    if not repository.read_only:
        raise ValueError("Session-031 preflight requires an OpsRepository opened read-only")
    if type(as_of_ns) is not int or as_of_ns < 0:
        raise ValueError("preflight as_of_ns must be a nonnegative UTC timestamp")
    entries = repository.artifact_entries_by_types(_REPORT_TYPES, limit=10_000, available_before_ns=as_of_ns)
    truncated = len(entries) == 10_000
    by_type: dict[str, list[Any]] = {}
    for item in entries:
        by_type.setdefault(item.artifact_type, []).append(item)

    observation_entries = by_type.get("PublicObservationIndexV2", [])
    source_counts: Counter[str] = Counter()
    instrument_counts: Counter[str] = Counter()
    event_counts: Counter[str] = Counter()
    chronology = []
    for item in observation_entries:
        metadata = item.metadata
        source_counts[str(metadata.get("source_id", "UNKNOWN"))] += 1
        instrument_counts[str(metadata.get("instrument_revision", "UNKNOWN"))] += 1
        event_counts[str(metadata.get("event_type", "UNKNOWN"))] += 1
        chronology.append({"ref": item.artifact_ref, "source_id": metadata.get("source_id"),
                           "event_type": metadata.get("event_type"), "event_at_ns": metadata.get("event_at_ns"),
                           "published_at_ns": metadata.get("published_at_ns"),
                           "received_at_ns": item.created_at_ns, "available_at_ns": item.available_at_ns,
                           "availability_class": metadata.get("availability_class"),
                           "bar_content_hash": metadata.get("bar_content_hash"),
                           "revision_of": metadata.get("revision_of")})
    chronology.sort(key=lambda row: (row["received_at_ns"], row["ref"]))
    health_states = _latest_health(repository, as_of_ns)
    health_report = [{"source_id": key, "state": value.state.value, "observed_at_ns": value.observed_at_ns,
                      "available_at_ns": value.available_at_ns, "evidence_ref": value.content_hash}
                     for key, value in sorted(health_states.items())]

    products: list[ProductContractV2] = []
    for item in by_type.get("ProductContractV2", []):
        body = item.metadata.get("product")
        if isinstance(body, dict):
            try:
                product = ProductContractV2.from_dict(body)
            except (KeyError, TypeError, ValueError):
                continue
            if product.content_hash == item.artifact_ref:
                products.append(product)
    readiness = [_readiness_for_product(repository, item, as_of_ns) for item in sorted(
        products, key=lambda row: row.key.to_canonical_json())]

    calendar = []
    for item in by_type.get("DecisionCalendarEntryV2", []):
        body = item.metadata.get("decision_entry")
        if isinstance(body, dict):
            try:
                row = DecisionCalendarEntryV2.from_dict(body)
            except (KeyError, TypeError, ValueError):
                continue
            calendar.append(row)
    matured = []
    for item in by_type.get("MaturedOutcomeV2", []):
        body = item.metadata.get("outcome")
        if isinstance(body, dict):
            try:
                matured.append(MaturedOutcomeV2.from_dict(body))
            except (KeyError, TypeError, ValueError):
                continue
    matured_by_decision = {row.decision_ref for row in matured}
    handoff_origins = []
    for item in by_type.get("OpsDecisionEventSourceV1", []):
        body = item.metadata.get("event")
        if isinstance(body, dict) and type(body.get("information_cutoff_ns")) is int:
            handoff_origins.append(int(body["information_cutoff_ns"]))
    slot_inventory_truncated = False
    if campaign_start_ns is not None:
        first_m1 = ((campaign_start_ns // 60_000_000_000) + 1) * 60_000_000_000
        m1_count = max(0, (as_of_ns - first_m1) // 60_000_000_000 + 1)
        slot_inventory_truncated = m1_count > 100_000
        expected_m1 = tuple(range(first_m1, min(as_of_ns + 1, first_m1 + 100_000 * 60_000_000_000),
                                  60_000_000_000))
        actual_handoffs = tuple(sorted({origin for origin in handoff_origins if origin >= campaign_start_ns}))
        first_m15 = ((campaign_start_ns // BarIntervalV2.M15.duration_ns) + 1) * BarIntervalV2.M15.duration_ns
        m15_count = max(0, (as_of_ns - first_m15) // BarIntervalV2.M15.duration_ns + 1)
        slot_inventory_truncated = slot_inventory_truncated or m15_count > 100_000
        expected_m15 = tuple(range(
            first_m15,
            min(as_of_ns + 1, first_m15 + 100_000 * BarIntervalV2.M15.duration_ns),
            BarIntervalV2.M15.duration_ns,
        ))
        missing_m15 = tuple(sorted(set(expected_m15) - set(actual_handoffs)))
    else:
        expected_m1, actual_handoffs = (), ()
        expected_m15, missing_m15 = (), ()
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
        if isinstance(body, dict):
            critic_statuses[str(body.get("critic_terminal_status", "UNKNOWN"))] += 1
            findings += len(body.get("finding_types", [])) if body.get("accepted_shadow_evidence") is True else 0

    science_types = (
        "M0CalibrationV2", "M1ModelArtifactV2", "AnalogueSupportV2", "PretradeExecutionScenarioV2",
        "ExecutionAssumptionV2", "CostObservationV2", "ResearchCalibrationV2",
        "DiscoveryExperimentV2", "DiscoveryAttemptV2", "DiscoveryRejectedAttemptV2",
    )
    science_counts = {name: len(by_type.get(name, [])) for name in science_types}
    experiments = by_type.get("DiscoveryExperimentV2", [])
    attempts = [*by_type.get("DiscoveryAttemptV2", []), *by_type.get("DiscoveryRejectedAttemptV2", [])]
    resource_sample = _read_only_resources(repository)

    report: dict[str, Any] = {
        "version": "PUBLIC_SHADOW_CAMPAIGN_PREFLIGHT_V1",
        "as_of_ns": as_of_ns,
        "campaign_start_ns": campaign_start_ns,
        "read_only": True,
        "reports": {
            "public_ingestion": {
                "status": "TEST GATE",
                "inventory_read_status": "TEST GATE" if truncated else "TESTED",
                "sources": dict(sorted(source_counts.items())),
                "instruments_by_revision": dict(sorted(instrument_counts.items())),
                "event_coverage": dict(sorted(event_counts.items())),
                "observed_raw_receipts": len(observation_entries),
                "chronology": chronology[-200:],
                "source_health": health_report,
                "duplicate_or_conflict_incidents": len(by_type.get("PublicDuplicateConflictV2", [])),
                "reconciliation_receipts": len(by_type.get("OpsPublicSourceReconciliationV1", [])),
                "last_confirmed_bar_refs_by_interval": {
                    interval.value: [row["ref"] for row in chronology
                                     if row["event_type"] == f"BAR_{interval.value}"
                                     and isinstance(row["bar_content_hash"], str)][-1:]
                    for interval in BarIntervalV2
                },
                "revision_observation_count": sum(row["revision_of"] is not None for row in chronology),
                "stale_observation_count": None,
                "stale_observation_reason": "No universal freshness threshold applies across bar, trade and quote feeds.",
                "coverage_limitation": "Endpoint availability is not production strategy input qualification.",
            },
            "strategy_and_funnel": {
                "status": "TEST GATE",
                "inventory_read_status": "TEST GATE" if truncated else "TESTED",
                "observation_calendar_entries": len(calendar),
                "expected_15m_origin_slots": list(expected_m15),
                "actually_observed_15m_handoffs": list(actual_handoffs),
                "missing_or_expired_origin_slots": list(missing_m15),
                "origin_slot_inventory_complete": not slot_inventory_truncated,
                "expired_unresolved_origin_slots": [slot for slot in missing_m15
                                                      if as_of_ns - slot > 5_000_000_000],
                "missing_slots_are_absent_opportunities": False,
                "selection_admission_states": dict(sorted(decision_stage_counts.items())),
                "expected_s3_native_m1_origins": cadence,
                "per_instrument_readiness": readiness,
                "watch_transitions": [
                    item for item in repository.watch_transition_history(limit=10_000)
                    if type(item.get("transition_at_ns")) is int and item["transition_at_ns"] <= as_of_ns
                ],
                "candidate_set_count": len(by_type.get("CandidateSetV2", [])),
                "candidate_count": len(by_type.get("CandidateActionV2", [])),
                "selected_calendar_entries": sum(row.selection_state.value == "SELECTED" for row in calendar),
                "unselected_or_rejected_calendar_entries": sum(
                    row.selection_state.value != "SELECTED" for row in calendar
                ),
                "frozen_action_count": sum(row.action_artifact_ref is not None for row in calendar),
                "risk_sized_calendar_entries": sum(row.admission_state.value == "RISK_SIZED" for row in calendar),
                "risk_input_resolution_count": len(by_type.get("OpsRiskEvidenceResolutionV1", [])),
                "pipeline_stage_statuses": dict(sorted(Counter(
                    f"{(item.metadata.get('stage_result') or {}).get('stage', 'UNKNOWN')}/"
                    f"{(item.metadata.get('stage_result') or {}).get('status', 'UNKNOWN')}"
                    for item in by_type.get("OpsSupervisorStageCheckpointV1", [])
                    if isinstance(item.metadata.get("stage_result"), dict)
                ).items())),
                "NO_CANDIDATE_NO_TRADE_NOT_ESTIMABLE_are_valid": True,
            },
            "science_and_research": {
                "status": "NOT ESTIMABLE",
                "available_artifact_counts": science_counts,
                "discovery_experiment_count": len(experiments),
                "attempt_and_rejection_count": len(attempts),
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
                "status": "TEST GATE" if critic_observations else "UNVERIFIED",
                "observation_count": len(critic_observations),
                "request_artifact_count": len(by_type.get("ActionAssessmentRequestV2", [])),
                "sealed_packet_count": len(by_type.get("SealedActionAssessmentPacketV1", [])),
                "attempt_count": None,
                "attempt_count_reason": "Attempts and authorization rows live in the S30 controller ledger, not this ops store.",
                "dispatch_authorized_count": sum(
                    isinstance(item.metadata.get("observation"), dict)
                    and item.metadata["observation"].get("dispatch_authorized_at_ns") is not None
                    for item in critic_observations
                ),
                "terminal_statuses": dict(sorted(critic_statuses.items())),
                "provider_profile_hashes": sorted({
                    str(item.metadata["observation"].get("provider_profile_hash"))
                    for item in critic_observations if isinstance(item.metadata.get("observation"), dict)
                    and isinstance(item.metadata["observation"].get("provider_profile_hash"), str)
                }),
                "model_profile_hashes": sorted({
                    str(item.metadata["observation"].get("model_profile_hash"))
                    for item in critic_observations if isinstance(item.metadata.get("observation"), dict)
                    and isinstance(item.metadata["observation"].get("model_profile_hash"), str)
                }),
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
                "recovery_epoch_count": len(by_type.get("OpsRecoveryEpochV1", [])),
                "reconciliation_receipts": len(by_type.get("OpsPublicSourceReconciliationV1", [])),
                "duplicate_conflict_incidents": len(by_type.get("PublicDuplicateConflictV2", [])),
                "restart_markers": len(by_type.get("OpsRuntimeRestartMarkerV1", [])),
                "unexercised_fault_gates": ["LIVE_NETWORK_DISCONNECT", "POWER_INTERRUPTION", "HOST_RESTART"],
            },
            "prospective_evidence_inventory": {
                "status": "NOT ESTIMABLE" if campaign_start_ns is None else "TEST GATE",
                "campaign_start_ns": campaign_start_ns,
                "genuine_forward_observation_origins": None if campaign_start_ns is None else sum(
                    1 for row in chronology if row["received_at_ns"] >= campaign_start_ns
                    and row.get("event_at_ns") is not None and row["event_at_ns"] >= campaign_start_ns
                ),
                "retrospective_or_reconstructed_origins": sum(
                    1 for row in chronology if row.get("availability_class") == "RECONSTRUCTED_MARKET"
                ),
                "development_contaminated_origins": None,
                "synthetic_fault_fixture_origins": None,
                "matured_outcomes": sum(row.label_state.value == "MATURED" for row in matured),
                "pending_outcomes": sum(row.content_hash not in matured_by_decision for row in calendar),
                "pending_decisions_without_qualified_horizon": [
                    {"decision_ref": row.content_hash, "decision_at_ns": row.decision_at_ns,
                     "expected_horizon_end_ns": None,
                     "reason": "Decision calendar entry has no horizon; no production outcome producer is qualified."}
                    for row in calendar if row.content_hash not in matured_by_decision
                ],
                "censored_outcomes": sum(row.label_state.value == "CENSORED" for row in matured),
                "ambiguous_outcomes": sum(bool(row.ambiguity) for row in matured),
                "missing_cost_or_fill_evidence": sum(
                    row.fees is None or row.fill_quantity is None or row.net_payoff is None for row in matured
                ),
                "calendar_denominator": len(calendar),
                "matured_linked_to_calendar": sum(row.content_hash in matured_by_decision for row in calendar),
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
