"""Persisted causal lineage checks for the Session-031 S3 preflight.

The S3 readiness evaluator accepts numerical arrays so it can remain a small,
deterministic mathematical check. This module validates the separately
persisted artifacts that must back those arrays before the production
preflight can report causal lineage as validated.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from atlas.v2._serialization import sha256_json, strict_fields, timestamp
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.health import PublicSourceHealthV2, PublicSourceStateV2
from atlas.v2.instruments import InstrumentKeyV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2
from atlas.v2.science.session031_readiness import S3_REQUIRED_M1_BARS, S3_STANDARDIZATION_PRECEDING
from atlas.v2.strategies.s1_trend import EventGate, EventState, ExecutableQuote
from atlas.v2.strategies.s3_mean_reversion import (
    CausalTradeV2,
    ResidualObservationV2,
    TradeVwapSnapshotV2,
    fit_ar1,
)

M1_NS = BarIntervalV2.M1.duration_ns
TRADE_HEALTH_MAX_AGE_NS = 60_000_000_000


@dataclass(frozen=True)
class S3PersistedLineageReportV1:
    """Compact summary of exact S3 references checked at one information cutoff."""

    cutoff_ns: int
    lineage_status: str
    numerical_arrays_status: str
    trade_continuity_status: str
    qualification_status: str
    reason_codes: tuple[str, ...]
    expected_observations: int
    validated_observations: int
    strictly_preceding_residuals: int
    validated_vwap_artifacts: int
    observed_trade_records: int
    duplicate_trade_identity_count: int
    half_life_minutes: float | None
    ordered_lineage_sha256: str
    first_close_at_ns: int | None
    last_close_at_ns: int | None
    bbo_age_ns: int | None
    processing_at_ns: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": "SESSION031_S3_PERSISTED_LINEAGE_V1",
            "cutoff_ns": self.cutoff_ns,
            "lineage_status": self.lineage_status,
            "numerical_arrays_status": self.numerical_arrays_status,
            "trade_continuity_status": self.trade_continuity_status,
            "qualification_status": self.qualification_status,
            "reason_codes": list(self.reason_codes),
            "expected_observations": self.expected_observations,
            "validated_observations": self.validated_observations,
            "strictly_preceding_residuals": self.strictly_preceding_residuals,
            "validated_vwap_artifacts": self.validated_vwap_artifacts,
            "observed_trade_records": self.observed_trade_records,
            "duplicate_trade_identity_count": self.duplicate_trade_identity_count,
            "half_life_minutes": self.half_life_minutes,
            "ordered_lineage_sha256": self.ordered_lineage_sha256,
            "first_close_at_ns": self.first_close_at_ns,
            "last_close_at_ns": self.last_close_at_ns,
            "bbo_age_ns": self.bbo_age_ns,
            "processing_at_ns": self.processing_at_ns,
            "processing_at_reason": "The accepted VWAP/residual wires do not carry processing timestamps.",
        }


def _decode_residual(body: Mapping[str, Any]) -> ResidualObservationV2:
    fields = {"schema_version", "key", "bar_ref", "vwap_ref", "close_at_ns", "available_at_ns",
              "residual", "replay_view"}
    value = strict_fields(body, expected=fields, required=fields, name="ResidualObservationV2")
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise ValueError("unsupported S3 residual schema")
    if not isinstance(value["key"], Mapping):
        raise ValueError("S3 residual instrument key is malformed")
    if (type(value["close_at_ns"]) is not int or type(value["available_at_ns"]) is not int
            or type(value["residual"]) not in (int, float) or isinstance(value["residual"], bool)
            or not isinstance(value["bar_ref"], str) or not isinstance(value["vwap_ref"], str)
            or not isinstance(value["replay_view"], str)):
        raise ValueError("S3 residual fields have invalid wire types")
    return ResidualObservationV2(
        InstrumentKeyV2.from_dict(value["key"]), value["bar_ref"], value["vwap_ref"],
        value["close_at_ns"], value["available_at_ns"], float(value["residual"]), value["replay_view"],
    )


def _entry_is_asof(entry: ArtifactIndexEntryV2, cutoff_ns: int) -> bool:
    return entry.created_at_ns <= cutoff_ns and entry.available_at_ns <= cutoff_ns


def _validate_window_bars(
    key: InstrumentKeyV2, cutoff_ns: int, bars: Sequence[CausalBarV2], reasons: set[str],
) -> tuple[CausalBarV2, ...]:
    eligible: list[CausalBarV2] = []
    seen_close: dict[int, str] = {}
    for bar in bars:
        if (bar.interval != BarIntervalV2.M1 or bar.instrument_revision != key.contract_revision
                or not bar.final or bar.raw.availability_class.value != "ACTUAL_SYSTEM"
                or bar.close_at_ns > cutoff_ns or bar.raw.available_at_ns > cutoff_ns):
            continue
        prior = seen_close.get(bar.close_at_ns)
        if prior is not None:
            reasons.add("S3_DUPLICATE_OR_CONFLICTING_M1_BAR_ORIGIN")
            continue
        seen_close[bar.close_at_ns] = bar.content_hash
        eligible.append(bar)
    ordered = tuple(sorted(eligible, key=lambda item: (item.close_at_ns, item.content_hash)))
    window = ordered[-S3_REQUIRED_M1_BARS:]
    if len(window) != S3_REQUIRED_M1_BARS:
        reasons.add("S3_PERSISTED_M1_WINDOW_INCOMPLETE")
    if any(right.close_at_ns - left.close_at_ns != M1_NS
           for left, right in zip(window, window[1:], strict=False)):
        reasons.add("S3_PERSISTED_M1_WINDOW_NONCONTIGUOUS")
    return window


def validate_s3_persisted_lineage_v1(
    *,
    key: InstrumentKeyV2,
    cutoff_ns: int,
    bars: Sequence[CausalBarV2],
    vwap_entries: Sequence[ArtifactIndexEntryV2],
    residual_entries: Sequence[ArtifactIndexEntryV2],
    trades: Sequence[CausalTradeV2],
    source_health: Sequence[PublicSourceHealthV2],
    quote: ExecutableQuote | None,
    event_gate: EventGate | None,
) -> S3PersistedLineageReportV1:
    """Validate exact persisted trade/VWAP/residual/bar references at ``cutoff_ns``.

    This verifies lineage, not trade-feed completeness. The S31 Bybit adapter
    uses bounded recent-trade REST snapshots and cannot certify that no trades
    were omitted between snapshots; its source health and contiguous bars do
    not change that limitation.
    """
    timestamp(cutoff_ns, field="cutoff_ns")
    reasons: set[str] = set()
    trade_by_ref: dict[str, CausalTradeV2] = {}
    trade_by_identity: dict[tuple[str, str], CausalTradeV2] = {}
    duplicate_trade_identity_count = 0
    for trade in trades:
        if (trade.key != key or trade.availability_class.value != "ACTUAL_SYSTEM"
                or trade.event_at_ns > cutoff_ns or trade.received_at_ns > cutoff_ns
                or trade.available_at_ns > cutoff_ns):
            continue
        previous = trade_by_ref.get(trade.raw_observation_ref)
        if previous is not None:
            reasons.add("S3_DUPLICATE_OR_CONFLICTING_TRADE_REFERENCE")
            continue
        trade_by_ref[trade.raw_observation_ref] = trade
        identity = (trade.source_id, trade.trade_id)
        previous_identity = trade_by_identity.get(identity)
        if previous_identity is not None:
            same_execution = (
                previous_identity.key == trade.key
                and previous_identity.event_at_ns == trade.event_at_ns
                and previous_identity.price == trade.price
                and previous_identity.quantity == trade.quantity
                and previous_identity.aggressor_side == trade.aggressor_side
                and previous_identity.availability_class == trade.availability_class
            )
            if not same_execution:
                reasons.add("S3_CONFLICTING_TRADE_IDENTITY")
            else:
                duplicate_trade_identity_count += 1
            if (trade.available_at_ns, trade.raw_observation_ref) < (
                    previous_identity.available_at_ns, previous_identity.raw_observation_ref):
                trade_by_identity[identity] = trade
        else:
            trade_by_identity[identity] = trade

    health_by_ref: dict[str, PublicSourceHealthV2] = {}
    for health in source_health:
        if health.available_at_ns <= cutoff_ns and health.observed_at_ns <= cutoff_ns:
            if health.content_hash in health_by_ref:
                reasons.add("S3_DUPLICATE_SOURCE_HEALTH_REFERENCE")
            health_by_ref[health.content_hash] = health

    snapshots_by_ref: dict[str, TradeVwapSnapshotV2] = {}
    for entry in vwap_entries:
        if entry.artifact_type != "S3TradeVwapSnapshotV2" or not _entry_is_asof(entry, cutoff_ns):
            continue
        body = entry.metadata.get("vwap")
        if not isinstance(body, Mapping):
            continue
        raw_key = body.get("key")
        if not isinstance(raw_key, Mapping) or dict(raw_key) != key.to_dict():
            continue
        try:
            snapshot = TradeVwapSnapshotV2.from_dict(body)
        except (ArithmeticError, KeyError, TypeError, ValueError):
            reasons.add("S3_MALFORMED_PERSISTED_TRADE_VWAP")
            continue
        if (snapshot.key != key or snapshot.replay_view != "ACTUAL_SYSTEM"
                or snapshot.information_cutoff_ns > cutoff_ns or snapshot.available_at_ns > cutoff_ns
                or snapshot.available_at_ns > snapshot.information_cutoff_ns
                or entry.artifact_ref != entry.content_hash or snapshot.content_hash != entry.artifact_ref
                or entry.available_at_ns != snapshot.available_at_ns):
            reasons.add("S3_TRADE_VWAP_IDENTITY_OR_AVAILABILITY_MISMATCH")
            continue
        snapshot_health = health_by_ref.get(snapshot.source_health_ref)
        if (snapshot_health is None or snapshot_health.state != PublicSourceStateV2.HEALTHY_CURRENT
                or snapshot_health.source_id == ""
                or snapshot.information_cutoff_ns - snapshot_health.available_at_ns > TRADE_HEALTH_MAX_AGE_NS
                or snapshot_health.available_at_ns > snapshot.available_at_ns):
            reasons.add("S3_TRADE_VWAP_SOURCE_HEALTH_UNAVAILABLE_OR_LATE")
            continue
        referenced_trades = [trade_by_ref.get(ref) for ref in snapshot.trade_refs]
        if any(trade is None for trade in referenced_trades):
            reasons.add("S3_TRADE_VWAP_REFERENCE_MISSING_OR_UNAVAILABLE")
            continue
        rows = [trade for trade in referenced_trades if trade is not None]
        if (any(trade.source_id != snapshot_health.source_id or trade.event_at_ns < snapshot.utc_day_start_ns
                or trade.event_at_ns > snapshot.information_cutoff_ns
                or trade.available_at_ns > snapshot.available_at_ns
                or trade.received_at_ns > snapshot.information_cutoff_ns for trade in rows)):
            reasons.add("S3_TRADE_VWAP_INPUT_CHRONOLOGY_OR_SOURCE_MISMATCH")
            continue
        quantity = sum((trade.quantity for trade in rows), Decimal(0))
        computed = sum((trade.price * trade.quantity for trade in rows), Decimal(0)) / quantity
        if computed != snapshot.vwap:
            reasons.add("S3_TRADE_VWAP_VALUE_DOES_NOT_MATCH_REFERENCED_TRADES")
            continue
        if snapshot.content_hash in snapshots_by_ref:
            reasons.add("S3_DUPLICATE_PERSISTED_TRADE_VWAP")
            continue
        snapshots_by_ref[snapshot.content_hash] = snapshot

    window = _validate_window_bars(key, cutoff_ns, bars, reasons)
    bar_by_ref = {bar.content_hash: bar for bar in window}
    residual_by_bar: dict[str, ResidualObservationV2] = {}
    for entry in residual_entries:
        if entry.artifact_type != "S3ResidualObservationV2" or not _entry_is_asof(entry, cutoff_ns):
            continue
        body = entry.metadata.get("residual")
        raw_key = body.get("key") if isinstance(body, Mapping) else None
        if not isinstance(body, Mapping) or not isinstance(raw_key, Mapping) or dict(raw_key) != key.to_dict():
            continue
        try:
            residual = _decode_residual(body)
        except (ArithmeticError, KeyError, TypeError, ValueError):
            reasons.add("S3_MALFORMED_PERSISTED_RESIDUAL")
            continue
        if (residual.key != key or residual.replay_view != "ACTUAL_SYSTEM"
                or residual.close_at_ns > cutoff_ns or residual.available_at_ns > cutoff_ns
                or entry.artifact_ref != entry.content_hash or residual.content_hash != entry.artifact_ref
                or entry.available_at_ns != residual.available_at_ns):
            reasons.add("S3_RESIDUAL_IDENTITY_OR_AVAILABILITY_MISMATCH")
            continue
        bar = bar_by_ref.get(residual.bar_ref)
        linked_snapshot = snapshots_by_ref.get(residual.vwap_ref)
        if bar is None:
            reasons.add("S3_RESIDUAL_CONFIRMED_BAR_REFERENCE_MISSING")
            continue
        if linked_snapshot is None:
            reasons.add("S3_RESIDUAL_TRADE_VWAP_REFERENCE_MISSING")
            continue
        if (residual.close_at_ns != bar.close_at_ns or linked_snapshot.information_cutoff_ns != bar.close_at_ns
                or linked_snapshot.key != key or bar.instrument_revision != key.contract_revision
                or residual.available_at_ns < max(bar.raw.available_at_ns, linked_snapshot.available_at_ns)):
            reasons.add("S3_RESIDUAL_BAR_VWAP_OR_AVAILABILITY_BINDING_MISMATCH")
            continue
        expected = math.log(float(bar.close)) - math.log(float(linked_snapshot.vwap))
        if residual.residual != expected:
            reasons.add("S3_RESIDUAL_VALUE_MISMATCH")
            continue
        if residual.bar_ref in residual_by_bar:
            reasons.add("S3_DUPLICATE_OR_CONFLICTING_RESIDUAL_FOR_BAR")
            continue
        residual_by_bar[residual.bar_ref] = residual

    ordered_residuals = tuple(sorted(
        (residual_by_bar[bar.content_hash] for bar in window if bar.content_hash in residual_by_bar),
        key=lambda item: (item.close_at_ns, item.bar_ref),
    ))
    expected_refs = tuple(bar.content_hash for bar in window)
    matched_refs = tuple(item.bar_ref for item in ordered_residuals)
    if len(ordered_residuals) != S3_REQUIRED_M1_BARS or matched_refs != expected_refs:
        reasons.add("S3_PERSISTED_RESIDUAL_WINDOW_INCOMPLETE_OR_MISALIGNED")
    strict_preceding = min(S3_STANDARDIZATION_PRECEDING, max(0, len(ordered_residuals) - 1))
    if strict_preceding < S3_STANDARDIZATION_PRECEDING:
        reasons.add("S3_STRICTLY_PRECEDING_120_PERSISTED_RESIDUALS_MISSING")

    half_life: float | None = None
    if len(ordered_residuals) == S3_REQUIRED_M1_BARS:
        try:
            _, _, half_life = fit_ar1(tuple(item.residual for item in ordered_residuals))
            if not 5 <= half_life <= 30:
                reasons.add("S3_AR_HALF_LIFE_OUTSIDE_SUPPORTED_RANGE")
        except (ValueError, ArithmeticError):
            reasons.add("S3_AR_PARAMETERS_NOT_ESTIMABLE")

    bbo_age: int | None = None
    if (quote is None or quote.key != key or not quote.valid_at(cutoff_ns, 1_000_000_000)):
        reasons.add("S3_FRESH_EXECUTABLE_BBO_MISSING_OR_OLDER_THAN_ONE_SECOND")
    else:
        bbo_age = cutoff_ns - quote.observed_at_ns
    if (event_gate is None or not event_gate.valid_at(cutoff_ns)
            or event_gate.state == EventState.UNKNOWN):
        reasons.add("S3_RELEVANT_EVENT_EVIDENCE_MISSING_OR_STALE")
    elif event_gate.state == EventState.BLOCKED:
        reasons.add("S3_RELEVANT_EVENT_GATE_BLOCKED")

    latest_vwap = (snapshots_by_ref.get(ordered_residuals[-1].vwap_ref)
                   if ordered_residuals else None)
    latest_vwap_health = health_by_ref.get(latest_vwap.source_health_ref) if latest_vwap is not None else None
    latest_source_health: dict[str, PublicSourceHealthV2] = {}
    for health in health_by_ref.values():
        old = latest_source_health.get(health.source_id)
        if old is None or (health.observed_at_ns, health.available_at_ns, health.content_hash) > (
                old.observed_at_ns, old.available_at_ns, old.content_hash):
            latest_source_health[health.source_id] = health
    latest_trade_health = (latest_source_health.get(latest_vwap_health.source_id)
                           if latest_vwap_health is not None else None)
    if (latest_vwap_health is None or latest_trade_health is None
            or latest_trade_health.state != PublicSourceStateV2.HEALTHY_CURRENT
            or latest_trade_health.available_at_ns > cutoff_ns
            or cutoff_ns - latest_trade_health.available_at_ns > TRADE_HEALTH_MAX_AGE_NS):
        reasons.add("S3_REQUIRED_CURRENT_TRADE_SOURCE_HEALTH_MISSING")
    if window:
        bar_source = window[-1].raw.source_id
        current_bar_health = latest_source_health.get(bar_source)
        if (current_bar_health is None or current_bar_health.state != PublicSourceStateV2.HEALTHY_CURRENT
                or current_bar_health.available_at_ns > cutoff_ns
                or cutoff_ns - current_bar_health.available_at_ns > TRADE_HEALTH_MAX_AGE_NS):
            reasons.add("S3_REQUIRED_CURRENT_BAR_SOURCE_HEALTH_MISSING")

    lineage_failures = {
        reason for reason in reasons
        if reason.startswith("S3_") and not reason.startswith("S3_AR_")
        and reason not in {"S3_FRESH_EXECUTABLE_BBO_MISSING_OR_OLDER_THAN_ONE_SECOND",
                           "S3_RELEVANT_EVENT_EVIDENCE_MISSING_OR_STALE",
                           "S3_RELEVANT_EVENT_GATE_BLOCKED",
                           "S3_REQUIRED_CURRENT_TRADE_SOURCE_HEALTH_MISSING"}
    }
    lineage_status = "VALIDATED" if not lineage_failures else "INVALID"
    numeric_ok = (len(window) == S3_REQUIRED_M1_BARS and len(ordered_residuals) == S3_REQUIRED_M1_BARS
                  and strict_preceding >= S3_STANDARDIZATION_PRECEDING and half_life is not None
                  and 5 <= half_life <= 30)
    numerical_status = "SUFFICIENT" if numeric_ok else "INSUFFICIENT"
    # The bounded recent-trades REST surface has no completeness/continuity proof.
    # Exact per-record lineage therefore never qualifies the 7-day S3 history.
    reasons.add("S3_TRADE_CONTINUITY_UNPROVEN_BY_BOUNDED_REST_SNAPSHOTS")
    operational_failures = reasons - {"S3_TRADE_CONTINUITY_UNPROVEN_BY_BOUNDED_REST_SNAPSHOTS"}
    qualification_status = ("TEST GATE" if lineage_status == "VALIDATED" and numeric_ok and not operational_failures
                            else "NOT ESTIMABLE")
    ordered_identity = [
        [bar.content_hash, residual_by_bar[bar.content_hash].content_hash,
         residual_by_bar[bar.content_hash].vwap_ref]
        for bar in window if bar.content_hash in residual_by_bar
    ]
    digest = sha256_json(ordered_identity)
    return S3PersistedLineageReportV1(
        cutoff_ns, lineage_status, numerical_status,
        "UNVERIFIABLE_BOUNDED_REST_RECENT_TRADE_HISTORY", qualification_status,
        tuple(sorted(reasons)), S3_REQUIRED_M1_BARS, len(ordered_residuals), strict_preceding,
        len(snapshots_by_ref), len(trade_by_ref), duplicate_trade_identity_count, half_life, digest,
        window[0].close_at_ns if window else None, window[-1].close_at_ns if window else None,
        bbo_age,
    )
