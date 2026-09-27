"""Measurable, cutoff-bound structural and UTC time context challengers.

These correlated research descriptors are not votes and do not mutate the
accepted INTRADAY_CORE_V1 feature pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, ClassVar

from .._serialization import decimal_value, nonblank, sha256_json, sha256_ref, timestamp
from ..data.bars import CausalBarV2
from ..data.capabilities import capability_for_public_channel_v2, default_evidence_capability_matrix_v2
from ..data.derivatives import DerivativeAvailabilityV2, FundingKindV2, FundingObservationV2
from ..data.microstructure import S4FeatureArtifactV2
from ..news.events import CalendarCoverageV2, EventSafetyGateV2, ScheduledEventV2

STRUCTURE_CONTEXT_VERSION = "STRUCTURE_WYCKOFF_MEASURABLE_CONTEXT_V1"
TIME_CONTEXT_VERSION = "UTC_TIME_CONTEXT_V1"
KILLZONE_CHALLENGER_VERSION = "ICT_KILLZONE_RESEARCH_CHALLENGER_V1"
EFFORT_RESULT_VERSION = "EFFORT_VS_RESULT_CAUSAL_V1"
STRUCTURE_CONTEXT_POLICY_HASH = sha256_json({"policy_id": STRUCTURE_CONTEXT_VERSION, "spec": {
    "spring_upthrust": "prior_range_boundary_false_break_then_same_bar_return",
    "effort_result": "prior_only_ols_expected_response_residual",
    "accepted_causal_structure": "EXACT_REFERENCES_CUTOFF_FILTERED",
    "baseline_mutation": "INTRADAY_CORE_V1_UNCHANGED", "context_vote": "NO_VOTE",
}})
EFFORT_RESULT_POLICY_HASH = sha256_json({"policy_id": EFFORT_RESULT_VERSION, "spec": {
    "fit_order": "CHRONOLOGICAL", "cutoff_rule": "TRAIN_AVAILABLE_STRICTLY_BEFORE_CURRENT_CUTOFF",
    "aggressive_flow_requires_valid_s4_and_qualified_matrix_coverage": True,
    "narrative_labels": "FORBIDDEN", "small_result_residual": "expected_abs_move_minus_actual_abs_move",
}})
TIME_CONTEXT_POLICY_HASH = sha256_json({"policy_id": TIME_CONTEXT_VERSION, "spec": {
    "timezone": "UTC", "weekday_encoding": "MONDAY_ZERO_SUNDAY_SIX",
    "sessions": "EXPLICIT_DECLARATION_ONLY", "macro_events": "ACCEPTED_S7_GATE_AND_VERIFIED_COVERAGE_ONLY",
    "time_to_funding": "ACTUAL_RECEIPT_AND_CAPABILITY_MATRIX_ONLY",
    "context_vote": "NO_VOTE",
}})
KILLZONE_POLICY_HASH = sha256_json({"policy_id": KILLZONE_CHALLENGER_VERSION, "spec": {
    "windows_utc_minutes": [[60, 240], [420, 600], [780, 960]],
    "qualification": "ENGINEERING_RESEARCH_DEFAULT_UNQUALIFIED",
    "distinct_from_calendar": True, "context_vote": "NO_VOTE",
}})


@dataclass(frozen=True)
class TimedSessionV2:
    session_id: str
    start_minute_utc: int
    end_minute_utc: int
    declaration_ref: str

    def __post_init__(self) -> None:
        nonblank(self.session_id, field="session_id")
        if not 0 <= self.start_minute_utc < 1440 or not 0 <= self.end_minute_utc < 1440:
            raise ValueError("UTC session minutes must lie in [0,1440)")
        if self.start_minute_utc == self.end_minute_utc:
            raise ValueError("session must have nonzero duration")
        sha256_ref(self.declaration_ref, field="declaration_ref")


@dataclass(frozen=True)
class S4FlowResponseObservationV2:
    feature: S4FeatureArtifactV2
    available_at_ns: int
    signed_flow: Decimal
    realized_response: Decimal
    input_ref: str

    def __post_init__(self) -> None:
        timestamp(self.available_at_ns, field="available_at_ns")
        object.__setattr__(self, "signed_flow", decimal_value(self.signed_flow, field="signed_flow"))
        object.__setattr__(self, "realized_response", decimal_value(self.realized_response, field="realized_response"))
        sha256_ref(self.input_ref, field="input_ref")
        flow_row = next((row for row in self.feature.flow_price_response_windows
                         if row[0] == "30" and row[3] == "ESTIMABLE"), None)
        response_row = next((row for row in self.feature.price_response_windows
                             if row[0] == "30" and row[2] == "ESTIMABLE"), None)
        if (not self.feature.estimable or self.feature.trade_coverage_state != "QUALIFIED"
                or self.available_at_ns < self.feature.cutoff_ns or self.input_ref != self.feature.content_hash
                or flow_row is None or response_row is None
                or self.signed_flow != Decimal(flow_row[1])
                or self.realized_response != Decimal(response_row[1])):
            raise ValueError("S4 flow response must bind qualified cutoff-known feature and input")


@dataclass(frozen=True)
class StructureContextArtifactV2:
    cutoff_ns: int
    spring_upthrust_state: str
    spring_upthrust_ref: str | None
    effort_result_state: str
    effort_result_residual: Decimal | None
    effort_kind: str
    effort_result_fit_ref: str | None
    input_refs: tuple[str, ...]
    accepted_structure_refs: tuple[str, ...] = ()
    version: str = STRUCTURE_CONTEXT_VERSION
    effort_version: str = EFFORT_RESULT_VERSION

    def __post_init__(self) -> None:
        timestamp(self.cutoff_ns, field="cutoff_ns")
        nonblank(self.spring_upthrust_state, field="spring_upthrust_state")
        nonblank(self.effort_result_state, field="effort_result_state")
        nonblank(self.effort_kind, field="effort_kind")
        nonblank(self.version, field="version")
        nonblank(self.effort_version, field="effort_version")
        if self.spring_upthrust_ref is not None:
            sha256_ref(self.spring_upthrust_ref, field="spring_upthrust_ref")
        if self.effort_result_fit_ref is not None:
            sha256_ref(self.effort_result_fit_ref, field="effort_result_fit_ref")
        if self.effort_result_residual is not None:
            object.__setattr__(self, "effort_result_residual", decimal_value(self.effort_result_residual, field="effort_result_residual"))
        for ref in self.input_refs:
            sha256_ref(ref, field="input_ref")
        for ref in self.accepted_structure_refs:
            sha256_ref(ref, field="accepted_structure_ref")
        if not set(self.accepted_structure_refs).issubset(self.input_refs):
            raise ValueError("accepted causal structure refs must be bound in input_refs")

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, "version": self.version,
                "policy_hash": STRUCTURE_CONTEXT_POLICY_HASH,
                "effort_version": self.effort_version, "effort_policy_hash": EFFORT_RESULT_POLICY_HASH,
                "cutoff_ns": self.cutoff_ns, "spring_upthrust_state": self.spring_upthrust_state,
                "spring_upthrust_ref": self.spring_upthrust_ref, "effort_result_state": self.effort_result_state,
                "effort_result_residual": str(self.effort_result_residual) if self.effort_result_residual is not None else None,
                "effort_kind": self.effort_kind,
                "effort_result_fit_ref": self.effort_result_fit_ref, "input_refs": list(self.input_refs),
                "accepted_structure_refs": list(self.accepted_structure_refs),
                "context_vote": "NO_VOTE"}

    @property
    def content_hash(self) -> str:
        return sha256_json({"artifact_type": "StructureContextArtifactV2", "artifact": self.to_dict()})


@dataclass(frozen=True)
class CausalStructureReferenceV2:
    family: str
    content_ref: str
    event_at_ns: int
    available_at_ns: int
    producer_version: str

    ALLOWED_FAMILIES: ClassVar[frozenset[str]] = frozenset({
        "CONFIRMED_SWING", "BOS", "CHoCH", "FVG", "SWEEP", "SUPPORT_RESISTANCE", "FIBONACCI", "MORPHOLOGY",
    })

    def __post_init__(self) -> None:
        if self.family not in self.ALLOWED_FAMILIES:
            raise ValueError("causal structure family must be an accepted measurable artifact")
        sha256_ref(self.content_ref, field="content_ref")
        timestamp(self.event_at_ns, field="event_at_ns")
        timestamp(self.available_at_ns, field="available_at_ns")
        nonblank(self.producer_version, field="producer_version")
        if self.available_at_ns < self.event_at_ns:
            raise ValueError("causal structure cannot be available before its event time")

    def to_dict(self) -> dict[str, Any]:
        return {"family": self.family, "content_ref": self.content_ref, "event_at_ns": self.event_at_ns,
                "available_at_ns": self.available_at_ns, "producer_version": self.producer_version}

    @property
    def content_hash(self) -> str:
        return sha256_json({"artifact_type": "CausalStructureReferenceV2", "reference": self.to_dict()})


def _ols_response(prior: list[tuple[Decimal, Decimal]], effort: Decimal) -> tuple[Decimal | None, str | None]:
    """OLS expected absolute price move from prior effort/result rows only."""
    if len(prior) < 3:
        return None, None
    xs = [x for x, _ in prior]
    ys = [y for _, y in prior]
    xbar = sum(xs, Decimal(0)) / len(xs)
    ybar = sum(ys, Decimal(0)) / len(ys)
    denom = sum(((x - xbar) ** 2 for x in xs), Decimal(0))
    if denom == 0:
        return None, None
    slope = sum(((x - xbar) * (y - ybar) for x, y in prior), Decimal(0)) / denom
    intercept = ybar - slope * xbar
    expected = intercept + slope * effort
    fit_ref = sha256_json({"model": EFFORT_RESULT_VERSION, "prior_effort_result": [[str(x), str(y)] for x, y in prior]})
    return expected, fit_ref


def build_structure_context(*, bars: tuple[CausalBarV2, ...], cutoff_ns: int,
                            range_lookback: int = 20,
                            s4: S4FeatureArtifactV2 | None = None,
                            s4_flow_history: tuple[S4FlowResponseObservationV2, ...] = (),
                            accepted_structure_evidence: tuple[CausalStructureReferenceV2, ...] = ()) -> StructureContextArtifactV2:
    cutoff = timestamp(cutoff_ns, field="cutoff_ns")
    if range_lookback <= 0:
        raise ValueError("range lookback must be positive")
    known = [bar for bar in bars if bar.final and bar.raw.available_at_ns <= cutoff]
    known.sort(key=lambda bar: (bar.close_at_ns, bar.raw.available_at_ns, bar.content_hash))
    inputs = tuple(sorted(bar.content_hash for bar in known))
    accepted_evidence = tuple(sorted(
        (item for item in accepted_structure_evidence
         if item.event_at_ns <= cutoff and item.available_at_ns <= cutoff),
        key=lambda item: (item.available_at_ns, item.event_at_ns, item.family, item.content_ref),
    ))
    accepted_refs = tuple(sorted({item.content_ref for item in accepted_evidence}))
    accepted_evidence_refs = {item.content_hash for item in accepted_evidence}
    spring_state, spring_ref = "NOT_ESTIMABLE_INSUFFICIENT_CAUSAL_RANGE", None
    if len(known) >= range_lookback + 1:
        current = known[-1]
        prior = known[-range_lookback - 1:-1]
        high = max(item.high for item in prior)
        low = min(item.low for item in prior)
        if current.low < low and current.close >= low:
            spring_state = "SPRING_FALSE_BREAK_AND_RETURN"
        elif current.high > high and current.close <= high:
            spring_state = "UPTHRUST_FALSE_BREAK_AND_RETURN"
        else:
            spring_state = "NO_FALSE_BREAK_RETURN"
        spring_ref = sha256_json({"version": STRUCTURE_CONTEXT_VERSION,
                                  "prior_range_refs": [item.content_hash for item in prior],
                                  "current_ref": current.content_hash, "range_high": str(high), "range_low": str(low),
                                  "state": spring_state})
    effort_state, residual, effort_kind, fit_ref = "NOT_ESTIMABLE_PRIOR_FIT_SUPPORT", None, "NOT_ESTIMABLE", None
    extra_inputs: set[str] = set()
    if len(known) >= 4:
        current = known[-1]
        flow_rows = [row for row in s4_flow_history if row.available_at_ns <= cutoff]
        flow_row = next((row for row in reversed(flow_rows) if s4 is not None
                         and row.feature.content_hash == s4.content_hash), None)
        if (flow_row and s4 and s4.estimable and s4.cutoff_ns <= cutoff
                and s4.trade_coverage_state == "QUALIFIED"):
            prior_flow_rows = [row for row in flow_rows if row.available_at_ns < flow_row.available_at_ns]
            prior_response = [(abs(row.signed_flow), abs(row.realized_response))
                              for row in prior_flow_rows[-20:]]
            effort = abs(flow_row.signed_flow)
            actual = abs(flow_row.realized_response)
            effort_kind = "SIGNED_AGGRESSIVE_FLOW"
            extra_inputs.update([flow_row.input_ref, flow_row.feature.content_hash,
                                 *[row.input_ref for row in prior_flow_rows],
                                 *[row.feature.content_hash for row in prior_flow_rows]])
        else:
            effort = abs(current.volume)
            actual = abs(current.close - current.open)
            prior_response = [(abs(bar.volume), abs(bar.close - bar.open)) for bar in known[:-1][-20:]]
            effort_kind = "BAR_VOLUME_PROXY"
        expected, fit_ref = _ols_response(prior_response, effort)
        if expected is not None:
            residual = expected - actual
            effort_state = "SMALL_RESULT_GIVEN_PRIOR_EFFORT" if residual > 0 else "RESULT_MATCHES_OR_EXCEEDS_EXPECTATION"
            fit_ref = sha256_json({"fit_ref": fit_ref, "current_effort_ref": flow_row.input_ref if effort_kind == "SIGNED_AGGRESSIVE_FLOW" and flow_row else current.content_hash,
                                   "effort_kind": effort_kind, "expected_move": str(expected),
                                   "actual_move": str(actual), "s4_ref": s4.content_hash if effort_kind == "SIGNED_AGGRESSIVE_FLOW" and s4 else None})
    return StructureContextArtifactV2(
        cutoff, spring_state, spring_ref, effort_state, residual, effort_kind, fit_ref,
        tuple(sorted(set(inputs) | extra_inputs | set(accepted_refs) | accepted_evidence_refs)), accepted_refs,
    )


@dataclass(frozen=True)
class TimeContextArtifactV2:
    cutoff_ns: int
    utc_minute_of_day: int
    weekday_utc: int
    weekend_utc: bool
    session_overlaps: tuple[str, ...]
    session_refs: tuple[str, ...]
    time_to_funding_ns: int | None
    macro_event_proximity_ns: int | None
    macro_event_ref: str | None
    macro_gate_ref: str | None
    macro_state: str
    source_refs: tuple[str, ...]
    version: str = TIME_CONTEXT_VERSION

    def __post_init__(self) -> None:
        timestamp(self.cutoff_ns, field="cutoff_ns")
        nonblank(self.version, field="version")
        if not 0 <= self.utc_minute_of_day < 1440 or not 0 <= self.weekday_utc <= 6:
            raise ValueError("invalid UTC time encoding")
        for name in ("macro_event_ref", "macro_gate_ref"):
            value = getattr(self, name)
            if value is not None:
                sha256_ref(value, field=name)
        for ref in self.session_refs + self.source_refs:
            sha256_ref(ref, field="source_ref")

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, "version": self.version, "policy_hash": TIME_CONTEXT_POLICY_HASH,
                "cutoff_ns": self.cutoff_ns,
                "utc_minute_of_day": self.utc_minute_of_day, "weekday_utc": self.weekday_utc,
                "weekend_utc": self.weekend_utc, "session_overlaps": list(self.session_overlaps),
                "session_refs": list(self.session_refs), "time_to_funding_ns": self.time_to_funding_ns,
                "macro_event_proximity_ns": self.macro_event_proximity_ns,
                "macro_event_ref": self.macro_event_ref, "macro_gate_ref": self.macro_gate_ref,
                "macro_state": self.macro_state, "source_refs": list(self.source_refs),
                "context_vote": "NO_VOTE"}

    @property
    def content_hash(self) -> str:
        return sha256_json({"artifact_type": "TimeContextArtifactV2", "artifact": self.to_dict()})


@dataclass(frozen=True)
class KillzoneResearchArtifactV2:
    cutoff_ns: int
    windows: tuple[str, ...]
    version: str = KILLZONE_CHALLENGER_VERSION
    research_only: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, "version": self.version, "policy_hash": KILLZONE_POLICY_HASH,
                "cutoff_ns": self.cutoff_ns,
                "windows": list(self.windows), "research_only": self.research_only,
                "window_definition_utc_minutes": [[60, 240], [420, 600], [780, 960]],
                "threshold_status": "ENGINEERING_RESEARCH_DEFAULT_UNQUALIFIED",
                "distinct_from_calendar_features": True, "context_vote": "NO_VOTE"}

    @property
    def content_hash(self) -> str:
        return sha256_json({"artifact_type": "KillzoneResearchArtifactV2", "artifact": self.to_dict()})


def build_time_context(*, cutoff_ns: int, sessions: tuple[TimedSessionV2, ...] = (),
                       funding: tuple[FundingObservationV2, ...] = (),
                       calendar_coverage: CalendarCoverageV2 | None = None,
                       scheduled_events: tuple[ScheduledEventV2, ...] = (),
                       s7_gate: EventSafetyGateV2 | None = None,
                       macro_window_ns: int = 60 * 60 * 1_000_000_000) -> tuple[TimeContextArtifactV2, KillzoneResearchArtifactV2]:
    cutoff = timestamp(cutoff_ns, field="cutoff_ns")
    dt = datetime.fromtimestamp(cutoff / 1_000_000_000, UTC)
    minute = dt.hour * 60 + dt.minute
    overlaps = []
    session_refs = []
    for session in sessions:
        start, end = session.start_minute_utc, session.end_minute_utc
        active = start <= minute < end if start < end else minute >= start or minute < end
        if active:
            overlaps.append(session.session_id)
            session_refs.append(session.declaration_ref)
    matrix = default_evidence_capability_matrix_v2()
    eligible_funding = [item for item in funding if item.next_funding_at_ns is not None
                        and item.available_at_ns <= cutoff and item.next_funding_at_ns >= cutoff
                        and item.availability == DerivativeAvailabilityV2.ACTUAL_RECEIPT
                        and item.source_health == "HEALTHY_CURRENT" and item.source_health_ref is not None
                        and item.kind in {FundingKindV2.CURRENT, FundingKindV2.PREDICTED}
                        and (item.kind != FundingKindV2.PREDICTED or item.kind_qualification_ref is not None)
                        and (capability := capability_for_public_channel_v2(matrix, item.instrument, item.channel)) is not None
                        and "crowding context" in " ".join(capability.permitted_uses)]
    eligible_funding.sort(key=lambda x: (x.available_at_ns, x.content_hash))
    next_funding = eligible_funding[-1] if eligible_funding else None
    next_funding_at = next_funding.next_funding_at_ns if next_funding else None
    ttf = next_funding_at - cutoff if next_funding_at is not None else None
    macro_state, macro_event, macro_distance = "NOT_ESTIMABLE_S7_GATE_OR_VERIFIED_COVERAGE_MISSING", None, None
    macro_gate_ref = None
    if (calendar_coverage is not None and calendar_coverage.available_at_ns <= cutoff
            and calendar_coverage.complete and calendar_coverage.source_qualification == "VERIFIED"
            and s7_gate is not None and s7_gate.cutoff_ns <= cutoff and not s7_gate.blocked
            and calendar_coverage.source_id == s7_gate.calendar_source_id
            and calendar_coverage.revision == s7_gate.schedule_revision):
        macro_gate_ref = s7_gate.content_hash
        visible_events = [item for item in scheduled_events if item.available_at_ns <= cutoff
                          and item.source_id == calendar_coverage.source_id
                          and item.schedule_revision == calendar_coverage.revision
                          and calendar_coverage.covered_from_ns <= item.scheduled_at_ns <= calendar_coverage.covered_through_ns]
        if visible_events:
            visible_events.sort(key=lambda x: (abs(x.scheduled_at_ns - cutoff), x.available_at_ns, x.content_hash))
            candidate = visible_events[0]
            # Use only the event explicitly bound by the accepted S7 gate when one is specified.
            if s7_gate.scheduled_event_ref is None or s7_gate.scheduled_event_ref == candidate.content_hash:
                distance = abs(candidate.scheduled_at_ns - cutoff)
                if distance <= macro_window_ns:
                    macro_event, macro_distance = candidate, distance
                    macro_state = "EVENT_WITHIN_DECLARED_WINDOW"
                else:
                    macro_state = "NO_EVENT_WITHIN_DECLARED_WINDOW"
            else:
                macro_state = "NOT_ESTIMABLE_GATE_EVENT_MISMATCH"
        else:
            macro_state = "NO_S7_EVENT_IN_COVERED_WINDOW"
    ordinary_refs = ([next_funding.raw_content_ref, next_funding.content_hash, matrix.content_hash]
                     if next_funding else [])
    ordinary_refs += [calendar_coverage.evidence_ref] if calendar_coverage and calendar_coverage.available_at_ns <= cutoff else []
    ordinary_refs += [macro_event.evidence_ref] if macro_event else []
    ordinary_refs += [macro_gate_ref] if macro_gate_ref else []
    time = TimeContextArtifactV2(cutoff, minute, dt.weekday(), dt.weekday() >= 5,
                                 tuple(sorted(overlaps)), tuple(sorted(set(session_refs))), ttf,
                                 macro_distance, macro_event.content_hash if macro_event else None,
                                 macro_gate_ref, macro_state, tuple(sorted(set(ordinary_refs))))
    # Research windows are deliberately a separate artifact and do not reuse session/calendar features.
    windows = []
    if 60 <= minute < 240:
        windows.append("ASIA_RESEARCH_WINDOW")
    if 420 <= minute < 600:
        windows.append("LONDON_RESEARCH_WINDOW")
    if 780 <= minute < 960:
        windows.append("NEW_YORK_RESEARCH_WINDOW")
    killzone = KillzoneResearchArtifactV2(cutoff, tuple(windows))
    return time, killzone
