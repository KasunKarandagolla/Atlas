"""Independent S5 continuation and post-cascade reversal research paths."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, ClassVar

from .._serialization import decimal_value, nonblank, sha256_json, sha256_ref, timestamp
from ..data.capabilities import capability_for_public_channel_v2, default_evidence_capability_matrix_v2
from ..data.derivatives import (
    LiquidationCoverageV2,
    LiquidationObservationV2,
    LiquidationWindowTotalV2,
    OIChangeEvidenceV2,
    OpenInterestObservationV2,
    S5CrowdingContextV2,
    S5LiquidationBaselineV2,
)
from ..data.microstructure import S4AbsorptionHypothesisV2, S4FeatureArtifactV2

S5_CONTINUATION_VERSION = "S5_DELEVERAGING_CONTINUATION_SHADOW_V1"
S5_REVERSAL_VERSION = "S5_POST_CASCADE_REVERSAL_SHADOW_V1"
S5_CONTINUATION_POLICY_HASH = sha256_json({"policy_id": S5_CONTINUATION_VERSION, "spec": {
    "conceptual_stages": ["VULNERABILITY", "BREAK", "DELEVERAGING_EVIDENCE", "CONTINUATION_CONFIRMED"],
    "asynchronous_availability": True, "confirmation_latency_floor_ns": 0,
    "confirmation_must_be_strictly_after_evidence": True,
    "backdating": "FORBIDDEN", "exact_action": "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT",
    "source_channel_capability_binding": True, "censored_liquidation": "WEAK_EVIDENCE_ONLY",
    "selector_influence": "ZERO",
}})
S5_REVERSAL_POLICY_HASH = sha256_json({"policy_id": S5_REVERSAL_VERSION, "spec": {
    "separate_from_continuation": True, "liquidation_baseline_windows_minimum": 20,
    "minimum_liquidation_z_default": "2", "s4_valid_absorption_required": True,
    "exhaustion_required": True, "reclaim_or_flow_reversal_required": True,
    "censored_coverage": "NOT_ESTIMABLE", "exact_action": "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT",
    "source_channel_capability_matrix_required": True,
    "selector_influence": "ZERO",
    "threshold_status": "ENGINEERING_RESEARCH_DEFAULT_UNQUALIFIED",
}})


@dataclass(frozen=True)
class StageEvidenceV2:
    stage: str
    event_at_ns: int | None
    available_at_ns: int
    refs: tuple[str, ...]
    state: str

    def __post_init__(self) -> None:
        nonblank(self.stage, field="stage")
        nonblank(self.state, field="state")
        if self.event_at_ns is not None:
            timestamp(self.event_at_ns, field="event_at_ns")
        timestamp(self.available_at_ns, field="available_at_ns")
        for ref in self.refs:
            sha256_ref(ref, field="evidence_ref")
        if self.state != "NOT_ESTIMABLE" and not self.refs:
            raise ValueError("estimable stage evidence requires exact evidence refs")

    def to_dict(self) -> dict[str, Any]:
        return {"stage": self.stage, "event_at_ns": self.event_at_ns,
                "available_at_ns": self.available_at_ns, "refs": list(self.refs), "state": self.state}


@dataclass(frozen=True)
class S5ContinuationArtifactV2:
    cutoff_ns: int
    state: str
    vulnerability: StageEvidenceV2 | None
    break_evidence: StageEvidenceV2 | None
    deleveraging_evidence: StageEvidenceV2 | None
    confirmation: StageEvidenceV2 | None
    liquidation_coverage: LiquidationCoverageV2
    fallback_state: str
    confirmation_latency_ns: int | None
    exact_action_status: str = "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT"
    policy_version: str = S5_CONTINUATION_VERSION

    SCHEMA_VERSION: ClassVar[int] = 1

    def __post_init__(self) -> None:
        timestamp(self.cutoff_ns, field="cutoff_ns")
        object.__setattr__(self, "liquidation_coverage", LiquidationCoverageV2(self.liquidation_coverage))
        nonblank(self.state, field="state")
        nonblank(self.fallback_state, field="fallback_state")
        nonblank(self.policy_version, field="policy_version")
        if self.confirmation_latency_ns is not None and self.confirmation_latency_ns < 0:
            raise ValueError("confirmation latency cannot be negative")

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": self.SCHEMA_VERSION, "policy_version": self.policy_version,
                "policy_hash": S5_CONTINUATION_POLICY_HASH,
                "cutoff_ns": self.cutoff_ns, "state": self.state,
                "vulnerability": self.vulnerability.to_dict() if self.vulnerability else None,
                "break": self.break_evidence.to_dict() if self.break_evidence else None,
                "deleveraging_evidence": self.deleveraging_evidence.to_dict() if self.deleveraging_evidence else None,
                "confirmation": self.confirmation.to_dict() if self.confirmation else None,
                "liquidation_coverage": self.liquidation_coverage.value,
                "fallback_state": self.fallback_state,
                "confirmation_latency_ns": self.confirmation_latency_ns,
                "exact_action_status": self.exact_action_status, "selector_influence": "ZERO"}

    @property
    def content_hash(self) -> str:
        return sha256_json({"artifact_type": "S5ContinuationArtifactV2", "artifact": self.to_dict()})


class S5ContinuationResearchV2:
    """Asynchronous event-state reducer. Evidence is timestamped when it became available."""

    def __init__(self, *, confirmation_latency_ns: int = 0) -> None:
        if confirmation_latency_ns < 0:
            raise ValueError("confirmation latency must be nonnegative")
        self.confirmation_latency_ns = confirmation_latency_ns
        self.vulnerabilities: list[StageEvidenceV2] = []
        self.breaks: list[StageEvidenceV2] = []
        self.deleveraging_events: list[tuple[StageEvidenceV2, LiquidationCoverageV2]] = []
        self.confirmations: list[StageEvidenceV2] = []

    def add_vulnerability(self, *, event_at_ns: int, available_at_ns: int, context: S5CrowdingContextV2,
                          structure_refs: tuple[str, ...], liquidity_refs: tuple[str, ...] = ()) -> None:
        refs = tuple(sorted(set(context.input_refs + structure_refs + liquidity_refs)))
        if context.cutoff_ns > available_at_ns:
            raise ValueError("vulnerability context cannot be used before its own availability cutoff")
        vulnerability_support = bool(
            context.state == "CROWDING_CONTEXT" and context.evidence_quality == "SUPPORTED"
            and structure_refs and liquidity_refs and context.liquidity_state != "UNKNOWN"
        )
        self.vulnerabilities.append(StageEvidenceV2("VULNERABILITY", event_at_ns, available_at_ns, refs,
                                                     "OBSERVED_CONTEXT" if vulnerability_support else "NOT_ESTIMABLE"))

    def add_break(self, *, event_at_ns: int, available_at_ns: int, structure_ref: str,
                  adverse_response_ref: str) -> None:
        refs = (structure_ref, adverse_response_ref)
        self.breaks.append(StageEvidenceV2("BREAK", event_at_ns, available_at_ns, refs, "CAUSAL_STRUCTURE_FAILURE"))

    def add_deleveraging(self, *, observation: OpenInterestObservationV2 | LiquidationObservationV2,
                         event_at_ns: int | None, available_at_ns: int,
                         coverage: LiquidationCoverageV2 | None = None,
                         oi_change_evidence: OIChangeEvidenceV2 | None = None) -> None:
        # Availability is the actual point the state reducer receives the record; never use its event time to backdate.
        if available_at_ns < observation.available_at_ns:
            raise ValueError("deleveraging stage cannot precede input availability")
        if isinstance(observation, LiquidationObservationV2):
            if observation.event_at_ns is not None and event_at_ns != observation.event_at_ns:
                raise ValueError("liquidation stage event time must preserve the source event timestamp")
            event_at_ns = observation.event_at_ns
            coverage_state = observation.coverage
            capability = capability_for_public_channel_v2(
                default_evidence_capability_matrix_v2(), observation.instrument, observation.channel,
            )
            capability_use = bool(capability and "observed event intensity" in " ".join(capability.permitted_uses))
            matrix_censored = bool(capability and any(
                marker in capability.coverage_censoring_limitations.lower()
                for marker in ("censored", "event-filtered", "population completeness")
            ))
            if not capability_use:
                coverage_state = LiquidationCoverageV2.UNKNOWN
                state = "LIQUIDATION_NOT_ESTIMABLE_CAPABILITY_MISSING"
            elif matrix_censored:
                coverage_state = LiquidationCoverageV2.CENSORED
                state = (f"LIQUIDATION_PRINT_COVERAGE_{coverage_state.value}"
                         if observation.quantity is not None else "OBSERVED_LIQUIDATION_EVENT")
            elif (observation.quantity is not None and observation.coverage == LiquidationCoverageV2.QUALIFIED
                    and observation.feed_health == "HEALTHY_CURRENT" and observation.source_health_ref is not None):
                state = "OBSERVED_LIQUIDATION_PRINT"
            elif observation.quantity is not None:
                state = f"LIQUIDATION_PRINT_COVERAGE_{observation.coverage.value}"
            else:
                state = "OBSERVED_LIQUIDATION_EVENT"
            ref = observation.raw_content_ref
        else:
            if observation.event_at_ns is not None and event_at_ns != observation.event_at_ns:
                raise ValueError("OI stage event time must preserve the source observation timestamp")
            if oi_change_evidence is not None and event_at_ns != oi_change_evidence.end_event_at_ns:
                raise ValueError("OI stage event time must preserve the derived 15M endpoint")
            coverage_state = coverage or LiquidationCoverageV2.UNKNOWN
            oi_change = oi_change_evidence.change_fraction if oi_change_evidence is not None else None
            oi_healthy = observation.source_health == "HEALTHY_CURRENT" and observation.source_health_ref is not None
            evidence_matches = bool(
                oi_change_evidence and oi_change_evidence.instrument == observation.instrument
                and oi_change_evidence.source_id == observation.source_id
                and oi_change_evidence.channel == observation.channel
                and oi_change_evidence.end_ref == observation.raw_content_ref
                and oi_change_evidence.available_at_ns <= available_at_ns
                and oi_change_evidence.end_available_at_ns <= available_at_ns
                and oi_change_evidence.end_event_at_ns == event_at_ns
            )
            state = ("OI_CONTRACTION_OBSERVED" if oi_change is not None and oi_change < 0 and oi_healthy
                     and evidence_matches else "OI_CONTRACTION_NOT_ESTIMABLE_OR_ABSENT")
            ref = observation.raw_content_ref
            refs = ((ref, oi_change_evidence.content_hash, *oi_change_evidence.input_refs)
                    if oi_change_evidence else (ref,))
            stage_refs = tuple(sorted(set(refs)))
        if isinstance(observation, LiquidationObservationV2):
            stage_refs = tuple(sorted({ref, observation.content_hash,
                                       *([observation.source_health_ref]
                                         if observation.source_health_ref else [])}))
        else:
            stage_refs = tuple(sorted(set(stage_refs) | {observation.content_hash}
                                      | ({observation.source_health_ref}
                                         if observation.source_health_ref else set())))
        self.deleveraging_events.append((StageEvidenceV2("DELEVERAGING_EVIDENCE", event_at_ns, available_at_ns, stage_refs, state),
                                         LiquidationCoverageV2(coverage_state)))

    def confirm(self, *, event_at_ns: int, available_at_ns: int, confirmation_ref: str) -> None:
        visible = [item for item in self.deleveraging_events if item[0].available_at_ns <= available_at_ns]
        if (not self.breaks or not visible or not self.vulnerabilities
                or self.vulnerabilities[-1].state != "OBSERVED_CONTEXT"):
            return
        break_event = self.breaks[-1]
        deleveraging = visible[-1][0]
        vulnerability = self.vulnerabilities[-1]
        if (break_event.available_at_ns > available_at_ns or vulnerability.available_at_ns > available_at_ns
                or break_event.state != "CAUSAL_STRUCTURE_FAILURE"
                or deleveraging.state not in {"OBSERVED_LIQUIDATION_PRINT", "OI_CONTRACTION_OBSERVED"}):
            return
        earliest = max(break_event.available_at_ns, deleveraging.available_at_ns, vulnerability.available_at_ns) + self.confirmation_latency_ns
        if available_at_ns <= earliest:
            return
        self.confirmations.append(StageEvidenceV2("CONTINUATION_CONFIRMED", event_at_ns, available_at_ns,
                                                  (confirmation_ref,), "CONFIRMED_AFTER_EVIDENCE_LATENCY"))

    def artifact(self, *, cutoff_ns: int) -> S5ContinuationArtifactV2:
        cutoff = timestamp(cutoff_ns, field="cutoff_ns")

        def select(rows: list[StageEvidenceV2]) -> StageEvidenceV2 | None:
            return max((item for item in rows if item.available_at_ns <= cutoff),
                       key=lambda item: (item.available_at_ns, item.event_at_ns or 0, item.refs), default=None)

        vuln = select(self.vulnerabilities)
        brk = select(self.breaks)
        known_deleveraging = [(item, coverage) for item, coverage in self.deleveraging_events if item.available_at_ns <= cutoff]
        deleveraging, coverage = max(known_deleveraging, key=lambda pair: (pair[0].available_at_ns, pair[0].event_at_ns or 0),
                                     default=(None, LiquidationCoverageV2.UNKNOWN))
        confirm = select(self.confirmations)
        strong_deleveraging = deleveraging is not None and deleveraging.state in {
            "OBSERVED_LIQUIDATION_PRINT", "OI_CONTRACTION_OBSERVED"
        }
        if confirm:
            state = "CONTINUATION_CONFIRMED"
        elif strong_deleveraging and brk:
            state = "DELEVERAGING_EVIDENCE"
        elif brk:
            state = "BREAK"
        elif vuln:
            state = "VULNERABILITY"
        else:
            state = "NOT_ESTIMABLE"
        if deleveraging is not None and deleveraging.state not in {"OBSERVED_LIQUIDATION_PRINT", "OI_CONTRACTION_OBSERVED"}:
            fallback = "NOT ESTIMABLE: liquidation coverage censored/unknown or OI contraction unavailable; wait for qualified evidence"
        elif not strong_deleveraging and brk:
            fallback = "TEST GATE: WAIT_FOR_CUTOFF_KNOWN_OI_OR_COVERED_LIQUIDATION_EVIDENCE"
        elif coverage in {LiquidationCoverageV2.UNKNOWN, LiquidationCoverageV2.DEGRADED}:
            fallback = "NOT ESTIMABLE: liquidation coverage unknown/degraded; use cutoff-known OI contraction only"
        else:
            fallback = "OBSERVED_DELEVERAGING_WITH_COVERAGE_CAVEAT"
        latency = (confirm.available_at_ns - max(brk.available_at_ns, deleveraging.available_at_ns, vuln.available_at_ns)
                   if confirm and brk and deleveraging and vuln else None)
        return S5ContinuationArtifactV2(cutoff, state, vuln, brk, deleveraging, confirm,
                                        coverage, fallback, latency)


@dataclass(frozen=True)
class S5ReversalArtifactV2:
    cutoff_ns: int
    state: str
    liquidation_refs: tuple[str, ...]
    exhaustion_ref: str | None
    absorption_ref: str | None
    reclaim_ref: str | None
    flow_reversal_ref: str | None
    s4_state: str
    missing_reason: str | None
    liquidation_window_ref: str | None = None
    liquidation_baseline_ref: str | None = None
    liquidation_intensity_z: Decimal | None = None
    exact_action_status: str = "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT"
    policy_version: str = S5_REVERSAL_VERSION
    threshold_status: str = "ENGINEERING_RESEARCH_DEFAULT_UNQUALIFIED"
    stage_evidence: tuple[StageEvidenceV2, ...] = ()
    liquidation_coverage: LiquidationCoverageV2 = LiquidationCoverageV2.UNKNOWN

    def __post_init__(self) -> None:
        timestamp(self.cutoff_ns, field="cutoff_ns")
        nonblank(self.state, field="state")
        nonblank(self.s4_state, field="s4_state")
        object.__setattr__(self, "liquidation_coverage", LiquidationCoverageV2(self.liquidation_coverage))
        for ref in self.liquidation_refs:
            sha256_ref(ref, field="liquidation_ref")
        for name in ("exhaustion_ref", "absorption_ref", "reclaim_ref", "flow_reversal_ref"):
            val = getattr(self, name)
            if val is not None:
                sha256_ref(val, field=name)
        if self.missing_reason is not None:
            nonblank(self.missing_reason, field="missing_reason")
        for name in ("liquidation_window_ref", "liquidation_baseline_ref"):
            val = getattr(self, name)
            if val is not None:
                sha256_ref(val, field=name)
        if self.liquidation_intensity_z is not None:
            object.__setattr__(self, "liquidation_intensity_z", decimal_value(self.liquidation_intensity_z, field="liquidation_intensity_z"))
        nonblank(self.threshold_status, field="threshold_status")

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, "policy_version": self.policy_version,
                "policy_hash": S5_REVERSAL_POLICY_HASH, "cutoff_ns": self.cutoff_ns,
                "state": self.state, "liquidation_refs": list(self.liquidation_refs),
                "exhaustion_ref": self.exhaustion_ref, "absorption_ref": self.absorption_ref,
                "reclaim_ref": self.reclaim_ref, "flow_reversal_ref": self.flow_reversal_ref,
                "s4_state": self.s4_state, "missing_reason": self.missing_reason,
                "liquidation_window_ref": self.liquidation_window_ref,
                "liquidation_baseline_ref": self.liquidation_baseline_ref,
                "liquidation_intensity_z": str(self.liquidation_intensity_z) if self.liquidation_intensity_z is not None else None,
                "liquidation_coverage": self.liquidation_coverage.value,
                "threshold_status": self.threshold_status,
                "stage_evidence": [item.to_dict() for item in self.stage_evidence],
                "exact_action_status": self.exact_action_status, "selector_influence": "ZERO"}

    @property
    def content_hash(self) -> str:
        return sha256_json({"artifact_type": "S5ReversalArtifactV2", "artifact": self.to_dict()})


def _visible_reversal_stages(*, cutoff: int, s4: S4FeatureArtifactV2,
                             absorption: S4AbsorptionHypothesisV2 | None,
                             liquidation_window: LiquidationWindowTotalV2 | None,
                             liquidation_baseline: S5LiquidationBaselineV2 | None,
                             exhaustion: StageEvidenceV2 | None, reclaim: StageEvidenceV2 | None,
                             flow_reversal: StageEvidenceV2 | None,
                             liquidation_state: str) -> tuple[StageEvidenceV2, ...]:
    rows: list[StageEvidenceV2] = []
    if liquidation_baseline is not None and liquidation_baseline.cutoff_ns <= cutoff:
        rows.append(StageEvidenceV2(
            "LIQUIDATION_BASELINE", liquidation_baseline.cutoff_ns, liquidation_baseline.cutoff_ns,
            (liquidation_baseline.content_hash, *liquidation_baseline.training_refs),
            "PRIOR_ONLY_VENUE_BASELINE",
        ))
    if liquidation_window is not None and liquidation_window.available_at_ns <= cutoff:
        rows.append(StageEvidenceV2(
            "LIQUIDATION_INTENSITY", liquidation_window.window_end_ns,
            liquidation_window.available_at_ns,
            (liquidation_window.content_hash, *liquidation_window.input_refs,
             liquidation_window.source_health_ref), liquidation_state,
        ))
    if s4.cutoff_ns <= cutoff:
        rows.append(StageEvidenceV2(
            "S4_FLOW_STATE", s4.cutoff_ns, s4.cutoff_ns,
            (s4.content_hash, *s4.input_refs),
            "VALID_SEQUENCE_FLOW" if s4.estimable else f"NOT_ESTIMABLE_{s4.sequence_state.value}",
        ))
    if absorption is not None and absorption.cutoff_ns <= cutoff:
        rows.append(StageEvidenceV2(
            "S4_ABSORPTION", absorption.cutoff_ns, absorption.cutoff_ns,
            (absorption.content_hash, *absorption.evidence_refs), absorption.state,
        ))
    for stage in (exhaustion, reclaim, flow_reversal):
        if stage is not None and stage.available_at_ns <= cutoff:
            rows.append(stage)
    return tuple(rows)


def evaluate_s5_reversal(*, cutoff_ns: int, s4: S4FeatureArtifactV2,
                         absorption: S4AbsorptionHypothesisV2 | None,
                         liquidation_window: LiquidationWindowTotalV2 | None,
                         liquidation_baseline: S5LiquidationBaselineV2 | None,
                         exhaustion: StageEvidenceV2 | None, reclaim: StageEvidenceV2 | None,
                         flow_reversal: StageEvidenceV2 | None,
                         minimum_liquidation_z: Decimal = Decimal("2")) -> S5ReversalArtifactV2:
    cutoff = timestamp(cutoff_ns, field="cutoff_ns")
    if not s4.estimable or s4.cutoff_ns > cutoff:
        return S5ReversalArtifactV2(cutoff, "NOT_ESTIMABLE_S4_FLOW_UNAVAILABLE", (),
                                    exhaustion.refs[0] if exhaustion and exhaustion.refs else None, None,
                                    reclaim.refs[0] if reclaim and reclaim.refs else None,
                                    flow_reversal.refs[0] if flow_reversal and flow_reversal.refs else None,
                                    s4.sequence_state.value, "S4_GAP_WARMUP_OR_CUTOFF_UNAVAILABLE",
                                    stage_evidence=_visible_reversal_stages(
                                        cutoff=cutoff, s4=s4, absorption=absorption,
                                        liquidation_window=liquidation_window,
                                        liquidation_baseline=liquidation_baseline,
                                        exhaustion=exhaustion, reclaim=reclaim, flow_reversal=flow_reversal,
                                        liquidation_state="NOT_ESTIMABLE_S4_FLOW_REQUIRED"),
                                    liquidation_coverage=liquidation_window.coverage if liquidation_window else LiquidationCoverageV2.UNKNOWN)
    if (liquidation_window is None or liquidation_baseline is None
            or liquidation_window.available_at_ns > cutoff or liquidation_baseline.cutoff_ns > cutoff):
        return S5ReversalArtifactV2(cutoff, "NOT_ESTIMABLE_LIQUIDATION_BASELINE_UNAVAILABLE", (),
                                    None, None, None, None, s4.sequence_state.value,
                                    "VENUE_SPECIFIC_PRIOR_LIQUIDATION_BASELINE_REQUIRED",
                                    stage_evidence=_visible_reversal_stages(
                                        cutoff=cutoff, s4=s4, absorption=absorption,
                                        liquidation_window=liquidation_window,
                                        liquidation_baseline=liquidation_baseline,
                                        exhaustion=exhaustion, reclaim=reclaim, flow_reversal=flow_reversal,
                                        liquidation_state="NOT_ESTIMABLE_BASELINE_REQUIRED"),
                                    liquidation_coverage=liquidation_window.coverage if liquidation_window else LiquidationCoverageV2.UNKNOWN)
    refs = liquidation_window.input_refs
    matrix = default_evidence_capability_matrix_v2()
    capability = capability_for_public_channel_v2(matrix, liquidation_window.instrument, liquidation_window.channel)
    capability_identity_ok = bool(
        liquidation_window.capability_matrix_ref == matrix.content_hash
        and capability is not None
        and "observed event intensity" in " ".join(capability.permitted_uses)
    )
    matrix_censored = bool(capability and any(
        marker in capability.coverage_censoring_limitations.lower()
        for marker in ("censored", "event-filtered", "population completeness")
    ))
    effective_coverage = (LiquidationCoverageV2.UNKNOWN if not capability_identity_ok
                          else LiquidationCoverageV2.CENSORED if matrix_censored
                          else liquidation_window.coverage)
    if (effective_coverage != LiquidationCoverageV2.QUALIFIED
            or liquidation_window.feed_health != "HEALTHY_CURRENT"):
        return S5ReversalArtifactV2(cutoff, "NOT_ESTIMABLE_LIQUIDATION_COVERAGE", refs,
                                    None, None, None, None, s4.sequence_state.value,
                                    "LIQUIDATION_COVERAGE_UNKNOWN_CENSORED_OR_DEGRADED",
                                    liquidation_window.content_hash, liquidation_baseline.content_hash,
                                    stage_evidence=_visible_reversal_stages(
                                        cutoff=cutoff, s4=s4, absorption=absorption,
                                        liquidation_window=liquidation_window,
                                        liquidation_baseline=liquidation_baseline,
                                        exhaustion=exhaustion, reclaim=reclaim, flow_reversal=flow_reversal,
                                    liquidation_state=f"COVERAGE_{effective_coverage.value}"),
                                    liquidation_coverage=effective_coverage)
    if (liquidation_window.instrument != s4.instrument
            or liquidation_baseline.instrument != s4.instrument
            or liquidation_window.source_id != liquidation_baseline.source_id
            or liquidation_window.quantity_unit != liquidation_baseline.quantity_unit
            or liquidation_window.channel != liquidation_baseline.channel
            or liquidation_window.capability_matrix_ref != liquidation_baseline.capability_matrix_ref
            or liquidation_baseline.capability_matrix_ref != matrix.content_hash
            or liquidation_baseline.coverage != LiquidationCoverageV2.QUALIFIED
            or liquidation_window.window_end_ns - liquidation_window.window_start_ns != liquidation_baseline.window_duration_ns):
        return S5ReversalArtifactV2(cutoff, "NOT_ESTIMABLE_LIQUIDATION_BASELINE_MISMATCH", refs,
                                    None, None, None, None, s4.sequence_state.value,
                                    "SOURCE_PRODUCT_UNIT_OR_WINDOW_MISMATCH",
                                    liquidation_window.content_hash, liquidation_baseline.content_hash,
                                    stage_evidence=_visible_reversal_stages(
                                        cutoff=cutoff, s4=s4, absorption=absorption,
                                        liquidation_window=liquidation_window,
                                        liquidation_baseline=liquidation_baseline,
                                        exhaustion=exhaustion, reclaim=reclaim, flow_reversal=flow_reversal,
                                        liquidation_state="NOT_ESTIMABLE_BASELINE_MISMATCH"),
                                    liquidation_coverage=liquidation_window.coverage)
    zscore = (liquidation_window.observed_quantity - liquidation_baseline.mean) / liquidation_baseline.std
    if zscore < decimal_value(minimum_liquidation_z, field="minimum_liquidation_z"):
        return S5ReversalArtifactV2(cutoff, "WATCHING_NO_UNUSUAL_LIQUIDATION_EXHAUSTION", refs,
                                    None, None, None, None, s4.sequence_state.value,
                                    "LIQUIDATION_INTENSITY_BELOW_PRIOR_ONLY_VENUE_BASELINE_THRESHOLD",
                                    liquidation_window.content_hash, liquidation_baseline.content_hash, zscore,
                                    stage_evidence=_visible_reversal_stages(
                                        cutoff=cutoff, s4=s4, absorption=absorption,
                                        liquidation_window=liquidation_window,
                                        liquidation_baseline=liquidation_baseline,
                                        exhaustion=exhaustion, reclaim=reclaim, flow_reversal=flow_reversal,
                                        liquidation_state="NO_UNUSUAL_LIQUIDATION_EXHAUSTION"),
                                    liquidation_coverage=liquidation_window.coverage)
    exhaustion_ok = bool(exhaustion and exhaustion.available_at_ns <= cutoff
                         and exhaustion.stage == "EXHAUSTION" and exhaustion.state == "EXHAUSTION_CONFIRMED")
    absorption_ok = bool(absorption and absorption.state == "ABSORPTION_HYPOTHESIS"
                         and absorption.feature_ref == s4.content_hash and absorption.cutoff_ns <= cutoff)
    if not exhaustion_ok or not absorption_ok:
        return S5ReversalArtifactV2(cutoff, "WATCHING_EXHAUSTION_ABSORPTION_REQUIRED", refs,
                                    exhaustion.refs[0] if exhaustion and exhaustion.refs else None,
                                    absorption.content_hash if absorption else None,
                                    None, None, s4.sequence_state.value,
                                    "LIQUIDATION_SPIKE_ALONE_IS_INSUFFICIENT",
                                    liquidation_window.content_hash, liquidation_baseline.content_hash, zscore,
                                    stage_evidence=_visible_reversal_stages(
                                        cutoff=cutoff, s4=s4, absorption=absorption,
                                        liquidation_window=liquidation_window,
                                        liquidation_baseline=liquidation_baseline,
                                        exhaustion=exhaustion, reclaim=reclaim, flow_reversal=flow_reversal,
                                        liquidation_state="LIQUIDATION_SPIKE_OBSERVED"),
                                    liquidation_coverage=liquidation_window.coverage)
    assert exhaustion is not None and absorption is not None
    reclaim_ok = bool(reclaim and reclaim.available_at_ns <= cutoff
                      and reclaim.stage == "RECLAIM" and reclaim.state == "RECLAIM_CONFIRMED")
    flow_ok = bool(flow_reversal and flow_reversal.available_at_ns <= cutoff
                   and flow_reversal.stage == "FLOW_REVERSAL" and flow_reversal.state == "FLOW_REVERSAL_CONFIRMED")
    if not reclaim_ok and not flow_ok:
        return S5ReversalArtifactV2(cutoff, "EXHAUSTION_WAITING_FOR_RECLAIM_OR_FLOW_REVERSAL", refs,
                                    exhaustion.refs[0], absorption.content_hash, None, None,
                                    s4.sequence_state.value, "RECLAIM_OR_FLOW_REVERSAL_REQUIRED",
                                    liquidation_window.content_hash, liquidation_baseline.content_hash, zscore,
                                    stage_evidence=_visible_reversal_stages(
                                        cutoff=cutoff, s4=s4, absorption=absorption,
                                        liquidation_window=liquidation_window,
                                        liquidation_baseline=liquidation_baseline,
                                        exhaustion=exhaustion, reclaim=reclaim, flow_reversal=flow_reversal,
                                        liquidation_state="EXHAUSTION_AWAITING_RECLAIM_OR_FLOW_REVERSAL"),
                                    liquidation_coverage=liquidation_window.coverage)
    return S5ReversalArtifactV2(cutoff, "POST_CASCADE_REVERSAL_CONFIRMED", refs,
                                exhaustion.refs[0], absorption.content_hash,
                                reclaim.refs[0] if reclaim_ok and reclaim and reclaim.refs else None,
                                flow_reversal.refs[0] if flow_ok and flow_reversal and flow_reversal.refs else None,
                                s4.sequence_state.value, None, liquidation_window.content_hash,
                                liquidation_baseline.content_hash, zscore,
                                stage_evidence=_visible_reversal_stages(
                                    cutoff=cutoff, s4=s4, absorption=absorption,
                                    liquidation_window=liquidation_window,
                                    liquidation_baseline=liquidation_baseline,
                                    exhaustion=exhaustion, reclaim=reclaim, flow_reversal=flow_reversal,
                                    liquidation_state="LIQUIDATION_SPIKE_CONFIRMED"),
                                liquidation_coverage=liquidation_window.coverage)
