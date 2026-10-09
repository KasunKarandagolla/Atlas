"""Whole-calendar selection, multiplicity, and feature-family audit contracts."""

from __future__ import annotations

import random
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from atlas.domain.money import canonical_decimal_str
from atlas.v2._serialization import FrozenMap, json_value, sha256_json, sha256_ref
from atlas.v2.contracts import CandidateSetV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.science.outcomes import (
    AdmissionStateV2,
    DecisionCalendarEntryV2,
    MaturedOutcomeV2,
    OutcomeTargetV2,
    SelectionStateV2,
    executable_action_value_training_eligible,
    index_matured_outcome,
)

SELECTION_AUDIT_VERSION = "WHOLE_CALENDAR_SELECTION_AUDIT_V2_V1"
MULTIPLICITY_VERSION = "DEPENDENCE_BLOCK_HOLM_AUDIT_V2_V1"
ABLATION_VERSION = "WHOLE_POLICY_FEATURE_ABLATION_V2_V1"
ABLATION_FAMILIES = (
    "candles", "smc_structure", "support_resistance", "fibonacci", "elliott_measurable_morphology",
    "wyckoff_measurable_challengers", "ordinary_time_calendar", "killzone_time_window_challengers",
    "derivatives_crowding", "s4_flow_context",
)


@dataclass(frozen=True)
class SelectionCalendarRowV2:
    decision_ref: str
    decision_event_id: str
    decision_at_ns: int
    candidate_set_ref: str
    candidate_ref: str | None
    policy_id: str
    selection_state: str
    admission_state: str
    reason_codes: tuple[str, ...]
    outcome_refs: tuple[str, ...]
    outcome_provenance: tuple[str, ...]
    execution_states: tuple[str, ...]
    realized_values: tuple[Decimal, ...]
    exploration_probability: Decimal | None

    def to_dict(self) -> dict[str, Any]:
        return {"decision_ref": self.decision_ref, "decision_event_id": self.decision_event_id,
            "decision_at_ns": self.decision_at_ns, "candidate_set_ref": self.candidate_set_ref,
            "candidate_ref": self.candidate_ref, "policy_id": self.policy_id,
            "selection_state": self.selection_state, "admission_state": self.admission_state,
            "reason_codes": list(self.reason_codes), "outcome_refs": list(self.outcome_refs),
            "outcome_provenance": list(self.outcome_provenance), "execution_states": list(self.execution_states),
            "realized_values": [canonical_decimal_str(value) for value in self.realized_values],
            "exploration_probability": canonical_decimal_str(self.exploration_probability)
                if self.exploration_probability is not None else None}


@dataclass(frozen=True)
class SelectionPolicyAuditV2:
    audit_id: str
    start_ns: int
    end_ns: int
    source_calendar_refs: tuple[str, ...]
    rows: tuple[SelectionCalendarRowV2, ...]
    selection_coverage: Decimal | None
    missed_value_share: Decimal | None
    warmup_exclusion_rate: Decimal | None
    deadline_loss_rate: Decimal | None
    selection_lift: Decimal | None
    block_uncertainty: tuple[Decimal, Decimal] | None
    inverse_probability_status: str
    provenance_counts: tuple[tuple[str, int], ...]
    fill_state_counts: tuple[tuple[str, int], ...]
    status: str
    reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        calendar_sets = {row.candidate_set_ref for row in self.rows}
        selected_sets = {row.candidate_set_ref for row in self.rows if row.selection_state == "SELECTED"}
        calendar_coverage = Decimal(len(selected_sets)) / Decimal(len(calendar_sets)) if calendar_sets else None
        return {"version": SELECTION_AUDIT_VERSION, "audit_id": self.audit_id,
            "start_ns": self.start_ns, "end_ns": self.end_ns,
            "source_calendar_refs": list(self.source_calendar_refs), "rows": [row.to_dict() for row in self.rows],
            "selection_coverage": canonical_decimal_str(self.selection_coverage) if self.selection_coverage is not None else None,
            "decision_calendar_selection_coverage": canonical_decimal_str(calendar_coverage) if calendar_coverage is not None else None,
            "metric_definitions": {"decision_calendar_selection_coverage": "selected decision sets / all retained calendar decision sets",
                "selection_coverage": "selected positive net value / total positive net value across fully qualified competitors",
                "warmup_exclusion_rate": "warmup exclusions / retained calendar member rows",
                "deadline_loss_rate": "expired or deadline-lost rows / retained calendar member rows",
                "uncertainty": "synchronized contiguous decision-time blocks covering maximum policy horizon; at least 20 blocks"},
            "missed_value_share": canonical_decimal_str(self.missed_value_share) if self.missed_value_share is not None else None,
            "warmup_exclusion_rate": canonical_decimal_str(self.warmup_exclusion_rate) if self.warmup_exclusion_rate is not None else None,
            "deadline_loss_rate": canonical_decimal_str(self.deadline_loss_rate) if self.deadline_loss_rate is not None else None,
            "selection_lift": canonical_decimal_str(self.selection_lift) if self.selection_lift is not None else None,
            "block_uncertainty": [canonical_decimal_str(v) for v in self.block_uncertainty]
                if self.block_uncertainty is not None else None,
            "inverse_probability_status": self.inverse_probability_status,
            "provenance_counts": [[key, value] for key, value in self.provenance_counts],
            "fill_state_counts": [[key, value] for key, value in self.fill_state_counts],
            "status": self.status, "reasons": list(self.reasons)}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def build_selection_policy_audit(repo: OpsRepository, *, audit_id: str, start_ns: int, end_ns: int,
        exploration_probabilities: Mapping[str, Decimal] | None = None, exploration_design_ref: str | None = None,
        block_length: int = 4,
        bootstrap_replicates: int = 1000, seed: int = 23023) -> SelectionPolicyAuditV2:
    """Retain every calendar/member state and measure only complete supported populations."""
    if not audit_id or start_ns < 0 or end_ns <= start_ns or block_length < 1 or bootstrap_replicates < 1:
        raise ValueError("selection audit window/configuration invalid")
    exploration_probabilities = exploration_probabilities or {}
    if exploration_probabilities:
        design = repo.get_artifact(exploration_design_ref) if exploration_design_ref else None
        if (design is None or design.artifact_type != "DeterministicExplorationDesignV2"
            or design.available_at_ns > start_ns or design.metadata.get("preregistered") is not True
            or design.metadata.get("inclusion_probabilities") is None
            or sha256_json(design.metadata["inclusion_probabilities"]) != sha256_json(exploration_probabilities)):
            raise ValueError("inclusion probabilities require the causal preregistered deterministic exploration design")
    for candidate_ref, probability in exploration_probabilities.items():
        sha256_ref(candidate_ref, field="candidate_ref")
        if not Decimal(0) < probability <= Decimal(1):
            raise ValueError("only positive known exploration inclusion probabilities are allowed")
    calendar: list[tuple[DecisionCalendarEntryV2, ArtifactIndexEntryV2]] = []
    outcomes_by_decision: dict[str, list[MaturedOutcomeV2]] = defaultdict(list)
    outcomes_by_candidate: dict[tuple[str, str], list[MaturedOutcomeV2]] = defaultdict(list)
    for entry in repo.artifact_entries("MaturedOutcomeV2"):
        raw = entry.metadata.get("outcome")
        if isinstance(raw, Mapping):
            outcome = MaturedOutcomeV2.from_dict(json_value(raw))
            if outcome.decision_at_ns < end_ns and outcome.available_at_ns <= end_ns:
                if outcome.content_hash != entry.content_hash:
                    raise ValueError("selection audit outcome index differs from immutable outcome")
                index_matured_outcome(repo, outcome)
                outcomes_by_decision[outcome.decision_ref].append(outcome)
                if outcome.candidate_ref is not None:
                    outcomes_by_candidate[(outcome.candidate_ref, outcome.candidate_set_ref)].append(outcome)
    for entry in repo.artifact_entries("DecisionCalendarEntryV2"):
        raw = entry.metadata.get("decision_entry")
        if not isinstance(raw, Mapping):
            continue
        decision = DecisionCalendarEntryV2.from_dict(json_value(raw))
        if decision.content_hash != entry.content_hash:
            raise ValueError("selection audit calendar index differs from immutable decision")
        if start_ns <= decision.decision_at_ns < end_ns and decision.available_at_ns <= end_ns:
            calendar.append((decision, entry))
    rows: list[SelectionCalendarRowV2] = []
    counts_provenance: dict[str, int] = defaultdict(int)
    counts_fill: dict[str, int] = defaultdict(int)
    counted_outcomes: set[str] = set()
    source_refs: set[str] = set()
    candidate_action_entries: dict[tuple[str, str], ArtifactIndexEntryV2] = {}
    # Resolve each member from that set's immutable input references. A later
    # candidate revision sharing an ID cannot replace a prior calendar member.
    for entry in repo.artifact_entries("CandidateSetV2"):
        raw_set = entry.metadata.get("candidate_set")
        if entry.available_at_ns > end_ns or not isinstance(raw_set, Mapping):
            continue
        source_set = CandidateSetV2.from_dict(json_value(raw_set))
        member_ids = {member.candidate_id for member in source_set.candidates}
        for ref in source_set.envelope.input_refs:
            source = repo.get_artifact(ref)
            if source is None or source.artifact_type != "CandidateActionV2":
                continue
            body = source.metadata.get("candidate")
            if not isinstance(body, Mapping) or body.get("candidate_id") not in member_ids:
                continue
            if source.available_at_ns > source_set.envelope.available_at_ns:
                raise ValueError("selection audit candidate was unavailable at immutable selection cutoff")
            identity = (source_set.content_hash, str(body["candidate_id"]))
            if identity in candidate_action_entries and candidate_action_entries[identity].content_hash != ref:
                raise ValueError("selection audit has contradictory exact candidate refs for one set member")
            candidate_action_entries[identity] = source
    for decision, indexed in sorted(calendar, key=lambda item: (item[0].decision_at_ns, item[0].available_at_ns, item[0].content_hash)):
        source_refs.add(indexed.content_hash)
        candidate_set_entry = repo.get_artifact(decision.candidate_set_ref)
        set_body = candidate_set_entry.metadata.get("candidate_set") if candidate_set_entry else None
        candidate_set = CandidateSetV2.from_dict(json_value(set_body)) if isinstance(set_body, Mapping) else None
        outcomes = tuple(sorted(outcomes_by_decision.get(indexed.content_hash, ()),
            key=lambda item: (item.available_at_ns, item.content_hash)))
        if decision.candidate_ref is not None:
            outcomes = tuple(sorted({item.content_hash: item for item in (
                *outcomes, *outcomes_by_candidate.get((decision.candidate_ref, decision.candidate_set_ref), ()))
            }.values(), key=lambda item: (item.available_at_ns, item.content_hash)))
        if any(not executable_action_value_training_eligible(outcome, end_ns)
            and outcome.outcome_target == OutcomeTargetV2.EXECUTABLE_ACTION_VALUE for outcome in outcomes):
            outcomes = tuple(item for item in outcomes if executable_action_value_training_eligible(item, end_ns))
        for outcome in outcomes:
            if outcome.content_hash not in counted_outcomes:
                counts_provenance[outcome.provenance.value] += 1
                counts_fill[outcome.execution_state.value] += 1
                counted_outcomes.add(outcome.content_hash)
        rows.append(SelectionCalendarRowV2(indexed.content_hash,
            candidate_set.decision_event_id if candidate_set is not None else decision.decision_identity_ref,
            decision.decision_at_ns, decision.candidate_set_ref, decision.candidate_ref, decision.policy_id,
            decision.selection_state.value, decision.admission_state.value, decision.reason_codes,
            tuple(item.content_hash for item in outcomes), tuple(item.provenance.value for item in outcomes),
            tuple(item.execution_state.value for item in outcomes),
            tuple(item.net_payoff for item in outcomes if item.net_payoff is not None),
            exploration_probabilities.get(decision.candidate_ref or "")))
        if candidate_set is None:
            continue
        source_refs.add(candidate_set.content_hash)
        represented = {item.candidate_ref for item in rows
            if item.candidate_set_ref == candidate_set.content_hash and item.candidate_ref is not None}
        for member in candidate_set.candidates:
            candidate_entry = candidate_action_entries.get((candidate_set.content_hash, member.candidate_id))
            if candidate_entry is None:
                continue
            if candidate_entry.content_hash in represented:
                continue
            represented.add(candidate_entry.content_hash)
            source_refs.add(candidate_entry.content_hash)
            state = (SelectionStateV2.UNSELECTED.value if member.eligibility_status.value == "ELIGIBLE"
                else SelectionStateV2.REJECTED.value if member.eligibility_status.value == "INELIGIBLE"
                else SelectionStateV2.NOT_ESTIMABLE.value)
            member_outcomes = tuple(sorted((item for item in outcomes_by_candidate.get(
                (candidate_entry.content_hash, candidate_set.content_hash), ())
                if executable_action_value_training_eligible(item, end_ns)),
                key=lambda item: (item.available_at_ns, item.content_hash)))
            for outcome in member_outcomes:
                if outcome.content_hash not in counted_outcomes:
                    counts_provenance[outcome.provenance.value] += 1
                    counts_fill[outcome.execution_state.value] += 1
                    counted_outcomes.add(outcome.content_hash)
            rows.append(SelectionCalendarRowV2(candidate_set.content_hash, candidate_set.decision_event_id,
                candidate_set.envelope.available_at_ns, candidate_set.content_hash, candidate_entry.content_hash,
                member.policy_id, state, AdmissionStateV2.NOT_APPLICABLE.value,
                (member.rejection_reason,) if member.rejection_reason else (),
                tuple(item.content_hash for item in member_outcomes),
                tuple(item.provenance.value for item in member_outcomes),
                tuple(item.execution_state.value for item in member_outcomes),
                tuple(item.net_payoff for item in member_outcomes if item.net_payoff is not None),
                exploration_probabilities.get(candidate_entry.content_hash)))
    # For a full missed-value/coverage diagnostic every exact-action member in
    # each set must have a mature, defensible outcome. Counterfactual outcomes
    # are consumed only as already-qualified evidence; no fills are generated.
    sets = {row.candidate_set_ref for row in rows}
    positive_total = Decimal(0)
    positive_selected = Decimal(0)
    positive_missed = Decimal(0)
    complete_events = 0
    lift_by_time: dict[int, Decimal] = {}
    all_complete = bool(rows)
    warmup_rows = sum(any("WARMUP" in reason for reason in row.reason_codes) for row in rows)
    deadline_rows = sum(row.admission_state == AdmissionStateV2.EXPIRED.value or
        any("DEADLINE" in reason or "EXPIRED" in reason for reason in row.reason_codes) for row in rows)
    for set_ref in sets:
        member_rows = [row for row in rows if row.candidate_set_ref == set_ref and row.candidate_ref is not None]
        candidate_set_entry = repo.get_artifact(set_ref)
        set_body = candidate_set_entry.metadata.get("candidate_set") if candidate_set_entry else None
        if not isinstance(set_body, Mapping):
            all_complete = False
            continue
        candidate_set = CandidateSetV2.from_dict(json_value(set_body))
        if not candidate_set.candidates:
            continue
        by_candidate = {row.candidate_ref: row for row in member_rows}
        event_values: dict[str, Decimal] = {}
        for member in candidate_set.candidates:
            candidate_entry = candidate_action_entries.get((candidate_set.content_hash, member.candidate_id))
            if candidate_entry is None:
                all_complete = False
                continue
            row = by_candidate.get(candidate_entry.content_hash)
            if row is None or len(row.realized_values) != 1:
                all_complete = False
                continue
            event_values[member.candidate_id] = row.realized_values[0]
        if len(event_values) != len(candidate_set.candidates):
            all_complete = False
            continue
        complete_events += 1
        best_row = next((row for row in rows if row.candidate_set_ref == set_ref
            and row.selection_state == SelectionStateV2.SELECTED.value), None)
        if best_row is None or best_row.candidate_ref is None:
            selected_id = None
        else:
            selected_entry = repo.get_artifact(best_row.candidate_ref)
            body = selected_entry.metadata.get("candidate") if selected_entry else None
            selected_id = body.get("candidate_id") if isinstance(body, Mapping) else None
        event_total = sum((max(Decimal(0), value) for value in event_values.values()), Decimal(0))
        selected_value = max(Decimal(0), event_values.get(selected_id, Decimal(0))) if selected_id else Decimal(0)
        positive_total += event_total
        positive_selected += selected_value
        positive_missed += event_total - selected_value
        alternatives = [value for key, value in event_values.items() if key != selected_id]
        if selected_id and alternatives:
            lift_by_time[candidate_set.envelope.available_at_ns] = event_values[selected_id] - sum(alternatives, Decimal(0)) / len(alternatives)
    reasons: list[str] = []
    coverage = missed = lift = None
    interval = None
    if all_complete and complete_events > 0 and positive_total > 0:
        coverage = positive_selected / positive_total
        missed = positive_missed / positive_total
        if lift_by_time:
            values = [float(value) for _, value in sorted(lift_by_time.items())]
            lift = Decimal(str(sum(values) / len(values)))
            times = sorted(lift_by_time)
            horizons = []
            for row in rows:
                candidate_entry = repo.get_artifact(row.candidate_ref) if row.candidate_ref else None
                body = candidate_entry.metadata.get("candidate") if candidate_entry else None
                if isinstance(body, Mapping):
                    horizons.append(int(body["horizon_end_ns"]) - int(body["decision_at_ns"]))
            horizon = max(horizons, default=0)
            positive_spacings = [right - left for left, right in zip(times, times[1:], strict=False) if right > left]
            if positive_spacings:
                block_length = max(block_length, (horizon + min(positive_spacings) - 1) // min(positive_spacings) + 1)
            if len(values) // block_length >= 20:
                rng = random.Random(seed)
                boot = []
                for _ in range(bootstrap_replicates):
                    sampled: list[float] = []
                    while len(sampled) < len(values):
                        start = rng.randrange(max(1, len(values) - block_length + 1))
                        sampled.extend(values[start:start + block_length])
                    boot.append(sum(sampled[:len(values)]) / len(values))
                boot.sort()
                interval = (Decimal(str(boot[int(0.025 * (len(boot) - 1))])),
                    Decimal(str(boot[int(0.975 * (len(boot) - 1))])))
            else:
                reasons.append("INSUFFICIENT_DECISION_TIME_BLOCKS_FOR_SELECTION_LIFT_UNCERTAINTY")
    else:
        reasons.append("INCOMPLETE_MATURED_COMPETITOR_OUTCOMES_OR_NO_POSITIVE_VALUE_SUPPORT")
    warmup_rate = Decimal(warmup_rows) / Decimal(len(rows)) if rows else None
    deadline_rate = Decimal(deadline_rows) / Decimal(len(rows)) if rows else None
    known_ipw = bool(exploration_probabilities) and all(
        row.candidate_ref is None or row.candidate_ref in exploration_probabilities for row in rows)
    ipw_status = "KNOWN_EXPLORATION_PROBABILITIES" if known_ipw else "NOT_ESTIMABLE_NO_KNOWN_INCLUSION_PROBABILITIES"
    status = "ESTIMABLE" if coverage is not None and missed is not None else "NOT_ESTIMABLE"
    return SelectionPolicyAuditV2(audit_id, start_ns, end_ns, tuple(sorted(source_refs)),
        tuple(sorted(rows, key=lambda row: (row.decision_at_ns, row.candidate_set_ref, row.candidate_ref or ""))),
        coverage, missed, warmup_rate, deadline_rate, lift, interval, ipw_status,
        tuple(sorted(counts_provenance.items())), tuple(sorted(counts_fill.items())), status,
        tuple(sorted(set(reasons))))


@dataclass(frozen=True)
class MultiplicityVariantV2:
    variant_id: str
    variant_hash: str
    decision_times_ns: tuple[int, ...]
    paired_after_cost_values: tuple[Decimal, ...]
    failure_reason: str | None = None
    outer_window_refs: tuple[str, ...] = ()
    whole_policy_outcome_refs: tuple[str, ...] = ()
    evidence_origin: str = "UNVERIFIED"
    independent_support_count: int = 0
    maximum_policy_horizon_ns: int = 0

    def __post_init__(self) -> None:
        sha256_ref(self.variant_hash, field="variant_hash")
        if not self.variant_id or len(self.decision_times_ns) != len(self.paired_after_cost_values):
            raise ValueError("multiplicity variant identity/value width invalid")
        if tuple(sorted(set(self.decision_times_ns))) != self.decision_times_ns:
            raise ValueError("multiplicity decision times must be sorted and unique")
        if self.failure_reason is None and not self.paired_after_cost_values:
            raise ValueError("an evaluated multiplicity variant requires decision outcomes")
        for ref in (*self.outer_window_refs, *self.whole_policy_outcome_refs):
            sha256_ref(ref, field="multiplicity_source_ref")
        if any(not value.is_finite() for value in self.paired_after_cost_values):
            raise ValueError("multiplicity whole-policy values must be finite")
        if self.independent_support_count < 0 or self.maximum_policy_horizon_ns < 0:
            raise ValueError("multiplicity support/horizon cannot be negative")


@dataclass(frozen=True)
class MultiplicityMemberResultV2:
    variant_id: str
    variant_hash: str
    raw_mean_difference: Decimal | None
    raw_p_value: Decimal | None
    adjusted_p_value: Decimal
    holm_threshold: Decimal
    effective_independent_blocks: int
    bootstrap_low: Decimal | None
    bootstrap_high: Decimal | None
    status: str
    failure_reason: str | None

    def to_dict(self) -> dict[str, Any]:
        return {name: canonical_decimal_str(value) if isinstance(value, Decimal) else value
            for name, value in self.__dict__.items()}


@dataclass(frozen=True)
class MultiplicityAuditV2:
    family_id: str
    preregistration_ref: str
    alpha: Decimal
    baseline_variant_id: str
    members: tuple[MultiplicityMemberResultV2, ...]
    block_length: int
    bootstrap_replicates: int
    seed: int
    dependence_method: str
    effective_support: int
    status: str
    decision: str
    family_inputs: tuple[FrozenMap, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {"version": MULTIPLICITY_VERSION, "family_id": self.family_id,
            "preregistration_ref": self.preregistration_ref, "alpha": canonical_decimal_str(self.alpha),
            "baseline_variant_id": self.baseline_variant_id,
            "members": [member.to_dict() for member in self.members], "block_length": self.block_length,
            "bootstrap_replicates": self.bootstrap_replicates, "seed": self.seed,
            "dependence_method": self.dependence_method, "effective_support": self.effective_support,
            "status": self.status, "decision": self.decision,
            "family_inputs": [body.to_dict() for body in self.family_inputs]}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def build_multiplicity_audit(*, family_id: str, preregistration_ref: str, baseline_variant_id: str,
        variants: Sequence[MultiplicityVariantV2], expected_family_member_ids: Sequence[str],
        alpha: Decimal = Decimal("0.05"), block_length: int = 4,
        bootstrap_replicates: int = 2000, seed: int = 23023) -> MultiplicityAuditV2:
    """Synchronized contiguous block bootstrap of paired whole-policy deltas, then Holm FWER."""
    sha256_ref(preregistration_ref, field="preregistration_ref")
    if not family_id or not Decimal(0) < alpha < Decimal(1) or block_length < 1 or bootstrap_replicates < 100:
        raise ValueError("multiplicity preregistration/configuration invalid")
    ids = tuple(sorted(variant.variant_id for variant in variants))
    expected = tuple(sorted(set(expected_family_member_ids)))
    if ids != expected or len(ids) != len(variants):
        raise ValueError("multiplicity family must retain every preregistered attempted variant exactly once")
    baseline = next((variant for variant in variants if variant.variant_id == baseline_variant_id), None)
    if baseline is None:
        raise ValueError("multiplicity baseline is absent from the complete family")
    testable: dict[str, tuple[float, ...]] = {}
    failed: dict[str, str] = {}
    common_times: tuple[int, ...] | None = None
    for variant in variants:
        if baseline.failure_reason is not None:
            failed[variant.variant_id] = "BASELINE_FAILED_OR_ABANDONED"
            continue
        if variant.failure_reason:
            failed[variant.variant_id] = variant.failure_reason
            continue
        if variant.decision_times_ns != baseline.decision_times_ns:
            failed[variant.variant_id] = "INCOMPLETE_OR_MISMATCHED_WHOLE_CALENDAR"
            continue
        common_times = variant.decision_times_ns
        if variant.variant_id == baseline_variant_id:
            testable[variant.variant_id] = tuple(0.0 for _ in variant.paired_after_cost_values)
        else:
            testable[variant.variant_id] = tuple(float(value - base)
                for value, base in zip(variant.paired_after_cost_values, baseline.paired_after_cost_values, strict=True))
    n = len(common_times or ())
    required_horizon = max((variant.maximum_policy_horizon_ns for variant in variants), default=0)
    times = common_times or ()
    while block_length < n and any(times[i + block_length - 1] - times[i] < required_horizon
        for i in range(n - block_length + 1)):
        block_length += 1
    qualified = all(variant.evidence_origin == "GENUINE_HISTORICAL"
        and len(set(variant.outer_window_refs)) >= 3 and variant.independent_support_count >= 20
        and len(variant.whole_policy_outcome_refs) == len(variant.decision_times_ns)
        and variant.maximum_policy_horizon_ns > 0 for variant in variants if variant.failure_reason is None)
    independent = min(n // block_length, min((variant.independent_support_count for variant in variants
        if variant.failure_reason is None), default=0)) if qualified else 0
    rng = random.Random(seed)
    block_samples: list[list[int]] = []
    if n:
        for _ in range(bootstrap_replicates):
            sample: list[int] = []
            while len(sample) < n:
                start = rng.randrange(max(1, n - block_length + 1))
                sample.extend(range(start, min(n, start + block_length)))
            block_samples.append(sample[:n])
    stats: dict[str, tuple[float, float, float, float]] = {}
    for variant_id, differences in testable.items():
        mean = sum(differences) / len(differences) if differences else 0.0
        centered = [value - mean for value in differences]
        null_boot = [sum(centered[index] for index in sample) / max(1, n) for sample in block_samples]
        value_boot = [sum(differences[index] for index in sample) / max(1, n) for sample in block_samples]
        p = (sum(value >= mean for value in null_boot) + 1) / (len(null_boot) + 1) if independent >= 20 else 1.0
        value_boot.sort()
        low = value_boot[int(0.025 * (len(value_boot) - 1))] if value_boot else 0.0
        high = value_boot[int(0.975 * (len(value_boot) - 1))] if value_boot else 0.0
        stats[variant_id] = (mean, p, low, high)
    raw = {variant_id: (Decimal(str(stats[variant_id][1])) if variant_id in stats and independent >= 20 else Decimal(1))
        for variant_id in ids}
    ranked = sorted(ids, key=lambda variant_id: (raw[variant_id], variant_id))
    adjusted: dict[str, Decimal] = {}
    thresholds: dict[str, Decimal] = {}
    running = Decimal(0)
    for index, variant_id in enumerate(ranked):
        multiplier = len(ids) - index
        threshold = alpha / Decimal(multiplier)
        thresholds[variant_id] = threshold
        running = max(running, min(Decimal(1), raw[variant_id] * Decimal(multiplier)))
        adjusted[variant_id] = running
    members: list[MultiplicityMemberResultV2] = []
    for variant in sorted(variants, key=lambda item: item.variant_id):
        if variant.variant_id in stats:
            mean, _, low, high = stats[variant.variant_id]
            members.append(MultiplicityMemberResultV2(variant.variant_id, variant.variant_hash,
                Decimal(str(mean)), raw[variant.variant_id], adjusted[variant.variant_id], thresholds[variant.variant_id],
                independent, Decimal(str(low)), Decimal(str(high)),
                "TESTED" if independent >= 20 else "NOT_ESTIMABLE", None))
        else:
            members.append(MultiplicityMemberResultV2(variant.variant_id, variant.variant_hash, None, None,
                Decimal(1), thresholds[variant.variant_id], independent, None, None,
                "NOT_ESTIMABLE", failed.get(variant.variant_id) or "INSUFFICIENT_PAIRED_OUTCOMES"))
    status = "NOT_ESTIMABLE" if independent < 20 or failed else "TESTED"
    # This audit never emits an economic promotion. It reports only a corrected
    # family inference status for the declared outer evidence.
    decision = "NOT_ESTIMABLE" if status != "TESTED" else "NO_FAMILY_PASS" if all(
        member.adjusted_p_value > alpha for member in members if member.variant_id != baseline_variant_id
    ) else "CORRECTED_SIGNAL_REQUIRES_INDEPENDENT_ECONOMIC_REVIEW"
    return MultiplicityAuditV2(family_id, preregistration_ref, alpha, baseline_variant_id,
        tuple(members), block_length, bootstrap_replicates, seed,
        "SYNCHRONIZED_CONTIGUOUS_DECISION_TIME_BLOCK_BOOTSTRAP_THEN_HOLM_FWER_V1", independent,
        status, decision, tuple(FrozenMap(variant.__dict__) for variant in sorted(variants, key=lambda value: value.variant_id)))


@dataclass(frozen=True)
class WholePolicyAblationRowV2:
    feature_family: str
    baseline_policy_ref: str
    ablated_policy_ref: str
    scanner_ref: str
    candidate_generation_ref: str
    selection_ref: str
    sizing_ref: str
    execution_assumptions_ref: str
    costs_ref: str
    no_fill_partial_fill_ref: str
    latency_ref: str
    gate_ref: str
    result_ref: str | None
    result_status: str

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


@dataclass(frozen=True)
class FeatureFamilyAblationAuditV2:
    audit_id: str
    family_id: str
    feature_families: tuple[str, ...]
    rows: tuple[WholePolicyAblationRowV2, ...]
    baseline_policy_ref: str
    metrics: tuple[str, ...]
    chronology: str
    purge_embargo: str
    multiplicity_family_ref: str
    status: str

    def to_dict(self) -> dict[str, Any]:
        return {"version": ABLATION_VERSION, "audit_id": self.audit_id, "family_id": self.family_id,
            "feature_families": list(self.feature_families), "rows": [row.to_dict() for row in self.rows],
            "baseline_policy_ref": self.baseline_policy_ref, "metrics": list(self.metrics),
            "chronology": self.chronology, "purge_embargo": self.purge_embargo,
            "multiplicity_family_ref": self.multiplicity_family_ref, "status": self.status}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def declare_feature_family_ablation(*, audit_id: str, family_id: str, baseline_policy_ref: str,
        scanner_ref: str, candidate_generation_ref: str, selection_ref: str, sizing_ref: str,
        execution_assumptions_ref: str, costs_ref: str, no_fill_partial_fill_ref: str,
        latency_ref: str, gate_ref: str, multiplicity_family_ref: str,
        result_refs: Mapping[str, str] | None = None) -> FeatureFamilyAblationAuditV2:
    for value, name in ((baseline_policy_ref, "baseline_policy_ref"), (scanner_ref, "scanner_ref"),
        (candidate_generation_ref, "candidate_generation_ref"), (selection_ref, "selection_ref"),
        (sizing_ref, "sizing_ref"), (execution_assumptions_ref, "execution_assumptions_ref"),
        (costs_ref, "costs_ref"), (no_fill_partial_fill_ref, "no_fill_partial_fill_ref"),
        (latency_ref, "latency_ref"), (gate_ref, "gate_ref"), (multiplicity_family_ref, "multiplicity_family_ref")):
        sha256_ref(value, field=name)
    result_refs = result_refs or {}
    rows = tuple(WholePolicyAblationRowV2(name, baseline_policy_ref, sha256_json({
        "baseline_policy_ref": baseline_policy_ref, "ablated_feature_family": name,
        "ablation_version": ABLATION_VERSION}), scanner_ref, candidate_generation_ref, selection_ref,
        sizing_ref, execution_assumptions_ref, costs_ref, no_fill_partial_fill_ref, latency_ref, gate_ref,
        result_refs.get(name), "UNVERIFIED" if name in result_refs else "NOT_ESTIMABLE")
        for name in ABLATION_FAMILIES)
    return FeatureFamilyAblationAuditV2(audit_id, family_id, ABLATION_FAMILIES, rows, baseline_policy_ref,
        ("net_policy_value_per_opportunity", "selection_coverage", "no_fill_rate", "partial_fill_rate",
         "drawdown", "expected_shortfall", "calibration", "compute_latency", "missingness"),
        "180D_TRAIN_30D_VALIDATION_30D_OUTER_MONTHLY_ADVANCE_FINAL_UNTOUCHED_HOLDOUT",
        "PURGE_OVERLAPPING_LABELS_AND_EMBARGO_AT_LEAST_MAX_POLICY_HOLDING_HORIZON",
        multiplicity_family_ref, "NOT_ESTIMABLE")


def persist_research_artifact(repo: OpsRepository, artifact_type: str, body: Mapping[str, Any], *,
        available_at_ns: int, key: str = "research_artifact",
        experiment_ref: str | None = None) -> str:
    ref = sha256_json(body)
    metadata: dict[str, Any] = {key: body}
    if experiment_ref is not None:
        sha256_ref(experiment_ref, field="experiment_ref")
        metadata["experiment_ref"] = experiment_ref
    repo.register_artifact(ArtifactIndexEntryV2(ref, artifact_type, ref, available_at_ns,
        available_at_ns, metadata))
    return ref
