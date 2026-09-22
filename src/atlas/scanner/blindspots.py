"""Minimal frozen scanner blind-spot estimators with decision-time block bootstrap."""

from __future__ import annotations

import random
from collections.abc import Sequence

from .models import (
    BlindSpotMetrics,
    BlindSpotObservation,
    BlindSpotStatus,
    DeadlineStatus,
    EligibilityStatus,
    RankBand,
    ScannerCalendarRow,
    ScannerMaturation,
    WarmupState,
)

DEFAULT_BOOTSTRAP_REPLICATES = 200
DEFAULT_BLOCK_SLOTS = 6


def _quantile(values: Sequence[float], probability: float) -> float:
    if not values:
        raise ValueError("quantile requires support")
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round(probability * (len(ordered) - 1))))
    return ordered[index]


def _block_bootstrap(values_by_slot: dict[int, float], *, replicates: int, block_slots: int,
                     seed: int) -> tuple[float, ...]:
    slots = sorted(values_by_slot)
    if not slots:
        return ()
    rng = random.Random(seed)
    if block_slots < 1:
        raise ValueError("block_slots must be positive")
    draws: list[float] = []
    for _ in range(replicates):
        sampled: list[int] = []
        while len(sampled) < len(slots):
            start = rng.randrange(len(slots))
            sampled.extend(slots[start:start + block_slots])
        draws.append(sum(values_by_slot[slot] for slot in sampled[:len(slots)]) / len(slots))
    return tuple(draws)


def _ratio_upper(numerator_by_slot: dict[int, float], denominator_by_slot: dict[int, float], *,
                 replicates: int, block_slots: int, seed: int) -> float | None:
    slots = sorted(denominator_by_slot)
    if not slots:
        return None
    rng = random.Random(seed)
    draws: list[float] = []
    for _ in range(replicates):
        sampled: list[int] = []
        while len(sampled) < len(slots):
            start = rng.randrange(len(slots))
            sampled.extend(slots[start:start + block_slots])
        selected = sampled[:len(slots)]
        denominator = sum(denominator_by_slot[slot] for slot in selected)
        if denominator <= 0:
            continue
        draws.append(sum(numerator_by_slot.get(slot, 0.0) for slot in selected) / denominator)
    return _quantile(draws, 0.95) if draws else None


def _mean_interval(values_by_slot: dict[int, float], *, replicates: int, block_slots: int,
                   seed: int) -> tuple[float, float] | None:
    draws = _block_bootstrap(values_by_slot, replicates=replicates, block_slots=block_slots, seed=seed)
    if not draws:
        return None
    return _quantile(draws, 0.025), _quantile(draws, 0.975)


def blindspot_metrics(observations: Sequence[BlindSpotObservation], *, tolerance: float = 0.20,
                      replicates: int = DEFAULT_BOOTSTRAP_REPLICATES,
                      block_slots: int = DEFAULT_BLOCK_SLOTS, seed: int = 17) -> BlindSpotMetrics:
    """Estimate coverage, missed value, warmup/deadline loss and paired lift.

    Inverse-probability weighting is applied only to exploration observations
    whose inclusion probability was recorded by the deterministic design.
    """
    if tolerance < 0:
        raise ValueError("tolerance must be nonnegative")
    materialized = tuple(observations)
    support_slots = len({item.slot_at_ns for item in materialized})
    probability_support = sum(1 for item in materialized
                              if item.exploration_selected and 0 < item.inclusion_probability <= 1)
    reasons: list[str] = []

    def value_of(item: BlindSpotObservation) -> float:
        if item.counterfactual_value is None:
            raise ValueError("counterfactual value required")
        return float(item.counterfactual_value)

    known = [item for item in materialized if item.counterfactual_value is not None]
    inside_by_slot: dict[int, float] = {}
    outside_by_slot: dict[int, float] = {}
    total_by_slot: dict[int, float] = {}
    for item in known:
        value = value_of(item)
        if item.top_k_selected:
            inside_by_slot[item.slot_at_ns] = inside_by_slot.get(item.slot_at_ns, 0.0) + max(0.0, value)
        elif 0 < item.inclusion_probability <= 1:
            weighted = max(0.0, value) / item.inclusion_probability
            outside_by_slot[item.slot_at_ns] = outside_by_slot.get(item.slot_at_ns, 0.0) + weighted
        total_by_slot[item.slot_at_ns] = total_by_slot.get(item.slot_at_ns, 0.0) + value

    coverage: float | None = None
    coverage_upper: float | None = None
    if inside_by_slot:
        denominator = {slot: inside_by_slot.get(slot, 0.0) + outside_by_slot.get(slot, 0.0)
                       for slot in sorted(set(inside_by_slot) | set(outside_by_slot))}
        if sum(denominator.values()) > 0:
            coverage = sum(inside_by_slot.values()) / sum(denominator.values())
            coverage_upper = _ratio_upper(inside_by_slot, denominator, replicates=replicates,
                                          block_slots=block_slots, seed=seed + 1)

    missed_share: float | None = None
    missed_upper: float | None = None
    if inside_by_slot or outside_by_slot:
        denominator = {slot: inside_by_slot.get(slot, 0.0) + outside_by_slot.get(slot, 0.0)
                       for slot in sorted(set(inside_by_slot) | set(outside_by_slot))}
        if sum(denominator.values()) > 0:
            missed_share = sum(outside_by_slot.values()) / sum(denominator.values())
            missed_upper = _ratio_upper(outside_by_slot, denominator, replicates=replicates,
                                        block_slots=block_slots, seed=seed + 2)

    warmup_required = [item for item in materialized if item.warmup_required]
    deadline_applicable = [item for item in materialized if item.deadline_applicable]
    warmup_rate = (None if not warmup_required
                   else sum(1 for item in warmup_required if not item.warmup_available) / len(warmup_required))
    deadline_rate = (None if not deadline_applicable
                     else sum(1 for item in deadline_applicable if not item.deadline_met)
                     / len(deadline_applicable))

    paired_by_slot: dict[int, float] = {}
    for slot_at_ns in sorted({item.slot_at_ns for item in known}):
        top_values = [value_of(item) for item in known
                      if item.slot_at_ns == slot_at_ns and item.top_k_selected]
        exploration_values = [value_of(item) for item in known
                              if item.slot_at_ns == slot_at_ns and item.exploration_selected]
        if top_values and exploration_values:
            paired_by_slot[slot_at_ns] = sum(exploration_values) / len(exploration_values) - sum(top_values) / len(top_values)
    selection_lift = None if not paired_by_slot else sum(paired_by_slot.values()) / len(paired_by_slot)
    lift_interval = _mean_interval(paired_by_slot, replicates=replicates, block_slots=block_slots, seed=seed + 3)

    adequate = (support_slots >= 2 and probability_support > 0 and missed_upper is not None
                and sum(inside_by_slot.values()) + sum(outside_by_slot.values()) > 0)
    if not adequate:
        status = BlindSpotStatus.INCONCLUSIVE
        if support_slots < 2:
            reasons.append("INSUFFICIENT_SLOT_SUPPORT")
        if probability_support == 0:
            reasons.append("INSUFFICIENT_KNOWN_INCLUSION_PROBABILITY")
        if missed_upper is None:
            reasons.append("INSUFFICIENT_COUNTERFACTUAL_SUPPORT")
    elif missed_upper is not None and missed_upper > tolerance:
        status = BlindSpotStatus.ATTENTION
        reasons.append("MISSED_VALUE_UPPER_BOUND_EXCEEDS_TOLERANCE")
    else:
        status = BlindSpotStatus.PASS
    return BlindSpotMetrics(status, coverage, coverage_upper, missed_share, missed_upper, warmup_rate,
                            deadline_rate, selection_lift, lift_interval, support_slots,
                            probability_support, tuple(reasons))


def observations_from_matured(rows: Sequence[ScannerCalendarRow],
                              maturations: Sequence[ScannerMaturation],
                              ) -> tuple[BlindSpotObservation, ...]:
    """Build blind-spot inputs only from appended matured outcome evidence."""
    matured = {(item.scan_slot_at_ns, item.instrument): item for item in maturations}
    observations: list[BlindSpotObservation] = []
    for row in sorted(rows, key=lambda item: (item.scan_slot_at_ns, item.instrument)):
        if row.eligibility_status is not EligibilityStatus.ELIGIBLE:
            continue
        outcome = matured.get((row.scan_slot_at_ns, row.instrument))
        if outcome is None:
            continue
        required = row.deep_selected or row.exploration_selected
        observations.append(BlindSpotObservation(
            slot_at_ns=row.scan_slot_at_ns,
            instrument=row.instrument,
            rank_band=row.rank_band or RankBand.UNRANKED,
            top_k_selected=row.top_k_selected,
            exploration_selected=row.exploration_selected,
            inclusion_probability=row.exploration_probability or 0.0,
            counterfactual_value=outcome.counterfactual_value,
            warmup_available=row.warmup_state is WarmupState.WARM_AVAILABLE,
            deadline_met=row.model_deadline_status is DeadlineStatus.MET,
            warmup_required=required,
            deadline_applicable=required,
        ))
    return tuple(observations)
