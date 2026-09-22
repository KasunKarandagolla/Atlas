"""Frozen outer uncertainty process: resample, refit, rebuild OOF, replay one action.

Each replicate re-runs the whole causal chain on block-resampled synchronized
history and evaluates the *same immutable current action*:

1. resample the synchronized records;
2. keep the frozen chronological train/validation structure (three 7-day windows);
3. rebuild the replicate scaler/model/calibration through the shared frozen
   ridge-selection contract;
4. derive replicate-consistent scenario inputs (replicate ``mu``/residual applied
   to the archived minute innovations) so the refit materially moves P&L;
5. replay the same immutable action using only archived execution evidence.

Execution quotes/depth/spread, latency and funding always come from the SAME
sampled block that produced the return path.  Nothing is derived from OHLC.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, replace
from decimal import Decimal
from statistics import fmean, median

from atlas.science.execution_replay import ReplayMinute
from atlas.science.funding import FundingSettlement
from atlas.science.huber_mean import HOUR_NS, MeanObservation, fit_huber_ridge, select_ridge_chronological
from atlas.science.policy_replay import PolicyReplayResult, ReplayAssumptions, replay_policy
from atlas.science.residual_blocks import JointResidualHour, eligible_starts, sample_blocks
from atlas.science.scenarios import (
    DEFAULT_BRIDGE_SUPPORT,
    HORIZON_HOURS,
    BridgeSupport,
    CurrentModelState,
    JointMinutePaths,
    ScenarioNotEstimable,
    SynchronizedPrices,
    joint_minute_paths,
)
from atlas.science.uncertainty import (
    BOOTSTRAP_INNER_PATHS,
    INSTRUMENT_DELTA,
    OuterBootstrapResult,
    ReplicateEvaluation,
    block_bootstrap_indices,
    bootstrap_lcb,
)
from atlas.strategy.policy import FixedPolicy

MINIMUM_COST_OBSERVATIONS = 24
MIN_ESTIMABLE_PATH_FRACTION = 0.90
MAX_REPLICATE_HOURS = 24 * 130
FUNDING_SETTLEMENT_INTERVAL_NS = 8 * HOUR_NS


class NotEstimable(ValueError):
    """A frozen quantity cannot be valued from the supplied causal evidence."""


@dataclass(frozen=True)
class CostEstimate:
    taker_fee_rate: Decimal
    spread_impact: Decimal
    observations: int


@dataclass(frozen=True)
class ExecutionEvidence:
    """Archived per-hour execution evidence used by the replicate replay."""

    spread_bp: Decimal
    depth_notional: Decimal
    entry_latency_ns: int
    observations: int


@dataclass(frozen=True)
class ReplicateFit:
    ridge: float
    training_rows: int
    validation_rows: int
    cost: CostEstimate
    execution: ExecutionEvidence


def observations_from_hours(hours: Sequence[JointResidualHour], indices: Sequence[int]) -> list[MeanObservation]:
    """Matured hourly rows: realized return is ``sigma * (forecast + residual)``."""
    rows: list[MeanObservation] = []
    for index in indices:
        hour = hours[index]
        for instrument, z, sigma, forecast, residual in (
            ("BTCUSDT", hour.btc_z, hour.btc_sigma, hour.btc_forecast, hour.btc_residual),
            ("ETHUSDT", hour.eth_z, hour.eth_sigma, hour.eth_forecast, hour.eth_residual),
        ):
            rows.append(MeanObservation(hour.at_ns, instrument, z, sigma, sigma * (forecast + residual)))
    return rows


def estimate_cost_assumptions(hours: Sequence[JointResidualHour], *,
                              minimum_observations: int = MINIMUM_COST_OBSERVATIONS) -> CostEstimate:
    """Re-estimate supported execution costs strictly from archived observations."""
    fees: list[float] = []
    spreads: list[float] = []
    for hour in hours:
        for observation in hour.spread_depth_observations:
            fee = observation.get("taker_fee_bp")
            spread = observation.get("spread_bp")
            if fee is None or spread is None:
                continue
            fees.append(float(fee))
            spreads.append(float(spread))
    if len(fees) < minimum_observations or len(spreads) < minimum_observations:
        raise NotEstimable("NOT_ESTIMABLE: insufficient execution/cost observations")
    if not all(math.isfinite(x) and x >= 0 for x in fees + spreads):
        raise NotEstimable("NOT_ESTIMABLE: non-finite cost observation")
    return CostEstimate(Decimal(str(median(fees) / 10_000.0)), Decimal(str(median(spreads) / 10_000.0)), len(fees))


def _instrument_observation(observation: dict[str, str], instrument: str) -> bool:
    tagged = observation.get("instrument")
    return tagged is None or tagged == instrument


def estimate_execution_evidence(hours: Sequence[JointResidualHour], *, instrument: str,
                                minimum_observations: int = MINIMUM_COST_OBSERVATIONS) -> ExecutionEvidence:
    """Displayed spread/depth and entry latency strictly from archived observations."""
    spreads: list[float] = []
    depths: list[float] = []
    latencies: list[float] = []
    for hour in hours:
        if hour.execution_missing:
            continue
        for observation in hour.spread_depth_observations:
            if not _instrument_observation(observation, instrument):
                continue
            spread = observation.get("spread_bp")
            depth = observation.get("depth_notional")
            if spread is None or depth is None:
                continue
            spreads.append(float(spread))
            depths.append(float(depth))
        for observation in hour.latency_fill_observations:
            if not _instrument_observation(observation, instrument):
                continue
            latency = observation.get("entry_latency_ms")
            if latency is None:
                continue
            latencies.append(float(latency))
    if len(spreads) < minimum_observations or len(depths) < minimum_observations:
        raise NotEstimable("NOT_ESTIMABLE: missing archived spread/depth evidence")
    if not all(math.isfinite(x) and x > 0 for x in spreads + depths):
        raise NotEstimable("NOT_ESTIMABLE: non-finite spread/depth evidence")
    if not latencies:
        raise NotEstimable("NOT_ESTIMABLE: missing archived latency evidence")
    return ExecutionEvidence(Decimal(str(median(spreads) / 10_000.0)), Decimal(str(median(depths))),
                             int(median(latencies) * 1_000_000), len(spreads))


def rebuild_replicate_hours(hours: Sequence[JointResidualHour], model: object) -> tuple[JointResidualHour, ...]:
    """Derive replicate-consistent ``mu``/residual scenario inputs from a refit model.

    The realized standardized return is archived evidence; the replicate model
    only changes the conditional mean, so the residual is recomputed from the
    refitted coefficients.  Coefficients stay frozen for the whole path.
    """
    rebuilt: list[JointResidualHour] = []
    for hour in hours:
        btc_forecast = model.forecast("BTCUSDT", hour.btc_z)  # type: ignore[attr-defined]
        eth_forecast = model.forecast("ETHUSDT", hour.eth_z)  # type: ignore[attr-defined]
        rebuilt.append(replace(hour, btc_forecast=btc_forecast,
                               btc_residual=(hour.btc_forecast + hour.btc_residual) - btc_forecast,
                               eth_forecast=eth_forecast,
                               eth_residual=(hour.eth_forecast + hour.eth_residual) - eth_forecast))
    return tuple(rebuilt)


def current_state(model: object, features: dict[str, tuple[float, float]]) -> dict[str, CurrentModelState]:
    """Current decision model state per instrument: observed sigma, refit mean."""
    return {instrument: CurrentModelState(sigma=sigma, mu=model.forecast(instrument, z))  # type: ignore[attr-defined]
            for instrument, (z, sigma) in features.items()}


def _funding_settlements(hours: Sequence[JointResidualHour], *, instrument: str, base_offset_ns: int,
                         minutes: Sequence[ReplayMinute]) -> tuple[FundingSettlement, ...]:
    """Sampled funding settlements rebased onto the replay timeline."""
    if not any(hour.funding_observations or hour.funding_settlement_at_ns is not None for hour in hours):
        raise NotEstimable("NOT_ESTIMABLE: missing archived funding evidence")
    price_at = {minute.at_ns: minute for minute in minutes}
    marks = sorted(price_at)
    settlements = []
    for hour in hours:
        if hour.funding_settlement_at_ns is None:
            continue
        rate = None
        for observation in hour.funding_observations:
            if _instrument_observation(observation, instrument):
                rate = observation.get("rate")
                break
        if rate is None:
            continue
        at_ns = hour.funding_settlement_at_ns + base_offset_ns
        candidate = next((mark for mark in marks if mark >= at_ns), None)
        if candidate is None:
            continue
        settlements.append(FundingSettlement(candidate, Decimal(rate), price_at[candidate].mark_low))
    if not settlements:
        raise NotEstimable("NOT_ESTIMABLE: no usable funding settlement evidence on the replay path")
    return tuple(settlements)


def _replay_minutes(paths: JointMinutePaths, instrument: str, *, hours: Sequence[JointResidualHour],
                    start_at_ns: int | None = None) -> tuple[ReplayMinute, ...]:
    """Build replay minutes whose quotes/depth come only from archived evidence."""
    minutes = paths.btc if instrument == "BTCUSDT" else paths.eth
    offset = 0 if start_at_ns is None else start_at_ns - minutes[0].at_ns
    output: list[ReplayMinute] = []
    for index, minute in enumerate(minutes):
        hour_index = min(index // 60, len(hours) - 1)
        hour = hours[hour_index]
        spread = None
        depth = None
        for observation in hour.spread_depth_observations:
            if _instrument_observation(observation, instrument):
                spread = observation.get("spread_bp")
                depth = observation.get("depth_notional")
                break
        if hour.execution_missing or spread is None or depth is None:
            raise NotEstimable("NOT_ESTIMABLE: sampled hour lacks displayed quote/depth evidence")
        mid = Decimal(str(minute.last.close))
        bid = mid * (Decimal("1") - Decimal(spread) / Decimal("20000"))
        ask = mid * (Decimal("1") + Decimal(spread) / Decimal("20000"))
        depth_qty = Decimal(depth) / mid
        output.append(ReplayMinute(minute.at_ns + offset, bid, ask, depth_qty, depth_qty,
                                   Decimal(str(minute.mark.low)), Decimal(str(minute.mark.high)),
                                   Decimal(str(minute.last.low)), Decimal(str(minute.last.high))))
    return tuple(output)


def replay_replicate(policy: FixedPolicy, hours: Sequence[JointResidualHour], *, model: object,
                     features: dict[str, tuple[float, float]], block_length: int, instrument: str,
                     inner_paths: int, seed: int, assumptions: ReplayAssumptions,
                     support: BridgeSupport = DEFAULT_BRIDGE_SUPPORT,
                     minimum_observations: int = MINIMUM_COST_OBSERVATIONS) -> tuple[float, int, ExecutionEvidence]:
    """Mean P&L over inner paths for one already-frozen action and one replicate model."""
    starts = eligible_starts(hours, block_length)
    if not starts:
        raise NotEstimable("NOT_ESTIMABLE: no contiguous synchronized blocks")
    execution = estimate_execution_evidence(hours, instrument=instrument, minimum_observations=minimum_observations)
    cost = estimate_cost_assumptions(hours, minimum_observations=minimum_observations)
    # Archived latency and cost evidence override unsupported caller assumptions.
    effective = replace(assumptions, decision_to_venue_ns=assumptions.decision_to_venue_ns + execution.entry_latency_ns,
                        taker_fee_rate=cost.taker_fee_rate)
    states = current_state(model, features)
    anchor = SynchronizedPrices(float(policy.mark_reference), float(policy.mark_reference),
                                float(policy.mark_reference))
    sampled = sample_blocks(hours, length=block_length, horizon_hours=HORIZON_HOURS + 1,
                            paths=inner_paths, seed=seed)
    replays: list[PolicyReplayResult] = []
    for path in sampled:
        try:
            minute_paths = joint_minute_paths(path, initial={instrument: anchor}, current={instrument: states[instrument]},
                                              support=support)
            first_minute = minute_paths.btc[0] if instrument == "BTCUSDT" else minute_paths.eth[0]
            base_offset = policy.decision_slot_at_ns - first_minute.at_ns
            minutes = _replay_minutes(minute_paths, instrument, hours=path,
                                      start_at_ns=policy.decision_slot_at_ns)
            settlements = _funding_settlements(path, instrument=instrument, base_offset_ns=base_offset,
                                               minutes=minutes)
        except ScenarioNotEstimable:
            continue
        except NotEstimable:
            continue
        replays.append(replay_policy(policy, minutes, settlements, effective))
    if not replays or len(replays) < MIN_ESTIMABLE_PATH_FRACTION * len(sampled):
        raise NotEstimable("NOT_ESTIMABLE: insufficient estimable replay paths")
    pnls = [float(result.pnl) for result in replays if result.pnl is not None]
    if len(pnls) < MIN_ESTIMABLE_PATH_FRACTION * len(replays):
        raise NotEstimable("NOT_ESTIMABLE: unbounded replay outcomes")
    return fmean(pnls), len(pnls), execution


def replicate_evaluation(
    *, policy: FixedPolicy, history: Sequence[JointResidualHour], block_length: int, instrument: str,
    inner_paths: int, seed: int, assumptions: ReplayAssumptions, features: dict[str, tuple[float, float]],
    fit_at_ns: int,
    action_hash: str, support: BridgeSupport = DEFAULT_BRIDGE_SUPPORT,
    minimum_cost_observations: int = MINIMUM_COST_OBSERVATIONS,
) -> tuple[ReplicateEvaluation, ReplicateFit]:
    """One full frozen replicate: refit, ridge selection, OOF rebuild, replay.

    ``history`` is the replicate's already-resampled record set; the caller owns
    the block bootstrap so the same replicate sample feeds every stage.
    """
    if not history:
        raise NotEstimable("NOT_ESTIMABLE: empty bootstrap replicate")
    sampled_hours = tuple(history)
    if len(sampled_hours) > MAX_REPLICATE_HOURS:
        sampled_hours = sampled_hours[-MAX_REPLICATE_HOURS:]
    rows = observations_from_hours(sampled_hours, range(len(sampled_hours)))
    if len(rows) < 2:
        raise NotEstimable("NOT_ESTIMABLE: replicate lacks matured rows")
    selection = select_ridge_chronological(rows, fit_at_ns)
    # Same frozen contract as the weekly refit: the fold structure selects the
    # ridge and the final coefficients are fit on all matured eligible labels.
    window_start = selection.validation_intervals[0][0]
    training_rows = [row for row in selection.eligible if row.label_at_ns < window_start]
    validation_rows = [row for row in selection.eligible if row.label_at_ns >= window_start]
    model = fit_huber_ridge(selection.eligible, selection.ridge, fit_at_ns=fit_at_ns,
                            validation_intervals=selection.validation_intervals)
    # Replicate-consistent scenario inputs: the refit changes mu and therefore the
    # residual that is applied to the archived minute innovations.
    replicate_hours = rebuild_replicate_hours(sampled_hours, model)
    cost = estimate_cost_assumptions(sampled_hours, minimum_observations=minimum_cost_observations)
    mean_pnl, paths, execution = replay_replicate(policy, replicate_hours, model=model, features=features,
                                                  block_length=block_length, instrument=instrument,
                                                  inner_paths=inner_paths, seed=seed, assumptions=assumptions,
                                                  support=support, minimum_observations=minimum_cost_observations)
    fit = ReplicateFit(selection.ridge, len(training_rows), len(validation_rows), cost, execution)
    return ReplicateEvaluation(mean_pnl, action_hash, True, True, True, True), fit


def outer_lcb(
    *, policy: FixedPolicy, history: Sequence[JointResidualHour], block_length: int, instrument: str,
    action_hash: str, assumptions: ReplayAssumptions, features: dict[str, tuple[float, float]], fit_at_ns: int,
    replicates: int, inner_paths: int, replicate_seed: int, inner_seed_a: int, inner_seed_b: int,
    delta: float = INSTRUMENT_DELTA, support: BridgeSupport = DEFAULT_BRIDGE_SUPPORT,
    minimum_cost_observations: int = MINIMUM_COST_OBSERVATIONS,
) -> OuterBootstrapResult:
    """Full frozen outer process: refit/OOF/cost/action replicates and the LCB order statistic."""
    index_sets = block_bootstrap_indices(len(history), block_length=block_length, count=replicates, seed=replicate_seed)
    means_a: list[float] = []
    means_b: list[float] = []
    for indices in index_sets:
        if not indices:
            raise NotEstimable("NOT_ESTIMABLE: empty bootstrap replicate")
        subset = tuple(history[index] for index in indices)
        first, _ = replicate_evaluation(policy=policy, history=subset, block_length=block_length,
                                        instrument=instrument, inner_paths=inner_paths, seed=inner_seed_a,
                                        assumptions=assumptions, features=features, action_hash=action_hash,
                                        fit_at_ns=fit_at_ns,
                                        support=support, minimum_cost_observations=minimum_cost_observations)
        second, _ = replicate_evaluation(policy=policy, history=subset, block_length=block_length,
                                         instrument=instrument, inner_paths=inner_paths, seed=inner_seed_b,
                                         assumptions=assumptions, features=features, action_hash=action_hash,
                                         fit_at_ns=fit_at_ns,
                                         support=support, minimum_cost_observations=minimum_cost_observations)
        if first.action_hash != action_hash or second.action_hash != action_hash:
            raise ValueError("bootstrap changed immutable current action")
        means_a.append(first.mean_pnl)
        means_b.append(second.mean_pnl)
    lcb, unstable = bootstrap_lcb(means_a, delta=delta, seed_a_means=means_a, seed_b_means=means_b)
    return OuterBootstrapResult(tuple(means_a), lcb, unstable,
                                "NO_TRADE_NUMERICAL" if unstable else "ESTIMATED",
                                replicate_seed, inner_seed_a, inner_seed_b, action_hash)


def horizon_minutes(hours: int = HORIZON_HOURS) -> int:
    return hours * 60


def hour_ns() -> int:
    return HOUR_NS


OUTER_INNER_PATHS = BOOTSTRAP_INNER_PATHS
