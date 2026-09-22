"""Frozen outer uncertainty process: resample, refit, rebuild OOF, replay one action.

Each replicate re-runs the whole causal chain on block-resampled synchronized
history and evaluates the *same immutable current action*.  In-sample residuals
are never reused as OOF residuals, and execution/cost assumptions are
re-estimated from supplied evidence rather than invented.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from statistics import fmean, median

from atlas.science.execution_replay import ReplayMinute
from atlas.science.huber_mean import (
    HOUR_NS,
    RIDGE_CANDIDATES,
    MeanObservation,
    fit_huber_ridge,
    mean_huber_loss,
)
from atlas.science.policy_replay import PolicyReplayResult, ReplayAssumptions, replay_policy
from atlas.science.residual_blocks import JointResidualHour, eligible_starts, sample_blocks
from atlas.science.scenarios import (
    DEFAULT_BRIDGE_SUPPORT,
    HORIZON_HOURS,
    BridgeSupport,
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

DEFAULT_FOLDS = 3
MINIMUM_COST_OBSERVATIONS = 24
MIN_ESTIMABLE_PATH_FRACTION = 0.90
TRAIN_FRACTION = 0.8


class NotEstimable(ValueError):
    """A frozen quantity cannot be valued from the supplied causal evidence."""


@dataclass(frozen=True)
class CostEstimate:
    taker_fee_rate: Decimal
    spread_impact: Decimal
    observations: int


@dataclass(frozen=True)
class ReplicateFit:
    ridge: float
    training_rows: int
    validation_rows: int
    cost: CostEstimate


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


def select_ridge_chronological(train_rows: Sequence[MeanObservation], *, folds: int = DEFAULT_FOLDS) -> float:
    if folds < 1 or len(train_rows) < 2 * folds:
        raise NotEstimable("NOT_ESTIMABLE: insufficient chronological training rows")
    ordered = sorted(train_rows, key=lambda row: (row.origin_at_ns, row.instrument))
    if len({row.origin_at_ns for row in ordered}) < 2:
        raise NotEstimable("NOT_ESTIMABLE: degenerate training support")
    size = len(ordered)
    losses: dict[float, float] = {}
    for ridge in RIDGE_CANDIDATES:
        fold_losses: list[float] = []
        for fold in range(folds):
            # Positional chronological split: every fold trains strictly on the
            # rows that precede its validation window, even after block resampling.
            train = ordered[: size * (fold + 1) // (folds + 1)]
            valid = ordered[size * (fold + 1) // (folds + 1) : size * (fold + 2) // (folds + 1)]
            if not train or not valid:
                raise NotEstimable("NOT_ESTIMABLE: chronological fold support unavailable")
            fold_losses.append(mean_huber_loss(fit_huber_ridge(train, ridge, fit_at_ns=train[-1].origin_at_ns), valid))
        losses[ridge] = fmean(fold_losses)
    best = min(losses.values())
    # Frozen tie resolution: within 1e-8 prefer the larger lambda.
    return max(ridge for ridge, loss in losses.items() if loss <= best + 1e-8)


def estimate_cost_assumptions(hours: Sequence[JointResidualHour], *, minimum_observations: int = MINIMUM_COST_OBSERVATIONS) -> CostEstimate:
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


def _replay_minutes(paths: JointMinutePaths, instrument: str, spread: Decimal,
                    start_at_ns: int | None = None) -> tuple[ReplayMinute, ...]:
    minutes = paths.btc if instrument == "BTCUSDT" else paths.eth
    half_spread = spread / Decimal("2")
    offset = 0 if start_at_ns is None else start_at_ns - minutes[0].at_ns
    output: list[ReplayMinute] = []
    for minute in minutes:
        mid = Decimal(str(minute.last.close))
        bid = mid * (Decimal("1") - half_spread)
        ask = mid * (Decimal("1") + half_spread)
        depth = Decimal(str(max(minute.last.low, float(mid) * 0.5)))
        output.append(ReplayMinute(minute.at_ns + offset, bid, ask, depth, depth,
                                   Decimal(str(minute.mark.low)), Decimal(str(minute.mark.high)),
                                   Decimal(str(minute.last.low)), Decimal(str(minute.last.high))))
    return tuple(output)


def replay_replicate(policy: FixedPolicy, hours: Sequence[JointResidualHour], *, block_length: int,
                     instrument: str, inner_paths: int, seed: int, cost: CostEstimate,
                     assumptions: ReplayAssumptions, archive_sigma: float,
                     support: BridgeSupport = DEFAULT_BRIDGE_SUPPORT) -> tuple[float, int]:
    """Mean P&L over inner paths for one already-frozen action."""
    starts = eligible_starts(hours, block_length)
    if not starts:
        raise NotEstimable("NOT_ESTIMABLE: no contiguous synchronized blocks")
    anchor = SynchronizedPrices(float(policy.mark_reference), float(policy.mark_reference), float(policy.mark_reference))
    # One extra hour so the frozen T+24h time exit and its escalation have evidence.
    sampled = sample_blocks(hours, length=block_length, horizon_hours=HORIZON_HOURS + 1,
                            paths=inner_paths, seed=seed)
    replays: list[PolicyReplayResult] = []
    for path in sampled:
        try:
            minute_paths = joint_minute_paths(path, initial={instrument: anchor}, archive_sigma={instrument: archive_sigma},
                                              support=support)
            minutes = _replay_minutes(minute_paths, instrument, cost.spread_impact,
                                      start_at_ns=policy.decision_slot_at_ns)
        except ScenarioNotEstimable:
            continue
        replays.append(replay_policy(policy, minutes, (), assumptions))
    if not replays or len(replays) < MIN_ESTIMABLE_PATH_FRACTION * len(sampled):
        raise NotEstimable("NOT_ESTIMABLE: insufficient estimable replay paths")
    pnls = [float(result.pnl) for result in replays if result.pnl is not None]
    if len(pnls) < MIN_ESTIMABLE_PATH_FRACTION * len(replays):
        raise NotEstimable("NOT_ESTIMABLE: unbounded replay outcomes")
    return fmean(pnls), len(pnls)


def replicate_evaluation(
    *, policy: FixedPolicy, history: Sequence[JointResidualHour], block_length: int, instrument: str,
    inner_paths: int, seed: int, assumptions: ReplayAssumptions, archive_sigma: float,
    action_hash: str, minimum_cost_observations: int = MINIMUM_COST_OBSERVATIONS,
) -> tuple[ReplicateEvaluation, ReplicateFit]:
    """One full frozen replicate: refit, ridge selection, OOF rebuild, replay.

    ``history`` is the replicate's already-resampled record set; the caller owns
    the block bootstrap so the same replicate sample feeds every stage.
    """
    if not history:
        raise NotEstimable("NOT_ESTIMABLE: empty bootstrap replicate")
    sampled_hours = tuple(history)
    rows = observations_from_hours(sampled_hours, range(len(sampled_hours)))
    split = int(len(rows) * TRAIN_FRACTION)
    train_rows = rows[:split]
    validation_rows = rows[split:]
    if not train_rows or not validation_rows:
        raise NotEstimable("NOT_ESTIMABLE: replicate lacks chronological validation support")
    ridge = select_ridge_chronological(train_rows)
    train_model = fit_huber_ridge(train_rows, ridge, fit_at_ns=train_rows[-1].origin_at_ns)
    # OOF calibration support is rebuilt from out-of-sample validation residuals only.
    oof_residuals = [row.y - train_model.forecast(row.instrument, row.z) for row in validation_rows]
    if not all(math.isfinite(x) for x in oof_residuals):
        raise NotEstimable("NOT_ESTIMABLE: non-finite OOF residual")
    cost = estimate_cost_assumptions(sampled_hours, minimum_observations=minimum_cost_observations)
    mean_pnl, paths = replay_replicate(policy, sampled_hours, block_length=block_length, instrument=instrument,
                                       inner_paths=inner_paths, seed=seed, cost=cost, assumptions=assumptions,
                                       archive_sigma=archive_sigma)
    fit = ReplicateFit(ridge, len(train_rows), len(validation_rows), cost)
    return ReplicateEvaluation(mean_pnl, action_hash, True, True, True, True), fit


def outer_lcb(
    *, policy: FixedPolicy, history: Sequence[JointResidualHour], block_length: int, instrument: str,
    action_hash: str, assumptions: ReplayAssumptions, archive_sigma: float,
    replicates: int, inner_paths: int, replicate_seed: int, inner_seed_a: int, inner_seed_b: int,
    delta: float = INSTRUMENT_DELTA, minimum_cost_observations: int = MINIMUM_COST_OBSERVATIONS,
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
                                        assumptions=assumptions, archive_sigma=archive_sigma, action_hash=action_hash,
                                        minimum_cost_observations=minimum_cost_observations)
        second, _ = replicate_evaluation(policy=policy, history=subset, block_length=block_length,
                                         instrument=instrument, inner_paths=inner_paths, seed=inner_seed_b,
                                         assumptions=assumptions, archive_sigma=archive_sigma, action_hash=action_hash,
                                         minimum_cost_observations=minimum_cost_observations)
        if first.action_hash != action_hash or second.action_hash != action_hash:
            raise ValueError("bootstrap changed immutable current action")
        means_a.append(first.mean_pnl)
        means_b.append(second.mean_pnl)
    lcb, unstable = bootstrap_lcb(means_a, delta=delta, seed_a_means=means_a, seed_b_means=means_b)
    return OuterBootstrapResult(tuple(means_a), lcb, unstable,
                                "NO_TRADE_NUMERICAL" if unstable else "ESTIMATED",
                                replicate_seed, inner_seed_a, inner_seed_b)


def horizon_minutes(hours: int = 24) -> int:
    return hours * 60


def hour_ns() -> int:
    return HOUR_NS


OUTER_INNER_PATHS = BOOTSTRAP_INNER_PATHS
