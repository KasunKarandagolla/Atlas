"""Frozen outer uncertainty process: resample, refit, rebuild OOF, replay one action.

Each replicate re-runs the whole causal chain on block-resampled synchronized
history and evaluates the *same immutable current action*:

1. resample the synchronized records;
2. refit the scaler/model with the LOCKED penalty (no per-replicate search);
3. rebuild genuine chronological OOF forecasts — every historical forecast uses
   only labels that matured before its origin and is never rewritten by a later
   fit;
4. replay the same immutable action with the simulated feature state evolving
   hourly through the frozen feature engine, causal funding forecasts and the
   same block's execution evidence.
"""

from __future__ import annotations

import math
from bisect import bisect_right
from collections.abc import Sequence
from dataclasses import dataclass, replace
from decimal import Decimal
from statistics import fmean, median

from atlas.science.execution_replay import ReplayMinute
from atlas.science.funding import FundingForecast, FundingSettlement, forecast_funding
from atlas.science.huber_mean import (
    HOUR_NS,
    HuberRidgeModel,
    MeanObservation,
    fit_huber_ridge,
    is_monday_midnight,
    select_ridge_chronological,
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
from atlas.strategy.features import HourlyClose
from atlas.strategy.policy import FixedPolicy

MINIMUM_COST_OBSERVATIONS = 24
MIN_ESTIMABLE_PATH_FRACTION = 0.90
MAX_REPLICATE_HOURS = 24 * 180


class NotEstimable(ValueError):
    """A frozen quantity cannot be valued from the supplied causal evidence."""


@dataclass(frozen=True)
class CostEstimate:
    taker_fee_rate: Decimal
    spread_impact: Decimal
    observations: int


@dataclass(frozen=True)
class ExecutionEvidence:
    """Archived execution evidence summary used by the replicate replay."""

    spread_bp: Decimal
    depth_notional: Decimal
    entry_latency_ns: int
    observations: int
    funding_anchor_kind: str = ""
    funding_downgrade: str | None = None


@dataclass(frozen=True)
class FundingAnchor:
    """Decision-time funding evidence: observable anchor plus known settlement."""

    next_settlement_at_ns: int | None = None
    latest_predicted_rate: Decimal | None = None
    latest_settled_rate: Decimal | None = None


@dataclass(frozen=True)
class ReplicateFit:
    ridge: float
    training_rows: int
    validation_rows: int
    cost: CostEstimate
    execution: ExecutionEvidence
    refit_instants: tuple[int, ...]


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


def path_spread_depth(hour: JointResidualHour, instrument: str) -> tuple[Decimal, Decimal] | None:
    """Displayed spread/depth for one sampled hour strictly from archived evidence."""
    if hour.execution_missing:
        return None
    for observation in hour.spread_depth_observations:
        if not _instrument_observation(observation, instrument):
            continue
        spread = observation.get("spread_bp")
        depth = observation.get("depth_notional")
        if spread is None or depth is None:
            continue
        if float(spread) <= 0 or float(depth) <= 0 or not math.isfinite(float(spread)):
            return None
        return Decimal(spread), Decimal(depth)
    return None


def path_latency_ns(hours: Sequence[JointResidualHour], instrument: str) -> int | None:
    """Entry latency observed on THIS sampled path (never a replicate-wide median)."""
    observed: list[float] = []
    for hour in hours:
        found = None
        for observation in hour.latency_fill_observations:
            if not _instrument_observation(observation, instrument):
                continue
            found = observation.get("entry_latency_ms")
            if found is not None:
                break
        if found is None:
            return None
        observed.append(float(found))
    if not observed or not all(math.isfinite(x) and x >= 0 for x in observed):
        return None
    return int(median(observed) * 1_000_000)


def estimate_execution_evidence(hours: Sequence[JointResidualHour], *, instrument: str,
                                minimum_observations: int = MINIMUM_COST_OBSERVATIONS,
                                funding_anchor_kind: str = "", funding_downgrade: str | None = None) -> ExecutionEvidence:
    """Aggregate archived spread/depth/latency for evidence reporting only."""
    spreads: list[float] = []
    depths: list[float] = []
    latencies: list[float] = []
    for hour in hours:
        if hour.execution_missing:
            continue
        evidence = path_spread_depth(hour, instrument)
        if evidence is not None:
            spreads.append(float(evidence[0]))
            depths.append(float(evidence[1]))
        for observation in hour.latency_fill_observations:
            if not _instrument_observation(observation, instrument):
                continue
            latency = observation.get("entry_latency_ms")
            if latency is not None:
                latencies.append(float(latency))
    if len(spreads) < minimum_observations or len(depths) < minimum_observations:
        raise NotEstimable("NOT_ESTIMABLE: missing archived spread/depth evidence")
    if not all(math.isfinite(x) and x > 0 for x in spreads + depths):
        raise NotEstimable("NOT_ESTIMABLE: non-finite spread/depth evidence")
    if not latencies:
        raise NotEstimable("NOT_ESTIMABLE: missing archived latency evidence")
    return ExecutionEvidence(Decimal(str(median(spreads) / 10_000.0)), Decimal(str(median(depths))),
                             int(median(latencies) * 1_000_000), len(spreads), funding_anchor_kind, funding_downgrade)


def replicate_refits(hours: Sequence[JointResidualHour], *, locked_ridge: float,
                     ) -> tuple[tuple[int, HuberRidgeModel], ...]:
    """Frozen Monday refits inside the replicate sample, using the LOCKED penalty."""
    rows = observations_from_hours(hours, range(len(hours)))
    refits: list[tuple[int, HuberRidgeModel]] = []
    for instant in sorted({hour.at_ns for hour in hours if is_monday_midnight(hour.at_ns)}):
        try:
            selection = select_ridge_chronological(rows, instant, locked_ridge=locked_ridge)
        except ValueError:
            continue
        refits.append((instant, fit_huber_ridge(selection.eligible, locked_ridge, fit_at_ns=instant,
                                                validation_intervals=selection.validation_intervals)))
    if not refits:
        raise NotEstimable("NOT_ESTIMABLE: replicate has no matured causal refit")
    return tuple(refits)


def replicate_oof_hours(hours: Sequence[JointResidualHour], *, locked_ridge: float,
                        ) -> tuple[tuple[JointResidualHour, ...], tuple[tuple[int, HuberRidgeModel], ...]]:
    """Genuine chronological OOF: every forecast uses only earlier matured labels.

    Weekly refits follow the frozen Monday cadence inside the replicate sample and
    reuse the frozen eligibility/fold contract with the LOCKED penalty.  A later
    fit never rewrites an earlier archived forecast.
    """
    sampled = tuple(hours)
    if not sampled:
        raise NotEstimable("NOT_ESTIMABLE: empty bootstrap replicate")
    refits = replicate_refits(sampled, locked_ridge=locked_ridge)
    refit_instants = tuple(instant for instant, _ in refits)
    rebuilt: list[JointResidualHour] = []
    for hour in sampled:
        index = bisect_right(refit_instants, hour.at_ns) - 1
        if index < 0:
            # No matured causal fit exists at this origin yet: the replicate OOF
            # archive simply does not contain that hour (never a back-filled forecast).
            continue
        instant, model = refits[index]
        btc_forecast = model.forecast("BTCUSDT", hour.btc_z)
        eth_forecast = model.forecast("ETHUSDT", hour.eth_z)
        rebuilt.append(replace(hour, btc_forecast=btc_forecast,
                               btc_residual=(hour.btc_forecast + hour.btc_residual) - btc_forecast,
                               eth_forecast=eth_forecast,
                               eth_residual=(hour.eth_forecast + hour.eth_residual) - eth_forecast))
    if not rebuilt:
        raise NotEstimable("NOT_ESTIMABLE: replicate has no hours with a matured causal fit")
    return tuple(rebuilt), refits


def _causal_window(causal_closes: dict[str, tuple[HourlyClose, ...]], instrument: str) -> tuple[HourlyClose, ...]:
    if instrument not in causal_closes:
        raise NotEstimable("NOT_ESTIMABLE: missing causal close window for the instrument")
    return causal_closes[instrument]


def _funding_settlements(hours: Sequence[JointResidualHour], *, instrument: str, base_offset_ns: int,
                         minutes: Sequence[ReplayMinute], anchor: FundingAnchor,
                         ) -> tuple[tuple[FundingSettlement, ...], FundingForecast]:
    """Current causal anchor plus sampled historical settlement CHANGES."""
    if anchor.latest_predicted_rate is None and anchor.latest_settled_rate is None:
        raise NotEstimable("NOT_ESTIMABLE: no observable funding anchor")
    rates_seen: list[Decimal] = []
    instants: list[int] = []
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
        rates_seen.append(Decimal(rate))
        instants.append(hour.funding_settlement_at_ns + base_offset_ns)
    if not instants:
        raise NotEstimable("NOT_ESTIMABLE: sampled block carries no funding settlement evidence")
    # Historical evidence supplies settlement-to-settlement changes only; the
    # absolute level is never replayed as if it were observable today.
    changes = tuple(rates_seen[index + 1] - rates_seen[index] for index in range(len(rates_seen) - 1))
    known_next = anchor.next_settlement_at_ns if anchor.next_settlement_at_ns is not None else instants[0]
    forecast = forecast_funding(next_settlement_at_ns=known_next,
                                latest_predicted_rate=anchor.latest_predicted_rate,
                                latest_settled_rate=anchor.latest_settled_rate,
                                historical_settlement_changes=changes,
                                horizon_settlements=len(instants))
    price_at = {minute.at_ns: minute for minute in minutes}
    available = sorted(price_at)
    settlements: list[FundingSettlement] = []
    for forecast_rate, settlement_at_ns in zip(forecast.rates, instants, strict=True):
        minute_at = next((candidate for candidate in available if candidate >= settlement_at_ns), None)
        if minute_at is None:
            continue
        settlements.append(FundingSettlement(minute_at, forecast_rate, price_at[minute_at].mark_low))
    if not settlements:
        raise NotEstimable("NOT_ESTIMABLE: no usable funding settlement on the replay path")
    return tuple(settlements), forecast


def replay_minutes(paths: JointMinutePaths, instrument: str, *, hours: Sequence[JointResidualHour],
                   start_at_ns: int | None = None) -> tuple[ReplayMinute, ...]:
    """Replay minutes whose quotes/depth come only from the sampled block's evidence."""
    minutes = paths.btc if instrument == "BTCUSDT" else paths.eth
    offset = 0 if start_at_ns is None else start_at_ns - minutes[0].at_ns
    output: list[ReplayMinute] = []
    for index, minute in enumerate(minutes):
        hour = hours[min(index // 60, len(hours) - 1)]
        evidence = path_spread_depth(hour, instrument)
        if evidence is None:
            raise NotEstimable("NOT_ESTIMABLE: sampled hour lacks displayed quote/depth evidence")
        spread, depth = evidence
        mid = Decimal(str(minute.last.close))
        bid = mid * (Decimal("1") - spread / Decimal("20000"))
        ask = mid * (Decimal("1") + spread / Decimal("20000"))
        depth_qty = depth / mid
        output.append(ReplayMinute(minute.at_ns + offset, bid, ask, depth_qty, depth_qty,
                                   Decimal(str(minute.mark.low)), Decimal(str(minute.mark.high)),
                                   Decimal(str(minute.last.low)), Decimal(str(minute.last.high))))
    return tuple(output)


def replay_replicate(policy: FixedPolicy, hours: Sequence[JointResidualHour], *, model: HuberRidgeModel,
                     causal_closes: dict[str, tuple[HourlyClose, ...]], block_length: int, instrument: str,
                     inner_paths: int, seed: int, assumptions: ReplayAssumptions, funding_anchor: FundingAnchor,
                     support: BridgeSupport = DEFAULT_BRIDGE_SUPPORT,
                     minimum_observations: int = MINIMUM_COST_OBSERVATIONS,
                     cost: CostEstimate | None = None) -> tuple[float, int, ExecutionEvidence]:
    """Mean P&L over inner paths for one frozen action and one replicate model."""
    starts = eligible_starts(hours, block_length)
    if not starts:
        raise NotEstimable("NOT_ESTIMABLE: no contiguous synchronized blocks")
    window = _causal_window(causal_closes, instrument)
    if funding_anchor.latest_predicted_rate is None and funding_anchor.latest_settled_rate is None:
        raise NotEstimable("NOT_ESTIMABLE: no observable funding anchor")
    cost = cost or estimate_cost_assumptions(hours, minimum_observations=minimum_observations)
    anchor_kind = "PREDICTED" if funding_anchor.latest_predicted_rate is not None else "SETTLED"
    anchor = SynchronizedPrices(float(policy.mark_reference), float(policy.mark_reference),
                                float(policy.mark_reference))
    sampled = sample_blocks(hours, length=block_length, horizon_hours=HORIZON_HOURS + 1,
                            paths=inner_paths, seed=seed)
    replays: list[PolicyReplayResult] = []
    downgrade: str | None = None
    for path in sampled:
        latency_ns = path_latency_ns(path, instrument)
        if latency_ns is None:
            continue
        try:
            minute_paths = joint_minute_paths(path, initial={instrument: anchor},
                                              causal_closes={instrument: window}, model=model, support=support)
            first_minute = minute_paths.btc[0] if instrument == "BTCUSDT" else minute_paths.eth[0]
            base_offset = policy.decision_slot_at_ns - first_minute.at_ns
            minutes = replay_minutes(minute_paths, instrument, hours=path,
                                     start_at_ns=policy.decision_slot_at_ns)
            settlements, forecast = _funding_settlements(path, instrument=instrument, base_offset_ns=base_offset,
                                                         minutes=minutes, anchor=funding_anchor)
            downgrade = forecast.downgrade
        except ScenarioNotEstimable:
            continue
        except NotEstimable:
            continue
        effective = replace(assumptions, decision_to_venue_ns=assumptions.decision_to_venue_ns + latency_ns,
                            taker_fee_rate=cost.taker_fee_rate)
        replays.append(replay_policy(policy, minutes, settlements, effective))
    if not replays or len(replays) < MIN_ESTIMABLE_PATH_FRACTION * len(sampled):
        raise NotEstimable("NOT_ESTIMABLE: insufficient estimable replay paths")
    pnls = [float(result.pnl) for result in replays if result.pnl is not None]
    if len(pnls) < MIN_ESTIMABLE_PATH_FRACTION * len(replays):
        raise NotEstimable("NOT_ESTIMABLE: unbounded replay outcomes")
    execution = estimate_execution_evidence(hours, instrument=instrument, minimum_observations=minimum_observations,
                                            funding_anchor_kind=anchor_kind, funding_downgrade=downgrade)
    return fmean(pnls), len(pnls), execution


def replicate_evaluation(
    *, policy: FixedPolicy, history: Sequence[JointResidualHour], block_length: int, instrument: str,
    inner_paths: int, seed: int, assumptions: ReplayAssumptions, causal_closes: dict[str, tuple[HourlyClose, ...]],
    funding_anchor: FundingAnchor, locked_ridge: float, action_hash: str,
    support: BridgeSupport = DEFAULT_BRIDGE_SUPPORT,
    minimum_cost_observations: int = MINIMUM_COST_OBSERVATIONS,
) -> tuple[ReplicateEvaluation, ReplicateFit]:
    """One full frozen replicate: resample, locked refit, causal OOF, replay."""
    if not history:
        raise NotEstimable("NOT_ESTIMABLE: empty bootstrap replicate")
    sampled_hours = tuple(history)[-MAX_REPLICATE_HOURS:]
    oof_hours, refits = replicate_oof_hours(sampled_hours, locked_ridge=locked_ridge)
    current_model = refits[-1][1]
    rows = observations_from_hours(sampled_hours, range(len(sampled_hours)))
    selection = select_ridge_chronological(rows, refits[-1][0], locked_ridge=locked_ridge)
    window_start = selection.validation_intervals[0][0]
    training_rows = sum(1 for row in selection.eligible if row.label_at_ns < window_start)
    validation_rows = len(selection.eligible) - training_rows
    cost = estimate_cost_assumptions(oof_hours, minimum_observations=minimum_cost_observations)
    mean_pnl, paths, execution = replay_replicate(policy, oof_hours, model=current_model,
                                                  causal_closes=causal_closes, block_length=block_length,
                                                  instrument=instrument, inner_paths=inner_paths, seed=seed,
                                                  assumptions=assumptions, funding_anchor=funding_anchor,
                                                  support=support,
                                                  minimum_observations=minimum_cost_observations, cost=cost)
    fit = ReplicateFit(locked_ridge, training_rows, validation_rows, cost, execution,
                       tuple(instant for instant, _ in refits))
    return ReplicateEvaluation(mean_pnl, action_hash, True, True, True, True), fit


def outer_lcb(
    *, policy: FixedPolicy, history: Sequence[JointResidualHour], block_length: int, instrument: str,
    action_hash: str, assumptions: ReplayAssumptions, causal_closes: dict[str, tuple[HourlyClose, ...]],
    funding_anchor: FundingAnchor, locked_ridge: float,
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
                                        assumptions=assumptions, causal_closes=causal_closes,
                                        funding_anchor=funding_anchor, locked_ridge=locked_ridge,
                                        action_hash=action_hash, support=support,
                                        minimum_cost_observations=minimum_cost_observations)
        second, _ = replicate_evaluation(policy=policy, history=subset, block_length=block_length,
                                         instrument=instrument, inner_paths=inner_paths, seed=inner_seed_b,
                                         assumptions=assumptions, causal_closes=causal_closes,
                                         funding_anchor=funding_anchor, locked_ridge=locked_ridge,
                                         action_hash=action_hash, support=support,
                                         minimum_cost_observations=minimum_cost_observations)
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
