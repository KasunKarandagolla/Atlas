"""§10: the full frozen outer uncertainty process (refit, OOF, cost, same action)."""

from __future__ import annotations

import math
from dataclasses import replace
from decimal import Decimal

import pytest
from support.phase4_factory import policy

from atlas.domain.enums import Side
from atlas.science.huber_mean import HOUR_NS
from atlas.science.outer_loop import (
    NotEstimable,
    estimate_cost_assumptions,
    observations_from_hours,
    outer_lcb,
    replicate_evaluation,
    select_ridge_chronological,
)
from atlas.science.policy_replay import ReplayAssumptions
from atlas.science.residual_blocks import JointResidualHour
from atlas.science.uncertainty import (
    BOOTSTRAP_INNER_PATHS,
    BOOTSTRAP_REPLICATES,
    FAMILY_ERROR,
    INSTRUMENT_DELTA,
    OuterBootstrapResult,
    ReplicateEvaluation,
    block_bootstrap_indices,
    bootstrap_lcb,
    lcb_order_statistic,
    outer_expected_mean_bootstrap,
)

SLOT_NS = (1_700_000_000_000_000_000 // (4 * HOUR_NS)) * 4 * HOUR_NS


def bars(scale: float = 1.0) -> tuple[tuple[float, float, float, float], ...]:
    return tuple((scale, scale * 1.0004, scale * 0.9996, scale * 1.0002) for _ in range(60))


def hour(index: int, *, spread_bp: float = 1.2, forecast: float = 0.1, residual: float = 0.05) -> JointResidualHour:
    return JointResidualHour(
        at_ns=SLOT_NS + index * HOUR_NS,
        btc_residual=residual + 0.01 * math.sin(index / 3), eth_residual=residual,
        btc_forecast=forecast, eth_forecast=forecast,
        btc_sigma=0.01, eth_sigma=0.01, btc_z=0.4, eth_z=0.2,
        btc_last_ohlc=bars(), eth_last_ohlc=bars(50.0),
        btc_mark_ohlc=bars(1.0001), eth_mark_ohlc=bars(50.005),
        btc_index_ohlc=bars(0.9999), eth_index_ohlc=bars(49.995),
        execution_missing=False, minute_replay_complete=True,
        spread_depth_observations=({"spread_bp": str(spread_bp), "taker_fee_bp": "5.0"},),
        calendar_identity="cal", universe_identity="BTCUSDT_ETHUSDT_V1",
    )


def history(length: int = 96) -> tuple[JointResidualHour, ...]:
    return tuple(hour(index) for index in range(length))


def assumptions() -> ReplayAssumptions:
    return ReplayAssumptions(decision_to_venue_ns=0, human_delay_ns=0, tick=Decimal("0.1"),
                             taker_fee_rate=Decimal("0.0005"), stop_spread_impact=Decimal("0.01"),
                             time_exit_market_escalation_supported=True, extension_bound_supported=True)


def test_production_defaults_and_frozen_delta_are_exposed():
    assert (BOOTSTRAP_REPLICATES, BOOTSTRAP_INNER_PATHS) == (200, 512)
    assert FAMILY_ERROR == 0.05 and INSTRUMENT_DELTA == 0.025
    means = list(range(1, 201))
    assert lcb_order_statistic(means) == 5  # 1-indexed ceil(200 * 0.025)
    with pytest.raises(ValueError):
        lcb_order_statistic([], 0.025)


def test_block_bootstrap_is_explicit_and_deterministic():
    first = block_bootstrap_indices(50, block_length=12, count=3, seed=9)
    assert first == block_bootstrap_indices(50, block_length=12, count=3, seed=9)
    assert first != block_bootstrap_indices(50, block_length=12, count=3, seed=10)
    assert all(len(indices) == 50 for indices in first)
    with pytest.raises(ValueError):
        block_bootstrap_indices(5, block_length=12, count=1, seed=1)


def test_two_inner_seed_sets_can_flag_numerical_instability():
    stable, unstable = bootstrap_lcb([1.0] * 40, seed_a_means=[1.0] * 40, seed_b_means=[1.0] * 40)
    assert stable == 1.0 and unstable is False
    _, flipping = bootstrap_lcb([0.0] * 40, seed_a_means=[1.0] * 40, seed_b_means=[-1.0] * 40)
    assert flipping is True


def test_outer_orchestration_requires_every_frozen_replicate_stage():
    def evaluator(sample, seed, paths):  # type: ignore[no-untyped-def]
        return ReplicateEvaluation(1.0, "action", True, True, True, True)

    result = outer_expected_mean_bootstrap(history(), block_length=24, action_hash="action", evaluator=evaluator,
                                           replicates=8, inner_paths=4, replicate_seed=3)
    assert isinstance(result, OuterBootstrapResult)
    assert result.status == "ESTIMATED" and result.replicate_means == (1.0,) * 8
    assert result.lcb == 1.0

    def omitted(sample, seed, paths):  # type: ignore[no-untyped-def]
        return ReplicateEvaluation(1.0, "action", True, True, False, True)

    with pytest.raises(ValueError, match="omitted frozen"):
        outer_expected_mean_bootstrap(history(), block_length=24, action_hash="action", evaluator=omitted,
                                      replicates=2, inner_paths=2)

    def drifted(sample, seed, paths):  # type: ignore[no-untyped-def]
        return ReplicateEvaluation(1.0, "other-action", True, True, True, True)

    with pytest.raises(ValueError, match="immutable current action"):
        outer_expected_mean_bootstrap(history(), block_length=24, action_hash="action", evaluator=drifted,
                                      replicates=2, inner_paths=2)


def test_observations_use_forecast_plus_residual_and_ridge_ties_prefer_larger_lambda():
    rows = observations_from_hours(history(48), range(48))
    assert len(rows) == 96
    first = rows[0]
    hour0 = history(1)[0]
    assert first.next_return == pytest.approx(hour0.btc_sigma * (hour0.btc_forecast + hour0.btc_residual))
    assert select_ridge_chronological(rows) in (0.01, 0.1, 1.0, 10.0)
    with pytest.raises(NotEstimable):
        select_ridge_chronological(rows[:2])


def test_cost_re_estimation_requires_supported_observations():
    estimate = estimate_cost_assumptions(history(48), minimum_observations=24)
    assert estimate.observations == 48
    assert estimate.taker_fee_rate == Decimal("0.0005")
    assert float(estimate.spread_impact) == pytest.approx(0.00012)
    with pytest.raises(NotEstimable, match="insufficient execution/cost observations"):
        estimate_cost_assumptions(history(10), minimum_observations=24)
    no_obs = tuple(replace(hour(i), spread_depth_observations=()) for i in range(30))
    with pytest.raises(NotEstimable):
        estimate_cost_assumptions(no_obs, minimum_observations=24)


def test_full_outer_process_evaluates_the_same_frozen_action_with_small_counts():
    action = policy(side=Side.LONG, quantity=Decimal("1"), mark=Decimal("100"), sigma=0.01, slot_at_ns=SLOT_NS)
    result = outer_lcb(policy=action, history=history(72), block_length=24, instrument="BTCUSDT",
                       action_hash="frozen-action", assumptions=assumptions(), archive_sigma=0.01,
                       replicates=4, inner_paths=2, replicate_seed=5, inner_seed_a=6, inner_seed_b=7,
                       minimum_cost_observations=24)
    assert isinstance(result, OuterBootstrapResult)
    assert len(result.replicate_means) == 4
    assert result.status in {"ESTIMATED", "NO_TRADE_NUMERICAL"}
    evaluation, fit = replicate_evaluation(policy=action, history=history(72), block_length=24,
                                           instrument="BTCUSDT", inner_paths=2, seed=1, assumptions=assumptions(),
                                           archive_sigma=0.01, action_hash="frozen-action",
                                           minimum_cost_observations=24)
    assert evaluation.action_hash == "frozen-action" and evaluation.costs_reestimated is True
    assert fit.training_rows > 0 and fit.validation_rows > 0
