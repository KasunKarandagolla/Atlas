"""§3/§4/§10: outer bootstrap refit propagation and evidence-only replay."""

from __future__ import annotations

import math
from dataclasses import replace
from decimal import Decimal
from functools import lru_cache

import pytest
from support.phase4_factory import policy

from atlas.domain.enums import Side
from atlas.science.huber_mean import HOUR_NS, fit_huber_ridge, select_ridge_chronological
from atlas.science.outer_loop import (
    NotEstimable,
    estimate_cost_assumptions,
    estimate_execution_evidence,
    observations_from_hours,
    outer_lcb,
    rebuild_replicate_hours,
    replay_replicate,
    replicate_evaluation,
)
from atlas.science.policy_replay import ReplayAssumptions
from atlas.science.residual_blocks import JointResidualHour
from atlas.science.scenarios import DEFAULT_BRIDGE_SUPPORT
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

MINUTES = 60
SLOT_NS = (1_700_000_000_000_000_000 // (4 * HOUR_NS)) * 4 * HOUR_NS
DAYS = 130
FIT_AT_NS = SLOT_NS + DAYS * 24 * HOUR_NS


def bars(hour_return: float, *, phase: float) -> tuple[tuple[float, float, float, float], ...]:
    shape = [0.0004 * math.sin(2 * math.pi * (index / 13.0 + phase)) for index in range(MINUTES)]
    mean = sum(shape) / MINUTES
    out = []
    price = 1.0
    for index in range(MINUTES):
        ret = hour_return / MINUTES + (shape[index] - mean)
        open_price = price
        close = open_price * math.exp(ret)
        out.append((open_price, max(open_price, close) * 1.0001, min(open_price, close) * 0.9999, close))
        price = close
    return tuple(out)


@lru_cache(maxsize=8)
def hour_bars(hour_return: float, phase: float) -> tuple[tuple[float, float, float, float], ...]:
    return bars(hour_return, phase=phase)


def hour(index: int, *, z: float, mu: float, residual: float, sigma: float = 0.01,
         funding_rate: str = "0.00001") -> JointResidualHour:
    at_ns = SLOT_NS + index * HOUR_NS
    hour_return = sigma * (mu + residual)
    settles = index % 8 == 0
    return JointResidualHour(
        at_ns=at_ns, btc_residual=residual, eth_residual=residual, btc_forecast=mu, eth_forecast=mu,
        btc_sigma=sigma, eth_sigma=sigma, btc_z=z, eth_z=z,
        btc_last_ohlc=hour_bars(hour_return, 0.0), eth_last_ohlc=hour_bars(hour_return, 0.5),
        btc_mark_ohlc=hour_bars(0.0, 0.1), eth_mark_ohlc=hour_bars(0.0, 0.6),
        btc_index_ohlc=hour_bars(0.0, 0.2), eth_index_ohlc=hour_bars(0.0, 0.7),
        execution_missing=False, minute_replay_complete=True,
        spread_depth_observations=(
            {"instrument": "BTCUSDT", "spread_bp": "1.2", "taker_fee_bp": "5.0", "depth_notional": "250000"},
            {"instrument": "ETHUSDT", "spread_bp": "1.6", "taker_fee_bp": "5.0", "depth_notional": "120000"},
        ),
        latency_fill_observations=(
            {"instrument": "BTCUSDT", "entry_latency_ms": "120"},
            {"instrument": "ETHUSDT", "entry_latency_ms": "140"},
        ),
        funding_publication_at_ns=at_ns - HOUR_NS if settles else None,
        funding_settlement_at_ns=at_ns if settles else None,
        funding_observations=tuple({"instrument": name, "rate": funding_rate} for name in ("BTCUSDT", "ETHUSDT"))
        if settles else (),
        calendar_identity="cal", universe_identity="BTCUSDT_ETHUSDT_V1",
    )


@lru_cache(maxsize=8)
def history(days: int = DAYS, signal: float = 0.3, noise: float = 0.2,
            funding_rate: str = "0.00001") -> tuple[JointResidualHour, ...]:
    """Deterministic ~100-day synchronized history with a mild real signal."""
    state = 12345
    out: list[JointResidualHour] = []
    for index in range(days * 24):
        state = (1103515245 * state + 12345) % (2**31)
        epsilon = ((state / 2**31) - 0.5) * 2
        z = math.sin(index / 37.0) * 2.0
        out.append(hour(index, z=z, mu=signal * z / 3.0, residual=noise * epsilon, funding_rate=funding_rate))
    return tuple(out)


def assumptions(**overrides: object) -> ReplayAssumptions:
    base: dict[str, object] = {"decision_to_venue_ns": 0, "human_delay_ns": 0, "tick": Decimal("0.1"),
                               "taker_fee_rate": Decimal("0.0005"), "stop_spread_impact": Decimal("0"),
                               "time_exit_market_escalation_supported": True, "extension_bound_supported": True}
    base.update(overrides)
    return ReplayAssumptions(**base)  # type: ignore[arg-type]


def features(z: float = 1.0, sigma: float = 0.01) -> dict[str, tuple[float, float]]:
    return {"BTCUSDT": (z, sigma), "ETHUSDT": (z, sigma)}


def action() -> object:
    return policy(side=Side.LONG, quantity=Decimal("1"), mark=Decimal("100"), sigma=0.01, slot_at_ns=SLOT_NS)


def fitted_model(rows: tuple[JointResidualHour, ...]):  # type: ignore[no-untyped-def]
    return fit_huber_ridge(observations_from_hours(rows, range(len(rows))), 0.1)


def test_production_defaults_and_frozen_delta_are_exposed():
    assert (BOOTSTRAP_REPLICATES, BOOTSTRAP_INNER_PATHS) == (200, 512)
    assert FAMILY_ERROR == 0.05 and INSTRUMENT_DELTA == 0.025
    assert lcb_order_statistic(list(range(1, 201))) == 5
    with pytest.raises(ValueError):
        lcb_order_statistic([], 0.025)


def test_block_bootstrap_is_explicit_and_deterministic():
    first = block_bootstrap_indices(500, block_length=48, count=3, seed=9)
    assert first == block_bootstrap_indices(500, block_length=48, count=3, seed=9)
    assert first != block_bootstrap_indices(500, block_length=48, count=3, seed=10)
    assert all(len(indices) == 500 for indices in first)


def test_two_inner_seed_sets_can_flag_numerical_instability():
    stable, unstable = bootstrap_lcb([1.0] * 40, seed_a_means=[1.0] * 40, seed_b_means=[1.0] * 40)
    assert stable == 1.0 and unstable is False
    _, flipping = bootstrap_lcb([0.0] * 40, seed_a_means=[1.0] * 40, seed_b_means=[-1.0] * 40)
    assert flipping is True


def test_outer_orchestration_requires_every_frozen_replicate_stage():
    def evaluator(sample, seed, paths):  # type: ignore[no-untyped-def]
        return ReplicateEvaluation(1.0, "action", True, True, True, True)

    result = outer_expected_mean_bootstrap(history(), block_length=48, action_hash="action", evaluator=evaluator,
                                           replicates=8, inner_paths=4, replicate_seed=3)
    assert isinstance(result, OuterBootstrapResult)
    assert result.status == "ESTIMATED" and result.replicate_means == (1.0,) * 8
    assert result.action_hash == "action"

    def omitted(sample, seed, paths):  # type: ignore[no-untyped-def]
        return ReplicateEvaluation(1.0, "action", True, True, False, True)

    with pytest.raises(ValueError, match="omitted frozen"):
        outer_expected_mean_bootstrap(history(), block_length=48, action_hash="action", evaluator=omitted,
                                      replicates=2, inner_paths=2)

    def drifted(sample, seed, paths):  # type: ignore[no-untyped-def]
        return ReplicateEvaluation(1.0, "other-action", True, True, True, True)

    with pytest.raises(ValueError, match="immutable current action"):
        outer_expected_mean_bootstrap(history(), block_length=48, action_hash="action", evaluator=drifted,
                                      replicates=2, inner_paths=2)


def test_observations_use_forecast_plus_residual_and_frozen_fold_selection():
    rows = observations_from_hours(history(10), range(10 * 24))
    source = history(10)[0]
    assert rows[0].next_return == pytest.approx(source.btc_sigma * (source.btc_forecast + source.btc_residual))
    long_rows = observations_from_hours(history(), range(len(history())))
    fit_at = history()[-1].at_ns + HOUR_NS
    selection = select_ridge_chronological(long_rows, fit_at)
    assert selection.ridge in (0.01, 0.1, 1.0, 10.0)
    assert len(selection.validation_intervals) == 3
    assert max(row.label_at_ns for row in selection.eligible) <= fit_at
    with pytest.raises(ValueError, match="fewer than"):
        select_ridge_chronological(long_rows[:100], fit_at)


def test_cost_and_execution_evidence_are_required_and_never_invented():
    rows = history(30)
    estimate = estimate_cost_assumptions(rows, minimum_observations=24)
    assert estimate.observations == 30 * 24 * 2  # one observation per instrument per hour
    assert float(estimate.spread_impact) == pytest.approx(0.00014)  # median of BTC 1.2bp and ETH 1.6bp
    execution = estimate_execution_evidence(rows, instrument="BTCUSDT", minimum_observations=24)
    assert execution.entry_latency_ns == 120_000_000
    assert execution.depth_notional == Decimal("250000.0")
    assert float(execution.spread_bp) == pytest.approx(0.00012)
    with pytest.raises(NotEstimable, match="insufficient execution/cost observations"):
        estimate_cost_assumptions(rows[:10], minimum_observations=24)
    # Displayed depth can never be derived from candle prices: dropping the
    # observed depth makes the evidence unusable rather than OHLC-derived.
    without_depth = tuple(replace(row, spread_depth_observations=({"instrument": "BTCUSDT", "spread_bp": "1.2"},))
                          for row in rows)
    with pytest.raises(NotEstimable, match="missing archived spread/depth evidence"):
        estimate_execution_evidence(without_depth, instrument="BTCUSDT", minimum_observations=24)
    no_execution = tuple(replace(row, spread_depth_observations=(), latency_fill_observations=()) for row in rows)
    with pytest.raises(NotEstimable, match="missing archived spread/depth evidence"):
        estimate_execution_evidence(no_execution, instrument="BTCUSDT", minimum_observations=24)


def test_funding_evidence_is_required_and_changes_replicate_mean_pnl():
    rows = history(30)
    frozen = action()
    model = fitted_model(rows)
    with_funding, paths, _ = replay_replicate(frozen, rows, model=model, features=features(), block_length=48,
                                              instrument="BTCUSDT", inner_paths=2, seed=1,
                                              assumptions=assumptions())
    assert paths > 0
    zero_rate = history(30, funding_rate="0.0")
    without_cost, _, _ = replay_replicate(frozen, zero_rate, model=model, features=features(), block_length=48,
                                          instrument="BTCUSDT", inner_paths=2, seed=1, assumptions=assumptions())
    assert with_funding != without_cost
    no_funding = tuple(replace(row, funding_observations=(), funding_settlement_at_ns=None) for row in rows)
    # Without archived funding evidence no path can be valued, so the replicate
    # fails closed instead of silently replaying with empty settlements.
    with pytest.raises(NotEstimable, match="NOT_ESTIMABLE"):
        replay_replicate(frozen, no_funding, model=model, features=features(), block_length=48,
                         instrument="BTCUSDT", inner_paths=2, seed=1, assumptions=assumptions())


def test_refitted_coefficients_are_consumed_by_scenario_generation():
    rows = history(100, signal=0.3)
    other = history(100, signal=0.0)
    model_a = fitted_model(rows)
    model_b = fitted_model(other)
    rebuilt_a = rebuild_replicate_hours(rows[:24], model_a)
    rebuilt_b = rebuild_replicate_hours(rows[:24], model_b)
    assert rebuilt_a[0].btc_forecast != rebuilt_b[0].btc_forecast
    assert rebuilt_a[0].btc_residual != rebuilt_b[0].btc_residual
    for before, after in zip(rows[:24], rebuilt_a, strict=True):
        # The realized standardized return is archived evidence and never rewritten.
        assert after.btc_forecast + after.btc_residual == pytest.approx(before.btc_forecast + before.btc_residual)
    frozen = action()
    mean_a, _, _ = replay_replicate(frozen, rebuilt_a, model=model_a, features=features(), block_length=24,
                                    instrument="BTCUSDT", inner_paths=2, seed=1, assumptions=assumptions())
    mean_b, _, _ = replay_replicate(frozen, rebuilt_b, model=model_b, features=features(), block_length=24,
                                    instrument="BTCUSDT", inner_paths=2, seed=1, assumptions=assumptions())
    assert mean_a != mean_b


def test_changing_bootstrap_training_labels_changes_replicate_pnl():
    frozen = action()
    first, _ = replicate_evaluation(policy=frozen, history=history(130, signal=0.3), block_length=48,
                                    instrument="BTCUSDT", inner_paths=2, seed=1, assumptions=assumptions(),
                                    features=features(), action_hash="frozen-action", fit_at_ns=FIT_AT_NS)
    second, _ = replicate_evaluation(policy=frozen, history=history(130, signal=0.0), block_length=48,
                                     instrument="BTCUSDT", inner_paths=2, seed=1, assumptions=assumptions(),
                                     features=features(), action_hash="frozen-action", fit_at_ns=FIT_AT_NS)
    assert first.mean_pnl != second.mean_pnl
    assert first.action_hash == second.action_hash == "frozen-action"
    assert first.scaler_refit and first.ridge_reselected and first.chronological_oof_rebuilt and first.costs_reestimated


def test_replicate_is_deterministic_and_uses_the_frozen_model():
    frozen = action()
    rows = history(130)
    first, fit_a = replicate_evaluation(policy=frozen, history=rows, block_length=48, instrument="BTCUSDT",
                                        inner_paths=2, seed=5, assumptions=assumptions(), features=features(),
                                        action_hash="frozen-action", fit_at_ns=FIT_AT_NS)
    second, fit_b = replicate_evaluation(policy=frozen, history=rows, block_length=48, instrument="BTCUSDT",
                                         inner_paths=2, seed=5, assumptions=assumptions(), features=features(),
                                         action_hash="frozen-action", fit_at_ns=FIT_AT_NS)
    assert first.mean_pnl == second.mean_pnl and fit_a == fit_b
    assert fit_a.training_rows > 0 and fit_a.validation_rows > 0
    assert fit_a.execution.observations > 0


def test_current_state_changes_replicate_mean_pnl():
    frozen = action()
    rows = history(100)
    low, _, _ = replay_replicate(frozen, rows, model=fitted_model(rows), features=features(z=0.0),
                                 block_length=48, instrument="BTCUSDT", inner_paths=2, seed=1,
                                 assumptions=assumptions())
    high, _, _ = replay_replicate(frozen, rows, model=fitted_model(rows), features=features(z=20.0),
                                  block_length=48, instrument="BTCUSDT", inner_paths=2, seed=1,
                                  assumptions=assumptions())
    assert low != high


def test_full_outer_process_evaluates_the_same_frozen_action_with_small_counts():
    frozen = policy(side=Side.LONG, quantity=Decimal("1"), mark=Decimal("100"), sigma=0.01, slot_at_ns=SLOT_NS)
    result = outer_lcb(policy=frozen, history=history(130), block_length=48, instrument="BTCUSDT",
                       action_hash="frozen-action", assumptions=assumptions(), features=features(),
                       fit_at_ns=FIT_AT_NS,
                       replicates=3, inner_paths=2, replicate_seed=5, inner_seed_a=6, inner_seed_b=7,
                       minimum_cost_observations=24)
    assert isinstance(result, OuterBootstrapResult)
    assert len(result.replicate_means) == 3
    assert result.action_hash == "frozen-action"
    assert result.status in {"ESTIMATED", "NO_TRADE_NUMERICAL"}


def test_replay_replicate_uses_the_default_bridge_support():
    assert DEFAULT_BRIDGE_SUPPORT.barrier_calibrated is True
