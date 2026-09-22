"""§3: outer bootstrap causal OOF, locked ridge, causal funding and evidence pairing."""

from __future__ import annotations

import math
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from functools import lru_cache

import pytest
from support.phase4_factory import policy

from atlas.domain.enums import Side
from atlas.science.huber_mean import (
    HOUR_NS,
    fit_huber_ridge,
    select_ridge_chronological,
)
from atlas.science.outer_loop import (
    FundingAnchor,
    NotEstimable,
    estimate_cost_assumptions,
    estimate_execution_evidence,
    observations_from_hours,
    outer_lcb,
    path_latency_ns,
    replay_replicate,
    replicate_evaluation,
    replicate_oof_hours,
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
from atlas.strategy.features import HourlyClose, feature_values

MINUTES = 60
SLOT_NS = int(datetime(2025, 1, 6, tzinfo=UTC).timestamp() * 1_000_000_000)  # Monday 00:00 UTC
DAYS = 150
LOCKED_RIDGE = 0.1


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


@lru_cache(maxsize=8)
def causal_window(*, price: float = 100.0, sigma: float = 0.01, momentum: float = 0.3,
                  end_at_ns: int = SLOT_NS, count: int = 721) -> tuple[HourlyClose, ...]:
    """A deterministic 721-close causal window ending at the decision instant."""
    returns = [sigma * (1.0 if index % 2 == 0 else -1.0) for index in range(count - 1)]
    for offset in range(24):
        returns[-24 + offset] += momentum * sigma
    closes = [price]
    for value in returns:
        closes.append(closes[-1] * math.exp(value))
    return tuple(HourlyClose(end_at_ns - (count - 1 - index) * HOUR_NS, Decimal(str(round(value, 8))),
                             end_at_ns - (count - 1 - index) * HOUR_NS, f"c{index}")
                 for index, value in enumerate(closes))


def hour(index: int, *, z: float, mu: float, residual: float, sigma: float = 0.01,
         funding_rate: str = "0.000010", **overrides: object) -> JointResidualHour:
    at_ns = SLOT_NS + index * HOUR_NS
    hour_return = sigma * (mu + residual)
    settles = index % 8 == 0
    kwargs: dict[str, object] = {
        "at_ns": at_ns, "btc_residual": residual, "eth_residual": residual, "btc_forecast": mu, "eth_forecast": mu,
        "btc_sigma": sigma, "eth_sigma": sigma, "btc_z": z, "eth_z": z,
        "btc_last_ohlc": hour_bars(hour_return, 0.0), "eth_last_ohlc": hour_bars(hour_return, 0.5),
        "btc_mark_ohlc": hour_bars(0.0, 0.1), "eth_mark_ohlc": hour_bars(0.0, 0.6),
        "btc_index_ohlc": hour_bars(0.0, 0.2), "eth_index_ohlc": hour_bars(0.0, 0.7),
        "execution_missing": False, "minute_replay_complete": True,
        "spread_depth_observations": (
            {"instrument": "BTCUSDT", "spread_bp": "1.2", "taker_fee_bp": "5.0", "depth_notional": "250000"},
            {"instrument": "ETHUSDT", "spread_bp": "1.6", "taker_fee_bp": "5.0", "depth_notional": "120000"},
        ),
        "latency_fill_observations": (
            {"instrument": "BTCUSDT", "entry_latency_ms": "120"},
            {"instrument": "ETHUSDT", "entry_latency_ms": "140"},
        ),
        "funding_publication_at_ns": at_ns - HOUR_NS if settles else None,
        "funding_settlement_at_ns": at_ns if settles else None,
        "funding_observations": tuple({"instrument": name, "rate": funding_rate} for name in ("BTCUSDT", "ETHUSDT"))
        if settles else (),
        "calendar_identity": "cal", "universe_identity": "BTCUSDT_ETHUSDT_V1",
    }
    kwargs.update(overrides)
    return JointResidualHour(**kwargs)  # type: ignore[arg-type]


@lru_cache(maxsize=8)
def history(days: int = DAYS, signal: float = 0.3, noise: float = 0.2,
            funding_base: float = 0.00001, funding_step: float = 0.000005) -> tuple[JointResidualHour, ...]:
    """Deterministic synchronized history with a mild real signal and funding changes."""
    state = 12345
    out: list[JointResidualHour] = []
    settlement_index = 0
    for index in range(days * 24):
        state = (1103515245 * state + 12345) % (2**31)
        epsilon = ((state / 2**31) - 0.5) * 2
        z = math.sin(index / 37.0) * 2.0
        rate = f"{funding_base + funding_step * settlement_index:.6f}"
        if index % 8 == 0:
            settlement_index += 1
        out.append(hour(index, z=z, mu=signal * z / 3.0, residual=noise * epsilon, funding_rate=rate))
    return tuple(out)


def assumptions(**overrides: object) -> ReplayAssumptions:
    base: dict[str, object] = {"decision_to_venue_ns": 0, "human_delay_ns": 0, "tick": Decimal("0.1"),
                               "taker_fee_rate": Decimal("0.0005"), "stop_spread_impact": Decimal("0"),
                               "time_exit_market_escalation_supported": True, "extension_bound_supported": True}
    base.update(overrides)
    return ReplayAssumptions(**base)  # type: ignore[arg-type]


def causal_closes(**kwargs: object) -> dict[str, tuple[HourlyClose, ...]]:
    return {"BTCUSDT": causal_window(**kwargs), "ETHUSDT": causal_window(**kwargs)}  # type: ignore[arg-type]


def anchor(*, predicted: str | None = "0.000010", settled: str | None = "0.000200",
           next_at_ns: int | None = SLOT_NS + 8 * HOUR_NS) -> FundingAnchor:
    return FundingAnchor(next_settlement_at_ns=next_at_ns,
                         latest_predicted_rate=Decimal(predicted) if predicted is not None else None,
                         latest_settled_rate=Decimal(settled) if settled is not None else None)


def action() -> object:
    return policy(side=Side.LONG, quantity=Decimal("1"), mark=Decimal("100"), sigma=0.01, slot_at_ns=SLOT_NS)


def fitted_model(rows: tuple[JointResidualHour, ...], *, days: int = DAYS):  # type: ignore[no-untyped-def]
    observations = observations_from_hours(rows, range(len(rows)))
    del days
    # The replay only needs frozen coefficients; the causal eligibility contract
    # itself is exercised through replicate_oof_hours/replicate_evaluation.
    return fit_huber_ridge(observations, LOCKED_RIDGE)


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
    fit_at = SLOT_NS + DAYS * 24 * HOUR_NS
    selection = select_ridge_chronological(long_rows, fit_at)
    assert selection.ridge in (0.01, 0.1, 1.0, 10.0)
    assert len(selection.validation_intervals) == 3
    assert max(row.label_at_ns for row in selection.eligible) <= fit_at
    with pytest.raises(ValueError, match="fewer than"):
        select_ridge_chronological(long_rows[:100], fit_at)


def test_locked_ridge_skips_the_replicate_search():
    rows = observations_from_hours(history(), range(len(history())))
    fit_at = SLOT_NS + DAYS * 24 * HOUR_NS
    searched = select_ridge_chronological(rows, fit_at)
    locked = select_ridge_chronological(rows, fit_at, locked_ridge=LOCKED_RIDGE)
    assert locked.ridge == LOCKED_RIDGE
    assert locked.eligible == searched.eligible
    assert locked.validation_intervals == searched.validation_intervals
    with pytest.raises(ValueError, match="unfrozen locked ridge"):
        select_ridge_chronological(rows, fit_at, locked_ridge=0.5)


def test_replicate_oof_is_chronological_and_uses_only_earlier_matured_labels():
    rows = history()
    oof_hours, refits = replicate_oof_hours(rows, locked_ridge=LOCKED_RIDGE)
    # Hours before the first matured 90-day refit have no genuine OOF forecast.
    assert len(oof_hours) == (DAYS - 91) * 24
    assert oof_hours[0].at_ns == refits[0][0]
    assert refits and all(model.ridge == LOCKED_RIDGE for _, model in refits)
    first_instant, first_model = refits[0]
    first = oof_hours[0]
    assert first_instant <= first.at_ns
    assert first.btc_forecast == pytest.approx(first_model.forecast("BTCUSDT", first.btc_z))
    # Recomputing that forecast from only the labels matured before the refit
    # reproduces the archived replicate OOF forecast exactly.
    observations = observations_from_hours(rows, range(len(rows)))
    selection = select_ridge_chronological(observations, first_instant, locked_ridge=LOCKED_RIDGE)
    independent = fit_huber_ridge(selection.eligible, LOCKED_RIDGE, fit_at_ns=first_instant)
    assert first.btc_forecast == pytest.approx(independent.forecast("BTCUSDT", first.btc_z))


def test_final_replicate_fit_cannot_rewrite_earlier_oof_forecasts():
    rows = history()
    oof_hours, refits = replicate_oof_hours(rows, locked_ridge=LOCKED_RIDGE)
    first, last_instant, last_model = oof_hours[0], refits[-1][0], refits[-1][1]
    assert last_instant > first.at_ns
    assert first.btc_forecast != pytest.approx(last_model.forecast("BTCUSDT", first.btc_z))
    baseline = tuple((hour.at_ns, hour.btc_forecast, hour.btc_residual) for hour in oof_hours[:240])
    mutated = tuple([replace(row, btc_forecast=row.btc_forecast * 50, eth_residual=row.eth_residual * 50)
                     if row.at_ns > oof_hours[240].at_ns else row for row in rows])
    rebuilt, _ = replicate_oof_hours(mutated, locked_ridge=LOCKED_RIDGE)
    assert tuple((hour.at_ns, hour.btc_forecast, hour.btc_residual) for hour in rebuilt[:240]) == baseline


def test_cost_and_execution_evidence_are_required_and_never_invented():
    rows = history(30)
    estimate = estimate_cost_assumptions(rows, minimum_observations=24)
    assert estimate.observations == 30 * 24 * 2
    assert float(estimate.spread_impact) == pytest.approx(0.00014)
    execution = estimate_execution_evidence(rows, instrument="BTCUSDT", minimum_observations=24)
    assert execution.entry_latency_ns == 120_000_000
    assert execution.depth_notional == Decimal("250000.0")
    assert float(execution.spread_bp) == pytest.approx(0.00012)
    with pytest.raises(NotEstimable, match="insufficient execution/cost observations"):
        estimate_cost_assumptions(rows[:10], minimum_observations=24)
    without_depth = tuple(replace(row, spread_depth_observations=({"instrument": "BTCUSDT", "spread_bp": "1.2"},))
                          for row in rows)
    with pytest.raises(NotEstimable, match="missing archived spread/depth evidence"):
        estimate_execution_evidence(without_depth, instrument="BTCUSDT", minimum_observations=24)


def test_path_latency_comes_from_the_sampled_block_not_a_replicate_median():
    rows = history(20)
    path = rows[:48]
    assert path_latency_ns(path, "BTCUSDT") == 120_000_000
    mutated = tuple(replace(row, latency_fill_observations=({"instrument": "BTCUSDT", "entry_latency_ms": "500"},))
                    for row in path)
    assert path_latency_ns(mutated, "BTCUSDT") == 500_000_000
    # Missing path latency is NOT_ESTIMABLE rather than substituted from elsewhere.
    missing = tuple(replace(row, latency_fill_observations=()) for row in path)
    assert path_latency_ns(missing, "BTCUSDT") is None


def test_funding_uses_current_anchor_plus_sampled_changes():
    frozen = action()
    rows = history(30)
    model = fitted_model(rows)
    base, _, execution = replay_replicate(frozen, rows, model=model, causal_closes=causal_closes(),
                                          block_length=48, instrument="BTCUSDT", inner_paths=2, seed=1,
                                          assumptions=assumptions(), funding_anchor=anchor(predicted="0.000010"))
    assert execution.funding_anchor_kind == "PREDICTED"
    assert execution.funding_downgrade is None
    # Changing the current anchor changes the simulated funding path.
    higher, _, _ = replay_replicate(frozen, rows, model=model, causal_closes=causal_closes(),
                                    block_length=48, instrument="BTCUSDT", inner_paths=2, seed=1,
                                    assumptions=assumptions(), funding_anchor=anchor(predicted="0.005000"))
    assert higher != base
    # The fallback settled anchor carries the explicit downgrade label.
    _, _, fallback = replay_replicate(frozen, rows, model=model, causal_closes=causal_closes(),
                                      block_length=48, instrument="BTCUSDT", inner_paths=2, seed=1,
                                      assumptions=assumptions(), funding_anchor=anchor(predicted=None))
    assert fallback.funding_anchor_kind == "SETTLED"
    assert fallback.funding_downgrade == "PREDICTED_HISTORY_UNAVAILABLE"
    # No observable anchor at all is not estimable.
    with pytest.raises(NotEstimable, match="no observable funding anchor"):
        replay_replicate(frozen, rows, model=model, causal_closes=causal_closes(), block_length=48,
                         instrument="BTCUSDT", inner_paths=2, seed=1, assumptions=assumptions(),
                         funding_anchor=FundingAnchor())


def test_historical_absolute_funding_level_is_not_replayed_as_the_anchor():
    """Two blocks with identical CHANGES but different absolute levels must agree."""
    frozen = action()
    low = history(30, funding_base=0.00001, funding_step=0.000005)
    high = history(30, funding_base=0.50000, funding_step=0.000005)
    model = fitted_model(low)
    base_anchor = anchor(predicted="0.000010")
    low_mean, _, _ = replay_replicate(frozen, low, model=model, causal_closes=causal_closes(), block_length=48,
                                      instrument="BTCUSDT", inner_paths=2, seed=1, assumptions=assumptions(),
                                      funding_anchor=base_anchor)
    high_mean, _, _ = replay_replicate(frozen, high, model=model, causal_closes=causal_closes(), block_length=48,
                                       instrument="BTCUSDT", inner_paths=2, seed=1, assumptions=assumptions(),
                                       funding_anchor=base_anchor)
    assert low_mean == pytest.approx(high_mean, abs=1e-9)


def test_sampled_funding_changes_move_the_simulated_path():
    frozen = action()
    flat = history(30, funding_step=0.0)
    rising = history(30, funding_step=0.000200)
    model = fitted_model(flat)
    base_anchor = anchor(predicted="0.000010")
    flat_mean, _, _ = replay_replicate(frozen, flat, model=model, causal_closes=causal_closes(), block_length=48,
                                       instrument="BTCUSDT", inner_paths=2, seed=1, assumptions=assumptions(),
                                       funding_anchor=base_anchor)
    rising_mean, _, _ = replay_replicate(frozen, rising, model=model, causal_closes=causal_closes(), block_length=48,
                                         instrument="BTCUSDT", inner_paths=2, seed=1, assumptions=assumptions(),
                                         funding_anchor=base_anchor)
    assert flat_mean != rising_mean


def test_changing_bootstrap_training_labels_changes_replicate_pnl():
    frozen = action()
    first, _ = replicate_evaluation(policy=frozen, history=history(150, signal=0.3), block_length=48,
                                    instrument="BTCUSDT", inner_paths=2, seed=1, assumptions=assumptions(),
                                    causal_closes=causal_closes(), funding_anchor=anchor(),
                                    locked_ridge=LOCKED_RIDGE, action_hash="frozen-action")
    second, _ = replicate_evaluation(policy=frozen, history=history(150, signal=0.0), block_length=48,
                                     instrument="BTCUSDT", inner_paths=2, seed=1, assumptions=assumptions(),
                                     causal_closes=causal_closes(), funding_anchor=anchor(),
                                     locked_ridge=LOCKED_RIDGE, action_hash="frozen-action")
    assert first.mean_pnl != second.mean_pnl
    assert first.action_hash == second.action_hash == "frozen-action"
    assert first.scaler_refit and first.ridge_reselected and first.chronological_oof_rebuilt and first.costs_reestimated


def test_replicate_is_deterministic_and_keeps_the_locked_ridge():
    frozen = action()
    rows = history(150)
    first, fit_a = replicate_evaluation(policy=frozen, history=rows, block_length=48, instrument="BTCUSDT",
                                        inner_paths=2, seed=5, assumptions=assumptions(),
                                        causal_closes=causal_closes(), funding_anchor=anchor(),
                                        locked_ridge=LOCKED_RIDGE, action_hash="frozen-action")
    second, fit_b = replicate_evaluation(policy=frozen, history=rows, block_length=48, instrument="BTCUSDT",
                                         inner_paths=2, seed=5, assumptions=assumptions(),
                                         causal_closes=causal_closes(), funding_anchor=anchor(),
                                         locked_ridge=LOCKED_RIDGE, action_hash="frozen-action")
    assert first.mean_pnl == second.mean_pnl and fit_a == fit_b
    assert fit_a.ridge == LOCKED_RIDGE and fit_b.ridge == LOCKED_RIDGE
    assert fit_a.training_rows > 0 and fit_a.validation_rows > 0
    assert len(fit_a.refit_instants) >= 1


def test_full_outer_process_evaluates_the_same_frozen_action_with_small_counts():
    frozen = policy(side=Side.LONG, quantity=Decimal("1"), mark=Decimal("100"), sigma=0.01, slot_at_ns=SLOT_NS)
    result = outer_lcb(policy=frozen, history=history(150), block_length=48, instrument="BTCUSDT",
                       action_hash="frozen-action", assumptions=assumptions(), causal_closes=causal_closes(),
                       funding_anchor=anchor(), locked_ridge=LOCKED_RIDGE,
                       replicates=2, inner_paths=2, replicate_seed=5, inner_seed_a=6, inner_seed_b=7,
                       minimum_cost_observations=24)
    assert isinstance(result, OuterBootstrapResult)
    assert len(result.replicate_means) == 2
    assert result.action_hash == "frozen-action"
    assert result.status in {"ESTIMATED", "NO_TRADE_NUMERICAL"}


def test_causal_window_is_produced_by_the_frozen_feature_engine():
    values = feature_values(causal_window())
    assert values.end_at_ns == SLOT_NS
    assert values.sigma > 0 and math.isfinite(values.z)
    assert DEFAULT_BRIDGE_SUPPORT.barrier_calibrated is True


def test_changed_bootstrap_training_labels_move_replicate_mean_pnl():
    frozen = action()
    rows = history(100)
    # Mutate only the early (training) labels of the resampled records: the
    # replicate refit must consume them and move the scenario P&L.
    mutated = tuple(replace(row, btc_forecast=row.btc_forecast + 0.5, eth_forecast=row.eth_forecast + 0.5)
                    if row.at_ns < rows[0].at_ns + 20 * 24 * HOUR_NS else row for row in rows)
    baseline, fit_a = replicate_evaluation(policy=frozen, history=rows, block_length=48, instrument="BTCUSDT",
                                           inner_paths=2, seed=3, assumptions=assumptions(),
                                           causal_closes=causal_closes(), funding_anchor=anchor(),
                                           locked_ridge=LOCKED_RIDGE, action_hash="frozen-action")
    changed, fit_b = replicate_evaluation(policy=frozen, history=mutated, block_length=48, instrument="BTCUSDT",
                                          inner_paths=2, seed=3, assumptions=assumptions(),
                                          causal_closes=causal_closes(), funding_anchor=anchor(),
                                          locked_ridge=LOCKED_RIDGE, action_hash="frozen-action")
    assert baseline.mean_pnl != changed.mean_pnl
    assert fit_a.ridge == fit_b.ridge == LOCKED_RIDGE


def test_locked_ridge_is_identical_across_bootstrap_replicates():
    frozen = action()
    rows = history(150)
    subsets = block_bootstrap_indices(len(rows), block_length=48, count=2, seed=11)
    ridges = set()
    for indices in subsets:
        subset = tuple(rows[index] for index in indices)
        _, fit = replicate_evaluation(policy=frozen, history=subset, block_length=48, instrument="BTCUSDT",
                                      inner_paths=2, seed=1, assumptions=assumptions(),
                                      causal_closes=causal_closes(), funding_anchor=anchor(),
                                      locked_ridge=LOCKED_RIDGE, action_hash="frozen-action")
        ridges.add(fit.ridge)
    assert ridges == {LOCKED_RIDGE}
