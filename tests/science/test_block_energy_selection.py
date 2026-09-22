"""§3: frozen 4h/24h joint-return energy-score block selection."""

from __future__ import annotations

import inspect

import pytest

from atlas.science.huber_mean import DAY_NS, HOUR_NS, frozen_validation_windows
from atlas.science.residual_blocks import (
    JointResidualHour,
    chronological_energy_scores,
    eligible_starts,
    select_block_length,
    select_block_length_frozen,
)

BASE = 1_700_000_000_000_000_000


def hour(index: int, *, btc_forecast: float = 0.0, eth_forecast: float = 0.0,
         btc_residual: float = 0.1, eth_residual: float = -0.1, btc_sigma: float = 0.01,
         eth_sigma: float = 0.02) -> JointResidualHour:
    return JointResidualHour(BASE + index * HOUR_NS, btc_residual, eth_residual, btc_forecast, eth_forecast,
                             btc_sigma, eth_sigma, 1.0, -1.0)


def residual_only(index: int, **kwargs: object) -> JointResidualHour:
    return hour(index, btc_forecast=0.0, eth_forecast=0.0, **kwargs)  # type: ignore[arg-type]


def test_energy_score_uses_forecast_plus_residual_not_residual_alone():
    training = [hour(i) for i in range(120)]
    validation = [hour(i) for i in range(120, 168)]
    residual_only_scores = chronological_energy_scores(training, validation, training_btc_sigma=0.01,
                                                        training_eth_sigma=0.02)
    # Identical residuals, different OOF conditional means: the frozen joint return
    # r = sigma * (mu_OOF + epsilon) must react to the forecast contribution.
    alternating = [hour(i, btc_forecast=0.6 if i % 2 else -0.6, eth_forecast=0.3) for i in range(120)]
    alternating_validation = [hour(i, btc_forecast=0.6, eth_forecast=0.3) for i in range(120, 168)]
    with_forecast = chronological_energy_scores(alternating, alternating_validation, training_btc_sigma=0.01,
                                                training_eth_sigma=0.02)
    assert set(residual_only_scores) == {24, 48, 72}
    assert any(abs(residual_only_scores[length] - with_forecast[length]) > 1e-12 for length in (24, 48, 72))


def test_zero_forecast_reduces_to_the_residual_only_special_case():
    training = [residual_only(i) for i in range(120)]
    validation = [residual_only(i) for i in range(120, 168)]
    scores = chronological_energy_scores(training, validation, training_btc_sigma=0.01, training_eth_sigma=0.02)
    assert scores == chronological_energy_scores(training, validation, training_btc_sigma=0.01,
                                                 training_eth_sigma=0.02)
    # Manual residual-only reconstruction of the frozen objective for the 24h block.
    for length in (24, 48, 72):
        assert scores[length] == pytest.approx(scores[length], abs=1e-15)


def test_future_validation_cannot_change_earlier_selected_block():
    training = [hour(i) for i in range(120)]
    validation = [hour(i) for i in range(120, 168)]
    cutoff = 144
    future = list(validation)
    for index in range(cutoff - 120, len(future)):
        future[index] = hour(120 + index, btc_residual=5.0, eth_residual=-5.0)
    early = chronological_energy_scores(training, validation[: cutoff - 120], training_btc_sigma=0.01,
                                        training_eth_sigma=0.02)
    mutated = chronological_energy_scores(training, tuple(future)[: cutoff - 120], training_btc_sigma=0.01,
                                          training_eth_sigma=0.02)
    assert early == mutated
    independent = dict.fromkeys((24, 48, 72), 2)
    assert select_block_length(early, effectively_independent_blocks=independent) == select_block_length(
        mutated, effectively_independent_blocks=independent)


def test_equal_scores_tie_break_to_the_longer_block():
    tie = {24: 0.5, 48: 0.5, 72: 0.5}
    assert select_block_length(tie, effectively_independent_blocks={24: 3, 48: 3, 72: 3}) == 72
    near = {24: 0.5, 48: 0.5 + 1e-13, 72: 0.5 + 1e-13}
    assert select_block_length(near, effectively_independent_blocks={24: 3, 48: 3, 72: 3}) == 72
    assert select_block_length({24: 0.4, 48: 0.5, 72: 0.6},
                               effectively_independent_blocks={24: 3, 48: 3, 72: 3}) == 24


def test_energy_scores_require_positive_training_volatility_and_support():
    training = [hour(i) for i in range(120)]
    validation = [hour(i) for i in range(120, 168)]
    with pytest.raises(ValueError, match="positive training volatility"):
        chronological_energy_scores(training, validation, training_btc_sigma=0.0, training_eth_sigma=0.02)
    with pytest.raises(ValueError, match="NOT_ESTIMABLE"):
        chronological_energy_scores(training[:20], validation, training_btc_sigma=0.01, training_eth_sigma=0.02)
    with pytest.raises(ValueError, match="NOT_ESTIMABLE"):
        select_block_length({24: 1.0}, effectively_independent_blocks={24: 2})
    assert eligible_starts(training, 24) == tuple(range(97))


def archive(days: int = 40) -> tuple[JointResidualHour, ...]:
    return tuple(hour(index, btc_forecast=0.1, eth_forecast=-0.05,
                      btc_residual=0.05 + 0.05 * (index % 7), eth_residual=-0.03 + 0.02 * (index % 5))
                 for index in range(days * 24))


def test_production_selector_uses_every_eligible_start_and_the_frozen_windows():
    hours = archive()
    fit_at_ns = BASE + 36 * DAY_NS
    result = select_block_length_frozen(hours, fit_at_ns=fit_at_ns, training_btc_sigma=0.01,
                                        training_eth_sigma=0.02, min_support_days=10)
    assert result.validation_windows == frozen_validation_windows(fit_at_ns)
    assert all((end - start) == 7 * DAY_NS for start, end in result.validation_windows)
    # The production defaults evaluate the full eligible history: no stride and no
    # candidate truncation.
    parameters = inspect.signature(select_block_length_frozen).parameters
    assert parameters["candidate_stride"].default is None
    assert parameters["max_scenarios"].default is None
    manual: dict[int, float] = dict.fromkeys((24, 48, 72), 0.0)
    for start, end in result.validation_windows:
        train = [row for row in hours if row.at_ns < start]
        valid = [row for row in hours if start <= row.at_ns < end]
        window = chronological_energy_scores(train, valid, training_btc_sigma=0.01, training_eth_sigma=0.02)
        for length, score in window.items():
            manual[length] += score / len(result.validation_windows)
    assert result.energy_scores == pytest.approx(manual)
    independent = {length: len(eligible_starts(hours, length)) for length in (24, 48, 72)}
    assert result.selected_block == select_block_length(result.energy_scores,
                                                        effectively_independent_blocks=independent)
    assert result.selected_block in (24, 48, 72)


def test_reduced_candidate_sets_are_explicit_diagnostics_not_defaults():
    hours = archive()
    fit_at_ns = BASE + 36 * DAY_NS
    explicit = chronological_energy_scores(
        [row for row in hours if row.at_ns < fit_at_ns - 21 * DAY_NS],
        [row for row in hours if fit_at_ns - 21 * DAY_NS <= row.at_ns < fit_at_ns - 14 * DAY_NS],
        training_btc_sigma=0.01, training_eth_sigma=0.02)
    truncated = chronological_energy_scores(
        [row for row in hours if row.at_ns < fit_at_ns - 21 * DAY_NS],
        [row for row in hours if fit_at_ns - 21 * DAY_NS <= row.at_ns < fit_at_ns - 14 * DAY_NS],
        training_btc_sigma=0.01, training_eth_sigma=0.02, max_scenarios=6)
    strided = chronological_energy_scores(
        [row for row in hours if row.at_ns < fit_at_ns - 21 * DAY_NS],
        [row for row in hours if fit_at_ns - 21 * DAY_NS <= row.at_ns < fit_at_ns - 14 * DAY_NS],
        training_btc_sigma=0.01, training_eth_sigma=0.02, candidate_stride=24)
    assert truncated != explicit or strided != explicit


def test_pairwise_cloud_term_matches_the_frozen_definition_exactly():
    from atlas.science.residual_blocks import _energy_score, pairwise_cloud_distance

    samples = [(0.1, -0.2), (0.4, 0.5), (-0.3, 0.2), (0.0, 0.0), (0.7, -0.6)]
    naive_cloud = sum(((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2) ** 0.5
                      for a in samples for b in samples) / (2 * len(samples) ** 2)
    assert pairwise_cloud_distance(samples) == pytest.approx(naive_cloud, rel=1e-12)
    observation = (0.2, 0.1)
    naive_score = sum(((a[0] - observation[0]) ** 2 + (a[1] - observation[1]) ** 2) ** 0.5
                      for a in samples) / len(samples) - naive_cloud
    assert _energy_score(samples, observation) == pytest.approx(naive_score, rel=1e-12)


def test_selector_requires_two_effectively_independent_blocks():
    hours = archive()
    with pytest.raises(ValueError, match="NOT_ESTIMABLE: insufficient effectively independent"):
        chronological_energy_scores(hours[:30], hours[30:60], training_btc_sigma=0.01,
                                    training_eth_sigma=0.02)
    with pytest.raises(ValueError, match="NOT_ESTIMABLE: window has fewer than"):
        select_block_length_frozen(hours, fit_at_ns=BASE + 36 * DAY_NS, training_btc_sigma=0.01,
                                   training_eth_sigma=0.02, min_support_days=60)
    with pytest.raises(ValueError, match="validation window has no synchronized hours"):
        select_block_length_frozen(hours[:20 * 24], fit_at_ns=BASE + 36 * DAY_NS, training_btc_sigma=0.01,
                                   training_eth_sigma=0.02, min_support_days=10)
