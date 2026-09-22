"""§3: frozen 4h/24h joint-return energy-score block selection."""

from __future__ import annotations

import pytest

from atlas.science.huber_mean import HOUR_NS
from atlas.science.residual_blocks import (
    JointResidualHour,
    chronological_energy_scores,
    eligible_starts,
    select_block_length,
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
