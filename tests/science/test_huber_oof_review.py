from datetime import UTC, datetime

import pytest

from atlas.science.huber_mean import (
    DAY_NS,
    HOUR_NS,
    RIDGE_CANDIDATES,
    MeanObservation,
    fit_huber_ridge,
    huber_ridge_objective,
    weekly_refit,
)
from atlas.science.oof import OOFArchive


def observations(days: int = 91, *, duplicate: int = 1) -> list[MeanObservation]:
    rows: list[MeanObservation] = []
    start = int(datetime(2025, 1, 1, tzinfo=UTC).timestamp() * 1_000_000_000)
    for hour in range(days * 24):
        for instrument, eth in (("BTCUSDT", 0), ("ETHUSDT", 1)):
            z = (hour % 11) - 5.0
            y = 0.2 + 0.4 * eth + 0.3 * z
            rows.extend(MeanObservation(start + hour * HOUR_NS, instrument, z, 1.0, y) for _ in range(duplicate))
    return rows


def test_mean_objective_penalty_is_invariant_to_duplicate_rows():
    rows = observations(days=2)
    once = fit_huber_ridge(rows, 1.0)
    duplicated = fit_huber_ridge(rows * 4, 1.0)
    assert duplicated.beta_eth == pytest.approx(once.beta_eth, abs=1e-8)
    assert duplicated.beta_z == pytest.approx(once.beta_z, abs=1e-8)
    assert huber_ridge_objective(once, rows) == pytest.approx(huber_ridge_objective(duplicated, rows), abs=1e-8)


def test_penalized_coefficients_change_across_frozen_lambdas_and_intercept_is_not_penalized():
    rows = observations(days=2)
    models = [fit_huber_ridge(rows, ridge) for ridge in RIDGE_CANDIDATES]
    assert len({round(x.beta_z, 8) for x in models}) == 4
    assert models[-1].beta_z < models[0].beta_z
    # Constant-label fixture: an unpenalized intercept retains the observed level.
    constant = [MeanObservation(i * HOUR_NS, "BTCUSDT", float(i), 1, 2.0) for i in range(10)]
    assert fit_huber_ridge(constant, 10).intercept == pytest.approx(2.0)


def test_zero_z_is_deterministic_and_unknown_instrument_fails():
    rows = [MeanObservation(i * HOUR_NS, "BTCUSDT", 0, 1, 1) for i in range(10)]
    assert fit_huber_ridge(rows, .1).beta_z == 0
    with pytest.raises(ValueError, match="universe"):
        MeanObservation(0, "DOGEUSDT", 0, 1, 0)


def test_weekly_refit_is_monday_and_uses_frozen_fold_contract():
    rows = observations()
    monday = int(datetime(2025, 4, 7, tzinfo=UTC).timestamp() * 1_000_000_000)
    result = weekly_refit(rows, monday)
    assert result.selected_ridge in RIDGE_CANDIDATES
    assert len(result.validation_intervals) == 3
    assert result.model.fit_at_ns == monday
    assert result.model.training_start_ns >= monday - 180 * DAY_NS
    with pytest.raises(ValueError, match="Monday"):
        weekly_refit(rows, monday + HOUR_NS)


def test_oof_rejects_future_fit_and_never_rewrites_or_matures_early():
    rows = observations(days=2)
    origin = rows[-1]
    model = fit_huber_ridge(rows[:-4], .1, fit_at_ns=origin.origin_at_ns)
    archive = OOFArchive()
    entry = archive.forecast(origin, model)
    with pytest.raises(ValueError, match="future"):
        archive.forecast(origin, fit_huber_ridge(rows, .1, fit_at_ns=origin.origin_at_ns + HOUR_NS))
    with pytest.raises(ValueError, match="not matured"):
        archive.mature(origin.instrument, origin.origin_at_ns, 0, entry.target_at_ns - 1)
    assert archive.mature(origin.instrument, origin.origin_at_ns, 0, entry.target_at_ns).residual is not None
