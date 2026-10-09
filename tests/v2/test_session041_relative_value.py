"""Hand-equation, chronology and identifiability checks for diagnostics only."""

from __future__ import annotations

import math
from dataclasses import replace

import numpy as np
import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.math.relative_value import (
    MAX_POINTS,
    VERSION,
    PairPoint,
    ScalarPoint,
    engle_granger,
    johansen_vecm,
    kalman_hedge_ratio,
    residual_half_life,
    stationarity_diagnostic,
)

CADENCE = 1_000_000_000


def scalar(values):
    return tuple(ScalarPoint((i + 1) * CADENCE, (i + 1) * CADENCE + 1, float(value),
                             sha256_json({"scalar": i})) for i, value in enumerate(values))


def pairs(a, b):
    return tuple(PairPoint((i + 1) * CADENCE, (i + 1) * CADENCE + 1,
                          (i + 1) * CADENCE + 2, float(x), float(y),
                          sha256_json({"a": i}), sha256_json({"b": i}))
                 for i, (x, y) in enumerate(zip(a, b, strict=True)))


def timing(points):
    cutoff = points[-1].at_ns + 100 if points else 100
    return {"cutoff_ns": cutoff, "published_at_ns": cutoff + 500}


def fixture_pair(count=40):
    # Fixed algebraic values, without stochastic calibration or holdout data.
    a = [0.25 * i + math.sin(i * 1.1) for i in range(count)]
    b = [0.15 * i + math.cos(i * 0.7) for i in range(count)]
    return pairs(a, b)


def manual_slope(x, y):
    xm, ym = sum(x) / len(x), sum(y) / len(y)
    xx = sum((a - xm) ** 2 for a in x)
    slope = sum((a - xm) * (b - ym) for a, b in zip(x, y, strict=True)) / xx
    intercept = ym - slope * xm
    return intercept, slope, xx


def test_adf_statistic_matches_hand_ols_with_intercept():
    values = [4, 3, 3.5, 2, 3, 2.8, 2.2, 2.4, 2, 2.1, 2.3, 2]
    observations = scalar(values)
    result = stationarity_diagnostic(observations, **timing(observations))
    x, y = values[:-1], [b - a for a, b in zip(values[:-1], values[1:], strict=True)]
    intercept, coefficient, xx = manual_slope(x, y)
    rss = sum((target - intercept - coefficient * level) ** 2
              for level, target in zip(x, y, strict=True))
    standard_error = math.sqrt(rss / (len(y) - 2) / xx)
    assert result.status == "AVAILABLE"
    assert result.metrics["level_coefficient"] == pytest.approx(coefficient)
    assert result.metrics["adf_statistic"] == pytest.approx(coefficient / standard_error)
    assert result.metrics["residual_df"] == 9
    wire = result.to_dict()
    assert wire["p_value"] is None and wire["critical_values"] is None
    assert wire["inference"] == "UNCALIBRATED"
    assert wire["capital_authority"] == "ZERO"
    assert "stationary" not in wire


def test_engle_granger_matches_hand_first_stage_and_residual_adf():
    b = list(range(16))
    a = [3 + 2 * x + math.sin(x * 1.7) for x in b]
    observations = pairs(a, b)
    result = engle_granger(observations, **timing(observations))
    alpha, beta, _ = manual_slope(b, a)
    residuals = [x - alpha - beta * y for x, y in zip(a, b, strict=True)]
    x = residuals[:-1]
    y = [next_value - previous for previous, next_value
         in zip(residuals[:-1], residuals[1:], strict=True)]
    xx = sum(value * value for value in x)
    rho = sum(previous * change for previous, change in zip(x, y, strict=True)) / xx
    rss = sum((change - rho * previous) ** 2 for previous, change in zip(x, y, strict=True))
    statistic = rho / math.sqrt(rss / (len(y) - 1) / xx)
    assert result.status == "AVAILABLE"
    assert result.metrics["alpha"] == pytest.approx(alpha)
    assert result.metrics["beta"] == pytest.approx(beta)
    assert result.metrics["residuals"] == pytest.approx(residuals)
    assert result.metrics["adf_statistic"] == pytest.approx(statistic)
    assert result.metrics["deterministic"] == "NONE"
    assert result.metrics["inference_distribution"] == "ENGLE_GRANGER_NOT_ORDINARY_ADF"
    assert result.to_dict()["p_value"] is None


def test_ar1_half_life_exact_discrete_hand_case():
    observations = scalar([2 + 8 * 0.5 ** i for i in range(12)])
    result = residual_half_life(observations, **timing(observations))
    assert result.status == "AVAILABLE"
    assert result.metrics["ar1_phi"] == pytest.approx(0.5)
    assert result.metrics["ar1_intercept"] == pytest.approx(1)
    assert result.metrics["half_life_observations"] == pytest.approx(1)
    assert result.metrics["half_life_ns"] == pytest.approx(CADENCE)
    assert result.metrics["ou_interpretation"] == "HYPOTHESIS_ONLY"


@pytest.mark.parametrize("phi", [-0.5, 0.0, 1.0, 1.2])
def test_ar1_unsupported_decay_has_no_half_life(phi):
    values = [1.0]
    for _ in range(11):
        values.append(0.3 + phi * values[-1])
    observations = scalar(values)
    result = residual_half_life(observations, **timing(observations))
    assert result.status == "NOT_ESTIMABLE"
    assert result.reason == "UNSUPPORTED_AR1_DECAY"
    assert not result.metrics


def test_kalman_first_update_matches_hand_and_prefix_cannot_change():
    observations = fixture_pair(16)
    observations = (replace(observations[0], value_a=2.0, value_b=3.0),) + observations[1:]
    kwargs = {"process_variance": 0.0, "observation_variance": 2.0}
    result = kalman_hedge_ratio(observations, **timing(observations), **kwargs)
    assert result.status == "AVAILABLE"
    first = result.metrics["trajectory"][0]
    a, b = observations[0].value_a, observations[0].value_b
    denom = 1 + b * b + 2
    assert first["innovation"] == a
    assert first["innovation_variance"] == denom
    assert first["alpha"] == pytest.approx(a / denom)
    assert first["beta"] == pytest.approx(b * a / denom)
    assert first["covariance"][0][0] == pytest.approx(1 - 1 / denom)
    prefix = observations[:8]
    short = kalman_hedge_ratio(prefix, **timing(prefix), **kwargs)
    assert short.metrics["trajectory"] == result.metrics["trajectory"][:8]
    altered = observations[:8] + tuple(replace(p, value_a=p.value_a + 100) for p in observations[8:])
    later = kalman_hedge_ratio(altered, **timing(altered), **kwargs)
    assert later.metrics["trajectory"][:8] == short.metrics["trajectory"]
    assert result.metrics["filter"] == "FORWARD_ONLY"


def _inverse2(matrix):
    a, b = matrix[0]
    c, d = matrix[1]
    determinant = a * d - b * c
    return [[d / determinant, -b / determinant], [-c / determinant, a / determinant]]


def _multiply2(left, right):
    return [[sum(left[i][k] * right[k][j] for k in range(2)) for j in range(2)] for i in range(2)]


def test_johansen_eigenproblem_matches_scalar_two_by_two_equations():
    observations = fixture_pair()
    levels = [[p.value_a, p.value_b] for p in observations[:-1]]
    differences = [[q.value_a - p.value_a, q.value_b - p.value_b]
                   for p, q in zip(observations[:-1], observations[1:], strict=True)]
    n = len(levels)

    def cross(x, y):
        return [[sum(a[i] * b[j] for a, b in zip(x, y, strict=True)) / n
                 for j in range(2)] for i in range(2)]

    s00, s11, s01 = cross(differences, differences), cross(levels, levels), cross(differences, levels)
    s10 = [[s01[j][i] for j in range(2)] for i in range(2)]
    kernel = _multiply2(_multiply2(s10, _inverse2(s00)), s01)
    system = _multiply2(_inverse2(s11), kernel)
    trace = system[0][0] + system[1][1]
    determinant = system[0][0] * system[1][1] - system[0][1] * system[1][0]
    root = math.sqrt(trace * trace - 4 * determinant)
    expected = ((trace + root) / 2, (trace - root) / 2)
    result = johansen_vecm(observations, **timing(observations))
    assert result.status == "AVAILABLE"
    assert result.metrics["eigenvalues"] == pytest.approx(expected)
    assert result.metrics["trace_statistics"] == pytest.approx(
        (-n * sum(math.log1p(-x) for x in expected), -n * math.log1p(-expected[1])))
    assert result.metrics["max_eigen_statistics"] == pytest.approx(
        tuple(-n * math.log1p(-x) for x in expected))
    beta = result.metrics["beta"]
    alpha = result.metrics["alpha"]
    assert beta[0] == 1
    for i in range(2):
        left = sum(kernel[i][j] * beta[j] for j in range(2))
        right = expected[0] * sum(s11[i][j] * beta[j] for j in range(2))
        assert left == pytest.approx(right, abs=1e-10)
    spread = [sum(level[j] * beta[j] for j in range(2)) for level in levels]
    expected_alpha = [sum(change[j] * s for change, s in zip(differences, spread, strict=True))
                      / sum(s * s for s in spread) for j in range(2)]
    assert alpha == pytest.approx(expected_alpha)
    assert result.metrics["rank_inferred"] is False
    assert result.to_dict()["p_value"] is None


def test_vecm_lagged_difference_nuisance_fit_satisfies_normal_equations():
    observations = tuple(replace(p, value_a=p.value_a + 0.1 * math.sin(i * 2.37),
                                 value_b=p.value_b + 0.13 * math.cos(i * 2.19))
                         for i, p in enumerate(fixture_pair(60)))
    result = johansen_vecm(observations, difference_lags=1, **timing(observations))
    assert result.status == "AVAILABLE"
    values = np.array([[p.value_a, p.value_b] for p in observations])
    differences = np.diff(values, axis=0)
    target, nuisance, levels = differences[1:], differences[:-1], values[1:-1]
    beta = np.array(result.metrics["beta"])
    alpha = np.array(result.metrics["alpha"])
    gamma = np.array(result.metrics["gamma"])
    errors = target - np.outer(levels @ beta, alpha) - nuisance @ gamma.T
    assert nuisance.T @ errors == pytest.approx(np.zeros((2, 2)), abs=1e-10)
    assert result.metrics["regression_observations"] == 58


@pytest.mark.parametrize("function", [engle_granger, kalman_hedge_ratio, johansen_vecm])
def test_constant_or_exact_collinear_pair_is_not_estimable(function):
    constant = pairs([1.0] * 20, [2.0] * 20)
    assert function(constant, **timing(constant)).status == "NOT_ESTIMABLE"
    if function is not kalman_hedge_ratio:
        exact = pairs(range(20), [2 * x for x in range(20)])
        assert function(exact, **timing(exact)).status == "NOT_ESTIMABLE"


@pytest.mark.parametrize("function", [stationarity_diagnostic, residual_half_life])
def test_constant_and_missing_scalar_is_not_estimable(function):
    observations = scalar([1.0] * 20)
    assert function(observations, **timing(observations)).status == "NOT_ESTIMABLE"
    for missing in ((), observations[:1]):
        assert function(missing, **timing(missing)).status == "NOT_ESTIMABLE"


@pytest.mark.parametrize("function", [engle_granger, kalman_hedge_ratio, johansen_vecm])
@pytest.mark.parametrize("field", ["available_a_ns", "available_b_ns"])
def test_each_leg_actual_availability_must_precede_cutoff(function, field):
    observations = fixture_pair()
    times = timing(observations)
    altered = observations[:-1] + (replace(observations[-1], **{field: times["cutoff_ns"] + 1}),)
    result = function(altered, **times)
    assert result.status == "NOT_ESTIMABLE"
    assert result.reason == "NONCAUSAL_INPUT"


@pytest.mark.parametrize("mutation,reason", [
    (lambda p: replace(p, available_a_ns=p.at_ns - 1), "NONCAUSAL_INPUT"),
    (lambda p: replace(p, value_a=float("nan")), "NONFINITE_INPUT"),
    (lambda p: replace(p, source_ref_a="missing"), "INVALID_INPUT_REF"),
    (lambda p: replace(p, at_ns=p.at_ns + 10), "NONCAUSAL_INPUT"),
])
def test_invalid_points_fail_closed(mutation, reason):
    observations = fixture_pair()
    altered = observations[:-1] + (mutation(observations[-1]),)
    result = engle_granger(altered, **timing(observations))
    assert result.status == "NOT_ESTIMABLE"
    assert result.reason == reason
    assert not result.metrics


def test_scalar_future_availability_publication_and_cadence_fail_closed():
    observations = scalar([2 + math.sin(i) for i in range(20)])
    times = timing(observations)
    future = observations[:-1] + (replace(observations[-1], available_at_ns=times["cutoff_ns"] + 1),)
    assert stationarity_diagnostic(future, **times).reason == "NONCAUSAL_INPUT"
    assert stationarity_diagnostic(observations, cutoff_ns=times["cutoff_ns"],
                                   published_at_ns=times["cutoff_ns"] - 1).reason == "PUBLICATION_PRECEDES_CUTOFF"
    duplicate = observations[:1] + (replace(observations[1], at_ns=observations[0].at_ns),) + observations[2:]
    assert stationarity_diagnostic(duplicate, **times).reason == "UNORDERED_OR_DUPLICATE_TIMESTAMPS"
    irregular = observations[:1] + (replace(observations[1], at_ns=observations[1].at_ns - 1),) + observations[2:]
    assert stationarity_diagnostic(irregular, **times).reason == "IRREGULAR_CADENCE"


@pytest.mark.parametrize("function,options", [
    (engle_granger, {"residual_adf_lags": 9}),
    (kalman_hedge_ratio, {"observation_variance": 0}),
    (johansen_vecm, {"assumed_rank": 0}),
    (johansen_vecm, {"assumed_rank": 2}),
    (johansen_vecm, {"difference_lags": -1}),
    (johansen_vecm, {"deterministic": "CONSTANT"}),
])
def test_unsupported_parameters_fail_closed(function, options):
    observations = fixture_pair()
    assert function(observations, **timing(observations), **options).status == "NOT_ESTIMABLE"


def test_bound_is_explicit_and_profile_cannot_be_changed():
    observations = fixture_pair(MAX_POINTS)
    result = engle_granger(observations, **timing(observations))
    assert result.status == "AVAILABLE"
    assert result.observation_count == MAX_POINTS
    excessive = fixture_pair(MAX_POINTS + 1)
    assert engle_granger(excessive, **timing(excessive)).reason == "MISSING_OR_EXCESS_POINTS"
    assert engle_granger(observations, **timing(observations), profile="UNKNOWN").reason == "UNSUPPORTED_PROFILE"
    assert result.version == VERSION


def test_provenance_identity_is_causal_input_bound_and_publication_is_explicit():
    observations = fixture_pair()
    times = timing(observations)
    result = engle_granger(observations, **times)
    assert result.input_refs == tuple(ref for p in observations for ref in (p.source_ref_a, p.source_ref_b))
    assert result.to_dict()["published_at_ns"] == times["published_at_ns"]
    assert result.to_dict()["cutoff_ns"] == times["cutoff_ns"]
    assert result.input_identity == engle_granger(observations, **times).input_identity
    changed = observations[:-1] + (replace(observations[-1], source_ref_b=sha256_json({"new": "ref"})),)
    assert result.input_identity != engle_granger(changed, **times).input_identity
    assert result.input_identity != engle_granger(observations, residual_adf_lags=1, **times).input_identity
    with pytest.raises(TypeError):
        result.metrics["beta"] = 5


def test_existing_frozen_s8_profile_hash_is_preserved():
    from atlas.v2.strategies.s8_pairs import S8_PROFILE_HASH

    assert S8_PROFILE_HASH == "c118b1e3f6732cf8897efc4c08a2d21eef50cec63b78d5bb40787f9fa5b0f7fe"
