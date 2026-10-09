"""Bounded causal relative-value diagnostics; no strategy or capital authority.

Values are caller-defined levels (for example log prices), never implicitly
transformed. Every fit uses an explicitly supplied, equally spaced prefix.
ADF/Engle-Granger/Johansen statistics have no calibrated inference here.
Reference equations: statsmodels' official stattools and VECM source, cited in
docs/v2/SESSION041_MATH_HANDOFF.json. No statsmodels dependency is imported.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
from scipy.linalg import eigh

from atlas.v2._serialization import FrozenMap, sha256_json, sha256_ref

VERSION = "CAUSAL_RELATIVE_VALUE_DIAGNOSTICS_V1"
MAX_POINTS = 720
MAX_LAGS = 8


@dataclass(frozen=True)
class ScalarPoint:
    at_ns: int
    available_at_ns: int
    value: float
    source_ref: str


@dataclass(frozen=True)
class PairPoint:
    """Two levels at exactly the same observation timestamp."""

    at_ns: int
    available_a_ns: int
    available_b_ns: int
    value_a: float
    value_b: float
    source_ref_a: str
    source_ref_b: str


@dataclass(frozen=True)
class DiagnosticResult:
    method: str
    status: str
    reason: str | None
    cutoff_ns: int
    published_at_ns: int
    observation_count: int
    input_refs: tuple[str, ...]
    metrics: FrozenMap
    input_identity: str | None
    version: str = VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version, "method": self.method, "status": self.status,
            "reason": self.reason, "cutoff_ns": self.cutoff_ns,
            "published_at_ns": self.published_at_ns,
            "observation_count": self.observation_count,
            "input_refs": list(self.input_refs), "metrics": self.metrics.to_dict(),
            "input_identity": self.input_identity, "capital_authority": "ZERO",
            "p_value": None, "critical_values": None, "inference": "UNCALIBRATED",
        }


class _NotEstimable(Exception):
    pass


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise _NotEstimable(reason)


def _integer(value: Any) -> bool:
    return type(value) is int and 0 <= value <= np.iinfo(np.int64).max


def _finite(value: Any) -> bool:
    try:
        return type(value) in (int, float) and math.isfinite(value)
    except OverflowError:
        return False


def _validate(
    points: Sequence[ScalarPoint] | Sequence[PairPoint], *, paired: bool,
    cutoff_ns: int, published_at_ns: int,
) -> tuple[np.ndarray, tuple[str, ...], list[dict[str, Any]]]:
    _require(_integer(cutoff_ns) and _integer(published_at_ns), "INVALID_TIMESTAMPS")
    _require(published_at_ns >= cutoff_ns, "PUBLICATION_PRECEDES_CUTOFF")
    _require(0 < len(points) <= MAX_POINTS, "MISSING_OR_EXCESS_POINTS")
    rows: list[dict[str, Any]] = []
    refs: list[str] = []
    values: list[Any] = []
    timestamps: list[int] = []
    for point in points:
        _require(isinstance(point, PairPoint if paired else ScalarPoint), "INVALID_POINT_TYPE")
        _require(_integer(point.at_ns), "INVALID_TIMESTAMPS")
        availability: tuple[int, ...]
        observed: tuple[float, ...]
        source_refs: tuple[str, ...]
        if isinstance(point, PairPoint):
            availability = (point.available_a_ns, point.available_b_ns)
            observed = (point.value_a, point.value_b)
            source_refs = (point.source_ref_a, point.source_ref_b)
        else:
            availability = (point.available_at_ns,)
            observed = (point.value,)
            source_refs = (point.source_ref,)
        _require(all(_integer(at) for at in availability), "INVALID_TIMESTAMPS")
        _require(all(point.at_ns <= at <= cutoff_ns for at in availability), "NONCAUSAL_INPUT")
        _require(all(_finite(value) for value in observed), "NONFINITE_INPUT")
        try:
            for ref in source_refs:
                sha256_ref(ref, field="source_ref")
                _require(ref == ref.strip(), "INVALID_INPUT_REF")
        except ValueError as exc:
            raise _NotEstimable("INVALID_INPUT_REF") from exc
        timestamps.append(point.at_ns)
        refs.extend(source_refs)
        values.append(observed if paired else observed[0])
        rows.append({"at_ns": point.at_ns, "available_at_ns": availability,
                     "values": observed, "input_refs": source_refs})
    if len(timestamps) > 1:
        gaps = np.diff(np.asarray(timestamps, dtype=np.int64))
        _require(bool(np.all(gaps > 0)), "UNORDERED_OR_DUPLICATE_TIMESTAMPS")
        _require(bool(np.all(gaps == gaps[0])), "IRREGULAR_CADENCE")
    return np.asarray(values, dtype=float), tuple(refs), rows


def _run(
    method: str, points: Sequence[ScalarPoint] | Sequence[PairPoint], *, paired: bool,
    cutoff_ns: int, published_at_ns: int, profile: str, parameters: Mapping[str, Any],
    calculate: Callable[[np.ndarray], Mapping[str, Any]],
) -> DiagnosticResult:
    refs: tuple[str, ...] = ()
    identity = None
    try:
        values, refs, rows = _validate(points, paired=paired, cutoff_ns=cutoff_ns,
                                      published_at_ns=published_at_ns)
        _require(profile == VERSION, "UNSUPPORTED_PROFILE")
        try:
            identity = sha256_json({"version": VERSION, "method": method,
                                    "cutoff_ns": cutoff_ns, "rows": rows,
                                    "parameters": parameters})
        except ValueError as exc:
            raise _NotEstimable("INVALID_PARAMETERS") from exc
        with np.errstate(all="raise"):
            metrics = FrozenMap(calculate(values))
        return DiagnosticResult(method, "AVAILABLE", None, cutoff_ns, published_at_ns,
                                len(points), refs, metrics, identity)
    except (_NotEstimable, np.linalg.LinAlgError, FloatingPointError, ValueError) as exc:
        reason = str(exc) if isinstance(exc, _NotEstimable) else "SINGULAR_OR_NUMERIC_FAILURE"
        return DiagnosticResult(method, "NOT_ESTIMABLE", reason, cutoff_ns, published_at_ns,
                                len(points), refs, FrozenMap(), identity)


def _lags(lags: int) -> None:
    _require(type(lags) is int and 0 <= lags <= MAX_LAGS, "UNSUPPORTED_LAG_PROFILE")


def _ols(design: np.ndarray, target: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Full rank OLS and coefficient covariance with explicit residual df."""
    _require(len(target) > design.shape[1], "INSUFFICIENT_DEGREES_OF_FREEDOM")
    coef, _, rank, singular = np.linalg.lstsq(design, target, rcond=None)
    _require(rank == design.shape[1], "SINGULAR_DESIGN")
    _require(float(singular[-1] / singular[0]) > 1e-10, "ILL_CONDITIONED_DESIGN")
    residual = target - design @ coef
    _, s, vt = np.linalg.svd(design, full_matrices=False)
    gram_inverse = (vt.T / (s * s)) @ vt
    return coef, residual, gram_inverse


def _adf(values: np.ndarray, *, lags: int, deterministic: str) -> dict[str, Any]:
    """Delta y_t = rho*y_(t-1) + deterministic + lagged differences."""
    _lags(lags)
    _require(deterministic in ("NONE", "CONSTANT"), "UNSUPPORTED_DETERMINISTIC_PROFILE")
    _require(len(values) >= max(8, 2 * lags + 5), "INSUFFICIENT_POINTS")
    differences = np.diff(values)
    columns = [values[lags:-1]]
    if deterministic == "CONSTANT":
        columns.append(np.ones(len(differences) - lags))
    columns.extend(differences[lags - lag:-lag] for lag in range(1, lags + 1))
    design = np.column_stack(columns)
    target = differences[lags:]
    coef, residual, gram_inverse = _ols(design, target)
    variance = float(residual @ residual) / (len(target) - design.shape[1])
    _require(variance > np.finfo(float).eps * max(float(target @ target) / len(target),
                                               np.finfo(float).tiny), "ZERO_RESIDUAL_VARIANCE")
    stderr = math.sqrt(variance * float(gram_inverse[0, 0]))
    _require(stderr > 0, "ZERO_STANDARD_ERROR")
    return {"adf_statistic": float(coef[0]) / stderr, "level_coefficient": float(coef[0]),
            "level_standard_error": stderr, "lags": lags, "deterministic": deterministic,
            "regression_observations": len(target), "residual_df": len(target) - design.shape[1]}


def stationarity_diagnostic(
    points: Sequence[ScalarPoint], *, cutoff_ns: int, published_at_ns: int,
    lags: int = 0, deterministic: str = "CONSTANT", profile: str = VERSION,
) -> DiagnosticResult:
    """ADF t statistic only; AVAILABLE never means stationary."""
    return _run("ADF", points, paired=False, cutoff_ns=cutoff_ns,
                published_at_ns=published_at_ns, profile=profile,
                parameters={"lags": lags, "deterministic": deterministic},
                calculate=lambda values: _adf(values, lags=lags, deterministic=deterministic))


def _half_life(values: np.ndarray, cadence_ns: int) -> dict[str, Any]:
    _require(len(values) >= 8, "INSUFFICIENT_POINTS")
    coef, _, _ = _ols(np.column_stack((np.ones(len(values) - 1), values[:-1])), values[1:])
    phi = float(coef[1])
    # A round-off perturbation of a unit root must not imply finite decay.
    _require(1e-10 < phi < 1 - 1e-10, "UNSUPPORTED_AR1_DECAY")
    half_life = -math.log(2) / math.log(phi)
    return {"ar1_intercept": float(coef[0]), "ar1_phi": phi,
            "half_life_observations": half_life, "half_life_ns": half_life * cadence_ns,
            "cadence_ns": cadence_ns, "ou_interpretation": "HYPOTHESIS_ONLY"}


def residual_half_life(
    points: Sequence[ScalarPoint], *, cutoff_ns: int, published_at_ns: int,
    profile: str = VERSION,
) -> DiagnosticResult:
    """Discrete positive AR(1) decay; negative/zero/explosive phi unsupported."""
    return _run("RESIDUAL_AR1_HALF_LIFE", points, paired=False, cutoff_ns=cutoff_ns,
                published_at_ns=published_at_ns, profile=profile, parameters={},
                calculate=lambda values: _half_life(values, points[1].at_ns - points[0].at_ns
                                                    if len(points) > 1 else 0))


def engle_granger(
    points: Sequence[PairPoint], *, cutoff_ns: int, published_at_ns: int,
    residual_adf_lags: int = 0, profile: str = VERSION,
) -> DiagnosticResult:
    """A-on-B OLS with intercept, then residual ADF without deterministic terms.

    Residual ADF has an Engle-Granger distribution, not ordinary ADF inference.
    No I(1) assumption, significance or cointegration rank is inferred.
    """
    def calculate(values: np.ndarray) -> Mapping[str, Any]:
        _require(len(values) >= 8, "INSUFFICIENT_POINTS")
        coef, residual, _ = _ols(np.column_stack((np.ones(len(values)), values[:, 1])), values[:, 0])
        _require(bool(float(residual @ residual) > np.finfo(float).eps * float(values[:, 0] @ values[:, 0])),
                 "ZERO_RESIDUAL_VARIANCE")
        result = _adf(residual, lags=residual_adf_lags, deterministic="NONE")
        result.update({"alpha": float(coef[0]), "beta": float(coef[1]),
                       "residuals": tuple(float(value) for value in residual),
                       "residual_definition": "A_MINUS_ALPHA_MINUS_BETA_B",
                       "integration_order_assumption": "I1_UNVERIFIED",
                       "inference_distribution": "ENGLE_GRANGER_NOT_ORDINARY_ADF"})
        try:
            result["residual_half_life"] = _half_life(residual, points[1].at_ns - points[0].at_ns)
            result["half_life_status"] = "AVAILABLE"
        except _NotEstimable as exc:
            result["residual_half_life"] = None
            result["half_life_status"] = "NOT_ESTIMABLE"
            result["half_life_reason"] = str(exc)
        return result

    return _run("ENGLE_GRANGER", points, paired=True, cutoff_ns=cutoff_ns,
                published_at_ns=published_at_ns, profile=profile,
                parameters={"residual_adf_lags": residual_adf_lags}, calculate=calculate)


def kalman_hedge_ratio(
    points: Sequence[PairPoint], *, cutoff_ns: int, published_at_ns: int,
    initial_alpha: float = 0.0, initial_beta: float = 0.0, initial_variance: float = 1.0,
    process_variance: float = 1e-4, observation_variance: float = 1.0,
    profile: str = VERSION,
) -> DiagnosticResult:
    """Forward random-walk [alpha,beta] filter for A = alpha + beta*B + noise.

    Fixed variances are caller assumptions, never estimated from future data.
    Innovations use the predicted state; posterior rows only use their prefix.
    """
    parameters = {"initial_alpha": initial_alpha, "initial_beta": initial_beta,
                  "initial_variance": initial_variance, "process_variance": process_variance,
                  "observation_variance": observation_variance}

    def calculate(values: np.ndarray) -> Mapping[str, Any]:
        _require(all(_finite(value) for value in parameters.values()), "INVALID_PARAMETERS")
        _require(initial_variance > 0 and process_variance >= 0 and observation_variance > 0,
                 "INVALID_VARIANCE_PROFILE")
        _require(len(values) >= 3, "INSUFFICIENT_POINTS")
        _ols(np.column_stack((np.ones(len(values)), values[:, 1])), values[:, 0])
        state = np.array((initial_alpha, initial_beta), dtype=float)
        covariance = np.eye(2) * initial_variance
        trajectory = []
        for point, (a, b) in zip(points, values, strict=True):
            h = np.array((1.0, b))
            predicted = covariance + np.eye(2) * process_variance
            innovation = float(a - h @ state)
            innovation_variance = float(h @ predicted @ h + observation_variance)
            gain = predicted @ h / innovation_variance
            state = state + gain * innovation
            update = np.eye(2) - np.outer(gain, h)
            covariance = update @ predicted @ update.T + observation_variance * np.outer(gain, gain)
            covariance = (covariance + covariance.T) / 2
            trajectory.append({"at_ns": point.at_ns, "alpha": float(state[0]), "beta": float(state[1]),
                               "innovation": innovation, "innovation_variance": innovation_variance,
                               "covariance": tuple(tuple(float(x) for x in row) for row in covariance)})
        return {"alpha": float(state[0]), "beta": float(state[1]),
                "trajectory": tuple(trajectory), "filter": "FORWARD_ONLY",
                "variance_assumptions": parameters}

    return _run("KALMAN_HEDGE_RATIO", points, paired=True, cutoff_ns=cutoff_ns,
                published_at_ns=published_at_ns, profile=profile, parameters=parameters, calculate=calculate)


def johansen_vecm(
    points: Sequence[PairPoint], *, cutoff_ns: int, published_at_ns: int,
    assumed_rank: int = 1, difference_lags: int = 0, deterministic: str = "NONE",
    profile: str = VERSION,
) -> DiagnosticResult:
    """Two-leg rank-one reduced-rank ML VECM, without deterministic terms.

    Solve (S10 S00^-1 S01)b = lambda S11 b after partialling out
    lagged differences. Rank one is a caller assumption, not a tested rank.
    Unsupported ranks/deterministics fail closed; no automatic lag selection.
    """
    def calculate(values: np.ndarray) -> Mapping[str, Any]:
        _lags(difference_lags)
        _require(type(assumed_rank) is int and assumed_rank == 1, "UNSUPPORTED_RANK_PROFILE")
        _require(deterministic == "NONE", "UNSUPPORTED_DETERMINISTIC_PROFILE")
        _require(len(values) >= max(12, 5 * difference_lags + 8), "INSUFFICIENT_POINTS")
        differences = np.diff(values, axis=0)
        target = differences[difference_lags:]
        levels = values[difference_lags:-1]
        nuisance = np.column_stack([differences[difference_lags - lag:-lag]
                                   for lag in range(1, difference_lags + 1)]) if difference_lags else None
        r0, r1 = target, levels
        if nuisance is not None:
            _, r0, _ = _ols(nuisance, target)
            _, r1, _ = _ols(nuisance, levels)
        n = len(target)
        s00, s11, s01 = r0.T @ r0 / n, r1.T @ r1 / n, r0.T @ r1 / n
        for covariance in (s00, s11):
            spectrum = np.linalg.eigvalsh(covariance)
            _require(float(spectrum[0]) > 1e-10 * float(spectrum[-1]), "SINGULAR_COVARIANCE")
        kernel = s01.T @ np.linalg.solve(s00, s01)
        eigenvalues, eigenvectors = eigh((kernel + kernel.T) / 2, s11)
        eigenvalues, eigenvectors = eigenvalues[::-1], eigenvectors[:, ::-1]
        _require(bool(np.all(eigenvalues >= -1e-12) and np.all(eigenvalues < 1 - 1e-12)),
                 "DEGENERATE_CANONICAL_CORRELATIONS")
        eigenvalues = np.maximum(eigenvalues, 0)
        _require(float(eigenvalues[0] - eigenvalues[1]) > 1e-10, "UNIDENTIFIABLE_RANK_ONE_VECTOR")
        beta = eigenvectors[:, 0]
        _require(abs(float(beta[0])) > 1e-10 * float(np.linalg.norm(beta)), "UNSUPPORTED_BETA_NORMALIZATION")
        beta = beta / beta[0]
        alpha = s01 @ beta / float(beta @ s11 @ beta)
        gamma = np.empty((2, 0))
        if nuisance is not None:
            gamma_coef, _, _ = _ols(nuisance, target - np.outer(levels @ beta, alpha))
            gamma = gamma_coef.T
        fitted_error = target - np.outer(levels @ beta, alpha)
        if nuisance is not None:
            fitted_error -= nuisance @ gamma.T
        error_covariance = fitted_error.T @ fitted_error / n
        error_spectrum = np.linalg.eigvalsh(error_covariance)
        _require(float(error_spectrum[0]) > 1e-10 * float(error_spectrum[-1]), "SINGULAR_VECM_ERRORS")
        log_terms = -n * np.log1p(-eigenvalues)
        return {"eigenvalues": tuple(float(x) for x in eigenvalues),
                "trace_statistics": (float(np.sum(log_terms)), float(log_terms[1])),
                "max_eigen_statistics": tuple(float(x) for x in log_terms),
                "beta": tuple(float(x) for x in beta), "alpha": tuple(float(x) for x in alpha),
                "gamma": tuple(tuple(float(x) for x in row) for row in gamma),
                "error_covariance": tuple(tuple(float(x) for x in row) for row in error_covariance),
                "assumed_rank": 1, "rank_inferred": False, "difference_lags": difference_lags,
                "deterministic": deterministic, "regression_observations": n,
                "integration_order_assumption": "I1_UNVERIFIED"}

    return _run("JOHANSEN_VECM", points, paired=True, cutoff_ns=cutoff_ns,
                published_at_ns=published_at_ns, profile=profile,
                parameters={"assumed_rank": assumed_rank, "difference_lags": difference_lags,
                            "deterministic": deterministic}, calculate=calculate)
