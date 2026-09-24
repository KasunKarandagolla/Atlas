"""Small online and origin-fitted mathematical primitives (version 1)."""

from __future__ import annotations

import hashlib
import math
import random
from dataclasses import dataclass
from statistics import median

from atlas.v2._serialization import sha256_json


def _finite(values: tuple[float, ...]) -> None:
    if any(not math.isfinite(x) for x in values):
        raise ValueError("observations must be finite")


def robust_slope(values: tuple[float, ...], *, lookback: int = 20) -> float | None:
    """Theil-Sen pairwise slope, units per observation; minimum three samples."""
    if lookback < 3:
        raise ValueError("lookback must be >= 3")
    window = values[-lookback:]
    _finite(window)
    if len(window) < 3:
        return None
    return float(median((window[j] - window[i]) / (j - i) for i in range(len(window)) for j in range(i + 1, len(window))))


@dataclass(frozen=True)
class KalmanState:
    estimate: float
    variance: float
    observation_count: int


def kalman_update(previous: KalmanState, observation: float, *, process_variance: float, observation_variance: float) -> KalmanState:
    """Scalar forward filter only. No smoother or future observations."""
    if not all(math.isfinite(x) for x in (previous.estimate, previous.variance, observation, process_variance, observation_variance)):
        raise ValueError("Kalman inputs must be finite")
    if min(previous.variance, process_variance) < 0 or observation_variance <= 0:
        raise ValueError("invalid Kalman variance")
    predicted = previous.variance + process_variance
    gain = predicted / (predicted + observation_variance)
    return KalmanState(previous.estimate + gain * (observation - previous.estimate), (1 - gain) * predicted, previous.observation_count + 1)


def realized_variance(returns: tuple[float, ...], *, minimum: int = 2) -> float | None:
    _finite(returns)
    return math.fsum(x * x for x in returns) if len(returns) >= minimum else None


def ewma_variance(returns: tuple[float, ...], *, decay: float = 0.94) -> float | None:
    _finite(returns)
    if not 0 < decay < 1:
        raise ValueError("decay must be in (0,1)")
    if not returns:
        return None
    state = returns[0] ** 2
    for value in returns[1:]:
        state = decay * state + (1 - decay) * value * value
    return state


def _solve(matrix: list[list[float]], vector: list[float]) -> tuple[float, ...] | None:
    size = len(vector)
    for col in range(size):
        pivot = max(range(col, size), key=lambda row: abs(matrix[row][col]))
        if abs(matrix[pivot][col]) < 1e-12:
            return None
        matrix[col], matrix[pivot] = matrix[pivot], matrix[col]
        vector[col], vector[pivot] = vector[pivot], vector[col]
        scale = matrix[col][col]
        for j in range(col, size):
            matrix[col][j] /= scale
        vector[col] /= scale
        for row in range(size):
            if row == col:
                continue
            factor = matrix[row][col]
            for j in range(col, size):
                matrix[row][j] -= factor * matrix[col][j]
            vector[row] -= factor * vector[col]
    return tuple(vector)


def har_forecast(history: tuple[float, ...], *, origin: int, minimum_rows: int = 5) -> float | None:
    """Fit daily/2-period/3-period HAR at origin from matured earlier targets.

    Small windows make the method hand-testable. Target at origin is excluded.
    Ordinary least squares is used only when the design has full rank.
    """
    if origin > len(history) or origin < 3:
        return None
    _finite(history[:origin])
    rows: list[tuple[float, float, float, float]] = []
    targets: list[float] = []
    for t in range(3, origin):
        rows.append((1.0, history[t - 1], math.fsum(history[t - 2:t]) / 2, math.fsum(history[t - 3:t]) / 3))
        targets.append(history[t])
    if len(rows) < max(4, minimum_rows):
        return None
    gram = [[math.fsum(row[i] * row[j] for row in rows) for j in range(4)] for i in range(4)]
    rhs = [math.fsum(row[i] * y for row, y in zip(rows, targets, strict=True)) for i in range(4)]
    coefficients = _solve(gram, rhs)
    if coefficients is None:
        return None
    predictors = (1.0, history[origin - 1], math.fsum(history[origin - 2:origin]) / 2, math.fsum(history[origin - 3:origin]) / 3)
    return max(0.0, math.fsum(a * b for a, b in zip(coefficients, predictors, strict=True)))


@dataclass(frozen=True)
class EmpiricalResiduals:
    values: tuple[float, ...]
    source_refs: tuple[str, ...]
    cutoff_ns: int
    status: str
    identity: str


def empirical_residuals(observations: tuple[tuple[str, int, float], ...], *, cutoff_ns: int, minimum: int = 3) -> EmpiricalResiduals:
    if cutoff_ns < 0 or minimum < 1:
        raise ValueError("invalid empirical residual cutoff or minimum")
    eligible = sorted(((ref, at, value) for ref, at, value in observations if at <= cutoff_ns), key=lambda row: (row[1], row[0]))
    if len({ref for ref, _, _ in eligible}) != len(eligible):
        raise ValueError("duplicate residual ref")
    _finite(tuple(value for _, _, value in eligible))
    refs = tuple(ref for ref, _, _ in eligible)
    values = tuple(value for _, _, value in eligible)
    status = "AVAILABLE" if len(values) >= minimum else "NOT_ESTIMABLE"
    identity = sha256_json({"version": "EMPIRICAL_RESIDUAL_V1", "cutoff_ns": cutoff_ns, "rows": eligible, "minimum": minimum})
    return EmpiricalResiduals(values, refs, cutoff_ns, status, identity)


@dataclass(frozen=True)
class CommonPaths:
    identity: str
    path_ids: tuple[str, ...]
    uniforms: tuple[float, ...]


def common_paths(*, seed: int, experiment_ref: str, decision_ref: str, count: int) -> CommonPaths:
    """Candidate-shared draws: no action ID participates in identity or seed."""
    if seed < 0 or count < 0 or not experiment_ref or not decision_ref:
        raise ValueError("invalid common-path identity")
    identity = sha256_json({"version": "COMMON_PATH_V1", "seed": seed, "experiment_ref": experiment_ref, "decision_ref": decision_ref})
    rng = random.Random(int(hashlib.sha256(identity.encode()).hexdigest(), 16))
    return CommonPaths(identity, tuple(sha256_json({"identity": identity, "index": i}) for i in range(count)), tuple(rng.random() for _ in range(count)))
