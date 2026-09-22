"""Small chronological Huber/ridge mean model implemented without ML frameworks."""

from __future__ import annotations

import hashlib
import json
import math
import platform
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from statistics import fmean

HUBER_TRANSITION = 1.345
RIDGE_CANDIDATES = (0.01, 0.1, 1.0, 10.0)
SOLVER_TOLERANCE = 1e-10
HOUR_NS = 3_600_000_000_000
DAY_NS = 24 * HOUR_NS


@dataclass(frozen=True)
class MeanObservation:
    origin_at_ns: int
    instrument: str
    z: float
    sigma: float
    next_return: float | None

    def __post_init__(self) -> None:
        if self.instrument not in {"BTCUSDT", "ETHUSDT"}:
            raise ValueError("frozen V1 universe is BTCUSDT/ETHUSDT")
        if self.origin_at_ns < 0 or self.origin_at_ns % HOUR_NS:
            raise ValueError("origin must be an UTC hour")
        if not math.isfinite(self.z) or not math.isfinite(self.sigma) or self.sigma <= 0:
            raise ValueError("finite z and positive finite sigma required")
        if self.next_return is not None and not math.isfinite(self.next_return):
            raise ValueError("finite label required")

    @property
    def label_at_ns(self) -> int:
        return self.origin_at_ns + HOUR_NS

    @property
    def y(self) -> float:
        if self.next_return is None or self.sigma <= 0:
            raise ValueError("matured label and positive sigma required")
        return self.next_return / self.sigma


@dataclass(frozen=True)
class HuberRidgeModel:
    intercept: float
    beta_eth: float
    beta_z: float
    z_mean: float
    z_std: float
    ridge: float
    training_start_ns: int
    training_end_ns: int
    fit_at_ns: int
    validation_intervals: tuple[tuple[int, int], ...] = ()
    solver: str = "deterministic_irls"
    tolerance: float = SOLVER_TOLERANCE

    def forecast(self, instrument: str, z: float) -> float:
        scaled = 0.0 if self.z_std == 0 else (z - self.z_mean) / self.z_std
        return self.intercept + (self.beta_eth if instrument == "ETHUSDT" else 0.0) + self.beta_z * scaled

    def manifest(self) -> dict[str, object]:
        return {"solver": self.solver, "solver_tolerance": self.tolerance, "python": platform.python_version(),
                "ridge": self.ridge, "training_start_ns": self.training_start_ns, "training_end_ns": self.training_end_ns,
                "fit_at_ns": self.fit_at_ns, "validation_intervals": self.validation_intervals,
                "scaler_mean": self.z_mean, "scaler_std": self.z_std,
                "coefficients": [self.intercept, self.beta_eth, self.beta_z]}

    def manifest_hash(self) -> str:
        return hashlib.sha256(json.dumps(self.manifest(), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def huber_loss(residual: float, transition: float = HUBER_TRANSITION) -> float:
    absolute = abs(residual)
    return 0.5 * residual * residual if absolute <= transition else transition * (absolute - 0.5 * transition)


def huber_ridge_objective(model: HuberRidgeModel, rows: Iterable[MeanObservation]) -> float:
    """The exact frozen mean-loss plus (unpenalized-intercept) ridge objective."""
    values = [huber_loss(x.y - model.forecast(x.instrument, x.z)) for x in rows if x.next_return is not None]
    if not values:
        raise ValueError("matured observations required")
    return fmean(values) + model.ridge * (model.beta_eth**2 + model.beta_z**2) / 2


def _solve_3x3(a: list[list[float]], b: list[float]) -> list[float]:
    """Deterministic pivoted elimination for the tiny weighted ridge system."""
    m = [row[:] + [rhs] for row, rhs in zip(a, b, strict=True)]
    for col in range(3):
        pivot = max(range(col, 3), key=lambda i: (abs(m[i][col]), -i))
        if abs(m[pivot][col]) < 1e-15:
            raise ValueError("singular model design")
        m[col], m[pivot] = m[pivot], m[col]
        factor = m[col][col]
        m[col] = [x / factor for x in m[col]]
        for row in range(3):
            if row == col:
                continue
            factor = m[row][col]
            m[row] = [x - factor * y for x, y in zip(m[row], m[col], strict=True)]
    return [m[i][3] for i in range(3)]


def fit_huber_ridge(
    rows: Sequence[MeanObservation], ridge: float, *, fit_at_ns: int | None = None,
    validation_intervals: tuple[tuple[int, int], ...] = (),
) -> HuberRidgeModel:
    matured = sorted((x for x in rows if x.next_return is not None), key=lambda x: (x.origin_at_ns, x.instrument))
    if not matured:
        raise ValueError("matured observations required")
    if ridge not in RIDGE_CANDIDATES:
        raise ValueError("unfrozen ridge candidate")
    zs = [x.z for x in matured]
    z_mean = fmean(zs)
    z_std = math.sqrt(fmean([(z - z_mean) ** 2 for z in zs]))
    # Explicit deterministic zero-variance behaviour: retain intercept/ETH and fix beta_z=0.
    use_z = z_std > 1e-15
    features = [(1.0, 1.0 if x.instrument == "ETHUSDT" else 0.0, (x.z - z_mean) / z_std if use_z else 0.0) for x in matured]
    labels = [x.y for x in matured]
    beta = [fmean(labels), 0.0, 0.0]
    for _ in range(200):
        weights = []
        for x, y in zip(features, labels, strict=True):
            # Explicit sum keeps the exact frozen arithmetic order (0 + a0b0 + a1b1 + a2b2).
            residual = y - (x[0] * beta[0] + x[1] * beta[1] + x[2] * beta[2])
            weights.append(1.0 if abs(residual) <= HUBER_TRANSITION else HUBER_TRANSITION / abs(residual))
        a = [[0.0] * 3 for _ in range(3)]
        rhs = [0.0] * 3
        for x, y, w in zip(features, labels, weights, strict=True):
            # Same accumulation order as w * x[i] * x[j]; the weight product is hoisted.
            w0, w1, w2 = w * x[0], w * x[1], w * x[2]
            rhs[0] += w0 * y
            rhs[1] += w1 * y
            rhs[2] += w2 * y
            a[0][0] += w0 * x[0]
            a[0][1] += w0 * x[1]
            a[0][2] += w0 * x[2]
            a[1][0] += w1 * x[0]
            a[1][1] += w1 * x[1]
            a[1][2] += w1 * x[2]
            a[2][0] += w2 * x[0]
            a[2][1] += w2 * x[1]
            a[2][2] += w2 * x[2]
        # Normal equations above are sums; the specified *mean* loss means the
        # lambda ridge contribution is multiplied by the sample count.
        a[1][1] += len(matured) * ridge
        a[2][2] += len(matured) * ridge
        new_beta = _solve_3x3(a, rhs)
        if max(abs(a - b) for a, b in zip(new_beta, beta, strict=True)) <= SOLVER_TOLERANCE:
            beta = new_beta
            break
        beta = new_beta
    if not use_z:
        beta[2] = 0.0
    return HuberRidgeModel(
        intercept=beta[0], beta_eth=beta[1], beta_z=beta[2], z_mean=z_mean,
        z_std=z_std if use_z else 0.0, ridge=ridge, training_start_ns=matured[0].origin_at_ns,
        training_end_ns=matured[-1].label_at_ns,
        fit_at_ns=fit_at_ns if fit_at_ns is not None else matured[-1].label_at_ns,
        validation_intervals=validation_intervals,
    )


def mean_huber_loss(model: HuberRidgeModel, rows: Iterable[MeanObservation]) -> float:
    values = [huber_loss(x.y - model.forecast(x.instrument, x.z)) for x in rows if x.next_return is not None]
    if not values:
        raise ValueError("validation labels required")
    return fmean(values)


def is_monday_midnight(at_ns: int) -> bool:
    dt = datetime.fromtimestamp(at_ns / 1_000_000_000, UTC)
    return dt.weekday() == 0 and dt.hour == dt.minute == dt.second == dt.microsecond == 0


@dataclass(frozen=True)
class WeeklyFit:
    model: HuberRidgeModel
    selected_ridge: float
    validation_intervals: tuple[tuple[int, int], ...]
    fit_at_ns: int


def weekly_refit(rows: Sequence[MeanObservation], fit_at_ns: int) -> WeeklyFit:
    if not is_monday_midnight(fit_at_ns):
        raise ValueError("frozen refit is Monday 00:00 UTC")
    # labels must have matured at the fit instant.  Last 180 days, and 90 days minimum.
    eligible = [r for r in rows if r.next_return is not None and r.label_at_ns <= fit_at_ns and r.label_at_ns > fit_at_ns - 180 * DAY_NS]
    if not eligible or min(r.label_at_ns for r in eligible) > fit_at_ns - 90 * DAY_NS:
        raise ValueError("NOT_ESTIMABLE: fewer than 90 days matured labels")
    folds = tuple((fit_at_ns - (3 - n) * 7 * DAY_NS, fit_at_ns - (2 - n) * 7 * DAY_NS) for n in range(3))
    losses: dict[float, float] = {}
    for ridge in RIDGE_CANDIDATES:
        fold_losses = []
        for start, end in folds:
            train = [r for r in eligible if r.label_at_ns < start]
            valid = [r for r in eligible if start <= r.label_at_ns < end]
            if not train or min(r.label_at_ns for r in train) > start - 60 * DAY_NS or not valid:
                raise ValueError("NOT_ESTIMABLE: fold has fewer than 60 preceding days")
            fold_losses.append(mean_huber_loss(fit_huber_ridge(train, ridge, fit_at_ns=start), valid))
        losses[ridge] = fmean(fold_losses)
    best = min(losses.values())
    # Freeze tie resolution within 1e-8 to larger lambda.
    selected = max(ridge for ridge, loss in losses.items() if loss <= best + 1e-8)
    final = fit_huber_ridge(eligible, selected, fit_at_ns=fit_at_ns, validation_intervals=folds)
    return WeeklyFit(final, selected, folds, fit_at_ns)
