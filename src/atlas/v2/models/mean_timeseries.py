"""Frozen time-major research ABI for the three selected pretrained models.

Numerical meaning only. P3 supplies loader/adapter glue, P4 real inference.
No checkpoint is downloaded or promoted by these contracts.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal
from typing import ClassVar

from atlas.v2.science.mean_common import OfflineArtifact, positive, unique

CHECKPOINTS = ("NX-AI/TiRex-2", "autogluon/chronos-2-small", "NeoQuasar/Kronos-mini")


@dataclass(frozen=True)
class NumericalModelManifestV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_RESEARCH_NUMERICAL_MODEL_MANIFEST_V1"
    checkpoint: str
    checkpoint_revision: str
    weight_hashes: tuple[str, ...]
    tokenizer: str | None
    tokenizer_revision: str | None
    tokenizer_hashes: tuple[str, ...]
    runtime_lock_ref: str
    preprocessing_ref: str
    postprocessing_ref: str
    source_ref: str
    context_limit: int
    horizon_limit: int
    native_quantiles: tuple[Decimal, ...]
    input_channels: tuple[str, ...]
    input_units: tuple[str, ...]
    target_transform: str
    quantile_mode: str
    training_cutoff_ns: int | None
    contamination_class: str
    sampling_seed: int
    sampling_paths: int

    def __post_init__(self) -> None:
        super().__post_init__()
        if (
            self.checkpoint not in CHECKPOINTS
            or not self.checkpoint_revision
            or not self.weight_hashes
            or not 1 <= self.context_limit <= 4096
            or not 1 <= self.horizon_limit <= 512
            or len(self.input_channels) != len(self.input_units)
            or not self.input_channels
        ):
            raise ValueError("exact selected model/time-series manifest required")
        unique(self.input_channels)
        unique(self.weight_hashes)
        if tuple(sorted(set(self.native_quantiles))) != self.native_quantiles or any(
            not 0 < q < 1 for q in self.native_quantiles
        ):
            raise ValueError("native quantiles must be ordered probabilities")
        if self.contamination_class not in ("UNKNOWN", "POTENTIAL_OVERLAP", "AUDITABLE_CUTOFF"):
            raise ValueError("pretraining evidence must remain explicit")
        if self.contamination_class == "AUDITABLE_CUTOFF" and self.training_cutoff_ns is None:
            raise ValueError("auditable cutoff needs actual evidence")
        if self.checkpoint == "NeoQuasar/Kronos-mini":
            if (
                self.tokenizer != "NeoQuasar/Kronos-Tokenizer-2k"
                or not self.tokenizer_revision
                or not self.tokenizer_hashes
                or self.input_channels != ("open", "high", "low", "close", "volume", "amount")
                or self.input_units != ("USDT/base", "USDT/base", "USDT/base", "USDT/base", "base", "USDT")
                or self.target_transform != "RAW_OHLCV_AMOUNT"
                or self.quantile_mode != "EMPIRICAL_TRAJECTORY_QUANTILES"
                or not 1 <= self.sampling_paths <= 256
            ):
                raise ValueError("Kronos-mini requires correct 2k tokenizer, six raw channels and pinned sampling")
        elif (
            self.target_transform != "LOG_CLOSE_USDT_PER_BASE"
            or self.input_channels[0] != "log_close"
            or self.input_units[0] != "log(USDT/base)"
            or self.quantile_mode != "NATIVE_ONLY"
        ):
            raise ValueError("general forecaster target transform/native output invalid")


@dataclass(frozen=True)
class TimeMajorResearchInputV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_TIME_MAJOR_RESEARCH_INPUT_V1"
    manifest: NumericalModelManifestV1
    instrument_ref: str
    source_ref: str
    origin_ns: int
    cadence_ns: int
    close_times_ns: tuple[int, ...]
    available_times_ns: tuple[int, ...]
    values: tuple[tuple[Decimal | None, ...], ...]
    missing_mask: tuple[tuple[bool, ...], ...]
    past_covariate_names: tuple[str, ...]
    past_covariates: tuple[tuple[Decimal | None, ...], ...]
    future_covariate_names: tuple[str, ...]
    future_covariates: tuple[tuple[Decimal, ...], ...]
    future_known_at_ns: tuple[int, ...]
    horizon_steps: int
    requested_quantiles: tuple[Decimal, ...]
    past_covariate_units: tuple[str, ...] = ()
    future_covariate_units: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        super().__post_init__()
        n, width = len(self.values), len(self.manifest.input_channels)
        if (
            not 1 <= n <= self.manifest.context_limit
            or self.cadence_ns <= 0
            or not 1 <= self.horizon_steps <= self.manifest.horizon_limit
            or len(self.close_times_ns) != n
            or len(self.available_times_ns) != n
            or len(self.missing_mask) != n
            or any(len(row) != width for row in self.values)
            or any(len(row) != width for row in self.missing_mask)
        ):
            raise ValueError("time-major T x C shape/horizon invalid")
        if any(
            a + self.cadence_ns != b for a, b in zip(self.close_times_ns, self.close_times_ns[1:], strict=False)
        ) or any(
            t > a or a > self.origin_ns for t, a in zip(self.close_times_ns, self.available_times_ns, strict=True)
        ):
            raise ValueError("future/gapped time-major prefix")
        if tuple(sorted(set(self.requested_quantiles))) != self.requested_quantiles or not set(
            self.requested_quantiles
        ) <= set(self.manifest.native_quantiles):
            raise ValueError("unsupported quantile cannot be fabricated or interpolated")
        for row, mask in zip(self.values, self.missing_mask, strict=True):
            if any((v is None) != m for v, m in zip(row, mask, strict=True)):
                raise ValueError("missing-mask semantics disagree")
            if self.manifest.checkpoint == "NeoQuasar/Kronos-mini":
                if any(v is None for v in row):
                    raise ValueError("Kronos input cannot silently impute missing OHLCV")
                o, h, low, c, v, a = row
                assert (
                    o is not None
                    and h is not None
                    and low is not None
                    and c is not None
                    and v is not None
                    and a is not None
                )
                if not 0 < low <= min(o, c) <= max(o, c) <= h or min(v, a) < 0:
                    raise ValueError("Kronos OHLCV/amount units or invariants invalid")
        if len(self.past_covariates) != n or any(
            len(r) != len(self.past_covariate_names) for r in self.past_covariates
        ):
            raise ValueError("past covariate T x K shape invalid")
        if len(self.past_covariate_units) != len(self.past_covariate_names) or any(
            not unit for unit in self.past_covariate_units
        ):
            raise ValueError("past covariate units must be explicit")
        future_registry = {
            "calendar_hour": ("UTC_hour_0_23", Decimal(0), Decimal(23)),
            "calendar_weekday": ("UTC_weekday_0_6", Decimal(0), Decimal(6)),
            "calendar_is_weekend": ("boolean_0_1", Decimal(0), Decimal(1)),
            "time_to_funding_seconds": ("seconds", Decimal(0), Decimal(604800)),
        }
        if len(self.future_covariate_units) != len(self.future_covariate_names):
            raise ValueError("future covariate units must be explicit")
        for i, name in enumerate(self.future_covariate_names):
            if name not in future_registry or self.future_covariate_units[i] != future_registry[name][0]:
                raise ValueError("unregistered future covariate numerical meaning")
            for row in self.future_covariates:
                if (
                    i >= len(row)
                    or not future_registry[name][1] <= row[i] <= future_registry[name][2]
                    or row[i] != row[i].to_integral_value()
                ):
                    raise ValueError("future covariate value/units invalid")
        if self.future_covariate_names:
            if (
                len(self.future_covariates) != self.horizon_steps
                or len(self.future_known_at_ns) != self.horizon_steps
                or any(len(r) != len(self.future_covariate_names) for r in self.future_covariates)
                or any(t > self.origin_ns for t in self.future_known_at_ns)
                or any(
                    not name.startswith(("calendar_", "scheduled_", "time_", "holiday_"))
                    for name in self.future_covariate_names
                )
            ):
                raise ValueError("future-realized covariate or H x K shape invalid")
        elif self.future_covariates or self.future_known_at_ns:
            raise ValueError("unlabelled future covariates")
        if self.manifest.checkpoint == "NeoQuasar/Kronos-mini" and (
            self.past_covariate_names or self.future_covariate_names
        ):
            raise ValueError("Kronos raw financial input has no generic covariate slot")

    @property
    def horizon_end_ns(self) -> int:
        return self.origin_ns + self.horizon_steps * self.cadence_ns


def log_price_target(price: Decimal) -> Decimal:
    positive(price)
    return Decimal(str(math.log(float(price))))


def forecast_log_return(log_price: Decimal, *, origin_price: Decimal) -> Decimal:
    positive(origin_price)
    if not log_price.is_finite():
        raise ValueError("nonfinite log-price forecast")
    return log_price - log_price_target(origin_price)


@dataclass(frozen=True)
class TimeMajorResearchOutputV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_TIME_MAJOR_RESEARCH_OUTPUT_V1"
    request: TimeMajorResearchInputV1
    started_ns: int
    completed_ns: int
    deadline_ns: int
    quantiles: tuple[Decimal, ...]
    values: tuple[tuple[Decimal, ...], ...]
    output_unit: str
    provenance: str

    def __post_init__(self) -> None:
        super().__post_init__()
        if (
            not self.request.origin_ns <= self.started_ns <= self.completed_ns < self.deadline_ns
            or self.quantiles != self.request.requested_quantiles
            or len(self.values) != self.request.horizon_steps
            or any(len(row) != len(self.quantiles) or tuple(sorted(row)) != row for row in self.values)
        ):
            raise ValueError("late, crossing, unsupported or malformed forecast quantiles")
        expected = "USDT/base" if self.request.manifest.target_transform == "RAW_OHLCV_AMOUNT" else "log(USDT/base)"
        if self.output_unit != expected or self.provenance not in ("ENGINEERING_FIXTURE", "ACTUAL_CHECKPOINT_RESEARCH"):
            raise ValueError("forecast units/evidence grade mismatch")
        if expected == "USDT/base" and any(v <= 0 for row in self.values for v in row):
            raise ValueError("nonpositive raw price forecast")


def kronos_close_quantiles(
    paths: tuple[tuple[tuple[Decimal, ...], ...], ...], *, horizon_steps: int, quantiles: tuple[Decimal, ...]
) -> tuple[tuple[Decimal, ...], ...]:
    """Time-major close-price order statistics from coherent raw trajectories.

    Paths are sample x horizon x [open,high,low,close,volume,amount]. Use the
    ceil(q*N) order statistic; never invent a path by joining marginal quantiles.
    These are predictive quantiles, not a cash-mean confidence bound.
    """
    if (
        not 1 <= len(paths) <= 256
        or horizon_steps < 1
        or not quantiles
        or tuple(sorted(set(quantiles))) != quantiles
        or any(not 0 < q < 1 for q in quantiles)
    ):
        raise ValueError("bounded Kronos trajectory quantile contract required")
    for path in paths:
        if len(path) != horizon_steps:
            raise ValueError("Kronos sample x horizon shape invalid")
        for row in path:
            if len(row) != 6 or any(not isinstance(v, Decimal) or not v.is_finite() for v in row):
                raise ValueError("six finite raw OHLCV/amount channels required")
            o, high, low, c, volume, amount = row
            if (
                not 0 < low <= min(o, c) <= max(o, c) <= high
                or min(volume, amount) < 0
                or not low * volume <= amount <= high * volume
            ):
                raise ValueError("incoherent Kronos OHLCV/amount trajectory")
    return tuple(
        tuple(sorted(path[t][3] for path in paths)[math.ceil(q * len(paths)) - 1] for q in quantiles)
        for t in range(horizon_steps)
    )
