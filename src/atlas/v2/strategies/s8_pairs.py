"""Research-only two-leg S8 basket forecast and frozen-beta replay contract."""

from __future__ import annotations

import math
import statistics
from collections.abc import Sequence
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any

from atlas.v2._serialization import json_value, sha256_json, sha256_ref
from atlas.v2.contracts import CandidateActionV2
from atlas.v2.instruments import InstrumentKeyV2

S8_PROFILE_ID = "S8_HOURLY_PAIRS_RESEARCH_V1"
S8_BASKET_VERSION = "RESEARCH_BASKET_FORECAST_V2_V1"
S8_SIMULATION_VERSION = "S8_BASKET_SIMULATION_V2_V1"
HOUR_NS = 3_600_000_000_000
FIT_HOURS = 720
ENTRY_ABS_Z = 2.0
CONVERGENCE_ABS_Z = 0.5
STOP_ABS_Z = 3.5
MAX_HOLD_NS = 4 * HOUR_NS
S8_PROFILE_BODY = {"profile_id": S8_PROFILE_ID, "basket_version": S8_BASKET_VERSION,
    "fit": "30D_SYNCHRONIZED_HOURLY_LOG_PRICE_OLS", "fit_hours": FIT_HOURS,
    "economic_pair_definition_required": True, "decision": "HOURLY", "entry_abs_z_gt": ENTRY_ABS_Z,
    "convergence_abs_z_lt": CONVERGENCE_ABS_Z, "stop_abs_z_gt": STOP_ABS_Z,
    "time_exit_ns": MAX_HOLD_NS, "beta": "FROZEN_AT_BASKET_DECISION",
    "both_leg_execution_required": True, "capital_authority": "ZERO", "single_action": False,
    "trade_plan_allowed": False}
S8_PROFILE_HASH = sha256_json(S8_PROFILE_BODY)


@dataclass(frozen=True)
class S8PairDefinitionV2:
    pair_id: str
    economic_pair_definition: str
    key_a: InstrumentKeyV2
    key_b: InstrumentKeyV2
    hedge_fit: str
    residual_definition: str
    version: str = "S8_PAIR_DEFINITION_V1"

    def __post_init__(self) -> None:
        if not self.pair_id or not self.economic_pair_definition or self.key_a == self.key_b:
            raise ValueError("S8 requires an explicit economic pair of two distinct instruments")
        if (self.key_a.venue != self.key_b.venue or self.key_a.product != self.key_b.product
            or self.key_a.environment != self.key_b.environment
            or self.key_a.settlement_asset != self.key_b.settlement_asset):
            raise ValueError("S8 initial research pairs require one venue/product semantics")
        if self.hedge_fit != "OLS_LOG_PRICE_A_ON_LOG_PRICE_B_30D_HOURLY_V1":
            raise ValueError("unsupported S8 hedge fit")
        if self.residual_definition != "LOG_A_MINUS_ALPHA_MINUS_BETA_LOG_B":
            raise ValueError("unsupported S8 residual definition")

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.version, "pair_id": self.pair_id,
            "economic_pair_definition": self.economic_pair_definition,
            "key_a": self.key_a.to_dict(), "key_b": self.key_b.to_dict(),
            "hedge_fit": self.hedge_fit, "residual_definition": self.residual_definition}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class S8HourlyPriceV2:
    instrument_key: InstrumentKeyV2
    hour_end_ns: int
    available_at_ns: int
    close: Decimal
    source_ref: str

    def __post_init__(self) -> None:
        if self.hour_end_ns < 0 or self.available_at_ns < self.hour_end_ns:
            raise ValueError("S8 hourly close availability must follow its bar end")
        if not self.close.is_finite() or self.close <= 0:
            raise ValueError("S8 hourly close must be positive finite Decimal")
        sha256_ref(self.source_ref, field="source_ref")


@dataclass(frozen=True)
class S8LegEvidenceV2:
    instrument_key: InstrumentKeyV2
    price_refs: tuple[str, ...]
    book_execution_refs: tuple[str, ...]
    fee_ref: str | None
    funding_refs: tuple[str, ...]
    partial_fill_assumptions: tuple[str, ...]
    sequential_delay_assumptions: tuple[str, ...]
    orphan_leg_risk_states: tuple[str, ...]
    available_at_ns: int

    def __post_init__(self) -> None:
        for name in ("price_refs", "book_execution_refs", "funding_refs"):
            refs = tuple(getattr(self, name))
            if tuple(sorted(set(refs))) != refs:
                raise ValueError(f"S8 {name} must be sorted/unique")
            for ref in refs:
                sha256_ref(ref, field=name)
        if self.fee_ref is not None:
            sha256_ref(self.fee_ref, field="fee_ref")
        if tuple(sorted(set(self.partial_fill_assumptions))) != self.partial_fill_assumptions:
            raise ValueError("S8 partial fill assumptions must be sorted/unique")
        if tuple(sorted(set(self.sequential_delay_assumptions))) != self.sequential_delay_assumptions:
            raise ValueError("S8 sequential delay assumptions must be sorted/unique")
        if tuple(sorted(set(self.orphan_leg_risk_states))) != self.orphan_leg_risk_states:
            raise ValueError("S8 orphan leg states must be sorted/unique")

    @property
    def complete_execution_evidence(self) -> bool:
        return bool(self.price_refs and self.book_execution_refs and self.fee_ref and self.funding_refs and self.partial_fill_assumptions
            and self.sequential_delay_assumptions and self.orphan_leg_risk_states)

    def to_dict(self) -> dict[str, Any]:
        return {"instrument_key": self.instrument_key.to_dict(), "price_refs": list(self.price_refs),
            "book_execution_refs": list(self.book_execution_refs), "fee_ref": self.fee_ref,
            "funding_refs": list(self.funding_refs), "partial_fill_assumptions": list(self.partial_fill_assumptions),
            "sequential_delay_assumptions": list(self.sequential_delay_assumptions),
            "orphan_leg_risk_states": list(self.orphan_leg_risk_states),
            "available_at_ns": self.available_at_ns}


@dataclass(frozen=True)
class ResearchBasketForecastV2:
    profile_id: str
    pair_definition_ref: str
    fit_start_ns: int
    fit_end_ns: int
    information_cutoff_ns: int
    synchronized_price_refs: tuple[tuple[str, str], ...]
    alpha: float
    beta: float
    residual_mean: float
    residual_std: float
    current_residual: float
    current_z: float
    beta_frozen: bool
    beta_frozen_at_ns: int
    decision_at_ns: int
    entry_threshold_abs_z: float
    convergence_threshold_abs_z: float
    stop_threshold_abs_z: float
    time_exit_ns: int
    leg_a_evidence: S8LegEvidenceV2
    leg_b_evidence: S8LegEvidenceV2
    economic_status: str
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        sha256_ref(self.pair_definition_ref, field="pair_definition_ref")
        if self.profile_id != S8_PROFILE_ID or not self.beta_frozen or self.beta_frozen_at_ns != self.decision_at_ns:
            raise ValueError("S8 beta/profile must freeze at the basket decision")
        if self.fit_end_ns != self.information_cutoff_ns or self.decision_at_ns != self.information_cutoff_ns:
            raise ValueError("S8 hourly fit and decision must bind the same cutoff")
        if self.fit_end_ns - self.fit_start_ns != FIT_HOURS * HOUR_NS:
            raise ValueError("S8 fit must span exactly thirty days")
        if len(self.synchronized_price_refs) != FIT_HOURS + 1:
            raise ValueError("S8 fit requires 721 synchronized hourly price pairs")
        if self.entry_threshold_abs_z != ENTRY_ABS_Z or self.convergence_threshold_abs_z != CONVERGENCE_ABS_Z:
            raise ValueError("S8 entry/convergence thresholds differ from frozen profile")
        if self.stop_threshold_abs_z != STOP_ABS_Z or self.time_exit_ns != MAX_HOLD_NS:
            raise ValueError("S8 stop/time-exit thresholds differ from frozen profile")
        if not self.residual_std > 0 or any(not math.isfinite(value) for value in (
            self.alpha, self.beta, self.residual_mean, self.residual_std, self.current_residual, self.current_z)):
            raise ValueError("S8 fit values must be finite with positive residual variance")

    def to_dict(self) -> dict[str, Any]:
        return {"version": S8_BASKET_VERSION, "profile_id": self.profile_id,
            "profile_hash": S8_PROFILE_HASH,
            "pair_definition_ref": self.pair_definition_ref, "fit_start_ns": self.fit_start_ns,
            "fit_end_ns": self.fit_end_ns, "information_cutoff_ns": self.information_cutoff_ns,
            "synchronized_price_refs": [list(row) for row in self.synchronized_price_refs],
            "alpha": self.alpha, "beta": self.beta, "residual_mean": self.residual_mean,
            "residual_std": self.residual_std, "current_residual": self.current_residual,
            "current_z": self.current_z, "beta_frozen": self.beta_frozen,
            "beta_frozen_at_ns": self.beta_frozen_at_ns, "decision_at_ns": self.decision_at_ns,
            "thresholds": {"entry_abs_z": self.entry_threshold_abs_z,
                "convergence_abs_z": self.convergence_threshold_abs_z, "stop_abs_z": self.stop_threshold_abs_z},
            "time_exit_ns": self.time_exit_ns, "leg_a_evidence": self.leg_a_evidence.to_dict(),
            "leg_b_evidence": self.leg_b_evidence.to_dict(), "economic_status": self.economic_status,
            "reasons": list(self.reasons), "capital_authority": "ZERO", "single_action": False,
            "trade_plan_allowed": False}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class S8BasketSimulationV2:
    forecast_ref: str
    beta_used: float
    beta_refit_during_path: bool
    path_refs: tuple[str, ...]
    entry_z: float | None
    exit_reason: str
    leg_a_fill_state: str
    leg_b_fill_state: str
    fee_refs: tuple[str, str]
    funding_refs: tuple[tuple[str, ...], tuple[str, ...]]
    sequential_delay_ns: tuple[int, int]
    orphan_leg_state: str
    status: str
    leg_fee_values: tuple[Decimal | None, Decimal | None] = (None, None)
    leg_funding_cashflows: tuple[Decimal | None, Decimal | None] = (None, None)
    leg_price_path_refs: tuple[tuple[str, ...], tuple[str, ...]] = ((), ())

    def __post_init__(self) -> None:
        sha256_ref(self.forecast_ref, field="forecast_ref")
        if self.beta_refit_during_path:
            raise ValueError("S8 simulated basket must keep decision-time beta frozen")
        if any(delay < 0 for delay in self.sequential_delay_ns) or not self.orphan_leg_state:
            raise ValueError("S8 sequential delay and orphan risk must be explicit")
        if any(value is not None and (not value.is_finite() or value < 0) for value in self.leg_fee_values):
            raise ValueError("S8 both-leg fee values must be finite and nonnegative")
        if len(self.fee_refs) != 2 or len(self.funding_refs) != 2 or len(self.sequential_delay_ns) != 2:
            raise ValueError("S8 simulation must retain both leg cost/execution records")
        for ref in (*self.path_refs, *self.fee_refs, *(ref for leg in self.funding_refs for ref in leg)):
            sha256_ref(ref, field="S8_simulation_ref")

    def to_dict(self) -> dict[str, Any]:
        body = json_value(dict(self.__dict__))
        return {"version": S8_SIMULATION_VERSION, **body,
            "single_action": False, "trade_plan_allowed": False, "capital_authority": "ZERO"}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def build_research_basket_forecast(pair: S8PairDefinitionV2, *,
        prices_a: Sequence[S8HourlyPriceV2], prices_b: Sequence[S8HourlyPriceV2],
        cutoff_ns: int, leg_a_evidence: S8LegEvidenceV2, leg_b_evidence: S8LegEvidenceV2) -> ResearchBasketForecastV2:
    """Fit the fixed 30-day synchronized hourly pair contract at a causal cutoff."""
    a = tuple(row for row in prices_a if row.instrument_key == pair.key_a and row.available_at_ns <= cutoff_ns)
    b = tuple(row for row in prices_b if row.instrument_key == pair.key_b and row.available_at_ns <= cutoff_ns)
    if leg_a_evidence.instrument_key != pair.key_a or leg_b_evidence.instrument_key != pair.key_b:
        raise ValueError("S8 both-leg evidence must match the economic pair")
    for rows in (a, b):
        seen: dict[tuple[int, int], Decimal] = {}
        for row in rows:
            stamp = (row.hour_end_ns, row.available_at_ns)
            if stamp in seen and seen[stamp] != row.close:
                raise ValueError("S8 synchronized price revision is contradictory at the same availability")
            seen[stamp] = row.close
    by_a = {row.hour_end_ns: row for row in sorted(a, key=lambda item: (item.available_at_ns, item.source_ref))}
    by_b = {row.hour_end_ns: row for row in sorted(b, key=lambda item: (item.available_at_ns, item.source_ref))}
    times = tuple(sorted(set(by_a) & set(by_b)))[-(FIT_HOURS + 1):]
    if len(times) != FIT_HOURS + 1 or times[-1] != cutoff_ns:
        raise ValueError("S8 requires a complete synchronized 30-day hourly prefix ending at cutoff")
    if any(times[i] - times[i - 1] != HOUR_NS for i in range(1, len(times))):
        raise ValueError("S8 synchronized pair prefix has a missing hourly bar")
    log_a = [math.log(float(by_a[t].close)) for t in times]
    log_b = [math.log(float(by_b[t].close)) for t in times]
    mean_a, mean_b = statistics.mean(log_a), statistics.mean(log_b)
    variance_b = statistics.pvariance(log_b)
    if variance_b <= 0:
        raise ValueError("S8 hedge fit has zero variance in leg B")
    beta = sum((x - mean_b) * (y - mean_a) for x, y in zip(log_b, log_a, strict=True)) / (len(times) * variance_b)
    alpha = mean_a - beta * mean_b
    residuals = [left - alpha - beta * right for left, right in zip(log_a, log_b, strict=True)]
    residual_mean = statistics.mean(residuals)
    residual_std = statistics.stdev(residuals)
    if not math.isfinite(residual_std) or residual_std <= 1e-12:
        raise ValueError("S8 residual dispersion is insufficient for a standardized basket forecast")
    current_residual = residuals[-1]
    current_z = (current_residual - residual_mean) / residual_std
    reasons: list[str] = []
    if not leg_a_evidence.complete_execution_evidence:
        reasons.append("LEG_A_BOTH_BOOK_COST_FUNDING_PARTIAL_DELAY_ORPHAN_EVIDENCE_INCOMPLETE")
    if not leg_b_evidence.complete_execution_evidence:
        reasons.append("LEG_B_BOTH_BOOK_COST_FUNDING_PARTIAL_DELAY_ORPHAN_EVIDENCE_INCOMPLETE")
    if leg_a_evidence.available_at_ns > cutoff_ns or leg_b_evidence.available_at_ns > cutoff_ns:
        reasons.append("BOTH_LEG_EXECUTION_EVIDENCE_NOT_AVAILABLE_AT_CUTOFF")
    reasons.append("BASKET_ECONOMICS_REQUIRES_MATURED_BOTH_LEG_EXECUTION_OUTCOMES")
    status = "NOT_ESTIMABLE"
    refs = tuple((by_a[t].source_ref, by_b[t].source_ref) for t in times)
    return ResearchBasketForecastV2(S8_PROFILE_ID, pair.content_hash, times[0], times[-1], cutoff_ns,
        refs, alpha, beta, residual_mean, residual_std, current_residual, current_z, True, cutoff_ns,
        cutoff_ns, ENTRY_ABS_Z, CONVERGENCE_ABS_Z, STOP_ABS_Z, MAX_HOLD_NS,
        leg_a_evidence, leg_b_evidence, status, tuple(sorted(reasons)))


def s8_entry_side(forecast: ResearchBasketForecastV2) -> str | None:
    if abs(forecast.current_z) <= ENTRY_ABS_Z:
        return None
    return "SHORT_SPREAD" if forecast.current_z > ENTRY_ABS_Z else "LONG_SPREAD"


def s8_exit_reason(forecast: ResearchBasketForecastV2, current_z: float, elapsed_ns: int) -> str | None:
    if abs(current_z) > STOP_ABS_Z:
        return "STOP_ABS_Z_GT_3_5"
    if abs(current_z) < CONVERGENCE_ABS_Z:
        return "CONVERGENCE_ABS_Z_LT_0_5"
    if elapsed_ns >= MAX_HOLD_NS:
        return "TIME_EXIT_FOUR_HOURS"
    return None


def simulate_s8_basket_path(forecast: ResearchBasketForecastV2, *, z_values: Sequence[float],
        path_refs: Sequence[str], leg_a_fill_state: str, leg_b_fill_state: str,
        fee_refs: tuple[str, str], funding_refs: tuple[tuple[str, ...], tuple[str, ...]],
        sequential_delay_ns: tuple[int, int], orphan_leg_state: str,
        leg_fee_values: tuple[Decimal | None, Decimal | None] = (None, None),
        leg_funding_cashflows: tuple[Decimal | None, Decimal | None] = (None, None)) -> S8BasketSimulationV2:
    """Replay a supplied two-leg research path with the decision-time beta frozen."""
    if len(z_values) != len(path_refs) or not z_values or any(not math.isfinite(value) for value in z_values):
        raise ValueError("S8 path requires one finite z value per retained path reference")
    if not s8_entry_side(forecast):
        raise ValueError("S8 path cannot open without the frozen entry residual threshold")
    exit_reason = "PATH_INCOMPLETE_NOT_ESTIMABLE"
    for index, value in enumerate(z_values):
        elapsed_ns = index * HOUR_NS
        reason = s8_exit_reason(forecast, value, elapsed_ns)
        if reason is not None:
            exit_reason = reason
            break
    states = {"NO_FILL", "PARTIAL_FILL", "FULL_FILL", "UNAVAILABLE"}
    if leg_a_fill_state not in states or leg_b_fill_state not in states:
        raise ValueError("S8 leg fill states must retain no-fill, partial, full or unavailable")
    return S8BasketSimulationV2(forecast.content_hash, forecast.beta, False, tuple(path_refs),
        forecast.current_z, exit_reason, leg_a_fill_state, leg_b_fill_state, fee_refs, funding_refs,
        sequential_delay_ns, orphan_leg_state, "NOT_ESTIMABLE", leg_fee_values, leg_funding_cashflows)


def simulate_s8_synchronized_prices(forecast: ResearchBasketForecastV2, *,
        prices_a: Sequence[S8HourlyPriceV2], prices_b: Sequence[S8HourlyPriceV2], **execution: Any) -> S8BasketSimulationV2:
    """Compute every replay residual with frozen alpha, beta and standardization."""
    if len(prices_a) != len(prices_b) or not prices_a or len(prices_a) > 5:
        raise ValueError("S8 replay requires synchronized hourly two-leg prices through at most four hours")
    z_values, refs = [], []
    for index, (a, b) in enumerate(zip(prices_a, prices_b, strict=True)):
        if (a.instrument_key != forecast.leg_a_evidence.instrument_key or
            b.instrument_key != forecast.leg_b_evidence.instrument_key or
            a.hour_end_ns != forecast.decision_at_ns + index * HOUR_NS or a.hour_end_ns != b.hour_end_ns):
            raise ValueError("S8 simulated hourly path must retain synchronized economic pair identity")
        residual = math.log(float(a.close)) - forecast.alpha - forecast.beta * math.log(float(b.close))
        z_values.append((residual - forecast.residual_mean) / forecast.residual_std)
        refs.append(sha256_json({"leg_a_price_ref": a.source_ref, "leg_b_price_ref": b.source_ref}))
    simulated = simulate_s8_basket_path(forecast, z_values=z_values, path_refs=refs, **execution)
    return replace(simulated, leg_price_path_refs=(tuple(row.source_ref for row in prices_a),
        tuple(row.source_ref for row in prices_b)))


def persist_s8_basket(repo: Any, pair: S8PairDefinitionV2, forecast: ResearchBasketForecastV2,
        *, available_at_ns: int) -> str:
    from atlas.v2.memory.repository import ArtifactIndexEntryV2

    if forecast.pair_definition_ref != pair.content_hash or available_at_ns < forecast.information_cutoff_ns:
        raise ValueError("S8 persistence must bind its economic pair and causal decision")
    pair_entry = repo.get_artifact(pair.content_hash)
    if pair_entry is None:
        repo.register_artifact(ArtifactIndexEntryV2(pair.content_hash, "S8PairDefinitionV2", pair.content_hash,
            forecast.information_cutoff_ns, forecast.information_cutoff_ns, {"pair": pair.to_dict()}))
    elif (pair_entry.artifact_type != "S8PairDefinitionV2" or pair_entry.content_hash != pair.content_hash
            or pair_entry.metadata.get("pair") != pair.to_dict()):
        raise ValueError("S8 registered pair identity conflicts with its exact definition")
    repo.register_artifact(ArtifactIndexEntryV2(forecast.content_hash, "ResearchBasketForecastV2", forecast.content_hash,
        forecast.information_cutoff_ns, available_at_ns, {"basket": forecast.to_dict()}))
    return forecast.content_hash


def reject_s8_single_action(value: Any) -> None:
    """Explicit boundary used by normal single-instrument selection/sizing seams."""
    if isinstance(value, CandidateActionV2):
        raise ValueError("S8 cannot emit a CandidateActionV2 from a multi-leg research basket")
    if isinstance(value, ResearchBasketForecastV2):
        raise TypeError("ResearchBasketForecastV2 is not a CandidateActionV2 and cannot be sized/planned")
    raise TypeError("unsupported single-action input")
