"""One bounded read/calculation registry for all eight offline tool families.

P4 connects source readers and due-work execution. No arbitrary code, SQL, URL,
filesystem path, protected holdout, training or provider dispatch is accepted.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, ClassVar, cast

from atlas.v2._serialization import FrozenMap, canonical_json, sha256_json
from atlas.v2.research.mean_evidence import NumericalFactV1
from atlas.v2.science.mean_common import OfflineArtifact, nonnegative, unique

TOOL_FAMILIES = {
    "inspect_market": ("CONFIRMED_STRUCTURE",),
    "assess_execution": ("COLLAR_DEPTH_COST",),
    "inspect_relationships": ("BTC_ETH_PEER_CONTEXT",),
    "retrieve_analogues": ("COMPATIBLE_ANALOGUES",),
    "inspect_economics": ("EXACT_ECONOMICS_READ", "M0_MEAN_PREDICT", "M1_MEAN_PREDICT"),
    "verify_event": ("EVENT_VERIFY",),
    "compare_management": ("MANAGEMENT_COMPARE",),
    "request_registered_work": (
        "HAR_VOL_FORECAST",
        "FORWARD_KALMAN_STATE",
        "AR1_HALF_LIFE",
        "FOUNDATION_FORECAST",
        "CHART_SNAPSHOT",
        "CHEAP_EVENT_RESCREEN",
    ),
}


@dataclass(frozen=True)
class ResearchWorkBudgetV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_RESEARCH_WORK_BUDGET_V1"
    maximum_rows: int = 512
    maximum_bytes: int = 65_536
    cpu_budget_ns: int = 20_000_000
    wall_budget_ns: int = 50_000_000
    maximum_tool_calls: int = 4
    maximum_model_calls: int = 3
    supplementary_rounds: int = 1
    paid_budget_cash: Decimal = Decimal(0)

    def __post_init__(self) -> None:
        super().__post_init__()
        nonnegative(self.paid_budget_cash)
        if (
            not 0 < self.maximum_rows <= 512
            or not 0 < self.maximum_bytes <= 65_536
            or self.cpu_budget_ns <= 0
            or self.wall_budget_ns <= 0
            or not 0 < self.maximum_tool_calls <= 4
            or not 0 <= self.maximum_model_calls <= 3
            or not 0 <= self.supplementary_rounds <= 1
        ):
            raise ValueError("bounded analysis budgets required")


@dataclass(frozen=True)
class RegisteredToolRequestV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_REGISTERED_TOOL_REQUEST_V1"
    job_ref: str
    parent_ref: str
    snapshot_ref: str
    population_ref: str
    action_ref: str | None
    family: str
    operation: str
    input_refs: tuple[str, ...]
    instrument_refs: tuple[str, ...]
    raw_cutoff_ns: int
    input_cutoff_ns: int
    deadline_ns: int
    budget: ResearchWorkBudgetV1
    missing_fact: str
    intended_use: str
    idempotency_key: str
    protected_holdout: bool = False
    recursion_depth: int = 0

    def __post_init__(self) -> None:
        super().__post_init__()
        unique(self.input_refs)
        unique(self.instrument_refs)
        if (
            self.family not in TOOL_FAMILIES
            or self.operation not in TOOL_FAMILIES[self.family]
            or self.protected_holdout
            or self.recursion_depth != 0
            or not self.missing_fact
            or not self.intended_use
            or not self.idempotency_key
            or not self.raw_cutoff_ns <= self.input_cutoff_ns < self.deadline_ns
            or not 1 <= len(self.instrument_refs) <= 6
            or len(self.input_refs) > 512
        ):
            raise ValueError("unregistered/out-of-scope tool request")
        if self.family in ("assess_execution", "inspect_economics", "compare_management") and self.action_ref is None:
            raise ValueError("exact action/position binding required")


@dataclass(frozen=True)
class RegisteredToolReceiptV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_REGISTERED_TOOL_RECEIPT_V1"
    request_ref: str
    input_refs: tuple[str, ...]
    calculation: str
    started_ns: int
    finished_ns: int
    available_at_ns: int
    cpu_ns: int
    output: FrozenMap
    status: str
    missing_reasons: tuple[str, ...]
    output_hash: str

    def __post_init__(self) -> None:
        super().__post_init__()
        if (
            self.started_ns > self.finished_ns
            or self.finished_ns > self.available_at_ns
            or self.output_hash != sha256_json(self.output)
            or self.status
            not in (
                "COMPLETE",
                "PARTIAL",
                "UNAVAILABLE",
                "STALE",
                "RESOURCE_DENIED",
                "BUDGET_DENIED",
                "DEADLINE_EXPIRED",
                "NEEDS_NEW_SNAPSHOT",
                "INVALID",
                "ERROR",
            )
        ):
            raise ValueError("invalid typed tool receipt")


def execute_registered_tool(
    request: RegisteredToolRequestV1,
    *,
    facts: tuple[NumericalFactV1, ...],
    now_ns: int,
    consumed_keys: tuple[str, ...] = (),
    tool_calls_used: int = 0,
    model_calls_used: int = 0,
    supplementary_rounds_used: int = 0,
) -> RegisteredToolReceiptV1:
    start = time.thread_time_ns()
    wall = time.perf_counter_ns()
    status = "COMPLETE"
    reasons: tuple[str, ...] = ()
    output: dict[str, Any] = {}
    if now_ns >= request.deadline_ns:
        status, reasons = "DEADLINE_EXPIRED", ("ORIGINAL_DEADLINE",)
    elif (
        request.idempotency_key in consumed_keys
        or tool_calls_used >= request.budget.maximum_tool_calls
        or model_calls_used > request.budget.maximum_model_calls
        or supplementary_rounds_used > request.budget.supplementary_rounds
    ):
        status, reasons = "BUDGET_DENIED", ("DUPLICATE_OR_BUDGET",)
    elif request.operation in ("M0_MEAN_PREDICT", "M1_MEAN_PREDICT"):
        status, reasons = "UNAVAILABLE", ("USE_TYPED_EXECUTE_MEAN_PREDICT_TOOL_WITH_PREFIT_BINDINGS",)
    elif len(facts) > request.budget.maximum_rows:
        status, reasons = "RESOURCE_DENIED", ("FACT_ROW_CAP",)
    elif {f.content_hash for f in facts} != set(request.input_refs) or any(
        f.instrument_ref not in request.instrument_refs for f in facts
    ):
        status, reasons = "INVALID", ("WRONG_INPUT_SCOPE",)
    elif any(f.available_at_ns > request.raw_cutoff_ns for f in facts):
        status, reasons = "NEEDS_NEW_SNAPSHOT", ("NEW_RAW_FACT",)
    elif now_ns < request.input_cutoff_ns:
        status, reasons = "INVALID", ("DERIVED_CHRONOLOGY",)
    else:
        complete = tuple(f for f in facts if f.value is not None)
        if len(complete) != len(facts):
            status, reasons = "PARTIAL", tuple(f.missing_reason for f in facts if f.missing_reason)
        # Each operation reads exact registered fact types; calculations below
        # produce genuinely new bounded information from the same raw prefix.
        output = {
            "facts": [f.to_dict() for f in facts],
            "information_class": "REGISTERED_READ",
            "operation": request.operation,
            "units": [f.unit for f in facts],
        }
        if request.operation == "AR1_HALF_LIFE":
            from atlas.v2.strategies.s3_mean_reversion import fit_ar1

            if len(complete) < 3 or len({f.unit for f in complete}) != 1:
                status, reasons = "UNAVAILABLE", ("AR1_SUPPORT_OR_UNITS",)
            else:
                alpha: float | None = None
                phi: float | None = None
                half_life: float | None = None
                try:
                    alpha, phi, half_life = fit_ar1(
                        [float(cast(Decimal, f.value)) for f in sorted(complete, key=lambda x: x.event_at_ns)]
                    )
                except ValueError as exc:
                    status, reasons = "UNAVAILABLE", (str(exc),)
                    alpha = phi = half_life = None
                output = {
                    "alpha": alpha,
                    "phi": phi,
                    "half_life_steps": half_life,
                    "information_class": "DERIVED_CALCULATION",
                }
        elif request.operation == "HAR_VOL_FORECAST":
            from atlas.v2.math.core import har_forecast

            history = tuple(float(cast(Decimal, f.value)) for f in sorted(complete, key=lambda x: x.event_at_ns))
            value = har_forecast(history, origin=len(history) - 1)
            output = {
                "forecast_variance": value,
                "unit": "squared_log_return",
                "information_class": "DERIVED_CALCULATION",
            }
            if value is None:
                status, reasons = "UNAVAILABLE", ("HAR_SUPPORT",)
        elif request.family == "request_registered_work":
            output = {
                "registered_work": request.operation,
                "request_ref": request.content_hash,
                "status": "VALIDATED_OFFLINE_WORK_REQUEST",
                "authority": "ZERO",
                "information_class": "WORK_CONTRACT_NO_RUNTIME_DISPATCH",
            }
        elif request.family == "retrieve_analogues" and len(facts) > 20:
            status, reasons = "RESOURCE_DENIED", ("ANALOGUE_NEIGHBOUR_CAP",)
        elif request.family == "compare_management" and len(facts) > 8:
            status, reasons = "RESOURCE_DENIED", ("OPTIONAL_MANAGEMENT_CAP",)
        elif request.family == "inspect_market" and len(facts) > 256:
            status, reasons = "RESOURCE_DENIED", ("BAR_CAP",)
    used = time.thread_time_ns() - start
    elapsed = time.perf_counter_ns() - wall
    if used > request.budget.cpu_budget_ns or elapsed > request.budget.wall_budget_ns:
        status, reasons = "RESOURCE_DENIED", ("MEASURED_TOOL_BUDGET",)
        output = {}
    if len(canonical_json(output).encode()) > request.budget.maximum_bytes:
        status, reasons = "RESOURCE_DENIED", ("OUTPUT_BYTE_CAP_NO_SILENT_TRUNCATION",)
        output = {}
    if now_ns + elapsed >= request.deadline_ns:
        status, reasons = "DEADLINE_EXPIRED", ("DERIVED_RESULT_LATE",)
        output = {}
    result = FrozenMap(output)
    return RegisteredToolReceiptV1(
        request.content_hash,
        request.input_refs,
        request.operation,
        now_ns,
        now_ns + elapsed,
        now_ns + elapsed,
        used,
        result,
        status,
        reasons,
        sha256_json(result),
    )


def mean_tool_feature_ref(
    *, action_ref: str, model_ref: str, compatibility_ref: str, features: tuple[float, ...], raw_cutoff_ns: int
) -> str:
    return sha256_json(
        {
            "version": "ATLAS_RED_MEAN_TOOL_FEATURES_V1",
            "action_ref": action_ref,
            "model_ref": model_ref,
            "compatibility_ref": compatibility_ref,
            "features": features,
            "raw_cutoff_ns": raw_cutoff_ns,
        }
    )


def execute_mean_predict_tool(
    request: RegisteredToolRequestV1,
    *,
    model,
    features: tuple[float, ...],
    compatibility_ref: str,
    now_ns: int,
    consumed_keys: tuple[str, ...] = (),
) -> RegisteredToolReceiptV1:
    """Actual compact prefit prediction, without fitting/framework import.

    Request identities bind the exact model/action/feature prefix. The host
    cannot shop among favourable models or substitute an unregistered feature.
    """
    from atlas.v2.science.mean_models import predict_mean

    cpu_start = time.thread_time_ns()
    wall_start = time.perf_counter_ns()
    output: dict[str, Any] = {}
    status = "COMPLETE"
    reasons: tuple[str, ...] = ()
    expected = {"M0_MEAN_PREDICT": "M0_MEAN_RIDGE_V1", "M1_MEAN_PREDICT": "M1_MEAN_LIGHTGBM_L2_V1"}
    if request.action_ref is None:
        raise ValueError("exact action binding required for prefit tool prediction")
    feature_ref = mean_tool_feature_ref(
        action_ref=request.action_ref,
        model_ref=model.content_hash,
        compatibility_ref=compatibility_ref,
        features=features,
        raw_cutoff_ns=request.raw_cutoff_ns,
    )
    if (
        request.family != "inspect_economics"
        or request.operation not in expected
        or expected[request.operation] != model.estimator
    ):
        status, reasons = "INVALID", ("WRONG_REGISTERED_MODEL",)
    elif request.idempotency_key in consumed_keys:
        status, reasons = "BUDGET_DENIED", ("DUPLICATE",)
    elif set(request.input_refs) != {model.content_hash, feature_ref}:
        status, reasons = "INVALID", ("CHANGED_MODEL_FEATURE_ACTION_BINDING",)
    elif now_ns >= request.deadline_ns:
        status, reasons = "DEADLINE_EXPIRED", ("ORIGINAL_DEADLINE",)
    else:
        try:
            value = predict_mean(
                model, features, compatibility_ref=compatibility_ref, cutoff_ns=request.raw_cutoff_ns, now_ns=now_ns
            )
            output = {
                "mean_cash": str(value),
                "unit": "USDT",
                "model_ref": model.content_hash,
                "action_ref": request.action_ref,
                "information_class": "DERIVED_COMPACT_PREFIT_PREDICTION",
            }
        except ValueError as exc:
            status, reasons = "UNAVAILABLE", (str(exc),)
    cpu_used = time.thread_time_ns() - cpu_start
    wall_used = time.perf_counter_ns() - wall_start
    if cpu_used > request.budget.cpu_budget_ns or wall_used > request.budget.wall_budget_ns:
        status, reasons, output = "RESOURCE_DENIED", ("MEASURED_WORK_BUDGET",), {}
    if now_ns + wall_used >= request.deadline_ns:
        status, reasons, output = "DEADLINE_EXPIRED", ("DERIVED_RESULT_LATE",), {}
    result = FrozenMap(output)
    return RegisteredToolReceiptV1(
        request.content_hash,
        request.input_refs,
        request.operation,
        now_ns,
        now_ns + wall_used,
        now_ns + wall_used,
        cpu_used,
        result,
        status,
        reasons,
        sha256_json(result),
    )
