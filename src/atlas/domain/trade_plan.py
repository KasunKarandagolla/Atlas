"""TradePlan (freeze §1.4): immutable/versioned plan + execution eligibility validator.

Frozen fields preserved; validation returns structured reasons (not just bool).
Unsupported/unverified protection causes eligibility failure via the validator,
not by hiding the plan.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .capability import CapabilityContract
from .enums import CapabilityStatus, Side
from .money import canonical_decimal_str, ensure_decimal, ensure_positive_decimal
from .time import ensure_utc_ns

CONTRACT_VERSION = "1.0"


def _nonblank(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-blank string")
    return value.strip()


@dataclass(frozen=True)
class TradePlan:
    plan_id: str
    version: str
    policy_hash: str
    snapshot_hash: str
    expires_at_ns: int
    market: str
    account_scope: str
    instrument: str
    side: Side
    qty_limit: Decimal
    entry_policy: str
    collar: Decimal
    stop: Decimal
    stop_trigger_basis: str
    management_policy: str
    horizon_end_ns: int
    cost_distribution_ref: str
    normal_risk: Decimal
    stress_risk: Decimal
    margin: Decimal
    leverage_bound: Decimal
    risk_config_hash: str
    created_at_ns: int
    available_at_ns: int
    reference_price: Decimal | None = None
    contract_version: str = CONTRACT_VERSION

    def __post_init__(self) -> None:
        _nonblank(self.plan_id, "plan_id")
        _nonblank(self.version, "version")
        _nonblank(self.policy_hash, "policy_hash")
        _nonblank(self.snapshot_hash, "snapshot_hash")
        _nonblank(self.market, "market")
        _nonblank(self.account_scope, "account_scope")
        _nonblank(self.instrument, "instrument")
        _nonblank(self.entry_policy, "entry_policy")
        _nonblank(self.stop_trigger_basis, "stop_trigger_basis")
        _nonblank(self.management_policy, "management_policy")
        _nonblank(self.cost_distribution_ref, "cost_distribution_ref")
        _nonblank(self.risk_config_hash, "risk_config_hash")
        _nonblank(self.contract_version, "contract_version")
        if not isinstance(self.side, Side):
            raise ValueError(f"side must be Side, got {self.side!r}")
        ensure_utc_ns(self.expires_at_ns, field="expires_at_ns")
        ensure_utc_ns(self.horizon_end_ns, field="horizon_end_ns")
        ensure_utc_ns(self.created_at_ns, field="created_at_ns")
        ensure_utc_ns(self.available_at_ns, field="available_at_ns")
        qty = ensure_positive_decimal(self.qty_limit, field="qty_limit")
        object.__setattr__(self, "qty_limit", qty)
        collar = ensure_decimal(self.collar, field="collar")
        object.__setattr__(self, "collar", collar)
        stop = ensure_decimal(self.stop, field="stop")
        object.__setattr__(self, "stop", stop)
        normal_risk = ensure_decimal(self.normal_risk, field="normal_risk")
        object.__setattr__(self, "normal_risk", normal_risk)
        stress_risk = ensure_decimal(self.stress_risk, field="stress_risk")
        object.__setattr__(self, "stress_risk", stress_risk)
        margin = ensure_decimal(self.margin, field="margin")
        object.__setattr__(self, "margin", margin)
        leverage = ensure_decimal(self.leverage_bound, field="leverage_bound")
        object.__setattr__(self, "leverage_bound", leverage)
        if self.qty_limit <= 0:
            raise ValueError("qty_limit must be positive")
        if self.expires_at_ns <= self.created_at_ns:
            raise ValueError("expires_at must be after creation")
        if self.horizon_end_ns <= self.created_at_ns:
            raise ValueError("horizon_end must be after creation")
        if self.available_at_ns < self.created_at_ns:
            raise ValueError("available_at cannot precede created_at")
        if self.normal_risk < 0 or self.stress_risk < 0 or self.margin < 0:
            raise ValueError("risk/margin must be non-negative")
        if self.collar <= 0 or self.stop <= 0:
            raise ValueError("collar/stop must be positive prices")
        if self.leverage_bound <= 0:
            raise ValueError("leverage_bound must be positive")
        if self.reference_price is not None:
            rp = ensure_decimal(self.reference_price, field="reference_price")
            object.__setattr__(self, "reference_price", rp)
            # Stop must be logically compatible with side when reference supplied:
            # LONG stop below reference; SHORT stop above reference.
            if self.side == Side.LONG and not (self.stop < rp):
                raise ValueError("LONG stop must be below reference price")
            if self.side == Side.SHORT and not (self.stop > rp):
                raise ValueError("SHORT stop must be above reference price")

    def is_expired(self, now_ns: int) -> bool:
        ensure_utc_ns(now_ns, field="now_ns")
        return now_ns >= self.expires_at_ns

    def to_dict(self) -> dict[str, Any]:
        def _d(v: Decimal) -> str:
            return canonical_decimal_str(v)

        return {
            "contract_version": self.contract_version,
            "plan_id": self.plan_id,
            "version": self.version,
            "policy_hash": self.policy_hash,
            "snapshot_hash": self.snapshot_hash,
            "expires_at_ns": self.expires_at_ns,
            "market": self.market,
            "account_scope": self.account_scope,
            "instrument": self.instrument,
            "side": self.side.value,
            "qty_limit": _d(self.qty_limit),
            "entry_policy": self.entry_policy,
            "collar": _d(self.collar),
            "stop": _d(self.stop),
            "stop_trigger_basis": self.stop_trigger_basis,
            "management_policy": self.management_policy,
            "horizon_end_ns": self.horizon_end_ns,
            "cost_distribution_ref": self.cost_distribution_ref,
            "normal_risk": _d(self.normal_risk),
            "stress_risk": _d(self.stress_risk),
            "margin": _d(self.margin),
            "leverage_bound": _d(self.leverage_bound),
            "risk_config_hash": self.risk_config_hash,
            "created_at_ns": self.created_at_ns,
            "available_at_ns": self.available_at_ns,
            "reference_price": _d(self.reference_price) if self.reference_price is not None else None,
        }

    def to_canonical_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    def plan_hash(self) -> str:
        return hashlib.sha256(self.to_canonical_json().encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ExecutionEligibility:
    ok: bool
    reasons: list[str]

    def __bool__(self) -> bool:
        return self.ok


def validate_plan_for_execution(
    plan: TradePlan, capabilities: CapabilityContract, now_ns: int
) -> ExecutionEligibility:
    """Deterministic eligibility check returning structured reasons."""
    ensure_utc_ns(now_ns, field="now_ns")
    reasons: list[str] = []
    if plan.is_expired(now_ns):
        reasons.append(f"plan expired: now={now_ns} >= expires_at={plan.expires_at_ns}")
    # Protection capability gate: every protection-relevant capability must be SUPPORTED.
    # Unknown never implies supported.
    for cap_name in (
        "entry_ioc_with_attached_full_mark_market_stop",
        "native_stop_visible_and_resizes_on_partial_fill",
        "reduce_only_wire_and_matching_enforcement",
        "ambiguous_submit_not_treated_as_definite_rejection",
        "external_native_stop_fill_reconciliation",
        "native_position_stop_read_and_repair_port",
    ):
        status = getattr(capabilities.capabilities, cap_name)
        if status != CapabilityStatus.SUPPORTED:
            reasons.append(f"protection capability {cap_name}={status.value} (requires SUPPORTED)")
    if capabilities.assisted_enabled and reasons:
        reasons.append("assisted_enabled cannot hold while blockers remain (contract invariant)")
    return ExecutionEligibility(ok=not reasons, reasons=reasons)
