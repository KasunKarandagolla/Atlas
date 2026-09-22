"""Immutable execution plan contract."""

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
    contract_version: str = "1.0"

    def __post_init__(self):
        for n in (
            "plan_id",
            "version",
            "policy_hash",
            "snapshot_hash",
            "market",
            "account_scope",
            "instrument",
            "entry_policy",
            "stop_trigger_basis",
            "management_policy",
            "cost_distribution_ref",
            "risk_config_hash",
            "contract_version",
        ):
            if not isinstance(getattr(self, n), str) or not getattr(self, n).strip():
                raise ValueError(f"{n} must be non-blank")
        if not isinstance(self.side, Side):
            raise ValueError("side must be Side")
        for n in ("expires_at_ns", "horizon_end_ns", "created_at_ns", "available_at_ns"):
            ensure_utc_ns(getattr(self, n), field=n)
        object.__setattr__(self, "qty_limit", ensure_positive_decimal(self.qty_limit, field="qty_limit"))
        for n in ("collar", "stop", "normal_risk", "stress_risk", "margin", "leverage_bound"):
            object.__setattr__(self, n, ensure_decimal(getattr(self, n), field=n))
        if self.expires_at_ns <= self.created_at_ns:
            raise ValueError("expires_at must be after creation")
        if self.horizon_end_ns <= self.created_at_ns:
            raise ValueError("horizon_end must be after creation")
        if self.available_at_ns < self.created_at_ns:
            raise ValueError("available_at before creation")
        if self.collar <= 0 or self.stop <= 0 or self.leverage_bound <= 0:
            raise ValueError("prices/leverage must be positive")
        if min(self.normal_risk, self.stress_risk, self.margin) < 0:
            raise ValueError("risk/margin nonnegative")
        if self.reference_price is not None:
            object.__setattr__(self, "reference_price", ensure_decimal(self.reference_price, field="reference_price"))
            if self.side == Side.LONG and not self.stop < self.reference_price:
                raise ValueError("LONG stop must be below reference")
            if self.side == Side.SHORT and not self.stop > self.reference_price:
                raise ValueError("SHORT stop must be above reference")

    def is_expired(self, now_ns: int) -> bool:
        ensure_utc_ns(now_ns, field="now_ns")
        return now_ns >= self.expires_at_ns

    def to_dict(self) -> dict[str, Any]:
        out = {}
        for f in self.__dataclass_fields__:
            v = getattr(self, f)
            out[f] = v.value if isinstance(v, Side) else canonical_decimal_str(v) if isinstance(v, Decimal) else v
        return out

    def to_canonical_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    def plan_hash(self) -> str:
        return hashlib.sha256(self.to_canonical_json().encode()).hexdigest()


@dataclass(frozen=True)
class ExecutionEligibility:
    ok: bool
    reasons: list[str]

    def __bool__(self):
        return self.ok


def validate_plan_for_execution(plan: TradePlan, capabilities: CapabilityContract, now_ns: int) -> ExecutionEligibility:
    reasons = []
    if plan.is_expired(now_ns):
        reasons.append("plan expired")
    for name in capabilities.capabilities.__dataclass_fields__:
        st = getattr(capabilities.capabilities, name)
        if st != CapabilityStatus.SUPPORTED:
            reasons.append(f"protection capability {name}={st.value} (requires SUPPORTED)")
    return ExecutionEligibility(not reasons, reasons)
