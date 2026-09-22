"""Frozen typed RiskPolicy primitives."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .money import canonical_decimal_str, ensure_decimal
from .time import ensure_utc_ns

CONTRACT_VERSION = "1.0"


def drawdown_scaling(drawdown: Decimal, reduce_threshold: Decimal, stop_threshold: Decimal) -> Decimal:
    for v in (drawdown, reduce_threshold, stop_threshold):
        if not isinstance(v, Decimal):
            raise ValueError("Decimal required")
    if not 0 <= reduce_threshold < stop_threshold:
        raise ValueError("invalid thresholds")
    if drawdown <= reduce_threshold:
        return Decimal("1")
    if drawdown >= stop_threshold:
        return Decimal("0")
    return (stop_threshold - drawdown) / (stop_threshold - reduce_threshold)


@dataclass(frozen=True)
class RiskPolicy:
    policy_version: str
    policy_effective_at_ns: int
    eligible_equity_definition: str
    normal_loss_per_trade_frac: Decimal
    aggregate_open_normal_loss_frac: Decimal
    stress_loss_per_trade_frac: Decimal
    portfolio_es_alpha: Decimal
    portfolio_es_limit_frac: Decimal
    account_gross_notional_limit: Decimal
    instrument_notional_limit: Decimal
    correlated_crypto_beta_limit: Decimal
    venue_collateral_limit: Decimal
    min_free_margin_reserve_frac: Decimal
    drawdown_reduce_threshold: Decimal
    drawdown_stop_threshold: Decimal
    drawdown_reduce_recovery: Decimal
    drawdown_stop_recovery: Decimal
    max_contract_leverage: Decimal
    max_simultaneous_new_risk_intents: int = 1
    external_capital_reference: Decimal | None = None

    def __post_init__(self):
        if not self.policy_version.strip() or not self.eligible_equity_definition.strip():
            raise ValueError("policy identity required")
        ensure_utc_ns(self.policy_effective_at_ns, field="policy_effective_at_ns")
        for n in (
            "normal_loss_per_trade_frac",
            "aggregate_open_normal_loss_frac",
            "stress_loss_per_trade_frac",
            "portfolio_es_limit_frac",
            "account_gross_notional_limit",
            "instrument_notional_limit",
            "correlated_crypto_beta_limit",
            "venue_collateral_limit",
            "min_free_margin_reserve_frac",
            "drawdown_reduce_threshold",
            "drawdown_stop_threshold",
            "drawdown_reduce_recovery",
            "drawdown_stop_recovery",
        ):
            d = ensure_decimal(getattr(self, n), field=n)
            object.__setattr__(self, n, d)
            if d < 0 or d > 1:
                raise ValueError(f"{n} must be fraction in [0,1]")
        object.__setattr__(
            self, "portfolio_es_alpha", ensure_decimal(self.portfolio_es_alpha, field="portfolio_es_alpha")
        )
        object.__setattr__(
            self, "max_contract_leverage", ensure_decimal(self.max_contract_leverage, field="max_contract_leverage")
        )
        if not Decimal("0") < self.portfolio_es_alpha < Decimal("1") or self.max_contract_leverage <= 0:
            raise ValueError("invalid alpha/leverage")
        if not self.max_simultaneous_new_risk_intents >= 1 or isinstance(self.max_simultaneous_new_risk_intents, bool):
            raise ValueError("max_simultaneous_new_risk_intents must be int >= 1")
        if self.external_capital_reference is not None and self.external_capital_reference <= 0:
            raise ValueError("external_capital_reference must be positive")
        if (
            not self.drawdown_reduce_recovery < self.drawdown_reduce_threshold < self.drawdown_stop_threshold
            or not self.drawdown_stop_recovery < self.drawdown_stop_threshold
            or not self.drawdown_reduce_recovery <= self.drawdown_stop_recovery
        ):
            raise ValueError("drawdown reduce/stop threshold or recovery ordering invalid")
        if self.normal_loss_per_trade_frac > self.aggregate_open_normal_loss_frac:
            raise ValueError("per-trade cannot exceed aggregate")

    def scaling_at(self, d: Decimal) -> Decimal:
        return drawdown_scaling(d, self.drawdown_reduce_threshold, self.drawdown_stop_threshold)

    def to_dict(self) -> dict[str, Any]:
        out = {}
        for f in self.__dataclass_fields__:
            v = getattr(self, f)
            out[f] = canonical_decimal_str(v) if isinstance(v, Decimal) else v
        out["contract_version"] = "1.0"
        return out

    def to_canonical_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    def policy_hash(self) -> str:
        return hashlib.sha256(self.to_canonical_json().encode()).hexdigest()


ENGINEERING_DEFAULTS: dict[str, Any] = {
    "_label": "ENGINEERING_DEFAULTS_ONLY_NOT_SAFE_FOR_LIVE",
    "normal_loss_per_trade_frac": Decimal("0.0010"),
    "aggregate_open_normal_loss_frac": Decimal("0.0050"),
    "stress_loss_per_trade_frac": Decimal("0.0025"),
    "portfolio_es_alpha": Decimal("0.975"),
    "portfolio_es_limit_frac": Decimal("0.0100"),
    "account_gross_notional_limit": Decimal("1.00"),
    "instrument_notional_limit": Decimal("0.50"),
    "correlated_crypto_beta_limit": Decimal("0.75"),
    "venue_collateral_limit": Decimal("1.00"),
    "min_free_margin_reserve_frac": Decimal("0.50"),
    "max_contract_leverage": Decimal("2.0"),
    "drawdown_reduce_threshold": Decimal("0.05"),
    "drawdown_stop_threshold": Decimal("0.10"),
    "drawdown_reduce_recovery": Decimal("0.04"),
    "drawdown_stop_recovery": Decimal("0.08"),
    "max_simultaneous_new_risk_intents": 1,
}


def engineering_default_policy(*, policy_version: str = "eng-0.1", policy_effective_at_ns: int = 0) -> RiskPolicy:
    return RiskPolicy(
        policy_version,
        policy_effective_at_ns,
        "ENGINEERING_DEFAULT eligible account equity (testnet/shadow only)",
        ENGINEERING_DEFAULTS["normal_loss_per_trade_frac"],
        ENGINEERING_DEFAULTS["aggregate_open_normal_loss_frac"],
        ENGINEERING_DEFAULTS["stress_loss_per_trade_frac"],
        ENGINEERING_DEFAULTS["portfolio_es_alpha"],
        ENGINEERING_DEFAULTS["portfolio_es_limit_frac"],
        ENGINEERING_DEFAULTS["account_gross_notional_limit"],
        ENGINEERING_DEFAULTS["instrument_notional_limit"],
        ENGINEERING_DEFAULTS["correlated_crypto_beta_limit"],
        ENGINEERING_DEFAULTS["venue_collateral_limit"],
        ENGINEERING_DEFAULTS["min_free_margin_reserve_frac"],
        ENGINEERING_DEFAULTS["drawdown_reduce_threshold"],
        ENGINEERING_DEFAULTS["drawdown_stop_threshold"],
        ENGINEERING_DEFAULTS["drawdown_reduce_recovery"],
        ENGINEERING_DEFAULTS["drawdown_stop_recovery"],
        ENGINEERING_DEFAULTS["max_contract_leverage"],
        ENGINEERING_DEFAULTS["max_simultaneous_new_risk_intents"],
    )
