"""RiskPolicy (freeze §7): typed/versioned, fractions not percentages.

0.001 means 0.10%. Default max_simultaneous_new_risk_intents = 1.
Engineering defaults are labeled explicitly and are NOT validated safe levels.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .money import canonical_decimal_str, ensure_decimal
from .time import ensure_utc_ns

CONTRACT_VERSION = "1.0"


def _frac(value: object, name: str) -> Decimal:
    d = ensure_decimal(value, field=name)  # type: ignore[arg-type]
    if d < 0 or d > 1:
        raise ValueError(f"{name}: fraction must be in [0,1], got {d}")
    return d


def drawdown_scaling(drawdown: Decimal, reduce_threshold: Decimal, stop_threshold: Decimal) -> Decimal:
    """Frozen drawdown scaling s(D) from §7.3 (pure function).

    s(D) = 1 if D <= D_reduce
         = (D_stop - D)/(D_stop - D_reduce) if D_reduce < D < D_stop
         = 0 if D >= D_stop
    All inputs are fractions (0.05 = 5%).
    """
    from decimal import Decimal as D

    if not isinstance(drawdown, D) or not isinstance(reduce_threshold, D) or not isinstance(stop_threshold, D):
        raise ValueError("drawdown scaling requires Decimal inputs")
    if reduce_threshold < 0 or stop_threshold < 0:
        raise ValueError("thresholds must be non-negative")
    if not (reduce_threshold < stop_threshold):
        raise ValueError("requires reduce_threshold < stop_threshold")
    if drawdown <= reduce_threshold:
        return D("1")
    if drawdown >= stop_threshold:
        return D("0")
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

    def __post_init__(self) -> None:
        if not isinstance(self.policy_version, str) or not self.policy_version.strip():
            raise ValueError("policy_version must be non-blank")
        ensure_utc_ns(self.policy_effective_at_ns, field="policy_effective_at_ns")
        if not isinstance(self.eligible_equity_definition, str) or not self.eligible_equity_definition.strip():
            raise ValueError("eligible_equity_definition must be non-blank")

        for name in (
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
            _frac(getattr(self, name), name)

        alpha = ensure_decimal(self.portfolio_es_alpha, field="portfolio_es_alpha")
        if not (Decimal("0") < alpha < Decimal("1")):
            raise ValueError("portfolio_es_alpha must be in (0,1)")
        lev = ensure_decimal(self.max_contract_leverage, field="max_contract_leverage")
        if lev <= 0:
            raise ValueError("max_contract_leverage must be positive")
        if not isinstance(self.max_simultaneous_new_risk_intents, int) or isinstance(
            self.max_simultaneous_new_risk_intents, bool
        ):
            raise ValueError("max_simultaneous_new_risk_intents must be int")
        if self.max_simultaneous_new_risk_intents < 1:
            raise ValueError("max_simultaneous_new_risk_intents must be >= 1")
        if self.external_capital_reference is not None:
            ext = ensure_decimal(self.external_capital_reference, field="external_capital_reference")
            if ext <= 0:
                raise ValueError("external_capital_reference must be positive if supplied")
            object.__setattr__(self, "external_capital_reference", ext)

        # Threshold ordering (hysteresis): recovery < threshold; reduce < stop.
        if not (self.drawdown_reduce_recovery < self.drawdown_reduce_threshold):
            raise ValueError("requires drawdown_reduce_recovery < drawdown_reduce_threshold")
        if not (self.drawdown_stop_recovery < self.drawdown_stop_threshold):
            raise ValueError("requires drawdown_stop_recovery < drawdown_stop_threshold")
        if not (self.drawdown_reduce_threshold < self.drawdown_stop_threshold):
            raise ValueError("requires drawdown_reduce_threshold < drawdown_stop_threshold")
        if not (self.drawdown_reduce_recovery <= self.drawdown_stop_recovery):
            raise ValueError("requires drawdown_reduce_recovery <= drawdown_stop_recovery")
        # Per-trade cannot exceed aggregate open budget.
        if self.normal_loss_per_trade_frac > self.aggregate_open_normal_loss_frac:
            raise ValueError("normal_loss_per_trade cannot exceed aggregate_open_normal_loss")

    def scaling_at(self, drawdown_frac: Decimal) -> Decimal:
        dd = ensure_decimal(drawdown_frac, field="drawdown_frac")
        if dd < 0:
            raise ValueError("drawdown must be >= 0")
        return drawdown_scaling(dd, self.drawdown_reduce_threshold, self.drawdown_stop_threshold)

    def to_dict(self) -> dict[str, Any]:
        def _d(v: Decimal) -> str:
            return canonical_decimal_str(v)

        return {
            "policy_version": self.policy_version,
            "policy_effective_at_ns": self.policy_effective_at_ns,
            "eligible_equity_definition": self.eligible_equity_definition,
            "normal_loss_per_trade_frac": _d(self.normal_loss_per_trade_frac),
            "aggregate_open_normal_loss_frac": _d(self.aggregate_open_normal_loss_frac),
            "stress_loss_per_trade_frac": _d(self.stress_loss_per_trade_frac),
            "portfolio_es_alpha": _d(self.portfolio_es_alpha),
            "portfolio_es_limit_frac": _d(self.portfolio_es_limit_frac),
            "account_gross_notional_limit": _d(self.account_gross_notional_limit),
            "instrument_notional_limit": _d(self.instrument_notional_limit),
            "correlated_crypto_beta_limit": _d(self.correlated_crypto_beta_limit),
            "venue_collateral_limit": _d(self.venue_collateral_limit),
            "min_free_margin_reserve_frac": _d(self.min_free_margin_reserve_frac),
            "drawdown_reduce_threshold": _d(self.drawdown_reduce_threshold),
            "drawdown_stop_threshold": _d(self.drawdown_stop_threshold),
            "drawdown_reduce_recovery": _d(self.drawdown_reduce_recovery),
            "drawdown_stop_recovery": _d(self.drawdown_stop_recovery),
            "max_contract_leverage": _d(self.max_contract_leverage),
            "max_simultaneous_new_risk_intents": self.max_simultaneous_new_risk_intents,
            "external_capital_reference": _d(self.external_capital_reference)
            if self.external_capital_reference is not None
            else None,
            "contract_version": CONTRACT_VERSION,
        }

    def to_canonical_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    def policy_hash(self) -> str:
        return hashlib.sha256(self.to_canonical_json().encode("utf-8")).hexdigest()


# Explicitly engineering defaults for testnet/shadow/canary ONLY.
# NOT validated safe levels and NOT a recommendation for live capital. (freeze §7.4)
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


def engineering_default_policy(
    *, policy_version: str = "eng-0.1", policy_effective_at_ns: int = 0
) -> RiskPolicy:
    return RiskPolicy(
        policy_version=policy_version,
        policy_effective_at_ns=policy_effective_at_ns,
        eligible_equity_definition="ENGINEERING_DEFAULT eligible account equity (testnet/shadow only)",
        normal_loss_per_trade_frac=ENGINEERING_DEFAULTS["normal_loss_per_trade_frac"],
        aggregate_open_normal_loss_frac=ENGINEERING_DEFAULTS["aggregate_open_normal_loss_frac"],
        stress_loss_per_trade_frac=ENGINEERING_DEFAULTS["stress_loss_per_trade_frac"],
        portfolio_es_alpha=ENGINEERING_DEFAULTS["portfolio_es_alpha"],
        portfolio_es_limit_frac=ENGINEERING_DEFAULTS["portfolio_es_limit_frac"],
        account_gross_notional_limit=ENGINEERING_DEFAULTS["account_gross_notional_limit"],
        instrument_notional_limit=ENGINEERING_DEFAULTS["instrument_notional_limit"],
        correlated_crypto_beta_limit=ENGINEERING_DEFAULTS["correlated_crypto_beta_limit"],
        venue_collateral_limit=ENGINEERING_DEFAULTS["venue_collateral_limit"],
        min_free_margin_reserve_frac=ENGINEERING_DEFAULTS["min_free_margin_reserve_frac"],
        drawdown_reduce_threshold=ENGINEERING_DEFAULTS["drawdown_reduce_threshold"],
        drawdown_stop_threshold=ENGINEERING_DEFAULTS["drawdown_stop_threshold"],
        drawdown_reduce_recovery=ENGINEERING_DEFAULTS["drawdown_reduce_recovery"],
        drawdown_stop_recovery=ENGINEERING_DEFAULTS["drawdown_stop_recovery"],
        max_contract_leverage=ENGINEERING_DEFAULTS["max_contract_leverage"],
        max_simultaneous_new_risk_intents=ENGINEERING_DEFAULTS[
            "max_simultaneous_new_risk_intents"
        ],
    )
