"""Risk reservation mathematics for proposed (never live-reserved) Phase-4 plans."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal

from atlas.domain.risk import RiskPolicy


@dataclass(frozen=True)
class RiskVector:
    normal_loss: Decimal
    stress_loss: Decimal
    notional: Decimal
    beta_notional: Decimal
    margin: Decimal
    es_contribution: Decimal
    remaining_open_qty: Decimal = Decimal("0")
    venue_collateral: Decimal = Decimal("0")

    def possible(self) -> RiskVector:
        return self  # Each record is already worst possible fill, including UNKNOWN/CANCEL_PENDING leaves.


@dataclass(frozen=True)
class AccountState:
    eligible_equity: Decimal
    margin_available: Decimal
    current_margin: Decimal
    drawdown: Decimal
    existing_es: Decimal
    gross_notional: Decimal = Decimal("0")
    beta_notional: Decimal = Decimal("0")
    instrument_notional: Decimal = Decimal("0")
    venue_collateral: Decimal = Decimal("0")
    new_risk_intents: int = 0


@dataclass(frozen=True)
class RiskDecision:
    accepted: bool
    reasons: tuple[str, ...]
    scaled_normal_budget: Decimal


def evaluate_reservation(policy: RiskPolicy, account: AccountState, existing: Iterable[RiskVector], candidate: RiskVector, *, leverage: Decimal) -> RiskDecision:
    e = account.eligible_equity
    if e <= 0:
        return RiskDecision(False, ("nonpositive eligible equity",), Decimal("0"))
    scale = policy.scaling_at(account.drawdown)
    current = tuple(existing)
    reasons = []
    normal = sum((x.normal_loss for x in current), Decimal("0")) + candidate.normal_loss
    if candidate.normal_loss > policy.normal_loss_per_trade_frac * e * scale:
        reasons.append("normal-loss-per-trade")
    if normal > policy.aggregate_open_normal_loss_frac * e * scale:
        reasons.append("aggregate-normal-loss")
    if candidate.stress_loss > policy.stress_loss_per_trade_frac * e * scale:
        reasons.append("stress-loss-per-trade")
    if account.existing_es + candidate.es_contribution > policy.portfolio_es_limit_frac * e * scale:
        reasons.append("portfolio-es")
    if account.gross_notional + sum((x.notional for x in current), Decimal("0")) + candidate.notional > policy.account_gross_notional_limit * e:
        reasons.append("gross-notional")
    if account.instrument_notional + candidate.notional > policy.instrument_notional_limit * e:
        reasons.append("instrument-notional")
    if abs(account.beta_notional + sum((x.beta_notional for x in current), Decimal("0")) + candidate.beta_notional) > policy.correlated_crypto_beta_limit * e:
        reasons.append("beta-notional")
    if account.current_margin + sum((x.margin for x in current), Decimal("0")) + candidate.margin > account.margin_available - policy.min_free_margin_reserve_frac * e:
        reasons.append("free-margin-reserve")
    if leverage > policy.max_contract_leverage:
        reasons.append("contract-leverage")
    if account.venue_collateral + sum((x.venue_collateral for x in current), Decimal("0")) + candidate.venue_collateral > policy.venue_collateral_limit * e:
        reasons.append("venue-collateral")
    if account.new_risk_intents >= policy.max_simultaneous_new_risk_intents:
        reasons.append("serial-new-risk-intent")
    return RiskDecision(not reasons, tuple(reasons), policy.normal_loss_per_trade_frac * e * scale)
