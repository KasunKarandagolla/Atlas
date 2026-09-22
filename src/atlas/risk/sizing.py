"""Largest feasible venue-rounded quantity; never searches an LCB/P&L optimum."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from decimal import Decimal

from atlas.domain.risk import RiskPolicy
from atlas.risk.engine import AccountState, RiskVector, evaluate_reservation
from atlas.strategy.policy import round_down


def largest_feasible_quantity(maximum: Decimal, lot: Decimal, minimum: Decimal, feasible: Callable[[Decimal], bool]) -> Decimal | None:
    q = round_down(maximum, lot)
    while q >= minimum:
        if feasible(q):
            return q
        q -= lot
    return None


@dataclass(frozen=True)
class PerUnitRisk:
    normal_loss: Decimal
    stress_loss: Decimal
    notional: Decimal
    beta_notional: Decimal
    margin: Decimal
    es_contribution: Decimal
    venue_collateral: Decimal

    def vector(self, quantity: Decimal) -> RiskVector:
        return RiskVector(self.normal_loss * quantity, self.stress_loss * quantity, self.notional * quantity,
                          self.beta_notional * quantity, self.margin * quantity,
                          self.es_contribution * quantity, remaining_open_qty=quantity,
                          venue_collateral=self.venue_collateral * quantity)


@dataclass(frozen=True)
class QuantitySelection:
    quantity: Decimal | None
    risk: RiskVector | None
    reason: str


def deterministic_risk_quantity(*, policy: RiskPolicy, account: AccountState, pending: tuple[RiskVector, ...],
                                per_unit: PerUnitRisk, venue_maximum: Decimal, lot: Decimal, minimum: Decimal,
                                leverage: Decimal) -> QuantitySelection:
    """Largest hard-constraint quantity, before any noisy LCB/scenario acceptance."""
    quantity = largest_feasible_quantity(
        venue_maximum, lot, minimum,
        lambda q: evaluate_reservation(policy, account, pending, per_unit.vector(q), leverage=leverage).accepted,
    )
    if quantity is None:
        return QuantitySelection(None, None, "MIN_SIZE_OR_HARD_RISK")
    return QuantitySelection(quantity, per_unit.vector(quantity), "")


def allocate_serial_btc_then_eth(*, policy: RiskPolicy, account: AccountState,
                                 requests: dict[str, tuple[PerUnitRisk, Decimal, Decimal, Decimal, Decimal]]) -> dict[str, QuantitySelection]:
    """Deterministic account-wide serial allocation; reservations update BTC then ETH."""
    results: dict[str, QuantitySelection] = {}
    pending: list[RiskVector] = []
    current = account
    for instrument in ("BTCUSDT", "ETHUSDT"):
        if instrument not in requests:
            continue
        per_unit, maximum, lot, minimum, leverage = requests[instrument]
        result = deterministic_risk_quantity(policy=policy, account=current, pending=tuple(pending), per_unit=per_unit,
                                             venue_maximum=maximum, lot=lot, minimum=minimum, leverage=leverage)
        results[instrument] = result
        if result.risk is not None:
            pending.append(result.risk)
            current = replace(current, new_risk_intents=current.new_risk_intents + 1)
    return results
