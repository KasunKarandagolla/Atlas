"""Largest feasible venue-rounded quantity; never searches an LCB/P&L optimum."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

from atlas.strategy.policy import round_down


def largest_feasible_quantity(maximum: Decimal, lot: Decimal, minimum: Decimal, feasible: Callable[[Decimal], bool]) -> Decimal | None:
    q = round_down(maximum, lot)
    while q >= minimum:
        if feasible(q):
            return q
        q -= lot
    return None
