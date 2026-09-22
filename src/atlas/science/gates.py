"""Pure causal operational, market, and scheduled-event entry gates."""

from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum


class GateStatus(StrEnum):
    PASS = "PASS"
    NO_TRADE_GATE = "NO_TRADE_GATE"
    NO_TRADE_EVENT = "NO_TRADE_EVENT"
    NOT_ESTIMABLE = "NOT_ESTIMABLE"
    GATE_DISABLED_DIAGNOSTIC = "GATE_DISABLED_DIAGNOSTIC"


@dataclass(frozen=True)
class MarketGateInput:
    now_ns: int
    quote_at_ns: int | None
    mark_at_ns: int | None
    funding_at_ns: int | None
    bid: Decimal | None
    ask: Decimal | None
    mark: Decimal | None
    last: Decimal | None
    opposite_depth: Decimal | None
    quantity: Decimal
    modeled_entry_impact: Decimal | None
    public_current: bool
    private_current: bool
    book_synchronized: bool
    filters_complete: bool


@dataclass(frozen=True)
class EventGateInput:
    calendar_available: bool
    events_at_ns: tuple[int, ...] = ()
    venue_maintenance: bool = False
    venue_incident: bool = False
    asset_incident: bool = False
    diagnostic_gate_disabled: bool = False


def market_gate(value: MarketGateInput) -> tuple[GateStatus, str]:
    if any(x is None for x in (value.quote_at_ns, value.mark_at_ns, value.funding_at_ns, value.bid, value.ask, value.mark, value.last, value.modeled_entry_impact)):
        return GateStatus.NOT_ESTIMABLE, "missing causal quote/mark/funding inputs"
    if value.opposite_depth is None:
        return GateStatus.NOT_ESTIMABLE, "missing displayed opposite depth"
    quote_at, mark_at, funding_at = value.quote_at_ns, value.mark_at_ns, value.funding_at_ns
    bid, ask, mark, last, impact, depth = value.bid, value.ask, value.mark, value.last, value.modeled_entry_impact, value.opposite_depth
    assert quote_at is not None and mark_at is not None and funding_at is not None
    assert bid is not None and ask is not None and mark is not None and last is not None and impact is not None and depth is not None
    if value.now_ns - quote_at > 1_000_000_000 or value.now_ns - mark_at > 1_000_000_000:
        return GateStatus.NO_TRADE_GATE, "stale quote or mark"
    if value.now_ns - funding_at > 60_000_000_000:
        return GateStatus.NO_TRADE_GATE, "stale funding state"
    if not (value.public_current and value.private_current and value.book_synchronized and value.filters_complete):
        return GateStatus.NO_TRADE_GATE, "transport/book/filter gate"
    midpoint = (bid + ask) / Decimal("2")
    if bid <= 0 or ask <= bid or (ask - bid) / midpoint > Decimal("0.0005"):
        return GateStatus.NO_TRADE_GATE, "spread gate"
    if impact > Decimal("0.0005"):
        return GateStatus.NO_TRADE_GATE, "entry impact gate"
    if abs(math.log(float(mark / last))) > 0.002:
        return GateStatus.NO_TRADE_GATE, "mark/last dislocation"
    if value.quantity > depth * Decimal("0.10"):
        return GateStatus.NO_TRADE_GATE, "participation cap"
    return GateStatus.PASS, ""


def event_gate(now_ns: int, value: EventGateInput) -> tuple[GateStatus, str]:
    if not value.calendar_available:
        return (GateStatus.GATE_DISABLED_DIAGNOSTIC, "calendar unavailable diagnostic") if value.diagnostic_gate_disabled else (GateStatus.NOT_ESTIMABLE, "required event calendar unavailable")
    if value.venue_maintenance or value.venue_incident or value.asset_incident:
        return GateStatus.NO_TRADE_EVENT, "maintenance or unresolved incident"
    for at_ns in value.events_at_ns:
        if at_ns - 1_800_000_000_000 <= now_ns <= at_ns + 900_000_000_000:
            return GateStatus.NO_TRADE_EVENT, "scheduled macro event window"
    return GateStatus.PASS, ""
