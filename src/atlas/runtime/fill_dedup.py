"""Durable execution evidence deduplicated by exchange execution ID."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

from atlas.domain.time import ensure_utc_ns

if TYPE_CHECKING:
    from atlas.persistence.sqlite import SQLiteJournal


@dataclass(frozen=True)
class FillRecord:
    execution_id: str
    order_id: str
    client_order_id: str
    intent_id: str
    instrument: str
    side: str
    qty: Decimal
    price: Decimal
    fee: Decimal
    fee_currency: str
    trade_time_ns: int
    receive_time_ns: int
    source: str
    raw_hash: str

    def __post_init__(self):
        for n in (
            "execution_id",
            "order_id",
            "client_order_id",
            "intent_id",
            "instrument",
            "side",
            "fee_currency",
            "source",
            "raw_hash",
        ):
            if not getattr(self, n).strip():
                raise ValueError(f"{n} required")
        if self.side not in ("Buy", "Sell") or self.qty <= 0 or self.price <= 0 or self.fee < 0:
            raise ValueError("invalid fill")
        ensure_utc_ns(self.trade_time_ns, field="trade_time_ns")
        ensure_utc_ns(self.receive_time_ns, field="receive_time_ns")
        if self.receive_time_ns < self.trade_time_ns:
            raise ValueError("receipt before trade")


@dataclass(frozen=True)
class OrderStatusRecord:
    order_id: str
    client_order_id: str
    intent_id: str
    status: str
    cum_exec_qty: Decimal
    cum_exec_fee: Decimal
    cum_exec_value: Decimal
    avg_exec_price: Decimal | None
    receive_time_ns: int
    source: str
    raw_hash: str


@dataclass(frozen=True)
class FillDedupResult:
    accepted: bool
    fill_record: FillRecord | None
    duplicate_of: str | None
    reason: str | None


class FillDeduplicator:
    def __init__(self, journal: SQLiteJournal | None = None):
        self.journal = journal
        self._fills: dict[str, FillRecord] = {}
        self._cum: dict[str, Decimal] = {}
        if journal:
            for f in journal.load_execution_evidence():
                self._remember(f)

    def _remember(self, f: FillRecord):
        self._fills[f.execution_id] = f
        self._cum[f.intent_id] = self._cum.get(f.intent_id, Decimal("0")) + f.qty

    def try_record_fill(self, f: FillRecord) -> FillDedupResult:
        if f.execution_id in self._fills:
            if self._fills[f.execution_id] != f:
                raise ValueError("conflicting execution payload")
            return FillDedupResult(False, None, f.execution_id, "duplicate execution_id")
        if self.journal:
            self.journal.append_execution_evidence(f)
        self._remember(f)
        return FillDedupResult(True, f, None, None)

    def record_status(self, s: OrderStatusRecord) -> None:
        if self.journal:
            self.journal.append_order_status_observation(s)

    def get_cumulative_qty(self, intent_id: str) -> Decimal:
        return self._cum.get(intent_id, Decimal("0"))


@dataclass(frozen=True)
class ExecutionEvidence:
    intent_id: str
    client_order_id: str
    fills: tuple[FillRecord, ...]
    status_observations: tuple[OrderStatusRecord, ...]
    cumulative_qty: Decimal
    cumulative_fee: Decimal
    last_update_ns: int


def build_execution_evidence(
    intent_id: str,
    client_order_id: str,
    fills: list[FillRecord],
    status_observations: list[OrderStatusRecord],
    now_ns: int,
) -> ExecutionEvidence:
    ids = [f.execution_id for f in fills]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate execution IDs")
    return ExecutionEvidence(
        intent_id,
        client_order_id,
        tuple(sorted(fills, key=lambda x: (x.trade_time_ns, x.execution_id))),
        tuple(sorted(status_observations, key=lambda x: x.receive_time_ns)),
        sum((f.qty for f in fills), Decimal("0")),
        sum((f.fee for f in fills), Decimal("0")),
        now_ns,
    )
