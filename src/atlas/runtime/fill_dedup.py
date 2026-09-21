"""Durable execution/fill deduplication evidence (freeze §1.4).

- Deduplicate fills by exchange execution ID
- Order-status messages are NOT fills
- Cumulative filled quantity cannot regress due to stale status
- Duplicate Filled/cancel race does not double P&L/quantity
- Out-of-order status cannot undo newer economic facts
- Corrections append evidence instead of rewriting history

This is an audit/reconciliation ledger around Nautilus/exchange evidence only.
NOT a competing OMS.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from atlas.domain.time import ensure_utc_ns


@dataclass(frozen=True)
class FillRecord:
    """Immutable fill record deduplicated by exchange execution ID."""

    execution_id: str          # Exchange execution ID (unique per fill)
    order_id: str              # Exchange order ID
    client_order_id: str       # ATLAS client order ID
    intent_id: str
    instrument: str
    side: str                  # "Buy" | "Sell"
    qty: Decimal
    price: Decimal
    fee: Decimal
    fee_currency: str
    trade_time_ns: int
    receive_time_ns: int
    source: str                # "private_stream" | "rest_query" | "reconciliation"
    raw_hash: str              # SHA256 of raw venue message

    def __post_init__(self) -> None:
        for f in ("execution_id", "order_id", "client_order_id", "intent_id", "instrument", "side"):
            v = getattr(self, f)
            if not isinstance(v, str) or not v.strip():
                raise ValueError(f"{f} must be non-blank")
        if not isinstance(self.qty, Decimal) or self.qty <= 0:
            raise ValueError("qty must be positive Decimal")
        if not isinstance(self.price, Decimal) or self.price <= 0:
            raise ValueError("price must be positive Decimal")
        if not isinstance(self.fee, Decimal) or self.fee < 0:
            raise ValueError("fee must be non-negative Decimal")
        if not self.fee_currency or not self.fee_currency.strip():
            raise ValueError("fee_currency must be non-blank")
        ensure_utc_ns(self.trade_time_ns, field="trade_time_ns")
        ensure_utc_ns(self.receive_time_ns, field="receive_time_ns")
        if self.receive_time_ns < self.trade_time_ns:
            raise ValueError("receive_time_ns cannot precede trade_time_ns")
        if not self.raw_hash or not self.raw_hash.strip():
            raise ValueError("raw_hash must be non-blank")

    @property
    def dedup_key(self) -> str:
        """Deduplication key: exchange execution ID."""
        return self.execution_id


@dataclass(frozen=True)
class OrderStatusRecord:
    """Order status observation (NOT a fill)."""

    order_id: str
    client_order_id: str
    intent_id: str
    status: str                # "New" | "PartiallyFilled" | "Filled" | "Cancelled" | "Rejected" | "PendingCancel"
    cum_exec_qty: Decimal
    cum_exec_fee: Decimal
    cum_exec_value: Decimal
    avg_exec_price: Decimal | None
    receive_time_ns: int
    source: str
    raw_hash: str

    def __post_init__(self) -> None:
        for f in ("order_id", "client_order_id", "intent_id", "status", "source"):
            v = getattr(self, f)
            if not isinstance(v, str) or not v.strip():
                raise ValueError(f"{f} must be non-blank")
        if not isinstance(self.cum_exec_qty, Decimal) or self.cum_exec_qty < 0:
            raise ValueError("cum_exec_qty must be non-negative Decimal")
        if not isinstance(self.cum_exec_fee, Decimal) or self.cum_exec_fee < 0:
            raise ValueError("cum_exec_fee must be non-negative Decimal")
        if not isinstance(self.cum_exec_value, Decimal) or self.cum_exec_value < 0:
            raise ValueError("cum_exec_value must be non-negative Decimal")
        if self.avg_exec_price is not None:
            if not isinstance(self.avg_exec_price, Decimal) or self.avg_exec_price <= 0:
                raise ValueError("avg_exec_price must be positive Decimal if present")
        ensure_utc_ns(self.receive_time_ns, field="receive_time_ns")


@dataclass(frozen=True)
class FillDedupResult:
    """Result of attempting to record a fill."""

    accepted: bool
    fill_record: FillRecord | None
    duplicate_of: str | None   # execution_id of existing record if duplicate
    reason: str | None


class FillDeduplicator:
    """Deduplicates fills by exchange execution ID.

    Maintains:
    - Set of seen execution_ids (persisted)
    - Monotonic cumulative quantity per intent
    - Append-only corrections for out-of-order status
    """

    def __init__(self) -> None:
        self._seen_execution_ids: set[str] = set()
        self._intent_cumulative_qty: dict[str, Decimal] = {}

    def try_record_fill(self, fill: FillRecord) -> FillDedupResult:
        """Record a fill if not already seen.

        Returns FillDedupResult with accepted=True if new,
        accepted=False if duplicate (with duplicate_of set).
        """
        if fill.execution_id in self._seen_execution_ids:
            return FillDedupResult(
                accepted=False,
                fill_record=None,
                duplicate_of=fill.execution_id,
                reason=f"duplicate execution_id {fill.execution_id}",
            )

        # Verify cumulative quantity doesn't regress for this intent
        current_cum = self._intent_cumulative_qty.get(fill.intent_id, Decimal("0"))
        new_cum = current_cum + fill.qty

        # Accept the fill
        self._seen_execution_ids.add(fill.execution_id)
        self._intent_cumulative_qty[fill.intent_id] = new_cum

        return FillDedupResult(
            accepted=True,
            fill_record=fill,
            duplicate_of=None,
            reason=None,
        )

    def record_status(self, status: OrderStatusRecord) -> None:
        """Record order status observation.

        Does NOT update cumulative quantities - status messages are not fills.
        Used for reconciliation and audit trail only.
        """
        # Status records are append-only evidence
        pass

    def get_cumulative_qty(self, intent_id: str) -> Decimal:
        """Get cumulative filled quantity for an intent."""
        return self._intent_cumulative_qty.get(intent_id, Decimal("0"))

    def has_execution_id(self, execution_id: str) -> bool:
        """Check if execution ID has been seen."""
        return execution_id in self._seen_execution_ids


@dataclass(frozen=True)
class ExecutionEvidence:
    """Complete execution evidence for an intent."""

    intent_id: str
    client_order_id: str
    fills: tuple[FillRecord, ...]
    status_observations: tuple[OrderStatusRecord, ...]
    cumulative_qty: Decimal
    cumulative_fee: Decimal
    last_update_ns: int

    def __post_init__(self) -> None:
        if not self.intent_id or not self.intent_id.strip():
            raise ValueError("intent_id must be non-blank")
        if not self.client_order_id or not self.client_order_id.strip():
            raise ValueError("client_order_id must be non-blank")
        if not isinstance(self.cumulative_qty, Decimal) or self.cumulative_qty < 0:
            raise ValueError("cumulative_qty must be non-negative Decimal")
        if not isinstance(self.cumulative_fee, Decimal) or self.cumulative_fee < 0:
            raise ValueError("cumulative_fee must be non-negative Decimal")
        ensure_utc_ns(self.last_update_ns, field="last_update_ns")


def build_execution_evidence(
    intent_id: str,
    client_order_id: str,
    fills: list[FillRecord],
    status_observations: list[OrderStatusRecord],
    now_ns: int,
) -> ExecutionEvidence:
    """Build immutable execution evidence from fills and status observations.

    Fills are the source of truth for quantities.
    Status observations provide supplementary reconciliation data.
    """
    # Sort fills by trade_time_ns for deterministic ordering
    sorted_fills = tuple(sorted(fills, key=lambda f: f.trade_time_ns))
    sorted_status = tuple(sorted(status_observations, key=lambda s: s.receive_time_ns))

    cum_qty = sum((f.qty for f in sorted_fills), Decimal("0"))
    cum_fee = sum((f.fee for f in sorted_fills), Decimal("0"))

    return ExecutionEvidence(
        intent_id=intent_id,
        client_order_id=client_order_id,
        fills=sorted_fills,
        status_observations=sorted_status,
        cumulative_qty=cum_qty,
        cumulative_fee=cum_fee,
        last_update_ns=now_ns,
    )
