"""Execution record models (freeze §1.4): immutable / append-oriented typed records.

Includes: Approval, Intent, Command, Reservation, Observation,
ProtectionObservation, EconomicEvent + 32-char client order ID helper.

No exchange submission here.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .enums import (
    CommandOutcome,
    CommandType,
    LifecycleState,
    ProtectionStatus,
    ReconciliationHealth,
)
from .money import canonical_decimal_str, ensure_decimal
from .time import ensure_utc_ns

_CLIENT_ORDER_ID_RE = re.compile(r"^[0-9a-f]{32}$")


def generate_client_order_id() -> str:
    """Generate from persisted random UUID128; exactly 32 lowercase hex chars."""
    return uuid.uuid4().hex  # uuid4().hex is already 32 lowercase hex


def validate_client_order_id(value: str) -> str:
    if not isinstance(value, str) or not _CLIENT_ORDER_ID_RE.match(value):
        raise ValueError(f"client_order_id must be exactly 32 lowercase hex chars, got {value!r}")
    return value


def _nonblank(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-blank string")
    return value.strip()


@dataclass(frozen=True)
class Approval:
    approval_id: str
    user_identity: str
    plan_id: str
    plan_version: str
    approved_at_ns: int
    expires_at_ns: int
    consumed_at_ns: int | None = None

    def __post_init__(self) -> None:
        _nonblank(self.approval_id, "approval_id")
        _nonblank(self.user_identity, "user_identity")
        _nonblank(self.plan_id, "plan_id")
        _nonblank(self.plan_version, "plan_version")
        ensure_utc_ns(self.approved_at_ns, field="approved_at_ns")
        ensure_utc_ns(self.expires_at_ns, field="expires_at_ns")
        if self.expires_at_ns <= self.approved_at_ns:
            raise ValueError("approval expires_at must be after approved_at")
        if self.consumed_at_ns is not None:
            ensure_utc_ns(self.consumed_at_ns, field="consumed_at_ns")

    def is_expired(self, now_ns: int) -> bool:
        return now_ns >= self.expires_at_ns

    def is_consumed(self) -> bool:
        return self.consumed_at_ns is not None


@dataclass(frozen=True)
class Intent:
    intent_id: str
    position_epoch: int
    plan_id: str
    plan_version: str
    client_order_id: str
    writer_epoch: int
    lifecycle: LifecycleState
    protection_status: ProtectionStatus
    reconciliation_health: ReconciliationHealth
    created_at_ns: int
    state_version: int = 0

    def __post_init__(self) -> None:
        _nonblank(self.intent_id, "intent_id")
        _nonblank(self.plan_id, "plan_id")
        _nonblank(self.plan_version, "plan_version")
        validate_client_order_id(self.client_order_id)
        ensure_utc_ns(self.created_at_ns, field="created_at_ns")
        if not isinstance(self.position_epoch, int) or isinstance(self.position_epoch, bool):
            raise ValueError("position_epoch must be int")
        if self.position_epoch < 0:
            raise ValueError("position_epoch must be >= 0")
        if not isinstance(self.writer_epoch, int) or isinstance(self.writer_epoch, bool):
            raise ValueError("writer_epoch must be int")
        if self.writer_epoch < 0:
            raise ValueError("writer_epoch must be >= 0")
        if not isinstance(self.lifecycle, LifecycleState):
            raise ValueError(f"lifecycle must be LifecycleState, got {self.lifecycle!r}")
        if not isinstance(self.protection_status, ProtectionStatus):
            raise ValueError("protection_status must be ProtectionStatus")
        if not isinstance(self.reconciliation_health, ReconciliationHealth):
            raise ValueError("reconciliation_health must be ReconciliationHealth")
        if not isinstance(self.state_version, int) or isinstance(self.state_version, bool):
            raise ValueError("state_version must be int")
        if self.state_version < 0:
            raise ValueError("state_version must be >= 0")
        # Protection and reconciliation are independent dimensions: no cross-constraint.


@dataclass(frozen=True)
class Command:
    command_id: str
    intent_id: str
    command_type: CommandType
    exact_payload_hash: str
    payload: str  # serialized payload (canonical JSON string)
    expected_state_version: int
    created_at_ns: int
    send_started_at_ns: int | None
    outcome: CommandOutcome

    def __post_init__(self) -> None:
        _nonblank(self.command_id, "command_id")
        _nonblank(self.intent_id, "intent_id")
        if not isinstance(self.command_type, CommandType):
            raise ValueError("command_type must be CommandType")
        _nonblank(self.exact_payload_hash, "exact_payload_hash")
        if not isinstance(self.payload, str) or not self.payload:
            raise ValueError("payload must be a non-empty serialized string")
        if not isinstance(self.expected_state_version, int) or isinstance(
            self.expected_state_version, bool
        ):
            raise ValueError("expected_state_version must be int")
        if self.expected_state_version < 0:
            raise ValueError("expected_state_version must be >= 0")
        ensure_utc_ns(self.created_at_ns, field="created_at_ns")
        if self.send_started_at_ns is not None:
            ensure_utc_ns(self.send_started_at_ns, field="send_started_at_ns")
            if self.send_started_at_ns < self.created_at_ns:
                raise ValueError("send_started_at cannot precede created_at")
        if not isinstance(self.outcome, CommandOutcome):
            raise ValueError("outcome must be CommandOutcome")
        # UNKNOWN is preserved explicitly: no auto-resolution.
        # Verify payload hash matches payload bytes (exact hash binding).
        actual = hashlib.sha256(self.payload.encode("utf-8")).hexdigest()
        if actual != self.exact_payload_hash:
            raise ValueError("exact_payload_hash does not match payload bytes")


def make_command(
    *,
    command_id: str,
    intent_id: str,
    command_type: CommandType,
    payload_dict: dict[str, Any],
    expected_state_version: int,
    created_at_ns: int,
) -> Command:
    payload = json.dumps(payload_dict, sort_keys=True, separators=(",", ":"))
    h = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return Command(
        command_id=command_id,
        intent_id=intent_id,
        command_type=command_type,
        exact_payload_hash=h,
        payload=payload,
        expected_state_version=expected_state_version,
        created_at_ns=created_at_ns,
        send_started_at_ns=None,
        outcome=CommandOutcome.UNSENT,
    )


@dataclass(frozen=True)
class Reservation:
    reservation_id: str
    intent_id: str
    remaining_open_qty: Decimal
    normal_loss: Decimal
    stress_loss: Decimal
    notional: Decimal
    beta_adjusted_notional: Decimal
    margin: Decimal
    es_contribution: Decimal
    version: int = 1

    def __post_init__(self) -> None:
        _nonblank(self.reservation_id, "reservation_id")
        _nonblank(self.intent_id, "intent_id")
        for name in (
            "remaining_open_qty",
            "normal_loss",
            "stress_loss",
            "notional",
            "beta_adjusted_notional",
            "margin",
            "es_contribution",
        ):
            d = ensure_decimal(getattr(self, name), field=name)
            object.__setattr__(self, name, d)
        if self.remaining_open_qty < 0:
            raise ValueError("remaining_open_qty must be >= 0")
        if self.normal_loss < 0 or self.stress_loss < 0 or self.margin < 0:
            raise ValueError("budget vector loss/margin must be >= 0")
        if self.notional < 0:
            raise ValueError("notional must be >= 0 (use absolute notional; side lives on intent/plan)")
        if not isinstance(self.version, int) or isinstance(self.version, bool) or self.version < 1:
            raise ValueError("version must be int >= 1")

    def to_dict(self) -> dict[str, Any]:
        return {
            "reservation_id": self.reservation_id,
            "intent_id": self.intent_id,
            "remaining_open_qty": canonical_decimal_str(self.remaining_open_qty),
            "normal_loss": canonical_decimal_str(self.normal_loss),
            "stress_loss": canonical_decimal_str(self.stress_loss),
            "notional": canonical_decimal_str(self.notional),
            "beta_adjusted_notional": canonical_decimal_str(self.beta_adjusted_notional),
            "margin": canonical_decimal_str(self.margin),
            "es_contribution": canonical_decimal_str(self.es_contribution),
            "version": self.version,
        }


@dataclass(frozen=True)
class Observation:
    observation_id: str
    source: str
    venue_identity: str
    source_time_ns: int | None
    receive_time_ns: int
    raw_hash: str
    request_id: str | None = None
    query_interval_ns: int | None = None
    completeness: str = ""

    def __post_init__(self) -> None:
        _nonblank(self.observation_id, "observation_id")
        _nonblank(self.source, "source")
        _nonblank(self.venue_identity, "venue_identity")
        _nonblank(self.raw_hash, "raw_hash")
        if self.source_time_ns is not None:
            ensure_utc_ns(self.source_time_ns, field="source_time_ns")
        ensure_utc_ns(self.receive_time_ns, field="receive_time_ns")
        if self.query_interval_ns is not None:
            ensure_utc_ns(self.query_interval_ns, field="query_interval_ns")


@dataclass(frozen=True)
class ProtectionObservation:
    position_epoch: int
    desired_stop_version: int
    qty: Decimal
    trigger_basis: str
    stop_price: Decimal
    semantics: str
    evidence_ids: tuple[str, ...]
    observed_at_ns: int

    def __post_init__(self) -> None:
        if not isinstance(self.position_epoch, int) or isinstance(self.position_epoch, bool):
            raise ValueError("position_epoch must be int")
        if not isinstance(self.desired_stop_version, int) or isinstance(
            self.desired_stop_version, bool
        ):
            raise ValueError("desired_stop_version must be int")
        qty = ensure_decimal(self.qty, field="qty")
        object.__setattr__(self, "qty", qty)
        _nonblank(self.trigger_basis, "trigger_basis")
        sp = ensure_decimal(self.stop_price, field="stop_price")
        object.__setattr__(self, "stop_price", sp)
        if sp <= 0:
            raise ValueError("stop_price must be positive")
        _nonblank(self.semantics, "semantics")
        ensure_utc_ns(self.observed_at_ns, field="observed_at_ns")
        object.__setattr__(self, "evidence_ids", tuple(self.evidence_ids))


@dataclass(frozen=True)
class EconomicEvent:
    account: str
    venue_transaction_id: str
    currency: str
    amount: Decimal
    effective_time_ns: int
    received_at_ns: int
    event_type: str
    revision: str

    def __post_init__(self) -> None:
        _nonblank(self.account, "account")
        _nonblank(self.venue_transaction_id, "venue_transaction_id")
        _nonblank(self.currency, "currency")
        amount = ensure_decimal(self.amount, field="amount")
        object.__setattr__(self, "amount", amount)
        ensure_utc_ns(self.effective_time_ns, field="effective_time_ns")
        ensure_utc_ns(self.received_at_ns, field="received_at_ns")
        _nonblank(self.event_type, "event_type")
        _nonblank(self.revision, "revision")
