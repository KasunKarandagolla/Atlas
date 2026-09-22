"""Execution record models (freeze §1.4): immutable / append-oriented typed records."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .enums import CommandOutcome, CommandType, LifecycleState, ProtectionStatus, ReconciliationHealth
from .money import canonical_decimal_str, ensure_decimal
from .time import ensure_utc_ns

_CLIENT_ORDER_ID_RE = re.compile(r"^[0-9a-f]{32}$")


def generate_client_order_id() -> str:
    return uuid.uuid4().hex


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
        for f in ("approval_id", "user_identity", "plan_id", "plan_version"):
            _nonblank(getattr(self, f), f)
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
        for n in ("position_epoch", "writer_epoch", "state_version"):
            v = getattr(self, n)
            if not isinstance(v, int) or isinstance(v, bool) or v < 0:
                raise ValueError(f"{n} must be int >= 0")
        if not isinstance(self.lifecycle, LifecycleState):
            raise ValueError("lifecycle must be LifecycleState")
        if not isinstance(self.protection_status, ProtectionStatus):
            raise ValueError("protection_status must be ProtectionStatus")
        if not isinstance(self.reconciliation_health, ReconciliationHealth):
            raise ValueError("reconciliation_health must be ReconciliationHealth")


@dataclass(frozen=True)
class Command:
    command_id: str
    intent_id: str
    command_type: CommandType
    exact_payload_hash: str
    payload: str
    expected_state_version: int
    created_at_ns: int
    send_started_at_ns: int | None
    outcome: CommandOutcome

    def __post_init__(self) -> None:
        _nonblank(self.command_id, "command_id")
        _nonblank(self.intent_id, "intent_id")
        _nonblank(self.exact_payload_hash, "exact_payload_hash")
        if not isinstance(self.command_type, CommandType):
            raise ValueError("command_type must be CommandType")
        if not isinstance(self.payload, str) or not self.payload:
            raise ValueError("payload must be non-empty")
        if (
            not isinstance(self.expected_state_version, int)
            or isinstance(self.expected_state_version, bool)
            or self.expected_state_version < 0
        ):
            raise ValueError("expected_state_version must be int >= 0")
        ensure_utc_ns(self.created_at_ns, field="created_at_ns")
        if self.send_started_at_ns is not None:
            ensure_utc_ns(self.send_started_at_ns, field="send_started_at_ns")
            if self.send_started_at_ns < self.created_at_ns:
                raise ValueError("send_started_at cannot precede created_at")
        if not isinstance(self.outcome, CommandOutcome):
            raise ValueError("outcome must be CommandOutcome")
        if hashlib.sha256(self.payload.encode()).hexdigest() != self.exact_payload_hash:
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
    h = hashlib.sha256(payload.encode()).hexdigest()
    return Command(
        command_id,
        intent_id,
        command_type,
        h,
        payload,
        expected_state_version,
        created_at_ns,
        None,
        CommandOutcome.UNSENT,
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
        for n in (
            "remaining_open_qty",
            "normal_loss",
            "stress_loss",
            "notional",
            "beta_adjusted_notional",
            "margin",
            "es_contribution",
        ):
            object.__setattr__(self, n, ensure_decimal(getattr(self, n), field=n))
        if any(
            getattr(self, n) < 0 for n in ("remaining_open_qty", "normal_loss", "stress_loss", "notional", "margin")
        ):
            raise ValueError("reservation quantities/budgets must be >= 0")
        if not isinstance(self.version, int) or isinstance(self.version, bool) or self.version < 1:
            raise ValueError("version must be int >= 1")

    def to_dict(self) -> dict[str, Any]:
        return {
            **{"reservation_id": self.reservation_id, "intent_id": self.intent_id},
            **{
                n: canonical_decimal_str(getattr(self, n))
                for n in (
                    "remaining_open_qty",
                    "normal_loss",
                    "stress_loss",
                    "notional",
                    "beta_adjusted_notional",
                    "margin",
                    "es_contribution",
                )
            },
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
        for f in ("observation_id", "source", "venue_identity", "raw_hash"):
            _nonblank(getattr(self, f), f)
        if self.source_time_ns is not None:
            ensure_utc_ns(self.source_time_ns, field="source_time_ns")
        ensure_utc_ns(self.receive_time_ns, field="receive_time_ns")


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
        for n in ("position_epoch", "desired_stop_version"):
            v = getattr(self, n)
            if not isinstance(v, int) or isinstance(v, bool) or v < 0:
                raise ValueError(f"{n} must be int >= 0")
        object.__setattr__(self, "qty", ensure_decimal(self.qty, field="qty"))
        object.__setattr__(self, "stop_price", ensure_decimal(self.stop_price, field="stop_price"))
        _nonblank(self.trigger_basis, "trigger_basis")
        _nonblank(self.semantics, "semantics")
        ensure_utc_ns(self.observed_at_ns, field="observed_at_ns")
        object.__setattr__(self, "evidence_ids", tuple(self.evidence_ids))
        if self.stop_price <= 0:
            raise ValueError("stop_price must be positive")


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
        for f in ("account", "venue_transaction_id", "currency", "event_type", "revision"):
            _nonblank(getattr(self, f), f)
        object.__setattr__(self, "amount", ensure_decimal(self.amount, field="amount"))
        ensure_utc_ns(self.effective_time_ns, field="effective_time_ns")
        ensure_utc_ns(self.received_at_ns, field="received_at_ns")
