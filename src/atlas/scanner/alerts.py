"""Alert-only projections of persisted scanner artifacts."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from .models import (
    AlertDelivery,
    AlertSeverity,
    AlertType,
    DeadlineStatus,
    DecisionStatus,
    ScannerAlert,
    ScannerCalendarRow,
    ScannerHealth,
    ScannerPolicy,
    WarmupState,
)


class AlertTransport(Protocol):
    def deliver(self, alert: ScannerAlert) -> AlertDelivery: ...


class RecordingAlertTransport:
    """Credential-free test/port stub; the transport is never source of truth."""

    def __init__(self, *, fail: bool = False, name: str = "recording-stub"):
        self._fail = fail
        self._name = name
        self.deliveries: list[AlertDelivery] = []

    def deliver(self, alert: ScannerAlert) -> AlertDelivery:
        delivery = AlertDelivery(alert.alert_id, "FAILED" if self._fail else "DELIVERED", self._name,
                                 "stub failure" if self._fail else None)
        self.deliveries.append(delivery)
        return delivery


def _row_ref(row: ScannerCalendarRow) -> str:
    return row.hash()


def alerts_from_rows(rows: Sequence[ScannerCalendarRow], *, policy: ScannerPolicy, created_at_ns: int,
                     health: ScannerHealth | None = None) -> tuple[ScannerAlert, ...]:
    """Deterministic alert projection; alert transport is not authoritative."""
    alerts: list[ScannerAlert] = []
    for row in sorted(rows, key=lambda item: (item.scan_slot_at_ns, item.instrument)):
        if row.plan_status == DecisionStatus.TRADE_CANDIDATE.value:
            alerts.append(ScannerAlert.create(
                alert_type=AlertType.TRADE_CANDIDATE,
                severity=AlertSeverity.WARNING,
                scan_slot_id=row.scan_slot_id,
                instrument=row.instrument,
                message_code="PHASE4_TRADE_CANDIDATE_ALERT_ONLY",
                evidence_refs=tuple(filter(None, (row.trade_plan_hash, row.phase4_evaluation_ref, _row_ref(row)))),
                created_at_ns=created_at_ns,
            ))
        if row.model_deadline_status is DeadlineStatus.MISSED:
            alerts.append(ScannerAlert.create(
                alert_type=AlertType.DEADLINE_WARMUP_FAILURE,
                severity=AlertSeverity.CRITICAL,
                scan_slot_id=row.scan_slot_id,
                instrument=row.instrument,
                message_code="WARMUP_DEADLINE_MISSED",
                evidence_refs=(_row_ref(row),),
                created_at_ns=created_at_ns,
            ))
        if (row.warmup_state is WarmupState.NOT_ESTIMABLE_WARMUP
                or row.plan_status == DecisionStatus.NOT_ESTIMABLE.value):
            alerts.append(ScannerAlert.create(
                alert_type=AlertType.NOT_ESTIMABLE,
                severity=AlertSeverity.WARNING,
                scan_slot_id=row.scan_slot_id,
                instrument=row.instrument,
                message_code=row.not_estimable_reason or "NOT_ESTIMABLE",
                evidence_refs=(_row_ref(row),),
                created_at_ns=created_at_ns,
            ))
    if policy.include_no_trade_summary:
        slots = sorted({row.scan_slot_at_ns for row in rows})
        for slot_at_ns in slots:
            slot_rows = [row for row in rows if row.scan_slot_at_ns == slot_at_ns]
            if not any(row.plan_status == DecisionStatus.TRADE_CANDIDATE.value for row in slot_rows):
                first = slot_rows[0]
                alerts.append(ScannerAlert.create(
                    alert_type=AlertType.NO_TRADE_SUMMARY,
                    severity=AlertSeverity.INFO,
                    scan_slot_id=first.scan_slot_id,
                    instrument=None,
                    message_code="NO_TRADE_CANDIDATE_IN_SLOT",
                    evidence_refs=tuple(row.hash() for row in slot_rows),
                    created_at_ns=created_at_ns,
                ))
    if health is not None and health.status != "HEALTHY":
        alerts.append(ScannerAlert.create(
            alert_type=AlertType.HEALTH_DEGRADATION,
            severity=AlertSeverity.CRITICAL,
            scan_slot_id=f"slot-{health.last_completed_scan_slot or 0}",
            instrument=None,
            message_code=f"SCANNER_HEALTH_{health.status}",
            evidence_refs=(),
            created_at_ns=created_at_ns,
        ))
    unique = {alert.alert_id: alert for alert in alerts}
    return tuple(unique[key] for key in sorted(unique))
