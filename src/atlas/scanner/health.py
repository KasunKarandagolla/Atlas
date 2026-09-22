"""Deterministic scanner health; no trading-safety or exchange qualification claim."""

from __future__ import annotations

from collections.abc import Sequence

from .models import (
    AlertDelivery,
    DeadlineStatus,
    ScannerCalendarRow,
    ScannerHealth,
    ScannerPolicy,
    WarmupState,
)


def _age(now_ns: int, then_ns: int | None) -> int | None:
    if then_ns is None:
        return None
    return max(0, now_ns - then_ns)


def scanner_health(*, now_ns: int, rows: Sequence[ScannerCalendarRow],
                   alert_deliveries: Sequence[AlertDelivery],
                   calendar_persistence_healthy: bool,
                   research_archive_healthy: bool,
                   phase4_evaluator_available: bool,
                   policy: ScannerPolicy,
                   universe_observed_at_ns: int | None = None,
                   cheap_scan_finished_at_ns: int | None = None,
                   deep_data_finished_at_ns: int | None = None,
                   last_completed_scan_slot: int | None = None,
                   ) -> ScannerHealth:
    missed_deadlines = sum(1 for row in rows if row.model_deadline_status is DeadlineStatus.MISSED)
    warmup_failures = sum(1 for row in rows if row.warmup_state is WarmupState.NOT_ESTIMABLE_WARMUP)
    if any(delivery.state == "FAILED" for delivery in alert_deliveries):
        delivery_state = "FAILED"
    elif alert_deliveries:
        delivery_state = "DELIVERED"
    else:
        delivery_state = "NOT_ATTEMPTED"
    degraded = (not calendar_persistence_healthy or not research_archive_healthy
                or not phase4_evaluator_available or missed_deadlines > 0
                or warmup_failures > 0 or delivery_state == "FAILED")
    return ScannerHealth(
        status="DEGRADED" if degraded else "HEALTHY",
        last_completed_scan_slot=last_completed_scan_slot,
        universe_freshness_ns=_age(now_ns, universe_observed_at_ns),
        cheap_scan_freshness_ns=_age(now_ns, cheap_scan_finished_at_ns),
        deep_data_freshness_ns=_age(now_ns, deep_data_finished_at_ns),
        calendar_persistence_healthy=calendar_persistence_healthy,
        research_archive_healthy=research_archive_healthy,
        phase4_evaluator_available=phase4_evaluator_available,
        missed_deadlines=missed_deadlines,
        warmup_failures=warmup_failures,
        alert_delivery_state=delivery_state,
        assisted_enabled=policy.assisted_enabled,
        bybit_capabilities=policy.bybit_capabilities,
    )
