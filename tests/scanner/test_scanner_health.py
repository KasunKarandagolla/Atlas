from __future__ import annotations

from support.scanner_fixture import EPOCH_NS, scanner_fixture

from atlas.scanner import AlertDelivery, scanner_health


def test_health_exposes_required_state_and_never_claims_trading_safety():
    fixture = scanner_fixture()
    health = scanner_health(now_ns=EPOCH_NS, rows=(), alert_deliveries=(),
                            calendar_persistence_healthy=True, research_archive_healthy=True,
                            phase4_evaluator_available=True,
                            policy=fixture.policy, universe_observed_at_ns=EPOCH_NS - 3_600_000_000_000,
                            cheap_scan_finished_at_ns=EPOCH_NS - 1_000_000_000,
                            deep_data_finished_at_ns=EPOCH_NS - 2_000_000_000,
                            last_completed_scan_slot=EPOCH_NS)
    assert health.status == "HEALTHY"
    assert health.last_completed_scan_slot == EPOCH_NS
    assert health.universe_freshness_ns == 3_600_000_000_000
    assert health.assisted_enabled is False
    assert health.bybit_capabilities == "UNVERIFIED"
    assert health.trading_safety_claimed is False


def test_health_degrades_on_calendar_archive_evaluator_deadline_or_alert_failure():
    fixture = scanner_fixture()
    health = scanner_health(now_ns=EPOCH_NS, rows=(), alert_deliveries=(AlertDelivery("a", "FAILED", "stub"),),
                            calendar_persistence_healthy=False, research_archive_healthy=False,
                            phase4_evaluator_available=False, policy=fixture.policy)
    assert health.status == "DEGRADED"
    assert health.alert_delivery_state == "FAILED"
