from __future__ import annotations

from support.scanner_fixture import scanner_fixture

from atlas.scanner import AlertType, RecordingAlertTransport, run_scan_slot


def test_alerts_are_deterministic_projections_and_transport_is_a_port():
    fixture = scanner_fixture()
    slot = fixture.slots[0]
    transport = RecordingAlertTransport()
    result = run_scan_slot(slot_at_ns=slot, universe=fixture.universes[0], cheap_inputs=fixture.cheap_inputs[0],
                           policy=fixture.policy, warmup_evidence=fixture.warmup_evidence[0],
                           evaluator=fixture.evaluator, alert_transport=transport, persist=False)
    assert result.alerts
    assert all(delivery.state == "DELIVERED" for delivery in result.alert_deliveries)
    assert len(transport.deliveries) == len(result.alerts)
    trade_alerts = [alert for alert in result.alerts if alert.alert_type is AlertType.TRADE_CANDIDATE]
    assert [alert.instrument for alert in trade_alerts] == ["BTCUSDT"]
    assert any(alert.alert_type is AlertType.DEADLINE_WARMUP_FAILURE and alert.instrument == "XRPUSDT"
               for alert in result.alerts)
    assert all(alert.alert_id.startswith("alert-") for alert in result.alerts)


def test_alert_transport_has_no_order_or_credential_action():
    fixture = scanner_fixture()
    slot = fixture.slots[0]
    transport = RecordingAlertTransport()
    result = run_scan_slot(slot_at_ns=slot, universe=fixture.universes[0], cheap_inputs=fixture.cheap_inputs[0],
                           policy=fixture.policy, warmup_evidence=fixture.warmup_evidence[0],
                           evaluator=fixture.evaluator, alert_transport=transport, persist=False)
    assert not hasattr(transport, "submit_order")
    assert not hasattr(transport, "place_order")
    assert all(row.approval_requested_at_ns is None and row.entry_attempted is None
               for row in result.calendar_rows)
