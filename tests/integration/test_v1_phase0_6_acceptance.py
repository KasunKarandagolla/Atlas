from __future__ import annotations

import json
from decimal import Decimal

import pytest
from conftest import T0
from support.assisted_control_fixture import (
    NOW_NS,
    FakeNautilusPort,
    evidence,
    make_approval,
    make_plan,
    persist_ready_recovery,
)
from support.phase4_factory import (
    decision_input,
    market_inputs,
    signal_snapshot,
)
from support.phase4_factory import (
    policy as phase4_policy,
)
from support.scanner_fixture import cheap_inputs_for, scanner_fixture, universe_for, warmup_for

from atlas.domain.enums import LifecycleState
from atlas.domain.execution import Approval
from atlas.persistence.sqlite import PersistenceError, SQLiteJournal
from atlas.runtime.assisted_control import (
    AssistedControlShell,
    DispatchAck,
    _dispatch_with_port,
)
from atlas.scanner import Phase4HandoffRequest, WarmupState, evaluator_from_phase4, run_scan_slot
from atlas.science.evaluation import DecisionStatus
from atlas.science.phase4_engine import evaluate_phase4
from atlas.strategy.crypto_trend_24h_v1 import Signal


def _phase4_input(request: Phase4HandoffRequest):
    slot = request.slot_at_ns
    signal = Signal.LONG if request.instrument == "BTCUSDT" else Signal.FLAT
    return decision_input(
        snapshot=signal_snapshot(signal, instrument=request.instrument, slot_at_ns=slot),
        policy=phase4_policy(slot_at_ns=slot),
        now_ns=slot + 1_000_000,
        market_inputs=market_inputs(now_ns=slot + 1_000_000),
        plan_id=f"plan-{request.instrument}",
        availability_cutoff_ns=slot + 30_000_000_000,
    )


def test_research_scanner_alert_handoff_preserves_phase4_identity_without_capital_side_effects(tmp_path):
    slot = T0
    fixture = scanner_fixture()
    calls: list[tuple[int, str]] = []

    def build(request: Phase4HandoffRequest):
        calls.append((request.slot_at_ns, request.instrument))
        return _phase4_input(request)

    result = run_scan_slot(
        slot_at_ns=slot,
        universe=universe_for(slot),
        cheap_inputs=cheap_inputs_for(slot),
        policy=fixture.policy,
        warmup_evidence=warmup_for(slot),
        evaluator=evaluator_from_phase4(build),
        persist=False,
    )
    assert [instrument for _, instrument in calls] == ["BTCUSDT", "ETHUSDT"]
    btc = next(row for row in result.calendar_rows if row.instrument == "BTCUSDT")
    eth = next(row for row in result.calendar_rows if row.instrument == "ETHUSDT")
    baseline = evaluate_phase4(_phase4_input(
        Phase4HandoffRequest(slot, "BTCUSDT", slot, result.universe.hash(), "cheap", WarmupState.WARM_AVAILABLE)))
    assert baseline.trade_plan is not None
    assert btc.plan_status == DecisionStatus.TRADE_CANDIDATE.value
    assert btc.trade_plan_hash == baseline.trade_plan.plan_hash()
    assert btc.phase4_evaluation_ref == baseline.evidence.artifact_hash()
    assert eth.plan_status == DecisionStatus.NO_SIGNAL.value
    assert any(alert.alert_type.value == "TRADE_CANDIDATE"
               and alert.evidence_refs and btc.trade_plan_hash in alert.evidence_refs for alert in result.alerts)
    journal = SQLiteJournal(tmp_path / "acceptance.db")
    assert (journal.count("approvals"), journal.count("intents"), journal.count("reservations"),
            journal.count("commands")) == (0, 0, 0, 0)
    journal.close()


def test_tradeplan_to_phase6_handoff_persists_durable_state_and_blocks_external_dispatch(tmp_path):
    slot = T0
    baseline = evaluate_phase4(_phase4_input(
        Phase4HandoffRequest(slot, "BTCUSDT", slot, "universe", "cheap", WarmupState.WARM_AVAILABLE)))
    plan = baseline.trade_plan
    assert plan is not None
    journal = SQLiteJournal(tmp_path / "handoff.db")
    persist_ready_recovery(journal)
    journal.create_trade_plan(plan)
    approval = Approval("approval-handoff", "user-1", plan.plan_id, plan.version, T0, T0 + 50_000_000_000)
    journal.create_approval(approval)
    mark = plan.reference_price or Decimal("100")
    shell = AssistedControlShell(journal=journal, runtime_instance_id="runtime", writer_id="writer",
                                 writer_epoch=1)
    result = shell.prepare_entry(
        plan=plan,
        approval_id=approval.approval_id,
        user_identity="user-1",
        evidence=evidence(plan, now_ns=T0 + 10_000_000_000, account_scope=plan.account_scope,
                          bid=mark - Decimal("0.1"), ask=mark + Decimal("0.1"), mark=mark),
    )
    assert result.status == "DISPATCH_BLOCKED"
    assert result.intent is not None and result.command is not None
    assert journal.load_approval(approval.approval_id).consumed_at_ns == T0 + 10_000_000_000
    assert journal.load_trade_plan(plan.plan_id).plan_hash() == plan.plan_hash()
    assert result.intent.plan_id == plan.plan_id and result.intent.plan_version == plan.version
    assert result.intent.position_epoch == 1
    assert journal.load_reservation(result.intent.intent_id).remaining_open_qty == plan.qty_limit
    assert result.command.send_started_at_ns is None
    assert json.loads(result.command.payload)["orderLinkId"] == result.intent.client_order_id
    journal.close()


def test_unknown_recovery_handoff_retains_identity_and_no_premature_release(tmp_path):
    journal = SQLiteJournal(tmp_path / "unknown.db")
    persist_ready_recovery(journal)
    plan = make_plan(journal)
    approval = make_approval(journal, plan)
    shell = AssistedControlShell(journal=journal, runtime_instance_id="runtime", writer_id="writer",
                                 writer_epoch=1)
    prepared = shell.prepare_entry(plan=plan, approval_id=approval.approval_id, user_identity="user-1",
                                   evidence=evidence(plan))
    assert prepared.status == "DISPATCH_BLOCKED" and prepared.intent is not None and prepared.command is not None
    port = FakeNautilusPort(ack=DispatchAck.UNKNOWN, journal=journal)
    _dispatch_with_port(journal=journal, command_id=prepared.command.command_id, port=port, now_ns=NOW_NS)
    durable = journal.load_intent(prepared.intent.intent_id)
    assert durable.lifecycle is LifecycleState.SUBMIT_UNKNOWN
    assert durable.client_order_id == prepared.intent.client_order_id
    assert durable.position_epoch == prepared.intent.position_epoch
    assert journal.load_reservation(durable.intent_id).remaining_open_qty == Decimal("0.01")
    retry_approval = make_approval(journal, plan, approval_id="retry")
    retry = shell.prepare_entry(plan=plan, approval_id=retry_approval.approval_id, user_identity="user-1",
                                evidence=evidence(plan))
    assert retry.status == "REVALIDATION_BLOCKED"
    with pytest.raises(PersistenceError, match="certified-flat"):
        journal.release_reservation(intent_id=durable.intent_id, certificate_id="none", released_at_ns=NOW_NS)
    journal.close()
