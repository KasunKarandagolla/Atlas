from __future__ import annotations

from support.scanner_fixture import EPOCH_NS, HOUR_NS, warmup_for

from atlas.scanner import DeadlineStatus, WarmupEvidence, WarmupState, evaluate_warmup


def test_warmup_available_before_deadline_is_met():
    slot = EPOCH_NS
    evidence = {item.instrument: item for item in warmup_for(slot)}
    statuses = {item.instrument: item for item in evaluate_warmup(
        slot_at_ns=slot, requested_instruments=("BTCUSDT", "ETHUSDT"), evidence=evidence,
        deadline_at_ns=slot + HOUR_NS // 2, observed_instruments=("BTCUSDT", "ETHUSDT", "SOLUSDT"))}
    assert statuses["BTCUSDT"].state is WarmupState.WARM_AVAILABLE
    assert statuses["BTCUSDT"].deadline_status is DeadlineStatus.MET
    assert statuses["SOLUSDT"].state is WarmupState.NOT_SELECTED
    assert statuses["SOLUSDT"].deadline_status is DeadlineStatus.NOT_APPLICABLE


def test_missing_warmup_and_late_completion_remain_visible():
    slot = EPOCH_NS
    late = WarmupEvidence("ETHUSDT", slot, True, slot - HOUR_NS, slot - HOUR_NS // 2,
                          slot + HOUR_NS, "LATE")
    statuses = {item.instrument: item for item in evaluate_warmup(
        slot_at_ns=slot, requested_instruments=("BTCUSDT", "ETHUSDT"), evidence={"ETHUSDT": late},
        deadline_at_ns=slot + 30_000_000_000, observed_instruments=("BTCUSDT", "ETHUSDT"))}
    assert statuses["BTCUSDT"].state is WarmupState.NOT_ESTIMABLE_WARMUP
    assert statuses["BTCUSDT"].deadline_status is DeadlineStatus.MISSED
    assert statuses["ETHUSDT"].state is WarmupState.NOT_ESTIMABLE_WARMUP
    assert statuses["ETHUSDT"].deadline_status is DeadlineStatus.MISSED


def test_incomplete_warmup_is_not_substituted_by_partial_data():
    slot = EPOCH_NS
    partial = WarmupEvidence("BTCUSDT", slot, False, slot - HOUR_NS, slot - HOUR_NS // 2,
                             slot - 1_000_000_000, "L2_PARTIAL")
    status = evaluate_warmup(slot_at_ns=slot, requested_instruments=("BTCUSDT",),
                             evidence={"BTCUSDT": partial},
                             deadline_at_ns=slot + 30_000_000_000)[0]
    assert status.state is WarmupState.NOT_ESTIMABLE_WARMUP
    assert status.deadline_status is DeadlineStatus.MET
    assert status.reason == "L2_PARTIAL"
