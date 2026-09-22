"""§15/§17: complete BTC/ETH decision calendar and Phase-5 handoff artifact."""

from __future__ import annotations

import pytest
from support.phase4_factory import SLOT, bootstrap, decision_input

from atlas.science.decision_calendar import (
    DAY_NS,
    FROZEN_SLOT_HOURS,
    HOUR_NS,
    DecisionCalendar,
    DecisionCalendarRecord,
    evaluate_calendar_interval,
    evaluate_complete_calendar,
    frozen_slot_grid,
    record_from_evaluation,
)
from atlas.science.evaluation import DecisionStatus
from atlas.science.phase4_engine import evaluate_phase4
from atlas.science.research_archive import ResearchArtifactArchive

FOUR_HOURS = 4 * 3_600_000_000_000


def test_complete_calendar_covers_every_btc_eth_four_hour_slot():
    calendar = DecisionCalendar()
    slots = [SLOT, SLOT + FOUR_HOURS, SLOT + 2 * FOUR_HOURS]

    def evaluator(slot_at_ns: int, instrument: str) -> DecisionCalendarRecord:
        evaluation = evaluate_phase4(decision_input())
        return record_from_evaluation(slot_id=f"{slot_at_ns}-{instrument}", strategy_version="1.0",
                                      availability_cutoff_ns=slot_at_ns + 30_000_000_000, evaluation=evaluation,
                                      slot_at_ns=slot_at_ns, instrument=instrument,
                                      feature_snapshot_hash="snap", signal=evaluation.b0.action.value)

    records = evaluate_complete_calendar(slots, evaluator, calendar)
    assert len(records) == 6
    assert {(record.slot_at_ns, record.instrument) for record in records} == {
        (slot, instrument) for slot in slots for instrument in ("BTCUSDT", "ETHUSDT")}
    with pytest.raises(ValueError, match="four-hour slots only"):
        evaluate_complete_calendar([SLOT + 1], evaluator, calendar)


def test_one_immutable_outcome_per_slot_and_instrument():
    calendar = DecisionCalendar()
    evaluation = evaluate_phase4(decision_input())
    record = record_from_evaluation(slot_id="s", strategy_version="1.0", availability_cutoff_ns=SLOT + 1,
                                    evaluation=evaluation, slot_at_ns=SLOT, instrument="BTCUSDT",
                                    feature_snapshot_hash="snap", signal="LONG")
    assert calendar.append(record) == record
    assert calendar.append(record) == record
    conflict = DecisionCalendarRecord(**{**record.__dict__, "signal": "SHORT"})
    with pytest.raises(ValueError, match="one immutable outcome"):
        calendar.append(conflict)
    assert len(calendar.records()) == 1


def test_rejected_and_no_fill_slots_stay_in_the_calendar_with_zero_pnl():
    calendar = DecisionCalendar()
    rejected = evaluate_phase4(decision_input(snapshot=None, policy=None))
    record = record_from_evaluation(slot_id="s", strategy_version="1.0", availability_cutoff_ns=SLOT + 1,
                                    evaluation=rejected, slot_at_ns=SLOT, instrument="ETHUSDT",
                                    feature_snapshot_hash="snap", signal="FLAT")
    calendar.append(record)
    assert record.a0_status is DecisionStatus.SKIP_DATA
    assert record.pnl == "0" and record.matured_outcome_ref is None
    matured = record.mature("outcome-1", "0")
    assert matured.pnl == "0" and matured.matured_outcome_ref == "outcome-1"
    assert record.mature("outcome-1", "0") == matured
    with pytest.raises(ValueError, match="immutable decision outcome conflict"):
        matured.mature("outcome-2", "5")


def test_calendar_record_is_a_generic_phase5_handoff_without_scanner_fields():
    evaluation = evaluate_phase4(decision_input())
    record = record_from_evaluation(slot_id="slot-1", strategy_version="1.0", availability_cutoff_ns=SLOT + 1,
                                    evaluation=evaluation, slot_at_ns=SLOT, instrument="BTCUSDT",
                                    feature_snapshot_hash="snap", signal="LONG")
    fields = set(record.__dataclass_fields__)
    assert {"decision_slot_id", "instrument", "strategy_version", "availability_cutoff_ns",
            "feature_snapshot_hash", "signal", "gate_status", "gate_reasons", "risk_status", "risk_reasons",
            "scenario_support_status", "b0_status", "a0_status", "trade_plan_id", "trade_plan_hash", "reasons",
            "matured_outcome_ref"} <= fields
    forbidden = {"rank", "top_k", "exploration_candidate", "scanner_score", "rank_band", "universe_refresh"}
    assert not (fields & forbidden)
    assert record.trade_plan_id is not None and record.trade_plan_hash is not None
    assert record.a0_status is DecisionStatus.TRADE_CANDIDATE
    assert record.hash() == record.hash()


def test_calendar_persists_append_only_parquet(tmp_path):
    archive = ResearchArtifactArchive(tmp_path)
    calendar = DecisionCalendar(archive)
    evaluation = evaluate_phase4(decision_input())
    record = record_from_evaluation(slot_id="slot-1", strategy_version="1.0", availability_cutoff_ns=SLOT + 1,
                                    evaluation=evaluation, slot_at_ns=SLOT, instrument="BTCUSDT",
                                    feature_snapshot_hash="snap", signal="LONG")
    calendar.append(record)
    calendar.append(record)
    files = list((tmp_path / "decision_calendar").glob("*.parquet"))
    assert len(files) == 1
    from atlas.science.research_archive import _canonical

    assert files[0].name == data_name(_canonical(record))


def data_name(payload: str) -> str:
    import hashlib

    return f"part-{hashlib.sha256(payload.encode()).hexdigest()}.parquet"


def test_bootstrap_evidence_is_bound_into_the_record_reasons():
    evaluation = evaluate_phase4(decision_input(bootstrap=bootstrap(lcb=-1.0)))
    record = record_from_evaluation(slot_id="slot-1", strategy_version="1.0", availability_cutoff_ns=SLOT + 1,
                                    evaluation=evaluation, slot_at_ns=SLOT, instrument="BTCUSDT",
                                    feature_snapshot_hash="snap", signal="LONG")
    assert record.a0_status is DecisionStatus.NO_TRADE_NO_EDGE
    assert record.reasons and record.trade_plan_id is None


def test_frozen_slot_grid_covers_all_six_daily_slots():
    start = SLOT - (SLOT % DAY_NS)
    grid = frozen_slot_grid(start, start + DAY_NS)
    assert len(grid) == 6
    assert [(slot // HOUR_NS) % 24 for slot in grid] == list(FROZEN_SLOT_HOURS)
    assert frozen_slot_grid(start, start + 2 * DAY_NS)[-1] == start + DAY_NS + 20 * HOUR_NS
    with pytest.raises(ValueError, match="must start at UTC midnight"):
        frozen_slot_grid(start + HOUR_NS, start + DAY_NS)


def test_complete_interval_api_requires_every_slot_and_instrument():
    start = SLOT - (SLOT % DAY_NS)
    calendar = DecisionCalendar()

    def evaluator(slot_at_ns: int, instrument: str) -> DecisionCalendarRecord:
        evaluation = evaluate_phase4(decision_input())
        return record_from_evaluation(slot_id=f"{slot_at_ns}-{instrument}", strategy_version="1.0",
                                      availability_cutoff_ns=slot_at_ns + 1, evaluation=evaluation,
                                      slot_at_ns=slot_at_ns, instrument=instrument,
                                      feature_snapshot_hash="snap", signal="LONG")

    records = evaluate_calendar_interval(start, start + 2 * DAY_NS, evaluator, calendar)
    assert len(records) == 12 * 2
    assert calendar.missing(start, start + 2 * DAY_NS) == ()
    calendar.assert_complete(start, start + 2 * DAY_NS)

    # A calendar that omits the 20:00 UTC slot cannot claim completeness.
    partial = DecisionCalendar()
    for record in records:
        if (record.slot_at_ns // HOUR_NS) % 24 != 20:
            partial.append(record)
    missing = partial.missing(start, start + 2 * DAY_NS)
    assert len(missing) == 4 and all(slot % DAY_NS == 20 * HOUR_NS for slot, _ in missing)
    with pytest.raises(ValueError, match="incomplete decision calendar"):
        partial.assert_complete(start, start + 2 * DAY_NS)
