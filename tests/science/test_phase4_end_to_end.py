"""§18: one deterministic chronological end-to-end Phase-4 integration fixture.

This proves implementation plumbing only: 721-close warmup, 90+ day model
support, Monday refit, OOF maturation, 60-day synchronized residual support,
block selection, 4-hour decisions, rejection paths, execution outcomes, fees,
funding, A0 vs B0, TradePlan creation, and decision-calendar persistence.
It is not a profitability claim.
"""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest
from support.phase4_factory import (
    bootstrap,
    complete_stress_input,
    decision_input,
    market_inputs,
    per_unit,
    policy,
    signal_snapshot,
)
from support.phase4_fixture import (
    DAY_NS,
    EVALUATION_DAY,
    FIRST_REFIT_DAY,
    HOUR_NS,
    Phase4Fixture,
    build_fixture,
    day_ns,
)

from atlas.domain.enums import Side
from atlas.science.decision_calendar import DecisionCalendar, evaluate_complete_calendar, record_from_evaluation
from atlas.science.evaluation import DecisionStatus
from atlas.science.execution_replay import FillStatus, ReplayMinute
from atlas.science.funding import FundingSettlement
from atlas.science.gates import EventGateInput
from atlas.science.phase4_engine import Phase4Evaluation, evaluate_phase4
from atlas.science.policy_replay import ReplayAssumptions, replay_policy
from atlas.science.research_archive import ResearchArtifactArchive
from atlas.science.residual_blocks import chronological_energy_scores, eligible_starts, select_block_length
from atlas.strategy.crypto_trend_24h_v1 import Signal, build_snapshot
from atlas.strategy.policy import VenueFilters, fixed_policy

MINUTE_NS = 60_000_000_000
HORIZON_NS = 24 * HOUR_NS


@pytest.fixture(scope="module")
def phase4_fixture() -> Phase4Fixture:
    return build_fixture()


def slot_snapshot(fixture: Phase4Fixture, slot_at_ns: int, instrument: str):  # type: ignore[no-untyped-def]
    window = fixture.market.window_ending(instrument, slot_at_ns)
    return build_snapshot(instrument, slot_at_ns, slot_at_ns + 1_000_000, window)


def slot_mark(fixture: Phase4Fixture, slot_at_ns: int, instrument: str) -> Decimal:
    index = (slot_at_ns - fixture.market.closes[instrument][0].end_at_ns) // HOUR_NS
    return Decimal(str(round(float(fixture.market.closes[instrument][index].close), 2)))


def evaluate_slot(fixture: Phase4Fixture, slot_at_ns: int, instrument: str, *, mode: str = "NORMAL") -> Phase4Evaluation:
    snapshot = slot_snapshot(fixture, slot_at_ns, instrument)
    if mode == "NO_SIGNAL":
        snapshot = signal_snapshot(Signal.FLAT, instrument=instrument, slot_at_ns=slot_at_ns)
    mark = slot_mark(fixture, slot_at_ns, instrument)
    venue = VenueFilters(Decimal("0.1"), Decimal("0.01"), Decimal("0.01"))
    side = Side.LONG if snapshot.signal is not Signal.SHORT else Side.SHORT
    frozen = fixed_policy(side, Decimal("1"), mark - Decimal("0.1"), mark + Decimal("0.1"), mark,
                          max(snapshot.values.sigma, 0.003), venue, slot_at_ns, slot_at_ns)
    common: dict[str, object] = {
        "snapshot": snapshot,
        "policy": frozen,
        "stress_input": complete_stress_input(side=side, quantity=Decimal("1"), mark=mark),
        "per_unit_risk": per_unit(mark=mark),
        "bootstrap": bootstrap(lcb=1.0),
        "now_ns": slot_at_ns + 1_000_000,
        "availability_cutoff_ns": slot_at_ns + 30_000_000_000,
        "venue_maximum_quantity": Decimal("10"),
        "market_inputs": market_inputs(quantity=Decimal("10"), now_ns=slot_at_ns + 1_000_000, mark=mark),
    }
    if mode == "MARKET_GATE":
        common["market_inputs"] = replace(market_inputs(quantity=Decimal("10"), now_ns=slot_at_ns + 1_000_000, mark=mark),
                                          quote_at_ns=slot_at_ns - 5_000_000_000)
    elif mode == "EVENT_GATE":
        common["event_inputs"] = EventGateInput(True, (slot_at_ns + 1_000,))
    elif mode == "NOT_ESTIMABLE":
        common["stress_input"] = None
    elif mode == "RISK":
        from atlas.risk.engine import AccountState

        common["account"] = AccountState(Decimal("1"), Decimal("1"), Decimal("0"), Decimal("0"), Decimal("0"))
    elif mode == "NO_EDGE":
        common["bootstrap"] = bootstrap(lcb=-1.0)
    return evaluate_phase4(decision_input(**common))


def test_fixture_satisfies_the_frozen_warmup_support_and_selection_contract(phase4_fixture: Phase4Fixture) -> None:
    fixture = phase4_fixture
    assert len(fixture.market.closes["BTCUSDT"]) == 153 * 24
    assert len(fixture.market.window_ending("BTCUSDT", day_ns(EVALUATION_DAY))) == 721
    first_refit = day_ns(FIRST_REFIT_DAY)
    matured = [entry for entry in fixture.oof.entries() if entry.residual is not None]
    assert min(entry.origin_at_ns for entry in matured) >= first_refit
    assert all(entry.training_end_ns <= entry.fit_at_ns <= entry.origin_at_ns for entry in matured)
    assert (max(entry.target_at_ns for entry in matured) - min(entry.origin_at_ns for entry in matured)) >= 60 * DAY_NS
    assert fixture.selected_block in (24, 48, 72)
    assert set(fixture.energy_scores) == {24, 48, 72}
    window = fixture.training_hours[-8 * 24 :]
    assert chronological_energy_scores(window, fixture.validation_hours[:24],
                                       training_btc_sigma=fixture.training_sigma["BTCUSDT"],
                                       training_eth_sigma=fixture.training_sigma["ETHUSDT"]) == fixture.energy_scores
    independent = {length: len(eligible_starts(window, length)) for length in (24, 48, 72)}
    assert select_block_length(fixture.energy_scores, effectively_independent_blocks=independent) == fixture.selected_block
    assert len(fixture.hours) >= 60 * 24
    assert all(hour.minute_replay_complete and not hour.execution_missing for hour in fixture.hours)


def test_end_to_end_decision_calendar_and_persistence(phase4_fixture: Phase4Fixture, tmp_path) -> None:  # type: ignore[no-untyped-def]
    fixture = phase4_fixture
    archive = ResearchArtifactArchive(tmp_path)
    calendar = DecisionCalendar(archive)
    slots = [day_ns(EVALUATION_DAY) + hour * HOUR_NS for hour in (0, 4, 8, 12, 16)]
    modes = {
        ("BTCUSDT", slots[1]): "MARKET_GATE",
        ("BTCUSDT", slots[3]): "EVENT_GATE",
        ("ETHUSDT", slots[0]): "NO_SIGNAL",
        ("ETHUSDT", slots[1]): "NOT_ESTIMABLE",
        ("ETHUSDT", slots[2]): "NO_EDGE",
        ("ETHUSDT", slots[3]): "RISK",
    }

    def evaluator(slot_at_ns: int, instrument: str):  # type: ignore[no-untyped-def]
        return evaluate_slot(fixture, slot_at_ns, instrument, mode=modes.get((instrument, slot_at_ns), "NORMAL"))

    records = evaluate_complete_calendar(
        slots,
        lambda slot, instrument: record_from_evaluation(
            slot_id=f"slot-{slot}-{instrument}", strategy_version="1.0",
            availability_cutoff_ns=slot + 30_000_000_000,
            evaluation=(evaluation := evaluator(slot, instrument)), slot_at_ns=slot, instrument=instrument,
            feature_snapshot_hash=slot_snapshot(fixture, slot, instrument).snapshot_hash(),
            signal=evaluation.b0.action.value),
        calendar)

    assert len(records) == 10
    assert {(record.slot_at_ns, record.instrument) for record in records} == {
        (slot, instrument) for slot in slots for instrument in ("BTCUSDT", "ETHUSDT")}
    statuses = {record.a0_status for record in records}
    assert {DecisionStatus.TRADE_CANDIDATE, DecisionStatus.NO_SIGNAL, DecisionStatus.NO_TRADE_GATE,
            DecisionStatus.NO_TRADE_EVENT, DecisionStatus.NOT_ESTIMABLE, DecisionStatus.NO_TRADE_RISK,
            DecisionStatus.NO_TRADE_NO_EDGE} <= statuses, statuses
    plans = [record for record in records if record.trade_plan_id is not None]
    assert plans, "at least one qualified candidate must create a TradePlan"
    assert all(record.trade_plan_hash for record in plans)
    assert all(record.pnl == "0" for record in records)
    for record in records:
        if record.a0_status is not DecisionStatus.TRADE_CANDIDATE:
            assert record.reasons, "every rejection carries a reason"
    assert len(list((tmp_path / "decision_calendar").glob("*.parquet"))) == len(records)
    assert len(calendar.records()) == len(records)


def test_end_to_end_execution_lifecycles_and_funding_are_accounted(phase4_fixture: Phase4Fixture) -> None:
    del phase4_fixture
    slot = day_ns(EVALUATION_DAY) + 16 * HOUR_NS
    evaluation = evaluate_phase4(decision_input())
    assert evaluation.trade_plan is not None
    frozen = policy(side=Side.LONG, quantity=Decimal("1"), mark=Decimal("100"), sigma=0.01, slot_at_ns=slot)

    def minutes(prices: tuple[Decimal, ...], *, depth: Decimal = Decimal("100"),
                start_ns: int | None = None) -> tuple[ReplayMinute, ...]:
        start = slot if start_ns is None else start_ns
        return tuple(ReplayMinute(start + index * MINUTE_NS, price - Decimal("0.1"), price + Decimal("0.1"), depth,
                                  depth, price, price, price, price) for index, price in enumerate(prices))

    assumptions = ReplayAssumptions(0, 0, Decimal("0.1"), Decimal("0.0005"), Decimal("0"),
                                    time_exit_market_escalation_supported=True, extension_bound_supported=True)
    horizon_leg = minutes((Decimal("110"),), start_ns=slot + HORIZON_NS)
    cases = (
        (FillStatus.NO_FILL, FillStatus.NO_FILL, replay_policy(frozen, minutes((Decimal("105"),)), (), assumptions)),
        (FillStatus.FULL_FILL, FillStatus.TIME_EXIT,
         replay_policy(frozen, minutes((Decimal("100"),)) + horizon_leg, (), assumptions)),
        (FillStatus.PARTIAL_FILL, FillStatus.TIME_EXIT,
         replay_policy(frozen, minutes((Decimal("100"),), depth=Decimal("5")) + horizon_leg, (), assumptions)),
        (FillStatus.FULL_FILL, FillStatus.STOP_EXIT,
         replay_policy(frozen, minutes((Decimal("100"),)) + minutes((Decimal("88"),), start_ns=slot + MINUTE_NS), (),
                       assumptions)),
        (FillStatus.FULL_FILL, FillStatus.EXTENDED_EXIT,
         replay_policy(frozen, minutes((Decimal("100"),))
                       + minutes((Decimal("101"),), start_ns=slot + HORIZON_NS, depth=Decimal("0"))
                       + minutes((Decimal("99"),), start_ns=slot + HORIZON_NS + 3_000_000_000), (), assumptions)),
    )
    for entry_status, exit_status, result in cases:
        assert result.status is entry_status, (entry_status, result.status, result.reason)
        assert result.outcome_status() is exit_status, (exit_status, result.outcome_status(), result.reason)
        assert result.remaining_qty >= 0
        assert sum(fill.quantity for fill in result.exit_fills) == result.filled_qty - result.remaining_qty

    funding_settlements = (FundingSettlement(slot + MINUTE_NS, Decimal("0.0001"), Decimal("100")),
                           FundingSettlement(slot + 2 * MINUTE_NS, Decimal("0.0002"), Decimal("100")))
    with_funding = replay_policy(frozen, minutes((Decimal("100"),)) + horizon_leg, funding_settlements, assumptions)
    without_funding = replay_policy(frozen, minutes((Decimal("100"),)) + horizon_leg, (), assumptions)
    assert len(with_funding.funding_costs) == 2
    assert with_funding.pnl is not None and without_funding.pnl is not None
    assert with_funding.pnl == without_funding.pnl - sum(with_funding.funding_costs)
    assert sum(fill.fee for fill in with_funding.exit_fills) > 0

    calendar = DecisionCalendar()
    for index, (_, exit_status, result) in enumerate(cases):
        case_slot = slot + index * 4 * HOUR_NS
        base = record_from_evaluation(slot_id=f"lifecycle-{index}", strategy_version="1.0",
                                      availability_cutoff_ns=case_slot + 30_000_000_000, evaluation=evaluation,
                                      slot_at_ns=case_slot, instrument="BTCUSDT", feature_snapshot_hash="snap",
                                      signal="LONG")
        calendar.append(base)
        matured = calendar.mature(case_slot, "BTCUSDT", f"outcome-{index}-{exit_status.value}",
                                  str(result.pnl or 0),
                                  fill_status=result.status.value if result.status else None,
                                  exit_status=exit_status.value)
        assert matured.matured_exit_status == exit_status.value
        assert matured.pnl == str(result.pnl or 0)
        with pytest.raises(ValueError, match="immutable decision outcome conflict"):
            calendar.mature(case_slot, "BTCUSDT", "conflicting", "1", fill_status="FULL_FILL",
                            exit_status="EXTENDED_EXIT")
    stored = calendar.records()
    assert len(stored) == len(cases)
    assert {record.matured_exit_status for record in stored} == {case[1].value for case in cases}
    assert all(record.trade_plan_id is not None for record in stored)
