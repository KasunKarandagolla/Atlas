"""§7/§8/§21: complete 24h policy replay, hand-calculated P&L, lot conservation."""

from __future__ import annotations

from decimal import Decimal

from support.phase4_factory import SLOT, policy, replay_minutes

from atlas.domain.enums import Side
from atlas.science.execution_replay import ExecutionEvidenceStatus, Fill, FillStatus, ReplayMinute, linear_pnl
from atlas.science.funding import FundingSettlement
from atlas.science.policy_replay import ReplayAssumptions, replay_policy
from atlas.strategy.policy import HORIZON_NS

ZERO = Decimal("0")
MINUTE_NS = 60_000_000_000
START_NS = SLOT
HORIZON_AT = SLOT + HORIZON_NS


def assumptions(**overrides: object) -> ReplayAssumptions:
    base: dict[str, object] = {
        "decision_to_venue_ns": 0, "human_delay_ns": 0, "tick": Decimal("0.1"),
        "taker_fee_rate": ZERO, "stop_spread_impact": ZERO,
        "time_exit_market_escalation_supported": False, "extension_bound_supported": False,
    }
    base.update(overrides)
    return ReplayAssumptions(**base)  # type: ignore[arg-type]


def path(entry: Decimal = Decimal("100"), exit_price: Decimal = Decimal("110"),
         *, depth: Decimal = Decimal("100")) -> tuple[ReplayMinute, ...]:
    return (replay_minutes((entry,), start_ns=START_NS, depth=depth)
            + replay_minutes((exit_price,), start_ns=HORIZON_AT, depth=depth))


def test_hand_calculated_long_and_short_profit_and_loss():
    # MarkPrice stop for sigma=0.01 sits at 90.7, so quiet paths never trigger it.
    long_policy = policy(side=Side.LONG, quantity=Decimal("1"), mark=Decimal("100"))
    profit = replay_policy(long_policy, path(), (), assumptions())
    assert profit.status is FillStatus.FULL_FILL
    assert profit.entry_fill is not None and profit.entry_fill.price == Decimal("100.1")
    assert profit.exit_fills[0].price == Decimal("109.9")
    assert profit.pnl == Decimal("9.8")
    assert replay_policy(long_policy, path(exit_price=Decimal("90")), (), assumptions()).pnl == Decimal("-10.2")

    short_policy = policy(side=Side.SHORT, quantity=Decimal("1"), mark=Decimal("100"))
    short_profit = replay_policy(short_policy, path(exit_price=Decimal("90")), (), assumptions())
    assert short_profit.entry_fill is not None and short_profit.entry_fill.price == Decimal("99.9")
    assert short_profit.pnl == Decimal("9.8")
    assert replay_policy(short_policy, path(exit_price=Decimal("110")), (), assumptions()).pnl == Decimal("-10.2")


def test_fees_are_charged_once_per_fill_and_lots_conserve_quantity():
    long_policy = policy(side=Side.LONG, quantity=Decimal("1"), mark=Decimal("100"))
    fee = Decimal("0.001")
    result = replay_policy(long_policy, path(), (), assumptions(taker_fee_rate=fee))
    expected_entry_fee = Decimal("100.1") * fee
    expected_exit_fee = Decimal("109.9") * fee
    assert result.entry_fill is not None and result.entry_fill.fee == expected_entry_fee
    assert sum(fill.fee for fill in result.exit_fills) == expected_exit_fee
    assert result.pnl == Decimal("109.9") - Decimal("100.1") - expected_entry_fee - expected_exit_fee
    assert result.remaining_qty == 0
    assert sum(fill.quantity for fill in result.exit_fills) == result.filled_qty


def test_partial_entry_partial_stop_and_partial_time_exit_stay_conserved():
    long_policy = policy(side=Side.LONG, quantity=Decimal("10"), mark=Decimal("100"))
    entry = replay_minutes((Decimal("100"),), start_ns=START_NS, depth=Decimal("5"))  # 10% participation -> 0.5 filled
    stop = replay_minutes((Decimal("90"),), start_ns=START_NS + MINUTE_NS, depth=Decimal("0.2"))
    execution = replay_minutes((Decimal("90"),), start_ns=START_NS + 2 * MINUTE_NS, depth=Decimal("0.2"))
    horizon = replay_minutes((Decimal("101"),), start_ns=HORIZON_AT, depth=Decimal("0.1"))
    retry = replay_minutes((Decimal("101"),), start_ns=HORIZON_AT + MINUTE_NS, depth=Decimal("0.2"))
    result = replay_policy(long_policy, entry + stop + execution + horizon + retry, (), assumptions())
    assert result.status is FillStatus.PARTIAL_FILL
    assert result.filled_qty == Decimal("0.5")
    assert result.stop_fills and result.time_exit_fills
    assert [fill.quantity for fill in result.exit_fills] == [Decimal("0.2"), Decimal("0.1"), Decimal("0.2")]
    assert sum(fill.quantity for fill in result.exit_fills) == result.filled_qty
    assert result.remaining_qty == 0
    assert result.bounded_extension is False


def test_stop_fills_at_the_adverse_gap_bound_and_never_reverses():
    long_policy = policy(side=Side.LONG, quantity=Decimal("1"), mark=Decimal("100"))
    trigger = replay_minutes((Decimal("90"),), start_ns=START_NS + MINUTE_NS)
    execution = replay_minutes((Decimal("89"),), start_ns=START_NS + 2 * MINUTE_NS)
    result = replay_policy(long_policy, replay_minutes((Decimal("100"),), start_ns=START_NS) + trigger + execution,
                           (), assumptions(stop_spread_impact=Decimal("0.5")))
    assert result.outcome_status() is FillStatus.STOP_EXIT
    # The fill uses the later execution bar, never the trigger bar's price.
    assert result.stop_fills[0].price == Decimal("88.4")  # min(bid 88.9, last_low 89) - 0.5
    assert result.pnl == Decimal("88.4") - Decimal("100.1")
    assert result.remaining_qty == 0


def test_no_fill_and_missing_execution_evidence_are_distinct():
    long_policy = policy(side=Side.LONG, quantity=Decimal("1"), mark=Decimal("100"))
    no_fill = replay_policy(long_policy, replay_minutes((Decimal("105"),), start_ns=START_NS), (), assumptions())
    assert no_fill.status is FillStatus.NO_FILL and no_fill.pnl == 0
    assert no_fill.evidence_status is ExecutionEvidenceStatus.EXECUTABLE
    blind = (ReplayMinute(START_NS, None, None, None, None, Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100")),)
    missing = replay_policy(long_policy, blind, (), assumptions())
    assert missing.status is None and missing.evidence_status is ExecutionEvidenceStatus.NO_EXECUTION_DATA
    assert missing.pnl is None and missing.entry_fill is None


def test_stop_time_race_prefers_the_stop_and_extension_is_bounded_or_not_estimable():
    long_policy = policy(side=Side.LONG, quantity=Decimal("1"), mark=Decimal("100"))
    race = (replay_minutes((Decimal("100"),), start_ns=START_NS)
            + replay_minutes((Decimal("90"),), start_ns=HORIZON_AT - MINUTE_NS)
            + replay_minutes((Decimal("100"),), start_ns=HORIZON_AT))
    # The trigger's execution minute falls at the horizon, so the frozen T+24h exit
    # applies instead of a fabricated trigger-minute stop fill.
    raced = replay_policy(long_policy, race, (), assumptions())
    assert raced.stop_fills == () and raced.outcome_status() is FillStatus.TIME_EXIT

    blocked = (replay_minutes((Decimal("100"),), start_ns=START_NS)
               + replay_minutes((Decimal("101"),), start_ns=HORIZON_AT, depth=ZERO))
    unbounded = replay_policy(long_policy, blocked, (), assumptions())
    assert unbounded.outcome_status() is FillStatus.EXTENDED_EXIT
    assert unbounded.evidence_status is ExecutionEvidenceStatus.NOT_ESTIMABLE
    assert unbounded.pnl is None

    bounded = replay_policy(long_policy, blocked, (), assumptions(extension_bound_supported=True))
    assert bounded.outcome_status() is FillStatus.EXTENDED_EXIT
    assert bounded.bounded_extension is True and bounded.remaining_qty == 0 and bounded.pnl is not None

    retry_path = blocked + replay_minutes((Decimal("101"),), start_ns=HORIZON_AT + MINUTE_NS)
    retried = replay_policy(long_policy, retry_path, (), assumptions(time_exit_market_escalation_supported=True))
    assert retried.time_exit_retry_fills and retried.outcome_status() is FillStatus.TIME_EXIT

    escalated_path = (blocked
                      + replay_minutes((Decimal("101"),), start_ns=HORIZON_AT + 3_000_000_000, depth=ZERO)
                      + replay_minutes((Decimal("99"),), start_ns=HORIZON_AT + 2 * MINUTE_NS))
    escalated = replay_policy(long_policy, escalated_path, (), assumptions(time_exit_market_escalation_supported=True))
    assert escalated.outcome_status() is FillStatus.EXTENDED_EXIT
    assert escalated.escalation_fills and escalated.remaining_qty == 0


def test_funding_is_charged_on_surviving_quantity_at_every_settlement():
    settlements = (FundingSettlement(START_NS + MINUTE_NS, Decimal("0.01"), Decimal("100")),
                   FundingSettlement(START_NS + 2 * MINUTE_NS, Decimal("0.01"), Decimal("100")))
    long_policy = policy(side=Side.LONG, quantity=Decimal("1"), mark=Decimal("100"))
    result = replay_policy(long_policy, path(), settlements, assumptions())
    assert result.funding_costs == (Decimal("1"), Decimal("1"))
    assert result.pnl == Decimal("9.8") - Decimal("2")

    short_policy = policy(side=Side.SHORT, quantity=Decimal("1"), mark=Decimal("100"))
    credited = replay_policy(short_policy, path(exit_price=Decimal("90")), settlements, assumptions())
    assert credited.funding_costs == (Decimal("-1"), Decimal("-1"))
    assert credited.pnl == Decimal("9.8") + Decimal("2")


def test_unfilled_slots_produce_no_fabricated_fills():
    result = replay_policy(policy(), (), (), assumptions())
    assert result.status is None and result.pnl is None and result.entry_fill is None
    assert result.exit_fills == () and result.remaining_qty == 0


def test_linear_pnl_sign_conventions():
    entry = Fill(Decimal("2"), Decimal("100"), Decimal("0.5"))
    exit_fill = Fill(Decimal("2"), Decimal("110"), Decimal("0.5"))
    assert linear_pnl(Side.LONG, (entry,), (exit_fill,)) == Decimal("19")
    assert linear_pnl(Side.SHORT, (entry,), (exit_fill,)) == Decimal("-21")
    assert linear_pnl(Side.LONG, (entry,), (exit_fill,), (Decimal("3"),)) == Decimal("16")


def test_entry_never_uses_a_minute_already_in_progress_at_arrival():
    long_policy = policy(side=Side.LONG, quantity=Decimal("1"), mark=Decimal("100"))
    minutes = (replay_minutes((Decimal("100"),), start_ns=START_NS)
               + replay_minutes((Decimal("100"),), start_ns=START_NS + MINUTE_NS))
    immediate = replay_policy(long_policy, minutes, (), assumptions())
    assert immediate.entry_fill is not None and immediate.entry_fill.at_ns == START_NS
    mid_bar = replay_policy(long_policy, minutes, (), assumptions(decision_to_venue_ns=30_000_000_000))
    assert mid_bar.entry_fill is not None and mid_bar.entry_fill.at_ns == START_NS + MINUTE_NS
    assert mid_bar.entry_fill.price == Decimal("100.1")


def test_stop_latency_delays_execution_and_can_miss_the_horizon():
    long_policy = policy(side=Side.LONG, quantity=Decimal("1"), mark=Decimal("100"))
    entry = replay_minutes((Decimal("100"),), start_ns=START_NS)
    trigger = replay_minutes((Decimal("90"),), start_ns=START_NS + MINUTE_NS)
    later = replay_minutes((Decimal("89"),), start_ns=START_NS + 4 * MINUTE_NS)
    delayed = replay_policy(long_policy, entry + trigger + later, (),
                            assumptions(stop_latency_ns=2 * MINUTE_NS))
    # trigger + one-minute resolution + two minutes of supplied latency.
    assert delayed.stop_fills and delayed.stop_fills[0].at_ns == START_NS + 4 * MINUTE_NS
    too_slow = replay_policy(long_policy, entry + trigger + later, (),
                             assumptions(stop_latency_ns=60 * MINUTE_NS, extension_bound_supported=True))
    assert too_slow.stop_fills == ()
