from decimal import Decimal

import pytest

from atlas.domain.enums import Side
from atlas.domain.risk import engineering_default_policy
from atlas.risk.engine import AccountState, RiskVector, evaluate_reservation
from atlas.risk.portfolio import decision_value, empirical_es
from atlas.science.evaluation import DecisionStatus, evaluate_baseline
from atlas.science.execution_replay import Fill, FillStatus, ReplayMinute, ioc_entry, linear_pnl, stop_triggered
from atlas.science.funding import FundingSettlement, funding_cashflow
from atlas.science.residual_blocks import JointResidualHour, eligible_starts, sample_blocks, select_block_length
from atlas.science.scenarios import HORIZON_HOURS, MINUTES_PER_HOUR, PRODUCTION_PATHS, MinuteOHLC, bridge_minute
from atlas.science.uncertainty import BOOTSTRAP_INNER_PATHS, BOOTSTRAP_REPLICATES, lcb_order_statistic
from atlas.strategy.crypto_trend_24h_v1 import Signal, is_decision_slot, signal_from_z
from atlas.strategy.features import HOUR_NS, HourlyClose, feature_values, finite_window_sigma
from atlas.strategy.policy import VenueFilters, entry_collar, fixed_policy, round_down, time_exit_collar


def closes(step: Decimal = Decimal("1.0005")) -> tuple[HourlyClose, ...]:
    return tuple(HourlyClose(i * HOUR_NS, Decimal("100") * step**i, i * HOUR_NS, str(i)) for i in range(721))


def test_exact_feature_window_signal_and_deadlines():
    c = closes()
    values = feature_values(c)
    assert len(values.returns) == 720
    assert values.z > 0.5
    assert signal_from_z(0.5) is Signal.FLAT
    assert signal_from_z(-0.5) is Signal.FLAT
    assert signal_from_z(-0.50001) is Signal.SHORT
    assert is_decision_slot(c[-1].end_at_ns)
    assert not is_decision_slot(c[-1].end_at_ns + 1)
    with pytest.raises(ValueError):
        feature_values(c[:-1])
    with pytest.raises(ValueError):
        feature_values(c[:300] + c[301:])


def test_finite_ewma_floor_and_decimal_policy():
    assert finite_window_sigma([0.0] * 720) == pytest.approx(0.0001)
    filters = VenueFilters(Decimal("0.1"), Decimal("0.01"), Decimal("0.01"))
    assert entry_collar(Side.LONG, Decimal("99"), Decimal("100.03"), filters.tick) == Decimal("100.1")
    assert entry_collar(Side.SHORT, Decimal("99.97"), Decimal("101"), filters.tick) == Decimal("99.9")
    assert round_down(Decimal("1.239"), Decimal(".01")) == Decimal("1.23")
    assert time_exit_collar(Side.LONG, Decimal("100"), Decimal("101"), filters.tick) == Decimal("99.8")
    p = fixed_policy(Side.LONG, Decimal("1"), Decimal("100"), Decimal("100.1"), Decimal("100"), .01, filters, 0, 0)
    assert p.stop < p.mark_reference and p.horizon_end_ns == 24 * HOUR_NS


def test_joint_non_circular_blocks_and_production_constants():
    h = tuple(JointResidualHour(i * HOUR_NS, .1, .2, .01, .02, .01, .01, 1, 1) for i in range(80))
    assert eligible_starts(h, 72) == tuple(range(9))
    paths = sample_blocks(h, length=24, horizon_hours=25, paths=2, seed=3)
    assert len(paths) == 2 and all(len(x) == 25 for x in paths)
    assert select_block_length({24: 1, 48: 1, 72: 1}, effectively_independent_blocks={24: 2, 48: 2, 72: 2}) == 72
    assert (PRODUCTION_PATHS, HORIZON_HOURS, MINUTES_PER_HOUR) == (2048, 24, 60)
    bridged = bridge_minute(100, MinuteOHLC(100, 102, 99, 101), 99, .01, 0, .02, .1)
    assert bridged.low <= min(bridged.open, bridged.close) <= max(bridged.open, bridged.close) <= bridged.high


def test_ioc_stop_pnl_funding_and_uncertainty():
    minute = ReplayMinute(0, Decimal("100"), Decimal("101"), Decimal("10"), Decimal("10"), Decimal("98"), Decimal("102"), Decimal("97"), Decimal("103"))
    no_fill = ioc_entry(Side.LONG, Decimal("1"), Decimal("100"), minute)
    assert no_fill.entry_status is FillStatus.NO_FILL
    partial = ioc_entry(Side.LONG, Decimal("2"), Decimal("102"), minute)
    assert partial.entry_status is FillStatus.PARTIAL_FILL and partial.entry is not None
    assert stop_triggered(Side.LONG, Decimal("99"), minute)
    assert linear_pnl(Side.SHORT, (Fill(Decimal("1"), Decimal("100")),), (Fill(Decimal("1"), Decimal("90")),)) == Decimal("10")
    assert funding_cashflow(Side.LONG, Decimal("2"), FundingSettlement(1, Decimal(".01"), Decimal("100"))) == Decimal("2")
    assert funding_cashflow(Side.SHORT, Decimal("2"), FundingSettlement(1, Decimal(".01"), Decimal("100"))) == Decimal("-2")
    assert lcb_order_statistic(list(range(1, 201))) == 5
    assert (BOOTSTRAP_REPLICATES, BOOTSTRAP_INNER_PATHS) == (200, 512)


def test_es_a0_b0_and_reservations():
    assert empirical_es([0.0, 1.0, 2.0, 3.0], .75) == pytest.approx(3.0)
    assert decision_value(1, 100, [0, 0], [1, 1]) > 0
    assert evaluate_baseline(baseline="B0", signal=Signal.LONG, gates_ok=True, support_ok=True, risk_ok=True).status is DecisionStatus.TRADE_CANDIDATE
    assert evaluate_baseline(baseline="A0", signal=Signal.LONG, gates_ok=True, support_ok=True, risk_ok=True, lcb=0, j=1).status is DecisionStatus.NO_TRADE_NO_EDGE
    policy = engineering_default_policy()
    account = AccountState(Decimal("1000"), Decimal("1000"), Decimal("0"), Decimal("0"), Decimal("0"))
    candidate = RiskVector(Decimal("1"), Decimal("2"), Decimal("100"), Decimal("100"), Decimal("100"), Decimal("0"))
    assert evaluate_reservation(policy, account, (), candidate, leverage=Decimal("1")).accepted
    assert not evaluate_reservation(policy, account, (), candidate, leverage=Decimal("3")).accepted
