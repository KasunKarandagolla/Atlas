"""Session-017 execution-aware S1/S2 replay and cash accounting."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from atlas.domain.risk import engineering_default_policy
from atlas.science.execution_replay import ReplayMinute
from atlas.v2.contracts import ArtifactEnvelope
from atlas.v2.instruments import ProductContractV2, TradingStatusV2, UniverseContractV2
from atlas.v2.math.core import common_paths
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.risk import (
    AccountRiskSnapshotV2,
    RiskPolicyV2,
    StressBoundV2,
    VenueSizingLimitsV2,
    index_risk_evidence,
    index_risk_policies,
    size_selected_candidate,
)
from atlas.v2.science.action import freeze_action
from atlas.v2.science.costs import FeeScheduleV2, FundingCashflowV2, FundingScheduleV2, index_cost_evidence
from atlas.v2.science.portfolio import ExistingPortfolioPathV2, index_existing_portfolio_path, pair_common_path
from atlas.v2.science.replay import (
    HOUR_NS,
    MINUTE_NS,
    ReplayAssumptionsV2,
    ReplayPathV2,
    ReplayStatusV2,
    index_replay_assumptions,
    index_replay_path,
    replay_action,
)
from atlas.v2.selection import SELECTION_POLICY_HASH, assemble_candidate_set
from atlas.v2.strategies.s1_trend import S1_POLICY
from atlas.v2.strategies.s2_breakout import S2_POLICY, S2ShadowCoordinator

from .test_session014_core import KEY, bar
from .test_session016_candidate_selection import evidence as scanner_evidence
from .test_session016_s2 import TRIGGER_INDEX
from .test_session016_s2 import fixture as s2_fixture
from .test_session017_risk import CUTOFF, risk_case, size, source


def minute(at, *, bid="100", ask="100", bid_depth="100", ask_depth="100",
           mark_low="100", mark_high="100", last_low="100", last_high="100"):
    return ReplayMinute(at, Decimal(bid) if bid is not None else None,
        Decimal(ask) if ask is not None else None,
        Decimal(bid_depth) if bid_depth is not None else None,
        Decimal(ask_depth) if ask_depth is not None else None,
        Decimal(mark_low), Decimal(mark_high), Decimal(last_low), Decimal(last_high))


def replay_context(repo, case, *, minutes, funding=(), expected_funding=(), closed15m=(),
                   assumptions=None, path_available=None, portfolio_components=(), path_seed=17):
    sizing = size(repo, case)
    action = freeze_action(repo, candidate=case.candidate, candidate_set=case.candidate_set,
        sizing=sizing, product=case.product, policy=S1_POLICY, v1=case.v1, v2=case.v2)
    manifest = source(repo, "scenario-manifest", CUTOFF)
    paths = common_paths(seed=path_seed, experiment_ref=source(repo, "experiment", CUTOFF),
                         decision_ref=case.candidate_set.content_hash, count=1)
    path_id = paths.path_ids[0]
    complete = source(repo, "portfolio-complete", CUTOFF)
    existing = ExistingPortfolioPathV2(path_id, manifest, CUTOFF, CUTOFF, complete, portfolio_components)
    index_existing_portfolio_path(repo, existing)
    assumptions = assumptions or ReplayAssumptionsV2(0, 0, 0, 0, Decimal("1"), Decimal("0"))
    index_replay_assumptions(repo, assumptions, CUTOFF)
    schedule = FundingScheduleV2(CUTOFF, tuple(expected_funding), not expected_funding,
        source(repo, "funding-schedule", CUTOFF))
    index_cost_evidence(repo, schedule)
    for settlement in funding:
        index_cost_evidence(repo, settlement)
    available = path_available or minutes[-1].at_ns + MINUTE_NS
    path = ReplayPathV2(path_id, manifest, available, tuple(minutes), tuple(closed15m),
                        tuple(funding), source(repo, "replay-path", CUTOFF))
    index_replay_path(repo, path)
    return action, existing, assumptions, schedule, path


def run(repo, case, context):
    action, existing, assumptions, schedule, path = context
    return replay_action(repo, action=action, candidate=case.candidate, path=path,
        existing_portfolio_ref=existing.content_hash, assumptions=assumptions,
        fee=case.fee, schedule=schedule, product=case.product, replay_cutoff_ns=path.available_at_ns)


def test_full_ioc_time_exit_fee_once_and_common_24h_cash(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        context = replay_context(repo, case, minutes=(
            minute(CUTOFF), minute(CUTOFF + 4 * HOUR_NS, bid="110", ask="110",
                                   mark_low="110", mark_high="110", last_low="110", last_high="110")))
        assert context[0].action.stop_price == case.candidate.stop_price
        assert context[0].action.management_rule == S1_POLICY.management_rule
        assert context[0].action.horizon_end_ns == CUTOFF + 4 * HOUR_NS
        result = run(repo, case, context)
        assert result.status == ReplayStatusV2.FULL_FILL
        assert result.entry is not None and result.entry.at_ns <= case.candidate.deadline_ns
        assert result.exit_reason == "TIME_EXIT"
        assert result.filled_quantity == Decimal("24.5") and result.remaining_quantity == 0
        assert len(result.exits) == 1 and result.exits[0].fee == Decimal("2.6950")
        assert result.entry is not None and result.entry.fee == Decimal("2.4500")
        assert result.payoff == Decimal("239.8550")
        paired = pair_common_path(repo, context[1], result, available_at_ns=context[4].available_at_ns)
        assert paired.horizon_end_ns == CUTOFF + 24 * HOUR_NS
        assert paired.candidate_payoff == result.payoff
        assert paired.combined_payoff == result.payoff
        assert repo.artifact_entries("EvaluationArtifactV2") == ()


def test_partial_no_fill_outside_collar_missing_depth_and_arrival(tmp_path):
    variants = (
        (minute(CUTOFF, ask="101"), ReplayStatusV2.NO_FILL),
        (minute(CUTOFF, ask_depth="0"), ReplayStatusV2.NO_FILL),
        (minute(CUTOFF, ask_depth=None), ReplayStatusV2.NOT_ESTIMABLE),
        (replace(minute(CUTOFF), available=False), ReplayStatusV2.NOT_ESTIMABLE),
        (minute(CUTOFF, ask_depth="0.5"), ReplayStatusV2.PARTIAL_FILL),
    )
    for i, (entry_minute, status) in enumerate(variants):
        with OpsRepository(tmp_path / f"ops-{i}.sqlite") as repo:
            case = risk_case(repo)
            context = replay_context(repo, case, minutes=(entry_minute,
                minute(CUTOFF + 4 * HOUR_NS, bid="101", ask="101",
                       mark_low="101", mark_high="101", last_low="101", last_high="101")))
            result = run(repo, case, context)
            assert result.status == status
            if status == ReplayStatusV2.NO_FILL:
                assert result.payoff == 0 and result.entry is None and result.exits == ()
            if status == ReplayStatusV2.PARTIAL_FILL:
                assert result.filled_quantity == Decimal("0.5")
                assert result.entry is not None and result.entry.fee == Decimal("0.0500")
                assert result.payoff == Decimal("0.3995")
    with OpsRepository(tmp_path / "arrival.sqlite") as repo:
        case = risk_case(repo)
        delayed = ReplayAssumptionsV2(MINUTE_NS // 120, 0, 0, 0, Decimal("1"), Decimal("0"))
        context = replay_context(repo, case, assumptions=delayed, minutes=(
            minute(CUTOFF, ask="100"), minute(CUTOFF + MINUTE_NS, ask="101"),
            minute(CUTOFF + 4 * HOUR_NS, bid="110", ask="110")))
        result = run(repo, case, context)
        assert result.status == ReplayStatusV2.NOT_ESTIMABLE
        assert result.exit_reason == "ENTRY_EXECUTION_RESOLUTION_EXCEEDS_DEADLINE"
    with OpsRepository(tmp_path / "late.sqlite") as repo:
        case = risk_case(repo)
        late = ReplayAssumptionsV2(10_000_000_000, 0, 0, 0, Decimal("1"), Decimal("0"))
        context = replay_context(repo, case, assumptions=late, minutes=(
            minute(CUTOFF), minute(CUTOFF + MINUTE_NS), minute(CUTOFF + 4 * HOUR_NS)))
        result = run(repo, case, context)
        assert result.status == ReplayStatusV2.NO_FILL
        assert result.exit_reason == "DEADLINE_EXPIRED_BEFORE_ARRIVAL"


def test_arrival_inside_deadline_but_next_complete_minute_is_too_late(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        half_second = 500_000_000
        delayed = ReplayAssumptionsV2(half_second, 0, 0, 0, Decimal("1"), Decimal("0"))
        assert CUTOFF + half_second < case.candidate.deadline_ns < CUTOFF + MINUTE_NS
        context = replay_context(repo, case, assumptions=delayed, minutes=(
            minute(CUTOFF, ask="100", ask_depth="100"),
            minute(CUTOFF + MINUTE_NS, ask="100", ask_depth="100"),
            minute(CUTOFF + 4 * HOUR_NS, bid="101", ask="101")))
        result = run(repo, case, context)
        assert result.status == ReplayStatusV2.NOT_ESTIMABLE
        assert result.exit_reason == "ENTRY_EXECUTION_RESOLUTION_EXCEEDS_DEADLINE"
        assert result.entry is None and result.payoff is None


def test_mark_stop_gap_after_latency_and_unresolved_residual(tmp_path):
    with OpsRepository(tmp_path / "stop.sqlite") as repo:
        case = risk_case(repo)
        minutes = (minute(CUTOFF),
            minute(CUTOFF + MINUTE_NS, bid="97", ask="98", mark_low="98", mark_high="101",
                   last_low="96", last_high="101"),
            minute(CUTOFF + 2 * MINUTE_NS, bid="97", ask="98", mark_low="97", mark_high="99",
                   last_low="95", last_high="99"))
        context = replay_context(repo, case, minutes=minutes)
        result = run(repo, case, context)
        assert result.exit_reason == "STOP_EXIT"
        assert result.exits[0].at_ns == CUTOFF + 2 * MINUTE_NS
        assert result.exits[0].price == Decimal("95")  # adverse OHLC bound, below frozen 99 stop
        assert result.payoff is not None and result.payoff < 0
    with OpsRepository(tmp_path / "residual.sqlite") as repo:
        case = risk_case(repo)
        context = replay_context(repo, case, minutes=(minute(CUTOFF),
            minute(CUTOFF + 4 * HOUR_NS, bid_depth="0")))
        result = run(repo, case, context)
        assert result.status == ReplayStatusV2.NOT_ESTIMABLE
        assert result.remaining_quantity == Decimal("24.5") and result.payoff is None


def test_signed_funding_on_partial_fill_and_missing_support(tmp_path):
    with OpsRepository(tmp_path / "funding.sqlite") as repo:
        case = risk_case(repo)
        at = CUTOFF + HOUR_NS
        settlement = FundingCashflowV2(at, at, Decimal("0.01"), Decimal("100"), source(repo, "settlement", at))
        context = replay_context(repo, case, funding=(settlement,), expected_funding=(at,), minutes=(
            minute(CUTOFF, ask_depth="0.5"), minute(CUTOFF + 4 * HOUR_NS, bid="101", ask="101")))
        result = run(repo, case, context)
        assert result.status == ReplayStatusV2.PARTIAL_FILL
        assert result.funding_cashflows == ((settlement.content_hash, Decimal("-0.5")),)
        assert result.payoff == Decimal("-0.1005")
    with OpsRepository(tmp_path / "missing.sqlite") as repo:
        case = risk_case(repo)
        context = replay_context(repo, case, expected_funding=(CUTOFF + HOUR_NS,), minutes=(
            minute(CUTOFF), minute(CUTOFF + 4 * HOUR_NS)))
        assert run(repo, case, context).status == ReplayStatusV2.NOT_ESTIMABLE


def test_partial_stop_exits_charge_each_fill_once_and_fund_surviving_quantity(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        settlement_at = CUTOFF + 3 * MINUTE_NS
        settlement = FundingCashflowV2(settlement_at, settlement_at, Decimal("0.01"),
            Decimal("100"), source(repo, "partial-exit-settlement", settlement_at))
        context = replay_context(repo, case, funding=(settlement,),
            expected_funding=(settlement_at,), minutes=(
                minute(CUTOFF, ask_depth="1"),
                minute(CUTOFF + MINUTE_NS, mark_low="98", mark_high="101", last_low="96"),
                minute(CUTOFF + 2 * MINUTE_NS, bid="97", ask="98", bid_depth="0.4",
                       mark_low="97", last_low="95"),
                minute(CUTOFF + 4 * MINUTE_NS, bid="97", ask="98", bid_depth="0.6",
                       mark_low="97", last_low="95")))
        result = run(repo, case, context)
        assert result.status == ReplayStatusV2.PARTIAL_FILL
        assert result.filled_quantity == Decimal("1") and result.remaining_quantity == 0
        assert tuple(x.quantity for x in result.exits) == (Decimal("0.4"), Decimal("0.6"))
        assert result.entry is not None and result.entry.fee == Decimal("0.100")
        assert tuple(x.fee for x in result.exits) == (Decimal("0.0380"), Decimal("0.0570"))
        assert result.funding_cashflows == ((settlement.content_hash, Decimal("-0.6")),)
        assert result.payoff == Decimal("-5.7950")


def test_future_funding_artifact_cannot_rewrite_prior_action_or_payoff(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        context = replay_context(repo, case, minutes=(minute(CUTOFF),
            minute(CUTOFF + 4 * HOUR_NS, bid="101", ask="101")))
        action, _, _, _, path = context
        original = run(repo, case, context)
        future_at = path.available_at_ns + HOUR_NS
        future = FundingCashflowV2(future_at, future_at, Decimal("0.9"),
            Decimal("999"), source(repo, "future-funding", future_at))
        index_cost_evidence(repo, future)
        repeated = run(repo, case, context)
        assert repeated.content_hash == original.content_hash
        assert repeated.payoff == original.payoff
        assert action.action.action_hash == repeated.action_hash
        indexed_payoff = repo.get_artifact(original.content_hash)
        assert indexed_payoff is not None
        assert future.content_hash not in indexed_payoff.metadata["input_refs"]


def s2_replay_context(repo, *, first_close="101", second_close="101"):
    _, joined, feature, original_universe, quote = s2_fixture()
    metadata = source(repo, "s2-product")
    product = ProductContractV2(KEY, 0, 0, 0, Decimal("1"), Decimal("0.01"),
        Decimal("0.1"), Decimal("0.1"), TradingStatusV2.TRADING, metadata,
        max_qty=Decimal("1"))
    index_risk_evidence(repo, product)
    t = joined.cutoff_ns
    universe = UniverseContractV2(ArtifactEnvelope(1, "s2-replay-u", t, t, "fixture",
        (product.content_hash,)), "fixture", t, SELECTION_POLICY_HASH,
        (replace(original_universe.entries[0], product_ref=product.content_hash),))
    decision = S2ShadowCoordinator(repo).on_trigger_close(joined, feature, universe=universe, bbo=quote)
    assert decision.candidate is not None
    item = decision.candidate
    rank_ref = scanner_evidence(repo, item, universe, 1, available_at_ns=t, event="S2_EVENT")
    candidate_set = assemble_candidate_set(repo, universe=universe, decision_event_id="S2_EVENT",
        cutoff_ns=t, candidates=(item,), policies={S2_POLICY.policy_hash: S2_POLICY},
        scanner_evidence_refs={item.candidate_id: (rank_ref,)})
    v1 = engineering_default_policy(policy_version="SESSION017_S2_FIXTURE", policy_effective_at_ns=0)
    v2 = RiskPolicyV2("SESSION017_S2_FIXTURE", 0, v1.policy_hash(),
        Decimal("0.05"), Decimal("0.02"), 1)
    complete = source(repo, "s2-account-complete", t)
    account = AccountRiskSnapshotV2("SHADOW_FAKE_ACCOUNT", t, Decimal("100000"),
        Decimal("100000"), Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"),
        Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"),
        0, (), (), (), complete)
    venue = VenueSizingLimitsV2(KEY, product.content_hash, t, (Decimal("1"),),
        Decimal("1"), Decimal("0"), source(repo, "s2-venue", t))
    stress = StressBoundV2(KEY, t, Decimal("90"), source(repo, "s2-stress", t))
    fee = FeeScheduleV2(KEY, t, Decimal("0"), Decimal("0"), source(repo, "s2-fee", t))
    index_risk_policies(repo, v1, v2)
    for artifact in (account, venue, stress):
        index_risk_evidence(repo, artifact)
    index_cost_evidence(repo, fee)
    sizing = size_selected_candidate(repo, candidate_set=candidate_set, candidate=item,
        universe=universe, policy=S2_POLICY, product=product, v1=v1, v2=v2,
        account=account, exposures=(), outcomes=(), venue=venue, stress=stress, fee=fee,
        cutoff_ns=t)
    assert sizing.quantity == Decimal("1")
    action = freeze_action(repo, candidate=item, candidate_set=candidate_set, sizing=sizing,
        product=product, policy=S2_POLICY, v1=v1, v2=v2)
    manifest = source(repo, "s2-manifest", t)
    path_id = common_paths(seed=18, experiment_ref=source(repo, "s2-experiment", t),
        decision_ref=item.content_hash, count=1).path_ids[0]
    existing = ExistingPortfolioPathV2(path_id, manifest, t, t, source(repo, "s2-portfolio", t), ())
    index_existing_portfolio_path(repo, existing)
    assumptions = ReplayAssumptionsV2(0, 0, 0, 0, Decimal("1"), Decimal("0"))
    index_replay_assumptions(repo, assumptions, t)
    schedule = FundingScheduleV2(t, (), True, source(repo, "s2-funding", t))
    index_cost_evidence(repo, schedule)
    first = bar(TRIGGER_INDEX + 1, close=first_close,
                high=str(max(Decimal(first_close), Decimal("100.3")) + Decimal("0.1")),
                low=str(min(Decimal(first_close), Decimal("99.7")) - Decimal("0.1")))
    second = bar(TRIGGER_INDEX + 2, close=second_close,
                 high=str(max(Decimal(second_close), Decimal("100.3")) + Decimal("0.1")),
                 low=str(min(Decimal(second_close), Decimal("99.7")) - Decimal("0.1")))
    return item, product, action, existing, assumptions, fee, schedule, manifest, path_id, first, second


def test_s2_failed_break_first_second_and_third_bar_exclusion(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        item, product, action, existing, assumptions, fee, schedule, manifest, path_id, first, second = s2_replay_context(
            repo, first_close="100", second_close="101")
        t = item.decision_at_ns
        mins = (minute(t, ask="100.6", bid="100.59"),
            minute(t + 15 * MINUTE_NS, bid="100", ask="100.01"),
            minute(t + 16 * MINUTE_NS, bid="99.9", ask="100"),
            minute(t + 30 * MINUTE_NS, bid="101", ask="101.01"))
        path = ReplayPathV2(path_id, manifest, t + 31 * MINUTE_NS, mins, (first, second), (),
                            source(repo, "s2-path", t))
        index_replay_path(repo, path)
        result = replay_action(repo, action=action, candidate=item, path=path,
            existing_portfolio_ref=existing.content_hash, assumptions=assumptions,
            fee=fee, schedule=schedule, product=product, replay_cutoff_ns=path.available_at_ns)
        assert result.exit_reason == "FAILED_BREAK_EXIT"
        assert result.exits[0].at_ns == t + 15 * MINUTE_NS
        assert result.payoff == Decimal("-0.6")
        scenarios = (
            (19, "101", "100", True),
            (20, "101", "101", False),
        )
        for seed, first_close, second_close, should_fail in scenarios:
            another_id = common_paths(seed=seed, experiment_ref=source(repo, f"s2-experiment-{seed}", t),
                decision_ref=item.content_hash, count=1).path_ids[0]
            another = ExistingPortfolioPathV2(another_id, manifest, t, t,
                source(repo, f"s2-portfolio-{seed}", t), ())
            index_existing_portfolio_path(repo, another)
            bars = tuple(bar(TRIGGER_INDEX + j, close=close,
                high=str(max(Decimal(close), Decimal("100.3")) + Decimal("0.1")),
                low=str(min(Decimal(close), Decimal("99.7")) - Decimal("0.1")))
                for j, close in ((1, first_close), (2, second_close), (3, "100")))
            later_minutes = (minute(t, ask="100.6", bid="100.59"),
                minute(t + 15 * MINUTE_NS, bid="101", ask="101.01"),
                minute(t + 30 * MINUTE_NS, bid="100", ask="100.01"),
                minute(t + 45 * MINUTE_NS, bid="100", ask="100.01"),
                minute(t + 2 * HOUR_NS, bid="101", ask="101.01"))
            later_path = ReplayPathV2(another_id, manifest, t + 2 * HOUR_NS + MINUTE_NS,
                later_minutes, bars, (), source(repo, f"s2-path-{seed}", t))
            index_replay_path(repo, later_path)
            later = replay_action(repo, action=action, candidate=item, path=later_path,
                existing_portfolio_ref=another.content_hash, assumptions=assumptions,
                fee=fee, schedule=schedule, product=product,
                replay_cutoff_ns=later_path.available_at_ns)
            assert later.exit_reason == ("FAILED_BREAK_EXIT" if should_fail else "TIME_EXIT")
            assert later.exits[0].at_ns == (t + 30 * MINUTE_NS if should_fail else t + 2 * HOUR_NS)
