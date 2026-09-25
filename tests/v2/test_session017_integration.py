"""Bounded S1/S2 selection through risk-sized action and common-path replay."""

from __future__ import annotations

from decimal import Decimal

from atlas.v2.memory.repository import OpsRepository
from atlas.v2.science.costs import FundingCashflowV2
from atlas.v2.science.portfolio import PortfolioPathComponentV2, pair_common_path
from atlas.v2.science.replay import HOUR_NS, MINUTE_NS, ReplayStatusV2

from .test_session017_replay import minute, replay_context, run
from .test_session017_risk import CUTOFF, risk_case, size, source


def test_s1_s2_selection_to_risk_action_and_four_execution_paths(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo, include_s2=True)
        assert case.s2_candidate is not None
        assert len(case.candidate_set.candidates) == 2
        assert case.candidate_set.selected_candidate_id == case.candidate.candidate_id
        assert case.candidate.quantity is None and case.s2_candidate.quantity is None
        sizing = size(repo, case)
        assert sizing.quantity == Decimal("24.5")
        pending = PortfolioPathComponentV2("UNKNOWN", Decimal("-5"), source(repo, "pending-path", CUTOFF))
        funding_at = CUTOFF + HOUR_NS
        funding = FundingCashflowV2(funding_at, funding_at, Decimal("0.01"), Decimal("100"),
            source(repo, "integration-settlement", funding_at))
        cases = (
            ((minute(CUTOFF, ask="101"), minute(CUTOFF + 4 * HOUR_NS)), (), (), ReplayStatusV2.NO_FILL),
            ((minute(CUTOFF, ask_depth="0.5"), minute(CUTOFF + 4 * HOUR_NS, bid="101", ask="101")),
             (funding,), (funding_at,), ReplayStatusV2.PARTIAL_FILL),
            ((minute(CUTOFF), minute(CUTOFF + MINUTE_NS, mark_low="98", last_low="96"),
              minute(CUTOFF + 2 * MINUTE_NS, bid="97", ask="98", mark_low="97", last_low="95")),
             (), (), ReplayStatusV2.FULL_FILL),
            ((minute(CUTOFF), minute(CUTOFF + 4 * HOUR_NS, bid="110", ask="110",
              mark_low="110", mark_high="110", last_low="110", last_high="110")),
             (), (), ReplayStatusV2.FULL_FILL),
        )
        outcomes = []
        for i, (minutes, funds, expected, status) in enumerate(cases):
            context = replay_context(repo, case, minutes=minutes, funding=funds,
                expected_funding=expected, portfolio_components=(pending,), path_seed=100 + i)
            payoff = run(repo, case, context)
            assert payoff.status == status and payoff.remaining_quantity == 0
            paired = pair_common_path(repo, context[1], payoff,
                                      available_at_ns=context[4].available_at_ns)
            assert paired.horizon_end_ns == CUTOFF + 24 * HOUR_NS
            assert paired.existing_payoff == Decimal("-5")
            assert paired.combined_payoff == Decimal("-5") + payoff.payoff
            outcomes.append(payoff)
        assert outcomes[0].payoff == 0 and outcomes[0].entry is None
        assert outcomes[1].funding_cashflows == ((funding.content_hash, Decimal("-0.5")),)
        assert outcomes[2].exit_reason == "STOP_EXIT" and outcomes[2].exits[0].price == Decimal("95")
        assert outcomes[3].exit_reason == "TIME_EXIT"
        assert len(repo.artifact_entries("ActionArtifactV2")) == 1
        for forbidden in ("EvaluationArtifactV2", "TradePlanEnvelopeV2", "Approval", "Reservation", "Order"):
            assert repo.artifact_entries(forbidden) == ()
