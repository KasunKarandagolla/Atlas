"""Session-017 hand-calculated risk, rolling-window and evidence-binding tests."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import pytest

from atlas.domain.money import canonical_decimal_str
from atlas.domain.risk import engineering_default_policy
from atlas.v2._serialization import FrozenMap, sha256_json
from atlas.v2.contracts import ArtifactEnvelope, EligibilityStatusV2, V2Side
from atlas.v2.instruments import (
    ProductContractV2,
    StrategyEligibilityV2,
    TradingStatusV2,
    UniverseContractV2,
    UniverseEntryV2,
)
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.risk import (
    ACTUAL_CLOSE_PROVENANCE,
    DAY_NS,
    AccountRiskSnapshotV2,
    ActualClosedPositionSourceV2,
    ClosedV2Outcome,
    ExposureKind,
    OutcomeClass,
    PossibleRiskV2,
    RiskPolicyV2,
    SizingStatus,
    StressBoundV2,
    VenueSizingLimitsV2,
    index_research_evidence,
    index_risk_evidence,
    index_risk_policies,
    rolling_realized_loss,
    size_selected_candidate,
)
from atlas.v2.science.costs import FeeScheduleV2, index_cost_evidence
from atlas.v2.selection import SELECTION_POLICY_HASH, assemble_candidate_set
from atlas.v2.strategies.s1_trend import S1_POLICY
from atlas.v2.strategies.s2_breakout import S2_POLICY

from .test_session014_core import KEY
from .test_session016_candidate_selection import CUTOFF, EVENT, candidate, evidence, index

HOUR_NS = 3_600_000_000_000


def source(repo, label, at=0):
    body = {"fixture_risk_source": label, "available_at_ns": at}
    ref = sha256_json(body)
    index_research_evidence(repo, "FixtureRiskInputV1", ref, at, body)
    return ref


def actual_outcome(repo, close_at_ns, available_at_ns, pnl, *, account_scope="SHADOW_FAKE_ACCOUNT"):
    epoch = sha256_json({"actual_fixture_epoch": close_at_ns, "pnl": str(pnl), "scope": account_scope})
    shared = {
        "account_scope": account_scope,
        "position_epoch_id": epoch,
        "key": KEY.to_dict(),
        "close_at_ns": close_at_ns,
    }
    execution = {**shared, "source_system": "VENUE_RECONCILED_EXECUTION"}
    economics = {**shared, "source_system": "ACCOUNT_RECONCILED_CASH", "realized_net_pnl": canonical_decimal_str(pnl)}
    execution_ref = sha256_json(execution)
    economic_ref = sha256_json(economics)
    index_research_evidence(repo, "V2ActualExecutionCloseObservationV1", execution_ref, available_at_ns, execution)
    index_research_evidence(repo, "V2ActualAccountPnlObservationV1", economic_ref, available_at_ns, economics)
    provenance = ActualClosedPositionSourceV2(
        account_scope,
        epoch,
        KEY,
        close_at_ns,
        pnl,
        available_at_ns,
        ACTUAL_CLOSE_PROVENANCE,
        execution_ref,
        economic_ref,
    )
    index_risk_evidence(repo, provenance)
    return ClosedV2Outcome(
        close_at_ns, available_at_ns, pnl, provenance.content_hash, OutcomeClass.ACTUAL_CLOSED_POSITION
    )


def risk_case(
    repo,
    *,
    account_overrides=None,
    product_overrides=None,
    venue_overrides=None,
    stress_overrides=None,
    fee_overrides=None,
    v1_overrides=None,
    v2_overrides=None,
    outcomes=(),
    exposures=(),
    include_s2=False,
    short=False,
    claim_non_actual=False,
    cutoff_ns=None,
    universe_override=None,
    candidate_override=None,
    candidate_factory=None,
    universe_available_at_ns=None,
    product_override=None,
    additional_candidates=(),
):
    cutoff = CUTOFF if cutoff_ns is None else cutoff_ns
    v1 = engineering_default_policy(policy_version="SESSION017_ENGINEERING_FIXTURE", policy_effective_at_ns=0)
    if v1_overrides:
        v1 = replace(v1, **v1_overrides)
    v2 = RiskPolicyV2("SESSION017_ENGINEERING_FIXTURE", 0, v1.policy_hash(), Decimal("0.05"), Decimal("0.02"), 1)
    if v2_overrides:
        v2 = replace(v2, **v2_overrides)
    product = product_override or ProductContractV2(
        KEY,
        0,
        0,
        0,
        Decimal("1"),
        Decimal("0.01"),
        Decimal("0.1"),
        Decimal("0.1"),
        TradingStatusV2.TRADING,
        source(repo, "product"),
        min_notional=Decimal("10"),
        max_qty=Decimal("1000"),
    )
    if product_overrides:
        product = replace(product, **product_overrides)
    key = product.key
    entry = UniverseEntryV2(
        key,
        product.content_hash,
        True,
        True,
        True,
        True,
        False,
        FrozenMap({p.policy_id: StrategyEligibilityV2(EligibilityStatusV2.ELIGIBLE) for p in (S1_POLICY, S2_POLICY)}),
        (),
    )
    universe_available = cutoff if universe_available_at_ns is None else universe_available_at_ns
    default_universe = UniverseContractV2(
        ArtifactEnvelope(1, "risk-u", universe_available, universe_available, "fixture", (product.content_hash,)),
        "fixture",
        cutoff,
        SELECTION_POLICY_HASH,
        (entry,),
    )
    universe = universe_override or default_universe
    universe_product_refs = {member.product_ref for member in universe.entries if member.key == key}
    if universe_product_refs != {product.content_hash}:
        raise ValueError("risk fixture universe must bind the exact product contract")
    if candidate_factory is not None:
        candidate_override = candidate_factory(repo, universe, product)
    shadow = candidate_override if candidate_override is not None else candidate(key=key)
    item = (
        shadow
        if candidate_override is not None
        else replace(
            shadow,
            envelope=replace(shadow.envelope, content_hash=""),
            horizon_end_ns=cutoff + 4 * HOUR_NS,
            side=V2Side.SHORT if short else shadow.side,
            entry_collar=Decimal("99.95") if short else shadow.entry_collar,
            stop_price=Decimal("101") if short else shadow.stop_price,
        )
    )
    if repo.get_artifact(item.content_hash) is None:
        index(repo, item, universe_ref=universe.content_hash)
    generated = [item, *additional_candidates]
    items = tuple(generated)
    evidence_refs = {}
    for rank, candidate_item in enumerate(items, start=1):
        if repo.get_artifact(candidate_item.content_hash) is None:
            index(repo, candidate_item, universe_ref=universe.content_hash)
        rank_ref = evidence(repo, candidate_item, universe, rank, available_at_ns=cutoff, event=EVENT)
        evidence_refs[candidate_item.candidate_id] = (rank_ref,)
    s2_item = next(
        (candidate_item for candidate_item in items if candidate_item.policy_hash == S2_POLICY.policy_hash), None
    )
    if include_s2:
        s2_item = s2_item or candidate(S2_POLICY, key=key)
        if repo.get_artifact(s2_item.content_hash) is None:
            index(repo, s2_item, universe_ref=universe.content_hash)
        evidence_refs[s2_item.candidate_id] = (
            evidence(repo, s2_item, universe, 2, available_at_ns=cutoff, event=EVENT),
        )
        if s2_item not in items:
            items += (s2_item,)
    candidate_set = assemble_candidate_set(
        repo,
        universe=universe,
        decision_event_id=EVENT,
        cutoff_ns=cutoff,
        candidates=items,
        policies={p.policy_hash: p for p in (S1_POLICY, S2_POLICY)},
        scanner_evidence_refs=evidence_refs,
    )
    venue = VenueSizingLimitsV2(
        key, product.content_hash, 0, (Decimal("1"), Decimal("2")), Decimal("2"), Decimal("0"), source(repo, "venue")
    )
    if venue_overrides:
        venue = replace(venue, **venue_overrides)
    stress = StressBoundV2(key, 0, Decimal("110") if short else Decimal("90"), source(repo, "stress"))
    if stress_overrides:
        stress = replace(stress, **stress_overrides)
    fee = FeeScheduleV2(key, 0, Decimal("0.001"), Decimal("0.001"), source(repo, "fee"))
    if fee_overrides:
        fee = replace(fee, **fee_overrides)
    for exposure in exposures:
        index_risk_evidence(repo, exposure)
    for outcome in outcomes:
        if outcome.available_at_ns <= cutoff:
            index_risk_evidence(repo, outcome)
    claimed_outcomes = tuple(
        x
        for x in outcomes
        if x.available_at_ns <= cutoff and (claim_non_actual or x.outcome_class == OutcomeClass.ACTUAL_CLOSED_POSITION)
    )
    open_items = tuple(x for x in exposures if x.kind in (ExposureKind.OPEN, ExposureKind.PARTIAL))
    pending_items = tuple(x for x in exposures if x.kind in (ExposureKind.PENDING, ExposureKind.UNKNOWN))
    account = AccountRiskSnapshotV2(
        "SHADOW_FAKE_ACCOUNT",
        cutoff,
        Decimal("100000"),
        Decimal("100000"),
        sum((x.possible_margin for x in exposures), Decimal("0")),
        Decimal("0"),
        sum((x.possible_normal_loss for x in open_items), Decimal("0")),
        sum((x.possible_normal_loss for x in pending_items), Decimal("0")),
        sum((x.possible_notional for x in exposures), Decimal("0")),
        sum((x.possible_notional for x in exposures if x.key == key), Decimal("0")),
        sum((x.signed_beta_notional for x in exposures), Decimal("0")),
        sum((x.possible_venue_collateral for x in exposures), Decimal("0")),
        Decimal("0"),
        len(pending_items) + sum(x.kind == ExposureKind.PARTIAL for x in exposures),
        tuple(x.content_hash for x in claimed_outcomes),
        tuple(x.content_hash for x in pending_items),
        tuple(x.content_hash for x in open_items),
        source(repo, "account-complete"),
    )
    if account_overrides:
        account = replace(account, **account_overrides)
    index_risk_policies(repo, v1, v2)
    for artifact in (product, venue, stress, account):
        index_risk_evidence(repo, artifact)
    index_cost_evidence(repo, fee)
    return SimpleNamespace(
        v1=v1,
        v2=v2,
        product=product,
        universe=universe,
        candidate=item,
        s2_candidate=s2_item,
        candidate_set=candidate_set,
        venue=venue,
        stress=stress,
        fee=fee,
        account=account,
        cutoff_ns=cutoff,
        outcomes=outcomes,
        exposures=exposures,
    )


def size(repo, case):
    return size_selected_candidate(
        repo,
        candidate_set=case.candidate_set,
        candidate=case.candidate,
        universe=case.universe,
        policy=S1_POLICY,
        product=case.product,
        v1=case.v1,
        v2=case.v2,
        account=case.account,
        exposures=case.exposures,
        outcomes=case.outcomes,
        venue=case.venue,
        stress=case.stress,
        fee=case.fee,
        cutoff_ns=case.cutoff_ns,
    )


def test_risk_policy_v2_strict_binding_and_hash(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        assert RiskPolicyV2.from_dict(case.v2.to_dict()) == case.v2
        assert case.v2.policy_hash != case.v1.policy_hash()
        with pytest.raises(ValueError, match="unknown"):
            RiskPolicyV2.from_dict({**case.v2.to_dict(), "unexpected": 1})
        with pytest.raises(ValueError, match="binding"):
            size(repo, SimpleNamespace(**{**vars(case), "v2": replace(case.v2, base_v1_risk_policy_hash="f" * 64)}))


def test_hand_calculated_largest_quantity_and_leverage_tie(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        decision = size(repo, case)
        assert decision.status == SizingStatus.SIZED
        # Normal/unit = 1 + .100 + .099 = 1.199; stress/unit = 10 + .100 + .090 = 10.190.
        # 0.0025 * 100000 / 10.190 = 24.53... -> 24.5 venue-rounded contracts.
        assert decision.quantity == Decimal("24.5")
        assert decision.normal_risk == Decimal("29.3755")
        assert decision.stress_risk == Decimal("249.6550")
        assert decision.notional == Decimal("2451.225")
        assert decision.notional_reference_price == Decimal("100.05")
        assert decision.leverage == Decimal("1")
        assert case.candidate.quantity is None
        assert repo.get_artifact(decision.content_hash) is not None


def test_rolling_loss_boundary_and_profit_nonnegative_allowance():
    pos = sha256_json({"position": 1})
    rows = (
        ClosedV2Outcome(CUTOFF - DAY_NS, CUTOFF - DAY_NS, Decimal("-50"), pos, OutcomeClass.ACTUAL_CLOSED_POSITION),
        ClosedV2Outcome(
            CUTOFF - DAY_NS + 1, CUTOFF - DAY_NS + 1, Decimal("-100"), pos, OutcomeClass.ACTUAL_CLOSED_POSITION
        ),
        ClosedV2Outcome(CUTOFF, CUTOFF, Decimal("20"), pos, OutcomeClass.ACTUAL_CLOSED_POSITION),
        ClosedV2Outcome(CUTOFF, CUTOFF + 1, Decimal("-500"), pos, OutcomeClass.ACTUAL_CLOSED_POSITION),
    )
    loss, refs = rolling_realized_loss(rows, CUTOFF)
    assert loss == Decimal("80")
    assert rows[0].content_hash not in refs and rows[1].content_hash in refs
    assert rows[2].content_hash in refs and rows[3].content_hash not in refs
    assert (
        rolling_realized_loss(
            (ClosedV2Outcome(CUTOFF, CUTOFF, Decimal("50"), pos, OutcomeClass.ACTUAL_CLOSED_POSITION),), CUTOFF
        )[0]
        == 0
    )


def test_future_account_product_policy_and_stress_fail_closed(tmp_path):
    for i, mutation in enumerate(
        (
            {"account_overrides": {"available_at_ns": CUTOFF + 1}},
            {"product_overrides": {"available_at_ns": CUTOFF + 1}},
            {"v2_overrides": {"effective_at_ns": CUTOFF + 1}},
            {"stress_overrides": {"available_at_ns": CUTOFF + 1}},
            {"account_overrides": {"operational_status": "STALE"}},
            {"account_overrides": {"operational_status": "UNKNOWN"}},
        )
    ):
        with OpsRepository(tmp_path / f"ops-{i}.sqlite") as repo:
            case = risk_case(repo, **mutation)
            result = size(repo, case)
            assert result.status == SizingStatus.NOT_ESTIMABLE
            assert result.quantity is None


def test_hard_risk_boundaries_and_pending_possible_exposure(tmp_path):
    cases = (
        ({"account_overrides": {"opening_intents": 1}}, "MAX_OPENING_INTENTS"),
        ({"account_overrides": {"drawdown": Decimal("0.10")}}, "DRAWDOWN_NEW_RISK_STOP"),
        ({"account_overrides": {"margin_available": Decimal("50000")}}, "MIN_SIZE_OR_HARD_RISK"),
        ({"product_overrides": {"min_qty": Decimal("30")}}, "MIN_SIZE_OR_HARD_RISK"),
        ({"product_overrides": {"min_notional": Decimal("3000")}}, "MIN_SIZE_OR_HARD_RISK"),
        ({"product_overrides": {"max_qty": Decimal("10")}}, ""),
        ({"venue_overrides": {"account_leverage_limit": Decimal("0.5")}}, "MIN_SIZE_OR_HARD_RISK"),
    )
    for i, (mutation, expected) in enumerate(cases):
        with OpsRepository(tmp_path / f"ops-{i}.sqlite") as repo:
            case = risk_case(repo, **mutation)
            decision = size(repo, case)
            if expected:
                assert decision.status == SizingStatus.NO_TRADE and expected in decision.reasons
            else:
                assert decision.status == SizingStatus.SIZED and decision.quantity == Decimal("10")
    with OpsRepository(tmp_path / "pending.sqlite") as repo:
        pending = PossibleRiskV2(
            ExposureKind.UNKNOWN,
            replace(KEY, native_symbol="ETHUSDT"),
            CUTOFF,
            Decimal("490"),
            Decimal("100"),
            Decimal("1000"),
            Decimal("1000"),
            Decimal("500"),
            Decimal("500"),
            source(repo, "pending-unknown"),
        )
        case = risk_case(repo, exposures=(pending,))
        assert case.account.pending_reserved_normal_loss == Decimal("490")
        assert size(repo, case).status == SizingStatus.NO_TRADE


def test_realized_stop_and_future_outcome_excluded(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        loss = actual_outcome(repo, CUTOFF - 1, CUTOFF, Decimal("-5001"))
        case = risk_case(repo, outcomes=(loss,))
        assert size(repo, case).reasons == ("ROLLING_REALIZED_LOSS_STOP",)
    with OpsRepository(tmp_path / "future.sqlite") as repo:
        future = actual_outcome(repo, CUTOFF, CUTOFF + 1, Decimal("-999999"))
        case = risk_case(repo, outcomes=(future,))
        assert size(repo, case).rolling_loss_consumed == Decimal("0")


def test_only_actual_reconciled_outcomes_enter_rolling_authority(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        actual_loss = actual_outcome(repo, CUTOFF - 2, CUTOFF, Decimal("-100"))
        actual_profit = actual_outcome(repo, CUTOFF - 1, CUTOFF, Decimal("20"))
        diagnostic_ref = source(repo, "simulated-position")
        simulated_loss = ClosedV2Outcome(CUTOFF, CUTOFF, Decimal("-900"), diagnostic_ref, OutcomeClass.SIMULATED)
        simulated_profit = ClosedV2Outcome(CUTOFF, CUTOFF, Decimal("1000"), diagnostic_ref, OutcomeClass.SIMULATED)
        counterfactual = ClosedV2Outcome(CUTOFF, CUTOFF, Decimal("-700"), diagnostic_ref, OutcomeClass.COUNTERFACTUAL)
        rows = (actual_loss, actual_profit, simulated_loss, simulated_profit, counterfactual)
        loss, refs = rolling_realized_loss(rows, CUTOFF)
        assert loss == Decimal("80")
        assert refs == tuple(sorted((actual_loss.content_hash, actual_profit.content_hash)))
        case = risk_case(repo, outcomes=rows)
        decision = size(repo, case)
        assert decision.status == SizingStatus.SIZED
        assert decision.rolling_loss_consumed == Decimal("80")
        assert set(case.account.closed_outcome_refs) == set(refs)
    for outcome_class in (OutcomeClass.SIMULATED, OutcomeClass.COUNTERFACTUAL):
        with OpsRepository(tmp_path / f"claimed-{outcome_class.value}.sqlite") as repo:
            ref = source(repo, "research-only-position")
            diagnostic = ClosedV2Outcome(CUTOFF, CUTOFF, Decimal("-200"), ref, outcome_class)
            case = risk_case(repo, outcomes=(diagnostic,), claim_non_actual=True)
            decision = size(repo, case)
            assert decision.status == SizingStatus.NOT_ESTIMABLE
            assert decision.reasons == ("NON_ACTUAL_ROLLING_OUTCOME",)


def test_actual_outcome_rejects_generic_or_wrong_account_provenance(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        generic = source(repo, "generic-position")
        forged = ClosedV2Outcome(CUTOFF, CUTOFF, Decimal("-10"), generic, OutcomeClass.ACTUAL_CLOSED_POSITION)
        with pytest.raises(ValueError, match="typed closed-position"):
            index_risk_evidence(repo, forged)
        actual = actual_outcome(repo, CUTOFF, CUTOFF, Decimal("-10"), account_scope="OTHER_FAKE_SCOPE")
        case = risk_case(repo, outcomes=(actual,))
        assert size(repo, case).reasons == ("ACTUAL_OUTCOME_PROVENANCE_UNAVAILABLE",)


@pytest.mark.parametrize(
    ("mutation", "quantity"),
    (
        ({"v1_overrides": {"account_gross_notional_limit": Decimal("0.01")}}, Decimal("9.9")),
        ({"v1_overrides": {"instrument_notional_limit": Decimal("0.01")}}, Decimal("9.9")),
        ({"v1_overrides": {"correlated_crypto_beta_limit": Decimal("0.01")}}, Decimal("9.9")),
        ({"v1_overrides": {"stress_loss_per_trade_frac": Decimal("0.0001")}}, Decimal("0.9")),
        ({"v1_overrides": {"portfolio_es_limit_frac": Decimal("0.0001")}}, Decimal("0.9")),
        ({"v1_overrides": {"normal_loss_per_trade_frac": Decimal("0.0001")}}, Decimal("8.3")),
        ({"v2_overrides": {"rolling_24h_new_risk_limit_frac": Decimal("0.0001")}}, Decimal("8.3")),
        ({"v1_overrides": {"venue_collateral_limit": Decimal("0.0001")}}, Decimal("0.1")),
    ),
)
def test_each_hard_limit_rounds_down_analytically(tmp_path, mutation, quantity):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        decision = size(repo, risk_case(repo, **mutation))
        assert decision.status == SizingStatus.SIZED
        assert decision.quantity == quantity


def test_largest_quantity_then_lowest_feasible_leverage(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo, account_overrides={"margin_available": Decimal("51000")})
        decision = size(repo, case)
        assert decision.quantity == Decimal("19.9")
        assert decision.leverage == Decimal("2")
    with OpsRepository(tmp_path / "capped.sqlite") as repo:
        case = risk_case(
            repo,
            account_overrides={"margin_available": Decimal("51000")},
            v1_overrides={"max_contract_leverage": Decimal("1")},
        )
        decision = size(repo, case)
        assert decision.quantity == Decimal("9.9") and decision.leverage == Decimal("1")


def test_only_selected_unexpired_unsized_candidate_can_enter_risk(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo, include_s2=True)
        assert case.s2_candidate is not None
        with pytest.raises(ValueError, match="only selected"):
            size_selected_candidate(
                repo,
                candidate_set=case.candidate_set,
                candidate=case.s2_candidate,
                universe=case.universe,
                policy=S2_POLICY,
                product=case.product,
                v1=case.v1,
                v2=case.v2,
                account=case.account,
                exposures=(),
                outcomes=(),
                venue=case.venue,
                stress=case.stress,
                fee=case.fee,
                cutoff_ns=CUTOFF,
            )
        with pytest.raises(ValueError, match="expired"):
            size_selected_candidate(
                repo,
                candidate_set=case.candidate_set,
                candidate=case.candidate,
                universe=case.universe,
                policy=S1_POLICY,
                product=case.product,
                v1=case.v1,
                v2=case.v2,
                account=case.account,
                exposures=(),
                outcomes=(),
                venue=case.venue,
                stress=case.stress,
                fee=case.fee,
                cutoff_ns=case.candidate.deadline_ns + 1,
            )


def test_existing_possible_stress_consumes_conservative_portfolio_bound(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        other_key = replace(KEY, native_symbol="ETHUSDT", base_asset_id="eth")
        exposure = PossibleRiskV2(
            ExposureKind.OPEN,
            other_key,
            CUTOFF,
            Decimal("0"),
            Decimal("200"),
            Decimal("0"),
            Decimal("0"),
            Decimal("0"),
            Decimal("0"),
            source(repo, "open-stress"),
        )
        case = risk_case(repo, exposures=(exposure,), v1_overrides={"portfolio_es_limit_frac": Decimal("0.0025")})
        decision = size(repo, case)
        assert decision.status == SizingStatus.SIZED
        assert decision.quantity == Decimal("4.9")


def test_initial_account_intent_cap_stays_one_even_if_fixture_policies_are_looser(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(
            repo,
            account_overrides={"opening_intents": 1},
            v1_overrides={"max_simultaneous_new_risk_intents": 2},
            v2_overrides={"max_opening_intents_per_account": 2},
        )
        assert size(repo, case).reasons == ("MAX_OPENING_INTENTS",)


@pytest.mark.parametrize("binding", ("gross", "instrument", "beta", "margin"))
def test_short_notional_caps_use_bid_reference_above_sell_collar(tmp_path, binding):
    with OpsRepository(tmp_path / f"{binding}.sqlite") as repo:
        cap = Decimal("0.009995")  # 999.5 on 100,000 eligible equity.
        v1_field = {
            "gross": "account_gross_notional_limit",
            "instrument": "instrument_notional_limit",
            "beta": "correlated_crypto_beta_limit",
        }.get(binding)
        case = risk_case(
            repo,
            short=True,
            v1_overrides={v1_field: cap} if v1_field is not None else None,
            account_overrides={"margin_available": Decimal("50999.5")} if binding == "margin" else None,
            venue_overrides={"allowed_leverages": (Decimal("1"),), "account_leverage_limit": Decimal("1")}
            if binding == "margin"
            else None,
        )
        assert case.candidate.side == V2Side.SHORT
        assert case.candidate.entry_reference == Decimal("100")
        assert case.candidate.entry_collar == Decimal("99.95")
        assert Decimal("10") * case.candidate.entry_collar == Decimal("999.50")
        decision = size(repo, case)
        assert decision.status == SizingStatus.SIZED
        assert decision.notional_reference_price == Decimal("100")
        assert decision.quantity == Decimal("9.9")
        assert decision.notional == Decimal("990")
        assert decision.margin == Decimal("990")
        assert decision.normal_risk == Decimal("11.8899")
        assert decision.stress_risk == Decimal("101.079")
