from dataclasses import replace
from decimal import Decimal as D

import pytest

from atlas.v2.research.mean_management import (
    MANAGEMENT_KINDS,
    ManagementPositionV1,
    RotationBranchV1,
    compare_future_management,
    cost_adjusted_breakeven,
    register_management_alternative,
    rotation_future_advantage,
    value_management_future,
)
from atlas.v2.science.mean_common import DAY_NS
from atlas.v2.science.mean_research import (
    ProgrammeObservationV1,
    ProgrammeProtocolV1,
    ProtectedMeanHoldoutV1,
    ResearchLedgerV1,
    SelectedPolicyResidualV1,
    advance_research_ledger,
    bootstrap_mean_interval,
    calibrate_selected_policy,
    chronological_mean_oof,
    family_programme_inference,
    holm_rejections,
    look_alpha,
    paired_programme_blocks,
    power_simulation,
    programme_inference,
    reserve_mean_holdout,
    spend_mean_holdout,
)

from .conftest import H, T, training_rows
from .test_mean_risk_cash import prepare


def protocol(**changes):
    base = ProgrammeProtocolV1(
        0,
        ("B-minus-A",),
        (H("A"), H("B")),
        ("NO_OPEN", "A", "B", "NUMERICAL", "TOOLS", "IMAGE", "M0", "M1"),
        (T,),
        7 * DAY_NS,
        (7 * DAY_NS,),
        20,
        D(".1"),
        D("1"),
        D("100"),
        D("1000"),
        H("same-information"),
        H("holdout"),
        bootstrap_resamples=1000,
    )
    return replace(base, **changes)


def test_real_block_inference_null_signal_power_and_holm():
    null, power = power_simulation(effect=1.0, sigma=1.0, blocks=40, trials=1000)
    assert 0.02 < null < 0.08 and power > 0.99
    assert holm_rejections((("a", 0.01), ("b", 0.04), ("c", 0.5)), alpha=0.05) == ("a",)
    assert sum(look_alpha(0.05, k, 1) for k in range(1, 1000)) < 0.05
    p = protocol()
    signal = programme_inference(
        tuple(2.0 for _ in range(40)), protocol=p, now_ns=T, look=1, dependence_qualified=True, power_qualified=True
    )
    assert signal["status"] == "SUPPORTED_OFFLINE_COMPARISON" and signal["capital_authority"] == "ZERO"
    negative = programme_inference(
        tuple(-0.1 for _ in range(40)), protocol=p, now_ns=T, look=1, dependence_qualified=True, power_qualified=True
    )
    assert negative["status"] == "NOT ESTIMABLE"
    for dependence, power_ok, count in ((False, True, 40), (True, False, 40), (True, True, 19)):
        assert (
            programme_inference(
                tuple(2.0 for _ in range(count)),
                protocol=p,
                now_ns=T,
                look=1,
                dependence_qualified=dependence,
                power_qualified=power_ok,
            )["status"]
            == "NOT ESTIMABLE"
        )
    with pytest.raises(ValueError, match="unplanned"):
        programme_inference(
            (2.0,) * 40, protocol=p, now_ns=T + 1, look=1, dependence_qualified=True, power_qualified=True
        )
    assert bootstrap_mean_interval((1.0, 2.0, 3.0), resamples=100, seed=1) == bootstrap_mean_interval(
        (1.0, 2.0, 3.0), resamples=100, seed=1
    )


def observations(arm, cash):
    return tuple(
        ProgrammeObservationV1(
            str(k),
            k * 7 * DAY_NS,
            k * 7 * DAY_NS + DAY_NS,
            arm,
            D(cash),
            D(".1"),
            D("0"),
            None,
            "FULL_FILL",
            "PROSPECTIVE_SHADOW",
            (H(f"bill-{arm}-{k}"),),
        )
        for k in range(40)
    )


def test_matched_population_evolving_ledgers_and_family_inference():
    a, b = observations("A", "1"), observations("B", "3")
    blocks = paired_programme_blocks(a, b, block_ns=7 * DAY_NS)
    assert len(blocks) == 40 and blocks == (2.0,) * 40
    states = []
    for arm, rows in (("A", a), ("B", b)):
        state = ResearchLedgerV1(arm, D("1000"), D("1000"), D("0"), D("0"), D("0"), (), ())
        for row in rows:
            state = advance_research_ledger(state, row, lock_delta_cash=D("0"))
        states.append(state)
    assert states[1].equity_cash - states[0].equity_cash == 80
    deposit = replace(b[0], opportunity_id="deposit", external_cashflow_cash=D("100"))
    with pytest.raises(ValueError, match="bill"):
        advance_research_ledger(states[1], deposit, lock_delta_cash=D("0"))
    deposit = replace(deposit, bill_refs=(H("newbill"),))
    updated = advance_research_ledger(states[1], deposit, lock_delta_cash=D("0"))
    assert updated.allocation_ceiling_cash == 1000
    assert updated.external_cashflow_cash == 100
    result = family_programme_inference(
        (("B-minus-A", a, b),), protocol=protocol(), now_ns=T, look=1, dependence_qualified=True, power_qualified=True
    )
    assert result["passing"] == ("B-minus-A",)
    with pytest.raises(ValueError, match="denominator"):
        paired_programme_blocks(a, b[:-1], block_ns=7 * DAY_NS)
    with pytest.raises(ValueError, match="hypotheses"):
        family_programme_inference(
            (), protocol=protocol(), now_ns=T, look=1, dependence_qualified=True, power_qualified=True
        )


def test_selected_policy_mean_error_not_payoff_quantile_or_simulation_count():
    rows = tuple(
        SelectedPolicyResidualV1(
            H("model"),
            H("policy"),
            H("cell"),
            H(f"outcome{k}"),
            k * 7 * DAY_NS,
            (k * 7 + 1) * DAY_NS,
            D("10"),
            D("5"),
            False,
        )
        for k in range(30)
    )
    args = {
        "model_ref": H("model"),
        "policy_ref": H("policy"),
        "compatibility_ref": H("cell"),
        "source_ref": H("source"),
        "cutoff_ns": T,
        "expires_at_ns": T + DAY_NS,
        "resamples": 100,
    }
    calibration = calibrate_selected_policy(rows, dependence_qualified=True, **args)
    # Declared V1 residuals stay readable but cannot establish new support.
    assert calibration.allowance_cash is None and calibration.effective_blocks == 30
    assert calibration.status == "NOT ESTIMABLE"
    assert calibrate_selected_policy(rows[:19], dependence_qualified=True, **args).allowance_cash is None
    assert calibrate_selected_policy(rows, dependence_qualified=False, **args).status == "NOT ESTIMABLE"
    with pytest.raises(ValueError, match="chronological"):
        calibrate_selected_policy((replace(rows[0], label_available_at_ns=T + 1),), dependence_qualified=True, **args)


def test_oof_only_earlier_mature_rows_purge_overlap_and_protect_holdout(case):
    rows = training_rows(H("oof"), count=40)
    result = chronological_mean_oof(rows, compatibility_ref=H("oof"), provenance="CAUSAL_REPLAY", minimum_rows=5)
    assert result
    by_ref = {r.outcome_ref: r for r in rows}
    for ref, _, trainrefs, _, _ in result:
        assert all(by_ref[t].label_available_at_ns < by_ref[ref].origin_ns - DAY_NS for t in trainrefs)
    with pytest.raises(ValueError, match="holdout"):
        chronological_mean_oof(
            rows, compatibility_ref=H("oof"), provenance="CAUSAL_REPLAY", protected_outcome_refs=(rows[0].outcome_ref,)
        )
    reservation = ProtectedMeanHoldoutV1(
        H("family"), H("cell"), H("source"), 0, 300 * DAY_NS, 350 * DAY_NS, (H("held-out"),)
    )
    reserve_mean_holdout(case.repo, reservation)
    with pytest.raises(ValueError, match="rolled"):
        reserve_mean_holdout(case.repo, replace(reservation, outcome_refs=(H("new-holdout"),)))
    p = protocol(holdout_ref=reservation.content_hash)
    spend_mean_holdout(case.repo, reservation, protocol=p, now_ns=T, look=1)
    with pytest.raises(ValueError, match="spent"):
        spend_mean_holdout(case.repo, reservation, protocol=p, now_ns=T, look=1)


def position(case, side="LONG", sunk=D("-100")):
    inp, _, _ = prepare(case)
    return inp, ManagementPositionV1(
        inp.product.content_hash,
        inp.fees.content_hash,
        inp.funding.content_hash,
        case.snapshot.content_hash,
        H("owned"),
        H("permission"),
        "epoch",
        side,
        D("2"),
        D("0"),
        D("100"),
        D("103") if side == "LONG" else D("97"),
        D("99") if side == "LONG" else D("101"),
        D("99") if side == "LONG" else D("101"),
        T + 4 * 3_600_000_000_000,
        T - 1000,
        T,
        D(".2"),
        D("0"),
        D("0"),
        sunk,
    )


def alternative(case, kind, side="LONG", **params):
    inp, p = position(case, side)
    args = {
        "requested_stop": D("100") if side == "LONG" else D("100"),
        "requested_tp": D("104") if side == "LONG" else D("96"),
        "trail_fraction": D(".01"),
        "observed_extrema": ((T, D("104") if side == "LONG" else D("97")),),
    }
    args.update(params)
    return (
        inp,
        p,
        register_management_alternative(
            p,
            kind=kind,
            product=inp.product,
            fees=inp.fees,
            registry_ref=H("registered-management"),
            source_ref=H("source"),
            **args,
        ),
    )


@pytest.mark.parametrize("kind", MANAGEMENT_KINDS)
@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_all_finite_management_kinds_bound_quantity_stop_horizon_and_units(case, kind, side):
    inp, p, a = alternative(case, kind, side)
    assert a.horizon_end_ns == p.horizon_end_ns and a.close_quantity <= p.quantity
    assert a.stop_price >= p.stop_price if side == "LONG" else a.stop_price <= p.stop_price
    if kind == "PARTIAL_TP":
        assert a.close_quantity == 1 and a.trigger_price == (D("102") if side == "LONG" else D("98"))
    if kind == "COST_ADJUSTED_BE":
        assert a.stop_price == cost_adjusted_breakeven(p, fee=inp.fees, product=inp.product)


def test_management_rounding_dust_future_trail_and_no_widening(case):
    with pytest.raises(ValueError, match="widens"):
        alternative(case, "TIGHTEN", requested_stop=D("98"))
    with pytest.raises(ValueError, match="causal"):
        alternative(case, "REGISTERED_TRAIL", observed_extrema=((T + 1, D("104")),))
    with pytest.raises(ValueError, match="causal"):
        alternative(case, "REGISTERED_TRAIL", observed_extrema=((T - 1001, D("104")),))
    with pytest.raises(ValueError, match="unregistered"):
        alternative(case, "PYRAMID")
    inp, p = position(case)
    product = replace(inp.product, min_qty=D("1.1"))
    p = replace(p, product_ref=product.content_hash)
    with pytest.raises(ValueError, match="dust"):
        register_management_alternative(
            p, kind="PARTIAL_TP", product=product, fees=inp.fees, registry_ref=H("registry"), source_ref=H("source")
        )


def test_future_hold_vs_close_sunk_loss_and_joint_rotation_win_and_expiry(case):
    inp, p, hold = alternative(case, "HOLD")
    _, _, close = alternative(case, "CLOSE")
    path = inp.scenarios.paths[0]
    h = value_management_future(p, hold, path=path, product=inp.product, fees=inp.fees, funding_schedule=inp.funding)
    c = value_management_future(p, close, path=path, product=inp.product, fees=inp.fees, funding_schedule=inp.funding)
    assert h.future_cash is not None and c.future_cash is not None
    delta = compare_future_management((h,), (c,), (D("1"),))
    p2 = replace(p, sunk_realized_cash=D("-100000"))
    h2 = value_management_future(
        p2,
        replace(hold, position_ref=p2.content_hash),
        path=path,
        product=inp.product,
        fees=inp.fees,
        funding_schedule=inp.funding,
    )
    c2 = value_management_future(
        p2,
        replace(close, position_ref=p2.content_hash),
        path=path,
        product=inp.product,
        fees=inp.fees,
        funding_schedule=inp.funding,
    )
    assert compare_future_management((h2,), (c2,), (D("1"),)) == delta
    branch = RotationBranchV1(
        path.common_path_id,
        h,
        c,
        H("replacement"),
        D("20"),
        D("50"),
        D("100"),
        T,
        T + 100,
        D(".1"),
        H("rotation-source"),
    )
    assert rotation_future_advantage(branch)[0] > 0
    expired = replace(branch, replacement_expires_at_ns=T + 1)
    assert rotation_future_advantage(expired)[1] == "REPLACEMENT_UNAVAILABLE_OR_EXPIRED"
    no_fill = replace(branch, replacement_cash=D("0"))
    assert rotation_future_advantage(no_fill)[0] == c.future_cash - D(".1") - h.future_cash


def test_materiality_power_capital_time_and_exposure_ledger():
    from atlas.v2.science.mean_research import (
        ResearchExposureV1,
        capital_time_cash_days,
        cash_return,
        effective_blocks_for_power,
        projected_research_lock,
        validate_matched_ledger_start,
    )

    assert effective_blocks_for_power(effect_cash=0.01, sigma_cash=1) > 20
    assert cash_return(net_cash=D("2"), capital_cash=D("100")) == D(".02")
    assert capital_time_cash_days(((0, D("100")), (DAY_NS, D("50"))), terminal_ns=2 * DAY_NS) == 150
    exposure = ResearchExposureV1(H("instrument"), "epoch", D("2"), D("100"), D("50"), D("1"), D("20"), H("source"))
    assert projected_research_lock((exposure,)) == 70
    ledger = ResearchLedgerV1("B", D("1000"), D("1000"), D("70"), D("0"), D("0"), (), (), (exposure,))
    with pytest.raises(ValueError, match="ledger mismatch"):
        replace(ledger, locked_cash=D("50"))
    a = replace(ledger, arm="A", locked_cash=D("0"), exposures=())
    b = replace(a, arm="B")
    assert validate_matched_ledger_start(a, b, protocol=protocol())


def test_management_funding_missingness_and_pending_uncertainty(case):
    from atlas.v2.science.costs import FundingScheduleV2, MeanFundingPaymentV1

    inp, p, hold = alternative(case, "HOLD")
    schedule = FundingScheduleV2(0, (T + 100,), False, H("management-funding"))
    p = replace(p, funding_ref=schedule.content_hash)
    hold = replace(hold, position_ref=p.content_hash)
    value = value_management_future(
        p, hold, path=inp.scenarios.paths[0], product=inp.product, fees=inp.fees, funding_schedule=schedule
    )
    assert value.future_cash is None and value.status == "NOT ESTIMABLE"
    payment = MeanFundingPaymentV1(T + 100, D(".01"), D("100"), H("management-payment"))
    path = replace(inp.scenarios.paths[0], funding=(payment,))
    known = value_management_future(p, hold, path=path, product=inp.product, fees=inp.fees, funding_schedule=schedule)
    assert known.future_cash is not None
    with pytest.raises(ValueError, match="PENDING"):
        register_management_alternative(
            replace(p, possible_closing_quantity=D(".1")),
            kind="CLOSE",
            product=inp.product,
            fees=inp.fees,
            registry_ref=H("registry"),
            source_ref=H("source"),
        )
