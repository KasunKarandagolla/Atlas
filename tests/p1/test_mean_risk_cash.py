from dataclasses import replace
from decimal import Decimal as D

import pytest

from atlas.v2.risk import SnapshotExposureV1, size_independent_candidate
from atlas.v2.science.action import ExactActionV1, freeze_independent_action
from atlas.v2.science.costs import (
    FundingScheduleV2,
    MeanCashFillV1,
    MeanCashPathV1,
    MeanFundingPaymentV1,
    value_mean_cash_path,
)
from atlas.v2.science.mean_scenarios import MeanMarketPointV1, replay_independent_action

from .conftest import H, T


def prepare(case, index=1):
    inp = list(case.inputs.values())[index - 1]
    size = size_independent_candidate(
        candidate=inp.candidate,
        product=inp.product,
        fee=inp.fees,
        snapshot=case.snapshot,
        profile=inp.risk,
        cutoff_ns=T,
        now_ns=T + 1,
    )
    action = freeze_independent_action(
        candidate=inp.candidate,
        population_ref=case.population.content_hash,
        sizing=size,
        product=inp.product,
        policy=inp.policy,
        permission_ref=case.snapshot.permission_ref,
        funding_ref=inp.funding.content_hash,
        execution_ref=inp.scenarios.execution_ref,
        model_ref=inp.model.content_hash,
        source_refs=case.population.candidates[index - 1].source_refs,
    )
    return inp, size, action


def size_with(case, **changes):
    inp = list(case.inputs.values())[0]
    params = {
        "candidate": inp.candidate,
        "product": inp.product,
        "fee": inp.fees,
        "snapshot": case.snapshot,
        "profile": inp.risk,
        "cutoff_ns": T,
        "now_ns": T + 1,
    }
    params.update(changes)
    return size_independent_candidate(**params)


def test_independent_same_snapshot_exact_lot_margin_and_risk(case):
    first = prepare(case, 1)[1]
    second = prepare(case, 2)[1]
    assert first.quantity == second.quantity == D("9.7")
    assert first.margin_cash == D("485.2425")
    assert first.leverage == 2
    assert first.normal_loss_cash == D("9.7") * (D("1.05") + D(".10005") + D(".099"))
    assert first.account_snapshot_ref == second.account_snapshot_ref == case.snapshot.content_hash
    assert first.quantity % D(".1") == 0


@pytest.mark.parametrize(
    "change",
    [
        {"complete": False},
        {"status": "UNKNOWN"},
        {"status": "CONFLICT"},
        {"observed_at_ns": T - 2 * 86_400_000_000_000},
        {"available_at_ns": T + 1},
        {"expires_at_ns": T + 1},
        {"permission_ref": H("changed-permission")},
        {"venue": "BYBIT"},
        {"environment": "MAINNET"},
        {"risk_policy_ref": H("changed-risk")},
    ],
)
def test_stale_incomplete_incompatible_snapshots_refuse(case, change):
    result = size_with(case, snapshot=replace(case.snapshot, **change))
    assert result.status == "UNAVAILABLE" and result.quantity == 0


def test_ten_dollar_account_minimum_lot_and_no_leverage_rescue(case):
    inp = list(case.inputs.values())[0]
    account = replace(
        case.snapshot,
        equity_cash=D("10"),
        exchange_available_cash=D("10"),
        bot_allocation_cash=D("10"),
        manual_reserve_cash=D("0"),
    )
    product = replace(inp.product, min_qty=D("1"))
    profile = replace(inp.risk, product_ref=product.content_hash)
    result = size_with(case, snapshot=account, product=product, profile=profile)
    assert result.status == "INFEASIBLE"
    # Reducing loss budget cannot be repaired by increasing leverage.
    assert size_with(case, profile=replace(inp.risk, normal_trade_limit_cash=D(".001"))).status == "INFEASIBLE"


def test_reflected_margin_not_subtracted_twice_possible_and_reserved_not_ignored(case):
    inp = list(case.inputs.values())[0]
    exp = SnapshotExposureV1(
        "existing",
        inp.product.key.content_hash,
        "ACTUAL",
        D("2"),
        D("10"),
        D("100"),
        D("100"),
        D("100"),
        True,
        D("100"),
        (D("-10"),),
        H("exposure"),
    )
    reflected = replace(case.snapshot, exposures=(exp,))
    unreflected = replace(reflected, exposures=(replace(exp, margin_reflected_in_available=False),))
    assert reflected.opening_cash_headroom == D("700")
    # exchange available 900 minus manual100 and unreflected100 =>700;
    # allocation800 minus committed100 =>700. Both are independent caps.
    assert unreflected.opening_cash_headroom == D("700")
    exp = replace(exp, margin_cash=D("300"), kind="POSSIBLE", bot_allocation_cash=D("0"))
    assert replace(case.snapshot, exposures=(exp,)).opening_cash_headroom == D("800")
    possible = replace(case.snapshot, exposures=(replace(exp, margin_reflected_in_available=False),))
    assert possible.opening_cash_headroom == D("500")
    assert size_with(case, snapshot=possible).quantity < size_with(case).quantity
    with pytest.raises(ValueError, match="duplicate"):
        replace(case.snapshot, exposures=(exp, exp))


def test_correlated_joint_altcoin_stress_and_convex_impact(case):
    inp = list(case.inputs.values())[0]
    exp = SnapshotExposureV1(
        "possible-alt",
        H("other-instrument"),
        "POSSIBLE",
        D("0"),
        D("190"),
        D("0"),
        D("0"),
        D("0"),
        True,
        D("0"),
        (D("-190"),),
        H("possible-alt-source"),
    )
    shocked = size_with(case, snapshot=replace(case.snapshot, exposures=(exp,)))
    assert shocked.quantity < size_with(case).quantity
    assert shocked.stress_loss_cash + D("190") <= inp.risk.joint_stress_limit_cash
    impacted = size_with(case, profile=replace(inp.risk, impact_per_unit_squared_cash=D("2")))
    assert impacted.quantity < size_with(case).quantity


def test_buffer_feasible_interval_and_margin_tiers(case):
    inp = list(case.inputs.values())[0]
    result = size_with(case, profile=replace(inp.risk, liquidation_buffer_cash=D("600")))
    # Small lots fail a fixed liquidation buffer, larger lots can pass.
    assert result.status == "SIZED" and result.quantity == D("7.9")
    tiers = (
        replace(inp.risk.tiers[0], notional_ceiling_cash=D("500")),
        replace(inp.risk.tiers[0], initial_margin_fraction=D(".7"), notional_ceiling_cash=D("10000")),
    )
    tiered = size_with(case, profile=replace(inp.risk, tiers=tiers, allowed_leverages=(D("2"),)))
    assert tiered.margin_cash == tiered.notional_cash * D(".7")


@pytest.mark.parametrize(
    "field,value",
    [
        ("entry_collar", D("100.06")),
        ("entry_reference", D("100.01")),
        ("stop_price", D("98.99")),
        ("horizon_end_ns", T + 5 * 3_600_000_000_000),
        ("fee_ref", H("new-fee")),
        ("funding_ref", H("new-funding")),
        ("execution_ref", H("new-execution")),
        ("model_ref", H("new-model")),
        ("permission_ref", H("new-permission")),
        ("setup_ref", H("new-setup")),
    ],
)
def test_bound_material_action_mutations_change_identity(case, field, value):
    _, _, action = prepare(case)
    if field == "fee_ref":
        changed = replace(action, fee_ref=value, sizing=replace(action.sizing, fee_ref=value))
    else:
        changed = replace(action, **{field: value})
    assert changed.content_hash != action.content_hash
    assert ExactActionV1.from_dict(changed.to_dict()) == changed


def test_quantity_and_account_revision_invalidates_action(case):
    _, _, action = prepare(case)
    with pytest.raises(ValueError, match="sizing"):
        replace(action, quantity=action.quantity + D(".1"))
    revised = H("new-account")
    changed = replace(action, account_snapshot_ref=revised, sizing=replace(action.sizing, account_snapshot_ref=revised))
    assert changed.content_hash != action.content_hash


@pytest.mark.parametrize("quantity", [D("0"), D("1"), D("2")])
@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_no_partial_full_cash_fee_both_legs_and_signed_funding(case, quantity, side):
    inp, _, a = prepare(case)
    schedule = FundingScheduleV2(0, (T + 100,), False, H("one-payment"))
    # Cash fixture uses an independently bound two-contract exact action.
    sizing = replace(a.sizing, quantity=D("2"))
    a = replace(
        a,
        quantity=D("2"),
        sizing=sizing,
        funding_ref=schedule.content_hash,
        side=side,
        entry_collar=D("100") if side == "LONG" else D("100"),
        stop_price=D("99") if side == "LONG" else D("101"),
    )
    fills = (
        ()
        if not quantity
        else (
            MeanCashFillV1("entry", T + 1, "ENTRY", quantity, D("100"), True, D("0"), H("entry")),
            MeanCashFillV1(
                "exit", T + 200, "EXIT", quantity, D("110") if side == "LONG" else D("90"), True, D("0"), H("exit")
            ),
        )
    )
    payment = MeanFundingPaymentV1(T + 100, D(".01"), D("100"), H("payment"))
    path = MeanCashPathV1(a.content_hash, H("path"), H("cash-source"), "CAUSAL_REPLAY", fills, (payment,))
    value = value_mean_cash_path(
        action=a, path=path, fee=inp.fees, funding_schedule=schedule, base_units_per_contract=D("1")
    )
    fee = quantity * (D("100") + (D("110") if side == "LONG" else D("90"))) * D(".001")
    funding = -quantity if side == "LONG" else quantity
    assert value.trading_cash == quantity * 10 - fee + funding
    assert value.fees_cash == fee and value.funding_cash == funding
    assert value.ioc_cancelled_quantity == 2 - quantity and value.residual_quantity == 0
    assert value.terminal_ns == a.cutoff_ns + 86_400_000_000_000


def test_funding_exposure_boundary_missing_cost_and_slippage_once(case):
    inp, _, a = prepare(case)
    schedule = FundingScheduleV2(0, (T + 1, T + 3), False, H("boundaries"))
    a = replace(a, funding_ref=schedule.content_hash)
    q = a.quantity
    entry = MeanCashFillV1("entry", T + 1, "ENTRY", q, D("100"), True, D("0"), H("entry"))
    exit = MeanCashFillV1("exit", T + 3, "EXIT", q, D("100"), True, D("0"), H("exit"))
    payment = MeanFundingPaymentV1(T + 3, D(".01"), D("100"), H("payment"))
    path = MeanCashPathV1(a.content_hash, H("path"), H("source"), "CAUSAL_REPLAY", (entry, exit), (payment,))
    value = value_mean_cash_path(
        action=a, path=path, fee=inp.fees, funding_schedule=schedule, base_units_per_contract=D("1")
    )
    assert value.funding_cash == -q  # entry exactly at first settlement excluded; exit at next included
    assert value.trading_cash == -q - q * D(".2")
    missing = value_mean_cash_path(
        action=a,
        path=replace(path, funding=()),
        fee=inp.fees,
        funding_schedule=schedule,
        base_units_per_contract=D("1"),
    )
    assert missing.trading_cash is None
    with pytest.raises(ValueError, match="twice"):
        replace(entry, additional_slippage_cash=D("1"))


def test_stop_gap_uses_executable_quote_and_partial_exit_remains_unavailable(case):
    inp, _, a = prepare(case)
    points = (
        inp.scenarios.paths[0].points[0],
        MeanMarketPointV1(T + 100, D("80"), D("79"), D("81"), a.quantity / 2, D("100"), H("gap")),
    )
    scenario = replace(inp.scenarios, paths=(replace(inp.scenarios.paths[0], points=points),))
    path = replay_independent_action(a, scenario, quantity_step=inp.product.qty_step)[0]
    assert path.fills[-1].executable_price == D("79")
    value = value_mean_cash_path(
        action=a, path=path, fee=inp.fees, funding_schedule=inp.funding, base_units_per_contract=D("1")
    )
    assert value.trading_cash is None and value.residual_quantity > 0
