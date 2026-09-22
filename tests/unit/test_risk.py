"""RiskPolicy + drawdown scaling tests."""

from __future__ import annotations

from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from atlas.domain.risk import (
    ENGINEERING_DEFAULTS,
    drawdown_scaling,
    engineering_default_policy,
)

D = Decimal


def test_engineering_defaults_labeled_not_safe():
    assert ENGINEERING_DEFAULTS["_label"].startswith("ENGINEERING_DEFAULTS")
    assert ENGINEERING_DEFAULTS["max_simultaneous_new_risk_intents"] == 1
    p = engineering_default_policy(policy_effective_at_ns=1_000)
    assert p.max_simultaneous_new_risk_intents == 1
    # fractions, not percentages
    assert p.normal_loss_per_trade_frac == D("0.0010")


def test_drawdown_scaling_frozen_formula():
    reduce_t = D("0.05")
    stop_t = D("0.10")
    assert drawdown_scaling(D("0"), reduce_t, stop_t) == D("1")
    assert drawdown_scaling(D("0.05"), reduce_t, stop_t) == D("1")
    assert drawdown_scaling(D("0.10"), reduce_t, stop_t) == D("0")
    assert drawdown_scaling(D("0.20"), reduce_t, stop_t) == D("0")
    mid = drawdown_scaling(D("0.075"), reduce_t, stop_t)
    assert mid == D("0.5")
    # linear interpolation check
    v = drawdown_scaling(D("0.06"), reduce_t, stop_t)
    assert abs(v - D("0.8")) < D("0.000001")


def test_drawdown_scaling_rejects_bad_thresholds():
    with pytest.raises(ValueError):
        drawdown_scaling(D("0.06"), D("0.10"), D("0.05"))
    with pytest.raises(ValueError):
        drawdown_scaling(D("0.06"), D("0.05"), D("0.05"))


@given(dd=st.decimals(min_value=0, max_value=1, places=4))
def test_drawdown_scaling_bounded(dd):
    # normalize hypothesis decimals to valid Decimal
    if dd.is_nan():
        return
    dd = abs(dd)
    if dd > D("1"):
        dd = D("1")
    s = drawdown_scaling(dd, D("0.05"), D("0.10"))
    assert D("0") <= s <= D("1")


def test_policy_rejects_nonsensical_ordering_and_negatives():
    base_kwargs = {
        "policy_version": "t",
        "policy_effective_at_ns": 1,
        "eligible_equity_definition": "E",
        "normal_loss_per_trade_frac": D("0.001"),
        "aggregate_open_normal_loss_frac": D("0.005"),
        "stress_loss_per_trade_frac": D("0.0025"),
        "portfolio_es_alpha": D("0.975"),
        "portfolio_es_limit_frac": D("0.01"),
        "account_gross_notional_limit": D("1.0"),
        "instrument_notional_limit": D("0.5"),
        "correlated_crypto_beta_limit": D("0.75"),
        "venue_collateral_limit": D("1.0"),
        "min_free_margin_reserve_frac": D("0.5"),
        "drawdown_reduce_threshold": D("0.05"),
        "drawdown_stop_threshold": D("0.10"),
        "drawdown_reduce_recovery": D("0.04"),
        "drawdown_stop_recovery": D("0.08"),
        "max_contract_leverage": D("2.0"),
    }
    from atlas.domain.risk import RiskPolicy

    RiskPolicy(**base_kwargs)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="reduce.*stop|threshold"):
        RiskPolicy(**{**base_kwargs, "drawdown_reduce_threshold": D("0.10"), "drawdown_stop_threshold": D("0.05")})  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="recovery"):
        RiskPolicy(**{**base_kwargs, "drawdown_reduce_recovery": D("0.06")})  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        RiskPolicy(**{**base_kwargs, "normal_loss_per_trade_frac": D("-0.001")})  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="cannot exceed aggregate"):
        RiskPolicy(
            **{**base_kwargs, "normal_loss_per_trade_frac": D("0.01"), "aggregate_open_normal_loss_frac": D("0.005")}
        )  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        RiskPolicy(**{**base_kwargs, "max_simultaneous_new_risk_intents": 0})  # type: ignore[arg-type]


def test_policy_hash_deterministic():
    a = engineering_default_policy(policy_effective_at_ns=7)
    b = engineering_default_policy(policy_effective_at_ns=7)
    assert a.policy_hash() == b.policy_hash()
