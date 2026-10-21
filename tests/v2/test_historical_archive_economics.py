"""Changed archive/replay seams only; fixtures are never market evidence."""

import io
import zipfile

import pytest

from atlas.v2.features.technical import atr
from atlas.v2.research.full_cycle import applicability as app
from atlas.v2.research.full_cycle.historical_archive import HEADER, read_zip
from atlas.v2.research.full_cycle.historical_economics import (
    BEGIN,
    M15,
    calendar_bootstrap,
    overlay,
    simulate,
    summarize,
)


def archive(rows, member="BTCUSDT-15m-2024-07.csv"):
    b = io.BytesIO()
    with zipfile.ZipFile(b, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(member, ",".join(HEADER) + "\n" + "\n".join(",".join(map(str, r)) for r in rows) + "\n")
    return b.getvalue()


def row(t=1719792000000):
    return [t, "100", "102", "99", "101", "5", t + 899999, "503", "8", "3", "302", "0"]


def test_archive_proves_bytes_schema_units_and_gap_denominator():
    b = archive([row(), row(1719793800000)])
    _, rows, info = read_zip(b, "BTCUSDT-15m-2024-07.zip", "15m", "2024-07")
    assert len(rows) == 2 and info["internal_missing_slots"] == 1
    assert info["first_open_ms"] == 1719792000000 and info["missing_month_slots"] == 2974


@pytest.mark.parametrize("change", ["microseconds", "duplicate", "wrong_month", "invalid_ohlc", "wrong_member"])
def test_archive_rejects_false_identity_and_rows(change):
    rows = [row()]
    member = "BTCUSDT-15m-2024-07.csv"
    if change == "microseconds":
        rows[0][0] *= 1000
    if change == "duplicate":
        rows *= 2
    if change == "wrong_month":
        rows = [row(1722470400000)]
    if change == "invalid_ohlc":
        rows[0][2] = "98"
    if change == "wrong_member":
        member = "../BTCUSDT-15m-2024-07.csv"
    with pytest.raises(ValueError):
        read_zip(archive(rows, member), "BTCUSDT-15m-2024-07.zip", "15m", "2024-07")


def test_native_s1_cached_prefix_matches_unaccelerated_and_future_tail():
    from .test_session014_s1 import KEY, SETUP, asof_join, bar, store_for_side

    market = store_for_side("LONG")
    market.append(bar(800, close="105.55"))
    market.append(bar(801, close="107"))
    j = asof_join(market, KEY, cutoff_ns=SETUP + 2 * M15)
    kwargs = {
        "cycle_id": "parity",
        "symbol": "BTCUSDT",
        "key": KEY,
        "m15": j.m15,
        "h1": j.h1,
        "h4": j.h4,
        "universe_eligible": False,
        "setup_cache": {},
        "max_trigger_bars": 0,
    }
    a = app.evaluate_s1_watch_lifecycle(**kwargs)
    b = app.evaluate_s1_watch_lifecycle(**kwargs, precomputed_atr=atr(j.m15))
    assert [x.as_row() for x in a] == [x.as_row() for x in b]
    extended = list(j.m15) + [bar(802, close="10000")]
    c = app.evaluate_s1_watch_lifecycle(**{**kwargs, "m15": extended}, precomputed_atr=atr(extended))
    assert [x.as_row() for x in a] == [x.as_row() for x in c[: len(a)]]
    with pytest.raises(ValueError, match="ATR_PREFIX_LENGTH"):
        app.evaluate_s1_watch_lifecycle(**kwargs, precomputed_atr=[])


def test_retrospective_simulation_stays_out_of_original_time_evidence():
    from .test_session014_s1 import bar

    b = bar(800, close="105.5")
    r = app.ApplicabilityRecordV1(
        "sim",
        "BTCUSDT",
        "S1",
        "CONFIRMED_15M_CLOSE",
        b.close_at_ns,
        b.close_at_ns,
        False,
        "INELIGIBLE",
        "UNIVERSE_INELIGIBLE",
        detail={"setup_at_ns": b.close_at_ns - M15},
        setup_predicates={"setup_side": "LONG", "setup_extreme": "104"},
        policy_id="S1_MTF_TREND_PULLBACK",
        policy_hash="1" * 64,
    )
    action, outcomes, refusal = simulate(r, b, [bar(801, close="105.5")], 1.0, scenario="BASE")
    assert not refusal and action.evidence_grade == "RECONSTRUCTED_HISTORICAL_RESEARCH"
    assert action.provenance["actual_historical_seal"] == "UNAVAILABLE"
    assert action.notional_usd <= 10000 and action.quantity * abs(action.entry_reference - action.stop_price) <= 50
    assert outcomes and all("FUNDING_UNOBSERVABLE" in x.uncertainty or x.entry_state == "NO_FILL" for x in outcomes)


def test_no_fill_censoring_and_cash_denominators_are_distinct():
    rows = [
        {
            "exit_state": "CLOSED",
            "entry_state": "FILLED",
            "conditional_before_funding_cash": "-3",
            "exit_at_ns": BEGIN + M15,
            "action_id": "a",
            "notional_usd": "1000",
            "modeled_decision_at_ns": BEGIN,
        },
        {
            "exit_state": "NO_POSITION",
            "entry_state": "NO_FILL",
            "conditional_before_funding_cash": "0",
            "exit_at_ns": BEGIN + M15,
            "action_id": "b",
            "notional_usd": "1000",
            "modeled_decision_at_ns": BEGIN,
        },
    ]
    summary = summarize(rows, 10)
    assert summary["mean_cash_per_modeled_action"] == -1.5
    assert summary["mean_cash_per_price_evaluated_slot"] == -0.3
    assert summary["cash_after_adverse_5bps_per_filled_action"] == -3.5
    assert summary["native_full_policy_cash"] is None
    incomplete = [{**rows[0], "exit_state": "CENSORED"}]
    assert summarize(incomplete, 1)["total_conditional_cash"] is None


def test_portfolio_overlay_blocks_overlaps_without_outcome_selection():
    rows = [
        {
            "modeled_decision_at_ns": BEGIN,
            "exit_at_ns": BEGIN + 2 * M15,
            "strategy": "S1",
            "symbol": "BTCUSDT",
            "action_id": "a",
            "entry_state": "FILLED",
        },
        {
            "modeled_decision_at_ns": BEGIN + M15,
            "exit_at_ns": BEGIN + M15,
            "strategy": "S1",
            "symbol": "ETHUSDT",
            "action_id": "b",
            "entry_state": "FILLED",
        },
    ]
    chosen, blocked = overlay(rows)
    assert [r["action_id"] for r in chosen] == ["a"] and blocked == 1


def test_calendar_inference_refuses_significance_on_small_sample():
    x = calendar_bootstrap([], BEGIN, BEGIN + 62 * 86400_000_000_000)
    assert x["1_week"]["calendar_blocks"] == 9 and x["2_week"]["calendar_blocks"] == 5
    assert all(r["significance"] == "NOT ESTIMABLE" for r in x.values())


def test_funding_interval_keeps_signed_cash_and_exit_timing_uncertainty():
    from decimal import Decimal as D

    from atlas.v2.research.full_cycle.historical_economics import funding_cash_interval

    assert funding_cash_interval("LONG", "2", ".001", "99", "101") == (D("-.202"), D("-.198"))
    assert funding_cash_interval("SHORT", "2", ".001", "99", "101") == (D(".198"), D(".202"))
    assert funding_cash_interval("LONG", "2", ".001", "99", "101", exposure_ambiguous=True) == (D("-.202"), D(0))
