"""Research-only chronology, numerical, cash and genuine source regressions."""

import ast
import copy
import hashlib
import json
import os
import pathlib
import sys
from dataclasses import replace
from decimal import Decimal as D
from types import MappingProxyType

import pytest

HERE = pathlib.Path(__file__).resolve().parents[2] / "docs/research/atlas-red-historical-blind-ml"
sys.path.insert(0, str(HERE))
import common  # noqa: E402
import decisions  # noqa: E402
import diagnostic_models as models  # noqa: E402
import economics  # noqa: E402
import evaluate_outcomes as outcomes  # noqa: E402
from causal_replay import (  # noqa: E402
    BASE_FEATURES,
    CONTEXT_FEATURES,
    BoundedBars,
    Market,
    NativeBar,
    PrefixView,
    Snapshot,
    accepted,
    feature_values,
    replay_events,
)
from independent_sol_reproduction import Episodes, Origin  # noqa: E402

from atlas.v2.science.mean_models import MeanTrainingRowV1, eligible_mean_rows, fit_m0_mean  # noqa: E402

T = common.ns("2025-04-01T00:00:00Z")


def bar(at, close="100", *, high="102", low="98", interval="15m", source=None):
    duration = common.HOUR if interval == "1h" else common.M15
    return NativeBar(
        accepted.INTERVALS[interval],
        at - duration,
        at,
        ("100", high, low, close, "1"),
        source or common.digest([at, interval]),
    )


def snapshot(*, side="LONG", zero_range=False, missing_anchor=False):
    hourly = tuple(bar(T - j * common.HOUR, interval="1h") for j in range(24, -1, -1))
    current = bar(T, high="100", low="100") if zero_range else bar(T)
    prefix = PrefixView(T, T + 6 * common.SECOND, hourly, current, bar(T - common.M15))
    state = MappingProxyType({"state": "SETUP", "h4_trend": "UP" if side == "LONG" else "DOWN"})
    rank = MappingProxyType({"state": "S6_FIXED_COHORT_RANK_DIAGNOSTIC", "score": 2.0, "decile": "TOP"})
    anchor = MappingProxyType({"h1_last_return": None if missing_anchor else 0.0, "h4_trend": "NEUTRAL"})
    return Snapshot("SOLUSDT", prefix, state, rank, anchor, anchor, common.digest("source"))


def training_rows(n=40):
    rows = []
    for i in range(n):
        at = common.ns("2024-03-01T00:00:00Z") + i * common.DAY
        values = [float((i % (j + 2)) / (j + 2)) for j in range(13)]
        rows.append(
            {
                "event_id": common.digest(["synthetic", i]),
                "cutoff_ns": at,
                "label_available_ns": at + 5 * common.HOUR,
                "features_complete": 1,
                "label_status": "OBSERVED",
                "target_4h_bps": float(i % 7 - 2),
                "evidence_grade": common.GRADE,
                **dict(zip((*BASE_FEATURES, *CONTEXT_FEATURES), values, strict=True)),
            }
        )
    return rows


def fitted(estimator="RIDGE_S1"):
    names = (*BASE_FEATURES, *CONTEXT_FEATURES) if estimator == "RIDGE_CONTEXT" else BASE_FEATURES
    return models.fit_numeric(training_rows(), names, estimator=estimator, fit_cutoff=common.ns("2025-05-01T00:00:00Z"))


def test_protocol_is_precommitted_fixed_scope_not_a_pristine_claim():
    p = common.protocol()
    assert len(p["cohort"]) == 35
    assert hashlib.sha256(("\n".join(p["cohort"]) + "\n").encode()).hexdigest() == p["ordered_cohort_sha256"]
    assert p["source_scope"]["new_months"] == ["2025-07", "2025-08", "2025-09"]
    assert p["partitions"]["embargo_hours"] == 48
    assert "human prior exposure" in p["test_design_label"]
    assert set(p["arms"]) == set(decisions.ARMS)


def test_future_bar_query_and_same_close_unready_hard_fail():
    source = {T: bar(T), T + common.M15: bar(T + common.M15, "10000", high="10001")}
    view = BoundedBars(source, T, T + 5 * common.SECOND)
    assert view.get(T).close == 100
    with pytest.raises(ValueError, match="FUTURE_PRICE_ACCESS"):
        view.get(T + common.M15)
    with pytest.raises(ValueError, match="NOT_REPLAY_VISIBLE"):
        BoundedBars(source, T, T + 4 * common.SECOND).get(T)
    assert BoundedBars({}, T, T + 6 * common.SECOND).get(T) is None


@pytest.mark.parametrize("lag", [5, 30, 60])
def test_declared_delay_visibility_and_frozen_deadline(lag):
    ready = T + (lag + 1) * common.SECOND
    assert BoundedBars({T: bar(T)}, T, ready, lag).get(T).end == T
    with pytest.raises(ValueError, match="FROZEN_DEADLINE"):
        economics.require_frozen_deadline(ready_ns=ready, fill_ns=T + common.M15, deadline_ns=T + 5 * common.SECOND)


def test_prefix_rejects_future_and_processing_lag():
    snap = snapshot()
    with pytest.raises(ValueError, match="FUTURE_OR_UNREADY"):
        replace(snap.prefix, current=bar(T + common.M15))
    with pytest.raises(ValueError, match="FUTURE_OR_UNREADY"):
        replace(snap.prefix, ready_ns=T + 5 * common.SECOND)
    with pytest.raises(ValueError, match="FUTURE_PRICE_ACCESS"):
        snap.prefix.bar(T + common.M15)


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_zero_features_are_observed_and_causal_snapshots_immutable(side):
    snap = snapshot(side=side, zero_range=True)
    base, context = feature_values(snap, side, T - common.M15, "100")
    assert base[2] == base[4] == base[5] == base[6] == 0.0
    assert base[3] == 0.5
    assert context[2] == context[3] == 0.0
    with pytest.raises(TypeError):
        snap.state["state"] = "FUTURE"
    assert len(base) == 7 and len(context) == 6


def test_missing_anchor_not_zero_and_incomplete_hourly_prefix_refused():
    snap = snapshot(missing_anchor=True)
    base, context = feature_values(snap, "LONG", T - common.M15, "99")
    assert len(base) == 7 and context is None
    with pytest.raises(ValueError, match="NEED_25"):
        feature_values(
            replace(snap, prefix=replace(snap.prefix, hourly=snap.prefix.hourly[:-1])), "LONG", T - common.M15, "99"
        )


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_episode_missing_first_does_not_create_false_later_first(side):
    born = T - common.HOUR
    orig = Origin("SOLUSDT", born, side, "99", common.digest("setup"))
    close = "103" if side == "LONG" else "97"
    raw = {
        born: bar(born),
        born + common.M15: bar(born + common.M15, close, high="104", low="96"),
        T: bar(T, close, high="104", low="96"),
    }
    engine = Episodes(born + common.M15, BoundedBars(raw, T, T + 6 * common.SECOND))
    first = engine.observe(orig, born + common.M15)
    assert first["s1_first_trigger_event"] == 1
    engine.observe(orig, born + 2 * common.M15)
    assert engine.observe(orig, T)["s1_first_trigger_event"] == 0
    missing = Episodes(
        born + common.M15,
        BoundedBars({born: raw[born], T: raw[T], T - common.M15: bar(T - common.M15)}, T, T + 6 * common.SECOND),
    )
    missing.observe(orig, born + common.M15)
    later = missing.observe(orig, T)
    assert later["s1_raw_trigger_predicate"] == 1
    assert later["s1_first_trigger_event"] == 0 and later["s1_trigger_episode_status"] == "CENSORED_ACTIVE"


def test_native_h1_boundary_prior_setup_owns_fourth_opportunity():
    previous, current = {"setup_id": "old"}, {"setup_id": "new"}
    assert accepted.s1_trigger_source("SOLUSDT", T, T, current, previous) is previous
    assert accepted.s1_trigger_source("SOLUSDT", T, T + common.M15, current, previous) is current


@pytest.mark.parametrize("kind", ["future_label", "test_origin", "duplicate"])
def test_invalid_training_hard_failure(kind):
    rows = training_rows()
    if kind == "future_label":
        rows[0]["label_available_ns"] = common.ns("2025-07-02T00:00:00Z")
    elif kind == "test_origin":
        rows[0]["cutoff_ns"] = common.ns("2025-07-01T00:00:00Z")
    else:
        rows.append(rows[0])
    with pytest.raises(ValueError, match="IMMATURE_OR_TEST|DUPLICATE"):
        models.fit_numeric(rows, BASE_FEATURES, estimator="RIDGE_S1", fit_cutoff=common.ns("2025-05-01T00:00:00Z"))


def test_chronological_partition_same_calendar_purges_boundary_labels():
    r = training_rows()[0]
    assert models.partition(r) == "TRAIN"
    assert models.partition({**r, "symbol": "ETHUSDT"}) == models.partition({**r, "symbol": "SOLUSDT"})
    assert models.partition({**r, "label_available_ns": common.ns("2024-07-01T00:00:00Z")}) == "PURGED_OR_OUTSIDE"
    assert models.partition({**r, "features_complete": "0"}) == "UNAVAILABLE"
    assert models.partition({**r, "cutoff_ns": common.ns("2025-06-27T00:00:00Z")}) == "PURGED_OR_OUTSIDE"


def test_test_informed_model_threshold_or_feature_selection_is_refused():
    with pytest.raises(ValueError, match="FIXED_ESTIMATOR"):
        models.fit_numeric(
            training_rows(), BASE_FEATURES, estimator="TEST_WINNER", fit_cutoff=common.ns("2025-05-01T00:00:00Z")
        )
    with pytest.raises(ValueError, match="UNREGISTERED_PRICE_FEATURE"):
        models.fit_numeric(
            training_rows(), BASE_FEATURES[:-1], estimator="RIDGE_S1", fit_cutoff=common.ns("2025-05-01T00:00:00Z")
        )
    with pytest.raises(ValueError, match="TEST_INFORMED_THRESHOLD"):
        economics.fixed_test_threshold(5.0)
    assert economics.fixed_test_threshold() == 12.0


@pytest.mark.parametrize("estimator", ["EMPIRICAL", "RIDGE_S1", "RIDGE_CONTEXT"])
def test_arithmetic_mean_tail_fixture_determinism_and_target_isolation(estimator):
    rows = training_rows(100)
    for r in rows:
        r["target_4h_bps"] = 1.0
        for name in (*BASE_FEATURES, *CONTEXT_FEATURES):
            r[name] = 0.0
    rows[-1]["target_4h_bps"] = -200.0
    names = (*BASE_FEATURES, *CONTEXT_FEATURES) if estimator == "RIDGE_CONTEXT" else BASE_FEATURES
    kwargs = {"estimator": estimator, "fit_cutoff": common.ns("2025-05-01T00:00:00Z")}
    model = models.fit_numeric(rows, names, **kwargs)
    assert model == models.fit_numeric(rows, names, **kwargs)
    prediction, status = models.predict_numeric(model, (0.0,) * len(names), at=common.ns("2025-07-01T00:00:06Z"))
    assert status == "SUPPORTED" and prediction == pytest.approx(-1.01, abs=1e-12)
    with pytest.raises(ValueError, match="PRICE_TARGET"):
        models.predict_numeric({**model, "target": "CASH"}, (0.0,) * len(names), at=T)


def test_ridge_matches_independent_augmented_lstsq_not_self_comparison():
    import numpy as np

    rows = training_rows()
    model = fitted()
    x = np.array([[r[n] for n in BASE_FEATURES] for r in rows])
    x = (x - np.mean(x, axis=0)) / np.maximum(1e-9, np.std(x, axis=0))
    x = np.column_stack((np.ones(len(x)), x))
    penalty = np.eye(x.shape[1])
    penalty[0, 0] = 0
    augmented = np.vstack((x / np.sqrt(len(x)), penalty))
    y = np.concatenate((np.array([r["target_4h_bps"] for r in rows]) / np.sqrt(len(rows)), np.zeros(x.shape[1])))
    expected = np.linalg.lstsq(augmented, y, rcond=None)[0]
    assert model["coefficients"] == pytest.approx(expected, abs=1e-10, rel=1e-10)


def test_ood_missing_and_causal_model_availability():
    model = fitted()
    at = common.ns("2025-07-01T00:00:06Z")
    assert models.predict_numeric(model, (100.0,) * 7, at=at) == (None, "OOD_TRAIN_ONLY_ENVELOPE")
    assert models.predict_numeric(model, (None,) * 7, at=at) == (None, "MISSING_OR_INVALID_FEATURES")
    with pytest.raises(ValueError, match="NOT_YET_AVAILABLE"):
        models.predict_numeric(model, (0.0,) * 7, at=T)
    with pytest.raises(ValueError, match="EXPIRED"):
        models.predict_numeric(model, (0.0,) * 7, at=common.ns("2025-10-01T00:00:00Z"))


def test_price_diagnostic_rows_cannot_enter_production_cash_training():
    h = common.digest("fixture")
    row = MeanTrainingRowV1(
        h, h, h, T - 1, T, T + 1, T + 2, (0.0,) * 6, D("1"), "DIAGNOSTIC", "FULL_FILL", "RECONSTRUCTED_MARKET"
    )
    with pytest.raises(ValueError, match="cannot establish"):
        eligible_mean_rows((row,), cutoff_ns=T + 3, compatibility_ref=h, provenance="DIAGNOSTIC")
    assert eligible_mean_rows((row,), cutoff_ns=T + 3, compatibility_ref=h, provenance="CAUSAL_REPLAY") == ()
    with pytest.raises(ValueError, match="insufficient"):
        fit_m0_mean(
            (row,),
            compatibility_ref=h,
            cutoff_ns=T + 3,
            available_at_ns=T + 4,
            expires_at_ns=T + 5,
            provenance="CAUSAL_REPLAY",
        )
    assert decisions.legal_input_audit({})["legal_candidate_or_EligibleSet"] is None
    with pytest.raises(ValueError, match="CANNOT_MINT"):
        decisions.legal_input_audit({f.name: object() for f in decisions.fields(decisions.CandidateMeanInputsV1)})


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_costs_once_and_unknown_funding_cannot_be_net_zero(side):
    value = economics.unit_cash(side=side, reference="100", endpoint="101", fees_bps=4, spread_bps=2, slippage_bps=2)
    assert value["fees_cash"] == D("0.000804")
    assert value["conditional_net_cash"] is None
    assumed = economics.unit_cash(
        side=side, reference="100", endpoint="101", fees_bps=4, spread_bps=2, slippage_bps=2, funding_charge_bps=5
    )
    assert assumed["conditional_net_cash"] == value["before_unknown_funding_cash"] - D("0.0005")
    with pytest.raises(ValueError, match="DOUBLE_CHARGE"):
        economics.unit_cash(
            side=side,
            reference="100",
            endpoint="101",
            fees_bps=4,
            spread_bps=2,
            slippage_bps=2,
            price_includes_spread_slippage=True,
        )


def test_no_fill_zero_trading_cash_retains_independent_programme_expense():
    x = economics.unit_cash(
        side="LONG", reference=None, endpoint=None, fees_bps=4, spread_bps=2, slippage_bps=2, filled=False
    )
    assert x["gross_cash"] == x["fees_cash"] == x["funding_charge_cash"] == 0
    assert x["conditional_net_cash"] == -economics.PROGRAMME_EXPENSE
    with pytest.raises(ValueError, match="MISSING_PRICE"):
        economics.unit_cash(side="LONG", reference=None, endpoint=None, fees_bps=0, spread_bps=0, slippage_bps=0)
    zero = economics.unit_cash(
        side="LONG", reference="100", endpoint="100", fees_bps=0, spread_bps=0, slippage_bps=0, funding_charge_bps=0
    )
    assert zero["gross_cash"] == 0


def test_ambiguous_intrabar_never_selects_favorable_ordering():
    lo, hi, status = economics.ambiguous_bar_bounds(stop_cash=-2, target_cash=3, stop_seen=True, target_seen=True)
    assert (lo, hi, status) == (-2, 3, "UNRESOLVED_ORDERING")


def outcome_fixture():
    row = {
        "cutoff_ns": T,
        "ready_ns": T + 6 * common.SECOND,
        "reference_ns": T + common.M15,
        "delayed_reference_ns": T + 2 * common.M15,
        "side": "LONG",
    }
    bars = {t: bar(t, "101" if t > T + common.M15 else "100") for t in range(T, T + 5 * common.HOUR, common.M15)}
    return row, bars


def test_separate_price_outcomes_mature_and_censor_without_imputation():
    row, bars = outcome_fixture()
    before = copy.deepcopy(row)
    x = outcomes.outcome(row, bars, 4)
    assert x["gross_bps"] == pytest.approx(100.0)
    assert x["reference_ns"] > row["ready_ns"]
    assert outcomes.outcome(row, bars, 4, delayed=True)["terminal_ns"] == x["terminal_ns"]
    assert row == before
    bars.pop(T + 3 * common.M15)
    assert outcomes.outcome(row, bars, 4) == {"status": "CENSORED_NATIVE_SOURCE_GAP"}
    assert outcomes.outcome(row, bars, 4, final=T + 4 * common.HOUR) == {"status": "CENSORED_WINDOW_END"}
    with pytest.raises(ValueError, match="NOT_AFTER_READY"):
        outcomes.outcome({**row, "reference_ns": T}, bars, 4)


def test_outcome_stage_has_no_fit_select_or_decision_callback():
    tree = ast.parse((HERE / "evaluate_outcomes.py").read_text())
    imports = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    assert not {"decisions", "diagnostic_models"} & imports
    calls = {node.func.id for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)}
    assert not {"fit_numeric", "fit_m0_mean", "fit_m1_mean", "decide"} & calls
    with pytest.raises(TypeError):
        outcomes.evaluate("sha", None, None, None, decision_callback=lambda: None)


def test_git_seal_refuses_mutated_decisions_before_outcome_source_reads(tmp_path, monkeypatch):
    d = tmp_path / "docs/research/x"
    d.mkdir(parents=True)
    p = d / "decisions"
    p.write_text("sealed")
    pin = str(p.relative_to(tmp_path))
    seal = {
        "pinned_files": {pin: common.sha(p)},
        "test_outcomes_computed": False,
        "protocol_sha256": common.PROTOCOL_SHA,
    }
    (d / "DECISION_SEAL.json").write_text(json.dumps(seal))
    monkeypatch.setattr(common, "HERE", d)
    monkeypatch.setattr(common, "ROOT", tmp_path)
    monkeypatch.setattr(
        outcomes.subprocess,
        "check_output",
        lambda args, **kwargs: json.dumps(seal).encode() if args[-1].endswith("DECISION_SEAL.json") else b"sealed",
    )
    assert outcomes.verify_seal("commit") == seal
    p.write_text("future-informed mutation")
    with pytest.raises(ValueError, match="SEALED_DECISION"):
        outcomes.verify_seal("commit")


def test_paired_calendar_uncertainty_repeat_and_missing_denominators():
    rows = [
        {"cutoff_ns": common.ns("2025-07-01T00:00:00Z"), "x": 1.0, "y": -1.0},
        {"cutoff_ns": common.ns("2025-08-01T00:00:00Z"), "x": 3.0, "y": -3.0},
    ]
    a = outcomes.paired_intervals(rows, ("x", "y"))
    assert a == outcomes.paired_intervals(rows, ("x", "y"))
    assert a["1_week"]["blocks"] == 14
    assert a["1_week"]["mean_intervals"]["x"] == [-v for v in reversed(a["1_week"]["mean_intervals"]["y"])]
    assert "TEST GATE" in a["1_week"]["inference_status"]


def test_common_original_denominator_does_not_hide_censored_acceptance():
    rows = []
    for i in range(3):
        r = {"outcome_4h_status": "OBSERVED" if i < 2 else "CENSORED", "gross_4h_bps": str(10 if i == 0 else -20)}
        for arm in outcomes.ARMS:
            r[arm + "_accept"] = "1" if arm == "S1_ALL" or (arm == "RIDGE_CONTEXT" and i == 0) else "0"
        rows.append(r)
    table = outcomes.economic_table(rows, 4)
    assert table["S1_ALL"]["original_N"] == 3 and table["S1_ALL"]["common_complete_N"] == 2
    assert table["S1_ALL"]["full_N_gross_mean_bps"] is None
    assert table["RIDGE_CONTEXT"]["common_original_gross_bps"]["mean"] == 5
    assert table["RIDGE_CONTEXT"]["accepted_gross_bps"]["mean"] == 10


def test_deterministic_gzip_not_metadata_timestamp(tmp_path):
    a, b = tmp_path / "a.gz", tmp_path / "b.gz"
    common.write_csv(a, [{"observed_zero": 0.0, "missing": ""}])
    common.write_csv(b, [{"observed_zero": 0.0, "missing": ""}])
    assert a.read_bytes() == b.read_bytes()
    assert list(common.read_csv(a)) == [{"observed_zero": "0.0", "missing": ""}]


@pytest.fixture(scope="module")
def real_market():
    old = pathlib.Path(
        os.environ.get("ATLAS_BLIND_OLD_CACHE", "/workspace/work/atlas-e1b-trigger-censor-qualified-scratch")
    )
    new = pathlib.Path("/workspace/work/blind-ml-new-source-cache")
    if not old.exists():
        pytest.skip("Official checksum-backed CSV cache not available")
    market = Market(2025, 6, range(3, 5), old, new)
    yield market
    market.state.cache_clear()
    market.s6.cache_clear()


def test_real_checksum_backed_week_matches_accepted_first_event_population(real_market):
    begin = common.ns("2025-04-01T00:00:00Z")
    end = common.ns("2025-04-08T00:00:00Z")
    actual = {row["first_event_id"] for _, _, row in replay_events(real_market, begin, end)}
    expected = {
        r["event_id"]
        for r in common.read_csv(common.OLD / "v3-corrected/FIRST_EVENT_FEATURES.csv.gz")
        if begin <= int(r["cutoff_ns"]) < end and r["window"] == "B"
    }
    assert actual == expected
    assert len(actual) > 300


def test_real_native_prefix_numerics_zero_anchor_and_future_tail_invariance(real_market):
    at = common.ns("2025-04-01T00:00:00Z")
    symbol = "SOLUSDT"
    before = real_market.snapshot(symbol, at)
    hcut = at - at % common.HOUR
    originals = real_market.h1[symbol], real_market.h4[symbol], real_market.ema[symbol]
    real_market.h1[symbol] = [b for b in originals[0] if b.end <= at]
    real_market.h4[symbol] = [b for b in originals[1] if b.close_at_ns <= at]
    accepted.BAR_INDEX[id(real_market.h1[symbol])] = [b.end for b in real_market.h1[symbol]]
    accepted.BAR_INDEX[id(real_market.h4[symbol])] = [b.close_at_ns for b in real_market.h4[symbol]]
    try:
        prefix = real_market._state(symbol, hcut)
        assert prefix == dict(before.state)
    finally:
        real_market.h1[symbol], real_market.h4[symbol], real_market.ema[symbol] = originals
    assert real_market.snapshot(symbol, at).source_ref == before.source_ref
    assert before.prefix.current.end == at


def test_old_production_and_frozen_authority_identities_preserved():
    gate = common.load(HERE / "GATE_A_FEASIBILITY.json")
    for name, expected in gate["freeze_sha256"].items():
        assert common.sha(common.ROOT / name) == expected
    for name, expected in gate["pinned_prior_artifacts_and_reused_code"].items():
        assert common.sha(common.ROOT / name) == expected


def test_absent_prefit_models_are_explicit_refusals_not_fallbacks():
    snap = snapshot()
    orig = Origin("SOLUSDT", T - common.M15, "LONG", "99", common.digest("origin"))
    row = decisions.decide(snap, orig, {"models": {}, "training_volatility_median": 1.0})
    assert row["S1_ALL_accept"] == 1
    assert row["frozen_policy"].startswith("BLOCKED")
    for name in decisions.MODELS:
        assert row[name + "_status"] == "MISSING_PREFIT_MODEL"
        assert row[name + "_accept"] == 0


def test_calibration_cannot_use_test_or_immature_rows():
    model = fitted()
    rows = training_rows()
    for row in rows:
        row["cutoff_ns"] = common.ns("2025-07-01T00:00:00Z")
        row["label_available_ns"] = common.ns("2025-07-02T00:00:00Z")
    with pytest.raises(ValueError, match="CALIBRATION_CHRONOLOGY"):
        models.calibrate(model, rows)


@pytest.mark.parametrize("corruption", ["missing", "wrong_key", "duplicate_episode"])
def test_whole_calendar_denominators_fail_on_missing_keys_or_duplicate_events(tmp_path, monkeypatch, corruption):
    begin = common.ns("2025-07-01T00:00:00Z")
    monkeypatch.setattr(outcomes, "FINAL", begin + 2 * common.M15)
    monkeypatch.setattr(common, "protocol", lambda: {"cohort": ["SOLUSDT", "ETHUSDT"]})
    rows = []
    for at in (begin, begin + common.M15):
        for s in ("SOLUSDT", "ETHUSDT"):
            rows.append(
                {
                    "symbol": s,
                    "cutoff_ns": at,
                    "source_current_m15": "OBSERVED",
                    "s1_precursor": "NO_SETUP",
                    "s6_state": "RANKED",
                    "first_event_id": "",
                }
            )
    calendar = tmp_path / "calendar.gz"
    dec = tmp_path / "decisions.gz"
    common.write_csv(calendar, rows)
    common.write_csv(dec, [], fields=["event_id"])
    assert outcomes.calendar_check(dec, calendar)["native_symbol_slots"] == 4
    if corruption == "missing":
        rows.pop()
    elif corruption == "wrong_key":
        rows[1]["symbol"] = "SOLUSDT"
    else:
        rows[0]["first_event_id"] = rows[1]["first_event_id"] = "one"
    common.write_csv(calendar, rows)
    with pytest.raises(ValueError, match="CALENDAR|DUPLICATE"):
        outcomes.calendar_check(dec, calendar)


def test_short_price_proxy_is_symmetric_without_executable_stop_claim():
    row, bars = outcome_fixture()
    x = outcomes.outcome(row, bars, 4)
    y = outcomes.outcome({**row, "side": "SHORT"}, bars, 4)
    assert y["gross_bps"] == -x["gross_bps"]
    assert y["mfe_bps"] == pytest.approx(-x["mae_bps"])
    assert y["mae_bps"] == pytest.approx(-x["mfe_bps"])


def test_count_thinning_and_time_at_risk_controls_are_not_extra_policies():
    rows = []
    for i, y in enumerate((10.0, -20.0)):
        row = {
            "cutoff_ns": common.ns("2025-07-01T00:00:00Z") + i * common.M15,
            "reference_ns": common.ns("2025-07-01T00:15:00Z") + i * common.M15,
            "symbol": "SOLUSDT",
            "side": "LONG",
            "btc_h4_trend": "UP",
            "eth_h4_trend": "UP",
            "outcome_4h_status": "OBSERVED",
            "gross_4h_bps": y,
        }
        for arm in outcomes.ARMS:
            row[arm + "_accept"] = "1" if arm == "S1_ALL" or (arm == "RIDGE_CONTEXT" and i == 0) else "0"
        rows.append(row)
    before = copy.deepcopy(rows)
    x = outcomes.mechanism_and_overlap(rows)
    arm = x["arms"]["RIDGE_CONTEXT"]
    assert arm["count_only_thinning_expectation_bps"] == -2.5
    assert arm["extra_vs_count_only_bps"] == 7.5
    assert arm["opportunity_hours"] == 4 and arm["peak_overlapping_hypothetical_horizons"] == 1
    assert rows == before


def test_real_prefix_matches_pure_production_math_without_future_fallback(real_market):
    from atlas.v2.features.joins import JoinedBars
    from atlas.v2.strategies.s1_trend import _regime, _setup

    at = common.ns("2025-04-01T00:00:00Z")
    s = "SOLUSDT"
    h1 = tuple(b for b in real_market.h1[s] if b.end <= at)
    h4 = tuple(b for b in real_market.h4[s] if b.close_at_ns <= at)
    join = JoinedBars(accepted.key_for(s), at, h4, h1, (), "AVAILABLE", None, None)
    regime = _regime(join)
    setup = _setup(join)
    state = real_market.state(s, at)
    assert state["regime"] == (regime.value if regime else "NEUTRAL")
    assert (state["state"] == "SETUP") == bool(setup)
    if setup:
        assert setup[0].value == state["side"] and str(setup[1]) == state["setup_extreme"]


def test_complete_synthetic_outcome_pipeline_preserves_decisions_and_censors(tmp_path, monkeypatch):
    # Synthetic fixtures exercise accounting plumbing; never real-model/fill evidence.
    monkeypatch.setitem(globals(), "T", common.ns("2025-07-01T00:00:00Z"))
    bundle = {"models": {n: fitted(n) for n in decisions.MODELS}, "training_volatility_median": 1.0}
    for m in bundle["models"].values():
        m.update(residual_q05_bps=-200.0, residual_q95_bps=200.0)
    snap = snapshot()
    orig = Origin("SOLUSDT", T - common.M15, "LONG", "99", common.digest("origin"))
    row = decisions.decide(snap, orig, bundle)
    second = {**row, "symbol": "BNBUSDT", "event_id": common.digest("second")}
    late_at = common.ns("2025-09-30T23:45:00Z")
    late = {
        **row,
        "event_id": common.digest("late"),
        "cutoff_ns": late_at,
        "ready_ns": late_at + 6 * common.SECOND,
        "reference_ns": late_at + common.M15,
        "delayed_reference_ns": late_at + 2 * common.M15,
    }
    common.write_csv(tmp_path / "DECISIONS.csv.gz", [row, second, late])
    common.save(tmp_path / "PRICE_MODELS.json", bundle)
    (tmp_path / "PROTOCOL.json").write_bytes((HERE / "PROTOCOL.json").read_bytes())
    before = common.sha(tmp_path / "DECISIONS.csv.gz")
    bars = [bar(t, "101" if t > T + common.M15 else "100") for t in range(T, T + 26 * common.HOUR, common.M15)]
    monkeypatch.setattr(common, "HERE", tmp_path)
    monkeypatch.setattr(outcomes, "verify_seal", lambda commit: {"synthetic_seal": True})
    monkeypatch.setattr(outcomes, "calendar_check", lambda *args: {"synthetic_calendar": True})
    monkeypatch.setattr(outcomes, "load_native", lambda *args: bars)
    monkeypatch.setattr(outcomes, "independent_price_checks", lambda *args: {"synthetic_only": True})
    result = outcomes.evaluate("SYNTHETIC_ONLY", tmp_path, tmp_path, tmp_path / "output")
    assert result["original_qualified_altcoin_N"] == 3
    assert result["censor_counts"]["4"] == {"OBSERVED": 2, "CENSORED_WINDOW_END": 1}
    assert common.sha(tmp_path / "DECISIONS.csv.gz") == before
    evidence = common.load(tmp_path / "output/RESULTS.json")
    assert evidence["full_programme_after_cost_cash"] == "NOT ESTIMABLE"
    assert evidence["arms"]["4"]["S1_ALL"]["full_N_gross_mean_bps"] is None
    assert evidence["cash_sensitivities"]["NO_OPEN"]["FIXED_BEFORE_UNKNOWN_FUNDING"][
        "sum_unit_notional_cash"
    ] == pytest.approx(-2 * float(economics.PROGRAMME_EXPENSE))


@pytest.mark.parametrize("script", ["acquire_sources.py", "decisions.py", "evaluate_outcomes.py"])
def test_fresh_cli_imports_do_not_depend_on_pytest_module_order(script, tmp_path):
    import subprocess

    result = subprocess.run(
        [sys.executable, str(HERE / script), "--help"], cwd=tmp_path, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    assert "usage:" in result.stdout


def test_real_S6_future_extreme_tail_does_not_change_prefix_scores(real_market):
    at = common.ns("2025-04-01T00:00:00Z")
    symbol = "SOLUSDT"
    expected = real_market._s6(at)
    last = real_market.h1[symbol][-1]
    aligned = real_market.aligned
    real_market.h1[symbol][-1] = replace(last, prices=("10000", "10001", "9999", "10000", "1"))
    try:
        real_market.aligned = accepted.aligned_return_series(real_market.h1)
        assert real_market._s6(at) == expected
    finally:
        real_market.h1[symbol][-1] = last
        real_market.aligned = aligned


def test_independent_raw_price_oracle_on_previously_inspected_real_source(real_market):
    begin = common.ns("2025-04-01T00:00:00Z")
    end = begin + common.DAY
    event = next(
        r
        for r in common.read_csv(common.OLD / "v3-corrected/FIRST_EVENT_FEATURES.csv.gz")
        if begin <= int(r["cutoff_ns"]) < end and r["study_status"] == "SOURCE_QUALIFIED_ALT_EVENT"
    )
    at = int(event["cutoff_ns"])
    bars = real_market.m15maps[event["symbol"]]
    row = {
        "event_id": event["event_id"],
        "symbol": event["symbol"],
        "cutoff_ns": at,
        "ready_ns": at + 6 * common.SECOND,
        "reference_ns": at + common.M15,
        "delayed_reference_ns": at + 2 * common.M15,
        "side": event["side"],
        "trigger_source": bars[at].source,
    }
    for h in outcomes.HORIZONS:
        result = outcomes.outcome(row, bars, h)
        assert result["status"] == "OBSERVED"
        row[f"outcome_{h}h_status"] = "OBSERVED"
        for k in ("gross_bps", "mfe_bps", "mae_bps", "reference_source", "endpoint_source", "path_sources_sha256"):
            name = f"{k.removesuffix('_bps')}_{h}h_bps" if k.endswith("_bps") else k.replace("_", f"_{h}h_", 1)
            row[name] = result[k]
    result = outcomes.independent_price_checks(
        [row],
        pathlib.Path("/workspace/work/atlas-e1b-trigger-censor-qualified-scratch"),
        pathlib.Path("/workspace/work/blind-ml-new-source-cache"),
    )
    assert result["cases"] == 3 and result["mismatches"] == 0
    assert result["maximum_absolute_error_bps"] < 1e-8


def test_calendar_transport_parts_assemble_exact_bytes_never_repair_mismatch(tmp_path, monkeypatch):
    a, b = tmp_path / "part000", tmp_path / "part001"
    a.write_bytes(b"first")
    b.write_bytes(b"second")
    monkeypatch.setattr(common, "ROOT", tmp_path)
    seal = {
        "pinned_files": {"part000": common.sha(a), "part001": common.sha(b)},
        "assembled_files": {
            "calendar.gz": {"parts": ["part000", "part001"], "sha256": hashlib.sha256(b"firstsecond").hexdigest()}
        },
    }
    outcomes.assemble_calendar(seal)
    assert (tmp_path / "calendar.gz").read_bytes() == b"firstsecond"
    (tmp_path / "calendar.gz").write_bytes(b"wrong")
    with pytest.raises(ValueError, match="ASSEMBLED_CALENDAR_IDENTITY"):
        outcomes.assemble_calendar(seal)
