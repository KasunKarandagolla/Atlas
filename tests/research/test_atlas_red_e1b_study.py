"""Synthetic-only mechanics for the preregistered nonexecuted price study."""

from __future__ import annotations

import ast
import copy
import pathlib
import subprocess
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "docs/research/atlas-red-e1b"))
import independent_sol_reproduction as oracle  # noqa: E402
import study_s1_s6 as study  # noqa: E402

T = oracle.timestamp(2024, 3, 2)


def event(side="LONG", decile="TOP"):
    return {
        "window": "A",
        "symbol": "TESTUSDT",
        "cutoff_ns": str(T),
        "event_id": "synthetic-only",
        "study_status": "SOURCE_QUALIFIED_ALT_EVENT",
        "side": side,
        "s6_state": "S6_FIXED_COHORT_RANK_DIAGNOSTIC",
        "s6_decile": decile,
        "s6_rank": "1",
        "reference_ns": str(T + oracle.M15),
        "s1_regime": "UP",
        "btc_h4_trend": "UP",
        "eth_h4_trend": "UP",
        "input_zero_volume": "NO",
    }


def bars(step=1.0, end=101):
    result = {}
    for i in range(end):
        p = 100.0 + i * step
        result[T + i * oracle.M15] = oracle.Bar(
            T + i * oracle.M15, "15m", (str(p), str(p + 1), str(p - 1), str(p), "1"), f"synthetic:{i}", "synthetic", i
        )
    return result


@pytest.mark.parametrize("lag", [5, 30, 60])
def test_reference_strictly_after_feature_ready_never_exact_close(lag):
    ref = study.reference_time(T, lag)
    assert ref == T + oracle.M15
    assert ref > T + (lag + 1) * oracle.SECOND
    assert ref - oracle.M15 <= T + (lag + 1) * oracle.SECOND


@pytest.mark.parametrize("side,sign", [("LONG", 1), ("SHORT", -1)])
def test_price_proxy_and_friction_are_not_cash_or_fill(side, sign):
    r = study.price_outcome(event(side), bars(), 4)
    assert r["status"] == "OBSERVED"
    assert r["gross_bps"] == pytest.approx(sign * (117 / 101 - 1) * 10000)
    assert r["reference_price"] == "101.0"
    assert r["endpoint_ns"] == T + 17 * oracle.M15
    assert r["mfe_bps"] >= 0 and r["mae_bps"] <= 0
    assert r["positive_friction_budget_bps"] == (r["gross_bps"] if sign > 0 else "")
    assert not any("pnl" in k or "fill" in k or "profit" in k for k in r)


@pytest.mark.parametrize(
    "missing,reason",
    [(1, "CENSORED_REFERENCE_MISSING"), (7, "CENSORED_NATIVE_OUTCOME_GAP"), (17, "CENSORED_NATIVE_OUTCOME_GAP")],
)
def test_missing_reference_intermediate_and_end_are_censored(missing, reason):
    data = bars()
    del data[T + missing * oracle.M15]
    assert study.price_outcome(event(), data, 4)["status"] == reason


def test_post_period_prices_cannot_repair_end_of_window():
    row = event()
    row["cutoff_ns"] = str(oracle.timestamp(2024, 7, 1) - oracle.HOUR)
    assert study.price_outcome(row, bars(), 4)["status"] == "CENSORED_WINDOW_END"


def test_future_bars_do_not_change_frozen_features_or_outcome():
    data = bars()
    before = study.price_outcome(event(), data, 4)
    data[T + 30 * oracle.M15] = oracle.Bar(
        T + 30 * oracle.M15, "15m", ("10000", "10001", "9999", "10000", "1"), "future", "synthetic", 30
    )
    assert study.price_outcome(event(), data, 4) == before
    future = event()
    future["proxy_4h_bps"] = -999999
    assert study.policy_status(future, "P1") == study.policy_status(event(), "P1")


@pytest.mark.parametrize(
    "side,decile,p1,p2",
    [
        ("LONG", "TOP", True, True),
        ("LONG", "MIDDLE", False, True),
        ("LONG", "BOTTOM", False, False),
        ("SHORT", "TOP", False, False),
        ("SHORT", "MIDDLE", False, True),
        ("SHORT", "BOTTOM", True, True),
    ],
)
def test_only_registered_policy_masks(side, decile, p1, p2):
    row = event(side, decile)
    assert study.policy_status(row, "P0") == "ACCEPTED"
    assert (study.policy_status(row, "P1") == "ACCEPTED") == p1
    assert (study.policy_status(row, "P2") == "ACCEPTED") == p2
    assert study.policy_status(row, "P3") == "REJECTED_NO_OPENING_REFERENCE"


def test_missing_rank_is_unavailable_not_observed_rejection_or_alt_action():
    row = event()
    row["s6_rank"] = ""
    assert study.policy_status(row, "P0") == "ACCEPTED"
    assert study.policy_status(row, "P1") == study.policy_status(row, "P2") == "UNAVAILABLE"
    row["study_status"] = "ANCHOR_CONTEXT_EXCLUDED"
    assert all(study.policy_status(row, p) == "UNAVAILABLE" for p in study.POLICIES)


def ledger_fixture():
    rows = []
    for i, value in enumerate((1.0, 10.0, None)):
        r = event(decile="TOP" if i == 1 else "BOTTOM")
        r["cutoff_ns"] = str(T + i * oracle.M15)
        r["event_id"] = str(i)
        for p in study.POLICIES:
            r[p + "_status"] = study.policy_status(r, p)
        for h in study.HORIZONS:
            r[f"outcome_{h}h_status"] = "OBSERVED" if value is not None else "CENSORED_WINDOW_END"
            r[f"proxy_{h}h_bps"] = value if value is not None else ""
            r[f"mae_{h}h_bps"] = -2 if value is not None else ""
            r[f"mfe_{h}h_bps"] = 12 if value is not None else ""
        rows.append(r)
    return rows


def test_same_original_population_no_censor_zero_imputation_and_no_selected_mean_promotion():
    report = study.summarize(ledger_fixture(), "A", study.protocol())
    r = report["horizons"]["4"]
    assert r["original_N"] == 3 and r["common_complete_original_C"] == 2
    assert r["arms"]["P0"]["mean_bps_per_original_N"] is None
    assert r["arms"]["P3"]["mean_bps_per_original_N"] == 0
    assert r["arms"]["P0"]["mean_bps_per_common_complete_original_C"] == 5.5
    assert r["arms"]["P1"]["mean_bps_per_common_complete_original_C"] == 5
    assert r["arms"]["P1"]["mean_bps_per_observed_accepted_secondary"] == 10
    assert report["mechanism_checks_4h"]["P1"]["delta_vs_P0_bps"] == -0.5
    assert report["mechanism_checks_4h"]["P1"]["retained_fraction"] == 0.5


def test_paired_calendar_blocks_and_determinism():
    rows = ledger_fixture()[:2]
    first = study.paired_blocks(rows, "A", 4, study.protocol())
    assert first == study.paired_blocks(rows, "A", 4, study.protocol())
    for r in first.values():
        assert r["below_20_block_diagnostic_floor"]
        assert r["exploratory_intervals_per_original_common_event_bps"]["P3"] == [0, 0]
        assert r["paired_delta_vs_P0_intervals_bps"]["P0"] == [0, 0]


def test_A_selection_tie_and_no_B_selection():
    result = {
        "window": "A",
        "horizons": {"4": {"arms": {p: {"mean_bps_per_common_complete_original_C": 1.0} for p in ("P0", "P1", "P2")}}},
    }
    assert study.choose_A(result, study.protocol())["selected_rule"] == "P0"
    result["window"] = "B"
    with pytest.raises(ValueError, match="A_ONLY"):
        study.choose_A(result, study.protocol())


def test_B_refuses_missing_or_changed_frozen_selection(tmp_path):
    with pytest.raises(ValueError, match="FROZEN_A_SELECTION"):
        study.run_outcomes(tmp_path / "missing", tmp_path, "B", tmp_path)


def test_independent_mathematical_oracle_ast_unchanged_from_reviewed_source():
    source = subprocess.check_output(
        [
            "git",
            "show",
            "c29dfc2c2ae6cb903b53f72fdf2d457c9a5bb32d:docs/research/atlas-red-e1b/independent_sol_reproduction.py",
        ],
        text=True,
    )
    current = pathlib.Path(oracle.__file__).read_text()
    names = {
        "independent_ema",
        "aggregate_h4",
        "S1Prefixes",
        "Origin",
        "Episodes",
        "observed_predicate",
        "s1_row",
        "s6_moments",
        "rank_scores",
        "s6_grid",
        "replay_readiness",
    }

    def bodies(text):
        return {
            n.name: ast.dump(n, include_attributes=False)
            for n in ast.parse(text).body
            if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name in names
        }

    assert bodies(source) == bodies(current)


def test_population_extraction_refuses_incomplete_validation(tmp_path):
    p = tmp_path / "validation.json"
    study.save(p, {"comparisons": {"checked_rows": {}, "rows_with_mismatch": {}}})
    with pytest.raises(ValueError, match="INCOMPLETE_COMPARISON"):
        study.extract(tmp_path / "missing", p, tmp_path / "output.gz")


def test_population_extraction_rejects_duplicate_first_event_weights(tmp_path):
    import csv
    import gzip

    with gzip.open(study.HERE / "COMPACT_REAL_MULTI_SYMBOL_SAMPLE.csv.gz", "rt") as f:
        row = next(r for r in csv.DictReader(f) if r["s1_first_trigger_event"] == "1")
    panel = tmp_path / "panel.csv.gz"
    study.write_csv(panel, [row, copy.copy(row)])
    validation = tmp_path / "validation.json"
    study.save(
        validation,
        {
            "comparisons": {
                "checked_rows": {
                    g + ":" + w: 409920
                    for g in ("s1_primary", "s6", "anchor_context", "physical_h1_source", "physical_m15_source")
                    for w in ("A", "B")
                },
                "rows_with_mismatch": {},
            },
            "pinned_artifacts": {panel.name: study.sha(panel)},
        },
    )
    with pytest.raises(ValueError, match="DUPLICATE_FIRST_EVENT_WEIGHT"):
        study.extract(panel, validation, tmp_path / "events.gz")


def test_deterministic_csv_bytes_and_explicit_grade(tmp_path):
    a, b = tmp_path / "a.csv.gz", tmp_path / "b.csv.gz"
    study.write_csv(a, [event()])
    study.write_csv(b, [event()])
    assert a.read_bytes() == b.read_bytes()
    assert study.GRADE == "HISTORICAL_PRICE_PROXY_NONEXECUTABLE"


def test_registered_count_decomposition_matches_paired_bootstrap():
    import summarize_synergy_evidence as diagnostic

    rows = ledger_fixture()[:2]
    extra = diagnostic.decomposition_intervals(rows, "A")
    paired = study.paired_blocks(rows, "A", 4, study.protocol())
    for name, value in extra.items():
        for p in ("P1", "P2"):
            assert value["arms"][p]["raw_delta_interval_bps"] == pytest.approx(
                paired[name]["paired_delta_vs_P0_intervals_bps"][p]
            )
        assert value["arms"]["P1"]["extra_vs_count_only_interval_bps"] == pytest.approx([2.25, 2.25])


def test_count_decomposition_does_not_change_selection_or_add_policy():
    import summarize_synergy_evidence as diagnostic

    rows = ledger_fixture()[:2]
    before = copy.deepcopy(rows)
    first = diagnostic.decomposition_intervals(rows, "A")
    assert rows == before
    assert first == diagnostic.decomposition_intervals(rows, "A")
    assert set(first["1_calendar_week_blocks"]["arms"]) == {"P1", "P2"}
