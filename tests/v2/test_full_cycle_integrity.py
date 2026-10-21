"""Adversarial correction oracles; synthetic cases never establish economic edge."""

import hashlib
import inspect
from decimal import Decimal as D

import pytest

from atlas.v2.research.full_cycle import actions, analysis, applicability, ml
from atlas.v2.research.full_cycle.actions import FundingSettlement
from atlas.v2.research.full_cycle.causality import CausalFact, EvidenceGrade, prospective_refusal
from atlas.v2.research.full_cycle.storage import StorageBudgetExceeded, StorageLayoutV2
from atlas.v2.research.full_cycle.store import ResearchStoreV1

from .test_atlas_red_full_cycle import _action, _bar

NS = 900_000_000_000
T = 1_792_425_600_000_000_000  # published BONK trigger, 2026-10-19 16:00 UTC


def test_published_bonk_cannot_be_prospective():
    a = _action(symbol="1000BONKUSDT", decision_at_ns=T - NS, sealed_at_ns=T + 6 * 3600 * 10**9)
    assert a.hard_refusal_reason == "MISSED_ORIGINAL_DEADLINE"


def test_policy_changes_action_identity():
    assert _action(policy_hash="a" * 64).action_id != _action(policy_hash="b" * 64).action_id


@pytest.mark.parametrize("field", ["policy_id", "policy_hash"])
def test_missing_policy_refused(field):
    with pytest.raises(ValueError, match="POLICY"):
        _action(**{field: ""})


def test_wrong_stop_side_rejected():
    with pytest.raises(ValueError, match="STOP"):
        _action(stop_price=D("105"))


def test_applicability_policy_survives_reopen(tmp_path):
    s = ResearchStoreV1.open(tmp_path / "r.sqlite")
    r = applicability.evaluate_s7(
        cycle_id="c", symbol="X", cutoff_ns=T, event_calendar_available=False, universe_eligible=True
    )
    s.record_applicability(r.as_row())
    read = ResearchStoreV1.open(s.path).query("select * from strategy_applicability")[0]
    assert read["policy_id"] == r.policy_id


def test_conflicting_applicability_refused(tmp_path):
    s = ResearchStoreV1.open(tmp_path / "r.sqlite")
    r = applicability.evaluate_s7(
        cycle_id="c", symbol="X", cutoff_ns=T, event_calendar_available=False, universe_eligible=True
    ).as_row()
    s.record_applicability(r)
    s.record_applicability(r)
    with pytest.raises(ValueError, match="IMMUTABLE"):
        s.record_applicability({**r, "source_grade": "QUALIFIED"})


def test_conflicting_raw_receipt_refused(tmp_path):
    s = ResearchStoreV1.open(tmp_path / "r.sqlite")
    r = {
        "observation_id": "o",
        "cycle_id": "c",
        "venue": "BINANCE",
        "source_id": "s",
        "endpoint": "p",
        "event_type": "BAR",
        "request_sha256": "a" * 64,
        "payload_sha256": "b" * 64,
        "payload_bytes": 2,
        "payload_zlib": b"xx",
        "received_at_ns": T,
        "available_at_ns": T,
    }
    s.record_observation(r)
    with pytest.raises(ValueError, match="IMMUTABLE"):
        s.record_observation({**r, "received_at_ns": T + 1})


def test_ml_purge_outcome_crosses_boundary():
    rows = [
        {
            "information_cutoff_ns": i * 100,
            "horizon_end_ns": i * 100 + 250,
            "label_available_at_ns": i * 100 + 251,
            "opportunity_id": str(i),
        }
        for i in range(100)
    ]
    split = ml.chronological_split(rows, embargo_ns=200)
    assert max(r["label_available_at_ns"] for r in split["train"]) + 200 < min(
        r["information_cutoff_ns"] for r in split["validation"]
    )
    assert max(r["label_available_at_ns"] for r in split["validation"]) + 200 < min(
        r["information_cutoff_ns"] for r in split["test"]
    )


def test_ml_repeated_opportunities_do_not_cross_folds():
    rows = [
        {
            "information_cutoff_ns": i * 100,
            "horizon_end_ns": i * 100,
            "label_available_at_ns": i * 100,
            "opportunity_id": "same" if i % 3 == 0 else str(i),
        }
        for i in range(100)
    ]
    s = ml.chronological_split(rows, embargo_ns=0)
    sets = [{r["opportunity_id"] for r in s[k]} for k in ("train", "validation", "test")]
    assert not sets[0] & sets[1] and not sets[0] & sets[2] and not sets[1] & sets[2]


def test_l2_challenger_is_mean_objective():
    source = inspect.getsource(ml.fit_and_compare)
    assert 'objective="regression_l1"' not in source
    assert 'objective="regression_l2"' in source


def test_maturity_refuses_gaps_and_early_evaluation():
    a = _action(decision_at_ns=T, sealed_at_ns=T, max_hold_ns=4 * NS)
    future = [_bar("rev", T + NS, "100"), _bar("rev", T + 4 * NS, "100")]
    assert actions.maturity_status(action=a, future_bars=future, evaluated_at_ns=T + 4 * NS)[0] != "MATURED"
    full = [_bar("rev", T + i * NS, "100") for i in range(1, 5)]
    assert actions.maturity_status(action=a, future_bars=full, evaluated_at_ns=T + NS)[0] != "MATURED"


def test_unknown_funding_not_silent_zero():
    a = _action(decision_at_ns=T, sealed_at_ns=T, max_hold_ns=4 * NS)
    future = [_bar("rev", T + i * NS, "100") for i in range(1, 5)]
    hold = actions.evaluate_lifecycle(a, future_bars=future, spread_bps=D("4"))[0]
    assert "FUNDING_UNOBSERVABLE" in hold.uncertainty


def test_stop_exit_records_actual_exposure_end():
    a = _action(decision_at_ns=T, sealed_at_ns=T, max_hold_ns=4 * NS)
    bars = [_bar("rev", T + i * NS, "90" if i == 1 else "110") for i in range(1, 5)]
    hold = actions.evaluate_lifecycle(a, future_bars=bars, spread_bps=D("4"))[0]
    assert hold.detail["exit_at_ns"] == T + NS


def test_discovery_proxy_never_claims_canonical_score():
    rows = analysis.build_discovery_population(
        cycle_id="c",
        cutoff_ns=T,
        products=[],
        tickers={"X": {"quote_volume_24h_usd": "100", "price_change_pct_24h": "1"}},
        premium={},
        books={},
        deep_symbols=[],
    )
    assert rows[0]["canonical_discovery_score"] is None
    assert rows[0]["discovery_policy_id"] == "RESEARCH_LIQUIDITY_ATTENTION_PROXY_V1"


def fact(kind="H1", **overrides):
    values = {
        "ref": "f",
        "kind": kind,
        "event_at_ns": T - 10,
        "confirmed_at_ns": T - 9,
        "received_at_ns": T - 8,
        "available_at_ns": T - 5,
        "ingested_at_ns": T - 7,
        "computed_at_ns": T - 6,
        "instrument_revision": "a" * 64,
        "source_health_ref": "health",
        "source_hash": "source",
        "code_hash": "code",
    }
    return CausalFact(**{**values, **overrides})


@pytest.mark.parametrize("kind", ["H1", "H4", "M15", "BBO", "FUNDING", "FEATURE"])
def test_future_fact_rejected(kind):
    assert fact(kind, available_at_ns=T + 1).refusal(T, "a" * 64) == "INPUT_UNAVAILABLE_AT_CUTOFF"


def test_missing_health_and_future_revision():
    assert fact(source_health_ref="").refusal(T, "a" * 64) == "SOURCE_HEALTH_OR_LINEAGE_MISSING"
    assert fact(instrument_revision="b" * 64).refusal(T, "a" * 64) == "POINT_IN_TIME_PRODUCT_REVISION_MISMATCH"
    assert fact(grade=EvidenceGrade.RECONSTRUCTED).refusal(T, "a" * 64) == "NOT_ORIGINAL_TIME_OBSERVATION"
    assert prospective_refusal([fact()], cutoff_ns=T, instrument_revision="a" * 64, required_kinds=["BBO"])


def test_genuinely_available_action_and_late_seal():
    facts = [fact(k) for k in ("M15", "H1", "H4", "PRODUCT", "UNIVERSE", "FEATURE", "BBO", "EVENT_GATE", "MARK_INDEX")]
    a = _action(
        decision_at_ns=T,
        sealed_at_ns=T,
        instrument_revision="a" * 64,
        evidence_grade=EvidenceGrade.PROSPECTIVE,
        facts=facts,
    )
    assert a.hard_refusal_reason is None
    assert a.as_row()["provenance"]["facts"][0]["received_at_ns"] == T - 8
    assert _action(decision_at_ns=T, sealed_at_ns=T + 6 * 10**9).hard_refusal_reason == "MISSED_ORIGINAL_DEADLINE"


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_signed_funding_and_early_close(side):
    a = _action(
        decision_at_ns=T,
        sealed_at_ns=T,
        max_hold_ns=4 * NS,
        side=side,
        stop_price=D("95") if side == "LONG" else D("105"),
    )
    bars = [_bar("rev", T + i * NS, "100") for i in range(1, 5)]
    # Irregular venue schedule; never inferred from UTC hour modulo 8.
    settlements = [
        FundingSettlement(T + 2 * NS, D(".001"), D("100"), T + 2 * NS, "f1"),
        FundingSettlement(T + 3 * NS, D(".002"), D("100"), T + 3 * NS, "f2"),
    ]
    outcomes = actions.evaluate_lifecycle(
        a, future_bars=bars, spread_bps=D("0"), funding_settlements=settlements, funding_coverage=(T, T + 4 * NS)
    )
    hold = outcomes[0]
    close = outcomes[1]
    assert D(close.detail["funding_cash"]) == 0
    expected = D("-3") if side == "LONG" else D("3")
    assert D(hold.detail["funding_cash"]) == expected
    assert hold.cost_cash == D(hold.detail["entry_fee"]) + D(hold.detail["exit_fee"]) - expected


def test_partial_fees_and_exposure_conserve_quantity():
    a = _action(
        decision_at_ns=T,
        sealed_at_ns=T,
        max_hold_ns=2 * NS,
        take_profit_price=D("110"),
        take_profit_policy="HALF_TP_AT_2R_COMPARATOR_V1",
    )
    bars = [_bar("rev", T + NS, "100", high="111", low="99"), _bar("rev", T + 2 * NS, "101")]
    p = next(
        o
        for o in actions.evaluate_lifecycle(
            a, future_bars=bars, spread_bps=D("0"), take_profit_price=D("110"), funding_coverage=(T, T + 2 * NS)
        )
        if o.management_kind == "PARTIAL_TP"
    )
    exits = p.detail["exits"]
    assert sum(D(e[1]) for e in exits) == a.quantity
    assert D(p.detail["exit_fee"]) == sum(D(e[1]) * D(e[2]) * D(".0005") for e in exits)


def test_same_bar_stop_target_is_adverse_bound():
    a = _action(
        decision_at_ns=T,
        sealed_at_ns=T,
        max_hold_ns=NS,
        take_profit_price=D("110"),
        take_profit_policy="HALF_TP_AT_2R_COMPARATOR_V1",
    )
    p = next(
        o
        for o in actions.evaluate_lifecycle(
            a,
            future_bars=[_bar("rev", T + NS, "100", high="111", low="90")],
            spread_bps=D("0"),
            take_profit_price=D("110"),
        )
        if o.management_kind == "PARTIAL_TP"
    )
    assert "STOP_TP_ORDER_UNOBSERVABLE" in p.uncertainty
    assert p.gross_cash < 0


def test_partial_fill_and_no_chase():
    a = _action(decision_at_ns=T, sealed_at_ns=T, max_hold_ns=NS)
    bars = [_bar("rev", T + NS, "100")]
    o = actions.evaluate_lifecycle(a, future_bars=bars, spread_bps=D("0"), filled_quantity=D("2"))[0]
    assert o.entry_state == "PARTIAL_FILL" and D(o.detail["quantity"]) == 2
    assert D(o.detail["entry_fee"]) == 2 * D(o.detail["entry_price"]) * D(".0005")
    assert actions.evaluate_lifecycle(a, future_bars=[_bar("rev", T + 2 * NS, "100")], spread_bps=D("0")) == ()


def test_outcome_versions_append_not_replace(tmp_path):
    s = ResearchStoreV1.open(tmp_path / "r.sqlite")
    r = {
        "action_id": "a",
        "cycle_id": "c",
        "symbol": "X",
        "strategy": "S1",
        "maturity_status": "PENDING",
        "horizon_ns": T,
        "net_cash": None,
        "return_bps": None,
        "max_favorable_bps": None,
        "max_adverse_bps": None,
        "censoring_reason": "WAIT",
        "evaluated_at_ns": T,
        "evaluation_version": "v2",
    }
    s.record_outcome(r)
    s.record_outcome(r)
    s.record_outcome({**r, "maturity_status": "CENSORED", "evaluated_at_ns": T + 1})
    assert s.scalar("select count(*) from outcome_revision") == 2
    assert s.query("select * from effective_outcome")[0]["maturity_status"] == "CENSORED"
    s.close()
    s = ResearchStoreV1.open(s.path)
    assert s.scalar("PRAGMA integrity_check") == "ok"
    assert s.scalar("select count(*) from outcome_revision") == 2


def test_permanent_excluded_and_space_floor(tmp_path):
    temp = tmp_path / "temp"
    permanent = tmp_path / "permanent"
    temp.mkdir()
    permanent.mkdir()
    layout = StorageLayoutV2(temp, permanent, physical_free_floor_bytes=10**30)
    with pytest.raises(StorageBudgetExceeded, match="PHYSICAL"):
        layout.reserve_physical(1)
    with pytest.raises(StorageBudgetExceeded, match="QUOTA"):
        StorageLayoutV2(temp, permanent).verify()
    with pytest.raises(ValueError, match="OVERLAP"):
        StorageLayoutV2(temp, temp / "models")


def test_interrupted_verified_placement(tmp_path):
    temp = tmp_path / "temp"
    permanent = tmp_path / "permanent"
    temp.mkdir()
    permanent.mkdir()
    source = temp / "source"
    source.write_bytes(b"original evidence")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    target = permanent / "checkpoint"
    target.with_name(target.name + ".atlas-part").write_bytes(b"original")
    layout = StorageLayoutV2(temp, permanent, physical_free_floor_bytes=0)
    layout.staged_copy(source, target, expected_bytes=17, expected_sha256=digest)
    assert target.read_bytes() == source.read_bytes()
    assert source.exists()


def test_connected_native_s1_to_censored_ml(tmp_path):
    from atlas.v2._serialization import sha256_json
    from atlas.v2.instruments import ProductContractV2, TradingStatusV2
    from atlas.v2.research.full_cycle.native import persist_modeled_outcomes, seal_native_candidate

    from .test_session014_s1 import (
        KEY,
        SETUP,
        ExecutableQuote,
        MarkIndexEvidence,
        OpsRepository,
        S1ShadowCoordinator,
        asof_join,
        bar,
        clear,
        feature_snapshot,
        store_for_side,
        universe_at,
    )

    market = store_for_side("LONG")
    setup = asof_join(market, KEY, cutoff_ns=SETUP)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        coordinator = S1ShadowCoordinator(repo)
        watch = coordinator.create_watch(
            setup, feature_snapshot(setup), event_gate=clear(SETUP), universe=universe_at(SETUP)
        ).watch
        assert watch is not None
        trigger = bar(800, close="105.55")
        market.append(trigger)
        cutoff = trigger.close_at_ns
        join = asof_join(market, KEY, cutoff_ns=cutoff, trigger_ref=trigger.content_hash)
        feat = feature_snapshot(join)
        quote = ExecutableQuote(KEY, D("105.54"), D("105.56"), cutoff, cutoff, sha256_json({"quote": cutoff}))
        mark = MarkIndexEvidence(KEY, D("105.55"), D("105.55"), cutoff, sha256_json({"mark": cutoff}))
        native = coordinator.on_bar(watch.watch_id, join, feat, event_gate=clear(cutoff), bbo=quote, mark_index=mark)
        assert native.candidate is not None
        assert (
            coordinator.on_bar(
                watch.watch_id, join, feat, event_gate=clear(cutoff), bbo=quote, mark_index=mark
            ).candidate
            is None
        )
        product = ProductContractV2(
            KEY,
            cutoff - 100,
            cutoff - 100,
            cutoff - 100,
            D(1),
            D(".01"),
            D(".001"),
            D(".001"),
            TradingStatusV2.TRADING,
            sha256_json({"product": 1}),
            min_notional=D(5),
        )
        facts = [
            fact(
                k,
                event_at_ns=cutoff - 10,
                confirmed_at_ns=cutoff - 9,
                received_at_ns=cutoff - 8,
                ingested_at_ns=cutoff - 7,
                computed_at_ns=cutoff - 6,
                available_at_ns=cutoff - 5,
                instrument_revision=KEY.contract_revision,
            )
            for k in ("M15", "H1", "H4", "PRODUCT", "UNIVERSE", "FEATURE", "BBO", "EVENT_GATE", "MARK_INDEX")
        ]
        a = seal_native_candidate(
            native.candidate,
            product=product,
            facts=facts,
            setup_at_ns=SETUP,
            sealed_at_ns=cutoff,
            atr_value=D(str(feat.values["m15.atr14"].value)),
            spread_bps=D("2"),
            cycle_id="fixture-native",
        )
        assert a.hard_refusal_reason is None and a.setup_at_ns < a.trigger_at_ns == a.decision_at_ns
        store = ResearchStoreV1.open(tmp_path / "research.sqlite")
        future = [bar(801, close="105.5")]
        paths = persist_modeled_outcomes(
            store, a, future_bars=future, spread_bps=D("2"), evaluated_at_ns=future[0].raw.available_at_ns
        )
        assert paths and all(p["action_id"] == a.action_id for p in paths)
        assert store.query("select * from effective_outcome")[0]["maturity_status"] == "CENSORED"
        assert ml.build_ml_rows(store)[0]["label_bps"] is None


def test_s1_reconstruction_first_event_and_trigger_time():
    from .test_session014_s1 import KEY, SETUP, asof_join, bar, store_for_side

    market = store_for_side("LONG")
    market.append(bar(800, close="105.55"))
    market.append(bar(801, close="107"))
    j = asof_join(market, KEY, cutoff_ns=SETUP + 2 * NS)
    records = applicability.evaluate_s1_watch_lifecycle(
        cycle_id="BONK-case",
        symbol="1000BONKUSDT",
        key=KEY,
        m15=j.m15,
        h1=j.h1,
        h4=j.h4,
        universe_eligible=True,
        setup_cache={},
        max_trigger_bars=64,
    )
    triggered = [r for r in records if r.reason_code == "TRIGGER_CONFIRMED"]
    assert len(triggered) == 1
    r = triggered[0]
    assert r.information_cutoff_ns == r.detail["trigger_close_at_ns"]
    assert r.detail["setup_at_ns"] < r.information_cutoff_ns
    assert r.source_grade == "RECONSTRUCTED_HISTORICAL_RESEARCH"


def test_ml_fitted_l2_real_import_and_missing_indicators():
    rows = []
    day = 86_400_000_000_000
    for i in range(90):
        t = i * 4 * day
        rows.append(
            {
                "action_id": str(i),
                "opportunity_id": str(i),
                "symbol": f"C{i % 3}",
                "instrument_revision": "a" * 64,
                "policy_hash": "b" * 64,
                "information_cutoff_ns": t,
                "horizon_end_ns": t + NS,
                "label_available_at_ns": t + NS,
                "feature_available_at_ns": t,
                "evidence_grade": EvidenceGrade.PROSPECTIVE,
                "label_state": "MATURED_NET_CASH",
                "label_bps": float(i % 7 - 4),
                "features": {k: None if i % 3 == 0 else float(i % 9) for k in ml.FEATURE_NAMES},
            }
        )
    result = ml.fit_and_compare(rows)
    assert result["status"] == "HISTORICAL_DIAGNOSTIC"
    assert result["economic_authority"] == "NOT ESTIMABLE"
    assert "test_rmse_bps" in result["models"]["LIGHTGBM_L2_CASH_MEAN"]


def test_all_context_roles_and_missing_sources_stay_explicit():
    common = {"cycle_id": "fixture", "symbol": "X", "cutoff_ns": T, "universe_eligible": True}
    assert (
        applicability.evaluate_s4(**common, depth_available=True, trade_evidence_available=False).status
        == "CONTEXT_ONLY"
    )
    assert (
        applicability.evaluate_s5(
            **common, oi_available=True, funding_available=True, liquidation_population_complete=False
        ).status
        == "SOURCE_UNAVAILABLE"
    )
    assert (
        applicability.evaluate_s6(**common, eligible_breadth=30, hours_used=720, score=1.0, beta_btc=1.0).status
        == "CONTEXT_ONLY"
    )
    assert (
        applicability.evaluate_s8(**common, pair_defined=False, hours_used=0).reason_code
        == "NO_REGISTERED_PAIR_FOR_SYMBOL"
    )
    assert applicability.evaluate_path_b(**common, eligible=True, novelty_score=None).status == "RESOURCE_NOT_EVALUATED"


def test_s2_native_source_case_and_missing_warmup():
    from .test_session016_s2 import fixture

    _, join, _, _, _ = fixture()
    r = applicability.evaluate_s2(
        cycle_id="fixture-s2",
        symbol="X",
        key=join.key,
        cutoff_ns=join.cutoff_ns,
        m15=join.m15,
        h1=join.h1,
        h4=join.h4,
        universe_eligible=True,
    )
    assert r.reason_code == "TRIGGER_CONFIRMED"
    missing = applicability.evaluate_s2(
        cycle_id="fixture-short",
        symbol="X",
        key=join.key,
        cutoff_ns=join.cutoff_ns,
        m15=join.m15[-20:],
        h1=join.h1,
        h4=join.h4,
        universe_eligible=True,
    )
    assert missing.reason_code == "INSUFFICIENT_30_DAY_HISTORY_AFTER_WARMUP"


def test_s2_failed_break_uses_frozen_range():
    a = _action(
        strategy="S2",
        decision_at_ns=T,
        sealed_at_ns=T,
        max_hold_ns=4 * NS,
        execution_plan={"range_low": "99", "range_high": "101"},
    )
    o = actions.evaluate_lifecycle(a, future_bars=[_bar("rev", T + NS, "100")], spread_bps=D(0))[0]
    assert o.exit_reason == "FAILED_BREAK" and o.detail["exit_at_ns"] == T + NS


def test_migration_retains_unqualified_legacy_and_meta(tmp_path):
    import sqlite3

    from atlas.v2.research.full_cycle.store import SCHEMA

    path = tmp_path / "legacy.sqlite"
    with sqlite3.connect(path) as c:
        c.executescript(SCHEMA)
        c.execute("insert into store_meta values('created_at_ns','123')")
        c.execute("insert into mature_outcome values('a','c','X','S1','MATURED',1,'10','100',NULL,NULL,NULL,2,'V1')")
    s = ResearchStoreV1.open(path)
    assert s.scalar("select net_cash from mature_outcome") == "10"
    assert s.scalar("select count(*) from effective_outcome") == 0
    assert s.get_meta("created_at_ns") == "123"
    assert s.scalar("PRAGMA integrity_check") == "ok"
    s.close()


def test_conflict_batch_rolls_back_and_terminal_cycle_is_immutable(tmp_path):
    s = ResearchStoreV1.open(tmp_path / "r.sqlite")
    s.begin_cycle("c", "CHEAP", started_at_ns=1, cutoff_ns=2)
    s.complete_cycle("c", status="COMPLETE", request_count=3)
    with pytest.raises(ValueError, match="IMMUTABLE"):
        s.complete_cycle("c", status="COMPLETE", request_count=4)
    from .test_atlas_red_full_cycle import _products

    products = [
        {**p, "observed_at_ns": T, "cycle_id": "c", "venue": "BINANCE", "product_ref": p["metadata_ref"]}
        for p in _products(2)
    ]
    s.record_products(products[:1])
    with pytest.raises(ValueError, match="IMMUTABLE"):
        s.record_products([products[1], {**products[0], "tick_size": ".02"}])
    assert s.scalar("select count(*) from product_metadata") == 1


def test_management_comparison_requires_matched_population():
    base = {
        "action_id": "a",
        "path_id": "p",
        "management_kind": "HOLD",
        "exit_state": "CLOSED",
        "net_cash": "-10",
        "detail": {"model_version": "v", "future_source_refs": ["raw"]},
    }
    other = {**base, "action_id": "b", "management_kind": "CLOSE", "net_cash": "10"}
    assert analysis.paired_management_comparisons([base, other]) == {}
    pair = {**other, "action_id": "a", "net_cash": "-5"}
    result = analysis.paired_management_comparisons([base, pair])
    assert result["CLOSE"]["cash_deltas"] == ["5"]
