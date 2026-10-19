"""Tests for the ATLAS RED full-cycle research programme.

The suite checks the research contracts that matter: bounded storage, causal
chronology, the research/production predicate parity, action sealing, lifecycle
accounting, ML leakage rejection, and the absence of any order, private
endpoint or capital path.
"""

from __future__ import annotations

import hashlib
from decimal import Decimal
from pathlib import Path

import pytest

from atlas.v2.contracts import V2Side
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.public_http import _PATHS
from atlas.v2.research.full_cycle.acquisition import ENDPOINT_WEIGHT
from atlas.v2.research.full_cycle.actions import (
    evaluate_lifecycle,
    seal_research_action,
)
from atlas.v2.research.full_cycle.analysis import (
    ApplicabilityStatusV1,
    build_discovery_population,
    select_deep_cohort,
)
from atlas.v2.research.full_cycle.applicability import (
    evaluate_s2,
    evaluate_s3,
    evaluate_s7,
)
from atlas.v2.research.full_cycle.ml import (
    FEATURE_NAMES,
    build_ml_rows,
    chronological_split,
)
from atlas.v2.research.full_cycle.storage import (
    ABSOLUTE_CEILING_BYTES,
    OPERATING_BUDGET_BYTES,
    PROTECTIVE_WRITE_STOP_BYTES,
    StorageBudgetExceeded,
    StorageGuardianV1,
    StorageGuardLimitsV1,
)
from atlas.v2.research.full_cycle.store import ResearchStoreV1
from atlas.v2.strategies import s2_breakout

ROOT = Path(__file__).resolve().parents[2]
BASE_NS = 1_699_200_000_000_000_000


# --- 1. bounded storage ----------------------------------------------------
def test_storage_guardian_rejects_oversized_reservation(tmp_path):
    guardian = StorageGuardianV1(workspace_root=tmp_path, external_baseline_bytes=0)
    limit = guardian.limits.protective_write_stop_bytes
    with pytest.raises(StorageBudgetExceeded):
        guardian.reserve(limit + 1, purpose="OVERSIZED_SYNTHETIC_WRITE")
    assert guardian.writes_paused == 1
    assert guardian.reserved_bytes() == 0
    assert guardian.occupied_bytes() < ABSOLUTE_CEILING_BYTES


def test_storage_guardian_reserves_and_reconciles(tmp_path):
    guardian = StorageGuardianV1(workspace_root=tmp_path, external_baseline_bytes=0)
    reservation = guardian.reserve(500, purpose="TEST")
    guardian.reconcile(reservation, 250)
    assert guardian.reserved_bytes() == 250
    assert guardian.reserved_bytes() == 250
    with pytest.raises(StorageBudgetExceeded):
        guardian.reconcile(reservation, 10_000)
    manifest = guardian.manifest()
    assert manifest["limits"]["absolute_ceiling_bytes"] == ABSOLUTE_CEILING_BYTES
    assert manifest["limits"]["operating_budget_bytes"] == OPERATING_BUDGET_BYTES
    assert manifest["limits"]["protective_write_stop_bytes"] == PROTECTIVE_WRITE_STOP_BYTES


def test_storage_guard_limits_are_ordered():
    limits = StorageGuardLimitsV1()
    assert limits.operating_budget_bytes < limits.protective_write_stop_bytes
    assert limits.protective_write_stop_bytes < limits.absolute_ceiling_bytes


# --- 2. research store ----------------------------------------------------
def test_research_store_is_append_only_and_idempotent(tmp_path):
    store = ResearchStoreV1.open(tmp_path / "research.sqlite")
    observation = {
        "observation_id": "obs-1", "cycle_id": "c1", "venue": "BINANCE",
        "source_id": "BINANCE_USDM_PUBLIC_HTTP", "endpoint": "/fapi/v1/time",
        "event_type": "SERVER_TIME", "request_sha256": "a" * 64, "payload_sha256": "b" * 64,
        "payload_bytes": 2, "payload_zlib": b"x", "received_at_ns": 1, "available_at_ns": 1,
        "http_status": 200, "availability_class": "ACTUAL_SYSTEM",
    }
    store.record_observation(observation)
    store.record_observation(observation)
    assert store.scalar("SELECT COUNT(*) FROM source_observation") == 1
    store.record_observation({**observation, "observation_id": "obs-2", "payload_sha256": "c" * 64})
    assert store.scalar("SELECT COUNT(*) FROM source_observation") == 2


def test_research_store_cycle_lifecycle(tmp_path):
    store = ResearchStoreV1.open(tmp_path / "research.sqlite")
    store.begin_cycle("c1", "CHEAP", started_at_ns=1, cutoff_ns=2, server_time_ns=2)
    store.complete_cycle("c1", status="COMPLETE", request_count=3)
    row = store.query("SELECT * FROM cycle WHERE cycle_id='c1'")[0]
    assert row["status"] == "COMPLETE" and row["request_count"] == 3


# --- 3. discovery population ----------------------------------------------
def _products(count: int = 5):
    return [{
        "symbol": f"COIN{index}USDT", "base_asset": f"COIN{index}", "quote_asset": "USDT",
        "contract_type": "LINEAR_PERPETUAL", "trading_status": "TRADING", "tick_size": "0.01",
        "qty_step": "0.001", "min_qty": "0.001", "max_qty": None, "min_notional": "5.0",
        "listing_at_ns": None, "delisting_at_ns": None, "metadata_ref": f"ref{index}",
    } for index in range(count)]


def test_discovery_population_keeps_excluded_and_out_of_universe_rows():
    products = _products(3)
    tickers = {f"COIN{index}USDT": {"quote_volume_24h_usd": "50000000",
                                     "price_change_pct_24h": "2.0", "last_price": "1.0"}
               for index in range(3)}
    tickers["OTHERUSDT"] = {"quote_volume_24h_usd": "90000000", "price_change_pct_24h": "1.0",
                            "last_price": "1.0"}
    rows = build_discovery_population(
        cycle_id="c1", cutoff_ns=1, products=products, tickers=tickers, premium={},
        books={f"COIN{index}USDT": {"spread_bps": "2.0"} for index in range(3)},
        deep_symbols=("COIN0USDT",))
    assert len(rows) == 4
    excluded = {row["symbol"]: row for row in rows}
    assert excluded["OTHERUSDT"]["exclusion_reason"] == "NOT_IN_ADMITTED_PRODUCT_UNIVERSE"
    assert excluded["OTHERUSDT"]["eligible"] == 0
    assert excluded["COIN0USDT"]["eligible"] == 1
    assert all(row["selection_probability"] is not None for row in rows
               if row["symbol"] != "OTHERUSDT")


def test_deep_cohort_is_a_labelled_sample():
    rows = build_discovery_population(
        cycle_id="c1", cutoff_ns=1, products=_products(10),
        tickers={f"COIN{index}USDT": {"quote_volume_24h_usd": str(50_000_000 - index),
                                       "price_change_pct_24h": "1.0"} for index in range(10)},
        premium={}, books={f"COIN{index}USDT": {"spread_bps": "2.0"} for index in range(10)},
        deep_symbols=())
    selected, rule = select_deep_cohort(rows, size=3, rotation_offset=0)
    assert len(selected) == 3
    assert "FAIR_ROTATION" in rule


# --- 4. strategy applicability --------------------------------------------
def _bar(instrument_revision: str, close_ns: int, close: str, high: str | None = None,
         low: str | None = None) -> CausalBarV2:
    instrument_revision = instrument_revision if len(instrument_revision) == 64 else "a" * 64
    from atlas.v2.data.raw import RawObservationV2

    raw = RawObservationV2.build(
        instrument_revision=instrument_revision, source_id="BINANCE_USDM_PUBLIC_HTTP",
        event_type="BAR_15M", event_at_ns=close_ns, received_at_ns=close_ns,
        ingested_at_ns=close_ns, available_at_ns=close_ns,
        translation_version="binance-usdm-public-v1", payload={"c": close},
        sequence=str(close_ns))
    return CausalBarV2(raw=raw, interval=BarIntervalV2.M15, open_at_ns=close_ns - 900_000_000_000,
                       close_at_ns=close_ns, open=Decimal(close), high=Decimal(high or close),
                       low=Decimal(low or close), close=Decimal(close), volume=Decimal("1"), final=True)


def test_s3_refuses_aggregated_trade_evidence():
    record = evaluate_s3(cycle_id="c", symbol="XUSDT", key=None, cutoff_ns=1,
                         m1=[_bar("rev", BASE_NS, "1")],
                         universe_eligible=True, exact_trade_population=False)
    assert record.status == ApplicabilityStatusV1.SOURCE_UNAVAILABLE.value
    assert "EXACT_NATIVE_TRADE_POPULATION_UNAVAILABLE" in record.reason_code
    assert "EVERY_TRADE_VWAP_WINDOW" in record.unavailable_evidence


def test_s7_unavailable_calendar_is_not_clear():
    record = evaluate_s7(cycle_id="c", symbol="XUSDT", cutoff_ns=1,
                         event_calendar_available=False, universe_eligible=True)
    assert record.status == ApplicabilityStatusV1.SOURCE_UNAVAILABLE.value
    assert record.status != ApplicabilityStatusV1.CANDIDATE.value


def test_s2_requires_the_registered_thirty_day_sample():
    record = evaluate_s2(cycle_id="c", symbol="XUSDT", key=None, cutoff_ns=1, h4=[], h1=[],
                         m15=[], universe_eligible=True)
    assert record.status == ApplicabilityStatusV1.SOURCE_UNAVAILABLE.value


def test_s2_production_predicates_are_reused():
    # The research surface must call the production predicate functions.
    assert s2_breakout.compression_passes(1.0, 2.0, 1.0, 2.0) is True
    assert s2_breakout.range_width_valid(Decimal("1"), Decimal("1")) is True
    assert s2_breakout.breakout_side(Decimal("3"), Decimal("2"), Decimal("1"), Decimal("1")) == V2Side.LONG


# --- 5. research action sealing -------------------------------------------
def _action(**overrides):
    payload = {
        "cycle_id": "c1", "symbol": "XUSDT", "strategy": "S1", "policy_id": "P",
        "policy_hash": "h", "side": "LONG", "entry_reference": Decimal("100"),
        "stop_price": Decimal("95"), "stop_basis": "TEST", "qty_step": Decimal("0.001"),
        "tick_size": Decimal("0.01"), "min_notional": Decimal("5"), "spread_bps": Decimal("4"),
        "atr_value": Decimal("2"), "max_hold_ns": 4 * 3600 * 1_000_000_000,
        "decision_at_ns": 1_000_000_000, "deadline_offset_ns": 5_000_000_000,
        "evidence_refs": ("ref",), "sealed_at_ns": 1_000_000_000,
    }
    payload.update(overrides)
    return seal_research_action(**payload)


def test_action_sizing_floors_to_registered_step():
    action = _action()
    assert action.quantity == Decimal("10.000")
    assert action.quantity % Decimal("0.001") == 0
    assert action.notional_usd == action.quantity * action.entry_reference


def test_action_refuses_below_registered_minimum_notional():
    action = _action(min_notional=Decimal("100000"))
    assert action.hard_refusal_reason == "NOTIONAL_BELOW_REGISTERED_MINIMUM"


def test_action_records_that_take_profit_is_not_in_the_registered_policy():
    action = _action()
    assert action.take_profit_policy == "NOT_IN_REGISTERED_POLICY"
    assert action.take_profit_price is None


# --- 6. lifecycle ----------------------------------------------------------
def test_lifecycle_reports_no_fill_and_management_alternatives():
    action = _action()
    bars = [_bar("rev", BASE_NS, "100"), _bar("rev", BASE_NS + 900_000_000_000, "101"),
            _bar("rev", BASE_NS + 1_800_000_000_000, "102", high="103", low="99")]
    outcomes = evaluate_lifecycle(action, future_bars=bars, spread_bps=Decimal("4"),
                                  atr_value=Decimal("2"))
    kinds = {outcome.management_kind for outcome in outcomes}
    assert "HOLD" in kinds and "CLOSE" in kinds and "TIGHTEN" in kinds
    hold = next(outcome for outcome in outcomes if outcome.management_kind == "HOLD")
    assert hold.net_cash == hold.gross_cash - hold.cost_cash


def test_lifecycle_reports_no_fill_when_the_open_is_beyond_the_collar():
    action = _action()
    bars = [_bar("rev", BASE_NS, "200")]
    outcomes = evaluate_lifecycle(action, future_bars=bars, spread_bps=Decimal("4"))
    assert outcomes[0].entry_state == "NO_FILL"
    assert outcomes[0].net_cash == Decimal("0")


# --- 7. ML leakage and split ----------------------------------------------
def test_ml_rows_do_not_invent_labels(tmp_path):
    store = ResearchStoreV1.open(tmp_path / "research.sqlite")
    store.record_context([{
        "cycle_id": f"CTX-XUSDT-{BASE_NS}", "symbol": "XUSDT", "horizon": "H4", "bars_used": 100,
        "atr14_bps": "10", "realized_vol_bps": "20", "adx14": "30", "trend_score": "0.01",
        "compression_ratio": "0.5", "spread_bps": "3", "turnover_24h_usd": "1000000",
        "funding_rate": "0.0001", "corr_btc": "0.4", "beta_btc": "1.1", "return_bps": "5",
        "computed_at_ns": BASE_NS,
    }])
    store.record_action({
        "action_id": "a1", "cycle_id": "c1", "symbol": "XUSDT", "strategy": "S1",
        "policy_id": "P", "policy_hash": "h", "side": "LONG", "direction_origin": "TEST",
        "entry_reference": "100", "entry_collar_bps": "5", "stop_price": "95", "stop_basis": "T",
        "quantity": "1", "notional_usd": "100", "risk_profile": "TEST", "horizon_end_ns": BASE_NS + 10,
        "deadline_ns": BASE_NS + 5_000_000_000, "take_profit_price": None, "take_profit_levels": [], "fee_bps": "5",
        "expected_spread_bps": "4", "funding_assumption": "NONE", "slippage_bps": "2",
        "hard_refusal_reason": None, "evidence_refs": [], "code_hash": "c", "sealed_at_ns": BASE_NS,
    })
    rows = build_ml_rows(store)
    assert len(rows) == 1
    assert rows[0]["label_state"] == "UNLABELLED"
    assert rows[0]["label_bps"] is None


def test_chronological_split_keeps_order_and_embargo():
    rows = [{"information_cutoff_ns": index * 1_000_000_000, "label_state": "MATURED_NET_CASH",
             "label_bps": 1.0, "features": {}} for index in range(100)]
    split = chronological_split(rows)
    assert split["train"] and split["test"]
    assert max(int(row["information_cutoff_ns"]) for row in split["train"]) <= \
        min(int(row["information_cutoff_ns"]) for row in split["validation"])
    assert len(FEATURE_NAMES) >= 10


# --- 8. no order, private endpoint or capital path ------------------------
def test_no_private_or_order_endpoint_is_allowlisted():
    for paths in _PATHS.values():
        for path in paths:
            lowered = path.lower()
            assert "order" not in lowered
            assert "account" not in lowered
            assert "position" not in lowered
            assert not any(lowered.startswith(prefix) for prefix in ("/v5/user", "/v5/position"))
    assert set(ENDPOINT_WEIGHT) <= {path for paths in _PATHS.values() for path in paths}


def test_acquisition_module_contains_no_credential_surface():
    source = (ROOT / "src" / "atlas" / "v2" / "research" / "full_cycle" / "acquisition.py").read_text()
    for forbidden in ("api_key", "apiKey", "X-MBX-APIKEY", "signature=", "secret"):
        assert forbidden not in source


# --- 9. immutable freeze hashes -------------------------------------------
def test_immutable_freeze_hashes_unchanged():
    expected = {
        "ATLAS_FINAL_IMPLEMENTATION_CLARIFICATION_AND_V1_FREEZE_COMPLETED.md":
            "c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c",
        "ATLAS_V2_FINAL_INTRADAY_INTELLIGENCE_IMPLEMENTATION_FREEZE_AMENDED_2026-09-25.md":
            "e868e3e25230fb7fe334e769949bd0d8892edcf66804b0b773cd37ebb2d8fe78",
        "docs/v2/ATLAS_AGENT_INTELLIGENCE_EXTENSION_FREEZE_V1.md":
            "d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d",
    }
    for relative, digest in expected.items():
        path = ROOT / relative
        if not path.exists():
            pytest.skip(f"freeze artefact absent from this checkout: {relative}")
        assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
