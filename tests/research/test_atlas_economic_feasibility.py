"""Bounded native contract probes; every constructed input is synthetic offline.

These tests do not generate historical actions, targets, models or outcomes.
Genuine historical execution stops before strategy qualification.
"""

from __future__ import annotations

import importlib.util
import json
from dataclasses import fields, replace
from decimal import Decimal
from pathlib import Path

import pytest
from v2.test_session016_s2 import fixture as s2_fixture

from atlas.v2._serialization import sha256_json
from atlas.v2.contracts import ReplayViewV2
from atlas.v2.data.bars import CausalBarStoreV2
from atlas.v2.data.raw import AvailabilityClassV2
from atlas.v2.features.joins import asof_join
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.research.mean_selection import validate_offline_b_selection
from atlas.v2.risk import IndependentRiskProfileV1
from atlas.v2.science.costs import FeeScheduleV2
from atlas.v2.science.mean_calibration import FEATURE_REF, GRADE, MeanEvidenceScopeV1
from atlas.v2.science.mean_common import DAY_NS
from atlas.v2.science.mean_evaluation import CandidateMeanInputsV1, read_mean_artifact
from atlas.v2.science.mean_models import MeanModelV1, MeanTrainingRowV1, eligible_mean_rows
from atlas.v2.science.mean_population import MeanPopulationRegistrationV1
from atlas.v2.science.mean_scenarios import MeanScenarioSetV1
from atlas.v2.strategies.s2_breakout import S2ShadowCoordinator

ROOT = Path(__file__).resolve().parents[2]
DOC = ROOT / "docs/research/atlas-red-economic-feasibility"
H = sha256_json("SYNTHETIC_FEASIBILITY_FIXTURE")


@pytest.mark.parametrize("grade", ["COMPLETE_ACTUAL", "QUALIFIED_MODELLED", "PROSPECTIVE_SHADOW"])
def test_native_calibration_scope_refuses_unimplemented_genuine_grade(grade):
    good = MeanEvidenceScopeV1(H, H, H, FEATURE_REF, DAY_NS, (0.0,) * 6, (1.0,) * 6,
                               1, 1_000, "CAUSAL_REPLAY", GRADE)
    with pytest.raises(ValueError, match="genuine market qualification remains separate"):
        replace(good, evidence_grade=grade)


@pytest.mark.parametrize("grade", ["COMPLETE_ACTUAL", "QUALIFIED_MODELLED", "PROSPECTIVE_SHADOW"])
def test_native_original_population_cannot_be_relabelled_genuine(grade):
    good = MeanPopulationRegistrationV1(H, H, H, H, "SYNTHETIC_ONLY", ((200, 201, 205),), 100)
    with pytest.raises(ValueError, match="INVALID_ORIGINAL_POPULATION_REGISTRATION"):
        replace(good, evidence_grade=grade)


@pytest.mark.parametrize("grade", ["RECONSTRUCTED_MARKET", "UNKNOWN", "PRICE_DIAGNOSTIC"])
def test_price_only_shape_cannot_qualify_mean_cash_training(grade):
    # A zero here is a synthetic guard input, never a missing historical outcome.
    row = MeanTrainingRowV1(H, H, H, 100, 100, 200, 201, (0.0,) * 6, Decimal(0),
                            "CAUSAL_REPLAY", "FULL_FILL", grade)
    assert eligible_mean_rows((row,), cutoff_ns=300, compatibility_ref=H, provenance="CAUSAL_REPLAY") == ()


def test_no_fill_cannot_carry_invented_positive_trading_cash():
    with pytest.raises(ValueError, match="no fill has zero trading cash"):
        MeanTrainingRowV1(H, H, H, 100, 100, 200, 201, (0.0,) * 6, Decimal(1),
                           "DIAGNOSTIC", "NO_FILL", "PRICE_DIAGNOSTIC")


def test_missing_agent_cannot_become_deterministic_opening():
    with pytest.raises(ValueError, match="PROVIDER_UNAVAILABLE_NO_OPENING_FALLBACK"):
        validate_offline_b_selection(None, request=None, response=None, eligible=None,
                                     snapshot=None, now_ns=100, current_evaluation_refs=())


@pytest.mark.parametrize("view", [AvailabilityClassV2.ACTUAL_SYSTEM, AvailabilityClassV2.RECONSTRUCTED_MARKET])
def test_source_view_does_not_invent_missing_health(view):
    _, join, _, _, _ = s2_fixture()
    result = asof_join(CausalBarStoreV2(), join.key, cutoff_ns=join.cutoff_ns, view=view)
    assert (result.status, result.reason, result.source_health_ref) == ("NOT_ESTIMABLE", "SOURCE_HEALTH_UNKNOWN", None)


@pytest.mark.parametrize("missing", ["health", "bbo", "eligibility", "revision"])
def test_native_s2_indispensable_source_refusals_survive_without_actions(tmp_path, missing):
    _, join, feature, universe, quote = s2_fixture()
    if missing == "health":
        join = replace(join, status="NOT_ESTIMABLE", reason="SOURCE_HEALTH_UNKNOWN", source_health_ref=None)
        expected = "SOURCE_HEALTH_UNKNOWN"
    elif missing == "bbo":
        quote = None
        expected = "BBO_STALE_OR_UNAVAILABLE"
    elif missing == "eligibility":
        universe = replace(universe, envelope=replace(universe.envelope, content_hash=""),
                           entries=(replace(universe.entries[0], data_eligible=False),))
        expected = "UNIVERSE_INELIGIBLE"
    else:
        join = replace(join, key=replace(join.key, contract_revision="b" * 64))
        expected = "FEATURE_OR_JOIN_MISMATCH"
    with OpsRepository(tmp_path / "synthetic-only.sqlite") as repo:
        result = S2ShadowCoordinator(repo).on_trigger_close(join, feature, universe=universe, bbo=quote)
        assert result.reason == expected and result.candidate is None
        for kind in ["ExactActionV1", "EligibleSetV1", "Order", "TradePlanEnvelopeV2"]:
            assert not repo.artifact_entries(kind)


def test_receipt_cannot_be_removed_or_reconstructed_view_promoted():
    from test_atlas_native_trace import reconstructed

    join = reconstructed()
    raw = join.m15[-1].raw
    with pytest.raises(ValueError):
        replace(raw, received_at_ns=None)
    with pytest.raises(ValueError, match="cannot carry reconstructed replay availability"):
        replace(raw, availability_class=AvailabilityClassV2.ACTUAL_SYSTEM)
    assert raw.received_at_ns > join.cutoff_ns
    assert raw.availability_class == AvailabilityClassV2.RECONSTRUCTED_MARKET
    assert ReplayViewV2.RECONSTRUCTED_MARKET != ReplayViewV2.ACTUAL_SYSTEM


def test_verdict_covers_native_contracts_and_distinguishes_environment_from_sources():
    verdict = json.loads((DOC / "ECONOMIC_FEASIBILITY_VERDICT.json").read_text())
    assert verdict["primary_recommendation"] == "PROSPECTIVE_OBSERVATION_REQUIRED"
    assert verdict["A_complete_programme"]["status"] == "NOT_ESTIMABLE"
    assert verdict["B_bounded_historical"]["frozen_executable_subset_ready"] is False
    assert verdict["C_prospective_observe"]["activated"] is False
    assert verdict["genuine_exact_action_now"]["available"] is False
    assert verdict["genuine_exact_action_now"]["execution_attempted"] is False
    assert verdict["dependency_installation_alone_sufficient"] is False
    assert verdict["qualified_actions_generated"] == verdict["orders"] == verdict["new_market_requests"] == 0
    assert verdict["native_deadline"]["frozen_delay_ns"] == 5_000_000_000
    assert verdict["native_deadline"]["target_environment_qualified"] is False
    assert set(verdict["candidate_mean_input_fields"]) == {f.name for f in fields(CandidateMeanInputsV1)}
    assert {r["strategy"] for r in verdict["strategies"]} == {"S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "PATH_B"}
    assert {r["classification"] for r in verdict["decisive_gates"]} >= {
        "SOURCE_MISSING", "READER_MISSING", "CONTRACT_BLOCKED", "TIMING_UNVERIFIED", "BLOCKED_BY_ENVIRONMENT",
        "ECONOMIC_OUTCOME_NOT_IDENTIFIABLE",
    }


def test_supplied_public_probe_summary_is_not_retained_original_source():
    source = ROOT / "docs/v2/transition-r9-local-evidence/api-qualification.json"
    record = json.loads(source.read_text())
    rest = record["binance_production_public_rest"]
    ws = record["binance_production_public_websocket"]
    assert rest["raw_payload_saved"] is False and ws["raw_frames_saved"] is False
    assert rest["bar_and_history_enrichment"] == "DEFERRED_BY_ACQUISITION_BUDGET"
    assert record["classification"] != "COMPLETE_ATLAS_PROGRAMME"


def test_verified_genuine_source_absence_is_not_an_import_failure():
    inventory = json.loads((DOC / "evidence/LOCAL_OPS_INVENTORY.json").read_text())
    assert inventory["capture_journals_or_arrow_parquet_paths"] == []
    for db in inventory["offline_ops_inventory"]:
        assert not (set(db["artifact_counts"]) & {"CandidateActionV2", "ExactActionV1", "EligibleSetV1", "MeanPopulationSlotV1"})
    # Neither result imports or requires PyArrow. Installing it cannot create
    # absent original-time records or authorize a new evidence grade.
    assert isinstance(importlib.util.find_spec("pyarrow") is not None, bool)


@pytest.mark.parametrize("contract", [FeeScheduleV2, IndependentRiskProfileV1, MeanModelV1, MeanScenarioSetV1])
def test_native_typed_reader_refuses_missing_cost_risk_model_or_scenario(tmp_path, contract):
    with OpsRepository(tmp_path / "missing-contract.sqlite") as repo:
        with pytest.raises(ValueError):
            read_mean_artifact(repo, H, contract, consumer_ns=100)
        assert not repo.artifact_entries("ExactActionV1")
        assert not repo.artifact_entries("EligibleSetV1")


def test_native_public_feed_capabilities_do_not_grant_trade_or_liquidation_completeness():
    from atlas.v2.data.capabilities import CapabilityStatusV2, default_evidence_capability_matrix_v2

    rows = {r.source_channel: r for r in default_evidence_capability_matrix_v2().rows if r.venue == "BINANCE"}
    trade = rows["USD-M aggTrade WS"]
    liquidation = rows["USD-M forceOrder liquidation WS"]
    assert trade.status.value == "UNVERIFIED"
    assert "not every individual" in trade.coverage_censoring_limitations
    assert liquidation.status is CapabilityStatusV2.NOT_ESTIMABLE
    assert liquidation.status.value == "NOT ESTIMABLE"
    assert "no feed completeness" in liquidation.coverage_censoring_limitations
