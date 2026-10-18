"""Causal replay repair, actual-system golden parity and native boundary tests."""

from __future__ import annotations

import importlib.util
import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest
from v2.test_session014_core import KEY, bar

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.contracts import ReplayViewV2
from atlas.v2.data.bars import BarIntervalV2, CausalBarStoreV2
from atlas.v2.data.raw import AvailabilityClassV2
from atlas.v2.features.candles import DAY_NS, CausalTrade, prior_utc_day_range
from atlas.v2.features.joins import JoinedBars, asof_join
from atlas.v2.features.pipeline import (
    FEATURE_SET_VERSION,
    GAP_QUALIFIED_FEATURE_SET_VERSIONS,
    RECONSTRUCTED_FEATURE_SET_VERSION,
    feature_snapshot,
)

CUTOFF = 2 * DAY_NS + 5_000_000_000
LOCAL_READ = 1_792_332_000_000_000_000
ROOT = Path(__file__).resolve().parents[2]


def actual_join():
    return JoinedBars(
        KEY, CUTOFF,
        tuple(bar(i, interval=BarIntervalV2.H4) for i in range(12)),
        tuple(bar(i, interval=BarIntervalV2.H1) for i in range(48)),
        tuple(bar(i) for i in range(96, 192)),
        "AVAILABLE", None, sha256_json("SYNTHETIC_ACTUAL_HEALTH"),
    )


def reconstructed_bar(b, *, replay_at=None):
    return replace(b, raw=replace(
        b.raw, availability_class=AvailabilityClassV2.RECONSTRUCTED_MARKET,
        received_at_ns=LOCAL_READ, ingested_at_ns=LOCAL_READ, available_at_ns=LOCAL_READ,
        replay_available_at_ns=b.close_at_ns + 5_000_000_000 if replay_at is None else replay_at,
    ))


def replay_join():
    j = actual_join()
    return replace(j, h4=tuple(map(reconstructed_bar, j.h4)), h1=tuple(map(reconstructed_bar, j.h1)),
                   m15=tuple(map(reconstructed_bar, j.m15)), status="NOT_ESTIMABLE", reason="SOURCE_HEALTH_UNKNOWN")


def native_trade():
    return CausalTrade(KEY, Decimal("10"), Decimal("2"), 2 * DAY_NS + 1,
                       2 * DAY_NS + 2, "synthetic-native-trade", "native-1")


@pytest.mark.parametrize("trades", [False, True])
def test_actual_system_full_parent_artifact_golden_unchanged(trades):
    golden = json.loads((ROOT / "tests/fixtures/causal-replay/actual_system_feature_v1.json").read_text())
    actual = feature_snapshot(actual_join(), trades=(native_trade(),) if trades else ())
    name = "feature_with_trades" if trades else "feature_without_trades"
    assert canonical_json(actual.to_dict()) == canonical_json(golden[name])
    assert actual.feature_set_version == FEATURE_SET_VERSION


def test_prior_day_replay_uses_only_explicit_replay_clock_without_changing_receipt():
    j = replay_join()
    before = tuple(b.raw.to_dict() for b in j.m15)
    assert prior_utc_day_range(j.m15, cutoff_ns=j.cutoff_ns) == (None, None, ())
    high, low, refs = prior_utc_day_range(j.m15, cutoff_ns=j.cutoff_ns,
                                       availability_class=AvailabilityClassV2.RECONSTRUCTED_MARKET)
    assert (high, low) == (max(b.high for b in j.m15), min(b.low for b in j.m15))
    assert refs == tuple(b.content_hash for b in j.m15)
    assert tuple(b.raw.to_dict() for b in j.m15) == before
    assert all(b.raw.received_at_ns == LOCAL_READ > j.cutoff_ns for b in j.m15)


def test_prior_day_pipeline_repairs_two_values_and_uses_new_research_version():
    j = replay_join()
    f = feature_snapshot(j, replay_view=ReplayViewV2.RECONSTRUCTED_MARKET)
    assert f.feature_set_version == RECONSTRUCTED_FEATURE_SET_VERSION
    assert f.feature_set_version not in GAP_QUALIFIED_FEATURE_SET_VERSIONS
    for name, expected in (("high", Decimal("100.3")), ("low", Decimal("99.7"))):
        value = f.values["location.prior_day_" + name]
        assert value.value == expected and value.missing_reason is None
    assert f.replay_view is ReplayViewV2.RECONSTRUCTED_MARKET
    assert f.source_health_ref == j.source_health_ref
    assert f.envelope.artifact_id != feature_snapshot(actual_join()).envelope.artifact_id


@pytest.mark.parametrize("kind", ["first_gap", "interior_gap", "last_gap", "late", "actual", "conflict", "wrong_revision"])
def test_incomplete_late_mixed_or_conflicting_prior_day_never_qualifies(kind):
    bars = replay_join().m15
    if kind.endswith("gap"):
        index = {"first_gap": 0, "interior_gap": 37, "last_gap": 95}[kind]
        bars = bars[:index] + bars[index + 1:]
    elif kind == "late":
        bars = bars[:-1] + (reconstructed_bar(bar(191), replay_at=CUTOFF + 1),)
    elif kind == "actual":
        bars = bars[:-1] + (bar(191),)
    elif kind == "wrong_revision":
        bars = bars[:-1] + (reconstructed_bar(bar(191, key=replace(KEY, contract_revision="a" * 64))),)
    else:
        bars += (reconstructed_bar(bar(191, close="101")),)
    assert prior_utc_day_range(bars, cutoff_ns=CUTOFF,
                               availability_class=AvailabilityClassV2.RECONSTRUCTED_MARKET) == (None, None, ())


def test_last_prior_bar_becomes_visible_at_exact_replay_boundary():
    bars = replay_join().m15
    assert prior_utc_day_range(bars, cutoff_ns=CUTOFF - 1,
                               availability_class=AvailabilityClassV2.RECONSTRUCTED_MARKET) == (None, None, ())
    assert len(prior_utc_day_range(bars, cutoff_ns=CUTOFF,
                                  availability_class=AvailabilityClassV2.RECONSTRUCTED_MARKET)[2]) == 96


def test_future_current_day_and_duplicate_future_correction_do_not_enter_prior_day():
    bars = replay_join().m15
    future = reconstructed_bar(bar(192, close="1000"))
    late_revision = reconstructed_bar(bar(191, close="2000"), replay_at=CUTOFF + 1)
    args = {"cutoff_ns": CUTOFF, "availability_class": AvailabilityClassV2.RECONSTRUCTED_MARKET}
    assert prior_utc_day_range(bars, **args) == prior_utc_day_range(bars + (future, late_revision), **args)


@pytest.mark.parametrize("view", ["UNKNOWN", "EVENT_ONLY", "", None])
def test_undeclared_availability_view_refused(view):
    with pytest.raises((ValueError, TypeError)):
        prior_utc_day_range(replay_join().m15, cutoff_ns=CUTOFF, availability_class=view)


def test_replay_trade_visibility_not_inferred_from_actual_trade_dataclass():
    result = feature_snapshot(replay_join(), replay_view=ReplayViewV2.RECONSTRUCTED_MARKET,
                              trades=(native_trade(),))
    v = result.values["location.utc_day_trade_vwap_partial_to_cutoff"]
    assert v.value is None and v.missing_reason == "REPLAY_TRADE_AVAILABILITY_UNAVAILABLE"
    assert native_trade().ref not in result.envelope.input_refs
    assert feature_snapshot(actual_join(), trades=(native_trade(),)).values[
        "location.utc_day_trade_vwap_partial_to_cutoff"].value == Decimal("10")


@pytest.mark.parametrize("mode", ["future_bar", "late_available"])
def test_pipeline_still_refuses_unavailable_reconstructed_dependency(mode):
    j = replay_join()
    bad = reconstructed_bar(bar(192)) if mode == "future_bar" else reconstructed_bar(bar(191), replay_at=CUTOFF + 1)
    j = replace(j, m15=j.m15 + (bad,) if mode == "future_bar" else j.m15[:-1] + (bad,))
    with pytest.raises(ValueError, match="unavailable at cutoff"):
        feature_snapshot(j, replay_view=ReplayViewV2.RECONSTRUCTED_MARKET)


def test_asof_join_selects_replay_bars_without_minting_health_or_actual_view():
    store = CausalBarStoreV2()
    bars = replay_join().m15
    for b in bars:
        store.append(b)
    replay = asof_join(store, KEY, cutoff_ns=CUTOFF, view=AvailabilityClassV2.RECONSTRUCTED_MARKET)
    actual = asof_join(store, KEY, cutoff_ns=CUTOFF)
    assert replay.m15 == bars and not actual.m15
    assert (replay.status, replay.reason, replay.source_health_ref) == ("NOT_ESTIMABLE", "SOURCE_HEALTH_UNKNOWN", None)
    assert asof_join(store, KEY, cutoff_ns=CUTOFF - 1,
                     view=AvailabilityClassV2.RECONSTRUCTED_MARKET).m15 == bars[:-1]


def load_doc(name):
    import sys

    path = ROOT / "docs/research/atlas-red-causal-replay" / (name + ".py")
    spec = importlib.util.spec_from_file_location("causal_replay_" + name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_historical_seal_remains_immutable_and_refuses_changed_native_code():
    audit = load_doc("audit").foundation
    path = ROOT / "docs/research/atlas-red-native-trace/evidence"
    seal = json.loads((path / "TRACE_SEAL.json").read_text())
    assert seal["trace_sha256"] == "e4117caf8700f68987506d1cb6801fb1b6d262ac68d964d36092cea41d49e220"
    with pytest.raises(ValueError, match="SEALED_NATIVE_CODE_CHANGED:src/atlas/v2/features/"):
        audit.audit(path)


def test_expanded_current_seal_calendar_sources_and_no_qualified_actions():
    directory = ROOT / "docs/research/atlas-red-causal-replay/evidence"
    result = load_doc("audit").audit(directory)
    seal = json.loads((directory / "TRACE_SEAL.json").read_text())
    assert seal["source_sha"] == "f8d5d95f2492a9af959a4d48b0664233cf9aa123"
    assert result["original_minute_observations"] == 2100 and result["calendar_key_mismatches"] == 0
    assert seal["native_price_prefixes"] == {"complete": 132, "unavailable": 8}
    assert len(result["native_full_feature_observations"]) == 12
    assert result["reconstructed_prior_day_values"] == 24
    assert result["timing_misses"] == 132
    assert result["qualified_actions"] == result["orders"] == result["agent_calls"] == 0
    assert result["decision_bytes_unchanged"] and result["native_feature_dependencies_resolved"]


def test_readiness_matrix_covers_all_strategies_and_precise_downstream_contracts():
    import ast
    import hashlib

    matrix = json.loads((ROOT / "docs/research/atlas-red-causal-replay/SOURCE_READINESS.json").read_text())
    rows = {r["id"]: r for r in matrix["rows"]}
    assert set(rows) == {"FEATURES", "S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "PATH_B", "P1", "BRIEF", "AGENT_B", "MANAGEMENT"}
    assert not matrix["market_acquisition_authorized"] and matrix["authority"] == "ZERO"
    for row in rows.values():
        owner = row["owner"]
        raw = (ROOT / owner["path"]).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == owner["sha256"]
        symbol = owner["symbol"].split(".")[0]
        assert any(isinstance(n, (ast.ClassDef, ast.FunctionDef)) and n.name == symbol
                   and n.lineno == owner["line"] for n in ast.parse(raw).body)
        assert row["missing_historical_inputs"] and row["minimum_to_advance"]
        assert set(row["requirements"]) == {"indicator", "setup", "exact_action", "economic_evaluation", "live_authority"}
        if row["id"].startswith("S") or row["id"] == "PATH_B":
            assert row["native_policy_hashes"]
    assert rows["S2"]["causal_run"] == "BLOCKED_BEFORE_COMPRESSION"
    assert rows["AGENT_B"]["causal_run"] == "BLOCKED"


def test_frozen_mechanical_handoff_roundtrip_and_input_hashes():
    import hashlib

    module = load_doc("handoff")
    raw = json.loads((ROOT / "docs/research/atlas-red-causal-replay/LUNA_HANDOFF.json").read_text())
    parsed = module.MechanicalHandoffV1.from_dict(raw)
    assert canonical_json(parsed.to_dict()) == canonical_json(raw)
    assert len(parsed.tasks) == 3
    for name, expected in parsed.input_sha256:
        assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == expected


@pytest.mark.parametrize("kind", ["authority", "acquisition", "outcomes", "model", "operation", "output", "unpinned", "review"])
def test_mechanical_handoff_cannot_expand_science_or_authority(kind):
    raw = json.loads((ROOT / "docs/research/atlas-red-causal-replay/LUNA_HANDOFF.json").read_text())
    if kind == "authority":
        raw["authority"] = "CAPITAL"
    elif kind in {"acquisition", "outcomes", "model"}:
        raw[{"acquisition": "market_acquisition", "outcomes": "outcome_generation", "model": "model_generation"}[kind]] = True
    elif kind == "operation":
        raw["tasks"][0]["operation"] = "MODIFY_STRATEGY"
    elif kind == "output":
        raw["tasks"][0]["output_paths"] = ["src/atlas/v2/strategies/s1_trend.py"]
    elif kind == "unpinned":
        raw["tasks"][0]["input_paths"] = ["unbound.json"]
    else:
        raw["review_gate"] = "AUTO_APPROVED"
    with pytest.raises(ValueError):
        load_doc("handoff").MechanicalHandoffV1.from_dict(raw)


def test_real_parent_comparison_has_only_declared_changes_and_all_denominators():
    directory = ROOT / "docs/research/atlas-red-causal-replay/evidence"
    result = json.loads((directory / "PARENT_COMPARISON.json").read_text())
    seal = json.loads((directory / "TRACE_SEAL.json").read_text())
    assert result["current_trace_sha256"] == seal["trace_sha256"]
    assert result["s1_s6_native_component_mismatches"] == result["prefix_mismatches"] == 0
    assert result["matched_native_price_prefixes"] == 132 and result["same_source_blocks"] == 8
    assert result["same_original_minute_observation_keys"] == 2100
    assert result["matched_full_feature_slots"] == 3 and result["additional_full_feature_slots"] == 9
    assert result["changed_fields"] == {
        "location.prior_day_high": 3, "location.prior_day_low": 3,
        "location.utc_day_trade_vwap_partial_to_cutoff": 3,
    }
    assert result["full_feature_budget_denominators"] == {
        "DECLARED_FULL_FEATURE_RESOURCE_NOT_EVALUATED": 120, "NATIVE_CLOCK_NOT_DUE": 1960,
        "OBSERVED": 12, "SOURCE_UNAVAILABLE": 8,
    }
    assert result["replay_processing_delay_ns"] == {"6000000000": 99, "7000000000": 33}


def test_current_reader_cli_is_read_only_and_available_in_fresh_process():
    import subprocess
    import sys

    doc = ROOT / "docs/research/atlas-red-causal-replay"
    for name in ("replay.py", "audit.py", "compare.py"):
        result = subprocess.run([sys.executable, str(doc / name), "--help"], capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
