"""Native-output parity and negative causal/persistence instrumentation cases."""

from __future__ import annotations

import ast
import importlib.util
import json
import pathlib
import sys
import time
from dataclasses import replace

import pytest
from v2.test_session014_core import KEY, bar
from v2.test_session014_s1 import SETUP, asof_join, clear, store_for_side, universe_at
from v2.test_session016_s2 import fixture as s2_fixture

from atlas.v2._serialization import FrozenMap, canonical_json, json_value, sha256_json
from atlas.v2.contracts import ReplayViewV2
from atlas.v2.data.bars import BarIntervalV2
from atlas.v2.data.raw import AvailabilityClassV2
from atlas.v2.features import pipeline
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.runtime import native_research_trace as trace
from atlas.v2.strategies import s1_trend as s1
from atlas.v2.strategies import s2_breakout as s2
from atlas.v2.strategies import s6_cross_section as s6

ROOT = pathlib.Path(__file__).resolve().parents[2]
DOC = ROOT / "docs/research/atlas-red-native-trace"
HOUR = BarIntervalV2.H1.duration_ns
M15 = BarIntervalV2.M15.duration_ns
REF = sha256_json("synthetic-source")
CODE = sha256_json("synthetic-code")


def reconstructed(side="LONG"):
    original = asof_join(store_for_side(side), KEY, cutoff_ns=SETUP)
    actual = time.time_ns()

    def cv(b):
        return replace(
            b,
            raw=replace(
                b.raw,
                availability_class=AvailabilityClassV2.RECONSTRUCTED_MARKET,
                received_at_ns=actual,
                ingested_at_ns=actual,
                available_at_ns=actual,
                replay_available_at_ns=b.close_at_ns + 5_000_000_000,
            ),
        )

    return replace(
        original,
        cutoff_ns=SETUP + 5_000_000_000,
        h4=tuple(map(cv, original.h4)),
        h1=tuple(map(cv, original.h1)),
        m15=tuple(map(cv, original.m15)),
    )


def publish_dependencies(writer):
    bodies = [{"fixture": "source", "authority": "ZERO"}, {"fixture": "code", "authority": "ZERO"}]
    refs = [sha256_json(b) for b in bodies]
    writer.publish({r: ("NativeTraceFixtureV1", b) for r, b in zip(refs, bodies, strict=True)})
    return refs


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_native_full_feature_and_s1_s6_outputs_unchanged(side):
    join = reconstructed(side)
    feature, native = trace.observe_prices(join, source_ref=REF)
    expected = pipeline.feature_snapshot(
        join, replay_view=ReplayViewV2.RECONSTRUCTED_MARKET, clock_ns=lambda: join.cutoff_ns + 1_000_000_000
    )
    assert canonical_json(feature["native_feature"]) == canonical_json(expected.to_dict())
    assert native["s1_precursor"]["native_result"] == json_value(s1._setup(join))
    trend, refs = s6._four_hour_context(join.h4, KEY, join.cutoff_ns, "RECONSTRUCTED_MARKET")
    assert native["s6_context"]["trend"] == trend
    assert native["s6_context"]["context_refs"] == list(refs)
    assert native["s1_precursor"]["status"] == "SETUP_OBSERVED"
    assert native["s1_precursor"]["candidate"] is None
    assert feature["native_feature"]["envelope"]["available_at_ns"] == SETUP + 6_000_000_000
    assert all(b.raw.received_at_ns > join.cutoff_ns for b in join.m15)


@pytest.mark.parametrize(
    "mode", ["future_close", "future_visibility", "actual_view", "wrong_revision", "gap", "stale_terminal", "empty"]
)
def test_future_incomplete_and_wrong_source_prefix_refused(mode):
    join = reconstructed()
    if mode == "future_close":
        future = bar(800)
        future = replace(
            future,
            raw=replace(
                future.raw,
                availability_class=AvailabilityClassV2.RECONSTRUCTED_MARKET,
                replay_available_at_ns=future.close_at_ns + 5_000_000_000,
            ),
        )
        join = replace(join, m15=join.m15 + (future,))
    elif mode == "future_visibility":
        b = join.m15[-1]
        b = replace(b, raw=replace(b.raw, replay_available_at_ns=join.cutoff_ns + 1))
        join = replace(join, m15=join.m15[:-1] + (b,))
    elif mode == "actual_view":
        b = join.m15[-1]
        b = replace(
            b, raw=replace(b.raw, availability_class=AvailabilityClassV2.ACTUAL_SYSTEM, replay_available_at_ns=None)
        )
        join = replace(join, m15=join.m15[:-1] + (b,))
    elif mode == "wrong_revision":
        join = replace(join, key=replace(KEY, contract_revision="a" * 64))
    elif mode == "gap":
        join = replace(join, m15=join.m15[:5] + join.m15[6:])
    elif mode == "stale_terminal":
        join = replace(join, m15=join.m15[:-1])
    else:
        join = replace(join, h1=())
    with pytest.raises(ValueError):
        trace.observe_components(join)


def test_missing_trades_profile_and_events_never_become_favorable_values():
    feature, _ = trace.observe_prices(reconstructed(), source_ref=REF)
    values = feature["native_feature"]["values"]
    for name in [
        "location.volume_profile",
        "location.anchored_vwap",
        "location.utc_day_trade_vwap",
        "regime.liquidity_state",
        "regime.event_state",
        "regime.crowding_state",
    ]:
        assert values[name]["value"] is None
        assert values[name]["missing_reason"]


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_native_coordinator_exact_watch_and_refusal_parity(tmp_path, side):
    join = asof_join(store_for_side(side), KEY, cutoff_ns=SETUP)
    feature = pipeline.feature_snapshot(join)
    with (
        OpsRepository(tmp_path / "expected.sqlite") as expected_repo,
        OpsRepository(tmp_path / "observed.sqlite") as repo,
    ):
        expected = s1.S1ShadowCoordinator(expected_repo).create_watch(
            join, feature, event_gate=clear(SETUP), universe=universe_at(SETUP)
        )
        expected_two = s2.S2ShadowCoordinator(expected_repo).on_trigger_close(
            join, feature, universe=universe_at(SETUP), bbo=None
        )
        actual = trace.observe_coordinators(repo, join, feature, universe=universe_at(SETUP), event_gate=clear(SETUP))
        assert actual["S1"]["native_result"] == trace.native_value(expected)
        assert actual["S2"]["native_result"] == trace.native_value(expected_two)
        assert actual["S1"]["native_result"]["status"] == "WATCH"
        assert not repo.artifact_entries("Order")


@pytest.mark.parametrize("missing", ["universe", "event_gate", "bbo"])
def test_unavailable_inputs_preserve_native_or_adapter_reason(tmp_path, missing):
    _, join, feature, universe, quote = s2_fixture()
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        result = trace.observe_coordinators(
            repo,
            join,
            feature,
            universe=None if missing == "universe" else universe,
            event_gate=None,
            bbo=None if missing == "bbo" else quote,
        )
        if missing == "universe":
            assert all(not r["native_invoked"] for r in result.values())
        elif missing == "event_gate":
            assert result["S1"]["native_result"]["reason"] == "EVENT_GATE_UNKNOWN_OR_STALE"
        else:
            assert result["S2"]["native_result"]["reason"] == "BBO_STALE_OR_UNAVAILABLE"
        assert not repo.artifact_entries("EligibleSetV1")


def test_complete_synthetic_s2_native_candidate_is_identical(tmp_path):
    _, join, feature, universe, quote = s2_fixture()
    with (
        OpsRepository(tmp_path / "expected.sqlite") as expected_repo,
        OpsRepository(tmp_path / "observed.sqlite") as repo,
    ):
        expected = s2.S2ShadowCoordinator(expected_repo).on_trigger_close(join, feature, universe=universe, bbo=quote)
        actual = trace.observe_coordinators(repo, join, feature, universe=universe, bbo=quote)
        assert expected.status == "CANDIDATE"
        assert actual["S2"]["native_result"] == trace.native_value(expected)
        assert actual["S2"]["native_result"]["candidate"]["quantity"] is None
        for kind in ["ExactActionV1", "EligibleSetV1", "TradePlanEnvelopeV2", "Order", "Reservation"]:
            assert not repo.artifact_entries(kind)


@pytest.mark.parametrize("offset", [0, 1, 14, 15, 59])
def test_native_clocks_deadline_and_complete_surface_accounting(offset):
    at = SETUP + offset * trace.MINUTE_NS
    row = trace.make_slot(key=KEY, at=at, source_ref=REF, code_ref=CODE, evaluated=True, native_ref=REF)
    states = row.stages["strategies"]
    assert set(states) == {"S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "PATH_B"}
    for name in ["S1", "S2"]:
        assert states[name]["due"] == (offset % 15 == 0)
        assert (states[name]["status"] == "NOT_DUE") == (offset % 15 != 0)
    assert states["S3"]["due"] is True
    for name in ["S4", "S5", "S7", "PATH_B"]:
        assert states[name]["due"] is None
    assert states["S8"]["due"] == (offset == 0)
    assert row.stages["economics"]["cash_label"] is None
    assert len(row.stages["economics"]["missing_CandidateMeanInputsV1"]) == 13
    assert row.stages["selection"]["fallback"] is None
    if offset % 15 == 0:
        assert row.stages["features"]["deadline_status"] == "MISSED"
        assert row.stages["features"]["replay_processing_ready_ns"] > row.stages["features"]["original_m15_deadline_ns"]


def test_non_applicable_is_not_failed_prediction_or_false_first_event():
    row = trace.make_slot(key=KEY, at=SETUP + trace.MINUTE_NS, source_ref=REF, code_ref=CODE)
    assert row.stages["strategies"]["S1"]["status"] == "NOT_DUE"
    assert row.stages["management"]["outcome_link"] is None
    assert trace.observe_components(reconstructed())["s1_precursor"]["watch_or_first_trigger_identity"] is None


def test_native_identity_mutation_and_stage_omission_refused():
    row = trace.make_slot(key=KEY, at=SETUP, source_ref=REF, code_ref=CODE)
    with pytest.raises(ValueError):
        replace(row, decision_id="a" * 64)
    with pytest.raises(ValueError):
        replace(row, stages=FrozenMap({"source": {}}))
    with pytest.raises(ValueError):
        trace.make_slot(key=KEY, at=SETUP + 1, source_ref=REF, code_ref=CODE)
    with pytest.raises(TypeError):
        row.stages["source"]["original_receipt"] = "fake"


def test_restart_duplicate_and_output_mutation_refusal(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        writer = trace.TraceWriter(repo)
        src, code = publish_dependencies(writer)
        row = trace.make_slot(key=KEY, at=SETUP, source_ref=src, code_ref=code)
        first = writer.publish_decision(row)
        assert first.created_at_ns > SETUP
        assert writer.publish_decision(row) == first
    with OpsRepository(path) as repo:
        writer = trace.TraceWriter(repo)
        assert writer.publish_decision(row) == first
        changed = row.stages.to_dict()
        changed["selection"]["reason"] = "changed"
        with pytest.raises(ValueError, match="conflicts"):
            writer.publish_decision(replace(row, stages=FrozenMap(changed)))
        assert repo.get_artifact(row.decision_id) == first


def test_unindexed_dependency_and_changed_artifact_refused(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        writer = trace.TraceWriter(repo)
        with pytest.raises(ValueError, match="dependency"):
            writer.publish_decision(trace.make_slot(key=KEY, at=SETUP, source_ref=REF, code_ref=CODE))
        with pytest.raises(ValueError, match="content identity"):
            writer.publish({REF: ("NativeTraceFixtureV1", {"changed": True})})


def test_atomic_write_failure_rolls_back_whole_batch(tmp_path, monkeypatch):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        writer = trace.TraceWriter(repo)
        # SQLite-backed failure, not a fake successful persistence fixture.
        repo._connection.execute(
            "CREATE TRIGGER trace_write_failure BEFORE INSERT ON artifact_index WHEN NEW.artifact_type='NativeTraceFailV1' BEGIN SELECT RAISE(ABORT,'injected disk write failure'); END"
        )
        good, bad = {"x": 1}, {"x": 2}
        with pytest.raises(Exception, match="injected disk write failure"):
            writer.publish(
                {sha256_json(good): ("NativeTraceGoodV1", good), sha256_json(bad): ("NativeTraceFailV1", bad)}
            )
        assert repo.get_artifact(sha256_json(good)) is None
        assert repo.get_artifact(sha256_json(bad)) is None


def test_record_and_batch_byte_bounds(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        writer = trace.TraceWriter(repo)
        body = {"data": "x" * (trace.MAX_RECORD_BYTES + 1)}
        with pytest.raises(ValueError, match="byte bound"):
            writer.publish({sha256_json(body): ("Oversize", body)})
        with pytest.raises(ValueError, match="batch"):
            writer.publish({str(i): ("x", {}) for i in range(65)})


def test_adapter_has_no_outcome_loader_or_substitute_numerical_strategy():
    module = pathlib.Path(trace.__file__).read_text()
    tree = ast.parse(module)
    assert not any(
        isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id in {"open", "eval", "exec", "compile"}
        for n in ast.walk(tree)
    )
    assert not any(
        isinstance(n, (ast.Import, ast.ImportFrom))
        and any(
            x in ast.unparse(n)
            for x in ("evaluate_outcomes", "economics", "requests", "subprocess", "socket", "lightgbm", "torch")
        )
        for n in ast.walk(tree)
    )
    assert "s1._setup(join)" in module and "s6._four_hour_context(" in module
    assert "pipeline.feature_snapshot(" in module
    assert "pstdev" not in module and "covariance" not in module


def test_cli_loads_in_fresh_process():
    import subprocess

    result = subprocess.run([sys.executable, str(DOC / "replay.py"), "--help"], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_genuine_cached_loader_binding_and_actual_local_read():
    spec = importlib.util.spec_from_file_location("native_trace_replay_test", DOC / "replay.py")
    replay = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(replay)
    old = pathlib.Path("/workspace/work/atlas-e1b-trigger-censor-qualified-scratch")
    new = pathlib.Path("/workspace/work/blind-ml-new-source-cache")
    if not old.exists() or not new.exists():
        pytest.skip("genuine retained source cache absent")
    rows = replay.sources.load_native("BTCUSDT", "1h", 2025, (6,), old, new)
    assert rows
    actual = time.time_ns()
    key = replay.sources.accepted.key_for("BTCUSDT")
    native = replay.adapt(rows[0], key, actual)
    assert native.raw.received_at_ns == actual > native.close_at_ns
    assert native.raw.replay_available_at_ns == native.close_at_ns + 5_000_000_000
    assert native.raw.quality_flags == ("LOCAL_ARCHIVE_READ_NOT_ORIGINAL_EXCHANGE_RECEIPT",)
    assert native.close == rows[0].close
    assert replay.adapt(rows[0], key, time.time_ns()).content_hash == native.content_hash


def test_frozen_deadline_and_actual_replay_separation(tmp_path):
    actual_join = asof_join(store_for_side("LONG"), KEY, cutoff_ns=SETUP)
    feature = pipeline.feature_snapshot(actual_join)
    late = replace(
        feature,
        envelope=replace(
            feature.envelope,
            content_hash="",
            created_at_ns=SETUP + 6_000_000_000,
            available_at_ns=SETUP + 6_000_000_000,
        ),
    )
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        blocked = trace.observe_coordinators(
            repo, actual_join, late, universe=universe_at(SETUP), event_gate=clear(SETUP)
        )
        assert all(r["reason"] == "ORIGINAL_T_PLUS_5_DEADLINE_MISSED" for r in blocked.values())
        assert all(not r["native_invoked"] for r in blocked.values())
        replay = reconstructed()
        observed = trace.observe_coordinators(
            repo,
            replay,
            pipeline.feature_snapshot(replay, replay_view=ReplayViewV2.RECONSTRUCTED_MARKET),
            universe=universe_at(SETUP),
            event_gate=clear(SETUP),
        )
        assert all(
            r["reason"] == "NATIVE_COORDINATOR_ACTUAL_SYSTEM_CONTRACT_NOT_REPLAY_QUALIFIED" for r in observed.values()
        )
        assert not repo.artifact_entries("CandidateActionV2")


def load_auditor():
    spec = importlib.util.spec_from_file_location("native_trace_audit_test", DOC / "audit.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def evidence_or_skip():
    # The causal continuation has a separately sealed protocol/code identity.
    # Keep foundation bytes intact; current-code parity uses the active trace.
    current = ROOT / "docs/research/atlas-red-causal-replay/evidence"
    path = current if (current / "TRACE_SEAL.json").exists() else DOC / "evidence"
    if not (path / "TRACE_SEAL.json").exists():
        pytest.skip("sealed integration evidence not present")
    seal = json.loads((path / "TRACE_SEAL.json").read_text())
    load_auditor().restore_trace(path, seal)
    return path


def test_whole_sealed_native_calendar_and_dependencies():
    audit = load_auditor()
    directory = evidence_or_skip()
    before = audit.digest(directory / "TRACE.jsonl.gz")
    result = audit.audit(directory)
    assert result["original_minute_observations"] == 2100
    assert result["calendar_key_mismatches"] == 0
    assert sum(result["native_s1_precursor_observations"].values()) == 132
    seal = json.loads((directory / "TRACE_SEAL.json").read_text())
    assert len(result["native_full_feature_observations"]) == seal["full_native_feature_calls"]
    assert result["timing_misses"] == 132
    assert result["native_feature_dependencies_resolved"]
    assert result["qualified_actions"] == 0 and result["orders"] == 0
    assert result["price_outcomes"] == "NOT_ESTIMABLE_NO_ELIGIBLE_ACTION"
    assert before == audit.digest(directory / "TRACE.jsonl.gz")


def test_lossless_transport_assembly_and_bad_existing_file_refused(tmp_path):
    import shutil

    audit = load_auditor()
    directory = evidence_or_skip()
    seal = json.loads((directory / "TRACE_SEAL.json").read_text())
    for part in seal["transport_parts"]:
        shutil.copyfile(directory / part["path"], tmp_path / part["path"])
    restored = audit.restore_trace(tmp_path, seal)
    assert audit.digest(restored) == seal["trace_sha256"]
    restored.write_bytes(b"changed")
    with pytest.raises(ValueError, match="SEALED_TRACE_BYTES_CHANGED"):
        audit.restore_trace(tmp_path, seal)


def test_bad_sealed_code_and_missing_transport_part_fail_before_audit(tmp_path):
    audit = load_auditor()
    directory = evidence_or_skip()
    seal = json.loads((directory / "TRACE_SEAL.json").read_text())
    key = next(iter(seal["code_hashes"]))
    seal["code_hashes"][key] = "0" * 64
    (tmp_path / "TRACE_SEAL.json").write_text(json.dumps(seal))
    with pytest.raises(ValueError, match="SEALED_NATIVE_CODE_CHANGED"):
        audit.audit(tmp_path)
    with pytest.raises(FileNotFoundError):
        audit.restore_trace(tmp_path, seal)
    assert not (tmp_path / "TRACE.assemble.tmp").exists()


def test_real_checksum_backed_native_prefix_matches_saved_results():
    spec = importlib.util.spec_from_file_location("native_trace_replay_real_test", DOC / "replay.py")
    replay = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(replay)
    old = pathlib.Path("/workspace/work/atlas-e1b-trigger-censor-qualified-scratch")
    new = pathlib.Path("/workspace/work/blind-ml-new-source-cache")
    if not old.exists() or not new.exists():
        pytest.skip("genuine retained source cache absent")
    audit = load_auditor()
    directory = evidence_or_skip()
    at = 1751328000000000000
    symbol = "BNBUSDT"
    key = replay.sources.accepted.key_for(symbol)
    h1 = [b for b in replay.sources.load_native(symbol, "1h", 2025, range(1, 8), old, new) if b.end <= at]
    m15 = [b for b in replay.sources.load_native(symbol, "15m", 2025, (6, 7), old, new) if b.end <= at]
    h4 = replay.sources.accepted.h4_bars_from_h1(symbol, h1)
    actual = time.time_ns()
    from atlas.v2.features.joins import JoinedBars

    join = JoinedBars(
        key,
        at + 6_000_000_000,
        tuple(replay.adapt(b, key, actual) for b in h4),
        tuple(replay.adapt(b, key, actual) for b in h1),
        tuple(replay.adapt(b, key, actual) for b in m15),
        "NOT_ESTIMABLE",
        "SOURCE_HEALTH_UNKNOWN",
        REF,
    )
    expected = trace.observe_components(join)
    for row in audit.rows(directory / "TRACE.jsonl.gz"):
        if row["artifact_type"] == "NativeTraceInstrumentSourceV1" and row["body"]["key"]["native_symbol"] == symbol:
            src = row["artifact_ref"]
            break
    prefixes = {
        r["artifact_ref"]
        for r in audit.rows(directory / "TRACE.jsonl.gz")
        if r["artifact_type"] == "NativeTracePrefixV1" and r["body"]["slot_ns"] == at and r["body"]["source_ref"] == src
    }
    saved = next(
        r["body"]["native"]
        for r in audit.rows(directory / "TRACE.jsonl.gz")
        if r["artifact_type"] == "NativeTraceComponentsV1" and r["body"]["source_prefix_ref"] in prefixes
    )
    assert {k: v for k, v in saved.items() if k != "qualified_coordinators"} == expected
    assert trace.observe_coordinators(None, join, None, universe=None)["S2"]["native_invoked"] is False


def test_missing_leading_history_cannot_reseed_declared_native_prefix():
    spec = importlib.util.spec_from_file_location("native_trace_replay_origins_test", DOC / "replay.py")
    replay = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(replay)
    join = reconstructed()
    origins = {
        BarIntervalV2.H1: join.h1[0].open_at_ns,
        BarIntervalV2.H4: join.h4[0].open_at_ns,
        BarIntervalV2.M15: join.m15[0].open_at_ns,
    }
    replay.require_declared_origins(join, origins)
    for field in ("h1", "h4", "m15"):
        shorter = replace(join, **{field: getattr(join, field)[1:]})
        with pytest.raises(ValueError, match="declared prefix origin missing"):
            replay.require_declared_origins(shorter, origins)
