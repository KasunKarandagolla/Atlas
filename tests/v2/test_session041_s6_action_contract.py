"""S6 exact-action shadow candidate contract and causal fail-closed gates."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import FrozenMap, sha256_json
from atlas.v2.chronology import causal_artifact, record_computation
from atlas.v2.contracts import (
    ArtifactEnvelope,
    CandidateSelectionStatus,
    FeatureArtifactV2,
    FeatureValueV2,
    ReplayViewV2,
    V2Side,
)
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.health import PublicSourceHealthV2, PublicSourceStateV2
from atlas.v2.data.raw import RawObservationV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.science.research_selection import (
    EXACT_ACTION_POLICIES,
    assemble_multisleeve_research_candidate_set,
    research_selection_universe,
)
from atlas.v2.selection import (
    ScannerRankEvidenceV1,
    ScannerSelectionSourceV1,
    register_scanner_rank,
    register_scanner_source,
)
from atlas.v2.strategies.s1_trend import EventGate, EventState, ExecutableQuote, MarkIndexEvidence
from atlas.v2.strategies.s6_cross_section import (
    S6_ACTION_POLICY,
    S6_POLICY,
    S6ActionBuildResultV1,
    S6ShadowCoordinator,
    _expire_due_s6_watches,
    build_s6_candidate_action,
)

from .test_session021_s6 import CUTOFF, HOUR, M15, _keys, _market, _seed_evidence, _universe

NS = 1_000_000_000


@pytest.fixture(scope="module")
def s6_cases(tmp_path_factory):
    root = tmp_path_factory.mktemp("session041-s6-action")
    (root / "long").mkdir()
    (root / "short").mkdir()
    return {
        V2Side.LONG: _case(root / "long", V2Side.LONG),
        V2Side.SHORT: _case(root / "short", V2Side.SHORT),
    }


def _timed_bar(key, interval, close_at, *, open_, high, low, close, available_at=None):
    available = close_at if available_at is None else available_at
    raw = RawObservationV2.build(
        instrument_revision=key.contract_revision,
        source_id="SESSION041_S6_ACTION_FIXTURE",
        event_type=f"BAR_{interval.value}",
        event_at_ns=close_at,
        received_at_ns=available,
        ingested_at_ns=available,
        available_at_ns=available,
        payload={"open": str(open_), "high": str(high), "low": str(low), "close": str(close)},
        translation_version="SESSION041_S6_ACTION_FIXTURE_V1",
        sequence=f"{key.native_symbol}:{interval.value}:{close_at}",
    )
    return CausalBarV2(raw, interval, close_at - interval.duration_ns, close_at,
        Decimal(str(open_)), Decimal(str(high)), Decimal(str(low)), Decimal(str(close)), Decimal("1"), True)


def _case(tmp_path, side: V2Side, *, atr: Decimal = Decimal("1"), cutoff_ns: int = CUTOFF,
          decision_delay_ns: int = 0, derived_delay_ns: int = 0):
    keys = _keys(20)
    hourly, four_hour, evidence = _market(keys)
    universe = _universe(keys)
    universe = replace(universe,
        envelope=replace(universe.envelope, content_hash="", created_at_ns=cutoff_ns,
            available_at_ns=cutoff_ns), decision_slot_ns=cutoff_ns,
        entries=tuple(replace(entry, scanner_eligible=True) for entry in universe.entries))
    if cutoff_ns != CUTOFF:
        health = replace(next(iter(evidence.values())).source_health,
            observed_at_ns=cutoff_ns, available_at_ns=cutoff_ns,
            transition_id=sha256_json({"health": cutoff_ns}))
        evidence = {key: replace(item, observed_at_ns=cutoff_ns, available_at_ns=cutoff_ns,
            liquidity_ref=sha256_json({"liquidity": key.to_dict(), "cutoff": cutoff_ns}),
            funding_ref=sha256_json({"funding": key.to_dict(), "cutoff": cutoff_ns}),
            source_health=health) for key, item in evidence.items()}
    repository = OpsRepository(tmp_path / "ops.sqlite")
    _seed_evidence(repository, evidence)
    repository.register_artifact(ArtifactIndexEntryV2(
        universe.content_hash, "UniverseContractV2", universe.content_hash,
        universe.envelope.created_at_ns, universe.envelope.available_at_ns,
        {"universe": universe.to_dict()},
    ))
    coordinator = S6ShadowCoordinator(repository)
    result = coordinator.evaluate(universe=universe, cutoff_ns=cutoff_ns, btc_proxy=keys[0],
        hourly_bars=hourly, four_hour_bars=four_hour, evidence=evidence)
    hypothesis = next(item for item in result.hypotheses if item.side == side)
    key = hypothesis.key
    h1 = hourly[key][-3:]
    last_close = h1[-1].close
    prior_close = hypothesis.cutoff_ns - hypothesis.cutoff_ns % M15
    if side == V2Side.LONG:
        prior = _timed_bar(key, BarIntervalV2.M15, prior_close,
            open_=last_close, high=last_close + 1, low=last_close - 1, close=last_close)
        trigger = _timed_bar(key, BarIntervalV2.M15, prior_close + M15,
            open_=last_close + 1, high=last_close + 3, low=last_close, close=last_close + 2)
    else:
        prior = _timed_bar(key, BarIntervalV2.M15, prior_close,
            open_=last_close, high=last_close + 1, low=last_close - 1, close=last_close)
        trigger = _timed_bar(key, BarIntervalV2.M15, prior_close + M15,
            open_=last_close - 1, high=last_close, low=last_close - 3, close=last_close - 2)

    decision_cutoff = trigger.close_at_ns + decision_delay_ns
    feature_available = decision_cutoff + derived_delay_ns
    gate_available = feature_available + (NS if derived_delay_ns else 0)
    now = gate_available
    quote = ExecutableQuote(key,
        last_close + Decimal("0.9") if side == V2Side.LONG else last_close - Decimal("1"),
        last_close + Decimal("1") if side == V2Side.LONG else last_close - Decimal("0.9"),
        decision_cutoff, decision_cutoff, sha256_json({"quote": side.value}))
    health = PublicSourceHealthV2("SESSION041_S6_ACTION_SOURCE", decision_cutoff,
        decision_cutoff, PublicSourceStateV2.HEALTHY_CURRENT,
        sha256_json({"health": decision_cutoff}), "test current health")
    h1_refs = tuple(bar.content_hash for bar in h1)
    feature_refs = tuple(sorted({*h1_refs, prior.content_hash, trigger.content_hash, health.content_hash}))
    feature = FeatureArtifactV2(
        ArtifactEnvelope(1, sha256_json({"feature": key.to_dict(), "cutoff": decision_cutoff}),
            feature_available, feature_available, "SESSION041_S6_ACTION_FEATURE_V1", feature_refs),
        key, "INTRADAY_CORE_EXACT_PREFIX_EMA_ATR_V1", decision_cutoff, feature_available,
        FrozenMap({"m15.atr14": FeatureValueV2(atr, "price")}), health.content_hash,
        ReplayViewV2.ACTUAL_SYSTEM,
    )
    repository.register_artifact(ArtifactIndexEntryV2(feature.content_hash,
        "FeatureArtifactV2", feature.content_hash, feature_available, feature_available, {"feature": feature.to_dict()}))
    repository.register_artifact(ArtifactIndexEntryV2(health.content_hash,
        "PublicSourceHealthV2", health.content_hash, health.available_at_ns,
        health.available_at_ns, {"health": health.to_dict()}))
    for bar in (*h1, prior, trigger):
        repository.register_artifact(ArtifactIndexEntryV2(bar.content_hash, "CausalBarV2",
            bar.content_hash, bar.raw.available_at_ns, bar.raw.available_at_ns, {"bar": bar.to_dict()}))
    if derived_delay_ns:
        record_computation(repository, artifact_ref=feature.content_hash,
            information_cutoff_ns=decision_cutoff, started_ns=decision_cutoff,
            finished_ns=feature_available, available_ns=feature_available,
            input_refs=feature.envelope.input_refs,
            deadline_ns=decision_cutoff + 5 * NS)

    mark_ref = sha256_json({"mark_index": side.value})
    mark = MarkIndexEvidence(key, quote.ask if side == V2Side.LONG else quote.bid,
        quote.ask if side == V2Side.LONG else quote.bid, decision_cutoff, mark_ref)
    repository.register_artifact(ArtifactIndexEntryV2(mark_ref, "MarkIndexEvidenceV2",
        mark_ref, decision_cutoff, decision_cutoff, {"evidence_ref": mark_ref, "key": key.to_dict()}))
    repository.register_artifact(ArtifactIndexEntryV2(quote.evidence_ref, "ExecutableQuoteV2",
        quote.evidence_ref, quote.available_at_ns, quote.available_at_ns,
        {"evidence_ref": quote.evidence_ref, "key": key.to_dict()}))
    gate_ref = sha256_json({"gate": "clear", "cutoff": decision_cutoff})
    gate = EventGate(EventState.CLEAR, gate_available, gate_ref, "EVENT_GATE_TEST_V1")
    repository.register_artifact(ArtifactIndexEntryV2(gate_ref, "EventSafetyGateV2",
        gate_ref, gate_available, gate_available, {"gate": {
            "cutoff_ns": decision_cutoff, "state": "CLEAR", "blocked": False,
            "gate_version": gate.version,
        }}))
    if derived_delay_ns:
        record_computation(repository, artifact_ref=gate_ref,
            information_cutoff_ns=decision_cutoff, started_ns=feature_available,
            finished_ns=gate_available, available_ns=gate_available,
            input_refs=(feature.content_hash, trigger.content_hash),
            deadline_ns=decision_cutoff + 5 * NS)

    trigger_status = coordinator.confirm_trigger(hypothesis_id=hypothesis.hypothesis_id,
        previous_15m=prior, trigger_15m=trigger, cutoff_ns=trigger.close_at_ns)
    assert trigger_status == ("NOT_ESTIMABLE", "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT")
    return (repository, coordinator, hypothesis, h1, prior, trigger, feature, quote, mark, gate,
            now, decision_cutoff, universe, hourly, four_hour)


def _build(case, **overrides) -> S6ActionBuildResultV1:
    repository, _, hypothesis, h1, prior, trigger, feature, quote, mark, gate, now, decision_cutoff = case[:12]
    cost_ref = sha256_json({"cost_model": "S6_SHADOW_COST_V1"})
    if repository.get_artifact(cost_ref) is None:
        repository.register_artifact(ArtifactIndexEntryV2(
            cost_ref, "S6ShadowCostNotEstimableV1", cost_ref, 0, 0,
            {"cost": {"status": "NOT_ESTIMABLE", "authority": "ZERO"}},
        ))
    values = {
        "hypothesis": hypothesis, "hypothesis_ref": hypothesis.hypothesis_id,
        "stop_window_h1": h1, "previous_15m": prior, "trigger_15m": trigger,
        "feature": feature, "quote": quote, "mark_index": mark, "event_gate": gate,
        "cost_model_ref": cost_ref,
        "decision_cutoff_ns": decision_cutoff, "now_ns": now,
    }
    values.update(overrides)
    return build_s6_candidate_action(repository, **values)


def test_post_cutoff_candidate_has_exact_chronology_receipts_before_selection(tmp_path):
    case = _case(tmp_path, V2Side.LONG, derived_delay_ns=1)
    result = _build(case)
    candidate = result.candidate
    trigger_ref = result.trigger_ref
    assert candidate is not None and trigger_ref is not None
    repository = case[0]

    # Production has exact CausalBarV2 indexes for these refs. Add only those
    # same source bars to the offline fixture before proving the chronology.
    source_bars = {
        bar.content_hash: bar
        for histories in (*case[13].values(), *case[14].values())
        for bar in histories
    }
    for ref in candidate.envelope.input_refs:
        bar = source_bars.get(ref)
        if bar is not None and repository.get_artifact(ref) is None:
            repository.register_artifact(ArtifactIndexEntryV2(
                ref, "CausalBarV2", ref, bar.raw.available_at_ns,
                bar.raw.available_at_ns, {"bar": bar.to_dict()},
            ))

    trigger_entry = repository.get_artifact(trigger_ref)
    candidate_entry = repository.get_artifact(candidate.content_hash)
    assert trigger_entry is not None and candidate_entry is not None
    trigger_inputs = trigger_entry.metadata["input_refs"]
    record_computation(repository, artifact_ref=trigger_ref,
        information_cutoff_ns=candidate.decision_at_ns,
        started_ns=trigger_entry.available_at_ns, finished_ns=trigger_entry.available_at_ns,
        available_ns=trigger_entry.available_at_ns, input_refs=trigger_inputs,
        deadline_ns=candidate.deadline_ns)
    record_computation(repository, artifact_ref=candidate.content_hash,
        information_cutoff_ns=candidate.decision_at_ns,
        started_ns=candidate_entry.available_at_ns, finished_ns=candidate_entry.available_at_ns,
        available_ns=candidate_entry.available_at_ns, input_refs=candidate.envelope.input_refs,
        deadline_ns=candidate.deadline_ns)

    assert candidate_entry.available_at_ns > candidate.decision_at_ns
    assert causal_artifact(repository, trigger_ref, cutoff_ns=candidate.decision_at_ns,
        consumer_at_ns=candidate_entry.available_at_ns, deadline_ns=candidate.deadline_ns)
    assert causal_artifact(repository, candidate.content_hash, cutoff_ns=candidate.decision_at_ns,
        consumer_at_ns=candidate_entry.available_at_ns, deadline_ns=candidate.deadline_ns)


@pytest.mark.parametrize("side", [V2Side.LONG, V2Side.SHORT])
def test_s6_builds_exact_unsized_zero_authority_shadow_candidate(s6_cases, side):
    case = s6_cases[side]
    result = _build(case)
    candidate = result.candidate
    assert (result.status, result.reason) == ("CANDIDATE", "S6_SHADOW_TRIGGER")
    assert candidate is not None and candidate.quantity is None and candidate.account_scope is None
    assert result.trigger_ref is not None
    assert result.trigger_ref in candidate.envelope.input_refs
    assert candidate.policy_hash == S6_ACTION_POLICY.policy_hash
    assert candidate.decision_at_ns == case[11]
    assert candidate.deadline_ns == case[11] + 5 * NS
    assert candidate.horizon_end_ns == case[11] + 4 * 60 * 60 * NS
    if side == V2Side.LONG:
        assert candidate.entry_reference == case[7].ask
        assert candidate.entry_collar == case[7].ask * Decimal("1.0005")
        assert candidate.stop_price < candidate.entry_reference
    else:
        assert candidate.entry_reference == case[7].bid
        assert candidate.entry_collar == case[7].bid * Decimal("0.9995")
        assert candidate.stop_price > candidate.entry_reference
    indexed = case[0].get_artifact(candidate.content_hash)
    assert indexed is not None and indexed.metadata["selector_influence"] == "ZERO"
    assert indexed.metadata["capital_authority"] == "ZERO"
    assert indexed.metadata["quantity"] is None
    trigger_artifact = case[0].get_artifact(result.trigger_ref)
    assert trigger_artifact is not None
    assert trigger_artifact.artifact_type == "S6ActionTriggerEligibilityV1"
    assert trigger_artifact.metadata["trigger"]["action_policy_hash"] == S6_ACTION_POLICY.policy_hash
    if side == V2Side.LONG:
        repository = case[0]
        base_universe = case[12]
        selection_universe = replace(base_universe,
            envelope=replace(base_universe.envelope, content_hash="", artifact_id="session041-s6-selection-universe",
                producer_version="SESSION041_S6_SELECTION_FIXTURE_V1"),
            decision_slot_ns=candidate.decision_at_ns)
        universe = research_selection_universe(selection_universe)
        repository.register_artifact(ArtifactIndexEntryV2(universe.content_hash, "UniverseContractV2",
            universe.content_hash, universe.envelope.created_at_ns, universe.envelope.available_at_ns,
            {"universe": universe.to_dict()}))
        event_id = "session041-s6-selector-acceptance"
        scanner_input = {"candidate_id": candidate.candidate_id, "scanner_rank": 1,
            "available_at_ns": case[10], "source": "S6_ACTION_ACCEPTANCE_FIXTURE"}
        scanner_input_ref = sha256_json(scanner_input)
        repository.register_artifact(ArtifactIndexEntryV2(scanner_input_ref, "ScannerInputFixtureV1",
            scanner_input_ref, case[10], case[10], scanner_input))
        source = ScannerSelectionSourceV1(candidate.candidate_id, candidate.key, 1,
            "SCANNER_V1", "1.0", universe.content_hash, event_id, case[10], (scanner_input_ref,))
        source_ref = register_scanner_source(repository, source)
        rank = ScannerRankEvidenceV1(candidate.candidate_id, 1, "SCANNER_V1", "1.0",
            universe.content_hash, event_id, case[10], source_ref)
        rank_ref = register_scanner_rank(repository, rank)
        selected = assemble_multisleeve_research_candidate_set(repository,
            universe=universe, decision_event_id=event_id, cutoff_ns=candidate.decision_at_ns,
                candidates=(candidate,),
                policies={policy.policy_hash: policy for policy in EXACT_ACTION_POLICIES.values()},
            scanner_evidence_refs={candidate.candidate_id: (rank_ref,)}, clock_ns=lambda: case[10])
        assert selected.selection_status == CandidateSelectionStatus.SELECTED
        assert selected.selected_candidate_id == candidate.candidate_id
        assert selected.candidates[0].policy_id == S6_ACTION_POLICY.policy_id
        assert candidate.quantity is None and candidate.account_scope is None


def test_s6_action_policy_is_additive_to_unchanged_rank_policy(tmp_path):
    before = S6_POLICY.to_dict()
    rank_hash = S6_POLICY.policy_hash
    assert S6_ACTION_POLICY.version == "2.0.0-shadow-single-action"
    assert S6_ACTION_POLICY.policy_id == S6_POLICY.policy_id
    assert S6_POLICY.to_dict() == before
    assert S6_POLICY.policy_hash == rank_hash


def test_s6_stop_window_uses_latest_completed_hour_at_non_hour_cutoff(tmp_path):
    decision_cutoff = CUTOFF + M15
    case = _case(tmp_path, V2Side.LONG, cutoff_ns=decision_cutoff)
    result = _build(case)
    assert result.status == "CANDIDATE"
    assert case[3][-1].close_at_ns == decision_cutoff - decision_cutoff % HOUR


def test_s6_accepts_receipt_lag_at_hypothesis_and_trigger_cutoffs(tmp_path):
    case = _case(tmp_path, V2Side.LONG, cutoff_ns=CUTOFF + 2 * NS, decision_delay_ns=4 * NS)
    result = _build(case)
    assert result.status == "CANDIDATE"
    assert case[4].close_at_ns == CUTOFF
    assert case[2].cutoff_ns - case[4].close_at_ns == 2 * NS
    assert case[11] - case[5].close_at_ns == 4 * NS


def test_s6_accepts_post_cutoff_feature_and_gate_with_valid_chronology_receipts(tmp_path):
    case = _case(tmp_path, V2Side.LONG, derived_delay_ns=2 * NS)
    result = _build(case)
    assert result.status == "CANDIDATE"
    candidate = result.candidate
    assert candidate is not None
    assert candidate.decision_at_ns == case[11]
    assert candidate.envelope.available_at_ns == case[10]
    assert candidate.envelope.available_at_ns > candidate.decision_at_ns
    assert candidate.deadline_ns > candidate.envelope.available_at_ns


def test_s6_expiry_sweep_terminally_closes_watches_across_repeated_cycles(s6_cases):
    case = s6_cases[V2Side.SHORT]
    repository, _, hypothesis = case[:3]
    watch = repository.get_watch(hypothesis.watch_id)
    assert watch is not None and watch.state.value == "CONFIRMED"
    cutoff = watch.expires_at_ns + 1
    _expire_due_s6_watches(repository, cutoff_ns=cutoff)
    _expire_due_s6_watches(repository, cutoff_ns=cutoff + 1)
    expired = repository.get_watch(hypothesis.watch_id)
    assert expired is not None and expired.state.value == "EXPIRED"
    assert all(item.watch_id != hypothesis.watch_id
        for item in repository.list_active_watches(limit=4096))


@pytest.mark.parametrize("case_name,expected", [
    ("stale_quote", "S6_EXECUTABLE_BBO_STALE_OR_MISBOUND"),
    ("misbound_quote", "S6_EXECUTABLE_BBO_STALE_OR_MISBOUND"),
    ("future_feature", "S6_FEATURE_OR_TRIGGER_NOT_EXACT_CAUSAL"),
    ("misbound_hypothesis", "S6_HYPOTHESIS_MISSING_MISBOUND_OR_FUTURE"),
    ("stale_gate", "S6_EVENT_GATE_UNKNOWN_STALE_OR_FUTURE"),
    ("late_candidate", "S6_CANDIDATE_DEADLINE_EXPIRED"),
    ("infeasible_stop", "S6_STOP_DISTANCE_OUTSIDE_0.5_TO_3_ATR"),
])
def test_s6_missing_stale_misbound_or_infeasible_action_inputs_fail_closed(s6_cases, case_name, expected):
    case = s6_cases[V2Side.LONG]
    repository, _, hypothesis, _, _, _, feature, quote, _, gate, now, decision_cutoff = case[:12]
    args = {}
    if case_name == "stale_quote":
        args["quote"] = replace(quote, observed_at_ns=now - 6 * NS, available_at_ns=now)
    elif case_name == "misbound_quote":
        wrong_key = _keys(20)[2]
        args["quote"] = replace(quote, key=wrong_key)
    elif case_name == "future_feature":
        future = replace(feature, envelope=replace(feature.envelope, content_hash="",
            created_at_ns=now + 1, available_at_ns=now + 1))
        repository.register_artifact(ArtifactIndexEntryV2(future.content_hash,
            "FeatureArtifactV2", future.content_hash, now + 1, now + 1,
            {"feature": future.to_dict()}))
        args["feature"] = future
    elif case_name == "misbound_hypothesis":
        args["hypothesis_ref"] = sha256_json({"wrong_hypothesis": hypothesis.hypothesis_id})
    elif case_name == "stale_gate":
        stale_ref = sha256_json({"stale_gate": gate.evidence_ref})
        stale = EventGate(EventState.CLEAR, decision_cutoff - 60 * 60 * NS,
            stale_ref, gate.version)
        repository.register_artifact(ArtifactIndexEntryV2(stale_ref, "EventSafetyGateV2",
            stale_ref, stale.available_at_ns, stale.available_at_ns, {"gate": {
                "cutoff_ns": stale.available_at_ns, "state": "CLEAR", "blocked": False,
                "gate_version": stale.version,
            }}))
        args["event_gate"] = stale
    elif case_name == "late_candidate":
        args["now_ns"] = decision_cutoff + 6 * NS
    elif case_name == "infeasible_stop":
        infeasible = replace(feature, envelope=replace(feature.envelope, content_hash=""),
            values=FrozenMap({"m15.atr14": FeatureValueV2(Decimal("0.01"), "price")}))
        repository.register_artifact(ArtifactIndexEntryV2(infeasible.content_hash,
            "FeatureArtifactV2", infeasible.content_hash, infeasible.envelope.available_at_ns,
            infeasible.envelope.available_at_ns, {"feature": infeasible.to_dict()}))
        args["feature"] = infeasible
    result = _build(case, **args)
    assert result.candidate is None
    assert result.status in {"NOT_ESTIMABLE", "NO_CANDIDATE"}
    assert result.reason == expected
