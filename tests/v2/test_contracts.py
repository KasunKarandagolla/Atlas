from dataclasses import FrozenInstanceError
from decimal import Decimal
from hashlib import sha256
from typing import Any

import pytest

from atlas.v2._serialization import FrozenMap, canonical_json
from atlas.v2.contracts import (
    ArtifactEnvelope,
    CandidateActionV2,
    CandidateSelectionStatus,
    CandidateSetEntryV2,
    CandidateSetV2,
    DecisionStatusV2,
    EvaluationArtifactV2,
    FeatureArtifactV2,
    FeatureValueV2,
    ForecastStatusV2,
    OpportunityWatchV2,
    PolicySpecV2,
    ReplayViewV2,
    StrategyForecastV2,
    TradePlanEnvelopeV2,
    V2Side,
    WatchStateV2,
)
from atlas.v2.instruments import (
    EligibilityStatusV2,
    EnvironmentV2,
    InstrumentKeyV2,
    ProductTypeV2,
    VenueV2,
)

H = "a" * 64
H2 = "b" * 64


def key(venue: VenueV2 = VenueV2.BYBIT, revision: str = "r1") -> InstrumentKeyV2:
    return InstrumentKeyV2(venue, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL, "BTCUSDT", "BTC", "USDT", "USDT", revision)


def envelope(*, created: int = 100, available: int = 100, refs: tuple[str, ...] = ()) -> ArtifactEnvelope:
    return ArtifactEnvelope(1, "artifact-1", created, available, "tests/1", refs)


def test_envelope_strict_version_determinism_and_digest() -> None:
    left = ArtifactEnvelope(1, "a", 10, 11, "producer", ("a", "b"), H)
    right = ArtifactEnvelope(1, "a", 10, 11, "producer", ("a", "b"), H)
    assert left.to_canonical_json() == right.to_canonical_json()
    assert sha256(left.to_canonical_json().encode()).hexdigest() == sha256(right.to_canonical_json().encode()).hexdigest()
    with pytest.raises(ValueError, match="unknown fields"):
        ArtifactEnvelope.from_dict({**left.to_dict(), "extra": 1})
    with pytest.raises(ValueError, match="unsupported"):
        ArtifactEnvelope(2, "a", 10, 10, "producer", ())
    with pytest.raises(ValueError, match="sorted and unique"):
        ArtifactEnvelope(1, "a", 10, 10, "producer", ("b", "a"))
    with pytest.raises(ValueError, match="available_at_ns"):
        ArtifactEnvelope(1, "a", 12, 11, "producer", ())
    with pytest.raises(ValueError, match="sorted and unique"):
        ArtifactEnvelope.from_dict({**left.to_dict(), "input_refs": ["b", "a"]})


def test_feature_artifact_decimal_hash_roundtrip_and_immutability() -> None:
    feature = FeatureArtifactV2(
        envelope(), key(), "features/1", 90, 95,
        FrozenMap({"atr": FeatureValueV2(Decimal("1.2300"), "USDT"), "missing": FeatureValueV2(None, "ratio", "NO_SOURCE")}),
        H, ReplayViewV2.ACTUAL_SYSTEM,
    )
    wire = feature.to_dict()
    assert wire["values"]["atr"]["value"] == "1.23"
    assert FeatureArtifactV2.from_dict(wire).to_canonical_json() == feature.to_canonical_json()
    assert feature.content_hash == feature.envelope.content_hash
    with pytest.raises(FrozenInstanceError):
        feature.feature_set_version = "changed"  # type: ignore[misc]
    with pytest.raises(ValueError, match="finite"):
        FeatureValueV2(float("nan"), "ratio")
    with pytest.raises(ValueError, match="missing_reason"):
        FeatureValueV2(None, "ratio")


def test_policy_hash_uses_canonical_payload_and_strict_wire() -> None:
    kwargs = {
        "policy_id": "policy", "version": "1", "strategy_family": "trend", "capital_status": "RESEARCH_ONLY",
        "decision_event": "BAR_CLOSE", "required_features": ("close",), "optional_features": ("volume",),
        "timeframe_rules": {"1h": {"close_only": True}}, "setup_parameters": {}, "direction_rule": {"rule": "fixed"},
        "entry_rule": {}, "collar_rule": {}, "stop_rule": {}, "trigger_basis": "MARK_PRICE", "management_rule": {},
        "time_exit_rule": {}, "max_hold_ns": 100, "expiry_rule": {}, "model_requirements": (),
    }
    one = PolicySpecV2.build(**kwargs)
    two = PolicySpecV2.build(**kwargs)
    assert one.policy_hash == two.policy_hash
    assert PolicySpecV2.from_dict(one.to_dict()).policy_hash == one.policy_hash
    with pytest.raises(ValueError, match="unknown fields"):
        PolicySpecV2.from_dict({**one.to_dict(), "future": True})


def test_candidate_set_persists_rejected_entries_and_selection_rules() -> None:
    rejected = CandidateSetEntryV2("c-1", "p", key(), V2Side.LONG, (), EligibilityStatusV2.INELIGIBLE, rejection_reason="FILTER")
    accepted = CandidateSetEntryV2("c-2", "p", key(revision="r2"), V2Side.SHORT, (), EligibilityStatusV2.ELIGIBLE, 1, "rank")
    selected = CandidateSetV2(envelope(refs=(H,)), "event", H, H2, (rejected, accepted), "c-2", "rank_then_id", CandidateSelectionStatus.SELECTED)
    assert [entry.candidate_id for entry in CandidateSetV2.from_dict(selected.to_dict()).candidates] == ["c-1", "c-2"]
    no_candidate = CandidateSetV2(envelope(refs=(H,)), "event-2", H, H2, (rejected,), None, "rank_then_id", CandidateSelectionStatus.NO_CANDIDATE)
    assert CandidateSetV2.from_dict(no_candidate.to_dict()).candidates == (rejected,)
    with pytest.raises(ValueError, match="must exist"):
        CandidateSetV2(envelope(refs=(H,)), "event", H, H2, (rejected,), "missing", "rank_then_id", CandidateSelectionStatus.SELECTED)
    with pytest.raises(ValueError, match="cannot contain"):
        CandidateSetV2(envelope(refs=(H,)), "event", H, H2, (), "c-2", "rank_then_id", CandidateSelectionStatus.NOT_ESTIMABLE)


def test_candidate_quantity_is_optional_and_evaluation_requires_reason() -> None:
    candidate = CandidateActionV2(
        envelope(created=100, available=101), "c", key(), H, H2, V2Side.LONG,
        90, 110, 200, Decimal("100"), Decimal("101"), Decimal("95"), 0, "cost-model",
    )
    assert candidate.quantity is None
    assert CandidateActionV2.from_dict(candidate.to_dict()).quantity is None
    fields: dict[str, Any] = {
        "envelope": envelope(), "action_hash": H, "quantity": Decimal("1"), "risk_policy_hash": H,
        "account_snapshot_ref": H, "universe_ref": H, "candidate_set_ref": H, "selection_policy_hash": H,
        "meta_version": "meta/1", "scenario_manifest_ref": H, "common_path_ids_ref": H,
        "existing_portfolio_ref": H, "stress_ref": H, "outcome_distribution_ref": H,
        "expires_at_ns": 200,
    }
    with pytest.raises(ValueError, match="requires a reason"):
        EvaluationArtifactV2(**fields, decision=DecisionStatusV2.NO_TRADE, reasons=())
    result = EvaluationArtifactV2(**fields, decision=DecisionStatusV2.NOT_ESTIMABLE, reasons=("INSUFFICIENT_DATA",))
    assert EvaluationArtifactV2.from_dict(result.to_dict()).content_hash == result.content_hash


def test_strategy_forecast_is_evidence_and_trade_plan_version_fails_closed() -> None:
    forecast = StrategyForecastV2(
        envelope(), "forecast", "candidate", key(), "strategy", "1", H, 1000, 200, "net_return",
        ForecastStatusV2.AVAILABLE, H2, mean=Decimal("0.01"), q05=Decimal("-0.02"), q50=Decimal("0.01"),
        q95=Decimal("0.03"), p_net_positive=Decimal("0.6"), contradictions=("HIGH_SPREAD",),
    )
    assert StrategyForecastV2.from_dict(forecast.to_dict()).content_hash == forecast.content_hash
    plan = TradePlanEnvelopeV2(
        envelope(), "plan", "2.0", "2.0", key(), H, "account", H, H, H, H, H, 1, V2Side.LONG,
        Decimal("1"), "IOC_LIMIT", Decimal("101"), Decimal("95"), "MARK_PRICE", "NATIVE_PROTECTION",
        300, Decimal("2"), Decimal("3"), Decimal("10"), Decimal("2"), Decimal("100"), 200,
    )
    assert TradePlanEnvelopeV2.from_dict(plan.to_dict()).content_hash == plan.content_hash
    with pytest.raises(ValueError, match="unsupported TradePlanEnvelopeV2 plan_version"):
        TradePlanEnvelopeV2(
            envelope(), "plan", "3.0", "2.0", key(), H, "account", H, H, H, H, H, 1, V2Side.LONG,
            Decimal("1"), "IOC_LIMIT", Decimal("101"), Decimal("95"), "MARK_PRICE", "NATIVE_PROTECTION",
            300, Decimal("2"), Decimal("3"), Decimal("10"), Decimal("2"), Decimal("100"), 200,
        )


def test_watch_contract_has_no_order_semantics_and_lifecycle_is_strict() -> None:
    watch = OpportunityWatchV2("watch", key(), "strat", "1", H, WatchStateV2.DETECTED, 0, 10, 10, H2, (), "BAR", 100, 10)
    waiting = watch.transition_to(WatchStateV2.WAITING_FOR_EVENT, event_id="e1", event_at_ns=11, transition_at_ns=11, required_next_event="BAR_CLOSE")
    with pytest.raises(ValueError, match="illegal"):
        watch.transition_to(WatchStateV2.HANDED_OFF, event_id="e2", event_at_ns=11, transition_at_ns=11, handoff_receipt="pipeline")
    ready = waiting.transition_to(WatchStateV2.READY_FOR_RECHECK, event_id="e2", event_at_ns=12, transition_at_ns=12)
    confirmed = ready.transition_to(WatchStateV2.CONFIRMED, event_id="e3", event_at_ns=13, transition_at_ns=13)
    handed = confirmed.transition_to(WatchStateV2.HANDED_OFF, event_id="e4", event_at_ns=14, transition_at_ns=14, handoff_receipt="accepted-by-pipeline")
    assert handed.handoff_receipt == "accepted-by-pipeline"
    assert "order" not in canonical_json(handed.to_dict()).lower()
    assert OpportunityWatchV2.from_dict(handed.to_dict()).content_hash == handed.content_hash
