"""Engineering fixtures with explicit causal features and accepted policy horizons."""

from dataclasses import replace
from decimal import Decimal

from atlas.v2._serialization import FrozenMap, sha256_json
from atlas.v2.contracts import ArtifactEnvelope, FeatureArtifactV2, FeatureValueV2, ReplayViewV2
from atlas.v2.instruments import StrategyEligibilityV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2
from atlas.v2.science.research_selection import assemble_multisleeve_research_candidate_set, research_selection_universe
from atlas.v2.strategies.s1_trend import S1_POLICY
from atlas.v2.strategies.s2_breakout import S2_POLICY
from atlas.v2.strategies.s3_mean_reversion import S3_POLICY

from .test_session014_core import KEY
from .test_session016_candidate_selection import CUTOFF, EVENT, candidate, evidence, index
from .test_session017_risk import risk_case


def feature_candidate(repo, policy=S1_POLICY, key=KEY, **kwargs):
    item = candidate(policy, key, **kwargs)
    duration = policy.time_exit_rule.get("after_ns", policy.time_exit_rule.get("max_hold_ns"))
    item = replace(item, envelope=replace(item.envelope, content_hash=""),
        horizon_end_ns=item.decision_at_ns + duration)
    names = ("h4.ema20", "h4.ema50", "h4.adx14", "h1.roc10", "h1.realized_variance20",
        "h1.ewma_variance", "m15.rsi14", "m15.atr14", "candle.signed_body_atr",
        "candle.close_position", "regime.trend_state", "regime.volatility_state")
    feature = FeatureArtifactV2(ArtifactEnvelope(1, f"session023-{item.candidate_id}",
        item.decision_at_ns, item.decision_at_ns, "SESSION023_SYNTHETIC_FIXTURE", ()), key,
        "SESSION023_ENGINEERING_FEATURES_V1", item.decision_at_ns, item.decision_at_ns,
        {name: FeatureValueV2(Decimal("1"), "FIXTURE") for name in names},
        sha256_json("session023-health"), ReplayViewV2.ACTUAL_SYSTEM)
    repo.register_artifact(ArtifactIndexEntryV2(feature.content_hash, "FeatureArtifactV2",
        feature.content_hash, item.decision_at_ns, item.decision_at_ns, {"feature": feature.to_dict()}))
    item = replace(item, envelope=replace(item.envelope, content_hash="", input_refs=(feature.content_hash,)),
        snapshot_hash=feature.content_hash)
    index(repo, item)
    return item


def research_case(repo):
    s2 = feature_candidate(repo, S2_POLICY)
    case = risk_case(repo, candidate_factory=lambda _repo, _universe, _product: feature_candidate(repo),
        additional_candidates=(s2,))
    case.baseline_candidate_set = case.candidate_set
    case.baseline_universe = case.universe
    expanded_entry = replace(case.universe.entries[0], strategy_eligibility=FrozenMap({
        **dict(case.universe.entries[0].strategy_eligibility),
        S3_POLICY.policy_id: StrategyEligibilityV2("ELIGIBLE")}))
    expanded = replace(case.universe, entries=(expanded_entry,), envelope=replace(case.universe.envelope,
        content_hash="", artifact_id="session023-expanded-causal-universe"))
    repo.register_artifact(ArtifactIndexEntryV2(expanded.content_hash, "UniverseContractV2",
        expanded.content_hash, CUTOFF, CUTOFF, {"universe": expanded.to_dict()}))
    case.universe = research_selection_universe(expanded)
    s3 = feature_candidate(repo, S3_POLICY)
    case.competitors = (case.candidate, s2, s3)
    ranks = {item.candidate_id: (evidence(repo, item, case.universe, rank),)
        for rank, item in enumerate(case.competitors, start=1)}
    case.candidate_set = assemble_multisleeve_research_candidate_set(repo,
        universe=case.universe, decision_event_id=EVENT, cutoff_ns=CUTOFF,
        candidates=case.competitors, policies={p.policy_hash: p for p in (S1_POLICY, S2_POLICY, S3_POLICY)},
        scanner_evidence_refs=ranks)
    return case
