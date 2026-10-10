"""Causal market-wide S6 relative-strength research policy and action shadow."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal
from statistics import mean, stdev

from atlas.v2._serialization import (
    FrozenMap,
    artifact_wire,
    canonical_json,
    seal_envelope,
    sha256_json,
    sha256_ref,
    timestamp,
)
from atlas.v2.chronology import causal_artifact, chronology_ref
from atlas.v2.contracts import (
    ArtifactEnvelope,
    CandidateActionV2,
    EligibilityStatusV2,
    FeatureArtifactV2,
    OpportunityWatchV2,
    PolicySpecV2,
    ReplayViewV2,
    V2Side,
    WatchStateV2,
)
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.health import PublicSourceHealthV2, PublicSourceStateV2
from atlas.v2.data.raw import AvailabilityClassV2
from atlas.v2.features.technical import ema
from atlas.v2.instruments import InstrumentKeyV2, UniverseContractV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.news.events import _source_evidence_available
from atlas.v2.strategies.s1_trend import EventGate, EventState, ExecutableQuote, MarkIndexEvidence

POLICY_ID = "S6_CROSS_SECTIONAL_RELATIVE_STRENGTH"
POLICY_VERSION = "1.0.0-shadow-research"
PRODUCER_VERSION = "S6_CROSS_SECTIONAL_V1"
HOUR_NS = BarIntervalV2.H1.duration_ns
FIFTEEN_MINUTE_NS = BarIntervalV2.M15.duration_ns
FOUR_HOURS_NS = BarIntervalV2.H4.duration_ns
DAY_NS = 86_400_000_000_000
TRAILING_RETURNS = 30 * 24
MIN_BREADTH = 20
MIN_DECILE_COUNT = 1
MAX_EVIDENCE_AGE_NS = 60_000_000_000
S6_ACTION_POLICY_VERSION = "2.0.0-shadow-single-action"
S6_ACTION_PRODUCER_VERSION = "S6_SINGLE_ACTION_SHADOW_V1"
S6_CANDIDATE_DEADLINE_NS = 5_000_000_000
S6_MAX_BBO_AGE_NS = 5_000_000_000
S6_MAX_MARK_INDEX_AGE_NS = 5_000_000_000
S6_STOP_BUFFER_ATR = Decimal("0.25")
S6_MIN_STOP_DISTANCE_ATR = Decimal("0.5")
S6_MAX_STOP_DISTANCE_ATR = Decimal("3")
S6_ENTRY_COLLAR_BPS = Decimal("5")


@dataclass(frozen=True)
class S6ResearchPolicyV2:
    """Research policy identity with no invented stop, holding horizon or action fields."""

    policy_id: str
    version: str
    capital_status: str
    score_convention: FrozenMap
    candidate_rule: FrozenMap
    missing_exact_action_contract: str
    policy_hash: str = ""

    def __post_init__(self) -> None:
        if self.policy_id != POLICY_ID or self.version != POLICY_VERSION or self.capital_status != "SHADOW_ONLY":
            raise ValueError("invalid S6 research policy identity")
        body = self._body()
        digest = sha256_json({"artifact_type": "S6ResearchPolicyV2", "policy": body})
        if self.policy_hash and self.policy_hash != digest:
            raise ValueError("S6 research policy hash mismatch")
        object.__setattr__(self, "policy_hash", digest)

    def _body(self) -> dict[str, object]:
        return {
            "policy_id": self.policy_id, "version": self.version, "capital_status": self.capital_status,
            "score_convention": self.score_convention.to_dict(),
            "candidate_rule": self.candidate_rule.to_dict(),
            "missing_exact_action_contract": self.missing_exact_action_contract,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self._body(), "schema_version": 2, "artifact_type": "S6ResearchPolicyV2",
                "policy_hash": self.policy_hash}


def policy_spec() -> S6ResearchPolicyV2:
    return S6ResearchPolicyV2(
        POLICY_ID, POLICY_VERSION, "SHADOW_ONLY",
        FrozenMap({
            "hourly_return": "log_close_ratio_of_completed_1H_bars",
            "alignment": "exact_timestamp_intersection_asset_and_BTC",
            "window": "trailing_30_days_completed_hourly_returns_720_samples",
            "instrument_join": "full_InstrumentKeyV2",
            "availability": "cutoff_known_actual_or_reconstructed_view_explicit",
            "minimum_strategy_eligible_breadth": MIN_BREADTH,
            "venue_scope": "BTC_proxy_venue_only_full_instrument_identity",
            "beta": "sample_covariance(asset,BTC)/sample_variance(BTC)",
            "residual_return": "asset_hourly_log_return-beta*BTC_hourly_log_return",
            "residual_volatility": "sample_standard_deviation_of_trailing_matched_residual_returns",
            "score": "sum(latest_4_aligned_hourly_residual_returns)/residual_volatility",
            "own_4h_trend": "latest_confirmed_close_and_ema20_vs_ema50_on_latest_50_contiguous_completed_4H_closes",
            "rank": "descending_score_then_canonical_full_instrument_identity_ascending",
            "decile_count": "ceil(0.10*rankable_breadth)",
            "engineering_definition": "transparent_research_convention_not_economic_optimization",
        }),
        FrozenMap({
            "top_decile": "LONG_IF_OWN_4H_TREND_ALIGNS",
            "bottom_decile": "SHORT_IF_OWN_4H_TREND_ALIGNS",
            "trigger": "subsequent_confirmed_15M_close_breaks_prior_15M_high_or_low",
            "candidate_action": "NOT_ESTIMABLE_UNTIL_S6_STOP_ACTION_CONTRACT_EXISTS",
            "selector_influence": "ZERO",
        }),
        "S6_STOP_AND_ACTION_HORIZON_CONTRACT_RESERVED_FOR_SESSION_023",
    )


S6_POLICY = policy_spec()


def action_policy_spec() -> PolicySpecV2:
    """Add an immutable shadow action contract without changing S6 rank identity."""
    return PolicySpecV2.build(
        policy_id=POLICY_ID,
        version=S6_ACTION_POLICY_VERSION,
        strategy_family="CROSS_SECTIONAL_RELATIVE_STRENGTH_SINGLE_ACTION",
        capital_status="SHADOW_ONLY",
        decision_event="CONFIRMED_15M_CLOSE",
        required_features=tuple(sorted((
            "S6_AVAILABLE_POINT_IN_TIME_RANK_STATE",
            "S6_TOP_BOTTOM_DECILE_AND_OWN_4H_TREND_ALIGNMENT",
            "three_prior_completed_1H_bars",
            "causal_m15_atr14",
            "subsequent_confirmed_15M_trigger_bar",
            "executable_bbo",
            "mark_index",
            "clear_deterministic_event_gate",
            "current_source_health",
        ))),
        optional_features=(),
        timeframe_rules=FrozenMap({
            "ranking": "existing_S6_720_completed_H1_return_state_and_own_50_H4_context",
            "stop_window": "three_latest_completed_H1_bars_strictly_before_trigger_M15",
            "trigger": "subsequent_confirmed_15M_close_breaks_previous_15M_high_or_low",
            "instrument_identity": "exact_full_InstrumentKeyV2_and_contract_revision",
            "availability": "explicit_actual_or_reconstructed_view_no_backdating",
        }),
        setup_parameters=FrozenMap({
            "source_rank_policy_hash": S6_POLICY.policy_hash,
            "stop_window_h1_bars": 3,
            "stop_window_excludes_trigger_interval": True,
            "atr_feature_id": "m15.atr14",
            "atr_algorithm": "TECHNICAL_V1_WILDER_SEEDED_FIRST_14_TR_MEAN",
        }),
        direction_rule=FrozenMap({
            "long": "top_decile_and_own_4H_UP_then_subsequent_15M_close_breaks_previous_high",
            "short": "bottom_decile_and_own_4H_DOWN_then_subsequent_15M_close_breaks_previous_low",
            "ranking_and_direction": "existing_S6_research_policy_unchanged",
        }),
        entry_rule=FrozenMap({
            "order_type": "LIMIT", "time_in_force": "IOC", "shadow_only": True,
            "long_reference": "fresh_executable_BBO_ask", "short_reference": "fresh_executable_BBO_bid",
            "no_same_epoch_reprice": True, "candidate_deadline_offset_ns": S6_CANDIDATE_DEADLINE_NS,
            "bbo_max_age_ns": S6_MAX_BBO_AGE_NS,
            "mark_index_max_age_ns": S6_MAX_MARK_INDEX_AGE_NS,
            "confirmed_bar_max_lag_ns": S6_MAX_BBO_AGE_NS,
        }),
        collar_rule=FrozenMap({
            "adverse_bps": int(S6_ENTRY_COLLAR_BPS),
            "long_reference": "ask", "short_reference": "bid",
            "formula": "long=ask*(1+5/10000);short=bid*(1-5/10000)",
        }),
        stop_rule=FrozenMap({
            "long": "minimum_low_of_three_prior_completed_H1_bars_minus_0.25_ATR14_M15",
            "short": "maximum_high_of_three_prior_completed_H1_bars_plus_0.25_ATR14_M15",
            "atr_feature_id": "m15.atr14",
            "minimum_distance_atr": str(S6_MIN_STOP_DISTANCE_ATR),
            "maximum_distance_atr": str(S6_MAX_STOP_DISTANCE_ATR),
            "rounding": "none_in_candidate;venue_tick_rounding_belongs_to_product_risk_contract",
        }),
        trigger_basis="CONFIRMED_CLOSE",
        management_rule=FrozenMap({
            "fixed_stop": True, "discretionary_trailing": False, "partial_tp": False,
            "pyramiding": False, "averaging": False, "same_epoch_repricing": False,
            "stop_widening": False,
        }),
        time_exit_rule=FrozenMap({"after_ns": FOUR_HOURS_NS, "type": "TIME_EXIT"}),
        max_hold_ns=FOUR_HOURS_NS,
        expiry_rule=FrozenMap({
            "watch": "existing_single_subsequent_15M_bar_expiry",
            "candidate_deadline_offset_ns": S6_CANDIDATE_DEADLINE_NS,
        }),
        model_requirements=(),
    )


S6_ACTION_POLICY = action_policy_spec()


@dataclass(frozen=True)
class S6ActionBuildResultV1:
    """Fail-closed result of trying to materialize a single-action shadow candidate."""

    status: str
    reason: str
    candidate: CandidateActionV2 | None = None
    trigger_ref: str | None = None

    def __post_init__(self) -> None:
        if self.status not in {"CANDIDATE", "NO_CANDIDATE", "NOT_ESTIMABLE"}:
            raise ValueError("invalid S6 action build status")
        if not self.reason:
            raise ValueError("S6 action build result requires a reason")
        if (self.status == "CANDIDATE") != (self.candidate is not None):
            raise ValueError("S6 candidate result/status mismatch")


def _same_btc_proxy_cohort(key: InstrumentKeyV2, btc_proxy: InstrumentKeyV2) -> bool:
    """Require comparable venue, environment and product for cross-sectional beta."""
    return (key.venue, key.environment, key.product) == (
        btc_proxy.venue, btc_proxy.environment, btc_proxy.product,
    )


@dataclass(frozen=True)
class LiquidityFundingEvidenceV2:
    key: InstrumentKeyV2
    observed_at_ns: int
    available_at_ns: int
    spread_bps: Decimal
    quote_turnover_24h: Decimal
    funding_rate: Decimal
    liquidity_ref: str
    funding_ref: str
    source_health: PublicSourceHealthV2
    turnover_ref: str | None = None

    def __post_init__(self) -> None:
        timestamp(self.observed_at_ns, field="S6_evidence.observed_at_ns")
        timestamp(self.available_at_ns, field="S6_evidence.available_at_ns")
        if not isinstance(self.key, InstrumentKeyV2) or not isinstance(self.source_health, PublicSourceHealthV2):
            raise ValueError("S6 liquidity/funding evidence requires full key and typed source health")
        if self.available_at_ns < self.observed_at_ns:
            raise ValueError("S6 evidence cannot be available before observation")
        for name in ("spread_bps", "quote_turnover_24h", "funding_rate"):
            value = Decimal(getattr(self, name))
            if not value.is_finite() or (name != "funding_rate" and value < 0):
                raise ValueError("invalid S6 liquidity/funding evidence")
            object.__setattr__(self, name, value)
        if any(len(ref) != 64 for ref in (self.liquidity_ref, self.funding_ref)):
            raise ValueError("S6 evidence requires content-addressed input refs")
        if self.turnover_ref is not None:
            sha256_ref(self.turnover_ref, field="S6_evidence.turnover_ref")

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        body: dict[str, object] = {"schema_version": 1, "key": self.key.to_dict(), "observed_at_ns": self.observed_at_ns,
                "available_at_ns": self.available_at_ns, "spread_bps": str(self.spread_bps),
                "quote_turnover_24h": str(self.quote_turnover_24h), "funding_rate": str(self.funding_rate),
                "liquidity_ref": self.liquidity_ref, "funding_ref": self.funding_ref,
                "source_health": self.source_health.to_dict()}
        if self.turnover_ref is not None:
            body["turnover_ref"] = self.turnover_ref
        return body

    @property
    def source_health_ref(self) -> str:
        return self.source_health.content_hash


@dataclass(frozen=True)
class S6RankRowV2:
    key: InstrumentKeyV2
    beta_btc: float | None
    residual_return_4h: float | None
    residual_volatility: float | None
    score: float | None
    rank: int | None
    decile: str | None
    own_4h_trend: str
    eligibility: str
    reason: str | None
    hourly_refs: tuple[str, ...]
    evidence_ref: str | None
    context_refs: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {"key": self.key.to_dict(), "beta_btc": self.beta_btc,
                "residual_return_4h": self.residual_return_4h,
                "residual_volatility": self.residual_volatility, "score": self.score,
                "rank": self.rank, "decile": self.decile, "own_4h_trend": self.own_4h_trend,
                "eligibility": self.eligibility, "reason": self.reason,
                "hourly_refs": list(self.hourly_refs), "evidence_ref": self.evidence_ref,
                "context_refs": list(self.context_refs)}


@dataclass(frozen=True)
class S6CrossSectionStateV2:
    envelope: ArtifactEnvelope
    policy_hash: str
    cutoff_ns: int
    universe_ref: str
    btc_proxy: InstrumentKeyV2
    status: str
    reason: str | None
    rows: tuple[S6RankRowV2, ...]
    eligible_breadth: int
    decile_size: int
    replay_view: str = "ACTUAL_SYSTEM"

    ARTIFACT_TYPE = "S6CrossSectionStateV2"

    def __post_init__(self) -> None:
        timestamp(self.cutoff_ns, field="S6.cutoff_ns")
        if self.status not in ("AVAILABLE", "NOT_ESTIMABLE"):
            raise ValueError("invalid S6 cross-section status")
        if self.eligible_breadth < 0 or self.decile_size < 0:
            raise ValueError("S6 breadth values must be nonnegative")
        if self.replay_view not in ("ACTUAL_SYSTEM", "RECONSTRUCTED_MARKET"):
            raise ValueError("S6 state requires an explicit availability view")
        ordered = tuple(sorted(self.rows, key=lambda row: row.key.to_canonical_json()))
        if ordered != self.rows:
            raise ValueError("S6 persisted rows must be sorted by full instrument identity")
        object.__setattr__(self, "envelope", seal_envelope(self.envelope, self._body(), artifact_type=self.ARTIFACT_TYPE))

    def _body(self) -> dict[str, object]:
        return {"policy_hash": self.policy_hash, "cutoff_ns": self.cutoff_ns, "universe_ref": self.universe_ref,
                "btc_proxy": self.btc_proxy.to_dict(), "status": self.status, "reason": self.reason,
                "rows": [row.to_dict() for row in self.rows], "eligible_breadth": self.eligible_breadth,
                "decile_size": self.decile_size, "replay_view": self.replay_view}

    @property
    def content_hash(self) -> str:
        return self.envelope.content_hash

    def to_dict(self) -> dict[str, object]:
        return artifact_wire(self.envelope, self._body(), artifact_type=self.ARTIFACT_TYPE)


@dataclass(frozen=True)
class S6HypothesisV2:
    hypothesis_id: str
    key: InstrumentKeyV2
    policy_hash: str
    state_ref: str
    side: V2Side
    cutoff_ns: int
    score: float
    beta_btc: float
    reason: str
    watch_id: str
    replay_view: str = "ACTUAL_SYSTEM"

    def to_dict(self) -> dict[str, object]:
        return {"schema_version": 1, "hypothesis_id": self.hypothesis_id, "key": self.key.to_dict(),
                "policy_hash": self.policy_hash, "state_ref": self.state_ref, "side": self.side.value,
                "cutoff_ns": self.cutoff_ns, "score": self.score, "beta_btc": self.beta_btc,
                "reason": self.reason, "watch_id": self.watch_id,
                "replay_view": self.replay_view,
                "exact_action_status": "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT",
                "missing_contract": "S6_stop_and_exact_action_semantics_RESERVED_FOR_SESSION_023"}


@dataclass(frozen=True)
class S6DecisionV2:
    status: str
    reason: str | None
    state: S6CrossSectionStateV2
    hypotheses: tuple[S6HypothesisV2, ...]


def _s6_action_bar_available(bar: CausalBarV2, *, key: InstrumentKeyV2,
                             view: ReplayViewV2, cutoff_ns: int) -> bool:
    if (not bar.final or bar.instrument_revision != key.contract_revision
            or bar.raw.instrument_revision != key.contract_revision
            or bar.raw.availability_class != AvailabilityClassV2(view.value)
            or bar.close_at_ns > cutoff_ns):
        return False
    available = bar.raw.available_at_ns if view == ReplayViewV2.ACTUAL_SYSTEM else bar.raw.replay_available_at_ns
    return available is not None and available <= cutoff_ns


def _indexed_exact(repository: OpsRepository, ref: str, *, artifact_type: str,
                   available_by_ns: int) -> ArtifactIndexEntryV2 | None:
    try:
        entry = repository.get_artifact(ref)
    except ValueError:
        return None
    if (entry is None or entry.artifact_type != artifact_type or entry.artifact_ref != ref
            or entry.content_hash != ref or entry.available_at_ns > available_by_ns):
        return None
    return entry


def _expire_due_s6_watches(repository: OpsRepository, *, cutoff_ns: int) -> None:
    """Terminally expire due S6 trigger watches during every S6 cadence."""
    for watch in repository.list_active_watches(limit=4096):
        if (watch.strategy_id != POLICY_ID or watch.policy_hash != S6_POLICY.policy_hash
                or cutoff_ns < watch.expires_at_ns):
            continue
        event_id = sha256_json({
            "watch_id": watch.watch_id,
            "event": "S6_SINGLE_SUBSEQUENT_M15_EXPIRED_V1",
            "expires_at_ns": watch.expires_at_ns,
        })
        repository.transition_watch(
            watch.watch_id,
            expected_state_version=watch.state_version,
            event_id=event_id,
            event_at_ns=watch.expires_at_ns,
            transition_at_ns=cutoff_ns,
            target_state=WatchStateV2.EXPIRED,
            outbox_id=sha256_json({"watch_id": watch.watch_id, "outbox": event_id}),
        )


def build_s6_candidate_action(
    repository: OpsRepository,
    *,
    hypothesis: S6HypothesisV2,
    hypothesis_ref: str,
    stop_window_h1: Sequence[CausalBarV2],
    previous_15m: CausalBarV2,
    trigger_15m: CausalBarV2,
    feature: FeatureArtifactV2,
    quote: ExecutableQuote,
    mark_index: MarkIndexEvidence,
    event_gate: EventGate,
    cost_model_ref: str,
    decision_cutoff_ns: int,
    now_ns: int,
) -> S6ActionBuildResultV1:
    """Build and persist one exact, unsized S6 shadow action or a named failure.

    ``S6ResearchPolicyV2`` and its previously emitted rank/hypothesis artifacts
    remain unchanged. This additive action profile consumes one previously
    recorded hypothesis and only creates a candidate after the exact S6 trigger,
    causal feature, quote, mark/index and event-gate evidence are verified.
    """
    timestamp(now_ns, field="S6_action.now_ns")
    timestamp(decision_cutoff_ns, field="S6_action.decision_cutoff_ns")

    def fail(status: str, reason: str) -> S6ActionBuildResultV1:
        return S6ActionBuildResultV1(status, reason)

    if repository.read_only:
        return fail("NOT_ESTIMABLE", "S6_ACTION_REPOSITORY_READ_ONLY")
    try:
        sha256_ref(hypothesis_ref, field="S6_action.hypothesis_ref")
        sha256_ref(cost_model_ref, field="S6_action.cost_model_ref")
    except ValueError:
        return fail("NOT_ESTIMABLE", "S6_ACTION_IDENTITY_INVALID")

    hypothesis_entry = _indexed_exact(repository, hypothesis_ref,
        artifact_type="S6HypothesisV2", available_by_ns=now_ns)
    if (hypothesis_entry is None
            or canonical_json(hypothesis_entry.metadata.get("hypothesis")) != canonical_json(hypothesis.to_dict())
            or hypothesis_ref != hypothesis.hypothesis_id):
        return fail("NOT_ESTIMABLE", "S6_HYPOTHESIS_MISSING_MISBOUND_OR_FUTURE")
    if hypothesis.policy_hash != S6_POLICY.policy_hash:
        return fail("NOT_ESTIMABLE", "S6_RANK_POLICY_IDENTITY_MISMATCH")

    # The fixed code policy is a declared candidate input. Persist its exact
    # immutable specification at the same time-independent identity used by
    # other installed PolicySpecV2 contracts so a post-cutoff candidate can
    # receive a complete causal publication receipt.
    _index(repository, S6_ACTION_POLICY.policy_hash, "PolicySpecV2", 0,
        {"policy": S6_ACTION_POLICY.to_dict()})

    state_entry = _indexed_exact(repository, hypothesis.state_ref,
        artifact_type="S6CrossSectionStateV2", available_by_ns=now_ns)
    state_body = state_entry.metadata.get("state") if state_entry is not None else None
    if not isinstance(state_body, Mapping):
        return fail("NOT_ESTIMABLE", "S6_RANK_STATE_MISSING_OR_MISBOUND")
    state_envelope = state_body.get("envelope")
    rows = state_body.get("rows")
    if (not isinstance(state_envelope, Mapping) or state_envelope.get("content_hash") != hypothesis.state_ref
            or state_body.get("policy_hash") != S6_POLICY.policy_hash
            or state_body.get("status") != "AVAILABLE"
            or state_body.get("cutoff_ns") != hypothesis.cutoff_ns
            or state_body.get("universe_ref") is None
            or state_body.get("replay_view") != hypothesis.replay_view
            or type(state_body.get("eligible_breadth")) is not int
            or state_body["eligible_breadth"] < MIN_BREADTH
            or not isinstance(rows, (list, tuple))):
        return fail("NOT_ESTIMABLE", "S6_RANK_STATE_NOT_ELIGIBLE_OR_CONTRADICTORY")
    matching_rows = [row for row in rows if isinstance(row, Mapping)
                     and canonical_json(row.get("key")) == hypothesis.key.to_canonical_json()]
    if len(matching_rows) != 1:
        return fail("NOT_ESTIMABLE", "S6_RANK_ROW_MISSING_OR_AMBIGUOUS")
    rank_row = matching_rows[0]
    expected_decile = "TOP" if hypothesis.side == V2Side.LONG else "BOTTOM"
    expected_trend = "UP" if hypothesis.side == V2Side.LONG else "DOWN"
    if (rank_row.get("eligibility") != "ELIGIBLE" or rank_row.get("decile") != expected_decile
            or rank_row.get("own_4h_trend") != expected_trend or rank_row.get("rank") is None
            or rank_row.get("score") != hypothesis.score or rank_row.get("beta_btc") != hypothesis.beta_btc
            or not isinstance(rank_row.get("hourly_refs"), (tuple, list))):
        return fail("NOT_ESTIMABLE", "S6_RANK_ROW_DOES_NOT_AUTHORIZE_HYPOTHESIS")
    rank_hourly_refs = set(rank_row["hourly_refs"])
    universe_ref = str(state_body["universe_ref"])
    universe_entry = _indexed_exact(repository, universe_ref,
        artifact_type="UniverseContractV2", available_by_ns=now_ns)
    if universe_entry is None or not isinstance(universe_entry.metadata.get("universe"), Mapping):
        return fail("NOT_ESTIMABLE", "S6_POINT_IN_TIME_UNIVERSE_MISSING_OR_FUTURE")
    try:
        universe = UniverseContractV2.from_dict(json.loads(canonical_json(universe_entry.metadata["universe"])))
    except (TypeError, ValueError, KeyError):
        return fail("NOT_ESTIMABLE", "S6_POINT_IN_TIME_UNIVERSE_INVALID")
    eligible_rows = [entry for entry in universe.entries if entry.key == hypothesis.key]
    if (universe.content_hash != universe_ref or universe.envelope.available_at_ns > hypothesis.cutoff_ns
            or hypothesis.cutoff_ns > universe.decision_slot_ns or len(eligible_rows) != 1
            or not eligible_rows[0].data_eligible or not eligible_rows[0].scanner_eligible
            or eligible_rows[0].capital_eligible
            or eligible_rows[0].strategy_eligibility.get(POLICY_ID) is None
            or eligible_rows[0].strategy_eligibility[POLICY_ID].status != EligibilityStatusV2.ELIGIBLE):
        return fail("NOT_ESTIMABLE", "S6_UNIVERSE_ELIGIBILITY_OR_CHRONOLOGY_INVALID")

    if len(stop_window_h1) != 3:
        return fail("NOT_ESTIMABLE", "S6_STOP_WINDOW_REQUIRES_THREE_H1_BARS")
    h1 = tuple(stop_window_h1)
    if (tuple(sorted(h1, key=lambda bar: bar.close_at_ns)) != h1
            or any(bar.interval != BarIntervalV2.H1 or not _s6_action_bar_available(
                bar, key=hypothesis.key, view=ReplayViewV2(hypothesis.replay_view),
                cutoff_ns=hypothesis.cutoff_ns) for bar in h1)
            or any(right.close_at_ns - left.close_at_ns != HOUR_NS
                   for left, right in zip(h1, h1[1:], strict=False))
            or h1[-1].close_at_ns != hypothesis.cutoff_ns - hypothesis.cutoff_ns % HOUR_NS
            or any(bar.content_hash not in rank_hourly_refs for bar in h1)):
        return fail("NOT_ESTIMABLE", "S6_STOP_WINDOW_NOT_EXACT_PRIOR_CAUSAL_H1")

    view = ReplayViewV2(hypothesis.replay_view)
    trigger_available_at_ns = (trigger_15m.raw.available_at_ns if view == ReplayViewV2.ACTUAL_SYSTEM
                               else trigger_15m.raw.replay_available_at_ns)
    if (feature.key != hypothesis.key or feature.replay_view != view
            or feature.information_cutoff_ns != decision_cutoff_ns
            or feature.confirmed_at_ns > now_ns
            or feature.envelope.available_at_ns > now_ns
            or previous_15m.interval != BarIntervalV2.M15 or trigger_15m.interval != BarIntervalV2.M15
            or not _s6_action_bar_available(previous_15m, key=hypothesis.key,
                view=view, cutoff_ns=hypothesis.cutoff_ns)
            or not trigger_15m.final or trigger_15m.instrument_revision != hypothesis.key.contract_revision
            or trigger_15m.raw.instrument_revision != hypothesis.key.contract_revision
            or trigger_15m.raw.availability_class != AvailabilityClassV2(view.value)
            or trigger_15m.close_at_ns - previous_15m.close_at_ns != FIFTEEN_MINUTE_NS
            or previous_15m.close_at_ns > hypothesis.cutoff_ns
            or hypothesis.cutoff_ns - previous_15m.close_at_ns > S6_MAX_BBO_AGE_NS
            or trigger_15m.close_at_ns <= hypothesis.cutoff_ns
            or not trigger_15m.close_at_ns <= decision_cutoff_ns <= trigger_15m.close_at_ns + S6_MAX_BBO_AGE_NS
            or trigger_available_at_ns is None or trigger_available_at_ns > decision_cutoff_ns
            or previous_15m.content_hash not in feature.envelope.input_refs
            or trigger_15m.content_hash not in feature.envelope.input_refs
            or any(bar.content_hash not in feature.envelope.input_refs for bar in h1)):
        return fail("NOT_ESTIMABLE", "S6_FEATURE_OR_TRIGGER_NOT_EXACT_CAUSAL")
    feature_entry = _indexed_exact(repository, feature.content_hash,
        artifact_type="FeatureArtifactV2", available_by_ns=now_ns)
    if (feature_entry is None
            or canonical_json(feature_entry.metadata.get("feature")) != canonical_json(feature.to_dict())):
        return fail("NOT_ESTIMABLE", "S6_FEATURE_ARTIFACT_MISSING_OR_MISBOUND")
    if (feature_entry.available_at_ns > decision_cutoff_ns
            and not causal_artifact(repository, feature.content_hash, cutoff_ns=decision_cutoff_ns,
                consumer_at_ns=now_ns, deadline_ns=decision_cutoff_ns + S6_CANDIDATE_DEADLINE_NS)):
        return fail("NOT_ESTIMABLE", "S6_FEATURE_CHRONOLOGY_RECEIPT_MISSING_OR_INVALID")
    atr_value = feature.values.get("m15.atr14")
    if (atr_value is None or atr_value.value is None or atr_value.unit != "price"):
        return fail("NOT_ESTIMABLE", "S6_ATR14_M15_MISSING_OR_INVALID")
    try:
        atr = Decimal(str(atr_value.value))
    except (ArithmeticError, TypeError, ValueError):
        return fail("NOT_ESTIMABLE", "S6_ATR14_M15_MISSING_OR_INVALID")
    if not atr.is_finite() or atr <= 0:
        return fail("NOT_ESTIMABLE", "S6_ATR14_M15_MISSING_OR_INVALID")

    if quote.key != hypothesis.key or not quote.valid_at(decision_cutoff_ns, S6_MAX_BBO_AGE_NS):
        return fail("NOT_ESTIMABLE", "S6_EXECUTABLE_BBO_STALE_OR_MISBOUND")
    if mark_index.key != hypothesis.key or not mark_index.valid_at(decision_cutoff_ns, S6_MAX_MARK_INDEX_AGE_NS):
        return fail("NOT_ESTIMABLE", "S6_MARK_INDEX_STALE_OR_MISBOUND")
    if (event_gate.state == EventState.UNKNOWN or event_gate.available_at_ns > now_ns
            or event_gate.available_at_ns > decision_cutoff_ns + S6_CANDIDATE_DEADLINE_NS
            or (event_gate.available_at_ns <= decision_cutoff_ns
                and not event_gate.valid_at(decision_cutoff_ns))):
        return fail("NOT_ESTIMABLE", "S6_EVENT_GATE_UNKNOWN_STALE_OR_FUTURE")
    if (event_gate.available_at_ns > decision_cutoff_ns
            and not causal_artifact(repository, event_gate.evidence_ref, cutoff_ns=decision_cutoff_ns,
                consumer_at_ns=now_ns, deadline_ns=decision_cutoff_ns + S6_CANDIDATE_DEADLINE_NS)):
        return fail("NOT_ESTIMABLE", "S6_EVENT_GATE_CHRONOLOGY_RECEIPT_MISSING_OR_INVALID")
    if event_gate.state == EventState.BLOCKED:
        return fail("NO_CANDIDATE", "S6_EVENT_GATE_BLOCKED")
    if event_gate.state != EventState.CLEAR:
        return fail("NOT_ESTIMABLE", "S6_EVENT_GATE_NOT_CLEAR")
    trigger_event_at_ns = trigger_15m.raw.event_at_ns
    if trigger_event_at_ns is None or trigger_event_at_ns > decision_cutoff_ns:
        return fail("NOT_ESTIMABLE", "S6_TRIGGER_BAR_LAG_EXCEEDS_POLICY")
    if now_ns > decision_cutoff_ns + S6_CANDIDATE_DEADLINE_NS:
        return fail("NOT_ESTIMABLE", "S6_CANDIDATE_DEADLINE_EXPIRED")
    crossed = (trigger_15m.close > previous_15m.high if hypothesis.side == V2Side.LONG
               else trigger_15m.close < previous_15m.low)
    if not crossed:
        return fail("NO_CANDIDATE", "S6_TRIGGER_RULE_NOT_MET")

    reference = quote.ask if hypothesis.side == V2Side.LONG else quote.bid
    collar_fraction = S6_ENTRY_COLLAR_BPS / Decimal("10000")
    collar = reference * (Decimal(1) + collar_fraction if hypothesis.side == V2Side.LONG
                          else Decimal(1) - collar_fraction)
    if hypothesis.side == V2Side.LONG:
        stop = min(bar.low for bar in h1) - S6_STOP_BUFFER_ATR * atr
        distance = reference - stop
    else:
        stop = max(bar.high for bar in h1) + S6_STOP_BUFFER_ATR * atr
        distance = stop - reference
    if stop <= 0 or distance <= 0:
        return fail("NO_CANDIDATE", "S6_STOP_NOT_ON_ADVERSE_SIDE")
    distance_atr = distance / atr
    if not S6_MIN_STOP_DISTANCE_ATR <= distance_atr <= S6_MAX_STOP_DISTANCE_ATR:
        return fail("NO_CANDIDATE", "S6_STOP_DISTANCE_OUTSIDE_0.5_TO_3_ATR")
    if now_ns < trigger_15m.close_at_ns:
        return fail("NOT_ESTIMABLE", "S6_TRIGGER_NOT_YET_AVAILABLE")

    for ref, kind in ((quote.evidence_ref, "ExecutableQuoteV2"),
                      (mark_index.evidence_ref, "MarkIndexEvidenceV2"),
                      (event_gate.evidence_ref, "EventSafetyGateV2")):
        evidence_entry = _indexed_exact(repository, ref, artifact_type=kind, available_by_ns=now_ns)
        if (evidence_entry is None or evidence_entry.available_at_ns > now_ns
                or (kind != "EventSafetyGateV2" and evidence_entry.available_at_ns > decision_cutoff_ns)):
            return fail("NOT_ESTIMABLE", f"S6_REQUIRED_EVIDENCE_NOT_INDEXED:{kind}")
        if kind == "EventSafetyGateV2":
            body = evidence_entry.metadata.get("gate")
            if (not isinstance(body, Mapping) or body.get("cutoff_ns") != decision_cutoff_ns
                    or body.get("state") != EventState.CLEAR.value or body.get("blocked") is not False
                    or body.get("gate_version") != event_gate.version):
                return fail("NOT_ESTIMABLE", "S6_EVENT_GATE_ARTIFACT_CONTRADICTORY")

    health_entry = _indexed_exact(repository, feature.source_health_ref,
        artifact_type="PublicSourceHealthV2", available_by_ns=decision_cutoff_ns)
    if health_entry is None:
        return fail("NOT_ESTIMABLE", "S6_FEATURE_SOURCE_HEALTH_MISSING_OR_FUTURE")
    health_body = health_entry.metadata.get("health")
    try:
        health = PublicSourceHealthV2.from_dict(dict(health_body)) if isinstance(health_body, Mapping) else None
    except (TypeError, ValueError):
        health = None
    if (health is None or health.content_hash != feature.source_health_ref
            or not health.data_eligible or health.observed_at_ns > decision_cutoff_ns
            or decision_cutoff_ns - health.observed_at_ns > MAX_EVIDENCE_AGE_NS):
        return fail("NOT_ESTIMABLE", "S6_CURRENT_SOURCE_HEALTH_NOT_QUALIFIED")

    deadline_ns = decision_cutoff_ns + S6_CANDIDATE_DEADLINE_NS
    horizon_end_ns = decision_cutoff_ns + FOUR_HOURS_NS
    if now_ns < decision_cutoff_ns or now_ns > deadline_ns or deadline_ns >= horizon_end_ns:
        return fail("NOT_ESTIMABLE", "S6_CANDIDATE_DEADLINE_OR_HORIZON_INVALID")
    watch = repository.get_watch(hypothesis.watch_id)
    if (watch is None or watch.key != hypothesis.key or watch.strategy_id != POLICY_ID
            or watch.policy_hash != S6_POLICY.policy_hash or watch.thesis_hash != hypothesis_ref
            or watch.state != WatchStateV2.CONFIRMED):
        return fail("NOT_ESTIMABLE", "S6_WATCH_NOT_CONFIRMED_OR_MISBOUND")
    trigger_body = {
        "schema_version": 1,
        "action_policy_hash": S6_ACTION_POLICY.policy_hash,
        "rank_policy_hash": S6_POLICY.policy_hash,
        "hypothesis_ref": hypothesis_ref,
        "key": hypothesis.key.to_dict(),
        "side": hypothesis.side.value,
        "trigger_close_at_ns": trigger_15m.close_at_ns,
        "decision_cutoff_ns": decision_cutoff_ns,
        "previous_15m_ref": previous_15m.content_hash,
        "trigger_15m_ref": trigger_15m.content_hash,
        "status": "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT",
        "reason": "S6_ACTION_APPROVAL_PENDING",
        "selector_influence": "ZERO",
    }
    trigger_ref = sha256_json(trigger_body)
    trigger_refs = tuple(sorted({hypothesis_ref, previous_15m.content_hash,
                                 trigger_15m.content_hash, S6_ACTION_POLICY.policy_hash}))
    _index(repository, trigger_ref, "S6ActionTriggerEligibilityV1", now_ns,
        {"trigger": trigger_body, "input_refs": trigger_refs})
    refs = tuple(sorted({
        S6_ACTION_POLICY.policy_hash, S6_POLICY.policy_hash, hypothesis_ref,
        hypothesis.state_ref, universe_ref, feature.content_hash, feature.source_health_ref,
        previous_15m.content_hash, trigger_15m.content_hash, quote.evidence_ref,
        mark_index.evidence_ref, event_gate.evidence_ref, trigger_ref, cost_model_ref,
        *(bar.content_hash for bar in h1), *rank_hourly_refs,
    }))
    candidate_id = sha256_json({
        "version": "S6_ACTION_CANDIDATE_ID_V1",
        "action_policy_hash": S6_ACTION_POLICY.policy_hash,
        "rank_policy_hash": S6_POLICY.policy_hash,
        "hypothesis_ref": hypothesis_ref,
        "state_ref": hypothesis.state_ref,
        "trigger_ref": trigger_15m.content_hash,
        "key": hypothesis.key.to_dict(), "side": hypothesis.side.value,
        "entry_reference": str(reference), "entry_collar": str(collar), "stop_price": str(stop),
        "input_refs": list(refs),
    })
    candidate = CandidateActionV2(
        ArtifactEnvelope(1, candidate_id, now_ns, now_ns, S6_ACTION_PRODUCER_VERSION, refs),
        candidate_id, hypothesis.key, S6_ACTION_POLICY.policy_hash, feature.content_hash,
        hypothesis.side, decision_cutoff_ns, deadline_ns, horizon_end_ns,
        reference, collar, stop, watch.state_version,
        cost_model_ref, quantity=None,
    )
    body = {
        "candidate": candidate.to_dict(), "hypothesis_ref": hypothesis_ref,
        "state_ref": hypothesis.state_ref, "rank_policy_hash": S6_POLICY.policy_hash,
        "action_policy_hash": S6_ACTION_POLICY.policy_hash,
        "stop_window_h1_refs": [bar.content_hash for bar in h1],
        "atr_feature_id": "m15.atr14", "atr_value": str(atr),
        "previous_15m_ref": previous_15m.content_hash, "trigger_15m_ref": trigger_15m.content_hash,
        "bbo_ref": quote.evidence_ref, "mark_index_ref": mark_index.evidence_ref,
        "event_gate_ref": event_gate.evidence_ref,
        "action_trigger_ref": trigger_ref,
        "cost_model_ref": cost_model_ref,
        "entry_policy": "IOC_LIMIT_NO_SAME_EPOCH_REPRICE", "selector_influence": "ZERO",
        "capital_authority": "ZERO", "quantity": None,
    }
    existing = repository.get_artifact(candidate.content_hash)
    if existing is not None:
        if (existing.artifact_type != CandidateActionV2.ARTIFACT_TYPE
                or existing.content_hash != candidate.content_hash
                or canonical_json(existing.metadata) != canonical_json(body)):
            return fail("NOT_ESTIMABLE", "S6_CANDIDATE_IDENTITY_CONFLICT")
    else:
        repository.register_artifact(ArtifactIndexEntryV2(candidate.content_hash,
            CandidateActionV2.ARTIFACT_TYPE, candidate.content_hash, now_ns, now_ns, body))
    return S6ActionBuildResultV1("CANDIDATE", "S6_SHADOW_TRIGGER", candidate, trigger_ref)


def _index(repository: OpsRepository, ref: str, kind: str, at_ns: int, metadata: Mapping[str, object]) -> None:
    existing = repository.get_artifact(ref)
    if existing is not None:
        if (existing.artifact_type != kind or existing.content_hash != ref
                or existing.available_at_ns > at_ns
                or canonical_json(existing.metadata) != canonical_json(metadata)):
            raise ValueError("S6 immutable artifact conflicts with its exact first publication")
        return
    repository.register_artifact(ArtifactIndexEntryV2(ref, kind, ref, at_ns, at_ns, metadata))


def _indexed_evidence_available(repository: OpsRepository, ref: str, cutoff_ns: int) -> bool:
    if not _source_evidence_available(repository, ref, cutoff_ns):
        return False
    entry = repository.get_artifact(ref)
    return bool(entry is not None and (entry.artifact_type != "PublicObservationIndexV2"
        or cutoff_ns - entry.available_at_ns <= MAX_EVIDENCE_AGE_NS))


def _bar_available(bar: CausalBarV2, cutoff_ns: int, replay_view: str) -> bool:
    if (not bar.final or bar.interval != BarIntervalV2.H1 or bar.close_at_ns > cutoff_ns
            or bar.raw.availability_class != AvailabilityClassV2(replay_view)):
        return False
    available = bar.raw.available_at_ns if replay_view == "ACTUAL_SYSTEM" else bar.replay_available_at_ns
    return available is not None and available <= cutoff_ns


def _hourly_returns(bars: Sequence[CausalBarV2], *, key: InstrumentKeyV2, cutoff_ns: int,
                    replay_view: str) -> dict[int, tuple[float, str, str]]:
    by_open: dict[int, CausalBarV2] = {}
    for bar in bars:
        if not _bar_available(bar, cutoff_ns, replay_view) or bar.instrument_revision != key.contract_revision:
            continue
        available = bar.raw.available_at_ns if replay_view == "ACTUAL_SYSTEM" else bar.replay_available_at_ns
        prior = by_open.get(bar.open_at_ns)
        prior_available = (prior.raw.available_at_ns if prior is not None and replay_view == "ACTUAL_SYSTEM"
                           else prior.replay_available_at_ns if prior is not None else None)
        if prior is None or (available, bar.raw.record_id) > (prior_available, prior.raw.record_id):
            by_open[bar.open_at_ns] = bar
    selected = tuple(sorted(by_open.values(), key=lambda bar: (bar.close_at_ns, bar.open_at_ns)))
    result: dict[int, tuple[float, str, str]] = {}
    refs = tuple(bar.content_hash for bar in selected)
    for index, (previous, current) in enumerate(zip(selected, selected[1:], strict=False), start=1):
        if current.close_at_ns - previous.close_at_ns != HOUR_NS:
            continue
        value = math.log(float(current.close) / float(previous.close))
        if math.isfinite(value):
            result[current.close_at_ns] = (value, refs[index - 1], refs[index])
    return result


def _four_hour_context(bars: Sequence[CausalBarV2], key: InstrumentKeyV2, cutoff_ns: int,
                       replay_view: str) -> tuple[str, tuple[str, ...]]:
    """Return the trend and exact completed 4H input refs used by its EMA state."""
    by_open: dict[int, CausalBarV2] = {}
    for bar in bars:
        if (not bar.final or bar.interval != BarIntervalV2.H4
                or bar.instrument_revision != key.contract_revision or bar.close_at_ns > cutoff_ns
                or bar.raw.availability_class != AvailabilityClassV2(replay_view)):
            continue
        available = bar.raw.available_at_ns if replay_view == "ACTUAL_SYSTEM" else bar.replay_available_at_ns
        if available is None or available > cutoff_ns:
            continue
        previous = by_open.get(bar.open_at_ns)
        previous_available = (previous.raw.available_at_ns if previous is not None and replay_view == "ACTUAL_SYSTEM"
                              else previous.replay_available_at_ns if previous is not None else None)
        if previous is None or (available, bar.raw.record_id) > (previous_available, previous.raw.record_id):
            by_open[bar.open_at_ns] = bar
    selected = tuple(sorted(by_open.values(), key=lambda bar: (bar.close_at_ns, bar.open_at_ns)))
    # Only the final 50 bars are needed by the slow EMA and latest-price test.
    sample = selected[-50:]
    if (len(sample) < 50
            or any(right.close_at_ns - left.close_at_ns != FOUR_HOURS_NS
                   for left, right in zip(sample, sample[1:], strict=False))):
        return "UNKNOWN", tuple(bar.content_hash for bar in sample)
    closes = [float(bar.close) for bar in sample]
    ema20, ema50 = ema(closes, 20)[-1], ema(closes, 50)[-1]
    if ema20 is None or ema50 is None:
        return "UNKNOWN", tuple(bar.content_hash for bar in sample)
    if closes[-1] > ema50 and ema20 > ema50:
        return "UP", tuple(bar.content_hash for bar in sample)
    if closes[-1] < ema50 and ema20 < ema50:
        return "DOWN", tuple(bar.content_hash for bar in sample)
    return "MIXED", tuple(bar.content_hash for bar in sample)


class S6ShadowCoordinator:
    def __init__(self, repository: OpsRepository, *, policy: S6ResearchPolicyV2 = S6_POLICY) -> None:
        if policy.policy_hash != S6_POLICY.policy_hash:
            raise ValueError("S6 coordinator requires the exact versioned S6 policy")
        self.repository = repository
        self.policy = policy

    def evaluate(
        self, *, universe: UniverseContractV2, cutoff_ns: int, btc_proxy: InstrumentKeyV2,
        hourly_bars: Mapping[InstrumentKeyV2, Sequence[CausalBarV2]],
        four_hour_bars: Mapping[InstrumentKeyV2, Sequence[CausalBarV2]],
        evidence: Mapping[InstrumentKeyV2, LiquidityFundingEvidenceV2],
        replay_view: str = "ACTUAL_SYSTEM",
        published_at_ns: int | None = None,
    ) -> S6DecisionV2:
        timestamp(cutoff_ns, field="S6.cutoff_ns")
        _expire_due_s6_watches(self.repository, cutoff_ns=cutoff_ns)
        publication = cutoff_ns if published_at_ns is None else timestamp(
            published_at_ns, field="S6.published_at_ns")
        if publication < cutoff_ns:
            raise ValueError("S6 publication cannot precede its information cutoff")
        if replay_view not in ("ACTUAL_SYSTEM", "RECONSTRUCTED_MARKET"):
            raise ValueError("S6 availability view must be explicit")
        refs: set[str] = {universe.content_hash}
        universe_ok = universe.envelope.available_at_ns <= cutoff_ns <= universe.decision_slot_ns
        original_universe_receipt_ref = None
        if universe.envelope.available_at_ns > cutoff_ns:
            original_universe_receipt_ref = chronology_ref(universe.content_hash)
            universe_ok = (universe.envelope.available_at_ns <= publication
                and causal_artifact(self.repository, universe.content_hash, cutoff_ns=cutoff_ns,
                    consumer_at_ns=universe.envelope.available_at_ns,
                    deadline_ns=universe.decision_slot_ns))
        eligible = tuple(entry for entry in universe.entries
                         if entry.data_eligible and not entry.capital_eligible
                         and _same_btc_proxy_cohort(entry.key, btc_proxy)
                         and POLICY_ID in entry.strategy_eligibility
                         and entry.strategy_eligibility[POLICY_ID].status == EligibilityStatusV2.ELIGIBLE)
        eligible_keys = {item.key for item in eligible}
        eligible_breadth = len(eligible)
        rows: list[S6RankRowV2] = []
        context_refs_by_key: dict[InstrumentKeyV2, tuple[str, ...]] = {}
        scored: list[tuple[InstrumentKeyV2, float, float, float, float, str, tuple[str, ...], str]] = []
        btc_rows = _hourly_returns(hourly_bars.get(btc_proxy, ()), key=btc_proxy,
                                   cutoff_ns=cutoff_ns, replay_view=replay_view)
        btc_end = cutoff_ns - cutoff_ns % HOUR_NS
        window_start = btc_end - 30 * DAY_NS
        if not universe_ok:
            reason = "UNIVERSE_NOT_AVAILABLE_AT_CUTOFF"
        elif btc_proxy not in eligible_keys:
            reason = "BTC_PROXY_NOT_STRATEGY_ELIGIBLE_AT_CUTOFF"
        elif eligible_breadth < MIN_BREADTH:
            reason = "INSUFFICIENT_STRATEGY_ELIGIBLE_BREADTH_LT_20"
        elif not btc_rows:
            reason = "BTC_PROXY_UNAVAILABLE"
        elif len([time for time in btc_rows if window_start < time <= btc_end]) < TRAILING_RETURNS:
            reason = "BTC_PROXY_INSUFFICIENT_30_DAY_COMPLETED_HOURLY_HISTORY"
        elif (not math.isfinite(stdev([btc_rows[time][0] for time in sorted(btc_rows)
                                       if window_start < time <= btc_end]) ** 2)
              or stdev([btc_rows[time][0] for time in sorted(btc_rows)
                        if window_start < time <= btc_end]) <= 0):
            reason = "BTC_PROXY_ZERO_OR_INVALID_VARIANCE"
        else:
            reason = None
        for entry in eligible:
            key = entry.key
            refs_for_key: list[str] = []
            item_evidence = evidence.get(key)
            if (item_evidence is not None and item_evidence.key == key
                    and item_evidence.observed_at_ns <= cutoff_ns
                    and item_evidence.available_at_ns <= cutoff_ns):
                refs_for_key.extend((item_evidence.liquidity_ref, item_evidence.funding_ref,
                                     item_evidence.source_health_ref, item_evidence.content_hash))
                if item_evidence.turnover_ref is not None:
                    refs_for_key.append(item_evidence.turnover_ref)
                refs.update(refs_for_key)
                _index(self.repository, item_evidence.content_hash, "S6LiquidityFundingEvidenceV2",
                       publication, {"evidence": item_evidence.to_dict()})
                _index(self.repository, item_evidence.source_health_ref, "PublicSourceHealthV2",
                       item_evidence.source_health.available_at_ns,
                       {"health": item_evidence.source_health.to_dict()})
            trend, context_refs = _four_hour_context(four_hour_bars.get(key, ()), key, cutoff_ns, replay_view)
            context_refs_by_key[key] = context_refs
            if key == btc_proxy:
                rows.append(S6RankRowV2(key, 1.0, None, None, None, None, None, trend,
                                        "EXCLUDED", "BTC_PROXY_IS_REFERENCE_NOT_COMPETITOR", (),
                                        item_evidence.content_hash if item_evidence else None))
                continue
            asset = _hourly_returns(hourly_bars.get(key, ()), key=key, cutoff_ns=cutoff_ns, replay_view=replay_view)
            if (item_evidence is None or item_evidence.key != key
                    or item_evidence.observed_at_ns > cutoff_ns or item_evidence.available_at_ns > cutoff_ns
                    or cutoff_ns - item_evidence.available_at_ns > MAX_EVIDENCE_AGE_NS
                    or not _indexed_evidence_available(self.repository, item_evidence.liquidity_ref, cutoff_ns)
                    or not _indexed_evidence_available(self.repository, item_evidence.funding_ref, cutoff_ns)
                    or (item_evidence.turnover_ref is not None
                        and not _indexed_evidence_available(self.repository, item_evidence.turnover_ref, cutoff_ns))
                    or item_evidence.source_health.state != PublicSourceStateV2.HEALTHY_CURRENT
                    or item_evidence.source_health.available_at_ns > cutoff_ns
                    or item_evidence.source_health.observed_at_ns > cutoff_ns
                    or cutoff_ns - item_evidence.source_health.observed_at_ns > MAX_EVIDENCE_AGE_NS
                    or cutoff_ns - item_evidence.source_health.available_at_ns > MAX_EVIDENCE_AGE_NS):
                rows.append(S6RankRowV2(key, None, None, None, None, None, None, trend,
                                        "EXCLUDED", "FUNDING_OR_LIQUIDITY_EVIDENCE_MISSING_OR_STALE",
                                        tuple(sorted(set(refs_for_key))), None))
                continue
            common_times = sorted(t for t in asset.keys() & btc_rows.keys() if window_start < t <= btc_end)
            considered_times = common_times[-TRAILING_RETURNS:]
            refs_for_key.extend(ref for time in considered_times for ref in (*asset[time][1:], *btc_rows[time][1:]))
            refs.update(refs_for_key)
            if len(common_times) < TRAILING_RETURNS:
                rows.append(S6RankRowV2(key, None, None, None, None, None, None, trend,
                                        "EXCLUDED", "MISSING_PEER_HISTORY_OR_SYNCHRONIZED_HOURLY_INTERSECTION",
                                        tuple(sorted(set(refs_for_key))), item_evidence.content_hash))
                continue
            common_times = considered_times
            asset_returns = [asset[t][0] for t in common_times]
            btc_returns = [btc_rows[t][0] for t in common_times]
            btc_variance = stdev(btc_returns) ** 2 if len(btc_returns) > 1 else 0.0
            if not math.isfinite(btc_variance) or btc_variance <= 0:
                rows.append(S6RankRowV2(key, None, None, None, None, None, None, trend,
                                        "NOT_ESTIMABLE", "BTC_PROXY_ZERO_OR_INVALID_VARIANCE",
                                        tuple(sorted(set(refs_for_key))), item_evidence.content_hash))
                continue
            btc_mean, asset_mean = mean(btc_returns), mean(asset_returns)
            covariance = math.fsum((a - asset_mean) * (b - btc_mean)
                                   for a, b in zip(asset_returns, btc_returns, strict=True)) / (len(common_times) - 1)
            beta = covariance / btc_variance
            residuals = [a - beta * b for a, b in zip(asset_returns, btc_returns, strict=True)]
            volatility = stdev(residuals) if len(residuals) > 1 else math.nan
            if not math.isfinite(beta) or not math.isfinite(volatility) or volatility <= 0:
                rows.append(S6RankRowV2(key, beta if math.isfinite(beta) else None, None,
                                        volatility if math.isfinite(volatility) else None, None, None,
                                        None, trend, "NOT_ESTIMABLE", "RESIDUAL_VOLATILITY_ZERO_OR_INVALID",
                                        tuple(sorted(set(refs_for_key))), item_evidence.content_hash))
                continue
            if len(common_times) < 4 or any(b - a != HOUR_NS for a, b in zip(common_times[-4:], common_times[-3:], strict=False)):
                rows.append(S6RankRowV2(key, beta, None, volatility, None, None, None, trend,
                                        "NOT_ESTIMABLE", "LATEST_FOUR_HOURLY_RETURNS_NOT_CONTIGUOUS",
                                        tuple(sorted(set(refs_for_key))), item_evidence.content_hash))
                continue
            current_4h = math.fsum(residuals[-4:])
            score = current_4h / volatility
            if not math.isfinite(score):
                rows.append(S6RankRowV2(key, beta, current_4h, volatility, None, None, None,
                                        trend, "NOT_ESTIMABLE", "SCORE_NONFINITE", tuple(sorted(set(refs_for_key))),
                                        item_evidence.content_hash))
                continue
            scored.append((key, beta, current_4h, volatility, score, trend,
                           tuple(sorted(set(refs_for_key))), item_evidence.content_hash))
        missing_peers = [row for row in rows if row.key != btc_proxy and row.eligibility != "ELIGIBLE"]
        if reason is None and missing_peers:
            reason = "MISSING_PEER_HISTORY_OR_REQUIRED_FUNDING_LIQUIDITY_EVIDENCE"
        if reason is None and len(scored) < max(0, MIN_BREADTH - 1):
            reason = "INSUFFICIENT_RANKABLE_BREADTH_LT_20"
        decile_size = max(MIN_DECILE_COUNT, math.ceil(len(scored) * 0.10)) if scored else 0
        ranked = sorted(scored, key=lambda item: (-item[4], item[0].to_canonical_json()))
        rank_by_key: dict[InstrumentKeyV2, tuple[int, str | None]] = {}
        for index, item in enumerate(ranked, start=1):
            decile = "TOP" if index <= decile_size else "BOTTOM" if index > len(ranked) - decile_size else None
            rank_by_key[item[0]] = (index, decile)
        scored_by_key = {item[0]: item for item in ranked}
        for row in rows:
            if row.key in rank_by_key:
                rank, decile = rank_by_key[row.key]
                item = scored_by_key[row.key]
                rows[rows.index(row)] = S6RankRowV2(row.key, item[1], item[2], item[3], item[4], rank,
                                                    decile, item[5], "ELIGIBLE", None,
                                                    tuple(sorted({*row.hourly_refs, *item[6]})), item[7])
        present = {item.key for item in rows}
        for key, beta, current_4h, volatility, score, trend, row_refs, evidence_ref in ranked:
            if key not in present:
                rank, decile = rank_by_key[key]
                rows.append(S6RankRowV2(key, beta, current_4h, volatility, score, rank, decile,
                                        trend, "ELIGIBLE", None, row_refs, evidence_ref))
        rows = sorted(rows, key=lambda row: row.key.to_canonical_json())
        rows = [replace(row, context_refs=context_refs_by_key.get(row.key, ())) for row in rows]
        refs.update(ref for row in rows for ref in (*row.hourly_refs, *row.context_refs))
        refs.add(universe.content_hash)
        state_id = sha256_json({"policy_hash": self.policy.policy_hash, "cutoff_ns": cutoff_ns,
                                "universe_ref": universe.content_hash, "btc_proxy": btc_proxy.to_dict(),
                                "status": "NOT_ESTIMABLE" if reason else "AVAILABLE", "reason": reason,
                                "rows": [row.to_dict() for row in rows], "replay_view": replay_view})
        state = S6CrossSectionStateV2(
            ArtifactEnvelope(1, state_id, cutoff_ns, publication, PRODUCER_VERSION, tuple(sorted(refs))),
            self.policy.policy_hash, cutoff_ns, universe.content_hash, btc_proxy,
            "NOT_ESTIMABLE" if reason else "AVAILABLE", reason, tuple(rows), eligible_breadth, decile_size,
            replay_view,
        )
        _index(self.repository, self.policy.policy_hash, "S6ResearchPolicyV2", publication,
               {"policy": self.policy.to_dict()})
        _index(self.repository, state.content_hash, state.ARTIFACT_TYPE, publication,
            {"state": state.to_dict(), "original_universe_receipt_ref": original_universe_receipt_ref,
             "original_decision_deadline_ns": universe.decision_slot_ns,
             "research_published_at_ns": publication, "selector_influence": "ZERO"})
        hypotheses: list[S6HypothesisV2] = []
        if state.status == "AVAILABLE":
            for row in rows:
                if (row.eligibility != "ELIGIBLE" or row.decile not in ("TOP", "BOTTOM")
                        or row.rank is None or row.score is None or row.beta_btc is None):
                    continue
                side = V2Side.LONG if row.decile == "TOP" else V2Side.SHORT
                if (side == V2Side.LONG and row.own_4h_trend != "UP") or (side == V2Side.SHORT and row.own_4h_trend != "DOWN"):
                    continue
                hypothesis_id = sha256_json({"state_ref": state.content_hash, "key": row.key.to_dict(),
                                             "side": side.value, "rank": row.rank, "policy_hash": self.policy.policy_hash})
                watch_id = sha256_json({"hypothesis_id": hypothesis_id, "watch": "S6_15M_TRIGGER"})
                hypothesis = S6HypothesisV2(hypothesis_id, row.key, self.policy.policy_hash,
                                            state.content_hash, side, cutoff_ns, float(row.score),
                                            float(row.beta_btc), "TOP_BOTTOM_DECILE_TREND_ALIGNED", watch_id,
                                            replay_view)
                prior = self.repository.get_watch(watch_id)
                if prior is None:
                    watch = OpportunityWatchV2(
                        watch_id, row.key, POLICY_ID, POLICY_VERSION, self.policy.policy_hash,
                        WatchStateV2.DETECTED, 0, publication, publication, hypothesis_id,
                        tuple(sorted({hypothesis_id, state.content_hash, *row.hourly_refs,
                                      *(ref for ref in (row.evidence_ref,) if ref)})),
                        "BAR_CLOSE_15M", cutoff_ns + BarIntervalV2.M15.duration_ns + 1, cutoff_ns,
                    )
                    self.repository.create_watch(watch)
                    self.repository.transition_watch(
                        watch_id, expected_state_version=0,
                        event_id=sha256_json({"watch": watch_id, "event": "WAIT_15M"}),
                        event_at_ns=cutoff_ns, transition_at_ns=publication,
                        target_state=WatchStateV2.WAITING_FOR_EVENT,
                        outbox_id=sha256_json({"watch": watch_id, "outbox": "WAIT_15M"}),
                    )
                _index(self.repository, hypothesis_id, "S6HypothesisV2", publication,
                       {"hypothesis": hypothesis.to_dict(), "selector_influence": "ZERO"})
                hypotheses.append(hypothesis)
        return S6DecisionV2("NOT_ESTIMABLE" if state.status == "NOT_ESTIMABLE" else "AVAILABLE",
                             reason, state, tuple(hypotheses))

    def confirm_trigger(self, *, hypothesis_id: str, previous_15m: CausalBarV2,
                        trigger_15m: CausalBarV2, cutoff_ns: int) -> tuple[str, str]:
        """Persist S1-style close confirmation, then stop at the absent action contract."""
        timestamp(cutoff_ns, field="S6.trigger_cutoff_ns")
        _expire_due_s6_watches(self.repository, cutoff_ns=cutoff_ns)
        entry = self.repository.get_artifact(hypothesis_id)
        if entry is None or entry.artifact_type != "S6HypothesisV2" or entry.available_at_ns > cutoff_ns:
            return "NOT_ESTIMABLE", "HYPOTHESIS_UNAVAILABLE_AT_TRIGGER"
        payload = entry.metadata.get("hypothesis")
        if not isinstance(payload, Mapping):
            return "NOT_ESTIMABLE", "HYPOTHESIS_EVIDENCE_INVALID"
        from atlas.v2.instruments import InstrumentKeyV2

        key = InstrumentKeyV2.from_dict(payload["key"])
        side = V2Side(payload["side"])
        replay_view = str(payload.get("replay_view", ""))
        previous_available = (previous_15m.raw.available_at_ns if replay_view == "ACTUAL_SYSTEM"
                              else previous_15m.replay_available_at_ns)
        trigger_available = (trigger_15m.raw.available_at_ns if replay_view == "ACTUAL_SYSTEM"
                             else trigger_15m.replay_available_at_ns)
        if (previous_15m.interval != BarIntervalV2.M15 or trigger_15m.interval != BarIntervalV2.M15
                or replay_view not in ("ACTUAL_SYSTEM", "RECONSTRUCTED_MARKET")
                or previous_15m.raw.availability_class != AvailabilityClassV2(replay_view)
                or trigger_15m.raw.availability_class != AvailabilityClassV2(replay_view)
                or not previous_15m.final or not trigger_15m.final
                or previous_15m.instrument_revision != key.contract_revision
                or trigger_15m.instrument_revision != key.contract_revision
                or trigger_15m.close_at_ns - previous_15m.close_at_ns != FIFTEEN_MINUTE_NS
                or trigger_15m.close_at_ns > cutoff_ns or trigger_available is None or trigger_available > cutoff_ns
                or previous_15m.close_at_ns > int(payload["cutoff_ns"])
                or previous_available is None or previous_available > int(payload["cutoff_ns"])
                or trigger_15m.close_at_ns <= int(payload["cutoff_ns"])):
            return "NOT_ESTIMABLE", "TRIGGER_BAR_NOT_SUBSEQUENT_CAUSAL_OR_FULL_KEY_MATCHED"
        crossed = trigger_15m.close > previous_15m.high if side == V2Side.LONG else trigger_15m.close < previous_15m.low
        reason = "CLOSE_CONFIRMED_DIRECTIONAL_TRIGGER" if crossed else "TRIGGER_RULE_NOT_MET"
        trigger_body = {"schema_version": 1, "hypothesis_ref": hypothesis_id,
                        "previous_15m_ref": previous_15m.content_hash,
                        "trigger_15m_ref": trigger_15m.content_hash, "cutoff_ns": cutoff_ns,
                        "side": side.value, "crossed": crossed,
                        "status": "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT" if crossed else "NO_CANDIDATE",
                        "reason": reason if crossed else reason,
                        "missing_contract": "S6_stop_and_exact_action_semantics_RESERVED_FOR_SESSION_023" if crossed else None}
        trigger_ref = sha256_json(trigger_body)
        _index(self.repository, trigger_ref, "S6ConfirmedTriggerV2", cutoff_ns,
               {"trigger": trigger_body, "selector_influence": "ZERO"})
        watch_id = str(payload["watch_id"])
        watch = self.repository.get_watch(watch_id)
        if crossed and watch is not None and watch.state == WatchStateV2.WAITING_FOR_EVENT:
            ready = self.repository.transition_watch(
                watch_id, expected_state_version=watch.state_version,
                event_id=sha256_json({"watch": watch_id, "trigger": trigger_ref, "state": "READY_FOR_RECHECK"}),
                event_at_ns=trigger_15m.close_at_ns, transition_at_ns=cutoff_ns,
                target_state=WatchStateV2.READY_FOR_RECHECK,
                outbox_id=sha256_json({"watch": watch_id, "trigger": trigger_ref, "outbox": "READY_FOR_RECHECK"}),
            ).watch
            self.repository.transition_watch(
                watch_id, expected_state_version=ready.state_version,
                event_id=sha256_json({"watch": watch_id, "trigger": trigger_ref, "state": "CONFIRMED"}),
                event_at_ns=trigger_15m.close_at_ns, transition_at_ns=cutoff_ns,
                target_state=WatchStateV2.CONFIRMED,
                outbox_id=sha256_json({"watch": watch_id, "trigger": trigger_ref, "outbox": "CONFIRMED"}),
            )
        return ("NOT_ESTIMABLE", "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT") if crossed else ("NO_CANDIDATE", reason)
