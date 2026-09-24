"""S1 frozen shadow trend/pullback policy and durable watch coordinator.

No order, reservation, approval, leverage, protection or quantity API exists here.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from atlas.domain.money import ensure_positive_decimal
from atlas.v2._serialization import FrozenMap, nonblank, sha256_json, timestamp
from atlas.v2.contracts import (
    ArtifactEnvelope,
    CandidateActionV2,
    EligibilityStatusV2,
    FeatureArtifactV2,
    OpportunityWatchV2,
    PolicySpecV2,
    V2Side,
    WatchStateV2,
)
from atlas.v2.data.bars import BarIntervalV2
from atlas.v2.features.joins import JoinedBars
from atlas.v2.features.technical import technical_series
from atlas.v2.instruments import InstrumentKeyV2, UniverseContractV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository

INTERVAL_NS = BarIntervalV2.M15.duration_ns
HOUR_NS = BarIntervalV2.H1.duration_ns
POLICY_ID = "S1_MTF_TREND_PULLBACK"
POLICY_VERSION = "1.0.0-shadow"


class EventState(StrEnum):
    CLEAR = "CLEAR"
    BLOCKED = "BLOCKED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class EventGate:
    state: EventState
    available_at_ns: int
    evidence_ref: str
    version: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "state", EventState(self.state))
        timestamp(self.available_at_ns, field="event_gate.available_at_ns")
        nonblank(self.evidence_ref, field="event_gate.evidence_ref")
        nonblank(self.version, field="event_gate.version")

    def valid_at(self, cutoff_ns: int, max_age_ns: int = HOUR_NS) -> bool:
        return self.state != EventState.UNKNOWN and self.available_at_ns <= cutoff_ns < self.available_at_ns + max_age_ns


@dataclass(frozen=True)
class ExecutableQuote:
    key: InstrumentKeyV2
    bid: Decimal
    ask: Decimal
    observed_at_ns: int
    available_at_ns: int
    evidence_ref: str

    def __post_init__(self) -> None:
        if not isinstance(self.key, InstrumentKeyV2):
            raise ValueError("BBO requires full InstrumentKeyV2")
        object.__setattr__(self, "bid", ensure_positive_decimal(self.bid, field="bid"))
        object.__setattr__(self, "ask", ensure_positive_decimal(self.ask, field="ask"))
        timestamp(self.observed_at_ns, field="bbo.observed_at_ns")
        timestamp(self.available_at_ns, field="bbo.available_at_ns")
        nonblank(self.evidence_ref, field="bbo.evidence_ref")

    def valid_at(self, cutoff_ns: int, max_age_ns: int = 5_000_000_000) -> bool:
        return (self.available_at_ns <= cutoff_ns and self.observed_at_ns <= self.available_at_ns
                and cutoff_ns - self.observed_at_ns <= max_age_ns and self.bid > 0 and self.ask >= self.bid)


@dataclass(frozen=True)
class MarkIndexEvidence:
    key: InstrumentKeyV2
    mark: Decimal
    index: Decimal
    available_at_ns: int
    evidence_ref: str

    def __post_init__(self) -> None:
        if not isinstance(self.key, InstrumentKeyV2):
            raise ValueError("mark/index requires full InstrumentKeyV2")
        object.__setattr__(self, "mark", ensure_positive_decimal(self.mark, field="mark"))
        object.__setattr__(self, "index", ensure_positive_decimal(self.index, field="index"))
        timestamp(self.available_at_ns, field="mark_index.available_at_ns")
        nonblank(self.evidence_ref, field="mark_index.evidence_ref")

    def valid_at(self, cutoff_ns: int, max_age_ns: int = 5_000_000_000) -> bool:
        return self.available_at_ns <= cutoff_ns < self.available_at_ns + max_age_ns and self.mark > 0 and self.index > 0


@dataclass(frozen=True)
class S1Decision:
    status: str
    reason: str
    watch: OpportunityWatchV2 | None = None
    candidate: CandidateActionV2 | None = None


def policy_spec(*, variant: str = "BASELINE", stop_buffer_atr: str = "0.25") -> PolicySpecV2:
    variants = {
        "BASELINE": (), "SIMPLE_4H_TREND": (), "NO_PULLBACK": (),
        "NO_CONTEXT": (), "FUNDING_OI": ("funding", "open_interest"),
        "FLOW": ("flow",), "FUNDING_OI_FLOW": ("funding", "open_interest", "flow"),
    }
    if variant not in variants:
        raise ValueError("unsupported S1 experimental variant")
    if Decimal(stop_buffer_atr) < 0:
        raise ValueError("invalid stop buffer")
    direction_name = {
        "BASELINE": "4H_trend_plus_three_1H_pullback_and_reclaim",
        "SIMPLE_4H_TREND": "4H_trend_only_research_baseline",
        "NO_PULLBACK": "4H_trend_plus_1H_reclaim_without_pullback",
        "NO_CONTEXT": "baseline_direction_without_SMC_candle_Fibonacci_context",
        "FUNDING_OI": "baseline_direction_with_separate_funding_OI_context",
        "FLOW": "baseline_direction_with_separate_flow_context",
        "FUNDING_OI_FLOW": "baseline_direction_with_separate_funding_OI_flow_context",
    }[variant]
    return PolicySpecV2.build(
        policy_id=POLICY_ID + ("_" + variant if variant != "BASELINE" else ""),
        version=POLICY_VERSION, strategy_family="TREND_CONTINUATION_PULLBACK",
        capital_status="SHADOW_ONLY", decision_event="CONFIRMED_15M_CLOSE",
        required_features=tuple(sorted(("final_4h_ohlcv", "final_1h_ohlcv", "final_15m_ohlcv", "atr15m", "realized_volatility",
                           "executable_bbo", "mark_index", "event_gate"))),
        optional_features=tuple(sorted(variants[variant])),
        timeframe_rules=FrozenMap({"regime": "latest_confirmed_4H", "setup": "latest_three_closed_1H",
                                   "trigger": "subsequent_confirmed_15M", "revision": "exact_full_InstrumentKeyV2"}),
        setup_parameters=FrozenMap({"variant": variant, "pullback_bars": 3,
                                    "setup_window": "same_three_closed_1H_bars_used_for_pullback_test",
                                    "event_gate_max_age_ns": HOUR_NS,
                                    "experiment_only": variant != "BASELINE" or stop_buffer_atr != "0.25"}),
        direction_rule=FrozenMap({"long": direction_name, "short": "exact_symmetric_of_" + direction_name}),
        entry_rule=FrozenMap({"order_type": "LIMIT", "time_in_force": "IOC", "shadow_only": True,
                              "no_same_epoch_reprice": True, "trigger": "15M_close_breaks_prior_confirmed_15M_high_or_low",
                              "bbo_max_age_ns": 5_000_000_000, "mark_index_max_age_ns": 5_000_000_000,
                              "confirmed_bar_max_lag_ns": 5_000_000_000, "candidate_deadline_offset_ns": 5_000_000_000}),
        collar_rule=FrozenMap({"adverse_bps": 5, "long_reference": "ask", "short_reference": "bid"}),
        stop_rule=FrozenMap({"setup_window": "same_three_closed_1H_bars", "atr15m_buffer": stop_buffer_atr,
                             "minimum_distance_atr": "0.5", "maximum_distance_atr": "3"}),
        trigger_basis="CONFIRMED_CLOSE", management_rule=FrozenMap({"fixed_stop": True,
            "discretionary_trailing": False, "partial_tp": False, "pyramiding": False,
            "averaging": False, "same_epoch_repricing": False}),
        time_exit_rule=FrozenMap({"after_ns": 4 * HOUR_NS, "type": "TIME_EXIT"}),
        max_hold_ns=4 * HOUR_NS,
        expiry_rule=FrozenMap({"subsequent_15m_bars": 4, "fourth_bar_inclusive": True,
                               "contrary_confirmed_4h_regime": "INVALIDATE"}),
        model_requirements=(),
    )


S1_POLICY = policy_spec()


def experiment_specs() -> tuple[PolicySpecV2, ...]:
    specs = [policy_spec(variant=name) for name in ("BASELINE", "SIMPLE_4H_TREND", "NO_PULLBACK", "NO_CONTEXT",
                                                       "FUNDING_OI", "FLOW", "FUNDING_OI_FLOW")]
    specs.extend((policy_spec(variant="BASELINE", stop_buffer_atr="0.2"),
                  policy_spec(variant="BASELINE", stop_buffer_atr="0.3")))
    return tuple(specs)


def _gate(gate: EventGate | None, cutoff_ns: int) -> tuple[str, str]:
    if gate is None or not gate.valid_at(cutoff_ns):
        return "NOT_ESTIMABLE", "EVENT_GATE_UNKNOWN_OR_STALE"
    if gate.state == EventState.BLOCKED:
        return "NO_CANDIDATE", "EVENT_BLOCKED"
    return "AVAILABLE", "CLEAR"


def _regime(join: JoinedBars) -> V2Side | None:
    if len(join.h4) < 50:
        return None
    values = technical_series(join.h4)[-1]
    e20, e50 = values["ema20"], values["ema50"]
    assert e20 is not None and e50 is not None
    close = float(join.h4[-1].close)
    if close > e50 and e20 > e50:
        return V2Side.LONG
    if close < e50 and e20 < e50:
        return V2Side.SHORT
    return None


def _setup(join: JoinedBars) -> tuple[V2Side, Decimal, tuple[str, ...]] | None:
    side = _regime(join)
    if side is None or len(join.h1) < 50:
        return None
    series = technical_series(join.h1)
    bars = join.h1[-3:]
    features = series[-3:]
    if len(bars) != 3 or any(x["ema20"] is None or x["ema50"] is None for x in features):
        return None
    ema20 = [float(row["ema20"]) for row in features if row["ema20"] is not None]
    ema50 = [float(row["ema50"]) for row in features if row["ema50"] is not None]
    if side == V2Side.LONG:
        touched = any(float(bar.low) <= level for bar, level in zip(bars, ema20, strict=True))
        reclaimed = float(bars[-1].close) > ema20[-1] and float(bars[-1].close) > ema50[-1]
        extreme = min(bar.low for bar in bars)
    else:
        touched = any(float(bar.high) >= level for bar, level in zip(bars, ema20, strict=True))
        reclaimed = float(bars[-1].close) < ema20[-1] and float(bars[-1].close) < ema50[-1]
        extreme = max(bar.high for bar in bars)
    if not touched or not reclaimed:
        return None
    return side, extreme, tuple(bar.content_hash for bar in bars)


def _id(value: object) -> str:
    return sha256_json(value)


class S1ShadowCoordinator:
    def __init__(self, repository: OpsRepository, *, policy: PolicySpecV2 = S1_POLICY, cost_model_ref: str = "S1_SHADOW_COST_UNESTIMATED_V1") -> None:
        if policy.policy_id != POLICY_ID or policy.policy_hash != S1_POLICY.policy_hash:
            raise ValueError("baseline coordinator requires exact S1 baseline policy")
        self.repository = repository
        self.policy = policy
        self.cost_model_ref = cost_model_ref

    def create_watch(self, join: JoinedBars, feature: FeatureArtifactV2, *, event_gate: EventGate | None,
                     universe: UniverseContractV2) -> S1Decision:
        if join.status != "AVAILABLE" or join.key != feature.key or feature.information_cutoff_ns != join.cutoff_ns:
            return S1Decision("NOT_ESTIMABLE", join.reason or "FEATURE_OR_JOIN_MISMATCH")
        if feature.values["location.utc_day_trade_vwap"].value is not None:
            return S1Decision("NOT_ESTIMABLE", "OPTIONAL_INPUT_REQUIRES_POLICY_VARIANT")
        gate_status, gate_reason = _gate(event_gate, join.cutoff_ns)
        if gate_status != "AVAILABLE":
            return S1Decision(gate_status, gate_reason)
        if not (universe.envelope.available_at_ns <= join.cutoff_ns <= universe.decision_slot_ns):
            return S1Decision("NOT_ESTIMABLE", "UNIVERSE_NOT_AVAILABLE_AT_CUTOFF")
        entries = [entry for entry in universe.entries if entry.key == join.key]
        if len(entries) != 1 or not entries[0].scanner_eligible or not entries[0].data_eligible or (
            POLICY_ID not in entries[0].strategy_eligibility or
            entries[0].strategy_eligibility[POLICY_ID].status != EligibilityStatusV2.ELIGIBLE
        ):
            return S1Decision("NO_CANDIDATE", "UNIVERSE_INELIGIBLE")
        if feature.values["m15.atr14"].value is None or feature.values["m15.realized_variance20"].value is None:
            return S1Decision("NOT_ESTIMABLE", "VOLATILITY_WARMUP_MISSING")
        if len(join.h4) < 50 or len(join.h1) < 50:
            return S1Decision("NOT_ESTIMABLE", "EMA_WARMUP_MISSING")
        setup = _setup(join)
        if setup is None:
            return S1Decision("NO_CANDIDATE", "SETUP_RULE_FAILED")
        self.repository.register_artifact(ArtifactIndexEntryV2(feature.content_hash, "FeatureArtifactV2",
            feature.content_hash, feature.envelope.created_at_ns, feature.envelope.available_at_ns,
            {"feature": feature.to_dict()}))
        side, extreme, setup_refs = setup
        assert event_gate is not None
        evidence = {"version": "S1_SETUP_V1", "key": join.key.to_dict(), "policy_hash": self.policy.policy_hash,
                    "side": side.value, "created_at_ns": join.cutoff_ns, "setup_bar_refs": setup_refs,
                    "setup_window_extreme": str(extreme), "h4_ref": join.h4[-1].content_hash,
                    "h1_ref": join.h1[-1].content_hash, "m15_at_creation_ref": join.m15[-1].content_hash,
                    "feature_ref": feature.content_hash, "event_gate_ref": event_gate.evidence_ref}
        thesis = _id(evidence)
        self.repository.register_artifact(ArtifactIndexEntryV2(thesis, "S1SetupEvidenceV1", thesis, join.cutoff_ns,
                                                                 join.cutoff_ns, evidence))
        watch_id = _id({"thesis": thesis, "policy": self.policy.policy_hash})
        prior = self.repository.get_watch(watch_id)
        if prior is not None:
            if prior.thesis_hash != thesis or prior.policy_hash != self.policy.policy_hash:
                raise ValueError("existing watch conflicts with immutable S1 setup")
            return S1Decision("WATCH" if prior.state == WatchStateV2.WAITING_FOR_EVENT else "NO_CANDIDATE",
                              "DUPLICATE_SETUP", prior)
        expires = join.cutoff_ns + 4 * INTERVAL_NS + 1
        watch = OpportunityWatchV2(watch_id, join.key, POLICY_ID, POLICY_VERSION, self.policy.policy_hash,
                                   WatchStateV2.DETECTED, 0, join.cutoff_ns, join.cutoff_ns, thesis,
                                   tuple(sorted({thesis, feature.content_hash, *setup_refs, join.h4[-1].content_hash,
                                                 join.h1[-1].content_hash, event_gate.evidence_ref})),
                                   "CANDLE_CLOSED_15M", expires, join.cutoff_ns)
        watch = self.repository.create_watch(watch)
        if watch.state == WatchStateV2.DETECTED:
            watch = self.repository.transition_watch(watch.watch_id, expected_state_version=watch.state_version,
                event_id=_id({"watch": watch_id, "event": "WAIT"}), event_at_ns=join.cutoff_ns,
                transition_at_ns=join.cutoff_ns, target_state=WatchStateV2.WAITING_FOR_EVENT,
                outbox_id=_id({"watch": watch_id, "outbox": "WAIT"})).watch
        return S1Decision("WATCH", "QUALIFIED_SETUP", watch)

    def on_bar(self, watch_id: str, join: JoinedBars, feature: FeatureArtifactV2, *, event_gate: EventGate | None,
               bbo: ExecutableQuote | None, mark_index: MarkIndexEvidence | None) -> S1Decision:
        watch = self.repository.get_watch(watch_id)
        if watch is None:
            raise KeyError(watch_id)
        if watch.state != WatchStateV2.WAITING_FOR_EVENT:
            return S1Decision("NO_CANDIDATE", "WATCH_NOT_WAITING", watch)
        if (join.key != watch.key or feature.key != watch.key or join.status != "AVAILABLE"
                or feature.information_cutoff_ns != join.cutoff_ns):
            return S1Decision("NOT_ESTIMABLE", join.reason or "REVISION_OR_JOIN_MISMATCH", watch)
        if feature.values["location.utc_day_trade_vwap"].value is not None:
            return S1Decision("NOT_ESTIMABLE", "OPTIONAL_INPUT_REQUIRES_POLICY_VARIANT", watch)
        bar = join.m15[-1]
        if bar.close_at_ns <= watch.created_at_ns or bar.raw.available_at_ns > join.cutoff_ns:
            return S1Decision("NO_CANDIDATE", "PRE_WATCH_OR_UNCONFIRMED_BAR", watch)
        if join.cutoff_ns - bar.close_at_ns > 5_000_000_000:
            return S1Decision("NOT_ESTIMABLE", "LATE_OLD_BAR_REPLAY", watch)
        if bar.close_at_ns > watch.created_at_ns + 4 * INTERVAL_NS or join.cutoff_ns > watch.expires_at_ns:
            expired = self.repository.transition_watch(watch_id, expected_state_version=watch.state_version,
                event_id=_id({"watch": watch_id, "event": "EXPIRE"}), event_at_ns=max(join.cutoff_ns, watch.expires_at_ns),
                transition_at_ns=max(join.cutoff_ns, watch.expires_at_ns), target_state=WatchStateV2.EXPIRED,
                outbox_id=_id({"watch": watch_id, "outbox": "EXPIRE"})).watch
            return S1Decision("NO_CANDIDATE", "FOUR_BAR_EXPIRY", expired)

        def no_candidate(status: str, reason: str) -> S1Decision:
            if bar.close_at_ns < watch.created_at_ns + 4 * INTERVAL_NS:
                return S1Decision(status, reason, watch)
            expired = self.repository.transition_watch(watch_id, expected_state_version=watch.state_version,
                event_id=_id({"watch": watch_id, "bar": bar.content_hash, "event": "EXPIRE"}),
                event_at_ns=bar.close_at_ns, transition_at_ns=watch.expires_at_ns,
                target_state=WatchStateV2.EXPIRED,
                outbox_id=_id({"watch": watch_id, "bar": bar.content_hash, "outbox": "EXPIRE"})).watch
            return S1Decision(status, "FOUR_BAR_EXPIRY" if reason == "TRIGGER_RULE_FAILED" else reason, expired)

        evidence_entry = self.repository.get_artifact(watch.thesis_hash)
        if evidence_entry is None:
            return no_candidate("NOT_ESTIMABLE", "SETUP_EVIDENCE_UNAVAILABLE")
        setup = evidence_entry.metadata
        side = V2Side(str(setup["side"]))
        if _regime(join) != side:
            invalid = self.repository.transition_watch(watch_id, expected_state_version=watch.state_version,
                event_id=_id({"watch": watch_id, "bar": bar.content_hash, "event": "CONTRARY_REGIME"}),
                event_at_ns=bar.close_at_ns, transition_at_ns=join.cutoff_ns,
                target_state=WatchStateV2.INVALIDATED, reason="CONTRARY_CONFIRMED_4H_REGIME",
                outbox_id=_id({"watch": watch_id, "bar": bar.content_hash, "outbox": "INVALIDATE"})).watch
            return S1Decision("NO_CANDIDATE", "CONTRARY_CONFIRMED_4H_REGIME", invalid)
        gate_status, gate_reason = _gate(event_gate, join.cutoff_ns)
        if gate_status != "AVAILABLE":
            return no_candidate(gate_status, gate_reason)
        if len(join.m15) < 2 or join.m15[-2].close_at_ns >= bar.close_at_ns:
            return no_candidate("NOT_ESTIMABLE", "PRIOR_15M_UNAVAILABLE")
        previous = join.m15[-2]
        crossed = bar.close > previous.high if side == V2Side.LONG else bar.close < previous.low
        if not crossed:
            return no_candidate("NO_CANDIDATE", "TRIGGER_RULE_FAILED")
        if bbo is None or bbo.key != watch.key or not bbo.valid_at(join.cutoff_ns):
            return no_candidate("NOT_ESTIMABLE", "BBO_STALE_OR_UNAVAILABLE")
        if mark_index is None or mark_index.key != watch.key or not mark_index.valid_at(join.cutoff_ns):
            return no_candidate("NOT_ESTIMABLE", "MARK_INDEX_STALE_OR_UNAVAILABLE")
        atr_value = feature.values["m15.atr14"].value
        if atr_value is None or feature.values["m15.realized_variance20"].value is None:
            return no_candidate("NOT_ESTIMABLE", "VOLATILITY_UNAVAILABLE")
        atr = Decimal(str(atr_value))
        if atr <= 0:
            return no_candidate("NOT_ESTIMABLE", "ATR_INVALID")
        extreme = Decimal(str(setup["setup_window_extreme"]))
        reference = bbo.ask if side == V2Side.LONG else bbo.bid
        collar = reference * (Decimal("1.0005") if side == V2Side.LONG else Decimal("0.9995"))
        stop = extreme - Decimal("0.25") * atr if side == V2Side.LONG else extreme + Decimal("0.25") * atr
        distance = reference - stop if side == V2Side.LONG else stop - reference
        if distance < Decimal("0.5") * atr or distance > Decimal(3) * atr:
            return no_candidate("NO_CANDIDATE", "STOP_DISTANCE_OUTSIDE_0.5_TO_3_ATR")
        self.repository.register_artifact(ArtifactIndexEntryV2(feature.content_hash, "FeatureArtifactV2",
            feature.content_hash, feature.envelope.created_at_ns, feature.envelope.available_at_ns,
            {"feature": feature.to_dict()}))
        refs = tuple(sorted({watch.thesis_hash, feature.content_hash, bar.content_hash, previous.content_hash,
                             bbo.evidence_ref, mark_index.evidence_ref, event_gate.evidence_ref if event_gate else ""} - {""}))
        candidate_id = _id({"watch_id": watch_id, "trigger_ref": bar.content_hash, "policy_hash": self.policy.policy_hash})
        envelope = ArtifactEnvelope(1, candidate_id, join.cutoff_ns, join.cutoff_ns, "S1_SHADOW_V1", refs)
        candidate = CandidateActionV2(envelope, candidate_id, watch.key, self.policy.policy_hash,
            feature.content_hash, side, join.cutoff_ns, join.cutoff_ns + 5_000_000_000,
            join.cutoff_ns + 4 * HOUR_NS, reference, collar, stop, watch.state_version + 2,
            self.cost_model_ref, quantity=None)
        self.repository.register_artifact(ArtifactIndexEntryV2(candidate.content_hash, "CandidateActionV2",
            candidate.content_hash, candidate.envelope.created_at_ns, candidate.envelope.available_at_ns,
            {"candidate": candidate.to_dict(), "watch_id": watch_id, "feature_hash": feature.content_hash,
             "setup_evidence_ref": watch.thesis_hash, "trigger_ref": bar.content_hash,
             "prior_15m_ref": previous.content_hash, "bbo_ref": bbo.evidence_ref,
             "bbo_observed_at_ns": bbo.observed_at_ns, "bbo_available_at_ns": bbo.available_at_ns,
             "mark_index_ref": mark_index.evidence_ref, "mark_index_available_at_ns": mark_index.available_at_ns,
             "event_gate_ref": event_gate.evidence_ref if event_gate else "",
             "entry_policy": "IOC_NO_SAME_EPOCH_REPRICE"}))
        ready = self.repository.transition_watch(watch_id, expected_state_version=watch.state_version,
            event_id=_id({"watch": watch_id, "bar": bar.content_hash, "event": "READY"}),
            event_at_ns=bar.close_at_ns, transition_at_ns=join.cutoff_ns,
            target_state=WatchStateV2.READY_FOR_RECHECK,
            outbox_id=_id({"watch": watch_id, "bar": bar.content_hash, "outbox": "READY"})).watch
        confirmed = self.repository.transition_watch(watch_id, expected_state_version=ready.state_version,
            event_id=_id({"watch": watch_id, "bar": bar.content_hash, "event": "CONFIRMED"}),
            event_at_ns=bar.close_at_ns, transition_at_ns=join.cutoff_ns,
            target_state=WatchStateV2.CONFIRMED,
            outbox_id=_id({"watch": watch_id, "bar": bar.content_hash, "outbox": "CONFIRMED"})).watch
        return S1Decision("CANDIDATE", "SHADOW_TRIGGER", confirmed, candidate)

    def accept_handoff(self, watch_id: str, candidate_ref: str, *, pipeline_acceptance_ref: str,
                       accepted_at_ns: int) -> OpportunityWatchV2:
        watch = self.repository.get_watch(watch_id)
        entry = self.repository.get_artifact(candidate_ref)
        if watch is None or watch.state != WatchStateV2.CONFIRMED or entry is None or entry.artifact_type != "CandidateActionV2":
            raise ValueError("handoff requires confirmed watch and indexed immutable candidate")
        if entry.metadata["watch_id"] != watch_id or entry.content_hash != candidate_ref or not pipeline_acceptance_ref:
            raise ValueError("handoff candidate or pipeline receipt mismatch")
        acceptance = self.repository.get_artifact(pipeline_acceptance_ref)
        if (acceptance is None or acceptance.artifact_type != "ResearchCandidateAcceptanceV1"
                or acceptance.content_hash != pipeline_acceptance_ref
                or acceptance.available_at_ns > accepted_at_ns
                or acceptance.metadata.get("status") != "ACCEPTED"
                or acceptance.metadata.get("candidate_ref") != candidate_ref
                or acceptance.metadata.get("feature_hash") != entry.metadata["feature_hash"]
                or acceptance.metadata.get("watch_id") != watch_id):
            raise ValueError("handoff requires indexed research-pipeline acceptance for exact candidate evidence")
        receipt_body = {"kind": "PIPELINE_ACCEPTED_S1_CANDIDATE_V1", "watch_id": watch_id,
                        "candidate_ref": candidate_ref, "candidate_hash": entry.content_hash,
                        "feature_hash": entry.metadata["feature_hash"],
                        "setup_evidence_ref": entry.metadata["setup_evidence_ref"],
                        "pipeline_acceptance_ref": pipeline_acceptance_ref,
                        "accepted_at_ns": accepted_at_ns}
        receipt = _id(receipt_body)
        self.repository.register_artifact(ArtifactIndexEntryV2(receipt, "S1PipelineHandoffReceiptV1",
            receipt, accepted_at_ns, accepted_at_ns, receipt_body))
        return self.repository.transition_watch(watch_id, expected_state_version=watch.state_version,
            event_id=_id({"watch": watch_id, "candidate": candidate_ref, "event": "HANDOFF"}),
            event_at_ns=accepted_at_ns, transition_at_ns=accepted_at_ns,
            target_state=WatchStateV2.HANDED_OFF, handoff_receipt=receipt,
            outbox_id=_id({"watch": watch_id, "candidate": candidate_ref, "outbox": "HANDOFF"})).watch
