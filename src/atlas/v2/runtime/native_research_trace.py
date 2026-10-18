"""Read-only native replay instrumentation; no trading rules or outcome reader.

Production policy owners compute every feature and strategy result. This adapter
preserves their outputs and names missing integration prerequisites separately.
It deliberately cannot turn historical prices into qualified live evidence.
"""

from __future__ import annotations

import hashlib
import inspect
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass, fields
from typing import Any

from atlas.v2._serialization import FrozenMap, canonical_json, json_value, sha256_json, sha256_ref
from atlas.v2.contracts import ReplayViewV2
from atlas.v2.data.bars import BarIntervalV2
from atlas.v2.data.raw import AvailabilityClassV2
from atlas.v2.features import pipeline
from atlas.v2.features.joins import JoinedBars
from atlas.v2.features.technical import interval_coverage_complete
from atlas.v2.instruments import UniverseContractV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.s3_native_cadence import s3_m1_origin_ref
from atlas.v2.science.m15_origin_accounting import M15_ORIGIN_MAX_LATENESS_NS, m15_origin_ref
from atlas.v2.science.mean_evaluation import CandidateMeanInputsV1
from atlas.v2.strategies import s1_trend as s1
from atlas.v2.strategies import s2_breakout as s2
from atlas.v2.strategies import s3_mean_reversion as s3
from atlas.v2.strategies import s6_cross_section as s6

VERSION = "NATIVE_ALGORITHM_RESEARCH_TRACE_V1"
STAGES = ("source", "features", "strategies", "economics", "selection", "management")
MINUTE_NS = BarIntervalV2.M1.duration_ns
M15_NS = BarIntervalV2.M15.duration_ns
HOUR_NS = BarIntervalV2.H1.duration_ns
MAX_RECORD_BYTES = 1_000_000
MAX_BATCH = 64


def owner_identity(function) -> dict[str, str]:
    """Exact owner source identity, not a claim of independent runtime attestation."""
    return {
        "callable": function.__module__ + "." + function.__qualname__,
        "implementation_sha256": hashlib.sha256(inspect.getsource(function).encode()).hexdigest(),
    }


def native_value(result: Any) -> Any:
    return json_value(asdict(result))


def unavailable(reason: str, *, owner: str, status: str = "SOURCE_UNAVAILABLE") -> dict:
    return {"status": status, "reason": reason, "native_invoked": False, "owner": owner}


@dataclass(frozen=True)
class NativeDecisionTraceV1:
    """One original instrument/minute observation, all native/event-clock paths.

    Trace statuses describe observability. Native return payloads are preserved
    separately, so NOT_EVALUATED can never be mistaken for native NO_SETUP.
    """

    decision_id: str
    instrument_key: FrozenMap
    slot_ns: int
    source_ref: str
    code_ref: str
    stages: FrozenMap
    version: str = VERSION
    authority: str = "ZERO"

    def __post_init__(self):
        for ref in (self.decision_id, self.source_ref, self.code_ref):
            sha256_ref(ref, field="trace ref")
        if (
            self.slot_ns % MINUTE_NS
            or self.version != VERSION
            or self.authority != "ZERO"
            or set(self.stages) != set(STAGES)
        ):
            raise ValueError("invalid native trace clock/version/stage population")
        expected = sha256_json(
            {
                "version": VERSION,
                "key": self.instrument_key.to_dict(),
                "slot_ns": self.slot_ns,
                "source_ref": self.source_ref,
                "code_ref": self.code_ref,
            }
        )
        if expected != self.decision_id:
            raise ValueError("decision identity mismatch")
        if len(canonical_json(self.to_dict()).encode()) > MAX_RECORD_BYTES:
            raise ValueError("native trace record exceeds fixed bound")

    def to_dict(self):
        return {
            "version": self.version,
            "decision_id": self.decision_id,
            "instrument_key": self.instrument_key.to_dict(),
            "slot_ns": self.slot_ns,
            "source_ref": self.source_ref,
            "code_ref": self.code_ref,
            "stages": self.stages.to_dict(),
            "authority": self.authority,
        }

    @property
    def content_hash(self):
        return sha256_json(self.to_dict())


class TraceWriter:
    """Reuse OpsRepository atomic immutable artifact batches; no new schema/DB."""

    def __init__(self, repository: OpsRepository):
        self.repository = repository

    def publish(self, records: Mapping[str, tuple[str, dict]]):
        if len(records) > MAX_BATCH:
            raise ValueError("trace batch exceeds fixed bound")
        now = time.time_ns()
        entries = []
        for ref, (kind, body) in records.items():
            if ref != sha256_json(body):
                raise ValueError("trace content identity mismatch")
            if len(canonical_json(body).encode()) > MAX_RECORD_BYTES:
                raise ValueError("trace artifact exceeds fixed byte bound")
            prior = self.repository.get_artifact(ref)
            metadata = {"trace": body, "authority": "ZERO"}
            if prior is not None:
                if prior.artifact_type != kind or canonical_json(prior.metadata) != canonical_json(metadata):
                    raise ValueError("immutable trace identity conflict")
                entries.append(prior)  # Keep original publication, never backdate a replay.
            else:
                entries.append(ArtifactIndexEntryV2(ref, kind, ref, now, now, metadata))
        return self.repository.register_artifacts(entries)

    def publish_decision(self, trace: NativeDecisionTraceV1):
        # A stable slot alias makes changed replay outputs conflict, not become a
        # second silently accepted decision for the same original identity.
        now = time.time_ns()
        dependencies = [trace.source_ref, trace.code_ref]
        dependencies.extend(
            ref
            for ref in (trace.stages["features"].get("ref"), trace.stages["features"].get("price_observation_ref"))
            if ref is not None
        )
        if any(self.repository.get_artifact(ref) is None for ref in dependencies):
            raise ValueError("native trace dependency unavailable")
        body = trace.to_dict()
        entry = ArtifactIndexEntryV2(
            trace.decision_id, VERSION, trace.content_hash, now, now, {"trace": body, "authority": "ZERO"}
        )
        prior = self.repository.get_artifact(trace.decision_id)
        if prior is not None:
            if prior.content_hash != trace.content_hash or canonical_json(prior.metadata) != canonical_json(
                entry.metadata
            ):
                raise ValueError("immutable original decision conflicts")
            return prior
        return self.repository.register_artifact(entry)


def check_prefix(join: JoinedBars):
    for bars, interval in ((join.h4, BarIntervalV2.H4), (join.h1, BarIntervalV2.H1), (join.m15, BarIntervalV2.M15)):
        for bar in bars:
            if (
                bar.interval != interval
                or not bar.final
                or bar.instrument_revision != join.key.contract_revision
                or bar.raw.availability_class != AvailabilityClassV2.RECONSTRUCTED_MARKET
                or bar.replay_available_at_ns is None
                or bar.replay_available_at_ns > join.cutoff_ns
                or bar.close_at_ns > join.cutoff_ns
            ):
                raise ValueError("future/unavailable/wrong-view native prefix")
        expected_close = (join.cutoff_ns - 5_000_000_000) // interval.duration_ns * interval.duration_ns
        if not bars or bars[-1].close_at_ns != expected_close:
            raise ValueError("native terminal interval missing")
        if bars and not interval_coverage_complete(bars):
            raise ValueError("incomplete native prefix cannot be repaired or reseeded")


def observe_prices(join: JoinedBars, *, source_ref: str) -> tuple[dict, dict]:
    """Invoke native stateless feature owner and pure S1/S6 price components.

    The full passed finite prefix is used. No shorter-window EMA/ATR fallback or
    synthetic ActiveCausalHistoryStateV1 is created. Original live state remains
    unknown; the source prefix origin travels with the source record.
    """
    sha256_ref(source_ref, field="source_ref")
    check_prefix(join)
    # Native feature owner demands a reference even for descriptive features.
    # It is an indexed archive-health limitation record, not HEALTHY_CURRENT.
    feature = pipeline.feature_snapshot(
        join, replay_view=ReplayViewV2.RECONSTRUCTED_MARKET, clock_ns=lambda: join.cutoff_ns + 1_000_000_000
    )
    feature_body = {
        "version": "NATIVE_PRICE_FEATURE_OBSERVATION_V1",
        "source_ref": source_ref,
        "native_feature": feature.to_dict(),
        "owner": owner_identity(pipeline.feature_snapshot),
        "scope": "FINITE_SOURCE_PREFIX_NOT_ORIGINAL_LIVE_STATE",
        "authority": "ZERO",
    }
    return feature_body, observe_components(join)


def observe_components(join: JoinedBars) -> dict:
    """Actual native pure S1 precursor/S6 context, with no qualified-action claim."""
    check_prefix(join)
    setup = s1._setup(join) if len(join.h1) >= 50 and len(join.h4) >= 50 else None
    trend, trend_refs = s6._four_hour_context(join.h4, join.key, join.cutoff_ns, "RECONSTRUCTED_MARKET")
    returns = s6._hourly_returns(join.h1, key=join.key, cutoff_ns=join.cutoff_ns, replay_view="RECONSTRUCTED_MARKET")
    native = {
        "s1_precursor": {
            "status": "SOURCE_UNAVAILABLE"
            if len(join.h1) < 50 or len(join.h4) < 50
            else "SETUP_OBSERVED"
            if setup
            else "NO_SETUP",
            "policy_hash": s1.S1_POLICY.policy_hash,
            "native_result": json_value(setup),
            "owner": owner_identity(s1._setup),
            "watch_or_first_trigger_identity": None,
            "candidate": None,
            "reason": "PURE_PRECURSOR_NOT_QUALIFIED_WATCH",
        },
        "s6_context": {
            "policy_hash": s6.S6_POLICY.policy_hash,
            "trend": trend,
            "context_refs": list(trend_refs),
            "completed_hourly_returns": len(returns),
            "latest_hourly_return": returns[max(returns)][0] if returns else None,
            "owner": owner_identity(s6._four_hour_context),
            "return_owner": owner_identity(s6._hourly_returns),
            "rank": None,
            "eligible_breadth": None,
            "reason": "FULL_ELIGIBILITY_FUNDING_LIQUIDITY_UNAVAILABLE",
        },
    }
    return native


def observe_coordinators(
    repository: OpsRepository,
    join: JoinedBars,
    feature,
    *,
    universe: UniverseContractV2 | None,
    event_gate=None,
    bbo=None,
    mark_index=None,
    watch_id=None,
) -> dict:
    """Exact native coordinator outputs only when the required typed seam exists.

    No permissive universe, calendar, quotes or watches are manufactured. Results
    retain every canonical status, reason and candidate/watch/setup identity.
    """
    if universe is None:
        return {
            name: unavailable("POINT_IN_TIME_UNIVERSE_UNAVAILABLE", owner=owner)
            for name, owner in (
                ("S1", owner_identity(s1.S1ShadowCoordinator.create_watch)["callable"]),
                ("S2", owner_identity(s2.S2ShadowCoordinator.on_trigger_close)["callable"]),
            )
        }
    # Current coordinators consume ACTUAL_SYSTEM contracts. Historical raw
    # availability is not backdated to make those owners accept a replay.
    bars = join.h4 + join.h1 + join.m15
    reason = None
    status = "SOURCE_UNAVAILABLE"
    if any(bar.raw.availability_class != AvailabilityClassV2.ACTUAL_SYSTEM for bar in bars):
        reason = "NATIVE_COORDINATOR_ACTUAL_SYSTEM_CONTRACT_NOT_REPLAY_QUALIFIED"
    elif (
        join.m15
        and max(join.cutoff_ns, feature.envelope.available_at_ns)
        > join.m15[-1].close_at_ns + M15_ORIGIN_MAX_LATENESS_NS
    ):
        reason = "ORIGINAL_T_PLUS_5_DEADLINE_MISSED"
        status = "NOT_EVALUATED_DEADLINE"
    if reason is not None:
        return {
            name: unavailable(reason, owner=owner, status=status)
            for name, owner in (
                ("S1", owner_identity(s1.S1ShadowCoordinator.create_watch)["callable"]),
                ("S2", owner_identity(s2.S2ShadowCoordinator.on_trigger_close)["callable"]),
            )
        }
    if join.key != feature.key or join.cutoff_ns != feature.information_cutoff_ns:
        raise ValueError("native coordinator input identity mismatch")
    # Preserve the existing native functions' own ordering of source/gate checks.
    coordinator = s1.S1ShadowCoordinator(repository)
    one = (
        coordinator.create_watch(join, feature, universe=universe, event_gate=event_gate)
        if watch_id is None
        else coordinator.on_bar(watch_id, join, feature, event_gate=event_gate, bbo=bbo, mark_index=mark_index)
    )
    two = s2.S2ShadowCoordinator(repository).on_trigger_close(join, feature, universe=universe, bbo=bbo)
    return {
        "S1": {"native_invoked": True, "native_result": native_value(one)},
        "S2": {"native_invoked": True, "native_result": native_value(two)},
    }


def make_slot(
    *,
    key,
    at: int,
    source_ref: str,
    code_ref: str,
    feature_ref: str | None = None,
    native_ref: str | None = None,
    evaluated: bool = False,
    source_missing: bool = False,
    processing_ready_ns: int | None = None,
):
    """Native-clock observation accounting, not candidate or strategy selection."""
    if at % MINUTE_NS:
        raise ValueError("native calendar must align to completed minutes")
    due_m15, due_h1 = at % M15_NS == 0, at % HOUR_NS == 0
    strategies = {}
    for name, policy in (("S1", s1.S1_POLICY), ("S2", s2.S2_POLICY), ("S3", s3.S3_POLICY)):
        due = due_m15 if policy.decision_event == "CONFIRMED_15M_CLOSE" else True
        row = {"policy_hash": policy.policy_hash, "native_clock": policy.decision_event, "due": due}
        row["origin_ref"] = s3_m1_origin_ref(key, at) if name == "S3" else m15_origin_ref(key, at) if due else None
        row["native_coordinator_invoked"] = False
        if not due:
            row.update(status="NOT_DUE", reason="NATIVE_CLOCK_NOT_DUE")
        elif name == "S3":
            row.update(status="SOURCE_UNAVAILABLE", reason="M1_TRADES_VWAP_COMPLETENESS_BBO_UNAVAILABLE")
        elif source_missing:
            row.update(status="SOURCE_UNAVAILABLE", reason="CURRENT_OR_PREFIX_PRICE_SOURCE_MISSING")
        elif not evaluated:
            row.update(status="NOT_EVALUATED_RESOURCE", reason="DECLARED_INSTRUMENTATION_PRICE_BUDGET")
        else:
            row.update(
                status="SOURCE_UNAVAILABLE",
                reason="POINT_IN_TIME_UNIVERSE_HEALTH_EVENT_BBO_MARK_UNAVAILABLE",
                price_observation_ref=native_ref,
            )
        strategies[name] = row
    for name, clock, due, reason in (
        ("S4", "SEQUENCE_VALID_BOOK_TRADE_EVENT", None, "L2_BBO_AGGRESSOR_SEQUENCE_HEALTH_UNAVAILABLE"),
        ("S5", "ASYNCHRONOUS_DERIVATIVE_STRUCTURAL_EVENT", None, "OI_FUNDING_LIQUIDATION_FLOW_UNAVAILABLE"),
        (
            "S6",
            "COMPLETED_H1_RANK_AND_SUBSEQUENT_M15_WATCH",
            due_h1 or due_m15,
            "PIT_UNIVERSE_FUNDING_LIQUIDITY_AND_WATCH_UNAVAILABLE",
        ),
        (
            "S7",
            "AUTHENTICATED_EVENT_AND_SUBSEQUENT_M5_CLOSE",
            None,
            "AUTHENTICATED_EVENT_CALENDAR_REACTION_UNAVAILABLE",
        ),
        ("S8", "HOURLY_OWNER_DEFINED_PAIR", due_h1, "PAIR_DEFINITION_BOTH_LEG_BOOK_FEES_FUNDING_UNAVAILABLE"),
        (
            "PATH_B",
            "BOUNDED_REGISTERED_RESEARCH_SCREEN",
            None,
            "REGISTERED_SCREEN_POPULATION_AND_ADMISSION_UNAVAILABLE",
        ),
    ):
        strategies[name] = {
            "native_clock": clock,
            "due": due,
            "status": "SOURCE_UNAVAILABLE" if due is not False else "NOT_DUE",
            "reason": reason if due is not False else "NATIVE_CLOCK_NOT_DUE",
            "native_invoked": False,
        }
    m15_deadline = at + M15_ORIGIN_MAX_LATENESS_NS if due_m15 else None
    stages = {
        "source": {
            "ref": source_ref,
            "market_cutoff_ns": at,
            "declared_raw_bar_delay_ns": 5_000_000_000,
            "current_m15_replay_visible_ns": at + 5_000_000_000 if due_m15 and not source_missing else None,
            "current_m1_source_status": "NOT_CAPTURED",
            "original_receipt": "UNKNOWN_NOT_CAPTURED",
            "source_health": "UNVERIFIED",
            "instrument_revision_grade": "RESEARCH_ARCHIVE_SYMBOL_NOT_PIT_CONTRACT_PROOF",
        },
        "features": {
            "ref": feature_ref,
            "price_observation_ref": native_ref,
            "status": "OBSERVED" if feature_ref else "NOT_EVALUATED",
            "replay_processing_ready_ns": (processing_ready_ns or at + 6_000_000_000) if evaluated else None,
            "original_m15_deadline_ns": m15_deadline,
            "deadline_status": "MISSED" if evaluated and due_m15 else "NOT_EVALUATED",
        },
        "strategies": strategies,
        "economics": {
            "status": "NOT ESTIMABLE",
            "native_invoked": False,
            "missing_CandidateMeanInputsV1": [f.name for f in fields(CandidateMeanInputsV1)],
            "candidate_refs": [],
            "exact_actions": [],
            "cash_label": None,
        },
        "selection": {
            "status": "SOURCE_UNAVAILABLE",
            "reason": "NO_QUALIFIED_ELIGIBLE_SET_OR_AGENT_B",
            "brief": None,
            "eligible_set": None,
            "agent_decision": None,
            "fallback": None,
        },
        "management": {
            "status": "NOT ESTIMABLE",
            "reason": "NO_ELIGIBLE_ACTION_OR_GENUINE_POSITION",
            "entry": None,
            "stop": None,
            "management_policy": None,
            "outcome_link": None,
        },
    }
    identity = sha256_json(
        {"version": VERSION, "key": key.to_dict(), "slot_ns": at, "source_ref": source_ref, "code_ref": code_ref}
    )
    return NativeDecisionTraceV1(identity, FrozenMap(key.to_dict()), at, source_ref, code_ref, FrozenMap(stages))


def persist_native_output(
    repository: OpsRepository, result, *, source_ref: str, slot_ns: int, evidence_grade: str
) -> str:
    """Instrument actual returned typed results without re-evaluating or selecting.

    P1 outputs must already exist in their owning index. This namespaced record
    is never a qualification receipt and cannot be consumed as an EligibleSet,
    Brief, action, fill or live control record.
    """
    from atlas.v2.contracts import EligibleSetV1
    from atlas.v2.research.decision_brief import DecisionBriefV1

    allowed = (s1.S1Decision, s2.S2Decision, s6.S6DecisionV2, EligibleSetV1, DecisionBriefV1)
    if not isinstance(result, allowed) or evidence_grade not in {"SYNTHETIC_OFFLINE", "RECONSTRUCTED_MARKET"}:
        raise ValueError("unsupported native result type/evidence grade")
    if repository.get_artifact(source_ref) is None:
        raise ValueError("native result source binding unavailable")
    if isinstance(result, EligibleSetV1):
        from atlas.v2.science.mean_evaluation import read_mean_artifact

        stored = read_mean_artifact(repository, result.content_hash, EligibleSetV1, consumer_ns=time.time_ns())
        if stored != result:
            raise ValueError("native P1 output differs from owning persisted result")
    value = result.to_dict() if hasattr(result, "to_dict") else native_value(result)
    body = {
        "version": "NATIVE_TRACE_RETURN_OBSERVATION_V1",
        "source_ref": source_ref,
        "slot_ns": slot_ns,
        "evidence_grade": evidence_grade,
        "native_type": type(result).__name__,
        "native_result": value,
        "authority": "ZERO",
        "qualification_authority": "NONE",
    }
    ref = sha256_json(body)
    TraceWriter(repository).publish({ref: ("NativeTraceReturnV1", body)})
    return ref
