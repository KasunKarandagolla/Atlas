"""Bounded controller composition for the existing zero-authority analogue API."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass

from .._serialization import json_value, sha256_json, timestamp
from ..contracts import CandidateActionV2, CandidateSetV2, FeatureArtifactV2
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository
from ..science.action import ActionArtifactV2
from ..science.analogue import (
    ANALOGUE_POLICY_HASH,
    DEFAULT_EMBARGO_NS,
    AnalogueActionValueV2,
    AnalogueNotEstimableError,
    build_analogue_compatibility,
    build_analogue_query,
    estimate_causal_analogue,
    not_estimable_analogue,
    observation_from_matured_outcome,
    persist_analogue,
)
from ..science.m0 import FEATURE_ORDER
from ..science.outcomes import MaturedOutcomeV2, executable_action_value_training_eligible

MAX_ANALOGUE_OUTCOMES_V1 = 2048
ANALOGUE_PAGE_SIZE_V1 = 128
_DAY_NS = 86_400_000_000_000
_FEATURE_NAMES = tuple(sorted(name for name in FEATURE_ORDER if not name.startswith("missing:")))
_POLICY = {"version": "RuntimeAnalogueRetrievalPolicyV1", "maximum_outcomes": MAX_ANALOGUE_OUTCOMES_V1,
    "analogue_policy_hash": ANALOGUE_POLICY_HASH, "embargo_ns": DEFAULT_EMBARGO_NS,
    "population_overflow": "NOT ESTIMABLE", "feature_names": list(_FEATURE_NAMES),
    "episode_policy": "UTC_DAY_SHARED_CAUSAL_MARKET_STREAM_V1",
    "regime_policy": "HASH_EXACT_CUTOFF_REGIME_VALUES_AND_MISSINGNESS_V1", "authority": "ZERO"}


@dataclass(frozen=True)
class RuntimeAnalogueDiagnosticV1:
    result_ref: str
    result: AnalogueActionValueV2
    retrieval_receipt_ref: str

    @property
    def reason(self) -> str | None:
        return self.result.reasons[0] if self.result.support_status == "NOT_ESTIMABLE" and self.result.reasons else None


def _regime(repo: OpsRepository, candidate_ref: str, cutoff_ns: int) -> str:
    candidate_entry = repo.get_artifact(candidate_ref)
    raw = candidate_entry.metadata.get("candidate") if candidate_entry else None
    if (candidate_entry is None or candidate_entry.artifact_type != "CandidateActionV2"
            or candidate_entry.content_hash != candidate_ref or not isinstance(raw, Mapping)):
        raise AnalogueNotEstimableError("NOT_ESTIMABLE_ORIGINAL_REGIME_CANDIDATE_UNAVAILABLE")
    candidate = CandidateActionV2.from_dict(json_value(raw))
    if candidate.content_hash != candidate_ref:
        raise AnalogueNotEstimableError("NOT_ESTIMABLE_ORIGINAL_REGIME_CANDIDATE_UNAVAILABLE")
    feature_entry = repo.get_artifact(candidate.snapshot_hash)
    raw_feature = feature_entry.metadata.get("feature") if feature_entry else None
    if (feature_entry is None or feature_entry.artifact_type != "FeatureArtifactV2"
            or feature_entry.content_hash != candidate.snapshot_hash
            or feature_entry.available_at_ns > cutoff_ns or not isinstance(raw_feature, Mapping)):
        raise AnalogueNotEstimableError("NOT_ESTIMABLE_ORIGINAL_REGIME_FEATURE_UNAVAILABLE")
    feature = FeatureArtifactV2.from_dict(json_value(raw_feature))
    if (feature.content_hash != candidate.snapshot_hash or feature.envelope.available_at_ns > cutoff_ns
            or feature.key != candidate.key or feature.information_cutoff_ns > cutoff_ns):
        raise AnalogueNotEstimableError("NOT_ESTIMABLE_ORIGINAL_REGIME_FEATURE_UNAVAILABLE")
    fields = {name: value.to_dict() for name, value in feature.values.items() if name.startswith("regime.")}
    return sha256_json({"version": _POLICY["regime_policy"], "values": fields,
                        "missing_regime_context": not bool(fields)})


def run_analogue_diagnostic_v1(
    repo: OpsRepository, *, action: ActionArtifactV2, candidate: CandidateActionV2,
    candidate_set: CandidateSetV2, cutoff_ns: int, available_at_ns: int,
    clock_ns: Callable[[], int] | None = None,
) -> RuntimeAnalogueDiagnosticV1:
    """Retrieve honest mature labels; fail closed on missing contracts or partial scans."""
    timestamp(cutoff_ns, field="cutoff_ns")
    timestamp(available_at_ns, field="available_at_ns")
    if (action.candidate_ref != candidate.content_hash or action.candidate_set_ref != candidate_set.content_hash
            or candidate.decision_at_ns != cutoff_ns or available_at_ns < max(cutoff_ns, action.available_at_ns)):
        raise ValueError("runtime analogue requires exact frozen action/candidate/cutoff and actual completion time")
    if repo.read_only:
        raise ValueError("runtime analogue persistence belongs to the ops writer")
    reasons: Counter[str] = Counter()
    outcome_refs: list[str] = []
    observations = []
    contracts = {}
    query_ref = None
    scanned = 0
    policy_ref = sha256_json(_POLICY)
    result: AnalogueActionValueV2

    def unavailable(reason: str) -> AnalogueActionValueV2:
        return not_estimable_analogue(action_ref=action.content_hash, candidate_ref=candidate.content_hash,
            candidate_set_ref=candidate_set.content_hash, action_hash=action.action.action_hash,
            information_cutoff_ns=cutoff_ns, reason=reason)

    try:
        if available_at_ns > candidate.deadline_ns:
            raise AnalogueNotEstimableError("NOT_ESTIMABLE_DECISION_DEADLINE_EXPIRED")
        if action.available_at_ns > cutoff_ns:
            raise AnalogueNotEstimableError("NOT_ESTIMABLE_ACTION_ARTIFACT_AVAILABLE_AFTER_MARKET_CUTOFF")
        compatibility = build_analogue_compatibility(repo, action_ref=action.content_hash)
        query = build_analogue_query(repo, action_ref=action.content_hash, candidate_ref=candidate.content_hash,
            candidate_set_ref=candidate_set.content_hash, cutoff_ns=cutoff_ns, compatibility=compatibility,
            feature_names=_FEATURE_NAMES, regime_id=_regime(repo, candidate.content_hash, cutoff_ns))
        query_ref = query.content_hash
        if repo.get_artifact(query_ref) is None:
            query_available_at_ns = max(available_at_ns, clock_ns()) if clock_ns else available_at_ns
            repo.register_artifact(ArtifactIndexEntryV2(query_ref, "AnalogueQueryV2", query_ref,
                query_available_at_ns, query_available_at_ns,
                {"query": query.to_dict(), "retrieval_policy_ref": policy_ref}))
        after = None
        while True:
            if clock_ns is not None and timestamp(clock_ns(), field="analogue page start") > candidate.deadline_ns:
                raise AnalogueNotEstimableError("NOT_ESTIMABLE_DECISION_DEADLINE_EXPIRED")
            page = repo.artifact_entries_by_types_page(("MaturedOutcomeV2",), as_of_ns=cutoff_ns,
                after=after, limit=ANALOGUE_PAGE_SIZE_V1)
            if page.invalid_entry_count:
                raise AnalogueNotEstimableError("NOT_ESTIMABLE_ANALOGUE_OUTCOME_INDEX_INVALID")
            if not page.entries:
                break
            scanned += len(page.entries)
            if scanned > MAX_ANALOGUE_OUTCOMES_V1:
                raise AnalogueNotEstimableError("NOT_ESTIMABLE_ANALOGUE_OUTCOME_POPULATION_EXCEEDS_BOUND")
            for entry in page.entries:
                raw_outcome = entry.metadata.get("outcome")
                if not isinstance(raw_outcome, Mapping):
                    raise AnalogueNotEstimableError("NOT_ESTIMABLE_ANALOGUE_OUTCOME_BODY_INVALID")
                outcome = MaturedOutcomeV2.from_dict(json_value(raw_outcome))
                if (entry.artifact_type != "MaturedOutcomeV2" or outcome.content_hash != entry.artifact_ref
                        or entry.content_hash != outcome.content_hash
                        or entry.available_at_ns != outcome.available_at_ns):
                    raise AnalogueNotEstimableError("NOT_ESTIMABLE_ANALOGUE_OUTCOME_IDENTITY_INVALID")
                if entry.available_at_ns > cutoff_ns:
                    raise AnalogueNotEstimableError("NOT_ESTIMABLE_ANALOGUE_OUTCOME_INDEX_FUTURE_EVIDENCE")
                if (not executable_action_value_training_eligible(outcome, cutoff_ns)
                        or outcome.decision_at_ns >= cutoff_ns or outcome.action_hash == action.action.action_hash):
                    reasons["INELIGIBLE_OR_NON_EXECUTABLE_OUTCOME"] += 1
                    continue
                if (outcome.policy_hash != action.action.policy_hash
                        or outcome.instrument_revision != action.action.key.contract_revision
                        or outcome.venue != action.action.key.venue.value
                        or outcome.product != action.action.key.product.value
                        or outcome.horizon_end_ns - outcome.decision_at_ns != candidate.horizon_end_ns - cutoff_ns):
                    reasons["INCOMPATIBLE_EXACT_ACTION_CONTRACT"] += 1
                    continue
                if outcome.horizon_end_ns > cutoff_ns - DEFAULT_EMBARGO_NS:
                    reasons["LABEL_HORIZON_WITHIN_FROZEN_ANALOGUE_EMBARGO"] += 1
                    continue
                assert outcome.action_artifact_ref and outcome.candidate_ref
                source_contract = build_analogue_compatibility(repo, action_ref=outcome.action_artifact_ref)
                if source_contract != compatibility:
                    reasons["INCOMPATIBLE_EXACT_ACTION_CONTRACT"] += 1
                    continue
                observation = observation_from_matured_outcome(repo, outcome_ref=entry.artifact_ref,
                    cutoff_ns=cutoff_ns, compatibility=source_contract, feature_names=_FEATURE_NAMES,
                    episode_id=sha256_json({"version": _POLICY["episode_policy"],
                        "utc_day": outcome.decision_at_ns // _DAY_NS}),
                    regime_id=_regime(repo, outcome.candidate_ref, outcome.decision_at_ns))
                observations.append(observation)
                contracts[source_contract.compatibility_key] = source_contract
                outcome_refs.append(entry.artifact_ref)
            after = page.next_cursor
            if len(page.raw_keys) < ANALOGUE_PAGE_SIZE_V1:
                break
        result = estimate_causal_analogue(repo, query, tuple(observations), compatibility_contracts=contracts)
    except AnalogueNotEstimableError as exc:
        reasons[exc.reason] += 1
        result = unavailable(exc.reason)
    except (ValueError, TypeError, KeyError, ArithmeticError):
        # No exception message or provider payload is persisted. A broken source
        # contract cannot disappear from a seemingly complete training population.
        reason = "NOT_ESTIMABLE_ANALOGUE_CAUSAL_EVIDENCE_INVALID"
        reasons[reason] += 1
        result = unavailable(reason)
    if clock_ns is not None:
        available_at_ns = max(available_at_ns, timestamp(clock_ns(), field="analogue actual completion"))
    if available_at_ns > candidate.deadline_ns and "NOT_ESTIMABLE_DECISION_DEADLINE_EXPIRED" not in result.reasons:
        reason = "NOT_ESTIMABLE_DECISION_DEADLINE_EXPIRED"
        reasons[reason] += 1
        result = unavailable(reason)
    result_ref = result.content_hash
    existing_result = repo.get_artifact(result_ref)
    if existing_result is None:
        result_ref = persist_analogue(repo, result, available_at_ns=available_at_ns)
    elif (existing_result.artifact_type != "AnalogueActionValueV2" or existing_result.content_hash != result_ref
            or json_value(existing_result.metadata.get("analogue")) != result.to_dict()
            or existing_result.available_at_ns > available_at_ns):
        raise ValueError("existing analogue result identity or actual availability conflicts")
    if repo.get_artifact(policy_ref) is None:
        repo.register_artifact(ArtifactIndexEntryV2(policy_ref, "RuntimeAnalogueRetrievalPolicyV1", policy_ref,
            available_at_ns, available_at_ns, {"policy": _POLICY}))
    receipt = {"version": "RuntimeAnalogueRetrievalReceiptV1", "retrieval_policy_ref": policy_ref,
        "action_ref": action.content_hash, "action_hash": action.action.action_hash,
        "candidate_ref": candidate.content_hash, "candidate_set_ref": candidate_set.content_hash,
        "information_cutoff_ns": cutoff_ns, "available_at_ns": available_at_ns,
        "query_ref": query_ref, "result_ref": result_ref, "loaded_compatible_outcome_refs": sorted(outcome_refs),
        "outcome_index_rows_inspected": scanned,
        "exclusion_reason_counts": dict(sorted(reasons.items())), "authority": "ZERO"}
    receipt_ref = sha256_json(receipt)
    repo.register_artifact(ArtifactIndexEntryV2(receipt_ref, "RuntimeAnalogueRetrievalReceiptV1", receipt_ref,
        available_at_ns, available_at_ns, {"receipt": receipt}))
    return RuntimeAnalogueDiagnosticV1(result_ref, result, receipt_ref)
