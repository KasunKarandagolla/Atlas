"""Additive computation receipts; market cutoffs never stand in for publication.

These records confer no decision or capital authority. Legacy artifacts available
by the cutoff retain their existing interpretation. A later derived artifact is
usable only through an exact, recursively checked computation receipt.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from ._serialization import sha256_json, timestamp
from .memory.repository import ArtifactIndexEntryV2, OpsRepository

VERSION = "DerivedComputationChronologyV1"
RECEIPT_FIELDS = frozenset({"version", "artifact_ref", "artifact_content_hash", "artifact_type",
    "market_information_cutoff_ns", "information_cutoff_ns", "computation_started_ns",
    "computation_finished_ns", "available_at_ns", "consumer_deadline_ns", "consumer_eligible",
    "input_refs", "authority"})
DERIVED_TYPES = frozenset({"UniverseObservationV2", "UniverseContractV2", "FeatureArtifactV2",
    "CandidateActionV2", "CandidateSetV2", "ScannerSelectionSourceV1", "ScannerRankEvidenceV1",
    "S1SetupEvidenceV1", "S2SetupEvidenceV1", "S2TriggerEvidenceV1", "SizingDecisionV2", "ActionArtifactV2",
    "EventSafetyGateV2", "ResearchPrerequisiteInventoryV1", "ActiveCausalHistoryStateV1",
    "BroadPreparedHistoryStateV1",
    "OpsEconomicEvidenceResolutionV1", "JointExecutionDataV2", "ScenarioSupportUnitV2",
    "S7DerivedM5BarV1", "S7DerivedPreEventBetaV1", "S7DerivedMarketReturn5MV1",
    "S7DerivedReactionSpreadV1", "EventReactionArtifactV2",
    "NativeStrategyHistoryBarV1",
    "S4FeatureArtifactV2", "S4AbsorptionHypothesisV2", "S4ExpectedResponseBaselineV2", "S5StructuralStageEvidenceV1",
    "S5PreparedDiagnosticsV1", "S5CrowdingContextV2", "OpenInterestObservationV2",
    "OIChangeEvidenceV2", "FundingObservationV2", "S5ContinuationResearchRoleV1",
    "S5ReversalResearchRoleV1",
    "S4ExecutionContextResearchRoleV1", "S4StandaloneShadowResearchRoleV1",
    "S4ResearchRoleV1", "S5ResearchRoleV1", "S6ResearchRoleV1",
    "S7_DIRECTIONAL_SHADOWResearchRoleV1", "MODEL_ARENA_OPTIONALResearchRoleV1",
    "S8_RESEARCH_BASKETResearchRoleV1",
    "S6LiquidityFundingEvidenceV2", "S6ActionTriggerEligibilityV1", "ResearchBasketForecastV2",
    "S8ResidualDiagnosticsV1", "S8BasketOutcomeEvidenceV1", "S8BasketOutcomeV1",
    "TradeLocationInputReceiptV1",
    "PretradeExecutionScenarioV2", "DecisionTimePortfolioCompletenessV2"})
MAX_DEPENDENCIES = 200_000  # Fixed archive context, never a history-sized walk.
PRIOR_PREFIX_TYPES = frozenset({
    "BroadPreparedHistoryStateV1",
    "NativeStrategyHistoryBarV1", "S4FeatureArtifactV2", "S4ExpectedResponseBaselineV2",
    "S4AbsorptionHypothesisV2", "S5StructuralStageEvidenceV1", "S5PreparedDiagnosticsV1",
    "S5CrowdingContextV2", "OpenInterestObservationV2", "OIChangeEvidenceV2", "FundingObservationV2",
    "S7DerivedM5BarV1", "S7DerivedPreEventBetaV1", "S7DerivedMarketReturn5MV1",
    "S7DerivedReactionSpreadV1", "S6LiquidityFundingEvidenceV2",
})


def sample(clock_ns: Callable[[], int], *, floor_ns: int) -> int:
    at = timestamp(clock_ns(), field="derived computation clock")
    if at < floor_ns:
        raise ValueError("derived computation clock regressed")
    return at


def chronology_ref(artifact_ref: str) -> str:
    return sha256_json({"version": VERSION, "artifact_ref": artifact_ref})


def _declared_inputs(entry: ArtifactIndexEntryV2) -> tuple[str, ...]:
    """Check formal target dependencies too; a receipt cannot omit future inputs."""
    wrapper = {"FeatureArtifactV2": "feature", "CandidateActionV2": "candidate",
        "CandidateSetV2": "candidate_set", "UniverseContractV2": "universe",
        "EventSafetyGateV2": "gate"}.get(entry.artifact_type)
    if wrapper is not None:
        body = entry.metadata.get(wrapper)
        envelope = body.get("envelope") if isinstance(body, Mapping) else None
        refs = envelope.get("input_refs", ()) if isinstance(envelope, Mapping) else ()
    elif entry.artifact_type == "SizingDecisionV2":
        body = entry.metadata.get("sizing")
        refs = body.get("risk_input_refs", ()) if isinstance(body, Mapping) else ()
    elif entry.artifact_type == "ResearchPrerequisiteInventoryV1":
        body = entry.metadata.get("prerequisites")
        refs = body.get("input_refs", ()) if isinstance(body, Mapping) else ()
    elif entry.artifact_type == "UniverseObservationV2":
        body = entry.metadata.get("observation")
        refs = body.get("source_refs", ()) if isinstance(body, Mapping) else ()
    elif entry.artifact_type in {"ScannerSelectionSourceV1", "ScannerRankEvidenceV1"}:
        refs = (*entry.metadata.get("input_refs", ()), entry.metadata["universe_ref"],
            *([entry.metadata["source_artifact_ref"]] if entry.artifact_type == "ScannerRankEvidenceV1" else []))
    elif entry.artifact_type in {"JointExecutionDataV2", "ScenarioSupportUnitV2",
            "OpsEconomicEvidenceResolutionV1", "PretradeExecutionScenarioV2",
            "DecisionTimePortfolioCompletenessV2"}:
        key = {"JointExecutionDataV2": "joint_execution_data", "ScenarioSupportUnitV2": "evidence",
            "OpsEconomicEvidenceResolutionV1": "resolution", "PretradeExecutionScenarioV2": "scenario",
            "DecisionTimePortfolioCompletenessV2": "evidence"}[entry.artifact_type]
        body = entry.metadata.get(key)
        if not isinstance(body, Mapping):
            raise ValueError("derived economic target body is unavailable")
        singles = {"JointExecutionDataV2": ("action_artifact_ref", "source_ref", "execution_model_ref", "fee_ref"),
            "ScenarioSupportUnitV2": ("source_episode_ref", "execution_model_ref", "calibration_ref", "template_ref"),
            "OpsEconomicEvidenceResolutionV1": ("candidate_set_ref", "candidate_ref", "action_artifact_ref",
                "product_ref", "account_ref", "source_manifest_ref", "capability_ref"),
            "PretradeExecutionScenarioV2": ("action_artifact_ref",),
            "DecisionTimePortfolioCompletenessV2": ("account_snapshot_ref",)}[entry.artifact_type]
        vectors = {"ScenarioSupportUnitV2": ("source_bundle_refs",),
            "OpsEconomicEvidenceResolutionV1": ("causal_input_refs",),
            "PretradeExecutionScenarioV2": ("source_joint_data_refs",),
            "DecisionTimePortfolioCompletenessV2": ("existing_exposure_refs", "pending_exposure_refs",)}
        refs = [*entry.metadata.get("input_refs", ())]
        refs.extend(body[name] for name in singles if body.get(name) is not None)
        refs.extend(ref for name in vectors.get(entry.artifact_type, ()) for ref in body.get(name, ()))
        if entry.metadata.get("binding_source_ref") is not None:
            refs.append(entry.metadata["binding_source_ref"])
        if entry.artifact_type == "PretradeExecutionScenarioV2":
            refs.extend(body[name]["ref"] for name in ("model_input", "calibration_input", "execution_model_input"))
            refs.extend(item["ref"] for item in body["source_inputs"])
    else:
        refs = entry.metadata.get("input_refs", ())
    if not isinstance(refs, (tuple, list)) or len(refs) > MAX_DEPENDENCIES:
        raise ValueError("derived target input references exceed their bound")
    if any(not isinstance(ref, str) or not ref for ref in refs):
        raise ValueError("derived target input reference is invalid")
    return tuple(refs)


def record_computation(repo: OpsRepository, *, artifact_ref: str, information_cutoff_ns: int,
                       started_ns: int, finished_ns: int, available_ns: int,
                       input_refs: Sequence[str], deadline_ns: int) -> str:
    for name, at in (("information_cutoff_ns", information_cutoff_ns), ("started_ns", started_ns),
            ("finished_ns", finished_ns), ("available_ns", available_ns), ("deadline_ns", deadline_ns)):
        timestamp(at, field=name)
    refs = tuple(sorted(set(input_refs)))
    if len(refs) > MAX_DEPENDENCIES or not (
        information_cutoff_ns <= started_ns <= finished_ns <= available_ns
    ):
        raise ValueError("invalid derived computation chronology")
    entry = repo.get_artifact(artifact_ref)
    if entry is None or entry.artifact_type not in DERIVED_TYPES or entry.available_at_ns != available_ns:
        raise ValueError("computation receipt does not bind exact publication")
    target_available = repo.effective_available_at_ns(artifact_ref)
    if target_available is None:
        raise ValueError("computation target has no durable post-commit observation")
    refs = tuple(sorted(set(refs) | set(_declared_inputs(entry))))
    if len(refs) > MAX_DEPENDENCIES:
        raise ValueError("derived computation dependencies exceed their bound")
    cache: dict[str, ArtifactIndexEntryV2 | None] = {}
    budget = [0]
    validated: set[tuple[str, int, int, int]] = set()
    if any(not causal_artifact(repo, ref, cutoff_ns=information_cutoff_ns,
                               consumer_at_ns=started_ns, deadline_ns=deadline_ns,
                               _cache=cache, _budget=budget, _validated=validated) for ref in refs):
        raise ValueError("derived computation has unavailable or noncausal dependency")
    # The market prefix stays sealed, while a downstream computation can consume
    # a later published transformation of that same prefix. Its own information
    # cutoff must include every immediate input's actual publication (§6.1).
    input_cutoff = information_cutoff_ns
    for ref in refs:
        dependency = cache.get(ref)
        if dependency is None:
            raise ValueError("derived computation lost its checked dependency")
        dependency_available = repo.effective_available_at_ns(ref)
        if dependency_available is None:
            raise ValueError("derived computation input lacks durable publication observation")
        input_cutoff = max(input_cutoff, dependency_available)
    if input_cutoff > started_ns:
        raise ValueError("derived input publication follows computation start")
    body: dict[str, Any] = {"version": VERSION, "artifact_ref": artifact_ref,
        "artifact_content_hash": entry.content_hash, "artifact_type": entry.artifact_type,
        "market_information_cutoff_ns": information_cutoff_ns,
        "information_cutoff_ns": input_cutoff, "computation_started_ns": started_ns,
        "computation_finished_ns": finished_ns, "available_at_ns": available_ns,
        "consumer_deadline_ns": deadline_ns, "consumer_eligible": available_ns <= deadline_ns,
        "input_refs": list(refs), "authority": "ZERO"}
    ref = chronology_ref(artifact_ref)
    repo.register_artifact(ArtifactIndexEntryV2(ref, VERSION, sha256_json(body),
        available_ns, available_ns, {"chronology": body}))
    return ref


def causal_artifact(repo: OpsRepository, ref: str, *, cutoff_ns: int,
                    consumer_at_ns: int, deadline_ns: int, _seen: set[str] | None = None,
                    _cache: dict[str, ArtifactIndexEntryV2 | None] | None = None,
                    _budget: list[int] | None = None,
                    _validated: set[tuple[str, int, int, int]] | None = None) -> bool:
    """Check raw availability at cutoff or a sealed later computation over that prefix."""
    cache = {} if _cache is None else _cache
    effective_cache: dict[str, int | None] = {}
    budget = [0] if _budget is None else _budget
    # This cache lives only within one exact validation call/row snapshot. The
    # same shared dependency can otherwise be recursively checked exponentially
    # often despite the row-read cache. Time bounds are part of its identity;
    # only successful proofs are reused, after checking the current cycle path.
    validated = set() if _validated is None else _validated
    validation_key = (ref, cutoff_ns, consumer_at_ns, deadline_ns)

    def read(artifact_ref: str) -> ArtifactIndexEntryV2 | None:
        if artifact_ref not in cache:
            budget[0] += 1
            if budget[0] > MAX_DEPENDENCIES:
                raise ValueError("derived dependency work budget exhausted")
            cache[artifact_ref] = repo.get_artifact(artifact_ref)
        return cache[artifact_ref]

    def effective(artifact_ref: str) -> int | None:
        if artifact_ref not in effective_cache:
            effective_cache[artifact_ref] = repo.effective_available_at_ns(artifact_ref)
        return effective_cache[artifact_ref]

    try:
        entry = read(ref)
        available = effective(ref)
        if entry is None or available is None or available > consumer_at_ns:
            return False
        if available <= cutoff_ns:
            return True
        if entry.artifact_type not in DERIVED_TYPES:
            return False
        seen = set() if _seen is None else _seen
        if ref in seen or len(seen) >= MAX_DEPENDENCIES:
            return False
        if validation_key in validated:
            return True
        seen.add(ref)
        receipt = read(chronology_ref(ref))
        receipt_available = effective(chronology_ref(ref))
        body = receipt.metadata.get("chronology") if receipt is not None else None
        if (receipt is None or receipt_available is None or receipt_available > consumer_at_ns
                or receipt.artifact_type != VERSION or not isinstance(body, Mapping)
                or set(body) != RECEIPT_FIELDS
                or receipt.content_hash != sha256_json(body)
                or body.get("version") != VERSION or body.get("artifact_ref") != ref
                or body.get("artifact_content_hash") != entry.content_hash
                or body.get("artifact_type") != entry.artifact_type
                or timestamp(body["market_information_cutoff_ns"], field="market_information_cutoff_ns") > cutoff_ns
                or (entry.artifact_type not in PRIOR_PREFIX_TYPES
                    and body.get("market_information_cutoff_ns") != cutoff_ns)
                or body.get("available_at_ns") != entry.available_at_ns
                or body.get("authority") != "ZERO"
                or receipt.available_at_ns != entry.available_at_ns
                or receipt.created_at_ns != entry.available_at_ns
                or type(body.get("consumer_deadline_ns")) is not int
                or type(body.get("consumer_eligible")) is not bool
                or body.get("consumer_eligible") != (entry.available_at_ns <= body["consumer_deadline_ns"])):
            return False
        input_cutoff, start, finish, available = (timestamp(body[name], field=name) for name in (
            "information_cutoff_ns", "computation_started_ns", "computation_finished_ns", "available_at_ns"))
        sealed_cutoff = body["market_information_cutoff_ns"]
        if not sealed_cutoff <= input_cutoff <= start <= finish <= available <= min(
                consumer_at_ns, deadline_ns, body["consumer_deadline_ns"]):
            return False
        deps = body.get("input_refs")
        if not isinstance(deps, (tuple, list)) or len(deps) > MAX_DEPENDENCIES:
            return False
        deps = tuple(sorted(set(deps) | set(_declared_inputs(entry))))
        result = all(causal_artifact(repo, dep, cutoff_ns=sealed_cutoff, consumer_at_ns=start,
                                    deadline_ns=deadline_ns, _seen=seen, _cache=cache, _budget=budget,
                                    _validated=validated) for dep in deps)
        if result:
            actual_input_cutoff = sealed_cutoff
            for dep in deps:
                dependency = read(dep)
                if dependency is None:
                    return False
                actual_input_cutoff = max(actual_input_cutoff, dependency.available_at_ns)
            result = input_cutoff == actual_input_cutoff
        seen.remove(ref)
        if result:
            if len(validated) >= MAX_DEPENDENCIES:
                return False
            validated.add(validation_key)
        return result
    except (TypeError, ValueError, KeyError, RecursionError):
        return False
