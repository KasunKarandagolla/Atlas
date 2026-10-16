"""Source-backed empirical scenario qualification, not a new model framework.

The supported replay method shifts immutable historical template events. Future
simulated event timestamps are legitimate; their generation inputs must be old.
Probability/ES support is computed from disjoint mature observations. Current
qualification grade is explicitly synthetic/offline, never actual market proof.
"""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import ClassVar

from atlas.v2._serialization import sha256_json
from atlas.v2.science.mean_calibration import EvidenceReader, MeanEvidenceScopeV1
from atlas.v2.science.mean_common import DAY_NS, ZERO, OfflineArtifact, unique
from atlas.v2.science.mean_research import ProtectedMeanHoldoutV1
from atlas.v2.science.mean_scenarios import MeanJointPathV1, MeanMarketPointV1, MeanScenarioSetV1


@dataclass(frozen=True)
class ScenarioMarketEvidenceV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_SCENARIO_MARKET_EVIDENCE_V1"
    scope_ref: str
    observed_at_ns: int
    available_at_ns: int
    mark: Decimal
    bid: Decimal
    ask: Decimal
    bid_quantity: Decimal
    ask_quantity: Decimal
    signed_funding_rate: Decimal | None = None
    closed_m15_price: Decimal | None = None

    def __post_init__(self):
        super().__post_init__()
        if self.observed_at_ns > self.available_at_ns:
            raise ValueError("RAW_SOURCE_PUBLICATION_PRECEDES_OBSERVATION")
        MeanMarketPointV1(
            self.observed_at_ns,
            self.mark,
            self.bid,
            self.ask,
            self.bid_quantity,
            self.ask_quantity,
            self.content_hash,
            self.closed_m15_price,
        )


@dataclass(frozen=True)
class ScenarioObservationV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_SCENARIO_CALIBRATION_OBSERVATION_V1"
    scope_ref: str
    origin_ns: int
    label_end_ns: int
    available_at_ns: int
    path_id: str
    entry_delay_ns: int
    exit_delay_ns: int
    loss_cash: Decimal

    def __post_init__(self):
        super().__post_init__()
        if not self.origin_ns < self.label_end_ns <= self.available_at_ns or not self.path_id:
            raise ValueError("SCENARIO_OBSERVATION_MATURITY")


@dataclass(frozen=True)
class ScenarioSourceV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_CAUSAL_SCENARIO_SOURCE_V1"
    scope_ref: str
    account_snapshot_ref: str
    available_at_ns: int
    expires_at_ns: int
    template_origins_ns: tuple[int, ...]
    templates: tuple[MeanJointPathV1, ...]
    stress_ids: tuple[str, ...]
    stress_exit_prices: tuple[Decimal, ...]
    background_path_refs: tuple[str, ...] = ()
    method: str = "SHIFTED_HISTORICAL_EMPIRICAL_PATHS_V1"

    def __post_init__(self):
        super().__post_init__()
        if (
            self.method != "SHIFTED_HISTORICAL_EMPIRICAL_PATHS_V1"
            or not 1 <= len(self.templates) <= 64
            or len(self.templates) != len(self.template_origins_ns)
            or self.available_at_ns >= self.expires_at_ns
        ):
            raise ValueError("BOUNDED_CAUSAL_TEMPLATE_METHOD_REQUIRED")
        unique(tuple(p.common_path_id for p in self.templates))


@dataclass(frozen=True)
class ScenarioProtocolV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_SCENARIO_QUALIFICATION_PROTOCOL_V1"
    scope_ref: str
    holdout_ref: str
    registered_at_ns: int
    train_end_ns: int
    validation_start_ns: int
    validation_end_ns: int
    confidence: Decimal = Decimal(".975")
    embargo_ns: int = DAY_NS
    block_ns: int = 7 * DAY_NS
    minimum_blocks: int = 20
    minimum_tail_observations: int = 5
    maximum_probability_error: Decimal = Decimal(".1")
    dependence_assumption: str = "INDEPENDENT_SYNTHETIC_CALENDAR_BLOCKS_V1"

    def __post_init__(self):
        super().__post_init__()
        if (
            not self.registered_at_ns < self.train_end_ns < self.validation_start_ns < self.validation_end_ns
            or self.validation_start_ns < self.train_end_ns + self.embargo_ns
            or self.embargo_ns != DAY_NS
            or self.dependence_assumption != "INDEPENDENT_SYNTHETIC_CALENDAR_BLOCKS_V1"
            or self.block_ns < 7 * DAY_NS
            or self.minimum_blocks < 20
            or self.minimum_tail_observations < 5
            or not Decimal(".95") <= self.confidence < 1
            or not ZERO <= self.maximum_probability_error <= Decimal(".1")
        ):
            raise ValueError("SCENARIO_PREREGISTRATION_OR_SUPPORT_INVALID")


@dataclass(frozen=True)
class ScenarioExecutionModelV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_SCENARIO_EXECUTION_MODEL_V1"
    scope_ref: str
    protocol_ref: str
    observation_refs: tuple[str, ...]
    available_at_ns: int
    expires_at_ns: int
    entry_delay_ns: int
    exit_delay_ns: int


@dataclass(frozen=True)
class ScenarioProbabilityCalibrationV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_SCENARIO_PROBABILITY_CALIBRATION_V1"
    source_ref: str
    execution_ref: str
    protocol_ref: str
    training_refs: tuple[str, ...]
    validation_refs: tuple[str, ...]
    available_at_ns: int
    expires_at_ns: int
    path_ids: tuple[str, ...]
    probabilities: tuple[Decimal, ...]


@dataclass(frozen=True)
class ScenarioRiskSupportV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_SCENARIO_RISK_SUPPORT_V1"
    scenario_binding_ref: str
    policy_ref: str
    risk_policy_ref: str
    account_snapshot_ref: str
    available_at_ns: int
    expires_at_ns: int
    confidence: Decimal
    effective_blocks: int
    tail_observations: int
    validation_es_cash: Decimal
    scenario_es_cash: Decimal


@dataclass(frozen=True)
class MeanScenarioSetV2(MeanScenarioSetV1):
    VERSION: ClassVar[str] = "ATLAS_RED_QUALIFIED_COMMON_SCENARIOS_V2"
    qualification_ref: str


def scenario_binding(scenarios):
    body = scenarios.to_dict()
    body.pop("version")
    body.pop("qualification_ref", None)
    return sha256_json(body)


def _source(reader, scenarios, *, product_ref, policy_ref, compatibility_ref):
    source = reader.read(scenarios.source_ref, ScenarioSourceV1)
    scope = reader.read(source.scope_ref, MeanEvidenceScopeV1)
    if (
        scope.product_ref != product_ref
        or scope.policy_ref != policy_ref
        or scope.compatibility_ref != compatibility_ref
        or scope.provenance != scenarios.provenance
        or scope.horizon_ns != scenarios.terminal_ns - scenarios.cutoff_ns
        or source.available_at_ns != scenarios.source_available_at_ns
        or source.account_snapshot_ref != scenarios.account_snapshot_ref
        or source.stress_ids != scenarios.stress_ids
        or source.stress_exit_prices != scenarios.stress_exit_prices
        or len(source.templates) != len(scenarios.paths)
    ):
        raise ValueError("SCENARIO_SOURCE_SCOPE_OR_BACKGROUND_CHANGED")
    for template, origin, path in zip(source.templates, source.template_origins_ns, scenarios.paths, strict=True):
        if (
            template.common_path_id != path.common_path_id
            or template.background_pnl_cash != path.background_pnl_cash
            or len(template.points) != len(path.points)
            or len(template.funding) != len(path.funding)
        ):
            raise ValueError("SCENARIO_POPULATION_CHANGED")
        offset = scenarios.cutoff_ns - origin
        for point, simulated in zip(template.points, path.points, strict=True):
            raw = reader.read(point.source_ref, ScenarioMarketEvidenceV1, by_ns=source.available_at_ns)
            expected = MeanMarketPointV1(
                raw.observed_at_ns,
                raw.mark,
                raw.bid,
                raw.ask,
                raw.bid_quantity,
                raw.ask_quantity,
                raw.content_hash,
                raw.closed_m15_price,
            )
            if (
                raw.scope_ref != scope.content_hash
                or point != expected
                or not origin <= point.at_ns <= origin + scope.horizon_ns
                or point.at_ns > source.available_at_ns
                or replace(point, at_ns=point.at_ns + offset) != simulated
            ):
                raise ValueError("SCENARIO_RAW_SOURCE_OR_SIMULATION_CHANGED")
        for payment, simulated in zip(template.funding, path.funding, strict=True):
            raw = reader.read(payment.source_ref, ScenarioMarketEvidenceV1, by_ns=source.available_at_ns)
            if (
                raw.scope_ref != scope.content_hash
                or raw.signed_funding_rate != payment.signed_rate
                or raw.observed_at_ns != payment.at_ns
                or raw.mark != payment.mark_price
                or replace(payment, at_ns=payment.at_ns + offset) != simulated
            ):
                raise ValueError("SCENARIO_FUNDING_SOURCE_CHANGED")
    return source, scope


def _observations(reader, refs, scope, hold, *, low_ns, high_ns):
    unique(refs)
    if not 30 <= len(refs) <= 4096:
        raise ValueError("SCENARIO_INSUFFICIENT_OBSERVATIONS")
    values = tuple(reader.read(r, ScenarioObservationV1) for r in refs)
    unique(tuple(v.origin_ns for v in values))
    if any(
        v.scope_ref != scope.content_hash
        or not low_ns <= v.origin_ns
        or v.label_end_ns >= high_ns
        or v.available_at_ns >= high_ns
        or v.content_hash in hold.outcome_refs
        or v.label_end_ns >= hold.start_ns
        for v in values
    ):
        raise ValueError("SCENARIO_MATURE_SPLIT_OR_HOLDOUT_LEAKAGE")
    return values


def empirical_path_probabilities(counts, ids, total):
    if total <= 0 or not ids:
        raise ValueError("SCENARIO_PROBABILITY_MEMBERSHIP_REQUIRED")
    first = tuple(Decimal(counts[k]) / total for k in ids[:-1])
    return first + (1 - sum(first, ZERO),)


def _probabilities(reader, scenarios, source, scope):
    calibration = reader.read(scenarios.calibration_ref, ScenarioProbabilityCalibrationV1)
    execution = reader.read(scenarios.execution_ref, ScenarioExecutionModelV1)
    protocol = reader.read(calibration.protocol_ref, ScenarioProtocolV1)
    hold = reader.read(protocol.holdout_ref, ProtectedMeanHoldoutV1, by_ns=protocol.registered_at_ns)
    identity = sha256_json({"first_mean_holdout": hold.family_ref, "cell": hold.compatibility_ref})
    marker = reader.repo.get_artifact(identity)
    if (
        protocol.scope_ref != scope.content_hash
        or execution.scope_ref != scope.content_hash
        or calibration.source_ref != source.content_hash
        or calibration.execution_ref != execution.content_hash
        or execution.protocol_ref != protocol.content_hash
        or hold.source_ref != scope.content_hash
        or hold.family_ref != scope.policy_ref
        or hold.compatibility_ref != scope.compatibility_ref
        or protocol.registered_at_ns < scope.registered_at_ns
        or protocol.validation_end_ns > hold.start_ns
        or marker is None
        or marker.metadata.get("holdout_ref") != hold.content_hash
        or marker.content_hash != sha256_json(marker.metadata)
        or set(calibration.training_refs) & set(calibration.validation_refs)
    ):
        raise ValueError("SCENARIO_CALIBRATION_EXECUTION_OR_HOLDOUT_IDENTITY")
    train = _observations(
        reader, calibration.training_refs, scope, hold, low_ns=protocol.registered_at_ns, high_ns=protocol.train_end_ns
    )
    validation = _observations(
        reader,
        calibration.validation_refs,
        scope,
        hold,
        low_ns=protocol.validation_start_ns,
        high_ns=protocol.validation_end_ns,
    )
    if (
        execution.observation_refs != calibration.training_refs
        or execution.entry_delay_ns < max(v.entry_delay_ns for v in train + validation)
        or execution.exit_delay_ns < max(v.exit_delay_ns for v in train + validation)
        or execution.entry_delay_ns != scenarios.entry_delay_ns
        or execution.exit_delay_ns != scenarios.exit_delay_ns
        or execution.available_at_ns < max(v.available_at_ns for v in train)
        or calibration.available_at_ns < max(v.available_at_ns for v in validation)
    ):
        raise ValueError("EXECUTION_DELAY_OR_CALIBRATION_AVAILABILITY_UNSUPPORTED")
    counts = Counter(v.path_id for v in train)
    validate_counts = Counter(v.path_id for v in validation)
    ids = tuple(p.common_path_id for p in scenarios.paths)
    probabilities = empirical_path_probabilities(counts, ids, len(train))
    if (
        set(counts) != set(ids)
        or set(validate_counts) != set(ids)
        or any(counts[k] < 5 for k in ids)
        or calibration.path_ids != ids
        or calibration.probabilities != probabilities
        or tuple(p.probability for p in scenarios.paths) != probabilities
        or any(
            abs(Decimal(validate_counts[k]) / len(validation) - p) > protocol.maximum_probability_error
            for k, p in zip(ids, probabilities, strict=True)
        )
    ):
        raise ValueError("SCENARIO_POPULATION_OR_PROBABILITY_WEIGHTS_UNQUALIFIED")
    from atlas.v2.science.mean_calibration import dependence_diagnostics

    _, effective_blocks, _ = dependence_diagnostics(tuple((v.origin_ns, v.loss_cash) for v in validation), protocol)

    # Independent *calendar* support, never path count or bootstrap draws.
    tail_count = math.floor((1 - protocol.confidence) * len(validation))
    if effective_blocks < protocol.minimum_blocks or tail_count < protocol.minimum_tail_observations:
        raise ValueError("ES_EFFECTIVE_BLOCK_OR_TAIL_SUPPORT_NOT_ESTIMABLE")
    from atlas.v2.science.portfolio import empirical_es_cash

    # Equal empirical observation masses; exact Decimal final mass correction.
    n = len(validation)
    masses = (Decimal(1) / n,) * (n - 1)
    validation_es = empirical_es_cash(
        tuple(v.loss_cash for v in validation), masses + (1 - sum(masses, ZERO),), protocol.confidence
    )
    return protocol, effective_blocks, tail_count, validation_es


def verify_background_paths(reader, source, scenarios, snapshot):
    from atlas.v2._serialization import canonical_json
    from atlas.v2.science.portfolio import ExistingPortfolioPathV2, PortfolioPathComponentV2

    if not snapshot.exposures:
        if source.background_path_refs or any(p.background_pnl_cash for p in scenarios.paths):
            raise ValueError("FLAT_ACCOUNT_BACKGROUND_MUST_BE_ZERO")
        return
    if len(source.background_path_refs) != len(scenarios.paths):
        raise ValueError("MISSING_COMMON_BACKGROUND_PORTFOLIO_EVIDENCE")
    for ref, path in zip(source.background_path_refs, scenarios.paths, strict=True):
        entry = reader.repo.get_artifact(ref)
        if (
            entry is None
            or entry.artifact_type != "ExistingPortfolioPathV2"
            or entry.available_at_ns > reader.cutoff_ns
        ):
            raise ValueError("MISSING_BACKGROUND_PORTFOLIO_PATH")
        body = entry.metadata.get("portfolio", {})
        if (
            set(body)
            != {
                "version",
                "common_path_id",
                "scenario_manifest_ref",
                "decision_at_ns",
                "available_at_ns",
                "horizon_end_ns",
                "completeness_ref",
                "components",
                "payoff",
            }
            or body.get("version") != "V2_EXISTING_PORTFOLIO_COMMON_PATH_V1"
        ):
            raise ValueError("BACKGROUND_PORTFOLIO_SCHEMA_INVALID")
        components = tuple(
            PortfolioPathComponentV2(c["kind"], Decimal(c["payoff"]), c["source_ref"])
            for c in body.get("components", ())
        )
        existing = ExistingPortfolioPathV2(
            body["common_path_id"],
            body["scenario_manifest_ref"],
            body["decision_at_ns"],
            body["available_at_ns"],
            body["completeness_ref"],
            components,
        )
        if (
            existing.content_hash != ref
            or entry.content_hash != ref
            or canonical_json(existing.to_dict()) != canonical_json(body)
            or existing.common_path_id != path.common_path_id
            or existing.completeness_ref != snapshot.content_hash
            or existing.scenario_manifest_ref != source.scope_ref
            or existing.decision_at_ns != scenarios.cutoff_ns
            or existing.horizon_end_ns != scenarios.terminal_ns
            or existing.payoff != path.background_pnl_cash
            or {c.source_ref for c in components} != {e.source_ref for e in snapshot.exposures}
        ):
            raise ValueError("BACKGROUND_PORTFOLIO_IDENTITY_OR_COVERAGE_CHANGED")
        for component in components:
            supporting = reader.repo.get_artifact(component.source_ref)
            from atlas.v2.science.mean_evaluation import _stored_body

            if (
                supporting is None
                or supporting.available_at_ns > reader.cutoff_ns
                or supporting.content_hash != component.source_ref
                or sha256_json(_stored_body(supporting)) != component.source_ref
            ):
                raise ValueError("BACKGROUND_COMPONENT_SOURCE_UNAVAILABLE")
            observation = reader.read(component.source_ref, ScenarioObservationV1)
            if (
                observation.scope_ref != source.scope_ref
                or observation.path_id != path.common_path_id
                or component.payoff != -observation.loss_cash
            ):
                raise ValueError("BACKGROUND_COMPONENT_PAYOFF_UNSUPPORTED")


def verify_scenario_evidence(
    repo, scenarios, *, product_ref, policy, snapshot, compatibility_ref, cutoff_ns, now_ns, scenario_losses=None
):
    if not isinstance(scenarios, MeanScenarioSetV2):
        raise ValueError("UNVERIFIED_LEGACY_SCENARIO_EVIDENCE")
    reader = EvidenceReader(repo, cutoff_ns=cutoff_ns, now_ns=now_ns)
    stored = reader.read(scenarios.content_hash, MeanScenarioSetV2)
    source, scope = _source(
        reader, stored, product_ref=product_ref, policy_ref=policy.content_hash, compatibility_ref=compatibility_ref
    )
    verify_background_paths(reader, source, stored, snapshot)
    protocol, blocks, tail_count, validation_es = _probabilities(reader, stored, source, scope)
    risk = reader.read(stored.qualification_ref, ScenarioRiskSupportV1)
    if (
        scenarios != stored
        or risk.scenario_binding_ref != scenario_binding(stored)
        or risk.policy_ref != policy.content_hash
        or risk.risk_policy_ref != snapshot.risk_policy_ref
        or risk.account_snapshot_ref != snapshot.content_hash
        or source.account_snapshot_ref != snapshot.content_hash
        or scenarios.stress_ids != snapshot.stress_ids
        or risk.confidence != protocol.confidence
        or (policy.require_es and protocol.confidence != policy.es_confidence)
        or risk.effective_blocks != blocks
        or risk.tail_observations != tail_count
        or risk.validation_es_cash != validation_es
        or risk.validation_es_cash > risk.scenario_es_cash
        or not scenarios.probability_support
        or not scenarios.es_qualified
    ):
        raise ValueError("SCENARIO_RISK_SUPPORT_OR_REVISION_CHANGED")
    if scenario_losses is not None:
        from atlas.v2.science.portfolio import empirical_es_cash

        actual_es = empirical_es_cash(scenario_losses, tuple(p.probability for p in scenarios.paths), risk.confidence)
        if actual_es > risk.scenario_es_cash:
            raise ValueError("SCENARIO_ECONOMIC_LOSS_OUTSIDE_QUALIFIED_ENVELOPE")
    return risk


def qualify_scenarios(repo, scenarios, *, policy, snapshot, scenario_es_cash, available_at_ns, expires_at_ns):
    """Offline producer; no qualification is derived from scenario booleans."""
    from atlas.v2.science.mean_evaluation import index_mean_artifact

    reader = EvidenceReader(repo, cutoff_ns=available_at_ns, now_ns=available_at_ns)
    source, scope = _source(
        reader,
        scenarios,
        product_ref=scenarios.product_ref,
        policy_ref=policy.content_hash,
        compatibility_ref=scenarios.compatibility_ref,
    )
    verify_background_paths(reader, source, scenarios, snapshot)
    protocol, blocks, tails, validation_es = _probabilities(reader, scenarios, source, scope)
    if validation_es > scenario_es_cash:
        raise ValueError("SCENARIO_TAIL_ENVELOPE_UNSUPPORTED")
    proof = ScenarioRiskSupportV1(
        scenario_binding(scenarios),
        policy.content_hash,
        snapshot.risk_policy_ref,
        snapshot.content_hash,
        available_at_ns,
        expires_at_ns,
        protocol.confidence,
        blocks,
        tails,
        validation_es,
        scenario_es_cash,
    )
    index_mean_artifact(repo, proof, available_at_ns=available_at_ns)
    payload = scenarios.to_dict()
    payload.pop("version")
    result = MeanScenarioSetV2.from_dict(
        {"version": MeanScenarioSetV2.VERSION, **payload, "qualification_ref": proof.content_hash}
    )
    index_mean_artifact(repo, result, available_at_ns=available_at_ns)
    return result
