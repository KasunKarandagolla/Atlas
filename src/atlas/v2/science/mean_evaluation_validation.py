"""Checked complete evaluation provenance; one bounded receipt read at B.

The offline writer reconstructs risk sizing, exact action and all economic and
eligibility terms from the original indexed inputs. A self-consistent saved
evaluation or a portable self-issued receipt is never sufficient.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import ClassVar

from atlas.v2._serialization import json_value, sha256_json
from atlas.v2.contracts import ActionEvaluationV1, CandidateActionV2, CandidateEvaluationSetV1, PolicySpecV2
from atlas.v2.instruments import InstrumentKeyV2, ProductContractV2, UniverseContractV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2
from atlas.v2.risk import IndependentRiskProfileV1, OfflineRiskSnapshotV1, size_independent_candidate
from atlas.v2.science.action import ExactActionV1, freeze_independent_action
from atlas.v2.science.costs import FeeScheduleV2, FundingScheduleV2
from atlas.v2.science.mean_calibration import MeanSelectionCalibrationV2
from atlas.v2.science.mean_common import OfflineArtifact
from atlas.v2.science.mean_evaluation import (
    CandidateMeanInputsV1,
    CashAllowanceV1,
    MeanContextV1,
    MeanEvaluationPolicyV1,
    _evaluate_one,
    _require,
    _stored_body,
    index_mean_artifact,
    read_mean_artifact,
)
from atlas.v2.science.mean_models import MeanModelV1
from atlas.v2.science.mean_scenario_evidence import (
    MeanScenarioSetV2,
    ScenarioExecutionModelV1,
    ScenarioProbabilityCalibrationV1,
    ScenarioRiskSupportV1,
    ScenarioSourceV1,
)

INPUT_TYPES = (CandidateActionV2, ProductContractV2, PolicySpecV2, FeeScheduleV2, FundingScheduleV2,
               IndependentRiskProfileV1, MeanModelV1, MeanSelectionCalibrationV2, MeanScenarioSetV2,
               CashAllowanceV1, CashAllowanceV1, CashAllowanceV1, MeanContextV1)


@dataclass(frozen=True)
class MeanEvaluationInputsV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_COMPLETE_MEAN_EVALUATION_INPUTS_V1"
    input_refs: tuple[str, ...]

    def __post_init__(self):
        super().__post_init__()
        if len(self.input_refs) != len(INPUT_TYPES):
            raise ValueError("COMPLETE_EVALUATION_INPUTS_REQUIRED")


@dataclass(frozen=True)
class MeanEvaluationValidationV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_COMPLETE_MEAN_EVALUATION_VALIDATION_V1"
    evaluation_ref: str
    inputs_ref: str
    implementation_ref: str
    available_at_ns: int
    expires_at_ns: int

    def __post_init__(self):
        super().__post_init__()
        if self.available_at_ns >= self.expires_at_ns:
            raise ValueError("EXPIRED_EVALUATION_VALIDATION")


def evaluation_validation_ref(evaluation_ref, implementation_ref):
    return sha256_json({"complete_mean_evaluation_validation": evaluation_ref,
                        "implementation_ref": implementation_ref})


def _entry(receipt):
    return ArtifactIndexEntryV2(evaluation_validation_ref(receipt.evaluation_ref, receipt.implementation_ref),
        type(receipt).__name__, receipt.content_hash, receipt.available_at_ns, receipt.available_at_ns,
        {"mean_artifact": receipt.to_dict(), "authority": "ZERO"})


def _read_source(repo, ref, cls, by_ns):
    entry = repo.get_artifact(ref)
    if entry is None:
        raise ValueError("EVALUATION_SOURCE_UNAVAILABLE")
    body = json_value(_stored_body(entry))
    if cls is FeeScheduleV2:
        obj = FeeScheduleV2(InstrumentKeyV2.from_dict(body["key"]), body["available_at_ns"],
                            Decimal(body["entry_taker_rate"]), Decimal(body["exit_taker_rate"]), body["source_ref"])
    elif cls is FundingScheduleV2:
        obj = FundingScheduleV2(body["available_at_ns"], tuple(body["expected_settlement_times_ns"]),
                                body["explicit_zero_funding"], body["source_ref"])
    else:
        obj = cls.from_dict(body)
    _require(repo, obj, by_ns)
    return obj


def _expires(repo, inp, population, snapshot):
    scenario_expiry = min(read_mean_artifact(repo, ref, cls, consumer_ns=population.deadline_ns).expires_at_ns
        for ref, cls in ((inp.scenarios.qualification_ref, ScenarioRiskSupportV1),
                         (inp.scenarios.execution_ref, ScenarioExecutionModelV1),
                         (inp.scenarios.calibration_ref, ScenarioProbabilityCalibrationV1),
                         (inp.scenarios.source_ref, ScenarioSourceV1)))
    return min(population.deadline_ns, snapshot.expires_at_ns, inp.model.expires_at_ns,
        inp.calibration.expires_at_ns, scenario_expiry,
        inp.execution_bias.expires_at_ns, inp.numerical.expires_at_ns, inp.incremental_programme.expires_at_ns,
        inp.context.source_observed_at_ns + inp.context.maximum_source_age_ns + 1,
        snapshot.observed_at_ns + inp.risk.max_snapshot_age_ns + 1)


def publish_evaluation_validation(repo, evaluation, inp):
    """Public offline publication: the reserved writer independently checks."""
    entry = _publication_entry(repo, evaluation, inp)
    repo.register_artifact(entry)


def _publication_entry(repo, evaluation, inp):
    from atlas.v2.science.mean_calibration_validation import validation_implementation_ref

    manifest = MeanEvaluationInputsV1(tuple(obj.content_hash for obj in vars(inp).values()))
    index_mean_artifact(repo, manifest, available_at_ns=evaluation.available_at_ns)
    population = read_mean_artifact(repo, evaluation.population_ref, CandidateEvaluationSetV1,
                                   consumer_ns=evaluation.available_at_ns)
    snapshot = read_mean_artifact(repo, evaluation.account_snapshot_ref, OfflineRiskSnapshotV1,
                                 consumer_ns=evaluation.available_at_ns)
    receipt = MeanEvaluationValidationV1(evaluation.content_hash, manifest.content_hash,
        validation_implementation_ref(), evaluation.available_at_ns, _expires(repo, inp, population, snapshot))
    return _entry(receipt)


def validate_evaluation_registration(repo, entry):
    """OFFLINE only: independently reconstruct every material source-bound fact."""
    from atlas.v2.science.mean_calibration_validation import validation_implementation_ref

    if getattr(repo, "_mean_evaluation_writes", {}).get(entry.artifact_ref) == entry:
        return
    receipt = MeanEvaluationValidationV1.from_dict(entry.metadata["mean_artifact"])
    now = receipt.available_at_ns
    evaluation = read_mean_artifact(repo, receipt.evaluation_ref, ActionEvaluationV1, consumer_ns=now)
    manifest = read_mean_artifact(repo, receipt.inputs_ref, MeanEvaluationInputsV1, consumer_ns=now)
    population = read_mean_artifact(repo, evaluation.population_ref, CandidateEvaluationSetV1, consumer_ns=now)
    snapshot = read_mean_artifact(repo, population.account_snapshot_ref, OfflineRiskSnapshotV1, consumer_ns=now)
    policy = read_mean_artifact(repo, population.evaluation_policy_ref, MeanEvaluationPolicyV1, consumer_ns=now)
    values = tuple(_read_source(repo, ref, cls,
        now if cls in (CandidateActionV2, MeanContextV1) else population.cutoff_ns)
        for ref, cls in zip(manifest.input_refs, INPUT_TYPES, strict=True))
    inp = CandidateMeanInputsV1(*values)
    row = next((r for r in population.candidates if r.candidate_ref == inp.candidate.content_hash), None)
    if (row is None or row.missing_reasons
            or inp.candidate.candidate_id != row.candidate_id
            or inp.candidate.content_hash != evaluation.candidate_ref
            or inp.candidate.decision_at_ns != population.cutoff_ns
            or inp.candidate.deadline_ns != population.deadline_ns
            or inp.context.candidate_ref != inp.candidate.content_hash
            or inp.context.raw_source_available_at_ns > population.cutoff_ns
            or inp.candidate.snapshot_hash not in row.source_refs
            or snapshot.permission_ref != population.permission_ref
            or evaluation.account_snapshot_ref != snapshot.content_hash
            or not population.cutoff_ns <= now < population.deadline_ns):
        raise ValueError("EVALUATION_ORIGINAL_INPUT_BINDING_CHANGED")
    for ref in row.source_refs:
        source = repo.get_artifact(ref)
        if source is None or source.available_at_ns > population.cutoff_ns:
            raise ValueError("EVALUATION_ORIGINAL_SOURCE_UNAVAILABLE")
    universe = _read_source(repo, population.universe_ref, UniverseContractV2, population.cutoff_ns)
    manifest_entry = repo.get_artifact(population.instrument_revision_ref)
    if manifest_entry is None or manifest_entry.available_at_ns > population.cutoff_ns:
        raise ValueError("EVALUATION_INSTRUMENT_MANIFEST_UNAVAILABLE")
    body = _stored_body(manifest_entry)
    if (sha256_json(body) != population.instrument_revision_ref
            or body.get("universe_ref") != universe.content_hash
            or set(body.get("product_refs", ())) != {e.product_ref for e in universe.entries}
            or not any(e.key == inp.product.key and e.product_ref == inp.product.content_hash and e.scanner_eligible
                       for e in universe.entries)):
        raise ValueError("EVALUATION_ASOF_UNIVERSE_CHANGED")
    sizing = size_independent_candidate(candidate=inp.candidate, product=inp.product, fee=inp.fees,
        snapshot=snapshot, profile=inp.risk, cutoff_ns=population.cutoff_ns, now_ns=now)
    action = freeze_independent_action(candidate=inp.candidate, population_ref=population.content_hash,
        sizing=sizing, product=inp.product, policy=inp.policy, permission_ref=population.permission_ref,
        funding_ref=inp.funding.content_hash, execution_ref=inp.scenarios.execution_ref,
        model_ref=inp.model.content_hash,
        source_refs=tuple(sorted((*row.source_refs, inp.context.content_hash))),
        setup_parameters=inp.context.setup_parameters)
    saved = read_mean_artifact(repo, evaluation.action_ref, ExactActionV1, consumer_ns=now)
    expected = _evaluate_one(repo, action, inp, rank=row.initial_rank, policy=policy, snapshot=snapshot, now_ns=now)
    if (action != saved or expected != evaluation or evaluation.status != "ELIGIBLE"
            or receipt.implementation_ref != validation_implementation_ref()
            or receipt.expires_at_ns != _expires(repo, inp, population, snapshot)
            or receipt.available_at_ns != evaluation.available_at_ns
            or entry != _entry(receipt)):
        raise ValueError("SAVED_EVALUATION_NOT_DERIVED_FROM_CHECKED_INPUTS")


def require_evaluation_validation(repo, evaluation, *, now_ns):
    """B deadline: exact receipt lookup only, no replay, training or bootstrap."""
    from atlas.v2.science.mean_calibration_validation import validation_implementation_ref

    if repo._mean_integrity_startup_error:
        raise ValueError("EVALUATION_IMPLEMENTATION_UNAVAILABLE_AT_STARTUP")
    implementation = validation_implementation_ref()
    entry = repo.get_artifact(evaluation_validation_ref(evaluation.content_hash, implementation))
    if entry is None or entry.artifact_type != MeanEvaluationValidationV1.__name__:
        raise ValueError("SAVED_EVALUATION_CHECKED_PRODUCER_PROOF_UNAVAILABLE")
    receipt = MeanEvaluationValidationV1.from_dict(entry.metadata["mean_artifact"])
    if (entry != _entry(receipt) or receipt.evaluation_ref != evaluation.content_hash
            or receipt.implementation_ref != implementation
            or receipt.available_at_ns != evaluation.available_at_ns
            or not receipt.available_at_ns <= now_ns < receipt.expires_at_ns):
        raise ValueError("SAVED_EVALUATION_CHECKED_PROOF_CHANGED_OR_EXPIRED")
