"""Typed Session 019 evidence, portfolio ES and deterministic admission."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any

from atlas.domain.capability import capability_contract_from_manifest
from atlas.domain.enums import Side
from atlas.domain.money import canonical_decimal_str
from atlas.domain.risk import RiskPolicy
from atlas.science.stresses import (
    FROZEN_STRESSES,
    StressCollateralAssumptions,
    StressExecutionAssumptions,
    StressFundingAssumptions,
    StressInput,
    StressMarginAssumptions,
    StressName,
    StressStatus,
    evaluate_stress_suite,
    max_liquidation_cost,
    max_trade_stress_loss,
    suite_is_estimable,
)
from atlas.v2._serialization import canonical_json, decimal_value, json_value, sha256_json, sha256_ref, strict_fields
from atlas.v2.contracts import (
    TRADE_PLAN_VERSION_V2,
    ArtifactEnvelope,
    CandidateActionV2,
    CandidateSetV2,
    DecisionStatusV2,
    TradePlanEnvelopeV2,
    V2Side,
)
from atlas.v2.instruments import EnvironmentV2, InstrumentKeyV2, VenueV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.risk import AccountRiskSnapshotV2
from atlas.v2.science.action import ActionArtifactV2, FrozenActionV2
from atlas.v2.science.m0 import (
    M0_CONFIG_VERSION,
    M0_FEATURE_SCHEMA_VERSION,
    M0_MODEL_VERSION,
    M0_RESIDUAL_ARCHIVE_VERSION,
    M0OODV2,
    M0CalibrationV2,
    M0FeatureVectorV2,
    M0PredictionV2,
    M0SupportV2,
    validate_m0_fit_evidence,
)
from atlas.v2.science.outcomes import (
    AdmissionStateV2,
    DecisionCalendarEntryV2,
    DecisionSourceStageV2,
    SelectionStateV2,
    index_decision_calendar_entry,
)
from atlas.v2.science.scenario_engine import (
    JointExecutionDataV2,
    PretradeExecutionScenarioV2,
    PretradePathPayoffV2,
    ScenarioGenerationStatusV2,
    validate_pretrade_scenario_evidence,
)

EVALUATION_VERSION = "EVALUATION_ARTIFACT_V2_AMENDED_V2"
ADMISSION_POLICY_VERSION = "DETERMINISTIC_M0_ADMISSION_V2"
LCB_METHOD_VERSION = "M0_MEAN_VALUE_LCB_V1"
LCB_COMPONENTS = ("estimation_uncertainty", "execution_model_uncertainty", "numerical_error")
ZERO = Decimal(0)


@dataclass(frozen=True)
class ScenarioSupportUnitV2:
    """Historical episode provenance for one coherent scenario template."""

    source_episode_ref: str
    source_window_start_ns: int
    source_window_end_ns: int
    source_bundle_refs: tuple[str, ...]
    venue: VenueV2
    product_ref: str
    policy_compatibility_class: str
    action_compatibility_class: str
    execution_model_ref: str
    calibration_ref: str
    template_ref: str
    available_at_ns: int
    synthetic_fixture: bool = False

    def __post_init__(self) -> None:
        for name in ("source_episode_ref", "product_ref", "policy_compatibility_class",
                "action_compatibility_class", "execution_model_ref", "calibration_ref", "template_ref"):
            sha256_ref(getattr(self, name), field=name)
        if (type(self.source_window_start_ns) is not int or type(self.source_window_end_ns) is not int or
                type(self.available_at_ns) is not int or self.source_window_start_ns < 0 or
                self.source_window_end_ns <= self.source_window_start_ns or
                self.available_at_ns < self.source_window_end_ns):
            raise ValueError("scenario support unit chronology invalid")
        if self.source_bundle_refs != tuple(sorted(set(self.source_bundle_refs))) or not self.source_bundle_refs:
            raise ValueError("scenario support source bundle must be nonempty, sorted and unique")
        for ref in self.source_bundle_refs:
            sha256_ref(ref, field="source_bundle_ref")
        object.__setattr__(self, "venue", VenueV2(self.venue))
        if type(self.synthetic_fixture) is not bool:
            raise ValueError("scenario support fixture marker must be boolean")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": "SCENARIO_SUPPORT_UNIT_V2_V1", **{
            name: getattr(self, name) for name in self.__dataclass_fields__}})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ScenarioSupportUnitV2:
        fields = set(cls.__dataclass_fields__) | {"version"}
        d = dict(strict_fields(data, expected=fields, required=fields, name="ScenarioSupportUnitV2"))
        if d.pop("version") != "SCENARIO_SUPPORT_UNIT_V2_V1" or not isinstance(d["source_bundle_refs"], list):
            raise ValueError("unsupported scenario support unit wire")
        d["source_bundle_refs"] = tuple(d["source_bundle_refs"])
        return cls(**{name: d[name] for name in cls.__dataclass_fields__})


@dataclass(frozen=True)
class ScenarioSupportV2:
    action_hash: str
    scenario_ref: str
    independent_support_unit_count: int
    support_unit_refs: tuple[str, ...]
    independent_unit_refs: tuple[str, ...]
    policy_compatible: bool
    horizon_compatible: bool
    venue_product_compatible: bool
    execution_mode_compatible: bool
    depth_supported: bool
    evidence_quality: str
    template_refs: tuple[str, ...]

    def __post_init__(self) -> None:
        sha256_ref(self.action_hash, field="action_hash")
        sha256_ref(self.scenario_ref, field="scenario_ref")
        if type(self.independent_support_unit_count) is not int or self.independent_support_unit_count < 0:
            raise ValueError("scenario support count invalid")
        for name in ("support_unit_refs", "independent_unit_refs", "template_refs"):
            refs = getattr(self, name)
            if refs != tuple(sorted(set(refs))):
                raise ValueError(f"scenario support {name} must be sorted unique")
            for ref in refs:
                sha256_ref(ref, field=name)
        if self.independent_support_unit_count != len(self.independent_unit_refs):
            raise ValueError("independent support count must match exact selected support units")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": "PRETRADE_SCENARIO_SUPPORT_V2", **{name: getattr(self, name) for name in self.__dataclass_fields__}})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ScenarioSupportV2:
        fields = set(cls.__dataclass_fields__) | {"version"}
        d = dict(strict_fields(data, expected=fields, required=fields, name="ScenarioSupportV2"))
        if d.pop("version") != "PRETRADE_SCENARIO_SUPPORT_V2":
            raise ValueError("unsupported scenario support wire")
        for name in ("support_unit_refs", "independent_unit_refs", "template_refs"):
            if not isinstance(d[name], list):
                raise ValueError("scenario support ref arrays required")
            d[name] = tuple(d[name])
        return cls(**{name: d[name] for name in cls.__dataclass_fields__})


@dataclass(frozen=True)
class InferenceSupportV2:
    action_hash: str
    m0_support_ref: str
    scenario_support_ref: str
    eligible_m0_samples: int
    independent_m0_samples: int
    independent_scenario_support_units: int
    missing_feature_coverage: Decimal
    policy_compatible: bool
    horizon_compatible: bool
    venue_product_compatible: bool
    execution_mode_compatible: bool
    depth_supported: bool
    evidence_quality: str
    status: str

    def __post_init__(self) -> None:
        for name in ("action_hash", "m0_support_ref", "scenario_support_ref"):
            sha256_ref(getattr(self, name), field=name)
        if min(self.eligible_m0_samples, self.independent_m0_samples, self.independent_scenario_support_units) < 0:
            raise ValueError("inference support counts invalid")
        if not ZERO <= self.missing_feature_coverage <= Decimal(1):
            raise ValueError("feature missingness coverage must be a fraction")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": "COMPOSITE_M0_SCENARIO_SUPPORT_V2", **{name: getattr(self, name) for name in self.__dataclass_fields__}})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class OutcomeDistributionV2:
    action_hash: str
    scenario_ref: str
    payoff_refs: tuple[str, ...]
    path_ids: tuple[str, ...]
    probabilities: tuple[Decimal, ...]
    net_pnls: tuple[Decimal, ...]
    expected_net_pnl: Decimal | None
    downside_q05: Decimal | None
    status: str
    reason: str | None = None

    def __post_init__(self) -> None:
        sha256_ref(self.action_hash, field="action_hash")
        sha256_ref(self.scenario_ref, field="scenario_ref")
        n = len(self.payoff_refs)
        if any(len(x) != n for x in (self.path_ids, self.probabilities, self.net_pnls)):
            raise ValueError("outcome distribution rows must align")
        if self.status == "NOT_ESTIMABLE":
            if n or self.expected_net_pnl is not None or self.downside_q05 is not None or not self.reason:
                raise ValueError("unestimable outcome distribution cannot carry fabricated paths/value")
            return
        if self.status != "AVAILABLE" or not n or self.expected_net_pnl is None or self.downside_q05 is None or self.reason is not None:
            raise ValueError("available outcome distribution needs probability-weighted path values")
        if self.path_ids != tuple(sorted(set(self.path_ids))):
            raise ValueError("outcome distribution path IDs must be ordered and unique")
        for ref in self.payoff_refs + self.path_ids:
            sha256_ref(ref, field="distribution ref")
        if sum(self.probabilities, ZERO) != Decimal(1) or any(p <= 0 for p in self.probabilities):
            raise ValueError("outcome probability must sum exactly to one")
        if sum((p * pnl for p, pnl in zip(self.probabilities, self.net_pnls, strict=True)), ZERO) != self.expected_net_pnl:
            raise ValueError("outcome expected PnL does not reconcile")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": "PRETRADE_OUTCOME_DISTRIBUTION_V2", **{
            name: getattr(self, name) for name in self.__dataclass_fields__}})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> OutcomeDistributionV2:
        fields = set(cls.__dataclass_fields__) | {"version"}
        d = dict(strict_fields(data, expected=fields, required=fields, name="OutcomeDistributionV2"))
        if d.pop("version") != "PRETRADE_OUTCOME_DISTRIBUTION_V2":
            raise ValueError("unsupported outcome distribution wire")
        for name in ("expected_net_pnl", "downside_q05"):
            d[name] = decimal_value(d[name], field=name, wire=True) if d[name] is not None else None
        d["payoff_refs"] = tuple(d["payoff_refs"])
        d["path_ids"] = tuple(d["path_ids"])
        d["probabilities"] = tuple(decimal_value(x, field="probability", wire=True) for x in d["probabilities"])
        d["net_pnls"] = tuple(decimal_value(x, field="net_pnl", wire=True) for x in d["net_pnls"])
        return cls(**{name: d[name] for name in cls.__dataclass_fields__})


@dataclass(frozen=True)
class EstimationUncertaintyV2:
    action_hash: str
    prediction_ref: str
    oof_archive_ref: str
    training_outcome_refs: tuple[str, ...]
    standard_error: Decimal | None
    confidence_multiplier: Decimal
    uncertainty_amount: Decimal | None
    status: str

    def __post_init__(self) -> None:
        for name in ("action_hash", "prediction_ref", "oof_archive_ref"):
            sha256_ref(getattr(self, name), field=name)
        for ref in self.training_outcome_refs:
            sha256_ref(ref, field="training_outcome_ref")
        if len(self.training_outcome_refs) != len(set(self.training_outcome_refs)):
            raise ValueError("estimation outcome refs must be unique and chronologically ordered")
        if self.confidence_multiplier <= 0:
            raise ValueError("estimation confidence multiplier must be positive")
        if self.status == "AVAILABLE":
            if self.standard_error is None or self.uncertainty_amount is None or self.standard_error < 0 or self.uncertainty_amount < 0:
                raise ValueError("available estimation uncertainty needs nonnegative values")
        elif self.standard_error is not None or self.uncertainty_amount is not None:
            raise ValueError("unestimable uncertainty cannot carry fabricated values")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": "M0_ESTIMATION_UNCERTAINTY_V1", **{name: getattr(self, name) for name in self.__dataclass_fields__}})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class ExecutionCalibrationResidualV2:
    evidence_ref: str
    action_compatibility_key: str
    predicted_cost: Decimal
    realized_cost: Decimal
    decision_at_ns: int
    available_at_ns: int
    provenance: str = "UNVERIFIED"
    source_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        sha256_ref(self.evidence_ref, field="execution calibration evidence")
        sha256_ref(self.action_compatibility_key, field="compatibility key")
        for name in ("predicted_cost", "realized_cost"):
            value = decimal_value(getattr(self, name), field=name)
            if value < 0:
                raise ValueError("execution calibration costs must be nonnegative")
            object.__setattr__(self, name, value)
        if type(self.decision_at_ns) is not int or type(self.available_at_ns) is not int or min(self.decision_at_ns, self.available_at_ns) < 0:
            raise ValueError("execution calibration times invalid")
        if self.available_at_ns < self.decision_at_ns:
            raise ValueError("execution calibration chronology invalid")
        if self.provenance not in {"ACTUAL", "SIMULATED", "COUNTERFACTUAL", "UNVERIFIED"}:
            raise ValueError("execution calibration provenance invalid")
        if self.source_refs != tuple(sorted(set(self.source_refs))):
            raise ValueError("execution calibration source refs must be sorted and unique")
        for ref in self.source_refs:
            sha256_ref(ref, field="execution calibration source ref")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": "EXECUTION_CALIBRATION_RESIDUAL_V1",
            "action_compatibility_key": self.action_compatibility_key,
            "predicted_cost": self.predicted_cost, "realized_cost": self.realized_cost,
            "decision_at_ns": self.decision_at_ns, "available_at_ns": self.available_at_ns,
            "provenance": self.provenance, "source_refs": self.source_refs})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any], *, evidence_ref: str) -> ExecutionCalibrationResidualV2:
        fields = {"version", "action_compatibility_key", "predicted_cost", "realized_cost",
            "decision_at_ns", "available_at_ns", "provenance", "source_refs"}
        d = dict(strict_fields(data, expected=fields, required=fields, name="ExecutionCalibrationResidualV2"))
        if d.pop("version") != "EXECUTION_CALIBRATION_RESIDUAL_V1" or not isinstance(d["source_refs"], list):
            raise ValueError("unsupported execution calibration residual wire")
        return cls(evidence_ref, d["action_compatibility_key"],
            decimal_value(d["predicted_cost"], field="predicted_cost", wire=True),
            decimal_value(d["realized_cost"], field="realized_cost", wire=True),
            d["decision_at_ns"], d["available_at_ns"], d["provenance"], tuple(d["source_refs"]))


@dataclass(frozen=True)
class ExecutionModelUncertaintyV2:
    action_hash: str
    execution_model_ref: str
    residual_refs: tuple[str, ...]
    independent_support_count: int
    absolute_cost_error_q90: Decimal | None
    status: str

    def __post_init__(self) -> None:
        sha256_ref(self.action_hash, field="action_hash")
        sha256_ref(self.execution_model_ref, field="execution_model_ref")
        for ref in self.residual_refs:
            sha256_ref(ref, field="execution residual ref")
        if type(self.independent_support_count) is not int or self.independent_support_count < 0:
            raise ValueError("execution support count invalid")
        if self.status == "AVAILABLE" and (self.absolute_cost_error_q90 is None or self.absolute_cost_error_q90 < 0):
            raise ValueError("execution uncertainty unavailable")
        if self.status != "AVAILABLE" and self.absolute_cost_error_q90 is not None:
            raise ValueError("unestimable execution uncertainty cannot carry a value")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": "EXECUTION_MODEL_UNCERTAINTY_V1", **{name: getattr(self, name) for name in self.__dataclass_fields__}})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class NumericalConvergenceRunV2:
    action_hash: str
    scenario_ref: str
    template_manifest_ref: str
    execution_model_ref: str
    cost_model_ref: str
    seed: int
    path_count: int
    payoff_refs: tuple[str, ...]
    weighted_mean_estimate: Decimal
    computed_at_ns: int
    available_at_ns: int

    def __post_init__(self) -> None:
        for name in ("action_hash", "scenario_ref", "template_manifest_ref", "execution_model_ref", "cost_model_ref"):
            sha256_ref(getattr(self, name), field=name)
        if type(self.seed) is not int or self.seed < 0 or type(self.path_count) is not int or self.path_count <= 0:
            raise ValueError("numerical convergence run seed/path count invalid")
        if self.payoff_refs != tuple(sorted(set(self.payoff_refs))) or not self.payoff_refs:
            raise ValueError("numerical convergence payoff refs must be sorted unique and nonempty")
        for ref in self.payoff_refs:
            sha256_ref(ref, field="payoff_ref")
        object.__setattr__(self, "weighted_mean_estimate", decimal_value(self.weighted_mean_estimate,
            field="weighted_mean_estimate"))
        if type(self.computed_at_ns) is not int or type(self.available_at_ns) is not int or not (
                0 <= self.computed_at_ns <= self.available_at_ns):
            raise ValueError("numerical convergence run chronology invalid")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": "NUMERICAL_CONVERGENCE_RUN_V2_V1", **{name: getattr(self, name) for name in self.__dataclass_fields__}})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> NumericalConvergenceRunV2:
        fields = set(cls.__dataclass_fields__) | {"version"}
        d = dict(strict_fields(data, expected=fields, required=fields, name="NumericalConvergenceRunV2"))
        if d.pop("version") != "NUMERICAL_CONVERGENCE_RUN_V2_V1" or not isinstance(d["payoff_refs"], list):
            raise ValueError("unsupported numerical convergence run wire")
        d["payoff_refs"] = tuple(d["payoff_refs"])
        d["weighted_mean_estimate"] = decimal_value(d["weighted_mean_estimate"], field="weighted_mean_estimate", wire=True)
        return cls(**{name: d[name] for name in cls.__dataclass_fields__})


@dataclass(frozen=True)
class NumericalErrorV2:
    action_hash: str
    scenario_ref: str
    template_manifest_ref: str
    execution_model_ref: str
    cost_model_ref: str | None
    prediction_ref: str
    run_a_ref: str | None
    run_b_ref: str | None
    seed_a: int | None
    seed_b: int | None
    path_count_a: int | None
    path_count_b: int | None
    m0_conversion_error: Decimal | None
    estimate_a: Decimal | None
    estimate_b: Decimal | None
    error_bound: Decimal | None
    computed_at_ns: int
    available_at_ns: int
    status: str
    reason: str | None = None

    def __post_init__(self) -> None:
        for name in ("action_hash", "scenario_ref", "template_manifest_ref", "execution_model_ref", "prediction_ref"):
            sha256_ref(getattr(self, name), field=name)
        for name in ("cost_model_ref", "run_a_ref", "run_b_ref"):
            value = getattr(self, name)
            if value is not None:
                sha256_ref(value, field=name)
        for name in ("m0_conversion_error", "estimate_a", "estimate_b", "error_bound"):
            value = getattr(self, name)
            if value is not None:
                value = decimal_value(value, field=name)
                object.__setattr__(self, name, value)
        if type(self.computed_at_ns) is not int or type(self.available_at_ns) is not int or not (
                0 <= self.computed_at_ns <= self.available_at_ns):
            raise ValueError("numerical error chronology invalid")
        if self.status == "AVAILABLE":
            if (self.cost_model_ref is None or self.run_a_ref is None or self.run_b_ref is None or
                    self.seed_a is None or self.seed_b is None or self.seed_a == self.seed_b or
                    self.path_count_a is None or self.path_count_a <= 0 or
                    self.path_count_b is None or self.path_count_b <= 0 or
                    self.m0_conversion_error is None or self.m0_conversion_error < 0 or
                    self.estimate_a is None or self.estimate_b is None or self.error_bound is None or
                    self.error_bound < abs(self.estimate_a - self.estimate_b) + self.m0_conversion_error or
                    self.reason is not None):
                raise ValueError("available numerical error requires two reproducible independent runs")
        elif self.status == "NOT_ESTIMABLE":
            if (self.run_a_ref is not None or self.run_b_ref is not None or self.seed_a is not None or
                    self.seed_b is not None or self.path_count_a is not None or self.path_count_b is not None or
                    self.estimate_a is not None or self.estimate_b is not None or self.error_bound is not None or
                    not self.reason):
                raise ValueError("unestimable numerical error cannot carry fabricated convergence runs")
        else:
            raise ValueError("unknown numerical-error status")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": "PRETRADE_NUMERICAL_ERROR_V2", **{
            name: getattr(self, name) for name in self.__dataclass_fields__}})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> NumericalErrorV2:
        fields = set(cls.__dataclass_fields__) | {"version"}
        d = dict(strict_fields(data, expected=fields, required=fields, name="NumericalErrorV2"))
        if d.pop("version") != "PRETRADE_NUMERICAL_ERROR_V2":
            raise ValueError("unsupported numerical error wire")
        for name in ("m0_conversion_error", "estimate_a", "estimate_b", "error_bound"):
            d[name] = decimal_value(d[name], field=name, wire=True) if d[name] is not None else None
        return cls(**{name: d[name] for name in cls.__dataclass_fields__})


def _pretrade_template_manifest_ref(scenario: PretradeExecutionScenarioV2) -> str:
    inputs = (scenario.model_input, scenario.calibration_input,
        scenario.execution_model_input, *scenario.source_inputs)
    return sha256_json({"version": "PRETRADE_EXECUTION_CAUSAL_MANIFEST_V1",
        "inputs": [item.to_dict() for item in inputs],
        "data_refs": sorted(scenario.source_joint_data_refs),
        "generator": scenario.generation_version})


def _reproduce_numerical_convergence_run(repo: OpsRepository, *, action: ActionArtifactV2,
        scenario_ref: str) -> NumericalConvergenceRunV2:
    entry = repo.get_artifact(scenario_ref)
    body = entry.metadata.get("scenario") if entry is not None else None
    scenario = PretradeExecutionScenarioV2.from_dict(json_value(body)) if isinstance(body, Mapping) else None
    if (entry is None or entry.artifact_type != "PretradeExecutionScenarioV2" or scenario is None or
            scenario.content_hash != scenario_ref or
            scenario.status != ScenarioGenerationStatusV2.AVAILABLE):
        raise ValueError("numerical convergence requires indexed real-support pretrade scenario runs")
    payoffs = validate_pretrade_scenario_evidence(repo, action=action, scenario=scenario)
    payoff_by_path = {item.joint_path_id: item for item in payoffs}
    if set(payoff_by_path) != {path_id for path_id, _, _ in scenario.rows}:
        raise ValueError("numerical run has incomplete exact path-payoff evidence")
    run_metadata = entry.metadata
    seed, path_count = run_metadata.get("seed"), run_metadata.get("scenario_count")
    if type(seed) is not int or type(path_count) is not int:
        raise ValueError("numerical run scenario seed/path count missing")
    mean = sum((probability * payoff_by_path[path_id].net_payoff
        for path_id, probability, _ in scenario.rows), ZERO)
    data_entry = repo.get_artifact(scenario.template_support_refs[0])
    data_body = data_entry.metadata.get("joint_execution_data") if data_entry is not None else None
    data = JointExecutionDataV2.from_dict(json_value(data_body)) if isinstance(data_body, Mapping) else None
    template_data = []
    for ref in scenario.template_support_refs:
        template_entry = repo.get_artifact(ref)
        template_body = template_entry.metadata.get("joint_execution_data") if template_entry is not None else None
        if not isinstance(template_body, Mapping):
            raise ValueError("numerical run template evidence missing")
        template_data.append(JointExecutionDataV2.from_dict(json_value(template_body)))
    if data is None or any(item.fee_ref != data.fee_ref for item in template_data):
        raise ValueError("numerical run cost model is not common across its coherent template set")
    return NumericalConvergenceRunV2(action.action.action_hash, scenario.content_hash,
        _pretrade_template_manifest_ref(scenario), scenario.execution_model_input.ref, data.fee_ref,
        seed, path_count, tuple(sorted(item.content_hash for item in payoffs)), mean,
        scenario.computed_at_ns, scenario.available_at_ns)


def index_numerical_convergence_run(repo: OpsRepository, *, action: ActionArtifactV2,
        scenario_ref: str) -> str:
    run = _reproduce_numerical_convergence_run(repo, action=action, scenario_ref=scenario_ref)
    repo.register_artifact(ArtifactIndexEntryV2(run.content_hash, "NumericalConvergenceRunV2",
        run.content_hash, run.computed_at_ns, run.available_at_ns, {"evidence": run.to_dict()}))
    return run.content_hash


def _lookup_numerical_convergence_run(repo: OpsRepository, *, action: ActionArtifactV2,
        run_ref: str, primary_scenario: PretradeExecutionScenarioV2) -> NumericalConvergenceRunV2:
    entry = repo.get_artifact(run_ref)
    body = entry.metadata.get("evidence") if entry is not None else None
    run = NumericalConvergenceRunV2.from_dict(json_value(body)) if isinstance(body, Mapping) else None
    if (entry is None or entry.artifact_type != "NumericalConvergenceRunV2" or run is None or
            entry.content_hash != run_ref or run.content_hash != run_ref or
            entry.created_at_ns != run.computed_at_ns or entry.available_at_ns != run.available_at_ns):
        raise ValueError("indexed typed numerical convergence run required")
    reproduced = _reproduce_numerical_convergence_run(repo, action=action, scenario_ref=run.scenario_ref)
    run_scenario_entry = repo.get_artifact(run.scenario_ref)
    run_scenario_body = run_scenario_entry.metadata.get("scenario") if run_scenario_entry is not None else None
    run_scenario = PretradeExecutionScenarioV2.from_dict(json_value(run_scenario_body)) if isinstance(run_scenario_body, Mapping) else None
    if (canonical_json(reproduced.to_dict()) != canonical_json(run.to_dict()) or run_scenario is None or
            run_scenario.common_scenario_set_id != primary_scenario.common_scenario_set_id or
            run.template_manifest_ref != _pretrade_template_manifest_ref(primary_scenario) or
            run_scenario.template_support_refs != primary_scenario.template_support_refs or
            run_scenario.source_joint_data_refs != primary_scenario.source_joint_data_refs or
            run_scenario.execution_model_input != primary_scenario.execution_model_input or
            run_scenario.calibration_input != primary_scenario.calibration_input or
            run_scenario.source_inputs != primary_scenario.source_inputs or
            run_scenario.action_hash != primary_scenario.action_hash or
            run_scenario.action_artifact_ref != primary_scenario.action_artifact_ref):
        raise ValueError("numerical run does not reproduce from the exact causal template manifest")
    return run


def make_numerical_error(repo: OpsRepository, *, action: ActionArtifactV2,
        scenario: PretradeExecutionScenarioV2, prediction: M0PredictionV2,
        run_a_ref: str | None = None, run_b_ref: str | None = None) -> NumericalErrorV2:
    if (scenario.action_hash != action.action.action_hash or
            prediction.action_hash != action.action.action_hash or
            prediction.action_artifact_ref != action.content_hash):
        raise ValueError("numerical convergence action/prediction identity mismatch")
    if run_a_ref is None or run_b_ref is None:
        if run_a_ref is not None or run_b_ref is not None:
            raise ValueError("both independent indexed convergence runs are required")
        return NumericalErrorV2(action.action.action_hash, scenario.content_hash,
            _pretrade_template_manifest_ref(scenario), scenario.execution_model_input.ref,
            None, prediction.content_hash, None, None, None, None, None, None,
            prediction.numerical_conversion_error, None, None, None,
            scenario.computed_at_ns, scenario.available_at_ns, "NOT_ESTIMABLE",
            "INDEPENDENT_CONVERGENCE_RUNS_MISSING")
    if run_a_ref == run_b_ref:
        raise ValueError("numerical convergence runs require distinct immutable refs")
    run_a = _lookup_numerical_convergence_run(repo, action=action, run_ref=run_a_ref,
        primary_scenario=scenario)
    run_b = _lookup_numerical_convergence_run(repo, action=action, run_ref=run_b_ref,
        primary_scenario=scenario)
    if run_a.seed == run_b.seed:
        raise ValueError("numerical convergence seeds must be independent")
    if run_a.cost_model_ref != run_b.cost_model_ref or run_a.execution_model_ref != run_b.execution_model_ref:
        raise ValueError("numerical convergence runs use different execution/cost models")
    conversion = prediction.numerical_conversion_error
    if conversion is None:
        raise ValueError("M0 float-to-Decimal conversion error is unavailable")
    return NumericalErrorV2(action.action.action_hash, scenario.content_hash,
        _pretrade_template_manifest_ref(scenario), scenario.execution_model_input.ref,
        run_a.cost_model_ref, prediction.content_hash, run_a_ref, run_b_ref,
        run_a.seed, run_b.seed, run_a.path_count, run_b.path_count, conversion,
        run_a.weighted_mean_estimate, run_b.weighted_mean_estimate,
        abs(run_a.weighted_mean_estimate - run_b.weighted_mean_estimate) + conversion,
        max(run_a.computed_at_ns, run_b.computed_at_ns),
        max(run_a.available_at_ns, run_b.available_at_ns), "AVAILABLE")


@dataclass(frozen=True)
class DeterministicStressV2:
    action_hash: str
    action_artifact_ref: str
    quantity: Decimal
    risk_policy_ref: str
    risk_policy_hash: str
    eligible_equity: Decimal | None
    drawdown: Decimal | None
    loss_limit: Decimal | None
    cutoff_ns: int
    stress_version: str
    stress_evidence_ref: str | None
    required_case_refs: tuple[str, ...]
    maximum_loss: Decimal | None
    breach: bool | None
    status: str
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        for name in ("action_hash", "action_artifact_ref", "risk_policy_ref", "risk_policy_hash"):
            sha256_ref(getattr(self, name), field=name)
        if self.stress_evidence_ref is not None:
            sha256_ref(self.stress_evidence_ref, field="stress evidence")
        for name in ("eligible_equity", "drawdown", "loss_limit", "maximum_loss"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, decimal_value(value, field=name))
        if self.eligible_equity is not None and self.eligible_equity <= 0:
            raise ValueError("stress eligible equity must be positive")
        if self.drawdown is not None and not ZERO <= self.drawdown <= Decimal(1):
            raise ValueError("stress drawdown must be a fraction")
        if self.loss_limit is not None and self.loss_limit < 0:
            raise ValueError("stress limit cannot be negative")
        if self.required_case_refs != tuple(sorted(set(self.required_case_refs))):
            raise ValueError("stress case refs must be sorted and unique")
        for ref in self.required_case_refs:
            sha256_ref(ref, field="stress case")
        if self.status == "AVAILABLE" and (self.maximum_loss is None or self.maximum_loss < 0 or self.breach is None or
                self.stress_evidence_ref is None or self.eligible_equity is None or self.drawdown is None or
                self.loss_limit is None):
            raise ValueError("available stress needs complete exact-action evidence")
        if self.status != "AVAILABLE" and (self.maximum_loss is not None or self.breach is not None):
            raise ValueError("unestimable stress has no computed loss")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": "V2_DETERMINISTIC_STRESS_RESULT_V1", **{name: getattr(self, name) for name in self.__dataclass_fields__}})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def stress_input_wire(state: StressInput) -> dict[str, Any]:
    return json_value({"version": "V2_DETERMINISTIC_STRESS_INPUT_V1", **asdict(state)})


def stress_input_from_wire(data: Mapping[str, Any]) -> StressInput:
    fields = set(StressInput.__dataclass_fields__) | {"version"}
    d = dict(strict_fields(data, expected=fields, required=fields, name="V2StressInput"))
    if d.pop("version") != "V2_DETERMINISTIC_STRESS_INPUT_V1":
        raise ValueError("unsupported deterministic stress input version")
    execution = dict(strict_fields(d["execution"], expected=set(StressExecutionAssumptions.__dataclass_fields__),
        required=set(StressExecutionAssumptions.__dataclass_fields__), name="StressExecutionAssumptions"))
    vectors = {"stressed_executable_prices", "stressed_available_depth", "exit_delay_path_prices"}
    for name, values in execution.items():
        if not isinstance(values, Mapping):
            raise ValueError("stress execution maps required")
        if name == "paired_instrument_paths":
            execution[name] = {StressName(k): {instrument: tuple(decimal_value(v, field=name, wire=True)
                for v in path) for instrument, path in paths.items()} for k, paths in values.items()}
        elif name in vectors:
            execution[name] = {StressName(k): tuple(decimal_value(v, field=name, wire=True) for v in path)
                for k, path in values.items()}
        else:
            execution[name] = {StressName(k): decimal_value(v, field=name, wire=True) for k, v in values.items()}
    margin = dict(strict_fields(d["margin"], expected=set(StressMarginAssumptions.__dataclass_fields__),
        required=set(StressMarginAssumptions.__dataclass_fields__), name="StressMarginAssumptions"))
    for name, values in margin.items():
        if not isinstance(values, Mapping):
            raise ValueError("stress margin maps required")
        if name == "maintenance_margin_tiers":
            margin[name] = {StressName(k): tuple(decimal_value(v, field=name, wire=True) for v in path)
                for k, path in values.items()}
        elif name == "liquidation_mechanics":
            margin[name] = {StressName(k): v for k, v in values.items()}
        else:
            margin[name] = {StressName(k): decimal_value(v, field=name, wire=True) for k, v in values.items()}
    funding = strict_fields(d["funding"], expected=set(StressFundingAssumptions.__dataclass_fields__),
        required=set(StressFundingAssumptions.__dataclass_fields__), name="StressFundingAssumptions")
    collateral = strict_fields(d["collateral"], expected=set(StressCollateralAssumptions.__dataclass_fields__),
        required=set(StressCollateralAssumptions.__dataclass_fields__), name="StressCollateralAssumptions")
    state = StressInput(Side(d["side"]), decimal_value(d["quantity"], field="quantity", wire=True),
        decimal_value(d["current_mark"], field="current_mark", wire=True), d["shock_at_ns"],
        StressExecutionAssumptions(**execution), StressMarginAssumptions(**margin),
        StressFundingAssumptions(**{name: decimal_value(value, field=name, wire=True)
            for name, value in funding.items()}),
        StressCollateralAssumptions(decimal_value(collateral["venue_collateral"],
            field="venue_collateral", wire=True), collateral["valuation_currency"]))
    if canonical_json(stress_input_wire(state)) != canonical_json(data):
        raise ValueError("stress input is not canonical")
    return state


def index_stress_input(repo: OpsRepository, state: StressInput, *, source_refs: tuple[str, ...],
        cutoff_ns: int, available_at_ns: int) -> str:
    if not source_refs or source_refs != tuple(sorted(set(source_refs))) or available_at_ns > cutoff_ns:
        raise ValueError("stress input needs cutoff-known sorted causal sources")
    for ref in source_refs:
        source = repo.get_artifact(ref)
        if source is None or source.content_hash != ref or source.available_at_ns > available_at_ns:
            raise ValueError("stress source unavailable by input vintage")
    body = {"version": "V2_STRESS_SUITE_EVIDENCE_V1", "stress_input": stress_input_wire(state),
        "source_refs": list(source_refs), "information_cutoff_ns": cutoff_ns}
    ref = sha256_json(body)
    repo.register_artifact(ArtifactIndexEntryV2(ref, "StressSuiteEvidenceV2", ref,
        available_at_ns, available_at_ns, body))
    return ref


def evaluate_deterministic_stress(repo: OpsRepository, *, action: ActionArtifactV2,
        risk_policy: RiskPolicy, risk_policy_ref: str, eligible_equity: Decimal,
        drawdown: Decimal | None, product_base_units: Decimal, stress_input: StressInput | None,
        stress_evidence_ref: str | None, cutoff_ns: int) -> DeterministicStressV2:
    required_refs = tuple(sorted(sha256_json({"version": "FROZEN_STRESS_CASE_REF_V1",
        "suite_version": "V1_EVIDENCE_VALUED_STRESS_SUITE_V1", "case": case.name.value})
        for case in FROZEN_STRESSES))
    evidence = repo.get_artifact(stress_evidence_ref) if stress_evidence_ref is not None else None
    source_refs = evidence.metadata.get("source_refs") if evidence is not None else None
    sources_valid = (evidence is not None and isinstance(source_refs, (list, tuple)) and bool(source_refs) and
        tuple(source_refs) == tuple(sorted(set(source_refs))) and
        all(isinstance(ref, str) and (source := repo.get_artifact(ref)) is not None and
            source.content_hash == ref and source.available_at_ns <= evidence.available_at_ns and
            source.created_at_ns <= evidence.available_at_ns for ref in source_refs))
    policy_available = repo.get_artifact(risk_policy_ref)
    product_entry = repo.get_artifact(action.action.product_ref)
    product_body = product_entry.metadata.get("product") if product_entry is not None else None
    loss_limit = (eligible_equity * risk_policy.stress_loss_per_trade_frac * risk_policy.scaling_at(drawdown)
        if eligible_equity > 0 and drawdown is not None and ZERO <= drawdown <= Decimal(1) else None)
    if (stress_input is None or stress_evidence_ref is None or evidence is None or
            evidence.available_at_ns > cutoff_ns or evidence.content_hash != stress_evidence_ref or
            evidence.artifact_type != "StressSuiteEvidenceV2" or sha256_json(evidence.metadata) != stress_evidence_ref or
            set(evidence.metadata) != {"version", "stress_input", "source_refs", "information_cutoff_ns"} or
            not sources_valid or
            evidence.metadata.get("version") != "V2_STRESS_SUITE_EVIDENCE_V1" or
            evidence.metadata.get("information_cutoff_ns") != cutoff_ns or
            canonical_json(evidence.metadata.get("stress_input")) != canonical_json(stress_input_wire(stress_input)) or
            evidence.created_at_ns > cutoff_ns or policy_available is None or
            policy_available.artifact_type != "RiskPolicyV1" or policy_available.content_hash != risk_policy_ref or
            policy_available.available_at_ns > cutoff_ns or product_entry is None or
            product_entry.artifact_type != "ProductContractV2" or not isinstance(product_body, Mapping) or
            product_entry.content_hash != action.action.product_ref or
            product_body.get("base_units_per_contract") != canonical_decimal_str(product_base_units) or
            action.action.risk_policy_hash != risk_policy_ref or eligible_equity <= 0 or drawdown is None or
            not ZERO <= drawdown <= Decimal(1) or
            stress_input.quantity != action.action.quantity * product_base_units or
            stress_input.side.value != action.action.side or product_base_units <= 0):
        return DeterministicStressV2(action.action.action_hash, action.content_hash,
            action.action.quantity, risk_policy_ref, risk_policy_ref, eligible_equity, drawdown,
            loss_limit, cutoff_ns,
            "V1_EVIDENCE_VALUED_STRESS_SUITE_V1", stress_evidence_ref, required_refs,
            None, None, "NOT_ESTIMABLE", ("STRESS_EVIDENCE_OR_ACTION_UNITS_MISSING",))
    results = evaluate_stress_suite(stress_input)
    if not suite_is_estimable(results):
        reasons = tuple(sorted({result.reason or "REQUIRED_STRESS_UNESTIMABLE" for result in results
                               if result.status == StressStatus.NOT_ESTIMABLE}))
        return DeterministicStressV2(action.action.action_hash, action.content_hash,
            action.action.quantity, risk_policy_ref, risk_policy_ref, eligible_equity, drawdown,
            loss_limit, cutoff_ns,
            "V1_EVIDENCE_VALUED_STRESS_SUITE_V1", stress_evidence_ref, required_refs,
            None, None, "NOT_ESTIMABLE", reasons)
    maximum = max_trade_stress_loss(results) + max_liquidation_cost(results)
    assert loss_limit is not None
    limit = loss_limit
    breach = maximum > limit
    return DeterministicStressV2(action.action.action_hash, action.content_hash,
        action.action.quantity, risk_policy_ref, risk_policy_ref, eligible_equity, drawdown, limit, cutoff_ns,
        "V1_EVIDENCE_VALUED_STRESS_SUITE_V1", stress_evidence_ref, required_refs,
        maximum, breach, "AVAILABLE", ("SUPPORTED_STRESS_LIMIT_BREACH",) if breach else ())


@dataclass(frozen=True)
class PortfolioPathV2:
    common_path_id: str
    probability: Decimal
    existing_net_pnl: Decimal
    candidate_net_pnl: Decimal
    candidate_payoff_ref: str
    candidate_exit_cash_retained_to_24h: bool
    existing_valuation_ref: str | None = None

    def __post_init__(self) -> None:
        sha256_ref(self.common_path_id, field="common_path_id")
        sha256_ref(self.candidate_payoff_ref, field="candidate_payoff_ref")
        if self.existing_valuation_ref is not None:
            sha256_ref(self.existing_valuation_ref, field="existing_valuation_ref")
        for name in ("probability", "existing_net_pnl", "candidate_net_pnl"):
            object.__setattr__(self, name, decimal_value(getattr(self, name), field=name))
        if self.probability <= 0 or not self.candidate_exit_cash_retained_to_24h:
            raise ValueError("common-horizon candidate path invalid")

    def to_dict(self) -> dict[str, Any]:
        return json_value({name: getattr(self, name) for name in self.__dataclass_fields__})


@dataclass(frozen=True)
class ExistingPortfolioPathV2:
    common_path_id: str
    common_scenario_set_id: str
    probability: Decimal
    existing_net_pnl: Decimal
    information_cutoff_ns: int | None = None
    horizon_end_ns: int | None = None
    available_at_ns: int | None = None
    exposure_refs: tuple[str, ...] = ()
    source_refs: tuple[str, ...] = ()
    model_ref: str | None = None
    calibration_ref: str | None = None

    def __post_init__(self) -> None:
        sha256_ref(self.common_path_id, field="common_path_id")
        sha256_ref(self.common_scenario_set_id, field="common_scenario_set_id")
        for name in ("probability", "existing_net_pnl"):
            object.__setattr__(self, name, decimal_value(getattr(self, name), field=name))
        if self.probability <= 0:
            raise ValueError("existing portfolio path probability invalid")
        for refs in (self.exposure_refs, self.source_refs):
            if refs != tuple(sorted(set(refs))):
                raise ValueError("existing valuation refs must be sorted and unique")
            for ref in refs:
                sha256_ref(ref, field="existing valuation ref")
        for model_ref in (self.model_ref, self.calibration_ref):
            if model_ref is not None:
                sha256_ref(model_ref, field="existing valuation model/calibration ref")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": "DECISION_TIME_EXISTING_PORTFOLIO_PATH_V1",
            **{name: getattr(self, name) for name in self.__dataclass_fields__}})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ExistingPortfolioPathV2:
        fields = set(cls.__dataclass_fields__) | {"version"}
        d = dict(strict_fields(data, expected=fields, required=fields, name="ExistingPortfolioPathV2"))
        if d.pop("version") != "DECISION_TIME_EXISTING_PORTFOLIO_PATH_V1":
            raise ValueError("unsupported existing portfolio path version")
        for name in ("exposure_refs", "source_refs"):
            if not isinstance(d[name], list):
                raise ValueError("existing valuation reference arrays required")
            d[name] = tuple(d[name])
        for name in ("probability", "existing_net_pnl"):
            d[name] = decimal_value(d[name], field=name, wire=True)
        return cls(**d)


def index_existing_portfolio_path(repo: OpsRepository, path: ExistingPortfolioPathV2) -> str:
    if (type(path.information_cutoff_ns) is not int or type(path.horizon_end_ns) is not int or
            type(path.available_at_ns) is not int or path.available_at_ns < path.information_cutoff_ns or
            path.horizon_end_ns - path.information_cutoff_ns != 24 * 3_600_000_000_000 or
            not path.exposure_refs or not path.source_refs or path.model_ref is None or path.calibration_ref is None):
        raise ValueError("existing portfolio valuation lacks cutoff/common-horizon/model/source evidence")
    for ref in (*path.exposure_refs, *path.source_refs, path.model_ref, path.calibration_ref):
        entry = repo.get_artifact(ref)
        if (entry is None or entry.content_hash != ref or entry.available_at_ns > path.information_cutoff_ns or
                entry.created_at_ns > path.information_cutoff_ns or
                entry.artifact_type in {"PairedPortfolioPayoffV2", "ReplayPathV2", "PolicyPayoffV2"}):
            raise ValueError("existing valuation has unavailable or retrospective inputs")
    ref = index_admission_evidence(repo, "ExistingPortfolioPathV2", path.to_dict(), path.available_at_ns)
    return ref


@dataclass(frozen=True)
class DecisionTimePortfolioCompletenessV2:
    """Cutoff-bound exposure inventory and synchronized valuation coverage."""

    account_snapshot_ref: str
    common_scenario_set_id: str
    existing_exposure_refs: tuple[str, ...]
    pending_exposure_refs: tuple[str, ...]
    unknown_exposure_refs: tuple[str, ...]
    common_path_ids: tuple[str, ...]
    eligible_equity: Decimal
    drawdown: Decimal
    available_at_ns: int
    status: str

    def __post_init__(self) -> None:
        for name in ("account_snapshot_ref", "common_scenario_set_id"):
            sha256_ref(getattr(self, name), field=name)
        for name in ("existing_exposure_refs", "pending_exposure_refs", "unknown_exposure_refs", "common_path_ids"):
            refs = getattr(self, name)
            if refs != tuple(sorted(set(refs))):
                raise ValueError(f"portfolio {name} must be sorted and unique")
            for ref in refs:
                sha256_ref(ref, field=name)
        for name in ("eligible_equity", "drawdown"):
            value = decimal_value(getattr(self, name), field=name)
            object.__setattr__(self, name, value)
        if self.eligible_equity <= 0 or not ZERO <= self.drawdown <= Decimal(1):
            raise ValueError("portfolio completeness equity/drawdown invalid")
        if type(self.available_at_ns) is not int or self.available_at_ns < 0:
            raise ValueError("portfolio completeness availability invalid")
        if self.status not in {"COMPLETE", "NOT_ESTIMABLE"}:
            raise ValueError("portfolio completeness status invalid")
        if self.status == "COMPLETE" and (self.unknown_exposure_refs or not self.common_path_ids):
            raise ValueError("complete portfolio inventory requires known exposures and path coverage")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": "DECISION_TIME_PORTFOLIO_COMPLETENESS_V1",
            **{name: getattr(self, name) for name in self.__dataclass_fields__}})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class DecisionTimePortfolioScenariosV2:
    action_hash: str
    action_artifact_ref: str
    candidate_scenario_ref: str
    common_scenario_set_id: str
    account_snapshot_ref: str
    completeness_ref: str
    existing_exposure_refs: tuple[str, ...]
    pending_exposure_refs: tuple[str, ...]
    unknown_exposure_present: bool
    horizon_start_ns: int
    horizon_end_ns: int
    equity: Decimal
    drawdown: Decimal
    paths: tuple[PortfolioPathV2, ...]
    status: str
    reason: str | None = None

    def __post_init__(self) -> None:
        for name in ("action_hash", "action_artifact_ref", "candidate_scenario_ref", "common_scenario_set_id",
                     "account_snapshot_ref", "completeness_ref"):
            sha256_ref(getattr(self, name), field=name)
        for refs in (self.existing_exposure_refs, self.pending_exposure_refs):
            if refs != tuple(sorted(set(refs))):
                raise ValueError("portfolio exposure refs must be sorted unique")
            for ref in refs:
                sha256_ref(ref, field="portfolio exposure ref")
        object.__setattr__(self, "equity", decimal_value(self.equity, field="equity"))
        object.__setattr__(self, "drawdown", decimal_value(self.drawdown, field="drawdown"))
        if self.horizon_end_ns - self.horizon_start_ns != 24 * 3_600_000_000_000:
            raise ValueError("portfolio ES horizon must be common 24 hours")
        if self.equity <= 0 or not ZERO <= self.drawdown <= Decimal(1):
            raise ValueError("portfolio ES requires positive eligible equity")
        if self.status == "AVAILABLE":
            if self.unknown_exposure_present or not self.paths or self.reason is not None:
                raise ValueError("available portfolio scenario requires complete known exposures")
            if sum((p.probability for p in self.paths), ZERO) != Decimal(1):
                raise ValueError("portfolio path probabilities must sum to one")
            if any(not p.candidate_exit_cash_retained_to_24h for p in self.paths):
                raise ValueError("early candidate exit cash must remain held to common horizon")
            if tuple(p.common_path_id for p in self.paths) != tuple(sorted({p.common_path_id for p in self.paths})):
                raise ValueError("portfolio common path IDs must be sorted and unique")
            if any(p.probability <= 0 for p in self.paths):
                raise ValueError("portfolio probabilities must be positive")
        elif self.status != "NOT_ESTIMABLE" or self.paths or not self.reason:
            raise ValueError("unestimable portfolio evidence requires reason and no fabricated paths")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": "DECISION_TIME_PORTFOLIO_SCENARIOS_V2", **{name: getattr(self, name) for name in self.__dataclass_fields__}})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def build_decision_time_portfolio_scenarios(*, action: ActionArtifactV2,
        repo: OpsRepository,
        scenario: PretradeExecutionScenarioV2, payoffs: Sequence[PretradePathPayoffV2],
        existing_paths: Sequence[ExistingPortfolioPathV2],
        completeness: DecisionTimePortfolioCompletenessV2) -> DecisionTimePortfolioScenariosV2:
    horizon_start_ns = scenario.information_cutoff_ns
    account_snapshot_ref = completeness.account_snapshot_ref
    completeness_ref = completeness.content_hash
    existing_exposure_refs = completeness.existing_exposure_refs
    pending_exposure_refs = completeness.pending_exposure_refs
    unknown_exposure_present = bool(completeness.unknown_exposure_refs)
    equity = completeness.eligible_equity
    drawdown = completeness.drawdown
    if (scenario.action_hash != action.action.action_hash or scenario.action_artifact_ref != action.content_hash or
            scenario.status != ScenarioGenerationStatusV2.AVAILABLE or scenario.synthetic_fixture):
        return DecisionTimePortfolioScenariosV2(action.action.action_hash, action.content_hash,
            scenario.content_hash, scenario.common_scenario_set_id, account_snapshot_ref, completeness_ref,
            tuple(sorted(set(existing_exposure_refs))), tuple(sorted(set(pending_exposure_refs))),
            unknown_exposure_present, horizon_start_ns, horizon_start_ns + 24 * 3_600_000_000_000,
            equity, drawdown, (), "NOT_ESTIMABLE", "CANDIDATE_JOINT_PATHS_UNAVAILABLE")
    scenario_ids = tuple(path_id for path_id, _, _ in scenario.rows)
    existing_ids = tuple(path.common_path_id for path in existing_paths)
    account_entry = repo.get_artifact(account_snapshot_ref)
    account_body = account_entry.metadata if account_entry is not None else None
    completeness_entry = repo.get_artifact(completeness_ref)
    completeness_body = completeness_entry.metadata.get("evidence") if completeness_entry is not None else None
    account = None
    if isinstance(account_body, Mapping):
        try:
            body = dict(strict_fields(account_body, expected={"version", "account_scope", "available_at_ns",
                "eligible_equity", "margin_available", "current_margin", "drawdown", "existing_open_normal_loss",
                "pending_reserved_normal_loss", "gross_notional", "instrument_notional", "beta_notional",
                "venue_collateral", "existing_portfolio_es", "opening_intents", "closed_outcome_refs",
                "pending_risk_refs", "existing_exposure_refs", "exposure_completeness_ref", "operational_status"},
                required={"version", "account_scope", "available_at_ns", "eligible_equity", "margin_available",
                "current_margin", "drawdown", "existing_open_normal_loss", "pending_reserved_normal_loss",
                "gross_notional", "instrument_notional", "beta_notional", "venue_collateral",
                "existing_portfolio_es", "opening_intents", "closed_outcome_refs", "pending_risk_refs",
                "existing_exposure_refs", "exposure_completeness_ref", "operational_status"},
                name="AccountRiskSnapshotV2"))
            if body.pop("version") != "SHADOW_ACCOUNT_RISK_SNAPSHOT_V1":
                raise ValueError("unsupported account risk snapshot")
            for name in ("eligible_equity", "margin_available", "current_margin", "drawdown",
                         "existing_open_normal_loss", "pending_reserved_normal_loss", "gross_notional",
                         "instrument_notional", "beta_notional", "venue_collateral", "existing_portfolio_es"):
                body[name] = decimal_value(body[name], field=name, wire=True)
            body["closed_outcome_refs"] = tuple(body["closed_outcome_refs"])
            body["pending_risk_refs"] = tuple(body["pending_risk_refs"])
            body["existing_exposure_refs"] = tuple(body["existing_exposure_refs"])
            account = AccountRiskSnapshotV2(**body)
        except (TypeError, ValueError):
            account = None
    account_matches = (account is not None and account_entry is not None and
        account_entry.artifact_type == "AccountRiskSnapshotV2" and account.content_hash == account_snapshot_ref and
        account.available_at_ns <= horizon_start_ns and account.operational_status == "CURRENT" and
        account.existing_exposure_refs == completeness.existing_exposure_refs and
        account.pending_risk_refs == completeness.pending_exposure_refs and
        account.eligible_equity == completeness.eligible_equity and account.drawdown == completeness.drawdown)
    completeness_matches = (completeness_entry is not None and
        completeness_entry.artifact_type == "DecisionTimePortfolioCompletenessV2" and
        completeness_entry.content_hash == completeness_ref and isinstance(completeness_body, Mapping) and
        sha256_json(completeness_body) == completeness_ref and
        canonical_json(completeness_body) == canonical_json(completeness.to_dict()) and
        completeness_entry.available_at_ns <= horizon_start_ns)
    if (not account_matches or not completeness_matches or completeness.status != "COMPLETE" or
            completeness.available_at_ns > horizon_start_ns or
            completeness.common_scenario_set_id != scenario.common_scenario_set_id or
            completeness.common_path_ids != scenario_ids or unknown_exposure_present or
            existing_ids != scenario_ids or
            any(path.common_scenario_set_id != scenario.common_scenario_set_id for path in existing_paths) or
            tuple(path.probability for path in existing_paths) != tuple(prob for _, prob, _ in scenario.rows)):
        return DecisionTimePortfolioScenariosV2(action.action.action_hash, action.content_hash,
            scenario.content_hash, scenario.common_scenario_set_id, account_snapshot_ref, completeness_ref,
            tuple(sorted(set(existing_exposure_refs))), tuple(sorted(set(pending_exposure_refs))),
            unknown_exposure_present, horizon_start_ns, horizon_start_ns + 24 * 3_600_000_000_000,
            equity, drawdown, (), "NOT_ESTIMABLE", "SYNCHRONIZED_EXISTING_PENDING_UNKNOWN_EXPOSURE_PATHS_MISSING")
    payoff_by_path = {item.joint_path_id: item for item in payoffs}
    if len(payoff_by_path) != len(payoffs) or set(payoff_by_path) != set(scenario_ids):
        return DecisionTimePortfolioScenariosV2(action.action.action_hash, action.content_hash,
            scenario.content_hash, scenario.common_scenario_set_id, account_snapshot_ref, completeness_ref,
            tuple(sorted(set(existing_exposure_refs))), tuple(sorted(set(pending_exposure_refs))),
            unknown_exposure_present, horizon_start_ns, horizon_start_ns + 24 * 3_600_000_000_000,
            equity, drawdown, (), "NOT_ESTIMABLE", "CANDIDATE_PAYOFF_PATHS_UNRESOLVED")
    exposure_refs = tuple(sorted(set(existing_exposure_refs + pending_exposure_refs)))
    for existing in existing_paths:
        if not exposure_refs:
            if existing.existing_net_pnl != ZERO:
                return DecisionTimePortfolioScenariosV2(action.action.action_hash, action.content_hash,
                    scenario.content_hash, scenario.common_scenario_set_id, account_snapshot_ref, completeness_ref,
                    (), (), False, horizon_start_ns, horizon_start_ns + 24 * 3_600_000_000_000,
                    equity, drawdown, (), "NOT_ESTIMABLE", "FLAT_PORTFOLIO_HAS_NONZERO_PATH_VALUE")
            continue
        entry = repo.get_artifact(existing.content_hash)
        if (entry is None or entry.artifact_type != "ExistingPortfolioPathV2" or
                canonical_json(entry.metadata.get("evidence")) != canonical_json(existing.to_dict()) or
                existing.exposure_refs != exposure_refs or existing.information_cutoff_ns != horizon_start_ns or
                existing.horizon_end_ns != horizon_start_ns + 24 * 3_600_000_000_000 or
                existing.available_at_ns is None or existing.available_at_ns >= scenario.expires_at_ns):
            return DecisionTimePortfolioScenariosV2(action.action.action_hash, action.content_hash,
                scenario.content_hash, scenario.common_scenario_set_id, account_snapshot_ref, completeness_ref,
                tuple(existing_exposure_refs), tuple(pending_exposure_refs), False, horizon_start_ns,
                horizon_start_ns + 24 * 3_600_000_000_000, equity, drawdown, (), "NOT_ESTIMABLE",
                "EXISTING_PENDING_VALUATION_EVIDENCE_MISSING")
        index_existing_portfolio_path(repo, existing)
    paths = tuple(PortfolioPathV2(path_id, probability, existing.existing_net_pnl,
        payoff_by_path[path_id].net_payoff, payoff_by_path[path_id].content_hash, True,
        existing.content_hash if exposure_refs else None)
        for (path_id, probability, _), existing in zip(scenario.rows, existing_paths, strict=True))
    return DecisionTimePortfolioScenariosV2(action.action.action_hash, action.content_hash,
        scenario.content_hash, scenario.common_scenario_set_id, account_snapshot_ref, completeness_ref,
        tuple(sorted(set(existing_exposure_refs))), tuple(sorted(set(pending_exposure_refs))),
        False, horizon_start_ns, horizon_start_ns + 24 * 3_600_000_000_000,
        equity, drawdown, paths, "AVAILABLE")


@dataclass(frozen=True)
class PortfolioESV2:
    action_hash: str
    portfolio_scenario_ref: str
    confidence: Decimal
    limit_fraction: Decimal
    es_before_fraction: Decimal | None
    es_after_fraction: Decimal | None
    status: str
    breach: bool | None
    reason: str | None = None

    def __post_init__(self) -> None:
        sha256_ref(self.action_hash, field="action_hash")
        sha256_ref(self.portfolio_scenario_ref, field="portfolio_scenario_ref")
        if not ZERO < self.confidence < Decimal(1) or not ZERO <= self.limit_fraction <= Decimal(1):
            raise ValueError("portfolio ES risk-policy alpha/limit invalid")
        for name in ("es_before_fraction", "es_after_fraction"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, decimal_value(value, field=name))
        if self.status == "AVAILABLE":
            if self.es_before_fraction is None or self.es_after_fraction is None or self.breach is None:
                raise ValueError("available ES requires paired before/after values")
        elif self.es_before_fraction is not None or self.es_after_fraction is not None or self.breach is not None:
            raise ValueError("unestimable ES cannot carry computed values")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": "DECISION_TIME_PORTFOLIO_ES_V1", **{name: getattr(self, name) for name in self.__dataclass_fields__}})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class AdmissionPolicyV2:
    version: str
    materiality_threshold: Decimal
    minimum_action_support: int
    minimum_scenario_support_units: int
    minimum_execution_calibration: int
    confidence_multiplier: Decimal
    required_margin_mode: str
    required_position_mode: str
    required_nautilus_distribution: str
    required_nautilus_version: str
    required_nautilus_source_commit: str
    required_nautilus_artifact_ref: str
    required_execution_profile_ref: str
    required_protection_profile_ref: str
    required_qualification_version: str

    def __post_init__(self) -> None:
        if self.version != ADMISSION_POLICY_VERSION:
            raise ValueError("unsupported admission policy")
        object.__setattr__(self, "materiality_threshold", decimal_value(self.materiality_threshold, field="materiality_threshold"))
        object.__setattr__(self, "confidence_multiplier", decimal_value(self.confidence_multiplier, field="confidence_multiplier"))
        if self.materiality_threshold < 0 or min(self.minimum_action_support, self.minimum_scenario_support_units, self.minimum_execution_calibration) <= 0:
            raise ValueError("admission threshold/support configuration invalid")
        if self.confidence_multiplier <= 0:
            raise ValueError("admission confidence config invalid")
        for name in ("required_nautilus_artifact_ref", "required_execution_profile_ref",
                "required_protection_profile_ref"):
            sha256_ref(getattr(self, name), field=name)
        for name in ("required_margin_mode", "required_position_mode", "required_nautilus_distribution",
                "required_nautilus_version", "required_nautilus_source_commit", "required_qualification_version"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise ValueError(f"admission {name} required")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": self.version, **{name: getattr(self, name) for name in self.__dataclass_fields__ if name != "version"}})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> AdmissionPolicyV2:
        fields = {"version", "materiality_threshold", "minimum_action_support",
            "minimum_scenario_support_units", "minimum_execution_calibration", "confidence_multiplier",
            "required_margin_mode", "required_position_mode", "required_nautilus_distribution",
            "required_nautilus_version", "required_nautilus_source_commit", "required_nautilus_artifact_ref",
            "required_execution_profile_ref", "required_protection_profile_ref", "required_qualification_version"}
        d = dict(strict_fields(data, expected=fields, required=fields, name="AdmissionPolicyV2"))
        if d["version"] != ADMISSION_POLICY_VERSION:
            raise ValueError("unsupported admission policy wire")
        d["materiality_threshold"] = decimal_value(d["materiality_threshold"], field="materiality_threshold", wire=True)
        d["confidence_multiplier"] = decimal_value(d["confidence_multiplier"], field="confidence_multiplier", wire=True)
        return cls(**d)


@dataclass(frozen=True)
class LCBMethodV2:
    """Versioned expected-value lower bound; outcome-tail dispersion is excluded."""

    version: str = LCB_METHOD_VERSION
    base_estimate: str = "M0_CONDITIONAL_MEAN_NET_ACTION_VALUE"
    subtracted_components: tuple[str, ...] = LCB_COMPONENTS

    def __post_init__(self) -> None:
        if (self.version != LCB_METHOD_VERSION or
                self.base_estimate != "M0_CONDITIONAL_MEAN_NET_ACTION_VALUE" or
                self.subtracted_components != LCB_COMPONENTS):
            raise ValueError("unsupported expected-value LCB method")

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.version, "base_estimate": self.base_estimate,
                "subtracted_components": list(self.subtracted_components)}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> LCBMethodV2:
        d = strict_fields(data, expected={"version", "base_estimate", "subtracted_components"},
            required={"version", "base_estimate", "subtracted_components"}, name="LCBMethodV2")
        if not isinstance(d["subtracted_components"], list):
            raise ValueError("LCB method components must be an ordered list")
        return cls(d["version"], d["base_estimate"], tuple(d["subtracted_components"]))


@dataclass(frozen=True)
class AmendedEvaluationArtifactV2:
    action_hash: str
    action_artifact_ref: str
    candidate_ref: str
    quantity: Decimal
    policy_hash: str
    risk_policy_ref: str
    risk_policy_hash: str
    risk_policy_v2_ref: str
    risk_policy_v2_hash: str
    account_snapshot_ref: str
    universe_ref: str
    candidate_set_ref: str
    selection_policy_hash: str
    causal_state_ref: str
    feature_artifact_ref: str
    m0_model_ref: str
    m0_prediction_ref: str
    meta_version: str
    pretrade_scenario_ref: str
    deterministic_stress_ref: str
    existing_portfolio_ref: str
    expected_net_value: Decimal | None
    expected_pnl_lcb: Decimal | None
    lcb_method_ref: str
    estimation_uncertainty_ref: str
    execution_uncertainty_ref: str
    numerical_error_ref: str
    support_ref: str
    calibration_ref: str
    ood_ref: str
    outcome_distribution_ref: str
    admission_policy_ref: str
    capability_evidence_ref: str
    es_before: Decimal | None
    es_after: Decimal | None
    decision: DecisionStatusV2
    reason_codes: tuple[str, ...]
    decision_at_ns: int
    available_at_ns: int
    action_expiry_ns: int

    def __post_init__(self) -> None:
        refs = ("action_hash", "action_artifact_ref", "candidate_ref", "policy_hash", "risk_policy_ref",
            "risk_policy_hash", "risk_policy_v2_ref", "risk_policy_v2_hash", "account_snapshot_ref", "universe_ref",
            "candidate_set_ref", "selection_policy_hash", "causal_state_ref", "feature_artifact_ref", "m0_model_ref",
            "m0_prediction_ref", "pretrade_scenario_ref", "deterministic_stress_ref", "existing_portfolio_ref",
            "lcb_method_ref", "estimation_uncertainty_ref", "execution_uncertainty_ref", "numerical_error_ref",
            "support_ref", "calibration_ref", "ood_ref", "outcome_distribution_ref", "admission_policy_ref",
            "capability_evidence_ref")
        for name in refs:
            sha256_ref(getattr(self, name), field=name)
        object.__setattr__(self, "quantity", decimal_value(self.quantity, field="quantity"))
        for name in ("expected_net_value", "expected_pnl_lcb", "es_before", "es_after"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, decimal_value(value, field=name))
        for name in ("decision_at_ns", "available_at_ns", "action_expiry_ns"):
            if type(getattr(self, name)) is not int or getattr(self, name) < 0:
                raise ValueError(f"evaluation {name} invalid")
        if self.quantity <= 0 or self.decision_at_ns > self.available_at_ns or self.available_at_ns >= self.action_expiry_ns:
            raise ValueError("evaluation quantity/chronology invalid")
        object.__setattr__(self, "decision", DecisionStatusV2(self.decision))
        if self.reason_codes != tuple(sorted(set(self.reason_codes))):
            raise ValueError("evaluation reason codes must be sorted and unique")
        if self.decision != DecisionStatusV2.CANDIDATE and not self.reason_codes:
            raise ValueError("rejected/unestimable evaluation requires reason codes")
        if self.decision == DecisionStatusV2.CANDIDATE and (
                self.expected_net_value is None or self.expected_pnl_lcb is None or
                self.es_before is None or self.es_after is None or self.reason_codes):
            raise ValueError("CANDIDATE evaluation requires full economic/ES evidence and no rejection reason")

    def _body(self) -> dict[str, Any]:
        values = {name: getattr(self, name) for name in self.__dataclass_fields__}
        for name in ("quantity", "expected_net_value", "expected_pnl_lcb", "es_before", "es_after"):
            value = values[name]
            values[name] = canonical_decimal_str(value) if value is not None else None
        values["decision"] = self.decision.value
        values["reason_codes"] = list(self.reason_codes)
        return json_value({"version": EVALUATION_VERSION, **values})

    def to_dict(self) -> dict[str, Any]:
        return self._body()

    @property
    def content_hash(self) -> str:
        return sha256_json(self._body())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> AmendedEvaluationArtifactV2:
        fields = set(cls.__dataclass_fields__) | {"version"}
        d = dict(strict_fields(data, expected=fields, required=fields, name="AmendedEvaluationArtifactV2"))
        if d["version"] != EVALUATION_VERSION or not isinstance(d["reason_codes"], list):
            raise ValueError("unknown or pre-amendment EvaluationArtifactV2 wire")
        for name in ("quantity", "expected_net_value", "expected_pnl_lcb", "es_before", "es_after"):
            d[name] = decimal_value(d[name], field=name, wire=True) if d[name] is not None else None
        d["decision"] = DecisionStatusV2(d["decision"])
        d["reason_codes"] = tuple(d["reason_codes"])
        return cls(**{name: d[name] for name in cls.__dataclass_fields__})


def empirical_es_decimal(losses: Sequence[Decimal], probabilities: Sequence[Decimal], confidence: Decimal) -> Decimal:
    if not losses or len(losses) != len(probabilities) or sum(probabilities, ZERO) != Decimal(1) or not ZERO < confidence < Decimal(1):
        raise ValueError("ES requires aligned common paths and confidence in (0,1)")
    tail = Decimal(1) - confidence
    rows = sorted(zip(losses, probabilities, strict=True), key=lambda item: item[0], reverse=True)
    remaining = tail
    weighted = ZERO
    for loss, probability in rows:
        mass = min(probability, remaining)
        weighted += mass * loss
        remaining -= mass
        if remaining <= ZERO:
            break
    if remaining != ZERO:
        raise ValueError("ES probability mass does not cover requested tail")
    return weighted / tail


def make_outcome_distribution(scenario: PretradeExecutionScenarioV2,
        payoffs: Sequence[PretradePathPayoffV2]) -> OutcomeDistributionV2:
    if scenario.status != ScenarioGenerationStatusV2.AVAILABLE:
        return OutcomeDistributionV2(scenario.action_hash, scenario.content_hash, (), (), (), (), None, None,
            "NOT_ESTIMABLE", scenario.reason or "PRETRADE_SCENARIO_UNAVAILABLE")
    if scenario.synthetic_fixture:
        return OutcomeDistributionV2(scenario.action_hash, scenario.content_hash, (), (), (), (), None, None,
            "NOT_ESTIMABLE", "SYNTHETIC_ENGINEERING_SCENARIO")
    by_path = {payoff.joint_path_id: payoff for payoff in payoffs}
    if len(by_path) != len(payoffs) or any(payoff.action_hash != scenario.action_hash or
            payoff.scenario_artifact_ref != scenario.content_hash for payoff in payoffs):
        raise ValueError("scenario path payoff identity mismatch")
    if set(by_path) != {row[0] for row in scenario.rows}:
        return OutcomeDistributionV2(scenario.action_hash, scenario.content_hash, (), (), (), (), None, None,
            "NOT_ESTIMABLE", "SCENARIO_PATH_PAYOFF_MAPPING_INCOMPLETE")
    refs = tuple(by_path[path_id].content_hash for path_id, _, _ in scenario.rows)
    probs = tuple(probability for _, probability, _ in scenario.rows)
    values = tuple(by_path[path_id].net_payoff for path_id, _, _ in scenario.rows)
    rows = sorted(zip(values, probs, strict=True), key=lambda row: row[0])
    q05 = rows[0][0]
    cumulative = ZERO
    for value, probability in rows:
        cumulative += probability
        if cumulative >= Decimal("0.05"):
            q05 = value
            break
    mean = sum((prob * value for prob, value in zip(probs, values, strict=True)), ZERO)
    return OutcomeDistributionV2(scenario.action_hash, scenario.content_hash, refs,
        tuple(path_id for path_id, _, _ in scenario.rows), probs, values, mean, q05, "AVAILABLE")


def scenario_support_compatibility_classes(action: ActionArtifactV2) -> tuple[str, str]:
    frozen = action.action
    policy_class = sha256_json({"version": "SCENARIO_POLICY_COMPATIBILITY_V1",
        "policy_id": frozen.policy_id, "policy_version": frozen.policy_version,
        "policy_hash": frozen.policy_hash, "entry_rule": frozen.entry_rule.to_dict(),
        "collar_rule": frozen.collar_rule.to_dict(), "stop_trigger_basis": frozen.stop_trigger_basis,
        "management_rule": frozen.management_rule.to_dict(), "time_exit_rule": frozen.time_exit_rule.to_dict()})
    action_class = sha256_json({"version": "SCENARIO_ACTION_COMPATIBILITY_V1",
        "key": frozen.key.to_dict(), "product_ref": frozen.product_ref, "side": frozen.side,
        "quantity": frozen.quantity, "stop_trigger_basis": frozen.stop_trigger_basis,
        "entry_trigger_basis": frozen.entry_trigger_basis, "policy_class": policy_class})
    return policy_class, action_class


def index_scenario_support_unit(repo: OpsRepository, unit: ScenarioSupportUnitV2) -> str:
    template_entry = repo.get_artifact(unit.template_ref)
    template_body = template_entry.metadata.get("joint_execution_data") if template_entry is not None else None
    template = JointExecutionDataV2.from_dict(json_value(template_body)) if isinstance(template_body, Mapping) else None
    if (template_entry is None or template_entry.artifact_type != "JointExecutionDataV2" or
            template_entry.content_hash != unit.template_ref or template is None or
            template.content_hash != unit.template_ref or
            unit.source_episode_ref != template.source_ref or
            template.source_ref not in unit.source_bundle_refs or
            template_entry.available_at_ns > unit.available_at_ns or
            unit.synthetic_fixture != template.synthetic_fixture):
        raise ValueError("scenario support unit does not bind exact template and historical source window")
    episode = repo.get_artifact(unit.source_episode_ref)
    if (episode is None or episode.content_hash != unit.source_episode_ref or
            episode.available_at_ns > unit.available_at_ns or episode.created_at_ns > unit.available_at_ns):
        raise ValueError("scenario support episode/source identity is unavailable by unit evidence time")
    for ref in unit.source_bundle_refs:
        source = repo.get_artifact(ref)
        if (source is None or source.content_hash != ref or source.available_at_ns > unit.available_at_ns or
                source.created_at_ns > unit.available_at_ns):
            raise ValueError("scenario support source bundle is unavailable by unit evidence time")
    ref = unit.content_hash
    repo.register_artifact(ArtifactIndexEntryV2(ref, "ScenarioSupportUnitV2", ref,
        unit.available_at_ns, unit.available_at_ns, {"evidence": unit.to_dict()}))
    return ref


def _load_scenario_support_unit(repo: OpsRepository, ref: str, cutoff_ns: int) -> ScenarioSupportUnitV2:
    entry = repo.get_artifact(ref)
    body = entry.metadata.get("evidence") if entry is not None else None
    unit = ScenarioSupportUnitV2.from_dict(json_value(body)) if isinstance(body, Mapping) else None
    if (entry is None or entry.artifact_type != "ScenarioSupportUnitV2" or entry.content_hash != ref or
            not isinstance(body, Mapping) or sha256_json(body) != ref or unit is None or
            unit.content_hash != ref or entry.available_at_ns > cutoff_ns or unit.available_at_ns > cutoff_ns):
        raise ValueError("scenario support unit is missing, future or hash-mismatched")
    return unit


def _independent_support_units(units: Sequence[ScenarioSupportUnitV2]) -> tuple[ScenarioSupportUnitV2, ...]:
    """Greedy earliest-finish selection with strict episode/source dedupe and no overlap."""
    ordered = sorted(units, key=lambda item: (item.source_window_end_ns, item.source_window_start_ns,
        item.source_episode_ref, item.source_bundle_refs, item.template_ref, item.content_hash))
    accepted: list[ScenarioSupportUnitV2] = []
    seen_episodes: set[str] = set()
    seen_sources: set[str] = set()
    last_end_ns = -1
    for unit in ordered:
        if (unit.source_episode_ref in seen_episodes or unit.source_window_start_ns < last_end_ns or
                set(unit.source_bundle_refs).intersection(seen_sources)):
            continue
        accepted.append(unit)
        seen_episodes.add(unit.source_episode_ref)
        seen_sources.update(unit.source_bundle_refs)
        last_end_ns = unit.source_window_end_ns
    return tuple(accepted)


def make_scenario_support(repo: OpsRepository, *, action: ActionArtifactV2,
        scenario: PretradeExecutionScenarioV2, support_unit_refs: Sequence[str]) -> ScenarioSupportV2:
    refs = tuple(sorted(set(support_unit_refs)))
    if tuple(support_unit_refs) != refs:
        raise ValueError("scenario support unit refs must be sorted and unique")
    units = tuple(_load_scenario_support_unit(repo, ref, scenario.information_cutoff_ns) for ref in refs)
    policy_class, action_class = scenario_support_compatibility_classes(action)
    template_by_ref = {unit.template_ref: unit for unit in units}
    complete_units = set(template_by_ref) == set(scenario.template_support_refs)
    compatible = (scenario.action_hash == action.action.action_hash and
        scenario.action_artifact_ref == action.content_hash and complete_units and bool(units) and
        all(unit.venue == action.action.key.venue and unit.product_ref == action.action.product_ref and
            unit.policy_compatibility_class == policy_class and unit.action_compatibility_class == action_class and
            unit.execution_model_ref == scenario.execution_model_input.ref and
            unit.calibration_ref == scenario.calibration_input.ref for unit in units))
    for unit in units:
        if unit.template_ref not in scenario.template_support_refs:
            compatible = False
        template_entry = repo.get_artifact(unit.template_ref)
        template_body = template_entry.metadata.get("joint_execution_data") if template_entry is not None else None
        template = JointExecutionDataV2.from_dict(json_value(template_body)) if isinstance(template_body, Mapping) else None
        if (template is None or unit.source_episode_ref != template.source_ref or
                template.source_ref not in unit.source_bundle_refs or
                template.action_hash != action.action.action_hash or
                template.execution_model_ref != scenario.execution_model_input.ref or
                template.information_cutoff_ns > scenario.information_cutoff_ns):
            compatible = False
    independent = _independent_support_units(units) if compatible else ()
    real_evidence = compatible and not scenario.synthetic_fixture and all(not unit.synthetic_fixture for unit in units)
    quality = "SUPPORTED" if real_evidence and independent else "UNSUPPORTED_OR_ENGINEERING_FIXTURE"
    return ScenarioSupportV2(action.action.action_hash, scenario.content_hash, len(independent), refs,
        tuple(sorted(unit.content_hash for unit in independent)), compatible, compatible, compatible,
        compatible, compatible, quality, scenario.template_support_refs)


def make_inference_support(action_hash: str, m0: M0SupportV2,
        scenario: ScenarioSupportV2) -> InferenceSupportV2:
    missing = dict(m0.missing_feature_coverage).get("__any_missing__", Decimal(1))
    checks = (scenario.policy_compatible, scenario.horizon_compatible, scenario.venue_product_compatible,
              scenario.execution_mode_compatible, scenario.depth_supported)
    supported = m0.evidence_quality == "SUPPORTED" and all(checks)
    return InferenceSupportV2(action_hash, m0.content_hash, scenario.content_hash,
        m0.eligible_sample_count, m0.independent_support_count, scenario.independent_support_unit_count,
        missing, *checks, scenario.evidence_quality,
        "SUPPORTED" if supported else "INSUFFICIENT")


def make_estimation_uncertainty(prediction: M0PredictionV2, *, training_refs: Sequence[str], confidence_multiplier: Decimal) -> EstimationUncertaintyV2:
    available = prediction.status == "AVAILABLE" and prediction.estimation_uncertainty is not None
    se = prediction.estimation_uncertainty if available else None
    amount = se * confidence_multiplier if se is not None else None
    return EstimationUncertaintyV2(prediction.action_hash, prediction.content_hash, prediction.oof_archive_ref,
        tuple(training_refs), se, confidence_multiplier, amount, "AVAILABLE" if available else "NOT_ESTIMABLE")


def make_execution_uncertainty(action_hash: str, execution_model_ref: str,
        residuals: Sequence[ExecutionCalibrationResidualV2], *, compatibility_key: str,
        cutoff_ns: int, minimum_support: int) -> ExecutionModelUncertaintyV2:
    rows_by_ref: dict[str, ExecutionCalibrationResidualV2] = {}
    for item in residuals:
        if item.action_compatibility_key != compatibility_key or item.available_at_ns > cutoff_ns:
            continue
        prior = rows_by_ref.get(item.evidence_ref)
        if prior is not None and prior != item:
            raise ValueError("execution calibration evidence ref has conflicting residual values")
        rows_by_ref[item.evidence_ref] = item
    rows = [rows_by_ref[ref] for ref in sorted(rows_by_ref)]
    # Distinct hashes alone are not independent execution observations. Use
    # nonoverlapping decision-to-label-availability windows and unique source
    # bundles; this deliberately errs toward less historical support.
    independent_rows: list[ExecutionCalibrationResidualV2] = []
    last_end = -1
    seen_sources: set[tuple[str, ...]] = set()
    for item in sorted(rows, key=lambda row: (row.decision_at_ns, row.available_at_ns, row.evidence_ref)):
        sources = item.source_refs or (item.evidence_ref,)
        if item.decision_at_ns < last_end or sources in seen_sources:
            continue
        independent_rows.append(item)
        seen_sources.add(sources)
        last_end = item.available_at_ns
    independent = len(independent_rows)
    if independent < minimum_support:
        return ExecutionModelUncertaintyV2(action_hash, execution_model_ref,
            tuple(item.evidence_ref for item in rows), independent, None, "NOT_ESTIMABLE")
    errors = sorted(abs(item.realized_cost - item.predicted_cost) for item in independent_rows)
    q90 = errors[(9 * len(errors) + 9) // 10 - 1]
    return ExecutionModelUncertaintyV2(action_hash, execution_model_ref,
        tuple(item.evidence_ref for item in rows), independent, q90, "AVAILABLE")


def make_portfolio_es(portfolio: DecisionTimePortfolioScenariosV2, *, risk_policy: RiskPolicy,
                      risk_policy_ref: str) -> PortfolioESV2:
    sha256_ref(risk_policy_ref, field="risk_policy_ref")
    if portfolio.status != "AVAILABLE":
        return PortfolioESV2(portfolio.action_hash, portfolio.content_hash,
            risk_policy.portfolio_es_alpha, risk_policy.portfolio_es_limit_frac * risk_policy.scaling_at(portfolio.drawdown),
            None, None, "NOT_ESTIMABLE", None, portfolio.reason or "PORTFOLIO_PATHS_UNAVAILABLE")
    probabilities = tuple(path.probability for path in portfolio.paths)
    before_losses = tuple(-path.existing_net_pnl / portfolio.equity for path in portfolio.paths)
    after_losses = tuple(-(path.existing_net_pnl + path.candidate_net_pnl) / portfolio.equity for path in portfolio.paths)
    before = empirical_es_decimal(before_losses, probabilities, risk_policy.portfolio_es_alpha)
    after = empirical_es_decimal(after_losses, probabilities, risk_policy.portfolio_es_alpha)
    limit = risk_policy.portfolio_es_limit_frac * risk_policy.scaling_at(portfolio.drawdown)
    return PortfolioESV2(portfolio.action_hash, portfolio.content_hash, risk_policy.portfolio_es_alpha,
        limit, before, after, "AVAILABLE", after > limit)


def expected_value_lcb(expected_net_value: Decimal, estimation: EstimationUncertaintyV2,
        execution: ExecutionModelUncertaintyV2, numerical: NumericalErrorV2) -> Decimal | None:
    """Subtract only mean-estimation, execution-model and numerical terms."""
    if (estimation.status != "AVAILABLE" or estimation.uncertainty_amount is None or
            execution.status != "AVAILABLE" or execution.absolute_cost_error_q90 is None or
            numerical.status != "AVAILABLE" or numerical.error_bound is None):
        return None
    return expected_net_value - estimation.uncertainty_amount - execution.absolute_cost_error_q90 - numerical.error_bound


@dataclass(frozen=True)
class AdmissionResultV2:
    decision: DecisionStatusV2
    reasons: tuple[str, ...]
    expected_net_value: Decimal | None
    lcb: Decimal | None
    lower_bound_components: tuple[tuple[str, Decimal], ...]


def decide_admission(*, action: ActionArtifactV2, prediction: M0PredictionV2,
        m0_support: M0SupportV2, calibration: M0CalibrationV2, ood: M0OODV2,
        scenario: PretradeExecutionScenarioV2, scenario_support: ScenarioSupportV2,
        outcome_distribution: OutcomeDistributionV2,
        estimation: EstimationUncertaintyV2, execution: ExecutionModelUncertaintyV2,
        numerical: NumericalErrorV2, stress: DeterministicStressV2, portfolio: PortfolioESV2,
        policy: AdmissionPolicyV2, capability: VenueCapabilitySnapshotV2 | None = None,
        account_scope: str | None = None,
        allow_synthetic_fixtures: bool = False) -> AdmissionResultV2:
    reasons: list[str] = []
    if action.action.action_hash != prediction.action_hash or scenario.action_hash != action.action.action_hash:
        reasons.append("ACTION_IDENTITY_CONFLICT")
    if prediction.status != "AVAILABLE" or prediction.expected_net_value is None:
        reasons.append("M0_NOT_ESTIMABLE")
    if m0_support.eligible_sample_count < policy.minimum_action_support or m0_support.evidence_quality != "SUPPORTED":
        reasons.append("M0_SUPPORT_INSUFFICIENT")
    if calibration.status != "OOF_CALIBRATED":
        reasons.append("M0_CALIBRATION_UNSUPPORTED")
    if ood.out_of_distribution is not False or ood.status != "IN_DISTRIBUTION":
        reasons.append("M0_STATE_OOD_OR_UNESTIMABLE")
    if scenario.status != ScenarioGenerationStatusV2.AVAILABLE or scenario.synthetic_fixture:
        reasons.append("PRETRADE_SCENARIO_UNSUPPORTED")
    if (scenario_support.independent_support_unit_count < policy.minimum_scenario_support_units or
            not all((scenario_support.policy_compatible, scenario_support.horizon_compatible,
                     scenario_support.venue_product_compatible, scenario_support.execution_mode_compatible,
                     scenario_support.depth_supported))):
        reasons.append("PRETRADE_SCENARIO_SUPPORT_INSUFFICIENT")
    if (outcome_distribution.status != "AVAILABLE" or
            outcome_distribution.action_hash != action.action.action_hash or
            outcome_distribution.scenario_ref != scenario.content_hash):
        reasons.append("OUTCOME_DISTRIBUTION_UNESTIMABLE")
    if estimation.status != "AVAILABLE" or estimation.uncertainty_amount is None:
        reasons.append("ESTIMATION_UNCERTAINTY_UNESTIMABLE")
    if execution.status != "AVAILABLE" or execution.absolute_cost_error_q90 is None or execution.independent_support_count < policy.minimum_execution_calibration:
        reasons.append("EXECUTION_MODEL_UNCERTAINTY_UNESTIMABLE")
    if numerical.status != "AVAILABLE" or numerical.error_bound is None:
        reasons.append("NUMERICAL_ERROR_UNESTIMABLE")
    if stress.status != "AVAILABLE" or stress.breach is None:
        reasons.append("DETERMINISTIC_STRESS_UNESTIMABLE")
    if portfolio.status != "AVAILABLE" or portfolio.breach is None:
        reasons.append("PORTFOLIO_ES_UNESTIMABLE")
    if capability is None or account_scope is None:
        reasons.append("VENUE_CAPABILITY_EVIDENCE_MISSING")
    elif not capability.supports(action, account_scope=account_scope,
            cutoff_ns=action_identity_cutoff_for_action(prediction, scenario), policy=policy,
            allow_synthetic_fixtures=allow_synthetic_fixtures):
        reasons.append("VENUE_CAPABILITY_UNQUALIFIED_OR_SCOPE_MISMATCH")
    if reasons:
        return AdmissionResultV2(DecisionStatusV2.NOT_ESTIMABLE, tuple(sorted(set(reasons))), prediction.expected_net_value, None, ())
    assert prediction.expected_net_value is not None
    assert estimation.uncertainty_amount is not None and execution.absolute_cost_error_q90 is not None
    assert numerical.error_bound is not None and stress.breach is not None and portfolio.breach is not None
    components = (("estimation_uncertainty", estimation.uncertainty_amount),
        ("execution_model_uncertainty", execution.absolute_cost_error_q90),
        ("numerical_error", numerical.error_bound))
    lcb = expected_value_lcb(prediction.expected_net_value, estimation, execution, numerical)
    if lcb is None:
        return AdmissionResultV2(DecisionStatusV2.NOT_ESTIMABLE, ("EXPECTED_VALUE_LCB_UNESTIMABLE",),
            prediction.expected_net_value, None, ())
    rejected: list[str] = []
    if lcb <= 0:
        rejected.append("EXPECTED_NET_VALUE_LCB_NOT_POSITIVE")
    if lcb < policy.materiality_threshold:
        rejected.append("EXPECTED_NET_VALUE_BELOW_MATERIALITY")
    if stress.breach:
        rejected.append("DETERMINISTIC_STRESS_BREACH")
    if portfolio.breach:
        rejected.append("PORTFOLIO_ES_LIMIT_BREACH")
    return AdmissionResultV2(DecisionStatusV2.NO_TRADE if rejected else DecisionStatusV2.CANDIDATE,
        tuple(sorted(set(rejected))), prediction.expected_net_value, lcb, components)


def action_identity_cutoff_for_action(prediction: M0PredictionV2,
        scenario: PretradeExecutionScenarioV2) -> int:
    if prediction.training_cutoff_ns != scenario.information_cutoff_ns:
        raise ValueError("M0 and scenario cutoff disagree")
    return prediction.training_cutoff_ns


def index_admission_evidence(repo: OpsRepository, artifact_type: str, body: Mapping[str, Any],
                             available_at_ns: int, *, metadata_key: str = "evidence") -> str:
    """Index immutable typed Session 019 evidence before downstream references it."""
    ref = sha256_json(body)
    repo.register_artifact(ArtifactIndexEntryV2(ref, artifact_type, ref,
        available_at_ns, available_at_ns, {metadata_key: json_value(body)}))
    return ref


def index_execution_calibration_residual(repo: OpsRepository,
        residual: ExecutionCalibrationResidualV2) -> str:
    if (residual.provenance == "UNVERIFIED" or not residual.source_refs or
            residual.evidence_ref != residual.content_hash):
        raise ValueError("execution calibration residual needs a content-bound source/provenance")
    for ref in residual.source_refs:
        entry = repo.get_artifact(ref)
        if entry is None or entry.available_at_ns > residual.available_at_ns:
            raise ValueError("execution calibration source is unavailable by residual time")
    ref = index_admission_evidence(repo, "ExecutionCalibrationResidualV2",
        residual.to_dict(), residual.available_at_ns, metadata_key="residual")
    if ref != residual.evidence_ref:
        raise ValueError("execution calibration residual evidence ref/content mismatch")
    return ref


def index_lcb_method(repo: OpsRepository, method: LCBMethodV2, *, available_at_ns: int) -> str:
    return index_admission_evidence(repo, "LCBMethodV2", method.to_dict(), available_at_ns,
        metadata_key="lcb_method")


def index_portfolio_completeness(repo: OpsRepository,
        evidence: DecisionTimePortfolioCompletenessV2) -> str:
    return index_admission_evidence(repo, "DecisionTimePortfolioCompletenessV2",
        evidence.to_dict(), evidence.available_at_ns)


def make_amended_evaluation(*, action: ActionArtifactV2, candidate: CandidateActionV2,
        candidate_set: CandidateSetV2, prediction: M0PredictionV2,
        scenario: PretradeExecutionScenarioV2, stress: DeterministicStressV2,
        portfolio: DecisionTimePortfolioScenariosV2, portfolio_es: PortfolioESV2,
        support: InferenceSupportV2, estimation_ref: str, execution_ref: str,
        numerical_ref: str, calibration_ref: str, ood_ref: str,
        outcome_distribution_ref: str, admission_policy_ref: str,
        capability_evidence_ref: str,
        lcb_method_ref: str, account_snapshot_ref: str, risk_policy_ref: str,
        risk_policy_v2_ref: str, causal_state_ref: str, available_at_ns: int,
        result: AdmissionResultV2) -> AmendedEvaluationArtifactV2:
    if (candidate.content_hash != action.candidate_ref or candidate_set.content_hash != action.candidate_set_ref or
            candidate_set.selected_candidate_id != candidate.candidate_id or
            prediction.action_hash != action.action.action_hash or
            prediction.action_artifact_ref != action.content_hash or prediction.training_cutoff_ns != candidate.decision_at_ns or
            scenario.action_hash != action.action.action_hash or scenario.action_artifact_ref != action.content_hash or
            scenario.information_cutoff_ns != candidate.decision_at_ns or
            stress.action_hash != action.action.action_hash or stress.action_artifact_ref != action.content_hash or
            stress.quantity != action.action.quantity or stress.risk_policy_ref != risk_policy_ref or
            portfolio.action_hash != action.action.action_hash or portfolio.action_artifact_ref != action.content_hash or
            portfolio.candidate_scenario_ref != scenario.content_hash or
            portfolio_es.action_hash != action.action.action_hash or
            portfolio_es.portfolio_scenario_ref != portfolio.content_hash or
            support.action_hash != action.action.action_hash or
            result.expected_net_value != prediction.expected_net_value):
        raise ValueError("evaluation evidence belongs to a different frozen action or selection")
    if (candidate.decision_at_ns > available_at_ns or available_at_ns >= candidate.deadline_ns or
            available_at_ns < max(prediction.available_at_ns, scenario.available_at_ns)):
        raise ValueError("evaluation computation outside exact action decision window")
    return AmendedEvaluationArtifactV2(action.action.action_hash, action.content_hash,
        candidate.content_hash, action.action.quantity, action.action.policy_hash,
        risk_policy_ref, action.action.risk_policy_hash, risk_policy_v2_ref,
        action.action.risk_policy_v2_hash, account_snapshot_ref, candidate_set.universe_ref,
        candidate_set.content_hash, candidate_set.selection_policy_hash, causal_state_ref,
        candidate.snapshot_hash, prediction.model_ref, prediction.content_hash,
        "M0_HUBER_RIDGE_ACTION_VALUE_V1", scenario.content_hash, stress.content_hash,
        portfolio.content_hash, result.expected_net_value, result.lcb, lcb_method_ref,
        estimation_ref, execution_ref, numerical_ref, support.content_hash,
        calibration_ref, ood_ref, outcome_distribution_ref, admission_policy_ref, capability_evidence_ref,
        portfolio_es.es_before_fraction, portfolio_es.es_after_fraction, result.decision,
        result.reasons, candidate.decision_at_ns, available_at_ns, candidate.deadline_ns)


def index_amended_evaluation(repo: OpsRepository, evaluation: AmendedEvaluationArtifactV2, *,
        allow_synthetic_fixtures: bool = False) -> str:
    action_entry = repo.get_artifact(evaluation.action_artifact_ref)
    action_body = action_entry.metadata.get("action_artifact") if action_entry is not None else None
    identity = action_entry.metadata.get("action_identity") if action_entry is not None else None
    if (action_entry is None or action_entry.artifact_type != "ActionArtifactV2" or
            not isinstance(action_body, Mapping) or not isinstance(identity, Mapping) or
            sha256_json(action_body) != evaluation.action_artifact_ref or
            sha256_json(identity) != evaluation.action_hash or identity.get("quantity") != canonical_decimal_str(evaluation.quantity) or
            identity.get("policy_hash") != evaluation.policy_hash or action_body.get("candidate_ref") != evaluation.candidate_ref or
            action_body.get("candidate_set_ref") != evaluation.candidate_set_ref or
            identity.get("risk_policy_hash") != evaluation.risk_policy_hash or
            identity.get("risk_policy_v2_hash") != evaluation.risk_policy_v2_hash or
            evaluation.risk_policy_ref != evaluation.risk_policy_hash or
            evaluation.risk_policy_v2_ref != evaluation.risk_policy_v2_hash):
        raise ValueError("amended evaluation exact action/candidate binding mismatch")
    resolved_action = ActionArtifactV2(FrozenActionV2(InstrumentKeyV2.from_dict(identity["key"]),
        identity["side"], decimal_value(identity["quantity"], field="quantity", wire=True),
        identity["product_ref"], identity["entry_rule"], identity["collar_rule"],
        decimal_value(identity["entry_reference"], field="entry_reference", wire=True),
        decimal_value(identity["entry_collar"], field="entry_collar", wire=True),
        decimal_value(identity["stop_price"], field="stop_price", wire=True),
        identity.get("entry_trigger_basis", identity["stop_trigger_basis"]), identity["stop_trigger_basis"],
        identity["management_rule"], identity["time_exit_rule"], identity["horizon_end_ns"],
        identity["policy_id"], identity["policy_version"], identity["policy_hash"],
        identity["risk_policy_hash"], identity["risk_policy_v2_hash"]),
        action_body["candidate_ref"], action_body["sizing_ref"], action_body["candidate_set_ref"],
        action_body["available_at_ns"])
    required = ((evaluation.candidate_ref, "CandidateActionV2"),
        (evaluation.candidate_set_ref, "CandidateSetV2"), (evaluation.risk_policy_ref, "RiskPolicyV1"),
        (evaluation.risk_policy_v2_ref, "RiskPolicyV2"), (evaluation.account_snapshot_ref, "AccountRiskSnapshotV2"),
        (evaluation.universe_ref, "UniverseContractV2"), (evaluation.m0_model_ref, "M0ModelFitV2"),
        (evaluation.m0_prediction_ref, "M0PredictionV2"), (evaluation.deterministic_stress_ref, "DeterministicStressV2"),
        (evaluation.existing_portfolio_ref, "DecisionTimePortfolioScenariosV2"),
        (evaluation.estimation_uncertainty_ref, "EstimationUncertaintyV2"),
        (evaluation.execution_uncertainty_ref, "ExecutionModelUncertaintyV2"),
        (evaluation.numerical_error_ref, "NumericalErrorV2"), (evaluation.support_ref, "InferenceSupportV2"),
        (evaluation.calibration_ref, "M0CalibrationV2"), (evaluation.ood_ref, "M0OODV2"),
        (evaluation.outcome_distribution_ref, "OutcomeDistributionV2"),
        (evaluation.admission_policy_ref, "AdmissionPolicyV2"),
        (evaluation.capability_evidence_ref, "VenueCapabilitySnapshotV2"),
        (evaluation.lcb_method_ref, "LCBMethodV2"))
    for ref, kind in required:
        entry = repo.get_artifact(ref)
        if entry is None or entry.artifact_type != kind or entry.available_at_ns > evaluation.available_at_ns:
            raise ValueError(f"evaluation required {kind} ref unavailable")
    evidence_specs = (
        (evaluation.account_snapshot_ref, "AccountRiskSnapshotV2", None),
        (evaluation.m0_model_ref, "M0ModelFitV2", "model_fit"),
        (evaluation.m0_prediction_ref, "M0PredictionV2", "prediction"),
        (evaluation.deterministic_stress_ref, "DeterministicStressV2", "evidence"),
        (evaluation.existing_portfolio_ref, "DecisionTimePortfolioScenariosV2", "evidence"),
        (evaluation.estimation_uncertainty_ref, "EstimationUncertaintyV2", "evidence"),
        (evaluation.execution_uncertainty_ref, "ExecutionModelUncertaintyV2", "evidence"),
        (evaluation.numerical_error_ref, "NumericalErrorV2", "evidence"),
        (evaluation.support_ref, "InferenceSupportV2", "evidence"),
        (evaluation.calibration_ref, "M0CalibrationV2", "calibration"),
        (evaluation.ood_ref, "M0OODV2", "ood"),
        (evaluation.outcome_distribution_ref, "OutcomeDistributionV2", "evidence"),
        (evaluation.admission_policy_ref, "AdmissionPolicyV2", "evidence"),
        (evaluation.capability_evidence_ref, "VenueCapabilitySnapshotV2", "capability"),
        (evaluation.lcb_method_ref, "LCBMethodV2", "lcb_method"),
    )
    evidence_bodies: dict[str, Mapping[str, Any]] = {}
    direct_hash_kinds = {"M0ModelFitV2", "M0PredictionV2", "DeterministicStressV2",
        "DecisionTimePortfolioScenariosV2", "EstimationUncertaintyV2", "ExecutionModelUncertaintyV2",
        "NumericalErrorV2", "InferenceSupportV2", "M0CalibrationV2", "M0OODV2",
        "OutcomeDistributionV2", "AdmissionPolicyV2", "VenueCapabilitySnapshotV2", "LCBMethodV2"}
    for ref, kind, metadata_key in evidence_specs:
        entry = repo.get_artifact(ref)
        body: Any = None
        if entry is not None:
            body = entry.metadata if metadata_key is None else entry.metadata.get(metadata_key)
        if not isinstance(body, Mapping):
            raise ValueError(f"evaluation required typed {kind} body missing")
        if kind in direct_hash_kinds and sha256_json(body) != ref:
            raise ValueError(f"evaluation {kind} body/content hash mismatch")
        evidence_bodies[ref] = body
    candidate_entry = repo.get_artifact(evaluation.candidate_ref)
    candidate_body = candidate_entry.metadata.get("candidate") if candidate_entry is not None else None
    candidate = CandidateActionV2.from_dict(json_value(candidate_body)) if isinstance(candidate_body, Mapping) else None
    candidate_set_entry = repo.get_artifact(evaluation.candidate_set_ref)
    candidate_set_body = candidate_set_entry.metadata.get("candidate_set") if candidate_set_entry is not None else None
    candidate_set = CandidateSetV2.from_dict(json_value(candidate_set_body)) if isinstance(candidate_set_body, Mapping) else None
    if (candidate is None or candidate_set is None or candidate.content_hash != evaluation.candidate_ref or
            candidate.snapshot_hash != evaluation.feature_artifact_ref or
            candidate_set.content_hash != evaluation.candidate_set_ref or
            candidate_set.selection_policy_hash != evaluation.selection_policy_hash or
            candidate_set.selected_candidate_id != candidate.candidate_id or
            candidate_set.universe_ref != evaluation.universe_ref or
            candidate_set.envelope.input_refs != tuple(sorted(set(candidate_set.envelope.input_refs)))):
        raise ValueError("evaluation candidate/CandidateSet/feature/universe identity mismatch")
    if candidate.decision_at_ns != evaluation.decision_at_ns or candidate.policy_hash != evaluation.policy_hash:
        raise ValueError("evaluation decision-time or policy identity mismatch")
    policy_entry = repo.get_artifact(evaluation.risk_policy_ref)
    policy_body = policy_entry.metadata.get("policy") if policy_entry is not None else None
    if (policy_entry is None or not isinstance(policy_body, Mapping) or
            policy_entry.artifact_type != "RiskPolicyV1" or policy_entry.content_hash != evaluation.risk_policy_ref):
        raise ValueError("evaluation RiskPolicyV1 body missing")
    policy_values = {name: policy_body[name] for name in RiskPolicy.__dataclass_fields__}
    for name in ("normal_loss_per_trade_frac", "aggregate_open_normal_loss_frac", "stress_loss_per_trade_frac",
        "portfolio_es_limit_frac", "account_gross_notional_limit", "instrument_notional_limit",
        "correlated_crypto_beta_limit", "venue_collateral_limit", "min_free_margin_reserve_frac",
        "drawdown_reduce_threshold", "drawdown_stop_threshold", "drawdown_reduce_recovery",
        "drawdown_stop_recovery", "max_contract_leverage"):
        policy_values[name] = decimal_value(policy_values[name], field=name, wire=True)
    policy_values["portfolio_es_alpha"] = decimal_value(policy_values["portfolio_es_alpha"], field="portfolio_es_alpha", wire=True)
    if policy_values["external_capital_reference"] is not None:
        policy_values["external_capital_reference"] = decimal_value(policy_values["external_capital_reference"],
            field="external_capital_reference", wire=True)
    risk_policy = RiskPolicy(**policy_values)
    if risk_policy.policy_hash() != evaluation.risk_policy_ref or canonical_json(risk_policy.to_dict()) != canonical_json(policy_body):
        raise ValueError("evaluation RiskPolicyV1 hash/body mismatch")
    risk_policy_v2_entry = repo.get_artifact(evaluation.risk_policy_v2_ref)
    risk_policy_v2_body = risk_policy_v2_entry.metadata.get("policy") if risk_policy_v2_entry is not None else None
    if (risk_policy_v2_entry is None or not isinstance(risk_policy_v2_body, Mapping) or
            risk_policy_v2_entry.artifact_type != "RiskPolicyV2" or
            risk_policy_v2_entry.content_hash != evaluation.risk_policy_v2_ref):
        raise ValueError("evaluation RiskPolicyV2 body missing")
    from atlas.v2.risk import RiskPolicyV2
    risk_policy_v2 = RiskPolicyV2.from_dict(dict(risk_policy_v2_body))
    if (risk_policy_v2.policy_hash != evaluation.risk_policy_v2_ref or
            risk_policy_v2.base_v1_risk_policy_hash != evaluation.risk_policy_ref):
        raise ValueError("evaluation V1/V2 risk policy binding mismatch")
    universe_entry = repo.get_artifact(evaluation.universe_ref)
    universe_body = universe_entry.metadata.get("universe") if universe_entry is not None else None
    if (universe_entry is None or not isinstance(universe_body, Mapping) or
            universe_entry.artifact_type != "UniverseContractV2" or
            universe_entry.content_hash != evaluation.universe_ref):
        raise ValueError("evaluation universe evidence missing")
    from atlas.v2.instruments import UniverseContractV2
    universe = UniverseContractV2.from_dict(json_value(universe_body))
    if universe.content_hash != evaluation.universe_ref:
        raise ValueError("evaluation universe content hash mismatch")
    account_body = evidence_bodies[evaluation.account_snapshot_ref]
    if (sha256_json(account_body) != evaluation.account_snapshot_ref or
            account_body.get("available_at_ns", evaluation.decision_at_ns + 1) > evaluation.decision_at_ns or
            account_body.get("operational_status") != "CURRENT"):
        raise ValueError("evaluation account snapshot is future, stale or hash-mismatched")
    if candidate.account_scope is not None:
        account_entry = repo.get_artifact(evaluation.account_snapshot_ref)
        if account_entry is None or account_entry.metadata.get("account_scope") != candidate.account_scope:
            raise ValueError("evaluation account snapshot scope conflicts with candidate")
    capability_body = evidence_bodies[evaluation.capability_evidence_ref]
    capability = VenueCapabilitySnapshotV2.from_dict(json_value(capability_body))
    admission_policy = AdmissionPolicyV2.from_dict(
        json_value(evidence_bodies[evaluation.admission_policy_ref]))
    expected_account_scope = account_body.get("account_scope")
    if (capability.content_hash != evaluation.capability_evidence_ref or
            capability.venue != resolved_action.action.key.venue or
            capability.environment != resolved_action.action.key.environment or
            capability.product_ref != resolved_action.action.product_ref or
            capability.instrument_key_ref != resolved_action.action.key.content_hash or
            capability.account_scope != expected_account_scope or
            (candidate.account_scope is not None and capability.account_scope != candidate.account_scope) or
            capability.margin_mode != admission_policy.required_margin_mode or
            capability.position_mode != admission_policy.required_position_mode or
            capability.nautilus_distribution != admission_policy.required_nautilus_distribution or
            capability.nautilus_version != admission_policy.required_nautilus_version or
            capability.nautilus_source_commit != admission_policy.required_nautilus_source_commit or
            capability.nautilus_artifact_ref != admission_policy.required_nautilus_artifact_ref or
            capability.execution_profile_ref != admission_policy.required_execution_profile_ref or
            capability.protection_profile_ref != admission_policy.required_protection_profile_ref or
            capability.qualification_version != admission_policy.required_qualification_version or
            capability.available_at_ns > evaluation.decision_at_ns):
        raise ValueError("evaluation capability evidence has wrong venue/account/product/runtime/profile or is future")
    validate_venue_capability_snapshot(repo, capability, cutoff_ns=evaluation.decision_at_ns,
        action=resolved_action)
    feature_entry = repo.get_artifact(evaluation.feature_artifact_ref)
    if (feature_entry is None or feature_entry.artifact_type != "FeatureArtifactV2" or
            feature_entry.content_hash != evaluation.feature_artifact_ref or
            feature_entry.available_at_ns > candidate.decision_at_ns):
        raise ValueError("evaluation original cutoff feature artifact is unavailable")
    if repo.get_artifact(evaluation.causal_state_ref) is None:
        raise ValueError("evaluation causal state artifact unavailable")
    model_entry = repo.get_artifact(evaluation.m0_model_ref)
    model_body = model_entry.metadata.get("model_fit") if model_entry is not None else None
    if (model_entry is None or not isinstance(model_body, Mapping) or
            model_entry.artifact_type != "M0ModelFitV2" or model_entry.content_hash != evaluation.m0_model_ref or
            model_body.get("current_action_hash") != evaluation.action_hash or
            model_entry.available_at_ns > evaluation.available_at_ns):
        raise ValueError("evaluation M0 fitted model belongs to another action or is unavailable")
    prediction_entry = repo.get_artifact(evaluation.m0_prediction_ref)
    prediction_body = prediction_entry.metadata.get("prediction") if prediction_entry is not None else None
    prediction = M0PredictionV2.from_dict(json_value(prediction_body)) if isinstance(prediction_body, Mapping) else None
    if (prediction_entry is None or prediction_entry.artifact_type != "M0PredictionV2" or
            prediction_entry.content_hash != evaluation.m0_prediction_ref or prediction is None or
            prediction.content_hash != evaluation.m0_prediction_ref or prediction.action_hash != evaluation.action_hash or
            prediction.action_artifact_ref != evaluation.action_artifact_ref or
            prediction.model_ref != evaluation.m0_model_ref or prediction.available_at_ns > evaluation.available_at_ns):
        raise ValueError("evaluation M0 prediction belongs to another action")
    model_body = evidence_bodies[evaluation.m0_model_ref]
    if (sha256_json(model_body) != evaluation.m0_model_ref or
            model_body.get("version") != M0_MODEL_VERSION or
            model_body.get("feature_schema_version") != M0_FEATURE_SCHEMA_VERSION or
            model_body.get("config_version") != M0_CONFIG_VERSION or
            model_body.get("current_action_hash") != evaluation.action_hash or
            model_body.get("current_action_artifact_ref") != evaluation.action_artifact_ref or
            model_body.get("oof_archive_ref") != prediction.oof_archive_ref or
            model_body.get("training_cutoff_ns") != prediction.training_cutoff_ns or
            model_body.get("current_feature_vector_ref") != prediction.feature_vector_ref or
            model_body.get("status") != prediction.status or
            tuple(model_body.get("reasons", ())) != prediction.reasons):
        raise ValueError("evaluation M0 fit does not reproduce the exact prediction inputs")
    feature_vector_entry = repo.get_artifact(prediction.feature_vector_ref)
    feature_vector_body = feature_vector_entry.metadata.get("feature_vector") if feature_vector_entry is not None else None
    feature_vector = M0FeatureVectorV2.from_dict(json_value(feature_vector_body)) if isinstance(feature_vector_body, Mapping) else None
    if (feature_vector_entry is None or feature_vector_entry.artifact_type != "M0FeatureVectorV2" or
            feature_vector_entry.content_hash != prediction.feature_vector_ref or feature_vector is None or
            feature_vector.content_hash != prediction.feature_vector_ref or
            feature_vector.action_hash != evaluation.action_hash or
            feature_vector.action_artifact_ref != evaluation.action_artifact_ref or
            feature_vector.feature_artifact_ref != evaluation.feature_artifact_ref or
            feature_vector.information_cutoff_ns != evaluation.decision_at_ns or
            feature_vector_entry.available_at_ns > evaluation.available_at_ns):
        raise ValueError("evaluation M0 feature vector does not match original cutoff/action")
    calibration_body = evidence_bodies[evaluation.calibration_ref]
    ood_body = evidence_bodies[evaluation.ood_ref]
    if (calibration_body.get("action_hash") != evaluation.action_hash or
            calibration_body.get("oof_archive_ref") != prediction.oof_archive_ref or
            calibration_body.get("training_cutoff_ns") != prediction.training_cutoff_ns or
            prediction.calibration_ref != evaluation.calibration_ref or
            prediction.ood_ref != evaluation.ood_ref or
            ood_body.get("action_hash") != evaluation.action_hash or
            ood_body.get("feature_vector_ref") != prediction.feature_vector_ref or
            ood_body.get("training_row_refs") != model_body.get("training_row_refs")):
        raise ValueError("evaluation calibration/OOD evidence does not match M0 chronology")
    oof_entry = repo.get_artifact(prediction.oof_archive_ref)
    oof_body = oof_entry.metadata.get("oof_archive") if oof_entry is not None else None
    if (oof_entry is None or oof_entry.artifact_type != "M0OOFResidualArchiveV2" or
            oof_entry.content_hash != prediction.oof_archive_ref or not isinstance(oof_body, Mapping) or
            sha256_json(oof_body) != prediction.oof_archive_ref or
            oof_body.get("version") != M0_RESIDUAL_ARCHIVE_VERSION or
            oof_body.get("feature_schema_version") != feature_vector.schema_version):
        raise ValueError("evaluation chronological OOF archive is unavailable or version-mismatched")
    support_body = evidence_bodies[evaluation.support_ref]
    if (support_body.get("version") != "COMPOSITE_M0_SCENARIO_SUPPORT_V2" or
            support_body.get("action_hash") != evaluation.action_hash):
        raise ValueError("evaluation composite support is not action-bound")
    m0_support_ref = support_body.get("m0_support_ref")
    scenario_support_ref = support_body.get("scenario_support_ref")
    m0_support_entry = repo.get_artifact(str(m0_support_ref))
    m0_support_body = m0_support_entry.metadata.get("support") if m0_support_entry is not None else None
    scenario_support_entry = repo.get_artifact(str(scenario_support_ref))
    scenario_support_body = scenario_support_entry.metadata.get("evidence") if scenario_support_entry is not None else None
    if (m0_support_entry is None or m0_support_entry.artifact_type != "M0SupportV2" or
            m0_support_entry.content_hash != m0_support_ref or not isinstance(m0_support_body, Mapping) or
            sha256_json(m0_support_body) != m0_support_ref or m0_support_body.get("action_hash") != evaluation.action_hash or
            prediction.support_ref != m0_support_ref or
            model_body.get("training_row_refs") != m0_support_body.get("training_outcome_refs") or
            scenario_support_entry is None or scenario_support_entry.artifact_type != "ScenarioSupportV2" or
            scenario_support_entry.content_hash != scenario_support_ref or not isinstance(scenario_support_body, Mapping) or
            sha256_json(scenario_support_body) != scenario_support_ref or
            scenario_support_body.get("action_hash") != evaluation.action_hash or
            scenario_support_body.get("scenario_ref") != evaluation.pretrade_scenario_ref or
            support_body.get("eligible_m0_samples") != m0_support_body.get("eligible_sample_count") or
            support_body.get("independent_scenario_support_units") != scenario_support_body.get("independent_support_unit_count")):
        raise ValueError("evaluation support refs/counts do not resolve to exact M0/scenario evidence")
    assert feature_vector is not None and isinstance(m0_support_body, Mapping)
    assert isinstance(oof_body, Mapping)
    validate_m0_fit_evidence(repo, action=resolved_action, prediction=prediction,
        feature_vector=feature_vector, model_body=model_body, oof_body=oof_body,
        support_body=m0_support_body, calibration_body=calibration_body, ood_body=ood_body)
    scenario_entry = repo.get_artifact(evaluation.pretrade_scenario_ref)
    scenario_body = scenario_entry.metadata.get("scenario") if scenario_entry is not None else None
    if scenario_entry is None or not isinstance(scenario_body, Mapping):
        raise ValueError("evaluation requires typed decision-time pretrade scenario")
    scenario = PretradeExecutionScenarioV2.from_dict(json_value(scenario_body))
    if (scenario_entry.artifact_type != "PretradeExecutionScenarioV2" or
            scenario.action_hash != evaluation.action_hash or scenario.action_artifact_ref != evaluation.action_artifact_ref or
            scenario.content_hash != evaluation.pretrade_scenario_ref or scenario.available_at_ns > evaluation.available_at_ns or
            scenario.information_cutoff_ns != evaluation.decision_at_ns):
        raise ValueError("evaluation scenario belongs to another action or is retrospective")
    validated_payoffs = validate_pretrade_scenario_evidence(repo, action=resolved_action, scenario=scenario)
    causal_inputs = (scenario.model_input, scenario.calibration_input,
        scenario.execution_model_input, *scenario.source_inputs)
    for causal in causal_inputs:
        causal_entry = repo.get_artifact(causal.ref)
        if (causal.kind in {"ReplayPathV2", "PolicyPayoffV2", "PairedPortfolioPayoffV2", "MaturedOutcomeV2"} or
                causal_entry is None or causal_entry.artifact_type != causal.kind or
                causal_entry.content_hash != causal.ref or causal_entry.available_at_ns > evaluation.decision_at_ns or
                causal_entry.created_at_ns > evaluation.decision_at_ns or causal.vintage_at_ns > evaluation.decision_at_ns or
                sha256_json(causal_entry.metadata) != causal.ref):
            raise ValueError("evaluation scenario has future or retrospective causal inputs")
    distribution_body = evidence_bodies[evaluation.outcome_distribution_ref]
    if (distribution_body.get("version") != "PRETRADE_OUTCOME_DISTRIBUTION_V2" or
            distribution_body.get("action_hash") != evaluation.action_hash or
            distribution_body.get("scenario_ref") != evaluation.pretrade_scenario_ref):
        raise ValueError("evaluation outcome distribution is not bound to exact action/scenario")
    outcome_distribution = OutcomeDistributionV2.from_dict(json_value(distribution_body))
    if outcome_distribution.status == "AVAILABLE" and (
            outcome_distribution.path_ids != tuple(row[0] for row in scenario.rows) or
            outcome_distribution.probabilities != tuple(row[1] for row in scenario.rows)):
        raise ValueError("evaluation outcome distribution is not bound to exact scenario paths")
    payoff_refs = distribution_body.get("payoff_refs")
    expected_payoffs = len(scenario.rows) if outcome_distribution.status == "AVAILABLE" else 0
    if not isinstance(payoff_refs, (list, tuple)) or len(payoff_refs) != expected_payoffs:
        raise ValueError("evaluation outcome distribution path payoffs are incomplete")
    if (outcome_distribution.status == "AVAILABLE" and
            tuple(payoff_refs) != tuple(payoff.content_hash for payoff in validated_payoffs)):
        raise ValueError("evaluation outcome distribution payoff refs disagree with exact scenario cashflows")
    for (path_id, _, _), payoff_ref in zip(scenario.rows, payoff_refs, strict=True):
        payoff_entry = repo.get_artifact(payoff_ref)
        payoff_body = payoff_entry.metadata.get("path_payoff") if payoff_entry is not None else None
        if (payoff_entry is None or payoff_entry.artifact_type != "PretradePathPayoffV2" or
                payoff_entry.content_hash != payoff_ref or not isinstance(payoff_body, Mapping) or
                sha256_json(payoff_body) != payoff_ref or payoff_body.get("action_hash") != evaluation.action_hash or
                payoff_body.get("scenario_artifact_ref") != evaluation.pretrade_scenario_ref or
                payoff_body.get("joint_path_id") != path_id or
                payoff_entry.available_at_ns > evaluation.available_at_ns):
            raise ValueError("evaluation outcome distribution contains wrong-action/retrospective payoff")
    stress_entry = repo.get_artifact(evaluation.deterministic_stress_ref)
    stress_body = stress_entry.metadata.get("evidence") if stress_entry is not None else None
    if (stress_entry is None or stress_entry.artifact_type != "DeterministicStressV2" or
            not isinstance(stress_body, Mapping) or stress_entry.content_hash != evaluation.deterministic_stress_ref or
            sha256_json(stress_body) != evaluation.deterministic_stress_ref or
            stress_body.get("action_hash") != evaluation.action_hash or
            stress_body.get("action_artifact_ref") != evaluation.action_artifact_ref or
            stress_body.get("quantity") != canonical_decimal_str(evaluation.quantity) or
            stress_body.get("risk_policy_ref") != evaluation.risk_policy_ref or
            stress_body.get("risk_policy_hash") != evaluation.risk_policy_hash or
            stress_body.get("cutoff_ns") != evaluation.decision_at_ns or
            stress_entry.available_at_ns > evaluation.available_at_ns):
        raise ValueError("evaluation deterministic stress action/quantity mismatch")
    portfolio_entry = repo.get_artifact(evaluation.existing_portfolio_ref)
    portfolio_body = portfolio_entry.metadata.get("evidence") if portfolio_entry is not None else None
    if (portfolio_entry is None or portfolio_entry.artifact_type != "DecisionTimePortfolioScenariosV2" or
            not isinstance(portfolio_body, Mapping) or portfolio_entry.content_hash != evaluation.existing_portfolio_ref or
            portfolio_body.get("version") != "DECISION_TIME_PORTFOLIO_SCENARIOS_V2" or
            sha256_json(portfolio_body) != evaluation.existing_portfolio_ref or
            portfolio_body.get("action_hash") != evaluation.action_hash or
            portfolio_body.get("candidate_scenario_ref") != evaluation.pretrade_scenario_ref or
            portfolio_entry.available_at_ns > evaluation.available_at_ns):
        raise ValueError("evaluation synchronized decision-time portfolio paths mismatch")
    completeness_ref = portfolio_body.get("completeness_ref")
    completeness_entry = repo.get_artifact(str(completeness_ref))
    completeness_body = completeness_entry.metadata.get("evidence") if completeness_entry is not None else None
    if (completeness_entry is None or completeness_entry.artifact_type != "DecisionTimePortfolioCompletenessV2" or
            completeness_entry.content_hash != completeness_ref or not isinstance(completeness_body, Mapping) or
            sha256_json(completeness_body) != completeness_ref or
            completeness_body.get("version") != "DECISION_TIME_PORTFOLIO_COMPLETENESS_V1" or
            completeness_body.get("account_snapshot_ref") != evaluation.account_snapshot_ref or
            completeness_body.get("common_scenario_set_id") != portfolio_body.get("common_scenario_set_id") or
            completeness_body.get("existing_exposure_refs") != portfolio_body.get("existing_exposure_refs") or
            completeness_body.get("pending_exposure_refs") != portfolio_body.get("pending_exposure_refs") or
            bool(completeness_body.get("unknown_exposure_refs")) != portfolio_body.get("unknown_exposure_present") or
            completeness_entry.available_at_ns > evaluation.decision_at_ns):
        raise ValueError("evaluation portfolio completeness evidence is missing or mismatched")
    if (account_body.get("existing_exposure_refs") != completeness_body.get("existing_exposure_refs") or
            account_body.get("pending_risk_refs") != completeness_body.get("pending_exposure_refs") or
            account_body.get("eligible_equity") != completeness_body.get("eligible_equity") or
            account_body.get("drawdown") != completeness_body.get("drawdown") or
            (completeness_body.get("status") == "COMPLETE" and
                completeness_body.get("common_path_ids") != [row[0] for row in
                    PretradeExecutionScenarioV2.from_dict(json_value(scenario_body)).rows])):
        raise ValueError("evaluation portfolio completeness does not match account/path inventory")
    portfolio_paths = tuple(PortfolioPathV2(row["common_path_id"],
        decimal_value(row["probability"], field="probability", wire=True),
        decimal_value(row["existing_net_pnl"], field="existing_net_pnl", wire=True),
        decimal_value(row["candidate_net_pnl"], field="candidate_net_pnl", wire=True),
        row["candidate_payoff_ref"], row["candidate_exit_cash_retained_to_24h"], row["existing_valuation_ref"])
        for row in portfolio_body["paths"])
    portfolio = DecisionTimePortfolioScenariosV2(
        portfolio_body["action_hash"], portfolio_body["action_artifact_ref"],
        portfolio_body["candidate_scenario_ref"], portfolio_body["common_scenario_set_id"],
        portfolio_body["account_snapshot_ref"], portfolio_body["completeness_ref"],
        tuple(portfolio_body["existing_exposure_refs"]), tuple(portfolio_body["pending_exposure_refs"]),
        portfolio_body["unknown_exposure_present"], portfolio_body["horizon_start_ns"],
        portfolio_body["horizon_end_ns"], decimal_value(portfolio_body["equity"], field="equity", wire=True),
        decimal_value(portfolio_body["drawdown"], field="drawdown", wire=True), portfolio_paths,
        portfolio_body["status"], portfolio_body.get("reason"))
    if (portfolio.equity != decimal_value(account_body["eligible_equity"], field="eligible_equity", wire=True) or
            portfolio.drawdown != decimal_value(account_body["drawdown"], field="drawdown", wire=True) or
            portfolio.common_scenario_set_id != scenario.common_scenario_set_id or
            portfolio.horizon_start_ns != evaluation.decision_at_ns):
        raise ValueError("portfolio equity/drawdown/common horizon differs from cutoff account/scenario")
    stress_body = evidence_bodies[evaluation.deterministic_stress_ref]
    stress = DeterministicStressV2(**{
        **{name: stress_body[name] for name in DeterministicStressV2.__dataclass_fields__ if name != "version"},
        "quantity": decimal_value(stress_body["quantity"], field="quantity", wire=True),
        "eligible_equity": decimal_value(stress_body["eligible_equity"], field="eligible_equity", wire=True)
            if stress_body["eligible_equity"] is not None else None,
        "drawdown": decimal_value(stress_body["drawdown"], field="drawdown", wire=True)
            if stress_body["drawdown"] is not None else None,
        "loss_limit": decimal_value(stress_body["loss_limit"], field="loss_limit", wire=True)
            if stress_body["loss_limit"] is not None else None,
        "maximum_loss": decimal_value(stress_body["maximum_loss"], field="maximum_loss", wire=True)
            if stress_body["maximum_loss"] is not None else None,
        "required_case_refs": tuple(stress_body["required_case_refs"]),
        "reasons": tuple(stress_body["reasons"]),
    })
    m0_support = M0SupportV2(
        m0_support_body["action_hash"], m0_support_body["information_cutoff_ns"],
        m0_support_body["eligible_sample_count"], m0_support_body["independent_support_count"],
        m0_support_body["compatible_policy_count"], tuple(tuple(x) for x in m0_support_body["provenance_counts"]),
        tuple(tuple(x) for x in m0_support_body["execution_state_counts"]),
        tuple((x[0], decimal_value(x[1], field="missing_coverage", wire=True))
            for x in m0_support_body["missing_feature_coverage"]),
        m0_support_body["training_start_ns"], m0_support_body["training_end_ns"],
        tuple(m0_support_body["training_outcome_refs"]), m0_support_body["compatibility_key"],
        m0_support_body["evidence_quality"])
    calibration = M0CalibrationV2(calibration_body["action_hash"], calibration_body["training_cutoff_ns"],
        calibration_body["oof_archive_ref"], calibration_body["chronological_oof_count"],
        decimal_value(calibration_body["absolute_residual_q90"], field="absolute_residual_q90", wire=True)
            if calibration_body["absolute_residual_q90"] is not None else None,
        calibration_body["status"], calibration_body.get("reason"))
    ood = M0OODV2(ood_body["action_hash"], ood_body["feature_vector_ref"],
        tuple(ood_body["training_row_refs"]), decimal_value(ood_body["robust_z_limit"], field="robust_z_limit", wire=True),
        decimal_value(ood_body["maximum_absolute_robust_z"], field="maximum_absolute_robust_z", wire=True)
            if ood_body["maximum_absolute_robust_z"] is not None else None,
        ood_body["out_of_distribution"], ood_body["status"])
    scenario_support = ScenarioSupportV2.from_dict(json_value(scenario_support_body))
    estimation_body = evidence_bodies[evaluation.estimation_uncertainty_ref]
    estimation = EstimationUncertaintyV2(estimation_body["action_hash"], estimation_body["prediction_ref"],
        estimation_body["oof_archive_ref"], tuple(estimation_body["training_outcome_refs"]),
        decimal_value(estimation_body["standard_error"], field="standard_error", wire=True)
            if estimation_body["standard_error"] is not None else None,
        decimal_value(estimation_body["confidence_multiplier"], field="confidence_multiplier", wire=True),
        decimal_value(estimation_body["uncertainty_amount"], field="uncertainty_amount", wire=True)
            if estimation_body["uncertainty_amount"] is not None else None, estimation_body["status"])
    execution_body = evidence_bodies[evaluation.execution_uncertainty_ref]
    execution = ExecutionModelUncertaintyV2(execution_body["action_hash"], execution_body["execution_model_ref"],
        tuple(execution_body["residual_refs"]), execution_body["independent_support_count"],
        decimal_value(execution_body["absolute_cost_error_q90"], field="absolute_cost_error_q90", wire=True)
            if execution_body["absolute_cost_error_q90"] is not None else None, execution_body["status"])
    numerical_body = evidence_bodies[evaluation.numerical_error_ref]
    numerical = NumericalErrorV2.from_dict(json_value(numerical_body))
    lcb_method = LCBMethodV2.from_dict(json_value(evidence_bodies[evaluation.lcb_method_ref]))
    if lcb_method.content_hash != evaluation.lcb_method_ref:
        raise ValueError("evaluation lower-bound method ref/content mismatch")
    if (execution.action_hash != evaluation.action_hash or
            execution.execution_model_ref != scenario.execution_model_input.ref):
        raise ValueError("execution uncertainty is not bound to the exact pretrade execution model")
    execution_residuals: list[ExecutionCalibrationResidualV2] = []
    for residual_ref in execution.residual_refs:
        residual_entry = repo.get_artifact(residual_ref)
        residual_body = residual_entry.metadata.get("residual") if residual_entry is not None else None
        if (residual_entry is None or residual_entry.artifact_type != "ExecutionCalibrationResidualV2" or
                residual_entry.content_hash != residual_ref or not isinstance(residual_body, Mapping) or
                sha256_json(residual_body) != residual_ref or residual_entry.available_at_ns > evaluation.decision_at_ns):
            raise ValueError("execution uncertainty residual is missing, future or hash-mismatched")
        residual = ExecutionCalibrationResidualV2.from_dict(json_value(residual_body), evidence_ref=residual_ref)
        if (residual.provenance == "UNVERIFIED" or not residual.source_refs or
                residual.available_at_ns > evaluation.decision_at_ns or
                residual.action_compatibility_key != m0_support.compatibility_key):
            raise ValueError("execution calibration residual lacks eligible causal support")
        for source_ref in residual.source_refs:
            source_entry = repo.get_artifact(source_ref)
            if (source_entry is None or source_entry.content_hash != source_ref or
                    source_entry.available_at_ns > residual.available_at_ns or
                    source_entry.created_at_ns > residual.available_at_ns):
                raise ValueError("execution calibration source is unavailable by residual time")
        execution_residuals.append(residual)
    reproduced_execution = make_execution_uncertainty(evaluation.action_hash,
        scenario.execution_model_input.ref, execution_residuals,
        compatibility_key=m0_support.compatibility_key, cutoff_ns=evaluation.decision_at_ns,
        minimum_support=admission_policy.minimum_execution_calibration)
    if canonical_json(reproduced_execution.to_dict()) != canonical_json(execution.to_dict()):
        raise ValueError("execution uncertainty does not reproduce from typed chronological residuals")
    reproduced_estimation = make_estimation_uncertainty(prediction,
        training_refs=m0_support.training_outcome_refs,
        confidence_multiplier=admission_policy.confidence_multiplier)
    if canonical_json(reproduced_estimation.to_dict()) != canonical_json(estimation.to_dict()):
        raise ValueError("estimation uncertainty does not reproduce from exact M0 evidence/policy")
    reproduced_scenario_support = make_scenario_support(repo, action=resolved_action, scenario=scenario,
        support_unit_refs=scenario_support.support_unit_refs)
    if canonical_json(reproduced_scenario_support.to_dict()) != canonical_json(scenario_support.to_dict()):
        raise ValueError("scenario support does not reproduce from coherent template evidence")
    reproduced_support = make_inference_support(evaluation.action_hash, m0_support, scenario_support)
    if canonical_json(reproduced_support.to_dict()) != canonical_json(support_body):
        raise ValueError("composite support does not reproduce from M0/scenario evidence")
    reproduced_distribution = make_outcome_distribution(scenario, validated_payoffs)
    if canonical_json(reproduced_distribution.to_dict()) != canonical_json(outcome_distribution.to_dict()):
        raise ValueError("outcome distribution does not reproduce from exact path cashflows")
    if (numerical.action_hash != evaluation.action_hash or numerical.scenario_ref != scenario.content_hash or
            numerical.prediction_ref != prediction.content_hash or
            numerical.m0_conversion_error != prediction.numerical_conversion_error):
        raise ValueError("numerical error does not bind exact action/scenario/M0 conversion")
    reproduced_numerical = make_numerical_error(repo, action=resolved_action, scenario=scenario,
        prediction=prediction, run_a_ref=numerical.run_a_ref, run_b_ref=numerical.run_b_ref)
    if canonical_json(reproduced_numerical.to_dict()) != canonical_json(numerical.to_dict()):
        raise ValueError("numerical convergence evidence does not reproduce from indexed scenario/payoff runs")
    stress_source = repo.get_artifact(stress.stress_evidence_ref) if stress.stress_evidence_ref is not None else None
    raw_stress_input = stress_source.metadata.get("stress_input") if stress_source is not None else None
    stress_input = stress_input_from_wire(json_value(raw_stress_input)) if isinstance(raw_stress_input, Mapping) else None
    product_entry = repo.get_artifact(resolved_action.action.product_ref)
    product_body = product_entry.metadata.get("product") if product_entry is not None else None
    if not isinstance(product_body, Mapping):
        raise ValueError("deterministic stress needs exact product units")
    reproduced_stress = evaluate_deterministic_stress(repo, action=resolved_action, risk_policy=risk_policy,
        risk_policy_ref=evaluation.risk_policy_ref,
        eligible_equity=decimal_value(account_body["eligible_equity"], field="eligible_equity", wire=True),
        drawdown=decimal_value(account_body["drawdown"], field="drawdown", wire=True),
        product_base_units=decimal_value(product_body["base_units_per_contract"], field="base_units_per_contract", wire=True),
        stress_input=stress_input, stress_evidence_ref=stress.stress_evidence_ref, cutoff_ns=evaluation.decision_at_ns)
    if canonical_json(reproduced_stress.to_dict()) != canonical_json(stress.to_dict()):
        raise ValueError("deterministic stress result does not reproduce from exact input/risk/account evidence")
    payoff_by_path = {payoff.joint_path_id: payoff for payoff in validated_payoffs}
    if portfolio.status == "AVAILABLE":
        if (tuple((path.common_path_id, path.probability) for path in portfolio.paths) !=
                tuple((path_id, probability) for path_id, probability, _ in scenario.rows) or
                portfolio.horizon_start_ns != evaluation.decision_at_ns):
            raise ValueError("portfolio uses incompatible common paths/probabilities/horizon")
        for path in portfolio.paths:
            payoff = payoff_by_path.get(path.common_path_id)
            if (payoff is None or path.candidate_payoff_ref != payoff.content_hash or
                    path.candidate_net_pnl != payoff.net_payoff):
                raise ValueError("portfolio candidate cash differs from exact path payoff")
            exposure_refs = tuple(sorted(set(portfolio.existing_exposure_refs + portfolio.pending_exposure_refs)))
            if not exposure_refs:
                if path.existing_net_pnl != ZERO or path.existing_valuation_ref is not None:
                    raise ValueError("complete flat portfolio must have zero existing path value")
                continue
            existing_entry = repo.get_artifact(path.existing_valuation_ref) if path.existing_valuation_ref else None
            existing_body = existing_entry.metadata.get("evidence") if existing_entry is not None else None
            if (existing_entry is None or existing_entry.artifact_type != "ExistingPortfolioPathV2" or
                    not isinstance(existing_body, Mapping) or existing_entry.available_at_ns > evaluation.available_at_ns):
                raise ValueError("portfolio existing/pending valuation artifact missing or future")
            existing = ExistingPortfolioPathV2.from_dict(json_value(existing_body))
            if (existing.content_hash != path.existing_valuation_ref or existing_entry.content_hash != existing.content_hash or
                    existing.common_path_id != path.common_path_id or existing.probability != path.probability or
                    existing.common_scenario_set_id != scenario.common_scenario_set_id or
                    existing.existing_net_pnl != path.existing_net_pnl or existing.exposure_refs != exposure_refs or
                    existing.information_cutoff_ns != evaluation.decision_at_ns or
                    existing.horizon_end_ns != portfolio.horizon_end_ns):
                raise ValueError("portfolio existing/pending value differs from exact common-path evidence")
            index_existing_portfolio_path(repo, existing)
    portfolio_es = make_portfolio_es(portfolio, risk_policy=risk_policy,
        risk_policy_ref=evaluation.risk_policy_ref)
    result = decide_admission(action=resolved_action, prediction=prediction, m0_support=m0_support,
        calibration=calibration, ood=ood, scenario=scenario, scenario_support=scenario_support,
        outcome_distribution=outcome_distribution,
        estimation=estimation, execution=execution, numerical=numerical, stress=stress,
        portfolio=portfolio_es, policy=admission_policy, capability=capability,
        account_scope=str(expected_account_scope) if expected_account_scope is not None else None,
        allow_synthetic_fixtures=allow_synthetic_fixtures)
    if (result.decision != evaluation.decision or result.reasons != evaluation.reason_codes or
            result.expected_net_value != evaluation.expected_net_value or result.lcb != evaluation.expected_pnl_lcb or
            portfolio_es.es_before_fraction != evaluation.es_before or
            portfolio_es.es_after_fraction != evaluation.es_after):
        raise ValueError("amended evaluation decision/LCB/ES does not reproduce its indexed evidence")
    body = evaluation.to_dict()
    ref = evaluation.content_hash
    repo.register_artifact(ArtifactIndexEntryV2(ref, "EvaluationArtifactV2", ref,
        evaluation.decision_at_ns, evaluation.available_at_ns, {"evaluation": body}))
    return ref


def persist_economic_decision(repo: OpsRepository, evaluation: AmendedEvaluationArtifactV2,
        *, policy_id: str, policy_version: str, created_at_ns: int) -> tuple[str, str]:
    """Persist amended evaluation first, then its one terminal economic calendar row."""
    evaluation_ref = index_amended_evaluation(repo, evaluation)
    admission = {
        DecisionStatusV2.CANDIDATE: AdmissionStateV2.CANDIDATE,
        DecisionStatusV2.NO_TRADE: AdmissionStateV2.NO_TRADE,
        DecisionStatusV2.NOT_ESTIMABLE: AdmissionStateV2.NOT_ESTIMABLE,
    }[evaluation.decision]
    calendar = DecisionCalendarEntryV2(
        evaluation.candidate_set_ref, evaluation.candidate_ref, policy_id, policy_version,
        evaluation.policy_hash, evaluation.decision_at_ns, SelectionStateV2.SELECTED, admission,
        evaluation.action_hash, evaluation.action_artifact_ref, DecisionSourceStageV2.ECONOMIC_EVALUATION,
        evaluation.reason_codes, evaluation_ref, created_at_ns, evaluation.available_at_ns)
    calendar_ref = index_decision_calendar_entry(repo, calendar)
    return evaluation_ref, calendar_ref


class VenueCapabilityStatusV2(StrEnum):
    UNVERIFIED = "UNVERIFIED"
    SUPPORTED = "SUPPORTED"
    UNSUPPORTED = "UNSUPPORTED"
    EXPIRED = "EXPIRED"


@dataclass(frozen=True)
class VenueCapabilitySnapshotV2:
    """Immutable qualification evidence scoped to one exact venue/profile/action."""

    venue: VenueV2
    environment: EnvironmentV2
    account_scope: str
    product_ref: str
    instrument_key_ref: str
    margin_mode: str
    position_mode: str
    nautilus_distribution: str
    nautilus_version: str
    nautilus_source_commit: str
    nautilus_artifact_ref: str
    execution_profile_ref: str
    protection_profile_ref: str
    qualification_version: str
    observed_status: VenueCapabilityStatusV2
    evidence_refs: tuple[str, ...]
    available_at_ns: int
    synthetic_fixture: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "venue", VenueV2(self.venue))
        object.__setattr__(self, "environment", EnvironmentV2(self.environment))
        object.__setattr__(self, "observed_status", VenueCapabilityStatusV2(self.observed_status))
        for name in ("product_ref", "instrument_key_ref", "nautilus_artifact_ref",
                "execution_profile_ref", "protection_profile_ref"):
            sha256_ref(getattr(self, name), field=name)
        for name in ("account_scope", "margin_mode", "position_mode", "nautilus_distribution",
                "nautilus_version", "nautilus_source_commit", "qualification_version"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip() or value.strip().upper() in {
                    "REQUIRED", "UNVERIFIED", "TEST_GATE", "PENDING"}:
                raise ValueError(f"capability {name} must identify an observed profile")
        if self.evidence_refs != tuple(sorted(set(self.evidence_refs))):
            raise ValueError("capability evidence refs must be sorted unique")
        for ref in self.evidence_refs:
            sha256_ref(ref, field="capability_evidence_ref")
        if type(self.available_at_ns) is not int or self.available_at_ns < 0 or type(self.synthetic_fixture) is not bool:
            raise ValueError("capability availability/fixture marker invalid")
        if self.observed_status == VenueCapabilityStatusV2.SUPPORTED and not self.evidence_refs:
            raise ValueError("supported capability requires immutable qualification evidence refs")
        if self.synthetic_fixture and self.observed_status != VenueCapabilityStatusV2.SUPPORTED:
            raise ValueError("synthetic fixture capability must explicitly model supported status")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": "VENUE_CAPABILITY_EVIDENCE_V2_V1", **{
            name: getattr(self, name) for name in self.__dataclass_fields__}})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> VenueCapabilitySnapshotV2:
        fields = set(cls.__dataclass_fields__) | {"version"}
        d = dict(strict_fields(data, expected=fields, required=fields, name="VenueCapabilitySnapshotV2"))
        if d.pop("version") != "VENUE_CAPABILITY_EVIDENCE_V2_V1" or not isinstance(d["evidence_refs"], list):
            raise ValueError("unsupported venue capability evidence wire")
        d["evidence_refs"] = tuple(d["evidence_refs"])
        return cls(**{name: d[name] for name in cls.__dataclass_fields__})

    def supports(self, action: ActionArtifactV2, *, account_scope: str, cutoff_ns: int,
            policy: AdmissionPolicyV2, allow_synthetic_fixtures: bool = False) -> bool:
        return (self.observed_status == VenueCapabilityStatusV2.SUPPORTED and
            self.venue == action.action.key.venue and self.environment == action.action.key.environment and
            self.product_ref == action.action.product_ref and
            self.instrument_key_ref == action.action.key.content_hash and
            self.account_scope == account_scope and self.available_at_ns <= cutoff_ns and
            self.margin_mode == policy.required_margin_mode and
            self.position_mode == policy.required_position_mode and
            self.nautilus_distribution == policy.required_nautilus_distribution and
            self.nautilus_version == policy.required_nautilus_version and
            self.nautilus_source_commit == policy.required_nautilus_source_commit and
            self.nautilus_artifact_ref == policy.required_nautilus_artifact_ref and
            self.execution_profile_ref == policy.required_execution_profile_ref and
            self.protection_profile_ref == policy.required_protection_profile_ref and
            self.qualification_version == policy.required_qualification_version and
            bool(self.evidence_refs) and (allow_synthetic_fixtures or not self.synthetic_fixture))


@dataclass(frozen=True)
class ReservationSnapshotV2:
    account_scope: str
    available_at_ns: int
    snapshot_version: int
    synthetic_fixture: bool

    def __post_init__(self) -> None:
        if (not self.account_scope or type(self.available_at_ns) is not int or self.available_at_ns < 0 or
                type(self.snapshot_version) is not int or self.snapshot_version < 0):
            raise ValueError("read-only reservation snapshot invalid")
        if type(self.synthetic_fixture) is not bool:
            raise ValueError("reservation fixture flag must be boolean")

    def to_dict(self) -> dict[str, Any]:
        return {"version": "READ_ONLY_RESERVATION_SNAPSHOT_V1", **{name: getattr(self, name) for name in self.__dataclass_fields__}}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def index_shadow_plan_snapshots(repo: OpsRepository, *, capability: VenueCapabilitySnapshotV2,
                                reservation: ReservationSnapshotV2) -> tuple[str, str]:
    index_venue_capability_snapshot(repo, capability)
    repo.register_artifact(ArtifactIndexEntryV2(reservation.content_hash, "ReservationSnapshotV2",
        reservation.content_hash, reservation.available_at_ns, reservation.available_at_ns,
        {"reservation": reservation.to_dict()}))
    return capability.content_hash, reservation.content_hash


def index_venue_capability_snapshot(repo: OpsRepository, capability: VenueCapabilitySnapshotV2) -> str:
    _validate_capability_sources(repo, capability, cutoff_ns=capability.available_at_ns)
    repo.register_artifact(ArtifactIndexEntryV2(capability.content_hash, "VenueCapabilitySnapshotV2",
        capability.content_hash, capability.available_at_ns, capability.available_at_ns,
        {"capability": capability.to_dict()}))
    return capability.content_hash


def _validate_capability_sources(repo: OpsRepository, capability: VenueCapabilitySnapshotV2,
        *, cutoff_ns: int, action: ActionArtifactV2 | None = None) -> None:
    if capability.observed_status != VenueCapabilityStatusV2.SUPPORTED:
        return
    if capability.synthetic_fixture:
        for ref in capability.evidence_refs:
            source = repo.get_artifact(ref)
            if (source is None or source.content_hash != ref or source.available_at_ns > cutoff_ns or
                    source.created_at_ns > cutoff_ns or
                    not source.artifact_type.startswith("SyntheticVenueCapability")):
                raise ValueError("synthetic capability ref is missing, future or not fixture evidence")
        return
    if not capability.evidence_refs:
        raise ValueError("real supported capability needs a qualified V1 capability manifest ref")
    manifests = []
    for ref in capability.evidence_refs:
        source = repo.get_artifact(ref)
        body = source.metadata.get("evidence") if source is not None else None
        if (source is None or source.artifact_type != "CapabilityContractV1" or
                source.content_hash != ref or source.available_at_ns > cutoff_ns or
                source.created_at_ns > cutoff_ns or not isinstance(body, Mapping) or
                sha256_json(body) != ref):
            raise ValueError("real capability refs must resolve to an exact indexed CapabilityContractV1")
        try:
            manifest = capability_contract_from_manifest(dict(body))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("real capability manifest is malformed") from exc
        venue = manifest.venue
        runtime = manifest.runtime
        normalized_margin = capability.margin_mode.casefold().replace("_", "-")
        observed_margin = venue.account_generation_and_margin_mode.casefold().replace("_", "-")
        if (not manifest.capabilities.all_supported() or manifest.assisted_blockers() or
                venue.environment.casefold() != capability.environment.value.casefold() or
                venue.account_identity_hash != capability.account_scope or
                venue.product.casefold() != "linear" or
                venue.position_mode.casefold().replace("-", "_") != capability.position_mode.casefold().replace("-", "_") or
                normalized_margin not in observed_margin or
                runtime.distribution != capability.nautilus_distribution or
                runtime.version != capability.nautilus_version or
                runtime.source_commit != capability.nautilus_source_commit or
                runtime.installed_artifact_sha256 != capability.nautilus_artifact_ref or
                manifest.contract_version != capability.qualification_version):
            raise ValueError("real V1 capability manifest does not qualify the exact V2 account/runtime profile")
        if action is not None and (action.action.key.native_symbol not in venue.supported_symbols or
                action.action.key.environment.value.casefold() != venue.environment.casefold() or
                capability.instrument_key_ref != action.action.key.content_hash or
                capability.product_ref != action.action.product_ref):
            raise ValueError("real capability manifest does not cover the exact action symbol/product")
        manifests.append(manifest)
    if not manifests:
        raise ValueError("real supported capability has no qualified capability manifest")


def validate_venue_capability_snapshot(repo: OpsRepository, capability: VenueCapabilitySnapshotV2,
        *, cutoff_ns: int, action: ActionArtifactV2 | None = None) -> None:
    entry = repo.get_artifact(capability.content_hash)
    body = entry.metadata.get("capability") if entry is not None else None
    if (entry is None or entry.artifact_type != "VenueCapabilitySnapshotV2" or
            entry.content_hash != capability.content_hash or entry.available_at_ns > cutoff_ns or
            not isinstance(body, Mapping) or sha256_json(body) != capability.content_hash or
            canonical_json(body) != canonical_json(capability.to_dict())):
        raise ValueError("exact indexed venue capability evidence is required")
    _validate_capability_sources(repo, capability, cutoff_ns=cutoff_ns, action=action)


def create_shadow_trade_plan(repo: OpsRepository, *, action: ActionArtifactV2,
        evaluation: AmendedEvaluationArtifactV2, capability: VenueCapabilitySnapshotV2,
        reservation: ReservationSnapshotV2, allow_synthetic_fixtures: bool = False) -> TradePlanEnvelopeV2 | None:
    """Copy frozen risk sizing fields verbatim into a shadow-only TradePlan."""
    if evaluation.decision != DecisionStatusV2.CANDIDATE:
        return None
    if (action.action.action_hash != evaluation.action_hash or action.content_hash != evaluation.action_artifact_ref or
            action.action.quantity != evaluation.quantity or capability.product_ref != action.action.product_ref or
            evaluation.capability_evidence_ref != capability.content_hash or
            capability.account_scope != reservation.account_scope or capability.available_at_ns > evaluation.decision_at_ns or
            reservation.available_at_ns > evaluation.available_at_ns):
        raise ValueError("shadow plan identity/capability/reservation mismatch")
    evaluation_entry = repo.get_artifact(evaluation.content_hash)
    evaluation_body = evaluation_entry.metadata.get("evaluation") if evaluation_entry is not None else None
    if (evaluation_entry is None or evaluation_entry.artifact_type != "EvaluationArtifactV2" or
            not isinstance(evaluation_body, Mapping) or evaluation_entry.content_hash != evaluation.content_hash or
            canonical_json(evaluation_body) != canonical_json(evaluation.to_dict())):
        raise ValueError("final amended EvaluationArtifact must be durable before shadow plan creation")
    policy_entry = repo.get_artifact(evaluation.admission_policy_ref)
    policy_body = policy_entry.metadata.get("evidence") if policy_entry is not None else None
    admission_policy = AdmissionPolicyV2.from_dict(json_value(policy_body)) if isinstance(policy_body, Mapping) else None
    if (admission_policy is None or not capability.supports(action,
            account_scope=capability.account_scope, cutoff_ns=evaluation.decision_at_ns,
            policy=admission_policy, allow_synthetic_fixtures=allow_synthetic_fixtures)):
        return None
    if (capability.synthetic_fixture != reservation.synthetic_fixture and
            (capability.synthetic_fixture or reservation.synthetic_fixture)):
        raise ValueError("synthetic plan evidence must be explicitly marked together")
    if capability.synthetic_fixture and not allow_synthetic_fixtures:
        return None
    if not capability.synthetic_fixture:
        # A SHA-shaped indexed summary is insufficient for a real shadow plan.
        # Reproduce all amended economic gates from their durable evidence.
        index_amended_evaluation(repo, evaluation)
    else:
        validate_venue_capability_snapshot(repo, capability, cutoff_ns=evaluation.decision_at_ns,
            action=action)
    candidate_entry = repo.get_artifact(action.candidate_ref)
    candidate_body = candidate_entry.metadata.get("candidate") if candidate_entry is not None else None
    candidate = CandidateActionV2.from_dict(json_value(candidate_body)) if isinstance(candidate_body, Mapping) else None
    sizing_entry = repo.get_artifact(action.sizing_ref)
    sizing_body = sizing_entry.metadata.get("sizing") if sizing_entry is not None else None
    if (candidate is None or candidate.account_scope is None or
            capability.account_scope != candidate.account_scope or sizing_entry is None or not isinstance(sizing_body, Mapping) or
            sizing_entry.artifact_type != "SizingDecisionV2" or sha256_json(sizing_body) != action.sizing_ref or
            sizing_body.get("status") != "SIZED" or sizing_body.get("quantity") != canonical_decimal_str(action.action.quantity) or
            sizing_body.get("product_ref") != action.action.product_ref or
            sizing_body.get("risk_policy_hash") != action.action.risk_policy_hash or
            sizing_body.get("risk_policy_v2_hash") != action.action.risk_policy_v2_hash or
            sizing_body.get("account_snapshot_ref") != evaluation.account_snapshot_ref or
            sizing_body.get("candidate_ref") != action.candidate_ref or
            sizing_body.get("candidate_set_ref") != action.candidate_set_ref):
        raise ValueError("shadow plan requires exact selected account and hard-risk sizing evidence")
    for ref, kind, body, key in ((capability.content_hash, "VenueCapabilitySnapshotV2", capability.to_dict(), "capability"),
                            (reservation.content_hash, "ReservationSnapshotV2", reservation.to_dict(), "reservation")):
        if sha256_json(body) != ref:
            raise ValueError(f"invalid {kind}")
        existing = repo.get_artifact(ref)
        if (existing is None or existing.artifact_type != kind or existing.content_hash != ref or
                existing.available_at_ns > evaluation.available_at_ns or
                canonical_json(existing.metadata.get(key)) != canonical_json(body)):
            raise ValueError(f"indexed {kind} required")
    evaluation_ref = evaluation.content_hash
    plan_id = sha256_json({"version": TRADE_PLAN_VERSION_V2, "action_hash": action.action.action_hash,
                           "evaluation_ref": evaluation_ref})
    input_refs = tuple(sorted({action.content_hash, action.sizing_ref, evaluation_ref,
        capability.content_hash, reservation.content_hash, evaluation.risk_policy_ref, evaluation.risk_policy_v2_ref}))
    envelope = ArtifactEnvelope(1, plan_id, evaluation.available_at_ns, evaluation.available_at_ns,
        "ATLAS_V2_SHADOW_TRADE_PLAN_V1", input_refs)
    plan = TradePlanEnvelopeV2(envelope, plan_id, TRADE_PLAN_VERSION_V2, TRADE_PLAN_VERSION_V2,
        action.action.key, action.action.product_ref, candidate.account_scope, action.action.policy_hash,
        action.action.action_hash, evaluation_ref, action.action.risk_policy_hash, capability.content_hash,
        reservation.snapshot_version, V2Side(action.action.side), action.action.quantity,
        canonical_json({"entry_rule": action.action.entry_rule.to_dict(),
            "trigger_basis": action.action.entry_trigger_basis}),
        action.action.entry_collar, action.action.stop_price,
        action.action.stop_trigger_basis, canonical_json(action.action.management_rule.to_dict()),
        action.action.horizon_end_ns, decimal_value(sizing_body["normal_risk"], field="normal_risk", wire=True),
        decimal_value(sizing_body["stress_risk"], field="stress_risk", wire=True),
        decimal_value(sizing_body["margin"], field="margin", wire=True),
        decimal_value(sizing_body["leverage"], field="leverage", wire=True), action.action.entry_reference,
        evaluation.action_expiry_ns)
    repo.register_artifact(ArtifactIndexEntryV2(plan.content_hash, "TradePlanEnvelopeV2", plan.content_hash,
        evaluation.available_at_ns, evaluation.available_at_ns, {"plan": plan.to_dict(),
        "shadow_plan_evidence": {"capability_ref": capability.content_hash,
            "reservation_snapshot_ref": reservation.content_hash, "risk_policy_v2_ref": evaluation.risk_policy_v2_ref,
            "shadow_read_only": True, "synthetic_fixture": capability.synthetic_fixture}}))
    return plan
