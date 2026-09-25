"""Decision-time joint scenario contract; generation and economic admission are later gates."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any

from atlas.v2._serialization import (
    decimal_value,
    json_value,
    nonblank,
    sha256_json,
    sha256_ref,
    strict_fields,
    timestamp,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository

VERSION = "PRETRADE_SCENARIO_ARTIFACT_V2_V1"
COST_ACCOUNTING_VERSION = "NET_CASHFLOW_ONCE_V1"
JOINT_DIMENSIONS = ("depth_liquidity", "entry_fill", "execution_latency", "exit_latency", "funding", "price_mark_index", "spread")
FORBIDDEN_INPUT_TYPES = frozenset({"ReplayPathV2", "PolicyPayoffV2", "PairedPortfolioPayoffV2", "MaturedOutcomeV2"})


class ScenarioStatusV2(StrEnum):
    AVAILABLE = "AVAILABLE"
    NOT_ESTIMABLE = "NOT_ESTIMABLE"


@dataclass(frozen=True)
class CausalInputV2:
    ref: str
    kind: str
    vintage_at_ns: int
    available_at_ns: int

    def __post_init__(self) -> None:
        sha256_ref(self.ref, field="causal input ref")
        nonblank(self.kind, field="input kind")
        timestamp(self.vintage_at_ns, field="vintage_at_ns")
        timestamp(self.available_at_ns, field="available_at_ns")
        if self.kind in FORBIDDEN_INPUT_TYPES or self.vintage_at_ns > self.available_at_ns:
            raise ValueError("retrospective or future-vintage input is not pretrade evidence")

    def to_dict(self) -> dict[str, Any]:
        return {"ref": self.ref, "kind": self.kind, "vintage_at_ns": self.vintage_at_ns, "available_at_ns": self.available_at_ns}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CausalInputV2:
        d = strict_fields(data, expected=set(cls.__dataclass_fields__), required=set(cls.__dataclass_fields__), name="CausalInputV2")
        return cls(**d)


@dataclass(frozen=True)
class JointScenarioV2:
    joint_path_id: str
    joint_payload_ref: str
    probability: Decimal

    def __post_init__(self) -> None:
        sha256_ref(self.joint_path_id, field="joint_path_id")
        sha256_ref(self.joint_payload_ref, field="joint_payload_ref")
        value = decimal_value(self.probability, field="probability")
        if value <= 0 or value > 1:
            raise ValueError("scenario probability must be in (0,1]")
        object.__setattr__(self, "probability", value)

    def to_dict(self) -> dict[str, Any]:
        return {"joint_path_id": self.joint_path_id, "joint_payload_ref": self.joint_payload_ref,
                "probability": json_value(self.probability)}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> JointScenarioV2:
        d = strict_fields(data, expected=set(cls.__dataclass_fields__), required=set(cls.__dataclass_fields__), name="JointScenarioV2")
        return cls(d["joint_path_id"], d["joint_payload_ref"], decimal_value(d["probability"], field="probability", wire=True))


@dataclass(frozen=True)
class PretradeScenarioArtifactV2:
    action_hash: str
    action_artifact_ref: str
    information_cutoff_ns: int
    generation_version: str
    model_version: str
    scenario_version: str
    model_input: CausalInputV2
    calibration_input: CausalInputV2
    source_inputs: tuple[CausalInputV2, ...]
    support_refs: tuple[str, ...]
    execution_model_input: CausalInputV2
    rows: tuple[JointScenarioV2, ...]
    common_scenario_set_id: str
    joint_dimensions: tuple[str, ...]
    deterministic_stress_refs: tuple[str, ...]
    outcome_dispersion_ref: str | None
    estimation_uncertainty_ref: str | None
    execution_uncertainty_ref: str | None
    numerical_error_ref: str | None
    cost_accounting_version: str
    created_at_ns: int
    computed_at_ns: int
    available_at_ns: int
    action_expires_at_ns: int
    status: ScenarioStatusV2
    inability_reason: str | None = None

    def __post_init__(self) -> None:
        for name in ("action_hash", "action_artifact_ref", "common_scenario_set_id"):
            sha256_ref(getattr(self, name), field=name)
        for name in ("generation_version", "model_version", "scenario_version", "cost_accounting_version"):
            nonblank(getattr(self, name), field=name)
        if self.cost_accounting_version != COST_ACCOUNTING_VERSION:
            raise ValueError("unsupported fee/funding single-count convention")
        for name in ("information_cutoff_ns", "created_at_ns", "computed_at_ns", "available_at_ns", "action_expires_at_ns"):
            timestamp(getattr(self, name), field=name)
        if not self.information_cutoff_ns <= self.created_at_ns <= self.computed_at_ns <= self.available_at_ns < self.action_expires_at_ns:
            raise ValueError("pretrade computation must finish and be available before expiry")
        object.__setattr__(self, "status", ScenarioStatusV2(self.status))
        inputs = (self.model_input, self.calibration_input, self.execution_model_input, *self.source_inputs)
        if any(not isinstance(item, CausalInputV2) or item.available_at_ns > self.information_cutoff_ns or item.vintage_at_ns > self.information_cutoff_ns for item in inputs):
            raise ValueError("every source, model and calibration vintage must be cutoff-known")
        if len({item.ref for item in inputs}) != len(inputs):
            raise ValueError("duplicate causal input")
        if tuple(sorted(self.joint_dimensions)) != JOINT_DIMENSIONS:
            raise ValueError("joint payload must declare price, liquidity, fill, latency and funding together")
        if self.status == ScenarioStatusV2.AVAILABLE:
            if not self.rows or sum((row.probability for row in self.rows), Decimal(0)) != 1:
                raise ValueError("probability-bearing joint rows must conserve exactly one")
            if not self.support_refs or any(getattr(self, name) is None for name in (
                    "outcome_dispersion_ref", "estimation_uncertainty_ref", "execution_uncertainty_ref", "numerical_error_ref")):
                raise ValueError("available scenarios require support and separate uncertainty refs")
            if self.inability_reason is not None:
                raise ValueError("available scenario cannot carry inability reason")
        elif self.rows or not self.inability_reason:
            raise ValueError("NOT_ESTIMABLE carries no synthetic probability rows and requires reason")
        ids = tuple(row.joint_path_id for row in self.rows)
        if ids != tuple(sorted(set(ids))) or len({row.joint_payload_ref for row in self.rows}) != len(self.rows):
            raise ValueError("joint rows must be unique and sorted")
        for name in ("support_refs", "deterministic_stress_refs"):
            refs = getattr(self, name)
            if refs != tuple(sorted(set(refs))):
                raise ValueError(f"{name} must be sorted and unique")
            for ref in refs:
                sha256_ref(ref, field=name)
        if set(self.deterministic_stress_refs) & {row.joint_payload_ref for row in self.rows}:
            raise ValueError("deterministic stress has no scenario probability mass")
        for name in ("outcome_dispersion_ref", "estimation_uncertainty_ref", "execution_uncertainty_ref", "numerical_error_ref"):
            value = getattr(self, name)
            if value is not None:
                sha256_ref(value, field=name)

    def _body(self) -> dict[str, Any]:
        return json_value({"version": VERSION, **{name: getattr(self, name) for name in self.__dataclass_fields__}})

    @property
    def content_hash(self) -> str:
        return sha256_json(self._body())

    def to_dict(self) -> dict[str, Any]:
        return {**self._body(), "content_hash": self.content_hash}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PretradeScenarioArtifactV2:
        d = strict_fields(data, expected=set(cls.__dataclass_fields__) | {"version", "content_hash"},
                          required=set(cls.__dataclass_fields__) | {"version", "content_hash"}, name="PretradeScenarioArtifactV2")
        if d["version"] != VERSION:
            raise ValueError("unsupported pretrade scenario version")
        values = {name: d[name] for name in cls.__dataclass_fields__}
        for name in ("model_input", "calibration_input", "execution_model_input"):
            values[name] = CausalInputV2.from_dict(values[name])
        for name, constructor in (("source_inputs", CausalInputV2.from_dict), ("rows", JointScenarioV2.from_dict)):
            if not isinstance(values[name], list):
                raise ValueError(f"{name} must be an array")
            values[name] = tuple(constructor(item) for item in values[name])
        for name in ("support_refs", "joint_dimensions", "deterministic_stress_refs"):
            if not isinstance(values[name], list):
                raise ValueError(f"{name} must be an array")
            values[name] = tuple(values[name])
        result = cls(**values)
        if result.content_hash != d["content_hash"]:
            raise ValueError("PretradeScenarioArtifactV2 content_hash mismatch")
        return result


def index_pretrade_scenario(repo: OpsRepository, scenario: PretradeScenarioArtifactV2) -> str:
    action = repo.get_artifact(scenario.action_artifact_ref)
    action_body = action.metadata.get("action_artifact") if action is not None else None
    if (action is None or action.artifact_type != "ActionArtifactV2" or action.available_at_ns > scenario.information_cutoff_ns
            or not isinstance(action_body, Mapping) or action_body.get("action_hash") != scenario.action_hash):
        raise ValueError("exact frozen action artifact unavailable by cutoff")
    for item in (scenario.model_input, scenario.calibration_input, scenario.execution_model_input, *scenario.source_inputs):
        entry = repo.get_artifact(item.ref)
        if (entry is None or entry.artifact_type != item.kind or entry.available_at_ns != item.available_at_ns
                or entry.available_at_ns > scenario.information_cutoff_ns or entry.created_at_ns > scenario.information_cutoff_ns):
            raise ValueError("causal source/model/calibration index mismatch")
    for ref in scenario.support_refs:
        entry = repo.get_artifact(ref)
        if entry is None or entry.available_at_ns > scenario.information_cutoff_ns:
            raise ValueError("scenario support was not cutoff-known")
    for row in scenario.rows:
        entry = repo.get_artifact(row.joint_payload_ref)
        if (entry is None or entry.artifact_type != "JointScenarioPayloadV2"
                or entry.available_at_ns > scenario.computed_at_ns
                or entry.metadata.get("joint_path_id") != row.joint_path_id
                or tuple(entry.metadata.get("joint_dimensions", ())) != JOINT_DIMENSIONS):
            raise ValueError("joint scenario payload identity/dimensions unavailable")
    for ref in scenario.deterministic_stress_refs:
        entry = repo.get_artifact(ref)
        if entry is None or entry.artifact_type != "DeterministicStressV2" or entry.available_at_ns > scenario.computed_at_ns:
            raise ValueError("separate deterministic stress unavailable")
    for uncertainty_ref in (scenario.outcome_dispersion_ref, scenario.estimation_uncertainty_ref,
                            scenario.execution_uncertainty_ref, scenario.numerical_error_ref):
        if uncertainty_ref is not None:
            entry = repo.get_artifact(uncertainty_ref)
            if entry is None or entry.available_at_ns > scenario.computed_at_ns:
                raise ValueError("separate uncertainty/dispersion evidence unavailable")
    repo.register_artifact(ArtifactIndexEntryV2(scenario.content_hash, "PretradeScenarioArtifactV2",
        scenario.content_hash, scenario.created_at_ns, scenario.available_at_ns, {"scenario": scenario.to_dict()}))
    return scenario.content_hash
