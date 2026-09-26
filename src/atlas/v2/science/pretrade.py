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

VERSION = "PRETRADE_SCENARIO_ARTIFACT_V2_V3"
COST_ACCOUNTING_VERSION = "NET_CASHFLOW_ONCE_V1"
JOINT_DIMENSIONS = ("depth_liquidity", "entry_fill", "execution_latency", "exit_latency", "funding", "price_mark_index", "spread")
JOINT_DATA_VERSION = "JOINT_SCENARIO_DATA_V2_V1"
JOINT_PAYLOAD_VERSION = "JOINT_SCENARIO_PAYLOAD_V2_V2"
FORBIDDEN_INPUT_TYPES = frozenset({"ReplayPathV2", "PolicyPayoffV2", "PairedPortfolioPayoffV2", "MaturedOutcomeV2"})
DERIVED_TYPES = frozenset({"SUPPORT", "STRESS", "OUTCOME_DISPERSION", "ESTIMATION_UNCERTAINTY",
                           "EXECUTION_UNCERTAINTY", "NUMERICAL_ERROR"})


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


class ScenarioFillStateV2(StrEnum):
    NO_FILL = "NO_FILL"
    PARTIAL_FILL = "PARTIAL_FILL"
    FULL_FILL = "FULL_FILL"
    NOT_ATTEMPTED = "NOT_ATTEMPTED"


@dataclass(frozen=True)
class JointScenarioPointV2:
    """One atomic time slice; all market, execution and funding values share one path identity."""

    at_ns: int
    joint_path_id: str
    last_price: Decimal
    mark_price: Decimal
    index_price: Decimal
    spread: Decimal
    bid_depth_contracts: Decimal
    ask_depth_contracts: Decimal
    entry_state: ScenarioFillStateV2
    requested_entry_quantity: Decimal
    entry_fill_quantity: Decimal
    decision_to_execution_latency_ns: int
    exit_latency_ns: int
    exit_state: ScenarioFillStateV2
    exit_fill_quantity: Decimal
    funding_cashflow_usdt: Decimal

    def __post_init__(self) -> None:
        timestamp(self.at_ns, field="joint point at_ns")
        sha256_ref(self.joint_path_id, field="joint point path id")
        for name in ("last_price", "mark_price", "index_price"):
            value = decimal_value(getattr(self, name), field=name)
            if value <= 0:
                raise ValueError(f"{name} must be positive")
            object.__setattr__(self, name, value)
        for name in ("spread", "bid_depth_contracts", "ask_depth_contracts",
                     "requested_entry_quantity", "entry_fill_quantity", "exit_fill_quantity"):
            value = decimal_value(getattr(self, name), field=name)
            if value < 0:
                raise ValueError(f"{name} must be nonnegative")
            object.__setattr__(self, name, value)
        object.__setattr__(self, "funding_cashflow_usdt",
                           decimal_value(self.funding_cashflow_usdt, field="funding_cashflow_usdt"))
        for name in ("decision_to_execution_latency_ns", "exit_latency_ns"):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be nonnegative integer nanoseconds")
        object.__setattr__(self, "entry_state", ScenarioFillStateV2(self.entry_state))
        object.__setattr__(self, "exit_state", ScenarioFillStateV2(self.exit_state))
        self._validate_fill("entry", self.entry_state, self.entry_fill_quantity, self.requested_entry_quantity)
        self._validate_fill("exit", self.exit_state, self.exit_fill_quantity, self.entry_fill_quantity)

    @staticmethod
    def _validate_fill(label: str, state: ScenarioFillStateV2, filled: Decimal, requested: Decimal) -> None:
        if state == ScenarioFillStateV2.NOT_ATTEMPTED:
            if filled != 0:
                raise ValueError(f"{label} not-attempted state cannot have fills")
        elif state == ScenarioFillStateV2.NO_FILL:
            if filled != 0:
                raise ValueError(f"{label} no-fill state must have zero quantity")
        elif requested <= 0 or filled <= 0 or filled > requested:
            raise ValueError(f"{label} fill quantity is inconsistent with requested quantity")
        elif state == ScenarioFillStateV2.PARTIAL_FILL and filled >= requested:
            raise ValueError(f"{label} partial-fill quantity must be below requested quantity")
        elif state == ScenarioFillStateV2.FULL_FILL and filled != requested:
            raise ValueError(f"{label} full-fill quantity must equal requested quantity")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"at_ns": self.at_ns, "joint_path_id": self.joint_path_id,
            **{name: getattr(self, name) for name in (
                "last_price", "mark_price", "index_price", "spread", "bid_depth_contracts", "ask_depth_contracts",
                "entry_state", "requested_entry_quantity", "entry_fill_quantity", "decision_to_execution_latency_ns",
                "exit_latency_ns", "exit_state", "exit_fill_quantity", "funding_cashflow_usdt")}})

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> JointScenarioPointV2:
        fields = set(cls.__dataclass_fields__)
        d = strict_fields(data, expected=fields, required=fields, name="JointScenarioPointV2")
        return cls(d["at_ns"], d["joint_path_id"],
            decimal_value(d["last_price"], field="last_price", wire=True),
            decimal_value(d["mark_price"], field="mark_price", wire=True),
            decimal_value(d["index_price"], field="index_price", wire=True),
            decimal_value(d["spread"], field="spread", wire=True),
            decimal_value(d["bid_depth_contracts"], field="bid_depth_contracts", wire=True),
            decimal_value(d["ask_depth_contracts"], field="ask_depth_contracts", wire=True),
            ScenarioFillStateV2(d["entry_state"]),
            decimal_value(d["requested_entry_quantity"], field="requested_entry_quantity", wire=True),
            decimal_value(d["entry_fill_quantity"], field="entry_fill_quantity", wire=True),
            d["decision_to_execution_latency_ns"], d["exit_latency_ns"],
            ScenarioFillStateV2(d["exit_state"]),
            decimal_value(d["exit_fill_quantity"], field="exit_fill_quantity", wire=True),
            decimal_value(d["funding_cashflow_usdt"], field="funding_cashflow_usdt", wire=True))


@dataclass(frozen=True)
class JointScenarioDataV2:
    action_hash: str
    action_artifact_ref: str
    information_cutoff_ns: int
    common_scenario_set_id: str
    joint_path_id: str
    generation_version: str
    scenario_version: str
    causal_input_manifest_hash: str
    requested_entry_quantity: Decimal
    points: tuple[JointScenarioPointV2, ...]
    created_at_ns: int
    computed_at_ns: int
    available_at_ns: int

    def __post_init__(self) -> None:
        for name in ("action_hash", "action_artifact_ref", "common_scenario_set_id", "joint_path_id",
                     "causal_input_manifest_hash"):
            sha256_ref(getattr(self, name), field=name)
        for name in ("generation_version", "scenario_version"):
            nonblank(getattr(self, name), field=name)
        quantity = decimal_value(self.requested_entry_quantity, field="requested_entry_quantity")
        if quantity <= 0:
            raise ValueError("requested entry quantity must be positive")
        object.__setattr__(self, "requested_entry_quantity", quantity)
        if not self.points or any(not isinstance(x, JointScenarioPointV2) for x in self.points):
            raise ValueError("joint data requires typed path points")
        times = tuple(x.at_ns for x in self.points)
        if (times != tuple(sorted(set(times))) or any(x.at_ns <= self.information_cutoff_ns for x in self.points)
                or any(x.joint_path_id != self.joint_path_id for x in self.points)):
            raise ValueError("joint point order or path identity mismatch")
        if any(x.requested_entry_quantity != quantity for x in self.points):
            raise ValueError("joint points must bind the parent requested quantity")
        for name in ("information_cutoff_ns", "created_at_ns", "computed_at_ns", "available_at_ns"):
            timestamp(getattr(self, name), field=name)
        if not self.information_cutoff_ns <= self.created_at_ns <= self.computed_at_ns <= self.available_at_ns:
            raise ValueError("joint data computation chronology invalid")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": JOINT_DATA_VERSION,
            **{name: getattr(self, name) for name in self.__dataclass_fields__ if name != "points"},
            "points": [point.to_dict() for point in self.points]})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> JointScenarioDataV2:
        fields = set(cls.__dataclass_fields__) | {"version"}
        d = strict_fields(data, expected=fields, required=fields, name="JointScenarioDataV2")
        if d["version"] != JOINT_DATA_VERSION or not isinstance(d["points"], list):
            raise ValueError("unsupported or opaque joint data wire")
        return cls(d["action_hash"], d["action_artifact_ref"], d["information_cutoff_ns"],
            d["common_scenario_set_id"], d["joint_path_id"], d["generation_version"], d["scenario_version"],
            d["causal_input_manifest_hash"], decimal_value(d["requested_entry_quantity"],
            field="requested_entry_quantity", wire=True), tuple(JointScenarioPointV2.from_dict(x) for x in d["points"]),
            d["created_at_ns"], d["computed_at_ns"], d["available_at_ns"])


@dataclass(frozen=True)
class JointScenarioPayloadV2:
    action_hash: str
    action_artifact_ref: str
    information_cutoff_ns: int
    common_scenario_set_id: str
    joint_path_id: str
    generation_version: str
    scenario_version: str
    causal_input_manifest_hash: str
    joint_dimensions: tuple[str, ...]
    joint_data_ref: str
    created_at_ns: int
    computed_at_ns: int
    available_at_ns: int

    def __post_init__(self) -> None:
        for name in ("action_hash", "action_artifact_ref", "common_scenario_set_id", "joint_path_id",
                     "causal_input_manifest_hash", "joint_data_ref"):
            sha256_ref(getattr(self, name), field=name)
        for name in ("generation_version", "scenario_version"):
            nonblank(getattr(self, name), field=name)
        if tuple(sorted(self.joint_dimensions)) != JOINT_DIMENSIONS:
            raise ValueError("joint payload must bind all required dimensions")
        for name in ("information_cutoff_ns", "created_at_ns", "computed_at_ns", "available_at_ns"):
            timestamp(getattr(self, name), field=name)
        if not self.information_cutoff_ns <= self.created_at_ns <= self.computed_at_ns <= self.available_at_ns:
            raise ValueError("joint payload chronology invalid")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": JOINT_PAYLOAD_VERSION,
                           **{name: getattr(self, name) for name in self.__dataclass_fields__}})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> JointScenarioPayloadV2:
        fields = set(cls.__dataclass_fields__) | {"version"}
        d = strict_fields(data, expected=fields, required=fields, name="JointScenarioPayloadV2")
        if d["version"] != JOINT_PAYLOAD_VERSION or not isinstance(d["joint_dimensions"], list):
            raise ValueError("unsupported joint payload wire")
        return cls(d["action_hash"], d["action_artifact_ref"], d["information_cutoff_ns"],
                   d["common_scenario_set_id"], d["joint_path_id"], d["generation_version"],
                   d["scenario_version"], d["causal_input_manifest_hash"], tuple(d["joint_dimensions"]),
                   d["joint_data_ref"], d["created_at_ns"], d["computed_at_ns"], d["available_at_ns"])


@dataclass(frozen=True)
class PretradeDerivedEvidenceV2:
    role: str
    action_hash: str
    action_artifact_ref: str
    information_cutoff_ns: int
    common_scenario_set_id: str
    causal_input_manifest_hash: str
    result_ref: str
    created_at_ns: int
    computed_at_ns: int
    available_at_ns: int

    def __post_init__(self) -> None:
        if self.role not in DERIVED_TYPES:
            raise ValueError("unsupported derived evidence role")
        for name in ("action_hash", "action_artifact_ref", "common_scenario_set_id",
                     "causal_input_manifest_hash", "result_ref"):
            sha256_ref(getattr(self, name), field=name)
        for name in ("information_cutoff_ns", "created_at_ns", "computed_at_ns", "available_at_ns"):
            timestamp(getattr(self, name), field=name)
        if not self.information_cutoff_ns <= self.created_at_ns <= self.computed_at_ns <= self.available_at_ns:
            raise ValueError("derived evidence chronology invalid")

    def to_dict(self) -> dict[str, Any]:
        return {"version": "PRETRADE_DERIVED_EVIDENCE_V2_V1",
                **{name: getattr(self, name) for name in self.__dataclass_fields__}}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PretradeDerivedEvidenceV2:
        fields = set(cls.__dataclass_fields__) | {"version"}
        d = strict_fields(data, expected=fields, required=fields, name="PretradeDerivedEvidenceV2")
        if d["version"] != "PRETRADE_DERIVED_EVIDENCE_V2_V1":
            raise ValueError("unsupported derived evidence wire")
        return cls(**{name: d[name] for name in cls.__dataclass_fields__})


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

    @property
    def causal_input_manifest_hash(self) -> str:
        return sha256_json({"version": "PRETRADE_CAUSAL_INPUT_MANIFEST_V2_V1",
            "model_version": self.model_version,
            "model": self.model_input.to_dict(), "calibration": self.calibration_input.to_dict(),
            "execution_model": self.execution_model_input.to_dict(),
            "sources": [item.to_dict() for item in self.source_inputs]})

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
        if self.source_inputs != tuple(sorted(self.source_inputs, key=lambda item: item.ref)):
            raise ValueError("causal source inputs must be sorted by exact ref")
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
    action_identity = action.metadata.get("action_identity") if action is not None else None
    if (action is None or action.artifact_type != "ActionArtifactV2" or action.available_at_ns > scenario.information_cutoff_ns
            or not isinstance(action_body, Mapping) or not isinstance(action_identity, Mapping)
            or sha256_json(action_body) != scenario.action_artifact_ref
            or sha256_json(action_identity) != scenario.action_hash
            or action_body.get("action_hash") != scenario.action_hash):
        raise ValueError("exact frozen action artifact unavailable by cutoff")
    for item in (scenario.model_input, scenario.calibration_input, scenario.execution_model_input, *scenario.source_inputs):
        entry = repo.get_artifact(item.ref)
        if (entry is None or entry.artifact_type != item.kind or entry.available_at_ns != item.available_at_ns
                or entry.available_at_ns > scenario.information_cutoff_ns or entry.created_at_ns > scenario.information_cutoff_ns
                or item.vintage_at_ns > entry.available_at_ns):
            raise ValueError("causal source/model/calibration index mismatch")
    for row in scenario.rows:
        entry = repo.get_artifact(row.joint_payload_ref)
        body = entry.metadata.get("joint_payload") if entry is not None else None
        if entry is None or entry.artifact_type != "JointScenarioPayloadV2" or not isinstance(body, Mapping):
            raise ValueError("typed joint scenario payload required")
        payload = JointScenarioPayloadV2.from_dict(json_value(body))
        if (payload.content_hash != row.joint_payload_ref
                or entry.created_at_ns != payload.created_at_ns
                or entry.available_at_ns != payload.available_at_ns
                or payload.action_hash != scenario.action_hash
                or payload.action_artifact_ref != scenario.action_artifact_ref
                or payload.information_cutoff_ns != scenario.information_cutoff_ns
                or payload.common_scenario_set_id != scenario.common_scenario_set_id
                or payload.joint_path_id != row.joint_path_id
                or payload.generation_version != scenario.generation_version
                or payload.scenario_version != scenario.scenario_version
                or payload.causal_input_manifest_hash != scenario.causal_input_manifest_hash
                or payload.computed_at_ns > scenario.computed_at_ns
                or payload.available_at_ns > scenario.computed_at_ns):
            raise ValueError("joint scenario action/cutoff/set/manifest or chronology mismatch")
        data = _resolve_joint_data(repo, payload)
        quantity_raw = action_identity.get("quantity")
        if not isinstance(quantity_raw, str):
            raise ValueError("frozen action quantity must use canonical Decimal wire form")
        quantity = decimal_value(quantity_raw, field="action quantity", wire=True)
        if data.requested_entry_quantity != quantity:
            raise ValueError("joint data requested quantity does not match exact frozen action")
    derived = (*( (ref, "SUPPORT") for ref in scenario.support_refs),
        *((ref, "STRESS") for ref in scenario.deterministic_stress_refs),
        (scenario.outcome_dispersion_ref, "OUTCOME_DISPERSION"),
        (scenario.estimation_uncertainty_ref, "ESTIMATION_UNCERTAINTY"),
        (scenario.execution_uncertainty_ref, "EXECUTION_UNCERTAINTY"),
        (scenario.numerical_error_ref, "NUMERICAL_ERROR"))
    for ref, role in derived:
        if ref is None:
            continue
        entry = repo.get_artifact(ref)
        body = entry.metadata.get("derived") if entry is not None else None
        if entry is None or entry.artifact_type != "PretradeDerivedEvidenceV2" or not isinstance(body, Mapping):
            raise ValueError("typed pretrade derived evidence required")
        derived_item = PretradeDerivedEvidenceV2.from_dict(json_value(body))
        if (derived_item.content_hash != ref or derived_item.role != role or derived_item.action_hash != scenario.action_hash
                or derived_item.action_artifact_ref != scenario.action_artifact_ref
                or derived_item.information_cutoff_ns != scenario.information_cutoff_ns
                or derived_item.common_scenario_set_id != scenario.common_scenario_set_id
                or derived_item.causal_input_manifest_hash != scenario.causal_input_manifest_hash
                or derived_item.computed_at_ns > scenario.computed_at_ns or derived_item.available_at_ns > scenario.computed_at_ns
                or entry.created_at_ns != derived_item.created_at_ns
                or entry.available_at_ns != derived_item.available_at_ns):
            raise ValueError("derived evidence action/cutoff/set/manifest or chronology mismatch")
        result = repo.get_artifact(derived_item.result_ref)
        if (result is None or result.created_at_ns > derived_item.computed_at_ns
                or result.available_at_ns > derived_item.available_at_ns
                or sha256_json(result.metadata) != derived_item.result_ref):
            raise ValueError("derived result payload unavailable")
    repo.register_artifact(ArtifactIndexEntryV2(scenario.content_hash, "PretradeScenarioArtifactV2",
        scenario.content_hash, scenario.created_at_ns, scenario.available_at_ns, {"scenario": scenario.to_dict()}))
    return scenario.content_hash


def index_joint_scenario_payload(repo: OpsRepository, payload: JointScenarioPayloadV2) -> str:
    _resolve_joint_data(repo, payload)
    repo.register_artifact(ArtifactIndexEntryV2(payload.content_hash, "JointScenarioPayloadV2",
        payload.content_hash, payload.created_at_ns, payload.available_at_ns, {"joint_payload": payload.to_dict()}))
    return payload.content_hash


def index_joint_scenario_data(repo: OpsRepository, data: JointScenarioDataV2) -> str:
    """Index one immutable, structurally joint path, never an opaque metadata label."""
    repo.register_artifact(ArtifactIndexEntryV2(data.content_hash, "JointScenarioDataV2",
        data.content_hash, data.created_at_ns, data.available_at_ns, {"joint_data": data.to_dict()}))
    return data.content_hash


def _resolve_joint_data(repo: OpsRepository, payload: JointScenarioPayloadV2) -> JointScenarioDataV2:
    entry = repo.get_artifact(payload.joint_data_ref)
    body = entry.metadata.get("joint_data") if entry is not None else None
    if (entry is None or entry.artifact_type != "JointScenarioDataV2" or not isinstance(body, Mapping)
            or entry.content_hash != payload.joint_data_ref):
        raise ValueError("typed immutable JointScenarioDataV2 required")
    data = JointScenarioDataV2.from_dict(json_value(body))
    if (data.content_hash != payload.joint_data_ref or entry.created_at_ns != data.created_at_ns
            or entry.available_at_ns != data.available_at_ns
            or data.available_at_ns > payload.available_at_ns
            or data.computed_at_ns > payload.computed_at_ns
            or data.action_hash != payload.action_hash
            or data.action_artifact_ref != payload.action_artifact_ref
            or data.information_cutoff_ns != payload.information_cutoff_ns
            or data.common_scenario_set_id != payload.common_scenario_set_id
            or data.joint_path_id != payload.joint_path_id
            or data.generation_version != payload.generation_version
            or data.scenario_version != payload.scenario_version
            or data.causal_input_manifest_hash != payload.causal_input_manifest_hash):
        raise ValueError("joint data action/cutoff/set/path/version/manifest or chronology mismatch")
    return data


def index_pretrade_derived_evidence(repo: OpsRepository, item: PretradeDerivedEvidenceV2) -> str:
    result = repo.get_artifact(item.result_ref)
    if (result is None or result.created_at_ns > item.computed_at_ns
            or result.available_at_ns > item.available_at_ns
            or sha256_json(result.metadata) != item.result_ref):
        raise ValueError("derived result unavailable")
    repo.register_artifact(ArtifactIndexEntryV2(item.content_hash, "PretradeDerivedEvidenceV2",
        item.content_hash, item.created_at_ns, item.available_at_ns, {"derived": item.to_dict()}))
    return item.content_hash
