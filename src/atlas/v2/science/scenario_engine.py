"""Decision-time, joint execution scenarios for Session 019.

This wire is deliberately additive to Session 018's scenario contracts.  It
stores executable quote prices and exit semantics which the accepted V1 joint
data format did not contain.  It never consumes retrospective replay artifacts.
"""

from __future__ import annotations

import hashlib
import random
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any

from atlas.domain.money import canonical_decimal_str
from atlas.v2._serialization import canonical_json, decimal_value, json_value, sha256_json, sha256_ref, strict_fields
from atlas.v2.contracts import CandidateActionV2
from atlas.v2.instruments import InstrumentKeyV2, ProductContractV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.science.action import ActionArtifactV2
from atlas.v2.science.costs import FeeScheduleV2
from atlas.v2.science.pretrade import CausalInputV2, ScenarioFillStateV2

JOINT_EXECUTION_DATA_VERSION = "JOINT_SCENARIO_DATA_V2_V2"
JOINT_EXECUTION_PAYLOAD_VERSION = "JOINT_SCENARIO_PAYLOAD_V2_V3"
PRETRADE_EXECUTION_SCENARIO_VERSION = "PRETRADE_SCENARIO_ARTIFACT_V2_V5"
PATH_PAYOFF_VERSION = "PRETRADE_PATH_PAYOFF_V2_V1"
SCENARIO_GENERATOR_VERSION = "COHERENT_JOINT_BLOCK_RESAMPLE_V1"
FORBIDDEN_PRETRADE_TYPES = frozenset({"ReplayPathV2", "PolicyPayoffV2", "PairedPortfolioPayoffV2", "MaturedOutcomeV2"})
ZERO = Decimal(0)


class ExitReasonV2(StrEnum):
    STOP = "STOP"
    FAILED_BREAK = "FAILED_BREAK"
    TIME_EXIT = "TIME_EXIT"


class ScenarioGenerationStatusV2(StrEnum):
    AVAILABLE = "AVAILABLE"
    NOT_ESTIMABLE = "NOT_ESTIMABLE"


@dataclass(frozen=True)
class JointMarketPointV2:
    """A coherent quote/price/depth/funding slice from one joint path."""

    at_ns: int
    joint_path_id: str
    last_price: Decimal
    mark_price: Decimal
    index_price: Decimal
    bid_price: Decimal
    ask_price: Decimal
    bid_depth: Decimal
    ask_depth: Decimal
    spread: Decimal
    funding_event_ref: str | None
    funding_cashflow_usdt: Decimal

    def __post_init__(self) -> None:
        sha256_ref(self.joint_path_id, field="joint_path_id")
        if type(self.at_ns) is not int or self.at_ns < 0:
            raise ValueError("joint point timestamp invalid")
        for name in ("last_price", "mark_price", "index_price", "bid_price", "ask_price"):
            value = decimal_value(getattr(self, name), field=name)
            if value <= 0:
                raise ValueError(f"{name} must be positive")
            object.__setattr__(self, name, value)
        for name in ("bid_depth", "ask_depth", "spread"):
            value = decimal_value(getattr(self, name), field=name)
            if value < 0:
                raise ValueError(f"{name} must be nonnegative")
            object.__setattr__(self, name, value)
        object.__setattr__(self, "funding_cashflow_usdt", decimal_value(self.funding_cashflow_usdt, field="funding_cashflow_usdt"))
        if self.funding_event_ref is not None:
            sha256_ref(self.funding_event_ref, field="funding_event_ref")
        elif self.funding_cashflow_usdt != 0:
            raise ValueError("nonzero funding cashflow requires exact settlement identity")
        if self.bid_price > self.ask_price or self.ask_price - self.bid_price != self.spread:
            raise ValueError("executable BBO is crossed or spread disagrees")

    def to_dict(self) -> dict[str, Any]:
        return json_value({name: getattr(self, name) for name in self.__dataclass_fields__})

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> JointMarketPointV2:
        fields = set(cls.__dataclass_fields__)
        d = dict(strict_fields(data, expected=fields, required=fields, name="JointMarketPointV2"))
        for name in ("last_price", "mark_price", "index_price", "bid_price", "ask_price", "bid_depth", "ask_depth", "spread", "funding_cashflow_usdt"):
            d[name] = decimal_value(d[name], field=name, wire=True)
        return cls(**d)


@dataclass(frozen=True)
class JointExecutionDataV2:
    """Immutable joint template whose execution fields use explicit BBO prices."""

    action_hash: str
    action_artifact_ref: str
    information_cutoff_ns: int
    common_scenario_set_id: str
    joint_path_id: str
    generation_version: str
    scenario_version: str
    source_ref: str
    execution_model_ref: str
    fee_ref: str
    requested_quantity: Decimal
    entry_state: ScenarioFillStateV2
    entry_quantity: Decimal
    entry_price: Decimal | None
    entry_at_ns: int | None
    entry_latency_ns: int | None
    exit_state: ScenarioFillStateV2
    exit_quantity: Decimal
    exit_price: Decimal | None
    exit_at_ns: int | None
    exit_latency_ns: int | None
    exit_reason: ExitReasonV2 | None
    s2_management_ref: str | None
    points: tuple[JointMarketPointV2, ...]
    computed_at_ns: int
    available_at_ns: int
    execution_depth_qualified: bool
    synthetic_fixture: bool = False

    def __post_init__(self) -> None:
        for name in ("action_hash", "action_artifact_ref", "common_scenario_set_id", "joint_path_id", "source_ref", "execution_model_ref", "fee_ref"):
            sha256_ref(getattr(self, name), field=name)
        if self.s2_management_ref is not None:
            sha256_ref(self.s2_management_ref, field="s2_management_ref")
        if self.generation_version != SCENARIO_GENERATOR_VERSION or not self.scenario_version.strip():
            raise ValueError("unsupported joint execution generator/scenario version")
        for name in ("information_cutoff_ns", "computed_at_ns", "available_at_ns"):
            if type(getattr(self, name)) is not int or getattr(self, name) < 0:
                raise ValueError(f"invalid {name}")
        if not self.information_cutoff_ns <= self.computed_at_ns <= self.available_at_ns:
            raise ValueError("joint execution chronology invalid")
        for name in ("requested_quantity", "entry_quantity", "exit_quantity"):
            value = decimal_value(getattr(self, name), field=name)
            if value < 0 or (name == "requested_quantity" and value == 0):
                raise ValueError(f"invalid {name}")
            object.__setattr__(self, name, value)
        object.__setattr__(self, "entry_state", ScenarioFillStateV2(self.entry_state))
        object.__setattr__(self, "exit_state", ScenarioFillStateV2(self.exit_state))
        object.__setattr__(self, "exit_reason", ExitReasonV2(self.exit_reason) if self.exit_reason is not None else None)
        for name in ("entry_price", "exit_price"):
            value = getattr(self, name)
            if value is not None:
                value = decimal_value(value, field=name)
                if value <= 0:
                    raise ValueError(f"{name} must be positive")
                object.__setattr__(self, name, value)
        for name in ("entry_at_ns", "entry_latency_ns", "exit_at_ns", "exit_latency_ns"):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError(f"{name} invalid")
        if self.entry_state == ScenarioFillStateV2.NO_FILL:
            if any(x is not None for x in (self.entry_price, self.entry_at_ns, self.entry_latency_ns)) or self.entry_quantity != 0:
                raise ValueError("NO_FILL cannot have entry execution fields")
        elif (self.entry_state == ScenarioFillStateV2.NOT_ATTEMPTED or self.entry_quantity <= 0 or
              self.entry_quantity > self.requested_quantity or self.entry_price is None or
              self.entry_at_ns is None or self.entry_latency_ns is None):
            raise ValueError("entry execution is incomplete")
        if self.entry_state == ScenarioFillStateV2.FULL_FILL and self.entry_quantity != self.requested_quantity:
            raise ValueError("full entry quantity must equal frozen request")
        if self.entry_state == ScenarioFillStateV2.PARTIAL_FILL and self.entry_quantity >= self.requested_quantity:
            raise ValueError("partial entry must be below frozen request")
        if self.entry_state == ScenarioFillStateV2.NO_FILL:
            if self.exit_state != ScenarioFillStateV2.NO_FILL or self.exit_quantity != 0 or any(
                x is not None for x in (self.exit_price, self.exit_at_ns, self.exit_latency_ns, self.exit_reason)
            ):
                raise ValueError("NO_FILL path cannot include an exit")
        elif (self.exit_state != ScenarioFillStateV2.FULL_FILL or self.exit_quantity != self.entry_quantity or
              self.exit_price is None or self.exit_at_ns is None or self.exit_latency_ns is None or self.exit_reason is None):
            raise ValueError("filled path must close all filled quantity with explicit exit ordering")
        if self.exit_at_ns is not None and self.entry_at_ns is not None and self.exit_at_ns <= self.entry_at_ns:
            raise ValueError("exit must follow entry")
        if (self.entry_at_ns is not None and self.entry_latency_ns is not None and
                self.entry_at_ns != self.information_cutoff_ns + self.entry_latency_ns):
            raise ValueError("entry execution disagrees with its modeled latency")
        if self.exit_reason == ExitReasonV2.FAILED_BREAK and self.s2_management_ref is None:
            raise ValueError("failed-break path requires S2 management evidence")
        if not self.points or tuple(p.at_ns for p in self.points) != tuple(sorted({p.at_ns for p in self.points})):
            raise ValueError("joint path points must be nonempty, unique and ordered")
        if any(p.joint_path_id != self.joint_path_id for p in self.points):
            raise ValueError("component swap/path identity mismatch")
        funding_refs = [p.funding_event_ref for p in self.points if p.funding_event_ref is not None]
        if len(funding_refs) != len(set(funding_refs)):
            raise ValueError("funding settlement cannot be counted twice on a joint path")
        if type(self.synthetic_fixture) is not bool:
            raise ValueError("synthetic fixture marker must be boolean")
        if type(self.execution_depth_qualified) is not bool:
            raise ValueError("execution/depth qualification must be explicit")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": JOINT_EXECUTION_DATA_VERSION,
            **{name: getattr(self, name) for name in self.__dataclass_fields__ if name != "points"},
            "points": [point.to_dict() for point in self.points]})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> JointExecutionDataV2:
        fields = set(cls.__dataclass_fields__) | {"version"}
        d = dict(strict_fields(data, expected=fields, required=fields, name="JointExecutionDataV2"))
        if d["version"] != JOINT_EXECUTION_DATA_VERSION or not isinstance(d["points"], list):
            raise ValueError("unsupported joint execution data wire")
        for name in ("requested_quantity", "entry_quantity", "exit_quantity"):
            d[name] = decimal_value(d[name], field=name, wire=True)
        for name in ("entry_price", "exit_price"):
            d[name] = decimal_value(d[name], field=name, wire=True) if d[name] is not None else None
        d["entry_state"] = ScenarioFillStateV2(d["entry_state"])
        d["exit_state"] = ScenarioFillStateV2(d["exit_state"])
        d["exit_reason"] = ExitReasonV2(d["exit_reason"]) if d["exit_reason"] is not None else None
        d["points"] = tuple(JointMarketPointV2.from_dict(point) for point in d["points"])
        return cls(**{name: d[name] for name in cls.__dataclass_fields__})


@dataclass(frozen=True)
class S2ScenarioManagementV2:
    action_hash: str
    joint_path_id: str
    setup_ref: str
    exit_reason: ExitReasonV2
    failed_break_at_ns: int | None
    range_low: Decimal
    range_high: Decimal
    subsequent_closed_bars: tuple[tuple[int, Decimal], ...]
    management_rule_hash: str
    available_at_ns: int

    def __post_init__(self) -> None:
        for name in ("action_hash", "joint_path_id", "setup_ref", "management_rule_hash"):
            sha256_ref(getattr(self, name), field=name)
        object.__setattr__(self, "exit_reason", ExitReasonV2(self.exit_reason))
        object.__setattr__(self, "range_low", decimal_value(self.range_low, field="range_low"))
        object.__setattr__(self, "range_high", decimal_value(self.range_high, field="range_high"))
        bars = tuple((at_ns, decimal_value(close, field="subsequent_close"))
                     for at_ns, close in self.subsequent_closed_bars)
        object.__setattr__(self, "subsequent_closed_bars", bars)
        if (self.range_low <= 0 or self.range_high <= self.range_low or len(bars) != 2 or
                tuple(at for at, _ in bars) != tuple(sorted({at for at, _ in bars})) or
                any(at < 0 or close <= 0 for at, close in bars) or
                bars[1][0] - bars[0][0] != 900_000_000_000):
            raise ValueError("S2 failed-break path requires exact range and first two closed 15m bars")
        if self.exit_reason == ExitReasonV2.FAILED_BREAK:
            if type(self.failed_break_at_ns) is not int or self.failed_break_at_ns < 0:
                raise ValueError("S2 failed-break event requires exact timestamp")
        elif self.failed_break_at_ns is not None:
            raise ValueError("non-failed-break S2 path cannot carry failed-break time")
        if type(self.available_at_ns) is not int or self.available_at_ns < 0:
            raise ValueError("S2 management evidence availability invalid")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": "S2_PRETRADE_MANAGEMENT_PATH_V2",
            **{name: getattr(self, name) for name in self.__dataclass_fields__}})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> S2ScenarioManagementV2:
        fields = set(cls.__dataclass_fields__) | {"version"}
        d = dict(strict_fields(data, expected=fields, required=fields, name="S2ScenarioManagementV2"))
        if (d.pop("version") != "S2_PRETRADE_MANAGEMENT_PATH_V2" or
                not isinstance(d["subsequent_closed_bars"], list) or
                any(not isinstance(row, list) or len(row) != 2 for row in d["subsequent_closed_bars"])):
            raise ValueError("unsupported S2 pretrade management wire")
        d["exit_reason"] = ExitReasonV2(d["exit_reason"])
        d["range_low"] = decimal_value(d["range_low"], field="range_low", wire=True)
        d["range_high"] = decimal_value(d["range_high"], field="range_high", wire=True)
        d["subsequent_closed_bars"] = tuple((row[0], decimal_value(row[1], field="subsequent_close", wire=True))
            for row in d["subsequent_closed_bars"])
        return cls(**{name: d[name] for name in cls.__dataclass_fields__})


@dataclass(frozen=True)
class JointExecutionPayloadV2:
    action_hash: str
    action_artifact_ref: str
    information_cutoff_ns: int
    common_scenario_set_id: str
    joint_path_id: str
    generation_version: str
    scenario_version: str
    source_manifest_hash: str
    joint_data_ref: str
    created_at_ns: int
    computed_at_ns: int
    available_at_ns: int

    def __post_init__(self) -> None:
        for name in ("action_hash", "action_artifact_ref", "common_scenario_set_id", "joint_path_id", "source_manifest_hash", "joint_data_ref"):
            sha256_ref(getattr(self, name), field=name)
        if self.generation_version != SCENARIO_GENERATOR_VERSION:
            raise ValueError("unsupported execution payload generator")
        if not self.scenario_version.strip():
            raise ValueError("scenario version required")
        if not self.information_cutoff_ns <= self.created_at_ns <= self.computed_at_ns <= self.available_at_ns:
            raise ValueError("execution payload chronology invalid")

    def to_dict(self) -> dict[str, Any]:
        return json_value({"version": JOINT_EXECUTION_PAYLOAD_VERSION, **{name: getattr(self, name) for name in self.__dataclass_fields__}})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> JointExecutionPayloadV2:
        fields = set(cls.__dataclass_fields__) | {"version"}
        d = dict(strict_fields(data, expected=fields, required=fields, name="JointExecutionPayloadV2"))
        if d["version"] != JOINT_EXECUTION_PAYLOAD_VERSION:
            raise ValueError("unsupported joint execution payload version")
        return cls(**{name: d[name] for name in cls.__dataclass_fields__})


@dataclass(frozen=True)
class PretradePathPayoffV2:
    action_hash: str
    scenario_artifact_ref: str
    joint_path_id: str
    joint_payload_ref: str
    execution_model_ref: str
    entry_cashflow: Decimal
    exit_cashflow: Decimal
    fees: Decimal
    funding: Decimal
    net_payoff: Decimal
    filled_quantity: Decimal
    available_at_ns: int

    def __post_init__(self) -> None:
        for name in ("action_hash", "scenario_artifact_ref", "joint_path_id", "joint_payload_ref", "execution_model_ref"):
            sha256_ref(getattr(self, name), field=name)
        for name in ("entry_cashflow", "exit_cashflow", "fees", "funding", "net_payoff", "filled_quantity"):
            value = decimal_value(getattr(self, name), field=name)
            object.__setattr__(self, name, value)
        if self.fees < 0 or self.filled_quantity < 0 or type(self.available_at_ns) is not int or self.available_at_ns < 0:
            raise ValueError("path payoff amount or availability invalid")
        if self.net_payoff != self.entry_cashflow + self.exit_cashflow - self.fees + self.funding:
            raise ValueError("path payoff cashflow components do not reconcile exactly")

    def to_dict(self) -> dict[str, Any]:
        return {"version": PATH_PAYOFF_VERSION, "action_hash": self.action_hash,
            "scenario_artifact_ref": self.scenario_artifact_ref, "joint_path_id": self.joint_path_id,
            "joint_payload_ref": self.joint_payload_ref, "execution_model_ref": self.execution_model_ref,
            **{name: canonical_decimal_str(getattr(self, name)) for name in (
                "entry_cashflow", "exit_cashflow", "fees", "funding", "net_payoff", "filled_quantity")},
            "available_at_ns": self.available_at_ns}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class PretradeExecutionScenarioV2:
    action_hash: str
    action_artifact_ref: str
    information_cutoff_ns: int
    model_input: CausalInputV2
    calibration_input: CausalInputV2
    execution_model_input: CausalInputV2
    source_inputs: tuple[CausalInputV2, ...]
    template_support_refs: tuple[str, ...]
    source_joint_data_refs: tuple[str, ...]
    rows: tuple[tuple[str, Decimal, str], ...]
    stress_refs: tuple[str, ...]
    generation_version: str
    common_scenario_set_id: str
    created_at_ns: int
    computed_at_ns: int
    available_at_ns: int
    expires_at_ns: int
    status: ScenarioGenerationStatusV2
    synthetic_fixture: bool = False
    reason: str | None = None
    seed: int = 0
    scenario_count: int = 0

    def __post_init__(self) -> None:
        for name in ("action_hash", "action_artifact_ref", "common_scenario_set_id"):
            sha256_ref(getattr(self, name), field=name)
        inputs = (self.model_input, self.calibration_input, self.execution_model_input, *self.source_inputs)
        if any(x.kind in FORBIDDEN_PRETRADE_TYPES or x.available_at_ns > self.information_cutoff_ns or x.vintage_at_ns > self.information_cutoff_ns for x in inputs):
            raise ValueError("retrospective/future evidence cannot generate pretrade scenarios")
        if self.source_inputs != tuple(sorted(self.source_inputs, key=lambda x: x.ref)) or len({x.ref for x in inputs}) != len(inputs):
            raise ValueError("causal scenario refs must be unique and sorted")
        if self.template_support_refs != tuple(sorted(set(self.template_support_refs))):
            raise ValueError("scenario template support refs must be sorted and unique")
        if self.source_joint_data_refs != tuple(sorted(set(self.source_joint_data_refs))):
            raise ValueError("scenario source joint data refs must be sorted and unique")
        for ref in self.template_support_refs + self.source_joint_data_refs:
            sha256_ref(ref, field="template_support_ref")
        if not self.information_cutoff_ns <= self.created_at_ns <= self.computed_at_ns <= self.available_at_ns < self.expires_at_ns:
            raise ValueError("scenario generation must finish before action expiry")
        object.__setattr__(self, "status", ScenarioGenerationStatusV2(self.status))
        if type(self.synthetic_fixture) is not bool:
            raise ValueError("synthetic fixture marker must be boolean")
        if type(self.seed) is not int or self.seed < 0 or type(self.scenario_count) is not int or self.scenario_count <= 0:
            raise ValueError("scenario run seed/path count must be immutable positive-count evidence")
        if self.status == ScenarioGenerationStatusV2.NOT_ESTIMABLE:
            if self.rows or not self.reason or self.template_support_refs:
                raise ValueError("NOT_ESTIMABLE scenario has no probability rows and requires a reason")
        elif self.reason is not None or not self.rows or not self.template_support_refs:
            raise ValueError("available scenario requires rows and no inability reason")
        if self.rows:
            if self.rows != tuple(sorted(self.rows, key=lambda x: x[0])) or len({x[0] for x in self.rows}) != len(self.rows):
                raise ValueError("scenario rows must be unique and path ordered")
            if sum((prob for _, prob, _ in self.rows), ZERO) != Decimal(1):
                raise ValueError("scenario probabilities must sum exactly to one")
            for path_id, probability, payload_ref in self.rows:
                sha256_ref(path_id, field="joint_path_id")
                sha256_ref(payload_ref, field="joint payload ref")
                if probability <= 0 or probability > 1:
                    raise ValueError("scenario path probability must be in (0,1]")
        if self.stress_refs != tuple(sorted(set(self.stress_refs))):
            raise ValueError("deterministic stress refs must be sorted and unique")
        for ref in self.stress_refs:
            sha256_ref(ref, field="stress_ref")

    def _body(self) -> dict[str, Any]:
        return json_value({"version": PRETRADE_EXECUTION_SCENARIO_VERSION,
            **{name: getattr(self, name) for name in self.__dataclass_fields__ if name != "rows"},
            "rows": [{"joint_path_id": path_id, "probability": probability, "payload_ref": payload_ref}
                     for path_id, probability, payload_ref in self.rows]})

    def to_dict(self) -> dict[str, Any]:
        return {**self._body(), "content_hash": self.content_hash}

    @property
    def content_hash(self) -> str:
        return sha256_json(self._body())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PretradeExecutionScenarioV2:
        fields = set(cls.__dataclass_fields__) | {"version", "content_hash"}
        d = dict(strict_fields(data, expected=fields, required=fields, name="PretradeExecutionScenarioV2"))
        if d["version"] != PRETRADE_EXECUTION_SCENARIO_VERSION or not isinstance(d["rows"], list):
            raise ValueError("unsupported pretrade execution scenario version")
        for name in ("model_input", "calibration_input", "execution_model_input"):
            d[name] = CausalInputV2.from_dict(d[name])
        d["source_inputs"] = tuple(CausalInputV2.from_dict(x) for x in d["source_inputs"])
        d["template_support_refs"] = tuple(d["template_support_refs"])
        d["source_joint_data_refs"] = tuple(d["source_joint_data_refs"])
        d["rows"] = tuple((r["joint_path_id"], decimal_value(r["probability"], field="probability", wire=True), r["payload_ref"]) for r in d["rows"])
        d["stress_refs"] = tuple(d["stress_refs"])
        result = cls(**{name: d[name] for name in cls.__dataclass_fields__})
        if result.content_hash != d["content_hash"]:
            raise ValueError("pretrade execution scenario content hash mismatch")
        return result


def _lookup_causal(repo: OpsRepository, evidence: CausalInputV2, cutoff_ns: int) -> None:
    if evidence.kind in FORBIDDEN_PRETRADE_TYPES:
        raise ValueError("retrospective replay cannot be a pretrade input")
    entry = repo.get_artifact(evidence.ref)
    if (entry is None or entry.artifact_type != evidence.kind or entry.content_hash != evidence.ref
            or entry.available_at_ns != evidence.available_at_ns or entry.available_at_ns > cutoff_ns
            or entry.created_at_ns > cutoff_ns or evidence.vintage_at_ns > cutoff_ns
            or sha256_json(entry.metadata) != evidence.ref):
        raise ValueError("scenario input/model/calibration not available by decision cutoff")


def _lookup_joint_data(repo: OpsRepository, ref: str) -> JointExecutionDataV2:
    entry = repo.get_artifact(ref)
    body = entry.metadata.get("joint_execution_data") if entry is not None else None
    if entry is None or entry.artifact_type != "JointExecutionDataV2" or entry.content_hash != ref or not isinstance(body, Mapping):
        raise ValueError("strict typed V2 execution data required")
    data = JointExecutionDataV2.from_dict(json_value(body))
    if data.content_hash != ref or entry.available_at_ns != data.available_at_ns:
        raise ValueError("joint execution data identity mismatch")
    return data


def index_joint_execution_data(repo: OpsRepository, data: JointExecutionDataV2) -> str:
    for ref in (data.source_ref, data.execution_model_ref, data.fee_ref):
        item = repo.get_artifact(ref)
        if item is None or item.available_at_ns > data.available_at_ns:
            raise ValueError("joint execution input source unavailable")
    repo.register_artifact(ArtifactIndexEntryV2(data.content_hash, "JointExecutionDataV2", data.content_hash,
        data.computed_at_ns, data.available_at_ns, {"joint_execution_data": data.to_dict()}))
    return data.content_hash


def index_s2_management_evidence(repo: OpsRepository, evidence: S2ScenarioManagementV2) -> str:
    repo.register_artifact(ArtifactIndexEntryV2(evidence.content_hash, "S2ScenarioManagementV2",
        evidence.content_hash, evidence.available_at_ns, evidence.available_at_ns, evidence.to_dict()))
    return evidence.content_hash


def _payoff(action: ActionArtifactV2, data: JointExecutionDataV2, fee: FeeScheduleV2,
            base_units: Decimal, scenario_ref: str, payload_ref: str, available_at_ns: int) -> PretradePathPayoffV2:
    frozen = action.action
    if (frozen.stop_trigger_basis != "MARK_PRICE" or
            frozen.entry_rule.get("order_type") != "LIMIT" or
            frozen.entry_rule.get("time_in_force") != "IOC" or
            frozen.entry_rule.get("no_same_epoch_reprice") is not True or
            frozen.collar_rule.get("long_reference") != "ask" or
            frozen.collar_rule.get("short_reference") != "bid"):
        raise ValueError("pretrade action lacks exact IOC/collar/MarkPrice mechanics")
    if data.action_hash != frozen.action_hash or data.action_artifact_ref != action.content_hash or data.requested_quantity != frozen.quantity:
        raise ValueError("scenario data changes frozen action identity or quantity")
    if data.entry_state == ScenarioFillStateV2.NO_FILL:
        return PretradePathPayoffV2(frozen.action_hash, scenario_ref, data.joint_path_id,
            payload_ref, data.execution_model_ref,
            ZERO, ZERO, ZERO, ZERO, ZERO, ZERO, available_at_ns)
    assert data.entry_at_ns is not None and data.exit_at_ns is not None
    assert data.entry_price is not None and data.exit_price is not None
    if data.exit_at_ns > frozen.horizon_end_ns:
        raise ValueError("scenario exit exceeds exact frozen action horizon")
    first = next((p for p in data.points if p.at_ns == data.entry_at_ns), None)
    last = next((p for p in data.points if p.at_ns == data.exit_at_ns), None)
    if first is None or last is None:
        raise ValueError("missing joint quote at exact entry/exit execution ordering")
    if frozen.side == "LONG":
        if data.entry_price > frozen.entry_collar or data.entry_quantity > first.ask_depth or data.exit_quantity > last.bid_depth:
            raise ValueError("long entry collar or executable depth is breached")
        if data.entry_price != first.ask_price or data.exit_price != last.bid_price:
            raise ValueError("long cashflows must use explicit executable ask/bid")
        entry = -(data.entry_quantity * data.entry_price * base_units)
        exit_ = data.exit_quantity * data.exit_price * base_units
    else:
        if data.entry_price < frozen.entry_collar or data.entry_quantity > first.bid_depth or data.exit_quantity > last.ask_depth:
            raise ValueError("short entry collar or executable depth is breached")
        if data.entry_price != first.bid_price or data.exit_price != last.ask_price:
            raise ValueError("short cashflows must use explicit executable bid/ask")
        entry = data.entry_quantity * data.entry_price * base_units
        exit_ = -(data.exit_quantity * data.exit_price * base_units)
    fees = (data.entry_quantity * data.entry_price * base_units * fee.entry_taker_rate +
            data.exit_quantity * data.exit_price * base_units * fee.exit_taker_rate)
    # Funding is represented on the coherent path. It is included once for
    # each timestamp strictly after entry and through the exit timestamp.
    funding = sum((p.funding_cashflow_usdt for p in data.points if data.entry_at_ns < p.at_ns <= data.exit_at_ns), ZERO)
    if frozen.policy_id == "S2_COMPRESSION_BREAKOUT":
        if data.s2_management_ref is None:
            raise ValueError("S2 pretrade path lacks explicit failed-break/management semantics")
    elif data.s2_management_ref is not None or data.exit_reason == ExitReasonV2.FAILED_BREAK:
        raise ValueError("S1 path cannot carry S2 failed-break exit semantics")
    trigger_at = next((p.at_ns for p in data.points if data.entry_at_ns <= p.at_ns <= data.exit_at_ns and (
        p.mark_price <= frozen.stop_price if frozen.side == "LONG" else p.mark_price >= frozen.stop_price
    )), None)
    if data.exit_reason == ExitReasonV2.STOP:
        assert data.exit_latency_ns is not None
        if trigger_at is None or data.exit_at_ns != trigger_at + data.exit_latency_ns:
            raise ValueError("MarkPrice stop trigger or execution ordering is unsupported")
    if data.exit_reason == ExitReasonV2.TIME_EXIT and (
            trigger_at is not None or data.exit_at_ns != frozen.horizon_end_ns or data.exit_latency_ns != 0):
        raise ValueError("time exit ignores an earlier stop or changes exact frozen horizon/latency")
    return PretradePathPayoffV2(frozen.action_hash, scenario_ref, data.joint_path_id,
        payload_ref, data.execution_model_ref,
        entry, exit_, fees, funding, entry + exit_ - fees + funding,
        data.entry_quantity, available_at_ns)


def _validate_policy_management(repo: OpsRepository, action: ActionArtifactV2,
        data: JointExecutionDataV2) -> None:
    candidate_entry = repo.get_artifact(action.candidate_ref)
    candidate_body = candidate_entry.metadata.get("candidate") if candidate_entry is not None else None
    candidate = CandidateActionV2.from_dict(json_value(candidate_body)) if isinstance(candidate_body, Mapping) else None
    if candidate is None or candidate.content_hash != action.candidate_ref:
        raise ValueError("scenario candidate selection is unavailable")
    if data.entry_state == ScenarioFillStateV2.NO_FILL:
        return
    if data.entry_at_ns is None or data.entry_at_ns >= candidate.deadline_ns:
        raise ValueError("scenario entry occurs after exact frozen action expiry")
    if action.action.policy_id != "S2_COMPRESSION_BREAKOUT":
        if data.s2_management_ref is not None or data.exit_reason == ExitReasonV2.FAILED_BREAK:
            raise ValueError("S1 path cannot carry S2 failed-break management semantics")
        return
    ref = data.s2_management_ref
    indexed = repo.get_artifact(ref) if ref is not None else None
    body = indexed.metadata if indexed is not None else None
    management = S2ScenarioManagementV2.from_dict(json_value(body)) if isinstance(body, Mapping) else None
    setup_entry = repo.get_artifact(management.setup_ref) if management is not None else None
    setup_body = setup_entry.metadata if setup_entry is not None else None
    setup_range_low = setup_body.get("range_low") if isinstance(setup_body, Mapping) else None
    setup_range_high = setup_body.get("range_high") if isinstance(setup_body, Mapping) else None
    if (indexed is None or indexed.artifact_type != "S2ScenarioManagementV2" or
            not isinstance(body, Mapping) or sha256_json(body) != ref or management is None or
            indexed.available_at_ns > candidate.decision_at_ns or management.available_at_ns > candidate.decision_at_ns or
            management.action_hash != action.action.action_hash or
            management.joint_path_id != data.joint_path_id or management.exit_reason != data.exit_reason or
            management.management_rule_hash != sha256_json(action.action.management_rule.to_dict()) or
            management.setup_ref not in candidate.envelope.input_refs or
            setup_entry is None or setup_entry.artifact_type != "S2SetupEvidenceV1" or
            setup_entry.content_hash != management.setup_ref or not isinstance(setup_body, Mapping) or
            sha256_json(setup_body) != management.setup_ref or setup_body.get("version") != "S2_SETUP_V1" or
            setup_body.get("policy_hash") != action.action.policy_hash or
            setup_body.get("key") != action.action.key.to_dict() or
            not isinstance(setup_range_low, str) or not isinstance(setup_range_high, str) or
            decimal_value(setup_range_low, field="range_low", wire=True) != management.range_low or
            decimal_value(setup_range_high, field="range_high", wire=True) != management.range_high or
            (action.action.side == "LONG" and action.action.stop_price != management.range_low) or
            (action.action.side == "SHORT" and action.action.stop_price != management.range_high)):
        raise ValueError("S2 management semantics missing")
    first_two = ((candidate.decision_at_ns + 900_000_000_000, management.subsequent_closed_bars[0][1]),
        (candidate.decision_at_ns + 1_800_000_000_000, management.subsequent_closed_bars[1][1]))
    path_points = {point.at_ns: point for point in data.points}
    if any(at not in path_points or path_points[at].last_price != close for at, close in first_two):
        raise ValueError("S2 failed-break bars are absent from coherent joint market path")
    failed_at = next((at for at, close in first_two
        if management.range_low <= close <= management.range_high), None)
    stop_at = next((point.at_ns for point in data.points if data.entry_at_ns <= point.at_ns <= action.action.horizon_end_ns and (
        point.mark_price <= action.action.stop_price if action.action.side == "LONG" else
        point.mark_price >= action.action.stop_price)), None)
    if stop_at is not None and (failed_at is None or stop_at <= failed_at):
        if management.failed_break_at_ns is not None or data.exit_reason != ExitReasonV2.STOP:
            raise ValueError("S2 path ignores earlier MarkPrice stop precedence")
        return
    if (management.failed_break_at_ns != failed_at or
            (failed_at is None and data.exit_reason == ExitReasonV2.FAILED_BREAK) or
            (failed_at is not None and (data.exit_reason != ExitReasonV2.FAILED_BREAK or
                data.exit_at_ns is None or data.exit_latency_ns is None or
                data.exit_at_ns != failed_at + data.exit_latency_ns))):
        raise ValueError("S2 failed-break exit disagrees with the first two post-entry closed bars")


def _sampled_probability_rows(supported: Sequence[JointExecutionDataV2], *, seed: int,
        scenario_count: int) -> tuple[tuple[str, Decimal], ...]:
    """Reproduce the exact path frequencies from the frozen joint-template set."""
    rng = random.Random(seed)
    counts: dict[str, int] = {}
    ordered = tuple(supported)
    for _ in range(scenario_count):
        path_id = ordered[rng.randrange(len(ordered))].joint_path_id
        counts[path_id] = counts.get(path_id, 0) + 1
    rows = sorted(counts.items())
    probabilities = [Decimal(count) / Decimal(scenario_count) for _, count in rows]
    if probabilities:
        probabilities[-1] = Decimal(1) - sum(probabilities[:-1], ZERO)
    return tuple((path_id, probability) for (path_id, _), probability in
        zip(rows, probabilities, strict=True))


def generate_pretrade_scenarios(repo: OpsRepository, *, action: ActionArtifactV2,
        model_input: CausalInputV2, calibration_input: CausalInputV2,
        execution_model_input: CausalInputV2, source_inputs: Sequence[CausalInputV2],
        joint_data_refs: Sequence[str], fee: FeeScheduleV2, base_units_per_contract: Decimal,
        cutoff_ns: int, created_at_ns: int, computed_at_ns: int, available_at_ns: int,
        expires_at_ns: int, seed: int, scenario_count: int,
        stress_refs: Sequence[str] = (), allow_synthetic_fixtures: bool = False) -> tuple[PretradeExecutionScenarioV2, tuple[PretradePathPayoffV2, ...]]:
    """Resample intact joint paths with a deterministic seed; support stays template-count based."""
    if scenario_count <= 0 or seed < 0:
        raise ValueError("scenario count must be positive and seed nonnegative")
    if not isinstance(base_units_per_contract, Decimal) or not base_units_per_contract.is_finite() or base_units_per_contract <= 0:
        raise ValueError("base units must be positive Decimal")
    candidate_index = repo.get_artifact(action.candidate_ref)
    candidate_body = candidate_index.metadata.get("candidate") if candidate_index is not None else None
    if not isinstance(candidate_body, Mapping):
        raise ValueError("exact candidate expiry/deadline evidence is required")
    candidate_deadline = candidate_body.get("deadline_ns")
    if (type(candidate_deadline) is not int or candidate_body.get("decision_at_ns") != cutoff_ns or
            expires_at_ns > candidate_deadline or
            not cutoff_ns <= created_at_ns <= computed_at_ns <= available_at_ns < expires_at_ns):
        raise ValueError("scenario chronology exceeds candidate decision window")
    if (action.available_at_ns > cutoff_ns or action.action.action_hash != sha256_json(action.action.to_dict())
            or fee.available_at_ns > cutoff_ns or fee.key != action.action.key):
        raise ValueError("exact action/fee evidence unavailable by cutoff")
    fee_entry = repo.get_artifact(fee.content_hash)
    fee_body = fee_entry.metadata if fee_entry is not None else None
    if (fee_entry is None or fee_entry.artifact_type != "FeeScheduleV2" or
            fee_entry.available_at_ns > cutoff_ns or not isinstance(fee_body, Mapping) or
            sha256_json(fee_body) != fee.content_hash or fee_body.get("version") != "V2_TAKER_FEES_V1"):
        raise ValueError("indexed fee schedule is required by cutoff")
    inputs = (model_input, calibration_input, execution_model_input, *source_inputs)
    for causal in inputs:
        _lookup_causal(repo, causal, cutoff_ns)
    if any(x.kind in FORBIDDEN_PRETRADE_TYPES for x in inputs):
        raise ValueError("retrospective outcome/replay evidence is forbidden")
    sources = tuple(sorted(source_inputs, key=lambda x: x.ref))
    source_refs = {source.ref for source in sources}
    if tuple(joint_data_refs) != tuple(sorted(set(joint_data_refs))):
        raise ValueError("joint execution data refs must be sorted and unique")
    data = tuple(_lookup_joint_data(repo, ref) for ref in joint_data_refs)
    if any(d.available_at_ns > cutoff_ns or d.computed_at_ns > cutoff_ns or
           d.information_cutoff_ns > cutoff_ns for d in data):
        raise ValueError("joint execution template is future evidence for this cutoff")
    source_data_refs = tuple(d.content_hash for d in data)
    # A real generator needs cutoff-qualified joint execution templates and an
    # explicit fee schedule. Empty/unsupported support is represented as an
    # honest non-estimable artifact, never synthetic probabilities.
    supported = tuple(d for d in data if d.available_at_ns <= cutoff_ns and
        d.action_hash == action.action.action_hash and d.action_artifact_ref == action.content_hash and
        d.requested_quantity == action.action.quantity and d.execution_model_ref == execution_model_input.ref and
        d.fee_ref == fee.content_hash and d.source_ref in source_refs and
        d.information_cutoff_ns <= cutoff_ns and d.execution_depth_qualified and
        (allow_synthetic_fixtures or not d.synthetic_fixture))
    manifest = sha256_json({"version": "PRETRADE_EXECUTION_CAUSAL_MANIFEST_V1",
        "inputs": [x.to_dict() for x in inputs], "data_refs": sorted(x.content_hash for x in data),
        "generator": SCENARIO_GENERATOR_VERSION})
    common_id = sha256_json({"action_hash": action.action.action_hash, "cutoff_ns": cutoff_ns,
        "manifest": manifest, "scenario_version": PRETRADE_EXECUTION_SCENARIO_VERSION})

    def persist_scenario(artifact: PretradeExecutionScenarioV2, template_support_count: int) -> None:
        repo.register_artifact(ArtifactIndexEntryV2(artifact.content_hash, "PretradeExecutionScenarioV2",
            artifact.content_hash, artifact.created_at_ns, artifact.available_at_ns,
            {"scenario": artifact.to_dict(), "seed": artifact.seed, "scenario_count": artifact.scenario_count,
             "template_support_count": template_support_count}))

    if not supported:
        scenario = PretradeExecutionScenarioV2(action.action.action_hash, action.content_hash, cutoff_ns,
            model_input, calibration_input, execution_model_input, sources, (), source_data_refs, (), tuple(sorted(set(stress_refs))),
            SCENARIO_GENERATOR_VERSION, common_id, created_at_ns, computed_at_ns, available_at_ns,
            expires_at_ns, ScenarioGenerationStatusV2.NOT_ESTIMABLE,
            reason="MISSING_QUALIFIED_JOINT_EXECUTION_SUPPORT", seed=seed, scenario_count=scenario_count)
        persist_scenario(scenario, 0)
        return scenario, ()
    if len({datum.joint_path_id for datum in supported}) != len(supported):
        scenario = PretradeExecutionScenarioV2(action.action.action_hash, action.content_hash, cutoff_ns,
            model_input, calibration_input, execution_model_input, sources, (), source_data_refs, (), tuple(sorted(set(stress_refs))),
            SCENARIO_GENERATOR_VERSION, common_id, created_at_ns, computed_at_ns, available_at_ns,
            expires_at_ns, ScenarioGenerationStatusV2.NOT_ESTIMABLE,
            reason="DUPLICATE_JOINT_PATH_TEMPLATE_IDENTITY", seed=seed, scenario_count=scenario_count)
        persist_scenario(scenario, 0)
        return scenario, ()
    if computed_at_ns < max(d.computed_at_ns for d in supported):
        raise ValueError("scenario construction completes before source template processing")
    if any(d.joint_path_id in FORBIDDEN_PRETRADE_TYPES for d in supported):
        raise ValueError("invalid retrospective joint path")
    # Validate mechanics before writing any path/payload that the downstream
    # scenario could appear to endorse. Missing ordering remains NOT_ESTIMABLE.
    try:
        for datum in supported:
            _validate_policy_management(repo, action, datum)
            _payoff(action, datum, fee, base_units_per_contract,
                sha256_json({"preflight_scenario": datum.content_hash}),
                sha256_json({"preflight_payload": datum.content_hash}), available_at_ns)
    except ValueError:
        scenario = PretradeExecutionScenarioV2(action.action.action_hash, action.content_hash, cutoff_ns,
            model_input, calibration_input, execution_model_input, sources, (), source_data_refs, (), tuple(sorted(set(stress_refs))),
            SCENARIO_GENERATOR_VERSION, common_id, created_at_ns, computed_at_ns, available_at_ns,
            expires_at_ns, ScenarioGenerationStatusV2.NOT_ESTIMABLE,
            reason="SCENARIO_POLICY_OR_EXECUTION_MECHANICS_UNRESOLVED", seed=seed,
            scenario_count=scenario_count)
        persist_scenario(scenario, 0)
        return scenario, ()
    # The draw unit is the complete joint path, never an independently shuffled component.
    sampled_rows = _sampled_probability_rows(supported, seed=seed, scenario_count=scenario_count)
    payload_refs: dict[str, str] = {}
    data_by_path = {datum.joint_path_id: datum for datum in supported}
    for datum in supported:
        payload = JointExecutionPayloadV2(datum.action_hash, datum.action_artifact_ref, cutoff_ns,
            common_id, datum.joint_path_id, SCENARIO_GENERATOR_VERSION, PRETRADE_EXECUTION_SCENARIO_VERSION,
            manifest, datum.content_hash, created_at_ns, computed_at_ns, available_at_ns)
        payload_refs[datum.joint_path_id] = payload.content_hash
        repo.register_artifact(ArtifactIndexEntryV2(payload.content_hash, "JointExecutionPayloadV2",
            payload.content_hash, created_at_ns, available_at_ns, {"joint_execution_payload": payload.to_dict()}))
    probability_rows = tuple((path_id, probability, payload_refs[path_id])
                             for path_id, probability in sampled_rows)
    synthetic_fixture = any(item.synthetic_fixture for item in supported)
    scenario = PretradeExecutionScenarioV2(action.action.action_hash, action.content_hash, cutoff_ns,
        model_input, calibration_input, execution_model_input, sources,
        tuple(sorted(datum.content_hash for datum in supported)), source_data_refs, probability_rows,
        tuple(sorted(set(stress_refs))), SCENARIO_GENERATOR_VERSION, common_id, created_at_ns,
        computed_at_ns, available_at_ns, expires_at_ns, ScenarioGenerationStatusV2.AVAILABLE,
        synthetic_fixture, seed=seed, scenario_count=scenario_count)
    scenario_ref = scenario.content_hash
    payoffs: list[PretradePathPayoffV2] = []
    for path_id, _, payload_ref in probability_rows:
        datum = data_by_path[path_id]
        payoff = _payoff(action, datum, fee, base_units_per_contract, scenario_ref, payload_ref, available_at_ns)
        payoffs.append(payoff)
        repo.register_artifact(ArtifactIndexEntryV2(payoff.content_hash, "PretradePathPayoffV2",
            payoff.content_hash, available_at_ns, available_at_ns, {"path_payoff": payoff.to_dict(), "payload_ref": payload_ref}))
    persist_scenario(scenario, len(supported))
    return scenario, tuple(payoffs)


def validate_pretrade_scenario_evidence(repo: OpsRepository, *, action: ActionArtifactV2,
        scenario: PretradeExecutionScenarioV2) -> tuple[PretradePathPayoffV2, ...]:
    """Resolve scenario lineage and reproduce each exact frozen-action cashflow."""
    frozen = action.action
    if (scenario.action_hash != frozen.action_hash or scenario.action_artifact_ref != action.content_hash or
            scenario.information_cutoff_ns != action_identity_cutoff(repo, action) or
            scenario.generation_version != SCENARIO_GENERATOR_VERSION):
        raise ValueError("pretrade scenario/action/cutoff identity mismatch")
    inputs = (scenario.model_input, scenario.calibration_input,
        scenario.execution_model_input, *scenario.source_inputs)
    for item in inputs:
        _lookup_causal(repo, item, scenario.information_cutoff_ns)
    if tuple(scenario.source_joint_data_refs) != tuple(sorted(set(scenario.source_joint_data_refs))):
        raise ValueError("pretrade source joint data refs are not canonical")
    data = tuple(_lookup_joint_data(repo, ref) for ref in scenario.source_joint_data_refs)
    if any(item.available_at_ns > scenario.information_cutoff_ns or
            item.computed_at_ns > scenario.information_cutoff_ns or
            item.information_cutoff_ns > scenario.information_cutoff_ns for item in data):
        raise ValueError("pretrade joint data is future evidence")
    if any((entry := repo.get_artifact(item.content_hash)) is None or
            entry.created_at_ns > scenario.information_cutoff_ns for item in data):
        raise ValueError("pretrade joint data artifact was created after its information cutoff")
    manifest = sha256_json({"version": "PRETRADE_EXECUTION_CAUSAL_MANIFEST_V1",
        "inputs": [item.to_dict() for item in inputs],
        "data_refs": list(scenario.source_joint_data_refs), "generator": scenario.generation_version})
    common_id = sha256_json({"action_hash": frozen.action_hash,
        "cutoff_ns": scenario.information_cutoff_ns, "manifest": manifest,
        "scenario_version": PRETRADE_EXECUTION_SCENARIO_VERSION})
    if common_id != scenario.common_scenario_set_id:
        raise ValueError("pretrade scenario causal manifest/common path identity mismatch")
    if scenario.status == ScenarioGenerationStatusV2.NOT_ESTIMABLE:
        return ()
    source_refs = {item.ref for item in scenario.source_inputs}
    supported = tuple(item for item in data if item.action_hash == frozen.action_hash and
        item.action_artifact_ref == action.content_hash and item.requested_quantity == frozen.quantity and
        item.execution_model_ref == scenario.execution_model_input.ref and
        item.source_ref in source_refs and item.information_cutoff_ns <= scenario.information_cutoff_ns and
        item.execution_depth_qualified and (scenario.synthetic_fixture or not item.synthetic_fixture))
    if (tuple(sorted(item.content_hash for item in supported)) != scenario.template_support_refs or
            not supported or len({item.joint_path_id for item in supported}) != len(supported) or
            scenario.synthetic_fixture != any(item.synthetic_fixture for item in supported)):
        raise ValueError("pretrade scenario template support does not reproduce from cutoff evidence")
    scenario_entry = repo.get_artifact(scenario.content_hash)
    run_metadata = scenario_entry.metadata if scenario_entry is not None else None
    seed = run_metadata.get("seed") if isinstance(run_metadata, Mapping) else None
    scenario_count = run_metadata.get("scenario_count") if isinstance(run_metadata, Mapping) else None
    if (scenario_entry is None or scenario_entry.artifact_type != "PretradeExecutionScenarioV2" or
            scenario_entry.content_hash != scenario.content_hash or scenario_entry.created_at_ns != scenario.created_at_ns or
            scenario_entry.available_at_ns != scenario.available_at_ns or
            not isinstance(run_metadata, Mapping) or
            canonical_json(run_metadata.get("scenario")) != canonical_json(scenario.to_dict()) or
            type(seed) is not int or seed < 0 or type(scenario_count) is not int or scenario_count <= 0 or
            seed != scenario.seed or scenario_count != scenario.scenario_count or
            run_metadata.get("template_support_count") != len(supported)):
        raise ValueError("pretrade scenario lacks immutable seed/path-count run metadata")
    expected_probabilities = _sampled_probability_rows(supported, seed=seed, scenario_count=scenario_count)
    actual_probabilities = tuple((path_id, probability) for path_id, probability, _ in scenario.rows)
    if expected_probabilities != actual_probabilities:
        raise ValueError("pretrade scenario probabilities do not reproduce from declared seed/path count")
    data_by_path = {item.joint_path_id: item for item in supported}
    if any(path_id not in data_by_path for path_id, _, _ in scenario.rows):
        raise ValueError("scenario probability row lacks its coherent joint template")
    product_entry = repo.get_artifact(frozen.product_ref)
    product_body = product_entry.metadata.get("product") if product_entry is not None else None
    product = ProductContractV2.from_dict(json_value(product_body)) if isinstance(product_body, Mapping) else None
    if product is None or product.content_hash != frozen.product_ref or product.key != frozen.key:
        raise ValueError("pretrade scenario product multiplier is unresolved")
    product_entry = repo.get_artifact(frozen.product_ref)
    if product_entry is None or product_entry.available_at_ns > scenario.information_cutoff_ns:
        raise ValueError("pretrade scenario product was unavailable by cutoff")
    payoffs: list[PretradePathPayoffV2] = []
    for path_id, _, payload_ref in scenario.rows:
        payload_entry = repo.get_artifact(payload_ref)
        payload_body = payload_entry.metadata.get("joint_execution_payload") if payload_entry is not None else None
        payload = JointExecutionPayloadV2.from_dict(json_value(payload_body)) if isinstance(payload_body, Mapping) else None
        datum = data_by_path[path_id]
        if (payload_entry is None or payload_entry.artifact_type != "JointExecutionPayloadV2" or
                payload_entry.content_hash != payload_ref or payload is None or payload.content_hash != payload_ref or
                payload_entry.available_at_ns != scenario.available_at_ns or
                payload.action_hash != frozen.action_hash or payload.action_artifact_ref != action.content_hash or
                payload.information_cutoff_ns != scenario.information_cutoff_ns or
                payload.common_scenario_set_id != scenario.common_scenario_set_id or
                payload.joint_path_id != path_id or payload.generation_version != scenario.generation_version or
                payload.scenario_version != PRETRADE_EXECUTION_SCENARIO_VERSION or
                payload.source_manifest_hash != manifest or payload.joint_data_ref != datum.content_hash or
                payload.created_at_ns != scenario.created_at_ns or payload.computed_at_ns != scenario.computed_at_ns or
                payload.available_at_ns != scenario.available_at_ns):
            raise ValueError("pretrade joint payload does not resolve to exact cutoff scenario lineage")
        fee_entry = repo.get_artifact(datum.fee_ref)
        fee_body = fee_entry.metadata if fee_entry is not None else None
        if (fee_entry is None or fee_entry.artifact_type != "FeeScheduleV2" or
                fee_entry.available_at_ns > scenario.information_cutoff_ns or
                not isinstance(fee_body, Mapping) or sha256_json(fee_body) != datum.fee_ref or
                fee_body.get("version") != "V2_TAKER_FEES_V1"):
            raise ValueError("pretrade fee model is unavailable by exact action cutoff")
        fee = FeeScheduleV2(InstrumentKeyV2.from_dict(fee_body["key"]), fee_body["available_at_ns"],
            decimal_value(fee_body["entry_taker_rate"], field="entry_taker_rate", wire=True),
            decimal_value(fee_body["exit_taker_rate"], field="exit_taker_rate", wire=True),
            fee_body["source_ref"])
        if fee.content_hash != datum.fee_ref:
            raise ValueError("pretrade fee schedule content hash mismatch")
        if (datum.fee_ref != fee.content_hash or datum.execution_model_ref != scenario.execution_model_input.ref or
                datum.source_ref not in {item.ref for item in scenario.source_inputs}):
            raise ValueError("joint path fee, execution model or source differs from scenario manifest")
        _validate_policy_management(repo, action, datum)
        payoff = _payoff(action, datum, fee, product.base_units_per_contract,
            scenario.content_hash, payload_ref, scenario.available_at_ns)
        payoff_entry = repo.get_artifact(payoff.content_hash)
        payoff_body = payoff_entry.metadata.get("path_payoff") if payoff_entry is not None else None
        if (payoff_entry is None or payoff_entry.artifact_type != "PretradePathPayoffV2" or
                payoff_entry.content_hash != payoff.content_hash or payoff_entry.available_at_ns != scenario.available_at_ns or
                not isinstance(payoff_body, Mapping) or sha256_json(payoff_body) != payoff.content_hash or
                canonical_json(payoff_body) != canonical_json(payoff.to_dict()) or
                payoff_entry.metadata.get("payload_ref") != payload_ref):
            raise ValueError("pretrade exact action cashflow payoff is missing or non-reproducible")
        payoffs.append(payoff)
    return tuple(payoffs)


def action_identity_cutoff(repo: OpsRepository, action: ActionArtifactV2) -> int:
    candidate_entry = repo.get_artifact(action.candidate_ref)
    candidate_body = candidate_entry.metadata.get("candidate") if candidate_entry is not None else None
    if candidate_entry is None or not isinstance(candidate_body, Mapping):
        raise ValueError("pretrade scenario exact candidate cutoff unavailable")
    candidate = CandidateActionV2.from_dict(json_value(candidate_body))
    if candidate.content_hash != action.candidate_ref:
        raise ValueError("pretrade scenario candidate content hash mismatch")
    return candidate.decision_at_ns


def scenario_seed(action_hash: str, cutoff_ns: int, seed_version: str = "V1") -> int:
    """Stable seed derivation for independent deterministic convergence runs."""
    sha256_ref(action_hash, field="action_hash")
    digest = hashlib.sha256(f"{seed_version}:{action_hash}:{cutoff_ns}".encode()).digest()
    return int.from_bytes(digest[:8], "big")
