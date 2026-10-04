"""Explicit cutoff-known economic declarations; no invented qualification.

The manifest scopes already indexed source/configuration evidence. Exact-action
binding is a later computation owned by production; registration grants no
selection, sizing, execution or capital authority.
"""
from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from atlas.v2._serialization import canonical_json, json_value, sha256_json, sha256_ref, strict_fields, timestamp
from atlas.v2.instruments import InstrumentKeyV2, ProductContractV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.science.admission import (
    AdmissionPolicyV2,
    ExecutionCalibrationResidualV2,
    ExistingPortfolioPathV2,
    ScenarioSupportUnitV2,
)
from atlas.v2.science.pretrade import CausalInputV2
from atlas.v2.science.scenario_engine import FORBIDDEN_PRETRADE_TYPES, JointExecutionDataV2, scenario_seed

VERSION = "ECONOMIC_SOURCE_MANIFEST_V1"
ARTIFACT_TYPE = "EconomicSourceManifestV1"
SEED_METHOD = "ACTION_HASH_CUTOFF_SCENARIO_SEED_V1"
MAX_REFERENCES = 128
MAX_MANIFESTS = 128
MAX_PATH_POINTS = 16384
_FORBIDDEN = FORBIDDEN_PRETRADE_TYPES | {
    "ActionReplaySourceEvidenceV1", "ActionReplayLifecycleSummaryV1", "ActionReplayMinuteEvidenceV1",
}


@dataclass(frozen=True)
class EconomicSourceManifestV1:
    instrument_key_ref: str
    product_ref: str
    account_scope: str
    policy_hash: str
    admission_policy: AdmissionPolicyV2
    model_input: CausalInputV2
    calibration_input: CausalInputV2
    execution_model_input: CausalInputV2
    scenario_count: int
    available_at_ns: int
    effective_at_ns: int
    source_inputs: tuple[CausalInputV2, ...] = ()
    joint_data_refs: tuple[str, ...] = ()
    support_unit_refs: tuple[str, ...] = ()
    execution_residual_refs: tuple[str, ...] = ()
    stress_input_ref: str | None = None
    existing_portfolio_path_refs: tuple[str, ...] = ()
    authority: str = "ZERO"
    version: str = VERSION
    seed_method: str = SEED_METHOD

    def __post_init__(self) -> None:
        for name in ("instrument_key_ref", "product_ref", "policy_hash"):
            sha256_ref(getattr(self, name), field=name)
        if (not isinstance(self.account_scope, str) or not self.account_scope.strip()
                or len(self.account_scope) > 128 or self.account_scope != self.account_scope.strip()):
            raise ValueError("economic source account scope must be explicit and bounded")
        if (self.version != VERSION or self.seed_method != SEED_METHOD or self.authority != "ZERO"
                or type(self.admission_policy) is not AdmissionPolicyV2):
            raise ValueError("unsupported economic source configuration or authority")
        if type(self.scenario_count) is not int or not 1 <= self.scenario_count <= 10_000:
            raise ValueError("economic scenario count is outside its bounded configuration")
        timestamp(self.available_at_ns, field="manifest available_at_ns")
        timestamp(self.effective_at_ns, field="manifest effective_at_ns")
        if self.effective_at_ns > self.available_at_ns:
            raise ValueError("economic manifest effective time exceeds its publication")
        for name in ("source_inputs", "joint_data_refs", "support_unit_refs", "execution_residual_refs",
                     "existing_portfolio_path_refs"):
            if len(getattr(self, name)) > MAX_REFERENCES:
                raise ValueError("economic declaration wire population exceeds its bound")
        inputs = (self.model_input, self.calibration_input, self.execution_model_input, *self.source_inputs)
        for item in inputs:
            if (type(item) is not CausalInputV2 or item.kind in _FORBIDDEN
                    or item.available_at_ns > self.available_at_ns
                    or item.vintage_at_ns > self.available_at_ns):
                raise ValueError("economic source input is retrospective or unavailable")
        if self.execution_model_input.kind != "ExecutionModelV1":
            raise ValueError("execution source must be the declared ExecutionModelV1 type")
        if tuple(item.ref for item in self.source_inputs) != tuple(sorted({item.ref for item in self.source_inputs})):
            raise ValueError("economic source inputs must be sorted and unique")
        for name in ("joint_data_refs", "support_unit_refs", "execution_residual_refs", "existing_portfolio_path_refs"):
            refs = tuple(getattr(self, name))
            if refs != tuple(sorted(set(refs))):
                raise ValueError("economic evidence references must be sorted and unique")
            for ref in refs:
                sha256_ref(ref, field=name)
            object.__setattr__(self, name, refs)
        object.__setattr__(self, "source_inputs", tuple(self.source_inputs))
        if self.stress_input_ref is not None:
            sha256_ref(self.stress_input_ref, field="stress_input_ref")
        if len(self.input_refs) > MAX_REFERENCES:
            raise ValueError("economic source reference population exceeds its bound")

    @property
    def input_refs(self) -> tuple[str, ...]:
        return tuple(sorted({self.product_ref, self.model_input.ref, self.calibration_input.ref,
            self.execution_model_input.ref, *(item.ref for item in self.source_inputs),
            *self.joint_data_refs, *self.support_unit_refs, *self.execution_residual_refs,
            *self.existing_portfolio_path_refs, *((self.stress_input_ref,) if self.stress_input_ref else ())}))

    def to_dict(self) -> dict[str, Any]:
        return json_value({name: getattr(self, name) for name in self.__dataclass_fields__})

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    def seed(self, action_hash: str, cutoff_ns: int) -> int:
        timestamp(cutoff_ns, field="scenario seed cutoff")
        return scenario_seed(action_hash, cutoff_ns)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EconomicSourceManifestV1:
        names = set(cls.__dataclass_fields__)
        body = dict(strict_fields(data, expected=names, required=names, name=ARTIFACT_TYPE))
        # Refuse the raw wire population before parsing its individual members.
        for name in ("source_inputs", "joint_data_refs", "support_unit_refs", "execution_residual_refs",
                     "existing_portfolio_path_refs"):
            if not isinstance(body[name], (list, tuple)) or len(body[name]) > MAX_REFERENCES:
                raise ValueError("economic declaration wire population exceeds its bound")
        body["admission_policy"] = AdmissionPolicyV2.from_dict(body["admission_policy"])
        for name in ("model_input", "calibration_input", "execution_model_input"):
            body[name] = CausalInputV2.from_dict(body[name])
        if not isinstance(body["source_inputs"], list):
            raise ValueError("economic source inputs wire must be an array")
        body["source_inputs"] = tuple(CausalInputV2.from_dict(item) for item in body["source_inputs"])
        for name in ("joint_data_refs", "support_unit_refs", "execution_residual_refs", "existing_portfolio_path_refs"):
            if not isinstance(body[name], list):
                raise ValueError("economic reference wire must be an array")
            body[name] = tuple(body[name])
        return cls(**body)


def _entry(repository: OpsRepository, ref: str, cutoff_ns: int, kind: str) -> ArtifactIndexEntryV2:
    entry = repository.get_artifact(ref)
    if (entry is None or entry.artifact_type != kind or entry.artifact_ref != ref or entry.content_hash != ref
            or entry.available_at_ns > cutoff_ns or entry.created_at_ns > cutoff_ns):
        raise ValueError("economic source is missing, foreign, future or contradictory")
    return entry


def _scope(body: Mapping[str, Any], manifest: EconomicSourceManifestV1, product: ProductContractV2) -> None:
    for name in ("instrument_key_ref", "product_ref", "account_scope", "policy_hash"):
        if name in body and body[name] != getattr(manifest, name):
            raise ValueError("economic source declares a foreign scope")
    if "key" in body and InstrumentKeyV2.from_dict(json_value(body["key"])) != product.key:
        raise ValueError("economic source declares a foreign instrument")


def _source_clock(body: Mapping[str, Any], available_at_ns: int) -> None:
    for name in ("information_cutoff_ns", "computed_at_ns", "created_at_ns", "vintage_at_ns"):
        if name in body:
            value = timestamp(body[name], field="economic source " + name)
            if value > available_at_ns:
                raise ValueError("economic source body declares a future input or computation")


def _validate_sources(repository: OpsRepository, manifest: EconomicSourceManifestV1) -> None:
    at = manifest.available_at_ns
    entry = _entry(repository, manifest.product_ref, at, "ProductContractV2")
    product = ProductContractV2.from_dict(json_value(entry.metadata["product"]))
    if (product.content_hash != manifest.product_ref or product.key.content_hash != manifest.instrument_key_ref
            or canonical_json(entry.metadata) != canonical_json({"product": product.to_dict()})
            or product.available_at_ns != entry.available_at_ns or product.effective_at_ns > manifest.effective_at_ns):
        raise ValueError("economic source product identity or canonical metadata mismatch")
    for item in (manifest.model_input, manifest.calibration_input, manifest.execution_model_input, *manifest.source_inputs):
        source = _entry(repository, item.ref, at, item.kind)
        if (source.available_at_ns != item.available_at_ns or sha256_json(source.metadata) != item.ref
                or ("available_at_ns" in source.metadata and source.metadata["available_at_ns"] != item.available_at_ns)
                or ("vintage_at_ns" in source.metadata and source.metadata["vintage_at_ns"] != item.vintage_at_ns)):
            raise ValueError("economic source input canonical metadata or availability mismatch")
        _scope(source.metadata, manifest, product)
        _source_clock(source.metadata, source.available_at_ns)
    dependencies = set(manifest.input_refs)
    for kind, wrapper, refs in (
        ("JointExecutionDataV2", "joint_execution_data", manifest.joint_data_refs),
        ("ScenarioSupportUnitV2", "evidence", manifest.support_unit_refs),
        ("ExecutionCalibrationResidualV2", "residual", manifest.execution_residual_refs),
        ("ExistingPortfolioPathV2", "evidence", manifest.existing_portfolio_path_refs),
        ("StressSuiteEvidenceV2", None, (manifest.stress_input_ref,) if manifest.stress_input_ref else ()),
    ):
        for ref in refs:
            source = _entry(repository, ref, at, kind)
            wire = source.metadata if wrapper is None else source.metadata.get(wrapper)
            if isinstance(wire, Mapping):
                if kind == "JointExecutionDataV2":
                    points = wire.get("points")
                    if not isinstance(points, (list, tuple)) or not 1 <= len(points) <= MAX_PATH_POINTS:
                        raise ValueError("economic declaration path population exceeds its bound")
                # A source's provenance belongs to the same bounded manifest
                # population; inspect lengths before hashing or typed parsing.
                for field in ("source_bundle_refs", "source_refs", "exposure_refs"):
                    if field in wire and (not isinstance(wire[field], (list, tuple))
                            or len(wire[field]) > MAX_REFERENCES):
                        raise ValueError("economic declaration provenance population exceeds its bound")
            if not isinstance(wire, Mapping) or sha256_json(wire) != ref:
                raise ValueError("economic optional evidence canonical content mismatch")
            if wrapper is not None and canonical_json(source.metadata) != canonical_json({wrapper: wire}):
                raise ValueError("economic optional evidence canonical metadata mismatch")
            _scope(wire, manifest, product)
            _source_clock(wire, source.available_at_ns)
            if "available_at_ns" in wire and wire["available_at_ns"] != source.available_at_ns:
                raise ValueError("economic optional evidence publication mismatch")
            nested: tuple[str, ...] = ()
            if kind == "JointExecutionDataV2":
                value = JointExecutionDataV2.from_dict(json_value(wire))
                if value.execution_model_ref != manifest.execution_model_input.ref:
                    raise ValueError("economic joint evidence execution source mismatch")
                nested = (value.source_ref, value.execution_model_ref, value.fee_ref,
                          *((value.s2_management_ref,) if value.s2_management_ref else ()))
            elif kind == "ScenarioSupportUnitV2":
                unit = ScenarioSupportUnitV2.from_dict(json_value(wire))
                if (unit.product_ref != manifest.product_ref or unit.venue != product.key.venue
                        or unit.execution_model_ref != manifest.execution_model_input.ref
                        or unit.calibration_ref != manifest.calibration_input.ref):
                    raise ValueError("economic support unit scope mismatch")
                nested = (unit.source_episode_ref, unit.template_ref, unit.execution_model_ref,
                          unit.calibration_ref, *unit.source_bundle_refs)
            elif kind == "ExecutionCalibrationResidualV2":
                residual = ExecutionCalibrationResidualV2.from_dict(json_value(wire), evidence_ref=ref)
                nested = residual.source_refs
            elif kind == "ExistingPortfolioPathV2":
                portfolio = ExistingPortfolioPathV2.from_dict(json_value(wire))
                nested = (*portfolio.exposure_refs, *portfolio.source_refs,
                          *((portfolio.model_ref,) if portfolio.model_ref else ()),
                          *((portfolio.calibration_ref,) if portfolio.calibration_ref else ()))
            elif wire.get("version") != "V2_STRESS_SUITE_EVIDENCE_V1":
                raise ValueError("unsupported economic stress evidence version")
            else:
                declared_refs = wire.get("source_refs")
                if not isinstance(declared_refs, (list, tuple)) or not declared_refs:
                    raise ValueError("economic stress evidence requires declared sources")
                nested = tuple(declared_refs)
            dependencies.update(nested)
            if len(dependencies) > MAX_REFERENCES:
                raise ValueError("economic source dependency population exceeds its bound")
            for dependency in nested:
                sha256_ref(dependency, field="economic nested dependency")
                indexed = repository.get_artifact(dependency)
                if (indexed is None or indexed.artifact_ref != dependency or indexed.content_hash != dependency
                        or indexed.available_at_ns > source.available_at_ns
                        or indexed.created_at_ns > source.available_at_ns or indexed.artifact_type in _FORBIDDEN):
                    raise ValueError("economic optional evidence dependency is unavailable or retrospective")


def index_economic_source_manifest(repository: OpsRepository, manifest: EconomicSourceManifestV1) -> str:
    if type(manifest) is not EconomicSourceManifestV1:
        raise ValueError("typed economic source manifest required")
    _validate_sources(repository, manifest)
    ref = manifest.content_hash
    repository.register_artifact(ArtifactIndexEntryV2(ref, ARTIFACT_TYPE, ref,
        manifest.available_at_ns, manifest.available_at_ns, {"economic_source_manifest": manifest.to_dict()}))
    return ref


def validate_economic_source_manifest(repository: OpsRepository, entry: ArtifactIndexEntryV2,
                                     *, cutoff_ns: int) -> EconomicSourceManifestV1:
    timestamp(cutoff_ns, field="economic source cutoff")
    manifest = EconomicSourceManifestV1.from_dict(json_value(entry.metadata["economic_source_manifest"]))
    if (entry.artifact_type != ARTIFACT_TYPE or entry.artifact_ref != manifest.content_hash
            or entry.content_hash != manifest.content_hash or entry.available_at_ns != manifest.available_at_ns
            or entry.created_at_ns != manifest.available_at_ns or entry.available_at_ns > cutoff_ns
            or manifest.effective_at_ns > cutoff_ns
            or canonical_json(entry.metadata) != canonical_json({"economic_source_manifest": manifest.to_dict()})):
        raise ValueError("economic manifest identity, chronology or canonical metadata mismatch")
    _validate_sources(repository, manifest)
    return manifest


def _population(repository: OpsRepository, cutoff_ns: int) -> tuple[EconomicSourceManifestV1, ...]:
    page = repository.artifact_entries_by_types_page((ARTIFACT_TYPE,), as_of_ns=cutoff_ns, limit=MAX_MANIFESTS + 1)
    if page.invalid_entry_count or len(page.raw_keys) > MAX_MANIFESTS:
        if not repository.read_only:
            pressure = {"version": "OpsActiveWorkPressureV1", "lane": "ECONOMIC_SOURCE_DECLARATIONS",
                "information_cutoff_ns": cutoff_ns, "max_manifest_rows": MAX_MANIFESTS,
                "observed_row_count": len(page.raw_keys), "invalid_entry_count": page.invalid_entry_count,
                "has_more": len(page.raw_keys) > MAX_MANIFESTS,
                "reason": "ECONOMIC_SOURCE_MANIFEST_POPULATION_OVERFLOW_OR_INVALID", "authority": "ZERO"}
            ref = sha256_json(pressure)
            published = max(time.time_ns(), cutoff_ns)
            prior = repository.get_artifact(ref)
            if prior is None:
                repository.register_artifact(ArtifactIndexEntryV2(ref, "OpsActiveWorkPressureV1", ref,
                    published, published, {"pressure": pressure}))
        raise ValueError("ECONOMIC_SOURCE_MANIFEST_POPULATION_OVERFLOW_OR_INVALID")
    return tuple(validate_economic_source_manifest(repository, entry, cutoff_ns=cutoff_ns) for entry in page.entries)


def resolve_economic_source_manifest(repository: OpsRepository, *, product: ProductContractV2,
        account_scope: str, policy_hash: str, cutoff_ns: int) -> tuple[EconomicSourceManifestV1 | None, str | None]:
    try:
        values = tuple(item for item in _population(repository, cutoff_ns)
            if item.instrument_key_ref == product.key.content_hash and item.product_ref == product.content_hash
            and item.account_scope == account_scope and item.policy_hash == policy_hash)
    except (ValueError, TypeError, KeyError):
        return None, "ECONOMIC_SOURCE_MANIFEST_INVALID_OR_OVERFLOW"
    if not values:
        return None, "ECONOMIC_SOURCE_MANIFEST_MISSING"
    latest = max(item.effective_at_ns for item in values)
    recent = tuple(item for item in values if item.effective_at_ns == latest)
    if len(recent) != 1:
        return None, "ECONOMIC_SOURCE_MANIFEST_AMBIGUOUS"
    return recent[0], None


def declared_execution_model(repository: OpsRepository, product: ProductContractV2,
        account_scope: str | None, cutoff_ns: int) -> CausalInputV2 | None:
    if account_scope is None:
        return None
    try:
        values = tuple(item for item in _population(repository, cutoff_ns)
            if item.instrument_key_ref == product.key.content_hash and item.product_ref == product.content_hash
            and item.account_scope == account_scope)
    except (ValueError, TypeError, KeyError):
        return None
    # Each strategy declaration retains its unique latest effective version.
    selected: dict[str, EconomicSourceManifestV1] = {}
    for item in values:
        previous = selected.get(item.policy_hash)
        if previous is None or item.effective_at_ns > previous.effective_at_ns:
            selected[item.policy_hash] = item
        elif item.effective_at_ns == previous.effective_at_ns and item.content_hash != previous.content_hash:
            return None
    models = {canonical_json(item.execution_model_input.to_dict()): item.execution_model_input
              for item in selected.values()}
    return next(iter(models.values())) if len(models) == 1 else None
