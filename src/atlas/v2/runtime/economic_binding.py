"""Bind predeclared, unchanged economic templates to one exact research action.

The caller owns the scoped source manifest. This module never discovers or
models a template, changes its economic assumptions, or creates independence.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from typing import Any

from atlas.v2._serialization import canonical_json, json_value, sha256_json, sha256_ref, timestamp
from atlas.v2.chronology import causal_artifact, chronology_ref, record_computation, sample
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.science.action import ActionArtifactV2
from atlas.v2.science.admission import ScenarioSupportUnitV2, scenario_support_compatibility_classes
from atlas.v2.science.scenario_engine import FORBIDDEN_PRETRADE_TYPES, JointExecutionDataV2

BINDING_VERSION = "EconomicTemplateBindingIdentityV1"
MAX_REQUESTED_REFS = 128
MAX_PROVENANCE_REFS = 4096
MAX_PATH_POINTS = 16384


def _raw(repo: OpsRepository, ref: str, cutoff: int, kind: str | None = None) -> ArtifactIndexEntryV2:
    sha256_ref(ref, field="economic template input")
    item = repo.get_artifact(ref)
    if (item is None or item.artifact_ref != ref or item.content_hash != ref
            or item.available_at_ns > cutoff or item.created_at_ns > cutoff
            or item.artifact_type in FORBIDDEN_PRETRADE_TYPES
            or (kind is not None and item.artifact_type != kind)):
        raise ValueError("economic template source is missing, future, foreign or retrospective")
    return item


def _fact(repo: OpsRepository, ref: str, cutoff: int) -> ArtifactIndexEntryV2:
    item = _raw(repo, ref, cutoff)
    if sha256_json(item.metadata) != ref:
        raise ValueError("economic template source/model/cost hash mismatch")
    return item


def _identity(kind: str, action_ref: str, original_ref: str, cutoff: int) -> dict[str, Any]:
    return {"version": BINDING_VERSION, "artifact_type": kind, "action_ref": action_ref,
            "binding_source_ref": original_ref, "market_information_cutoff_ns": cutoff, "authority": "ZERO"}


def _existing(repo: OpsRepository, identity: dict[str, Any], *, cutoff: int,
              deadline: int, now: int) -> ArtifactIndexEntryV2 | None:
    marker = repo.get_artifact(sha256_json(identity))
    if marker is None:
        return None
    body = marker.metadata
    if (marker.artifact_type != BINDING_VERSION or marker.content_hash != sha256_json(body)
            or set(body) != set(identity) | {"bound_ref"}
            or any(body.get(key) != value for key, value in identity.items())):
        raise ValueError("economic binding identity conflicts")
    bound = repo.get_artifact(str(body["bound_ref"]))
    receipt = repo.get_artifact(chronology_ref(str(body["bound_ref"])))
    chronology = receipt.metadata.get("chronology") if receipt is not None else None
    if (bound is None or bound.artifact_type != identity["artifact_type"]
            or bound.artifact_ref != bound.content_hash or not isinstance(chronology, Mapping)
            or chronology.get("computation_finished_ns") != bound.created_at_ns
            or marker.created_at_ns != bound.available_at_ns or marker.available_at_ns != bound.available_at_ns
            or not causal_artifact(repo, bound.artifact_ref, cutoff_ns=cutoff,
                                   consumer_at_ns=now, deadline_ns=deadline)):
        raise ValueError("economic binding publication is missing, late or noncausal")
    return bound


def _publish(repo: OpsRepository, *, identity: dict[str, Any], kind: str, wrapper: str,
             value: JointExecutionDataV2 | ScenarioSupportUnitV2, refs: tuple[str, ...],
             cutoff: int, deadline: int, started: int, finished: int) -> str:
    if value.available_at_ns > deadline:
        raise ValueError("economic binding missed the original decision deadline")
    ref = value.content_hash
    repo.register_artifact(ArtifactIndexEntryV2(ref, kind, ref, finished, value.available_at_ns,
        {wrapper: value.to_dict(), "binding_source_ref": identity["binding_source_ref"], "input_refs": refs}))
    record_computation(repo, artifact_ref=ref, information_cutoff_ns=cutoff, started_ns=started,
        finished_ns=finished, available_ns=value.available_at_ns, input_refs=refs, deadline_ns=deadline)
    body = {**identity, "bound_ref": ref}
    repo.register_artifact(ArtifactIndexEntryV2(sha256_json(identity), BINDING_VERSION,
        sha256_json(body), value.available_at_ns, value.available_at_ns, body))
    return ref


def bind_economic_templates(repository: OpsRepository, action: ActionArtifactV2, *,
        cutoff_ns: int, deadline_ns: int, execution_model_ref: str, fee_ref: str,
        joint_data_refs: Sequence[str], support_unit_refs: Sequence[str],
        clock_ns: Callable[[], int]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Bind only exact caller-scoped templates; refuse incompatible populations whole.

    Original templates and all their scientific facts must be cutoff-known.
    Only action publication identity and computation/publication clocks change.
    All writes, including restart locators and chronology, are atomic together.
    """
    timestamp(cutoff_ns, field="economic market cutoff")
    timestamp(deadline_ns, field="economic consumer deadline")
    if len(joint_data_refs) + len(support_unit_refs) > MAX_REQUESTED_REFS:
        raise ValueError("economic binding requested population exceeds 128")
    joint_refs, support_refs = tuple(joint_data_refs), tuple(support_unit_refs)
    if len(set(joint_refs)) != len(joint_refs) or len(set(support_refs)) != len(support_refs):
        raise ValueError("economic binding requested population is ambiguous")
    if not joint_refs and not support_refs:
        return (), ()
    started = sample(clock_ns, floor_ns=max(cutoff_ns, action.available_at_ns))
    if started > deadline_ns:
        raise ValueError("economic binding missed the original decision deadline")
    indexed_action = repository.get_artifact(action.content_hash)
    if (indexed_action is None or indexed_action.artifact_type != "ActionArtifactV2"
            or indexed_action.content_hash != action.content_hash
            or indexed_action.available_at_ns != action.available_at_ns
            or canonical_json(indexed_action.metadata.get("action_artifact")) != canonical_json(action.to_dict())
            or canonical_json(indexed_action.metadata.get("action_identity")) != canonical_json(action.action.to_dict())
            or not causal_artifact(repository, action.content_hash, cutoff_ns=cutoff_ns,
                                   consumer_at_ns=started, deadline_ns=deadline_ns)):
        raise ValueError("economic binding exact action unavailable or noncausal")
    _fact(repository, execution_model_ref, cutoff_ns)
    _fact(repository, fee_ref, cutoff_ns)
    originals: dict[str, JointExecutionDataV2] = {}
    for ref in joint_refs:
        entry = _raw(repository, ref, cutoff_ns, "JointExecutionDataV2")
        raw_body = entry.metadata.get("joint_execution_data")
        if (not isinstance(raw_body, Mapping) or not isinstance(raw_body.get("points"), (tuple, list))
                or not 1 <= len(raw_body["points"]) <= MAX_PATH_POINTS):
            raise ValueError("economic template path population exceeds its explicit bound")
        data = JointExecutionDataV2.from_dict(json_value(raw_body))
        if (data.content_hash != ref or data.available_at_ns != entry.available_at_ns
                or data.computed_at_ns != entry.created_at_ns or len(data.points) > MAX_PATH_POINTS
                or data.action_hash != action.action.action_hash or data.requested_quantity != action.action.quantity
                or data.information_cutoff_ns != cutoff_ns or data.execution_model_ref != execution_model_ref
                or data.fee_ref != fee_ref or "binding_source_ref" in entry.metadata):
            raise ValueError("economic template differs from exact action/cutoff/model/cost or original source")
        _fact(repository, data.source_ref, cutoff_ns)
        if data.s2_management_ref is not None:
            _fact(repository, data.s2_management_ref, cutoff_ns)
        originals[ref] = data
    policy_class, action_class = scenario_support_compatibility_classes(action)
    units: dict[str, ScenarioSupportUnitV2] = {}
    for ref in support_refs:
        entry = _raw(repository, ref, cutoff_ns, "ScenarioSupportUnitV2")
        raw_unit = entry.metadata.get("evidence")
        if (not isinstance(raw_unit, Mapping) or not isinstance(raw_unit.get("source_bundle_refs"), (tuple, list))
                or not 1 <= len(raw_unit["source_bundle_refs"]) <= MAX_PROVENANCE_REFS):
            raise ValueError("economic support provenance population exceeds its explicit bound")
        unit = ScenarioSupportUnitV2.from_dict(json_value(raw_unit))
        template = originals.get(unit.template_ref)
        if (unit.content_hash != ref or unit.available_at_ns != entry.available_at_ns
                or entry.created_at_ns != unit.available_at_ns or "binding_source_ref" in entry.metadata
                or template is None or unit.source_episode_ref != template.source_ref
                or template.source_ref not in unit.source_bundle_refs
                or unit.execution_model_ref != execution_model_ref or unit.product_ref != action.action.product_ref
                or unit.venue != action.action.key.venue or unit.policy_compatibility_class != policy_class
                or unit.action_compatibility_class != action_class or unit.synthetic_fixture != template.synthetic_fixture
                or unit.source_window_end_ns > cutoff_ns or len(unit.source_bundle_refs) > MAX_PROVENANCE_REFS):
            raise ValueError("economic support provenance is foreign, future or incompatible")
        for source_ref in (*unit.source_bundle_refs, unit.source_episode_ref, unit.calibration_ref, unit.product_ref):
            _raw(repository, source_ref, cutoff_ns)
        units[ref] = unit
    bound_joint: dict[str, str] = {}
    bound_units: list[str] = []
    with repository.atomic_composition():
        for ref, template in originals.items():
            identity = _identity("JointExecutionDataV2", action.content_hash, ref, cutoff_ns)
            existing = _existing(repository, identity, cutoff=cutoff_ns, deadline=deadline_ns, now=started)
            if existing is not None:
                value = JointExecutionDataV2.from_dict(json_value(existing.metadata["joint_execution_data"]))
                expected = replace(template, action_artifact_ref=action.content_hash,
                                   computed_at_ns=value.computed_at_ns, available_at_ns=value.available_at_ns)
                if (value != expected or value.content_hash != existing.artifact_ref
                        or value.computed_at_ns != existing.created_at_ns
                        or value.available_at_ns != existing.available_at_ns
                        or existing.metadata.get("binding_source_ref") != ref):
                    raise ValueError("economic binding changed execution template facts")
                bound_joint[ref] = existing.artifact_ref
                continue
            finished = sample(clock_ns, floor_ns=started)
            published = sample(clock_ns, floor_ns=finished)
            value = replace(template, action_artifact_ref=action.content_hash,
                            computed_at_ns=finished, available_at_ns=published)
            inputs = tuple(sorted({ref, action.content_hash, template.source_ref, execution_model_ref, fee_ref,
                                   *((template.s2_management_ref,) if template.s2_management_ref else ())}))
            bound_joint[ref] = _publish(repository, identity=identity, kind="JointExecutionDataV2",
                wrapper="joint_execution_data", value=value, refs=inputs, cutoff=cutoff_ns,
                deadline=deadline_ns, started=started, finished=finished)
        for ref, unit in units.items():
            identity = _identity("ScenarioSupportUnitV2", action.content_hash, ref, cutoff_ns)
            now = sample(clock_ns, floor_ns=started)
            existing = _existing(repository, identity, cutoff=cutoff_ns, deadline=deadline_ns, now=now)
            if existing is not None:
                value_unit = ScenarioSupportUnitV2.from_dict(json_value(existing.metadata["evidence"]))
                if (value_unit != replace(unit, template_ref=bound_joint[unit.template_ref],
                                          available_at_ns=value_unit.available_at_ns)
                        or value_unit.content_hash != existing.artifact_ref
                        or value_unit.available_at_ns != existing.created_at_ns
                        or value_unit.available_at_ns != existing.available_at_ns
                        or existing.metadata.get("binding_source_ref") != ref):
                    raise ValueError("economic binding changed historical support facts")
                bound_units.append(existing.artifact_ref)
                continue
            published = sample(clock_ns, floor_ns=now)
            value_unit = replace(unit, template_ref=bound_joint[unit.template_ref], available_at_ns=published)
            inputs = tuple(sorted({ref, action.content_hash, value_unit.template_ref, unit.source_episode_ref,
                                   *unit.source_bundle_refs, unit.execution_model_ref, unit.calibration_ref, unit.product_ref}))
            bound_units.append(_publish(repository, identity=identity, kind="ScenarioSupportUnitV2",
                wrapper="evidence", value=value_unit, refs=inputs, cutoff=cutoff_ns,
                deadline=deadline_ns, started=now, finished=published))
    return tuple(sorted(bound_joint.values())), tuple(sorted(bound_units))
