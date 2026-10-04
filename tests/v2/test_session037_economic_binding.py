"""Exact, causal economic template binding without changing economic facts."""
from __future__ import annotations

import itertools
from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import json_value, sha256_json
from atlas.v2.chronology import causal_artifact, record_computation
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.economic_binding import bind_economic_templates
from atlas.v2.science.admission import (
    ScenarioSupportUnitV2,
    index_scenario_support_unit,
    scenario_support_compatibility_classes,
)
from atlas.v2.science.scenario_engine import JointExecutionDataV2

from .test_session016_candidate_selection import CUTOFF
from .test_session019_scenarios import _fixture


def _case(repo):
    original_action, template, scenario, _ = _fixture(repo)
    policy_class, action_class = scenario_support_compatibility_classes(original_action)
    unit = ScenarioSupportUnitV2(template.source_ref, CUTOFF - 200, CUTOFF - 100,
        (template.source_ref,), original_action.action.key.venue, original_action.action.product_ref,
        policy_class, action_class, template.execution_model_ref, scenario.calibration_input.ref,
        template.content_hash, CUTOFF, template.synthetic_fixture)
    index_scenario_support_unit(repo, unit)
    action = replace(original_action, available_at_ns=CUTOFF + 13)
    repo.register_artifact(ArtifactIndexEntryV2(action.content_hash, "ActionArtifactV2", action.content_hash,
        action.available_at_ns, action.available_at_ns, {"action_artifact": action.to_dict(),
        "action_identity": action.action.to_dict(), "input_refs": [original_action.content_hash]}))
    record_computation(repo, artifact_ref=action.content_hash, information_cutoff_ns=CUTOFF,
        started_ns=CUTOFF + 11, finished_ns=CUTOFF + 12, available_ns=action.available_at_ns,
        input_refs=(original_action.content_hash,), deadline_ns=CUTOFF + 1000)
    return action, template, unit


def _bind(repo, action, template, unit, *, clock=None, **changes):
    args = {"cutoff_ns": CUTOFF, "deadline_ns": CUTOFF + 1000,
        "execution_model_ref": template.execution_model_ref, "fee_ref": template.fee_ref,
        "joint_data_refs": (template.content_hash,), "support_unit_refs": (unit.content_hash,),
        "clock_ns": clock or itertools.count(CUTOFF + 20).__next__}
    args.update(changes)
    return bind_economic_templates(repo, action, **args)


def test_binding_preserves_every_economic_and_independence_fact_and_exact_restart(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        action, template, unit = _case(repo)
        joint_refs, unit_refs = _bind(repo, action, template, unit)
        bound_entry = repo.get_artifact(joint_refs[0])
        bound_unit_entry = repo.get_artifact(unit_refs[0])
        assert bound_entry is not None and bound_unit_entry is not None
        bound = JointExecutionDataV2.from_dict(json_value(bound_entry.metadata["joint_execution_data"]))
        bound_unit = ScenarioSupportUnitV2.from_dict(json_value(bound_unit_entry.metadata["evidence"]))
        assert bound == replace(template, action_artifact_ref=action.content_hash,
                               computed_at_ns=bound.computed_at_ns, available_at_ns=bound.available_at_ns)
        assert bound_unit == replace(unit, template_ref=bound.content_hash, available_at_ns=bound_unit.available_at_ns)
        assert bound.available_at_ns > action.available_at_ns > CUTOFF
        assert bound_unit.source_episode_ref == unit.source_episode_ref
        assert causal_artifact(repo, bound.content_hash, cutoff_ns=CUTOFF,
            consumer_at_ns=CUTOFF + 100, deadline_ns=CUTOFF + 1000)
        assert causal_artifact(repo, bound_unit.content_hash, cutoff_ns=CUTOFF,
            consumer_at_ns=CUTOFF + 100, deadline_ns=CUTOFF + 1000)
        assert _bind(repo, action, template, unit, clock=itertools.count(CUTOFF + 100).__next__) == (joint_refs, unit_refs)
        assert repo.get_artifact(bound.content_hash) == bound_entry
    with OpsRepository(tmp_path / "ops.sqlite") as restarted:
        assert _bind(restarted, action, template, unit, clock=itertools.count(CUTOFF + 200).__next__) == (
            joint_refs, unit_refs)


@pytest.mark.parametrize("change", ["quantity", "action", "cutoff", "model", "fee", "future"])
def test_binding_refuses_foreign_or_future_template_without_any_partial_publication(tmp_path, change):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        action, template, unit = _case(repo)
        if change == "quantity":
            # Keep valid partial-fill fields while testing exact requested quantity.
            template = replace(template, requested_quantity=template.requested_quantity + Decimal(1),
                entry_state="PARTIAL_FILL")
        elif change == "action":
            template = replace(template, action_hash=sha256_json("foreign action"))
        elif change == "cutoff":
            template = replace(template, information_cutoff_ns=CUTOFF - 1,
                               entry_latency_ns=template.entry_latency_ns + 1)
        elif change == "model":
            template = replace(template, execution_model_ref=sha256_json("foreign model"))
        elif change == "fee":
            template = replace(template, fee_ref=sha256_json("foreign fee"))
        else:
            template = replace(template, computed_at_ns=CUTOFF + 1, available_at_ns=CUTOFF + 1)
        repo.register_artifact(ArtifactIndexEntryV2(template.content_hash, "JointExecutionDataV2",
            template.content_hash, template.computed_at_ns, template.available_at_ns,
            {"joint_execution_data": template.to_dict()}))
        refs_before = repo._connection.execute("SELECT COUNT(*) FROM artifact_index").fetchone()[0]
        with pytest.raises(ValueError):
            _bind(repo, action, template, unit, support_unit_refs=(),
                execution_model_ref=unit.execution_model_ref, fee_ref=repo.get_artifact(unit.template_ref).metadata[
                    "joint_execution_data"]["fee_ref"])
        assert repo._connection.execute("SELECT COUNT(*) FROM artifact_index").fetchone()[0] == refs_before


def test_binding_refuses_late_atomic_population_and_raw_templates(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        action, template, unit = _case(repo)
        before = repo._connection.execute("SELECT COUNT(*) FROM artifact_index").fetchone()[0]
        with pytest.raises(ValueError, match="deadline"):
            _bind(repo, action, template, unit, clock=itertools.count(CUTOFF + 998).__next__)
        assert repo._connection.execute("SELECT COUNT(*) FROM artifact_index").fetchone()[0] == before
        with pytest.raises(ValueError, match="source"):
            _bind(repo, action, template, unit, joint_data_refs=(template.source_ref,), support_unit_refs=())
        with pytest.raises(ValueError, match="128"):
            _bind(repo, action, template, unit, joint_data_refs=(template.content_hash,) * 129, support_unit_refs=())
        with pytest.raises(ValueError, match="ambiguous"):
            _bind(repo, action, template, unit, joint_data_refs=(template.content_hash,) * 2, support_unit_refs=())


def test_support_provenance_is_not_created_or_repaired_by_binding(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        action, template, unit = _case(repo)
        foreign = replace(unit, source_episode_ref=sha256_json("invented episode"))
        repo.register_artifact(ArtifactIndexEntryV2(foreign.content_hash, "ScenarioSupportUnitV2",
            foreign.content_hash, CUTOFF, CUTOFF, {"evidence": foreign.to_dict()}))
        with pytest.raises(ValueError, match="provenance"):
            _bind(repo, action, template, unit, support_unit_refs=(foreign.content_hash,))
        assert bind_economic_templates(repo, action, cutoff_ns=CUTOFF, deadline_ns=CUTOFF,
            execution_model_ref=template.execution_model_ref, fee_ref=template.fee_ref,
            joint_data_refs=(), support_unit_refs=(), clock_ns=lambda: CUTOFF) == ((), ())


def test_restart_refuses_changed_publication_chronology(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        action, template, unit = _case(repo)
        joint_refs, _ = _bind(repo, action, template, unit)
        with repo._transaction() as connection:
            connection.execute("UPDATE artifact_index SET created_at_ns=created_at_ns+1 WHERE artifact_ref=?",
                               (joint_refs[0],))
        with pytest.raises(ValueError, match="publication"):
            _bind(repo, action, template, unit, clock=itertools.count(CUTOFF + 100).__next__)


def test_derived_populations_are_sorted_independently_of_original_hash_order(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        action, template, unit = _case(repo)
        bound_at = CUTOFF + 20
        for number in range(100):
            path = sha256_json({"economic binding alternate path": number})
            second = replace(template, joint_path_id=path,
                             points=tuple(replace(point, joint_path_id=path) for point in template.points))
            originals = sorted((template, second), key=lambda value: value.content_hash)
            expected = [replace(value, action_artifact_ref=action.content_hash,
                computed_at_ns=bound_at, available_at_ns=bound_at).content_hash for value in originals]
            if expected != sorted(expected):
                break
        else:
            raise AssertionError("fixture must reproduce reversed derived identity order")
        repo.register_artifact(ArtifactIndexEntryV2(second.content_hash, "JointExecutionDataV2",
            second.content_hash, second.computed_at_ns, second.available_at_ns,
            {"joint_execution_data": second.to_dict()}))
        second_unit = replace(unit, template_ref=second.content_hash)
        index_scenario_support_unit(repo, second_unit)
        joint_refs, unit_refs = _bind(repo, action, template, unit, clock=lambda: bound_at,
            joint_data_refs=tuple(value.content_hash for value in originals),
            support_unit_refs=tuple(sorted((unit.content_hash, second_unit.content_hash))))
        assert joint_refs == tuple(sorted(expected))
        assert unit_refs == tuple(sorted(unit_refs))
        assert len(joint_refs) == len(unit_refs) == 2
        source_episodes = {ScenarioSupportUnitV2.from_dict(json_value(repo.get_artifact(ref).metadata[
            "evidence"])).source_episode_ref for ref in unit_refs}
        assert source_episodes == {unit.source_episode_ref}
