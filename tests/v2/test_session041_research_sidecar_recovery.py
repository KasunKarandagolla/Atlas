"""Installed broad sidecar crash recovery and atomic research publication."""

from __future__ import annotations

import pytest

from atlas.v2._serialization import json_value
from atlas.v2.instruments import UniverseContractV2
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.runtime.broad_research_queue import LANE_BROAD_RESEARCH_V1
from atlas.v2.runtime.production import (
    BroadProductionOpsCyclePortV2,
    ProductionEventInputsV1,
    _seal_public_composition,
)

from .test_session041_broad_research_queue import CUTOFF, _inputs, _universe
from .test_session041_production_breadth import FakeBroadRuntime, InventorySource


class Runtime(FakeBroadRuntime):
    sequence_books = {}

    def service(self, repo, **kwargs):
        assert not repo._connection.in_transaction


def _port(now):
    return BroadProductionOpsCyclePortV2(public_source=InventorySource(()),
        broad_runtime=Runtime(), clock_ns=lambda: now)


def _seal_slot(repo, port):
    event, _universe_ref, _ = _inputs(repo)
    universe_ref = _universe(repo, available=CUTOFF - 10,
        decision_slot=event.information_cutoff_ns)
    universe = UniverseContractV2.from_dict(json_value(repo.get_artifact(universe_ref).metadata["universe"]))
    with repo.atomic_composition():
        _seal_public_composition(repo, event, ProductionEventInputsV1(universe, (), {}, {}, {}),
            clock_ns=port.clock_ns)
        port._observe_prepared_research(event, universe, {}, repo)
    return event, universe


def test_crash_after_composition_seal_recovers_research_once_from_durable_slot(tmp_path):
    path = tmp_path / "ops.sqlite"
    port = _port(CUTOFF + 10)
    try:
        with OpsRepository(path) as repo:
            event, universe = _seal_slot(repo, port)
            assert len(repo.due_work_items(LANE_BROAD_RESEARCH_V1, as_of_ns=CUTOFF + 20)) == 1
    finally:
        port.close()
    restarted = _port(CUTOFF + 20)
    try:
        with OpsRepository(path) as repo:
            result_ref = restarted.run_research_maintenance(repo, cutoff_ns=CUTOFF + 20)
            surface = repo.get_artifact(result_ref).metadata["surface"]
            assert surface["cutoff_ns"] == CUTOFF
            assert surface["authority"] == "ZERO" and surface["trade_plan_allowed"] is False
            assert surface["role_status"]["S4"] == "NOT_ESTIMABLE"
            assert repo.due_work_items(LANE_BROAD_RESEARCH_V1, as_of_ns=CUTOFF + 30) == ()
            assert restarted.run_research_maintenance(repo, cutoff_ns=CUTOFF + 20) is None
            restarted._observe_prepared_research(event, universe, {}, repo)
            assert len(repo.artifact_entries("FULL_STRATEGY_RESEARCH_SURFACE_V1")) == 1
            assert len(repo.artifact_entries("BroadResearchCompletionV1")) == 1
    finally:
        restarted.close()


def test_crash_during_surface_publication_rolls_back_outputs_and_keeps_original_job(tmp_path, monkeypatch):
    from atlas.v2.runtime import full_strategy_surface

    port = _port(CUTOFF + 10)
    try:
        with OpsRepository(tmp_path / "ops.sqlite") as repo:
            _seal_slot(repo, port)
            original = full_strategy_surface.compose_full_strategy_surface

            def interrupted(*args, **kwargs):
                original(*args, **kwargs)
                raise RuntimeError("INJECTED_PROCESS_LOSS_BEFORE_COMPLETION")

            monkeypatch.setattr(full_strategy_surface, "compose_full_strategy_surface", interrupted)
            with pytest.raises(RuntimeError, match="INJECTED_PROCESS_LOSS"):
                port.run_research_maintenance(repo, cutoff_ns=CUTOFF + 10)
            assert repo.artifact_entries("FULL_STRATEGY_RESEARCH_SURFACE_V1") == ()
            assert repo.artifact_entries("BroadResearchCompletionV1") == ()
            assert len(repo.due_work_items(LANE_BROAD_RESEARCH_V1, as_of_ns=CUTOFF + 20)) == 1
            monkeypatch.setattr(full_strategy_surface, "compose_full_strategy_surface", original)
            assert port.run_research_maintenance(repo, cutoff_ns=CUTOFF + 10) is not None
            assert len(repo.artifact_entries("FULL_STRATEGY_RESEARCH_SURFACE_V1")) == 1
            assert len(repo.artifact_entries("BroadResearchCompletionV1")) == 1
    finally:
        port.close()


def test_corrupt_queued_snapshot_is_durably_quarantined_without_hot_retries(tmp_path):
    port = _port(CUTOFF + 10)
    try:
        with OpsRepository(tmp_path / "ops.sqlite") as repo:
            _seal_slot(repo, port)
            item, = repo.due_work_items(LANE_BROAD_RESEARCH_V1, as_of_ns=CUTOFF + 20)
            repo._connection.execute("UPDATE artifact_index SET metadata_json='{' WHERE artifact_ref=?",
                (item.source_ref,))
            failure_ref = port.run_research_maintenance(repo, cutoff_ns=CUTOFF + 10)
            failure = repo.get_artifact(failure_ref).metadata["failure"]
            assert failure["snapshot_ref"] == item.source_ref
            assert failure["status"] == "NOT_ESTIMABLE" and failure["authority"] == "ZERO"
            assert repo.due_work_pressure(LANE_BROAD_RESEARCH_V1, as_of_ns=CUTOFF + 20)["quarantined_count"] == 1
            assert port.run_research_maintenance(repo, cutoff_ns=CUTOFF + 10) is None
    finally:
        port.close()
