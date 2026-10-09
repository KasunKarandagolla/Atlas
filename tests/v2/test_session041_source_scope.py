from dataclasses import replace

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository, SourceHealthV2
from atlas.v2.runtime.ops_supervisor import OpsSourceStateV1, OpsSupervisorV2
from atlas.v2.runtime.production import BroadProductionOpsCyclePortV2

from .test_session027_ops_supervisor import CUTOFF, DeterministicPort, make_event


def gate(repo, event):
    # The production gate needs no network, provider or mutable qualification flag.
    return BroadProductionOpsCyclePortV2.event_source_health_state(object(), repo, event, now_ns=CUTOFF)


def seed(repo, event, *, additional_source=None):
    metadata = {"source_id": additional_source} if additional_source else {}
    repo.register_artifact(ArtifactIndexEntryV2(event.trigger_ref, "Trigger", event.trigger_ref,
        CUTOFF, CUTOFF, metadata))
    repo.record_source_health(SourceHealthV2("BYBIT_PUBLIC_V2", CUTOFF, CUTOFF, "HEALTHY_CURRENT"))
    repo.record_source_health(SourceHealthV2("BINANCE_USDM_PUBLIC_V2", CUTOFF, CUTOFF, "DISCONNECTED"))


def test_other_venue_outage_does_not_block_independent_research(tmp_path):
    event = replace(make_event(), source_id="BYBIT_PUBLIC_V2")
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        seed(repo, event)
        assert gate(repo, event) == "HEALTHY_CURRENT"
        scopes = repo.artifact_entries("OpsDecisionSourceScopeV2")
        assert scopes[0].metadata["source_scope"]["required_source_ids"] == ("BYBIT_PUBLIC_V2",)
        assert scopes[0].metadata["source_scope"]["capital_enabled"] is False


def test_actual_cross_venue_input_requires_both_sources(tmp_path):
    event = replace(make_event(), source_id="BYBIT_PUBLIC_V2")
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        seed(repo, event, additional_source="BINANCE_USDM_PUBLIC_V2")
        assert gate(repo, event) == "DISCONNECTED"


def test_later_reconnection_does_not_create_historical_health(tmp_path):
    event = replace(make_event(cutoff_ns=CUTOFF - 1), source_id="BYBIT_PUBLIC_V2")
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        repo.register_artifact(ArtifactIndexEntryV2(event.trigger_ref, "Trigger", event.trigger_ref,
            CUTOFF - 1, CUTOFF - 1, {}))
        repo.record_source_health(SourceHealthV2("BYBIT_PUBLIC_V2", CUTOFF, CUTOFF, "HEALTHY_CURRENT"))
        assert gate(repo, event) == "UNKNOWN"


def test_missing_causal_dependency_fails_closed(tmp_path):
    event = replace(make_event(causal_input_refs=(sha256_json("missing"),)), source_id="BYBIT_PUBLIC_V2")
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        seed(repo, event)
        assert gate(repo, event) == "INCOMPLETE_SNAPSHOT"


class ScopedPort(DeterministicPort):
    def event_source_health_state(self, repository, event, *, now_ns):
        return "HEALTHY_CURRENT"


@pytest.mark.parametrize("scoped,processed", [(False, 0), (True, 1)])
def test_supervisor_retains_global_component_failure(scoped, processed, tmp_path):
    port_class = ScopedPort if scoped else DeterministicPort
    states = (OpsSourceStateV1("PUBLIC_MARKET", "HEALTHY_CURRENT", CUTOFF, CUTOFF),
              OpsSourceStateV1("OTHER_VENUE", "DISCONNECTED", CUTOFF, CUTOFF))
    port = port_class(event=make_event(), required_sources=("PUBLIC_MARKET", "OTHER_VENUE"),
        source_states=states, reconciled=False)
    with OpsSupervisorV2(tmp_path / "ops.sqlite", port=port, clock_ns=lambda: CUTOFF) as supervisor:
        result = supervisor.run_once()
        assert port.process_calls == processed
        assert result.cycle.source_health_state != "HEALTHY_CURRENT"
