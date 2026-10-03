"""Production publishes a current product-specific gate from exact source facts."""

from dataclasses import replace

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.news.events import (
    EventSafetyGateBuilderV2,
    IncidentStateV2,
    OperationalIncidentV2,
)
from atlas.v2.runtime.production import _indexed_event_prerequisites, _publish_public_prerequisites

from .test_session017_risk import risk_case, source
from .test_session037_research_prerequisites import _event, _event_facts


def test_other_product_clear_gate_cannot_hide_current_product_incident(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        cutoff = case.candidate.decision_at_ns
        coverage, abnormality = _event_facts(repo, cutoff)
        incident = OperationalIncidentV2("incident", case.product.key.venue.value,
            case.product.key.base_asset_id, IncidentStateV2.OPEN, 1, cutoff, cutoff,
            source(repo, "incident-proof", cutoff))
        repo.register_artifact(ArtifactIndexEntryV2(incident.content_hash, "OperationalIncidentV2",
            incident.content_hash, cutoff, cutoff, {"evidence": incident.to_dict()}))
        other_key = replace(case.product.key, base_asset_id="OTHER")
        other = EventSafetyGateBuilderV2(repo).evaluate(key=other_key, cutoff_ns=cutoff,
            coverage=coverage, abnormality=abnormality, scheduled_events=(), incidents=(incident,))
        assert other.state.value == "CLEAR"
        publication = _publish_public_prerequisites(repo, _event(cutoff), case.product,
            clock_ns=lambda: cutoff + 100)
        assert publication.prerequisite_statuses["EVENT"]["state"] == "BLOCKED"
        assert publication.event_gate_ref != other.content_hash
        gate = repo.get_artifact(publication.event_gate_ref)
        assert incident.evidence_ref in gate.metadata["gate"]["incident_refs"]
        assert gate.available_at_ns == cutoff + 100
        assert gate.metadata["gate"]["cutoff_ns"] == cutoff
        repeated = _publish_public_prerequisites(repo, _event(cutoff), case.product,
            clock_ns=lambda: cutoff + 200)
        assert repeated == publication


def test_unbound_clear_gate_cannot_replace_missing_event_sources(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        cutoff = case.candidate.decision_at_ns
        ref = sha256_json({"unbound": "CLEAR"})
        repo.register_artifact(ArtifactIndexEntryV2(ref, "EventSafetyGateV2", ref, cutoff, cutoff,
            {"gate": {"schema_version": 2, "cutoff_ns": cutoff, "state": "CLEAR",
                "availability_view": "ACTUAL_SYSTEM", "gate_version": "UNBOUND"}}))
        publication = _publish_public_prerequisites(repo, _event(cutoff), case.product,
            clock_ns=lambda: cutoff + 100)
        assert publication.prerequisite_statuses["EVENT"]["state"] == "UNKNOWN"
        assert publication.event_gate_ref != ref


def test_event_input_conflict_abstains_and_tampered_wire_is_rejected(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        cutoff = case.candidate.decision_at_ns
        coverage, abnormality = _event_facts(repo, cutoff)
        EventSafetyGateBuilderV2(repo).evaluate(key=case.product.key, cutoff_ns=cutoff,
            coverage=coverage, abnormality=abnormality, scheduled_events=(), incidents=())
        conflict = replace(coverage, complete=False)
        repo.register_artifact(ArtifactIndexEntryV2(conflict.content_hash, "CalendarCoverageV2",
            conflict.content_hash, cutoff, cutoff, {"evidence": conflict.to_dict()}))
        assert _indexed_event_prerequisites(repo, cutoff_ns=cutoff)["coverage"] is None
        bad = replace(abnormality, observed_at_ns=cutoff + 1, available_at_ns=cutoff + 1)
        repo.register_artifact(ArtifactIndexEntryV2(bad.content_hash, "AbnormalityEvidenceV2",
            bad.content_hash, cutoff + 1, cutoff + 1, {"evidence": abnormality.to_dict()}))
        with pytest.raises(ValueError, match="identity or chronology"):
            _indexed_event_prerequisites(repo, cutoff_ns=cutoff + 1)
