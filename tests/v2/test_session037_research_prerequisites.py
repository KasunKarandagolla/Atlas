"""Fresh public research preserves unsupported facts and real publication time."""

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.news.events import (
    AbnormalityEvidenceV2,
    AbnormalityStateV2,
    CalendarCoverageV2,
    IncidentStateV2,
    OperationalIncidentV2,
    ScheduledEventV2,
)
from atlas.v2.runtime.ops_supervisor import OpsDecisionEventV1
from atlas.v2.runtime.research_prerequisites import MAX_EVENT_INPUTS, publish_research_prerequisites
from atlas.v2.science.admission import VenueCapabilitySnapshotV2, index_venue_capability_snapshot
from atlas.v2.science.costs import FeeScheduleV2, index_cost_evidence
from atlas.v2.science.pretrade import CausalInputV2

from .test_session017_risk import risk_case, source


def _event(cutoff):
    ref = sha256_json({"event": "public-prerequisite-test"})
    return OpsDecisionEventV1(ref, "CONFIRMED_15M_CLOSE", "PUBLIC", ref,
        cutoff, None, cutoff, cutoff, cutoff, cutoff + 5_000_000_000, (ref,))


def _clock(cutoff, *, late=False):
    offset = 6_000_000_000 if late else 10
    values = iter((cutoff + offset, cutoff + offset + 10, cutoff + offset + 20))
    return lambda: next(values)


def _event_facts(repo, cutoff):
    proof = source(repo, "verified-calendar", cutoff)
    coverage = CalendarCoverageV2("OFFICIAL_TEST_FIXTURE", cutoff - 3_600_000_000_000,
        cutoff + 3_600_000_000_000, cutoff, cutoff, cutoff, True, "r1", proof, "VERIFIED")
    abnormality = AbnormalityEvidenceV2(AbnormalityStateV2.NORMAL, cutoff, cutoff,
        source(repo, "normal-abnormality", cutoff))
    return coverage, abnormality


def test_fresh_public_missing_prerequisites_have_no_fabricated_values(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        event = _event(case.candidate.decision_at_ns)
        publication = publish_research_prerequisites(repo, event=event, product=case.product,
            clock_ns=_clock(event.information_cutoff_ns))
        assert publication.status == "NOT_ESTIMABLE"
        assert publication.consumer_eligible
        assert publication.available_at_ns > event.information_cutoff_ns
        assert publication.prerequisite_statuses["PRODUCT"]["status"] == "AVAILABLE"
        for role in ("ACCOUNT", "FEE", "STRESS", "VENUE_SIZING", "VENUE_CAPABILITY", "EXECUTION_MODEL"):
            assert publication.prerequisite_statuses[role]["status"] == "NOT_ESTIMABLE"
            assert publication.prerequisite_statuses[role]["evidence_ref"] is None
        gate = repo.get_artifact(publication.event_gate_ref)
        assert gate.metadata["gate"]["state"] == "UNKNOWN"
        assert gate.available_at_ns == publication.available_at_ns
        assert gate.created_at_ns > event.information_cutoff_ns
        assert gate.metadata["gate"]["cutoff_ns"] == event.information_cutoff_ns
        body = repo.get_artifact(publication.inventory_ref).metadata["prerequisites"]
        assert body["authority"] == "ZERO"
        assert body["execution_evidence"] == "NOT_ESTIMABLE_PUBLIC_SHADOW"
        assert body["capital_enabled"] is body["assisted_execution_enabled"] is False


def test_supplied_supported_facts_publish_exact_refs_and_causal_clear_gate(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        cutoff = case.candidate.decision_at_ns
        coverage, abnormality = _event_facts(repo, cutoff)
        supplied = {"POLICY_V1": case.v1, "POLICY_V2": case.v2, "ACCOUNT": case.account,
                    "FEE": case.fee, "STRESS": case.stress, "VENUE_SIZING": case.venue}
        publication = publish_research_prerequisites(repo, event=_event(cutoff), product=case.product,
            clock_ns=_clock(cutoff), evidence=supplied, coverage=coverage, abnormality=abnormality)
        for role in supplied:
            assert publication.prerequisite_statuses[role]["status"] == "AVAILABLE"
        assert publication.prerequisite_statuses["ACCOUNT"]["evidence_ref"] == case.account.content_hash
        assert publication.prerequisite_statuses["EVENT"]["state"] == "CLEAR"
        assert publication.status == "NOT_ESTIMABLE"  # No execution/venue qualification inferred.
        gate = repo.get_artifact(publication.event_gate_ref)
        assert coverage.content_hash in gate.metadata["gate"]["envelope"]["input_refs"]
        assert abnormality.content_hash in gate.metadata["gate"]["envelope"]["input_refs"]
        assert all(repo.get_artifact(ref).available_at_ns <= cutoff
                   for ref in gate.metadata["gate"]["envelope"]["input_refs"])


def test_restart_reuses_exact_publication_and_rejects_changed_evidence(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        case = risk_case(repo)
        event = _event(case.candidate.decision_at_ns)
        first = publish_research_prerequisites(repo, event=event, product=case.product,
            clock_ns=_clock(event.information_cutoff_ns), evidence={"FEE": case.fee})
    with OpsRepository(path) as repo:
        def forbidden_clock():
            raise AssertionError("restart must not recompute")
        second = publish_research_prerequisites(repo, event=event, product=case.product,
            clock_ns=forbidden_clock, evidence={"FEE": case.fee})
        assert second == first
        with pytest.raises(ValueError, match="cannot revise"):
            publish_research_prerequisites(repo, event=event, product=case.product,
                clock_ns=forbidden_clock)


def test_late_prerequisite_publication_is_retained_but_not_consumer_eligible(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        event = _event(case.candidate.decision_at_ns)
        publication = publish_research_prerequisites(repo, event=event, product=case.product,
            clock_ns=_clock(event.information_cutoff_ns, late=True))
        assert not publication.consumer_eligible
        assert "PREREQUISITE_PUBLICATION_AFTER_CONSUMER_DEADLINE" in publication.missing_reasons
        assert repo.get_artifact(publication.inventory_ref).available_at_ns > event.deadline_ns


@pytest.mark.parametrize("mutation", ["type", "body", "source", "key"])
def test_invalid_supplied_evidence_is_not_promoted(tmp_path, mutation):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        cutoff = case.candidate.decision_at_ns
        fee = replace(case.fee, entry_taker_rate=Decimal("0.002"))
        if mutation == "key":
            fee = replace(fee, key=replace(fee.key, native_symbol="ETHUSDT"))
            index_cost_evidence(repo, fee)
        elif mutation == "source":
            fee = replace(fee, source_ref="f" * 64)
            repo.register_artifact(ArtifactIndexEntryV2(fee.content_hash, "FeeScheduleV2", fee.content_hash,
                fee.available_at_ns, fee.available_at_ns, fee.to_dict()))
        else:
            repo.register_artifact(ArtifactIndexEntryV2(fee.content_hash,
                "NotFeeSchedule" if mutation == "type" else "FeeScheduleV2", fee.content_hash,
                fee.available_at_ns, fee.available_at_ns,
                {**fee.to_dict(), "entry_taker_rate": "0.9"} if mutation == "body" else fee.to_dict()))
        with pytest.raises(ValueError, match="mismatch|source"):
            publish_research_prerequisites(repo, event=_event(cutoff), product=case.product,
                clock_ns=_clock(cutoff), evidence={"FEE": fee})
        assert repo.artifact_entries("ResearchPrerequisiteInventoryV1") == ()


def test_future_fee_and_future_event_facts_rejected(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        cutoff = case.candidate.decision_at_ns
        fee = FeeScheduleV2(case.product.key, cutoff + 100, Decimal("0.1"), Decimal("0.1"),
            source(repo, "future-fee", cutoff + 100))
        index_cost_evidence(repo, fee)
        with pytest.raises(ValueError, match="information cutoff"):
            publish_research_prerequisites(repo, event=_event(cutoff), product=case.product,
                clock_ns=_clock(cutoff), evidence={"FEE": fee})
        coverage, abnormality = _event_facts(repo, cutoff)
        with pytest.raises(ValueError, match="information cutoff"):
            publish_research_prerequisites(repo, event=_event(cutoff), product=case.product,
                clock_ns=_clock(cutoff), coverage=replace(coverage, available_at_ns=cutoff + 1),
                abnormality=abnormality)


def test_source_backed_unresolved_incident_remains_blocked(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        cutoff = case.candidate.decision_at_ns
        coverage, abnormality = _event_facts(repo, cutoff)
        incident = OperationalIncidentV2("fixture-incident", case.product.key.venue.value, None,
            IncidentStateV2.OPEN, 1, cutoff, cutoff, source(repo, "incident", cutoff))
        publication = publish_research_prerequisites(repo, event=_event(cutoff), product=case.product,
            clock_ns=_clock(cutoff), coverage=coverage, abnormality=abnormality, incidents=(incident,))
        assert publication.prerequisite_statuses["EVENT"]["state"] == "BLOCKED"
        assert "UNRESOLVED_VENUE_OR_ASSET_INCIDENT" in publication.missing_reasons


def test_regressing_processing_clock_cannot_publish_inventory(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        cutoff = case.candidate.decision_at_ns
        observations = iter((cutoff + 10, cutoff + 9, cutoff + 20))
        with pytest.raises(ValueError, match="regressed"):
            publish_research_prerequisites(repo, event=_event(cutoff), product=case.product,
                clock_ns=lambda: next(observations))
        assert repo.artifact_entries("ResearchPrerequisiteInventoryV1") == ()
        assert repo.artifact_entries("EventSafetyGateV2") == ()


def test_restart_with_event_facts_reuses_exact_gate_and_cannot_change_origin(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        case = risk_case(repo)
        cutoff = case.candidate.decision_at_ns
        event = _event(cutoff)
        coverage, abnormality = _event_facts(repo, cutoff)
        first = publish_research_prerequisites(repo, event=event, product=case.product,
            clock_ns=_clock(cutoff), coverage=coverage, abnormality=abnormality)
        gate = repo.get_artifact(first.event_gate_ref)
        inventory = repo.get_artifact(first.inventory_ref)
    with OpsRepository(path) as repo:
        def forbidden_clock():
            raise AssertionError("sealed event facts must not be recomputed")
        assert publish_research_prerequisites(repo, event=event, product=case.product,
            clock_ns=forbidden_clock, coverage=coverage, abnormality=abnormality) == first
        assert repo.get_artifact(first.event_gate_ref) == gate
        assert repo.get_artifact(first.inventory_ref) == inventory
        with pytest.raises(ValueError, match="cannot revise"):
            publish_research_prerequisites(repo, event=replace(event, source_id="CHANGED_SOURCE"),
                product=case.product, clock_ns=forbidden_clock,
                coverage=coverage, abnormality=abnormality)


def test_conflicting_event_input_rolls_back_gate_inventory_and_other_inputs(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        cutoff = case.candidate.decision_at_ns
        coverage, abnormality = _event_facts(repo, cutoff)
        repo.register_artifact(ArtifactIndexEntryV2(coverage.content_hash, "CalendarCoverageV2",
            coverage.content_hash, cutoff, cutoff, {"evidence": {"conflicting": True}}))
        with pytest.raises(ValueError, match="different immutable content"):
            publish_research_prerequisites(repo, event=_event(cutoff), product=case.product,
                clock_ns=_clock(cutoff), coverage=coverage, abnormality=abnormality)
        assert repo.artifact_entries("ResearchPrerequisiteInventoryV1") == ()
        assert repo.artifact_entries("EventSafetyGateV2") == ()
        assert repo.get_artifact(abnormality.content_hash) is None


@pytest.mark.parametrize("count", [MAX_EVENT_INPUTS, MAX_EVENT_INPUTS + 1])
def test_event_fact_and_reference_bounds_do_not_leave_partial_publications(tmp_path, count):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        cutoff = case.candidate.decision_at_ns
        coverage, abnormality = _event_facts(repo, cutoff)
        scheduled = tuple(ScheduledEventV2(str(index), "US_CPI", cutoff + 3_600_000_000_000,
            "r1", "OFFICIAL_TEST_FIXTURE", cutoff, cutoff, cutoff,
            source(repo, f"calendar-event-{index}", cutoff)) for index in range(count))
        with pytest.raises(ValueError, match="bounded publication budget"):
            publish_research_prerequisites(repo, event=_event(cutoff), product=case.product,
                clock_ns=_clock(cutoff), coverage=coverage, abnormality=abnormality,
                scheduled_events=scheduled)
        assert repo.artifact_entries("ResearchPrerequisiteInventoryV1") == ()
        assert repo.artifact_entries("EventSafetyGateV2") == ()
        assert repo.get_artifact(coverage.content_hash) is None
        assert repo.get_artifact(abnormality.content_hash) is None


def test_unqualified_venue_and_unknown_account_remain_unestimable(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo, account_overrides={"operational_status": "UNKNOWN"})
        cutoff = case.candidate.decision_at_ns
        profiles = tuple(sha256_json({"profile": name}) for name in ("runtime", "execution", "protection"))
        capability = VenueCapabilitySnapshotV2(case.product.key.venue, case.product.key.environment,
            case.account.account_scope, case.product.content_hash, case.product.key.content_hash,
            "ISOLATED", "ONE_WAY", "nautilus_trader", "2.0.0rc5", "fixture-commit", *profiles,
            "FIXTURE_QUALIFICATION_V1", "UNVERIFIED", (), cutoff)
        index_venue_capability_snapshot(repo, capability)
        publication = publish_research_prerequisites(repo, event=_event(cutoff), product=case.product,
            clock_ns=_clock(cutoff), evidence={"ACCOUNT": case.account, "VENUE_CAPABILITY": capability})
        assert publication.prerequisite_statuses["ACCOUNT"]["status"] == "NOT_ESTIMABLE"
        assert publication.prerequisite_statuses["VENUE_CAPABILITY"]["status"] == "NOT_ESTIMABLE"
        # A forged SUPPORTED label with a generic source cannot qualify the venue.
        claimed = replace(capability, observed_status="SUPPORTED",
            evidence_refs=(source(repo, "public-socket-health", cutoff),))
        repo.register_artifact(ArtifactIndexEntryV2(claimed.content_hash, "VenueCapabilitySnapshotV2",
            claimed.content_hash, cutoff, cutoff, {"capability": claimed.to_dict()}))
        with pytest.raises(ValueError, match="CapabilityContractV1"):
            publish_research_prerequisites(repo, event=replace(_event(cutoff), event_id=sha256_json("new")),
                product=case.product, clock_ns=_clock(cutoff), evidence={"VENUE_CAPABILITY": claimed})


def test_declared_execution_model_requires_exact_indexed_content_and_availability(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        cutoff = case.candidate.decision_at_ns
        body = {"execution_model": "EXPLICIT_RESEARCH_FIXTURE", "available_at_ns": cutoff}
        ref = sha256_json(body)
        repo.register_artifact(ArtifactIndexEntryV2(ref, "ExecutionModelV1", ref, cutoff, cutoff, body))
        model = CausalInputV2(ref, "ExecutionModelV1", cutoff, cutoff)
        publication = publish_research_prerequisites(repo, event=_event(cutoff), product=case.product,
            clock_ns=_clock(cutoff), evidence={"EXECUTION_MODEL": model})
        assert publication.prerequisite_statuses["EXECUTION_MODEL"]["evidence_ref"] == ref
        assert publication.status == "NOT_ESTIMABLE"
        with pytest.raises(ValueError, match="availability mismatch"):
            publish_research_prerequisites(repo, event=_event(cutoff), product=case.product,
                clock_ns=_clock(cutoff), evidence={"EXECUTION_MODEL": replace(model, available_at_ns=cutoff + 1)})
