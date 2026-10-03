"""The public composition inventory reuses only exact cutoff-visible facts."""
from dataclasses import replace
from decimal import Decimal

from atlas.v2.memory.repository import OpsRepository
from atlas.v2.runtime.production import _indexed_public_prerequisite_evidence
from atlas.v2.runtime.research_prerequisites import publish_research_prerequisites
from atlas.v2.science.costs import index_cost_evidence

from .test_session017_risk import risk_case
from .test_session037_research_prerequisites import _clock, _event


def test_indexed_public_facts_are_published_without_defaults(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        cutoff = case.candidate.decision_at_ns
        evidence = _indexed_public_prerequisite_evidence(repo, case.product, cutoff_ns=cutoff)
        assert set(evidence) == {"POLICY_V1", "POLICY_V2", "ACCOUNT", "FEE", "STRESS", "VENUE_SIZING"}
        publication = publish_research_prerequisites(repo, event=_event(cutoff), product=case.product,
            evidence=evidence, clock_ns=_clock(cutoff))
        assert publication.prerequisite_statuses["FEE"]["evidence_ref"] == case.fee.content_hash
        assert publication.prerequisite_statuses["ACCOUNT"]["evidence_ref"] == case.account.content_hash
        assert publication.prerequisite_statuses["VENUE_CAPABILITY"]["status"] == "NOT_ESTIMABLE"
        assert publication.prerequisite_statuses["EXECUTION_MODEL"]["status"] == "NOT_ESTIMABLE"
        assert publication.prerequisite_statuses["EVENT"]["state"] == "UNKNOWN"


def test_future_fee_is_excluded_and_conflicting_current_fee_abstains(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        cutoff = case.candidate.decision_at_ns
        future = replace(case.fee, available_at_ns=cutoff + 1, entry_taker_rate=Decimal("0.003"))
        index_cost_evidence(repo, future)
        evidence = _indexed_public_prerequisite_evidence(repo, case.product, cutoff_ns=cutoff)
        assert evidence["FEE"].content_hash == case.fee.content_hash
        conflict = replace(case.fee, entry_taker_rate=Decimal("0.004"))
        index_cost_evidence(repo, conflict)
        assert "FEE" not in _indexed_public_prerequisite_evidence(repo, case.product, cutoff_ns=cutoff)
