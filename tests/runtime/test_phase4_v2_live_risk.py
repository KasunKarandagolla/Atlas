from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest
from v2.test_session014_core import KEY
from v2.test_session017_risk import actual_outcome

from atlas.domain.risk import engineering_default_policy
from atlas.runtime.phase4_v2 import V2LiveRiskEvidence, revalidate_v2_rolling_risk
from atlas.v2._serialization import canonical_decimal_str, sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.risk import (
    DAY_NS,
    AccountRiskSnapshotV2,
    ClosedV2Outcome,
    ExposureKind,
    OutcomeClass,
    PossibleRiskV2,
    RiskPolicyV2,
    index_research_evidence,
    index_risk_evidence,
)

_CUTOFF = 1_800_000_000_000_000_000
_ACCOUNT = sha256_json("phase4-redacted-account")


def _indexed_source(repo: OpsRepository, name: str) -> str:
    body = {"phase4_risk_fixture_source": name}
    ref = sha256_json(body)
    index_research_evidence(repo, "Phase4RiskSourceFixture", ref, _CUTOFF, body)
    return ref


def _possible(
    repo: OpsRepository,
    kind: ExposureKind,
    loss: str,
    name: str,
    *,
    available_at_ns: int = _CUTOFF,
    source_available_at_ns: int | None = None,
) -> PossibleRiskV2:
    source_metadata = {
        "account_scope": _ACCOUNT,
        "source_system": "VENUE_RECONCILED_POSITION" if kind == ExposureKind.OPEN else "ATLAS_DURABLE_RESERVATION",
        "kind": kind.value,
        "key": KEY.to_dict(),
        "possible_normal_loss": canonical_decimal_str(Decimal(loss)),
        "fixture_source_label": name,
    }
    source_ref = sha256_json(source_metadata)
    source_available_at_ns = available_at_ns if source_available_at_ns is None else source_available_at_ns
    repo.register_artifact(
        ArtifactIndexEntryV2(
            source_ref,
            "V2ActualOpenPositionRiskObservationV1"
            if kind == ExposureKind.OPEN
            else "V2ActualReservationRiskObservationV1",
            source_ref,
            source_available_at_ns,
            source_available_at_ns,
            source_metadata,
        )
    )
    item = PossibleRiskV2(
        kind,
        KEY,
        available_at_ns,
        Decimal(loss),
        Decimal(loss),
        Decimal("100"),
        Decimal("100"),
        Decimal("20"),
        Decimal("20"),
        source_ref,
    )
    index_risk_evidence(repo, item)
    return item


def _nonactual_outcome(repo: OpsRepository, pnl: str, outcome_class: OutcomeClass, name: str) -> ClosedV2Outcome:
    body = {"fixture_position": name}
    position_ref = sha256_json(body)
    index_research_evidence(repo, "Phase4NonActualPositionFixture", position_ref, _CUTOFF, body)
    item = ClosedV2Outcome(_CUTOFF - 1, _CUTOFF, Decimal(pnl), position_ref, outcome_class)
    repo.register_artifact(
        ArtifactIndexEntryV2(item.content_hash, "ClosedV2Outcome", item.content_hash, _CUTOFF, _CUTOFF, item.to_dict())
    )
    return item


def _case(
    repo: OpsRepository,
    *,
    unknown_loss: str = "20",
    opening_intents: int = 0,
    exposure_age_ns: int = 0,
    reconciliation_age_ns: int = 0,
    source_age_ns: int = 0,
):
    within_loss = actual_outcome(repo, _CUTOFF - DAY_NS + 1, _CUTOFF, Decimal("-20"), account_scope=_ACCOUNT)
    boundary_loss = actual_outcome(repo, _CUTOFF - DAY_NS, _CUTOFF, Decimal("-900"), account_scope=_ACCOUNT)
    actual_profit = actual_outcome(repo, _CUTOFF - 2, _CUTOFF, Decimal("100"), account_scope=_ACCOUNT)
    for outcome in (within_loss, boundary_loss, actual_profit):
        index_risk_evidence(repo, outcome)
    simulated = _nonactual_outcome(repo, "-500", OutcomeClass.SIMULATED, "simulated-loss")
    counterfactual = _nonactual_outcome(repo, "-800", OutcomeClass.COUNTERFACTUAL, "counterfactual-loss")
    closed = tuple(
        sorted(
            (within_loss, boundary_loss, actual_profit, simulated, counterfactual), key=lambda item: item.content_hash
        )
    )
    exposure_available_at_ns = _CUTOFF - exposure_age_ns
    opened = _possible(repo, ExposureKind.OPEN, "10", "open-risk", available_at_ns=exposure_available_at_ns)
    pending = _possible(
        repo,
        ExposureKind.UNKNOWN,
        unknown_loss,
        "unknown-risk",
        available_at_ns=exposure_available_at_ns,
        source_available_at_ns=min(exposure_available_at_ns, _CUTOFF - source_age_ns),
    )
    partial = _possible(repo, ExposureKind.PARTIAL, "5", "partial-risk", available_at_ns=exposure_available_at_ns)
    possible = tuple(sorted((opened, pending, partial), key=lambda item: item.content_hash))
    completeness_ref = _indexed_source(repo, "complete-exposure-set")
    snapshot = AccountRiskSnapshotV2(
        _ACCOUNT,
        _CUTOFF,
        Decimal("1000"),
        Decimal("700"),
        Decimal("300"),
        Decimal("0"),
        Decimal("10"),
        Decimal(unknown_loss) + Decimal("5"),
        Decimal("100"),
        Decimal("100"),
        Decimal("100"),
        Decimal("50"),
        Decimal("10"),
        opening_intents,
        tuple(item.content_hash for item in closed),
        (pending.content_hash, partial.content_hash),
        (opened.content_hash,),
        completeness_ref,
        "CURRENT",
    )
    index_risk_evidence(repo, snapshot)
    observation = {
        "account_snapshot_ref": snapshot.content_hash,
        "account_scope": _ACCOUNT,
        "source_class": "AUTHENTICATED_VENUE_RECONCILIATION",
        "sensitive_fields_excluded": True,
        "observed_at_ns": _CUTOFF,
    }
    observation_ref = sha256_json(observation)
    repo.register_artifact(
        ArtifactIndexEntryV2(
            observation_ref, "AuthenticatedVenueRiskObservationV2", observation_ref, _CUTOFF, _CUTOFF, observation
        )
    )
    reconciliation = {
        "account_scope": _ACCOUNT,
        "status": "CURRENT",
        "complete": True,
        "exposure_refs": [item.content_hash for item in possible],
        "closed_outcome_refs": [item.content_hash for item in closed],
    }
    reconciliation_ref = sha256_json(reconciliation)
    repo.register_artifact(
        ArtifactIndexEntryV2(
            reconciliation_ref,
            "AuthenticatedExposureReconciliationV2",
            reconciliation_ref,
            _CUTOFF - reconciliation_age_ns,
            _CUTOFF - reconciliation_age_ns,
            reconciliation,
        )
    )
    evidence = V2LiveRiskEvidence(
        snapshot, closed, possible, observation_ref, reconciliation_ref, "AUTHENTICATED_VENUE_RECONCILIATION"
    )
    v1 = engineering_default_policy(policy_version="SESSION024_RISK_FIXTURE", policy_effective_at_ns=0)
    v2 = RiskPolicyV2("SESSION024_RISK_FIXTURE", 0, v1.policy_hash(), Decimal("0.5"), Decimal("0.2"), 3)
    return evidence, v1, v2


def test_rolling_actual_loss_is_individual_and_excludes_simulated_counterfactual_and_boundary(tmp_path):
    with OpsRepository(tmp_path / "risk.sqlite") as repo:
        evidence, v1, v2 = _case(repo)
        result = revalidate_v2_rolling_risk(
            repo,
            evidence=evidence,
            risk_policy_v1=v1,
            risk_policy_v2=v2,
            account_scope=_ACCOUNT,
            cutoff_ns=_CUTOFF,
            proposed_normal_loss=Decimal("10"),
        )
        assert result.allowed is True
        assert result.realized_loss_consumed_24h == Decimal("20")
        assert result.existing_open_normal_loss == Decimal("10")
        assert result.pending_reserved_normal_loss == Decimal("25")
        assert result.evidence_hash == sha256_json(
            {
                "snapshot": evidence.account_snapshot.to_dict(),
                "closed_outcomes": [item.to_dict() for item in evidence.closed_outcomes],
                "possible_risks": [item.to_dict() for item in evidence.possible_risks],
                "account_observation_ref": evidence.account_observation_ref,
                "reconciliation_ref": evidence.reconciliation_ref,
                "cutoff_ns": _CUTOFF,
                "v1_risk_policy_hash": v1.policy_hash(),
                "risk_policy_v2_hash": v2.policy_hash,
            }
        )


def test_unknown_and_partial_reserved_loss_counts_against_new_risk_limit(tmp_path):
    with OpsRepository(tmp_path / "risk.sqlite") as repo:
        evidence, v1, v2 = _case(repo, unknown_loss="175")
        result = revalidate_v2_rolling_risk(
            repo,
            evidence=evidence,
            risk_policy_v1=v1,
            risk_policy_v2=v2,
            account_scope=_ACCOUNT,
            cutoff_ns=_CUTOFF,
            proposed_normal_loss=Decimal("10"),
        )
        assert result.allowed is False
        assert result.pending_reserved_normal_loss == Decimal("180")
        assert any("rolling 24h" in reason for reason in result.reasons)


def test_one_opening_intent_concurrency_is_enforced_even_when_v2_limit_is_higher(tmp_path):
    with OpsRepository(tmp_path / "risk.sqlite") as repo:
        evidence, v1, v2 = _case(repo, opening_intents=1)
        result = revalidate_v2_rolling_risk(
            repo,
            evidence=evidence,
            risk_policy_v1=v1,
            risk_policy_v2=v2,
            account_scope=_ACCOUNT,
            cutoff_ns=_CUTOFF,
            proposed_normal_loss=Decimal("1"),
        )
        assert result.allowed is False
        assert "max opening intents per account exceeded" in result.reasons


def test_current_v2_evidence_must_cover_durable_v1_reservations_and_intents(tmp_path):
    with OpsRepository(tmp_path / "risk.sqlite") as repo:
        evidence, v1, v2 = _case(repo)
        with pytest.raises(ValueError, match="omits durable V1 reservation"):
            revalidate_v2_rolling_risk(
                repo,
                evidence=evidence,
                risk_policy_v1=v1,
                risk_policy_v2=v2,
                account_scope=_ACCOUNT,
                cutoff_ns=_CUTOFF,
                proposed_normal_loss=Decimal("1"),
                v1_reserved_normal_loss=Decimal("36"),
            )
        with pytest.raises(ValueError, match="omits a durable V1 opening intent"):
            revalidate_v2_rolling_risk(
                repo,
                evidence=evidence,
                risk_policy_v1=v1,
                risk_policy_v2=v2,
                account_scope=_ACCOUNT,
                cutoff_ns=_CUTOFF,
                proposed_normal_loss=Decimal("1"),
                v1_unresolved_opening_intents=1,
            )


def test_stale_evidence_changed_policy_and_wrong_account_fail_closed(tmp_path):
    with OpsRepository(tmp_path / "risk.sqlite") as repo:
        evidence, v1, v2 = _case(repo)
        stale = replace(
            evidence, account_snapshot=replace(evidence.account_snapshot, available_at_ns=_CUTOFF - 1_000_000_001)
        )
        with pytest.raises(ValueError, match="cutoff-current"):
            revalidate_v2_rolling_risk(
                repo,
                evidence=stale,
                risk_policy_v1=v1,
                risk_policy_v2=v2,
                account_scope=_ACCOUNT,
                cutoff_ns=_CUTOFF,
                proposed_normal_loss=Decimal("1"),
            )
        changed_v2 = RiskPolicyV2("changed", 0, _ACCOUNT, Decimal("0.5"), Decimal("0.2"), 1)
        with pytest.raises(ValueError, match="binding mismatch"):
            revalidate_v2_rolling_risk(
                repo,
                evidence=evidence,
                risk_policy_v1=v1,
                risk_policy_v2=changed_v2,
                account_scope=_ACCOUNT,
                cutoff_ns=_CUTOFF,
                proposed_normal_loss=Decimal("1"),
            )
        with pytest.raises(ValueError, match="account identity"):
            revalidate_v2_rolling_risk(
                repo,
                evidence=evidence,
                risk_policy_v1=v1,
                risk_policy_v2=v2,
                account_scope=sha256_json("other-account"),
                cutoff_ns=_CUTOFF,
                proposed_normal_loss=Decimal("1"),
            )


@pytest.mark.parametrize(
    ("exposure_age_ns", "reconciliation_age_ns", "source_age_ns"),
    [(0, 1_000_000_001, 0), (1_000_000_001, 0, 0), (0, 0, 1_000_000_001)],
)
def test_stale_exposure_reconciliation_or_possible_risk_fails_closed(
    tmp_path, exposure_age_ns, reconciliation_age_ns, source_age_ns
):
    with OpsRepository(tmp_path / "risk.sqlite") as repo:
        evidence, v1, v2 = _case(
            repo,
            exposure_age_ns=exposure_age_ns,
            reconciliation_age_ns=reconciliation_age_ns,
            source_age_ns=source_age_ns,
        )
        message = "complete current exposure reconciliation" if reconciliation_age_ns else "cutoff-current"
        with pytest.raises(ValueError, match=message):
            revalidate_v2_rolling_risk(
                repo,
                evidence=evidence,
                risk_policy_v1=v1,
                risk_policy_v2=v2,
                account_scope=_ACCOUNT,
                cutoff_ns=_CUTOFF,
                proposed_normal_loss=Decimal("1"),
            )


def test_nonredacted_or_unindexed_authenticated_source_does_not_authorize_live_risk(tmp_path):
    with OpsRepository(tmp_path / "risk.sqlite") as repo:
        evidence, v1, v2 = _case(repo)
        original = repo.get_artifact(evidence.account_observation_ref)
        assert original is not None
        forged_metadata = dict(original.metadata) | {"sensitive_fields_excluded": False}
        forged_ref = sha256_json(forged_metadata)
        repo.register_artifact(
            ArtifactIndexEntryV2(
                forged_ref, "AuthenticatedVenueRiskObservationV2", forged_ref, _CUTOFF, _CUTOFF, forged_metadata
            )
        )
        forged_evidence = replace(evidence, account_observation_ref=forged_ref)
        with pytest.raises(ValueError, match="authenticated current venue account observation"):
            revalidate_v2_rolling_risk(
                repo,
                evidence=forged_evidence,
                risk_policy_v1=v1,
                risk_policy_v2=v2,
                account_scope=_ACCOUNT,
                cutoff_ns=_CUTOFF,
                proposed_normal_loss=Decimal("1"),
            )
