"""Strict zero-authority B contract validator and deterministic A control.

Consumes offline request/response identities in the existing ops artifact index.
This is not a capital approval, reservation transaction or OMS dispatcher.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import ClassVar, cast

from atlas.v2._serialization import sha256_json
from atlas.v2.contracts import EligibleSetV1, SelectionResultV1
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.research.mean_evidence import ChartEvidenceBundleV1, EvidenceClaimV1, verify_chart_claim
from atlas.v2.risk import OfflineRiskSnapshotV1
from atlas.v2.science.action import ExactActionV1
from atlas.v2.science.mean_calibration import MeanSelectionCalibrationV2, verify_mean_calibration
from atlas.v2.science.mean_common import OfflineArtifact, unique
from atlas.v2.science.mean_evaluation import (
    CashAllowanceV1,
    MeanContextV1,
    _require,
    index_mean_artifact,
    read_mean_artifact,
)
from atlas.v2.science.mean_models import MeanModelV1
from atlas.v2.science.mean_scenario_evidence import MeanScenarioSetV2, verify_scenario_evidence


@dataclass(frozen=True)
class MeanSelectionRequestV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_B_SELECTION_REQUEST_OFFLINE_V1"
    eligible_set_ref: str
    account_snapshot_ref: str
    permission_ref: str
    source_prefix_ref: str
    issued_at_ns: int
    deadline_ns: int
    nonce: str
    allowed_bundle_refs: tuple[str, ...]
    operation_scope: str = "OFFLINE_SELECT_ONE"

    def __post_init__(self) -> None:
        super().__post_init__()
        unique(self.allowed_bundle_refs)
        if not self.nonce or self.issued_at_ns >= self.deadline_ns or self.operation_scope != "OFFLINE_SELECT_ONE":
            raise ValueError("invalid exact selection request")


@dataclass(frozen=True)
class MeanSelectionResponseV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_B_SELECTION_RESPONSE_OFFLINE_V1"
    request_ref: str
    eligible_set_ref: str
    account_snapshot_ref: str
    permission_ref: str
    source_prefix_ref: str
    response_id: str
    nonce: str
    decision: str
    action_ref: str | None
    returned_at_ns: int
    claims: tuple[EvidenceClaimV1, ...]
    reason: str
    operation_scope: str = "OFFLINE_SELECT_ONE"

    def __post_init__(self) -> None:
        super().__post_init__()
        if (
            self.decision not in ("SELECT_ONE", "REJECT_ALL", "ABSTAIN")
            or not self.response_id
            or not self.reason
            or (self.decision == "SELECT_ONE") != (self.action_ref is not None)
            or len(self.claims) > 16
            or self.operation_scope != "OFFLINE_SELECT_ONE"
        ):
            raise ValueError("invalid strict B response")


def validate_offline_b_selection(
    repo: OpsRepository,
    *,
    request: MeanSelectionRequestV1,
    response: MeanSelectionResponseV1 | None,
    eligible: EligibleSetV1,
    snapshot: OfflineRiskSnapshotV1,
    now_ns: int,
    current_evaluation_refs: tuple[str, ...],
    bundles: tuple[ChartEvidenceBundleV1, ...] = (),
) -> SelectionResultV1:
    if response is None:
        raise ValueError("PROVIDER_UNAVAILABLE_NO_OPENING_FALLBACK")
    if (
        request.eligible_set_ref != eligible.content_hash
        or response.eligible_set_ref != eligible.content_hash
        or response.request_ref != request.content_hash
        or response.nonce != request.nonce
        or request.account_snapshot_ref != snapshot.content_hash
        or response.account_snapshot_ref != snapshot.content_hash
        or snapshot.content_hash != eligible.population.account_snapshot_ref
        or request.permission_ref != snapshot.permission_ref
        or response.permission_ref != snapshot.permission_ref
        or response.source_prefix_ref != request.source_prefix_ref
        or request.source_prefix_ref != eligible.population.instrument_revision_ref
    ):
        raise ValueError("CHANGED_ACTION_SET_ACCOUNT_PERMISSION_OR_PREFIX")
    if (
        not eligible.sealed_at_ns <= request.issued_at_ns <= response.returned_at_ns <= now_ns < request.deadline_ns
        or request.deadline_ns != eligible.population.deadline_ns
        or now_ns >= snapshot.expires_at_ns
        or not snapshot.complete
        or snapshot.status != "CURRENT"
    ):
        raise ValueError("STALE_EXPIRED_OR_PRETRIGGER_RESPONSE")
    if tuple(sorted(current_evaluation_refs)) != tuple(sorted(e.content_hash for e in eligible.evaluations)):
        raise ValueError("MATERIAL_EVALUATION_REVISIONS_CHANGED")
    for evaluated_member in eligible.members:
        action = read_mean_artifact(repo, evaluated_member.action_ref, ExactActionV1, consumer_ns=now_ns)
        for source_ref in action.source_refs:
            source = repo.get_artifact(source_ref)
            if source is not None and source.artifact_type == "MeanContextV1":
                context = read_mean_artifact(repo, source_ref, MeanContextV1, consumer_ns=now_ns)
                if now_ns - context.source_observed_at_ns > context.maximum_source_age_ns:
                    raise ValueError("ACTION_SOURCE_FRESHNESS_EXPIRED")
        model = read_mean_artifact(repo, evaluated_member.model_ref, MeanModelV1, consumer_ns=now_ns)
        calibration = read_mean_artifact(
            repo, evaluated_member.calibration_ref, MeanSelectionCalibrationV2, consumer_ns=now_ns
        )
        from atlas.v2.science.mean_evaluation import MeanEvaluationPolicyV1

        policy = read_mean_artifact(
            repo, eligible.population.evaluation_policy_ref, MeanEvaluationPolicyV1, consumer_ns=now_ns
        )
        scenarios = read_mean_artifact(repo, evaluated_member.scenario_ref, MeanScenarioSetV2, consumer_ns=now_ns)
        verify_mean_calibration(
            repo,
            calibration,
            model=model,
            policy_ref=policy.content_hash,
            product_ref=action.product_ref,
            cutoff_ns=action.cutoff_ns,
            now_ns=now_ns,
        )
        if evaluated_member.mean_selection_allowance != calibration.allowance_cash:
            raise ValueError("SAVED_EVALUATION_CALIBRATION_ALLOWANCE_CHANGED")
        from atlas.v2.science.mean_scenario_evidence import ScenarioSourceV1

        scenario_source = read_mean_artifact(repo, scenarios.source_ref, ScenarioSourceV1, consumer_ns=now_ns)
        if scenario_source.scope_ref != calibration.source_ref:
            raise ValueError("CALIBRATION_SCENARIO_SCOPE_MISMATCH")
        verify_scenario_evidence(
            repo,
            scenarios,
            product_ref=action.product_ref,
            policy=policy,
            snapshot=snapshot,
            compatibility_ref=model.compatibility_ref,
            cutoff_ns=action.cutoff_ns,
            now_ns=now_ns,
        )
        if now_ns >= min(model.expires_at_ns, calibration.expires_at_ns):
            raise ValueError("MODEL_OR_CALIBRATION_EXPIRED")
        for ref in (
            evaluated_member.execution_bias_ref,
            evaluated_member.numerical_ref,
            evaluated_member.incremental_programme_ref,
        ):
            allowance = read_mean_artifact(repo, ref, CashAllowanceV1, consumer_ns=now_ns)
            if now_ns >= allowance.expires_at_ns:
                raise ValueError("ECONOMIC_EVIDENCE_EXPIRED")
        from atlas.v2.science.mean_evaluation_validation import require_evaluation_validation

        require_evaluation_validation(repo, evaluated_member, now_ns=now_ns)
    for obj in (request, eligible, snapshot):
        _require(repo, obj, now_ns)
    if response.decision == "SELECT_ONE":
        member = next((e for e in eligible.members if e.action_ref == response.action_ref), None)
        if member is None:
            raise ValueError("OUTSIDE_ELIGIBLE_SET")
        _require(repo, member, now_ns)
    if response.action_ref is not None:
        selected_action = read_mean_artifact(repo, response.action_ref, ExactActionV1, consumer_ns=now_ns)
        if any(
            b.action_ref == response.action_ref
            and (
                b.key != selected_action.key
                or b.product_ref != selected_action.product_ref
                or b.action_side != selected_action.side
                or b.cutoff_ns != eligible.population.cutoff_ns
            )
            for b in bundles
        ):
            raise ValueError("CHART_WRONG_ACTION_INSTRUMENT_SIDE_OR_PREFIX")
    by_ref = {b.content_hash: b for b in bundles}
    for claim in response.claims:
        bundle = by_ref.get(claim.bundle_ref)
        if (
            bundle is None
            or claim.bundle_ref not in request.allowed_bundle_refs
            or bundle.eligible_set_ref != eligible.content_hash
            or bundle.deadline_ns != request.deadline_ns
            or (response.action_ref is not None and bundle.action_ref != response.action_ref)
            or not verify_chart_claim(bundle, claim)
        ):
            raise ValueError("UNSUPPORTED_TOOL_OR_CHART_ASSERTION")
    request_key = sha256_json({"offline_selection_consumption": request.content_hash})
    response_key = sha256_json({"offline_response_consumption": response.response_id})
    result = SelectionResultV1(
        request.content_hash,
        eligible.content_hash,
        response.response_id,
        response.decision,
        response.action_ref,
        now_ns,
    )
    # Existing ops composition provides durable all-or-none receipt writes.
    # No operational approval/reservation/account state is changed.
    with repo.atomic_composition():
        if repo.get_artifact(request_key) is not None or repo.get_artifact(response_key) is not None:
            raise ValueError("DUPLICATE_OR_CONSUMED_RESPONSE")
        index_mean_artifact(repo, response, available_at_ns=now_ns)
        index_mean_artifact(
            repo, result, available_at_ns=now_ns, input_refs=(request.content_hash, eligible.content_hash)
        )
        body = {
            "request_ref": request.content_hash,
            "response_ref": response.content_hash,
            "result_ref": result.content_hash,
            "authority": "ZERO",
        }
        repo.register_artifacts(
            tuple(
                ArtifactIndexEntryV2(ref, "OfflineSelectionConsumptionV1", sha256_json(body), now_ns, now_ns, body)
                for ref in (request_key, response_key)
            )
        )
    return result


def deterministic_a_control(eligible: EligibleSetV1) -> str | None:
    """Corrected deterministic research comparator; never outage fallback."""
    return (
        min(eligible.members, key=lambda e: (-cast(Decimal, e.j_cash), e.action_ref)).action_ref
        if eligible.members
        else None
    )
