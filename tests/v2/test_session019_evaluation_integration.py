"""Strict persistence path for a decision-time economic admission."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import canonical_json, json_value, sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.science.action import freeze_action
from atlas.v2.science.admission import (
    ADMISSION_POLICY_VERSION,
    AdmissionPolicyV2,
    VenueCapabilitySnapshotV2,
    VenueCapabilityStatusV2,
    index_admission_evidence,
    index_amended_evaluation,
    index_venue_capability_snapshot,
)
from atlas.v2.science.evaluation_service import run_phase2_economic_evaluation
from atlas.v2.science.outcomes import AdmissionStateV2, DecisionCalendarEntryV2
from atlas.v2.science.pretrade import CausalInputV2
from atlas.v2.strategies.s1_trend import S1_POLICY

from .test_session014_core import KEY
from .test_session017_risk import CUTOFF, size


def test_unestimable_exact_action_persists_amended_evaluation_and_terminal_calendar(tmp_path, monkeypatch):
    from atlas.v2.contracts import ArtifactEnvelope, FeatureArtifactV2, FeatureValueV2, ReplayViewV2

    from . import test_session017_risk as risk_module

    original_candidate = risk_module.candidate
    features: dict[str, FeatureArtifactV2] = {}
    active_repository: OpsRepository | None = None

    def candidate_with_feature(policy=S1_POLICY, key=KEY, **kwargs):
        item = original_candidate(policy, key, **kwargs)
        indexed_feature = active_repository.get_artifact(item.snapshot_hash) if active_repository is not None else None
        if indexed_feature is not None and indexed_feature.artifact_type == "FeatureArtifactV2":
            return item
        feature = FeatureArtifactV2(
            ArtifactEnvelope(
                1, f"session019-admission-feature-{item.candidate_id}", CUTOFF, CUTOFF, "session019-fixture", ()
            ),
            item.key,
            "SESSION019_FIXTURE_V1",
            CUTOFF,
            CUTOFF,
            {"h4.ema20": FeatureValueV2(Decimal("100"), "PRICE")},
            sha256_json("fixture-source-health"),
            ReplayViewV2.ACTUAL_SYSTEM,
        )
        item = replace(
            item,
            snapshot_hash=feature.content_hash,
            envelope=replace(item.envelope, content_hash="", input_refs=(feature.content_hash,)),
        )
        features[feature.content_hash] = feature
        return item

    monkeypatch.setattr(risk_module, "candidate", candidate_with_feature)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        active_repository = repo
        case = risk_module.risk_case(repo)
        for ref, feature in features.items():
            repo.register_artifact(
                ArtifactIndexEntryV2(ref, "FeatureArtifactV2", ref, CUTOFF, CUTOFF, {"feature": feature.to_dict()})
            )
        sizing = size(repo, case)
        action = freeze_action(
            repo,
            candidate=case.candidate,
            candidate_set=case.candidate_set,
            sizing=sizing,
            product=case.product,
            policy=S1_POLICY,
            v1=case.v1,
            v2=case.v2,
        )

        def causal(kind: str) -> CausalInputV2:
            body = {"kind": kind, "decision_cutoff_ns": CUTOFF}
            ref = sha256_json(body)
            repo.register_artifact(ArtifactIndexEntryV2(ref, kind, ref, CUTOFF, CUTOFF, body))
            return CausalInputV2(ref, kind, CUTOFF, CUTOFF)

        model_input = causal("Session019ModelFixtureV1")
        calibration_input = causal("Session019CalibrationFixtureV1")
        execution_input = causal("Session019ExecutionModelFixtureV1")
        profile_refs = {
            name: sha256_json({"session019-profile": name}) for name in ("nautilus-artifact", "execution", "protection")
        }
        policy = AdmissionPolicyV2(
            ADMISSION_POLICY_VERSION,
            Decimal("1"),
            30,
            30,
            20,
            Decimal("1.96"),
            "ISOLATED",
            "ONE_WAY",
            "nautilus_trader",
            "2.0.0rc5",
            "1b0a49d2792a9432a3aca3fcb617ce7a630d905e",
            profile_refs["nautilus-artifact"],
            profile_refs["execution"],
            profile_refs["protection"],
            "SESSION019_CAPABILITY_PROFILE_V1",
        )
        capability = VenueCapabilitySnapshotV2(
            action.action.key.venue,
            action.action.key.environment,
            case.account.account_scope,
            action.action.product_ref,
            action.action.key.content_hash,
            "ISOLATED",
            "ONE_WAY",
            "nautilus_trader",
            "2.0.0rc5",
            "1b0a49d2792a9432a3aca3fcb617ce7a630d905e",
            profile_refs["nautilus-artifact"],
            profile_refs["execution"],
            profile_refs["protection"],
            "SESSION019_CAPABILITY_PROFILE_V1",
            VenueCapabilityStatusV2.UNVERIFIED,
            (),
            CUTOFF,
        )
        evaluated = run_phase2_economic_evaluation(
            repo,
            action=action,
            candidate=case.candidate,
            candidate_set=case.candidate_set,
            sizing=sizing,
            product=case.product,
            risk_policy=case.v1,
            risk_policy_v2=case.v2,
            account=case.account,
            fee=case.fee,
            admission_policy=policy,
            capability=capability,
            model_input=model_input,
            calibration_input=calibration_input,
            execution_model_input=execution_input,
            available_at_ns=CUTOFF + 10,
            scenario_seed=1901,
            scenario_count=100,
        )
        prediction = evaluated.prediction
        assert prediction.status == "NOT_ESTIMABLE"
        support = evaluated.inference_support
        estimation, numerical = evaluated.estimation, evaluated.numerical
        result, evaluation = evaluated.admission, evaluated.evaluation
        evaluation_ref, calendar_ref = evaluated.evaluation_ref, evaluated.calendar_ref
        assert result.decision.value == "NOT_ESTIMABLE"
        indexed_eval = repo.get_artifact(evaluation_ref)
        indexed_calendar = repo.get_artifact(calendar_ref)
        assert indexed_eval is not None and canonical_json(indexed_eval.metadata["evaluation"]) == canonical_json(
            evaluation.to_dict()
        )
        assert indexed_calendar is not None
        calendar = DecisionCalendarEntryV2.from_dict(json_value(indexed_calendar.metadata["decision_entry"]))
        assert calendar.source_artifact_ref == evaluation_ref
        assert calendar.action_hash == action.action.action_hash
        assert calendar.action_artifact_ref == action.content_hash
        assert calendar.selection_state.value == "SELECTED"
        assert calendar.admission_state == AdmissionStateV2.NOT_ESTIMABLE
        assert calendar.reason_codes == evaluation.reason_codes
        for field in (
            "action_hash",
            "candidate_ref",
            "candidate_set_ref",
            "risk_policy_ref",
            "pretrade_scenario_ref",
            "m0_prediction_ref",
            "support_ref",
            "ood_ref",
            "capability_evidence_ref",
            "numerical_error_ref",
        ):
            with pytest.raises(ValueError):
                index_amended_evaluation(
                    repo, replace(evaluation, **{field: sha256_json({"wrong-session019-ref": field})})
                )
        forged_capability = replace(capability, account_scope="WRONG_ACCOUNT_SCOPE")
        forged_capability_ref = index_venue_capability_snapshot(repo, forged_capability)
        with pytest.raises(ValueError, match="wrong venue/account/product/runtime/profile"):
            index_amended_evaluation(repo, replace(evaluation, capability_evidence_ref=forged_capability_ref))
        forged_support = replace(support, independent_scenario_support_units=1)
        forged_support_ref = index_admission_evidence(repo, "InferenceSupportV2", forged_support.to_dict(), CUTOFF + 3)
        with pytest.raises(ValueError):
            index_amended_evaluation(repo, replace(evaluation, support_ref=forged_support_ref))
        forged_estimation = replace(
            estimation, status="AVAILABLE", standard_error=Decimal(0), uncertainty_amount=Decimal(0)
        )
        forged_ref = index_admission_evidence(repo, "EstimationUncertaintyV2", forged_estimation.to_dict(), CUTOFF + 3)
        with pytest.raises(ValueError, match="estimation uncertainty does not reproduce"):
            index_amended_evaluation(repo, replace(evaluation, estimation_uncertainty_ref=forged_ref))
        forged_numerical = replace(
            numerical,
            cost_model_ref=sha256_json("forged-cost-model"),
            run_a_ref=sha256_json("missing-convergence-run-a"),
            run_b_ref=sha256_json("missing-convergence-run-b"),
            seed_a=1901,
            seed_b=1902,
            path_count_a=100,
            path_count_b=1_000,
            estimate_a=Decimal(0),
            estimate_b=Decimal(0),
            m0_conversion_error=Decimal(0),
            error_bound=Decimal(0),
            status="AVAILABLE",
            reason=None,
        )
        forged_numerical_ref = index_admission_evidence(
            repo, "NumericalErrorV2", forged_numerical.to_dict(), CUTOFF + 3
        )
        with pytest.raises(ValueError):
            index_amended_evaluation(repo, replace(evaluation, numerical_error_ref=forged_numerical_ref))
        assert len(repo.artifact_entries("EvaluationArtifactV2")) == 1
        assert len(repo.artifact_entries("DecisionCalendarEntryV2")) == 1
