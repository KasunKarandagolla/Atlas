"""Postreceipt zero-authority comparison of persisted frozen-action diagnostics.

This maintenance pass only reads sealed model outputs. It never fits a model,
votes on a decision, or changes admission or action state.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

from atlas.v2._serialization import FrozenMap, json_value, sha256_json
from atlas.v2.contracts import CandidateActionV2, CandidateSetV2
from atlas.v2.instruments import InstrumentKeyV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.ops_supervisor import OpsSupervisorReceiptV1, _receipt_from_dict
from atlas.v2.science.action import ActionArtifactV2, FrozenActionV2
from atlas.v2.science.admission import AmendedEvaluationArtifactV2
from atlas.v2.science.m0 import M0PredictionV2
from atlas.v2.science.m1 import M1PredictionV2
from atlas.v2.science.phase3 import COMPARISON_VERSION, compare_frozen_action

COMPARISON_RECEIPT_V1 = "FrozenActionComparisonMaintenanceReceiptV1"
COMPARISON_LANE_V1 = "FROZEN_ACTION_COMPARISON_V1"


@dataclass(frozen=True)
class FrozenActionComparisonWorkResultV1:
    processed_receipt_ref: str | None
    comparison_ref: str | None
    receipt_ref: str | None
    status: str


class _DiagnosticView:
    """Typed comparison projection over an already hash-verified wire body."""

    def __init__(self, body: Mapping[str, Any]):
        self._body = dict(body)
        self.missing_feature_counts = tuple(tuple(item) for item in body.get("missing_feature_counts", ()))

    def to_dict(self) -> dict[str, Any]:
        return json_value(self._body)


def _artifact(repository: OpsRepository, ref: str | None, kind: str, *, now_ns: int,
              deadline_ns: int | None = None, body_key: str) -> tuple[ArtifactIndexEntryV2 | None, Mapping[str, Any] | None, str | None]:
    if not isinstance(ref, str):
        return None, None, f"MISSING_{kind.upper()}_REF"
    entry = repository.get_artifact(ref)
    if entry is None:
        return None, None, f"MISSING_{kind.upper()}_ARTIFACT"
    body = entry.metadata.get(body_key)
    if (entry.artifact_type != kind or entry.artifact_ref != ref or entry.content_hash != ref
            or not isinstance(body, Mapping) or sha256_json(body) != ref):
        return entry, None, f"INVALID_{kind.upper()}_ARTIFACT"
    if entry.available_at_ns > now_ns:
        return entry, None, f"FUTURE_{kind.upper()}_ARTIFACT"
    if deadline_ns is not None and entry.available_at_ns > deadline_ns:
        return entry, None, f"LATE_{kind.upper()}_ARTIFACT"
    return entry, body, None


def _load_m1(repository: OpsRepository, ref: str, *, now_ns: int, deadline_ns: int,
             expected_action_hash: str, expected_action_ref: str, expected_candidate_ref: str,
             expected_candidate_set_ref: str, cutoff_ns: int) -> tuple[Any | None, str | None]:
    existing = repository.get_artifact(ref)
    if existing is not None and existing.artifact_type == "OpsZeroAuthorityDiagnosticV1":
        diagnostic = existing.metadata.get("diagnostic")
        if (existing.content_hash == ref and isinstance(diagnostic, Mapping)
                and sha256_json(diagnostic) == ref and diagnostic.get("kind") == "M1"
                and diagnostic.get("action_hash") == expected_action_hash
                and diagnostic.get("action_artifact_ref") == expected_action_ref
                and existing.available_at_ns <= now_ns and existing.available_at_ns <= deadline_ns):
            return None, "M1_MODEL_ARTIFACT_UNAVAILABLE"
        return None, "INVALID_M1_UNAVAILABLE_DIAGNOSTIC"
    entry, body, reason = _artifact(repository, ref, "M1PredictionV2", now_ns=now_ns,
        deadline_ns=deadline_ns, body_key="prediction")
    if reason:
        return None, reason
    assert entry is not None and body is not None
    try:
        row = dict(body)
        row.pop("version", None)
        from atlas.v2._serialization import decimal_value
        row["expected_net_value"] = decimal_value(row["expected_net_value"], field="expected_net_value", wire=True) if row["expected_net_value"] is not None else None
        row["training_row_refs"] = tuple(row["training_row_refs"])
        prediction = M1PredictionV2(**row)
        if (prediction.content_hash != ref or prediction.action_hash != expected_action_hash
                or prediction.action_artifact_ref != expected_action_ref
                or prediction.candidate_ref != expected_candidate_ref
                or prediction.candidate_set_ref != expected_candidate_set_ref
                or prediction.information_cutoff_ns != cutoff_ns or prediction.available_at_ns != entry.available_at_ns):
            return None, "MISMATCHED_M1_PREDICTION"
        pieces: dict[str, Mapping[str, Any]] = {}
        for child_ref, child_kind, key in (
            (prediction.model_fit_ref, "M1ModelFitV2", "model_fit"),
            (prediction.support_ref, "M1SupportV2", "support"),
            (prediction.calibration_ref, "M1CalibrationV2", "calibration"),
            (prediction.ood_ref, "M1OODV2", "ood"),
        ):
            _, child, child_reason = _artifact(repository, child_ref, child_kind, now_ns=now_ns,
                deadline_ns=deadline_ns, body_key=key)
            if child_reason or child is None:
                return None, child_reason or f"MISSING_{child_kind.upper()}"
            pieces[key] = child
        support = pieces["support"]
        if support.get("action_hash") != expected_action_hash or support.get("cutoff_ns") != cutoff_ns:
            return None, "MISMATCHED_M1_SUPPORT"
        calibration, ood = pieces["calibration"], pieces["ood"]
        model_fit = pieces["model_fit"]
        reservation_ref = model_fit.get("final_holdout_reservation_ref")
        _, reservation, reservation_reason = _artifact(repository, reservation_ref,
            "M1FinalHoldoutReservationV2", now_ns=now_ns, deadline_ns=deadline_ns, body_key="reservation")
        if reservation_reason or reservation is None:
            return None, reservation_reason or "MISSING_M1_FINAL_HOLDOUT_RESERVATION"
        reservation_start = reservation.get("start_ns")
        reservation_end = reservation.get("end_ns")
        if (not isinstance(reservation_start, int) or isinstance(reservation_start, bool)
                or not isinstance(reservation_end, int) or isinstance(reservation_end, bool)):
            return None, "INVALID_M1_HOLDOUT_RESERVATION_CHRONOLOGY"
        # M1RunV2's comparison projection consumes prediction, support,
        # calibration, OOD and chronology. The latter is bound by the exact
        # persisted OOF archive referenced by the prediction.
        _, archive, archive_reason = _artifact(repository, prediction.oof_archive_ref,
            "M1OOFArchiveV2", now_ns=now_ns, deadline_ns=deadline_ns, body_key="archive")
        if archive_reason or archive is None:
            return None, archive_reason or "MISSING_M1_OOF_ARCHIVE"
        chronology_ref = archive.get("chronology_ref")
        _, chronology, chronology_reason = _artifact(repository, chronology_ref,
            "M1ChronologyV2", now_ns=now_ns, deadline_ns=deadline_ns, body_key="chronology")
        if chronology_reason or chronology is None:
            return None, chronology_reason or "MISSING_M1_CHRONOLOGY"
        if (calibration.get("action_hash") != expected_action_hash
                or calibration.get("cutoff_ns") != reservation_start
                or reservation_end > cutoff_ns
                or reservation_start >= reservation_end
                or reservation.get("state") != "UNTOUCHED"
                or reservation.get("compatibility_key") != prediction.compatibility_key
                or chronology.get("as_of_ns") != reservation_end
                or chronology.get("holdout_state") != "UNTOUCHED"
                or ood.get("action_hash") != expected_action_hash
                or ood.get("feature_ref") != prediction.feature_vector_ref
                or model_fit.get("compatibility_key") != prediction.compatibility_key
                or model_fit.get("fit_cutoff_ns") != cutoff_ns
                or archive.get("version") != "M1_CHRONOLOGICAL_OOF_ARCHIVE_V1"
                or calibration.get("oof_archive_ref") != prediction.oof_archive_ref):
            return None, "MISMATCHED_M1_OOF_ARCHIVE"
        run = SimpleNamespace(prediction=prediction, support=_DiagnosticView(support),
            calibration=_DiagnosticView(pieces["calibration"]), ood=_DiagnosticView(pieces["ood"]),
            chronology=_DiagnosticView(chronology))
        return run, None
    except (KeyError, TypeError, ValueError, ArithmeticError):
        return None, "INVALID_M1_PREDICTION_BODY"


def _load_analogue(repository: OpsRepository, ref: str, *, now_ns: int, deadline_ns: int,
                   expected_action_hash: str, expected_action_ref: str, expected_candidate_ref: str,
                   expected_candidate_set_ref: str, cutoff_ns: int) -> tuple[Any | None, str | None]:
    entry, body, reason = _artifact(repository, ref, "AnalogueActionValueV2", now_ns=now_ns,
        deadline_ns=deadline_ns, body_key="analogue")
    if reason:
        return None, reason
    assert entry is not None and body is not None
    if (body.get("version") != "ANALOGUE_ACTION_VALUE_V2_V2"
            or body.get("query_action_hash") != expected_action_hash
            or body.get("query_action_ref") != expected_action_ref
            or body.get("query_candidate_ref") != expected_candidate_ref
            or body.get("query_candidate_set_ref") != expected_candidate_set_ref
            or body.get("information_cutoff_ns") != cutoff_ns):
        return None, "MISMATCHED_ANALOGUE_ACTION_VALUE"
    # Exact wire hash plus all fields consumed by compare_frozen_action.
    try:
        return SimpleNamespace(
            content_hash=ref, query_action_hash=body["query_action_hash"],
            query_action_ref=body["query_action_ref"], query_candidate_ref=body["query_candidate_ref"],
            query_candidate_set_ref=body["query_candidate_set_ref"], information_cutoff_ns=body["information_cutoff_ns"],
            weighted_estimate=body.get("weighted_estimate"), independent_support_count=body["independent_support_count"],
            effective_sample_size=body.get("effective_sample_size"), ood_status=body["ood_status"],
            compatibility_key=body.get("compatibility_key"), support_status=body["support_status"],
            missing_features=tuple(body.get("missing_features", ()))), None
    except (KeyError, TypeError, ValueError):
        return None, "INVALID_ANALOGUE_ACTION_VALUE"


def _receipt_body(repository: OpsRepository, receipt_ref: str, *, now_ns: int) -> tuple[OpsSupervisorReceiptV1 | None, str | None]:
    entry = repository.get_artifact(receipt_ref)
    body = entry.metadata.get("receipt") if entry is not None else None
    if (entry is None or entry.artifact_type != "OpsSupervisorReceiptV1"
            or entry.artifact_ref != sha256_json({"artifact_type": "OpsSupervisorReceiptV1",
                "content_hash": entry.content_hash})
            or not isinstance(body, Mapping) or sha256_json(body) != entry.content_hash
            or entry.available_at_ns > now_ns):
        return None, "INVALID_OR_FUTURE_ORIGINATING_RECEIPT"
    try:
        receipt = _receipt_from_dict(json_value(body))
        if (receipt.content_hash != entry.content_hash or receipt.created_at_ns != entry.available_at_ns
                or receipt.created_at_ns > now_ns or receipt.action_ref is None or receipt.candidate_set_ref is None):
            return None, "ORIGINATING_RECEIPT_HAS_NO_FROZEN_ACTION"
        return receipt, None
    except (KeyError, TypeError, ValueError):
        return None, "INVALID_ORIGINATING_RECEIPT_BODY"


class FrozenActionComparisonMaintenanceV1:
    """Process at most one oldest unprocessed durable supervisor receipt."""

    def __init__(self, repository: OpsRepository, *, clock_ns: Callable[[], int] = time.time_ns):
        if repository.read_only:
            raise ValueError("comparison maintenance requires the operational writer")
        self.repository = repository
        self.clock_ns = clock_ns

    def run_one(self) -> FrozenActionComparisonWorkResultV1:
        now = self.clock_ns()
        work = self.repository.due_work_items(COMPARISON_LANE_V1, as_of_ns=now, limit=1)
        if not work:
            return FrozenActionComparisonWorkResultV1(None, None, None, "IDLE")
        item = work[0]
        receipt_ref = item.source_ref
        identity_ref = sha256_json({"version": COMPARISON_RECEIPT_V1,
            "originating_receipt_ref": receipt_ref})
        with self.repository.atomic_composition():
            prior = self.repository.get_artifact(identity_ref)
            if prior is not None:
                body = prior.metadata.get("comparison_receipt")
                if (prior.artifact_type != COMPARISON_RECEIPT_V1 or not isinstance(body, Mapping)
                        or prior.content_hash != sha256_json(body)
                        or body.get("originating_receipt_ref") != receipt_ref):
                    raise ValueError("frozen-action comparison completion identity is corrupt")
                result = dict(body)
            else:
                try:
                    result = self._process(receipt_ref, now_ns=now)
                except (ValueError, TypeError, KeyError, ArithmeticError):
                    # Indexed source corruption is a terminal observation for
                    # this maintenance item. Keep the reason stable and do not
                    # let one bad source retain the head of the bounded lane.
                    # SQLite and other operational failures deliberately escape
                    # so the transaction rolls back and the work stays pending.
                    result = self._result_body(receipt_ref,
                        {"m0_prediction_ref": None, "m1_prediction_ref": None, "analogue_ref": None},
                        ["INVALID_SOURCE_ARTIFACT"], None, now)
                self.repository.register_artifact(ArtifactIndexEntryV2(identity_ref,
                    COMPARISON_RECEIPT_V1, sha256_json(result), now, now,
                    {"comparison_receipt": result}))
            self.repository.retire_due_work(COMPARISON_LANE_V1, item.work_id,
                reason_code="COMPARISON_SEALED" if result["status"] == "COMPLETE" else "NOT_ESTIMABLE_SEALED")
        return FrozenActionComparisonWorkResultV1(receipt_ref, result.get("comparison_ref"),
            identity_ref, result["status"])

    def _process(self, receipt_ref: str, *, now_ns: int) -> dict[str, Any]:
        missing: list[str] = []
        refs: dict[str, str | None] = {"m0_prediction_ref": None, "m1_prediction_ref": None,
            "analogue_ref": None}
        receipt, reason = _receipt_body(self.repository, receipt_ref, now_ns=now_ns)
        if receipt is None:
            missing.append(reason or "INVALID_ORIGINATING_RECEIPT")
            return self._result_body(receipt_ref, refs, missing, None, now_ns)
        event = receipt.event
        if (receipt.capital_enabled or receipt.assisted_enabled or receipt.agent_mode != "DISABLED"
                or receipt.action_ref is None or receipt.candidate_set_ref is None):
            missing.append("ORIGINATING_RECEIPT_AUTHORITY_OR_ACTION_INVALID")
            return self._result_body(receipt_ref, refs, missing, None, now_ns)
        action_entry, action_body, problem = _artifact(self.repository, receipt.action_ref,
            "ActionArtifactV2", now_ns=now_ns, deadline_ns=event.deadline_ns, body_key="action_artifact")
        if problem or action_entry is None or action_body is None:
            missing.append(problem or "MISSING_ACTION_ARTIFACT")
            return self._result_body(receipt_ref, refs, missing, None, now_ns)
        try:
            identity = action_entry.metadata.get("action_identity") if action_entry is not None else None
            if not isinstance(identity, Mapping):
                raise ValueError("missing exact frozen action identity")
            raw = dict(identity)
            raw.pop("version", None)
            from atlas.v2._serialization import decimal_value
            raw["key"] = InstrumentKeyV2.from_dict(json_value(raw["key"]))
            for field in ("quantity", "entry_reference", "entry_collar", "stop_price"):
                raw[field] = decimal_value(raw[field], field=field, wire=True)
            for field in ("entry_rule", "collar_rule", "management_rule", "time_exit_rule"):
                raw[field] = FrozenMap(raw[field])
            frozen = FrozenActionV2(**raw)
            art = dict(action_body)
            art.pop("version", None)
            art.pop("action_hash", None)
            action = ActionArtifactV2(frozen, art["candidate_ref"], art["sizing_ref"],
                art["candidate_set_ref"], art["available_at_ns"])
            if (action.content_hash != receipt.action_ref
                    or action.action.action_hash != action_body.get("action_hash")
                    or action.available_at_ns != action_entry.available_at_ns):
                raise ValueError("frozen action artifact does not reproduce")
        except (KeyError, TypeError, ValueError):
            missing.append("INVALID_ACTION_ARTIFACT")
            return self._result_body(receipt_ref, refs, missing, None, now_ns)
        candidate_entry = self.repository.get_artifact(action.candidate_ref)
        candidate_set_entry = self.repository.get_artifact(receipt.candidate_set_ref)
        try:
            candidate = CandidateActionV2.from_dict(json_value(candidate_entry.metadata["candidate"])) if candidate_entry else None
            candidate_set = CandidateSetV2.from_dict(json_value(candidate_set_entry.metadata["candidate_set"])) if candidate_set_entry else None
        except (KeyError, TypeError, ValueError):
            candidate = candidate_set = None
        if (candidate_entry is None or candidate_entry.artifact_type != "CandidateActionV2"
                or candidate_entry.content_hash != action.candidate_ref or candidate is None
                or candidate_set_entry is None or candidate_set_entry.artifact_type != "CandidateSetV2"
                or candidate_set_entry.content_hash != receipt.candidate_set_ref or candidate_set is None
                or candidate.envelope.available_at_ns != candidate_entry.available_at_ns
                or candidate_set.envelope.available_at_ns != candidate_set_entry.available_at_ns
                or candidate_entry.available_at_ns > now_ns
                or candidate_set_entry.available_at_ns > now_ns
                or candidate_entry.available_at_ns > event.deadline_ns
                or candidate_set_entry.available_at_ns > event.deadline_ns
                or candidate.decision_at_ns != event.information_cutoff_ns
                or candidate.content_hash != action.candidate_ref
                or candidate_set.content_hash != receipt.candidate_set_ref
                or action.candidate_set_ref != receipt.candidate_set_ref
                or candidate_set.selected_candidate_id != candidate.candidate_id):
            missing.append("INVALID_OR_MISMATCHED_FROZEN_CANDIDATE_BINDING")
            return self._result_body(receipt_ref, refs, missing, None, now_ns)
        stages = {stage.stage.value: stage for stage in receipt.result.stages}
        evaluation_stage = stages.get("ECONOMIC_EVALUATION")
        m1_stage = stages.get("M1_DIAGNOSTIC")
        analogue_stage = stages.get("ANALOGUE_DIAGNOSTIC")
        evaluation_ref = (evaluation_stage.artifact_refs[0]
            if evaluation_stage is not None and evaluation_stage.artifact_refs else None)
        m1_refs = m1_stage.artifact_refs if m1_stage is not None else ()
        analogue_refs = analogue_stage.artifact_refs if analogue_stage is not None else ()
        evaluation: AmendedEvaluationArtifactV2 | None = None
        if evaluation_ref:
            _, eval_body, eval_problem = _artifact(self.repository, evaluation_ref, "EvaluationArtifactV2",
                now_ns=now_ns, deadline_ns=event.deadline_ns, body_key="evaluation")
            if eval_problem or eval_body is None:
                missing.append(eval_problem or "MISSING_ECONOMIC_EVALUATION")
            else:
                try:
                    evaluation = AmendedEvaluationArtifactV2.from_dict(json_value(eval_body))
                    if (evaluation.content_hash != evaluation_ref or evaluation.action_hash != action.action.action_hash
                            or evaluation.action_artifact_ref != action.content_hash
                            or evaluation.candidate_ref != candidate.content_hash
                            or evaluation.candidate_set_ref != candidate_set.content_hash
                            or evaluation.decision_at_ns != event.information_cutoff_ns):
                        evaluation = None
                        missing.append("MISMATCHED_ECONOMIC_EVALUATION")
                except (KeyError, TypeError, ValueError):
                    missing.append("INVALID_ECONOMIC_EVALUATION")
        else:
            missing.append("MISSING_ECONOMIC_EVALUATION_REF")
        m0 = None
        if evaluation is not None:
            refs["m0_prediction_ref"] = evaluation.m0_prediction_ref
            m0_entry, m0_body, m0_problem = _artifact(self.repository, evaluation.m0_prediction_ref,
                "M0PredictionV2", now_ns=now_ns, deadline_ns=event.deadline_ns, body_key="prediction")
            if m0_problem or m0_entry is None or m0_body is None:
                missing.append(m0_problem or "MISSING_M0_PREDICTION")
            else:
                try:
                    m0 = M0PredictionV2.from_dict(json_value(m0_body))
                    if (m0.content_hash != evaluation.m0_prediction_ref or m0.action_hash != action.action.action_hash
                            or m0.action_artifact_ref != action.content_hash
                            or m0.training_cutoff_ns != event.information_cutoff_ns
                            or m0.available_at_ns != m0_entry.available_at_ns):
                        m0 = None
                        missing.append("MISMATCHED_M0_PREDICTION")
                except (KeyError, TypeError, ValueError):
                    missing.append("INVALID_M0_PREDICTION")
        else:
            missing.append("MISSING_M0_PREDICTION_REF")
        m1 = None
        if m1_refs:
            refs["m1_prediction_ref"] = m1_refs[0]
            m1, m1_problem = _load_m1(self.repository, m1_refs[0], now_ns=now_ns,
                deadline_ns=event.deadline_ns, expected_action_hash=action.action.action_hash,
                expected_action_ref=action.content_hash, expected_candidate_ref=candidate.content_hash,
                expected_candidate_set_ref=candidate_set.content_hash, cutoff_ns=event.information_cutoff_ns)
            if m1_problem:
                missing.append(m1_problem)
        else:
            missing.append("MISSING_M1_PREDICTION_REF")
        analogue = None
        analogue_ref = None
        for ref in analogue_refs:
            analogue_entry = self.repository.get_artifact(ref)
            if analogue_entry is not None and analogue_entry.artifact_type == "AnalogueActionValueV2":
                analogue_ref = ref
                break
        if analogue_ref is not None:
            refs["analogue_ref"] = analogue_ref
            analogue, analogue_problem = _load_analogue(self.repository, analogue_ref, now_ns=now_ns,
                deadline_ns=event.deadline_ns, expected_action_hash=action.action.action_hash,
                expected_action_ref=action.content_hash, expected_candidate_ref=candidate.content_hash,
                expected_candidate_set_ref=candidate_set.content_hash, cutoff_ns=event.information_cutoff_ns)
            if analogue_problem:
                missing.append(analogue_problem)
        else:
            missing.append("MISSING_ANALOGUE_ACTION_VALUE_REF")
        if missing:
            return self._result_body(receipt_ref, refs, sorted(set(missing)), None, now_ns)
        if m0 is None or m1 is None or analogue is None:
            return self._result_body(receipt_ref, refs, ["FROZEN_COMPARISON_INPUT_MISSING"], None, now_ns)
        try:
            comparison = compare_frozen_action(m0=m0, m1=m1, analogue=analogue)
            body = comparison.to_dict()
            self.repository.register_artifact(ArtifactIndexEntryV2(comparison.content_hash, COMPARISON_VERSION,
                comparison.content_hash, now_ns, now_ns, {"comparison": body}))
            return self._result_body(receipt_ref, refs, [], comparison.content_hash, now_ns)
        except (ValueError, TypeError, KeyError, ArithmeticError):
            return self._result_body(receipt_ref, refs, ["COMPARISON_INPUT_CONTRACT_INVALID"], None, now_ns)

    @staticmethod
    def _result_body(receipt_ref: str, refs: Mapping[str, str | None], missing: list[str],
                     comparison_ref: str | None, now_ns: int) -> dict[str, Any]:
        return {"version": COMPARISON_RECEIPT_V1, "originating_receipt_ref": receipt_ref,
            "comparison_ref": comparison_ref, "source_refs": dict(refs),
            "status": "COMPLETE" if comparison_ref is not None else "NOT_ESTIMABLE",
            "reason_codes": sorted(set(missing)), "available_at_ns": now_ns,
            "authority": "ZERO", "model_voting": False, "model_fusion": False,
            "admission_influence": False, "action_influence": False}
