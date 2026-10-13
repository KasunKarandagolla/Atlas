"""Thin adapter around the frozen Phase-4 evaluation result."""

from __future__ import annotations

from collections.abc import Callable

from atlas.science.phase4_engine import (
    Phase4DecisionInput,
    Phase4Evaluation,
    canonical_hash,
    evaluate_phase4_historical,
    evaluate_phase4_qualified,
)
from atlas.strategy.source_proof import ParquetFeatureSourceStore

from .models import DecisionStatus, Phase4HandoffRequest, Phase4ScannerResult


def result_from_evaluation(evaluation: Phase4Evaluation, *, slot_at_ns: int,
                           instrument: str) -> Phase4ScannerResult:
    """Project an immutable Phase-4 evaluation into scanner-facing fields.

    No Phase-4 calculation is repeated here; the TradePlan is the same object
    produced by the existing evaluator.
    """
    evidence = evaluation.evidence
    reasons = tuple(dict.fromkeys(filter(None, (evaluation.reason, *evidence.rejection_reasons))))
    return Phase4ScannerResult(
        slot_at_ns=slot_at_ns,
        instrument=instrument,
        status=evaluation.status,
        evaluation_ref=evidence.artifact_hash(),
        trade_plan=evaluation.trade_plan,
        reasons=reasons,
        not_estimable_reasons=evidence.not_estimable_reasons,
        evidence_hash=evidence.artifact_hash(),
    )


def not_applicable_result(*, request: Phase4HandoffRequest, reason: str) -> Phase4ScannerResult:
    reference = canonical_hash({"phase4": "NOT_APPLICABLE", "slot": request.slot_at_ns,
                                "instrument": request.instrument, "reason": reason})
    return Phase4ScannerResult(request.slot_at_ns, request.instrument, DecisionStatus.SKIP_DATA, reference,
                               None, (reason,), (), reference)


def evaluator_from_phase4(
    build_input: Callable[[Phase4HandoffRequest], Phase4DecisionInput],
    *,
    source_store: ParquetFeatureSourceStore | Callable[[Phase4HandoffRequest], ParquetFeatureSourceStore | None] | None = None,
) -> Callable[[Phase4HandoffRequest], Phase4ScannerResult]:
    """Bind new scanner decisions to the source-verified qualified evaluator.

    Omitting the configured archive fails closed with NOT_ESTIMABLE.
    """

    def evaluate(request: Phase4HandoffRequest) -> Phase4ScannerResult:
        value = build_input(request)
        store = source_store(request) if callable(source_store) else source_store
        evaluation = evaluate_phase4_qualified(
            value,
            store,
            expected_instrument=request.instrument,
            expected_slot_at_ns=request.slot_at_ns,
            expected_cutoff_ns=request.availability_cutoff_ns,
        )
        return result_from_evaluation(evaluation, slot_at_ns=request.slot_at_ns, instrument=request.instrument)

    return evaluate


def historical_evaluator_from_phase4(
    build_input: Callable[[Phase4HandoffRequest], Phase4DecisionInput],
) -> Callable[[Phase4HandoffRequest], Phase4ScannerResult]:
    """Bind diagnostic replay to the pre-receipt V1 Phase-4 interpretation."""

    def evaluate(request: Phase4HandoffRequest) -> Phase4ScannerResult:
        evaluation = evaluate_phase4_historical(build_input(request))
        return result_from_evaluation(evaluation, slot_at_ns=request.slot_at_ns, instrument=request.instrument)

    return evaluate
