"""Bridge genuine existing native S1/S2 candidates to research evidence.

This is not another strategy, exchange client, or account. Production predicates,
watches, source-health joins and native deadlines remain the original owners.
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from ...contracts import CandidateActionV2
from ...instruments import ProductContractV2
from ...strategies.s1_trend import S1_POLICY
from ...strategies.s2_breakout import S2_POLICY
from .actions import ResearchActionV1, seal_research_action
from .causality import CausalFact, EvidenceGrade


def seal_native_candidate(
    candidate: CandidateActionV2,
    *,
    product: ProductContractV2,
    facts: Sequence[CausalFact],
    setup_at_ns: int,
    sealed_at_ns: int,
    atr_value: Decimal,
    spread_bps: Decimal,
    cycle_id: str,
    range_low: Decimal | None = None,
    range_high: Decimal | None = None,
) -> ResearchActionV1:
    """Preserve the native policy/trigger/collar/stop; size only a virtual research lot."""
    policy = next((p for p in (S1_POLICY, S2_POLICY) if p.policy_hash == candidate.policy_hash), None)
    if policy is None:
        raise ValueError("EXACT_NATIVE_POLICY_REQUIRED")
    if product.base_units_per_contract != 1:
        raise ValueError("UNSUPPORTED_RESEARCH_CONTRACT_MULTIPLIER")
    if product.key != candidate.key or product.available_at_ns > candidate.decision_at_ns:
        raise ValueError("POINT_IN_TIME_PRODUCT_REVISION_MISMATCH")
    if candidate.deadline_ns != candidate.decision_at_ns + 5_000_000_000:
        raise ValueError("ORIGINAL_NATIVE_DEADLINE_MISMATCH")
    if candidate.horizon_end_ns != candidate.decision_at_ns + policy.max_hold_ns:
        raise ValueError("ORIGINAL_NATIVE_HORIZON_MISMATCH")
    strategy = "S1" if policy == S1_POLICY else "S2"
    plan = {
        "native_candidate_hash": candidate.content_hash,
        "feature_hash": candidate.snapshot_hash,
        "collar_price": str(candidate.entry_collar),
        "source_refs": list(candidate.envelope.input_refs),
    }
    if strategy == "S2":
        if range_low is None or range_high is None:
            raise ValueError("S2_FROZEN_RANGE_REQUIRED")
        expected = range_low if candidate.side.value == "LONG" else range_high
        if candidate.stop_price != expected:
            raise ValueError("S2_NATIVE_STOP_MISMATCH")
        plan.update(range_low=str(range_low), range_high=str(range_high))
    return seal_research_action(
        cycle_id=cycle_id,
        symbol=candidate.key.native_symbol,
        strategy=strategy,
        policy_id=policy.policy_id,
        policy_hash=policy.policy_hash,
        side=candidate.side.value,
        entry_reference=candidate.entry_reference,
        stop_price=candidate.stop_price,
        stop_basis=policy.trigger_basis,
        qty_step=product.qty_step,
        tick_size=product.tick_size,
        min_notional=product.min_notional,
        min_qty=product.min_qty,
        max_qty=product.max_qty,
        spread_bps=spread_bps,
        atr_value=atr_value,
        max_hold_ns=policy.max_hold_ns,
        decision_at_ns=candidate.decision_at_ns,
        deadline_offset_ns=5_000_000_000,
        setup_at_ns=setup_at_ns,
        trigger_at_ns=candidate.decision_at_ns,
        sealed_at_ns=sealed_at_ns,
        instrument_revision=candidate.key.contract_revision,
        evidence_grade=EvidenceGrade.PROSPECTIVE,
        facts=facts,
        evidence_refs=(candidate.content_hash,),
        opportunity_id=candidate.candidate_id,
        execution_plan=plan,
    )


def persist_modeled_outcomes(
    store,
    action: ResearchActionV1,
    *,
    future_bars,
    spread_bps: Decimal,
    evaluated_at_ns: int,
    funding_settlements=(),
    funding_coverage=None,
    filled_quantity: Decimal | None = None,
) -> list[dict]:
    """One original action, versioned paths, availability-censored ML targets."""
    import hashlib
    import json

    from .actions import ACTIONS_VERSION, evaluate_lifecycle

    store.record_action(action.as_row())
    paths = evaluate_lifecycle(
        action,
        future_bars=future_bars,
        spread_bps=spread_bps,
        evaluated_at_ns=evaluated_at_ns,
        funding_settlements=funding_settlements,
        funding_coverage=funding_coverage,
        filled_quantity=filled_quantity,
    )
    records = []
    for outcome in paths:
        body = {
            "action_id": action.action_id,
            "management_kind": outcome.management_kind,
            "path_id": outcome.path_id,
            "entry_state": outcome.entry_state,
            "exit_state": outcome.exit_state,
            "exit_reason": outcome.exit_reason,
            "gross_cash": str(outcome.gross_cash),
            "cost_cash": str(outcome.cost_cash),
            "net_cash": str(outcome.net_cash),
            "horizon_end_ns": outcome.horizon_end_ns,
            "uncertainty": outcome.uncertainty,
            "detail": dict(outcome.detail),
        }
        body["detail"]["future_source_refs"] = [
            b.content_hash for b in future_bars if b.raw.available_at_ns <= evaluated_at_ns
        ]
        identity = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
        body["lifecycle_id"] = identity
        store.record_lifecycle(body)
        records.append(body)
    hold = next((r for r in records if r["management_kind"] == "HOLD"), None)
    # OHLC executable bounds do not define a point-valued qualified training label.
    qualified = bool(
        hold
        and hold["exit_state"] == "CLOSED"
        and "UNOBSERVABLE" not in hold["uncertainty"]
        and hold["detail"]["label_available_at_ns"] <= evaluated_at_ns
    )
    store.record_outcome(
        {
            "action_id": action.action_id,
            "cycle_id": action.cycle_id,
            "symbol": action.symbol,
            "strategy": action.strategy,
            "horizon_ns": action.horizon_end_ns,
            "maturity_status": "MATURED" if qualified else "CENSORED",
            "net_cash": hold["net_cash"] if qualified else None,
            "return_bps": None,
            "max_favorable_bps": None,
            "max_adverse_bps": None,
            "censoring_reason": None if qualified else "EXECUTION_OR_FUNDING_NOT_POINT_IDENTIFIED",
            "evaluated_at_ns": evaluated_at_ns,
            "evaluation_version": ACTIONS_VERSION,
            "evidence_grade": str(EvidenceGrade.MODELED),
            "label_available_at_ns": hold["detail"]["label_available_at_ns"] if qualified else None,
            "lifecycle_id": hold["lifecycle_id"] if hold else "",
        }
    )
    return records


def persist_native_feature_context(store, action: ResearchActionV1, feature) -> None:
    """Project only originally constructed, exact action-bound feature values."""
    if (
        str(feature.replay_view) != "ACTUAL_SYSTEM"
        or feature.content_hash != action.execution_plan.get("feature_hash")
        or feature.key.contract_revision != action.instrument_revision
        or feature.information_cutoff_ns != action.decision_at_ns
        or feature.envelope.available_at_ns > action.sealed_at_ns
    ):
        raise ValueError("NATIVE_FEATURE_TIME_OR_ACTION_BINDING_MISMATCH")

    def number(name):
        value = feature.values.get(name)
        return None if value is None or value.value is None else Decimal(str(value.value))

    atr = number("m15.atr14")
    context = {
        "cycle_id": "NATIVE-FEATURE-" + feature.content_hash,
        "symbol": action.symbol,
        "horizon": "M15",
        "bars_used": 1,
        "atr14_bps": str(atr / action.entry_reference * 10000) if atr is not None else None,
        "computed_at_ns": feature.envelope.created_at_ns,
        "available_at_ns": feature.envelope.available_at_ns,
        "information_cutoff_ns": action.decision_at_ns,
        "instrument_revision": action.instrument_revision,
        "source_grade": str(EvidenceGrade.PROSPECTIVE),
        "feature_ref": feature.content_hash,
        "evidence_refs": list(feature.envelope.input_refs),
    }
    store.record_context([context])
