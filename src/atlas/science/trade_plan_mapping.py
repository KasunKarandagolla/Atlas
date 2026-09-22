"""Pure mapping of a qualified offline candidate to the existing immutable TradePlan."""

from __future__ import annotations

from decimal import Decimal

from atlas.domain.enums import Side
from atlas.domain.trade_plan import TradePlan
from atlas.strategy.crypto_trend_24h_v1 import STRATEGY_VERSION, FeatureSnapshot, Signal
from atlas.strategy.policy import FixedPolicy


def candidate_trade_plan(*, plan_id: str, snapshot: FeatureSnapshot, policy: FixedPolicy, policy_hash: str,
                         normal_risk: Decimal, stress_risk: Decimal, margin: Decimal, leverage: Decimal,
                         cost_evidence_ref: str, account_scope: str, quantity: Decimal | None = None,
                         market: str = "BYBIT_LINEAR_USDT") -> TradePlan:
    if snapshot.signal is Signal.FLAT:
        raise ValueError("flat signal creates no TradePlan")
    expected_side = Side.LONG if snapshot.signal is Signal.LONG else Side.SHORT
    if policy.side is not expected_side or policy.decision_slot_at_ns != snapshot.slot_at_ns:
        raise ValueError("candidate policy not bound to frozen snapshot direction/slot")
    sized = policy.quantity if quantity is None else quantity
    if sized <= 0 or sized > policy.quantity:
        raise ValueError("sized quantity must be positive and bounded by the frozen policy quantity")
    return TradePlan(
        plan_id=plan_id, version=f"CRYPTO_TREND_24H_V1:{STRATEGY_VERSION}", policy_hash=policy_hash,
        snapshot_hash=snapshot.snapshot_hash(), expires_at_ns=snapshot.expires_at_ns, market=market,
        account_scope=account_scope, instrument=snapshot.instrument, side=policy.side, qty_limit=sized,
        entry_policy=policy.entry_policy, collar=policy.entry_collar, stop=policy.stop,
        stop_trigger_basis=policy.stop_trigger_basis, management_policy=policy.management_policy,
        horizon_end_ns=snapshot.horizon_end_ns, cost_distribution_ref=cost_evidence_ref,
        normal_risk=normal_risk, stress_risk=stress_risk, margin=margin, leverage_bound=leverage,
        risk_config_hash=policy_hash, created_at_ns=policy.created_at_ns, available_at_ns=policy.created_at_ns,
        reference_price=policy.mark_reference,
    )
