"""Small deterministic builders shared by the Phase-4 test modules."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from atlas.domain.enums import Side
from atlas.domain.risk import RiskPolicy, engineering_default_policy
from atlas.risk.engine import AccountState, RiskVector
from atlas.science.execution_replay import MINUTE_NS, ReplayMinute
from atlas.science.gates import EventGateInput, MarketGateInput
from atlas.science.phase4_engine import (
    CandidateRiskInputs,
    Phase4DecisionInput,
    Phase4ScenarioEvaluation,
    ScenarioSupport,
    candidate_risk_inputs_hash,
    path_hash,
    phase4_action_hash,
    seed_identity,
)
from atlas.science.stresses import (
    FROZEN_STRESSES,
    StressCollateralAssumptions,
    StressExecutionAssumptions,
    StressFundingAssumptions,
    StressInput,
    StressMarginAssumptions,
    StressName,
)
from atlas.science.trade_plan_mapping import candidate_trade_plan  # noqa: F401  (re-exported for tests)
from atlas.science.uncertainty import OuterBootstrapResult
from atlas.strategy.crypto_trend_24h_v1 import SNAPSHOT_DEADLINE_NS, FeatureSnapshot, Signal
from atlas.strategy.features import FeatureValues
from atlas.strategy.policy import FixedPolicy, VenueFilters, fixed_policy

SLOT = 4 * 3_600_000_000_000
TICK = Decimal("0.1")
LOT = Decimal("0.01")
MINUTE_NS = MINUTE_NS


def filters() -> VenueFilters:
    return VenueFilters(TICK, LOT, Decimal("0.01"))


def snapshot(*, instrument: str = "BTCUSDT", slot_at_ns: int = SLOT, z: float = 1.0,
             sigma: float = 0.01, computed_at_ns: int | None = None) -> FeatureSnapshot:
    values = FeatureValues(end_at_ns=slot_at_ns, returns=tuple([0.0] * 720), variance=sigma**2, sigma=sigma, z=z)
    return FeatureSnapshot(instrument, slot_at_ns, computed_at_ns or slot_at_ns + 1_000_000, values, ("r1", "r2"))


def signal_snapshot(signal: Signal, *, instrument: str = "BTCUSDT", slot_at_ns: int = SLOT) -> FeatureSnapshot:
    z = {Signal.LONG: 2.0, Signal.SHORT: -2.0, Signal.FLAT: 0.0}[signal]
    return snapshot(instrument=instrument, slot_at_ns=slot_at_ns, z=z)


def policy(*, side: Side = Side.LONG, quantity: Decimal = Decimal("1"), mark: Decimal = Decimal("100"),
           sigma: float = 0.01, slot_at_ns: int = SLOT) -> FixedPolicy:
    return fixed_policy(side, quantity, mark - Decimal("0.1"), mark + Decimal("0.1"), mark, sigma, filters(),
                        slot_at_ns, slot_at_ns)


def replay_minutes(prices: tuple[Decimal, ...], *, start_ns: int = 0, step_ns: int = MINUTE_NS,
                   depth: Decimal = Decimal("100")) -> tuple[ReplayMinute, ...]:
    minutes = []
    for index, price in enumerate(prices):
        at_ns = start_ns + index * step_ns
        minutes.append(ReplayMinute(at_ns, price - Decimal("0.1"), price + Decimal("0.1"), depth, depth,
                                    price, price, price, price))
    return tuple(minutes)


def complete_stress_input(*, side: Side = Side.LONG, quantity: Decimal = Decimal("1"),
                          mark: Decimal = Decimal("100")) -> StressInput:
    """A fully-evidenced stress suite (no invented venue mechanics)."""
    prices = {
        StressName.JUMP_5_LIQUIDITY: (mark * Decimal("0.95"), mark * Decimal("0.94")),
        StressName.JUMP_10_LIQUIDITY: (mark * Decimal("0.90"), mark * Decimal("0.89")),
        StressName.MARK_LAST_DIVERGENCE_1: (mark * Decimal("0.99"),),
        StressName.SPREAD_10X: (mark * Decimal("0.995"),),
        StressName.DEPTH_MINUS_90: (mark * Decimal("0.99"),),
        StressName.BTC_ETH_CORRELATED_SHOCK: (mark * Decimal("0.90"),),
    }
    depths = {
        StressName.JUMP_5_LIQUIDITY: (quantity, quantity),
        StressName.JUMP_10_LIQUIDITY: (quantity, quantity),
        StressName.DEPTH_MINUS_90: (quantity,),
        StressName.BTC_ETH_CORRELATED_SHOCK: (quantity,),
    }
    execution = StressExecutionAssumptions(
        stressed_executable_prices=prices,
        stressed_available_depth=depths,
        stressed_spreads={StressName.SPREAD_10X: mark * Decimal("0.005"),
                          StressName.JUMP_5_LIQUIDITY: mark * Decimal("0.005"),
                          StressName.JUMP_10_LIQUIDITY: mark * Decimal("0.005"),
                          StressName.BTC_ETH_CORRELATED_SHOCK: mark * Decimal("0.005")},
        exit_delay_path_prices={StressName.NO_EXIT_60_SECONDS: (mark * Decimal("0.97"),),
                                StressName.NO_EXIT_15_MINUTES: (mark * Decimal("0.92"),)},
        mark_last_divergences={StressName.MARK_LAST_DIVERGENCE_1: Decimal("0.01")},
        correlation_values={StressName.BTC_ETH_CORRELATED_SHOCK: Decimal("1")},
        paired_instrument_paths={StressName.BTC_ETH_CORRELATED_SHOCK: {
            "BTCUSDT": (mark * Decimal("0.90"),), "ETHUSDT": (mark * Decimal("0.90"),)}},
    )
    margin = StressMarginAssumptions(
        maintenance_margin_tiers={case.name: (Decimal("100"),) for case in FROZEN_STRESSES},
        liquidation_thresholds={case.name: Decimal("1000") for case in FROZEN_STRESSES},
        liquidation_costs={case.name: Decimal("5") for case in FROZEN_STRESSES},
        liquidation_mechanics={case.name: "supplied-tier-schedule-v1" for case in FROZEN_STRESSES},
    )
    return StressInput(side, quantity, mark, 0, execution, margin,
                       StressFundingAssumptions(Decimal("0.0001"), Decimal("0.0003"), mark * quantity),
                       StressCollateralAssumptions(Decimal("500")))


def bootstrap(*, lcb: float = 1.0, unstable: bool = False, action_hash: str = "") -> OuterBootstrapResult:
    return OuterBootstrapResult((lcb,) * 10, lcb, unstable, "NO_TRADE_NUMERICAL" if unstable else "ESTIMATED",
                                1, 2, 3, action_hash)


def candidate_risk_inputs(*, mark: Decimal = Decimal("100"), beta: Decimal = Decimal("1"),
                          venue_collateral: Decimal = Decimal("10")) -> CandidateRiskInputs:
    return CandidateRiskInputs(beta=beta, taker_fee_rate=Decimal("0.0005"),
                               maintenance_margin_per_unit=mark / Decimal("5"),
                               venue_collateral=venue_collateral)


def account(equity: Decimal = Decimal("100000")) -> AccountState:
    return AccountState(equity, equity, Decimal("0"), Decimal("0"), Decimal("0"))


def risk_policy() -> RiskPolicy:
    return engineering_default_policy()


def risk_vector(*, normal: Decimal = Decimal("10"), stress: Decimal = Decimal("20"),
                notional: Decimal = Decimal("100"), es: Decimal = Decimal("0")) -> RiskVector:
    return RiskVector(normal, stress, notional, notional, notional / Decimal("2"), es)


def market_inputs(*, quantity: Decimal = Decimal("1"), now_ns: int = SLOT + 1_000_000,
                  mark: Decimal = Decimal("100")) -> MarketGateInput:
    # Two basis points of relative spread keeps the market gate meaningful for
    # instruments at any price level.
    bid, ask = mark * Decimal("0.9999"), mark * Decimal("1.0001")
    return MarketGateInput(now_ns, now_ns - 1_000_000, now_ns - 1_000_000, now_ns - 1_000_000, bid, ask, mark, mark,
                           Decimal("100"), quantity, Decimal("0.0001"), True, True, True, True)


def event_inputs() -> EventGateInput:
    return EventGateInput(True, ())


def scenario_evaluation(*, snapshot: FeatureSnapshot, policy: FixedPolicy, risk_policy_hash: str,
                        quantity: Decimal, model_manifest_hash: str = "m", block_manifest_hash: str = "b",
                        scenario_config_hash: str = "s", support: ScenarioSupport | None = None,
                        bootstrap_result: OuterBootstrapResult | None = None,
                        pi0_path_pnl: tuple[float, ...] = (0.0, 0.0),
                        candidate_path_pnl: tuple[float, ...] = (2.0, 0.5),
                        stress_template: StressInput | None = None,
                        action_hash: str | None = None,
                        risk_inputs_hash: str | None = None) -> Phase4ScenarioEvaluation:
    support = support or ScenarioSupport(True, (), 8, 24, 1)
    resolved_action = action_hash if action_hash is not None else phase4_action_hash(policy, snapshot, quantity)
    template = stress_template if stress_template is not None else complete_stress_input(
        side=policy.side, quantity=quantity, mark=policy.mark_reference)
    resolved_bootstrap = bootstrap_result if bootstrap_result is not None else bootstrap(action_hash=resolved_action)
    if not resolved_bootstrap.action_hash:
        resolved_bootstrap = replace(resolved_bootstrap, action_hash=resolved_action)
    return Phase4ScenarioEvaluation(
        snapshot_hash=snapshot.snapshot_hash(), action_hash=resolved_action, quantity=quantity,
        risk_policy_hash=risk_policy_hash, model_manifest_hash=model_manifest_hash,
        block_manifest_hash=block_manifest_hash, scenario_config_hash=scenario_config_hash,
        seed_identity=seed_identity(support), candidate_path_hash=path_hash(candidate_path_pnl),
        portfolio_path_hash=path_hash(pi0_path_pnl),
        risk_inputs_hash=risk_inputs_hash if risk_inputs_hash is not None else candidate_risk_inputs_hash(
            candidate_risk_inputs(mark=policy.mark_reference)),
        bootstrap=resolved_bootstrap,
        pi0_path_pnl=pi0_path_pnl, candidate_path_pnl=candidate_path_pnl, scenario_support=support,
        stress_template=template)


def decision_input(**overrides: object) -> Phase4DecisionInput:
    snapshot_value: FeatureSnapshot | None = overrides.pop("snapshot", signal_snapshot(Signal.LONG))  # type: ignore[assignment]
    policy_value: FixedPolicy | None = overrides.pop("policy", policy())  # type: ignore[assignment]
    risk_policy_value: RiskPolicy = overrides.pop("risk_policy", risk_policy())  # type: ignore[assignment]
    account_value = overrides.pop("account", account())
    market_value = overrides.pop("market_inputs", market_inputs())
    event_value = overrides.pop("event_inputs", event_inputs())
    default_mark = policy_value.mark_reference if policy_value is not None else Decimal("100")
    risk_inputs_value: CandidateRiskInputs = overrides.pop("risk_inputs", candidate_risk_inputs(mark=default_mark))  # type: ignore[assignment]
    venue_maximum: Decimal = overrides.pop("venue_maximum_quantity", Decimal("10"))  # type: ignore[assignment]
    model_hash: str = overrides.pop("model_manifest_hash", "m")  # type: ignore[assignment]
    block_hash: str = overrides.pop("block_manifest_hash", "b")  # type: ignore[assignment]
    scenario_hash: str = overrides.pop("scenario_config_hash", "s")  # type: ignore[assignment]
    scenario_value = overrides.pop("scenario", "AUTO")
    if scenario_value == "AUTO" and snapshot_value is not None and policy_value is not None:
        quantity: Decimal = overrides.pop("quantity", None) or min(venue_maximum, policy_value.quantity)  # type: ignore[assignment]
        scenario_value = scenario_evaluation(
            snapshot=snapshot_value, policy=policy_value, risk_policy_hash=risk_policy_value.policy_hash(),
            quantity=quantity, model_manifest_hash=model_hash, block_manifest_hash=block_hash,
            scenario_config_hash=scenario_hash,
            support=overrides.pop("scenario_support", None),  # type: ignore[arg-type]
            bootstrap_result=overrides.pop("bootstrap", None),  # type: ignore[arg-type]
            pi0_path_pnl=overrides.pop("pi0_path_pnl", (0.0, 0.0)),  # type: ignore[arg-type]
            candidate_path_pnl=overrides.pop("candidate_path_pnl", (2.0, 0.5)),  # type: ignore[arg-type]
            stress_template=overrides.pop("stress_template", None),  # type: ignore[arg-type]
            action_hash=overrides.pop("action_hash", None),  # type: ignore[arg-type]
            risk_inputs_hash=candidate_risk_inputs_hash(risk_inputs_value))
    base: dict[str, object] = {
        "now_ns": SLOT + 1_000_000,
        "snapshot": snapshot_value,
        "policy": policy_value,
        "risk_policy": risk_policy_value,
        "account": account_value,
        "pending_reservations": (),
        "market_inputs": market_value,
        "event_inputs": event_value,
        "scenario": scenario_value if scenario_value != "AUTO" else None,
        "risk_inputs": risk_inputs_value,
        "model_manifest_hash": model_hash,
        "block_manifest_hash": block_hash,
        "scenario_config_hash": scenario_hash,
        "venue_maximum_quantity": venue_maximum,
        "lot": LOT,
        "minimum_quantity": Decimal("0.01"),
        "leverage": Decimal("1"),
        "cost_evidence_ref": "cost-1",
        "account_scope": "OFFLINE_RESEARCH",
        "plan_id": "plan-1",
        "availability_cutoff_ns": SLOT + SNAPSHOT_DEADLINE_NS,
    }
    base.update(overrides)
    return Phase4DecisionInput(**base)  # type: ignore[arg-type]
