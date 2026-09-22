"""Small deterministic builders shared by the Phase-4 test modules."""

from __future__ import annotations

from decimal import Decimal

from atlas.domain.enums import Side
from atlas.domain.risk import RiskPolicy, engineering_default_policy
from atlas.risk.engine import AccountState, RiskVector
from atlas.risk.sizing import PerUnitRisk
from atlas.science.execution_replay import ReplayMinute
from atlas.science.gates import EventGateInput, MarketGateInput
from atlas.science.phase4_engine import Phase4DecisionInput, ScenarioSupport
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


def replay_minutes(prices: tuple[Decimal, ...], *, start_ns: int = 0, step_ns: int = 60_000_000_000,
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


def bootstrap(*, lcb: float = 1.0, unstable: bool = False) -> OuterBootstrapResult:
    return OuterBootstrapResult((lcb,) * 10, lcb, unstable, "NO_TRADE_NUMERICAL" if unstable else "ESTIMATED", 1, 2, 3)


def shared(minutes: tuple[ReplayMinute, ...]) -> tuple[ReplayMinute, ...]:
    return minutes


def account(equity: Decimal = Decimal("100000")) -> AccountState:
    return AccountState(equity, equity, Decimal("0"), Decimal("0"), Decimal("0"))


def risk_policy() -> RiskPolicy:
    return engineering_default_policy()


def risk_vector(*, normal: Decimal = Decimal("10"), stress: Decimal = Decimal("20"),
                notional: Decimal = Decimal("100"), es: Decimal = Decimal("0")) -> RiskVector:
    return RiskVector(normal, stress, notional, notional, notional / Decimal("2"), es)


def per_unit(*, normal: Decimal = Decimal("2"), stress: Decimal = Decimal("4"),
             mark: Decimal = Decimal("100"), margin: Decimal = Decimal("20")) -> PerUnitRisk:
    return PerUnitRisk(normal, stress, mark, mark, margin, Decimal("0"), Decimal("10"))


def market_inputs(*, quantity: Decimal = Decimal("1"), now_ns: int = SLOT + 1_000_000,
                  mark: Decimal = Decimal("100")) -> MarketGateInput:
    bid, ask = mark - Decimal("0.01"), mark + Decimal("0.01")
    return MarketGateInput(now_ns, now_ns - 1_000_000, now_ns - 1_000_000, now_ns - 1_000_000, bid, ask, mark, mark,
                           Decimal("100"), quantity, Decimal("0.0001"), True, True, True, True)


def event_inputs() -> EventGateInput:
    return EventGateInput(True, ())


def decision_input(**overrides: object) -> Phase4DecisionInput:
    base: dict[str, object] = {
        "now_ns": SLOT + 1_000_000,
        "snapshot": signal_snapshot(Signal.LONG),
        "policy": policy(),
        "risk_policy": risk_policy(),
        "account": account(),
        "pending_reservations": (),
        "market_inputs": market_inputs(),
        "event_inputs": event_inputs(),
        "scenario_support": ScenarioSupport(True, (), 8, 24, 1),
        "bootstrap": bootstrap(),
        "pi0_path_pnl": (0.0, 0.0),
        "candidate_path_pnl": (2.0, 0.5),
        "per_unit_risk": per_unit(),
        "venue_maximum_quantity": Decimal("5"),
        "lot": LOT,
        "minimum_quantity": Decimal("0.01"),
        "leverage": Decimal("1"),
        "model_manifest_hash": "m",
        "block_manifest_hash": "b",
        "scenario_config_hash": "s",
        "cost_evidence_ref": "cost-1",
        "account_scope": "OFFLINE_RESEARCH",
        "plan_id": "plan-1",
        "availability_cutoff_ns": SLOT + SNAPSHOT_DEADLINE_NS,
        "stress_input": complete_stress_input(),
    }
    base.update(overrides)
    return Phase4DecisionInput(**base)  # type: ignore[arg-type]
