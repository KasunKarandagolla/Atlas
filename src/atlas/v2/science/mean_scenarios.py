"""Exact-action IOC/stop/time replay on qualified common offline market paths.

Templates retain market/liquidity dependence. They are neither actual fills nor
probability evidence merely because a test fixture supplied probabilities.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_FLOOR, Decimal
from typing import ClassVar

from atlas.v2.science.action import ExactActionV1
from atlas.v2.science.costs import MeanCashFillV1, MeanCashPathV1, MeanFundingPaymentV1
from atlas.v2.science.mean_common import ZERO, OfflineArtifact, nonnegative, positive, unique


@dataclass(frozen=True)
class MeanMarketPointV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_COMMON_EXECUTION_POINT_V1"
    at_ns: int
    mark: Decimal
    bid: Decimal
    ask: Decimal
    bid_quantity: Decimal
    ask_quantity: Decimal
    source_ref: str
    closed_m15_price: Decimal | None = None

    def __post_init__(self) -> None:
        super().__post_init__()
        positive(self.mark, self.bid, self.ask)
        nonnegative(self.bid_quantity, self.ask_quantity)
        if self.bid > self.ask:
            raise ValueError("crossed execution quotes")


@dataclass(frozen=True)
class MeanJointPathV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_COMMON_MARKET_PATH_V1"
    common_path_id: str
    probability: Decimal
    background_pnl_cash: Decimal
    points: tuple[MeanMarketPointV1, ...]
    funding: tuple[MeanFundingPaymentV1, ...]

    def __post_init__(self) -> None:
        super().__post_init__()
        positive(self.probability)
        if not self.points or tuple(sorted({p.at_ns for p in self.points})) != tuple(p.at_ns for p in self.points):
            raise ValueError("complete ordered joint event path required")


@dataclass(frozen=True)
class MeanScenarioSetV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_QUALIFIED_COMMON_SCENARIOS_V1"
    product_ref: str
    account_snapshot_ref: str
    compatibility_ref: str
    source_ref: str
    execution_ref: str
    calibration_ref: str
    cutoff_ns: int
    terminal_ns: int
    source_available_at_ns: int
    entry_delay_ns: int
    exit_delay_ns: int
    paths: tuple[MeanJointPathV1, ...]
    stress_ids: tuple[str, ...]
    stress_exit_prices: tuple[Decimal, ...]
    probability_support: bool
    es_qualified: bool
    provenance: str

    def __post_init__(self) -> None:
        super().__post_init__()
        if not self.source_available_at_ns <= self.cutoff_ns < self.terminal_ns or not self.paths:
            raise ValueError("causal pretrade template required")
        if sum((p.probability for p in self.paths), ZERO) != 1:
            raise ValueError("scenario probability mass must equal one")
        unique(tuple(p.common_path_id for p in self.paths))
        unique(self.stress_ids)
        if not self.stress_ids or len(self.stress_ids) != len(self.stress_exit_prices):
            raise ValueError("joint deterministic stress coverage required")
        for price in self.stress_exit_prices:
            positive(price)
        if self.provenance not in ("PROSPECTIVE_SHADOW", "CAUSAL_REPLAY"):
            raise ValueError("pretrade scenarios are never actual execution labels")
        if len(self.paths) > 4096 or any(len(p.points) > 2048 for p in self.paths):
            raise ValueError("scenario resource bound exceeded")


def replay_independent_action(
    action: ExactActionV1, scenarios: MeanScenarioSetV1, *, quantity_step: Decimal
) -> tuple[MeanCashPathV1, ...]:
    """One IOC then registered stop/failed-break/target/time exits on common paths.

    The first two completed S2 bars and the frozen S3 VWAP target are explicit
    policy inputs. Missing required path evidence refuses valuation. Executable
    prices include spread/impact/delay; there is no fill at a stop trigger.
    """
    from atlas.v2.strategies.s2_breakout import S2_POLICY
    from atlas.v2.strategies.s3_mean_reversion import S3_POLICY

    positive(quantity_step)
    if (
        action.product_ref != scenarios.product_ref
        or action.execution_ref != scenarios.execution_ref
        or action.cutoff_ns != scenarios.cutoff_ns
        or action.terminal_ns != scenarios.terminal_ns
    ):
        raise ValueError("action/common scenario identity mismatch")
    output = []
    long = action.side == "LONG"
    s2 = action.policy_ref == S2_POLICY.policy_hash
    s3 = action.policy_ref == S3_POLICY.policy_hash
    for path in scenarios.paths:
        fills: list[MeanCashFillV1] = []
        entered = False
        remaining = ZERO
        close_at: int | None = None
        m15_seen: set[int] = set()
        expected_m15: tuple[int, ...]
        if s2:
            origin = int(action.registered_setup["trigger_bar_close_ns"])
            expected_m15 = (origin + 900_000_000_000, origin + 1_800_000_000_000)
        else:
            expected_m15 = ()
        for p in path.points:
            if not action.cutoff_ns <= p.at_ns <= action.terminal_ns:
                raise ValueError("scenario event outside common horizon")
            if not entered and p.at_ns >= action.cutoff_ns + scenarios.entry_delay_ns:
                entered = True
                price = p.ask if long else p.bid
                capacity = p.ask_quantity if long else p.bid_quantity
                capacity = (capacity / quantity_step).to_integral_value(rounding=ROUND_FLOOR) * quantity_step
                within = price <= action.entry_collar if long else price >= action.entry_collar
                if p.at_ns < action.deadline_ns and within:
                    remaining = min(action.quantity, capacity)
                    if remaining:
                        fills.append(
                            MeanCashFillV1("entry", p.at_ns, "ENTRY", remaining, price, True, ZERO, p.source_ref)
                        )
            if not remaining:
                continue
            if s2 and close_at is None and any(t < p.at_ns and t not in m15_seen for t in expected_m15):
                raise ValueError("MISSING_S2_SUBSEQUENT_CLOSED_BAR")
            failed_break = False
            if s2 and p.at_ns in expected_m15:
                if p.closed_m15_price is None:
                    raise ValueError("MISSING_S2_SUBSEQUENT_CLOSED_BAR")
                m15_seen.add(p.at_ns)
                failed_break = (
                    Decimal(action.registered_setup["range_low"])
                    <= p.closed_m15_price
                    <= Decimal(action.registered_setup["range_high"])
                )
            target_hit = False
            if s3:
                target = Decimal(action.registered_setup["vwap_target"])
                target_hit = p.bid >= target if long else p.ask <= target
            stop_hit = p.mark <= action.stop_price if long else p.mark >= action.stop_price
            if close_at is None and (stop_hit or failed_break or target_hit or p.at_ns >= action.horizon_end_ns):
                close_at = p.at_ns + scenarios.exit_delay_ns
            if close_at is not None and p.at_ns >= close_at:
                capacity = p.bid_quantity if long else p.ask_quantity
                capacity = (capacity / quantity_step).to_integral_value(rounding=ROUND_FLOOR) * quantity_step
                qty = min(remaining, capacity)
                if qty:
                    fills.append(
                        MeanCashFillV1(
                            f"exit-{len(fills)}",
                            p.at_ns,
                            "EXIT",
                            qty,
                            p.bid if long else p.ask,
                            True,
                            ZERO,
                            p.source_ref,
                        )
                    )
                    remaining -= qty
        output.append(
            MeanCashPathV1(
                action.content_hash,
                path.common_path_id,
                scenarios.content_hash,
                scenarios.provenance,
                tuple(fills),
                path.funding,
            )
        )
    return tuple(output)
