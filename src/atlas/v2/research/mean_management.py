"""Finite offline future-value management and financed-rotation research.

No native orders, journal migration, ownership controller or live rotation.
Historical management_shadow.review_partial_tp retains its exact identity.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import ClassVar, cast

from atlas.v2.instruments import ProductContractV2
from atlas.v2.science.costs import FeeScheduleV2, FundingScheduleV2, fee_cash
from atlas.v2.science.mean_common import DAY_NS, ZERO, OfflineArtifact, nonnegative, positive, unique
from atlas.v2.science.mean_scenarios import MeanJointPathV1

MANAGEMENT_KINDS = ("HOLD", "CLOSE", "PARTIAL_TP", "TIGHTEN", "COST_ADJUSTED_BE", "REGISTERED_TRAIL", "TP_ADJUST")


@dataclass(frozen=True)
class ManagementPositionV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_MANAGEMENT_POSITION_RESEARCH_V1"
    product_ref: str
    fee_ref: str
    funding_ref: str
    account_snapshot_ref: str
    ownership_ref: str
    permission_ref: str
    position_epoch: str
    side: str
    quantity: Decimal
    possible_closing_quantity: Decimal
    entry_price: Decimal
    current_price: Decimal
    stop_price: Decimal
    initial_stop_price: Decimal
    horizon_end_ns: int
    entry_at_ns: int
    cutoff_ns: int
    entry_fees_remaining_cash: Decimal
    funding_debit_cash: Decimal
    future_slippage_cash: Decimal
    sunk_realized_cash: Decimal

    def __post_init__(self) -> None:
        super().__post_init__()
        positive(self.quantity, self.entry_price, self.current_price, self.stop_price, self.initial_stop_price)
        nonnegative(self.possible_closing_quantity, self.entry_fees_remaining_cash, self.future_slippage_cash)
        if (
            self.side not in ("LONG", "SHORT")
            or self.possible_closing_quantity > self.quantity
            or not self.entry_at_ns <= self.cutoff_ns < self.horizon_end_ns <= self.cutoff_ns + DAY_NS
            or not self.position_epoch
        ):
            raise ValueError("invalid exact management position")


@dataclass(frozen=True)
class ManagementAlternativeV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_REGISTERED_MANAGEMENT_ALTERNATIVE_V1"
    position_ref: str
    registry_ref: str
    kind: str
    close_quantity: Decimal
    stop_price: Decimal
    take_profit_price: Decimal | None
    trailing_fraction: Decimal | None
    horizon_end_ns: int
    trigger_price: Decimal | None
    source_ref: str
    authority: str = "OFFLINE_RESEARCH_ONLY"

    def __post_init__(self) -> None:
        super().__post_init__()
        nonnegative(self.close_quantity)
        positive(self.stop_price)
        if self.kind not in MANAGEMENT_KINDS or self.authority != "OFFLINE_RESEARCH_ONLY":
            raise ValueError("unsupported management alternative")
        if self.trailing_fraction is not None and not ZERO < self.trailing_fraction < 1:
            raise ValueError("invalid registered trail fraction")
        if (self.kind == "REGISTERED_TRAIL") != (self.trailing_fraction is not None):
            raise ValueError("trail parameter/kind mismatch")
        if (self.kind == "TP_ADJUST") != (self.take_profit_price is not None):
            raise ValueError("TP parameter/kind mismatch")
        if self.kind == "PARTIAL_TP" and self.trigger_price is None:
            raise ValueError("registered partial trigger required")
        if self.kind not in ("CLOSE", "PARTIAL_TP") and self.close_quantity:
            raise ValueError("nonclosing alternative cannot change quantity")


def round_protective_stop(price: Decimal, *, tick: Decimal, side: str) -> Decimal:
    positive(price, tick)
    if side not in ("LONG", "SHORT"):
        raise ValueError("invalid stop side")
    return (price / tick).to_integral_value(rounding=ROUND_CEILING if side == "LONG" else ROUND_FLOOR) * tick


def cost_adjusted_breakeven(
    position: ManagementPositionV1, *, fee: FeeScheduleV2, product: ProductContractV2
) -> Decimal:
    if position.product_ref != product.content_hash or fee.key != product.key:
        raise ValueError("BE instrument mismatch")
    direction = Decimal(1) if position.side == "LONG" else Decimal(-1)
    qty = position.quantity * product.base_units_per_contract
    costs = position.entry_fees_remaining_cash + position.funding_debit_cash + position.future_slippage_cash
    price = (direction * position.entry_price + costs / qty) / (direction - fee.exit_taker_rate)
    return round_protective_stop(price, tick=product.tick_size, side=position.side)


def register_management_alternative(
    position: ManagementPositionV1,
    *,
    kind: str,
    product: ProductContractV2,
    fees: FeeScheduleV2,
    registry_ref: str,
    source_ref: str,
    fraction: Decimal = Decimal("0.5"),
    requested_stop: Decimal | None = None,
    requested_tp: Decimal | None = None,
    trail_fraction: Decimal | None = None,
    observed_extrema: tuple[tuple[int, Decimal], ...] = (),
) -> ManagementAlternativeV1:
    if kind not in MANAGEMENT_KINDS or product.content_hash != position.product_ref or fees.key != product.key:
        raise ValueError("unregistered/wrong-product management")
    if position.possible_closing_quantity and kind != "HOLD":
        raise ValueError("UNKNOWN_PENDING_CLOSING_EFFECTS")
    available = position.quantity - position.possible_closing_quantity
    qty = ZERO
    stop = position.stop_price
    trigger = None
    if kind in ("CLOSE", "PARTIAL_TP"):
        if not ZERO < fraction <= 1:
            raise ValueError("closing fraction outside (0,1]")
        qty = (available * (Decimal(1) if kind == "CLOSE" else fraction) / product.qty_step).to_integral_value(
            rounding=ROUND_FLOOR
        ) * product.qty_step
        residual = position.quantity - qty
        if (
            qty < product.min_qty
            or (
                product.min_notional is not None
                and qty * product.base_units_per_contract * position.current_price < product.min_notional
            )
            or (
                residual
                and (
                    residual < product.min_qty
                    or (
                        product.min_notional is not None
                        and residual * product.base_units_per_contract * position.current_price < product.min_notional
                    )
                )
            )
        ):
            raise ValueError("NO_ACTION: invalid partial quantity or residual dust")
        if kind == "PARTIAL_TP":
            direction = Decimal(1) if position.side == "LONG" else Decimal(-1)
            trigger = position.entry_price + direction * 2 * abs(position.entry_price - position.initial_stop_price)
    if kind == "COST_ADJUSTED_BE":
        stop = cost_adjusted_breakeven(position, fee=fees, product=product)
    elif kind == "TIGHTEN":
        if requested_stop is None:
            raise ValueError("exact tightening stop required")
        stop = round_protective_stop(requested_stop, tick=product.tick_size, side=position.side)
    elif kind == "REGISTERED_TRAIL":
        if (
            not observed_extrema
            or any(t > position.cutoff_ns or t < position.entry_at_ns for t, _ in observed_extrema)
            or trail_fraction is None
        ):
            raise ValueError("causal observed trail extrema required")
        if not ZERO < trail_fraction < 1:
            raise ValueError("invalid trail distance")
        for _, p in observed_extrema:
            positive(p)
        extreme = (max if position.side == "LONG" else min)(p for _, p in observed_extrema)
        raw = extreme * (1 - trail_fraction if position.side == "LONG" else 1 + trail_fraction)
        stop = round_protective_stop(raw, tick=product.tick_size, side=position.side)
    if position.side == "LONG":
        if stop < position.stop_price or stop >= position.current_price:
            raise ValueError("stop widens or crosses current trigger side")
    elif stop > position.stop_price or stop <= position.current_price:
        raise ValueError("stop widens or crosses current trigger side")
    if kind == "TP_ADJUST":
        if (
            requested_tp is None
            or requested_tp % product.tick_size
            or (position.side == "LONG" and requested_tp <= position.current_price)
            or (position.side == "SHORT" and requested_tp >= position.current_price)
        ):
            raise ValueError("invalid sealed TP adjustment")
    return ManagementAlternativeV1(
        position.content_hash,
        registry_ref,
        kind,
        qty,
        stop,
        requested_tp if kind == "TP_ADJUST" else None,
        trail_fraction if kind == "REGISTERED_TRAIL" else None,
        position.horizon_end_ns,
        trigger,
        source_ref,
    )


@dataclass(frozen=True)
class ManagementFutureValueV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_MANAGEMENT_FUTURE_VALUE_V1"
    position_ref: str
    alternative_ref: str
    common_path_id: str
    future_cash: Decimal | None
    residual_quantity: Decimal
    released_at_ns: int | None
    status: str


def value_management_future(
    position: ManagementPositionV1,
    alternative: ManagementAlternativeV1,
    *,
    path: MeanJointPathV1,
    product: ProductContractV2,
    fees: FeeScheduleV2,
    funding_schedule: FundingScheduleV2,
    exit_delay_ns: int = 0,
) -> ManagementFutureValueV1:
    if (
        alternative.position_ref != position.content_hash
        or alternative.horizon_end_ns != position.horizon_end_ns
        or product.content_hash != position.product_ref
        or fees.key != product.key
        or fees.content_hash != position.fee_ref
        or funding_schedule.content_hash != position.funding_ref
        or max(fees.available_at_ns, funding_schedule.available_at_ns) > position.cutoff_ns
        or exit_delay_ns < 0
    ):
        raise ValueError("management future-value binding mismatch")
    if alternative.close_quantity > position.quantity - position.possible_closing_quantity:
        raise ValueError("management quantity exceeds fresh proven residual")
    if (position.side == "LONG" and alternative.stop_price < position.stop_price) or (
        position.side == "SHORT" and alternative.stop_price > position.stop_price
    ):
        raise ValueError("management stop widening refused")
    if alternative.kind == "COST_ADJUSTED_BE" and alternative.stop_price != cost_adjusted_breakeven(
        position, fee=fees, product=product
    ):
        raise ValueError("cost-adjusted BE mathematical identity mismatch")
    direction = Decimal(1) if position.side == "LONG" else Decimal(-1)
    remaining = position.quantity
    cash = ZERO
    stop = alternative.stop_price
    partial_done = False
    partial_due: int | None = None
    exit_due = None
    fills: list[tuple[int, Decimal]] = []
    release = None
    for p in path.points:
        if not position.cutoff_ns <= p.at_ns <= position.cutoff_ns + DAY_NS:
            raise ValueError("future management path precedes current snapshot")
        price = p.bid if position.side == "LONG" else p.ask
        capacity = p.bid_quantity if position.side == "LONG" else p.ask_quantity
        capacity = (capacity / product.qty_step).to_integral_value(rounding=ROUND_FLOOR) * product.qty_step
        if alternative.kind == "REGISTERED_TRAIL":
            assert alternative.trailing_fraction is not None
            raw = p.mark * (1 - alternative.trailing_fraction if direction == 1 else 1 + alternative.trailing_fraction)
            proposed = round_protective_stop(raw, tick=product.tick_size, side=position.side)
            stop = max(stop, proposed) if direction == 1 else min(stop, proposed)
        stop_hit = p.mark <= stop if direction == 1 else p.mark >= stop
        tp_hit = alternative.take_profit_price is not None and (
            p.mark >= alternative.take_profit_price if direction == 1 else p.mark <= alternative.take_profit_price
        )
        if exit_due is None and (
            alternative.kind == "CLOSE" or stop_hit or tp_hit or p.at_ns >= position.horizon_end_ns
        ):
            exit_due = p.at_ns + exit_delay_ns
        to_close = ZERO
        if exit_due is not None and p.at_ns >= exit_due:
            to_close = min(remaining, capacity)
        elif alternative.kind == "PARTIAL_TP" and not partial_done and alternative.trigger_price is not None:
            hit = p.mark >= alternative.trigger_price if direction == 1 else p.mark <= alternative.trigger_price
            if hit and partial_due is None:
                partial_due = p.at_ns + exit_delay_ns
            if partial_due is not None and p.at_ns >= partial_due:
                to_close = min(alternative.close_quantity, remaining, capacity)
                partial_done = True
        if to_close:
            cash += direction * to_close * product.base_units_per_contract * (price - position.current_price)
            cash -= fee_cash(to_close, price, product.base_units_per_contract, fees.exit_taker_rate)
            fills.append((p.at_ns, to_close))
            remaining -= to_close
            if not remaining:
                release = p.at_ns
                break
    payments = {p.at_ns: p for p in path.funding}
    if any(t not in funding_schedule.expected_settlement_times_ns for t in payments):
        raise ValueError("unregistered future funding payment")
    missing_funding = False
    for at in funding_schedule.expected_settlement_times_ns:
        if position.cutoff_ns < at <= position.cutoff_ns + DAY_NS:
            qty = position.quantity - sum((q for fill_at, q in fills if fill_at < at), ZERO)
            if qty:
                payment = payments.get(at)
                if payment is None:
                    missing_funding = True
                else:
                    cash -= direction * qty * product.base_units_per_contract * payment.mark_price * payment.signed_rate
    unavailable = bool(remaining or missing_funding or position.possible_closing_quantity)
    return ManagementFutureValueV1(
        position.content_hash,
        alternative.content_hash,
        path.common_path_id,
        None if unavailable else cash,
        remaining,
        release,
        "NOT ESTIMABLE" if unavailable else "VALUED",
    )


def compare_future_management(
    hold: tuple[ManagementFutureValueV1, ...],
    alternative: tuple[ManagementFutureValueV1, ...],
    probabilities: tuple[Decimal, ...],
) -> Decimal:
    unique(tuple(x.common_path_id for x in hold))
    if (
        not hold
        or len(hold) != len(alternative)
        or len(hold) != len(probabilities)
        or sum(probabilities, ZERO) != 1
        or any(p <= 0 for p in probabilities)
    ):
        raise ValueError("coherent common-path management comparison required")
    if any(
        a.common_path_id != b.common_path_id
        or a.position_ref != b.position_ref
        or a.future_cash is None
        or b.future_cash is None
        for a, b in zip(hold, alternative, strict=True)
    ):
        raise ValueError("NOT ESTIMABLE: incompatible/unresolved management paths")
    return sum(
        (
            p * (cast(Decimal, b.future_cash) - cast(Decimal, a.future_cash))
            for p, a, b in zip(probabilities, hold, alternative, strict=True)
        ),
        ZERO,
    )


@dataclass(frozen=True)
class RotationBranchV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_CONDITIONAL_ROTATION_RESEARCH_V1"
    common_path_id: str
    hold_a: ManagementFutureValueV1
    close_a: ManagementFutureValueV1
    replacement_action_ref: str
    replacement_cash: Decimal | None
    replacement_margin_cash: Decimal
    released_margin_cash: Decimal
    replacement_available_at_ns: int
    replacement_expires_at_ns: int
    future_programme_cost_cash: Decimal
    source_ref: str

    def __post_init__(self) -> None:
        super().__post_init__()
        nonnegative(self.replacement_margin_cash, self.released_margin_cash, self.future_programme_cost_cash)
        if (
            self.common_path_id != self.hold_a.common_path_id
            or self.common_path_id != self.close_a.common_path_id
            or self.hold_a.position_ref != self.close_a.position_ref
        ):
            raise ValueError("rotation requires the same joint market/position paths")


def rotation_future_advantage(branch: RotationBranchV1) -> tuple[Decimal, str]:
    if branch.hold_a.future_cash is None or branch.close_a.future_cash is None:
        raise ValueError("NOT ESTIMABLE: unresolved close/hold A")
    release = branch.close_a.released_at_ns
    can_open = (
        release is not None
        and max(release, branch.replacement_available_at_ns) < branch.replacement_expires_at_ns
        and branch.released_margin_cash >= branch.replacement_margin_cash
        and branch.replacement_cash is not None
    )
    replacement = cast(Decimal, branch.replacement_cash) if can_open else ZERO
    # Closing A is irreversible even if B later expires or has no fill.
    value = branch.close_a.future_cash + replacement - branch.future_programme_cost_cash - branch.hold_a.future_cash
    return value, "CONDITIONAL_OPEN_RESEARCH" if can_open else "REPLACEMENT_UNAVAILABLE_OR_EXPIRED"
