"""Versioned fee and signed funding evidence for V2 shadow replay."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from atlas.domain.money import canonical_decimal_str as _c
from atlas.v2._serialization import sha256_json, sha256_ref
from atlas.v2.instruments import InstrumentKeyV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository


def _rate(value: Decimal, field: str) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite() or not Decimal("0") <= value <= Decimal("1"):
        raise ValueError(f"{field} requires a finite Decimal fraction")
    return value


@dataclass(frozen=True)
class FeeScheduleV2:
    key: InstrumentKeyV2
    available_at_ns: int
    entry_taker_rate: Decimal
    exit_taker_rate: Decimal
    source_ref: str

    def __post_init__(self) -> None:
        if type(self.available_at_ns) is not int or self.available_at_ns < 0:
            raise ValueError("fee availability requires UTC nanoseconds")
        _rate(self.entry_taker_rate, "entry_taker_rate")
        _rate(self.exit_taker_rate, "exit_taker_rate")
        sha256_ref(self.source_ref, field="source_ref")

    def to_dict(self) -> dict[str, Any]:
        return {"version": "V2_TAKER_FEES_V1", "key": self.key.to_dict(),
                "available_at_ns": self.available_at_ns, "entry_taker_rate": _c(self.entry_taker_rate),
                "exit_taker_rate": _c(self.exit_taker_rate), "source_ref": self.source_ref}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def fee_cash(quantity: Decimal, price: Decimal, base_units_per_contract: Decimal, rate: Decimal) -> Decimal:
    _rate(rate, "rate")
    if any(not isinstance(x, Decimal) or not x.is_finite() or x < 0 for x in
           (quantity, price, base_units_per_contract)):
        raise ValueError("fee inputs require nonnegative finite Decimal")
    return quantity * base_units_per_contract * price * rate


@dataclass(frozen=True)
class FundingCashflowV2:
    at_ns: int
    available_at_ns: int
    signed_rate: Decimal
    mark_price: Decimal
    source_ref: str

    def __post_init__(self) -> None:
        if type(self.at_ns) is not int or type(self.available_at_ns) is not int or min(self.at_ns, self.available_at_ns) < 0:
            raise ValueError("funding times require UTC nanoseconds")
        if self.available_at_ns < self.at_ns:
            raise ValueError("settled funding cannot precede settlement")
        if not isinstance(self.signed_rate, Decimal) or not self.signed_rate.is_finite():
            raise ValueError("funding rate requires finite Decimal")
        if not isinstance(self.mark_price, Decimal) or not self.mark_price.is_finite() or self.mark_price <= 0:
            raise ValueError("funding mark requires positive Decimal")
        sha256_ref(self.source_ref, field="source_ref")

    def to_dict(self) -> dict[str, Any]:
        return {"version": "V2_SETTLED_FUNDING_V1", "at_ns": self.at_ns,
                "available_at_ns": self.available_at_ns, "signed_rate": _c(self.signed_rate),
                "mark_price": _c(self.mark_price), "source_ref": self.source_ref}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def funding_cash(side: str, quantity: Decimal, base_units_per_contract: Decimal,
                 settlement: FundingCashflowV2) -> Decimal:
    """Positive rate debits long and credits short; return signed cash P&L."""
    direction = Decimal("1") if side == "LONG" else Decimal("-1") if side == "SHORT" else None
    if direction is None or quantity < 0:
        raise ValueError("invalid funding side/quantity")
    return -direction * quantity * base_units_per_contract * settlement.mark_price * settlement.signed_rate


@dataclass(frozen=True)
class FundingScheduleV2:
    available_at_ns: int
    expected_settlement_times_ns: tuple[int, ...]
    explicit_zero_funding: bool
    source_ref: str

    def __post_init__(self) -> None:
        if type(self.available_at_ns) is not int or self.available_at_ns < 0:
            raise ValueError("funding schedule availability invalid")
        times = tuple(sorted(self.expected_settlement_times_ns))
        if len(set(times)) != len(times) or any(type(x) is not int or x < 0 for x in times):
            raise ValueError("funding settlement times invalid")
        if self.explicit_zero_funding and times:
            raise ValueError("explicit zero funding contradicts scheduled settlement")
        if not times and not self.explicit_zero_funding:
            raise ValueError("missing funding support is not zero")
        sha256_ref(self.source_ref, field="source_ref")
        object.__setattr__(self, "expected_settlement_times_ns", times)

    def to_dict(self) -> dict[str, Any]:
        return {"version": "V2_FUNDING_SCHEDULE_V1", "available_at_ns": self.available_at_ns,
                "expected_settlement_times_ns": list(self.expected_settlement_times_ns),
                "explicit_zero_funding": self.explicit_zero_funding, "source_ref": self.source_ref}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class ActionCostContractV2:
    """Cutoff-known cost contract explicitly selected by a candidate policy."""

    key: InstrumentKeyV2
    policy_hash: str
    available_at_ns: int
    fee_schedule_ref: str
    funding_schedule_ref: str
    execution_assumptions_ref: str
    source_ref: str

    def __post_init__(self) -> None:
        if type(self.available_at_ns) is not int or self.available_at_ns < 0:
            raise ValueError("action cost contract availability requires UTC nanoseconds")
        for name in ("policy_hash", "fee_schedule_ref", "funding_schedule_ref", "execution_assumptions_ref", "source_ref"):
            sha256_ref(getattr(self, name), field=name)

    def to_dict(self) -> dict[str, Any]:
        return {"version": "V2_ACTION_COST_CONTRACT_V1", "key": self.key.to_dict(),
            "policy_hash": self.policy_hash, "available_at_ns": self.available_at_ns,
            "fee_schedule_ref": self.fee_schedule_ref, "funding_schedule_ref": self.funding_schedule_ref,
            "execution_assumptions_ref": self.execution_assumptions_ref, "source_ref": self.source_ref}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def index_cost_evidence(repo: OpsRepository, item: FeeScheduleV2 | FundingCashflowV2 |
                        FundingScheduleV2) -> str:
    source = repo.get_artifact(item.source_ref)
    if source is None or source.available_at_ns > item.available_at_ns:
        raise ValueError("cost source artifact unindexed or future")
    repo.register_artifact(ArtifactIndexEntryV2(item.content_hash, type(item).__name__, item.content_hash,
        item.available_at_ns, item.available_at_ns, item.to_dict()))
    return item.content_hash


def index_action_cost_contract(repo: OpsRepository, item: ActionCostContractV2) -> str:
    refs = (item.fee_schedule_ref, item.funding_schedule_ref, item.execution_assumptions_ref, item.source_ref)
    if any((entry := repo.get_artifact(ref)) is None or entry.available_at_ns > item.available_at_ns for ref in refs):
        raise ValueError("action cost contract inputs must be typed and cutoff-known")
    repo.register_artifact(ArtifactIndexEntryV2(item.content_hash, "ActionCostContractV2", item.content_hash,
        item.available_at_ns, item.available_at_ns, {"cost_contract": item.to_dict()}))
    return item.content_hash

from typing import TYPE_CHECKING, ClassVar  # noqa: E402

if TYPE_CHECKING:
    from atlas.v2.science.action import ExactActionV1
from atlas.v2.science.mean_common import ZERO, OfflineArtifact, nonnegative, positive, unique  # noqa: E402


@dataclass(frozen=True)
class MeanCashFillV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_CASH_FILL_V1"
    fill_id: str
    at_ns: int
    leg: str
    quantity: Decimal
    executable_price: Decimal
    prices_include_spread_impact_delay: bool
    additional_slippage_cash: Decimal
    source_ref: str

    def __post_init__(self) -> None:
        super().__post_init__()
        positive(self.quantity, self.executable_price)
        nonnegative(self.additional_slippage_cash)
        if not self.fill_id or self.leg not in ("ENTRY", "EXIT"):
            raise ValueError("invalid cash-fill identity/leg")
        if self.prices_include_spread_impact_delay and self.additional_slippage_cash:
            raise ValueError("slippage would be charged twice")


@dataclass(frozen=True)
class MeanFundingPaymentV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_MODELLED_OR_SETTLED_FUNDING_PAYMENT_V1"
    at_ns: int
    signed_rate: Decimal
    mark_price: Decimal
    source_ref: str

    def __post_init__(self) -> None:
        super().__post_init__()
        positive(self.mark_price)
        if abs(self.signed_rate) > 1:
            raise ValueError("unsupported funding fraction")


@dataclass(frozen=True)
class MeanCashPathV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_CASH_PATH_V1"
    action_ref: str
    common_path_id: str
    source_ref: str
    provenance: str
    fills: tuple[MeanCashFillV1, ...]
    funding: tuple[MeanFundingPaymentV1, ...]

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.provenance not in ("ACTUAL_RECONCILED", "PROSPECTIVE_SHADOW", "CAUSAL_REPLAY", "DIAGNOSTIC", "CENSORED"):
            raise ValueError("unknown cash-path evidence grade")
        unique(tuple(f.fill_id for f in self.fills))
        unique(tuple(f.at_ns for f in self.funding))
        if tuple(sorted(f.at_ns for f in self.fills)) != tuple(f.at_ns for f in self.fills):
            raise ValueError("fills must have actual event ordering")


@dataclass(frozen=True)
class MeanCashValueV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_COMMON_TERMINAL_CASH_VALUE_V1"
    action_ref: str
    path_ref: str
    terminal_ns: int
    trading_cash: Decimal | None
    fees_cash: Decimal
    funding_cash: Decimal
    slippage_cash: Decimal
    filled_quantity: Decimal
    ioc_cancelled_quantity: Decimal
    residual_quantity: Decimal
    status: str
    reasons: tuple[str, ...]


def value_mean_cash_path(*, action: ExactActionV1, path: MeanCashPathV1,
        fee: FeeScheduleV2, funding_schedule: FundingScheduleV2,
        base_units_per_contract: Decimal) -> MeanCashValueV1:
    """Y(a) at the common terminal versus no new opening; no reinvestment.

    Settlement uses exposure immediately before its timestamp. A fill exactly
    at payment therefore cannot retroactively earn/avoid that payment. Entry
    and exit at equal timestamps require supplied event ordering in fills.
    Residual inventory is NOT ESTIMABLE, never assumed closed at the trigger.
    Programme expense is accounted separately, once per incurred bill.
    """
    positive(base_units_per_contract)
    if (path.action_ref != action.content_hash or fee.content_hash != action.fee_ref
            or fee.key != action.key or funding_schedule.content_hash != action.funding_ref
            or max(fee.available_at_ns, funding_schedule.available_at_ns) > action.cutoff_ns):
        raise ValueError("cash path/action/cost revision mismatch")
    direction = Decimal(1) if action.side == "LONG" else Decimal(-1)
    inventory = entered = fees = flows = slip = ZERO
    entry_times: set[int] = set()
    for f in path.fills:
        if not action.cutoff_ns <= f.at_ns <= action.terminal_ns:
            raise ValueError("fill outside common finite horizon")
        if f.leg == "ENTRY":
            if f.at_ns >= action.deadline_ns:
                raise ValueError("opening fill outside sealed IOC window")
            if (action.side == "LONG" and f.executable_price > action.entry_collar) or (
                    action.side == "SHORT" and f.executable_price < action.entry_collar):
                raise ValueError("entry outside collar")
            entry_times.add(f.at_ns)
            entered += f.quantity
            inventory += f.quantity
            flows -= direction * f.quantity * base_units_per_contract * f.executable_price
            rate = fee.entry_taker_rate
        else:
            inventory -= f.quantity
            flows += direction * f.quantity * base_units_per_contract * f.executable_price
            rate = fee.exit_taker_rate
        if inventory < 0 or entered > action.quantity or len(entry_times) > 1:
            raise ValueError("entry chase, overfill or exit reversal")
        fees += fee_cash(f.quantity, f.executable_price, base_units_per_contract, rate)
        slip += f.additional_slippage_cash
    payments = {p.at_ns: p for p in path.funding}
    expected = funding_schedule.expected_settlement_times_ns
    if any(t not in expected for t in payments):
        raise ValueError("funding payment not in bound venue schedule")
    funding = ZERO
    missing: list[str] = []
    for at in expected:
        if not action.cutoff_ns <= at <= action.terminal_ns:
            continue
        qty = sum((f.quantity if f.leg == "ENTRY" else -f.quantity for f in path.fills if f.at_ns < at), ZERO)
        if qty:
            p = payments.get(at)
            if p is None:
                missing.append("MISSING_FUNDING_PAYMENT")
            else:
                # Reuse the historical signed-funding primitive, without
                # changing its settled evidence identity.
                settlement = FundingCashflowV2(at, at, p.signed_rate, p.mark_price, p.source_ref)
                funding += funding_cash(action.side, qty, base_units_per_contract, settlement)
    if inventory:
        missing.append("UNRESOLVED_EXIT_RESIDUAL")
    if path.provenance in ("CENSORED", "DIAGNOSTIC"):
        missing.append("UNQUALIFIED_EXECUTION_LABEL")
    return MeanCashValueV1(action.content_hash, path.content_hash, action.terminal_ns,
        None if missing else flows - fees + funding - slip, fees, funding, slip, entered,
        action.quantity - entered, inventory, "NOT ESTIMABLE" if missing else "VALUED", tuple(missing))
