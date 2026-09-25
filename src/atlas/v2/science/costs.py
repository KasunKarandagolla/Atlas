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


def index_cost_evidence(repo: OpsRepository, item: FeeScheduleV2 | FundingCashflowV2 |
                        FundingScheduleV2) -> str:
    source = repo.get_artifact(item.source_ref)
    if source is None or source.available_at_ns > item.available_at_ns:
        raise ValueError("cost source artifact unindexed or future")
    repo.register_artifact(ArtifactIndexEntryV2(item.content_hash, type(item).__name__, item.content_hash,
        item.available_at_ns, item.available_at_ns, item.to_dict()))
    return item.content_hash
