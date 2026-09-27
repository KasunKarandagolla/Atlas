"""Causal, unit-preserving public derivatives evidence for S5 context."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any, ClassVar

from .._serialization import decimal_value, nonblank, sha256_json, sha256_ref, timestamp
from ..instruments import InstrumentKeyV2
from .capabilities import (
    FeedCoverageEvidenceV2,
    FeedCoverageStateV2,
    capability_for_public_channel_v2,
    default_evidence_capability_matrix_v2,
)

S5_CROWDING_CONTEXT_VERSION = "S5_CROWDING_CONTEXT_V1"
DEFAULT_LIQUIDATION_CONTEXT_WINDOW_NS = 5 * 60 * 1_000_000_000
S5_CROWDING_CONTEXT_POLICY_HASH = sha256_json({"policy_id": S5_CROWDING_CONTEXT_VERSION, "spec": {
    "dimensions": ["funding", "basis", "open_interest", "price_oi", "liquidation_coverage", "liquidity"],
    "funding_kinds": ["CURRENT", "PREDICTED_IF_QUALIFIED", "SETTLED"],
    "source_channel_capability_matrix_required": True, "derived_input_refs_complete": True,
    "liquidation_population_censoring_preserved": True,
    "ownership_inference": "UNSUPPORTED", "leverage_inference": "UNSUPPORTED",
    "exact_liquidation_map": "UNSUPPORTED", "selector_influence": "ZERO",
}})


class FundingKindV2(StrEnum):
    CURRENT = "CURRENT"
    PREDICTED = "PREDICTED"
    SETTLED = "SETTLED"
    UNQUALIFIED = "UNQUALIFIED"


class DerivativeAvailabilityV2(StrEnum):
    ACTUAL_RECEIPT = "ACTUAL_RECEIPT"
    RECONSTRUCTED_MARKET = "RECONSTRUCTED_MARKET"
    UNKNOWN = "UNKNOWN"


class LiquidationCoverageV2(StrEnum):
    QUALIFIED = "QUALIFIED"
    CENSORED = "CENSORED"
    DEGRADED = "DEGRADED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class FundingObservationV2:
    instrument: InstrumentKeyV2
    source_id: str
    kind: FundingKindV2
    rate: Decimal | None
    unit: str
    event_at_ns: int | None
    received_at_ns: int
    available_at_ns: int
    next_funding_at_ns: int | None
    source_semantics_ref: str
    raw_content_ref: str
    availability: DerivativeAvailabilityV2
    revision_of: str | None = None
    source_health: str = "UNKNOWN"
    source_health_ref: str | None = None
    kind_qualification_ref: str | None = None
    channel: str = "UNKNOWN"

    def __post_init__(self) -> None:
        if not isinstance(self.instrument, InstrumentKeyV2):
            raise ValueError("funding observation requires full instrument identity")
        object.__setattr__(self, "kind", FundingKindV2(self.kind))
        object.__setattr__(self, "availability", DerivativeAvailabilityV2(self.availability))
        for name in ("source_id", "unit", "channel"):
            nonblank(getattr(self, name), field=name)
        nonblank(self.source_health, field="source_health")
        if self.rate is not None:
            object.__setattr__(self, "rate", decimal_value(self.rate, field="rate"))
        for name in ("event_at_ns", "received_at_ns", "available_at_ns", "next_funding_at_ns"):
            value = getattr(self, name)
            if value is not None:
                timestamp(value, field=name)
        if self.available_at_ns < self.received_at_ns:
            raise ValueError("funding availability cannot precede actual receipt")
        if self.availability != DerivativeAvailabilityV2.ACTUAL_RECEIPT and self.kind in {FundingKindV2.CURRENT, FundingKindV2.PREDICTED}:
            raise ValueError("current/predicted funding requires actual receipt availability")
        sha256_ref(self.source_semantics_ref, field="source_semantics_ref")
        sha256_ref(self.raw_content_ref, field="raw_content_ref")
        if self.revision_of is not None:
            nonblank(self.revision_of, field="revision_of")
        if self.source_health_ref is not None:
            sha256_ref(self.source_health_ref, field="source_health_ref")
        if self.kind == FundingKindV2.PREDICTED:
            if self.kind_qualification_ref is None:
                raise ValueError("predicted funding semantics require an exact qualification evidence ref")
        if self.kind_qualification_ref is not None:
            sha256_ref(self.kind_qualification_ref, field="kind_qualification_ref")
        if self.source_health == "HEALTHY_CURRENT" and self.source_health_ref is None:
            raise ValueError("healthy funding evidence requires exact source-health ref")

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, "instrument": self.instrument.to_dict(), "source_id": self.source_id,
                "kind": self.kind.value, "rate": str(self.rate) if self.rate is not None else None,
                "unit": self.unit, "event_at_ns": self.event_at_ns, "received_at_ns": self.received_at_ns,
                "available_at_ns": self.available_at_ns, "next_funding_at_ns": self.next_funding_at_ns,
                "source_semantics_ref": self.source_semantics_ref, "raw_content_ref": self.raw_content_ref,
                "availability": self.availability.value, "revision_of": self.revision_of,
                "source_health": self.source_health, "source_health_ref": self.source_health_ref,
                "kind_qualification_ref": self.kind_qualification_ref, "channel": self.channel}

    @property
    def content_hash(self) -> str:
        return sha256_json({"artifact_type": "FundingObservationV2", "observation": self.to_dict()})


@dataclass(frozen=True)
class OpenInterestObservationV2:
    instrument: InstrumentKeyV2
    source_id: str
    quantity: Decimal | None
    quantity_unit: str | None
    value: Decimal | None
    value_unit: str | None
    event_at_ns: int | None
    received_at_ns: int
    available_at_ns: int
    source_semantics_ref: str
    raw_content_ref: str
    availability: DerivativeAvailabilityV2
    revision_of: str | None = None
    mark_price: Decimal | None = None
    index_price: Decimal | None = None
    last_price: Decimal | None = None
    source_health: str = "UNKNOWN"
    source_health_ref: str | None = None
    channel: str = "UNKNOWN"

    def __post_init__(self) -> None:
        if not isinstance(self.instrument, InstrumentKeyV2):
            raise ValueError("open interest requires full instrument identity")
        object.__setattr__(self, "availability", DerivativeAvailabilityV2(self.availability))
        nonblank(self.source_id, field="source_id")
        nonblank(self.channel, field="channel")
        nonblank(self.source_health, field="source_health")
        for field in ("quantity", "value", "mark_price", "index_price", "last_price"):
            val = getattr(self, field)
            if val is not None:
                object.__setattr__(self, field, decimal_value(val, field=field))
        for field in ("quantity_unit", "value_unit"):
            val = getattr(self, field)
            if val is not None:
                nonblank(val, field=field)
        if (self.quantity is None) != (self.quantity_unit is None):
            raise ValueError("OI quantity and its raw unit must be supplied together")
        if (self.value is None) != (self.value_unit is None):
            raise ValueError("OI value and its raw unit must be supplied together")
        for name in ("event_at_ns", "received_at_ns", "available_at_ns"):
            val = getattr(self, name)
            if val is not None:
                timestamp(val, field=name)
        if self.available_at_ns < self.received_at_ns:
            raise ValueError("OI availability cannot precede actual receipt")
        sha256_ref(self.source_semantics_ref, field="source_semantics_ref")
        sha256_ref(self.raw_content_ref, field="raw_content_ref")
        if self.revision_of is not None:
            nonblank(self.revision_of, field="revision_of")
        if self.source_health_ref is not None:
            sha256_ref(self.source_health_ref, field="source_health_ref")
        if self.source_health == "HEALTHY_CURRENT" and self.source_health_ref is None:
            raise ValueError("healthy OI evidence requires exact source-health ref")

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, "instrument": self.instrument.to_dict(), "source_id": self.source_id,
                "quantity": str(self.quantity) if self.quantity is not None else None,
                "quantity_unit": self.quantity_unit, "value": str(self.value) if self.value is not None else None,
                "value_unit": self.value_unit, "event_at_ns": self.event_at_ns,
                "received_at_ns": self.received_at_ns, "available_at_ns": self.available_at_ns,
                "source_semantics_ref": self.source_semantics_ref, "raw_content_ref": self.raw_content_ref,
                "availability": self.availability.value, "revision_of": self.revision_of,
                "mark_price": str(self.mark_price) if self.mark_price is not None else None,
                "index_price": str(self.index_price) if self.index_price is not None else None,
                "last_price": str(self.last_price) if self.last_price is not None else None,
                "source_health": self.source_health, "source_health_ref": self.source_health_ref,
                "channel": self.channel}

    @property
    def content_hash(self) -> str:
        return sha256_json({"artifact_type": "OpenInterestObservationV2", "observation": self.to_dict()})


@dataclass(frozen=True)
class LiquidationObservationV2:
    instrument: InstrumentKeyV2
    source_id: str
    event_id: str
    side: str | None
    side_convention: str | None
    price: Decimal | None
    quantity: Decimal | None
    quantity_unit: str | None
    event_at_ns: int | None
    received_at_ns: int
    available_at_ns: int
    coverage: LiquidationCoverageV2
    feed_health: str
    raw_content_ref: str
    availability: DerivativeAvailabilityV2
    source_health_ref: str | None = None
    channel: str = "UNKNOWN"

    def __post_init__(self) -> None:
        if not isinstance(self.instrument, InstrumentKeyV2):
            raise ValueError("liquidation observation requires full instrument identity")
        object.__setattr__(self, "coverage", LiquidationCoverageV2(self.coverage))
        object.__setattr__(self, "availability", DerivativeAvailabilityV2(self.availability))
        for name in ("source_id", "event_id", "feed_health", "channel"):
            nonblank(getattr(self, name), field=name)
        if self.source_health_ref is not None:
            sha256_ref(self.source_health_ref, field="source_health_ref")
        if self.feed_health == "HEALTHY_CURRENT" and self.source_health_ref is None:
            raise ValueError("healthy liquidation record requires exact source-health ref")
        if self.side not in {None, "BUY_POSITION_LIQUIDATED", "SELL_POSITION_LIQUIDATED", "UNKNOWN"}:
            raise ValueError("liquidation side must retain explicit position-side convention")
        for name in ("side_convention", "quantity_unit"):
            value = getattr(self, name)
            if value is not None:
                nonblank(value, field=name)
        for name in ("price", "quantity"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, decimal_value(value, field=name))
        for name in ("event_at_ns", "received_at_ns", "available_at_ns"):
            value = getattr(self, name)
            if value is not None:
                timestamp(value, field=name)
        if self.available_at_ns < self.received_at_ns:
            raise ValueError("liquidation availability cannot precede actual receipt")
        sha256_ref(self.raw_content_ref, field="raw_content_ref")

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, "instrument": self.instrument.to_dict(), "source_id": self.source_id,
                "event_id": self.event_id, "side": self.side, "side_convention": self.side_convention,
                "price": str(self.price) if self.price is not None else None,
                "quantity": str(self.quantity) if self.quantity is not None else None,
                "quantity_unit": self.quantity_unit, "event_at_ns": self.event_at_ns,
                "received_at_ns": self.received_at_ns, "available_at_ns": self.available_at_ns,
                "coverage": self.coverage.value, "feed_health": self.feed_health,
                "raw_content_ref": self.raw_content_ref, "availability": self.availability.value,
                "source_health_ref": self.source_health_ref, "channel": self.channel}

    @property
    def content_hash(self) -> str:
        return sha256_json({"artifact_type": "LiquidationObservationV2", "observation": self.to_dict()})


@dataclass(frozen=True)
class S5CrowdingContextV2:
    instrument: InstrumentKeyV2
    cutoff_ns: int
    state: str
    funding_kind: str | None
    funding_event_at_ns: int | None
    funding_received_at_ns: int | None
    funding_available_at_ns: int | None
    funding_availability: DerivativeAvailabilityV2 | None
    funding_kind_qualification_ref: str | None
    funding_rate: Decimal | None
    funding_percentile: Decimal | None
    next_funding_at_ns: int | None
    oi_quantity: Decimal | None
    oi_quantity_unit: str | None
    oi_value: Decimal | None
    oi_value_unit: str | None
    oi_event_at_ns: int | None
    oi_received_at_ns: int | None
    oi_available_at_ns: int | None
    oi_availability: DerivativeAvailabilityV2 | None
    oi_change_15m: Decimal | None
    price_oi_relationship: str
    mark_price: Decimal | None
    index_price: Decimal | None
    last_price: Decimal | None
    basis: Decimal | None
    liquidation_intensity: Decimal | None
    liquidation_intensity_unit: str | None
    liquidation_coverage: LiquidationCoverageV2
    liquidity_state: str
    evidence_quality: str
    input_refs: tuple[str, ...]
    liquidation_window_ns: int = DEFAULT_LIQUIDATION_CONTEXT_WINDOW_NS
    capability_matrix_ref: str | None = None
    version: str = S5_CROWDING_CONTEXT_VERSION

    SCHEMA_VERSION: ClassVar[int] = 1

    def __post_init__(self) -> None:
        if not isinstance(self.instrument, InstrumentKeyV2):
            raise ValueError("crowding context requires full instrument key")
        timestamp(self.cutoff_ns, field="cutoff_ns")
        object.__setattr__(self, "liquidation_coverage", LiquidationCoverageV2(self.liquidation_coverage))
        if self.funding_availability is not None:
            object.__setattr__(self, "funding_availability", DerivativeAvailabilityV2(self.funding_availability))
        if self.oi_availability is not None:
            object.__setattr__(self, "oi_availability", DerivativeAvailabilityV2(self.oi_availability))
        for name in ("state", "price_oi_relationship", "liquidity_state", "evidence_quality", "version"):
            nonblank(getattr(self, name), field=name)
        for name in ("funding_rate", "funding_percentile", "oi_quantity", "oi_value", "oi_change_15m", "mark_price", "index_price", "last_price", "basis", "liquidation_intensity"):
            val = getattr(self, name)
            if val is not None:
                object.__setattr__(self, name, decimal_value(val, field=name))
        if self.next_funding_at_ns is not None:
            timestamp(self.next_funding_at_ns, field="next_funding_at_ns")
        for name in ("funding_event_at_ns", "funding_received_at_ns", "funding_available_at_ns",
                     "oi_event_at_ns", "oi_received_at_ns", "oi_available_at_ns"):
            value = getattr(self, name)
            if value is not None:
                timestamp(value, field=name)
        if self.funding_kind_qualification_ref is not None:
            sha256_ref(self.funding_kind_qualification_ref, field="funding_kind_qualification_ref")
        for ref in self.input_refs:
            sha256_ref(ref, field="input_ref")
        if type(self.liquidation_window_ns) is not int or self.liquidation_window_ns <= 0:
            raise ValueError("liquidation context window must be positive")
        if self.capability_matrix_ref is not None:
            sha256_ref(self.capability_matrix_ref, field="capability_matrix_ref")

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": self.SCHEMA_VERSION, "version": self.version,
                "policy_hash": S5_CROWDING_CONTEXT_POLICY_HASH,
                "instrument": self.instrument.to_dict(), "cutoff_ns": self.cutoff_ns, "state": self.state,
                "funding_kind": self.funding_kind,
                "funding_event_at_ns": self.funding_event_at_ns,
                "funding_received_at_ns": self.funding_received_at_ns,
                "funding_available_at_ns": self.funding_available_at_ns,
                "funding_availability": self.funding_availability.value if self.funding_availability else None,
                "funding_kind_qualification_ref": self.funding_kind_qualification_ref,
                "funding_rate": str(self.funding_rate) if self.funding_rate is not None else None,
                "funding_percentile": str(self.funding_percentile) if self.funding_percentile is not None else None,
                "next_funding_at_ns": self.next_funding_at_ns,
                "oi_quantity": str(self.oi_quantity) if self.oi_quantity is not None else None,
                "oi_quantity_unit": self.oi_quantity_unit,
                "oi_value": str(self.oi_value) if self.oi_value is not None else None,
                "oi_value_unit": self.oi_value_unit,
                "oi_event_at_ns": self.oi_event_at_ns, "oi_received_at_ns": self.oi_received_at_ns,
                "oi_available_at_ns": self.oi_available_at_ns,
                "oi_availability": self.oi_availability.value if self.oi_availability else None,
                "oi_change_15m": str(self.oi_change_15m) if self.oi_change_15m is not None else None,
                "price_oi_relationship": self.price_oi_relationship,
                "mark_price": str(self.mark_price) if self.mark_price is not None else None,
                "index_price": str(self.index_price) if self.index_price is not None else None,
                "last_price": str(self.last_price) if self.last_price is not None else None,
                "basis": str(self.basis) if self.basis is not None else None,
                "liquidation_intensity": str(self.liquidation_intensity) if self.liquidation_intensity is not None else None,
                "liquidation_intensity_unit": self.liquidation_intensity_unit,
                "liquidation_coverage": self.liquidation_coverage.value, "liquidity_state": self.liquidity_state,
                "evidence_quality": self.evidence_quality, "input_refs": list(self.input_refs),
                "liquidation_window_ns": self.liquidation_window_ns,
                "capability_matrix_ref": self.capability_matrix_ref,
                "ownership_inference": "UNSUPPORTED", "leverage_inference": "UNSUPPORTED",
                "exact_liquidation_map": "UNSUPPORTED"}

    @property
    def content_hash(self) -> str:
        return sha256_json({"artifact_type": "S5CrowdingContextV2", "artifact": self.to_dict()})


@dataclass(frozen=True)
class LiquidationWindowTotalV2:
    instrument: InstrumentKeyV2
    source_id: str
    window_start_ns: int
    window_end_ns: int
    available_at_ns: int
    observed_quantity: Decimal
    quantity_unit: str
    coverage: LiquidationCoverageV2
    feed_health: str
    source_health_ref: str
    input_refs: tuple[str, ...]
    channel: str = "UNKNOWN"
    capability_matrix_ref: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.instrument, InstrumentKeyV2):
            raise ValueError("liquidation window requires full instrument")
        nonblank(self.source_id, field="source_id")
        nonblank(self.channel, field="channel")
        nonblank(self.quantity_unit, field="quantity_unit")
        nonblank(self.feed_health, field="feed_health")
        for name in ("window_start_ns", "window_end_ns", "available_at_ns"):
            timestamp(getattr(self, name), field=name)
        if self.window_end_ns <= self.window_start_ns or self.available_at_ns < self.window_end_ns:
            raise ValueError("liquidation window chronology is invalid")
        object.__setattr__(self, "observed_quantity", decimal_value(self.observed_quantity, field="observed_quantity"))
        if self.observed_quantity < 0:
            raise ValueError("liquidation window quantity cannot be negative")
        object.__setattr__(self, "coverage", LiquidationCoverageV2(self.coverage))
        sha256_ref(self.source_health_ref, field="source_health_ref")
        for ref in self.input_refs:
            sha256_ref(ref, field="input_ref")
        if self.capability_matrix_ref is not None:
            sha256_ref(self.capability_matrix_ref, field="capability_matrix_ref")
        matrix = default_evidence_capability_matrix_v2()
        capability = capability_for_public_channel_v2(matrix, self.instrument, self.channel)
        if (self.capability_matrix_ref == matrix.content_hash and capability is not None
                and any(marker in capability.coverage_censoring_limitations.lower()
                        for marker in ("censored", "event-filtered", "population completeness"))):
            object.__setattr__(self, "coverage", LiquidationCoverageV2.CENSORED)

    @property
    def content_hash(self) -> str:
        return sha256_json({"artifact_type": "LiquidationWindowTotalV2", "window": self.to_dict()})

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, "instrument": self.instrument.to_dict(), "source_id": self.source_id,
                "window_start_ns": self.window_start_ns, "window_end_ns": self.window_end_ns,
                "available_at_ns": self.available_at_ns, "observed_quantity": str(self.observed_quantity),
                "quantity_unit": self.quantity_unit, "coverage": self.coverage.value,
                "feed_health": self.feed_health, "source_health_ref": self.source_health_ref,
                "input_refs": list(self.input_refs), "channel": self.channel,
                "capability_matrix_ref": self.capability_matrix_ref}


@dataclass(frozen=True)
class S5LiquidationBaselineV2:
    instrument: InstrumentKeyV2
    source_id: str
    cutoff_ns: int
    window_duration_ns: int
    quantity_unit: str
    training_refs: tuple[str, ...]
    mean: Decimal
    std: Decimal
    coverage: LiquidationCoverageV2 = LiquidationCoverageV2.UNKNOWN
    channel: str = "UNKNOWN"
    capability_matrix_ref: str | None = None
    version: str = "S5_LIQUIDATION_BASELINE_PRIOR_V1"

    def __post_init__(self) -> None:
        if not isinstance(self.instrument, InstrumentKeyV2):
            raise ValueError("liquidation baseline requires full instrument")
        nonblank(self.source_id, field="source_id")
        nonblank(self.quantity_unit, field="quantity_unit")
        nonblank(self.channel, field="channel")
        object.__setattr__(self, "coverage", LiquidationCoverageV2(self.coverage))
        timestamp(self.cutoff_ns, field="cutoff_ns")
        if self.window_duration_ns <= 0 or len(self.training_refs) < 20:
            raise ValueError("liquidation baseline requires at least 20 prior fixed-duration windows")
        for ref in self.training_refs:
            sha256_ref(ref, field="training_ref")
        object.__setattr__(self, "mean", decimal_value(self.mean, field="mean"))
        object.__setattr__(self, "std", decimal_value(self.std, field="std"))
        if self.mean < 0 or self.std <= 0:
            raise ValueError("liquidation baseline mean/std are invalid")
        if self.capability_matrix_ref is not None:
            sha256_ref(self.capability_matrix_ref, field="capability_matrix_ref")
        nonblank(self.version, field="version")

    @property
    def content_hash(self) -> str:
        return sha256_json({"artifact_type": "S5LiquidationBaselineV2", "baseline": self.to_dict()})

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, "version": self.version, "instrument": self.instrument.to_dict(),
                "source_id": self.source_id, "cutoff_ns": self.cutoff_ns,
                "window_duration_ns": self.window_duration_ns, "quantity_unit": self.quantity_unit,
                "training_refs": list(self.training_refs), "mean": str(self.mean), "std": str(self.std),
                "coverage": self.coverage.value, "channel": self.channel,
                "capability_matrix_ref": self.capability_matrix_ref,
                "training_availability_rule": "STRICTLY_BEFORE_CUTOFF"}


@dataclass(frozen=True)
class OIChangeEvidenceV2:
    """Exact 15-minute open-interest change with both source observations bound."""

    instrument: InstrumentKeyV2
    source_id: str
    cutoff_ns: int
    available_at_ns: int
    interval_ns: int
    start_quantity: Decimal
    end_quantity: Decimal
    quantity_unit: str
    change_fraction: Decimal
    start_event_at_ns: int
    end_event_at_ns: int
    start_available_at_ns: int
    end_available_at_ns: int
    start_ref: str
    end_ref: str
    source_health_refs: tuple[str, ...]
    availability: DerivativeAvailabilityV2
    version: str = "S5_OI_CHANGE_15M_V1"
    channel: str = "UNKNOWN"
    capability_matrix_ref: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.instrument, InstrumentKeyV2):
            raise ValueError("OI change requires full instrument identity")
        nonblank(self.source_id, field="source_id")
        nonblank(self.quantity_unit, field="quantity_unit")
        nonblank(self.channel, field="channel")
        nonblank(self.version, field="version")
        matrix = default_evidence_capability_matrix_v2()
        if not _capability_supports(self.instrument, self.channel, "crowding context"):
            raise ValueError("OI change channel is absent from the capability matrix")
        if self.capability_matrix_ref != matrix.content_hash:
            raise ValueError("OI change must bind the current capability matrix hash")
        for name in ("cutoff_ns", "available_at_ns", "start_event_at_ns", "end_event_at_ns",
                     "start_available_at_ns", "end_available_at_ns"):
            timestamp(getattr(self, name), field=name)
        if self.interval_ns != 15 * 60 * 1_000_000_000:
            raise ValueError("OI change evidence must span exactly 15 minutes")
        if self.end_event_at_ns - self.start_event_at_ns != self.interval_ns:
            raise ValueError("OI evidence observation event times do not span 15 minutes")
        if self.end_available_at_ns > self.cutoff_ns or self.available_at_ns > self.cutoff_ns:
            raise ValueError("OI change evidence cannot be used before actual availability")
        if self.start_available_at_ns > self.cutoff_ns:
            raise ValueError("OI start observation was unavailable at the evaluation cutoff")
        object.__setattr__(self, "start_quantity", decimal_value(self.start_quantity, field="start_quantity"))
        object.__setattr__(self, "end_quantity", decimal_value(self.end_quantity, field="end_quantity"))
        object.__setattr__(self, "change_fraction", decimal_value(self.change_fraction, field="change_fraction"))
        if self.start_quantity == 0 or self.change_fraction != (self.end_quantity - self.start_quantity) / abs(self.start_quantity):
            raise ValueError("OI change value does not match its bound observations")
        object.__setattr__(self, "availability", DerivativeAvailabilityV2(self.availability))
        for ref in (self.start_ref, self.end_ref, *self.source_health_refs):
            sha256_ref(ref, field="OI evidence ref")

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, "version": self.version, "instrument": self.instrument.to_dict(),
                "source_id": self.source_id, "cutoff_ns": self.cutoff_ns,
                "available_at_ns": self.available_at_ns, "interval_ns": self.interval_ns,
                "start_quantity": str(self.start_quantity), "end_quantity": str(self.end_quantity),
                "quantity_unit": self.quantity_unit, "change_fraction": str(self.change_fraction),
                "start_event_at_ns": self.start_event_at_ns, "end_event_at_ns": self.end_event_at_ns,
                "start_available_at_ns": self.start_available_at_ns,
                "end_available_at_ns": self.end_available_at_ns,
                "start_ref": self.start_ref, "end_ref": self.end_ref,
                "source_health_refs": list(self.source_health_refs),
                "availability": self.availability.value,
                "channel": self.channel, "capability_matrix_ref": self.capability_matrix_ref,
                "causal_rule": "BOTH_OBSERVATIONS_ACTUALLY_AVAILABLE_BY_CUTOFF"}

    @property
    def input_refs(self) -> tuple[str, ...]:
        return tuple(sorted({self.start_ref, self.end_ref, *self.source_health_refs}))

    @property
    def content_hash(self) -> str:
        return sha256_json({"artifact_type": "OIChangeEvidenceV2", "artifact": self.to_dict()})


def fit_s5_liquidation_baseline(windows: tuple[LiquidationWindowTotalV2, ...], *,
                                instrument: InstrumentKeyV2, source_id: str,
                                cutoff_ns: int, minimum_windows: int = 20) -> S5LiquidationBaselineV2 | None:
    cutoff = timestamp(cutoff_ns, field="cutoff_ns")
    if minimum_windows < 20:
        raise ValueError("venue liquidation baseline minimum is fixed at 20 windows")
    matrix = default_evidence_capability_matrix_v2()
    eligible = [w for w in windows if w.instrument == instrument and w.source_id == source_id
                and w.capability_matrix_ref == matrix.content_hash
                and _capability_supports(w.instrument, w.channel, "observed event intensity")
                and w.available_at_ns < cutoff
                and w.coverage in {LiquidationCoverageV2.QUALIFIED, LiquidationCoverageV2.CENSORED}
                and w.feed_health == "HEALTHY_CURRENT"]
    eligible.sort(key=lambda w: (w.window_end_ns, w.available_at_ns, w.content_hash))
    if len(eligible) < minimum_windows:
        return None
    latest = eligible[-1]
    duration = latest.window_end_ns - latest.window_start_ns
    homogeneous = [w for w in eligible if w.channel == latest.channel
                   and w.capability_matrix_ref == latest.capability_matrix_ref
                   and w.quantity_unit == latest.quantity_unit
                   and w.window_end_ns - w.window_start_ns == duration]
    if len(homogeneous) < minimum_windows:
        return None
    selected = homogeneous[-minimum_windows:]
    values = [w.observed_quantity for w in selected]
    mean = sum(values, Decimal(0)) / Decimal(len(values))
    variance = sum(((value - mean) ** 2 for value in values), Decimal(0)) / Decimal(len(values))
    std = variance.sqrt()
    if std == 0:
        return None
    capability = capability_for_public_channel_v2(matrix, instrument, latest.channel)
    censored = any(w.coverage == LiquidationCoverageV2.CENSORED for w in selected) or bool(
        capability and any(marker in capability.coverage_censoring_limitations.lower()
                           for marker in ("censored", "event-filtered", "population completeness"))
    )
    return S5LiquidationBaselineV2(instrument, source_id, cutoff, duration, latest.quantity_unit,
                                   tuple(w.content_hash for w in selected), mean, std,
                                   LiquidationCoverageV2.CENSORED if censored else LiquidationCoverageV2.QUALIFIED,
                                   latest.channel, matrix.content_hash)


def _capability_supports(instrument: InstrumentKeyV2, channel: str, use: str) -> bool:
    capability = capability_for_public_channel_v2(
        default_evidence_capability_matrix_v2(), instrument, channel,
    )
    return capability is not None and any(use in permitted for permitted in capability.permitted_uses)


def funding_percentile_prior(observations: tuple[FundingObservationV2, ...], *, cutoff_ns: int,
                             current: FundingObservationV2) -> Decimal | None:
    """Empirical percentile uses only actual-receipt observations available strictly before cutoff."""
    prior = _funding_percentile_prior_rows(observations, cutoff_ns=cutoff_ns, current=current)
    values = [x.rate for x in prior if x.rate is not None]
    if (not values or current.rate is None or current.available_at_ns > cutoff_ns
            or current.availability != DerivativeAvailabilityV2.ACTUAL_RECEIPT
            or current.source_health != "HEALTHY_CURRENT" or current.source_health_ref is None):
        return None
    if not _capability_supports(current.instrument, current.channel, "crowding context"):
        return None
    if current.kind not in {FundingKindV2.CURRENT, FundingKindV2.PREDICTED, FundingKindV2.SETTLED}:
        return None
    if current.kind == FundingKindV2.PREDICTED and current.kind_qualification_ref is None:
        return None
    less = sum(1 for value in values if value < current.rate)
    equal = sum(1 for value in values if value == current.rate)
    return (Decimal(less) + Decimal(equal) / Decimal(2)) / Decimal(len(values))


def _funding_percentile_prior_rows(observations: tuple[FundingObservationV2, ...], *, cutoff_ns: int,
                                   current: FundingObservationV2) -> list[FundingObservationV2]:
    return [x for x in observations if x.instrument == current.instrument and x.rate is not None
             and x.source_id == current.source_id and x.unit == current.unit
             and x.channel == current.channel
             and _capability_supports(x.instrument, x.channel, "crowding context")
             and x.available_at_ns < cutoff_ns and x.availability == DerivativeAvailabilityV2.ACTUAL_RECEIPT
             and x.source_health == "HEALTHY_CURRENT" and x.source_health_ref is not None
             and x.kind in {FundingKindV2.SETTLED, FundingKindV2.CURRENT}]


def oi_change_15m(observations: tuple[OpenInterestObservationV2, ...], *, cutoff_ns: int,
                  instrument: InstrumentKeyV2) -> Decimal | None:
    evidence = oi_change_15m_evidence(observations, cutoff_ns=cutoff_ns, instrument=instrument)
    return evidence.change_fraction if evidence is not None else None


def oi_change_15m_evidence(observations: tuple[OpenInterestObservationV2, ...], *, cutoff_ns: int,
                           instrument: InstrumentKeyV2) -> OIChangeEvidenceV2 | None:
    cutoff = timestamp(cutoff_ns, field="cutoff_ns")
    eligible = [x for x in observations if x.instrument == instrument and x.quantity is not None
                and x.quantity_unit is not None and x.event_at_ns is not None and x.event_at_ns <= cutoff
                and _capability_supports(x.instrument, x.channel, "crowding context")
                and x.available_at_ns <= cutoff and x.source_health == "HEALTHY_CURRENT"
                and x.source_health_ref is not None]
    if not eligible:
        return None
    # Choose the latest market observation that was actually known at cutoff;
    # a delayed older sample cannot replace a newer event-time sample.
    eligible.sort(key=lambda x: (x.event_at_ns or 0, x.available_at_ns, x.content_hash))
    latest = eligible[-1]
    target = (latest.event_at_ns or 0) - 15 * 60 * 1_000_000_000
    prior = [x for x in eligible[:-1] if x.event_at_ns == target
             and x.quantity_unit == latest.quantity_unit and x.source_id == latest.source_id
             and x.channel == latest.channel
             and x.availability == latest.availability]
    if not prior or latest.quantity == 0 or latest.quantity is None or latest.quantity_unit is None:
        return None
    start = max(prior, key=lambda x: (x.available_at_ns, x.content_hash))
    if start.quantity is None or start.quantity == 0 or start.event_at_ns is None or latest.event_at_ns is None:
        return None
    if start.source_health_ref is None or latest.source_health_ref is None:
        return None
    change = (latest.quantity - start.quantity) / abs(start.quantity)
    available = max(start.available_at_ns, latest.available_at_ns)
    return OIChangeEvidenceV2(
        instrument, latest.source_id, cutoff, available, 15 * 60 * 1_000_000_000,
        start.quantity, latest.quantity, latest.quantity_unit, change,
        start.event_at_ns, latest.event_at_ns, start.available_at_ns, latest.available_at_ns,
        start.raw_content_ref, latest.raw_content_ref,
        tuple(sorted({start.source_health_ref, latest.source_health_ref})), latest.availability,
        channel=latest.channel, capability_matrix_ref=default_evidence_capability_matrix_v2().content_hash,
    )


def _price_change_15m(observations: tuple[OpenInterestObservationV2, ...], *, cutoff_ns: int,
                      instrument: InstrumentKeyV2, source_id: str | None = None,
                      channel: str | None = None) -> tuple[Decimal | None, tuple[str, ...]]:
    eligible = [x for x in observations if x.instrument == instrument and x.available_at_ns <= cutoff_ns
                and (source_id is None or x.source_id == source_id)
                and (channel is None or x.channel == channel)
                and _capability_supports(x.instrument, x.channel, "crowding context")
                and x.event_at_ns is not None and x.event_at_ns <= cutoff_ns
                and (x.last_price is not None or x.mark_price is not None)
                and x.source_health == "HEALTHY_CURRENT" and x.source_health_ref is not None]
    if not eligible:
        return None, ()
    eligible.sort(key=lambda x: (x.event_at_ns or 0, x.available_at_ns, x.content_hash))
    latest = eligible[-1]
    latest_price = latest.last_price if latest.last_price is not None else latest.mark_price
    target = (latest.event_at_ns or 0) - 15 * 60 * 1_000_000_000
    prior = [x for x in eligible[:-1] if x.event_at_ns == target
             and x.source_id == latest.source_id and x.channel == latest.channel
             and x.availability == latest.availability]
    if latest_price is None or not prior:
        return None, ()
    start = max(prior, key=lambda x: (x.available_at_ns, x.content_hash))
    if latest.mark_price is not None and start.mark_price is not None:
        latest_price, start_price = latest.mark_price, start.mark_price
    elif latest.last_price is not None and start.last_price is not None:
        latest_price, start_price = latest.last_price, start.last_price
    else:
        return None, ()
    if start_price in (None, Decimal(0)):
        return None, ()
    refs = tuple(sorted({latest.raw_content_ref, start.raw_content_ref,
                         *([latest.source_health_ref] if latest.source_health_ref else []),
                         *([start.source_health_ref] if start.source_health_ref else [])}))
    return (latest_price - start_price) / abs(start_price), refs


def build_s5_crowding_context(*, instrument: InstrumentKeyV2, cutoff_ns: int,
                              funding: tuple[FundingObservationV2, ...] = (),
                              open_interest: tuple[OpenInterestObservationV2, ...] = (),
                              liquidations: tuple[LiquidationObservationV2, ...] = (),
                              liquidity_state: str = "UNKNOWN",
                              liquidation_feed_coverage: FeedCoverageEvidenceV2 | None = None,
                              liquidation_window_ns: int = DEFAULT_LIQUIDATION_CONTEXT_WINDOW_NS) -> S5CrowdingContextV2:
    cutoff = timestamp(cutoff_ns, field="cutoff_ns")
    if type(liquidation_window_ns) is not int or liquidation_window_ns <= 0:
        raise ValueError("liquidation context window must be positive")
    matrix = default_evidence_capability_matrix_v2()
    frows = [x for x in funding if x.instrument == instrument
             and _capability_supports(x.instrument, x.channel, "crowding context")
             and x.available_at_ns <= cutoff
             and x.source_health == "HEALTHY_CURRENT" and x.source_health_ref is not None
             and x.kind in {FundingKindV2.CURRENT, FundingKindV2.PREDICTED, FundingKindV2.SETTLED}
             and (x.kind != FundingKindV2.PREDICTED or x.kind_qualification_ref is not None)]
    frows.sort(key=lambda x: (x.event_at_ns or x.next_funding_at_ns or 0,
                              x.available_at_ns, x.content_hash))
    f = frows[-1] if frows else None
    rows = [x for x in open_interest if x.instrument == instrument
            and _capability_supports(x.instrument, x.channel, "crowding context")
            and x.available_at_ns <= cutoff and (x.event_at_ns is None or x.event_at_ns <= cutoff)
            and x.source_health == "HEALTHY_CURRENT" and x.source_health_ref is not None]
    rows.sort(key=lambda x: (x.event_at_ns or 0, x.available_at_ns, x.content_hash))
    oi = rows[-1] if rows else None
    oi_delta_evidence = oi_change_15m_evidence(tuple(open_interest), cutoff_ns=cutoff, instrument=instrument)
    oi_delta = oi_delta_evidence.change_fraction if oi_delta_evidence is not None else None
    prior_percentile = funding_percentile_prior(tuple(funding), cutoff_ns=cutoff, current=f) if f else None
    percentile_training_rows = (_funding_percentile_prior_rows(tuple(funding), cutoff_ns=cutoff, current=f)
                                if f is not None and prior_percentile is not None else [])
    basis = None
    if oi is not None and oi.mark_price is not None and oi.index_price is not None and oi.index_price != 0:
        basis = (oi.mark_price - oi.index_price) / oi.index_price
    price_direction, price_input_refs = _price_change_15m(
        tuple(open_interest), cutoff_ns=cutoff, instrument=instrument,
        source_id=oi_delta_evidence.source_id if oi_delta_evidence else None,
        channel=oi_delta_evidence.channel if oi_delta_evidence else None,
    )
    if oi_delta is None or price_direction is None:
        relation = "NOT_ESTIMABLE"
    elif oi_delta > 0 and price_direction > 0:
        relation = "PRICE_UP_OI_UP"
    elif oi_delta < 0 and price_direction < 0:
        relation = "PRICE_DOWN_OI_DOWN"
    elif oi_delta > 0 and price_direction < 0:
        relation = "PRICE_DOWN_OI_UP"
    elif oi_delta < 0 and price_direction > 0:
        relation = "PRICE_UP_OI_DOWN"
    else:
        relation = "MIXED_OR_FLAT"
    known_liquidations = [x for x in liquidations if x.instrument == instrument
                          and _capability_supports(x.instrument, x.channel, "observed event intensity")
                          and x.available_at_ns <= cutoff]
    window_start = cutoff - liquidation_window_ns
    lrows = [x for x in known_liquidations if x.event_at_ns is not None
             and window_start <= x.event_at_ns <= cutoff]
    liquidation_coverage_capability = (capability_for_public_channel_v2(
        matrix, instrument, liquidation_feed_coverage.channel,
    ) if liquidation_feed_coverage is not None else None)
    coverage_identity_ok = bool(liquidation_feed_coverage
                                and liquidation_feed_coverage.capability_matrix_ref == matrix.content_hash
                                and liquidation_coverage_capability is not None
                                and "observed event intensity" in " ".join(
                                    liquidation_coverage_capability.permitted_uses))
    coverage_ok = bool(liquidation_feed_coverage and coverage_identity_ok
                       and liquidation_feed_coverage.instrument == instrument
                       and liquidation_feed_coverage.available_at_ns <= cutoff
                       and liquidation_feed_coverage.covered_from_ns <= window_start
                       and liquidation_feed_coverage.covered_through_ns >= cutoff
                       and liquidation_feed_coverage.state == FeedCoverageStateV2.QUALIFIED)
    has_unplaced_event = any(x.event_at_ns is None for x in known_liquidations)
    matrix_declares_censoring = bool(liquidation_coverage_capability and any(
        marker in liquidation_coverage_capability.coverage_censoring_limitations.lower()
        for marker in ("censored", "event-filtered", "population completeness")
    ))
    coverage = (LiquidationCoverageV2.DEGRADED if has_unplaced_event or any(
                    x.coverage in {LiquidationCoverageV2.DEGRADED, LiquidationCoverageV2.UNKNOWN}
                    or x.feed_health != "HEALTHY_CURRENT" for x in known_liquidations
                ) else LiquidationCoverageV2.CENSORED if matrix_declares_censoring or any(
                    x.coverage == LiquidationCoverageV2.CENSORED for x in known_liquidations
                ) else LiquidationCoverageV2.QUALIFIED if coverage_ok else LiquidationCoverageV2.UNKNOWN)
    intensity = None
    units = {x.quantity_unit for x in lrows if x.quantity is not None and x.quantity_unit is not None}
    if lrows and len(units) == 1 and all(x.quantity is not None for x in lrows):
        intensity = sum((x.quantity or Decimal(0) for x in lrows), Decimal(0))
    intensity_unit = next(iter(units)) if len(units) == 1 else None
    selected_observations = ([f] if f else []) + ([oi] if oi else []) + lrows
    health_refs = {x.source_health_ref for x in selected_observations if x.source_health_ref is not None}
    refs = tuple(sorted({x.raw_content_ref for x in selected_observations}
                        | {x.content_hash for x in selected_observations}
                        | health_refs
                        | ({x.raw_content_ref for x in percentile_training_rows})
                        | ({x.content_hash for x in percentile_training_rows})
                        | ({x.source_health_ref for x in percentile_training_rows
                            if x.source_health_ref is not None})
                        | (set(oi_delta_evidence.input_refs) | {oi_delta_evidence.content_hash}
                           if oi_delta_evidence is not None else set())
                        | set(price_input_refs)
                        | ({liquidation_feed_coverage.content_hash}
                           if coverage_identity_ok and liquidation_feed_coverage else set())
                        | (set(liquidation_feed_coverage.input_refs)
                           if coverage_identity_ok and liquidation_feed_coverage else set())
                        | ({liquidation_feed_coverage.source_health_ref}
                           if coverage_identity_ok and liquidation_feed_coverage else set())))
    evidence_quality = "SUPPORTED" if f and oi else "PARTIAL" if f or oi else "UNVERIFIED_OR_MISSING"
    state = "CROWDING_CONTEXT" if f or oi or lrows else "NOT_ESTIMABLE"
    return S5CrowdingContextV2(
        instrument, cutoff, state, f.kind.value if f else None,
        f.event_at_ns if f else None, f.received_at_ns if f else None,
        f.available_at_ns if f else None, f.availability if f else None,
        f.kind_qualification_ref if f else None, f.rate if f else None, prior_percentile,
        f.next_funding_at_ns if f else None, oi.quantity if oi else None,
        oi.quantity_unit if oi else None, oi.value if oi else None, oi.value_unit if oi else None,
        oi.event_at_ns if oi else None, oi.received_at_ns if oi else None,
        oi.available_at_ns if oi else None, oi.availability if oi else None,
        oi_delta, relation,
        oi.mark_price if oi else None, oi.index_price if oi else None, oi.last_price if oi else None,
        basis, intensity, intensity_unit, coverage, liquidity_state, evidence_quality, refs,
        liquidation_window_ns, matrix.content_hash,
    )
