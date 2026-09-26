"""Canonical V2 instrument/product identity and point-in-time universe types."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any, ClassVar

from atlas.domain.money import canonical_decimal_str, ensure_non_negative_decimal, ensure_positive_decimal

from ._serialization import (
    FrozenMap,
    artifact_wire,
    canonical_json,
    decimal_value,
    nonblank,
    seal_envelope,
    sha256_json,
    sha256_ref,
    strict_fields,
    string_tuple,
    timestamp,
)
from .contracts import ArtifactEnvelope, EligibilityStatusV2


class VenueV2(StrEnum):
    BYBIT = "BYBIT"
    BINANCE = "BINANCE"


class EnvironmentV2(StrEnum):
    MAINNET = "MAINNET"
    TESTNET = "TESTNET"


class ProductTypeV2(StrEnum):
    LINEAR_PERPETUAL = "LINEAR_PERPETUAL"


class TradingStatusV2(StrEnum):
    PRELISTING = "PRELISTING"
    TRADING = "TRADING"
    SUSPENDED = "SUSPENDED"
    DELISTED = "DELISTED"


def _enum(enum_type: type[StrEnum], value: Any, *, field_name: str) -> Any:
    try:
        return enum_type(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"invalid {field_name}: {value!r}") from exc


@dataclass(frozen=True)
class InstrumentKeyV2:
    venue: VenueV2
    environment: EnvironmentV2
    product: ProductTypeV2
    native_symbol: str
    base_asset_id: str
    quote_asset: str
    settlement_asset: str
    contract_revision: str

    SCHEMA_VERSION: ClassVar[int] = 1

    def __post_init__(self) -> None:
        object.__setattr__(self, "venue", _enum(VenueV2, self.venue, field_name="venue"))
        object.__setattr__(self, "environment", _enum(EnvironmentV2, self.environment, field_name="environment"))
        object.__setattr__(self, "product", _enum(ProductTypeV2, self.product, field_name="product"))
        for field_name in ("native_symbol", "base_asset_id", "quote_asset", "settlement_asset", "contract_revision"):
            nonblank(getattr(self, field_name), field=field_name)
        if self.quote_asset != "USDT" or self.settlement_asset != "USDT":
            raise ValueError("V2 initial linear perpetuals must be USDT quoted and settled")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.SCHEMA_VERSION,
            "venue": self.venue.value,
            "environment": self.environment.value,
            "product": self.product.value,
            "native_symbol": self.native_symbol,
            "base_asset_id": self.base_asset_id,
            "quote_asset": self.quote_asset,
            "settlement_asset": self.settlement_asset,
            "contract_revision": self.contract_revision,
        }

    def to_canonical_json(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def content_hash(self) -> str:
        return sha256_json({"contract_type": "InstrumentKeyV2", "key": self.to_dict()})

    def __hash__(self) -> int:
        # Python string hashes are process-randomized; derive a stable integer.
        return int(self.content_hash[:16], 16)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InstrumentKeyV2:
        fields = {
            "schema_version",
            "venue",
            "environment",
            "product",
            "native_symbol",
            "base_asset_id",
            "quote_asset",
            "settlement_asset",
            "contract_revision",
        }
        d = strict_fields(data, expected=fields, required=fields, name=cls.__name__)
        if type(d["schema_version"]) is not int or d["schema_version"] != cls.SCHEMA_VERSION:
            raise ValueError("unsupported InstrumentKeyV2 schema_version")
        return cls(
            _enum(VenueV2, d["venue"], field_name="venue"),
            _enum(EnvironmentV2, d["environment"], field_name="environment"),
            _enum(ProductTypeV2, d["product"], field_name="product"),
            d["native_symbol"],
            d["base_asset_id"],
            d["quote_asset"],
            d["settlement_asset"],
            d["contract_revision"],
        )


@dataclass(frozen=True)
class ProductContractV2:
    key: InstrumentKeyV2
    effective_at_ns: int
    observed_at_ns: int
    available_at_ns: int
    base_units_per_contract: Decimal
    tick_size: Decimal
    qty_step: Decimal
    min_qty: Decimal
    trading_status: TradingStatusV2
    metadata_ref: str
    min_notional: Decimal | None = None
    max_qty: Decimal | None = None
    listing_at_ns: int | None = None
    delisting_at_ns: int | None = None
    fee_schedule_ref: str | None = None
    margin_tiers_ref: str | None = None
    funding_schedule_ref: str | None = None

    SCHEMA_VERSION: ClassVar[int] = 1

    def __post_init__(self) -> None:
        if not isinstance(self.key, InstrumentKeyV2):
            raise ValueError("key must be InstrumentKeyV2")
        for field_name in ("effective_at_ns", "observed_at_ns", "available_at_ns"):
            timestamp(getattr(self, field_name), field=field_name)
        if self.available_at_ns < self.observed_at_ns:
            raise ValueError("available_at_ns cannot precede observed_at_ns")
        for field_name in ("base_units_per_contract", "tick_size", "qty_step"):
            object.__setattr__(self, field_name, ensure_positive_decimal(getattr(self, field_name), field=field_name))
        object.__setattr__(self, "min_qty", ensure_non_negative_decimal(self.min_qty, field="min_qty"))
        for field_name in ("min_notional", "max_qty"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, ensure_non_negative_decimal(value, field=field_name))
        if self.max_qty is not None and self.max_qty < self.min_qty:
            raise ValueError("max_qty cannot be below min_qty")
        for field_name in ("listing_at_ns", "delisting_at_ns"):
            value = getattr(self, field_name)
            if value is not None:
                timestamp(value, field=field_name)
        if (
            self.listing_at_ns is not None
            and self.delisting_at_ns is not None
            and self.delisting_at_ns < self.listing_at_ns
        ):
            raise ValueError("delisting_at_ns cannot precede listing_at_ns")
        object.__setattr__(
            self, "trading_status", _enum(TradingStatusV2, self.trading_status, field_name="trading_status")
        )
        nonblank(self.metadata_ref, field="metadata_ref")
        for field_name in ("fee_schedule_ref", "margin_tiers_ref", "funding_schedule_ref"):
            value = getattr(self, field_name)
            if value is not None:
                nonblank(value, field=field_name)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.SCHEMA_VERSION,
            "key": self.key.to_dict(),
            "effective_at_ns": self.effective_at_ns,
            "observed_at_ns": self.observed_at_ns,
            "available_at_ns": self.available_at_ns,
            "base_units_per_contract": canonical_decimal_str(self.base_units_per_contract),
            "tick_size": canonical_decimal_str(self.tick_size),
            "qty_step": canonical_decimal_str(self.qty_step),
            "min_qty": canonical_decimal_str(self.min_qty),
            "min_notional": canonical_decimal_str(self.min_notional) if self.min_notional is not None else None,
            "max_qty": canonical_decimal_str(self.max_qty) if self.max_qty is not None else None,
            "trading_status": self.trading_status.value,
            "listing_at_ns": self.listing_at_ns,
            "delisting_at_ns": self.delisting_at_ns,
            "fee_schedule_ref": self.fee_schedule_ref,
            "margin_tiers_ref": self.margin_tiers_ref,
            "funding_schedule_ref": self.funding_schedule_ref,
            "metadata_ref": self.metadata_ref,
        }

    def to_canonical_json(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def content_hash(self) -> str:
        return sha256_json({"contract_type": "ProductContractV2", "product": self.to_dict()})

    def hash(self) -> str:
        return self.content_hash

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ProductContractV2:
        fields = {
            "schema_version",
            "key",
            "effective_at_ns",
            "observed_at_ns",
            "available_at_ns",
            "base_units_per_contract",
            "tick_size",
            "qty_step",
            "min_qty",
            "min_notional",
            "max_qty",
            "trading_status",
            "listing_at_ns",
            "delisting_at_ns",
            "fee_schedule_ref",
            "margin_tiers_ref",
            "funding_schedule_ref",
            "metadata_ref",
        }
        optional = {
            "min_notional",
            "max_qty",
            "listing_at_ns",
            "delisting_at_ns",
            "fee_schedule_ref",
            "margin_tiers_ref",
            "funding_schedule_ref",
        }
        d = strict_fields(data, expected=fields, required=fields - optional, name=cls.__name__)
        if type(d["schema_version"]) is not int or d["schema_version"] != cls.SCHEMA_VERSION:
            raise ValueError("unsupported ProductContractV2 schema_version")
        decimals: dict[str, Any] = {}
        for name in ("base_units_per_contract", "tick_size", "qty_step", "min_qty", "min_notional", "max_qty"):
            value = d.get(name)
            decimals[name] = decimal_value(value, field=name, wire=True) if value is not None else None
        from_dict_key = InstrumentKeyV2.from_dict(d["key"])
        return cls(
            from_dict_key,
            d["effective_at_ns"],
            d["observed_at_ns"],
            d["available_at_ns"],
            decimals["base_units_per_contract"],
            decimals["tick_size"],
            decimals["qty_step"],
            decimals["min_qty"],
            _enum(TradingStatusV2, d["trading_status"], field_name="trading_status"),
            d["metadata_ref"],
            decimals["min_notional"],
            decimals["max_qty"],
            d.get("listing_at_ns"),
            d.get("delisting_at_ns"),
            d.get("fee_schedule_ref"),
            d.get("margin_tiers_ref"),
            d.get("funding_schedule_ref"),
        )


class InstrumentRegistryV2:
    """Pure append-only registry with point-in-time resolution."""

    def __init__(self) -> None:
        self._contracts: dict[tuple[InstrumentKeyV2, int], ProductContractV2] = {}
        self._by_hash: dict[str, ProductContractV2] = {}

    def register(self, contract: ProductContractV2) -> ProductContractV2:
        if not isinstance(contract, ProductContractV2):
            raise ValueError("only ProductContractV2 may be registered")
        identity = (contract.key, contract.effective_at_ns)
        current = self._contracts.get(identity)
        if current is not None:
            if current.content_hash != contract.content_hash:
                raise ValueError("conflicting product content for identical key/effective time")
            return current
        self._contracts[identity] = contract
        self._by_hash[contract.content_hash] = contract
        return contract

    def get_by_ref(self, product_ref: str) -> ProductContractV2:
        sha256_ref(product_ref, field="product_ref")
        try:
            return self._by_hash[product_ref]
        except KeyError as exc:
            raise KeyError(f"unknown product_ref: {product_ref}") from exc

    def resolve_as_of(
        self, key: InstrumentKeyV2, *, decision_slot_ns: int, information_cutoff_ns: int
    ) -> ProductContractV2 | None:
        if not isinstance(key, InstrumentKeyV2):
            raise ValueError("registry resolution requires full InstrumentKeyV2 identity")
        timestamp(decision_slot_ns, field="decision_slot_ns")
        timestamp(information_cutoff_ns, field="information_cutoff_ns")
        if information_cutoff_ns > decision_slot_ns:
            raise ValueError("information cutoff cannot follow the decision slot")
        eligible = [
            contract
            for (registered_key, _), contract in self._contracts.items()
            if registered_key == key
            and contract.effective_at_ns <= decision_slot_ns
            and contract.available_at_ns <= information_cutoff_ns
            and (contract.listing_at_ns is None or contract.listing_at_ns <= decision_slot_ns)
        ]
        if not eligible:
            return None
        return max(eligible, key=lambda item: (item.effective_at_ns, item.available_at_ns, item.content_hash))

    def validate_universe_as_of(
        self, entries: Sequence[UniverseEntryV2], *, decision_slot_ns: int, information_cutoff_ns: int
    ) -> None:
        for entry in entries:
            product = self.resolve_as_of(
                entry.key, decision_slot_ns=decision_slot_ns, information_cutoff_ns=information_cutoff_ns
            )
            if product is None or product.content_hash != entry.product_ref:
                raise ValueError("universe entry references product metadata unavailable at its information cutoff")

    def contracts(self) -> tuple[ProductContractV2, ...]:
        return tuple(
            sorted(
                self._contracts.values(),
                key=lambda item: (
                    item.key.to_canonical_json(),
                    item.effective_at_ns,
                    item.available_at_ns,
                    item.content_hash,
                ),
            )
        )

    def resolve_key_for_revision(self, contract_revision: str) -> InstrumentKeyV2:
        """Resolve a revision only when it names one exact instrument identity.

        ``contract_revision`` is scoped by an InstrumentKeyV2 in the frozen
        contract. It is not globally unique, so data that carries only the
        revision may be bound to a key only when this registry is unambiguous.
        """
        nonblank(contract_revision, field="contract_revision")
        keys = {item.key for item in self._contracts.values() if item.key.contract_revision == contract_revision}
        if not keys:
            raise ValueError("instrument revision is unresolved in InstrumentRegistryV2")
        if len(keys) != 1:
            raise ValueError("instrument revision is ambiguous across InstrumentKeyV2 identities")
        return next(iter(keys))


@dataclass(frozen=True)
class StrategyEligibilityV2:
    status: EligibilityStatusV2
    reason: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", _enum(EligibilityStatusV2, self.status, field_name="strategy status"))
        if self.status != EligibilityStatusV2.ELIGIBLE and self.reason is None:
            raise ValueError("non-eligible strategy status requires a reason")
        if self.reason is not None:
            nonblank(self.reason, field="strategy reason")

    def to_dict(self) -> dict[str, Any]:
        return {"status": self.status.value, "reason": self.reason}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> StrategyEligibilityV2:
        fields = {"status", "reason"}
        d = strict_fields(data, expected=fields, required=fields, name=cls.__name__)
        return cls(_enum(EligibilityStatusV2, d["status"], field_name="status"), d["reason"])


@dataclass(frozen=True)
class UniverseEntryV2:
    key: InstrumentKeyV2
    product_ref: str
    observed: bool
    data_eligible: bool
    scanner_eligible: bool
    deep_analysis_eligible: bool
    capital_eligible: bool
    strategy_eligibility: FrozenMap
    reasons: tuple[str, ...]
    cheap_feature_ref: str | None = None
    warmup_ref: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.key, InstrumentKeyV2):
            raise ValueError("key must be InstrumentKeyV2")
        sha256_ref(self.product_ref, field="product_ref")
        for field_name in (
            "observed",
            "data_eligible",
            "scanner_eligible",
            "deep_analysis_eligible",
            "capital_eligible",
        ):
            if type(getattr(self, field_name)) is not bool:
                raise ValueError(f"{field_name} must be an explicit boolean")
        strategy = (
            self.strategy_eligibility
            if isinstance(self.strategy_eligibility, FrozenMap)
            else FrozenMap(self.strategy_eligibility)
        )
        if any(not isinstance(value, StrategyEligibilityV2) for value in strategy.values()):
            raise ValueError("strategy_eligibility values must be StrategyEligibilityV2")
        object.__setattr__(self, "strategy_eligibility", strategy)
        reasons = string_tuple(self.reasons, field="reasons", sorted_unique=True)
        object.__setattr__(self, "reasons", reasons)
        for field_name in ("cheap_feature_ref", "warmup_ref"):
            value = getattr(self, field_name)
            if value is not None:
                nonblank(value, field=field_name)

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key.to_dict(),
            "product_ref": self.product_ref,
            "observed": self.observed,
            "data_eligible": self.data_eligible,
            "scanner_eligible": self.scanner_eligible,
            "deep_analysis_eligible": self.deep_analysis_eligible,
            "capital_eligible": self.capital_eligible,
            "strategy_eligibility": {policy: status.to_dict() for policy, status in self.strategy_eligibility.items()},
            "reasons": list(self.reasons),
            "cheap_feature_ref": self.cheap_feature_ref,
            "warmup_ref": self.warmup_ref,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> UniverseEntryV2:
        fields = {
            "key",
            "product_ref",
            "observed",
            "data_eligible",
            "scanner_eligible",
            "deep_analysis_eligible",
            "capital_eligible",
            "strategy_eligibility",
            "reasons",
            "cheap_feature_ref",
            "warmup_ref",
        }
        required = fields - {"cheap_feature_ref", "warmup_ref"}
        d = strict_fields(data, expected=fields, required=required, name=cls.__name__)
        if not isinstance(d["strategy_eligibility"], Mapping) or not isinstance(d["reasons"], list):
            raise ValueError("strategy_eligibility/reasons have invalid wire types")
        return cls(
            InstrumentKeyV2.from_dict(d["key"]),
            d["product_ref"],
            d["observed"],
            d["data_eligible"],
            d["scanner_eligible"],
            d["deep_analysis_eligible"],
            d["capital_eligible"],
            FrozenMap(
                {policy: StrategyEligibilityV2.from_dict(value) for policy, value in d["strategy_eligibility"].items()}
            ),
            tuple(d["reasons"]),
            d.get("cheap_feature_ref"),
            d.get("warmup_ref"),
        )


@dataclass(frozen=True)
class UniverseContractV2:
    envelope: ArtifactEnvelope
    universe_version: str
    decision_slot_ns: int
    selection_policy_hash: str
    entries: tuple[UniverseEntryV2, ...]

    ARTIFACT_TYPE: ClassVar[str] = "UniverseContractV2"

    def __post_init__(self) -> None:
        nonblank(self.universe_version, field="universe_version")
        timestamp(self.decision_slot_ns, field="decision_slot_ns")
        if self.envelope.available_at_ns > self.decision_slot_ns:
            raise ValueError("universe was unavailable at its decision slot")
        sha256_ref(self.selection_policy_hash, field="selection_policy_hash")
        entries = tuple(self.entries)
        if any(not isinstance(entry, UniverseEntryV2) for entry in entries):
            raise ValueError("entries must contain UniverseEntryV2")
        keys = tuple(entry.key.to_canonical_json() for entry in entries)
        if keys != tuple(sorted(keys)) or len(set(keys)) != len(keys):
            raise ValueError("universe entries must be uniquely sorted by canonical instrument key")
        refs = {entry.product_ref for entry in entries}
        if not refs.issubset(set(self.envelope.input_refs)):
            raise ValueError("all product revisions must be bound as envelope input_refs")
        object.__setattr__(self, "entries", entries)
        object.__setattr__(
            self, "envelope", seal_envelope(self.envelope, self._body(), artifact_type=self.ARTIFACT_TYPE)
        )

    def _body(self) -> dict[str, Any]:
        return {
            "universe_version": self.universe_version,
            "decision_slot_ns": self.decision_slot_ns,
            "selection_policy_hash": self.selection_policy_hash,
            "entries": [entry.to_dict() for entry in self.entries],
        }

    @property
    def content_hash(self) -> str:
        return self.envelope.content_hash

    def to_dict(self) -> dict[str, Any]:
        return artifact_wire(self.envelope, self._body(), artifact_type=self.ARTIFACT_TYPE)

    def to_canonical_json(self) -> str:
        return canonical_json(self.to_dict())

    def hash(self) -> str:
        return self.content_hash

    def validate_as_of(self, registry: InstrumentRegistryV2, *, information_cutoff_ns: int) -> None:
        if information_cutoff_ns > self.decision_slot_ns or self.envelope.available_at_ns > information_cutoff_ns:
            raise ValueError("universe artifact is not available at requested replay cutoff")
        registry.validate_universe_as_of(
            self.entries, decision_slot_ns=self.decision_slot_ns, information_cutoff_ns=information_cutoff_ns
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> UniverseContractV2:
        fields = {"envelope", "universe_version", "decision_slot_ns", "selection_policy_hash", "entries"}
        d = strict_fields(data, expected=fields, required=fields, name=cls.ARTIFACT_TYPE)
        if not isinstance(d["entries"], list):
            raise ValueError("entries must be an array")
        return cls(
            ArtifactEnvelope.from_dict(d["envelope"]),
            d["universe_version"],
            d["decision_slot_ns"],
            d["selection_policy_hash"],
            tuple(UniverseEntryV2.from_dict(entry) for entry in d["entries"]),
        )
