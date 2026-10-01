"""Causal S3 VWAP/statistical mean-reversion research policy.

This module emits immutable research states, watches, and unsized candidate
artifacts. It has no capital, order, leverage, protection, or quantity API.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from statistics import mean, stdev

from atlas.v2._serialization import FrozenMap, artifact_wire, seal_envelope, sha256_json, strict_fields, timestamp
from atlas.v2.contracts import (
    ArtifactEnvelope,
    CandidateActionV2,
    EligibilityStatusV2,
    FeatureArtifactV2,
    OpportunityWatchV2,
    PolicySpecV2,
    V2Side,
    WatchStateV2,
)
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.health import PublicSourceHealthV2, PublicSourceStateV2
from atlas.v2.data.raw import AvailabilityClassV2
from atlas.v2.features.joins import JoinedBars
from atlas.v2.instruments import InstrumentKeyV2, UniverseContractV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.strategies.s1_trend import EventGate, EventState, ExecutableQuote

POLICY_ID = "S3_VWAP_STAT_MEAN_REVERSION"
POLICY_VERSION = "1.0.0-shadow-research"
PRODUCER_VERSION = "S3_MEAN_REVERSION_V1"
MINUTE_NS = BarIntervalV2.M1.duration_ns
FIVE_MINUTE_NS = BarIntervalV2.M5.duration_ns
FIFTEEN_MINUTE_NS = BarIntervalV2.M15.duration_ns
DAY_NS = 86_400_000_000_000
AR_OBSERVATION_COUNT = 7 * 24 * 60 + 1
STANDARDIZATION_COUNT = 120
BBO_MAX_AGE_NS = 1_000_000_000
SOURCE_HEALTH_MAX_AGE_NS = 60_000_000_000
COLLAR_BPS = 5
MAX_HOLD_NS = 60 * MINUTE_NS


def policy_spec() -> PolicySpecV2:
    """Return the frozen initial S3 engineering/research policy identity."""
    return PolicySpecV2.build(
        policy_id=POLICY_ID,
        version=POLICY_VERSION,
        strategy_family="VWAP_STATISTICAL_MEAN_REVERSION",
        capital_status="SHADOW_ONLY",
        decision_event="CONFIRMED_1M_CLOSE",
        required_features=tuple(sorted((
            "causal_trade_vwap_utc_day",
            "causal_trade_stream",
            "fresh_executable_bbo",
            "completed_1m_residuals",
            "completed_15m_context",
            "m15.adx14",
            "m15.realized_variance20",
            "m15.atr14",
            "latest_confirmed_4h_trend",
            "event_safety_gate_v2",
            "point_in_time_universe_eligibility",
            "bar_source_health",
            "trade_source_health",
        ))),
        optional_features=(),
        timeframe_rules=FrozenMap({
            "bar_interval": "1M",
            "context_intervals": ["15M", "4H"],
            "bar_boundary": "UTC_HALF_OPEN_COMPLETED_ONLY",
            "identity": "FULL_InstrumentKeyV2",
            "availability": "ACTUAL_SYSTEM_OR_RECONSTRUCTED_MARKET_AS_EXPLICITLY_TAGGED",
        }),
        setup_parameters=FrozenMap({
            "residual": "ln(completed_or_cutoff_price)-ln(causal_UTC_day_trade_VWAP)",
            "standardization": "current_cutoff_midpoint_residual_minus_mean_of_the_120_completed_residuals_strictly_before_the_latest_completed_1M_bar_divided_by_sample_sd_of_those_120",
            "standardization_count": STANDARDIZATION_COUNT,
            "ar_window": "trailing_7_days_of_completed_1M_residuals",
            "ar_observation_count": AR_OBSERVATION_COUNT,
            "ar_fit": "OLS(r_t=alpha+phi*r_(t-1)+epsilon), contiguous 1M observations, no future samples",
            "phi_open_interval": [0, 1],
            "half_life_minutes_inclusive": [5, 30],
            "watch_threshold": "strict_abs_z_gt_2",
            "strong_trend_rejection": "cutoff_known_ADX14_gt_25",
            "vwap_source": "causal_available_public_trades_only; never candle_OHLCV",
        }),
        direction_rule=FrozenMap({"positive_residual": "SHORT", "negative_residual": "LONG"}),
        entry_rule=FrozenMap({
            "trigger": "subsequent_completed_1M_residual_moves_measurably_toward_frozen_UTC_day_VWAP",
            "price_source": "fresh_executable_BBO",
            "time_in_force": "IOC",
            "research_only": True,
        }),
        collar_rule=FrozenMap({
            "adverse_bps": COLLAR_BPS,
            "status": "ENGINEERING_DEFAULT_NOT_ECONOMICALLY_VALIDATED",
            "long_rounding": "down_to_tick",
            "short_rounding": "up_to_tick",
        }),
        stop_rule=FrozenMap({
            "long": "frozen_entry_time_VWAP*exp(-one_residual_sigma), rounded down to tick",
            "short": "frozen_entry_time_VWAP*exp(+one_residual_sigma), rounded up to tick",
            "reject": "nonpositive, wrong_side, invalid, or effective_distance_less_than_one_tick",
        }),
        trigger_basis="COMPLETED_1M_CLOSE",
        management_rule=FrozenMap({"no_resize": True, "no_target_move": True, "no_size_or_capital_authority": True}),
        time_exit_rule=FrozenMap({"max_hold_ns": MAX_HOLD_NS, "type": "TIME_EXIT"}),
        max_hold_ns=MAX_HOLD_NS,
        expiry_rule=FrozenMap({"watch_max_age_ns": MAX_HOLD_NS, "duplicate_trigger": "IDEMPOTENT"}),
        model_requirements=(),
    )


S3_POLICY = policy_spec()


@dataclass(frozen=True)
class CausalTradeV2:
    """A public trade with exact identity and actual causal timestamps."""

    key: InstrumentKeyV2
    raw_observation_ref: str
    source_id: str
    trade_id: str
    event_at_ns: int
    received_at_ns: int
    available_at_ns: int
    price: Decimal
    quantity: Decimal
    aggressor_side: str | None = None
    availability_class: AvailabilityClassV2 = AvailabilityClassV2.ACTUAL_SYSTEM
    replay_available_at_ns: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.key, InstrumentKeyV2):
            raise ValueError("trade requires full InstrumentKeyV2")
        timestamp(self.event_at_ns, field="trade.event_at_ns")
        timestamp(self.received_at_ns, field="trade.received_at_ns")
        timestamp(self.available_at_ns, field="trade.available_at_ns")
        if not self.event_at_ns <= self.received_at_ns <= self.available_at_ns:
            raise ValueError("trade event, receipt and availability chronology is invalid")
        if not self.source_id or not self.trade_id:
            raise ValueError("trade source and raw identity are required")
        if len(self.raw_observation_ref) != 64:
            raise ValueError("raw_observation_ref must be a SHA-256 reference")
        object.__setattr__(self, "price", Decimal(self.price))
        object.__setattr__(self, "quantity", Decimal(self.quantity))
        if not self.price.is_finite() or self.price <= 0 or not self.quantity.is_finite() or self.quantity <= 0:
            raise ValueError("trade price and quantity must be positive and finite")
        if self.aggressor_side not in (None, "BUY", "SELL", "UNKNOWN"):
            raise ValueError("aggressor side must preserve the source's known/unknown semantics")
        object.__setattr__(self, "availability_class", AvailabilityClassV2(self.availability_class))
        if self.availability_class == AvailabilityClassV2.ACTUAL_SYSTEM:
            if self.replay_available_at_ns is not None:
                raise ValueError("actual trade evidence cannot carry reconstructed availability")
        elif self.availability_class == AvailabilityClassV2.RECONSTRUCTED_MARKET:
            if self.replay_available_at_ns is None:
                raise ValueError("reconstructed trades require replay availability")
            timestamp(self.replay_available_at_ns, field="trade.replay_available_at_ns")
            if self.replay_available_at_ns < self.event_at_ns:
                raise ValueError("reconstructed trade cannot be available before its event")
        else:
            raise ValueError("S3 trades require actual or reconstructed availability")

    def effective_available_at(self, replay_view: str) -> int | None:
        return self.available_at_ns if replay_view == "ACTUAL_SYSTEM" else self.replay_available_at_ns

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "key": self.key.to_dict(),
            "raw_observation_ref": self.raw_observation_ref,
            "source_id": self.source_id,
            "trade_id": self.trade_id,
            "event_at_ns": self.event_at_ns,
            "received_at_ns": self.received_at_ns,
            "available_at_ns": self.available_at_ns,
            "price": str(self.price),
            "quantity": str(self.quantity),
            "aggressor_side": self.aggressor_side,
            "availability_class": self.availability_class.value,
            "replay_available_at_ns": self.replay_available_at_ns,
        }


@dataclass(frozen=True)
class TradeVwapSnapshotV2:
    key: InstrumentKeyV2
    utc_day_start_ns: int
    information_cutoff_ns: int
    available_at_ns: int
    vwap: Decimal
    trade_refs: tuple[str, ...]
    source_health_ref: str
    replay_view: str = "ACTUAL_SYSTEM"

    def __post_init__(self) -> None:
        timestamp(self.utc_day_start_ns, field="vwap.utc_day_start_ns")
        timestamp(self.information_cutoff_ns, field="vwap.information_cutoff_ns")
        timestamp(self.available_at_ns, field="vwap.available_at_ns")
        if self.utc_day_start_ns % DAY_NS or self.utc_day_start_ns > self.information_cutoff_ns:
            raise ValueError("VWAP snapshot must bind the current UTC day")
        object.__setattr__(self, "vwap", Decimal(self.vwap))
        if not self.vwap.is_finite() or self.vwap <= 0:
            raise ValueError("VWAP must be positive and finite")
        refs = tuple(sorted(set(self.trade_refs)))
        if not refs or len(refs) != len(self.trade_refs):
            raise ValueError("VWAP requires unique causal trade refs")
        object.__setattr__(self, "trade_refs", refs)
        if not self.source_health_ref:
            raise ValueError("VWAP requires source-health evidence")
        if self.replay_view not in ("ACTUAL_SYSTEM", "RECONSTRUCTED_MARKET"):
            raise ValueError("unknown VWAP replay view")

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "key": self.key.to_dict(),
            "utc_day_start_ns": self.utc_day_start_ns,
            "information_cutoff_ns": self.information_cutoff_ns,
            "available_at_ns": self.available_at_ns,
            "vwap": str(self.vwap),
            "trade_refs": list(self.trade_refs),
            "source_health_ref": self.source_health_ref,
            "replay_view": self.replay_view,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> TradeVwapSnapshotV2:
        """Decode persisted S3 VWAP evidence without accepting extra fields."""
        fields = {
            "schema_version", "key", "utc_day_start_ns", "information_cutoff_ns", "available_at_ns",
            "vwap", "trade_refs", "source_health_ref", "replay_view",
        }
        value = strict_fields(data, expected=fields, required=fields, name=cls.__name__)
        if type(value["schema_version"]) is not int or value["schema_version"] != 1:
            raise ValueError("unsupported TradeVwapSnapshotV2 schema_version")
        if not isinstance(value["key"], Mapping):
            raise ValueError("TradeVwapSnapshotV2 key must be an object")
        if not isinstance(value["vwap"], str) or not isinstance(value["trade_refs"], (list, tuple)):
            raise ValueError("TradeVwapSnapshotV2 decimal and trade refs have invalid wire types")
        if not isinstance(value["source_health_ref"], str) or not isinstance(value["replay_view"], str):
            raise ValueError("TradeVwapSnapshotV2 source/view have invalid wire types")
        return cls(
            InstrumentKeyV2.from_dict(value["key"]),
            value["utc_day_start_ns"], value["information_cutoff_ns"], value["available_at_ns"],
            Decimal(value["vwap"]), tuple(value["trade_refs"]), value["source_health_ref"], value["replay_view"],
        )


def utc_day_trade_vwap(
    trades: Sequence[CausalTradeV2], *, key: InstrumentKeyV2, cutoff_ns: int,
    source_health: PublicSourceHealthV2, replay_view: str = "ACTUAL_SYSTEM",
) -> TradeVwapSnapshotV2 | None:
    """Compute VWAP only from cutoff-available public trades, never candle volume."""
    timestamp(cutoff_ns, field="cutoff_ns")
    if source_health.state != PublicSourceStateV2.HEALTHY_CURRENT or source_health.available_at_ns > cutoff_ns:
        return None
    if cutoff_ns - source_health.available_at_ns > SOURCE_HEALTH_MAX_AGE_NS:
        return None
    day_start = cutoff_ns - cutoff_ns % DAY_NS
    causal_trades: list[CausalTradeV2] = []
    for item in trades:
        effective_available = item.effective_available_at(replay_view)
        if (item.key == key and item.event_at_ns >= day_start and item.event_at_ns <= cutoff_ns
                and item.availability_class.value == replay_view and effective_available is not None
                and effective_available <= cutoff_ns
                and (replay_view != "ACTUAL_SYSTEM" or item.received_at_ns <= cutoff_ns)):
            causal_trades.append(item)
    selected = tuple(sorted(causal_trades,
                            key=lambda item: (item.event_at_ns, item.trade_id, item.raw_observation_ref)))
    if not selected:
        return None
    unique: dict[tuple[str, str], CausalTradeV2] = {}
    for trade in selected:
        identity = (trade.source_id, trade.trade_id)
        prior = unique.get(identity)
        if prior is not None and prior.content_hash != trade.content_hash:
            return None
        unique[identity] = trade
    rows = tuple(unique.values())
    total_qty = sum((trade.quantity for trade in rows), Decimal(0))
    if total_qty <= 0:
        return None
    vwap = sum((trade.price * trade.quantity for trade in rows), Decimal(0)) / total_qty
    return TradeVwapSnapshotV2(
        key, day_start, cutoff_ns, max(item.effective_available_at(replay_view) or 0 for item in rows), vwap,
        tuple(item.raw_observation_ref for item in rows), source_health.content_hash, replay_view,
    )


@dataclass(frozen=True)
class ResidualObservationV2:
    key: InstrumentKeyV2
    bar_ref: str
    vwap_ref: str
    close_at_ns: int
    available_at_ns: int
    residual: float
    replay_view: str

    def __post_init__(self) -> None:
        timestamp(self.close_at_ns, field="residual.close_at_ns")
        timestamp(self.available_at_ns, field="residual.available_at_ns")
        if self.available_at_ns < self.close_at_ns:
            raise ValueError("completed residual cannot be available before bar close")
        if self.replay_view not in ("ACTUAL_SYSTEM", "RECONSTRUCTED_MARKET"):
            raise ValueError("unknown residual replay view")
        if not math.isfinite(self.residual):
            raise ValueError("residual must be finite")
        if len(self.bar_ref) != 64 or len(self.vwap_ref) != 64:
            raise ValueError("residual must bind exact bar and trade-VWAP refs")

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1, "key": self.key.to_dict(), "bar_ref": self.bar_ref,
            "vwap_ref": self.vwap_ref, "close_at_ns": self.close_at_ns,
            "available_at_ns": self.available_at_ns, "residual": self.residual,
            "replay_view": self.replay_view,
        }


def residual_observation(bar: CausalBarV2, vwap: TradeVwapSnapshotV2) -> ResidualObservationV2:
    if (not bar.final or bar.interval != BarIntervalV2.M1
            or bar.instrument_revision != vwap.key.contract_revision
            or bar.raw.availability_class.value != vwap.replay_view):
        raise ValueError("S3 residual requires the exact completed 1M bar and instrument revision")
    if vwap.information_cutoff_ns != bar.close_at_ns:
        raise ValueError("VWAP snapshot information cutoff must equal the completed bar close")
    bar_available = bar.raw.available_at_ns if vwap.replay_view == "ACTUAL_SYSTEM" else bar.replay_available_at_ns
    if bar_available is None:
        raise ValueError("bar is unavailable in the requested residual replay view")
    return ResidualObservationV2(
        vwap.key, bar.content_hash, vwap.content_hash, bar.close_at_ns,
        max(bar_available, vwap.available_at_ns),
        math.log(float(bar.close)) - math.log(float(vwap.vwap)), vwap.replay_view,
    )


def validate_historical_residual_v2(
    residual: ResidualObservationV2, *, key: InstrumentKeyV2, bar: CausalBarV2,
    vwap: TradeVwapSnapshotV2, replay_view: str,
) -> str | None:
    """Validate a historical residual against its exact selected causal inputs."""
    if (not bar.final or bar.interval != BarIntervalV2.M1 or bar.content_hash != residual.bar_ref
            or residual.close_at_ns != bar.close_at_ns or bar.instrument_revision != key.contract_revision
            or residual.key != key):
        return "HISTORICAL_RESIDUAL_BAR_REF_MISMATCH"
    if residual.replay_view != replay_view or bar.raw.availability_class.value != replay_view:
        return "HISTORICAL_RESIDUAL_VIEW_MISMATCH"
    bar_available = (bar.raw.available_at_ns if replay_view == "ACTUAL_SYSTEM" else bar.replay_available_at_ns)
    if bar_available is None:
        return "HISTORICAL_RESIDUAL_BAR_AVAILABILITY_MISSING"
    if vwap.key != key or vwap.replay_view != replay_view:
        return "HISTORICAL_VWAP_IDENTITY_OR_VIEW_MISMATCH"
    if (bar_available > residual.available_at_ns or vwap.available_at_ns > residual.available_at_ns
            or residual.available_at_ns < max(bar_available, vwap.available_at_ns)):
        return "HISTORICAL_RESIDUAL_AVAILABILITY_PRECEDES_INPUT"
    if vwap.information_cutoff_ns != bar.close_at_ns:
        return "HISTORICAL_VWAP_NOT_CAUSAL_FOR_RESIDUAL"
    derived = math.log(float(bar.close)) - math.log(float(vwap.vwap))
    if residual.residual != derived:
        return "HISTORICAL_RESIDUAL_VALUE_MISMATCH"
    return None


def persist_trade_vwap_v2(repository: OpsRepository, snapshot: TradeVwapSnapshotV2) -> None:
    """Index an immutable causal trade-VWAP snapshot in the existing ops writer."""
    _index(repository, snapshot.content_hash, "S3TradeVwapSnapshotV2", snapshot.available_at_ns,
           {"vwap": snapshot.to_dict()})


def standardized_current(history: Sequence[float], current: float) -> tuple[float, float, float]:
    """Standardize against exactly the preceding 120 completed observations."""
    if len(history) != STANDARDIZATION_COUNT or not math.isfinite(current) or not all(math.isfinite(x) for x in history):
        raise ValueError("S3 standardization requires exactly 120 finite preceding observations")
    center = mean(history)
    sigma = stdev(history)
    if not math.isfinite(sigma) or sigma <= 0:
        raise ValueError("S3 residual standard deviation is invalid")
    return (current - center) / sigma, center, sigma


def preceding_standardization_residuals(sample: Sequence[ResidualObservationV2]) -> tuple[float, ...]:
    """Return exactly the 120 completed residuals before the latest completed bar."""
    if len(sample) < STANDARDIZATION_COUNT + 1:
        raise ValueError("S3 preceding standardization window requires 121 completed residuals")
    return tuple(item.residual for item in sample[-STANDARDIZATION_COUNT - 1:-1])


def deviation_exceeds_watch_threshold(z_score: float) -> bool:
    """Frozen initial setup boundary: watches require a strict two-sigma exceedance."""
    if not math.isfinite(z_score):
        return False
    return abs(z_score) > 2.0


def strong_trend_rejected(adx14: float) -> bool:
    """The frozen S3 trend rejection is strictly ADX14 greater than 25."""
    return math.isfinite(adx14) and adx14 > 25.0


def moves_toward_frozen_vwap(initial_residual: float, subsequent_residual: float) -> bool:
    """A later completed bar must strictly reduce the absolute frozen-VWAP residual."""
    return (math.isfinite(initial_residual) and math.isfinite(subsequent_residual)
            and abs(subsequent_residual) < abs(initial_residual))


def fit_ar1(residuals: Sequence[float]) -> tuple[float, float, float]:
    """OLS r_t=alpha+phi*r_(t-1)+epsilon; input is an ordered causal prefix."""
    if len(residuals) < 3 or not all(math.isfinite(x) for x in residuals):
        raise ValueError("invalid AR(1) residual sample")
    prior = tuple(residuals[:-1])
    current = tuple(residuals[1:])
    mean_x, mean_y = mean(prior), mean(current)
    denominator = math.fsum((x - mean_x) ** 2 for x in prior)
    if denominator <= 0 or not math.isfinite(denominator):
        raise ValueError("invalid AR(1) predictor variance")
    phi = math.fsum((x - mean_x) * (y - mean_y) for x, y in zip(prior, current, strict=True)) / denominator
    alpha = mean_y - phi * mean_x
    if not math.isfinite(phi) or not math.isfinite(alpha) or not 0 < phi < 1:
        raise ValueError("AR(1) phi is outside the required open interval")
    half_life = -math.log(2.0) / math.log(phi)
    if not math.isfinite(half_life):
        raise ValueError("AR(1) half-life is invalid")
    return alpha, phi, half_life


def round_stop(vwap: Decimal, sigma: float, side: V2Side, tick: Decimal) -> Decimal:
    if not math.isfinite(sigma) or sigma <= 0 or tick <= 0:
        raise ValueError("stop inputs must be positive and finite")
    raw = vwap * Decimal(str(math.exp(-sigma if side == V2Side.LONG else sigma)))
    if raw <= 0 or not raw.is_finite():
        raise ValueError("stop conversion produced invalid price")
    rounding = ROUND_DOWN if side == V2Side.LONG else ROUND_UP
    rounded = (raw / tick).to_integral_value(rounding=rounding) * tick
    if rounded <= 0:
        raise ValueError("rounded stop is nonpositive")
    return rounded


def effective_stop_valid(entry: Decimal, stop: Decimal, tick: Decimal, side: V2Side) -> bool:
    """Reject invalid or sub-tick adverse stop distance after conservative rounding."""
    if not all(value.is_finite() and value > 0 for value in (entry, stop, tick)):
        return False
    distance = entry - stop if side == V2Side.LONG else stop - entry
    return distance >= tick


@dataclass(frozen=True)
class S3MeanReversionStateV2:
    envelope: ArtifactEnvelope
    key: InstrumentKeyV2
    policy_hash: str
    cutoff_ns: int
    status: str
    reason: str | None
    residual: float | None
    z_score: float | None
    residual_sigma: float | None
    ar_alpha: float | None
    ar_phi: float | None
    half_life_minutes: float | None
    frozen_vwap: str | None
    replay_view: str

    ARTIFACT_TYPE = "S3MeanReversionStateV2"

    def __post_init__(self) -> None:
        timestamp(self.cutoff_ns, field="S3.cutoff_ns")
        if self.replay_view not in ("ACTUAL_SYSTEM", "RECONSTRUCTED_MARKET"):
            raise ValueError("unknown S3 replay view")
        if self.status not in ("WATCH", "TRIGGERED", "NO_CANDIDATE", "NOT_ESTIMABLE"):
            raise ValueError("unknown S3 state status")
        for name in ("residual", "z_score", "residual_sigma", "ar_alpha", "ar_phi", "half_life_minutes"):
            value = getattr(self, name)
            if value is not None and not math.isfinite(value):
                raise ValueError(f"S3 {name} must be finite or unavailable")
        object.__setattr__(self, "envelope", seal_envelope(self.envelope, self._body(), artifact_type=self.ARTIFACT_TYPE))

    def _body(self) -> dict[str, object]:
        return {
            "key": self.key.to_dict(), "policy_hash": self.policy_hash, "cutoff_ns": self.cutoff_ns,
            "status": self.status, "reason": self.reason, "residual": self.residual,
            "z_score": self.z_score, "residual_sigma": self.residual_sigma,
            "ar_alpha": self.ar_alpha, "ar_phi": self.ar_phi,
            "half_life_minutes": self.half_life_minutes, "frozen_vwap": self.frozen_vwap,
            "replay_view": self.replay_view,
        }

    @property
    def content_hash(self) -> str:
        return self.envelope.content_hash

    def to_dict(self) -> dict[str, object]:
        return artifact_wire(self.envelope, self._body(), artifact_type=self.ARTIFACT_TYPE)


@dataclass(frozen=True)
class S3DecisionV2:
    status: str
    reason: str
    state: S3MeanReversionStateV2
    watch: OpportunityWatchV2 | None = None
    candidate: CandidateActionV2 | None = None


def _index(repository: OpsRepository, ref: str, kind: str, at_ns: int, metadata: dict[str, object]) -> None:
    repository.register_artifact(ArtifactIndexEntryV2(ref, kind, ref, at_ns, at_ns, metadata))


def _utc_day_start(at_ns: int) -> int:
    return at_ns - at_ns % DAY_NS


def _bar_available_at_view(bar: CausalBarV2, cutoff_ns: int, replay_view: str) -> bool:
    if bar.raw.availability_class.value != replay_view or bar.close_at_ns > cutoff_ns:
        return False
    available = bar.raw.available_at_ns if replay_view == "ACTUAL_SYSTEM" else bar.replay_available_at_ns
    return available is not None and available <= cutoff_ns


def select_causal_setup_prefix_v2(
    *, key: InstrumentKeyV2, cutoff_ns: int, replay_view: str,
    completed_1m: Sequence[CausalBarV2], residuals: Sequence[ResidualObservationV2],
) -> tuple[tuple[CausalBarV2, ...], tuple[ResidualObservationV2, ...]]:
    """Select the latest bar and residual revisions known in one replay view."""
    eligible_bars = tuple(
        bar for bar in completed_1m
        if bar.final and bar.interval == BarIntervalV2.M1 and bar.instrument_revision == key.contract_revision
        and _bar_available_at_view(bar, cutoff_ns, replay_view)
    )
    by_open: dict[int, CausalBarV2] = {}
    for bar in eligible_bars:
        available = bar.raw.available_at_ns if replay_view == "ACTUAL_SYSTEM" else bar.replay_available_at_ns
        previous_bar = by_open.get(bar.open_at_ns)
        previous_available = (
            previous_bar.raw.available_at_ns if previous_bar is not None and replay_view == "ACTUAL_SYSTEM"
            else previous_bar.replay_available_at_ns if previous_bar is not None else None
        )
        if (previous_bar is None
                or (available, bar.raw.record_id) > (previous_available, previous_bar.raw.record_id)):
            by_open[bar.open_at_ns] = bar
    causal_bars = tuple(sorted(by_open.values(), key=lambda bar: (bar.close_at_ns, bar.content_hash)))
    causal_bar_refs = {bar.content_hash for bar in causal_bars}
    residual_by_bar: dict[str, ResidualObservationV2] = {}
    for item in residuals:
        if (item.key != key or item.bar_ref not in causal_bar_refs or item.available_at_ns > cutoff_ns
                or item.close_at_ns > cutoff_ns or item.replay_view != replay_view):
            continue
        previous_residual = residual_by_bar.get(item.bar_ref)
        if (previous_residual is None
                or (item.available_at_ns, item.content_hash) >
                (previous_residual.available_at_ns, previous_residual.content_hash)):
            residual_by_bar[item.bar_ref] = item
    causal_residuals = tuple(sorted(residual_by_bar.values(), key=lambda item: (item.close_at_ns, item.bar_ref)))
    return causal_bars, causal_residuals


class S3ShadowCoordinator:
    def __init__(self, repository: OpsRepository, *, policy: PolicySpecV2 = S3_POLICY,
                 cost_model_ref: str = "S3_SHADOW_COST_UNESTIMATED_V1") -> None:
        if policy.policy_hash != S3_POLICY.policy_hash:
            raise ValueError("S3 coordinator requires the versioned S3 policy")
        self.repository = repository
        self.policy = policy
        self.cost_model_ref = cost_model_ref

    def _persist_state(self, state: S3MeanReversionStateV2) -> None:
        _index(self.repository, state.content_hash, state.ARTIFACT_TYPE, state.cutoff_ns, {"state": state.to_dict()})

    def _state(self, *, key: InstrumentKeyV2, cutoff_ns: int, status: str, reason: str | None,
               refs: Sequence[str], residual: float | None = None, z_score: float | None = None,
               sigma: float | None = None, alpha: float | None = None, phi: float | None = None,
               half_life: float | None = None, vwap: Decimal | None = None,
               replay_view: str = "ACTUAL_SYSTEM") -> S3MeanReversionStateV2:
        ordered_refs = tuple(sorted(set(refs)))
        artifact_id = sha256_json({"policy_hash": self.policy.policy_hash, "key": key.to_dict(),
                                   "cutoff_ns": cutoff_ns, "status": status, "reason": reason,
                                   "inputs": ordered_refs, "residual": residual, "z_score": z_score,
                                   "vwap": str(vwap) if vwap is not None else None})
        envelope = ArtifactEnvelope(1, artifact_id, cutoff_ns, cutoff_ns, PRODUCER_VERSION, ordered_refs)
        state = S3MeanReversionStateV2(envelope, key, self.policy.policy_hash, cutoff_ns, status, reason,
                                       residual, z_score, sigma, alpha, phi, half_life,
                                       str(vwap) if vwap is not None else None, replay_view)
        self._persist_state(state)
        return state

    @staticmethod
    def _source_health_ok(health: PublicSourceHealthV2 | None, cutoff_ns: int) -> bool:
        return bool(health is not None and health.state == PublicSourceStateV2.HEALTHY_CURRENT
                    and health.available_at_ns <= cutoff_ns
                    and health.observed_at_ns <= cutoff_ns
                    and cutoff_ns - health.observed_at_ns <= SOURCE_HEALTH_MAX_AGE_NS
                    and cutoff_ns - health.available_at_ns <= SOURCE_HEALTH_MAX_AGE_NS)

    def _event_gate_is_current_s7_projection(self, gate: EventGate | None, cutoff_ns: int) -> bool:
        """Accept only the exact-cutoff projection of a persisted S7 gate artifact."""
        if (gate is None or gate.state == EventState.UNKNOWN or gate.available_at_ns != cutoff_ns
                or not gate.valid_at(cutoff_ns)):
            return False
        try:
            indexed = self.repository.get_artifact(gate.evidence_ref)
        except ValueError:
            return False
        body = indexed.metadata.get("gate") if indexed is not None else None
        return bool(
            indexed is not None
            and indexed.artifact_type == "EventSafetyGateV2"
            and indexed.content_hash == gate.evidence_ref
            and indexed.available_at_ns == cutoff_ns
            and isinstance(body, Mapping)
            and body.get("cutoff_ns") == cutoff_ns
            and body.get("state") == gate.state.value
            and body.get("blocked") == (gate.state == EventState.BLOCKED)
            and body.get("gate_version") == gate.version
        )

    def evaluate_setup(
        self, *, key: InstrumentKeyV2, cutoff_ns: int, residuals: Sequence[ResidualObservationV2],
        current_vwap: TradeVwapSnapshotV2 | None, trades: Sequence[CausalTradeV2],
        completed_1m: Sequence[CausalBarV2], context: JoinedBars, feature: FeatureArtifactV2,
        quote: ExecutableQuote | None, tick_size: Decimal, universe: UniverseContractV2,
        event_gate: EventGate | None, bar_health: PublicSourceHealthV2 | None,
        trade_health: PublicSourceHealthV2 | None,
        trade_completeness_proven: bool,
    ) -> S3DecisionV2:
        timestamp(cutoff_ns, field="S3.cutoff_ns")
        if type(trade_completeness_proven) is not bool:
            raise ValueError("S3 trade completeness qualification must be bool")
        replay_view = feature.replay_view.value
        # Filter by availability before choosing a revision so future corrections
        # cannot rewrite an already emitted setup.
        causal_bars, causal_residuals = select_causal_setup_prefix_v2(
            key=key, cutoff_ns=cutoff_ns, replay_view=replay_view,
            completed_1m=completed_1m, residuals=residuals,
        )
        refs = [item.content_hash for item in causal_residuals[-AR_OBSERVATION_COUNT:]]
        refs.extend(item.vwap_ref for item in causal_residuals[-AR_OBSERVATION_COUNT:])
        for trade in trades:
            effective_available = trade.effective_available_at(feature.replay_view.value)
            if trade.key == key and effective_available is not None and effective_available <= cutoff_ns:
                refs.append(trade.raw_observation_ref)
        refs.extend(item.content_hash for item in causal_bars[-AR_OBSERVATION_COUNT:])
        refs.extend((feature.content_hash, universe.content_hash))
        if current_vwap is not None:
            refs.extend((current_vwap.content_hash, *current_vwap.trade_refs, current_vwap.source_health_ref))
        if quote is not None:
            refs.append(quote.evidence_ref)
        if event_gate is not None:
            refs.append(event_gate.evidence_ref)
        for health in (bar_health, trade_health):
            if health is not None:
                refs.append(health.content_hash)

        def fail(reason: str, *, residual: float | None = None, z: float | None = None,
                 sigma: float | None = None, alpha: float | None = None, phi: float | None = None,
                 half: float | None = None, vwap: Decimal | None = None) -> S3DecisionV2:
            state = self._state(key=key, cutoff_ns=cutoff_ns, status="NOT_ESTIMABLE", reason=reason,
                                refs=refs, residual=residual, z_score=z, sigma=sigma, alpha=alpha,
                                phi=phi, half_life=half, vwap=vwap,
                                replay_view=feature.replay_view.value)
            return S3DecisionV2("NOT_ESTIMABLE", reason, state)

        if context.status != "AVAILABLE" or context.key != key or context.cutoff_ns != cutoff_ns:
            return fail(context.reason or "CONTEXT_NOT_AVAILABLE_AT_CUTOFF")
        if feature.key != key or feature.information_cutoff_ns != cutoff_ns or feature.envelope.available_at_ns > cutoff_ns:
            return fail("FEATURE_NOT_CAUSAL_FOR_CUTOFF")
        if feature.replay_view.value not in ("ACTUAL_SYSTEM", "RECONSTRUCTED_MARKET"):
            return fail("AVAILABILITY_VIEW_UNKNOWN")
        universe_entries = [item for item in universe.entries if item.key == key]
        universe_eligibility = (
            universe_entries[0].strategy_eligibility.get(POLICY_ID) if len(universe_entries) == 1 else None
        )
        if (universe.envelope.available_at_ns > cutoff_ns or cutoff_ns > universe.decision_slot_ns
                or len(universe_entries) != 1 or not universe_entries[0].data_eligible
                or universe_entries[0].capital_eligible
                or universe_eligibility is None
                or universe_eligibility.status != EligibilityStatusV2.ELIGIBLE):
            return fail("POINT_IN_TIME_UNIVERSE_INELIGIBLE")
        if bar_health is None or not self._source_health_ok(bar_health, cutoff_ns):
            return fail("BAR_SOURCE_HEALTH_STALE_OR_UNAVAILABLE")
        if trade_health is None or not self._source_health_ok(trade_health, cutoff_ns):
            return fail("TRADE_SOURCE_HEALTH_STALE_OR_UNAVAILABLE")
        if any(bar.raw.source_id != bar_health.source_id for bar in causal_bars[-AR_OBSERVATION_COUNT:]):
            return fail("BAR_SOURCE_HEALTH_IDENTITY_MISMATCH")
        for trade in trades:
            effective_available = trade.effective_available_at(replay_view)
            if (trade.key == key and effective_available is not None and effective_available <= cutoff_ns
                    and trade.source_id != trade_health.source_id):
                return fail("TRADE_SOURCE_HEALTH_IDENTITY_MISMATCH")
        if event_gate is None or not self._event_gate_is_current_s7_projection(event_gate, cutoff_ns):
            return fail("EVENT_GATE_UNKNOWN_BLOCKED")
        if event_gate.state == EventState.BLOCKED:
            state = self._state(key=key, cutoff_ns=cutoff_ns, status="NO_CANDIDATE", reason="EVENT_GATE_BLOCKED",
                                refs=refs, replay_view=feature.replay_view.value)
            return S3DecisionV2("NO_CANDIDATE", "EVENT_GATE_BLOCKED", state)
        if not trade_completeness_proven:
            return fail("TRADE_COMPLETENESS_UNPROVEN")
        if quote is None or quote.key != key or not quote.valid_at(cutoff_ns, BBO_MAX_AGE_NS):
            return fail("BBO_STALE_OR_UNAVAILABLE")
        if not context.h4 or not context.m15:
            return fail("MISSING_CONFIRMED_4H_OR_15M_CONTEXT")
        if not {context.h4[-1].content_hash, context.m15[-1].content_hash}.issubset(feature.envelope.input_refs):
            return fail("FEATURE_CONTEXT_REFS_MISSING")
        adx_value = feature.values.get("m15.adx14")
        atr_value = feature.values.get("m15.atr14")
        vol_value = feature.values.get("m15.realized_variance20")
        trend_value = feature.values.get("regime.trend_state")
        if (adx_value is None or adx_value.value is None or atr_value is None or atr_value.value is None
                or vol_value is None or vol_value.value is None or trend_value is None or trend_value.value is None):
            return fail("MISSING_OR_INVALID_VOLATILITY_OR_4H_TREND")
        if not all(math.isfinite(float(item.value)) for item in (adx_value, atr_value, vol_value, trend_value)):
            return fail("MISSING_OR_INVALID_VOLATILITY_OR_4H_TREND")
        if float(atr_value.value) <= 0 or float(vol_value.value) <= 0:
            return fail("MISSING_OR_INVALID_VOLATILITY_OR_4H_TREND")
        if strong_trend_rejected(float(adx_value.value)):
            state = self._state(key=key, cutoff_ns=cutoff_ns, status="NO_CANDIDATE", reason="STRONG_TREND_ADX14_GT_25",
                                refs=refs, replay_view=feature.replay_view.value)
            return S3DecisionV2("NO_CANDIDATE", "STRONG_TREND_ADX14_GT_25", state)
        if not causal_bars:
            return fail("INSUFFICIENT_1M_HISTORY")
        if len(causal_bars) < AR_OBSERVATION_COUNT:
            return fail("INSUFFICIENT_1M_HISTORY")
        bars = causal_bars[-AR_OBSERVATION_COUNT:]
        latest = bars[-1]
        latest_available = latest.raw.available_at_ns if feature.replay_view.value == "ACTUAL_SYSTEM" else latest.replay_available_at_ns
        if latest_available is None or latest.close_at_ns > cutoff_ns or latest_available > cutoff_ns:
            return fail("INSUFFICIENT_1M_HISTORY")
        if len(causal_residuals) < AR_OBSERVATION_COUNT:
            return fail("INCOMPLETE_7_DAY_AR_WINDOW")
        sample = tuple(causal_residuals[-AR_OBSERVATION_COUNT:])
        if (any(item.key != key or item.available_at_ns > cutoff_ns or item.replay_view != feature.replay_view.value for item in sample)
                or any(item.bar_ref != bar.content_hash for item, bar in zip(sample, bars, strict=True))
                or sample[-1].bar_ref != latest.content_hash or sample[-1].close_at_ns != latest.close_at_ns):
            return fail("INCOMPLETE_7_DAY_AR_WINDOW")
        selected_bars = {bar.content_hash: bar for bar in bars}
        persisted_vwaps = self.repository.get_artifact_metadata_by_refs(tuple(item.vwap_ref for item in sample))
        for item in sample:
            historical_vwap = persisted_vwaps.get(item.vwap_ref)
            if (historical_vwap is None
                    or historical_vwap.get("artifact_type") != "S3TradeVwapSnapshotV2"
                    or historical_vwap.get("content_hash") != item.vwap_ref
                    or not isinstance(historical_vwap.get("available_at_ns"), int)
                    or historical_vwap["available_at_ns"] > cutoff_ns):
                return fail("HISTORICAL_VWAP_EVIDENCE_UNAVAILABLE")
            snapshot_metadata = historical_vwap.get("metadata")
            snapshot_body = snapshot_metadata.get("vwap") if isinstance(snapshot_metadata, Mapping) else None
            if not isinstance(snapshot_body, Mapping):
                return fail("HISTORICAL_VWAP_EVIDENCE_INVALID")
            try:
                snapshot = TradeVwapSnapshotV2.from_dict(snapshot_body)
            except (TypeError, ValueError, ArithmeticError):
                return fail("HISTORICAL_VWAP_EVIDENCE_INVALID")
            if (snapshot.content_hash != item.vwap_ref
                    or historical_vwap["available_at_ns"] != snapshot.available_at_ns):
                return fail("HISTORICAL_VWAP_EVIDENCE_INVALID")
            reason = validate_historical_residual_v2(
                item, key=key, bar=selected_bars[item.bar_ref], vwap=snapshot, replay_view=replay_view,
            )
            if reason is not None:
                return fail(reason)
        if any(b.close_at_ns - a.close_at_ns != MINUTE_NS for a, b in zip(sample, sample[1:], strict=False)):
            return fail("INCOMPLETE_7_DAY_AR_WINDOW")
        if current_vwap is None:
            return fail("VWAP_UNAVAILABLE")
        if (current_vwap.key != key or current_vwap.information_cutoff_ns > cutoff_ns
                or current_vwap.available_at_ns > cutoff_ns or current_vwap.utc_day_start_ns != _utc_day_start(cutoff_ns)):
            return fail("VWAP_UNAVAILABLE")
        if current_vwap.replay_view != replay_view:
            return fail("VWAP_AVAILABILITY_VIEW_MISMATCH")
        recomputed_vwap = utc_day_trade_vwap(
            trades, key=key, cutoff_ns=cutoff_ns, source_health=trade_health,
            replay_view=feature.replay_view.value,
        )
        if recomputed_vwap is None:
            return fail("MISSING_CAUSAL_TRADES", vwap=current_vwap.vwap)
        if (recomputed_vwap.vwap != current_vwap.vwap
                or set(recomputed_vwap.trade_refs) != set(current_vwap.trade_refs)
                or current_vwap.source_health_ref != trade_health.content_hash):
            return fail("VWAP_UNAVAILABLE", vwap=current_vwap.vwap)
        for trade in trades:
            effective_available = trade.effective_available_at(feature.replay_view.value)
            if trade.key != key or effective_available is None or effective_available > cutoff_ns:
                continue
            indexed_trade = self.repository.get_artifact(trade.raw_observation_ref)
            replay_available = indexed_trade.metadata.get("replay_available_at_ns") if indexed_trade else None
            if indexed_trade is None or indexed_trade.metadata.get("source_id") != trade.source_id or (
                feature.replay_view.value == "ACTUAL_SYSTEM" and indexed_trade.available_at_ns > cutoff_ns
            ) or (
                feature.replay_view.value == "RECONSTRUCTED_MARKET" and replay_available != effective_available
            ):
                return fail("MISSING_CAUSAL_TRADES", vwap=current_vwap.vwap)
        persist_trade_vwap_v2(self.repository, current_vwap)
        if not quote.valid_at(cutoff_ns, BBO_MAX_AGE_NS):
            return fail("BBO_STALE_OR_UNAVAILABLE", vwap=current_vwap.vwap)
        try:
            alpha, phi, half_life = fit_ar1(tuple(item.residual for item in sample))
        except ValueError:
            return fail("INVALID_AR_FIT", vwap=current_vwap.vwap)
        if not 5.0 <= half_life <= 30.0:
            return fail("HALF_LIFE_OUTSIDE_5_TO_30_MINUTES", alpha=alpha, phi=phi, half=half_life,
                        vwap=current_vwap.vwap)
        current_residual = math.log(float((quote.bid + quote.ask) / 2)) - math.log(float(current_vwap.vwap))
        try:
            z_score, _, sigma = standardized_current(
                preceding_standardization_residuals(sample), current_residual
            )
        except ValueError:
            return fail("INVALID_120_OBSERVATION_STANDARDIZATION", alpha=alpha, phi=phi,
                        half=half_life, vwap=current_vwap.vwap)
        self.repository.register_artifacts(tuple(
            ArtifactIndexEntryV2(
                item.content_hash, "S3ResidualObservationV2", item.content_hash,
                item.available_at_ns, item.available_at_ns, {"residual": item.to_dict()},
            )
            for item in sample
        ))
        if not deviation_exceeds_watch_threshold(z_score):
            state = self._state(key=key, cutoff_ns=cutoff_ns, status="NO_CANDIDATE", reason="DEVIATION_NOT_BEYOND_2_SIGMA",
                                refs=refs, residual=current_residual, z_score=z_score, sigma=sigma,
                                alpha=alpha, phi=phi, half_life=half_life, vwap=current_vwap.vwap,
                                replay_view=feature.replay_view.value)
            return S3DecisionV2("NO_CANDIDATE", "DEVIATION_NOT_BEYOND_2_SIGMA", state)
        state = self._state(key=key, cutoff_ns=cutoff_ns, status="WATCH", reason="DEVIATION_BEYOND_2_SIGMA",
                            refs=refs, residual=current_residual, z_score=z_score, sigma=sigma,
                            alpha=alpha, phi=phi, half_life=half_life, vwap=current_vwap.vwap,
                            replay_view=feature.replay_view.value)
        _index(self.repository, self.policy.policy_hash, "PolicySpecV2", cutoff_ns, {"policy": self.policy.to_dict()})
        setup_ref = state.content_hash
        watch_id = sha256_json({"policy_hash": self.policy.policy_hash, "setup_ref": setup_ref,
                                "key": key.to_dict(), "cutoff_ns": cutoff_ns})
        watch = self.repository.get_watch(watch_id)
        if watch is None:
            created = OpportunityWatchV2(
                watch_id, key, POLICY_ID, POLICY_VERSION, self.policy.policy_hash, WatchStateV2.DETECTED, 0,
                cutoff_ns, cutoff_ns, setup_ref,
                tuple(sorted({setup_ref, *[item.content_hash for item in sample], current_vwap.content_hash,
                              feature.content_hash, current_vwap.source_health_ref, bar_health.content_hash,
                              trade_health.content_hash, event_gate.evidence_ref, quote.evidence_ref})),
                "BAR_CLOSE_1M", cutoff_ns + MAX_HOLD_NS, cutoff_ns,
            )
            self.repository.create_watch(created)
            watch = self.repository.transition_watch(
                watch_id, expected_state_version=0,
                event_id=sha256_json({"watch": watch_id, "event": "WAIT_FOR_1M"}),
                event_at_ns=cutoff_ns, transition_at_ns=cutoff_ns, target_state=WatchStateV2.WAITING_FOR_EVENT,
                outbox_id=sha256_json({"watch": watch_id, "outbox": "WAIT_FOR_1M"}),
            ).watch
        return S3DecisionV2("WATCH", "DEVIATION_BEYOND_2_SIGMA", state, watch=watch)

    def on_subsequent_bar(
        self, *, watch_id: str, trigger: CausalBarV2, cutoff_ns: int,
        frozen_vwap: TradeVwapSnapshotV2, residual_sigma: float, quote: ExecutableQuote | None,
        tick_size: Decimal, feature: FeatureArtifactV2, universe: UniverseContractV2,
        event_gate: EventGate | None, bar_health: PublicSourceHealthV2 | None,
        trade_completeness_proven: bool,
    ) -> S3DecisionV2:
        watch = self.repository.get_watch(watch_id)
        if watch is None:
            raise KeyError(watch_id)
        if type(trade_completeness_proven) is not bool:
            raise ValueError("S3 trade completeness qualification must be bool")
        refs: tuple[str, ...] = (watch.thesis_hash, trigger.content_hash, frozen_vwap.content_hash, feature.content_hash,
                                 event_gate.evidence_ref if event_gate else "", quote.evidence_ref if quote else "")
        refs = tuple(sorted({ref for ref in refs if ref}))
        if watch.state == WatchStateV2.HANDED_OFF:
            state = self._state(key=watch.key, cutoff_ns=cutoff_ns, status="TRIGGERED", reason="DUPLICATE_TRIGGER",
                                refs=refs, residual=None, sigma=residual_sigma, vwap=frozen_vwap.vwap,
                                replay_view=feature.replay_view.value)
            return S3DecisionV2("NO_CANDIDATE", "DUPLICATE_TRIGGER", state, watch=watch)

        def no(reason: str, status: str = "NOT_ESTIMABLE") -> S3DecisionV2:
            state = self._state(key=watch.key, cutoff_ns=cutoff_ns, status=status, reason=reason,
                                refs=refs, sigma=residual_sigma, vwap=frozen_vwap.vwap,
                                replay_view=feature.replay_view.value)
            return S3DecisionV2(status, reason, state, watch=watch)

        if watch.state != WatchStateV2.WAITING_FOR_EVENT:
            return no("WATCH_NOT_WAITING", "NO_CANDIDATE")
        if not trade_completeness_proven:
            return no("TRADE_COMPLETENESS_UNPROVEN")
        if cutoff_ns < trigger.close_at_ns or trigger.interval != BarIntervalV2.M1 or not trigger.final:
            return no("TRIGGER_BAR_NOT_COMPLETED")
        if trigger.instrument_revision != watch.key.contract_revision or frozen_vwap.key != watch.key:
            return no("FULL_INSTRUMENT_IDENTITY_MISMATCH")
        if (trigger.close_at_ns <= watch.created_at_ns
                or not _bar_available_at_view(trigger, cutoff_ns, feature.replay_view.value)
                or trigger.raw.source_id != (bar_health.source_id if bar_health else None)):
            return no("TRIGGER_NOT_SUBSEQUENT_OR_UNAVAILABLE")
        if cutoff_ns > watch.expires_at_ns:
            return no("WATCH_EXPIRED", "NO_CANDIDATE")
        setup_entry = self.repository.get_artifact(watch.thesis_hash)
        if setup_entry is None or setup_entry.available_at_ns > cutoff_ns:
            return no("SETUP_EVIDENCE_UNAVAILABLE")
        setup_state = setup_entry.metadata.get("state")
        if not isinstance(setup_state, Mapping) or setup_state.get("status") != "WATCH":
            return no("SETUP_EVIDENCE_INVALID")
        initial_residual = setup_state.get("residual")
        frozen_price = Decimal(str(setup_state.get("frozen_vwap")))
        if initial_residual is None or frozen_price != frozen_vwap.vwap:
            return no("FROZEN_ENTRY_VWAP_MISMATCH")
        if (frozen_vwap.content_hash not in watch.evidence_refs
                or frozen_vwap.information_cutoff_ns > watch.created_at_ns
                or frozen_vwap.available_at_ns > watch.created_at_ns):
            return no("FROZEN_ENTRY_VWAP_MISMATCH")
        setup_sigma = setup_state.get("residual_sigma")
        if setup_sigma is None or float(setup_sigma) != residual_sigma:
            return no("RESIDUAL_SIGMA_MISMATCH")
        trigger_residual = math.log(float(trigger.close)) - math.log(float(frozen_price))
        if not moves_toward_frozen_vwap(float(initial_residual), trigger_residual):
            state = self._state(key=watch.key, cutoff_ns=cutoff_ns, status="WATCH", reason="NOT_MOVED_TOWARD_FROZEN_VWAP",
                                refs=refs, residual=trigger_residual, sigma=residual_sigma,
                                vwap=frozen_price, replay_view=feature.replay_view.value)
            return S3DecisionV2("WATCH", "NOT_MOVED_TOWARD_FROZEN_VWAP", state, watch=watch)
        if (event_gate is None or not self._event_gate_is_current_s7_projection(event_gate, cutoff_ns)
                or event_gate.state != EventState.CLEAR):
            return no("EVENT_GATE_UNKNOWN_OR_BLOCKED")
        if quote is None or quote.key != watch.key or not quote.valid_at(cutoff_ns, BBO_MAX_AGE_NS):
            return no("BBO_STALE_OR_UNAVAILABLE")
        if bar_health is None or not self._source_health_ok(bar_health, cutoff_ns):
            return no("BAR_SOURCE_HEALTH_STALE_OR_UNAVAILABLE")
        entry_ref = quote.ask if float(initial_residual) < 0 else quote.bid
        side = V2Side.LONG if float(initial_residual) < 0 else V2Side.SHORT
        try:
            stop = round_stop(frozen_price, residual_sigma, side, tick_size)
        except (ValueError, ArithmeticError):
            return no("INVALID_OR_TINY_ROUNDED_STOP")
        if not effective_stop_valid(entry_ref, stop, tick_size, side):
            return no("INVALID_OR_TINY_ROUNDED_STOP")
        collar_raw = entry_ref * (Decimal("1.0005") if side == V2Side.LONG else Decimal("0.9995"))
        collar_rounding = ROUND_DOWN if side == V2Side.LONG else ROUND_UP
        collar = (collar_raw / tick_size).to_integral_value(rounding=collar_rounding) * tick_size
        if collar <= 0:
            return no("INVALID_OR_TINY_ROUNDED_STOP")
        if (feature.key != watch.key or feature.information_cutoff_ns != cutoff_ns
                or feature.envelope.available_at_ns > cutoff_ns):
            return no("FEATURE_NOT_CAUSAL_FOR_TRIGGER")
        entries = [item for item in universe.entries if item.key == watch.key]
        eligibility = entries[0].strategy_eligibility.get(POLICY_ID) if len(entries) == 1 else None
        if (universe.envelope.available_at_ns > cutoff_ns or cutoff_ns > universe.decision_slot_ns
                or len(entries) != 1 or not entries[0].data_eligible
                or eligibility is None or eligibility.status != EligibilityStatusV2.ELIGIBLE
                or entries[0].capital_eligible):
            return no("POINT_IN_TIME_UNIVERSE_INELIGIBLE")
        state = self._state(key=watch.key, cutoff_ns=cutoff_ns, status="TRIGGERED", reason="MEASURED_MOVE_TOWARD_FROZEN_VWAP",
                            refs=refs, residual=trigger_residual, sigma=residual_sigma,
                            vwap=frozen_price, replay_view=feature.replay_view.value)
        candidate_id = sha256_json({"watch_id": watch_id, "trigger_ref": trigger.content_hash,
                                    "policy_hash": self.policy.policy_hash})
        candidate = CandidateActionV2(
            ArtifactEnvelope(1, candidate_id, cutoff_ns, cutoff_ns, PRODUCER_VERSION,
                             tuple(sorted({*refs, state.content_hash, universe.content_hash, bar_health.content_hash}))),
            candidate_id, watch.key, self.policy.policy_hash, feature.content_hash, side,
            cutoff_ns, cutoff_ns + 5_000_000_000, cutoff_ns + MAX_HOLD_NS,
            entry_ref, collar, stop, watch.state_version + 3, self.cost_model_ref, quantity=None,
        )
        _index(self.repository, candidate.content_hash, "CandidateActionV2", cutoff_ns,
               {"candidate": candidate.to_dict(), "sleeve": POLICY_ID, "selector_influence": "ZERO",
                "watch_id": watch_id, "trigger_ref": trigger.content_hash,
                "frozen_vwap_ref": frozen_vwap.content_hash, "exact_action_status": "AVAILABLE"})
        ready = self.repository.transition_watch(
            watch_id, expected_state_version=watch.state_version,
            event_id=sha256_json({"watch": watch_id, "trigger": trigger.content_hash, "state": "READY_FOR_RECHECK"}),
            event_at_ns=trigger.close_at_ns, transition_at_ns=cutoff_ns,
            target_state=WatchStateV2.READY_FOR_RECHECK,
            outbox_id=sha256_json({"watch": watch_id, "trigger": trigger.content_hash, "outbox": "READY_FOR_RECHECK"}),
        ).watch
        confirmed = self.repository.transition_watch(
            watch_id, expected_state_version=ready.state_version,
            event_id=sha256_json({"watch": watch_id, "trigger": trigger.content_hash, "state": "CONFIRMED"}),
            event_at_ns=trigger.close_at_ns, transition_at_ns=cutoff_ns, target_state=WatchStateV2.CONFIRMED,
            outbox_id=sha256_json({"watch": watch_id, "trigger": trigger.content_hash, "outbox": "CONFIRMED"}),
        ).watch
        handed = self.repository.transition_watch(
            watch_id, expected_state_version=confirmed.state_version,
            event_id=sha256_json({"watch": watch_id, "trigger": trigger.content_hash, "state": "HANDED_OFF"}),
            event_at_ns=trigger.close_at_ns, transition_at_ns=cutoff_ns, target_state=WatchStateV2.HANDED_OFF,
            outbox_id=sha256_json({"watch": watch_id, "trigger": trigger.content_hash, "outbox": "HANDED_OFF"}),
            handoff_receipt=candidate.content_hash,
        ).watch
        return S3DecisionV2("CANDIDATE", "SHADOW_TRIGGER", state, handed, candidate)
