"""Immutable Phase-5 scanner records and typed state."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from atlas.domain.trade_plan import TradePlan
from atlas.science.evaluation import DecisionStatus

HOUR_NS = 3_600_000_000_000
SLOT_NS = 4 * HOUR_NS
CAPITAL_ENABLED_INSTRUMENTS = ("BTCUSDT", "ETHUSDT")


def canonical_json(value: object) -> str:
    """Canonical JSON for stable artifact and content hashes."""

    def default(item: object) -> object:
        if isinstance(item, StrEnum):
            return item.value
        if hasattr(item, "value"):
            return item.value  # type: ignore[attr-defined]
        if isinstance(item, Decimal):
            return str(item)
        if hasattr(item, "__dataclass_fields__"):
            return {name: getattr(item, name) for name in item.__dataclass_fields__}  # type: ignore[attr-defined]
        if isinstance(item, tuple):
            return list(item)
        return str(item)

    return json.dumps(value, default=default, sort_keys=True, separators=(",", ":"))


def content_hash(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


class ListingStatus(StrEnum):
    LISTED = "LISTED"
    SUSPENDED = "SUSPENDED"
    DELISTED = "DELISTED"
    FAILED = "FAILED"


class EligibilityStatus(StrEnum):
    ELIGIBLE = "ELIGIBLE"
    EXCLUDED = "EXCLUDED"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class RankBand(StrEnum):
    TOP_3 = "TOP_3"
    B1 = "B1"
    B2 = "B2"
    B3 = "B3"
    UNRANKED = "UNRANKED"


class WarmupState(StrEnum):
    WARM_AVAILABLE = "WARM_AVAILABLE"
    NOT_ESTIMABLE_WARMUP = "NOT_ESTIMABLE_WARMUP"
    NOT_SELECTED = "NOT_SELECTED"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class DeadlineStatus(StrEnum):
    MET = "MET"
    MISSED = "MISSED"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class AlertType(StrEnum):
    TRADE_CANDIDATE = "TRADE_CANDIDATE"
    NO_TRADE_SUMMARY = "NO_TRADE_SUMMARY"
    NOT_ESTIMABLE = "NOT_ESTIMABLE"
    HEALTH_DEGRADATION = "HEALTH_DEGRADATION"
    DEADLINE_WARMUP_FAILURE = "DEADLINE_WARMUP_FAILURE"


class AlertSeverity(StrEnum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class BlindSpotStatus(StrEnum):
    PASS = "PASS"
    ATTENTION = "ATTENTION"
    INCONCLUSIVE = "INCONCLUSIVE"


@dataclass(frozen=True)
class ScannerPolicy:
    policy_version: str
    cheap_scorer_version: str
    top_k: int = 3
    deep_k: int = 5
    exploration_bands: tuple[tuple[int, int | None], ...] = ((4, 10), (11, 20), (21, None))
    blindspot_tolerance: float = 0.20
    warmup_deadline_ns: int = 30_000_000_000
    include_no_trade_summary: bool = False
    assisted_enabled: bool = False
    bybit_capabilities: str = "UNVERIFIED"

    def __post_init__(self) -> None:
        if not self.policy_version.strip() or not self.cheap_scorer_version.strip():
            raise ValueError("scanner policy/scorer versions required")
        if self.top_k < 1 or self.deep_k < self.top_k:
            raise ValueError("top-K/deep-K must satisfy 0 < top_k <= deep_k")
        if self.blindspot_tolerance < 0:
            raise ValueError("blindspot tolerance must be nonnegative")
        if self.assisted_enabled:
            raise ValueError("Phase 5 is alert-only; assisted_enabled must remain false")
        if self.bybit_capabilities != "UNVERIFIED":
            raise ValueError("Phase 5 does not qualify Bybit capabilities")

    def hash(self) -> str:
        return content_hash(self)


@dataclass(frozen=True)
class UniverseEntry:
    observed_at_ns: int
    effective_at_ns: int
    available_at_ns: int
    venue: str
    instrument: str
    product_type: str
    listing_status: ListingStatus
    eligibility_status: EligibilityStatus
    exclusion_reason: str | None
    source_ref: str
    capital_enabled: bool
    causal_return_history: tuple[float, ...] = ()

    def __post_init__(self) -> None:
        if not all((self.venue, self.instrument, self.product_type, self.source_ref)):
            raise ValueError("universe entry identity fields required")
        if min(self.observed_at_ns, self.effective_at_ns, self.available_at_ns) < 0:
            raise ValueError("universe timestamps must be UTC nanoseconds")
        if self.available_at_ns < self.observed_at_ns:
            raise ValueError("universe availability precedes observation")
        if self.eligibility_status is EligibilityStatus.EXCLUDED and not self.exclusion_reason:
            raise ValueError("excluded universe entries require an exclusion reason")
        if self.eligibility_status is EligibilityStatus.ELIGIBLE and self.exclusion_reason is not None:
            raise ValueError("eligible universe entries may not carry an exclusion reason")
        if self.capital_enabled and self.instrument not in CAPITAL_ENABLED_INSTRUMENTS:
            raise ValueError("V1 capital scope is BTCUSDT/ETHUSDT only")
        if self.instrument in CAPITAL_ENABLED_INSTRUMENTS and not self.capital_enabled:
            raise ValueError("BTCUSDT/ETHUSDT must remain capital-enabled in V1")


@dataclass(frozen=True)
class UniverseSnapshot:
    snapshot_id: str
    observed_at_ns: int
    available_at_ns: int
    venue: str
    version: str
    entries: tuple[UniverseEntry, ...]
    source_ref: str

    def __post_init__(self) -> None:
        if not all((self.snapshot_id, self.venue, self.version, self.source_ref)):
            raise ValueError("snapshot identity fields required")
        identifiers = [entry.instrument for entry in self.entries]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("universe snapshot contains duplicate instruments")

    def hash(self) -> str:
        return content_hash(self)

    def eligible_entries(self) -> tuple[UniverseEntry, ...]:
        return tuple(entry for entry in self.entries if entry.eligibility_status is EligibilityStatus.ELIGIBLE)

    def entry(self, instrument: str) -> UniverseEntry:
        for item in self.entries:
            if item.instrument == instrument:
                return item
        raise KeyError(instrument)


@dataclass(frozen=True)
class CheapScanInput:
    instrument: str
    slot_at_ns: int
    availability_cutoff_ns: int
    return_24h: float
    volatility_24h: float
    quote_volume_notional: Decimal | None = None

    def hash(self) -> str:
        return content_hash(self)


@dataclass(frozen=True)
class CheapScanObservation:
    slot_at_ns: int
    scanner_policy_version: str
    scorer_version: str
    instrument: str
    availability_cutoff_ns: int
    return_24h: float
    volatility_24h: float
    quote_volume_notional: Decimal | None
    score: float
    input_hash: str
    label: str = "SCANNER_PRIORITY_ONLY_NOT_ALPHA"

    def hash(self) -> str:
        return content_hash(self)


@dataclass(frozen=True)
class RankedObservation:
    slot_at_ns: int
    instrument: str
    cheap_score: float
    cheap_observation_hash: str
    universe_hash: str
    rank: int
    tie_break_key: str
    rank_band: RankBand
    correlation_cluster: str

    def hash(self) -> str:
        return content_hash(self)


@dataclass(frozen=True)
class ScannerSelection:
    slot_at_ns: int
    instrument: str
    cheap_score: float
    rank: int
    rank_band: RankBand
    correlation_cluster: str
    top_k_selected: bool
    deep_selected: bool
    selection_reason: str

    def hash(self) -> str:
        return content_hash(self)


@dataclass(frozen=True)
class ExplorationSelection:
    slot_at_ns: int
    policy_version: str
    universe_hash: str
    band: RankBand | None
    instrument: str | None
    selection_key: str | None
    inclusion_probability: float
    preferred_band: RankBand
    fallback_path: str | None
    reason: str

    def hash(self) -> str:
        return content_hash(self)


@dataclass(frozen=True)
class WarmupEvidence:
    instrument: str
    slot_at_ns: int
    available: bool
    job_enqueued_at_ns: int | None = None
    job_started_at_ns: int | None = None
    job_finished_at_ns: int | None = None
    reason: str | None = None


@dataclass(frozen=True)
class WarmupStatus:
    slot_at_ns: int
    instrument: str
    deep_requested: bool
    state: WarmupState
    job_enqueued_at_ns: int | None
    job_started_at_ns: int | None
    job_finished_at_ns: int | None
    deadline_at_ns: int | None
    deadline_status: DeadlineStatus
    reason: str

    def hash(self) -> str:
        return content_hash(self)


@dataclass(frozen=True)
class Phase4HandoffRequest:
    slot_at_ns: int
    instrument: str
    availability_cutoff_ns: int
    universe_hash: str
    cheap_observation_hash: str
    warmup_state: WarmupState


@dataclass(frozen=True)
class Phase4ScannerResult:
    slot_at_ns: int
    instrument: str
    status: DecisionStatus
    evaluation_ref: str
    trade_plan: TradePlan | None = None
    reasons: tuple[str, ...] = ()
    not_estimable_reasons: tuple[str, ...] = ()
    evidence_hash: str = ""

    def plan_status(self) -> str:
        return self.status.value

    def plan_id(self) -> str | None:
        return self.trade_plan.plan_id if self.trade_plan is not None else None

    def plan_hash(self) -> str | None:
        return self.trade_plan.plan_hash() if self.trade_plan is not None else None


@dataclass(frozen=True)
class ScannerCalendarRow:
    scan_slot_id: str
    scan_slot_at_ns: int
    scanner_policy_version: str
    universe_version: str
    universe_hash: str
    instrument: str
    availability_cutoff_ns: int
    eligibility_status: EligibilityStatus
    exclusion_reason: str | None
    cheap_snapshot_hash: str | None
    cheap_score: float | None
    rank: int | None
    rank_tie_break: str | None
    rank_band: RankBand | None
    correlation_cluster: str | None
    top_k_selected: bool
    deep_selected: bool
    exploration_selected: bool
    exploration_probability: float | None
    warmup_state: WarmupState
    model_job_enqueued_at_ns: int | None
    model_job_started_at_ns: int | None
    model_job_finished_at_ns: int | None
    model_deadline_status: DeadlineStatus
    plan_status: str
    rejection_reason: str | None
    not_estimable_reason: str | None
    phase4_evaluation_ref: str | None
    trade_plan_id: str | None
    trade_plan_hash: str | None
    counterfactual_value: float | None = None
    approval_requested_at_ns: int | None = None
    approved_at_ns: int | None = None
    approval_expiry_ns: int | None = None
    human_delay_ms: int | None = None
    entry_attempted: bool | None = None
    filled_qty: str | None = None
    no_fill_reason: str | None = None
    execution_delay_ms: int | None = None
    matured_counterfactual_label_id: str | None = None
    realized_policy_outcome_id: str | None = None
    outcome_status: str = "NOT_APPLICABLE"
    selection_reason: str | None = None

    def hash(self) -> str:
        return content_hash(self)


@dataclass(frozen=True)
class ScannerAlert:
    alert_id: str
    alert_type: AlertType
    severity: AlertSeverity
    scan_slot_id: str
    instrument: str | None
    message_code: str
    evidence_refs: tuple[str, ...]
    created_at_ns: int

    @staticmethod
    def create(*, alert_type: AlertType, severity: AlertSeverity, scan_slot_id: str,
               instrument: str | None, message_code: str, evidence_refs: tuple[str, ...],
               created_at_ns: int) -> ScannerAlert:
        payload = {"alert_type": alert_type.value, "severity": severity.value, "scan_slot_id": scan_slot_id,
                   "instrument": instrument, "message_code": message_code, "evidence_refs": list(evidence_refs),
                   "created_at_ns": created_at_ns}
        return ScannerAlert(f"alert-{content_hash(payload)}", alert_type, severity, scan_slot_id, instrument,
                            message_code, evidence_refs, created_at_ns)


@dataclass(frozen=True)
class AlertDelivery:
    alert_id: str
    state: str
    transport: str
    reason: str | None = None

    def __post_init__(self) -> None:
        if self.state not in {"DELIVERED", "FAILED", "NOT_ATTEMPTED"}:
            raise ValueError("alert delivery state must be DELIVERED/FAILED/NOT_ATTEMPTED")


@dataclass(frozen=True)
class ScannerHealth:
    status: str
    last_completed_scan_slot: int | None
    universe_freshness_ns: int | None
    cheap_scan_freshness_ns: int | None
    deep_data_freshness_ns: int | None
    calendar_persistence_healthy: bool
    research_archive_healthy: bool
    phase4_evaluator_available: bool
    missed_deadlines: int
    warmup_failures: int
    alert_delivery_state: str
    assisted_enabled: bool = False
    bybit_capabilities: str = "UNVERIFIED"
    trading_safety_claimed: bool = False


@dataclass(frozen=True)
class BlindSpotObservation:
    slot_at_ns: int
    instrument: str
    rank_band: RankBand
    top_k_selected: bool
    exploration_selected: bool
    inclusion_probability: float
    counterfactual_value: float | None
    warmup_available: bool
    deadline_met: bool


@dataclass(frozen=True)
class BlindSpotMetrics:
    status: BlindSpotStatus
    selection_coverage: float | None
    selection_coverage_upper: float | None
    missed_value_share: float | None
    missed_value_share_upper: float | None
    warmup_exclusion_rate: float | None
    deadline_loss_rate: float | None
    selection_lift: float | None
    selection_lift_interval: tuple[float, float] | None
    support_slots: int
    exploration_probability_support: int
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class ScannerRevisionComparison:
    previous_policy_version: str
    revised_policy_version: str
    paired_slots: int
    paired_instruments: int
    status: str
    compute_delay_delta_ms: float | None
    warmup_miss_delta: int
    deadline_miss_delta: int
    rejection_delta: int
    trade_candidate_delta: int
    no_fill_delta: int
    selection_effect_delta: float | None
    reasons: tuple[str, ...] = ()
