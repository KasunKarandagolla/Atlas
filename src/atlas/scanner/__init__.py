"""Phase-5 continuous scanner and alert-only orchestration.

The scanner ranks a point-in-time universe, allocates a bounded deep-evaluation
budget, and delegates qualified BTC/ETH decisions to the frozen Phase-4 engine.
It never submits orders, reserves capital or consumes approvals.
"""

from .alerts import AlertDelivery, AlertTransport, RecordingAlertTransport, alerts_from_rows
from .blindspots import BlindSpotObservation, blindspot_metrics
from .calendar import ScannerCalendar
from .cheap_scan import CHEAP_SCANNER_VERSION, SCANNER_PRIORITY_LABEL, cheap_scan
from .engine import ScanSlotResult, run_scan_slot
from .handoff import Phase4HandoffRequest, Phase4ScannerResult, evaluator_from_phase4, result_from_evaluation
from .health import scanner_health
from .models import (
    SLOT_NS,
    AlertSeverity,
    AlertType,
    BlindSpotStatus,
    CheapScanInput,
    CheapScanObservation,
    DeadlineStatus,
    EligibilityStatus,
    ExplorationSelection,
    ListingStatus,
    RankBand,
    RankedObservation,
    ScannerAlert,
    ScannerCalendarRow,
    ScannerHealth,
    ScannerPolicy,
    ScannerRevisionComparison,
    ScannerSelection,
    UniverseEntry,
    UniverseSnapshot,
    WarmupEvidence,
    WarmupState,
    WarmupStatus,
)
from .ranking import rank_observations
from .revision import compare_scanner_revisions
from .selection import (
    CAPITAL_ENABLED_INSTRUMENTS,
    DEFAULT_DEEP_K,
    DEFAULT_TOP_K,
    select_candidates,
    select_exploration,
)
from .universe import build_universe_snapshot
from .warmup import evaluate_warmup

__all__ = [
    "AlertDelivery",
    "AlertSeverity",
    "AlertTransport",
    "AlertType",
    "BlindSpotObservation",
    "BlindSpotStatus",
    "CAPITAL_ENABLED_INSTRUMENTS",
    "CHEAP_SCANNER_VERSION",
    "CheapScanInput",
    "CheapScanObservation",
    "DEFAULT_DEEP_K",
    "DEFAULT_TOP_K",
    "DeadlineStatus",
    "EligibilityStatus",
    "ExplorationSelection",
    "ListingStatus",
    "Phase4HandoffRequest",
    "Phase4ScannerResult",
    "RankBand",
    "RankedObservation",
    "RecordingAlertTransport",
    "SCANNER_PRIORITY_LABEL",
    "SLOT_NS",
    "ScanSlotResult",
    "ScannerAlert",
    "ScannerCalendar",
    "ScannerCalendarRow",
    "ScannerHealth",
    "ScannerPolicy",
    "ScannerRevisionComparison",
    "ScannerSelection",
    "UniverseEntry",
    "UniverseSnapshot",
    "WarmupEvidence",
    "WarmupState",
    "WarmupStatus",
    "alerts_from_rows",
    "blindspot_metrics",
    "build_universe_snapshot",
    "cheap_scan",
    "compare_scanner_revisions",
    "evaluator_from_phase4",
    "evaluate_warmup",
    "rank_observations",
    "result_from_evaluation",
    "run_scan_slot",
    "scanner_health",
    "select_candidates",
    "select_exploration",
]
