"""Small deterministic Phase-5 scanner fixture; orchestration proof only."""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from support.phase4_factory import policy as phase4_policy
from support.phase4_factory import snapshot as phase4_snapshot

from atlas.domain.enums import Side
from atlas.scanner import (
    CHEAP_SCANNER_VERSION,
    CheapScanInput,
    EligibilityStatus,
    ListingStatus,
    Phase4HandoffRequest,
    Phase4ScannerResult,
    RankBand,
    RankedObservation,
    ScannerPolicy,
    UniverseEntry,
    UniverseSnapshot,
    WarmupEvidence,
    build_universe_snapshot,
)
from atlas.science.evaluation import DecisionStatus
from atlas.science.trade_plan_mapping import candidate_trade_plan

EPOCH_NS = int(datetime(2025, 1, 6, tzinfo=UTC).timestamp() * 1_000_000_000)
SLOT_NS = 4 * 3_600_000_000_000
HOUR_NS = 3_600_000_000_000
SLOTS = (EPOCH_NS, EPOCH_NS + SLOT_NS, EPOCH_NS + 2 * SLOT_NS)

ELIGIBLE = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT", "AVAXUSDT")
CAPITAL_ENABLED = {"BTCUSDT", "ETHUSDT"}
EXCLUDED = ("LUNAUSDT",)

CHEAP_PRIORITY = {
    "BTCUSDT": (0.050, 0.010),
    "ETHUSDT": (0.040, 0.010),
    "SOLUSDT": (0.030, 0.010),
    "XRPUSDT": (0.020, 0.010),
    "ADAUSDT": (0.015, 0.010),
    "DOGEUSDT": (0.010, 0.010),
    "AVAXUSDT": (0.005, 0.010),
}

COUNTERFACTUAL_VALUE = {
    "BTCUSDT": 2.0,
    "ETHUSDT": 1.0,
    "SOLUSDT": 0.4,
    "XRPUSDT": -0.3,
    "ADAUSDT": 0.6,
    "DOGEUSDT": 0.2,
    "AVAXUSDT": -0.1,
}


def return_history(instrument: str, count: int = 24) -> tuple[float, ...]:
    phase = 0.0 if instrument in {"BTCUSDT", "ETHUSDT"} else 0.7
    offset = (sum(ord(ch) for ch in instrument) % 11) / 11.0
    return tuple(round(math.sin(index / 3.0 + phase + offset) * 0.01, 8) for index in range(count))


def make_ranked(pairs: list[tuple[str, float]], *, slot_at_ns: int = EPOCH_NS) -> tuple[RankedObservation, ...]:
    ordered = sorted(pairs, key=lambda item: (-item[1], item[0]))
    rows = []
    for rank, (instrument, score) in enumerate(ordered, start=1):
        if rank <= 3:
            band = RankBand.TOP_3
        elif rank <= 10:
            band = RankBand.B1
        elif rank <= 20:
            band = RankBand.B2
        else:
            band = RankBand.B3
        rows.append(RankedObservation(slot_at_ns, instrument, score, f"cheap-{instrument}", "universe",
                                      rank, instrument, band, "cluster-01"))
    return tuple(rows)


def universe_for(slot_at_ns: int) -> UniverseSnapshot:
    entries: list[UniverseEntry] = []
    for instrument in ELIGIBLE:
        entries.append(UniverseEntry(
            observed_at_ns=slot_at_ns - HOUR_NS,
            effective_at_ns=slot_at_ns - HOUR_NS // 2,
            available_at_ns=slot_at_ns - HOUR_NS // 2,
            venue="BYBIT",
            instrument=instrument,
            product_type="LINEAR_USDT_PERPETUAL",
            listing_status=ListingStatus.LISTED,
            eligibility_status=EligibilityStatus.ELIGIBLE,
            exclusion_reason=None,
            source_ref=f"universe-source-{instrument}-{slot_at_ns}",
            capital_enabled=instrument in CAPITAL_ENABLED,
            contract_spec_ref=f"contract-spec-{instrument}-{slot_at_ns}",
            contract_spec_hash=f"contract-spec-hash-{instrument}",
            contract_spec_available_at_ns=slot_at_ns - HOUR_NS,
            causal_return_history=return_history(instrument),
        ))
    for instrument in EXCLUDED:
        entries.append(UniverseEntry(
            observed_at_ns=slot_at_ns - HOUR_NS,
            effective_at_ns=slot_at_ns - HOUR_NS // 2,
            available_at_ns=slot_at_ns - HOUR_NS // 2,
            venue="BYBIT",
            instrument=instrument,
            product_type="LINEAR_USDT_PERPETUAL",
            listing_status=ListingStatus.DELISTED,
            eligibility_status=EligibilityStatus.EXCLUDED,
            exclusion_reason="DELISTED_RETAINED_IN_HISTORICAL_UNIVERSE",
            source_ref=f"universe-source-{instrument}-{slot_at_ns}",
            capital_enabled=False,
            contract_spec_ref=f"contract-spec-{instrument}-{slot_at_ns}",
            contract_spec_hash=f"contract-spec-hash-{instrument}",
            contract_spec_available_at_ns=slot_at_ns - HOUR_NS,
            causal_return_history=return_history(instrument),
        ))
    return build_universe_snapshot(
        snapshot_id=f"universe-{slot_at_ns}",
        observed_at_ns=slot_at_ns - HOUR_NS,
        available_at_ns=slot_at_ns - HOUR_NS // 2,
        venue="BYBIT",
        version="UNIVERSE_V1",
        entries=tuple(entries),
        source_ref=f"universe-snapshot-source-{slot_at_ns}",
    )


def cheap_inputs_for(slot_at_ns: int) -> tuple[CheapScanInput, ...]:
    return tuple(
        CheapScanInput(instrument, slot_at_ns, slot_at_ns, CHEAP_PRIORITY[instrument][0],
                       CHEAP_PRIORITY[instrument][1], Decimal("1000000"))
        for instrument in ELIGIBLE
    )


def warmup_for(slot_at_ns: int) -> tuple[WarmupEvidence, ...]:
    return tuple(
        WarmupEvidence(
            instrument=instrument,
            slot_at_ns=slot_at_ns,
            available=True,
            job_enqueued_at_ns=slot_at_ns - HOUR_NS,
            job_started_at_ns=slot_at_ns - HOUR_NS // 2,
            job_finished_at_ns=slot_at_ns - 2 * 60 * 1_000_000_000,
        )
        for instrument in ELIGIBLE if instrument != "XRPUSDT"
    )


def phase4_trade_plan(slot_at_ns: int):
    feature_snapshot = phase4_snapshot(instrument="BTCUSDT", slot_at_ns=slot_at_ns, z=2.0)
    frozen_policy = phase4_policy(side=Side.LONG, quantity=Decimal("1"), mark=Decimal("100"),
                                  sigma=0.01, slot_at_ns=slot_at_ns)
    return candidate_trade_plan(
        plan_id=f"plan-{slot_at_ns}",
        snapshot=feature_snapshot,
        policy=frozen_policy,
        policy_hash="phase4-risk-policy-hash",
        normal_risk=Decimal("10"),
        stress_risk=Decimal("20"),
        margin=Decimal("20"),
        leverage=Decimal("5"),
        cost_evidence_ref="phase4-cost-evidence",
        account_scope="RESEARCH",
        quantity=Decimal("1"),
    )


def evaluator(request: Phase4HandoffRequest) -> Phase4ScannerResult:
    if request.instrument == "BTCUSDT":
        plan = phase4_trade_plan(request.slot_at_ns)
        return Phase4ScannerResult(request.slot_at_ns, request.instrument, DecisionStatus.TRADE_CANDIDATE,
                                   f"phase4-eval-{request.slot_at_ns}-{request.instrument}", plan,
                                   ("A0 LCB/ES qualified",), (), plan.plan_hash())
    if request.instrument == "ETHUSDT":
        return Phase4ScannerResult(request.slot_at_ns, request.instrument, DecisionStatus.NO_SIGNAL,
                                   f"phase4-eval-{request.slot_at_ns}-{request.instrument}", None,
                                   ("frozen trend signal is flat",), (), "phase4-eth-no-signal")
    raise AssertionError("Phase 5 must not hand research instruments to the Phase-4 capital evaluator")


@dataclass(frozen=True)
class ScannerFixture:
    policy: ScannerPolicy
    slots: tuple[int, ...]
    universes: tuple[UniverseSnapshot, ...]
    cheap_inputs: tuple[tuple[CheapScanInput, ...], ...]
    warmup_evidence: tuple[tuple[WarmupEvidence, ...], ...]
    counterfactual_values: tuple[dict[str, float], ...]
    evaluator: Callable[[Phase4HandoffRequest], Phase4ScannerResult]


def scanner_fixture() -> ScannerFixture:
    return ScannerFixture(
        policy=ScannerPolicy(policy_version="SCANNER_BASELINE_V1",
                             cheap_scorer_version=CHEAP_SCANNER_VERSION,
                             include_no_trade_summary=True),
        slots=SLOTS,
        universes=tuple(universe_for(slot) for slot in SLOTS),
        cheap_inputs=tuple(cheap_inputs_for(slot) for slot in SLOTS),
        warmup_evidence=tuple(warmup_for(slot) for slot in SLOTS),
        counterfactual_values=tuple(dict(COUNTERFACTUAL_VALUE) for _ in SLOTS),
        evaluator=evaluator,
    )
