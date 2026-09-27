from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from atlas.v2.data.capabilities import (
    CapabilityStatusV2,
    EvidenceCapabilityMatrixV2,
    FeedCoverageEvidenceV2,
    FeedCoverageStateV2,
    default_evidence_capability_matrix_v2,
)
from atlas.v2.data.derivatives import (
    DerivativeAvailabilityV2,
    FundingKindV2,
    FundingObservationV2,
    LiquidationCoverageV2,
    LiquidationWindowTotalV2,
    OpenInterestObservationV2,
    S5CrowdingContextV2,
    build_s5_crowding_context,
    fit_s5_liquidation_baseline,
    funding_percentile_prior,
    oi_change_15m,
    oi_change_15m_evidence,
)
from atlas.v2.data.microstructure import (
    BookStateV2,
    S4AbsorptionHypothesisV2,
)
from atlas.v2.data.public_microstructure_ws import parse_bybit_liquidations
from atlas.v2.features.context import (
    KILLZONE_POLICY_HASH,
    STRUCTURE_CONTEXT_POLICY_HASH,
    TIME_CONTEXT_POLICY_HASH,
    CausalStructureReferenceV2,
    S4FlowResponseObservationV2,
    TimedSessionV2,
    build_structure_context,
    build_time_context,
)
from atlas.v2.instruments import EnvironmentV2, VenueV2
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.strategies.s5_crowding import (
    S5_CONTINUATION_POLICY_HASH,
    S5_REVERSAL_POLICY_HASH,
    S5ContinuationResearchV2,
    StageEvidenceV2,
    evaluate_s5_reversal,
)

from .test_session014_core import KEY
from .test_session016_candidate_selection import (
    CUTOFF,
    assemble,
    candidate,
    evidence,
    index,
    universe,
)
from .test_session021_s7 import _coverage, _macro_event, _normal_abnormality
from .test_session022_s4_microstructure import (
    HEALTH_REF,
    T0,
    ref,
)
from .test_session022_s4_microstructure import (
    book as s4_book,
)
from .test_session022_s4_microstructure import (
    delta as s4_delta,
)
from .test_session022_s4_microstructure import (
    key as s4_key,
)
from .test_session022_s4_microstructure import (
    snapshot as s4_snapshot,
)

NS = 1_000_000_000
HEALTH = HEALTH_REF
SEMANTICS = ref("derivative-source-semantics")
USD_VALUE = "USD_NOTIONAL"
QTY_UNIT = "RAW_CONTRACT_NATIVE"
MATRIX = default_evidence_capability_matrix_v2()
DERIVATIVE_KEY = s4_key()


def funding(*, at: int, rate: str, kind: FundingKindV2 = FundingKindV2.SETTLED,
            available: int | None = None, source: str = "BYBIT_PUBLIC_REST",
            unit: str = "RATE_FRACTION", revision_of: str | None = None,
            availability: DerivativeAvailabilityV2 = DerivativeAvailabilityV2.ACTUAL_RECEIPT) -> FundingObservationV2:
    received = at if available is None else available
    published = received
    return FundingObservationV2(
        DERIVATIVE_KEY, source, kind, Decimal(rate), unit, at, received, published,
        at + 8 * 60 * 60 * NS, SEMANTICS, ref(["funding", at, rate, revision_of]),
        availability, revision_of, "HEALTHY_CURRENT", HEALTH,
        ref("predicted-funding-semantics") if kind == FundingKindV2.PREDICTED else None,
        "V5 REST ticker/funding-history/open-interest",
    )


def oi(*, event: int, quantity: str, available: int | None = None,
       mark: str | None = "101", index: str | None = "100", last: str | None = "102",
       source: str = "BYBIT_PUBLIC_REST", unit: str = QTY_UNIT,
       availability: DerivativeAvailabilityV2 = DerivativeAvailabilityV2.ACTUAL_RECEIPT,
       revision_of: str | None = None) -> OpenInterestObservationV2:
    received = event if available is None else available
    return OpenInterestObservationV2(
        DERIVATIVE_KEY, source, Decimal(quantity), unit, Decimal(quantity) * Decimal("100"), USD_VALUE,
        event, received, received, SEMANTICS, ref(["oi", event, quantity, received, revision_of]),
        availability, revision_of, Decimal(mark) if mark is not None else None,
        Decimal(index) if index is not None else None, Decimal(last) if last is not None else None,
        "HEALTHY_CURRENT", HEALTH, "V5 REST ticker/funding-history/open-interest",
    )


def test_capability_matrix_is_versioned_explicit_and_does_not_claim_live_support() -> None:
    matrix = default_evidence_capability_matrix_v2()
    assert matrix.version == "EVIDENCE_CAPABILITY_MATRIX_V2_4"
    assert len(matrix.rows) == 9
    assert matrix.content_hash == default_evidence_capability_matrix_v2().content_hash
    checked_in = json.loads(Path("docs/v2/EVIDENCE_CAPABILITY_MATRIX_V2.json").read_text())
    assert checked_in == matrix.to_dict()
    binance_snapshot = matrix.lookup("BINANCE", "MAINNET", "LINEAR_PERPETUAL", "USD-M depth snapshot REST")
    bybit = matrix.lookup("BYBIT", "MAINNET", "LINEAR_PERPETUAL", "public/orderbook.50 WS")
    assert bybit is not None
    assert bybit.status == CapabilityStatusV2.UNVERIFIED
    assert "fails closed" in (bybit.sequence_update_semantics or "")
    assert bybit.declared_book_depth == "50 levels per side"
    assert "position ownership" in bybit.explicitly_unsupported_uses
    binance_depth = matrix.lookup("BINANCE", "MAINNET", "LINEAR_PERPETUAL", "USD-M depth diff WS")
    binance_trades = matrix.lookup("BINANCE", "MAINNET", "LINEAR_PERPETUAL", "USD-M aggTrade WS")
    official_sync_doc = (
        "https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/"
        "websocket-market-streams/How-to-manage-a-local-order-book-correctly"
    )
    assert binance_snapshot is not None and binance_snapshot.status == CapabilityStatusV2.TEST_GATE
    assert official_sync_doc in binance_snapshot.evidence_refs
    assert binance_depth is not None and binance_depth.status == CapabilityStatusV2.UNVERIFIED
    assert "pu == previous accepted u" in (binance_depth.sequence_update_semantics or "")
    assert "U <= L <= u" in (binance_depth.sequence_update_semantics or "")
    assert "L+1" not in (binance_depth.sequence_update_semantics or "")
    assert official_sync_doc in binance_depth.evidence_refs
    assert any("/ws-streams/public" in item for item in binance_depth.evidence_refs)
    assert binance_trades is not None and binance_trades.status == CapabilityStatusV2.UNVERIFIED
    assert "/market" in (binance_trades.availability_semantics or "")
    assert any("/ws-streams/market" in item for item in binance_trades.evidence_refs)
    assert matrix.lookup("UNKNOWN", "MAINNET", "LINEAR_PERPETUAL", "public/orderbook") is None
    with pytest.raises(ValueError, match="row identities"):
        EvidenceCapabilityMatrixV2((bybit, bybit))


def test_qualified_feed_coverage_requires_measured_gap_and_exact_refs() -> None:
    with pytest.raises(ValueError, match="measured gap"):
        FeedCoverageEvidenceV2(
            s4_key(), "SRC", "trade", T0 - 10, T0, T0, 1, 10, 0, None,
            FeedCoverageStateV2.QUALIFIED, HEALTH, MATRIX.content_hash, (),
        )
    valid = FeedCoverageEvidenceV2(
        s4_key(), "SRC", "trade", T0 - 10, T0, T0, 1, 10, 0, 1,
        FeedCoverageStateV2.QUALIFIED, HEALTH, MATRIX.content_hash, (ref("raw-a"),),
    )
    assert valid.content_hash == valid.content_hash


def test_derivative_channel_and_environment_must_match_declared_capability() -> None:
    observed = funding(at=T0, rate="0.05", kind=FundingKindV2.CURRENT)
    testnet = replace(observed, instrument=replace(DERIVATIVE_KEY, environment=EnvironmentV2.TESTNET))
    unknown_channel = replace(observed, channel="unknown.derivatives.feed")
    for item in (testnet, unknown_channel):
        context = build_s5_crowding_context(
            instrument=item.instrument, cutoff_ns=T0, funding=(item,),
        )
        assert context.state == "NOT_ESTIMABLE"
        assert context.funding_rate is None
        assert context.capability_matrix_ref == MATRIX.content_hash
        timed, _ = build_time_context(cutoff_ns=T0, funding=(item,))
        assert timed.time_to_funding_ns is None


def test_funding_kinds_units_actual_availability_revisions_and_prior_percentile() -> None:
    prior = (funding(at=T0 - 300, rate="0.01"), funding(at=T0 - 200, rate="0.02"),
             funding(at=T0 - 100, rate="0.03"))
    current = funding(at=T0, rate="0.04", kind=FundingKindV2.CURRENT)
    predicted = funding(at=T0 + 10, rate="0.50", kind=FundingKindV2.PREDICTED,
                        available=T0 + 10)
    settled = funding(at=T0 - 10, rate="0.05", kind=FundingKindV2.SETTLED)
    assert current.kind == FundingKindV2.CURRENT
    assert predicted.next_funding_at_ns == T0 + 10 + 8 * 60 * 60 * NS
    assert settled.kind == FundingKindV2.SETTLED
    predicted_current = funding(at=T0, rate="0.04", kind=FundingKindV2.PREDICTED)
    assert funding_percentile_prior(prior, cutoff_ns=T0, current=predicted_current) == Decimal(1)
    assert funding_percentile_prior(prior + (predicted,), cutoff_ns=T0, current=current) == Decimal(1)
    assert funding_percentile_prior(prior + (settled,), cutoff_ns=T0, current=current) == Decimal("0.75")
    wrong_unit = funding(at=T0 - 50, rate="99", unit="OTHER_RATE")
    assert funding_percentile_prior(prior + (wrong_unit,), cutoff_ns=T0, current=current) == Decimal(1)
    with pytest.raises(ValueError, match="actual receipt"):
        replace(current, availability=DerivativeAvailabilityV2.RECONSTRUCTED_MARKET,
                kind=FundingKindV2.PREDICTED)
    revision = funding(at=T0 - 200, rate="0.20", available=T0 + 20,
                       revision_of=prior[1].content_hash)
    assert revision.revision_of == prior[1].content_hash
    assert funding_percentile_prior(prior + (revision,), cutoff_ns=T0, current=current) == Decimal(1)
    assert funding_percentile_prior(prior + (predicted,), cutoff_ns=T0, current=current) == Decimal(1)


def test_oi_units_basis_exact_15m_price_relationship_and_future_append_visibility() -> None:
    start = oi(event=T0 - 15 * 60 * NS, quantity="100", mark="99", index="100", last="98")
    latest = oi(event=T0, quantity="80", mark="102", index="100", last="103")
    future = oi(event=T0 + 15 * 60 * NS, quantity="20", available=T0 + 15 * 60 * NS)
    evidence = oi_change_15m_evidence((start, latest, future), cutoff_ns=T0, instrument=DERIVATIVE_KEY)
    assert evidence is not None
    assert evidence.change_fraction == Decimal("-0.2")
    assert evidence.input_refs == tuple(sorted({start.raw_content_ref, latest.raw_content_ref, HEALTH}))
    assert oi_change_15m((start, latest, future), cutoff_ns=T0, instrument=DERIVATIVE_KEY) == Decimal("-0.2")
    assert evidence.end_ref == latest.raw_content_ref
    assert evidence.quantity_unit == QTY_UNIT
    assert evidence.channel == "V5 REST ticker/funding-history/open-interest"
    assert evidence.capability_matrix_ref == MATRIX.content_hash
    assert evidence.to_dict()["availability"] == "ACTUAL_RECEIPT"
    delivered_early_future = replace(future, received_at_ns=T0 - 1, available_at_ns=T0 - 1)
    still_latest = oi_change_15m_evidence(
        (start, latest, delivered_early_future), cutoff_ns=T0, instrument=DERIVATIVE_KEY,
    )
    assert still_latest is not None and still_latest.end_ref == latest.raw_content_ref
    inexact = oi(event=T0 - 14 * 60 * NS, quantity="90")
    assert oi_change_15m((start, inexact), cutoff_ns=T0, instrument=DERIVATIVE_KEY) is None
    delayed = replace(latest, available_at_ns=T0 + 1, received_at_ns=T0 + 1)
    assert oi_change_15m((start, delayed), cutoff_ns=T0, instrument=DERIVATIVE_KEY) is None


def test_s5_context_is_causal_and_funding_alone_has_no_directional_hypothesis() -> None:
    f0 = funding(at=T0 - 15 * 60 * NS, rate="0.01")
    f1 = funding(at=T0, rate="0.05", kind=FundingKindV2.CURRENT)
    f_mid = funding(at=T0 - 5 * 60 * NS, rate="0.10")
    old_oi = oi(event=T0 - 15 * 60 * NS, quantity="100", mark="100", index="99", last="100")
    current_oi = oi(event=T0, quantity="120", mark="102", index="100", last="101")
    hidden_future = oi(event=T0 + NS, quantity="999", available=T0 + NS)
    context = build_s5_crowding_context(
        instrument=DERIVATIVE_KEY, cutoff_ns=T0, funding=(f0, f_mid, f1),
        open_interest=(old_oi, current_oi, hidden_future), liquidity_state="THIN",
    )
    assert context.funding_percentile == Decimal("0.5")
    assert context.oi_change_15m == Decimal("0.2")
    assert context.price_oi_relationship == "PRICE_UP_OI_UP"
    assert context.basis == Decimal("0.02")
    assert context.mark_price == Decimal("102") and context.index_price == Decimal("100")
    assert context.last_price == Decimal("101")
    assert context.funding_availability == DerivativeAvailabilityV2.ACTUAL_RECEIPT
    assert context.funding_received_at_ns == f1.received_at_ns
    assert context.oi_availability == DerivativeAvailabilityV2.ACTUAL_RECEIPT
    assert context.oi_available_at_ns == current_oi.available_at_ns
    oi_evidence = oi_change_15m_evidence(
        (old_oi, current_oi, hidden_future), cutoff_ns=T0, instrument=DERIVATIVE_KEY,
    )
    assert oi_evidence is not None and oi_evidence.content_hash in context.input_refs
    assert {f0.content_hash, f_mid.content_hash} <= set(context.input_refs)
    assert context.liquidation_coverage == LiquidationCoverageV2.UNKNOWN
    assert context.liquidation_intensity is None
    assert context.to_dict()["ownership_inference"] == "UNSUPPORTED"
    assert context.to_dict()["leverage_inference"] == "UNSUPPORTED"
    assert context.to_dict()["exact_liquidation_map"] == "UNSUPPORTED"
    only_funding = build_s5_crowding_context(instrument=DERIVATIVE_KEY, cutoff_ns=T0, funding=(f1,))
    assert only_funding.state == "CROWDING_CONTEXT"
    assert only_funding.evidence_quality == "PARTIAL"
    assert "short" not in only_funding.to_dict()
    assert set(only_funding.input_refs) == {f1.raw_content_ref, f1.content_hash, HEALTH}


def test_s5_revisions_do_not_rewrite_earlier_cutoffs_and_reconstructed_is_labeled() -> None:
    prior = oi(event=T0 - 15 * 60 * NS, quantity="10")
    observed = oi(event=T0, quantity="20")
    revision = oi(event=T0, quantity="99", available=T0 + 100,
                  revision_of=observed.content_hash)
    original = build_s5_crowding_context(
        instrument=DERIVATIVE_KEY, cutoff_ns=T0, open_interest=(prior, observed, revision),
    )
    after = build_s5_crowding_context(
        instrument=DERIVATIVE_KEY, cutoff_ns=T0 + 100, open_interest=(prior, observed, revision),
    )
    assert original.oi_quantity == Decimal("20")
    assert revision.revision_of == observed.content_hash
    assert after.oi_quantity == Decimal("99")
    imported = replace(observed, availability=DerivativeAvailabilityV2.RECONSTRUCTED_MARKET)
    assert imported.to_dict()["availability"] == "RECONSTRUCTED_MARKET"


def test_liquidation_feed_unknown_censored_and_context_window_are_explicit() -> None:
    from atlas.v2.data.derivatives import LiquidationObservationV2

    old = LiquidationObservationV2(
        DERIVATIVE_KEY, "BYBIT_LIQ", "old", "BUY_POSITION_LIQUIDATED", "position-side", Decimal("100"),
        Decimal("50"), QTY_UNIT, T0 - 6 * 60 * NS, T0 - 6 * 60 * NS, T0 - 6 * 60 * NS,
        LiquidationCoverageV2.CENSORED, "HEALTHY_CURRENT", ref("old-liq"),
        DerivativeAvailabilityV2.ACTUAL_RECEIPT, HEALTH, "allLiquidation.BTCUSDT",
    )
    current = replace(old, event_id="current", event_at_ns=T0 - NS, received_at_ns=T0,
                      available_at_ns=T0, raw_content_ref=ref("current-liq"), quantity=Decimal("3"))
    context = build_s5_crowding_context(instrument=DERIVATIVE_KEY, cutoff_ns=T0, liquidations=(old, current))
    assert context.liquidation_intensity == Decimal("3")
    assert context.liquidation_intensity_unit == QTY_UNIT
    assert context.liquidation_coverage == LiquidationCoverageV2.CENSORED
    assert context.liquidation_window_ns == 5 * 60 * NS
    no_coverage = build_s5_crowding_context(instrument=DERIVATIVE_KEY, cutoff_ns=T0)
    assert no_coverage.liquidation_coverage == LiquidationCoverageV2.UNKNOWN
    measured_feed = FeedCoverageEvidenceV2(
        DERIVATIVE_KEY, "BYBIT_LIQ", "allLiquidation.BTCUSDT", T0 - 5 * 60 * NS, T0, T0,
        500_000_000, 1, 0, 500_000_000, FeedCoverageStateV2.QUALIFIED,
        HEALTH, MATRIX.content_hash, (ref("measured-empty-liquidation-window"),),
    )
    empty_but_censored = build_s5_crowding_context(
        instrument=DERIVATIVE_KEY, cutoff_ns=T0, liquidation_feed_coverage=measured_feed,
    )
    assert empty_but_censored.liquidation_coverage == LiquidationCoverageV2.CENSORED
    assert empty_but_censored.liquidation_intensity is None


def test_bybit_liquidation_parser_preserves_position_side_and_censoring() -> None:
    import hashlib
    import json

    from atlas.v2.data.public_microstructure_ws import CapturedPublicFrameV2

    payload = {"topic": "allLiquidation.BTCUSDT", "ts": 1750000000000,
               "data": [{"T": 1750000000000, "S": "Buy", "p": "100", "v": "2"}]}
    raw = json.dumps(payload, separators=(",", ":")).encode()
    frame = CapturedPublicFrameV2(VenueV2.BYBIT, "BYBIT_PUBLIC", "allLiquidation.BTCUSDT",
                                  raw, hashlib.sha256(raw).hexdigest(), T0, T0)
    observations = parse_bybit_liquidations(frame, instrument=DERIVATIVE_KEY, source_health="HEALTHY_CURRENT",
                                            source_health_ref=HEALTH)
    assert observations[0].side == "BUY_POSITION_LIQUIDATED"
    assert "POSITION_SIDE" in (observations[0].side_convention or "")
    assert observations[0].coverage == LiquidationCoverageV2.CENSORED
    assert observations[0].availability == DerivativeAvailabilityV2.ACTUAL_RECEIPT


def _supported_context() -> S5CrowdingContextV2:
    f = funding(at=T0, rate="0.01", kind=FundingKindV2.CURRENT)
    open_interest = oi(event=T0, quantity="100")
    return build_s5_crowding_context(
        instrument=DERIVATIVE_KEY, cutoff_ns=T0, funding=(f,), open_interest=(open_interest,),
        liquidity_state="THIN",
    )


def test_cascade_stages_accept_async_order_and_preserve_late_oi_availability() -> None:
    context = _supported_context()
    end_event = T0 + 22
    start_event = end_event - 15 * 60 * NS
    start = oi(event=start_event, quantity="100", available=T0)
    end = oi(event=end_event, quantity="80", available=T0 + 90)
    oi_evidence = oi_change_15m_evidence((start, end), cutoff_ns=T0 + 90, instrument=DERIVATIVE_KEY)
    assert oi_evidence is not None
    policy = S5ContinuationResearchV2(confirmation_latency_ns=5)
    policy.add_vulnerability(event_at_ns=T0, available_at_ns=T0, context=context,
                              structure_refs=(ref("structure"),), liquidity_refs=(ref("liquidity"),))
    policy.add_break(event_at_ns=T0 + 10, available_at_ns=T0 + 20,
                     structure_ref=ref("break"), adverse_response_ref=ref("response"))
    before_oi = policy.artifact(cutoff_ns=T0 + 80)
    assert before_oi.state == "BREAK"
    assert before_oi.break_evidence is not None and before_oi.break_evidence.available_at_ns == T0 + 20
    policy.add_deleveraging(observation=end, event_at_ns=end_event, available_at_ns=T0 + 90,
                            oi_change_evidence=oi_evidence)
    after_oi = policy.artifact(cutoff_ns=T0 + 90)
    assert after_oi.state == "DELEVERAGING_EVIDENCE"
    assert after_oi.deleveraging_evidence is not None
    assert after_oi.deleveraging_evidence.event_at_ns == end_event
    assert after_oi.deleveraging_evidence.available_at_ns == T0 + 90
    assert after_oi.confirmation is None
    policy.confirm(event_at_ns=T0 + 91, available_at_ns=T0 + 91,
                   confirmation_ref=ref("too-early-confirm"))
    assert policy.artifact(cutoff_ns=T0 + 91).confirmation is None
    policy.confirm(event_at_ns=T0 + 96, available_at_ns=T0 + 96,
                   confirmation_ref=ref("confirmed"))
    final = policy.artifact(cutoff_ns=T0 + 96)
    assert final.state == "CONTINUATION_CONFIRMED"
    assert final.confirmation_latency_ns == 6
    assert final.confirmation is not None
    assert final.confirmation.available_at_ns > after_oi.deleveraging_evidence.available_at_ns
    assert final.exact_action_status == "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT"
    assert final.to_dict()["selector_influence"] == "ZERO"


def test_cascade_without_vulnerability_or_qualified_deleveraging_fails_closed() -> None:
    policy = S5ContinuationResearchV2()
    policy.add_break(event_at_ns=T0, available_at_ns=T0,
                     structure_ref=ref("structure-break"), adverse_response_ref=ref("flow"))
    no_vulnerability = policy.artifact(cutoff_ns=T0)
    assert no_vulnerability.state == "BREAK"
    assert no_vulnerability.fallback_state.startswith("TEST GATE")
    assert no_vulnerability.confirmation is None


def _valid_s4_feature():
    b = s4_book(warmup=0, stale=1000, cadence=10)
    b.apply_snapshot(s4_snapshot(T0))
    b.apply_delta(s4_delta(T0 + 10, 101))
    feature = b.feature(cutoff_ns=T0 + 10)
    assert feature.estimable
    return b, feature


def _s4_absorption(feature) -> S4AbsorptionHypothesisV2:
    return S4AbsorptionHypothesisV2(
        feature.content_hash, feature.cutoff_ns, "ABSORPTION_HYPOTHESIS", Decimal("5"),
        Decimal("1"), Decimal("0.9"), Decimal("2"), feature.input_refs[:3],
        "S4_EXPECTED_RESPONSE_PRIOR_ONLY_V1", ref("prior-fit"),
        "ENGINEERING_RESEARCH_DEFAULT_UNQUALIFIED",
    )


def _liquidation_baseline_and_window(feature):
    instrument = feature.instrument
    start = feature.cutoff_ns - 21 * 60 * NS
    windows = tuple(LiquidationWindowTotalV2(
        instrument, "BYBIT_LIQ", start + i * 60 * NS, start + (i + 1) * 60 * NS,
        start + (i + 1) * 60 * NS, Decimal(i + 1), QTY_UNIT,
        LiquidationCoverageV2.QUALIFIED, "HEALTHY_CURRENT", HEALTH, (ref(["window", i]),),
        "allLiquidation.BTCUSDT", MATRIX.content_hash,
    ) for i in range(20))
    baseline = fit_s5_liquidation_baseline(
        windows, instrument=instrument, source_id="BYBIT_LIQ", cutoff_ns=feature.cutoff_ns,
    )
    assert baseline is not None
    window = LiquidationWindowTotalV2(
        instrument, "BYBIT_LIQ", feature.cutoff_ns - 60 * NS, feature.cutoff_ns,
        feature.cutoff_ns, Decimal("100"), QTY_UNIT, LiquidationCoverageV2.QUALIFIED,
        "HEALTHY_CURRENT", HEALTH, (ref("liquidation-current-window"),),
        "allLiquidation.BTCUSDT", MATRIX.content_hash,
    )
    return baseline, window


def test_reversal_is_separate_requires_exhaustion_absorption_and_confirmation() -> None:
    _, feature = _valid_s4_feature()
    absorption = _s4_absorption(feature)
    baseline, window = _liquidation_baseline_and_window(feature)
    assert baseline.coverage == LiquidationCoverageV2.CENSORED
    assert window.coverage == LiquidationCoverageV2.CENSORED
    only_spike = evaluate_s5_reversal(
        cutoff_ns=feature.cutoff_ns, s4=feature, absorption=None,
        liquidation_window=window, liquidation_baseline=baseline,
        exhaustion=None, reclaim=None, flow_reversal=None,
    )
    assert only_spike.state == "NOT_ESTIMABLE_LIQUIDATION_COVERAGE"
    assert only_spike.missing_reason == "LIQUIDATION_COVERAGE_UNKNOWN_CENSORED_OR_DEGRADED"
    exhaustion = StageEvidenceV2("EXHAUSTION", T0, feature.cutoff_ns,
                                 (ref("exhaustion"),), "EXHAUSTION_CONFIRMED")
    still_waiting = evaluate_s5_reversal(
        cutoff_ns=feature.cutoff_ns, s4=feature, absorption=absorption,
        liquidation_window=window, liquidation_baseline=baseline,
        exhaustion=exhaustion, reclaim=None, flow_reversal=None,
    )
    assert still_waiting.state == "NOT_ESTIMABLE_LIQUIDATION_COVERAGE"
    reclaim = StageEvidenceV2("RECLAIM", T0 + 10, feature.cutoff_ns,
                              (ref("reclaim"),), "RECLAIM_CONFIRMED")
    confirmed = evaluate_s5_reversal(
        cutoff_ns=feature.cutoff_ns, s4=feature, absorption=absorption,
        liquidation_window=window, liquidation_baseline=baseline,
        exhaustion=exhaustion, reclaim=reclaim, flow_reversal=None,
    )
    assert confirmed.state == "NOT_ESTIMABLE_LIQUIDATION_COVERAGE"
    assert confirmed.exact_action_status == "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT"
    assert confirmed.liquidation_coverage == LiquidationCoverageV2.CENSORED
    stages = {item.stage: item for item in confirmed.stage_evidence}
    assert {"LIQUIDATION_BASELINE", "LIQUIDATION_INTENSITY", "S4_FLOW_STATE",
            "S4_ABSORPTION", "EXHAUSTION", "RECLAIM"} <= set(stages)
    assert stages["LIQUIDATION_INTENSITY"].event_at_ns == window.window_end_ns
    assert stages["LIQUIDATION_INTENSITY"].available_at_ns == window.available_at_ns
    assert stages["RECLAIM"].available_at_ns == reclaim.available_at_ns
    assert S5_REVERSAL_POLICY_HASH != S5_CONTINUATION_POLICY_HASH
    assert confirmed.to_dict()["selector_influence"] == "ZERO"


def test_s4_gap_propagates_to_post_cascade_reversal() -> None:
    b = s4_book(warmup=0, stale=1000, cadence=10)
    b.apply_snapshot(s4_snapshot(T0))
    b.apply_delta(s4_delta(T0 + 10, 101))
    b.apply_delta(s4_delta(T0 + 20, 103))
    invalid = b.feature(cutoff_ns=T0 + 20)
    assert invalid.sequence_state == BookStateV2.GAP_DETECTED
    result = evaluate_s5_reversal(
        cutoff_ns=T0 + 20, s4=invalid, absorption=None, liquidation_window=None,
        liquidation_baseline=None, exhaustion=None, reclaim=None, flow_reversal=None,
    )
    assert result.state == "NOT_ESTIMABLE_S4_FLOW_UNAVAILABLE"
    assert result.s4_state == "GAP_DETECTED"
    assert result.missing_reason == "S4_GAP_WARMUP_OR_CUTOFF_UNAVAILABLE"


def _bar(open_at: int, *, op: str, high: str, low: str, close: str, volume: str):
    from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
    from atlas.v2.data.raw import RawObservationV2

    close_at = open_at + BarIntervalV2.M15.duration_ns
    raw = RawObservationV2.build(
        instrument_revision=KEY.contract_revision, source_id="BAR_FIXTURE", event_type="BAR_15M",
        event_at_ns=close_at, received_at_ns=close_at, ingested_at_ns=close_at,
        available_at_ns=close_at, payload={"open": op, "high": high, "low": low, "close": close},
        translation_version="session022-test", sequence=str(open_at),
    )
    return CausalBarV2(raw, BarIntervalV2.M15, open_at, close_at,
                       Decimal(op), Decimal(high), Decimal(low), Decimal(close), Decimal(volume), True)


def test_spring_upthrust_effort_result_and_time_context_are_measurable() -> None:
    step = 15 * 60 * NS
    base_open = (T0 // step) * step
    bars = tuple(_bar(base_open + i * step, op="100", high="101", low="99", close="100",
                      volume=str(i + 1)) for i in range(4)) + (
        _bar(base_open + 4 * step, op="100", high="101", low="98", close="100.5", volume="8"),
    )
    structure = build_structure_context(bars=bars, cutoff_ns=bars[-1].close_at_ns, range_lookback=4)
    assert structure.spring_upthrust_state == "SPRING_FALSE_BREAK_AND_RETURN"
    assert structure.effort_kind == "BAR_VOLUME_PROXY"
    assert structure.effort_result_fit_ref is not None
    assert STRUCTURE_CONTEXT_POLICY_HASH != ""
    cutoff_structure = bars[-1].close_at_ns
    accepted = CausalStructureReferenceV2(
        "BOS", ref("accepted-causal-bos"), cutoff_structure - 20, cutoff_structure - 10,
        "ACCEPTED_SMC_STRUCTURE_V1",
    )
    future_structure = CausalStructureReferenceV2(
        "FVG", ref("future-fvg"), cutoff_structure + 1, cutoff_structure + 2,
        "ACCEPTED_SMC_STRUCTURE_V1",
    )
    with_accepted = build_structure_context(
        bars=bars, cutoff_ns=cutoff_structure, range_lookback=4,
        accepted_structure_evidence=(accepted,),
    )
    with_future_tail = build_structure_context(
        bars=bars, cutoff_ns=cutoff_structure, range_lookback=4,
        accepted_structure_evidence=(accepted, future_structure),
    )
    assert with_accepted.accepted_structure_refs == (accepted.content_ref,)
    assert with_future_tail.content_hash == with_accepted.content_hash
    upthrust_bars = tuple(_bar(base_open + i * step, op="100", high="101", low="99", close="100",
                               volume=str(i + 1)) for i in range(4)) + (
        _bar(base_open + 4 * step, op="100", high="102", low="99", close="100.5", volume="8"),
    )
    upthrust = build_structure_context(bars=upthrust_bars,
                                       cutoff_ns=upthrust_bars[-1].close_at_ns, range_lookback=4)
    assert upthrust.spring_upthrust_state == "UPTHRUST_FALSE_BREAK_AND_RETURN"

    cutoff = int(datetime(2026, 9, 28, 1, 30, tzinfo=UTC).timestamp() * NS)
    funding_record = funding(at=cutoff - 100, rate="0.01", kind=FundingKindV2.CURRENT)
    session = TimedSessionV2("ASIA", 60, 240, ref("session-declaration"))
    time_context, killzone = build_time_context(cutoff_ns=cutoff, sessions=(session,), funding=(funding_record,))
    assert time_context.utc_minute_of_day == 90
    assert time_context.weekday_utc == 0 and not time_context.weekend_utc
    assert time_context.session_overlaps == ("ASIA",)
    assert funding_record.next_funding_at_ns is not None
    assert time_context.time_to_funding_ns == funding_record.next_funding_at_ns - cutoff
    assert killzone.windows == ("ASIA_RESEARCH_WINDOW",)
    assert killzone.to_dict()["distinct_from_calendar_features"] is True
    assert TIME_CONTEXT_POLICY_HASH != KILLZONE_POLICY_HASH
    sunday_night = int(datetime(2026, 9, 27, 23, 50, tzinfo=UTC).timestamp() * NS)
    overnight = TimedSessionV2("ASIA_CROSS_MIDNIGHT", 1380, 120, ref("overnight-session"))
    weekend, _ = build_time_context(cutoff_ns=sunday_night, sessions=(overnight,))
    assert weekend.weekday_utc == 6 and weekend.weekend_utc
    assert weekend.session_overlaps == ("ASIA_CROSS_MIDNIGHT",)
    session_end = int(datetime(2026, 9, 28, 4, 0, tzinfo=UTC).timestamp() * NS)
    at_end, _ = build_time_context(cutoff_ns=session_end, sessions=(session,))
    assert at_end.session_overlaps == ()


def test_effort_result_can_use_only_prior_cutoff_known_s4_flow_and_response() -> None:
    from atlas.v2.data.bars import BarIntervalV2

    step = BarIntervalV2.M15.duration_ns
    base_open = (T0 // step) * step
    cutoff = base_open + 4 * step
    bars = tuple(_bar(base_open + i * step, op="100", high="101", low="99", close="100",
                      volume=str(i + 1)) for i in range(4))
    from .test_session022_s4_microstructure import qualified_feature

    flow_features = tuple(qualified_feature(cutoff_ns=cutoff - (3 - index),
                                            trade_quantity=str(index + 1))
                          for index in range(4))
    flow_rows = tuple(S4FlowResponseObservationV2(
        feature, feature.cutoff_ns,
        Decimal(next(row[1] for row in feature.flow_price_response_windows if row[0] == "30")),
        Decimal(next(row[1] for row in feature.price_response_windows if row[0] == "30")),
        feature.content_hash,
    ) for feature in flow_features)
    current = flow_features[-1]
    result = build_structure_context(bars=bars, cutoff_ns=cutoff, s4=current,
                                     s4_flow_history=flow_rows)
    assert result.effort_kind == "SIGNED_AGGRESSIVE_FLOW"
    assert result.effort_result_state != "NOT_ESTIMABLE_PRIOR_FIT_SUPPORT"
    assert current.content_hash in result.input_refs
    assert all(row.feature.content_hash in result.input_refs for row in flow_rows)
    future = qualified_feature(cutoff_ns=cutoff + 1, trade_quantity="99")
    future_row = S4FlowResponseObservationV2(
        future, cutoff + 1,
        Decimal(next(row[1] for row in future.flow_price_response_windows if row[0] == "30")),
        Decimal(next(row[1] for row in future.price_response_windows if row[0] == "30")),
        future.content_hash,
    )
    with_future = build_structure_context(bars=bars, cutoff_ns=cutoff, s4=current,
                                          s4_flow_history=flow_rows + (future_row,))
    assert with_future.content_hash == result.content_hash


def test_time_context_s7_event_needs_accepted_clear_gate_and_future_revision_is_invisible(tmp_path) -> None:
    from atlas.v2.news.events import EventGateStateV2, EventSafetyGateBuilderV2

    cutoff = CUTOFF
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        coverage = _coverage(repository, cutoff)
        abnormality = _normal_abnormality(repository, cutoff)
        event = replace(_macro_event(repository, cutoff + 40 * 60 * NS, cutoff),
                        schedule_revision=coverage.revision)
        gate = EventSafetyGateBuilderV2(repository).evaluate(
            key=KEY, cutoff_ns=cutoff, coverage=coverage, scheduled_events=(event,),
            abnormality=abnormality, incidents=(),
        )
        assert gate.state == EventGateStateV2.CLEAR
        accepted, _ = build_time_context(
            cutoff_ns=cutoff, calendar_coverage=coverage, scheduled_events=(event,), s7_gate=gate,
        )
        assert accepted.macro_event_proximity_ns == 40 * 60 * NS
        assert accepted.macro_event_ref == event.content_hash
        mismatched_schedule = replace(coverage, revision="schedule-next")
        mismatch_context, _ = build_time_context(
            cutoff_ns=cutoff, calendar_coverage=mismatched_schedule,
            scheduled_events=(event,), s7_gate=gate,
        )
        assert mismatch_context.macro_event_ref is None
        assert mismatch_context.macro_state == "NOT_ESTIMABLE_S7_GATE_OR_VERIFIED_COVERAGE_MISSING"
        later_revision = replace(event, schedule_revision="calendar-rev2", available_at_ns=cutoff + 1,
                                 received_at_ns=cutoff + 1, scheduled_at_ns=cutoff + 20 * 60 * NS)
        earlier, _ = build_time_context(
            cutoff_ns=cutoff, calendar_coverage=coverage, scheduled_events=(event, later_revision), s7_gate=gate,
        )
        assert earlier.content_hash == accepted.content_hash
        future_only, _ = build_time_context(
            cutoff_ns=cutoff, calendar_coverage=coverage, scheduled_events=(later_revision,), s7_gate=gate,
        )
        assert future_only.macro_event_ref is None
        assert future_only.macro_state == "NO_S7_EVENT_IN_COVERED_WINDOW"


def test_session022_artifacts_leave_accepted_candidate_set_bytes_and_selector_unchanged(tmp_path) -> None:
    snapshot = universe()
    action = candidate()
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        index(repository, action)
        rank_ref = evidence(repository, action, snapshot, 1)
        before = assemble(repository, snapshot, (action,), {action.candidate_id: (rank_ref,)})
        for kind, body in (
            ("S4FeatureArtifactV2", {"policy_id": "S4_MICROSTRUCTURE_FEATURE_V1"}),
            ("S5CrowdingContextV2", {"policy_id": "S5_CROWDING_CONTEXT_V1"}),
            ("S5ContinuationArtifactV2", {"policy_id": "S5_DELEVERAGING_CONTINUATION_SHADOW_V1"}),
            ("S5ReversalArtifactV2", {"policy_id": "S5_POST_CASCADE_REVERSAL_SHADOW_V1"}),
            ("StructureContextArtifactV2", {"policy_id": "STRUCTURE_WYCKOFF_MEASURABLE_CONTEXT_V1"}),
        ):
            artifact = type("ResearchArtifact", (), {
                "to_dict": lambda self, value=body: {**value, "selector_influence": "ZERO"},
            })()
            from atlas.v2.data.research_artifacts import persist_research_artifact

            # Exercise the artifact bridge's existing atlas-ops single writer.
            persist_research_artifact(repository, kind, artifact, available_at_ns=CUTOFF)
        after = assemble(repository, snapshot, (action,), {action.candidate_id: (rank_ref,)})
        assert after.to_canonical_json() == before.to_canonical_json()
        assert after.content_hash == before.content_hash
        assert after.selection_policy_hash == "36f8fd58c9e791ea580a1855f9e98555131e0828604d484eda3df4c7d5529cac"
