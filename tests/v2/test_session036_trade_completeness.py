"""Exact trade completeness requires native boundaries and accepted evidence."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.raw import AvailabilityClassV2
from atlas.v2.data.trade_completeness import (
    EXACT_SEQUENCE_SEMANTICS_V1,
    NativeTradePrefixV1,
    PersistedTradeEvidenceCheckV1,
    SequencedNativeTradeV1,
    TradeCompletenessRequestV1,
    TradeSourceContractV1,
    TradeWindowEvidenceV1,
    collect_exact_trade_window_v1,
    validate_trade_window_v1,
)
from atlas.v2.instruments import InstrumentKeyV2
from atlas.v2.strategies.s3_mean_reversion import CausalTradeV2

KEY = InstrumentKeyV2("BYBIT", "MAINNET", "LINEAR_PERPETUAL", "BTCUSDT", "BTC", "USDT", "USDT", "a" * 64)


def _fixture() -> tuple[TradeSourceContractV1, TradeCompletenessRequestV1, TradeWindowEvidenceV1]:
    contract = TradeSourceContractV1(KEY, "EXACT_FIXTURE_ONLY", "b" * 64, "c" * 64, 5,
                                   EXACT_SEQUENCE_SEMANTICS_V1, True)
    request = TradeCompletenessRequestV1(KEY, contract.source_id, contract.content_hash, 10, 20, 30, 8)
    observations = tuple(SequencedNativeTradeV1(sequence, CausalTradeV2(KEY,
        sha256_json({"raw-trade": sequence}), contract.source_id, str(sequence), sequence + 6, 21, 22,
        Decimal("100"), Decimal("2"))) for sequence in (5, 6))
    evidence = TradeWindowEvidenceV1(request.content_hash,
        NativeTradePrefixV1(10, 4, 11, 12, "d" * 64), NativeTradePrefixV1(20, 6, 21, 22, "e" * 64),
        observations, "f" * 64, recovery_epoch=3)
    return contract, request, evidence


def _accept(check: PersistedTradeEvidenceCheckV1) -> bool:
    # Fixture stands in for accepted immutable indexes and hash-verified bytes.
    assert check.instrument == KEY and check.source_id == "EXACT_FIXTURE_ONLY"
    assert check.evidence_cutoff_ns == 30
    if check.purpose in {"SOURCE_HEALTH", "NATIVE_REPAIR"}:
        assert check.recovery_epoch == 3
    if check.purpose == "SOURCE_QUALIFICATION":
        assert check.typed_content_hash == _fixture()[0].definition_hash
        assert check.available_at_ns == 5
    return True


def test_complete_and_empty_windows_have_native_boundary_proof_and_hashes() -> None:
    contract, request, evidence = _fixture()
    result = validate_trade_window_v1(contract, request, evidence, accepted_evidence=_accept)
    assert result.complete and result.reason_codes == () and result.evidence_ref == evidence.content_hash
    assert result.trades == tuple(row.trade for row in evidence.observations)
    empty = replace(evidence, end_prefix=replace(evidence.end_prefix, last_sequence=4), observations=())
    assert validate_trade_window_v1(contract, request, empty, accepted_evidence=_accept).complete
    assert empty.content_hash != evidence.content_hash


@pytest.mark.parametrize("mutation,reason", [
    ("missing_row", "TRADE_SEQUENCE_COVERAGE_INCOMPLETE"),
    ("sequence_gap", "TRADE_SEQUENCE_COVERAGE_INCOMPLETE"),
    ("wrong_revision", "TRADE_NATIVE_IDENTITY_OR_WINDOW_INVALID"),
    ("future_event", "TRADE_NATIVE_IDENTITY_OR_WINDOW_INVALID"),
    ("late_trade", "TRADE_EVIDENCE_AFTER_CUTOFF"),
    ("late_boundary", "TRADE_EVIDENCE_AFTER_CUTOFF"),
    ("reconstructed", "TRADE_NATIVE_IDENTITY_OR_WINDOW_INVALID"),
    ("wrong_boundary", "TRADE_WINDOW_BOUNDARY_MISMATCH"),
    ("duplicate_identity", "TRADE_DUPLICATE_IDENTITY"),
    ("wrong_request", "TRADE_REQUEST_IDENTITY_MISMATCH"),
])
def test_wrong_running_conclusions_fail_closed(mutation: str, reason: str) -> None:
    contract, request, evidence = _fixture()
    row = evidence.observations[0]
    if mutation == "missing_row":
        evidence = replace(evidence, observations=evidence.observations[:1])
    elif mutation == "sequence_gap":
        evidence = replace(evidence, observations=(replace(row, sequence=99), evidence.observations[1]))
    elif mutation == "wrong_revision":
        evidence = replace(evidence, observations=(replace(row, trade=replace(row.trade,
            key=replace(KEY, contract_revision="1" * 64))), evidence.observations[1]))
    elif mutation == "future_event":
        evidence = replace(evidence, observations=(replace(row, trade=replace(row.trade,
            event_at_ns=25, received_at_ns=26, available_at_ns=27)), evidence.observations[1]))
    elif mutation == "late_trade":
        evidence = replace(evidence, observations=(replace(row, trade=replace(row.trade,
            received_at_ns=31, available_at_ns=32)), evidence.observations[1]))
    elif mutation == "late_boundary":
        evidence = replace(evidence, end_prefix=replace(evidence.end_prefix, available_at_ns=31))
    elif mutation == "reconstructed":
        evidence = replace(evidence, observations=(replace(row, trade=replace(row.trade,
            availability_class=AvailabilityClassV2.RECONSTRUCTED_MARKET, replay_available_at_ns=12)),
            evidence.observations[1]))
    elif mutation == "wrong_boundary":
        evidence = replace(evidence, start_prefix=replace(evidence.start_prefix, before_at_ns=9))
    elif mutation == "duplicate_identity":
        evidence = replace(evidence, observations=(row, replace(row, sequence=6)))
    elif mutation == "wrong_request":
        evidence = replace(evidence, request_ref="9" * 64)
    result = validate_trade_window_v1(contract, request, evidence, accepted_evidence=_accept)
    assert not result.complete and reason in result.reason_codes and result.trades == ()


def test_missing_unaccepted_and_unqualified_evidence_cannot_claim_completeness() -> None:
    contract, request, evidence = _fixture()
    assert not validate_trade_window_v1(contract, request, None, accepted_evidence=_accept).complete
    for purpose in ("SOURCE_HEALTH", "NATIVE_TRADE", "PRODUCT_METADATA", "SOURCE_QUALIFICATION", "NATIVE_PREFIX"):
        result = validate_trade_window_v1(contract, request, evidence,
            accepted_evidence=lambda check, rejected=purpose: check.purpose != rejected)
        assert result.reason_codes == ("TRADE_EVIDENCE_NOT_ACCEPTED",)
    unqualified = replace(contract, sequence_semantics="BYBIT_GROUPED_CROSS_SEQUENCE")
    unqualified_request = replace(request, source_contract_ref=unqualified.content_hash)
    result = validate_trade_window_v1(unqualified, unqualified_request, evidence, accepted_evidence=_accept)
    assert "EXACT_TRADE_COMPLETENESS_UNSUPPORTED" in result.reason_codes


class _Port:
    def __init__(self, contract: TradeSourceContractV1, evidence: TradeWindowEvidenceV1) -> None:
        self.contract = contract
        self.evidence = evidence
        self.calls: list[str] = []
        self.failure: Exception | None = None

    def read_window(self, request: TradeCompletenessRequestV1) -> TradeWindowEvidenceV1:
        self.calls.append("read")
        if self.failure:
            raise self.failure
        return self.evidence

    def repair_window(self, request: TradeCompletenessRequestV1) -> TradeWindowEvidenceV1:
        self.calls.append("repair")
        return replace(self.evidence, repair_evidence_ref="8" * 64)


def test_no_fallback_no_hidden_repair_and_late_repair_cannot_rescue_old_decision() -> None:
    contract, request, evidence = _fixture()
    port = _Port(contract, evidence)
    assert collect_exact_trade_window_v1(port, request, accepted_evidence=_accept).complete
    assert port.calls == ["read"]
    assert collect_exact_trade_window_v1(port, request, accepted_evidence=_accept, repair=True).complete
    port.evidence = replace(evidence, end_prefix=replace(evidence.end_prefix, available_at_ns=35))
    result = collect_exact_trade_window_v1(port, request, accepted_evidence=_accept, repair=True)
    assert not result.complete and "TRADE_EVIDENCE_AFTER_CUTOFF" in result.reason_codes
    assert request.evidence_cutoff_ns == 30
    port.failure = TimeoutError("untrusted transport message")
    assert collect_exact_trade_window_v1(port, request, accepted_evidence=_accept).reason_codes == (
        "EXACT_TRADE_SOURCE_TIMEOUT",)
    port.failure = ConnectionError("untrusted transport message")
    assert collect_exact_trade_window_v1(port, request, accepted_evidence=_accept).reason_codes == (
        "EXACT_TRADE_SOURCE_UNAVAILABLE",)
    port.failure = ValueError("malformed native trade output")
    assert collect_exact_trade_window_v1(port, request, accepted_evidence=_accept).reason_codes == (
        "EXACT_TRADE_SOURCE_INVALID_OUTPUT",)
    assert collect_exact_trade_window_v1(None, request, accepted_evidence=_accept).reason_codes == (
        "EXACT_TRADE_SOURCE_UNAVAILABLE",)


def test_unqualified_contract_and_unsupported_repair_do_not_start_transport() -> None:
    contract, request, evidence = _fixture()
    contract = replace(contract, sequence_semantics="GROUPED_MESSAGES")
    request = replace(request, source_contract_ref=contract.content_hash)
    port = _Port(contract, evidence)
    assert not collect_exact_trade_window_v1(port, request, accepted_evidence=_accept).complete
    assert port.calls == []
    contract = replace(contract, sequence_semantics=EXACT_SEQUENCE_SEMANTICS_V1, repair_supported=False)
    port.contract = contract
    request = replace(request, source_contract_ref=contract.content_hash)
    assert collect_exact_trade_window_v1(port, request, accepted_evidence=_accept, repair=True).reason_codes == (
        "EXACT_TRADE_REPAIR_UNSUPPORTED",)
    assert port.calls == []


def test_request_budget_and_native_chronology_are_enforced() -> None:
    _, request, evidence = _fixture()
    with pytest.raises(ValueError, match="budget"):
        replace(request, max_rows=100_001)
    with pytest.raises(ValueError, match="chronology"):
        replace(evidence.start_prefix, available_at_ns=1)
    with pytest.raises(ValueError, match="immutable"):
        replace(evidence, observations=list(evidence.observations))  # type: ignore[arg-type]
