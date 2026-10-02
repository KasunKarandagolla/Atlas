"""Provider-neutral, bounded port for exact native trade coverage.

An adapter needs a separately qualified contract promising a contiguous,
instrument-scoped sequence for *every* trade and native prefix watermarks.
Transport health, grouped cross-sequences and recent-trade REST snapshots do
not satisfy that contract. This module qualifies no venue or feed.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from .._serialization import nonblank, sha256_json, sha256_ref, timestamp
from ..instruments import InstrumentKeyV2
from ..strategies.s3_mean_reversion import CausalTradeV2
from .raw import AvailabilityClassV2

MAX_TRADE_WINDOW_ROWS_V1 = 100_000
EXACT_SEQUENCE_SEMANTICS_V1 = "INSTRUMENT_ALL_TRADES_CONTIGUOUS_SEQUENCE_WITH_NATIVE_PREFIX_WATERMARK"


@dataclass(frozen=True)
class TradeCompletenessRequestV1:
    instrument: InstrumentKeyV2
    source_id: str
    source_contract_ref: str
    start_at_ns: int
    end_at_ns: int
    evidence_cutoff_ns: int
    max_rows: int = MAX_TRADE_WINDOW_ROWS_V1

    def __post_init__(self) -> None:
        if not isinstance(self.instrument, InstrumentKeyV2):
            raise ValueError("exact instrument revision is required")
        nonblank(self.source_id, field="source_id")
        sha256_ref(self.source_contract_ref, field="source_contract_ref")
        for name in ("start_at_ns", "end_at_ns", "evidence_cutoff_ns"):
            timestamp(getattr(self, name), field=name)
        if not self.start_at_ns < self.end_at_ns <= self.evidence_cutoff_ns:
            raise ValueError("trade window must be causal and half-open")
        if type(self.max_rows) is not int or not 1 <= self.max_rows <= MAX_TRADE_WINDOW_ROWS_V1:
            raise ValueError("trade request exceeds the bounded row budget")

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, "instrument": self.instrument.to_dict(),
                "source_id": self.source_id, "source_contract_ref": self.source_contract_ref,
                "start_at_ns": self.start_at_ns, "end_at_ns": self.end_at_ns,
                "evidence_cutoff_ns": self.evidence_cutoff_ns, "max_rows": self.max_rows}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class TradeSourceContractV1:
    """Immutable identity of an independently qualified source contract.

    Native prefix watermarks assert the last sequence strictly before a UTC
    timestamp, including intervals with zero trades. Sequence continuity alone
    cannot prove that the beginning or end of a requested interval is covered.
    """

    instrument: InstrumentKeyV2
    source_id: str
    metadata_ref: str
    qualification_ref: str
    qualified_at_ns: int
    sequence_semantics: str
    repair_supported: bool

    def __post_init__(self) -> None:
        if not isinstance(self.instrument, InstrumentKeyV2):
            raise ValueError("exact instrument revision is required")
        nonblank(self.source_id, field="source_id")
        sha256_ref(self.metadata_ref, field="metadata_ref")
        sha256_ref(self.qualification_ref, field="qualification_ref")
        timestamp(self.qualified_at_ns, field="qualified_at_ns")
        nonblank(self.sequence_semantics, field="sequence_semantics")
        if type(self.repair_supported) is not bool:
            raise ValueError("repair capability must be explicit")

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, "instrument": self.instrument.to_dict(),
                "source_id": self.source_id, "metadata_ref": self.metadata_ref,
                "qualification_ref": self.qualification_ref, "qualified_at_ns": self.qualified_at_ns,
                "sequence_semantics": self.sequence_semantics, "repair_supported": self.repair_supported}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @property
    def definition_hash(self) -> str:
        """Definition bound by qualification, without a circular self-reference."""
        body = self.to_dict()
        del body["qualification_ref"]
        return sha256_json(body)


@dataclass(frozen=True)
class NativeTradePrefixV1:
    """Source assertion of the final sequence strictly before a timestamp."""

    before_at_ns: int
    last_sequence: int
    received_at_ns: int
    available_at_ns: int
    raw_evidence_ref: str

    def __post_init__(self) -> None:
        for name in ("before_at_ns", "received_at_ns", "available_at_ns"):
            timestamp(getattr(self, name), field=name)
        if not self.before_at_ns <= self.received_at_ns <= self.available_at_ns:
            raise ValueError("native watermark chronology is invalid")
        if type(self.last_sequence) is not int or self.last_sequence < 0:
            raise ValueError("native sequence must be nonnegative")
        sha256_ref(self.raw_evidence_ref, field="raw_evidence_ref")

    def to_dict(self) -> dict[str, Any]:
        return dict(vars(self))


@dataclass(frozen=True)
class SequencedNativeTradeV1:
    sequence: int
    trade: CausalTradeV2

    def __post_init__(self) -> None:
        if type(self.sequence) is not int or self.sequence < 0 or not isinstance(self.trade, CausalTradeV2):
            raise ValueError("native sequence and typed trade are required")

    def to_dict(self) -> dict[str, Any]:
        return {"sequence": self.sequence, "trade": self.trade.to_dict()}


@dataclass(frozen=True)
class TradeWindowEvidenceV1:
    request_ref: str
    start_prefix: NativeTradePrefixV1
    end_prefix: NativeTradePrefixV1
    observations: tuple[SequencedNativeTradeV1, ...]
    source_health_ref: str
    recovery_epoch: int = 0
    repair_evidence_ref: str | None = None

    def __post_init__(self) -> None:
        sha256_ref(self.request_ref, field="request_ref")
        sha256_ref(self.source_health_ref, field="source_health_ref")
        if type(self.recovery_epoch) is not int or self.recovery_epoch < 0:
            raise ValueError("source recovery epoch must be nonnegative")
        if self.repair_evidence_ref is not None:
            sha256_ref(self.repair_evidence_ref, field="repair_evidence_ref")
        if not isinstance(self.observations, tuple) or len(self.observations) > MAX_TRADE_WINDOW_ROWS_V1:
            raise ValueError("trade evidence must be immutable and bounded")
        if (not isinstance(self.start_prefix, NativeTradePrefixV1)
                or not isinstance(self.end_prefix, NativeTradePrefixV1)
                or any(not isinstance(row, SequencedNativeTradeV1) for row in self.observations)):
            raise ValueError("typed native boundaries and observations are required")

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, "request_ref": self.request_ref,
                "start_prefix": self.start_prefix.to_dict(), "end_prefix": self.end_prefix.to_dict(),
                "observations": [row.to_dict() for row in self.observations],
                "source_health_ref": self.source_health_ref, "recovery_epoch": self.recovery_epoch,
                "repair_evidence_ref": self.repair_evidence_ref}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


class ExactTradeSourcePortV1(Protocol):
    """No framework/provider objects cross this boundary.

    Repair returns newly received evidence with its actual availability. It
    never rewrites an old request's cutoff or original archived observations.
    Timeout, missing feed and overflow return no evidence; no fallback feed.
    """

    @property
    def contract(self) -> TradeSourceContractV1: ...

    def read_window(self, request: TradeCompletenessRequestV1) -> TradeWindowEvidenceV1 | None: ...

    def repair_window(self, request: TradeCompletenessRequestV1) -> TradeWindowEvidenceV1 | None: ...


@dataclass(frozen=True)
class TradeWindowValidationV1:
    complete: bool
    reason_codes: tuple[str, ...]
    evidence_ref: str | None
    trades: tuple[CausalTradeV2, ...] = ()


@dataclass(frozen=True)
class PersistedTradeEvidenceCheckV1:
    evidence_ref: str
    instrument: InstrumentKeyV2
    source_id: str
    source_contract_ref: str
    evidence_cutoff_ns: int
    purpose: str
    recovery_epoch: int | None = None
    received_at_ns: int | None = None
    available_at_ns: int | None = None
    typed_content_hash: str | None = None


# Called against accepted, hash-verified persistence, including native payload
# semantics and source-health validity. No invented timestamps for reference
# types whose exact timestamps must be obtained from the accepted index.
AcceptedEvidenceCheckV1 = Callable[[PersistedTradeEvidenceCheckV1], bool]


def validate_trade_window_v1(
    contract: TradeSourceContractV1,
    request: TradeCompletenessRequestV1,
    evidence: TradeWindowEvidenceV1 | None,
    *,
    accepted_evidence: AcceptedEvidenceCheckV1,
    repaired: bool = False,
) -> TradeWindowValidationV1:
    """Fail closed on missing, late, mismatched or incomplete native proof.

    ``accepted_evidence`` must validate an accepted index *and* archived bytes;
    checking that a reference exists is insufficient. Source-health references
    additionally require a healthy exact source epoch at the request cutoff.
    """
    if type(repaired) is not bool:
        raise ValueError("repair validation must be explicit")
    reasons: set[str] = set()
    if (contract.content_hash != request.source_contract_ref or contract.instrument != request.instrument
            or contract.source_id != request.source_id):
        reasons.add("TRADE_SOURCE_CONTRACT_MISMATCH")
    if contract.sequence_semantics != EXACT_SEQUENCE_SEMANTICS_V1:
        reasons.add("EXACT_TRADE_COMPLETENESS_UNSUPPORTED")
    if contract.qualified_at_ns > request.evidence_cutoff_ns:
        reasons.add("TRADE_CONTRACT_NOT_CAUSALLY_QUALIFIED")
    if evidence is None:
        reasons.add("TRADE_WINDOW_EVIDENCE_MISSING")
        return TradeWindowValidationV1(False, tuple(sorted(reasons)), None)
    if reasons:
        return TradeWindowValidationV1(False, tuple(sorted(reasons)), evidence.content_hash)
    if evidence.request_ref != request.content_hash:
        reasons.add("TRADE_REQUEST_IDENTITY_MISMATCH")
    if repaired and (not contract.repair_supported or evidence.repair_evidence_ref is None):
        reasons.add("EXACT_TRADE_REPAIR_UNSUPPORTED")
    if not repaired and evidence.repair_evidence_ref is not None:
        reasons.add("TRADE_REPAIR_NOT_DECLARED")
    boundaries = (evidence.start_prefix, evidence.end_prefix)
    if tuple(row.before_at_ns for row in boundaries) != (request.start_at_ns, request.end_at_ns):
        reasons.add("TRADE_WINDOW_BOUNDARY_MISMATCH")
    expected_count = evidence.end_prefix.last_sequence - evidence.start_prefix.last_sequence
    if expected_count < 0 or expected_count != len(evidence.observations) or len(evidence.observations) > request.max_rows:
        reasons.add("TRADE_SEQUENCE_COVERAGE_INCOMPLETE")
    refs: set[str] = set()
    ids: set[str] = set()

    def accepted(ref: str, purpose: str, receipt: int | None = None,
                 available: int | None = None, body_hash: str | None = None) -> None:
        if available is not None and available > request.evidence_cutoff_ns:
            reasons.add("TRADE_EVIDENCE_AFTER_CUTOFF")
        elif accepted_evidence(PersistedTradeEvidenceCheckV1(
                ref, request.instrument, request.source_id, request.source_contract_ref,
                request.evidence_cutoff_ns,
                purpose, evidence.recovery_epoch if purpose in {"SOURCE_HEALTH", "NATIVE_REPAIR"} else None,
                receipt, available, body_hash)) is not True:
            reasons.add("TRADE_EVIDENCE_NOT_ACCEPTED")

    for boundary in boundaries:
        accepted(boundary.raw_evidence_ref, "NATIVE_PREFIX", boundary.received_at_ns, boundary.available_at_ns,
                 sha256_json(boundary.to_dict()))
    for offset, observation in enumerate(evidence.observations, start=1):
        trade = observation.trade
        if observation.sequence != evidence.start_prefix.last_sequence + offset:
            reasons.add("TRADE_SEQUENCE_COVERAGE_INCOMPLETE")
        if (trade.key != request.instrument or trade.source_id != request.source_id
                or trade.availability_class != AvailabilityClassV2.ACTUAL_SYSTEM
                or not request.start_at_ns <= trade.event_at_ns < request.end_at_ns):
            reasons.add("TRADE_NATIVE_IDENTITY_OR_WINDOW_INVALID")
        if trade.raw_observation_ref in refs or trade.trade_id in ids:
            reasons.add("TRADE_DUPLICATE_IDENTITY")
        refs.add(trade.raw_observation_ref)
        ids.add(trade.trade_id)
        accepted(trade.raw_observation_ref, "NATIVE_TRADE", trade.received_at_ns,
                 trade.available_at_ns, trade.content_hash)
    for purpose, ref in (("PRODUCT_METADATA", contract.metadata_ref),
                         ("SOURCE_QUALIFICATION", contract.qualification_ref),
                         ("SOURCE_HEALTH", evidence.source_health_ref),
                         ("NATIVE_REPAIR", evidence.repair_evidence_ref)):
        if ref is not None:
            accepted(ref, purpose,
                     available=contract.qualified_at_ns if purpose == "SOURCE_QUALIFICATION" else None,
                     body_hash=contract.definition_hash if purpose == "SOURCE_QUALIFICATION" else None)
    if reasons:
        return TradeWindowValidationV1(False, tuple(sorted(reasons)), evidence.content_hash)
    return TradeWindowValidationV1(True, (), evidence.content_hash, tuple(row.trade for row in evidence.observations))


def collect_exact_trade_window_v1(
    port: ExactTradeSourcePortV1 | None,
    request: TradeCompletenessRequestV1,
    *,
    accepted_evidence: AcceptedEvidenceCheckV1,
    repair: bool = False,
) -> TradeWindowValidationV1:
    """Use one declared source without fallback or automatic late repair.

    Invoke source I/O through the caller's bounded transport/worker. A repair
    request is explicit and remains bound to the original evidence cutoff;
    late receipts can support a later decision but cannot rescue this one.
    """
    if type(repair) is not bool:
        raise ValueError("repair selection must be explicit")
    if port is None:
        return TradeWindowValidationV1(False, ("EXACT_TRADE_SOURCE_UNAVAILABLE",), None)
    contract = port.contract
    if not isinstance(contract, TradeSourceContractV1):
        return TradeWindowValidationV1(False, ("EXACT_TRADE_SOURCE_INVALID_OUTPUT",), None)
    preflight = validate_trade_window_v1(contract, request, None, accepted_evidence=accepted_evidence)
    if preflight.reason_codes != ("TRADE_WINDOW_EVIDENCE_MISSING",):
        return preflight
    if repair and not contract.repair_supported:
        return TradeWindowValidationV1(False, ("EXACT_TRADE_REPAIR_UNSUPPORTED",), None)
    try:
        evidence = port.repair_window(request) if repair else port.read_window(request)
    except TimeoutError:
        return TradeWindowValidationV1(False, ("EXACT_TRADE_SOURCE_TIMEOUT",), None)
    except (ConnectionError, OSError):
        return TradeWindowValidationV1(False, ("EXACT_TRADE_SOURCE_UNAVAILABLE",), None)
    except (TypeError, ValueError):
        return TradeWindowValidationV1(False, ("EXACT_TRADE_SOURCE_INVALID_OUTPUT",), None)
    if evidence is not None and not isinstance(evidence, TradeWindowEvidenceV1):
        return TradeWindowValidationV1(False, ("EXACT_TRADE_SOURCE_INVALID_OUTPUT",), None)
    return validate_trade_window_v1(contract, request, evidence,
                                  accepted_evidence=accepted_evidence, repaired=repair)
