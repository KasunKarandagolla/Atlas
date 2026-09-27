"""Explicit, immutable support boundaries for V2 public evidence.

This matrix records documented or locally exercised semantics separately from
live qualification.  An endpoint name alone never grants an inference.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, ClassVar

from .._serialization import canonical_json, nonblank, sha256_json, timestamp
from ..instruments import InstrumentKeyV2

EVIDENCE_CAPABILITY_MATRIX_V2_VERSION = "EVIDENCE_CAPABILITY_MATRIX_V2_1"


class CapabilityStatusV2(StrEnum):
    UNVERIFIED = "UNVERIFIED"
    TEST_GATE = "TEST GATE"
    IMPLEMENTED = "IMPLEMENTED"
    NOT_ESTIMABLE = "NOT ESTIMABLE"


class FeedCoverageStateV2(StrEnum):
    UNVERIFIED = "UNVERIFIED"
    TEST_GATE = "TEST GATE"
    QUALIFIED = "QUALIFIED"
    DEGRADED = "DEGRADED"
    NOT_ESTIMABLE = "NOT ESTIMABLE"


@dataclass(frozen=True)
class FeedCoverageEvidenceV2:
    instrument: InstrumentKeyV2
    source_id: str
    channel: str
    covered_from_ns: int
    covered_through_ns: int
    available_at_ns: int
    expected_cadence_ns: int | None
    observed_count: int
    gap_count: int
    max_observed_gap_ns: int | None
    state: FeedCoverageStateV2
    source_health_ref: str
    capability_matrix_ref: str
    input_refs: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.instrument, InstrumentKeyV2):
            raise ValueError("feed coverage requires full InstrumentKeyV2")
        for name in ("source_id", "channel"):
            nonblank(getattr(self, name), field=name)
        for name in ("covered_from_ns", "covered_through_ns", "available_at_ns"):
            timestamp(getattr(self, name), field=name)
        if self.covered_through_ns < self.covered_from_ns or self.available_at_ns < self.covered_through_ns:
            raise ValueError("coverage interval or publication availability is invalid")
        if self.expected_cadence_ns is not None and self.expected_cadence_ns <= 0:
            raise ValueError("expected cadence must be positive or absent")
        if self.observed_count < 0 or self.gap_count < 0:
            raise ValueError("coverage counts cannot be negative")
        if self.max_observed_gap_ns is not None and self.max_observed_gap_ns < 0:
            raise ValueError("maximum observed gap cannot be negative")
        object.__setattr__(self, "state", FeedCoverageStateV2(self.state))
        for name in ("source_health_ref", "capability_matrix_ref"):
            value = getattr(self, name)
            if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
                raise ValueError(f"{name} must be a lowercase SHA-256 reference")
        for ref in self.input_refs:
            if len(ref) != 64 or any(char not in "0123456789abcdef" for char in ref):
                raise ValueError("coverage input refs must be lowercase SHA-256 references")
        if self.state == FeedCoverageStateV2.QUALIFIED and (
            self.expected_cadence_ns is None or self.observed_count == 0 or self.gap_count != 0
            or self.max_observed_gap_ns is None or not self.input_refs
        ):
            raise ValueError("qualified coverage requires cadence, observed frames, measured gap and exact input refs")

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": 1, "instrument": self.instrument.to_dict(),
                "source_id": self.source_id, "channel": self.channel,
                "covered_from_ns": self.covered_from_ns, "covered_through_ns": self.covered_through_ns,
                "available_at_ns": self.available_at_ns, "expected_cadence_ns": self.expected_cadence_ns,
                "observed_count": self.observed_count, "gap_count": self.gap_count,
                "max_observed_gap_ns": self.max_observed_gap_ns, "state": self.state.value,
                "source_health_ref": self.source_health_ref,
                "capability_matrix_ref": self.capability_matrix_ref, "input_refs": list(self.input_refs)}

    @property
    def content_hash(self) -> str:
        return sha256_json({"artifact_type": "FeedCoverageEvidenceV2", "coverage": self.to_dict()})


@dataclass(frozen=True)
class EvidenceCapabilityV2:
    venue: str
    environment: str
    instrument_product: str
    source_channel: str
    status: CapabilityStatusV2
    snapshot_delta_semantics: str | None
    sequence_update_semantics: str | None
    exchange_event_timestamp_semantics: str | None
    receipt_timestamp_semantics: str
    availability_semantics: str
    declared_book_depth: str | None
    cadence_resolution: str | None
    units: str | None
    aggressor_side_convention: str | None
    oi_quantity_value_convention: str | None
    funding_current_predicted_settled: str | None
    basis_inputs: str | None
    liquidation_side_product_convention: str | None
    coverage_censoring_limitations: str
    gap_reset_reconnect_behavior: str
    repair_capability: str
    source_health_requirement: str
    warmup_requirement: str
    permitted_uses: tuple[str, ...]
    explicitly_unsupported_uses: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    evidence_version: str = EVIDENCE_CAPABILITY_MATRIX_V2_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", CapabilityStatusV2(self.status))
        for name in (
            "venue", "environment", "instrument_product", "source_channel",
            "receipt_timestamp_semantics", "availability_semantics",
            "coverage_censoring_limitations", "gap_reset_reconnect_behavior",
            "repair_capability", "source_health_requirement", "warmup_requirement",
            "evidence_version",
        ):
            nonblank(getattr(self, name), field=name)
        for name in (
            "snapshot_delta_semantics", "sequence_update_semantics",
            "exchange_event_timestamp_semantics", "declared_book_depth",
            "cadence_resolution", "units", "aggressor_side_convention",
            "oi_quantity_value_convention", "funding_current_predicted_settled",
            "basis_inputs", "liquidation_side_product_convention",
        ):
            value = getattr(self, name)
            if value is not None:
                nonblank(value, field=name)
        for name in ("permitted_uses", "explicitly_unsupported_uses", "evidence_refs"):
            value = tuple(nonblank(item, field=name) for item in getattr(self, name))
            object.__setattr__(self, name, value)
        if not self.evidence_refs:
            raise ValueError("each capability row requires evidence refs or an explicit local absence ref")

    def to_dict(self) -> dict[str, Any]:
        return {
            "venue": self.venue,
            "environment": self.environment,
            "instrument_product": self.instrument_product,
            "source_channel": self.source_channel,
            "status": self.status.value,
            "snapshot_delta_semantics": self.snapshot_delta_semantics,
            "sequence_update_semantics": self.sequence_update_semantics,
            "exchange_event_timestamp_semantics": self.exchange_event_timestamp_semantics,
            "receipt_timestamp_semantics": self.receipt_timestamp_semantics,
            "availability_semantics": self.availability_semantics,
            "declared_book_depth": self.declared_book_depth,
            "cadence_resolution": self.cadence_resolution,
            "units": self.units,
            "aggressor_side_convention": self.aggressor_side_convention,
            "oi_quantity_value_convention": self.oi_quantity_value_convention,
            "funding_current_predicted_settled": self.funding_current_predicted_settled,
            "basis_inputs": self.basis_inputs,
            "liquidation_side_product_convention": self.liquidation_side_product_convention,
            "coverage_censoring_limitations": self.coverage_censoring_limitations,
            "gap_reset_reconnect_behavior": self.gap_reset_reconnect_behavior,
            "repair_capability": self.repair_capability,
            "source_health_requirement": self.source_health_requirement,
            "warmup_requirement": self.warmup_requirement,
            "permitted_uses": list(self.permitted_uses),
            "explicitly_unsupported_uses": list(self.explicitly_unsupported_uses),
            "evidence_refs": list(self.evidence_refs),
            "evidence_version": self.evidence_version,
        }


@dataclass(frozen=True)
class EvidenceCapabilityMatrixV2:
    rows: tuple[EvidenceCapabilityV2, ...]
    version: str = EVIDENCE_CAPABILITY_MATRIX_V2_VERSION

    SCHEMA_VERSION: ClassVar[int] = 2

    def __post_init__(self) -> None:
        object.__setattr__(self, "rows", tuple(self.rows))
        if not self.rows or any(not isinstance(row, EvidenceCapabilityV2) for row in self.rows):
            raise ValueError("capability matrix requires typed non-empty rows")
        keys = [(r.venue, r.environment, r.instrument_product, r.source_channel) for r in self.rows]
        if len(keys) != len(set(keys)):
            raise ValueError("capability matrix row identities must be unique")
        nonblank(self.version, field="version")

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": self.SCHEMA_VERSION, "version": self.version,
                "rows": [row.to_dict() for row in self.rows]}

    def to_canonical_json(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def content_hash(self) -> str:
        return sha256_json({"artifact_type": "EvidenceCapabilityMatrixV2", "matrix": self.to_dict()})

    def lookup(self, venue: str, environment: str, product: str, source_channel: str) -> EvidenceCapabilityV2 | None:
        for row in self.rows:
            if (row.venue, row.environment, row.instrument_product, row.source_channel) == (
                venue, environment, product, source_channel
            ):
                return row
        return None


_DOC_BYBIT = "https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook"
_DOC_BYBIT_TRADES = "https://bybit-exchange.github.io/docs/v5/websocket/public/trade"
_DOC_BYBIT_LIQ = "https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation"
_DOC_BINANCE = "https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams"
_DOC_BYBIT_TICKER = "https://bybit-exchange.github.io/docs/v5/market/tickers"
_DOC_BYBIT_FUNDING = "https://bybit-exchange.github.io/docs/v5/market/history-fund-rate"
_DOC_BYBIT_OI = "https://bybit-exchange.github.io/docs/v5/market/open-interest"
_DOC_BINANCE_MARK = "https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Mark-Price"
_DOC_BINANCE_FUNDING = "https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-History"
_DOC_BINANCE_OI = "https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest"
_LOCAL = "repo:docs/v2/EVIDENCE_CAPABILITY_MATRIX_V1.json"


def default_evidence_capability_matrix_v2() -> EvidenceCapabilityMatrixV2:
    """Return conservative public feed declarations; none imply live qualification."""
    shared_unsupported = (
        "historical L2 reconstruction from candles", "position ownership", "trader leverage",
        "exact liquidation levels/maps", "complete forced-liquidation population", "institutional intent",
    )
    rows = (
        EvidenceCapabilityV2(
            "BYBIT", "MAINNET", "LINEAR_PERPETUAL", "public/orderbook.50 WS", CapabilityStatusV2.UNVERIFIED,
            "snapshot followed by level deltas; size zero removes a level", "u update id; seq is cross-sequence; update-ID continuity guarantee not published; implementation fails closed on nonconsecutive u; u=1 reset",
            "cts matching-engine timestamp when supplied, else ts exchange message timestamp; provenance not live-qualified", "local monotonic/UTC receipt captured on frame arrival",
            "decision availability is max(actual receipt, parse completion); actual receipt retained", "50 levels per side",
            "documented push 20ms for depth 50; observed cadence still unqualified", "price and contract quantity strings",
            None, None, None, None, None,
            "public book feed coverage can be interrupted; no claim of full market liquidity", "gap, reset u=1, disconnect or stale frame invalidates; reconnect requires new snapshot reconciliation",
            "fresh REST snapshot plus buffered-delta bridge is required; implementation local only", "HEALTHY_CURRENT plus current receipt age and no sequence conflict",
            "30s continuous valid post-recovery book/trade coverage (engineering default)",
            ("sequence-valid displayed depth context after explicit recovery",), shared_unsupported,
            (_DOC_BYBIT, _LOCAL),
        ),
        EvidenceCapabilityV2(
            "BYBIT", "MAINNET", "LINEAR_PERPETUAL", "public/trade WS", CapabilityStatusV2.UNVERIFIED,
            None, "trade id i; cross sequence may be shared by grouped records", "T matched time; frame ts is message time",
            "local receipt captured when frame arrives", "decision availability is receipt/parse completion, never matched-time backdating",
            None, "up to 1024 trades/message; live cadence unqualified", "price p; size v in contract-native quantity",
            "S is taker side (Buy/Sell)", None, None, None, None,
            "stream continuity/coverage and trades omitted during disconnect are unknown", "duplicate trade id idempotent only for identical content; reconnect gap is coverage loss",
            "no historical repair declared", "HEALTHY_CURRENT and bounded receipt age", "trade windows require explicit timestamp and coverage support",
            ("signed aggressive trade flow when side convention and coverage are qualified",), shared_unsupported,
            (_DOC_BYBIT_TRADES, _LOCAL),
        ),
        EvidenceCapabilityV2(
            "BYBIT", "MAINNET", "LINEAR_PERPETUAL", "public/allLiquidation WS", CapabilityStatusV2.UNVERIFIED,
            None, "record id and message sequence are not a completeness cursor", "T execution time; frame ts message time",
            "local frame receipt", "available only at actual receipt/parse completion", None,
            "documented 500ms pushes; event selection/censoring unresolved", "price p and executed contract size v",
            None, None, None, None, "S denotes liquidated position side (Buy = long position liquidated; not aggressor side)",
            "stream is censored/event-filtered; population completeness and cross-venue coverage unknown",
            "disconnect loses events; reconnect cannot repair omitted observations", "no repair capability qualified",
            "separate liquidation source health and coverage state required", "venue-specific baseline requires qualified historical coverage",
            ("observed event intensity with explicit censored/unknown coverage",), shared_unsupported,
            (_DOC_BYBIT_LIQ, _LOCAL),
        ),
        EvidenceCapabilityV2(
            "BINANCE", "MAINNET", "LINEAR_PERPETUAL", "USD-M depth snapshot REST", CapabilityStatusV2.TEST_GATE,
            "REST snapshot at lastUpdateId", "lastUpdateId only; snapshot alone has no continuous update chain",
            "no exchange event timestamp in depth snapshot", "local HTTP response receipt", "available at response receipt/parse completion",
            "request limit/depth field varies; must record returned levels", "on demand, not a stream", "price and base-asset quantity",
            None, None, None, None, None, "snapshot is point-in-time; does not certify continuous liquidity or hidden depth",
            "a later gap/reconnect invalidates until a new snapshot bridge", "fresh snapshot bridge possible only with diff stream; live transport currently absent",
            "healthy request plus independent stream state", "30s continuous valid post-recovery book (engineering default)",
            ("snapshot initialization input for a sequence-reconciled book",), shared_unsupported,
            (_LOCAL,),
        ),
        EvidenceCapabilityV2(
            "BINANCE", "MAINNET", "LINEAR_PERPETUAL", "USD-M depth diff WS", CapabilityStatusV2.UNVERIFIED,
            "diff event fields U/u; bridge against REST lastUpdateId", "U first update id, u final update id, pu prior final update id; verify bridge then enforce pu continuity",
            "E event time and T transaction time when supplied", "local frame receipt captured on arrival",
            "available at max(receipt, parse completion); event time never substitutes receipt", "diff updates have level changes only",
            "documented 100/250/500ms stream options; live cadence unqualified", "price and base-asset quantity",
            None, None, None, None, None, "public depth stream is bounded/censored to exchange-published levels",
            "gap, out-of-order, reconnect, stale feed or failed pu invalidates immediately", "buffer deltas, fetch snapshot, bridge U<=lastUpdateId+1<=u; new snapshot required after gap",
            "HEALTHY_CURRENT, sequence chain, current receipt age", "30s valid post-recovery (engineering default)",
            ("sequence-valid displayed depth context after full snapshot/delta reconciliation",), shared_unsupported,
            (_DOC_BINANCE, _LOCAL),
        ),
        EvidenceCapabilityV2(
            "BINANCE", "MAINNET", "LINEAR_PERPETUAL", "USD-M aggTrade WS", CapabilityStatusV2.UNVERIFIED,
            None, "a/f aggregate trade id range", "T trade time and E event time", "local frame receipt",
            "available at receipt/parse completion", None, "documented event stream, observed cadence unqualified",
            "p price; q base-asset quantity", "m=true means buyer is maker, so seller is taker; otherwise buyer aggressor",
            None, None, None, None, "aggregate trades are not every individual matching-engine print; disconnect creates gaps",
            "trade id overlap may de-duplicate; reconnect coverage gap remains", "no historical repair declared",
            "HEALTHY_CURRENT plus explicit id continuity/coverage policy", "window only with supported actual cadence/coverage",
            ("signed aggressive aggregate trade flow with explicitly labeled aggregation",), shared_unsupported,
            (_LOCAL,),
        ),
        EvidenceCapabilityV2(
            "BYBIT", "MAINNET", "LINEAR_PERPETUAL", "V5 REST ticker/funding-history/open-interest", CapabilityStatusV2.TEST_GATE,
            snapshot_delta_semantics="REST points/history records; revision semantics source-specific",
            sequence_update_semantics="no continuous sequence semantics",
            exchange_event_timestamp_semantics="source event/period timestamps may not equal publication time",
            receipt_timestamp_semantics="actual ATLAS response receipt only; imports retain separate historical import receipt",
            availability_semantics="history is reconstructed availability unless original publication receipt is evidenced",
            declared_book_depth=None, cadence_resolution="REST endpoint intervals; do not imply tick cadence",
            units="native source raw units retained", aggressor_side_convention=None,
            oi_quantity_value_convention="raw openInterest and openInterestValue fields; contract/quote conversion not live-qualified",
            funding_current_predicted_settled="current/settled/predicted distinct only when source field is qualified; otherwise UNQUALIFIED",
            basis_inputs="basis only from explicit same-cutoff mark and index inputs", liquidation_side_product_convention=None,
            coverage_censoring_limitations="historical revisions append; coverage/publication receipt may be unavailable; no earlier visibility inference",
            gap_reset_reconnect_behavior="REST refresh does not repair missing original receipt history",
            repair_capability="no continuous sequence repair; refresh creates a new observation",
            source_health_requirement="source healthy/current for live values; reconstructed history separately labeled",
            warmup_requirement="15M OI change only from cutoff-known observations at both ends",
            permitted_uses=("crowding context and observed economic-cost inputs with named missingness",),
            explicitly_unsupported_uses=shared_unsupported, evidence_refs=(_DOC_BYBIT_TICKER, _DOC_BYBIT_FUNDING, _DOC_BYBIT_OI, _LOCAL),
        ),
        EvidenceCapabilityV2(
            "BINANCE", "MAINNET", "LINEAR_PERPETUAL", "USD-M REST mark/funding/open-interest", CapabilityStatusV2.TEST_GATE,
            snapshot_delta_semantics="REST current points and historical interval samples",
            sequence_update_semantics="no continuous sequence semantics",
            exchange_event_timestamp_semantics="endpoint-specific event/funding-period timestamps; publication receipt may be absent",
            receipt_timestamp_semantics="actual ATLAS HTTP response receipt; imported history keeps import receipt separate",
            availability_semantics="historical download/import is RECONSTRUCTED_MARKET unless original receipt is archived",
            declared_book_depth=None, cadence_resolution="REST request intervals; OI history has source-declared intervals",
            units="raw JSON numeric strings retained", aggressor_side_convention=None,
            oi_quantity_value_convention="raw openInterest and sumOpenInterestValue/sumOpenInterest fields remain separately unit-tagged; no inferred conversion",
            funding_current_predicted_settled="premiumIndex fundingRate semantics not upgraded to predicted/settled without qualified source evidence; history is SETTLED only when source contract proves it",
            basis_inputs="mark/index basis only from same-cutoff premiumIndex inputs",
            liquidation_side_product_convention=None,
            coverage_censoring_limitations="REST points omit intervals between requests; historical revisions do not rewrite actual receipt chronology",
            gap_reset_reconnect_behavior="refresh provides a new point; no sequence continuity or missing-point repair",
            repair_capability="no continuous sequence repair; archived originals remain append-only",
            source_health_requirement="healthy current HTTP source for live point values; reconstructed history labeled separately",
            warmup_requirement="15M OI change requires two same-source, same-unit cutoff-known intervals",
            permitted_uses=("crowding context and observed economic-cost inputs with named missingness",),
            explicitly_unsupported_uses=shared_unsupported,
            evidence_refs=(_DOC_BINANCE_MARK, _DOC_BINANCE_FUNDING, _DOC_BINANCE_OI, _LOCAL),
        ),
        EvidenceCapabilityV2(
            "BINANCE", "MAINNET", "LINEAR_PERPETUAL", "USD-M forceOrder liquidation WS", CapabilityStatusV2.NOT_ESTIMABLE,
            snapshot_delta_semantics=None, sequence_update_semantics=None,
            exchange_event_timestamp_semantics="event time field semantics not live-qualified",
            receipt_timestamp_semantics="actual local frame receipt only",
            availability_semantics="actual receipt distinct from event time; imports remain reconstructed",
            declared_book_depth=None, cadence_resolution="forceOrder stream transport not implemented/live-qualified",
            units="raw venue fields only", aggressor_side_convention=None,
            oi_quantity_value_convention=None, funding_current_predicted_settled=None, basis_inputs=None,
            liquidation_side_product_convention="forceOrder schema/product/side and selection semantics not qualified",
            coverage_censoring_limitations="no feed completeness or population coverage claim",
            gap_reset_reconnect_behavior="not implemented; any disconnect is unknown coverage",
            repair_capability="no repair declared",
            source_health_requirement="dedicated feed health and explicit coverage qualification required",
            warmup_requirement="venue baseline requires sufficient earlier qualified fixed-duration windows",
            permitted_uses=("none until schema and coverage are qualified",),
            explicitly_unsupported_uses=shared_unsupported, evidence_refs=(_LOCAL,),
        ),
    )
    return EvidenceCapabilityMatrixV2(rows)


def capability_for_public_channel_v2(
    matrix: EvidenceCapabilityMatrixV2, instrument: InstrumentKeyV2, channel: str,
) -> EvidenceCapabilityV2 | None:
    """Resolve a concrete feed channel to a declared matrix row.

    Runtime symbols are retained in evidence, while this maps only the known
    public channel forms documented by the matrix. Unknown channel spellings
    never inherit a nearby capability.
    """
    symbol = instrument.native_symbol
    row_channel: str | None = None
    if instrument.venue.value == "BYBIT":
        if channel == f"orderbook.50.{symbol}":
            row_channel = "public/orderbook.50 WS"
        elif channel == f"publicTrade.{symbol}":
            row_channel = "public/trade WS"
        elif channel == f"allLiquidation.{symbol}":
            row_channel = "public/allLiquidation WS"
        elif channel == "V5 REST ticker/funding-history/open-interest":
            row_channel = channel
    elif instrument.venue.value == "BINANCE":
        stream = channel.lower()
        symbol_lower = symbol.lower()
        if stream == f"{symbol_lower}@aggtrade":
            row_channel = "USD-M aggTrade WS"
        elif stream in {f"{symbol_lower}@depth@100ms", f"{symbol_lower}@depth@250ms",
                        f"{symbol_lower}@depth@500ms"}:
            row_channel = "USD-M depth diff WS"
        elif channel in {"USD-M depth snapshot REST", "USD-M REST mark/funding/open-interest"}:
            row_channel = channel
    if row_channel is None:
        return None
    return matrix.lookup(instrument.venue.value, instrument.environment.value,
                         instrument.product.value, row_channel)
