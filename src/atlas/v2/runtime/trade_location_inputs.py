"""Bounded actual-system public-trade inputs for causal location diagnostics.

The public observation archive proves the identity and availability of rows it
contains. It does not prove that the selected interval is complete. Therefore
this adapter preserves observed native trades for audit but deliberately keeps
them out of estimators until a separately qualified coverage capability exists.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from atlas.v2._serialization import sha256_json, sha256_ref, timestamp
from atlas.v2.data.binance import translate_agg_trades
from atlas.v2.data.bybit import translate_recent_trades
from atlas.v2.data.history import reconstruct_public_observations_from_archive
from atlas.v2.data.raw import AvailabilityClassV2
from atlas.v2.data.trade_completeness import (
    EXACT_SEQUENCE_SEMANTICS_V1,
    MAX_TRADE_WINDOW_ROWS_V1,
    ExactTradeSourcePortV1,
    PersistedTradeEvidenceCheckV1,
    TradeCompletenessRequestV1,
    collect_exact_trade_window_v1,
)
from atlas.v2.features.candles import (
    DAY_NS,
    MAX_LOCATION_WINDOW_NS,
    CausalTrade,
    TradeLocationConfig,
)
from atlas.v2.instruments import InstrumentKeyV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.strategies.s3_mean_reversion import CausalTradeV2

MAX_ARCHIVED_TRADE_ROWS = 8193  # One over the estimator bound detects truncation.
MAX_ESTIMABLE_TRADE_ROWS = 8192
TRADE_LOCATION_INPUT_ARTIFACT_TYPE = "TradeLocationInputReceiptV1"


@dataclass(frozen=True)
class TradeLocationInputsV1:
    """Exact bounded source rows plus explicit non-qualified coverage state."""

    key: InstrumentKeyV2
    cutoff_ns: int
    configuration: TradeLocationConfig | None
    observed_trades: tuple[CausalTrade, ...]
    source_refs: tuple[str, ...]
    rejected_row_count: int
    completeness_evidence_ref: str | None = None
    proof_window_start_ns: int | None = None
    proof_window_end_exclusive_ns: int | None = None
    proof_request_ref: str | None = None
    proof_source_contract_ref: str | None = None
    proof_source_id: str | None = None
    coverage_state: str = "UNVERIFIED"
    reason: str = "TRADE_COVERAGE_UNVERIFIED"
    capital_authority: str = "ZERO"

    def __post_init__(self) -> None:
        if not isinstance(self.key, InstrumentKeyV2):
            raise ValueError("trade-location inputs require full instrument identity")
        timestamp(self.cutoff_ns, field="cutoff_ns")
        if self.configuration is not None and not isinstance(self.configuration, TradeLocationConfig):
            raise ValueError("trade-location configuration must be explicit and typed")
        if any(trade.key != self.key or trade.available_at_ns > self.cutoff_ns
               or trade.event_at_ns > self.cutoff_ns for trade in self.observed_trades):
            raise ValueError("trade-location rows exceed exact identity or information cutoff")
        if tuple(sorted(set(self.source_refs))) != self.source_refs:
            raise ValueError("trade-location source refs must be unique and sorted")
        if self.completeness_evidence_ref is not None:
            from atlas.v2._serialization import sha256_ref
            sha256_ref(self.completeness_evidence_ref, field="completeness_evidence_ref")
        proof_fields = (self.proof_window_start_ns, self.proof_window_end_exclusive_ns,
                        self.proof_request_ref, self.proof_source_contract_ref, self.proof_source_id)
        if any(value is not None for value in proof_fields):
            if (self.proof_window_start_ns is None or self.proof_window_end_exclusive_ns is None
                    or self.proof_request_ref is None or self.proof_source_contract_ref is None
                    or self.proof_source_id is None):
                raise ValueError("trade proof window identity must be complete")
            timestamp(self.proof_window_start_ns, field="proof_window_start_ns")
            timestamp(self.proof_window_end_exclusive_ns, field="proof_window_end_exclusive_ns")
            if self.proof_window_start_ns >= self.proof_window_end_exclusive_ns:
                raise ValueError("trade proof interval must be non-empty and half-open")
            from atlas.v2._serialization import nonblank, sha256_ref
            nonblank(self.proof_source_id, field="proof_source_id")
            sha256_ref(self.proof_request_ref, field="proof_request_ref")
            sha256_ref(self.proof_source_contract_ref, field="proof_source_contract_ref")
        if self.coverage_state not in {"UNVERIFIED", "VERIFIED"} or self.capital_authority != "ZERO":
            raise ValueError("unsupported trade-location coverage or authority")
        if (self.coverage_state == "VERIFIED") != (self.completeness_evidence_ref is not None):
            raise ValueError("verified trade inputs require exact completeness evidence")
        if (self.coverage_state == "VERIFIED") != (self.proof_request_ref is not None):
            raise ValueError("verified trade inputs require exact half-open request identity")

    @property
    def estimator_trades(self) -> tuple[CausalTrade, ...]:
        """Only coverage-qualified rows may reach location estimators.

        Archived rows without a separate exact completeness proof remain
        unavailable to estimators.
        """
        return self.observed_trades if self.coverage_state == "VERIFIED" else ()

    def receipt_body(self, *, created_at_ns: int, available_at_ns: int) -> dict[str, Any]:
        timestamp(created_at_ns, field="created_at_ns")
        timestamp(available_at_ns, field="available_at_ns")
        if not self.cutoff_ns <= created_at_ns <= available_at_ns:
            raise ValueError("trade-location receipt chronology must follow its information cutoff")
        return {
            "schema_version": 1,
            "key": self.key.to_dict(),
            "information_cutoff_ns": self.cutoff_ns,
            "created_at_ns": created_at_ns,
            "available_at_ns": available_at_ns,
            "configuration": self.configuration.to_dict() if self.configuration is not None else None,
            "configuration_ref": self.configuration.content_hash if self.configuration is not None else None,
            "source_refs": list(self.source_refs),
            "observed_trade_refs": sorted({trade.ref for trade in self.observed_trades}),
            "estimator_trade_refs": [trade.ref for trade in self.estimator_trades],
            "completeness_evidence_ref": self.completeness_evidence_ref,
            "completeness_window": ({
                "start_at_ns": self.proof_window_start_ns,
                "end_exclusive_at_ns": self.proof_window_end_exclusive_ns,
                "request_ref": self.proof_request_ref,
                "source_contract_ref": self.proof_source_contract_ref,
                "source_id": self.proof_source_id,
                "sequence_semantics": EXACT_SEQUENCE_SEMANTICS_V1,
            } if self.proof_request_ref is not None else None),
            "coverage_state": self.coverage_state,
            "reason": self.reason,
            "rejected_row_count": self.rejected_row_count,
            "capital_authority": "ZERO",
        }


@dataclass(frozen=True)
class TradeLocationInputReceiptV1:
    artifact_ref: str
    body: Mapping[str, Any]

    def __post_init__(self) -> None:
        from atlas.v2._serialization import sha256_ref
        sha256_ref(self.artifact_ref, field="artifact_ref")
        if sha256_json(dict(self.body)) != self.artifact_ref:
            raise ValueError("trade-location input receipt hash mismatch")

    @property
    def content_hash(self) -> str:
        return self.artifact_ref

    def to_dict(self) -> dict[str, Any]:
        return dict(self.body)

    @classmethod
    def from_dict(cls, body: Mapping[str, Any], *, artifact_ref: str | None = None
                  ) -> TradeLocationInputReceiptV1:
        fields = {
            "schema_version", "key", "information_cutoff_ns", "created_at_ns", "available_at_ns",
            "configuration", "configuration_ref", "source_refs", "observed_trade_refs", "estimator_trade_refs",
            "completeness_evidence_ref", "completeness_window", "coverage_state", "reason",
            "rejected_row_count", "capital_authority",
        }
        if set(body) != fields or type(body.get("schema_version")) is not int or body["schema_version"] != 1:
            raise ValueError("trade-location receipt schema is invalid")
        key_body = body.get("key")
        if not isinstance(key_body, Mapping):
            raise ValueError("trade-location receipt instrument identity is missing")
        InstrumentKeyV2.from_dict(key_body)
        cutoff = timestamp(cast(int, body.get("information_cutoff_ns")), field="information_cutoff_ns")
        created = timestamp(cast(int, body.get("created_at_ns")), field="created_at_ns")
        available = timestamp(cast(int, body.get("available_at_ns")), field="available_at_ns")
        if not cutoff <= created <= available:
            raise ValueError("trade-location receipt chronology is invalid")
        configuration = body.get("configuration")
        configuration_ref = body.get("configuration_ref")
        if configuration is None:
            if configuration_ref is not None:
                raise ValueError("unconfigured trade-location receipt has a config ref")
        elif (not isinstance(configuration, Mapping) or not isinstance(configuration_ref, str)
              or sha256_json(dict(configuration)) != configuration_ref):
            raise ValueError("trade-location receipt configuration hash is invalid")
        for name in ("source_refs", "observed_trade_refs", "estimator_trade_refs"):
            refs = body.get(name)
            if (not isinstance(refs, list) or tuple(sorted(set(refs))) != tuple(refs)
                    or any(not isinstance(reference, str) for reference in refs)):
                raise ValueError(f"trade-location receipt {name} are invalid")
            for reference in refs:
                sha256_ref(reference, field=name)
        if not set(body["observed_trade_refs"]).issubset(set(body["source_refs"])):
            raise ValueError("observed trade refs must be sealed receipt dependencies")
        if not set(body["estimator_trade_refs"]).issubset(set(body["observed_trade_refs"])):
            raise ValueError("estimator trade refs must be observed refs")
        proof_ref = body.get("completeness_evidence_ref")
        coverage = body.get("coverage_state")
        window = body.get("completeness_window")
        if coverage == "VERIFIED":
            if proof_ref is None or not isinstance(window, Mapping):
                raise ValueError("verified trade-location receipt lacks interval proof")
            sha256_ref(proof_ref, field="completeness_evidence_ref")
            window_fields = {"start_at_ns", "end_exclusive_at_ns", "request_ref", "source_contract_ref",
                             "source_id", "sequence_semantics"}
            if set(window) != window_fields:
                raise ValueError("trade-location receipt completeness interval is malformed")
            timestamp(window["start_at_ns"], field="window.start_at_ns")
            timestamp(window["end_exclusive_at_ns"], field="window.end_exclusive_at_ns")
            if not window["start_at_ns"] < window["end_exclusive_at_ns"] <= cutoff:
                raise ValueError("trade-location receipt completeness interval exceeds cutoff")
            sha256_ref(window["request_ref"], field="window.request_ref")
            sha256_ref(window["source_contract_ref"], field="window.source_contract_ref")
            if (not isinstance(window["source_id"], str) or not window["source_id"].strip()
                    or window["sequence_semantics"] != EXACT_SEQUENCE_SEMANTICS_V1):
                raise ValueError("trade-location receipt source qualification is invalid")
            if not {proof_ref, window["source_contract_ref"]}.issubset(set(body["source_refs"])):
                raise ValueError("verified trade-location proof refs are not sealed dependencies")
        elif coverage == "UNVERIFIED":
            if proof_ref is not None or window is not None:
                raise ValueError("unverified trade-location receipt carries a proof claim")
        else:
            raise ValueError("trade-location receipt coverage state is invalid")
        if (type(body.get("rejected_row_count")) is not int or body["rejected_row_count"] < 0
                or body.get("capital_authority") != "ZERO"):
            raise ValueError("trade-location receipt authority or row count is invalid")
        expected_reason = ("TRADE_COVERAGE_VERIFIED" if coverage == "VERIFIED"
                           else "TRADE_COVERAGE_UNVERIFIED")
        if body.get("reason") != expected_reason:
            raise ValueError("trade-location receipt reason does not match its coverage state")
        reference = artifact_ref if artifact_ref is not None else sha256_json(dict(body))
        return cls(reference, dict(body))


def persist_trade_location_input_receipt(
    repository: OpsRepository,
    inputs: TradeLocationInputsV1,
    *,
    created_at_ns: int,
    available_at_ns: int,
) -> TradeLocationInputReceiptV1:
    """Persist the exact diagnostic/configuration outcome before feature use."""
    if not isinstance(inputs, TradeLocationInputsV1):
        raise ValueError("typed trade-location inputs are required")
    body = inputs.receipt_body(created_at_ns=created_at_ns, available_at_ns=available_at_ns)
    reference = sha256_json(body)
    receipt = TradeLocationInputReceiptV1(reference, body)
    input_refs = set(inputs.source_refs)
    if inputs.configuration is not None:
        if inputs.configuration.anchor is not None:
            input_refs.add(inputs.configuration.anchor.ref)
        if inputs.configuration.product is not None:
            input_refs.add(inputs.configuration.product.content_hash)
    ordered_input_refs = tuple(sorted(input_refs))
    repository.register_artifact(ArtifactIndexEntryV2(
        reference, TRADE_LOCATION_INPUT_ARTIFACT_TYPE, reference,
        created_at_ns, available_at_ns, {"receipt": body, "input_refs": list(ordered_input_refs)},
    ))
    if isinstance(repository, OpsRepository):
        from atlas.v2.chronology import record_computation
        record_computation(repository, artifact_ref=reference,
            information_cutoff_ns=inputs.cutoff_ns, started_ns=created_at_ns,
            finished_ns=available_at_ns, available_ns=available_at_ns,
            input_refs=ordered_input_refs, deadline_ns=available_at_ns)
    return receipt


def _native_trade(item: Any, key: InstrumentKeyV2, cutoff_ns: int) -> CausalTrade | None:
    observation = item.observation
    if (observation.availability_class != AvailabilityClassV2.ACTUAL_SYSTEM
            or observation.event_at_ns is None or observation.event_at_ns > cutoff_ns
            or observation.received_at_ns > cutoff_ns or observation.available_at_ns > cutoff_ns
            or observation.published_at_ns is not None and observation.published_at_ns > cutoff_ns):
        return None
    try:
        payload = json.loads(item.raw_payload_bytes)
        if not isinstance(payload, Mapping):
            return None
        if key.venue.value == "BYBIT" and observation.event_type == "TRADE":
            translated, = translate_recent_trades((payload,), key=key,
                received_at_ns=observation.received_at_ns, source_id=observation.source_id)
            identity = payload.get("execId", payload.get("i"))
            price, quantity = Decimal(str(payload["p"])), Decimal(str(payload["v"]))
        elif key.venue.value == "BINANCE" and observation.event_type == "AGG_TRADE":
            translated, = translate_agg_trades((payload,), key=key,
                received_at_ns=observation.received_at_ns, source_id=observation.source_id)
            identity = payload.get("a")
            price, quantity = Decimal(str(payload["p"])), Decimal(str(payload["q"]))
        else:
            return None
        trade_id = str(identity)
        if (not trade_id or trade_id == "None" or translated.record_id != observation.record_id
                or translated.raw_payload_hash != observation.raw_payload_hash
                or translated.event_at_ns != observation.event_at_ns
                or translated.sequence != observation.sequence):
            return None
        return CausalTrade(key, price, quantity, observation.event_at_ns,
                           observation.available_at_ns, item.observation_index_ref, trade_id)
    except (ArithmeticError, AttributeError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None


def load_trade_location_inputs(
    repository: OpsRepository,
    archive_root: str | Path,
    *,
    key: InstrumentKeyV2,
    cutoff_ns: int,
    configuration: TradeLocationConfig | None,
    exact_trade_source: ExactTradeSourcePortV1 | None = None,
    accepted_evidence: Callable[[PersistedTradeEvidenceCheckV1], bool] | None = None,
) -> TradeLocationInputsV1:
    """Load exact native trades, without claiming complete interval coverage.

    Anchors, profile windows, and product tick metadata are supplied explicitly
    in ``configuration``. No anchor is inferred from candles or trade extrema.
    The archive read is actual-system only, bounded, exact-key and exact-revision.
    """
    if not isinstance(key, InstrumentKeyV2):
        raise ValueError("trade-location archive load requires full InstrumentKeyV2")
    timestamp(cutoff_ns, field="cutoff_ns")
    if configuration is not None and not isinstance(configuration, TradeLocationConfig):
        raise ValueError("trade-location configuration must be explicit and typed")
    if configuration is not None:
        anchor = configuration.anchor
        product = configuration.product
        if anchor is not None and (anchor.key != key or anchor.available_at_ns > cutoff_ns):
            raise ValueError("trade-location anchor must match the exact key and cutoff")
        if product is not None and (product.key != key or product.available_at_ns > cutoff_ns):
            raise ValueError("trade-location product metadata must match key and cutoff")
        if configuration.profile_window_start_ns is not None and configuration.profile_window_start_ns > cutoff_ns:
            raise ValueError("trade-location profile window starts after the cutoff")

    observations = reconstruct_public_observations_from_archive(
        repository, archive_root, instrument_revision=key.contract_revision,
        information_cutoff_ns=cutoff_ns, event_types=("TRADE", "AGG_TRADE"),
        limit=MAX_ARCHIVED_TRADE_ROWS, key=key,
        availability_class=AvailabilityClassV2.ACTUAL_SYSTEM,
    )
    # Filter by explicit configured windows for bounded in-memory retention.
    starts = []
    if configuration is not None:
        if configuration.anchor is not None:
            starts.append(configuration.anchor.event_at_ns)
        if configuration.profile_window_start_ns is not None:
            starts.append(configuration.profile_window_start_ns)
    window_start = min(starts) if starts else None
    translated: list[CausalTrade] = []
    rejected = 0
    for item in observations:
        if window_start is not None and (item.observation.event_at_ns is None
                                         or item.observation.event_at_ns < window_start):
            continue
        trade = _native_trade(item, key, cutoff_ns)
        if trade is None:
            rejected += 1
            continue
        translated.append(trade)
    translated.sort(key=lambda row: (row.event_at_ns, row.ref))
    # The extra sentinel row makes archive truncation visible. The output remains
    # bounded even when the requested window contains more rows than downstream.
    overflow = len(translated) > MAX_ESTIMABLE_TRADE_ROWS
    if overflow:
        translated = translated[-MAX_ESTIMABLE_TRADE_ROWS:]
    refs = tuple(sorted({trade.ref for trade in translated}))
    rejected += int(overflow)

    # Observed archive trades alone never qualify completeness. A separately
    # audited source contract must supply native prefix boundaries and accepted
    # raw evidence for this exact bounded window.
    proof_ref = None
    proof_window_start_ns = None
    proof_window_end_exclusive_ns = None
    proof_request_ref = None
    proof_source_contract_ref = None
    proof_source_id = None
    coverage_state = "UNVERIFIED"
    if (configuration is not None and exact_trade_source is not None
            and accepted_evidence is not None):
        starts = []
        if configuration.anchor is not None:
            starts.append(configuration.anchor.event_at_ns)
        if configuration.profile_window_start_ns is not None:
            starts.append(configuration.profile_window_start_ns)
        if starts:
            start_ns = min(starts)
            contract = exact_trade_source.contract
            utc_day_start_ns = cutoff_ns // DAY_NS * DAY_NS
            if (0 < cutoff_ns - start_ns <= MAX_LOCATION_WINDOW_NS
                    and start_ns <= utc_day_start_ns
                    and contract.instrument == key and contract.sequence_semantics == EXACT_SEQUENCE_SEMANTICS_V1
                    and contract.qualified_at_ns <= cutoff_ns):
                request = TradeCompletenessRequestV1(
                    key, contract.source_id, contract.content_hash, start_ns, cutoff_ns,
                    cutoff_ns, min(MAX_ESTIMABLE_TRADE_ROWS, MAX_TRADE_WINDOW_ROWS_V1),
                )
                validation = collect_exact_trade_window_v1(
                    exact_trade_source, request, accepted_evidence=accepted_evidence,
                )
                if validation.complete and validation.evidence_ref is not None:
                    # The proof's exact source population is authoritative;
                    # discard archive-only rows from the estimation path.
                    proof_trades: list[CausalTrade] = []
                    for row in validation.trades:
                        if not isinstance(row, CausalTradeV2):
                            proof_trades = []
                            break
                        proof_trades.append(CausalTrade(
                            row.key, row.price, row.quantity, row.event_at_ns,
                            row.available_at_ns, row.raw_observation_ref, row.trade_id,
                        ))
                    if (len(proof_trades) <= MAX_ESTIMABLE_TRADE_ROWS
                            and len(proof_trades) == len(validation.trades)):
                        translated = sorted(proof_trades, key=lambda row: (row.event_at_ns, row.ref))
                        refs = tuple(sorted({trade.ref for trade in translated} | {
                            validation.evidence_ref, contract.content_hash,
                            contract.metadata_ref, contract.qualification_ref,
                        }))
                        proof_ref = validation.evidence_ref
                        proof_window_start_ns = start_ns
                        proof_window_end_exclusive_ns = cutoff_ns
                        proof_request_ref = request.content_hash
                        proof_source_contract_ref = contract.content_hash
                        proof_source_id = contract.source_id
                        coverage_state = "VERIFIED"
    return TradeLocationInputsV1(key, cutoff_ns, configuration, tuple(translated), refs,
                                 rejected, proof_ref, proof_window_start_ns,
                                 proof_window_end_exclusive_ns, proof_request_ref,
                                 proof_source_contract_ref, proof_source_id, coverage_state,
                                 "TRADE_COVERAGE_VERIFIED" if coverage_state == "VERIFIED"
                                 else "TRADE_COVERAGE_UNVERIFIED")
