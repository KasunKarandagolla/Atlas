"""Bounded, zero-authority maturation for S8 research baskets.

The forecast-side residual diagnostics use only the frozen 30-day prefix. The
outcome lane consumes an injected bounded loader for exact post-cutoff evidence;
it never derives fills, books, fees or funding from candle prices.
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from atlas.v2._serialization import json_value, sha256_json, sha256_ref, timestamp
from atlas.v2.math.relative_value import ScalarPoint, residual_half_life, stationarity_diagnostic
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.strategies.s8_pairs import (
    MAX_HOLD_NS,
    ResearchBasketForecastV2,
    S8HourlyPriceV2,
    S8PairDefinitionV2,
    s8_entry_side,
    simulate_s8_synchronized_prices,
)

S8_OUTCOME_LANE_V1 = "S8_BASKET_OUTCOME_V1"
MAX_S8_OUTCOMES_PER_CYCLE_V1 = 8
OUTCOME_RETRY_NS_V1 = 60_000_000_000


@dataclass(frozen=True)
class S8ResidualDiagnosticsV1:
    pair_ref: str
    forecast_ref: str
    cutoff_ns: int
    published_at_ns: int
    synchronized_price_refs: tuple[tuple[str, str], ...]
    half_life: Mapping[str, Any]
    stationarity: Mapping[str, Any]
    authority: str = "ZERO"

    def to_dict(self) -> dict[str, Any]:
        return json_value(
            {
                "version": "S8ResidualDiagnosticsV1",
                **self.__dict__,
                "capital_authority": "ZERO",
                "selector_influence": "ZERO",
            }
        )

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def diagnose_s8_residuals(
    pair: S8PairDefinitionV2,
    forecast: ResearchBasketForecastV2,
    *,
    prices_a: Sequence[S8HourlyPriceV2],
    prices_b: Sequence[S8HourlyPriceV2],
    published_at_ns: int,
) -> S8ResidualDiagnosticsV1:
    """Add residual AR(1) half-life and ADF diagnostics on the exact fit prefix.

    Both diagnostics are descriptive, uncalibrated research values. In
    particular an ADF statistic never turns this result into a stationarity or
    cointegration finding.
    """
    if forecast.pair_definition_ref != pair.content_hash:
        raise ValueError("S8 residual diagnostics pair/forecast identity mismatch")
    cutoff = timestamp(forecast.information_cutoff_ns, field="cutoff_ns")
    published = timestamp(published_at_ns, field="published_at_ns")
    if published < cutoff:
        raise ValueError("S8 residual diagnostics publication precedes cutoff")
    a_by_time: dict[int, S8HourlyPriceV2] = {}
    b_by_time: dict[int, S8HourlyPriceV2] = {}
    for row in prices_a:
        if row.instrument_key == pair.key_a and row.available_at_ns <= cutoff and row.hour_end_ns <= cutoff:
            if row.hour_end_ns in a_by_time and a_by_time[row.hour_end_ns].source_ref != row.source_ref:
                raise ValueError("S8 residual diagnostics reject ambiguous leg-A hour")
            a_by_time[row.hour_end_ns] = row
    for row in prices_b:
        if row.instrument_key == pair.key_b and row.available_at_ns <= cutoff and row.hour_end_ns <= cutoff:
            if row.hour_end_ns in b_by_time and b_by_time[row.hour_end_ns].source_ref != row.source_ref:
                raise ValueError("S8 residual diagnostics reject ambiguous leg-B hour")
            b_by_time[row.hour_end_ns] = row
    times = tuple(sorted(set(a_by_time) & set(b_by_time)))[-721:]
    expected_refs = tuple((a_by_time[t].source_ref, b_by_time[t].source_ref) for t in times)
    if (
        len(times) != 721
        or times[-1] != cutoff
        or expected_refs != forecast.synchronized_price_refs
        or any(times[i] - times[i - 1] != 3_600_000_000_000 for i in range(1, len(times)))
    ):
        raise ValueError("S8 residual diagnostics require the exact frozen synchronized 30-day prefix")
    residuals = tuple(
        math.log(float(a_by_time[t].close)) - forecast.alpha - forecast.beta * math.log(float(b_by_time[t].close))
        for t in times
    )
    points = tuple(
        ScalarPoint(
            t,
            max(a_by_time[t].available_at_ns, b_by_time[t].available_at_ns),
            value,
            sha256_json(
                {
                    "pair": pair.content_hash,
                    "a": a_by_time[t].source_ref,
                    "b": b_by_time[t].source_ref,
                    "frozen_beta": forecast.beta,
                }
            ),
        )
        for t, value in zip(times, residuals, strict=True)
    )
    # relative_value diagnostics have a 720-point hard bound.
    points = points[-720:]
    half_life = residual_half_life(points, cutoff_ns=cutoff, published_at_ns=published).to_dict()
    stationarity = stationarity_diagnostic(points, cutoff_ns=cutoff, published_at_ns=published).to_dict()
    return S8ResidualDiagnosticsV1(
        pair.content_hash, forecast.content_hash, cutoff, published, expected_refs, half_life, stationarity
    )


def persist_s8_residual_diagnostics(repo: OpsRepository, diagnostics: S8ResidualDiagnosticsV1) -> str:
    ref = diagnostics.content_hash
    entry = ArtifactIndexEntryV2(
        ref,
        "S8ResidualDiagnosticsV1",
        ref,
        diagnostics.published_at_ns,
        diagnostics.published_at_ns,
        {"diagnostics": diagnostics.to_dict()},
    )
    existing = repo.get_artifact(ref)
    if existing is None:
        repo.register_artifact(entry)
    elif existing != entry:
        raise ValueError("S8 residual diagnostic identity conflicts with retained artifact")
    return ref


@dataclass(frozen=True)
class S8BasketOutcomeEvidenceV1:
    """Loader result containing exact measured path and both-leg execution evidence.

    `source_refs` must point to the already indexed price, book/fill, fee and
    funding artifacts. The loader must return only artifacts available by the
    supplied evidence cutoff and retain their actual availability times.
    """

    forecast_ref: str
    prices_a: tuple[S8HourlyPriceV2, ...]
    prices_b: tuple[S8HourlyPriceV2, ...]
    leg_a_book_refs: tuple[str, ...]
    leg_b_book_refs: tuple[str, ...]
    leg_a_fee_ref: str | None
    leg_b_fee_ref: str | None
    leg_a_fee_value: Decimal | None
    leg_b_fee_value: Decimal | None
    leg_a_funding_refs: tuple[str, ...]
    leg_b_funding_refs: tuple[str, ...]
    leg_a_funding_cashflow: Decimal | None
    leg_b_funding_cashflow: Decimal | None
    leg_a_fill_state: str
    leg_b_fill_state: str
    sequential_delay_ns: tuple[int, int]
    orphan_leg_state: str
    available_at_ns: int

    def __post_init__(self) -> None:
        sha256_ref(self.forecast_ref, field="forecast_ref")
        timestamp(self.available_at_ns, field="available_at_ns")
        for name in ("leg_a_book_refs", "leg_b_book_refs", "leg_a_funding_refs", "leg_b_funding_refs"):
            refs = tuple(getattr(self, name))
            if refs != tuple(sorted(set(refs))):
                raise ValueError(f"{name} must be sorted and unique")
            for ref in refs:
                sha256_ref(ref, field=name)
        for ref in (self.leg_a_fee_ref, self.leg_b_fee_ref):
            if ref is not None:
                sha256_ref(ref, field="fee_ref")
        if (self.leg_a_fill_state not in {"UNAVAILABLE", "NO_FILL", "PARTIAL_FILL", "FULL_FILL"}
                or self.leg_b_fill_state not in {"UNAVAILABLE", "NO_FILL", "PARTIAL_FILL", "FULL_FILL"}):
            raise ValueError("S8 outcome fill state is invalid")
        if len(self.sequential_delay_ns) != 2 or any(type(x) is not int or x < 0 for x in self.sequential_delay_ns):
            raise ValueError("S8 outcome requires explicit nonnegative per-leg sequential delays")
        if not self.orphan_leg_state:
            raise ValueError("S8 outcome requires explicit orphan-leg risk state")

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        body = {
            **self.__dict__,
            "prices_a": [_price_dict(row) for row in self.prices_a],
            "prices_b": [_price_dict(row) for row in self.prices_b],
        }
        return json_value({"version": "S8BasketOutcomeEvidenceV1", **body})


@dataclass(frozen=True)
class S8BasketOutcomeV1:
    forecast_ref: str
    pair_ref: str
    decision_at_ns: int
    maturity_at_ns: int
    evidence_cutoff_ns: int
    outcome_status: str
    economic_status: str
    reason_codes: tuple[str, ...]
    evidence_ref: str | None
    price_path_a: tuple[Mapping[str, Any], ...]
    price_path_b: tuple[Mapping[str, Any], ...]
    simulation: Mapping[str, Any] | None
    source_refs: tuple[str, ...]
    available_at_ns: int

    def to_dict(self) -> dict[str, Any]:
        return json_value(
            {
                "version": "S8BasketOutcomeV1",
                **self.__dict__,
                "capital_authority": "ZERO",
                "single_action": False,
                "trade_plan_allowed": False,
            }
        )

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def enqueue_s8_basket_outcome(
    repo: OpsRepository, forecast: ResearchBasketForecastV2, *, forecast_ref: str, published_at_ns: int
) -> str:
    """Schedule one bounded maturity item after the frozen four-hour expiry."""
    sha256_ref(forecast_ref, field="forecast_ref")
    published = timestamp(published_at_ns, field="published_at_ns")
    if forecast_ref != forecast.content_hash or published < forecast.information_cutoff_ns:
        raise ValueError("S8 outcome schedule must bind its persisted causal forecast")
    due_at = forecast.decision_at_ns + MAX_HOLD_NS
    work_id = sha256_json({"version": "S8BasketOutcomeIdentityV1", "forecast_ref": forecast_ref})
    repo.enqueue_due_work(
        lane=S8_OUTCOME_LANE_V1,
        work_id=work_id,
        source_ref=forecast_ref,
        created_at_ns=published,
        due_at_ns=due_at,
        payload={"forecast_ref": forecast_ref, "decision_at_ns": forecast.decision_at_ns, "maturity_at_ns": due_at},
    )
    return work_id


class S8ResearchBasketOutcomeProducerV1:
    """Consume a bounded due page using a source-specific exact evidence loader."""

    def __init__(self, *, clock_ns: Callable[[], int]):
        self.clock_ns = clock_ns

    def run_cycle(
        self,
        repo: OpsRepository,
        *,
        evidence_cutoff_ns: int,
        max_items: int = MAX_S8_OUTCOMES_PER_CYCLE_V1,
        evidence_loader: Callable[[OpsRepository, ResearchBasketForecastV2, int], S8BasketOutcomeEvidenceV1 | None]
        | None = None,
    ) -> Mapping[str, Any]:
        cutoff = timestamp(evidence_cutoff_ns, field="evidence_cutoff_ns")
        if type(max_items) is not int or not 1 <= max_items <= MAX_S8_OUTCOMES_PER_CYCLE_V1:
            raise ValueError("S8 outcome page must remain within its fixed item bound")
        if repo.read_only:
            raise ValueError("S8 outcome maintenance requires the existing controller writer")
        work_items = repo.due_work_items(S8_OUTCOME_LANE_V1, as_of_ns=cutoff, limit=max_items)
        written = 0
        diagnostics: list[Mapping[str, str]] = []
        for work in work_items:
            try:
                forecast_entry = repo.get_artifact(work.source_ref)
                if (
                    forecast_entry is None
                    or forecast_entry.artifact_type != "ResearchBasketForecastV2"
                    or forecast_entry.content_hash != work.source_ref
                    or forecast_entry.available_at_ns > cutoff
                ):
                    raise ValueError("S8 forecast source is missing or unavailable")
                forecast_body = forecast_entry.metadata.get("basket")
                if not isinstance(forecast_body, Mapping):
                    raise ValueError("S8 forecast body is missing")
                forecast = _forecast_from_entry(forecast_body)
                if (
                    forecast.content_hash != work.source_ref
                    or work.payload.get("forecast_ref") != work.source_ref
                    or work.payload.get("maturity_at_ns") != forecast.decision_at_ns + MAX_HOLD_NS
                ):
                    raise ValueError("S8 due-work forecast identity conflicts")
                evidence = (evidence_loader or load_s8_basket_outcome_evidence)(repo, forecast, cutoff)
                publication_ns = timestamp(self.clock_ns(), field="published_at_ns")
                def outcome_clock(at_ns: int = publication_ns) -> int:
                    return at_ns

                outcome = _make_outcome(
                    repo, forecast, work.source_ref, evidence, cutoff,
                    outcome_clock,
                )
                if evidence is not None:
                    _persist_outcome_evidence(repo, evidence, published_at_ns=publication_ns)
                _persist_outcome(repo, outcome)
                repo.retire_due_work(
                    work.lane,
                    work.work_id,
                    reason_code="MATURED" if outcome.outcome_status == "OBSERVED_RESEARCH_PATH" else "NOT_ESTIMABLE",
                )
                written += 1
            except (ValueError, TypeError, KeyError, ArithmeticError) as exc:
                # A malformed source is quarantined; absence is represented by
                # the loader as None and becomes a durable NOT_ESTIMABLE row.
                repo.quarantine_due_work(work.lane, work.work_id, reason_code="S8_OUTCOME_LINEAGE_INVALID")
                diagnostics.append({"work_id": work.work_id, "reason": str(exc)[:160]})
        return {
            "status": "IMPLEMENTED",
            "outcomes_written": written,
            "inspected": len(work_items),
            "due_work": repo.due_work_pressure(S8_OUTCOME_LANE_V1, as_of_ns=cutoff, page_limit=max_items),
            "diagnostics": diagnostics,
            "authority": "ZERO",
        }


def _forecast_from_entry(body: Mapping[str, Any]) -> ResearchBasketForecastV2:
    from atlas.v2.instruments import InstrumentKeyV2
    from atlas.v2.strategies.s8_pairs import S8LegEvidenceV2

    a, b = body["leg_a_evidence"], body["leg_b_evidence"]
    return ResearchBasketForecastV2(
        body["profile_id"],
        body["pair_definition_ref"],
        body["fit_start_ns"],
        body["fit_end_ns"],
        body["information_cutoff_ns"],
        tuple(tuple(x) for x in body["synchronized_price_refs"]),
        body["alpha"],
        body["beta"],
        body["residual_mean"],
        body["residual_std"],
        body["current_residual"],
        body["current_z"],
        body["beta_frozen"],
        body["beta_frozen_at_ns"],
        body["decision_at_ns"],
        body["thresholds"]["entry_abs_z"],
        body["thresholds"]["convergence_abs_z"],
        body["thresholds"]["stop_abs_z"],
        body["time_exit_ns"],
        S8LegEvidenceV2(
            InstrumentKeyV2.from_dict(a["instrument_key"]),
            tuple(a["price_refs"]),
            tuple(a["book_execution_refs"]),
            a["fee_ref"],
            tuple(a["funding_refs"]),
            tuple(a["partial_fill_assumptions"]),
            tuple(a["sequential_delay_assumptions"]),
            tuple(a["orphan_leg_risk_states"]),
            a["available_at_ns"],
        ),
        S8LegEvidenceV2(
            InstrumentKeyV2.from_dict(b["instrument_key"]),
            tuple(b["price_refs"]),
            tuple(b["book_execution_refs"]),
            b["fee_ref"],
            tuple(b["funding_refs"]),
            tuple(b["partial_fill_assumptions"]),
            tuple(b["sequential_delay_assumptions"]),
            tuple(b["orphan_leg_risk_states"]),
            b["available_at_ns"],
        ),
        body["economic_status"],
        tuple(body["reasons"]),
    )


def _evidence_from_dict(body: Mapping[str, Any]) -> S8BasketOutcomeEvidenceV1:
    from atlas.v2.instruments import InstrumentKeyV2

    fields = {
        "version", "forecast_ref", "prices_a", "prices_b", "leg_a_book_refs", "leg_b_book_refs",
        "leg_a_fee_ref", "leg_b_fee_ref", "leg_a_fee_value", "leg_b_fee_value",
        "leg_a_funding_refs", "leg_b_funding_refs", "leg_a_funding_cashflow", "leg_b_funding_cashflow",
        "leg_a_fill_state", "leg_b_fill_state", "sequential_delay_ns", "orphan_leg_state", "available_at_ns",
    }
    if set(body) != fields or body.get("version") != "S8BasketOutcomeEvidenceV1":
        raise ValueError("S8 outcome evidence schema is invalid")

    def price_rows(name: str) -> tuple[S8HourlyPriceV2, ...]:
        rows = body[name]
        if not isinstance(rows, list) or len(rows) > 5:
            raise ValueError("S8 outcome evidence price path exceeds its bound")
        result = []
        for row in rows:
            if not isinstance(row, Mapping) or set(row) != {
                "instrument_key", "hour_end_ns", "available_at_ns", "close", "source_ref"
            }:
                raise ValueError("S8 outcome evidence price row is malformed")
            result.append(S8HourlyPriceV2(
                InstrumentKeyV2.from_dict(row["instrument_key"]), row["hour_end_ns"],
                row["available_at_ns"], Decimal(row["close"]), row["source_ref"],
            ))
        return tuple(result)

    def decimal_value(name: str) -> Decimal | None:
        value = body[name]
        if value is None:
            return None
        result = Decimal(str(value))
        if not result.is_finite():
            raise ValueError(f"S8 outcome evidence {name} is not finite")
        return result

    return S8BasketOutcomeEvidenceV1(
        body["forecast_ref"], price_rows("prices_a"), price_rows("prices_b"),
        tuple(body["leg_a_book_refs"]), tuple(body["leg_b_book_refs"]),
        body["leg_a_fee_ref"], body["leg_b_fee_ref"], decimal_value("leg_a_fee_value"),
        decimal_value("leg_b_fee_value"), tuple(body["leg_a_funding_refs"]),
        tuple(body["leg_b_funding_refs"]), decimal_value("leg_a_funding_cashflow"),
        decimal_value("leg_b_funding_cashflow"), body["leg_a_fill_state"], body["leg_b_fill_state"],
        tuple(body["sequential_delay_ns"]), body["orphan_leg_state"], body["available_at_ns"],
    )


def _price_dict(row: S8HourlyPriceV2) -> Mapping[str, Any]:
    return {
        "instrument_key": row.instrument_key.to_dict(),
        "hour_end_ns": row.hour_end_ns,
        "available_at_ns": row.available_at_ns,
        "close": str(row.close),
        "source_ref": row.source_ref,
    }


def _make_outcome(
    repo: OpsRepository,
    forecast: ResearchBasketForecastV2,
    forecast_ref: str,
    evidence: S8BasketOutcomeEvidenceV1 | None,
    cutoff: int,
    clock_ns: Callable[[], int],
) -> S8BasketOutcomeV1:
    from atlas.v2.instruments import InstrumentKeyV2

    maturity = forecast.decision_at_ns + MAX_HOLD_NS
    published = timestamp(clock_ns(), field="published_at_ns")
    if published < cutoff:
        raise ValueError("S8 outcome publication precedes evidence cutoff")
    if cutoff < maturity:
        raise ValueError("S8 outcome cannot mature before the frozen four-hour expiry")
    if evidence is None:
        return S8BasketOutcomeV1(
            forecast_ref,
            forecast.pair_definition_ref,
            forecast.decision_at_ns,
            maturity,
            cutoff,
            "NOT_ESTIMABLE",
            "NOT_ESTIMABLE",
            ("POST_CUTOFF_SYNCHRONIZED_PRICES_BOOKS_FEES_FUNDING_AND_BOTH_LEG_FILLS_MISSING",),
            None,
            (),
            (),
            None,
            (),
            published,
        )
    if evidence.forecast_ref != forecast_ref or evidence.available_at_ns > cutoff:
        raise ValueError("S8 matured evidence is not bound to this forecast and cutoff")
    missing: list[str] = []
    if not evidence.leg_a_book_refs:
        missing.append("LEG_A_POST_CUTOFF_BOOK_FILL_EVIDENCE_MISSING")
    if not evidence.leg_b_book_refs:
        missing.append("LEG_B_POST_CUTOFF_BOOK_FILL_EVIDENCE_MISSING")
    if evidence.leg_a_fee_ref is None or evidence.leg_a_fee_value is None:
        missing.append("LEG_A_FEE_EVIDENCE_MISSING")
    if evidence.leg_b_fee_ref is None or evidence.leg_b_fee_value is None:
        missing.append("LEG_B_FEE_EVIDENCE_MISSING")
    if not evidence.leg_a_funding_refs or evidence.leg_a_funding_cashflow is None:
        missing.append("LEG_A_FUNDING_EVIDENCE_MISSING")
    if not evidence.leg_b_funding_refs or evidence.leg_b_funding_cashflow is None:
        missing.append("LEG_B_FUNDING_EVIDENCE_MISSING")
    # NO_FILL and PARTIAL_FILL are observed simulation outcomes, not missing
    # evidence. Only an unavailable state blocks path classification.
    if evidence.leg_a_fill_state == "UNAVAILABLE":
        missing.append("LEG_A_EXECUTION_OUTCOME_UNAVAILABLE")
    if evidence.leg_b_fill_state == "UNAVAILABLE":
        missing.append("LEG_B_EXECUTION_OUTCOME_UNAVAILABLE")
    refs = set(
        evidence.leg_a_book_refs + evidence.leg_b_book_refs + evidence.leg_a_funding_refs + evidence.leg_b_funding_refs
    )
    refs.update(ref for ref in (evidence.leg_a_fee_ref, evidence.leg_b_fee_ref) if ref)
    refs.update(row.source_ref for row in (*evidence.prices_a, *evidence.prices_b))
    price_a_refs = {row.source_ref for row in evidence.prices_a}
    price_b_refs = {row.source_ref for row in evidence.prices_b}
    book_a_refs, book_b_refs = set(evidence.leg_a_book_refs), set(evidence.leg_b_book_refs)
    funding_a_refs, funding_b_refs = set(evidence.leg_a_funding_refs), set(evidence.leg_b_funding_refs)
    typed_groups = (price_a_refs, price_b_refs, book_a_refs, book_b_refs, funding_a_refs, funding_b_refs,
                    {ref for ref in (evidence.leg_a_fee_ref,) if ref},
                    {ref for ref in (evidence.leg_b_fee_ref,) if ref})
    if any(left & right for index, left in enumerate(typed_groups) for right in typed_groups[index + 1:]):
        raise ValueError("S8 typed source reference is assigned to conflicting evidence roles")
    book_fill_states: dict[str, list[str]] = {"a": [], "b": []}
    funding_totals: dict[str, Decimal] = {"a": Decimal("0"), "b": Decimal("0")}
    fee_values: dict[str, Decimal] = {}
    quote_source_refs: set[str] = set()
    quote_indexes: dict[str, tuple[ArtifactIndexEntryV2, Any]] = {}
    stream_quote_indexes: dict[str, tuple[ArtifactIndexEntryV2, Any, int]] = {}
    for ref in refs:
        entry = repo.get_artifact(ref)
        if entry is None or entry.artifact_ref != ref or entry.available_at_ns > cutoff:
            raise ValueError("S8 outcome component source is absent, conflicting or future")
        if ref in {row.source_ref for row in (*evidence.prices_a, *evidence.prices_b)}:
            match = next(row for row in (*evidence.prices_a, *evidence.prices_b) if row.source_ref == ref)
            if (
                entry.artifact_type != "PublicObservationIndexV2"
                or entry.metadata.get("instrument_key_json") != match.instrument_key.to_canonical_json()
                or entry.metadata.get("event_at_ns") != match.hour_end_ns
                or not isinstance(entry.metadata.get("bar_content_hash"), str)
            ):
                raise ValueError("S8 path price reference has a different instrument/hour identity")
        elif ref in evidence.leg_a_book_refs + evidence.leg_b_book_refs:
            leg_key = (
                forecast.leg_a_evidence.instrument_key
                if ref in evidence.leg_a_book_refs
                else forecast.leg_b_evidence.instrument_key
            )
            body = entry.metadata.get("book_execution")
            wire = json_value(body) if isinstance(body, Mapping) else None
            if (
                entry.artifact_type != "S8BasketBookExecutionEvidenceV1"
                or not isinstance(body, Mapping)
                or not isinstance(wire, Mapping)
                or wire.get("instrument_key") != leg_key.to_dict()
                or type(wire.get("observed_at_ns")) is not int
                or not forecast.information_cutoff_ns < wire["observed_at_ns"] <= maturity
                or entry.available_at_ns < wire["observed_at_ns"]
                or not isinstance(wire.get("quote_source_refs"), list)
                or len(wire["quote_source_refs"]) > 32
                or not wire["quote_source_refs"]
                or wire.get("simulated_fill_state") not in {"NO_FILL", "PARTIAL_FILL", "FULL_FILL"}
                or sha256_json(wire) != entry.content_hash
            ):
                raise ValueError(
                    "S8 book/fill source is not typed book execution evidence: "
                    f"{entry.artifact_type}/{dict(body) if isinstance(body, Mapping) else None}"
                )
            leg_name = "a" if ref in book_a_refs else "b"
            book_fill_states[leg_name].append(wire["simulated_fill_state"])
            for quote_ref in wire["quote_source_refs"]:
                quote_entry = repo.get_artifact(quote_ref)
                quote_metadata = json_value(quote_entry.metadata) if quote_entry is not None else None
                if (
                    quote_entry is None
                    or quote_entry.artifact_type not in {
                        "PublicObservationIndexV2",
                        "PublicStreamFrameIndexV1",
                    }
                    or quote_entry.available_at_ns > cutoff
                    or quote_entry.available_at_ns > wire["observed_at_ns"]
                    or not isinstance(quote_metadata, Mapping)
                    or quote_ref in refs
                ):
                    raise ValueError("S8 book quote reference is absent, mistyped or future")
                if quote_entry.artifact_type == "PublicObservationIndexV2":
                    event_type = quote_metadata.get("event_type")
                    if event_type not in {"TICKER_MARK_INDEX_FUNDING_OI", "BOOK_TICKER", "TICKER_24H"}:
                        raise ValueError("S8 book quote observation is not an approved best-bid/ask source")
                    try:
                        quote_key = InstrumentKeyV2.from_dict(
                            json.loads(quote_metadata["instrument_key_json"])
                        )
                    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
                        raise ValueError("S8 book quote has no exact instrument identity") from error
                    quote_at = quote_metadata.get("event_at_ns")
                else:
                    instrument = quote_metadata.get("instrument")
                    if not isinstance(instrument, Mapping):
                        raise ValueError("S8 stream quote has no exact instrument identity")
                    try:
                        quote_key = InstrumentKeyV2.from_dict(instrument)
                    except (TypeError, ValueError) as error:
                        raise ValueError("S8 stream quote has no exact instrument identity") from error
                    quote_at = quote_metadata.get("event_at_ns")
                if (
                    quote_key != leg_key
                    or type(quote_at) is not int
                    or quote_at > wire["observed_at_ns"]
                    or quote_at > quote_entry.available_at_ns
                    or quote_metadata.get("available_at_ns", quote_entry.available_at_ns)
                    != quote_entry.available_at_ns
                ):
                    raise ValueError("S8 book quote identity or chronology conflicts with its leg")
                if quote_entry.artifact_type == "PublicObservationIndexV2":
                    quote_indexes[quote_ref] = (quote_entry, leg_key)
                else:
                    frame_type = quote_metadata.get("frame_type")
                    channel = quote_metadata.get("channel")
                    if (not isinstance(frame_type, str) or frame_type not in {"SNAPSHOT", "DELTA"}
                            or not isinstance(channel, str)
                            or not (channel.startswith("orderbook.") or "@depth" in channel)):
                        raise ValueError("S8 book quote does not reference a typed public order-book frame")
                    stream_quote_indexes[quote_ref] = (quote_entry, leg_key, wire["observed_at_ns"])
                quote_source_refs.add(quote_ref)
        elif ref in evidence.leg_a_funding_refs + evidence.leg_b_funding_refs:
            leg_key = (
                forecast.leg_a_evidence.instrument_key
                if ref in evidence.leg_a_funding_refs
                else forecast.leg_b_evidence.instrument_key
            )
            body = entry.metadata.get("funding")
            wire = json_value(body) if isinstance(body, Mapping) else None
            if (
                entry.artifact_type != "S8BasketFundingEvidenceV1"
                or not isinstance(body, Mapping)
                or not isinstance(wire, Mapping)
                or wire.get("instrument_key") != leg_key.to_dict()
                or type(wire.get("at_ns")) is not int
                or type(wire.get("available_at_ns")) is not int
                or not forecast.information_cutoff_ns < wire["at_ns"] <= maturity
                or wire["available_at_ns"] < wire["at_ns"]
                or wire["available_at_ns"] > cutoff
                or entry.available_at_ns < wire["available_at_ns"]
                or sha256_json(wire) != entry.content_hash
                ):
                raise ValueError(
                    "S8 funding source is not typed settlement evidence: "
                    f"{entry.artifact_type}/{dict(body) if isinstance(body, Mapping) else None}"
                )
            cashflow = wire.get("cashflow")
            if not isinstance(cashflow, str):
                raise ValueError("S8 funding source cashflow must be an exact decimal string")
            try:
                parsed_cashflow = Decimal(cashflow)
            except InvalidOperation as error:
                raise ValueError("S8 funding source cashflow is invalid") from error
            if not parsed_cashflow.is_finite():
                raise ValueError("S8 funding source cashflow is non-finite")
            leg_name = "a" if ref in funding_a_refs else "b"
            funding_totals[leg_name] += parsed_cashflow
        elif ref in (evidence.leg_a_fee_ref, evidence.leg_b_fee_ref):
            leg_key = (
                forecast.leg_a_evidence.instrument_key
                if ref == evidence.leg_a_fee_ref
                else forecast.leg_b_evidence.instrument_key
            )
            body = entry.metadata.get("fee")
            wire = json_value(body) if isinstance(body, Mapping) else None
            if (
                entry.artifact_type != "S8BasketFeeEvidenceV1"
                or not isinstance(body, Mapping)
                or not isinstance(wire, Mapping)
                or wire.get("instrument_key") != leg_key.to_dict()
                or type(wire.get("available_at_ns")) is not int
                or wire["available_at_ns"] > cutoff
                or entry.available_at_ns < wire["available_at_ns"]
                or not isinstance(wire.get("fee_policy"), Mapping)
                or sha256_json(wire) != entry.content_hash
            ):
                raise ValueError("S8 fee source is not typed fee evidence")
            paid = wire["fee_policy"].get("paid")
            if not isinstance(paid, str):
                raise ValueError("S8 fee source paid amount must be an exact decimal string")
            try:
                parsed_paid = Decimal(paid)
            except InvalidOperation as error:
                raise ValueError("S8 fee source paid amount is invalid") from error
            if not parsed_paid.is_finite():
                raise ValueError("S8 fee source paid amount is non-finite")
            fee_values[ref] = parsed_paid
        origin_refs = set(forecast.synchronized_price_refs[-1])
        if entry.available_at_ns <= forecast.information_cutoff_ns and ref not in origin_refs | {
            evidence.leg_a_fee_ref,
            evidence.leg_b_fee_ref,
        }:
                raise ValueError("S8 outcome path evidence must be published after the forecast cutoff")
    expected_fill_states = {
        "a": evidence.leg_a_fill_state,
        "b": evidence.leg_b_fill_state,
    }
    for leg_name, source_states in book_fill_states.items():
        if source_states:
            if len(set(source_states)) != 1 or expected_fill_states[leg_name] != source_states[0]:
                raise ValueError("S8 aggregate fill state conflicts with typed book evidence")
        elif expected_fill_states[leg_name] != "UNAVAILABLE":
            raise ValueError("S8 aggregate fill state has no typed book evidence")
    for leg_name, refs_for_leg, aggregate in (
        ("a", evidence.leg_a_funding_refs, evidence.leg_a_funding_cashflow),
        ("b", evidence.leg_b_funding_refs, evidence.leg_b_funding_cashflow),
    ):
        if refs_for_leg:
            if aggregate is None or aggregate != funding_totals[leg_name]:
                raise ValueError("S8 aggregate funding cashflow conflicts with typed funding evidence")
        elif aggregate is not None:
            raise ValueError("S8 aggregate funding cashflow has no typed funding evidence")
    for fee_ref, aggregate in ((evidence.leg_a_fee_ref, evidence.leg_a_fee_value),
                               (evidence.leg_b_fee_ref, evidence.leg_b_fee_value)):
        if fee_ref is not None:
            if aggregate is None or aggregate != fee_values[fee_ref]:
                raise ValueError("S8 aggregate fee value conflicts with typed fee evidence")
        elif aggregate is not None:
            raise ValueError("S8 aggregate fee value has no typed fee evidence")
    _verify_s8_public_quote_archives(repo, quote_indexes, cutoff_ns=cutoff)
    _verify_s8_stream_quote_archives(repo, stream_quote_indexes, cutoff_ns=cutoff)
    _verify_s8_price_path_against_archive(
        repo,
        forecast.leg_a_evidence.instrument_key,
        evidence.prices_a,
        cutoff_ns=cutoff,
    )
    _verify_s8_price_path_against_archive(
        repo,
        forecast.leg_b_evidence.instrument_key,
        evidence.prices_b,
        cutoff_ns=cutoff,
    )
    sim = None
    prices_complete = (
        len(evidence.prices_a) == 5
        and len(evidence.prices_b) == 5
        and all(a.hour_end_ns == b.hour_end_ns for a, b in zip(evidence.prices_a, evidence.prices_b, strict=True))
        and tuple(row.hour_end_ns for row in evidence.prices_a)
        == tuple(forecast.decision_at_ns + i * 3_600_000_000_000 for i in range(5))
    )
    if not prices_complete:
        missing.append("FOUR_HOUR_SYNCHRONIZED_PRICE_PATH_INCOMPLETE")
    elif s8_entry_side(forecast) is None:
        missing = ["NO_ENTRY_THRESHOLD_CROSSED"]
    if prices_complete and not (
        evidence.leg_a_fill_state == "UNAVAILABLE" or evidence.leg_b_fill_state == "UNAVAILABLE"
    ):
        if "NO_ENTRY_THRESHOLD_CROSSED" not in missing and not missing:
            raw_sim = simulate_s8_synchronized_prices(
                forecast,
                prices_a=evidence.prices_a,
                prices_b=evidence.prices_b,
                leg_a_fill_state=evidence.leg_a_fill_state,
                leg_b_fill_state=evidence.leg_b_fill_state,
                fee_refs=(evidence.leg_a_fee_ref, evidence.leg_b_fee_ref),
                funding_refs=(evidence.leg_a_funding_refs, evidence.leg_b_funding_refs),
                sequential_delay_ns=evidence.sequential_delay_ns,
                orphan_leg_state=evidence.orphan_leg_state,
                leg_fee_values=(evidence.leg_a_fee_value, evidence.leg_b_fee_value),
                leg_funding_cashflows=(evidence.leg_a_funding_cashflow, evidence.leg_b_funding_cashflow),
            )
            sim = raw_sim.to_dict()
    refs.update(quote_source_refs)
    refs.add(evidence.content_hash)
    source_refs = tuple(sorted(refs))
    outcome_status = (
        "NO_ENTRY"
        if "NO_ENTRY_THRESHOLD_CROSSED" in missing
        else "OBSERVED_RESEARCH_PATH"
        if sim is not None or prices_complete
        else "NOT_ESTIMABLE"
    )
    return S8BasketOutcomeV1(
        forecast_ref,
        forecast.pair_definition_ref,
        forecast.decision_at_ns,
        maturity,
        cutoff,
        outcome_status,
        "NOT_ESTIMABLE",
        tuple(sorted(missing)) if missing else ("ECONOMIC_PNL_NOT_COMPUTED_NO_EXECUTABLE_AUTHORITY",),
        evidence.content_hash,
        tuple(_price_dict(row) for row in evidence.prices_a),
        tuple(_price_dict(row) for row in evidence.prices_b),
        sim,
        source_refs,
        published,
    )


def _verify_s8_price_path_against_archive(
    repo: OpsRepository,
    key: Any,
    prices: Sequence[S8HourlyPriceV2],
    *,
    cutoff_ns: int,
) -> None:
    """Bind every supplied H1 value to its exact immutable archived bar."""
    if not prices:
        return
    from atlas.v2.data.bars import BarIntervalV2
    from atlas.v2.data.history import reconstruct_indexed_causal_bars_v1

    entries = tuple(repo.get_artifact(row.source_ref) for row in prices)
    if any(entry is None for entry in entries):
        raise ValueError("S8 outcome price path source index is absent")
    reconstructed = reconstruct_indexed_causal_bars_v1(
        repo,
        Path(repo.path).parent / "ops-observations",
        key=key,
        interval=BarIntervalV2.H1,
        index_entries=entries,
        information_cutoff_ns=cutoff_ns,
    )
    bars = {item.observation_index_ref: item.bar for item in reconstructed}
    if len(bars) != len(prices):
        raise ValueError("S8 outcome path archive is incomplete or ambiguous")
    for price in prices:
        bar = bars.get(price.source_ref)
        if (
            bar is None
            or price.instrument_key != key
            or price.hour_end_ns != bar.close_at_ns
            or price.available_at_ns != bar.raw.available_at_ns
            or price.close != bar.close
        ):
            raise ValueError("S8 outcome price differs from its exact archived source bar")


def _verify_s8_stream_quote_archives(
    repo: OpsRepository,
    quote_indexes: Mapping[str, tuple[ArtifactIndexEntryV2, Any, int]],
    *,
    cutoff_ns: int,
) -> None:
    """Bind stream quote indexes to exact, identity-checked order-book bytes."""
    if not quote_indexes:
        return
    from ..data.microstructure_archive import L2FrameArchiveV2, L2RawFrameV2
    from ..instruments import InstrumentKeyV2

    archive = L2FrameArchiveV2(Path(repo.path).parent / "ops-l2-frames", repo)
    frames_by_chunk: dict[str, dict[str, L2RawFrameV2]] = {}
    for _quote_ref, (entry, expected_key, observed_at_ns) in quote_indexes.items():
        body = json_value(entry.metadata)
        required = {
            "record_id", "instrument", "instrument_hash", "source_id", "channel", "frame_type",
            "event_at_ns", "received_at_ns", "available_at_ns", "raw_payload_hash",
            "archive_chunk_id", "sequence_semantics", "authority",
        }
        if (not isinstance(body, Mapping) or set(body) != required
                or body.get("authority") != "ZERO"
                or entry.content_hash != sha256_json(body)
                or entry.artifact_ref != sha256_json({
                    "artifact_type": "PublicStreamFrameIndexV1", "record_id": body.get("record_id")
                })
                or entry.available_at_ns != body.get("available_at_ns")
                or entry.created_at_ns != entry.available_at_ns
                or entry.available_at_ns > min(cutoff_ns, observed_at_ns)):
            raise ValueError("S8 order-book frame index identity or chronology is invalid")
        try:
            indexed_key = InstrumentKeyV2.from_dict(body["instrument"])
            chunk_id = body["archive_chunk_id"]
            sha256_ref(chunk_id, field="archive_chunk_id")
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("S8 order-book frame index lacks its exact instrument or chunk") from error
        channel = body["channel"]
        if (indexed_key != expected_key or body["instrument_hash"] != expected_key.content_hash
                or not isinstance(body["frame_type"], str)
                or body["frame_type"] not in {"SNAPSHOT", "DELTA"}
                or not isinstance(channel, str)
                or not (channel.startswith("orderbook.") or "@depth" in channel)):
            raise ValueError("S8 order-book frame index does not match its book leg")
        frames_by_record = frames_by_chunk.get(chunk_id)
        if frames_by_record is None:
            rows = archive.read_chunk(chunk_id)
            if not 1 <= len(rows) <= 512:
                raise ValueError("S8 order-book frame archive exceeds its read bound")
            frames: list[L2RawFrameV2] = []
            for row in rows:
                instrument = row.get("instrument")
                payload = row.get("raw_payload_bytes")
                if not isinstance(instrument, Mapping) or not isinstance(payload, (bytes, bytearray, memoryview)):
                    raise ValueError("S8 archived order-book frame is missing typed bytes")
                try:
                    frame = L2RawFrameV2(
                        InstrumentKeyV2.from_dict(instrument), str(row["source_id"]), str(row["channel"]),
                        str(row["frame_type"]), bytes(payload), str(row["raw_payload_hash"]),
                        row.get("event_at_ns"), int(row["received_at_ns"]), int(row["available_at_ns"]),
                        row.get("first_update_id"), row.get("last_update_id"), row.get("previous_update_id"),
                        str(row["sequence_semantics"]), str(row["source_health"]),
                        str(row["availability_class"]), row.get("source_health_ref"),
                    )
                except (KeyError, TypeError, ValueError) as error:
                    raise ValueError("S8 archived order-book frame schema is invalid") from error
                if frame.record_id != row.get("record_id"):
                    raise ValueError("S8 archived order-book frame record identity mismatch")
                frames.append(frame)
            ordered = sorted(frames, key=lambda frame: (
                frame.available_at_ns, frame.source_id, frame.channel,
                frame.last_update_id if frame.last_update_id is not None else -1,
                frame.raw_payload_hash,
            ))
            expected_chunk_id = sha256_json({
                "archive_type": "L2RawFrameChunkV2",
                "frames": [frame.metadata_dict() for frame in ordered],
            })
            if expected_chunk_id != chunk_id:
                raise ValueError("S8 archived order-book chunk identity mismatch")
            frames_by_record = {frame.record_id: frame for frame in frames}
            if len(frames_by_record) != len(frames):
                raise ValueError("S8 archived order-book chunk has duplicate record identities")
            frames_by_chunk[chunk_id] = frames_by_record
        selected_frame = frames_by_record.get(body["record_id"])
        if selected_frame is None:
            raise ValueError("S8 order-book frame index points outside its archived chunk")
        expected_index = {
            "record_id": selected_frame.record_id,
            "instrument": selected_frame.instrument.to_dict(),
            "instrument_hash": selected_frame.instrument.content_hash,
            "source_id": selected_frame.source_id,
            "channel": selected_frame.channel,
            "frame_type": selected_frame.frame_type,
            "event_at_ns": selected_frame.event_at_ns,
            "received_at_ns": selected_frame.received_at_ns,
            "available_at_ns": selected_frame.available_at_ns,
            "raw_payload_hash": selected_frame.raw_payload_hash,
            "archive_chunk_id": chunk_id,
            "sequence_semantics": selected_frame.sequence_semantics,
            "authority": "ZERO",
        }
        if body != expected_index or selected_frame.available_at_ns > observed_at_ns:
            raise ValueError("S8 order-book bytes conflict with the indexed quote receipt")


def _verify_s8_public_quote_archives(
    repo: OpsRepository,
    quote_indexes: Mapping[str, tuple[ArtifactIndexEntryV2, Any]],
    *,
    cutoff_ns: int,
) -> None:
    """Bind a bounded public quote set to archived bytes with one scan per leg."""
    if not quote_indexes:
        return
    from atlas.v2.data.history import reconstruct_public_observations_from_archive

    by_key: dict[str, tuple[Any, set[str], set[str]]] = {}
    for ref, (entry, key) in quote_indexes.items():
        event_type = entry.metadata.get("event_type")
        if not isinstance(event_type, str) or not event_type:
            raise ValueError("S8 quote index lacks its exact event type")
        canonical_key = key.to_canonical_json()
        group = by_key.setdefault(canonical_key, (key, set(), set()))
        group[1].add(event_type)
        group[2].add(ref)
    if len(by_key) > 2:
        raise ValueError("S8 basket quote lineage exceeds its two-leg bound")
    for key, event_types, expected_refs in by_key.values():
        if len(event_types) > 8 or len(expected_refs) > 256:
            raise ValueError("S8 basket quote source vector exceeds its fixed bound")
        rows = reconstruct_public_observations_from_archive(
            repo,
            Path(repo.path).parent / "ops-observations",
            instrument_revision=key.contract_revision,
            information_cutoff_ns=cutoff_ns,
            event_types=tuple(sorted(event_types)),
            limit=8193,
            key=key,
        )
        retained_refs = {row.observation_index_ref for row in rows}
        if not expected_refs.issubset(retained_refs):
            raise ValueError("S8 quote index does not resolve to retained immutable public bytes")


def load_s8_basket_outcome_evidence(
    repo: OpsRepository, forecast: ResearchBasketForecastV2, evidence_cutoff_ns: int
) -> S8BasketOutcomeEvidenceV1 | None:
    """Load only the bounded indexed H1 origin path; books/costs/fills stay missing.

    The archive index selects at most five exact recent H1 origins per leg and
    the causal reconstructor verifies each selected row against immutable
    Parquet bytes. This loader intentionally does not invent execution evidence.
    A venue specific evidence publisher may enrich it with typed book, fee,
    funding and simulated fill records using the callback contract.
    """
    from atlas.v2.data.bars import BarIntervalV2
    from atlas.v2.data.history import reconstruct_indexed_causal_bars_v1

    cutoff = timestamp(evidence_cutoff_ns, field="evidence_cutoff_ns")
    if cutoff < forecast.decision_at_ns + MAX_HOLD_NS:
        return None
    archive_root = Path(repo.path).parent / "ops-observations"
    price_paths: list[tuple[S8HourlyPriceV2, ...]] = []
    for leg in (forecast.leg_a_evidence.instrument_key, forecast.leg_b_evidence.instrument_key):
        entries = repo.public_archive_history_entries(
            instrument_revision=leg.contract_revision,
            instrument_key_json=leg.to_canonical_json(),
            event_types=("BAR_1H",),
            information_cutoff_ns=cutoff,
            limit=5,
            availability_class="ACTUAL_SYSTEM",
        )
        indexed = reconstruct_indexed_causal_bars_v1(
            repo, archive_root, key=leg, interval=BarIntervalV2.H1, index_entries=entries, information_cutoff_ns=cutoff
        )
        converted = tuple(
            S8HourlyPriceV2(
                leg, item.bar.close_at_ns, item.bar.raw.available_at_ns, item.bar.close, item.observation_index_ref
            )
            for item in indexed
        )
        price_paths.append(converted)
    if len(price_paths[0]) != 5 or len(price_paths[1]) != 5:
        return S8BasketOutcomeEvidenceV1(
            forecast.content_hash,
            price_paths[0],
            price_paths[1],
            (),
            (),
            None,
            None,
            None,
            None,
            (),
            (),
            None,
            None,
            "UNAVAILABLE",
            "UNAVAILABLE",
            (0, 0),
            "UNAVAILABLE",
            cutoff,
        )
    return S8BasketOutcomeEvidenceV1(
        forecast.content_hash,
        price_paths[0],
        price_paths[1],
        (),
        (),
        None,
        None,
        None,
        None,
        (),
        (),
        None,
        None,
        "UNAVAILABLE",
        "UNAVAILABLE",
        (0, 0),
        "UNAVAILABLE",
        cutoff,
    )


def _outcome_evidence_input_refs(
    repo: OpsRepository, evidence: S8BasketOutcomeEvidenceV1,
) -> tuple[str, ...]:
    refs = {evidence.forecast_ref, *[row.source_ref for row in (*evidence.prices_a, *evidence.prices_b)],
        *evidence.leg_a_book_refs, *evidence.leg_b_book_refs,
        *evidence.leg_a_funding_refs, *evidence.leg_b_funding_refs}
    refs.update(ref for ref in (evidence.leg_a_fee_ref, evidence.leg_b_fee_ref) if ref is not None)
    for book_ref in (*evidence.leg_a_book_refs, *evidence.leg_b_book_refs):
        entry = repo.get_artifact(book_ref)
        body = entry.metadata.get("book_execution") if entry is not None else None
        wire = json_value(body) if isinstance(body, Mapping) else None
        quote_refs = wire.get("quote_source_refs") if isinstance(wire, Mapping) else None
        if not isinstance(quote_refs, list):
            raise ValueError("S8 outcome evidence book source is unavailable")
        for quote_ref in quote_refs:
            sha256_ref(quote_ref, field="quote_source_ref")
        refs.update(quote_refs)
    if len(refs) > 256:
        raise ValueError("S8 outcome evidence source vector exceeds its bound")
    return tuple(sorted(refs))


def _persist_outcome_evidence(
    repo: OpsRepository,
    evidence: S8BasketOutcomeEvidenceV1,
    *,
    published_at_ns: int,
) -> str:
    from atlas.v2.chronology import record_computation

    ref = evidence.content_hash
    body = evidence.to_dict()
    input_refs = _outcome_evidence_input_refs(repo, evidence)
    entry = ArtifactIndexEntryV2(ref, "S8BasketOutcomeEvidenceV1", ref,
        published_at_ns, published_at_ns,
        {"evidence": body, "input_refs": list(input_refs)})
    existing = repo.get_artifact(ref)
    if existing is None:
        repo.register_artifact(entry)
    elif existing != entry:
        raise ValueError("S8 outcome evidence identity conflicts with retained artifact")
    record_computation(
        repo,
        artifact_ref=ref,
        information_cutoff_ns=evidence.available_at_ns,
        started_ns=published_at_ns,
        finished_ns=published_at_ns,
        available_ns=published_at_ns,
        input_refs=input_refs,
        deadline_ns=published_at_ns,
    )
    return ref


def _persist_outcome(repo: OpsRepository, outcome: S8BasketOutcomeV1) -> str:
    ref = outcome.content_hash
    existing = repo.get_artifact(ref)
    entry = ArtifactIndexEntryV2(
        ref, "S8BasketOutcomeV1", ref, outcome.available_at_ns, outcome.available_at_ns, {"outcome": outcome.to_dict()}
    )
    if existing is None:
        repo.register_artifact(entry)
    elif existing != entry:
        raise ValueError("S8 outcome identity conflicts with retained artifact")
    from atlas.v2.chronology import record_computation

    record_computation(
        repo,
        artifact_ref=ref,
        information_cutoff_ns=outcome.evidence_cutoff_ns,
        started_ns=outcome.available_at_ns,
        finished_ns=outcome.available_at_ns,
        available_ns=outcome.available_at_ns,
        input_refs=(outcome.forecast_ref, *outcome.source_refs),
        deadline_ns=outcome.available_at_ns,
    )
    return ref
