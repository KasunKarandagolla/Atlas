"""Focused bounded S8 residual diagnostics and outcome maturation tests."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.history import ImportedObservationV2, ParquetObservationArchiveV2
from atlas.v2.data.microstructure_archive import L2FrameArchiveV2, L2RawFrameV2
from atlas.v2.data.raw import RawObservationV2
from atlas.v2.instruments import EnvironmentV2, InstrumentKeyV2, ProductTypeV2, VenueV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.research_basket_outcomes import (
    S8BasketOutcomeEvidenceV1,
    S8ResearchBasketOutcomeProducerV1,
    _make_outcome,
    diagnose_s8_residuals,
    enqueue_s8_basket_outcome,
)
from atlas.v2.strategies.s8_pairs import (
    HOUR_NS,
    S8HourlyPriceV2,
    S8LegEvidenceV2,
    S8PairDefinitionV2,
    build_research_basket_forecast,
)

START = 1_800_000_000_000_000_000


def _key(symbol: str, base: str) -> InstrumentKeyV2:
    return InstrumentKeyV2(VenueV2.BYBIT, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL, symbol,
        base, "USDT", "USDT", sha256_json({"test_contract_revision": symbol}))


def _artifact(repo, kind: str, identity: object, available_at: int) -> str:
    body = {"kind": kind, "identity": identity, "available_at_ns": available_at}
    ref = sha256_json(body)
    repo.register_artifact(ArtifactIndexEntryV2(ref, kind, ref, available_at, available_at, body))
    return ref


def _typed_artifact(repo, kind: str, wrapper: str, body: dict, available_at: int) -> str:
    ref = sha256_json(body)
    repo.register_artifact(ArtifactIndexEntryV2(ref, kind, ref, available_at, available_at,
        {wrapper: body}))
    return ref


def _quote_index(repo, key, event_at_ns: int, label: str, *, event_type: str = "TICKER_MARK_INDEX_FUNDING_OI") -> str:
    payload = canonical_json({"symbol": key.native_symbol, "quote_fixture": label}).encode()
    raw = RawObservationV2.build(
        instrument_revision=key.contract_revision,
        source_id=f"{key.venue.value}_PUBLIC_V2",
        event_type=event_type,
        event_at_ns=event_at_ns,
        received_at_ns=event_at_ns,
        ingested_at_ns=event_at_ns,
        available_at_ns=event_at_ns,
        payload=payload,
        translation_version="SESSION041_S8_QUOTE_FIXTURE_V1",
        sequence=f"{label}:{event_at_ns}",
    )
    chunk_id = sha256_json({"quote_chunk": raw.record_id})
    ParquetObservationArchiveV2(Path(repo.path).parent / "ops-observations").write_observation_chunk(
        chunk_id, (ImportedObservationV2(1, raw, payload),)
    )
    ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": raw.record_id})
    metadata = {name: raw.to_dict()[name] for name in (
        "record_id", "source_id", "event_type", "instrument_revision", "event_at_ns",
        "published_at_ns", "translation_version", "revision_of", "quality_flags",
        "availability_class", "replay_available_at_ns", "raw_payload_hash",
    )}
    metadata.update({"instrument_key_json": key.to_canonical_json(), "archive_chunk_id": chunk_id})
    repo.register_artifact(ArtifactIndexEntryV2(
        ref, "PublicObservationIndexV2", raw.content_hash, raw.received_at_ns,
        raw.available_at_ns, metadata,
    ))
    return ref


def _stream_quote_index(repo, key, event_at_ns: int, *, received_at_ns: int | None = None) -> str:
    received = event_at_ns if received_at_ns is None else received_at_ns
    payload = canonical_json({"symbol": key.native_symbol, "bid": "100", "ask": "101"}).encode()
    frame = L2RawFrameV2(
        key, f"{key.venue.value}_PUBLIC_WS_BROAD_V2", f"orderbook.50.{key.native_symbol}",
        "SNAPSHOT", payload, hashlib.sha256(payload).hexdigest(), event_at_ns,
        received, received, 10, 10, None, "BYBIT_U", "HEALTHY_CURRENT", "ACTUAL_SYSTEM",
    )
    archive = L2FrameArchiveV2(Path(repo.path).parent / "ops-l2-frames", repo,
        compact_live=True, clock_ns=lambda: received + 1)
    chunk_id, _path = archive.write_chunk((frame,))
    body = {
        "record_id": frame.record_id,
        "instrument": frame.instrument.to_dict(),
        "instrument_hash": frame.instrument.content_hash,
        "source_id": frame.source_id,
        "channel": frame.channel,
        "frame_type": frame.frame_type,
        "event_at_ns": frame.event_at_ns,
        "received_at_ns": frame.received_at_ns,
        "available_at_ns": frame.available_at_ns,
        "raw_payload_hash": frame.raw_payload_hash,
        "archive_chunk_id": chunk_id,
        "sequence_semantics": frame.sequence_semantics,
        "authority": "ZERO",
    }
    ref = sha256_json({"artifact_type": "PublicStreamFrameIndexV1", "record_id": frame.record_id})
    repo.register_artifact(ArtifactIndexEntryV2(
        ref, "PublicStreamFrameIndexV1", sha256_json(body), received, received, body,
    ))
    return ref


def _archive_price_rows(repo, key, values):
    imported = []
    records = []
    for index, (at, close) in enumerate(values, start=1):
        payload = canonical_json({"symbol": key.native_symbol, "hour_end_ns": at, "close": str(close)}).encode()
        raw = RawObservationV2.build(
            instrument_revision=key.contract_revision,
            source_id=f"{key.venue.value}_PUBLIC_V2",
            event_type="BAR_1H",
            event_at_ns=at,
            received_at_ns=at,
            ingested_at_ns=at,
            available_at_ns=at,
            payload=payload,
            translation_version="SESSION041_S8_ARCHIVE_FIXTURE_V1",
            sequence=f"{key.native_symbol}:{at}",
        )
        bar = CausalBarV2(
            raw,
            BarIntervalV2.H1,
            at - HOUR_NS,
            at,
            close,
            close * Decimal("1.001"),
            close * Decimal("0.999"),
            close,
            Decimal("100"),
            True,
        )
        imported.append(ImportedObservationV2(index, raw, payload, bar))
        records.append((key, raw, bar))
    chunk_id = sha256_json(
        {"version": "SESSION041_S8_ARCHIVE_FIXTURE_V1", "bar_refs": [bar.content_hash for _, _, bar in records]}
    )
    ParquetObservationArchiveV2(Path(repo.path).parent / "ops-observations").write_observation_chunk(
        chunk_id, tuple(imported)
    )
    prices = []
    entries = []
    for item_key, raw, bar in records:
        ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": raw.record_id})
        wire = raw.to_dict()
        metadata = {name: wire[name] for name in (
            "record_id", "source_id", "event_type", "instrument_revision", "event_at_ns",
            "published_at_ns", "translation_version", "revision_of", "quality_flags",
            "availability_class", "replay_available_at_ns", "raw_payload_hash",
        )}
        metadata.update({
            "instrument_key_json": item_key.to_canonical_json(),
            "archive_chunk_id": chunk_id,
            "bar_content_hash": bar.content_hash,
        })
        entries.append(ArtifactIndexEntryV2(
            ref, "PublicObservationIndexV2", raw.content_hash,
            raw.received_at_ns, raw.available_at_ns, metadata,
        ))
        prices.append(S8HourlyPriceV2(item_key, bar.close_at_ns, bar.raw.available_at_ns, bar.close, ref))
    repo.register_artifacts(tuple(entries))
    return tuple(prices)


def _case(repo):
    ka, kb = _key("BTCUSDT", "BTC"), _key("ETHUSDT", "ETH")
    pair = S8PairDefinitionV2("BTC-ETH", "explicit same-venue linear pair", ka, kb,
        "OLS_LOG_PRICE_A_ON_LOG_PRICE_B_30D_HOURLY_V1", "LOG_A_MINUS_ALPHA_MINUS_BETA_LOG_B")
    values_a, values_b = [], []
    residual = 0.0
    for i in range(721):
        at = START + i * HOUR_NS
        log_b = 4.0 + 0.0004 * i + 0.002 * math.sin(i / 9)
        residual = 0.004 * math.sin(i * 0.41)
        if i == 720:
            residual = 0.08
        log_a = 1.3 + 0.85 * log_b + residual
        values_a.append((at, Decimal(str(math.exp(log_a)))))
        values_b.append((at, Decimal(str(math.exp(log_b)))))
    prices_a = list(_archive_price_rows(repo, ka, values_a))
    prices_b = list(_archive_price_rows(repo, kb, values_b))
    book_a = _artifact(repo, "S8BasketBookExecutionEvidenceV1", "a", START)
    book_b = _artifact(repo, "S8BasketBookExecutionEvidenceV1", "b", START)
    fee_a = _artifact(repo, "S8BasketFeeEvidenceV1", "a", START)
    fee_b = _artifact(repo, "S8BasketFeeEvidenceV1", "b", START)
    fund_a = _artifact(repo, "S8BasketFundingEvidenceV1", "a", START)
    fund_b = _artifact(repo, "S8BasketFundingEvidenceV1", "b", START)
    ev_a = S8LegEvidenceV2(ka, tuple(sorted(p.source_ref for p in prices_a)), (book_a,), fee_a,
        (fund_a,), ("partial-fill-recorded",), ("sequential-delay-recorded",), ("orphan-risk-recorded",), START)
    ev_b = S8LegEvidenceV2(kb, tuple(sorted(p.source_ref for p in prices_b)), (book_b,), fee_b,
        (fund_b,), ("partial-fill-recorded",), ("sequential-delay-recorded",), ("orphan-risk-recorded",), START)
    forecast = build_research_basket_forecast(pair, prices_a=prices_a, prices_b=prices_b,
        cutoff_ns=prices_a[-1].hour_end_ns, leg_a_evidence=ev_a, leg_b_evidence=ev_b)
    ref = forecast.content_hash
    repo.register_artifact(ArtifactIndexEntryV2(ref, "ResearchBasketForecastV2", ref,
        forecast.information_cutoff_ns, forecast.information_cutoff_ns, {"basket": forecast.to_dict()}))
    return pair, forecast, ref, prices_a, prices_b


def test_residual_diagnostics_are_bound_to_frozen_cutoff_and_zero_authority(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        pair, forecast, _, a, b = _case(repo)
        result = diagnose_s8_residuals(pair, forecast, prices_a=a, prices_b=b,
            published_at_ns=forecast.information_cutoff_ns + 1)
        assert result.half_life["cutoff_ns"] == forecast.information_cutoff_ns
        assert result.stationarity["cutoff_ns"] == forecast.information_cutoff_ns
        assert result.half_life["method"] == "RESIDUAL_AR1_HALF_LIFE"
        assert result.stationarity["method"] == "ADF"
        assert result.to_dict()["capital_authority"] == "ZERO"
        assert result.stationarity["inference"] == "UNCALIBRATED"


def test_residual_diagnostics_export_preserves_all_synchronized_lineage(tmp_path):
    from atlas.v2.chronology import record_computation
    from atlas.v2.science.broad_export import project_broad_evidence

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        pair, forecast, forecast_ref, prices_a, prices_b = _case(repo)
        repo.register_artifact(ArtifactIndexEntryV2(pair.content_hash, "S8PairDefinitionV2",
            pair.content_hash, forecast.information_cutoff_ns, forecast.information_cutoff_ns,
            {"pair": pair.to_dict()}))
        diagnostic = diagnose_s8_residuals(pair, forecast, prices_a=prices_a, prices_b=prices_b,
            published_at_ns=forecast.information_cutoff_ns + 1)
        repo.register_artifact(ArtifactIndexEntryV2(diagnostic.content_hash,
            "S8ResidualDiagnosticsV1", diagnostic.content_hash, diagnostic.published_at_ns,
            diagnostic.published_at_ns, {"diagnostics": diagnostic.to_dict()}))
        source_refs = [ref for pair_refs in forecast.synchronized_price_refs for ref in pair_refs]
        record_computation(repo, artifact_ref=diagnostic.content_hash,
            information_cutoff_ns=forecast.information_cutoff_ns,
            started_ns=diagnostic.published_at_ns, finished_ns=diagnostic.published_at_ns,
            available_ns=diagnostic.published_at_ns,
            input_refs=(*source_refs, forecast_ref, pair.content_hash),
            deadline_ns=diagnostic.published_at_ns)
        entry = repo.get_artifact(diagnostic.content_hash)
        assert entry is not None
        exported = project_broad_evidence(repo, entry)
        assert '"synchronized_price_refs"' in exported["evidence_payload_json"]


def test_due_outcome_without_after_cutoff_evidence_is_explicit_not_estimable(tmp_path):
    from atlas.v2.chronology import record_computation
    from atlas.v2.science.broad_export import project_broad_evidence

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        pair, forecast, ref, _, _ = _case(repo)
        enqueue_s8_basket_outcome(repo, forecast, forecast_ref=ref,
            published_at_ns=forecast.information_cutoff_ns)
        producer = S8ResearchBasketOutcomeProducerV1(clock_ns=lambda: forecast.decision_at_ns + 4 * HOUR_NS + 1)
        result = producer.run_cycle(repo, evidence_cutoff_ns=forecast.decision_at_ns + 4 * HOUR_NS,
            evidence_loader=lambda *_: None)
        assert result["outcomes_written"] == 1, result["diagnostics"]
        items = repo.latest_artifact_entries("S8BasketOutcomeV1",
            as_of_ns=forecast.decision_at_ns + 4 * HOUR_NS + 1, limit=2)
        assert len(items.entries) == 1
        outcome = items.entries[0].metadata["outcome"]
        assert outcome["outcome_status"] == "NOT_ESTIMABLE"
        assert "POST_CUTOFF_SYNCHRONIZED_PRICES_BOOKS_FEES_FUNDING_AND_BOTH_LEG_FILLS_MISSING" in outcome["reason_codes"]
        assert outcome["trade_plan_allowed"] is False
        assert outcome["evidence_cutoff_ns"] == forecast.decision_at_ns + 4 * HOUR_NS
        exported = project_broad_evidence(repo, items.entries[0])
        assert '"outcome_status":"NOT_ESTIMABLE"' in exported["evidence_payload_json"]
        forged_body = dict(outcome) | {"source_refs": [forecast.pair_definition_ref]}
        forged_ref = sha256_json(forged_body)
        forged = ArtifactIndexEntryV2(
            forged_ref, "S8BasketOutcomeV1", forged_ref, items.entries[0].available_at_ns,
            items.entries[0].available_at_ns, {"outcome": forged_body},
        )
        repo.register_artifact(forged)
        repo.register_artifact(ArtifactIndexEntryV2(
            forecast.pair_definition_ref, "S8PairDefinitionV2", forecast.pair_definition_ref,
            forecast.information_cutoff_ns, forecast.information_cutoff_ns,
            {"pair": pair.to_dict()},
        ))
        record_computation(repo, artifact_ref=forged_ref,
            information_cutoff_ns=forged_body["evidence_cutoff_ns"],
            started_ns=forged.available_at_ns, finished_ns=forged.available_at_ns,
            available_ns=forged.available_at_ns, input_refs=(ref, forecast.pair_definition_ref),
            deadline_ns=forged.available_at_ns)
        with pytest.raises(ValueError, match="deterministic replay"):
            project_broad_evidence(repo, forged)


def test_complete_observed_path_keeps_economic_result_not_estimable(tmp_path):
    from atlas.v2.science.broad_export import project_broad_evidence

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _, forecast, ref, a, b = _case(repo)
        enqueue_s8_basket_outcome(repo, forecast, forecast_ref=ref,
            published_at_ns=forecast.information_cutoff_ns)
        cutoff = forecast.decision_at_ns + 4 * HOUR_NS
        future_a, future_b = [a[-1]], [b[-1]]
        values_a, values_b = [], []
        for i in range(1, 5):
            at = forecast.decision_at_ns + i * HOUR_NS
            values_a.append((at, a[-1].close * Decimal("1.001")))
            values_b.append((at, b[-1].close * Decimal("1.0005")))
            quote_a = _stream_quote_index(repo, a[-1].instrument_key, forecast.decision_at_ns + 1)
            quote_b = _quote_index(repo, b[-1].instrument_key, forecast.decision_at_ns + 2, "bid-ask-b")
            book_a = _typed_artifact(repo, "S8BasketBookExecutionEvidenceV1", "book_execution",
                {"instrument_key": a[-1].instrument_key.to_dict(), "observed_at_ns": forecast.decision_at_ns + 1,
                 "quote_source_refs": [quote_a], "simulated_fill_state": "FULL_FILL"},
                forecast.decision_at_ns + 1)
            book_b = _typed_artifact(repo, "S8BasketBookExecutionEvidenceV1", "book_execution",
                {"instrument_key": b[-1].instrument_key.to_dict(), "observed_at_ns": forecast.decision_at_ns + 2,
                 "quote_source_refs": [quote_b], "simulated_fill_state": "FULL_FILL"},
                forecast.decision_at_ns + 2)
            fund_a = _typed_artifact(repo, "S8BasketFundingEvidenceV1", "funding",
                {"instrument_key": a[-1].instrument_key.to_dict(), "at_ns": cutoff,
                 "available_at_ns": cutoff, "cashflow": "0"}, cutoff)
            fund_b = _typed_artifact(repo, "S8BasketFundingEvidenceV1", "funding",
                {"instrument_key": b[-1].instrument_key.to_dict(), "at_ns": cutoff,
                 "available_at_ns": cutoff, "cashflow": "0"}, cutoff)
            fee_a = _typed_artifact(repo, "S8BasketFeeEvidenceV1", "fee",
                {"instrument_key": a[-1].instrument_key.to_dict(), "available_at_ns": forecast.decision_at_ns + 1,
                 "fee_policy": {"paid": "0.01"}}, forecast.decision_at_ns + 1)
            fee_b = _typed_artifact(repo, "S8BasketFeeEvidenceV1", "fee",
                {"instrument_key": b[-1].instrument_key.to_dict(), "available_at_ns": forecast.decision_at_ns + 2,
                 "fee_policy": {"paid": "0.02"}}, forecast.decision_at_ns + 2)
        future_a.extend(_archive_price_rows(repo, a[-1].instrument_key, values_a))
        future_b.extend(_archive_price_rows(repo, b[-1].instrument_key, values_b))
        evidence = S8BasketOutcomeEvidenceV1(ref, tuple(future_a), tuple(future_b), (book_a,), (book_b,),
            fee_a, fee_b, Decimal("0.01"), Decimal("0.02"), (fund_a,), (fund_b,),
            Decimal("0.00"), Decimal("0.00"), "FULL_FILL", "FULL_FILL", (1_000_000, 2_000_000),
            "BOTH_LEGS_FILLED", cutoff)
        for altered, message in (
            (replace(evidence, leg_a_fill_state="NO_FILL"), "fill state conflicts"),
            (replace(evidence, leg_a_fee_value=Decimal("9")), "fee value conflicts"),
            (replace(evidence, leg_a_funding_cashflow=Decimal("0.5")), "funding cashflow conflicts"),
        ):
            with pytest.raises(ValueError, match=message):
                _make_outcome(repo, forecast, ref, altered, cutoff, lambda: cutoff + 10)
        producer = S8ResearchBasketOutcomeProducerV1(clock_ns=lambda: cutoff + 10)
        result = producer.run_cycle(repo, evidence_cutoff_ns=cutoff,
            evidence_loader=lambda *_: evidence)
        assert result["outcomes_written"] == 1, result["diagnostics"]
        entries = repo.latest_artifact_entries("S8BasketOutcomeV1", as_of_ns=cutoff + 10, limit=2).entries
        outcome = entries[0].metadata["outcome"]
        assert outcome["outcome_status"] == "OBSERVED_RESEARCH_PATH"
        assert outcome["economic_status"] == "NOT_ESTIMABLE"
        assert outcome["simulation"]["beta_refit_during_path"] is False
        assert outcome["simulation"]["trade_plan_allowed"] is False
        outcome_entry = repo.get_artifact(entries[0].artifact_ref)
        assert outcome_entry is not None
        assert json.loads(project_broad_evidence(repo, outcome_entry)["evidence_payload_json"])["evidence_ref"] \
            == outcome["evidence_ref"]
        evidence_entry = repo.get_artifact(outcome["evidence_ref"])
        assert evidence_entry is not None
        assert evidence_entry.metadata["evidence"]["available_at_ns"] == cutoff
        assert evidence_entry.available_at_ns == cutoff + 10
        assert json.loads(project_broad_evidence(repo, evidence_entry)["evidence_payload_json"])["forecast_ref"] == ref


def test_book_quote_received_after_book_observation_is_rejected(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _, forecast, forecast_ref, _, _ = _case(repo)
        observed_at = forecast.decision_at_ns + 1
        quote_ref = _stream_quote_index(
            repo, forecast.leg_a_evidence.instrument_key, observed_at, received_at_ns=observed_at + 1,
        )
        book_ref = _typed_artifact(repo, "S8BasketBookExecutionEvidenceV1", "book_execution", {
            "instrument_key": forecast.leg_a_evidence.instrument_key.to_dict(),
            "observed_at_ns": observed_at,
            "quote_source_refs": [quote_ref],
            "simulated_fill_state": "NO_FILL",
        }, observed_at)
        cutoff = forecast.decision_at_ns + 4 * HOUR_NS
        evidence = S8BasketOutcomeEvidenceV1(
            forecast_ref, (), (), (book_ref,), (), None, None, None, None, (), (), None, None,
            "UNAVAILABLE", "UNAVAILABLE", (0, 0), "UNAVAILABLE", cutoff,
        )
        with pytest.raises(ValueError, match="quote reference is absent, mistyped or future"):
            _make_outcome(repo, forecast, forecast_ref, evidence, cutoff, lambda: cutoff + 1)


def test_unrelated_public_observation_cannot_substitute_for_book_quote(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _, forecast, forecast_ref, _, _ = _case(repo)
        observed_at = forecast.information_cutoff_ns + 1
        quote_ref = _quote_index(repo, forecast.leg_a_evidence.instrument_key, observed_at,
                                 "bar-is-not-a-quote", event_type="BAR_1H")
        book_ref = _typed_artifact(repo, "S8BasketBookExecutionEvidenceV1", "book_execution", {
            "instrument_key": forecast.leg_a_evidence.instrument_key.to_dict(),
            "observed_at_ns": observed_at, "quote_source_refs": [quote_ref],
            "simulated_fill_state": "NO_FILL",
        }, observed_at)
        cutoff = forecast.decision_at_ns + 4 * HOUR_NS
        evidence = S8BasketOutcomeEvidenceV1(
            forecast_ref, (), (), (book_ref,), (), None, None, None, None, (), (), None, None,
            "NO_FILL", "UNAVAILABLE", (0, 0), "UNAVAILABLE", cutoff,
        )
        with pytest.raises(ValueError, match="approved best-bid/ask source"):
            _make_outcome(repo, forecast, forecast_ref, evidence, cutoff, lambda: cutoff + 1)


@pytest.mark.parametrize("kind", ["book", "funding", "fee"])
def test_typed_evidence_body_cannot_postdate_its_index_availability(tmp_path, kind):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _, forecast, forecast_ref, _, _ = _case(repo)
        observed_at = forecast.information_cutoff_ns + 100
        cutoff = forecast.decision_at_ns + 4 * HOUR_NS
        book_refs_a = funding_refs_a = ()
        fee_ref_a = None
        fee_value_a = funding_value_a = None
        fill_a = "UNAVAILABLE"
        if kind == "book":
            quote_ref = _quote_index(repo, forecast.leg_a_evidence.instrument_key,
                                     observed_at, "valid-quote")
            body = {"instrument_key": forecast.leg_a_evidence.instrument_key.to_dict(),
                    "observed_at_ns": observed_at, "quote_source_refs": [quote_ref],
                    "simulated_fill_state": "NO_FILL"}
            ref = sha256_json(body)
            repo.register_artifact(ArtifactIndexEntryV2(ref, "S8BasketBookExecutionEvidenceV1", ref,
                observed_at - 1, observed_at - 1, {"book_execution": body}))
            book_refs_a, fill_a = (ref,), "NO_FILL"
        elif kind == "funding":
            body = {"instrument_key": forecast.leg_a_evidence.instrument_key.to_dict(),
                    "at_ns": observed_at, "available_at_ns": observed_at, "cashflow": "0"}
            ref = sha256_json(body)
            repo.register_artifact(ArtifactIndexEntryV2(ref, "S8BasketFundingEvidenceV1", ref,
                observed_at - 1, observed_at - 1, {"funding": body}))
            funding_refs_a, funding_value_a = (ref,), Decimal("0")
        else:
            body = {"instrument_key": forecast.leg_a_evidence.instrument_key.to_dict(),
                    "available_at_ns": observed_at, "fee_policy": {"paid": "0.01"}}
            ref = sha256_json(body)
            repo.register_artifact(ArtifactIndexEntryV2(ref, "S8BasketFeeEvidenceV1", ref,
                observed_at - 1, observed_at - 1, {"fee": body}))
            fee_ref_a, fee_value_a = ref, Decimal("0.01")
        evidence = S8BasketOutcomeEvidenceV1(
            forecast_ref, (), (), book_refs_a, (), fee_ref_a, None, fee_value_a, None,
            funding_refs_a, (), funding_value_a, None, fill_a, "UNAVAILABLE", (0, 0),
            "UNAVAILABLE", cutoff,
        )
        expected = {"book": "typed book execution", "funding": "typed settlement", "fee": "typed fee"}[kind]
        with pytest.raises(ValueError, match=expected):
            _make_outcome(repo, forecast, forecast_ref, evidence, cutoff, lambda: cutoff + 1)


def test_s8_typed_source_reference_cannot_fill_multiple_evidence_roles(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _, forecast, forecast_ref, _, _ = _case(repo)
        cutoff = forecast.decision_at_ns + 4 * HOUR_NS
        shared_ref = "f" * 64
        evidence = S8BasketOutcomeEvidenceV1(
            forecast_ref, (), (), (shared_ref,), (), None, None, None, None,
            (shared_ref,), (), None, None, "UNAVAILABLE", "UNAVAILABLE", (0, 0),
            "UNAVAILABLE", cutoff,
        )
        with pytest.raises(ValueError, match="conflicting evidence roles"):
            _make_outcome(repo, forecast, forecast_ref, evidence, cutoff, lambda: cutoff + 1)


def test_matured_price_path_without_entry_threshold_is_durable_no_entry(tmp_path):
    from atlas.v2.science.broad_export import project_broad_evidence

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _, original, _, a, b = _case(repo)
        forecast = replace(original, current_z=0.0, current_residual=original.residual_mean)
        ref = forecast.content_hash
        repo.register_artifact(ArtifactIndexEntryV2(ref, "ResearchBasketForecastV2", ref,
            forecast.information_cutoff_ns, forecast.information_cutoff_ns, {"basket": forecast.to_dict()}))
        enqueue_s8_basket_outcome(repo, forecast, forecast_ref=ref,
            published_at_ns=forecast.information_cutoff_ns)
        cutoff = forecast.decision_at_ns + 4 * HOUR_NS
        future_a, future_b = [a[-1]], [b[-1]]
        values_a, values_b = [], []
        for i in range(1, 5):
            at = forecast.decision_at_ns + i * HOUR_NS
            values_a.append((at, a[-1].close))
            values_b.append((at, b[-1].close))
        future_a.extend(_archive_price_rows(repo, a[-1].instrument_key, values_a))
        future_b.extend(_archive_price_rows(repo, b[-1].instrument_key, values_b))
        evidence = S8BasketOutcomeEvidenceV1(ref, tuple(future_a), tuple(future_b), (), (),
            None, None, None, None, (), (), None, None, "UNAVAILABLE", "UNAVAILABLE", (0, 0),
            "UNAVAILABLE", cutoff)
        producer = S8ResearchBasketOutcomeProducerV1(clock_ns=lambda: cutoff + 5)
        result = producer.run_cycle(repo, evidence_cutoff_ns=cutoff,
            evidence_loader=lambda *_: evidence)
        assert result["outcomes_written"] == 1, result["diagnostics"]
        outcome = repo.latest_artifact_entries("S8BasketOutcomeV1", as_of_ns=cutoff + 5,
            limit=1).entries[0].metadata["outcome"]
        assert outcome["outcome_status"] == "NO_ENTRY"
        assert outcome["reason_codes"] == ("NO_ENTRY_THRESHOLD_CROSSED",)
        assert outcome["decision_at_ns"] + 4 * HOUR_NS == outcome["maturity_at_ns"]
        outcome_entry = repo.get_artifact(sha256_json(outcome))
        assert outcome_entry is not None
        assert json.loads(project_broad_evidence(repo, outcome_entry)["evidence_payload_json"])["outcome_status"] == "NO_ENTRY"
