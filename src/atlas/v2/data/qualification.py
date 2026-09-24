"""Opt-in bounded credential-free Bybit/Binance public-data qualification."""

from __future__ import annotations

import argparse
import json
import platform
import tempfile
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .._serialization import canonical_json, sha256_json
from ..contracts import OpportunityWatchV2, WatchStateV2
from ..instruments import EnvironmentV2, InstrumentKeyV2, InstrumentRegistryV2
from ..memory.repository import OpsRepository
from .bars import BarIntervalV2, CausalBarStoreV2, CausalBarV2, translate_final_bar
from .binance import (
    SOURCE_ID as BINANCE_SOURCE_ID,
)
from .binance import (
    BinanceUsdMPublicReaderV2,
    translate_agg_trades,
    translate_book_ticker,
    translate_exchange_info,
    translate_premium_index,
)
from .binance import (
    translate_funding_history as translate_binance_funding,
)
from .binance import (
    translate_kline as translate_binance_kline,
)
from .binance import (
    translate_open_interest as translate_binance_oi,
)
from .binance import (
    translate_open_interest_history as translate_binance_oi_history,
)
from .bybit import (
    SOURCE_ID as BYBIT_SOURCE_ID,
)
from .bybit import (
    BybitPublicReaderV2,
    translate_instrument_info,
    translate_recent_trades,
    translate_ticker,
)
from .bybit import (
    translate_funding_history as translate_bybit_funding,
)
from .bybit import (
    translate_kline as translate_bybit_kline,
)
from .bybit import (
    translate_open_interest_history as translate_bybit_oi_history,
)
from .collector import PublicCollectorV2
from .history import ParquetObservationArchiveV2
from .public_http import PublicDataError
from .raw import AppendStatusV2, RawObservationV2
from .universe import ComputeTierV2

H = "a" * 64
H2 = "b" * 64


def _utc(at_ns: int) -> str:
    return datetime.fromtimestamp(at_ns / 1_000_000_000, tz=UTC).isoformat()


def _bybit_rows(payload: Any, *, channel: str) -> list[Mapping[str, Any]]:
    if not isinstance(payload, Mapping) or payload.get("retCode") != 0:
        raise ValueError(f"Bybit {channel} response was unsuccessful")
    result = payload.get("result")
    rows = result.get("list") if isinstance(result, Mapping) else None
    if not isinstance(rows, list):
        raise ValueError(f"Bybit {channel} response did not contain a list")
    return [row for row in rows if isinstance(row, Mapping)]


def _binance_rows(payload: Any, *, channel: str) -> list[Mapping[str, Any]]:
    if not isinstance(payload, list):
        raise ValueError(f"Binance {channel} response was not an array")
    if any(not isinstance(row, Mapping) for row in payload):
        raise ValueError(f"Binance {channel} response contained a non-object row")
    return payload


def _rows_of_arrays(payload: Any, *, channel: str) -> list[Sequence[Any]]:
    if not isinstance(payload, list) or any(not isinstance(row, (list, tuple)) for row in payload):
        raise ValueError(f"{channel} response was not an array of arrays")
    return payload


def _count_interval_gaps(opens: Sequence[int], interval: BarIntervalV2) -> int:
    total = 0
    for prior_open, current_open in zip(opens[:-1], opens[1:], strict=True):
        difference = current_open - prior_open
        if difference <= 0 or difference % interval.duration_ns:
            total += 1
        elif difference > interval.duration_ns:
            total += difference // interval.duration_ns - 1
    return total


class _VenueRun:
    def __init__(
        self,
        *,
        venue: str,
        source_id: str,
        collector: PublicCollectorV2,
        clock_ns: Callable[[], int],
        bar_store: CausalBarStoreV2,
    ) -> None:
        self.venue = venue
        self.source_id = source_id
        self.collector = collector
        self.clock_ns = clock_ns
        self.bar_store = bar_store
        self.translated: Counter[str] = Counter()
        self.final_bars = 0
        self.duplicates = 0
        self.gap_count = 0

    def ingest(
        self,
        observation: RawObservationV2,
        *,
        payload: Any,
        raw_payload: bytes | None = None,
        bar: CausalBarV2 | None = None,
    ) -> None:
        self.translated[observation.event_type] += 1
        result = self.collector.ingest(
            observation,
            raw_payload=raw_payload if raw_payload is not None else canonical_json(payload),
            bar=bar,
        )
        if result.append.status == AppendStatusV2.DUPLICATE:
            self.duplicates += 1

    def metadata(self, response: Any, key: InstrumentKeyV2) -> None:
        observation = RawObservationV2.build(
            instrument_revision=key.contract_revision,
            source_id=self.source_id,
            event_type="INSTRUMENT_INFO",
            event_at_ns=response.received_at_ns,
            received_at_ns=response.received_at_ns,
            ingested_at_ns=response.received_at_ns,
            available_at_ns=response.received_at_ns,
            payload=response.raw_body,
            translation_version="public-qualification-metadata-v1",
        )
        self.ingest(observation, payload={"raw_sha256": sha256_json(response.raw_body.hex())}, raw_payload=response.raw_body)

    def bars(
        self,
        records: Sequence[tuple[RawObservationV2, dict[str, str], int, bool]],
        *,
        interval: BarIntervalV2,
        payload_rows: Sequence[Any],
    ) -> tuple[RawObservationV2, CausalBarV2]:
        if len(payload_rows) != len(records):
            raise ValueError("venue kline payload and translation row counts disagree")
        ordered_opens = sorted(item[2] for item in records)
        gap_count = _count_interval_gaps(ordered_opens, interval)
        self.gap_count += gap_count
        if gap_count:
            self.collector.mark_incomplete_snapshot(
                self.source_id,
                at_ns=self.clock_ns(),
                details=f"{gap_count} missing or ambiguous {interval.value} kline intervals in public snapshot",
            )
        final_candidates: list[tuple[RawObservationV2, CausalBarV2]] = []
        for (observation, values, open_at_ns, final), payload in zip(records, payload_rows, strict=True):
            bar = translate_final_bar(
                raw=observation,
                interval=interval,
                open_at_ns=open_at_ns,
                values=values,
                final=final,
            )
            self.ingest(observation, payload=payload, bar=bar)
            if final:
                if bar is not None:
                    self.bar_store.append(bar)
                    self.final_bars += 1
                    final_candidates.append((observation, bar))
        if not final_candidates:
            raise ValueError(f"{self.venue} public kline response contained no final {interval.value} bar")
        return max(final_candidates, key=lambda item: (item[0].event_at_ns or 0, item[0].record_id))


def _collect_bybit(
    run: _VenueRun,
    reader: BybitPublicReaderV2,
    registry: InstrumentRegistryV2,
    *,
    at_ns: int,
) -> tuple[InstrumentKeyV2, tuple[RawObservationV2, CausalBarV2], Any]:
    server_time = reader.server_time_ns()
    metadata_response = reader.instruments(limit=1000)
    contracts = translate_instrument_info(
        metadata_response.payload,
        environment=EnvironmentV2.MAINNET,
        observed_at_ns=metadata_response.received_at_ns,
        available_at_ns=metadata_response.received_at_ns,
    )
    product = next((item for item in contracts if item.key.native_symbol == "BTCUSDT"), None)
    if product is None:
        raise ValueError("Bybit linear metadata did not resolve BTCUSDT")
    registry.register(product)
    run.metadata(metadata_response, product.key)

    ticker_response = reader.ticker("BTCUSDT")
    ticker_rows = _bybit_rows(ticker_response.payload, channel="ticker")
    ticker_row = next(row for row in ticker_rows if row.get("symbol") == "BTCUSDT")
    run.ingest(translate_ticker(ticker_row, key=product.key, received_at_ns=ticker_response.received_at_ns), payload=ticker_row)

    kline_response = reader.klines("BTCUSDT", BarIntervalV2.M15, limit=12)
    kline_rows = _rows_of_arrays(
        (kline_response.payload.get("result", {}).get("list") if isinstance(kline_response.payload, Mapping) else None),
        channel="Bybit kline",
    )
    bars: list[tuple[RawObservationV2, dict[str, str], int, bool]] = [
        translate_bybit_kline(
            row,
            key=product.key,
            interval=BarIntervalV2.M15,
            received_at_ns=kline_response.received_at_ns,
            server_time_ns=server_time,
        )
        for row in kline_rows
    ]
    last_final = run.bars(bars, interval=BarIntervalV2.M15, payload_rows=kline_rows)

    trades_response = reader.recent_trades("BTCUSDT", limit=50)
    trades = translate_recent_trades(
        _bybit_rows(trades_response.payload, channel="recent trades"),
        key=product.key,
        received_at_ns=trades_response.received_at_ns,
    )
    for observation, row in zip(trades, _bybit_rows(trades_response.payload, channel="recent trades"), strict=True):
        run.ingest(observation, payload=row)

    funding_response = reader.funding_history("BTCUSDT", limit=10)
    funding_rows = _bybit_rows(funding_response.payload, channel="funding history")
    for observation, row in zip(
        translate_bybit_funding(funding_rows, key=product.key, received_at_ns=funding_response.received_at_ns),
        funding_rows,
        strict=True,
    ):
        run.ingest(observation, payload=row)

    oi_response = reader.open_interest_history("BTCUSDT", interval="5min", limit=10)
    oi_rows = _bybit_rows(oi_response.payload, channel="open interest history")
    for observation, row in zip(
        translate_bybit_oi_history(oi_rows, key=product.key, received_at_ns=oi_response.received_at_ns),
        oi_rows,
        strict=True,
    ):
        run.ingest(observation, payload=row)
    return product.key, last_final, metadata_response


def _collect_binance(
    run: _VenueRun,
    reader: BinanceUsdMPublicReaderV2,
    registry: InstrumentRegistryV2,
    *,
    at_ns: int,
) -> tuple[InstrumentKeyV2, tuple[RawObservationV2, CausalBarV2], Any]:
    server_time = reader.server_time_ns()
    metadata_response = reader.exchange_info()
    contracts = translate_exchange_info(
        metadata_response.payload,
        environment=EnvironmentV2.MAINNET,
        observed_at_ns=metadata_response.received_at_ns,
        available_at_ns=metadata_response.received_at_ns,
    )
    product = next((item for item in contracts if item.key.native_symbol == "BTCUSDT"), None)
    if product is None:
        raise ValueError("Binance USD-M metadata did not resolve BTCUSDT")
    registry.register(product)
    run.metadata(metadata_response, product.key)

    book_response = reader.book_ticker("BTCUSDT")
    run.ingest(translate_book_ticker(book_response.payload, key=product.key, received_at_ns=book_response.received_at_ns), payload=book_response.payload)
    premium_response = reader.premium_index("BTCUSDT")
    run.ingest(translate_premium_index(premium_response.payload, key=product.key, received_at_ns=premium_response.received_at_ns), payload=premium_response.payload)

    kline_response = reader.klines("BTCUSDT", BarIntervalV2.M15, limit=12)
    klines = _rows_of_arrays(kline_response.payload, channel="Binance kline")
    bars: list[tuple[RawObservationV2, dict[str, str], int, bool]] = [
        translate_binance_kline(
            row,
            key=product.key,
            interval=BarIntervalV2.M15,
            received_at_ns=kline_response.received_at_ns,
            server_time_ns=server_time,
        )
        for row in klines
    ]
    last_final = run.bars(bars, interval=BarIntervalV2.M15, payload_rows=klines)

    trades_response = reader.aggregate_trades("BTCUSDT", limit=50)
    trades_rows = _binance_rows(trades_response.payload, channel="aggregate trades")
    for observation, row in zip(
        translate_agg_trades(trades_rows, key=product.key, received_at_ns=trades_response.received_at_ns),
        trades_rows,
        strict=True,
    ):
        run.ingest(observation, payload=row)

    funding_response = reader.funding_history("BTCUSDT", limit=10)
    funding_rows = _binance_rows(funding_response.payload, channel="funding history")
    for observation, row in zip(
        translate_binance_funding(funding_rows, key=product.key, received_at_ns=funding_response.received_at_ns),
        funding_rows,
        strict=True,
    ):
        run.ingest(observation, payload=row)

    oi_response = reader.open_interest("BTCUSDT")
    run.ingest(translate_binance_oi(oi_response.payload, key=product.key, received_at_ns=oi_response.received_at_ns), payload=oi_response.payload)
    oi_hist_response = reader.open_interest_history("BTCUSDT", period="5m", limit=10)
    oi_rows = _binance_rows(oi_hist_response.payload, channel="open interest history")
    for observation, row in zip(
        translate_binance_oi_history(oi_rows, key=product.key, received_at_ns=oi_hist_response.received_at_ns),
        oi_rows,
        strict=True,
    ):
        run.ingest(observation, payload=row)
    return product.key, last_final, metadata_response


def run_short_public_qualification(
    output_path: str | Path,
    *,
    clock_ns: Callable[[], int] = time.time_ns,
) -> dict[str, Any]:
    """Collect and translate a small public sample; evidence contains no market payloads."""
    started = clock_ns()
    registry = InstrumentRegistryV2()
    bar_store = CausalBarStoreV2()
    evidence: dict[str, Any] = {
        "schema_version": 1,
        "run_kind": "ATLAS_V2_SESSION_012_SHORT_PUBLIC_QUALIFICATION",
        "started_at_utc": _utc(started),
        "environment": {"python": platform.python_version(), "platform": platform.system(), "credential_mode": "PUBLIC_ONLY"},
        "venues": {},
        "reconnect_mechanism": "REST reconnect state transition plus a second overlapping final-kline request",
        "conflicts_observed": 0,
        "gaps_observed": 0,
        "anomalies": [],
        "artifact_count": 0,
        "final_bar_count": 0,
        "watch_resubscription_verified": False,
        "point_in_time_mapping_verified": False,
        "network_status": "TEST GATE",
    }
    with tempfile.TemporaryDirectory(prefix="atlas-session012-public-") as temporary:
        repository_path = Path(temporary) / "ops.sqlite"
        repository = OpsRepository(repository_path)
        archive = ParquetObservationArchiveV2(Path(temporary) / "observations")
        collector = PublicCollectorV2(repository=repository, registry=registry, clock_ns=clock_ns, archive=archive)
        outcomes: dict[str, tuple[InstrumentKeyV2, tuple[RawObservationV2, CausalBarV2], Any, _VenueRun]] = {}
        venue_inputs: tuple[tuple[str, str, Any, Any], ...] = (
            ("BYBIT", BYBIT_SOURCE_ID, BybitPublicReaderV2(), _collect_bybit),
            ("BINANCE_USDM", BINANCE_SOURCE_ID, BinanceUsdMPublicReaderV2(), _collect_binance),
        )
        for venue_name, source_id, reader, collect in venue_inputs:
            venue_run = _VenueRun(venue=venue_name, source_id=source_id, collector=collector,
                                  clock_ns=clock_ns, bar_store=bar_store)
            venue_evidence: dict[str, Any] = {"symbols": ["BTCUSDT"], "channels": [], "status": "TEST GATE"}
            try:
                key, last_bar, metadata_response = collect(venue_run, reader, registry, at_ns=clock_ns())
                venue_evidence["channels"] = sorted(venue_run.translated)
                venue_evidence["translated_artifacts"] = int(sum(venue_run.translated.values()))
                venue_evidence["final_bars"] = venue_run.final_bars
                venue_evidence["kline_gap_count"] = venue_run.gap_count
                venue_evidence["metadata_revision"] = key.contract_revision
                venue_evidence["metadata_received_at_ns"] = metadata_response.received_at_ns
                venue_evidence["status"] = "TESTED"
                outcomes[venue_name] = (key, last_bar, reader, venue_run)
            except Exception as exc:
                # Error type only: public payload/error bodies never enter the durable report.
                venue_evidence["failure_type"] = type(exc).__name__
                venue_evidence["status"] = "BLOCKED BY ENVIRONMENT"
                if isinstance(exc, PublicDataError) and exc.rate_limited:
                    collector.on_rate_limited(source_id, at_ns=clock_ns())
                elif isinstance(exc, PublicDataError):
                    collector.on_disconnect(source_id, at_ns=clock_ns())
                else:
                    collector.reconcile_after_reconnect(
                        source_id, at_ns=clock_ns(), complete_snapshot=False, missed_interval_repaired=False
                    )
            evidence["venues"][venue_name] = venue_evidence

        if len(outcomes) == 2:
            mapping_cutoff = clock_ns()
            evidence["point_in_time_mapping_verified"] = (
                outcomes["BYBIT"][0] != outcomes["BINANCE_USDM"][0]
                and all(registry.resolve_as_of(
                    key, decision_slot_ns=mapping_cutoff, information_cutoff_ns=mapping_cutoff
                ) is not None
                        for key, _, _, _ in outcomes.values())
            )
            duplicate_statuses: list[str] = []
            reconnect_statuses: dict[str, str] = {}
            for venue_name, (key, prior_bar, reader, venue_run) in outcomes.items():
                prior_raw, prior_causal_bar = prior_bar
                now = clock_ns()
                collector.on_disconnect(venue_run.source_id, at_ns=now)
                collector.begin_reconnect(venue_run.source_id, attempt=0, at_ns=now + 1)
                collector.reconnected(venue_run.source_id, at_ns=now + 2)
                # Re-read an overlap window after the disconnect state transition.
                try:
                    overlap = reader.klines("BTCUSDT", BarIntervalV2.M15, limit=12)
                    server_time = reader.server_time_ns()
                    if venue_name == "BYBIT":
                        rows = _rows_of_arrays(
                            overlap.payload.get("result", {}).get("list") if isinstance(overlap.payload, Mapping) else None,
                            channel="Bybit reconnect overlap",
                        )
                        translated: list[tuple[RawObservationV2, dict[str, str], int, bool]] = [translate_bybit_kline(
                            row, key=key, interval=BarIntervalV2.M15, received_at_ns=overlap.received_at_ns,
                            server_time_ns=server_time,
                        ) for row in rows]
                    else:
                        rows = _rows_of_arrays(overlap.payload, channel="Binance reconnect overlap")
                        translated = [translate_binance_kline(
                            row, key=key, interval=BarIntervalV2.M15, received_at_ns=overlap.received_at_ns,
                            server_time_ns=server_time,
                        ) for row in rows]
                    matched_index = next(
                        (index for index, item in enumerate(translated) if item[0].record_id == prior_raw.record_id), None
                    )
                    matched = translated[matched_index] if matched_index is not None else None
                    if matched is None:
                        raise ValueError("overlap did not include the last finalized bar")
                    matched_payload = rows[matched_index]  # type: ignore[index]
                    matched_raw, matched_values, matched_open, matched_final = matched
                    matched_bar = translate_final_bar(
                        raw=matched_raw,
                        interval=BarIntervalV2.M15,
                        open_at_ns=matched_open,
                        values=matched_values,
                        final=matched_final,
                    )
                    if matched_bar is None or matched_bar.content_hash != prior_causal_bar.content_hash:
                        raise ValueError("overlap finalized bar content conflicts with the archived version")
                    duplicate = collector.ingest(
                        matched_raw,
                        raw_payload=canonical_json(matched_payload),
                        bar=matched_bar,
                    )
                    duplicate_statuses.append(duplicate.append.status.value)
                    overlap_records = translated
                    overlap_opens = sorted(item[2] for item in overlap_records)
                    overlap_gap_count = _count_interval_gaps(overlap_opens, BarIntervalV2.M15)
                    evidence["gaps_observed"] += overlap_gap_count
                    overlap_verified = (
                        duplicate.append.status != AppendStatusV2.CONFLICT_QUARANTINED
                        and overlap_gap_count == 0
                    )
                    collector.reconcile_after_reconnect(
                        venue_run.source_id,
                        at_ns=clock_ns(),
                        complete_snapshot=overlap_verified,
                        missed_interval_repaired=overlap_verified,
                    )
                    reconnect_statuses[venue_name] = "TESTED" if overlap_verified else "TEST GATE"
                except Exception as exc:
                    evidence["anomalies"].append({"venue": venue_name, "stage": "reconnect_overlap", "failure_type": type(exc).__name__})
                    collector.reconcile_after_reconnect(
                        venue_run.source_id,
                        at_ns=max(clock_ns(), now + 3),
                        complete_snapshot=False,
                        missed_interval_repaired=False,
                    )
                    reconnect_statuses[venue_name] = "BLOCKED BY ENVIRONMENT"
            evidence["reconnect_health_transitions"] = {
                venue: [item.state.value for item in collector.health.history(source)]
                for venue, source in (("BYBIT", BYBIT_SOURCE_ID), ("BINANCE_USDM", BINANCE_SOURCE_ID))
            }
            evidence["overlap_duplicate_statuses"] = duplicate_statuses
            evidence["reconnect_statuses"] = reconnect_statuses
            evidence["conflicts_observed"] = len(collector.quarantined_conflicts())

            watches: list[OpportunityWatchV2] = []
            for venue, (key, _, _, _) in outcomes.items():
                watch_id = f"session012-{venue.lower()}-bbo"
                watches.append(
                    OpportunityWatchV2(
                        watch_id, key, "qualification", "1", H, WatchStateV2.DETECTED, 0,
                        started, started, H2, (), "BOOK_TICKER", started + 7 * 86_400 * 1_000_000_000, started,
                    )
                )
                repository.create_watch(watches[-1])
            before = collector.restore_subscriptions(
                {key: ComputeTierV2.TIER_0 for key, _, _, _ in outcomes.values()}, now_ns=clock_ns()
            )
            collector.flush_archive()
            collector.checkpoint_cursors(at_ns=clock_ns())
            repository.close()
            reopened = OpsRepository(repository_path)
            restarted = PublicCollectorV2(repository=reopened, registry=registry, clock_ns=clock_ns, archive=archive)
            after = restarted.restore_subscriptions({}, now_ns=clock_ns())
            restart_overlap_statuses: list[str] = []
            restart_reconnect_statuses: dict[str, str] = {}
            for venue_name, (key, prior_bar, reader, _) in outcomes.items():
                prior_raw, prior_causal_bar = prior_bar
                try:
                    overlap = reader.klines("BTCUSDT", BarIntervalV2.M15, limit=12)
                    server_time = reader.server_time_ns()
                    if venue_name == "BYBIT":
                        rows = _rows_of_arrays(
                            overlap.payload.get("result", {}).get("list") if isinstance(overlap.payload, Mapping) else None,
                            channel="Bybit restart overlap",
                        )
                        translated = [translate_bybit_kline(
                            row, key=key, interval=BarIntervalV2.M15, received_at_ns=overlap.received_at_ns,
                            server_time_ns=server_time,
                        ) for row in rows]
                    else:
                        rows = _rows_of_arrays(overlap.payload, channel="Binance restart overlap")
                        translated = [translate_binance_kline(
                            row, key=key, interval=BarIntervalV2.M15, received_at_ns=overlap.received_at_ns,
                            server_time_ns=server_time,
                        ) for row in rows]
                    matched_index = next(
                        (index for index, item in enumerate(translated) if item[0].record_id == prior_raw.record_id), None
                    )
                    matched = translated[matched_index] if matched_index is not None else None
                    if matched is None:
                        raise ValueError("restart overlap did not include the last finalized bar")
                    matched_payload = rows[matched_index]  # type: ignore[index]
                    matched_raw, matched_values, matched_open, matched_final = matched
                    matched_bar = translate_final_bar(
                        raw=matched_raw, interval=BarIntervalV2.M15, open_at_ns=matched_open,
                        values=matched_values, final=matched_final,
                    )
                    if matched_bar is None or matched_bar.content_hash != prior_causal_bar.content_hash:
                        raise ValueError("restart overlap content conflicts with the archived final bar")
                    duplicate = restarted.ingest(
                        matched_raw, raw_payload=canonical_json(matched_payload), bar=matched_bar
                    )
                    restart_overlap_statuses.append(duplicate.append.status.value)
                    overlap_opens = sorted(item[2] for item in translated)
                    overlap_gap_count = _count_interval_gaps(overlap_opens, BarIntervalV2.M15)
                    evidence["gaps_observed"] += overlap_gap_count
                    verified = duplicate.append.status == AppendStatusV2.DUPLICATE and overlap_gap_count == 0
                    restarted.reconcile_after_reconnect(
                        outcomes[venue_name][3].source_id,
                        at_ns=clock_ns(),
                        complete_snapshot=verified,
                        missed_interval_repaired=verified,
                    )
                    restart_reconnect_statuses[venue_name] = "TESTED" if verified else "TEST GATE"
                except Exception as exc:
                    evidence["anomalies"].append({"venue": venue_name, "stage": "restart_overlap", "failure_type": type(exc).__name__})
                    restart_reconnect_statuses[venue_name] = "BLOCKED BY ENVIRONMENT"
            evidence["watch_resubscription_verified"] = (
                len(after.watches.active_watches) == 2
                and before.subscriptions.plan_id == after.subscriptions.plan_id
                and all(any(channel.value == "BOOK_TICKER" for channel in spec.channels)
                        for spec in after.subscriptions.specs)
            )
            evidence["restart_overlap_duplicate_statuses"] = restart_overlap_statuses
            evidence["restart_reconnect_statuses"] = restart_reconnect_statuses
            evidence["restart_health_transitions"] = {
                venue: [item.state.value for item in restarted.health.history(source)]
                for venue, source in (("BYBIT", BYBIT_SOURCE_ID), ("BINANCE_USDM", BINANCE_SOURCE_ID))
            }
            evidence["artifact_count"] = sum(sum(run.translated.values()) for _, _, _, run in outcomes.values())
            evidence["final_bar_count"] = sum(run.final_bars for _, _, _, run in outcomes.values())
            evidence["gaps_observed"] += sum(run.gap_count for _, _, _, run in outcomes.values())
            for venue_name, (_, _, _, run) in outcomes.items():
                if run.gap_count:
                    evidence["anomalies"].append(
                        {"venue": venue_name, "stage": "initial_kline_snapshot", "gap_count": run.gap_count}
                    )
            evidence["network_status"] = (
                "TESTED" if evidence["point_in_time_mapping_verified"] and evidence["watch_resubscription_verified"]
                and all(status == AppendStatusV2.DUPLICATE.value for status in duplicate_statuses)
                and all(status == "TESTED" for status in reconnect_statuses.values())
                and all(status == "TESTED" for status in restart_reconnect_statuses.values())
                and all(status == AppendStatusV2.DUPLICATE.value for status in restart_overlap_statuses)
                and evidence["conflicts_observed"] == 0
                and evidence["gaps_observed"] == 0
                and evidence["final_bar_count"] >= 2 else "TEST GATE"
            )
            reopened.close()
        else:
            repository.close()

        evidence["source_health_transitions"] = {
            venue: [item.state.value for item in collector.health.history(source)]
            for venue, source in (("BYBIT", BYBIT_SOURCE_ID), ("BINANCE_USDM", BINANCE_SOURCE_ID))
        }

    evidence["ended_at_utc"] = _utc(clock_ns())
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    temporary_path = Path(output_path).with_suffix(Path(output_path).suffix + ".tmp")
    temporary_path.write_text(json.dumps(evidence, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    temporary_path.replace(output_path)
    return evidence


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, help="sanitized JSON evidence output path")
    arguments = parser.parse_args(argv)
    evidence = run_short_public_qualification(arguments.output)
    print(json.dumps({key: evidence[key] for key in ("started_at_utc", "ended_at_utc", "network_status", "venues")}, sort_keys=True))
    return 0 if evidence["network_status"] == "TESTED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
