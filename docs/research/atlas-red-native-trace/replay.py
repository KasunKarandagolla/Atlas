"""Bounded offline integration replay; only already revealed, verified sources.

This loader has file access. Only cutoff-filtered immutable JoinedBars values
reach native_research_trace. No outcome file or model/provider API is imported.
"""

# ruff: noqa: E402
from __future__ import annotations

import argparse
import bisect
import gzip
import json
import pathlib
import resource
import sys
import time
from decimal import Decimal

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parents[2]
PRIOR = HERE.parent / "atlas-red-historical-blind-ml"
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(PRIOR))
import common

# isort: split
import causal_replay as sources

# isort: split
from verify_sol_sources import verify_member

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.raw import AvailabilityClassV2, RawObservationV2
from atlas.v2.features.joins import JoinedBars
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.runtime.native_research_trace import (
    TraceWriter,
    check_prefix,
    make_slot,
    observe_components,
    observe_coordinators,
    observe_prices,
)


def file_sha(path):
    return common.sha(path)


def source_manifest(old_csv, old_zip, new_cache, symbols):
    records = []
    for row, location in sources.source_rows():
        if row.get("symbol") not in symbols or row.get("month") not in [f"2025-{m:02}" for m in range(1, 8)]:
            continue
        if row["interval"] != "1h" and not (row["interval"] == "15m" and row["month"] in ("2025-06", "2025-07")):
            continue
        if row.get("status") != "VERIFIED":
            records.append(
                {
                    "symbol": row["symbol"],
                    "interval": row["interval"],
                    "month": row["month"],
                    "status": row["status"],
                    "url": row["url"],
                }
            )
            continue
        zip_root = old_zip if location == "old" else new_cache
        csv_root = old_csv if location == "old" else new_cache
        name = row["url"].rsplit("/", 1)[1]
        checksum = (zip_root / (name + ".CHECKSUM")).read_text().split()
        if checksum != [row["published_sha256"], name]:
            raise ValueError("CACHED_OFFICIAL_CHECKSUM_MISMATCH")
        proof = verify_member((zip_root / name).read_bytes(), row, (csv_root / row["csv_scratch"]).read_bytes())
        if proof["csv_sha256"] != row["csv_sha256"]:
            raise ValueError("CACHED_CSV_BINDING_MISMATCH")
        records.append(
            {
                k: row[k]
                for k in ("symbol", "interval", "month", "url", "status", "published_sha256", "csv_sha256", "member")
            }
        )
    return {
        "version": "NATIVE_TRACE_SOURCE_BINDINGS_V1",
        "records": records,
        "network_requests": 0,
        "original_receipt": "UNKNOWN_NOT_CAPTURED",
        "original_health": "UNKNOWN",
        "archive_read": "LOCAL_CACHED_ZIP_CHECKSUM_CSV_REVERIFIED",
        "authority": "ZERO",
    }


def adapt(bar, key, actual_local_read_ns):
    raw = RawObservationV2.build(
        instrument_revision=key.contract_revision,
        source_id="BINANCE_VISION_LOCAL_ARCHIVE_REPLAY",
        event_type="BAR_" + bar.interval.value,
        received_at_ns=actual_local_read_ns,
        ingested_at_ns=actual_local_read_ns,
        available_at_ns=actual_local_read_ns,
        translation_version="NATIVE_TRACE_LOCAL_ARCHIVE_READ_V1",
        payload={
            "source_row_id": bar.stable_source_row_id,
            "values": [str(getattr(bar, x)) for x in ("open", "high", "low", "close", "volume")],
        },
        event_at_ns=bar.close_at_ns,
        sequence=bar.stable_source_row_id,
        quality_flags=("LOCAL_ARCHIVE_READ_NOT_ORIGINAL_EXCHANGE_RECEIPT",),
        availability_class=AvailabilityClassV2.RECONSTRUCTED_MARKET,
        replay_available_at_ns=bar.close_at_ns + (6_000_000_000 if bar.interval == BarIntervalV2.H4 else 5_000_000_000),
    )
    return CausalBarV2(
        raw,
        bar.interval,
        bar.open_at_ns,
        bar.close_at_ns,
        *(Decimal(str(getattr(bar, x))) for x in ("open", "high", "low", "close", "volume")),
        True,
    )


def require_declared_origins(join, origins):
    """A missing leading prefix cannot silently become a new indicator seed."""
    for frame, bars in ((BarIntervalV2.H1, join.h1), (BarIntervalV2.H4, join.h4), (BarIntervalV2.M15, join.m15)):
        if not bars or bars[0].open_at_ns != origins[frame]:
            raise ValueError("native declared prefix origin missing")


def write_gzip(path, rows):
    with path.open("wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as z:
        for row in rows:
            z.write((canonical_json(row) + "\n").encode())


def run(old_csv, old_zip, new_cache, outdir, db_path):
    start = time.monotonic()
    protocol = json.loads((HERE / "PROTOCOL.json").read_text())
    begin, end = common.ns(protocol["begin_utc"]), common.ns(protocol["end_exclusive_utc"])
    symbols = common.protocol()["cohort"]
    code_paths = [
        "src/atlas/v2/runtime/native_research_trace.py",
        str((HERE / "replay.py").relative_to(ROOT)),
        str((HERE / "audit.py").relative_to(ROOT)),
        str((HERE / "PROTOCOL.json").relative_to(ROOT)),
    ]
    pinned = common.verify_prior_inputs()
    identities = {name: file_sha(ROOT / name) for name in code_paths}
    native_paths = [
        "strategies/s5_crowding.py",
        "strategies/s8_pairs.py",
        "news/events.py",
        "data/microstructure.py",
        "features/pipeline.py",
        "features/structure.py",
        "features/candles.py",
        "features/technical.py",
        "strategies/s1_trend.py",
        "strategies/s2_breakout.py",
        "strategies/s3_mean_reversion.py",
        "strategies/s6_cross_section.py",
        "runtime/full_strategy_surface.py",
        "science/m15_origin_accounting.py",
        "runtime/s3_native_cadence.py",
        "science/mean_evaluation.py",
        "research/decision_brief.py",
        "memory/repository.py",
    ]
    identities.update({"src/atlas/v2/" + p: file_sha(ROOT / "src/atlas/v2" / p) for p in native_paths})
    code_body = {
        "version": "NATIVE_TRACE_CODE_IDENTITY_V1",
        "identities": identities,
        "prior_pins": pinned,
        "authority": "ZERO",
    }
    code_ref = sha256_json(code_body)
    manifest = source_manifest(old_csv, old_zip, new_cache, symbols)
    manifest_ref = sha256_json(manifest)
    health = {
        "version": "NATIVE_TRACE_SOURCE_HEALTH_LIMIT_V1",
        "source_ref": manifest_ref,
        "status": "UNVERIFIED",
        "reason": "ORIGINAL_SOURCE_RECEIPT_CONTINUITY_NOT_CAPTURED",
        "authority": "ZERO",
    }
    health_ref = sha256_json(health)
    outdir.mkdir(parents=True, exist_ok=True)
    published = []
    rich_calls = 0
    prefix_counts = {"complete": 0, "unavailable": 0}
    first_read = time.time_ns()
    with OpsRepository(db_path) as repo:
        writer = TraceWriter(repo)

        def publish(body, kind):
            ref = sha256_json(body)
            writer.publish({ref: (kind, body)})
            published.append((ref, kind))
            return ref

        publish(code_body, "NativeTraceCodeV1")
        publish(manifest, "NativeTraceSourcesV1")
        publish(health, "NativeTraceHealthLimitV1")
        for symbol in symbols:
            if time.monotonic() - start > protocol["compute"]["wall_cap_seconds"]:
                raise ValueError("REPLAY_WALL_CAP")
            key = sources.accepted.key_for(symbol)
            native_h1 = sources.load_native(symbol, "1h", 2025, range(1, 8), old_csv, new_cache)
            native_m15 = sources.load_native(symbol, "15m", 2025, (6, 7), old_csv, new_cache)
            # Filter before native adapter input construction; only the already
            # revealed integration interval and earlier source prefixes remain.
            native_h1 = [b for b in native_h1 if b.end < end]
            native_m15 = [b for b in native_m15 if b.end < end]
            native_h4 = sources.accepted.h4_bars_from_h1(symbol, native_h1)
            local_read = time.time_ns()
            frames = {
                BarIntervalV2.H1: tuple(adapt(b, key, local_read) for b in native_h1),
                BarIntervalV2.H4: tuple(adapt(b, key, local_read) for b in native_h4),
                BarIntervalV2.M15: tuple(adapt(b, key, local_read) for b in native_m15),
            }
            times = {i: [b.close_at_ns for b in bars] for i, bars in frames.items()}
            base = {
                "version": "NATIVE_TRACE_INSTRUMENT_SOURCE_V1",
                "source_manifest_ref": manifest_ref,
                "key": key.to_dict(),
                "code_ref": code_ref,
                "prefix_origins": protocol["prefixes"],
                "revision_status": "RESEARCH_UNKNOWN_REVISION_NOT_PIT_CONTRACT",
                "authority": "ZERO",
            }
            source_ref = publish(base, "NativeTraceInstrumentSourceV1")
            for at in range(begin, end, common.MINUTE if hasattr(common, "MINUTE") else 60_000_000_000):
                feature_ref = native_ref = None
                processing_ready_ns = None
                missing = False
                evaluated = False
                if at % common.M15 == 0:
                    selected = {i: bars[: bisect.bisect_right(times[i], at)] for i, bars in frames.items()}
                    join = JoinedBars(
                        key,
                        max(
                            at + 5_000_000_000,
                            max((b.replay_available_at_ns for b in selected[BarIntervalV2.H4]), default=0),
                        ),
                        selected[BarIntervalV2.H4],
                        selected[BarIntervalV2.H1],
                        selected[BarIntervalV2.M15],
                        "NOT_ESTIMABLE",
                        "SOURCE_HEALTH_UNKNOWN",
                        health_ref,
                    )
                    try:
                        check_prefix(join)
                        require_declared_origins(
                            join,
                            {
                                BarIntervalV2.H1: common.ns(protocol["prefixes"]["H1_H4"]),
                                BarIntervalV2.H4: common.ns(protocol["prefixes"]["H1_H4"]),
                                BarIntervalV2.M15: common.ns(protocol["prefixes"]["M15"]),
                            },
                        )
                    except ValueError as exc:
                        if (
                            "incomplete native prefix" not in str(exc)
                            and "terminal interval missing" not in str(exc)
                            and "declared prefix origin missing" not in str(exc)
                        ):
                            raise
                        missing = True
                        prefix_counts["unavailable"] += 1
                        native_ref = publish(
                            {
                                "version": "NATIVE_TRACE_PRICE_BLOCK_V1",
                                "source_ref": source_ref,
                                "slot_ns": at,
                                "reason": str(exc),
                                "status": "SOURCE_UNAVAILABLE",
                                "authority": "ZERO",
                            },
                            "NativeTracePriceBlockV1",
                        )
                    else:
                        prefix_counts["complete"] += 1
                        processing_ready_ns = join.cutoff_ns + 1_000_000_000
                        evaluated = True
                        prefix = {
                            "version": "NATIVE_TRACE_PREFIX_V1",
                            "source_ref": source_ref,
                            "slot_ns": at,
                            "frames": {
                                i.value: {
                                    "count": len(bars),
                                    "first_close_ns": bars[0].close_at_ns,
                                    "last_close_ns": bars[-1].close_at_ns,
                                    "ordered_bar_hashes_sha256": sha256_json([b.content_hash for b in bars]),
                                }
                                for i, bars in selected.items()
                            },
                            "authority": "ZERO",
                        }
                        prefix_ref = publish(prefix, "NativeTracePrefixV1")
                        native = observe_components(join)
                        native["qualified_coordinators"] = observe_coordinators(repo, join, None, universe=None)
                        native_ref = publish(
                            {
                                "version": "NATIVE_TRACE_COMPONENTS_V1",
                                "source_prefix_ref": prefix_ref,
                                "slot_ns": at,
                                "native": native,
                                "authority": "ZERO",
                            },
                            "NativeTraceComponentsV1",
                        )
                        if at == begin and symbol in symbols[:3]:
                            feature_body, repeated_native = observe_prices(join, source_ref=prefix_ref)
                            assert {k: v for k, v in native.items() if k != "qualified_coordinators"} == repeated_native
                            feature_ref = publish(feature_body, "NativeTraceFeatureV1")
                            rich_calls += 1
                            # Every native feature dependency is resolvable to its
                            # exact source-backed bar and stable official row ID.
                            binding_batch = {}
                            for bars in selected.values():
                                for bar in bars:
                                    body = {
                                        "version": "NATIVE_TRACE_BAR_BINDING_V1",
                                        "native_bar_ref": bar.content_hash,
                                        "bar": bar.to_dict(),
                                        "source_row_identity": str(bar.raw.sequence),
                                        "parent_bar_refs": [
                                            b.content_hash
                                            for b in selected[BarIntervalV2.H1]
                                            if bar.open_at_ns <= b.open_at_ns < bar.close_at_ns
                                        ]
                                        if bar.interval == BarIntervalV2.H4
                                        else [],
                                        "source_manifest_ref": manifest_ref,
                                        "original_receipt": "UNKNOWN_NOT_CAPTURED",
                                        "replay_available_ns": bar.replay_available_at_ns,
                                        "authority": "ZERO",
                                    }
                                    ref = sha256_json(body)
                                    if repo.get_artifact(ref) is not None:
                                        continue
                                    binding_batch[ref] = ("NativeTraceBarBindingV1", body)
                                    published.append((ref, "NativeTraceBarBindingV1"))
                                    if len(binding_batch) == 64:
                                        writer.publish(binding_batch)
                                        binding_batch = {}
                            if binding_batch:
                                writer.publish(binding_batch)
                trace = make_slot(
                    key=key,
                    at=at,
                    source_ref=source_ref,
                    code_ref=code_ref,
                    feature_ref=feature_ref,
                    native_ref=native_ref,
                    evaluated=evaluated,
                    source_missing=missing,
                    processing_ready_ns=processing_ready_ns,
                )
                entry = writer.publish_decision(trace)
                published.append((entry.artifact_ref, entry.artifact_type))
            print(
                json.dumps({"symbol": symbol, "native_price_prefixes": prefix_counts, "rich_calls": rich_calls}),
                flush=True,
            )
        # Export immutable domain records in deterministic order. Actual current
        # SQLite publication clocks remain in Ops, never inside replay clocks.
        unique = sorted(set(published))

        def bodies():
            for ref, kind in unique:
                entry = repo.get_artifact(ref)
                if entry is None or entry.artifact_type != kind:
                    raise ValueError("MISSING_PERSISTED_TRACE")
                yield {
                    "artifact_ref": ref,
                    "artifact_type": kind,
                    "content_hash": entry.content_hash,
                    "body": entry.metadata["trace"].to_dict(),
                }

        write_gzip(outdir / "TRACE.jsonl.gz", bodies())
        disk_before = repo.persistence_metrics()
        repo.checkpoint()
        publication_span = {
            "minimum_index_publication_ns": min(repo.get_artifact(r).created_at_ns for r, k in unique),
            "maximum_index_publication_ns": max(repo.get_artifact(r).available_at_ns for r, k in unique),
        }
    size = sum(p.stat().st_size for p in outdir.iterdir() if p.is_file())
    if (
        size > protocol["storage"]["output_cap_bytes"]
        or db_path.stat().st_size > protocol["storage"]["scratch_cap_bytes"]
    ):
        raise ValueError("TRACE_STORAGE_CAP")
    parts = []
    with (outdir / "TRACE.jsonl.gz").open("rb") as f:
        for index, data in enumerate(iter(lambda: f.read(3_000_000), b"")):
            part = outdir / f"TRACE.jsonl.gz.part{index:03}"
            part.write_bytes(data)
            parts.append({"path": part.name, "sha256": file_sha(part), "bytes": len(data)})
    size = sum(p.stat().st_size for p in outdir.iterdir() if p.is_file())
    if size > protocol["storage"]["output_cap_bytes"]:
        raise ValueError("FINAL_TRACE_TRANSPORT_STORAGE_CAP")
    seal = {
        "version": "NATIVE_TRACE_SEAL_V1",
        "trace_sha256": file_sha(outdir / "TRACE.jsonl.gz"),
        "transport_parts": parts,
        "code_ref": code_ref,
        "code_hashes": identities,
        "source_manifest_ref": manifest_ref,
        "source_sha": protocol["source_sha"],
        "protocol_sha256": file_sha(HERE / "PROTOCOL.json"),
        "rows": len(unique),
        "decision_slots": len(symbols) * 60,
        "native_price_prefixes": prefix_counts,
        "full_native_feature_calls": rich_calls,
        "authority": "ZERO",
        "outcomes": "NOT_STARTED",
    }
    common.save(outdir / "TRACE_SEAL.json", seal)
    common.save(
        outdir / "REPLAY_RESOURCES.json",
        {
            "wall_seconds": time.monotonic() - start,
            "max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            "output_bytes": size,
            "ops_sqlite_bytes": db_path.stat().st_size,
            "publication_span_actual": publication_span,
            "adapter_started_actual_ns": first_read,
            "persistence_metrics": disk_before,
            "network_requests": 0,
        },
    )
    print(json.dumps(seal), flush=True)
    return seal


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--old-csv", type=pathlib.Path, required=True)
    p.add_argument("--old-zip", type=pathlib.Path, required=True)
    p.add_argument("--new-cache", type=pathlib.Path, required=True)
    p.add_argument("--outdir", type=pathlib.Path, required=True)
    p.add_argument("--db", type=pathlib.Path, required=True)
    a = p.parse_args()
    run(a.old_csv, a.old_zip, a.new_cache, a.outdir, a.db)


if __name__ == "__main__":
    main()
