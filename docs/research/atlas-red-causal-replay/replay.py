"""Fixed development replay; reuse native foundation readers, owners and audit.

No market transport or outcome reader exists here. Only cutoff-filtered,
complete immutable prefixes reach native_research_trace. Existing historical
packages are preserved; this continuation has a distinct protocol and seal.
"""

# ruff: noqa: E402
from __future__ import annotations

import argparse
import bisect
import importlib.util
import json
import pathlib
import resource
import sys
import time

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parents[2]
FOUNDATION = HERE.parent / "atlas-red-native-trace"
sys.path.insert(0, str(ROOT / "src"))
spec = importlib.util.spec_from_file_location("native_foundation_replay", FOUNDATION / "replay.py")
foundation = importlib.util.module_from_spec(spec)
spec.loader.exec_module(foundation)
common, sources = foundation.common, foundation.sources

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.data.bars import BarIntervalV2
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


def run(old_csv, old_zip, new_cache, outdir, db_path):
    start = time.monotonic()
    protocol = json.loads((HERE / "PROTOCOL.json").read_text())
    begin, end = common.ns(protocol["begin_utc"]), common.ns(protocol["end_exclusive_utc"])
    symbols = common.protocol()["cohort"]
    # Bind every pre-existing source/policy owner sealed by the foundation, plus
    # source adapters/serialization and this new mechanical replay/audit path.
    old_seal = json.loads((FOUNDATION / "evidence/TRACE_SEAL.json").read_text())
    paths = set(old_seal["code_hashes"]) | {
        str((HERE / name).relative_to(ROOT)) for name in ("replay.py", "audit.py", "PROTOCOL.json", "SOURCE_READINESS.json")
    } | {"src/atlas/v2/features/joins.py", "src/atlas/v2/data/raw.py", "src/atlas/v2/data/bars.py",
         "src/atlas/v2/contracts.py", "src/atlas/v2/_serialization.py",
         "docs/research/atlas-red-historical-blind-ml/causal_replay.py",
         "docs/research/atlas-red-historical-blind-ml/common.py"}
    code = {"version": "NATIVE_TRACE_CODE_IDENTITY_V1", "authority": "ZERO",
            "identities": {p: foundation.file_sha(ROOT / p) for p in sorted(paths)},
            "prior_pins": common.verify_prior_inputs()}
    code_ref = sha256_json(code)
    manifest = foundation.source_manifest(old_csv, old_zip, new_cache, symbols)
    manifest_ref = sha256_json(manifest)
    health = {"version": "NATIVE_TRACE_SOURCE_HEALTH_LIMIT_V1", "source_ref": manifest_ref,
              "status": "UNVERIFIED", "reason": "ORIGINAL_SOURCE_RECEIPT_CONTINUITY_NOT_CAPTURED", "authority": "ZERO"}
    health_ref = sha256_json(health)
    readiness = json.loads((HERE / "SOURCE_READINESS.json").read_text())
    outdir.mkdir(parents=True, exist_ok=True)
    if db_path.exists() or any(outdir.glob("TRACE*")):
        raise ValueError("FRESH_RUN_TARGET_REQUIRED")
    published = set()
    rich_calls = 0
    prefix_counts = {"complete": 0, "unavailable": 0}
    first_read = time.time_ns()
    with OpsRepository(db_path) as repo:
        writer = TraceWriter(repo)

        def publish(body, kind):
            ref = sha256_json(body)
            writer.publish({ref: (kind, body)})
            published.add((ref, kind))
            return ref

        publish(code, "NativeTraceCodeV1")
        publish(manifest, "NativeTraceSourcesV1")
        publish(health, "NativeTraceHealthLimitV1")
        readiness_ref = publish(readiness, "NativeReplaySourceReadinessV1")
        for symbol in symbols:
            if time.monotonic() - start > protocol["compute"]["wall_cap_seconds"]:
                raise ValueError("REPLAY_WALL_CAP")
            key = sources.accepted.key_for(symbol)
            native_h1 = [b for b in sources.load_native(symbol, "1h", 2025, range(1, 8), old_csv, new_cache) if b.end < end]
            native_m15 = [b for b in sources.load_native(symbol, "15m", 2025, (6, 7), old_csv, new_cache) if b.end < end]
            native_h4 = sources.accepted.h4_bars_from_h1(symbol, native_h1)
            local_read = time.time_ns()
            frames = {frame: tuple(foundation.adapt(b, key, local_read) for b in bars)
                      for frame, bars in ((BarIntervalV2.H1, native_h1), (BarIntervalV2.H4, native_h4), (BarIntervalV2.M15, native_m15))}
            times = {i: [b.close_at_ns for b in bars] for i, bars in frames.items()}
            base = {"version": "NATIVE_TRACE_INSTRUMENT_SOURCE_V1", "source_manifest_ref": manifest_ref,
                    "key": key.to_dict(), "code_ref": code_ref, "prefix_origins": protocol["prefixes"],
                    "source_readiness_ref": readiness_ref,
                    "revision_status": "RESEARCH_UNKNOWN_REVISION_NOT_PIT_CONTRACT", "authority": "ZERO"}
            source_ref = publish(base, "NativeTraceInstrumentSourceV1")
            for at in range(begin, end, BarIntervalV2.M1.duration_ns):
                feature_ref = native_ref = processing_ready_ns = None
                missing = evaluated = False
                if at % common.M15 == 0:
                    selected = {i: bars[:bisect.bisect_right(times[i], at)] for i, bars in frames.items()}
                    ready = max(at + 5_000_000_000,
                                max((b.replay_available_at_ns for b in selected[BarIntervalV2.H4]), default=0))
                    join = JoinedBars(key, ready, selected[BarIntervalV2.H4], selected[BarIntervalV2.H1],
                                      selected[BarIntervalV2.M15], "NOT_ESTIMABLE", "SOURCE_HEALTH_UNKNOWN", health_ref)
                    try:
                        check_prefix(join)
                        foundation.require_declared_origins(join, {
                            BarIntervalV2.H1: common.ns(protocol["prefixes"]["H1_H4"]),
                            BarIntervalV2.H4: common.ns(protocol["prefixes"]["H1_H4"]),
                            BarIntervalV2.M15: common.ns(protocol["prefixes"]["M15"]),
                        })
                    except ValueError as exc:
                        if not any(x in str(exc) for x in ("incomplete native prefix", "terminal interval missing", "declared prefix origin missing")):
                            raise
                        missing = True
                        prefix_counts["unavailable"] += 1
                        native_ref = publish({"version": "NATIVE_TRACE_PRICE_BLOCK_V1", "source_ref": source_ref,
                                              "slot_ns": at, "reason": str(exc), "status": "SOURCE_UNAVAILABLE",
                                              "authority": "ZERO"}, "NativeTracePriceBlockV1")
                    else:
                        evaluated = True
                        processing_ready_ns = ready + 1_000_000_000
                        prefix_counts["complete"] += 1
                        prefix_ref = publish({"version": "NATIVE_TRACE_PREFIX_V1", "source_ref": source_ref,
                            "slot_ns": at, "frames": {i.value: {"count": len(bars), "first_close_ns": bars[0].close_at_ns,
                                "last_close_ns": bars[-1].close_at_ns, "ordered_bar_hashes_sha256": sha256_json([b.content_hash for b in bars])}
                                for i, bars in selected.items()}, "authority": "ZERO"}, "NativeTracePrefixV1")
                        native = observe_components(join)
                        native["qualified_coordinators"] = observe_coordinators(repo, join, None, universe=None)
                        native_ref = publish({"version": "NATIVE_TRACE_COMPONENTS_V1", "source_prefix_ref": prefix_ref,
                                              "slot_ns": at, "native": native, "authority": "ZERO"}, "NativeTraceComponentsV1")
                        if symbol in protocol["full_native_feature_symbols"]:
                            feature, parity = observe_prices(join, source_ref=prefix_ref)
                            assert parity == {k: v for k, v in native.items() if k != "qualified_coordinators"}
                            feature_ref = publish(feature, "NativeTraceFeatureV1")
                            rich_calls += 1
                            batch = {}
                            for bars in selected.values():
                                for b in bars:
                                    body = {"version": "NATIVE_TRACE_BAR_BINDING_V1", "native_bar_ref": b.content_hash,
                                        "bar": b.to_dict(), "source_row_identity": str(b.raw.sequence),
                                        "parent_bar_refs": [x.content_hash for x in selected[BarIntervalV2.H1]
                                            if b.open_at_ns <= x.open_at_ns < b.close_at_ns] if b.interval == BarIntervalV2.H4 else [],
                                        "source_manifest_ref": manifest_ref, "original_receipt": "UNKNOWN_NOT_CAPTURED",
                                        "replay_available_ns": b.replay_available_at_ns, "authority": "ZERO"}
                                    ref = sha256_json(body)
                                    if repo.get_artifact(ref) is not None:
                                        continue
                                    batch[ref] = ("NativeTraceBarBindingV1", body)
                                    published.add((ref, "NativeTraceBarBindingV1"))
                                    if len(batch) == 64:
                                        writer.publish(batch)
                                        batch = {}
                            if batch:
                                writer.publish(batch)
                trace = make_slot(key=key, at=at, source_ref=source_ref, code_ref=code_ref, feature_ref=feature_ref,
                                  native_ref=native_ref, evaluated=evaluated, source_missing=missing,
                                  processing_ready_ns=processing_ready_ns)
                entry = writer.publish_decision(trace)
                published.add((entry.artifact_ref, entry.artifact_type))
            print(json.dumps({"symbol": symbol, "prefixes": prefix_counts, "feature_calls": rich_calls}), flush=True)
        if rich_calls != protocol["compute"]["rich_feature_calls"]:
            raise ValueError("NATIVE_FEATURE_BUDGET_RECONCILIATION")

        def bodies():
            for ref, kind in sorted(published):
                row = repo.get_artifact(ref)
                if row is None or row.artifact_type != kind:
                    raise ValueError("MISSING_PERSISTED_TRACE")
                yield {"artifact_ref": ref, "artifact_type": kind, "content_hash": row.content_hash,
                       "body": row.metadata["trace"].to_dict()}

        foundation.write_gzip(outdir / "TRACE.jsonl.gz", bodies())
        metrics = repo.persistence_metrics()
        repo.checkpoint()
        publication = {"minimum_index_publication_ns": min(repo.get_artifact(r).created_at_ns for r, _ in published),
                       "maximum_index_publication_ns": max(repo.get_artifact(r).available_at_ns for r, _ in published)}
    parts = []
    with (outdir / "TRACE.jsonl.gz").open("rb") as f:
        for index, data in enumerate(iter(lambda: f.read(3_000_000), b"")):
            part = outdir / f"TRACE.jsonl.gz.part{index:03}"
            part.write_bytes(data)
            parts.append({"path": part.name, "bytes": len(data), "sha256": foundation.file_sha(part)})
    size = sum(p.stat().st_size for p in outdir.iterdir() if p.is_file())
    if size > protocol["storage"]["output_cap_bytes"] or db_path.stat().st_size > protocol["storage"]["scratch_cap_bytes"]:
        raise ValueError("TRACE_STORAGE_CAP")
    seal = {"version": "NATIVE_TRACE_SEAL_V1", "trace_sha256": foundation.file_sha(outdir / "TRACE.jsonl.gz"),
            "transport_parts": parts, "code_ref": code_ref, "code_hashes": code["identities"], "source_manifest_ref": manifest_ref,
            "source_sha": protocol["source_sha"], "protocol_sha256": foundation.file_sha(HERE / "PROTOCOL.json"),
            "rows": len(published), "decision_slots": len(symbols) * 60, "native_price_prefixes": prefix_counts,
            "full_native_feature_calls": rich_calls, "authority": "ZERO", "outcomes": "NOT_STARTED"}
    common.save(outdir / "TRACE_SEAL.json", seal)
    common.save(outdir / "REPLAY_RESOURCES.json", {"wall_seconds": time.monotonic() - start,
        "max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss, "output_bytes": size,
        "ops_sqlite_bytes": db_path.stat().st_size, "publication_span_actual": publication,
        "adapter_started_actual_ns": first_read, "persistence_metrics": metrics, "network_requests": 0})
    print(canonical_json(seal), flush=True)
    return seal


def main():
    p = argparse.ArgumentParser()
    for name in ("old-csv", "old-zip", "new-cache", "outdir", "db"):
        p.add_argument("--" + name, type=pathlib.Path, required=True)
    a = p.parse_args()
    run(a.old_csv, a.old_zip, a.new_cache, a.outdir, a.db)


if __name__ == "__main__":
    main()
