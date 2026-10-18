"""Separate fixed audit of SEALED native trace records; no market/outcome access.

No trade existed in the historical source path. Consequently this stage audits
native outputs, chronology, refusals and denominators. It creates no price-path,
fill, return or cash labels and cannot change a decision.
"""

from __future__ import annotations

import argparse
import collections
import gzip
import hashlib
import json
import os
import pathlib
import shutil
import sys

ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))
from atlas.v2._serialization import sha256_json  # noqa: E402
from atlas.v2.memory.repository import OpsRepository  # noqa: E402


def digest(path):
    h = hashlib.sha256()
    with pathlib.Path(path).open("rb") as f:
        for b in iter(lambda: f.read(1024 * 1024), b""):
            h.update(b)
    return h.hexdigest()


def restore_trace(directory, seal):
    full = directory / "TRACE.jsonl.gz"
    if full.exists():
        if digest(full) != seal["trace_sha256"]:
            raise ValueError("SEALED_TRACE_BYTES_CHANGED")
        return full
    temporary = directory / "TRACE.assemble.tmp"
    try:
        with temporary.open("xb") as out:
            for part in seal["transport_parts"]:
                p = directory / part["path"]
                if digest(p) != part["sha256"]:
                    raise ValueError("SEALED_TRACE_PART_CHANGED")
                with p.open("rb") as source:
                    shutil.copyfileobj(source, out, 1024 * 1024)
            out.flush()
            os.fsync(out.fileno())
        if digest(temporary) != seal["trace_sha256"]:
            raise ValueError("SEALED_TRACE_ASSEMBLY_CHANGED")
        os.replace(temporary, full)
    finally:
        temporary.unlink(missing_ok=True)
    return full


def rows(path):
    with gzip.open(path, "rt") as f:
        for line in f:
            yield json.loads(line)


def audit(directory, db=None):
    seal = json.loads((directory / "TRACE_SEAL.json").read_text())
    for name, expected in seal["code_hashes"].items():
        if digest(ROOT / name) != expected:
            raise ValueError("SEALED_NATIVE_CODE_CHANGED:" + name)
    path = restore_trace(directory, seal)
    before = digest(path)
    refs = set()
    derived_parents = set()
    bar_refs = set()
    required = set()
    decisions = set()
    slots = set()
    types = collections.Counter()
    statuses = collections.Counter()
    precursors = collections.Counter()
    reasons = collections.Counter()
    source_statuses = collections.Counter()
    trends = collections.Counter()
    feature_fields = collections.Counter()
    features = []
    timing_misses = 0
    max_record_bytes = 0
    published = {"min_ns": None, "max_ns": None}
    repo = OpsRepository(db, read_only=True) if db else None
    try:
        for row in rows(path):
            body = row["body"]
            ref = row["artifact_ref"]
            kind = row["artifact_type"]
            expected = sha256_json(body)
            if expected != row["content_hash"] or ref in refs:
                raise ValueError("TRACE_HASH_OR_DUPLICATE")
            refs.add(ref)
            types[kind] += 1
            max_record_bytes = max(max_record_bytes, len(json.dumps(body, separators=(",", ":")).encode()))
            if body.get("authority") != "ZERO":
                raise ValueError("NONZERO_TRACE_AUTHORITY")
            if repo:
                entry = repo.get_artifact(ref)
                if entry is None or entry.artifact_type != kind or entry.content_hash != expected:
                    raise ValueError("PERSISTED_TRACE_MISMATCH")
                if entry.created_at_ns <= 1751328000000000000:
                    raise ValueError("BACKDATED_PUBLICATION")
                published["min_ns"] = min(published["min_ns"] or entry.created_at_ns, entry.created_at_ns)
                published["max_ns"] = max(published["max_ns"] or entry.available_at_ns, entry.available_at_ns)
            if kind == "NATIVE_ALGORITHM_RESEARCH_TRACE_V1":
                if ref != body["decision_id"]:
                    raise ValueError("DECISION_ALIAS_MISMATCH")
                key = (body["instrument_key"]["native_symbol"], body["slot_ns"])
                if key in slots:
                    raise ValueError("ORIGINAL_CALENDAR_DUPLICATE")
                slots.add(key)
                decisions.add(ref)
                stages = body["stages"]
                required.update((body["source_ref"], body["code_ref"]))
                if set(stages) != {"source", "features", "strategies", "economics", "selection", "management"}:
                    raise ValueError("TRACE_STAGE_OMISSION")
                for role, item in stages["strategies"].items():
                    statuses[role + ":" + item["status"]] += 1
                    reasons[role + ":" + item["reason"]] += 1
                if stages["features"]["deadline_status"] == "MISSED":
                    timing_misses += 1
                    if (
                        stages["features"]["replay_processing_ready_ns"]
                        <= stages["features"]["original_m15_deadline_ns"]
                    ):
                        raise ValueError("FALSE_DEADLINE_MISS")
                for name in ("ref", "price_observation_ref"):
                    if stages["features"][name]:
                        required.add(stages["features"][name])
                if stages["economics"]["exact_actions"] or stages["economics"]["cash_label"] is not None:
                    raise ValueError("UNJUSTIFIED_EXACT_ACTION_OR_CASH")
                if (
                    stages["selection"]["eligible_set"]
                    or stages["selection"]["agent_decision"]
                    or stages["selection"]["fallback"]
                ):
                    raise ValueError("UNJUSTIFIED_AGENT_SELECTION")
                if stages["management"]["outcome_link"]:
                    raise ValueError("UNJUSTIFIED_ACTION_OUTCOME")
                if stages["source"]["original_receipt"] != "UNKNOWN_NOT_CAPTURED":
                    raise ValueError("FABRICATED_ORIGINAL_RECEIPT")
            elif ref != expected:
                raise ValueError("TRACE_CONTENT_ADDRESS_MISMATCH")
            if kind == "NativeTraceBarBindingV1":
                bar_refs.add(body["native_bar_ref"])
                derived_parents.update(body["parent_bar_refs"])
            elif kind == "NativeTraceSourcesV1":
                source_statuses.update(r["status"] for r in body["records"])
                if body["network_requests"] != 0:
                    raise ValueError("UNDECLARED_NETWORK")
            elif kind == "NativeTraceComponentsV1":
                required.add(body["source_prefix_ref"])
                native = body["native"]
                precursors[native["s1_precursor"]["status"]] += 1
                trends[native["s6_context"]["trend"]] += 1
                if native["s1_precursor"]["candidate"] is not None or native["s6_context"]["rank"] is not None:
                    raise ValueError("FALSE_PRECURSOR_OR_CONTEXT_ACTION")
            elif kind == "NativeTracePriceBlockV1":
                reasons["PRICE_PREFIX:" + body["reason"]] += 1
            elif kind == "NativeTraceFeatureV1":
                required.add(body["source_ref"])
                f = body["native_feature"]
                feature_fields.update(
                    "AVAILABLE" if v["value"] is not None else "MISSING:" + v["missing_reason"]
                    for v in f["values"].values()
                )
                features.append(
                    {
                        "ref": ref,
                        "key": f["key"],
                        "feature_set_version": f["feature_set_version"],
                        "field_count": len(f["values"]),
                        "values": f["values"],
                    }
                )
        prior = ROOT / "docs/research/atlas-red-historical-blind-ml/PROTOCOL.json"
        symbols = json.loads(prior.read_text())["cohort"]
        begin = 1751328000000000000
        end = begin + 3600_000_000_000
        expected_slots = {(symbol, at) for symbol in symbols for at in range(begin, end, 60_000_000_000)}
        if slots != expected_slots or len(decisions) != seal["decision_slots"]:
            raise ValueError("CALENDAR_DENOMINATOR_MISMATCH")
        if not required.issubset(refs):
            raise ValueError("UNRESOLVED_TRACE_PARENT")
        health_refs = {r["artifact_ref"] for r in rows(path) if r["artifact_type"] == "NativeTraceHealthLimitV1"}
        for r in rows(path):
            if r["artifact_type"] == "NativeTraceFeatureV1":
                inputs = set(r["body"]["native_feature"]["envelope"]["input_refs"])
                if not inputs.issubset(bar_refs | health_refs):
                    raise ValueError("UNRESOLVED_NATIVE_FEATURE_INPUT")
        if not derived_parents.issubset(bar_refs):
            raise ValueError("UNRESOLVED_DERIVED_H4_PARENT")
        if before != digest(path):
            raise ValueError("AUDIT_MUTATED_DECISIONS")
        return {
            "version": "NATIVE_TRACE_POST_SEAL_AUDIT_V1",
            "trace_sha256": before,
            "artifact_count": len(refs),
            "original_minute_observations": len(slots),
            "calendar_key_mismatches": 0,
            "strategy_surface_statuses": dict(sorted(statuses.items())),
            "reasons": dict(sorted(reasons.items())),
            "native_s1_precursor_observations": dict(precursors),
            "native_s6_context_observations": dict(trends),
            "native_full_feature_observations": features,
            "native_feature_field_counts": dict(feature_fields),
            "source_objects": dict(source_statuses),
            "timing_misses": timing_misses,
            "artifact_types": dict(types),
            "native_feature_dependencies_resolved": True,
            "decision_bytes_unchanged": True,
            "maximum_record_bytes": max_record_bytes,
            "actual_ops_publication": published if repo else "NOT_CHECKED_NO_DB",
            "price_outcomes": "NOT_ESTIMABLE_NO_ELIGIBLE_ACTION",
            "cash_outcomes": "NOT_ESTIMABLE",
            "qualified_actions": 0,
            "orders": 0,
            "agent_calls": 0,
            "authority": "ZERO",
            "protocol_sha256": seal["protocol_sha256"],
            "code_ref": seal["code_ref"],
        }
    finally:
        if repo:
            repo.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--evidence", type=pathlib.Path, required=True)
    p.add_argument("--db", type=pathlib.Path)
    p.add_argument("--output", type=pathlib.Path)
    p.add_argument("--decision-id")
    a = p.parse_args()
    if a.decision_id:
        if not a.db:
            p.error("--decision-id requires --db")
        with OpsRepository(a.db, read_only=True) as repo:
            row = repo.get_artifact(a.decision_id)
            if row is None:
                raise ValueError("DECISION_NOT_FOUND")
            print(json.dumps(row.metadata.to_dict(), sort_keys=True, indent=2))
        return
    result = audit(a.evidence, a.db)
    text = json.dumps(result, sort_keys=True, indent=2) + "\n"
    if a.output:
        a.output.write_text(text)
    else:
        print(text)


if __name__ == "__main__":
    main()
