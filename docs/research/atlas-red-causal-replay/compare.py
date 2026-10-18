"""Fixed post-seal field comparison; no future market or outcome access."""

from __future__ import annotations

import argparse
import collections
import importlib.util
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("comparison_foundation_audit", HERE.parent / "atlas-red-native-trace/audit.py")
reader = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reader)


def observations(directory):
    seal = json.loads((directory / "TRACE_SEAL.json").read_text())
    path = reader.restore_trace(directory, seal)
    sources = {r["artifact_ref"]: r["body"]["key"]["native_symbol"] for r in reader.rows(path)
               if r["artifact_type"] == "NativeTraceInstrumentSourceV1"}
    prefixes = {}
    prefix_values = {}
    for row in reader.rows(path):
        if row["artifact_type"] == "NativeTracePrefixV1":
            b = row["body"]
            key = (sources[b["source_ref"]], b["slot_ns"])
            prefixes[row["artifact_ref"]] = key
            prefix_values[key] = b["frames"]
    components, features, blocks, slots = {}, {}, {}, {}
    feature_budget = collections.Counter()
    delay_counts = collections.Counter()
    for row in reader.rows(path):
        b, kind = row["body"], row["artifact_type"]
        if kind == "NativeTraceComponentsV1":
            components[prefixes[b["source_prefix_ref"]]] = b["native"]
        elif kind == "NativeTraceFeatureV1":
            features[prefixes[b["source_ref"]]] = b["native_feature"]["values"]
        elif kind == "NativeTracePriceBlockV1":
            blocks[(sources[b["source_ref"]], b["slot_ns"])] = b["reason"]
        elif kind == "NATIVE_ALGORITHM_RESEARCH_TRACE_V1":
            key = (b["instrument_key"]["native_symbol"], b["slot_ns"])
            stage = b["stages"]
            slots[key] = stage["strategies"]
            f = stage["features"]
            due = stage["strategies"]["S1"]["due"]
            if not due:
                budget = "NATIVE_CLOCK_NOT_DUE"
            elif key in blocks:
                budget = "SOURCE_UNAVAILABLE"
            elif f["ref"]:
                budget = "OBSERVED"
            else:
                budget = "DECLARED_FULL_FEATURE_RESOURCE_NOT_EVALUATED"
            # Blocks can precede or follow decisions in content-sorted exports;
            # source_missing is also explicit in the strategy status reason.
            if due and stage["strategies"]["S1"]["reason"] == "CURRENT_OR_PREFIX_PRICE_SOURCE_MISSING":
                budget = "SOURCE_UNAVAILABLE"
            feature_budget[budget] += 1
            if f["replay_processing_ready_ns"] is not None:
                delay_counts[str(f["replay_processing_ready_ns"] - b["slot_ns"])] += 1
    return seal, prefix_values, components, features, blocks, slots, feature_budget, delay_counts


def compare(parent, current):
    old, new = observations(parent), observations(current)
    if old[1] != new[1] or old[2] != new[2] or old[4] != new[4] or old[5].keys() != new[5].keys():
        raise ValueError("SOURCE_PREFIX_NATIVE_COMPONENT_BLOCK_OR_CALENDAR_CHANGED")
    expected = {"location.prior_day_high", "location.prior_day_low", "location.utc_day_trade_vwap_partial_to_cutoff"}
    fields = collections.Counter()
    examples = []
    for key, values in old[3].items():
        if key not in new[3]:
            raise ValueError("PARENT_FEATURE_SLOT_LOST")
        changes = {name for name in values if values[name] != new[3][key][name]}
        if changes != expected:
            raise ValueError("UNDECLARED_NATIVE_FEATURE_DIFFERENCE")
        fields.update(changes)
        examples.append({"symbol": key[0], "slot_ns": key[1],
                         "fields": {name: {"parent": values[name], "current": new[3][key][name]}
                                    for name in sorted(changes)}})
    if new[7] != {"6000000000": 99, "7000000000": 33}:
        raise ValueError("FROZEN_REPLAY_READINESS_CHANGED")
    return {"version": "CAUSAL_REPLAY_PARENT_COMPARISON_V1", "authority": "ZERO",
            "parent_trace_sha256": old[0]["trace_sha256"], "current_trace_sha256": new[0]["trace_sha256"],
            "matched_native_price_prefixes": len(old[1]), "prefix_mismatches": 0,
            "s1_s6_native_component_mismatches": 0, "same_source_blocks": len(old[4]),
            "same_original_minute_observation_keys": len(old[5]), "matched_full_feature_slots": len(old[3]),
            "additional_full_feature_slots": len(new[3]) - len(old[3]),
            "changed_fields": dict(sorted(fields.items())), "bounded_field_examples": examples,
            "full_feature_budget_denominators": dict(sorted(new[6].items())),
            "replay_processing_delay_ns": dict(sorted(new[7].items())),
            "original_m15_deadline_delay_ns": 5_000_000_000,
            "comparison_grade": "NATIVE_DIAGNOSTIC_AND_CONTRACT_CORRECTION_NOT_ACTION_OR_ECONOMIC_ACCEPTANCE"}


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--parent", type=Path, required=True)
    p.add_argument("--current", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    a.output.write_text(json.dumps(compare(a.parent, a.current), sort_keys=True, indent=2) + "\n")
