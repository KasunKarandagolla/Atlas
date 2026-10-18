"""Generate causal decisions/refusals only. No outcomes, fitting or paid calls."""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import hashlib
import pathlib
import resource
import time
from dataclasses import fields

import common
from causal_replay import BASE_FEATURES, CONTEXT_FEATURES, Market, feature_values, replay_events
from diagnostic_models import predict_numeric

from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.science.mean_evaluation import CandidateMeanInputsV1

ARMS = ("S1_ALL", "S1_S6", "EMPIRICAL", "RIDGE_S1", "RIDGE_CONTEXT", "NO_OPEN")
MODELS = ("EMPIRICAL", "RIDGE_S1", "RIDGE_CONTEXT")
CALENDAR_FIELDS = (
    "symbol",
    "cutoff_ns",
    "ready_ns",
    "source_current_m15",
    "s1_precursor",
    "setup_id",
    "trigger_status",
    "first_event_id",
    "s6_state",
    "source_ref",
    "frozen_policy",
)


def legal_input_audit(values):
    missing = [f.name for f in fields(CandidateMeanInputsV1) if values.get(f.name) is None]
    if not missing:
        raise ValueError("QUARANTINED_ADAPTER_CANNOT_MINT_QUALIFIED_ACTION")
    return {"status": "NOT ESTIMABLE", "missing_required_P1_inputs": missing, "legal_candidate_or_EligibleSet": None}


def decide(snapshot, orig, model_bundle):
    """Pure decision surface: no market/cache/path/label/outcome capability."""
    at = snapshot.prefix.cutoff_ns
    row = {
        "event_id": hashlib.sha256(f"{orig.identity}:{at}".encode()).hexdigest(),
        "setup_id": orig.identity,
        "symbol": snapshot.symbol,
        "side": orig.side,
        "cutoff_ns": at,
        "ready_ns": snapshot.prefix.ready_ns,
        "setup_cutoff_ns": orig.born,
        "setup_extreme": orig.extreme,
        "setup_ref": orig.ref,
        "source_ref": snapshot.source_ref,
        "trigger_source": snapshot.prefix.current.source if snapshot.prefix.current else "",
        "prior_source": snapshot.prefix.previous.source if snapshot.prefix.previous else "",
        "observed_trigger_volume": str(snapshot.prefix.current.volume) if snapshot.prefix.current else "",
        "actual_receipt_status": "UNKNOWN_NOT_CAPTURED_AT_ACQUISITION",
        "frozen_policy": "BLOCKED_TIMING_AND_REQUIRED_INPUTS",
        "evidence_grade": common.GRADE,
        "study_status": "SOURCE_QUALIFIED_ALT_EVENT",
        "s6_decile": snapshot.s6.get("decile", "UNAVAILABLE"),
        "btc_h4_trend": snapshot.btc.get("h4_trend", "UNKNOWN"),
        "eth_h4_trend": snapshot.eth.get("h4_trend", "UNKNOWN"),
        "s1_regime": snapshot.state.get("h4_trend", "UNKNOWN"),
        "reference_ns": (at // common.M15 + 1) * common.M15,
        "delayed_reference_ns": (at // common.M15 + 2) * common.M15,
    }
    row.update(dict.fromkeys((*BASE_FEATURES, *CONTEXT_FEATURES), ""))
    try:
        base, context = feature_values(snapshot, orig.side, orig.born, orig.extreme)
    except ValueError:
        base, context = (), None
        row["study_status"] = "UNAVAILABLE_SOURCE_FEATURES"
    if snapshot.symbol in ("BTCUSDT", "ETHUSDT"):
        row["study_status"] = "ANCHOR_CONTEXT_EXCLUDED"
    if base:
        row.update(zip(BASE_FEATURES, base, strict=True))
    if context:
        row.update(zip(CONTEXT_FEATURES, context, strict=True))
    qualified = row["study_status"] == "SOURCE_QUALIFIED_ALT_EVENT"
    for name in MODELS:
        model = model_bundle["models"].get(name)
        if not qualified:
            prediction, status = None, row["study_status"]
        elif model is None:
            prediction, status = None, "MISSING_PREFIT_MODEL"
        else:
            values = tuple(float(row[n]) if row[n] != "" else None for n in model["feature_order"])
            prediction, status = predict_numeric(model, values, at=row["ready_ns"])
        row[name + "_prediction_bps"] = prediction if prediction is not None else ""
        row[name + "_status"] = status
    row["common_model_support"] = int(qualified and all(row[n + "_status"] == "SUPPORTED" for n in MODELS))
    row["volatility_slice"] = (
        "HIGH" if base and base[-1] > model_bundle["training_volatility_median"] else "LOW" if base else "UNAVAILABLE"
    )
    for arm in ARMS:
        if arm == "S1_ALL":
            take = qualified
        elif arm == "S1_S6":
            take = qualified and row["s6_decile"] == ("TOP" if orig.side == "LONG" else "BOTTOM")
        elif arm == "NO_OPEN":
            take = False
        else:
            prediction = row[arm + "_prediction_bps"]
            take = qualified and prediction != "" and prediction > 12.0
        row[arm + "_accept"] = int(take)
    row["decision_hash"] = common.digest(row)
    return row


def run(old_cache, new_cache, outdir, ops_path, begin=None, final=None):
    common.protocol()
    common.verify_prior_inputs()
    started = time.monotonic()
    bundle = common.load(common.HERE / "PRICE_MODELS.json")
    if bundle["protocol_sha256"] != common.PROTOCOL_SHA:
        raise ValueError("MODEL_PROTOCOL_IDENTITY")
    begin = begin or common.ns("2025-07-01T00:00:00Z")
    final = final or common.ns("2025-10-01T00:00:00Z")
    if begin >= final:
        raise ValueError("EMPTY_REPLAY")
    market = Market(2025, 9, range(6, 10), old_cache, new_cache)
    outdir.mkdir(parents=True, exist_ok=True)
    calendar = outdir / "CALENDAR.csv.gz"
    events = []
    counts, states, inference = collections.Counter(), collections.Counter(), []
    with common.csv_writer(calendar, CALENDAR_FIELDS) as writer:
        for snapshot, orig, native in replay_events(market, begin, final, writer):
            tick = time.perf_counter()
            row = decide(snapshot, orig, bundle)
            inference.append(time.perf_counter() - tick)
            if row["event_id"] != native["first_event_id"]:
                raise ValueError("EVENT_IDENTITY_DRIFT")
            events.append(row)
            counts[row["study_status"]] += 1
            states.update(row[n + "_status"] for n in MODELS)
    if len({r["event_id"] for r in events}) != len(events):
        raise ValueError("DUPLICATE_EPISODE_WEIGHT")
    common.write_csv(outdir / "DECISIONS.csv.gz", events)
    # Reuse the existing immutable Ops artifact owner, with actual 2026 publication.
    # Replay-time fields remain inside diagnostic metadata; no receipt is backdated.
    repo = OpsRepository(ops_path)
    audit = legal_input_audit({})
    actual = int(dt.datetime.now(dt.UTC).timestamp() * common.SECOND)
    try:
        for row in events:
            metadata = {
                "historical_price_origin": row,
                "P1_input_audit": audit,
                "authority": "ZERO",
                "source_grade": "RECONSTRUCTED_MARKET",
                "actual_publication_ns": actual,
            }
            ref = common.digest(metadata)
            repo.register_artifact(
                ArtifactIndexEntryV2(ref, "HistoricalPriceOriginDiagnosticV1", ref, actual, actual, metadata)
            )
    finally:
        repo.close()
    summary = {
        "version": "SEALED_HISTORICAL_DECISIONS_V1",
        "evidence_grade": common.GRADE,
        "begin_ns": begin,
        "final_ns": final,
        "expected_native_symbol_slots": (final - begin) // common.M15 * len(market.symbols),
        "original_first_events": len(events),
        "first_event_status_counts": dict(counts),
        "model_status_counts": dict(states),
        "decisions_sha256": common.sha(outdir / "DECISIONS.csv.gz"),
        "calendar_sha256": common.sha(calendar),
        "models_sha256": common.sha(common.HERE / "PRICE_MODELS.json"),
        "protocol_sha256": common.PROTOCOL_SHA,
        "source_manifest_sha256": common.sha(common.HERE / "SOURCE_OBJECTS.jsonl"),
        "P1_input_audit": audit,
        "frozen_approved_actions": 0,
        "qualified_Agent_B_requests": 0,
        "outcomes_read": False,
    }
    common.save(outdir / "DECISION_SUMMARY.json", summary)
    common.save(
        outdir / "REPLAY_RESOURCES.json",
        {
            "actual_created_at": dt.datetime.now(dt.UTC).isoformat(),
            "wall_seconds": time.monotonic() - started,
            "max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            "pure_decision_count": len(inference),
            "pure_decision_total_seconds": sum(inference),
            "pure_decision_max_seconds": max(inference, default=0),
            "actual_Ops_publication_ns": actual,
        },
    )
    return summary


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--old-cache", type=pathlib.Path, required=True)
    p.add_argument("--new-cache", type=pathlib.Path, required=True)
    p.add_argument("--outdir", type=pathlib.Path, required=True)
    p.add_argument("--ops-path", type=pathlib.Path, required=True)
    args = p.parse_args()
    print(common.canonical(run(args.old_cache, args.new_cache, args.outdir, args.ops_path)))
