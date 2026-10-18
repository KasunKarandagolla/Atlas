"""Read-only continuation audit; reuse the foundation's fixed trace audit."""

from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("foundation_audit", HERE.parent / "atlas-red-native-trace/audit.py")
foundation = importlib.util.module_from_spec(spec)
spec.loader.exec_module(foundation)


def audit(directory, db=None):
    result = foundation.audit(directory, db)
    # All 12 native snapshots use complete prior UTC days and cannot silently
    # consume actual-only trade visibility under reconstructed replay.
    for item in result["native_full_feature_observations"]:
        values = item["values"]
        for field in ("location.prior_day_high", "location.prior_day_low"):
            if values[field]["value"] is None or values[field]["missing_reason"] is not None:
                raise ValueError("REPLAY_PRIOR_DAY_NOT_REPRODUCED")
        value = values["location.utc_day_trade_vwap_partial_to_cutoff"]
        if value["value"] is not None or value["missing_reason"] != "REPLAY_TRADE_AVAILABILITY_UNAVAILABLE":
            raise ValueError("UNQUALIFIED_REPLAY_TRADE_VISIBILITY")
    result["continuation_version"] = "CAUSAL_REPLAY_QUALIFICATION_AUDIT_V1"
    result["reconstructed_prior_day_values"] = 2 * len(result["native_full_feature_observations"])
    return result


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--evidence", type=Path, required=True)
    p.add_argument("--db", type=Path)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    a.output.write_text(json.dumps(audit(a.evidence, a.db), sort_keys=True, indent=2) + "\n")


if __name__ == "__main__":
    main()
