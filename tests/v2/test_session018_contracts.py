"""Session-018 capability and policy-preservation checks."""
import json
from pathlib import Path

from atlas.v2.selection import SELECTION_POLICY_HASH
from atlas.v2.strategies.s1_trend import S1_POLICY
from atlas.v2.strategies.s2_breakout import S2_POLICY


def test_capability_matrix_and_frozen_hashes():
    matrix = json.loads(Path("docs/v2/EVIDENCE_CAPABILITY_MATRIX_V1.json").read_text())
    assert matrix["schema_version"] == 1
    required = {"L2_BBO", "L2_DEPTH", "TRADES_AGGRESSOR", "OPEN_INTEREST", "FUNDING_CURRENT_PREDICTED",
        "FUNDING_SETTLED", "BASIS_MARK_INDEX_LAST", "LIQUIDATIONS", "NEWS_EVENT_RAW", "EVENT_EXTRACTION",
        "INSTRUMENT_FILTER_UNIVERSE", "FEE_REVISION", "COLLECTOR_HEALTH", "CLOCK_LATENCY",
        "OFFLINE_RECONNECT", "INCLUSION_RETENTION"}
    assert {row["family"] for row in matrix["rows"]} == required
    fields = {"source_channel", "instrument_product_scope", "fields_units", "cadence_resolution",
        "sequence_update_semantics", "timestamp_semantics", "raw_payload_retention", "gap_reset_reconnect",
        "current_coverage", "known_limitations", "permitted_uses", "prohibited_uses", "status"}
    assert all(fields <= row.keys() and all(row[key] for key in fields) for row in matrix["rows"])
    assert S1_POLICY.policy_hash == "c559659ace0239200f7d26d81a24b489a8a4ee0bc849b6954faf901126b5dff0"
    assert S2_POLICY.policy_hash == "fbcdacd8ec6a79ea2595fa367d220b55b1d84c326ec3a28d787d23f356062dcd"
    assert SELECTION_POLICY_HASH == "36f8fd58c9e791ea580a1855f9e98555131e0828604d484eda3df4c7d5529cac"
