from __future__ import annotations

from support.scanner_fixture import EPOCH_NS, cheap_inputs_for, return_history

from atlas.scanner import rank_observations
from atlas.scanner.cheap_scan import cheap_scan


def test_ranking_is_deterministic_with_canonical_instrument_tie_break():
    inputs = cheap_inputs_for(EPOCH_NS)
    observations = cheap_scan(inputs, scanner_policy_version="SCANNER_BASELINE_V1")
    histories = {item.instrument: return_history(item.instrument) for item in inputs}
    first = rank_observations(observations, universe_hash="u", return_histories=histories)
    second = rank_observations(tuple(reversed(observations)), universe_hash="u", return_histories=histories)
    assert first == second
    assert [item.instrument for item in first] == ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT",
                                                   "DOGEUSDT", "AVAXUSDT"]

    tied = cheap_scan((inputs[0],), scanner_policy_version="SCANNER_BASELINE_V1")
    from atlas.scanner.models import CheapScanObservation

    other = CheapScanObservation(tied[0].slot_at_ns, tied[0].scanner_policy_version, tied[0].scorer_version,
                                 "AAAUSDT", tied[0].availability_cutoff_ns, tied[0].return_24h,
                                 tied[0].volatility_24h, None, tied[0].score, "tie-hash")
    ranked = rank_observations((tied[0], other), universe_hash="u")
    assert [item.instrument for item in ranked] == ["AAAUSDT", "BTCUSDT"]


def test_correlation_clusters_are_deterministic_and_past_only():
    histories = {instrument: return_history(instrument) for instrument in ("BTCUSDT", "ETHUSDT", "SOLUSDT")}
    first = rank_observations(cheap_scan(cheap_inputs_for(EPOCH_NS), scanner_policy_version="p"),
                              universe_hash="u", return_histories=histories)
    second = rank_observations(cheap_scan(cheap_inputs_for(EPOCH_NS), scanner_policy_version="p"),
                               universe_hash="u", return_histories=histories)
    assert {item.instrument: item.correlation_cluster for item in first} == {
        item.instrument: item.correlation_cluster for item in second
    }
    assert all(item.correlation_cluster.startswith("cluster-") for item in first)
