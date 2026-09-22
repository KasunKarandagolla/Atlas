from __future__ import annotations

from support.scanner_fixture import make_ranked

from atlas.scanner import CAPITAL_ENABLED_INSTRUMENTS, DEFAULT_DEEP_K, DEFAULT_TOP_K, select_candidates


def test_top_k_and_deep_budget_are_frozen():
    ranked = make_ranked([(f"I{index:02d}", float(100 - index)) for index in range(1, 9)])
    selections = {item.instrument: item for item in select_candidates(ranked)}
    assert (DEFAULT_TOP_K, DEFAULT_DEEP_K) == (3, 5)
    assert [item.instrument for item in selections.values() if item.top_k_selected] == ["I01", "I02", "I03"]
    assert [item.instrument for item in selections.values() if item.deep_selected] == ["I01", "I02", "I03", "I04", "I05"]
    assert selections["I01"].selection_reason == "TOP_K"
    assert selections["I04"].selection_reason == "DEEP_BUDGET"
    assert selections["I08"].selection_reason == "NOT_SELECTED"


def test_capital_instruments_stay_deep_warm_regardless_of_scanner_rank():
    ranked = make_ranked([("BTCUSDT", 0.1), ("ETHUSDT", 0.2)] + [(f"I{index:02d}", float(100 - index))
                                                                 for index in range(1, 8)])
    selections = {item.instrument: item for item in select_candidates(ranked)}
    for instrument in CAPITAL_ENABLED_INSTRUMENTS:
        assert selections[instrument].deep_selected is True
    assert selections["BTCUSDT"].rank > 3
    assert selections["BTCUSDT"].selection_reason == "CAPITAL_WARM"
