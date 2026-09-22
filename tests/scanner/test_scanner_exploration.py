from __future__ import annotations

from support.scanner_fixture import EPOCH_NS, make_ranked

from atlas.scanner import SLOT_NS, RankBand, select_exploration


def _band_rows(counts: dict[RankBand, int], *, slot_at_ns: int):
    pairs = []
    index = 1
    for _band, count in counts.items():
        for _ in range(count):
            pairs.append((f"I{index:02d}", float(1000 - index)))
            index += 1
    return make_ranked(pairs, slot_at_ns=slot_at_ns)


def test_exploration_band_rotates_by_utc_slot_modulo_three():
    ranked = _band_rows({RankBand.TOP_3: 3, RankBand.B1: 2, RankBand.B2: 2, RankBand.B3: 2},
                        slot_at_ns=EPOCH_NS)
    slots = (EPOCH_NS, EPOCH_NS + SLOT_NS, EPOCH_NS + 2 * SLOT_NS)
    bands = [select_exploration(ranked, policy_version="p", universe_hash="u", slot_at_ns=slot).preferred_band
             for slot in slots]
    assert bands == [RankBand.B1, RankBand.B2, RankBand.B3]


def test_exploration_hash_is_stable_and_known_probability_is_recorded():
    ranked = _band_rows({RankBand.TOP_3: 3, RankBand.B1: 4}, slot_at_ns=EPOCH_NS)
    first = select_exploration(ranked, policy_version="p", universe_hash="u", slot_at_ns=EPOCH_NS)
    second = select_exploration(ranked, policy_version="p", universe_hash="u", slot_at_ns=EPOCH_NS)
    assert first == second
    assert first.band is RankBand.B1
    assert first.inclusion_probability == 0.25
    assert first.selection_key is not None
    assert first.instrument in {item.instrument for item in ranked if item.rank_band is RankBand.B1}


def test_empty_preferred_band_uses_cyclic_fallback():
    slot = EPOCH_NS + 2 * SLOT_NS
    ranked = _band_rows({RankBand.TOP_3: 3, RankBand.B1: 2}, slot_at_ns=slot)
    selection = select_exploration(ranked, policy_version="p", universe_hash="u", slot_at_ns=slot)
    assert selection.preferred_band is RankBand.B3
    assert selection.band is RankBand.B1
    assert selection.fallback_path == "B3_EMPTY->B1"
    assert selection.inclusion_probability == 0.5
