"""Exact seeded recursive indicators stay bounded through tails and restarts."""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.active_history import (
    DAY_NS,
    MAX_ADVANCE_BARS,
    MAX_INPUT_REFS,
    TAIL_LIMITS,
    ActiveCausalHistoryStateV1,
    HistoryInvalidationRequired,
    HistoryTailItemV1,
    advance,
)
from atlas.v2.data.bars import BarIntervalV2
from atlas.v2.data.history import IndexedCausalBarV2
from atlas.v2.data.raw import AvailabilityClassV2
from atlas.v2.features.technical import atr, ema

from .test_session014_core import KEY, bar


def indexed(index, interval=BarIntervalV2.M15):
    price = 100 + ((index % 101) - 50) / 10 + (index % 7) / 1000
    item = bar(index, interval=interval, close=str(price))
    ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": item.raw.record_id})
    return IndexedCausalBarV2(item, ref)


@pytest.mark.parametrize("interval", tuple(TAIL_LIMITS))
def test_exact_full_prefix_equivalence_beyond_two_tail_windows(interval):
    count = 2 * TAIL_LIMITS[interval] + 17
    inputs = tuple(indexed(i, interval) for i in range(count))
    closes = tuple(float(item.bar.close) for item in inputs)
    expected20, expected50, expected_atr = ema(closes, 20), ema(closes, 50), atr(tuple(x.bar for x in inputs))
    state = None
    for offset in range(0, count, MAX_ADVANCE_BARS):
        previous = state
        state = advance(state, inputs[offset:offset + MAX_ADVANCE_BARS], key=KEY, interval=interval)
        assert len(state.tail) <= TAIL_LIMITS[interval]
        assert len(state.input_refs) <= MAX_INPUT_REFS
        if previous:
            assert state.previous_state_ref == previous.content_hash
        stop = min(offset + MAX_ADVANCE_BARS, count)
        assert (state.ema20, state.ema50, state.atr14) == (
            expected20[stop - 1], expected50[stop - 1], expected_atr[stop - 1])
    assert state is not None
    start = count - len(state.tail)
    assert tuple(x.ema20 for x in state.tail) == expected20[start:]
    assert tuple(x.ema50 for x in state.tail) == expected50[start:]
    assert tuple(x.atr14 for x in state.tail) == expected_atr[start:]
    assert state.total_count == count
    assert state.observed_unique_utc_close_days == len({x.bar.close_at_ns // DAY_NS for x in inputs})
    assert state.last_close_at_ns == inputs[-1].bar.close_at_ns
    assert state.last_bar_ref == inputs[-1].bar.content_hash
    assert state.last_observation_index_ref == inputs[-1].observation_index_ref


def test_serialized_restart_preserves_seeds_exactly_and_hash_is_immutable():
    inputs = tuple(indexed(i) for i in range(3100))
    state = None
    for offset in range(0, 3000, 128):
        state = advance(state, inputs[offset:min(offset + 128, 3000)], key=KEY, interval=BarIntervalV2.M15)
    assert state is not None
    wire = json.loads(json.dumps(state.to_dict()))
    restored = ActiveCausalHistoryStateV1.from_dict(wire)
    assert restored.content_hash == state.content_hash
    assert restored.to_dict() == state.to_dict()
    actual = advance(restored, inputs[3000:], key=KEY, interval=BarIntervalV2.M15)
    expected = advance(state, inputs[3000:], key=KEY, interval=BarIntervalV2.M15)
    assert actual == expected
    wire["ema20"] += 0.1
    with pytest.raises(ValueError, match="checksum"):
        ActiveCausalHistoryStateV1.from_dict(wire)
    assert state.total_count == 3000


def test_warmup_seed_boundaries_and_empty_increment_are_explicit():
    state = None
    for i in range(50):
        state = advance(state, (indexed(i),), key=KEY, interval=BarIntervalV2.M15)
        assert (state.ema20 is None) == (i < 19)
        assert (state.ema50 is None) == (i < 49)
        assert (state.atr14 is None) == (i < 13)
        restored = ActiveCausalHistoryStateV1.from_dict(state.to_dict())
        assert restored == state
    assert advance(state, (), key=KEY, interval=BarIntervalV2.M15) is state
    with pytest.raises(ValueError, match="bootstrap"):
        advance(None, (), key=KEY, interval=BarIntervalV2.M15)


def test_late_revision_duplicate_or_reordered_page_requires_explicit_rebuild():
    inputs = tuple(indexed(i) for i in range(60))
    state = advance(None, inputs, key=KEY, interval=BarIntervalV2.M15)
    for incoming in ((inputs[-1],), (inputs[3],), (indexed(61), indexed(60))):
        with pytest.raises(HistoryInvalidationRequired) as failure:
            advance(state, incoming, key=KEY, interval=BarIntervalV2.M15)
        assert failure.value.reason_code == "NON_APPEND_SOURCE_REVISION_REQUIRES_REBUILD"
    assert state.total_count == 60


def test_gaps_are_retained_and_counted_without_manufacturing_bars_or_days():
    inputs = tuple(indexed(i) for i in (0, 1, 1000, 1001))
    state = advance(None, inputs, key=KEY, interval=BarIntervalV2.M15)
    assert state.total_count == 4 and len(state.tail) == 4
    assert state.observed_unique_utc_close_days == 2
    assert state.bars == tuple(x.bar for x in inputs)
    assert state.last_utc_close_day == inputs[-1].bar.close_at_ns // DAY_NS


def test_work_and_wire_budgets_identity_and_authority_are_strict():
    state = advance(None, (indexed(0),), key=KEY, interval=BarIntervalV2.M15)
    with pytest.raises(ValueError, match="128-bar"):
        advance(state, tuple(indexed(i) for i in range(1, 130)), key=KEY, interval=BarIntervalV2.M15)
    for key in (replace(KEY, native_symbol="ETHUSDT"), replace(KEY, contract_revision="f" * 64)):
        with pytest.raises(ValueError, match="full-key"):
            advance(state, (indexed(1),), key=key, interval=BarIntervalV2.M15)
    with pytest.raises(ValueError, match="full-key"):
        advance(state, (indexed(1),), key=KEY, interval=BarIntervalV2.H1)
    with pytest.raises(ValueError, match="source identity"):
        advance(None, (indexed(0),), key=replace(KEY, contract_revision="f" * 64), interval=BarIntervalV2.M15)
    with pytest.raises(ValueError, match="unknown fields"):
        ActiveCausalHistoryStateV1.from_dict({**state.to_dict(), "extra": 1})
    for field, value in (("authority", "CAPITAL"), ("algorithm_version", "OTHER"),
                         ("availability_class", "RECONSTRUCTED_MARKET")):
        wire = {**state.to_dict(), field: value}
        with pytest.raises(ValueError, match="unsupported"):
            ActiveCausalHistoryStateV1.from_dict(wire)
    with pytest.raises(ValueError, match="wire tail"):
        ActiveCausalHistoryStateV1.from_dict({**state.to_dict(), "tail": [{}] * (TAIL_LIMITS[state.interval] + 1)})
    with pytest.raises(ValueError, match="finite"):
        replace(state.tail[0], ema20=float("nan"))
    with pytest.raises(ValueError, match="count"):
        replace(state, total_count=True)
    with pytest.raises(ValueError, match="locator"):
        replace(state.tail[0], observation_index_ref="a" * 64)


def test_reconstructed_market_and_forming_bars_never_enter_actual_state():
    item = indexed(0)
    reconstructed = replace(item.bar.raw, availability_class=AvailabilityClassV2.RECONSTRUCTED_MARKET,
                            replay_available_at_ns=item.bar.close_at_ns)
    with pytest.raises(ValueError, match="ACTUAL_SYSTEM"):
        advance(None, (replace(item, bar=replace(item.bar, raw=reconstructed)),),
                key=KEY, interval=BarIntervalV2.M15)
    with pytest.raises(ValueError, match="final"):
        HistoryTailItemV1(replace(item.bar, final=False), item.observation_index_ref, None, None, None)


def test_every_middle_tail_indicator_and_raw_receipt_is_identity_bound():
    state = advance(None, tuple(indexed(i) for i in range(70)), key=KEY, interval=BarIntervalV2.M15)
    original = state.to_dict()
    middle = 55
    for field in ("ema20", "ema50", "atr14"):
        tampered = deepcopy(original)
        tampered["tail"][middle][field] += .25
        with pytest.raises(ValueError, match="tail checksum"):
            ActiveCausalHistoryStateV1.from_dict(tampered)
        changed_item = replace(state.tail[middle], **{field: getattr(state.tail[middle], field) + .25})
        changed = replace(state, tail=state.tail[:middle] + (changed_item,) + state.tail[middle + 1:])
        assert changed.content_hash != state.content_hash
    tampered = deepcopy(original)
    tampered["tail"][middle]["raw"]["available_at_ns"] += 1
    with pytest.raises(ValueError, match="tail checksum"):
        ActiveCausalHistoryStateV1.from_dict(tampered)
    # A substituted ordered-tail digest also has to match the overall state.
    changed_wire = changed.to_dict()
    substituted_digest = {**original, "tail_hash": changed_wire["tail_hash"], "tail": changed_wire["tail"]}
    with pytest.raises(ValueError, match="state checksum"):
        ActiveCausalHistoryStateV1.from_dict(substituted_digest)
    assert ActiveCausalHistoryStateV1.from_dict(original) == state


@pytest.mark.parametrize("tail", ([], [None], [1], ["bar"], [True], {}))
def test_invalid_empty_or_non_object_wire_tail_is_explicitly_rejected(tail):
    state = advance(None, (indexed(0),), key=KEY, interval=BarIntervalV2.M15)
    wire = {**state.to_dict(), "tail": tail}
    with pytest.raises(ValueError, match="wire tail"):
        ActiveCausalHistoryStateV1.from_dict(wire)


def test_wire_tail_summary_count_rejects_boolean_and_wrong_source_locator():
    state = advance(None, (indexed(0),), key=KEY, interval=BarIntervalV2.M15)
    with pytest.raises(ValueError, match="tail identity"):
        ActiveCausalHistoryStateV1.from_dict({**state.to_dict(), "tail_count": True})
    bad = replace(indexed(0), observation_index_ref="a" * 64)
    with pytest.raises(ValueError, match="locator"):
        advance(None, (bad,), key=KEY, interval=BarIntervalV2.M15)
    with pytest.raises(ValueError, match="typed previous"):
        advance(object(), (), key=KEY, interval=BarIntervalV2.M15)
