"""Bounded feature contexts retain exact policy EMA/ATR and full prior-day range."""
from dataclasses import replace

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.active_history import advance
from atlas.v2.data.bars import BarIntervalV2
from atlas.v2.features.joins import JoinedBars
from atlas.v2.features.pipeline import EXACT_PREFIX_FEATURE_SET_VERSION, FEATURE_SET_VERSION, feature_snapshot
from atlas.v2.features.technical import atr, ema

from .test_session014_core import KEY
from .test_session037_active_history import indexed


def _case():
    states = {}
    frames = {}
    for frame in (BarIntervalV2.M15,BarIntervalV2.H1,BarIntervalV2.H4):
        inputs = tuple(indexed(i,frame) for i in range(310))
        state = None
        for start in range(0,len(inputs),128):
            state = advance(state,inputs[start:start+128],key=KEY,interval=frame)
        states[frame] = state
        frames[frame] = tuple(item.bar for item in inputs)
    cutoff = max(item[-1].raw.available_at_ns for item in frames.values())
    join = JoinedBars(KEY,cutoff,frames[BarIntervalV2.H4][-100:],
        frames[BarIntervalV2.H1][-100:],frames[BarIntervalV2.M15][-192:],"AVAILABLE",None,sha256_json("health"))
    return join,states,frames


def test_policy_features_use_exact_prefix_indicators_and_bound_checkpoint_refs():
    join,states,frames = _case()
    result = feature_snapshot(join,exact_histories=states)
    legacy = feature_snapshot(join)
    assert result.feature_set_version == EXACT_PREFIX_FEATURE_SET_VERSION
    assert legacy.feature_set_version == FEATURE_SET_VERSION
    for label,frame in (("m15",BarIntervalV2.M15),("h1",BarIntervalV2.H1),("h4",BarIntervalV2.H4)):
        assert result.values[label+".atr14"].value == atr(frames[frame])[-1]
        for period in (20,50):
            assert result.values[label+".ema"+str(period)].value == ema(
                [float(bar.close) for bar in frames[frame]],period)[-1]
        assert states[frame].content_hash in result.envelope.input_refs
    assert result.values["m15.atr14"].value != legacy.values["m15.atr14"].value
    assert result.content_hash != legacy.content_hash


def test_exact_prefix_feature_context_refuses_mismatched_tail_or_incomplete_manifest():
    join,states,_frames = _case()
    with pytest.raises(ValueError,match="checkpoint"):
        feature_snapshot(replace(join,m15=join.m15[:-1]),exact_histories=states)
    with pytest.raises(ValueError,match="checkpoint"):
        feature_snapshot(join,exact_histories={})
