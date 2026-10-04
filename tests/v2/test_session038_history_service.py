"""Verified history reuse preserves strict recovery and bounded servicing."""
from __future__ import annotations

import json
from dataclasses import replace

import pytest

from atlas.v2.data.active_history import advance
from atlas.v2.data.bars import BarIntervalV2
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.runtime import active_history

from .test_session037_active_history import KEY, indexed


def head(state):
    return {"state_json": json.dumps(state.to_dict()), "state_ref": state.content_hash}


def test_verified_head_reuses_only_exact_wire_and_services_strict_restart(tmp_path, monkeypatch):
    state = advance(None, tuple(indexed(i) for i in range(96)), key=KEY, interval=BarIntervalV2.M15)
    wire = head(state)
    calls = []
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        decoded = active_history._decode_head(repo, wire, key=KEY, interval=BarIntervalV2.M15,
                                             service=lambda: calls.append(1))
        assert decoded is not None and decoded.content_hash == state.content_hash
        assert len(calls) >= 4
        assert decoded.to_dict() == state.to_dict()
        monkeypatch.setattr(active_history.ActiveCausalHistoryStateV1, "from_dict",
                            lambda value: pytest.fail("exact verified head was decoded again"))
        assert active_history._decode_head(repo, wire, key=KEY, interval=BarIntervalV2.M15,
                                          service=None) is decoded
    monkeypatch.undo()
    with OpsRepository(tmp_path / "ops.sqlite") as reopened:
        restored = active_history._decode_head(reopened, wire, key=KEY, interval=BarIntervalV2.M15,
                                              service=None)
        assert restored is not decoded
        assert restored is not None and restored.content_hash == state.content_hash
        bad = json.loads(wire["state_json"])
        bad["tail"][20]["ema20"] = 777.0
        with pytest.raises(ValueError, match="checksum"):
            active_history._decode_head(reopened, {**wire, "state_json": json.dumps(bad)},
                                       key=KEY, interval=BarIntervalV2.M15, service=None)


def test_verified_head_cache_is_bounded_by_repository_lifetime(tmp_path):
    state = advance(None, (indexed(0),), key=KEY, interval=BarIntervalV2.M15)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        for ordinal in range(12):
            revised = replace(state, key=replace(KEY, native_symbol=f"SYMBOL{ordinal}USDT"))
            active_history._remember_head(repo, head(revised), revised)
        assert len(active_history._VERIFIED_HEADS[repo]) == active_history.MAX_VERIFIED_HISTORY_HEADS_V1
