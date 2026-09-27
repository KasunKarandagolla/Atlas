from __future__ import annotations

import importlib.metadata
import inspect
import json
from pathlib import Path

import nautilus_trader.adapters.binance as binance
import nautilus_trader.adapters.bybit as bybit

from atlas.runtime.phase4_v2 import (
    BINANCE_REQUIRED_CAPABILITIES_V2,
    BYBIT_REQUIRED_CAPABILITIES_V2,
    required_capabilities_v2,
)
from atlas.v2.instruments import VenueV2


def test_installed_nautilus_distribution_is_the_frozen_rc5():
    assert importlib.metadata.version("nautilus_trader") == "2.0.0rc5"


def test_installed_bybit_native_tpsl_api_exposes_full_mark_stop_and_reduce_only():
    params = bybit.BybitNativeTpSlParams
    fields = inspect.signature(params).parameters
    assert {"stop_loss", "sl_trigger_by", "sl_order_type", "tpsl_mode"} <= set(fields)
    instance = params(stop_loss="48000", sl_trigger_by="MarkPrice", sl_order_type="Market", tpsl_mode="Full")
    assert instance is not None

    stub = Path(bybit.__file__).with_suffix(".pyi").read_text(encoding="utf-8")
    assert "reduce_only: bool = False" in stub
    assert "native_tp_sl: BybitNativeTpSlParams | None = None" in stub
    assert "position_idx: BybitPositionIdx | None = None" in stub


def test_installed_binance_config_has_no_attached_native_stop_contract():
    params = inspect.signature(binance.BinanceExecutionClientConfig).parameters
    assert {"futures_leverages", "futures_margin_types"} <= set(params)
    assert not ({"native_tp_sl", "stop_loss", "take_profit", "attached_stop"} & set(params))
    assert "algoOrder" not in Path(binance.__file__).with_suffix(".pyi").read_text(encoding="utf-8")


def test_bybit_and_binance_have_separate_complete_phase4_capability_matrices():
    assert required_capabilities_v2(VenueV2.BYBIT) == BYBIT_REQUIRED_CAPABILITIES_V2
    assert required_capabilities_v2(VenueV2.BINANCE) == BINANCE_REQUIRED_CAPABILITIES_V2
    assert "full_position_stop_visibility" in BYBIT_REQUIRED_CAPABILITIES_V2
    assert "venue_native_entry_protection" in BINANCE_REQUIRED_CAPABILITIES_V2


def test_committed_venue_artifact_never_upgrades_unqualified_profiles():
    artifact = json.loads(Path("docs/v2/SESSION024_VENUE_CAPABILITIES.json").read_text(encoding="utf-8"))
    assert artifact["profile_contract"]["profile_instance_status"] == "BLOCKED BY ENVIRONMENT"
    assert artifact["profile_contract"]["account_identity_hash"] is None
    assert artifact["profile_contract"]["profile_hash"] is None
    for profile in artifact["profiles"]:
        assert profile["capital_capable"] is False
        assert profile["qualification_status"] == "UNVERIFIED"
        assert all(not row["evidence_refs"] for row in profile["rows"])
