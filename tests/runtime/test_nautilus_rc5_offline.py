from __future__ import annotations

import pytest

from atlas.runtime.nautilus_boundary import build_offline_rc5_bybit_configs


def test_exact_rc5_bybit_configs_construct_without_transport():
    try:
        configs = build_offline_rc5_bybit_configs()
    except ModuleNotFoundError:
        pytest.skip("exact Nautilus rc5 is not installed in this interpreter")
    assert configs.data.environment.name.lower() == "testnet"
    assert configs.execution.environment.name.lower() == "testnet"
    assert [product.name.upper() for product in configs.data.product_types] == ["LINEAR"]
    assert [product.name.upper() for product in configs.execution.product_types] == ["LINEAR"]
    assert configs.execution.margin_mode.name == "ISOLATED_MARGIN"
