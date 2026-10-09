import json

import pytest

from atlas.v2.instruments import InstrumentKeyV2
from atlas.v2.runtime.owner_pairs import (
    MAX_OWNER_PAIR_CONFIGURATION_BYTES,
    OwnerPairConfigurationV1,
    read_owner_pair_configuration,
)
from atlas.v2.strategies.s8_pairs import S8PairDefinitionV2


def _pair() -> S8PairDefinitionV2:
    a = InstrumentKeyV2.from_dict({
        "schema_version": 1, "venue": "BYBIT", "environment": "MAINNET",
        "product": "LINEAR_PERPETUAL", "native_symbol": "BTCUSDT",
        "base_asset_id": "BTC", "quote_asset": "USDT", "settlement_asset": "USDT",
        "contract_revision": "bybit-linear-v1",
    })
    b = InstrumentKeyV2.from_dict({
        "schema_version": 1, "venue": "BYBIT", "environment": "MAINNET",
        "product": "LINEAR_PERPETUAL", "native_symbol": "ETHUSDT",
        "base_asset_id": "ETH", "quote_asset": "USDT", "settlement_asset": "USDT",
        "contract_revision": "bybit-linear-v1",
    })
    return S8PairDefinitionV2("BTC-ETH", "BTC versus ETH linear perpetual relative value",
        a, b, "OLS_LOG_PRICE_A_ON_LOG_PRICE_B_30D_HOURLY_V1",
        "LOG_A_MINUS_ALPHA_MINUS_BETA_LOG_B")


def _body(pair=None):
    return {"version": "S8_OWNER_PAIR_CONFIGURATION_V1", "owner_id": "research-owner",
        "pairs": [(pair or _pair()).to_dict()]}


def test_reads_exact_owner_pair_and_hashes_canonical_content(tmp_path):
    path = tmp_path / "owner-pairs.json"
    path.write_text(json.dumps(_body(), indent=2), encoding="utf-8")
    config = read_owner_pair_configuration(path)
    assert isinstance(config, OwnerPairConfigurationV1)
    assert config.pairs == (_pair(),)
    assert config.to_dict() == _body()
    assert config.content_hash == OwnerPairConfigurationV1("research-owner", (_pair(),)).content_hash


def test_run_copies_and_binds_owner_pair_configuration(tmp_path):
    from atlas.v2.product import ResearchRunConfigV2, create_run, load_run

    source = tmp_path / "selected-pairs.json"
    source.write_text(json.dumps(_body()), encoding="utf-8")
    parsed = read_owner_pair_configuration(source)
    config = ResearchRunConfigV2(data_root=str(tmp_path.resolve()),
        owner_pair_configuration_hash=parsed.content_hash)
    run = create_run(tmp_path, config, owner_pair_configuration_path=source)
    assert load_run(run)["configuration"]["owner_pair_configuration_hash"] == parsed.content_hash
    source.write_text(json.dumps({**_body(), "owner_id": "other"}), encoding="utf-8")
    assert read_owner_pair_configuration(run / "owner-pairs.json").owner_id == "research-owner"
    assert load_run(run)["capital_enabled"] is False
    (run / "owner-pairs.json").write_text(source.read_text(), encoding="utf-8")
    with pytest.raises(ValueError, match="pair configuration drift"):
        load_run(run)


def test_run_requires_matching_pair_configuration_before_creating_evidence(tmp_path):
    from atlas.v2.product import ResearchRunConfigV2, create_run

    config = ResearchRunConfigV2(data_root=str(tmp_path.resolve()),
        owner_pair_configuration_hash=OwnerPairConfigurationV1("research-owner", (_pair(),)).content_hash)
    with pytest.raises(ValueError, match="immutable run configuration"):
        create_run(tmp_path, config)
    assert not (tmp_path / "runs").exists()


@pytest.mark.parametrize("body", [
    {**_body(), "trade_plan": {}},
    {**_body(), "capital_authority": "SOME"},
    {**_body(), "version": "OTHER"},
])
def test_rejects_unknown_or_unsupported_top_level_fields(tmp_path, body):
    path = tmp_path / "owner-pairs.json"
    path.write_text(json.dumps(body), encoding="utf-8")
    with pytest.raises(ValueError):
        read_owner_pair_configuration(path)


def test_rejects_duplicate_ids_and_unrecognized_pair_fields(tmp_path):
    path = tmp_path / "owner-pairs.json"
    pair = _pair().to_dict()
    path.write_text(json.dumps({**_body(), "pairs": [pair, pair]}), encoding="utf-8")
    with pytest.raises(ValueError, match="unique"):
        read_owner_pair_configuration(path)
    path.write_text(json.dumps({**_body(), "pairs": [{**pair, "source": "module.py"}]}), encoding="utf-8")
    with pytest.raises(ValueError, match="unknown fields"):
        read_owner_pair_configuration(path)


def test_rejects_duplicate_json_keys_floats_and_invalid_utf8(tmp_path):
    path = tmp_path / "owner-pairs.json"
    path.write_text('{"version":"S8_OWNER_PAIR_CONFIGURATION_V1","owner_id":"a",'
        '"owner_id":"b","pairs":[]}', encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate JSON"):
        read_owner_pair_configuration(path)
    path.write_text(json.dumps({**_body(), "extra": 0.25}), encoding="utf-8")
    with pytest.raises(ValueError, match="floating-point"):
        read_owner_pair_configuration(path)
    path.write_bytes(b"\xff")
    with pytest.raises(ValueError, match="UTF-8"):
        read_owner_pair_configuration(path)


def test_rejects_symlink_oversize_and_invalid_economic_definition(tmp_path):
    real = tmp_path / "real.json"
    real.write_text(json.dumps(_body()), encoding="utf-8")
    link = tmp_path / "owner-pairs.json"
    link.symlink_to(real)
    with pytest.raises(ValueError, match="symlink"):
        read_owner_pair_configuration(link)
    link.unlink()
    link.write_bytes(b" " * (MAX_OWNER_PAIR_CONFIGURATION_BYTES + 1))
    with pytest.raises(ValueError, match="131072 bytes"):
        read_owner_pair_configuration(link)
    pair = _pair().to_dict()
    pair["economic_pair_definition"] = ""
    link.write_text(json.dumps({**_body(), "pairs": [pair]}), encoding="utf-8")
    with pytest.raises(ValueError):
        read_owner_pair_configuration(link)


def test_configuration_hash_changes_when_owner_source_changes(tmp_path):
    path = tmp_path / "owner-pairs.json"
    path.write_text(json.dumps(_body()), encoding="utf-8")
    first = read_owner_pair_configuration(path)
    changed = _pair().to_dict()
    changed["economic_pair_definition"] = "Owner's different economic hypothesis"
    path.write_text(json.dumps({**_body(), "pairs": [changed]}), encoding="utf-8")
    second = read_owner_pair_configuration(path)
    assert first.content_hash != second.content_hash
    assert first.pairs[0].content_hash != second.pairs[0].content_hash
