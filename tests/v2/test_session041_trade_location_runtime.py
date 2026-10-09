from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.data.raw import AvailabilityClassV2
from atlas.v2.data.trade_completeness import (
    EXACT_SEQUENCE_SEMANTICS_V1,
    NativeTradePrefixV1,
    PersistedTradeEvidenceCheckV1,
    SequencedNativeTradeV1,
    TradeSourceContractV1,
    TradeWindowEvidenceV1,
)
from atlas.v2.features.candles import DAY_NS, CausalTradeAnchor, TradeLocationConfig
from atlas.v2.features.joins import JoinedBars
from atlas.v2.features.pipeline import feature_snapshot
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    VenueV2,
)
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.runtime import production
from atlas.v2.runtime import trade_location_inputs as adapter
from atlas.v2.runtime.trade_location_inputs import TradeLocationInputsV1, persist_trade_location_input_receipt

CUTOFF = 1_750_000_100_000_000_000
START = CUTOFF - 60_000_000_000
KEY = InstrumentKeyV2(
    VenueV2.BYBIT, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
    "BTCUSDT", "BTC", "USDT", "USDT", "a" * 64,
)


def ref(value: object) -> str:
    return sha256_json({"trade_location_runtime_test": value})


@dataclass(frozen=True)
class Translation:
    record_id: str
    raw_payload_hash: str
    event_at_ns: int
    sequence: int


def observation(*, event_type: str = "TRADE", event_at_ns: int = START,
                available_at_ns: int = START + 10, raw_payload: bytes = b"payload",
                record_id: str = "r1", event_enum: AvailabilityClassV2 = AvailabilityClassV2.ACTUAL_SYSTEM):
    obs = SimpleNamespace(
        availability_class=event_enum, event_at_ns=event_at_ns,
        received_at_ns=event_at_ns + 5, available_at_ns=available_at_ns,
        published_at_ns=None, event_type=event_type, record_id=record_id, sequence=1,
        raw_payload_hash=__import__("hashlib").sha256(raw_payload).hexdigest(),
        source_id="bybit-public",
    )
    return SimpleNamespace(observation=obs, raw_payload_bytes=raw_payload,
                           observation_index_ref=ref(["index", record_id]))


def config() -> TradeLocationConfig:
    return TradeLocationConfig(
        anchor=CausalTradeAnchor(KEY, START, START + 10, ref("anchor")),
        profile_window_start_ns=START,
    )


def utc_day_config() -> TradeLocationConfig:
    start_ns = CUTOFF // DAY_NS * DAY_NS
    return TradeLocationConfig(
        anchor=CausalTradeAnchor(KEY, start_ns, start_ns, ref("utc-day-anchor")),
        profile_window_start_ns=start_ns,
    )


def test_loader_reconstructs_exact_actual_native_trade_inputs_but_never_qualifies_coverage(monkeypatch):
    item = observation()
    seen = {}

    def reconstruct(repository, archive_root, **kwargs):
        seen.update(kwargs)
        return (item,)

    monkeypatch.setattr(adapter, "reconstruct_public_observations_from_archive", reconstruct)
    monkeypatch.setattr(adapter, "translate_recent_trades", lambda rows, **kwargs: (
        Translation("r1", item.observation.raw_payload_hash, START, 1),
    ))
    monkeypatch.setattr(adapter.json, "loads", lambda _payload: {
        "execId": "exec-1", "p": "100.5", "v": "2",
    })

    result = adapter.load_trade_location_inputs(
        object(), "/archive", key=KEY, cutoff_ns=CUTOFF, configuration=config(),
    )

    assert seen == {
        "instrument_revision": KEY.contract_revision,
        "information_cutoff_ns": CUTOFF,
        "event_types": ("TRADE", "AGG_TRADE"),
        "limit": adapter.MAX_ARCHIVED_TRADE_ROWS,
        "key": KEY,
        "availability_class": AvailabilityClassV2.ACTUAL_SYSTEM,
    }
    assert result.observed_trades[0].key == KEY
    assert result.observed_trades[0].trade_id == "exec-1"
    assert result.observed_trades[0].price == Decimal("100.5")
    assert result.observed_trades[0].quantity == Decimal("2")
    assert result.configuration == config()
    assert result.coverage_state == "UNVERIFIED"
    assert result.reason == "TRADE_COVERAGE_UNVERIFIED"
    assert result.estimator_trades == ()
    assert result.capital_authority == "ZERO"


def test_unqualified_runtime_receipt_keeps_trade_location_features_not_estimable(monkeypatch):
    item = observation()
    monkeypatch.setattr(adapter, "reconstruct_public_observations_from_archive",
                        lambda *args, **kwargs: (item,))
    monkeypatch.setattr(adapter, "translate_recent_trades", lambda rows, **kwargs: (
        Translation("r1", item.observation.raw_payload_hash, START, 1),
    ))
    monkeypatch.setattr(adapter.json, "loads", lambda _payload: {
        "execId": "exec-1", "p": "100.5", "v": "2",
    })
    # Keep the observed row in the declared profile interval for this fixture.
    configuration = TradeLocationConfig(
        anchor=CausalTradeAnchor(KEY, START, START + 10, ref("anchor")),
        profile_window_start_ns=START,
    )
    inputs = adapter.load_trade_location_inputs(
        object(), "/archive", key=KEY, cutoff_ns=CUTOFF, configuration=configuration,
    )
    class Persisted:
        def register_artifact(self, entry):
            self.entry = entry

    repository = Persisted()
    receipt = persist_trade_location_input_receipt(
        repository, inputs, created_at_ns=CUTOFF, available_at_ns=CUTOFF,
    )
    join = JoinedBars(KEY, CUTOFF, (), (), (), "NOT_ESTIMABLE", "MISSING_FRAMES", ref("health"))
    features = feature_snapshot(
        join, trades=inputs.estimator_trades, trade_location=configuration,
        trade_location_input_refs=(receipt.content_hash,),
    )
    assert receipt.body["coverage_state"] == "UNVERIFIED"
    assert receipt.body["completeness_window"] is None
    assert inputs.observed_trades and inputs.estimator_trades == ()
    assert features.values["location.anchored_vwap"].value is None
    assert features.values["location.anchored_vwap"].missing_reason == "CAUSAL_TRADES_UNAVAILABLE"
    assert features.values["location.volume_profile"].value is None
    assert receipt.content_hash in features.envelope.input_refs


def test_trade_location_receipt_has_later_publication_chronology_and_exports(tmp_path):
    from atlas.v2.science.broad_export import project_broad_evidence

    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        inputs = TradeLocationInputsV1(KEY, CUTOFF, None, (), (), 0)
        receipt = persist_trade_location_input_receipt(repository, inputs,
            created_at_ns=CUTOFF + 10, available_at_ns=CUTOFF + 10)
        entry = repository.get_artifact(receipt.content_hash)
        assert entry is not None
        row = project_broad_evidence(repository, entry)
        assert row["coverage_state"] == "UNVERIFIED"
        assert row["information_cutoff_ns"] == CUTOFF


@pytest.mark.parametrize("event_type", ["KLINE", "BOOK", "QUOTE"])
def test_loader_rejects_non_trade_public_rows(monkeypatch, event_type):
    item = observation(event_type=event_type)
    monkeypatch.setattr(adapter, "reconstruct_public_observations_from_archive",
                        lambda *args, **kwargs: (item,))
    result = adapter.load_trade_location_inputs(
        object(), "/archive", key=KEY, cutoff_ns=CUTOFF, configuration=config(),
    )
    assert result.observed_trades == ()
    assert result.estimator_trades == ()


def test_loader_requires_anchor_and_product_to_match_full_key_and_cutoff():
    wrong_key = InstrumentKeyV2(
        VenueV2.BYBIT, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL,
        "ETHUSDT", "ETH", "USDT", "USDT", "b" * 64,
    )
    bad = TradeLocationConfig(anchor=CausalTradeAnchor(wrong_key, START, START, ref("anchor")))
    with pytest.raises(ValueError, match="anchor"):
        adapter.load_trade_location_inputs(object(), "/archive", key=KEY,
                                           cutoff_ns=CUTOFF, configuration=bad)


def test_production_config_uses_latest_cutoff_confirmed_swing_and_explicit_utc_day(monkeypatch):
    product = ProductContractV2(
        KEY, START, START, START, Decimal("1"), Decimal("0.5"), Decimal("0.1"),
        Decimal("0.1"), TradingStatusV2.TRADING, ref("product"),
    )
    confirmed = SimpleNamespace(kind="LOW", pivot_ref=ref("pivot"), pivot_at_ns=START,
                                confirmation_ref=ref("confirmation"), confirmed_at_ns=START + 10)
    future = SimpleNamespace(kind="HIGH", pivot_ref=ref("future pivot"), pivot_at_ns=START + 20,
                             confirmation_ref=ref("future confirmation"), confirmed_at_ns=CUTOFF + 1)
    monkeypatch.setattr(production, "confirmed_swings", lambda _bars: (confirmed, future))
    result = production._trade_location_config_v1(product, (), cutoff_ns=CUTOFF)
    assert result.anchor == CausalTradeAnchor(KEY, START, START + 10, confirmed.confirmation_ref)
    assert result.profile_window_start_ns == CUTOFF // adapter.DAY_NS * adapter.DAY_NS
    assert result.product == product


def test_loader_marks_overflow_and_bounds_observed_trade_retention(monkeypatch):
    item = observation()
    monkeypatch.setattr(adapter, "reconstruct_public_observations_from_archive",
                        lambda *args, **kwargs: tuple(item for _ in range(adapter.MAX_ESTIMABLE_TRADE_ROWS + 1)))
    monkeypatch.setattr(adapter, "translate_recent_trades", lambda rows, **kwargs: (
        Translation("r1", item.observation.raw_payload_hash, START, 1),
    ))
    monkeypatch.setattr(adapter.json, "loads", lambda _payload: {
        "execId": "exec-1", "p": "100.5", "v": "2",
    })
    result = adapter.load_trade_location_inputs(
        object(), "/archive", key=KEY, cutoff_ns=CUTOFF, configuration=config(),
    )
    assert len(result.observed_trades) == adapter.MAX_ESTIMABLE_TRADE_ROWS
    assert result.rejected_row_count > 0
    assert result.estimator_trades == ()


class CompleteWindowPort:
    def __init__(self, contract, evidence):
        self.contract = contract
        self.evidence = evidence
        self.requests = []

    def read_window(self, request):
        self.requests.append(request)
        return self.evidence

    def repair_window(self, request):
        raise AssertionError("adapter must not start hidden repair")


def test_exact_prefix_contract_proof_opens_estimator_path(monkeypatch):
    monkeypatch.setattr(adapter, "reconstruct_public_observations_from_archive",
                        lambda *args, **kwargs: ())
    contract = TradeSourceContractV1(
        KEY, "audited-native-source", ref("source metadata"), ref("source qualification"),
        START - 1, EXACT_SEQUENCE_SEMANTICS_V1, False,
    )
    proof_config = utc_day_config()
    proof_start = proof_config.profile_window_start_ns
    assert proof_start is not None
    # Native prefix watermarks prove exactly one source trade in [START, CUTOFF).
    from atlas.v2.strategies.s3_mean_reversion import CausalTradeV2

    source_trade = CausalTradeV2(
        KEY, ref("indexed native trade"), contract.source_id, "native-1", proof_start + 1,
        proof_start + 2, proof_start + 3, Decimal("100.5"), Decimal("2"),
    )
    # Build the same request identity that the adapter constructs.
    from atlas.v2.data.trade_completeness import TradeCompletenessRequestV1

    request = TradeCompletenessRequestV1(
        KEY, contract.source_id, contract.content_hash, proof_start, CUTOFF, CUTOFF,
        adapter.MAX_ESTIMABLE_TRADE_ROWS,
    )
    evidence = TradeWindowEvidenceV1(
        request.content_hash,
        NativeTradePrefixV1(proof_start, 10, proof_start, proof_start + 1, ref("start prefix")),
        NativeTradePrefixV1(CUTOFF, 11, CUTOFF, CUTOFF, ref("end prefix")),
        (SequencedNativeTradeV1(11, source_trade),), ref("healthy source epoch"), recovery_epoch=4,
    )
    port = CompleteWindowPort(contract, evidence)
    checks = []

    def accept(check: PersistedTradeEvidenceCheckV1) -> bool:
        checks.append(check)
        return True  # Test port models accepted immutable indexes + verified payload bytes.

    result = adapter.load_trade_location_inputs(
        object(), "/archive", key=KEY, cutoff_ns=CUTOFF,
        configuration=proof_config, exact_trade_source=port, accepted_evidence=accept,
    )

    assert result.coverage_state == "VERIFIED"
    assert result.completeness_evidence_ref == evidence.content_hash
    assert len(result.estimator_trades) == 1
    assert result.estimator_trades[0].trade_id == "native-1"
    assert result.estimator_trades[0].price == Decimal("100.5")
    assert result.reason == "TRADE_COVERAGE_VERIFIED"
    assert {item.purpose for item in checks} == {
        "NATIVE_PREFIX", "NATIVE_TRADE", "PRODUCT_METADATA", "SOURCE_QUALIFICATION", "SOURCE_HEALTH",
    }
    assert port.requests == [request]

    class Persisted:
        def register_artifact(self, entry):
            self.entry = entry

    repository = Persisted()
    receipt = persist_trade_location_input_receipt(
        repository, result, created_at_ns=CUTOFF, available_at_ns=CUTOFF,
    )
    assert repository.entry.artifact_type == adapter.TRADE_LOCATION_INPUT_ARTIFACT_TYPE
    assert repository.entry.content_hash == receipt.content_hash
    assert receipt.body["coverage_state"] == "VERIFIED"
    assert receipt.body["configuration_ref"] == proof_config.content_hash
    assert receipt.body["completeness_window"] == {
        "start_at_ns": proof_start,
        "end_exclusive_at_ns": CUTOFF,
        "request_ref": request.content_hash,
        "source_contract_ref": contract.content_hash,
        "source_id": contract.source_id,
        "sequence_semantics": EXACT_SEQUENCE_SEMANTICS_V1,
    }
    assert adapter.TradeLocationInputReceiptV1.from_dict(
        receipt.to_dict(), artifact_ref=receipt.content_hash,
    ) == receipt
    malformed = dict(receipt.to_dict())
    malformed["completeness_window"] = dict(malformed["completeness_window"])
    malformed["completeness_window"]["end_exclusive_at_ns"] = CUTOFF + 1
    with pytest.raises(ValueError, match="interval exceeds cutoff"):
        adapter.TradeLocationInputReceiptV1.from_dict(malformed)
    join = JoinedBars(KEY, CUTOFF, (), (), (), "NOT_ESTIMABLE", "MISSING_FRAMES", ref("health"))
    features = feature_snapshot(
        join, trades=result.estimator_trades, trade_location=result.configuration,
        trade_location_input_refs=(receipt.content_hash,),
    )
    assert receipt.content_hash in features.envelope.input_refs
    assert result.configuration.content_hash not in features.envelope.input_refs
    assert receipt.body["configuration_ref"] == result.configuration.content_hash
    assert features.values["location.anchored_vwap"].value == Decimal("100.5")


def test_exact_prefix_gap_or_unaccepted_persisted_row_stays_unverified(monkeypatch):
    monkeypatch.setattr(adapter, "reconstruct_public_observations_from_archive",
                        lambda *args, **kwargs: ())
    contract = TradeSourceContractV1(
        KEY, "audited-native-source", ref("source metadata"), ref("source qualification"),
        START - 1, EXACT_SEQUENCE_SEMANTICS_V1, False,
    )
    proof_config = utc_day_config()
    proof_start = proof_config.profile_window_start_ns
    assert proof_start is not None
    from atlas.v2.data.trade_completeness import TradeCompletenessRequestV1
    from atlas.v2.strategies.s3_mean_reversion import CausalTradeV2

    request = TradeCompletenessRequestV1(
        KEY, contract.source_id, contract.content_hash, proof_start, CUTOFF, CUTOFF,
        adapter.MAX_ESTIMABLE_TRADE_ROWS,
    )
    source_trade = CausalTradeV2(KEY, ref("indexed native trade"), contract.source_id, "native-1",
        proof_start + 1, proof_start + 2, proof_start + 3, Decimal("100.5"), Decimal("2"))
    evidence = TradeWindowEvidenceV1(
        request.content_hash,
        NativeTradePrefixV1(proof_start, 10, proof_start, proof_start + 1, ref("start prefix")),
        NativeTradePrefixV1(CUTOFF, 12, CUTOFF, CUTOFF, ref("end prefix")),
        (SequencedNativeTradeV1(11, source_trade),), ref("healthy source epoch"), recovery_epoch=4,
    )
    result = adapter.load_trade_location_inputs(
        object(), "/archive", key=KEY, cutoff_ns=CUTOFF, configuration=proof_config,
        exact_trade_source=CompleteWindowPort(contract, evidence),
        accepted_evidence=lambda check: check.purpose != "NATIVE_TRADE",
    )
    assert result.coverage_state == "UNVERIFIED"
    assert result.estimator_trades == ()
