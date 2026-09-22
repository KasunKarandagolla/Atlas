from __future__ import annotations

from atlas.data.models import DataKind
from atlas.data.public_bybit import PublicMarketCollector, translate_nautilus_public_event


def test_public_nautilus_shapes_translate_with_real_local_receipt_clock():
    event = {
        "type": "Bar",
        "instrument_id": "BTCUSDT.BYBIT",
        "bar_type": "1-MINUTE-LAST",
        "ts_event": 1_700_000_000_000_000_000,
        "ts_init": 1_700_000_000_060_000_000,
        "open": "50000",
        "high": "50100",
        "low": "49900",
        "close": "50050",
        "volume": "12",
    }
    translated = translate_nautilus_public_event(event, record_id="bar-1")
    assert translated.instrument == "BTCUSDT"
    assert translated.data_kind == DataKind.BAR_1M_LAST
    ticks = iter((1_700_000_000_100_000_000, 1_700_000_000_101_000_000, 1_700_000_000_102_000_000))
    record = PublicMarketCollector(lambda: next(ticks)).ingest(translated)
    assert record.received_at_ns == 1_700_000_000_100_000_000
    assert record.data_ingested_at_ns == record.received_at_ns
    assert record.recorded_at_ns >= record.processed_at_ns
