"""Point-in-time archive revision ordering and fail-closed scan bounds."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2, close_boundary_ns
from atlas.v2.data.collector import PublicCollectorV2
from atlas.v2.data.history import (
    ArchiveScanBoundExceededV2,
    ParquetObservationArchiveV2,
    reconstruct_causal_bars_from_archive,
)
from atlas.v2.data.raw import AvailabilityClassV2, RawObservationV2
from atlas.v2.desktop.projection import project_chart_series
from atlas.v2.instruments import (
    InstrumentRegistryV2,
    ProductContractV2,
    TradingStatusV2,
    VenueV2,
)
from atlas.v2.memory.repository import OpsRepository

from .test_session014_core import KEY

OPEN_AT_NS = 3_600_000_000_000
INTERVAL = BarIntervalV2.H1
CLOSE_AT_NS = close_boundary_ns(OPEN_AT_NS, INTERVAL)


def _contract(key) -> ProductContractV2:
    return ProductContractV2(
        key,
        1,
        1,
        1,
        Decimal("1"),
        Decimal("0.01"),
        Decimal("0.001"),
        Decimal("0.001"),
        TradingStatusV2.TRADING,
        sha256_json({"product": key.to_dict()}),
    )


def _revision_bar(
    key,
    *,
    source_id: str,
    sequence: str,
    close: str,
    effective_available_at_ns: int,
    revision_of: str | None = None,
    availability_class: AvailabilityClassV2,
) -> tuple[RawObservationV2, CausalBarV2, bytes]:
    replay_available = (
        effective_available_at_ns
        if availability_class == AvailabilityClassV2.RECONSTRUCTED_MARKET
        else None
    )
    actual_available = (
        effective_available_at_ns
        if availability_class == AvailabilityClassV2.ACTUAL_SYSTEM
        else CLOSE_AT_NS + 1_000
    )
    close_decimal = Decimal(close)
    bar_payload = {
        "open_at_ns": OPEN_AT_NS,
        "open": "100",
        "high": str(max(Decimal("100"), close_decimal) + Decimal("1")),
        "low": str(min(Decimal("100"), close_decimal) - Decimal("1")),
        "close": close,
        "volume": "10",
        "final": True,
    }
    raw_payload = canonical_json({"revision": sequence, "bar": bar_payload}).encode("utf-8")
    observation = RawObservationV2.build(
        instrument_revision=key.contract_revision,
        source_id=source_id,
        event_type=f"BAR_{INTERVAL.value}",
        event_at_ns=CLOSE_AT_NS,
        received_at_ns=actual_available,
        ingested_at_ns=actual_available,
        available_at_ns=actual_available,
        payload=raw_payload,
        translation_version="archive-revision-fixture-v1",
        revision_of=revision_of,
        availability_class=availability_class,
        sequence=sequence,
        replay_available_at_ns=replay_available,
    )
    bar = CausalBarV2(
        observation,
        INTERVAL,
        OPEN_AT_NS,
        CLOSE_AT_NS,
        Decimal("100"),
        max(Decimal("100"), close_decimal) + Decimal("1"),
        min(Decimal("100"), close_decimal) - Decimal("1"),
        close_decimal,
        Decimal("10"),
        True,
    )
    return observation, bar, raw_payload


def _seed_revision_archive(repository: OpsRepository, root: Path, availability_class: AvailabilityClassV2):
    key_a = KEY
    # Deliberately share contract_revision text while changing the complete key.
    key_other = replace(
        KEY,
        venue=VenueV2.BINANCE,
        native_symbol="ETHUSDT",
        base_asset_id="ethereum",
    )
    registry = InstrumentRegistryV2()
    registry.register(_contract(key_a))
    registry.register(_contract(key_other))
    archive = ParquetObservationArchiveV2(root)
    collector = PublicCollectorV2(
        repository=repository,
        registry=registry,
        clock_ns=lambda: CLOSE_AT_NS + 10_000,
        archive=archive,
    )

    obs_a, bar_a, payload_a = _revision_bar(
        key_a,
        source_id="BYBIT_PUBLIC_HTTP",
        sequence="bar-revision-a",
        close="101",
        effective_available_at_ns=CLOSE_AT_NS + 100,
        availability_class=availability_class,
    )
    obs_b, bar_b, payload_b = _revision_bar(
        key_a,
        source_id="BYBIT_PUBLIC_HTTP",
        sequence="bar-revision-b",
        close="103",
        effective_available_at_ns=CLOSE_AT_NS + 200,
        revision_of=obs_a.record_id,
        availability_class=availability_class,
    )
    obs_other, bar_other, payload_other = _revision_bar(
        key_other,
        source_id="BINANCE_PUBLIC_HTTP",
        sequence="other-instrument-same-revision",
        close="997",
        effective_available_at_ns=CLOSE_AT_NS + 250,
        availability_class=availability_class,
    )

    paths: dict[str, Path] = {}
    for label, key, observation, bar, payload in (
        ("old", key_a, obs_a, bar_a, payload_a),
        ("new", key_a, obs_b, bar_b, payload_b),
        ("other", key_other, obs_other, bar_other, payload_other),
    ):
        collector.ingest(observation, raw_payload=payload, instrument_key=key, bar=bar)
        path = collector.flush_archive()
        assert path is not None
        paths[label] = Path(path)
    refs = {
        label: sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": observation.record_id})
        for label, observation in (("old", obs_a), ("new", obs_b), ("other", obs_other))
    }
    return key_a, (obs_a, obs_b, obs_other), (bar_a, bar_b, bar_other), paths, refs


def _recreate_chunks_in_order(paths: dict[str, Path], labels: tuple[str, ...]) -> None:
    """Vary directory insertion order while preserving immutable chunk locators."""
    contents = {label: path.read_bytes() for label, path in paths.items()}
    for path in paths.values():
        path.unlink()
    for label in labels:
        paths[label].write_bytes(contents[label])


def _reconstruct(repository, root, key, cutoff_ns, availability_class):
    return reconstruct_causal_bars_from_archive(
        repository,
        root,
        key=key,
        interval=INTERVAL,
        information_cutoff_ns=cutoff_ns,
        availability_class=availability_class,
    )


@pytest.mark.parametrize(
    "availability_class",
    [AvailabilityClassV2.ACTUAL_SYSTEM, AvailabilityClassV2.RECONSTRUCTED_MARKET],
)
def test_same_bar_revision_selection_is_cutoff_and_directory_order_independent(
    tmp_path, availability_class
) -> None:
    root = tmp_path / "archive"
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        key, observations, bars, paths, refs = _seed_revision_archive(
            repository, root, availability_class
        )
        obs_a, obs_b, obs_other = observations
        bar_a, bar_b, _ = bars
        def availability(item: RawObservationV2) -> int | None:
            if availability_class == AvailabilityClassV2.ACTUAL_SYSTEM:
                return item.available_at_ns
            return item.replay_available_at_ns

        availability_a = availability(obs_a)
        availability_b = availability(obs_b)
        availability_other = availability(obs_other)
        assert availability_a is not None and availability_b is not None and availability_other is not None
        assert availability_a < availability_b < availability_other
        assert obs_b.revision_of == obs_a.record_id
        assert repository.get_artifact(refs["old"]) is not None
        assert repository.get_artifact(refs["new"]) is not None

        # The indexed locator owns filenames; directory insertion order cannot
        # determine which cutoff-visible revision supplies the feature bar.
        _recreate_chunks_in_order(paths, ("new", "other", "old"))
        before_b = availability_b - 1
        earlier = _reconstruct(repository, root, key, before_b, availability_class)
        assert len(earlier) == 1
        assert earlier[0].bar == bar_a
        assert earlier[0].observation_index_ref == refs["old"]

        after_other = availability_other
        later = _reconstruct(repository, root, key, after_other, availability_class)
        assert len(later) == 1
        assert later[0].bar == bar_b
        assert later[0].observation_index_ref == refs["new"]
        assert later[0].bar.raw.record_id == obs_b.record_id
        assert later[0].bar.close == Decimal("103")

        chart_cutoff = availability_b
        chart_source = _reconstruct(repository, root, key, chart_cutoff, availability_class)
        assert len(chart_source) == 1
        assert chart_source[0].bar == bar_b
        chart = project_chart_series(
            repository,
            key_json=key.to_canonical_json(),
            interval=INTERVAL.value,
            information_cutoff_ns=chart_cutoff,
            archive_root=root,
            availability_view=availability_class.value,
            limit=10,
        )
        assert chart.state == "AVAILABLE" and len(chart.bars) == 1
        assert chart.bars[0].observation_ref == chart_source[0].observation_index_ref
        assert chart.bars[0].close == str(bar_b.close)

        _recreate_chunks_in_order(paths, ("old", "other", "new"))
        reordered = _reconstruct(repository, root, key, after_other, availability_class)
        assert len(reordered) == 1
        assert reordered[0] == later[0]
        reordered_chart = project_chart_series(
            repository,
            key_json=key.to_canonical_json(),
            interval=INTERVAL.value,
            information_cutoff_ns=chart_cutoff,
            archive_root=root,
            availability_view=availability_class.value,
            limit=10,
        )
        assert reordered_chart.bars == chart.bars


@pytest.mark.parametrize(
    ("bound", "maximum"),
    [("file-count", 1), ("row-count", 1)],
)
def test_archive_scan_bound_exhaustion_raises_without_returning_partial_rows(
    tmp_path, monkeypatch, bound, maximum
) -> None:
    from atlas.v2.data import history

    root = tmp_path / "archive"
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        key, observations, _, _, _ = _seed_revision_archive(
            repository, root, AvailabilityClassV2.ACTUAL_SYSTEM
        )
        if bound == "file-count":
            monkeypatch.setattr(history, "MAX_CAUSAL_ARCHIVE_FILES", maximum)
        else:
            monkeypatch.setattr(history, "MAX_CAUSAL_ARCHIVE_ROWS", maximum)
        returned = None
        with pytest.raises(ArchiveScanBoundExceededV2) as raised:
            returned = _reconstruct(
                repository,
                root,
                key,
                observations[-1].available_at_ns,
                AvailabilityClassV2.ACTUAL_SYSTEM,
            )
        assert returned is None
        assert raised.value.bound == bound
        assert raised.value.maximum == maximum


def test_malformed_unrelated_archive_file_is_skipped_without_hiding_valid_rows(tmp_path) -> None:
    root = tmp_path / "archive"
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        key, observations, bars, _, refs = _seed_revision_archive(
            repository, root, AvailabilityClassV2.ACTUAL_SYSTEM
        )
        (root / "00-malformed.parquet").write_bytes(b"not a parquet file")
        result = _reconstruct(
            repository,
            root,
            key,
            observations[-1].available_at_ns,
            AvailabilityClassV2.ACTUAL_SYSTEM,
        )
        assert len(result) == 1
        assert result[0].bar == bars[1]
        assert result[0].observation_index_ref == refs["new"]


def test_missing_immutable_index_locator_fails_closed_without_glob_rediscovery(tmp_path) -> None:
    root = tmp_path / "archive"
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        key, observations, _, paths, _ = _seed_revision_archive(
            repository, root, AvailabilityClassV2.ACTUAL_SYSTEM
        )
        paths["new"].rename(root / "renamed-but-valid.parquet")
        with pytest.raises(ValueError, match="missing or unsafe"):
            _reconstruct(repository, root, key, observations[-1].available_at_ns,
                AvailabilityClassV2.ACTUAL_SYSTEM)
