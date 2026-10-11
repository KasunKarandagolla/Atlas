"""Selected-venue context binding for ordinary V2 research runs."""

from __future__ import annotations

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.news.events import (
    DEFAULT_NEWS_SOURCES_V2,
    FetchedDocumentV2,
    NewsSourceConfigV2,
    public_context_sources_for_venues,
)
from atlas.v2.product import (
    ResearchRunConfigV1,
    ResearchRunConfigV2,
    _public_context_sources_for_config,
    canonical_data_root,
)
from atlas.v2.runtime.public_context import PublicContextMaintenanceV1


class _Clock:
    def __init__(self) -> None:
        self.at = 1_800_000_000_000

    def __call__(self) -> int:
        return self.at


class _Transport:
    def __init__(self, clock: _Clock, *, failure: bool = False) -> None:
        self.clock = clock
        self.failure = failure
        self.calls: list[NewsSourceConfigV2] = []

    def fetch(self, source: NewsSourceConfigV2) -> FetchedDocumentV2:
        self.calls.append(source)
        if self.failure:
            raise RuntimeError("fixture fetch failure")
        body = b'{"articles":[{"title":"Bitcoin security incident",' \
            b'"url":"https://news.example.org/a","description":"BTC incident"}]}'
        return FetchedDocumentV2(source.feed_url, source.feed_url, 200, body, self.clock(), {})


def _binance_config(root) -> ResearchRunConfigV2:
    return ResearchRunConfigV2(
        data_root=canonical_data_root(root),
        public_venues=("BINANCE",),
        selected_execution_venue="BINANCE",
        execution_profile="DISABLED",
        provider_profile="DISABLED",
    )


def _finish_fetch(maintenance: PublicContextMaintenanceV1) -> None:
    worker = maintenance._thread
    assert worker is not None
    worker.join(timeout=2)
    assert not worker.is_alive()


def test_v2_context_sources_are_bound_to_venues_and_preserve_legacy_defaults(tmp_path) -> None:
    config = _binance_config(tmp_path)
    selected = _public_context_sources_for_config(config)
    assert selected is not None
    ids = {source.source_id for source in selected}
    assert ids == {
        "FEDERAL_RESERVE", "BLS", "SEC", "BINANCE_OFFICIAL", "BINANCE_STATUS",
        "GDELT_DISCOVERY", "COIN_METRICS_COMMUNITY",
    }
    assert not any("bybit" in source.feed_url.casefold() for source in selected)
    assert any(source.source_id == "SEC" for source in selected)
    assert any(source.source_id == "BINANCE_OFFICIAL" for source in selected)
    assert any(source.source_id == "GDELT_DISCOVERY" for source in selected)

    both = public_context_sources_for_venues(("BYBIT", "BINANCE"))
    assert both == DEFAULT_NEWS_SOURCES_V2
    legacy_profile = PublicContextMaintenanceV1(sources=both)
    legacy_default = PublicContextMaintenanceV1()
    assert legacy_profile.policy_hash == legacy_default.policy_hash
    legacy_profile.close()
    legacy_default.close()
    assert _public_context_sources_for_config(ResearchRunConfigV1()) is None
    legacy_context = PublicContextMaintenanceV1()
    assert legacy_context.sources == DEFAULT_NEWS_SOURCES_V2
    legacy_context.close()

    other_config = _binance_config(tmp_path)
    other_config = ResearchRunConfigV2(
        **{**other_config.__dict__, "public_venues": ("BYBIT", "BINANCE")},
    )
    assert config.content_hash != other_config.content_hash
    assert public_context_sources_for_venues(("BINANCE",)) == selected


def test_selected_source_policy_and_receipt_bind_only_binance_context_and_restart(tmp_path) -> None:
    clock = _Clock()
    config = _binance_config(tmp_path)
    selected = _public_context_sources_for_config(config)
    assert selected is not None
    transport = _Transport(clock)
    maintenance = PublicContextMaintenanceV1(
        clock_ns=clock, transport=transport, sources=selected,
    )
    expected_policy_hash = sha256_json(maintenance.policy)
    assert maintenance.policy_hash == expected_policy_hash
    assert maintenance.policy["sources"] == [source.to_dict() for source in selected]

    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        started = maintenance.run_cycle(repository, information_cutoff_ns=clock.at)
        assert started.status == "FETCH_STARTED"
        _finish_fetch(maintenance)
        report = maintenance.run_cycle(repository, information_cutoff_ns=clock.at)
        assert report.status == "COLLECTED"
        assert report.policy_hash == expected_policy_hash
        assert report.source_id == selected[0].source_id
        assert report.source_ref == sha256_json(selected[0].to_dict())
        assert [source.source_id for source in transport.calls] == [selected[0].source_id]
        assert all("bybit" not in source.feed_url.casefold() for source in transport.calls)

        policy_entry = repository.get_artifact(expected_policy_hash)
        source_entry = repository.get_artifact(report.source_ref)
        report_entry = repository.get_artifact(report.content_hash)
        assert policy_entry is not None
        assert canonical_json(policy_entry.metadata["sources"]) == canonical_json(
            [source.to_dict() for source in selected])
        assert source_entry is not None
        assert canonical_json(source_entry.metadata) == canonical_json(selected[0].to_dict())
        assert report_entry is not None
        assert report_entry.metadata["policy_hash"] == expected_policy_hash
        assert report_entry.metadata["source_ref"] == source_entry.artifact_ref
        assert "BYBIT_OFFICIAL" not in str(policy_entry.metadata)
        assert "BYBIT_STATUS" not in str(policy_entry.metadata)
    maintenance.close()

    restarted_transport = _Transport(clock)
    restarted = PublicContextMaintenanceV1(
        clock_ns=clock, transport=restarted_transport, sources=selected,
    )
    assert restarted.policy_hash == expected_policy_hash
    assert restarted.sources == selected
    restarted.close()


def test_selected_binance_source_failure_is_not_replaced_with_healthy_evidence(tmp_path) -> None:
    clock = _Clock()
    selected = public_context_sources_for_venues(("BINANCE",))
    target_index = next(index for index, source in enumerate(selected)
                        if source.source_id == "BINANCE_STATUS")
    transport = _Transport(clock, failure=True)
    maintenance = PublicContextMaintenanceV1(
        clock_ns=clock, transport=transport, sources=selected,
    )
    maintenance._position = target_index
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        started = maintenance.run_cycle(repository, information_cutoff_ns=clock.at)
        assert started.status == "FETCH_STARTED"
        _finish_fetch(maintenance)
        report = maintenance.run_cycle(repository, information_cutoff_ns=clock.at)
        assert report.status == "FAILED"
        assert report.reasons == ("PUBLIC_CONTEXT_FETCH_FAILED",)
        assert report.source_id == "BINANCE_STATUS"
        assert report.policy_hash == maintenance.policy_hash
        assert len(transport.calls) == 1
        assert transport.calls[0].source_id == "BINANCE_STATUS"
        assert report.event_refs == () and report.alert_refs == ()
    maintenance.close()
