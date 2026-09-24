from decimal import Decimal

import pytest

from atlas.v2.contracts import OpportunityWatchV2, WatchStateV2
from atlas.v2.instruments import EnvironmentV2, InstrumentKeyV2, ProductTypeV2, VenueV2
from atlas.v2.memory.repository import (
    ArtifactIndexEntryV2,
    DedupeConflict,
    OpsRepository,
    SourceHealthV2,
    StaleWatchVersion,
)
from atlas.v2.models.protocol import ModelManifestV2, PromotionStatusV2

H = "a" * 64
H2 = "b" * 64


def key() -> InstrumentKeyV2:
    return InstrumentKeyV2(VenueV2.BYBIT, EnvironmentV2.MAINNET, ProductTypeV2.LINEAR_PERPETUAL, "BTCUSDT", "BTC", "USDT", "USDT", "r1")


def watch(watch_id: str = "watch", expires: int = 200) -> OpportunityWatchV2:
    return OpportunityWatchV2(watch_id, key(), "strategy", "1", H, WatchStateV2.DETECTED, 0, 100, 100, H2, (), "BAR_CLOSE", expires, 100)


def manifest() -> ModelManifestV2:
    return ModelManifestV2(
        "provider", "repo", "commit", "checkpoint", "revision", (H,), "code", "weights", "research",
        H2, H, H2, "cpu", "fp32", ("input",), ("output",), 10, "UNKNOWN", PromotionStatusV2.INTEGRATED,
    )


def test_watch_transition_outbox_atomic_and_duplicate_is_idempotent(tmp_path) -> None:
    path = tmp_path / "ops.sqlite"
    repository = OpsRepository(path)
    repository.create_watch(watch())
    result = repository.transition_watch(
        "watch", expected_state_version=0, event_id="event-1", event_at_ns=110, transition_at_ns=110,
        target_state=WatchStateV2.WAITING_FOR_EVENT, outbox_id="outbox-1", required_next_event="BAR_CLOSE",
    )
    assert result.inserted and result.watch.state_version == 1
    repeated = repository.transition_watch(
        "watch", expected_state_version=0, event_id="event-1", event_at_ns=110, transition_at_ns=110,
        target_state=WatchStateV2.WAITING_FOR_EVENT, outbox_id="outbox-1", required_next_event="BAR_CLOSE",
    )
    assert not repeated.inserted and repeated.watch == result.watch
    with pytest.raises(DedupeConflict):
        repository.transition_watch(
            "watch", expected_state_version=0, event_id="event-1", event_at_ns=111, transition_at_ns=111,
            target_state=WatchStateV2.WAITING_FOR_EVENT, outbox_id="outbox-1",
        )
    with pytest.raises(StaleWatchVersion):
        repository.transition_watch(
            "watch", expected_state_version=0, event_id="event-2", event_at_ns=112, transition_at_ns=112,
            target_state=WatchStateV2.READY_FOR_RECHECK, outbox_id="outbox-2",
        )
    assert len(repository.pending_outbox()) == 1
    item = repository.record_outbox_handling("outbox-1", handled_at_ns=120, handling_ref="consumer/receipt")
    assert item.handled_at_ns == 120 and not repository.pending_outbox()
    repository.close()
    reopened = OpsRepository(path)
    assert reopened.get_watch("watch") == result.watch
    assert reopened.list_active_watches() == (result.watch,)
    assert reopened.pending_outbox() == ()
    reopened.close()


def test_transition_and_outbox_rollback_together_on_failure(tmp_path) -> None:
    class FailingRepository(OpsRepository):
        def _before_outbox_insert(self, changed_watch):
            raise RuntimeError("simulated outbox failure")

    path = tmp_path / "atomic.sqlite"
    failing = FailingRepository(path)
    failing.create_watch(watch("atomic"))
    with pytest.raises(RuntimeError, match="simulated"):
        failing.transition_watch(
            "atomic", expected_state_version=0, event_id="event", event_at_ns=110, transition_at_ns=110,
            target_state=WatchStateV2.WAITING_FOR_EVENT, outbox_id="outbox",
        )
    failing.close()
    reopened = OpsRepository(path)
    assert reopened.get_watch("atomic") == watch("atomic")
    assert reopened.pending_outbox() == ()
    assert reopened._connection.execute("SELECT count(*) FROM watch_transition").fetchone()[0] == 0
    reopened.close()


def test_restart_recovery_subscriptions_expiry_and_terminal_state(tmp_path) -> None:
    path = tmp_path / "restart.sqlite"
    repository = OpsRepository(path)
    repository.create_watch(watch("active", expires=200))
    repository.create_watch(watch("due", expires=120))
    with pytest.raises(ValueError, match="supply deterministic expiry IDs"):
        repository.recover_active_watches(now_ns=120)
    snapshot = repository.recover_active_watches(now_ns=120, expiry_ids={"due": ("expiry-event", "expiry-outbox")})
    assert tuple(item.watch_id for item in snapshot.active_watches) == ("active",)
    assert snapshot.required_events == (("active", "BAR_CLOSE"),)
    expired = repository.get_watch("due")
    assert expired is not None and expired.state == WatchStateV2.EXPIRED
    assert expired.last_event_id == "expiry-event"
    repository.close()
    reopened = OpsRepository(path)
    assert tuple(item.watch_id for item in reopened.list_active_watches()) == ("active",)
    expired_after_restart = reopened.get_watch("due")
    assert expired_after_restart is not None and expired_after_restart.state == WatchStateV2.EXPIRED
    reopened.close()


def test_source_health_history_manifest_and_artifact_index_are_immutable(tmp_path) -> None:
    repository = OpsRepository(tmp_path / "ops.sqlite")
    one = SourceHealthV2("source", 10, 11, "OK", "health-ref")
    repository.record_source_health(one)
    repository.record_source_health(one)
    repository.record_source_health(SourceHealthV2("source", 20, 22, "DEGRADED"))
    assert repository.source_health_history("source") == (one, SourceHealthV2("source", 20, 22, "DEGRADED"))
    value = manifest()
    assert repository.register_model_manifest(value) == value.manifest_hash
    assert repository.get_model_manifest(value.manifest_hash) == value
    entry = ArtifactIndexEntryV2(H2, "FeatureArtifactV2", H, 10, 12, {"slot": 1, "quality": Decimal("0.5")})
    repository.register_artifact(entry)
    assert repository.get_artifact(H2) == entry
    repository.close()


def test_future_unknown_schema_and_non_ops_db_fail_closed(tmp_path) -> None:
    future = tmp_path / "future.sqlite"
    repository = OpsRepository(future)
    repository._connection.execute("UPDATE schema_meta SET schema_version=99")
    repository.close()
    with pytest.raises(RuntimeError, match="future"):
        OpsRepository(future)
    unrelated = tmp_path / "unrelated.sqlite"
    import sqlite3

    connection = sqlite3.connect(unrelated)
    connection.execute("CREATE TABLE legacy_live_db(id INTEGER)")
    connection.commit()
    connection.close()
    with pytest.raises(RuntimeError, match="not an initialized atlas-ops"):
        OpsRepository(unrelated)


def test_ops_schema_is_separate_versioned_wal_and_foreign_keys_enabled(tmp_path) -> None:
    repository = OpsRepository(tmp_path / "separate-ops.sqlite")
    assert repository.schema_version == 1
    assert repository.schema_namespace == "atlas-ops"
    assert repository._connection.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
    assert repository._connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    assert repository._connection.execute("PRAGMA synchronous").fetchone()[0] == 2
    names = {row[0] for row in repository._connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"watch", "watch_transition", "ops_outbox", "source_health", "model_registry", "artifact_index"}.issubset(names)
    repository.close()
