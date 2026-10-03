"""Native origin discovery pages work before, and independently of, close pagination."""
from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository

from .test_session014_core import KEY

STEP = 900_000_000_000


def _entries(count, *, available=1000*STEP):
    return tuple(ArtifactIndexEntryV2(sha256_json({"origin":i}),"PublicObservationIndexV2",
        sha256_json({"raw":i}),available,available,
        {"instrument_key_json":KEY.to_canonical_json(),"event_type":"BAR_15M",
         "availability_class":"ACTUAL_SYSTEM","event_at_ns":(i+1)*STEP,
         "bar_content_hash":sha256_json({"bar":i})}) for i in range(count))


def test_large_window_discovery_is_restartable_and_oldest_close_exact(tmp_path):
    path = tmp_path/"ops.sqlite"
    with OpsRepository(path) as repo:
        repo.register_artifacts(_entries(300))
        page = repo.m15_origin_observation_page(KEY,available_from_ns=0,available_through_ns=1000*STEP)
        assert page.entries == () and page.has_more and page.last_close_at_ns == 0
        head = repo._connection.execute("SELECT * FROM native_origin_window_head").fetchone()
        assert not head["complete"]
        assert repo._connection.execute("SELECT COUNT(*) FROM native_origin_window").fetchone()[0] == 128
    with OpsRepository(path) as repo:
        page = repo.m15_origin_observation_page(KEY,available_from_ns=0,available_through_ns=1000*STEP,
                                               after_close_at_ns=0)
        assert page.entries == () and page.has_more
        page = repo.m15_origin_observation_page(KEY,available_from_ns=0,available_through_ns=1000*STEP,
                                               after_close_at_ns=0)
        assert [item.metadata["event_at_ns"] for item in page.entries] == [STEP,2*STEP,3*STEP,4*STEP]
        assert page.has_more
        last = 0
        seen = []
        while True:
            page = repo.m15_origin_observation_page(KEY,available_from_ns=0,available_through_ns=1000*STEP,
                                                   after_close_at_ns=last)
            seen.extend(item.metadata["event_at_ns"] for item in page.entries)
            if not page.has_more:
                break
            last = page.last_close_at_ns
        assert seen == [i*STEP for i in range(1,301)]


def test_completed_window_rewinds_for_same_clock_source_and_preserves_first_revision(tmp_path):
    with OpsRepository(tmp_path/"ops.sqlite") as repo:
        original = _entries(1)[0]
        repo.register_artifact(original)
        args = {"available_from_ns":0,"available_through_ns":1000*STEP}
        first = repo.m15_origin_observation_page(KEY,**args)
        assert first.entries == (original,) and not first.has_more
        revised = ArtifactIndexEntryV2("0"*64,"PublicObservationIndexV2","1"*64,
            original.created_at_ns,original.available_at_ns,original.metadata)
        repo.register_artifact(revised)
        second = repo.m15_origin_observation_page(KEY,**args)
        assert second.entries == (revised,)  # Frozen equal-clock tie convention.


def test_completed_close_page_work_does_not_scan_retained_window(tmp_path):
    with OpsRepository(tmp_path/"ops.sqlite") as repo:
        repo.register_artifacts(_entries(1000))
        args = {"available_from_ns":0,"available_through_ns":1000*STEP}
        for _ in range(8):
            repo.m15_origin_observation_page(KEY,**args)
        assert repo._connection.execute("SELECT complete FROM native_origin_window_head").fetchone()[0]
        instructions = [0]
        def progress():
            instructions[0] += 1
            return instructions[0]>1000
        repo._connection.set_progress_handler(progress,1)
        try:
            page = repo.m15_origin_observation_page(KEY,**args,after_close_at_ns=950*STEP)
        finally:
            repo._connection.set_progress_handler(None,0)
        assert len(page.entries)==4 and page.last_close_at_ns==954*STEP
        assert instructions[0]<1000


def test_controller_preserves_accounting_cursor_while_source_discovery_advances(tmp_path):
    from atlas.v2.data.collector import PublicCollectorV2
    from atlas.v2.instruments import InstrumentRegistryV2
    from atlas.v2.runtime.production import IndexedPublicCycleSourceV1

    from .test_session017_risk import risk_case

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        registry = InstrumentRegistryV2()
        registry.register(case.product)
        repo.register_artifacts(_entries(300))
        collector = PublicCollectorV2(repository=repo, registry=registry, clock_ns=lambda: 1000 * STEP)
        source = IndexedPublicCycleSourceV1(clock_ns=lambda: 1000 * STEP)
        assert source._account_m15_origins(repo, collector, now_ns=1000 * STEP, allow_events=False) == ()
        first = repo.latest_m15_origin_accounting_checkpoint(KEY)
        assert first.metadata["checkpoint"]["source_scan_after_close_at_ns"] == 0
        assert source._account_m15_origins(repo, collector, now_ns=1000 * STEP + 1, allow_events=False) == ()
        assert repo.latest_m15_origin_accounting_checkpoint(KEY) == first
        head = repo._connection.execute("SELECT * FROM native_origin_window_head").fetchone()
        assert not head["complete"]
        assert repo._connection.execute("SELECT COUNT(*) FROM native_origin_window").fetchone()[0] == 256
