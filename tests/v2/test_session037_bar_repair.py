"""Recovery checks only the outage interval, in restartable causal pages."""
import pytest

from atlas.v2.data.bars import BarIntervalV2
from atlas.v2.data.collector import PublicCollectorV2
from atlas.v2.instruments import InstrumentRegistryV2
from atlas.v2.memory.repository import OpsRepository, SourceHealthV2
from atlas.v2.runtime import bar_repair

from .test_session014_core import KEY
from .test_session037_history_runtime import _indexed, _register


@pytest.fixture
def sources(monkeypatch):
    values = {}
    pages = []
    def reconstruct(_repo,_root,*,index_entries,max_origins,**_kwargs):
        assert max_origins==128 and len(index_entries)<=128
        pages.append(len(index_entries))
        return tuple(values[row.artifact_ref] for row in index_entries)
    monkeypatch.setattr(bar_repair,"reconstruct_native_bars_from_index_page",reconstruct)
    return values,pages


def _attempt(repo,root,cutoff):
    return bar_repair.repair_bar_interval(repo,root,key=KEY,interval=BarIntervalV2.M15,
        source_id=_indexed(0).bar.raw.source_id,cutoff_ns=cutoff,clock_ns=lambda:cutoff+10)


def test_gap_repair_progress_survives_restart_and_bounds_each_page(tmp_path,sources):
    bars = tuple(_indexed(i) for i in range(150))
    sources[0].update((item.observation_index_ref,item) for item in bars)
    cutoff = bars[-1].bar.close_at_ns
    path = tmp_path/"ops.sqlite"
    with OpsRepository(path) as repo:
        repo.record_source_health(SourceHealthV2(bars[0].bar.raw.source_id,1,1,"HEALTHY_CURRENT"))
        _register(repo,bars)
        ready,ref = _attempt(repo,tmp_path,cutoff)
        assert not ready and sources[1]==[128]
        evidence = repo.get_artifact(ref).metadata["repair"]
        assert evidence["reason_code"]=="BAR_GAP_REPAIR_BACKLOG"
        assert evidence["exact_trade_completeness"]=="NOT ESTIMABLE"
        repeated,_ = _attempt(repo,tmp_path,cutoff)
        assert not repeated and sources[1]==[128]
    with OpsRepository(path) as repo:
        ready,ref = _attempt(repo,tmp_path,cutoff+20)
        assert ready and sources[1]==[128,22]
        assert repo.get_artifact(ref).metadata["repair"]["verified_close_at_ns"]==cutoff
        head = repo.public_bar_repair_head(KEY,BarIntervalV2.M15.value)
        repo._connection.execute("UPDATE public_bar_repair_head SET verified_close_at_ns=?",(cutoff+1,))
        with pytest.raises(ValueError,match="certificate"):
            _attempt(repo,tmp_path,cutoff+40)
        assert head["certificate_ref"]==ref


def test_missing_bar_cannot_be_rescued_by_contiguous_later_tail(tmp_path,sources):
    bars = tuple(_indexed(i) for i in (0,1,3,4))
    sources[0].update((item.observation_index_ref,item) for item in bars)
    with OpsRepository(tmp_path/"ops.sqlite") as repo:
        repo.record_source_health(SourceHealthV2(bars[0].bar.raw.source_id,1,1,"HEALTHY_CURRENT"))
        _register(repo,bars)
        cutoff = bars[-1].bar.close_at_ns
        ready,ref = _attempt(repo,tmp_path,cutoff)
        assert not ready
        assert repo.get_artifact(ref).metadata["repair"]["reason_code"]=="CONFIRMED_BAR_GAP_UNREPAIRED"
        assert not _attempt(repo,tmp_path,cutoff+20)[0]
        missing = _indexed(2,available=cutoff+30)
        sources[0][missing.observation_index_ref] = missing
        _register(repo,(missing,))
        assert _attempt(repo,tmp_path,cutoff+40)[0]


def test_repair_requires_a_previous_healthy_boundary(tmp_path,sources):
    with OpsRepository(tmp_path/"ops.sqlite") as repo:
        assert _attempt(repo,tmp_path,BarIntervalV2.M15.duration_ns)==(False,None)
        assert sources[1]==[]


def test_reconnect_binds_repair_certificates_and_refuses_incomplete_or_future_proof(tmp_path, sources):
    from .test_session017_risk import risk_case

    bars = tuple(_indexed(i) for i in range(150))
    sources[0].update((item.observation_index_ref, item) for item in bars)
    cutoff = bars[-1].bar.close_at_ns
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        registry = InstrumentRegistryV2()
        registry.register(case.product)
        source_id = bars[0].bar.raw.source_id
        repo.record_source_health(SourceHealthV2(source_id, 1, 1, "HEALTHY_CURRENT"))
        _register(repo, bars)
        collector = PublicCollectorV2(repository=repo, registry=registry, clock_ns=lambda: cutoff + 100)
        ready, incomplete = _attempt(repo, tmp_path, cutoff)
        assert not ready
        with pytest.raises(ValueError, match="completed bar repair"):
            collector.reconcile_after_reconnect(source_id, at_ns=cutoff + 20,
                complete_snapshot=True, missed_interval_repaired=True,
                snapshot_refs=(bars[-1].observation_index_ref,), repair_certificate_refs=(incomplete,))
        ready, completed = _attempt(repo, tmp_path, cutoff + 20)
        assert ready
        with pytest.raises(ValueError, match="completed bar repair"):
            collector.reconcile_after_reconnect(source_id, at_ns=cutoff + 25,
                complete_snapshot=True, missed_interval_repaired=True,
                snapshot_refs=(bars[-1].observation_index_ref,), repair_certificate_refs=(completed,))
        collector.reconcile_after_reconnect(source_id, at_ns=cutoff + 100,
            complete_snapshot=True, missed_interval_repaired=True,
            snapshot_refs=(bars[-1].observation_index_ref,), repair_certificate_refs=(completed,))
        reconciliation, = repo.artifact_entries("OpsPublicSourceReconciliationV1")
        assert reconciliation.metadata["reconciliation"]["bar_repair_certificate_refs"] == (completed,)
