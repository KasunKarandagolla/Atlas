"""Small actual composition fixture; archive I/O is exercised separately."""
from atlas.v2._serialization import sha256_json
from atlas.v2.data.bars import BarIntervalV2
from atlas.v2.data.health import PublicSourceHealthV2, PublicSourceStateV2
from atlas.v2.data.history import IndexedCausalBarV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.ops_supervisor import OpsDecisionEventV1
from atlas.v2.runtime.production import IndexedProductionEventInputsV1

from .test_session014_core import bar
from .test_session017_risk import risk_case
from .test_session037_chronology import Clock
from .test_session037_history_runtime import _register


def test_small_no_candidate_public_composition_seals_actual_chronology(tmp_path, monkeypatch):
    from atlas.v2.runtime import production
    with OpsRepository(tmp_path / 'ops.sqlite') as repo:
        case = risk_case(repo)
        cutoff = case.candidate.decision_at_ns
        frames = {}
        for interval in (BarIntervalV2.M15, BarIntervalV2.H1, BarIntervalV2.H4, BarIntervalV2.M1):
            last = cutoff // interval.duration_ns - 1
            bars = tuple(bar(i, interval=interval, key=case.product.key) for i in range(last - 2, last + 1))
            frames[interval] = tuple(IndexedCausalBarV2(b, sha256_json({
                "artifact_type": "PublicObservationIndexV2", "record_id": b.raw.record_id})) for b in bars)
            _register(repo, frames[interval])
        trigger = frames[BarIntervalV2.M15][-1].bar
        health = PublicSourceHealthV2(trigger.raw.source_id, cutoff, cutoff, PublicSourceStateV2.HEALTHY_CURRENT,
            sha256_json({'source': 'small-public-composition'}), 'fixture')
        repo.register_artifact(ArtifactIndexEntryV2(health.content_hash, 'PublicSourceHealthV2', health.content_hash,
            cutoff, cutoff, {'health': health.to_dict()}))
        repo.record_source_health(health.to_ops_record())
        body = {'version': 'OPS_PUBLIC_FINAL_BAR_TRIGGER_V1', 'information_cutoff_ns': cutoff,
            'product_ref': case.product.content_hash, 'bar_ref': trigger.content_hash}
        trigger_ref = sha256_json(body)
        repo.register_artifact(ArtifactIndexEntryV2(trigger_ref, 'OpsPublicFinalBarTriggerV1', trigger_ref,
            cutoff, cutoff, {'trigger': body}))
        event = OpsDecisionEventV1(sha256_json({'small': trigger_ref}), 'CONFIRMED_15M_CLOSE', trigger.raw.source_id,
            trigger_ref, cutoff, None, cutoff, cutoff, cutoff, cutoff + 5_000_000_000,
            (trigger_ref, case.product.content_hash))
        from atlas.v2.runtime import active_history
        monkeypatch.setattr(active_history, 'reconstruct_native_bars_from_index_page',
            lambda *a, interval, **k: frames[interval])
        monkeypatch.setattr(production, '_indexed_quote_and_mark', lambda *a, **k: (None, None, ()))
        clock = Clock(cutoff)
        service_calls = []
        def service(repository):
            assert not repository._connection.in_transaction
            service_calls.append(1)
        provider = IndexedProductionEventInputsV1(clock_ns=clock, stream_service=service)
        result = provider.resolve(repo, event)
        assert result.universe is not None and result.candidates == ()
        assert result.universe.envelope.available_at_ns > cutoff
        assert result.causal_feature_refs
        assert len(service_calls) >= 8
        before = len(service_calls)
        assert provider.resolve(repo, event) == result
        assert len(service_calls) == before
