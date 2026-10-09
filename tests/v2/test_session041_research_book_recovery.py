"""A deferred sidecar consumes sealed book features, never a restarted book."""

from __future__ import annotations

import pytest

from atlas.v2.chronology import record_computation
from atlas.v2.data.microstructure import SequenceValidBookV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.full_strategy_surface import load_full_strategy_inputs

from .session023_support import research_case
from .test_session014_core import KEY
from .test_session017_risk import CUTOFF


def _feature():
    return SequenceValidBookV2(instrument=KEY, source_id="RECOVERY_FIXTURE",
        channel="orderbook.50.BTCUSDT", sequence_semantics="BYBIT_U", warmup_ns=0,
        stale_ns=1000, declared_cadence_ns=10).feature(cutoff_ns=CUTOFF)


def test_restart_reuses_exact_sealed_s4_feature_and_preserves_missingness(tmp_path):
    path = tmp_path / "ops.sqlite"
    feature = _feature()
    with OpsRepository(path) as repo:
        universe = research_case(repo).universe
        repo.register_artifact(ArtifactIndexEntryV2(feature.content_hash, "S4FeatureArtifactV2",
            feature.content_hash, CUTOFF + 1, CUTOFF + 1, {"feature": feature.to_dict()}))
        record_computation(repo, artifact_ref=feature.content_hash, information_cutoff_ns=CUTOFF,
            started_ns=CUTOFF + 1, finished_ns=CUTOFF + 1, available_ns=CUTOFF + 1,
            input_refs=feature.input_refs, deadline_ns=CUTOFF + 1)
    with OpsRepository(path) as repo:
        loaded = load_full_strategy_inputs(repo, universe, CUTOFF, {}, lambda: CUTOFF + 10,
            prepared_s4_features={KEY: feature})
        assert loaded.s4_features[KEY].content_hash == feature.content_hash
        assert loaded.s4_features[KEY].estimable is False
        assert repo.get_artifact(feature.content_hash).available_at_ns == CUTOFF + 1
        restarted_book = SequenceValidBookV2(instrument=KEY, source_id="RECOVERY_FIXTURE",
            channel="orderbook.50.BTCUSDT", sequence_semantics="BYBIT_U")
        with pytest.raises(ValueError, match="cannot be replaced"):
            load_full_strategy_inputs(repo, universe, CUTOFF, {}, lambda: CUTOFF + 10,
                prepared_s4_features={KEY: feature}, sequence_books={KEY: restarted_book})


def test_unsealed_post_cutoff_book_feature_cannot_be_recovered_as_causal_input(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        universe = research_case(repo).universe
        feature = _feature()
        repo.register_artifact(ArtifactIndexEntryV2(feature.content_hash, "S4FeatureArtifactV2",
            feature.content_hash, CUTOFF + 1, CUTOFF + 1, {"feature": feature.to_dict()}))
        with pytest.raises(ValueError, match="cutoff-bound causal evidence"):
            load_full_strategy_inputs(repo, universe, CUTOFF, {}, lambda: CUTOFF + 10,
                prepared_s4_features={KEY: feature})
