"""An hourly metadata receipt does not multiply the exact universe key."""

from dataclasses import replace

import pytest

from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime.production import _causal_products

from .test_session017_risk import risk_case


def _index(repo, product):
    repo.register_artifact(ArtifactIndexEntryV2(product.content_hash, "ProductContractV2",
        product.content_hash, product.available_at_ns, product.available_at_ns, {"product": product.to_dict()}))


def test_hourly_unchanged_metadata_refresh_retains_one_causal_full_key(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        cutoff = case.candidate.decision_at_ns
        current = replace(case.product, effective_at_ns=case.product.effective_at_ns + 1,
            available_at_ns=cutoff)
        _index(repo, current)
        future = replace(current, effective_at_ns=cutoff + 1, available_at_ns=cutoff + 1)
        _index(repo, future)
        assert _causal_products(repo, cutoff_ns=cutoff) == (current,)
        assert _causal_products(repo, cutoff_ns=cutoff + 1) == (future,)
        assert len(repo.artifact_entries("ProductContractV2")) == 3


def test_conflicting_same_effective_product_receipts_fail_closed(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        conflict = replace(case.product, available_at_ns=case.product.available_at_ns + 1)
        _index(repo, conflict)
        with pytest.raises(ValueError, match="effective revision is ambiguous"):
            _causal_products(repo, cutoff_ns=case.candidate.decision_at_ns)
