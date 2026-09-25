"""Frozen V2 action identity changes only with declared executable terms."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import FrozenMap, sha256_json
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.science.action import action_identity, freeze_action
from atlas.v2.strategies.s1_trend import S1_POLICY

from .test_session017_risk import risk_case, size


def test_action_artifact_is_separate_from_unsized_candidate(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        sizing = size(repo, case)
        artifact = freeze_action(repo, candidate=case.candidate, candidate_set=case.candidate_set,
            sizing=sizing, product=case.product, policy=S1_POLICY, v1=case.v1, v2=case.v2)
        assert case.candidate.quantity is None
        assert artifact.action.quantity == Decimal("24.5")
        assert repo.get_artifact(artifact.content_hash) is not None
        assert artifact.action.action_hash != artifact.content_hash
        assert replace(artifact, available_at_ns=artifact.available_at_ns + 1).action.action_hash == artifact.action.action_hash
        for forbidden in ("EvaluationArtifactV2", "TradePlanEnvelopeV2", "Approval", "Reservation", "Order"):
            assert repo.artifact_entries(forbidden) == ()


def test_every_frozen_action_field_has_independent_hash_effect(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = risk_case(repo)
        original = action_identity(case.candidate, size(repo, case), case.product, S1_POLICY, case.v1, case.v2)
        changes = (
            {"key": replace(original.key, contract_revision=sha256_json({"new_revision": 1}))},
            {"key": replace(original.key, venue="BINANCE")},
            {"side": "SHORT"},
            {"quantity": original.quantity + Decimal("0.1")},
            {"entry_collar": original.entry_collar + Decimal("0.01")},
            {"stop_price": original.stop_price - Decimal("0.01")},
            {"horizon_end_ns": original.horizon_end_ns + 1},
            {"management_rule": FrozenMap({**original.management_rule, "changed": True})},
            {"policy_id": "S2_COMPRESSION_BREAKOUT", "policy_hash": "f" * 64},
            {"risk_policy_hash": "a" * 64},
            {"risk_policy_v2_hash": "b" * 64},
        )
        for change in changes:
            assert replace(original, **change).action_hash != original.action_hash
        assert original.action_hash == replace(original).action_hash
        with pytest.raises(ValueError, match="risk-sized"):
            action_identity(case.candidate, replace(size(repo, case), quantity=None), case.product,
                S1_POLICY, case.v1, case.v2)
