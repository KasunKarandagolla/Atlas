"""Frozen risk-sized research action identity, without TradePlan authority."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from atlas.domain.money import canonical_decimal_str as _c
from atlas.domain.risk import RiskPolicy
from atlas.v2._serialization import FrozenMap, canonical_json, sha256_json, sha256_ref
from atlas.v2.contracts import CandidateActionV2, CandidateSetV2, PolicySpecV2
from atlas.v2.instruments import InstrumentKeyV2, ProductContractV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.risk import RiskPolicyV2, SizingDecisionV2, SizingStatus

ACTION_VERSION = "V2_FROZEN_RISK_SIZED_ACTION_V2"


@dataclass(frozen=True)
class FrozenActionV2:
    key: InstrumentKeyV2
    side: str
    quantity: Decimal
    product_ref: str
    entry_rule: FrozenMap
    collar_rule: FrozenMap
    entry_reference: Decimal
    entry_collar: Decimal
    stop_price: Decimal
    entry_trigger_basis: str
    stop_trigger_basis: str
    management_rule: FrozenMap
    time_exit_rule: FrozenMap
    horizon_end_ns: int
    policy_id: str
    policy_version: str
    policy_hash: str
    risk_policy_hash: str
    risk_policy_v2_hash: str

    def __post_init__(self) -> None:
        if not isinstance(self.key, InstrumentKeyV2) or self.side not in ("LONG", "SHORT"):
            raise ValueError("frozen action key/side invalid")
        for name in ("quantity", "entry_reference", "entry_collar", "stop_price"):
            value = getattr(self, name)
            if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
                raise ValueError(f"frozen action {name} requires positive Decimal")
        if type(self.horizon_end_ns) is not int or self.horizon_end_ns <= 0:
            raise ValueError("frozen action horizon invalid")
        for name in ("product_ref", "policy_hash", "risk_policy_hash", "risk_policy_v2_hash"):
            sha256_ref(getattr(self, name), field=name)
        if not all(getattr(self, name).strip() for name in (
                "entry_trigger_basis", "stop_trigger_basis", "policy_id", "policy_version")):
            raise ValueError("frozen action policy identity invalid")
        for name in ("entry_rule", "collar_rule", "management_rule", "time_exit_rule"):
            value = getattr(self, name)
            if not isinstance(value, (FrozenMap, Mapping)):
                raise ValueError(f"frozen action {name} must be a mapping")
            object.__setattr__(self, name, FrozenMap(value))

    def to_dict(self) -> dict[str, Any]:
        return {"version": ACTION_VERSION, "key": self.key.to_dict(), "side": self.side,
                "quantity": _c(self.quantity), "product_ref": self.product_ref,
                "entry_rule": self.entry_rule.to_dict(), "collar_rule": self.collar_rule.to_dict(),
                "entry_reference": _c(self.entry_reference), "entry_collar": _c(self.entry_collar),
                "stop_price": _c(self.stop_price), "entry_trigger_basis": self.entry_trigger_basis,
                "stop_trigger_basis": self.stop_trigger_basis,
                "management_rule": self.management_rule.to_dict(), "time_exit_rule": self.time_exit_rule.to_dict(),
                "horizon_end_ns": self.horizon_end_ns, "policy_id": self.policy_id,
                "policy_version": self.policy_version, "policy_hash": self.policy_hash,
                "risk_policy_hash": self.risk_policy_hash,
                "risk_policy_v2_hash": self.risk_policy_v2_hash}

    @property
    def action_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class ActionArtifactV2:
    action: FrozenActionV2
    candidate_ref: str
    sizing_ref: str
    candidate_set_ref: str
    available_at_ns: int

    def to_dict(self) -> dict[str, Any]:
        return {"version": "V2_SHADOW_ACTION_ARTIFACT_V1", "action_hash": self.action.action_hash,
                "candidate_ref": self.candidate_ref, "sizing_ref": self.sizing_ref,
                "candidate_set_ref": self.candidate_set_ref, "product_ref": self.action.product_ref,
                "risk_policy_hash": self.action.risk_policy_hash,
                "risk_policy_v2_hash": self.action.risk_policy_v2_hash,
                "policy_hash": self.action.policy_hash,
                "available_at_ns": self.available_at_ns}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def action_identity(candidate: CandidateActionV2, sizing: SizingDecisionV2,
                    product: ProductContractV2, policy: PolicySpecV2,
                    v1: RiskPolicy, v2: RiskPolicyV2) -> FrozenActionV2:
    if (sizing.status != SizingStatus.SIZED or sizing.quantity is None or sizing.quantity <= 0 or
            candidate.quantity is not None or candidate.policy_hash != policy.policy_hash or
            candidate.key != product.key or sizing.product_ref != product.content_hash or
            sizing.candidate_ref != candidate.content_hash or
            sizing.risk_policy_hash != v1.policy_hash() or sizing.risk_policy_v2_hash != v2.policy_hash):
        raise ValueError("risk-sized action identity mismatch")
    return FrozenActionV2(candidate.key, candidate.side.value, sizing.quantity, product.content_hash,
        policy.entry_rule, policy.collar_rule, candidate.entry_reference,
        candidate.entry_collar, candidate.stop_price, policy.trigger_basis, "MARK_PRICE",
        policy.management_rule, policy.time_exit_rule, candidate.horizon_end_ns,
        policy.policy_id, policy.version, policy.policy_hash, v1.policy_hash(), v2.policy_hash)


def freeze_action(repo: OpsRepository, *, candidate: CandidateActionV2, candidate_set: CandidateSetV2,
                  sizing: SizingDecisionV2, product: ProductContractV2, policy: PolicySpecV2,
                  v1: RiskPolicy, v2: RiskPolicyV2, clock_ns: Callable[[], int] | None = None) -> ActionArtifactV2:
    """Index a pure research action only after exact selected sizing evidence exists."""
    if candidate_set.selected_candidate_id != candidate.candidate_id or sizing.candidate_set_ref != candidate_set.content_hash:
        raise ValueError("selected CandidateSet mismatch")
    indexed_sizing = repo.get_artifact(sizing.content_hash)
    if indexed_sizing is None or indexed_sizing.artifact_type != "SizingDecisionV2" or canonical_json(
            indexed_sizing.metadata.get("sizing")) != canonical_json(sizing.to_dict()):
        raise ValueError("sizing evidence must already be indexed")
    from atlas.v2.chronology import record_computation, sample
    identity_ref = sha256_json({"version": "ActionComputationIdentityV1", "sizing_ref": sizing.content_hash,
        "candidate_ref": candidate.content_hash, "candidate_set_ref": candidate_set.content_hash})
    if clock_ns and (prior_identity := repo.get_artifact(identity_ref)) is not None:
        if (prior_identity.artifact_type != "ActionComputationIdentityV1"
                or prior_identity.content_hash != sha256_json(prior_identity.metadata)
                or prior_identity.metadata.get("authority") != "ZERO"):
            raise ValueError("sealed action identity receipt conflicts")
        prior = repo.get_artifact(str(prior_identity.metadata.get("action_ref")))
        if prior is None or prior.artifact_type != "ActionArtifactV2":
            raise ValueError("sealed action computation is missing")
        action = action_identity(candidate, sizing, product, policy, v1, v2)
        artifact = ActionArtifactV2(action, candidate.content_hash, sizing.content_hash,
            candidate_set.content_hash, prior.available_at_ns)
        from atlas.v2.chronology import causal_artifact
        consumed_at = sample(clock_ns, floor_ns=prior.available_at_ns)
        if (artifact.content_hash != prior.artifact_ref or prior.content_hash != artifact.content_hash or canonical_json(prior.metadata["action_identity"]) != canonical_json(action.to_dict())
                or not causal_artifact(repo, prior.artifact_ref, cutoff_ns=candidate.decision_at_ns,
                    consumer_at_ns=consumed_at, deadline_ns=candidate.deadline_ns)):
            raise ValueError("sealed action computation identity conflicts")
        return artifact
    started = sample(clock_ns, floor_ns=sizing.available_at_ns) if clock_ns else sizing.available_at_ns
    action = action_identity(candidate, sizing, product, policy, v1, v2)
    finished = sample(clock_ns, floor_ns=started) if clock_ns else started
    available = sample(clock_ns, floor_ns=finished) if clock_ns else finished
    if available > candidate.deadline_ns:
        raise ValueError("action computation missed the unextendable deadline")
    artifact = ActionArtifactV2(action, candidate.content_hash, sizing.content_hash,
                                candidate_set.content_hash, available)
    refs = (candidate.content_hash, sizing.content_hash, candidate_set.content_hash,
            product.content_hash, v1.policy_hash(), v2.policy_hash)
    if any((item := repo.get_artifact(ref)) is None or item.available_at_ns > artifact.available_at_ns for ref in refs):
        raise ValueError("action has unavailable input")
    repo.register_artifact(ArtifactIndexEntryV2(artifact.content_hash, "ActionArtifactV2",
        artifact.content_hash, artifact.available_at_ns, artifact.available_at_ns,
        {"action_artifact": artifact.to_dict(), "action_identity": action.to_dict(),
         "input_refs": sorted(refs)}))
    if clock_ns:
        record_computation(repo, artifact_ref=artifact.content_hash, information_cutoff_ns=candidate.decision_at_ns,
            started_ns=started, finished_ns=finished, available_ns=available,
            input_refs=refs, deadline_ns=candidate.deadline_ns)
        body = {"action_ref": artifact.content_hash, "authority": "ZERO"}
        repo.register_artifact(ArtifactIndexEntryV2(identity_ref, "ActionComputationIdentityV1",
            sha256_json(body), available, available, body))
    return artifact
