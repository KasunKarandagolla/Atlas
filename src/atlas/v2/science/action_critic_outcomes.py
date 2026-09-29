"""Additive exact identity link from critic observations to existing matured outcomes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from atlas.v2._serialization import json_value, sha256_json, sha256_ref, strict_fields, timestamp
from atlas.v2.agent_intelligence.shadow_measurement import ActionCriticShadowObservationV1
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.science.outcomes import (
    LabelStateV2,
    MaturedOutcomeV2,
    _resolve_decision_calendar_entry,
    index_matured_outcome,
)


@dataclass(frozen=True)
class ActionCriticShadowMaturityLinkV1:
    observation_ref: str
    decision_calendar_ref: str
    decision_identity_ref: str
    matured_outcome_ref: str
    candidate_set_ref: str
    action_hash: str
    action_artifact_ref: str
    available_at_ns: int

    VERSION = "ActionCriticShadowMaturityLinkV1"

    def __post_init__(self) -> None:
        for name in ("observation_ref", "decision_calendar_ref", "decision_identity_ref",
                     "matured_outcome_ref", "candidate_set_ref", "action_hash", "action_artifact_ref"):
            sha256_ref(getattr(self, name), field=name)
        timestamp(self.available_at_ns, field="available_at_ns")

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.VERSION, **{name: getattr(self, name) for name in self.__dataclass_fields__}}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ActionCriticShadowMaturityLinkV1:
        fields = set(cls.__dataclass_fields__) | {"version"}
        row = dict(strict_fields(data, expected=fields, required=fields, name=cls.VERSION))
        if row.pop("version") != cls.VERSION:
            raise ValueError("unsupported action-critic maturity link")
        return cls(**row)


def link_action_critic_matured_outcome(repository: OpsRepository, observation_ref: str,
                                      matured_outcome_ref: str, *, as_of_ns: int
                                      ) -> ActionCriticShadowMaturityLinkV1:
    """Create a deterministic immutable measurement link only for one exact valid matured label."""
    timestamp(as_of_ns, field="link as-of time")
    observation_entry = repository.get_artifact(observation_ref)
    observation_body = observation_entry.metadata.get("observation") if observation_entry is not None else None
    if (observation_entry is None or observation_entry.artifact_type != ActionCriticShadowObservationV1.VERSION
            or not isinstance(observation_body, Mapping)
            or observation_entry.content_hash != observation_ref):
        raise ValueError("indexed action-critic shadow observation required")
    observation = ActionCriticShadowObservationV1.from_dict(observation_body)
    if observation.content_hash != observation_ref:
        raise ValueError("shadow observation content hash mismatch")
    if observation_entry.available_at_ns > as_of_ns or observation.recorded_at_ns > as_of_ns:
        raise ValueError("shadow observation is not yet available at the requested as-of time")

    outcome_entry = repository.get_artifact(matured_outcome_ref)
    outcome_body = outcome_entry.metadata.get("outcome") if outcome_entry is not None else None
    if (outcome_entry is None or outcome_entry.artifact_type != "MaturedOutcomeV2"
            or not isinstance(outcome_body, Mapping) or outcome_entry.content_hash != matured_outcome_ref):
        raise ValueError("indexed MaturedOutcomeV2 required")
    outcome = MaturedOutcomeV2.from_dict(json_value(outcome_body))
    if outcome.content_hash != matured_outcome_ref:
        raise ValueError("matured outcome content hash mismatch")

    decision = _resolve_decision_calendar_entry(repository, observation.decision_calendar_ref)
    if (outcome.decision_ref != observation.decision_calendar_ref
            or outcome.candidate_set_ref != observation.candidate_set_ref
            or outcome.candidate_set_ref != decision.candidate_set_ref
            or outcome.action_hash != observation.action_hash
            or outcome.action_artifact_ref != observation.action_artifact_ref
            or decision.action_hash != observation.action_hash
            or decision.action_artifact_ref != observation.action_artifact_ref):
        raise ValueError("matured outcome decision/candidate/action identity does not match observation")
    if outcome.label_state != LabelStateV2.MATURED:
        raise ValueError("unavailable, unresolved or censored outcome cannot be linked as matured")
    if (outcome.horizon_end_ns > outcome.matured_at_ns
            or outcome.matured_at_ns > outcome.available_at_ns
            or outcome.available_at_ns > as_of_ns
            or outcome_entry.available_at_ns > as_of_ns):
        raise ValueError("future matured outcome is not available at the requested as-of time")

    all_outcomes = repository.artifact_entries_by_types(("MaturedOutcomeV2",), limit=10_000)
    if len(all_outcomes) >= 10_000:
        raise ValueError("matured outcome identity inventory is ambiguous")
    decision_outcomes: list[tuple[str, MaturedOutcomeV2]] = []
    for entry in all_outcomes:
        body = entry.metadata.get("outcome")
        if not isinstance(body, Mapping):
            continue
        try:
            candidate = MaturedOutcomeV2.from_dict(json_value(body))
        except (ValueError, TypeError, KeyError):
            continue
        if candidate.decision_ref == observation.decision_calendar_ref:
            decision_outcomes.append((entry.artifact_ref, candidate))
    if len(decision_outcomes) != 1 or decision_outcomes[0][0] != matured_outcome_ref:
        raise ValueError("multiple or mismatched MaturedOutcomeV2 identities make linkage ambiguous")

    # Re-run the existing outcome validator; this only validates/re-indexes the supplied immutable label.
    if index_matured_outcome(repository, outcome) != matured_outcome_ref:
        raise ValueError("existing matured outcome validation returned a different identity")
    link = ActionCriticShadowMaturityLinkV1(observation_ref, observation.decision_calendar_ref,
        decision.decision_identity_ref, matured_outcome_ref, observation.candidate_set_ref,
        observation.action_hash, observation.action_artifact_ref,
        max(observation_entry.available_at_ns, outcome.available_at_ns))
    repository.register_artifact(ArtifactIndexEntryV2(link.content_hash, link.VERSION, link.content_hash,
        link.available_at_ns, link.available_at_ns, {"link": link.to_dict()}))
    return link
