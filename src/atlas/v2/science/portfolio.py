"""Synchronized 24-hour portfolio path primitives; no ES or economic admission."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from atlas.domain.money import canonical_decimal_str as _c
from atlas.v2._serialization import canonical_json, sha256_json, sha256_ref
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository

from .replay import HOUR_NS, PolicyPayoffV2, ReplayStatusV2


@dataclass(frozen=True)
class PortfolioPathComponentV2:
    kind: str
    payoff: Decimal
    source_ref: str

    def __post_init__(self) -> None:
        if self.kind not in ("OPEN", "PENDING", "UNKNOWN", "PARTIAL"):
            raise ValueError("portfolio component kind invalid")
        if not isinstance(self.payoff, Decimal) or not self.payoff.is_finite():
            raise ValueError("portfolio payoff must be finite Decimal")
        sha256_ref(self.source_ref, field="source_ref")

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.kind, "payoff": _c(self.payoff), "source_ref": self.source_ref}


@dataclass(frozen=True)
class ExistingPortfolioPathV2:
    common_path_id: str
    scenario_manifest_ref: str
    decision_at_ns: int
    available_at_ns: int
    completeness_ref: str
    components: tuple[PortfolioPathComponentV2, ...]

    def __post_init__(self) -> None:
        for name in ("common_path_id", "scenario_manifest_ref", "completeness_ref"):
            sha256_ref(getattr(self, name), field=name)
        if (type(self.decision_at_ns) is not int or type(self.available_at_ns) is not int or
                self.decision_at_ns < 0 or self.available_at_ns < 0):
            raise ValueError("portfolio path times invalid")
        refs = tuple(x.source_ref for x in self.components)
        if len(set(refs)) != len(refs):
            raise ValueError("duplicate portfolio component")

    @property
    def payoff(self) -> Decimal:
        return sum((x.payoff for x in self.components), Decimal("0"))

    @property
    def horizon_end_ns(self) -> int:
        return self.decision_at_ns + 24 * HOUR_NS

    def to_dict(self) -> dict[str, Any]:
        return {"version": "V2_EXISTING_PORTFOLIO_COMMON_PATH_V1", "common_path_id": self.common_path_id,
                "scenario_manifest_ref": self.scenario_manifest_ref,
                "decision_at_ns": self.decision_at_ns, "available_at_ns": self.available_at_ns,
                "horizon_end_ns": self.horizon_end_ns, "completeness_ref": self.completeness_ref,
                "components": [x.to_dict() for x in self.components], "payoff": _c(self.payoff)}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def index_existing_portfolio_path(repo: OpsRepository, path: ExistingPortfolioPathV2) -> str:
    refs = {path.completeness_ref, path.scenario_manifest_ref} | {x.source_ref for x in path.components}
    if any((entry := repo.get_artifact(ref)) is None or entry.available_at_ns > path.available_at_ns for ref in refs):
        raise ValueError("existing/pending portfolio support unavailable")
    repo.register_artifact(ArtifactIndexEntryV2(path.content_hash, "ExistingPortfolioPathV2",
        path.content_hash, path.available_at_ns, path.available_at_ns,
        {"portfolio": path.to_dict(), "input_refs": sorted(refs)}))
    return path.content_hash


@dataclass(frozen=True)
class PairedPortfolioPayoffV2:
    common_path_id: str
    scenario_manifest_ref: str
    action_hash: str
    existing_portfolio_ref: str
    candidate_payoff_ref: str
    existing_payoff: Decimal
    candidate_payoff: Decimal
    combined_payoff: Decimal
    horizon_end_ns: int
    available_at_ns: int

    def to_dict(self) -> dict[str, Any]:
        return {"version": "V2_PAIRED_PORTFOLIO_PAYOFF_V1", "common_path_id": self.common_path_id,
                "scenario_manifest_ref": self.scenario_manifest_ref, "action_hash": self.action_hash,
                "existing_portfolio_ref": self.existing_portfolio_ref,
                "candidate_payoff_ref": self.candidate_payoff_ref,
                "existing_payoff": _c(self.existing_payoff), "candidate_payoff": _c(self.candidate_payoff),
                "combined_payoff": _c(self.combined_payoff), "horizon_end_ns": self.horizon_end_ns,
                "available_at_ns": self.available_at_ns}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def pair_common_path(repo: OpsRepository, existing: ExistingPortfolioPathV2,
                     candidate: PolicyPayoffV2, *, available_at_ns: int) -> PairedPortfolioPayoffV2:
    """Carry closed candidate cash through 24h without scheduling another trade."""
    if (candidate.common_path_id != existing.common_path_id or
            candidate.scenario_manifest_ref != existing.scenario_manifest_ref or
            candidate.existing_portfolio_ref != existing.content_hash or
            candidate.portfolio_horizon_end_ns != existing.horizon_end_ns):
        raise ValueError("common path/horizon identity mismatch")
    if candidate.status == ReplayStatusV2.NOT_ESTIMABLE or candidate.payoff is None or candidate.remaining_quantity:
        raise ValueError("unresolved candidate path cannot be paired as a known payoff")
    a = repo.get_artifact(existing.content_hash)
    b = repo.get_artifact(candidate.content_hash)
    if (a is None or b is None or a.available_at_ns > available_at_ns or b.available_at_ns > available_at_ns or
            canonical_json(a.metadata.get("portfolio")) != canonical_json(existing.to_dict()) or
            canonical_json(b.metadata.get("payoff")) != canonical_json(candidate.to_dict())):
        raise ValueError("paired portfolio artifacts unavailable")
    result = PairedPortfolioPayoffV2(existing.common_path_id, existing.scenario_manifest_ref,
        candidate.action_hash, existing.content_hash, candidate.content_hash, existing.payoff,
        candidate.payoff, existing.payoff + candidate.payoff, existing.horizon_end_ns, available_at_ns)
    repo.register_artifact(ArtifactIndexEntryV2(result.content_hash, "PairedPortfolioPayoffV2",
        result.content_hash, available_at_ns, available_at_ns, result.to_dict()))
    return result
