"""Research-only V2 risk evidence and deterministic hard-constraint sizing."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_FLOOR, Decimal
from enum import StrEnum
from typing import Any

from atlas.domain.enums import Side as V1Side
from atlas.domain.money import canonical_decimal_str as _c
from atlas.domain.risk import RiskPolicy
from atlas.science.stresses import stress_path_is_coherent

from ._serialization import canonical_json, decimal_value, sha256_json, sha256_ref, strict_fields
from .contracts import CandidateActionV2, CandidateSelectionStatus, CandidateSetV2, PolicySpecV2, V2Side
from .instruments import InstrumentKeyV2, ProductContractV2, TradingStatusV2, UniverseContractV2
from .memory.repository import ArtifactIndexEntryV2, OpsRepository
from .science.costs import FeeScheduleV2

DAY_NS = 86_400_000_000_000
SIZING_VERSION = "V2_HARD_RISK_SIZING_V1"
STRESS_VERSION = "V1_JUMP_10_LIQUIDITY_EXECUTABLE_BOUND_V1"
LEVERAGE_CONVENTION = "MAX_ROUNDED_HARD_FEASIBLE_QTY_THEN_LOWEST_ALLOWED_LEVERAGE_V1"
ZERO = Decimal("0")
ONE = Decimal("1")


def _decimal(value: Decimal, name: str, *, nonnegative: bool = True) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite() or (nonnegative and value < 0):
        raise ValueError(f"{name} requires finite {'nonnegative ' if nonnegative else ''}Decimal")
    return value


def _ns(value: int, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} requires nonnegative UTC nanoseconds")
    return value


def _indexed(repo: OpsRepository, ref: str, cutoff_ns: int, kind: str | None = None) -> ArtifactIndexEntryV2 | None:
    try:
        item = repo.get_artifact(ref)
    except ValueError:
        return None
    return item if item is not None and item.content_hash == ref and item.available_at_ns <= cutoff_ns and (
        kind is None or item.artifact_type == kind) else None


def _matches(repo: OpsRepository, ref: str, cutoff_ns: int, kind: str,
             expected: dict[str, Any]) -> bool:
    item = _indexed(repo, ref, cutoff_ns, kind)
    return item is not None and canonical_json(item.metadata) == canonical_json(expected)


def index_research_evidence(repo: OpsRepository, kind: str, ref: str, available_at_ns: int,
                            body: dict[str, Any]) -> str:
    """Index an immutable research body whose ref is its canonical content hash."""
    if sha256_json(body) != ref:
        raise ValueError("research evidence hash/content mismatch")
    repo.register_artifact(ArtifactIndexEntryV2(ref, kind, ref, available_at_ns, available_at_ns, body))
    return ref


@dataclass(frozen=True)
class RiskPolicyV2:
    policy_version: str
    effective_at_ns: int
    base_v1_risk_policy_hash: str
    rolling_24h_realized_loss_limit_frac: Decimal
    rolling_24h_new_risk_limit_frac: Decimal
    max_opening_intents_per_account: int

    SCHEMA_VERSION = 1

    def __post_init__(self) -> None:
        if not self.policy_version.strip():
            raise ValueError("RiskPolicyV2 version required")
        _ns(self.effective_at_ns, "effective_at_ns")
        sha256_ref(self.base_v1_risk_policy_hash, field="base_v1_risk_policy_hash")
        for name in ("rolling_24h_realized_loss_limit_frac", "rolling_24h_new_risk_limit_frac"):
            value = _decimal(getattr(self, name), name)
            if value > ONE:
                raise ValueError(f"{name} must be a fraction")
        if type(self.max_opening_intents_per_account) is not int or self.max_opening_intents_per_account < 1:
            raise ValueError("max_opening_intents_per_account must be positive int")

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": self.SCHEMA_VERSION, "policy_version": self.policy_version,
                "effective_at_ns": self.effective_at_ns,
                "base_v1_risk_policy_hash": self.base_v1_risk_policy_hash,
                "rolling_24h_realized_loss_limit_frac": _c(self.rolling_24h_realized_loss_limit_frac),
                "rolling_24h_new_risk_limit_frac": _c(self.rolling_24h_new_risk_limit_frac),
                "max_opening_intents_per_account": self.max_opening_intents_per_account}

    @property
    def policy_hash(self) -> str:
        return sha256_json({"contract_type": "RiskPolicyV2", "policy": self.to_dict()})

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RiskPolicyV2:
        fields = {"schema_version", "policy_version", "effective_at_ns", "base_v1_risk_policy_hash",
                  "rolling_24h_realized_loss_limit_frac", "rolling_24h_new_risk_limit_frac",
                  "max_opening_intents_per_account"}
        d = strict_fields(data, expected=fields, required=fields, name="RiskPolicyV2")
        if d["schema_version"] != cls.SCHEMA_VERSION or type(d["schema_version"]) is not int:
            raise ValueError("unsupported RiskPolicyV2 schema")
        return cls(d["policy_version"], d["effective_at_ns"], d["base_v1_risk_policy_hash"],
                   decimal_value(d["rolling_24h_realized_loss_limit_frac"], field="rolling realized", wire=True),
                   decimal_value(d["rolling_24h_new_risk_limit_frac"], field="rolling new risk", wire=True),
                   d["max_opening_intents_per_account"])


def index_risk_policies(repo: OpsRepository, v1: RiskPolicy, v2: RiskPolicyV2) -> None:
    if v1.policy_hash() != v2.base_v1_risk_policy_hash:
        raise ValueError("V1/V2 risk policy binding mismatch")
    repo.register_artifact(ArtifactIndexEntryV2(v1.policy_hash(), "RiskPolicyV1", v1.policy_hash(),
        v1.policy_effective_at_ns, v1.policy_effective_at_ns, {"policy": v1.to_dict()}))
    repo.register_artifact(ArtifactIndexEntryV2(v2.policy_hash, "RiskPolicyV2", v2.policy_hash,
        v2.effective_at_ns, v2.effective_at_ns, {"policy": v2.to_dict()}))


def index_risk_evidence(repo: OpsRepository, item: ClosedV2Outcome | PossibleRiskV2 |
                        AccountRiskSnapshotV2 | VenueSizingLimitsV2 | StressBoundV2 | ProductContractV2 |
                        FeeScheduleV2) -> str:
    """Durably index one supplied research artifact after its declared sources."""
    required: tuple[str, ...]
    if isinstance(item, ProductContractV2):
        kind, body, ref, at = "ProductContractV2", {"product": item.to_dict()}, item.content_hash, item.available_at_ns
        required = (item.metadata_ref,)
    else:
        kind, body, ref, at = type(item).__name__, item.to_dict(), item.content_hash, item.available_at_ns
        if isinstance(item, AccountRiskSnapshotV2):
            required = (item.exposure_completeness_ref,) + item.closed_outcome_refs + item.pending_risk_refs + item.existing_exposure_refs
        elif isinstance(item, (PossibleRiskV2, VenueSizingLimitsV2, StressBoundV2, FeeScheduleV2)):
            required = (item.source_ref,)
        elif isinstance(item, ClosedV2Outcome):
            required = (item.position_ref,)
        else:
            required = ()
    if any(_indexed(repo, source, at) is None for source in required):
        raise ValueError("risk evidence source is unindexed or future")
    repo.register_artifact(ArtifactIndexEntryV2(ref, kind, ref, at, at, body))
    return ref


@dataclass(frozen=True)
class ClosedV2Outcome:
    close_at_ns: int
    available_at_ns: int
    realized_net_pnl: Decimal
    position_ref: str

    def __post_init__(self) -> None:
        _ns(self.close_at_ns, "close_at_ns")
        _ns(self.available_at_ns, "available_at_ns")
        if self.available_at_ns < self.close_at_ns:
            raise ValueError("closed PnL cannot be available before close")
        _decimal(self.realized_net_pnl, "realized_net_pnl", nonnegative=False)
        sha256_ref(self.position_ref, field="position_ref")

    def to_dict(self) -> dict[str, Any]:
        return {"version": "CLOSED_ATLAS_V2_NET_PNL_V1", "close_at_ns": self.close_at_ns,
                "available_at_ns": self.available_at_ns, "realized_net_pnl": _c(self.realized_net_pnl),
                "position_ref": self.position_ref}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def rolling_realized_loss(outcomes: tuple[ClosedV2Outcome, ...], cutoff_ns: int) -> tuple[Decimal, tuple[str, ...]]:
    _ns(cutoff_ns, "cutoff_ns")
    eligible = tuple(x for x in outcomes if cutoff_ns - DAY_NS < x.close_at_ns <= cutoff_ns
                     and x.available_at_ns <= cutoff_ns)
    refs = tuple(sorted(x.content_hash for x in eligible))
    if len(set(refs)) != len(refs):
        raise ValueError("duplicate closed outcome")
    return max(ZERO, -sum((x.realized_net_pnl for x in eligible), ZERO)), refs


class ExposureKind(StrEnum):
    OPEN = "OPEN"
    PENDING = "PENDING"
    UNKNOWN = "UNKNOWN"
    PARTIAL = "PARTIAL"


@dataclass(frozen=True)
class PossibleRiskV2:
    kind: ExposureKind
    key: InstrumentKeyV2
    available_at_ns: int
    possible_normal_loss: Decimal
    possible_stress_loss: Decimal
    possible_notional: Decimal
    signed_beta_notional: Decimal
    possible_margin: Decimal
    possible_venue_collateral: Decimal
    source_ref: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", ExposureKind(self.kind))
        _ns(self.available_at_ns, "available_at_ns")
        for name in ("possible_normal_loss", "possible_stress_loss", "possible_notional",
                     "possible_margin", "possible_venue_collateral"):
            _decimal(getattr(self, name), name)
        _decimal(self.signed_beta_notional, "signed_beta_notional", nonnegative=False)
        sha256_ref(self.source_ref, field="source_ref")

    def to_dict(self) -> dict[str, Any]:
        return {"version": "POSSIBLE_RISK_V1", "kind": self.kind.value, "key": self.key.to_dict(),
                "available_at_ns": self.available_at_ns, "possible_normal_loss": _c(self.possible_normal_loss),
                "possible_stress_loss": _c(self.possible_stress_loss), "possible_notional": _c(self.possible_notional),
                "signed_beta_notional": _c(self.signed_beta_notional), "possible_margin": _c(self.possible_margin),
                "possible_venue_collateral": _c(self.possible_venue_collateral), "source_ref": self.source_ref}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class AccountRiskSnapshotV2:
    account_scope: str
    available_at_ns: int
    eligible_equity: Decimal
    margin_available: Decimal
    current_margin: Decimal
    drawdown: Decimal
    existing_open_normal_loss: Decimal
    pending_reserved_normal_loss: Decimal
    gross_notional: Decimal
    instrument_notional: Decimal
    beta_notional: Decimal
    venue_collateral: Decimal
    existing_portfolio_es: Decimal
    opening_intents: int
    closed_outcome_refs: tuple[str, ...]
    pending_risk_refs: tuple[str, ...]
    existing_exposure_refs: tuple[str, ...]
    exposure_completeness_ref: str
    operational_status: str = "CURRENT"

    def __post_init__(self) -> None:
        if not self.account_scope.strip() or self.operational_status not in ("CURRENT", "UNKNOWN", "STALE"):
            raise ValueError("account scope/status invalid")
        _ns(self.available_at_ns, "available_at_ns")
        for name in ("eligible_equity", "margin_available", "current_margin", "drawdown",
                     "existing_open_normal_loss", "pending_reserved_normal_loss", "gross_notional",
                     "instrument_notional", "venue_collateral", "existing_portfolio_es"):
            _decimal(getattr(self, name), name)
        _decimal(self.beta_notional, "beta_notional", nonnegative=False)
        if type(self.opening_intents) is not int or self.opening_intents < 0:
            raise ValueError("opening_intents must be nonnegative int")
        for name in ("closed_outcome_refs", "pending_risk_refs", "existing_exposure_refs"):
            refs = tuple(sorted(getattr(self, name)))
            if len(refs) != len(set(refs)):
                raise ValueError(f"duplicate {name}")
            for ref in refs:
                sha256_ref(ref, field=name)
            object.__setattr__(self, name, refs)
        sha256_ref(self.exposure_completeness_ref, field="exposure_completeness_ref")

    def to_dict(self) -> dict[str, Any]:
        names = ("eligible_equity", "margin_available", "current_margin", "drawdown",
                 "existing_open_normal_loss", "pending_reserved_normal_loss", "gross_notional",
                 "instrument_notional", "beta_notional", "venue_collateral", "existing_portfolio_es")
        return {"version": "SHADOW_ACCOUNT_RISK_SNAPSHOT_V1", "account_scope": self.account_scope,
                "available_at_ns": self.available_at_ns, **{name: _c(getattr(self, name)) for name in names},
                "opening_intents": self.opening_intents, "closed_outcome_refs": list(self.closed_outcome_refs),
                "pending_risk_refs": list(self.pending_risk_refs), "existing_exposure_refs": list(self.existing_exposure_refs),
                "exposure_completeness_ref": self.exposure_completeness_ref,
                "operational_status": self.operational_status}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class VenueSizingLimitsV2:
    key: InstrumentKeyV2
    product_ref: str
    available_at_ns: int
    allowed_leverages: tuple[Decimal, ...]
    account_leverage_limit: Decimal
    margin_addon_frac: Decimal
    source_ref: str

    def __post_init__(self) -> None:
        sha256_ref(self.product_ref, field="product_ref")
        sha256_ref(self.source_ref, field="source_ref")
        _ns(self.available_at_ns, "available_at_ns")
        _decimal(self.account_leverage_limit, "account_leverage_limit")
        _decimal(self.margin_addon_frac, "margin_addon_frac")
        if self.margin_addon_frac > ONE:
            raise ValueError("margin addon must be a fraction")
        levers = tuple(sorted(self.allowed_leverages))
        if not levers or len(levers) != len(set(levers)) or any(_decimal(x, "leverage") <= 0 for x in levers):
            raise ValueError("explicit positive allowed leverage set required")
        object.__setattr__(self, "allowed_leverages", levers)

    def to_dict(self) -> dict[str, Any]:
        return {"version": "VENUE_SIZING_LIMITS_V1", "key": self.key.to_dict(), "product_ref": self.product_ref,
                "available_at_ns": self.available_at_ns, "allowed_leverages": [_c(x) for x in self.allowed_leverages],
                "account_leverage_limit": _c(self.account_leverage_limit),
                "margin_addon_frac": _c(self.margin_addon_frac), "source_ref": self.source_ref}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class StressBoundV2:
    key: InstrumentKeyV2
    available_at_ns: int
    worst_executable_exit_price: Decimal
    source_ref: str
    profile_version: str = STRESS_VERSION

    def __post_init__(self) -> None:
        _ns(self.available_at_ns, "available_at_ns")
        _decimal(self.worst_executable_exit_price, "worst_executable_exit_price")
        if self.worst_executable_exit_price <= 0 or self.profile_version != STRESS_VERSION:
            raise ValueError("unsupported stress bound")
        sha256_ref(self.source_ref, field="source_ref")

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.profile_version, "key": self.key.to_dict(),
                "available_at_ns": self.available_at_ns,
                "worst_executable_exit_price": _c(self.worst_executable_exit_price),
                "source_ref": self.source_ref}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


class SizingStatus(StrEnum):
    SIZED = "SIZED"
    NO_TRADE = "NO_TRADE"
    NOT_ESTIMABLE = "NOT_ESTIMABLE"


@dataclass(frozen=True)
class SizingDecisionV2:
    candidate_ref: str
    candidate_set_ref: str
    selected_candidate_id: str
    risk_policy_hash: str
    risk_policy_v2_hash: str
    account_snapshot_ref: str
    product_ref: str
    risk_input_refs: tuple[str, ...]
    quantity: Decimal | None
    normal_risk: Decimal | None
    stress_risk: Decimal | None
    notional: Decimal | None
    margin: Decimal | None
    leverage: Decimal | None
    rolling_loss_consumed: Decimal | None
    rolling_new_risk_consumed: Decimal | None
    status: SizingStatus
    reasons: tuple[str, ...]
    available_at_ns: int

    def to_dict(self) -> dict[str, Any]:
        return {"version": SIZING_VERSION, "candidate_ref": self.candidate_ref,
                "candidate_set_ref": self.candidate_set_ref, "selected_candidate_id": self.selected_candidate_id,
                "risk_policy_hash": self.risk_policy_hash, "risk_policy_v2_hash": self.risk_policy_v2_hash,
                "account_snapshot_ref": self.account_snapshot_ref, "product_ref": self.product_ref,
                "risk_input_refs": list(self.risk_input_refs), "quantity": _c(self.quantity) if self.quantity is not None else None,
                "normal_risk": _c(self.normal_risk) if self.normal_risk is not None else None,
                "stress_risk": _c(self.stress_risk) if self.stress_risk is not None else None,
                "notional": _c(self.notional) if self.notional is not None else None,
                "margin": _c(self.margin) if self.margin is not None else None,
                "leverage": _c(self.leverage) if self.leverage is not None else None,
                "rolling_loss_consumed": _c(self.rolling_loss_consumed) if self.rolling_loss_consumed is not None else None,
                "rolling_new_risk_consumed": _c(self.rolling_new_risk_consumed) if self.rolling_new_risk_consumed is not None else None,
                "status": self.status.value, "reasons": list(self.reasons), "available_at_ns": self.available_at_ns}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def _floor_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


def _selected(repo: OpsRepository, candidate_set: CandidateSetV2, candidate: CandidateActionV2,
              universe: UniverseContractV2, policy: PolicySpecV2, cutoff_ns: int) -> None:
    indexed_set = _indexed(repo, candidate_set.content_hash, cutoff_ns, "CandidateSetV2")
    indexed_candidate = _indexed(repo, candidate.content_hash, cutoff_ns, "CandidateActionV2")
    if indexed_set is None or indexed_candidate is None:
        raise ValueError("selected CandidateSet and candidate must be durably indexed")
    if canonical_json(indexed_set.metadata.get("candidate_set")) != candidate_set.to_canonical_json():
        raise ValueError("indexed CandidateSet content mismatch")
    if canonical_json(indexed_candidate.metadata.get("candidate")) != candidate.to_canonical_json():
        raise ValueError("indexed candidate content mismatch")
    if candidate_set.selection_status != CandidateSelectionStatus.SELECTED or candidate_set.selected_candidate_id != candidate.candidate_id:
        raise ValueError("only selected candidate may be sized")
    if candidate_set.selection_policy_hash != universe.selection_policy_hash or candidate_set.universe_ref != universe.content_hash:
        raise ValueError("CandidateSet universe/selection identity mismatch")
    if candidate.content_hash not in candidate_set.envelope.input_refs or candidate.policy_hash != policy.policy_hash:
        raise ValueError("candidate policy/selection identity mismatch")
    if candidate.quantity is not None or candidate.deadline_ns < cutoff_ns or candidate.decision_at_ns > cutoff_ns:
        raise ValueError("selected candidate is sized, expired or from future event")
    if not any(x.candidate_id == candidate.candidate_id and x.key == candidate.key and x.side == candidate.side
               for x in candidate_set.candidates):
        raise ValueError("selected candidate entry mismatch")
    if _indexed(repo, universe.content_hash, cutoff_ns, "UniverseContractV2") is None:
        raise ValueError("universe artifact unavailable")


def size_selected_candidate(repo: OpsRepository, *, candidate_set: CandidateSetV2,
                            candidate: CandidateActionV2, universe: UniverseContractV2,
                            policy: PolicySpecV2, product: ProductContractV2, v1: RiskPolicy,
                            v2: RiskPolicyV2, account: AccountRiskSnapshotV2,
                            exposures: tuple[PossibleRiskV2, ...], outcomes: tuple[ClosedV2Outcome, ...],
                            venue: VenueSizingLimitsV2, stress: StressBoundV2,
                            fee: FeeScheduleV2,
                            cutoff_ns: int) -> SizingDecisionV2:
    """Size the selected shadow candidate from indexed, cutoff-causal hard-risk evidence."""
    _selected(repo, candidate_set, candidate, universe, policy, cutoff_ns)
    if v1.policy_hash() != v2.base_v1_risk_policy_hash:
        raise ValueError("V1/V2 risk policy binding mismatch")
    base_refs = {candidate_set.content_hash, candidate.content_hash, universe.content_hash}
    optional = (v1.policy_hash(), v2.policy_hash, account.content_hash, product.content_hash,
                venue.content_hash, stress.content_hash, fee.content_hash)
    valid_refs = base_refs | {ref for ref in optional if _indexed(repo, ref, cutoff_ns) is not None}
    def not_est(reason: str) -> SizingDecisionV2:
        return persist(SizingStatus.NOT_ESTIMABLE, (reason,))
    def no_trade(reason: str) -> SizingDecisionV2:
        return persist(SizingStatus.NO_TRADE, (reason,))
    def persist(status: SizingStatus, why: tuple[str, ...], *, q: Decimal | None = None,
                normal: Decimal | None = None, stressed: Decimal | None = None,
                notional: Decimal | None = None, margin: Decimal | None = None,
                leverage: Decimal | None = None, loss: Decimal | None = None,
                rolling: Decimal | None = None) -> SizingDecisionV2:
        decision = SizingDecisionV2(candidate.content_hash, candidate_set.content_hash, candidate.candidate_id,
            v1.policy_hash(), v2.policy_hash, account.content_hash, product.content_hash, tuple(sorted(valid_refs)),
            q, normal, stressed, notional, margin, leverage, loss, rolling, status, why, cutoff_ns)
        repo.register_artifact(ArtifactIndexEntryV2(decision.content_hash, "SizingDecisionV2", decision.content_hash,
            cutoff_ns, cutoff_ns, {"sizing": decision.to_dict(), "diagnostic_reasons": list(why)}))
        return decision
    if (v1.policy_effective_at_ns > cutoff_ns or v2.effective_at_ns > cutoff_ns or
            account.available_at_ns > cutoff_ns or fee.available_at_ns > cutoff_ns or product.available_at_ns > cutoff_ns or
            product.effective_at_ns > cutoff_ns or venue.available_at_ns > cutoff_ns or
            stress.available_at_ns > cutoff_ns):
        return not_est("FUTURE_REQUIRED_RISK_EVIDENCE")
    required_artifacts = ((v1.policy_hash(), "RiskPolicyV1", {"policy": v1.to_dict()}),
                          (v2.policy_hash, "RiskPolicyV2", {"policy": v2.to_dict()}),
                          (account.content_hash, "AccountRiskSnapshotV2", account.to_dict()),
                          (product.content_hash, "ProductContractV2", {"product": product.to_dict()}),
                          (venue.content_hash, "VenueSizingLimitsV2", venue.to_dict()),
                          (stress.content_hash, "StressBoundV2", stress.to_dict()),
                          (fee.content_hash, "FeeScheduleV2", fee.to_dict()))
    for ref, kind, body in required_artifacts:
        if not _matches(repo, ref, cutoff_ns, kind, body):
            return not_est("REQUIRED_RISK_ARTIFACT_UNAVAILABLE")
    if account.operational_status != "CURRENT" or account.eligible_equity <= 0:
        return not_est("ACCOUNT_STATE_NOT_CURRENT")
    if product.key != candidate.key or venue.key != candidate.key or stress.key != candidate.key or fee.key != candidate.key:
        return not_est("PRODUCT_OR_RISK_KEY_MISMATCH")
    universe_entries = [x for x in universe.entries if x.key == candidate.key]
    if len(universe_entries) != 1 or universe_entries[0].product_ref != product.content_hash or venue.product_ref != product.content_hash:
        return not_est("PRODUCT_UNIVERSE_REF_MISMATCH")
    if product.trading_status != TradingStatusV2.TRADING:
        return no_trade("PRODUCT_NOT_TRADING")
    if _indexed(repo, account.exposure_completeness_ref, account.available_at_ns) is None:
        return not_est("EXPOSURE_COMPLETENESS_UNAVAILABLE")
    valid_refs.add(account.exposure_completeness_ref)
    if tuple(sorted(x.content_hash for x in exposures)) != tuple(sorted(account.pending_risk_refs + account.existing_exposure_refs)):
        return not_est("EXPOSURE_REFS_INCOMPLETE")
    for item in exposures:
        if item.available_at_ns > account.available_at_ns or not _matches(
                repo, item.content_hash, cutoff_ns, "PossibleRiskV2", item.to_dict()):
            return not_est("POSSIBLE_EXPOSURE_UNAVAILABLE")
        if _indexed(repo, item.source_ref, item.available_at_ns) is None:
            return not_est("POSSIBLE_EXPOSURE_SOURCE_UNAVAILABLE")
        valid_refs.update((item.content_hash, item.source_ref))
    if any((item.kind in (ExposureKind.PENDING, ExposureKind.UNKNOWN)) !=
           (item.content_hash in account.pending_risk_refs) for item in exposures):
        return not_est("EXPOSURE_CLASS_CONTRADICTION")
    open_items = tuple(x for x in exposures if x.kind in (ExposureKind.OPEN, ExposureKind.PARTIAL))
    pending_items = tuple(x for x in exposures if x.kind in (ExposureKind.PENDING, ExposureKind.UNKNOWN))
    if account.opening_intents < len(pending_items) + sum(x.kind == ExposureKind.PARTIAL for x in exposures):
        return not_est("OPENING_INTENT_COUNT_CONTRADICTION")
    if (sum((x.possible_normal_loss for x in open_items), ZERO) != account.existing_open_normal_loss or
            sum((x.possible_normal_loss for x in pending_items), ZERO) != account.pending_reserved_normal_loss or
            sum((x.possible_notional for x in exposures), ZERO) != account.gross_notional or
            sum((x.signed_beta_notional for x in exposures), ZERO) != account.beta_notional or
            sum((x.possible_venue_collateral for x in exposures), ZERO) != account.venue_collateral or
            sum((x.possible_margin for x in exposures), ZERO) != account.current_margin or
            sum((x.possible_notional for x in exposures if x.key == candidate.key), ZERO) != account.instrument_notional):
        return not_est("ACCOUNT_EXPOSURE_TOTAL_CONTRADICTION")
    if any(x.key == candidate.key for x in exposures):
        return no_trade("NO_PYRAMIDING")
    if any(x.available_at_ns > account.available_at_ns or not _matches(
           repo, x.content_hash, cutoff_ns, "ClosedV2Outcome", x.to_dict())
           for x in outcomes if x.content_hash in account.closed_outcome_refs):
        return not_est("CLOSED_OUTCOME_UNAVAILABLE")
    if tuple(sorted(x.content_hash for x in outcomes if x.available_at_ns <= account.available_at_ns)) != account.closed_outcome_refs:
        return not_est("CLOSED_OUTCOME_REFS_INCOMPLETE")
    loss, used_outcomes = rolling_realized_loss(tuple(x for x in outcomes if x.content_hash in account.closed_outcome_refs), cutoff_ns)
    valid_refs.update(used_outcomes)
    if any(_indexed(repo, ref, cutoff_ns) is None for ref in (venue.source_ref, stress.source_ref, fee.source_ref)):
        return not_est("RISK_SOURCE_UNAVAILABLE")
    valid_refs.update((venue.source_ref, stress.source_ref, fee.source_ref))
    if account.opening_intents >= min(1, v1.max_simultaneous_new_risk_intents,
                                      v2.max_opening_intents_per_account):
        return no_trade("MAX_OPENING_INTENTS")
    if loss > v2.rolling_24h_realized_loss_limit_frac * account.eligible_equity:
        return no_trade("ROLLING_REALIZED_LOSS_STOP")
    scale = v1.scaling_at(account.drawdown)
    if scale <= 0:
        return no_trade("DRAWDOWN_NEW_RISK_STOP")
    if candidate.side == V2Side.LONG and candidate.stop_price >= candidate.entry_reference or (
        candidate.side == V2Side.SHORT and candidate.stop_price <= candidate.entry_reference):
        return no_trade("STOP_WRONG_SIDE_OR_ZERO_DISTANCE")
    if not stress_path_is_coherent((stress.worst_executable_exit_price,), candidate.entry_reference,
            jump=Decimal("0.10"), side=V1Side.LONG if candidate.side == V2Side.LONG else V1Side.SHORT):
        return not_est("STRESS_10_PERCENT_BOUND_UNSUPPORTED")
    fee_entry_rate, fee_exit_rate = fee.entry_taker_rate, fee.exit_taker_rate
    base = product.base_units_per_contract
    normal_unit = (abs(candidate.entry_reference - candidate.stop_price)
                   + candidate.entry_reference * fee_entry_rate + candidate.stop_price * fee_exit_rate) * base
    stress_unit = max(normal_unit, (abs(candidate.entry_reference - stress.worst_executable_exit_price)
                   + candidate.entry_reference * fee_entry_rate
                   + stress.worst_executable_exit_price * fee_exit_rate) * base)
    notional_unit = candidate.entry_collar * base
    beta_unit = notional_unit if candidate.side == V2Side.LONG else -notional_unit
    if normal_unit <= 0 or stress_unit <= 0 or notional_unit <= 0:
        return not_est("INVALID_PER_UNIT_RISK")
    e = account.eligible_equity
    rolling_base = loss + account.existing_open_normal_loss + account.pending_reserved_normal_loss
    existing_stress_bound = max(account.existing_portfolio_es,
        sum((item.possible_stress_loss for item in exposures), ZERO))
    bounds = [v1.normal_loss_per_trade_frac * e * scale / normal_unit,
              (v1.aggregate_open_normal_loss_frac * e * scale - account.existing_open_normal_loss
               - account.pending_reserved_normal_loss) / normal_unit,
              v1.stress_loss_per_trade_frac * e * scale / stress_unit,
              (v2.rolling_24h_new_risk_limit_frac * e - rolling_base) / normal_unit,
              (v1.account_gross_notional_limit * e - account.gross_notional) / notional_unit,
              (v1.instrument_notional_limit * e - account.instrument_notional) / notional_unit,
              (v1.portfolio_es_limit_frac * e * scale - existing_stress_bound) / stress_unit]
    beta_limit = v1.correlated_crypto_beta_limit * e
    bounds.append((beta_limit - account.beta_notional) / beta_unit if beta_unit > 0
                  else (beta_limit + account.beta_notional) / -beta_unit)
    if product.max_qty is not None:
        bounds.append(product.max_qty)
    upper = max(ZERO, min(bounds))
    margin_headroom = min(account.margin_available - v1.min_free_margin_reserve_frac * e - account.current_margin,
                          v1.venue_collateral_limit * e - account.venue_collateral)
    best_qty, best_leverage, best_margin = ZERO, None, None
    for leverage in venue.allowed_leverages:
        if leverage > min(v1.max_contract_leverage, venue.account_leverage_limit):
            continue
        margin_unit = notional_unit * (ONE / leverage + venue.margin_addon_frac)
        feasible_qty = _floor_step(min(upper, max(ZERO, margin_headroom) / margin_unit), product.qty_step)
        if feasible_qty > best_qty:
            best_qty, best_leverage, best_margin = feasible_qty, leverage, feasible_qty * margin_unit
    if (best_qty <= 0 or best_qty < product.min_qty or best_leverage is None or best_margin is None or
            (product.min_notional is not None and best_qty * notional_unit < product.min_notional)):
        return no_trade("MIN_SIZE_OR_HARD_RISK")
    proposed_normal, proposed_stress, proposed_notional = best_qty * normal_unit, best_qty * stress_unit, best_qty * notional_unit
    return persist(SizingStatus.SIZED, (), q=best_qty, normal=proposed_normal,
                   stressed=proposed_stress, notional=proposed_notional, margin=best_margin,
                   leverage=best_leverage, loss=loss, rolling=rolling_base + proposed_normal)
