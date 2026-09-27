"""Additive Phase-4 qualification and capital bridge contracts for V2.

This module has no venue transport. It binds an already admitted V2 action to
the existing V1 durable control shell and rejects qualification without
cutoff-current, reconciled evidence.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any

from atlas.domain.enums import Side
from atlas.domain.money import canonical_decimal_str
from atlas.domain.risk import RiskPolicy
from atlas.domain.trade_plan import TradePlan
from atlas.runtime.v2_capital_authority import (
    V2CapitalAuthorityAttestation,
    V2CapitalAuthorityStatus,
    V2LiveAuthorityEvidenceKind,
)
from atlas.v2._serialization import canonical_json, decimal_value, sha256_json, sha256_ref, strict_fields
from atlas.v2.contracts import (
    CandidateActionV2,
    CandidateSelectionStatus,
    CandidateSetV2,
    DecisionStatusV2,
    TradePlanEnvelopeV2,
)
from atlas.v2.instruments import EnvironmentV2, VenueV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.risk import (
    DAY_NS,
    AccountRiskSnapshotV2,
    ClosedV2Outcome,
    ExposureKind,
    OutcomeClass,
    PossibleRiskV2,
    RiskPolicyV2,
    SizingDecisionV2,
    SizingStatus,
)
from atlas.v2.science.action import ACTION_VERSION, ActionArtifactV2
from atlas.v2.science.admission import (
    AmendedEvaluationArtifactV2,
    VenueCapabilitySnapshotV2,
    VenueCapabilityStatusV2,
)
from atlas.v2.science.m0 import M0OODV2

BRIDGE_VERSION_V2 = "V2_CAPITAL_BRIDGE_ENVELOPE_V1"
CAPABILITY_PROFILE_VERSION_V2 = "VENUE_CAPABILITY_PROFILE_V2_V1"
LIVE_RISK_EVIDENCE_VERSION_V2 = "V2_LIVE_RISK_EVIDENCE_V1"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_MAX_EVIDENCE_AGE_NS = 1_000_000_000


class QualificationStatusV2(StrEnum):
    IMPLEMENTED = "IMPLEMENTED"
    TESTED = "TESTED"
    UNVERIFIED = "UNVERIFIED"
    TEST_GATE = "TEST GATE"
    NOT_ESTIMABLE = "NOT ESTIMABLE"
    BLOCKED_BY_ENVIRONMENT = "BLOCKED BY ENVIRONMENT"


BYBIT_REQUIRED_CAPABILITIES_V2 = (
    "account_environment_identity",
    "usdt_linear_perpetual_product",
    "one_way_position_mode",
    "isolated_margin_mode",
    "instrument_filter_revision",
    "tick_qty_min_notional_rules",
    "leverage_and_risk_tier",
    "fee_schedule_revision",
    "client_order_identity_mapping",
    "ioc_and_partial_fill_semantics",
    "reduce_only_enforcement",
    "full_position_stop_visibility",
    "stop_quantity_after_partial_fill",
    "mark_price_stop_trigger",
    "external_stop_fill_reconciliation",
    "order_history_and_query_retention",
    "private_stream_reconnect_and_execution_ids",
    "funding_and_cash_evidence",
    "unknown_submit_recovery",
    "emergency_flatten",
)

BINANCE_REQUIRED_CAPABILITIES_V2 = (
    "account_environment_identity",
    "usd_m_perpetual_product",
    "one_way_position_mode",
    "isolated_margin_mode",
    "symbol_filter_revision",
    "tick_step_min_qty_notional_rules",
    "leverage_and_risk_limits",
    "fee_and_funding_semantics",
    "client_order_identity_mapping",
    "partial_fill_semantics",
    "reduce_only_closing_behavior",
    "venue_native_entry_protection",
    "protection_visibility_and_partial_fill_quantity",
    "history_and_reconciliation_queries",
    "private_user_stream_and_reconnect",
    "unknown_submit_recovery",
    "emergency_flatten",
)


@dataclass(frozen=True)
class VenueCapabilityRowV2:
    capability: str
    engineering_status: QualificationStatusV2
    venue_status: QualificationStatusV2
    evidence_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.capability.strip():
            raise ValueError("capability name required")
        object.__setattr__(self, "engineering_status", QualificationStatusV2(self.engineering_status))
        object.__setattr__(self, "venue_status", QualificationStatusV2(self.venue_status))
        if self.evidence_refs != tuple(sorted(set(self.evidence_refs))):
            raise ValueError("capability evidence refs must be sorted and unique")
        for ref in self.evidence_refs:
            sha256_ref(ref, field="capability evidence ref")

    def to_dict(self) -> dict[str, Any]:
        return {
            "capability": self.capability,
            "engineering_status": self.engineering_status.value,
            "venue_status": self.venue_status.value,
            "evidence_refs": list(self.evidence_refs),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> VenueCapabilityRowV2:
        d = strict_fields(
            data,
            expected={"capability", "engineering_status", "venue_status", "evidence_refs"},
            required={"capability", "engineering_status", "venue_status", "evidence_refs"},
            name="VenueCapabilityRowV2",
        )
        if not isinstance(d["evidence_refs"], list):
            raise ValueError("capability evidence refs must be an array")
        return cls(
            d["capability"],
            QualificationStatusV2(d["engineering_status"]),
            QualificationStatusV2(d["venue_status"]),
            tuple(d["evidence_refs"]),
        )


@dataclass(frozen=True)
class VenueCapabilityProfileV2:
    venue: VenueV2
    environment: EnvironmentV2
    account_identity_hash: str
    product_ref: str
    instrument_key_ref: str
    position_mode: str
    margin_mode: str
    nautilus_distribution: str
    nautilus_version: str
    nautilus_source_commit: str
    nautilus_artifact_ref: str
    dependency_lock_hash: str
    protection_profile_ref: str
    fee_revision_ref: str
    filter_revision_ref: str
    capability_snapshot_ref: str
    rows: tuple[VenueCapabilityRowV2, ...]
    qualified_at_ns: int
    expires_at_ns: int
    synthetic_fixture: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "venue", VenueV2(self.venue))
        object.__setattr__(self, "environment", EnvironmentV2(self.environment))
        if not _SHA256.fullmatch(self.account_identity_hash):
            raise ValueError("account identity must be a redacted SHA-256 hash")
        for name in (
            "product_ref",
            "instrument_key_ref",
            "nautilus_artifact_ref",
            "dependency_lock_hash",
            "protection_profile_ref",
            "fee_revision_ref",
            "filter_revision_ref",
            "capability_snapshot_ref",
        ):
            sha256_ref(getattr(self, name), field=name)
        for name in (
            "position_mode",
            "margin_mode",
            "nautilus_distribution",
            "nautilus_version",
            "nautilus_source_commit",
        ):
            if not getattr(self, name).strip():
                raise ValueError(f"{name} is required")
        required = required_capabilities_v2(self.venue)
        names = tuple(row.capability for row in self.rows)
        if names != required:
            raise ValueError("capability rows must exactly match the venue's required matrix")
        if (
            type(self.qualified_at_ns) is not int
            or type(self.expires_at_ns) is not int
            or self.qualified_at_ns < 0
            or self.expires_at_ns <= self.qualified_at_ns
            or type(self.synthetic_fixture) is not bool
        ):
            raise ValueError("capability profile chronology/fixture flag invalid")

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": CAPABILITY_PROFILE_VERSION_V2,
            "venue": self.venue.value,
            "environment": self.environment.value,
            "account_identity_hash": self.account_identity_hash,
            "product_ref": self.product_ref,
            "instrument_key_ref": self.instrument_key_ref,
            "position_mode": self.position_mode,
            "margin_mode": self.margin_mode,
            "nautilus_distribution": self.nautilus_distribution,
            "nautilus_version": self.nautilus_version,
            "nautilus_source_commit": self.nautilus_source_commit,
            "nautilus_artifact_ref": self.nautilus_artifact_ref,
            "dependency_lock_hash": self.dependency_lock_hash,
            "protection_profile_ref": self.protection_profile_ref,
            "fee_revision_ref": self.fee_revision_ref,
            "filter_revision_ref": self.filter_revision_ref,
            "capability_snapshot_ref": self.capability_snapshot_ref,
            "rows": [row.to_dict() for row in self.rows],
            "qualified_at_ns": self.qualified_at_ns,
            "expires_at_ns": self.expires_at_ns,
            "synthetic_fixture": self.synthetic_fixture,
        }

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @property
    def capital_capable(self) -> bool:
        return (
            not self.synthetic_fixture
            and self.environment == EnvironmentV2.TESTNET
            and all(
                row.engineering_status == QualificationStatusV2.TESTED
                and row.venue_status == QualificationStatusV2.TESTED
                and bool(row.evidence_refs)
                for row in self.rows
            )
        )

    def qualification_status(self) -> QualificationStatusV2:
        if self.capital_capable:
            return QualificationStatusV2.TESTED
        statuses = {row.venue_status for row in self.rows}
        if QualificationStatusV2.BLOCKED_BY_ENVIRONMENT in statuses:
            return QualificationStatusV2.BLOCKED_BY_ENVIRONMENT
        if QualificationStatusV2.TEST_GATE in statuses:
            return QualificationStatusV2.TEST_GATE
        return QualificationStatusV2.UNVERIFIED

    def is_current(self, now_ns: int) -> bool:
        return self.qualified_at_ns <= now_ns < self.expires_at_ns

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> VenueCapabilityProfileV2:
        fields = set(cls.__dataclass_fields__) | {"version"}
        d = dict(strict_fields(data, expected=fields, required=fields, name="VenueCapabilityProfileV2"))
        if d.pop("version") != CAPABILITY_PROFILE_VERSION_V2 or not isinstance(d["rows"], list):
            raise ValueError("unsupported venue capability profile wire")
        d["rows"] = tuple(VenueCapabilityRowV2.from_dict(item) for item in d["rows"])
        d["venue"] = VenueV2(d["venue"])
        d["environment"] = EnvironmentV2(d["environment"])
        return cls(**{name: d[name] for name in cls.__dataclass_fields__})


def required_capabilities_v2(venue: VenueV2) -> tuple[str, ...]:
    venue = VenueV2(venue)
    return BYBIT_REQUIRED_CAPABILITIES_V2 if venue == VenueV2.BYBIT else BINANCE_REQUIRED_CAPABILITIES_V2


def index_venue_capability_profile_v2(repo: OpsRepository, profile: VenueCapabilityProfileV2) -> str:
    repo.register_artifact(
        ArtifactIndexEntryV2(
            profile.content_hash,
            "VenueCapabilityProfileV2",
            profile.content_hash,
            profile.qualified_at_ns,
            profile.qualified_at_ns,
            {"profile": profile.to_dict()},
        )
    )
    return profile.content_hash


def _validate_actual_profile_evidence(repo: OpsRepository, profile: VenueCapabilityProfileV2, *, now_ns: int) -> None:
    if not profile.capital_capable or not profile.is_current(now_ns):
        raise ValueError("venue profile is not current and capital capable")
    for row in profile.rows:
        for ref in row.evidence_refs:
            item = repo.get_artifact(ref)
            metadata = item.metadata if item is not None else {}
            if (
                item is None
                or item.artifact_type != "AuthenticatedVenueQualificationEvidenceV2"
                or item.content_hash != ref
                or sha256_json(metadata) != ref
                or metadata.get("qualification_class") != "AUTHENTICATED_TESTNET"
                or metadata.get("venue") != profile.venue.value
                or metadata.get("environment") != profile.environment.value
                or metadata.get("capability") != row.capability
                or metadata.get("account_identity_hash") != profile.account_identity_hash
                or metadata.get("product_ref") != profile.product_ref
                or metadata.get("instrument_key_ref") != profile.instrument_key_ref
                or metadata.get("position_mode") != profile.position_mode
                or metadata.get("margin_mode") != profile.margin_mode
                or metadata.get("nautilus_artifact_ref") != profile.nautilus_artifact_ref
                or metadata.get("dependency_lock_hash") != profile.dependency_lock_hash
                or metadata.get("protection_profile_ref") != profile.protection_profile_ref
                or metadata.get("fee_revision_ref") != profile.fee_revision_ref
                or metadata.get("filter_revision_ref") != profile.filter_revision_ref
                or metadata.get("sensitive_fields_excluded") is not True
                or item.available_at_ns > now_ns
            ):
                raise ValueError("qualification row lacks exact redacted authenticated testnet evidence")


def validate_venue_profile_for_capital(repo: OpsRepository, profile: VenueCapabilityProfileV2, *, now_ns: int) -> None:
    if profile.synthetic_fixture or not profile.capital_capable:
        raise ValueError("venue capability profile is not actually qualified for capital")
    _validate_actual_profile_evidence(repo, profile, now_ns=now_ns)


@dataclass(frozen=True)
class V2CapitalBridgeEnvelope:
    candidate_set_ref: str
    selected_candidate_id: str
    candidate_ref: str
    action_artifact_ref: str
    frozen_action_hash: str
    sizing_ref: str
    sizing_hash: str
    v1_risk_policy_hash: str
    risk_policy_v2_hash: str
    evaluation_ref: str
    venue_capability_profile_ref: str
    venue_capability_snapshot_ref: str
    product_ref: str
    product_revision: str
    cost_model_ref: str
    account_identity_hash: str
    venue: VenueV2
    environment: EnvironmentV2
    instrument_key_ref: str
    side: str
    quantity: Decimal
    collar: Decimal
    absolute_stop: Decimal
    trigger_basis: str
    horizon_end_ns: int
    expires_at_ns: int
    normal_risk: Decimal
    stress_risk: Decimal
    margin: Decimal
    leverage: Decimal
    v2_trade_plan_ref: str
    v2_trade_plan_hash: str
    action_version: str
    version: str = BRIDGE_VERSION_V2

    def __post_init__(self) -> None:
        if self.version != BRIDGE_VERSION_V2:
            raise ValueError("unsupported V2 capital bridge version")
        for name in (
            "candidate_set_ref",
            "candidate_ref",
            "action_artifact_ref",
            "frozen_action_hash",
            "sizing_ref",
            "sizing_hash",
            "v1_risk_policy_hash",
            "risk_policy_v2_hash",
            "evaluation_ref",
            "venue_capability_profile_ref",
            "venue_capability_snapshot_ref",
            "product_ref",
            "cost_model_ref",
            "instrument_key_ref",
            "v2_trade_plan_ref",
            "v2_trade_plan_hash",
        ):
            sha256_ref(getattr(self, name), field=name)
        if not _SHA256.fullmatch(self.account_identity_hash):
            raise ValueError("account identity must be a redacted SHA-256 hash")
        if not self.selected_candidate_id.strip():
            raise ValueError("selected candidate identity required")
        object.__setattr__(self, "venue", VenueV2(self.venue))
        object.__setattr__(self, "environment", EnvironmentV2(self.environment))
        if self.side not in ("LONG", "SHORT") or not self.product_revision.strip() or not self.action_version.strip():
            raise ValueError("bridge side/product/action version invalid")
        if not self.trigger_basis.strip():
            raise ValueError("absolute stop trigger basis required")
        for name in ("quantity", "collar", "absolute_stop", "normal_risk", "stress_risk", "margin", "leverage"):
            value = getattr(self, name)
            if not isinstance(value, Decimal) or not value.is_finite():
                raise ValueError(f"{name} must be finite Decimal")
            if name in ("quantity", "collar", "absolute_stop", "leverage") and value <= 0:
                raise ValueError(f"{name} must be positive")
            if name in ("normal_risk", "stress_risk", "margin") and value < 0:
                raise ValueError(f"{name} must be nonnegative")
        if (
            self.quantity <= 0
            or type(self.horizon_end_ns) is not int
            or type(self.expires_at_ns) is not int
            or self.expires_at_ns <= 0
            or self.horizon_end_ns < self.expires_at_ns
        ):
            raise ValueError("bridge quantity/expiry/horizon invalid")

    def to_dict(self) -> dict[str, Any]:
        values = {name: getattr(self, name) for name in self.__dataclass_fields__}
        values["venue"] = self.venue.value
        values["environment"] = self.environment.value
        for name in ("quantity", "collar", "absolute_stop", "normal_risk", "stress_risk", "margin", "leverage"):
            values[name] = canonical_decimal_str(values[name])
        return {"artifact_type": "V2CapitalBridgeEnvelope", **values}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> V2CapitalBridgeEnvelope:
        fields = set(cls.__dataclass_fields__) | {"artifact_type"}
        d = dict(strict_fields(data, expected=fields, required=fields, name="V2CapitalBridgeEnvelope"))
        if d.pop("artifact_type") != "V2CapitalBridgeEnvelope":
            raise ValueError("unsupported V2 capital bridge wire")
        for name in ("quantity", "collar", "absolute_stop", "normal_risk", "stress_risk", "margin", "leverage"):
            d[name] = decimal_value(d[name], field=name, wire=True)
        d["venue"] = VenueV2(d["venue"])
        d["environment"] = EnvironmentV2(d["environment"])
        return cls(**d)


def _require_indexed(
    repo: OpsRepository, ref: str, kind: str, *, metadata_key: str | None = None, expected: dict[str, Any] | None = None
) -> dict[str, Any]:
    item = repo.get_artifact(ref)
    if item is None or item.content_hash != ref or item.artifact_type != kind:
        raise ValueError(f"durable {kind} artifact required")
    body = item.metadata.get(metadata_key) if metadata_key else dict(item.metadata)
    if not isinstance(body, Mapping) or (expected is not None and canonical_json(body) != canonical_json(expected)):
        raise ValueError(f"durable {kind} body/hash mismatch")
    return dict(body)


def validate_v2_bridge_material(
    *,
    candidate_set: CandidateSetV2,
    candidate: CandidateActionV2,
    action: ActionArtifactV2,
    sizing: SizingDecisionV2,
    evaluation: AmendedEvaluationArtifactV2,
    capability_snapshot: VenueCapabilitySnapshotV2,
    capability_profile: VenueCapabilityProfileV2,
    plan: TradePlanEnvelopeV2,
    risk_policy_v1: RiskPolicy,
    risk_policy_v2: RiskPolicyV2,
    now_ns: int,
) -> None:
    """Validate exact V2 source binding without granting venue qualification."""
    if evaluation.decision != DecisionStatusV2.CANDIDATE:
        raise ValueError("capital bridge requires an economic CANDIDATE evaluation")
    if now_ns >= evaluation.action_expiry_ns or now_ns >= plan.expires_at_ns:
        raise ValueError("capital bridge action/evaluation expired")
    if now_ns >= candidate.deadline_ns:
        raise ValueError("selected candidate deadline expired")
    if not capability_profile.is_current(now_ns):
        raise ValueError("venue capability profile expired or is not yet effective")
    key = action.action.key
    if (
        candidate_set.selection_status != CandidateSelectionStatus.SELECTED
        or candidate_set.selected_candidate_id != candidate.candidate_id
        or action.candidate_set_ref != candidate_set.content_hash
        or action.candidate_ref != candidate.content_hash
        or candidate.account_scope != capability_profile.account_identity_hash
        or candidate.key != key
        or candidate.side.value != action.action.side
        or candidate.policy_hash != action.action.policy_hash
        or candidate.quantity is not None
        or candidate.horizon_end_ns != action.action.horizon_end_ns
        or candidate.entry_reference != action.action.entry_reference
        or candidate.entry_collar != action.action.entry_collar
        or candidate.stop_price != action.action.stop_price
        or not any(
            row.candidate_id == candidate.candidate_id
            and row.key == key
            and row.side == candidate.side
            and row.policy_id == action.action.policy_id
            for row in candidate_set.candidates
        )
    ):
        raise ValueError("CandidateSet/selected candidate/frozen action binding mismatch")
    if (
        sizing.content_hash != action.sizing_ref
        or sizing.status != SizingStatus.SIZED
        or sizing.quantity != action.action.quantity
        or sizing.candidate_ref != candidate.content_hash
        or sizing.candidate_set_ref != candidate_set.content_hash
        or sizing.selected_candidate_id != candidate.candidate_id
        or sizing.product_ref != action.action.product_ref
        or sizing.account_snapshot_ref != evaluation.account_snapshot_ref
        or sizing.risk_policy_hash != risk_policy_v1.policy_hash()
        or sizing.risk_policy_v2_hash != risk_policy_v2.policy_hash
    ):
        raise ValueError("hard-risk sizing or policy identity mismatch")
    if (
        risk_policy_v2.base_v1_risk_policy_hash != risk_policy_v1.policy_hash()
        or action.action.risk_policy_hash != risk_policy_v1.policy_hash()
        or action.action.risk_policy_v2_hash != risk_policy_v2.policy_hash
        or evaluation.risk_policy_ref != risk_policy_v1.policy_hash()
        or evaluation.risk_policy_hash != risk_policy_v1.policy_hash()
        or evaluation.risk_policy_v2_ref != risk_policy_v2.policy_hash
        or evaluation.risk_policy_v2_hash != risk_policy_v2.policy_hash
    ):
        raise ValueError("current V1/V2 RiskPolicy differs from frozen evaluation")
    if (
        evaluation.action_hash != action.action.action_hash
        or evaluation.action_artifact_ref != action.content_hash
        or evaluation.candidate_ref != candidate.content_hash
        or evaluation.candidate_set_ref != candidate_set.content_hash
        or evaluation.quantity != action.action.quantity
        or evaluation.policy_hash != action.action.policy_hash
    ):
        raise ValueError("economic evaluation does not bind the exact frozen action")
    if (
        capability_snapshot.content_hash != capability_profile.capability_snapshot_ref
        or capability_snapshot.observed_status != VenueCapabilityStatusV2.SUPPORTED
        or capability_snapshot.synthetic_fixture
        or capability_snapshot.account_scope != capability_profile.account_identity_hash
        or capability_snapshot.venue != capability_profile.venue
        or capability_snapshot.environment != capability_profile.environment
        or capability_snapshot.product_ref != capability_profile.product_ref
        or capability_snapshot.instrument_key_ref != capability_profile.instrument_key_ref
        or capability_snapshot.position_mode.casefold().replace("-", "_")
        != capability_profile.position_mode.casefold().replace("-", "_")
        or capability_snapshot.margin_mode.casefold() != capability_profile.margin_mode.casefold()
        or capability_snapshot.nautilus_distribution != capability_profile.nautilus_distribution
        or capability_snapshot.nautilus_version != capability_profile.nautilus_version
        or capability_snapshot.nautilus_source_commit != capability_profile.nautilus_source_commit
        or capability_snapshot.nautilus_artifact_ref != capability_profile.nautilus_artifact_ref
        or capability_snapshot.protection_profile_ref != capability_profile.protection_profile_ref
        or capability_snapshot.available_at_ns > now_ns
        or evaluation.capability_evidence_ref != capability_snapshot.content_hash
    ):
        raise ValueError("venue capability snapshot is not the exact qualified account profile")
    if (
        capability_profile.venue != key.venue
        or capability_profile.environment != key.environment
        or capability_profile.instrument_key_ref != key.content_hash
        or capability_profile.product_ref != action.action.product_ref
        or capability_profile.position_mode.casefold().replace("-", "_") != "one_way"
        or capability_profile.margin_mode.casefold() != "isolated"
    ):
        raise ValueError("venue profile does not cover the exact instrument/account/modes")
    if key.venue == VenueV2.BYBIT and key.native_symbol not in {"BTCUSDT", "ETHUSDT"}:
        raise ValueError("Bybit V1 baseline bridge is limited to BTCUSDT and ETHUSDT")
    if (
        plan.action_hash != action.action.action_hash
        or plan.evaluation_ref != evaluation.content_hash
        or plan.policy_hash != action.action.policy_hash
        or plan.risk_policy_hash != risk_policy_v1.policy_hash()
        or plan.capability_manifest_hash != capability_snapshot.content_hash
        or plan.product_ref != action.action.product_ref
        or plan.key != key
        or plan.account_scope != capability_profile.account_identity_hash
        or plan.side.value != action.action.side
        or plan.qty_limit != action.action.quantity
        or plan.collar != action.action.entry_collar
        or plan.stop != action.action.stop_price
        or plan.stop_trigger_basis != action.action.stop_trigger_basis
        or plan.horizon_end_ns != action.action.horizon_end_ns
        or plan.expires_at_ns > evaluation.action_expiry_ns
        or plan.reference_price != action.action.entry_reference
        or plan.entry_policy
        != canonical_json(
            {"entry_rule": action.action.entry_rule.to_dict(), "trigger_basis": action.action.entry_trigger_basis}
        )
        or plan.management_policy != canonical_json(action.action.management_rule.to_dict())
        or plan.normal_risk != sizing.normal_risk
        or plan.stress_risk != sizing.stress_risk
        or plan.margin != sizing.margin
        or plan.leverage_bound != sizing.leverage
    ):
        raise ValueError("TradePlanEnvelopeV2 does not exactly reproduce admitted action and sizing")
    if not _SHA256.fullmatch(candidate.cost_model_ref):
        raise ValueError("cost model must be identified by an immutable SHA-256 ref")


def build_v2_capital_bridge(
    repo: OpsRepository,
    *,
    candidate_set: CandidateSetV2,
    candidate: CandidateActionV2,
    action: ActionArtifactV2,
    sizing: SizingDecisionV2,
    evaluation: AmendedEvaluationArtifactV2,
    capability_snapshot: VenueCapabilitySnapshotV2,
    capability_profile: VenueCapabilityProfileV2,
    plan: TradePlanEnvelopeV2,
    risk_policy_v1: RiskPolicy,
    risk_policy_v2: RiskPolicyV2,
    now_ns: int,
) -> V2CapitalBridgeEnvelope:
    """Bind admitted artifacts verbatim; this function does not rerun strategy science."""
    validate_v2_bridge_material(
        candidate_set=candidate_set,
        candidate=candidate,
        action=action,
        sizing=sizing,
        evaluation=evaluation,
        capability_snapshot=capability_snapshot,
        capability_profile=capability_profile,
        plan=plan,
        risk_policy_v1=risk_policy_v1,
        risk_policy_v2=risk_policy_v2,
        now_ns=now_ns,
    )
    validate_venue_profile_for_capital(repo, capability_profile, now_ns=now_ns)
    key = action.action.key

    _require_indexed(
        repo,
        candidate_set.content_hash,
        "CandidateSetV2",
        metadata_key="candidate_set",
        expected=candidate_set.to_dict(),
    )
    _require_indexed(
        repo, candidate.content_hash, "CandidateActionV2", metadata_key="candidate", expected=candidate.to_dict()
    )
    _require_indexed(repo, sizing.content_hash, "SizingDecisionV2", metadata_key="sizing", expected=sizing.to_dict())
    action_body = _require_indexed(
        repo, action.content_hash, "ActionArtifactV2", metadata_key="action_artifact", expected=action.to_dict()
    )
    if action_body.get("action_hash") != action.action.action_hash:
        raise ValueError("durable action artifact identity mismatch")
    _require_indexed(
        repo, evaluation.content_hash, "EvaluationArtifactV2", metadata_key="evaluation", expected=evaluation.to_dict()
    )
    _require_indexed(
        repo,
        capability_snapshot.content_hash,
        "VenueCapabilitySnapshotV2",
        metadata_key="capability",
        expected=capability_snapshot.to_dict(),
    )
    _require_indexed(
        repo,
        capability_profile.content_hash,
        "VenueCapabilityProfileV2",
        metadata_key="profile",
        expected=capability_profile.to_dict(),
    )
    _require_indexed(repo, plan.content_hash, "TradePlanEnvelopeV2", metadata_key="plan", expected=plan.to_dict())
    ood_body = _require_indexed(repo, evaluation.ood_ref, "M0OODV2", metadata_key="ood")
    ood_fields = {
        "version",
        "action_hash",
        "feature_vector_ref",
        "training_row_refs",
        "robust_z_limit",
        "maximum_absolute_robust_z",
        "out_of_distribution",
        "status",
    }
    ood_body = dict(strict_fields(ood_body, expected=ood_fields, required=ood_fields, name="M0OODV2"))
    if ood_body["version"] != "M0_ROBUST_OOD_V1" or not isinstance(ood_body["training_row_refs"], list):
        raise ValueError("unsupported OOD evidence wire")
    ood = M0OODV2(
        ood_body["action_hash"],
        ood_body["feature_vector_ref"],
        tuple(ood_body["training_row_refs"]),
        decimal_value(ood_body["robust_z_limit"], field="robust_z_limit", wire=True),
        decimal_value(ood_body["maximum_absolute_robust_z"], field="maximum_absolute_robust_z", wire=True)
        if ood_body["maximum_absolute_robust_z"] is not None
        else None,
        ood_body["out_of_distribution"],
        ood_body["status"],
    )
    if (
        ood.action_hash != action.action.action_hash
        or ood.out_of_distribution is not False
        or ood.status != "IN_DISTRIBUTION"
    ):
        raise ValueError("out-of-distribution or rejected action cannot bridge to capital")

    bridge = V2CapitalBridgeEnvelope(
        candidate_set.content_hash,
        candidate.candidate_id,
        candidate.content_hash,
        action.content_hash,
        action.action.action_hash,
        sizing.content_hash,
        sizing.content_hash,
        risk_policy_v1.policy_hash(),
        risk_policy_v2.policy_hash,
        evaluation.content_hash,
        capability_profile.content_hash,
        capability_snapshot.content_hash,
        action.action.product_ref,
        key.contract_revision,
        candidate.cost_model_ref,
        capability_profile.account_identity_hash,
        key.venue,
        key.environment,
        key.content_hash,
        action.action.side,
        action.action.quantity,
        action.action.entry_collar,
        action.action.stop_price,
        action.action.stop_trigger_basis,
        action.action.horizon_end_ns,
        plan.expires_at_ns,
        plan.normal_risk,
        plan.stress_risk,
        plan.margin,
        plan.leverage_bound,
        plan.plan_id,
        plan.content_hash,
        f"{ACTION_VERSION}:{action.action.policy_id}:{action.action.policy_version}",
    )
    repo.register_artifact(
        ArtifactIndexEntryV2(
            bridge.content_hash,
            "V2CapitalBridgeEnvelope",
            bridge.content_hash,
            plan.envelope.available_at_ns,
            plan.envelope.available_at_ns,
            {"bridge": bridge.to_dict()},
        )
    )
    return bridge


def bridge_to_v1_trade_plan(bridge: V2CapitalBridgeEnvelope, plan: TradePlanEnvelopeV2) -> TradePlan:
    """Map exact bridged values into the existing approval-bound V1 plan type."""
    if plan.content_hash != bridge.v2_trade_plan_hash or plan.plan_id != bridge.v2_trade_plan_ref:
        raise ValueError("bridge references a different V2 TradePlanEnvelope")
    plan_id = sha256_json({"version": BRIDGE_VERSION_V2, "bridge_hash": bridge.content_hash})
    created = plan.envelope.available_at_ns
    return TradePlan(
        plan_id=plan_id,
        version=f"V2BRIDGE:{bridge.content_hash}",
        policy_hash=plan.policy_hash,
        snapshot_hash=bridge.content_hash,
        expires_at_ns=plan.expires_at_ns,
        market=bridge.venue.value,
        account_scope=bridge.account_identity_hash,
        instrument=plan.key.native_symbol,
        side=Side(plan.side),
        qty_limit=bridge.quantity,
        entry_policy=plan.entry_policy,
        collar=bridge.collar,
        stop=bridge.absolute_stop,
        stop_trigger_basis=bridge.trigger_basis,
        management_policy=plan.management_policy,
        horizon_end_ns=bridge.horizon_end_ns,
        cost_distribution_ref=bridge.cost_model_ref,
        normal_risk=bridge.normal_risk,
        stress_risk=bridge.stress_risk,
        margin=bridge.margin,
        leverage_bound=bridge.leverage,
        risk_config_hash=bridge.v1_risk_policy_hash,
        created_at_ns=created,
        available_at_ns=created,
        reference_price=plan.reference_price,
    )


@dataclass(frozen=True)
class V2LiveRiskEvidence:
    account_snapshot: AccountRiskSnapshotV2
    closed_outcomes: tuple[ClosedV2Outcome, ...]
    possible_risks: tuple[PossibleRiskV2, ...]
    account_observation_ref: str
    reconciliation_ref: str
    source_class: str
    synthetic_fixture: bool = False

    def __post_init__(self) -> None:
        for name in ("account_observation_ref", "reconciliation_ref"):
            sha256_ref(getattr(self, name), field=name)
        if self.source_class != "AUTHENTICATED_VENUE_RECONCILIATION" or self.synthetic_fixture:
            raise ValueError("live risk evidence must come from authenticated venue reconciliation")
        if tuple(sorted(x.content_hash for x in self.closed_outcomes)) != self.account_snapshot.closed_outcome_refs:
            raise ValueError("closed outcomes must exactly cover the account snapshot refs")
        risk_refs = tuple(sorted(x.content_hash for x in self.possible_risks))
        expected_refs = tuple(
            sorted(self.account_snapshot.pending_risk_refs + self.account_snapshot.existing_exposure_refs)
        )
        if risk_refs != expected_refs:
            raise ValueError("possible-risk evidence must exactly cover open and reserved exposure refs")


@dataclass(frozen=True)
class V2LiveRiskDecision:
    allowed: bool
    reasons: tuple[str, ...]
    realized_loss_consumed_24h: Decimal
    existing_open_normal_loss: Decimal
    pending_reserved_normal_loss: Decimal
    proposed_normal_loss: Decimal
    opening_intents: int
    evidence_hash: str


def _verify_indexed_body(repo: OpsRepository, ref: str, kind: str, body: dict[str, Any]) -> None:
    item = repo.get_artifact(ref)
    if (
        item is None
        or item.artifact_type != kind
        or item.content_hash != ref
        or canonical_json(item.metadata) != canonical_json(body)
        or sha256_json(body) != ref
    ):
        raise ValueError(f"authenticated {kind} source evidence missing or mismatched")


def revalidate_v2_rolling_risk(
    repo: OpsRepository,
    *,
    evidence: V2LiveRiskEvidence,
    risk_policy_v1: RiskPolicy,
    risk_policy_v2: RiskPolicyV2,
    account_scope: str,
    cutoff_ns: int,
    proposed_normal_loss: Decimal,
    v1_reserved_normal_loss: Decimal = Decimal(0),
    v1_unresolved_opening_intents: int = 0,
) -> V2LiveRiskDecision:
    """Require complete ACTUAL risk evidence; profits never replenish loss allowance."""
    snapshot = evidence.account_snapshot
    if risk_policy_v2.base_v1_risk_policy_hash != risk_policy_v1.policy_hash():
        raise ValueError("V1/V2 risk policy binding mismatch")
    if account_scope != snapshot.account_scope or not _SHA256.fullmatch(account_scope):
        raise ValueError("current hashed account identity mismatch")
    if type(cutoff_ns) is not int or cutoff_ns < 0 or snapshot.available_at_ns > cutoff_ns:
        raise ValueError("risk snapshot is future or cutoff invalid")
    if cutoff_ns - snapshot.available_at_ns > _MAX_EVIDENCE_AGE_NS:
        raise ValueError("cutoff-current actual risk evidence required")
    if snapshot.operational_status != "CURRENT" or snapshot.eligible_equity <= 0:
        raise ValueError("actual account evidence is stale, incomplete, or has no eligible equity")
    if risk_policy_v1.policy_effective_at_ns > cutoff_ns or risk_policy_v2.effective_at_ns > cutoff_ns:
        raise ValueError("current risk policy is not yet effective")
    if (
        not isinstance(proposed_normal_loss, Decimal)
        or not proposed_normal_loss.is_finite()
        or proposed_normal_loss < 0
    ):
        raise ValueError("proposed normal loss must be a nonnegative Decimal")
    if (
        not isinstance(v1_reserved_normal_loss, Decimal)
        or not v1_reserved_normal_loss.is_finite()
        or v1_reserved_normal_loss < 0
        or type(v1_unresolved_opening_intents) is not int
        or v1_unresolved_opening_intents < 0
    ):
        raise ValueError("current V1 reservation/intent evidence invalid")

    observation = repo.get_artifact(evidence.account_observation_ref)
    reconciliation = repo.get_artifact(evidence.reconciliation_ref)
    if (
        observation is None
        or observation.artifact_type != "AuthenticatedVenueRiskObservationV2"
        or observation.content_hash != evidence.account_observation_ref
        or observation.metadata.get("account_snapshot_ref") != sha256_json(snapshot.to_dict())
        or observation.metadata.get("account_scope") != account_scope
        or observation.metadata.get("source_class") != evidence.source_class
        or observation.metadata.get("observed_at_ns") != snapshot.available_at_ns
        or observation.metadata.get("sensitive_fields_excluded") is not True
        or sha256_json(observation.metadata) != evidence.account_observation_ref
        or observation.available_at_ns > cutoff_ns
    ):
        raise ValueError("authenticated current venue account observation missing")
    if (
        reconciliation is None
        or reconciliation.artifact_type != "AuthenticatedExposureReconciliationV2"
        or reconciliation.content_hash != evidence.reconciliation_ref
        or reconciliation.metadata.get("account_scope") != account_scope
        or reconciliation.metadata.get("status") != "CURRENT"
        or reconciliation.metadata.get("complete") is not True
        or reconciliation.available_at_ns > cutoff_ns
        or cutoff_ns - reconciliation.available_at_ns > _MAX_EVIDENCE_AGE_NS
    ):
        raise ValueError("complete current exposure reconciliation required")
    if sha256_json(reconciliation.metadata) != evidence.reconciliation_ref:
        raise ValueError("exposure reconciliation hash/content mismatch")
    if tuple(sorted(reconciliation.metadata.get("exposure_refs", ()))) != tuple(
        sorted(possible.content_hash for possible in evidence.possible_risks)
    ) or tuple(sorted(reconciliation.metadata.get("closed_outcome_refs", ()))) != tuple(
        sorted(outcome.content_hash for outcome in evidence.closed_outcomes)
    ):
        raise ValueError("exposure/closed-outcome reconciliation does not cover exact risk evidence")

    realized_loss = Decimal(0)
    for outcome in evidence.closed_outcomes:
        entry = repo.get_artifact(outcome.content_hash)
        if (
            entry is None
            or entry.artifact_type != "ClosedV2Outcome"
            or entry.content_hash != outcome.content_hash
            or canonical_json(entry.metadata) != canonical_json(outcome.to_dict())
            or outcome.available_at_ns > cutoff_ns
        ):
            raise ValueError("closed outcome is not durably indexed at the cutoff")
        if outcome.outcome_class != OutcomeClass.ACTUAL_CLOSED_POSITION:
            continue
        if not cutoff_ns - DAY_NS < outcome.close_at_ns <= cutoff_ns:
            continue
        actual = repo.get_artifact(outcome.position_ref)
        metadata = actual.metadata if actual is not None else {}
        if (
            actual is None
            or actual.artifact_type != "ActualClosedPositionSourceV2"
            or actual.content_hash != outcome.position_ref
            or metadata.get("account_scope") != account_scope
            or metadata.get("close_at_ns") != outcome.close_at_ns
            or metadata.get("realized_net_pnl") != canonical_decimal_str(outcome.realized_net_pnl)
            or metadata.get("actual_system_provenance") != "RECONCILED_ATLAS_V2_POSITION_AND_ACCOUNT_PNL"
            or sha256_json(metadata) != outcome.position_ref
        ):
            raise ValueError("ACTUAL closed loss lacks matching reconciled position and account-PnL evidence")
        execution = repo.get_artifact(metadata.get("execution_source_ref", ""))
        economics = repo.get_artifact(metadata.get("economic_source_ref", ""))
        shared = {
            "account_scope": account_scope,
            "position_epoch_id": metadata.get("position_epoch_id"),
            "key": metadata.get("key"),
            "close_at_ns": outcome.close_at_ns,
        }
        if (
            execution is None
            or economics is None
            or execution.artifact_type != "V2ActualExecutionCloseObservationV1"
            or economics.artifact_type != "V2ActualAccountPnlObservationV1"
            or sha256_json(execution.metadata) != metadata.get("execution_source_ref")
            or sha256_json(economics.metadata) != metadata.get("economic_source_ref")
            or execution.metadata.get("source_system") != "VENUE_RECONCILED_EXECUTION"
            or economics.metadata.get("source_system") != "ACCOUNT_RECONCILED_CASH"
            or any(
                canonical_json(execution.metadata.get(k)) != canonical_json(v)
                or canonical_json(economics.metadata.get(k)) != canonical_json(v)
                for k, v in shared.items()
            )
        ):
            raise ValueError("ACTUAL close execution/cash sources do not reconcile")
        # Sum losses individually; a profit cannot offset or replenish the allowance.
        if outcome.realized_net_pnl < 0:
            realized_loss += -outcome.realized_net_pnl

    existing = Decimal(0)
    pending = Decimal(0)
    for possible in evidence.possible_risks:
        entry = repo.get_artifact(possible.content_hash)
        if (
            entry is None
            or entry.artifact_type != "PossibleRiskV2"
            or entry.content_hash != possible.content_hash
            or canonical_json(entry.metadata) != canonical_json(possible.to_dict())
            or possible.available_at_ns > cutoff_ns
            or cutoff_ns - possible.available_at_ns > _MAX_EVIDENCE_AGE_NS
        ):
            raise ValueError("cutoff-current indexed possible-risk evidence required")
        source = repo.get_artifact(possible.source_ref)
        source_kind = (
            "V2ActualOpenPositionRiskObservationV1"
            if possible.kind == ExposureKind.OPEN
            else "V2ActualReservationRiskObservationV1"
        )
        expected_system = (
            "VENUE_RECONCILED_POSITION" if possible.kind == ExposureKind.OPEN else "ATLAS_DURABLE_RESERVATION"
        )
        if (
            source is None
            or source.artifact_type != source_kind
            or source.content_hash != possible.source_ref
            or sha256_json(source.metadata) != possible.source_ref
            or source.metadata.get("account_scope") != account_scope
            or source.metadata.get("source_system") != expected_system
            or source.metadata.get("kind") != possible.kind.value
            or canonical_json(source.metadata.get("key")) != canonical_json(possible.key.to_dict())
            or source.metadata.get("possible_normal_loss") != canonical_decimal_str(possible.possible_normal_loss)
            or source.available_at_ns > cutoff_ns
            or cutoff_ns - source.available_at_ns > _MAX_EVIDENCE_AGE_NS
        ):
            raise ValueError("cutoff-current open/UNKNOWN/PARTIAL position or reservation evidence required")
        if possible.kind == ExposureKind.OPEN:
            existing += possible.possible_normal_loss
        else:
            # PENDING, UNKNOWN, and PARTIAL preserve the full possible remaining loss.
            pending += possible.possible_normal_loss
    if existing != snapshot.existing_open_normal_loss or pending != snapshot.pending_reserved_normal_loss:
        raise ValueError("account risk totals do not reconcile to possible open and reserved exposure")
    if existing + pending < v1_reserved_normal_loss:
        raise ValueError("V2 account evidence omits durable V1 reservation risk")
    if snapshot.opening_intents < v1_unresolved_opening_intents:
        raise ValueError("V2 account evidence omits a durable V1 opening intent")

    v1_concurrency = risk_policy_v1.max_simultaneous_new_risk_intents
    concurrency = min(1, v1_concurrency, risk_policy_v2.max_opening_intents_per_account)
    realized_limit = snapshot.eligible_equity * risk_policy_v2.rolling_24h_realized_loss_limit_frac
    new_risk_limit = snapshot.eligible_equity * risk_policy_v2.rolling_24h_new_risk_limit_frac
    reasons: list[str] = []
    if realized_loss > realized_limit:
        reasons.append("rolling 24h ACTUAL realized-loss limit exceeded")
    if realized_loss + existing + pending + proposed_normal_loss > new_risk_limit:
        reasons.append("rolling 24h realized/open/reserved/proposed normal-loss limit exceeded")
    if snapshot.opening_intents + 1 > concurrency:
        reasons.append("max opening intents per account exceeded")
    return V2LiveRiskDecision(
        not reasons,
        tuple(reasons),
        realized_loss,
        existing,
        pending,
        proposed_normal_loss,
        snapshot.opening_intents,
        sha256_json(
            {
                "snapshot": snapshot.to_dict(),
                "closed_outcomes": [x.to_dict() for x in evidence.closed_outcomes],
                "possible_risks": [x.to_dict() for x in evidence.possible_risks],
                "account_observation_ref": evidence.account_observation_ref,
                "reconciliation_ref": evidence.reconciliation_ref,
                "cutoff_ns": cutoff_ns,
                "v1_risk_policy_hash": risk_policy_v1.policy_hash(),
                "risk_policy_v2_hash": risk_policy_v2.policy_hash,
            }
        ),
    )


def persist_v2_bridged_trade_plan(
    journal: Any, bridge: V2CapitalBridgeEnvelope, plan: TradePlanEnvelopeV2
) -> TradePlan:
    """Persist the exact bridge-bound V1 plan before a human can approve it."""
    v1_plan = bridge_to_v1_trade_plan(bridge, plan)
    journal.create_trade_plan(v1_plan)
    return v1_plan


def _v2_authority_ops_refs(
    repo: OpsRepository,
    bridge: V2CapitalBridgeEnvelope,
    plan: TradePlanEnvelopeV2,
    capability_profile: VenueCapabilityProfileV2,
    risk_evidence: V2LiveRiskEvidence,
) -> tuple[str, ...]:
    """Return every immutable ops ref used by an authority decision."""
    refs = {
        bridge.content_hash,
        bridge.candidate_set_ref,
        bridge.candidate_ref,
        bridge.action_artifact_ref,
        bridge.sizing_ref,
        bridge.evaluation_ref,
        bridge.venue_capability_profile_ref,
        bridge.venue_capability_snapshot_ref,
        bridge.product_ref,
        bridge.cost_model_ref,
        bridge.v2_trade_plan_hash,
        plan.content_hash,
        capability_profile.content_hash,
        risk_evidence.account_snapshot.content_hash,
        risk_evidence.account_observation_ref,
        risk_evidence.reconciliation_ref,
    }
    refs.update(row_ref for row in capability_profile.rows for row_ref in row.evidence_refs)
    for outcome in risk_evidence.closed_outcomes:
        refs.add(outcome.content_hash)
        refs.add(outcome.position_ref)
        position = repo.get_artifact(outcome.position_ref)
        if position is not None:
            for key in ("execution_source_ref", "economic_source_ref"):
                source_ref = position.metadata.get(key)
                if isinstance(source_ref, str) and _SHA256.fullmatch(source_ref):
                    refs.add(source_ref)
    for possible in risk_evidence.possible_risks:
        refs.add(possible.content_hash)
        refs.add(possible.source_ref)
    return tuple(sorted(refs))


def _require_current_ops_refs(repo: OpsRepository, refs: tuple[str, ...]) -> tuple[tuple[str, str], ...]:
    fingerprints = []
    for ref in refs:
        item = repo.get_artifact(ref)
        if item is None or item.artifact_ref != ref or item.content_hash != ref:
            raise ValueError("attested immutable ops artifact reference is missing or changed")
        fingerprints.append(
            (
                ref,
                sha256_json(
                    {
                        "artifact_ref": item.artifact_ref,
                        "artifact_type": item.artifact_type,
                        "content_hash": item.content_hash,
                        "created_at_ns": item.created_at_ns,
                        "available_at_ns": item.available_at_ns,
                        "metadata": dict(item.metadata),
                    }
                ),
            )
        )
    return tuple(fingerprints)


def _require_v2_current_recovery(
    journal: Any,
    *,
    recovery_run_id: str,
    writer_id: str,
    writer_epoch: int,
    runtime_instance_id: str,
) -> None:
    certificate = journal.load_latest_recovery_certificate()
    if certificate is None or certificate.recovery_run_id != recovery_run_id:
        raise ValueError("current live recovery certificate is missing or changed")
    if (
        certificate.decision.value != "READY"
        or certificate.reconciliation_health.value != "CURRENT"
        or certificate.writer_id != writer_id
        or certificate.writer_epoch != writer_epoch
        or certificate.runtime_instance_id != runtime_instance_id
        or certificate.journal_schema_version != (journal.schema_version() or 0)
        or certificate.unresolved_intents
        or certificate.unresolved_commands
        or certificate.unknown_commands
    ):
        raise ValueError("recovery-required or stale live-control state invalidates V2 opening authority")


def _live_authority_evidence(
    journal: Any,
    *,
    profile: VenueCapabilityProfileV2,
    product_revision: str,
    risk_evidence: V2LiveRiskEvidence,
    risk_snapshot_hash: str,
    protection_evidence_ref: str,
    recovery_run_id: str,
    writer_id: str,
    writer_epoch: int,
    runtime_instance_id: str,
    cutoff_ns: int,
    allow_synthetic_fixture: bool = False,
) -> tuple[tuple[str, ...], bool]:
    """Require exact, cutoff-current evidence stored by this live writer."""
    sha256_ref(protection_evidence_ref, field="protection evidence ref")
    required: list[tuple[str, V2LiveAuthorityEvidenceKind, str | None, str | None]] = [
        (ref, V2LiveAuthorityEvidenceKind.CAPABILITY, row.capability, None)
        for row in profile.rows
        for ref in row.evidence_refs
    ]
    required.extend(
        (
            (risk_evidence.account_observation_ref, V2LiveAuthorityEvidenceKind.ACCOUNT_RISK, None, risk_snapshot_hash),
            (risk_evidence.reconciliation_ref, V2LiveAuthorityEvidenceKind.EXPOSURE_RECONCILIATION, None, None),
            (protection_evidence_ref, V2LiveAuthorityEvidenceKind.PROTECTION, None, None),
        )
    )
    seen: list[str] = []
    synthetic = False
    for evidence_ref, expected_kind, expected_capability, expected_snapshot_hash in sorted(
        required, key=lambda value: (value[0], value[1].value, value[2] or "")
    ):
        evidence = journal.load_v2_live_authority_evidence_for_source(
            evidence_ref,
            writer_id=writer_id,
            writer_epoch=writer_epoch,
            runtime_instance_id=runtime_instance_id,
            recovery_run_id=recovery_run_id,
            account_identity_hash=profile.account_identity_hash,
            venue=profile.venue,
            environment=profile.environment,
            capability_profile_hash=profile.content_hash,
            kind=expected_kind,
            capability=expected_capability,
            account_risk_snapshot_hash=expected_snapshot_hash,
            cutoff_ns=cutoff_ns,
            max_age_ns=_MAX_EVIDENCE_AGE_NS,
            require_live=not allow_synthetic_fixture,
        )
        if evidence.product_revision != product_revision:
            raise ValueError("live-control evidence product revision differs from the current profile")
        synthetic = synthetic or evidence.synthetic_fixture
        seen.append(evidence.content_hash)
    return tuple(sorted(seen)), synthetic
def _attest_v2_bridge_authority(
    shell: Any,
    *,
    repo: OpsRepository,
    bridge: V2CapitalBridgeEnvelope,
    plan: TradePlanEnvelopeV2,
    capability_profile: VenueCapabilityProfileV2,
    risk_evidence: V2LiveRiskEvidence,
    risk_policy_v1: RiskPolicy,
    risk_policy_v2: RiskPolicyV2,
    account_scope: str,
    cutoff_ns: int,
    attested_at_ns: int,
    expires_at_ns: int,
    protection_evidence_ref: str,
    v1_revalidation_evidence: Any,
) -> V2CapitalAuthorityAttestation:
    """Persist authority only after the active live writer accepts all evidence.

    Offline fixtures are persisted as ``SYNTHETIC_TEST_ONLY`` and can never
    authorize an opening. Ops labels alone do not create live evidence rows.
    """
    from atlas.persistence.sqlite import PersistenceError
    from atlas.runtime.assisted_control import revalidate_plan

    context = getattr(shell, "v2_live_writer_context", lambda: None)()
    if context is None:
        raise ValueError("V2 capital attestation requires the active SafeRuntime writer")
    writer_id, writer_epoch, runtime_instance_id = context
    if cutoff_ns != v1_revalidation_evidence.now_ns or attested_at_ns != cutoff_ns:
        raise ValueError("live V1/V2 evidence cutoff and attestation timestamp must match")
    _require_v2_current_recovery(
        shell.journal,
        recovery_run_id=v1_revalidation_evidence.recovery_run_id,
        writer_id=writer_id,
        writer_epoch=writer_epoch,
        runtime_instance_id=runtime_instance_id,
    )
    if (
        expires_at_ns <= attested_at_ns
        or expires_at_ns
        > min(plan.expires_at_ns, capability_profile.expires_at_ns, attested_at_ns + _MAX_EVIDENCE_AGE_NS)
    ):
        raise ValueError("attestation expiry must be within the exact plan and capability expiry")
    if (
        account_scope != bridge.account_identity_hash
        or risk_policy_v1.policy_hash() != bridge.v1_risk_policy_hash
        or risk_policy_v2.policy_hash != bridge.risk_policy_v2_hash
        or plan.content_hash != bridge.v2_trade_plan_hash
        or capability_profile.content_hash != bridge.venue_capability_profile_ref
    ):
        raise ValueError("current account, plan, capability, or risk policy differs from the bridge")

    validate_venue_profile_for_capital(repo, capability_profile, now_ns=cutoff_ns)
    refs = _v2_authority_ops_refs(repo, bridge, plan, capability_profile, risk_evidence)
    ops_fingerprints = _require_current_ops_refs(repo, refs)
    _require_indexed(
        repo, bridge.content_hash, "V2CapitalBridgeEnvelope", metadata_key="bridge", expected=bridge.to_dict()
    )
    _require_indexed(
        repo,
        capability_profile.content_hash,
        "VenueCapabilityProfileV2",
        metadata_key="profile",
        expected=capability_profile.to_dict(),
    )
    _require_indexed(repo, plan.content_hash, "TradePlanEnvelopeV2", metadata_key="plan", expected=plan.to_dict())

    v1_plan = bridge_to_v1_trade_plan(bridge, plan)
    current_v1 = revalidate_plan(journal=shell.journal, plan=v1_plan, evidence=v1_revalidation_evidence)
    if not current_v1.ok:
        raise ValueError("current V1 recovery/risk revalidation failed: " + "; ".join(current_v1.reasons))
    decision = revalidate_v2_rolling_risk(
        repo=repo,
        evidence=risk_evidence,
        risk_policy_v1=risk_policy_v1,
        risk_policy_v2=risk_policy_v2,
        account_scope=account_scope,
        cutoff_ns=cutoff_ns,
        proposed_normal_loss=bridge.normal_risk,
        v1_reserved_normal_loss=shell.journal.reservation_totals()["normal_loss"],
        v1_unresolved_opening_intents=len(shell.journal.load_unresolved_intents()),
    )
    if not decision.allowed:
        raise ValueError("current RiskPolicyV2 revalidation failed: " + "; ".join(decision.reasons))
    try:
        live_refs, synthetic = _live_authority_evidence(
            shell.journal,
            profile=capability_profile,
            product_revision=bridge.product_revision,
            risk_evidence=risk_evidence,
            risk_snapshot_hash=risk_evidence.account_snapshot.content_hash,
            protection_evidence_ref=protection_evidence_ref,
            recovery_run_id=v1_revalidation_evidence.recovery_run_id,
            writer_id=writer_id,
            writer_epoch=writer_epoch,
            runtime_instance_id=runtime_instance_id,
            cutoff_ns=cutoff_ns,
            allow_synthetic_fixture=True,
        )
    except (AttributeError, PersistenceError, ValueError) as exc:
        raise ValueError(f"live authority evidence rejected: {exc}") from exc
    evidence_refs = tuple(sorted({ref for row in capability_profile.rows for ref in row.evidence_refs}))
    attestation = V2CapitalAuthorityAttestation(
        bridge.content_hash,
        plan.content_hash,
        capability_profile.content_hash,
        bridge.venue_capability_snapshot_ref,
        evidence_refs,
        account_scope,
        bridge.venue,
        bridge.environment,
        bridge.instrument_key_ref,
        bridge.product_ref,
        bridge.product_revision,
        risk_policy_v1.policy_hash(),
        risk_policy_v2.policy_hash,
        risk_evidence.account_snapshot.content_hash,
        risk_evidence.account_observation_ref,
        decision.evidence_hash,
        risk_evidence.reconciliation_ref,
        protection_evidence_ref,
        refs,
        ops_fingerprints,
        live_refs,
        writer_id,
        writer_epoch,
        runtime_instance_id,
        v1_revalidation_evidence.recovery_run_id,
        cutoff_ns,
        attested_at_ns,
        expires_at_ns,
        V2CapitalAuthorityStatus.SYNTHETIC_TEST_ONLY if synthetic else V2CapitalAuthorityStatus.LIVE_ATTESTED,
        "SYNTHETIC_TEST_FIXTURE" if synthetic else "AUTHENTICATED_LIVE_CONTROL",
    )
    shell.journal.append_v2_capital_authority_attestation(attestation)
    return attestation


def prepare_v2_bridge_entry(
    shell: Any,
    *,
    repo: OpsRepository,
    bridge: V2CapitalBridgeEnvelope,
    plan: TradePlanEnvelopeV2,
    capability_profile: VenueCapabilityProfileV2,
    risk_evidence: V2LiveRiskEvidence,
    risk_policy_v1: RiskPolicy,
    risk_policy_v2: RiskPolicyV2,
    account_scope: str,
    cutoff_ns: int,
    approval_id: str,
    user_identity: str,
    v1_revalidation_evidence: Any,
    authority_attestation_ref: str | None = None,
    protection_evidence_ref: str | None = None,
) -> Any:
    """Require live-control authority, then delegate unchanged to V1 durable control."""
    from atlas.persistence.sqlite import PersistenceError
    from atlas.runtime.assisted_control import AssistedControlResult, revalidate_plan, validate_approval

    if authority_attestation_ref is None:
        return AssistedControlResult("V2_AUTHORITY_BLOCKED", ("live-control capital attestation required",))
    if protection_evidence_ref is None:
        return AssistedControlResult("V2_AUTHORITY_BLOCKED", ("current live protection evidence required",))
    try:
        attestation = shell.journal.load_v2_capital_authority_attestation(authority_attestation_ref)
    except (AttributeError, PersistenceError, ValueError) as exc:
        return AssistedControlResult("V2_AUTHORITY_BLOCKED", (str(exc),))
    if attestation is None or attestation.content_hash != authority_attestation_ref:
        return AssistedControlResult("V2_AUTHORITY_BLOCKED", ("durable live-control attestation missing",))
    context = getattr(shell, "v2_live_writer_context", lambda: None)()
    if context is None:
        return AssistedControlResult("V2_AUTHORITY_BLOCKED", ("active SafeRuntime writer context required",))
    writer_id, writer_epoch, runtime_instance_id = context
    try:
        v1_plan = bridge_to_v1_trade_plan(bridge, plan)
    except (AttributeError, ValueError) as exc:
        return AssistedControlResult("V2_AUTHORITY_BLOCKED", (str(exc),))
    early_mismatches: list[str] = []
    early_expected = {
        "bridge_hash": bridge.content_hash,
        "trade_plan_hash": plan.content_hash,
        "capability_profile_hash": capability_profile.content_hash,
        "capability_snapshot_hash": bridge.venue_capability_snapshot_ref,
        "account_identity_hash": account_scope,
        "venue": bridge.venue,
        "environment": bridge.environment,
        "instrument_key_hash": bridge.instrument_key_ref,
        "product_hash": bridge.product_ref,
        "product_revision": bridge.product_revision,
        "v1_risk_policy_hash": risk_policy_v1.policy_hash(),
        "risk_policy_v2_hash": risk_policy_v2.policy_hash,
        "writer_id": writer_id,
        "writer_epoch": writer_epoch,
        "runtime_instance_id": runtime_instance_id,
        "recovery_run_id": v1_revalidation_evidence.recovery_run_id,
        "evidence_cutoff_ns": cutoff_ns,
        "protection_evidence_hash": protection_evidence_ref,
    }
    early_mismatches.extend(
        f"attestation {name} mismatch" for name, expected in early_expected.items()
        if getattr(attestation, name) != expected
    )
    if attestation.capability_evidence_refs != tuple(
        sorted({ref for row in capability_profile.rows for ref in row.evidence_refs})
    ):
        early_mismatches.append("attestation capability evidence set mismatch")
    if cutoff_ns < attestation.attested_at_ns or cutoff_ns >= attestation.expires_at_ns:
        early_mismatches.append("live-control attestation is stale or expired")
    if early_mismatches:
        return AssistedControlResult("V2_AUTHORITY_BLOCKED", tuple(early_mismatches))

    # Approval remains plan-bound and single-use; it cannot create or refresh authority.
    try:
        validate_approval(
            journal=shell.journal,
            plan=v1_plan,
            approval_id=approval_id,
            user_identity=user_identity,
            now_ns=cutoff_ns,
        )
    except PersistenceError as exc:
        return AssistedControlResult("APPROVAL_BLOCKED", (str(exc),))
    try:
        _require_v2_current_recovery(
            shell.journal,
            recovery_run_id=v1_revalidation_evidence.recovery_run_id,
            writer_id=writer_id,
            writer_epoch=writer_epoch,
            runtime_instance_id=runtime_instance_id,
        )
    except (AttributeError, PersistenceError, ValueError) as exc:
        return AssistedControlResult("V2_AUTHORITY_BLOCKED", (str(exc),))
    if v1_revalidation_evidence.now_ns != cutoff_ns:
        return AssistedControlResult("V2_RISK_BLOCKED", ("V1/V2 revalidation cutoffs differ",))
    current_v1 = revalidate_plan(journal=shell.journal, plan=v1_plan, evidence=v1_revalidation_evidence)
    if not current_v1.ok:
        return AssistedControlResult("V2_RISK_BLOCKED", current_v1.reasons)

    if capability_profile.content_hash != bridge.venue_capability_profile_ref:
        return AssistedControlResult("V2_CAPABILITY_BLOCKED", ("venue capability profile changed",))
    if (
        account_scope != bridge.account_identity_hash
        or not capability_profile.is_current(cutoff_ns)
        or not capability_profile.capital_capable
    ):
        return AssistedControlResult("V2_CAPABILITY_BLOCKED", ("venue capability profile is not current/qualified",))
    if (
        risk_policy_v1.policy_hash() != bridge.v1_risk_policy_hash
        or risk_policy_v2.policy_hash != bridge.risk_policy_v2_hash
    ):
        return AssistedControlResult("V2_RISK_BLOCKED", ("RiskPolicy or RiskPolicyV2 changed after approval",))
    try:
        validate_venue_profile_for_capital(repo, capability_profile, now_ns=cutoff_ns)
        _require_indexed(
            repo, bridge.content_hash, "V2CapitalBridgeEnvelope", metadata_key="bridge", expected=bridge.to_dict()
        )
        _require_indexed(
            repo,
            capability_profile.content_hash,
            "VenueCapabilityProfileV2",
            metadata_key="profile",
            expected=capability_profile.to_dict(),
        )
        _require_indexed(repo, plan.content_hash, "TradePlanEnvelopeV2", metadata_key="plan", expected=plan.to_dict())
        stored_plan = shell.journal.load_trade_plan(v1_plan.plan_id)
        if stored_plan.plan_hash() != v1_plan.plan_hash():
            return AssistedControlResult("V2_BRIDGE_BLOCKED", ("persisted V1 plan differs from bridge",))
        ops_refs = _v2_authority_ops_refs(repo, bridge, plan, capability_profile, risk_evidence)
        if ops_refs != attestation.ops_artifact_refs:
            return AssistedControlResult("V2_AUTHORITY_BLOCKED", ("ops artifact set differs from live attestation",))
        ops_fingerprints = _require_current_ops_refs(repo, ops_refs)
        if ops_fingerprints != attestation.ops_artifact_fingerprints:
            return AssistedControlResult("V2_AUTHORITY_BLOCKED", ("ops artifact content changed after attestation",))
        journal_reserved = shell.journal.reservation_totals()["normal_loss"]
        journal_opening_intents = len(shell.journal.load_unresolved_intents())
        decision = revalidate_v2_rolling_risk(
            repo=repo,
            evidence=risk_evidence,
            risk_policy_v1=risk_policy_v1,
            risk_policy_v2=risk_policy_v2,
            account_scope=account_scope,
            cutoff_ns=cutoff_ns,
            proposed_normal_loss=bridge.normal_risk,
            v1_reserved_normal_loss=journal_reserved,
            v1_unresolved_opening_intents=journal_opening_intents,
        )
    except (AttributeError, PersistenceError, ValueError) as exc:
        return AssistedControlResult("V2_RISK_BLOCKED", (str(exc),))
    if not decision.allowed:
        return AssistedControlResult("V2_RISK_BLOCKED", decision.reasons)
    try:
        live_refs, synthetic = _live_authority_evidence(
            shell.journal,
            profile=capability_profile,
            product_revision=bridge.product_revision,
            risk_evidence=risk_evidence,
            risk_snapshot_hash=risk_evidence.account_snapshot.content_hash,
            protection_evidence_ref=protection_evidence_ref,
            recovery_run_id=v1_revalidation_evidence.recovery_run_id,
            writer_id=writer_id,
            writer_epoch=writer_epoch,
            runtime_instance_id=runtime_instance_id,
            cutoff_ns=cutoff_ns,
            allow_synthetic_fixture=True,
        )
    except (AttributeError, PersistenceError, ValueError) as exc:
        return AssistedControlResult("V2_AUTHORITY_BLOCKED", (str(exc),))
    if live_refs != attestation.live_evidence_refs:
        return AssistedControlResult("V2_AUTHORITY_BLOCKED", ("live evidence receipt set changed after attestation",))
    if synthetic or attestation.status is not V2CapitalAuthorityStatus.LIVE_ATTESTED:
        return AssistedControlResult("V2_AUTHORITY_BLOCKED", ("synthetic or non-live attestation cannot authorize opening risk",))
    binding_reasons = attestation.binding_reasons(
        bridge=bridge,
        plan=plan,
        capability_profile=capability_profile,
        account_identity_hash=account_scope,
        v1_risk_policy_hash=risk_policy_v1.policy_hash(),
        risk_policy_v2_hash=risk_policy_v2.policy_hash,
        account_risk_snapshot_hash=risk_evidence.account_snapshot.content_hash,
        account_risk_observation_hash=risk_evidence.account_observation_ref,
        risk_evidence_hash=decision.evidence_hash,
        exposure_reconciliation_hash=risk_evidence.reconciliation_ref,
        protection_evidence_hash=protection_evidence_ref,
        writer_id=writer_id,
        writer_epoch=writer_epoch,
        runtime_instance_id=runtime_instance_id,
        recovery_run_id=v1_revalidation_evidence.recovery_run_id,
        cutoff_ns=cutoff_ns,
        now_ns=cutoff_ns,
    )
    if binding_reasons:
        return AssistedControlResult("V2_AUTHORITY_BLOCKED", binding_reasons)
    return shell.prepare_entry(
        plan=v1_plan, approval_id=approval_id, user_identity=user_identity, evidence=v1_revalidation_evidence
    )
