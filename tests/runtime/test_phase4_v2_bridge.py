from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import pytest
from v2.test_session014_core import KEY
from v2.test_session019_admission import _evaluation

from atlas.domain.enums import Side
from atlas.domain.execution import Approval
from atlas.domain.risk import engineering_default_policy
from atlas.persistence.sqlite import PersistenceError, SQLiteJournal
from atlas.runtime.assisted_control import validate_approval
from atlas.runtime.phase4_v2 import (
    BYBIT_REQUIRED_CAPABILITIES_V2,
    QualificationStatusV2,
    V2CapitalBridgeEnvelope,
    VenueCapabilityProfileV2,
    VenueCapabilityRowV2,
    bridge_to_v1_trade_plan,
    build_v2_capital_bridge,
    persist_v2_bridged_trade_plan,
    validate_v2_bridge_material,
    validate_venue_profile_for_capital,
)
from atlas.v2._serialization import sha256_json
from atlas.v2.contracts import (
    ArtifactEnvelope,
    CandidateSelectionStatus,
    DecisionStatusV2,
    TradePlanEnvelopeV2,
    V2Side,
)
from atlas.v2.instruments import EnvironmentV2, VenueV2
from atlas.v2.risk import RiskPolicyV2, SizingStatus
from atlas.v2.science.admission import VenueCapabilityStatusV2


def _ref(name: str) -> str:
    return sha256_json({"phase4-test-ref": name})


def _profile(*, venue_status: QualificationStatusV2 = QualificationStatusV2.UNVERIFIED,
             evidence_refs: tuple[str, ...] = ()) -> VenueCapabilityProfileV2:
    account = _ref("hashed-account")
    rows = tuple(VenueCapabilityRowV2(name, QualificationStatusV2.TESTED, venue_status, evidence_refs)
                 for name in BYBIT_REQUIRED_CAPABILITIES_V2)
    return VenueCapabilityProfileV2(VenueV2.BYBIT, EnvironmentV2.TESTNET, account,
        _ref("product"), KEY.content_hash, "ONE_WAY", "ISOLATED", "nautilus_trader", "2.0.0rc5",
        "1b0a49d2792a9432a3aca3fcb617ce7a630d905e", _ref("nautilus-wheel"), _ref("requirements-lock"),
        _ref("protection"), _ref("fees"), _ref("filters"), _ref("capability-snapshot"), rows,
        100, 300, False)


def _plan() -> TradePlanEnvelopeV2:
    return TradePlanEnvelopeV2(
        ArtifactEnvelope(1, "phase4-plan", 100, 100, "phase4-test", ()),
        _ref("plan-id"), "2.0", "2.0", KEY, _ref("product"), _ref("hashed-account"),
        _ref("policy"), _ref("action"), _ref("evaluation"), _ref("v1-risk"), _ref("capability"),
        1, V2Side.LONG, Decimal("0.01"), "ioc-entry", Decimal("100"), Decimal("90"),
        "MARK_PRICE", "fixed-stop", 300, Decimal("10"), Decimal("20"), Decimal("100"),
        Decimal("2"), Decimal("99"), 200)


def _bridge(plan: TradePlanEnvelopeV2) -> V2CapitalBridgeEnvelope:
    return V2CapitalBridgeEnvelope(
        _ref("candidate-set"), "selected", _ref("candidate"), _ref("action-artifact"), _ref("action-hash"),
        _ref("sizing"), _ref("sizing-hash"), _ref("v1-risk"), _ref("v2-risk"), _ref("evaluation"),
        _ref("profile"), _ref("capability"), _ref("product"), KEY.contract_revision, _ref("cost-model"),
        _ref("hashed-account"), VenueV2.BYBIT, EnvironmentV2.TESTNET, KEY.content_hash, "LONG",
        Decimal("0.01"), Decimal("100"), Decimal("90"), "MARK_PRICE", 300, 200,
        Decimal("10"), Decimal("20"), Decimal("100"), Decimal("2"), plan.plan_id,
        plan.content_hash, "S1:1")


def _material():
    v1 = engineering_default_policy(policy_version="phase4-binding", policy_effective_at_ns=0)
    v2 = RiskPolicyV2("phase4-binding", 0, v1.policy_hash(), Decimal("0.5"), Decimal("0.2"), 2)
    profile = _profile()
    selected_id = "selected-candidate"
    candidate_ref = _ref("candidate")
    set_ref = _ref("candidate-set")
    action_hash = _ref("frozen-action")
    action_ref = _ref("action-artifact")
    sizing_ref = _ref("sizing")
    eval_ref = _ref("evaluation")
    capability_ref = profile.capability_snapshot_ref
    product_ref = profile.product_ref
    entry_rule = {"kind": "IOC"}
    management = {"kind": "fixed-stop"}
    candidate = SimpleNamespace(candidate_id=selected_id, content_hash=candidate_ref,
        account_scope=profile.account_identity_hash, key=KEY, side=V2Side.LONG,
        deadline_ns=1_000, cost_model_ref=_ref("cost-model"), policy_hash=_ref("policy"),
        quantity=None, horizon_end_ns=300, entry_reference=Decimal("99"),
        entry_collar=Decimal("100"), stop_price=Decimal("90"))
    candidate_set = SimpleNamespace(selection_status=CandidateSelectionStatus.SELECTED,
        selected_candidate_id=selected_id, content_hash=set_ref,
        candidates=(SimpleNamespace(candidate_id=selected_id, key=KEY, side=V2Side.LONG, policy_id="S1"),))
    frozen = SimpleNamespace(key=KEY, side="LONG", quantity=Decimal("0.01"), action_hash=action_hash,
        product_ref=product_ref, policy_hash=_ref("policy"), risk_policy_hash=v1.policy_hash(),
        risk_policy_v2_hash=v2.policy_hash, entry_collar=Decimal("100"), stop_price=Decimal("90"),
        stop_trigger_basis="MARK_PRICE", horizon_end_ns=300, entry_reference=Decimal("99"),
        entry_rule=SimpleNamespace(to_dict=lambda: entry_rule), entry_trigger_basis="MARK_PRICE",
        management_rule=SimpleNamespace(to_dict=lambda: management), policy_id="S1", policy_version="1")
    action = SimpleNamespace(action=frozen, content_hash=action_ref, candidate_ref=candidate_ref,
        candidate_set_ref=set_ref, sizing_ref=sizing_ref)
    sizing = SimpleNamespace(content_hash=sizing_ref, status=SizingStatus.SIZED,
        quantity=Decimal("0.01"), candidate_ref=candidate_ref, candidate_set_ref=set_ref,
        selected_candidate_id=selected_id, product_ref=product_ref, account_snapshot_ref=_ref("account-snapshot"),
        risk_policy_hash=v1.policy_hash(),
        risk_policy_v2_hash=v2.policy_hash, normal_risk=Decimal("10"), stress_risk=Decimal("20"),
        margin=Decimal("100"), leverage=Decimal("2"))
    evaluation = SimpleNamespace(decision=DecisionStatusV2.CANDIDATE, action_expiry_ns=200,
        content_hash=eval_ref, action_hash=action_hash, action_artifact_ref=action_ref,
        candidate_ref=candidate_ref, candidate_set_ref=set_ref, quantity=Decimal("0.01"),
        policy_hash=frozen.policy_hash, risk_policy_ref=v1.policy_hash(), risk_policy_hash=v1.policy_hash(),
        risk_policy_v2_ref=v2.policy_hash, risk_policy_v2_hash=v2.policy_hash,
        account_snapshot_ref=_ref("account-snapshot"), capability_evidence_ref=capability_ref)
    cap = SimpleNamespace(content_hash=capability_ref, observed_status=VenueCapabilityStatusV2.SUPPORTED,
        synthetic_fixture=False, account_scope=profile.account_identity_hash,
        venue=profile.venue, environment=profile.environment, product_ref=profile.product_ref,
        instrument_key_ref=profile.instrument_key_ref, position_mode=profile.position_mode,
        margin_mode=profile.margin_mode, nautilus_distribution=profile.nautilus_distribution,
        nautilus_version=profile.nautilus_version, nautilus_source_commit=profile.nautilus_source_commit,
        nautilus_artifact_ref=profile.nautilus_artifact_ref,
        protection_profile_ref=profile.protection_profile_ref, available_at_ns=100)
    plan = SimpleNamespace(content_hash=_ref("plan"), plan_id=_ref("plan-id"),
        action_hash=action_hash, evaluation_ref=eval_ref, risk_policy_hash=v1.policy_hash(),
        policy_hash=frozen.policy_hash, capability_manifest_hash=capability_ref, product_ref=product_ref, key=KEY,
        account_scope=profile.account_identity_hash, side=V2Side.LONG, qty_limit=Decimal("0.01"),
        collar=Decimal("100"), stop=Decimal("90"), stop_trigger_basis="MARK_PRICE", horizon_end_ns=300,
        expires_at_ns=200, reference_price=Decimal("99"), entry_policy='{"entry_rule":{"kind":"IOC"},"trigger_basis":"MARK_PRICE"}',
        management_policy='{"kind":"fixed-stop"}', normal_risk=Decimal("10"), stress_risk=Decimal("20"),
        margin=Decimal("100"), leverage_bound=Decimal("2"))
    return SimpleNamespace(v1=v1, v2=v2, profile=profile, candidate=candidate, candidate_set=candidate_set,
        action=action, sizing=sizing, evaluation=evaluation, capability=cap, plan=plan)


def test_capability_profile_is_immutable_exact_and_unverified_without_actual_evidence():
    profile = _profile()
    assert profile.qualification_status() == QualificationStatusV2.UNVERIFIED
    assert profile.capital_capable is False
    assert VenueCapabilityProfileV2.from_dict(profile.to_dict()) == profile
    changed = replace(profile, margin_mode="CROSS")
    assert changed.content_hash != profile.content_hash
    with pytest.raises(ValueError, match="exactly match"):
        replace(profile, rows=profile.rows[:-1])


@pytest.mark.parametrize("decision", [DecisionStatusV2.NO_TRADE, DecisionStatusV2.NOT_ESTIMABLE])
def test_non_candidate_economic_decisions_never_create_bridge(tmp_path, decision):
    with pytest.raises(ValueError, match="economic CANDIDATE"):
        build_v2_capital_bridge(None, candidate_set=None, candidate=None, action=None, sizing=None,
            evaluation=_evaluation(decision), capability_snapshot=None, capability_profile=_profile(),
            plan=None, risk_policy_v1=None, risk_policy_v2=None, now_ns=102)


def test_unqualified_venue_cannot_create_bridge_even_for_candidate_evaluation(tmp_path):
    from atlas.v2.memory.repository import OpsRepository

    with OpsRepository(tmp_path / "ops.sqlite") as repo, pytest.raises(ValueError, match="not actually qualified"):
        validate_venue_profile_for_capital(repo, _profile(), now_ns=102)


def test_exact_candidate_action_sizing_policy_and_plan_bindings_are_required():
    case = _material()
    validate_v2_bridge_material(candidate_set=case.candidate_set, candidate=case.candidate,
        action=case.action, sizing=case.sizing, evaluation=case.evaluation,
        capability_snapshot=case.capability, capability_profile=case.profile, plan=case.plan,
        risk_policy_v1=case.v1, risk_policy_v2=case.v2, now_ns=102)

    changed_set = SimpleNamespace(**(vars(case.candidate_set) | {"selected_candidate_id": "other"}))
    with pytest.raises(ValueError, match="CandidateSet"):
        validate_v2_bridge_material(candidate_set=changed_set, candidate=case.candidate,
            action=case.action, sizing=case.sizing, evaluation=case.evaluation,
            capability_snapshot=case.capability, capability_profile=case.profile, plan=case.plan,
            risk_policy_v1=case.v1, risk_policy_v2=case.v2, now_ns=102)

    changed_action = SimpleNamespace(**(vars(case.action) | {"action": SimpleNamespace(
        **(vars(case.action.action) | {"action_hash": _ref("changed-action")}))}))
    with pytest.raises(ValueError, match="economic evaluation"):
        validate_v2_bridge_material(candidate_set=case.candidate_set, candidate=case.candidate,
            action=changed_action, sizing=case.sizing, evaluation=case.evaluation,
            capability_snapshot=case.capability, capability_profile=case.profile, plan=case.plan,
            risk_policy_v1=case.v1, risk_policy_v2=case.v2, now_ns=102)

    changed_sizing = SimpleNamespace(**(vars(case.sizing) | {"quantity": Decimal("0.005")}))
    with pytest.raises(ValueError, match="sizing"):
        validate_v2_bridge_material(candidate_set=case.candidate_set, candidate=case.candidate,
            action=case.action, sizing=changed_sizing, evaluation=case.evaluation,
            capability_snapshot=case.capability, capability_profile=case.profile, plan=case.plan,
            risk_policy_v1=case.v1, risk_policy_v2=case.v2, now_ns=102)

    changed_v2 = RiskPolicyV2("changed", 0, case.v1.policy_hash(), Decimal("0.5"), Decimal("0.2"), 2)
    with pytest.raises(ValueError, match="policy identity"):
        validate_v2_bridge_material(candidate_set=case.candidate_set, candidate=case.candidate,
            action=case.action, sizing=case.sizing, evaluation=case.evaluation,
            capability_snapshot=case.capability, capability_profile=case.profile, plan=case.plan,
            risk_policy_v1=case.v1, risk_policy_v2=changed_v2, now_ns=102)


@pytest.mark.parametrize("field,value", [("qty_limit", Decimal("0.005")),
    ("collar", Decimal("101")), ("stop", Decimal("89"))])
def test_quantity_collar_and_stop_mutation_break_bridge_binding(field, value):
    case = _material()
    changed_plan = SimpleNamespace(**(vars(case.plan) | {field: value}))
    with pytest.raises(ValueError, match="TradePlanEnvelopeV2"):
        validate_v2_bridge_material(candidate_set=case.candidate_set, candidate=case.candidate,
            action=case.action, sizing=case.sizing, evaluation=case.evaluation,
            capability_snapshot=case.capability, capability_profile=case.profile, plan=changed_plan,
            risk_policy_v1=case.v1, risk_policy_v2=case.v2, now_ns=102)


def test_changed_capability_and_expiry_break_bridge_binding():
    case = _material()
    changed_capability = SimpleNamespace(**(vars(case.capability) | {"content_hash": _ref("new-capability")}))
    with pytest.raises(ValueError, match="capability snapshot"):
        validate_v2_bridge_material(candidate_set=case.candidate_set, candidate=case.candidate,
            action=case.action, sizing=case.sizing, evaluation=case.evaluation,
            capability_snapshot=changed_capability, capability_profile=case.profile, plan=case.plan,
            risk_policy_v1=case.v1, risk_policy_v2=case.v2, now_ns=102)

    changed_evaluation = SimpleNamespace(**(vars(case.evaluation) | {"action_expiry_ns": 102}))
    with pytest.raises(ValueError, match="expired"):
        validate_v2_bridge_material(candidate_set=case.candidate_set, candidate=case.candidate,
            action=case.action, sizing=case.sizing, evaluation=changed_evaluation,
            capability_snapshot=case.capability, capability_profile=case.profile, plan=case.plan,
            risk_policy_v1=case.v1, risk_policy_v2=case.v2, now_ns=102)


def test_bridge_materializes_exact_v1_approval_identity_and_mutation_gets_new_identity(tmp_path):
    v2_plan = _plan()
    bridge = _bridge(v2_plan)
    assert V2CapitalBridgeEnvelope.from_dict(bridge.to_dict()) == bridge
    v1_plan = bridge_to_v1_trade_plan(bridge, v2_plan)
    assert v1_plan.snapshot_hash == bridge.content_hash
    assert v1_plan.qty_limit == bridge.quantity
    assert v1_plan.side == Side.LONG
    assert v1_plan.risk_config_hash == bridge.v1_risk_policy_hash
    assert bridge.content_hash == sha256_json(bridge.to_dict())

    journal = SQLiteJournal(tmp_path / "v1-journal.sqlite")
    persisted = persist_v2_bridged_trade_plan(journal, bridge, v2_plan)
    approval = Approval("approval-v2-bridge", "operator", persisted.plan_id, persisted.version,
        persisted.created_at_ns + 1, persisted.expires_at_ns - 1)
    journal.create_approval(approval)
    assert validate_approval(journal=journal, plan=persisted, approval_id=approval.approval_id,
        user_identity="operator", now_ns=persisted.created_at_ns + 2) == approval

    changed_bridge = replace(bridge, quantity=Decimal("0.005"))
    changed_plan = bridge_to_v1_trade_plan(changed_bridge, v2_plan)
    assert changed_bridge.content_hash != bridge.content_hash
    assert changed_plan.plan_id != persisted.plan_id
    with pytest.raises(PersistenceError, match="trade plan not found"):
        validate_approval(journal=journal, plan=changed_plan, approval_id=approval.approval_id,
            user_identity="operator", now_ns=persisted.created_at_ns + 2)
    assert journal.load_approval(approval.approval_id).consumed_at_ns is None
    journal.close()


def test_v2_plan_mutation_cannot_be_used_with_older_bridge():
    plan = _plan()
    bridge = _bridge(plan)
    changed_plan = replace(plan, qty_limit=Decimal("0.005"), envelope=replace(plan.envelope, content_hash=""))
    with pytest.raises(ValueError, match="different V2 TradePlanEnvelope"):
        bridge_to_v1_trade_plan(bridge, changed_plan)
